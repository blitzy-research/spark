/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.streaming

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap, ConcurrentSkipListSet,
  LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}
import java.util.concurrent.locks.ReentrantLock

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener}

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys.{BLOCK_ID, CHECKSUM, CONFIG, COUNT, ERROR, HOST_PORT,
  MAP_ID, MAX_ATTEMPTS, MESSAGE, NUM_BYTES, NUM_EVENTS, NUM_SKIPPED, PARTITION_ID, REASON,
  SHUFFLE_ID, THRESHOLD, TIMEOUT, VALUE}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.protocol.OneWayMessage
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, DataBlockMessage,
  HeartbeatMessage, RetransmitRequestMessage, StreamingShuffleMessage, StreamingShuffleMessageType,
  StreamTerminationMessage}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.util.{Clock, SystemClock}

/**
 * The consumer side of a streaming shuffle channel: the transport handler that accepts blocks a
 * producer pushes, hands them to the reduce task, acknowledges what the task has consumed, repairs
 * bounded corruption by asking for a replay, and exerts TCP-level backpressure by turning the
 * channel's `autoRead` off and on again.
 *
 * Only this handler touches `autoRead`: reading is stopped when the credit the backpressure
 * protocol owns is exhausted and restored when an acknowledgement advances the ledger, which is the
 * third and last of the subsystem's backpressure layers. A replay is only ever asked for within the
 * producer's retained unacknowledged window; a position outside it is a fetch failure instead,
 * because the bytes are no longer held.
 *
 * Every method here runs on a Netty event-loop thread. Nothing blocks, sleeps or waits for the task
 * thread; no shuffle metrics reporter is called, since those belong to the task thread alone; no
 * exception is allowed to escape a callback; and a failure is handed to
 * [[StreamingShuffleErrorNotifier]] so the reduce task re-throws it rather than waiting for input
 * that will never come.
 *
 * @param conf the executor's configuration, read once at construction and then held immutably,
 *     which is what makes "a configuration change requires an executor restart" true by
 *     construction
 * @param shuffleId the shuffle this channel serves; every frame that names another shuffle is
 *     refused at the boundary
 * @param mapId the map output this channel carries, which together with the task attempt is the
 *     producer generation the receiving ledger is keyed by
 * @param taskAttemptId the producing attempt, so a superseded attempt cannot share accounting
 *     with the attempt that replaced it
 * @param consumerId this consumer session's identity, which is what the producer's retained
 *     store keys its per-consumer acknowledgement cursor by
 * @param startPartition the first reduce partition this task reads, inclusive.
 * @param endPartition one past the last reduce partition this task reads
 * @param backpressure the flow-control protocol that owns the credit ledger, the liveness
 *     timers and the backpressure-event counter; must not be null
 * @param errorNotifier the bridge that carries a failure observed on this I/O thread to the
 *     task thread; must not be null
 * @param clock the single time source for every timer in this class, so each one advances with
 *     that clock rather than with wall time and nothing here sleeps
 */
private[spark] class StreamingShuffleClientHandler(
    conf: SparkConf,
    val shuffleId: Int,
    val mapId: Long,
    taskAttemptId: Long,
    val consumerId: String,
    startPartition: Int,
    endPartition: Int,
    backpressure: BackpressureProtocol,
    errorNotifier: StreamingShuffleErrorNotifier,
    clock: Clock = new SystemClock)
  extends RpcHandler with Logging {

  import StreamingShuffleClientHandler._

  require(conf != null, "The Spark configuration must not be null.")
  require(shuffleId >= 0, s"The shuffle id must be non-negative but was $shuffleId.")
  require(mapId >= 0L, s"The producing map id must be non-negative but was $mapId.")
  require(taskAttemptId >= 0L,
    s"The producing task attempt id must be non-negative but was $taskAttemptId.")
  require(consumerId != null && consumerId.nonEmpty,
    "The streaming shuffle consumer session id must not be empty.")
  require(startPartition >= 0,
    s"The first reduce partition read must be non-negative but was $startPartition.")
  require(endPartition >= startPartition,
    s"The reduce partition range [$startPartition, $endPartition) must not be inverted.")
  require(backpressure != null, "The streaming shuffle backpressure protocol must not be null.")
  require(errorNotifier != null, "The streaming shuffle error notifier must not be null.")
  require(clock != null, "The streaming shuffle clock must not be null.")

  /**
   * The ledger identity of one partition of this producer's output, as seen from the receiving end.
   */
  private def consumerKey(partitionId: Int): BackpressureStreamKey =
    BackpressureStreamKey.forConsumer(shuffleId, mapId, taskAttemptId, partitionId, consumerId)

  /** The fixed-width form of [[consumerId]], declared on every heartbeat this handler sends. */
  private val consumerToken: Long = BackpressureStreamKey.consumerTokenOf(consumerId)

  // Read once and held immutably.
  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  /**
   * Cumulative totals, per channel, for the conditions a producer's behaviour decides the frequency
   * of.
   */
  private val repairsRequested = new AtomicLong(0L)

  // Repair windows of this channel whose five-attempt budget has been spent, which is the point
  // past which a position can no longer be recovered by asking again.
  private val replayBudgetsSpent = new AtomicLong(0L)

  /**
   * Emits one report through an executor-scoped window, or accounts it and traces it under the
   * debug key.
   *
   * @param aggregator the executor-scoped window that decides, named by the caller so the scope
   *     is visible at the report rather than only at the declaration
   * @param total the running total for this channel, advanced here so a caller cannot forget to
   * @param unit the noun the per-channel figure is counted in, so one helper can serve
   *     conditions that are counted in repairs, frames and channels alike
   * @param entry the report, built only when it is going to be emitted or traced
   * @param cause the failure to attach, or `null` when the report is not about one
   */
  private def reportBounded(
      aggregator: MemorySpillManager.ExecutorLogAggregator,
      total: AtomicLong,
      unit: String,
      entry: => MessageWithContext,
      cause: Throwable = null): Unit = {
    val occurrences = total.incrementAndGet()
    aggregator.record(clock.getTimeMillis()) match {
      case Some(summary) =>
        val message = entry +
          log" (${MDC(NUM_EVENTS, occurrences)} ${MDC(REASON, unit)} on this channel, " +
          log"${MDC(COUNT, summary.occurrences)} on this executor, " +
          log"${MDC(NUM_SKIPPED, summary.unreported)} not reported individually)"
        if (cause == null) logWarning(message) else logWarning(message, cause)
      case None =>
        if (debugEnabled) {
          if (cause == null) logDebug(entry) else logDebug(entry, cause)
        }
    }
  }

  /** Emits one repair report through the executor's repair window. */
  private def reportRepair(entry: => MessageWithContext): Unit =
    reportBounded(StreamingShuffleClientHandler.repairLogAggregator, repairsRequested,
      "repair(s)", entry)

  // Transport tuning for streaming shuffle alone, built by the SAME function the producer side
  // uses.
  private val transportConf: TransportConf =
    StreamingShuffleServerHandler.streamingTransportConf(conf)

  // Replay budget for a corrupt or missing block inside the retained window, taken from the
  // streaming module's own retry knobs rather than from a constant of this file's invention.
  private val maxIoRetries: Int = transportConf.maxIORetries()

  private val ioRetryWaitMillis: Int = transportConf.ioRetryWaitTimeMs()

  // OS-level keepalive removes a connection that has been idle too long, but the transport exposes
  // it as a boolean with no interval and the JDK exposes no keepalive-interval socket option, so it
  // cannot be the five-second detector this subsystem promises.
  private val tcpKeepAliveEnabled: Boolean = transportConf.enableTcpKeepAlive()

  // The hand-off queue between the event loop and the task thread.
  private val inbound = new LinkedBlockingQueue[Inbound](INBOUND_QUEUE_CAPACITY)

  // Payload bytes currently sitting in that queue.
  private val queuedPayloadBytes = new AtomicLong(0L)

  // Per-partition bookkeeping.
  private val partitions = new ConcurrentHashMap[Integer, PartitionState]()

  // The channel this handler is attached to, captured as early as Netty offers it so that the task
  // thread can acknowledge, request a replay and close without having to be handed a context.
  private val channelRef = new AtomicReference[Channel](null)

  // The one monitor under which this handler's participation in a receive window is transitioned:
  // entering and leaving a throttling episode, moving between gates, and departing on close.
  private val gateLock = new Object

  // Whether this handler has left its gate for good.
  private var gateDeparted = false

  // The receive window of the physical channel, shared with every other handler multiplexed onto
  // it.
  private val readGate =
    new AtomicReference[StreamingShuffleChannelReadGate](new StreamingShuffleChannelReadGate)

  // Transitions into the throttled state observed on this channel.
  private val throttleTransitions = new AtomicLong(0L)

  private val throttleCause = new AtomicReference[String](null)

  // Instant at which this channel went inactive with work outstanding, read from the injected
  // clock.
  private val producerLostAtMillis = new AtomicLong(NO_TIMESTAMP)

  // Failures escalated from an I/O thread to the task thread on this channel.
  private val escalationsReported = new AtomicLong(0L)

  // Admissions that have charged the executor's shared receive budget but have not yet finished
  // recording ownership of it.
  private val admissionsInFlight = new AtomicInteger(0)

  private val dataPlaneTasksInFlight = new AtomicInteger(0)

  private val lifecycleLock = new ReentrantLock()

  private val lifecycleChanged = lifecycleLock.newCondition()

  private val dataPlaneRefusals = new AtomicLong(0L)

  /** Bridge from the wire decoder to the executor-wide aggregate allowance. */
  private val payloadReservation = new StreamingShuffleMessage.PayloadReservation {
    override def tryReserve(
        frameShuffleId: Int,
        frameMapId: Long,
        framePartitionId: Int,
        frameSequenceNumber: Long,
        payloadBytes: Int): Boolean = {
      backpressure.tryReserveReceiveQuota(payloadBytes.toLong)
    }

    override def release(payloadBytes: Int): Unit =
      backpressure.releaseReceiveQuota(payloadBytes.toLong)
  }

  // Admissions whose charge was returned because this handler closed while they were in flight.
  private val admissionsRolledBack = new AtomicLong(0L)

  // Netty identity of the one channel this handler is bound to, latched on the first callback the
  // transport makes and never replaced.
  private val boundChannelId = new AtomicReference[String](null)

  // Principal established by Spark's transport authentication for the bound channel.
  private val boundTransportPrincipal = new AtomicReference[String](null)

  // Callbacks refused because Spark authentication established no principal for their channel.
  private val unauthenticatedCallbacks = new AtomicLong(0L)

  // One-shot warning for unauthenticated callbacks; the task notifier retains the actual failure.
  private val unauthenticatedReported = new AtomicBoolean(false)

  // Callbacks refused because they arrived on a channel other than the bound one.
  private val foreignChannelCallbacks = new AtomicLong(0L)

  // Frames dropped because they named a shuffle or a partition this handler does not serve.
  private val misaddressedFrames = new AtomicLong(0L)

  // Control frames refused because they only ever travel from consumer to producer.
  private val wrongDirectionFrames = new AtomicLong(0L)

  // Protocol revision of the first frame this build could not read, or NO_OBSERVED_VERSION.
  private val observedIncompatibleVersion = new AtomicInteger(NO_OBSERVED_VERSION)

  // Latch for the channel-level producer-loss marker, which belongs to no single partition.
  private val channelLostSignalled = new AtomicBoolean(false)

  private val closed = new AtomicBoolean(false)

  // Transport callbacks.

  /**
   * A consumer channel serves no chunked stream, so the stream manager offered here is an empty
   * one.
   */
  private val streamManager = new OneForOneStreamManager()

  override def getStreamManager(): StreamManager = streamManager

  /**
   * Whether a frame names the producer this handler serves.
   *
   * @param message the frame, whose position is left untouched
   * @return true when the frame's header names this handler's shuffle and map output
   */
  def routes(message: ByteBuffer): Boolean = {
    try {
      StreamingShuffleMessage.peekShuffleId(message) == shuffleId &&
        StreamingShuffleMessage.peekMapId(message) == mapId
    } catch {
      case NonFatal(_) => false
    }
  }

  /** Consumes one streaming frame that arrived as a one-way message. */
  override def receive(client: TransportClient, message: ByteBuffer): Unit = {
    StreamingShuffleServerHandler.authenticatedPrincipal(client) match {
      case Some(principal) => guard(consumeFrame(client, message, principal))
      case None => rejectUnauthenticatedChannel(client)
    }
  }

  /** Consumes one streaming frame that arrived as a request expecting a reply. */
  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    StreamingShuffleServerHandler.authenticatedPrincipal(client) match {
      case Some(principal) =>
        guard(consumeFrame(client, message, principal))
        callback.onSuccess(ByteBuffer.allocate(0))
      case None =>
        callback.onFailure(rejectUnauthenticatedChannel(client))
    }
  }

  /** Binds the channel and announces this consumer's subscription on it. */
  override def channelActive(client: TransportClient): Unit = {
    guard {
      StreamingShuffleServerHandler.authenticatedPrincipal(client) match {
        case Some(principal) =>
          if (bindChannel(client, principal)) {
            submitDataPlaneOperation(client, "a channel-activation transition") {
              activateChannel(client)
            }
          } else {
            rejectForeignChannel(client)
          }
        case None =>
          rejectUnauthenticatedChannel(client)
      }
    }
  }

  /** Announces this consumer on the channel this handler has just been bound to. */
  private def activateChannel(client: TransportClient): Unit = {
    producerLostAtMillis.set(NO_TIMESTAMP)
    val announced = announceSubscription()
    if (debugEnabled) {
      logDebug(log"Streaming shuffle consumer channel for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} is active against " +
        log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))}, subscribed " +
        log"${MDC(COUNT, announced)} partition(s) with OS keepalive " +
        log"${MDC(CONFIG, String.valueOf(tcpKeepAliveEnabled))} and a transport replay budget " +
        log"of ${MDC(MAX_ATTEMPTS, maxIoRetries)} attempt(s) " +
        log"${MDC(TIMEOUT, ioRetryWaitMillis)} ms apart")
    }
  }

  /** Signals producer loss. */
  override def channelInactive(client: TransportClient): Unit = {
    guard {
      if (isBoundChannel(client)) {
        submitDataPlaneOperation(client, "an inactive-channel transition") {
          deactivateChannel(client)
        }
      } else {
        rejectForeignChannel(client)
      }
    }
  }

  /** Reports the loss of the bound channel to every stream that was still owed data on it. */
  private def deactivateChannel(client: TransportClient): Unit = {
    val outstanding = partitionsAwaitingData
    if (outstanding.isEmpty) {
      if (debugEnabled) {
        logDebug(log"Streaming shuffle consumer channel for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} closed with every stream already terminated")
      }
    } else {
      producerLostAtMillis.compareAndSet(NO_TIMESTAMP, clock.getTimeMillis())
      // Under the debug key, for the reason set out on [[escalate]]: this is one producer loss seen
      // from the I/O thread, and the reader reports the same loss at default level with what it
      // cost and what follows.
      if (debugEnabled) {
        logDebug(log"Streaming shuffle producer for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} closed the connection from " +
          log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))} with " +
          log"${MDC(COUNT, outstanding.size)} stream(s) still awaiting data")
      }
      // Prompting the protocol is the whole of the hand-off available: it owns the five-second
      // window, judges it from its own clock and re-evaluates every ledger in one non-blocking pass
      // that takes no lock and performs no I/O. This callback reaches it on the ordered data-plane
      // worker, never on the Netty event loop.
      backpressure.pollOnce()
      // No cause: the peer closed the connection rather than raising anything, so there is no
      // transport-level throwable to carry and the reason string is the whole of what is known.
      outstanding.foreach(partitionId =>
        signalProducerLost(partitionId, PRODUCER_CLOSED_REASON, null))
    }
  }

  /** Records a channel-level failure and closes the channel. */
  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    guard {
      if (isBoundChannel(client)) {
        submitDataPlaneOperation(client, "a channel-failure transition") {
          escalate(cause, UNKNOWN_PARTITION_ID)
          client.close()
        }
      } else {
        // Closing is still correct -- a channel nobody owns should not be left open -- but the
        // failure belongs to whoever owns that channel and must not reach this task's notifier,
        // where it would fail a reduce task over a socket it never read from.
        rejectForeignChannel(client)
        client.close()
      }
    }
  }

  /** Decodes and routes one frame, then re-evaluates everything arrival may have changed. */
  private def consumeFrame(
      client: TransportClient,
      frame: ByteBuffer,
      principal: String): Unit = {
    if (!bindChannel(client, principal)) {
      // Refused before a single byte is decoded.
      rejectForeignChannel(client)
      return
    }
    var partitionId = UNKNOWN_PARTITION_ID
    var decoded: StreamingShuffleMessage = null
    var submitted = false
    try {
      decoded = decodeFramed(frame)
      partitionId = decoded.partitionId()
      if (validateInboundAddress(decoded)) {
        submitted = submitDecodedFrame(client, decoded)
      }
    } catch {
      case refused: StreamingShuffleMessage.PayloadReservationRejectedException =>
        partitionId = refused.partitionId()
        handlePayloadReservationRefusal(refused)
      // A failure must never escape into the transport: it would tear the channel down instead of
      // failing the task cleanly.
      case NonFatal(e) =>
        escalate(e, partitionId)
    } finally {
      if (!submitted) {
        decoded match {
          case block: DataBlockMessage => block.releasePayloadReservation()
          case _ => ()
        }
      }
    }
    if (!submitted) {
      flushDeferredAcks()
      evaluateAutoRead()
    }
  }

  /** Validates frame addressing and protocol version before worker processing is enqueued. */
  private def validateInboundAddress(message: StreamingShuffleMessage): Boolean = {
    if (message.shuffleId() != shuffleId || message.mapId() != mapId ||
        !servesPartition(message.partitionId())) {
      rejectMisaddressed(message)
      false
    } else {
      if (!backpressure.observeProtocolVersion(
          consumerKey(message.partitionId()), message.protocolVersion())) {
        StreamingShuffleMessage.checkProtocolVersion(message.protocolVersion())
      }
      true
    }
  }

  /** Transfers one decoded frame to the executor-wide bounded data-plane workers. */
  private def submitDecodedFrame(
      client: TransportClient,
      message: StreamingShuffleMessage): Boolean = {
    dataPlaneTasksInFlight.incrementAndGet()
    val accepted = backpressure.executeDataPlane(this, new Runnable {
      override def run(): Unit = {
        try {
          if (!closed.get()) {
            dispatch(message)
            flushDeferredAcks()
            evaluateAutoRead()
          }
        } catch {
          case NonFatal(e) =>
            escalate(e, message.partitionId())
        } finally {
          message match {
            case block: DataBlockMessage => block.releasePayloadReservation()
            case _ => ()
          }
          dataPlaneTaskSettled()
        }
      }
    })
    if (!accepted) {
      dataPlaneTaskSettled()
      dataPlaneRefusals.incrementAndGet()
      val failure = new IllegalStateException(
        s"Streaming shuffle consumer of shuffle $shuffleId map $mapId could not enqueue a frame " +
          "because the executor-wide data-plane worker queue is full.")
      escalate(failure, message.partitionId())
      client.close()
    }
    accepted
  }

  /** Submits a non-frame channel transition to the same ordered worker stripe as decoded frames. */
  private def submitDataPlaneOperation(
      client: TransportClient,
      description: String)(operation: => Unit): Boolean = {
    dataPlaneTasksInFlight.incrementAndGet()
    val accepted = backpressure.executeDataPlane(this, new Runnable {
      override def run(): Unit = {
        try {
          if (!closed.get()) {
            operation
          }
        } catch {
          case NonFatal(e) =>
            escalate(e, UNKNOWN_PARTITION_ID)
        } finally {
          dataPlaneTaskSettled()
        }
      }
    })
    if (!accepted) {
      dataPlaneTaskSettled()
      dataPlaneRefusals.incrementAndGet()
      val failure = new IllegalStateException(
        s"Streaming shuffle consumer of shuffle $shuffleId map $mapId could not enqueue " +
          s"$description because the executor-wide data-plane worker queue is full.")
      escalate(failure, UNKNOWN_PARTITION_ID)
      client.close()
    }
    accepted
  }

  /** Records one accepted frame task as settled and wakes bounded lifecycle waits. */
  private def dataPlaneTaskSettled(): Unit = {
    dataPlaneTasksInFlight.decrementAndGet()
    signalLifecycleChange()
  }

  private def signalLifecycleChange(): Unit = {
    lifecycleLock.lock()
    try {
      lifecycleChanged.signalAll()
    } finally {
      lifecycleLock.unlock()
    }
  }

  /** Converts a pre-allocation quota refusal into the same bounded replay used after decode. */
  private def handlePayloadReservationRefusal(
      refused: StreamingShuffleMessage.PayloadReservationRejectedException): Unit = {
    if (refused.shuffleId() != shuffleId || refused.mapId() != mapId ||
        !servesPartition(refused.partitionId())) {
      misaddressedFrames.incrementAndGet()
    } else {
      val state = partitionStateOf(refused.partitionId())
      state.lastInboundMillis.set(clock.getTimeMillis())
      backpressure.onDiscardedData(
        consumerKey(refused.partitionId()), refused.payloadBytes().toLong)
      refuseForQuota(state, refused.sequenceNumber(), refused.payloadBytes().toLong)
    }
  }

  /**
   * Binds this handler to one channel, once, and reports whether the caller is that channel.
   *
   * @param client the client the transport made this callback for
   * @param principal identity Spark authentication established for the client
   * @return true when this handler is bound to that client and principal
   */
  private def bindChannel(client: TransportClient, principal: String): Boolean = {
    val principalMatches =
      boundTransportPrincipal.compareAndSet(null, principal) ||
        boundTransportPrincipal.get() == principal
    val channel = client.getChannel()
    if (!principalMatches) {
      false
    } else if (channel == null) {
      true
    } else {
      val observed = channel.id().asLongText()
      if (boundChannelId.compareAndSet(null, observed) || boundChannelId.get() == observed) {
        channelRef.set(channel)
        // The gate governs the socket rather than this handler, so it has to learn about the socket
        // as soon as this handler does -- otherwise a throttle stated on the first frame of a
        // channel would be recorded and never applied.
        readGate.get().attach(channel)
        true
      } else {
        false
      }
    }
  }

  /** Whether a client is the one this handler is bound to, without binding it. */
  private def isBoundChannel(client: TransportClient): Boolean = {
    val channel = client.getChannel()
    if (channel == null) {
      true
    } else {
      val bound = boundChannelId.get()
      bound == null || bound == channel.id().asLongText()
    }
  }

  /** Counts and reports one callback refused because it arrived on a foreign channel. */
  private def rejectForeignChannel(client: TransportClient): Unit = {
    reportBounded(StreamingShuffleClientHandler.foreignChannelLogAggregator,
      foreignChannelCallbacks, "refused callback(s)",
      log"Streaming shuffle consumer for shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
        log"${MDC(MAP_ID, mapId)} refused a transport callback from " +
        log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))} because it arrived on a " +
        log"channel this handler is not bound to")
  }

  /**
   * Rejects a callback before decode when Spark authentication established no channel identity.
   *
   * @return the failure reported to a request-shaped transport callback
   */
  private def rejectUnauthenticatedChannel(client: TransportClient): SecurityException = {
    val failure = new SecurityException(
      s"Streaming shuffle $shuffleId map $mapId requires an authenticated transport channel.")
    val occurrence = unauthenticatedCallbacks.incrementAndGet()
    if (unauthenticatedReported.compareAndSet(false, true)) {
      logWarning(log"Streaming shuffle consumer for shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
        log"${MDC(MAP_ID, mapId)} refused a callback from " +
        log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))} before decode because " +
        log"the channel did not complete Spark authentication. Further occurrences are counted " +
        log"but not logged")
    }
    if (occurrence == 1L) {
      escalate(failure, UNKNOWN_PARTITION_ID)
    }
    client.close()
    failure
  }

  /**
   * Announces this consumer on the channel, one heartbeat per partition in the requested range.
   *
   * @return how many partitions were announced
   */
  private def announceSubscription(): Int = {
    var announced = 0
    var partitionId = startPartition
    while (partitionId < endPartition) {
      val state = partitionStateOf(partitionId)
      if (writeHeartbeat(partitionId, buildHeartbeat(state))) {
        announced += 1
      }
      partitionId += 1
    }
    announced
  }

  /** Runs one transport callback, absorbing anything it raises. */
  private def guard(operation: => Any): Unit = {
    try {
      operation
    } catch {
      case NonFatal(e) =>
        escalate(e, UNKNOWN_PARTITION_ID)
    }
  }

  // The TCP-level backpressure layer: autoRead.

  /**
   * Re-evaluates whether this channel should be reading from its socket, and applies the answer.
   */
  private def evaluateAutoRead(): Unit = {
    if (!closed.get()) {
      if (isAutoReadEnabled) {
        val cause = throttleCauseNow()
        if (cause != null) {
          disableAutoRead(cause)
        }
      } else if (resumeAllowed()) {
        enableAutoRead()
      }
    }
  }

  /** The reason to stop reading right now, or null when there is none. */
  private def throttleCauseNow(): String = {
    if (isAnyStreamCreditExhausted) {
      THROTTLE_CAUSE_CREDIT
    } else if (backpressure.receiveQuotaExhausted) {
      // Tested here as well as at admission, so an exhausted executor budget stops the producer at
      // the socket instead of being discovered one refused block at a time.
      THROTTLE_CAUSE_EXECUTOR_QUOTA
    } else if (inbound.size() >= INBOUND_QUEUE_HIGH_WATER_MARK ||
        queuedPayloadBytes.get() >= INBOUND_HIGH_WATER_BYTES) {
      THROTTLE_CAUSE_QUEUE
    } else {
      null
    }
  }

  /** Whether both throttling conditions have cleared with margin to spare. */
  private def resumeAllowed(): Boolean = {
    inbound.size() <= INBOUND_QUEUE_LOW_WATER_MARK &&
      queuedPayloadBytes.get() <= INBOUND_LOW_WATER_BYTES &&
      !isAnyStreamCreditExhausted &&
      !backpressure.receiveQuotaExhausted
  }

  /** Stops reading from the socket, once per episode. */
  private def disableAutoRead(cause: String): Unit = {
    // The intent record and the socket update are one transition, taken under this handler's own
    // monitor so that a concurrent resume cannot land between reading the record and stating the
    // new intent.
    val transitioned = gateLock.synchronized {
      val gate = readGate.get()
      if (gateDeparted || gate.isThrottling(this)) {
        false
      } else {
        throttleCause.set(cause)
        gate.throttle(this, cause)
        true
      }
    }
    if (transitioned) {
      throttleTransitions.incrementAndGet()
      backpressure.pollOnce()
      // Reported after the poll, so a stale episode is closed before this one is opened.
      partitions.keySet().asScala.foreach { partitionId =>
        backpressure.noteThrottled(consumerKey(partitionId), cause)
      }
      if (debugEnabled) {
        logDebug(log"Streaming shuffle consumer stopped reading for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} because ${MDC(REASON, cause)}, holding " +
          log"${MDC(COUNT, inbound.size())} block(s) and " +
          log"${MDC(NUM_BYTES, queuedPayloadBytes.get())} queued byte(s)")
      }
    }
  }

  /** Resumes reading from the socket, once per episode, under the same compare-and-set guard. */
  private def enableAutoRead(): Unit = {
    val transitioned = gateLock.synchronized {
      val gate = readGate.get()
      if (gateDeparted || !gate.isThrottling(this)) {
        false
      } else {
        throttleCause.set(null)
        gate.resume(this)
        true
      }
    }
    if (transitioned) {
      partitions.keySet().asScala.foreach { partitionId =>
        backpressure.noteResumed(consumerKey(partitionId))
      }
      if (debugEnabled) {
        logDebug(log"Streaming shuffle consumer resumed reading for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} with ${MDC(COUNT, inbound.size())} block(s) and " +
          log"${MDC(NUM_BYTES, queuedPayloadBytes.get())} queued byte(s)")
      }
    }
  }

  /**
   * Binds this handler to the receive window of the channel it now shares.
   *
   * @param gate the receive window of the physical channel this handler is multiplexed onto
   */
  private[streaming] def joinReadGate(gate: StreamingShuffleChannelReadGate): Unit = {
    if (gate != null) {
      // Under the same monitor as an ordinary transition, and for the same reason: the intent is
      // read from the gate being left and re-stated on the gate being joined, and a throttle or
      // resume landing between those two steps would either be applied to a gate this handler has
      // already abandoned or be lost altogether.
      gateLock.synchronized {
        if (!gateDeparted) {
          val previous = readGate.getAndSet(gate)
          val wasThrottling = previous.isThrottling(this)
          if (previous ne gate) {
            // Withdrawn from the gate being left, or its throttle would outlive this handler's
            // participation in it -- which for a private gate costs nothing and for a shared one
            // would wedge a window shut with no participant able to clear it.
            previous.withdraw(this)
          }
          val channel = channelRef.get()
          if (channel != null) {
            gate.attach(channel)
          }
          if (wasThrottling) {
            gate.throttle(this, Option(throttleCause.get()).getOrElse(THROTTLE_CAUSE_QUEUE))
          }
        }
      }
    }
  }

  /** The receive window this handler currently participates in. */
  private[streaming] def currentReadGate: StreamingShuffleChannelReadGate = readGate.get()

  /** Whether any stream still expecting data has spent its credit. */
  private def isAnyStreamCreditExhausted: Boolean = {
    var exhausted = false
    val states = partitions.values().iterator()
    while (states.hasNext && !exhausted) {
      val state = states.next()
      if (!state.terminated.get() && isCreditExhausted(state)) {
        exhausted = true
      }
    }
    exhausted
  }

  /** Whether one stream has spent its credit, asked in both of the ways available. */
  private def isCreditExhausted(state: PartitionState): Boolean = {
    val limit = backpressure.creditLimitBytes(consumerKey(state.partitionId))
    val outstanding = state.outstandingBytes
    !backpressure.hasCredit(consumerKey(state.partitionId)) ||
      (limit > 0L && outstanding >= limit)
  }

  // Decoding and dispatch.

  /** Declines one block because this executor's shared consumer budget is fully committed. */
  private def refuseForQuota(
      state: PartitionState,
      sequenceNumber: Long,
      payloadBytes: Long): Unit = {
    state.quotaRefusals.incrementAndGet()
    if (!state.quarantine(sequenceNumber, sequenceNumber)) {
      escalate(
        new IllegalStateException(s"The streaming shuffle consumer of shuffle $shuffleId " +
          s"partition ${state.partitionId} refused block $sequenceNumber of $payloadBytes " +
          s"byte(s) because this executor's shared receive budget of " +
          s"${backpressure.receiveQuotaBytes} byte(s) is fully committed, and could not " +
          "record the position for replay"),
        state.partitionId)
    } else if (debugEnabled) {
      logDebug(log"Streaming shuffle refused block " +
        log"${MDC(BLOCK_ID, blockIdOf(state.partitionId, sequenceNumber))} of " +
        log"${MDC(NUM_BYTES, payloadBytes)} byte(s) because the executor's shared receive budget " +
        log"of ${MDC(THRESHOLD, backpressure.receiveQuotaBytes)} byte(s) is fully committed; the " +
        log"position is quarantined and will be requested again")
    }
  }

  /** Settles the compatibility question before committing to a decode, then decodes. */
  private def decodeFramed(framed: ByteBuffer): StreamingShuffleMessage = {
    val version = StreamingShuffleMessage.peekProtocolVersion(framed)
    if (!StreamingShuffleMessage.isCompatible(version)) {
      observedIncompatibleVersion.compareAndSet(NO_OBSERVED_VERSION, version.toInt)
      backpressure.observeProtocolVersion(consumerKey(startPartition), version)
      StreamingShuffleMessage.checkProtocolVersion(version)
    }
    StreamingShuffleMessage.Decoder.fromByteBuffer(framed, payloadReservation)
  }

  /**
   * The protocol revision of the first frame this build could not read, or `None` if every frame so
   * far has been legible.
   */
  def incompatibleProtocolVersion: Option[Byte] = {
    val observed = observedIncompatibleVersion.get()
    if (observed == NO_OBSERVED_VERSION) None else Some(observed.toByte)
  }

  /** Routes one decoded message, after proving it is addressed to a stream this task reads. */
  private def dispatch(message: StreamingShuffleMessage): Unit = {
    message match {
      case block: DataBlockMessage =>
        handleDataBlock(block)
      case heartbeat: HeartbeatMessage =>
        handleHeartbeat(heartbeat)
      case termination: StreamTerminationMessage =>
        handleTermination(termination)
      case ack: AckMessage =>
        refuseOutboundOnlyMessage(ack, StreamingShuffleMessageType.ACK)
      case request: RetransmitRequestMessage =>
        refuseOutboundOnlyMessage(request, StreamingShuffleMessageType.RETRANSMIT_REQUEST)
      case other =>
        escalate(
          StreamingShuffleErrors.unexpectedMessageType(
            STREAMING_MESSAGE_DESCRIPTION, describeClassOf(other)),
          other.partitionId())
    }
  }

  /** Whether a partition id names a stream this reduce task actually reads. */
  private def servesPartition(partitionId: Int): Boolean =
    partitionId >= startPartition && partitionId < endPartition

  /** Counts and drops a frame addressed to a stream this handler does not serve. */
  private def rejectMisaddressed(message: StreamingShuffleMessage): Unit = {
    reportBounded(StreamingShuffleClientHandler.misaddressedFrameLogAggregator,
      misaddressedFrames, "dropped frame(s)",
      log"Streaming shuffle consumer for shuffle ${MDC(SHUFFLE_ID, shuffleId)} " +
        log"partitions [${MDC(COUNT, startPartition)}, ${MDC(THRESHOLD, endPartition)}) dropped " +
        log"a frame addressed to shuffle ${MDC(VALUE, message.shuffleId())} map " +
        log"${MDC(MAP_ID, message.mapId())} partition " +
        log"${MDC(PARTITION_ID, message.partitionId())}")
  }

  /** Accepts a data block, or repairs it. */
  private def handleDataBlock(block: DataBlockMessage): Unit = {
    val partitionId = block.partitionId()
    val sequenceNumber = block.sequenceNumber()
    val state = partitionStateOf(partitionId)

    state.lastInboundMillis.set(clock.getTimeMillis())

    val expected = state.expectedSequenceNumber.get()
    val payloadLength = block.payloadLength().toLong
    // The quarantine is read once and both classifications derived from that one reading, because
    // the task thread quarantines positions while this thread is deciding: two reads could call the
    // same frame a duplicate in one breath and an awaited repair in the next.
    val awaitedRepair = sequenceNumber < expected && state.isQuarantined(sequenceNumber)
    val staleDuplicate = sequenceNumber < expected && !awaitedRepair

    if (state.terminated.get()) {
      // A stream that has ended is still reachable by three different frames, and they are not the
      // same event.
      if (staleDuplicate) {
        discardDuplicateBlock(state, sequenceNumber, expected, payloadLength)
      } else if (awaitedRepair && isAwaitedRepairAfterTermination(state, sequenceNumber)) {
        state.repairsAfterTermination.incrementAndGet()
        reportRepair(log"Streaming shuffle accepted the replacement for position " +
          log"${MDC(BLOCK_ID, blockIdOf(partitionId, sequenceNumber))} of shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} after that stream had ended at " +
          log"${MDC(COUNT, state.announcedBlocks.get())} block(s); the repair was asked for by " +
          log"this consumer and lies inside the total the producer committed to")
        acceptInboundBlock(state, block, expected, payloadLength)
      } else {
        rejectBlockAfterTermination(state, sequenceNumber)
      }
      return
    }

    if (staleDuplicate) {
      discardDuplicateBlock(state, sequenceNumber, expected, payloadLength)
    } else {
      val ledgerRecorded =
        backpressure.onDataReceived(consumerKey(partitionId), sequenceNumber, payloadLength)
      if (!ledgerRecorded) {
        refuseForQuota(state, sequenceNumber, payloadLength)
      } else if (sequenceNumber > expected) {
        repairSequenceGap(state, expected, sequenceNumber)
      } else if (!block.verifyChecksum()) {
        repairCorruptBlock(state, block)
      } else {
        admitBlock(state, block)
      }
    }
  }

  /**
   * Charges an inbound block to the stream and then admits, repairs or replays it.
   *
   * @param state the partition the block belongs to
   * @param block the block that arrived
   * @param expected the frontier the block is judged against, sampled by the caller
   * @param payloadBytes the payload the block carried
   */
  private def acceptInboundBlock(
      state: PartitionState,
      block: DataBlockMessage,
      expected: Long,
      payloadBytes: Long): Unit = {
    val sequenceNumber = block.sequenceNumber()
    backpressure.onDataReceived(consumerKey(state.partitionId), sequenceNumber, payloadBytes)
    if (sequenceNumber > expected) {
      repairSequenceGap(state, expected, sequenceNumber)
    } else if (!block.verifyChecksum()) {
      repairCorruptBlock(state, block)
    } else {
      admitBlock(state, block)
    }
  }

  /**
   * Whether a block arriving after the terminator is the in-window repair this consumer asked for.
   *
   * @param state the partition whose stream has ended
   * @param sequenceNumber the position the late block carried
   * @return true when the block may be admitted as a repair
   */
  private def isAwaitedRepairAfterTermination(
      state: PartitionState,
      sequenceNumber: Long): Boolean = {
    val announced = state.announcedBlocks.get()
    announced != NO_BLOCK_TOTAL && sequenceNumber < announced
  }

  /**
   * Discards a block for a position this consumer has already accepted.
   *
   * @param state the partition the block belongs to
   * @param sequenceNumber the position the duplicate carried
   * @param expected the frontier the duplicate is judged against
   * @param payloadBytes the payload the duplicate carried
   */
  private def discardDuplicateBlock(
      state: PartitionState,
      sequenceNumber: Long,
      expected: Long,
      payloadBytes: Long): Unit = {
    // Liveness and the link bytes, but not the window charge and not the stream's received total.
    backpressure.onDiscardedData(consumerKey(state.partitionId), payloadBytes)
    if (debugEnabled) {
      logDebug(log"Streaming shuffle discarded a duplicate block " +
        log"${MDC(BLOCK_ID, blockIdOf(state.partitionId, sequenceNumber))} of shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} because position " +
        log"${MDC(COUNT, expected)} has already been accepted")
    }
    state.duplicateBlocks.incrementAndGet()
  }

  /** Admits one verified block: delivers it, then commits the bookkeeping that delivery earns. */
  private def admitBlock(state: PartitionState, block: DataBlockMessage): Unit = {
    val partitionId = state.partitionId
    val sequenceNumber = block.sequenceNumber()
    val payloadLength = block.payloadLength().toLong
    val decoderReservation = block.hasPayloadReservation()
    // Sampled before the quarantine is released, so the frontier this admission is judged against
    // is the one that was in force when the block arrived rather than one a concurrent admission
    // moved.
    val expectedAtAdmission = state.expectedSequenceNumber.get()
    val replacement = state.releaseQuarantine(sequenceNumber)
    // Production decode already reserved this payload before allocating it.
    if (!decoderReservation && !backpressure.tryReserveReceiveQuota(payloadLength)) {
      refuseForQuota(state, sequenceNumber, payloadLength)
      return
    }
    // From here to the matching decrement this admission is *in flight*, and [[close]] can see that
    // it is.
    admissionsInFlight.incrementAndGet()
    try {
      if (enqueue(BlockReceived(block), payloadLength)) {
        // Re-checked after the hand-off succeeded, because closure can have been decided between
        // the check inside enqueue() and this point.
        if (closed.get()) {
          rollBackAdmission(payloadLength, releaseQuota = !decoderReservation)
          return
        }
        // Per-sequence accounting, so that an acknowledgement releases exactly the bytes of the
        // prefix it names and never the whole of what has arrived.
        val superseded = state.recordReceived(sequenceNumber, payloadLength)
        if (superseded > 0L) {
          backpressure.releaseReceiveQuota(superseded)
        }
        if (decoderReservation && !block.transferPayloadReservation()) {
          throw new IllegalStateException(
            s"Streaming shuffle block $sequenceNumber of partition $partitionId lost its " +
              "pre-allocation quota reservation before consumer accounting adopted it")
        }
        // The cursor advances exactly when this block occupies or extends the frontier, which is a
        // stricter statement than "this block was not a replacement" and a necessary one.
        if (sequenceNumber >= expectedAtAdmission) {
          state.expectedSequenceNumber.set(sequenceNumber + 1L)
          state.highestReceivedSequenceNumber.set(sequenceNumber)
        }
        state.acceptedBlocks.incrementAndGet()
        // An admission is the only event that can advance the position a held end-of-stream is
        // waiting for, so this is where a deferred terminator is applied.
        applyDeferredTermination(state)
        if (debugEnabled) {
          logDebug(log"Streaming shuffle accepted block " +
            log"${MDC(BLOCK_ID, blockIdOf(partitionId, sequenceNumber))} of " +
            log"${MDC(NUM_BYTES, payloadLength)} byte(s) for shuffle " +
            log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, partitionId)}, " +
            log"replacement: ${MDC(REASON, replacement)}")
        }
      } else {
        // The hand-off failed, so nothing is holding the bytes and the charge must come back.
        if (!decoderReservation) {
          backpressure.releaseReceiveQuota(payloadLength)
        }
        if (replacement) {
          // The quarantine must stand: something has to ask for this position again, and the
          // escalation enqueue() has already recorded is what fails the task if nothing can.
          state.quarantine(sequenceNumber, sequenceNumber)
        }
      }
    } finally {
      admissionsInFlight.decrementAndGet()
      signalLifecycleChange()
    }
  }

  /**
   * Returns the charge of an admission that a concurrent closure made worthless.
   *
   * @param payloadBytes the bytes this admission reserved
   */
  private def rollBackAdmission(payloadBytes: Long, releaseQuota: Boolean): Unit = {
    admissionsRolledBack.incrementAndGet()
    drainAndRelease()
    if (releaseQuota && payloadBytes > 0L) {
      backpressure.releaseReceiveQuota(payloadBytes)
    }
    if (debugEnabled) {
      logDebug(log"Streaming shuffle returned ${MDC(NUM_BYTES, payloadBytes)} byte(s) of the " +
        log"executor's shared receive budget for shuffle ${MDC(SHUFFLE_ID, shuffleId)} because " +
        log"the consumer handler closed while the block was being admitted")
    }
  }

  /** Repairs, or escalates, a gap in a partition's block sequence. */
  private def repairSequenceGap(state: PartitionState, expected: Long, actual: Long): Unit = {
    val failure =
      StreamingShuffleErrors.invalidSequenceNumber(shuffleId, state.partitionId, expected, actual)
    if (requestRetransmission(state.partitionId, expected, actual)) {
      state.repairedGaps.incrementAndGet()
      reportRepair(log"Streaming shuffle has shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, state.partitionId)} replaying position(s) " +
        log"${MDC(COUNT, expected)} through ${MDC(THRESHOLD, actual)} after a gap in the block " +
        log"sequence")
    } else {
      escalate(failure, state.partitionId)
    }
  }

  /** Repairs, or escalates, a block that failed verification. */
  private def repairCorruptBlock(state: PartitionState, block: DataBlockMessage): Unit = {
    val partitionId = state.partitionId
    val sequenceNumber = block.sequenceNumber()
    val blockId = blockIdOf(partitionId, sequenceNumber)
    val expectedChecksum = block.checksum()
    val computedChecksum = block.computedChecksum()
    val failure = StreamingShuffleErrors.checksumVerificationFailed(
      blockId, shuffleId, expectedChecksum, computedChecksum)
    state.corruptBlocks.incrementAndGet()
    if (requestRetransmission(partitionId, sequenceNumber, sequenceNumber)) {
      reportRepair(log"Streaming shuffle block ${MDC(BLOCK_ID, blockId)} of shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} failed verification with checksum " +
        log"${MDC(CHECKSUM, computedChecksum)} against an expected " +
        log"${MDC(THRESHOLD, expectedChecksum)} and a replay has been requested")
    } else {
      logWarning(log"Streaming shuffle block ${MDC(BLOCK_ID, blockId)} of shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} failed verification and can no longer be replayed, so " +
        log"the fetch must fail and the upstream stage be recomputed")
      escalate(failure, partitionId)
    }
  }

  /** Records a producer heartbeat. */
  private def handleHeartbeat(heartbeat: HeartbeatMessage): Unit = {
    val state = partitionStateOf(heartbeat.partitionId())
    val arrivedAtMillis = clock.getTimeMillis()
    state.lastInboundMillis.set(arrivedAtMillis)
    state.remoteHeartbeatMillis.set(arrivedAtMillis)
    backpressure.onHeartbeat(consumerKey(heartbeat.partitionId()), heartbeat)
    if (debugEnabled) {
      logDebug(log"Streaming shuffle received a heartbeat for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, heartbeat.partitionId())} at position " +
        log"${MDC(COUNT, heartbeat.sequenceNumber())}")
    }
  }

  /**
   * Records an orderly end of stream -- once, and only when the stream really has reached its end.
   */
  private def handleTermination(termination: StreamTerminationMessage): Unit = {
    val partitionId = termination.partitionId()
    val totalBlocks = termination.totalBlocks()
    val state = partitionStateOf(partitionId)
    state.lastInboundMillis.set(clock.getTimeMillis())
    val expected = state.expectedSequenceNumber.get()
    if (totalBlocks > expected && state.hasOutstandingRepairs) {
      // A terminator that names MORE blocks than this consumer has reached, while a repair of this
      // partition is still outstanding, is not a contradiction: it is the truth arriving before the
      // blocks that make it true.
      deferTermination(state, totalBlocks)
    } else if (totalBlocks != expected) {
      rejectTermination(state, totalBlocks, expected)
    } else if (state.terminated.compareAndSet(false, true)) {
      // One helper, shared with the deferred path, so that a stream which ends immediately and one
      // which ends after its repairs close the gap leave *identical* state behind: the same total
      // published, the same protocol ledger retired, the same single completion marker queued.
      completeStream(state, totalBlocks)
      if (debugEnabled) {
        logDebug(log"Streaming shuffle stream for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, partitionId)} ended " +
          log"after ${MDC(COUNT, totalBlocks)} block(s)")
      }
    } else if (state.announcedBlocks.get() != totalBlocks) {
      rejectTermination(state, totalBlocks, state.announcedBlocks.get())
    } else {
      state.duplicateTerminations.incrementAndGet()
      if (debugEnabled) {
        logDebug(log"Streaming shuffle ignored a repeated end-of-stream for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, partitionId)}, which " +
          log"had already ended after ${MDC(COUNT, totalBlocks)} block(s)")
      }
    }
  }

  /**
   * Holds an end-of-stream that is ahead of this consumer's position while a repair is outstanding.
   */
  private def deferTermination(state: PartitionState, totalBlocks: Long): Unit = {
    if (!state.deferTermination(totalBlocks)) {
      rejectTermination(state, totalBlocks, state.deferredTerminationTotal)
    } else {
      state.deferredTerminations.incrementAndGet()
      reportRepair(log"Streaming shuffle is holding the end-of-stream of shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, state.partitionId)} at " +
        log"${MDC(COUNT, totalBlocks)} block(s) while ${MDC(THRESHOLD, state.quarantinedCount)} " +
        log"position(s) are still being repaired")
    }
  }

  /** Applies a held end-of-stream once the repairs that delayed it have closed the gap. */
  private def applyDeferredTermination(state: PartitionState): Unit = {
    val deferred = state.deferredTerminationTotal
    if (deferred != NO_BLOCK_TOTAL && deferred == state.expectedSequenceNumber.get() &&
        state.terminated.compareAndSet(false, true)) {
      completeStream(state, deferred)
      if (debugEnabled) {
        logDebug(log"Streaming shuffle applied the held end-of-stream of shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, state.partitionId)} " +
          log"after its repairs completed ${MDC(COUNT, deferred)} block(s)")
      }
    }
  }

  /**
   * Completes one stream: publishes its total, retires its ledger and marks the task's queue.
   *
   * @param state the partition whose stream has ended
   * @param totalBlocks the total the producer committed to, which is also the position reached
   */
  private def completeStream(state: PartitionState, totalBlocks: Long): Unit = {
    state.announcedBlocks.set(totalBlocks)
    backpressure.onStreamTermination(consumerKey(state.partitionId), totalBlocks)
    if (state.completionSignalled.compareAndSet(false, true)) {
      enqueue(StreamCompleted(state.partitionId, totalBlocks), 0L)
    }
  }

  /**
   * Refuses a data block that arrived after this stream had ended, and declares the producer lost.
   *
   * @param state the partition whose stream had already ended
   * @param sequenceNumber the position the late block claimed
   */
  private def rejectBlockAfterTermination(state: PartitionState, sequenceNumber: Long): Unit = {
    // Bounded on the executor's window, because the frequency is the peer's to choose: a producer
    // that keeps sending past its own end of stream sends as many blocks as it likes, and every one
    // of them reaches here.
    reportBounded(StreamingShuffleClientHandler.lateBlockLogAggregator,
      state.blocksAfterTermination, "late block(s)",
      log"Streaming shuffle refused block " +
        log"${MDC(BLOCK_ID, blockIdOf(state.partitionId, sequenceNumber))} of shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} because that stream had already ended after " +
        log"${MDC(COUNT, state.announcedBlocks.get())} block(s); a producer still sending past " +
        log"its own end of stream cannot be completed, so the upstream stage is recomputed")
    escalate(
      StreamingShuffleErrors.invalidSequenceNumber(
        shuffleId, state.partitionId, state.announcedBlocks.get(), sequenceNumber),
      state.partitionId,
      StreamingShuffleInvalidationReason.IncompleteStream)
  }

  /**
   * Refuses a terminator that does not describe this stream, and declares the producer lost.
   *
   * @param state the partition whose stream was being ended
   * @param claimedTotal the block total the terminator claimed
   * @param reconciledTotal the total the claim is refused against -- the position this consumer has
   *     actually reached, or the total an accepted terminator already established
   */
  private def rejectTermination(
      state: PartitionState,
      claimedTotal: Long,
      reconciledTotal: Long): Unit = {
    // Bounded for the same reason as the late-block record above: a peer may send as many
    // terminators as it likes and each one reaches here, while the escalation fires once.
    reportBounded(StreamingShuffleClientHandler.rejectedTerminationLogAggregator,
      state.rejectedTerminations, "refused end(s) of stream",
      log"Streaming shuffle refused an end-of-stream for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, state.partitionId)} " +
        log"claiming ${MDC(COUNT, claimedTotal)} block(s) against " +
        log"${MDC(THRESHOLD, reconciledTotal)}; a stream may only end at the position it has " +
        log"reached, so the producer is treated as lost and the upstream stage is recomputed")
    escalate(
      StreamingShuffleErrors.invalidSequenceNumber(
        shuffleId, state.partitionId, reconciledTotal, claimedTotal),
      state.partitionId,
      StreamingShuffleInvalidationReason.IncompleteStream)
  }

  /** Refuses a control message that only ever travels in the other direction. */
  private def refuseOutboundOnlyMessage(
      message: StreamingShuffleMessage,
      actual: StreamingShuffleMessageType): Unit = {
    reportBounded(StreamingShuffleClientHandler.wrongDirectionLogAggregator,
      wrongDirectionFrames, "wrong-direction frame(s)",
      log"Streaming shuffle consumer for shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, message.partitionId())} refused a " +
        log"${MDC(MESSAGE, actual.name())} frame, which only ever travels from consumer to " +
        log"producer, so the producer is treated as lost and the upstream stage is recomputed")
    escalate(
      StreamingShuffleErrors.unexpectedMessageType(CONSUMER_INBOUND_DESCRIPTION, actual.name()),
      message.partitionId())
  }

  // The hand-off queue, read by the reduce task's own thread.

  /**
   * Places one event on the hand-off queue.
   *
   * @return true if the event was handed on, which is the condition under which the caller may
   *     commit the bookkeeping that depends on it having been delivered
   */
  private def enqueue(event: Inbound, payloadBytes: Long): Boolean = {
    if (closed.get()) {
      if (debugEnabled) {
        logDebug(log"Streaming shuffle dropped an inbound event for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} partition " +
          log"${MDC(PARTITION_ID, event.partitionId)} because the handler is closed")
      }
      false
    } else if (inbound.offer(event)) {
      if (payloadBytes > 0L) {
        queuedPayloadBytes.addAndGet(payloadBytes)
      }
      true
    } else {
      escalate(
        new IllegalStateException(s"The streaming shuffle hand-off queue of shuffle $shuffleId " +
          s"partition ${event.partitionId} is full at ${inbound.size()} event(s) holding " +
          s"${queuedPayloadBytes.get()} byte(s); the receive window should have closed at " +
          s"$INBOUND_QUEUE_HIGH_WATER_MARK event(s) or $INBOUND_HIGH_WATER_BYTES byte(s)"),
        event.partitionId)
      false
    }
  }

  /** Takes the next inbound event without waiting, or `None` when nothing has arrived yet. */
  def poll(): Option[Inbound] = accountForPolled(inbound.poll())

  /** Takes the next inbound event, waiting at most `timeoutMillis` for one. */
  def poll(timeoutMillis: Long): Option[Inbound] = {
    // The backoff timer of the replay protocol, driven from the one thread that is guaranteed to be
    // running while a consumer waits.
    retryDueReplays()
    if (timeoutMillis <= 0L) {
      poll()
    } else {
      accountForPolled(inbound.poll(timeoutMillis, TimeUnit.MILLISECONDS))
    }
  }

  /**
   * Discounts a taken event from the queue's byte accounting and re-evaluates the receive window.
   */
  private def accountForPolled(event: Inbound): Option[Inbound] = {
    if (event == null) {
      None
    } else {
      event match {
        case received: BlockReceived =>
          queuedPayloadBytes.addAndGet(-received.block.payloadLength().toLong)
        case _ =>
          ()
      }
      evaluateAutoRead()
      Some(event)
    }
  }

  /** How many inbound events are waiting to be taken. */
  def queuedEventCount: Int = inbound.size()

  /** How many payload bytes those events are holding. */
  def queuedByteCount: Long = queuedPayloadBytes.get()

  // Outbound: acknowledgements, replay requests and heartbeats.

  /**
   * Acknowledges everything up to and including `consumedSequenceNumber` for one partition.
   *
   * @param partitionId the reduce partition being acknowledged
   * @param consumedSequenceNumber the highest block position the task has consumed, or
   *     `AckMessage.NOTHING_CONSUMED` to state that it has consumed nothing
   * @return true if this call advanced the acknowledged position
   */
  def acknowledge(partitionId: Int, consumedSequenceNumber: Long): Boolean = {
    require(consumedSequenceNumber >= AckMessage.NOTHING_CONSUMED,
      s"The consumed position must be at least ${AckMessage.NOTHING_CONSUMED} but was " +
        s"$consumedSequenceNumber.")
    if (!servesPartition(partitionId)) {
      // The invariant boundary, not a convenience.
      return false
    }
    val state = partitionStateOf(partitionId)
    val previous = state.acknowledgedPosition.get()
    if (consumedSequenceNumber <= previous) {
      false
    } else {
      state.acknowledgedPosition.set(consumedSequenceNumber)
      // Released to the executor's shared budget at the same moment it is released to this stream's
      // own ledger, so the two readings can never disagree about what this consumer is holding.
      backpressure.releaseReceiveQuota(state.releaseThrough(consumedSequenceNumber))
      // The protocol releases the credit these blocks held and leaves the throttled state, which is
      // exactly the transition that lets the receive window reopen below.
      backpressure.onAck(consumerKey(partitionId), consumedSequenceNumber)
      val channel = channelRef.get()
      if (channel != null && channel.isActive && channel.isWritable) {
        writeMessage(channel, new AckMessage(shuffleId, mapId, partitionId,
          consumedSequenceNumber))
      } else {
        state.pendingAckPosition.set(consumedSequenceNumber)
        if (debugEnabled) {
          logDebug(log"Streaming shuffle deferred an acknowledgement at position " +
            log"${MDC(COUNT, consumedSequenceNumber)} for shuffle " +
            log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, partitionId)} " +
            log"because the channel is not writable")
        }
      }
      evaluateAutoRead()
      true
    }
  }

  /**
   * Writes every acknowledgement that was held back, in partition order so the sequence of writes
   * is deterministic.
   */
  private def flushDeferredAcks(): Unit = {
    val channel = channelRef.get()
    if (channel != null && channel.isActive && channel.isWritable) {
      partitions.values().asScala.toSeq.sortBy(_.partitionId).foreach { state =>
        val pending = state.pendingAckPosition.getAndSet(AckMessage.NOTHING_CONSUMED)
        if (pending > AckMessage.NOTHING_CONSUMED) {
          writeMessage(channel, new AckMessage(shuffleId, mapId, state.partitionId, pending))
        }
      }
    }
  }

  /**
   * Asks the producer to replay an inclusive run of block positions.
   *
   * @return true if the repair is under way, meaning the request was written now or is waiting
   *     only for its backoff to elapse; false if the caller must escalate instead
   */
  def requestRetransmission(partitionId: Int, firstSequenceNumber: Long,
      lastSequenceNumber: Long): Boolean = {
    val channel = channelRef.get()
    val bounded = math.min(
      math.max(firstSequenceNumber, lastSequenceNumber),
      firstSequenceNumber + MAX_REPLAY_WINDOW_SPAN)
    if (closed.get() || channel == null || !channel.isActive) {
      false
    } else if (firstSequenceNumber < 0L || !servesPartition(partitionId)) {
      false
    } else if (!isRetained(partitionId, firstSequenceNumber) || !isRetained(partitionId, bounded)) {
      false
    } else {
      val state = partitionStateOf(partitionId)
      val window = state.replayWindowFor(firstSequenceNumber, bounded)
      window.charge(clock.getTimeMillis()) match {
        case ReplayVerdict.Exhausted =>
          // Bounded on the executor's window: a stream that is systematically corrupt exhausts a
          // budget per repair window, and a repair window exists per position range, so the record
          // recurs as often as the corruption does.
          reportBounded(StreamingShuffleClientHandler.replayBudgetLogAggregator,
            replayBudgetsSpent, "spent replay budget(s)",
            log"Streaming shuffle will not ask shuffle ${MDC(SHUFFLE_ID, shuffleId)} " +
              log"partition ${MDC(PARTITION_ID, partitionId)} to replay position(s) " +
              log"${MDC(COUNT, firstSequenceNumber)} through ${MDC(THRESHOLD, bounded)} again " +
              log"because the replay budget of " +
              log"${MDC(MAX_ATTEMPTS, BackpressureProtocol.MAX_RETRY_ATTEMPTS)} attempt(s) is " +
              log"spent")
          state.discardReplayWindow(window)
          false
        case ReplayVerdict.Deferred =>
          // The repair is real and already recorded; only its next attempt is waiting for the
          // backoff to elapse.
          state.quarantine(firstSequenceNumber, bounded)
          true
        case ReplayVerdict.Granted(attempt) =>
          state.quarantine(firstSequenceNumber, bounded)
          state.replayRequests.incrementAndGet()
          val written = writeReplayRequests(channel, partitionId, firstSequenceNumber, bounded)
          if (written && debugEnabled) {
            logDebug(log"Streaming shuffle asked shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
              log"${MDC(PARTITION_ID, partitionId)} to replay position(s) " +
              log"${MDC(COUNT, firstSequenceNumber)} through ${MDC(THRESHOLD, bounded)} on " +
              log"attempt ${MDC(MAX_ATTEMPTS, attempt)}")
          }
          written
      }
    }
  }

  /**
   * Asks the producer to replay an inclusive run of positions, as one request naming the whole run.
   *
   * @param channel the producer's channel, already checked to be active and writable by the
   *     caller
   * @param partitionId the stream being repaired
   * @param first inclusive first position to replay
   * @param last inclusive last position to replay, never below `first`
   * @return true when the request reached the channel, which is what makes the repair under
   *     way; false when it could not be written
   */
  private def writeReplayRequests(
      channel: Channel,
      partitionId: Int,
      first: Long,
      last: Long): Boolean = {
    writeMessage(channel,
      new RetransmitRequestMessage(shuffleId, mapId, partitionId, first, math.max(first, last)))
  }

  /** Sends every replay request whose backoff has elapsed, and reports how many went out. */
  def retryDueReplays(): Int = {
    val channel = channelRef.get()
    if (closed.get() || channel == null || !channel.isActive) {
      0
    } else {
      var sent = 0
      val nowMillis = clock.getTimeMillis()
      partitions.values().asScala.foreach { state =>
        state.dueReplayWindows(nowMillis).foreach { window =>
          if (state.isRangeQuarantined(window.first, window.last)) {
            window.charge(nowMillis) match {
              case ReplayVerdict.Granted(_) =>
                state.replayRequests.incrementAndGet()
                if (writeReplayRequests(channel, state.partitionId, window.first, window.last)) {
                  sent += 1
                }
              case _ =>
                ()
            }
          } else {
            state.discardReplayWindow(window)
          }
        }
      }
      sent
    }
  }

  /** Whether a block position is still retained by the producer and can therefore be replayed. */
  private def isRetained(partitionId: Int, sequenceNumber: Long): Boolean = {
    backpressure.isWithinUnacknowledgedWindow(consumerKey(partitionId), sequenceNumber) ||
      sequenceNumber > partitionStateOf(partitionId).acknowledgedPosition.get()
  }

  /**
   * Sends a heartbeat for one partition if the heartbeat interval has elapsed, that interval being
   * a third of the producer-liveness bound it refreshes.
   *
   * @return true if a heartbeat was written
   */
  def sendHeartbeatIfDue(partitionId: Int): Boolean = {
    if (closed.get() || !servesPartition(partitionId) ||
        backpressure.isStreamTerminated(consumerKey(partitionId))) {
      false
    } else if (backpressure.shouldSendHeartbeat(consumerKey(partitionId))) {
      // The interval is the protocol's to own, but the position is this handler's to declare: only
      // this handler knows which position it expects next.
      val stamped = backpressure.heartbeatFor(
        consumerKey(partitionId), announcedPosition(partitionStateOf(partitionId)))
      stamped.exists(heartbeat => writeHeartbeat(partitionId, heartbeat))
    } else if (backpressure.isStreamRegistered(consumerKey(partitionId))) {
      // The protocol owns the interval for a registered stream and has just said it is not due.
      false
    } else {
      val state = partitionStateOf(partitionId)
      val nowMillis = clock.getTimeMillis()
      val lastSent = state.lastHeartbeatSentMillis.get()
      if (lastSent != NO_TIMESTAMP &&
          nowMillis - lastSent < BackpressureProtocol.HEARTBEAT_INTERVAL_MS) {
        false
      } else {
        writeHeartbeat(partitionId, buildHeartbeat(state))
      }
    }
  }

  /** Builds a heartbeat positioned at how far this consumer has got; it carries no time value. */
  private def buildHeartbeat(state: PartitionState): HeartbeatMessage = {
    new HeartbeatMessage(
      shuffleId, mapId, state.partitionId, announcedPosition(state), consumerToken)
  }

  /** The position this consumer announces for one partition: the next position it expects. */
  private def announcedPosition(state: PartitionState): Long =
    math.max(0L, state.expectedSequenceNumber.get())

  /** Writes a heartbeat and restarts this handler's own interval for the partition. */
  private def writeHeartbeat(partitionId: Int, heartbeat: HeartbeatMessage): Boolean = {
    val channel = channelRef.get()
    val written = channel != null && writeMessage(channel, heartbeat)
    if (written) {
      partitionStateOf(partitionId).lastHeartbeatSentMillis.set(clock.getTimeMillis())
    }
    written
  }

  /**
   * Writes one control message as the body of a one-way RPC.
   *
   * @return true if the message was handed to the channel
   */
  private def writeMessage(channel: Channel, message: StreamingShuffleMessage): Boolean = {
    if (!channel.isActive) {
      false
    } else {
      try {
        val partitionId = message.partitionId()
        channel.writeAndFlush(new OneWayMessage(message.toManagedBuffer()))
          .addListener(new ChannelFutureListener {
            override def operationComplete(future: ChannelFuture): Unit = {
              if (!future.isSuccess) {
                val cause = Option(future.cause())
                  .getOrElse(new IllegalStateException(
                    s"The streaming shuffle consumer of shuffle $shuffleId partition " +
                      s"$partitionId could not write a " +
                      s"${message.getClass.getSimpleName} frame."))
                escalate(cause, partitionId)
              }
            }
          })
        true
      } catch {
        case NonFatal(e) =>
          escalate(e, message.partitionId())
          false
      }
    }
  }

  // Failure handling and the bridge to the task thread.

  /** Records a failure for the task thread to raise, and wakes a task that is waiting for input. */
  private def escalate(cause: Throwable, partitionId: Int): Unit = {
    escalate(cause, partitionId, StreamingShuffleInvalidationReason.ConnectionTimeout)
  }

  /**
   * The same escalation, attributing the loss to a specific invalidation reason.
   *
   * @param cause the failure that revealed the loss
   * @param partitionId the partition the loss is attributed to
   * @param invalidation how the reader should attribute its invalidation
   */
  private def escalate(
      cause: Throwable,
      partitionId: Int,
      invalidation: StreamingShuffleInvalidationReason): Unit = {
    // The reason carried on the marker becomes the operator-facing text of the fetch failure the
    // reader raises from it, so it is the cause's own message -- which for a typed streaming
    // condition names the condition, the disagreeing numbers and the SQLSTATE.
    signalProducerLost(partitionId, describeCause(cause), cause, invalidation)
    errorNotifier.setError(cause)
    // The tally still advances on every escalation, whatever is logged: the reader reads it to
    // decide whether an I/O thread has already diagnosed a failure, and a suite asserts on it.
    val reported = escalationsReported.incrementAndGet()
    if (debugEnabled) {
      logDebug(log"Streaming shuffle consumer failed for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, partitionId)} " +
        log"(${MDC(COUNT, reported)} escalation(s) on this channel): " +
        log"${MDC(ERROR, cause.getMessage)}", cause)
    }
  }

  /**
   * The operator-facing rendering of a failure, phrased so that it composes into the fetch-failure
   * reason the reader builds from it: the cause's own message without its terminating full stop, or
   * the cause's simple type name when it carries no message at all.
   */
  private def describeCause(cause: Throwable): String = {
    val message = if (cause == null) null else cause.getMessage
    if (message == null || message.trim.isEmpty) {
      if (cause == null) "no cause was reported" else cause.getClass.getSimpleName
    } else {
      val trimmed = message.trim
      if (trimmed.endsWith(".")) trimmed.dropRight(1) else trimmed
    }
  }

  /** Marks one partition's producer as gone, at most once, and wakes whoever is waiting on it. */
  private def signalProducerLost(
      partitionId: Int,
      reason: String,
      cause: Throwable,
      invalidation: StreamingShuffleInvalidationReason =
        StreamingShuffleInvalidationReason.ConnectionTimeout): Unit = {
    // A failure that belongs to the channel rather than to one stream must still be reported, and
    // it cannot use a partition's latch because it names no partition.
    val firstSignal =
      if (servesPartition(partitionId)) {
        partitionStateOf(partitionId).producerLostSignalled.compareAndSet(false, true)
      } else {
        channelLostSignalled.compareAndSet(false, true)
      }
    if (firstSignal) {
      producerLostAtMillis.compareAndSet(NO_TIMESTAMP, clock.getTimeMillis())
      enqueue(ProducerLost(partitionId, reason, cause, invalidation), 0L)
    }
  }

  // Observability, all of it derived from state this handler already keeps.

  /** How many failures this handler has recorded on the shared error notifier. */
  def reportedEscalationCount: Long = escalationsReported.get()

  /** Whether THIS handler wants its channel to be reading. */
  def isAutoReadEnabled: Boolean = !readGate.get().isThrottling(this)

  /** Whether the physical channel is currently reading from its socket. */
  def isChannelReadEnabled: Boolean = readGate.get().isReadEnabled

  /** How many participants of this handler's channel currently want reading stopped. */
  private[streaming] def throttlingParticipantCount: Int = readGate.get().throttlingParticipantCount

  /** Why reading is currently stopped, or `None` when it is not. */
  def currentThrottleCause: Option[String] = Option(throttleCause.get())

  /** Transitions into the throttled state observed on this channel. */
  def throttleTransitionCount: Long = throttleTransitions.get()

  /** Whether the producer of this channel has been declared gone. */
  def isProducerLost: Boolean = producerLostAtMillis.get() != NO_TIMESTAMP

  /**
   * Milliseconds since the producer was declared gone, read from the injected clock, or `None`
   * while it is still believed alive.
   */
  def producerLostElapsedMillis: Option[Long] = {
    val lostAt = producerLostAtMillis.get()
    if (lostAt == NO_TIMESTAMP) None else Some(math.max(0L, clock.getTimeMillis() - lostAt))
  }

  /**
   * Whether nothing at all has been received for one partition for longer than the five-second
   * connection timeout, which is the detection the reader turns into a partial-read invalidation
   * and a fetch failure.
   */
  def isProducerSilent(partitionId: Int): Boolean = {
    val state = partitions.get(Integer.valueOf(partitionId))
    if (state == null || state.terminated.get()) {
      false
    } else {
      val lastInbound = state.lastInboundMillis.get()
      val silentLocally = lastInbound != NO_TIMESTAMP &&
        clock.getTimeMillis() - lastInbound >= PRODUCER_CONNECTION_TIMEOUT_MS
      silentLocally || backpressure.isProducerTimedOut(consumerKey(partitionId))
    }
  }

  /** Milliseconds since anything was received for one partition, or `None` for an unknown one. */
  def millisSinceInbound(partitionId: Int): Option[Long] = {
    val state = partitions.get(Integer.valueOf(partitionId))
    if (state == null || state.lastInboundMillis.get() == NO_TIMESTAMP) {
      None
    } else {
      Some(math.max(0L, clock.getTimeMillis() - state.lastInboundMillis.get()))
    }
  }

  /** Whether the producer has announced the orderly end of one partition's stream. */
  def isStreamComplete(partitionId: Int): Boolean = {
    val state = partitions.get(Integer.valueOf(partitionId))
    state != null && state.terminated.get()
  }

  /** Blocks the producer claims to have sent for one partition, or `None` before it has said. */
  def announcedBlockCount(partitionId: Int): Option[Long] = {
    val state = partitions.get(Integer.valueOf(partitionId))
    if (state == null || state.announcedBlocks.get() == NO_BLOCK_TOTAL) {
      None
    } else {
      Some(state.announcedBlocks.get())
    }
  }

  /** Reads one per-partition counter without allocating state for a partition that has none. */
  private def readState[T](partitionId: Int, absent: T)(read: PartitionState => T): T = {
    val state = partitions.get(Integer.valueOf(partitionId))
    if (state == null) absent else read(state)
  }

  /** The next block position this handler expects for one partition. */
  def expectedSequenceNumber(partitionId: Int): Long =
    readState(partitionId, 0L)(_.expectedSequenceNumber.get())

  /** The highest position acknowledged for one partition, or the nothing-consumed sentinel. */
  def acknowledgedPosition(partitionId: Int): Long =
    readState(partitionId, AckMessage.NOTHING_CONSUMED)(_.acknowledgedPosition.get())

  /** Blocks accepted and handed on for one partition. */
  def acceptedBlockCount(partitionId: Int): Long =
    readState(partitionId, 0L)(_.acceptedBlocks.get())

  /** Blocks refused for one partition because they failed verification. */
  def corruptBlockCount(partitionId: Int): Long =
    readState(partitionId, 0L)(_.corruptBlocks.get())

  /** Replays requested for one partition, whether for a gap or for corruption. */
  def replayRequestCount(partitionId: Int): Long =
    readState(partitionId, 0L)(_.replayRequests.get())

  /** Blocks discarded for one partition because they had already been accepted. */
  def duplicateBlockCount(partitionId: Int): Long =
    readState(partitionId, 0L)(_.duplicateBlocks.get())

  /** Positions of one partition awaiting a replacement block. */
  def quarantinedPositionCount(partitionId: Int): Int =
    readState(partitionId, 0)(_.quarantinedCount)

  /** Frames dropped because they named a shuffle or a partition this handler does not serve. */
  def misaddressedFrameCount: Long = misaddressedFrames.get()

  /** Control frames refused because they only ever travel from consumer to producer. */
  def wrongDirectionFrameCount: Long = wrongDirectionFrames.get()

  /** Transport callbacks refused because they arrived on a channel this handler is not bound to. */
  def foreignChannelCallbackCount: Long = foreignChannelCallbacks.get()

  /** End-of-stream frames of one partition held while a repair of it was still outstanding. */
  def deferredTerminationCount(partitionId: Int): Long =
    readState(partitionId, 0L)(_.deferredTerminations.get())

  /** Transport callbacks refused because their channel had no authenticated identity. */
  def unauthenticatedCallbackCount: Long = unauthenticatedCallbacks.get()

  /** Whether this handler has latched a principal established by Spark transport authentication. */
  def authenticatedTransportBound: Boolean = boundTransportPrincipal.get() != null

  /** Blocks of one partition declined because the executor's shared budget was exhausted. */
  def quotaRefusalCount(partitionId: Int): Long =
    readState(partitionId, 0L)(_.quotaRefusals.get())

  /** End-of-stream frames of one partition refused because they did not describe its stream. */
  def rejectedTerminationCount(partitionId: Int): Long =
    readState(partitionId, 0L)(_.rejectedTerminations.get())

  /** End-of-stream frames of one partition ignored because an identical one had been accepted. */
  def duplicateTerminationCount(partitionId: Int): Long =
    readState(partitionId, 0L)(_.duplicateTerminations.get())

  /** Data blocks of one partition refused because that stream had already ended. */
  def blocksAfterTerminationCount(partitionId: Int): Long =
    readState(partitionId, 0L)(_.blocksAfterTermination.get())

  /** Replacements of one partition admitted after that stream had ended. */
  def repairsAfterTerminationCount(partitionId: Int): Long =
    readState(partitionId, 0L)(_.repairsAfterTermination.get())

  /**
   * Block admissions whose share of the executor's shared receive budget was returned because this
   * handler closed while the admission was in flight.
   */
  def rolledBackAdmissionCount: Long = admissionsRolledBack.get()

  /** Admissions that have charged the shared receive budget but not yet recorded ownership. */
  def admissionsInFlightCount: Int = admissionsInFlight.get()

  /** Decoded frames accepted by the bounded workers but not yet settled. */
  def dataPlaneTasksInFlightCount: Int = dataPlaneTasksInFlight.get()

  /** Frames refused because the executor-wide bounded worker queue was full. */
  def dataPlaneRefusalCount: Long = dataPlaneRefusals.get()

  /** Test and lifecycle seam for awaiting accepted data-plane work without polling thread names. */
  def awaitDataPlaneIdle(timeoutMs: Long): Boolean =
    backpressure.awaitDataPlaneIdle(timeoutMs)

  /**
   * Partitions this handler holds metadata for, which can never exceed the width of the range it
   * was constructed with however many partition ids a peer names.
   */
  def trackedPartitionCount: Int = partitions.size()

  /** The partitions this channel has seen traffic for, in a deterministic order. */
  def observedPartitionIds: Seq[Int] =
    partitions.values().asScala.map(_.partitionId).toSeq.sorted

  /** First partition this handler was created to consume, before any frame has arrived. */
  private[streaming] def firstRequestedPartitionId: Option[Int] =
    if (startPartition < endPartition) Some(startPartition) else None

  /** Partitions that have neither terminated nor had their producer declared gone. */
  private def partitionsAwaitingData: Seq[Int] = {
    partitions.values().asScala
      .filter(state => !state.terminated.get() && !state.producerLostSignalled.get())
      .map(_.partitionId)
      .toSeq
      .sorted
  }

  /**
   * The transport configuration this channel is tuned by, drawn from the streaming module's own
   * `spark.shuffle-streaming.io.*` namespace rather than from the settings block transfer uses.
   */
  def streamingTransportConf: TransportConf = transportConf

  /** Whether OS-level keepalive is enabled for the streaming module. */
  def isTcpKeepAliveEnabled: Boolean = tcpKeepAliveEnabled

  /** Transport-level replay attempts permitted for this module. */
  def maxTransportRetries: Int = maxIoRetries

  /** Pause between transport-level replay attempts, in milliseconds. */
  def transportRetryWaitMillis: Int = ioRetryWaitMillis

  // Cleanup.

  /** Releases everything this handler holds, once. */
  def close(): Unit = close(releaseChannel = true)

  /**
   * Releases everything this handler holds, once, optionally leaving the channel open.
   *
   * @param releaseChannel whether to close the channel beneath this handler.
   */
  def close(releaseChannel: Boolean): Unit = {
    if (closed.compareAndSet(false, true)) {
      awaitFrameTasksInFlight()
      awaitAdmissionsInFlight()
      val drained = drainAndRelease()
      // Everything still charged to the executor's shared budget comes back here, whether this
      // handler is closing after an orderly end of stream, after a producer failure or after task
      // cancellation.
      val returned = releaseAllQuota()
      partitions.clear()
      // Withdrawn before the channel reference is dropped, and the departure is recorded under the
      // same monitor as every other transition.
      gateLock.synchronized {
        gateDeparted = true
        readGate.get().withdraw(this)
      }
      val channel = channelRef.getAndSet(null)
      if (channel != null && releaseChannel) {
        channel.close()
      }
      if (debugEnabled) {
        logDebug(log"Streaming shuffle consumer for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} released ${MDC(COUNT, drained)} queued event(s) and " +
          log"${MDC(NUM_BYTES, returned)} byte(s) of the executor's shared receive budget after " +
          log"${MDC(THRESHOLD, throttleTransitions.get())} throttling transition(s)")
      }
    }
  }

  /**
   * Waits on a completion condition for decoded frame work already accepted by the worker stripes.
   */
  private def awaitFrameTasksInFlight(): Unit = {
    val stillInFlight = awaitCounter(dataPlaneTasksInFlight, FRAME_TASK_SETTLE_TIMEOUT_MS)
    if (stillInFlight > 0) {
      logWarning(log"Streaming shuffle consumer for shuffle ${MDC(SHUFFLE_ID, shuffleId)} closed " +
        log"with ${MDC(COUNT, stillInFlight)} decoded frame task(s) still in flight after " +
        log"${MDC(TIMEOUT, FRAME_TASK_SETTLE_TIMEOUT_MS)} ms; each data block returns its own " +
        log"payload reservation when it observes the closure")
    }
  }

  /** Waits on the same completion condition for admissions already past their reservation. */
  private def awaitAdmissionsInFlight(): Unit = {
    val stillInFlight = awaitCounter(admissionsInFlight, ADMISSION_SETTLE_TIMEOUT_MS)
    if (stillInFlight > 0) {
      logWarning(log"Streaming shuffle consumer for shuffle ${MDC(SHUFFLE_ID, shuffleId)} closed " +
        log"with ${MDC(COUNT, stillInFlight)} block admission(s) still in flight after " +
        log"${MDC(TIMEOUT, ADMISSION_SETTLE_TIMEOUT_MS)} ms; each returns its own share of " +
        log"the executor's shared receive budget as it discovers the closure")
    }
  }

  /** Bounded condition wait shared by the two close-time lifecycle counters. */
  private def awaitCounter(counter: AtomicInteger, timeoutMs: Long): Int = {
    var remainingNanos = TimeUnit.MILLISECONDS.toNanos(math.max(0L, timeoutMs))
    lifecycleLock.lock()
    try {
      while (counter.get() > 0 && remainingNanos > 0L) {
        try {
          remainingNanos = lifecycleChanged.awaitNanos(remainingNanos)
        } catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
            remainingNanos = 0L
        }
      }
      counter.get()
    } finally {
      lifecycleLock.unlock()
    }
  }

  /** Whether [[close]] has run. */
  def isClosed: Boolean = closed.get()

  /**
   * Returns every byte this handler still has charged to the executor's shared receive budget.
   *
   * @return how many bytes were returned
   */
  private def releaseAllQuota(): Long = {
    var released = 0L
    partitions.values().asScala.foreach { state =>
      // Drained rather than read, so the ledger cannot report the same bytes twice.
      val outstanding = state.drainOutstandingBytes()
      if (outstanding > 0L) {
        released += outstanding
        backpressure.releaseReceiveQuota(outstanding)
      }
    }
    released
  }

  /**
   * Empties the hand-off queue and rewinds the byte accounting.
   *
   * @return how many events were discarded
   */
  private def drainAndRelease(): Int = {
    var discarded = 0
    var event = inbound.poll()
    while (event != null) {
      discarded += 1
      event = inbound.poll()
    }
    inbound.clear()
    queuedPayloadBytes.set(0L)
    discarded
  }

  // Small helpers.

  /** The bookkeeping for one partition, created on first sight. */
  private def partitionStateOf(partitionId: Int): PartitionState = {
    require(servesPartition(partitionId),
      s"Partition $partitionId is outside the range [$startPartition, $endPartition) this " +
        s"streaming shuffle consumer of shuffle $shuffleId reads.")
    val key = Integer.valueOf(partitionId)
    val existing = partitions.get(key)
    if (existing != null) {
      existing
    } else {
      val created = new PartitionState(partitionId)
      val raced = partitions.putIfAbsent(key, created)
      if (raced == null) created else raced
    }
  }

  /** A stable identifier for one streaming block, for diagnostics only. */
  private def blockIdOf(partitionId: Int, sequenceNumber: Long): String =
    s"streaming_shuffle_${shuffleId}_${partitionId}_$sequenceNumber"

  /** The class of an unexpected inbound object, named without dereferencing it. */
  private def describeClassOf(obj: Any): String =
    if (obj == null) "null" else obj.getClass.getName
}

/**
 * The receive window of one '''physical''' channel, shared by every consumer handler multiplexed
 * onto it.
 */
private[streaming] final class StreamingShuffleChannelReadGate {

  /** The one monitor every transition of this gate is performed under. */
  private val stateLock = new Object

  /** The participants that currently want reading stopped, mapped to why. */
  private val throttling = new ConcurrentHashMap[AnyRef, String]()

  /** The channel whose window this gate owns, or null until a participant attaches one. */
  private val channelRef = new AtomicReference[Channel](null)

  /** The state last written to the channel, so a redundant configuration write is skipped. */
  private val applied = new AtomicBoolean(true)

  /**
   * Attaches the physical channel this gate governs and applies the standing state to it.
   *
   * @param channel the channel to govern
   */
  def attach(channel: Channel): Unit = {
    if (channel != null) {
      stateLock.synchronized {
        channelRef.set(channel)
        applyUnionLocked()
      }
    }
  }

  /**
   * States that one participant wants reading stopped.
   *
   * @param participant the participant stating the intent
   * @param cause operator-facing reason, retained for diagnostics
   * @return true when this call was the one that closed the window
   */
  def throttle(participant: AnyRef, cause: String): Boolean = {
    stateLock.synchronized {
      throttling.put(participant, cause)
      applyUnionLocked()
    }
  }

  /**
   * States that one participant no longer wants reading stopped.
   *
   * @param participant the participant withdrawing the intent
   * @return true when this call was the one that reopened the window
   */
  def resume(participant: AnyRef): Boolean = {
    stateLock.synchronized {
      throttling.remove(participant)
      applyUnionLocked()
    }
  }

  /**
   * Withdraws a departing participant from the gate entirely.
   *
   * @param participant the participant leaving
   */
  def withdraw(participant: AnyRef): Unit = {
    resume(participant)
  }

  /** Whether the socket is currently being read. */
  def isReadEnabled: Boolean = applied.get()

  /** How many participants currently want reading stopped. */
  def throttlingParticipantCount: Int = throttling.size()

  /**
   * Whether this participant currently wants reading stopped.
   *
   * @param participant the participant to ask about
   * @return true when the participant is holding the window shut
   */
  def isThrottling(participant: AnyRef): Boolean = throttling.containsKey(participant)

  /**
   * Applies the union of the participants' intents to the channel, writing only on a change.
   *
   * @return true when this call changed the applied state
   */
  private def applyUnionLocked(): Boolean = {
    val wanted = throttling.isEmpty
    val changed = applied.getAndSet(wanted) != wanted
    val channel = channelRef.get()
    if (channel != null) {
      // Written whenever the socket disagrees with the union, not only when the union moved.
      if (changed || channel.config().isAutoRead != wanted) {
        channel.config().setAutoRead(wanted)
      }
    }
    changed
  }
}

/**
 * Constants, per-partition bookkeeping and the envelope type the consumer channel hands to the
 * reduce task.
 */
private[spark] object StreamingShuffleClientHandler {

  /**
   * Transport module name for streaming shuffle, passed as an argument to
   * `SparkTransportConf.fromSparkConf` and never added to a shared file.
   */
  val TRANSPORT_MODULE: String = "shuffle-streaming"

  /**
   * The executor-scoped windows bounding every recurring, default-level record a consumer makes,
   * one per condition.
   */
  private[streaming] val repairLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val replayBudgetLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val foreignChannelLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val misaddressedFrameLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val lateBlockLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val rejectedTerminationLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val wrongDirectionLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private val logAggregators: Seq[MemorySpillManager.ExecutorLogAggregator] =
    Seq(repairLogAggregator, replayBudgetLogAggregator, foreignChannelLogAggregator,
      misaddressedFrameLogAggregator, lateBlockLogAggregator, rejectedTerminationLogAggregator,
      wrongDirectionLogAggregator)

  /** Returns the executor-scoped log aggregation above to its initial state. */
  private[streaming] def resetLogAggregationForTesting(): Unit = {
    logAggregators.foreach(aggregator => aggregator.reset())
  }

  /** Silence after which a producer is declared gone. */
  val PRODUCER_CONNECTION_TIMEOUT_MS: Long = BackpressureProtocol.ACK_TIMEOUT_MS

  /** Hard bound on the hand-off queue. */
  val INBOUND_QUEUE_CAPACITY: Int = 32

  val INBOUND_QUEUE_HIGH_WATER_MARK: Int = 8

  /** Queued events at which it may reopen, low enough that a slow task cannot make it flap. */
  val INBOUND_QUEUE_LOW_WATER_MARK: Int = 4

  /** Queued payload bytes at which the receive window closes, sixteen mebibytes. */
  val INBOUND_HIGH_WATER_BYTES: Long = 8L * DataBlockMessage.MAX_BLOCK_SIZE_BYTES

  /** Queued payload bytes at which it may reopen, four mebibytes. */
  val INBOUND_LOW_WATER_BYTES: Long = 2L * DataBlockMessage.MAX_BLOCK_SIZE_BYTES

  /**
   * Widest run of positions a single repair may cover, so that an over-wide window is narrowed
   * rather than refused.
   */
  val MAX_REPLAY_WINDOW_BLOCKS: Long = RetransmitRequestMessage.MAX_REQUESTED_BLOCKS

  /** Widest span between the two ends of a repair, the run being inclusive of both. */
  val MAX_REPLAY_WINDOW_SPAN: Long = MAX_REPLAY_WINDOW_BLOCKS - 1L

  /** Positions of one partition that may await a replacement block at any moment. */
  val MAX_QUARANTINED_POSITIONS: Int = (4L * MAX_REPLAY_WINDOW_BLOCKS).toInt

  val NO_TIMESTAMP: Long = Long.MinValue

  /** Bound for close waiting on decoded frames already accepted by the worker stripes. */
  val FRAME_TASK_SETTLE_TIMEOUT_MS: Long = 1000L

  /** Bound for close waiting on an admission already past its quota reservation. */
  val ADMISSION_SETTLE_TIMEOUT_MS: Long = 100L

  /** Sentinel for "no incompatible protocol revision has been seen". */
  val NO_OBSERVED_VERSION: Int = Int.MinValue

  val NO_BLOCK_TOTAL: Long = -1L

  /**
   * Sentinel partition for a frame refused before its header could be read, and for a failure that
   * belongs to the channel rather than to any one stream.
   */
  val UNKNOWN_PARTITION_ID: Int = -1

  /** Reported cause when the receive window closes because a consumer has no credit left. */
  val THROTTLE_CAUSE_CREDIT: String = "consumer credit is exhausted"

  /** Reported cause when it closes because the reduce task is not keeping up with the queue. */
  val THROTTLE_CAUSE_QUEUE: String = "the hand-off queue reached its high-water mark"

  /** Named cause for a receive window closed by the executor's shared consumer budget. */
  val THROTTLE_CAUSE_EXECUTOR_QUOTA: String =
    "the executor's shared consumer receive budget is fully committed"

  val PRODUCER_CLOSED_REASON: String = "the producer closed the connection"

  val CONSUMER_INBOUND_DESCRIPTION: String =
    StreamingShuffleMessageType.DATA_BLOCK.name() + ", " +
      StreamingShuffleMessageType.HEARTBEAT.name() + " or " +
      StreamingShuffleMessageType.STREAM_TERMINATION.name()

  /** What the pipeline is expected to deliver, named when it delivers something else entirely. */
  val STREAMING_MESSAGE_DESCRIPTION: String = classOf[StreamingShuffleMessage].getName

  /** What charging one replay attempt against a window's budget decided. */
  sealed abstract class ReplayVerdict

  object ReplayVerdict {

    /** The attempt is granted; the request may be written now. */
    final case class Granted(attempt: Int) extends ReplayVerdict

    /** The budget is intact but the exponential pause has not elapsed. */
    case object Deferred extends ReplayVerdict

    /** Five attempts have been spent on this window. */
    case object Exhausted extends ReplayVerdict
  }

  /**
   * The replay budget of one inclusive range of block positions.
   *
   * @param first lowest position the range covers, inclusive
   * @param last highest position the range covers, inclusive
   */
  private[streaming] final class ReplayWindow(val first: Long, val last: Long) {

    private val attemptCount = new AtomicInteger(0)

    private val nextAttemptAtMillis = new AtomicLong(0L)

    /** Attempts already spent on this range. */
    def attempts: Int = attemptCount.get()

    /** The instant from which the next attempt is permitted. */
    def nextAttemptAt: Long = nextAttemptAtMillis.get()

    /** Whether the pause has elapsed and the budget is intact. */
    def isDue(nowMillis: Long): Boolean =
      attemptCount.get() < BackpressureProtocol.MAX_RETRY_ATTEMPTS &&
        nowMillis >= nextAttemptAtMillis.get()

    /** Spends one attempt if the budget and the schedule both allow it. */
    def charge(nowMillis: Long): ReplayVerdict = {
      var verdict: ReplayVerdict = null
      while (verdict == null) {
        val spent = attemptCount.get()
        if (spent >= BackpressureProtocol.MAX_RETRY_ATTEMPTS) {
          verdict = ReplayVerdict.Exhausted
        } else if (nowMillis < nextAttemptAtMillis.get()) {
          verdict = ReplayVerdict.Deferred
        } else if (attemptCount.compareAndSet(spent, spent + 1)) {
          nextAttemptAtMillis.set(
            nowMillis + BackpressureProtocol.retryBackoffMillis(spent + 1))
          verdict = ReplayVerdict.Granted(spent + 1)
        }
      }
      verdict
    }
  }

  /** One inbound event on the way from the channel to the reduce task. */
  sealed abstract class Inbound {
    /** The reduce partition this event belongs to, or [[UNKNOWN_PARTITION_ID]]. */
    def partitionId: Int
  }

  /** A verified block, carried exactly as the codec produced it. */
  final case class BlockReceived(block: DataBlockMessage) extends Inbound {
    override def partitionId: Int = block.partitionId()
  }

  /** The orderly end of one partition's stream. */
  final case class StreamCompleted(partitionId: Int, totalBlocks: Long) extends Inbound

  /**
   * The producer of one partition is gone and nothing further is coming.
   *
   * @param partitionId the partition that will receive nothing further, or
   *     [[StreamingShuffleClientHandler.UNKNOWN_PARTITION_ID]] when the loss belongs to the channel
   *     rather than to one stream
   * @param reason short operator-facing description of why the producer is considered gone
   * @param cause the throwable that revealed the loss, or null when the loss was observed as an
   *     orderly channel close rather than as a failure.
   * @param invalidation how the loss should be attributed when the reader invalidates what it
   *     took from this producer.
   */
  final case class ProducerLost(
      partitionId: Int,
      reason: String,
      cause: Throwable,
      invalidation: StreamingShuffleInvalidationReason =
        StreamingShuffleInvalidationReason.ConnectionTimeout)
    extends Inbound

  /** Per-partition bookkeeping for one channel. */
  private[streaming] class PartitionState(val partitionId: Int) {

    /** Next block position expected, so a gap or a duplicate is detected rather than spliced. */
    val expectedSequenceNumber = new AtomicLong(0L)

    val highestReceivedSequenceNumber = new AtomicLong(NO_BLOCK_TOTAL)

    /** Payload bytes accepted for this partition. */
    val receivedBytes = new AtomicLong(0L)

    /** Payload bytes covered by the acknowledgements already sent. */
    val acknowledgedBytes = new AtomicLong(0L)

    /**
     * Framed payload size of every accepted block that has not yet been acknowledged, by position.
     */
    private val receivedBytesBySequence =
      new ConcurrentSkipListMap[java.lang.Long, java.lang.Long]()

    /** Positions awaiting a replacement block, because this consumer asked for them again. */
    private val quarantined = new ConcurrentSkipListSet[java.lang.Long]()

    /** The replay budget of each requested range, keyed by the range's first position. */
    private val replayWindows = new ConcurrentHashMap[java.lang.Long, ReplayWindow]()

    /**
     * An end-of-stream this consumer has been told about but cannot apply yet, or
     * [[StreamingShuffleClientHandler.NO_BLOCK_TOTAL]] when there is none.
     */
    private val deferredTermination = new AtomicLong(NO_BLOCK_TOTAL)

    val deferredTerminations = new AtomicLong(0L)

    /**
     * Records a held end-of-stream total, or refuses one that contradicts the total already held.
     *
     * @return true when the total is the one being held afterwards
     */
    def deferTermination(totalBlocks: Long): Boolean =
      deferredTermination.compareAndSet(NO_BLOCK_TOTAL, totalBlocks) ||
        deferredTermination.get() == totalBlocks

    /** The held end-of-stream total, or the sentinel when none is held. */
    def deferredTerminationTotal: Long = deferredTermination.get()

    /**
     * Whether a repair of this partition is still outstanding: a position is quarantined awaiting a
     * replacement, or a replay window has been recorded and not yet discarded.
     */
    def hasOutstandingRepairs: Boolean = !quarantined.isEmpty || !replayWindows.isEmpty

    val acknowledgedPosition = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    /**
     * An acknowledgement held back because the channel was not writable, coalesced to one position:
     * acknowledgements are monotonic, so a later position supersedes an earlier one outright and
     * what is held is a single number rather than a backlog of messages.
     */
    val pendingAckPosition = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    /** Instant of the last inbound event of any kind, from the injected clock. */
    val lastInboundMillis = new AtomicLong(NO_TIMESTAMP)

    /** Instant at which this consumer last emitted a heartbeat, from the injected clock. */
    val lastHeartbeatSentMillis = new AtomicLong(NO_TIMESTAMP)

    /** Local instant at which the producer's last heartbeat arrived. */
    val remoteHeartbeatMillis = new AtomicLong(NO_TIMESTAMP)

    /** Whether the producer has announced the orderly end of this stream. */
    val terminated = new AtomicBoolean(false)

    /** Blocks the producer claims to have sent, or [[NO_BLOCK_TOTAL]] before it has said. */
    val announcedBlocks = new AtomicLong(NO_BLOCK_TOTAL)

    val rejectedTerminations = new AtomicLong(0L)

    val duplicateTerminations = new AtomicLong(0L)

    val blocksAfterTermination = new AtomicLong(0L)

    /** Replacements admitted after this stream had ended. */
    val repairsAfterTermination = new AtomicLong(0L)

    /** Latch ensuring completion is delivered to the task exactly once. */
    val completionSignalled = new AtomicBoolean(false)

    /** Latch ensuring producer loss is delivered to the task exactly once. */
    val producerLostSignalled = new AtomicBoolean(false)

    val acceptedBlocks = new AtomicLong(0L)

    val corruptBlocks = new AtomicLong(0L)

    val duplicateBlocks = new AtomicLong(0L)

    val repairedGaps = new AtomicLong(0L)

    val replayRequests = new AtomicLong(0L)

    val quotaRefusals = new AtomicLong(0L)

    /** Records one accepted block's payload size against its position. */
    def recordReceived(sequenceNumber: Long, payloadBytes: Long): Long = {
      val previous = receivedBytesBySequence.put(
        java.lang.Long.valueOf(sequenceNumber), java.lang.Long.valueOf(payloadBytes))
      val superseded = if (previous == null) 0L else previous.longValue()
      val delta = payloadBytes - superseded
      if (delta != 0L) {
        receivedBytes.addAndGet(delta)
      }
      superseded
    }

    /**
     * Credits the bytes of every accepted block at or below one position and forgets them.
     *
     * @return how many bytes the acknowledgement released
     */
    def releaseThrough(consumedSequenceNumber: Long): Long = {
      var released = 0L
      val prefix = receivedBytesBySequence.headMap(
        java.lang.Long.valueOf(consumedSequenceNumber), true)
      var entry = prefix.pollFirstEntry()
      while (entry != null) {
        released += entry.getValue.longValue()
        entry = prefix.pollFirstEntry()
      }
      if (released > 0L) {
        acknowledgedBytes.addAndGet(released)
      }
      released
    }

    /** Bytes of accepted blocks that no acknowledgement has covered yet. */
    def outstandingBytes: Long = math.max(0L, receivedBytes.get() - acknowledgedBytes.get())

    /**
     * Takes every byte this ledger still holds and empties it, in one step.
     *
     * @return the bytes that were outstanding, which the caller now owns the release of
     */
    def drainOutstandingBytes(): Long = {
      receivedBytesBySequence.clear()
      val outstanding = math.max(0L, receivedBytes.get() - acknowledgedBytes.getAndSet(
        receivedBytes.get()))
      outstanding
    }

    /**
     * Marks an inclusive range of positions as awaiting a replacement.
     *
     * @return true if every position in the range is now quarantined
     */
    def quarantine(first: Long, last: Long): Boolean = {
      if (quarantined.size() + (last - first + 1L) > MAX_QUARANTINED_POSITIONS.toLong) {
        false
      } else {
        var position = first
        while (position <= last) {
          quarantined.add(java.lang.Long.valueOf(position))
          position += 1L
        }
        true
      }
    }

    /** Whether one position is awaiting a replacement block. */
    def isQuarantined(sequenceNumber: Long): Boolean =
      quarantined.contains(java.lang.Long.valueOf(sequenceNumber))

    /** Whether any position of an inclusive range is still awaiting a replacement block. */
    def isRangeQuarantined(first: Long, last: Long): Boolean =
      !quarantined.subSet(
        java.lang.Long.valueOf(first), true, java.lang.Long.valueOf(last), true).isEmpty

    /**
     * Clears one position's quarantine.
     *
     * @return true if the position had been awaiting a replacement, which tells the caller that
     *     the block just accepted is a replacement and must not move the sequence cursor
     */
    def releaseQuarantine(sequenceNumber: Long): Boolean =
      quarantined.remove(java.lang.Long.valueOf(sequenceNumber))

    /** Positions of this partition awaiting a replacement block. */
    def quarantinedCount: Int = quarantined.size()

    /** The replay budget of one range, created on first request for it. */
    def replayWindowFor(first: Long, last: Long): ReplayWindow = {
      val key = java.lang.Long.valueOf(first)
      val existing = replayWindows.get(key)
      if (existing != null && existing.last >= last) {
        existing
      } else {
        val created = new ReplayWindow(first, math.max(first, last))
        val raced = replayWindows.put(key, created)
        if (raced != null && raced.last >= created.last) raced else created
      }
    }

    /** The ranges whose pause has elapsed and whose budget is intact, in a deterministic order. */
    def dueReplayWindows(nowMillis: Long): Seq[ReplayWindow] =
      replayWindows.values().asScala.filter(_.isDue(nowMillis)).toSeq.sortBy(_.first)

    /** Forgets one range's budget, together with the quarantine it was created for. */
    def discardReplayWindow(window: ReplayWindow): Unit = {
      replayWindows.remove(java.lang.Long.valueOf(window.first), window)
      var position = window.first
      while (position <= window.last) {
        quarantined.remove(java.lang.Long.valueOf(position))
        position += 1L
      }
    }
  }
}
