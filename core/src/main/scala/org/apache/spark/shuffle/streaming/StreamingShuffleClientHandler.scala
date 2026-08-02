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
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap, ConcurrentSkipListSet, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener}

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys.{BLOCK_ID, CHECKSUM, CONFIG, COUNT, ERROR, HOST_PORT, MAP_ID, MAX_ATTEMPTS, NUM_BYTES, NUM_EVENTS, NUM_SKIPPED, PARTITION_ID, REASON, SHUFFLE_ID, THRESHOLD, TIMEOUT, VALUE}
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.protocol.OneWayMessage
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, DataBlockMessage, HeartbeatMessage, RetransmitRequestMessage, StreamingShuffleChecksum, StreamingShuffleMessage, StreamingShuffleMessageType, StreamTerminationMessage}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.util.{Clock, SystemClock}

/**
 * The consumer side of a streaming shuffle channel: the transport handler that accepts blocks a
 * producer pushes, hands them to the reduce task, acknowledges what the task has consumed, repairs
 * bounded corruption by asking for a replay, and exerts TCP-level backpressure by turning the
 * channel's `autoRead` off and on again.
 *
 * <b>Why `autoRead` lives here, and only here.</b> Toggling `autoRead` is a new idiom in this code
 * base: it appears nowhere in the shared transport module and nowhere else in Spark core. This file
 * is its sole owner -- the producer-side handler never touches it, and participates in flow control
 * only through the protocol -- and confining the idiom to one file is what satisfies the directive
 * to prefer the least modification to the network transport layer. No shared transport class is
 * modified by this feature: `TransportContext`, its pipeline initialisation, `TransportConf` and
 * `SparkTransportConf` are all consumed exactly as they stand. The one piece of transport
 * configuration this handler needs -- an independent tuning namespace -- is obtained by passing a
 * module name as an argument to `SparkTransportConf.fromSparkConf`, which yields
 * `spark.shuffle-streaming.io.*` without a single line added to a shared file. That is also why OS
 * level keepalive can be enabled for streaming alone: it is a per-module boolean, and the transport
 * layer exposes no keepalive interval, so the five-second liveness bound this subsystem promises is
 * enforced at application level by the heartbeat timer rather than by TCP.
 *
 * <b>How the three layers of backpressure cooperate.</b> The streaming shuffle holds a producer
 * back in three ways at once, and this handler owns the third of them.
 *
 *  - Application-level credit. [[BackpressureProtocol]] keeps a per-stream ledger and answers
 *    `hasCredit`; a producer whose credit is spent must stop and wait for an acknowledgement.
 *  - Rate limiting. A token bucket paces egress and is consulted by the producer, never here.
 *  - TCP-level throttling. When credit is spent, or when this handler's own hand-off queue is at
 *    its high-water mark, `autoRead` is turned off: Netty stops reading from the socket, the
 *    receive window closes, and the pressure reaches the producer through TCP itself rather than
 *    through any message this subsystem has to invent. An acknowledgement that advances the
 *    consumer position, or a queue that has drained, turns it back on.
 *
 * Every transition into the throttled state belongs to [[BackpressureProtocol]], which owns the
 * `shuffle.streaming.backpressureEvents` counter and advances it from the one place a throttling
 * episode begins. This handler therefore reports the edge to the protocol and deliberately
 * increments no metric of its own: the throttling of a producer and the closing of a consumer's
 * receive window are two faces of one episode, and counting both would report an operator a number
 * twice the size of the thing it names.
 *
 * <b>The thread model, which is binding.</b> Two kinds of thread touch this class.
 *
 *  - A Netty event-loop thread runs every `channel*` callback. It may decode a frame, update the
 *    lock-free bookkeeping, enqueue a block and write an acknowledgement, and it may do nothing
 *    else. In particular it never calls a `ShuffleReadMetricsReporter` method -- that interface
 *    documents itself as single-threaded -- it never parks, blocks or sleeps, and it never lets an
 *    exception escape a callback.
 *  - The reduce task's own thread drains the queue, performs the authoritative checksum
 *    verification, updates the read metrics and throws. A failure this handler observes on an I/O
 *    thread is handed to [[StreamingShuffleErrorNotifier]], whose first-error-wins latch the task
 *    thread reads inside `read()`. Without that bridge a failure seen only on an I/O thread would
 *    be swallowed and the task would wait forever for input that is never going to arrive, because
 *    `ShuffleReader` exposes only `read()` and its `stop()` is commented out.
 *
 * Because that trait has no lifecycle hook, release is not something this handler can schedule for
 * itself. It exposes an idempotent [[close]] instead, which the reader registers on
 * `TaskContext.addTaskCompletionListener` so that it runs on success, on failure and on
 * cancellation alike. No task-completion listener is ever registered from a Netty thread.
 *
 * <b>Framing, and why this is an `RpcHandler`.</b> Every streaming frame travels as the body of a
 * one-way RPC on Spark's own transport, in both directions, which is what makes the producer side
 * and the consumer side two ends of one protocol rather than two codecs that have to be kept in
 * step. The transport owns the length prefix, the optional encryption and the frame accounting; the
 * body is exactly what `StreamingShuffleMessage.toByteBuffer` produces and what
 * `StreamingShuffleMessage.Decoder` consumes, so this handler decodes a whole message and never a
 * partial one. Nothing here reassembles bytes, and nothing here holds a reference-counted buffer:
 * the transport releases the body as soon as `receive` returns, and the decoder allocates a heap
 * array of its own for a block's payload, so a decoded block is safe to hand to the task thread.
 *
 * Messages are discriminated on the concrete class, which is the type byte by another name, and
 * never on length: three of the four control messages encode to the same number of bytes, and the
 * fourth varies with the identity it carries, so length distinguishes nothing at all.
 *
 * One instance serves one producer channel and holds per-channel state, so it is never shared
 * between channels. Any number of reduce partitions of one shuffle may be multiplexed on the
 * channel; the partition each frame belongs to is read from its header and is accepted only if it
 * falls inside the range this reduce task asked for, which is what bounds the state a remote peer
 * can provoke.
 *
 * @param conf the executor's configuration, read once at construction and then held immutably,
 *             which is what makes "a configuration change requires an executor restart" true by
 *             construction
 * @param shuffleId the shuffle this channel serves; every frame that names another shuffle is
 *                  refused at the boundary
 * @param mapId the map output this channel carries, which together with the task attempt is the
 *              producer generation the receiving ledger is keyed by
 * @param taskAttemptId the producing attempt, so a superseded attempt cannot share accounting with
 *                      the attempt that replaced it
 * @param consumerId this consumer session's identity, which is what the producer's retained store
 *                   keys its per-consumer acknowledgement cursor by
 * @param startPartition the first reduce partition this task reads, inclusive. Together with
 *                       `endPartition` it is the *authenticated* request: a frame naming a
 *                       partition outside it is refused before any state is allocated for it, so
 *                       the metadata a remote peer can cause this handler to hold is bounded by
 *                       what the task itself asked for rather than by what the peer chooses to send
 * @param endPartition one past the last reduce partition this task reads
 * @param backpressure the flow-control protocol that owns the credit ledger, the liveness timers
 *                     and the backpressure-event counter; must not be null
 * @param errorNotifier the bridge that carries a failure observed on this I/O thread to the task
 *                      thread; must not be null
 * @param clock the single time source for every timer in this class, so each one advances with
 *              that clock rather than with wall time and nothing here sleeps
 */
private[spark] class StreamingShuffleClientHandler(
    conf: SparkConf,
    shuffleId: Int,
    mapId: Long,
    taskAttemptId: Long,
    consumerId: String,
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
   *
   * Every component is load bearing. The shuffle and the partition alone would merge the flows
   * arriving from every map task of the shuffle into one credit window; the producer generation --
   * map id and task attempt id together -- keeps a superseded attempt from sharing accounting with
   * the attempt that replaced it; the consumer session keeps a reconnecting reduce task from
   * inheriting the receive window of the session it replaced. The role makes this receiving ledger
   * distinct from the producer's sending ledger even when a shuffle is entirely local to this JVM,
   * which it always is under `local[*]`.
   */
  private def consumerKey(partitionId: Int): BackpressureStreamKey =
    BackpressureStreamKey.forConsumer(shuffleId, mapId, taskAttemptId, partitionId, consumerId)

  // Read once and held immutably. This flag is the single authority over every verbose line this
  // handler writes, so an operator who leaves spark.shuffle.streaming.debug at its default of false
  // sees nothing per block and nothing per autoRead toggle even when logging is globally set to
  // DEBUG. That is what keeps the subsystem inside its budget of under 10 MB per hour per executor,
  // because autoRead can flap once per throttling episode on every channel.
  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  /**
   * Cumulative block repairs on this channel, and the window that bounds reporting them.
   *
   * A gap in the sequence and a block that fails its checksum are both per-block conditions, and a
   * stream that is systematically wrong rather than occasionally unlucky exhibits them once per
   * block. Reporting each occurrence would make the log volume of a corrupt shuffle equal to the
   * block count of that shuffle. One window covers both, because they are the same fact from an
   * operator's point of view -- this consumer had to ask for output again -- and each admitted
   * report names which of them it was. The terminal case, a block that can no longer be replayed at
   * all, is reported unconditionally: there is one of those per partition and it ends the read.
   */
  private val repairsRequested = new AtomicLong(0L)
  private val repairLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /**
   * Emits one repair report through the window, or accounts it and traces it under the debug key.
   */
  private def reportRepair(entry: => MessageWithContext): Unit = {
    val occurrences = repairsRequested.incrementAndGet()
    repairLogGate.admit(clock.getTimeMillis()) match {
      case Some(unreported) =>
        logWarning(entry +
          log" (${MDC(NUM_EVENTS, occurrences)} repair(s) on this channel, " +
          log"${MDC(NUM_SKIPPED, unreported)} not reported individually)")
      case None =>
        if (debugEnabled) {
          logDebug(entry)
        }
    }
  }

  // Transport tuning for streaming shuffle alone, built by the SAME function the producer side
  // uses. The module name is an ARGUMENT, never an entry added to a shared file, and the builder
  // clones the configuration before touching it, so asking for it here perturbs nothing that block
  // transfer relies on. The result is an independent spark.shuffle-streaming.io.* namespace whose
  // thread counts, retry policy and keepalive setting can be tuned without any effect on the
  // existing shuffle transport.
  //
  // Going through the shared builder rather than calling fromSparkConf directly is what makes OS
  // level keepalive true for streaming on BOTH ends: the builder sets the module's keepalive key
  // before reading it, so producer and consumer cannot be configured differently, and a consumer
  // can no longer merely observe a default of false while the producer has it on. Enabling it is a
  // per-module boolean and the transport exposes no keepalive INTERVAL -- the JDK has no such
  // socket option -- so it is a coarse idle-connection reaper beneath the five-second application
  // level heartbeat, never a substitute for it.
  private val transportConf: TransportConf =
    StreamingShuffleServerHandler.streamingTransportConf(conf)

  // Replay budget for a corrupt or missing block inside the retained window, taken from the
  // streaming module's own retry knobs rather than from a constant of this file's invention. The
  // protocol caps attempts at five with an exponential pause; these two bound the transport-level
  // retries that sit underneath that.
  private val maxIoRetries: Int = transportConf.maxIORetries()

  private val ioRetryWaitMillis: Int = transportConf.ioRetryWaitTimeMs()

  // OS-level keepalive removes a connection that has been idle too long, but the transport exposes
  // it as a boolean with no interval and the JDK exposes no keepalive-interval socket option, so it
  // cannot be the five-second detector this subsystem promises. It is recorded here so a diagnostic
  // can state which of the two mechanisms is in play, and the five-second bound is enforced by the
  // heartbeat timer below.
  private val tcpKeepAliveEnabled: Boolean = transportConf.enableTcpKeepAlive()

  // The hand-off queue between the event loop and the task thread. Bounded, because an unbounded
  // one would turn a slow reduce task into unbounded retained heap; the high-water mark below is
  // what keeps it from ever reaching the bound, since crossing that mark closes the receive window
  // and the producer's credit limit caps what can already be in flight.
  private val inbound = new LinkedBlockingQueue[Inbound](INBOUND_QUEUE_CAPACITY)

  // Payload bytes currently sitting in that queue. Counted as well as the events themselves because
  // a block may carry anything from nothing to two mebibytes, so an event count alone is a poor
  // description of the heap the queue is holding.
  private val queuedPayloadBytes = new AtomicLong(0L)

  // Per-partition bookkeeping. Written only by this channel's event-loop thread, which serialises
  // every mutation, and read by the task thread, which is why each cell is an atomic: the atomics
  // are here for visibility across the two threads, not to order writes against each other.
  private val partitions = new ConcurrentHashMap[Integer, PartitionState]()

  // The channel this handler is attached to, captured as early as Netty offers it so that the task
  // thread can acknowledge, request a replay and close without having to be handed a context.
  private val channelRef = new AtomicReference[Channel](null)

  // Current autoRead state as this handler believes it to be. Every setAutoRead call in this file
  // is guarded by a compare-and-set on this cell, so a redundant call is skipped and the toggle is
  // idempotent no matter which thread reaches it.
  private val autoReadEnabled = new AtomicBoolean(true)

  // Transitions into the throttled state observed on this channel. The executor-wide
  // shuffle.streaming.backpressureEvents counter is NOT advanced from here -- the protocol owns it
  // -- so this local total exists for assertions that need to speak about one channel.
  private val throttleTransitions = new AtomicLong(0L)

  private val throttleCause = new AtomicReference[String](null)

  // Instant at which this channel went inactive with work outstanding, read from the injected
  // clock. NO_TIMESTAMP while the producer is believed alive.
  private val producerLostAtMillis = new AtomicLong(NO_TIMESTAMP)

  // Warn lines already spent on escalated failures. A peer sending malformed frames produces one
  // failure per frame, so the warn level is spent on the first few and everything after that is
  // debug: the notifier retains the root cause either way, and the count is observable.
  private val escalationsReported = new AtomicLong(0L)

  // Netty identity of the one channel this handler is bound to, latched on the first callback the
  // transport makes and never replaced. This is the capability binding: a handler is created per
  // producer generation and connected to exactly one authenticated socket, so the channel's
  // identity *is* the generation's identity on the wire, which no field of the protocol header
  // carries. Null until the first callback arrives.
  private val boundChannelId = new AtomicReference[String](null)

  // Callbacks refused because they arrived on a channel other than the bound one. Counted rather
  // than escalated: a stray callback is evidence of a routing fault somewhere else, and failing
  // this reduce task over it would turn another component's mistake into a recomputed stage.
  private val foreignChannelCallbacks = new AtomicLong(0L)

  // One-shot latch for the warn line that reports the first refused foreign callback.
  private val foreignChannelReported = new AtomicBoolean(false)

  // Frames dropped because they named a shuffle or a partition this handler does not serve. Counted
  // rather than escalated, because a producer that routed a block to the wrong consumer has made a
  // routing mistake and not a protocol violation.
  private val misaddressedFrames = new AtomicLong(0L)

  // One-shot latch for the warn line that reports the first misaddressed frame, so that a peer
  // sending nothing but misaddressed frames cannot spend the executor's whole log budget.
  private val misaddressReported = new AtomicBoolean(false)

  // Protocol revision of the first frame this build could not read, or NO_OBSERVED_VERSION. Held
  // as an int so that the sentinel cannot collide with any legal byte value. It is recorded rather
  // than only thrown because a version disagreement is one of the four conditions under which
  // streaming yields to the sort-based shuffle, and the reader has to be able to attribute a failed
  // read to that rather than to a timeout.
  private val observedIncompatibleVersion = new AtomicInteger(NO_OBSERVED_VERSION)

  // Latch for the channel-level producer-loss marker, which belongs to no single partition. A
  // per-partition failure uses that partition's own latch; this one keeps a channel-wide failure
  // from being reported once per frame while still reporting it at least once.
  private val channelLostSignalled = new AtomicBoolean(false)

  private val closed = new AtomicBoolean(false)

  // Transport callbacks. Every one of them runs inside guard(...), carries an explicit result type,
  // never lets an exception escape and never performs work that could park an event-loop thread.

  /**
   * A consumer channel serves no chunked stream, so the stream manager offered here is an empty
   * one. It is a real instance rather than null because the transport dereferences it
   * unconditionally when a stream request arrives, and answering "no such stream" is the correct
   * response to a request this subsystem never invites.
   */
  private val streamManager = new OneForOneStreamManager()

  override def getStreamManager(): StreamManager = streamManager

  /**
   * Consumes one streaming frame that arrived as a one-way message, which is the shape every frame
   * of this protocol travels in.
   */
  override def receive(client: TransportClient, message: ByteBuffer): Unit = {
    guard(consumeFrame(client, message))
  }

  /**
   * Consumes one streaming frame that arrived as a request expecting a reply.
   *
   * A producer has no need for this shape, but a transport peer may use it, and answering is
   * cheaper than refusing: the frame is handled exactly as a one-way frame and the reply is empty.
   * Replying is what stops the peer from waiting out its RPC timeout for an answer this protocol
   * never intended to send.
   */
  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    guard(consumeFrame(client, message))
    callback.onSuccess(ByteBuffer.allocate(0))
  }

  /**
   * Binds the channel and announces this consumer's subscription on it.
   *
   * The announcement is not optional and it is not a nicety: the producer has no request message of
   * its own, so a heartbeat naming the partition and stating the position this consumer has reached
   * *is* the subscription, and it is also the resume handshake. Until it arrives the producer has
   * allocated nothing for this channel and will send nothing down it, which is exactly the bound on
   * remotely provoked state that the producer side enforces. Sending it here, on the first callback
   * the transport raises for a live channel, is what makes the in-progress request happen while the
   * map stage is still producing rather than after the reduce task's first poll.
   *
   * One heartbeat is sent per partition in the requested range, and each carries that partition's
   * own position, so a reconnecting consumer resumes stream by stream from where it left off.
   */
  override def channelActive(client: TransportClient): Unit = {
    guard {
      if (bindChannel(client)) {
        activateChannel(client)
      } else {
        rejectForeignChannel(client)
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

  /**
   * Signals producer loss.
   *
   * A channel that goes inactive can mean either of two things, and the difference matters: after
   * an orderly end of stream it is simply the socket closing behind a completed transfer, while
   * with blocks still owing it is the producer disappearing. Only the second case is a failure.
   *
   * In that second case the protocol is prompted to re-evaluate, so that its five-second connection
   * timeout is measured from an up-to-date reading rather than from whenever a poll last happened,
   * and every partition still owing data is handed a marker on the queue. That marker is what wakes
   * a reduce task blocked on input which is never going to arrive; leaving it out would leave the
   * task waiting for a full timeout on a socket that has already gone.
   */
  override def channelInactive(client: TransportClient): Unit = {
    guard {
      if (isBoundChannel(client)) {
        deactivateChannel(client)
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
      logWarning(log"Streaming shuffle producer for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} closed the connection from " +
        log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))} with " +
        log"${MDC(COUNT, outstanding.size)} stream(s) still awaiting data")
      // Prompting the protocol is the whole of the hand-off available: it owns the five-second
      // window, judges it from its own clock and re-evaluates every ledger in one non-blocking
      // pass that takes no lock and performs no I/O, so it is safe on an event-loop thread.
      backpressure.pollOnce()
      // No cause: the peer closed the connection rather than raising anything, so there is no
      // transport-level throwable to carry and the reason string is the whole of what is known.
      outstanding.foreach(partitionId =>
        signalProducerLost(partitionId, PRODUCER_CLOSED_REASON, null))
    }
  }

  /**
   * Records a channel-level failure and closes the channel.
   *
   * The notifier is the route by which the task thread learns of it. Closing the channel afterwards
   * is what causes `channelInactive` to run, which is where the streams still awaiting data are
   * marked so that a blocked reduce task wakes immediately.
   */
  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    guard {
      if (isBoundChannel(client)) {
        escalate(cause, UNKNOWN_PARTITION_ID)
        client.close()
      } else {
        // Closing is still correct -- a channel nobody owns should not be left open -- but the
        // failure belongs to whoever owns that channel and must not reach this task's notifier,
        // where it would fail a reduce task over a socket it never read from.
        rejectForeignChannel(client)
        client.close()
      }
    }
  }

  /**
   * Decodes and routes one frame, then re-evaluates everything arrival may have changed.
   *
   * The channel is bound first, because a frame can in principle reach a handler before the
   * transport has raised `channelActive` for it, and the task thread must be able to acknowledge
   * from the moment anything has been received.
   *
   * The partition is captured between the decode and the dispatch so that a failure raised by
   * either is attributed to the stream it belongs to rather than to the channel as a whole -- a
   * distinction the reader relies on, because a per-partition failure invalidates one stream while
   * a channel-level one is attributed to whichever partition the task is waiting on.
   *
   * Deferred acknowledgements are flushed and the receive window re-evaluated at the end of every
   * frame rather than on a Netty read-complete or writability callback, because an `RpcHandler` is
   * offered neither: the frame boundary is the point at which both answers can have changed, and
   * the task thread re-evaluates them again on every poll.
   */
  private def consumeFrame(client: TransportClient, frame: ByteBuffer): Unit = {
    if (!bindChannel(client)) {
      // Refused before a single byte is decoded. A frame from a channel this handler is not bound
      // to has not passed this handler's capability check, and a streaming payload reaches Spark's
      // deserialization: the per-block CRC32C detects corruption and forges trivially, so the
      // authenticated channel is the only thing standing between a peer and that deserialization.
      rejectForeignChannel(client)
      return
    }
    var partitionId = UNKNOWN_PARTITION_ID
    try {
      val message = decodeFramed(frame)
      partitionId = message.partitionId()
      dispatch(message)
    } catch {
      // A failure must never escape into the transport: it would tear the channel down instead of
      // failing the task cleanly. Recording it is what makes the task thread raise it from read().
      case NonFatal(e) =>
        escalate(e, partitionId)
    }
    flushDeferredAcks()
    evaluateAutoRead()
  }

  /**
   * Binds this handler to one channel, once, and reports whether the caller is that channel.
   *
   * <b>What this is for.</b> A streaming frame's payload is handed to Spark's deserialization, so
   * the question "may this peer send me bytes to deserialize" has to be answered before the payload
   * is read, and it cannot be answered from the frame: the protocol header carries a version, a
   * shuffle id, a partition id and a sequence number, and nothing that names a producer generation.
   * The channel answers it instead. This handler is constructed for one producer generation and
   * connected over one unmanaged, authenticated socket, so latching that socket's identity binds
   * the handler to the generation, to the capability the auth handshake established, and -- with
   * the partition range it was constructed with -- to exactly the streams the reduce task asked
   * for. All three of the bindings the review requires are then checked before a frame is decoded.
   *
   * <b>Why latch rather than follow.</b> Following the channel would defeat the purpose: a peer
   * that reached this handler on a second connection would simply become the new owner. Latching
   * makes a second channel a refusal instead. A reconnection is not affected, because a reconnect
   * builds a new handler -- the reader creates one per `connect` -- and a handler whose channel has
   * gone is closed rather than reused.
   *
   * A null channel is accepted without binding. Only a client with no channel of its own reaches
   * that path, which is a test double rather than a socket, and refusing it would make the handler
   * untestable without granting a real peer anything.
   *
   * @param client the client the transport made this callback for
   * @return true when this handler is bound to that client's channel
   */
  private def bindChannel(client: TransportClient): Boolean = {
    val channel = client.getChannel()
    if (channel == null) {
      true
    } else {
      val observed = channel.id().asLongText()
      if (boundChannelId.compareAndSet(null, observed) || boundChannelId.get() == observed) {
        channelRef.set(channel)
        true
      } else {
        false
      }
    }
  }

  /**
   * Whether a client is the one this handler is bound to, without binding it.
   *
   * Used by the callbacks that must not create a binding -- a channel going inactive, or raising an
   * exception, is not an event that should adopt it. An unbound handler answers true, because the
   * first callback for a channel may legitimately be either of those and the binding is what the
   * frame path establishes.
   */
  private def isBoundChannel(client: TransportClient): Boolean = {
    val channel = client.getChannel()
    if (channel == null) {
      true
    } else {
      val bound = boundChannelId.get()
      bound == null || bound == channel.id().asLongText()
    }
  }

  /**
   * Counts and reports one callback refused because it arrived on a foreign channel.
   *
   * The first occurrence is logged at warn and the rest are counted, so a peer that does nothing
   * but arrive on the wrong channel cannot spend the executor's log budget.
   */
  private def rejectForeignChannel(client: TransportClient): Unit = {
    foreignChannelCallbacks.incrementAndGet()
    if (foreignChannelReported.compareAndSet(false, true)) {
      logWarning(log"Streaming shuffle consumer for shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
        log"${MDC(MAP_ID, mapId)} refused a transport callback from " +
        log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))} because it arrived on a " +
        log"channel this handler is not bound to. Further occurrences are counted but not logged")
    }
  }

  /**
   * Announces this consumer on the channel, one heartbeat per partition in the requested range.
   *
   * The position announced is the *next* position this consumer expects, which is the same reading
   * the producer's own heartbeat carries, so one field means one thing in both directions. A fresh
   * consumer therefore announces zero and is served from the first block; a reconnecting one
   * announces where it stopped and is served the retained window from exactly that point.
   *
   * @return how many partitions were announced
   */
  private def announceSubscription(): Int = {
    var announced = 0
    var partitionId = startPartition
    while (partitionId < endPartition) {
      val state = partitionStateOf(partitionId)
      if (writeHeartbeat(partitionId, buildHeartbeat(state, clock.getTimeMillis()))) {
        announced += 1
      }
      partitionId += 1
    }
    announced
  }

  /**
   * Runs one transport callback, absorbing anything it raises.
   *
   * Every callback the transport makes into this handler is wrapped, and not only the ones that
   * decode a frame: an exception escaping `channelActive`, `channelInactive` or `exceptionCaught`
   * would be raised on an I/O thread with no task thread watching, so it would tear the channel
   * down while the reduce task waited out its whole timeout on input that is never going to arrive.
   * Recording it through the first-error-wins notifier instead is what makes the task raise it from
   * `read()`, which is the only place a fetch failure can legitimately be constructed.
   *
   * The result type is deliberately `Any`: several callers pass an expression that yields a value,
   * and requiring `Unit` would silently discard it behind a warning rather than at the call site.
   */
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
   *
   * Two conditions close the receive window, and either is sufficient.
   *
   *  - Consumer credit is exhausted. This is the condition the feature specifies, and the answer
   *    belongs to [[BackpressureProtocol]]: `hasCredit` is the predicate, and the bytes this
   *    handler has received but not yet acknowledged, measured against the credit limit the
   *    protocol published for the stream, is the same question asked from the consumer's own books.
   *  - The hand-off queue has reached its high-water mark. A bounded queue that filled up would
   *    leave the event loop with a block it can neither hand on nor drop, and the only alternatives
   *    at that point are to block the event loop or to lose data. Closing the window well before
   *    the bound removes the choice.
   *
   * Reopening is deliberately hysteretic: the window stays closed until the queue has drained to
   * the low-water mark and credit is available again, so that a task consuming one block at a time
   * cannot make the toggle flap once per block. Flapping would be correct but would spend the log
   * budget and the syscalls of an episode on every single block.
   *
   * Safe to call from any thread. `setAutoRead` is a channel configuration write that Netty permits
   * from any thread, the guard is a compare-and-set, and nothing here blocks -- which matters
   * because the task thread calls this after every poll and the event loop calls it after every
   * frame.
   */
  private def evaluateAutoRead(): Unit = {
    if (!closed.get()) {
      if (autoReadEnabled.get()) {
        val cause = throttleCauseNow()
        if (cause != null) {
          disableAutoRead(cause)
        }
      } else if (resumeAllowed()) {
        enableAutoRead()
      }
    }
  }

  /**
   * The reason to stop reading right now, or null when there is none. Credit is tested first
   * because it is the condition the protocol owns and the one an operator will want named.
   */
  private def throttleCauseNow(): String = {
    if (isAnyStreamCreditExhausted) {
      THROTTLE_CAUSE_CREDIT
    } else if (backpressure.receiveQuotaExhausted) {
      // Tested here as well as at admission, so an exhausted executor budget stops the producer at
      // the socket instead of being discovered one refused block at a time. It is a shared reading,
      // so a consumer whose own credit and queue are healthy still stops reading when the executor
      // as a whole has nothing left to hold a block in.
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

  /**
   * Stops reading from the socket, once per episode.
   *
   * The compare-and-set is what makes the toggle idempotent: a redundant `setAutoRead(false)` is
   * skipped, so the transition is counted once and logged at most once however many frames or polls
   * observe the same exhausted stream. The protocol is told about the edge because it owns both the
   * throttled state and the `shuffle.streaming.backpressureEvents` counter, and it cannot observe a
   * closed receive window for itself: its poll confirms a return to flowing but never an entry into
   * throttling, so a consumer that stops reading would otherwise be missing from that counter
   * entirely. This handler still advances no metric of its own, since a throttled producer and a
   * closed receive window are one episode seen from two ends.
   */
  private def disableAutoRead(cause: String): Unit = {
    if (autoReadEnabled.compareAndSet(true, false)) {
      throttleCause.set(cause)
      throttleTransitions.incrementAndGet()
      applyAutoRead(enabled = false)
      backpressure.pollOnce()
      // Reported after the poll, so a stale episode is closed before this one is opened. Only the
      // partitions actually in flight on this channel are named: an unknown stream is ignored, and
      // this runs on an event-loop thread, so the scan stays bounded by what is being consumed.
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

  /**
   * Resumes reading from the socket, once per episode, under the same compare-and-set guard.
   *
   * Closing the episode in the protocol is what keeps the event counter honest: an episode left
   * open would swallow the next entry, so two throttling episodes on one stream would be counted
   * as one.
   */
  private def enableAutoRead(): Unit = {
    if (autoReadEnabled.compareAndSet(false, true)) {
      throttleCause.set(null)
      applyAutoRead(enabled = true)
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
   * The single point at which this file touches `setAutoRead`, so that the containment claim in the
   * class documentation is checkable by inspection. A channel that has already gone is not an error
   * here: the flag has been moved either way, and there is nothing left to read from.
   */
  private def applyAutoRead(enabled: Boolean): Unit = {
    val channel = channelRef.get()
    if (channel != null) {
      channel.config().setAutoRead(enabled)
    }
  }

  /**
   * Whether any stream still expecting data has spent its credit.
   *
   * Iterates rather than allocating, because this runs after every frame. A stream whose producer
   * has announced the end of its output is skipped: withholding traffic from a stream that has
   * nothing further to send would close the window for no reason and delay the streams sharing the
   * channel with it.
   */
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

  /**
   * Whether one stream has spent its credit, asked in both of the ways available.
   *
   * `hasCredit` is the protocol's own answer and is authoritative wherever a ledger tracks the
   * outstanding volume. The second test is the same question from the consumer's books: bytes
   * received but not yet acknowledged, against the limit the protocol published. Both are consulted
   * because the ledger of a stream is kept by whichever side is accounting for it, and a consumer
   * that waited for the producer's view of its own backlog would close the window too late.
   */
  private def isCreditExhausted(state: PartitionState): Boolean = {
    val limit = backpressure.creditLimitBytes(consumerKey(state.partitionId))
    val outstanding = state.outstandingBytes
    !backpressure.hasCredit(consumerKey(state.partitionId)) ||
      (limit > 0L && outstanding >= limit)
  }

  // Decoding and dispatch.

  /**
   * Declines one block because this executor's shared consumer budget is fully committed.
   *
   * Declining is not dropping. The position is quarantined, which is the same state a corrupt or
   * missing block leaves behind, so the replay protocol asks the producer for it again on its own
   * backoff schedule; and the sequence cursor is left where it was, so nothing downstream can
   * conclude that the position was consumed. The producer still holds the block -- it has not been
   * acknowledged -- so the replay is servable from the retained window rather than requiring a
   * recomputation.
   *
   * If the quarantine itself cannot be recorded, the position has nothing left that would ask for
   * it again, and that is a failure rather than a state to be tidied: it is escalated so the reduce
   * task fails cleanly instead of waiting out a producer timeout for a block nobody will request.
   *
   * The receive window is re-evaluated by the caller at the end of the frame, which is where the
   * exhausted budget turns `autoRead` off and stops the producer at the socket.
   */
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

  /**
   * Settles the compatibility question before committing to a decode, then decodes.
   *
   * The version is read with `peekProtocolVersion`, which is absolute and non-consuming: the
   * buffer's position, limit and mark are all left exactly as they were, so the very same buffer is
   * handed to the decoder below. A mismatch is therefore an explicit check and never a parse
   * failure blamed on the wrong field.
   *
   * A mismatch is reported to the protocol, which latches it as the fourth condition under which
   * streaming steps aside in favour of sort-based shuffle -- the fallback policy reads the latched
   * reasons from there. It is then raised rather than swallowed: silently dropping the frame would
   * leave the reduce task waiting on input this executor cannot decode, so the exception naming
   * both revisions is allowed to propagate to this handler's own recorder, which fails the task and
   * lets the manager delegate.
   */
  private def decodeFramed(framed: ByteBuffer): StreamingShuffleMessage = {
    val version = StreamingShuffleMessage.peekProtocolVersion(framed)
    if (!StreamingShuffleMessage.isCompatible(version)) {
      observedIncompatibleVersion.compareAndSet(NO_OBSERVED_VERSION, version.toInt)
      backpressure.observeProtocolVersion(consumerKey(startPartition), version)
      StreamingShuffleMessage.checkProtocolVersion(version)
    }
    StreamingShuffleMessage.Decoder.fromByteBuffer(framed)
  }

  /**
   * The protocol revision of the first frame this build could not read, or `None` if every frame so
   * far has been legible.
   *
   * Published rather than only thrown because the reader must be able to attribute a failed read to
   * a version disagreement -- the fourth of the documented fallback conditions -- instead of to the
   * connection timeout that a channel closing behind an undecodable frame would otherwise look
   * like.
   */
  def incompatibleProtocolVersion: Option[Byte] = {
    val observed = observedIncompatibleVersion.get()
    if (observed == NO_OBSERVED_VERSION) None else Some(observed.toByte)
  }

  /**
   * Routes one decoded message, after proving it is addressed to a stream this task reads.
   *
   * Three boundary checks run before any handler sees the message, and the order is load bearing.
   *
   *  1. Addressing. The shuffle must be this channel's shuffle and the partition must fall inside
   *     the range this reduce task asked for. Both are checked against values this executor holds,
   *     never against a field of the frame compared with itself, so a peer cannot name its way into
   *     a stream nobody requested. A misaddressed frame is counted and dropped here, before
   *     [[partitionStateOf]] is reached, which is what keeps the per-partition metadata bounded by
   *     the task's own request instead of by whatever partition ids a remote peer chooses to send.
   *  2. Protocol revision, checked again on the decoded message so that a message reaching this
   *     method by any route is held to the same rule as bytes decoded above.
   *  3. Type. Dispatch is on the concrete class, which is the type byte by another name. Length is
   *     never consulted: the acknowledgement, heartbeat, retransmission-request and termination
   *     messages all encode to the same number of bytes, so a codec that discriminated on length
   *     would read one as another.
   *
   * Only two of the four control messages belong on a consumer channel. An acknowledgement and a
   * retransmission request are messages this handler *sends*; receiving one means the peer has this
   * channel's direction backwards, and it is refused with the typed unexpected-type condition
   * rather than applied to a ledger it does not describe.
   */
  private def dispatch(message: StreamingShuffleMessage): Unit = {
    if (message.shuffleId() != shuffleId || message.mapId() != mapId ||
        !servesPartition(message.partitionId())) {
      rejectMisaddressed(message)
      return
    }
    if (!backpressure.observeProtocolVersion(
        consumerKey(message.partitionId()), message.protocolVersion())) {
      StreamingShuffleMessage.checkProtocolVersion(message.protocolVersion())
    }
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

  /**
   * Whether a partition id names a stream this reduce task actually reads.
   *
   * This is the authenticated request: the range came from the shuffle reader's own arguments, and
   * the reader in turn validated the shuffle's registered partition count against its handle before
   * a channel was opened, so a partition inside this range is necessarily inside the registered
   * count as well. Asking the question against the task's own bounds -- rather than against a field
   * of the frame being validated -- is the difference between a check and a tautology.
   *
   * It is also the whole of the bound on per-partition metadata. Every allocation of a
   * [[PartitionState]] is behind this predicate, so the map can never hold more entries than the
   * range width this handler was constructed with, whatever a peer sends. That bound is entirely
   * independent of the payload bound: the hand-off queue is limited by its own capacity and
   * high-water marks, and neither limit is influenced by the other.
   */
  private def servesPartition(partitionId: Int): Boolean =
    partitionId >= startPartition && partitionId < endPartition

  /**
   * Counts and drops a frame addressed to a stream this handler does not serve.
   *
   * Dropped rather than escalated, and deliberately: a producer multiplexing several reduce ranges
   * may legitimately have routed a block to the wrong consumer, which is a routing quirk rather
   * than a protocol violation, and failing the reduce task over it would turn someone else's
   * mistake into a recomputed stage. The first occurrence is logged and the rest are counted, so a
   * peer that sends nothing but misaddressed frames cannot spend the executor's log budget.
   */
  private def rejectMisaddressed(message: StreamingShuffleMessage): Unit = {
    misaddressedFrames.incrementAndGet()
    if (misaddressReported.compareAndSet(false, true)) {
      logWarning(log"Streaming shuffle consumer for shuffle ${MDC(SHUFFLE_ID, shuffleId)} " +
        log"partitions [${MDC(COUNT, startPartition)}, ${MDC(THRESHOLD, endPartition)}) dropped " +
        log"a frame addressed to shuffle ${MDC(VALUE, message.shuffleId())} map " +
        log"${MDC(MAP_ID, message.mapId())} partition " +
        log"${MDC(PARTITION_ID, message.partitionId())}. Further occurrences are counted but not " +
        log"logged")
    }
  }

  /**
   * Accepts a data block, or repairs it.
   *
   * The order of the four steps is deliberate, and no step commits state that a later one could
   * still invalidate.
   *
   *  1. Liveness is recorded first, before anything can reject the block. Arrival is itself proof
   *     that the producer is alive, and a consumer that refreshed liveness only for a block it went
   *     on to accept would declare a healthy producer dead five seconds after the first corrupt
   *     frame.
   *  2. Sequence position, which decides whether the block is charged at all. A block below the
   *     expected position is either the replacement for a position this consumer has *quarantined*
   *     -- asked to have sent again -- or a genuine duplicate. The first is accepted, because
   *     refusing it would make the repair mechanism this very file implements incapable of ever
   *     completing; the second is discarded, because treating a duplicate as a fault would make
   *     that same mechanism the cause of a failure. It is discarded *before* its volume is charged
   *     to the stream: a duplicate names a position already acknowledged, and charging it would put
   *     back a window entry that only an acknowledgement naming that same position could release,
   *     which never arrives because acknowledgement positions only advance. Its bytes are still
   *     reported as ingress, because the link carried them. A block above the expected position, by
   *     contrast, is new volume and means blocks were lost, which is repairable while they are
   *     still retained.
   *  3. Checksum. Verified before the block is admitted, so a corrupt block is never delivered and
   *     is instead replaced by a replay. The reader performs the authoritative verification on the
   *     task thread; this one exists to make the repair possible, not to replace it.
   *  4. Commit. The block is handed to the queue *first*, and the sequence cursor and the byte and
   *     block counts advance only if that hand-off succeeded. Advancing the cursor before delivery
   *     was assured would move the expectation past a block the queue then refused, and a position
   *     the consumer has silently skipped is the one kind of loss no checksum can detect: nothing
   *     downstream would ever ask for it again.
   */
  private def handleDataBlock(block: DataBlockMessage): Unit = {
    val partitionId = block.partitionId()
    val sequenceNumber = block.sequenceNumber()
    val state = partitionStateOf(partitionId)

    state.lastInboundMillis.set(clock.getTimeMillis())

    val expected = state.expectedSequenceNumber.get()
    val payloadLength = block.payloadLength().toLong
    if (sequenceNumber < expected && !state.isQuarantined(sequenceNumber)) {
      // Liveness and the link bytes, but not the window charge and not the stream's received total.
      backpressure.onDiscardedData(consumerKey(partitionId), payloadLength)
      if (debugEnabled) {
        logDebug(log"Streaming shuffle discarded a duplicate block " +
          log"${MDC(BLOCK_ID, blockIdOf(partitionId, sequenceNumber))} of shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} because position " +
          log"${MDC(COUNT, expected)} has already been accepted")
      }
      state.duplicateBlocks.incrementAndGet()
    } else {
      backpressure.onDataReceived(consumerKey(partitionId), sequenceNumber, payloadLength)
      if (sequenceNumber > expected) {
        repairSequenceGap(state, expected, sequenceNumber)
      } else if (!block.verifyChecksum()) {
        repairCorruptBlock(state, block)
      } else {
        admitBlock(state, block)
      }
    }
  }

  /**
   * Admits one verified block: delivers it, then commits the bookkeeping that delivery earns.
   *
   * A replacement for a quarantined position does not move the cursor -- the cursor is already past
   * it -- and its byte accounting overwrites rather than adds to the entry the earlier delivery of
   * the same position left, so a replayed block cannot be counted twice.
   */
  private def admitBlock(state: PartitionState, block: DataBlockMessage): Unit = {
    val partitionId = state.partitionId
    val sequenceNumber = block.sequenceNumber()
    val payloadLength = block.payloadLength().toLong
    val replacement = state.releaseQuarantine(sequenceNumber)
    // The executor-wide budget is charged before the block is retained anywhere, and refused
    // admission is treated exactly as a failed hand-off: the sequence cursor does not move, the
    // position is quarantined, and the replay protocol asks for it again. That is what makes the
    // budget a bound on consumer heap rather than a report of how far past it this executor went --
    // the block whose admission would break the budget is the block the budget refuses.
    if (!backpressure.tryReserveReceiveQuota(payloadLength)) {
      refuseForQuota(state, sequenceNumber, payloadLength)
      return
    }
    if (enqueue(BlockReceived(block), payloadLength)) {
      // Per-sequence accounting, so that an acknowledgement releases exactly the bytes of the
      // prefix it names and never the whole of what has arrived. A replacement overwrites the
      // entry it replaces, so the quota charge above must be matched by releasing whatever that
      // position was already holding, or the two ledgers would drift by one block per repair.
      val superseded = state.recordReceived(sequenceNumber, payloadLength)
      if (superseded > 0L) {
        backpressure.releaseReceiveQuota(superseded)
      }
      if (!replacement) {
        state.expectedSequenceNumber.set(sequenceNumber + 1L)
        state.highestReceivedSequenceNumber.set(sequenceNumber)
      }
      state.acceptedBlocks.incrementAndGet()
      if (debugEnabled) {
        logDebug(log"Streaming shuffle accepted block " +
          log"${MDC(BLOCK_ID, blockIdOf(partitionId, sequenceNumber))} of " +
          log"${MDC(NUM_BYTES, payloadLength)} byte(s) for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, partitionId)}, " +
          log"replacement: ${MDC(REASON, replacement)}")
      }
    } else {
      // The hand-off failed, so nothing is holding the bytes and the charge must come back.
      backpressure.releaseReceiveQuota(payloadLength)
      if (replacement) {
        // The quarantine must stand: something has to ask for this position again, and the
        // escalation enqueue() has already recorded is what fails the task if nothing can.
        // Re-arming is what keeps "no position is silently skipped" true on this path too.
        state.quarantine(sequenceNumber, sequenceNumber)
      }
    }
  }

  /**
   * Repairs, or escalates, a gap in a partition's block sequence.
   *
   * The out-of-order block is not delivered: splicing it in ahead of the blocks that should have
   * preceded it would reorder the reduce partition, which no checksum would ever detect. The window
   * asked for instead spans the missing positions together with this one, inclusively, so the
   * producer replays the run and the consumer resumes exactly where it left off.
   */
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

  /**
   * Repairs, or escalates, a block that failed verification.
   *
   * Repair is bounded exactly by what the producer is still holding. Inside the unacknowledged
   * window the bytes exist and a single-position replay restores them. Outside it they were
   * released the moment this consumer acknowledged them, so no replay can succeed and correctness
   * has to be recovered the other way: the failure is recorded, the reduce task raises a fetch
   * failure from it, and the unmodified scheduler recomputes the upstream stage.
   *
   * The recomputation of the checksum on this path is deliberate and costs nothing on the happy
   * path, where `verifyChecksum` has already answered without copying. Here both numbers are
   * needed, because the typed condition reports the value the block arrived with alongside the
   * value this executor computed, and a diagnostic naming only one of them says nothing about the
   * other.
   */
  private def repairCorruptBlock(state: PartitionState, block: DataBlockMessage): Unit = {
    val partitionId = state.partitionId
    val sequenceNumber = block.sequenceNumber()
    val blockId = blockIdOf(partitionId, sequenceNumber)
    val expectedChecksum = block.checksum()
    val computedChecksum = StreamingShuffleChecksum.computeBlock(
      shuffleId, mapId, partitionId, sequenceNumber, block.copyPayload())
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

  /**
   * Records a producer heartbeat.
   *
   * The protocol keeps the sender's timestamp for diagnostics and judges liveness from the local
   * instant of arrival, because two hosts do not agree on the wall clock and a detector built on a
   * remote reading would mistake skew for a failure.
   */
  private def handleHeartbeat(heartbeat: HeartbeatMessage): Unit = {
    val state = partitionStateOf(heartbeat.partitionId())
    state.lastInboundMillis.set(clock.getTimeMillis())
    state.remoteHeartbeatMillis.set(heartbeat.timestampMs())
    backpressure.onHeartbeat(consumerKey(heartbeat.partitionId()), heartbeat)
    if (debugEnabled) {
      logDebug(log"Streaming shuffle received a heartbeat for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, heartbeat.partitionId())} stamped at " +
        log"${MDC(TIMEOUT, heartbeat.timestampMs())}")
    }
  }

  /**
   * Records an orderly end of stream.
   *
   * This is what distinguishes silence that means completion from silence that means failure. A
   * terminated stream stops arming the liveness timers, so a partition that legitimately produced
   * nothing at all -- announced with a total of zero blocks, which is entirely valid -- is never
   * mistaken for a producer that died five seconds ago. Completion is delivered as a marker on the
   * queue rather than left to be inferred from a timeout, so the reduce task learns of it at once.
   */
  private def handleTermination(termination: StreamTerminationMessage): Unit = {
    val partitionId = termination.partitionId()
    val totalBlocks = termination.totalBlocks()
    val state = partitionStateOf(partitionId)
    state.lastInboundMillis.set(clock.getTimeMillis())
    state.announcedBlocks.set(totalBlocks)
    state.terminated.set(true)
    backpressure.onStreamTermination(consumerKey(partitionId), termination)
    if (state.completionSignalled.compareAndSet(false, true)) {
      enqueue(StreamCompleted(partitionId, totalBlocks), 0L)
    }
    if (debugEnabled) {
      logDebug(log"Streaming shuffle stream for shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} ended after ${MDC(COUNT, totalBlocks)} block(s)")
    }
  }

  /**
   * Refuses a control message that only ever travels in the other direction. Named with the type
   * that was expected in its place, so the diagnostic states what the channel is for rather than
   * only what arrived on it.
   */
  private def refuseOutboundOnlyMessage(
      message: StreamingShuffleMessage,
      actual: StreamingShuffleMessageType): Unit = {
    escalate(
      StreamingShuffleErrors.unexpectedMessageType(CONSUMER_INBOUND_DESCRIPTION, actual.name()),
      message.partitionId())
  }

  // The hand-off queue, read by the reduce task's own thread.

  /**
   * Places one event on the hand-off queue.
   *
   * A closed handler retains nothing: the reduce task it was serving has gone, so a block arriving
   * after cleanup is dropped rather than held, which is what keeps a late frame from outliving the
   * task that asked for it.
   *
   * The queue cannot be allowed to fill, and the design ensures it does not: the receive window
   * closes at a high-water mark well below the bound, and the producer's credit limit caps what can
   * already be in flight. If it nevertheless does, the event loop has a block it can neither hand
   * on nor discard, and it must not block. The failure is recorded instead, which fails the reduce
   * task and lets the upstream stage be recomputed -- slower than a transfer that worked, but never
   * a partition that silently lost a block.
   *
   * @return true if the event was handed on, which is the condition under which the caller may
   *         commit the bookkeeping that depends on it having been delivered
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

  /**
   * Takes the next inbound event without waiting, or `None` when nothing has arrived yet.
   *
   * Intended for the reduce task's thread. Taking an event frees the heap it was holding, which is
   * the condition that may reopen the receive window, so the window is re-evaluated here rather
   * than left until the next frame arrives -- a channel throttled to a standstill has no next frame
   * to be left until.
   */
  def poll(): Option[Inbound] = accountForPolled(inbound.poll())

  /**
   * Takes the next inbound event, waiting at most `timeoutMillis` for one.
   *
   * Must never be called from a Netty event-loop thread: it is the one method in this class that
   * blocks, and it does so on purpose, for the reduce task that has nothing to do until input
   * arrives. A non-positive timeout degrades to [[poll()]].
   */
  def poll(timeoutMillis: Long): Option[Inbound] = {
    // The backoff timer of the replay protocol, driven from the one thread that is guaranteed to be
    // running while a consumer waits. A request held back for its pause goes out from here, so no
    // scheduler, thread or timer wheel has to be introduced to honour the exponential spacing.
    retryDueReplays()
    if (timeoutMillis <= 0L) {
      poll()
    } else {
      accountForPolled(inbound.poll(timeoutMillis, TimeUnit.MILLISECONDS))
    }
  }

  /**
   * Discounts a taken event from the queue's byte accounting and re-evaluates the receive window.
   * Null in means an empty queue, which is not an event and must not move the accounting.
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
   * This is the single event that frees producer memory and lifts backpressure, and it is why the
   * feature's hundred-millisecond reclamation bound is achievable at all: the producer releases the
   * buffers behind every block at or below the acknowledged position the moment this message lands.
   * Acknowledgements are monotonic, so a position at or below the one already sent is ignored
   * rather than treated as a regression -- which is what makes this safe to call on every iterator
   * advance without the caller having to remember how far it had got.
   *
   * The local ledger is advanced before the message is written, so the consumer's own view of its
   * outstanding volume is correct even if the write has to be deferred. The protocol is told as
   * well, because its `hasCredit` answer is what reopens the receive window.
   *
   * <b>Only the acknowledged prefix is released.</b> The bytes credited here are the sum of the
   * blocks at or below the position being acknowledged, taken from the per-sequence record every
   * accepted block leaves, and those entries are then removed. Crediting everything received
   * instead -- which is the same number only when the consumer is exactly caught up -- would report
   * an outstanding volume of zero for a consumer that is arbitrarily far behind, and outstanding
   * volume against the published credit limit is precisely what closes the receive window. A
   * consumer that acknowledged its first block would therefore stop exerting backpressure at all,
   * however far behind it then fell.
   *
   * A channel that is not writable does not get written to: the position is held instead, coalesced
   * to one number per partition, and flushed when writability returns. Growing Netty's outbound
   * queue while throttling the inbound side would defeat the purpose of throttling it.
   *
   * @param partitionId the reduce partition being acknowledged
   * @param consumedSequenceNumber the highest block position the task has consumed, or
   *                               `AckMessage.NOTHING_CONSUMED` to state that it has consumed
   *                               nothing
   * @return true if this call advanced the acknowledged position
   */
  def acknowledge(partitionId: Int, consumedSequenceNumber: Long): Boolean = {
    require(consumedSequenceNumber >= AckMessage.NOTHING_CONSUMED,
      s"The consumed position must be at least ${AckMessage.NOTHING_CONSUMED} but was " +
        s"$consumedSequenceNumber.")
    if (!servesPartition(partitionId)) {
      // The invariant boundary, not a convenience. An acknowledgement advances the producer's
      // retained-window cursor for the consumer that sent it and frees the buffers behind it, so
      // confirming a partition this task never asked for would let this consumer make another
      // reducer's input unreadable. The reader therefore never asks; this refusal is what keeps the
      // guarantee independent of that, and it answers rather than raises so that the guarantee
      // holds without turning a routing anomaly into a task failure.
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
          state.outboundSequenceNumber.getAndIncrement(), consumedSequenceNumber))
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
   * is deterministic. Each partition contributes at most one message, because a deferred position
   * supersedes any earlier one outright.
   */
  private def flushDeferredAcks(): Unit = {
    val channel = channelRef.get()
    if (channel != null && channel.isActive && channel.isWritable) {
      partitions.values().asScala.toSeq.sortBy(_.partitionId).foreach { state =>
        val pending = state.pendingAckPosition.getAndSet(AckMessage.NOTHING_CONSUMED)
        if (pending > AckMessage.NOTHING_CONSUMED) {
          writeMessage(channel, new AckMessage(shuffleId, mapId, state.partitionId,
            state.outboundSequenceNumber.getAndIncrement(), pending))
        }
      }
    }
  }

  /**
   * Asks the producer to replay an inclusive run of block positions.
   *
   * Both ends are inclusive and a single-position window is the normal case, being what a corrupt
   * block calls for. The request is only sent when it can actually be served, which is what makes
   * repair bounded rather than a retry that cannot succeed.
   *
   *  - Every position asked for must still be retained by the producer. A position this consumer
   *    has not acknowledged is retained by construction, because acknowledgement is the only thing
   *    that releases it; the protocol's own view of the unacknowledged window is consulted as well,
   *    for the case where this executor is keeping that ledger.
   *  - The replay budget must not be spent. Attempts are counted <b>per range</b> rather than per
   *    stream, and charged atomically: five are permitted, spaced by a pause that grows
   *    exponentially from one second, and the sixth is refused so that the failure escalates
   *    instead of retrying for ever. Counting per range is what makes the bound meaningful -- a
   *    stream-wide counter would let one repeatedly failing position consume the budget of every
   *    other position in the same stream, and would let a stream with many independently repairable
   *    gaps be escalated after five of them rather than after five attempts at any one of them.
   *  - The window must fit what the message type permits, so an over-wide request is narrowed to
   *    the widest legal one rather than being rejected outright: a narrower replay still makes
   *    progress, and the next gap is repaired the same way.
   *
   * Every position in the accepted window is <b>quarantined</b>: recorded as awaiting a
   * replacement, so that the block the producer sends back is admitted even though the consumer's
   * cursor may already be past it. Without that record a replay of an already-accepted position
   * would be indistinguishable from a duplicate and would be discarded, which would make a repair
   * requested by the reduce task -- the authoritative verifier -- impossible to complete.
   *
   * @return true if the repair is under way, meaning the request was written now or is waiting only
   *         for its backoff to elapse; false if the caller must escalate instead
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
          logWarning(log"Streaming shuffle will not ask shuffle ${MDC(SHUFFLE_ID, shuffleId)} " +
            log"partition ${MDC(PARTITION_ID, partitionId)} to replay position(s) " +
            log"${MDC(COUNT, firstSequenceNumber)} through ${MDC(THRESHOLD, bounded)} again " +
            log"because the replay budget of " +
            log"${MDC(MAX_ATTEMPTS, BackpressureProtocol.MAX_RETRY_ATTEMPTS)} attempt(s) is spent")
          state.discardReplayWindow(window)
          false
        case ReplayVerdict.Deferred =>
          // The repair is real and already recorded; only its next attempt is waiting for the
          // backoff to elapse. Reporting this as a refusal would escalate a stage recomputation for
          // a block the producer is still holding and will still replay, so the quarantine stands
          // and retryDueReplays sends the request when the pause is over.
          state.quarantine(firstSequenceNumber, bounded)
          true
        case ReplayVerdict.Granted(attempt) =>
          state.quarantine(firstSequenceNumber, bounded)
          state.replayRequests.incrementAndGet()
          val written = writeMessage(channel, new RetransmitRequestMessage(
            shuffleId, mapId, partitionId, firstSequenceNumber, bounded))
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
   * Sends every replay request whose backoff has elapsed, and reports how many went out.
   *
   * This is the timer the exponential backoff needs and that a handler with no scheduler of its own
   * would otherwise lack. It runs on the reduce task's thread, from the same poll window that
   * renews the in-progress request, which is why no thread and no timer wheel is introduced for it:
   * a consumer that is waiting for a replay is by definition polling.
   *
   * A window whose positions have all been replaced is dropped rather than retried. A window whose
   * budget is spent is deliberately *not* dropped here: keeping it is what makes the five-attempt
   * bound mean five attempts at that range rather than five per surviving window, since dropping it
   * would let the very next request for the same positions start a fresh budget. Its failure is
   * escalated instead by whichever caller next asks for those positions -- a refusal the reader
   * turns into a fetch failure -- which is the one terminus the retransmission protocol has.
   */
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
                if (writeMessage(channel, new RetransmitRequestMessage(
                    shuffleId, mapId, state.partitionId, window.first, window.last))) {
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

  /**
   * Whether a block position is still retained by the producer and can therefore be replayed.
   *
   * Two readings, either of which is sufficient. The protocol answers directly wherever this
   * executor keeps the stream's ledger. Failing that, the consumer's own books answer it: a
   * position above the highest one this consumer has acknowledged has not been released, because
   * acknowledgement is the only thing that releases it.
   */
  private def isRetained(partitionId: Int, sequenceNumber: Long): Boolean = {
    backpressure.isWithinUnacknowledgedWindow(consumerKey(partitionId), sequenceNumber) ||
      sequenceNumber > partitionStateOf(partitionId).acknowledgedPosition.get()
  }

  /**
   * Sends a heartbeat for one partition if the five-second interval has elapsed.
   *
   * The interval and the "already sent" bookkeeping belong to the protocol wherever it keeps a
   * ledger for the stream, so that one notion of time governs both sides of the liveness question.
   * Where it keeps none, this handler's own interval applies and the message is stamped from the
   * injected clock; the heartbeat type reads no clock of its own, which is what allows a suite to
   * freeze time and still exercise the timer.
   *
   * @return true if a heartbeat was written
   */
  def sendHeartbeatIfDue(partitionId: Int): Boolean = {
    if (closed.get() || !servesPartition(partitionId) ||
        backpressure.isStreamTerminated(consumerKey(partitionId))) {
      false
    } else if (backpressure.shouldSendHeartbeat(consumerKey(partitionId))) {
      // The interval is the protocol's to own, but the position is this handler's to declare: only
      // this handler knows which position it expects next. The protocol's ledger holds the highest
      // position charged, and after a gap that is above the position still missing, so a heartbeat
      // built from it would invite the producer to resume past output this consumer needs.
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
        writeHeartbeat(partitionId, buildHeartbeat(state, nowMillis))
      }
    }
  }

  /**
   * Builds a heartbeat from the injected clock, positioned at how far this consumer has got.
   *
   * The position reported is the <b>next</b> position this consumer expects, which is exactly the
   * reading a producer's own heartbeat carries -- the next position it will produce -- so one
   * header field means one thing in both directions and the producer's resume handshake needs no
   * second convention. It also has to be the next expected position rather than the highest
   * received one: after a gap the two differ, and announcing the highest received would have the
   * producer resume past the positions still missing.
   *
   * The value is naturally non-negative, which matters because the message type refuses a negative
   * sequence number: a consumer that has received nothing announces zero, and the producer reads
   * that as "serve me from the beginning" rather than as "I have consumed block zero". The
   * timestamp is clamped for the same reason, since a wall clock adjusted backwards past the epoch
   * would otherwise construct a message the type rejects.
   *
   * The heartbeat also declares this consumer's stable identity, which is what makes the producer's
   * resume handshake a resumption rather than a restart: the producer keys the position it has
   * recorded for this consumer by that identity, and an identity taken from the connection would
   * change on exactly the reconnection the handshake exists to serve.
   */
  private def buildHeartbeat(state: PartitionState, nowMillis: Long): HeartbeatMessage = {
    new HeartbeatMessage(
      shuffleId,
      mapId,
      state.partitionId,
      announcedPosition(state),
      math.max(0L, nowMillis),
      consumerId)
  }

  /**
   * The position this consumer announces for one partition: the next position it expects.
   *
   * One expression, read by both heartbeat paths -- the one the protocol's interval drives and the
   * one this handler's own interval drives -- so the two cannot come to disagree about what the
   * number means. Clamped at zero because the message type refuses a negative sequence number, and
   * because a consumer that has received nothing must announce zero: the producer's resume
   * handshake reads that as "serve me from the beginning" rather than as a claim about block zero.
   */
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
   * This is what `TransportClient.send` constructs, and it is built here rather than delegated to
   * that method so that the write is observable: `send` returns nothing, and an acknowledgement
   * that silently failed to leave the socket is indistinguishable, at the producer, from a consumer
   * that has stopped consuming -- which is the condition its ten-second detector treats as consumer
   * failure. The transport's encoding, its optional encryption and its frame accounting are used
   * exactly as they stand.
   *
   * <b>Every outbound write carries a completion listener.</b> A write that fails after being
   * accepted by the channel fails asynchronously, on an I/O thread, with no caller left to observe
   * it, so without the listener a lost acknowledgement or a lost replay request would strand the
   * reduce task until some other timer noticed. Recording the cause through the first-error-wins
   * notifier is what makes the task raise it from `read()`, and it is the same bridge every other
   * failure this handler sees travels over.
   *
   * @return true if the message was handed to the channel
   */
  private def writeMessage(channel: Channel, message: StreamingShuffleMessage): Boolean = {
    if (!channel.isActive) {
      false
    } else {
      try {
        val partitionId = message.partitionId()
        channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message.toByteBuffer())))
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

  /**
   * Records a failure for the task thread to raise, and wakes a task that is waiting for input.
   *
   * This is the only route by which a failure observed on a Netty thread reaches the reduce task,
   * and it deliberately does not construct a fetch failure. That exception registers itself on the
   * task context as it is built, and the context is a thread-local of the task thread, so one
   * constructed here would register nothing at all; it also has to be thrown in the same expression
   * that creates it. Both are reasons the reader builds and throws it, on the task thread, from the
   * cause recorded here.
   *
   * The marker placed on the queue is what makes the failure prompt rather than eventual: without
   * it a task blocked on the queue would wait out its whole timeout before it thought to look at
   * the notifier.
   *
   * <b>The marker is queued before the raw failure is latched, and the order is the point.</b> The
   * notifier is a safety net for a task that is not polling, but a task that is polling must be
   * given the chance to convert this loss into a fetch failure itself -- only the task thread can
   * discard what it has already taken from this producer, and only a fetch failure makes the
   * unmodified scheduler recompute the upstream stage. Latching the transport error first left a
   * window in which the task raised that error instead, failing the reduce attempt over a lost
   * producer without ever asking for the stage that produced it to be recomputed.
   *
   * Log volume is bounded rather than proportional to the misbehaviour of a peer: a peer sending
   * malformed frames produces one failure per frame, so the first few are reported at warning level
   * and the rest at debug. Nothing is lost by that -- the notifier keeps the root cause and counts
   * what it dropped.
   */
  private def escalate(cause: Throwable, partitionId: Int): Unit = {
    signalProducerLost(partitionId, cause.getClass.getSimpleName, cause)
    errorNotifier.setError(cause)
    val reported = escalationsReported.incrementAndGet()
    if (reported <= MAX_REPORTED_ESCALATIONS) {
      logWarning(log"Streaming shuffle consumer failed for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, partitionId)}: " +
        log"${MDC(ERROR, cause.getMessage)}", cause)
    } else if (debugEnabled) {
      logDebug(log"Streaming shuffle consumer failed again for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, partitionId)} after " +
        log"${MDC(COUNT, reported)} reported failure(s): ${MDC(ERROR, cause.getMessage)}")
    }
  }

  /**
   * Marks one partition's producer as gone, at most once, and wakes whoever is waiting on it.
   *
   * The invalidation of the blocks already accepted from that producer, and the fetch failure that
   * makes the scheduler recompute the upstream stage, belong to the reader: they have to be atomic
   * across everything the task has taken from this queue as well as everything still on it, and
   * only the task thread can see both. This handler's part is to say that no more is coming.
   */
  private def signalProducerLost(partitionId: Int, reason: String, cause: Throwable): Unit = {
    // A failure that belongs to the channel rather than to one stream must still be reported, and
    // it cannot use a partition's latch because it names no partition. Latching it separately is
    // what stops a peer sending malformed frames from filling the hand-off queue with markers while
    // still guaranteeing the reduce task is woken at least once.
    val firstSignal =
      if (servesPartition(partitionId)) {
        partitionStateOf(partitionId).producerLostSignalled.compareAndSet(false, true)
      } else {
        channelLostSignalled.compareAndSet(false, true)
      }
    if (firstSignal) {
      producerLostAtMillis.compareAndSet(NO_TIMESTAMP, clock.getTimeMillis())
      enqueue(ProducerLost(partitionId, reason, cause), 0L)
    }
  }

  // Observability, all of it derived from state this handler already keeps.

  /** Whether this channel is currently reading from its socket. */
  def isAutoReadEnabled: Boolean = autoReadEnabled.get()

  /** Why reading is currently stopped, or `None` when it is not. */
  def currentThrottleCause: Option[String] = Option(throttleCause.get())

  /**
   * Transitions into the throttled state observed on this channel. The executor-wide
   * `shuffle.streaming.backpressureEvents` counter is advanced by [[BackpressureProtocol]] and
   * never from here, so this is a per-channel reading and not a second copy of that metric.
   */
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
   *
   * A terminated stream is never silent: its quiet means completion, and recomputing a stage that
   * had in fact finished would be the worst possible response to it. Both this handler's own
   * reading and the protocol's are consulted, so a stream the protocol never learned about is still
   * covered.
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

  /**
   * Blocks the producer claims to have sent for one partition, or `None` before it has said. A
   * returned zero is a genuine reading and means the partition was empty.
   */
  def announcedBlockCount(partitionId: Int): Option[Long] = {
    val state = partitions.get(Integer.valueOf(partitionId))
    if (state == null || state.announcedBlocks.get() == NO_BLOCK_TOTAL) {
      None
    } else {
      Some(state.announcedBlocks.get())
    }
  }

  /**
   * Reads one per-partition counter without allocating state for a partition that has none.
   *
   * Every observer below goes through this rather than through [[partitionStateOf]], and for a
   * reason that is a correctness one and not a matter of taste: an observer that allocated would
   * let a diagnostic, an assertion or a metric scrape create exactly the unbounded per-partition
   * metadata the addressing check exists to prevent.
   */
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

  /** Transport callbacks refused because they arrived on a channel this handler is not bound to. */
  def foreignChannelCallbackCount: Long = foreignChannelCallbacks.get()

  /** Blocks of one partition declined because the executor's shared budget was exhausted. */
  def quotaRefusalCount(partitionId: Int): Long =
    readState(partitionId, 0L)(_.quotaRefusals.get())

  /**
   * Partitions this handler holds metadata for, which can never exceed the width of the range it
   * was constructed with however many partition ids a peer names.
   */
  def trackedPartitionCount: Int = partitions.size()

  /** The partitions this channel has seen traffic for, in a deterministic order. */
  def observedPartitionIds: Seq[Int] =
    partitions.values().asScala.map(_.partitionId).toSeq.sorted

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

  /**
   * Releases everything this handler holds, once.
   *
   * Idempotent by compare-and-set, because the reader registers it on
   * `TaskContext.addTaskCompletionListener` and it therefore runs on success, on failure and on
   * cancellation alike, possibly alongside a channel that is closing for its own reasons. It is
   * never registered from a Netty thread; the reader owns that registration, on the task thread.
   *
   * The queue is drained before the channel is closed, so nothing is left reachable from a handler
   * whose channel has gone. Nothing reference-counted is ever on it: the transport releases a
   * frame's body as soon as `receive` returns, and the decoder gives a block a heap array of its
   * own, so the queue cannot be the site of a Netty leak. The test JVM fails a leaked buffer
   * outright, which is what makes that guarantee machine-checked rather than asserted.
   *
   * `setAutoRead` is deliberately not touched here: reopening the receive window of a channel that
   * is about to close could only pull in bytes nobody will read.
   */
  def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      val drained = drainAndRelease()
      // Everything still charged to the executor's shared budget comes back here, whether this
      // handler is closing after an orderly end of stream, after a producer failure or after task
      // cancellation. Without it a failed reduce task would permanently shrink the budget available
      // to every consumer that follows it on this executor.
      val returned = releaseAllQuota()
      partitions.clear()
      val channel = channelRef.getAndSet(null)
      if (channel != null) {
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

  /** Whether [[close]] has run. */
  def isClosed: Boolean = closed.get()

  /**
   * Empties the hand-off queue and rewinds the byte accounting.
   *
   * @return how many events were discarded
   */
  /**
   * Returns every byte this handler still has charged to the executor's shared receive budget.
   *
   * Computed from the per-partition ledgers rather than from the hand-off queue, because the charge
   * spans both: a block is charged when it is admitted and discharged when the reduce task
   * acknowledges it, so bytes the task has taken from the queue but not yet consumed are still this
   * handler's to return.
   *
   * @return how many bytes were returned
   */
  private def releaseAllQuota(): Long = {
    var released = 0L
    partitions.values().asScala.foreach { state =>
      val outstanding = state.outstandingBytes
      if (outstanding > 0L) {
        released += outstanding
        backpressure.releaseReceiveQuota(outstanding)
      }
    }
    released
  }

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

  /**
   * The bookkeeping for one partition, created on first sight.
   *
   * Written without a lambda so that the common path -- a partition already seen -- is a single map
   * read with no allocation at all, which matters because it runs once per frame.
   *
   * The precondition is the bound on remotely provoked state: every route to this method passes the
   * addressing check first, so the map can hold at most one entry per partition in the range this
   * task asked for. It is asserted rather than assumed, because a future caller that forgot the
   * check would reintroduce exactly the exposure the check removes, and failing loudly here is far
   * better than growing a map quietly.
   */
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

  /**
   * A stable identifier for one streaming block, for diagnostics only.
   *
   * Deliberately not a `BlockId`: the streaming path introduces no new block-identity type, because
   * the block-manager storage contracts are a preservation zone. The prefix names the subsystem so
   * that the three numbers cannot be mistaken for the map and reduce ids of a materialised shuffle
   * block.
   */
  private def blockIdOf(partitionId: Int, sequenceNumber: Long): String =
    s"streaming_shuffle_${shuffleId}_${partitionId}_$sequenceNumber"

  /** The class of an unexpected inbound object, named without dereferencing it. */
  private def describeClassOf(obj: Any): String =
    if (obj == null) "null" else obj.getClass.getName
}

/**
 * Constants, per-partition bookkeeping and the envelope type the consumer channel hands to the
 * reduce task.
 *
 * The envelope is this handler's own delivery type and not a second rendering of any wire message:
 * a received block travels inside it exactly as the codec produced it, and the other two cases
 * carry information no wire message has, namely that a stream has finished and that a producer has
 * gone.
 */
private[spark] object StreamingShuffleClientHandler {

  /**
   * Transport module name for streaming shuffle, passed as an argument to
   * `SparkTransportConf.fromSparkConf` and never added to a shared file. It yields the independent
   * `spark.shuffle-streaming.io.*` tuning namespace, which is what lets thread counts, retry policy
   * and OS keepalive be set for streaming alone.
   */
  val TRANSPORT_MODULE: String = "shuffle-streaming"

  /**
   * Silence after which a producer is declared gone. Enforced at application level, from the
   * injected clock, because the transport exposes keepalive as a boolean with no interval and the
   * JDK offers no keepalive-interval socket option.
   */
  val PRODUCER_CONNECTION_TIMEOUT_MS: Long = BackpressureProtocol.ACK_TIMEOUT_MS

  /**
   * Hard bound on the hand-off queue. A final safety cap rather than an expected state: the receive
   * window closes at the high-water mark below and the producer's credit limit caps what can
   * already be in flight, but frames already on the wire when the window closes can still reach
   * this bound.
   */
  val INBOUND_QUEUE_CAPACITY: Int = 32

  /** Queued events at which the receive window closes. */
  val INBOUND_QUEUE_HIGH_WATER_MARK: Int = 8

  /** Queued events at which it may reopen, low enough that a slow task cannot make it flap. */
  val INBOUND_QUEUE_LOW_WATER_MARK: Int = 4

  /**
   * Queued payload bytes at which the receive window closes, sixteen mebibytes.
   *
   * Counted as well as the events themselves because a block carries anything from nothing up to
   * the two-mebibyte cap, so a count of events is a poor description of the heap the queue is
   * holding.
   */
  val INBOUND_HIGH_WATER_BYTES: Long = 8L * DataBlockMessage.MAX_BLOCK_SIZE_BYTES

  /** Queued payload bytes at which it may reopen, four mebibytes. */
  val INBOUND_LOW_WATER_BYTES: Long = 2L * DataBlockMessage.MAX_BLOCK_SIZE_BYTES

  /**
   * Widest span a single replay request may name, so that an over-wide window is narrowed rather
   * than refused. One below the message type's own maximum, because the window includes both of its
   * ends.
   */
  val MAX_REPLAY_WINDOW_SPAN: Long = RetransmitRequestMessage.MAX_REQUESTED_BLOCKS - 1L

  /**
   * Escalated failures reported at warning level before the rest drop to debug. A peer sending
   * malformed frames produces one failure per frame, so the ceiling is what keeps the log budget
   * independent of how badly a peer misbehaves.
   */
  val MAX_REPORTED_ESCALATIONS: Long = 4L

  /**
   * Positions of one partition that may await a replacement block at any moment.
   *
   * A quarantined position is one this consumer has asked to have sent again, and it is remembered
   * so that the replacement is admitted rather than mistaken for a duplicate. The bound exists
   * because every entry is provoked by a repair, and a peer that returned a corrupt block for every
   * position in a stream would otherwise have this consumer remember the whole stream. It is set
   * comfortably above what any legitimate repair needs: a single replay window can name at most
   * [[MAX_REPLAY_WINDOW_SPAN]] positions plus one, and the five-attempt budget means a stream that
   * needs more than a handful of windows is failing rather than repairing.
   */
  val MAX_QUARANTINED_POSITIONS: Int = (4L * RetransmitRequestMessage.MAX_REQUESTED_BLOCKS).toInt

  /** Sentinel for a timestamp that has not been taken. */
  val NO_TIMESTAMP: Long = Long.MinValue

  /**
   * Sentinel for "no incompatible protocol revision has been seen".
   *
   * An int, so that it cannot collide with any of the 256 values a protocol version byte can take.
   */
  val NO_OBSERVED_VERSION: Int = Int.MinValue

  /** Sentinel for a block total the producer has not announced. */
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

  /** Reason recorded when a producer closes the connection with blocks still owing. */
  val PRODUCER_CLOSED_REASON: String = "the producer closed the connection"

  /** What a consumer channel expects to receive, named in an unexpected-type diagnostic. */
  val CONSUMER_INBOUND_DESCRIPTION: String =
    StreamingShuffleMessageType.DATA_BLOCK.name() + ", " +
      StreamingShuffleMessageType.HEARTBEAT.name() + " or " +
      StreamingShuffleMessageType.STREAM_TERMINATION.name()

  /** What the pipeline is expected to deliver, named when it delivers something else entirely. */
  val STREAMING_MESSAGE_DESCRIPTION: String = classOf[StreamingShuffleMessage].getName

  /**
   * What charging one replay attempt against a window's budget decided.
   *
   * Three outcomes and not two, because "not now" and "never" call for opposite responses from the
   * caller: a deferred attempt means the repair is still under way and must not be escalated, while
   * an exhausted budget means no message can recover the bytes and the failure has to become a
   * fetch failure so that the upstream stage is recomputed.
   */
  sealed abstract class ReplayVerdict

  object ReplayVerdict {

    /** The attempt is granted; the request may be written now. */
    final case class Granted(attempt: Int) extends ReplayVerdict

    /** The budget is intact but the exponential pause has not elapsed. Do not escalate. */
    case object Deferred extends ReplayVerdict

    /** Five attempts have been spent on this window. The caller must escalate. */
    case object Exhausted extends ReplayVerdict
  }

  /**
   * The replay budget of one inclusive range of block positions.
   *
   * <b>Why per range and not per stream.</b> A stream-wide counter conflates independent repairs:
   * one position that keeps arriving corrupt would consume the budget of every other position in
   * the same stream, and a stream with several independently repairable gaps would be escalated
   * after five gaps rather than after five attempts at any one of them. Counting per range makes
   * the documented bound -- at most five attempts, spaced by a pause growing from one second --
   * mean what it says.
   *
   * The two counters move together under a compare-and-set on the attempt count, so two threads
   * that both observe a due window cannot both be granted the same attempt. The schedule itself is
   * taken from [[BackpressureProtocol]] rather than restated here, so producer-side and
   * consumer-side replay pacing cannot drift apart.
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

    /**
     * Spends one attempt if the budget and the schedule both allow it.
     *
     * The pause programmed for the next attempt is the backoff of the attempt just granted, so the
     * five permitted attempts are spaced by one, two, four, eight and sixteen seconds.
     */
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

  /**
   * One inbound event on the way from the channel to the reduce task.
   *
   * Three cases, because a reduce task has to be able to tell three outcomes apart: a block
   * arrived, the stream ended, or the producer went away. Only the first carries data; the other
   * two are what let the task stop waiting, and the difference between them is the difference
   * between a completed partition and one that has to be recomputed.
   */
  sealed abstract class Inbound {
    /** The reduce partition this event belongs to, or [[UNKNOWN_PARTITION_ID]]. */
    def partitionId: Int
  }

  /**
   * A verified block, carried exactly as the codec produced it.
   *
   * The block owns a heap array, so nothing reference-counted travels on the queue and the payload
   * can be read as often as the task likes through `payloadBuffer`, which copies nothing.
   *
   * Verification happens here, on the channel thread, and that placement is what makes the repair
   * protocol reachable: a corrupt block can be replaced by a replay before the task ever sees it.
   * The reader asks the same question again where it turns the block into records, so the guarantee
   * it acts on is its own rather than one it takes on trust -- but the block remembers a successful
   * verification, so asking costs nothing rather than a second pass over every payload byte.
   */
  final case class BlockReceived(block: DataBlockMessage) extends Inbound {
    override def partitionId: Int = block.partitionId()
  }

  /**
   * The orderly end of one partition's stream.
   *
   * A total of zero is valid and means the partition was empty. Delivering completion rather than
   * leaving it to be inferred from silence is what keeps an empty partition from being mistaken for
   * a producer that died five seconds ago.
   */
  final case class StreamCompleted(partitionId: Int, totalBlocks: Long) extends Inbound

  /**
   * The producer of one partition is gone and nothing further is coming.
   *
   * The reader turns this into the atomic invalidation of every block accepted from that producer
   * and into the fetch failure that makes the unmodified scheduler recompute the upstream stage. It
   * is delivered on the queue so that a task blocked on input stops waiting at once rather than
   * after a full timeout on a socket that has already gone.
   *
   * @param partitionId the partition that will receive nothing further, or
   *                    [[StreamingShuffleClientHandler.UNKNOWN_PARTITION_ID]] when the loss belongs
   *                    to the channel rather than to one stream
   * @param reason short operator-facing description of why the producer is considered gone
   * @param cause the throwable that revealed the loss, or null when the loss was observed as an
   *              orderly channel close rather than as a failure. Carried on the marker so that the
   *              fetch failure the reader raises names the transport-level cause instead of leaving
   *              it to be recovered from a raw error the reader must no longer prefer.
   */
  final case class ProducerLost(partitionId: Int, reason: String, cause: Throwable) extends Inbound

  /**
   * Per-partition bookkeeping for one channel.
   *
   * Every cell is an atomic, and for one reason: the writes are all made by this channel's single
   * event-loop thread, which orders them against each other already, while the reads come from the
   * reduce task's thread, which needs to see them. The atomics are there for that visibility, not
   * to order writes that are already ordered, and none of them is ever the subject of a
   * read-modify-write that spans two cells.
   */
  private[streaming] class PartitionState(val partitionId: Int) {

    /** Next block position expected, so a gap or a duplicate is detected rather than spliced. */
    val expectedSequenceNumber = new AtomicLong(0L)

    /** Highest position actually received, which is the position a heartbeat reports. */
    val highestReceivedSequenceNumber = new AtomicLong(NO_BLOCK_TOTAL)

    /** Payload bytes accepted for this partition. */
    val receivedBytes = new AtomicLong(0L)

    /** Payload bytes covered by the acknowledgements already sent. */
    val acknowledgedBytes = new AtomicLong(0L)

    /**
     * Framed payload size of every accepted block that has not yet been acknowledged, by position.
     *
     * This is what makes an acknowledgement release exactly its own prefix. The outstanding volume
     * the receive window is judged by is bytes received minus bytes acknowledged, so crediting
     * anything other than the prefix named would misreport how far behind this consumer is -- and a
     * consumer that under-reports its backlog stops exerting backpressure precisely when it most
     * needs to.
     *
     * A skip-list map rather than a hash map because the operation that matters is "everything at
     * or below this position", which it answers as a view rather than a scan.
     */
    private val receivedBytesBySequence =
      new ConcurrentSkipListMap[java.lang.Long, java.lang.Long]()

    /**
     * Positions awaiting a replacement block, because this consumer asked for them again.
     *
     * A position is quarantined by [[StreamingShuffleClientHandler.requestRetransmission]] and left
     * by the arrival of a block that verifies. While it is quarantined a block bearing it is
     * admitted even though the cursor is already past it, which is the only way a repair requested
     * for an already-accepted position can ever complete.
     */
    private val quarantined = new ConcurrentSkipListSet[java.lang.Long]()

    /** The replay budget of each requested range, keyed by the range's first position. */
    private val replayWindows = new ConcurrentHashMap[java.lang.Long, ReplayWindow]()

    /** Highest position acknowledged, or the nothing-consumed sentinel. */
    val acknowledgedPosition = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    /**
     * An acknowledgement held back because the channel was not writable, coalesced to one position:
     * acknowledgements are monotonic, so a later position supersedes an earlier one outright and
     * what is held is a single number rather than a backlog of messages.
     */
    val pendingAckPosition = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    /**
     * Position stamped into this consumer's own outbound control messages. The consumer's direction
     * has a sequence of its own, independent of the producer's block numbering.
     */
    val outboundSequenceNumber = new AtomicLong(0L)

    /** Instant of the last inbound event of any kind, from the injected clock. */
    val lastInboundMillis = new AtomicLong(NO_TIMESTAMP)

    /** Instant at which this consumer last emitted a heartbeat, from the injected clock. */
    val lastHeartbeatSentMillis = new AtomicLong(NO_TIMESTAMP)

    /** Timestamp the producer stamped into its last heartbeat. Diagnostic only. */
    val remoteHeartbeatMillis = new AtomicLong(NO_TIMESTAMP)

    /** Whether the producer has announced the orderly end of this stream. */
    val terminated = new AtomicBoolean(false)

    /** Blocks the producer claims to have sent, or [[NO_BLOCK_TOTAL]] before it has said. */
    val announcedBlocks = new AtomicLong(NO_BLOCK_TOTAL)

    /** Latch ensuring completion is delivered to the task exactly once. */
    val completionSignalled = new AtomicBoolean(false)

    /** Latch ensuring producer loss is delivered to the task exactly once. */
    val producerLostSignalled = new AtomicBoolean(false)

    /** Blocks accepted and handed on. */
    val acceptedBlocks = new AtomicLong(0L)

    /** Blocks refused because they failed verification. */
    val corruptBlocks = new AtomicLong(0L)

    /** Blocks discarded because they had already been accepted. */
    val duplicateBlocks = new AtomicLong(0L)

    /** Sequence gaps for which a replay was requested. */
    val repairedGaps = new AtomicLong(0L)

    /** Replay requests written, whether for a gap or for corruption. */
    val replayRequests = new AtomicLong(0L)

    /** Blocks declined because this executor's shared consumer receive budget was exhausted. */
    val quotaRefusals = new AtomicLong(0L)

    /**
     * Records one accepted block's payload size against its position.
     *
     * `put` rather than an addition, so a replacement for a position that had already been recorded
     * overwrites its entry instead of counting the same block twice, and the running total is
     * moved by the difference the replacement makes rather than by the whole of it. Adding the
     * whole would leave the total permanently above the sum of the positions still outstanding,
     * which is the one reading the receive window is judged by -- a consumer whose repairs
     * inflated that figure would throttle itself for bytes it had already released.
     */
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
     * Marks an inclusive range of positions as awaiting a replacement.
     *
     * Bounded, because every entry is provoked by a repair and a peer that corrupted everything
     * would otherwise have this consumer remember a whole stream. Reaching the bound is a failure
     * rather than a state to be trimmed, so the range is simply not recorded and the caller's own
     * escalation path -- a replacement that never arrives -- is what fails the task.
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
     * @return true if the position had been awaiting a replacement, which tells the caller that the
     *         block just accepted is a replacement and must not move the sequence cursor
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
