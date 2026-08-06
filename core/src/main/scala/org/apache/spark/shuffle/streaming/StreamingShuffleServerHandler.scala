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

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.Comparator
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ConcurrentSkipListMap,
  PriorityBlockingQueue, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener, ChannelInitializer,
  ChannelOption, EventLoopGroup}
import io.netty.channel.socket.SocketChannel

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv, SparkException}
import org.apache.spark.internal.{Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys.{CONFIG, COUNT, DESCRIPTION, DURATION, ERROR, HOST_PORT,
  MAP_ID, MAX_ATTEMPTS, MAX_SIZE, NUM_BLOCKS, NUM_BYTES, NUM_EVENTS, NUM_FAILURES, NUM_ITERATIONS,
  NUM_SKIPPED, PARTITION_ID, PORT, PROTOCOL_VERSION, REASON, SESSION_ID, SHUFFLE_ID, STATUS,
  TASK_ATTEMPT_ID, THRESHOLD, TIMEOUT, VALUE, VERSION_NUM}
import org.apache.spark.internal.config.{NETWORK_AUTH_ENABLED, SHUFFLE_STREAMING_DEBUG}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient,
  TransportClientBootstrap}
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.protocol.OneWayMessage
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager,
  TransportServerBootstrap}
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, DataBlockMessage,
  HeartbeatMessage, RetransmitRequestMessage, StreamingShuffleMessage, StreamingShuffleMessageType,
  StreamTerminationMessage}
import org.apache.spark.network.util.{IOMode, NettyUtils, TransportConf}
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

/**
 * The inbound-routing table a producer generation is published in, as an abstraction rather than as
 * the executor's listener itself.
 */
private[spark] trait StreamingShuffleRouteRegistry {

  /**
   * Removes one producer generation's routing entry, but only while `handler` is still the entry.
   *
   * @param shuffleId shuffle whose producer is being withdrawn
   * @param mapId map task whose producer is being withdrawn
   * @param handler the handler that must still be routed for the removal to happen
   * @return whether this handler was routed and has now been removed
   */
  def withdrawRoute(
      shuffleId: Int,
      mapId: Long,
      handler: StreamingShuffleServerHandler): Boolean

  /**
   * Gives up one producer's participation in one consumer channel.
   *
   * @param channel the consumer channel this producer is leaving
   * @param handler the producer leaving it
   * @param closeWhenLast whether to close the channel if no producer participates in it any
   *     more.
   * @return true when the channel was closed by this call
   */
  def releaseChannelParticipation(
      channel: Channel,
      handler: StreamingShuffleServerHandler,
      closeWhenLast: Boolean): Boolean

  /**
   * Closes a consumer channel for a fault that impugns the channel itself.
   *
   * @param channel the channel to close
   * @param handler the producer that observed the fault, for the diagnostic
   * @param reason operator-facing description of the fault
   */
  def closeFaultedChannel(
      channel: Channel,
      handler: StreamingShuffleServerHandler,
      reason: String): Unit
}

/**
 * The producer side channel handler of the streaming shuffle: it orders the blocks a
 * [[StreamingShuffleWriter]] has admitted to the retained store, charges them against the
 * executor's egress budget, writes them to every consumer that has subscribed, and consumes the
 * control traffic the reduce side sends back -- acknowledgements, heartbeats and retransmission
 * requests -- turning an acknowledgement into reclaimed producer memory.
 *
 * A map output is read by every reduce task that wants one of its partitions, so this handler
 * serves many channels at once and keeps one session per channel: an authenticated identity, the
 * partitions that consumer asked for, its own egress queue and its own acknowledgement cursors.
 * Nothing is written to a channel that did not ask for the partition, and a block is retained until
 * *every* subscribed consumer has acknowledged it, which is what the retained store's
 * minimum-across-consumers retirement rule enforces. Subscription needs no message of its own: a
 * consumer announces the partition it wants with the same heartbeat that proves it is alive and
 * carries the position it has reached, so resumption is the ordinary case of subscription.
 *
 * This handler owns no payload. Every block it sends is read, as it is framed, from the
 * [[MemorySpillManager]] that already charged those bytes against the buffer budget -- reached
 * through the executor-scoped [[StreamingShuffleBlockResolver]], which also refuses the store of a
 * superseded generation. A second copy here would double the memory a bounded budget exists to
 * bound, and would make a replay servable from memory but not from spill.
 *
 * It is an `RpcHandler` rather than a Netty channel handler, which is what lets `TransportContext`
 * install it and so inherit the transport's authentication, its optional SSL and its keep-alive
 * with no edit to any shared transport class; every streaming frame travels as the body of a
 * one-way RPC. Toggling `autoRead` belongs to the consumer side handler alone and appears nowhere
 * here.
 *
 * Threading. The shuffle metrics reporters are documented as single-threaded, so no reporter is
 * ever called from here: this handler publishes plain counters that the writer reads on the task
 * thread and forwards. Nothing here parks, blocks or sleeps; shared state is atomic or concurrent;
 * no lock is held across a channel write; no exception escapes a Netty callback; and every failure
 * observed on an event-loop thread is handed to [[StreamingShuffleErrorNotifier]] so the task
 * thread re-throws it, without which a producing task would wait forever on progress that cannot
 * come. Every instant comes from the injected `Clock`, so no timer thread of this handler's own is
 * needed: liveness is evaluated when it is asked for, on the thread that asks.
 *
 * A healthy shuffle can legitimately put nothing on the wire. Whether a consumer exists while a
 * producer runs is decided by task submission, and the unmodified scheduler submits a stage only
 * once its parents report their output available, so at an ordinary stage boundary every block is
 * enqueued, retained, made durable at the writer's stop and served afterwards from the block
 * resolver instead. Zero bytes streamed live is that ordering, not a fault, and the byte accounting
 * is kept exact precisely so the difference is measurable rather than assumed.
 *
 * @param conf the executor's configuration, read exactly once during construction
 * @param shuffleId the shuffle whose partitions this handler streams
 * @param mapId the map output whose partitions this handler streams
 * @param taskAttemptId the producing generation, which is what keeps a superseded attempt's
 *     accounting separate from that of the attempt which replaced it
 * @param blockResolver the executor-scoped registry through which this handler reaches the
 *     retained output of the map task it serves.
 * @param backpressure the protocol that owns the credit ledger, the acknowledgement and
 *     heartbeat timeouts, cross shuffle utilisation aggregation and the backpressure event counter.
 * @param rateLimiter the egress bucket this shuffle's share of the link capacity is charged
 *     against
 * @param errorNotifier the bridge that carries a failure observed on an event-loop thread
 *     across to the task thread
 * @param clock time source for heartbeat stamps, liveness evaluation, reclamation budgeting and
 *     retransmission backoff, injected so that behaviour is deterministic under test
 */
private[spark] class StreamingShuffleServerHandler(
    conf: SparkConf,
    val shuffleId: Int,
    val mapId: Long,
    val taskAttemptId: Long,
    val numPartitions: Int,
    blockResolver: StreamingShuffleBlockResolver,
    routes: StreamingShuffleRouteRegistry,
    backpressure: BackpressureProtocol,
    rateLimiter: TokenBucketRateLimiter,
    errorNotifier: StreamingShuffleErrorNotifier,
    fallbackPolicy: StreamingShuffleFallbackPolicy,
    clock: Clock = new SystemClock)
  extends RpcHandler with Logging {

  import StreamingShuffleServerHandler._

  require(mapId >= 0L, s"The map id must be non-negative but was $mapId.")
  require(taskAttemptId >= 0L,
    s"The producing task attempt id must be non-negative but was $taskAttemptId.")
  // The partition domain is fixed at construction, from the shuffle handle, and never learned from
  // a peer or from a registration that may not have happened yet.
  require(numPartitions > 0,
    s"The reduce partition count must be positive but was $numPartitions.")
  require(numPartitions <= MemorySpillManager.MAX_TRACKED_PARTITIONS,
    s"The reduce partition count must not exceed " +
      s"${MemorySpillManager.MAX_TRACKED_PARTITIONS} but was $numPartitions.")

  /**
   * The ledger identity of one partition of this map output as it flows to '''one''' consumer.
   *
   * @param partitionId the reduce partition the flow feeds
   * @param consumerId the consumer session's identity, as adopted from its own heartbeat
   */
  private def consumerLedgerKey(partitionId: Int, consumerId: String): BackpressureStreamKey =
    BackpressureStreamKey.forProducer(shuffleId, mapId, taskAttemptId, partitionId, consumerId)

  /** The value of spark.shuffle.streaming.debug, read once and held immutably. */
  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  /** The transport configuration for the streaming shuffle's own module. */
  val transportConf: TransportConf = streamingTransportConf(conf)

  /** Per partition egress and acknowledgement state, created on first use. */
  private val streams = new ConcurrentHashMap[Int, PartitionStream]()

  /** One session per consumer channel, keyed by the channel's own identity. */
  private val sessions = new ConcurrentHashMap[String, ConsumerSession]()

  /** Serialises the two registry updates and the session-slot transfer performed by a reconnect. */
  private val sessionRegistryLock = new Object()

  /**
   * The session currently serving each '''logical''' consumer, keyed by the identity that consumer
   * declares on its own heartbeats.
   */
  private val sessionsByConsumer = new ConcurrentHashMap[String, ConsumerSession]()

  /** When each logical consumer was last heard from, kept beyond the life of its session. */
  private val consumerLastSeenMs = new ConcurrentHashMap[String, java.lang.Long]()

  private val egressTicket = new AtomicLong(0L)

  /** Monotonic ticket that makes equally served ready sessions stable in the worker queue. */
  private val readyTicket = new AtomicLong(0L)

  /** Sessions that have work a bounded data-plane worker can make progress on. */
  private val readySessions = new PriorityBlockingQueue[ConsumerSession](
    INITIAL_READY_SESSION_CAPACITY,
    new Comparator[ConsumerSession] {
      override def compare(left: ConsumerSession, right: ConsumerSession): Int = {
        val byBytes = java.lang.Long.compare(left.readyBytesSnapshot, right.readyBytesSnapshot)
        if (byBytes != 0) byBytes
        else java.lang.Long.compare(left.readyTicketValue, right.readyTicketValue)
      }
    })

  /** One accepted worker drain at a time is enough; it consumes the whole ready queue. */
  private val drainScheduled = new AtomicBoolean(false)

  /** Bounded-worker refusals, which fail the producer rather than running heavy work on Netty. */
  private val dataPlaneRefusals = new AtomicLong(0L)

  /** One-shot close transition, so teardown happens exactly once however many threads reach it. */
  private val closed = new AtomicBoolean(false)

  /** Whether the producing task has completed and handed retained output to the executor owner. */
  private val producerTaskComplete = new AtomicBoolean(false)

  /** One-shot generation-withdrawal transition, deliberately distinct from the close transition. */
  private val withdrawn = new AtomicBoolean(false)

  /** The retained output of the map task this handler serves, or `None` when it is not servable. */
  private def retainedOutput: Option[MemorySpillManager] = {
    if (blockResolver.registeredGeneration(shuffleId, mapId).contains(taskAttemptId)) {
      blockResolver.producerFor(shuffleId, mapId)
    } else {
      None
    }
  }

  /**
   * The producing task's attributes, which decide flush order between blocks that are otherwise
   * equally ready.
   */
  private val attempt = new AtomicReference[EgressPriority](DEFAULT_PRIORITY)

  private val bytesWritten = new AtomicLong(0L)
  private val blocksWritten = new AtomicLong(0L)
  private val writeNanos = new AtomicLong(0L)
  private val throttles = new AtomicLong(0L)
  private val acks = new AtomicLong(0L)
  private val heartbeats = new AtomicLong(0L)
  private val retransmittedBlocks = new AtomicLong(0L)
  private val misaddressedMessages = new AtomicLong(0L)
  private val misaddressReported = new AtomicBoolean(false)
  private val versionMismatch = new AtomicBoolean(false)
  private val unservableBlocks = new AtomicLong(0L)
  private val refusedAcks = new AtomicLong(0L)
  private val resumedSessions = new AtomicLong(0L)
  private val supersededSessions = new AtomicLong(0L)
  private val renameRefusals = new AtomicLong(0L)
  private val duplicateAcks = new AtomicLong(0L)
  private val expiredSessions = new AtomicLong(0L)
  private val expiredConsumers = new AtomicLong(0L)
  private val refusedSessions = new AtomicLong(0L)
  private val unauthenticatedRefusals = new AtomicLong(0L)
  private val identityConflicts = new AtomicLong(0L)
  private val untrackedConsumers = new AtomicLong(0L)
  private val deferredBlocks = new AtomicLong(0L)
  private val sessionCapacityReported = new AtomicBoolean(false)
  private val consumerCapacityReported = new AtomicBoolean(false)

  /** Consumer-session slots currently claimed, which is the concurrency ceiling made atomic. */
  private val liveSessionSlots = new AtomicInteger(0)

  /** Consumer sessions this producer has ever accepted, as opposed to those it holds now. */
  private val acceptedSessions = new AtomicLong(0L)

  /** Subscribers currently admitted per reduce partition. */
  private val subscribersByPartition = new ConcurrentHashMap[Integer, AtomicInteger]()

  // Partitions whose end of stream has been requested.
  private val terminationRequests = new AtomicInteger(0)

  /**
   * Cumulative totals, per map output, for the conditions whose frequency is not this producer's to
   * decide.
   */
  private val egressFailures = new AtomicLong(0L)
  private val replayRefusals = new AtomicLong(0L)
  private val subscriptionRefusals = new AtomicLong(0L)
  private val lossyChannelClosures = new AtomicLong(0L)
  private val framingBudgetRefusals = new AtomicLong(0L)
  private val throttleReports = new AtomicLong(0L)
  private val orderingDeferrals = new AtomicLong(0L)

  /**
   * Emits one report through an executor-scoped window, or accounts it and stays quiet.
   *
   * @param aggregator the executor-scoped window that decides, named by the caller so the scope
   *     is visible at the report rather than only at the declaration
   * @param total the running total for this map output, advanced here so a caller cannot forget
   *     to
   * @param entry the report, built only when it is going to be emitted or traced
   * @param cause the failure to attach, or `null` when the report is not about one
   */
  private def reportBounded(
      aggregator: MemorySpillManager.ExecutorLogAggregator,
      total: AtomicLong,
      entry: => MessageWithContext,
      cause: Throwable = null): Unit = {
    val occurrences = total.incrementAndGet()
    aggregator.record(clock.getTimeMillis()) match {
      case Some(summary) =>
        val message = entry +
          log" (${MDC(NUM_EVENTS, occurrences)} occurrence(s) on this map output, " +
          log"${MDC(COUNT, summary.occurrences)} on this executor, " +
          log"${MDC(NUM_SKIPPED, summary.unreported)} not reported individually)"
        if (cause == null) logWarning(message) else logWarning(message, cause)
      case None =>
        if (debugEnabled) {
          if (cause == null) logDebug(entry) else logDebug(entry, cause)
        }
    }
  }

  /** Reports an asynchronous write failure to the notifier. */
  private val writeFailureListener: ChannelFutureListener = new ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = {
      if (!future.isSuccess) {
        errorNotifier.setError(future.cause())
        // One write is issued per block, so one failing channel fails every write on it.
        reportBounded(StreamingShuffleServerHandler.egressFailureLogAggregator, egressFailures,
          log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} failed to write to the consumer " +
            log"channel", future.cause())
      }
    }
  }

  if (debugEnabled) {
    logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} egress handler created on " +
      log"transport module ${MDC(VALUE, transportConf.getModuleName())} with " +
      log"${MDC(CONFIG, TCP_KEEP_ALIVE_KEY)} set to " +
      log"${MDC(STATUS, transportConf.enableTcpKeepAlive())} and a per block cap of " +
      log"${MDC(NUM_BYTES, MAX_PAYLOAD_BYTES)} payload byte(s)")
  }

  // Producing task identity, which decides flush order

  /**
   * Records the attributes of the task attempt whose output this handler streams.
   *
   * @param priority the attributes of the producing attempt
   */
  def registerTaskAttempt(priority: EgressPriority): Unit = {
    require(priority != null, "The streaming shuffle egress priority must not be null.")
    attempt.set(priority)
    if (debugEnabled) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} egress will be ordered for " +
        log"stage ${MDC(VALUE, priority.stageId)} task attempt " +
        log"${MDC(COUNT, priority.taskAttemptId)}, attempt number " +
        log"${MDC(MAX_ATTEMPTS, priority.attemptNumber)}")
    }
  }

  /** The attributes flush ordering is currently derived from. */
  def taskPriority: EgressPriority = attempt.get()

  /** Transfers silent-consumer expiry from the task's replay loop to executor maintenance. */
  def markProducerTaskComplete(): Unit = producerTaskComplete.set(true)

  // Egress: framing, checksumming and enqueueing

  /**
   * Enqueues one block for one partition, referenced by its payload size.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param payloadBytes the block's payload size, at most [[MAX_PAYLOAD_BYTES]]
   * @return the sequence number assigned to the block
   */
  def enqueueBlock(partitionId: Int, payloadBytes: Int): Long = {
    enqueueBlock(partitionId, payloadBytes, attempt.get())
  }

  /**
   * Enqueues one block for one partition under an explicit priority.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param payloadBytes the block's payload size, at most [[MAX_PAYLOAD_BYTES]]
   * @param priority the attributes of the attempt that produced the block
   * @return the sequence number assigned to the block
   * @throws IllegalArgumentException if the size is negative or larger than the block cap
   * @throws IllegalStateException if the partition's stream has already been terminated, if the
   *     retained store does not hold the block under the assigned sequence number, or if another
   *     producer advanced this partition's numbering concurrently
   */
  def enqueueBlock(partitionId: Int, payloadBytes: Int, priority: EgressPriority): Long = {
    require(payloadBytes >= 0,
      s"A streaming shuffle block payload size must not be negative, but was $payloadBytes.")
    require(payloadBytes <= MAX_PAYLOAD_BYTES,
      s"A streaming shuffle block carries $payloadBytes byte(s), which exceeds the protocol cap " +
        s"of $MAX_PAYLOAD_BYTES byte(s). Frame the payload into smaller blocks before offering it.")
    require(priority != null, "The streaming shuffle egress priority must not be null.")
    val stream = streamFor(partitionId)
    if (stream.terminationRequested.get()) {
      throw new IllegalStateException(s"Streaming shuffle $shuffleId partition $partitionId has " +
        "already been terminated, so no further block may be enqueued for it.")
    }
    val sequenceNumber = stream.nextSequenceNumber.get()
    // Asserted, not assumed.
    val store = retainedOutput.getOrElse {
      throw new IllegalStateException(s"Streaming shuffle $shuffleId map $mapId has no retained " +
        s"output registered for generation $taskAttemptId, so partition $partitionId block " +
        s"$sequenceNumber cannot be streamed.")
    }
    if (!store.retainsBlock(partitionId, sequenceNumber)) {
      throw new IllegalStateException(s"Streaming shuffle $shuffleId partition $partitionId " +
        s"block $sequenceNumber was offered for egress but the retained store does not hold it.")
    }
    val framedBytes = framedLengthOf(payloadBytes)
    // The single commit point: the block is counted by the act of advancing the numbering past it,
    // and nothing above this line has changed any state a consumer can observe.
    if (!stream.nextSequenceNumber.compareAndSet(sequenceNumber, sequenceNumber + 1L)) {
      throw new IllegalStateException(s"Streaming shuffle $shuffleId partition $partitionId had " +
        s"its block numbering advanced past $sequenceNumber while a block was being offered for " +
        "egress, which means two producers are sharing one handler. One map output must be " +
        "streamed by one producer generation.")
    }
    stream.highestOffered.set(sequenceNumber)
    // Queued volume is accounted per session, and aggregated for the writer by summing over them.
    val subscribed = fanOut(partitionId, sequenceNumber, framedBytes, priority, replay = false)
    if (debugEnabled) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} enqueued block ${MDC(COUNT, sequenceNumber)} of " +
        log"${MDC(NUM_BYTES, framedBytes)} framed byte(s) for ${MDC(VALUE, subscribed)} " +
        log"subscribed consumer(s)")
    }
    requestDrain()
    if (debugEnabled && subscribed == 0) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} is holding block ${MDC(COUNT, sequenceNumber)}: " +
        log"egress is paced, no consumer has subscribed, or no channel is writable")
    }
    sequenceNumber
  }

  /**
   * Queues one block reference on every session subscribed to its partition.
   *
   * @return the number of sessions the block was queued for
   */
  private def fanOut(
      partitionId: Int,
      sequenceNumber: Long,
      framedBytes: Int,
      priority: EgressPriority,
      replay: Boolean): Int = {
    if (closed.get()) {
      0
    } else {
      var queued = 0
      sessions.values().asScala.foreach { session =>
        if (session.subscribedTo(partitionId)) {
          if (!registerRetainedConsumer(session)) {
            // A session may subscribe just before the writer publishes its retained store.
            releaseSession(session,
              "the retained output's consumer cap refused its registration")
          } else if (session.owedBlocksFor(partitionId) > 0L) {
            // Order before immediacy.
            if (session.deferOwed(partitionId, sequenceNumber, sequenceNumber)) {
              deferredBlocks.incrementAndGet()
              markSessionReady(session)
            }
          } else {
            val pending = PendingBlock(partitionId, sequenceNumber, framedBytes, priority,
              egressTicket.getAndIncrement(), replay)
            if (session.offer(pending)) {
              queued += 1
              markSessionReady(session)
            } else if (session.deferOwed(partitionId, sequenceNumber, sequenceNumber)) {
              deferredBlocks.incrementAndGet()
              markSessionReady(session)
            }
          }
        }
      }
      queued
    }
  }

  /**
   * Queues up to one page of the blocks owed to one consumer for one partition.
   *
   * @return the number of references queued
   */
  private def topUpOwed(session: ConsumerSession, partitionId: Int, budget: Int): Int = {
    val store = retainedOutput
    // The acknowledged prefix first, in one step.
    session.discardOwedThrough(partitionId, session.ackPosition(partitionId))
    if (store.isEmpty) {
      // This generation cannot serve its retained output any more -- it was withdrawn, superseded
      // by a newer attempt, or released when this executor stopped streaming -- so every position
      // still owed to this consumer is unservable for good, and the protocol requires that the
      // consumer be told.
      val firstOwed = session.discardAllOwed(partitionId)
      if (firstOwed != MemorySpillManager.UNSET_SEQUENCE) {
        abortUnservableStream(session, partitionId, firstOwed)
      } else {
        considerTerminationReady(session, partitionId)
      }
      return 0
    }
    var queued = 0
    var keepGoing = !session.isClosed
    while (keepGoing && queued < budget) {
      val sequenceNumber = session.takeOwed(partitionId)
      if (sequenceNumber == MemorySpillManager.UNSET_SEQUENCE) {
        session.deindexIfSettled(partitionId)
        considerTerminationReady(session, partitionId)
        keepGoing = false
      } else {
        val payloadBytes = store.flatMap(_.retainedPayloadLength(partitionId, sequenceNumber))
        val framedBytes = framedLengthOf(payloadBytes.getOrElse(0))
        val replay = sequenceNumber <= session.sentPosition(partitionId)
        val pending = PendingBlock(partitionId, sequenceNumber, framedBytes, attempt.get(),
          egressTicket.getAndIncrement(), replay)
        if (session.offer(pending)) {
          queued += 1
          markSessionReady(session)
        } else {
          // The queue reached its ceiling again.
          if (session.deferOwed(partitionId, sequenceNumber, sequenceNumber)) {
            markSessionReady(session)
          }
          keepGoing = false
        }
      }
    }
    queued
  }

  /**
   * Queues up to one page of everything owed to one consumer, over every partition it is owed for.
   */
  private def topUpSession(session: ConsumerSession): Int = {
    var queued = 0
    session.owedPartitionIds.foreach { partitionId =>
      queued += topUpOwed(session, partitionId, REPLAY_PAGE_BLOCKS)
    }
    queued
  }

  /** Framed size of a block carrying a payload of the given length. */
  private def framedLengthOf(payloadBytes: Int): Int = {
    payloadBytes + DataBlockMessage.FRAMING_OVERHEAD_BYTES
  }

  // Egress: draining onto the channel

  /**
   * Writes as much of the queued output as pacing and the socket presently allow.
   *
   * @return the number of framed bytes handed to the channel by this call
   */
  def flushPending(): Long = {
    val before = bytesWritten.get()
    sessions.values().asScala.foreach(markSessionReady)
    requestDrain()
    backpressure.awaitDataPlaneIdle(DATA_PLANE_AWAIT_TIMEOUT_MS)
    math.max(0L, bytesWritten.get() - before)
  }

  /** Marks one session ready for the bounded data-plane workers. */
  private def markSessionReady(session: ConsumerSession): Unit = {
    if (!closed.get() && !session.isClosed && session.hasDrainWork &&
        session.readyQueued.compareAndSet(false, true)) {
      session.readyBytesSnapshot = session.orderingKey
      session.readyTicketValue = readyTicket.getAndIncrement()
      readySessions.offer(session)
    }
  }

  /** Requests one worker pass over the ready-session queue. */
  private def requestDrain(): Unit = {
    if (!closed.get() && !readySessions.isEmpty &&
        drainScheduled.compareAndSet(false, true)) {
      val accepted = backpressure.executeDataPlane(this, new Runnable {
        override def run(): Unit = {
          try {
            guard(drainReadySessions())
          } finally {
            drainScheduled.set(false)
            if (!closed.get() && !readySessions.isEmpty) {
              requestDrain()
            }
          }
        }
      })
      if (!accepted) {
        drainScheduled.set(false)
        val failure = new SparkException(
          s"Streaming shuffle $shuffleId map $mapId could not enqueue an egress drain because " +
            "the executor-wide data-plane worker queue is full.")
        errorNotifier.setError(failure)
        reportBounded(StreamingShuffleServerHandler.dataPlaneRefusalLogAggregator,
          dataPlaneRefusals,
          log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not enqueue an egress drain: " +
            log"${MDC(REASON, "the executor-wide data-plane worker queue is full")}", failure)
      }
    }
  }

  /** Drains only sessions that an event or producer action marked ready, with no registry scan. */
  private def drainReadySessions(): Long = {
    var total = 0L
    var session = readySessions.poll()
    while (session != null && !closed.get()) {
      session.readyQueued.set(false)
      if (!session.isClosed) {
        total += drainSession(session)
      }
      session = readySessions.poll()
    }
    total
  }

  private def drainSession(session: ConsumerSession): Long = {
    var total = 0L
    var again = true
    while (again) {
      again = false
      if (session.draining.compareAndSet(false, true)) {
        session.drainWakeup.set(false)
        val written = try {
          drainOnce(session)
        } finally {
          session.draining.set(false)
        }
        total += written
        // Re-run when this pass made progress and work remains, or when a contending thread left a
        // note while the guard was held, so a block offered during the pass is not stranded.
        again = !session.queue.isEmpty && (written > 0L || session.drainWakeup.get())
      } else {
        session.drainWakeup.set(true)
      }
    }
    total
  }

  /** One pass of one session's drain loop, executed by the single thread holding its guard. */
  private def drainOnce(session: ConsumerSession): Long = {
    val channel = session.channel
    if (session.isClosed || !channel.isActive()) {
      0L
    } else {
      val startedAtNanos = clock.nanoTime()
      var written = 0L
      var flushNeeded = false
      var keepGoing = true
      // Three refusals, three retry strategies, and the distinction is what keeps a throttled
      // channel from spinning.
      var pacingDelayMs = 0L
      var budgetDelayed = false
      var writabilityDelayed = false
      while (keepGoing) {
        if (!channel.isWritable()) {
          // The socket's outbound buffer is full, so leaving the block queued is correct -- but the
          // queue has to be revisited by something.
          writabilityDelayed = true
          keepGoing = false
        } else {
          var pending = session.queue.poll()
          if (pending == null && topUpSession(session) > 0) {
            // The queue is empty but this consumer is still owed blocks a ceiling refused earlier;
            // topping up here is what makes the bounded queue a delay rather than a truncation.
            pending = session.queue.poll()
          }
          if (pending == null) {
            keepGoing = false
          } else if (leavesSequenceHole(session, pending)) {
            deferSequenceHole(session, pending)
          } else if (!session.canTrackSent(pending.partitionId, pending.sequenceNumber)) {
            if (session.requeue(pending)) {
              throttles.incrementAndGet()
              budgetDelayed = true
            }
            keepGoing = false
          } else if (!backpressure.tryReserveMetadataQuota(SENT_BLOCK_METADATA_BYTES)) {
            if (session.requeue(pending)) {
              throttles.incrementAndGet()
              budgetDelayed = true
            }
            keepGoing = false
          } else if (!backpressure.tryReserveTransientQuota(pending.framedBytes.toLong)) {
            // The executor already has as many transient framing copies in flight as it permits.
            val requeued = session.requeue(pending)
            backpressure.releaseMetadataQuota(SENT_BLOCK_METADATA_BYTES)
            if (requeued) {
              throttles.incrementAndGet()
              budgetDelayed = true
              reportBounded(StreamingShuffleServerHandler.framingBudgetLogAggregator,
                framingBudgetRefusals,
                log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
                  log"${MDC(PARTITION_ID, pending.partitionId)} is holding " +
                  log"${MDC(NUM_BYTES, pending.framedBytes)} framed byte(s) because this " +
                  log"executor " +
                  log"already has " +
                  log"${MDC(MAX_SIZE, backpressure.reservedTransientQuotaBytes)} byte(s) of " +
                  log"streaming shuffle egress in flight inside its aggregate ceiling of " +
                  log"${MDC(THRESHOLD, backpressure.receiveQuotaBytes)} byte(s)")
            }
            keepGoing = false
          } else if (!admitForEgress(session, pending)) {
            // Pacing and credit are consulted only after the allocation gates above have succeeded.
            val requeued = session.requeue(pending)
            backpressure.releaseTransientQuota(pending.framedBytes.toLong)
            backpressure.releaseMetadataQuota(SENT_BLOCK_METADATA_BYTES)
            if (requeued) {
              throttles.incrementAndGet()
              pacingDelayMs = math.max(pacingDelayMs,
                rateLimiter.millisUntilAvailable(pending.framedBytes.toLong))
              reportBounded(
                StreamingShuffleServerHandler.throttleLogAggregator, throttleReports,
                log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
                  log"${MDC(PARTITION_ID, pending.partitionId)} is throttled holding " +
                  log"${MDC(NUM_BYTES, pending.framedBytes)} framed byte(s); " +
                  log"${MDC(VALUE, rateLimiter.availableTokens)} token(s) available and " +
                  log"${MDC(THRESHOLD, session.outstandingFor(pending.partitionId))} " +
                  log"block(s) unacknowledged by the consumer")
            }
            keepGoing = false
          } else if (writeBlock(session, pending, sentMetadataReserved = true)) {
            written += pending.framedBytes.toLong
            flushNeeded = true
          }
        }
      }
      // One flush for the whole batch rather than one per block: the two mebibyte cap only
      // pipelines if consecutive blocks share a syscall instead of trickling out one at a time.
      if (flushNeeded) {
        channel.flush()
      }
      emitDeferredTerminations(session)
      if (pacingDelayMs > 0L) {
        scheduleRetryDrain(session, pacingDelayMs)
      } else if (budgetDelayed || writabilityDelayed) {
        // Both conditions are released by write completions rather than by a clock, and a
        // completion releases them without draining anything, so both need a timer.
        scheduleRetryDrain(session, FRAMING_BUDGET_RETRY_WAIT_MS)
      }
      writeNanos.addAndGet(math.max(0L, clock.nanoTime() - startedAtNanos))
      written
    }
  }

  /** Whether writing this block would leave a gap in its partition's sequence for this consumer. */
  private def leavesSequenceHole(session: ConsumerSession, pending: PendingBlock): Boolean = {
    val frontier = math.max(session.sentPosition(pending.partitionId),
      session.ackPosition(pending.partitionId))
    pending.sequenceNumber > frontier + 1L
  }

  /**
   * Puts a block that would have left a gap back onto the owed run, together with the run it
   * skipped.
   */
  private def deferSequenceHole(session: ConsumerSession, pending: PendingBlock): Unit = {
    val partitionId = pending.partitionId
    val frontier = math.max(session.sentPosition(partitionId), session.ackPosition(partitionId))
    val firstMissing = math.max(0L, frontier + 1L)
    // The reference this pass polled is not going back on the queue -- the owed run below covers
    // its position and the top-up will queue it in sequence -- so its charge against the queue's
    // two ceilings has to be returned here, exactly as [[writeBlock]] returns the charge of a block
    // that leaves for good.
    session.releasePending(pending)
    if (session.deferOwed(partitionId, firstMissing, pending.sequenceNumber)) {
      markSessionReady(session)
    }
    reportBounded(StreamingShuffleServerHandler.orderingLogAggregator, orderingDeferrals,
      log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} held block " +
        log"${MDC(COUNT, pending.sequenceNumber)} for consumer " +
        log"${MDC(SESSION_ID, session.consumerId)} because position " +
        log"${MDC(THRESHOLD, firstMissing)} has not been sent yet; the run is queued in sequence " +
        log"instead")
  }

  /**
   * Arranges for one more drain attempt after the given delay.
   *
   * @param session the session to drain again
   * @param requestedDelayMs how long to wait, clamped into [1, [[MAX_REFILL_WAIT_MS]]]
   */
  private def scheduleRetryDrain(session: ConsumerSession, requestedDelayMs: Long): Unit = {
    if (!closed.get() && !session.isClosed &&
        session.refillScheduled.compareAndSet(false, true)) {
      val delayMs = math.max(1L, math.min(requestedDelayMs, MAX_REFILL_WAIT_MS))
      try {
        session.channel.eventLoop().schedule(new Runnable {
          override def run(): Unit = {
            session.refillScheduled.set(false)
            markSessionReady(session)
            requestDrain()
          }
        }, delayMs, TimeUnit.MILLISECONDS)
      } catch {
        case NonFatal(e) =>
          // A rejected schedule means the event loop is shutting down, which the inactivity
          // callback handles; clearing the flag keeps a later attempt possible.
          session.refillScheduled.set(false)
          if (debugEnabled) {
            logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not schedule a " +
              log"refill drain for consumer ${MDC(SESSION_ID, session.consumerId)}: " +
              log"${MDC(REASON, e.getMessage())}")
          }
      }
    }
  }

  /**
   * Frames one block from the retained store and writes it to one consumer's channel.
   *
   * @return true if a frame was handed to the channel
   */
  private def writeBlock(
      session: ConsumerSession,
      pending: PendingBlock,
      sentMetadataReserved: Boolean): Boolean = {
    val partitionId = pending.partitionId
    val sequenceNumber = pending.sequenceNumber
    val framedBytes = pending.framedBytes.toLong
    val payload = retainedOutput.flatMap(_.retainedPayload(partitionId, sequenceNumber))
    session.releasePending(pending)
    payload match {
      case Some(bytes) =>
        val block = DataBlockMessage.withComputedChecksumAndOwnedPayload(
          shuffleId, mapId, partitionId, sequenceNumber, bytes)
        if (!session.recordSent(
            partitionId, sequenceNumber, framedBytes, sentMetadataReserved)) {
          backpressure.releaseTransientQuota(framedBytes)
          false
        } else {
          considerTerminationReady(session, partitionId)
          session.channel.write(sendable(block))
            .addListener(releaseTransientOn(framedBytes))
            .addListener(writeFailureListener)
          bytesWritten.addAndGet(framedBytes)
          blocksWritten.incrementAndGet()
          if (pending.replay) {
            retransmittedBlocks.incrementAndGet()
          }
          if (debugEnabled) {
            logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
              log"${MDC(PARTITION_ID, partitionId)} wrote block ${MDC(COUNT, sequenceNumber)} of " +
              log"${MDC(NUM_BYTES, framedBytes)} framed byte(s) to consumer " +
              log"${MDC(SESSION_ID, session.consumerId)}, checksum ${MDC(VALUE, block.checksum())}")
          }
          true
        }
      case None =>
        // No frame was built, so no copy is in flight and the reservation the drain took for this
        // block is returned at once.
        backpressure.releaseTransientQuota(framedBytes)
        if (sentMetadataReserved) {
          backpressure.releaseMetadataQuota(SENT_BLOCK_METADATA_BYTES)
        }
        unservableBlocks.incrementAndGet()
        if (session.ackPosition(partitionId) >= sequenceNumber) {
          // Stale queue entry: this consumer confirmed the block before the drain reached it, so
          // there is nothing owed and nothing to report.
          if (debugEnabled) {
            logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
              log"${MDC(PARTITION_ID, partitionId)} dropped block ${MDC(COUNT, sequenceNumber)} " +
              log"for consumer ${MDC(SESSION_ID, session.consumerId)}: " +
              log"${MDC(REASON, "it had already been acknowledged")}")
          }
        } else {
          abortUnservableStream(session, partitionId, sequenceNumber)
        }
        false
    }
  }

  /** Returns a transient frame-copy reservation when its channel write has completed. */
  private def releaseTransientOn(bytes: Long): ChannelFutureListener = new ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit =
      backpressure.releaseTransientQuota(bytes)
  }

  /** Ends a subscribed stream that this generation has become unable to serve. */
  private def abortIfUnservable(session: ConsumerSession, partitionId: Int): Unit = {
    if (retainedOutput.isEmpty && session.subscribedTo(partitionId)) {
      val stream = streams.get(partitionId)
      if (stream != null) {
        val committedBlocks = stream.nextSequenceNumber.get()
        val sentBlocks = session.sentPosition(partitionId) + 1L
        if (committedBlocks > sentBlocks) {
          abortUnservableStream(session, partitionId, sentBlocks)
        }
      }
    }
  }

  /** Answers a consumer that is owed a block this producer can no longer produce. */
  private def abortUnservableStream(
      session: ConsumerSession,
      partitionId: Int,
      sequenceNumber: Long): Unit = {
    val stream = streamFor(partitionId)
    val committedBlocks = stream.nextSequenceNumber.get()
    val firstReport = session.markStreamAborted(partitionId)
    if (firstReport) {
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
        log"${MDC(MAP_ID, mapId)} partition ${MDC(PARTITION_ID, partitionId)} cannot serve block " +
        log"${MDC(COUNT, sequenceNumber)} to consumer ${MDC(SESSION_ID, session.consumerId)}, " +
        log"which has acknowledged only up to " +
        log"${MDC(VALUE, session.ackPosition(partitionId))}; the stream is ended at " +
        log"${MDC(NUM_BLOCKS, committedBlocks)} announced block(s) so that the consumer " +
        log"recomputes rather than waiting for bytes that no longer exist")
      guard {
        writeControl(session, new StreamTerminationMessage(
          shuffleId, mapId, partitionId, committedBlocks))
      }
      errorNotifier.setError(new SparkException(s"Streaming shuffle $shuffleId map $mapId " +
        s"partition $partitionId could not serve block $sequenceNumber to a consumer that had " +
        s"acknowledged only up to ${session.ackPosition(partitionId)}, so its retained window no " +
        "longer holds output a subscribed consumer is owed. The map output is reproduced rather " +
        "than reported as complete."))
    } else if (debugEnabled) {
      logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} could not serve a further block " +
        log"${MDC(COUNT, sequenceNumber)} to consumer ${MDC(SESSION_ID, session.consumerId)}")
    }
  }

  /** Wraps one message as the body of a one-way RPC, which is how every streaming frame travels. */
  private def sendable(message: StreamingShuffleMessage): OneWayMessage = {
    new OneWayMessage(message.toManagedBuffer())
  }

  /** Writes a control frame immediately, bypassing the pacing verdict but not the accounting. */
  private def writeControl(
      session: ConsumerSession,
      message: StreamingShuffleMessage): ChannelFuture = {
    val framedBytes = StreamingShuffleMessage.framedLength(message.encodedLength()).toLong
    val charged = rateLimiter.tryAcquire(framedBytes)
    val future = session.channel.writeAndFlush(sendable(message))
    future.addListener(writeFailureListener)
    bytesWritten.addAndGet(framedBytes)
    if (debugEnabled && !charged) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, message.partitionId())} sent an uncharged control frame of " +
        log"${MDC(NUM_BYTES, framedBytes)} byte(s) because the egress bucket was empty")
    }
    future
  }

  // Liveness and orderly end of stream

  /**
   * Emits a producer heartbeat for one partition.
   *
   * @param partitionId the reduce partition whose stream is being kept alive
   * @return true if the frame was handed to the channel, false if there is no active channel
   */
  def sendHeartbeat(partitionId: Int): Boolean = {
    val stream = streamFor(partitionId)
    val subscribers = sessions.values().asScala.filter { session =>
      !session.isClosed && session.channel.isActive() && session.subscribedTo(partitionId)
    }.toSeq
    var sent = false
    val nextPosition = stream.nextSequenceNumber.get()
    subscribers.foreach { session =>
      // The position announced is the next one this producer will commit, which is exactly the
      // reading a consumer's own heartbeat carries, so one header field means one thing in both
      // directions.
      val stamped = backpressure
        .heartbeatFor(consumerLedgerKey(partitionId, session.consumerId), nextPosition)
        .getOrElse(new HeartbeatMessage(shuffleId, mapId, partitionId, nextPosition))
      writeControl(session, stamped)
      sent = true
    }
    sent
  }

  /**
   * Signals the orderly end of one partition's stream.
   *
   * @param partitionId the reduce partition whose stream is complete
   * @return true if the terminator was written by this call, false if it was deferred
   */
  def terminateStream(partitionId: Int): Boolean = terminateStreams(Seq(partitionId)) == 1

  /**
   * Signals the orderly end of several partitions' streams with a single egress pass.
   *
   * @param partitionIds the reduce partitions whose streams are complete
   * @return how many of them had their terminator written by this call rather than deferred
   */
  def terminateStreams(partitionIds: Seq[Int]): Int = {
    val requestedPartitions = partitionIds.toSet
    partitionIds.foreach { partitionId =>
      val stream = streamFor(partitionId)
      // Counted on the edge, so the hot-path gate sees a request once however often termination is
      // requested for the same partition -- which a retried or repeated finish does.
      if (stream.terminationRequested.compareAndSet(false, true)) {
        terminationRequests.incrementAndGet()
      }
      // The numbering is the count: it has been advanced exactly once per accepted block and never
      // for a refused one, so it is the total a consumer can verify its own tally against.
      stream.totalBlocksAtTermination.set(stream.nextSequenceNumber.get())
    }
    sessions.values().asScala.foreach { session =>
      session.subscribedPartitions.foreach { partitionId =>
        if (requestedPartitions.contains(partitionId)) {
          considerTerminationReady(session, partitionId)
        }
      }
      markSessionReady(session)
    }
    requestDrain()
    var written = 0
    partitionIds.foreach { partitionId =>
      if (terminationSignalled(partitionId)) {
        written += 1
      }
    }
    written
  }

  /** Whether every consumer subscribed to one partition has been told its stream ended. */
  private def terminationSignalled(partitionId: Int): Boolean = {
    val subscribers = subscriberCount(partitionId)
    val delivered = terminationsDelivered(partitionId)
    val sent = subscribers > 0 && delivered >= subscribers
    // One record per partition, and a wide shuffle has tens of thousands of them, so this stays
    // under the streaming debug key: deferral is routine whenever egress is paced or the socket is
    // full rather than an incident, the terminator's eventual delivery is what matters, and a
    // failure to deliver it is reported at warning level regardless of this gate.
    if (!sent && debugEnabled) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} deferred its end of stream marker: " +
        log"${MDC(COUNT, delivered)} of ${MDC(VALUE, subscribers)} subscribed consumer(s) have " +
        log"been told, with ${MDC(NUM_BLOCKS, pendingBlocksFor(partitionId))} block(s) queued")
    }
    sent
  }

  /** Adds one partition to a session's ready-termination queue when all preceding data has left. */
  private def considerTerminationReady(session: ConsumerSession, partitionId: Int): Unit = {
    val stream = streams.get(partitionId)
    if (stream != null &&
        stream.terminationRequested.get() &&
        session.subscribedTo(partitionId) &&
        session.pendingBlocksFor(partitionId) <= 0L &&
        session.owedBlocksFor(partitionId) <= 0L &&
        session.caughtUpWith(partitionId, stream.highestOffered.get())) {
      if (session.enqueueReadyTermination(partitionId)) {
        markSessionReady(session)
      }
    }
  }

  /** Emits terminators whose partitions have become ready for one consumer. */
  private def emitDeferredTerminations(session: ConsumerSession): Unit = {
    if (terminationRequests.get() > 0 && !session.isClosed && session.channel.isActive()) {
      var partitionId = session.pollReadyTermination()
      while (partitionId >= 0) {
        val readyPartitionId = partitionId
        val stream = streams.get(readyPartitionId)
        if (stream != null && session.claimTermination(readyPartitionId)) {
          val totalBlocks = stream.totalBlocksAtTermination.get()
          val future = writeControl(session, new StreamTerminationMessage(
            shuffleId, mapId, readyPartitionId, totalBlocks))
          future.addListener(new ChannelFutureListener {
            override def operationComplete(completed: ChannelFuture): Unit = {
              if (completed.isSuccess) {
                if (session.confirmTermination(readyPartitionId)) {
                  stream.deliveredTerminations.incrementAndGet()
                }
                session.recordSubscriptionCompletion(
                  readyPartitionId, stream.highestOffered.get())
                retireCompletedConsumer(session)
                // One record per partition per subscribed consumer -- the largest log source this
                // handler has -- so the per-consumer detail is debug only.
                if (debugEnabled) {
                  logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
                    log"${MDC(PARTITION_ID, readyPartitionId)} streamed " +
                    log"${MDC(NUM_BLOCKS, totalBlocks)} block(s) and signalled end of stream to " +
                    log"consumer ${MDC(SESSION_ID, session.consumerId)}")
                }
              } else {
                // The claim is surrendered so a reconnecting consumer is terminated properly, and
                // the failure travels to the task thread rather than being lost with the write.
                session.releaseTerminationClaim(readyPartitionId)
                considerTerminationReady(session, readyPartitionId)
                requestDrain()
                errorNotifier.setError(completed.cause())
                logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
                  log"${MDC(PARTITION_ID, readyPartitionId)} could not signal end of stream to " +
                  log"consumer ${MDC(SESSION_ID, session.consumerId)}", completed.cause())
              }
            }
          })
        }
        partitionId = session.pollReadyTermination()
      }
    }
  }

  /** How many subscribed consumers have had the terminator for one partition confirmed. */
  private def terminationsDelivered(partitionId: Int): Int = {
    val stream = streams.get(partitionId)
    if (stream == null) 0 else math.max(0, stream.deliveredTerminations.get())
  }

  /**
   * Blocks queued but not yet written for one partition, summed over every session.
   *
   * @param partitionId the reduce partition to measure
   * @return the number of queued, unwritten blocks across every subscribed consumer
   */
  def pendingBlocksFor(partitionId: Int): Long = {
    var total = 0L
    sessions.values().asScala.foreach { session =>
      // Owed blocks count as pending.
      total += session.pendingBlocksFor(partitionId) + session.owedBlocksFor(partitionId)
    }
    total
  }

  /**
   * Whether one partition's consumer has stopped acknowledging for longer than the liveness window.
   *
   * @param partitionId the reduce partition to test
   * @return true if the partition has unacknowledged output and has seen no progress in the
   *     window
   */
  def isConsumerStalled(partitionId: Int): Boolean = {
    stalled(partitionId, clock.getTimeMillis())
  }

  /** Every partition whose consumer has stopped acknowledging for longer than the window. */
  def stalledPartitions: Seq[Int] = {
    val nowMs = clock.getTimeMillis()
    val stalledIds = new ArrayBuffer[Int](streams.size())
    streams.keySet().asScala.foreach { partitionId =>
      if (stalled(partitionId, nowMs)) {
        stalledIds += partitionId
      }
    }
    stalledIds.sorted.toSeq
  }

  /** Whether any subscribed consumer of one partition has stopped *acknowledging*. */
  private def stalled(partitionId: Int, nowMs: Long): Boolean = {
    sessions.values().asScala.exists { session =>
      session.subscribedTo(partitionId) &&
        session.outstandingFor(partitionId) > 0L &&
        nowMs - session.lastAckProgressMs >= CONSUMER_LIVENESS_TIMEOUT_MS
    }
  }

  /**
   * Runs one round of the upkeep this producer's consumers need, from a thread that is not a
   * task's.
   *
   * @return the number of consumers this round retired, session and logical expiry together
   */
  def runMaintenance(): Int = {
    if (closed.get()) {
      0
    } else {
      val nowMs = clock.getTimeMillis()
      heartbeatSubscribedPartitions()
      val retired = expireStalledSessions(nowMs) + expireSilentConsumers(nowMs)
      sessions.values().asScala.foreach(markSessionReady)
      requestDrain()
      retired
    }
  }

  /** Sends a producer heartbeat on every partition a live consumer is reading. */
  private def heartbeatSubscribedPartitions(): Unit = {
    streams.keySet().asScala.foreach { partitionId =>
      val due = sessions.values().asScala.exists { session =>
        !session.isClosed && session.channel.isActive() && session.subscribedTo(partitionId) &&
          backpressure.shouldSendHeartbeat(consumerLedgerKey(partitionId, session.consumerId))
      }
      if (due) {
        sendHeartbeat(partitionId)
      }
    }
  }

  /**
   * Releases the sessions whose channels have been silent for a whole liveness window.
   *
   * @return the number of sessions retired
   */
  private def expireStalledSessions(nowMs: Long): Int = {
    var retired = 0
    sessions.values().asScala.toSeq.foreach { session =>
      val silentForMs = nowMs - session.lastInboundMs
      val channelAnswering = !session.isClosed && session.channel.isActive()
      val toleranceMs =
        if (channelAnswering) CONSUMER_EXPIRY_TIMEOUT_MS else CONSUMER_LIVENESS_TIMEOUT_MS
      // The writer's replay loop and this sweep reach the end of the replay budget at the same
      // clock reading.
      val taskOwnsOutstandingReplay =
        channelAnswering && !producerTaskComplete.get() && session.unacknowledgedBytes > 0L
      if (silentForMs >= toleranceMs && !taskOwnsOutstandingReplay) {
        retired += 1
        // Producer-local, never channel-global: one session is one consumer of THIS producer, and
        // the socket it arrived on may carry every other producer this executor serves, so closing
        // the channel for a condition local to this session severed streams that were healthy.
        releaseSession(session,
          s"it sent no frame for $silentForMs ms, beyond the $toleranceMs ms tolerated")
        // Bounded on the executor's window rather than emitted per session.
        reportBounded(
          StreamingShuffleServerHandler.sessionExpiryLogAggregator,
          expiredSessions,
          log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} released the egress " +
            log"session of consumer ${MDC(SESSION_ID, session.consumerId)} after " +
            log"${MDC(DURATION, silentForMs)} ms without a frame, beyond the " +
            log"${MDC(TIMEOUT, toleranceMs)} ms tolerated for a channel that was " +
            log"${MDC(REASON, if (channelAnswering) "still open" else "not answering")}; its " +
            log"position is retained so a reconnection resumes from it")
      }
    }
    retired
  }

  /**
   * Unregisters the logical consumers that have been silent long enough to have failed for good.
   *
   * @return the number of logical consumers unregistered
   */
  private def expireSilentConsumers(nowMs: Long): Int = {
    var retired = 0
    consumerLastSeenMs.entrySet().asScala.toSeq.foreach { entry =>
      val consumerId = entry.getKey
      val silentForMs = nowMs - entry.getValue.longValue()
      val hasLiveSession = sessionsByConsumer.containsKey(consumerId)
      if (!hasLiveSession && silentForMs >= CONSUMER_EXPIRY_TIMEOUT_MS &&
          consumerLastSeenMs.remove(consumerId, entry.getValue)) {
        retired += 1
        val freedBytes = retainedOutput.map(_.unregisterConsumer(consumerId)).getOrElse(0L)
        // Through the window rather than direct: one expiry is worth a warning, and a remote peer
        // deciding how many warnings this executor emits is not.
        reportBounded(StreamingShuffleServerHandler.consumerExpiryLogAggregator, expiredConsumers,
          log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} unregistered consumer " +
            log"${MDC(SESSION_ID, consumerId)} after ${MDC(DURATION, silentForMs)} ms of " +
            log"silence, beyond the ${MDC(TIMEOUT, CONSUMER_EXPIRY_TIMEOUT_MS)} ms expiry " +
            log"window, releasing ${MDC(NUM_BYTES, freedBytes)} byte(s); its spilled output " +
            log"stays readable")
      }
    }
    retired
  }

  /**
   * Performs the final unregister once a consumer has acknowledged every terminated subscription.
   */
  private def retireCompletedConsumer(session: ConsumerSession): Boolean = {
    val complete = session.allSubscriptionsComplete
    val releasedBytes = sessionRegistryLock.synchronized {
      // A late callback from a superseded channel must not unregister the replacement's stable
      // cursor or credit ledger.
      val isCurrent =
        sessions.get(session.sessionKey) == session &&
          sessionsByConsumer.get(session.consumerId) == session
      if (complete && isCurrent && session.claimCompletionRetirement() &&
          forgetSessionLocked(session, releaseSlot = true)) {
        consumerLastSeenMs.remove(session.consumerId)
        val freed =
          retainedOutput.map(_.unregisterConsumer(session.consumerId)).getOrElse(0L)
        releaseConsumerLedgers(session)
        session.close()
        Some(freed)
      } else {
        None
      }
    }
    releasedBytes.foreach { freedBytes =>
      // The executor listener multiplexes many producer routes on one transport channel.
      if (debugEnabled) {
        logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} finally unregistered " +
          log"consumer ${MDC(SESSION_ID, session.consumerId)} after all subscribed streams " +
          log"completed, releasing ${MDC(NUM_BYTES, freedBytes)} byte(s)")
      }
    }
    releasedBytes.isDefined
  }

  /**
   * Waits, up to a bound, for every subscribed consumer to have been sent everything it is owed.
   *
   * @param timeoutMs the longest this call may wait; a non-positive value polls once
   * @return true when nothing is queued for any consumer and every subscribed consumer of every
   *     terminated partition has been sent its terminator
   */
  def awaitDrain(timeoutMs: Long): Boolean = {
    val deadlineMs = clock.getTimeMillis() + math.max(0L, timeoutMs)
    var satisfied = drainSatisfied()
    while (!satisfied && clock.getTimeMillis() < deadlineMs && !closed.get()) {
      sessions.values().asScala.foreach(markSessionReady)
      requestDrain()
      backpressure.awaitDataPlaneIdle(DATA_PLANE_WAIT_STEP_MS)
      satisfied = drainSatisfied()
    }
    satisfied
  }

  /** Whether every session's queue is empty and every terminated partition has been terminated. */
  private def drainSatisfied(): Boolean = {
    // A session whose channel is open but which has sent nothing for a whole liveness window is
    // presumed lost and is not waited for.
    val nowMs = clock.getTimeMillis()
    val live = sessions.values().asScala
      .filterNot(_.isClosed)
      .filter(session => nowMs - session.lastInboundMs < CONSUMER_LIVENESS_TIMEOUT_MS)
      .toSeq
    live.forall(session => session.queue.isEmpty && session.owedBlocks == 0L) &&
      streams.values().asScala.forall { stream =>
      !stream.terminationRequested.get() ||
        live.forall(session =>
          !session.subscribedTo(stream.partitionId) ||
            session.terminationConfirmed(stream.partitionId))
    }
  }

  // Retransmission, bounded to the retained window

  /**
   * Replays retained blocks for one partition over the inclusive range the consumer asked for.
   *
   * @param partitionId the reduce partition whose blocks are being replayed
   * @param firstSequenceNumber inclusive lower bound of the requested range
   * @param lastSequenceNumber inclusive upper bound of the requested range
   * @return the number of blocks re-queued for the wire, zero if the request was deferred,
   *     refused or escalated
   */
  def retransmit(partitionId: Int, firstSequenceNumber: Long, lastSequenceNumber: Long): Int = {
    val session = sessions.values().asScala.find(_.subscribedTo(partitionId))
    session.map { subscribed =>
      replayGate(subscribed, partitionId, firstSequenceNumber, lastSequenceNumber, resume = false)
    }.getOrElse {
      misaddressedMessages.incrementAndGet()
      reportBounded(StreamingShuffleServerHandler.replayRefusalLogAggregator, replayRefusals,
        log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} discarded a retransmission request " +
          log"for partition ${MDC(PARTITION_ID, partitionId)}: " +
          log"${MDC(REASON, "no consumer is subscribed to it")}")
      0
    }
  }

  /**
   * The one gate every re-delivery of retained output passes through, whether an explicit
   * retransmission request asked for it or a consumer subscribing from a position it has reached
   * arranged it.
   *
   * @param session the consumer the blocks are for
   * @param partitionId the reduce partition whose blocks are being re-delivered
   * @param firstSequenceNumber inclusive lower bound of the range
   * @param lastSequenceNumber inclusive upper bound of the range
   * @param resume true when this is a first delivery to a subscribing consumer, false when it
   *     is a repair of blocks that consumer has already been sent
   * @return the number of blocks re-queued, zero if the request was deferred, refused or
   *     escalated
   */
  private def replayGate(
      session: ConsumerSession,
      partitionId: Int,
      firstSequenceNumber: Long,
      lastSequenceNumber: Long,
      resume: Boolean): Int = {
    val stream = streams.get(partitionId)
    if (stream == null || lastSequenceNumber < firstSequenceNumber) {
      if (!resume) {
        misaddressedMessages.incrementAndGet()
        reportBounded(StreamingShuffleServerHandler.replayRefusalLogAggregator, replayRefusals,
          log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} discarded a retransmission request " +
            log"for partition ${MDC(PARTITION_ID, partitionId)}: " +
            log"${MDC(REASON, "no such stream or an inverted window")}")
      }
      // A resume of an empty range is the ordinary case for a consumer that is already up to date,
      // so it is answered with nothing rather than reported as a misaddressed frame.
      0
    } else {
      val nowMs = clock.getTimeMillis()
      if (nowMs < session.nextRetransmitAtMs(partitionId)) {
        // Deferred rather than refused: the consumer may ask again once the backoff has elapsed.
        0
      } else {
        // A range reaching at or below what this session has already been sent is a repair,
        // whatever frame asked for it, and only a repair spends the budget.
        val repairsSentBlocks = firstSequenceNumber <= session.sentPosition(partitionId)
        if (!resume || repairsSentBlocks) {
          val attempts = session.chargeRetransmitAttempt(partitionId)
          if (attempts > MAX_RETRANSMIT_ATTEMPTS) {
            escalateRetransmissionBudget(partitionId, attempts)
            return 0
          }
          session.deferRetransmitUntil(partitionId, nowMs + backoffMs(attempts))
        }
        serviceRetransmission(session, partitionId, firstSequenceNumber, lastSequenceNumber)
      }
    }
  }

  /**
   * Re-queues every block in the requested range, having first established that the whole of it is
   * still retained.
   */
  private def serviceRetransmission(
      session: ConsumerSession,
      partitionId: Int,
      firstSequenceNumber: Long,
      lastSequenceNumber: Long): Int = {
    val store = retainedOutput
    val lowestRetained =
      store.map(_.lowestRetainedSequence(partitionId)).getOrElse(MemorySpillManager.UNSET_SEQUENCE)
    val highestRetained =
      store.map(_.lastAcceptedSequence(partitionId)).getOrElse(MemorySpillManager.UNSET_SEQUENCE)
    val withinBounds = store.isDefined &&
      lowestRetained != MemorySpillManager.UNSET_SEQUENCE &&
      firstSequenceNumber >= lowestRetained &&
      lastSequenceNumber <= highestRetained
    val complete = withinBounds && store.exists { retained =>
      var sequenceNumber = firstSequenceNumber
      var held = true
      while (held && sequenceNumber <= lastSequenceNumber) {
        held = retained.retainsBlock(partitionId, sequenceNumber)
        sequenceNumber += 1L
      }
      held
    }
    if (!complete) {
      errorNotifier.setError(StreamingShuffleErrors.invalidSequenceNumber(
        shuffleId, partitionId, math.max(0L, lowestRetained), firstSequenceNumber))
      reportBounded(StreamingShuffleServerHandler.replayRefusalLogAggregator, replayRefusals,
        log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
          log"${MDC(PARTITION_ID, partitionId)} cannot replay blocks " +
          log"${MDC(COUNT, firstSequenceNumber)} through ${MDC(VALUE, lastSequenceNumber)}: the " +
          log"retained window is ${MDC(NUM_BLOCKS, lowestRetained)} through " +
          log"${MDC(MAX_SIZE, highestRetained)}, so the read must be invalidated and the " +
          log"upstream stage recomputed")
      0
    } else {
      // Recorded as owed and then drained, rather than queued here.
      val owed = if (session.deferOwed(partitionId, firstSequenceNumber, lastSequenceNumber)) {
        math.max(0L, lastSequenceNumber - firstSequenceNumber + 1L)
      } else {
        0L
      }
      if (owed > 0L) {
        // A consumer may ask for the same window several times inside its retry budget, and every
        // partition it reads can do so, so the per-replay record is detail.
        if (debugEnabled) {
          logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
            log"${MDC(PARTITION_ID, partitionId)} is replaying ${MDC(NUM_BLOCKS, owed)} " +
            log"retained block(s) to consumer ${MDC(SESSION_ID, session.consumerId)}")
        }
        markSessionReady(session)
        requestDrain()
      }
      math.min(owed, Int.MaxValue.toLong).toInt
    }
  }

  /** Escalates a consumer that keeps asking for the same bytes past the retry budget. */
  private def escalateRetransmissionBudget(partitionId: Int, attempts: Int): Unit = {
    errorNotifier.setError(new SparkException(s"Streaming shuffle $shuffleId partition " +
      s"$partitionId exhausted its retransmission budget of $MAX_RETRANSMIT_ATTEMPTS attempt(s) " +
      s"after $attempts request(s); the consumer is not making progress."))
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
      log"${MDC(PARTITION_ID, partitionId)} exhausted its retransmission budget of " +
      log"${MDC(MAX_ATTEMPTS, MAX_RETRANSMIT_ATTEMPTS)} attempt(s)")
  }

  /** Exponential backoff from one second, doubling per attempt and capped by the attempt budget. */
  private def backoffMs(attempts: Int): Long = {
    val shift = math.min(math.max(0, attempts - 1), MAX_RETRANSMIT_BACKOFF_SHIFT)
    RETRANSMIT_INITIAL_BACKOFF_MS << shift
  }

  // Transport callbacks This handler is installed by `TransportContext`, so every callback below is
  // the transport's rather than Netty's directly.

  /**
   * This handler serves no chunked streams, only one-way messages, so the stream manager it offers
   * is an ordinary empty one.
   */
  private val streamManager = new OneForOneStreamManager()

  /** Producer channels never accept data payloads, so reject them before the decoder allocates. */
  private val rejectInboundPayload = new StreamingShuffleMessage.PayloadReservation {
    override def tryReserve(
        frameShuffleId: Int,
        frameMapId: Long,
        framePartitionId: Int,
        frameSequenceNumber: Long,
        payloadBytes: Int): Boolean = false

    override def release(payloadBytes: Int): Unit = {}
  }

  override def getStreamManager(): StreamManager = streamManager

  /** Enqueues callback work on the executor-wide bounded data-plane stripes. */
  private def submitDataPlane(
      client: TransportClient,
      description: String)(operation: => Unit): Boolean = {
    val accepted = backpressure.executeDataPlane(this, new Runnable {
      override def run(): Unit = guard(operation)
    })
    if (!accepted) {
      val failure = new SparkException(
        s"Streaming shuffle $shuffleId map $mapId could not enqueue $description because the " +
          "executor-wide data-plane worker queue is full.")
      errorNotifier.setError(failure)
      reportBounded(StreamingShuffleServerHandler.dataPlaneRefusalLogAggregator,
        dataPlaneRefusals,
        log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not enqueue " +
          log"${MDC(DESCRIPTION, description)}: " +
          log"${MDC(REASON, "the executor-wide data-plane worker queue is full")}", failure)
      client.close()
    }
    accepted
  }

  /** Consumes one control frame that arrived as a one-way message. */
  override def receive(client: TransportClient, message: ByteBuffer): Unit = {
    if (authenticated(client)) {
      guard {
        decodeAndHandle(client, message)
      }
    } else {
      rejectUnauthenticated(client)
    }
  }

  /** Consumes one control frame that arrived as a request expecting a reply. */
  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    if (authenticated(client)) {
      guard {
        decodeAndHandle(client, message)
      }
      callback.onSuccess(ByteBuffer.allocate(0))
    } else {
      val failure = rejectUnauthenticated(client)
      callback.onFailure(failure)
    }
  }

  /** Whether Spark's transport authentication established a non-empty identity for this channel. */
  private def authenticated(client: TransportClient): Boolean =
    StreamingShuffleServerHandler.authenticatedPrincipal(client).isDefined

  /**
   * Refuses a frame that reached this handler without completing Spark authentication.
   *
   * @return the security failure reported to a request-shaped caller
   */
  private def rejectUnauthenticated(client: TransportClient): SecurityException = {
    val failure = new SecurityException(
      s"Streaming shuffle $shuffleId refuses a frame from an unauthenticated transport.")
    reportBounded(StreamingShuffleServerHandler.unauthenticatedRefusalLogAggregator,
      unauthenticatedRefusals,
      log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} refused a frame from " +
        log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))} because the channel did " +
        log"not complete Spark authentication")
    client.close()
    failure
  }

  /** Notes the arrival of a consumer's channel, without allocating anything for it yet. */
  override def channelActive(client: TransportClient): Unit = {
    guard {
      // One record per accepted connection, and a wide shuffle brings one connection per reduce
      // task, so the arrival is debug detail.
      if (debugEnabled) {
        logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} accepted an egress channel " +
          log"from ${MDC(HOST_PORT, client.getSocketAddress())}")
      }
    }
  }

  /** Records the loss of one consumer's channel, retaining everything it had not acknowledged. */
  override def channelInactive(client: TransportClient): Unit = {
    submitDataPlane(client, "an inactive-channel transition") {
      val session = sessions.get(sessionKeyOf(client))
      if (session != null && forgetSession(session)) {
        // Sampled before the ledgers are released, because releasing them is what zeroes them.
        val owedBytes = session.unacknowledgedBytes
        val queuedBytes = session.pendingBytes
        // The ledgers describe a flow over this channel and end with it; the consumer's cursor in
        // the retained store does not, which is what its next connection resumes against.
        releaseConsumerLedgers(session)
        session.close()
        if (owedBytes == 0L && queuedBytes == 0L) {
          if (debugEnabled) {
            logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} closed its egress " +
              log"channel to consumer ${MDC(SESSION_ID, session.consumerId)} at " +
              log"${MDC(HOST_PORT, client.getSocketAddress())} with nothing owed to it")
          }
        } else {
          reportBounded(
            StreamingShuffleServerHandler.lossyChannelLogAggregator,
            lossyChannelClosures,
            log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} lost its egress channel to " +
              log"consumer ${MDC(SESSION_ID, session.consumerId)} at " +
              log"${MDC(HOST_PORT, client.getSocketAddress())} with " +
              log"${MDC(NUM_BYTES, owedBytes)} unacknowledged and " +
              log"${MDC(MAX_SIZE, queuedBytes)} undelivered byte(s) retained for replay")
        }
      }
      reportPeerLoss()
    }
  }

  /** Latches a channel level failure and closes that consumer's channel. */
  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    submitDataPlane(client, "a channel-failure transition") {
      errorNotifier.setError(cause)
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} egress channel to " +
        log"${MDC(HOST_PORT, client.getSocketAddress())} raised " +
        log"${MDC(ERROR, cause.getMessage())}; closing it and failing the producing task", cause)
      closeSession(client, "the channel itself raised a transport failure")
    }
  }

  // Inbound dispatch

  /**
   * Decodes one framed message, checking the peer's protocol revision before its body is trusted.
   */
  private def decodeAndHandle(client: TransportClient, frame: ByteBuffer): Unit = {
    val version = StreamingShuffleMessage.peekProtocolVersion(frame)
    if (StreamingShuffleMessage.isCompatible(version)) {
      val message = StreamingShuffleMessage.Decoder.fromByteBuffer(frame, rejectInboundPayload)
      if (validateInboundAddress(message)) {
        submitDataPlane(client, "a consumer control frame") {
          handleInbound(client, message)
        }
      }
    } else {
      submitDataPlane(client, "a protocol-version refusal") {
        reportVersionMismatch(client, version)
      }
    }
  }

  /** Validates frame addressing on the event loop before any worker state transition is queued. */
  private def validateInboundAddress(message: StreamingShuffleMessage): Boolean = {
    if (message.shuffleId() != shuffleId || message.mapId() != mapId ||
        !servesPartition(message.partitionId())) {
      misaddressedMessages.incrementAndGet()
      if (misaddressReported.compareAndSet(false, true)) {
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} dropped a message " +
          log"addressed to shuffle ${MDC(VALUE, message.shuffleId())} partition " +
          log"${MDC(PARTITION_ID, message.partitionId())}: " +
          log"${MDC(REASON, "the frame names no stream this handler serves")}. Further " +
          log"occurrences are counted but not logged")
      }
      false
    } else {
      true
    }
  }

  /** Routes one inbound message, discriminating on its concrete type. */
  private def handleInbound(client: TransportClient, message: StreamingShuffleMessage): Unit = {
    message match {
      case ack: AckMessage =>
        handleAck(client, ack)
      case heartbeat: HeartbeatMessage =>
        handleHeartbeat(client, heartbeat)
      case request: RetransmitRequestMessage =>
        handleRetransmitRequest(client, request)
      case unexpected =>
        rejectUnexpected(client, unexpected)
    }
  }

  /** Whether a partition id could belong to this map output at all. */
  private def servesPartition(partitionId: Int): Boolean = {
    partitionId >= 0 && partitionId < numPartitions
  }

  /**
   * Whether this producer generation can never serve output again, as distinct from not having
   * published it yet.
   */
  private def generationRetired: Boolean = {
    closed.get() || withdrawn.get() ||
      blockResolver.registeredGeneration(shuffleId, mapId).exists(_ != taskAttemptId)
  }

  /** Refuses a message a producer can never legitimately receive. */
  private def rejectUnexpected(
      client: TransportClient,
      message: StreamingShuffleMessage): Unit = {
    val actual = message match {
      case _: DataBlockMessage => StreamingShuffleMessageType.DATA_BLOCK.name()
      case _: StreamTerminationMessage => StreamingShuffleMessageType.STREAM_TERMINATION.name()
      case other => other.getClass.getName
    }
    val expected = s"${StreamingShuffleMessageType.ACK.name()}, " +
      s"${StreamingShuffleMessageType.HEARTBEAT.name()} or " +
      s"${StreamingShuffleMessageType.RETRANSMIT_REQUEST.name()}"
    errorNotifier.setError(StreamingShuffleErrors.unexpectedMessageType(expected, actual))
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} received " +
      log"${MDC(VALUE, actual)} on its egress channel, which a producer may never be sent; " +
      log"closing the channel")
    closeSession(client, s"it sent $actual, which a producer may never be sent")
  }

  /** Applies one acknowledgement: advances that consumer's position and releases what it covers. */
  private def handleAck(client: TransportClient, ack: AckMessage): Unit = {
    val partitionId = ack.partitionId()
    val position = ack.consumerPosition()
    val session = sessions.get(sessionKeyOf(client))
    if (session == null || !session.subscribedTo(partitionId)) {
      refusedAcks.incrementAndGet()
      misaddressedMessages.incrementAndGet()
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} refused an acknowledgement " +
        log"of partition ${MDC(PARTITION_ID, partitionId)} through ${MDC(COUNT, position)} from " +
        log"${MDC(HOST_PORT, client.getSocketAddress())}: " +
        log"${MDC(REASON, "that channel is not subscribed to the partition")}")
    } else if (!ack.acknowledgesWithin(session.sentPosition(partitionId))) {
      // The per-session bound, and the one a forged acknowledgement runs into first: this consumer
      // may only confirm blocks that were written to *its* channel.
      refusedAcks.incrementAndGet()
      errorNotifier.setError(StreamingShuffleErrors.invalidSequenceNumber(
        shuffleId, partitionId, session.sentPosition(partitionId), position))
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} refused an acknowledgement through " +
        log"${MDC(COUNT, position)} from consumer ${MDC(SESSION_ID, session.consumerId)}: " +
        log"only ${MDC(MAX_SIZE, session.sentPosition(partitionId))} has been written to that " +
        log"channel; closing it")
      closeSession(client,
        "it acknowledged a position beyond the bytes this channel was written")
    } else if (!session.applyControlSequence(partitionId, ack)) {
      // Inert, not invalid.
      acks.incrementAndGet()
      duplicateAcks.incrementAndGet()
      if (debugEnabled) {
        logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
          log"${MDC(PARTITION_ID, partitionId)} ignored an acknowledgement through " +
          log"${MDC(COUNT, position)} from consumer ${MDC(SESSION_ID, session.consumerId)}: " +
          log"${MDC(REASON, "it does not advance the position already recorded")}")
      }
    } else {
      acks.incrementAndGet()
      val startedAtMs = clock.getTimeMillis()
      val ledgerKey = consumerLedgerKey(partitionId, session.consumerId)
      // The ledger refuses a position beyond what the producer charged, and its refusal is what
      // makes the whole transition atomic: nothing is released, no cursor moves, the channel fails.
      backpressure.tryAcknowledge(ledgerKey, ack) match {
        case None =>
          refusedAcks.incrementAndGet()
          errorNotifier.setError(StreamingShuffleErrors.invalidSequenceNumber(
            shuffleId, partitionId,
            backpressure.highestChargedSequenceNumber(ledgerKey), position))
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
            log"${MDC(PARTITION_ID, partitionId)} refused an acknowledgement through " +
            log"${MDC(COUNT, position)} from consumer ${MDC(SESSION_ID, session.consumerId)}: " +
            log"${MDC(REASON, "no such block has been sent")}; closing the channel")
          closeSession(client, "it acknowledged a block that has never been sent")
        case Some(_) =>
          val advanced = session.advanceAck(partitionId, position)
          val reclaimedBytes = if (advanced) {
            // Progress, established rather than assumed: only now may the stall detector's clock be
            // refreshed, and only now is there anything to release.
            session.stampAckProgress(startedAtMs)
            trackConsumer(session.consumerId, startedAtMs)
            // Progress means the peer is healthy, so the retransmission budget starts afresh.
            session.resetRetransmitBudget(partitionId)
            retainedOutput
              .map(_.acknowledge(session.consumerId, partitionId, position))
              .getOrElse(0L)
          } else {
            duplicateAcks.incrementAndGet()
            0L
          }
          val elapsedMs = clock.getTimeMillis() - startedAtMs
          if (elapsedMs > ACK_RECLAMATION_BUDGET_MS && session.warnReclamationOnce()) {
            logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
              log"${MDC(PARTITION_ID, partitionId)} took ${MDC(DURATION, elapsedMs)} ms to " +
              log"reclaim ${MDC(NUM_BYTES, reclaimedBytes)} byte(s), beyond the " +
              log"${MDC(TIMEOUT, ACK_RECLAMATION_BUDGET_MS)} ms budget. Further breaches on this " +
              log"session are not logged")
          }
          if (debugEnabled) {
            logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
              log"${MDC(PARTITION_ID, partitionId)} acknowledged through " +
              log"${MDC(COUNT, position)} by consumer ${MDC(SESSION_ID, session.consumerId)}, " +
              log"reclaiming ${MDC(NUM_BYTES, reclaimedBytes)} byte(s) in " +
              log"${MDC(DURATION, elapsedMs)} ms")
          }
          val partitionStream = streams.get(partitionId)
          if (partitionStream != null) {
            session.recordSubscriptionCompletion(
              partitionId, partitionStream.highestOffered.get())
          }
          retireCompletedConsumer(session)
          // Reclaimed memory and advanced credit may both have unblocked egress.
          markSessionReady(session)
          requestDrain()
      }
    }
  }

  /** Applies one consumer heartbeat, which is also how a consumer subscribes and how it resumes. */
  private def handleHeartbeat(client: TransportClient, heartbeat: HeartbeatMessage): Unit = {
    val partitionId = heartbeat.partitionId()
    heartbeats.incrementAndGet()
    // No session, no state.
    sessionFor(client)
      .filter(session => adoptDeclaredIdentity(session, heartbeat))
      .filter { session =>
        session.stampHeartbeat(clock.getTimeMillis())
        // The session's own identity is tracked here -- after adoption and before subscription --
        // because it is what the retained store's cursor and this partition's credit ledger are
        // keyed by.
        trackConsumer(session.consumerId, clock.getTimeMillis())
        true
      }
      .foreach { session =>
        if (session.subscribedTo(partitionId)) {
          // Already subscribed on this channel: an ordinary repeat heartbeat, which neither opens a
          // ledger nor claims a subscriber slot.
          ()
        } else if (generationRetired) {
          // This generation can never serve output again, so a subscription to it would open a
          // credit ledger, claim a subscriber slot and seed two cursors that nothing will ever
          // advance.
          reportBounded(
            StreamingShuffleServerHandler.subscriptionRefusalLogAggregator,
            subscriptionRefusals,
            log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} refused consumer " +
              log"${MDC(SESSION_ID, session.consumerId)} a subscription to partition " +
              log"${MDC(PARTITION_ID, partitionId)}: " +
              log"${MDC(REASON, "this producer generation has been retired, so it can never " +
                "serve output again")}")
        } else if (!claimSubscriberSlot(partitionId)) {
          // Refused before a ledger, a queue entry or a payload copy exists.
          reportBounded(
            StreamingShuffleServerHandler.subscriptionRefusalLogAggregator,
            subscriptionRefusals,
            log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} refused consumer " +
              log"${MDC(SESSION_ID, session.consumerId)} a subscription to partition " +
              log"${MDC(PARTITION_ID, partitionId)}: it already serves " +
              log"${MDC(COUNT, subscriberCount(partitionId))} subscriber(s) of that partition, " +
              log"the ceiling of ${MDC(MAX_SIZE, MAX_SUBSCRIBERS_PER_PARTITION)}")
        } else if (!registerRetainedConsumer(session)) {
          // The consumer registry is capped independently of live channels.
          releaseSubscriberSlot(partitionId)
          releaseSession(session, "the retained consumer registry is at capacity")
        } else if (session.subscribe(partitionId)) {
          trackConsumer(session.consumerId, clock.getTimeMillis())
          if (openConsumerLedger(session, partitionId)) {
            resumeFrom(session, partitionId, heartbeat.sequenceNumber())
          } else {
            reportBounded(StreamingShuffleServerHandler.subscriptionRefusalLogAggregator,
              subscriptionRefusals,
              log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} cannot admit consumer " +
                log"${MDC(SESSION_ID, session.consumerId)} to partition " +
                log"${MDC(PARTITION_ID, partitionId)}: " +
                log"${MDC(REASON,
                  "the executor-wide metadata allowance is fully committed")}")
            releaseSession(session, "the executor-wide metadata allowance is fully committed")
          }
        } else {
          // The session refused the subscription itself -- it is closing -- so the slot claimed
          // just above is surplus and goes straight back.
          releaseSubscriberSlot(partitionId)
        }
        reportHeartbeat(session, heartbeat)
        considerTerminationReady(session, partitionId)
        abortIfUnservable(session, partitionId)
        // A drain unconditionally, and this is load bearing rather than tidy.
        markSessionReady(session)
        requestDrain()
        if (debugEnabled) {
          logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
            log"${MDC(PARTITION_ID, partitionId)} saw a heartbeat from consumer " +
            log"${MDC(SESSION_ID, session.consumerId)} at position " +
            log"${MDC(COUNT, heartbeat.sequenceNumber())}")
        }
      }
  }

  /** Registers one stable consumer identity with the retained store before any per-stream state. */
  private def registerRetainedConsumer(session: ConsumerSession): Boolean = {
    if (session.hasRetainedConsumerRegistration) {
      true
    } else {
      retainedOutput match {
        case None =>
          // A live consumer is allowed to subscribe before the producing task has entered `write`
          // and published its store.
          true
        case Some(store) =>
          val admitted = store.registerConsumer(session.consumerId)
          if (admitted) {
            session.markRetainedConsumerRegistered()
          } else {
            reportBounded(StreamingShuffleServerHandler.subscriptionRefusalLogAggregator,
              subscriptionRefusals,
              log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} cannot admit consumer " +
                log"${MDC(SESSION_ID, session.consumerId)}: " +
                log"${MDC(REASON,
                  "the retained output is unavailable or its consumer cap is full")}")
          }
          admitted
      }
    }
  }

  /** Settles every bounded subscription that arrived before this producer published its store. */
  def registerPendingRetainedConsumers(): Unit = {
    sessions.values().asScala.toSeq.foreach { session =>
      if (!registerRetainedConsumer(session)) {
        releaseSession(session,
          "the retained output's consumer cap refused its registration")
      }
    }
  }

  /** Opens the credit ledger of one consumer's flow of one partition. */
  private def openConsumerLedger(session: ConsumerSession, partitionId: Int): Boolean = {
    val creditLimitBytes = retainedOutput
      .map(_.perPartitionBudgetBytes)
      .filter(_ > 0L)
      .getOrElse(MAX_FRAMED_BYTES.toLong)
    backpressure.registerStream(
      consumerLedgerKey(partitionId, session.consumerId), creditLimitBytes)
  }

  /**
   * Queues everything one newly subscribed consumer is owed, from its own position onward.
   *
   * @param session the consumer session that has just subscribed to the partition
   * @param partitionId the reduce partition the consumer subscribed to
   * @param announcedNextPosition the next block position the consumer says it expects
   */
  private def resumeFrom(
      session: ConsumerSession,
      partitionId: Int,
      announcedNextPosition: Long): Unit = {
    retainedOutput.filter { store =>
      val admitted = store.registerConsumer(session.consumerId)
      if (!admitted) {
        // The store no longer serves its output: the generation was withdrawn, or the shuffle was
        // unregistered, between this consumer's frame arriving and this line.
        reportBounded(
          StreamingShuffleServerHandler.subscriptionRefusalLogAggregator,
          subscriptionRefusals,
          log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
            log"${MDC(PARTITION_ID, partitionId)} cannot admit consumer " +
            log"${MDC(SESSION_ID, session.consumerId)}: " +
            log"${MDC(REASON, "the retained output of this generation is no longer served")}")
      }
      admitted
    }.foreach { store =>
      val lowestRetained = store.lowestRetainedSequence(partitionId)
      val highestRetained = store.lastAcceptedSequence(partitionId)
      // A heartbeat states the next expected position, so the confirmed position is one below it,
      // and it may not exceed what this producer has charged for the partition.
      val chargedThrough =
        if (highestRetained == MemorySpillManager.UNSET_SEQUENCE) AckMessage.NOTHING_CONSUMED
        else highestRetained
      val announcedPosition = math.min(
        math.max(AckMessage.NOTHING_CONSUMED, announcedNextPosition - 1L), chargedThrough)
      val recorded = store.consumerPosition(session.consumerId, partitionId)
        .getOrElse(AckMessage.NOTHING_CONSUMED)
      val resumeAfter = math.max(announcedPosition, recorded)
      session.advanceAck(partitionId, resumeAfter)
      // A first heartbeat on a replacement channel is the cursor-migration boundary.
      if (resumeAfter > recorded) {
        store.acknowledge(session.consumerId, partitionId, resumeAfter)
      }
      if (highestRetained == MemorySpillManager.UNSET_SEQUENCE) {
        // Nothing has been produced for this partition yet; the subscription alone is enough, and
        // blocks will be fanned out to this session as they are admitted.
        session.advanceSent(partitionId, resumeAfter)
      } else if (lowestRetained != MemorySpillManager.UNSET_SEQUENCE &&
          lowestRetained > resumeAfter + 1L) {
        errorNotifier.setError(StreamingShuffleErrors.invalidSequenceNumber(
          shuffleId, partitionId, lowestRetained, resumeAfter + 1L))
        reportBounded(
          StreamingShuffleServerHandler.subscriptionRefusalLogAggregator,
          subscriptionRefusals,
          log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
            log"${MDC(PARTITION_ID, partitionId)} cannot resume consumer " +
            log"${MDC(SESSION_ID, session.consumerId)} from ${MDC(COUNT, resumeAfter + 1L)}: the " +
            log"retained window begins at ${MDC(NUM_BLOCKS, lowestRetained)}, so the read must " +
            log"be invalidated and the upstream stage recomputed")
      } else {
        session.advanceSent(partitionId, resumeAfter)
        val queued = replayGate(
          session, partitionId, math.max(0L, resumeAfter + 1L), highestRetained, resume = true)
        if (queued > 0) {
          resumedSessions.incrementAndGet()
          // One record per resumed partition per reconnecting consumer.
          if (debugEnabled) {
            logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
              log"${MDC(PARTITION_ID, partitionId)} resumed consumer " +
              log"${MDC(SESSION_ID, session.consumerId)} with ${MDC(NUM_BLOCKS, queued)} " +
              log"retained block(s) from position ${MDC(COUNT, resumeAfter)}")
          }
        }
      }
    }
  }

  /**
   * Handles an inbound retransmission request by putting it through the protocol's own transition
   * before servicing it.
   */
  private def handleRetransmitRequest(
      client: TransportClient,
      request: RetransmitRequestMessage): Unit = {
    val partitionId = request.partitionId()
    val session = sessions.get(sessionKeyOf(client))
    if (session == null || !session.subscribedTo(partitionId)) {
      misaddressedMessages.incrementAndGet()
      reportBounded(StreamingShuffleServerHandler.replayRefusalLogAggregator, replayRefusals,
        log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} refused a replay request for " +
          log"partition ${MDC(PARTITION_ID, partitionId)} from " +
          log"${MDC(HOST_PORT, client.getSocketAddress())}: " +
          log"${MDC(REASON, "that channel is not subscribed to the partition")}")
      return
    }
    val admitted = backpressure.onRetransmitRequest(
      consumerLedgerKey(partitionId, session.consumerId), request)
    val serviced = if (admitted) {
      replayGate(session, partitionId, request.firstSequenceNumber(),
        request.lastSequenceNumber(), resume = false)
    } else {
      0
    }
    if (!admitted) {
      reportBounded(StreamingShuffleServerHandler.replayRefusalLogAggregator, replayRefusals,
        log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
          log"${MDC(PARTITION_ID, partitionId)} refused a request to replay blocks " +
          log"${MDC(COUNT, request.firstSequenceNumber())} through " +
          log"${MDC(VALUE, request.lastSequenceNumber())}: " +
          log"${MDC(REASON, "the range is no longer retained or the replay budget is spent")}")
    } else if (debugEnabled) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} was asked to replay " +
        log"${MDC(NUM_BLOCKS, request.blockCount())} block(s) and re-queued " +
        log"${MDC(COUNT, serviced)}")
    }
  }

  private def reportVersionMismatch(client: TransportClient, version: Byte): Unit = {
    versionMismatch.set(true)
    // Reported to the shared policy FIRST, and that ordering is what makes the signal act on
    // anything.
    fallbackPolicy.checkProtocolVersion(version)
    errorNotifier.setError(new SparkException(s"Streaming shuffle $shuffleId received protocol " +
      s"version $version from its consumer but this executor speaks " +
      s"${StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION}; the shuffle must fall back to the " +
      "sort based implementation."))
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} detected a protocol version " +
      log"mismatch: peer sent ${MDC(PROTOCOL_VERSION, version)} against " +
      log"${MDC(VERSION_NUM, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)}. " +
      log"${MDC(REASON, "streaming yields to sort based shuffle")}")
    closeSession(client, s"it speaks wire revision $version, which this executor cannot read")
  }

  // Reporting to the backpressure protocol This is the ONLY region of this file that calls
  // BackpressureProtocol.

  /**
   * Asks the protocol to admit one pending block for egress, and reports the outcome.
   *
   * @return true if the block may be written now, false if it must stay queued and be retried
   */
  private def admitForEgress(session: ConsumerSession, pending: PendingBlock): Boolean = {
    val key = consumerLedgerKey(pending.partitionId, session.consumerId)
    if (pending.replay) {
      // A replay is paced but not charged for credit and not counted as production.
      backpressure.tryAdmitReplay(key, pending.framedBytes.toLong, pending.sequenceNumber)
    } else {
      backpressure.tryAdmit(key, pending.framedBytes.toLong, pending.sequenceNumber)
    }
  }

  /** Reports a consumer heartbeat, carrying the frame rather than just its instant. */
  private def reportHeartbeat(session: ConsumerSession, heartbeat: HeartbeatMessage): Unit = {
    backpressure.onHeartbeat(
      consumerLedgerKey(heartbeat.partitionId(), session.consumerId), heartbeat)
  }

  /** Reports that the consumer's channel was lost, without latching a degradation. */
  private def reportPeerLoss(): Unit = {
    backpressure.pollOnce()
  }

  // Observers, read by the task thread The write metrics reporter documents that its methods are
  // called on a single thread, so an event-loop thread must never touch it.

  /** Framed bytes handed to the channel, data and control frames together. */
  def bytesWrittenToChannel: Long = bytesWritten.get()

  /** Data blocks handed to the channel, retransmissions included. */
  def blocksWrittenToChannel: Long = blocksWritten.get()

  /** Nanoseconds spent inside the drain loop, suitable for the reporter's write time counter. */
  def writeTimeNanos: Long = writeNanos.get()

  /** Framed bytes queued for the wire but not yet written. */
  def pendingBytes: Long = sumOverSessions(session => session.pendingBytes)

  /**
   * Framed bytes written but not yet acknowledged by every consumer, and therefore still retained
   * for replay.
   */
  def unacknowledgedBytes: Long = sumOverSessions(session => session.unacknowledgedBytes)

  /** Blocks retained for replay across every partition of this shuffle, per the retained store. */
  def unacknowledgedBlockCount: Int = {
    val store = retainedOutput
    var total = 0
    streams.keySet().asScala.foreach { partitionId =>
      total += store.map(_.retainedBlockCount(partitionId)).getOrElse(0)
    }
    total
  }

  /**
   * The highest block sequence number that *every* subscribed consumer of one partition has
   * acknowledged, or `AckMessage.NOTHING_CONSUMED` when one of them has acknowledged nothing.
   */
  def acknowledgedPosition(partitionId: Int): Long = {
    val subscribed = sessions.values().asScala.filter(_.subscribedTo(partitionId)).toSeq
    if (subscribed.isEmpty) {
      AckMessage.NOTHING_CONSUMED
    } else {
      subscribed.map(_.ackPosition(partitionId)).min
    }
  }

  /** Consumer sessions currently attached to this map output. */
  def sessionCount: Int = sessions.size()

  /** The channels of this producer's live consumer sessions, each channel appearing once. */
  private[streaming] def activeSessionChannels: Seq[Channel] = {
    sessions.values().asScala.iterator
      .filterNot(_.isClosed)
      .map(_.channel)
      .filter(channel => channel != null && channel.isActive())
      .toSeq
      .distinct
  }

  /** Distinct (consumer, partition) subscriptions currently held. */
  def subscriptionCount: Int = {
    var total = 0
    sessions.values().asScala.foreach(session => total += session.subscriptionCount)
    total
  }

  /** Acknowledgements refused because they were unauthorised or named unsent output. */
  def refusedAckCount: Long = refusedAcks.get()

  /** Blocks that could not be framed because the retained store no longer held them. */
  def unservableBlockCount: Long = unservableBlocks.get()

  /** Consumers resumed from a recorded position after reconnecting. */
  def resumedSessionCount: Long = resumedSessions.get()

  /** Connections released because the consumer behind them identified itself on a newer one. */
  def supersededSessionCount: Long = supersededSessions.get()

  /** Heartbeats that tried to give a live session a second identity, and were refused. */
  def renameRefusalCount: Long = renameRefusals.get()

  /** Acknowledgements that were valid but advanced nothing, so they refreshed no progress clock. */
  def duplicateAckCount: Long = duplicateAcks.get()

  /** Sessions released for going silent for longer than the ten-second liveness window. */
  def expiredSessionCount: Long = expiredSessions.get()

  /** Logical consumers unregistered for going silent for longer than the expiry window. */
  def expiredConsumerCount: Long = expiredConsumers.get()

  /**
   * Egress channels that closed while still owing their consumer bytes, either unacknowledged or
   * undelivered.
   */
  def lossyChannelClosureCount: Long = lossyChannelClosures.get()

  /** How many channels have been refused because the session ceiling was already reached. */
  def refusedSessionCount: Long = refusedSessions.get()

  /** Frames refused before decode because the channel did not complete Spark authentication. */
  def unauthenticatedRefusalCount: Long = unauthenticatedRefusals.get()

  /** Channels closed because they attempted to change or omit their logical consumer identity. */
  def identityConflictCount: Long = identityConflicts.get()

  /** Consumer-session slots currently claimed against the concurrency ceiling. */
  def claimedSessionSlots: Int = liveSessionSlots.get()

  /** Consumer sessions this producer has accepted over its lifetime. */
  def acceptedSessionCount: Long = acceptedSessions.get()

  /** Blocks delayed because this executor had no room for another transient framing copy. */
  def framingBudgetRefusalCount: Long = framingBudgetRefusals.get()

  /** Operations refused because the executor-wide bounded data-plane queue was full. */
  def dataPlaneRefusalCount: Long = dataPlaneRefusals.get()

  /** Test and lifecycle seam for awaiting accepted data-plane work. */
  def awaitDataPlaneIdle(timeoutMs: Long): Boolean =
    backpressure.awaitDataPlaneIdle(timeoutMs)

  /**
   * How many subscriptions have been refused, whether by the per-partition subscriber ceiling or by
   * a request this producer could not honour.
   */
  def subscriptionRefusalCount: Long = subscriptionRefusals.get()

  /** How many consumer identities have gone untracked because the ledger ceiling was reached. */
  def untrackedConsumerCount: Long = untrackedConsumers.get()

  /** How many block references a queue ceiling has deferred onto an owed run. */
  def deferredBlockCount: Long = deferredBlocks.get()

  /** Blocks this producer still owes its consumers but has not queued, across every session. */
  def owedBlockCount: Long = sumOverSessions(session => session.owedBlocks)

  /** Logical consumers this handler currently holds a session for. */
  def liveConsumerCount: Int = sessionsByConsumer.size()

  /**
   * The identities those sessions are keyed by, which is how a caller sees whether an identity was
   * adopted from a declaration or is still the per-connection one a session starts with.
   */
  def consumerIdentities: Set[String] = sessionsByConsumer.keySet().asScala.toSet

  /** Logical consumers whose last-seen instant is still tracked, session or no session. */
  def trackedConsumerCount: Int = consumerLastSeenMs.size()

  /** How many times egress was refused by the rate limiter and the block was held instead. */
  def throttleCount: Long = throttles.get()

  /** Acknowledgements consumed from the reduce side. */
  def ackCount: Long = acks.get()

  /** Heartbeats consumed from the reduce side. */
  def heartbeatCount: Long = heartbeats.get()

  /** Blocks re-queued from the retained window in response to retransmission requests. */
  def retransmittedBlockCount: Long = retransmittedBlocks.get()

  /** Frames dropped because they named a different shuffle than the one this handler serves. */
  def misaddressedMessageCount: Long = misaddressedMessages.get()

  /** Whether a peer announced a protocol revision this build cannot speak. */
  def versionMismatchDetected: Boolean = versionMismatch.get()

  /** Whether there is at least one live consumer channel to write to right now. */
  def isChannelReady: Boolean = {
    !closed.get() && sessions.values().asScala.exists { session =>
      !session.isClosed && session.channel.isActive()
    }
  }

  /** Whether this handler still has retained output it is entitled to serve. */
  def servesRetainedOutput: Boolean = retainedOutput.isDefined

  private def sumOverSessions(measure: ConsumerSession => Long): Long = {
    var total = 0L
    sessions.values().asScala.foreach(session => total += measure(session))
    total
  }

  // Teardown

  /**
   * Retires this producer generation from every owner of it on this executor, exactly once.
   *
   * @param reason short description of what is being recovered from, for the operator's log
   * @return whether this call performed the withdrawal, as opposed to finding it already done
   */
  def withdrawGeneration(reason: String): Boolean = {
    if (!withdrawn.compareAndSet(false, true)) {
      return false
    }
    // Routing first.
    val routed = withdrawalStep("stop routing")(routes.withdrawRoute(shuffleId, mapId, this))
    // Then the publication, which unlinks the spill files behind it.
    val published = withdrawalStep("withdraw the retained output")(
      blockResolver.unregisterProducer(shuffleId, mapId, taskAttemptId))
    // Last, this handler's own sessions, queues and consumer ledgers.
    releaseAll()
    // Bounded on an executor-scoped window rather than emitted per generation.
    val entry = log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} withdrew the producer " +
      log"generation of map ${MDC(MAP_ID, mapId)} attempt " +
      log"${MDC(TASK_ATTEMPT_ID, taskAttemptId)} because ${MDC(REASON, reason)}; routing " +
      log"withdrawn: ${MDC(STATUS, routed)}, retained output withdrawn: ${MDC(STATUS, published)}"
    StreamingShuffleServerHandler.withdrawalLogAggregator.record(clock.getTimeMillis()) match {
      case Some(summary) =>
        logInfo(entry + log"; ${MDC(NUM_EVENTS, summary.occurrences)} generation(s) withdrawn on " +
          log"this executor, ${MDC(MAX_SIZE, summary.unreported)} of them not reported " +
          log"individually so that the executor stays inside its log budget")
      case None =>
        // At the level it was emitted at, under the feature's own key.
        if (debugEnabled) {
          logInfo(entry)
        }
    }
    true
  }

  /** Runs one withdrawal step, reporting rather than propagating a failure. */
  private def withdrawalStep(step: String)(body: => Boolean): Boolean = {
    try {
      body
    } catch {
      case NonFatal(cause) =>
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not " +
          log"${MDC(STATUS, step)} for map ${MDC(MAP_ID, mapId)} attempt " +
          log"${MDC(TASK_ATTEMPT_ID, taskAttemptId)} while withdrawing the generation", cause)
        false
    }
  }

  /** Releases every queued reference and every consumer session, exactly once. */
  def releaseAll(): Unit = {
    if (closed.compareAndSet(false, true)) {
      val releasedBytes = pendingBytes
      val queuedBlocks = sumOverSessions(session => session.pendingBlocks)
      sessions.values().asScala.foreach { session =>
        releaseConsumerLedgers(session)
        session.close()
      }
      sessions.clear()
      sessionsByConsumer.clear()
      readySessions.clear()
      // The two accounted ceilings are cleared with the registries they account for.
      liveSessionSlots.set(0)
      subscribersByPartition.clear()
      consumerLastSeenMs.clear()
      streams.clear()
      if (releasedBytes > 0L || debugEnabled) {
        logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} released " +
          log"${MDC(NUM_BLOCKS, queuedBlocks)} queued block reference(s) totalling " +
          log"${MDC(NUM_BYTES, releasedBytes)} framed byte(s) after " +
          log"${MDC(COUNT, blocksWritten.get())} block(s) written")
      }
    }
  }

  // Internal helpers

  /** Per partition state, created on first use without ever taking a lock. */
  private def streamFor(partitionId: Int): PartitionStream = {
    require(partitionId >= 0,
      s"A streaming shuffle partition id must be non-negative but was $partitionId.")
    val existing = streams.get(partitionId)
    if (existing != null) {
      existing
    } else {
      val created = new PartitionStream(partitionId)
      val raced = streams.putIfAbsent(partitionId, created)
      if (raced != null) raced else created
    }
  }

  /** The session key of one consumer channel. */
  private def sessionKeyOf(client: TransportClient): String = {
    client.getChannel().id().asLongText()
  }

  /** Composes the identity a session holds once its consumer has declared one. */
  private def stableIdentityOf(principal: String, token: Long): String =
    s"$principal$IDENTITY_TOKEN_SEPARATOR$token"

  /**
   * Adopts the identity a consumer declares on its heartbeat, superseding its previous connection.
   *
   * @param session the session the frame arrived on
   * @param heartbeat the frame, which declares a token or declares none
   * @return true when the frame may go on to be applied, false when the session has been closed
   *     and nothing further may be done with it
   */
  private def adoptDeclaredIdentity(
      session: ConsumerSession,
      heartbeat: HeartbeatMessage): Boolean = {
    val token = heartbeat.consumerToken()
    if (token == HeartbeatMessage.NO_CONSUMER_TOKEN || session.declaredToken == token) {
      // Nothing declared -- a producer's own heartbeat, or a peer of an older protocol revision --
      // or the same declaration this session already adopted, which every heartbeat after the first
      // repeats.
      true
    } else if (session.declaredToken != HeartbeatMessage.NO_CONSUMER_TOKEN) {
      refuseDeclaration(session,
        log"it declared a second identity, and the credit ledgers and replay cursor of a live " +
          log"session cannot be re-keyed")
      false
    } else {
      val stableId = stableIdentityOf(session.principal, token)
      val nowMs = clock.getTimeMillis()
      // The incumbent's fate and this session's name are settled in one atomic map operation,
      // because deciding and installing separately would let a second connection appear between the
      // two -- and the decision is precisely about which connections exist.
      val displaced = new AtomicReference[ConsumerSession](null)
      val adoptedFrom = new AtomicReference[String](null)
      val refused = new AtomicBoolean(false)
      sessionsByConsumer.compute(stableId, (_, existing) => {
        if (existing != null && (existing ne session) && !isDisplaceable(existing, nowMs)) {
          refused.set(true)
          existing
        } else {
          session.adoptIdentity(token, stableId) match {
            case Some(previousId) =>
              adoptedFrom.set(previousId)
              if (existing != null && (existing ne session)) {
                displaced.set(existing)
              }
              session
            case None =>
              // The session is closing, or another frame on this same channel adopted first.
              existing
          }
        }
      })
      if (refused.get()) {
        // A live, punctual connection already holds this identity.
        refuseDeclaration(session,
          log"it declared the identity of a connection that is still answering, and a consumer " +
            log"that is being served may not be dispossessed by a frame")
        false
      } else {
        Option(adoptedFrom.get()).foreach { previousId =>
          // Conditionally, so a teardown that has already unindexed this session cannot be undone.
          sessionsByConsumer.remove(previousId, session)
        }
        Option(displaced.get()).foreach { superseded =>
          supersededSessions.incrementAndGet()
          // The ledgers released here are the displaced session's own: adoption precedes
          // subscription, so this session holds none yet.
          releaseSession(superseded,
            "its consumer reconnected on a new channel, which superseded this one")
          if (debugEnabled) {
            logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} superseded the channel " +
              log"of consumer ${MDC(SESSION_ID, stableId)}: it reconnected, so the retained " +
              log"window and credit of the lost connection are released to the new one")
          }
        }
        true
      }
    }
  }

  /** Refuses a declaration and closes the channel that made it. */
  private def refuseDeclaration(
      session: ConsumerSession,
      reason: MessageWithContext): Unit = {
    renameRefusals.incrementAndGet()
    misaddressedMessages.incrementAndGet()
    reportBounded(
      StreamingShuffleServerHandler.subscriptionRefusalLogAggregator,
      subscriptionRefusals,
      log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} closed the channel of consumer " +
        log"${MDC(SESSION_ID, session.consumerId)}: " + reason)
    if (forgetSession(session)) {
      releaseConsumerLedgers(session)
      session.close()
    }
    routes.closeFaultedChannel(session.channel, this,
      "a consumer declaration this producer refused")
  }

  /** Whether a session holding an identity may be superseded by a connection declaring it. */
  private def isDisplaceable(session: ConsumerSession, nowMs: Long): Boolean = {
    session.isClosed || !session.channel.isActive() ||
      nowMs - session.lastInboundMs >= CONSUMER_LIVENESS_TIMEOUT_MS
  }

  /**
   * The session for one consumer channel, created on first use.
   *
   * @return the session, or `None` when the ceiling was reached and this producer stood aside
   */
  private def sessionFor(client: TransportClient): Option[ConsumerSession] = {
    val key = sessionKeyOf(client)
    val existing = sessions.get(key)
    if (existing != null) {
      Some(existing)
    } else {
      // The client id is the peer's authenticated application identity when the application has
      // authentication enabled, and a value the peer supplied in its handshake when it does not.
      val principal = Option(client.getClientId())
        .map(sanitizedIdentity)
        .filter(_.nonEmpty)
        .getOrElse(ANONYMOUS_PRINCIPAL)
      val provisional = s"$principal@${client.getSocketAddress()}"
      var capacityRefused = false
      // Creation, the identity index and the slot claim settle as one transition under the registry
      // lock.
      val selected = sessionRegistryLock.synchronized {
        val raced = sessions.get(key)
        if (raced != null) {
          raced
        } else if (!claimSessionSlot()) {
          capacityRefused = true
          null
        } else {
          val created = new ConsumerSession(
            key, principal, provisional, client.getChannel(), clock.getTimeMillis(), backpressure)
          sessions.put(key, created)
          // Indexed by identity as well as by channel, and here rather than only on the frame that
          // declares one, because the index is what [[liveConsumerCount]] reports and what the
          // stale-consumer sweep consults before it retires a consumer's retained output: a session
          // missing from it would be invisible to both -- the sweep would treat a consumer it is
          // actively serving as gone -- and a channel can be announced well before its first frame
          // arrives.
          sessionsByConsumer.put(created.consumerId, created)
          // Counted cumulatively as well as held live, because "how many consumers has this
          // producer ever served" and "how many is it serving now" answer different questions and
          // the second cannot answer the first: every session is gone by the time a shuffle is
          // over, so a live count read afterwards is zero whether the producer served a thousand
          // consumers or none.
          acceptedSessions.incrementAndGet()
          created
        }
      }
      if (capacityRefused) {
        refusedSessions.incrementAndGet()
        if (sessionCapacityReported.compareAndSet(false, true)) {
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} refused an egress " +
            log"channel from ${MDC(HOST_PORT, client.getSocketAddress())}: it already serves " +
            log"${MDC(COUNT, liveSessionSlots.get())} consumer session(s), the ceiling of " +
            log"${MDC(MAX_SIZE, MAX_CONCURRENT_SESSIONS)}. Further refusals are counted but not " +
            log"logged")
        }
        // Producer-local, so the socket is left alone.
        routes.releaseChannelParticipation(client.getChannel(), this, closeWhenLast = true)
        None
      } else {
        Option(selected)
      }
    }
  }

  /**
   * Claims one of this map output's consumer-session slots, or refuses.
   *
   * @return true when a slot was claimed and the caller owns its release
   */
  private def claimSessionSlot(): Boolean = {
    var current = liveSessionSlots.get()
    var settled = false
    var granted = false
    while (!settled) {
      if (current >= MAX_CONCURRENT_SESSIONS) {
        settled = true
      } else if (liveSessionSlots.compareAndSet(current, current + 1)) {
        granted = true
        settled = true
      } else {
        current = liveSessionSlots.get()
      }
    }
    granted
  }

  /** Returns one consumer-session slot. */
  private def releaseSessionSlot(): Unit = {
    liveSessionSlots.updateAndGet(current => math.max(0, current - 1))
  }

  /**
   * Removes one session from both registries and returns everything it held.
   *
   * @param session the session to forget
   * @return true when this call was the one that removed it
   */
  private def forgetSession(session: ConsumerSession): Boolean = {
    sessionRegistryLock.synchronized {
      forgetSessionLocked(session, releaseSlot = true)
    }
  }

  /** Removes one session while the caller holds [[sessionRegistryLock]]. */
  private def forgetSessionLocked(
      session: ConsumerSession,
      releaseSlot: Boolean): Boolean = {
    val removed = sessions.remove(session.sessionKey, session)
    if (removed) {
      if (releaseSlot) {
        releaseSessionSlot()
      }
      session.subscribedPartitions.foreach { partitionId =>
        if (session.terminationConfirmed(partitionId)) {
          val stream = streams.get(partitionId)
          if (stream != null) {
            stream.deliveredTerminations.updateAndGet(current => math.max(0, current - 1))
          }
        }
        releaseSubscriberSlot(partitionId)
      }
    }
    sessionsByConsumer.remove(session.consumerId, session)
    removed
  }

  /**
   * Claims one subscriber slot on one partition, or refuses.
   *
   * @param partitionId the partition being subscribed to
   * @return true when a subscriber slot was claimed
   */
  private def claimSubscriberSlot(partitionId: Int): Boolean = {
    val counter = subscribersByPartition.computeIfAbsent(
      Integer.valueOf(partitionId), _ => new AtomicInteger(0))
    var current = counter.get()
    var settled = false
    var granted = false
    while (!settled) {
      if (current >= MAX_SUBSCRIBERS_PER_PARTITION) {
        settled = true
      } else if (counter.compareAndSet(current, current + 1)) {
        granted = true
        settled = true
      } else {
        current = counter.get()
      }
    }
    granted
  }

  /** Returns one subscriber slot on one partition. */
  private def releaseSubscriberSlot(partitionId: Int): Unit = {
    val counter = subscribersByPartition.get(Integer.valueOf(partitionId))
    if (counter != null) {
      counter.updateAndGet(current => math.max(0, current - 1))
    }
  }

  /** How many sessions are currently subscribed to one partition. */
  def subscriberCount(partitionId: Int): Int = {
    val counter = subscribersByPartition.get(Integer.valueOf(partitionId))
    if (counter == null) 0 else counter.get()
  }

  /** Records that one logical consumer was heard from, if the liveness ledger has room for it. */
  private def trackConsumer(consumerId: String, nowMs: Long): Unit = {
    if (consumerLastSeenMs.containsKey(consumerId) ||
        consumerLastSeenMs.size() < MAX_TRACKED_CONSUMERS) {
      consumerLastSeenMs.put(consumerId, nowMs)
    } else {
      untrackedConsumers.incrementAndGet()
      if (consumerCapacityReported.compareAndSet(false, true)) {
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} is not tracking the " +
          log"liveness of consumer ${MDC(SESSION_ID, consumerId)}: it already tracks " +
          log"${MDC(COUNT, consumerLastSeenMs.size())} identities, the ceiling of " +
          log"${MDC(MAX_SIZE, MAX_TRACKED_CONSUMERS)}. Further omissions are counted but not " +
          log"logged")
      }
    }
  }

  /**
   * Replaces every character that could break a log record with a single visible substitute.
   *
   * @param identity the identity to render safe, which may be empty
   * @return the identity with every control character, delete and Unicode line or paragraph
   *     separator replaced
   */
  private def sanitizedIdentity(identity: String): String = {
    var index = 0
    var sanitized: java.lang.StringBuilder = null
    while (index < identity.length) {
      val candidate = identity.charAt(index)
      // Code points rather than character literals, and deliberately: a `'\uXXXX'` literal for a
      // non-ASCII character is itself a non-ASCII character in this source file as far as the style
      // gate is concerned, and the two values named here -- U+2028 LINE SEPARATOR and U+2029
      // PARAGRAPH SEPARATOR -- are line breaks to many readers and log pipelines while being
      // neither control characters nor ASCII.
      val forbidden = candidate <= 0x1f || candidate == 0x7f ||
        (candidate >= 0x80 && candidate <= 0x9f) ||
        candidate == UNICODE_LINE_SEPARATOR || candidate == UNICODE_PARAGRAPH_SEPARATOR
      if (forbidden) {
        // Allocated only once something has to change, so the common path -- an identity that is
        // already safe -- copies nothing at all and returns the string it was given.
        if (sanitized == null) {
          sanitized = new java.lang.StringBuilder(identity.length).append(identity, 0, index)
        }
        sanitized.append('?')
      } else if (sanitized != null) {
        sanitized.append(candidate)
      }
      index += 1
    }
    if (sanitized == null) identity else sanitized.toString
  }

  /**
   * Closes one consumer's session for a fault that impugns the '''channel''', and closes the
   * channel.
   *
   * @param client the channel the fault was observed on
   * @param reason operator-facing description of the fault
   */
  private def closeSession(client: TransportClient, reason: String): Unit = {
    val session = sessions.get(sessionKeyOf(client))
    if (session != null && forgetSession(session)) {
      releaseConsumerLedgers(session)
      session.close()
    }
    routes.closeFaultedChannel(client.getChannel(), this, reason)
  }

  /**
   * Releases one consumer session of THIS producer, leaving the physical channel to its owner.
   *
   * @param session the session to release
   * @param reason operator-facing description of why it is being released, for the diagnostic
   */
  private def releaseSession(session: ConsumerSession, reason: String): Unit = {
    forgetSession(session)
    releaseConsumerLedgers(session)
    session.close()
    try {
      val closed = routes.releaseChannelParticipation(
        session.channel, this, closeWhenLast = true)
      if (debugEnabled) {
        logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} released its egress session " +
          log"for consumer ${MDC(SESSION_ID, session.consumerId)} because " +
          log"${MDC(REASON, reason)}; the physical channel was " +
          log"${MDC(STATUS, if (closed) "closed as its last producer left" else "left to its " +
            "other producers")}")
      }
    } catch {
      case NonFatal(e) =>
        if (debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not release its " +
            log"participation in the channel of consumer " +
            log"${MDC(SESSION_ID, session.consumerId)}: ${MDC(REASON, e.getMessage())}")
        }
    }
  }

  /** Closes the per-consumer credit ledgers one departing session owns. */
  private def releaseConsumerLedgers(session: ConsumerSession): Unit = {
    val consumerId = session.consumerId
    session.subscribedPartitions.foreach { partitionId =>
      backpressure.unregisterStream(consumerLedgerKey(partitionId, consumerId))
    }
  }

  /** Runs a Netty callback body so that no failure escapes into the event loop. */
  private def guard(operation: => Any): Unit = {
    try {
      operation
    } catch {
      case NonFatal(e) =>
        errorNotifier.setError(e)
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} contained a failure " +
          log"raised on an I/O thread; the task thread will re-throw it", e)
    }
  }
}

/**
 * Wire and pacing constants of the producer side handler, plus the transport configuration factory
 * that gives the streaming shuffle its own tuning namespace.
 */
private[spark] object StreamingShuffleServerHandler {

  /** Transport module name of the streaming shuffle. */
  val TRANSPORT_MODULE_NAME: String = "shuffle-streaming"

  /**
   * Executor-scoped window bounding the logical-consumer expiry report, which was this subsystem's
   * loudest record by a wide margin.
   */
  private[streaming] val consumerExpiryLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /** Executor-scoped aggregation of the producer-generation withdrawal record. */
  private[streaming] val withdrawalLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /**
   * The executor-scoped windows bounding every other recurring, default-level record a producer
   * makes, one per condition.
   */
  private[streaming] val egressFailureLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val replayRefusalLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val subscriptionRefusalLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val lossyChannelLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val framingBudgetLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val throttleLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val orderingLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /**
   * Executor-scoped window bounding the refusal of a frame that arrived on an unauthenticated
   * transport.
   */
  private[streaming] val unauthenticatedRefusalLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /**
   * Executor-scoped window bounding the record made when the shared data-plane worker queue is
   * full, so the drain or the reply that could not be enqueued is reported without one saturated
   * executor emitting a line per refused submission.
   */
  private[streaming] val dataPlaneRefusalLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /** Executor-scoped window bounding the egress-session expiry record. */
  private[streaming] val sessionExpiryLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private val logAggregators: Seq[MemorySpillManager.ExecutorLogAggregator] =
    Seq(consumerExpiryLogAggregator, withdrawalLogAggregator, egressFailureLogAggregator,
      replayRefusalLogAggregator, subscriptionRefusalLogAggregator, lossyChannelLogAggregator,
      framingBudgetLogAggregator, throttleLogAggregator, orderingLogAggregator,
      unauthenticatedRefusalLogAggregator, dataPlaneRefusalLogAggregator,
      sessionExpiryLogAggregator)

  /** Returns the executor-scoped log aggregation above to its initial state. */
  private[streaming] def resetLogAggregationForTesting(): Unit = {
    logAggregators.foreach(aggregator => aggregator.reset())
  }

  /** The key that enables operating system keep-alive for the streaming module only. */
  val TCP_KEEP_ALIVE_KEY: String = s"spark.$TRANSPORT_MODULE_NAME.io.enableTcpKeepAlive"

  /** The key that bounds how long a streaming channel may be idle, for this module only. */
  val CONNECTION_TIMEOUT_KEY: String = s"spark.$TRANSPORT_MODULE_NAME.io.connectionTimeout"

  /** The key that bounds how long opening a streaming channel may take, for this module only. */
  val CONNECTION_CREATION_TIMEOUT_KEY: String =
    s"spark.$TRANSPORT_MODULE_NAME.io.connectionCreationTimeout"

  /** The five-second connection bound of this subsystem, expressed in whole seconds. */
  val CONNECTION_TIMEOUT_SECONDS: Long = math.max(1L, BackpressureProtocol.ACK_TIMEOUT_MS / 1000L)

  /** Largest payload a single block may carry, being the protocol's own cap of two mebibytes. */
  val MAX_PAYLOAD_BYTES: Int = DataBlockMessage.MAX_BLOCK_SIZE_BYTES

  /** Largest number of bytes a single block occupies on the wire, framing prefix included. */
  val MAX_FRAMED_BYTES: Int = DataBlockMessage.MAX_ENCODED_FRAME_BYTES

  /** Cadence at which a producer should raise a heartbeat, in milliseconds. */
  val PRODUCER_HEARTBEAT_INTERVAL_MS: Long = BackpressureProtocol.HEARTBEAT_INTERVAL_MS

  /** How long a consumer may go without acknowledging before it is treated as stalled. */
  val CONSUMER_LIVENESS_TIMEOUT_MS: Long = 10000L

  /** The budget within which an acknowledgement must have released the memory it covers. */
  val ACK_RECLAMATION_BUDGET_MS: Long = 100L

  /** First retransmission backoff, doubled on each subsequent attempt. */
  val RETRANSMIT_INITIAL_BACKOFF_MS: Long = 1000L

  /** Retransmission attempts allowed for one partition before the condition is escalated. */
  val MAX_RETRANSMIT_ATTEMPTS: Int = 5

  /** Largest doubling the backoff applies, which is one less than the attempt budget. */
  val MAX_RETRANSMIT_BACKOFF_SHIFT: Int = MAX_RETRANSMIT_ATTEMPTS - 1

  /** How long a consumer may be silent before it is unregistered outright, in milliseconds. */
  val CONSUMER_EXPIRY_TIMEOUT_MS: Long =
    CONSUMER_LIVENESS_TIMEOUT_MS +
      RETRANSMIT_INITIAL_BACKOFF_MS * ((1L << MAX_RETRANSMIT_ATTEMPTS) - 1L)

  val INITIAL_EGRESS_QUEUE_CAPACITY: Int = 16

  val INITIAL_READY_SESSION_CAPACITY: Int = 16

  /** Bound for a task-thread flush waiting on accepted data-plane work. */
  val DATA_PLANE_AWAIT_TIMEOUT_MS: Long = 10000L

  /** Short completion wait used while a producer is awaiting a fully drained egress window. */
  val DATA_PLANE_WAIT_STEP_MS: Long = 10L

  /** Most consumer sessions one map output's egress may hold at once. */
  val MAX_CONCURRENT_SESSIONS: Int = 4096

  /** Most logical consumer identities the liveness ledger tracks at once. */
  val MAX_TRACKED_CONSUMERS: Int = 2 * MAX_CONCURRENT_SESSIONS

  /** Most sessions that may be subscribed to one reduce partition of one map output at a time. */
  val MAX_SUBSCRIBERS_PER_PARTITION: Int = 8

  /** The principal of a peer whose transport handshake carries no application identity. */
  val ANONYMOUS_PRINCIPAL: String = "anonymous"

  /** Separates a principal from the token it scopes in an adopted consumer identity. */
  val IDENTITY_TOKEN_SEPARATOR: String = "#"

  /** U+2028 LINE SEPARATOR, which ends a line for many readers and log pipelines. */
  val UNICODE_LINE_SEPARATOR: Int = 0x2028

  /** U+2029 PARAGRAPH SEPARATOR, a line break to the same readers, named for the same reason. */
  val UNICODE_PARAGRAPH_SEPARATOR: Int = 0x2029

  /** Most block references one consumer's egress queue may hold at once. */
  val MAX_QUEUED_BLOCKS_PER_SESSION: Int = 256

  /** Most framed bytes one consumer's egress queue may reference at once. */
  val MAX_QUEUED_BYTES_PER_SESSION: Long = 32L * 1024L * 1024L

  val PENDING_BLOCK_METADATA_BYTES: Long = 96L

  val SENT_BLOCK_METADATA_BYTES: Long = 64L

  val MAX_SENT_BLOCKS_PER_SUBSCRIPTION: Int = 4096

  /** Most owed blocks one top-up may queue for one partition of one consumer. */
  val REPLAY_PAGE_BLOCKS: Int = 64

  /** Longest a refill wake-up may be scheduled for, whatever the bucket's own arithmetic says. */
  val MAX_REFILL_WAIT_MS: Long = 1000L

  /** How long a drain waits before retrying a block the executor-wide framing ceiling refused. */
  val FRAMING_BUDGET_RETRY_WAIT_MS: Long = 20L

  /**
   * Attributes of the task attempt that produced a block, which decide flush order.
   *
   * @param stageId the stage the producing attempt belongs to
   * @param stageAttemptNumber the stage attempt the producing attempt belongs to
   * @param taskAttemptId the globally unique identifier of the producing attempt
   * @param attemptNumber how many times this task has been attempted, counted from zero
   */
  final case class EgressPriority(
      stageId: Int,
      stageAttemptNumber: Int,
      taskAttemptId: Long,
      attemptNumber: Int) {

    /** Whether this is a retry or speculative copy rather than the original attempt. */
    def speculative: Boolean = attemptNumber > 0
  }

  /** The ordering used before any task attempt has been registered. */
  val DEFAULT_PRIORITY: EgressPriority = EgressPriority(0, 0, 0L, 0)

  /**
   * Builds the transport configuration for the streaming shuffle module.
   *
   * @param conf the executor's configuration, left untouched
   * @param numUsableCores cores this JVM may use for transport threads, or zero for the default
   * @param security the security manager whose RPC SSL options protect the channel, defaulting
   *     to this executor's own
   * @return a configuration whose keys live under `spark.shuffle-streaming.io.*`
   */
  def streamingTransportConf(
      conf: SparkConf,
      numUsableCores: Int = 0,
      security: Option[SecurityManager] = currentSecurityManager): TransportConf = {
    val streamingConf = conf.clone
    streamingConf.set(TCP_KEEP_ALIVE_KEY, "true")
    if (!streamingConf.contains(CONNECTION_TIMEOUT_KEY)) {
      streamingConf.set(CONNECTION_TIMEOUT_KEY, s"${CONNECTION_TIMEOUT_SECONDS}s")
    }
    if (!streamingConf.contains(CONNECTION_CREATION_TIMEOUT_KEY)) {
      streamingConf.set(CONNECTION_CREATION_TIMEOUT_KEY, s"${CONNECTION_TIMEOUT_SECONDS}s")
    }
    SparkTransportConf.fromSparkConf(streamingConf, TRANSPORT_MODULE_NAME, numUsableCores,
      sslOptions = security.map(_.getRpcSSLOptions()))
  }

  /**
   * The client-side bootstraps a streaming consumer channel must be created with.
   *
   * @param conf the executor's configuration, read for the application id the handshake names
   * @param transportConf the streaming module's transport configuration
   * @param security the security manager holding the application secret, defaulting to this
   *     executor's own
   * @return the bootstraps to hand to `TransportContext.createClientFactory`
   */
  def streamingClientBootstraps(
      conf: SparkConf,
      transportConf: TransportConf,
      security: Option[SecurityManager] = currentSecurityManager)
    : java.util.List[TransportClientBootstrap] = {
    val manager = requireAuthenticatedSecurityManager(security, "consumer channel")
    val bootstraps = new java.util.ArrayList[TransportClientBootstrap]()
    bootstraps.add(new AuthClientBootstrap(transportConf, conf.getAppId, manager))
    bootstraps
  }

  /**
   * The server-side bootstraps a streaming producer server must be created with.
   *
   * @param transportConf the streaming module's transport configuration
   * @param security the security manager holding the application secret, defaulting to this
   *     executor's own
   * @return the bootstraps to hand to `TransportContext.createServer`
   */
  def streamingServerBootstraps(
      transportConf: TransportConf,
      security: Option[SecurityManager] = currentSecurityManager)
    : java.util.List[TransportServerBootstrap] = {
    val manager = requireAuthenticatedSecurityManager(security, "producer listener")
    val bootstraps = new java.util.ArrayList[TransportServerBootstrap]()
    bootstraps.add(new AuthServerBootstrap(transportConf, manager))
    bootstraps
  }

  /**
   * Returns the identity established by Spark's authentication handshake.
   *
   * @param client channel whose authenticated identity is required
   * @return the authenticated principal, or `None` when the handshake did not complete
   */
  private[streaming] def authenticatedPrincipal(client: TransportClient): Option[String] = {
    Option(client).flatMap(current => Option(current.getClientId())).filter(_.nonEmpty)
  }

  /** Requires the security material from which one side of a streaming channel is built. */
  private def requireAuthenticatedSecurityManager(
      security: Option[SecurityManager],
      subject: String): SecurityManager = {
    security.filter(_.isAuthenticationEnabled()).getOrElse {
      throw new SparkException(
        s"Streaming shuffle refuses to create a $subject while ${NETWORK_AUTH_ENABLED.key} is " +
          "false or no SecurityManager is available. Enable Spark authentication or use the " +
          "sort-based shuffle path.")
    }
  }

  /** This executor's security manager, when there is a live environment to read it from. */
  private def currentSecurityManager: Option[SecurityManager] =
    Option(SparkEnv.get).map(_.securityManager)

  /**
   * One block waiting for the wire, identified rather than carried.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param sequenceNumber the block's position in its partition's sequence
   * @param framedBytes bytes the block occupies on the wire, framing prefix included
   * @param priority attributes of the attempt that produced it
   * @param ticket monotonic enqueue order, which makes the ordering total and stable
   * @param replay whether this entry is a retransmission rather than an original send
   */
  private final case class PendingBlock(
      partitionId: Int,
      sequenceNumber: Long,
      framedBytes: Int,
      priority: EgressPriority,
      ticket: Long,
      replay: Boolean = false)

  /**
   * Flush order: original attempts before retries, lower attempt numbers first, and enqueue order
   * between blocks of equal urgency so that a partition's blocks never leave out of sequence.
   *
   * The first key is monotone in the second, since `speculative` is defined as a non-zero attempt
   * number, so the order this comparator produces is the same with or without it. It is kept
   * because it states the specified rule -- an original attempt outranks a re-attempt -- in the
   * vocabulary the requirement uses, rather than leaving a reader to derive that intent from an
   * integer comparison.
   */
  private val EgressOrdering: Comparator[PendingBlock] = new Comparator[PendingBlock] {
    override def compare(left: PendingBlock, right: PendingBlock): Int = {
      val bySpeculation =
        java.lang.Boolean.compare(left.priority.speculative, right.priority.speculative)
      if (bySpeculation != 0) {
        bySpeculation
      } else {
        val byAttempt =
          java.lang.Integer.compare(left.priority.attemptNumber, right.priority.attemptNumber)
        if (byAttempt != 0) {
          byAttempt
        } else {
          java.lang.Long.compare(left.ticket, right.ticket)
        }
      }
    }
  }

  /**
   * Production state of one reduce partition, shared by every consumer of it.
   *
   * @param partitionId the reduce partition this state belongs to
   */
  private final class PartitionStream(val partitionId: Int) {

    /**
     * Next sequence number to assign, counted from zero and increasing by one per accepted block.
     */
    val nextSequenceNumber: AtomicLong = new AtomicLong(0L)

    /**
     * Highest sequence number offered to any consumer, or `AckMessage.NOTHING_CONSUMED` when the
     * partition has produced nothing.
     */
    val highestOffered: AtomicLong = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    val terminationRequested: AtomicBoolean = new AtomicBoolean(false)

    val totalBlocksAtTermination: AtomicLong = new AtomicLong(0L)

    val deliveredTerminations: AtomicInteger = new AtomicInteger(0)
  }

  /**
   * Everything one consumer's channel is owed, and everything it has confirmed.
   *
   * @param sessionKey the channel's own identifier, this session's identity in the registry
   * @param principal the transport's authenticated client id where there is one and a sanitized
   *     stand-in otherwise; every identity this session can ever hold is scoped to it, which is
   *     what stops one peer adopting another peer's declared token
   * @param provisionalId the identity this session's cursors are keyed by until the consumer
   *     declares one, formed from the principal and the socket
   * @param channel the consumer's channel, used for writes and for writability
   * @param createdAtMs the instant the session was opened, which seeds its activity stamps
   * @param backpressure owner of the executor-wide aggregate metadata allowance
   */
  private final class ConsumerSession(
      val sessionKey: String,
      val principal: String,
      provisionalId: String,
      val channel: Channel,
      createdAtMs: Long,
      backpressure: BackpressureProtocol) {

    /** The identity this session's cursors and ledgers are keyed by. */
    @volatile private var identity: String = provisionalId

    /** The token behind [[identity]], or [[HeartbeatMessage.NO_CONSUMER_TOKEN]] before adoption. */
    @volatile private var declaredTokenValue: Long = HeartbeatMessage.NO_CONSUMER_TOKEN

    /** The identity this session's cursors and ledgers are keyed by. */
    def consumerId: String = identity

    /** The token this session has adopted, or the reserved value when it has adopted none. */
    def declaredToken: Long = declaredTokenValue

    /**
     * Adopts the identity a consumer declares, once and only once.
     *
     * @param token the declared token, which must be a real one
     * @param stableId the identity to hold from now on, scoped to this session's principal
     * @return the identity this session held before adopting, or `None` when it did not adopt
     */
    def adoptIdentity(token: Long, stableId: String): Option[String] = synchronized {
      if (isClosed || declaredTokenValue != HeartbeatMessage.NO_CONSUMER_TOKEN) {
        None
      } else {
        val previous = identity
        declaredTokenValue = token
        identity = stableId
        Some(previous)
      }
    }

    /** Blocks queued for this consumer, ordered by the egress priority they were queued under. */
    val queue: PriorityBlockingQueue[PendingBlock] =
      new PriorityBlockingQueue[PendingBlock](INITIAL_EGRESS_QUEUE_CAPACITY, EgressOrdering)

    /** Guard that keeps exactly one thread writing to this channel at a time. */
    val draining: AtomicBoolean = new AtomicBoolean(false)

    val readyQueued: AtomicBoolean = new AtomicBoolean(false)

    /** Fairness snapshot captured when this session enters the ready queue. */
    @volatile var readyBytesSnapshot: Long = 0L

    /** Stable tie-break ticket captured with [[readyBytesSnapshot]]. */
    @volatile var readyTicketValue: Long = 0L

    /** Set when work arrives while a drain is in flight, so the drain runs one more pass. */
    val drainWakeup: AtomicBoolean = new AtomicBoolean(false)

    /** One outstanding refill wake-up per session is enough; this is that one-shot guard. */
    val refillScheduled: AtomicBoolean = new AtomicBoolean(false)

    private val subscriptions: ConcurrentSkipListMap[Int, Subscription] =
      new ConcurrentSkipListMap[Int, Subscription]()

    /** The partitions this consumer is currently owed blocks for, as a set. */
    private val owedPartitions: ConcurrentSkipListMap[Int, java.lang.Boolean] =
      new ConcurrentSkipListMap[Int, java.lang.Boolean]()

    /** Partitions whose terminator is ready to write, without a scan of every subscription. */
    private val readyTerminationPartitions = new ConcurrentLinkedQueue[Integer]()

    private val readyTerminationSet: ConcurrentHashMap[Int, java.lang.Boolean] =
      new ConcurrentHashMap[Int, java.lang.Boolean]()

    private val closedFlag: AtomicBoolean = new AtomicBoolean(false)
    private val completionRetired: AtomicBoolean = new AtomicBoolean(false)
    private val retainedConsumerRegistered: AtomicBoolean = new AtomicBoolean(false)
    private val completedSubscriptions: AtomicInteger = new AtomicInteger(0)
    private val queuedBlockCount: AtomicLong = new AtomicLong(0L)
    private val queuedByteCount: AtomicLong = new AtomicLong(0L)
    private val bytesServed: AtomicLong = new AtomicLong(0L)
    private val ackProgressMs: AtomicLong = new AtomicLong(createdAtMs)
    private val inboundMs: AtomicLong = new AtomicLong(createdAtMs)
    private val reclamationWarned: AtomicBoolean = new AtomicBoolean(false)

    /** The key sessions are visited in when egress is drained, lowest first. */
    def orderingKey: Long = bytesServed.get()

    /** Whether this session has been released; a closed session accepts nothing further. */
    def isClosed: Boolean = closedFlag.get()

    /** Whether a worker drain can inspect queued, owed or termination work for this session. */
    def hasDrainWork: Boolean =
      !queue.isEmpty || !owedPartitions.isEmpty || !readyTerminationPartitions.isEmpty

    /** Claims the one-shot final consumer unregister after all subscribed streams complete. */
    def claimCompletionRetirement(): Boolean = completionRetired.compareAndSet(false, true)

    /** Whether this session's stable cursor has been admitted to the retained store. */
    def hasRetainedConsumerRegistration: Boolean = retainedConsumerRegistered.get()

    /** Records the idempotent retained-store registration after the store admits this cursor. */
    def markRetainedConsumerRegistered(): Unit = retainedConsumerRegistered.set(true)

    /** Releases this session's queue, once. */
    def close(): Unit = {
      if (closedFlag.compareAndSet(false, true)) {
        readyQueued.set(false)
        readyTerminationPartitions.clear()
        readyTerminationSet.clear()
        var pending = queue.poll()
        while (pending != null) {
          releasePending(pending)
          pending = queue.poll()
        }
        subscriptions.values().asScala.foreach { subscription =>
          var sentEntries = 0L
          var sent = subscription.sentBytes.pollFirstEntry()
          while (sent != null) {
            sentEntries += 1L
            sent = subscription.sentBytes.pollFirstEntry()
          }
          subscription.unacknowledgedBytes.set(0L)
          if (sentEntries > 0) {
            backpressure.releaseMetadataQuota(
              sentEntries * SENT_BLOCK_METADATA_BYTES)
          }
        }
      }
    }

    /** Whether this consumer has subscribed to one partition on this channel. */
    def subscribedTo(partitionId: Int): Boolean = subscriptions.containsKey(partitionId)

    /**
     * Subscribes this consumer to one partition.
     *
     * @return true when the subscription is new, which is the signal to resume delivery from
     *     the position this consumer reports
     */
    def subscribe(partitionId: Int): Boolean = {
      !isClosed && subscriptions.putIfAbsent(partitionId, new Subscription()) == null
    }

    /** How many partitions this consumer has subscribed to. */
    def subscriptionCount: Int = subscriptions.size()

    /**
     * The partitions this consumer subscribed to, so that a teardown can release exactly the
     * per-consumer ledgers this session opened and no others.
     */
    def subscribedPartitions: Seq[Int] =
      subscriptions.keySet().asScala.toSeq.map(_.intValue())

    /**
     * Queues one block reference for this consumer, if the queue has room for it.
     *
     * @return true if the reference was queued
     */
    def offer(pending: PendingBlock): Boolean = {
      val subscription = subscriptions.get(pending.partitionId)
      if (isClosed || subscription == null || !hasQueueCapacity(pending.framedBytes)) {
        false
      } else if (!backpressure.tryReserveMetadataQuota(PENDING_BLOCK_METADATA_BYTES)) {
        false
      } else if (isClosed) {
        backpressure.releaseMetadataQuota(PENDING_BLOCK_METADATA_BYTES)
        false
      } else {
        subscription.queuedBlocks.incrementAndGet()
        subscription.queuedBytes.addAndGet(pending.framedBytes.toLong)
        queuedBlockCount.incrementAndGet()
        queuedByteCount.addAndGet(pending.framedBytes.toLong)
        val queued = queue.offer(pending)
        if (!queued) {
          releasePending(pending)
          false
        } else if (isClosed && queue.remove(pending)) {
          // Closure may have drained the queue between the pre-reservation check and this offer.
          releasePending(pending)
          false
        } else {
          drainWakeup.set(true)
          true
        }
      }
    }

    /** Returns one polled reference to the queue unless closure has made it unservable. */
    def requeue(pending: PendingBlock): Boolean = {
      if (isClosed) {
        releasePending(pending)
        false
      } else {
        queue.offer(pending)
        if (isClosed && queue.remove(pending)) {
          releasePending(pending)
          false
        } else {
          true
        }
      }
    }

    /** Whether the queue can hold one more reference of the given framed size. */
    def hasQueueCapacity(framedBytes: Int): Boolean = {
      queuedBlockCount.get() < MAX_QUEUED_BLOCKS_PER_SESSION &&
        queuedByteCount.get() + framedBytes.toLong <= MAX_QUEUED_BYTES_PER_SESSION
    }

    /**
     * Records that this consumer is owed an inclusive run of blocks it has not been queued.
     *
     * @return true when this consumer is owed anything at all afterwards
     */
    def deferOwed(partitionId: Int, first: Long, last: Long): Boolean = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null || last < first) {
        false
      } else {
        subscription.owedFrom.getAndUpdate { current =>
          if (current == MemorySpillManager.UNSET_SEQUENCE) first else math.min(current, first)
        }
        subscription.owedThrough.getAndUpdate { current =>
          if (current == MemorySpillManager.UNSET_SEQUENCE) last else math.max(current, last)
        }
        // Indexed after the range is published, so a concurrent top-up that has just read the range
        // as empty and is about to de-index the partition cannot lose this record: its own re-read
        // happens after its removal and sees the range this call has already written.
        owedPartitions.put(partitionId, java.lang.Boolean.TRUE)
        true
      }
    }

    /**
     * Takes the next sequence number owed for one partition, advancing the run past it.
     *
     * @return the sequence number, or [[MemorySpillManager.UNSET_SEQUENCE]] when nothing is
     *     owed
     */
    def takeOwed(partitionId: Int): Long = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null) {
        MemorySpillManager.UNSET_SEQUENCE
      } else {
        val from = subscription.owedFrom.get()
        val through = subscription.owedThrough.get()
        if (from == MemorySpillManager.UNSET_SEQUENCE || from > through) {
          MemorySpillManager.UNSET_SEQUENCE
        } else if (subscription.owedFrom.compareAndSet(from, from + 1L)) {
          from
        } else {
          // Another drain took this position; the caller re-reads on its next pass.
          MemorySpillManager.UNSET_SEQUENCE
        }
      }
    }

    /** Drops everything at or below one position from the owed run of one partition. */
    def discardOwedThrough(partitionId: Int, position: Long): Unit = {
      val subscription = subscriptions.get(partitionId)
      if (subscription != null && position >= 0L) {
        subscription.owedFrom.getAndUpdate { current =>
          if (current == MemorySpillManager.UNSET_SEQUENCE) current
          else math.max(current, position + 1L)
        }
      }
    }

    /**
     * Clears the whole owed run of one partition and reports the first position it held.
     *
     * @param partitionId the partition whose owed run is being abandoned
     * @return the first position that was owed, or `UNSET_SEQUENCE` when nothing was
     */
    def discardAllOwed(partitionId: Int): Long = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null) {
        MemorySpillManager.UNSET_SEQUENCE
      } else {
        val first = subscription.owedFrom.getAndSet(MemorySpillManager.UNSET_SEQUENCE)
        val through = subscription.owedThrough.getAndSet(MemorySpillManager.UNSET_SEQUENCE)
        owedPartitions.remove(partitionId)
        if (first == MemorySpillManager.UNSET_SEQUENCE || first > through) {
          MemorySpillManager.UNSET_SEQUENCE
        } else {
          first
        }
      }
    }

    /** Drops one partition from the owed index if its run has in fact emptied. */
    def deindexIfSettled(partitionId: Int): Unit = {
      owedPartitions.remove(partitionId)
      if (owedBlocksFor(partitionId) > 0L) {
        owedPartitions.put(partitionId, java.lang.Boolean.TRUE)
      }
    }

    /** The partitions this consumer may still be owed blocks for; empty in the ordinary case. */
    def owedPartitionIds: Seq[Int] =
      owedPartitions.keySet().asScala.toSeq.map(_.intValue())

    /** Blocks owed to this consumer for one partition but not yet queued. */
    def owedBlocksFor(partitionId: Int): Long = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null) {
        0L
      } else {
        val from = subscription.owedFrom.get()
        val through = subscription.owedThrough.get()
        if (from == MemorySpillManager.UNSET_SEQUENCE || from > through) 0L else through - from + 1L
      }
    }

    /** Blocks owed to this consumer but not yet queued, across every partition. */
    def owedBlocks: Long = {
      var total = 0L
      owedPartitions.keySet().asScala.foreach(partitionId =>
        total += owedBlocksFor(partitionId.intValue()))
      total
    }

    /** Discharges the queue accounting of one reference that has left the queue. */
    def releasePending(pending: PendingBlock): Unit = {
      val subscription = subscriptions.get(pending.partitionId)
      if (subscription != null) {
        subscription.queuedBlocks.decrementAndGet()
        subscription.queuedBytes.addAndGet(-pending.framedBytes.toLong)
      }
      queuedBlockCount.decrementAndGet()
      queuedByteCount.addAndGet(-pending.framedBytes.toLong)
      backpressure.releaseMetadataQuota(PENDING_BLOCK_METADATA_BYTES)
    }

    /** Records that one block has been handed to this consumer's channel. */
    def canTrackSent(partitionId: Int, sequenceNumber: Long): Boolean = {
      val subscription = subscriptions.get(partitionId)
      !isClosed && subscription != null && (
        subscription.sentBytes.containsKey(sequenceNumber) ||
          subscription.sentBytes.size() < MAX_SENT_BLOCKS_PER_SUBSCRIPTION)
    }

    def recordSent(
        partitionId: Int,
        sequenceNumber: Long,
        framedBytes: Long,
        metadataReserved: Boolean): Boolean = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null || isClosed) {
        if (metadataReserved) {
          backpressure.releaseMetadataQuota(SENT_BLOCK_METADATA_BYTES)
        }
        false
      } else {
        val key = java.lang.Long.valueOf(sequenceNumber)
        val value = java.lang.Long.valueOf(framedBytes)
        val inserted = subscription.sentBytes.putIfAbsent(key, value) == null
        if (inserted) {
          subscription.unacknowledgedBytes.addAndGet(framedBytes)
        } else if (metadataReserved) {
          backpressure.releaseMetadataQuota(SENT_BLOCK_METADATA_BYTES)
        }
        if (isClosed && inserted && subscription.sentBytes.remove(key, value)) {
          subscription.unacknowledgedBytes.addAndGet(-framedBytes)
          if (metadataReserved) {
            backpressure.releaseMetadataQuota(SENT_BLOCK_METADATA_BYTES)
          }
          false
        } else if (isClosed) {
          if (inserted) {
            subscription.unacknowledgedBytes.set(0L)
          }
          false
        } else {
          advanceSent(partitionId, sequenceNumber)
          bytesServed.addAndGet(framedBytes)
          true
        }
      }
    }

    /** Advances the highest position this consumer has been sent, never backwards. */
    def advanceSent(partitionId: Int, position: Long): Unit = {
      val subscription = subscriptions.get(partitionId)
      if (subscription != null) {
        subscription.sentPosition.accumulateAndGet(position, (a, b) => math.max(a, b))
      }
    }

    /**
     * Advances this consumer's acknowledged position and discharges the prefix it covers.
     *
     * @return true when the position genuinely advanced, which is what authorises the retained
     *     store to be told about it; a repeated or stale position moves nothing
     */
    def advanceAck(partitionId: Int, position: Long): Boolean = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null) {
        false
      } else {
        val previous = subscription.ackPosition.getAndAccumulate(position, (a, b) => math.max(a, b))
        if (position <= previous) {
          false
        } else {
          val released = subscription.sentBytes.headMap(position, true)
          var releasedBytes = 0L
          var entry = released.pollFirstEntry()
          var releasedEntries = 0L
          while (entry != null) {
            releasedBytes += entry.getValue.longValue()
            releasedEntries += 1L
            entry = released.pollFirstEntry()
          }
          if (releasedEntries > 0L) {
            backpressure.releaseMetadataQuota(
              releasedEntries * SENT_BLOCK_METADATA_BYTES)
          }
          if (releasedBytes > 0L) {
            subscription.unacknowledgedBytes.addAndGet(-releasedBytes)
          }
          true
        }
      }
    }

    /** The highest position written to this consumer for one partition, or nothing sent. */
    def sentPosition(partitionId: Int): Long = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null) AckMessage.NOTHING_CONSUMED else subscription.sentPosition.get()
    }

    /** The highest position this consumer has acknowledged for one partition. */
    def ackPosition(partitionId: Int): Long = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null) AckMessage.NOTHING_CONSUMED else subscription.ackPosition.get()
    }

    /**
     * Claims one acknowledgement as the newest applied for its partition, or refuses it as stale.
     *
     * @param partitionId the stream the acknowledgement concerns
     * @param ack the acknowledgement, whose control sequence number decides the outcome
     * @return true when this call is the one that applied it; false for a duplicate, a
     *     reordered delivery, or a partition this consumer is not subscribed to
     */
    def applyControlSequence(partitionId: Int, ack: AckMessage): Boolean = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null) {
        false
      } else {
        var current = subscription.appliedControlSequence.get()
        var applied = false
        var settled = false
        while (!settled) {
          if (!ack.supersedes(current)) {
            settled = true
          } else if (subscription.appliedControlSequence
              .compareAndSet(current, ack.sequenceNumber())) {
            applied = true
            settled = true
          } else {
            current = subscription.appliedControlSequence.get()
          }
        }
        applied
      }
    }

    /** Blocks sent to this consumer but not yet acknowledged by it, for one partition. */
    def outstandingFor(partitionId: Int): Long = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null) 0L else subscription.sentBytes.size().toLong
    }

    /**
     * Records that this partition's stream has been ended for this consumer because a block it was
     * owed could not be produced, and answers whether this call is the one that recorded it.
     */
    def markStreamAborted(partitionId: Int): Boolean = {
      val subscription = subscriptions.get(partitionId)
      subscription != null && subscription.streamAborted.compareAndSet(false, true)
    }

    /** Blocks queued but not yet written for one partition. */
    def pendingBlocksFor(partitionId: Int): Long = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null) 0L else math.max(0L, subscription.queuedBlocks.get())
    }

    /** Blocks queued but not yet written, across every partition. */
    def pendingBlocks: Long = math.max(0L, queuedBlockCount.get())

    /** Framed bytes queued but not yet written, across every partition. */
    def pendingBytes: Long = math.max(0L, queuedByteCount.get())

    /** Framed bytes written to this consumer but not yet acknowledged by it. */
    def unacknowledgedBytes: Long = {
      var total = 0L
      subscriptions.values().asScala.foreach { subscription =>
        total += math.max(0L, subscription.unacknowledgedBytes.get())
      }
      total
    }

    /** Framed size of the block at the head of the queue, which is what a refill must cover. */
    def headFramedBytes: Long = {
      val head = queue.peek()
      if (head == null) 0L else head.framedBytes.toLong
    }

    /** Whether everything a partition has offered has been written to this consumer. */
    def caughtUpWith(partitionId: Int, highestOffered: Long): Boolean = {
      val subscription = subscriptions.get(partitionId)
      subscription != null && subscription.sentPosition.get() >= highestOffered
    }

    /** Claims the one-shot right to write one partition's terminator to this consumer. */
    def claimTermination(partitionId: Int): Boolean = {
      val subscription = subscriptions.get(partitionId)
      subscription != null && subscription.terminationClaimed.compareAndSet(false, true)
    }

    /** Queues one ready terminator exactly once until a worker polls it. */
    def enqueueReadyTermination(partitionId: Int): Boolean = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null || subscription.terminationDelivered.get()) {
        false
      } else if (readyTerminationSet.putIfAbsent(
          partitionId, java.lang.Boolean.TRUE) == null) {
        readyTerminationPartitions.offer(Integer.valueOf(partitionId))
        true
      } else {
        false
      }
    }

    /** Polls one ready terminator, or -1 when no partition is ready. */
    def pollReadyTermination(): Int = {
      val partitionId = readyTerminationPartitions.poll()
      if (partitionId == null) {
        -1
      } else {
        readyTerminationSet.remove(partitionId)
        partitionId.intValue()
      }
    }

    /** Records a terminator reaching the socket and whether this was the first record. */
    def confirmTermination(partitionId: Int): Boolean = {
      val subscription = subscriptions.get(partitionId)
      subscription != null && subscription.terminationDelivered.compareAndSet(false, true)
    }

    /** Surrenders a termination claim whose write failed, so a later pass may try again. */
    def releaseTerminationClaim(partitionId: Int): Unit = {
      val subscription = subscriptions.get(partitionId)
      if (subscription != null) {
        subscription.terminationClaimed.set(false)
      }
    }

    /** Whether one partition's terminator has been confirmed onto this consumer's socket. */
    def terminationConfirmed(partitionId: Int): Boolean = {
      val subscription = subscriptions.get(partitionId)
      subscription != null && subscription.terminationDelivered.get()
    }

    /**
     * Counts one subscription complete once both its terminator and final acknowledgement exist.
     */
    def recordSubscriptionCompletion(partitionId: Int, highestOffered: Long): Boolean = {
      val subscription = subscriptions.get(partitionId)
      if (subscription != null &&
          subscription.terminationDelivered.get() &&
          subscription.ackPosition.get() >= highestOffered &&
          subscription.completionCounted.compareAndSet(false, true)) {
        completedSubscriptions.incrementAndGet()
        true
      } else {
        false
      }
    }

    /** Whether every subscription has reached its final acknowledgement and terminator. */
    def allSubscriptionsComplete: Boolean = {
      val total = subscriptions.size()
      total > 0 && completedSubscriptions.get() >= total
    }

    /** Stamps acknowledgement progress, which is the only stamp the stall detector consults. */
    def stampAckProgress(nowMs: Long): Unit = {
      ackProgressMs.set(nowMs)
      inboundMs.set(nowMs)
    }

    /** When this consumer last acknowledged anything. */
    def lastAckProgressMs: Long = ackProgressMs.get()

    /** Stamps liveness without stamping progress. */
    def stampHeartbeat(nowMs: Long): Unit = inboundMs.set(nowMs)

    /** When this consumer last sent anything at all, acknowledgement or heartbeat. */
    def lastInboundMs: Long = inboundMs.get()

    /** The instant from which this consumer's next retransmission request may be serviced. */
    def nextRetransmitAtMs(partitionId: Int): Long = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null) 0L else subscription.nextRetransmitAtMs.get()
    }

    /** Charges one retransmission attempt against this consumer's budget for one partition. */
    def chargeRetransmitAttempt(partitionId: Int): Int = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null) Int.MaxValue else subscription.retransmitAttempts.incrementAndGet()
    }

    /** Defers this consumer's next retransmission of one partition until an instant. */
    def deferRetransmitUntil(partitionId: Int, atMs: Long): Unit = {
      val subscription = subscriptions.get(partitionId)
      if (subscription != null) {
        subscription.nextRetransmitAtMs.set(atMs)
      }
    }

    /** Restores the retransmission budget of one partition, because the consumer made progress. */
    def resetRetransmitBudget(partitionId: Int): Unit = {
      val subscription = subscriptions.get(partitionId)
      if (subscription != null) {
        subscription.retransmitAttempts.set(0)
        subscription.nextRetransmitAtMs.set(0L)
      }
    }

    /** Claims the one-shot right to report a reclamation budget breach for this session. */
    def warnReclamationOnce(): Boolean = reclamationWarned.compareAndSet(false, true)
  }

  /** One consumer's state for one reduce partition. */
  private final class Subscription {

    val queuedBlocks: AtomicLong = new AtomicLong(0L)

    /** Framed bytes queued for this partition on this session but not yet written. */
    val queuedBytes: AtomicLong = new AtomicLong(0L)

    /**
     * Framed size of each block written to this consumer and not yet acknowledged, keyed by
     * sequence number.
     */
    val sentBytes: ConcurrentSkipListMap[Long, java.lang.Long] =
      new ConcurrentSkipListMap[Long, java.lang.Long]()

    val sentPosition: AtomicLong = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    /**
     * Whether this partition's stream has been ended for this consumer because a block it was owed
     * could not be produced.
     */
    val streamAborted: AtomicBoolean = new AtomicBoolean(false)

    val ackPosition: AtomicLong = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    /** The highest <em>control</em> sequence number of an acknowledgement already applied here. */
    val appliedControlSequence: AtomicLong = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    /** Framed bytes written to this consumer and not yet acknowledged. */
    val unacknowledgedBytes: AtomicLong = new AtomicLong(0L)

    /** One-shot guard so the terminator is written to this consumer at most once at a time. */
    val terminationClaimed: AtomicBoolean = new AtomicBoolean(false)

    val terminationDelivered: AtomicBoolean = new AtomicBoolean(false)

    val completionCounted: AtomicBoolean = new AtomicBoolean(false)

    /**
     * Lowest sequence number owed to this consumer that has not been queued yet, or
     * [[MemorySpillManager.UNSET_SEQUENCE]] when nothing is owed.
     */
    val owedFrom: AtomicLong = new AtomicLong(MemorySpillManager.UNSET_SEQUENCE)

    /** Highest sequence number in the owed run, inclusive; unused while [[owedFrom]] is unset. */
    val owedThrough: AtomicLong = new AtomicLong(MemorySpillManager.UNSET_SEQUENCE)

    /** Retransmission attempts serviced for this consumer since its last acknowledgement. */
    val retransmitAttempts: AtomicInteger = new AtomicInteger(0)

    /** The instant from which this consumer's next retransmission may be serviced. */
    val nextRetransmitAtMs: AtomicLong = new AtomicLong(0L)
  }
}

/**
 * The one streaming shuffle listener an executor binds, shared by every producer running on it.
 *
 * @param conf the executor's configuration, used only to build the transport tuning namespace
 * @param clock the time source the log suppression windows are measured on.
 * @param limits executor-wide and per-peer bounds on remotely keyed listener state
 */
private[spark] class StreamingShuffleListener(
    conf: SparkConf,
    clock: Clock = new SystemClock,
    limits: StreamingShuffleListener.Limits = StreamingShuffleListener.DefaultLimits)
  extends RpcHandler with StreamingShuffleRouteRegistry with Logging {

  import StreamingShuffleListener._

  require(conf != null, "The Spark configuration must not be null.")
  require(limits != null, "The streaming shuffle listener limits must not be null.")

  /** The producer handlers this executor is currently serving, keyed by shuffle and map id. */
  private val producers = new ConcurrentHashMap[ProducerKey, StreamingShuffleServerHandler]()

  /** Bounded routing state held for one authenticated consumer channel. */
  private final class ChannelParticipation(val peerKey: String) {
    val handlers = new ConcurrentHashMap[ProducerKey, StreamingShuffleServerHandler]()
  }

  /** Which producer handlers each consumer channel has reached, keyed by the channel's own id. */
  private val channelParticipants =
    new ConcurrentHashMap[String, ChannelParticipation]()

  /** Guards first-use and teardown of remotely keyed participation state. */
  private val participantRegistryLock = new Object()

  private var liveParticipantChannels = 0
  private var liveParticipantRoutes = 0

  private val participantChannelsByPeer = HashMap.empty[String, Int]
  private val participantRoutesByPeer = HashMap.empty[String, Int]

  /** The executor-scoped upkeep thread, created when the first producer registers. */
  private var maintenance: ScheduledExecutorService = null

  private val maintenanceLock = new Object()

  /** Rounds of upkeep this listener has run, for diagnostics and for assertions in tests. */
  private val maintenanceRounds = new AtomicLong(0L)

  /**
   * The windows that bound the two per-map-task records this router makes: a producer becoming
   * served, and a producer ceasing to be.
   */
  private val registrationLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private val withdrawalLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /**
   * Bytes and blocks this executor has put on the wire, and consumer sessions it has accepted,
   * accumulated across every producer it has ever served.
   */
  private val streamedBytesTotal = new AtomicLong(0L)

  private val streamedBlocksTotal = new AtomicLong(0L)

  private val acknowledgedBlocksTotal = new AtomicLong(0L)

  private val acceptedSessionsTotal = new AtomicLong(0L)

  private val retiredConsumers = new AtomicLong(0L)

  private val unroutableFrames = new AtomicLong(0L)

  /** Whether an unroutable frame has been logged, so the log budget is not spent on them. */
  private val unroutableReported = new AtomicBoolean(false)

  private val malformedFrames = new AtomicLong(0L)

  private val abusiveChannelsClosed = new AtomicLong(0L)

  private val unauthenticatedFrames = new AtomicLong(0L)

  /** Channels refused before participant state could exceed an executor or peer quota. */
  private val remoteStateRefusals = new AtomicLong(0L)

  /**
   * Channels closed because a fault impugned the channel itself rather than one producer's session.
   */
  private val faultedChannelsClosed = new AtomicLong(0L)

  /** The windowed reporter for failures a peer can provoke: unhandled frames and channel faults. */
  private val abuseReporter = new BoundedReporter

  /** The windowed reporter for routine notices a peer can provoke, such as accepting a channel. */
  private val noticeReporter = new BoundedReporter

  /** Callback bodies contained by [[guard]], and the window that bounds reporting them. */
  private val guardFailures = new AtomicLong(0L)

  private val guardReporter = new BoundedReporter

  // The feature's own debug key, read once.
  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  /** Frame failures charged to each live channel, keyed the same way participation is. */
  private val malformedByChannel = new ConcurrentHashMap[String, AtomicLong]()

  private val malformedRegistryLock = new Object()

  /**
   * This router serves no chunked streams, only one-way messages, so it offers an ordinary empty
   * stream manager.
   */
  private val streamManager = new OneForOneStreamManager()

  override def getStreamManager(): StreamManager = streamManager

  // Registration, driven by the manager

  /**
   * Begins routing frames for one producer.
   *
   * @param shuffleId the shuffle whose output the handler serves
   * @param mapId the map task whose output the handler serves
   * @param handler the producer side handler to route to
   */
  def register(shuffleId: Int, mapId: Long, handler: StreamingShuffleServerHandler): Unit = {
    require(handler != null, "The streaming shuffle producer handler must not be null.")
    val key = ProducerKey(shuffleId, mapId)
    val previous = producers.put(key, handler)
    if (previous != null && previous.ne(handler)) {
      // Superseded rather than duplicated: forget the old handler on every channel that had reached
      // it, so a channel closing later cannot deliver a loss notice to a handler no longer serving.
      removeParticipantRoute(key, previous)
    }
    startMaintenance()
    // Bounded per executor, because this fires once per streaming map task and the number of map
    // tasks is the workload's to choose: a record apiece is a log volume proportional to
    // throughput, which was the second largest contributor to this subsystem's output.
    reportRegistration(
      log"Streaming shuffle listener is serving shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
        log"${MDC(MAP_ID, mapId)}; ${MDC(COUNT, producers.size())} producer(s) registered",
      registrationLogAggregator)
  }

  /**
   * Reports one registration or withdrawal at default level at most once per window, and otherwise
   * only under the debug key.
   *
   * @param entry the record to make
   * @param aggregator the executor-scoped window this record is bounded by
   */
  private def reportRegistration(
      entry: => MessageWithContext,
      aggregator: MemorySpillManager.ExecutorLogAggregator): Unit = {
    aggregator.record(clock.getTimeMillis()) match {
      case Some(summary) =>
        logInfo(entry + log"; ${MDC(NUM_EVENTS, summary.occurrences)} such change(s) on this " +
          log"executor, ${MDC(NUM_SKIPPED, summary.unreported)} of them not reported " +
          log"individually so that the executor stays inside its log budget")
      case None =>
        // At the level it was emitted at, under the feature's own key, which is this class's own
        // convention for opted-in detail: `logDebug` would also demand the logging framework be at
        // DEBUG, so setting the streaming debug key alone would restore nothing.
        if (debugEnabled) {
          logInfo(entry)
        }
    }
  }

  /** Starts the upkeep sweep, once, on the first registration. */
  private def startMaintenance(): Unit = {
    maintenanceLock.synchronized {
      if (maintenance == null) {
        val scheduler =
          ThreadUtils.newDaemonSingleThreadScheduledExecutor("streaming-shuffle-maintenance")
        maintenance = scheduler
        scheduler.scheduleWithFixedDelay(
          () => runMaintenance(),
          MAINTENANCE_INTERVAL_MS,
          MAINTENANCE_INTERVAL_MS,
          TimeUnit.MILLISECONDS)
        logInfo(log"Streaming shuffle listener started its upkeep sweep at a " +
          log"${MDC(DURATION, MAINTENANCE_INTERVAL_MS)} ms cadence")
      }
    }
  }

  /**
   * Runs one round of upkeep across every producer this executor serves.
   *
   * @return the number of consumers this round retired across every producer
   */
  def runMaintenance(): Int = {
    maintenanceRounds.incrementAndGet()
    var retired = 0
    producers.values().asScala.toSeq.foreach { handler =>
      guard(s"run upkeep for ${handler.shuffleId}/${handler.mapId}") {
        retired += handler.runMaintenance()
      }
    }
    if (retired > 0) {
      retiredConsumers.addAndGet(retired.toLong)
    }
    retired
  }

  /** Rounds of upkeep this listener has run. */
  def maintenanceRoundCount: Long = maintenanceRounds.get()

  /** Consumers the upkeep sweep has retired across every producer. */
  def retiredConsumerCount: Long = retiredConsumers.get()

  /**
   * Withdraws one producer generation from every owner of it on this executor.
   *
   * @param shuffleId the shuffle whose producer is being withdrawn
   * @param mapId the map task whose producer is being withdrawn
   * @param reason short description of what is being recovered from, for the operator's log
   * @return true if a generation was routed and has now been withdrawn
   */
  def deregister(shuffleId: Int, mapId: Long, reason: String): Boolean = {
    val routed = producers.get(ProducerKey(shuffleId, mapId))
    if (routed == null) {
      false
    } else {
      var withdrawn = false
      guard(s"withdraw the producer generation of shuffle $shuffleId map $mapId") {
        withdrawn = routed.withdrawGeneration(reason)
      }
      withdrawn
    }
  }

  /** Removes one producer generation's routing entry, and nothing else. */
  override def withdrawRoute(
      shuffleId: Int,
      mapId: Long,
      handler: StreamingShuffleServerHandler): Boolean = {
    val key = ProducerKey(shuffleId, mapId)
    val removed = producers.remove(key, handler)
    if (removed) {
      removeParticipantRoute(key)
      // Harvested before the handler becomes unreachable.
      streamedBytesTotal.addAndGet(handler.bytesWrittenToChannel)
      streamedBlocksTotal.addAndGet(handler.blocksWrittenToChannel)
      acknowledgedBlocksTotal.addAndGet(handler.ackCount)
      acceptedSessionsTotal.addAndGet(handler.acceptedSessionCount)
      // Bounded on a window of its own rather than the registration window, so that a stage's
      // registrations cannot silence its withdrawals or the reverse: the two are opposite halves of
      // the same ledger, and an operator reading only one of them would infer a producer count that
      // never comes back down.
      reportRegistration(
        log"Streaming shuffle listener stopped serving shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)}; " +
          log"${MDC(COUNT, producers.size())} producer(s) remain",
        withdrawalLogAggregator)
    }
    removed
  }

  /**
   * Drops one producer's participation in one consumer channel, and closes the channel only when no
   * producer participates in it any more.
   *
   * @param channel the consumer channel the producer is leaving
   * @param handler the producer leaving it
   * @param closeWhenLast whether an unreferenced channel should be closed
   * @return true when this call closed the channel
   */
  override def releaseChannelParticipation(
      channel: Channel,
      handler: StreamingShuffleServerHandler,
      closeWhenLast: Boolean): Boolean = {
    if (channel == null) {
      return false
    }
    var closed = false
    guard("release a producer's participation in a streaming shuffle channel") {
      val key = channel.id().asLongText()
      // The route and, when it was the last, the channel are given back inside the participation
      // ledger's own lock, so the executor-wide and per-peer allowances this channel was holding
      // are released by the same transaction that forgets it.
      val emptied = participantRegistryLock.synchronized {
        val participants = channelParticipants.get(key)
        if (participants == null) {
          false
        } else {
          val removed =
            participants.handlers.remove(ProducerKey(handler.shuffleId, handler.mapId), handler)
          if (removed) {
            liveParticipantRoutes = math.max(0, liveParticipantRoutes - 1)
            decrementPeerUsage(participantRoutesByPeer, participants.peerKey, 1)
          }
          // The entry is withdrawn only when the socket is going with it: while it stays open its
          // channel allowance is legitimately held, and [[removeParticipantChannel]] returns that
          // one when the channel goes inactive.
          closeWhenLast && participants.handlers.isEmpty &&
            channelParticipants.remove(key, participants) && {
              liveParticipantChannels = math.max(0, liveParticipantChannels - 1)
              decrementPeerUsage(participantChannelsByPeer, participants.peerKey, 1)
              true
            }
        }
      }
      if (emptied) {
        // Outside the ledger's lock: a close is asynchronous and its callbacks belong to the event
        // loop, and nothing after this point touches the ledger.
        forgetChannelFailures(channel)
        channel.close()
        closed = true
        if (debugEnabled) {
          logDebug(log"Streaming shuffle listener closed the channel from " +
            log"${MDC(HOST_PORT, channel.remoteAddress())} after its last producer left")
        }
      }
    }
    closed
  }

  /**
   * Closes a consumer channel for a fault that impugns the channel itself.
   *
   * @param channel the channel to close
   * @param handler the producer that observed the fault
   * @param reason operator-facing description of the fault
   */
  override def closeFaultedChannel(
      channel: Channel,
      handler: StreamingShuffleServerHandler,
      reason: String): Unit = {
    if (channel != null) {
      guard("close a faulted streaming shuffle channel") {
        faultedChannelsClosed.incrementAndGet()
        reportAbuse(log"Streaming shuffle listener is closing the channel from " +
          log"${MDC(HOST_PORT, channel.remoteAddress())} because " +
          log"${MDC(REASON, reason)}; it was observed by the producer of shuffle " +
          log"${MDC(SHUFFLE_ID, handler.shuffleId)} map ${MDC(MAP_ID, handler.mapId)}, and every " +
          log"producer the channel reached is told through the channel's own inactivity callback",
          null)
        channel.close()
      }
    }
  }

  /**
   * Channels closed because a fault impugned the channel itself rather than one producer's session.
   */
  def faultedChannelCloseCount: Long = faultedChannelsClosed.get()

  /**
   * Closes every consumer channel this executor is currently serving, as a channel-global fault.
   *
   * @param reason operator-facing description of the fault, carried into each channel's
   *     diagnostic
   * @return how many distinct channels were closed
   */
  private[streaming] def faultEveryServedChannel(reason: String): Int = {
    val victims = producers.values().asScala.iterator.flatMap { handler =>
      handler.activeSessionChannels.iterator.map(channel => (channel, handler))
    }.toSeq
    val distinctVictims = victims.groupBy { case (channel, _) => channel.id().asLongText() }
      .values
      .map(_.head)
      .toSeq
    distinctVictims.foreach { case (channel, handler) =>
      closeFaultedChannel(channel, handler, reason)
    }
    distinctVictims.size
  }

  /** How many producers one channel has reached. */
  private[streaming] def channelParticipantCount(channel: Channel): Int = {
    if (channel == null) {
      0
    } else {
      val participants = channelParticipants.get(channel.id().asLongText())
      if (participants == null) 0 else participants.handlers.size()
    }
  }

  /**
   * Payload and framing bytes this executor has put on the wire for streaming shuffle output, over
   * every producer it has served.
   */
  def streamedBytes: Long = streamedBytesTotal.get() + sumOverProducers(_.bytesWrittenToChannel)

  /** Data blocks this executor has put on the wire. */
  def streamedBlocks: Long = streamedBlocksTotal.get() + sumOverProducers(_.blocksWrittenToChannel)

  /** Acknowledgements consumers have returned to this executor's producers. */
  def acknowledgedBlocks: Long =
    acknowledgedBlocksTotal.get() + sumOverProducers(_.ackCount)

  /** Consumer sessions this executor's producers have accepted. */
  def acceptedSessions: Long =
    acceptedSessionsTotal.get() + sumOverProducers(_.acceptedSessionCount)

  /** Sums one counter across the producers currently registered. */
  private def sumOverProducers(counter: StreamingShuffleServerHandler => Long): Long = {
    var total = 0L
    producers.values().asScala.foreach(handler => total += counter(handler))
    total
  }

  /**
   * Stops routing every producer of one shuffle, which is how `unregisterShuffle` reaches them.
   *
   * @param shuffleId the shuffle being unregistered
   * @return how many producers were released
   */
  def deregisterShuffle(shuffleId: Int): Int = {
    val keys = producers.keySet().asScala.filter(_.shuffleId == shuffleId).toSeq
    keys.count(key =>
      deregister(key.shuffleId, key.mapId, s"shuffle $shuffleId was unregistered"))
  }

  /** Releases every producer this listener is serving, for the manager's own shutdown. */
  def releaseAll(): Unit = {
    stopMaintenance()
    producers.keySet().asScala.toSeq.foreach(key =>
      deregister(key.shuffleId, key.mapId, "the streaming shuffle manager stopped"))
    clearParticipantState()
    malformedRegistryLock.synchronized(malformedByChannel.clear())
    logRouterSummary()
  }

  /** States what this listener routed and what it refused, once, as it is released. */
  private def logRouterSummary(): Unit = {
    logInfo(log"Streaming shuffle listener released after " +
      log"${MDC(NUM_ITERATIONS, maintenanceRoundCount)} upkeep round(s), retiring " +
      log"${MDC(COUNT, retiredConsumerCount)} consumer(s); " +
      log"${MDC(NUM_EVENTS, unroutableFrameCount)} unroutable frame(s), " +
      log"${MDC(NUM_FAILURES, malformedFrameCount)} unhandled frame(s), " +
      log"${MDC(NUM_SKIPPED, unauthenticatedFrameCount)} unauthenticated frame(s), " +
      log"${MDC(THRESHOLD, remoteStateRefusalCount)} remote-state refusal(s) and " +
      log"${MDC(VALUE, abusiveChannelClosedCount)} channel(s) closed for exceeding the " +
      log"per-channel failure allowance")
    // Reported separately, and only when there is something to report: a healthy executor contains
    // nothing, so a line that always appeared would say "zero" for the life of every executor and
    // would train a reader to skip the one place the figure is stated.
    val contained = guardFailureCount
    if (contained > 0L) {
      logInfo(log"Streaming shuffle listener contained " +
        log"${MDC(NUM_FAILURES, contained)} callback failure(s) so that none reached the event " +
        log"loop shared by every producer on this executor")
    }
  }

  /** Stops the upkeep sweep, once, without waiting for a round in flight to finish. */
  private def stopMaintenance(): Unit = {
    val scheduler = maintenanceLock.synchronized {
      val current = maintenance
      maintenance = null
      current
    }
    if (scheduler != null) {
      scheduler.shutdownNow()
      // Awaited, not merely requested.
      val terminated = try {
        scheduler.awaitTermination(MAINTENANCE_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          scheduler.isTerminated
      }
      if (!terminated) {
        logWarning(log"The streaming shuffle listener's upkeep sweep did not terminate within " +
          log"${MDC(TIMEOUT, MAINTENANCE_SHUTDOWN_TIMEOUT_MS)} ms of being asked to stop")
      }
    }
  }

  /** How many producers this executor is currently serving. */
  def producerCount: Int = producers.size()

  /** How many frames have been dropped for naming a producer this executor does not serve. */
  def unroutableFrameCount: Long = unroutableFrames.get()

  /** How many frames this listener could not handle at all, whatever the reason. */
  def malformedFrameCount: Long = malformedFrames.get()

  /** How many channels were closed for exhausting their frame-failure allowance. */
  def abusiveChannelClosedCount: Long = abusiveChannelsClosed.get()

  /** Frames refused before decode because the transport had no authenticated identity. */
  def unauthenticatedFrameCount: Long = unauthenticatedFrames.get()

  /** Channels refused before their remotely keyed routing state could exceed a quota. */
  def remoteStateRefusalCount: Long = remoteStateRefusals.get()

  /** Consumer channels currently holding at least one producer route. */
  def participantChannelCount: Int =
    participantRegistryLock.synchronized(liveParticipantChannels)

  /** Producer routes currently attributed to consumer channels. */
  def participantRouteCount: Int =
    participantRegistryLock.synchronized(liveParticipantRoutes)

  /** Malformed-channel allowances currently held in the bounded ledger. */
  def trackedMalformedChannelCount: Int =
    malformedRegistryLock.synchronized(malformedByChannel.size())

  /** Whether a producer is currently routed, for the manager's own bookkeeping. */
  def serves(shuffleId: Int, mapId: Long): Boolean =
    producers.containsKey(ProducerKey(shuffleId, mapId))

  // Transport callbacks

  /** Routes one control frame that arrived as a one-way message. */
  override def receive(client: TransportClient, message: ByteBuffer): Unit = {
    if (StreamingShuffleServerHandler.authenticatedPrincipal(client).isDefined) {
      guardFrame(client, "route a one-way streaming shuffle frame")(route(client, message))
    } else {
      rejectUnauthenticated(client)
    }
  }

  /** Routes one control frame that arrived as a request expecting a reply. */
  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    if (StreamingShuffleServerHandler.authenticatedPrincipal(client).isDefined) {
      guardFrame(client, "route a request-shaped streaming shuffle frame")(route(client, message))
      callback.onSuccess(ByteBuffer.allocate(0))
    } else {
      callback.onFailure(rejectUnauthenticated(client))
    }
  }

  /** Notes a consumer channel, without allocating anything for it. */
  override def channelActive(client: TransportClient): Unit = {
    guard("note a new streaming shuffle channel") {
      // Reported through the shared window rather than per channel.
      reportNotice(log"Streaming shuffle listener accepted a channel from " +
        log"${MDC(HOST_PORT, client.getSocketAddress())}; " +
        log"${MDC(COUNT, producers.size())} producer(s) registered")
    }
  }

  /** Reports the loss of a consumer channel to exactly the producers it had reached. */
  override def channelInactive(client: TransportClient): Unit = {
    guard("report the loss of a streaming shuffle channel") {
      forgetChannelFailures(client)
      val participants = removeParticipantChannel(client)
      if (participants.nonEmpty) {
        participants.foreach { handler =>
          guard("report a channel loss to a producer handler")(handler.channelInactive(client))
        }
      }
    }
  }

  /** Reports a channel level failure to exactly the producers that channel had reached. */
  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    guard("report a streaming shuffle channel failure") {
      forgetChannelFailures(client)
      val participants = removeParticipantChannel(client)
      if (participants.isEmpty) {
        // Bounded for the same reason the frame path is: a peer can provoke a channel-level fault
        // as often as it can open a connection, and each one carried a full stack trace.
        reportAbuse(log"Streaming shuffle listener saw " +
          log"${MDC(ERROR, cause.getMessage())} on a channel from " +
          log"${MDC(HOST_PORT, client.getSocketAddress())} that had reached no producer; " +
          log"closing it", cause)
        guard("close a failed streaming shuffle channel")(client.getChannel().close())
      } else {
        participants.foreach { handler =>
          guard("report a channel failure to a producer handler") {
            handler.exceptionCaught(cause, client)
          }
        }
      }
    }
  }

  // Internals

  /** Hands one frame to the producer handler it names, recording the channel's participation. */
  private def route(client: TransportClient, message: ByteBuffer): Unit = {
    val shuffleId = StreamingShuffleMessage.peekShuffleId(message)
    val mapId = StreamingShuffleMessage.peekMapId(message)
    val key = ProducerKey(shuffleId, mapId)
    val handler = producers.get(key)
    if (handler == null) {
      dropUnroutable(client, shuffleId, mapId)
    } else if (admitParticipant(client, key, handler)) {
      handler.receive(client, message)
    } else {
      rejectRemoteState(client, key)
    }
  }

  /** Counts and drops a frame naming a producer this executor is not serving. */
  private def dropUnroutable(client: TransportClient, shuffleId: Int, mapId: Long): Unit = {
    unroutableFrames.incrementAndGet()
    if (unroutableReported.compareAndSet(false, true)) {
      logWarning(log"Streaming shuffle listener dropped a frame from " +
        log"${MDC(HOST_PORT, client.getSocketAddress())} addressed to shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)}, which this executor is not " +
        log"serving. Further occurrences are counted but not logged")
    }
  }

  /**
   * Admits one authenticated channel-to-producer route without exceeding any remote-state ceiling.
   */
  private def admitParticipant(
      client: TransportClient,
      key: ProducerKey,
      handler: StreamingShuffleServerHandler): Boolean = {
    StreamingShuffleServerHandler.authenticatedPrincipal(client) match {
      case None => false
      case Some(principal) =>
        val channelKey = channelKeyOf(client)
        val peerKey = peerKeyOf(client, principal)
        val observed = channelParticipants.get(channelKey)
        if (observed != null && observed.peerKey == peerKey &&
            observed.handlers.get(key) == handler) {
          true
        } else {
          participantRegistryLock.synchronized {
            var state = channelParticipants.get(channelKey)
            val newChannel = state == null
            val peerChannels = participantChannelsByPeer.getOrElse(peerKey, 0)
            val peerRoutes = participantRoutesByPeer.getOrElse(peerKey, 0)
            val channelRoutes = if (newChannel) 0 else state.handlers.size()
            val wrongPeer = !newChannel && state.peerKey != peerKey
            val channelLimitReached = newChannel &&
              (liveParticipantChannels >= limits.maxParticipantChannels ||
                peerChannels >= limits.maxParticipantChannelsPerPeer)
            val routeLimitReached =
              liveParticipantRoutes >= limits.maxParticipantRoutes ||
                peerRoutes >= limits.maxParticipantRoutesPerPeer ||
                channelRoutes >= limits.maxParticipantRoutesPerChannel

            if (wrongPeer || channelLimitReached) {
              false
            } else {
              val current = if (newChannel) null else state.handlers.get(key)
              if (current != null) {
                state.handlers.put(key, handler)
                true
              } else if (routeLimitReached) {
                false
              } else {
                if (newChannel) {
                  state = new ChannelParticipation(peerKey)
                  channelParticipants.put(channelKey, state)
                  liveParticipantChannels += 1
                  participantChannelsByPeer.update(peerKey, peerChannels + 1)
                }
                state.handlers.put(key, handler)
                liveParticipantRoutes += 1
                participantRoutesByPeer.update(peerKey, peerRoutes + 1)
                true
              }
            }
          }
        }
    }
  }

  /** Removes one channel's bounded participation state and returns the handlers it had reached. */
  private def removeParticipantChannel(
      client: TransportClient): Seq[StreamingShuffleServerHandler] = {
    participantRegistryLock.synchronized {
      val state = channelParticipants.remove(channelKeyOf(client))
      if (state == null) {
        Seq.empty
      } else {
        val routes = state.handlers.size()
        liveParticipantChannels = math.max(0, liveParticipantChannels - 1)
        liveParticipantRoutes = math.max(0, liveParticipantRoutes - routes)
        decrementPeerUsage(participantChannelsByPeer, state.peerKey, 1)
        decrementPeerUsage(participantRoutesByPeer, state.peerKey, routes)
        state.handlers.values().asScala.toSeq
      }
    }
  }

  /**
   * Removes one producer from every channel that had reached it, preserving the quota accounting.
   */
  private def removeParticipantRoute(
      key: ProducerKey,
      expected: StreamingShuffleServerHandler = null): Unit = {
    participantRegistryLock.synchronized {
      channelParticipants.entrySet().asScala.toSeq.foreach { entry =>
        val state = entry.getValue
        val removed = if (expected == null) {
          state.handlers.remove(key) != null
        } else {
          state.handlers.remove(key, expected)
        }
        if (removed) {
          liveParticipantRoutes = math.max(0, liveParticipantRoutes - 1)
          decrementPeerUsage(participantRoutesByPeer, state.peerKey, 1)
          if (state.handlers.isEmpty && channelParticipants.remove(entry.getKey, state)) {
            liveParticipantChannels = math.max(0, liveParticipantChannels - 1)
            decrementPeerUsage(participantChannelsByPeer, state.peerKey, 1)
          }
        }
      }
    }
  }

  /** Clears every participation ledger as one shutdown transaction. */
  private def clearParticipantState(): Unit = {
    participantRegistryLock.synchronized {
      channelParticipants.clear()
      participantChannelsByPeer.clear()
      participantRoutesByPeer.clear()
      liveParticipantChannels = 0
      liveParticipantRoutes = 0
    }
  }

  /** Decrements one peer count and removes its key when no state remains. */
  private def decrementPeerUsage(
      usage: HashMap[String, Int],
      peerKey: String,
      amount: Int): Unit = {
    val remaining = math.max(0, usage.getOrElse(peerKey, 0) - amount)
    if (remaining == 0) usage.remove(peerKey) else usage.update(peerKey, remaining)
  }

  /** Per-peer identity: authenticated principal plus remote host, never the source port. */
  private def peerKeyOf(client: TransportClient, principal: String): String = {
    val host = client.getSocketAddress() match {
      case address: InetSocketAddress =>
        Option(address.getAddress).map(_.getHostAddress).getOrElse(address.getHostString)
      case address => String.valueOf(address)
    }
    s"${principal.length}:$principal@$host"
  }

  /** Refuses an unauthenticated frame before inspecting its header or creating route state. */
  private def rejectUnauthenticated(client: TransportClient): SecurityException = {
    unauthenticatedFrames.incrementAndGet()
    val failure =
      new SecurityException("Streaming shuffle requires a Spark-authenticated transport channel.")
    reportAbuse(log"Streaming shuffle listener refused a frame from " +
      log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))} before decode because the " +
      log"channel did not complete Spark authentication", failure)
    client.close()
    failure
  }

  /** Refuses a new route before it can exceed an executor, peer or channel metadata quota. */
  private def rejectRemoteState(client: TransportClient, key: ProducerKey): Unit = {
    remoteStateRefusals.incrementAndGet()
    reportAbuse(log"Streaming shuffle listener refused ${MDC(DESCRIPTION, key.toString)} from " +
      log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))} before allocating routing " +
      log"state because an executor, peer or channel quota was exhausted", null)
    client.close()
  }

  /** The routing key of one channel. */
  private def channelKeyOf(client: TransportClient): String =
    client.getChannel().id().asLongText()

  /**
   * Runs one frame-driven callback body, bounding what a failure costs and what a failing channel
   * costs.
   *
   * @param client the channel the frame arrived on, charged for the failure and closed past the
   *     threshold
   * @param operation what was being attempted, for the report
   * @param body the frame-handling work
   */
  private def guardFrame(client: TransportClient, operation: String)(body: => Any): Unit = {
    try {
      body
    } catch {
      case NonFatal(e) =>
        malformedFrames.incrementAndGet()
        // Charged and closed before the report is considered, so that a channel past the threshold
        // is disconnected even in a window where its report is suppressed.
        if (chargeChannelFailure(client) && client.isActive()) {
          abusiveChannelsClosed.incrementAndGet()
          guard("close a channel that kept sending frames this listener could not handle") {
            client.getChannel().close()
          }
        }
        reportAbuse(log"Streaming shuffle listener failed to " +
          log"${MDC(DESCRIPTION, operation)} from " +
          log"${MDC(HOST_PORT, client.getSocketAddress())}; the listener stays up and the " +
          log"affected consumer's own liveness timer recovers. " +
          log"${MDC(COUNT, malformedFrames.get())} frame failure(s) so far, and " +
          log"${MDC(THRESHOLD, abusiveChannelsClosed.get())} channel(s) closed for exceeding " +
          log"the per-channel failure allowance", e)
    }
  }

  /**
   * One stream of reports a peer can provoke, emitting at most one per [[ABUSE_REPORT_INTERVAL_NS]]
   * and counting the rest.
   */
  private final class BoundedReporter {

    /** The monotonic instant from which the next report may be emitted. */
    private val nextReportNs = new AtomicLong(clock.nanoTime())

    private val withheld = new AtomicLong(0L)

    /**
     * Answers whether this report may be emitted, and if so how many of its predecessors were
     * withheld since the last one that was.
     *
     * @return the withheld count when the report may be emitted, or empty when it may not
     */
    def claim(): Option[Long] = {
      val now = clock.nanoTime()
      val deadline = nextReportNs.get()
      if (now - deadline >= 0L &&
        nextReportNs.compareAndSet(deadline, now + ABUSE_REPORT_INTERVAL_NS)) {
        Some(withheld.getAndSet(0L))
      } else {
        withheld.incrementAndGet()
        None
      }
    }
  }

  /**
   * Emits one warning through the abuse window, or accounts it as suppressed.
   *
   * @param entry the report, evaluated only when it is going to be emitted
   * @param cause the failure to attach, or `null` when the report is not about one
   */
  private def reportAbuse(entry: => MessageWithContext, cause: Throwable): Unit = {
    abuseReporter.claim().foreach { withheld =>
      val message = annotate(entry, withheld)
      if (cause == null) logWarning(message) else logWarning(message, cause)
    }
  }

  /** Emits one routine notice through the notice window, or accounts it as suppressed. */
  private def reportNotice(entry: => MessageWithContext): Unit = {
    noticeReporter.claim().foreach(withheld => logInfo(annotate(entry, withheld)))
  }

  /** Appends the withheld count to a report, when there is one to state. */
  private def annotate(entry: MessageWithContext, withheld: Long): MessageWithContext = {
    if (withheld == 0L) {
      entry
    } else {
      entry + log". ${MDC(VALUE, withheld)} similar report(s) were suppressed since the last one"
    }
  }

  /**
   * Charges one frame failure to a channel and answers whether it has now exhausted its allowance.
   *
   * @param client the channel the failure occurred on
   * @return `true` on the single failure that exhausts the channel's allowance
   */
  private def chargeChannelFailure(client: TransportClient): Boolean = {
    malformedRegistryLock.synchronized {
      val key = channelKeyOf(client)
      val existing = malformedByChannel.get(key)
      if (existing != null) {
        existing.incrementAndGet() == limits.maxMalformedFramesPerChannel.toLong
      } else if (malformedByChannel.size() >= limits.maxTrackedMalformedChannels) {
        true
      } else {
        val counter = new AtomicLong(1L)
        malformedByChannel.put(key, counter)
        limits.maxMalformedFramesPerChannel == 1
      }
    }
  }

  /** Drops a channel's failure allowance, so the ledger cannot outlive the channels it counts. */
  private def forgetChannelFailures(client: TransportClient): Unit = {
    malformedRegistryLock.synchronized(malformedByChannel.remove(channelKeyOf(client)))
  }

  /**
   * The same, reachable from a channel alone.
   *
   * @param channel the channel whose failure tally is being forgotten
   */
  private def forgetChannelFailures(channel: Channel): Unit = {
    malformedByChannel.remove(channel.id().asLongText())
  }

  /** Runs one callback body so that no failure escapes into the event loop. */
  private def guard(operation: String)(body: => Any): Unit = {
    try {
      body
    } catch {
      case NonFatal(e) =>
        guardFailures.incrementAndGet()
        guardReporter.claim() match {
          case Some(withheld) =>
            logWarning(annotate(log"Streaming shuffle listener failed to " +
              log"${MDC(DESCRIPTION, operation)}; the listener stays up and the affected " +
              log"consumer's own liveness timer recovers " +
              log"(${MDC(NUM_FAILURES, guardFailures.get())} guarded failure(s) so far)",
              withheld), e)
          case None =>
            if (debugEnabled) {
              logDebug(log"Streaming shuffle listener failed to " +
                log"${MDC(DESCRIPTION, operation)}", e)
            }
        }
    }
  }

  /** Callback bodies this listener has contained, whatever the reason. */
  def guardFailureCount: Long = guardFailures.get()
}

/**
 * Construction of the executor's single streaming listener, and the identity its router keys on.
 */
private[spark] object StreamingShuffleListener extends Logging {

  /** Bounds on state whose keys arrive from remote channels. */
  private[streaming] final case class Limits(
      maxMalformedFramesPerChannel: Int,
      maxTrackedMalformedChannels: Int,
      maxParticipantChannels: Int,
      maxParticipantChannelsPerPeer: Int,
      maxParticipantRoutes: Int,
      maxParticipantRoutesPerPeer: Int,
      maxParticipantRoutesPerChannel: Int) {
    require(maxMalformedFramesPerChannel > 0,
      "The malformed-frame allowance per channel must be positive.")
    require(maxTrackedMalformedChannels > 0,
      "The malformed-channel tracking ceiling must be positive.")
    require(maxParticipantChannels > 0,
      "The participant-channel ceiling must be positive.")
    require(maxParticipantChannelsPerPeer > 0 &&
        maxParticipantChannelsPerPeer <= maxParticipantChannels,
      "The per-peer participant-channel ceiling must be positive and no larger than the " +
        "executor-wide channel ceiling.")
    require(maxParticipantRoutes > 0, "The participant-route ceiling must be positive.")
    require(maxParticipantRoutesPerPeer > 0 &&
        maxParticipantRoutesPerPeer <= maxParticipantRoutes,
      "The per-peer participant-route ceiling must be positive and no larger than the " +
        "executor-wide route ceiling.")
    require(maxParticipantRoutesPerChannel > 0 &&
        maxParticipantRoutesPerChannel <= maxParticipantRoutesPerPeer,
      "The per-channel participant-route ceiling must be positive and no larger than the " +
        "per-peer route ceiling.")
  }

  /** Bind to an ephemeral port: the chosen port is published through the coordinator. */
  private val EPHEMERAL_PORT = 0

  /**
   * How often the executor-scoped upkeep sweep runs, in milliseconds. One second is well inside
   * both the producer-liveness bound and the heartbeat interval that refreshes it, so the sweep is
   * never the reason a bound is missed. A live producer's writer runs the same duties at its own
   * 100 ms maintenance cadence, so a consumer is served the same way before and after that task has
   * ended.
   */
  val MAINTENANCE_INTERVAL_MS: Long = 1000L

  /**
   * The bound, in milliseconds, within which the upkeep sweep must be gone once it has been asked
   * to stop.
   */
  val MAINTENANCE_SHUTDOWN_TIMEOUT_MS: Long = 5L * MAINTENANCE_INTERVAL_MS

  /** Shortest interval between two abuse reports from one listener, in nanoseconds. */
  val ABUSE_REPORT_INTERVAL_NS: Long = TimeUnit.MINUTES.toNanos(1L)

  /** Frame failures one channel may cause before this listener closes it. */
  val MAX_MALFORMED_FRAMES_PER_CHANNEL: Int = 64

  /** Channels whose failure counts are tracked at once. */
  val MAX_TRACKED_MALFORMED_CHANNELS: Int = 4096

  val MAX_PARTICIPANT_CHANNELS: Int = 4096

  /** Routing channels allowed from one authenticated principal and remote host. */
  val MAX_PARTICIPANT_CHANNELS_PER_PEER: Int = 64

  /** Producer routes held across all consumer channels on one executor. */
  val MAX_PARTICIPANT_ROUTES: Int = 65536

  val MAX_PARTICIPANT_ROUTES_PER_PEER: Int = 8192

  /** Producer routes one multiplexed channel may name before it must reconnect or be refused. */
  val MAX_PARTICIPANT_ROUTES_PER_CHANNEL: Int = 4096

  private[streaming] val DefaultLimits: Limits = Limits(
    maxMalformedFramesPerChannel = MAX_MALFORMED_FRAMES_PER_CHANNEL,
    maxTrackedMalformedChannels = MAX_TRACKED_MALFORMED_CHANNELS,
    maxParticipantChannels = MAX_PARTICIPANT_CHANNELS,
    maxParticipantChannelsPerPeer = MAX_PARTICIPANT_CHANNELS_PER_PEER,
    maxParticipantRoutes = MAX_PARTICIPANT_ROUTES,
    maxParticipantRoutesPerPeer = MAX_PARTICIPANT_ROUTES_PER_PEER,
    maxParticipantRoutesPerChannel = MAX_PARTICIPANT_ROUTES_PER_CHANNEL)

  /**
   * A streaming listener's bound transport, with direct ownership of both Netty event-loop groups.
   */
  private[streaming] final class BoundServer(
      transportContext: TransportContext,
      transportConf: TransportConf,
      listener: RpcHandler,
      bootstraps: java.util.List[TransportServerBootstrap])
    extends AutoCloseable with Logging {

    require(transportContext != null, "The streaming transport context must not be null.")
    require(transportConf != null, "The streaming transport configuration must not be null.")
    require(listener != null, "The streaming listener must not be null.")
    require(bootstraps != null, "The streaming server bootstraps must not be null.")

    private val closed = new AtomicBoolean(false)
    private val ioMode = IOMode.valueOf(transportConf.ioMode())
    private val allocator: PooledByteBufAllocator = {
      if (transportConf.sharedByteBufAllocators()) {
        NettyUtils.getSharedPooledByteBufAllocator(
          transportConf.preferDirectBufsForSharedByteBufAllocators(), true)
      } else {
        NettyUtils.createPooledByteBufAllocator(
          transportConf.preferDirectBufs(), true, transportConf.serverThreads())
      }
    }
    private val bossGroup: EventLoopGroup =
      NettyUtils.createEventLoop(ioMode, 1, transportConf.getModuleName() + "-boss")
    private val workerGroup: EventLoopGroup =
      NettyUtils.createEventLoop(
        ioMode, transportConf.serverThreads(), transportConf.getModuleName() + "-server")
    private val bootstrap = new ServerBootstrap()
    @volatile private var channelFuture: ChannelFuture = null
    @volatile private var boundPort: Int = -1

    bind()

    /** The ephemeral port published through the streaming coordinator. */
    def getPort: Int = {
      if (boundPort < 0) {
        throw new IllegalStateException("The streaming transport server is not initialized.")
      }
      boundPort
    }

    /** Whether both event-loop groups have completed termination. */
    def isTerminated: Boolean = bossGroup.isTerminated && workerGroup.isTerminated

    /** Closes the listening channel and awaits both owned event-loop termination futures. */
    override def close(): Unit = {
      if (closed.compareAndSet(false, true)) {
        val future = channelFuture
        channelFuture = null
        if (future != null) {
          future.channel().close().awaitUninterruptibly(
            TRANSPORT_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        }
        val bossTermination = bossGroup.shutdownGracefully()
        val workerTermination = workerGroup.shutdownGracefully()
        val deadlineNanos =
          System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TRANSPORT_SHUTDOWN_TIMEOUT_MS)
        val bossStopped = awaitTermination(bossTermination, deadlineNanos)
        val workerStopped = awaitTermination(workerTermination, deadlineNanos)
        if (!bossStopped || !workerStopped) {
          logWarning(log"Streaming shuffle listener transport did not release both event-loop " +
            log"groups within ${MDC(TIMEOUT, TRANSPORT_SHUTDOWN_TIMEOUT_MS)} ms; boss " +
            log"terminated=${MDC(STATUS, bossStopped)}, worker terminated=" +
            log"${MDC(VALUE, workerStopped)}")
        }
      }
    }

    private def bind(): Unit = {
      try {
        configureBootstrap()
        channelFuture = bootstrap.bind(new InetSocketAddress(EPHEMERAL_PORT))
        channelFuture.syncUninterruptibly()
        boundPort = channelFuture.channel().localAddress()
          .asInstanceOf[InetSocketAddress].getPort
      } catch {
        case NonFatal(e) =>
          close()
          throw e
      }
    }

    private def configureBootstrap(): Unit = {
      val osName = System.getProperty("os.name")
      val reuseAddress = osName == null ||
        !osName.regionMatches(true, 0, "Windows", 0, "Windows".length)
      bootstrap
        .group(bossGroup, workerGroup)
        .channel(NettyUtils.getServerChannelClass(ioMode))
        .option(ChannelOption.ALLOCATOR, allocator)
        .option(ChannelOption.SO_REUSEADDR, Boolean.box(reuseAddress))
        .childOption(ChannelOption.ALLOCATOR, allocator)
      if (transportConf.backLog() > 0) {
        bootstrap.option(ChannelOption.SO_BACKLOG, Int.box(transportConf.backLog()))
      }
      if (transportConf.receiveBuf() > 0) {
        bootstrap.childOption(ChannelOption.SO_RCVBUF, Int.box(transportConf.receiveBuf()))
      }
      if (transportConf.sendBuf() > 0) {
        bootstrap.childOption(ChannelOption.SO_SNDBUF, Int.box(transportConf.sendBuf()))
      }
      if (transportConf.enableTcpKeepAlive()) {
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, Boolean.box(true))
      }
      bootstrap.childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(channel: SocketChannel): Unit = {
          var rpcHandler = listener
          bootstraps.asScala.foreach { serverBootstrap =>
            rpcHandler = serverBootstrap.doBootstrap(channel, rpcHandler)
          }
          transportContext.initializePipeline(channel, rpcHandler, false)
        }
      })
    }

    private def awaitTermination(
        future: io.netty.util.concurrent.Future[_],
        deadlineNanos: Long): Boolean = {
      val remaining = math.max(0L, deadlineNanos - System.nanoTime())
      future.awaitUninterruptibly(remaining, TimeUnit.NANOSECONDS)
    }
  }

  /** Deadline shared by the listener channel and both owned event-loop groups. */
  private val TRANSPORT_SHUTDOWN_TIMEOUT_MS: Long = 10000L

  /**
   * Binds the executor's one streaming shuffle listener.
   *
   * @param conf the executor's configuration
   * @return the router, the transport context that owns the pipeline, and the bound server
   */
  def bind(conf: SparkConf): (StreamingShuffleListener, TransportContext, BoundServer) = {
    val listener = new StreamingShuffleListener(conf)
    val security =
      Option(SparkEnv.get).map(_.securityManager).getOrElse(new SecurityManager(conf))
    val transportConf =
      StreamingShuffleServerHandler.streamingTransportConf(conf, security = Some(security))
    val transportContext = new TransportContext(transportConf, listener)
    val server = new BoundServer(transportContext, transportConf, listener,
      StreamingShuffleServerHandler.streamingServerBootstraps(transportConf, Some(security)))
    logInfo(log"Streaming shuffle listener bound one port for this executor: " +
      log"${MDC(PORT, server.getPort)}")
    (listener, transportContext, server)
  }

  /** The identity of one producer, which is what a frame is routed on. */
  private[streaming] case class ProducerKey(shuffleId: Int, mapId: Long) {
    override def toString: String = s"shuffle $shuffleId map $mapId"
  }
}
