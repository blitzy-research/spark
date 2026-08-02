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
import java.util.Comparator
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap, PriorityBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener}

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CONFIG, COUNT, DURATION, ERROR, HOST_PORT, MAX_ATTEMPTS, MAX_SIZE, NUM_BLOCKS, NUM_BYTES, PARTITION_ID, REASON, SESSION_ID, SHUFFLE_ID, STATUS, TIMEOUT, VALUE}
import org.apache.spark.internal.config.SHUFFLE_STREAMING_DEBUG
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient, TransportClientBootstrap}
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.protocol.OneWayMessage
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager, TransportServerBootstrap}
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, DataBlockMessage, HeartbeatMessage, RetransmitRequestMessage, StreamingShuffleMessage, StreamingShuffleMessageType, StreamTerminationMessage}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.util.{Clock, SystemClock}

/**
 * The producer side channel handler of the streaming shuffle.
 *
 * This handler owns egress for one map output: it takes the blocks a `StreamingShuffleWriter` has
 * admitted to the retained store, orders them, charges them against the executor's egress budget
 * and puts them on the wire for every consumer that has subscribed; and it consumes the control
 * traffic the reduce side sends back -- acknowledgements, heartbeats and retransmission requests --
 * turning acknowledgements into reclaimed producer memory.
 *
 * Together with `StreamingShuffleClientHandler` it is the only place in Spark where channel level
 * flow control idioms live. That containment is deliberate: neither this file nor its consumer side
 * counterpart requires a single edit to the shared transport, so `TransportContext`,
 * `TransportConf` and the shared Netty pipeline are consumed exactly as they stand. The division
 * between the two handlers is equally deliberate -- toggling `Channel.setAutoRead` to exert TCP
 * level backpressure belongs to the consumer side handler alone, and appears nowhere in this file.
 *
 * =Three layers of backpressure=
 *
 * The streaming shuffle applies three, and this handler participates in exactly two of them:
 *
 *  - application level credit derived from consumer acknowledgements, whose ledger is owned by
 *    `BackpressureProtocol`. This handler reports the events that ledger is built from and never
 *    duplicates it;
 *  - rate limiting, owned by [[TokenBucketRateLimiter]]: every data block is charged against the
 *    bucket before it is written, and a refusal means the block is held and retried rather than
 *    dropped. The charge is taken through the protocol's admission call, which holds the same
 *    bucket, so a block's bytes are debited once. That refusal is also the signal the memory spill
 *    manager reacts to, which is why it is surfaced through [[throttleCount]] rather than
 *    swallowed;
 *  - TCP level throttling by way of channel auto-read, which belongs to the consumer side handler.
 *
 * =Thread model=
 *
 * `ShuffleWriteMetricsReporter` documents that all of its methods are called on a single thread and
 * that implementations therefore need not synchronize. A Netty event-loop thread consequently must
 * never touch a reporter, so this handler calls none of them. It instead publishes plain counters
 * -- [[bytesWrittenToChannel]], [[blocksWrittenToChannel]], [[writeTimeNanos]] and the rest -- that
 * the writer reads on the task thread and forwards to the reporter there.
 *
 * The other rules the same model imposes are honoured throughout: no method here parks, blocks or
 * sleeps; every piece of shared state is an atomic or a concurrent collection, and no lock is ever
 * held across a channel write; no exception is allowed to escape a Netty callback; and every
 * failure observed on an event-loop thread is handed to [[StreamingShuffleErrorNotifier]] so that
 * the task thread re-throws it. Without that bridge a failure raised on an I/O thread is swallowed
 * and the producing task waits forever on progress that will never come.
 *
 * =Timing=
 *
 * Every instant this handler needs comes from the injected `Clock`, and no wall clock is read
 * directly. That is what makes the protocol's timing observable and testable without a sleep: a
 * suite advances a manual clock and asserts on [[stalledPartitions]] or on retransmission backoff
 * rather than waiting for real time to pass. It is also why this handler starts no timer thread of
 * its own: liveness is evaluated when it is asked for, on the thread that asks.
 *
 * =Configuration=
 *
 * Configuration is read once, here, and held immutably, which is what makes "streaming shuffle
 * configuration changes require an executor restart" true by construction rather than by
 * convention. The upstream Java message family in `org.apache.spark.network.shuffle.protocol
 * .streaming` is deliberately free of any Spark core coupling -- it reads no configuration, emits
 * no log line and reads no clock -- so this package owns all four of those concerns.
 *
 * =One handler, many consumers=
 *
 * A map output is read by every reduce task that wants one of its partitions, and each of those
 * consumers opens its own connection. This handler therefore serves many channels at once, and it
 * keeps one [[StreamingShuffleServerHandler.ConsumerSession]] per channel: an authenticated
 * identity, the set of partitions that consumer has asked for, its own egress queue, and its own
 * acknowledgement cursors. Nothing is ever written to a channel that did not ask for the partition
 * in question, and a block is retained until *every* subscribed consumer has acknowledged it, which
 * is what the retained store's minimum-across-consumers retirement rule enforces.
 *
 * Subscription needs no message type of its own. A consumer announces the partition it wants with
 * the same heartbeat that proves it is alive, and that heartbeat carries the position it has
 * reached -- so the first heartbeat of a fresh consumer subscribes it at the beginning of the
 * stream, and the first heartbeat of a reconnecting one subscribes it exactly where it left off.
 * Resumption is therefore the ordinary case of subscription rather than a separate protocol.
 *
 * =Where the bytes live=
 *
 * This handler owns no payload. Every block it sends is read, at the moment it is framed, from the
 * [[MemorySpillManager]] that already charged those bytes against the executor's buffer budget and
 * that will spill them to disk under pressure -- reached through the executor-scoped
 * [[StreamingShuffleBlockResolver]], which also refuses to hand over the store of a superseded
 * generation. Holding a second copy here would double the memory a bounded budget is supposed to
 * bound, and would make a replay servable from memory but not from spill. Because the lookup goes
 * through the retained store, a retransmission is answered identically whether the block is still
 * in memory or has been evicted.
 *
 * This handler is a `RpcHandler` rather than a Netty channel handler, which is what lets it be
 * installed by `TransportContext` and therefore inherit the transport's authentication, its
 * optional SSL and its keep-alive without a single edit to any shared transport class. Every
 * streaming frame travels as the body of a one-way RPC.
 *
 * @param conf the executor's configuration, read exactly once during construction
 * @param shuffleId the shuffle whose partitions this handler streams
 * @param mapId the map output whose partitions this handler streams
 * @param taskAttemptId the producing generation, which is what keeps a superseded attempt's
 *                      accounting separate from that of the attempt which replaced it
 * @param blockResolver the executor-scoped registry through which this handler reaches the retained
 *                      output of the map task it serves. Retained output outlives the producing
 *                      task, so the store is found through the resolver rather than held directly,
 *                      and the resolver's generation check is what stops a superseded attempt from
 *                      serving the bytes of the attempt that replaced it
 * @param backpressure the protocol that owns the credit ledger, the acknowledgement and heartbeat
 *                     timeouts, cross shuffle utilisation aggregation and the backpressure event
 *                     counter. This handler reports to it and consumes its pacing decisions; it
 *                     never reimplements any part of it
 * @param rateLimiter the egress bucket this shuffle's share of the link capacity is charged against
 * @param errorNotifier the bridge that carries a failure observed on an event-loop thread across to
 *                      the task thread
 * @param clock time source for heartbeat stamps, liveness evaluation, reclamation budgeting and
 *              retransmission backoff, injected so that behaviour is deterministic under test
 */
private[spark] class StreamingShuffleServerHandler(
    conf: SparkConf,
    val shuffleId: Int,
    val mapId: Long,
    val taskAttemptId: Long,
    blockResolver: StreamingShuffleBlockResolver,
    backpressure: BackpressureProtocol,
    rateLimiter: TokenBucketRateLimiter,
    errorNotifier: StreamingShuffleErrorNotifier,
    clock: Clock = new SystemClock)
  extends RpcHandler with Logging {

  import StreamingShuffleServerHandler._

  require(mapId >= 0L, s"The map id must be non-negative but was $mapId.")
  require(taskAttemptId >= 0L,
    s"The producing task attempt id must be non-negative but was $taskAttemptId.")

  /**
   * The ledger identity of one partition of this map output, as seen from the sending end.
   *
   * The producer generation is part of the identity because two attempts of one map task -- a
   * speculative copy, or a retry after a failure -- produce the same shuffle and the same
   * partitions and are nevertheless two separate flows with two separate windows of unacknowledged
   * bytes. The role distinguishes this sending ledger from the receiving ledger of a consumer that
   * happens to be running in the same JVM, which is always the case under `local[*]`.
   *
   * The consumer is deliberately '''not''' part of the identity, even though this handler may serve
   * several. What this ledger measures is one producer's partition flow -- the credit, pacing and
   * reclamation window the rate limiter and the spill decision are driven by -- which is a
   * property of the sending side and is the same however many consumers are attached. What is
   * genuinely per consumer is the release of retained bytes, and that is keyed per consumer by the
   * retained store, which retires only to the minimum position across every consumer registered for
   * a partition. Splitting the ledger per consumer would duplicate the pacing state without making
   * any release safer, since no ledger releases anything.
   */
  private def producerKey(partitionId: Int): BackpressureStreamKey =
    BackpressureStreamKey.forProducer(shuffleId, mapId, taskAttemptId, partitionId)

  /**
   * The value of spark.shuffle.streaming.debug, read once and held immutably.
   *
   * It is the sole authority over whether this handler emits per message diagnostics. Logging one
   * line per block would defeat the subsystem's log budget of under ten megabytes an hour for each
   * executor on its own, so per message lines are gated on this flag while lifecycle events --
   * channel activation and loss, stream termination, escalation -- are always reported.
   */
  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  /**
   * The transport configuration for the streaming shuffle's own module.
   *
   * Asking `SparkTransportConf` for a configuration under the module name "shuffle-streaming"
   * yields an independent `spark.shuffle-streaming.io.*` namespace, so thread counts, buffer sizes,
   * retry behaviour and TCP keep-alive can all be tuned for streaming without perturbing the block
   * transfer service that every other shuffle depends on. The component that builds the
   * `TransportContext` for streaming reads this value, so that producer and consumer are configured
   * from one place rather than from two that can drift apart.
   */
  val transportConf: TransportConf = streamingTransportConf(conf)

  /**
   * Per partition egress and acknowledgement state, created on first use.
   *
   * A handler serves every reduce partition this map task produces for one shuffle, so the state is
   * keyed by partition rather than held as fields.
   */
  private val streams = new ConcurrentHashMap[Int, PartitionStream]()

  /**
   * One session per consumer channel, keyed by the channel's own identity.
   *
   * Keying by channel rather than by consumer identity is what makes a reconnection safe: the new
   * channel is a new session, and the old one is torn down independently by its own inactivity
   * callback, so a late teardown of the lost connection can never dispossess the live one. The
   * consumer identity is carried inside the session and is what the retained store's per-consumer
   * cursors are keyed by, so a reconnecting consumer resumes against its own acknowledged position
   * rather than starting again.
   */
  private val sessions = new ConcurrentHashMap[String, ConsumerSession]()

  /** Monotonic enqueue ticket, which makes the ordering stable for equally urgent blocks. */
  private val egressTicket = new AtomicLong(0L)

  /**
   * One-shot close transition, so teardown happens exactly once however many threads reach it.
   *
   * The producing task's completion listener, an unsuccessful stop and a channel failure can all
   * arrive concurrently, and every one of them wants everything released. Latching the transition
   * here means the release path runs once, and everything that could schedule further work consults
   * this flag first, so nothing is queued after the queues have been emptied.
   */
  private val closed = new AtomicBoolean(false)

  /**
   * The retained output of the map task this handler serves, or `None` when it is not servable.
   *
   * Resolved on every use rather than captured once, because the store is registered by the
   * producing task and is deliberately outlived by nothing: after the shuffle is unregistered, or
   * once a newer attempt has taken the registration over, this returns `None` and the handler stops
   * being able to send -- which is exactly the intended behaviour, since the bytes it would send no
   * longer belong to it. The generation check is what distinguishes the two cases from a
   * registration that simply has not happened yet.
   */
  private def retainedOutput: Option[MemorySpillManager] = {
    if (blockResolver.registeredGeneration(shuffleId, mapId).contains(taskAttemptId)) {
      blockResolver.producerFor(shuffleId, mapId)
    } else {
      None
    }
  }

  /**
   * The producing task's attributes, which decide flush order between blocks that are otherwise
   * equally ready. Accepted through [[registerTaskAttempt]] rather than by reading
   * `TaskContext.get()`, because that method is meaningless on an event-loop thread.
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

  // Partitions whose end of stream has been requested. Read on the egress hot path to decide
  // whether the deferral scan below has anything to look for: that scan visits every stream this
  // handler owns, so running it after each drain pass while the task is still producing would make
  // the cost of writing one block a function of the shuffle's partition count. Terminations are
  // requested only as a map task finishes, so the counter is zero for the whole of the write and
  // the scan costs nothing until it can do something.
  private val terminationRequests = new AtomicInteger(0)

  /**
   * Reports an asynchronous write failure to the notifier.
   *
   * A channel write completes on an event-loop thread long after the call that issued it returned,
   * so a failure that is not observed through the future is lost outright: the producing task would
   * believe its bytes reached the consumer. One listener instance is allocated for the handler's
   * lifetime rather than one per write, because a write happens per block.
   */
  private val writeFailureListener: ChannelFutureListener = new ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = {
      if (!future.isSuccess) {
        errorNotifier.setError(future.cause())
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} failed to write to the " +
          log"consumer channel", future.cause())
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

  // ==========================================================================================
  // Producing task identity, which decides flush order
  // ==========================================================================================

  /**
   * Records the attributes of the task attempt whose output this handler streams.
   *
   * The writer calls this once, from the task thread, with values taken from its `TaskContext`.
   * Accepting them rather than reading `TaskContext.get()` here is required, not merely tidy: the
   * drain loop can run on a Netty event-loop thread, where no task context exists at all.
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

  // ==========================================================================================
  // Egress: framing, checksumming and enqueueing
  // ==========================================================================================

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
   * The block's bytes are not passed to this call at all, and that is the point. By the time the
   * writer reaches here it has already admitted these exact bytes to the retained store under this
   * exact sequence number, so the store is where they live and this call needs nothing but their
   * size in order to pace them. Accepting an array would invite a second copy of every block in
   * flight, which would double the memory the buffer budget is supposed to bound and would make a
   * block replayable while it sat in memory but not once it had been evicted. The bytes are read
   * back, and the CRC32C computed over them, at the moment the block is framed for a particular
   * consumer -- by `DataBlockMessage.withComputedChecksum`, which routes the arithmetic through
   * `StreamingShuffleChecksum` so that producer and consumer are provably running the same
   * computation, and which binds the value to the shuffle, partition and sequence number rather
   * than covering the bytes in isolation.
   *
   * The block is charged against the executor's egress budget only when it is written, never here:
   * enqueueing is free, so a writer is never refused the chance to hand over bytes it has already
   * produced. Pacing decides when those bytes leave, not whether they may be offered.
   *
   * A block is fanned out to every session that has subscribed to its partition, and to no other.
   * A partition with no subscriber yet queues nothing at all: the bytes are retained by the store
   * regardless, and a consumer that subscribes later is served from its own acknowledged position,
   * so nothing is lost by not having speculated about who would ask.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param payloadBytes the block's payload size, at most [[MAX_PAYLOAD_BYTES]]
   * @param priority the attributes of the attempt that produced the block
   * @return the sequence number assigned to the block
   * @throws IllegalArgumentException if the size is negative or larger than the block cap
   * @throws IllegalStateException if the partition's stream has already been terminated, or if the
   *                               retained store does not hold the block under the assigned
   *                               sequence number
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
    val sequenceNumber = stream.nextSequenceNumber.getAndIncrement()
    // Asserted, not assumed. The writer admits a block to the retained store before offering it
    // here, so an absence at this point means the two have disagreed about identity or ordering --
    // the signature of a handler shared between two producers of the same map output -- and sending
    // bytes read under a sequence number nobody admitted would corrupt the consumer's stream.
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
    stream.blocksEnqueued.incrementAndGet()
    stream.highestOffered.set(sequenceNumber)
    // Queued volume is accounted per session, and aggregated for the writer by summing over them.
    // The protocol's ledger records a block when it is admitted for egress, not when it is queued.
    val subscribed = fanOut(partitionId, sequenceNumber, framedBytes, priority, replay = false)
    if (debugEnabled) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} enqueued block ${MDC(COUNT, sequenceNumber)} of " +
        log"${MDC(NUM_BYTES, framedBytes)} framed byte(s) for ${MDC(VALUE, subscribed)} " +
        log"subscribed consumer(s)")
    }
    val written = drain()
    if (debugEnabled && written == 0L) {
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
        if (session.subscribedTo(partitionId) && session.offer(
            PendingBlock(partitionId, sequenceNumber, framedBytes, priority,
              egressTicket.getAndIncrement(), replay))) {
          queued += 1
        }
      }
      queued
    }
  }

  /**
   * Framed size of a block carrying a payload of the given length.
   *
   * The overhead is read from the message type rather than restated, which is what keeps this
   * handler's accounting equal to the number of bytes that actually leave the socket. It is derived
   * arithmetically instead of by framing a message, so a block's cost is known before its bytes
   * have been read back out of the retained store.
   */
  private def framedLengthOf(payloadBytes: Int): Int = {
    payloadBytes + DataBlockMessage.FRAMING_OVERHEAD_BYTES
  }

  // ==========================================================================================
  // Egress: draining onto the channel
  // ==========================================================================================

  /**
   * Writes as much of the queued output as pacing and the socket presently allow.
   *
   * Safe to call from the task thread and from a Netty event-loop thread alike; `Channel.write`
   * itself is thread safe, and the drain guard means only one thread is writing at any moment. It
   * never parks: when the bucket refuses a block or the socket's outbound buffer is full, the block
   * stays queued and the method returns. Holding rather than dropping is the whole of the response
   * to a refusal, and it is also the signal the spill manager reacts to.
   *
   * @return the number of framed bytes handed to the channel by this call
   */
  def flushPending(): Long = drain()

  /**
   * Drains every session, in the flush order the producing attempts' priorities imply.
   *
   * Sessions are drained independently, and that independence is the point: one consumer whose
   * socket is full or whose credit is exhausted must not hold up another consumer that is keeping
   * up, which a single shared queue could not avoid. Each session holds its own guard, so several
   * threads may drain different sessions at once while never writing twice to one channel.
   */
  private def drain(): Long = {
    if (closed.get()) {
      0L
    } else {
      var total = 0L
      val ordered = sessions.values().asScala.toSeq.sortBy(_.orderingKey)
      ordered.foreach(session => total += drainSession(session))
      total
    }
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
        // note while the guard was held, so a block offered during the pass is not stranded. The
        // progress condition is what stops a throttled or unwritable channel from spinning: a pass
        // that wrote nothing runs at most once more, and the note is cleared at the top of it.
        again = !session.queue.isEmpty && (written > 0L || session.drainWakeup.get())
      } else {
        session.drainWakeup.set(true)
      }
    }
    total
  }

  /**
   * One pass of one session's drain loop, executed by the single thread holding its guard.
   *
   * A block is removed from the queue before it is charged, and returned to the queue unchanged if
   * the charge is refused. Returning it is order preserving, because a pending block's position is
   * decided by its priority and its monotonic ticket, neither of which the round trip alters.
   *
   * A refusal by the rate limiter schedules a wake-up at the bucket's own next refill instant, so
   * the block leaves as soon as pacing permits instead of waiting for another event to happen to
   * trigger a drain. Without that wake-up a final rate-limited block could sit queued until the
   * producing task shut down and discarded it, which is a silent loss of output rather than the
   * throttle it is meant to be.
   */
  private def drainOnce(session: ConsumerSession): Long = {
    val channel = session.channel
    if (session.isClosed || !channel.isActive()) {
      0L
    } else {
      val startedAtNanos = clock.nanoTime()
      var written = 0L
      var flushNeeded = false
      var keepGoing = true
      var throttled = false
      while (keepGoing) {
        if (!channel.isWritable()) {
          // The socket's outbound buffer is full. Leaving the block queued is correct: writability
          // is signalled on this very channel, and the writability callback resumes the drain.
          keepGoing = false
        } else {
          val pending = session.queue.poll()
          if (pending == null) {
            keepGoing = false
          } else if (!admitForEgress(pending)) {
            session.queue.offer(pending)
            throttles.incrementAndGet()
            throttled = true
            if (debugEnabled) {
              logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
                log"${MDC(PARTITION_ID, pending.partitionId)} is throttled holding " +
                log"${MDC(NUM_BYTES, pending.framedBytes)} framed byte(s); " +
                log"${MDC(VALUE, rateLimiter.availableTokens)} token(s) available")
            }
            keepGoing = false
          } else if (writeBlock(session, pending)) {
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
      if (throttled) {
        scheduleRefillDrain(session)
      }
      writeNanos.addAndGet(math.max(0L, clock.nanoTime() - startedAtNanos))
      written
    }
  }

  /**
   * Arranges for one more drain attempt once the egress bucket has refilled.
   *
   * The delay comes from the bucket itself, so the wake-up lands when tokens are actually available
   * rather than at an interval this handler guessed. It is scheduled on the session's own event
   * loop, which needs no thread of this handler's own and serialises naturally with every other
   * callback on that channel. One outstanding wake-up per session is enough, because a drain that
   * is still throttled schedules the next one before it returns.
   */
  private def scheduleRefillDrain(session: ConsumerSession): Unit = {
    if (!closed.get() && !session.isClosed &&
        session.refillScheduled.compareAndSet(false, true)) {
      val delayMs = math.max(1L,
        math.min(rateLimiter.millisUntilAvailable(session.headFramedBytes), MAX_REFILL_WAIT_MS))
      try {
        session.channel.eventLoop().schedule(new Runnable {
          override def run(): Unit = {
            session.refillScheduled.set(false)
            guard(drainSession(session))
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
   * The bytes are read at this moment rather than held since production, so a block that has been
   * evicted to disk is framed from its spill segment and one still in memory is framed from memory,
   * with no difference visible on the wire. A block the store no longer retains cannot be framed at
   * all: that means every subscribed consumer had already acknowledged it, so the write is dropped
   * and counted rather than failed, because there is no consumer left that needs it.
   *
   * <b>Why the framing copy is not charged to the buffer budget.</b> Building the message and
   * encoding it allocates one transient copy of the payload, and that copy is deliberately
   * outside the executor-wide quota, because it is not retained: it is handed to the channel and
   * released as the socket drains it. What bounds it is the transport's own outbound accounting --
   * the caller writes only while `Channel.isWritable` holds, so the number of framing copies in
   * flight at once is capped by Netty's write water marks rather than being unbounded. The quota's
   * job is the different one of bounding what is *held*, and every held byte is in the retained
   * store, exactly once, charged before it was admitted. A framing copy that was charged as well
   * would double-count the same block and shrink the real streaming window to half of what the
   * operator configured.
   *
   * @return true if a frame was handed to the channel
   */
  private def writeBlock(session: ConsumerSession, pending: PendingBlock): Boolean = {
    val partitionId = pending.partitionId
    val sequenceNumber = pending.sequenceNumber
    val payload = retainedOutput.flatMap(_.retainedPayload(partitionId, sequenceNumber))
    session.releasePending(pending)
    payload match {
      case Some(bytes) =>
        val block = DataBlockMessage.withComputedChecksum(
          shuffleId, mapId, partitionId, sequenceNumber, bytes)
        val framedBytes = pending.framedBytes.toLong
        session.recordSent(partitionId, sequenceNumber, framedBytes)
        session.channel.write(sendable(block)).addListener(writeFailureListener)
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
      case None =>
        unservableBlocks.incrementAndGet()
        if (debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
            log"${MDC(PARTITION_ID, partitionId)} dropped block ${MDC(COUNT, sequenceNumber)} " +
            log"for consumer ${MDC(SESSION_ID, session.consumerId)}: " +
            log"${MDC(REASON, "the retained store no longer holds it")}")
        }
        false
    }
  }

  /**
   * Wraps one message as the body of a one-way RPC, which is how every streaming frame travels.
   *
   * This is precisely what `TransportClient.send` constructs, and it is built here rather than
   * delegated to that method for one reason: `send` flushes on every call, and the two mebibyte
   * block cap only pipelines if consecutive blocks share a syscall. Writing the same message and
   * flushing once per batch keeps the transport's encoding, its optional encryption and its frame
   * accounting exactly as they are, and changes only how often the socket is poked.
   */
  private def sendable(message: StreamingShuffleMessage): OneWayMessage = {
    new OneWayMessage(new NioManagedBuffer(message.toByteBuffer()))
  }

  /**
   * Writes a control frame immediately, bypassing the pacing verdict but not the accounting.
   *
   * Control frames are charged against the bucket and then written whatever the verdict.
   * Withholding a heartbeat because egress is paced would be indistinguishable, at the consumer,
   * from the producer having died, and would trip the very failure detector the heartbeat exists
   * to satisfy: pacing must never be able to manufacture a failure. Debiting the bucket regardless
   * keeps the accounting of bytes on the wire honest, and control frames are tens of bytes, so the
   * surplus a refusal lets through cannot meaningfully perturb the rate.
   */
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

  // ==========================================================================================
  // Liveness and orderly end of stream
  // ==========================================================================================

  /**
   * Emits a producer heartbeat for one partition, stamped from the injected clock.
   *
   * The wire message carries a timestamp and reads no clock of its own, deliberately: the instant
   * it reports is supplied from here, which is what lets a suite drive the five second liveness
   * bound with a manual clock instead of a sleep. The consumer's own detector is satisfied by this
   * frame, not by TCP keep-alive: keep-alive is a boolean with no interval, and the JDK exposes no
   * socket option for one, so the bound is enforced at the application level or not at all.
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
    subscribers.foreach { session =>
      writeControl(session, new HeartbeatMessage(
        shuffleId, mapId, partitionId, stream.nextSequenceNumber.get(), clock.getTimeMillis()))
      sent = true
    }
    sent
  }

  /**
   * Signals the orderly end of one partition's stream.
   *
   * This is what makes the absence of further data mean completion rather than producer loss: with
   * no terminator the consumer cannot distinguish a finished producer from a dead one, and would
   * wait out its five second detector before concluding anything. A partition that produced no
   * records terminates with a total of zero, which the protocol accepts.
   *
   * The terminator must never overtake the data it terminates, so it is deferred until the
   * partition's queued blocks have all been written. When egress is paced or the socket is full the
   * request is remembered and emitted by a later drain; the return value says which happened.
   *
   * The single-partition form of [[terminateStreams]], which is where the work is done.
   *
   * @param partitionId the reduce partition whose stream is complete
   * @return true if the terminator was written by this call, false if it was deferred
   */
  def terminateStream(partitionId: Int): Boolean = terminateStreams(Seq(partitionId)) == 1

  /**
   * Signals the orderly end of several partitions' streams with a single egress pass.
   *
   * This is the form a finishing map task uses, and the reason it exists is cost rather than
   * convenience. Requesting a terminator has to be followed by a drain, because a terminator is
   * deferred until the partition's queued blocks have been written; doing that once per partition
   * would run one drain per partition, and a drain visits every session and every stream, so
   * finishing would cost the square of the partition count in scans. Requesting every terminator
   * first and draining once afterwards produces exactly the same wire output for a cost linear in
   * partitions.
   *
   * @param partitionIds the reduce partitions whose streams are complete
   * @return how many of them had their terminator written by this call rather than deferred
   */
  def terminateStreams(partitionIds: Seq[Int]): Int = {
    partitionIds.foreach { partitionId =>
      val stream = streamFor(partitionId)
      // Counted on the edge, so the hot-path gate sees a request once however often termination is
      // requested for the same partition -- which a retried or repeated finish does.
      if (stream.terminationRequested.compareAndSet(false, true)) {
        terminationRequests.incrementAndGet()
      }
      stream.totalBlocksAtTermination.set(stream.blocksEnqueued.get())
    }
    drain()
    var written = 0
    partitionIds.foreach { partitionId =>
      if (terminationSignalled(partitionId)) {
        written += 1
      }
    }
    written
  }

  /**
   * Whether every consumer subscribed to one partition has been told its stream ended.
   *
   * A partition with no subscriber has told nobody, which is not the same as having finished, so it
   * reports false and the terminator stays pending for a consumer that subscribes later.
   */
  private def terminationSignalled(partitionId: Int): Boolean = {
    val subscribers = sessions.values().asScala.count(_.subscribedTo(partitionId))
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

  /**
   * Emits any terminator whose partition has finished draining, for one consumer.
   *
   * Called at the end of every drain pass, which is the only moment at which a partition can have
   * become empty for that consumer, and returning at once while no terminator has been requested,
   * which is the whole of the write, because the scan below visits every stream this handler owns.
   * Termination is per session because it is per stream: one consumer may have caught up while
   * another is still receiving, and telling the second that the stream has ended before its blocks
   * have been written would make it stop reading early.
   *
   * The one-shot guard is claimed before the write and *released* if the write fails, and the
   * delivery is recorded only in a successful listener. A terminator marked sent on the strength of
   * an enqueue that never reached the socket would leave the consumer waiting out its producer
   * liveness detector for a stream that will never be terminated again.
   */
  private def emitDeferredTerminations(session: ConsumerSession): Unit = {
    if (terminationRequests.get() > 0 && !session.isClosed && session.channel.isActive()) {
      streams.values().asScala.foreach { stream =>
        val partitionId = stream.partitionId
        val ready = stream.terminationRequested.get() &&
          session.subscribedTo(partitionId) &&
          session.pendingBlocksFor(partitionId) <= 0L &&
          session.caughtUpWith(partitionId, stream.highestOffered.get())
        if (ready && session.claimTermination(partitionId)) {
          val totalBlocks = stream.totalBlocksAtTermination.get()
          val future = writeControl(session, new StreamTerminationMessage(
            shuffleId, mapId, partitionId, stream.nextSequenceNumber.get(), totalBlocks))
          future.addListener(new ChannelFutureListener {
            override def operationComplete(completed: ChannelFuture): Unit = {
              if (completed.isSuccess) {
                session.confirmTermination(partitionId)
                // One record per partition per subscribed consumer -- the largest log source this
                // handler has -- so the per-consumer detail is debug only. The task's own summary
                // reports the aggregate at default level once the write completes.
                if (debugEnabled) {
                  logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
                    log"${MDC(PARTITION_ID, partitionId)} streamed " +
                    log"${MDC(NUM_BLOCKS, totalBlocks)} block(s) and signalled end of stream to " +
                    log"consumer ${MDC(SESSION_ID, session.consumerId)}")
                }
              } else {
                // The claim is surrendered so a reconnecting consumer is terminated properly, and
                // the failure travels to the task thread rather than being lost with the write.
                session.releaseTerminationClaim(partitionId)
                errorNotifier.setError(completed.cause())
                logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
                  log"${MDC(PARTITION_ID, partitionId)} could not signal end of stream to " +
                  log"consumer ${MDC(SESSION_ID, session.consumerId)}", completed.cause())
              }
            }
          })
        }
      }
    }
  }

  /** How many subscribed consumers have had the terminator for one partition confirmed. */
  private def terminationsDelivered(partitionId: Int): Int = {
    sessions.values().asScala.count(_.terminationConfirmed(partitionId))
  }

  /**
   * Blocks queued but not yet written for one partition, summed over every session.
   *
   * Read by the writer to decide whether a terminated stream still owes a consumer anything. A
   * stream that has been marked terminated but still has queued blocks must keep being heartbeated,
   * because the consumer waiting for those blocks measures producer liveness on its own timer and
   * would otherwise declare a healthy producer dead while its bytes were still in the queue.
   *
   * @param partitionId the reduce partition to measure
   * @return the number of queued, unwritten blocks across every subscribed consumer
   */
  def pendingBlocksFor(partitionId: Int): Long = {
    var total = 0L
    sessions.values().asScala.foreach(session => total += session.pendingBlocksFor(partitionId))
    total
  }

  /**
   * Whether one partition's consumer has stopped acknowledging for longer than the liveness window.
   *
   * Evaluated on the calling thread against the injected clock rather than by a timer, so this
   * handler starts no thread of its own and a suite can assert the ten second window
   * deterministically. A partition with nothing outstanding is never stalled, however long it has
   * been quiet: there is nothing for the consumer to acknowledge.
   *
   * The writer polls this to decide whether to spill the unacknowledged window; the window itself
   * is retained here either way, because a consumer that reconnects must be able to be served from
   * memory or from spill rather than forcing the whole stage to be recomputed.
   *
   * @param partitionId the reduce partition to test
   * @return true if the partition has unacknowledged output and has seen no progress in the window
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

  /**
   * Whether any subscribed consumer of one partition has stopped *acknowledging*.
   *
   * The distinction between acknowledging and merely being alive is the whole of this method. A
   * heartbeat proves a consumer's process is running; it says nothing about whether that consumer
   * is consuming. Timing them together would let a consumer that heartbeats every five seconds and
   * acknowledges nothing hold the producer's window open indefinitely, which is exactly the ten
   * second missing-acknowledgement condition the failure protocol exists to detect. Each session
   * therefore stamps the two events separately, and only the acknowledgement stamp is consulted
   * here.
   *
   * A partition with nothing outstanding for a session is never stalled on that session's account,
   * however long it has been quiet: there is nothing for that consumer to acknowledge. A partition
   * with no subscriber at all is likewise not stalled -- there is no consumer to be slow.
   */
  private def stalled(partitionId: Int, nowMs: Long): Boolean = {
    sessions.values().asScala.exists { session =>
      session.subscribedTo(partitionId) &&
        session.outstandingFor(partitionId) > 0L &&
        nowMs - session.lastAckProgressMs >= CONSUMER_LIVENESS_TIMEOUT_MS
    }
  }

  /**
   * Waits, up to a bound, for every subscribed consumer to have been sent everything it is owed.
   *
   * This is what makes a successful producer stop honest. Egress is paced, so a block offered at
   * the very end of a map task may still be queued when the task finishes; discarding it there
   * would lose output the reduce side is entitled to and report success while doing it. Waiting
   * gives pacing the chance to release those bytes, and the return value says plainly whether it
   * did, so the caller can fail rather than claim a delivery that did not happen.
   *
   * The wait drains rather than sleeps through the whole interval: each pass attempts real progress
   * and then yields for a short step, so a refill or a writability change is acted on immediately.
   * It reads the injected clock, so a suite drives it deterministically.
   *
   * @param timeoutMs the longest this call may wait; a non-positive value polls once
   * @return true when nothing is queued for any consumer and every subscribed consumer of every
   *         terminated partition has been sent its terminator
   */
  def awaitDrain(timeoutMs: Long): Boolean = {
    val deadlineMs = clock.getTimeMillis() + math.max(0L, timeoutMs)
    var satisfied = drainSatisfied()
    while (!satisfied && clock.getTimeMillis() < deadlineMs && !closed.get()) {
      drain()
      satisfied = drainSatisfied()
      if (!satisfied) {
        // Yielding rather than sleeping the whole remaining interval: the event loop that will
        // deliver a writability change or a refill wake-up needs the CPU more than this one does.
        Thread.`yield`()
      }
    }
    satisfied
  }

  /** Whether every session's queue is empty and every terminated partition has been terminated. */
  private def drainSatisfied(): Boolean = {
    // A session whose channel is open but which has sent nothing for a whole liveness window is
    // presumed lost and is not waited for. Its output is not abandoned: it stays in the retained
    // store, replayable in full when that consumer reconnects, which is what makes excluding it
    // from the drain condition honest rather than a way of declaring success prematurely.
    val nowMs = clock.getTimeMillis()
    val live = sessions.values().asScala
      .filterNot(_.isClosed)
      .filter(session => nowMs - session.lastInboundMs < CONSUMER_LIVENESS_TIMEOUT_MS)
      .toSeq
    live.forall(_.queue.isEmpty) && streams.values().asScala.forall { stream =>
      !stream.terminationRequested.get() ||
        live.forall(session =>
          !session.subscribedTo(stream.partitionId) ||
            session.terminationConfirmed(stream.partitionId))
    }
  }

  // ==========================================================================================
  // Retransmission, bounded to the retained window
  // ==========================================================================================

  /**
   * Replays retained blocks for one partition over the inclusive range the consumer asked for.
   *
   * Retransmission is bounded by what is still retained, and the bound is not a limitation of this
   * implementation but the price of reclaiming producer memory on acknowledgement: once a block
   * has been acknowledged its bytes are gone, so no amount of asking can bring them back. A
   * request that reaches below the acknowledged position is therefore not serviceable, and is
   * escalated through the error notifier so that the task thread raises the fetch failure that has
   * the unmodified scheduler recompute the upstream stage. This handler never constructs that
   * fetch failure itself; raising it belongs to the reduce side, which is the party the scheduler
   * attributes it to.
   *
   * Requests are paced by exponential backoff starting at one second and are refused after five
   * attempts, at which point the condition is escalated rather than retried forever. A serviced
   * acknowledgement resets that budget, because progress means the peer is healthy again.
   *
   * @param partitionId the reduce partition whose blocks are being replayed
   * @param firstSequenceNumber inclusive lower bound of the requested range
   * @param lastSequenceNumber inclusive upper bound of the requested range
   * @return the number of blocks re-queued for the wire, zero if the request was deferred, refused
   *         or escalated
   */
  def retransmit(partitionId: Int, firstSequenceNumber: Long, lastSequenceNumber: Long): Int = {
    val session = sessions.values().asScala.find(_.subscribedTo(partitionId))
    session.map(retransmitTo(_, partitionId, firstSequenceNumber, lastSequenceNumber)).getOrElse {
      misaddressedMessages.incrementAndGet()
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} discarded a retransmission " +
        log"request for partition ${MDC(PARTITION_ID, partitionId)}: " +
        log"${MDC(REASON, "no consumer is subscribed to it")}")
      0
    }
  }

  /**
   * Replays retained blocks for one partition to one consumer, bounded by that consumer's own
   * retransmission budget.
   *
   * The budget is per session rather than per partition, because it measures a *peer's* health: a
   * consumer that keeps asking for the same bytes has a problem that its neighbour, reading the
   * same partition perfectly well, does not share and must not be escalated for.
   */
  private def retransmitTo(
      session: ConsumerSession,
      partitionId: Int,
      firstSequenceNumber: Long,
      lastSequenceNumber: Long): Int = {
    val stream = streams.get(partitionId)
    if (stream == null || lastSequenceNumber < firstSequenceNumber) {
      misaddressedMessages.incrementAndGet()
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} discarded a retransmission " +
        log"request for partition ${MDC(PARTITION_ID, partitionId)}: " +
        log"${MDC(REASON, "no such stream or an inverted window")}")
      0
    } else {
      val nowMs = clock.getTimeMillis()
      if (nowMs < session.nextRetransmitAtMs(partitionId)) {
        // Deferred rather than refused: the consumer may ask again once the backoff has elapsed.
        0
      } else {
        val attempts = session.chargeRetransmitAttempt(partitionId)
        if (attempts > MAX_RETRANSMIT_ATTEMPTS) {
          escalateRetransmissionBudget(partitionId, attempts)
          0
        } else {
          session.deferRetransmitUntil(partitionId, nowMs + backoffMs(attempts))
          serviceRetransmission(session, partitionId, firstSequenceNumber, lastSequenceNumber)
        }
      }
    }
  }

  /**
   * Re-queues every block in the requested range, having first established that the whole of it is
   * still retained.
   *
   * Validating the complete range before emitting any part of it is the correctness requirement
   * here, not an optimisation. A partial replay is worse than a refusal: the consumer receives some
   * of what it asked for, cannot tell that the rest will never arrive, and waits out its producer
   * liveness detector before failing -- by which time the diagnosis points at the network rather
   * than at reclaimed memory. Refusing the whole request instead escalates immediately, through the
   * notifier, to the fetch failure whose stage recomputation is the real recovery.
   *
   * Eligibility is read from the retained store rather than inferred from an acknowledged position,
   * because the store is the authority on what it still holds -- in memory, mid-eviction or on disk
   * -- and because the store, not this handler, is what retires a block once every subscribed
   * consumer has confirmed it. A block already evicted to disk is fully serviceable and would have
   * been wrongly refused by a memory-only view of the window.
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
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} cannot replay blocks " +
        log"${MDC(COUNT, firstSequenceNumber)} through ${MDC(VALUE, lastSequenceNumber)}: the " +
        log"retained window is ${MDC(NUM_BLOCKS, lowestRetained)} through " +
        log"${MDC(MAX_SIZE, highestRetained)}, so the read must be invalidated and the upstream " +
        log"stage recomputed")
      0
    } else {
      val priority = attempt.get()
      var sequenceNumber = firstSequenceNumber
      var queued = 0
      while (sequenceNumber <= lastSequenceNumber) {
        val framedBytes = framedLengthOf(
          store.flatMap(_.retainedPayload(partitionId, sequenceNumber)).map(_.length).getOrElse(0))
        if (session.offer(PendingBlock(partitionId, sequenceNumber, framedBytes, priority,
            egressTicket.getAndIncrement(), replay = true))) {
          queued += 1
        }
        sequenceNumber += 1L
      }
      if (queued > 0) {
        // A consumer may ask for the same window several times inside its retry budget, and every
        // partition it reads can do so, so the per-replay record is detail. The producer reports
        // the replay total at default level in its summary, and an exhausted budget escalates.
        if (debugEnabled) {
          logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
            log"${MDC(PARTITION_ID, partitionId)} is replaying ${MDC(NUM_BLOCKS, queued)} " +
            log"retained block(s) to consumer ${MDC(SESSION_ID, session.consumerId)}")
        }
        drainSession(session)
      }
      queued
    }
  }

  /**
   * Escalates a consumer that keeps asking for the same bytes past the retry budget.
   *
   * The producing task is failed rather than left replaying forever, which is the escalation the
   * failure protocol prescribes once backoff is exhausted: stage recomputation then recovers the
   * output. The condition has no entry in Spark's error catalogue and none is added for it, because
   * the catalogue is not this file's to extend.
   */
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

  // ==========================================================================================
  // Transport callbacks
  //
  // This handler is installed by `TransportContext`, so every callback below is the transport's
  // rather than Netty's directly. That is what gives streaming its authentication, its optional
  // SSL and its keep-alive without touching a single shared transport class: a `TransportClient`
  // arrives already authenticated when `spark.authenticate` is on, and its identity is what a
  // session is bound to.
  // ==========================================================================================

  /**
   * This handler serves no chunked streams, only one-way messages, so the stream manager it offers
   * is an ordinary empty one. It is a real instance rather than null because the transport
   * dereferences it unconditionally when a stream request arrives, and answering "no such stream"
   * is the correct response to a request this subsystem never invites.
   */
  private val streamManager = new OneForOneStreamManager()

  override def getStreamManager(): StreamManager = streamManager

  /**
   * Consumes one control frame that arrived as a one-way message.
   *
   * One-way is the shape every streaming frame travels in, in both directions: the protocol's own
   * acknowledgements and heartbeats are the reply, so a request-response round trip would add a
   * second, redundant reply to every one of them.
   */
  override def receive(client: TransportClient, message: ByteBuffer): Unit = {
    guard {
      decodeAndHandle(client, message)
    }
  }

  /**
   * Consumes one control frame that arrived as a request expecting a reply.
   *
   * A consumer has no need to use this shape, but a transport peer may, and answering it is cheaper
   * than refusing it: the frame is handled exactly as a one-way frame would be and the reply is
   * empty. Replying is what stops the peer from waiting out its RPC timeout for an answer this
   * protocol never intended to send.
   */
  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    guard {
      decodeAndHandle(client, message)
    }
    callback.onSuccess(ByteBuffer.allocate(0))
  }

  /**
   * Notes the arrival of a consumer's channel, without allocating anything for it yet.
   *
   * No session is created here, and deliberately: a channel that has not yet named a partition has
   * asked for nothing, and allocating per-partition state for a peer that has made no request is
   * exactly the unbounded-state exposure this subsystem has to avoid. The session appears with the
   * consumer's first control frame, which is also the frame that says which partition it wants.
   */
  override def channelActive(client: TransportClient): Unit = {
    guard {
      // One record per accepted connection, and a wide shuffle brings one connection per reduce
      // task, so the arrival is debug detail. A connection that goes on to matter -- because it
      // subscribes, stalls, or is lost -- is reported by the path that observes that instead.
      if (debugEnabled) {
        logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} accepted an egress channel " +
          log"from ${MDC(HOST_PORT, client.getSocketAddress())}")
      }
    }
  }

  /**
   * Records the loss of one consumer's channel, retaining everything it had not acknowledged.
   *
   * Retention is the point: the failure protocol requires that a consumer which reconnects be
   * served from memory or from spill rather than forcing the whole upstream stage to be recomputed.
   * The session is dropped, because that channel will never carry anything again, but the *bytes*
   * belong to the retained store and stay there -- the consumer's acknowledged position is recorded
   * against its identity in that store, so its next connection resumes from exactly where this one
   * stopped. Releasing output is [[releaseAll]]'s job alone.
   */
  override def channelInactive(client: TransportClient): Unit = {
    guard {
      val session = sessions.remove(sessionKeyOf(client))
      if (session != null) {
        session.close()
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} lost its egress channel " +
          log"to consumer ${MDC(SESSION_ID, session.consumerId)} at " +
          log"${MDC(HOST_PORT, client.getSocketAddress())} with " +
          log"${MDC(NUM_BYTES, unacknowledgedBytes)} unacknowledged byte(s) retained for replay")
      }
      reportPeerLoss()
    }
  }

  /**
   * Latches a channel level failure and closes that consumer's channel.
   *
   * Only the failing channel is closed. A fault on one consumer's connection says nothing about the
   * others, and tearing them all down turns one consumer's problem into the map output's problem.
   */
  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    guard {
      errorNotifier.setError(cause)
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} egress channel to " +
        log"${MDC(HOST_PORT, client.getSocketAddress())} raised " +
        log"${MDC(ERROR, cause.getMessage())}; closing it and failing the producing task", cause)
      closeSession(client)
    }
  }

  // ==========================================================================================
  // Inbound dispatch
  // ==========================================================================================

  /**
   * Decodes one framed message, checking the peer's protocol revision before its body is trusted.
   *
   * The version is peeked without consuming, so a mismatch is attributed to the version rather
   * than blamed on an unknown type byte further down. A mismatch is one of the conditions under
   * which the streaming shuffle steps aside in favour of the sort based implementation, which is
   * why it is published through [[versionMismatchDetected]] as well as escalated.
   */
  private def decodeAndHandle(client: TransportClient, frame: ByteBuffer): Unit = {
    val version = StreamingShuffleMessage.peekProtocolVersion(frame)
    if (StreamingShuffleMessage.isCompatible(version)) {
      handleInbound(client, StreamingShuffleMessage.Decoder.fromByteBuffer(frame))
    } else {
      reportVersionMismatch(client, version)
    }
  }

  /**
   * Routes one inbound message, discriminating on its concrete type.
   *
   * Discrimination is by type and never by encoded length: an acknowledgement, a heartbeat, a
   * retransmission request and an end of stream marker all encode to exactly the same number of
   * bytes, so length carries no information about which of them arrived.
   */
  private def handleInbound(client: TransportClient, message: StreamingShuffleMessage): Unit = {
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
    } else {
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
  }

  /**
   * Whether a partition id could belong to this map output at all.
   *
   * Checked before any state is created for it, which is what bounds the metadata a remote peer can
   * provoke: an arbitrary partition id names nothing, so it allocates nothing. The bound is the
   * partition count the retained store was told at registration, and before that is known the
   * protocol's own non-negativity is all that can honestly be enforced.
   */
  private def servesPartition(partitionId: Int): Boolean = {
    partitionId >= 0 && retainedOutput.forall { store =>
      !store.partitionCountRegistered || partitionId < store.numPartitions
    }
  }

  /**
   * Refuses a message a producer can never legitimately receive.
   *
   * Data blocks and end of stream markers travel from producer to consumer, so their arrival here
   * means the channel is carrying the wrong direction of the protocol. That is a typed condition in
   * Spark's error catalogue and is raised as one.
   */
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
    closeSession(client)
  }

  /**
   * Applies one acknowledgement: advances that consumer's position and releases what it covers.
   *
   * This is the moment producer memory is reclaimed, and it is also the single most abusable frame
   * in the protocol -- an acknowledgement is a request to *forget* output, so a forged one is a
   * request to lose data. Four conditions therefore stand between a frame and the bytes it would
   * retire, and no part of the release happens until all four hold.
   *
   *  - The frame must arrive on a channel that holds a session, and it may only acknowledge a
   *    partition that session subscribed to. A peer cannot acknowledge on another consumer's behalf
   *    or for output it never asked for.
   *  - The position must strictly advance that session's own previous position, so a replayed frame
   *    is inert and a late lower one cannot un-retire what a higher one covered.
   *  - The position may not exceed the highest sequence number this producer has actually charged
   *    for the partition. A position plucked from the air -- `Long.MaxValue` being the obvious
   *    choice -- is refused rather than honoured, which is what stops unconsumed output from being
   *    released. The ledger is the authority, and its refusal is fatal to the channel: a peer
   *    acknowledging bytes that were never sent is not a peer this producer can go on serving.
   *  - The retained store applies the same bound independently, and retires only to the *minimum*
   *    position across every registered consumer, so a block survives until every consumer entitled
   *    to it has confirmed receipt.
   *
   * Reclamation is bounded: the protocol requires it to complete within one hundred milliseconds of
   * the acknowledgement, so the elapsed time is measured and a breach is reported once per session
   * rather than on every acknowledgement.
   */
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
    } else if (position > session.sentPosition(partitionId)) {
      // The per-session bound, and the one a forged acknowledgement runs into first: this consumer
      // may only confirm blocks that were written to *its* channel. Every send records its sequence
      // number before the bytes are handed to the socket, so a position beyond that record cannot
      // be the report of a real receipt. Refusing it here rather than letting the ledger and the
      // store each apply their own weaker bound is what keeps the three cursors on one prefix.
      refusedAcks.incrementAndGet()
      errorNotifier.setError(StreamingShuffleErrors.invalidSequenceNumber(
        shuffleId, partitionId, session.sentPosition(partitionId), position))
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} refused an acknowledgement through " +
        log"${MDC(COUNT, position)} from consumer ${MDC(SESSION_ID, session.consumerId)}: " +
        log"only ${MDC(MAX_SIZE, session.sentPosition(partitionId))} has been written to that " +
        log"channel; closing it")
      closeSession(client)
    } else {
      acks.incrementAndGet()
      val startedAtMs = clock.getTimeMillis()
      session.stampAckProgress(startedAtMs)
      // The ledger refuses a position beyond what the producer charged, and its refusal is what
      // makes the whole transition atomic: nothing is released, no cursor moves, the channel fails.
      backpressure.tryAcknowledge(producerKey(partitionId), ack) match {
        case None =>
          refusedAcks.incrementAndGet()
          errorNotifier.setError(StreamingShuffleErrors.invalidSequenceNumber(
            shuffleId, partitionId,
            backpressure.highestChargedSequenceNumber(producerKey(partitionId)), position))
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
            log"${MDC(PARTITION_ID, partitionId)} refused an acknowledgement through " +
            log"${MDC(COUNT, position)} from consumer ${MDC(SESSION_ID, session.consumerId)}: " +
            log"${MDC(REASON, "no such block has been sent")}; closing the channel")
          closeSession(client)
        case Some(_) =>
          val advanced = session.advanceAck(partitionId, position)
          val reclaimedBytes = if (advanced) {
            retainedOutput
              .map(_.acknowledge(session.consumerId, partitionId, position))
              .getOrElse(0L)
          } else {
            0L
          }
          if (advanced) {
            // Progress means the peer is healthy, so the retransmission budget starts afresh.
            session.resetRetransmitBudget(partitionId)
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
          // Reclaimed memory and advanced credit may both have unblocked egress.
          drain()
      }
    }
  }

  /**
   * Applies one consumer heartbeat, which is also how a consumer subscribes and how it resumes.
   *
   * The protocol needs no request message of its own because a heartbeat already carries everything
   * a subscription requires: it names the partition the consumer wants and it states the position
   * that consumer has reached. So the first heartbeat from a fresh consumer subscribes it from the
   * beginning, and the first heartbeat from a reconnecting one subscribes it from exactly where it
   * left off -- which is the resume handshake the failure protocol calls for, with the retained
   * window replayed before any newly produced block, because the queue is filled in sequence order
   * from that position.
   *
   * A heartbeat stamps liveness and *not* acknowledgement progress. Conflating the two would let a
   * consumer that heartbeats punctually and consumes nothing keep the producer's window open for
   * ever, which is the condition the ten second detector exists to catch.
   *
   * <b>What the position on a heartbeat means.</b> One thing in both directions: the <b>next</b>
   * position the sender expects to handle. A producer's heartbeat carries the next position it will
   * produce and a consumer's carries the next position it expects to receive, so a consumer that
   * has received nothing announces zero. It has to be stated in those terms rather than as
   * "consumed through", because the message type refuses a negative sequence number and there is
   * therefore no value with which a fresh consumer could say "nothing yet" -- and because after a
   * gap the next expected position and the highest received one differ, so announcing the latter
   * would have the producer resume past the positions still missing.
   */
  private def handleHeartbeat(client: TransportClient, heartbeat: HeartbeatMessage): Unit = {
    val partitionId = heartbeat.partitionId()
    heartbeats.incrementAndGet()
    val session = sessionFor(client)
    session.stampHeartbeat(clock.getTimeMillis())
    if (session.subscribe(partitionId)) {
      resumeFrom(session, partitionId, heartbeat.sequenceNumber())
    }
    reportHeartbeat(heartbeat)
    // A drain unconditionally, and this is load bearing rather than tidy. Every consumer of a
    // completed map output subscribes *after* the producing task has ended -- the scheduler starts
    // no reduce task before its map stage finishes -- so this heartbeat is frequently the only
    // event that will ever occur on this stream, with no producer thread left to flush anything.
    // A pass here is what delivers the deferred end of stream markers for the two cases
    // [[resumeFrom]] leaves undrained: a partition this map produced nothing for, and a consumer
    // whose position already covers everything retained.
    drain()
    if (debugEnabled) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} saw a heartbeat from consumer " +
        log"${MDC(SESSION_ID, session.consumerId)} stamped " +
        log"${MDC(VALUE, heartbeat.timestampMs())} at position " +
        log"${MDC(COUNT, heartbeat.sequenceNumber())}")
    }
  }

  /**
   * Queues everything one newly subscribed consumer is owed, from its own position onward.
   *
   * The consumer announces the <b>next</b> position it expects, the convention every heartbeat on
   * this protocol follows in both directions, so the position it has effectively confirmed is one
   * below that. Converting here rather than at the call site keeps the conversion in the one place
   * that reasons about retained windows, and means a consumer that has received nothing -- which
   * announces zero, because the message type permits no negative sequence number -- is correctly
   * read as having confirmed nothing and is served from block zero rather than from block one.
   *
   * That converted position is trusted only as a lower bound and is reconciled against two
   * authorities before anything is queued: the store's record of what that consumer has already
   * acknowledged, which a peer cannot talk its way past, and the store's record of what is still
   * retained. The higher of the announced and recorded positions is where delivery starts, so a
   * consumer cannot replay bytes it has already confirmed -- which would be a request to un-retire
   * released memory -- and the lower bound of the retained window is where delivery starts if the
   * consumer is further behind than that.
   *
   * A consumer behind the retained window has lost output that no longer exists. That is not
   * repairable here and is not treated as repairable: it escalates through the notifier so the read
   * is invalidated and the upstream stage recomputed.
   *
   * @param session the consumer session that has just subscribed to the partition
   * @param partitionId the reduce partition the consumer subscribed to
   * @param announcedNextPosition the next block position the consumer says it expects
   */
  private def resumeFrom(
      session: ConsumerSession,
      partitionId: Int,
      announcedNextPosition: Long): Unit = {
    // A heartbeat states the next expected position, so the confirmed position is one below it.
    val announcedPosition = math.max(AckMessage.NOTHING_CONSUMED, announcedNextPosition - 1L)
    retainedOutput.foreach { store =>
      store.registerConsumer(session.consumerId)
      val recorded = store.consumerPosition(session.consumerId, partitionId)
        .getOrElse(AckMessage.NOTHING_CONSUMED)
      val resumeAfter = math.max(announcedPosition, recorded)
      session.advanceAck(partitionId, resumeAfter)
      val lowestRetained = store.lowestRetainedSequence(partitionId)
      val highestRetained = store.lastAcceptedSequence(partitionId)
      if (highestRetained == MemorySpillManager.UNSET_SEQUENCE) {
        // Nothing has been produced for this partition yet; the subscription alone is enough, and
        // blocks will be fanned out to this session as they are admitted.
        session.advanceSent(partitionId, resumeAfter)
      } else if (lowestRetained != MemorySpillManager.UNSET_SEQUENCE &&
          lowestRetained > resumeAfter + 1L) {
        errorNotifier.setError(StreamingShuffleErrors.invalidSequenceNumber(
          shuffleId, partitionId, lowestRetained, resumeAfter + 1L))
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
          log"${MDC(PARTITION_ID, partitionId)} cannot resume consumer " +
          log"${MDC(SESSION_ID, session.consumerId)} from ${MDC(COUNT, resumeAfter + 1L)}: the " +
          log"retained window begins at ${MDC(NUM_BLOCKS, lowestRetained)}, so the read must be " +
          log"invalidated and the upstream stage recomputed")
      } else {
        val queued = serviceRetransmission(
          session, partitionId, math.max(0L, resumeAfter + 1L), highestRetained)
        session.advanceSent(partitionId, resumeAfter)
        if (queued > 0) {
          resumedSessions.incrementAndGet()
          // One record per resumed partition per reconnecting consumer. The counter incremented
          // just above is what carries the volume to an operator; the identity of each resumed
          // stream is detail behind the streaming debug key.
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
   *
   * The protocol is the authority on whether a request can be served and on how many attempts a
   * stream has spent, because it owns the unacknowledged window that bounds replay and the retry
   * budget the failure protocol prescribes. Consulting it also refreshes the stream's inbound
   * activity instant, so a consumer that is asking for repairs is recognised as alive rather than
   * timed out while it waits for them.
   *
   * A request the protocol refuses is one whose bytes are no longer retained, or one from a stream
   * whose replay budget is spent. Neither is serviceable, and the reader escalates such a request
   * to a fetch failure so that the unmodified scheduler recomputes the upstream stage -- which is
   * why refusing here is a complete answer rather than a dropped frame.
   */
  private def handleRetransmitRequest(
      client: TransportClient,
      request: RetransmitRequestMessage): Unit = {
    val partitionId = request.partitionId()
    val session = sessions.get(sessionKeyOf(client))
    if (session == null || !session.subscribedTo(partitionId)) {
      misaddressedMessages.incrementAndGet()
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} refused a replay request " +
        log"for partition ${MDC(PARTITION_ID, partitionId)} from " +
        log"${MDC(HOST_PORT, client.getSocketAddress())}: " +
        log"${MDC(REASON, "that channel is not subscribed to the partition")}")
      return
    }
    val admitted = backpressure.onRetransmitRequest(producerKey(partitionId), request)
    val serviced = if (admitted) {
      retransmitTo(session, partitionId, request.firstSequenceNumber(),
        request.lastSequenceNumber())
    } else {
      0
    }
    if (!admitted) {
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
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
    errorNotifier.setError(new SparkException(s"Streaming shuffle $shuffleId received protocol " +
      s"version $version from its consumer but this executor speaks " +
      s"${StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION}; the shuffle must fall back to the " +
      "sort based implementation."))
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} detected a protocol version " +
      log"mismatch: peer sent ${MDC(VALUE, version)} against " +
      log"${MDC(COUNT, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)}. " +
      log"${MDC(REASON, "streaming yields to sort based shuffle")}")
    closeSession(client)
  }

  // ==========================================================================================
  // Reporting to the backpressure protocol
  //
  // This is the ONLY region of this file that calls BackpressureProtocol. Keeping every call in one
  // place is deliberate: the protocol owns the credit ledger, the acknowledgement and heartbeat
  // timeouts, the consumer liveness window, cross shuffle utilisation aggregation, arbitration on
  // partition count and pending volume, and the backpressure event counter. This handler supplies
  // the observations that ledger is computed from and reimplements none of it.
  //
  // Acknowledgement is the exception to "reported here": it is applied in handleAck rather than
  // forwarded from here, because the ledger's refusal of an impossible position is not a report but
  // a decision -- nothing may be released and the channel must be failed -- and splitting the
  // decision from its consequences across two methods is how a refusal gets ignored.
  // ==========================================================================================

  /**
   * Asks the protocol to admit one pending block for egress, and reports the outcome.
   *
   * This is the single producer side gate, and it is the protocol's rather than this handler's on
   * purpose. `tryAdmit` performs, indivisibly, the three things a block has to clear before it may
   * go on the wire: the stream's consumer credit is checked, the block's framed bytes are charged
   * against the shared egress bucket, and the block is entered into the ledger's unacknowledged
   * window under its sequence number. A refusal is what latches the throttled state and advances
   * the `shuffle.streaming.backpressureEvents` counter, so a throttle is counted once, by the owner
   * of the ledger, instead of once per observer.
   *
   * The bucket is charged here and nowhere else on the data path. The protocol holds the same
   * [[TokenBucketRateLimiter]] this handler is given, so charging it locally as well would debit a
   * block's bytes twice and halve the effective rate. Control frames are the one exception and are
   * charged directly in [[writeControl]], because they bypass the pacing verdict by design.
   *
   * Recording the send is idempotent in the sequence number, which is what makes a retransmitted
   * block safe: replaying it re-enters the same window slot rather than charging the stream twice.
   *
   * @return true if the block may be written now, false if it must stay queued and be retried
   */
  private def admitForEgress(pending: PendingBlock): Boolean = {
    val key = producerKey(pending.partitionId)
    if (pending.replay) {
      // A replay is paced but not charged for credit and not counted as production. Its bytes are
      // already inside the unacknowledged window, so charging them again would refuse a request to
      // replay the whole window against the very allowance that window is measured by; and counting
      // them as newly produced output would inflate this producer's measured rate, which is one
      // half of the ratio the sustained-slowness fallback trips on.
      backpressure.tryAdmitReplay(key, pending.framedBytes.toLong, pending.sequenceNumber)
    } else {
      backpressure.tryAdmit(key, pending.framedBytes.toLong, pending.sequenceNumber)
    }
  }

  /**
   * Reports a consumer heartbeat, carrying the frame rather than just its instant.
   *
   * The message is handed over whole because the ledger records two quantities from it: the local
   * arrival time, which arms the liveness window, and the peer's own stamp, which is what lets a
   * consumer's clock be compared with this executor's.
   */
  private def reportHeartbeat(heartbeat: HeartbeatMessage): Unit = {
    backpressure.onHeartbeat(producerKey(heartbeat.partitionId()), heartbeat)
  }

  /**
   * Reports that the consumer's channel was lost, without latching a degradation.
   *
   * A poll is the whole of it. Losing a channel is explicitly not a fallback condition -- the
   * failure protocol requires that a consumer which reconnects be replayed from the retained window
   * -- so nothing here may trip the sort based fallback. Polling makes the protocol re-evaluate the
   * acknowledgement and heartbeat windows at the moment of the loss instead of at its next
   * scheduled sweep, which is the observation this handler is in a position to contribute; whether
   * the gap has become a timeout stays the protocol's decision.
   */
  private def reportPeerLoss(): Unit = {
    backpressure.pollOnce()
  }

  // ==========================================================================================
  // Observers, read by the task thread
  //
  // The write metrics reporter documents that its methods are called on a single thread, so an
  // event-loop thread must never touch it. These counters exist so that the writer can read the
  // handler's progress on the task thread and forward it to the reporter from there.
  // ==========================================================================================

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
   * for replay. Summed over sessions, because "unacknowledged" is a per-consumer fact: a block one
   * consumer has confirmed is still outstanding for another that has not.
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
   *
   * The minimum rather than the maximum, and the distinction is a correctness one: this figure is
   * what the producer treats as safely delivered, so taking the highest would let one fast consumer
   * authorise the release of bytes a slower one has not yet received. A partition nobody has
   * subscribed to reports nothing consumed, which is the truth.
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

  /**
   * Whether a peer announced a protocol revision this build cannot speak.
   *
   * Published so that the fallback policy can trip on a version mismatch, which is one of the four
   * conditions under which the streaming shuffle yields to the sort based implementation.
   */
  def versionMismatchDetected: Boolean = versionMismatch.get()

  /** Whether there is at least one live consumer channel to write to right now. */
  def isChannelReady: Boolean = {
    !closed.get() && sessions.values().asScala.exists { session =>
      !session.isClosed && session.channel.isActive()
    }
  }

  /**
   * Whether this handler still has retained output it is entitled to serve.
   *
   * True exactly while the executor-scoped resolver holds this generation's registration, which is
   * the same question every send already asks before it reads a byte. Published because the
   * producing task has to be able to distinguish a handler that is finished from one that is merely
   * idle: a successful map task's handler is idle for as long as it takes the reduce stage to
   * start, and tearing it down then would destroy the only endpoint its consumers can reach.
   */
  def servesRetainedOutput: Boolean = retainedOutput.isDefined

  private def sumOverSessions(measure: ConsumerSession => Long): Long = {
    var total = 0L
    sessions.values().asScala.foreach(session => total += measure(session))
    total
  }

  // ==========================================================================================
  // Teardown
  // ==========================================================================================

  /**
   * Releases every queued reference and every consumer session, exactly once.
   *
   * This is the counterpart of the writer's unsuccessful stop and of its task completion listener:
   * once nothing this handler could serve remains published, nothing may be queued or scheduled on
   * it again. A successful producer deliberately does *not* come here when its task ends -- its
   * output stays published and, because the scheduler starts no reduce task until the map stage has
   * finished, its consumers have not started yet -- so the callers are failure, cancellation, a
   * refused hand-off and a superseded generation. Channel loss deliberately does not come here
   * either, because a consumer that reconnects must still be replayable.
   *
   * The order matters and is the whole of the fix for the race this replaces. The close transition
   * is latched first, so no thread can queue a block, schedule a refill wake-up or start a drain
   * pass after this point; only then are the sessions closed and their queues emptied. Latching
   * first also makes the method idempotent: a second caller -- and there are three plausible ones,
   * arriving concurrently -- returns without touching anything.
   *
   * The payload bytes themselves are *not* released here, and must not be: they belong to the
   * retained store, whose own lifetime is the shuffle's rather than the task's, and whose release
   * is driven by acknowledgement, by the shuffle being unregistered, or by a generation being
   * invalidated. Discarding them here is what would destroy output a reduce task is still entitled
   * to read.
   */
  def releaseAll(): Unit = {
    if (closed.compareAndSet(false, true)) {
      val releasedBytes = pendingBytes
      val queuedBlocks = sumOverSessions(session => session.pendingBlocks)
      sessions.values().asScala.foreach(_.close())
      sessions.clear()
      streams.clear()
      if (releasedBytes > 0L || debugEnabled) {
        logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} released " +
          log"${MDC(NUM_BLOCKS, queuedBlocks)} queued block reference(s) totalling " +
          log"${MDC(NUM_BYTES, releasedBytes)} framed byte(s) after " +
          log"${MDC(COUNT, blocksWritten.get())} block(s) written")
      }
    }
  }

  // ==========================================================================================
  // Internal helpers
  // ==========================================================================================

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

  /**
   * The session key of one consumer channel.
   *
   * The channel's own id, not the consumer's identity: a reconnection is a different channel and
   * must be a different session, so that a late teardown of the connection that was lost cannot
   * dispossess the one that replaced it. The consumer identity lives inside the session and is what
   * the retained store's cursors are keyed by, which is what makes the resumption work across the
   * two channels.
   */
  private def sessionKeyOf(client: TransportClient): String = {
    client.getChannel().id().asLongText()
  }

  /**
   * The session for one consumer channel, created on first use.
   *
   * The consumer identity is taken from the transport's authenticated client id when there is one,
   * and from the socket identity when authentication is off. That is the honest binding available:
   * with `spark.authenticate` on, the identity has been established by SASL before this handler
   * sees a single frame, and with it off nothing stronger exists to bind to than the connection.
   * Either way a session may only ever affect the partitions it subscribed to on its own channel,
   * which is the invariant that does not depend on the operator's authentication choice.
   */
  private def sessionFor(client: TransportClient): ConsumerSession = {
    val key = sessionKeyOf(client)
    val existing = sessions.get(key)
    if (existing != null) {
      existing
    } else {
      val identity = Option(client.getClientId())
        .filter(_.nonEmpty)
        .map(id => s"$id@${client.getSocketAddress()}")
        .getOrElse(s"anonymous@${client.getSocketAddress()}")
      val created =
        new ConsumerSession(key, identity, client.getChannel(), clock.getTimeMillis())
      val raced = sessions.putIfAbsent(key, created)
      if (raced != null) raced else created
    }
  }

  /** Closes one consumer's session and its channel, leaving every other consumer untouched. */
  private def closeSession(client: TransportClient): Unit = {
    val session = sessions.remove(sessionKeyOf(client))
    if (session != null) {
      session.close()
    }
    client.getChannel().close()
  }

  /**
   * Runs a Netty callback body so that no failure escapes into the event loop.
   *
   * A non-fatal failure is latched in the notifier, which is what carries it to the task thread;
   * the event loop itself sees nothing, because an exception thrown from a callback would tear
   * down the pipeline without ever reaching the task that is waiting. A fatal error is
   * deliberately not contained: those belong to the JVM, not to this handler.
   */
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
 *
 * The numeric constants restate the protocol's timings in one place. They are protocol facts rather
 * than tunables, and none of them is configurable: a producer and a consumer that disagreed on the
 * liveness window or on the reclamation budget would each be correct by its own reckoning and wrong
 * about the other.
 */
private[spark] object StreamingShuffleServerHandler {

  /**
   * Transport module name of the streaming shuffle.
   *
   * Asking `SparkTransportConf` for this module yields an independent
   * `spark.shuffle-streaming.io.*` namespace. That is the whole reason the name exists: thread
   * counts, buffer sizes, retry knobs and TCP keep-alive can be set for streaming alone, leaving
   * the block transfer service that every other shuffle depends on exactly as it was. The name is
   * passed as an argument and is never added to any shared file.
   */
  val TRANSPORT_MODULE_NAME: String = "shuffle-streaming"

  /**
   * The key that enables operating system keep-alive for the streaming module only.
   *
   * `TransportConf` exposes keep-alive as a boolean with no interval, and the JDK offers no socket
   * option for one, so this switch cannot express the protocol's five second bound. That bound is
   * enforced by the heartbeat timer at the application level, and never by TCP; keep-alive is
   * enabled here purely so that a connection nobody is using is eventually reaped by the kernel.
   */
  val TCP_KEEP_ALIVE_KEY: String = s"spark.$TRANSPORT_MODULE_NAME.io.enableTcpKeepAlive"

  /**
   * The key that bounds how long a streaming channel may be idle, for this module only.
   *
   * The transport derives this from `spark.network.timeout` when it is unset, which is normally two
   * minutes -- twenty-four times the bound this subsystem promises. Setting it per module is what
   * makes the promise true of the socket as well as of the protocol timer, and setting it *per
   * module* is what keeps that aggressive value away from the block transfer service, where a
   * two-minute idle window is entirely appropriate.
   */
  val CONNECTION_TIMEOUT_KEY: String = s"spark.$TRANSPORT_MODULE_NAME.io.connectionTimeout"

  /**
   * The key that bounds how long opening a streaming channel may take, for this module only.
   *
   * The transport defaults this to the idle timeout above, which in turn defaults to
   * `spark.network.timeout`. A consumer that waited that long to discover a producer had gone would
   * report the loss long after the five-second detector had promised to, so the connect deadline is
   * pinned to the same five seconds rather than inherited.
   */
  val CONNECTION_CREATION_TIMEOUT_KEY: String =
    s"spark.$TRANSPORT_MODULE_NAME.io.connectionCreationTimeout"

  /**
   * The five-second connection bound of this subsystem, expressed in whole seconds.
   *
   * Derived from the one place the bound is declared rather than restated, so the transport and the
   * protocol timer can never disagree. Seconds, because the transport parses both timeout keys at
   * second granularity and would silently truncate anything finer; never below one second, so a
   * hypothetical sub-second acknowledgement timeout could not turn into a zero-length deadline that
   * refuses every connection.
   */
  val CONNECTION_TIMEOUT_SECONDS: Long = math.max(1L, BackpressureProtocol.ACK_TIMEOUT_MS / 1000L)

  /** Largest payload a single block may carry, being the protocol's own cap of two mebibytes. */
  val MAX_PAYLOAD_BYTES: Int = DataBlockMessage.MAX_BLOCK_SIZE_BYTES

  /** Largest number of bytes a single block occupies on the wire, framing prefix included. */
  val MAX_FRAMED_BYTES: Int = DataBlockMessage.MAX_ENCODED_FRAME_BYTES

  /**
   * Cadence at which a producer should raise a heartbeat, in milliseconds.
   *
   * It matches the consumer's connection timeout, so a healthy producer refreshes liveness exactly
   * as often as the detector requires and no more; heartbeats are the cheapest frames on the wire
   * but they are not free.
   */
  val PRODUCER_HEARTBEAT_INTERVAL_MS: Long = 5000L

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

  /** Starting size of the egress queue; it grows on demand and is never a bound on output. */
  val INITIAL_EGRESS_QUEUE_CAPACITY: Int = 16

  /**
   * Longest a refill wake-up may be scheduled for, whatever the bucket's own arithmetic says.
   *
   * A very small bandwidth cap can make the wait for one block's worth of tokens arbitrarily long,
   * and an arbitrarily long wake-up would look exactly like a producer that had stopped. Capping
   * the wait costs nothing but a re-evaluation, and it keeps the drain loop responsive to
   * writability changes and acknowledgements that arrive while the bucket is still empty.
   */
  val MAX_REFILL_WAIT_MS: Long = 1000L

  /**
   * Attributes of the task attempt that produced a block, which decide flush order.
   *
   * This is the concrete meaning of prioritising shuffle traffic over speculative execution in a
   * system that marks no packets anywhere: blocks from a first attempt are flushed ahead of blocks
   * from a retry, so a speculative copy of a task cannot delay the attempt that is most likely to
   * be the one whose output is used. The attempt number is the only signal Spark exposes for this
   * on the producer side, and it is exactly the signal `TaskContext` publishes.
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
   * Three things are settled here, and all three are settled for the streaming module alone.
   *
   *  - <b>Keep-alive</b> is enabled, so a connection nobody is using is eventually reaped by the
   *    kernel. It is a coarse safety net beneath the protocol's own timer and never a substitute
   *    for it.
   *  - <b>The five-second connection bound</b> is applied to the socket as well as to the protocol
   *    timer. Without it the transport would inherit `spark.network.timeout`, normally two minutes,
   *    and a consumer would wait twenty-four times its stated deadline before reporting a producer
   *    it could not reach. Both keys are set only when the operator has not set them, so an
   *    explicit value in the executor's configuration still wins.
   *  - <b>Transport-level encryption</b> is taken from the security manager's RPC SSL options,
   *    which is the same material every other Spark connection is protected with. Passing `None`
   *    yields a plaintext channel, which is correct only when the application is unprotected.
   *
   * The configuration is cloned before any key is set, so none of it can leak into the caller's own
   * `SparkConf` and perturb another module. `SparkTransportConf` clones again on its own account,
   * which means the returned configuration is a snapshot: reading it later cannot observe a change
   * made afterwards, and that immutability is what makes "streaming shuffle configuration changes
   * require an executor restart" true rather than merely intended.
   *
   * @param conf the executor's configuration, left untouched
   * @param numUsableCores cores this JVM may use for transport threads, or zero for the default
   * @param security the security manager whose RPC SSL options protect the channel, defaulting to
   *                 this executor's own
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
   * A streaming shuffle channel carries serialized records straight into Spark's deserialization,
   * so an unauthenticated one is a remote code execution surface: the block checksum is a CRC32C,
   * which detects corruption and forges trivially, and is therefore no part of the answer. The
   * answer is the platform's own: when the application has authentication enabled, every channel
   * completes the auth handshake before a single frame is exchanged, exactly as the block transfer
   * service does. When it is disabled the list is empty, which reproduces the platform's behaviour
   * for every other connection in the same application rather than inventing a different one.
   *
   * @param conf the executor's configuration, read for the application id the handshake names
   * @param transportConf the streaming module's transport configuration
   * @param security the security manager holding the application secret, defaulting to this
   *                 executor's own
   * @return the bootstraps to hand to `TransportContext.createClientFactory`
   */
  def streamingClientBootstraps(
      conf: SparkConf,
      transportConf: TransportConf,
      security: Option[SecurityManager] = currentSecurityManager)
    : java.util.List[TransportClientBootstrap] = {
    val bootstraps = new java.util.ArrayList[TransportClientBootstrap]()
    security.filter(_.isAuthenticationEnabled()).foreach { manager =>
      bootstraps.add(new AuthClientBootstrap(transportConf, conf.getAppId, manager))
    }
    bootstraps
  }

  /**
   * The server-side bootstraps a streaming producer server must be created with.
   *
   * The counterpart of [[streamingClientBootstraps]], and required for the same reason: a producer
   * that accepted unauthenticated channels would serve one map task's output to any peer that could
   * reach the port, and would accept acknowledgements -- which release producer memory -- from that
   * same peer. Installing the bootstrap makes the channel's authenticated identity the capability
   * that admits a consumer, and it is installed by the manager when it creates the server, so no
   * shared transport class is touched to achieve it.
   *
   * @param transportConf the streaming module's transport configuration
   * @param security the security manager holding the application secret, defaulting to this
   *                 executor's own
   * @return the bootstraps to hand to `TransportContext.createServer`
   */
  def streamingServerBootstraps(
      transportConf: TransportConf,
      security: Option[SecurityManager] = currentSecurityManager)
    : java.util.List[TransportServerBootstrap] = {
    val bootstraps = new java.util.ArrayList[TransportServerBootstrap]()
    security.filter(_.isAuthenticationEnabled()).foreach { manager =>
      bootstraps.add(new AuthServerBootstrap(transportConf, manager))
    }
    bootstraps
  }

  /**
   * This executor's security manager, when there is a live environment to read it from.
   *
   * Read through `SparkEnv` rather than accepted as a constructor argument because the manager that
   * owns these components is itself constructed by `SparkEnv`, before the environment it belongs to
   * is published; a component that demanded the security manager at construction could therefore
   * not be built at all. `None` means no environment, which happens only outside a running executor
   * and yields the plaintext, unauthenticated configuration that such a context has no secret for.
   */
  private def currentSecurityManager: Option[SecurityManager] =
    Option(SparkEnv.get).map(_.securityManager)

  /**
   * One block waiting for the wire, identified rather than carried.
   *
   * The block's bytes are deliberately absent. They live in the producer's retained store, which is
   * charged against the executor's memory quota and which spills under pressure, and they are read
   * back only at the moment the frame is written. A queue entry that carried the payload would be a
   * second, unaccounted copy of every block in flight -- the bytes would be charged once to the
   * store and held again here, outside any budget -- and a block the store had spilled would still
   * be pinned in memory by this queue, defeating the eviction that spilling performed.
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
   * What remains here is only what is genuinely common to all consumers: the sequence the producer
   * assigns, how many blocks it has produced, and whether it has declared the stream complete.
   * Everything that differs between consumers -- what has been sent, what has been acknowledged,
   * what is queued, and whether the terminator has been delivered -- belongs to the session that
   * consumer holds, because treating any of it as a property of the partition would let one
   * consumer's progress be mistaken for another's.
   *
   * Every field is atomic, because the writer's task thread and the channels' event-loop threads
   * all reach this state and none of them may wait for the others.
   *
   * @param partitionId the reduce partition this state belongs to
   */
  private final class PartitionStream(val partitionId: Int) {

    /** Next sequence number to assign, counted from zero and increasing by one per block. */
    val nextSequenceNumber: AtomicLong = new AtomicLong(0L)

    /** Blocks ever produced for this partition, which is the total the terminator reports. */
    val blocksEnqueued: AtomicLong = new AtomicLong(0L)

    /**
     * Highest sequence number offered to any consumer, or `AckMessage.NOTHING_CONSUMED` when the
     * partition has produced nothing. This is what a session is measured against to decide it has
     * received everything, so a partition that produced no records is caught up from the outset.
     */
    val highestOffered: AtomicLong = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    /** Whether the writer has declared the end of this partition's stream. */
    val terminationRequested: AtomicBoolean = new AtomicBoolean(false)

    /** The block total captured when termination was declared. */
    val totalBlocksAtTermination: AtomicLong = new AtomicLong(0L)
  }

  /**
   * Everything one consumer's channel is owed, and everything it has confirmed.
   *
   * A session exists per channel, not per consumer identity: a reconnection is a new channel and so
   * a new session, which is what stops a late teardown of the connection that was lost from
   * dispossessing the one that replaced it. The consumer identity is carried inside, because it is
   * what the retained store's release cursors are keyed by, and that is what lets a reconnecting
   * consumer resume from exactly the position its predecessor reached.
   *
   * Two properties of this class are load-bearing for correctness rather than tidiness:
   *
   *  - A session may only ever affect the partitions it subscribed to on its own channel. Every
   *    inbound frame is checked against that subscription set before it is allowed to move a cursor
   *    or release a byte, so a peer cannot acknowledge, terminate or ask for the replay of a
   *    partition it never asked to receive.
   *  - Queues are per session, not shared. One slow consumer's un-writable socket therefore cannot
   *    hold the head of a queue that a fast consumer is waiting on, which a single shared queue
   *    would allow and which would make the slowest reader the pace of the whole map output.
   *
   * @param sessionKey the channel's own identifier, this session's identity in the registry
   * @param consumerId the consumer's identity, authenticated where authentication is enabled
   * @param channel the consumer's channel, used for writes and for writability
   * @param createdAtMs the instant the session was opened, which seeds its activity stamps
   */
  private final class ConsumerSession(
      val sessionKey: String,
      val consumerId: String,
      val channel: Channel,
      createdAtMs: Long) {

    /** Blocks queued for this consumer, ordered by the egress priority they were queued under. */
    val queue: PriorityBlockingQueue[PendingBlock] =
      new PriorityBlockingQueue[PendingBlock](INITIAL_EGRESS_QUEUE_CAPACITY, EgressOrdering)

    /** Guard that keeps exactly one thread writing to this channel at a time. */
    val draining: AtomicBoolean = new AtomicBoolean(false)

    /** Set when work arrives while a drain is in flight, so the drain runs one more pass. */
    val drainWakeup: AtomicBoolean = new AtomicBoolean(false)

    /** One outstanding refill wake-up per session is enough; this is that one-shot guard. */
    val refillScheduled: AtomicBoolean = new AtomicBoolean(false)

    /** Per partition subscription state, created when the consumer first names the partition. */
    private val subscriptions: ConcurrentHashMap[Int, Subscription] =
      new ConcurrentHashMap[Int, Subscription]()

    private val closedFlag: AtomicBoolean = new AtomicBoolean(false)
    private val queuedBlockCount: AtomicLong = new AtomicLong(0L)
    private val queuedByteCount: AtomicLong = new AtomicLong(0L)
    private val bytesServed: AtomicLong = new AtomicLong(0L)
    private val ackProgressMs: AtomicLong = new AtomicLong(createdAtMs)
    private val inboundMs: AtomicLong = new AtomicLong(createdAtMs)
    private val reclamationWarned: AtomicBoolean = new AtomicBoolean(false)

    /**
     * The key sessions are visited in when egress is drained, lowest first.
     *
     * Fewest bytes served goes first, which is max-min fairness under a shared rate cap: when the
     * bucket holds less than the whole batch, the consumer that has had the least of the budget so
     * far is the one that gets the next tokens. A fixed order would instead let whichever consumer
     * happened to sort first take the entire refill on every pass.
     */
    def orderingKey: Long = bytesServed.get()

    /** Whether this session has been released; a closed session accepts nothing further. */
    def isClosed: Boolean = closedFlag.get()

    /**
     * Releases this session's queue, once.
     *
     * The channel is deliberately left alone: closing it belongs to whoever decided the session was
     * over, and the retained payloads belong to the store, whose lifetime is the shuffle's rather
     * than this connection's.
     */
    def close(): Unit = {
      if (closedFlag.compareAndSet(false, true)) {
        var pending = queue.poll()
        while (pending != null) {
          releasePending(pending)
          pending = queue.poll()
        }
      }
    }

    /** Whether this consumer has subscribed to one partition on this channel. */
    def subscribedTo(partitionId: Int): Boolean = subscriptions.containsKey(partitionId)

    /**
     * Subscribes this consumer to one partition.
     *
     * @return true when the subscription is new, which is the signal to resume delivery from the
     *         position this consumer reports
     */
    def subscribe(partitionId: Int): Boolean = {
      !isClosed && subscriptions.putIfAbsent(partitionId, new Subscription()) == null
    }

    /** How many partitions this consumer has subscribed to. */
    def subscriptionCount: Int = subscriptions.size()

    /**
     * Queues one block reference for this consumer.
     *
     * Refused for a partition this consumer never subscribed to, which is what keeps fan-out from
     * accumulating state for a peer that never asked for the data.
     *
     * @return true if the reference was queued
     */
    def offer(pending: PendingBlock): Boolean = {
      val subscription = subscriptions.get(pending.partitionId)
      if (isClosed || subscription == null) {
        false
      } else {
        subscription.queuedBlocks.incrementAndGet()
        subscription.queuedBytes.addAndGet(pending.framedBytes.toLong)
        queuedBlockCount.incrementAndGet()
        queuedByteCount.addAndGet(pending.framedBytes.toLong)
        val queued = queue.offer(pending)
        if (!queued) {
          releasePending(pending)
        } else {
          drainWakeup.set(true)
        }
        queued
      }
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
    }

    /**
     * Records that one block has been handed to this consumer's channel.
     *
     * The block's framed size is remembered against its sequence number -- metadata only, never the
     * payload -- so that an acknowledgement releases the exact bytes of the exact prefix it covers
     * rather than an estimate. Without per-sequence sizes an acknowledgement spanning several
     * blocks could only guess at how much it had released, and the outstanding figure the writer
     * uses to decide whether to spill would drift away from the truth.
     */
    def recordSent(partitionId: Int, sequenceNumber: Long, framedBytes: Long): Unit = {
      val subscription = subscriptions.get(partitionId)
      if (subscription != null) {
        if (subscription.sentBytes.putIfAbsent(sequenceNumber, framedBytes) == null) {
          subscription.unacknowledgedBytes.addAndGet(framedBytes)
        }
        advanceSent(partitionId, sequenceNumber)
        bytesServed.addAndGet(framedBytes)
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
     *         store to be told about it; a repeated or stale position moves nothing
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
          while (entry != null) {
            releasedBytes += entry.getValue.longValue()
            entry = released.pollFirstEntry()
          }
          if (releasedBytes > 0L) {
            subscription.unacknowledgedBytes.addAndGet(-releasedBytes)
          }
          true
        }
      }
    }

    /**
     * The highest position written to this consumer for one partition, or nothing sent.
     *
     * This is the bound an acknowledgement is held to. It is recorded before each block's bytes are
     * handed to the socket, so it is never behind what the consumer could legitimately have seen.
     */
    def sentPosition(partitionId: Int): Long = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null) AckMessage.NOTHING_CONSUMED else subscription.sentPosition.get()
    }

    /** The highest position this consumer has acknowledged for one partition. */
    def ackPosition(partitionId: Int): Long = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null) AckMessage.NOTHING_CONSUMED else subscription.ackPosition.get()
    }

    /** Blocks sent to this consumer but not yet acknowledged by it, for one partition. */
    def outstandingFor(partitionId: Int): Long = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null) 0L else subscription.sentBytes.size().toLong
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

    /** Records that a terminator reached the socket. */
    def confirmTermination(partitionId: Int): Unit = {
      val subscription = subscriptions.get(partitionId)
      if (subscription != null) {
        subscription.terminationDelivered.set(true)
      }
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

    /** Stamps acknowledgement progress, which is the only stamp the stall detector consults. */
    def stampAckProgress(nowMs: Long): Unit = {
      ackProgressMs.set(nowMs)
      inboundMs.set(nowMs)
    }

    /** When this consumer last acknowledged anything. */
    def lastAckProgressMs: Long = ackProgressMs.get()

    /**
     * Stamps liveness without stamping progress.
     *
     * Deliberately separate: a heartbeat proves the consumer's process is running and says nothing
     * about whether it is consuming. Timing them together would let a consumer that heartbeats
     * punctually and acknowledges nothing hold the producer's window open indefinitely.
     */
    def stampHeartbeat(nowMs: Long): Unit = inboundMs.set(nowMs)

    /** When this consumer last sent anything at all, acknowledgement or heartbeat. */
    def lastInboundMs: Long = inboundMs.get()

    /** The instant from which this consumer's next retransmission request may be serviced. */
    def nextRetransmitAtMs(partitionId: Int): Long = {
      val subscription = subscriptions.get(partitionId)
      if (subscription == null) 0L else subscription.nextRetransmitAtMs.get()
    }

    /**
     * Charges one retransmission attempt against this consumer's budget for one partition.
     *
     * A request for a partition this consumer is not subscribed to spends an unbounded budget, so
     * the caller refuses it rather than servicing a stream that does not exist.
     */
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

    /**
     * Claims the one-shot right to report a reclamation budget breach for this session.
     *
     * Once per session rather than once per breach, because a consumer whose acknowledgements are
     * consistently slow would otherwise produce one warning per acknowledgement and, on a
     * many-partition shuffle, put the log budget of ten megabytes an hour out of reach on its own.
     */
    def warnReclamationOnce(): Boolean = reclamationWarned.compareAndSet(false, true)
  }

  /**
   * One consumer's state for one reduce partition.
   *
   * Held per (session, partition) rather than per partition, because every quantity here is a fact
   * about a particular consumer: a block one consumer has acknowledged is still outstanding for
   * another that has not, and a terminator delivered to one says nothing about the other.
   */
  private final class Subscription {

    /** Blocks queued for this partition on this session but not yet written. */
    val queuedBlocks: AtomicLong = new AtomicLong(0L)

    /** Framed bytes queued for this partition on this session but not yet written. */
    val queuedBytes: AtomicLong = new AtomicLong(0L)

    /**
     * Framed size of each block written to this consumer and not yet acknowledged, keyed by
     * sequence number. Metadata only -- the payloads themselves are the retained store's -- and
     * ordered so that the prefix an acknowledgement releases is a sub-map view rather than a scan.
     */
    val sentBytes: ConcurrentSkipListMap[Long, java.lang.Long] =
      new ConcurrentSkipListMap[Long, java.lang.Long]()

    /** Highest sequence number written to this consumer, or nothing sent. */
    val sentPosition: AtomicLong = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    /** Highest sequence number this consumer has acknowledged, or nothing consumed. */
    val ackPosition: AtomicLong = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    /** Framed bytes written to this consumer and not yet acknowledged. */
    val unacknowledgedBytes: AtomicLong = new AtomicLong(0L)

    /** One-shot guard so the terminator is written to this consumer at most once at a time. */
    val terminationClaimed: AtomicBoolean = new AtomicBoolean(false)

    /** Set once the terminator has been confirmed onto this consumer's socket. */
    val terminationDelivered: AtomicBoolean = new AtomicBoolean(false)

    /** Retransmission attempts serviced for this consumer since its last acknowledgement. */
    val retransmitAttempts: AtomicInteger = new AtomicInteger(0)

    /** The instant from which this consumer's next retransmission may be serviced. */
    val nextRetransmitAtMs: AtomicLong = new AtomicLong(0L)
  }
}
