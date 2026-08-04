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
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap, PriorityBlockingQueue, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener}

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv, SparkException}
import org.apache.spark.internal.{Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys.{CONFIG, COUNT, DESCRIPTION, DURATION, ERROR, HOST_PORT, MAP_ID, MAX_ATTEMPTS, MAX_SIZE, NUM_BLOCKS, NUM_BYTES, NUM_EVENTS, NUM_FAILURES, NUM_ITERATIONS, NUM_SKIPPED, PARTITION_ID, PORT, PROTOCOL_VERSION, REASON, SESSION_ID, SHUFFLE_ID, STATUS, TASK_ATTEMPT_ID, THRESHOLD, TIMEOUT, VALUE, VERSION_NUM}
import org.apache.spark.internal.config.SHUFFLE_STREAMING_DEBUG
import org.apache.spark.network.TransportContext
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient, TransportClientBootstrap}
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.protocol.OneWayMessage
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager, TransportServer, TransportServerBootstrap}
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, DataBlockMessage, HeartbeatMessage, RetransmitRequestMessage, StreamingShuffleMessage, StreamingShuffleMessageType, StreamTerminationMessage}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

/**
 * The inbound-routing table a producer generation is published in, as an abstraction rather than as
 * the executor's listener itself.
 *
 * A generation's state has several owners on a producer executor -- the routing table that decides
 * which handler an inbound frame reaches, the retained-output registry a consumer resolves through,
 * this handler's own sessions and queues, and the spill files behind them. Withdrawing a generation
 * has to touch all of them, in one order, or a consumer can be routed to a handler whose bytes have
 * gone, or resolve a store nothing can be requested from. [[StreamingShuffleServerHandler]] is
 * therefore where that single operation lives, because it is the one component that already knows
 * its own generation and holds every other owner -- and this abstraction is what gives it the last
 * one without making it depend on the listener's whole surface.
 *
 * Withdrawal is qualified by the handler, not merely by the key. Under
 * `spark.shuffle.useOldFetchProtocol` a map id is the partition index rather than the task attempt
 * id, so two attempts of one map task share a routing key; a straggling older attempt must not be
 * able to withdraw the route its replacement installed.
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
}

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
 * directly, so every window it enforces -- the liveness bound behind [[stalledPartitions]], the
 * retransmission backoff -- advances with that clock rather than with real time. It is also why
 * this handler starts no timer thread of its own: liveness is evaluated when it is asked for, on
 * the thread that asks.
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
 * =Why a healthy shuffle can put nothing on the wire=
 *
 * Stated here because this handler is where the evidence shows up, and it reads like a fault when
 * it is not one. At an ordinary stage boundary this handler reports every block as enqueued and
 * held -- "egress is paced, no consumer has subscribed, or no channel is writable" -- and finishes
 * with zero bytes on the wire, having streamed nothing live to anyone.
 *
 * That is the expected reading, and the reason is structural rather than local. Whether a consumer
 * exists while a producer runs is decided by task submission, which belongs to the DAG scheduler
 * and the task scheduler -- absolute preservation zones for this feature, which may not be modified
 * at all -- and the unmodified scheduler submits a stage only once every parent stage reports its
 * output available. So for a map stage whose tasks each run once, the reduce tasks are submitted
 * after the last of them has finished: no consumer can be subscribed while this handler is offered
 * blocks, every block is retained, made durable at the writer's stop, and served afterwards from
 * the executor-scoped [[StreamingShuffleBlockResolver]] instead. What the streaming path removes in
 * that configuration is the index-and-fetch round trip and the reduce side's whole-partition
 * materialisation; the producer/consumer overlap is capability rather than outcome.
 *
 * Live egress through this handler is therefore exercised where a consumer really is attached -- a
 * replay requested by a reconnecting consumer, and any subscriber a scheduler that submitted
 * consumers earlier would provide. Nothing in this file assumes either case: the accounting of
 * bytes on the wire is kept exact precisely so the difference is measurable rather than assumed,
 * and `StreamingShuffleWriter`'s own class documentation carries the same statement from the
 * producing side. Do not read a zero as a defect in egress, and do not add a warning for it: it is
 * the scheduler's submission order, visible from here.
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
    routes: StreamingShuffleRouteRegistry,
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
   * The ledger identity of one partition of this map output as it flows to '''one''' consumer.
   *
   * The producer generation is part of the identity because two attempts of one map task -- a
   * speculative copy, or a retry after a failure -- produce the same shuffle and the same
   * partitions and are nevertheless two separate flows with two separate windows of unacknowledged
   * bytes. The role distinguishes this sending ledger from the receiving ledger of a consumer that
   * happens to be running in the same JVM, which is always the case under `local[*]`.
   *
   * One ledger per consumer session, and the reason is a correctness one rather than a matter of
   * resolution. A ledger's unacknowledged window is what bounds replay: a block inside it can be
   * served again and a block outside it cannot. Two consumers of the same partition acknowledge at
   * their own pace, so a single shared window would advance to whatever the *fastest* of them had
   * confirmed -- and a slower sibling asking to have block twenty replayed would then be refused on
   * the grounds that block twenty was acknowledged, by somebody else. Splitting the window per
   * consumer makes the bound mean what it says: what this consumer has been sent and has not yet
   * confirmed.
   *
   * It also makes credit mean what it says. Admission is charged once per consumer per block,
   * because that is how many copies of the block go on the wire, so a partition fanned out to three
   * consumers is paced as the three transfers it is instead of the one it is not.
   *
   * The release of retained bytes remains the store's decision and is keyed by consumer there too,
   * retiring only to the minimum position across every registered consumer. The ledger and the
   * store therefore answer the same two questions the same way: what this consumer has confirmed,
   * and what may still be replayed to it.
   *
   * @param partitionId the reduce partition the flow feeds
   * @param consumerId the consumer session's identity, as adopted from its own heartbeat
   */
  private def consumerLedgerKey(partitionId: Int, consumerId: String): BackpressureStreamKey =
    BackpressureStreamKey.forProducer(shuffleId, mapId, taskAttemptId, partitionId, consumerId)

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

  /**
   * The session currently serving each '''logical''' consumer, keyed by the identity that consumer
   * declares on its own heartbeats.
   *
   * A reduce task's identity is stable across the connections it makes -- it is derived from its
   * task attempt and its partition range, not from a socket -- so this map is what makes a
   * reconnection resume rather than restart. It also makes a superseded connection detectable: when
   * a second channel declares an identity this map already holds, the earlier session is stale by
   * construction and is evicted, which stops two channels from both believing they are serving the
   * same reduce task.
   */
  private val sessionsByConsumer = new ConcurrentHashMap[String, ConsumerSession]()

  /**
   * When each logical consumer was last heard from, kept beyond the life of its session.
   *
   * A session ends when its channel does, but a consumer's entitlement to its output does not, so
   * the instant it was last seen has to outlive the connection in order to be the basis of an
   * expiry decision at all. This is what bounds the per-consumer state the retained store accrues:
   * without it a cursor for a consumer that never returns would be kept for the whole life of the
   * shuffle.
   */
  private val consumerLastSeenMs = new ConcurrentHashMap[String, java.lang.Long]()

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
   * One-shot generation-withdrawal transition, deliberately distinct from the close transition.
   *
   * The two are not the same event and must not share a latch. Closing releases what this handler
   * holds; withdrawing retires the generation from every owner on this executor, and closing is
   * only its last step. Sharing one latch would let whichever arrived first suppress the other, so
   * a handler already closed by a channel failure could never have its routing entry or its
   * retained output withdrawn.
   */
  private val withdrawn = new AtomicBoolean(false)

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
  private val duplicateAcks = new AtomicLong(0L)
  private val expiredSessions = new AtomicLong(0L)
  private val expiredConsumers = new AtomicLong(0L)
  private val supersededSessions = new AtomicLong(0L)
  private val identityConflicts = new AtomicLong(0L)
  private val refusedSessions = new AtomicLong(0L)
  private val untrackedConsumers = new AtomicLong(0L)
  private val deferredBlocks = new AtomicLong(0L)
  private val sessionCapacityReported = new AtomicBoolean(false)
  private val consumerCapacityReported = new AtomicBoolean(false)

  /**
   * Consumer-session slots currently claimed, which is the concurrency ceiling made atomic.
   *
   * A counter rather than `sessions.size()`, because a ceiling enforced by reading a size and then
   * inserting is two steps: a burst of connections all read a size below the ceiling and then all
   * insert past it, so the ceiling fails under exactly the arrival pattern it exists to bound. A
   * slot is claimed by compare-and-set before a session is built and returned by [[forgetSession]],
   * which is the single teardown point every removal path routes through.
   */
  private val liveSessionSlots = new AtomicInteger(0)

  /**
   * Subscribers currently admitted per reduce partition.
   *
   * The number of sessions is bounded and one session's subscription map is bounded, but their
   * product decides how much per-stream state a single partition can be made to carry -- and a
   * partition is legitimately read by exactly one reduce task. Bounding the subscribers of a
   * partition closes that product. Entries are created on first subscription and cleared with the
   * handler; there is at most one per reduce partition, so the map is bounded by the shuffle's own
   * width rather than by anything a peer chooses.
   */
  private val subscribersByPartition = new ConcurrentHashMap[Integer, AtomicInteger]()

  // Partitions whose end of stream has been requested. Read on the egress hot path to decide
  // whether the deferral scan below has anything to look for: that scan visits every stream this
  // handler owns, so running it after each drain pass while the task is still producing would make
  // the cost of writing one block a function of the shuffle's partition count. Terminations are
  // requested only as a map task finishes, so the counter is zero for the whole of the write and
  // the scan costs nothing until it can do something.
  private val terminationRequests = new AtomicInteger(0)

  /**
   * Cumulative totals and reporting windows for the three conditions whose frequency is not this
   * producer's to decide.
   *
   * Each recurs once per block or once per frame rather than once per task, and each of them is
   * something a peer can provoke: a consumer channel that fails accepts a write per block and fails
   * every one of them; a consumer that asks for a replay it cannot have may ask again as often as
   * it likes; a consumer whose producer generation has been withdrawn keeps heartbeating until it
   * times out. Reporting each occurrence would put this executor's log volume under the control of
   * whatever is wrong at the other end of the socket, which is the one thing a diagnostic must
   * never be.
   *
   * Three windows and not one, because the three say different things -- our egress failing, a
   * replay that cannot be served, and a consumer that cannot be admitted -- and a burst of any one
   * of them must not silence the first occurrence of another. Each admitted report carries the
   * running total and the number of occurrences it stands in for, so the volume survives even when
   * the individual events do not, and per-occurrence identity remains available behind the
   * streaming debug key.
   */
  private val egressFailures = new AtomicLong(0L)
  private val egressFailureLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)
  private val replayRefusals = new AtomicLong(0L)
  private val replayRefusalLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)
  private val subscriptionRefusals = new AtomicLong(0L)
  private val subscriptionRefusalLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)
  private val lossyChannelClosures = new AtomicLong(0L)
  private val lossyChannelLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)
  private val framingBudgetRefusals = new AtomicLong(0L)
  private val framingBudgetLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)
  private val throttleReports = new AtomicLong(0L)
  private val throttleLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)
  private val orderingDeferrals = new AtomicLong(0L)
  private val orderingLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /**
   * Emits one report through a window, or accounts it against that window and stays quiet.
   *
   * @param gate the window that decides
   * @param total the running total for the condition, advanced here so a caller cannot forget to
   * @param entry the report, built only when it is going to be emitted or traced
   * @param cause the failure to attach, or `null` when the report is not about one
   */
  private def reportBounded(
      gate: MemorySpillManager.LogAggregationGate,
      total: AtomicLong,
      entry: => MessageWithContext,
      cause: Throwable = null): Unit = {
    val occurrences = total.incrementAndGet()
    gate.admit(clock.getTimeMillis()) match {
      case Some(unreported) =>
        val message = entry +
          log" (${MDC(NUM_EVENTS, occurrences)} occurrence(s) on this map output, " +
          log"${MDC(NUM_SKIPPED, unreported)} not reported individually)"
        if (cause == null) logWarning(message) else logWarning(message, cause)
      case None =>
        if (debugEnabled) {
          if (cause == null) logDebug(entry) else logDebug(entry, cause)
        }
    }
  }

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
        // One write is issued per block, so one failing channel fails every write on it. The
        // notifier is already first-error-wins and carries the diagnosis to the task thread; this
        // record exists for the operator, and is bounded so that a single dead consumer cannot
        // spend the executor's whole log budget restating itself once per block.
        reportBounded(egressFailureLogGate, egressFailures,
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
   * <b>The sequence number is derived first and committed last.</b> Every check below can refuse
   * the block, and a refused block must leave the partition's numbering exactly as it was: an
   * advanced counter would give the next accepted block a number the consumer is not expecting,
   * breaking the dense numbering the reader reassembles on, and it would inflate the total the
   * terminator reports so that a consumer which received everything still concluded it was short.
   * The commit is a compare-and-set rather than an increment, so the numbering is also the detector
   * for the one condition that would corrupt it silently -- two producers sharing one handler.
   *
   * There is deliberately only one counter. How many blocks a partition has produced is not tracked
   * separately from the numbering they are produced under, because the two are the same quantity,
   * and a second counter is a second thing that can fall out of step with the first.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param payloadBytes the block's payload size, at most [[MAX_PAYLOAD_BYTES]]
   * @param priority the attributes of the attempt that produced the block
   * @return the sequence number assigned to the block
   * @throws IllegalArgumentException if the size is negative or larger than the block cap
   * @throws IllegalStateException if the partition's stream has already been terminated, if the
   *                               retained store does not hold the block under the assigned
   *                               sequence number, or if another producer advanced this partition's
   *                               numbering concurrently
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
   * A session whose queue is at one of its ceilings has the block recorded as owed to it instead,
   * so the reference list stays bounded without the block going undelivered: [[topUpOwed]] queues
   * it as the queue drains. That is the whole of what a queue ceiling costs -- a delay -- since the
   * bytes live in the retained store regardless of whether a reference to them is queued.
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
          if (session.owedBlocksFor(partitionId) > 0L) {
            // Order before immediacy. This consumer is already owed earlier positions of this
            // partition, and an owed position is queued with a fresh ticket when the drain pays the
            // run down -- so queueing this block now would give it a *lower* ticket than the
            // positions that must precede it and put it on the wire out of sequence. A consumer
            // that receives a block ahead of its predecessor cannot deliver it: it quarantines the
            // position, asks for the run to be replayed, and a repair that should never have been
            // needed ends in the producer being reported lost. Joining the owed run instead keeps
            // one ascending sequence per partition per consumer, which is the invariant the whole
            // retransmission protocol rests on.
            if (session.deferOwed(partitionId, sequenceNumber, sequenceNumber)) {
              deferredBlocks.incrementAndGet()
            }
          } else {
            val pending = PendingBlock(partitionId, sequenceNumber, framedBytes, priority,
              egressTicket.getAndIncrement(), replay)
            if (session.offer(pending)) {
              queued += 1
            } else if (session.deferOwed(partitionId, sequenceNumber, sequenceNumber)) {
              deferredBlocks.incrementAndGet()
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
   * This is the other half of every bounded path in this handler: the queue ceiling and the replay
   * page both work by refusing to queue a reference now and recording that it is still owed, and
   * this is what honours the record. It is called from the drain loop, so a consumer's owed run is
   * paid down at exactly the rate its channel and its credit allow rather than all at once.
   *
   * <b>Why the size comes from the store rather than a read.</b> Pacing a block needs its framed
   * size before the block is queued. Reading the payload to obtain it would cost a disk round trip
   * and an allocation per block for a number the store already recorded, which is what
   * `retainedPayloadLength` answers.
   *
   * <b>Why replay-ness is derived per block rather than passed in.</b> A block at or below what
   * this consumer has already been sent is a repair: its bytes are in the unacknowledged window,
   * so it is paced but not charged again. A block above that position is reaching this consumer for
   * the first time, whatever asked for it, so it is charged as the production it is. Deriving the
   * distinction per block rather than per request is what keeps it right for a resume, whose range
   * can span the boundary.
   *
   * A block the store no longer holds is queued anyway, at the size of an empty frame. That looks
   * odd and is deliberate: [[writeBlock]] is the single place that decides what an unservable block
   * means, and duplicating that judgement here would give the subsystem two answers to one
   * question.
   * Because the acknowledged prefix has already been discarded above, such a block is one this
   * consumer is genuinely owed and cannot be sent, which is the escalation case rather than the
   * droppable one.
   *
   * @return the number of references queued
   */
  private def topUpOwed(session: ConsumerSession, partitionId: Int, budget: Int): Int = {
    val store = retainedOutput
    // The acknowledged prefix first, in one step. An owed run is a range and can therefore cover
    // positions this consumer confirmed before the run was recorded; queueing those would charge
    // credit for blocks that are then dropped as stale, and that charge has nothing left to
    // release it. Settling the prefix here keeps every position this loop queues one the consumer
    // is genuinely still owed.
    session.discardOwedThrough(partitionId, session.ackPosition(partitionId))
    var queued = 0
    var keepGoing = store.isDefined && !session.isClosed
    while (keepGoing && queued < budget) {
      val sequenceNumber = session.takeOwed(partitionId)
      if (sequenceNumber == MemorySpillManager.UNSET_SEQUENCE) {
        session.deindexIfSettled(partitionId)
        keepGoing = false
      } else {
        val payloadBytes = store.flatMap(_.retainedPayloadLength(partitionId, sequenceNumber))
        val framedBytes = framedLengthOf(payloadBytes.getOrElse(0))
        val replay = sequenceNumber <= session.sentPosition(partitionId)
        val pending = PendingBlock(partitionId, sequenceNumber, framedBytes, attempt.get(),
          egressTicket.getAndIncrement(), replay)
        if (session.offer(pending)) {
          queued += 1
        } else {
          // The queue reached its ceiling again. The position goes back on the owed run so the next
          // pass resumes from it, and this pass stops rather than spinning against a full queue.
          session.deferOwed(partitionId, sequenceNumber, sequenceNumber)
          keepGoing = false
        }
      }
    }
    queued
  }

  /**
   * Queues up to one page of everything owed to one consumer, over every partition it is owed for.
   *
   * Driven by the session's owed index rather than by its subscriptions, which is what keeps this
   * free on the ordinary path: a consumer that is keeping up is owed nothing, so the index is empty
   * and this returns without touching a partition.
   */
  private def topUpSession(session: ConsumerSession): Int = {
    var queued = 0
    session.owedPartitionIds.foreach { partitionId =>
      queued += topUpOwed(session, partitionId, REPLAY_PAGE_BLOCKS)
    }
    queued
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

  // Egress: draining onto the channel

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
      // Two refusals, two retry strategies, and the distinction is what keeps a throttled channel
      // from spinning. A pacing refusal is repaired by the passage of time, so it schedules a
      // wake-up at the bucket's own next refill instant. A refusal for want of consumer credit is
      // repaired only by an acknowledgement -- which drains this session as it is applied -- and by
      // channel writability, which does the same, so scheduling a timer for it would poll a
      // condition no clock can change. With an unlimited bucket the refill instant is *now*, so
      // that timer used to re-enter this method every millisecond for as long as the consumer
      // stayed behind, burning a core and emitting one record per pass.
      var pacingDelayMs = 0L
      var budgetDelayed = false
      while (keepGoing) {
        if (!channel.isWritable()) {
          // The socket's outbound buffer is full. Leaving the block queued is correct: writability
          // is signalled on this very channel, and the writability callback resumes the drain.
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
          } else if (!admitForEgress(session, pending)) {
            session.queue.offer(pending)
            throttles.incrementAndGet()
            pacingDelayMs = math.max(pacingDelayMs,
              rateLimiter.millisUntilAvailable(pending.framedBytes.toLong))
            reportBounded(throttleLogGate, throttleReports,
              log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
                log"${MDC(PARTITION_ID, pending.partitionId)} is throttled holding " +
                log"${MDC(NUM_BYTES, pending.framedBytes)} framed byte(s); " +
                log"${MDC(VALUE, rateLimiter.availableTokens)} token(s) available and " +
                log"${MDC(THRESHOLD, session.outstandingFor(pending.partitionId))} " +
                log"block(s) unacknowledged by the consumer")
            keepGoing = false
          } else if (!StreamingShuffleServerHandler.EgressFramingBudget
              .tryReserve(pending.framedBytes.toLong)) {
            // Every credit and pacing check has passed, but this executor has as many transient
            // framing copies in flight as it is allowed. The block is returned to the queue exactly
            // as a pacing refusal returns it, and the refill wake-up below retries it -- so the
            // budget delays a block rather than dropping one. The ledger entry the admission above
            // created is left in place deliberately: it names a block inside the unacknowledged
            // window, which is where this block still is, and withdrawing it would make the very
            // replay this queue is about to perform unserviceable.
            session.queue.offer(pending)
            throttles.incrementAndGet()
            budgetDelayed = true
            reportBounded(framingBudgetLogGate, framingBudgetRefusals,
              log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
                log"${MDC(PARTITION_ID, pending.partitionId)} is holding " +
                log"${MDC(NUM_BYTES, pending.framedBytes)} framed byte(s) because this executor " +
                log"already has " +
                log"${MDC(MAX_SIZE, StreamingShuffleServerHandler.EgressFramingBudget.inFlight)} " +
                log"byte(s) of streaming shuffle egress in flight, its ceiling of " +
                log"${MDC(THRESHOLD, StreamingShuffleServerHandler.EgressFramingBudget.capacity)}")
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
      if (pacingDelayMs > 0L) {
        scheduleRetryDrain(session, pacingDelayMs)
      } else if (budgetDelayed) {
        // The executor-wide framing ceiling is released by write completions rather than by a
        // clock, and a completion releases the budget without draining anything, so this one
        // condition does need a timer. It is a coarse one: the ceiling is measured in mebibytes and
        // clears as soon as a socket accepts what is already in flight.
        scheduleRetryDrain(session, FRAMING_BUDGET_RETRY_WAIT_MS)
      }
      writeNanos.addAndGet(math.max(0L, clock.nanoTime() - startedAtNanos))
      written
    }
  }

  /**
   * Whether writing this block would leave a gap in its partition's sequence for this consumer.
   *
   * The frontier a consumer has reached is the higher of what it has been sent and what it has
   * acknowledged -- the second matters on a reconnection, where the session is new and has sent
   * nothing but the consumer's cursor says where it is -- and a block more than one position beyond
   * that frontier would arrive before its predecessors.
   *
   * Cheap and unconditional: two atomic reads per block, on a path that is already reading the
   * session's ledgers.
   */
  private def leavesSequenceHole(session: ConsumerSession, pending: PendingBlock): Boolean = {
    val frontier = math.max(session.sentPosition(pending.partitionId),
      session.ackPosition(pending.partitionId))
    pending.sequenceNumber > frontier + 1L
  }

  /**
   * Puts a block that would have left a gap back onto the owed run, together with the run it
   * skipped.
   *
   * <b>Why this guard exists at all.</b> Every path that queues a block queues it in ascending
   * order, and yet the ordering is not thereby guaranteed: the owed run is paid down by taking a
   * position and then queueing it as two steps, so a position taken by one thread can be queued
   * after a position taken later by another, and the egress order is decided by the ticket taken at
   * queueing time. Rather than serialise every producer of queue entries against every other --
   * which would put a lock on the egress hot path -- the invariant is enforced where it is cheap
   * and absolute: at the one point a block is about to leave. A block that would arrive out of
   * sequence is not written at all; the whole run from the consumer's frontier to that block is
   * recorded as owed, and the drain loop's own top-up queues it in ascending order on the next
   * pass. Nothing is lost, because the bytes live in the retained store and an owed run is
   * precisely a claim on them.
   *
   * The deferral is reported through the aggregation window rather than per occurrence: it is a
   * repair of an internal race, so its volume matters to an operator and its individual instances
   * do not.
   */
  private def deferSequenceHole(session: ConsumerSession, pending: PendingBlock): Unit = {
    val partitionId = pending.partitionId
    val frontier = math.max(session.sentPosition(partitionId), session.ackPosition(partitionId))
    val firstMissing = math.max(0L, frontier + 1L)
    // The reference this pass polled is not going back on the queue -- the owed run below covers
    // its position and the top-up will queue it in sequence -- so its charge against the queue's
    // two ceilings has to be returned here, exactly as [[writeBlock]] returns the charge of a block
    // that leaves for good. Holding the charge for a reference nobody will ever write would shrink
    // this consumer's queue permanently, one entry per repair, until the ceiling refused every
    // block and every position had to travel as an owed run.
    session.releasePending(pending)
    session.deferOwed(partitionId, firstMissing, pending.sequenceNumber)
    reportBounded(orderingLogGate, orderingDeferrals,
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
   * Called only for a condition a clock can repair -- a pacing refusal, whose delay comes from the
   * bucket itself so the wake-up lands when tokens are actually available, or the executor-wide
   * framing ceiling, which is released by write completions. A refusal for want of consumer credit
   * schedules nothing: the acknowledgement that grants credit drains this session as it is applied,
   * and so does channel writability, so a timer would only poll a condition time cannot change --
   * which, with an unlimited bucket, meant re-entering the drain every millisecond for as long as a
   * consumer stayed behind.
   *
   * It is scheduled on the session's own event loop, which needs no thread of this handler's own
   * and serialises naturally with every other callback on that channel. One outstanding wake-up per
   * session is enough, because a drain that is still delayed schedules the next one before it
   * returns.
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
   * with no difference visible on the wire.
   *
   * <b>A block the store cannot produce is answered, never merely dropped.</b> There are two ways
   * to arrive there and they are not the same event. If this consumer has already acknowledged past
   * the block, the queue entry is stale -- it holds nothing this consumer needs -- and dropping it
   * is the whole of the correct response. If it has not, the consumer is owed bytes that no longer
   * exist, and silence was the wrong answer: the consumer would wait out its full five second
   * connection timeout for a block that is never coming, and a producer would meanwhile be free to
   * report a successful map output that at least one reader cannot read. That case is escalated by
   * [[abortUnservableStream]] instead.
   *
   * <b>Why the framing copy is charged to its own budget rather than to the buffer budget.</b>
   * Building the message and encoding it allocates a transient copy of the payload, and that copy
   * is deliberately outside the executor-wide *buffer* quota, because it is not retained: it is
   * handed to the channel and released as the socket drains it. Charging it there as well would
   * double-count the same block and shrink the real streaming window to half of what the operator
   * configured -- every held byte is already in the retained store, exactly once, charged before it
   * was admitted.
   *
   * It is nonetheless charged, to
   * [[StreamingShuffleServerHandler.EgressFramingBudget]], and Netty's own write water marks are
   * not a substitute for that. A water mark bounds one channel: the caller writes only while
   * `Channel.isWritable` holds, so a single consumer cannot accumulate copies without limit. But
   * the ceiling on consumers is [[MAX_CONCURRENT_SESSIONS]] per map output, across every map output
   * on the executor, and per-channel bounds multiply -- so thousands of sessions each holding one
   * high-water mark's worth of transient copies is an aggregate no per-channel limit constrains.
   * The budget is executor wide precisely because that is the quantity that has to be bounded, and
   * the reservation is taken in [[drainOnce]] before this method runs, so the copy below is never
   * made until there is room for it. The listener released with the write is what returns it: a
   * copy stops occupying the executor the moment the transport is done with it, not when this
   * method returns.
   *
   * @return true if a frame was handed to the channel
   */
  private def writeBlock(session: ConsumerSession, pending: PendingBlock): Boolean = {
    val partitionId = pending.partitionId
    val sequenceNumber = pending.sequenceNumber
    val framedBytes = pending.framedBytes.toLong
    val payload = retainedOutput.flatMap(_.retainedPayload(partitionId, sequenceNumber))
    session.releasePending(pending)
    payload match {
      case Some(bytes) =>
        val block = DataBlockMessage.withComputedChecksum(
          shuffleId, mapId, partitionId, sequenceNumber, bytes)
        session.recordSent(partitionId, sequenceNumber, framedBytes)
        session.channel.write(sendable(block))
          .addListener(StreamingShuffleServerHandler.EgressFramingBudget.releaseOn(framedBytes))
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
      case None =>
        // No frame was built, so no copy is in flight and the reservation the drain took for this
        // block is returned at once. Returning it here rather than leaving it to a write listener
        // that will never fire is what keeps the budget a live measure of bytes actually in flight
        // instead of a figure that only ever rises.
        StreamingShuffleServerHandler.EgressFramingBudget.release(framedBytes)
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

  /**
   * Answers a consumer that is owed a block this producer can no longer produce.
   *
   * Two things happen, and each addresses a different party's blindness.
   *
   * The consumer is told by terminating the partition's stream with the number of blocks this
   * producer actually committed. That is the ordinary terminator, carrying the ordinary total, and
   * the consumer's own completeness check does the rest: it has consumed fewer blocks than the
   * total announced, so it discards everything taken from this producer and raises a fetch failure,
   * which is what makes the unmodified scheduler recompute the upstream stage. Using the existing
   * terminator rather than a new message type is deliberate -- the protocol's message family is
   * fixed, and a short stream is already a condition the consumer detects and recovers from, so
   * what was missing was not a way to say it but the act of saying it. The alternative, silence,
   * cost the consumer a full five second timeout and told it nothing about why.
   *
   * The producer is told through the error notifier, so that a map task still running fails rather
   * than going on to publish a map status for output that at least one reader has been unable to
   * read. A retained window that has lost a block a subscribed consumer had not acknowledged is an
   * invariant violation, not a pacing outcome, and the conservative response to it is to reproduce
   * the output rather than to trust the part of it that survived.
   *
   * Reported once per partition per session. A retention window that has lost one block has usually
   * lost a run of them, and one line per block would be one line per two megabytes.
   */
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
          shuffleId, mapId, partitionId, committedBlocks, committedBlocks))
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

  // Liveness and orderly end of stream

  /**
   * Emits a producer heartbeat for one partition, stamped from the injected clock.
   *
   * The wire message carries a timestamp and reads no clock of its own, deliberately: the instant
   * it reports is supplied from here, so the five second liveness bound is measured on one clock
   * rather than on two. The consumer's own detector is satisfied by this
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
    val nextPosition = stream.nextSequenceNumber.get()
    subscribers.foreach { session =>
      // The position announced is the next one this producer will commit, which is exactly the
      // reading a consumer's own heartbeat carries, so one header field means one thing in both
      // directions. Built through the protocol wherever it holds this session's ledger, because the
      // emission has to be recorded against the very ledger whose interval decided the beat was
      // due; recording it elsewhere would leave the interval permanently elapsed and every
      // maintenance sweep would send another beat.
      val stamped = backpressure
        .heartbeatFor(consumerLedgerKey(partitionId, session.consumerId), nextPosition)
        .getOrElse(new HeartbeatMessage(
          shuffleId, mapId, partitionId, nextPosition, clock.getTimeMillis()))
      writeControl(session, stamped)
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
      // The numbering is the count: it has been advanced exactly once per accepted block and never
      // for a refused one, so it is the total a consumer can verify its own tally against.
      stream.totalBlocksAtTermination.set(stream.nextSequenceNumber.get())
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
   *
   * <b>Why an owed run holds the terminator back as firmly as a queued block.</b> A block this
   * consumer is owed is output this producer has undertaken to deliver and has not yet queued --
   * the queue ceiling refused it, or a resume recorded a run that is still being paid down a page
   * at a time. Reading emptiness from the queue alone therefore reports a stream as finished while
   * delivery is still outstanding, and the drain that follows pays the owed run down *after* the
   * terminator has left: the consumer, which has by then reconciled its count against the total the
   * terminator fixed and closed the stream, sees blocks arrive past an end it was told about, and
   * the only reading available to it is that its producer is unsound. That is the spurious
   * producer-loss condition -- a healthy shuffle failing its reduce stage and recomputing an intact
   * map stage -- and the ordering rule that prevents it is stated here once: a stream ends for a
   * consumer only when nothing is queued for it, nothing is owed to it, and everything offered has
   * been sent.
   */
  private def emitDeferredTerminations(session: ConsumerSession): Unit = {
    if (terminationRequests.get() > 0 && !session.isClosed && session.channel.isActive()) {
      streams.values().asScala.foreach { stream =>
        val partitionId = stream.partitionId
        val ready = stream.terminationRequested.get() &&
          session.subscribedTo(partitionId) &&
          session.pendingBlocksFor(partitionId) <= 0L &&
          session.owedBlocksFor(partitionId) <= 0L &&
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
    sessions.values().asScala.foreach { session =>
      // Owed blocks count as pending. They are output this producer still has to deliver, and the
      // consumer waiting for them measures producer liveness on its own timer, so a stream with an
      // unpaid owed run must go on being heartbeated exactly as one with a queued block does.
      total += session.pendingBlocksFor(partitionId) + session.owedBlocksFor(partitionId)
    }
    total
  }

  /**
   * Whether one partition's consumer has stopped acknowledging for longer than the liveness window.
   *
   * Evaluated on the calling thread against the injected clock rather than by a timer, so this
   * handler starts no thread of its own and the ten second window advances with that clock. A
   * partition with nothing outstanding is never stalled, however long it has been quiet: there is
   * nothing for the consumer to acknowledge.
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
   * Runs one round of the upkeep this producer's consumers need, from a thread that is not a
   * task's.
   *
   * <b>Why this exists.</b> A map task's writer performs the same upkeep while it runs, and for the
   * resources it owns that is the only place it can be performed: buffered bytes are task-managed
   * execution memory, and the executor takes them back when the task ends. But a consumer's need
   * for upkeep does not end when the producing task does. A consumer attached during production is
   * driven by the writer itself -- [[enqueueBlock]] drains to it on the spot -- and one that
   * attaches after the task has ended has no producer thread left to drive anything, yet is served
   * by exactly the same egress path. The heartbeats that hold its stream open, the expiry that
   * bounds its state, and the drain that delivers its blocks therefore have to be callable from
   * outside a task, and that is what this method is. The caller is [[StreamingShuffleListener]],
   * whose lifetime is the executor's, on a fixed cadence.
   *
   * The post-completion case is not a rare one, and that is a consequence of the DAG scheduler
   * rather than of anything here: the scheduler submits a reduce stage only once its map stage
   * reports available output, and it is an absolute preservation zone for this feature (AAP 0.2.1
   * and 0.2.2 forbid modifying the DAG scheduler or the task lifecycle; AAP 0.8.2 Tier 1 restates
   * it). So at an ordinary stage boundary this method is the whole of what serves a consumer, while
   * during production the writer's own maintenance is -- and both reach the same code.
   *
   * Four duties, in the order that makes each one's input current:
   *
   *  1. Heartbeat every partition a live consumer is subscribed to, so a consumer waiting on
   *     retained blocks does not time its producer out while they are queued behind pacing.
   *  2. Retire sessions that have gone quiet for a whole liveness window, releasing the
   *     socket-scoped state -- queue, credit ledgers, retry budget -- while leaving the consumer's
   *     cursor intact so its next connection resumes rather than restarts.
   *  3. Retire logical consumers silent long enough to have exhausted the whole replay budget,
   *     which stops a cursor for a consumer that never returns from being held for the life of the
   *     shuffle.
   *  4. Drain, because steps two and three can have released the credit and the memory that the
   *     remaining consumers were waiting on.
   *
   * Idempotent and safe to call concurrently with the writer's own maintenance: every step is
   * expressed over concurrent collections and atomics, and the two overlap harmlessly -- a
   * heartbeat sent twice is a heartbeat, and an expiry decision is latched by the removal that
   * performs it.
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
      drain()
      retired
    }
  }

  /**
   * Sends a producer heartbeat on every partition a live consumer is reading.
   *
   * Driven by the protocol's own five-second timer rather than by a timer of this handler's, so the
   * producing task's writer and this sweep cannot each keep their own idea of when a beat is due
   * and between them send two.
   */
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
   * <b>What is released and what is not.</b> Released: the queue of block references, the credit
   * ledgers, the retry budget -- everything that describes a transfer over a connection that has
   * stopped answering. Kept: the consumer's acknowledged position in the retained store, and the
   * retained bytes themselves. That asymmetry is the failure protocol's requirement rather than a
   * convenience: a consumer that reconnects must be replayed from memory or from spill instead of
   * forcing the whole upstream stage to be recomputed, and the position it resumes from is exactly
   * the cursor this method declines to touch.
   *
   * <b>A live channel is given the whole replay budget, not the liveness window.</b> Silence on an
   * open connection is what a consumer that is busy reducing looks like: it sends a frame when it
   * acknowledges a block or when a heartbeat comes due while it waits, and it sends nothing at all
   * while it is deserialising and aggregating what it already has. Retiring such a session at the
   * ten-second mark took its queue, its credit ledgers and its retry budget away mid-stream, and
   * the reconnection that followed was observed by the consumer as a reset connection and by the
   * producer as a failed egress channel -- a producer loss manufactured out of a healthy consumer's
   * silence. The window is therefore applied to what it can actually diagnose: a channel that is
   * closed or no longer active has stopped answering, and that is retired at the liveness window. A
   * channel that is still active is retired only once it has been silent for
   * [[CONSUMER_EXPIRY_TIMEOUT_MS]] -- the liveness window plus every backoff the failure protocol
   * would have spent on a repair -- which is the point past which no repair remains outstanding and
   * a half-open socket is the only remaining explanation. Memory is still reclaimed, on the same
   * bound the logical-consumer expiry uses; what is no longer reclaimed is a session that was
   * working.
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
      if (silentForMs >= toleranceMs) {
        expiredSessions.incrementAndGet()
        retired += 1
        evictSession(session)
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} released the egress " +
          log"session of consumer ${MDC(SESSION_ID, session.consumerId)} after " +
          log"${MDC(DURATION, silentForMs)} ms without a frame, beyond the " +
          log"${MDC(TIMEOUT, toleranceMs)} ms tolerated for a channel that was " +
          log"${MDC(REASON, if (channelAnswering) "still open" else "no longer answering")}; its " +
          log"position is retained so a reconnection resumes from it")
      }
    }
    retired
  }

  /**
   * Unregisters the logical consumers that have been silent long enough to have failed for good.
   *
   * <b>Why a second, longer window.</b> The ten-second window says a *connection* has stopped
   * answering, and the specified response to that is to retain the consumer's window and replay it
   * when the consumer comes back -- so it cannot also be the moment the consumer is forgotten. But
   * a consumer that never comes back must eventually be forgotten, or its cursor pins the minimum
   * acknowledged position across consumers for the life of the shuffle and the memory behind it
   * with it. This window is therefore the liveness window plus the entire replay budget: once a
   * consumer has been silent for longer than every backoff the failure protocol would have spent
   * trying to repair it, no repair remains outstanding and there is nothing left to hold open on
   * its behalf.
   *
   * Forgetting a consumer is safe in the sense that matters: it releases memory rather than output.
   * Spilled segments are this map task's output on disk and are unlinked by the block resolver at
   * generation withdrawal or shuffle unregistration, never here, so a consumer that returns after
   * this point is still served from disk -- and if the blocks it needs were only ever in memory,
   * its resume escalates to a fetch failure and the unmodified scheduler recomputes the map task,
   * which is the recovery the protocol prescribes for a window that no longer exists.
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
        expiredConsumers.incrementAndGet()
        retired += 1
        val freedBytes = retainedOutput.map(_.unregisterConsumer(consumerId)).getOrElse(0L)
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} unregistered consumer " +
          log"${MDC(SESSION_ID, consumerId)} after ${MDC(DURATION, silentForMs)} ms of silence, " +
          log"beyond the ${MDC(TIMEOUT, CONSUMER_EXPIRY_TIMEOUT_MS)} ms expiry window, releasing " +
          log"${MDC(NUM_BYTES, freedBytes)} byte(s); its spilled output stays readable")
      }
    }
    retired
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
    session.map { subscribed =>
      replayGate(subscribed, partitionId, firstSequenceNumber, lastSequenceNumber, resume = false)
    }.getOrElse {
      misaddressedMessages.incrementAndGet()
      reportBounded(replayRefusalLogGate, replayRefusals,
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
   * <b>Why the two share a gate.</b> They are the same operation seen from two directions -- take
   * blocks the store still holds and put them back on one consumer's queue -- and when they had
   * separate implementations the resume path went round the checks the request path applied. That
   * is a difference a peer can exploit and, worse, one a maintainer cannot see: the budget, the
   * backoff and the escalation all appeared to govern replay while one of the two ways of reaching
   * replay ignored them. One gate makes the governing rules the same by construction.
   *
   * <b>What the two do not share, and must not.</b> How the blocks are admitted for egress.
   *
   *  - A '''resume''' is a first delivery. These blocks have never been on this consumer's channel,
   *    so they are admitted as ordinary output: charged against its credit and entered into its
   *    unacknowledged window. That is what makes the window mean "what this consumer has been sent
   *    and not confirmed", which is the property every later replay decision for this consumer is
   *    read from. Admitting them as replays instead would leave the window empty and have the very
   *    next repair request refused on the grounds that nothing was outstanding.
   *  - A '''repair''' is a second delivery. These blocks are already inside the window and already
   *    hold their credit, so they are paced but not charged again -- charging twice would refuse a
   *    request to replay a whole window against the allowance it is measured by -- and they count
   *    as replay rather than production, so a stream repairing itself cannot inflate the producer
   *    rate that the sustained-slowness fallback compares against the consumer's.
   *
   * The attempt budget is charged only when the range overlaps what this session has already been
   * sent, which is the definition of a repair however it was reached. A first delivery spends none
   * of it: the budget bounds repeated repair of the same bytes, and a subscription is not that. The
   * backoff deferral, by contrast, applies to both, so a session that has just been told to wait
   * cannot shorten the wait by re-subscribing.
   *
   * The budget is per session rather than per partition, because it measures a *peer's* health: a
   * consumer that keeps asking for the same bytes has a problem that its neighbour, reading the
   * same partition perfectly well, does not share and must not be escalated for.
   *
   * @param session the consumer the blocks are for
   * @param partitionId the reduce partition whose blocks are being re-delivered
   * @param firstSequenceNumber inclusive lower bound of the range
   * @param lastSequenceNumber inclusive upper bound of the range
   * @param resume true when this is a first delivery to a subscribing consumer, false when it is a
   *               repair of blocks that consumer has already been sent
   * @return the number of blocks re-queued, zero if the request was deferred, refused or escalated
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
        reportBounded(replayRefusalLogGate, replayRefusals,
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
   *
   * <b>Bounded, however long the range is.</b> The whole range is recorded as owed to this consumer
   * and only one page of it is queued here; the drain loop pays the rest down as the channel and
   * the credit window allow. A resume is why that matters: a consumer returning after a long gap is
   * owed everything from its acknowledged position to the producer's high-water mark, and queueing
   * that in one pass would materialise a reference per block of a whole partition's history --
   * remotely triggered, once per reconnection, on a path that has no other ceiling.
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
      reportBounded(replayRefusalLogGate, replayRefusals,
        log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
          log"${MDC(PARTITION_ID, partitionId)} cannot replay blocks " +
          log"${MDC(COUNT, firstSequenceNumber)} through ${MDC(VALUE, lastSequenceNumber)}: the " +
          log"retained window is ${MDC(NUM_BLOCKS, lowestRetained)} through " +
          log"${MDC(MAX_SIZE, highestRetained)}, so the read must be invalidated and the " +
          log"upstream stage recomputed")
      0
    } else {
      // Recorded as owed and then drained, rather than queued here. This method runs on whichever
      // event-loop thread delivered the subscription or the retransmission request, while the drain
      // loop pays the owed run down under the session's own guard -- and taking a position off that
      // run and queueing it are two steps, so two threads doing it at once can queue an earlier
      // position after a later one. The ticket taken at queueing time is what orders egress, so
      // that race put a block on the wire ahead of its predecessor: the consumer quarantined the
      // position, asked for a replay of the run, and the producer's honest end-of-stream was then
      // refused against a position the consumer had never reached -- reported as a lost producer
      // and paid for with a full recomputation of the map stage. Leaving the transfer to the
      // guarded drain makes one thread the only queuer of a session's blocks, which is what keeps a
      // partition's egress strictly ascending.
      val owed = if (session.deferOwed(partitionId, firstSequenceNumber, lastSequenceNumber)) {
        math.max(0L, lastSequenceNumber - firstSequenceNumber + 1L)
      } else {
        0L
      }
      if (owed > 0L) {
        // A consumer may ask for the same window several times inside its retry budget, and every
        // partition it reads can do so, so the per-replay record is detail. The producer reports
        // the replay total at default level in its summary, and an exhausted budget escalates.
        if (debugEnabled) {
          logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
            log"${MDC(PARTITION_ID, partitionId)} is replaying ${MDC(NUM_BLOCKS, owed)} " +
            log"retained block(s) to consumer ${MDC(SESSION_ID, session.consumerId)}")
        }
        drainSession(session)
      }
      math.min(owed, Int.MaxValue.toLong).toInt
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

  // Transport callbacks
  //
  // This handler is installed by `TransportContext`, so every callback below is the transport's
  // rather than Netty's directly. That is what gives streaming its authentication, its optional
  // SSL and its keep-alive without touching a single shared transport class: a `TransportClient`
  // arrives already authenticated when `spark.authenticate` is on, and its identity is what a
  // session is bound to.

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
   *
   * '''Why the ordinary close is not a warning.''' A reduce task closes its channel when it has
   * finished reading, so one close per consumer is the expected end of every healthy shuffle, and
   * the count of them is `numMaps * numReduces` for the application as a whole. Reporting each at
   * warning level would put an executor's log volume in direct proportion to the width of its
   * shuffles -- a thousand-by-a-thousand shuffle produces hundreds of thousands of lines, one to
   * two orders of magnitude past the volume this feature is allowed -- and every one of them would
   * describe a clean teardown as a lost channel, which trains an operator to ignore the subsystem's
   * warnings.
   *
   * So the two cases are separated by what the close actually leaves behind. A session that owes
   * its consumer nothing -- nothing written and unacknowledged, nothing still queued -- has
   * completed its work and is reported only under the streaming debug key. A session that closes
   * still owing bytes is the condition the failure protocol exists for, and that keeps its warning,
   * bounded through the same aggregation window as every other recurring condition here so a storm
   * of dead consumers cannot spend the whole log budget either. The per-session figures are used
   * rather than the handler-wide totals, because what this channel left owed is the fact reported.
   */
  override def channelInactive(client: TransportClient): Unit = {
    guard {
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
          reportBounded(lossyChannelLogGate, lossyChannelClosures,
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

  // Inbound dispatch

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
   *  - The position may not exceed the highest sequence number written to this session's own
   *    channel, which is the bound a forged acknowledgement runs into first and the one
   *    `AckMessage.acknowledgesWithin` states. A position plucked from the air -- `Long.MaxValue`
   *    being the obvious choice -- is refused rather than honoured, and the refusal is fatal to the
   *    channel: a peer acknowledging bytes that were never sent is not a peer to go on serving.
   *  - The frame's own control sequence number must strictly advance the highest this session has
   *    already applied, which is what `AckMessage.supersedes` states. Note the number space: the
   *    test is on the consumer's outbound message counter, not on the position it reports, because
   *    one acknowledgement covers however many blocks were consumed since the last and a
   *    reconnecting consumer restarts its counter while its position resumes where it left off.
   *    A replayed or reordered frame is inert: it releases nothing, it refreshes nothing, and it is
   *    counted rather than acted on.
   *  - The per-consumer ledger refuses a position beyond what it charged, and the retained store
   *    applies the same bound independently and retires only to the *minimum* position across every
   *    registered consumer, so memory holding a block survives until every consumer entitled to it
   *    has confirmed receipt.
   *
   * <b>Why a duplicate must not refresh the progress clock.</b> The ten-second stall detector reads
   * the instant of the last acknowledgement *progress*, and that reading is what makes a consumer
   * which has stopped consuming observable. Stamping it on arrival rather than on advancement would
   * let a peer hold its producer's window open indefinitely by re-sending one acknowledgement it
   * had already sent: it would look like progress for ever while confirming nothing new. The stamp
   * therefore happens after -- and only after -- the position is established as a strict advance.
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
    } else if (!ack.acknowledgesWithin(session.sentPosition(partitionId))) {
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
    } else if (!session.applyControlSequence(partitionId, ack)) {
      // Inert, not invalid. A duplicate or reordered acknowledgement is an ordinary consequence of
      // the wire, so it is counted and dropped -- and, crucially, it does not touch the progress
      // stamp the stall detector reads. The claim is made on the consumer's control sequence
      // number, so an acknowledgement that covers many blocks at once is applied rather than
      // mistaken for a repeat of the position already recorded.
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
          closeSession(client)
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
    // No session, no state. Either the session ceiling was reached, in which case the channel has
    // already been closed, or the peer tried to rename a live session, in which case the same is
    // true -- and in both there is nothing to subscribe, nothing to resume and nothing to report.
    sessionFor(client)
      .filter { session =>
        session.stampHeartbeat(clock.getTimeMillis())
        // Identity before subscription, because the identity is what the retained store's cursor
        // and this partition's credit ledger are keyed by: subscribing first would open both
        // under the connection's provisional identity and abandon them once it adopted its own.
        adoptIdentity(client, session, heartbeat.consumerId())
      }
      .foreach { session =>
        if (session.subscribedTo(partitionId)) {
          // Already subscribed on this channel: an ordinary repeat heartbeat, which neither opens a
          // ledger nor claims a subscriber slot.
          ()
        } else if (!claimSubscriberSlot(partitionId)) {
          // Refused before a ledger, a queue entry or a payload copy exists. The number of sessions
          // and the width of one session's subscription map are each bounded, but their product is
          // what decides how much per-stream state a single partition can be made to carry, and a
          // partition is legitimately read by exactly one reduce task. Refusing here is what closes
          // that product, and refusing it *before* the drain is what keeps a fan-out attempt from
          // costing this executor a payload copy per session.
          reportBounded(subscriptionRefusalLogGate, subscriptionRefusals,
            log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} refused consumer " +
              log"${MDC(SESSION_ID, session.consumerId)} a subscription to partition " +
              log"${MDC(PARTITION_ID, partitionId)}: it already serves " +
              log"${MDC(COUNT, subscriberCount(partitionId))} subscriber(s) of that partition, " +
              log"the ceiling of ${MDC(MAX_SIZE, MAX_SUBSCRIBERS_PER_PARTITION)}")
        } else if (session.subscribe(partitionId)) {
          openConsumerLedger(session, partitionId)
          resumeFrom(session, partitionId, heartbeat.sequenceNumber())
        } else {
          // The session refused the subscription itself -- it is closing -- so the slot claimed
          // just above is surplus and goes straight back.
          releaseSubscriberSlot(partitionId)
        }
        reportHeartbeat(session, heartbeat)
        // A drain unconditionally, and this is load bearing rather than tidy. Every consumer of a
        // completed map output subscribes *after* the producing task has ended -- the scheduler
        // starts no reduce task before its map stage finishes -- so this heartbeat is frequently
        // the only event that will ever occur on this stream, with no producer thread left to flush
        // anything. A pass here is what delivers the deferred end of stream markers for the two
        // cases [[resumeFrom]] leaves undrained: a partition this map produced nothing for, and a
        // consumer whose position already covers everything retained.
        drain()
        if (debugEnabled) {
          logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
            log"${MDC(PARTITION_ID, partitionId)} saw a heartbeat from consumer " +
            log"${MDC(SESSION_ID, session.consumerId)} stamped " +
            log"${MDC(VALUE, heartbeat.timestampMs())} at position " +
            log"${MDC(COUNT, heartbeat.sequenceNumber())}")
        }
      }
  }

  /**
   * Opens the credit ledger of one consumer's flow of one partition.
   *
   * Registration happens as the consumer subscribes and before anything is queued for it, because
   * admission for an unregistered stream fails open: the block would go on the wire uncharged,
   * unpaced and outside any window that could replay it. The credit limit is the per-partition
   * share of the producer's buffer budget, which is the honest figure -- the bytes a producer may
   * hold unacknowledged are exactly the bytes it is able to retain in order to replay them.
   *
   * A store that no longer serves this generation leaves the ledger at the framing cap rather than
   * refusing to open one. The stream cannot carry anything in that state, and a positive limit is
   * what keeps the refusal a pacing decision rather than a rejected registration.
   */
  private def openConsumerLedger(session: ConsumerSession, partitionId: Int): Unit = {
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
   * The consumer announces the <b>next</b> position it expects, the convention every heartbeat on
   * this protocol follows in both directions, so the position it has effectively confirmed is one
   * below that. Converting here rather than at the call site keeps the conversion in the one place
   * that reasons about retained windows, and means a consumer that has received nothing -- which
   * announces zero, because the message type permits no negative sequence number -- is correctly
   * read as having confirmed nothing and is served from block zero rather than from block one.
   *
   * That converted position is trusted only as a lower bound and is reconciled against three
   * authorities before anything is queued: the highest position this producer has actually
   * charged, which is the ceiling; the store's record of what that consumer has already
   * acknowledged, which a peer cannot talk its way past; and the store's record of what is still
   * retained. The higher of the announced and recorded positions is where delivery starts, so a
   * consumer cannot replay bytes it has already confirmed -- which would be a request to un-retire
   * released memory -- and the lower bound of the retained window is where delivery starts when the
   * consumer is further behind than that.
   *
   * <b>Why the announcement is clamped.</b> The announced position is a number chosen by the peer,
   * and it is used to seed both of this session's cursors. Seeded unclamped, a peer announcing a
   * position beyond anything produced would set its own sent and acknowledged bounds above the
   * charged high-water -- and those bounds are precisely what the acknowledgement path checks a
   * later frame against. The lower layers would still refuse the release, so this is defence in
   * depth rather than the only guard, but a bound a peer can inflate is not a bound.
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
    retainedOutput.filter { store =>
      val admitted = store.registerConsumer(session.consumerId)
      if (!admitted) {
        // The store no longer serves its output: the generation was withdrawn, or the shuffle was
        // unregistered, between this consumer's frame arriving and this line. Nothing can be queued
        // and nothing may be claimed on its behalf, so the subscription is left inert; the consumer
        // times its producer out and the unmodified scheduler recomputes the map task.
        reportBounded(subscriptionRefusalLogGate, subscriptionRefusals,
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
      if (highestRetained == MemorySpillManager.UNSET_SEQUENCE) {
        // Nothing has been produced for this partition yet; the subscription alone is enough, and
        // blocks will be fanned out to this session as they are admitted.
        session.advanceSent(partitionId, resumeAfter)
      } else if (lowestRetained != MemorySpillManager.UNSET_SEQUENCE &&
          lowestRetained > resumeAfter + 1L) {
        errorNotifier.setError(StreamingShuffleErrors.invalidSequenceNumber(
          shuffleId, partitionId, lowestRetained, resumeAfter + 1L))
        reportBounded(subscriptionRefusalLogGate, subscriptionRefusals,
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
      reportBounded(replayRefusalLogGate, replayRefusals,
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
      reportBounded(replayRefusalLogGate, replayRefusals,
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
    errorNotifier.setError(new SparkException(s"Streaming shuffle $shuffleId received protocol " +
      s"version $version from its consumer but this executor speaks " +
      s"${StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION}; the shuffle must fall back to the " +
      "sort based implementation."))
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} detected a protocol version " +
      log"mismatch: peer sent ${MDC(PROTOCOL_VERSION, version)} against " +
      log"${MDC(VERSION_NUM, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)}. " +
      log"${MDC(REASON, "streaming yields to sort based shuffle")}")
    closeSession(client)
  }

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
  private def admitForEgress(session: ConsumerSession, pending: PendingBlock): Boolean = {
    val key = consumerLedgerKey(pending.partitionId, session.consumerId)
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
  private def reportHeartbeat(session: ConsumerSession, heartbeat: HeartbeatMessage): Unit = {
    backpressure.onHeartbeat(
      consumerLedgerKey(heartbeat.partitionId(), session.consumerId), heartbeat)
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

  // Observers, read by the task thread
  //
  // The write metrics reporter documents that its methods are called on a single thread, so an
  // event-loop thread must never touch it. These counters exist so that the writer can read the
  // handler's progress on the task thread and forward it to the reporter from there.

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

  /** Acknowledgements that were valid but advanced nothing, so they refreshed no progress clock. */
  def duplicateAckCount: Long = duplicateAcks.get()

  /** Sessions released for going silent for longer than the ten-second liveness window. */
  def expiredSessionCount: Long = expiredSessions.get()

  /** Logical consumers unregistered for going silent for longer than the expiry window. */
  def expiredConsumerCount: Long = expiredConsumers.get()

  /** Sessions released because the same consumer declared its identity on a newer channel. */
  def supersededSessionCount: Long = supersededSessions.get()

  /** Channels closed for attempting to change the consumer identity of a live session. */
  def identityConflictCount: Long = identityConflicts.get()

  /**
   * Egress channels that closed while still owing their consumer bytes, either unacknowledged or
   * undelivered.
   *
   * Counted separately from an ordinary close, which every healthy consumer performs once and which
   * is therefore not a condition worth an operator's attention. This is the figure the producing
   * task reports in its own summary, so the total survives even when the individual warnings are
   * suppressed by their aggregation window.
   */
  def lossyChannelClosureCount: Long = lossyChannelClosures.get()

  /** How many channels have been refused because the session ceiling was already reached. */
  def refusedSessionCount: Long = refusedSessions.get()

  /**
   * Consumer-session slots currently claimed against the concurrency ceiling.
   *
   * Normally equal to [[sessionCount]], and deliberately reported separately: the two are
   * maintained by different mechanisms -- a counter claimed before insertion and the registry's own
   * size -- and a divergence between them is the observable signature of a teardown path that
   * forgot to return its slot, which would silently retire capacity for every later connection.
   */
  def claimedSessionSlots: Int = liveSessionSlots.get()

  /** Blocks delayed because this executor had no room for another transient framing copy. */
  def framingBudgetRefusalCount: Long = framingBudgetRefusals.get()

  /**
   * How many subscriptions have been refused, whether by the per-partition subscriber ceiling or by
   * a request this producer could not honour.
   *
   * Reported because a refusal is the one outcome of a subscription attempt that leaves no other
   * trace: the session survives, the channel stays open and the partition simply never streams to
   * that peer. An operator investigating a reduce task that is waiting on output has no other way
   * to distinguish "the producer refused you" from "the producer has nothing yet".
   */
  def subscriptionRefusalCount: Long = subscriptionRefusals.get()

  /** How many consumer identities have gone untracked because the ledger ceiling was reached. */
  def untrackedConsumerCount: Long = untrackedConsumers.get()

  /**
   * How many block references a queue ceiling has deferred onto an owed run.
   *
   * A non-zero figure is not a fault. It says a consumer's queue reached its bound and the producer
   * recorded what that consumer was still owed instead of growing the queue, which is the bounded
   * behaviour working. A figure that keeps climbing while output is not moving is the signal worth
   * acting on, and it is the consumer's acknowledgement rate rather than this ceiling that explains
   * it.
   */
  def deferredBlockCount: Long = deferredBlocks.get()

  /** Blocks this producer still owes its consumers but has not queued, across every session. */
  def owedBlockCount: Long = sumOverSessions(session => session.owedBlocks)

  /** Logical consumers this handler currently holds a session for. */
  def liveConsumerCount: Int = sessionsByConsumer.size()

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

  // Teardown

  /**
   * Retires this producer generation from every owner of it on this executor, exactly once.
   *
   * This is the subsystem's single cross-owner generation withdrawal, and every deliberate
   * withdrawal goes through it: the producing task's unsuccessful stop; the executor listener's
   * per-generation deregistration, which is how `unregisterShuffle` and the manager's own shutdown
   * reach it; and a generation the driver has retired, which the producing writer learns from its
   * coordinator heartbeat and turns into a task failure that arrives here. Having exactly one
   * operation is the point. Each owner was otherwise retired by whichever caller happened to know
   * about it, so a failed map task withdrew its retained output but left its routing entry behind
   * for the executor's whole life, and a driver-side invalidation reached no local owner at all.
   *
   * The order is routing, then publication, then this handler's own state, and each step is
   * contained so that one owner failing cannot leave the others held.
   *
   * It is deliberately '''not''' reached on the successful path. A successful map output stays
   * published and stays routed because a consumer is entitled to read it after the producing task
   * has ended -- whether it had already attached during production and is resuming, or is attaching
   * for the first time. Withdrawing on success would break both, and the second is the ordinary
   * case under the unmodified DAG scheduler, which submits a reduce stage only once its map stage
   * reports available output and which this feature may not modify (AAP 0.2.1, 0.2.2, 0.8.2 Tier
   * 1).
   *
   * @param reason short description of what is being recovered from, for the operator's log
   * @return whether this call performed the withdrawal, as opposed to finding it already done
   */
  def withdrawGeneration(reason: String): Boolean = {
    if (!withdrawn.compareAndSet(false, true)) {
      return false
    }
    // Routing first. Once no inbound frame can reach this handler, nothing can ask it for a block
    // whose bytes the next step unlinks, so a consumer that was mid-request sees its channel go
    // away -- "the producer is gone", which is what makes it invalidate its partial read -- rather
    // than an unservable-block reply about output that did exist.
    val routed = withdrawalStep("stop routing")(routes.withdrawRoute(shuffleId, mapId, this))
    // Then the publication, which unlinks the spill files behind it. The resolver qualifies the
    // withdrawal by task attempt id, so a straggling older generation cannot withdraw the output of
    // the attempt that replaced it.
    val published = withdrawalStep("withdraw the retained output")(
      blockResolver.unregisterProducer(shuffleId, mapId, taskAttemptId))
    // Last, this handler's own sessions, queues and consumer ledgers. Reached whether or not a
    // route was found, because a generation whose route was never installed -- an announcement the
    // driver declined, a hand-off that was refused -- still holds all of this.
    releaseAll()
    logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} withdrew the producer generation " +
      log"of map ${MDC(MAP_ID, mapId)} attempt ${MDC(TASK_ATTEMPT_ID, taskAttemptId)} because " +
      log"${MDC(REASON, reason)}; routing withdrawn: ${MDC(STATUS, routed)}, retained output " +
      log"withdrawn: ${MDC(STATUS, published)}")
    true
  }

  /**
   * Runs one withdrawal step, reporting rather than propagating a failure.
   *
   * Every caller of [[withdrawGeneration]] is already handling a failure or ending a task, and the
   * steps are independent owners: abandoning the remaining owners on the first one to fail would
   * leak exactly what the withdrawal exists to release.
   */
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

  /**
   * Releases every queued reference and every consumer session, exactly once.
   *
   * This is the counterpart of the writer's unsuccessful stop and of its task completion listener:
   * once nothing this handler could serve remains published, nothing may be queued or scheduled on
   * it again. A successful producer deliberately does *not* come here when its task ends -- its
   * output stays published, and its consumers are entitled to keep reading it whether they attached
   * during production or after it, which under the unmodified DAG scheduler is usually after it --
   * so the callers are failure, cancellation, a refused hand-off and a superseded generation.
   * Channel loss deliberately does not come here either, because a consumer that reconnects must
   * still be replayable.
   *
   * The order matters. The close transition is latched first, so no thread can queue a block,
   * schedule a refill wake-up or start a drain pass after this point; only then are the sessions
   * closed and their queues emptied. Latching first is also what makes the method idempotent: a
   * second caller -- and there are three plausible ones, arriving concurrently -- returns without
   * touching anything.
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
      sessions.values().asScala.foreach { session =>
        releaseConsumerLedgers(session)
        session.close()
      }
      sessions.clear()
      sessionsByConsumer.clear()
      // The two accounted ceilings are cleared with the registries they account for. They are
      // counters rather than derived sizes, so clearing the maps alone would leave this handler
      // believing it still served every session it ever admitted.
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
   * The session opens under a '''provisional''' identity taken from the transport's authenticated
   * client id when there is one and from the socket when authentication is off. That is the honest
   * binding available at this instant: with `spark.authenticate` on, the identity has been
   * established by SASL before this handler sees a single frame, and with it off nothing stronger
   * exists to bind to than the connection. It is provisional because neither form is stable across
   * connections, and stability is what a resumption needs -- so the consumer's own logical identity
   * is adopted from its first heartbeat by [[adoptIdentity]], which is the frame that can carry it.
   *
   * Either way a session may only ever affect the partitions it subscribed to on its own channel,
   * which is the invariant that does not depend on the operator's authentication choice.
   *
   * <b>Why creation is capped.</b> A session exists because a peer sent a frame, so the number of
   * them is a quantity the peer chooses. Each one carries a queue, a subscription map and a set of
   * credit ledgers, so a peer connecting in a loop would consume the executor's heap through a path
   * that had no ceiling at all. Past [[MAX_CONCURRENT_SESSIONS]] the connection is refused and
   * closed instead, which is the difference between a bounded rejection and an exhausted executor.
   *
   * @return the session, or `None` when the ceiling was reached and the channel has been closed
   */
  private def sessionFor(client: TransportClient): Option[ConsumerSession] = {
    val key = sessionKeyOf(client)
    val existing = sessions.get(key)
    if (existing != null) {
      Some(existing)
    } else if (!claimSessionSlot()) {
      // The slot is claimed before the session is built, and that ordering is the whole of the
      // ceiling. Reading the map's size and then inserting is two steps, so every thread in a burst
      // of connections can read a size below the ceiling and then insert past it -- which makes the
      // ceiling a suggestion under exactly the load it exists for. Claiming a slot with a
      // compare-and-set admits one thread per slot and no more, whatever the arrival pattern.
      refusedSessions.incrementAndGet()
      if (sessionCapacityReported.compareAndSet(false, true)) {
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} refused an egress channel " +
          log"from ${MDC(HOST_PORT, client.getSocketAddress())}: it already serves " +
          log"${MDC(COUNT, liveSessionSlots.get())} consumer session(s), the ceiling of " +
          log"${MDC(MAX_SIZE, MAX_CONCURRENT_SESSIONS)}. Further refusals are counted but not " +
          log"logged")
      }
      closeSession(client)
      None
    } else {
      // The client id is the peer's authenticated application identity when the application has
      // authentication enabled, and a value the peer supplied in its handshake when it does not. It
      // is sanitized either way, and for the same reason the protocol refuses control characters in
      // a declared consumer identity: this string names the consumer in this producer's log
      // records, and a record separator inside it would end a line early and start one whose whole
      // content the peer chose. Sanitizing rather than refusing, because a session must still be
      // created for a peer whose handshake identity is odd -- refusing would turn a cosmetic
      // anomaly into an unservable consumer -- and because the socket address that follows it keeps
      // the result unique.
      val provisional = Option(client.getClientId())
        .map(sanitizedIdentity)
        .filter(_.nonEmpty)
        .map(id => s"$id@${client.getSocketAddress()}")
        .getOrElse(s"anonymous@${client.getSocketAddress()}")
      val created =
        new ConsumerSession(key, provisional, client.getChannel(), clock.getTimeMillis())
      val raced = sessions.putIfAbsent(key, created)
      if (raced != null) {
        // Another thread built the session for this same channel first. The slot this thread
        // claimed is therefore surplus and must go back at once, or a channel that two callbacks
        // reached simultaneously would consume two slots for the one session it created.
        releaseSessionSlot()
        Some(raced)
      } else {
        Some(created)
      }
    }
  }

  /**
   * Claims one of this map output's consumer-session slots, or refuses.
   *
   * A compare-and-set loop rather than a size check followed by an insertion, because those are two
   * steps and a burst of connections all pass the first before any of them reaches the second -- so
   * the ceiling would be exceeded by exactly the arrival pattern it exists to bound. The claim
   * admits one thread per slot whatever the pattern, and every path that removes a session returns
   * its slot through [[releaseSessionSlot]].
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

  /**
   * Returns one consumer-session slot.
   *
   * Floored at zero rather than allowed to go negative, because a negative reading would silently
   * raise the ceiling for every later connection -- turning one accounting slip into an unbounded
   * one. Every teardown path routes through [[forgetSession]], so there is exactly one place a slot
   * comes back from and it cannot be forgotten by a new caller.
   */
  private def releaseSessionSlot(): Unit = {
    liveSessionSlots.updateAndGet(current => math.max(0, current - 1))
  }

  /**
   * Removes one session from both registries and returns everything it held.
   *
   * The single teardown point, and it has to be single: a session holds a slot in the concurrency
   * ceiling and a subscriber slot in each partition it subscribed to, and both are accounted rather
   * than derived, so a removal path that forgot either would leak a ceiling. Idempotent in the
   * session, because several paths can race to release the same one -- an inactive channel, a
   * superseded reconnection, an expiry sweep and the handler's own shutdown.
   *
   * @param session the session to forget
   * @return true when this call was the one that removed it
   */
  private def forgetSession(session: ConsumerSession): Boolean = {
    val removed = sessions.remove(session.sessionKey, session)
    if (removed) {
      releaseSessionSlot()
      session.subscribedPartitions.foreach(releaseSubscriberSlot)
    }
    sessionsByConsumer.remove(session.consumerId, session)
    removed
  }

  /**
   * Claims one subscriber slot on one partition, or refuses.
   *
   * <b>Why partitions are bounded separately from sessions.</b> A session's own subscription map is
   * bounded by the partition count, and the number of sessions is bounded by
   * [[MAX_CONCURRENT_SESSIONS]] -- but the product of the two is what decides how much per-stream
   * state one partition can be made to carry, and a partition is legitimately read by exactly one
   * reduce task. A peer that opened many sessions and subscribed every one of them to the same
   * partition would multiply that partition's credit ledgers, deferred-termination records and
   * queue entries by the session count while staying inside both existing ceilings. Bounding the
   * subscribers of a partition closes that product, and it does so *before* any payload is copied,
   * because a refused subscription never reaches the drain at all.
   *
   * The ceiling is generous against the legitimate case: one reduce task per partition, plus room
   * for the reconnections a genuine consumer makes while its previous session is still within the
   * expiry window.
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

  /**
   * Records that one logical consumer was heard from, if the liveness ledger has room for it.
   *
   * The ledger is keyed by an identity the consumer declares, which makes its size another quantity
   * a peer chooses rather than one this producer controls. Past [[MAX_TRACKED_CONSUMERS]] a new
   * identity is not tracked, and that is a bounded loss rather than a correctness one: the consumer
   * is still served, and its own session's stall detector still releases it. What it forgoes is
   * only the second, longer sweep that unregisters a consumer which never returns -- and a peer
   * churning through enough identities to reach this ceiling is not one that is coming back.
   */
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
   * Binds one session to the logical identity its consumer declares, once.
   *
   * <b>Why the consumer names itself rather than being named.</b> Everything about a connection
   * changes when a reduce task reconnects -- the channel, the ephemeral port, and therefore any
   * identity derived from either -- while the entitlement being resumed belongs to the reduce task,
   * which has not changed at all. A cursor keyed by the connection is consequently a cursor that
   * is abandoned by the very event it exists to survive. The consumer's heartbeat therefore carries
   * the one identifier that is stable across its connections, and this is where that identifier
   * becomes the key the retained store's cursors and this handler's ledgers are held under.
   *
   * Three outcomes, and each is a decision rather than a default:
   *
   *  - <b>Nothing declared.</b> A peer that names no identity keeps its provisional one. It is
   *    then a fresh consumer on every connection, which is the prior behaviour and is safe: it can
   *    never adopt another consumer's cursor, it merely cannot resume against its own.
   *  - <b>First declaration.</b> Adopted, and recorded as the session now serving that consumer.
   *    Any session still registered for the same identity on a different channel is stale by
   *    construction -- one reduce task attempt reads through one connection at a time -- and is
   *    evicted, which is what stops a superseded connection from continuing to hold credit, queue
   *    blocks and pin a retained window.
   *  - <b>A second, different declaration on the same channel.</b> Refused, and the channel closed.
   *    A session that could be renamed mid-flight could be talked into adopting the cursors, and
   *    therefore the unread output, of a consumer that is not the peer holding the socket.
   *
   * @return true when the session may go on to subscribe, false when the channel has been closed
   */
  private def adoptIdentity(
      client: TransportClient,
      session: ConsumerSession,
      declared: String): Boolean = {
    if (declared == null || declared.isEmpty || declared == session.consumerId) {
      trackConsumer(session.consumerId, clock.getTimeMillis())
      true
    } else if (!session.declareIdentity(declared)) {
      identityConflicts.incrementAndGet()
      misaddressedMessages.incrementAndGet()
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} refused a consumer identity " +
        log"change from ${MDC(SESSION_ID, session.consumerId)} to " +
        log"${MDC(DESCRIPTION, declared)} on the channel from " +
        log"${MDC(HOST_PORT, client.getSocketAddress())}: " +
        log"${MDC(REASON, "a session may declare its identity once")}; closing it")
      closeSession(client)
      false
    } else {
      val superseded = sessionsByConsumer.put(declared, session)
      trackConsumer(declared, clock.getTimeMillis())
      if (superseded != null && superseded.ne(session)) {
        supersededSessions.incrementAndGet()
        evictSession(superseded)
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} superseded the egress " +
          log"session of consumer ${MDC(SESSION_ID, declared)}: it has reconnected on a new " +
          log"channel, so the previous one has been released and its output will be replayed")
      }
      true
    }
  }

  /**
   * Replaces every character that could break a log record with a single visible substitute.
   *
   * Applied to the one identity this producer derives rather than receives -- the transport
   * handshake identity -- because every identity that arrives on the wire is already refused at the
   * protocol boundary by `HeartbeatMessage`'s own validation. The two together mean no consumer
   * identity holding a record separator can exist anywhere in this subsystem, which is a stronger
   * guarantee than sanitising at each of the many sites that log one: a sanitiser omitted at a
   * single site would reinstate the whole problem, and there is no correct value for a sanitiser to
   * miss here.
   *
   * The substitute is a single question mark per offending character rather than an escape
   * sequence, so the result stays a stable map key of predictable length and two identities
   * differing only in their forbidden characters remain distinguishable from one that had them
   * removed.
   *
   * @param identity the identity to render safe, which may be empty
   * @return the identity with every control character, delete and Unicode line or paragraph
   *         separator replaced
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

  /** Closes one consumer's session and its channel, leaving every other consumer untouched. */
  private def closeSession(client: TransportClient): Unit = {
    val session = sessions.get(sessionKeyOf(client))
    if (session != null && forgetSession(session)) {
      releaseConsumerLedgers(session)
      session.close()
    }
    client.getChannel().close()
  }

  /**
   * Releases one session and the channel behind it, without going through a `TransportClient`.
   *
   * The eviction paths -- a superseded connection and an expired one -- hold a session rather than
   * the client that created it, so the teardown has to be reachable from the session alone. The
   * channel is closed as well as the session released, because a session whose queue has been
   * emptied would otherwise leave a socket open that nothing serves.
   */
  private def evictSession(session: ConsumerSession): Unit = {
    forgetSession(session)
    releaseConsumerLedgers(session)
    session.close()
    try {
      session.channel.close()
    } catch {
      case NonFatal(e) =>
        if (debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not close the " +
            log"channel of consumer ${MDC(SESSION_ID, session.consumerId)}: " +
            log"${MDC(REASON, e.getMessage())}")
        }
    }
  }

  /**
   * Closes the per-consumer credit ledgers one departing session owns.
   *
   * A ledger holds credit, a retry budget and an unacknowledged window, all of which describe a
   * flow that has ended. Leaving them open would keep an executor-wide map growing with one entry
   * per partition per connection ever made, and would let a stale window answer a question about a
   * live one. The retained *bytes* are untouched: they belong to the store, and a consumer that
   * reconnects is served from them.
   */
  private def releaseConsumerLedgers(session: ConsumerSession): Unit = {
    val consumerId = session.consumerId
    session.subscribedPartitions.foreach { partitionId =>
      backpressure.unregisterStream(consumerLedgerKey(partitionId, consumerId))
    }
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

  /**
   * How long a consumer may be silent before it is unregistered outright, in milliseconds.
   *
   * The liveness window plus the whole replay budget. The liveness window says one connection has
   * stopped answering, and the prescribed response to that is to retain the consumer's window and
   * replay it when it returns -- so that instant cannot also be the moment the consumer is
   * forgotten. Adding the sum of the five backoffs the failure protocol would have spent trying to
   * repair the stream gives the instant after which no repair can still be outstanding, which is
   * the earliest honest point at which nothing is left to hold open on that consumer's behalf.
   */
  val CONSUMER_EXPIRY_TIMEOUT_MS: Long =
    CONSUMER_LIVENESS_TIMEOUT_MS +
      RETRANSMIT_INITIAL_BACKOFF_MS * ((1L << MAX_RETRANSMIT_ATTEMPTS) - 1L)

  /** Starting size of the egress queue; it grows up to [[MAX_QUEUED_BLOCKS_PER_SESSION]]. */
  val INITIAL_EGRESS_QUEUE_CAPACITY: Int = 16

  /**
   * Most consumer sessions one map output's egress may hold at once.
   *
   * A session is created by the arrival of a heartbeat on a connection, which makes the number of
   * them a quantity a remote peer chooses rather than one this producer controls: with no ceiling,
   * a peer that opens connections in a loop grows a queue, a subscription map and a set of credit
   * ledgers on each one until the executor's heap is gone. The ceiling is what turns that from a
   * denial of service into a refused connection.
   *
   * The value is generous against every legitimate use and tight against the abusive one. One map
   * output is read by one reduce task per partition, each over one connection, so the honest demand
   * is the reduce-side partition count -- and a reduce stage of four thousand tasks all reading one
   * map task's output is already far outside the shape of workload streaming shuffle is chosen for.
   * A peer opening more connections than there are reduce partitions is, by that arithmetic, not a
   * reduce task. Subscriptions within a session are separately bounded, to the partition count the
   * retained store was told, so the two ceilings together bound the whole of the per-peer state.
   */
  val MAX_CONCURRENT_SESSIONS: Int = 4096

  /**
   * Most bytes of transient egress framing this executor may have in flight at once, across every
   * map output it is producing and every consumer session on each of them.
   *
   * <b>Why an executor-wide ceiling is required and per-channel water marks are not enough.</b> A
   * Netty write water mark bounds one channel: a caller that writes only while `Channel.isWritable`
   * holds cannot accumulate copies for a single slow consumer without limit. But the ceilings on
   * consumers are per map output -- [[MAX_CONCURRENT_SESSIONS]] sessions each -- and an executor
   * produces many map outputs, so per-channel bounds multiply. Thousands of sessions each holding
   * one high-water mark's worth of transient copies is an aggregate that no per-channel limit
   * constrains, and a transient copy is up to a whole block, which is what makes the aggregate the
   * quantity that has to be bounded.
   *
   * The value is thirty-two blocks' worth of framing. That is deliberately modest against the heap
   * of any executor streaming shuffle would be enabled on, and generous against the work it bounds:
   * a copy exists only from the moment a frame is built to the moment the transport is done with
   * it, so thirty-two concurrent copies is thirty-two writes genuinely in the socket layer at the
   * same instant, not thirty-two consumers. It is expressed as a multiple of the block cap rather
   * than as an absolute figure so that it tracks the protocol's own framing size if that ever
   * changes.
   *
   * Reaching it delays a block; it never drops one. A refused block is returned to its session's
   * queue exactly as a pacing refusal returns it, and the same refill wake-up retries it.
   */
  val MAX_IN_FLIGHT_EGRESS_BYTES: Long = 32L * MAX_FRAMED_BYTES

  /**
   * The executor-wide ceiling on transient egress framing, as a live reservation ledger.
   *
   * An `object` because the quantity is executor wide: one JVM produces many map outputs, each with
   * its own handler, and a budget held per handler would bound nothing that needed bounding. It is
   * lock free and allocation free on both paths, because both run on Netty event-loop threads where
   * a lock would stall a socket and an allocation per block would be an allocation per two
   * mebibytes.
   */
  object EgressFramingBudget {

    private val reserved = new AtomicLong(0L)

    private val refusals = new AtomicLong(0L)

    /**
     * Takes room for one framing copy, or refuses.
     *
     * A compare-and-set loop rather than an unconditional add, because an add that overshot and
     * then corrected itself would let two threads both build a copy the budget has room for only
     * one of -- which is precisely the aggregate this ceiling exists to prevent. Non-negative
     * requests of zero are admitted without touching the ledger, so a caller need not special-case
     * them.
     *
     * @param bytes framed bytes the copy will occupy
     * @return true when the reservation was taken and the caller owns its release
     */
    def tryReserve(bytes: Long): Boolean = {
      if (bytes <= 0L) {
        true
      } else {
        var current = reserved.get()
        var settled = false
        var granted = false
        while (!settled) {
          val candidate = current + bytes
          if (candidate > MAX_IN_FLIGHT_EGRESS_BYTES) {
            refusals.incrementAndGet()
            settled = true
          } else if (reserved.compareAndSet(current, candidate)) {
            granted = true
            settled = true
          } else {
            current = reserved.get()
          }
        }
        granted
      }
    }

    /**
     * Returns a reservation.
     *
     * Floored at zero rather than allowed to go negative, because a ledger that could read negative
     * would silently raise the effective ceiling for everyone else -- turning one accounting
     * mistake into an unbounded one. A release that would underflow is a defect in a caller, and
     * clamping keeps the ceiling honest while that caller is found.
     *
     * @param bytes framed bytes to return
     */
    def release(bytes: Long): Unit = {
      if (bytes > 0L) {
        reserved.updateAndGet(current => math.max(0L, current - bytes))
      }
    }

    /**
     * A channel listener that returns one reservation when the transport is done with its copy.
     *
     * The listener is what makes the budget a measure of bytes genuinely in flight rather than of
     * bytes some method has returned from: a `Channel.write` completes on an event-loop thread long
     * after the call that issued it, and the copy occupies the executor for exactly that interval.
     * It fires on success, on failure and on cancellation alike, which is what stops a failing
     * consumer from retiring the executor's whole allowance one block at a time.
     *
     * @param bytes framed bytes to return once the write completes
     * @return the listener to attach to the write future
     */
    def releaseOn(bytes: Long): ChannelFutureListener = new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture): Unit = release(bytes)
    }

    /** Framed bytes currently reserved for copies in flight across this executor. */
    def inFlight: Long = reserved.get()

    /** The ceiling, restated here so an operator-facing diagnostic reads both from one place. */
    def capacity: Long = MAX_IN_FLIGHT_EGRESS_BYTES

    /** Blocks delayed because the executor had no room for another framing copy. */
    def refusalCount: Long = refusals.get()

    /**
     * Clears the ledger, for a test that must assert against a known baseline.
     *
     * The state is JVM wide, so a suite that measured it without resetting would be measuring
     * whatever ran before it. Never called from production code, which has no reason to forget a
     * reservation it still owes.
     */
    private[streaming] def resetForTesting(): Unit = {
      reserved.set(0L)
      refusals.set(0L)
    }
  }

  /**
   * Most logical consumer identities the liveness ledger tracks at once.
   *
   * The ledger is keyed by an identity the consumer declares, so its size is likewise a quantity
   * the peer chooses: a peer declaring a fresh identity on every connection would otherwise add an
   * entry per connection and hold each one for the whole expiry window. Twice the session ceiling
   * leaves room for the reconnections a genuine consumer makes -- its previous session is gone
   * while its identity is still within the expiry window -- and stops there.
   *
   * Declining to track an identity is a bounded loss rather than a correctness one: that consumer
   * is still tracked by its own session, whose stall detector releases it, and is still served
   * from the retained store. What it loses is only the second, longer sweep that would eventually
   * unregister it outright.
   */
  val MAX_TRACKED_CONSUMERS: Int = 2 * MAX_CONCURRENT_SESSIONS

  /**
   * Most sessions that may be subscribed to one reduce partition of one map output at a time.
   *
   * The two existing ceilings bound the number of sessions and the width of one session's
   * subscription map, but neither bounds their product -- and it is the product that decides how
   * much per-stream state one partition can be made to carry. A peer that opened many connections
   * and subscribed every one of them to the same partition would multiply that partition's credit
   * ledgers, its deferred-termination records and its queue entries by the session count while
   * staying comfortably inside both of them.
   *
   * Eight, because the legitimate figure is one: a reduce partition is read by exactly one reduce
   * task. The remaining seven are headroom for the reconnections a genuine consumer makes -- a task
   * whose channel dropped reconnects while its previous session is still inside the expiry window,
   * and a speculative attempt of the same reduce task reads the same partition legitimately. A peer
   * needing more than eight simultaneous subscriptions to one partition is not a reduce task.
   *
   * A refused subscription is refused before a ledger, a queue entry or a payload copy exists,
   * which is what makes this a bound on work as well as on state.
   */
  val MAX_SUBSCRIBERS_PER_PARTITION: Int = 8

  /**
   * U+2028 LINE SEPARATOR, which ends a line for many readers and log pipelines.
   *
   * Named as a code point rather than written as a character literal because a literal for a
   * non-ASCII character is a non-ASCII character in the source file, which the style gate refuses
   * -- and because the name says which character it is, where the escape would not.
   */
  val UNICODE_LINE_SEPARATOR: Int = 0x2028

  /** U+2029 PARAGRAPH SEPARATOR, a line break to the same readers, named for the same reason. */
  val UNICODE_PARAGRAPH_SEPARATOR: Int = 0x2029

  /**
   * Most block references one consumer's egress queue may hold at once.
   *
   * The queue holds references, not payloads -- every queued block's bytes live in the retained
   * store, charged once against the buffer budget -- so this bounds metadata rather than output.
   * It still needs bounding: a consumer that subscribes and then stops reading has its queue grow
   * by one reference per block the producer goes on to admit, and a producer streaming a large
   * partition admits a great many.
   *
   * A refused reference is <b>not</b> a dropped block. The block stays retained, and the sequence
   * number is recorded as owed to that consumer, so the queue is topped up from the owed run as it
   * drains. The cap therefore decides how far ahead of the wire the reference list may run, and
   * nothing about what is eventually delivered.
   */
  val MAX_QUEUED_BLOCKS_PER_SESSION: Int = 1024

  /**
   * Most framed bytes one consumer's egress queue may reference at once.
   *
   * The companion bound to [[MAX_QUEUED_BLOCKS_PER_SESSION]], and whichever binds first is the one
   * that applies: a stream of full-sized blocks reaches this figure at a hundred and twenty-eight
   * references, while a stream of small ones reaches the reference ceiling long before it. Two
   * bounds rather than one because a single reference ceiling would let a queue of maximum-sized
   * blocks reference two gibibytes of pending output, which is a pacing decision nobody made.
   */
  val MAX_QUEUED_BYTES_PER_SESSION: Long = 256L * 1024L * 1024L

  /**
   * Most owed blocks one top-up may queue for one partition of one consumer.
   *
   * A resume is the unbounded case this exists for. A consumer reconnecting after a long absence is
   * owed everything between its acknowledged position and the producer's high-water mark, which for
   * a large partition is thousands of blocks; queueing that range in one pass would materialise
   * thousands of references, and measuring each block would have cost a disk read apiece before
   * [[MemorySpillManager.retainedPayloadLength]] existed. Paging replaces the single unbounded
   * batch with a run of bounded ones, each topped up by the drain loop as the previous one leaves,
   * so the reference list stays small however long the owed run is.
   *
   * The explicit retransmission path is separately bounded by the protocol itself, whose request
   * message caps a range at four thousand and ninety-six blocks; paging applies to it too, so the
   * two bounds compose rather than one substituting for the other.
   */
  val REPLAY_PAGE_BLOCKS: Int = 64

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
   * How long a drain waits before retrying a block the executor-wide framing ceiling refused.
   *
   * Unlike pacing, this ceiling is released by write completions rather than by the passage of
   * time, so there is no instant a bucket can name and the wait has to be chosen. It is chosen
   * coarse: the ceiling is measured in mebibytes and clears as soon as the sockets already holding
   * it accept their bytes, so retrying sooner would burn passes for nothing, and retrying later
   * would idle a link that had become free. Twenty milliseconds is two orders of magnitude below
   * the consumer liveness window, so no consumer can time a producer out across one of these waits.
   */
  val FRAMING_BUDGET_RETRY_WAIT_MS: Long = 20L

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
   * assigns -- which is also how many blocks it has produced, since one counter answers both -- and
   * whether it has declared the stream complete.
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

    /**
     * Next sequence number to assign, counted from zero and increasing by one per accepted block.
     *
     * Advanced only once a block has passed every admission check, so its value is simultaneously
     * the number the next block will carry and the number of blocks this partition has produced --
     * which is the total the terminator reports. Keeping one counter for both is what makes them
     * incapable of disagreeing.
     */
    val nextSequenceNumber: AtomicLong = new AtomicLong(0L)

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
   * @param provisionalId the identity to answer with until the consumer declares its own, taken
   *                      from the transport's authenticated client id where there is one
   * @param channel the consumer's channel, used for writes and for writability
   * @param createdAtMs the instant the session was opened, which seeds its activity stamps
   */
  private final class ConsumerSession(
      val sessionKey: String,
      provisionalId: String,
      val channel: Channel,
      createdAtMs: Long) {

    /**
     * The identity this session's cursors are keyed by.
     *
     * Provisional until the consumer declares one on a heartbeat, and settled from then on. It has
     * to be settleable rather than fixed at construction because the identity that matters is the
     * consumer's own logical one -- stable across the connections a reduce task makes -- and the
     * first frame that can carry it is the first frame the consumer sends, which arrives after the
     * session exists. It has to be settleable exactly '''once''' because a session whose identity
     * could change mid-flight could be talked into adopting another consumer's cursors.
     */
    private val identity: AtomicReference[String] = new AtomicReference[String](provisionalId)

    /** Latched when a declared identity has been adopted, so a second, different one is refused. */
    private val identityDeclared: AtomicBoolean = new AtomicBoolean(false)

    /** The identity this session's retained-store cursors and ledgers are keyed by. */
    def consumerId: String = identity.get()

    /** Whether this session is keyed by an identity the consumer itself declared. */
    def hasDeclaredIdentity: Boolean = identityDeclared.get()

    /**
     * Adopts the identity a consumer declares on its heartbeat.
     *
     * @return true when the session now answers to `declared`, false when it had already adopted a
     *         different identity -- a mid-session identity change, which is refused
     */
    def declareIdentity(declared: String): Boolean = {
      if (identityDeclared.compareAndSet(false, true)) {
        identity.set(declared)
        true
      } else {
        identity.get() == declared
      }
    }

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

    /**
     * The partitions this consumer is currently owed blocks for, as a set.
     *
     * An index rather than a convenience. The drain loop tops up owed blocks every time a queue
     * empties, which is once per drain pass, and a pass happens for every session on every block
     * this producer enqueues -- so a scan of every subscription on every pass would make the cost
     * of writing one block a product of the consumer count and the partition count, for a set
     * that is empty in the ordinary case. Consulting this instead makes the ordinary case free.
     *
     * Maintained conservatively: entries are added when a run is recorded and removed only after
     * re-reading the run, so the set may briefly name a partition whose run has just emptied but
     * never omits one whose run has not.
     */
    private val owedPartitions: ConcurrentHashMap[Int, java.lang.Boolean] =
      new ConcurrentHashMap[Int, java.lang.Boolean]()

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
     * The partitions this consumer subscribed to, so that a teardown can release exactly the
     * per-consumer ledgers this session opened and no others.
     */
    def subscribedPartitions: Seq[Int] =
      subscriptions.keySet().asScala.toSeq.map(_.intValue()).sorted

    /**
     * Queues one block reference for this consumer, if the queue has room for it.
     *
     * Refused for a partition this consumer never subscribed to, which is what keeps fan-out from
     * accumulating state for a peer that never asked for the data, and refused when the queue is at
     * either of its ceilings -- [[MAX_QUEUED_BLOCKS_PER_SESSION]] references or
     * [[MAX_QUEUED_BYTES_PER_SESSION]] framed bytes.
     *
     * <b>A refusal is a deferral, and the caller must treat it as one.</b> The block's bytes are in
     * the retained store either way, so nothing is lost by not queueing a reference to them; what
     * would be lost is the *knowledge* that this consumer is still owed the block. Every caller
     * therefore records a refused sequence number on the owed run through [[deferOwed]], and
     * [[topUpOwed]] queues it once the queue has drained enough to hold it.
     *
     * The accounting is incremented only on success. Charging a refused reference would leave the
     * queue permanently appearing fuller than it is, which would turn one refusal into a ceiling
     * that never lifts.
     *
     * @return true if the reference was queued
     */
    def offer(pending: PendingBlock): Boolean = {
      val subscription = subscriptions.get(pending.partitionId)
      if (isClosed || subscription == null || !hasQueueCapacity(pending.framedBytes)) {
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

    /**
     * Whether the queue can hold one more reference of the given framed size.
     *
     * Read rather than reserved, so two threads offering at once can momentarily overshoot by the
     * blocks they are each holding. That is deliberate: the alternative is a lock on the hot egress
     * path to enforce a ceiling whose purpose is to bound an order of magnitude, not a unit. The
     * overshoot is at most one reference per concurrently offering thread.
     */
    def hasQueueCapacity(framedBytes: Int): Boolean = {
      queuedBlockCount.get() < MAX_QUEUED_BLOCKS_PER_SESSION &&
        queuedByteCount.get() + framedBytes.toLong <= MAX_QUEUED_BYTES_PER_SESSION
    }

    /**
     * Records that this consumer is owed an inclusive run of blocks it has not been queued.
     *
     * The recorded run is the union of what was already owed and what is being added, taken as a
     * range: the lower of the two starts and the higher of the two ends. Union rather than
     * replacement because a consumer can accumulate owings from two directions at once -- a resume
     * owing an old range while the producer goes on admitting new blocks that the queue ceiling
     * refuses -- and either of those forgetting the other would strand output.
     *
     * Taking the range rather than the exact set can only ever *over*-state what is owed, never
     * understate it, and an over-statement is self-correcting: a top-up that reaches a sequence
     * number the consumer has already been sent and acknowledged finds nothing retained for it and
     * skips it, which is the same answer the store gives for any block it no longer holds.
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
     * @return the sequence number, or [[MemorySpillManager.UNSET_SEQUENCE]] when nothing is owed
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

    /**
     * Drops everything at or below one position from the owed run of one partition.
     *
     * The counterpart to [[deferOwed]]'s union. Recording an owed run as a range can over-state
     * it -- two owings on one partition merge into the range spanning both, which can cover
     * positions in between that were delivered and confirmed long ago -- and this removes the part
     * of the over-statement that is provably settled. One step rather than a scan, because an
     * acknowledged position is a confirmed prefix by construction.
     *
     * Discarding is not merely tidying. A queued block that this consumer has already acknowledged
     * would be charged against its credit window on the way out and then dropped as stale, so its
     * charge would never be released by an acknowledgement -- and enough of them would fill the
     * window with charges for blocks nobody is waiting for.
     */
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
     * Drops one partition from the owed index if its run has in fact emptied.
     *
     * Removed first and then re-added on a re-read, rather than tested and then removed. That order
     * is what makes the index safe against a [[deferOwed]] running concurrently: a record written
     * between the removal and the re-read is seen by the re-read, and one written after the re-read
     * indexes the partition itself.
     */
    def deindexIfSettled(partitionId: Int): Unit = {
      owedPartitions.remove(partitionId)
      if (owedBlocksFor(partitionId) > 0L) {
        owedPartitions.put(partitionId, java.lang.Boolean.TRUE)
      }
    }

    /** The partitions this consumer may still be owed blocks for; empty in the ordinary case. */
    def owedPartitionIds: Seq[Int] =
      owedPartitions.keySet().asScala.toSeq.map(_.intValue()).sorted

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

    /**
     * Claims one acknowledgement as the newest applied for its partition, or refuses it as stale.
     *
     * The freshness test is `AckMessage.supersedes` against
     * [[Subscription.appliedControlSequence]] -- the shared predicate applied at the boundary, on
     * the number space its contract names -- and the claim is a compare-and-set, so of two
     * deliveries reporting the same control number exactly one is applied however they interleave.
     * Combining the test with the advance is what makes the gate idempotent rather than merely
     * monotonic: a check followed by a separate advance could admit a duplicate between them.
     *
     * @param partitionId the stream the acknowledgement concerns
     * @param ack the acknowledgement, whose control sequence number decides the outcome
     * @return true when this call is the one that applied it; false for a duplicate, a reordered
     *         delivery, or a partition this consumer is not subscribed to
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
     *
     * A subscription that has already gone answers false, because there is nobody left to tell.
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

    /**
     * Whether this partition's stream has been ended for this consumer because a block it was owed
     * could not be produced. One-shot, so the escalation is reported once however many blocks of
     * the same lost run are drained afterwards.
     */
    val streamAborted: AtomicBoolean = new AtomicBoolean(false)

    /** Highest sequence number this consumer has acknowledged, or nothing consumed. */
    val ackPosition: AtomicLong = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    /**
     * The highest <em>control</em> sequence number of an acknowledgement already applied here.
     *
     * <b>A second cursor, because there are two number spaces and they are not
     * interchangeable.</b> [[ackPosition]] counts data blocks travelling towards the consumer;
     * this counts the consumer's own outbound acknowledgements, which it numbers from zero and
     * increments once per message. One acknowledgement routinely covers many blocks, so the
     * control number advances strictly more slowly than the position -- and after a reconnection
     * it restarts at zero while the position is seeded at the resume point. Deciding freshness
     * against the position would therefore reject every acknowledgement whose control number had
     * not yet overtaken it: most of them in the batching case and all of them after a resume, so
     * the producer would reclaim nothing beyond the first acknowledgement and the stream would
     * throttle itself to a halt. `AckMessage.supersedes` says exactly this in its own contract,
     * and this field is what lets that contract be honoured at the boundary.
     *
     * Per subscription, and therefore per channel, which is precisely the scope the consumer's
     * counter has: a reconnection produces a fresh session and so a fresh cursor at
     * `AckMessage.NOTHING_CONSUMED`, matching the counter the new connection starts from.
     */
    val appliedControlSequence: AtomicLong = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    /** Framed bytes written to this consumer and not yet acknowledged. */
    val unacknowledgedBytes: AtomicLong = new AtomicLong(0L)

    /** One-shot guard so the terminator is written to this consumer at most once at a time. */
    val terminationClaimed: AtomicBoolean = new AtomicBoolean(false)

    /** Set once the terminator has been confirmed onto this consumer's socket. */
    val terminationDelivered: AtomicBoolean = new AtomicBoolean(false)

    /**
     * Lowest sequence number owed to this consumer that has not been queued yet, or
     * [[MemorySpillManager.UNSET_SEQUENCE]] when nothing is owed.
     *
     * The owed run is what makes a bounded queue lossless. A block refused by the queue's ceiling
     * -- whether it is arriving for the first time or being replayed -- is recorded here instead of
     * dropped, and the drain loop tops the queue up from the run as it empties. The run is one
     * contiguous range rather than a set because both of the things that populate it are dense:
     * a resume owes everything from a position to the high-water mark, and a queue ceiling is
     * reached at one point and then refuses every block after it.
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
 * <b>Why this exists at all.</b> Each map task needs a reachable endpoint from which its output can
 * be streamed, and the obvious implementation -- let every map task bind its own listener -- is
 * wrong twice over, for reasons that are properties of the platform rather than of this subsystem.
 *
 *  1. <b>A listener per task multiplies threads and sockets.</b> `TransportServer.init()`
 *     unconditionally creates its own boss and worker event loop groups, sized from
 *     `spark.shuffle-streaming.io.serverThreads` which defaults to twice the core count. Sharing a
 *     [[org.apache.spark.network.TransportContext]] between servers does not share those groups,
 *     because they are created per server. On a sixteen core executor running a hundred map tasks
 *     that is thousands of threads and a hundred listening sockets for a subsystem that needs one.
 *  2. <b>A listener per task dies before any consumer can use it.</b> The DAG scheduler submits a
 *     stage only once it has no missing parents, so a reduce task never starts until its whole map
 *     stage has finished. A listener whose lifetime is its map task's is therefore guaranteed to be
 *     gone by the time the first consumer tries to connect. The scheduler is an absolute
 *     preservation zone, so the resolution is not to change when reduce tasks start; it is to make
 *     the serving endpoint outlive the task that produced the bytes, which the retained output
 *     already does.
 *
 * This listener is consequently owned by [[StreamingShuffleManager]] and lives as long as the
 * executor's shuffle manager does. It holds no output of its own: it is a router, and everything it
 * routes to belongs to [[StreamingShuffleBlockResolver]], whose lifetime is the shuffle's.
 *
 * <b>What routing requires.</b> A frame has to be attributed to the producer it concerns before
 * anything acts on it, and the protocol makes that a two field lookup: the header carries the
 * shuffle id and the map id at fixed offsets, so
 * [[org.apache.spark.network.shuffle.protocol.streaming.StreamingShuffleMessage#peekShuffleId]] and
 * its map id counterpart answer without allocating, without decoding a body, and without disturbing
 * the buffer that is then handed on to the real handler. A frame naming a producer that is not
 * registered is dropped rather than escalated -- see [[dropUnroutable]] for why.
 *
 * <b>Channel fan-out.</b> One consumer channel may legitimately carry frames for several producers,
 * because a reduce task reads every map output of its shuffle and the executor exposes one port for
 * all of them. Losing that channel therefore has to be reported to each producer handler that has
 * seen traffic on it, and to no others: telling an uninvolved handler that a channel it never
 * served has closed would have it drop a session that does not exist, and worse, would let one
 * consumer's disconnection be attributed to producers it never spoke to. The
 * [[channelParticipants]] map is exactly the set of handlers a channel has actually reached.
 *
 * <b>Thread safety.</b> Every method here runs on a Netty event loop thread and every collection is
 * concurrent. Nothing blocks, and nothing throws into the event loop: failures are contained the
 * same way the per-producer handler contains them, because an exception escaping a callback would
 * tear down a pipeline shared by every producer on the executor.
 *
 * <b>Why this shares a file with [[StreamingShuffleServerHandler]].</b> Producer-side channel
 * handling is one declared component of this subsystem, and routing is not a second one -- it is
 * the part of that component which cannot be per producer, because an executor binds one port for
 * all of them. The two belong together in fact as well as on paper: this class is the only
 * implementation of [[StreamingShuffleRouteRegistry]], which is declared above precisely so that a
 * handler can withdraw its own route; its [[StreamingShuffleListener.bind]] builds the transport
 * context from the handler's own module-scoped configuration and bootstraps; and every frame it
 * routes is routed to a handler. Splitting them would put one component's transport attachment in
 * a file the declared inventory does not contain, and would leave a trait and its sole
 * implementation apart for no reason beyond line count.
 *
 * @param conf the executor's configuration, used only to build the transport tuning namespace
 * @param clock the time source the log suppression windows are measured on. A window that cannot be
 *              advanced deliberately can only be tested by waiting it out, which is exactly the
 *              shape of test this subsystem avoids everywhere else; injecting the clock lets a
 *              suite assert both halves of the bound -- that the second report inside a window is
 *              withheld and that the first one after it is not
 */
private[spark] class StreamingShuffleListener(
    conf: SparkConf,
    clock: Clock = new SystemClock)
  extends RpcHandler with StreamingShuffleRouteRegistry with Logging {

  import StreamingShuffleListener._

  require(conf != null, "The Spark configuration must not be null.")

  /**
   * The producer handlers this executor is currently serving, keyed by shuffle and map id.
   *
   * Registration is driven by `getWriter` and removal by generation withdrawal, by the shuffle
   * being unregistered, or by the manager stopping -- never by task completion, which is the whole
   * point of the class.
   */
  private val producers = new ConcurrentHashMap[ProducerKey, StreamingShuffleServerHandler]()

  /**
   * Which producer handlers each consumer channel has reached, keyed by the channel's own id.
   *
   * Populated as frames arrive rather than on channel activation, because until a frame names a
   * producer there is nothing to record and allocating for a peer that has asked for nothing is the
   * unbounded-state exposure this subsystem avoids everywhere else.
   */
  private val channelParticipants =
    new ConcurrentHashMap[String, ConcurrentHashMap[ProducerKey, StreamingShuffleServerHandler]]()

  /**
   * The executor-scoped upkeep thread, created when the first producer registers.
   *
   * <b>Why the upkeep cannot live in the map task.</b> A producer's consumers almost always appear
   * after the producing task has ended, because the DAG scheduler starts no reduce task until its
   * map stage has finished. Heartbeating those consumers' streams, expiring the ones that have gone
   * silent, and draining the blocks they are owed therefore have no task thread to run on: the
   * writer's own maintenance loop stopped when the task did. This is the thread that carries those
   * duties for the whole executor, and it is the reason the retained output remains served rather
   * than merely retained.
   *
   * One daemon thread for the executor, not one per producer: the work per producer is a scan of
   * its sessions on a one-second cadence, and a thread apiece would cost more than the work.
   */
  private var maintenance: ScheduledExecutorService = null

  /** Guards [[maintenance]], which is created lazily and shut down once. */
  private val maintenanceLock = new Object()

  /** Rounds of upkeep this listener has run, for diagnostics and for assertions in tests. */
  private val maintenanceRounds = new AtomicLong(0L)

  /** Consumers the upkeep sweep has retired, session and logical expiry together. */
  private val retiredConsumers = new AtomicLong(0L)

  /** Frames dropped because they named a producer this executor is not serving. */
  private val unroutableFrames = new AtomicLong(0L)

  /** Whether an unroutable frame has been logged, so the log budget is not spent on them. */
  private val unroutableReported = new AtomicBoolean(false)

  /** Frames whose handling failed outright, over the lifetime of this listener. */
  private val malformedFrames = new AtomicLong(0L)

  /** Channels closed for exceeding [[MAX_MALFORMED_FRAMES_PER_CHANNEL]] frame failures. */
  private val abusiveChannelsClosed = new AtomicLong(0L)

  /**
   * The windowed reporter for failures a peer can provoke: unhandled frames and channel faults.
   *
   * Separate from [[noticeReporter]] on purpose. A single shared window would have whichever report
   * happened to arrive first silence the other for the rest of the minute, so routine traffic could
   * mask a warning -- the report that actually matters would be the one suppressed. Severities
   * therefore get a window apiece, which costs a second bounded stream and buys the guarantee that
   * a warning is never withheld on account of an informational line.
   */
  private val abuseReporter = new BoundedReporter

  /** The windowed reporter for routine notices a peer can provoke, such as accepting a channel. */
  private val noticeReporter = new BoundedReporter

  /**
   * Callback bodies contained by [[guard]], and the window that bounds reporting them.
   *
   * A third window rather than a reuse of either existing one, because the three describe three
   * different things: a peer misbehaving, this router declining to act, and this executor failing
   * to act at all. Only the last of them says something is wrong on this side, so it is the one
   * that must not be suppressed by the volume of the other two.
   */
  private val guardFailures = new AtomicLong(0L)

  private val guardReporter = new BoundedReporter

  // The feature's own debug key, read once. Per-frame and per-callback detail is available through
  // it and is unavailable without it, which is what keeps this router inside the log budget while
  // still being diagnosable when an operator asks for it.
  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  /**
   * Frame failures charged to each live channel, keyed the same way participation is.
   *
   * This is the state the close threshold is evaluated against, and it is deliberately per channel
   * rather than per peer address: a peer that reconnects gets a fresh allowance, which is what
   * makes a transient decode fault on one connection survivable while a channel that keeps failing
   * is still terminated. Bounded by [[MAX_TRACKED_MALFORMED_CHANNELS]] and cleared with the
   * channel.
   */
  private val malformedByChannel = new ConcurrentHashMap[String, AtomicLong]()

  /**
   * This router serves no chunked streams, only one-way messages, so it offers an ordinary empty
   * stream manager. A real instance rather than null, because the transport dereferences it
   * unconditionally when a stream request arrives.
   */
  private val streamManager = new OneForOneStreamManager()

  override def getStreamManager(): StreamManager = streamManager

  // Registration, driven by the manager

  /**
   * Begins routing frames for one producer.
   *
   * A second registration for the same producer replaces the first, which is the correct answer for
   * a re-attempted map task: the newer attempt's handler is the one bound to the retained output's
   * current generation, and the older one can only serve output that has already been withdrawn.
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
      channelParticipants.values().asScala.foreach(_.remove(key, previous))
    }
    startMaintenance()
    logInfo(log"Streaming shuffle listener is serving shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
      log"${MDC(MAP_ID, mapId)}; ${MDC(COUNT, producers.size())} producer(s) registered")
  }

  /**
   * Starts the upkeep sweep, once, on the first registration.
   *
   * Lazily rather than in the constructor, so an executor that never runs a streaming map task pays
   * for no thread at all -- including every executor of every job that leaves the feature switched
   * off, which is the default.
   */
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
   * A fixed *delay* rather than a fixed rate, so a slow round cannot queue the next one behind
   * itself and turn a transient stall into a backlog. Each producer's round is guarded
   * independently: one handler failing must not stop the sweep from reaching the others, since the
   * consumers of those others are relying on it for their heartbeats.
   *
   * Public so the manager can force a round rather than wait out the cadence.
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
   * The work is delegated rather than done here, and that is the whole point: the handler owns the
   * one cross-owner withdrawal -- routing, retained output, its own sessions and the spill files
   * behind them -- so a caller that reaches a generation through this routing table retires exactly
   * what a caller that reaches it through the producing writer retires. This method's own
   * contribution is finding the generation; [[StreamingShuffleServerHandler.withdrawGeneration]]
   * removes the routing entry through [[withdrawRoute]] as its first step.
   *
   * Called when a generation is withdrawn or a task fails -- both cases in which the output itself
   * is going away. It is deliberately not called on the success path: a successful map task's
   * output is exactly the output a consumer has yet to read.
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

  /**
   * Removes one producer generation's routing entry, and nothing else.
   *
   * Value-qualified rather than key-qualified: under `spark.shuffle.useOldFetchProtocol` a map id
   * is the partition index instead of the task attempt id, so two attempts of one map task share a
   * key, and a straggling older attempt must not remove the entry its replacement installed.
   *
   * This is one step of a withdrawal, not a withdrawal. It is called by
   * [[StreamingShuffleServerHandler.withdrawGeneration]], which is the only correct way to retire a
   * generation, so nothing else should call it: removing the route while the retained output stayed
   * published would leave a consumer resolving a store it has no way to request from.
   */
  override def withdrawRoute(
      shuffleId: Int,
      mapId: Long,
      handler: StreamingShuffleServerHandler): Boolean = {
    val key = ProducerKey(shuffleId, mapId)
    val removed = producers.remove(key, handler)
    if (removed) {
      channelParticipants.values().asScala.foreach(_.remove(key))
      logInfo(log"Streaming shuffle listener stopped serving shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)}; " +
        log"${MDC(COUNT, producers.size())} producer(s) remain")
    }
    removed
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

  /**
   * Releases every producer this listener is serving, for the manager's own shutdown.
   *
   * The upkeep sweep is stopped first and stopped for good. There is nothing left for it to do once
   * every producer has been released, and leaving a scheduled task behind on a manager that has
   * stopped is the executor-lifetime leak this whole class exists to avoid on the other axis.
   */
  def releaseAll(): Unit = {
    stopMaintenance()
    producers.keySet().asScala.toSeq.foreach(key =>
      deregister(key.shuffleId, key.mapId, "the streaming shuffle manager stopped"))
    channelParticipants.clear()
    malformedByChannel.clear()
    logRouterSummary()
  }

  /**
   * States what this listener routed and what it refused, once, as it is released.
   *
   * Emitted here rather than on a timer because it is the only point at which the figures are final
   * and the only point at which one line per executor lifetime is guaranteed. Each is the total a
   * bound was protecting: the frames this router could not attribute, the frames it could not
   * handle, the channels it disconnected for persisting, and the upkeep the sweep performed on
   * behalf of producers whose tasks had already ended. A healthy executor reports zeros for the
   * first three, which is what makes a non-zero figure worth looking at. The failures this listener
   * contained on behalf of the shared event loop follow on a line of their own, and only when there
   * were any.
   */
  private def logRouterSummary(): Unit = {
    logInfo(log"Streaming shuffle listener released after " +
      log"${MDC(NUM_ITERATIONS, maintenanceRoundCount)} upkeep round(s), retiring " +
      log"${MDC(COUNT, retiredConsumerCount)} consumer(s); " +
      log"${MDC(NUM_EVENTS, unroutableFrameCount)} unroutable frame(s), " +
      log"${MDC(NUM_FAILURES, malformedFrameCount)} unhandled frame(s) and " +
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

  /** Whether a producer is currently routed, for the manager's own bookkeeping. */
  def serves(shuffleId: Int, mapId: Long): Boolean =
    producers.containsKey(ProducerKey(shuffleId, mapId))

  // Transport callbacks

  /**
   * Routes one control frame that arrived as a one-way message.
   *
   * The frame's buffer is passed on untouched: both peeks read at absolute indices, so the handler
   * that receives it sees the buffer exactly as the transport delivered it.
   */
  override def receive(client: TransportClient, message: ByteBuffer): Unit = {
    guardFrame(client, "route a one-way streaming shuffle frame")(route(client, message))
  }

  /**
   * Routes one control frame that arrived as a request expecting a reply.
   *
   * The reply is empty and is sent whether or not the frame could be routed, because a peer left
   * waiting would spend its whole RPC timeout on an answer this protocol never intended to send.
   */
  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    guardFrame(client, "route a request-shaped streaming shuffle frame")(route(client, message))
    callback.onSuccess(ByteBuffer.allocate(0))
  }

  /**
   * Notes a consumer channel, without allocating anything for it.
   *
   * Nothing is recorded here on purpose. Which producers a channel concerns is not known until it
   * says so, and a channel that has named none has asked for nothing.
   */
  override def channelActive(client: TransportClient): Unit = {
    guard("note a new streaming shuffle channel") {
      // Reported through the shared window rather than per channel. An accepted connection is one
      // frame's worth of information and a peer controls how many it opens, so an unconditional
      // line here is the same log-amplification vector a malformed frame is, differing only in
      // being cheaper for the peer to trigger.
      reportNotice(log"Streaming shuffle listener accepted a channel from " +
        log"${MDC(HOST_PORT, client.getSocketAddress())}; " +
        log"${MDC(COUNT, producers.size())} producer(s) registered")
    }
  }

  /**
   * Reports the loss of a consumer channel to exactly the producers it had reached.
   *
   * Each participating handler retains what that consumer had not acknowledged, so a reconnection
   * resumes rather than forcing the upstream stage to be recomputed. Handlers the channel never
   * reached are not told, because the channel's loss says nothing about them.
   */
  override def channelInactive(client: TransportClient): Unit = {
    guard("report the loss of a streaming shuffle channel") {
      forgetChannelFailures(client)
      val participants = channelParticipants.remove(channelKeyOf(client))
      if (participants != null) {
        participants.values().asScala.foreach { handler =>
          guard("report a channel loss to a producer handler")(handler.channelInactive(client))
        }
      }
    }
  }

  /**
   * Reports a channel level failure to exactly the producers that channel had reached.
   *
   * Only those handlers latch the failure, and only that channel is closed by them. A fault on one
   * consumer's connection says nothing about the other consumers or the other producers, and
   * broadcasting it would turn one channel's problem into every map output's problem.
   */
  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    guard("report a streaming shuffle channel failure") {
      forgetChannelFailures(client)
      val participants = channelParticipants.remove(channelKeyOf(client))
      if (participants == null || participants.isEmpty) {
        // Bounded for the same reason the frame path is: a peer can provoke a channel-level fault
        // as often as it can open a connection, and each one carried a full stack trace. The close
        // below is unconditional and is what makes the provocation terminate; only its report
        // competes for the window.
        reportAbuse(log"Streaming shuffle listener saw " +
          log"${MDC(ERROR, cause.getMessage())} on a channel from " +
          log"${MDC(HOST_PORT, client.getSocketAddress())} that had reached no producer; " +
          log"closing it", cause)
        guard("close a failed streaming shuffle channel")(client.getChannel().close())
      } else {
        participants.values().asScala.foreach { handler =>
          guard("report a channel failure to a producer handler") {
            handler.exceptionCaught(cause, client)
          }
        }
      }
    }
  }

  // Internals

  /**
   * Hands one frame to the producer handler it names, recording the channel's participation.
   *
   * Participation is recorded before the handler runs, so that a frame which provokes a failure
   * still leaves the channel attributable: otherwise a handler could latch an error for a channel
   * whose later loss it would never be told about.
   */
  private def route(client: TransportClient, message: ByteBuffer): Unit = {
    val shuffleId = StreamingShuffleMessage.peekShuffleId(message)
    val mapId = StreamingShuffleMessage.peekMapId(message)
    val key = ProducerKey(shuffleId, mapId)
    val handler = producers.get(key)
    if (handler == null) {
      dropUnroutable(client, shuffleId, mapId)
    } else {
      participantsOf(client).put(key, handler)
      handler.receive(client, message)
    }
  }

  /**
   * Counts and drops a frame naming a producer this executor is not serving.
   *
   * Dropped rather than escalated, and the distinction matters. A consumer legitimately holds a
   * producer address for as long as its rendezvous answer says so, and a producer can stop being
   * served at any moment -- its generation withdrawn, its shuffle unregistered, its executor's
   * manager stopped. A frame that arrives in that window is stale, not hostile, and failing
   * something over it would turn ordinary lifecycle timing into a recomputed stage. The consumer
   * discovers the producer is gone through its own liveness timer, which is the mechanism that
   * exists for exactly this.
   *
   * The first occurrence is logged and the rest are counted, so a peer that sends nothing but
   * unroutable frames cannot spend the executor's log budget.
   */
  private def dropUnroutable(client: TransportClient, shuffleId: Int, mapId: Long): Unit = {
    unroutableFrames.incrementAndGet()
    if (unroutableReported.compareAndSet(false, true)) {
      logWarning(log"Streaming shuffle listener dropped a frame from " +
        log"${MDC(HOST_PORT, client.getSocketAddress())} addressed to shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)}, which this executor is not " +
        log"serving. Further occurrences are counted but not logged")
    }
  }

  /** The participation set of one channel, created on first use without taking a lock. */
  private def participantsOf(
      client: TransportClient): ConcurrentHashMap[ProducerKey, StreamingShuffleServerHandler] = {
    val key = channelKeyOf(client)
    val existing = channelParticipants.get(key)
    if (existing != null) {
      existing
    } else {
      val created = new ConcurrentHashMap[ProducerKey, StreamingShuffleServerHandler]()
      val raced = channelParticipants.putIfAbsent(key, created)
      if (raced != null) raced else created
    }
  }

  /**
   * The routing key of one channel.
   *
   * The channel's own id, matching what the per-producer handler keys its sessions by, so that a
   * reconnection is a different channel here as well and a late teardown of the connection that was
   * lost cannot dispossess the one that replaced it.
   */
  private def channelKeyOf(client: TransportClient): String =
    client.getChannel().id().asLongText()

  /**
   * Runs one frame-driven callback body, bounding what a failure costs and what a failing channel
   * costs.
   *
   * Identical to [[guard]] in what it contains and why, and different in what it does afterwards.
   * The bodies this wraps are reached once per inbound frame, and a peer decides how many frames it
   * sends: a report emitted per failure therefore scales with hostile traffic rather than with the
   * number of distinct things that have gone wrong, which is the whole of the exposure. So the
   * report goes through the shared window, and the channel is charged for the failure so that one
   * which keeps failing is disconnected rather than merely un-logged.
   *
   * @param client the channel the frame arrived on, charged for the failure and closed past the
   * threshold
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
        // is disconnected even in a window where its report is suppressed. Bounding the log must
        // not become the reason the abuse continues.
        if (chargeChannelFailure(client)) {
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
   *
   * <b>Why the withheld count is carried rather than discarded.</b> Suppression that leaves no
   * trace is indistinguishable, to whoever reads the log, from nothing having happened. Whichever
   * report next escapes the window therefore states how many were withheld since the last one, so
   * the gap is stated rather than inferred.
   *
   * The claim is a compare-and-set on the deadline, so concurrent event-loop threads produce
   * exactly one report between them and the losers account themselves as suppressed rather than
   * blocking -- this runs on a Netty thread and must never wait.
   */
  private final class BoundedReporter {

    /**
     * The monotonic instant from which the next report may be emitted.
     *
     * Held as a deadline rather than as the time of the last report, because that is the form in
     * which an elapsed-time comparison against a monotonic reading is safe: `now - deadline >= 0`
     * stays correct across the wrap that `now >= last + interval` does not. Seeded with the current
     * instant, so the first report of the process is always emitted.
     */
    private val nextReportNs = new AtomicLong(clock.nanoTime())

    /** Reports withheld because one had already been emitted inside the current window. */
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
   * Answers `true` exactly once per channel, on the failure that reaches the threshold, so the
   * caller closes the channel once however many further failures the same channel produces before
   * the close completes -- a close is asynchronous and frames already in the pipeline still arrive.
   *
   * A channel that cannot be tracked because [[MAX_TRACKED_MALFORMED_CHANNELS]] is reached is never
   * reported as exhausted. That is the safe direction: refusing to allocate for it costs the peer
   * nothing but costs this executor nothing either, whereas closing an untracked channel on its
   * first failure would disconnect legitimate consumers precisely when the executor is busiest.
   *
   * @param client the channel the failure occurred on
   * @return `true` on the single failure that exhausts the channel's allowance
   */
  private def chargeChannelFailure(client: TransportClient): Boolean = {
    val key = channelKeyOf(client)
    val existing = malformedByChannel.get(key)
    val counter = if (existing != null) {
      existing
    } else if (malformedByChannel.size() >= MAX_TRACKED_MALFORMED_CHANNELS) {
      null
    } else {
      val created = new AtomicLong(0L)
      val raced = malformedByChannel.putIfAbsent(key, created)
      if (raced != null) raced else created
    }
    counter != null && counter.incrementAndGet() == MAX_MALFORMED_FRAMES_PER_CHANNEL.toLong
  }

  /** Drops a channel's failure allowance, so the ledger cannot outlive the channels it counts. */
  private def forgetChannelFailures(client: TransportClient): Unit = {
    malformedByChannel.remove(channelKeyOf(client))
  }

  /**
   * Runs one callback body so that no failure escapes into the event loop.
   *
   * This pipeline is shared by every producer on the executor, so an exception thrown from a
   * callback would not fail one map task -- it would tear down the listener all of them depend on.
   * A fatal `Error` is deliberately allowed to propagate; those belong to the JVM.
   *
   * <b>Why the report is windowed.</b> This wraps every routing callback, and a routing callback
   * runs once per frame per channel. A condition that makes one of them fail -- a handler that has
   * been released while its consumer is still sending, a peer whose frames cannot be attributed --
   * makes all of them fail, so reporting each occurrence would put the executor's log volume under
   * the control of whatever is broken, which is the situation a diagnostic exists to describe
   * rather than to join. The window is the guard's own, shared with neither the abuse reports nor
   * the routine notices: a peer sending malformed frames and this executor failing to route a good
   * one are different facts, and one must not suppress the other.
   */
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

  /** Bind to an ephemeral port: the chosen port is published through the coordinator. */
  private val EPHEMERAL_PORT = 0

  /**
   * How often the executor-scoped upkeep sweep runs, in milliseconds.
   *
   * One second, which is the cadence the producing writer uses for the same duties while it is
   * alive, so a consumer sees the same service before and after its producer's task has ended. It
   * has to be well inside the five-second heartbeat interval and the ten-second liveness window, or
   * the sweep would be the reason a bound was missed; a second gives both an order of magnitude.
   */
  val MAINTENANCE_INTERVAL_MS: Long = 1000L

  /**
   * Shortest interval between two abuse reports from one listener, in nanoseconds.
   *
   * <b>Why a window rather than a one-shot latch.</b> An unroutable frame is reported once and
   * never again, because the fact it reports -- this executor is not serving that producer -- does
   * not change while the process runs. A frame this listener could not handle at all is different:
   * the reason changes, it is worth seeing when it changes, and a permanently silenced report would
   * make a genuine protocol regression invisible for the life of the executor. A window keeps the
   * report alive without letting it scale with the traffic that provokes it.
   *
   * One minute, which bounds each report stream to sixty an hour and the listener, which keeps one
   * stream per severity, to a hundred and twenty. A report with a stack trace is on the order of a
   * kilobyte, so the whole of this surface fits inside a rounding error of the ten megabytes an
   * hour the subsystem is allowed, whatever a peer sends.
   *
   * Measured with [[java.lang.System#nanoTime]] rather than wall clock, because the only question
   * asked of it is how much time has elapsed, and an interval must not be lengthened or shortened
   * by an administrator correcting the clock.
   */
  val ABUSE_REPORT_INTERVAL_NS: Long = TimeUnit.MINUTES.toNanos(1L)

  /**
   * Frame failures one channel may cause before this listener closes it.
   *
   * <b>Why a channel is closed at all.</b> Suppressing the log bounds what a hostile peer costs the
   * log, and nothing else: it still costs a decode attempt and an exception construction per frame,
   * for as long as it cares to keep sending. Closing the channel is what makes the cost terminate.
   * A consumer whose frames this listener cannot handle is not a consumer that will make progress,
   * so nothing is lost by disconnecting it, and a legitimate one reconnects and resumes from its
   * retained cursor -- which is the mechanism that makes closing safe rather than merely cheap.
   *
   * Sixty-four is well above anything a correct peer produces, since a correct peer produces none,
   * and well below a number that would let one channel dominate the executor's decode budget.
   */
  val MAX_MALFORMED_FRAMES_PER_CHANNEL: Int = 64

  /**
   * Channels whose failure counts are tracked at once.
   *
   * The tracking ledger is keyed by a peer-supplied channel and would otherwise be exactly the
   * unbounded remote-keyed state this subsystem bounds everywhere else -- a ceiling on the log that
   * introduced a leak in the heap would be no improvement at all. Entries are removed when the
   * channel goes away, so this ceiling is only ever reached by more channels failing concurrently
   * than an executor has any reason to accept; past it, a channel is still served and still logged,
   * it merely forgoes the close threshold.
   */
  val MAX_TRACKED_MALFORMED_CHANNELS: Int = 4096

  /**
   * Binds the executor's one streaming shuffle listener.
   *
   * The transport configuration comes from the same builder the consumer side uses, so keepalive
   * and the rest of the streaming tuning namespace cannot be configured differently on the two ends
   * of a channel. Server bootstraps carry the authentication and encryption the operator has asked
   * for, so a consumer is authenticated before this listener routes a single frame.
   *
   * @param conf the executor's configuration
   * @return the router, the transport context that owns the pipeline, and the bound server
   */
  def bind(conf: SparkConf): (StreamingShuffleListener, TransportContext, TransportServer) = {
    val listener = new StreamingShuffleListener(conf)
    val transportConf = StreamingShuffleServerHandler.streamingTransportConf(conf)
    val transportContext = new TransportContext(transportConf, listener)
    val server = transportContext.createServer(EPHEMERAL_PORT,
      StreamingShuffleServerHandler.streamingServerBootstraps(transportConf))
    logInfo(log"Streaming shuffle listener bound one port for this executor: " +
      log"${MDC(PORT, server.getPort)}")
    (listener, transportContext, server)
  }

  /**
   * The identity of one producer, which is what a frame is routed on.
   *
   * Shuffle and map id together, and nothing else. Not the task attempt id: a frame's header does
   * not carry one, and it must not, because a consumer holding an address for an earlier attempt
   * has to be able to reach whichever attempt is currently registered rather than be refused for
   * naming the attempt it was told about. Which attempt's output is served is decided inside the
   * handler, against the retained store's current generation.
   */
  private[streaming] case class ProducerKey(shuffleId: Int, mapId: Long) {
    override def toString: String = s"shuffle $shuffleId map $mapId"
  }
}
