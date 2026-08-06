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
 * <b>Why `autoRead` lives here, and only here.</b> Toggling `autoRead` is a new idiom in this code
 * base: it appears nowhere in the shared transport module and nowhere else in Spark core. This file
 * owns it as a flow-control mechanism -- the producer-side handler never touches it, and
 * participates in flow control only through the protocol -- and confining the idiom to one file is
 * what satisfies the directive to prefer the least modification to the network transport layer. The
 * one call outside this file is a fault-injection hook that only ever disables reading and never
 * paces anything; [[StreamingShuffleChannelReadGate]] names it, for the reason given there. Within
 * this file the single point that writes the flag is that gate, because the flag belongs to the
 * '''socket''' and a socket now carries several handlers: the window is closed while ANY
 * participant is throttled and reopened only when none is. A flag per handler let one participant
 * reopen a window another still needed shut, which is the one way this layer can fail silently.
 * No shared transport class is modified by this feature: `TransportContext`, its pipeline
 * initialisation, `TransportConf` and `SparkTransportConf` are all consumed exactly as they stand.
 * The one piece of transport configuration this handler needs -- an independent tuning namespace --
 * is obtained by passing a module name as an argument to `SparkTransportConf.fromSparkConf`, which
 * yields `spark.shuffle-streaming.io.*` without a single line added to a shared file. That is also
 * why OS level keepalive can be enabled for streaming alone: it is a per-module boolean, and the
 * transport layer exposes no keepalive interval, so the five-second liveness bound this subsystem
 * promises is enforced at application level by the heartbeat timer rather than by TCP.
 *
 * <b>How the three layers of backpressure cooperate.</b> The streaming shuffle holds a producer
 * back in three ways at once, and this handler owns the third of them.
 *
 * - Application-level credit. [[BackpressureProtocol]] keeps a per-stream ledger and answers
 * `hasCredit`; a producer whose credit is spent must stop and wait for an acknowledgement.
 * - Rate limiting. A token bucket paces egress and is consulted by the producer, never here.
 * - TCP-level throttling. When credit is spent, or when this handler's own hand-off queue is at
 * its high-water mark, `autoRead` is turned off: Netty stops reading from the socket, the
 * receive window closes, and the pressure reaches the producer through TCP itself rather than
 * through any message this subsystem has to invent. An acknowledgement that advances the
 * consumer position, or a queue that has drained, withdraws this handler's throttle -- and the
 * socket reopens once no participant of the channel is throttled any more.
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
 * - A Netty event-loop thread runs every `channel*` callback. It may decode a frame, update the
 * lock-free bookkeeping, enqueue a block and write an acknowledgement, and it may do nothing
 * else. In particular it never calls a `ShuffleReadMetricsReporter` method -- that interface
 * documents itself as single-threaded -- it never parks, blocks or sleeps, and it never lets an
 * exception escape a callback.
 * - The reduce task's own thread drains the queue, performs the authoritative checksum
 * verification, updates the read metrics and throws. A failure this handler observes on an I/O
 * thread is handed to [[StreamingShuffleErrorNotifier]], whose first-error-wins latch the task
 * thread reads inside `read()`. Without that bridge a failure seen only on an I/O thread would
 * be swallowed and the task would wait forever for input that is never going to arrive, because
 * `ShuffleReader` exposes only `read()` and its `stop()` is commented out.
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
 * which is what makes "a configuration change requires an executor restart" true by
 * construction
 * @param shuffleId the shuffle this channel serves; every frame that names another shuffle is
 * refused at the boundary
 * @param mapId the map output this channel carries, which together with the task attempt is the
 * producer generation the receiving ledger is keyed by
 * @param taskAttemptId the producing attempt, so a superseded attempt cannot share accounting with
 * the attempt that replaced it
 * @param consumerId this consumer session's identity, which is what the producer's retained store
 * keys its per-consumer acknowledgement cursor by
 * @param startPartition the first reduce partition this task reads, inclusive. Together with
 * `endPartition` it is the *authenticated* request: a frame naming a
 * partition outside it is refused before any state is allocated for it, so
 * the metadata a remote peer can cause this handler to hold is bounded by
 * what the task itself asked for rather than by what the peer chooses to send
 * @param endPartition one past the last reduce partition this task reads
 * @param backpressure the flow-control protocol that owns the credit ledger, the liveness timers
 * and the backpressure-event counter; must not be null
 * @param errorNotifier the bridge that carries a failure observed on this I/O thread to the task
 * thread; must not be null
 * @param clock the single time source for every timer in this class, so each one advances with
 * that clock rather than with wall time and nothing here sleeps
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

  /**
   * The fixed-width form of [[consumerId]], declared on every heartbeat this handler sends.
   *
   * This is the value that makes a reconnection resume rather than restart. A producer keys the
   * cursor that bounds replay -- what this consumer has acknowledged -- by the identity it holds
   * for the consumer, and until this field existed the only identity available to it was derived
   * from the socket. Under that arrangement a returning consumer was an unrelated peer: it
   * inherited no cursor, and the connection it was replacing went on holding retained output and a
   * credit window until a sweep expired it. Declaring an identity the consumer chooses, and which
   * survives its own reconnections, is what lets the producer recognise the two connections as one
   * consumer.
   *
   * Derived from [[consumerId]] rather than carried alongside it so that the two cannot disagree,
   * and computed once here rather than per heartbeat because the digest is the only part of
   * building a heartbeat that is not a field read. The derivation lives on
   * [[BackpressureStreamKey]] because the key is the identity, and the protocol stamps the same
   * value onto the heartbeats it builds from that key -- so the two heartbeat paths of this handler
   * cannot come to declare different identities for one consumer.
   */
  private val consumerToken: Long = BackpressureStreamKey.consumerTokenOf(consumerId)

  // Read once and held immutably. This flag is the single authority over every verbose line this
  // handler writes, so an operator who leaves spark.shuffle.streaming.debug at its default of false
  // sees nothing per block and nothing per autoRead toggle even when logging is globally set to
  // DEBUG. That is what keeps the subsystem inside its budget of under 10 MB per hour per executor,
  // because autoRead can flap once per throttling episode on every channel.
  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  /**
   * Cumulative totals, per channel, for the conditions a producer's behaviour decides the frequency
   * of.
   *
   * A gap in the sequence and a block that fails its checksum are both per-block conditions, and a
   * stream that is systematically wrong rather than occasionally unlucky exhibits them once per
   * block. Reporting each occurrence would make the log volume of a corrupt shuffle equal to the
   * block count of that shuffle. The same is true of every other counter here: a peer that sends
   * nothing but misaddressed frames, or that keeps sending past its own end of stream, produces one
   * occurrence per frame it chooses to send.
   *
   * '''These counters are per channel; the windows that bound reporting them are not.''' One of
   * these handlers exists per producer channel, and a reduce task opens one channel per map output
   * it reads -- so a window owned by a handler admits its own first occurrence whatever the
   * executor has already reported, and executor log volume then scales with the product of reduce
   * tasks and map outputs. Every window therefore lives on the companion object, one per condition,
   * and is named at each call site so the scope is visible where the report is made. The counters
   * stay here because a per-channel tally is real information -- it is what a test asserts on, and
   * what tells an operator whether one producer is at fault or the whole executor is affected --
   * and an admitted record quotes both figures.
   *
   * The terminal cases are deliberately '''not''' aggregated: a block that can no longer be
   * replayed at all, and a producer declared lost, happen once per partition and end the read, so
   * there is nothing recurring to bound and suppressing one would remove the diagnosis of the very
   * read that failed.
   */
  private val repairsRequested = new AtomicLong(0L)

  // Repair windows of this channel whose five-attempt budget has been spent, which is the point
  // past which a position can no longer be recovered by asking again.
  private val replayBudgetsSpent = new AtomicLong(0L)

  /**
   * Emits one report through an executor-scoped window, or accounts it and traces it under the
   * debug key.
   *
   * @param aggregator the executor-scoped window that decides, named by the caller so the scope is
   *                   visible at the report rather than only at the declaration
   * @param total the running total for this channel, advanced here so a caller cannot forget to
   * @param unit the noun the per-channel figure is counted in, so one helper can serve conditions
   *             that are counted in repairs, frames and channels alike
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

  // The one monitor under which this handler's participation in a receive window is transitioned:
  // entering and leaving a throttling episode, moving between gates, and departing on close. This
  // handler keeps NO flag of its own for that intent -- the gate's participant record is the single
  // source of truth, asked through `isThrottling` -- because a second cell updated in a separate
  // atomic step is how an intent and its record diverge: a compare-and-set that moved while another
  // thread's gate call had not yet landed left this handler believing it was reading while the gate
  // still held its throttle, with no living path able to withdraw it. Reading the record and acting
  // on it under one monitor is what makes an episode entered and left exactly once.
  //
  // Held only across the gate call. Everything a transition then causes -- the backpressure poll,
  // the per-stream notes, the counters and the log records -- is issued after it is released, so no
  // thread can hold one handler's monitor while a callback reaches for another's.
  private val gateLock = new Object

  // Whether this handler has left its gate for good. Guarded by `gateLock`. A closed handler that
  // could still state a throttle would leave one standing in a gate it no longer participates in,
  // which for a shared channel is a window wedged shut for every producer multiplexed onto it.
  private var gateDeparted = false

  // The receive window of the physical channel, shared with every other handler multiplexed onto
  // it. A handler starts with a gate of its own, because a frame can be delivered synchronously on
  // the connecting thread before the connector has published the channel's share, and a handler
  // must be able to close its window at that point too. The connector swaps in the channel's shared
  // gate when it binds or joins this handler, carrying this handler's standing intent across. The
  // reference is swapped under `gateLock` and kept in an atomic so the accessors can read it
  // without taking the monitor.
  private val readGate =
    new AtomicReference[StreamingShuffleChannelReadGate](new StreamingShuffleChannelReadGate)

  // Transitions into the throttled state observed on this channel. The executor-wide
  // shuffle.streaming.backpressureEvents counter is NOT advanced from here -- the protocol owns it
  // -- so this local total exists for assertions that need to speak about one channel.
  private val throttleTransitions = new AtomicLong(0L)

  private val throttleCause = new AtomicReference[String](null)

  // Instant at which this channel went inactive with work outstanding, read from the injected
  // clock. NO_TIMESTAMP while the producer is believed alive.
  private val producerLostAtMillis = new AtomicLong(NO_TIMESTAMP)

  // Failures escalated from an I/O thread to the task thread on this channel. Read by the reader to
  // decide whether a transport-level diagnosis already exists, and asserted on by the suites. It is
  // not a log budget: escalation itself writes nothing at default level, because the reader owns
  // the one default-level record of a producer loss. See [[escalate]].
  private val escalationsReported = new AtomicLong(0L)

  // Admissions that have charged the executor's shared receive budget but have not yet finished
  // recording ownership of it. Raised for the whole of that window by admitBlock and read by
  // close(), which is the one place two threads can otherwise both believe the other owns a charge:
  // close() returns what the per-partition ledgers hold and then drops them, so an admission that
  // recorded itself afterwards would leave bytes charged to a ledger nobody reads. close() waits
  // out this counter, and any admission that discovers the closure first rolls its own charge back.
  private val admissionsInFlight = new AtomicInteger(0)

  /** Consumer operations accepted by the bounded data-plane workers but not yet settled. */
  private val dataPlaneTasksInFlight = new AtomicInteger(0)

  /** Completion primitive shared by frame-task and admission lifecycle waits. */
  private val lifecycleLock = new ReentrantLock()

  private val lifecycleChanged = lifecycleLock.newCondition()

  /** Worker-queue refusals, which fail closed rather than running CRC work on Netty. */
  private val dataPlaneRefusals = new AtomicLong(0L)

  /**
   * Bridge from the wire decoder to the executor-wide aggregate allowance.
   *
   * The decoder invokes this after validating the frame's length prefix and before allocating its
   * payload array. A successful reservation stays attached to the decoded block until this handler
   * either transfers it to per-sequence accounting or rejects the block and returns it.
   */
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
  // Zero on every ordinary run; a non-zero reading means the race really was taken and the rollback
  // really did run, which is what makes the guarantee observable rather than merely intended.
  private val admissionsRolledBack = new AtomicLong(0L)

  // Netty identity of the one channel this handler is bound to, latched on the first callback the
  // transport makes and never replaced. This is the capability binding: a handler is created per
  // producer generation and connected to exactly one authenticated socket, so the channel's
  // identity *is* the generation's identity on the wire, which no field of the protocol header
  // carries. Null until the first callback arrives.
  private val boundChannelId = new AtomicReference[String](null)

  // Principal established by Spark's transport authentication for the bound channel. It is latched
  // with the channel and checked before every frame is decoded, so the reader can prove that no
  // unauthenticated payload reached its deserializer.
  private val boundTransportPrincipal = new AtomicReference[String](null)

  // Callbacks refused because Spark authentication established no principal for their channel.
  private val unauthenticatedCallbacks = new AtomicLong(0L)

  // One-shot warning for unauthenticated callbacks; the task notifier retains the actual failure.
  private val unauthenticatedReported = new AtomicBoolean(false)

  // Callbacks refused because they arrived on a channel other than the bound one. Counted rather
  // than escalated: a stray callback is evidence of a routing fault somewhere else, and failing
  // this reduce task over it would turn another component's mistake into a recomputed stage.
  // Reported through the executor's own window rather than a latch of this handler's: a one-shot
  // latch per handler is still one record per producer channel, and a reduce task opens one of
  // those per map output it reads.
  private val foreignChannelCallbacks = new AtomicLong(0L)

  // Frames dropped because they named a shuffle or a partition this handler does not serve. Counted
  // rather than escalated, because a producer that routed a block to the wrong consumer has made a
  // routing mistake and not a protocol violation. Reported on the executor's window, for the reason
  // above.
  private val misaddressedFrames = new AtomicLong(0L)

  // Control frames refused because they only ever travel from consumer to producer. A protocol
  // violation rather than a routing quirk, so it escalates as well as being counted, and its record
  // is bounded on the executor's window because a peer chooses how many it sends.
  private val wrongDirectionFrames = new AtomicLong(0L)

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
  /**
   * Whether a frame names the producer this handler serves.
   *
   * Used by the connector to route a frame on a channel that carries several producers of one
   * consumer. The header is peeked rather than decoded, so routing costs no allocation and no body
   * parse -- exactly as the producer side's own router does it -- and a frame too short or
   * malformed to name a producer answers false, leaving the decision to the caller rather than
   * claiming a frame this handler may not own.
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

  override def receive(client: TransportClient, message: ByteBuffer): Unit = {
    StreamingShuffleServerHandler.authenticatedPrincipal(client) match {
      case Some(principal) => guard(consumeFrame(client, message, principal))
      case None => rejectUnauthenticatedChannel(client)
    }
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
    StreamingShuffleServerHandler.authenticatedPrincipal(client) match {
      case Some(principal) =>
        guard(consumeFrame(client, message, principal))
        callback.onSuccess(ByteBuffer.allocate(0))
      case None =>
        callback.onFailure(rejectUnauthenticatedChannel(client))
    }
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
      // cost and what follows. Two records per loss per channel, on a stage whose every reduce task
      // opens a channel per map output, is exactly the volume this feature may not produce.
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
  private def consumeFrame(
      client: TransportClient,
      frame: ByteBuffer,
      principal: String): Unit = {
    if (!bindChannel(client, principal)) {
      // Refused before a single byte is decoded. A frame from a channel this handler is not bound
      // to has not passed this handler's capability check, and a streaming payload reaches Spark's
      // deserialization: the per-block CRC32C detects corruption and forges trivially, so the
      // authenticated channel is the only thing standing between a peer and that deserialization.
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
      // failing the task cleanly. Recording it is what makes the task thread raise it from read().
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

  /**
   * Transfers one decoded frame to the executor-wide bounded data-plane workers.
   *
   * Decoding has already copied the transport-owned bytes into the message and, for a data block,
   * reserved its payload against the aggregate quota. The worker therefore owns the message until
   * the final reservation release below.
   */
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
   * Authentication is checked before this method is called and its principal is latched alongside
   * the channel. A null channel is accepted only with that authenticated principal, which preserves
   * support for a transport test double without creating an unauthenticated exception to the
   * production rule.
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
   * Bounded on the executor's window rather than on a first occurrence of this handler's, because a
   * routing fault that misdelivers one callback misdelivers them to every consumer it reaches: a
   * latch per handler admits one record per producer channel, and executor log volume would then
   * scale with the product of reduce tasks and map outputs. Per-occurrence identity stays available
   * behind the streaming debug key, and the per-channel tally is exposed for assertions.
   */
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
   * The connector's bootstrap requirement normally prevents this path. Keeping it here is the final
   * consumer-side invariant: even if a connector is assembled incorrectly, the frame is neither
   * decoded nor queued, the task is failed through its notifier, and the channel is closed before
   * any payload can reach `SerializerInstance.deserializeStream`.
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
      if (writeHeartbeat(partitionId, buildHeartbeat(state))) {
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
    // The intent record and the socket update are one transition, taken under this handler's own
    // monitor so that a concurrent resume cannot land between reading the record and stating the
    // new intent.
    // A handler that has departed states nothing: its throttle would stand in a gate it no longer
    // participates in, which for a shared channel is a window no living participant could reopen.
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
      // Reported after the poll, so a stale episode is closed before this one is opened. Only the
      // partitions actually in flight on this channel are named: an unknown stream is ignored, and
      // this runs on an event-loop thread, so the scan stays bounded by what is being consumed.
      // Outside the monitor, because the protocol may re-enter another handler of this channel and
      // holding one participant's monitor while reaching for another's is how a cycle forms.
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
   * Called by the connector at the moment it publishes or joins this handler against a physical
   * channel, and it is what turns a handler-local window into a channel-wide one. This handler's
   * standing intent is carried across, so a handler that had already closed its own window does not
   * silently reopen the shared one by joining it; and the gate is told about the channel, so a
   * throttle stated before the socket was known still reaches the socket.
   *
   * Idempotent: joining the gate a handler already holds re-applies the same intent and writes
   * nothing.
   *
   * @param gate the receive window of the physical channel this handler is multiplexed onto
   */
  private[streaming] def joinReadGate(gate: StreamingShuffleChannelReadGate): Unit = {
    if (gate != null) {
      // Under the same monitor as an ordinary transition, and for the same reason: the intent is
      // read
      // from the gate being left and re-stated on the gate being joined, and a throttle or resume
      // landing between those two steps would either be applied to a gate this handler has already
      // abandoned or be lost altogether. A departed handler joins nothing.
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

  /** The receive window this handler currently participates in. Exposed for assertions. */
  private[streaming] def currentReadGate: StreamingShuffleChannelReadGate = readGate.get()

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
    StreamingShuffleMessage.Decoder.fromByteBuffer(framed, payloadReservation)
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
   * mistake into a recomputed stage. Reported on the executor's window rather than on this
   * handler's first occurrence, so that neither a peer that sends nothing but misaddressed frames
   * nor a stage whose every reduce task opens a channel per map output can spend the executor's log
   * budget.
   */
  private def rejectMisaddressed(message: StreamingShuffleMessage): Unit = {
    reportBounded(StreamingShuffleClientHandler.misaddressedFrameLogAggregator,
      misaddressedFrames, "dropped frame(s)",
      log"Streaming shuffle consumer for shuffle ${MDC(SHUFFLE_ID, shuffleId)} " +
        log"partitions [${MDC(COUNT, startPartition)}, ${MDC(THRESHOLD, endPartition)}) dropped " +
        log"a frame addressed to shuffle ${MDC(VALUE, message.shuffleId())} map " +
        log"${MDC(MAP_ID, message.mapId())} partition " +
        log"${MDC(PARTITION_ID, message.partitionId())}")
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
   *
   * Ahead of all four sits one further check, which is not about this block but about the stream:
   * a stream that has already ended accepts no NEW block. Admitting a block past an accepted
   * terminator would put the stream's block count above the total that terminator fixed, which is
   * the total the reader completes against -- so the completion check would be comparing a
   * consumed count against a total the data had already outgrown. Such a block is refused and the
   * producer declared lost, exactly as a contradictory terminator is; see
   * [[rejectBlockAfterTermination]].
   *
   * That check is deliberately narrower than "anything after the terminator", and it distinguishes
   * three arrivals rather than two.
   *
   *  - '''A copy of a position already accepted''' carries no new volume and moves no cursor, so it
   *    cannot put the stream past its total. It is the same redundant copy the live path discards,
   *    and it arrives after a terminator for the ordinary reason that a producer had already
   *    committed a repair to the wire when it ended the stream. Escalating it would fail the reduce
   *    stage of a shuffle whose output is complete and correct, and pay for the whole map stage
   *    again; see [[discardDuplicateBlock]].
   *  - '''A replacement for a quarantined position below the announced total''' is the repair this
   *    consumer asked for, and it must be accepted for exactly the same reason. The reader performs
   *    the authoritative verification on the task thread, which necessarily runs *behind* the
   *    channel: a block can therefore be found corrupt, and its replacement requested, after the
   *    terminator for that stream has already been accepted -- the total was reconciled against the
   *    position reached, and a repair does not change that position. Refusing the replacement in
   *    that ordering made in-window retransmission unable to complete at all: the consumer asked
   *    for a block and then declared the producer lost for sending it. The position is below the
   *    total the terminator fixed, so admitting it moves no cursor and cannot outgrow that total --
   *    the completion check stays exactly as meaningful as it was; see
   *    [[isAwaitedRepairAfterTermination]].
   *  - '''Anything at or beyond the announced frontier''' is the contradiction that cannot be
   *    completed around: either the terminator or the block is wrong. That is what escalates, and
   *    it is the only thing that does; see [[rejectBlockAfterTermination]].
   */
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
      // same event. A position this consumer has already accepted is a redundant copy of output it
      // holds: discarding it is exactly the answer the live path gives the same frame, it loses
      // nothing, and the partition stays complete. A replacement for a position this consumer
      // quarantined and that lies below the announced total is the repair it asked for, and it is
      // accepted on the same path the live stream would have used -- a repair moves no cursor, so
      // the total the terminator fixed still describes the stream. A position at or beyond that
      // frontier is the contradiction this stream cannot be completed around -- either the
      // terminator or the block is wrong -- and that alone escalates.
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
   * The three steps after the charge are the sequence check, the checksum and the commit, in that
   * order, and this method exists so that the live path and the post-termination repair path run
   * *exactly* the same ones. The alternative -- a second, shorter admission path for repairs that
   * arrive after a terminator -- is how the two get to disagree about window accounting, liveness
   * or the replay budget, and a disagreement there is invisible until a stream stalls.
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
   * Two conditions, and both are load-bearing. The position must be one this consumer quarantined,
   * which is what makes the block a replacement it requested rather than output the producer
   * invented; the caller establishes that from its single reading of the quarantine. And the
   * position must lie *strictly below* the total the terminator committed to, which is what makes
   * admitting it safe: the cursor is already past it, so no cursor moves, the stream's accepted
   * count cannot pass the announced total, and the reader's completion reconciliation is left
   * comparing exactly the quantities it was designed to compare.
   *
   * A stream observed as terminated has always had its total published first -- [[completeStream]]
   * writes the total before anything else, on the one thread that won the latch, and that thread is
   * this one -- so the sentinel reading is impossible in practice. It is still checked, because a
   * frontier of [[NO_BLOCK_TOTAL]] would compare as larger than every real position and turn the
   * bound this method exists to enforce into no bound at all.
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
   * Delivery on this protocol is idempotent by design rather than by accident. A producer may put a
   * position on the wire a second time -- a resume records a run from the position a consumer
   * announced, a queue ceiling defers a block that is later re-queued, and a repair replays a run
   * that spans the boundary between what has been delivered and what has not -- so a copy of
   * something already held is a routine event and not a fault. It is accounted for exactly as much
   * as it deserves: the arrival stamps producer liveness and the bytes count as link traffic
   * through `onDiscardedData`, while the receive window is not charged, the stream's received total
   * does not move, and the sequence cursor stays where it is.
   *
   * Shared by the live path and the ended-stream path deliberately. The same frame arriving a
   * millisecond either side of a terminator is the same redundant copy of the same output, and
   * giving the two arrivals different answers -- discard on one side, escalate to a fetch failure
   * and an upstream recomputation on the other -- is what turned a producer's harmless repair into
   * a spurious producer-loss report.
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
    val decoderReservation = block.hasPayloadReservation()
    // Sampled before the quarantine is released, so the frontier this admission is judged against
    // is the one that was in force when the block arrived rather than one a concurrent admission
    // moved.
    val expectedAtAdmission = state.expectedSequenceNumber.get()
    val replacement = state.releaseQuarantine(sequenceNumber)
    // Production decode already reserved this payload before allocating it. The fallback branch is
    // retained for a package-local test or collaborator that hands over a constructed block rather
    // than a decoded one; it preserves the same ownership contract without weakening production's
    // reserve-before-allocation guarantee.
    if (!decoderReservation && !backpressure.tryReserveReceiveQuota(payloadLength)) {
      refuseForQuota(state, sequenceNumber, payloadLength)
      return
    }
    // From here to the matching decrement this admission is *in flight*, and [[close]] can see that
    // it is. Without the announcement the two could interleave so that this thread reserved the
    // quota and enqueued the block, close() then returned every byte its ledgers knew about and
    // dropped the ledgers, and this thread finally charged the bytes to a ledger nothing would ever
    // read again -- a permanent reduction of the budget every later consumer on this executor draws
    // from. The counter is what makes that window observable, and [[rollBackAdmission]] below is
    // what closes it.
    admissionsInFlight.incrementAndGet()
    try {
      if (enqueue(BlockReceived(block), payloadLength)) {
        // Re-checked after the hand-off succeeded, because closure can have been decided between
        // the check inside enqueue() and this point. A closed handler serves a task that has gone,
        // so the block is worthless: what matters is that its charge is returned exactly once and
        // that no ownership is recorded against a ledger close() has already accounted for.
        if (closed.get()) {
          rollBackAdmission(payloadLength, releaseQuota = !decoderReservation)
          return
        }
        // Per-sequence accounting, so that an acknowledgement releases exactly the bytes of the
        // prefix it names and never the whole of what has arrived. A replacement overwrites the
        // entry it replaces, so the quota charge above must be matched by releasing whatever that
        // position was already holding, or the two ledgers would drift by one block per repair.
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
        // stricter statement than "this block was not a replacement" and a necessary one. A
        // replacement for a position *below* the frontier must not move it -- the cursor is already
        // past that position and moving it would skip the blocks in between -- but a replacement
        // for the frontier position itself must, because that is the position the stream has now
        // reached. Deciding it by the replacement flag alone left the frontier stuck whenever the
        // very first block of a repaired position was the one being repaired, and a frontier that
        // lags the data makes every statement derived from it -- the end-of-stream reconciliation
        // above all -- wrong by exactly the number of repairs the stream needed.
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
          // Re-arming is what keeps "no position is silently skipped" true on this path too.
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
   * The block was reserved and handed to the queue, and then this handler closed; the queue it was
   * handed to has been drained, so nothing is holding the bytes and nothing ever will. Exactly one
   * ledger must therefore give them back, and it is this one -- the per-partition ledgers were
   * either already accounted for by [[close]] or never learned about this block at all, so charging
   * it to them would either double-return the bytes or lose them.
   *
   * The hand-off queue is drained again as well. The block was placed on it after [[close]] had
   * already emptied it, and a closed handler serves a task that has gone, so nothing will ever take
   * it off again -- draining is what stops a payload from being retained for the whole life of
   * whatever still references this handler. It is safe to drain unconditionally here because the
   * closure has been observed: every event on the queue at this point is one no reader can consume.
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
   * The checksum computed by `verifyChecksum` is cached on the block and reused here. The corrupt
   * path therefore performs neither a second payload scan nor the old full-payload clone, while the
   * typed condition still reports the value the block arrived with alongside the value this
   * executor computed.
   */
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

  /**
   * Records a producer heartbeat.
   *
   * Liveness is judged from the local instant of arrival, and the heartbeat carries no time value
   * at all: two hosts do not agree on the wall clock, so a detector built on a remote reading would
   * mistake skew for a failure. What the message does carry is the next position its sender
   * expects, which is recorded by the protocol.
   */
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
   *
   * This is what distinguishes silence that means completion from silence that means failure. A
   * terminated stream stops arming the liveness timers, so a partition that legitimately produced
   * nothing at all -- announced with a total of zero blocks, which is entirely valid -- is never
   * mistaken for a producer that died five seconds ago. Completion is delivered as a marker on the
   * queue rather than left to be inferred from a timeout, so the reduce task learns of it at once.
   *
   * <b>Acceptance is a state transition, not a field assignment, and that is the whole point.</b>
   * A terminator is a peer-authored claim that a stream is finished, and a claim taken on trust is
   * the one loss no checksum can detect: every block that did arrive was intact, so a partition
   * short by its last hundred blocks reads as a complete, silently truncated result. Three
   * conditions therefore govern it, and each closes a distinct way a stream could end short:
   *
   *  1. '''The claim must match the position actually reached.''' `totalBlocks` is compared against
   *     the next position this consumer expects, which is exactly the number of blocks it has
   *     admitted. A terminator naming more than that is premature -- the producer is claiming
   *     blocks this consumer never received -- and a terminator naming fewer contradicts what has
   *     already been delivered. Neither may be accepted, and neither is merely dropped: a stream
   *     that is being told it is over when it is not has lost its producer as surely as one whose
   *     socket closed, so the mismatch is escalated and the reduce task raises a fetch failure that
   *     the unmodified scheduler resolves by recomputing the upstream stage.
   *  2. '''Exactly one terminator wins.''' The transition is latched by compare-and-set on
   *     [[PartitionState.terminated]], so a second terminator can never overwrite the total the
   *     first one established -- the total the queued completion event already carries and the
   *     reader reconciles against. A repeat of the same total is a harmless retransmission of a
   *     control frame and is ignored; a repeat naming a *different* total is a contradiction from
   *     the producer and is escalated.
   *  3. '''Nothing may follow it.''' A data block arriving after acceptance is refused by
   *     [[handleDataBlock]] rather than admitted, because a block accepted past the announced total
   *     would make the total wrong and the completion check meaningless.
   *
   * The order is deliberate: the total is published *before* the flag is raised, so no observer can
   * see a terminated stream whose total has not yet been written, and the queued marker is emitted
   * only by the thread that won the latch.
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
      // blocks that make it true. The producer committed every one of those positions -- some of
      // them were quarantined here for a replay that has been asked for and is still on its way --
      // so refusing the terminator would report a producer that has done nothing wrong as lost and
      // pay for it with a recomputation of the whole upstream stage. It is held instead, and
      // applied by [[applyDeferredTermination]] the moment the repairs close the gap. The wait is
      // bounded by the reader's own producer-liveness timer, which raises a fetch failure if
      // nothing arrives for [[PRODUCER_CONNECTION_TIMEOUT_MS]], and by the replay budget, whose
      // exhaustion escalates.
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
   *
   * The total is recorded rather than applied, so nothing observes the stream as ended: the reader
   * keeps polling, the replay protocol keeps working, and the announced total is available to the
   * admission path that will eventually reach it. A repeated terminator naming the same total is
   * absorbed by the deferral itself; one naming a different total contradicts the first and is
   * refused, because a producer may commit to exactly one total.
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

  /**
   * Applies a held end-of-stream once the repairs that delayed it have closed the gap.
   *
   * Called after every admission, which is the only event that can advance the position a deferred
   * terminator is waiting for. Does nothing in the ordinary case -- one atomic read -- and
   * completes the stream exactly as [[handleTermination]] would have, through the same latch, so a
   * deferred completion and an immediate one are indistinguishable to the reader.
   */
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
   * <b>Why one helper and not two call sites.</b> A stream can end in two ways -- a terminator that
   * matches the position already reached, and one held while repairs were outstanding and applied
   * the moment they closed the gap -- and the two must leave the same state behind, because nothing
   * downstream knows which of them happened. When the deferred path did its own bookkeeping it
   * published the total and queued the marker but left [[BackpressureProtocol]] believing the
   * stream was still live, and a live ledger is not inert: it keeps arming the liveness timers and
   * keeps contributing to the consumer-slowness comparison, so a partition that had finished
   * perfectly well went on to be measured as a silent producer and as a slow consumer for as long
   * as the reader held the handler. That is a fallback trip and a spurious producer-loss report
   * earned by a stream that did nothing wrong. Routing both paths through here makes the omission
   * unrepresentable.
   *
   * Called by, and only by, the thread that won the compare-and-set on
   * [[PartitionState.terminated]], which is what makes every step below exactly-once without a lock
   * of its own. The order matters: the total is published first, so no observer can read a
   * terminated stream whose total is still the sentinel; the ledger is retired next, which is what
   * stops both liveness timers; and the completion marker is queued last, behind its own latch, so
   * a reader cannot reconcile against a total that has not been written.
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
   * Refuses a terminator that does not describe this stream, and declares the producer lost.
   *
   * Escalation rather than a dropped frame, for the reason [[handleTermination]] states: a stream
   * whose producer claims an end it has not reached cannot be completed correctly, and the only
   * correct outcome is the one every other unrecoverable producer fault reaches -- a fetch failure
   * raised on the task thread, which the unmodified scheduler resolves by recomputing the upstream
   * stage. The typed condition names both totals, because a diagnostic that reported only the
   * claimed one would say nothing about the position the claim contradicts.
   *
   * @param state the partition whose stream was being ended
   * @param claimedTotal the block total the terminator claimed
   * @param reconciledTotal the total the claim is refused against -- the position this consumer has
   * actually reached, or the total an accepted terminator already
   * established
   */
  /**
   * Refuses a data block that arrived after this stream had ended, and declares the producer lost.
   *
   * Not dropped, and not admitted. Dropping it would leave the reduce task reading a stream whose
   * producer is demonstrably still sending output it has already declared finished -- so either the
   * terminator was wrong or this block is, and neither reading permits the partition to be reported
   * complete. Admitting it would be worse still: the stream's consumed count would pass the total
   * the terminator fixed, and [[StreamingShuffleReader]]'s completion reconciliation would compare
   * a count against a total the data had outgrown. The one safe answer is the one every other
   * unrecoverable producer fault reaches, a fetch failure and an upstream recomputation.
   *
   * @param state the partition whose stream had already ended
   * @param sequenceNumber the position the late block claimed
   */
  private def rejectBlockAfterTermination(state: PartitionState, sequenceNumber: Long): Unit = {
    // Bounded on the executor's window, because the frequency is the peer's to choose: a producer
    // that keeps sending past its own end of stream sends as many blocks as it likes, and every one
    // of them reaches here. The escalation below is once-only per partition, so without this the
    // record would be the only unbounded thing left on the path.
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

  /**
   * Refuses a control message that only ever travels in the other direction. Named with the type
   * that was expected in its place, so the diagnostic states what the channel is for rather than
   * only what arrived on it.
   *
   * Reported here on the executor's own window, and not left to [[escalate]], because escalation no
   * longer owns a default-level record and a frame travelling the wrong way is a protocol violation
   * an operator has to be able to see. A peer may send as many of them as it likes, so the record
   * has to be bounded by the executor rather than by the frame count.
   */
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
   * `AckMessage.NOTHING_CONSUMED` to state that it has consumed
   * nothing
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
   * is deterministic. Each partition contributes at most one message, because a deferred position
   * supersedes any earlier one outright.
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
          // backoff to elapse. Reporting this as a refusal would escalate a stage recomputation for
          // a block the producer is still holding and will still replay, so the quarantine stands
          // and retryDueReplays sends the request when the pause is over.
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
   * <b>One frame, and that is a correctness requirement rather than an economy.</b> A
   * retransmission request carries the window's inclusive upper bound as a field of its own, so a
   * run of positions is one request. Emitting a frame per position instead would split one repair
   * into siblings the producer cannot recognise as one: its replay budget and its backoff are per
   * request, so the first frame of a multi-position gap would arm the stream's pause and every
   * sibling behind it would be deferred, spend the budget re-asking for the first position, and
   * escalate to a stage recomputation while the producer still held every byte that was asked for.
   * The run is therefore named once and answered once -- one serviceability decision over the whole
   * range, one attempt charged, one backoff armed.
   *
   * The width is already bounded by the caller, which clamps a window to
   * [[StreamingShuffleClientHandler.MAX_REPLAY_WINDOW_SPAN]], and by the message type itself, which
   * refuses a window wider than `RetransmitRequestMessage.MAX_REQUESTED_BLOCKS`.
   *
   * @param channel the producer's channel, already checked to be active and writable by the caller
   * @param partitionId the stream being repaired
   * @param first inclusive first position to replay
   * @param last inclusive last position to replay, never below `first`
   * @return true when the request reached the channel, which is what makes the repair under way;
   *         false when it could not be written
   */
  private def writeReplayRequests(
      channel: Channel,
      partitionId: Int,
      first: Long,
      last: Long): Boolean = {
    writeMessage(channel,
      new RetransmitRequestMessage(shuffleId, mapId, partitionId, first, math.max(first, last)))
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
        writeHeartbeat(partitionId, buildHeartbeat(state))
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
   * that as "serve me from the beginning" rather than as "I have consumed block zero".
   *
   * The position and this handler's stable consumer identity form the resume handshake. The
   * identity survives a reconnect because it belongs to the reduce task attempt rather than to the
   * socket; the position says where that same attempt must resume.
   */
  private def buildHeartbeat(state: PartitionState): HeartbeatMessage = {
    new HeartbeatMessage(
      shuffleId, mapId, state.partitionId, announcedPosition(state), consumerToken)
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
   * <b>This method does not own a default-level record, and deliberately.</b> One producer loss was
   * being reported twice at warning level: here, on the I/O thread that noticed the channel, and
   * again by [[StreamingShuffleReader]] when it converted the marker into the fetch failure that
   * actually recovers the read -- before Spark then logged the failed task a third time. The
   * reader's record is the one to keep, because it is the only one that can state what the loss
   * cost and what happens next: the bytes discarded, the invalidation reason sent to the driver,
   * and that the upstream stage will be recomputed. Nor could this record be merely bounded instead
   * of demoted: a bound per handler is one record per producer channel, and a reduce task opens one
   * of those per map output it reads, so executor log volume would still scale with the product of
   * reduce tasks and map outputs. The record here is therefore kept under the streaming debug key,
   * with the cause attached; nothing is lost, because the notifier retains the root cause, the
   * per-channel tally is exposed for assertions, and every distinguishable protocol violation on
   * this path -- a late block, a refused end of stream, a frame that only travels the other way --
   * keeps an executor-bounded record of its own.
   */
  private def escalate(cause: Throwable, partitionId: Int): Unit = {
    escalate(cause, partitionId, StreamingShuffleInvalidationReason.ConnectionTimeout)
  }

  /**
   * The same escalation, attributing the loss to a specific invalidation reason.
   *
   * Separated so that the two callers who *know* what went wrong -- a producer that contradicted
   * its own end of stream, and one that kept sending past it -- can say so, while every
   * transport-level failure keeps the connection-timeout attribution without having to name it. The
   * reason travels to the reader on the marker and reaches the driver's invalidation telemetry
   * unchanged, so an operator sees a truncated stream reported as a truncated stream rather than as
   * a silent socket.
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
    // condition names the condition, the disagreeing numbers and the SQLSTATE. Reporting the class
    // name here left the scheduler's failure reason reading "was lost: SparkException." and put
    // everything actionable behind a getCause() walk.
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

  /**
   * Marks one partition's producer as gone, at most once, and wakes whoever is waiting on it.
   *
   * The invalidation of the blocks already accepted from that producer, and the fetch failure that
   * makes the scheduler recompute the upstream stage, belong to the reader: they have to be atomic
   * across everything the task has taken from this queue as well as everything still on it, and
   * only the task thread can see both. This handler's part is to say that no more is coming.
   */
  private def signalProducerLost(
      partitionId: Int,
      reason: String,
      cause: Throwable,
      invalidation: StreamingShuffleInvalidationReason =
        StreamingShuffleInvalidationReason.ConnectionTimeout): Unit = {
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
      enqueue(ProducerLost(partitionId, reason, cause, invalidation), 0L)
    }
  }

  // Observability, all of it derived from state this handler already keeps.

  /**
   * How many failures this handler has recorded on the shared error notifier.
   *
   * Read by the reader to attribute a latched failure to the producer whose channel produced it.
   * The count is the honest answer to "did this handler report anything", which a marker on the
   * hand-off queue is not: a marker is raised once per partition and can be refused by a full
   * queue, whereas every escalation increments this.
   */
  def reportedEscalationCount: Long = escalationsReported.get()

  /**
   * Whether THIS handler wants its channel to be reading.
   *
   * A handler's intent, not the state of the socket: the socket is shared with every other handler
   * multiplexed onto the same channel and obeys the union of their intents. Use
   * [[isChannelReadEnabled]] for the physical state.
   *
   * Derived from the gate's participant record rather than from a flag of this handler's own, so
   * there is exactly one place an intent is written and one place it is read. The monitor is not
   * taken here: this is a single read of one concurrent map, and a caller that needs the answer
   * and the transition it implies to be atomic takes [[gateLock]] and asks the gate directly, which
   * is what [[disableAutoRead]] and [[enableAutoRead]] do.
   */
  def isAutoReadEnabled: Boolean = !readGate.get().isThrottling(this)

  /**
   * Whether the physical channel is currently reading from its socket.
   *
   * This is the union across every participant of the channel, which is the only reading that
   * describes the socket: a channel stays closed while any participant is throttled.
   */
  def isChannelReadEnabled: Boolean = readGate.get().isReadEnabled

  /** How many participants of this handler's channel currently want reading stopped. */
  private[streaming] def throttlingParticipantCount: Int = readGate.get().throttlingParticipantCount

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

  /**
   * End-of-stream frames of one partition refused because they did not describe its stream.
   *
   * A non-zero reading is a producer that claimed an end it had not reached, or that contradicted
   * an end it had already announced; both are escalated rather than accepted, so this is the
   * observable evidence that a silent truncation was refused.
   */
  def rejectedTerminationCount(partitionId: Int): Long =
    readState(partitionId, 0L)(_.rejectedTerminations.get())

  /** End-of-stream frames of one partition ignored because an identical one had been accepted. */
  def duplicateTerminationCount(partitionId: Int): Long =
    readState(partitionId, 0L)(_.duplicateTerminations.get())

  /** Data blocks of one partition refused because that stream had already ended. */
  def blocksAfterTerminationCount(partitionId: Int): Long =
    readState(partitionId, 0L)(_.blocksAfterTermination.get())

  /**
   * Replacements of one partition admitted after that stream had ended.
   *
   * Zero unless task-thread verification found a block corrupt after its producer had already
   * committed to a total, which is an ordering the reader cannot prevent and must survive. A
   * non-zero reading is positive evidence that an in-window repair completed in that ordering
   * rather than being escalated into a fetch failure and a recomputation of the upstream stage.
   */
  def repairsAfterTerminationCount(partitionId: Int): Long =
    readState(partitionId, 0L)(_.repairsAfterTermination.get())

  /**
   * Block admissions whose share of the executor's shared receive budget was returned because this
   * handler closed while the admission was in flight.
   *
   * Zero on every ordinary run. A non-zero reading is positive evidence that the admission-versus-
   * closure race was taken and that the rollback path returned the charge, which is the one way to
   * tell "the race never happened" from "the race was handled".
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
   *
   * <b>Admissions in flight are settled before the ledgers are read.</b> The closed flag is raised
   * first, so every admission that has not yet reserved anything sees it and stops; the ones
   * already past that point are then waited out, so that by the time the per-partition ledgers are
   * summed and dropped, each in-flight admission has either recorded its charge on a ledger this
   * method is about to return, or has seen the closure and rolled its own charge back. Both
   * outcomes return the bytes exactly once. The wait is bounded and does not block indefinitely: an
   * admission is a handful of lock-free operations with no I/O and no lock in it, and the bound
   * exists only so that a pathologically descheduled data-plane worker cannot stall an executor's
   * shutdown -- a charge left behind by an admission that outlives the bound is still returned by
   * that admission's own rollback, so the accounting stays exact even in the case the bound exists
   * for.
   */
  def close(): Unit = close(releaseChannel = true)

  /**
   * Releases everything this handler holds, once, optionally leaving the channel open.
   *
   * <b>Why the channel is now separable from the handler.</b> A handler used to own its channel
   * outright, because there was one channel per handler: a reduce task opened a socket per producer
   * it read from. That is no longer true, and could not remain so -- a reduce task reading two
   * hundred map outputs from one executor opened two hundred sockets to one port, and a stage of
   * two hundred such tasks opened forty thousand, which is a connect storm that fails connections
   * rather than a shuffle that streams. Channels are now shared by the handlers of one consumer
   * talking to one producer executor, and the connector that owns the share is the only party that
   * can know when the last of them has finished with it. So a handler releasing itself out of a
   * share must leave the socket alone; a handler that owns its channel outright still closes it,
   * which is what the default preserves for every caller and every test that had one channel
   * apiece.
   *
   * @param releaseChannel whether to close the channel beneath this handler. False when the channel
   *                       is shared and its owner will release it once its last participant has
   * gone
   */
  def close(releaseChannel: Boolean): Unit = {
    if (closed.compareAndSet(false, true)) {
      awaitFrameTasksInFlight()
      awaitAdmissionsInFlight()
      val drained = drainAndRelease()
      // Everything still charged to the executor's shared budget comes back here, whether this
      // handler is closing after an orderly end of stream, after a producer failure or after task
      // cancellation. Without it a failed reduce task would permanently shrink the budget available
      // to every consumer that follows it on this executor.
      val returned = releaseAllQuota()
      partitions.clear()
      // Withdrawn before the channel reference is dropped, and the departure is recorded under the
      // same monitor as every other transition. A handler that closed while throttled would
      // otherwise leave its intent standing in a gate shared with the participants that outlive it
      // -- a closed receive window no living participant could ever reopen -- and a frame still in
      // flight on the event loop could re-state that throttle a moment after the withdrawal if the
      // departure were not part of the same transition.
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
      // Drained rather than read, so the ledger cannot report the same bytes twice. The ledgers are
      // dropped straight afterwards, but a state can still be reachable through a reference an
      // observer took before the closure, and a second read of an undrained ledger would return
      // bytes this call has already given back -- inflating the executor's budget instead of
      // restoring it.
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
 * The receive window of one '''physical''' channel, shared by every consumer handler multiplexed
 * onto it.
 *
 * <b>Why this exists at all.</b> A consumer channel now carries every producer a reduce task reads
 * from one executor, because a socket per producer is a socket per map output and therefore a
 * connect storm at any realistic shuffle width. `autoRead`, however, is a property of the socket
 * and not of a producer: with a flag per handler, two handlers on one channel each believed they
 * owned the window. Handler A throttling set the flag false for the whole socket; handler B --
 * whose own flag was still true, so it had no transition to make -- then resumed and set it true
 * again, and A could not put it back because its own compare-and-set had already moved. The socket
 * read on while a participant was out of credit, which is precisely the pressure the layer exists
 * to apply, and the block that arrived had nowhere to go but A's bounded queue.
 *
 * The rule this class enforces instead is the only one that is safe for a shared resource:
 * '''reading is disabled while ANY participant is throttled, and enabled only when none is.''' A
 * participant states its own intent; the socket obeys the union of those intents.
 *
 * <b>Why a participant must be able to leave.</b> A handler that closed while throttled would
 * otherwise hold the window shut for the participants that outlive it -- a throttle that no living
 * participant could ever clear. [[withdraw]] is therefore called from the handler's own close path,
 * and re-evaluates the socket exactly as a resume would.
 *
 * <b>Thread safety: one state machine, not three cells that happen to be atomic.</b> Recording a
 * participant's intent, computing the union, deciding whether the applied state moves and writing
 * `setAutoRead` are four steps that only mean anything performed together, so they are performed
 * together, under this gate's own monitor. An earlier revision made each step individually atomic
 * and left the sequence unserialized, which is not the same property and is not sufficient: with a
 * throttle and a resume in flight at once, one thread could read `throttling` as non-empty, the
 * other read it as empty, and whichever reached the applied-state update last decided the socket --
 * so a channel with no participant throttling could be left wedged shut, unreadable by every
 * producer multiplexed onto it, or a channel whose participant was out of credit could be left
 * reading into a bounded queue. Serialising the sequence is what makes the socket state a function
 * of the participants' intents rather than of a schedule.
 *
 * Holding the monitor across the channel write is deliberate and is what closes the last of that
 * race. It is also safe: `DefaultChannelConfig.setAutoRead` either calls `Channel.read()` or clears
 * a read-pending flag, and neither re-enters this gate or any participant -- for an NIO channel
 * `read()` sets an interest op, and for an embedded channel it does nothing -- so this monitor is a
 * leaf that no callback can acquire out of order. Participants take their own monitor before this
 * one and never the reverse, and every consequence of a transition that could re-enter a
 * participant -- the backpressure poll, the counters, the log records -- is issued by the caller
 * after its monitor has been released.
 *
 * The write is still issued only when the union actually changes, so a redundant configuration
 * write is skipped however many participants observe the same condition, and the common path is one
 * uncontended monitor acquisition with no allocation, no I/O and no blocking call inside it.
 *
 * <b>Containment.</b> This class is the single place in the subsystem that writes `setAutoRead` for
 * '''flow control''', and it lives in this file so that the containment claim in
 * [[StreamingShuffleClientHandler]]'s documentation stays checkable by inspection.
 *
 * There is exactly one other `setAutoRead` call anywhere in the subsystem, and it is not flow
 * control: `StreamingShuffleReader`'s package-private `pauseInboundTraffic`, which end-to-end
 * failure injection uses to make a live socket stop delivering bytes so that the reader's
 * application-level connection timeout is the path under test rather than the channel-closed path.
 * It is named here rather than glossed over because a containment claim that quietly excluded a
 * caller would be worth nothing. Two properties keep it harmless to this gate: it only ever
 * disables reading, so it cannot reopen a window a participant still needs shut, and it remembers
 * nothing -- the paused channels are closed by failed-task cleanup and a retry opens fresh ones in
 * the normal state -- so it can neither be cleared by a resume nor outlive the fault it injects.
 * What it does bypass is this gate's own bookkeeping, so the state this gate believes it applied
 * may afterwards disagree with the socket until the channel is closed; that is acceptable in
 * exactly the run where the channel is about to be discarded, and it is why the hook is confined to
 * the injection path and is not offered as a way to exert backpressure.
 */
private[streaming] final class StreamingShuffleChannelReadGate {

  /**
   * The one monitor every transition of this gate is performed under.
   *
   * Private and owned outright, so that a participant synchronising on the gate object itself could
   * neither serialise against these transitions by accident nor delay them: what is protected is
   * this gate's own state machine and nothing else.
   */
  private val stateLock = new Object

  /**
   * The participants that currently want reading stopped, mapped to why. Identity-keyed on the
   * participant itself, so a participant can state its intent idempotently and withdraw it exactly
   * once. Guarded by [[stateLock]]; read without it only through the size accessor, where a
   * momentarily stale count is a diagnostic rather than a decision.
   */
  private val throttling = new ConcurrentHashMap[AnyRef, String]()

  /**
   * The channel whose window this gate owns, or null until a participant attaches one. Written
   * under [[stateLock]] and kept in an atomic so accessors need not take the monitor to read it.
   */
  private val channelRef = new AtomicReference[Channel](null)

  /**
   * The state last written to the channel, so a redundant configuration write is skipped. Written
   * under [[stateLock]]; the atomic is here for visibility to [[isReadEnabled]], not to order
   * writes against each other -- the monitor does that.
   */
  private val applied = new AtomicBoolean(true)

  /**
   * Attaches the physical channel this gate governs and applies the standing state to it.
   *
   * Idempotent for the same channel, which matters because every participant announces the channel
   * it joined and the first of them may have throttled before the others arrived. A channel that
   * replaces another is adopted, because a gate travels with the share and a share is withdrawn
   * when its channel dies.
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
   * Identical to [[resume]] in effect and different in meaning: a participant that has closed is
   * not resuming, it is gone, and leaving its throttle behind would wedge the window shut for every
   * participant that outlives it.
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
   * This is the participant's intent, and it is the only record of it: a participant that kept a
   * flag of its own alongside this map would have two state machines to keep in step, and the pair
   * diverging is exactly how a participant ends up recorded here with no living code path able to
   * withdraw it. [[StreamingShuffleClientHandler]] therefore asks this question rather than
   * answering it for itself.
   *
   * @param participant the participant to ask about
   * @return true when the participant is holding the window shut
   */
  def isThrottling(participant: AnyRef): Boolean = throttling.containsKey(participant)

  /**
   * Applies the union of the participants' intents to the channel, writing only on a change.
   *
   * Must be called with [[stateLock]] held: the intent that was just recorded, the union computed
   * from it, the applied state and the channel write are one transition, and interleaving two of
   * them is the race this gate exists to prevent.
   *
   * @return true when this call changed the applied state
   */
  private def applyUnionLocked(): Boolean = {
    val wanted = throttling.isEmpty
    val changed = applied.getAndSet(wanted) != wanted
    val channel = channelRef.get()
    if (channel != null) {
      // Written whenever the socket disagrees with the union, not only when the union moved. The
      // second case is a gate that has just adopted a channel: the union did not change, but a
      // channel attached while a participant was throttled would read on until something else moved
      // it. Reading the channel's own state is what makes this idempotent rather than repetitive.
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
   * The executor-scoped windows bounding every recurring, default-level record a consumer makes,
   * one per condition.
   *
   * '''Why the scope is the executor and not the handler.''' One of these handlers exists per
   * producer channel, and a reduce task opens one channel per map output it reads -- so a window
   * owned by a handler admits that handler's first occurrence of a condition whatever the executor
   * has already reported, and executor log volume then scales with the product of reduce tasks and
   * map outputs rather than with time. Every one of these conditions is also provoked at the other
   * end of a socket: a producer sending a corrupt block per block, a misaddressed frame per frame,
   * a block past its own end of stream, a control frame travelling the wrong way. A per-handler
   * bound therefore left the volume in the peer's hands twice over. The gate was never wrong; its
   * scope was. See [[MemorySpillManager.ExecutorLogAggregator]] for the same argument stated once,
   * and [[StreamingShuffleClientHandler.reportBounded]] for what an admitted record quotes.
   *
   * One aggregator per condition, deliberately: a burst of misaddressed frames must not silence the
   * first checksum repair, because the two send an operator to look at different things.
   *
   * `private[streaming]` rather than private so that the shared test seam can return them to their
   * initial state; nothing in service resets them, for the reason set out on
   * [[MemorySpillManager.LogAggregationGate.reset]].
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

  /** Every executor-scoped aggregator this class owns, for the test seam's bulk reset. */
  private val logAggregators: Seq[MemorySpillManager.ExecutorLogAggregator] =
    Seq(repairLogAggregator, replayBudgetLogAggregator, foreignChannelLogAggregator,
      misaddressedFrameLogAggregator, lateBlockLogAggregator, rejectedTerminationLogAggregator,
      wrongDirectionLogAggregator)

  /**
   * Returns the executor-scoped log aggregation above to its initial state.
   *
   * Reached only through [[MemorySpillManager.resetSharedStateForTesting]], so that a suite has one
   * call to make rather than one per component, and never called in service for the reason set out
   * there: the bound belongs to the executor's lifetime.
   */
  private[streaming] def resetLogAggregationForTesting(): Unit = {
    logAggregators.foreach(aggregator => aggregator.reset())
  }

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
   * Widest run of positions a single repair may cover, so that an over-wide window is narrowed
   * rather than refused.
   *
   * A repair of `n` positions puts exactly one request on the wire, naming the closed interval, so
   * this bound is a bound on the work one repair may ask of a producer rather than on the number of
   * frames. It is deliberately the same value the message type enforces --
   * `RetransmitRequestMessage.MAX_REQUESTED_BLOCKS` -- so that a window this handler narrows is
   * always one the wire will accept, and it is set generously rather than tightly: at the
   * two-mebibyte block cap, 4096 blocks is eight gibibytes of replay, far more than a producer can
   * be holding when buffers are capped at half of executor memory. It never narrows a window a real
   * consumer would open, while narrowing every window no real consumer could.
   */
  val MAX_REPLAY_WINDOW_BLOCKS: Long = RetransmitRequestMessage.MAX_REQUESTED_BLOCKS

  /** Widest span between the two ends of a repair, the run being inclusive of both. */
  val MAX_REPLAY_WINDOW_SPAN: Long = MAX_REPLAY_WINDOW_BLOCKS - 1L

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
  val MAX_QUARANTINED_POSITIONS: Int = (4L * MAX_REPLAY_WINDOW_BLOCKS).toInt

  /** Sentinel for a timestamp that has not been taken. */
  val NO_TIMESTAMP: Long = Long.MinValue

  /** Bound for close waiting on decoded frames already accepted by the worker stripes. */
  val FRAME_TASK_SETTLE_TIMEOUT_MS: Long = 1000L

  /** Bound for close waiting on an admission already past its quota reservation. */
  val ADMISSION_SETTLE_TIMEOUT_MS: Long = 100L

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
   * @param invalidation how the loss should be attributed when the reader invalidates what it took
   *                     from this producer. Carried on the marker because only the component that
   *                     observed the loss can tell a silent channel from a producer that
   * contradicted                     its own end of stream, and the two are different facts for an
   * operator                     reading the invalidation telemetry. Defaults to the connection
   * timeout, which                     is what every transport-level loss is.
   */
  final case class ProducerLost(
      partitionId: Int,
      reason: String,
      cause: Throwable,
      invalidation: StreamingShuffleInvalidationReason =
        StreamingShuffleInvalidationReason.ConnectionTimeout)
    extends Inbound

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

    /**
     * An end-of-stream this consumer has been told about but cannot apply yet, or
     * [[StreamingShuffleClientHandler.NO_BLOCK_TOTAL]] when there is none.
     *
     * A producer that has committed every position it names may announce the end of its stream
     * while a replay this consumer asked for is still in flight. The announcement is true and must
     * not be refused; it simply cannot be applied until the repair lands, so it is held here and
     * applied by the admission that closes the gap.
     */
    private val deferredTermination = new AtomicLong(NO_BLOCK_TOTAL)

    /** How many end-of-stream announcements had to be held for a repair. */
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
     * replacement, or a replay window has been recorded and not yet discarded. Either means blocks
     * the producer committed are still expected to arrive.
     */
    def hasOutstandingRepairs: Boolean = !quarantined.isEmpty || !replayWindows.isEmpty

    /** Highest position acknowledged, or the nothing-consumed sentinel. */
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

    /** Local instant at which the producer's last heartbeat arrived. Diagnostic only. */
    val remoteHeartbeatMillis = new AtomicLong(NO_TIMESTAMP)

    /**
     * Whether the producer has announced the orderly end of this stream.
     *
     * Raised by compare-and-set in
     * [[StreamingShuffleClientHandler.handleTermination]] and never lowered, so the transition into
     * a terminated stream happens exactly once and the total published alongside it is immutable
     * from that instant. Everything downstream -- the completion marker on the hand-off queue, the
     * reader's reconciliation against it, and the refusal of any later block -- depends on that
     * being a latch rather than an assignment.
     */
    val terminated = new AtomicBoolean(false)

    /**
     * Blocks the producer claims to have sent, or [[NO_BLOCK_TOTAL]] before it has said.
     *
     * Written exactly once, by the thread that wins [[terminated]], and only after the claim has
     * been reconciled against the position this consumer actually reached.
     */
    val announcedBlocks = new AtomicLong(NO_BLOCK_TOTAL)

    /** End-of-stream frames refused because they did not describe this stream. */
    val rejectedTerminations = new AtomicLong(0L)

    /** End-of-stream frames ignored because an identical one had already been accepted. */
    val duplicateTerminations = new AtomicLong(0L)

    /** Data blocks refused because this stream had already ended. */
    val blocksAfterTermination = new AtomicLong(0L)

    /**
     * Replacements admitted after this stream had ended.
     *
     * Zero on a stream whose blocks all verified on the task thread before its terminator arrived.
     * A non-zero reading is the ordering the reader cannot prevent -- task-thread verification runs
     * behind the channel, so a repair can be asked for after the producer has already committed to
     * a total -- and it is counted rather than merely permitted so that a suite can prove the
     * repair completed instead of inferring it from the absence of a failure.
     */
    val repairsAfterTermination = new AtomicLong(0L)

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
     * Takes every byte this ledger still holds and empties it, in one step.
     *
     * The emptying is what makes the answer safe to act on: the caller is returning these bytes to
     * the executor's shared budget, and a ledger that still reported them afterwards could have
     * them returned a second time. Emptying it by crediting the acknowledged total rather than by
     * zeroing both totals keeps the received figure available as the diagnostic it is, while making
     * [[outstandingBytes]] read zero from this point on.
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
