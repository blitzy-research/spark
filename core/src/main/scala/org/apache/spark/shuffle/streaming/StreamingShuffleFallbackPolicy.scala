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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys.{CONFIG, COUNT, ELAPSED_TIME, MAX_SIZE, MEMORY_SIZE,
  NUM_BYTES, PERCENT, PROTOCOL_VERSION, RATIO, REASON, SHUFFLE_ID, THRESHOLD, VALUE, VERSION_NUM}
import org.apache.spark.network.shuffle.protocol.streaming.StreamingShuffleMessage
import org.apache.spark.util.{Clock, SystemClock}

/**
 * Why the streaming shuffle stepped aside in favour of sort-based shuffle.
 *
 * The four members of this type are exactly the four conditions the streaming shuffle is specified
 * to degrade on, and they are modelled as values rather than as strings so that every consumer
 * matches on them exhaustively and names the precise one that fired. Nothing else about a
 * reason is behavioural: every one of them routes to the identical terminus, which is delegation to
 * the unmodified sort-based shuffle. A reason is therefore diagnostic information, never a
 * behavioural switch.
 */
private[spark] sealed trait StreamingShuffleFallbackReason {

  /**
   * A one-clause, operator-facing rendering of this condition, phrased so that it composes into the
   * single warning the policy emits when it trips. There is no leading capital and no terminating
   * punctuation, precisely so it can be embedded mid-sentence.
   *
   * `toString` is deliberately left as the synthesised case-object name, because that is the stable
   * identifier a test asserts on, while this is the prose an operator reads.
   */
  def description: String
}

/**
 * The four fallback conditions, and nothing else.
 *
 * This set is closed on purpose. A fifth condition would be a fifth way for streaming shuffle to
 * abandon the fast path, and every such way has to be specified, tested and documented before it
 * can be trusted; the sealed trait above makes adding one a compile-time visible act rather than an
 * incidental one.
 */
private[spark] object StreamingShuffleFallbackReason {

  /**
   * The two ends of a pipelined shuffle could not be kept in step.
   *
   * The condition has a '''measured''' form and an '''observed''' form, and they are one condition
   * rather than two because the thing that has failed is the same in both: the producer and the
   * consumer of one shuffle can no longer sustain a pipeline between them.
   *
   *  - Measured: the consumer has been unable to keep up with the producer by the tolerated factor,
   *    continuously for longer than the tolerated window. Sustained rather than instantaneous,
   *    because a momentary stall is ordinary flow control that backpressure and spill already
   *    absorb and must not cost the job its fast path.
   *  - Observed: a consumer could not resolve a producer at all, or producers of the shuffle kept
   *    being lost to the connection timeout across successive recomputations. The window of the
   *    measured form is sixty seconds and the connection timeout is five, so a stream that keeps
   *    dying is never available to be measured -- which is precisely why the observed form exists.
   *
   * The rendering below therefore names the failure rather than one of its two symptoms, and the
   * free-text detail recorded alongside the verdict states which observation was actually made.
   */
  case object ConsumerTooSlow extends StreamingShuffleFallbackReason {
    override val description: String =
      "a streaming shuffle producer and consumer could not be kept in step: either the consumer " +
        "stayed at least 2x slower than the producer for more than 60 seconds, or its producers " +
        "kept being lost to the connection timeout"
  }

  /**
   * A streaming shuffle buffer reservation could not be satisfied even after eviction, so
   * continuing to buffer would risk exhausting executor memory. Streaming trades memory for
   * latency; when the memory is not there, the trade is off.
   */
  case object MemoryPressure extends StreamingShuffleFallbackReason {
    override val description: String =
      "memory pressure prevented a streaming shuffle buffer allocation, risking memory exhaustion"
  }

  /**
   * Egress is consuming more of the administered link than the policy tolerates. Pipelining a
   * shuffle across a link that is already saturated buys no latency and starves every other tenant
   * of the same link.
   */
  case object NetworkSaturation extends StreamingShuffleFallbackReason {
    override val description: String =
      "network utilisation exceeded 90% of the administered link capacity"
  }

  /**
   * A peer announced a streaming shuffle protocol revision this build cannot speak. Detected by an
   * explicit compatibility check on the version byte the wire header carries, never inferred from a
   * decode failure, so a rolling upgrade degrades deterministically instead of misreading frames.
   */
  case object ProtocolVersionMismatch extends StreamingShuffleFallbackReason {
    override val description: String =
      "a producer/consumer streaming shuffle protocol version mismatch was detected"
  }

  /**
   * Every reason, in the order the specification enumerates them. Exposed so that the closed set
   * can be walked without restating it, and so that adding a member cannot silently escape a
   * consumer that walks it.
   */
  val all: Seq[StreamingShuffleFallbackReason] =
    Seq(ConsumerTooSlow, MemoryPressure, NetworkSaturation, ProtocolVersionMismatch)

  /**
   * Resolves a reason from its stable name, answering `None` for anything outside the closed set.
   *
   * A shuffle-wide fallback is agreed over RPC, so the condition has to cross a process boundary,
   * and it crosses as this name rather than as the object. Two properties follow, and both matter.
   * The name is stable -- it is the synthesised case-object name, which is also what tests assert
   * on -- so it survives serialization without depending on how a `case object` happens to be
   * reconstructed. And resolution is a lookup in a set this build owns, so a peer can neither
   * introduce a fifth condition nor choose the text of a driver log record: an unrecognised name
   * resolves to `None` and is refused at the boundary rather than recorded.
   *
   * @param name candidate reason name, which may be `null`
   * @return the matching reason, or `None` when this build does not know the name
   */
  def fromName(name: String): Option[StreamingShuffleFallbackReason] = {
    if (name == null) None else all.find(_.toString == name)
  }
}

/**
 * Decides when the streaming shuffle must stop streaming and let sort-based shuffle take over.
 *
 * This class is the load-bearing element of the streaming shuffle's zero-regression guarantee. It
 * exists so that the guarantee is structural rather than merely tested: there is no configuration,
 * no failure and no resource condition under which a job is left without a functioning shuffle
 * implementation, because every condition this class recognises routes to the same terminus as the
 * operator kill switch -- delegation to an internally held, completely unmodified
 * `SortShuffleManager`. A trip therefore costs throughput and latency. It never costs correctness,
 * and it never costs the job.
 *
 * ==Coexistence strategy==
 *
 * Streaming shuffle coexists with sort-based shuffle; it never replaces it. `SortShuffleManager`
 * remains both the default and the fallback, and the whole of `org.apache.spark.shuffle.sort` stays
 * byte for byte as it was. The two are joined by a two-tier activation model, and both tiers end in
 * the same place:
 *
 *  - Tier 1, `spark.shuffle.manager=streaming`, selects which manager class Spark instantiates. Any
 *    other value, including the `sort` default, means this class is never even constructed.
 *  - Tier 2, `spark.shuffle.streaming.enabled` (default `false`), gates the behaviour of the class
 *    Tier 1 selected. While it is `false`, `StreamingShuffleManager` forwards every
 *    service-provider call verbatim to its internal delegate, built as
 *    `new SortShuffleManager(conf)` -- note the single argument, which is the whole of that class's
 *    constructor. That is the operator kill switch: it restores sort behaviour without redeploying
 *    a different manager class.
 *
 * A trip observed by this policy shares that identical terminus, including mid-execution. The
 * manager asks exactly one question, [[shouldDelegateToSortShuffle]], and the answer folds the kill
 * switch and all four trip conditions into a single boolean. There is deliberately no second exit
 * and no bespoke recovery mechanism: everything either degrades to retransmission inside the
 * unacknowledged window, to spill, or to this delegation.
 *
 * ==The four trip conditions==
 *
 * Each is fed by the component that already measures it, so nothing is re-derived here:
 *
 *  - [[recordProducerThroughput]] and [[recordConsumerThroughput]] carry rate samples from the
 *    backpressure protocol, which already tracks acknowledgement progress and pending volume.
 *    Sustained slowness trips [[StreamingShuffleFallbackReason.ConsumerTooSlow]].
 *  - [[recordAllocationGrant]] carries the outcome of a buffer reservation from the memory spill
 *    manager. A partial grant is Spark's own idiom for memory pressure -- `acquireMemory` answers
 *    with the amount it could actually grant -- and a partial grant that eviction could not reverse
 *    trips [[StreamingShuffleFallbackReason.MemoryPressure]].
 *  - [[recordLinkUtilization]] carries egress against the administered link capacity, and
 *    saturation trips [[StreamingShuffleFallbackReason.NetworkSaturation]].
 *  - [[checkProtocolVersion]] performs an explicit compatibility check on the version byte in the
 *    wire header, and an incompatible peer trips
 *    [[StreamingShuffleFallbackReason.ProtocolVersionMismatch]].
 *
 * One invariant runs through all four, and it is what makes the policy safe to consult from
 * anywhere: '''an un-evaluable sample never trips.''' A rate that is not a finite non-negative
 * number, a reservation of nothing, a link whose capacity is unknown -- none of these is evidence
 * of anything, and a component whose purpose is to keep the job running must never degrade it on
 * the strength of noise. Un-evaluable samples are counted in [[unevaluableSampleCount]] rather than
 * acted upon.
 *
 * ==Latching, and why==
 *
 * The tripped state latches: once tripped, this instance stays tripped until [[reset]] is called.
 * Streaming and sort-based shuffle cannot both own the same shuffle, so oscillating back to
 * streaming mid-shuffle would mean two producers of the same data. Latching also makes the decision
 * monotone, which is what allows [[hasTripped]] to be a single atomic read on a hot path: one
 * volatile load, no allocation, no lock and no clock read. [[reset]] exists for tests and for reuse
 * of an instance across independent workloads, not as a runtime recovery path.
 *
 * ==Timing and determinism==
 *
 * No wall-clock or nanosecond call appears anywhere in this class. Every instant either arrives as
 * an explicit `nowMs` argument or comes from the injected [[Clock]], and nothing here ever sleeps
 * or waits. That is what lets the sustained-slowness window be exercised exactly at its boundary by
 * a test holding a stepped clock, with no timing tolerance and no flakiness.
 *
 * ==Configuration==
 *
 * All five streaming shuffle configuration values are read once, during construction, and held
 * immutably. That makes "streaming shuffle configuration changes require an executor restart" true
 * by construction and removes any need for a dynamic-reconfiguration path. The keys themselves are
 * declared centrally in `org.apache.spark.internal.config`; this class only consumes them.
 *
 * ==Thread safety==
 *
 * Every method is safe to call concurrently. The decision surface never blocks. The only monitor
 * taken is a per-shuffle one, held for the few field updates of a single throughput sample and
 * never across I/O, a callback or an allocation of consequence, so a producer thread, a consumer
 * thread and a Netty event-loop thread can all report into the same instance without contending on
 * anything shared.
 *
 * @param conf the executor's configuration, read once here and never consulted again
 * @param clock the time source for trip timestamps and for the sampling overloads that do not take
 *              an explicit instant; injected so every trip instant is a function of this clock
 *              rather than of wall time, and so nothing in this class ever sleeps
 */
private[spark] class StreamingShuffleFallbackPolicy(
    conf: SparkConf,
    clock: Clock = new SystemClock)
  extends Logging {

  import StreamingShuffleFallbackPolicy._
  import StreamingShuffleFallbackReason._

  // Configuration is read exactly once, here, and held immutably for the life of the component.
  // Nothing below re-reads `conf`, which is what makes the restart-to-reconfigure contract a
  // property of the code rather than a note in the documentation.
  private val streamingEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_ENABLED)
  private val bufferSizePercent: Int = conf.get(config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)
  private val spillThresholdPercent: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)
  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  /**
   * The administered link capacity in bytes per second, or `None` when no capacity is administered.
   *
   * `spark.shuffle.streaming.maxBandwidthMBps` is an optional entry, and absence means unlimited --
   * emphatically not zero. An unlimited link has no capacity figure to divide by, so saturation
   * simply cannot be evaluated against it, and a condition that cannot be evaluated never trips.
   * This is also why the value is kept as an `Option` rather than flattened to a sentinel: a
   * sentinel would eventually be compared with `>` by someone and would silently mean "always
   * saturated" or "never saturated" depending on which sentinel was chosen.
   *
   * Note that this is the declared capacity of the link, unscaled. The 80% share that the egress
   * token bucket holds itself to is a different constant with a different job, and the two must not
   * be conflated: 80% is the rate ceiling streaming imposes on itself, while
   * [[StreamingShuffleFallbackPolicy.SATURATION_TRIP_PERCENT]] is the point past which the link is
   * considered saturated and streaming stands down altogether.
   */
  private val administeredCapacityBytesPerSecond: Option[Long] =
    conf.get(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS).map(_.toLong * BYTES_PER_MIB)

  /**
   * The latched reason, or `None` while the policy has not tripped.
   *
   * An `AtomicReference` over an `Option` rather than a nullable reference: `None` is a singleton
   * so the compare-and-set that latches the first reason is a plain reference comparison, and
   * reading the state allocates nothing because the stored `Option` is handed straight back.
   */
  private val trippedReasonRef =
    new AtomicReference[Option[StreamingShuffleFallbackReason]](None)

  /** When the policy latched, in milliseconds, or `UNSET_TIME_MS` while it has not. */
  private val trippedAtMs = new AtomicLong(UNSET_TIME_MS)

  /** How many trip conditions have been observed in total, including after latching. */
  private val observedTripConditions = new AtomicLong(0L)

  /** How many samples were rejected as un-evaluable rather than acted upon. */
  private val unevaluableSamples = new AtomicLong(0L)

  /** How many samples named a shuffle past [[MAX_TRACKED_SHUFFLES]] and were therefore dropped. */
  private val untrackedShuffleSamples = new AtomicLong(0L)

  /**
   * Per-shuffle throughput state, keyed by shuffle id.
   *
   * Bounded by [[MAX_TRACKED_SHUFFLES]] so that a caller which never deregisters cannot grow this
   * map without limit; that bound is the reason a long-running executor cannot leak here.
   */
  private val throughputWindows = new ConcurrentHashMap[Int, ThroughputWindow]()

  /**
   * Shuffles whose fallback this executor has already declared to the coordinator.
   *
   * A trip latches once per executor but has to be declared once per shuffle, because the decision
   * that has to be agreed is "this shuffle stands down", not "this executor stands down". The set
   * is what makes the declaration exactly-once per shuffle from this executor: every producer and
   * consumer of the shuffle running here observes the same latch, and without this they would each
   * send the same declaration. Bounded by [[MAX_TRACKED_SHUFFLES]], with an over-bound claim
   * answered `true` rather than refused, because a duplicate declaration is harmless -- the
   * coordinator's own latch is first-writer-wins -- whereas a suppressed one would leave a shuffle
   * streaming after streaming had been withdrawn.
   */
  private val announcedShuffles: java.util.Set[Int] =
    ConcurrentHashMap.newKeySet[Int]()

  /**
   * The shuffle-wide fallback verdict this executor knows about, keyed by shuffle id.
   *
   * The executor-local latch and this map answer different questions, and both have to be asked.
   * The latch says streaming is no longer sustainable here; this map says a particular shuffle has
   * stood streaming down everywhere, which is a fact that may have been established by a trip on an
   * entirely different executor. A producer that consulted only the latch would keep streaming a
   * shuffle whose consumers had already been told to stop reading it.
   *
   * It is a cache of a verdict held authoritatively by the coordinator, populated by whichever
   * component learns it first -- a consumer from a lookup reply, a producer from its own
   * declaration, the manager from a registration grant or an explicit query -- so that every other
   * component on the executor can then read it for the cost of one map lookup instead of one ask.
   * The verdict latches at the coordinator, so a cached entry can never become stale in the
   * direction that matters.
   *
   * Bounded by [[MAX_TRACKED_SHUFFLES]] for the same reason the sampling map is, and released by
   * [[forgetShuffle]] when a shuffle is unregistered.
   */
  private val shuffleFallbacks = new ConcurrentHashMap[Int, StreamingShuffleFallbackState]()

  logConstruction()
  logSaturationCoverage()

  // The decision surface. Hot path: lock-free, allocation-free, clock-free.

  /**
   * Whether a fallback condition has been observed and latched.
   *
   * This is consulted on hot paths, so it is deliberately nothing more than one volatile load and
   * one predicate: no lock, no allocation, no clock read and no arithmetic. Callers may treat it as
   * free enough to test per block.
   */
  def hasTripped: Boolean = trippedReasonRef.get().isDefined

  /**
   * The reason the policy tripped, or `None` while it has not.
   *
   * Because the state latches, this is the '''first''' condition observed, which is the one worth
   * reporting: later conditions are frequently consequences of the first.
   */
  def trippedReason: Option[StreamingShuffleFallbackReason] = trippedReasonRef.get()

  /**
   * The instant the policy latched, in milliseconds as reported by the injected clock, or `None`
   * while it has not tripped.
   */
  def trippedAtTimeMillis: Option[Long] = {
    val stamped = trippedAtMs.get()
    if (stamped == UNSET_TIME_MS) None else Some(stamped)
  }

  /**
   * Whether the operator has enabled streaming behaviour at all, that is the Tier 2 gate
   * `spark.shuffle.streaming.enabled`, which defaults to `false`.
   *
   * This reports the configured intent only. It says nothing about whether streaming is currently
   * usable; [[streamingActive]] answers that.
   */
  def streamingEnabledByConfig: Boolean = streamingEnabled

  /**
   * Whether the operator kill switch is engaged, that is `spark.shuffle.streaming.enabled=false`.
   *
   * Distinguished from a trip on purpose. Both delegate to sort-based shuffle, but only one of them
   * indicates that streaming was attempted and found wanting, and an operator reading
   * [[shouldDelegateToSortShuffle]] alone could not tell the two apart.
   */
  def killSwitchEngaged: Boolean = !streamingEnabled

  /**
   * Whether the streaming path may handle work right now: enabled by configuration and not tripped.
   *
   * Both tiers of the activation model and all four trip conditions collapse into this one boolean,
   * which is the point of the class.
   */
  def streamingActive: Boolean = streamingEnabled && trippedReasonRef.get().isEmpty

  /**
   * Whether the caller must hand this call to the sort-based shuffle instead.
   *
   * The exact negation of [[streamingActive]], named for the call site: `StreamingShuffleManager`
   * asks this at the top of every service-provider method and, when the answer is `true`, forwards
   * the call verbatim to its internal `SortShuffleManager`. Kill switch and trip share this single
   * terminus, which is why there is no configuration or failure that leaves a job without a working
   * shuffle.
   */
  def shouldDelegateToSortShuffle: Boolean = !streamingActive

  /**
   * How many trip conditions have been observed in total, including those observed after the policy
   * had already latched.
   *
   * Latching hides repetition from [[trippedReason]], and this counter is where that repetition
   * remains visible -- useful when diagnosing whether a shuffle brushed a single condition once or
   * is being held down by it continuously.
   */
  def observedTripConditionCount: Long = observedTripConditions.get()

  /**
   * How many samples were rejected as un-evaluable rather than acted upon.
   *
   * Lets a caller, and a test, distinguish "did not trip because the measurement was within
   * tolerance" from "did not trip because there was no usable measurement at all". Those look
   * identical from [[hasTripped]] and are entirely different situations.
   */
  def unevaluableSampleCount: Long = unevaluableSamples.get()

  /**
   * How many samples were dropped because they named a shuffle beyond the tracking bound.
   *
   * Non-zero here means throughput sampling is no longer complete for this executor, which is worth
   * knowing: the sustained-slowness condition can only fire for a shuffle that is being tracked.
   */
  def untrackedShuffleSampleCount: Long = untrackedShuffleSamples.get()

  /** How many shuffles currently have throughput state, bounded by [[MAX_TRACKED_SHUFFLES]]. */
  def trackedShuffleCount: Int = throughputWindows.size()

  // Trip 1: the consumer sustained at 2x slower than the producer for more than 60 seconds.

  /**
   * Records the rate at which a shuffle's producer is emitting bytes.
   *
   * Samples come from the backpressure protocol, which already measures acknowledgement progress
   * and pending volume; this class never re-derives a rate from the network. A sample that is not a
   * finite non-negative number is counted as un-evaluable and clears the previous reading rather
   * than being silently ignored, so a conclusion is never drawn from a stale rate paired with a
   * fresh one.
   *
   * @param shuffleId the shuffle the sample belongs to
   * @param bytesPerSecond the producer's measured rate, in bytes per second
   * @param nowMs the instant of the measurement, in milliseconds
   */
  def recordProducerThroughput(shuffleId: Int, bytesPerSecond: Double, nowMs: Long): Unit = {
    recordThroughput(shuffleId, bytesPerSecond, nowMs, fromProducer = true)
  }

  /**
   * Records a producer rate sample taken now, as reported by the injected clock.
   *
   * A convenience for a caller with no reason to name an instant. The three-argument form is the
   * primitive: it takes the instant explicitly, which is the only way the sustained window can be
   * walked exactly to its boundary rather than merely approached.
   */
  def recordProducerThroughput(shuffleId: Int, bytesPerSecond: Double): Unit = {
    recordThroughput(shuffleId, bytesPerSecond, clock.getTimeMillis(), fromProducer = true)
  }

  /**
   * Records the rate at which a shuffle's consumer is draining bytes.
   *
   * Evaluated against the most recent producer sample for the same shuffle. The condition holds
   * only while '''both''' rates are known and the producer is actually producing, so an idle
   * shuffle, where both rates are zero, can never trip: zero is not 2x slower than zero in any
   * sense that should cost a job its fast path.
   *
   * @param shuffleId the shuffle the sample belongs to
   * @param bytesPerSecond the consumer's measured rate, in bytes per second
   * @param nowMs the instant of the measurement, in milliseconds
   */
  def recordConsumerThroughput(shuffleId: Int, bytesPerSecond: Double, nowMs: Long): Unit = {
    recordThroughput(shuffleId, bytesPerSecond, nowMs, fromProducer = false)
  }

  /** Records a consumer rate sample taken now, as reported by the injected clock. */
  def recordConsumerThroughput(shuffleId: Int, bytesPerSecond: Double): Unit = {
    recordThroughput(shuffleId, bytesPerSecond, clock.getTimeMillis(), fromProducer = false)
  }

  /**
   * The instant at which a shuffle's consumer was first seen to be behind by the tolerated factor
   * and has been continuously since, or `None` when it is currently keeping up.
   *
   * Exposed because the two halves of "sustained" are separately observable: the timer arms when
   * the deficit appears, and it clears the moment the consumer recovers.
   */
  def slownessArmedSinceMs(shuffleId: Int): Option[Long] = {
    val window = throughputWindows.get(shuffleId)
    if (window == null) None else window.armedSinceMs
  }

  /**
   * Applies one throughput sample and escalates if it completes a sustained deficit.
   *
   * A single private path for both directions keeps the arming, clearing and boundary arithmetic in
   * exactly one place, which is the only way to be sure the producer and consumer sides cannot
   * disagree about when the window opened.
   */
  private def recordThroughput(
      shuffleId: Int,
      bytesPerSecond: Double,
      nowMs: Long,
      fromProducer: Boolean): Unit = {
    windowFor(shuffleId).foreach { window =>
      val sample = if (fromProducer) {
        window.recordProducer(bytesPerSecond, nowMs)
      } else {
        window.recordConsumer(bytesPerSecond, nowMs)
      }
      if (sample.sustainedMs != NOT_SUSTAINED) {
        trip(ConsumerTooSlow,
          log"shuffle ${MDC(SHUFFLE_ID, shuffleId)} produced at " +
            log"${MDC(NUM_BYTES, sample.producerBytesPerSecond)} bytes/s while its consumer " +
            log"sustained only ${MDC(VALUE, sample.consumerBytesPerSecond)} bytes/s, a shortfall " +
            log"of at least ${MDC(RATIO, CONSUMER_SLOWNESS_RATIO)}x held continuously for " +
            log"${MDC(ELAPSED_TIME, sample.sustainedMs)} ms, longer than the " +
            log"${MDC(THRESHOLD, SUSTAINED_SLOWNESS_WINDOW_MS)} ms this policy tolerates.")
      }
    }
  }

  /**
   * The throughput state for a shuffle, creating it on first use, or `None` once the tracking bound
   * is reached.
   *
   * The bound is what keeps a caller that never deregisters from growing the map without limit. It
   * is enforced by refusing admission rather than by eviction, because evicting a window would
   * silently reset a sustained-slowness timer that had nearly elapsed, turning a bound on memory
   * into a bound on correctness.
   *
   * Admission and insertion happen in one atomic `compute`, and they have to. Reading the size and
   * then inserting are two steps, and every thread that passed the read before any of them inserted
   * would go on to insert: the bound would then be exceeded by as many entries as there were
   * concurrent first samples, which on an executor sampling from several producer and consumer
   * threads at once is precisely the situation the bound exists for. Consulting `size()` inside the
   * remapping function is safe -- it reads the map's own counters without taking a bin lock, so it
   * neither deadlocks against the update in progress nor blocks another shuffle's insertion.
   *
   * A fast path reads the map first, so the overwhelmingly common case of a shuffle that is already
   * tracked costs one lookup and takes no lock at all.
   */
  private def windowFor(shuffleId: Int): Option[ThroughputWindow] = {
    val existing = throughputWindows.get(shuffleId)
    if (existing != null) {
      Some(existing)
    } else {
      var refused = false
      val admitted = throughputWindows.compute(shuffleId,
        (_: Int, current: ThroughputWindow) =>
          if (current != null) {
            current
          } else if (throughputWindows.size() >= MAX_TRACKED_SHUFFLES) {
            refused = true
            null
          } else {
            new ThroughputWindow
          })
      if (refused) {
        noteUntrackedShuffle(shuffleId)
        None
      } else {
        Some(admitted)
      }
    }
  }

  /** Counts a dropped sample, and says so once at warning level and thereafter only under debug. */
  private def noteUntrackedShuffle(shuffleId: Int): Unit = {
    val dropped = untrackedShuffleSamples.incrementAndGet()
    if (dropped == 1L) {
      logWarning(log"Streaming shuffle fallback policy is already tracking " +
        log"${MDC(COUNT, MAX_TRACKED_SHUFFLES)} shuffles, so throughput samples for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} are being dropped; consumer-slowness fallback cannot " +
        log"fire for untracked shuffles. Deregister completed shuffles to restore tracking.")
    } else if (debugEnabled) {
      logInfo(log"Dropped a streaming shuffle throughput sample for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)}; ${MDC(COUNT, dropped)} samples dropped so far")
    }
  }

  // Trip 2: memory pressure prevented a buffer allocation, so streaming risks exhausting memory.

  /**
   * Records the outcome of a streaming shuffle buffer reservation, and trips when the reservation
   * could not be satisfied.
   *
   * The signal is a '''partial grant''', which is Spark's own established idiom for memory pressure
   * rather than anything invented here. `MemoryConsumer.acquireMemory` answers with the amount it
   * was actually able to grant, and that amount may be smaller than the request; Spark's spillable
   * collections treat exactly that outcome as the cue to spill, and the streaming shuffle's spill
   * manager does the same. Nothing new is introduced, and nothing bespoke has to be trusted.
   *
   * This policy is the '''escalation, not the first responder'''. The spill manager is what
   * attempts eviction, and a partial grant that eviction reverses is ordinary flow control that
   * must not cost the job its fast path. Callers must therefore invoke this only once eviction has
   * been attempted and could not free enough room -- that is, at the point where the allocation
   * genuinely cannot proceed. Called at that point, a short grant means streaming has run out of
   * the memory it trades for latency, and the trade is off.
   *
   * A request of zero or fewer bytes cannot be short-granted, so it is counted as un-evaluable and
   * never trips.
   *
   * @param requestedBytes the number of bytes the reservation asked for
   * @param grantedBytes the number of bytes actually granted, which may be zero
   */
  def recordAllocationGrant(requestedBytes: Long, grantedBytes: Long): Unit = {
    if (requestedBytes <= 0L) {
      unevaluableSamples.incrementAndGet()
      if (debugEnabled) {
        logInfo(log"Ignoring a streaming shuffle allocation sample for " +
          log"${MDC(MEMORY_SIZE, requestedBytes)} bytes: a request for nothing cannot be short " +
          log"granted, so there is no pressure to conclude anything from")
      }
    } else if (grantedBytes < requestedBytes) {
      trip(MemoryPressure,
        log"a streaming shuffle buffer reservation of " +
          log"${MDC(MEMORY_SIZE, requestedBytes)} bytes was granted only " +
          log"${MDC(NUM_BYTES, grantedBytes)} bytes, and eviction at the " +
          log"${MDC(THRESHOLD, spillThresholdPercent)}% spill threshold could not free enough " +
          log"room inside the buffer budget of ${MDC(PERCENT, bufferSizePercent)}% of executor " +
          log"memory.")
    }
  }

  // Trip 3: network utilisation above 90% of the administered link capacity.

  /**
   * Records observed egress against an explicit link capacity, and trips when utilisation is
   * '''strictly above''' the tolerated share.
   *
   * Exactly at the threshold is not saturation. The specification tolerates utilisation up to
   * [[StreamingShuffleFallbackPolicy.SATURATION_TRIP_PERCENT]] and trips past it, so the comparison
   * is `>` and never `>=`.
   *
   * Division is guarded rather than attempted: a capacity that is zero, negative or not a finite
   * number is not a capacity, and a link of unknown capacity cannot be described as saturated. Such
   * a sample is counted as un-evaluable and never trips, which is also why this method can never
   * divide by zero and can never produce a NaN comparison.
   *
   * @param usedBytesPerSecond observed egress, in bytes per second
   * @param capacityBytesPerSecond the link capacity the observation is measured against, in bytes
   *                               per second
   */
  def recordLinkUtilization(usedBytesPerSecond: Double, capacityBytesPerSecond: Double): Unit = {
    if (!isMeasurable(usedBytesPerSecond) || !isMeasurable(capacityBytesPerSecond) ||
        capacityBytesPerSecond <= 0.0d) {
      unevaluableSamples.incrementAndGet()
    } else {
      val utilization = usedBytesPerSecond / capacityBytesPerSecond
      if (utilization <= SATURATION_TRIP_RATIO) {
        // Under the tolerated share, so any run of over-capacity samples ends here.
        consecutiveSaturatedSamples.set(0L)
      } else {
        val consecutive = consecutiveSaturatedSamples.incrementAndGet()
        if (consecutive >= SATURATION_SUSTAINED_SAMPLES) {
          trip(NetworkSaturation,
            log"streaming shuffle egress reached " +
              log"${MDC(PERCENT, math.round(utilization * PERCENT_SCALE))}% of the administered " +
              log"link capacity, ${MDC(NUM_BYTES, usedBytesPerSecond)} bytes/s against " +
              log"${MDC(MAX_SIZE, capacityBytesPerSecond)} bytes/s, above the " +
              log"${MDC(THRESHOLD, SATURATION_TRIP_PERCENT)}% this policy tolerates, on " +
              log"${MDC(COUNT, consecutive)} consecutive sample(s).")
        } else if (debugEnabled) {
          logDebug(log"Streaming shuffle egress read " +
            log"${MDC(PERCENT, math.round(utilization * PERCENT_SCALE))}% of the administered " +
            log"link capacity on ${MDC(COUNT, consecutive)} consecutive sample(s), short of the " +
            log"${MDC(MAX_SIZE, SATURATION_SUSTAINED_SAMPLES)} a sustained saturation needs")
        }
      }
    }
  }

  /**
   * Consecutive over-capacity samples observed, which is what distinguishes a burst from
   * saturation.
   *
   * <b>Why one sample cannot be enough.</b> The pacing bucket must be able to admit one
   * maximum-sized block -- a bucket whose capacity were smaller would refuse every block forever,
   * which is a deadlock rather than a rate limit -- so its burst allowance is at least one frame
   * however small its paced share is. A bucket that starts full therefore legitimately delivers
   * that burst inside a single sampling interval, and with several concurrent shuffles the sum of
   * those bursts exceeds the administered capacity for exactly that interval before any of them can
   * refill. Tripping on one sample turned that legal burst into a stand-down: egress read at nearly
   * twice the administered capacity with not one stream throttled, every live producer of the
   * shuffle was invalidated, and the recomputation wrote more than a quarter of the shuffle's
   * records a second time. The ceiling was being enforced by abandoning streaming rather than by
   * pacing it.
   *
   * A run, by contrast, is the property the fallback condition is about. A burst clears on the next
   * sample because the bucket has to refill at its paced rate before it can burst again; a link
   * that really is saturated stays over capacity sample after sample, and still stands streaming
   * down within a few seconds -- far inside the sixty-second window the sustained-slowness
   * condition beside it uses for the same kind of reason.
   */
  private val consecutiveSaturatedSamples = new AtomicLong(0L)

  /**
   * Consecutive over-capacity samples observed with no intervening sample under capacity.
   */
  def consecutiveSaturatedSampleCount: Long = consecutiveSaturatedSamples.get()

  /**
   * Records observed egress against the capacity the operator administered through
   * `spark.shuffle.streaming.maxBandwidthMBps`.
   *
   * When that key is unset the link is uncapped, and an uncapped link has no capacity figure to
   * measure against, so saturation simply cannot be evaluated and this call does nothing. Absence
   * means unlimited; it emphatically does not mean zero, and treating it as zero would make every
   * sample infinitely saturated and would fall back permanently on the default configuration.
   *
   * @param usedBytesPerSecond observed egress, in bytes per second
   */
  def recordLinkUtilization(usedBytesPerSecond: Double): Unit = {
    administeredCapacityBytesPerSecond match {
      case Some(capacity) => recordLinkUtilization(usedBytesPerSecond, capacity.toDouble)
      case None => unevaluableSamples.incrementAndGet()
    }
  }

  /** The administered link capacity in bytes per second, or `None` when no cap is administered. */
  def administeredLinkCapacityBytesPerSecond: Option[Long] = administeredCapacityBytesPerSecond

  // Trip 4: a producer/consumer protocol version mismatch, detected explicitly.

  /**
   * Checks a peer's announced streaming shuffle protocol version and trips when this build cannot
   * speak it.
   *
   * This is an '''explicit''' compatibility check and never an inference from a failed decode. The
   * wire header carries a protocol version byte for precisely this purpose: so that an executor
   * rolled to a different revision is recognised before any attempt is made to interpret its
   * frames, and degrades deterministically to sort-based shuffle instead of misreading bytes. The
   * decision is delegated to `StreamingShuffleMessage.isCompatible`, which is the single place that
   * owns the compatibility rule; this method adds the escalation and nothing else. In particular it
   * does not call the throwing variant of the check and catch what comes back, because an exception
   * used as a predicate is exactly the inference this design exists to avoid.
   *
   * @param peerVersion the protocol version the peer stamped into its header
   * @return true when the version is one this build can decode, false when the policy has tripped
   */
  def checkProtocolVersion(peerVersion: Byte): Boolean = {
    val compatible = StreamingShuffleMessage.isCompatible(peerVersion)
    if (!compatible) {
      trip(ProtocolVersionMismatch,
        log"a peer announced streaming shuffle protocol version " +
          log"${MDC(PROTOCOL_VERSION, peerVersion)} but this executor speaks version " +
          log"${MDC(VERSION_NUM, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)}.")
    }
    compatible
  }

  /**
   * Checks the protocol version of a framed message without consuming it.
   *
   * `StreamingShuffleMessage.peekProtocolVersion` reads the version at an absolute index, so the
   * buffer's position, limit and mark are all left exactly as they were and the very same buffer
   * can still be handed on for decoding afterwards. That non-destructive read is what makes it
   * possible to settle compatibility '''before''' committing to a decode.
   *
   * A buffer too short to carry a version byte is a framing or corruption problem, not a version
   * mismatch, and the `IllegalArgumentException` it raises is allowed to propagate rather than
   * being quietly turned into a fallback: truncation is recovered by checksum verification and
   * retransmission, and mislabelling it here would hide a real fault behind a performance
   * regression.
   *
   * @param framedMessage a framed message, that is the type byte followed by the encoded body
   * @return true when the peer's version is one this build can decode
   */
  def checkProtocolVersion(framedMessage: ByteBuffer): Boolean = {
    checkProtocolVersion(StreamingShuffleMessage.peekProtocolVersion(framedMessage))
  }

  /**
   * Claims the right to declare one shuffle's fallback to the coordinator, once.
   *
   * The executor-local latch and the shuffle-wide one answer different questions. This policy
   * decides that streaming is no longer sustainable '''here'''; the coordinator records that a
   * shuffle has stood down '''everywhere'''. Only the second is enough to keep a shuffle from
   * having two producers of the same output, so a local trip has to be propagated -- and propagated
   * by whichever participant notices it, since none of them is privileged.
   *
   * That makes duplicate declarations the norm rather than the exception: every writer and reader
   * of the shuffle on this executor sees the same latch at the same moment. This method suppresses
   * the duplicates without suppressing the declaration, so the wire cost of a fallback is one ask
   * per shuffle per executor instead of one per task.
   *
   * Answering `false` while the policy has not tripped is deliberate: there is nothing to declare,
   * and claiming the slot early would consume the one-shot for a shuffle that is still healthy.
   *
   * @param shuffleId the shuffle whose fallback the caller intends to declare
   * @return `true` when the caller must perform the declaration, `false` when it is already done or
   *         not yet warranted
   */
  def claimFallbackAnnouncement(shuffleId: Int): Boolean = {
    if (trippedReasonRef.get().isEmpty) {
      false
    } else if (announcedShuffles.size() >= MAX_TRACKED_SHUFFLES) {
      // Bound reached, so the claim is not recorded -- but it is granted, because the coordinator
      // deduplicates for us and a shuffle left streaming would be a correctness fault where a
      // repeated ask is only a cost.
      true
    } else {
      announcedShuffles.add(shuffleId)
    }
  }

  /** How many shuffles this executor has declared a fallback for, bounded by the tracking bound. */
  def announcedFallbackCount: Int = announcedShuffles.size()

  /**
   * Records a shuffle-wide fallback verdict learned from the coordinator, so that every other
   * streaming component on this executor can see it without asking again.
   *
   * A verdict that reports streaming still in force is deliberately '''not''' recorded. The map is
   * a record of decisions taken, not of the last answer received: caching "not fallen back" would
   * have to be invalidated on some cadence to stay correct, whereas a latched decision never needs
   * to be.
   *
   * @param shuffleId the shuffle the verdict concerns
   * @param state the verdict as the coordinator reported it
   * @return `true` when this call is the one that recorded the verdict
   */
  def observeShuffleFallback(
      shuffleId: Int,
      state: StreamingShuffleFallbackState): Boolean = {
    if (!state.fallenBack) {
      false
    } else if (shuffleFallbacks.size() >= MAX_TRACKED_SHUFFLES &&
        !shuffleFallbacks.containsKey(shuffleId)) {
      // At the bound the verdict is not cached, which costs another component one ask rather than
      // its correctness: every path that acts on a fallback still has the coordinator's own answer
      // ahead of it -- a declined producer registration, or a lookup reply carrying the verdict.
      noteUntrackedShuffle(shuffleId)
      false
    } else {
      val recorded = shuffleFallbacks.putIfAbsent(shuffleId, state) == null
      if (recorded) {
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} has stood streaming down " +
          log"for every participant: ${MDC(REASON, describeFallback(state))}. Every streaming " +
          log"component of that shuffle on this executor now stands down, and the shuffle is " +
          log"served by the unmodified SortShuffleManager.")
      }
      recorded
    }
  }

  /**
   * Whether a shuffle is known to have stood streaming down for every participant.
   *
   * One map lookup, no allocation, no clock read and no ask, so a producer may consult it as often
   * as it consults [[hasTripped]] -- which it must, because the two cover different failures.
   */
  def shuffleHasFallenBack(shuffleId: Int): Boolean = shuffleFallbacks.containsKey(shuffleId)

  /** The verdict recorded for a shuffle, or `None` when none has been learned. */
  def knownShuffleFallback(shuffleId: Int): Option[StreamingShuffleFallbackState] =
    Option(shuffleFallbacks.get(shuffleId))

  /** How many shuffle-wide verdicts this executor has cached, bounded by the tracking bound. */
  def knownShuffleFallbackCount: Int = shuffleFallbacks.size()

  /**
   * An operator-facing rendering of a verdict: the declarer's own account when it gave one, this
   * build's prose for the latched member otherwise, and the bare name only for a name this build
   * cannot resolve.
   */
  private def describeFallback(state: StreamingShuffleFallbackState): String = state.condition

  // Lifecycle.

  /**
   * Discards the throughput state for a shuffle that has finished or been unregistered.
   *
   * Callers should invoke this when a shuffle is unregistered, so that a long-lived executor's
   * tracking stays within [[MAX_TRACKED_SHUFFLES]] and no per-shuffle state outlives the shuffle.
   * It is idempotent and safe to call for a shuffle that was never tracked. It deliberately does
   * not clear a latched trip: the fact that streaming proved unsustainable on this executor is not
   * invalidated by one shuffle completing.
   *
   * @param shuffleId the shuffle whose sampling state should be released
   */
  def forgetShuffle(shuffleId: Int): Unit = {
    val removed = throughputWindows.remove(shuffleId)
    // The announcement slot is released with the sampling state, so that the bound tracks live
    // shuffles rather than every shuffle the executor has ever served. Releasing it cannot cause a
    // fallback to be missed: the shuffle is being unregistered, so there is nothing left to
    // declare.
    announcedShuffles.remove(shuffleId)
    shuffleFallbacks.remove(shuffleId)
    if (removed != null && debugEnabled) {
      logInfo(log"Released streaming shuffle fallback sampling state for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)}")
    }
  }

  /**
   * Restores the untripped state and discards all sampling state.
   *
   * This exists for tests, and for reusing one instance across independent workloads. It is
   * emphatically '''not''' a runtime recovery path: un-latching mid-shuffle would let streaming and
   * sort-based shuffle both believe they own the same shuffle output. Nothing on the streaming hot
   * path calls this, and nothing should.
   *
   * The cumulative diagnostic tallies are cleared too, so a count read after a reset is absolute
   * rather than a difference against whatever came before it.
   */
  def reset(): Unit = {
    trippedReasonRef.set(None)
    trippedAtMs.set(UNSET_TIME_MS)
    observedTripConditions.set(0L)
    unevaluableSamples.set(0L)
    untrackedShuffleSamples.set(0L)
    throughputWindows.clear()
    announcedShuffles.clear()
    shuffleFallbacks.clear()
    if (debugEnabled) {
      logInfo(log"Streaming shuffle fallback policy reset to its untripped state")
    }
  }

  override def toString: String = {
    val state = trippedReasonRef.get().map(_.toString).getOrElse("untripped")
    s"StreamingShuffleFallbackPolicy(enabled=$streamingEnabled, state=$state, " +
      s"trackedShuffles=${throughputWindows.size()})"
  }

  // The single terminus. Every trip condition, without exception, arrives here.

  /**
   * Latches the first fallback condition observed and reports it once.
   *
   * First writer wins, by compare-and-set against the singleton `None`, so concurrent conditions
   * cannot interleave into a torn state and the reported reason is the one that actually came
   * first. Exactly one warning is emitted per instance, naming both the condition and the terminus,
   * because a fallback is a rare and consequential event that an operator must be able to find
   * without having debug logging enabled -- and because emitting it only on the latching call is
   * what keeps a persistent condition from flooding the log. Subsequent conditions are counted
   * always and logged only under `spark.shuffle.streaming.debug`.
   *
   * @param reason the condition observed
   * @param detail the measurements behind it, evaluated only when it is actually going to be logged
   */
  private def trip(
      reason: StreamingShuffleFallbackReason,
      detail: => MessageWithContext): Unit = {
    observedTripConditions.incrementAndGet()
    if (trippedReasonRef.compareAndSet(None, Some(reason))) {
      trippedAtMs.set(clock.getTimeMillis())
      logWarning(log"Streaming shuffle is standing down on this executor because " +
        log"${MDC(REASON, reason.description)}: " + detail +
        log" Every subsequent shuffle service-provider call is delegated verbatim to the " +
        log"unmodified SortShuffleManager, which is the same terminus as the " +
        log"${MDC(CONFIG, config.SHUFFLE_STREAMING_ENABLED.key)} kill switch, so the job " +
        log"continues on the production-stable sort-based path. Throughput and latency change; " +
        log"correctness and job success do not.")
    } else if (debugEnabled) {
      logInfo(log"Streaming shuffle fallback condition " +
        log"${MDC(REASON, reason.description)} observed after the policy had already stood down " +
        log"because ${MDC(VALUE, latchedDescription)}; the terminus is unchanged")
    }
  }

  /** The latched reason's description, or a placeholder if it was cleared concurrently. */
  private def latchedDescription: String =
    trippedReasonRef.get().map(_.description).getOrElse("an earlier condition")

  // Private helpers and per-shuffle state.

  /**
   * Whether a sampled quantity can be reasoned about at all: a finite, non-negative number.
   *
   * NaN and the infinities are rejected because every comparison this class makes against them
   * would be meaningless, and negative values because neither a throughput nor a capacity can be
   * negative. `java.lang.Double.isFinite` is used explicitly so the intent is unmistakable at the
   * call site.
   */
  private def isMeasurable(value: Double): Boolean =
    java.lang.Double.isFinite(value) && value >= 0.0d

  /**
   * Records how this instance was configured, once, at debug level.
   *
   * Nothing here is logged unconditionally: a policy that has not tripped must be silent, and the
   * settings it holds are already visible through the configuration itself.
   */
  private def logConstruction(): Unit = {
    if (debugEnabled) {
      // Rendered rather than reported as a number, because "unlimited" is a state and not a value:
      // there is no integer that honestly stands for an uncapped link.
      val capacity = administeredCapacityBytesPerSecond
        .map(bytesPerSecond => s"$bytesPerSecond bytes/s")
        .getOrElse("unlimited, so saturation cannot be evaluated and never trips")
      logInfo(log"Streaming shuffle fallback policy created with " +
        log"${MDC(CONFIG, config.SHUFFLE_STREAMING_ENABLED.key)}=" +
        log"${MDC(VALUE, streamingEnabled)}, tripping on a consumer held at " +
        log"${MDC(RATIO, CONSUMER_SLOWNESS_RATIO)}x behind for more than " +
        log"${MDC(THRESHOLD, SUSTAINED_SLOWNESS_WINDOW_MS)} ms, on utilisation above " +
        log"${MDC(PERCENT, SATURATION_TRIP_PERCENT)}% of an administered link capacity of " +
        log"${MDC(MAX_SIZE, capacity)}, and on any protocol version other than " +
        log"${MDC(VERSION_NUM, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)}")
    }
  }

  /**
   * Tells the operator, once, when one of the four fallback conditions cannot be evaluated at all.
   *
   * Network saturation is defined relative to an administered link capacity, and
   * `spark.shuffle.streaming.maxBandwidthMBps` has no default -- absence means unlimited. So on a
   * default configuration there is no capacity to measure utilisation against, and the saturation
   * condition is not merely untripped but unevaluable: three of the four conditions are live and
   * the fourth is inert. That is a legitimate configuration and not a fault, which is why this is a
   * notice rather than a warning, but leaving it visible only under the debug key means the
   * operator most likely to rely on the condition is the one least likely to know it is absent.
   *
   * Emitted at most once per JVM, latched in the companion object rather than in this instance: the
   * notice describes the configuration of the whole executor, so however many policies a JVM
   * constructs it states the fact once, and nothing on any task path reaches this method at all.
   */
  private def logSaturationCoverage(): Unit = {
    if (streamingEnabled && administeredCapacityBytesPerSecond.isEmpty &&
      saturationNoticeEmitted.compareAndSet(false, true)) {
      logInfo(log"Streaming shuffle is enabled with no " +
        log"${MDC(CONFIG, config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS.key)}, so the link has no " +
        log"administered capacity to measure utilisation against and the network saturation " +
        log"fallback condition above " +
        log"${MDC(PERCENT, SATURATION_TRIP_PERCENT)}% cannot be evaluated and will never " +
        log"trip. The consumer slowness, memory pressure and protocol version conditions are " +
        log"unaffected; set that property to a link capacity in MB/s to " +
        log"make the saturation condition active")
    }
  }

  /**
   * One shuffle's producer and consumer rates, plus how long its consumer has been behind.
   *
   * Guarded by its own monitor rather than by atomics, because arming the sustained-slowness timer
   * has to be indivisible with respect to the rate comparison that arms it: two atomics could be
   * read either side of an update and yield a window that never opened or one that never closed.
   * The monitor is held only for a handful of field assignments and one comparison -- never across
   * I/O, a callback or a log statement -- and it is per shuffle, so producer and consumer threads
   * reporting on different shuffles never contend at all.
   *
   * A rate that cannot be measured clears the stored reading instead of leaving a stale one in
   * place. Pairing a fresh sample with a stale one is exactly how a policy comes to trip on a
   * deficit that no longer exists, and refusing to do so is the conservative choice this class
   * makes everywhere.
   */
  private final class ThroughputWindow {

    private var producerBytesPerSecond: Double = UNSET_RATE
    private var consumerBytesPerSecond: Double = UNSET_RATE
    private var slowSinceMs: Long = 0L
    private var slowArmed: Boolean = false

    /** Applies a producer sample and re-evaluates the sustained deficit. */
    def recordProducer(bytesPerSecond: Double, nowMs: Long): SlownessSample = synchronized {
      producerBytesPerSecond = normalise(bytesPerSecond)
      evaluate(nowMs)
    }

    /** Applies a consumer sample and re-evaluates the sustained deficit. */
    def recordConsumer(bytesPerSecond: Double, nowMs: Long): SlownessSample = synchronized {
      consumerBytesPerSecond = normalise(bytesPerSecond)
      evaluate(nowMs)
    }

    /** When the deficit was first observed and has held since, or `None` while it does not hold. */
    def armedSinceMs: Option[Long] = synchronized {
      if (slowArmed) Some(slowSinceMs) else None
    }

    /**
     * Accepts a measurable rate, or records the sample as un-evaluable and forgets the reading.
     */
    private def normalise(bytesPerSecond: Double): Double = {
      if (isMeasurable(bytesPerSecond)) {
        bytesPerSecond
      } else {
        unevaluableSamples.incrementAndGet()
        UNSET_RATE
      }
    }

    /**
     * Arms, holds or clears the sustained-slowness timer and reports whether it has now elapsed.
     *
     * The deficit holds only while both rates are known and the producer is actually producing.
     * That second guard is what stops an idle shuffle, whose rates are both zero, from satisfying
     * `0 * 2 <= 0` and standing streaming down for no reason at all.
     *
     * The elapsed comparison is strictly greater than the window, so a deficit held for exactly the
     * window has not yet exceeded it. A clock that appears to move backwards re-arms the timer
     * rather than yielding a negative interval, which keeps the arithmetic honest under a clock
     * adjustment.
     */
    private def evaluate(nowMs: Long): SlownessSample = {
      val producer = producerBytesPerSecond
      val consumer = consumerBytesPerSecond
      val behind = producer > 0.0d && consumer >= 0.0d &&
        consumer * CONSUMER_SLOWNESS_RATIO <= producer
      if (!behind) {
        slowArmed = false
        slowSinceMs = 0L
        SlownessSample(producer, consumer, NOT_SUSTAINED)
      } else if (!slowArmed || nowMs < slowSinceMs) {
        slowArmed = true
        slowSinceMs = nowMs
        SlownessSample(producer, consumer, NOT_SUSTAINED)
      } else {
        val elapsedMs = nowMs - slowSinceMs
        val sustainedMs =
          if (elapsedMs > SUSTAINED_SLOWNESS_WINDOW_MS) elapsedMs else NOT_SUSTAINED
        SlownessSample(producer, consumer, sustainedMs)
      }
    }
  }

  /**
   * The result of applying one throughput sample.
   *
   * Carrying the two rates out alongside the verdict is what lets the warning quote the exact
   * numbers the decision was made on, taken under the same monitor as the decision itself, rather
   * than re-reading them afterwards and quoting a pair that never coexisted.
   *
   * @param producerBytesPerSecond the producer rate in force, or `UNSET_RATE` if unknown
   * @param consumerBytesPerSecond the consumer rate in force, or `UNSET_RATE` if unknown
   * @param sustainedMs how long the deficit has held, or `NOT_SUSTAINED` if it has not held long
   *                    enough to trip
   */
  private case class SlownessSample(
      producerBytesPerSecond: Double,
      consumerBytesPerSecond: Double,
      sustainedMs: Long)
}

/**
 * The numeric contracts of the fallback policy.
 *
 * These are specified values, not tuning knobs, which is why they are constants here rather than
 * further configuration keys: the streaming shuffle already exposes exactly five keys, and each of
 * these thresholds is part of what "streaming shuffle" is defined to mean. They are declared in
 * initialisation order, with no value referring to one declared below it, so that no constant can
 * be read as zero during class initialisation.
 */
private[spark] object StreamingShuffleFallbackPolicy {

  /**
   * How far behind the consumer must be for the deficit to count: the producer must be running at
   * least this many times faster.
   *
   * Expressed as a multiplier on the consumer rate rather than a divisor on the producer rate, so
   * that the comparison stays a multiplication and can never divide by a zero consumer rate.
   */
  val CONSUMER_SLOWNESS_RATIO: Double = 2.0d

  /**
   * How long the deficit must hold, continuously, before the policy trips: strictly longer than
   * this many milliseconds.
   *
   * The window is what makes the condition "sustained" rather than instantaneous. A momentary stall
   * is ordinary flow control, already absorbed by backpressure and spill, and must not cost a job
   * its fast path. Held for exactly this long is not yet long enough.
   */
  val SUSTAINED_SLOWNESS_WINDOW_MS: Long = 60000L

  /**
   * The share of the administered link, as a percentage, above which the link counts as saturated.
   *
   * Not to be confused with the 80% ceiling the egress token bucket holds itself to. That is a rate
   * limit streaming imposes on itself while continuing to stream; this is the point at which
   * streaming stops altogether. Two different numbers doing two different jobs.
   */
  val SATURATION_TRIP_PERCENT: Long = 90L

  /**
   * Whether the notice about an unevaluable saturation condition has already been emitted.
   *
   * Process wide rather than per instance, so a JVM that constructs more than one policy states the
   * fact once rather than once per construction.
   */
  private[streaming] val saturationNoticeEmitted = new AtomicBoolean(false)

  /** [[SATURATION_TRIP_PERCENT]] as a fraction, which is the form the comparison actually uses. */
  val SATURATION_TRIP_RATIO: Double = SATURATION_TRIP_PERCENT.toDouble / 100.0d

  /**
   * Consecutive over-capacity samples a link must produce before its saturation is treated as
   * sustained and streaming stands down.
   *
   * Three. Sized against what it must exclude rather than picked round: the pacing buckets' burst
   * allowance can exceed the administered capacity for exactly one sampling interval apiece and
   * then not again until it has refilled at the paced rate, so two consecutive over-capacity
   * samples already rule a burst out and three leave margin for a second wave of limiters created
   * part-way through a shuffle. Against what it must catch it costs almost nothing: the samples
   * arrive on the protocol's own one-second measurement cadence, so a genuinely saturated link
   * still stands streaming down within a few seconds, an order of magnitude sooner than the
   * sixty-second sustained-slowness condition beside it. Deliberately kept in step with
   * `BackpressureProtocol.LINK_SATURATION_SUSTAINED_INTERVALS`, which applies the same rule to the
   * protocol's own view of the same condition.
   */
  val SATURATION_SUSTAINED_SAMPLES: Long = 3L

  /**
   * Bytes in one MiB, used to convert the administered bandwidth from MB/s into bytes per second.
   *
   * Defined here rather than shared, so that this class depends on nothing but its own contracts;
   * the conversion matches the one the egress rate limiter applies to the same configuration value.
   */
  val BYTES_PER_MIB: Long = 1024L * 1024L

  /**
   * The greatest number of shuffles this policy will hold throughput state for at once.
   *
   * An executor serves a bounded number of concurrent shuffles, so this is generous by a wide
   * margin in normal operation. It exists so that a caller which never deregisters a completed
   * shuffle cannot turn per-shuffle sampling state into an unbounded leak on a long-lived executor.
   */
  val MAX_TRACKED_SHUFFLES: Int = 1024

  /** Scale factor for rendering a fraction as a percentage in an operator-facing message. */
  private val PERCENT_SCALE: Double = 100.0d

  /** The rate held for a direction that has never reported a measurable sample. */
  private val UNSET_RATE: Double = -1.0d

  /** The trip timestamp held while the policy has not tripped. */
  private val UNSET_TIME_MS: Long = -1L

  /** The sustained duration reported when the deficit has not held long enough to trip. */
  private val NOT_SUSTAINED: Long = -1L
}
