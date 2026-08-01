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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{COUNT, MAX_ATTEMPTS, NUM_BYTES, PARTITION_ID, PERCENT,
  REASON, SHUFFLE_ID, THRESHOLD}
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, HeartbeatMessage,
  RetransmitRequestMessage, StreamingShuffleMessage, StreamingShuffleMessageType,
  StreamTerminationMessage}
import org.apache.spark.util.{Clock, SystemClock}

/**
 * The state a streaming shuffle stream, or the executor's whole streaming shuffle, is in as far as
 * flow control is concerned.
 *
 * The four states form a strict severity order, which is what lets one reading describe a set of
 * streams: the executor is in the most severe state any of its streams is in. `Degraded` dominates
 * `Spilling`, which dominates `Throttled`, which dominates `Flowing`, because each of them is a
 * strictly stronger statement about how much progress the subsystem is able to make.
 *
 * @param name stable, ASCII-only label used in logs and in assertions
 * @param severity rank within the order above; higher is more severe
 */
private[spark] sealed abstract class BackpressureState(val name: String, val severity: Int) {
  override def toString: String = name
}

private[spark] object BackpressureState {

  /** Data is moving: the consumer has credit outstanding and the rate limiter is admitting. */
  case object Flowing extends BackpressureState("Flowing", 0)

  /**
   * Egress is being held back, either because the consumer's credit is exhausted or because the
   * token bucket refused an acquisition. This is the state whose every entry is counted by the
   * `shuffle.streaming.backpressureEvents` metric.
   */
  case object Throttled extends BackpressureState("Throttled", 1)

  /**
   * Aggregate buffer utilisation has reached `spark.shuffle.streaming.spillThreshold`, so buffered
   * partitions must be evicted to disk. Detected here; the eviction itself belongs to the spill
   * manager.
   */
  case object Spilling extends BackpressureState("Spilling", 2)

  /**
   * Streaming cannot be sustained and the sort-based path must take over. Detected here; the trip
   * decision and the delegation belong to the fallback policy and to the shuffle manager.
   */
  case object Degraded extends BackpressureState("Degraded", 3)

  /** Every state, in ascending severity, so a caller can enumerate them without a match. */
  val values: Seq[BackpressureState] = Seq(Flowing, Throttled, Spilling, Degraded)
}

/**
 * Why the streaming path can no longer be sustained.
 *
 * These are exactly the four conditions the feature specifies for reverting to sort-based shuffle.
 * This protocol owns the observations behind all four -- it sees the acknowledgement rates, the
 * allocation refusals, the pacing bucket and the message headers -- so it is where they are
 * detected. It deliberately does not act on them: publishing a reason is the whole of its part,
 * and the fallback policy is what turns a published reason into a delegation to the sort-based
 * manager.
 *
 * @param code stable, ASCII-only identifier suitable for a log line or an assertion
 */
private[spark] sealed abstract class BackpressureDegradationReason(val code: String) {
  override def toString: String = code
}

private[spark] object BackpressureDegradationReason {

  /**
   * The consumer has been at least twice as slow as the producer, continuously, for longer than
   * the sustained-slowness window. A momentary imbalance is absorbed by credit and by spill; only
   * an imbalance that persists means streaming is the wrong shape for this workload.
   */
  case object ConsumerSustainedSlowness
    extends BackpressureDegradationReason("consumerSustainedSlowness")

  /** A buffer allocation could not be satisfied even after spilling, so streaming risks an OOM. */
  case object BufferAllocationFailure
    extends BackpressureDegradationReason("bufferAllocationFailure")

  /** Observed egress has saturated the administered link capacity beyond its safe ceiling. */
  case object LinkSaturation extends BackpressureDegradationReason("linkSaturation")

  /** A peer sent a message stamped with a wire revision this executor does not speak. */
  case object ProtocolVersionMismatch
    extends BackpressureDegradationReason("protocolVersionMismatch")

  /** Every reason, in a stable order, so a caller can enumerate them without a match. */
  val values: Seq[BackpressureDegradationReason] = Seq(
    ConsumerSustainedSlowness, BufferAllocationFailure, LinkSaturation, ProtocolVersionMismatch)
}

/**
 * Identity of one streaming shuffle stream: the flow of blocks from the producers of one shuffle to
 * the consumer of one of its reduce partitions.
 *
 * A shuffle id alone is too coarse to carry credit, because each reduce partition is consumed by a
 * different task and acknowledges at its own pace, and a partition id alone is not unique across
 * concurrent shuffles. The pair is the unit the credit ledger is keyed on, and it is a case class
 * so that value equality and a matching hash code come from the compiler rather than from a
 * hand-written pair of methods that could drift apart.
 *
 * @param shuffleId shuffle the stream belongs to
 * @param partitionId reduce partition the stream feeds
 */
private[spark] case class BackpressureStreamKey(shuffleId: Int, partitionId: Int) {
  override def toString: String = s"shuffle $shuffleId partition $partitionId"
}

/**
 * How much of the executor's buffered egress one shuffle is responsible for, which is the input to
 * priority arbitration between concurrent shuffles.
 *
 * Both quantities the feature names as arbitration keys are carried, and they are carried
 * separately rather than pre-combined into a score, so that a caller -- and a test -- can see
 * exactly why one shuffle was ordered ahead of another.
 *
 * @param shuffleId the shuffle being described
 * @param numPartitions reduce partitions registered for it, the coarse measure of its footprint
 * @param pendingBytes bytes it currently holds unacknowledged across all of its streams
 * @param streamCount streams of this shuffle currently registered on this executor
 */
private[spark] case class BackpressureShuffleDemand(
    shuffleId: Int,
    numPartitions: Int,
    pendingBytes: Long,
    streamCount: Int)

/**
 * Application-level flow control for the streaming shuffle: the credit ledger, the liveness timers
 * and the arbitration between concurrent shuffles on one executor.
 *
 * Three layers of backpressure act together on the streaming path, and this class owns exactly one
 * of them.
 *
 *  1. Application-level credit derived from consumer acknowledgements -- owned here. A producer may
 *     hold at most a bounded number of unacknowledged bytes in flight to any one consumer, and an
 *     advancing acknowledgement is what restores that allowance.
 *  2. Rate limiting -- owned by [[TokenBucketRateLimiter]] and consulted here through its
 *     non-blocking `tryAcquire`. This class never re-implements pacing arithmetic.
 *  3. TCP-level throttling by toggling a channel's `autoRead` -- owned by the streaming client
 *     handler. This class publishes the predicate that handler reads, [[hasCredit]], but never
 *     touches a channel itself.
 *
 * Keeping the layers apart is what lets each be reasoned about, and tested, on its own, and it is
 * why a refusal from any one of them is expressed the same way: the caller keeps the data it was
 * about to send and tries again, which is precisely the pressure the spill manager reacts to.
 *
 * What this class detects but does not do. Reaching the spill threshold and tripping a degradation
 * condition are both observed here, because this is where the acknowledgement rates, the pacing
 * bucket and the message headers are seen. Neither is acted on here: the spill manager evicts, and
 * the fallback policy decides to delegate to the sort-based manager. Publishing the signal is the
 * whole of this class's part in both, and that division is what keeps the streaming subsystem free
 * of a component that both measures and reacts.
 *
 * Determinism. Every time this class reads is read through the injected [[Clock]]. There is no
 * `Thread.sleep`, no direct `System.currentTimeMillis()` and no direct `System.nanoTime()` anywhere
 * in this file, and no method waits on anything, so a suite can freeze the clock, advance it by
 * exactly five or ten seconds and observe the timers trip to the millisecond instead of racing wall
 * time. The heartbeats this class builds are stamped from the same injected clock, which is why
 * [[HeartbeatMessage]] takes its timestamp from the caller rather than reading a clock of its own.
 *
 * Liveness and clock skew. A stream's liveness is judged from the local instant at which this
 * executor observed inbound activity, never from the timestamp a peer wrote into a heartbeat. The
 * timestamp a peer sends is retained for diagnostics only. Two hosts do not agree on the wall
 * clock, and a detector built on a remote reading would declare a healthy peer dead, or fail to
 * declare a dead one dead, purely from skew.
 *
 * Thread model. The shuffle metrics reporters are documented as single-threaded, so on the
 * streaming path a network event-loop thread may only enqueue while the task thread verifies,
 * counts and throws. This class is consulted from both, so every field is an atomic or a concurrent
 * collection, every method is non-blocking and lock-free, and no metrics *reporter* is ever called
 * from here. [[StreamingShuffleMetricsSource]] is different: it is a lock-free Dropwizard registry,
 * and the one counter this class owns -- `shuffle.streaming.backpressureEvents` -- is advanced
 * through it from whichever thread observed the transition.
 *
 * Log volume. Nothing is logged per record, per block, per acknowledgement or per refusal. A
 * stream's first entry into the throttled state is reported once, at info level, and every
 * subsequent entry is silent unless `spark.shuffle.streaming.debug` is set; the same
 * once-per-stream rule applies to the two liveness timers, and each degradation reason is reported
 * once for the lifetime of the protocol. Log volume is therefore bounded by the number of streams
 * rather than by the volume of data, which is what keeps the subsystem inside its budget of under
 * 10 MB per hour per executor with debug off.
 *
 * Configuration is read once here and held immutably, which is what makes "a configuration change
 * requires an executor restart" true by construction rather than by convention.
 *
 * @param conf the executor's configuration; the spill threshold and the debug gate are read from it
 *             once, at construction
 * @param coordinator the streaming shuffle rendezvous endpoint, which supplies the set of active
 *                    shuffles and the concurrency count that the egress budget is divided by. May
 *                    be null on a host that holds only a remote reference to the driver's endpoint,
 *                    in which case arbitration falls back to the shuffles registered locally
 * @param rateLimiter the pacing layer this protocol consults; must not be null
 * @param clock time source for every liveness and rate decision, injected so that tests are
 *              deterministic
 */
private[spark] class BackpressureProtocol(
    conf: SparkConf,
    coordinator: StreamingShuffleCoordinator,
    rateLimiter: TokenBucketRateLimiter,
    clock: Clock = new SystemClock) extends Logging {

  require(conf != null, "The Spark configuration must not be null.")
  require(rateLimiter != null, "The streaming shuffle rate limiter must not be null.")
  require(clock != null, "The streaming shuffle clock must not be null.")

  // Read once at construction and held immutably. The streaming shuffle has no dynamic
  // reconfiguration by design, so holding the value is what makes an executor restart the only way
  // to change it. This flag is read first because it is the single authority over every verbose
  // line this class writes: an operator who leaves spark.shuffle.streaming.debug at its default of
  // false sees nothing verbose from the protocol even when logging is globally set to DEBUG.
  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  // Aggregate buffer utilisation, as a percentage, at which buffered partitions must be evicted.
  // The configuration entry validates the 50-95 range at read time, so no range check is repeated
  // here; a value outside it never reaches this field.
  private val spillThreshold: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)

  // The credit ledger: one entry per (shuffleId, partitionId) stream. A concurrent map because the
  // entries are created and read from the task thread and from network event-loop threads alike,
  // and because a stream must be able to leave without disturbing any other.
  private val streams =
    new ConcurrentHashMap[BackpressureStreamKey, BackpressureProtocol.StreamLedger]()

  // Reduce partition count per registered shuffle, which is one of the two arbitration keys. The
  // coordinator records this per shuffle but publishes no accessor for it, and the count is known
  // at the moment a shuffle registers here, so it is held locally rather than asked for remotely on
  // a path that must not block.
  private val shufflePartitionCounts = new ConcurrentHashMap[Int, Integer]()

  // Buffered bytes and the budget they are measured against, per registered shuffle, as reported by
  // whichever component owns those buffers. Utilisation is an executor-wide quantity that no single
  // shuffle can compute, so each contributes the two numbers it knows and this class sums them --
  // the same division of labour the buffer-utilisation gauge uses, for the same reason: a shuffle
  // holding almost nothing must not be able to erase the reading of one sitting at the threshold.
  private val shuffleBufferedBytes = new ConcurrentHashMap[Int, java.lang.Long]()

  private val shuffleBudgetBytes = new ConcurrentHashMap[Int, java.lang.Long]()

  // Degradation reasons observed so far. A set rather than a single cell because more than one
  // condition can hold at once and an operator needs to see all of them, and latched rather than
  // momentary because the fallback policy may sample it after the condition that caused it has
  // passed. clearDegradation() is the only way back.
  private val degradations =
    ConcurrentHashMap.newKeySet[BackpressureDegradationReason]()

  // Transitions into the throttled state observed by this protocol instance. The executor-wide
  // Dropwizard counter is advanced in step with this, but a JVM-singleton counter accumulates
  // across every protocol in the process, so a local total is kept for assertions that need to
  // speak about one instance.
  private val throttleTransitions = new AtomicLong(0L)

  // Acknowledgement-driven reclamations that were not confirmed inside the 100 ms bound. Counted
  // rather than logged per occurrence, because the bound is per acknowledgement and a log line per
  // breach would be a per-acknowledgement line by another name.
  private val reclamationBreaches = new AtomicLong(0L)

  // Latch ensuring each degradation reason is reported at most once per protocol instance.
  private val reportedDegradations =
    ConcurrentHashMap.newKeySet[BackpressureDegradationReason]()

  // Latch for the wire-revision warning. A peer that speaks the wrong revision refuses every
  // message it sends, so an ungated warning would be a per-message line; the two revisions only
  // need saying once, and the refusal count is observable through the degradation reason set.
  private val versionMismatchReported = new AtomicBoolean(false)

  // Instant of the last pollOnce(), so that a caller driving the protocol on a timer can ask
  // whether the next poll is due instead of keeping that bookkeeping itself.
  private val lastPollMillis = new AtomicLong(clock.getTimeMillis())

  /**
   * Registers a shuffle with the protocol, recording the reduce partition count that arbitration
   * and the aggregate utilisation reading are computed from.
   *
   * Idempotent in the sense that registering the same shuffle again simply refreshes the partition
   * count, which is what a second map task of the same shuffle on the same executor does. The
   * buffered and budget readings of an already-registered shuffle are left untouched, so a
   * re-registration cannot silently zero a live utilisation contribution.
   *
   * @param shuffleId the shuffle to register; must be non-negative
   * @param numPartitions its reduce partition count; must be positive
   */
  def registerShuffle(shuffleId: Int, numPartitions: Int): Unit = {
    require(shuffleId >= 0, s"The shuffle id must be non-negative but was $shuffleId.")
    require(numPartitions > 0,
      s"The partition count of shuffle $shuffleId must be positive but was $numPartitions.")
    shufflePartitionCounts.put(shuffleId, Integer.valueOf(numPartitions))
    shuffleBufferedBytes.putIfAbsent(shuffleId, java.lang.Long.valueOf(0L))
    shuffleBudgetBytes.putIfAbsent(shuffleId, java.lang.Long.valueOf(0L))
    if (debugEnabled) {
      logDebug(log"Registered shuffle ${MDC(SHUFFLE_ID, shuffleId)} with the streaming shuffle " +
        log"backpressure protocol across ${MDC(COUNT, numPartitions)} partitions")
    }
  }

  /**
   * Drops a shuffle and every stream belonging to it, releasing all of the credit those streams
   * held. Safe to call for a shuffle that was never registered, and safe to call more than once,
   * because unregistration runs on cleanup paths that may execute on success, on failure and on
   * cancellation alike.
   *
   * Dropping the utilisation contribution along with the shuffle is what stops a finished shuffle
   * from inflating the executor's reading forever.
   *
   * @param shuffleId the shuffle to drop
   * @return how many streams were dropped with it
   */
  def unregisterShuffle(shuffleId: Int): Int = {
    shufflePartitionCounts.remove(shuffleId)
    shuffleBufferedBytes.remove(shuffleId)
    shuffleBudgetBytes.remove(shuffleId)
    var dropped = 0
    val entries = streams.keySet().iterator()
    while (entries.hasNext) {
      val key = entries.next()
      if (key.shuffleId == shuffleId) {
        // Removing through the key set's own iterator removes from the backing map, so the walk
        // stays a single pass and cannot observe a key it has already dropped.
        entries.remove()
        dropped += 1
      }
    }
    if (dropped > 0) {
      logInfo(log"Streaming shuffle backpressure protocol dropped " +
        log"${MDC(COUNT, dropped)} stream(s) of shuffle ${MDC(SHUFFLE_ID, shuffleId)}")
    }
    dropped
  }

  /** Ids of every shuffle registered here, ascending, so a reading is always deterministic. */
  def registeredShuffleIds: Seq[Int] = shufflePartitionCounts.keySet().asScala.toSeq.sorted

  /** How many shuffles are registered here. */
  def numRegisteredShuffles: Int = shufflePartitionCounts.size()

  /**
   * Reduce partition count registered for a shuffle, or `None` when the shuffle is unknown here.
   */
  def partitionCountOf(shuffleId: Int): Option[Int] =
    Option(shufflePartitionCounts.get(shuffleId)).map(_.intValue())

  /**
   * The divisor the executor's egress budget is split by, as reported by the coordinator, clamped
   * to at least one.
   *
   * The coordinator is the only component that knows how many shuffles an executor is concurrently
   * serving, and the token bucket's refill rate is the administered link capacity divided by
   * exactly this number. When no coordinator is available in this process the locally registered
   * count is the honest answer, and it is never zero, so the division the rate limiter performs is
   * always safe.
   */
  def numConcurrentShuffles: Int = {
    if (coordinator == null) {
      math.max(1, numRegisteredShuffles)
    } else {
      coordinator.numConcurrentShuffles
    }
  }

  /**
   * The shuffles whose buffers make up this executor's aggregate utilisation, ascending.
   *
   * A shuffle counts when it is registered here, which is what makes the reading executor-scoped
   * rather than cluster-scoped. When the coordinator does report an active set, that set is used to
   * filter out a shuffle this executor has registered but the cluster has already finished with; if
   * the intersection is empty -- which is the normal state on an executor whose coordinator is a
   * remote reference, and in the window before a registration has propagated -- the locally
   * registered shuffles are used unfiltered rather than reporting no utilisation at all.
   */
  def concurrentShuffleIds: Seq[Int] = {
    val local = registeredShuffleIds
    if (coordinator == null || local.isEmpty) {
      local
    } else {
      val active = coordinator.activeShuffleIds.toSet
      val shared = local.filter(active.contains)
      if (shared.isEmpty) local else shared
    }
  }

  /**
   * Opens the credit ledger for one stream.
   *
   * The credit limit is the number of bytes the producer may hold unacknowledged to this consumer
   * at once. The writer derives it from the per-partition share of its buffer budget, because the
   * bytes in flight are exactly the bytes it must retain in order to be able to retransmit them:
   * granting more credit than the buffer can hold would promise a retransmission the producer could
   * not perform, and granting less would idle a consumer that is keeping up.
   *
   * Registering an already-registered stream is a no-op that keeps the existing ledger, so a
   * defensive second call from a retried task cannot discard credit accounting that is in use.
   *
   * @param shuffleId shuffle the stream belongs to; must be non-negative
   * @param partitionId reduce partition the stream feeds; must be non-negative
   * @param creditLimitBytes bytes the producer may hold unacknowledged; must be positive
   * @return true if a new ledger was opened, false if one already existed
   */
  def registerStream(shuffleId: Int, partitionId: Int, creditLimitBytes: Long): Boolean = {
    require(shuffleId >= 0, s"The shuffle id must be non-negative but was $shuffleId.")
    require(partitionId >= 0, s"The partition id must be non-negative but was $partitionId.")
    require(creditLimitBytes > 0L,
      s"The credit limit of shuffle $shuffleId partition $partitionId must be positive but was " +
        s"$creditLimitBytes.")
    val key = BackpressureStreamKey(shuffleId, partitionId)
    val ledger = new BackpressureProtocol.StreamLedger(key, creditLimitBytes, clock.getTimeMillis())
    val existing = streams.putIfAbsent(key, ledger)
    val opened = existing == null
    if (opened && debugEnabled) {
      logDebug(log"Opened a streaming shuffle credit ledger for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, partitionId)} with " +
        log"${MDC(NUM_BYTES, creditLimitBytes)} bytes of credit")
    }
    opened
  }

  /**
   * Closes the ledger of one stream, releasing every byte of credit it held. Idempotent and safe
   * for a stream that was never registered, because this runs from task-completion cleanup that
   * executes on success, on failure and on cancellation alike.
   *
   * @return true if a ledger was closed
   */
  def unregisterStream(shuffleId: Int, partitionId: Int): Boolean = {
    streams.remove(BackpressureStreamKey(shuffleId, partitionId)) != null
  }

  /** Whether a ledger is open for this stream. */
  def isStreamRegistered(shuffleId: Int, partitionId: Int): Boolean =
    streams.containsKey(BackpressureStreamKey(shuffleId, partitionId))

  /** How many streams currently hold a ledger. */
  def streamCount: Int = streams.size()

  /**
   * Every registered stream, ordered by shuffle id and then partition id, so that a reading of the
   * ledger set is deterministic and an assertion can compare it directly.
   */
  def registeredStreams: Seq[BackpressureStreamKey] = {
    streams.keySet().asScala.toSeq.sortBy(key => (key.shuffleId, key.partitionId))
  }

  /**
   * Asks whether a block may be sent now, and charges it against both the consumer's credit and the
   * pacing bucket when the answer is yes.
   *
   * This is the single admission gate on the producer's egress path, and it applies the two layers
   * in the order that costs least: credit first, because it is a field comparison, and the token
   * bucket second, because it reads the clock and performs a compare-and-set. Either refusal leaves
   * the block entirely uncharged -- no credit consumed, no tokens debited -- so a caller may retry
   * the identical block later without leaking allowance, and the stream enters the throttled state
   * exactly once per episode rather than once per refusal.
   *
   * Admission records the block in the unacknowledged window, which is what makes retransmission
   * possible: the window is precisely the set of blocks whose bytes the producer must still be
   * holding, and it is bounded by the credit limit rather than by the size of the shuffle.
   *
   * Does not block, park or sleep, so it is safe to call from a network event-loop thread. An
   * unregistered stream is admitted without accounting, because a protocol that never learned about
   * a stream must not be the reason a legitimate transfer stalls.
   *
   * @param shuffleId shuffle the block belongs to
   * @param partitionId reduce partition the block is destined for
   * @param bytes encoded size of the block; a non-positive size is admitted without charge
   * @param sequenceNumber position of the block within its partition's stream; must be non-negative
   * @return true if the caller may send the block now, false if it must hold it and retry
   */
  def tryAdmit(
      shuffleId: Int,
      partitionId: Int,
      bytes: Long,
      sequenceNumber: Long): Boolean = {
    require(sequenceNumber >= 0L,
      s"The sequence number must be non-negative but was $sequenceNumber.")
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) {
      true
    } else if (bytes <= 0L) {
      // An empty block consumes neither credit nor tokens, so it can never be the cause of
      // backpressure and must not be recorded in a window that exists to be retransmitted.
      true
    } else if (!ledger.hasCreditFor(bytes)) {
      enterThrottled(ledger, BackpressureProtocol.THROTTLE_CAUSE_CREDIT)
      false
    } else if (!rateLimiter.tryAcquire(bytes)) {
      enterThrottled(ledger, BackpressureProtocol.THROTTLE_CAUSE_RATE)
      false
    } else {
      ledger.recordSent(sequenceNumber, bytes)
      true
    }
  }

  /**
   * Whether the producer may send anything at all to this consumer right now.
   *
   * This is the predicate the streaming client handler reads when it decides whether to leave a
   * channel's `autoRead` enabled: exhausted credit is what disables it, and the acknowledgement
   * that advances the consumer position is what re-enables it. The handler owns the toggle; this
   * class owns the answer.
   *
   * Fails open. A stream with no ledger, and a stream whose producer has already announced the end
   * of its output, both report credit, because in neither case is withholding traffic the correct
   * response: the first is a stream this protocol was never told about and the second has nothing
   * further to send.
   */
  def hasCredit(shuffleId: Int, partitionId: Int): Boolean = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    ledger == null || ledger.isTerminated || ledger.hasCredit
  }

  /**
   * Bytes of credit still available to this stream, or `Long.MaxValue` for a stream with no ledger,
   * which is the honest answer to "how much may I send" when no allowance is being tracked.
   */
  def availableCreditBytes(shuffleId: Int, partitionId: Int): Long = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) Long.MaxValue else ledger.availableCredit
  }

  /** Bytes this stream has sent but not yet had acknowledged; zero for an unknown stream. */
  def outstandingBytes(shuffleId: Int, partitionId: Int): Long = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) 0L else ledger.outstandingBytes
  }

  /** The credit limit this stream was opened with, or zero for an unknown stream. */
  def creditLimitBytes(shuffleId: Int, partitionId: Int): Long = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) 0L else ledger.creditLimitBytes
  }

  /** Bytes this stream has sent in total, acknowledged or not; zero for an unknown stream. */
  def sentBytes(shuffleId: Int, partitionId: Int): Long = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) 0L else ledger.sentBytes
  }

  /** Bytes of this stream the consumer has acknowledged; zero for an unknown stream. */
  def acknowledgedBytes(shuffleId: Int, partitionId: Int): Long = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) 0L else ledger.acknowledgedBytes
  }

  /**
   * Applies a consumer acknowledgement, which is the single event that frees producer memory and
   * lifts backpressure.
   *
   * Every block at or below `consumerPosition` leaves the unacknowledged window and its bytes are
   * returned to the stream's credit, so the producer may both send again and release the buffers it
   * was holding for a retransmission that can no longer be requested. A stream that was throttled
   * because its credit was exhausted returns to the flowing state here, which is also what lets the
   * client handler re-enable `autoRead`.
   *
   * Acknowledgements are monotonic and a stale or duplicate one is ignored rather than treated as a
   * regression: a position at or below the one already recorded releases nothing and returns zero.
   * That is what makes the method safe to call from a network event-loop thread that may deliver
   * two acknowledgements out of order.
   *
   * Applying the acknowledgement also opens the reclamation window: the owner of the freed buffers
   * is expected to confirm the release within 100 ms through [[confirmReclamation]], and a
   * confirmation that arrives later is counted as a breach.
   *
   * @param shuffleId shuffle being acknowledged
   * @param partitionId reduce partition being acknowledged
   * @param consumerPosition highest block sequence number the consumer has consumed, or
   *                         [[BackpressureProtocol.NOTHING_ACKNOWLEDGED]] when it has consumed
   *                         nothing
   * @return bytes released by this acknowledgement, zero if it advanced nothing
   */
  def onAck(shuffleId: Int, partitionId: Int, consumerPosition: Long): Long = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) {
      0L
    } else {
      val nowMillis = clock.getTimeMillis()
      val released = ledger.applyAck(consumerPosition, nowMillis)
      if (released > 0L || ledger.hasCredit) {
        // Credit has been restored, so an episode of credit-driven throttling is over. Leaving the
        // throttled flag set here would suppress the next transition and undercount the metric,
        // which is why the flag is cleared on the acknowledgement rather than on the next send.
        leaveThrottled(ledger)
      }
      if (debugEnabled && released > 0L) {
        logDebug(log"Acknowledgement released ${MDC(NUM_BYTES, released)} bytes of streaming " +
          log"shuffle credit for shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
          log"${MDC(PARTITION_ID, partitionId)}")
      }
      released
    }
  }

  /**
   * Applies an acknowledgement that arrived on the wire. The message carries its own stream
   * identity and consumed position, so no call site has to restate either.
   *
   * @param ack the acknowledgement received; a null message releases nothing
   * @return bytes released by this acknowledgement
   */
  def onAck(ack: AckMessage): Long = {
    if (ack == null) {
      0L
    } else {
      onAck(ack.shuffleId(), ack.partitionId(), ack.consumerPosition())
    }
  }

  /**
   * Highest block sequence number the consumer of this stream has acknowledged, or
   * [[BackpressureProtocol.NOTHING_ACKNOWLEDGED]] when it has acknowledged nothing and for an
   * unknown stream.
   */
  def acknowledgedPosition(shuffleId: Int, partitionId: Int): Long = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) BackpressureProtocol.NOTHING_ACKNOWLEDGED else ledger.acknowledgedPosition
  }

  /**
   * Confirms that the buffers freed by the most recent acknowledgement have been released, and
   * reports how long that took.
   *
   * The feature requires reclamation to complete within 100 ms of an acknowledgement. That bound
   * cannot be observed by whoever performs the release, because it starts at an event on another
   * thread, so it is measured here: the acknowledgement opens the window and this call closes it. A
   * confirmation outside the bound increments [[reclamationDeadlineBreaches]] rather than failing
   * the task, because a late release is a performance fault and not a correctness one.
   *
   * @return the observed latency in milliseconds, or `None` when no acknowledgement is awaiting
   *         confirmation for this stream
   */
  def confirmReclamation(shuffleId: Int, partitionId: Int): Option[Long] = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) {
      None
    } else {
      val latency = ledger.confirmReclamation(clock.getTimeMillis())
      latency.foreach { observed =>
        if (observed > BackpressureProtocol.RECLAMATION_DEADLINE_MS) {
          reclamationBreaches.incrementAndGet()
        }
      }
      latency
    }
  }

  /**
   * Whether an acknowledgement for this stream is still awaiting a reclamation confirmation and the
   * 100 ms bound has already elapsed. This is the reading a poll uses to notice a buffer owner that
   * is not keeping up, without waiting for it to confirm at all.
   */
  def isReclamationOverdue(shuffleId: Int, partitionId: Int): Boolean = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    ledger != null && ledger.isReclamationOverdue(clock.getTimeMillis(),
      BackpressureProtocol.RECLAMATION_DEADLINE_MS)
  }

  /** Reclamations confirmed outside the 100 ms bound, across every stream of this protocol. */
  def reclamationDeadlineBreaches: Long = reclamationBreaches.get()

  /**
   * Inclusive bounds of the window of blocks this stream has sent but not had acknowledged, or
   * `None` when nothing is outstanding.
   *
   * This window is the exact extent of the producer's retransmission capability, and therefore the
   * boundary of the zero-data-loss guarantee: a block inside it can be replayed from memory or from
   * spill, and a block outside it cannot be replayed at all, because its bytes were released the
   * moment the consumer acknowledged them. Loss or corruption outside the window is recovered
   * instead by failing the fetch and letting the unmodified scheduler recompute the upstream stage.
   */
  def unacknowledgedWindow(shuffleId: Int, partitionId: Int): Option[(Long, Long)] = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) None else ledger.unacknowledgedWindow
  }

  /** How many blocks this stream has sent but not had acknowledged. */
  def unacknowledgedBlockCount(shuffleId: Int, partitionId: Int): Int = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) 0 else ledger.unacknowledgedBlockCount
  }

  /** Whether one block is still inside this stream's unacknowledged window. */
  def isWithinUnacknowledgedWindow(
      shuffleId: Int,
      partitionId: Int,
      sequenceNumber: Long): Boolean = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    ledger != null && ledger.containsUnacknowledged(sequenceNumber)
  }

  /**
   * Whether a retransmission request can be served in full.
   *
   * The requested window is inclusive at both ends and the message itself has already refused an
   * upper bound below its lower one, so the only question left is whether every block it names is
   * still retained -- that is, still inside the unacknowledged window. A request that is not
   * entirely serviceable is refused as a whole rather than served in part, because a consumer
   * splicing a partial replay into its input would reorder the partition; the reader escalates such
   * a request to a fetch failure and the upstream stage is recomputed.
   *
   * A pure predicate: it records nothing and changes no state, so it can be consulted as often as a
   * caller likes.
   *
   * @param request the retransmission request received; a null request is never serviceable
   */
  def canServeRetransmit(request: RetransmitRequestMessage): Boolean = {
    if (request == null) {
      false
    } else {
      val ledger =
        streams.get(BackpressureStreamKey(request.shuffleId(), request.partitionId()))
      ledger != null &&
        ledger.containsUnacknowledged(request.firstSequenceNumber()) &&
        ledger.containsUnacknowledged(request.lastSequenceNumber()) &&
        request.blockCount() <= ledger.unacknowledgedBlockCount.toLong
    }
  }

  /**
   * Records an inbound retransmission request and reports whether it can be served.
   *
   * A request is also evidence that the peer is alive, so it refreshes the stream's inbound
   * activity instant in the same way a heartbeat does. A serviceable request advances the stream's
   * retry count, which is what bounds replay at five attempts with an exponentially growing pause;
   * an unserviceable one advances nothing, because there is nothing to retry.
   *
   * @return true if the producer may replay every block the request names
   */
  def onRetransmitRequest(request: RetransmitRequestMessage): Boolean = {
    if (request == null) {
      false
    } else {
      val key = BackpressureStreamKey(request.shuffleId(), request.partitionId())
      val ledger = streams.get(key)
      if (ledger != null) {
        ledger.recordInbound(clock.getTimeMillis())
      }
      val serviceable = ledger != null && canServeRetransmit(request)
      if (serviceable) {
        val attempt = ledger.recordRetransmitAttempt()
        if (debugEnabled) {
          logDebug(log"Serving a streaming shuffle retransmission of " +
            log"${MDC(COUNT, request.blockCount())} block(s) for shuffle " +
            log"${MDC(SHUFFLE_ID, request.shuffleId())} partition " +
            log"${MDC(PARTITION_ID, request.partitionId())} on attempt " +
            log"${MDC(MAX_ATTEMPTS, attempt)}")
        }
      }
      serviceable
    }
  }

  /**
   * The pause a caller should observe before the next replay of this stream, or `None` when the
   * attempt budget is spent and the failure must be escalated instead of retried.
   *
   * Attempts are counted per stream and the pause grows exponentially from one second, so the five
   * permitted attempts span one, two, four, eight and sixteen seconds. Progress resets the count:
   * an acknowledgement that advances the consumer position means the stream is healthy again, so a
   * later, unrelated failure gets the full budget rather than inheriting an exhausted one.
   */
  def nextRetryBackoffMillis(shuffleId: Int, partitionId: Int): Option[Long] = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) {
      Some(BackpressureProtocol.RETRY_BASE_BACKOFF_MS)
    } else {
      val attempt = ledger.retransmitAttempts
      if (attempt >= BackpressureProtocol.MAX_RETRY_ATTEMPTS) {
        None
      } else {
        Some(BackpressureProtocol.retryBackoffMillis(attempt + 1))
      }
    }
  }

  /** Replay attempts already made for this stream since it last made progress. */
  def retransmitAttempts(shuffleId: Int, partitionId: Int): Int = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) 0 else ledger.retransmitAttempts
  }

  /**
   * Whether this stream has spent its five replay attempts, so that the next failure must be
   * escalated to a fetch failure rather than retried.
   */
  def isRetryExhausted(shuffleId: Int, partitionId: Int): Boolean = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    ledger != null && ledger.retransmitAttempts >= BackpressureProtocol.MAX_RETRY_ATTEMPTS
  }

  /**
   * Records inbound data on a stream, which is both consumer-side progress and proof that the
   * producer is alive.
   *
   * The consumer calls this as each block arrives, before it acknowledges: liveness must be
   * refreshed by the arrival itself, because a consumer that is slow to acknowledge would otherwise
   * declare a perfectly healthy producer dead at the five-second mark.
   *
   * @param shuffleId shuffle the block belongs to
   * @param partitionId reduce partition it was received for
   * @param sequenceNumber position of the block within the stream; must be non-negative
   * @param bytes encoded size of the block; a non-positive size records activity but no volume
   */
  def onDataReceived(
      shuffleId: Int,
      partitionId: Int,
      sequenceNumber: Long,
      bytes: Long): Unit = {
    require(sequenceNumber >= 0L,
      s"The sequence number must be non-negative but was $sequenceNumber.")
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger != null) {
      ledger.recordReceived(sequenceNumber, bytes, clock.getTimeMillis())
    }
  }

  /**
   * Records that a heartbeat was received for a stream.
   *
   * The instant recorded is this executor's own, read from the injected clock. The timestamp the
   * peer wrote into the heartbeat is kept separately, for diagnostics only, because two hosts do
   * not agree on the wall clock and a liveness detector built on a remote reading would mistake
   * skew for a failure.
   */
  def onHeartbeat(shuffleId: Int, partitionId: Int): Unit = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger != null) {
      ledger.recordHeartbeat(clock.getTimeMillis(), BackpressureProtocol.NO_TIMESTAMP)
    }
  }

  /**
   * Records a heartbeat that arrived on the wire, keeping the sender's timestamp for diagnostics
   * while judging liveness from the local instant of arrival.
   *
   * @param heartbeat the heartbeat received; a null message records nothing
   */
  def onHeartbeat(heartbeat: HeartbeatMessage): Unit = {
    if (heartbeat != null) {
      val key = BackpressureStreamKey(heartbeat.shuffleId(), heartbeat.partitionId())
      val ledger = streams.get(key)
      if (ledger != null) {
        ledger.recordHeartbeat(clock.getTimeMillis(), heartbeat.timestampMs())
      }
    }
  }

  /**
   * The timestamp the peer stamped into the most recent heartbeat of this stream, or `None` when no
   * heartbeat carrying one has been received. Diagnostic only: no liveness decision reads it.
   */
  def remoteHeartbeatTimestamp(shuffleId: Int, partitionId: Int): Option[Long] = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) None else ledger.remoteHeartbeatTimestamp
  }

  /**
   * Whether this stream is due to emit a heartbeat, that is whether the last one it sent is older
   * than the five-second interval.
   *
   * A terminated stream is never due: its producer has announced the end of its output, so there is
   * nothing left for a heartbeat to keep alive.
   */
  def shouldSendHeartbeat(shuffleId: Int, partitionId: Int): Boolean = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    ledger != null && !ledger.isTerminated &&
      ledger.isHeartbeatDue(clock.getTimeMillis(), BackpressureProtocol.HEARTBEAT_INTERVAL_MS)
  }

  /**
   * Builds the heartbeat to send for a stream, stamped from the injected clock and positioned at
   * the highest sequence number the stream has reached.
   *
   * The heartbeat message type takes its timestamp from the caller and reads no clock of its own,
   * which is exactly what allows this protocol's notion of time to be the single one in play and to
   * be frozen by a test. Emitting is recorded here, so [[shouldSendHeartbeat]] answers false again
   * until the interval has elapsed once more.
   *
   * @return the heartbeat to send, or `None` for a stream with no ledger
   */
  def heartbeatFor(shuffleId: Int, partitionId: Int): Option[HeartbeatMessage] = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) {
      None
    } else {
      val nowMillis = clock.getTimeMillis()
      ledger.recordHeartbeatSent(nowMillis)
      // The clock is the caller's contract with the message type, and a wall clock adjusted
      // backwards past the epoch would otherwise construct a heartbeat the type itself rejects.
      Some(new HeartbeatMessage(shuffleId, partitionId, ledger.heartbeatSequenceNumber,
        math.max(0L, nowMillis)))
    }
  }

  /**
   * Records that a producer has announced the orderly end of a stream.
   *
   * This is what distinguishes silence that means completion from silence that means failure. A
   * terminated stream stops arming both liveness timers, so a stream that legitimately produced
   * nothing at all -- an empty reduce partition, announced with a total of zero blocks, which the
   * protocol treats as entirely valid -- is never mistaken for a producer that died five seconds
   * ago.
   *
   * @param shuffleId shuffle that has finished
   * @param partitionId reduce partition that has finished
   * @param totalBlocks blocks the producer claims to have sent; must be non-negative, and zero is
   *                    the valid announcement of an empty partition
   */
  def onStreamTermination(shuffleId: Int, partitionId: Int, totalBlocks: Long): Unit = {
    require(totalBlocks >= 0L,
      s"The total block count must be non-negative but was $totalBlocks.")
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger != null) {
      ledger.recordTermination(totalBlocks, clock.getTimeMillis())
      // One line per stream at completion is O(reduce partitions), the same order as the
      // coordinator's shuffle-level logging, so it stays inside the log budget with debug off.
      if (debugEnabled) {
        logDebug(log"Streaming shuffle stream for shuffle ${MDC(SHUFFLE_ID, shuffleId)} " +
          log"partition ${MDC(PARTITION_ID, partitionId)} terminated after " +
          log"${MDC(COUNT, totalBlocks)} block(s)")
      }
    }
  }

  /**
   * Records an orderly end-of-stream that arrived on the wire.
   *
   * @param termination the terminator received; a null message records nothing
   */
  def onStreamTermination(termination: StreamTerminationMessage): Unit = {
    if (termination != null) {
      onStreamTermination(
        termination.shuffleId(), termination.partitionId(), termination.totalBlocks())
    }
  }

  /** Whether a producer has announced the orderly end of this stream. */
  def isStreamTerminated(shuffleId: Int, partitionId: Int): Boolean = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    ledger != null && ledger.isTerminated
  }

  /**
   * Blocks the producer of this stream claims to have sent, or `None` when it has not yet announced
   * the end of the stream. A returned zero is a genuine reading and means the partition was empty.
   */
  def announcedBlockCount(shuffleId: Int, partitionId: Int): Option[Long] = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) None else ledger.announcedBlockCount
  }

  /**
   * Applies one inbound control message to the ledger, dispatching on its concrete type.
   *
   * Dispatch is on the type of the message, never on its encoded length: all four fixed-shape
   * control messages encode to the same number of bytes, so length distinguishes nothing and a
   * codec that tried to use it would silently read one message as another.
   *
   * A data block is not a control message and is reported as unhandled rather than rejected,
   * because the block path belongs to the writer and the reader; this method exists so a channel
   * handler can hand off everything else without restating the mapping from message to update.
   *
   * A message stamped with a wire revision this executor does not speak is not applied at all. It
   * latches the version-mismatch degradation reason instead, which is the sanctioned route to the
   * sort-based fallback, and reports itself unhandled.
   *
   * @param message the message received; a null message is unhandled
   * @return the type that was applied, or `None` when nothing was
   */
  def onControlMessage(
      message: StreamingShuffleMessage): Option[StreamingShuffleMessageType] = {
    if (message == null) {
      None
    } else if (!observeProtocolVersion(
        message.shuffleId(), message.partitionId(), message.protocolVersion())) {
      None
    } else {
      message match {
        case ack: AckMessage =>
          onAck(ack)
          Some(StreamingShuffleMessageType.ACK)
        case heartbeat: HeartbeatMessage =>
          onHeartbeat(heartbeat)
          Some(StreamingShuffleMessageType.HEARTBEAT)
        case request: RetransmitRequestMessage =>
          onRetransmitRequest(request)
          Some(StreamingShuffleMessageType.RETRANSMIT_REQUEST)
        case termination: StreamTerminationMessage =>
          onStreamTermination(termination)
          Some(StreamingShuffleMessageType.STREAM_TERMINATION)
        case _ =>
          None
      }
    }
  }

  /**
   * Whether the producer of this stream has fallen silent for longer than the five-second
   * connection timeout, which is the detection the reader turns into a partial-read invalidation
   * and a fetch failure.
   *
   * Silence is measured from the last inbound event of any kind -- a block, a heartbeat or a
   * retransmission request -- so a producer that is alive but momentarily has no data to send stays
   * alive by heartbeating alone. A terminated stream never times out: its silence means completion,
   * and treating completion as failure would recompute a stage that had in fact finished.
   */
  def isProducerTimedOut(shuffleId: Int, partitionId: Int): Boolean = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    ledger != null && isProducerTimedOut(ledger, clock.getTimeMillis())
  }

  /**
   * Whether the consumer of this stream has failed to acknowledge for longer than the ten-second
   * liveness window, which is the detection the writer turns into retention, spill and replay of
   * the unacknowledged window.
   *
   * The window only arms while something is actually outstanding: a consumer with nothing to
   * acknowledge is not slow, it is idle, and reporting it as timed out would make every completed
   * stream look like a failure ten seconds later. A terminated stream is likewise never timed out.
   */
  def isConsumerTimedOut(shuffleId: Int, partitionId: Int): Boolean = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    ledger != null && isConsumerTimedOut(ledger, clock.getTimeMillis())
  }

  /**
   * Every stream whose producer has fallen silent past the five-second bound, in a deterministic
   * order. One reading of the whole ledger set, so a caller acting on several failed producers sees
   * a single consistent snapshot rather than a different answer per stream.
   */
  def timedOutProducerStreams: Seq[BackpressureStreamKey] = {
    val nowMillis = clock.getTimeMillis()
    streams.values().asScala
      .filter(ledger => isProducerTimedOut(ledger, nowMillis))
      .map(_.key)
      .toSeq
      .sortBy(key => (key.shuffleId, key.partitionId))
  }

  /** Every stream whose consumer has stopped acknowledging past the ten-second bound. */
  def timedOutConsumerStreams: Seq[BackpressureStreamKey] = {
    val nowMillis = clock.getTimeMillis()
    streams.values().asScala
      .filter(ledger => isConsumerTimedOut(ledger, nowMillis))
      .map(_.key)
      .toSeq
      .sortBy(key => (key.shuffleId, key.partitionId))
  }

  /**
   * Milliseconds since anything at all was received on this stream, or `None` for an unknown
   * stream. Exposed so that a diagnostic can report how close a stream is to its liveness bound
   * instead of only whether it has crossed it.
   */
  def millisSinceInbound(shuffleId: Int, partitionId: Int): Option[Long] = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) None else Some(ledger.millisSinceInbound(clock.getTimeMillis()))
  }

  /** Milliseconds since this stream's consumer last advanced its position. */
  def millisSinceAck(shuffleId: Int, partitionId: Int): Option[Long] = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) None else Some(ledger.millisSinceAck(clock.getTimeMillis()))
  }

  /**
   * Publishes how many bytes one shuffle is holding in its streaming buffers, and the budget those
   * bytes are measured against.
   *
   * Utilisation is executor-wide and no single shuffle can compute it, so each publishes only the
   * two numbers it knows and this class sums them. Reporting a shuffle that is not registered is
   * ignored rather than allowed to create a contribution with no partition count behind it, which
   * would distort arbitration; a negative reading is clamped to zero for the same reason the gauge
   * clamps it, namely that it can only be an accounting slip and must not be able to reduce the
   * total.
   *
   * @param shuffleId the shuffle reporting
   * @param bufferedBytes bytes it currently holds
   * @param budgetBytes the budget those bytes are measured against
   */
  def reportBufferUtilization(shuffleId: Int, bufferedBytes: Long, budgetBytes: Long): Unit = {
    if (shufflePartitionCounts.containsKey(shuffleId)) {
      shuffleBufferedBytes.put(shuffleId, java.lang.Long.valueOf(math.max(0L, bufferedBytes)))
      shuffleBudgetBytes.put(shuffleId, java.lang.Long.valueOf(math.max(0L, budgetBytes)))
    }
  }

  /** Bytes one shuffle last reported holding; zero for a shuffle that has reported nothing. */
  def reportedBufferedBytes(shuffleId: Int): Long = readLongEntry(shuffleBufferedBytes, shuffleId)

  /** The budget one shuffle last reported; zero for a shuffle that has reported nothing. */
  def reportedBudgetBytes(shuffleId: Int): Long = readLongEntry(shuffleBudgetBytes, shuffleId)

  /**
   * Executor-wide buffer utilisation across the concurrent shuffles, as a percentage of their
   * aggregate budget.
   *
   * The buffered bytes and the budgets are summed independently and the percentage is taken from
   * the two totals, so one busy shuffle and one idle shuffle on the same executor produce a single
   * honest figure rather than two competing ones. Both sums saturate rather than wrap, and an empty
   * set or a zero total budget answers zero, so this reading is total: there is no input for which
   * it throws and no sentinel it can return instead of a percentage.
   *
   * The value is deliberately not clamped at 100. A momentarily over-budget executor stays visible
   * rather than being masked, which is the entire reason a spill threshold is watched.
   */
  def aggregateBufferUtilizationPercent: Long = {
    var bufferedBytes = 0L
    var budgetBytes = 0L
    concurrentShuffleIds.foreach { shuffleId =>
      bufferedBytes =
        BackpressureProtocol.saturatingAdd(bufferedBytes, readLongEntry(shuffleBufferedBytes,
          shuffleId))
      budgetBytes =
        BackpressureProtocol.saturatingAdd(budgetBytes, readLongEntry(shuffleBudgetBytes,
          shuffleId))
    }
    BackpressureProtocol.percentOf(bufferedBytes, budgetBytes)
  }

  /** The configured utilisation percentage at which buffered partitions must be evicted. */
  def spillThresholdPercent: Int = spillThreshold

  /**
   * Whether aggregate utilisation has reached the configured spill threshold, which is the signal
   * the spill manager acts on. Detected here, because utilisation spans the shuffles this protocol
   * arbitrates between; acted on there, because eviction is the spill manager's responsibility.
   */
  def isSpillRequired: Boolean = aggregateBufferUtilizationPercent >= spillThreshold.toLong

  /**
   * The concurrent shuffles ordered by how much of the executor's buffered egress each is
   * responsible for, most demanding first.
   *
   * This is the arbitration the feature requires between concurrent shuffles, and it is keyed on
   * the two quantities it names: pending volume first, because bytes held are what the spill
   * threshold actually measures, then reduce partition count, because a wide shuffle has the larger
   * footprint when two hold equal volume, and finally the shuffle id, so that the order is total
   * and a suite can assert on it without tolerating ties. Every key is carried on the result rather
   * than folded into a score, so it is visible why one shuffle was placed ahead of another.
   */
  def arbitrationOrder: Seq[BackpressureShuffleDemand] = {
    // One pass over the ledger set folds every stream into its shuffle's totals, so the cost is
    // linear in the number of streams rather than quadratic in shuffles times streams. Plain
    // mutable maps suffice because they never escape this method.
    val pending = mutable.Map.empty[Int, Long]
    val counts = mutable.Map.empty[Int, Int]
    val ledgers = streams.values().iterator()
    while (ledgers.hasNext) {
      val ledger = ledgers.next()
      val shuffleId = ledger.key.shuffleId
      pending(shuffleId) = BackpressureProtocol.saturatingAdd(
        pending.getOrElse(shuffleId, 0L), ledger.outstandingBytes)
      counts(shuffleId) = counts.getOrElse(shuffleId, 0) + 1
    }
    concurrentShuffleIds.map { shuffleId =>
      BackpressureShuffleDemand(
        shuffleId,
        partitionCountOf(shuffleId).getOrElse(0),
        pending.getOrElse(shuffleId, 0L),
        counts.getOrElse(shuffleId, 0))
    }.sortBy { demand =>
      // Negating the two descending keys keeps the whole comparison in one total ordering, and the
      // ascending shuffle id breaks every remaining tie, so the result never depends on the order
      // the concurrent map happened to enumerate.
      (-demand.pendingBytes, -demand.numPartitions, demand.shuffleId)
    }
  }

  /** The arbitration order reduced to shuffle ids, the one that must yield first leading. */
  def yieldOrder: Seq[Int] = arbitrationOrder.map(_.shuffleId)

  /**
   * The shuffle that is guaranteed to keep making progress while the executor is over its spill
   * threshold, namely the least demanding of the concurrent shuffles.
   *
   * Exempting exactly one shuffle from yielding is what turns "the largest yields first" into a
   * genuine anti-starvation guarantee: without it, a small shuffle sharing an executor with a very
   * large one could be asked to yield on every arbitration pass and never finish.
   */
  def guaranteedShuffleId: Option[Int] = yieldOrder.lastOption

  /**
   * Whether one shuffle should yield buffer space on this pass.
   *
   * Only asked when the executor is actually over its threshold, and never true for the guaranteed
   * shuffle, so a shuffle with few partitions and little pending data always makes progress.
   */
  def shouldYield(shuffleId: Int): Boolean = {
    isSpillRequired && guaranteedShuffleId.exists(_ != shuffleId) &&
      shufflePartitionCounts.containsKey(shuffleId)
  }

  /**
   * Bytes per second the producer of this stream has sustained since the stream opened, or zero
   * while less than a millisecond has elapsed and for an unknown stream.
   */
  def producerRateBytesPerSecond(shuffleId: Int, partitionId: Int): Long = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) 0L else ledger.producerRate(clock.getTimeMillis())
  }

  /** Bytes per second the consumer of this stream has acknowledged since the stream opened. */
  def consumerRateBytesPerSecond(shuffleId: Int, partitionId: Int): Long = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) 0L else ledger.consumerRate(clock.getTimeMillis())
  }

  /**
   * Whether this stream's consumer is at least twice as slow as its producer right now.
   *
   * Compared as byte totals rather than as rates, because both totals are measured over the same
   * elapsed interval and so share a denominator: dropping the division makes the comparison exact
   * integer arithmetic with no rounding and no divide by zero. A consumer that has acknowledged
   * nothing while the producer has sent something is slower by any ratio, which is the correct
   * reading and not a degenerate one.
   *
   * A stream is only judged slow while something is actually outstanding, so a consumer that has
   * caught up completely is never reported as slow, however little it has consumed in total.
   */
  def isConsumerSlow(shuffleId: Int, partitionId: Int): Boolean = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    ledger != null && ledger.isConsumerSlow(BackpressureProtocol.CONSUMER_SLOWNESS_RATIO)
  }

  /**
   * Whether this stream's consumer has been at least twice as slow as its producer continuously
   * for longer than sixty seconds, which is the first of the four conditions under which streaming
   * steps aside.
   *
   * Evaluated rather than merely read: each call re-observes the instantaneous imbalance and
   * updates the latch that records when the imbalance began, then answers from that latch. A
   * consumer that recovers clears the latch, so the sixty seconds must be continuous and a workload
   * that oscillates is never tripped by an accumulation of unrelated slow moments. That also makes
   * the detector fully deterministic under a frozen clock: observe once, advance the clock past the
   * window, observe again.
   *
   * Detection only. Latching the reason is as far as this class goes; the fallback policy decides
   * whether to delegate to the sort-based manager.
   */
  def isConsumerSustainedSlow(shuffleId: Int, partitionId: Int): Boolean = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (ledger == null) {
      false
    } else {
      val sustained = ledger.observeSlowness(
        clock.getTimeMillis(),
        BackpressureProtocol.CONSUMER_SLOWNESS_RATIO,
        BackpressureProtocol.SUSTAINED_SLOWNESS_WINDOW_MS)
      if (sustained) {
        latchDegradation(BackpressureDegradationReason.ConsumerSustainedSlowness)
      }
      sustained
    }
  }

  /**
   * Records that a streaming buffer could not be allocated even after spilling, which is the second
   * condition under which streaming steps aside: continuing would risk exhausting the heap rather
   * than merely slowing the job down.
   */
  def reportBufferAllocationFailure(shuffleId: Int, partitionId: Int): Unit = {
    latchDegradation(BackpressureDegradationReason.BufferAllocationFailure)
    if (debugEnabled) {
      logDebug(log"Streaming shuffle buffer allocation failed for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, partitionId)}")
    }
  }

  /**
   * How saturated the administered link is, as a percentage, derived from how depleted the pacing
   * bucket is.
   *
   * No operating-system or network counter is available inside the JVM, so the bucket is the honest
   * proxy: it is refilled at exactly the rate the operator's declared capacity permits, therefore
   * the fraction of it that has been consumed is the fraction of that permitted rate currently in
   * use. An unlimited limiter reports zero, because a link with no declared capacity cannot be
   * saturated relative to one.
   */
  def linkSaturationPercent: Long = {
    if (rateLimiter.isUnlimited) {
      0L
    } else {
      val capacity = rateLimiter.capacityBytes
      if (capacity <= 0L) {
        0L
      } else {
        val available = math.max(0L, math.min(rateLimiter.availableTokens, capacity))
        BackpressureProtocol.percentOf(capacity - available, capacity)
      }
    }
  }

  /**
   * Whether the link is saturated beyond ninety percent, which is the third condition under which
   * streaming steps aside. Latches the reason when it holds, and reports it; the trip decision
   * belongs to the fallback policy.
   */
  def isLinkSaturated: Boolean = {
    val saturated = linkSaturationPercent > BackpressureProtocol.LINK_SATURATION_PERCENT
    if (saturated) {
      latchDegradation(BackpressureDegradationReason.LinkSaturation)
    }
    saturated
  }

  /**
   * Checks a wire revision seen on a message against the one this executor speaks, which is the
   * fourth condition under which streaming steps aside.
   *
   * Compatibility is asked of the protocol type itself rather than restated here, so there is one
   * definition of it in the subsystem. A mismatch is a recoverable condition and not an error: it
   * latches the reason and reports false so the caller can decline the message, and the fallback
   * policy turns it into a delegation rather than a failed job.
   *
   * @return true if a message stamped with this revision may be applied
   */
  def observeProtocolVersion(shuffleId: Int, partitionId: Int, version: Byte): Boolean = {
    if (StreamingShuffleMessage.isCompatible(version)) {
      true
    } else {
      latchDegradation(BackpressureDegradationReason.ProtocolVersionMismatch)
      // Warned once and then only under the debug gate. Every subsequent message from the same
      // peer is refused for the same reason, so repeating the pair of revisions would spend the
      // executor's log budget restating a fact the first line already established.
      if (versionMismatchReported.compareAndSet(false, true)) {
        logWarning(log"Streaming shuffle message for shuffle ${MDC(SHUFFLE_ID, shuffleId)} " +
          log"partition ${MDC(PARTITION_ID, partitionId)} was refused because the peer speaks " +
          log"wire revision ${MDC(COUNT, version)} and this executor speaks " +
          log"${MDC(THRESHOLD, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)}")
      } else if (debugEnabled) {
        logDebug(log"Streaming shuffle message for shuffle ${MDC(SHUFFLE_ID, shuffleId)} " +
          log"partition ${MDC(PARTITION_ID, partitionId)} was refused at wire revision " +
          log"${MDC(COUNT, version)}")
      }
      false
    }
  }

  /** Whether any of the four degradation conditions has been observed. */
  def isDegraded: Boolean = !degradations.isEmpty

  /**
   * Every degradation condition observed so far, in the stable order the reasons are declared in,
   * so a log line or an assertion is never at the mercy of set iteration order.
   */
  def degradationReasons: Seq[BackpressureDegradationReason] =
    BackpressureDegradationReason.values.filter(degradations.contains)

  /**
   * Forgets every degradation condition.
   *
   * Reasons are latched, because the fallback policy may sample them after the condition that
   * caused one has passed, so there has to be an explicit way back. Clearing is what a caller does
   * once it has delegated to the sort-based path and is starting a fresh measurement, and what a
   * suite does between cases.
   */
  def clearDegradation(): Unit = {
    degradations.clear()
    reportedDegradations.clear()
  }

  /**
   * The state of the executor's streaming shuffle as a whole: the most severe state any of its
   * streams is in.
   *
   * The overlays are applied in severity order, which is what makes one reading meaningful across a
   * set of streams. Degradation dominates everything, because a subsystem that has to step aside is
   * in no position to describe itself as merely throttled. Spilling dominates throttling, because
   * utilisation is an executor-wide condition while credit is per stream.
   */
  def state: BackpressureState = {
    if (isDegraded) {
      BackpressureState.Degraded
    } else if (isSpillRequired) {
      BackpressureState.Spilling
    } else if (throttledStreamCount > 0) {
      BackpressureState.Throttled
    } else {
      BackpressureState.Flowing
    }
  }

  /**
   * The state of one stream, with the executor-wide overlays applied.
   *
   * A stream with no ledger reports `Flowing`, consistent with [[hasCredit]] failing open: a
   * protocol never told about a stream imposes nothing on it and must not describe it as impeded.
   */
  def streamState(shuffleId: Int, partitionId: Int): BackpressureState = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    if (isDegraded) {
      BackpressureState.Degraded
    } else if (isSpillRequired) {
      BackpressureState.Spilling
    } else if (ledger != null && ledger.isThrottled) {
      BackpressureState.Throttled
    } else {
      BackpressureState.Flowing
    }
  }

  /** How many streams are currently held back by credit or by pacing. */
  def throttledStreamCount: Int = streams.values().asScala.count(_.isThrottled)

  /** Whether this particular stream is currently held back. */
  def isStreamThrottled(shuffleId: Int, partitionId: Int): Boolean = {
    val ledger = streams.get(BackpressureStreamKey(shuffleId, partitionId))
    ledger != null && ledger.isThrottled
  }

  /**
   * Transitions into the throttled state counted by this protocol instance.
   *
   * The executor-wide `shuffle.streaming.backpressureEvents` counter is advanced in step with this,
   * but that counter is a JVM singleton and accumulates across every protocol in the process, so a
   * local total is kept for a caller that needs to speak about one instance.
   */
  def backpressureEventCount: Long = throttleTransitions.get()

  /**
   * Re-evaluates every stream and reports how many changed state, which is what a caller driving
   * the protocol on a timer at the hundred-millisecond cadence calls.
   *
   * A poll performs no I/O, takes no lock and reads the clock exactly once, so it costs one pass
   * over the ledger set. It exists because two of this class's judgements are time-derived rather
   * than event-derived -- the sustained-slowness latch and the reclamation deadline -- and a
   * subsystem with no data flowing would otherwise never notice either.
   *
   * @return the number of streams whose effective state differed from the state they were in before
   */
  def pollOnce(): Int = {
    val nowMillis = clock.getTimeMillis()
    lastPollMillis.set(nowMillis)
    var changed = 0
    val ledgers = streams.values().iterator()
    while (ledgers.hasNext) {
      val ledger = ledgers.next()
      val wasThrottled = ledger.isThrottled
      // Credit may have been restored by an acknowledgement processed on a network thread that had
      // no reason to consult the pacing layer, so the flowing transition is confirmed here.
      if (wasThrottled && !ledger.isTerminated && ledger.hasCredit) {
        leaveThrottled(ledger)
      }
      val sustained = ledger.observeSlowness(
        nowMillis,
        BackpressureProtocol.CONSUMER_SLOWNESS_RATIO,
        BackpressureProtocol.SUSTAINED_SLOWNESS_WINDOW_MS)
      if (sustained) {
        latchDegradation(BackpressureDegradationReason.ConsumerSustainedSlowness)
      }
      if (ledger.isReclamationOverdue(nowMillis, BackpressureProtocol.RECLAMATION_DEADLINE_MS)) {
        reclamationBreaches.incrementAndGet()
        ledger.forgetReclamationDeadline()
      }
      if (wasThrottled != ledger.isThrottled) {
        changed += 1
      }
    }
    changed
  }

  /**
   * Whether the next poll is due, that is whether at least a hundred milliseconds have elapsed
   * since the last one. Offered so that a caller sharing a timer with other work does not have to
   * keep this bookkeeping itself.
   */
  def isPollDue: Boolean =
    clock.getTimeMillis() - lastPollMillis.get() >= BackpressureProtocol.POLL_INTERVAL_MS

  /** Instant of the most recent poll, as read from the injected clock. */
  def lastPollTimeMillis: Long = lastPollMillis.get()

  /**
   * Drops every ledger, every utilisation contribution, every registration and every latched
   * degradation, and rewinds the local counters, so that this protocol reports what a freshly
   * constructed one would.
   *
   * Useful between measurements and between test cases. The rate limiter is left alone, because it
   * is owned by the caller that supplied it and may be shared with another protocol instance.
   */
  def reset(): Unit = {
    streams.clear()
    shufflePartitionCounts.clear()
    shuffleBufferedBytes.clear()
    shuffleBudgetBytes.clear()
    degradations.clear()
    reportedDegradations.clear()
    versionMismatchReported.set(false)
    throttleTransitions.set(0L)
    reclamationBreaches.set(0L)
    lastPollMillis.set(clock.getTimeMillis())
  }

  override def toString: String = {
    s"BackpressureProtocol(state=$state, streams=$streamCount, " +
      s"shuffles=$numRegisteredShuffles, utilizationPercent=$aggregateBufferUtilizationPercent, " +
      s"spillThresholdPercent=$spillThreshold, backpressureEvents=${throttleTransitions.get()})"
  }

  /**
   * Moves a stream into the throttled state, counting the transition exactly once per episode.
   *
   * The edge, not the refusal, is what the `shuffle.streaming.backpressureEvents` metric counts: a
   * stream that stays throttled while its producer retries the same block many times represents one
   * episode of backpressure, and counting each retry would report an operator a number that grows
   * with retry frequency rather than with the number of times flow control engaged. The compare and
   * set is what makes that exact without a lock, and it is also what bounds the log volume, since a
   * line is only ever considered on an edge.
   */
  private def enterThrottled(
      ledger: BackpressureProtocol.StreamLedger,
      cause: String): Unit = {
    if (ledger.enterThrottled()) {
      throttleTransitions.incrementAndGet()
      StreamingShuffleMetricsSource.incrementBackpressureEvents(1L)
      // At most one info line per stream for the whole of its life, reported on its first episode
      // only. Log volume is therefore O(streams) rather than O(episodes), which is what keeps the
      // subsystem inside its budget when a slow consumer throttles a producer repeatedly.
      if (ledger.markThrottleReported()) {
        logInfo(log"Streaming shuffle throttled shuffle ${MDC(SHUFFLE_ID, ledger.key.shuffleId)} " +
          log"partition ${MDC(PARTITION_ID, ledger.key.partitionId)} because " +
          log"${MDC(REASON, cause)}, holding ${MDC(NUM_BYTES, ledger.outstandingBytes)} " +
          log"unacknowledged byte(s)")
      } else if (debugEnabled) {
        logDebug(log"Streaming shuffle throttled shuffle " +
          log"${MDC(SHUFFLE_ID, ledger.key.shuffleId)} partition " +
          log"${MDC(PARTITION_ID, ledger.key.partitionId)} because ${MDC(REASON, cause)}")
      }
    }
  }

  /**
   * Moves a stream back into the flowing state. Silent by design: the return to normal is visible
   * in the state accessor and in the counter that did not advance, and a line here would double the
   * log volume of every throttling episode for no diagnostic gain.
   */
  private def leaveThrottled(ledger: BackpressureProtocol.StreamLedger): Unit = {
    if (ledger.leaveThrottled() && debugEnabled) {
      logDebug(log"Streaming shuffle resumed shuffle ${MDC(SHUFFLE_ID, ledger.key.shuffleId)} " +
        log"partition ${MDC(PARTITION_ID, ledger.key.partitionId)} with " +
        log"${MDC(NUM_BYTES, ledger.availableCredit)} byte(s) of credit")
    }
  }

  /**
   * Latches a degradation reason and reports it at most once for the lifetime of this protocol.
   *
   * There are only four reasons, so an unconditional line per reason is bounded by four lines per
   * protocol instance and is worth emitting at default level: a job that quietly reverted to
   * sort-based shuffle is exactly the situation in which an operator needs to find out why.
   */
  private def latchDegradation(reason: BackpressureDegradationReason): Unit = {
    degradations.add(reason)
    if (reportedDegradations.add(reason)) {
      logInfo(log"Streaming shuffle is no longer sustainable and should yield to sort-based " +
        log"shuffle: ${MDC(REASON, reason.code)}. Aggregate buffer utilisation is " +
        log"${MDC(PERCENT, aggregateBufferUtilizationPercent)}% against a threshold of " +
        log"${MDC(THRESHOLD, spillThreshold)}% across " +
        log"${MDC(COUNT, numRegisteredShuffles)} shuffle(s)")
    }
  }

  /**
   * Whether a producer has been silent past the five-second bound. Shared by the single-stream
   * predicate and by the whole-set scan so that both apply one definition, including the exemption
   * that matters most: a terminated stream is complete rather than silent, so the silence that
   * follows an orderly end of stream is never mistaken for a producer that died.
   */
  private def isProducerTimedOut(
      ledger: BackpressureProtocol.StreamLedger,
      nowMillis: Long): Boolean = {
    !ledger.isTerminated &&
      ledger.millisSinceInbound(nowMillis) > BackpressureProtocol.ACK_TIMEOUT_MS
  }

  /**
   * Whether a consumer has stopped acknowledging past the ten-second bound. Arms only while bytes
   * are actually outstanding, because a consumer with nothing to acknowledge is idle rather than
   * unresponsive.
   */
  private def isConsumerTimedOut(
      ledger: BackpressureProtocol.StreamLedger,
      nowMillis: Long): Boolean = {
    !ledger.isTerminated && ledger.outstandingBytes > 0L &&
      ledger.millisSinceAck(nowMillis) > BackpressureProtocol.CONSUMER_LIVENESS_TIMEOUT_MS
  }

  /**
   * Reads a byte count out of one of the per-shuffle maps, answering zero for an absent entry. The
   * maps hold boxed values because they are Java concurrent maps, and an absent key reads back as
   * null rather than as zero, so every read goes through here rather than repeating the null check.
   */
  private def readLongEntry(source: ConcurrentHashMap[Int, java.lang.Long], key: Int): Long = {
    val value = source.get(key)
    if (value == null) 0L else value.longValue()
  }
}

/**
 * Constants, pure helpers and the per-stream ledger of the streaming shuffle backpressure protocol.
 *
 * Every numeric bound the feature fixes is declared here exactly once, so that the protocol, the
 * components that consult it and the suites that exercise it all speak in terms of the same value
 * rather than repeating a literal that could drift.
 */
private[spark] object BackpressureProtocol {

  /**
   * Producer-liveness bound: a stream that receives nothing at all for this long is treated as
   * having lost its producer. It is also the interval at which a heartbeat is emitted, so a
   * producer with no data to send stays alive by heartbeating at the cadence the detector expects.
   *
   * Enforced at application level and not by TCP. The transport exposes keepalive as a boolean only
   * and the JDK offers no socket option for a keepalive interval, so this timer is the whole of the
   * five-second bound.
   */
  val ACK_TIMEOUT_MS: Long = 5000L

  /** Interval at which a stream emits a heartbeat, equal to the producer-liveness bound. */
  val HEARTBEAT_INTERVAL_MS: Long = ACK_TIMEOUT_MS

  /**
   * Consumer-liveness bound: a consumer that has bytes outstanding and acknowledges none of them
   * for this long is treated as gone, which is what makes the writer retain, spill and later replay
   * its unacknowledged window.
   */
  val CONSUMER_LIVENESS_TIMEOUT_MS: Long = 10000L

  /**
   * The bound within which the buffers freed by an acknowledgement must actually be released. A
   * confirmation later than this is counted as a breach rather than failing the task, because a
   * late release costs throughput and not correctness.
   */
  val RECLAMATION_DEADLINE_MS: Long = 100L

  /** Cadence at which a caller should drive [[BackpressureProtocol.pollOnce]]. */
  val POLL_INTERVAL_MS: Long = 100L

  /**
   * How long a consumer must remain at least [[CONSUMER_SLOWNESS_RATIO]] times slower than its
   * producer, continuously, before streaming is judged unsustainable for the workload.
   */
  val SUSTAINED_SLOWNESS_WINDOW_MS: Long = 60000L

  /** The factor by which a consumer must lag its producer to count as slow. */
  val CONSUMER_SLOWNESS_RATIO: Long = 2L

  /** Link utilisation, as a percentage, above which the network is judged saturated. */
  val LINK_SATURATION_PERCENT: Long = 90L

  /** Divisor and multiplier of every percentage this protocol computes. */
  val PERCENT_SCALE: Long = 100L

  /** Milliseconds in one second; every rate this protocol reports is per second. */
  val MILLIS_PER_SECOND: Long = 1000L

  /** The pause before the first replay attempt, which each further attempt doubles. */
  val RETRY_BASE_BACKOFF_MS: Long = 1000L

  /** Replay attempts permitted for one stream before a failure must be escalated. */
  val MAX_RETRY_ATTEMPTS: Int = 5

  /**
   * The acknowledged position of a stream whose consumer has consumed nothing. Taken from the
   * acknowledgement message type rather than restated, so the protocol and the wire agree on the
   * sentinel by construction.
   */
  val NOTHING_ACKNOWLEDGED: Long = AckMessage.NOTHING_CONSUMED

  /** Sentinel for a timestamp that has never been recorded. */
  val NO_TIMESTAMP: Long = Long.MinValue

  /** Sentinel for a sequence number that has never been recorded. */
  val NO_SEQUENCE: Long = Long.MinValue

  /** Sentinel for a block total that a producer has not yet announced. */
  val NO_BLOCK_TOTAL: Long = -1L

  /** Reason recorded when a stream is throttled because its consumer's credit is exhausted. */
  val THROTTLE_CAUSE_CREDIT: String = "consumer credit is exhausted"

  /** Reason recorded when a stream is throttled because the pacing bucket refused it. */
  val THROTTLE_CAUSE_RATE: String = "the egress rate limit was reached"

  /**
   * The pause before a given replay attempt: one second doubled once per attempt, so the five
   * permitted attempts span one, two, four, eight and sixteen seconds.
   *
   * Pure and total. An attempt at or below zero is treated as the first, and the shift is bounded
   * by [[MAX_RETRY_ATTEMPTS]] so the result can never overflow however it is called.
   *
   * @param attempt the attempt about to be made, counted from one
   * @return the pause in milliseconds
   */
  def retryBackoffMillis(attempt: Int): Long = {
    val bounded = math.min(math.max(1, attempt), MAX_RETRY_ATTEMPTS)
    RETRY_BASE_BACKOFF_MS << (bounded - 1)
  }

  /**
   * Adds two byte counts, treating a negative contribution as zero and saturating at
   * `Long.MaxValue` rather than wrapping. Wrapping would turn an implausibly large total into a
   * negative one and thence into a nonsense percentage, so it is ruled out arithmetically.
   */
  def saturatingAdd(runningTotal: Long, contribution: Long): Long = {
    val sum = runningTotal + math.max(0L, contribution)
    if (sum < 0L) Long.MaxValue else sum
  }

  /**
   * Multiplies two non-negative values, saturating at `Long.MaxValue` rather than wrapping. Used by
   * the slowness comparison, where wrapping would turn a badly lagging consumer into an apparently
   * healthy one.
   */
  def saturatingMultiply(left: Long, right: Long): Long = {
    if (left <= 0L || right <= 0L) {
      0L
    } else if (left > Long.MaxValue / right) {
      Long.MaxValue
    } else {
      left * right
    }
  }

  /**
   * Expresses `part` as a percentage of `whole` without overflowing for any pair of non-negative
   * inputs. The straightforward product is used wherever it is provably safe and the divisor is
   * scaled down instead where it is not. A non-positive input answers zero, and the result is
   * deliberately not clamped at one hundred so that an over-budget reading stays visible.
   */
  def percentOf(part: Long, whole: Long): Long = {
    if (part <= 0L || whole <= 0L) {
      0L
    } else if (part <= Long.MaxValue / PERCENT_SCALE) {
      (part * PERCENT_SCALE) / whole
    } else {
      part / math.max(1L, whole / PERCENT_SCALE)
    }
  }

  /**
   * The credit ledger of one streaming shuffle stream: what has been sent, what has been
   * acknowledged, when the peer was last heard from, and whether the stream is currently held back.
   *
   * All state is atomic or concurrent and no method takes a lock, blocks or reads a clock. Time
   * always arrives as a parameter, supplied by the protocol from its injected clock, which is what
   * keeps a single notion of time in the subsystem and lets a suite freeze it.
   *
   * The unacknowledged window is a sorted map from a block's sequence number to its encoded size,
   * and it is the load-bearing structure of the zero-data-loss guarantee. It is exactly the set of
   * blocks whose bytes the producer must still be holding, so it defines both what a retransmission
   * request may ask for and what an acknowledgement is permitted to release. Its bound is the
   * credit limit rather than the size of the shuffle, so it cannot grow with the data.
   *
   * @param key identity of the stream this ledger belongs to
   * @param creditLimitBytes bytes the producer may hold unacknowledged at once
   * @param openedAtMillis instant the stream opened, from the protocol's clock, and the origin
   * every                       rate and every liveness interval is measured from
   */
  private[streaming] class StreamLedger(
      val key: BackpressureStreamKey,
      val creditLimitBytes: Long,
      openedAtMillis: Long) {

    // Sequence number to encoded size, for every block sent and not yet acknowledged. Sorted so
    // that an acknowledgement can drain the window from its low end without scanning it, and
    // concurrent so that a send on one thread and an acknowledgement on another need no lock.
    private val unacknowledged = new ConcurrentSkipListMap[java.lang.Long, java.lang.Long]()

    // Sum of the window's sizes, maintained alongside it so that the hot-path credit check is one
    // atomic read rather than a walk of the window.
    private val outstanding = new AtomicLong(0L)

    private val sentTotal = new AtomicLong(0L)

    private val acknowledgedTotal = new AtomicLong(0L)

    private val receivedTotal = new AtomicLong(0L)

    private val highestSent = new AtomicLong(NO_SEQUENCE)

    private val highestReceived = new AtomicLong(NO_SEQUENCE)

    private val ackedPosition = new AtomicLong(NOTHING_ACKNOWLEDGED)

    // Every instant below is a reading of the protocol's clock, never a value a peer sent, so no
    // liveness decision can be distorted by clock skew between two hosts.
    private val lastInboundMillis = new AtomicLong(openedAtMillis)

    private val lastAckMillis = new AtomicLong(openedAtMillis)

    private val lastHeartbeatReceivedMillis = new AtomicLong(openedAtMillis)

    private val lastHeartbeatSentMillis = new AtomicLong(openedAtMillis)

    // The one timestamp that does come from a peer. Diagnostic only: it is reported but never
    // compared against this executor's clock.
    private val remoteHeartbeatMillis = new AtomicLong(NO_TIMESTAMP)

    // Instant at which the most recent acknowledgement freed buffers, and therefore the origin of
    // the 100 ms reclamation bound. Cleared when the release is confirmed or the breach counted.
    private val reclamationOpenedMillis = new AtomicLong(NO_TIMESTAMP)

    private val lastReclamationLatency = new AtomicLong(NO_TIMESTAMP)

    // Instant at which the consumer was first observed to be lagging by the slowness ratio, or the
    // sentinel when it is not lagging now. Cleared on recovery, so the sustained window measures a
    // continuous condition rather than an accumulation of unrelated slow moments.
    private val slownessSinceMillis = new AtomicLong(NO_TIMESTAMP)

    private val retransmitAttemptCount = new AtomicInteger(0)

    private val throttled = new AtomicBoolean(false)

    private val throttleReported = new AtomicBoolean(false)

    private val terminated = new AtomicBoolean(false)

    private val announcedBlocks = new AtomicLong(NO_BLOCK_TOTAL)

    /** Bytes sent and not yet acknowledged. */
    def outstandingBytes: Long = outstanding.get()

    /** Bytes sent in total, replays included, which is the numerator of the producer's rate. */
    def sentBytes: Long = sentTotal.get()

    /** Bytes acknowledged in total, which is the numerator of the consumer's rate. */
    def acknowledgedBytes: Long = acknowledgedTotal.get()

    /** Bytes received in total, for a ledger held by the consuming side of a stream. */
    def receivedBytes: Long = receivedTotal.get()

    /** Credit still available, never negative. */
    def availableCredit: Long = math.max(0L, creditLimitBytes - outstanding.get())

    /** Whether any credit at all remains. */
    def hasCredit: Boolean = outstanding.get() < creditLimitBytes

    /**
     * Whether a block of this size fits in the credit that remains.
     *
     * A block larger than the entire allowance is admitted when the window is empty. Refusing it
     * would stall the stream permanently, because no acknowledgement can create room that the limit
     * does not contain, and holding exactly one oversized block is the smallest amount of progress
     * the stream can make. Guaranteeing liveness matters more here than holding the allowance to
     * the byte, and the framing layer caps a block at two megabytes in any case.
     */
    def hasCreditFor(bytes: Long): Boolean = {
      val used = outstanding.get()
      bytes <= math.max(0L, creditLimitBytes - used) || used <= 0L
    }

    /** Highest position the consumer has acknowledged, or the nothing-consumed sentinel. */
    def acknowledgedPosition: Long = ackedPosition.get()

    /** Highest sequence number this side has sent, or the sentinel when it has sent nothing. */
    def highestSentSequenceNumber: Long = highestSent.get()

    /** Highest sequence number this side has received, or the sentinel when it has none. */
    def highestReceivedSequenceNumber: Long = highestReceived.get()

    /**
     * The position a heartbeat for this stream should carry: how far this side has got, whether it
     * is producing or consuming. Never negative, so the heartbeat message type can never reject it.
     */
    def heartbeatSequenceNumber: Long =
      math.max(0L, math.max(highestSent.get(), highestReceived.get()))

    /** Blocks still in the window. */
    def unacknowledgedBlockCount: Int = unacknowledged.size()

    /** Whether one block is still in the window and can therefore still be replayed. */
    def containsUnacknowledged(sequenceNumber: Long): Boolean =
      unacknowledged.containsKey(java.lang.Long.valueOf(sequenceNumber))

    /** Inclusive bounds of the window, or `None` when nothing is outstanding. */
    def unacknowledgedWindow: Option[(Long, Long)] = {
      val first = unacknowledged.firstEntry()
      val last = unacknowledged.lastEntry()
      if (first == null || last == null) {
        None
      } else {
        Some((first.getKey.longValue(), last.getKey.longValue()))
      }
    }

    /**
     * Records that a block has been sent, charging it to the window and to the credit.
     *
     * Re-recording a block already in the window replaces its size rather than adding a second
     * entry, so a retransmission of a retained block cannot inflate the outstanding total and
     * cannot consume credit twice for the same bytes.
     */
    def recordSent(sequenceNumber: Long, bytes: Long): Unit = {
      val previous =
        unacknowledged.put(java.lang.Long.valueOf(sequenceNumber), java.lang.Long.valueOf(bytes))
      if (previous == null) {
        outstanding.addAndGet(bytes)
      } else {
        outstanding.addAndGet(bytes - previous.longValue())
      }
      sentTotal.addAndGet(bytes)
      advance(highestSent, sequenceNumber)
    }

    /**
     * Applies an acknowledgement, releasing every retained block at or below the acknowledged
     * position and returning the bytes that release freed.
     *
     * Monotonic: a position at or below the one already recorded releases nothing, so a duplicate
     * or a reordered acknowledgement delivered by a network thread is absorbed rather than allowed
     * to rewind the window. Any acknowledgement, even one that reports no progress at all, still
     * counts as proof that the consumer is alive.
     */
    def applyAck(consumerPosition: Long, nowMillis: Long): Long = {
      recordInbound(nowMillis)
      if (consumerPosition < 0L) {
        0L
      } else if (!advance(ackedPosition, consumerPosition)) {
        0L
      } else {
        val released = releaseThrough(consumerPosition)
        acknowledgedTotal.addAndGet(released)
        lastAckMillis.set(nowMillis)
        // Progress means the stream is healthy, so the replay budget is restored in full rather
        // than left partly spent for a later, unrelated failure to inherit.
        retransmitAttemptCount.set(0)
        if (released > 0L) {
          reclamationOpenedMillis.set(nowMillis)
        }
        released
      }
    }

    /** Confirms release of the buffers the last acknowledgement freed, reporting its latency. */
    def confirmReclamation(nowMillis: Long): Option[Long] = {
      val opened = reclamationOpenedMillis.getAndSet(NO_TIMESTAMP)
      if (opened == NO_TIMESTAMP) {
        None
      } else {
        val latency = math.max(0L, nowMillis - opened)
        lastReclamationLatency.set(latency)
        Some(latency)
      }
    }

    /** Whether a release is still awaited and the bound has already elapsed. */
    def isReclamationOverdue(nowMillis: Long, deadlineMs: Long): Boolean = {
      val opened = reclamationOpenedMillis.get()
      opened != NO_TIMESTAMP && nowMillis - opened > deadlineMs
    }

    /** Abandons the pending reclamation bound, after a breach has been counted for it. */
    def forgetReclamationDeadline(): Unit = reclamationOpenedMillis.set(NO_TIMESTAMP)

    /** Latency of the last confirmed reclamation, or `None` when none has been confirmed. */
    def lastReclamationLatencyMillis: Option[Long] = {
      val observed = lastReclamationLatency.get()
      if (observed == NO_TIMESTAMP) None else Some(observed)
    }

    /** Records inbound activity of any kind, which is what keeps a producer judged alive. */
    def recordInbound(nowMillis: Long): Unit = advance(lastInboundMillis, nowMillis)

    /** Records a received block: its volume, its position and the activity it evidences. */
    def recordReceived(sequenceNumber: Long, bytes: Long, nowMillis: Long): Unit = {
      if (bytes > 0L) {
        receivedTotal.addAndGet(bytes)
      }
      advance(highestReceived, sequenceNumber)
      recordInbound(nowMillis)
    }

    /**
     * Records a received heartbeat. `remoteTimestampMs` is retained for diagnostics only; liveness
     * is judged from `nowMillis`, which is this executor's own reading.
     */
    def recordHeartbeat(nowMillis: Long, remoteTimestampMs: Long): Unit = {
      advance(lastHeartbeatReceivedMillis, nowMillis)
      if (remoteTimestampMs != NO_TIMESTAMP) {
        remoteHeartbeatMillis.set(remoteTimestampMs)
      }
      recordInbound(nowMillis)
    }

    /** Records that a heartbeat has been emitted, restarting the interval. */
    def recordHeartbeatSent(nowMillis: Long): Unit = advance(lastHeartbeatSentMillis, nowMillis)

    /** Whether the heartbeat interval has elapsed since the last one was emitted. */
    def isHeartbeatDue(nowMillis: Long, intervalMs: Long): Boolean =
      nowMillis - lastHeartbeatSentMillis.get() >= intervalMs

    /** The timestamp the peer stamped into its last heartbeat, if it has sent one. */
    def remoteHeartbeatTimestamp: Option[Long] = {
      val observed = remoteHeartbeatMillis.get()
      if (observed == NO_TIMESTAMP) None else Some(observed)
    }

    /** Instant of the last received heartbeat, from this executor's clock. */
    def lastHeartbeatMillis: Long = lastHeartbeatReceivedMillis.get()

    /** Milliseconds since anything at all arrived on this stream. */
    def millisSinceInbound(nowMillis: Long): Long =
      math.max(0L, nowMillis - lastInboundMillis.get())

    /** Milliseconds since the consumer last advanced its position. */
    def millisSinceAck(nowMillis: Long): Long = math.max(0L, nowMillis - lastAckMillis.get())

    /** Records the orderly end of the stream, which retires both liveness timers. */
    def recordTermination(totalBlocks: Long, nowMillis: Long): Unit = {
      announcedBlocks.set(totalBlocks)
      terminated.set(true)
      recordInbound(nowMillis)
    }

    /** Whether the producer has announced the end of this stream. */
    def isTerminated: Boolean = terminated.get()

    /**
     * Blocks the producer announced, or `None` before it has announced anything. A returned zero is
     * a genuine reading: an empty reduce partition terminates with a total of zero.
     */
    def announcedBlockCount: Option[Long] =
      if (terminated.get()) Some(announcedBlocks.get()) else None

    /** Counts a replay attempt and reports which attempt it is, counted from one. */
    def recordRetransmitAttempt(): Int = retransmitAttemptCount.incrementAndGet()

    /** Replay attempts made since the stream last made progress. */
    def retransmitAttempts: Int = retransmitAttemptCount.get()

    /** Bytes per second this stream's producer has sustained since it opened. */
    def producerRate(nowMillis: Long): Long = ratePerSecond(sentTotal.get(), nowMillis)

    /** Bytes per second this stream's consumer has acknowledged since it opened. */
    def consumerRate(nowMillis: Long): Long = ratePerSecond(acknowledgedTotal.get(), nowMillis)

    /**
     * Whether the consumer is lagging its producer by at least `ratio`.
     *
     * Compared as totals rather than as rates, because both are measured over the same interval and
     * therefore share a denominator, which makes the comparison exact integer arithmetic. Only a
     * stream with bytes actually outstanding can be lagging: a consumer that has caught up entirely
     * is not slow, however little it has consumed in total.
     */
    def isConsumerSlow(ratio: Long): Boolean = {
      val sent = sentTotal.get()
      sent > 0L && outstanding.get() > 0L &&
        sent >= saturatingMultiply(acknowledgedTotal.get(), ratio)
    }

    /**
     * Re-observes the instantaneous lag, maintains the latch that records when it began, and
     * reports whether it has now persisted for longer than `windowMs`.
     *
     * Recovery clears the latch, so the window measures a continuous condition. A terminated stream
     * is never lagging, because its producer has finished.
     */
    def observeSlowness(nowMillis: Long, ratio: Long, windowMs: Long): Boolean = {
      if (terminated.get() || !isConsumerSlow(ratio)) {
        slownessSinceMillis.set(NO_TIMESTAMP)
        false
      } else {
        val since = slownessSinceMillis.get()
        if (since == NO_TIMESTAMP) {
          slownessSinceMillis.compareAndSet(NO_TIMESTAMP, nowMillis)
          false
        } else {
          nowMillis - since > windowMs
        }
      }
    }

    /** Instant at which the current episode of lag began, if one is in progress. */
    def slownessSince: Option[Long] = {
      val since = slownessSinceMillis.get()
      if (since == NO_TIMESTAMP) None else Some(since)
    }

    /** Whether this stream is currently held back. */
    def isThrottled: Boolean = throttled.get()

    /** Moves into the throttled state, reporting true only on the edge. */
    def enterThrottled(): Boolean = throttled.compareAndSet(false, true)

    /** Moves out of the throttled state, reporting true only on the edge. */
    def leaveThrottled(): Boolean = throttled.compareAndSet(true, false)

    /** Claims the single log line this stream is allowed for its throttling episodes. */
    def markThrottleReported(): Boolean = throttleReported.compareAndSet(false, true)

    override def toString: String = {
      s"StreamLedger($key, outstanding=${outstanding.get()}, credit=$creditLimitBytes, " +
        s"acked=${ackedPosition.get()}, throttled=${throttled.get()}, " +
        s"terminated=${terminated.get()})"
    }

    /**
     * Drains every retained block at or below `position`, returning the bytes freed.
     *
     * Lock-free and exact under concurrency. Each entry is removed by a two-argument remove that
     * succeeds for exactly one caller, and only the caller whose removal succeeded subtracts that
     * entry's bytes, so two acknowledgements racing on one stream drain disjoint parts of the
     * window and their subtractions sum to precisely the bytes removed. The loop retries rather
     * than assuming, which is why a lost race costs one extra iteration instead of a lost byte.
     */
    private def releaseThrough(position: Long): Long = {
      var released = 0L
      var draining = true
      while (draining) {
        val entry = unacknowledged.firstEntry()
        if (entry == null || entry.getKey.longValue() > position) {
          draining = false
        } else if (unacknowledged.remove(entry.getKey, entry.getValue)) {
          val bytes = entry.getValue.longValue()
          released += bytes
          outstanding.addAndGet(-bytes)
        }
      }
      released
    }

    /**
     * Raises a monotonic cell to `candidate`, reporting whether it moved. A compare-and-set loop
     * rather than a plain set, so that a stale value delivered out of order by a network thread can
     * never pull the cell backwards.
     */
    private def advance(cell: AtomicLong, candidate: Long): Boolean = {
      var current = cell.get()
      while (candidate > current) {
        if (cell.compareAndSet(current, candidate)) {
          return true
        }
        current = cell.get()
      }
      false
    }

    /** Bytes per second, from a total and the interval since the stream opened. */
    private def ratePerSecond(bytes: Long, nowMillis: Long): Long = {
      val elapsedMillis = nowMillis - openedAtMillis
      if (bytes <= 0L || elapsedMillis <= 0L) {
        0L
      } else {
        saturatingMultiply(bytes, MILLIS_PER_SECOND) / elapsedMillis
      }
    }
  }
}

