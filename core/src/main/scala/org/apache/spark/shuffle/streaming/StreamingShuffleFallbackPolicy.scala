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

/** Why a streaming shuffle stopped streaming, whatever the kind of reason. */
private[spark] sealed trait StreamingShuffleStandDownCause {

  /**
   * A one-clause, operator-facing rendering of this cause, phrased so that it composes into the
   * single warning that is emitted when a shuffle stands down.
   */
  def description: String
}

/** Why the streaming shuffle stepped aside in favour of sort-based shuffle. */
private[spark] sealed trait StreamingShuffleFallbackReason extends StreamingShuffleStandDownCause

/** The four fallback conditions, and nothing else. */
private[spark] object StreamingShuffleFallbackReason {

  /** The consumer of a shuffle has been unable to keep up with its producer. */
  case object ConsumerTooSlow extends StreamingShuffleFallbackReason {
    override val description: String =
      "a streaming shuffle consumer stayed at least 2x slower than its producer for more than 60 " +
        "seconds"
  }

  /**
   * A streaming shuffle buffer reservation could not be satisfied even after eviction, so
   * continuing to buffer would risk exhausting executor memory.
   */
  case object MemoryPressure extends StreamingShuffleFallbackReason {
    override val description: String =
      "memory pressure prevented a streaming shuffle buffer allocation, risking memory exhaustion"
  }

  /** Egress is consuming more of the administered link than the policy tolerates. */
  case object NetworkSaturation extends StreamingShuffleFallbackReason {
    override val description: String =
      "network utilisation exceeded 90% of the administered link capacity"
  }

  /** A peer announced a streaming shuffle protocol revision this build cannot speak. */
  case object ProtocolVersionMismatch extends StreamingShuffleFallbackReason {
    override val description: String =
      "a producer/consumer streaming shuffle protocol version mismatch was detected"
  }

  /** Every reason, in the order the specification enumerates them. */
  val all: Seq[StreamingShuffleFallbackReason] =
    Seq(ConsumerTooSlow, MemoryPressure, NetworkSaturation, ProtocolVersionMismatch)

  /**
   * Resolves a reason from its stable name, answering `None` for anything outside the closed set.
   *
   * @param name candidate reason name, which may be `null`
   * @return the matching reason, or `None` when this build does not know the name
   */
  def fromName(name: String): Option[StreamingShuffleFallbackReason] = {
    if (name == null) None else all.find(_.toString == name)
  }

  /**
   * The name a stand-down record carries when streaming was never available for a shuffle at all,
   * rather than when one of the four conditions was observed.
   */
  val UNAVAILABLE_NAME: String = "StreamingUnavailable"

  /** Prose rendered for [[UNAVAILABLE_NAME]] when a declaration carried no detail of its own. */
  val UNAVAILABLE_DESCRIPTION: String =
    "streaming shuffle was not available for this shuffle, so it stood down without any of the " +
      "four fallback conditions having been measured"

  /**
   * Whether a name may be recorded as a stand-down: any cause in the closed
   * [[StreamingShuffleStandDownCause]] set -- the four conditions or a structural decline -- or
   * [[UNAVAILABLE_NAME]].
   *
   * @param name candidate record name, which may be `null`
   * @return true when this build is willing to latch a stand-down under that name
   */
  def isDeclarable(name: String): Boolean =
    StreamingShuffleStandDownCause.fromName(name).isDefined || UNAVAILABLE_NAME == name
}

/**
 * The causes a shuffle can stand streaming down for: the four specified fallback conditions, plus
 * the structural declines that are not fallback conditions at all.
 */
private[spark] object StreamingShuffleStandDownCause {

  /** A structural decline: something the streaming shuffle protocol cannot serve. */
  private[spark] sealed trait StructuralDecline extends StreamingShuffleStandDownCause

  /** A consumer asked for a read shape the streaming protocol cannot serve. */
  case object UnsupportedReadShape extends StructuralDecline {
    override val description: String =
      "a streaming shuffle read shape the streaming protocol cannot serve was requested"
  }

  /** The rendezvous between the two ends of a shuffle could not be established. */
  case object ProducerUnavailable extends StructuralDecline {
    override val description: String =
      "a streaming shuffle producer and consumer could not establish a rendezvous"
  }

  val structuralDeclines: Seq[StructuralDecline] = Seq(UnsupportedReadShape, ProducerUnavailable)

  /**
   * Every cause a shuffle may stand streaming down for: the four specified fallback conditions
   * first, in the order the specification enumerates them, then the structural declines.
   */
  val all: Seq[StreamingShuffleStandDownCause] =
    StreamingShuffleFallbackReason.all ++ structuralDeclines

  /**
   * Resolves a cause from its stable name, answering `None` for anything outside the closed set.
   *
   * @param name candidate cause name, which may be `null`
   * @return the matching cause, or `None` when this build does not know the name
   */
  def fromName(name: String): Option[StreamingShuffleStandDownCause] = {
    if (name == null) None else all.find(_.toString == name)
  }
}

/**
 * Decides when the streaming shuffle must stop streaming and let sort-based shuffle take over.
 *
 * Four conditions trip it, and no others: a consumer sustained below the tolerated fraction of its
 * producer's rate for longer than the sustained-deficit window, a buffer reservation that cannot be
 * satisfied even after eviction, egress above the tolerated share of an administered link, and a
 * peer announcing a protocol revision this build cannot speak. A trip latches, so a shuffle that
 * has stood down never oscillates back into streaming, and it shares its terminus with the operator
 * kill switch `spark.shuffle.streaming.enabled=false`: both leave every service-provider call
 * delegated to the unmodified sort-based shuffle, so a job always has a working shuffle.
 *
 * This class only decides. Eviction belongs to the spill manager and delegation to the manager, so
 * nothing here both measures and reacts.
 *
 * @param conf the executor's configuration, read once here and never consulted again
 * @param clock the time source for trip timestamps and for the sampling overloads that do not
 *     take an explicit instant; injected so every trip instant is a function of this clock rather
 *     than of wall time, and so nothing in this class ever sleeps
 */
private[spark] class StreamingShuffleFallbackPolicy(
    conf: SparkConf,
    clock: Clock = new SystemClock)
  extends Logging {

  import StreamingShuffleFallbackPolicy._
  import StreamingShuffleFallbackReason._

  // Configuration is read exactly once, here, and held immutably for the life of the component.
  private val streamingEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_ENABLED)
  private val bufferSizePercent: Int = conf.get(config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)
  private val spillThresholdPercent: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)
  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  /**
   * The administered link capacity in bytes per second, or `None` when no capacity is administered.
   */
  private val administeredCapacityBytesPerSecond: Option[Long] =
    conf.get(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS).map(_.toLong * BYTES_PER_MIB)

  /** The latched reason, or `None` while the policy has not tripped. */
  private val trippedReasonRef =
    new AtomicReference[Option[StreamingShuffleFallbackReason]](None)

  private val trippedAtMs = new AtomicLong(UNSET_TIME_MS)

  /** How many trip conditions have been observed in total, including after latching. */
  private val observedTripConditions = new AtomicLong(0L)

  private val unevaluableSamples = new AtomicLong(0L)

  /**
   * The run of consecutive measurement intervals whose utilisation read above the tolerated share,
   * and the interval the most recent of them belonged to.
   *
   * Two fields rather than one, because a run has both a length and an end: the length is what
   * [[SATURATION_SUSTAINED_INTERVALS]] is compared against, and the end is what distinguishes the
   * next observation continuing the run from it starting a new one. See
   * [[recordSaturatedInterval]].
   */
  private val saturatedIntervals = new AtomicLong(0L)

  private val lastSaturatedInterval =
    new AtomicLong(BackpressureProtocol.NO_SATURATED_INTERVAL)

  /** How many samples named a shuffle past [[MAX_TRACKED_SHUFFLES]] and were therefore dropped. */
  private val untrackedShuffleSamples = new AtomicLong(0L)

  /** Per-shuffle throughput state, keyed by shuffle id. */
  private val throughputWindows = new ConcurrentHashMap[Int, ThroughputWindow]()

  /** Shuffles whose fallback this executor has already declared to the coordinator. */
  private val announcedShuffles: java.util.Set[Int] =
    ConcurrentHashMap.newKeySet[Int]()

  /** The shuffle-wide fallback verdict this executor knows about, keyed by shuffle id. */
  private val shuffleFallbacks = new ConcurrentHashMap[Int, StreamingShuffleFallbackState]()

  logConstruction()
  logSaturationCoverage()

  // The decision surface.

  /** Whether a fallback condition has been observed and latched. */
  def hasTripped: Boolean = trippedReasonRef.get().isDefined

  /** The reason the policy tripped, or `None` while it has not. */
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
   */
  def streamingEnabledByConfig: Boolean = streamingEnabled

  /**
   * Whether the operator kill switch is engaged, that is `spark.shuffle.streaming.enabled=false`.
   */
  def killSwitchEngaged: Boolean = !streamingEnabled

  /**
   * Whether the streaming path may handle work right now: enabled by configuration and not tripped.
   */
  def streamingActive: Boolean = streamingEnabled && trippedReasonRef.get().isEmpty

  /** Whether the caller must hand this call to the sort-based shuffle instead. */
  def shouldDelegateToSortShuffle: Boolean = !streamingActive

  /**
   * How many trip conditions have been observed in total, including those observed after the policy
   * had already latched.
   */
  def observedTripConditionCount: Long = observedTripConditions.get()

  /** How many samples were rejected as un-evaluable rather than acted upon. */
  def unevaluableSampleCount: Long = unevaluableSamples.get()

  /**
   * Consecutive measurement intervals whose utilisation read above the tolerated share, which is
   * what distinguishes one legal burst from a saturated link.
   *
   * Zero once a reading came back inside the share. Published so that a caller -- and a test -- can
   * tell "the link has brushed the share once" from "the link is being held above it", which
   * [[hasTripped]] cannot express until the run is long enough to trip.
   */
  def saturatedIntervalCount: Long = saturatedIntervals.get()

  /**
   * Consecutive over-share intervals a saturation of the ADMINISTERED capacity has to span before
   * this policy attributes it to the link rather than to the subsystem's own mandatory burst.
   *
   * The floor when no capacity is administered, because saturation is then unevaluable and no run
   * is ever counted. See [[StreamingShuffleFallbackPolicy.sustainedIntervalsFor]] for how it is
   * derived.
   */
  def administeredSustainedIntervals: Long = {
    administeredCapacityBytesPerSecond
      .map(capacity => sustainedIntervalsFor(capacity.toDouble))
      .getOrElse(SATURATION_SUSTAINED_INTERVALS)
  }

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
   * @param shuffleId the shuffle the sample belongs to
   * @param bytesPerSecond the producer's measured rate, in bytes per second
   * @param nowMs the instant of the measurement, in milliseconds
   */
  def recordProducerThroughput(shuffleId: Int, bytesPerSecond: Double, nowMs: Long): Unit = {
    recordThroughput(shuffleId, bytesPerSecond, nowMs, fromProducer = true)
  }

  /** Records a producer rate sample taken now, as reported by the injected clock. */
  def recordProducerThroughput(shuffleId: Int, bytesPerSecond: Double): Unit = {
    recordThroughput(shuffleId, bytesPerSecond, clock.getTimeMillis(), fromProducer = true)
  }

  /**
   * Records the rate at which a shuffle's consumer is draining bytes.
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
   */
  def slownessArmedSinceMs(shuffleId: Int): Option[Long] = {
    val window = throughputWindows.get(shuffleId)
    if (window == null) None else window.armedSinceMs
  }

  /** Applies one throughput sample and escalates if it completes a sustained deficit. */
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
   * Records observed egress against an explicit link capacity, and trips once utilisation
   * '''strictly above''' the tolerated share has held across
   * [[StreamingShuffleFallbackPolicy.SATURATION_SUSTAINED_INTERVALS]] consecutive measurement
   * intervals.
   *
   * The specification states the condition as network saturation exceeding
   * [[StreamingShuffleFallbackPolicy.SATURATION_TRIP_PERCENT]] of the link capacity. What it does
   * not say, and what only the implementation can know, is that this subsystem is '''required''' to
   * emit traffic which reads above that share on one interval of a small administered link: a
   * pacing bucket must be able to admit one maximum-sized encoded frame or it would refuse every
   * frame forever, so its burst allowance is at least one frame however small its paced share is,
   * and a bucket that starts full delivers that burst plus one interval's refill inside the first
   * interval. Measured against a link declared at a few MB/s that is one interval at half again the
   * capacity -- on a link carrying nothing but correctly paced streaming traffic. Tripping on it
   * invalidated every producer of a healthy shuffle and forced its map stage to be recomputed.
   *
   * A run of intervals is therefore what the condition is evaluated over, and the run is what makes
   * the reading evidence rather than an artefact. Steady-state egress is held to
   * [[TokenBucketRateLimiter.BANDWIDTH_CEILING_PERCENT]] of the administered capacity, which is
   * below the trip share by construction, so a burst clears on the very next interval while a link
   * that really is saturated stays over capacity interval after interval and trips within the run
   * [[StreamingShuffleFallbackPolicy.sustainedIntervalsFor]] derives for that capacity -- at worst
   * a minute, so never slower than the sustained-slowness condition beside it.
   *
   * Exactly at the threshold is not saturation. The specification tolerates utilisation up to the
   * threshold and trips past it, so the comparison is `>` and never `>=`, and a reading at or below
   * the threshold ends any run in progress.
   *
   * Division is guarded rather than attempted: a capacity that is zero, negative or not a finite
   * number is not a capacity, and a link of unknown capacity cannot be described as saturated. Such
   * a sample is counted as un-evaluable and never trips, which is also why this method can never
   * divide by zero and can never produce a NaN comparison. An un-evaluable sample is not evidence
   * at all, so it neither extends a run nor ends one.
   *
   * The sample is attributed to the instant the injected clock reports. Use the three-argument form
   * when the caller already holds the instant its measurement belongs to.
   *
   * @param usedBytesPerSecond observed egress, in bytes per second
   * @param capacityBytesPerSecond the link capacity the observation is measured against, in
   *     bytes per second
   */
  def recordLinkUtilization(usedBytesPerSecond: Double, capacityBytesPerSecond: Double): Unit = {
    recordLinkUtilization(usedBytesPerSecond, capacityBytesPerSecond, clock.getTimeMillis())
  }

  /**
   * Records observed egress against an explicit link capacity, at an explicit instant.
   *
   * @param usedBytesPerSecond observed egress, in bytes per second
   * @param capacityBytesPerSecond the link capacity the observation is measured against, in
   *     bytes per second
   * @param sampleTimeMillis the instant the measurement belongs to, in milliseconds; accepted
   *     for uniformity with the sustained recorders and deliberately not consulted by this
   *     instantaneous rule
   */
  def recordLinkUtilization(
      usedBytesPerSecond: Double,
      capacityBytesPerSecond: Double,
      sampleTimeMillis: Long): Unit = {
    if (!isMeasurable(usedBytesPerSecond) || !isMeasurable(capacityBytesPerSecond) ||
        capacityBytesPerSecond <= 0.0d) {
      unevaluableSamples.incrementAndGet()
    } else {
      val utilization = usedBytesPerSecond / capacityBytesPerSecond
      if (utilization <= SATURATION_TRIP_RATIO) {
        // Inside the tolerated share, so whatever run was in progress ends here. This is the branch
        // a correctly paced link takes, because pacing holds egress to eighty percent of the
        // administered capacity while the trip share is ninety.
        clearSaturatedIntervals()
        if (debugEnabled) {
          logDebug(log"Streaming shuffle egress read " +
            log"${MDC(PERCENT, math.round(utilization * PERCENT_SCALE))}% of the administered " +
            log"link capacity, inside the ${MDC(THRESHOLD, SATURATION_TRIP_PERCENT)}% this " +
            log"policy tolerates")
        }
      } else {
        val consecutive = recordSaturatedInterval(sampleTimeMillis)
        val required = sustainedIntervalsFor(capacityBytesPerSecond)
        if (consecutive >= required) {
          trip(NetworkSaturation,
            log"streaming shuffle egress reached " +
              log"${MDC(PERCENT, math.round(utilization * PERCENT_SCALE))}% of the administered " +
              log"link capacity, ${MDC(NUM_BYTES, usedBytesPerSecond)} bytes/s against " +
              log"${MDC(MAX_SIZE, capacityBytesPerSecond)} bytes/s, above the " +
              log"${MDC(THRESHOLD, SATURATION_TRIP_PERCENT)}% this policy tolerates, across " +
              log"${MDC(COUNT, consecutive)} consecutive measurement interval(s), which is more " +
              log"than the mandatory burst allowance of this capacity can account for.")
        } else if (debugEnabled) {
          logDebug(log"Streaming shuffle egress read " +
            log"${MDC(PERCENT, math.round(utilization * PERCENT_SCALE))}% of the administered " +
            log"link capacity across ${MDC(COUNT, consecutive)} consecutive measurement " +
            log"interval(s), short of the ${MDC(MAX_SIZE, required)} this capacity's burst " +
            log"allowance has to be amortised into before saturation can be concluded")
        }
      }
    }
  }

  /**
   * Counts one over-capacity observation into the run of saturated measurement intervals, and
   * reports how many consecutive intervals the run now spans.
   *
   * Intervals are identified by quantising the observation's instant onto
   * [[BackpressureProtocol.SATURATION_SAMPLE_WINDOW_MS]], so several observations of the same
   * published rate -- which is what a hundred-millisecond poll of a rate republished once a second
   * produces -- count once between them. An observation in the interval immediately after the run's
   * end extends it; a gap means at least one interval was not saturated, so the run restarts at
   * this one.
   *
   * @param sampleTimeMillis the instant the observation belongs to, in milliseconds
   * @return consecutive saturated intervals observed, at least one
   */
  private def recordSaturatedInterval(sampleTimeMillis: Long): Long = {
    val interval = sampleTimeMillis / BackpressureProtocol.SATURATION_SAMPLE_WINDOW_MS
    val previous = lastSaturatedInterval.getAndSet(interval)
    if (previous == interval) {
      // Already counted, so the standing length is reported rather than advanced. Without this a
      // fast poll would reach any threshold inside one interval, which is precisely how a sustained
      // rule came to stand streaming down on a single legal burst.
      math.max(1L, saturatedIntervals.get())
    } else if (previous == interval - 1L) {
      saturatedIntervals.incrementAndGet()
    } else {
      saturatedIntervals.set(1L)
      1L
    }
  }

  /** Ends any run of saturated intervals, because this reading was inside the tolerated share. */
  private def clearSaturatedIntervals(): Unit = {
    saturatedIntervals.set(0L)
    lastSaturatedInterval.set(BackpressureProtocol.NO_SATURATED_INTERVAL)
  }

  /**
   * Records observed egress against the capacity the operator administered through
   * `spark.shuffle.streaming.maxBandwidthMBps`.
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
   * @param peerVersion the protocol version the peer stamped into its header
   * @return true when the version is one this build can decode, false when the policy has
   *     tripped
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
   * @param framedMessage a framed message, that is the type byte followed by the encoded body
   * @return true when the peer's version is one this build can decode
   */
  def checkProtocolVersion(framedMessage: ByteBuffer): Boolean = {
    checkProtocolVersion(StreamingShuffleMessage.peekProtocolVersion(framedMessage))
  }

  /**
   * Claims the right to declare one shuffle's fallback to the coordinator, once.
   *
   * @param shuffleId the shuffle whose fallback the caller intends to declare
   * @return `true` when the caller must perform the declaration, `false` when it is already
   *     done or not yet warranted
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

  /** Whether a shuffle is known to have stood streaming down for every participant. */
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
   * @param shuffleId the shuffle whose sampling state should be released
   */
  def forgetShuffle(shuffleId: Int): Unit = {
    val removed = throughputWindows.remove(shuffleId)
    // The announcement slot is released with the sampling state, so that the bound tracks live
    // shuffles rather than every shuffle the executor has ever served.
    announcedShuffles.remove(shuffleId)
    shuffleFallbacks.remove(shuffleId)
    if (removed != null && debugEnabled) {
      logInfo(log"Released streaming shuffle fallback sampling state for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)}")
    }
  }

  /** Restores the untripped state and discards all sampling state. */
  def reset(): Unit = {
    trippedReasonRef.set(None)
    trippedAtMs.set(UNSET_TIME_MS)
    observedTripConditions.set(0L)
    unevaluableSamples.set(0L)
    untrackedShuffleSamples.set(0L)
    // The saturation run is sampling state like the throughput windows below it: a run left
    // standing across a reset described a link the next workload never used.
    clearSaturatedIntervals()
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

  // The single terminus.

  /**
   * Latches the first fallback condition observed and reports it once.
   *
   * @param reason the condition observed
   * @param detail the measurements behind it, evaluated only when it is actually going to be
   *     logged
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

  /** Whether a sampled quantity can be reasoned about at all: a finite, non-negative number. */
  private def isMeasurable(value: Double): Boolean =
    java.lang.Double.isFinite(value) && value >= 0.0d

  /** Records how this instance was configured, once, at debug level. */
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
        log"${MDC(MAX_SIZE, capacity)} across " +
        log"${MDC(COUNT, administeredSustainedIntervals)} consecutive measurement interval(s), " +
        log"and on any protocol version other than " +
        log"${MDC(VERSION_NUM, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)}")
    }
  }

  /**
   * Tells the operator, once, when one of the four fallback conditions cannot be evaluated at all.
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

  /** One shuffle's producer and consumer rates, plus how long its consumer has been behind. */
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

    /** Accepts a measurable rate, or records the sample as un-evaluable and forgets the reading. */
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
   * @param producerBytesPerSecond the producer rate in force, or `UNSET_RATE` if unknown
   * @param consumerBytesPerSecond the consumer rate in force, or `UNSET_RATE` if unknown
   * @param sustainedMs how long the deficit has held, or `NOT_SUSTAINED` if it has not held
   *     long enough to trip
   */
  private case class SlownessSample(
      producerBytesPerSecond: Double,
      consumerBytesPerSecond: Double,
      sustainedMs: Long)
}

/** The numeric contracts of the fallback policy. */
private[spark] object StreamingShuffleFallbackPolicy {

  /**
   * How far behind the consumer must be for the deficit to count: the producer must be running at
   * least this many times faster.
   */
  val CONSUMER_SLOWNESS_RATIO: Double = 2.0d

  /**
   * How long the deficit must hold, continuously, before the policy trips: strictly longer than
   * this many milliseconds.
   */
  val SUSTAINED_SLOWNESS_WINDOW_MS: Long = 60000L

  /**
   * The share of the administered link, as a percentage, above which the link counts as saturated.
   */
  val SATURATION_TRIP_PERCENT: Long = 90L

  /** [[SATURATION_TRIP_PERCENT]] as a fraction, which is the form the comparison actually uses. */
  val SATURATION_TRIP_RATIO: Double = SATURATION_TRIP_PERCENT.toDouble / 100.0d

  /**
   * The '''fewest''' consecutive measurement intervals a link must read above the tolerated share
   * in before its saturation can be treated as sustained.
   *
   * Three, which at the protocol's one-second measurement window is three seconds. A floor rather
   * than the rule: the run actually required is derived from the administered capacity by
   * [[sustainedIntervalsFor]], and is never shorter than this. Deliberately kept in step with
   * `BackpressureProtocol.LINK_SATURATION_SUSTAINED_INTERVALS`, which applies the same rule to the
   * protocol's own view of the same condition.
   */
  val SATURATION_SUSTAINED_INTERVALS: Long = 3L

  /**
   * The '''most''' consecutive intervals a run is ever required to span.
   *
   * Sixty, which at a one-second window is one minute -- the same order as the sustained-slowness
   * window beside it, so no degradation condition is ever slower to be recognised than that one. It
   * binds only for a capacity so small that the mandatory burst is tens of seconds of traffic at
   * the paced rate, which `spark.shuffle.streaming.maxBandwidthMBps` cannot express because its
   * unit is MB/s; it exists so an explicitly supplied capacity cannot make the condition
   * unreachable.
   */
  val SATURATION_MAX_SUSTAINED_INTERVALS: Long = 60L

  /**
   * The share of the administered capacity that separates compliant pacing from saturation.
   *
   * Ten percent, and derived rather than written: it is the gap between the share this policy calls
   * saturation and the share the egress limiter holds itself to. That gap is the whole of the
   * headroom a compliant producer has, and it is what the mandatory burst has to be amortised into.
   */
  val SATURATION_HEADROOM_RATIO: Double =
    SATURATION_TRIP_RATIO -
      TokenBucketRateLimiter.BANDWIDTH_CEILING_PERCENT.toDouble /
        TokenBucketRateLimiter.PERCENT_SCALE.toDouble

  /**
   * How many consecutive over-share intervals an administered capacity's saturation must span
   * before it can be attributed to the link rather than to this subsystem's own mandatory burst.
   *
   * <b>Why this is derived and not a constant.</b> A token bucket has to be able to admit one
   * maximum-sized encoded frame -- a bucket that could not hold one would refuse every frame
   * forever, which is a deadlock and not a rate limit -- so its burst allowance is at least one
   * frame however small its paced share is. A bucket that starts full therefore delivers, inside
   * one measurement interval, its burst plus that interval's refill. Compliant pacing already
   * occupies [[TokenBucketRateLimiter.BANDWIDTH_CEILING_PERCENT]] of the capacity, so the burst
   * has only [[SATURATION_HEADROOM_RATIO]] of it to fit into before the reading crosses the trip
   * share: the run must be long enough for `burst / (capacity * headroom)` intervals, or a
   * producer doing exactly what it was configured to do reads as a saturated link.
   *
   * At a one MB/s capacity that is twenty intervals, because one two-mebibyte frame is two and a
   * half seconds of traffic at the paced rate. At any capacity large enough for the burst to be one
   * second of its own paced rate it settles at eight, which is where every realistic configuration
   * lands. Both are bounded below by [[SATURATION_SUSTAINED_INTERVALS]] and above by
   * [[SATURATION_MAX_SUSTAINED_INTERVALS]], so the condition is neither instantaneous nor
   * unreachable, and a link that genuinely stays above the share stands streaming down inside a
   * minute at worst -- against a sustained-slowness condition that takes a minute by specification.
   *
   * A capacity that cannot be reasoned about yields the floor, because such a sample is refused
   * before it is ever compared and the value is then never used.
   *
   * @param capacityBytesPerSecond the administered capacity the observation is measured against
   * @return intervals the run must span, at least [[SATURATION_SUSTAINED_INTERVALS]]
   */
  def sustainedIntervalsFor(capacityBytesPerSecond: Double): Long = {
    if (!java.lang.Double.isFinite(capacityBytesPerSecond) || capacityBytesPerSecond <= 0.0d) {
      SATURATION_SUSTAINED_INTERVALS
    } else {
      val pacedBytesPerSecond = TokenBucketRateLimiter.applyLinkCapacityCeiling(
        math.max(1L, capacityBytesPerSecond.toLong))
      val burstBytes = TokenBucketRateLimiter.burstCapacityBytes(pacedBytesPerSecond).toDouble
      val amortising = math.ceil(burstBytes / (capacityBytesPerSecond * SATURATION_HEADROOM_RATIO))
      val bounded =
        if (amortising >= SATURATION_MAX_SUSTAINED_INTERVALS.toDouble) {
          SATURATION_MAX_SUSTAINED_INTERVALS
        } else {
          amortising.toLong
        }
      math.max(SATURATION_SUSTAINED_INTERVALS, bounded)
    }
  }

  /**
   * Whether the notice about an unevaluable saturation condition has already been emitted.
   *
   * Process wide rather than per instance, so a JVM that constructs more than one policy states the
   * fact once rather than once per construction.
   */
  private[streaming] val saturationNoticeEmitted = new AtomicBoolean(false)

  /**
   * Bytes in one MiB, used to convert the administered bandwidth from MB/s into bytes per second.
   */
  val BYTES_PER_MIB: Long = 1024L * 1024L

  /** The greatest number of shuffles this policy will hold throughput state for at once. */
  val MAX_TRACKED_SHUFFLES: Int = 1024

  /** Scale factor for rendering a fraction as a percentage in an operator-facing message. */
  private val PERCENT_SCALE: Double = 100.0d

  /** The rate held for a direction that has never reported a measurable sample. */
  private val UNSET_RATE: Double = -1.0d

  private val UNSET_TIME_MS: Long = -1L

  /** The sustained duration reported when the deficit has not held long enough to trip. */
  private val NOT_SUSTAINED: Long = -1L
}
