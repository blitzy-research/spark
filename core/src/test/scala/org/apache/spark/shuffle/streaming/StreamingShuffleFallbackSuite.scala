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
import java.util.Locale

import scala.collection.mutable

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.LogEvent
import org.mockito.Mockito.mock

import org.apache.spark.{FetchFailed, LocalSparkContext, ShuffleDependency, SparkConf, SparkContext,
  SparkEnv, SparkFunSuite, SparkThrowableHelper, TaskFailedReason}
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT,
  SHUFFLE_STREAMING_ENABLED, SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS,
  SHUFFLE_STREAMING_SPILL_THRESHOLD, STAGE_MAX_CONSECUTIVE_ATTEMPTS, TASK_MAX_FAILURES}
import org.apache.spark.network.shuffle.protocol.streaming.StreamingShuffleMessage
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.ManualClock

/**
 * Tests the streaming shuffle's graceful-degradation contract: the four conditions on which it
 * stands down, the operator kill switch, and the one property that makes every path safe.
 */
class StreamingShuffleFallbackSuite
  extends SparkFunSuite
  with LocalSparkContext
  with StreamingShuffleTestHelper
  with StreamingShuffleHadoopCredentialIsolation {

  import StreamingShuffleFallbackReason._
  import StreamingShuffleTestHelper._

  private val ShuffleId: Int = 0

  private val OtherShuffleId: Int = 1

  private val MapId: Long = 0L

  private val PartitionId: Int = 0

  private val PartitionCount: Int = DefaultPartitionCount

  private val ProducerCount: Int = 6

  private val ListenerDrainTimeoutMillis: Long = 60000L

  /** A producer rate that is unambiguously producing. */
  private val ProducerBytesPerSecond: Double = 1024.0d

  /** A consumer rate exactly the tolerated factor behind the producer. */
  private val LaggingConsumerBytesPerSecond: Double =
    ProducerBytesPerSecond / ConsumerSlownessRatio

  private val LinkCapacityBytesPerSecond: Double = 100.0d

  private val EgressAtSaturationThreshold: Double = LinkSaturationTripPercent.toDouble

  private val SaturationSampleRepetitions: Long = 4L

  private val RequestedBufferBytes: Long = 1024L

  private val AdministeredBandwidthMbps: Int = 100

  private val IncompatibleProtocolVersion: Byte =
    (StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION + 1).toByte

  private val FramePaddingBytes: Int = 3

  private val ConcurrentReaderCount: Int = 8

  private val LatchReadIterations: Int = 20000

  private def activePolicy(clock: ManualClock): StreamingShuffleFallbackPolicy = {
    new StreamingShuffleFallbackPolicy(streamingConf(), clock)
  }

  private def withCoordinator(conf: SparkConf)(body: StreamingShuffleCoordinator => Unit): Unit = {
    val coordinator = new StreamingShuffleCoordinator(mock(classOf[RpcEnv]), conf, newManualClock())
    try {
      body(coordinator)
    } finally {
      coordinator.onStop()
    }
  }

  private def registerShuffleFor(
      coordinator: StreamingShuffleCoordinator,
      shuffleId: Int,
      numMaps: Int = ProducerCount): String = {
    val grant = coordinator.registerShuffle(shuffleId, PartitionCount, numMaps, ProtocolVersion)
    assert(grant.isDefined, s"shuffle $shuffleId must have been registered")
    assert(grant.get.capabilityToken.nonEmpty, "the grant must carry a non-empty token")
    grant.get.capabilityToken
  }

  private def registerProducerFor(
      coordinator: StreamingShuffleCoordinator,
      shuffleId: Int,
      token: String,
      mapIndex: Int,
      attemptId: Long): Unit = {
    val host = s"host-$mapIndex"
    val location = StreamingShuffleProducerLocation(s"exec-$mapIndex", host, 7337 + mapIndex,
      mapId = attemptId, mapIndex = mapIndex, taskAttemptId = attemptId,
      BlockManagerId(s"exec-$mapIndex", host, 7337 + mapIndex))
    assert(coordinator.registerProducer(shuffleId, token, location, PartitionCount,
        ProtocolVersion).accepted,
      s"the producer of map output $mapIndex attempt $attemptId must have been registered")
  }

  private def timeOutProducer(
      coordinator: StreamingShuffleCoordinator,
      shuffleId: Int,
      token: String,
      mapIndex: Int,
      attemptId: Long,
      register: Boolean = true): Long = {
    if (register) {
      registerProducerFor(coordinator, shuffleId, token, mapIndex, attemptId)
    }
    coordinator.invalidateProducer(shuffleId, token,
      StreamingShuffleProducerGeneration(mapIndex = mapIndex, mapId = attemptId,
        taskAttemptId = attemptId),
      StreamingShuffleInvalidationReason.ConnectionTimeout,
      s"partition 0 received nothing for " +
        s"${StreamingShuffleCoordinator.PRODUCER_CONNECTION_TIMEOUT_MS + 14} ms")
  }

  /**
   * Arms the sustained-slowness timer for `ShuffleId` at the clock's current reading.
   *
   * @param clock the clock whose current reading becomes the arming instant
   */
  private def armConsumerSlowness(
      policy: StreamingShuffleFallbackPolicy,
      clock: ManualClock): Long = {
    val armedAt = clock.getTimeMillis()
    policy.recordProducerThroughput(ShuffleId, ProducerBytesPerSecond, armedAt)
    policy.recordConsumerThroughput(ShuffleId, LaggingConsumerBytesPerSecond, armedAt)
    armedAt
  }

  /**
   * Reports the deficit again at the clock's current reading, without disturbing the arming
   * instant.
   *
   * @param clock the clock whose current reading becomes the sample instant
   */
  private def resampleConsumerSlowness(
      policy: StreamingShuffleFallbackPolicy,
      clock: ManualClock): Unit = {
    policy.recordConsumerThroughput(ShuffleId, LaggingConsumerBytesPerSecond, clock.getTimeMillis())
  }

  private def driveSustainedConsumerSlowness(
      policy: StreamingShuffleFallbackPolicy,
      clock: ManualClock): Unit = {
    armConsumerSlowness(policy, clock)
    advancePastSustainedSlownessWindow(clock)
    resampleConsumerSlowness(policy, clock)
  }

  private def framedMessage(): ByteBuffer = {
    ack(ShuffleId, MapId, PartitionId, NothingConsumedPosition).toByteBuffer()
  }

  private def framedMessageWithVersion(version: Byte): ByteBuffer = {
    val framed = framedMessage()
    val bytes = new Array[Byte](framed.remaining())
    framed.get(bytes)
    bytes(FrameTypePrefixLength) = version
    ByteBuffer.wrap(bytes)
  }

  private def paddedFramedMessage(paddingBytes: Int): ByteBuffer = {
    val framed = framedMessage()
    val padded = ByteBuffer.allocate(paddingBytes + framed.remaining())
    padded.put(new Array[Byte](paddingBytes))
    padded.put(framed)
    padded.flip()
    padded.position(paddingBytes)
    padded
  }

  private def saturationNotices(appender: LogAppender): Seq[LogEvent] = {
    appender.loggingEvents.filter { event =>
      val text = event.getMessage.getFormattedMessage
      text.contains(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS.key) && text.contains("saturation")
    }.toSeq
  }

  test("consumer slowness does not trip before the sustained window has been exceeded") {
    val clock = newManualClock()
    val policy = activePolicy(clock)
    val armedAt = armConsumerSlowness(policy, clock)

    assert(policy.slownessArmedSinceMs(ShuffleId).contains(armedAt),
      "the first sample that observes the deficit must arm the window at that instant")
    assert(!policy.hasTripped, "arming the window is not itself a trip")
    assert(policy.trackedShuffleCount == 1,
      "sampling one shuffle must create throughput state for exactly that shuffle")

    advanceJustBeforeSustainedSlownessWindow(clock)
    resampleConsumerSlowness(policy, clock)
    assert(clock.getTimeMillis() - armedAt == JustBeforeSustainedSlownessMillis,
      "the fixture must place the sample exactly one millisecond short of the window")
    assert(!policy.hasTripped,
      "a deficit held for less than the window must not trip; 59999 ms is not more than 60000 ms")

    clock.advance(1L)
    resampleConsumerSlowness(policy, clock)
    assert(clock.getTimeMillis() - armedAt == SustainedSlownessWindowMillis,
      "the fixture must place the sample exactly at the window boundary")
    assert(!policy.hasTripped,
      "a deficit held for exactly the window must not trip; the comparison is strictly greater")

    assert(policy.trippedReason.isEmpty,
      "no reason may be reported while the policy has not tripped")
    assert(policy.trippedAtTimeMillis.isEmpty, "no trip instant may be stamped before a trip")
    assert(policy.streamingActive, "streaming must remain active while no condition has completed")
    assert(!policy.shouldDelegateToSortShuffle,
      "an untripped, enabled policy must keep serving the streaming path")
    assert(policy.slownessArmedSinceMs(ShuffleId).contains(armedAt),
      "a deficit that has held throughout must keep its original arming instant")
  }

  test("consumer slowness trips once the sustained window is exceeded") {
    val clock = newManualClock()
    val policy = activePolicy(clock)
    val armedAt = armConsumerSlowness(policy, clock)

    advancePastSustainedSlownessWindow(clock)
    val trippedAt = clock.getTimeMillis()
    assert(trippedAt - armedAt == SustainedSlownessTripMillis,
      "the fixture must place the sample exactly one millisecond past the window")
    resampleConsumerSlowness(policy, clock)

    assert(policy.hasTripped,
      "a deficit held for longer than the window must trip; 60001 ms is more than 60000 ms")
    assert(policy.trippedReason.contains(ConsumerTooSlow),
      s"the reported reason must be ConsumerTooSlow but was ${policy.trippedReason}")
    assert(policy.trippedAtTimeMillis.contains(trippedAt),
      "the trip instant must come from the injected clock rather than from wall time")
    assert(policy.observedTripConditionCount == 1L,
      "exactly one condition was driven, so exactly one must have been observed")
    assert(!policy.streamingActive, "a tripped policy must not report streaming as active")
    assert(policy.shouldDelegateToSortShuffle,
      "consumer slowness must route to the sort-based shuffle, which is the only terminus")
    assert(!policy.killSwitchEngaged,
      "a trip must be distinguishable from an operator turning streaming off")
  }

  test("a consumer that recovers clears the sustained slowness timer") {
    val clock = newManualClock()
    val policy = activePolicy(clock)
    val armedAt = armConsumerSlowness(policy, clock)
    assert(policy.slownessArmedSinceMs(ShuffleId).contains(armedAt), "the window must arm first")

    clock.advance(SustainedSlownessWindowMillis / 2L)
    policy.recordConsumerThroughput(ShuffleId, ProducerBytesPerSecond, clock.getTimeMillis())
    assert(policy.slownessArmedSinceMs(ShuffleId).isEmpty,
      "a consumer that keeps up must clear the sustained-slowness timer")
    assert(!policy.hasTripped, "recovery must not trip anything")

    advancePastSustainedSlownessWindow(clock)
    val rearmedAt = clock.getTimeMillis()
    resampleConsumerSlowness(policy, clock)
    assert(policy.slownessArmedSinceMs(ShuffleId).contains(rearmedAt),
      "a deficit that reappears must re-arm at the instant it reappeared")
    assert(!policy.hasTripped,
      "time that elapsed before the consumer recovered must not count towards a later deficit")

    advancePastSustainedSlownessWindow(clock)
    resampleConsumerSlowness(policy, clock)
    assert(policy.hasTripped, "the re-armed window must trip once it is exceeded in its own right")
    assert(policy.trippedReason.contains(ConsumerTooSlow),
      s"the reported reason must be ConsumerTooSlow but was ${policy.trippedReason}")
    assert(policy.shouldDelegateToSortShuffle, "the terminus is the same for a re-armed window")
  }

  test("an idle shuffle and an unmeasurable rate never trip consumer slowness") {
    val clock = newManualClock()
    val policy = activePolicy(clock)

    policy.recordProducerThroughput(ShuffleId, 0.0d, clock.getTimeMillis())
    policy.recordConsumerThroughput(ShuffleId, 0.0d, clock.getTimeMillis())
    assert(policy.slownessArmedSinceMs(ShuffleId).isEmpty,
      "an idle shuffle must not arm the sustained-slowness window")
    advancePastSustainedSlownessWindow(clock)
    policy.recordConsumerThroughput(ShuffleId, 0.0d, clock.getTimeMillis())
    assert(!policy.hasTripped, "an idle shuffle must never trip consumer slowness")

    val unevaluableBefore = policy.unevaluableSampleCount
    policy.recordProducerThroughput(OtherShuffleId, Double.NaN, clock.getTimeMillis())
    policy.recordConsumerThroughput(OtherShuffleId, Double.PositiveInfinity, clock.getTimeMillis())
    policy.recordProducerThroughput(OtherShuffleId, -1.0d, clock.getTimeMillis())
    assert(policy.unevaluableSampleCount == unevaluableBefore + 3L,
      "every unmeasurable rate must be counted as un-evaluable rather than silently dropped")
    assert(!policy.hasTripped, "an un-evaluable sample must never trip")
    assert(policy.streamingActive, "streaming must survive samples that cannot be reasoned about")
    assert(policy.trackedShuffleCount == 2,
      "sampling two shuffles must hold their throughput state independently")
  }

  test("losing the same map output repeatedly is recovered by recomputation, never by standing " +
      "the shuffle down") {
    withCoordinator(streamingConf()) { coordinator =>
      val shuffleId = 7101
      val token = registerShuffleFor(coordinator, shuffleId)

      val original = timeOutProducer(coordinator, shuffleId, token, mapIndex = 0, attemptId = 100L)
      assert(original !== StreamingShuffleCoordinator.NO_EPOCH,
        "the invalidation must have withdrawn the registration and advanced the epoch")
      assert(coordinator.producerTimeoutCount(shuffleId, 0) === 1,
        s"one loss must be recorded but ${coordinator.producerTimeoutCount(shuffleId, 0)} were")
      assert(!coordinator.fallbackStateFor(shuffleId, token).fallenBack,
        "a producer loss is recovered by recomputation, never by standing the shuffle down")

      timeOutProducer(coordinator, shuffleId, token, mapIndex = 0, attemptId = 100L,
        register = false)
      assert(coordinator.producerTimeoutCount(shuffleId, 0) === 1,
        "re-invalidating a generation already gone must not be counted a second time")

      (101L to 108L).foreach { attemptId =>
        timeOutProducer(coordinator, shuffleId, token, mapIndex = 0, attemptId)
        val declared = coordinator.fallbackStateFor(shuffleId, token)
        assert(!declared.fallenBack,
          s"attempt $attemptId was lost to the connection timeout, which must be recovered by " +
            s"recomputing the upstream stage; instead the shuffle stood streaming down as " +
            s"'${declared.condition}'")
      }
      assert(coordinator.producerTimeoutCount(shuffleId, 0) === 9,
        s"every distinct generation lost must be tallied, but " +
          s"${coordinator.producerTimeoutCount(shuffleId, 0)} of 9 were")
      assert(coordinator.producerTimeoutTotal(shuffleId) === 9,
        s"and the shuffle-wide tally must agree, but reads " +
          s"${coordinator.producerTimeoutTotal(shuffleId)}")
      assert(coordinator.fallbackStateFor(shuffleId, token).reason.isEmpty,
        "no fallback condition may be reported for a shuffle that only ever lost producers")
    }
  }

  test("liveness timeouts spread across map outputs leave the shuffle streaming too") {
    withCoordinator(streamingConf()) { coordinator =>
      val shuffleId = 7102
      val token = registerShuffleFor(coordinator, shuffleId)

      (0 until ProducerCount).foreach { mapIndex =>
        timeOutProducer(coordinator, shuffleId, token, mapIndex, attemptId = 200L + mapIndex)
        assert(coordinator.producerTimeoutCount(shuffleId, mapIndex) === 1,
          s"map output $mapIndex must have been lost exactly once")
        assert(!coordinator.fallbackStateFor(shuffleId, token).fallenBack,
          s"${mapIndex + 1} loss(es) spread across map outputs must leave the shuffle streaming")
      }
      assert(coordinator.producerTimeoutTotal(shuffleId) === ProducerCount,
        s"the shuffle-wide tally must read $ProducerCount but read " +
          s"${coordinator.producerTimeoutTotal(shuffleId)}")
      val declared = coordinator.fallbackStateFor(shuffleId, token)
      assert(!declared.fallenBack,
        s"losing every map output's producer once must still not stand the shuffle down, but it " +
          s"stood down as '${declared.condition}'")
    }
  }

  test("only an authorized liveness timeout advances the diagnostic tallies") {
    withCoordinator(streamingConf()) { coordinator =>
      val shuffleId = 7103
      val token = registerShuffleFor(coordinator, shuffleId)
      val foreign = registerShuffleFor(coordinator, shuffleId + 1)
      assert(foreign !== token, "the two shuffles must not share a token")

      val repeats = 8
      (0 until repeats).foreach { attempt =>
        timeOutProducer(coordinator, shuffleId, token, mapIndex = 0, attemptId = 100L + attempt)
        val counted = coordinator.producerTimeoutCount(shuffleId, 0)
        assert(counted === attempt + 1,
          s"the loss must be counted, but the tally read $counted")
        assert(!coordinator.fallbackStateFor(shuffleId, token).fallenBack,
          s"${attempt + 1} loss(es) of one map output must leave the shuffle streaming: the " +
            "answer to a lost producer is the specified producer-failure flow, bounded by the " +
            "scheduler's own consecutive-attempt allowance, and not a fallback of this " +
            "subsystem's")
      }
      (1 to repeats).foreach { mapIndex =>
        timeOutProducer(coordinator, shuffleId, token, mapIndex, attemptId = 200L + mapIndex)
        assert(!coordinator.fallbackStateFor(shuffleId, token).fallenBack,
          s"losses spread across ${mapIndex + 1} map outputs must likewise leave it streaming")
      }
      assert(coordinator.producerTimeoutTotal(shuffleId) === repeats * 2,
        s"every loss must be counted for diagnosis, but the shuffle-wide tally read " +
          s"${coordinator.producerTimeoutTotal(shuffleId)}")

      val uncounted = Seq(
        StreamingShuffleInvalidationReason.IncompleteStream,
        StreamingShuffleInvalidationReason.ChecksumMismatch,
        StreamingShuffleInvalidationReason.StaleEpoch,
        StreamingShuffleInvalidationReason.Unknown)
      val otherShuffle = shuffleId + 2
      val otherToken = registerShuffleFor(coordinator, otherShuffle)
      uncounted.zipWithIndex.foreach { case (reason, index) =>
        val attemptId = 300L + index
        registerProducerFor(coordinator, otherShuffle, otherToken, mapIndex = 0, attemptId)
        coordinator.invalidateProducer(otherShuffle, otherToken,
          StreamingShuffleProducerGeneration(mapIndex = 0, mapId = attemptId, attemptId),
          reason, s"an invalidation reported as $reason")
        assert(coordinator.producerTimeoutCount(otherShuffle, 0) === 0,
          s"$reason must not be counted as a producer-liveness loss")
      }
      assert(coordinator.producerTimeoutTotal(otherShuffle) === 0,
        "no shuffle-wide loss may be recorded for reasons that are not liveness timeouts")
      assert(!coordinator.fallbackStateFor(otherShuffle, otherToken).fallenBack,
        "and the shuffle must still be streaming, however many of them arrive")

      registerProducerFor(coordinator, otherShuffle, otherToken, mapIndex = 1, attemptId = 400L)
      (1 to 4).foreach { _ =>
        assert(coordinator.invalidateProducer(otherShuffle, foreign,
            StreamingShuffleProducerGeneration(mapIndex = 1, mapId = 400L, taskAttemptId = 400L),
            StreamingShuffleInvalidationReason.ConnectionTimeout, "unauthorized") ===
            StreamingShuffleCoordinator.NO_EPOCH,
          "an unauthorized invalidation must be refused without disclosing the epoch")
      }
      assert(coordinator.producerTimeoutCount(otherShuffle, 1) === 0,
        "an unauthorized invalidation must not be counted")
      assert(!coordinator.fallbackStateFor(otherShuffle, otherToken).fallenBack,
        "and it must leave the shuffle streaming")

      timeOutProducer(coordinator, otherShuffle, token = otherToken, mapIndex = 1,
        attemptId = 400L, register = false)
      assert(coordinator.producerTimeoutCount(otherShuffle, 1) === 1,
        "the authorized timeout of a registered generation must be counted")
      assert(!coordinator.fallbackStateFor(otherShuffle, otherToken).fallenBack,
        "and counting it must still not stand the shuffle down")
    }
  }

  test("a partial buffer grant trips memory pressure") {
    val clock = newManualClock()
    val policy = activePolicy(clock)

    policy.recordAllocationGrant(RequestedBufferBytes, RequestedBufferBytes - 1L)

    assert(policy.hasTripped,
      "a reservation granted less than it asked for must trip once eviction has been attempted")
    assert(policy.trippedReason.contains(MemoryPressure),
      s"the reported reason must be MemoryPressure but was ${policy.trippedReason}")
    assert(policy.trippedAtTimeMillis.contains(clock.getTimeMillis()),
      "the trip instant must come from the injected clock")
    assert(!policy.streamingActive, "a tripped policy must not report streaming as active")
    assert(policy.shouldDelegateToSortShuffle,
      "memory pressure must route to the sort-based shuffle, which is the only terminus")
  }

  test("a buffer grant refused outright trips memory pressure") {
    val policy = activePolicy(newManualClock())

    policy.recordAllocationGrant(RequestedBufferBytes, 0L)

    assert(policy.hasTripped, "a reservation granted nothing at all must trip")
    assert(policy.trippedReason.contains(MemoryPressure),
      s"the reported reason must be MemoryPressure but was ${policy.trippedReason}")
    assert(policy.shouldDelegateToSortShuffle, "the terminus is the same for a refused reservation")
  }

  test("a satisfied buffer grant does not trip memory pressure") {
    val policy = activePolicy(newManualClock())

    policy.recordAllocationGrant(RequestedBufferBytes, RequestedBufferBytes)
    assert(!policy.hasTripped,
      "a reservation granted exactly what it asked for is not memory pressure")

    policy.recordAllocationGrant(RequestedBufferBytes, RequestedBufferBytes + 1L)
    assert(!policy.hasTripped,
      "a reservation granted more than it asked for is not memory pressure")

    val unevaluableBefore = policy.unevaluableSampleCount
    policy.recordAllocationGrant(0L, 0L)
    policy.recordAllocationGrant(-1L, 0L)
    assert(policy.unevaluableSampleCount == unevaluableBefore + 2L,
      "a reservation of nothing must be counted as un-evaluable rather than acted upon")
    assert(!policy.hasTripped, "a reservation of nothing must never trip")
    assert(policy.observedTripConditionCount == 0L,
      "no trip condition was driven, so none may have been observed")
    assert(policy.streamingActive, "streaming must remain active while reservations are satisfied")
    assert(!policy.shouldDelegateToSortShuffle,
      "a satisfied reservation must leave the streaming path in service")
  }

  // Trip 3: network utilisation above ninety per cent of the administered link capacity, SUSTAINED
  // across consecutive measurement intervals. Four independent properties are asserted. The
  // comparison is STRICTLY greater than, so exactly at the threshold is tolerated rather than
  // tripped; the run has to span distinct intervals, so one legal burst -- which this subsystem is
  // required to emit, because a pacing bucket must admit one maximum-sized frame -- is not read as
  // a saturated link; a reading back inside the share ends the run; and a capacity that is zero,
  // negative or not finite is not a capacity at all and is refused before any division, which is
  // why no division by zero and no NaN comparison is reachable however the method is called. Ninety
  // per cent is the SATURATION TRIP while eighty per cent is the token bucket's ceiling -- one
  // is a rate limit streaming imposes on itself while it keeps streaming, the other is where it
  // stops -- and the last case in this group exists to keep them from being conflated.

  test("link utilisation exactly at the saturation threshold does not trip") {
    val clock = newManualClock()
    val policy = activePolicy(clock)

    // Repeated, so that "exactly ninety does not trip" is established over a run of readings and
    // not merely for the first one: a reading AT the share must be inert however many of them
    // arrive, and must leave no partial run behind for a later burst to complete.
    (0L until SaturationSampleRepetitions).foreach { interval =>
      policy.recordLinkUtilization(EgressAtSaturationThreshold, LinkCapacityBytesPerSecond,
        clock.getTimeMillis() + interval * SaturationSampleWindowMillis)
    }

    assert(!policy.hasTripped,
      "utilisation of exactly ninety per cent must not trip; the comparison is strictly greater")
    assert(policy.saturatedIntervalCount == 0L,
      "a reading at the tolerated share must leave no run of saturated intervals standing")
    assert(policy.trippedReason.isEmpty, "no reason may be reported at the tolerated threshold")
    assert(policy.unevaluableSampleCount == 0L,
      "a sample with a known capacity must be evaluated rather than refused")
    assert(policy.streamingActive, "streaming must remain active at the tolerated threshold")
    assert(!policy.shouldDelegateToSortShuffle,
      "utilisation at the threshold must leave the streaming path in service")
  }

  test("one legal burst above the saturation threshold does not trip, a sustained run does") {
    val clock = newManualClock()
    val policy = activePolicy(clock)
    val saturatingEgress = EgressAtSaturationThreshold + 1.0d
    val firstInterval = clock.getTimeMillis()

    // WHY A RUN AND NOT ONE READING. This subsystem is required to emit traffic that reads above
    // the share for one interval of a small administered link: a pacing bucket must be able to
    // admit one maximum-sized encoded frame or it would refuse every frame forever, so its burst
    // allowance is at least one frame however small its share is, and a bucket that starts full
    // delivers that burst plus one interval's refill in the first interval. Tripping on that
    // reading stood healthy shuffles down -- every producer invalidated and the map stage
    // recomputed -- on links carrying nothing but correctly paced streaming traffic. Steady-state
    // egress is held to the token bucket's eighty percent ceiling, which is below this share, so a
    // burst clears on the next interval while a genuinely saturated link stays over capacity
    // interval after interval.
    assert(!policy.hasTripped, "the policy must start untripped, or this case proves nothing")

    policy.recordLinkUtilization(saturatingEgress, LinkCapacityBytesPerSecond, firstInterval)
    assert(!policy.hasTripped,
      "one interval above the share is a legal burst and must not stand streaming down")
    assert(policy.saturatedIntervalCount == 1L, "but it must be counted as one saturated interval")

    // Observations of the SAME interval, which is what a hundred-millisecond poll of a rate that is
    // republished once a second produces. They must count once between them, or a fast caller would
    // reach any run length inside a single interval and one burst would trip after 200 ms.
    (1L until SaturationSampleRepetitions).foreach { poll =>
      policy.recordLinkUtilization(saturatingEgress, LinkCapacityBytesPerSecond,
        firstInterval + poll * (SaturationSampleWindowMillis / SaturationSampleRepetitions))
    }
    assert(policy.saturatedIntervalCount == 1L,
      "repeated observations of one measurement interval must count once between them")
    assert(!policy.hasTripped, "so however fast the caller polls, one interval cannot trip")

    // A reading back inside the share ends the run, because the interval it describes was not
    // saturated and a run is consecutive by definition.
    policy.recordLinkUtilization(EgressAtSaturationThreshold, LinkCapacityBytesPerSecond,
      firstInterval + SaturationSampleWindowMillis)
    assert(policy.saturatedIntervalCount == 0L,
      "a reading inside the tolerated share must end the run of saturated intervals")

    // And a genuinely saturated link, which stays over capacity interval after interval, trips on
    // the interval that completes the run -- and not before it. The run required is asked of the
    // production derivation rather than written down, because it is a function of the capacity: the
    // mandatory burst allowance of a small link is many intervals of its paced rate and of a large
    // one is a single interval, so a fixed number would be either unreachable or too short.
    val required = saturationIntervalsFor(LinkCapacityBytesPerSecond)
    assert(required >= SaturationSustainedIntervals,
      s"the derived run of $required interval(s) may never fall below the floor of " +
        s"$SaturationSustainedIntervals")
    val runStart = firstInterval + 2L * SaturationSampleWindowMillis
    (0L until required - 1L).foreach { interval =>
      policy.recordLinkUtilization(saturatingEgress, LinkCapacityBytesPerSecond,
        runStart + interval * SaturationSampleWindowMillis)
      assert(!policy.hasTripped,
        s"a run of ${interval + 1L} interval(s) is short of $required and must not trip")
    }
    policy.recordLinkUtilization(saturatingEgress, LinkCapacityBytesPerSecond,
      runStart + (required - 1L) * SaturationSampleWindowMillis)

    assert(policy.hasTripped,
      s"$required consecutive saturated interval(s) must trip the condition")
    assert(policy.trippedReason.contains(NetworkSaturation),
      s"the reported reason must be NetworkSaturation but was ${policy.trippedReason}")
    assert(policy.trippedAtTimeMillis.contains(clock.getTimeMillis()),
      "the trip instant must come from the injected clock")
    assert(!policy.streamingActive, "a tripped policy must not report streaming as active")
    assert(policy.shouldDelegateToSortShuffle,
      "network saturation must route to the sort-based shuffle, which is the only terminus")
  }

  test("an unknown link capacity never trips and never divides by zero") {
    val policy = activePolicy(newManualClock())
    val unevaluableBefore = policy.unevaluableSampleCount

    policy.recordLinkUtilization(RequestedBufferBytes.toDouble, 0.0d)
    policy.recordLinkUtilization(0.0d, 0.0d)
    policy.recordLinkUtilization(RequestedBufferBytes.toDouble, -1.0d)
    policy.recordLinkUtilization(RequestedBufferBytes.toDouble, Double.NaN)
    policy.recordLinkUtilization(Double.PositiveInfinity, LinkCapacityBytesPerSecond)

    assert(policy.unevaluableSampleCount == unevaluableBefore + 5L,
      "every sample without a usable capacity must be counted as un-evaluable")
    assert(!policy.hasTripped,
      "a link whose capacity is unknown can never be described as saturated")
    assert(policy.streamingActive, "streaming must survive samples that cannot be evaluated")
    assert(!policy.shouldDelegateToSortShuffle,
      "an un-evaluable saturation sample must leave the streaming path in service")
  }

  test("an unset bandwidth cap means unlimited rather than zero") {
    val policy = activePolicy(newManualClock())

    assert(policy.administeredLinkCapacityBytesPerSecond.isEmpty,
      "no administered capacity may be reported when the bandwidth cap is unset")

    val unevaluableBefore = policy.unevaluableSampleCount
    policy.recordLinkUtilization(Double.MaxValue)
    assert(policy.unevaluableSampleCount == unevaluableBefore + 1L,
      "an uncapped link leaves saturation unevaluable, which is counted rather than acted upon")
    assert(!policy.hasTripped,
      "an uncapped link must never trip saturation, however much egress is reported")
    assert(policy.streamingActive, "the default configuration must leave streaming active")
  }

  test("an administered bandwidth cap makes saturation evaluable in bytes per second") {
    val conf = streamingConfWithOverrides(maxBandwidthMBps = Some(AdministeredBandwidthMbps))
    val clock = newManualClock()
    val policy = new StreamingShuffleFallbackPolicy(conf, clock)
    val capacityBytesPerSecond = AdministeredBandwidthMbps.toLong * BytesPerMebibyte

    assert(policy.administeredLinkCapacityBytesPerSecond.contains(capacityBytesPerSecond),
      "the administered cap must be converted from MB/s into bytes per second")

    val capacity = capacityBytesPerSecond.toDouble
    policy.recordLinkUtilization(capacity * LinkSaturationTripPercent.toDouble / 100.0d)
    assert(!policy.hasTripped,
      "the administered capacity must tolerate utilisation at the threshold")

    // The one-argument overload, which is the production path: a reader reports measured ingress
    // and the policy takes the capacity from the configuration and the instant from its clock. The
    // clock is advanced a whole measurement interval between readings, because that is what makes
    // each one a distinct interval rather than another poll of the same one.
    (0L until saturationIntervalsFor(capacity)).foreach { interval =>
      if (interval > 0L) clock.advance(SaturationSampleWindowMillis)
      policy.recordLinkUtilization(capacity)
    }
    assert(policy.hasTripped, "a fully saturated administered link must trip")
    assert(policy.trippedReason.contains(NetworkSaturation),
      s"the reported reason must be NetworkSaturation but was ${policy.trippedReason}")
    assert(policy.shouldDelegateToSortShuffle, "the terminus is the same for an administered link")
  }

  test("the unevaluable saturation notice is stated once per jvm and never repeated") {
    val latch = StreamingShuffleFallbackPolicy.saturationNoticeEmitted
    val latchedOnEntry = latch.get()
    try {
      latch.set(false)

      val silentAppender = new LogAppender("saturation coverage notice, other paths")
      withLogAppender(
        silentAppender,
        loggerNames = Seq(classOf[StreamingShuffleFallbackPolicy].getName),
        level = Some(Level.INFO)) {
        val capped = new StreamingShuffleFallbackPolicy(
          streamingConfWithOverrides(maxBandwidthMBps = Some(AdministeredBandwidthMbps)),
          newManualClock())
        assert(capped.administeredLinkCapacityBytesPerSecond.isDefined,
          "the capped fixture must have an administered capacity, or it proves nothing")

        val gatedOff = new StreamingShuffleFallbackPolicy(gatedOffStreamingConf(), newManualClock())
        assert(gatedOff.administeredLinkCapacityBytesPerSecond.isEmpty,
          "the gated-off fixture must have no capacity, or it is silent for the wrong reason")
        assert(!gatedOff.streamingActive,
          "the gated-off fixture must have the kill switch engaged")
      }
      assert(saturationNotices(silentAppender).isEmpty,
        "an evaluable condition and a disengaged subsystem both have nothing to say: " +
          s"${saturationNotices(silentAppender).mkString("; ")}")
      assert(!latch.get(),
        "neither of those constructions may consume the once-per-JVM notice")

      val appender = new LogAppender("saturation coverage notice")
      var constructed = 0
      withLogAppender(
        appender,
        loggerNames = Seq(classOf[StreamingShuffleFallbackPolicy].getName),
        level = Some(Level.INFO)) {
        val first = new StreamingShuffleFallbackPolicy(streamingConf(), newManualClock())
        constructed += 1
        assert(latch.get(),
          "the first construction that cannot evaluate saturation must latch the notice")
        assert(first.streamingActive, "the first construction must have streaming enabled")
        val noticesAfterFirst = saturationNotices(appender)
        assert(noticesAfterFirst.size == 1,
          s"exactly one notice must be emitted but ${noticesAfterFirst.size} were")

        // A second and a third policy in the same JVM -- which every local and local-cluster run
        // constructs -- describe the same executor configuration and must add nothing.
        (0 until 2).foreach { _ =>
          val repeat = new StreamingShuffleFallbackPolicy(streamingConf(), newManualClock())
          constructed += 1
          assert(repeat.streamingActive,
            "a repeat construction must have streaming enabled")
          assert(repeat.administeredLinkCapacityBytesPerSecond.isEmpty,
            "a repeat construction must still be unable to evaluate saturation")
          val unevaluableBefore = repeat.unevaluableSampleCount
          repeat.recordLinkUtilization(Double.MaxValue)
          assert(repeat.unevaluableSampleCount == unevaluableBefore + 1L,
            "a repeat construction must still count saturation samples as un-evaluable")
          assert(!repeat.hasTripped,
            "an un-evaluable sample must not trip, however large")
        }
      }

      val notices = saturationNotices(appender)
      assert(notices.size == 1,
        s"the notice is once per JVM, not once per policy, but ${notices.size} were emitted")
      assert(latch.get(), "the latch must remain set after the repeats")
      assert(constructed == 3, "three policies must have been constructed")

      val notice = notices.head
      assert(notice.getLevel == Level.INFO,
        s"an unevaluable condition is a notice rather than a fault but was ${notice.getLevel}")
      val text = notice.getMessage.getFormattedMessage
      assert(text.contains(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS.key),
        s"the notice must name the property that makes saturation evaluable: $text")
      assert(text.contains(LinkSaturationTripPercent.toString),
        s"the notice must name the share that cannot be evaluated: $text")
      assert(text.contains("never") && text.contains("trip"),
        s"the notice must say the condition will never trip: $text")
      assert(text.contains("consumer slowness") && text.contains("memory pressure") &&
          text.contains("protocol version"),
        s"the notice must say which conditions remain active: $text")
    } finally {
      latch.set(latchedOnEntry)
    }
  }

  test("the saturation trip share and the egress bandwidth ceiling are different constants") {
    assert(StreamingShuffleFallbackPolicy.SATURATION_TRIP_PERCENT == 90L,
      "the saturation trip share is specified as ninety per cent")
    assert(StreamingShuffleFallbackPolicy.SATURATION_TRIP_PERCENT == LinkSaturationTripPercent,
      "the policy and the backpressure protocol must agree on the saturation share")
    assert(BandwidthCeilingPercent == 80L,
      "the egress bandwidth ceiling is specified as eighty per cent")
    assert(StreamingShuffleFallbackPolicy.SATURATION_TRIP_PERCENT != BandwidthCeilingPercent,
      "the saturation trip and the bandwidth ceiling are different numbers doing different jobs")
    assert(StreamingShuffleFallbackPolicy.SATURATION_TRIP_RATIO ==
        StreamingShuffleFallbackPolicy.SATURATION_TRIP_PERCENT.toDouble / 100.0d,
      "the ratio the comparison uses must be the trip percentage expressed as a fraction")
    assert(StreamingShuffleFallbackPolicy.CONSUMER_SLOWNESS_RATIO == 2.0d,
      "the tolerated consumer shortfall is specified as a factor of two")
    assert(StreamingShuffleFallbackPolicy.CONSUMER_SLOWNESS_RATIO == ConsumerSlownessRatio,
      "the policy and the shared fixtures must agree on the tolerated shortfall")
    assert(StreamingShuffleFallbackPolicy.SUSTAINED_SLOWNESS_WINDOW_MS == 60000L,
      "the sustained window is specified as sixty seconds")
    assert(
      StreamingShuffleFallbackPolicy.SUSTAINED_SLOWNESS_WINDOW_MS == SustainedSlownessWindowMillis,
      "the policy and the backpressure protocol must agree on the sustained window")
  }

  // Trip 4: a producer/consumer protocol version mismatch, detected explicitly and never inferred
  // from a failed decode.

  test("only the current streaming shuffle protocol version is compatible") {
    verifyWireContractAgainstEncoder()
    assert(ProtocolVersion == 1,
      "the streaming protocol revision this build speaks is 1, stated independently of the encoder")
    assert(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION == 1,
      s"the encoder must stamp revision 1, but stamps " +
        s"${StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION}; a revision change is one " +
        "every peer of a different build has to be considered against")
    assert(StreamingShuffleMessage.isCompatible(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION),
      "this build must consider its own protocol revision compatible")

    val everyOtherVersion = (Byte.MinValue.toInt to Byte.MaxValue.toInt)
      .map(_.toByte)
      .filter(_ != StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)
    assert(everyOtherVersion.size == 255,
      "the byte domain holds two hundred and fifty six values, one of which is the current one")
    assert(everyOtherVersion.forall(version => !StreamingShuffleMessage.isCompatible(version)),
      "no revision other than the current one may be considered compatible")

    assert(HeaderEncodedLength == 1 + 4 + 4 + 8,
      "the shared header is a version byte, a shuffle id, a partition id and a sequence number")
    assert(ProducerIdEncodedLength == 8,
      "and the producer identifier that opens every body is an eight-byte map id")
    assert(FrameTypePrefixLength == 1,
      "a framed message opens with exactly one type-discriminator byte")
  }

  test("peeking the protocol version does not consume the framed message") {
    val padded = paddedFramedMessage(FramePaddingBytes)
    val positionBefore = padded.position()
    val limitBefore = padded.limit()
    val remainingBefore = padded.remaining()
    assert(positionBefore == FramePaddingBytes,
      "the fixture must present the frame at a non-zero position")

    val peeked = StreamingShuffleMessage.peekProtocolVersion(padded)

    assert(peeked == StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
      "the peek must read the version this build stamped, from the buffer's own position")
    assert(padded.position() == positionBefore,
      "the read is absolute, so the buffer's position must be exactly as it was")
    assert(padded.limit() == limitBefore, "the peek must not disturb the buffer's limit")
    assert(padded.remaining() == remainingBefore, "the peek must not consume any of the frame")
    assert(padded.get(positionBefore + FrameTypePrefixLength) == peeked,
      "the version byte must sit immediately after the framing prefix")

    val decoded = StreamingShuffleMessage.Decoder.fromByteBuffer(padded)
    assert(decoded.protocolVersion() == StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
      "the same buffer must still decode after being peeked")
    assert(decoded.shuffleId() == ShuffleId,
      "the decoded frame must name the shuffle it was built for")
    assert(padded.position() == positionBefore,
      "decoding through the shared entry point must also leave the caller's position untouched")
  }

  test("an incompatible peer version trips through an explicit check rather than a parse failure") {
    val clock = newManualClock()
    val policy = activePolicy(clock)

    val compatible = policy.checkProtocolVersion(IncompatibleProtocolVersion)

    assert(!compatible, "the check must report an incompatible peer rather than raise")
    assert(policy.hasTripped, "an incompatible peer version must trip")
    assert(policy.trippedReason.contains(ProtocolVersionMismatch),
      s"the reported reason must be ProtocolVersionMismatch but was ${policy.trippedReason}")
    assert(policy.trippedAtTimeMillis.contains(clock.getTimeMillis()),
      "the trip instant must come from the injected clock")
    assert(!policy.streamingActive, "a tripped policy must not report streaming as active")
    assert(policy.shouldDelegateToSortShuffle,
      "a version mismatch must route to the sort-based shuffle, which is the only terminus")

    val thrown = intercept[IllegalArgumentException] {
      StreamingShuffleMessage.checkProtocolVersion(IncompatibleProtocolVersion)
    }
    assert(thrown.getMessage.contains("Incompatible streaming shuffle protocol version"),
      s"the failure must name the condition it reports but said: ${thrown.getMessage}")
  }

  test("an incompatible framed message trips without being decoded") {
    val policy = activePolicy(newManualClock())
    val framed = framedMessageWithVersion(IncompatibleProtocolVersion)
    val positionBefore = framed.position()

    val compatible = policy.checkProtocolVersion(framed)

    assert(!compatible, "a frame stamped with an unknown revision must be reported incompatible")
    assert(policy.hasTripped, "a frame stamped with an unknown revision must trip")
    assert(policy.trippedReason.contains(ProtocolVersionMismatch),
      s"the reported reason must be ProtocolVersionMismatch but was ${policy.trippedReason}")
    assert(framed.position() == positionBefore,
      "settling compatibility must not consume the frame it was settled from")
    assert(policy.shouldDelegateToSortShuffle, "the terminus is the same for a framed mismatch")
  }

  test("a compatible framed message is accepted without tripping") {
    val policy = activePolicy(newManualClock())

    assert(policy.checkProtocolVersion(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION),
      "this build's own revision must be accepted")
    assert(policy.checkProtocolVersion(framedMessage()),
      "a frame this build stamped must be accepted through the framed overload too")
    assert(!policy.hasTripped, "accepting a compatible peer must not trip anything")
    assert(policy.observedTripConditionCount == 0L,
      "no condition was observed, so none may have been counted")
    assert(policy.streamingActive, "a compatible peer must leave streaming active")
    assert(!policy.shouldDelegateToSortShuffle,
      "a compatible peer must leave the streaming path in service")
  }

  test("a frame too short to carry a version is a framing fault and not a version mismatch") {
    val policy = activePolicy(newManualClock())
    val truncated = ByteBuffer.allocate(FrameTypePrefixLength)

    intercept[IllegalArgumentException] {
      policy.checkProtocolVersion(truncated)
    }

    assert(!policy.hasTripped, "truncation must not be mislabelled as a version mismatch")
    assert(policy.trippedReason.isEmpty, "a framing fault must leave no fallback reason behind")
    assert(policy.streamingActive, "a framing fault must leave streaming active")
    assert(!policy.shouldDelegateToSortShuffle,
      "a framing fault must leave the streaming path in service")
  }

  test("the operator kill switch is engaged by default and is not a trip") {
    val defaults = new SparkConf(false).set(SHUFFLE_MANAGER, StreamingShuffleManager.SHORT_NAME)
    assert(!defaults.get(SHUFFLE_STREAMING_ENABLED),
      "the behaviour gate must default to false, so streaming is strictly opt in")

    val policy = new StreamingShuffleFallbackPolicy(defaults, newManualClock())

    assert(policy.killSwitchEngaged, "an unset behaviour gate must report the kill switch engaged")
    assert(!policy.streamingEnabledByConfig, "the configured intent must be reported as disabled")
    assert(!policy.streamingActive, "streaming may not be active while the gate is closed")
    assert(policy.shouldDelegateToSortShuffle,
      "the kill switch must route to the sort-based shuffle, which is the only terminus")
    assert(!policy.hasTripped,
      "the kill switch is an operator decision, not an observed condition, so it is not a trip")
    assert(policy.trippedReason.isEmpty,
      "the closed condition set has no member for an operator turning streaming off")
    assert(policy.trippedAtTimeMillis.isEmpty, "no trip instant may be stamped for the kill switch")
    assert(policy.observedTripConditionCount == 0L,
      "engaging the kill switch must not be counted as a condition having been observed")
  }

  test("with the kill switch engaged every service provider call is delegated to sort shuffle") {
    val conf = withLocalMaster(gatedOffStreamingConf(), "streaming-shuffle-fallback-delegation")
    sc = new SparkContext(conf)

    val delegate = new SortShuffleManager(conf)
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    try {
      val dependency = shuffleDependencyFor(sc, conf, numPartitions = PartitionCount)
      val shuffleId = dependency.shuffleId

      assert(manager.shuffleBlockResolver.getClass == delegate.shuffleBlockResolver.getClass,
        "a gated-off manager must publish the sort delegate's own block resolver")
      assert(!manager.shuffleBlockResolver.getClass.getName.contains(".streaming."),
        "no streaming resolver may be published while the kill switch is engaged")

      val expectedHandle = delegate.registerShuffle(shuffleId, dependency)
      val actualHandle = manager.registerShuffle(shuffleId, dependency)
      assert(actualHandle.getClass == expectedHandle.getClass,
        s"registerShuffle must return the sort handle ${expectedHandle.getClass.getName} but " +
          s"returned ${actualHandle.getClass.getName}")
      assert(!actualHandle.getClass.getName.contains(".streaming."),
        "no streaming handle may be minted while the kill switch is engaged")
      assert(actualHandle.shuffleId == shuffleId,
        "the handle must name the shuffle it was asked for")

      val context = fakeTaskContext(sc)
      val readMetrics = new RecordingStreamingShuffleReadMetrics
      val expectedReader = delegate.getReader[Int, Int](expectedHandle, 0, Int.MaxValue, 0,
        PartitionCount, context, readMetrics)
      val actualReader = manager.getReader[Int, Int](actualHandle, 0, Int.MaxValue, 0,
        PartitionCount, context, readMetrics)
      assert(actualReader.getClass == expectedReader.getClass,
        s"getReader must return the sort reader ${expectedReader.getClass.getName} but returned " +
          s"${actualReader.getClass.getName}")
      assert(!actualReader.getClass.getName.contains(".streaming."),
        "no streaming reader may be built while the kill switch is engaged")

      assert(manager.unregisterShuffle(shuffleId) == delegate.unregisterShuffle(shuffleId),
        "unregisterShuffle must answer exactly as the sort delegate answers")
    } finally {
      manager.stop()
      delegate.stop()
    }
  }

  test("a job completes on the sort based path while the kill switch is engaged") {
    val baseline = sortBaselineGroupedOutput(numPartitions = PartitionCount)
    assert(baseline.nonEmpty, "the sort-based baseline must produce output to compare against")

    val conf = withLocalMaster(gatedOffStreamingConf(), "streaming-shuffle-fallback-kill-switch")
    sc = new SparkContext(conf)

    assert(sc.getConf.get(SHUFFLE_MANAGER) == StreamingShuffleManager.SHORT_NAME,
      "the streaming manager class must be the one this application selected")
    assert(!sc.getConf.get(SHUFFLE_STREAMING_ENABLED),
      "the behaviour gate must be closed for this application")

    val observed = groupedOutputAsSet(sc, PartitionCount)
    assertNoDataLoss(observed, baseline, "a job run with the streaming kill switch engaged")
  }

  // Latching, and the diagnostics that survive it.

  test("the tripped state latches on the first condition observed") {
    val clock = newManualClock()
    val policy = activePolicy(clock)

    // A sustained run at the full administered capacity, which is strictly above the tolerated
    // share. Driven through the shared helper so the run length lives in one place.
    driveLinkSaturation(policy, LinkCapacityBytesPerSecond, LinkCapacityBytesPerSecond,
      clock.getTimeMillis())
    assert(policy.trippedReason.contains(NetworkSaturation), "the first condition must be recorded")
    val firstTrippedAt = policy.trippedAtTimeMillis
    assert(firstTrippedAt.contains(clock.getTimeMillis()), "the first trip must be stamped")

    clock.advance(SustainedSlownessWindowMillis)
    policy.recordAllocationGrant(RequestedBufferBytes, 0L)
    assert(!policy.checkProtocolVersion(IncompatibleProtocolVersion),
      "a later condition must still be evaluated and reported to its caller")
    driveSustainedConsumerSlowness(policy, clock)

    assert(policy.trippedReason.contains(NetworkSaturation),
      s"the first reason must be retained but became ${policy.trippedReason}")
    assert(policy.trippedAtTimeMillis == firstTrippedAt,
      "the trip instant must record when the policy latched, not when it was last provoked")
    assert(policy.observedTripConditionCount == 4L,
      "latching hides repetition from the reason, so every condition must still be counted")
    assert(policy.shouldDelegateToSortShuffle, "the terminus does not change once it is reached")
  }

  test("reset restores the untripped state and discards sampling state") {
    val clock = newManualClock()
    val policy = activePolicy(clock)

    driveSustainedConsumerSlowness(policy, clock)
    policy.recordProducerThroughput(OtherShuffleId, ProducerBytesPerSecond, clock.getTimeMillis())
    policy.recordAllocationGrant(0L, 0L)
    // A saturating reading is part of that sampling state, and it is taken here so that reset is
    // exercised against a policy that has taken one. A reading left standing across a reset
    // described a link the next workload never used, and an operator reading it after a later trip
    // would be told about capacity this workload never consumed.
    driveLinkSaturation(policy, EgressAtSaturationThreshold + 1.0d, LinkCapacityBytesPerSecond,
      clock.getTimeMillis())
    assert(policy.hasTripped, "the fixture must have tripped before reset is exercised")
    assert(policy.trackedShuffleCount == 2, "two shuffles must have been sampled")
    assert(policy.unevaluableSampleCount == 1L, "one un-evaluable sample must have been counted")

    policy.reset()

    (0L until SaturationSampleRepetitions).foreach { _ =>
      policy.recordLinkUtilization(EgressAtSaturationThreshold, LinkCapacityBytesPerSecond)
    }
    assert(!policy.hasTripped,
      "a reset policy must tolerate utilisation at the threshold exactly as a fresh one does")
    assert(policy.saturatedIntervalCount == 0L,
      "reset must discard the run of saturated intervals along with the rest of the sampling state")

    assert(!policy.hasTripped, "reset must restore the untripped state")
    assert(policy.trippedReason.isEmpty, "reset must discard the latched reason")
    assert(policy.trippedAtTimeMillis.isEmpty, "reset must discard the trip instant")
    assert(policy.observedTripConditionCount == 0L,
      "a count read after a reset must be absolute rather than a difference against the past")
    assert(policy.unevaluableSampleCount == 0L, "reset must clear the un-evaluable tally")
    assert(policy.trackedShuffleCount == 0, "reset must discard per-shuffle sampling state")
    assert(policy.slownessArmedSinceMs(ShuffleId).isEmpty,
      "reset must discard an armed sustained-slowness window")
    assert(policy.streamingActive,
      "a reset policy on an enabled configuration must be active again")
    assert(!policy.shouldDelegateToSortShuffle,
      "a reset policy must return the streaming path to service")
  }

  test("concurrent readers observe the latch without locking and never see a torn state") {
    val policy = activePolicy(newManualClock())

    // One thread trips the policy while the rest read it as fast as they can.
    val outcomes = runConcurrently(ConcurrentReaderCount + 1,
      "streaming-shuffle-fallback-latch") { threadIndex =>
      if (threadIndex == 0) {
        policy.recordAllocationGrant(RequestedBufferBytes, 0L)
        true
      } else {
        var consistent = true
        var iteration = 0
        while (iteration < LatchReadIterations) {
          val trippedFirst = policy.hasTripped
          val reasonAfter = policy.trippedReason
          if (trippedFirst && reasonAfter.isEmpty) {
            consistent = false
          }
          val reasonFirst = policy.trippedReason
          val trippedAfter = policy.hasTripped
          if (reasonFirst.isDefined && !trippedAfter) {
            consistent = false
          }
          if (reasonAfter.exists(reason => reason != MemoryPressure)) {
            consistent = false
          }
          iteration += 1
        }
        consistent
      }
    }

    assert(outcomes.size == ConcurrentReaderCount + 1,
      "every thread must have completed within the bounded deadline")
    assert(outcomes.forall(identity),
      "a concurrent reader observed a torn latch, which a monotone latch must make impossible")
    assert(policy.hasTripped, "the writing thread must have tripped the policy")
    assert(policy.trippedReason.contains(MemoryPressure),
      s"the reported reason must be MemoryPressure but was ${policy.trippedReason}")
    assert(policy.shouldDelegateToSortShuffle, "the terminus is reached under contention too")
  }

  test("every fallback condition and the kill switch reach the policy's sort shuffle terminus") {
    val drivers: Seq[(StreamingShuffleFallbackReason,
        (StreamingShuffleFallbackPolicy, ManualClock) => Unit)] = Seq(
      ConsumerTooSlow -> ((policy, clock) => driveSustainedConsumerSlowness(policy, clock)),
      MemoryPressure -> ((policy, _) => policy.recordAllocationGrant(RequestedBufferBytes, 0L)),
      NetworkSaturation -> ((policy, clock) =>
        // A sustained run at the full administered capacity, strictly above the tolerated share.
        driveLinkSaturation(policy, LinkCapacityBytesPerSecond, LinkCapacityBytesPerSecond,
          clock.getTimeMillis())),
      ProtocolVersionMismatch -> ((policy, _) =>
        assert(!policy.checkProtocolVersion(IncompatibleProtocolVersion),
          "the fixture must actually drive the protocol mismatch it claims to drive")))

    assert(drivers.map(_._1) == StreamingShuffleFallbackReason.all,
      "this test must drive every condition in the closed set, in the order the set lists them")

    drivers.foreach { case (reason, drive) =>
      val clock = newManualClock()
      val policy = activePolicy(clock)
      assert(policy.streamingActive, s"streaming must start active before $reason is driven")
      assert(!policy.shouldDelegateToSortShuffle,
        s"the streaming path must be in service before $reason is driven")

      drive(policy, clock)

      assert(policy.hasTripped, s"$reason must latch the policy")
      assert(policy.trippedReason.contains(reason),
        s"$reason must be the reason reported but ${policy.trippedReason} was")
      assert(!policy.streamingActive, s"$reason must withdraw the streaming path")
      assert(policy.shouldDelegateToSortShuffle,
        s"$reason must route to the unmodified sort-based shuffle, so the job keeps a shuffle")
      assert(!policy.killSwitchEngaged,
        s"$reason must stay distinguishable from an operator turning streaming off")
      assert(policy.streamingEnabledByConfig,
        s"$reason must not rewrite the operator's configured intent")
    }

    val gated = new StreamingShuffleFallbackPolicy(gatedOffStreamingConf(), newManualClock())
    assert(gated.shouldDelegateToSortShuffle,
      "the kill switch must reach the same terminus as every trip condition")
    assert(!gated.streamingActive, "the kill switch must withdraw the streaming path")
    assert(!gated.hasTripped, "the kill switch must reach that terminus without any trip at all")
  }

  /**
   * Records what the scheduler was told during a job, so a stand-down can be told from a failure.
   */
  private class StandDownRecorder extends SparkListener {

    private val counted = new mutable.ArrayBuffer[TaskFailedReason]()
    private val fetched = new mutable.ArrayBuffer[FetchFailed]()
    private val submissions = new mutable.HashMap[Int, Int]()

    override def onTaskEnd(event: SparkListenerTaskEnd): Unit = synchronized {
      event.reason match {
        case failed: FetchFailed =>
          fetched += failed
        case failed: TaskFailedReason if failed.countTowardsTaskFailures =>
          counted += failed
        case _ =>
      }
    }

    override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = synchronized {
      val stageId = event.stageInfo.stageId
      submissions.put(stageId, submissions.getOrElse(stageId, 0) + 1)
    }

    def countedFailures: Seq[TaskFailedReason] = synchronized(counted.toSeq)

    def fetchFailures: Seq[FetchFailed] = synchronized(fetched.toSeq)

    def maxStageSubmissions: Int = synchronized {
      if (submissions.isEmpty) 0 else submissions.values.max
    }
  }
  test("a job completes under a one-failure master when each condition trips mid-write") {
    val baseline = sortBaselineGroupedOutput(numPartitions = PartitionCount)
    assert(baseline.nonEmpty, "the sort-based baseline must produce output to compare against")

    val conf = withLocalMaster(streamingConf(), "streaming-shuffle-midwrite-standdown")
      .set(TASK_MAX_FAILURES, 1)
      .set(STAGE_MAX_CONSECUTIVE_ATTEMPTS, 2 * PartitionCount + 4)
    sc = new SparkContext(conf)

    val manager = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]
    assert(manager.streamingFallbackPolicy.streamingActive,
      "streaming must be in service before the first condition is driven")

    StreamingShuffleFallbackReason.all.foreach { reason =>
      manager.streamingFallbackPolicy.reset()
      assert(manager.streamingFallbackPolicy.streamingActive,
        s"streaming must be back in service before $reason is driven")

      val holder = new MidWriteTripHolder(reason)
      val shuffled = midWriteTrippingWorkload(sc, PartitionCount, holder)
      holder.shuffleId = shuffled.dependencies.head
        .asInstanceOf[ShuffleDependency[Int, String, _]].shuffleId
      assert(holder.shuffleId >= 0, "the workload must have registered a shuffle to trip")

      val recorder = new StandDownRecorder
      sc.addSparkListener(recorder)
      val observed = try {
        shuffled.mapValues(values => values.toSeq.sorted).collect().toSet
      } finally {
        sc.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
        sc.removeSparkListener(recorder)
      }

      assertNoDataLoss(observed, baseline,
        s"a job whose map stage stood streaming down mid-write because of $reason")
      assert(manager.streamingFallbackPolicy.hasTripped ||
          manager.streamingFallbackPolicy.shuffleHasFallenBack(holder.shuffleId),
        s"$reason must have been latched by the run rather than merely attempted")

      assert(recorder.countedFailures.isEmpty,
        s"a stand-down for $reason must count no task failure, yet the run reported " +
          s"${recorder.countedFailures.size}: " +
          recorder.countedFailures.take(3).map(_.toErrorString).mkString("[", ", ", "]"))
      assert(recorder.maxStageSubmissions <= 1 + PartitionCount,
        s"a stand-down for $reason must be recovered within one resubmission per map partition, " +
          s"yet the busiest stage of a $PartitionCount partition shuffle was submitted " +
          s"${recorder.maxStageSubmissions} time(s), with ${recorder.fetchFailures.size} fetch " +
          "failure(s) reported")
    }
  }

  test("the fallback condition set is closed at exactly four conditions") {
    val all = StreamingShuffleFallbackReason.all

    assert(all.size == 4,
      s"the specification enumerates four conditions but the set holds ${all.size}")
    assert(all.distinct.size == all.size, "no condition may appear in the set twice")
    assert(all.toSet == Set[StreamingShuffleFallbackReason](
        ConsumerTooSlow, MemoryPressure, NetworkSaturation, ProtocolVersionMismatch),
      s"the set must hold exactly the four specified conditions but held $all")

    assert(
      all.forall(reason =>
        StreamingShuffleFallbackReason.fromName(reason.toString).contains(reason)),
      "every condition must resolve from its own stable name")
    assert(all.map(_.toString).distinct.size == all.size,
      "every condition must have a distinct name")
    assert(StreamingShuffleFallbackReason.fromName("ConsumerFarTooSlowIndeed").isEmpty,
      "a name outside the closed set must resolve to nothing, so no peer can add a condition")
    assert(StreamingShuffleFallbackReason.fromName(null).isEmpty,
      "resolution must tolerate a null name rather than raise on it")

    assert(all.forall(_.description.nonEmpty),
      "every condition must carry an operator-facing wording")
    assert(all.map(_.description).distinct.size == all.size,
      "every condition must be distinguishable from the others in the log")

    assert(ConsumerTooSlow.description.contains("2x slower") &&
        ConsumerTooSlow.description.contains("60 seconds"),
      s"consumer slowness must render as the measured rate predicate: '${
        ConsumerTooSlow.description}'")
    assert(!ConsumerTooSlow.description.contains("timeout") &&
        !ConsumerTooSlow.description.contains("producer(s)"),
      s"and must claim nothing about producer loss: '${ConsumerTooSlow.description}'")
    assert(MemoryPressure.description.contains("memory"),
      s"memory pressure must render as a memory condition: '${MemoryPressure.description}'")
    assert(NetworkSaturation.description.contains("90%"),
      s"network saturation must render with its threshold: '${NetworkSaturation.description}'")
    assert(ProtocolVersionMismatch.description.contains("version"),
      s"a version mismatch must render as one: '${ProtocolVersionMismatch.description}'")
  }

  test("a capability stand-down is not a fallback condition and never borrows one of their names") {
    val causes = StreamingShuffleStandDownCause.structuralDeclines
    assert(causes === Seq(
        StreamingShuffleStandDownCause.UnsupportedReadShape,
        StreamingShuffleStandDownCause.ProducerUnavailable),
      s"the build must hold exactly these two capability causes but held $causes")

    val conditionNames = StreamingShuffleFallbackReason.all.map(_.toString).toSet
    assert(causes.forall(cause => !conditionNames.contains(cause.toString)),
      s"no cause may borrow a condition's name, yet ${causes.map(_.toString)} overlaps " +
        s"$conditionNames")
    assert(causes.forall(cause => StreamingShuffleFallbackReason.fromName(cause.toString).isEmpty),
      "and no cause may resolve onto the four conditions")
    assert(causes.forall(cause =>
        StreamingShuffleStandDownCause.fromName(cause.toString).exists(resolved =>
          !resolved.isInstanceOf[StreamingShuffleFallbackReason])),
      "a cause resolved through the shared entry point must report no measured condition")
    assert(StreamingShuffleFallbackReason.all.forall(reason =>
        StreamingShuffleStandDownCause.fromName(reason.toString).exists {
          case _: StreamingShuffleStandDownCause.StructuralDecline => false
          case resolved => resolved == reason
        }),
      "and no condition may resolve onto the causes")
    assert(causes.forall(_.description.nonEmpty) &&
        causes.map(_.description).distinct.size == causes.size,
      s"every cause must carry a distinct operator-facing wording but held " +
        s"${causes.map(_.description)}")
    assert(StreamingShuffleStandDownCause.fromName("NotACauseName").isEmpty &&
        StreamingShuffleStandDownCause.fromName(null).isEmpty,
      "an unknown or null name must resolve to nothing, so no peer can introduce a cause")

    val policy = activePolicy(newManualClock())
    assert(policy.trippedReason.isEmpty && !policy.hasTripped,
      "the policy must start untripped, or the assertion below proves nothing")
    policy.observeShuffleFallback(4242, StreamingShuffleFallbackState(
      StreamingShuffleStandDownCause.ProducerUnavailable.toString, declaredAtEpoch = 3L))
    assert(policy.shuffleHasFallenBack(4242),
      "the shuffle-wide verdict must be cached whatever kind of stand-down it carries")
    assert(policy.trippedReason.isEmpty,
      s"but no condition may be latched locally by it, yet ${policy.trippedReason} was")
    assert(policy.knownShuffleFallback(4242).flatMap(_.reason).isEmpty,
      "and the cached verdict must report no measured condition")
    assert(policy.knownShuffleFallback(4242).map(_.condition) ===
        Some(StreamingShuffleStandDownCause.ProducerUnavailable.description),
      s"while still rendering the cause an operator reads, but rendered " +
        s"${policy.knownShuffleFallback(4242).map(_.condition)}")
  }

  test("fallback publishes no fifth metric and raises no error condition") {
    resetStreamingShuffleMetrics()
    val expectedMetrics = MetricNames.toSet
    assert(expectedMetrics.size == 4,
      s"the specification names four metrics but the fixture lists ${expectedMetrics.size}")
    assert(streamingShuffleMetricNames() == expectedMetrics,
      s"the registry must publish exactly the four named metrics but published " +
        s"${streamingShuffleMetricNames()}")
    assert(StreamingShuffleMetricsSource.sourceName == MetricsSourceName,
      "the four metrics must live under the single streaming shuffle namespace")

    val catalogued = SparkThrowableHelper.errorReader.errorInfoMap.keys
      .filter(_.startsWith(StreamingShuffleConditionPrefix)).toSet
    assert(catalogued == AuthorizedErrorConditions,
      s"the catalogue must hold exactly the authorized streaming shuffle conditions " +
        s"${AuthorizedErrorConditions.toSeq.sorted.mkString("[", ", ", "]")} but held " +
        s"${catalogued.toSeq.sorted.mkString("[", ", ", "]")}")
    catalogued.foreach { condition =>
      assert(SparkThrowableHelper.getSqlState(condition) == StreamingShuffleSqlState,
        s"$condition must carry SQLSTATE $StreamingShuffleSqlState, matching the checksum " +
          s"verification precedent, but carried ${SparkThrowableHelper.getSqlState(condition)}")
    }
    StreamingShuffleFallbackReason.all.foreach { reason =>
      val named = catalogued.filter(_.toUpperCase(Locale.ROOT).contains(
        reason.toString.toUpperCase(Locale.ROOT)))
      assert(named.isEmpty,
        s"$reason is a degradation and must be reported through the sealed set rather than as an " +
          s"error condition, yet the catalogue held ${named.mkString("[", ", ", "]")}")
    }

    val clock = newManualClock()
    val policy = activePolicy(clock)
    driveSustainedConsumerSlowness(policy, clock)
    policy.recordAllocationGrant(RequestedBufferBytes, 0L)
    driveLinkSaturation(policy, LinkCapacityBytesPerSecond, LinkCapacityBytesPerSecond,
      clock.getTimeMillis())
    assert(!policy.checkProtocolVersion(IncompatibleProtocolVersion),
      "the incompatible peer must be reported by return value rather than by an exception")
    assert(policy.hasTripped, "the fixture must have latched")
    assert(policy.trippedReason.contains(ConsumerTooSlow),
      s"the first condition driven must be the one reported but ${policy.trippedReason} was")

    assert(streamingShuffleMetricNames() == expectedMetrics,
      s"driving every fallback condition must publish no further metric but published " +
        s"${streamingShuffleMetricNames()}")
    assert(SparkThrowableHelper.errorReader.errorInfoMap.keys
        .filter(_.startsWith(StreamingShuffleConditionPrefix)).toSet == AuthorizedErrorConditions,
      "driving every fallback condition must leave the authorized condition set exactly as it was")
  }

  test("configuration is read once at construction and never re-read") {
    val conf = streamingConf()
    val policy = new StreamingShuffleFallbackPolicy(conf, newManualClock())
    assert(policy.streamingEnabledByConfig, "the fixture must start with streaming enabled")
    assert(!policy.killSwitchEngaged, "the fixture must start with the kill switch disengaged")
    assert(policy.administeredLinkCapacityBytesPerSecond.isEmpty,
      "the fixture must start with an uncapped link")

    conf.set(SHUFFLE_STREAMING_ENABLED, false)
    conf.set(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, AdministeredBandwidthMbps)

    assert(policy.streamingEnabledByConfig,
      "a live policy must hold the value it read at construction, not the configuration's latest")
    assert(!policy.killSwitchEngaged, "the kill switch must not engage without an executor restart")
    assert(policy.streamingActive,
      "a live policy must not change behaviour under the caller's feet")
    assert(policy.administeredLinkCapacityBytesPerSecond.isEmpty,
      "a live policy must not pick up a bandwidth cap administered after it was constructed")

    val restarted = new StreamingShuffleFallbackPolicy(conf, newManualClock())
    assert(restarted.killSwitchEngaged, "a restarted policy must observe the new configuration")
    assert(restarted.administeredLinkCapacityBytesPerSecond
        .contains(AdministeredBandwidthMbps.toLong * BytesPerMebibyte),
      "a restarted policy must convert the newly administered cap into bytes per second")
  }

  test("streaming shuffle configuration outside its documented range is refused when it is read") {
    val tooLargeBuffer = streamingConf().set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT,
      MaxBufferSizePercent + 1)
    val bufferFailure = intercept[IllegalArgumentException] {
      new StreamingShuffleFallbackPolicy(tooLargeBuffer, newManualClock())
    }
    assert(bufferFailure.getMessage.contains("The buffer size percent must be in [1, 50]."),
      s"the failure must quote the documented range but said: ${bufferFailure.getMessage}")

    val tooSmallThreshold = streamingConf().set(SHUFFLE_STREAMING_SPILL_THRESHOLD,
      MinSpillThresholdPercent - 1)
    val thresholdFailure = intercept[IllegalArgumentException] {
      new StreamingShuffleFallbackPolicy(tooSmallThreshold, newManualClock())
    }
    assert(thresholdFailure.getMessage.contains("The spill threshold must be in [50, 95]."),
      s"the failure must quote the documented range but said: ${thresholdFailure.getMessage}")

    val defaults = streamingConf()
    assert(defaults.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT) == DefaultBufferSizePercent,
      "the buffer budget must default to the documented share of executor memory")
    assert(defaults.get(SHUFFLE_STREAMING_SPILL_THRESHOLD) == DefaultSpillThresholdPercent,
      "the spill threshold must default to the documented utilisation")
    assert(new StreamingShuffleFallbackPolicy(defaults, newManualClock()).streamingActive,
      "a policy built from the documented defaults must be active")
  }
}
