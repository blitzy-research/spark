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

import org.apache.spark.{FetchFailed, LocalSparkContext, ShuffleDependency, SparkConf, SparkContext, SparkEnv, SparkFunSuite, SparkThrowableHelper, TaskFailedReason}
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_ENABLED, SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, SHUFFLE_STREAMING_SPILL_THRESHOLD, STAGE_MAX_CONSECUTIVE_ATTEMPTS, TASK_MAX_FAILURES}
import org.apache.spark.network.shuffle.protocol.streaming.StreamingShuffleMessage
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.ManualClock

/**
 * Tests the streaming shuffle's graceful-degradation contract: the four conditions on which it
 * stands down, the operator kill switch, and the one property that makes every path safe.
 *
 * That property is why this suite exists: '''there is no configuration, no failure and no resource
 * condition under which a job is left without a functioning shuffle implementation.''' Every
 * condition `StreamingShuffleFallbackPolicy` recognises routes to the same terminus as the kill
 * switch -- delegation to an internally held, unmodified `SortShuffleManager`. A trip costs
 * throughput and latency, never correctness and never the job, so zero regression is a structural
 * guarantee and the tests below are what make the structure observable.
 *
 * Each of the four conditions is asserted at its exact boundary:
 *
 *  - Consumer slowness: at least 2x behind the producer, continuously, for '''strictly longer'''
 *    than sixty seconds. Asserted from both sides, and a consumer that recovers clears the timer so
 *    the elapsed time cannot be re-used later.
 *  - Memory pressure: a '''partial''' buffer grant that eviction could not reverse. A satisfied
 *    grant does not trip, and a reservation of nothing cannot be short-granted at all.
 *  - Network saturation: utilisation '''strictly above''' ninety per cent of the administered link
 *    capacity. Exactly at the threshold is not saturation, and a link of unknown capacity is never
 *    described as saturated, which is also why no division by zero is reachable.
 *  - Protocol version mismatch: an '''explicit''' check on the version byte in the wire header,
 *    never an inference from a failed decode. The peek that reads it is non-consuming, so the same
 *    buffer still decodes afterwards.
 *
 * Plus the kill switch, `spark.shuffle.streaming.enabled`, which defaults to `false` and reaches
 * the identical terminus with no condition observed at all.
 *
 * Nothing here sleeps: every elapsed interval is an advance of an injected `ManualClock` walked to
 * the exact millisecond, which is what lets the suite assert a timer does '''not''' fire early, and
 * the one concurrent case is released from a barrier with a bounded timeout. The condition, metric
 * and error-condition sets are all closed, and the tests at the end assert degradation adds nothing
 * to any of them -- a fallback is reported through a boolean and a value from a sealed set rather
 * than by raising anything.
 */
class StreamingShuffleFallbackSuite
  extends SparkFunSuite
  with LocalSparkContext
  with StreamingShuffleTestHelper {

  import StreamingShuffleFallbackReason._
  import StreamingShuffleTestHelper._

  private val ShuffleId: Int = 0

  private val OtherShuffleId: Int = 1

  private val MapId: Long = 0L

  private val PartitionId: Int = 0

  /** Partitions the end-to-end kill-switch job groups into. */
  private val PartitionCount: Int = DefaultPartitionCount

  /**
   * Map outputs the coordinator fixtures declare.
   *
   * Wider than the shuffle-wide producer-liveness tolerance, so a case can lose one producer per
   * map output up to that bound and still have an output left to lose -- which is what makes the
   * shuffle-wide bound, rather than the per-output one, the thing being asserted.
   */
  private val ProducerCount: Int = 6

  /**
   * How long an end-to-end test waits for the listener bus to deliver a finished job's events.
   *
   * The bus delivers asynchronously, so a test that read a recorder without draining it first would
   * be asserting on whatever had arrived by then. Generous rather than tight, because it is only
   * ever paid when something has gone wrong.
   */
  private val ListenerDrainTimeoutMillis: Long = 60000L

  /**
   * A producer rate that is unambiguously producing.
   *
   * The deficit only holds while the producer is actually producing, which is what stops an idle
   * shuffle -- both rates zero -- from satisfying the comparison and standing streaming down for no
   * reason. Every slowness fixture therefore needs a strictly positive producer rate.
   */
  private val ProducerBytesPerSecond: Double = 1024.0d

  /**
   * A consumer rate exactly the tolerated factor behind the producer.
   *
   * Derived from the ratio rather than written as a literal, and deliberately sitting exactly ON
   * the boundary: the contract is "at least 2x slower", so a consumer precisely 2x behind is
   * already behind, and a fixture comfortably past the boundary would not prove that.
   */
  private val LaggingConsumerBytesPerSecond: Double =
    ProducerBytesPerSecond / ConsumerSlownessRatio

  /**
   * The link capacity every explicit-capacity saturation case measures against.
   *
   * One hundred, so that an egress figure equal to the trip percentage divides to exactly the same
   * double the policy compares against. The boundary case is then exact rather than approximate,
   * and "not at the threshold" is a real assertion instead of a rounding accident.
   */
  private val LinkCapacityBytesPerSecond: Double = 100.0d

  private val EgressAtSaturationThreshold: Double = LinkSaturationTripPercent.toDouble

  private val RequestedBufferBytes: Long = 1024L

  private val AdministeredBandwidthMbps: Int = 100

  /**
   * A protocol version this build cannot speak.
   *
   * Compatibility requires exact equality with the current revision, so the next value up is
   * incompatible by construction and stays incompatible when the current revision is bumped.
   */
  private val IncompatibleProtocolVersion: Byte =
    (StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION + 1).toByte

  /** Padding placed ahead of a framed message, to prove a peek honours the buffer's position. */
  private val FramePaddingBytes: Int = 3

  private val ConcurrentReaderCount: Int = 8

  private val LatchReadIterations: Int = 20000

  /**
   * A policy on which streaming is enabled and nothing has yet been observed.
   *
   * Each case builds its own, because the tripped state latches for the life of an instance and
   * sharing one would make the outcome of a test depend on the order the suite happened to run in.
   *
   * @param clock the injected time source, which is the only clock the policy reads
   * @return an untripped policy with streaming active
   */
  private def activePolicy(clock: ManualClock): StreamingShuffleFallbackPolicy = {
    new StreamingShuffleFallbackPolicy(streamingConf(), clock)
  }

  /**
   * Runs a body against a coordinator endpoint of its own, stopped afterwards whatever happens.
   *
   * The endpoint is constructed rather than registered, and its `RpcEnv` is a mock, because every
   * operation these cases drive is available as a direct method call: the escalation under test is
   * a decision the endpoint takes about state it holds, so routing it over a real message loop
   * would add a thread and a timeout without adding anything to what is being asserted. The reaper
   * timer is the one piece of real machinery involved, which is why the endpoint is always stopped.
   *
   * A shuffle-wide stand-down also asks the driver's map-output tracker to withdraw the shuffle's
   * registrations, and that step tolerates the absence of a live environment by design, so no
   * `SparkContext` is needed here either.
   *
   * @param conf configuration the endpoint reads once at construction
   * @param body what to run against it
   */
  private def withCoordinator(conf: SparkConf)(body: StreamingShuffleCoordinator => Unit): Unit = {
    val coordinator = new StreamingShuffleCoordinator(mock(classOf[RpcEnv]), conf, newManualClock())
    try {
      body(coordinator)
    } finally {
      coordinator.onStop()
    }
  }

  /**
   * Registers a shuffle on a coordinator and answers with its capability token.
   *
   * @param coordinator endpoint to register with
   * @param shuffleId shuffle to register
   * @param numMaps map outputs the shuffle declares
   * @return the token every later operation on that shuffle must present
   */
  private def registerShuffleFor(
      coordinator: StreamingShuffleCoordinator,
      shuffleId: Int,
      numMaps: Int = ProducerCount): String = {
    val grant = coordinator.registerShuffle(shuffleId, PartitionCount, numMaps, ProtocolVersion)
    assert(grant.isDefined, s"shuffle $shuffleId must have been registered")
    assert(grant.get.capabilityToken.nonEmpty, "the grant must carry a non-empty token")
    grant.get.capabilityToken
  }

  /**
   * Publishes one producer of one map output, so that an invalidation of it has something to
   * withdraw. Only a withdrawal that really happened is counted, which is what makes a registration
   * a precondition of the cases below rather than a detail of them.
   *
   * @param coordinator endpoint to register with
   * @param shuffleId shuffle the producer streams
   * @param token capability token of that shuffle
   * @param mapIndex map output the producer serves
   * @param attemptId task attempt id of the producer, which is what makes it a distinct generation
   */
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

  /**
   * Invalidates one producer generation the way a consumer whose connection timeout expired does.
   *
   * @param coordinator endpoint to drive
   * @param shuffleId shuffle the producer streamed
   * @param token capability token of that shuffle
   * @param mapIndex map output whose producer is lost
   * @param attemptId task attempt id of the lost generation
   * @param register whether to publish the generation first; false expresses a consumer retrying an
   *                 invalidation of a generation that has already gone
   * @return the epoch the coordinator reports afterwards
   */
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
   * The producer sample is applied first and the consumer sample second, because the deficit can
   * only be evaluated once both rates are known: the producer sample alone leaves the consumer rate
   * unset, and an unset rate is not evidence of a deficit.
   *
   * @param policy the policy to sample into
   * @param clock the clock whose current reading becomes the arming instant
   * @return the instant the timer armed at
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
   * Only the consumer side is resampled. The window is a property of the shuffle rather than of
   * either direction, so re-reporting one rate is enough to have the deficit re-evaluated, and
   * leaving the producer rate alone keeps the fixture honest about what changed.
   *
   * @param policy the policy to sample into
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

  /**
   * The same framed message with its protocol version byte overwritten.
   *
   * This is the only way to obtain a frame this build considers incompatible, because every message
   * it constructs stamps the current revision by definition. The byte sits immediately after the
   * framing prefix, and the offset is taken from the protocol's own constant rather than written as
   * a literal.
   *
   * @param version the version to stamp
   * @return the framed bytes, positioned at the start of the frame
   */
  private def framedMessageWithVersion(version: Byte): ByteBuffer = {
    val framed = framedMessage()
    val bytes = new Array[Byte](framed.remaining())
    framed.get(bytes)
    bytes(FrameTypePrefixLength) = version
    ByteBuffer.wrap(bytes)
  }

  /**
   * A framed message preceded by padding, positioned at the start of the frame.
   *
   * A peek that read from index zero rather than from the buffer's position would pass every test
   * written against an unpadded buffer and fail in service, where a frame arrives inside a larger
   * receive buffer. The padding is what makes the distinction visible.
   *
   * @param paddingBytes bytes of padding to place ahead of the frame
   * @return the padded buffer, positioned at the first byte of the frame
   */
  private def paddedFramedMessage(paddingBytes: Int): ByteBuffer = {
    val framed = framedMessage()
    val padded = ByteBuffer.allocate(paddingBytes + framed.remaining())
    padded.put(new Array[Byte](paddingBytes))
    padded.put(framed)
    padded.flip()
    padded.position(paddingBytes)
    padded
  }

  /**
   * The captured log events that are the unevaluable-saturation notice, and nothing else.
   *
   * Selected on two independent substrings rather than on one, so that an unrelated policy log line
   * that happens to mention saturation -- or one that happens to mention the bandwidth property --
   * cannot be counted as the notice and make a once-per-JVM assertion pass for the wrong reason.
   *
   * @param appender the appender the constructions under test logged through
   * @return the matching events, in the order they were emitted
   */
  private def saturationNotices(appender: LogAppender): Seq[LogEvent] = {
    appender.loggingEvents.filter { event =>
      val text = event.getMessage.getFormattedMessage
      text.contains(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS.key) && text.contains("saturation")
    }.toSeq
  }

  // -----------------------------------------------------------------------------------------------
  // Trip 1: the consumer held at least 2x behind the producer for longer than sixty seconds.
  //
  // The window is what makes this condition "sustained" rather than instantaneous. A momentary
  // stall is ordinary flow control that backpressure and spill already absorb, and it must not cost
  // a job its fast path -- so the boundary is asserted from both sides, and the recovery case is
  // asserted too, because a timer that armed and was never cleared would eventually trip a shuffle
  // whose consumer had long since caught up.
  // -----------------------------------------------------------------------------------------------

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

    // Partway through the window the consumer catches up. Parity is not a deficit at all, so the
    // timer must clear rather than merely pause.
    clock.advance(SustainedSlownessWindowMillis / 2L)
    policy.recordConsumerThroughput(ShuffleId, ProducerBytesPerSecond, clock.getTimeMillis())
    assert(policy.slownessArmedSinceMs(ShuffleId).isEmpty,
      "a consumer that keeps up must clear the sustained-slowness timer")
    assert(!policy.hasTripped, "recovery must not trip anything")

    // The deficit reappears much later. It has to open a NEW window: if the elapsed time before
    // recovery were still counted, this single sample would trip immediately, which is precisely
    // the defect the clearing behaviour exists to prevent.
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

    // An idle shuffle reports zero in both directions. Zero is not 2x slower than zero in any sense
    // that should cost a job its fast path, so the window must never even arm.
    policy.recordProducerThroughput(ShuffleId, 0.0d, clock.getTimeMillis())
    policy.recordConsumerThroughput(ShuffleId, 0.0d, clock.getTimeMillis())
    assert(policy.slownessArmedSinceMs(ShuffleId).isEmpty,
      "an idle shuffle must not arm the sustained-slowness window")
    advancePastSustainedSlownessWindow(clock)
    policy.recordConsumerThroughput(ShuffleId, 0.0d, clock.getTimeMillis())
    assert(!policy.hasTripped, "an idle shuffle must never trip consumer slowness")

    // A rate that is not a finite non-negative number is not evidence of anything. It is counted so
    // that "did not trip because the measurement was within tolerance" stays distinguishable from
    // "did not trip because there was no usable measurement", which look identical from hasTripped.
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

  // -----------------------------------------------------------------------------------------------
  // Trip 1, observed rather than measured: producer-liveness invalidations that keep repeating.
  //
  // The rate-based form of this condition needs sixty seconds of evidence, and the connection
  // timeout fires at five, so a producer is invalidated and its reduce attempt gone long before the
  // window can be evaluated. Each invalidation costs one recomputation, nothing about recomputing
  // changes what caused the timeout, and the scheduler abandons a stage after
  // `spark.stage.maxConsecutiveAttempts` -- so without a bound the streaming path can spend a job's
  // whole attempt budget and have it aborted, which is the one outcome graceful degradation exists
  // to make impossible. The driver is the only participant that sees every consumer's invalidations
  // across every attempt, so the bound lives on the coordinator, and both of its forms are asserted
  // here: the specific one (the same map output lost twice) and the safety net (losses spread one
  // per map output, which satisfies no per-output bound while spending an attempt each).
  // -----------------------------------------------------------------------------------------------

  test("the producer-liveness tolerances follow the scheduler's consecutive-attempt allowance") {
    // Stated as pure arithmetic first, because the whole point of deriving the bounds is that an
    // operator who raises their own tolerance for recomputation raises streaming's with it.
    Seq(
      (4, 2, 3), // the shipped default: two per map output, three per shuffle
      (5, 3, 4),
      (12, 10, 11)).foreach { case (allowance, expectedPerMap, expectedPerShuffle) =>
      assert(StreamingShuffleCoordinator.perMapProducerTimeoutTolerance(allowance) ===
          expectedPerMap,
        s"an allowance of $allowance must tolerate $expectedPerMap loss(es) of one map output")
      assert(StreamingShuffleCoordinator.shuffleProducerTimeoutTolerance(allowance) ===
          expectedPerShuffle,
        s"an allowance of $allowance must tolerate $expectedPerShuffle loss(es) per shuffle")
      assert(StreamingShuffleCoordinator.shuffleProducerTimeoutTolerance(allowance) >=
          StreamingShuffleCoordinator.perMapProducerTimeoutTolerance(allowance),
        "the shuffle-wide bound may never sit below the per-map-output one, or the safety net " +
          "would fire before the signal it exists to back up")
    }

    // And floored, so an installation that has tightened the allowance cannot reduce the tolerance
    // to a value that stands a shuffle down on its first ordinary producer failure -- which is the
    // fault the specified producer-failure flow exists to absorb rather than to degrade for.
    Seq(0, 1, 2, 3).foreach { tight =>
      assert(StreamingShuffleCoordinator.perMapProducerTimeoutTolerance(tight) >=
          StreamingShuffleCoordinator.MIN_PRODUCER_TIMEOUT_TOLERANCE,
        s"an allowance of $tight must still tolerate at least " +
          s"${StreamingShuffleCoordinator.MIN_PRODUCER_TIMEOUT_TOLERANCE} loss(es) of one output")
    }

    // The live endpoint reports what its configuration derives, read once at construction.
    withCoordinator(streamingConf().set(STAGE_MAX_CONSECUTIVE_ATTEMPTS, 12)) { coordinator =>
      assert(coordinator.producerTimeoutTolerancePerMap === 10,
        s"the endpoint must derive 10 from an allowance of 12 but derived " +
          s"${coordinator.producerTimeoutTolerancePerMap}")
      assert(coordinator.producerTimeoutToleranceForShuffle === 11,
        s"the endpoint must derive 11 from an allowance of 12 but derived " +
          s"${coordinator.producerTimeoutToleranceForShuffle}")
    }
  }

  test("losing the same map output twice to a liveness timeout stands the whole shuffle down") {
    withCoordinator(streamingConf()) { coordinator =>
      val shuffleId = 7101
      val token = registerShuffleFor(coordinator, shuffleId)
      assert(coordinator.producerTimeoutTolerancePerMap === 2,
        "this case is written against the shipped tolerance of two losses per map output")

      // The original attempt of map output zero is lost to the connection timeout. That is the
      // ordinary producer-failure flow and must NOT cost the shuffle its fast path.
      val original = timeOutProducer(coordinator, shuffleId, token, mapIndex = 0, attemptId = 100L)
      assert(original !== StreamingShuffleCoordinator.NO_EPOCH,
        "the invalidation must have withdrawn the registration and advanced the epoch")
      assert(coordinator.producerTimeoutCount(shuffleId, 0) === 1,
        s"one loss must be recorded but ${coordinator.producerTimeoutCount(shuffleId, 0)} were")
      assert(!coordinator.fallbackStateFor(shuffleId, token).fallenBack,
        "a single producer loss is recovered by recomputation, never by standing the shuffle down")

      // A consumer that retries the very same invalidation must not be able to inflate the tally:
      // the generation it names has already been withdrawn, so nothing is withdrawn again.
      timeOutProducer(coordinator, shuffleId, token, mapIndex = 0, attemptId = 100L,
        register = false)
      assert(coordinator.producerTimeoutCount(shuffleId, 0) === 1,
        "re-invalidating a generation already gone must not be counted a second time")
      assert(!coordinator.fallbackStateFor(shuffleId, token).fallenBack,
        "and it must not be able to stand the shuffle down either")

      // The recomputed attempt of the same map output is lost the same way. Recomputation is
      // therefore not converging, and this is the point at which streaming must yield.
      timeOutProducer(coordinator, shuffleId, token, mapIndex = 0, attemptId = 101L)
      assert(coordinator.producerTimeoutCount(shuffleId, 0) === 2,
        s"two losses must be recorded but ${coordinator.producerTimeoutCount(shuffleId, 0)} were")

      val declared = coordinator.fallbackStateFor(shuffleId, token)
      assert(declared.fallenBack,
        "the shuffle must have stood streaming down once the same map output was lost twice")
      assert(declared.reason.contains(StreamingShuffleFallbackReason.ConsumerTooSlow),
        s"the condition recorded must be the pacing one but was ${declared.reason}")
      assert(declared.detail.contains("map index 0") && declared.detail.contains("tolerance"),
        s"the record must name the observation that was made, but reads ${declared.detail}")
      assert(declared.detail.contains(STAGE_MAX_CONSECUTIVE_ATTEMPTS.key),
        s"and it must name the allowance the bound is derived from, but reads ${declared.detail}")
    }
  }

  test("liveness timeouts spread across map outputs stand the shuffle down at the wider bound") {
    withCoordinator(streamingConf()) { coordinator =>
      val shuffleId = 7102
      val token = registerShuffleFor(coordinator, shuffleId)
      val tolerance = coordinator.producerTimeoutToleranceForShuffle
      assert(tolerance === 3, "this case is written against the shipped shuffle-wide tolerance")

      // One loss per map output satisfies no per-output bound, so only the shuffle-wide count can
      // terminate this pattern -- and it must, because each loss still spends a stage attempt.
      (0 until tolerance - 1).foreach { mapIndex =>
        timeOutProducer(coordinator, shuffleId, token, mapIndex, attemptId = 200L + mapIndex)
        assert(coordinator.producerTimeoutCount(shuffleId, mapIndex) === 1,
          s"map output $mapIndex must have been lost exactly once")
        assert(!coordinator.fallbackStateFor(shuffleId, token).fallenBack,
          s"${mapIndex + 1} loss(es) spread across map outputs must stay inside the tolerance of " +
            s"$tolerance")
      }

      timeOutProducer(coordinator, shuffleId, token, mapIndex = tolerance - 1,
        attemptId = 200L + tolerance - 1)
      assert(coordinator.producerTimeoutTotal(shuffleId) === tolerance,
        s"the shuffle-wide tally must read $tolerance but read " +
          s"${coordinator.producerTimeoutTotal(shuffleId)}")
      val declared = coordinator.fallbackStateFor(shuffleId, token)
      assert(declared.fallenBack,
        s"$tolerance losses spread one per map output must stand the shuffle down, because each " +
          "one spends a stage attempt the scheduler will not give back")
      assert(declared.reason.contains(StreamingShuffleFallbackReason.ConsumerTooSlow),
        s"the condition recorded must be the pacing one but was ${declared.reason}")
    }
  }

  test("only a liveness timeout counts towards the stand down, and only an authorized one") {
    withCoordinator(streamingConf()) { coordinator =>
      val shuffleId = 7103
      val token = registerShuffleFor(coordinator, shuffleId)
      val foreign = registerShuffleFor(coordinator, shuffleId + 1)
      assert(foreign !== token, "the two shuffles must not share a token")

      // A producer withdrawing its own generation after its map task failed reports an incomplete
      // stream, and so does a consumer that saw the stream close early. Both are ordinary task
      // failure, which the unmodified scheduler retries; counting them would stand a shuffle down
      // for injected faults and user-code failures that have nothing to do with pacing.
      val uncounted = Seq(
        StreamingShuffleInvalidationReason.IncompleteStream,
        StreamingShuffleInvalidationReason.ChecksumMismatch,
        StreamingShuffleInvalidationReason.StaleEpoch,
        StreamingShuffleInvalidationReason.Unknown)
      uncounted.zipWithIndex.foreach { case (reason, index) =>
        val attemptId = 300L + index
        registerProducerFor(coordinator, shuffleId, token, mapIndex = 0, attemptId)
        coordinator.invalidateProducer(shuffleId, token,
          StreamingShuffleProducerGeneration(mapIndex = 0, mapId = attemptId, attemptId),
          reason, s"an invalidation reported as $reason")
        assert(coordinator.producerTimeoutCount(shuffleId, 0) === 0,
          s"$reason must not be counted as a producer-liveness loss")
      }
      assert(coordinator.producerTimeoutTotal(shuffleId) === 0,
        "no shuffle-wide loss may be recorded for reasons that are not liveness timeouts")
      assert(!coordinator.fallbackStateFor(shuffleId, token).fallenBack,
        "and the shuffle must still be streaming, however many of them arrive")

      // An unauthorized invalidation is refused before anything is recorded, so a stranger cannot
      // drive a shuffle onto the sort-based path by repeating one.
      registerProducerFor(coordinator, shuffleId, token, mapIndex = 1, attemptId = 400L)
      (1 to coordinator.producerTimeoutToleranceForShuffle + 1).foreach { _ =>
        assert(coordinator.invalidateProducer(shuffleId, foreign,
            StreamingShuffleProducerGeneration(mapIndex = 1, mapId = 400L, taskAttemptId = 400L),
            StreamingShuffleInvalidationReason.ConnectionTimeout, "unauthorized") ===
            StreamingShuffleCoordinator.NO_EPOCH,
          "an unauthorized invalidation must be refused without disclosing the epoch")
      }
      assert(coordinator.producerTimeoutCount(shuffleId, 1) === 0,
        "an unauthorized invalidation must not be counted")
      assert(!coordinator.fallbackStateFor(shuffleId, token).fallenBack,
        "and it must not be able to stand the shuffle down")

      // The authorized timeout of the same generation is counted, which is what makes every
      // assertion above a statement about the guard rather than about an inert code path.
      timeOutProducer(coordinator, shuffleId, token, mapIndex = 1, attemptId = 400L,
        register = false)
      assert(coordinator.producerTimeoutCount(shuffleId, 1) === 1,
        "the authorized timeout of a registered generation must be counted")
    }
  }

  // Trip 2: memory pressure prevented a buffer allocation, so streaming risks exhausting memory.
  // The signal is a PARTIAL GRANT, which is Spark's own idiom rather than anything invented here:
  // MemoryConsumer.acquireMemory answers with the amount it could grant, which may be less than the
  // request, and Spark's spillable collections treat exactly that as the cue to spill. This policy
  // is the escalation rather than the first responder, so a caller reports here only once eviction
  // has been attempted and could not free enough room.

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

  // Trip 3: network utilisation above ninety per cent of the administered link capacity. Two
  // independent properties are asserted. The comparison is STRICTLY greater than, so exactly at the
  // threshold is tolerated rather than tripped; and a capacity that is zero, negative or not finite
  // is not a capacity at all and is refused before any division, which is why no division by zero
  // and no NaN comparison is reachable however the method is called. Ninety per cent is the
  // SATURATION TRIP while eighty per cent is the token bucket's bandwidth ceiling -- one is a rate
  // limit streaming imposes on itself while it keeps streaming, the other is where it stops -- and
  // the last case in this group exists to keep them from being conflated.

  test("link utilisation exactly at the saturation threshold does not trip") {
    val policy = activePolicy(newManualClock())

    // Repeated as many times as a sustained run would need, so that "exactly ninety does not trip"
    // is established against the sustained rule rather than merely against the first sample of it.
    (0L until StreamingShuffleFallbackPolicy.SATURATION_SUSTAINED_SAMPLES + 1L).foreach { _ =>
      policy.recordLinkUtilization(EgressAtSaturationThreshold, LinkCapacityBytesPerSecond)
    }
    assert(policy.consecutiveSaturatedSampleCount == 0L,
      "a sample at exactly the threshold is not over capacity, so no run may be counted, but the " +
        s"run reads ${policy.consecutiveSaturatedSampleCount}")

    assert(!policy.hasTripped,
      "utilisation of exactly ninety per cent must not trip; the comparison is strictly greater")
    assert(policy.trippedReason.isEmpty, "no reason may be reported at the tolerated threshold")
    assert(policy.unevaluableSampleCount == 0L,
      "a sample with a known capacity must be evaluated rather than refused")
    assert(policy.streamingActive, "streaming must remain active at the tolerated threshold")
    assert(!policy.shouldDelegateToSortShuffle,
      "utilisation at the threshold must leave the streaming path in service")
  }

  test("link utilisation strictly above the saturation threshold trips") {
    val clock = newManualClock()
    val policy = activePolicy(clock)

    // Sustained, not instantaneous, and the reason is the pacing bucket rather than caution. A
    // bucket must be able to admit one maximum-sized block or it would refuse every block forever,
    // so its burst allowance is at least one frame however small its paced share is -- and a bucket
    // that starts full legitimately delivers that burst inside one sampling interval, which across
    // several concurrent shuffles sums to a multiple of the administered capacity for exactly that
    // interval. Tripping on the first sample turned that legal burst into a stand-down: egress read
    // at nearly twice the administered capacity with not one stream throttled, every live producer
    // invalidated, and over a quarter of the shuffle's records written again by the recomputation.
    // A burst clears on the next sample; a saturated link does not, and still trips within a few
    // seconds.
    var sample = 1L
    while (sample < StreamingShuffleFallbackPolicy.SATURATION_SUSTAINED_SAMPLES) {
      policy.recordLinkUtilization(EgressAtSaturationThreshold + 1.0d, LinkCapacityBytesPerSecond)
      assert(!policy.hasTripped,
        s"sample $sample is over capacity but short of a sustained run, so it must not trip")
      assert(policy.consecutiveSaturatedSampleCount == sample,
        s"the run must stand at $sample but reads ${policy.consecutiveSaturatedSampleCount}")
      assert(policy.streamingActive, s"streaming must remain active through sample $sample")
      sample += 1L
    }

    policy.recordLinkUtilization(EgressAtSaturationThreshold + 1.0d, LinkCapacityBytesPerSecond)

    assert(policy.hasTripped, "utilisation past the tolerated share must trip")
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

    // Absence is the unlimited state, encoded as absence rather than as a sentinel. A sentinel of
    // zero would make every sample infinitely saturated and would fall back permanently on the
    // default configuration, which is the single worst outcome available to this condition.
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
    val policy = new StreamingShuffleFallbackPolicy(conf, newManualClock())
    val capacityBytesPerSecond = AdministeredBandwidthMbps.toLong * BytesPerMebibyte

    assert(policy.administeredLinkCapacityBytesPerSecond.contains(capacityBytesPerSecond),
      "the administered cap must be converted from MB/s into bytes per second")

    val capacity = capacityBytesPerSecond.toDouble
    policy.recordLinkUtilization(capacity * LinkSaturationTripPercent.toDouble / 100.0d)
    assert(!policy.hasTripped,
      "the administered capacity must tolerate utilisation at the threshold")

    // A run of samples, because a single one is a pacing bucket's legal burst: see
    // StreamingShuffleFallbackPolicy.SATURATION_SUSTAINED_SAMPLES.
    (1L to StreamingShuffleFallbackPolicy.SATURATION_SUSTAINED_SAMPLES).foreach { _ =>
      policy.recordLinkUtilization(capacity)
    }
    assert(policy.hasTripped, "a fully saturated administered link must trip")
    assert(policy.trippedReason.contains(NetworkSaturation),
      s"the reported reason must be NetworkSaturation but was ${policy.trippedReason}")
    assert(policy.shouldDelegateToSortShuffle, "the terminus is the same for an administered link")
  }

  test("the unevaluable saturation notice is stated once per jvm and never repeated") {
    // The latch is process wide by design, so it has to be returned to whatever this JVM found it
    // at: this suite must not decide, by running order, what a later suite observes.
    val latch = StreamingShuffleFallbackPolicy.saturationNoticeEmitted
    val latchedOnEntry = latch.get()
    try {
      latch.set(false)

      // The other path, asserted first, because "emitted once" is worth nothing unless there is a
      // configuration under which it is not emitted at all. Two of them: a link with an
      // administered capacity has an evaluable saturation condition and nothing to report, and a
      // policy on which streaming is switched off is not going to evaluate any condition whatever.
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

      // Now the branch itself: streaming enabled, no administered capacity, so three conditions are
      // live and the fourth is inert. That is a legitimate configuration and the operator is told,
      // exactly once.
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
        // constructs -- describe the same executor configuration and must add nothing. Each is
        // shown to be in the very state that produced the notice, so silence is suppression and
        // not a differently configured instance that had nothing to report.
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

      // What the one notice says. It is a notice and not a warning, because an uncapped link is a
      // legitimate configuration; it names the property that would make the condition evaluable;
      // and it says plainly that the other three conditions are unaffected, so an operator reading
      // it does not conclude that fallback as a whole is off.
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
  // from a failed decode. The wire header carries a protocol version byte for exactly this purpose,
  // so an executor rolled to a different revision is recognised BEFORE any attempt to interpret its
  // frames and degrades deterministically instead of misreading bytes. Two further properties are
  // asserted: the peek that reads the byte does not consume the buffer, so the same buffer still
  // decodes afterwards; and a frame too short to carry a version is reported as the framing fault
  // it is rather than mislabelled a version mismatch, because truncation is recovered by checksum
  // verification and retransmission.

  test("only the current streaming shuffle protocol version is compatible") {
    // The whole wire contract is compared with the encoder here, in one call, so a layout change
    // fails with a message naming the field that moved rather than being absorbed silently by an
    // assertion built on the drifted value. The literals themselves live in the shared fixtures and
    // are deliberately independent of production.
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

    // The layout the version byte opens, stated as the sum of its parts rather than as a literal so
    // that this assertion describes the header rather than merely echoing a number.
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

    // The point of a non-destructive read is that compatibility can be settled BEFORE committing to
    // a decode. Decoding the very same buffer afterwards is what proves the peek left it usable.
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

    // The throwing variant exists for callers already committed to a decode, and it signals with an
    // IllegalArgumentException rather than an error precisely because a mismatch is recoverable:
    // the caller has to be able to catch it and stand streaming down. The policy above deliberately
    // does NOT go through it, because an exception used as a predicate is the inference this design
    // avoids.
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

    // Truncation is recovered by checksum verification and retransmission. Quietly turning it into
    // a fallback would hide a real fault behind a performance regression, so the failure is allowed
    // to propagate and the policy must be left exactly as it was.
    intercept[IllegalArgumentException] {
      policy.checkProtocolVersion(truncated)
    }

    assert(!policy.hasTripped, "truncation must not be mislabelled as a version mismatch")
    assert(policy.trippedReason.isEmpty, "a framing fault must leave no fallback reason behind")
    assert(policy.streamingActive, "a framing fault must leave streaming active")
    assert(!policy.shouldDelegateToSortShuffle,
      "a framing fault must leave the streaming path in service")
  }

  // The operator kill switch, the lower rung of the two-tier activation model. Tier 1,
  // spark.shuffle.manager=streaming, decides which manager class Spark instantiates; tier 2,
  // spark.shuffle.streaming.enabled, gates the behaviour of the class tier 1 selected and defaults
  // to false. While it is false the streaming manager forwards every service-provider call verbatim
  // to its internal delegate, built as new SortShuffleManager(conf), which is what lets an operator
  // restore sort behaviour without redeploying a different manager class -- and it is the SAME
  // TERMINUS as all four trips.

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
      // Both are stopped, and stopping the gated-off manager must be safe even though it built
      // nothing streaming: an executor that never streamed must not be asked to tear down a
      // subsystem it never created.
      manager.stop()
      delegate.stop()
    }
  }

  test("a job completes on the sort based path while the kill switch is engaged") {
    // The baseline is captured first, in a context of its own that is stopped before this one
    // starts, because Spark permits a single context per JVM. It is the operational definition of a
    // working shuffle used throughout these suites: the output a stock sort-based application
    // produces from the same input.
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

  // Latching, and the diagnostics that survive it. Once tripped an instance stays tripped until
  // reset, because streaming and sort-based shuffle cannot both own one shuffle and oscillating
  // back mid-shuffle would mean two producers of the same data. Latching also makes the decision
  // monotone, which is what allows the hot-path read to be a single atomic load with no lock, no
  // allocation and no clock read; reset exists for tests and for reusing an instance, never as a
  // runtime recovery path.

  test("the tripped state latches on the first condition observed") {
    val clock = newManualClock()
    val policy = activePolicy(clock)

    // A run of samples, since one is a pacing bucket's legal burst rather than a saturated link.
    (1L to StreamingShuffleFallbackPolicy.SATURATION_SUSTAINED_SAMPLES).foreach { _ =>
      policy.recordLinkUtilization(LinkCapacityBytesPerSecond, LinkCapacityBytesPerSecond)
    }
    assert(policy.trippedReason.contains(NetworkSaturation), "the first condition must be recorded")
    val firstTrippedAt = policy.trippedAtTimeMillis
    assert(firstTrippedAt.contains(clock.getTimeMillis()), "the first trip must be stamped")

    // Later conditions are frequently consequences of the first, so the reported reason must stay
    // the one that actually came first -- and the trip instant must not be restamped either.
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
    assert(policy.hasTripped, "the fixture must have tripped before reset is exercised")
    assert(policy.trackedShuffleCount == 2, "two shuffles must have been sampled")
    assert(policy.unevaluableSampleCount == 1L, "one un-evaluable sample must have been counted")

    policy.reset()

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

    // One thread trips the policy while the rest read it as fast as they can. The reads are checked
    // pairwise in both orders against the one invariant a monotone latch must satisfy: a reason,
    // once present, stays present, so a reason observed before a subsequent hasTripped read implies
    // that read is true, and a hasTripped read of true implies a reason on any read after it. A
    // torn state would break one of the two.
    //
    // That every reader finishes at all is the operational statement about locking: the whole run
    // is released from one barrier and collected under a bounded deadline, so a read that blocked
    // behind the writer's update would fail this test by timing out rather than by asserting.
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

  // The central assertion: every path terminates in a working shuffle. It is asserted over the
  // complete set of paths rather than one at a time, so a fifth path added later without a terminus
  // fails here. No configuration, no failure and no resource condition leaves a job without a
  // functioning shuffle implementation, which is how zero regression is guaranteed structurally
  // rather than merely tested for.

  test("every fallback condition and the kill switch reach the policy's sort shuffle terminus") {
    // What is asserted here is the POLICY's verdict, which is one half of the guarantee. The other
    // half -- that a live StreamingShuffleManager acts on that verdict by routing every
    // service-provider call to its internal SortShuffleManager, and that a real shuffle registered
    // afterwards produces the sort-based baseline's output -- is asserted against a live manager by
    // "every fallback condition makes a live manager delegate every service provider call" in the
    // manager suite, and against a live job mid-write by the case further down this file. Naming
    // all three is what stops this one from being read as more than it proves.
    val drivers: Seq[(StreamingShuffleFallbackReason,
        (StreamingShuffleFallbackPolicy, ManualClock) => Unit)] = Seq(
      ConsumerTooSlow -> ((policy, clock) => driveSustainedConsumerSlowness(policy, clock)),
      MemoryPressure -> ((policy, _) => policy.recordAllocationGrant(RequestedBufferBytes, 0L)),
      NetworkSaturation -> ((policy, _) =>
        // A run of samples: one over-capacity sample is a pacing bucket's legal burst, so the
        // policy requires the run before it calls the link saturated.
        (1L to StreamingShuffleFallbackPolicy.SATURATION_SUSTAINED_SAMPLES).foreach { _ =>
          policy.recordLinkUtilization(LinkCapacityBytesPerSecond, LinkCapacityBytesPerSecond)
        }),
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
   *
   * Three facts are collected and each answers a different question. The COUNTED failures answer
   * whether a stand-down spent one of the task attempts the master permits -- it must not, which is
   * the whole of the finding. The FETCH failures answer whether recovery came through the one
   * scheduler-facing signal this subsystem is allowed to use. And the per-stage submission counts
   * answer whether the map stage was genuinely recomputed, which is what distinguishes a mid-write
   * stand-down from a preflight delegation that never needed recomputing at all.
   *
   * Every callback is synchronized, because the listener bus delivers on its own thread while the
   * test thread reads.
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

    /** Failures the scheduler charged against `spark.task.maxFailures`. */
    def countedFailures: Seq[TaskFailedReason] = synchronized(counted.toSeq)

    /** Fetch failures, which the scheduler resolves by recomputing rather than by counting. */
    def fetchFailures: Seq[FetchFailed] = synchronized(fetched.toSeq)

    /** Submissions of the busiest stage, which is one more than the times it was recomputed. */
    def maxStageSubmissions: Int = synchronized {
      if (submissions.isEmpty) 0 else submissions.values.max
    }
  }
  test("a job completes under a one-failure master when each condition trips mid-write") {
    // The claim, and why a master that permits exactly one task failure is the setting that proves
    // it: a mid-write trip cannot delegate, because the records already framed cannot be re-driven
    // into another writer, so the map output has to be produced again. If the mechanism for that
    // were a task failure it would consume the single attempt `local[n]` permits and abort the job.
    // Streaming standing down would then break the job it exists to accelerate, which is the exact
    // opposite of graceful degradation.
    //
    // So each of the four conditions is driven ON THE EXECUTOR, from inside a map function running
    // while the streaming writer is pulling records from it -- a genuine mid-write trip through the
    // live policy the production code consults -- and the job must still produce the sort-based
    // baseline's output.
    val baseline = sortBaselineGroupedOutput(numPartitions = PartitionCount)
    assert(baseline.nonEmpty, "the sort-based baseline must produce output to compare against")

    val conf = withLocalMaster(streamingConf(), "streaming-shuffle-midwrite-standdown")
      // Explicit rather than relied upon. A plain local[n] master already fixes this at one, and
      // stating it here is what makes the test's premise visible instead of incidental.
      .set(TASK_MAX_FAILURES, 1)
      // Recovery is by fetch failure, and a fetch failure is reported per map output that cannot be
      // found. A stood-down map stage therefore takes as many resubmissions as it has void outputs,
      // which is a property of the recovery mechanism rather than of this feature; the allowance is
      // raised so the test measures the feature and not the default.
      .set(STAGE_MAX_CONSECUTIVE_ATTEMPTS, 2 * PartitionCount + 4)
    sc = new SparkContext(conf)

    val manager = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]
    assert(manager.streamingFallbackPolicy.streamingActive,
      "streaming must be in service before the first condition is driven")

    StreamingShuffleFallbackReason.all.foreach { reason =>
      // Each condition gets a shuffle of its own, and the executor's policy is returned to service
      // between them: the latch is monotone by design, so a second condition driven against a
      // tripped policy would degrade at preflight and never reach the mid-write path under test.
      manager.streamingFallbackPolicy.reset()
      assert(manager.streamingFallbackPolicy.streamingActive,
        s"streaming must be back in service before $reason is driven")

      val holder = new MidWriteTripHolder(reason)
      val shuffled = midWriteTrippingWorkload(sc, PartitionCount, holder)
      // Set after the graph is built and before the action, so the closure carries the real shuffle
      // id: task closures are serialized when the job is submitted, not when the RDD is defined.
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

      // The three properties that distinguish this from a preflight delegation, and that together
      // are the whole of what the finding asked to be proven.
      //
      // 1. NO task failure was counted. This is the claim: a stand-down completes its attempt, so
      //    the one failure this master permits was never spent and the job could not abort.
      assert(recorder.countedFailures.isEmpty,
        s"a stand-down for $reason must count no task failure, yet the run reported " +
          s"${recorder.countedFailures.size}: " +
          recorder.countedFailures.take(3).map(_.toErrorString).mkString("[", ", ", "]"))
      // 2. Recovery never charged the scheduler, and it never thrashed. Three routes exist, all
      //    three are sanctioned, and which one a run takes depends on how far the map stage had got
      //    when the condition was driven -- so the assertion states what all three guarantee rather
      //    than picking one of them:
      //      - the tripping attempt reconstructs the records it had already framed and rewrites the
      //        whole attempt through the sort-based delegate, so nothing is missing and the one
      //        submission suffices;
      //      - a producer that had ALREADY registered streamed output has that registration
      //        withdrawn on the driver, so the map stage reports itself unavailable and is
      //        resubmitted for exactly the partitions withdrawn, served from the sort-based
      //        delegate, before any consumer has read them;
      //      - an attempt that could not rewrite its output at all completes with a void status,
      //        which a consumer turns into a fetch failure -- the one scheduler-facing signal this
      //        subsystem uses, and one the scheduler answers by recomputing, never by counting.
      //    A bound of one resubmission per map partition is what separates all three from the
      //    failure mode this finding was about: a stage resubmitted over and over until the
      //    stage-attempt limit aborts the job.
      assert(recorder.maxStageSubmissions <= 1 + PartitionCount,
        s"a stand-down for $reason must be recovered within one resubmission per map partition, " +
          s"yet the busiest stage of a $PartitionCount partition shuffle was submitted " +
          s"${recorder.maxStageSubmissions} time(s), with ${recorder.fetchFailures.size} fetch " +
          "failure(s) reported")
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Closed sets: the conditions, the metrics, and what a fallback is allowed to raise. A fifth
  // condition would be a fifth way for streaming to abandon the fast path, and every such way has
  // to be specified, tested and documented before it can be trusted; a fifth metric would be an
  // operator-facing surface nobody agreed to; and a fallback raises nothing at all, being reported
  // through a boolean and a value from a sealed set, which is why it needs no error condition.
  // -----------------------------------------------------------------------------------------------

  test("the fallback condition set is closed at exactly four conditions") {
    val all = StreamingShuffleFallbackReason.all

    assert(all.size == 4,
      s"the specification enumerates four conditions but the set holds ${all.size}")
    assert(all.distinct.size == all.size, "no condition may appear in the set twice")
    assert(all.toSet == Set[StreamingShuffleFallbackReason](
        ConsumerTooSlow, MemoryPressure, NetworkSaturation, ProtocolVersionMismatch),
      s"the set must hold exactly the four specified conditions but held $all")

    // The reason crosses a process boundary as its stable name rather than as the object, so the
    // name has to round-trip and an unrecognised one has to be refused at the boundary rather than
    // stored.
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

    // The error catalogue, read as the catalogue rather than inferred from the absence of a throw.
    // Three conditions are authorized under this feature's prefix and all three describe a WIRE
    // fault -- a block whose checksum did not verify, a block out of sequence, a frame of a type
    // the peer should not have sent. None of them describes a degradation, and that is the claim:
    // standing streaming down is reported through `hasTripped` and a value from a sealed set, so it
    // needs no condition of its own. Comparing the set exactly is what makes the claim checkable:
    // an invented fourth condition, or one for a fallback reason, fails here whether or not
    // anything ever throws it.
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
    // No condition names a degradation. Checked against the sealed set itself rather than against a
    // list of words, so a fifth reason added later is covered without this test being revisited.
    StreamingShuffleFallbackReason.all.foreach { reason =>
      val named = catalogued.filter(_.toUpperCase(Locale.ROOT).contains(
        reason.toString.toUpperCase(Locale.ROOT)))
      assert(named.isEmpty,
        s"$reason is a degradation and must be reported through the sealed set rather than as an " +
          s"error condition, yet the catalogue held ${named.mkString("[", ", ", "]")}")
    }

    // Every condition is driven on one policy. None of these calls may raise, which is the runtime
    // half of the same claim: the catalogue has nothing for a degradation AND nothing is thrown.
    val clock = newManualClock()
    val policy = activePolicy(clock)
    driveSustainedConsumerSlowness(policy, clock)
    policy.recordAllocationGrant(RequestedBufferBytes, 0L)
    policy.recordLinkUtilization(LinkCapacityBytesPerSecond, LinkCapacityBytesPerSecond)
    assert(!policy.checkProtocolVersion(IncompatibleProtocolVersion),
      "the incompatible peer must be reported by return value rather than by an exception")
    assert(policy.hasTripped, "the fixture must have latched")
    assert(policy.trippedReason.contains(ConsumerTooSlow),
      s"the first condition driven must be the one reported but ${policy.trippedReason} was")

    assert(streamingShuffleMetricNames() == expectedMetrics,
      s"driving every fallback condition must publish no further metric but published " +
        s"${streamingShuffleMetricNames()}")
    // And the catalogue is unchanged by having driven them, which is what closes the loop: nothing
    // a degradation does adds a condition.
    assert(SparkThrowableHelper.errorReader.errorInfoMap.keys
        .filter(_.startsWith(StreamingShuffleConditionPrefix)).toSet == AuthorizedErrorConditions,
      "driving every fallback condition must leave the authorized condition set exactly as it was")
  }

  // Configuration: read once, held immutably, and validated when it is read. Reading every value at
  // construction and never consulting the configuration again is what makes "streaming shuffle
  // configuration changes require an executor restart" true by construction rather than by note,
  // and it is why there is no dynamic-reconfiguration path to test.

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
    // The typed entries carry their own validators, so a misconfiguration is refused at the point
    // the value is read -- which is construction -- rather than degrading quietly into a fallback.
    // Being refused loudly is the correct behaviour here: an operator who mistyped a percentage
    // needs to be told, not to discover months later that streaming silently never engaged.
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
