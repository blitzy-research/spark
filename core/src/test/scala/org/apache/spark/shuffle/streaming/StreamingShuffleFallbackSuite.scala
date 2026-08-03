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

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_ENABLED, SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, SHUFFLE_STREAMING_SPILL_THRESHOLD}
import org.apache.spark.network.shuffle.protocol.streaming.StreamingShuffleMessage
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.util.ManualClock

/**
 * Tests the streaming shuffle's graceful-degradation contract: the four conditions on which it
 * stands down, the operator kill switch, and the one property that makes the whole subsystem safe
 * to ship.
 *
 * That property is worth stating before the tests that check it, because it is the reason this
 * suite exists: '''there is no configuration, no failure and no resource condition under which a
 * job is left without a functioning shuffle implementation.''' Every condition
 * `StreamingShuffleFallbackPolicy` recognises routes to the same terminus as the kill switch --
 * delegation to an internally held, completely unmodified `SortShuffleManager`. A trip costs
 * throughput and latency. It never costs correctness, and it never costs the job. Zero regression
 * is therefore a structural guarantee rather than a hope, and the tests below are what make the
 * structure observable.
 *
 * ==What is asserted, and how==
 *
 * Four conditions, each at its exact boundary:
 *
 *  - Consumer slowness: the consumer held at least 2x behind the producer, continuously, for
 *    '''strictly longer''' than sixty seconds. Both halves are asserted -- that it does not fire
 *    one millisecond early, that it does not fire at exactly the window, that it does fire one
 *    millisecond past it, and that a consumer which recovers clears the timer so the elapsed time
 *    cannot be re-used later.
 *  - Memory pressure: a '''partial''' buffer grant that eviction could not reverse. A satisfied
 *    grant does not trip, and a reservation of nothing cannot be short-granted so it is not
 *    evidence of anything.
 *  - Network saturation: utilisation '''strictly above''' ninety per cent of the administered link
 *    capacity. Exactly at the threshold is not saturation, and a link whose capacity is unknown is
 *    never described as saturated -- which is also why no division by zero is reachable.
 *  - Protocol version mismatch: an '''explicit''' compatibility check on the version byte the wire
 *    header carries, never an inference from a failed decode. The peek that reads that byte is
 *    non-consuming, so the very same buffer still decodes afterwards.
 *
 * Plus the kill switch, `spark.shuffle.streaming.enabled`, which defaults to `false` and reaches
 * the identical terminus without any condition having been observed at all.
 *
 * ==Determinism==
 *
 * Nothing here sleeps. Every elapsed interval is an advance of an injected `ManualClock`, and every
 * timing boundary is walked to the exact millisecond, which is what lets this suite assert that a
 * timer does '''not''' fire early -- an assertion a wall-clock test cannot make at all. The one
 * concurrent case is released from a barrier with a bounded timeout rather than from a sleep.
 *
 * ==Scope==
 *
 * The condition set, the metric set and the error-condition set are all closed, and the tests at
 * the end of this suite assert that degradation adds nothing to any of them: no fifth fallback
 * condition, no fifth metric, and no error condition at all, because a fallback is reported through
 * a boolean and a value from a sealed set rather than by raising anything.
 */
class StreamingShuffleFallbackSuite
  extends SparkFunSuite
  with LocalSparkContext
  with StreamingShuffleTestHelper {

  import StreamingShuffleFallbackReason._
  import StreamingShuffleTestHelper._

  /** Shuffle id every single-shuffle fixture in this suite samples under. */
  private val ShuffleId: Int = 0

  /** A second shuffle id, for the cases that must show sampling state is held per shuffle. */
  private val OtherShuffleId: Int = 1

  /** Map id stamped into the framed messages this suite builds. */
  private val MapId: Long = 0L

  /** Partition id stamped into the framed messages this suite builds. */
  private val PartitionId: Int = 0

  /** Sequence number stamped into the framed messages this suite builds. */
  private val SequenceNumber: Long = 0L

  /** Partitions the end-to-end kill-switch job groups into. */
  private val PartitionCount: Int = DefaultPartitionCount

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

  /** Egress sitting exactly at the saturation threshold, which must not trip. */
  private val EgressAtSaturationThreshold: Double = LinkSaturationTripPercent.toDouble

  /** Bytes a buffer reservation asks for in the memory-pressure cases. */
  private val RequestedBufferBytes: Long = 1024L

  /** An administered bandwidth cap, in MB/s, for the cases that need saturation to be evaluable. */
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

  /** Readers used by the concurrent latch case, alongside the one thread that trips the policy. */
  private val ConcurrentReaderCount: Int = 8

  /** Reads each concurrent reader performs, enough to straddle the trip on any scheduler. */
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

  /**
   * Drives the consumer-slowness condition all the way to a trip.
   *
   * @param policy the policy to drive
   * @param clock the clock to advance past the sustained window
   */
  private def driveSustainedConsumerSlowness(
      policy: StreamingShuffleFallbackPolicy,
      clock: ManualClock): Unit = {
    armConsumerSlowness(policy, clock)
    advancePastSustainedSlownessWindow(clock)
    resampleConsumerSlowness(policy, clock)
  }

  /**
   * A framed streaming shuffle message: the one-byte type discriminator followed by the encoded
   * body.
   *
   * Built from a real message rather than from hand-written bytes, so the header layout under test
   * is the production one and cannot drift from it.
   *
   * @return the framed bytes, positioned at the start of the frame
   */
  private def framedMessage(): ByteBuffer = {
    ack(ShuffleId, MapId, PartitionId, SequenceNumber, NothingConsumedPosition).toByteBuffer()
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

    // One millisecond short of the window: 59999 ms of deficit is not sixty seconds of deficit.
    advanceJustBeforeSustainedSlownessWindow(clock)
    resampleConsumerSlowness(policy, clock)
    assert(clock.getTimeMillis() - armedAt == JustBeforeSustainedSlownessMillis,
      "the fixture must place the sample exactly one millisecond short of the window")
    assert(!policy.hasTripped,
      "a deficit held for less than the window must not trip; 59999 ms is not more than 60000 ms")

    // Exactly at the window. The contract is strictly greater than, so this is still not a trip:
    // held for exactly the window is not yet held for longer than it.
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

    // And the re-armed window behaves exactly like the first one: it trips once exceeded.
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
  // Trip 2: memory pressure prevented a buffer allocation, so streaming risks exhausting memory.
  //
  // The signal is a PARTIAL GRANT, which is Spark's own established idiom for memory pressure and
  // not anything invented for this subsystem: MemoryConsumer.acquireMemory answers with the amount
  // it was actually able to grant, and that amount may be smaller than the request. Spark's own
  // spillable collections treat exactly that outcome as the cue to spill. The policy is the
  // escalation rather than the first responder, so a caller reports here only once eviction has
  // been attempted and could not free enough room -- streaming trades memory for latency, and when
  // the memory is not there the trade is off.
  // -----------------------------------------------------------------------------------------------

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

    // Zero granted is the extreme of the same signal, not a different one: there is no separate
    // condition for "granted nothing", because the cause and the response are identical.
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

    // A request for nothing cannot be short-granted, so it is no evidence of pressure either way.
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

  // -----------------------------------------------------------------------------------------------
  // Trip 3: network utilisation above ninety per cent of the administered link capacity.
  //
  // Two independent properties are asserted here, and both matter. The comparison is STRICTLY
  // greater than, so exactly at the threshold is tolerated rather than tripped. And a capacity that
  // is zero, negative or not a finite number is not a capacity at all: such a sample is refused
  // before any division is attempted, which is why no division by zero and no NaN comparison is
  // reachable from this method however it is called.
  //
  // Note also that ninety per cent is the SATURATION TRIP and eighty per cent is the token bucket's
  // bandwidth ceiling. They are different constants doing different jobs -- one is a rate limit
  // streaming imposes on itself while it continues to stream, the other is the point at which it
  // stops altogether -- and the last case in this group exists to keep them from being conflated.
  // -----------------------------------------------------------------------------------------------

  test("link utilisation exactly at the saturation threshold does not trip") {
    val policy = activePolicy(newManualClock())

    policy.recordLinkUtilization(EgressAtSaturationThreshold, LinkCapacityBytesPerSecond)

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

    // Each of these would be a division by zero, a division by a negative, or a NaN comparison if
    // the guard were missing. None of them may throw, and none of them may trip.
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

    // Exactly at the threshold, expressed against the administered capacity rather than a fixture
    // constant, so the boundary is asserted on the value the policy will actually divide by.
    val capacity = capacityBytesPerSecond.toDouble
    policy.recordLinkUtilization(capacity * LinkSaturationTripPercent.toDouble / 100.0d)
    assert(!policy.hasTripped,
      "the administered capacity must tolerate utilisation at the threshold")

    // A fully saturated link is strictly above the tolerated share and must trip.
    policy.recordLinkUtilization(capacity)
    assert(policy.hasTripped, "a fully saturated administered link must trip")
    assert(policy.trippedReason.contains(NetworkSaturation),
      s"the reported reason must be NetworkSaturation but was ${policy.trippedReason}")
    assert(policy.shouldDelegateToSortShuffle, "the terminus is the same for an administered link")
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

  // -----------------------------------------------------------------------------------------------
  // Trip 4: a producer/consumer protocol version mismatch, detected explicitly.
  //
  // This condition is an EXPLICIT compatibility check and never an inference from a failed decode.
  // The wire header carries a protocol version byte for precisely this purpose, so that an executor
  // rolled to a different revision is recognised BEFORE any attempt is made to interpret its frames
  // and degrades deterministically to sort-based shuffle instead of misreading bytes. Two
  // properties are therefore asserted beyond the trip itself: that the peek which reads that byte
  // does not consume the buffer, so the very same buffer can still be decoded afterwards; and that
  // a frame too short to carry a version is reported as the framing fault it is rather than
  // mislabelled as a version mismatch, because truncation is recovered by checksum verification and
  // retransmission.
  // -----------------------------------------------------------------------------------------------

  test("only the current streaming shuffle protocol version is compatible") {
    assert(ProtocolVersion == StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
      "the shared fixtures must report the version this build actually speaks")
    assert(StreamingShuffleMessage.isCompatible(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION),
      "this build must consider its own protocol revision compatible")

    // Compatibility requires exact equality, so every other value of the byte is incompatible. The
    // whole domain is walked rather than one sample of it, because "a different version" is not a
    // single case and a check that accepted a neighbouring revision would pass a narrower test.
    val everyOtherVersion = (Byte.MinValue.toInt to Byte.MaxValue.toInt)
      .map(_.toByte)
      .filter(_ != StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)
    assert(everyOtherVersion.size == 255,
      "the byte domain holds two hundred and fifty six values, one of which is the current one")
    assert(everyOtherVersion.forall(version => !StreamingShuffleMessage.isCompatible(version)),
      "no revision other than the current one may be considered compatible")

    // The layout the version byte opens, stated as the sum of its parts rather than as a literal so
    // that this assertion describes the header rather than merely echoing a number.
    assert(HeaderEncodedLength == 1 + 4 + 8 + 4 + 8,
      "the shared header is a version byte, a shuffle id, a map id, a partition id and a sequence")
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

  // -----------------------------------------------------------------------------------------------
  // The operator kill switch, which is the lower rung of the two-tier activation model.
  //
  // Tier 1, spark.shuffle.manager=streaming, decides which manager class Spark instantiates. Tier
  // 2, spark.shuffle.streaming.enabled, gates the behaviour of the class Tier 1 selected, and it
  // defaults to false. While it is false the streaming manager forwards every service-provider call
  // verbatim to its internal delegate, built as new SortShuffleManager(conf) -- one argument, which
  // is the whole of that class's constructor. That is what lets an operator restore sort behaviour
  // without redeploying a different manager class, and it is the SAME TERMINUS as all four trips.
  // -----------------------------------------------------------------------------------------------

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

    // The delegate is built exactly as the streaming manager builds its own, with the single
    // constructor argument that is the whole of SortShuffleManager's constructor. Comparing the two
    // managers call for call is what turns "gated off is indistinguishable from sort" from a claim
    // into an observation: the assertion is not that the result looks reasonable, it is that it is
    // the very same kind of object the sort manager itself would have returned.
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

    // This is the write path as well as the read path: a groupByKey shuffles for real, so a job
    // that produces the baseline's output proves the writer was the sort writer and the reader the
    // sort reader. Nothing is mocked and nothing is inspected -- the job either shuffles correctly
    // or it does not.
    val observed = groupedOutputAsSet(sc, PartitionCount)
    assertNoDataLoss(observed, baseline, "a job run with the streaming kill switch engaged")
  }

  // -----------------------------------------------------------------------------------------------
  // Latching, and the diagnostics that survive it.
  //
  // The tripped state latches: once tripped, an instance stays tripped until reset. Streaming and
  // sort-based shuffle cannot both own the same shuffle, so oscillating back into streaming
  // mid-shuffle would mean two producers of the same data. Latching also makes the decision
  // monotone, which is what allows the hot-path read to be a single atomic load with no lock, no
  // allocation and no clock read -- and reset exists for tests and for reusing an instance, never
  // as a runtime recovery path.
  // -----------------------------------------------------------------------------------------------

  test("the tripped state latches on the first condition observed") {
    val clock = newManualClock()
    val policy = activePolicy(clock)

    policy.recordLinkUtilization(LinkCapacityBytesPerSecond, LinkCapacityBytesPerSecond)
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

  // -----------------------------------------------------------------------------------------------
  // The central assertion: every path terminates in a working shuffle.
  //
  // This is the whole point of the fallback policy, and it is asserted over the complete set of
  // paths rather than one at a time, so that a fifth path added later without a terminus would fail
  // here. There is no configuration, no failure and no resource condition that leaves a job without
  // a functioning shuffle implementation, which is how zero regression is guaranteed structurally
  // rather than merely tested for.
  // -----------------------------------------------------------------------------------------------

  test("every fallback condition and the kill switch terminate in a working shuffle") {
    val drivers: Seq[(StreamingShuffleFallbackReason,
        (StreamingShuffleFallbackPolicy, ManualClock) => Unit)] = Seq(
      ConsumerTooSlow -> ((policy, clock) => driveSustainedConsumerSlowness(policy, clock)),
      MemoryPressure -> ((policy, _) => policy.recordAllocationGrant(RequestedBufferBytes, 0L)),
      NetworkSaturation -> ((policy, _) =>
        policy.recordLinkUtilization(LinkCapacityBytesPerSecond, LinkCapacityBytesPerSecond)),
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

    // The kill switch reaches the identical terminus with no condition observed at all, which is
    // what makes it a runtime switch rather than a fifth failure mode.
    val gated = new StreamingShuffleFallbackPolicy(gatedOffStreamingConf(), newManualClock())
    assert(gated.shouldDelegateToSortShuffle,
      "the kill switch must reach the same terminus as every trip condition")
    assert(!gated.streamingActive, "the kill switch must withdraw the streaming path")
    assert(!gated.hasTripped, "the kill switch must reach that terminus without any trip at all")
  }

  // -----------------------------------------------------------------------------------------------
  // Closed sets: the conditions, the metrics, and what a fallback is allowed to raise.
  //
  // Each of these sets is closed on purpose. A fifth condition would be a fifth way for streaming
  // to abandon the fast path, and every such way has to be specified, tested and documented before
  // it can be trusted. A fifth metric would be an operator-facing surface nobody had agreed to. And
  // a fallback raises nothing at all: it is reported through a boolean and a value from a sealed
  // set, which is exactly why it needs no error condition of its own.
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

    // The prose an operator reads, which composes into the single warning a trip emits.
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

    // Every condition is driven on one policy. None of these calls may raise: degradation is
    // reported through hasTripped and a value from the sealed set, so there is no fallback error
    // condition to add to the catalogue and this suite would fail here if one had been invented and
    // thrown.
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
  }

  // -----------------------------------------------------------------------------------------------
  // Configuration: read once, held immutably, and validated when it is read.
  //
  // Reading every value at construction and never consulting the configuration again is what makes
  // "streaming shuffle configuration changes require an executor restart" true by construction
  // rather than true by note, and it is why there is no dynamic-reconfiguration path to test.
  // -----------------------------------------------------------------------------------------------

  test("configuration is read once at construction and never re-read") {
    val conf = streamingConf()
    val policy = new StreamingShuffleFallbackPolicy(conf, newManualClock())
    assert(policy.streamingEnabledByConfig, "the fixture must start with streaming enabled")
    assert(!policy.killSwitchEngaged, "the fixture must start with the kill switch disengaged")
    assert(policy.administeredLinkCapacityBytesPerSecond.isEmpty,
      "the fixture must start with an uncapped link")

    // Mutating the configuration after construction changes nothing about the live instance.
    conf.set(SHUFFLE_STREAMING_ENABLED, false)
    conf.set(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, AdministeredBandwidthMbps)

    assert(policy.streamingEnabledByConfig,
      "a live policy must hold the value it read at construction, not the configuration's latest")
    assert(!policy.killSwitchEngaged, "the kill switch must not engage without an executor restart")
    assert(policy.streamingActive,
      "a live policy must not change behaviour under the caller's feet")
    assert(policy.administeredLinkCapacityBytesPerSecond.isEmpty,
      "a live policy must not pick up a bandwidth cap administered after it was constructed")

    // A policy constructed afterwards does observe the new values, which is what an executor
    // restart amounts to: the value is held per instance and is not cached anywhere process wide.
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

    // The documented defaults, by contrast, construct without complaint.
    val defaults = streamingConf()
    assert(defaults.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT) == DefaultBufferSizePercent,
      "the buffer budget must default to the documented share of executor memory")
    assert(defaults.get(SHUFFLE_STREAMING_SPILL_THRESHOLD) == DefaultSpillThresholdPercent,
      "the spill threshold must default to the documented utilisation")
    assert(new StreamingShuffleFallbackPolicy(defaults, newManualClock()).streamingActive,
      "a policy built from the documented defaults must be active")
  }
}
