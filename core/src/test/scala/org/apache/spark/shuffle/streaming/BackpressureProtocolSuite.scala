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

import java.util.concurrent.atomic.AtomicLong

import org.mockito.Mockito.mock
import org.scalatest.matchers.should.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.internal.config.{EXECUTOR_MEMORY, SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS}
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, StreamingShuffleMessageType}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.shuffle.{ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter}
import org.apache.spark.util.{Clock, ManualClock}

/**
 * Unit tests for [[BackpressureProtocol]] and [[TokenBucketRateLimiter]], the two components that
 * together own the streaming shuffle's flow control.
 *
 * Three layers of backpressure act on the streaming path and this suite exercises the two that are
 * expressible without a socket: application-level credit derived from consumer acknowledgements,
 * owned by the protocol, and rate limiting, owned by the token bucket. The third layer -- TCP-level
 * throttling by toggling a channel's `autoRead` -- belongs to the streaming client handler and is
 * covered where that handler is covered. What this suite does assert about the third layer is the
 * predicate the handler reads, [[BackpressureProtocol.hasCredit]], because that predicate is the
 * whole of the protocol's part in it.
 *
 * <b>Determinism.</b> Every timer in the subsystem reads an injected [[Clock]], so a five-second
 * bound is one method call rather than a wait. Nothing in this file sleeps, nothing polls a wall
 * clock, and the two concurrent cases release their threads from a barrier so that contention is
 * proven rather than hoped for. Every boundary is asserted twice -- once that the timer fires at
 * its bound and once that it does not fire one millisecond early -- because "the timer fired" and
 * "the timer did not fire early" are different claims and a suite owes both.
 *
 * <b>Global state.</b> [[StreamingShuffleMetricsSource]] is a JVM singleton whose counters outlive
 * a test, so `beforeEach` returns them to zero and the assertions that read them work in deltas
 * from a baseline captured after that reset. Both together are what make the metric assertions
 * independent of whatever ran before them in the same JVM.
 */
class BackpressureProtocolSuite extends SparkFunSuite
  with Matchers
  with StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  // Shuffle ids are distinct per role in the arbitration cases so that a contribution leaking from
  // one shuffle into another is visible as a wrong ordering rather than as a wrong total.
  private val firstShuffleId: Int = 0
  private val secondShuffleId: Int = 1
  private val thirdShuffleId: Int = 2
  private val fourthShuffleId: Int = 3

  /** Producer generation every key in this suite belongs to, held fixed so keys differ by role. */
  private val mapId: Long = 0L
  private val taskAttemptId: Long = 0L
  private val partitionId: Int = 0

  /**
   * Consumer session identity. Deliberately ASCII, so its UTF-8 encoded length is its character
   * count and the encoded-length assertions can state that length directly.
   */
  private val consumerId: String = "consumer-a"

  /** Reduce partition count registered for a shuffle unless a case needs a specific width. */
  private val partitionCount: Int = 4

  /** Credit allowance opened for a stream, chosen so four equal blocks fill it exactly. */
  private val creditLimitBytes: Long = 4096L

  /** A modest block, small enough that pacing never interferes with a credit assertion. */
  private val blockBytes: Long = 1024L

  /**
   * Milliseconds in one second, taken from the protocol rather than restated, so the interval this
   * suite advances a clock by is the interval the protocol measures rates over.
   */
  private val millisPerSecond: Long = BackpressureProtocol.MILLIS_PER_SECOND

  /**
   * Administered link capacity used by the pacing and saturation cases, in MB/s.
   *
   * One mebibyte per second is small enough that a single maximum-sized frame drains the burst
   * allowance, which is what makes a pacing refusal reachable without moving megabytes of data.
   */
  private val linkCapacityMBps: Int = 1

  /**
   * Returns the four streaming shuffle metrics to zero before every case.
   *
   * The metrics source is an `object`, so its counters accumulate for the lifetime of the process.
   * Without this a case asserting "exactly one backpressure event" would be asserting on whatever
   * the rest of the JVM had already counted, which is precisely the shape of a flaky test.
   */
  protected override def beforeEach(): Unit = {
    super.beforeEach()
    resetStreamingShuffleMetrics()
  }

  /**
   * A clock that counts every reading taken through it.
   *
   * This exists for one assertion that cannot be made any other way: an unlimited rate limiter is
   * required to admit every request with no arithmetic, no compare-and-set and no clock read, and
   * "no clock read" is only observable by counting reads. A [[ManualClock]] supplies the readings
   * so the wrapper stays deterministic.
   *
   * @param delegate the clock every reading is answered from
   */
  private class ReadCountingClock(delegate: ManualClock) extends Clock {

    private val reads = new AtomicLong(0L)

    /** Readings taken through this clock since it was created. */
    def readCount: Long = reads.get()

    override def getTimeMillis(): Long = {
      reads.incrementAndGet()
      delegate.getTimeMillis()
    }

    override def nanoTime(): Long = {
      reads.incrementAndGet()
      delegate.nanoTime()
    }

    override def waitTillTime(targetTime: Long): Long = {
      reads.incrementAndGet()
      delegate.waitTillTime(targetTime)
    }
  }

  /**
   * A key for the sending end of a stream.
   *
   * Every identifying component is supplied, because a key that omitted one would make two
   * genuinely distinct flows share a credit ledger. The defaults pin the components a case is not
   * varying so that the one it is varying stands out at the call site.
   *
   * @param shuffleId shuffle the stream belongs to
   * @param partition reduce partition the stream feeds
   * @param consumer consumer session reading that partition
   * @return the producer-side key
   */
  private def producerKey(
      shuffleId: Int = firstShuffleId,
      partition: Int = partitionId,
      consumer: String = consumerId): BackpressureStreamKey = {
    BackpressureStreamKey.forProducer(shuffleId, mapId, taskAttemptId, partition, consumer)
  }

  /**
   * A key for the receiving end of a stream.
   *
   * The role is part of the key, so a producer key and a consumer key naming the same shuffle,
   * map, attempt and partition are two ledgers rather than one. That matters here because these
   * cases run both ends inside one JVM, exactly as Spark does when it runs locally.
   *
   * @param shuffleId shuffle the stream belongs to
   * @param partition reduce partition the stream feeds
   * @param consumer consumer session reading that partition
   * @return the consumer-side key
   */
  private def consumerKey(
      shuffleId: Int = firstShuffleId,
      partition: Int = partitionId,
      consumer: String = consumerId): BackpressureStreamKey = {
    BackpressureStreamKey.forConsumer(shuffleId, mapId, taskAttemptId, partition, consumer)
  }

  /**
   * A protocol over the given configuration, with its egress budget derived from that same
   * configuration so the two can never disagree about the operator's cap.
   *
   * @param conf configuration the protocol and the budget both read once
   * @param clock the frozen clock the case controls
   * @param coordinator rendezvous endpoint, or `null` to make the protocol fall back to its own
   *                    registrations, which is a documented and supported shape
   * @return the protocol under test
   */
  private def protocolWith(
      conf: SparkConf,
      clock: ManualClock,
      coordinator: StreamingShuffleCoordinator): BackpressureProtocol = {
    new BackpressureProtocol(
      conf, coordinator, TokenBucketRateLimiter.executorBudget(conf, clock), clock)
  }

  /**
   * The common fixture: uncapped egress, no coordinator, a frozen clock.
   *
   * Uncapped is the default an operator gets, and it is also what keeps a credit assertion about
   * credit: with no bandwidth cap the pacing layer admits everything, so a refusal can only have
   * come from the credit ledger.
   *
   * @param clock the frozen clock the case controls
   * @return the protocol under test
   */
  private def newProtocol(clock: ManualClock): BackpressureProtocol = {
    protocolWith(streamingConfWithOverrides(), clock, null)
  }

  /**
   * A protocol whose egress is paced against a declared link capacity.
   *
   * @param clock the frozen clock the case controls
   * @return the protocol under test
   */
  private def newPacedProtocol(clock: ManualClock): BackpressureProtocol = {
    protocolWith(
      streamingConfWithOverrides(maxBandwidthMBps = Some(linkCapacityMBps)), clock, null)
  }

  // -----------------------------------------------------------------------------------------------
  // 1. Consumer acknowledgement processing and buffer reclamation.
  //
  // An acknowledgement is the single event that frees producer memory and lifts backpressure, so
  // this is the case that pins what an acknowledgement does: it advances the credit ledger, it
  // drains the unacknowledged window, and it opens the hundred-millisecond window inside which the
  // buffers it freed must actually be released.
  // -----------------------------------------------------------------------------------------------

  test("consumer acknowledgement processing and buffer reclamation") {
    val clock = newManualClock()
    val protocol = newProtocol(clock)
    val key = producerKey()
    protocol.registerShuffle(firstShuffleId, partitionCount)
    assert(protocol.registerStream(key, creditLimitBytes),
      "registering a stream this protocol has never seen must open a fresh credit ledger")
    assert(protocol.isStreamRegistered(key), "an opened ledger must be visible to its owner")
    assert(protocol.creditLimitBytes(key) === creditLimitBytes,
      "the ledger must remember the allowance it was opened with")
    assert(protocol.registeredStreams === Seq(key),
      "the ledger set must read back deterministically")

    // Four equal blocks fill the allowance exactly, so the fourth send is what exhausts credit
    // rather than some arbitrary later one.
    val blockSize = creditLimitBytes / 4L
    (0L until 4L).foreach { sequenceNumber =>
      assert(protocol.tryAdmit(key, blockSize, sequenceNumber),
        s"block $sequenceNumber must be admitted while credit remains")
    }
    assert(protocol.sentBytes(key) === creditLimitBytes,
      "every admitted block must be charged to the producer's output total")
    assert(protocol.outstandingBytes(key) === creditLimitBytes,
      "the outstanding total must equal the sum of the window's block sizes")
    assert(protocol.availableCreditBytes(key) === 0L, "a full window leaves no credit available")
    assert(!protocol.hasCredit(key),
      "a stream holding its whole allowance unacknowledged must report no credit, which is the " +
        "predicate the client handler reads before it leaves autoRead enabled")
    assert(protocol.unacknowledgedWindow(key) === Some((0L, 3L)),
      "the window is the exact extent of what the producer can still retransmit")
    assert(protocol.unacknowledgedBlockCount(key) === 4,
      "four blocks were sent and none acknowledged")

    // A fifth block is refused. That is flow control, not failure: the caller keeps the block.
    assert(!protocol.tryAdmit(key, blockSize, 4L),
      "a block that does not fit the remaining credit must be refused")
    assert(protocol.outstandingBytes(key) === creditLimitBytes,
      "a refused block must leave the window entirely uncharged so it can be retried unchanged")
    assert(protocol.isStreamThrottled(key),
      "a refused admission puts its stream in the throttled state")
    assert(protocol.state === BackpressureState.Throttled,
      "the executor reports the most severe state any of its streams is in")

    // An acknowledgement of the first two blocks releases exactly their bytes, and nothing more.
    val released = protocol.onAck(key, 1L)
    assert(released === 2L * blockSize,
      "an acknowledgement must release every block at or below the consumed position and no other")
    assert(protocol.acknowledgedPosition(key) === 1L,
      "the acknowledged position must advance to the ack")
    assert(protocol.acknowledgedBytes(key) === 2L * blockSize,
      "released bytes must accumulate on the consumer's total, which is its measured rate")
    assert(protocol.outstandingBytes(key) === 2L * blockSize,
      "the remaining two blocks stay charged")
    assert(protocol.availableCreditBytes(key) === 2L * blockSize,
      "credit returns to the producer in exactly the volume the acknowledgement released")
    assert(protocol.hasCredit(key), "restored credit is what lets the producer send again")
    assert(!protocol.isStreamThrottled(key),
      "an acknowledgement that restores credit ends the throttling episode")
    assert(protocol.state === BackpressureState.Flowing, "a stream with credit is flowing")
    assert(protocol.unacknowledgedWindow(key) === Some((2L, 3L)),
      "the window must be drained from its low end, leaving the unacknowledged suffix")
    assert(protocol.unacknowledgedBlockCount(key) === 2, "two of the four blocks remain retained")

    // Reclamation is measured from the acknowledgement, because the release happens on another
    // thread and only the protocol can see both ends of the interval.
    assert(!protocol.isReclamationOverdue(key),
      "nothing is overdue in the instant an ack is applied")
    clock.advance(ReclamationDeadlineMillis)
    assert(!protocol.isReclamationOverdue(key),
      "a release is overdue only strictly beyond the hundred-millisecond bound")
    val latency = protocol.confirmReclamation(key)
    assert(latency === Some(ReclamationDeadlineMillis),
      "the confirmed latency must be the interval the clock actually advanced")
    assert(latency.exists(_ <= ReclamationDeadlineMillis),
      "reclamation must complete within 100 ms of a consumer acknowledgement")
    assert(protocol.reclamationDeadlineBreaches === 0L,
      "a release inside the bound is not a breach")
    assert(protocol.confirmReclamation(key).isEmpty,
      "one acknowledgement opens one reclamation window, so a second confirmation closes nothing")

    // A duplicate or reordered acknowledgement is absorbed rather than allowed to rewind anything.
    assert(protocol.onAck(key, 1L) === 0L, "a duplicate acknowledgement releases nothing")
    assert(protocol.onAck(key, 0L) === 0L, "a reordered acknowledgement releases nothing")
    assert(protocol.acknowledgedPosition(key) === 1L,
      "acknowledgement positions are monotonic and a stale one must not pull the cursor back")

    // A release outside the bound is counted as a breach rather than failing the task, because a
    // late release costs throughput and not correctness.
    assert(protocol.onAck(key, 3L) === 2L * blockSize, "the remaining window is released in full")
    assert(protocol.outstandingBytes(key) === 0L, "a fully acknowledged stream holds nothing")
    assert(protocol.unacknowledgedWindow(key).isEmpty, "an empty window has no bounds to report")
    clock.advance(ReclamationDeadlineMillis + 1L)
    assert(protocol.isReclamationOverdue(key), "one millisecond past the bound is overdue")
    assert(protocol.confirmReclamation(key) === Some(ReclamationDeadlineMillis + 1L),
      "a late confirmation still reports its true latency")
    assert(protocol.reclamationDeadlineBreaches === 1L, "a release outside the bound is one breach")
  }

  test("consumer acknowledgement processing accepts the wire form and refuses an impossible one") {
    val clock = newManualClock()
    val protocol = newProtocol(clock)
    val key = producerKey()
    protocol.registerShuffle(firstShuffleId, partitionCount)
    assert(protocol.registerStream(key, creditLimitBytes), "the ledger must open")
    assert(protocol.tryAdmit(key, blockBytes, 0L),
      "one block is admitted so there is something to ack")

    // The framing of an acknowledgement: the shared twenty-five byte header plus one long, and one
    // further byte of type discriminator once it is framed for the wire.
    val acknowledgement = ack(firstShuffleId, mapId, partitionId, 1L, 0L)
    assert(HeaderEncodedLength === 25,
      "the streaming wire header is twenty-five bytes: version, shuffle, map, partition, sequence")
    assert(acknowledgement.encodedLength() === FixedMessageEncodedLength,
      "an acknowledgement is the header plus its eight-byte consumed position")
    assert(framedLength(acknowledgement.encodedLength()) === acknowledgement.encodedLength() + 1,
      "framing adds exactly the one-byte type discriminator")
    assert(acknowledgement.consumerPosition() === 0L,
      "the message must carry the position it was built with")

    // Applied through the wire form, an acknowledgement does exactly what the positional form does.
    assert(protocol.onAck(key, acknowledgement) === blockBytes,
      "an acknowledgement that arrived on the wire must release the same bytes")
    assert(protocol.outstandingBytes(key) === 0L, "the window is drained by the wire form too")

    // The stream key is supplied by the handler that owns the channel, because the wire carries
    // only a shuffle, a map and a partition and those three do not identify a stream. A frame whose
    // identity contradicts the key it arrived as is refused rather than applied to another ledger.
    val misaddressed = ack(secondShuffleId, mapId, partitionId, 2L, 0L)
    assert(protocol.tryAcknowledge(key, misaddressed).isEmpty,
      "a frame naming another shuffle must never be applied to this ledger")
    assert(protocol.misaddressedFrameCount === 1L,
      "a misrouted frame is counted rather than logged")
    val absentFrame: AckMessage = null
    assert(protocol.tryAcknowledge(key, absentFrame).isEmpty, "a null frame releases nothing")

    // A position beyond anything this side has charged is impossible rather than merely stale: no
    // consumer can have consumed a block that was never sent to it, so honouring it would release
    // output the real consumer has not read.
    val impossible = protocol.highestChargedSequenceNumber(key) + 1L
    assert(protocol.tryAcknowledge(key, impossible).isEmpty,
      "an acknowledgement naming the future must be refused outright")
    assert(protocol.impossibleAckPositionCount === 1L,
      "an impossible position is counted so a misbehaving peer is distinguishable from a slow one")
    assert(protocol.onAck(key, impossible) === 0L,
      "the total form of the call absorbs a refusal as zero released bytes")
    assert(protocol.acknowledgedPosition(key) === 0L, "a refused acknowledgement advances nothing")

    // The nothing-consumed sentinel is taken from the message type, so the wire and the protocol
    // agree on it by construction rather than by two matching literals.
    assert(NothingConsumedPosition === BackpressureProtocol.NOTHING_ACKNOWLEDGED,
      "the protocol's sentinel is the message type's sentinel")
    assert(NothingConsumedPosition === -1L, "nothing consumed is minus one, not zero")

    // Releasing the ledger releases every byte of credit it held, and it is safe to do twice
    // because it runs from cleanup that executes on success, on failure and on cancellation alike.
    assert(protocol.unregisterStream(key), "the last owner closing the ledger reports that it did")
    assert(!protocol.isStreamRegistered(key), "a closed ledger is gone")
    assert(protocol.outstandingBytes(key) === 0L, "a stream with no ledger holds nothing")
    assert(!protocol.unregisterStream(key), "closing an already closed ledger changes nothing")
    assert(protocol.hasCredit(key),
      "a stream with no ledger fails open, because a protocol never told about a stream must not " +
        "be the reason a legitimate transfer stalls")
  }

  // -----------------------------------------------------------------------------------------------
  // 2. Rate limiting via the token bucket.
  //
  // The bucket is the canonical two-parameter formulation: a bucket size, which is the burst a
  // producer may emit after an idle period, and a token rate, which is the sustained throughput it
  // is allowed. One token is one byte. Tokens accrue at the rate, surplus is discarded once the
  // bucket is full, and a request arriving when the bucket cannot cover it is over limit.
  //
  // This is first-party code by deliberate choice. Guava's rate limiter is on the classpath but has
  // zero usages anywhere in Spark, is relocated under shading, and is oriented towards blocking
  // acquisition -- and non-blocking is a hard requirement here, because every caller is a network
  // event-loop thread and a parked one stalls every channel multiplexed onto it.
  // -----------------------------------------------------------------------------------------------

  test("rate limiting via token bucket") {
    val base = newManualClock()
    val clock = new ReadCountingClock(base)
    val capacityBytes = 4096L
    val refillRate = 1024L
    val limiter = new TokenBucketRateLimiter(capacityBytes, refillRate, clock)

    assert(!limiter.isUnlimited, "a limiter built with a finite rate enforces that rate")
    assert(limiter.capacityBytes === capacityBytes, "the bucket size must be the one requested")
    assert(limiter.refillBytesPerSecond === refillRate, "the token rate must be the one requested")
    assert(limiter.maxAcquirableBytes === capacityBytes,
      "the largest admissible request is the whole bucket, published so a caller frames to it")
    assert(limiter.availableTokens === capacityBytes,
      "a bucket starts full so a producer's first burst is not delayed by a refill interval")

    // Draining the whole bucket succeeds and returns immediately: acquisition never parks.
    assert(limiter.tryAcquire(capacityBytes), "a full bucket admits a request for all of it")
    assert(limiter.availableTokens === 0L, "an admitted request is debited in full")

    // A refusal is a signal, not an error, and it publishes nothing at all.
    assert(!limiter.tryAcquire(1L),
      "an empty bucket refuses, which is flow control and not failure")
    assert(limiter.availableTokens === 0L, "a refusal must leave the ledger untouched")
    assert(limiter.millisUntilAvailable(refillRate) === millisPerSecond,
      "a refused caller is told exactly when the bucket's own arithmetic will admit it")

    // Accrual is monotonic in the clock, up to the bucket size and no further.
    base.advance(millisPerSecond)
    assert(limiter.availableTokens === refillRate,
      "one second of accrual mints exactly one second of the token rate")
    base.advance(millisPerSecond)
    assert(limiter.availableTokens === 2L * refillRate,
      "accrual is measured from the recorded refill point, so observing it does not consume it")
    base.advance(10L * millisPerSecond)
    assert(limiter.availableTokens === capacityBytes, "accrual is clamped at the bucket size")

    // An empty payload needs no special case at the call site.
    assert(limiter.tryAcquire(0L), "a zero-sized request is admitted without charge")
    assert(limiter.tryAcquire(-1L), "a non-positive request is admitted without charge")
    assert(limiter.availableTokens === capacityBytes, "neither of those may debit anything")

    // A request larger than the whole bucket is refused outright rather than admitted against a
    // full bucket and charged only the capacity, which is what would let the surplus bytes travel
    // entirely unpaced and defeat the operator's cap.
    assert(!limiter.tryAcquire(capacityBytes + 1L), "an oversized request is refused")
    assert(limiter.oversizedRequestCount === 1L,
      "an oversized request is counted, because it means the caller is framing illegal blocks")
    assert(limiter.availableTokens === capacityBytes, "an oversized refusal charges nothing")
    assert(limiter.millisUntilAvailable(capacityBytes + 1L) === Long.MaxValue,
      "no amount of waiting satisfies a request the bucket can never hold")

    // Refusing an oversized request cannot deadlock a producer, because every bucket the factory
    // builds is floored at one maximum-sized encoded frame, so every legal block fits whole.
    assert(MaxEncodedFrameBytes.toLong >= MinTokenBucketCapacityBytes,
      "the encoded frame cap is the payload cap plus framing, so it is never the smaller one")
    val flooredLimiter =
      new TokenBucketRateLimiter(TokenBucketRateLimiter.burstCapacityBytes(1L), 1L, clock)
    assert(flooredLimiter.capacityBytes === MaxEncodedFrameBytes.toLong,
      "a bucket at the slowest conceivable rate is still sized to hold one maximum frame")
    assert(flooredLimiter.tryAcquire(MinTokenBucketCapacityBytes),
      "a maximum-sized block must never be permanently refused for size")

    // Returning a limiter to a known state is what lets one bucket serve several measurements.
    assert(limiter.tryAcquire(capacityBytes), "the bucket is drained again")
    assert(limiter.availableTokens === 0L, "and is empty")
    limiter.reset()
    assert(limiter.availableTokens === capacityBytes, "a reset bucket is full again")
  }

  test("rate limiting via token bucket discards surplus tokens once the bucket is full") {
    val clock = newManualClock()
    val capacityBytes = 2048L
    val refillRate = 1024L
    val limiter = new TokenBucketRateLimiter(capacityBytes, refillRate, clock)
    assert(limiter.tryAcquire(capacityBytes), "the bucket starts full and is drained")
    assert(limiter.availableTokens === 0L, "so it now holds nothing")

    // An hour of idleness would earn well over three million tokens at this rate. The bucket keeps
    // one bucket's worth and throws every one of the rest away, which is what stops an idle
    // producer from banking an unbounded burst.
    val idleMillis = 3600L * millisPerSecond
    val earned = TokenBucketRateLimiter.tokensEarned(
      idleMillis * (TokenBucketRateLimiter.NANOS_PER_SECOND / millisPerSecond), refillRate)
    assert(earned === 3600L * refillRate, "an hour at this rate earns an hour's worth of tokens")
    assert(earned > capacityBytes, "and that is far more than the bucket can hold")
    clock.advance(idleMillis)
    assert(limiter.availableTokens === capacityBytes,
      "accrual above the bucket size is discarded, not banked")
    assert(limiter.tryAcquire(capacityBytes), "one bucket's worth of burst is available")
    assert(!limiter.tryAcquire(1L),
      "and no more, because the surplus an hour of idleness would have earned was thrown away")
  }

  test("rate limiting via token bucket leaves a refused request entirely uncharged") {
    val clock = newManualClock()
    val capacityBytes = 4096L
    val refillRate = 1024L
    val limiter = new TokenBucketRateLimiter(capacityBytes, refillRate, clock)
    assert(limiter.tryAcquire(3072L), "three quarters of the bucket is admitted")
    val remainingTokens = limiter.availableTokens
    assert(remainingTokens === 1024L, "leaving exactly one quarter")

    // A tight loop of refusals. None of them may move the ledger and none of them may throw: a
    // refusal is the signal that makes the caller hold its block, and holding blocks is precisely
    // what drives the spill decision.
    val refusals = 64
    (0 until refusals).foreach { attempt =>
      assert(!limiter.tryAcquire(remainingTokens + 1L),
        s"attempt $attempt asked for one byte more than the bucket holds and must be refused")
      assert(limiter.availableTokens === remainingTokens,
        s"refusal $attempt must publish nothing, so the token count is unchanged")
    }
    assert(limiter.oversizedRequestCount === 0L,
      "a request within the bucket size is over limit, not oversized, and is not counted as one")

    // And the caller may retry the identical request, unchanged, once accrual has caught up.
    clock.advance(millisPerSecond)
    assert(limiter.tryAcquire(remainingTokens + 1L),
      "the request a refusal made the caller hold is admitted verbatim once tokens have accrued")
  }

  test("rate limiting via token bucket treats an absent bandwidth cap as unlimited") {
    val base = newManualClock()
    val clock = new ReadCountingClock(base)
    val limiter = TokenBucketRateLimiter.unlimited(clock)

    // The constructor records an accrual origin, so the baseline is taken after construction. Every
    // reading after that point is one the acquisition path would have had to take.
    val readsAfterConstruction = clock.readCount
    assert(limiter.isUnlimited, "an unset bandwidth cap selects the unlimited shape")
    assert(limiter.availableTokens === Long.MaxValue,
      "the honest answer to how much may be sent when no rate is enforced")
    assert(limiter.maxAcquirableBytes === Long.MaxValue, "an unlimited limiter has no size ceiling")
    assert(limiter.millisUntilAvailable(Long.MaxValue) === 0L, "nothing is ever withheld")
    assert(!limiter.updateRate(MaxEncodedFrameBytes.toLong),
      "an unlimited limiter has no rate to redistribute, so a redistribution pass is a no-op on it")

    val requests = 128
    (0 until requests).foreach { attempt =>
      assert(limiter.tryAcquire(MaxEncodedFrameBytes.toLong),
        s"request $attempt must be admitted, because there is no rate to enforce")
    }
    assert(clock.readCount === readsAfterConstruction,
      "the unlimited fast path must admit with no arithmetic, no compare-and-set and no clock read")

    // Behaviour is identical at two different clock readings, which is the same fact stated from
    // the outside: nothing the clock says can change what an unlimited limiter does.
    base.setTime(ManualClockEpochMillis + 3600L * millisPerSecond)
    assert(limiter.tryAcquire(Long.MaxValue), "an hour later, still admitted")
    assert(limiter.availableTokens === Long.MaxValue, "and still reporting no ceiling")
    assert(clock.readCount === readsAfterConstruction, "and still without consulting the clock")

    // Absence of the cap, not a zero and not a negative sentinel, is what selects this shape. The
    // entry is optional precisely so that "unlimited" needs no magic value.
    val uncapped = streamingConfWithOverrides()
    assert(uncapped.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS).isEmpty,
      "the default configuration declares no link capacity at all")
    val budget = TokenBucketRateLimiter.executorBudget(uncapped, base)
    assert(budget.limiterFor(firstShuffleId).isUnlimited,
      "an absent cap must route to a limiter that paces nothing, never to a rate of zero")
    assert(budget.currentShareBytesPerSecond.isEmpty,
      "there is no per-shuffle share to report when no capacity was declared")
  }

  test("rate limiting via token bucket derives its refill rate from the cap and the divisor") {
    val declaredMBps = 1000
    val administeredBytesPerSecond = declaredMBps.toLong * BytesPerMebibyte

    // Step one: the declared link capacity is split evenly across the shuffles the executor serves.
    assert(TokenBucketRateLimiter.perShuffleBytesPerSecond(declaredMBps, 1) ===
      administeredBytesPerSecond, "one shuffle takes the whole declared capacity as its share")
    assert(TokenBucketRateLimiter.perShuffleBytesPerSecond(declaredMBps, 4) ===
      administeredBytesPerSecond / 4L, "four shuffles take a quarter of it each")

    // Step two, and only then: the share is held to eighty percent, which is streaming shuffle's
    // ceiling on the declared capacity. Eighty percent here is the bandwidth ceiling and is NOT the
    // ninety percent at which measured saturation trips the fallback.
    assert(BandwidthCeilingPercent === 80L, "the bandwidth ceiling is eighty percent of the cap")
    assert(applyBandwidthCeiling(administeredBytesPerSecond) ===
      administeredBytesPerSecond / PercentScale * BandwidthCeilingPercent,
      "the ceiling divides before it multiplies, which is what keeps the arithmetic overflow free")
    assert(applyBandwidthCeiling(0L) === 1L,
      "a rate is floored at one byte per second so a very small cap throttles rather than halts")

    // Composed, the two steps are the contract: (0.8 * cap) / numConcurrentShuffles.
    (1 to 4).foreach { shuffles =>
      val share = refillBytesPerSecond(declaredMBps, shuffles)
      assert(share === applyBandwidthCeiling(
        TokenBucketRateLimiter.perShuffleBytesPerSecond(declaredMBps, shuffles)),
        s"the share for $shuffles shuffles is the ceiling applied to the split")
      assert(share * shuffles.toLong <= applyBandwidthCeiling(administeredBytesPerSecond),
        s"the aggregate of $shuffles limiters must stay inside the ceiling, not exceed it each")
    }

    // A non-positive divisor is read as one, so the division the limiter performs is always safe.
    assert(TokenBucketRateLimiter.perShuffleBytesPerSecond(declaredMBps, 0) ===
      TokenBucketRateLimiter.perShuffleBytesPerSecond(declaredMBps, 1),
      "zero concurrent shuffles is treated as one, so there is no division by zero")
    assert(TokenBucketRateLimiter.perShuffleBytesPerSecond(declaredMBps, -8) ===
      TokenBucketRateLimiter.perShuffleBytesPerSecond(declaredMBps, 1),
      "a negative count is treated as one for the same reason")

    // The burst allowance is one second of the rate, floored at one maximum-sized encoded frame.
    assert(TokenBucketRateLimiter.burstCapacityBytes(1L) === MaxEncodedFrameBytes.toLong,
      "a tiny rate still yields a bucket that can hold one whole frame")
    assert(TokenBucketRateLimiter.burstCapacityBytes(administeredBytesPerSecond) ===
      administeredBytesPerSecond, "a large rate yields one second of itself")

    // End to end through the budget the executor holds, including the redistribution that keeps
    // the aggregate obeying the cap as shuffles come and go.
    val clock = newManualClock()
    val budget = TokenBucketRateLimiter.executorBudget(
      streamingConfWithOverrides(maxBandwidthMBps = Some(declaredMBps)), clock)
    val firstLimiter = budget.limiterFor(firstShuffleId)
    assert(budget.divisor === 1, "one limiter means one shuffle to divide the cap by")
    assert(firstLimiter.refillBytesPerSecond === refillBytesPerSecond(declaredMBps, 1),
      "the only shuffle paces at the whole ceiling")
    val secondLimiter = budget.limiterFor(secondShuffleId)
    assert(budget.divisor === 2, "admitting a second shuffle moves the divisor")
    assert(firstLimiter.refillBytesPerSecond === refillBytesPerSecond(declaredMBps, 2),
      "and republishes the first limiter's share, which is what stops the pair exceeding the cap")
    assert(secondLimiter.refillBytesPerSecond === refillBytesPerSecond(declaredMBps, 2),
      "so both limiters pace at the same halved share")
    assert(budget.currentShareBytesPerSecond === Some(refillBytesPerSecond(declaredMBps, 2)),
      "and the budget reports that share")
    assert(budget.release(secondShuffleId), "retiring a shuffle reports that a limiter was retired")
    assert(budget.divisor === 1, "which returns the divisor")
    assert(firstLimiter.refillBytesPerSecond === refillBytesPerSecond(declaredMBps, 1),
      "and the retired share to the shuffle that remains")
    assert(!budget.release(secondShuffleId), "releasing a shuffle that holds no limiter is a no-op")
  }

  // -----------------------------------------------------------------------------------------------
  // 3. Timeout detection and failure signalling.
  //
  // Two windows, and they are different lengths for different reasons. A producer that sends
  // nothing at all for five seconds has lost its connection, and that is what the reader turns
  // into a partial-read invalidation and a fetch failure. A consumer that has bytes outstanding
  // and acknowledges none of them for ten seconds is gone, and that is what makes the writer
  // retain, spill and later replay its unacknowledged window.
  //
  // Both are enforced at application level rather than by TCP: the transport exposes keepalive as a
  // boolean only and the JDK offers no socket option for a keepalive interval, so these timers are
  // the whole of the two bounds. Both read the injected clock, which is what makes them exact.
  // -----------------------------------------------------------------------------------------------

  test("timeout detection and failure signalling") {
    assert(ProducerConnectionTimeoutMillis === 5000L,
      "the producer connection timeout is five seconds")
    assert(ConsumerLivenessTimeoutMillis === 10000L, "the consumer liveness window is ten seconds")
    assert(HeartbeatIntervalMillis === ProducerConnectionTimeoutMillis,
      "a heartbeat is emitted at the cadence the producer detector expects, so a producer with " +
        "nothing to send stays alive by heartbeating alone")

    val clock = newManualClock()
    val protocol = newProtocol(clock)
    val key = consumerKey()
    protocol.registerShuffle(firstShuffleId, partitionCount)
    assert(protocol.registerStream(key, creditLimitBytes), "the ledger must open")
    assert(!protocol.isProducerTimedOut(key),
      "a stream that has just opened has heard from its peer")
    assert(protocol.millisSinceInbound(key) === Some(0L), "no time has passed since it opened")

    // The boundary, both ways. Silence is measured from the last inbound event of any kind.
    advanceJustBeforeProducerTimeout(clock)
    assert(protocol.millisSinceInbound(key) === Some(ProducerConnectionTimeoutMillis - 1L),
      "the detector must see exactly the interval the clock advanced")
    assert(!protocol.isProducerTimedOut(key),
      "the producer liveness timer must NOT fire at 4999 ms")
    assert(protocol.timedOutProducerStreams.isEmpty, "so no stream is reported as having lost it")
    clock.advance(1L)
    assert(protocol.isProducerTimedOut(key), "and must fire at exactly 5000 ms")
    assert(protocol.timedOutProducerStreams === Seq(key),
      "the whole-set scan must apply the same definition as the single-stream predicate")

    // Inbound activity of any kind clears it, which is the point of the heartbeat: a producer that
    // is alive but momentarily has nothing to send is not a failed producer.
    protocol.onHeartbeat(key)
    assert(!protocol.isProducerTimedOut(key), "a heartbeat alone keeps a producer judged alive")
    assert(protocol.millisSinceInbound(key) === Some(0L), "and re-bases the interval")
    protocol.onDataReceived(key, 0L, blockBytes)
    assert(protocol.millisSinceInbound(key) === Some(0L),
      "arrival refreshes liveness before any acknowledgement, so a consumer slow to acknowledge " +
        "cannot declare a healthy producer dead")
    clock.advance(ProducerConnectionTimeoutMillis)
    assert(protocol.isProducerTimedOut(key), "and the timer arms again from that arrival")

    // Silence that means completion is not silence that means failure.
    protocol.onStreamTermination(key, 1L)
    assert(protocol.isStreamTerminated(key), "the producer announced the orderly end of the stream")
    assert(protocol.announcedBlockCount(key) === Some(1L), "and how many blocks it sent")
    clock.advance(10L * ProducerConnectionTimeoutMillis)
    assert(!protocol.isProducerTimedOut(key),
      "silence after an orderly end of stream must never be read as a failed producer")
    assert(protocol.timedOutProducerStreams.isEmpty, "nor reported by the whole-set scan")
    assert(!protocol.shouldSendHeartbeat(key),
      "and a finished stream has nothing left for a heartbeat to keep alive")

    // An empty reduce partition terminates with a total of zero, and that is a valid reading rather
    // than an absent one, so it must not be mistaken for a producer that never started.
    val emptyKey = consumerKey(partition = 1, consumer = "consumer-empty")
    assert(protocol.registerStream(emptyKey, creditLimitBytes), "a second ledger opens")
    protocol.onStreamTermination(emptyKey, 0L)
    assert(protocol.announcedBlockCount(emptyKey) === Some(0L),
      "zero blocks is the valid announcement of an empty partition")
    clock.advance(10L * ProducerConnectionTimeoutMillis)
    assert(!protocol.isProducerTimedOut(emptyKey), "and it is never treated as a failure")
  }

  test("timeout detection and failure signalling for the consumer liveness window") {
    val clock = newManualClock()
    val protocol = newProtocol(clock)
    val key = producerKey()
    protocol.registerShuffle(firstShuffleId, partitionCount)
    assert(protocol.registerStream(key, creditLimitBytes), "the ledger must open")

    // Sending arms the window, and the boundary holds both ways. The window is measured from the
    // last acknowledgement, and a ledger that has had none is dated from the moment it opened, so
    // the arming instant is read from the clock rather than assumed.
    assert(protocol.tryAdmit(key, blockBytes, 0L), "one block goes out")
    assert(protocol.outstandingBytes(key) === blockBytes, "so one block is outstanding")
    val armedAtMillis = clock.getTimeMillis()
    advanceJustBeforeConsumerLivenessTimeout(clock)
    assert(!protocol.isConsumerTimedOut(key),
      "the consumer liveness timer must NOT fire at 9999 ms")
    assert(protocol.timedOutConsumerStreams.isEmpty, "so no stream is reported as having lost it")
    clock.advance(1L)
    assert(clock.getTimeMillis() - armedAtMillis === ConsumerLivenessTimeoutMillis,
      "the clock has advanced exactly the liveness window")
    assert(protocol.isConsumerTimedOut(key), "and it must fire at exactly 10000 ms")
    assert(protocol.timedOutConsumerStreams === Seq(key),
      "which is the detection the writer turns into retention, spill and replay")

    // An acknowledgement that advances the position re-bases the window and empties the ledger, so
    // the consumer is neither behind nor unresponsive any more.
    assert(protocol.onAck(key, 0L) === blockBytes, "the outstanding block is acknowledged")
    assert(!protocol.isConsumerTimedOut(key), "a consumer that has caught up is not timed out")
    assert(protocol.outstandingBytes(key) === 0L, "and holds nothing")

    // The window only arms while something is actually outstanding. A consumer with nothing to
    // acknowledge is idle, and reporting it as timed out would make every completed stream look
    // like a failure ten seconds later.
    clock.advance(2L * ConsumerLivenessTimeoutMillis)
    assert(!protocol.isConsumerTimedOut(key),
      "a consumer with nothing outstanding is idle rather than unresponsive")
    assert(protocol.timedOutConsumerStreams.isEmpty, "so the whole-set scan reports nothing")

    // A terminated stream retires the consumer timer as well as the producer one, so the silence
    // that follows an orderly end of stream is never read as a consumer that has gone away.
    assert(protocol.tryAdmit(key, blockBytes, 1L), "another block goes out")
    assert(protocol.isConsumerTimedOut(key),
      "and because the window elapsed long ago, that alone reports the consumer as gone")
    protocol.onStreamTermination(key, 2L)
    clock.advance(2L * ConsumerLivenessTimeoutMillis)
    assert(!protocol.isConsumerTimedOut(key),
      "whereas a stream whose producer has finished is not an unresponsive consumer")
    assert(protocol.timedOutConsumerStreams.isEmpty, "and the whole-set scan agrees")
  }

  test("timeout detection and failure signalling through the heartbeat interval") {
    val clock = newManualClock()
    val protocol = newProtocol(clock)
    val key = consumerKey()
    protocol.registerShuffle(firstShuffleId, partitionCount)
    assert(protocol.registerStream(key, creditLimitBytes), "the ledger must open")

    assert(!protocol.shouldSendHeartbeat(key), "a stream that has just opened is not due")
    advanceJustBeforeProducerTimeout(clock)
    assert(!protocol.shouldSendHeartbeat(key), "a heartbeat is NOT due at 4999 ms")
    clock.advance(1L)
    assert(protocol.shouldSendHeartbeat(key), "and is due at exactly the five-second interval")

    // The heartbeat is stamped from the injected clock. That is the design fact that makes clock
    // injection possible AND mandatory: the message type takes its timestamp from the caller and
    // reads no clock of its own, so the protocol's notion of time is the only one in play.
    val nextPosition = 7L
    val emitted = protocol.heartbeatFor(key, nextPosition)
    assert(emitted.isDefined, "a registered stream must be able to build its heartbeat")
    val message = emitted.get
    assert(message.timestampMs() === clock.getTimeMillis(),
      "the stamp must be this protocol's clock reading, not a wall-clock reading of its own")
    assert(message.sequenceNumber() === nextPosition,
      "the position is supplied by the handler that owns the channel and taken on trust")
    assert(message.consumerId() === consumerId,
      "a consumer heartbeat declares the session identity the producer keys its cursor by")
    assert(message.shuffleId() === firstShuffleId, "and names the stream it belongs to")
    assert(message.partitionId() === partitionId, "including its reduce partition")

    // The framing of a heartbeat: the shared header, twelve fixed body bytes, and the identity.
    assert(heartbeatEncodedLength(0) === HeartbeatBaseEncodedLength,
      "a heartbeat naming no consumer is the header plus its fixed body")
    assert(message.encodedLength() === heartbeatEncodedLength(consumerId.length),
      "and one naming a consumer is that plus the identity's encoded bytes")
    assert(framedLength(message.encodedLength()) === message.encodedLength() + 1,
      "framing adds exactly the one-byte type discriminator")

    assert(!protocol.shouldSendHeartbeat(key), "emitting restarts the interval")
    clock.advance(HeartbeatIntervalMillis)
    assert(protocol.shouldSendHeartbeat(key), "which elapses again on the same cadence")

    // An inbound heartbeat refreshes liveness from the LOCAL instant of arrival, and retains the
    // peer's own stamp for diagnostics only. Two hosts do not agree on the wall clock, so a
    // detector built on a remote reading would mistake skew for a failure.
    clock.advance(ProducerConnectionTimeoutMillis)
    assert(protocol.isProducerTimedOut(key), "the producer has fallen silent")
    val skewedPeerStamp = clock.getTimeMillis() + 60L * millisPerSecond
    protocol.onHeartbeat(key, heartbeat(firstShuffleId, mapId, partitionId, 8L, skewedPeerStamp))
    assert(!protocol.isProducerTimedOut(key), "and is alive again the moment its heartbeat arrives")
    assert(protocol.millisSinceInbound(key) === Some(0L),
      "liveness is judged from the local instant of arrival")
    assert(protocol.remoteHeartbeatTimestamp(key) === Some(skewedPeerStamp),
      "while the peer's stamp is retained, unmodified, for diagnostics and nothing else")

    // The control-message dispatcher applies each message by its concrete type, never by its
    // encoded length: the fixed-shape control messages do not all encode to the same length, and a
    // codec that tried to use length would silently read one message as another.
    assert(protocol.onControlMessage(
      key, heartbeat(firstShuffleId, mapId, partitionId, 9L, skewedPeerStamp)) ===
        Some(StreamingShuffleMessageType.HEARTBEAT), "a heartbeat dispatches as a heartbeat")
    protocol.onDataReceived(key, 0L, blockBytes)
    assert(protocol.onControlMessage(
      key, ack(firstShuffleId, mapId, partitionId, 10L, 0L)) ===
        Some(StreamingShuffleMessageType.ACK), "an acknowledgement dispatches as an ack")
    assert(protocol.acknowledgedPosition(key) === 0L, "and is applied on the way through")
    assert(protocol.onControlMessage(
      key, streamTermination(firstShuffleId, mapId, partitionId, 1L)) ===
        Some(StreamingShuffleMessageType.STREAM_TERMINATION), "so does a terminator")
    assert(protocol.isStreamTerminated(key), "which is applied on the way through as well")
  }

  test("timeout detection and failure signalling exhausts a bounded retransmission budget") {
    val clock = newManualClock()
    val protocol = newProtocol(clock)
    val key = producerKey()
    protocol.registerShuffle(firstShuffleId, partitionCount)
    assert(protocol.registerStream(key, creditLimitBytes), "the ledger must open")

    // The ladder is one second doubled once per attempt, over five permitted attempts.
    assert(RetryBaseBackoffMillis === 1000L, "the first pause is one second")
    assert(MaxRetryAttempts === 5, "and five attempts are permitted")
    assert(RetryBackoffLadderMillis === Seq(1000L, 2000L, 4000L, 8000L, 16000L),
      "so the ladder is one, two, four, eight and sixteen seconds")
    assert((1 to MaxRetryAttempts).map(BackpressureProtocol.retryBackoffMillis) ===
      RetryBackoffLadderMillis, "and the fixture's ladder is the production function's own")
    assert(BackpressureProtocol.retryBackoffMillis(0) === RetryBaseBackoffMillis,
      "an attempt at or below zero is treated as the first, so the function is total")
    assert(BackpressureProtocol.retryBackoffMillis(MaxRetryAttempts + 3) ===
      RetryBackoffLadderMillis.last,
      "and the shift is bounded, so the result can never overflow however it is called")

    // Retransmission is bounded to the unacknowledged window, which is the exact extent of what the
    // producer is still holding and therefore the boundary of the zero-data-loss guarantee.
    assert(protocol.tryAdmit(key, blockBytes, 0L), "one block is sent and retained")
    assert(protocol.isWithinUnacknowledgedWindow(key, 0L), "so it can still be replayed")
    assert(!protocol.isWithinUnacknowledgedWindow(key, 1L), "a block never sent cannot be")
    val retained = retransmitRequest(firstShuffleId, mapId, partitionId, 0L, 0L)
    assert(protocol.canServeRetransmit(key, retained),
      "a request naming only retained blocks is serviceable")
    val unretained = retransmitRequest(firstShuffleId, mapId, partitionId, 1L, 2L)
    assert(!protocol.canServeRetransmit(key, unretained),
      "a request that is not entirely serviceable is refused as a whole, because splicing a " +
        "partial replay into a consumer's input would reorder its partition")

    assert(protocol.nextRetryBackoffMillis(key) === Some(RetryBaseBackoffMillis),
      "a stream that has made no attempt is owed the first pause")
    (1 to MaxRetryAttempts).foreach { attempt =>
      assert(protocol.onRetransmitRequest(key, retained),
        s"replay attempt $attempt names a retained block and must be serviceable")
      assert(protocol.retransmitAttempts(key) === attempt,
        s"and must be counted as attempt $attempt")
    }
    assert(protocol.isRetryExhausted(key), "five attempts spend the budget")
    assert(protocol.nextRetryBackoffMillis(key).isEmpty,
      "a spent budget must escalate rather than report a sixth pause")

    // Progress restores the budget in full, so a later, unrelated failure is not made to inherit an
    // exhausted one.
    assert(protocol.onAck(key, 0L) === blockBytes, "the consumer acknowledges the block")
    assert(protocol.retransmitAttempts(key) === 0, "which resets the attempt count")
    assert(!protocol.isRetryExhausted(key), "and restores the budget")
    assert(protocol.nextRetryBackoffMillis(key) === Some(RetryBaseBackoffMillis),
      "so the ladder starts from its first rung again")

    // A replay occupies the link exactly as an original send does, but it is not new output:
    // counting it as production would inflate the producer's measured rate, and that ratio is
    // exactly what the sustained-slowness fallback trips on.
    assert(protocol.tryAdmit(key, blockBytes, 1L), "a further block is sent")
    val sentBeforeReplay = protocol.sentBytes(key)
    assert(protocol.tryAdmitReplay(key, blockBytes, 1L), "and is then replayed to repair a loss")
    assert(protocol.sentBytes(key) === sentBeforeReplay,
      "a replay must not inflate the producer's measured output")
    assert(protocol.replayedBytes(key) === blockBytes, "it lands on the replay total instead")
    assert(protocol.outstandingBytes(key) === blockBytes,
      "and re-charging a retained block replaces its entry rather than counting it twice")

    // The clock walks the whole ladder, deterministically, with no sleeping anywhere.
    val startMillis = clock.getTimeMillis()
    val instants = advanceThroughRetryBackoff(clock)
    assert(instants.length === MaxRetryAttempts, "one instant per permitted attempt")
    assert(instants === RetryBackoffLadderMillis.scanLeft(startMillis)(_ + _).tail,
      "and each is the previous one plus that attempt's pause")
    assert(clock.getTimeMillis() - startMillis === RetryBackoffLadderMillis.sum,
      "so the whole ladder spans thirty-one seconds of injected time and no real time at all")
  }

  // -----------------------------------------------------------------------------------------------
  // 4. Priority arbitration under concurrent load.
  //
  // Buffer utilisation is an executor-wide quantity that no single shuffle can compute, so each
  // contributes the two numbers it knows and the protocol sums them. Arbitration between the
  // resulting concurrent shuffles is keyed on the two quantities the feature names -- pending
  // volume and reduce partition count -- with the shuffle id breaking every remaining tie so the
  // order is total and never depends on the order a concurrent map happened to enumerate.
  //
  // Concurrency here is real rather than nominal: the threads are released from one barrier, so
  // every one of them is proven to be inside the body before any of them proceeds. Nothing sleeps.
  // -----------------------------------------------------------------------------------------------

  test("priority arbitration under concurrent load") {
    val clock = newManualClock()
    val protocol = newProtocol(clock)

    // Four shuffles chosen so that every rung of the comparison is exercised: the widest is not the
    // most demanding, two hold equal volume with unequal width, and two are equal on both keys.
    val widths = Map(firstShuffleId -> 2, secondShuffleId -> 8, thirdShuffleId -> 8,
      fourthShuffleId -> 8)
    val perStreamBytes = Map(firstShuffleId -> 512L, secondShuffleId -> 2048L,
      thirdShuffleId -> 1024L, fourthShuffleId -> 1024L)
    val streamsPerShuffle = 2
    val shuffleOrder = Seq(firstShuffleId, secondShuffleId, thirdShuffleId, fourthShuffleId)
    shuffleOrder.foreach(shuffleId => protocol.registerShuffle(shuffleId, widths(shuffleId)))
    assert(protocol.registeredShuffleIds === shuffleOrder,
      "the registered set reads back ascending, so a reading of it is always deterministic")
    assert(protocol.numRegisteredShuffles === shuffleOrder.size, "all four shuffles are registered")

    // Every stream is opened and charged from its own thread, all released together.
    val parties = shuffleOrder.size * streamsPerShuffle
    val keys = runConcurrently(parties, "backpressure-arbitration") { index =>
      val shuffleId = shuffleOrder(index / streamsPerShuffle)
      val partition = index % streamsPerShuffle
      val key = producerKey(shuffleId, partition, s"consumer-$shuffleId-$partition")
      assert(protocol.registerStream(key, creditLimitBytes),
        s"thread $index must open the ledger for shuffle $shuffleId partition $partition")
      assert(protocol.tryAdmit(key, perStreamBytes(shuffleId), partition.toLong),
        s"thread $index must be admitted, because uncapped egress paces nothing")
      key
    }
    assert(keys.distinct.size === parties, "every thread must have opened a distinct stream")
    assert(protocol.streamCount === parties,
      "and every one of those ledgers must have survived the contention")
    shuffleOrder.foreach { shuffleId =>
      assert(protocol.streamCount(shuffleId) === streamsPerShuffle,
        s"shuffle $shuffleId must hold exactly its own streams")
      assert(protocol.partitionCountOf(shuffleId) === Some(widths(shuffleId)),
        s"shuffle $shuffleId must report the width it registered")
    }

    // The computed ordering, key by key. Pending volume leads, because bytes held are what the
    // spill threshold actually measures; width breaks a tie on volume, because a wide shuffle has
    // the larger footprint; the id breaks what remains.
    val expectedOrder = Seq(
      BackpressureShuffleDemand(secondShuffleId, 8, 2L * 2048L, streamsPerShuffle),
      BackpressureShuffleDemand(thirdShuffleId, 8, 2L * 1024L, streamsPerShuffle),
      BackpressureShuffleDemand(fourthShuffleId, 8, 2L * 1024L, streamsPerShuffle),
      BackpressureShuffleDemand(firstShuffleId, 2, 2L * 512L, streamsPerShuffle))
    withClue("the arbitration order must be total and keyed on volume then width: ") {
      protocol.arbitrationOrder should be (expectedOrder)
    }
    val firstReading = protocol.arbitrationOrder
    val secondReading = protocol.arbitrationOrder
    assert(firstReading === secondReading,
      "and repeated readings must agree, whatever order the ledger map happened to enumerate")
    assert(protocol.yieldOrder === Seq(secondShuffleId, thirdShuffleId, fourthShuffleId,
      firstShuffleId), "the ids of that order are the sequence in which shuffles must yield")
    assert(protocol.arbitrationOrder.map(_.pendingBytes) === Seq(4096L, 2048L, 2048L, 1024L),
      "pending volume descends across the order")
    assert(protocol.arbitrationOrder.head.numPartitions === 8,
      "the most demanding shuffle carries its width on the result rather than folded into a score")

    // Exactly one shuffle is exempt, and it is the least demanding one. Without the exemption a
    // small shuffle sharing an executor with a large one would be asked to yield on every pass and
    // never finish.
    assert(protocol.guaranteedShuffleId === Some(firstShuffleId),
      "the least demanding shuffle is the one arbitration never asks to yield")
    shuffleOrder.foreach { shuffleId =>
      assert(!protocol.shouldYield(shuffleId),
        s"nobody yields while the executor is under its threshold, including shuffle $shuffleId")
    }

    // Once the executor is over its threshold, everyone but the exempt shuffle gives way.
    shuffleOrder.foreach(shuffleId => protocol.reportBufferUtilization(shuffleId, 80L, 100L))
    assert(protocol.aggregateBufferUtilizationPercent === DefaultSpillThresholdPercent.toLong,
      "four shuffles each at eighty percent aggregate to eighty percent, not to three hundred")
    assert(protocol.isSpillRequired, "so the executor is over its threshold")
    assert(protocol.shouldYield(secondShuffleId), "the most demanding shuffle yields first")
    assert(protocol.shouldYield(thirdShuffleId), "and so does the next")
    assert(protocol.shouldYield(fourthShuffleId), "and the next")
    assert(!protocol.shouldYield(firstShuffleId),
      "but the exempt shuffle is never the one asked to give way")
    assert(!protocol.shouldYield(fourthShuffleId + 1),
      "and a shuffle this protocol has never seen is not asked either")

    // Retiring a shuffle takes its streams, its utilisation contribution and its place in the order
    // with it, which is what stops a finished shuffle from arbitrating forever.
    assert(protocol.unregisterShuffle(secondShuffleId) === streamsPerShuffle,
      "unregistering a shuffle drops every stream belonging to it")
    assert(protocol.streamCount === parties - streamsPerShuffle, "and only those")
    assert(protocol.yieldOrder === Seq(thirdShuffleId, fourthShuffleId, firstShuffleId),
      "so the order re-forms over the shuffles that remain")
    assert(protocol.guaranteedShuffleId === Some(firstShuffleId), "and the exemption follows it")
  }

  test("priority arbitration under concurrent load aggregates utilisation across shuffles") {
    val clock = newManualClock()
    val protocol = newProtocol(clock)
    assert(protocol.spillThresholdPercent === DefaultSpillThresholdPercent,
      "the spill threshold is read once from its typed entry and defaults to eighty percent")
    assert(protocol.aggregateBufferUtilizationPercent === 0L,
      "an executor with no shuffles registered has no utilisation to report")
    assert(!protocol.isSpillRequired, "so nothing needs to be evicted")
    assert(protocol.arbitrationOrder.isEmpty, "and there is nothing to arbitrate between")
    assert(protocol.guaranteedShuffleId.isEmpty, "so no shuffle is exempt from anything")

    // A report for a shuffle that is not registered is ignored rather than allowed to create a
    // contribution with no partition count behind it, which would distort the ordering.
    protocol.reportBufferUtilization(firstShuffleId, 100L, 100L)
    assert(protocol.aggregateBufferUtilizationPercent === 0L,
      "an unregistered shuffle cannot contribute to the executor's reading")

    protocol.registerShuffle(firstShuffleId, 2)
    protocol.registerShuffle(secondShuffleId, 8)

    // The two sums are taken independently and the percentage from the two totals, so one busy
    // shuffle and one idle shuffle on the same executor produce a single honest figure rather than
    // two competing ones.
    protocol.reportBufferUtilization(firstShuffleId, 150L, 100L)
    protocol.reportBufferUtilization(secondShuffleId, 10L, 100L)
    assert(protocol.aggregateBufferUtilizationPercent ===
      BackpressureProtocol.percentOf(160L, 200L),
      "the reading is the ratio of the summed buffers to the summed budgets")
    assert(protocol.aggregateBufferUtilizationPercent === 80L, "which here is eighty percent")
    assert(protocol.isSpillRequired,
      "and the threshold is reached at exactly the configured percentage, not beyond it")

    // A negative reading can only be an accounting slip, and it must not be able to reduce the
    // total, so it is clamped rather than subtracted.
    protocol.reportBufferUtilization(secondShuffleId, -50L, 100L)
    assert(protocol.aggregateBufferUtilizationPercent === 75L,
      "a negative contribution is read as zero")
    assert(!protocol.isSpillRequired, "which puts the executor back under its threshold")

    // An over-budget executor stays visible rather than being masked at a hundred, which is the
    // entire reason a threshold is watched at all.
    protocol.reportBufferUtilization(firstShuffleId, 300L, 100L)
    assert(protocol.aggregateBufferUtilizationPercent === 150L,
      "utilisation is deliberately not clamped at a hundred percent")
    assert(protocol.isSpillRequired, "and an over-budget executor certainly needs to evict")

    // Dropping a shuffle drops its contribution with it.
    assert(protocol.unregisterShuffle(firstShuffleId) === 0,
      "a shuffle with no streams drops none")
    assert(protocol.aggregateBufferUtilizationPercent === 0L,
      "and its buffered bytes leave the executor's reading with it")
    assert(!protocol.isSpillRequired, "so nothing is asked to evict any more")
  }

  test("priority arbitration under concurrent load divides egress by the shuffle count") {
    val clock = newManualClock()
    val declaredMBps = 1000
    val conf = streamingConfWithOverrides(maxBandwidthMBps = Some(declaredMBps))

    // The coordinator is the executor-scoped registry of active shuffles, and it is the SOLE source
    // of the divisor: no existing Spark API reports how many shuffles an executor is serving. Its
    // constructor touches no transport, so a mock endpoint environment is all it needs here.
    val coordinator = new StreamingShuffleCoordinator(mock(classOf[RpcEnv]), conf, clock)
    try {
      val budget = TokenBucketRateLimiter.executorBudget(
        conf, clock, () => Some(coordinator.numConcurrentShuffles))
      val protocol = new BackpressureProtocol(conf, coordinator, budget, clock)

      assert(coordinator.activeShuffleIds.isEmpty, "the registry starts empty")
      assert(coordinator.numConcurrentShuffles === 1,
        "yet the count is clamped to one, which is what makes the bucket's division always safe")
      assert(protocol.numConcurrentShuffles === 1,
        "and the protocol reports the coordinator's count rather than inventing one")

      Seq((firstShuffleId, 4), (secondShuffleId, 8)).foreach { registration =>
        val (shuffleId, numPartitions) = registration
        assert(coordinator.registerShuffle(shuffleId, numPartitions, 1, ProtocolVersion).isDefined,
          s"shuffle $shuffleId must be admitted to the registry at the current wire revision")
        protocol.registerShuffle(shuffleId, numPartitions)
      }
      assert(coordinator.activeShuffleIds === Seq(firstShuffleId, secondShuffleId),
        "both shuffles are now active")
      assert(coordinator.numConcurrentShuffles === 2, "so the count is two")
      assert(protocol.numConcurrentShuffles === 2, "and the protocol reports two")
      assert(protocol.concurrentShuffleIds === Seq(firstShuffleId, secondShuffleId),
        "and arbitrates over exactly the shuffles the registry agrees are active")

      // Registration alone already moved the divisor, because each registration claims a share.
      assert(budget.divisor === 2, "two live limiters divide the cap two ways")
      assert(budget.currentShareBytesPerSecond === Some(refillBytesPerSecond(declaredMBps, 2)),
        "which is exactly the contract's (0.8 * cap) / numConcurrentShuffles")

      // Two views of concurrency exist and the larger is used, because over-estimating paces
      // conservatively and can only keep the aggregate under the cap, whereas under-estimating
      // it is the overrun this arithmetic exists to prevent.
      assert(budget.observeConcurrency(5),
        "a higher count reported by the coordinator moves the divisor")
      assert(budget.divisor === 5, "and is adopted immediately")
      assert(budget.currentShareBytesPerSecond === Some(refillBytesPerSecond(declaredMBps, 5)),
        "so every live limiter paces at a fifth of the ceiling")
      assert(!budget.observeConcurrency(5), "an unchanged count republishes nothing")
      assert(!budget.observeConcurrency(0), "and a non-positive count is ignored outright")
      assert(budget.divisor === 5, "leaving the divisor conservatively high rather than wrong")

      // The declared capacity itself is what saturation is measured against, and it is NOT the
      // limiter's refill rate: the refill rate is already this executor's throttled share, so
      // dividing measured egress by it would report correct pacing as a saturated link.
      assert(protocol.declaredLinkCapacity ===
        Some(declaredMBps.toLong * BytesPerMebibyte),
        "the administered capacity is the operator's declared figure, unmodified")
      assert(budget.currentShareBytesPerSecond.exists(_ < declaredMBps.toLong * BytesPerMebibyte),
        "while a limiter's share is strictly less than it, because the ceiling holds it down")
    } finally {
      coordinator.reset()
    }

    // A protocol with no coordinator falls back to the shuffles registered locally, which is the
    // honest answer in that process and is never zero.
    val local = newProtocol(clock)
    assert(local.numConcurrentShuffles === 1,
      "an executor with no registrations still divides by one rather than by zero")
    Seq(firstShuffleId, secondShuffleId, thirdShuffleId).foreach { shuffleId =>
      local.registerShuffle(shuffleId, partitionCount)
    }
    assert(local.numConcurrentShuffles === 3, "and by its own count once it has one")
    assert(local.concurrentShuffleIds ===
      Seq(firstShuffleId, secondShuffleId, thirdShuffleId),
      "arbitrating over its own registrations unfiltered when no registry is reachable")
  }

  // -----------------------------------------------------------------------------------------------
  // 5. Telemetry: the backpressure event counter is edge-triggered.
  //
  // The metric an operator reads must answer "how many times did flow control engage", not "how
  // often did a producer happen to retry". Those are different numbers, and the second one grows
  // with retry frequency rather than with anything about the job, so the counter is advanced on the
  // transition into a throttled state and not on each refusal inside it.
  // -----------------------------------------------------------------------------------------------

  test("backpressure events are edge-triggered rather than counted per refusal") {
    assert(streamingShuffleMetricNames() === MetricNames.toSet,
      "exactly four streaming shuffle metrics exist and there is no fifth")
    assert(MetricNames.contains(BackpressureEventsMetricName),
      "and the counter this protocol owns is one of them")

    val baseline = observedBackpressureEvents()
    assert(baseline === 0L,
      "beforeEach returns the JVM-singleton metric source to zero, which is what makes an " +
        "absolute assertion on a process-wide counter safe")

    val clock = newManualClock()
    val protocol = newProtocol(clock)
    val key = producerKey()
    protocol.registerShuffle(firstShuffleId, partitionCount)
    assert(protocol.registerStream(key, creditLimitBytes), "the ledger must open")
    assert(protocol.backpressureEventCount === 0L, "a fresh protocol has counted nothing")

    // Fill the window, then refuse the same block over and over. A producer holding one block and
    // retrying it is ONE episode of backpressure however many times it tries.
    assert(protocol.tryAdmit(key, creditLimitBytes, 0L), "the whole allowance goes out at once")
    assert(!protocol.hasCredit(key), "so no credit remains")
    val refusals = 64
    (0 until refusals).foreach { attempt =>
      assert(!protocol.tryAdmit(key, creditLimitBytes, 1L),
        s"retry $attempt of the held block must be refused while the window is full")
    }
    assert(protocol.backpressureEventCount === 1L,
      s"$refusals refusals form ONE throttling episode and must be counted exactly once")
    assert(observedBackpressureEvents() - baseline === 1L,
      "and the operator-facing counter must agree with the protocol's own total")

    // Recovery closes the episode, so a later entry counts a fresh edge rather than being
    // suppressed by a flag nobody cleared.
    assert(protocol.onAck(key, 0L) === creditLimitBytes, "the acknowledgement restores credit")
    assert(!protocol.isStreamThrottled(key), "which ends the episode")
    assert(protocol.tryAdmit(key, creditLimitBytes, 1L), "the held block finally goes out")
    (0 until refusals).foreach { attempt =>
      assert(!protocol.tryAdmit(key, creditLimitBytes, 2L),
        s"retry $attempt of the next block must be refused")
    }
    assert(protocol.backpressureEventCount === 2L, "the second episode is the second event")
    assert(observedBackpressureEvents() - baseline === 2L, "as the operator sees it too")

    // The consumer side reports its own edges explicitly, because a poll can confirm a return to
    // flowing but can never enter a throttled state, so without this entry point consumer-side
    // throttling would be absent from the metric altogether.
    assert(!protocol.noteThrottled(key, "a stream already throttled is not a new edge"),
      "an already throttled stream reports no edge")
    assert(protocol.backpressureEventCount === 2L, "and counts nothing")
    assert(protocol.noteResumed(key), "resuming a throttled stream is an edge")
    assert(!protocol.noteResumed(key), "resuming a flowing stream is not")
    assert(protocol.noteThrottled(key, "the consumer stopped reading its socket"),
      "and throttling a flowing stream is an edge again")
    assert(protocol.backpressureEventCount === 3L, "which is the third event")
    assert(!protocol.noteThrottled(key, "still throttled"), "and staying throttled is not a fourth")
    assert(protocol.backpressureEventCount === 3L, "so the counter does not move")

    // The executor-wide consumer receive budget counts one edge per episode of exhaustion too,
    // while still counting every refusal separately: an operator needs both the episode count and
    // the volume, and they are different questions.
    assert(protocol.receiveQuotaBytes > 0L, "the executor has a consumer receive budget")
    assert(protocol.reservedReceiveQuotaBytes === 0L, "and none of it is held yet")
    assert(protocol.tryReserveReceiveQuota(protocol.receiveQuotaBytes),
      "a reservation for the whole budget fits exactly")
    assert(protocol.receiveQuotaExhausted, "and commits it in full")
    (0 until refusals).foreach { attempt =>
      assert(!protocol.tryReserveReceiveQuota(1L),
        s"refusal $attempt must be refused rather than waited out, because the caller is a " +
          "network event-loop thread")
    }
    assert(protocol.receiveQuotaRefusalCount === refusals.toLong,
      "every refusal is counted, because the volume is what tells an operator how far behind the " +
        "reduce side is")
    assert(protocol.backpressureEventCount === 4L,
      "but the episode is ONE backpressure event however many frames were refused inside it")
    protocol.releaseReceiveQuota(protocol.receiveQuotaBytes)
    assert(protocol.reservedReceiveQuotaBytes === 0L, "releasing returns the whole budget")
    assert(!protocol.receiveQuotaExhausted, "so it is no longer committed")
    assert(!protocol.tryReserveReceiveQuota(protocol.receiveQuotaBytes + 1L),
      "a reservation one byte beyond the budget is refused")
    assert(protocol.backpressureEventCount === 5L,
      "and because the release closed the previous episode, that refusal is a fresh edge")
    assert(observedBackpressureEvents() - baseline === 5L,
      "five episodes, five events, and not one event per refusal")

    // A non-positive reservation is granted without touching the ledger, so a control frame that
    // carries no payload needs no special case at the call site.
    assert(protocol.tryReserveReceiveQuota(0L), "a zero-sized reservation is free")
    assert(protocol.reservedReceiveQuotaBytes === 0L, "and charges nothing")
    assert(protocol.backpressureEventCount === 5L, "and is not an event")
  }

  // -----------------------------------------------------------------------------------------------
  // 6. The full state machine.
  //
  // Four states in a strict severity order, so that one reading can describe a set of streams: the
  // executor is in the most severe state any of its streams is in. Degradation dominates all of
  // them, because a subsystem that has to step aside is in no position to call itself merely
  // throttled; spilling dominates throttling, because utilisation is executor-wide while credit
  // is per stream.
  //
  // Eighty percent and ninety percent are DIFFERENT constants here. Eighty is the default spill
  // threshold and, separately, the bandwidth ceiling; ninety is where measured link saturation
  // trips the fallback. Conflating them is the easiest mistake available in this file.
  // -----------------------------------------------------------------------------------------------

  test("the backpressure state machine covers every specified transition") {
    val clock = newManualClock()
    val protocol = newPacedProtocol(clock)
    val key = producerKey()
    protocol.registerShuffle(firstShuffleId, partitionCount)
    assert(protocol.registerStream(key, creditLimitBytes), "the ledger must open")

    // Flowing is the resting state.
    assert(protocol.state === BackpressureState.Flowing, "a protocol with nothing wrong is flowing")
    assert(protocol.streamState(key) === BackpressureState.Flowing, "and so is each of its streams")
    assert(protocol.throttledStreamCount === 0, "with nothing held back")
    assert(!protocol.isDegraded, "and nothing latched against it")

    // Flowing -> Throttled, because the consumer's credit is exhausted.
    assert(protocol.tryAdmit(key, creditLimitBytes, 0L), "the whole allowance goes out")
    assert(!protocol.tryAdmit(key, creditLimitBytes, 1L), "so the next block is refused on credit")
    assert(protocol.state === BackpressureState.Throttled, "which throttles the executor's reading")
    assert(protocol.streamState(key) === BackpressureState.Throttled, "and the stream's")
    assert(protocol.throttledStreamCount === 1, "with exactly one stream held back")

    // Throttled -> Flowing, because an acknowledgement advanced the consumer position.
    assert(protocol.onAck(key, 0L) === creditLimitBytes, "the acknowledgement releases the window")
    assert(protocol.confirmReclamation(key) === Some(0L),
      "and the owner of the freed buffers confirms the release at once")
    assert(protocol.state === BackpressureState.Flowing, "which returns the executor to flowing")
    assert(protocol.throttledStreamCount === 0, "with nothing held back again")

    // Flowing -> Throttled, this time because the pacing bucket is exhausted rather than the
    // credit. The two causes are distinct, and the second needs a declared link capacity.
    //
    // The paced stream belongs to a SECOND shuffle, and that is load-bearing rather than tidiness:
    // a bucket is per shuffle, so a stream sharing the first shuffle's bucket would find it partly
    // spent by the credit episode above, and the refusal would no longer be attributable to a full
    // bucket having been drained by this stream alone.
    protocol.registerShuffle(secondShuffleId, partitionCount)
    val pacedKey = producerKey(secondShuffleId, partitionId, "consumer-paced")
    val generousCredit = 8L * MaxEncodedFrameBytes.toLong
    assert(protocol.registerStream(pacedKey, generousCredit), "a stream with ample credit opens")
    assert(protocol.tryAdmit(pacedKey, MaxEncodedFrameBytes.toLong, 0L),
      "a bucket starts full, so the first maximum-sized frame is admitted")
    assert(protocol.availableCreditBytes(pacedKey) > 0L,
      "credit still remains, so whatever refuses the next frame can only be the pacing bucket")
    assert(!protocol.tryAdmit(pacedKey, MaxEncodedFrameBytes.toLong, 1L),
      "and the drained bucket does refuse it")
    assert(protocol.isStreamThrottled(pacedKey), "which throttles the stream")
    assert(protocol.state === BackpressureState.Throttled, "and the executor")

    // Throttled -> Flowing, because the bucket refilled. A poll is what confirms the return, since
    // an acknowledgement processed on a network thread had no reason to consult the pacing layer.
    //
    // One second of the share is deliberately not enough here: the burst allowance is floored at
    // one maximum-sized encoded frame, and at this modest declared capacity that floor is larger
    // than a second of the share. Ten seconds of injected time is comfortably more than the bucket
    // needs and costs nothing, because no wall clock is involved.
    clock.advance(10L * millisPerSecond)
    assert(protocol.pollOnce() === 1,
      "a poll re-evaluates every stream and reports the one that changed state")
    assert(!protocol.isStreamThrottled(pacedKey), "so the paced stream is flowing again")
    assert(protocol.tryAdmit(pacedKey, MaxEncodedFrameBytes.toLong, 1L),
      "and the refilled bucket admits the frame the caller was holding")
    assert(protocol.state === BackpressureState.Flowing, "leaving the executor flowing")
    assert(!protocol.isPollDue, "and the poll cadence restarts from the poll just taken")
    assert(protocol.lastPollTimeMillis === clock.getTimeMillis(),
      "recorded from the injected clock")

    // Flowing -> Spilling, because aggregate buffer utilisation reached the spill threshold.
    protocol.reportBufferUtilization(firstShuffleId, 80L, 100L)
    assert(protocol.aggregateBufferUtilizationPercent === DefaultSpillThresholdPercent.toLong,
      "the executor is at exactly its configured threshold")
    assert(protocol.isSpillRequired, "so buffered partitions must be evicted")
    assert(protocol.state === BackpressureState.Spilling, "which is what the executor now reports")
    assert(protocol.streamState(key) === BackpressureState.Spilling,
      "and utilisation is executor-wide, so it overlays every stream")

    // Throttled -> Spilling: a stream that is ALSO held back reports the more severe of the two,
    // because utilisation is executor-wide while credit is per stream.
    assert(protocol.tryAdmit(key, creditLimitBytes, 1L), "the credit-bound stream sends again")
    assert(!protocol.tryAdmit(key, creditLimitBytes, 2L), "and is refused again")
    assert(protocol.isStreamThrottled(key), "so it is genuinely throttled")
    assert(protocol.state === BackpressureState.Spilling, "yet spilling dominates the reading")
    assert(protocol.streamState(key) === BackpressureState.Spilling, "for that stream too")

    // Spilling -> Flowing: eviction completed, and the buffers the acknowledgement freed were
    // released inside the hundred-millisecond bound.
    assert(protocol.onAck(key, 1L) === creditLimitBytes, "the acknowledgement frees the buffers")
    clock.advance(ReclamationDeadlineMillis)
    assert(protocol.confirmReclamation(key) === Some(ReclamationDeadlineMillis),
      "and the owner confirms the release at the bound")
    assert(protocol.reclamationDeadlineBreaches === 0L, "which is not a breach")
    protocol.reportBufferUtilization(firstShuffleId, 10L, 100L)
    assert(!protocol.isSpillRequired, "the eviction brought the executor back under its threshold")
    assert(protocol.state === BackpressureState.Flowing, "so it is flowing once more")

    // Spilling -> Degraded: an allocation that still fails after a spill is an OOM risk, and
    // continuing would cost the job its heap rather than merely its throughput.
    protocol.reportBufferUtilization(firstShuffleId, 95L, 100L)
    assert(protocol.state === BackpressureState.Spilling,
      "the executor is over its threshold again")
    protocol.reportBufferAllocationFailure(key)
    assert(protocol.state === BackpressureState.Degraded, "and an allocation failure degrades it")
    assert(protocol.degradationReasons ===
      Seq(BackpressureDegradationReason.BufferAllocationFailure),
      "naming the second of the four conditions under which streaming steps aside")

    // Degraded is latched, because the fallback policy may sample it after the condition that
    // caused it has passed, so an explicit clear is the only way back.
    protocol.clearDegradation()
    assert(!protocol.isDegraded, "clearing forgets every latched reason")
    assert(protocol.state === BackpressureState.Spilling, "revealing the condition underneath it")
    protocol.reportBufferUtilization(firstShuffleId, 10L, 100L)
    assert(protocol.state === BackpressureState.Flowing, "which is itself cleared by eviction")

    // Throttled -> Degraded: the consumer has been at least twice as slow as its producer,
    // continuously, for longer than sixty seconds. The window must be continuous, so a workload
    // that oscillates is never tripped by an accumulation of unrelated slow moments.
    val slowKey = producerKey(partition = 2, consumer = "consumer-slow")
    assert(protocol.registerStream(slowKey, generousCredit), "a third stream opens")
    assert(protocol.tryAdmit(slowKey, blockBytes, 0L), "and sends one block")
    assert(protocol.isConsumerSlow(slowKey),
      "a consumer that has acknowledged nothing while its producer has sent something is slower " +
        "by any ratio, which is the correct reading rather than a degenerate one")
    assert(!protocol.isConsumerSustainedSlow(slowKey),
      "the first observation only records when the imbalance began")
    advanceJustBeforeSustainedSlownessWindow(clock)
    assert(!protocol.isConsumerSustainedSlow(slowKey),
      "the sustained-slowness window has NOT elapsed at 59999 ms")
    clock.advance(1L)
    assert(!protocol.isConsumerSustainedSlow(slowKey),
      "and the trip is strictly beyond sixty seconds, so it does not fire at exactly 60000 ms")
    clock.advance(1L)
    assert(protocol.isConsumerSustainedSlow(slowKey), "but it does fire at 60001 ms")
    assert(protocol.state === BackpressureState.Degraded, "which degrades the executor")
    assert(protocol.degradationReasons ===
      Seq(BackpressureDegradationReason.ConsumerSustainedSlowness),
      "naming the first of the four conditions")

    // Recovery clears the latch, which is what makes the window continuous rather than cumulative.
    protocol.clearDegradation()
    assert(protocol.onAck(slowKey, 0L) === blockBytes, "the consumer catches up")
    assert(!protocol.isConsumerSlow(slowKey),
      "a consumer that has caught up entirely is not slow, however little it consumed in total")
    assert(!protocol.isConsumerSustainedSlow(slowKey), "so the latch is cleared with it")
    assert(protocol.state === BackpressureState.Flowing, "and the executor is flowing")

    // Flowing -> Degraded: MEASURED link usage beyond ninety percent of the DECLARED capacity.
    // Neither term is the pacing bucket. Token scarcity is not link saturation: a bucket empties
    // whenever a burst briefly outruns its refill rate, which is routine on an idle link, and it
    // empties persistently on a stream the limiter is pacing exactly as configured.
    val declaredCapacityBytes = linkCapacityMBps.toLong * BytesPerMebibyte
    assert(protocol.declaredLinkCapacity === Some(declaredCapacityBytes),
      "saturation is taken against the capacity the operator declared")
    assert(LinkSaturationTripPercent === 90L, "and the trip is at ninety percent of it")
    assert(protocol.linkSaturationPercent === 0L,
      "a link quiet for longer than two sampling intervals reads as idle, not as saturated")
    assert(!protocol.isLinkSaturated, "so it has not tripped")

    // A rate is only meaningful over a closed interval, so the window is re-based and then exactly
    // one sampling interval is made to carry a known volume. The bytes are recorded as ingress on
    // an unregistered stream, so the reading is of the link and not of any ledger.
    val observedKey = consumerKey(partition = 3, consumer = "consumer-observed")
    val atNinetyPercent =
      (LinkSaturationTripPercent * declaredCapacityBytes + PercentScale - 1L) / PercentScale
    val aboveNinetyPercent =
      ((LinkSaturationTripPercent + 1L) * declaredCapacityBytes + PercentScale - 1L) / PercentScale
    assert(BackpressureProtocol.percentOf(atNinetyPercent, declaredCapacityBytes) ===
      LinkSaturationTripPercent, "the first rate reads as exactly ninety percent of the capacity")
    assert(BackpressureProtocol.percentOf(aboveNinetyPercent, declaredCapacityBytes) >
      LinkSaturationTripPercent, "and the second as strictly more than ninety")

    protocol.onDataReceived(observedKey, 0L, 1L)
    clock.advance(BackpressureProtocol.SATURATION_SAMPLE_WINDOW_MS)
    protocol.onDataReceived(observedKey, 1L, atNinetyPercent)
    assert(protocol.ingressBytesPerSecond === atNinetyPercent,
      "one sampling interval carrying that volume publishes exactly that rate")
    assert(protocol.linkSaturationPercent === LinkSaturationTripPercent, "which is ninety percent")
    assert(!protocol.isLinkSaturated,
      "the trip is strictly beyond ninety percent, so exactly ninety does NOT fire")
    assert(protocol.state === BackpressureState.Flowing, "and the executor stays flowing")

    clock.advance(BackpressureProtocol.SATURATION_SAMPLE_WINDOW_MS)
    protocol.onDataReceived(observedKey, 2L, aboveNinetyPercent)
    assert(protocol.ingressBytesPerSecond === aboveNinetyPercent, "one interval later, one rung up")
    assert(protocol.linkSaturationPercent > LinkSaturationTripPercent, "the link is beyond ninety")
    assert(protocol.isLinkSaturated, "so saturation trips")
    assert(protocol.state === BackpressureState.Degraded, "and the executor degrades")
    assert(protocol.degradationReasons === Seq(BackpressureDegradationReason.LinkSaturation),
      "naming the third of the four conditions")
    assert(protocol.ingressBytes >= aboveNinetyPercent,
      "and the cumulative total counts every byte the link carried")
    protocol.clearDegradation()

    // Flowing -> Degraded: a peer stamped a message with a wire revision this executor does not
    // speak. That is a compatibility check rather than a parse failure, which is exactly why the
    // header carries a protocol version at all.
    assert(protocol.observeProtocolVersion(key, ProtocolVersion),
      "the current wire revision is compatible")
    assert(!protocol.isDegraded, "so nothing is latched against it")
    assert(protocol.state === BackpressureState.Flowing, "and the executor is flowing")
    val foreignVersion = (ProtocolVersion + 1).toByte
    assert(!protocol.observeProtocolVersion(key, foreignVersion),
      "a revision this executor does not speak is refused")
    assert(protocol.state === BackpressureState.Degraded, "which degrades the executor")
    assert(protocol.degradationReasons ===
      Seq(BackpressureDegradationReason.ProtocolVersionMismatch),
      "naming the fourth of the four conditions")

    // All four reasons are reachable, they accumulate rather than replace one another, and they are
    // reported in the order they are declared so a log line is never at the mercy of set iteration.
    protocol.reportBufferAllocationFailure(key)
    assert(protocol.degradationReasons === Seq(
      BackpressureDegradationReason.BufferAllocationFailure,
      BackpressureDegradationReason.ProtocolVersionMismatch),
      "more than one condition can hold at once and an operator needs to see all of them")
    assert(BackpressureDegradationReason.values.length === 4,
      "there are exactly four conditions under which streaming steps aside")
    assert(BackpressureState.values.map(_.name) ===
      Seq("Flowing", "Throttled", "Spilling", "Degraded"),
      "and exactly four states, in ascending severity")
    assert(BackpressureState.values.map(_.severity) === Seq(0, 1, 2, 3),
      "whose severities are what let one reading describe a whole set of streams")

    // Degraded is the terminus this class publishes. Delegating to the sort-based manager is the
    // fallback policy's decision and the shuffle manager's action, not this protocol's: publishing
    // the reason is the whole of its part, and that division is what keeps the subsystem free of a
    // component that both measures and reacts.
    protocol.clearDegradation()
    assert(protocol.state === BackpressureState.Flowing, "clearing is the only way back")

    // And a reset returns the protocol to exactly what a freshly constructed one reports.
    protocol.reset()
    assert(protocol.streamCount === 0, "every ledger is dropped")
    assert(protocol.numRegisteredShuffles === 0, "every registration with them")
    assert(protocol.aggregateBufferUtilizationPercent === 0L, "every utilisation contribution too")
    assert(protocol.backpressureEventCount === 0L, "the local event total is rewound")
    assert(protocol.reclamationDeadlineBreaches === 0L, "so is the breach total")
    assert(!protocol.isDegraded, "no reason stays latched")
    assert(protocol.state === BackpressureState.Flowing, "and the state is the resting one")
  }

  // -----------------------------------------------------------------------------------------------
  // 7. Thread model: the protocol never calls a shuffle metrics reporter.
  //
  // The read and write metrics reporters document that they assume single-threaded use, so an
  // implementation of one is entitled to skip synchronisation. This protocol is consulted from the
  // task thread and from network event-loop threads alike, so calling a reporter from here would
  // break that promise silently. The Dropwizard metric source is different in kind -- it is a
  // lock-free registry -- and is the only telemetry this protocol advances.
  // -----------------------------------------------------------------------------------------------

  test("the protocol never calls a shuffle metrics reporter") {
    val reporterTypes =
      Seq(classOf[ShuffleReadMetricsReporter], classOf[ShuffleWriteMetricsReporter])

    // Structurally: no constructor and no method of the protocol so much as mentions a reporter, so
    // there is no seam through which one could be reached.
    val constructorMentions = classOf[BackpressureProtocol].getDeclaredConstructors
      .flatMap(_.getParameterTypes)
      .filter(parameter => reporterTypes.exists(_.isAssignableFrom(parameter)))
    assert(constructorMentions.isEmpty,
      "no constructor of the protocol may take a metrics reporter, yet it takes " +
        constructorMentions.map(_.getName).mkString(", "))
    val methodMentions = classOf[BackpressureProtocol].getDeclaredMethods.filter { method =>
      (method.getParameterTypes.toSeq :+ method.getReturnType)
        .exists(candidate => reporterTypes.exists(_.isAssignableFrom(candidate)))
    }
    assert(methodMentions.isEmpty,
      "no method of the protocol may accept or return a metrics reporter, yet these do: " +
        methodMentions.map(_.getName).mkString(", "))

    // Behaviourally: recorders that would notice a single call stay silent while every path this
    // suite exercises is driven, and the lock-free counter the protocol may advance does move.
    val writeMetrics = new RecordingStreamingShuffleWriteMetrics
    val readMetrics = new RecordingStreamingShuffleReadMetrics
    val baseline = observedBackpressureEvents()
    val clock = newManualClock()
    val protocol = newProtocol(clock)
    val key = producerKey()
    protocol.registerShuffle(firstShuffleId, partitionCount)
    assert(protocol.registerStream(key, creditLimitBytes), "the ledger must open")
    assert(protocol.tryAdmit(key, creditLimitBytes, 0L), "one full window goes out")
    assert(!protocol.tryAdmit(key, creditLimitBytes, 1L), "the next block is throttled")
    assert(protocol.onAck(key, 0L) === creditLimitBytes, "and then acknowledged")
    protocol.onDataReceived(key, 1L, blockBytes)
    protocol.onDiscardedData(key, blockBytes)
    protocol.onHeartbeat(key)
    assert(protocol.heartbeatFor(key, 2L).isDefined, "a heartbeat is built")
    protocol.reportBufferUtilization(firstShuffleId, 10L, 100L)
    assert(protocol.pollOnce() >= 0, "a poll runs")
    assert(!protocol.isConsumerSustainedSlow(key), "slowness is observed")
    protocol.reportBufferAllocationFailure(key)
    assert(protocol.isDegraded, "a reason is latched")
    protocol.clearDegradation()
    protocol.onStreamTermination(key, 2L)
    assert(protocol.unregisterShuffle(firstShuffleId) === 1, "and the shuffle is retired")

    assert(writeMetrics.callCount === 0L,
      "the protocol must never call a write metrics reporter, because the write path reports " +
        "from the task thread while the protocol runs on network threads too")
    assert(readMetrics.callCount === 0L, "and it must never call a read metrics reporter either")
    assertSingleThreadedReporting(
      writeMetrics.foreignThreadCallCount, "the streaming shuffle write reporter")
    assertSingleThreadedReporting(
      readMetrics.foreignThreadCallCount, "the streaming shuffle read reporter")
    assert(observedBackpressureEvents() - baseline === 1L,
      "while the lock-free Dropwizard counter, which is safe from any thread, did advance")
  }

  // -----------------------------------------------------------------------------------------------
  // 8. Configuration: consumed from its typed entries, never re-declared.
  //
  // The five streaming shuffle properties are declared exactly once, in the core configuration
  // package object, and every fixture and component reads them through those typed entries. A range
  // is enforced by the entry's own validator at READ time, so a value outside it never reaches a
  // component at all -- and because the validator raises an EXISTING error condition, the streaming
  // shuffle adds no error-catalogue entry for configuration validation.
  // -----------------------------------------------------------------------------------------------

  test("the spill threshold and the receive budget are consumed from their typed entries") {
    val clock = newManualClock()
    Seq(MinSpillThresholdPercent, DefaultSpillThresholdPercent, MaxSpillThresholdPercent)
      .foreach { threshold =>
        val conf = streamingConfWithOverrides(spillThreshold = threshold)
        val protocol = protocolWith(conf, clock, null)
        assert(protocol.spillThresholdPercent === threshold,
          s"the protocol must hold the configured threshold of $threshold percent")
        protocol.registerShuffle(firstShuffleId, partitionCount)
        protocol.reportBufferUtilization(firstShuffleId, threshold.toLong - 1L, 100L)
        assert(!protocol.isSpillRequired,
          s"one percent below $threshold must not require a spill")
        protocol.reportBufferUtilization(firstShuffleId, threshold.toLong, 100L)
        assert(protocol.isSpillRequired,
          s"and exactly $threshold percent must, because the threshold is inclusive")
      }

    // A value outside the documented range is refused where the entry is read.
    val rejected = intercept[SparkIllegalArgumentException] {
      protocolWith(
        streamingConfWithOverrides(spillThreshold = MaxSpillThresholdPercent + 1), clock, null)
    }
    assert(rejected.getCondition === "INVALID_CONF_VALUE.REQUIREMENT",
      "the rejection uses the existing configuration-validation condition")
    assert(rejected.getSqlState === "22022", "at its existing SQLSTATE")
    assert(rejected.getMessage.contains("The spill threshold must be in [50, 95]."),
      "and quotes the entry's own requirement verbatim")

    // The consumer receive budget is the same percentage of the same executor memory the producer
    // side is bounded by, read from the same entry, so the two halves of a streaming shuffle are
    // sized from one number rather than from two that could drift.
    val quotaConf = streamingConfWithOverrides(bufferSizePercent = MaxBufferSizePercent)
    val quotaProtocol = protocolWith(quotaConf, clock, null)
    val executorMemoryBytes = quotaConf.get(EXECUTOR_MEMORY) * BytesPerMebibyte
    assert(quotaProtocol.receiveQuotaBytes ===
      executorMemoryBytes / PercentScale * MaxBufferSizePercent.toLong,
      "the budget is bufferSizePercent of the executor's memory")
    assert(quotaProtocol.receiveQuotaBytes > MaxEncodedFrameBytes.toLong,
      "which on a default executor is comfortably more than one maximum-sized frame")

    // On an executor too small for that percentage to cover one frame the budget is floored at one
    // frame, because a budget that could not admit the largest legal block would refuse every block
    // and make no progress at all.
    val tinyConf = streamingConfWithOverrides(bufferSizePercent = MinBufferSizePercent)
      .set(EXECUTOR_MEMORY, 1L)
    val tinyProtocol = protocolWith(tinyConf, clock, null)
    assert(tinyProtocol.receiveQuotaBytes === MaxEncodedFrameBytes.toLong,
      "the receive budget is floored at one maximum-sized encoded frame")
    assert(tinyProtocol.tryReserveReceiveQuota(MaxEncodedFrameBytes.toLong),
      "so the largest legal frame is always admissible")
  }
}
