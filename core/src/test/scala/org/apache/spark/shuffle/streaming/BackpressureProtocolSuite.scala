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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.mockito.Mockito.mock
import org.scalatest.matchers.should.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite, SparkIllegalArgumentException, TaskContext}
import org.apache.spark.internal.config.{EXECUTOR_MEMORY, SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT,
  SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, SHUFFLE_STREAMING_SPILL_THRESHOLD}
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, StreamingShuffleMessageType}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.shuffle.{ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter}
import org.apache.spark.util.{Clock, ManualClock}

/**
 * Unit tests for [[BackpressureProtocol]] and [[TokenBucketRateLimiter]], which together own the
 * streaming shuffle's flow control.
 */
class BackpressureProtocolSuite extends SparkFunSuite
  with Matchers
  with StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  private val firstShuffleId: Int = 0
  private val secondShuffleId: Int = 1
  private val thirdShuffleId: Int = 2
  private val fourthShuffleId: Int = 3

  private val mapId: Long = 0L
  private val taskAttemptId: Long = 0L
  private val partitionId: Int = 0

  private val consumerId: String = "consumer-a"

  private val partitionCount: Int = 4

  private val creditLimitBytes: Long = 4096L

  private val blockBytes: Long = 1024L

  private val millisPerSecond: Long = BackpressureProtocol.MILLIS_PER_SECOND

  private val linkCapacityMBps: Int = 1

  /** Returns the four streaming shuffle metrics to zero before every case. */
  protected override def beforeEach(): Unit = {
    super.beforeEach()
    MemorySpillManager.resetSharedStateForTesting()
    resetStreamingShuffleMetrics()
  }

  /** Returns the same process-scoped state to zero after every case as well as before it. */
  protected override def afterEach(): Unit = {
    try {
      super.afterEach()
    } finally {
      resetStreamingShuffleMetrics()
      MemorySpillManager.resetSharedStateForTesting()
    }
  }

  /**
   * A clock that counts every reading taken through it.
   *
   * @param delegate the clock every reading is answered from
   */
  private class ReadCountingClock(delegate: ManualClock) extends Clock {

    private val reads = new AtomicLong(0L)

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

  private def producerKey(
      shuffleId: Int = firstShuffleId,
      partition: Int = partitionId,
      consumer: String = consumerId): BackpressureStreamKey = {
    BackpressureStreamKey.forProducer(shuffleId, mapId, taskAttemptId, partition, consumer)
  }

  private def consumerKey(
      shuffleId: Int = firstShuffleId,
      partition: Int = partitionId,
      consumer: String = consumerId): BackpressureStreamKey = {
    BackpressureStreamKey.forConsumer(shuffleId, mapId, taskAttemptId, partition, consumer)
  }

  private def protocolWith(
      conf: SparkConf,
      clock: ManualClock,
      coordinator: StreamingShuffleCoordinator): BackpressureProtocol = {
    MemorySpillManager.resetSharedStateForTesting()
    new BackpressureProtocol(
      conf, coordinator, TokenBucketRateLimiter.executorBudget(conf, clock), clock)
  }

  /**
   * The common fixture: uncapped egress, no coordinator, a frozen clock.
   *
   * @param clock the frozen clock the case controls
   */
  private def newProtocol(clock: ManualClock): BackpressureProtocol = {
    protocolWith(streamingConfWithOverrides(), clock, null)
  }

  private def newPacedProtocol(clock: ManualClock): BackpressureProtocol = {
    protocolWith(
      streamingConfWithOverrides(maxBandwidthMBps = Some(linkCapacityMBps)), clock, null)
  }

  /** Advances the injected clock far enough for the executor-wide egress ceiling to top back up. */
  private def accrueExecutorEgressCeiling(clock: ManualClock): Unit = {
    clock.advance(millisPerSecond / 100L)
  }

  /**
   * An allowance no other component shares, sized so that no ledger charge is ever refused, which
   * is what lets a case assert on the exact number of bytes the protocol holds.
   */
  private def ledgerAccountingQuota(): MemorySpillManager.ExecutorBufferQuota = {
    new MemorySpillManager.ExecutorBufferQuota(
      bufferSizePercent = 50,
      spillThresholdPercent = DefaultSpillThresholdPercent,
      executorMemoryProvider = () => 64L * BytesPerMebibyte)
  }

  test("data-plane worker stripes preserve per-owner FIFO and expose idle completion") {
    val protocol = newProtocol(newManualClock())
    val owner = new Object()
    val observed = new Array[Int](128)
    val next = new AtomicInteger(0)

    observed.indices.foreach { value =>
      assert(protocol.executeDataPlane(owner, new Runnable {
        override def run(): Unit = {
          observed(next.getAndIncrement()) = value
        }
      }), s"task $value must fit in an otherwise empty worker stripe")
    }

    assert(protocol.awaitDataPlaneIdle(10000L),
      "the completion primitive must report when every accepted worker task has settled")
    assert(next.get() === observed.length)
    assert(observed.toSeq === observed.indices,
      "one handler's tasks must execute in submission order on its stable worker stripe")
  }

  test("data-plane stripes reject beyond their fixed queue instead of running on the caller") {
    val protocol = newProtocol(newManualClock())
    assert(protocol.awaitDataPlaneIdle(10000L), "the shared workers must start this case idle")
    val owner = new Object()
    val blockerStarted = new CountDownLatch(1)
    val releaseBlocker = new CountDownLatch(1)
    val executed = new AtomicInteger(0)

    assert(protocol.executeDataPlane(owner, new Runnable {
      override def run(): Unit = {
        blockerStarted.countDown()
        try {
          releaseBlocker.await(30L, TimeUnit.SECONDS)
        } catch {
          case _: InterruptedException => Thread.currentThread().interrupt()
        }
        executed.incrementAndGet()
      }
    }))
    assert(blockerStarted.await(10L, TimeUnit.SECONDS),
      "the first task must occupy its stripe before the queue is filled")

    try {
      (0 until BackpressureProtocol.DATA_PLANE_TASKS_PER_STRIPE).foreach { index =>
        assert(protocol.executeDataPlane(owner, new Runnable {
          override def run(): Unit = {
            executed.incrementAndGet()
          }
        }), s"bounded queue slot $index must be admitted")
      }
      val overflowRan = new AtomicInteger(0)
      assert(!protocol.executeDataPlane(owner, new Runnable {
        override def run(): Unit = {
          overflowRan.incrementAndGet()
        }
      }), "the first task beyond the fixed stripe capacity must fail closed")
      assert(overflowRan.get() === 0,
        "a refused task must never run synchronously on the submitting thread")
    } finally {
      releaseBlocker.countDown()
    }

    assert(protocol.awaitDataPlaneIdle(10000L),
      "every admitted task must settle after the blocked stripe is released")
    assert(executed.get() === BackpressureProtocol.DATA_PLANE_TASKS_PER_STRIPE + 1)
  }

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

    assert(!protocol.tryAdmit(key, blockSize, 4L),
      "a block that does not fit the remaining credit must be refused")
    assert(protocol.outstandingBytes(key) === creditLimitBytes,
      "a refused block must leave the window entirely uncharged so it can be retried unchanged")
    assert(protocol.isStreamThrottled(key),
      "a refused admission puts its stream in the throttled state")
    assert(protocol.state === BackpressureState.Throttled,
      "the executor reports the most severe state any of its streams is in")

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

    assert(protocol.onAck(key, 1L) === 0L, "a duplicate acknowledgement releases nothing")
    assert(protocol.onAck(key, 0L) === 0L, "a reordered acknowledgement releases nothing")
    assert(protocol.acknowledgedPosition(key) === 1L,
      "acknowledgement positions are monotonic and a stale one must not pull the cursor back")

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

    verifyWireContractAgainstEncoder()
    val acknowledgement = ack(firstShuffleId, mapId, partitionId, 0L)
    assert(HeaderEncodedLength === 17,
      "the streaming wire header is seventeen bytes: version, shuffle, partition, sequence")
    assert(FixedMessageEncodedLength === 25,
      "a control message is twenty-five bytes: the header plus the producer identifier")
    assert(acknowledgement.encodedLength() === FixedMessageEncodedLength,
      "an acknowledgement is the header plus the producer identifier and nothing more")
    assert(framedLength(acknowledgement.encodedLength()) === acknowledgement.encodedLength() + 1,
      "framing adds exactly the one-byte type discriminator")
    assert(acknowledgement.consumerPosition() === 0L,
      "the message must carry the position it was built with")
    assert(acknowledgement.sequenceNumber() === 1L,
      "the header carries the next position expected, which is one beyond the one consumed")

    assert(protocol.onAck(key, acknowledgement) === blockBytes,
      "an acknowledgement that arrived on the wire must release the same bytes")
    assert(protocol.outstandingBytes(key) === 0L, "the window is drained by the wire form too")

    // The stream key is supplied by the handler that owns the channel, because the wire carries
    // only a shuffle, a map and a partition and those three do not identify a stream.
    val misaddressed = ack(secondShuffleId, mapId, partitionId, 0L)
    assert(protocol.tryAcknowledge(key, misaddressed).isEmpty,
      "a frame naming another shuffle must never be applied to this ledger")
    assert(protocol.misaddressedFrameCount === 1L,
      "a misrouted frame is counted rather than logged")
    val absentFrame: AckMessage = null
    assert(protocol.tryAcknowledge(key, absentFrame).isEmpty, "a null frame releases nothing")

    val impossible = protocol.highestChargedSequenceNumber(key) + 1L
    assert(protocol.tryAcknowledge(key, impossible).isEmpty,
      "an acknowledgement naming the future must be refused outright")
    assert(protocol.impossibleAckPositionCount === 1L,
      "an impossible position is counted so a misbehaving peer is distinguishable from a slow one")
    assert(protocol.onAck(key, impossible) === 0L,
      "the total form of the call absorbs a refusal as zero released bytes")
    assert(protocol.acknowledgedPosition(key) === 0L, "a refused acknowledgement advances nothing")

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

    assert(limiter.tryAcquire(capacityBytes), "a full bucket admits a request for all of it")
    assert(limiter.availableTokens === 0L, "an admitted request is debited in full")

    assert(!limiter.tryAcquire(1L),
      "an empty bucket refuses, which is flow control and not failure")
    assert(limiter.availableTokens === 0L, "a refusal must leave the ledger untouched")
    assert(limiter.millisUntilAvailable(refillRate) === millisPerSecond,
      "a refused caller is told exactly when the bucket's own arithmetic will admit it")

    base.advance(millisPerSecond)
    assert(limiter.availableTokens === refillRate,
      "one second of accrual mints exactly one second of the token rate")
    base.advance(millisPerSecond)
    assert(limiter.availableTokens === 2L * refillRate,
      "accrual is measured from the recorded refill point, so observing it does not consume it")
    base.advance(10L * millisPerSecond)
    assert(limiter.availableTokens === capacityBytes, "accrual is clamped at the bucket size")

    assert(limiter.tryAcquire(0L), "a zero-sized request is admitted without charge")
    intercept[IllegalArgumentException] {
      limiter.tryAcquire(-1L)
    }
    intercept[IllegalArgumentException] {
      limiter.tryAcquire(Long.MinValue)
    }
    assert(limiter.availableTokens === capacityBytes,
      "neither the free zero nor the refused negatives may debit anything")

    val unlimited = TokenBucketRateLimiter.unlimited(clock)
    assert(unlimited.isUnlimited, "the fixture must actually be the unlimited form")
    assert(unlimited.tryAcquire(0L), "an unlimited limiter still admits an empty payload for free")
    intercept[IllegalArgumentException] {
      unlimited.tryAcquire(-1L)
    }

    assert(!limiter.tryAcquire(capacityBytes + 1L), "an oversized request is refused")
    assert(limiter.oversizedRequestCount === 1L,
      "an oversized request is counted, because it means the caller is framing illegal blocks")
    assert(limiter.availableTokens === capacityBytes, "an oversized refusal charges nothing")
    assert(limiter.millisUntilAvailable(capacityBytes + 1L) === Long.MaxValue,
      "no amount of waiting satisfies a request the bucket can never hold")

    assert(MaxEncodedFrameBytes.toLong >= MinTokenBucketCapacityBytes,
      "the encoded frame cap is the payload cap plus framing, so it is never the smaller one")
    val flooredLimiter =
      new TokenBucketRateLimiter(TokenBucketRateLimiter.burstCapacityBytes(1L), 1L, clock)
    assert(flooredLimiter.capacityBytes === MaxEncodedFrameBytes.toLong,
      "a bucket at the slowest conceivable rate is still sized to hold one maximum frame")
    assert(flooredLimiter.tryAcquire(MinTokenBucketCapacityBytes),
      "a maximum-sized block must never be permanently refused for size")

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

    val refusals = 64
    (0 until refusals).foreach { attempt =>
      assert(!limiter.tryAcquire(remainingTokens + 1L),
        s"attempt $attempt asked for one byte more than the bucket holds and must be refused")
      assert(limiter.availableTokens === remainingTokens,
        s"refusal $attempt must publish nothing, so the token count is unchanged")
    }
    assert(limiter.oversizedRequestCount === 0L,
      "a request within the bucket size is over limit, not oversized, and is not counted as one")

    clock.advance(millisPerSecond)
    assert(limiter.tryAcquire(remainingTokens + 1L),
      "the request a refusal made the caller hold is admitted verbatim once tokens have accrued")
  }

  test("rate limiting via token bucket treats an absent bandwidth cap as unlimited") {
    val base = newManualClock()
    val clock = new ReadCountingClock(base)
    val limiter = TokenBucketRateLimiter.unlimited(clock)

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

    base.setTime(ManualClockEpochMillis + 3600L * millisPerSecond)
    assert(limiter.tryAcquire(Long.MaxValue), "an hour later, still admitted")
    assert(limiter.availableTokens === Long.MaxValue, "and still reporting no ceiling")
    assert(clock.readCount === readsAfterConstruction, "and still without consulting the clock")

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

    assert(TokenBucketRateLimiter.perShuffleBytesPerSecond(declaredMBps, 1) ===
      administeredBytesPerSecond, "one shuffle takes the whole declared capacity as its share")
    assert(TokenBucketRateLimiter.perShuffleBytesPerSecond(declaredMBps, 4) ===
      administeredBytesPerSecond / 4L, "four shuffles take a quarter of it each")

    assert(BandwidthCeilingPercent === 80L, "the bandwidth ceiling is eighty percent of the cap")
    assert(applyBandwidthCeiling(administeredBytesPerSecond) ===
      administeredBytesPerSecond / PercentScale * BandwidthCeilingPercent,
      "the ceiling divides before it multiplies, which is what keeps the arithmetic overflow free")
    assert(applyBandwidthCeiling(0L) === 1L,
      "a rate is floored at one byte per second so a very small cap throttles rather than halts")

    (1 to 4).foreach { shuffles =>
      val share = refillBytesPerSecond(declaredMBps, shuffles)
      assert(share === applyBandwidthCeiling(
        TokenBucketRateLimiter.perShuffleBytesPerSecond(declaredMBps, shuffles)),
        s"the share for $shuffles shuffles is the ceiling applied to the split")
      assert(share * shuffles.toLong <= applyBandwidthCeiling(administeredBytesPerSecond),
        s"the aggregate of $shuffles limiters must stay inside the ceiling, not exceed it each")
    }

    assert(TokenBucketRateLimiter.perShuffleBytesPerSecond(declaredMBps, 0) ===
      TokenBucketRateLimiter.perShuffleBytesPerSecond(declaredMBps, 1),
      "zero concurrent shuffles is treated as one, so there is no division by zero")
    assert(TokenBucketRateLimiter.perShuffleBytesPerSecond(declaredMBps, -8) ===
      TokenBucketRateLimiter.perShuffleBytesPerSecond(declaredMBps, 1),
      "a negative count is treated as one for the same reason")

    assert(TokenBucketRateLimiter.burstCapacityBytes(1L) === MaxEncodedFrameBytes.toLong,
      "a tiny rate still yields a bucket that can hold one whole frame")
    assert(TokenBucketRateLimiter.burstCapacityBytes(administeredBytesPerSecond) ===
      administeredBytesPerSecond, "a large rate yields one second of itself")

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

  test("rate limiting via token bucket holds the executor's aggregate burst to the ceiling") {
    val declaredMBps = 1
    val clock = newManualClock()
    val budget = TokenBucketRateLimiter.executorBudget(
      streamingConfWithOverrides(maxBandwidthMBps = Some(declaredMBps)), clock)
    val shuffleCount = 5
    val limiters = (0 until shuffleCount).map(shuffleId => budget.limiterFor(shuffleId))
    assert(budget.divisor === shuffleCount, s"$shuffleCount limiters divide the cap that many ways")

    val ceilingRate = budget.aggregateBytesPerSecond.get
    assert(ceilingRate === applyBandwidthCeiling(declaredMBps.toLong * BytesPerMebibyte),
      "the aggregate paces at the ceiling applied to the whole declared capacity, undivided")
    val aggregateCapacity = TokenBucketRateLimiter.burstCapacityBytes(ceilingRate)
    val perShuffleCapacity = limiters.head.capacityBytes
    assert(perShuffleCapacity * shuffleCount.toLong > aggregateCapacity,
      "the fixture must actually present the overrun this ceiling exists to prevent: the sum of " +
        s"$shuffleCount bucket capacities ($perShuffleCapacity each) must exceed the aggregate " +
        s"capacity of $aggregateCapacity, or this case proves nothing")

    val block = MaxEncodedFrameBytes.toLong
    var admittedBytes = 0L
    limiters.foreach { limiter =>
      var admitting = true
      while (admitting) {
        if (limiter.tryAcquire(block)) {
          admittedBytes += block
        } else {
          admitting = false
        }
      }
    }
    assert(admittedBytes <= aggregateCapacity,
      s"the executor admitted $admittedBytes byte(s) at one instant, above the aggregate burst " +
        s"allowance of $aggregateCapacity: the per-shuffle buckets are pacing themselves but not " +
        "the executor")
    assert(admittedBytes < perShuffleCapacity * shuffleCount.toLong,
      "and strictly less than the sum of the individual allowances, which is the whole point")
    assert(budget.aggregateRefusalCount > 0L,
      "a refusal must be recorded against the executor-wide ceiling, because a cap reached with " +
        "nothing ever throttled is a cap enforced by standing streaming down rather than by pacing")
    assert(limiters.exists(_.refusalCount > 0L),
      "and the refusal must be visible at the limiter the caller actually holds")

    val exhausted = limiters.head
    val beforeRefusal = exhausted.capacityBytes - exhausted.availableTokens
    assert(!exhausted.tryAcquire(block), "the fixture must be at the ceiling for this assertion")
    assert(exhausted.capacityBytes - exhausted.availableTokens === beforeRefusal,
      "a request the ceiling refused must leave the shuffle's own tokens exactly as they were")

    assert(exhausted.millisUntilAvailable(block) > 0L,
      "a caller refused by the ceiling must be given the ceiling's own refill wait")

    clock.advance(millisPerSecond)
    assert(exhausted.tryAcquire(block) || exhausted.availableTokens > 0L,
      "one second of accrual at the ceiling rate must put the executor back in service")
    assert(budget.aggregateLimiter.refillBytesPerSecond === ceilingRate,
      "and the rate it comes back at is the administered ceiling, which the divisor never touches")

    val uncapped = TokenBucketRateLimiter.executorBudget(streamingConfWithOverrides(), clock)
    assert(uncapped.aggregateLimiter.isUnlimited,
      "an absent cap must leave the aggregate unlimited rather than pacing at a derived rate")
    assert(uncapped.aggregateBytesPerSecond.isEmpty,
      "and report no aggregate rate at all, because none was declared")
    assert(uncapped.limiterFor(firstShuffleId).tryAcquire(Long.MaxValue),
      "so an uncapped limiter still admits everything, ceiling or no ceiling")
  }

  // Timeout detection and failure signalling.

  test("timeout detection and failure signalling") {
    assert(ProducerConnectionTimeoutMillis === 5000L,
      "the producer connection timeout is five seconds")
    assert(ConsumerLivenessTimeoutMillis === 10000L, "the consumer liveness window is ten seconds")
    assert(HeartbeatIntervalMillis < ProducerConnectionTimeoutMillis,
      s"the heartbeat cadence ($HeartbeatIntervalMillis ms) must be strictly below the producer " +
        s"detector's bound ($ProducerConnectionTimeoutMillis ms), or a healthy idle producer is " +
        "one scheduling delay away from being declared dead")
    assert(HeartbeatIntervalMillis * BackpressureProtocol.HEARTBEAT_SAFETY_DIVISOR <=
        ProducerConnectionTimeoutMillis,
      s"${BackpressureProtocol.HEARTBEAT_SAFETY_DIVISOR} heartbeat intervals must fit inside the " +
        s"bound, so ${BackpressureProtocol.HEARTBEAT_SAFETY_DIVISOR - 1L} consecutive heartbeats " +
        s"may be lost before a producer is judged gone, but it is $HeartbeatIntervalMillis")
    assert(BackpressureProtocol.HEARTBEAT_SAFETY_DIVISOR >= 2L,
      "a divisor of one would be a cadence equal to the bound by another name")
    assert(StreamingShuffleServerHandler.PRODUCER_HEARTBEAT_INTERVAL_MS === HeartbeatIntervalMillis,
      "the producer's own cadence must be the protocol's, derived rather than restated, so the " +
        "two ends of the same timer can never drift apart")

    val clock = newManualClock()
    val protocol = newProtocol(clock)
    val key = consumerKey()
    protocol.registerShuffle(firstShuffleId, partitionCount)
    assert(protocol.registerStream(key, creditLimitBytes), "the ledger must open")
    assert(!protocol.isProducerTimedOut(key),
      "a stream that has just opened has heard from its peer")
    assert(protocol.millisSinceInbound(key) === Some(0L), "no time has passed since it opened")

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

    protocol.onHeartbeat(key)
    assert(!protocol.isProducerTimedOut(key), "a heartbeat alone keeps a producer judged alive")
    assert(protocol.millisSinceInbound(key) === Some(0L), "and re-bases the interval")
    protocol.onDataReceived(key, 0L, blockBytes)
    assert(protocol.millisSinceInbound(key) === Some(0L),
      "arrival refreshes liveness before any acknowledgement, so a consumer slow to acknowledge " +
        "cannot declare a healthy producer dead")
    clock.advance(ProducerConnectionTimeoutMillis)
    assert(protocol.isProducerTimedOut(key), "and the timer arms again from that arrival")

    protocol.onStreamTermination(key, 1L)
    assert(protocol.isStreamTerminated(key), "the producer announced the orderly end of the stream")
    assert(protocol.announcedBlockCount(key) === Some(1L), "and how many blocks it sent")
    clock.advance(10L * ProducerConnectionTimeoutMillis)
    assert(!protocol.isProducerTimedOut(key),
      "silence after an orderly end of stream must never be read as a failed producer")
    assert(protocol.timedOutProducerStreams.isEmpty, "nor reported by the whole-set scan")
    assert(!protocol.shouldSendHeartbeat(key),
      "and a finished stream has nothing left for a heartbeat to keep alive")

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

    assert(protocol.onAck(key, 0L) === blockBytes, "the outstanding block is acknowledged")
    assert(!protocol.isConsumerTimedOut(key), "a consumer that has caught up is not timed out")
    assert(protocol.outstandingBytes(key) === 0L, "and holds nothing")

    clock.advance(2L * ConsumerLivenessTimeoutMillis)
    assert(!protocol.isConsumerTimedOut(key),
      "a consumer with nothing outstanding is idle rather than unresponsive")
    assert(protocol.timedOutConsumerStreams.isEmpty, "so the whole-set scan reports nothing")

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

    val openedAtMillis = clock.getTimeMillis()
    assert(!protocol.shouldSendHeartbeat(key), "a stream that has just opened is not due")
    clock.advance(HeartbeatIntervalMillis - 1L)
    assert(!protocol.shouldSendHeartbeat(key),
      s"a heartbeat is NOT due one millisecond short of the $HeartbeatIntervalMillis ms cadence")
    clock.advance(1L)
    assert(protocol.shouldSendHeartbeat(key),
      s"and is due at exactly the $HeartbeatIntervalMillis ms cadence, which is a whole safety " +
        s"divisor inside the $ProducerConnectionTimeoutMillis ms bound it refreshes")
    assert(clock.getTimeMillis() - openedAtMillis < ProducerConnectionTimeoutMillis,
      "the first heartbeat must fall strictly inside the producer detector's window")

    // A heartbeat carries no time value: the receiver measures its local arrival instant, so clock
    // skew cannot masquerade as liveness.
    val nextPosition = 7L
    val emitted = protocol.heartbeatFor(key, nextPosition)
    assert(emitted.isDefined, "a registered stream must be able to build its heartbeat")
    val message = emitted.get
    assert(message.sequenceNumber() === nextPosition,
      "the position is supplied by the handler that owns the channel and taken on trust")
    assert(message.shuffleId() === firstShuffleId, "and names the stream it belongs to")
    assert(message.partitionId() === partitionId, "including its reduce partition")
    assert(message.mapId() === mapId, "and the producer whose stream it paces")
    assert(message.consumerToken() === key.consumerToken,
      "and declares exactly the identity the ledger key carries")
    assert(message.consumerToken() !== NoConsumerToken,
      "which for a consumer's own ledger is a real identity rather than the reserved value")

    assert(HeartbeatBaseEncodedLength === FixedMessageEncodedLength + 8,
      "a heartbeat is a control message plus the eight bytes of the consumer session token")
    assert(message.encodedLength() === 33,
      "which the specification fixes at thirty-three bytes")
    assert(framedLength(message.encodedLength()) === message.encodedLength() + 1,
      "framing adds exactly the one-byte type discriminator")

    assert(!protocol.shouldSendHeartbeat(key), "emitting restarts the interval")
    clock.advance(HeartbeatIntervalMillis)
    assert(protocol.shouldSendHeartbeat(key), "which elapses again on the same cadence")

    // An inbound heartbeat refreshes liveness from the LOCAL instant of arrival, which is the only
    // instant available now that the wire carries none.
    clock.advance(ProducerConnectionTimeoutMillis)
    assert(protocol.isProducerTimedOut(key), "the producer has fallen silent")
    val arrivalInstant = clock.getTimeMillis()
    protocol.onHeartbeat(key, heartbeat(firstShuffleId, mapId, partitionId, 8L))
    assert(!protocol.isProducerTimedOut(key), "and is alive again the moment its heartbeat arrives")
    assert(protocol.millisSinceInbound(key) === Some(0L),
      "liveness is judged from the local instant of arrival")
    assert(protocol.remoteHeartbeatTimestamp(key) === Some(arrivalInstant),
      "and the instant recorded for diagnostics is that same local reading")

    assert(protocol.onControlMessage(
      key, heartbeat(firstShuffleId, mapId, partitionId, 9L)) ===
        Some(StreamingShuffleMessageType.HEARTBEAT), "a heartbeat dispatches as a heartbeat")
    protocol.onDataReceived(key, 0L, blockBytes)
    assert(protocol.onControlMessage(
      key, ack(firstShuffleId, mapId, partitionId, 0L)) ===
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

    assert(protocol.tryAdmit(key, blockBytes, 0L), "one block is sent and retained")
    assert(protocol.isWithinUnacknowledgedWindow(key, 0L), "so it can still be replayed")
    assert(!protocol.isWithinUnacknowledgedWindow(key, 1L), "a block never sent cannot be")
    val retained = retransmitRequest(firstShuffleId, mapId, partitionId, 0L)
    assert(retained.blockCount() === 1L,
      "a request built from one position names exactly that one block")
    assert(retained.lastSequenceNumber() === retained.firstSequenceNumber(),
      "and expresses it as the inclusive single-element window it is")
    assert(protocol.canServeRetransmit(key, retained),
      "a request naming a retained block is serviceable")
    val unretained = retransmitRequest(firstShuffleId, mapId, partitionId, 1L)
    assert(!protocol.canServeRetransmit(key, unretained),
      "a request naming a block that was never retained is refused, because splicing a partial " +
        "replay into a consumer's input would reorder its partition")

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

    assert(protocol.onAck(key, 0L) === blockBytes, "the consumer acknowledges the block")
    assert(protocol.retransmitAttempts(key) === 0, "which resets the attempt count")
    assert(!protocol.isRetryExhausted(key), "and restores the budget")
    assert(protocol.nextRetryBackoffMillis(key) === Some(RetryBaseBackoffMillis),
      "so the ladder starts from its first rung again")

    assert(protocol.tryAdmit(key, blockBytes, 1L), "a further block is sent")
    val sentBeforeReplay = protocol.sentBytes(key)
    assert(protocol.tryAdmitReplay(key, blockBytes, 1L), "and is then replayed to repair a loss")
    assert(protocol.sentBytes(key) === sentBeforeReplay,
      "a replay must not inflate the producer's measured output")
    assert(protocol.replayedBytes(key) === blockBytes, "it lands on the replay total instead")
    assert(protocol.outstandingBytes(key) === blockBytes,
      "and re-charging a retained block replaces its entry rather than counting it twice")

    val startMillis = clock.getTimeMillis()
    val instants = advanceThroughRetryBackoff(clock)
    assert(instants.length === MaxRetryAttempts, "one instant per permitted attempt")
    assert(instants === RetryBackoffLadderMillis.scanLeft(startMillis)(_ + _).tail,
      "and each is the previous one plus that attempt's pause")
    assert(clock.getTimeMillis() - startMillis === RetryBackoffLadderMillis.sum,
      "so the whole ladder spans thirty-one seconds of injected time and no real time at all")
  }

  test("a multi block repair window is charged as one attempt, not one per position") {
    val clock = newManualClock()
    val protocol = newProtocol(clock)
    val key = producerKey()
    protocol.registerShuffle(firstShuffleId, partitionCount)
    assert(protocol.registerStream(key, creditLimitBytes * 8L), "the ledger must open")

    val runLength = 5
    (0 until runLength).foreach { position =>
      assert(protocol.tryAdmit(key, blockBytes, position.toLong),
        s"block $position must be sent and retained so that it can be replayed")
    }
    assert(protocol.unacknowledgedWindow(key).contains((0L, runLength - 1L)),
      s"the whole run must be inside the unacknowledged window, but it was " +
        s"${protocol.unacknowledgedWindow(key)}")

    val window = retransmitRequest(firstShuffleId, mapId, partitionId, 0L, runLength - 1L)
    assert(window.blockCount() === runLength.toLong,
      s"one request must name the whole run of $runLength block(s) but named " +
        s"${window.blockCount()}")
    assert(protocol.canServeRetransmit(key, window),
      "a run wholly inside the unacknowledged window must be serviceable in full")
    assert(protocol.onRetransmitRequest(key, window),
      "and the gate must admit it")
    assert(protocol.retransmitAttempts(key) === 1,
      s"the whole run must cost exactly one attempt, but the gate charged " +
        s"${protocol.retransmitAttempts(key)}")
    assert(!protocol.isRetryExhausted(key),
      "so a single multi block repair can never exhaust the budget on its own")
    assert(protocol.nextRetryBackoffMillis(key) === Some(RetryBackoffLadderMillis(1)),
      "and the pause owed next is the second rung of the ladder, not its last")

    (2 to MaxRetryAttempts).foreach { attempt =>
      assert(protocol.onRetransmitRequest(key, window),
        s"repair attempt $attempt of the same run must still be admitted")
      assert(protocol.retransmitAttempts(key) === attempt,
        s"and must be counted as attempt $attempt")
    }
    assert(protocol.isRetryExhausted(key),
      "five attempts at the range spend the budget, whatever the width of the range")

    val overreaching = retransmitRequest(firstShuffleId, mapId, partitionId, 0L, runLength.toLong)
    assert(!protocol.canServeRetransmit(key, overreaching),
      "a run naming one position that was never retained must be refused entirely")
    assert(!protocol.onRetransmitRequest(key, overreaching),
      "and the gate must refuse it rather than serve the retained prefix")
  }

  // Priority arbitration under concurrent load.

  test("priority arbitration under concurrent load") {
    val clock = newManualClock()
    val protocol = newProtocol(clock)

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

    assert(protocol.guaranteedShuffleId === Some(firstShuffleId),
      "the least demanding shuffle is the one arbitration never asks to yield")
    shuffleOrder.foreach { shuffleId =>
      assert(!protocol.shouldYield(shuffleId),
        s"nobody yields while the executor is under its threshold, including shuffle $shuffleId")
    }

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

    protocol.reportBufferUtilization(firstShuffleId, 100L, 100L)
    assert(protocol.aggregateBufferUtilizationPercent === 0L,
      "an unregistered shuffle cannot contribute to the executor's reading")

    protocol.registerShuffle(firstShuffleId, 2)
    protocol.registerShuffle(secondShuffleId, 8)

    protocol.reportBufferUtilization(firstShuffleId, 150L, 100L)
    protocol.reportBufferUtilization(secondShuffleId, 10L, 100L)
    assert(protocol.aggregateBufferUtilizationPercent ===
      BackpressureProtocol.percentOf(160L, 200L),
      "the reading is the ratio of the summed buffers to the summed budgets")
    assert(protocol.aggregateBufferUtilizationPercent === 80L, "which here is eighty percent")
    assert(protocol.isSpillRequired,
      "and the threshold is reached at exactly the configured percentage, not beyond it")

    protocol.reportBufferUtilization(secondShuffleId, -50L, 100L)
    assert(protocol.aggregateBufferUtilizationPercent === 75L,
      "a negative contribution is read as zero")
    assert(!protocol.isSpillRequired, "which puts the executor back under its threshold")

    protocol.reportBufferUtilization(firstShuffleId, 300L, 100L)
    assert(protocol.aggregateBufferUtilizationPercent === 150L,
      "utilisation is deliberately not clamped at a hundred percent")
    assert(protocol.isSpillRequired, "and an over-budget executor certainly needs to evict")

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

    val coordinator = new StreamingShuffleCoordinator(mock(classOf[RpcEnv]), conf, clock)
    try {
      val budget = TokenBucketRateLimiter.executorBudget(
        conf, clock, () => Some(coordinator.numConcurrentShuffles))
      val quota = new MemorySpillManager.ExecutorBufferQuota(
        conf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT),
        conf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD),
        () => conf.get(EXECUTOR_MEMORY) * BytesPerMebibyte)
      val protocol = new BackpressureProtocol(conf, coordinator, budget, clock, quota)

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

      assert(budget.divisor === 2, "two live limiters divide the cap two ways")
      assert(budget.currentShareBytesPerSecond === Some(refillBytesPerSecond(declaredMBps, 2)),
        "which is exactly the contract's (0.8 * cap) / numConcurrentShuffles")

      assert(budget.observeConcurrency(5),
        "a higher count reported by the coordinator moves the divisor")
      assert(budget.divisor === 5, "and is adopted immediately")
      assert(budget.currentShareBytesPerSecond === Some(refillBytesPerSecond(declaredMBps, 5)),
        "so every live limiter paces at a fifth of the ceiling")
      assert(!budget.observeConcurrency(5), "an unchanged count republishes nothing")
      assert(!budget.observeConcurrency(0), "and a non-positive count is ignored outright")
      assert(budget.divisor === 5, "leaving the divisor conservatively high rather than wrong")

      assert(protocol.declaredLinkCapacity ===
        Some(declaredMBps.toLong * BytesPerMebibyte),
        "the administered capacity is the operator's declared figure, unmodified")
      assert(budget.currentShareBytesPerSecond.exists(_ < declaredMBps.toLong * BytesPerMebibyte),
        "while a limiter's share is strictly less than it, because the ceiling holds it down")
    } finally {
      coordinator.reset()
    }

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

    assert(protocol.onAck(key, 0L) === creditLimitBytes, "the acknowledgement restores credit")
    assert(!protocol.isStreamThrottled(key), "which ends the episode")
    assert(protocol.tryAdmit(key, creditLimitBytes, 1L), "the held block finally goes out")
    (0 until refusals).foreach { attempt =>
      assert(!protocol.tryAdmit(key, creditLimitBytes, 2L),
        s"retry $attempt of the next block must be refused")
    }
    assert(protocol.backpressureEventCount === 2L, "the second episode is the second event")
    assert(observedBackpressureEvents() - baseline === 2L, "as the operator sees it too")

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

    assert(protocol.receiveQuotaBytes > 0L, "the executor has a consumer receive budget")
    assert(protocol.reservedReceiveQuotaBytes === 0L, "and none of it is held yet")
    val availableForReceive = protocol.aggregateAvailableBytes
    assert(availableForReceive > 0L && availableForReceive < protocol.receiveQuotaBytes,
      "the open stream's metadata must already occupy part of the one aggregate budget")
    assert(protocol.tryReserveReceiveQuota(availableForReceive),
      "a consumer reservation for all remaining aggregate room fits exactly")
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
    protocol.releaseReceiveQuota(availableForReceive)
    assert(protocol.reservedReceiveQuotaBytes === 0L, "releasing returns the whole budget")
    assert(!protocol.receiveQuotaExhausted, "so it is no longer committed")
    assert(!protocol.tryReserveReceiveQuota(protocol.receiveQuotaBytes + 1L),
      "a reservation one byte beyond the budget is refused")
    assert(protocol.backpressureEventCount === 5L,
      "and because the release closed the previous episode, that refusal is a fresh edge")
    assert(observedBackpressureEvents() - baseline === 5L,
      "five episodes, five events, and not one event per refusal")

    assert(protocol.tryReserveReceiveQuota(0L), "a zero-sized reservation is free")
    assert(protocol.reservedReceiveQuotaBytes === 0L, "and charges nothing")
    assert(protocol.backpressureEventCount === 5L, "and is not an event")

    // A HOSTILE length is the case the budget exists for, because the figure it is given comes off
    // the wire.
    assert(protocol.tryReserveReceiveQuota(1L), "one byte is held, so the ledger is non-zero")
    val reservedBeforeHostileRequest = protocol.reservedReceiveQuotaBytes
    val refusalsBeforeHostileRequest = protocol.receiveQuotaRefusalCount
    assert(reservedBeforeHostileRequest === 1L, "and it is exactly the one byte")
    assert(!protocol.tryReserveReceiveQuota(Long.MaxValue),
      "a reservation of the largest representable length must be refused, not wrapped")
    assert(protocol.reservedReceiveQuotaBytes === reservedBeforeHostileRequest,
      "and a refusal must leave the ledger exactly as it was")
    assert(protocol.receiveQuotaRefusalCount === refusalsBeforeHostileRequest + 1L,
      "while still being counted as the refusal it is")
    assert(!protocol.tryReserveReceiveQuota(protocol.receiveQuotaBytes),
      "the byte already held is enough to refuse a request for the whole budget, which is the " +
        "arithmetic the wrap would have hidden")
    intercept[IllegalArgumentException] {
      protocol.tryReserveReceiveQuota(-1L)
    }
    intercept[IllegalArgumentException] {
      protocol.tryReserveReceiveQuota(Long.MinValue)
    }
    intercept[IllegalArgumentException] {
      protocol.releaseReceiveQuota(-1L)
    }
    assert(protocol.reservedReceiveQuotaBytes === reservedBeforeHostileRequest,
      "no refused request, in either direction, may move the ledger")
    protocol.releaseReceiveQuota(1L)
    assert(protocol.reservedReceiveQuotaBytes === 0L, "and the held byte returns on release")
  }

  test("the backpressure state machine covers every specified transition") {
    val clock = newManualClock()
    val protocol = newPacedProtocol(clock)
    val key = producerKey()
    protocol.registerShuffle(firstShuffleId, partitionCount)
    assert(protocol.registerStream(key, creditLimitBytes), "the ledger must open")

    assert(protocol.state === BackpressureState.Flowing, "a protocol with nothing wrong is flowing")
    assert(protocol.streamState(key) === BackpressureState.Flowing, "and so is each of its streams")
    assert(protocol.throttledStreamCount === 0, "with nothing held back")
    assert(!protocol.isDegraded, "and nothing latched against it")

    assert(protocol.tryAdmit(key, creditLimitBytes, 0L), "the whole allowance goes out")
    assert(!protocol.tryAdmit(key, creditLimitBytes, 1L), "so the next block is refused on credit")
    assert(protocol.state === BackpressureState.Throttled, "which throttles the executor's reading")
    assert(protocol.streamState(key) === BackpressureState.Throttled, "and the stream's")
    assert(protocol.throttledStreamCount === 1, "with exactly one stream held back")

    assert(protocol.onAck(key, 0L) === creditLimitBytes, "the acknowledgement releases the window")
    assert(protocol.confirmReclamation(key) === Some(0L),
      "and the owner of the freed buffers confirms the release at once")
    assert(protocol.state === BackpressureState.Flowing, "which returns the executor to flowing")
    assert(protocol.throttledStreamCount === 0, "with nothing held back again")

    protocol.registerShuffle(secondShuffleId, partitionCount)
    val pacedKey = producerKey(secondShuffleId, partitionId, "consumer-paced")
    val generousCredit = 8L * MaxEncodedFrameBytes.toLong
    assert(protocol.registerStream(pacedKey, generousCredit), "a stream with ample credit opens")
    accrueExecutorEgressCeiling(clock)
    assert(protocol.tryAdmit(pacedKey, MaxEncodedFrameBytes.toLong, 0L),
      "a bucket starts full, so the first maximum-sized frame is admitted")
    assert(protocol.availableCreditBytes(pacedKey) > 0L,
      "credit still remains, so whatever refuses the next frame can only be the pacing layer")
    assert(!protocol.tryAdmit(pacedKey, MaxEncodedFrameBytes.toLong, 1L),
      "and the drained bucket does refuse it")
    assert(protocol.isStreamThrottled(pacedKey), "which throttles the stream")
    assert(protocol.state === BackpressureState.Throttled, "and the executor")

    // Throttled -> Flowing, because the bucket refilled.
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

    protocol.reportBufferUtilization(firstShuffleId, 80L, 100L)
    assert(protocol.aggregateBufferUtilizationPercent === DefaultSpillThresholdPercent.toLong,
      "the executor is at exactly its configured threshold")
    assert(protocol.isSpillRequired, "so buffered partitions must be evicted")
    assert(protocol.state === BackpressureState.Spilling, "which is what the executor now reports")
    assert(protocol.streamState(key) === BackpressureState.Spilling,
      "and utilisation is executor-wide, so it overlays every stream")

    accrueExecutorEgressCeiling(clock)
    assert(protocol.tryAdmit(key, creditLimitBytes, 1L), "the credit-bound stream sends again")
    assert(!protocol.tryAdmit(key, creditLimitBytes, 2L), "and is refused again")
    assert(protocol.isStreamThrottled(key), "so it is genuinely throttled")
    assert(protocol.state === BackpressureState.Spilling, "yet spilling dominates the reading")
    assert(protocol.streamState(key) === BackpressureState.Spilling, "for that stream too")

    assert(protocol.onAck(key, 1L) === creditLimitBytes, "the acknowledgement frees the buffers")
    clock.advance(ReclamationDeadlineMillis)
    assert(protocol.confirmReclamation(key) === Some(ReclamationDeadlineMillis),
      "and the owner confirms the release at the bound")
    assert(protocol.reclamationDeadlineBreaches === 0L, "which is not a breach")
    protocol.reportBufferUtilization(firstShuffleId, 10L, 100L)
    assert(!protocol.isSpillRequired, "the eviction brought the executor back under its threshold")
    assert(protocol.state === BackpressureState.Flowing, "so it is flowing once more")

    protocol.reportBufferUtilization(firstShuffleId, 95L, 100L)
    assert(protocol.state === BackpressureState.Spilling,
      "the executor is over its threshold again")
    assert(protocol.durableSpillAdmissionCount === 0L, "and nothing has been retained on disk yet")
    protocol.reportDurableSpillAdmission(key)
    assert(protocol.durableSpillAdmissionCount === 1L,
      "a block retained on disk because the allowance was met must be counted")
    assert(!protocol.isDegraded,
      "spilling past the allowance is the specified answer to a full buffer, so it must NOT " +
        s"latch a reason, but it latched ${protocol.degradationReasons.mkString(", ")}")
    assert(protocol.degradationReasons.isEmpty, "no reason at all, not merely a different one")
    assert(protocol.state === BackpressureState.Spilling,
      "and the state must still be the spilling one it was in, because the producer is streaming")
    protocol.reportDurableSpillAdmission(key)
    assert(protocol.durableSpillAdmissionCount === 2L,
      "every retained block is counted, however few of them are reported individually")
    assert(!protocol.isDegraded, "and no number of them ever degrades the subsystem")

    protocol.reportBufferAllocationFailure(key)
    assert(protocol.state === BackpressureState.Degraded, "and an allocation failure degrades it")
    assert(protocol.degradationReasons ===
      Seq(BackpressureDegradationReason.BufferAllocationFailure),
      "naming the second of the four conditions under which streaming steps aside")

    protocol.clearDegradation()
    assert(!protocol.isDegraded, "clearing forgets every latched reason")
    assert(protocol.state === BackpressureState.Spilling, "revealing the condition underneath it")
    protocol.reportBufferUtilization(firstShuffleId, 10L, 100L)
    assert(protocol.state === BackpressureState.Flowing, "which is itself cleared by eviction")

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

    protocol.clearDegradation()
    assert(protocol.onAck(slowKey, 0L) === blockBytes, "the consumer catches up")
    assert(!protocol.isConsumerSlow(slowKey),
      "a consumer that has caught up entirely is not slow, however little it consumed in total")
    assert(!protocol.isConsumerSustainedSlow(slowKey), "so the latch is cleared with it")
    assert(protocol.state === BackpressureState.Flowing, "and the executor is flowing")

    val declaredCapacityBytes = linkCapacityMBps.toLong * BytesPerMebibyte
    assert(protocol.declaredLinkCapacity === Some(declaredCapacityBytes),
      "saturation is taken against the capacity the operator declared")
    assert(LinkSaturationTripPercent === 90L, "and the trip is at ninety percent of it")
    assert(protocol.linkSaturationPercent === 0L,
      "a link quiet for longer than two sampling intervals reads as idle, not as saturated")
    assert(!protocol.isLinkSaturated, "so it has not tripped")

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
    assert(protocol.isLinkSaturated,
      "so saturation trips on the FIRST interval that reads over capacity")
    assert(protocol.state === BackpressureState.Degraded, "and the executor degrades")
    assert(protocol.degradationReasons === Seq(BackpressureDegradationReason.LinkSaturation),
      "naming the third of the four conditions")
    assert(protocol.ingressBytes >= aboveNinetyPercent,
      "and the cumulative total counts every byte the link carried")
    protocol.clearDegradation()

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

    protocol.clearDegradation()
    assert(protocol.state === BackpressureState.Flowing, "clearing is the only way back")

    protocol.reset()
    assert(protocol.streamCount === 0, "every ledger is dropped")
    assert(protocol.numRegisteredShuffles === 0, "every registration with them")
    assert(protocol.aggregateBufferUtilizationPercent === 0L, "every utilisation contribution too")
    assert(protocol.backpressureEventCount === 0L, "the local event total is rewound")
    assert(protocol.reclamationDeadlineBreaches === 0L, "so is the breach total")
    assert(protocol.durableSpillAdmissionCount === 0L, "and so is the durable-admission total")
    assert(!protocol.isDegraded, "no reason stays latched")
    assert(protocol.state === BackpressureState.Flowing, "and the state is the resting one")
  }

  test("the protocol cannot reach a shuffle metrics reporter and reports through the source") {
    val reporterTypes =
      Seq(classOf[ShuffleReadMetricsReporter], classOf[ShuffleWriteMetricsReporter])

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

    val forbiddenReferences = Seq(
      "org/apache/spark/shuffle/ShuffleReadMetricsReporter",
      "org/apache/spark/shuffle/ShuffleWriteMetricsReporter",
      "org/apache/spark/TaskContext")
    val protocolClassFiles = compiledClassFilesOf(classOf[BackpressureProtocol])
    assert(protocolClassFiles.size >= 4,
      "the scan must have read the protocol, its companion and its nested types, yet it read " +
        s"only ${protocolClassFiles.size}: ${protocolClassFiles.map(_.getName).mkString(", ")}")
    Seq("BackpressureProtocol$StreamLedger.class", "BackpressureProtocol$RateWindow.class")
      .foreach { nested =>
        assert(protocolClassFiles.exists(_.getName == nested),
          s"the scan must include the nested $nested, yet it read only " +
            protocolClassFiles.map(_.getName).mkString(", "))
      }
    protocolClassFiles.foreach { classFile =>
      val bytecode = Files.readAllBytes(classFile.toPath)
      forbiddenReferences.foreach { reference =>
        assert(!containsUtf8(bytecode, reference),
          s"${classFile.getName} references $reference, so the protocol can reach a metrics " +
            "reporter from a network event-loop thread and the reporter's single-threaded " +
            "contract is broken")
      }
    }

    val taskContext = TaskContext.empty()
    TaskContext.setTaskContext(taskContext)
    try {
      val ambientWriteMetrics = taskContext.taskMetrics.shuffleWriteMetrics
      val ambientReadMetrics = taskContext.taskMetrics.shuffleReadMetrics
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
      protocol.reportDurableSpillAdmission(key)
      assert(!protocol.isDegraded, "a durable admission is reported without latching anything")
      protocol.reportBufferAllocationFailure(key)
      assert(protocol.isDegraded, "a reason is latched")
      protocol.clearDegradation()
      protocol.onStreamTermination(key, 2L)
      assert(protocol.unregisterShuffle(firstShuffleId) === 1, "and the shuffle is retired")

      assert(TaskContext.get() eq taskContext,
        "the fixture must actually be the ambient context throughout, or the observation below " +
          "would be of a context nothing could have reached")
      assert(ambientWriteMetrics.bytesWritten === 0L && ambientWriteMetrics.recordsWritten === 0L &&
          ambientWriteMetrics.writeTime === 0L,
        "the protocol must never report through the ambient task context's write metrics, since " +
          "the write path reports from the task thread while the protocol runs on network " +
          s"threads too, yet it recorded ${ambientWriteMetrics.bytesWritten} byte(s), " +
          s"${ambientWriteMetrics.recordsWritten} record(s) and " +
          s"${ambientWriteMetrics.writeTime} ns")
      assert(ambientReadMetrics.remoteBytesRead === 0L &&
          ambientReadMetrics.localBytesRead === 0L &&
          ambientReadMetrics.recordsRead === 0L &&
          ambientReadMetrics.remoteBlocksFetched === 0L &&
          ambientReadMetrics.localBlocksFetched === 0L &&
          ambientReadMetrics.fetchWaitTime === 0L,
        "and it must never report through the ambient read metrics either, yet it recorded " +
          s"${ambientReadMetrics.remoteBytesRead} remote byte(s), " +
          s"${ambientReadMetrics.recordsRead} record(s) and " +
          s"${ambientReadMetrics.remoteBlocksFetched} remote block(s)")
      assert(observedBackpressureEvents() - baseline === 1L,
        "while the lock-free Dropwizard counter, which is safe from any thread, did advance")
    } finally {
      TaskContext.unset()
    }
    val fieldMentions = classOf[BackpressureProtocol].getDeclaredFields
      .filter(field => reporterTypes.exists(_.isAssignableFrom(field.getType)))
    assert(fieldMentions.isEmpty,
      "no field of the protocol may hold a metrics reporter, yet these do: " +
        fieldMentions.map(field => s"${field.getName}: ${field.getType.getName}").mkString(", "))

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

    assert(observedBackpressureEvents() - baseline === 1L,
      "the throttle above must have advanced the Dropwizard backpressure counter exactly once, " +
        s"but it moved by ${observedBackpressureEvents() - baseline}")
  }

  /**
   * Every compiled class file of a class, its Scala companion and their nested types.
   *
   * @return the class files, which the caller must check is not vacuously empty
   */
  private def compiledClassFilesOf(cls: Class[_]): Seq[File] = {
    val simpleName = cls.getName.split('.').last
    val resource = cls.getResource(s"$simpleName.class")
    assert(resource != null, s"$simpleName must have been compiled to a readable class file")
    assert(resource.getProtocol == "file",
      s"$simpleName must be loaded from a directory of class files, not from ${resource}")
    val directory = new File(resource.toURI).getParentFile
    val prefix = s"$simpleName."
    val nestedPrefix = s"$simpleName$$"
    Option(directory.listFiles()).toSeq.flatten
      .filter(_.isFile)
      .filter { file =>
        file.getName.endsWith(".class") &&
          (file.getName == s"$simpleName.class" ||
            file.getName == s"$simpleName$$.class" ||
            file.getName.startsWith(nestedPrefix))
      }
      .filter(_.getName.startsWith(prefix.dropRight(1)))
      .sortBy(_.getName)
  }

  private def containsUtf8(bytecode: Array[Byte], name: String): Boolean = {
    val needle = name.getBytes(StandardCharsets.UTF_8)
    if (needle.length > bytecode.length) {
      false
    } else {
      (0 to bytecode.length - needle.length).exists { start =>
        var index = 0
        while (index < needle.length && bytecode(start + index) == needle(index)) {
          index += 1
        }
        index == needle.length
      }
    }
  }

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

    val rejected = intercept[SparkIllegalArgumentException] {
      protocolWith(
        streamingConfWithOverrides(spillThreshold = MaxSpillThresholdPercent + 1), clock, null)
    }
    assert(rejected.getCondition === "INVALID_CONF_VALUE.REQUIREMENT",
      "the rejection uses the existing configuration-validation condition")
    assert(rejected.getSqlState === "22022", "at its existing SQLSTATE")
    assert(rejected.getMessage.contains("The spill threshold must be in [50, 95]."),
      "and quotes the entry's own requirement verbatim")

    val quotaConf = streamingConfWithOverrides(bufferSizePercent = MaxBufferSizePercent)
    val quotaProtocol = protocolWith(quotaConf, clock, null)
    val executorMemoryBytes = quotaConf.get(EXECUTOR_MEMORY) * BytesPerMebibyte
    assert(quotaProtocol.receiveQuotaBytes ===
      StreamingShuffleTestHelper.aggregateBudgetBytes(executorMemoryBytes, MaxBufferSizePercent),
      "the budget is bufferSizePercent of the executor's memory")
    assert(quotaProtocol.receiveQuotaBytes > MaxEncodedFrameBytes.toLong,
      "which on a default executor is comfortably more than one maximum-sized frame")

    val tinyConf = streamingConfWithOverrides(bufferSizePercent = MinBufferSizePercent)
      .set(EXECUTOR_MEMORY, 1L)
    val tinyProtocol = protocolWith(tinyConf, clock, null)
    val tinyExpected =
      tinyConf.get(EXECUTOR_MEMORY) * BytesPerMebibyte / PercentScale * MinBufferSizePercent
    assert(tinyProtocol.receiveQuotaBytes === tinyExpected,
      "the aggregate budget must remain the configured percentage even on a tiny executor")
    assert(!tinyProtocol.tryReserveReceiveQuota(MaxEncodedFrameBytes.toLong),
      "a frame larger than the whole aggregate allowance must be refused before allocation")
  }

  test("stream-ledger registration is charged and fails closed when metadata has no room") {
    val capacity = 2L * BackpressureProtocol.STREAM_LEDGER_BASE_BYTES
    val quota = new MemorySpillManager.ExecutorBufferQuota(
      bufferSizePercent = 50,
      spillThresholdPercent = DefaultSpillThresholdPercent,
      executorMemoryProvider = () => 2L * capacity)
    val clock = newManualClock()
    val protocol = new BackpressureProtocol(
      streamingConfWithOverrides(),
      null,
      TokenBucketRateLimiter.executorBudget(streamingConfWithOverrides(), clock),
      clock,
      quota)
    val key = producerKey()
    val occupied = capacity - BackpressureProtocol.STREAM_LEDGER_BASE_BYTES + 1L
    assert(quota.tryReserve(occupied), "The fixture must leave one byte too little for a ledger")

    assert(!protocol.registerStream(key, creditLimitBytes),
      "A new stream must be refused when its base metadata cannot be charged")
    assert(!protocol.isStreamRegistered(key),
      "A refused registration must not leave an unaccounted fail-open ledger")
    assert(protocol.reservedMetadataQuotaBytes === 0L)

    quota.release(occupied)
    assert(protocol.registerStream(key, creditLimitBytes),
      "Returning aggregate room must make the stream registrable")
    assert(protocol.registerStream(key, creditLimitBytes),
      "A second owner must join the existing charged ledger without another base allocation")
    assert(protocol.reservedMetadataQuotaBytes === BackpressureProtocol.STREAM_LEDGER_BASE_BYTES)
    assert(!protocol.unregisterStream(key),
      "The first owner release must preserve the ledger for its second owner")
    assert(protocol.unregisterStream(key), "The final owner release must close the ledger")
    assert(protocol.reservedMetadataQuotaBytes === 0L)
    assert(quota.reservedBytes === 0L)
  }

  test("one stream ledger refuses entries beyond its bounded metadata window") {
    val entries = BackpressureProtocol.MAX_LEDGER_ENTRIES_PER_STREAM
    val metadataCapacity = BackpressureProtocol.STREAM_LEDGER_BASE_BYTES +
      (entries + 1L) * BackpressureProtocol.STREAM_LEDGER_ENTRY_BYTES
    val quota = new MemorySpillManager.ExecutorBufferQuota(
      bufferSizePercent = 50,
      spillThresholdPercent = DefaultSpillThresholdPercent,
      executorMemoryProvider = () => 2L * metadataCapacity)
    val clock = newManualClock()
    val conf = streamingConfWithOverrides()
    val protocol = new BackpressureProtocol(
      conf, null, TokenBucketRateLimiter.executorBudget(conf, clock), clock, quota)
    val key = producerKey()
    assert(protocol.registerStream(key, Long.MaxValue), "The bounded ledger must open")

    (0 until entries).foreach { sequenceNumber =>
      assert(protocol.tryAdmit(key, 1L, sequenceNumber.toLong),
        s"Entry $sequenceNumber must be admitted through the exact per-stream cap")
    }
    assert(protocol.unacknowledgedBlockCount(key) === entries)
    assert(protocol.reservedMetadataQuotaBytes ===
      BackpressureProtocol.STREAM_LEDGER_BASE_BYTES +
        entries.toLong * BackpressureProtocol.STREAM_LEDGER_ENTRY_BYTES)
    assert(!protocol.tryAdmit(key, 1L, entries.toLong),
      "The first entry beyond the cap must be held back without growing metadata")
    assert(protocol.unacknowledgedBlockCount(key) === entries)

    assert(protocol.onAck(key, entries - 1L) === entries.toLong,
      "Acknowledging the bounded window must release every entry")
    assert(protocol.reservedMetadataQuotaBytes === BackpressureProtocol.STREAM_LEDGER_BASE_BYTES,
      "Only the stream's base charge may remain after its window drains")
    assert(protocol.unregisterStream(key))
    assert(quota.reservedBytes === 0L)
  }

  test("dropping a shuffle returns every live ledger's metadata to the executor allowance") {
    val entriesPerStream = 4
    val quota = ledgerAccountingQuota()
    val clock = newManualClock()
    val conf = streamingConfWithOverrides()
    val protocol = new BackpressureProtocol(
      conf, null, TokenBucketRateLimiter.executorBudget(conf, clock), clock, quota)
    assert(quota.reservedBytes === 0L, "the allowance starts with nothing charged to it")

    protocol.registerShuffle(firstShuffleId, partitionCount)
    protocol.registerShuffle(secondShuffleId, partitionCount)
    val doomedKeys = Seq(producerKey(partition = 0), producerKey(partition = 1))
    val survivingKey = producerKey(shuffleId = secondShuffleId, partition = 0)
    val allKeys = doomedKeys :+ survivingKey
    allKeys.foreach { key =>
      assert(protocol.registerStream(key, creditLimitBytes), s"the ledger of $key must open")
    }
    assert(protocol.registerStream(doomedKeys.head, creditLimitBytes),
      "a second owner must join one of the doomed ledgers, so the drop has to override the count")
    allKeys.foreach { key =>
      (0 until entriesPerStream).foreach { sequenceNumber =>
        assert(protocol.tryAdmit(key, blockBytes, sequenceNumber.toLong),
          s"entry $sequenceNumber of $key must enter the unacknowledged window")
      }
    }
    val perLedgerBytes = BackpressureProtocol.STREAM_LEDGER_BASE_BYTES +
      entriesPerStream * BackpressureProtocol.STREAM_LEDGER_ENTRY_BYTES
    assert(protocol.reservedMetadataQuotaBytes === allKeys.size * perLedgerBytes,
      "each live ledger holds its base charge plus one charge per unacknowledged entry")

    assert(protocol.unregisterShuffle(firstShuffleId) === doomedKeys.size,
      "dropping a shuffle drops every stream belonging to it, second owners included")
    assert(protocol.streamCount === 1, "and only those")
    assert(protocol.reservedMetadataQuotaBytes === perLedgerBytes,
      "a dropped ledger returns its whole charge, so only the surviving stream's remains")

    doomedKeys.foreach { key =>
      assert(!protocol.unregisterStream(key),
        s"the surviving owner of the dropped $key finds no ledger left to release")
    }
    assert(protocol.reservedMetadataQuotaBytes === perLedgerBytes,
      "and that release cannot return a second time what the drop already returned")

    assert(protocol.unregisterShuffle(secondShuffleId) === 1, "the last shuffle drops its stream")
    assert(protocol.reservedMetadataQuotaBytes === 0L, "leaving this protocol holding no metadata")
    assert(quota.reservedBytes === 0L,
      "and the executor's aggregate allowance back exactly where it began, because the allowance " +
        "is shared process-wide and a charge left on it would be read as another owner's buffers")
  }

  test("resetting the protocol returns every live ledger's metadata to the executor allowance") {
    val entriesPerStream = 3
    val quota = ledgerAccountingQuota()
    val clock = newManualClock()
    val conf = streamingConfWithOverrides()
    val protocol = new BackpressureProtocol(
      conf, null, TokenBucketRateLimiter.executorBudget(conf, clock), clock, quota)

    protocol.registerShuffle(firstShuffleId, partitionCount)
    val keys = Seq(producerKey(partition = 0), producerKey(partition = 1))
    keys.foreach { key =>
      assert(protocol.registerStream(key, creditLimitBytes), s"the ledger of $key must open")
      (0 until entriesPerStream).foreach { sequenceNumber =>
        assert(protocol.tryAdmit(key, blockBytes, sequenceNumber.toLong),
          s"entry $sequenceNumber of $key must enter the unacknowledged window")
      }
    }
    assert(protocol.reservedMetadataQuotaBytes === keys.size *
      (BackpressureProtocol.STREAM_LEDGER_BASE_BYTES +
        entriesPerStream * BackpressureProtocol.STREAM_LEDGER_ENTRY_BYTES),
      "both ledgers are charged before the reset")

    protocol.reset()
    assert(protocol.streamCount === 0, "the reset drops every ledger")
    assert(protocol.reservedMetadataQuotaBytes === 0L,
      "and a protocol that reports what a freshly constructed one would cannot still hold metadata")
    assert(quota.reservedBytes === 0L,
      "so a suite that resets between cases leaves the next one its whole allowance")
  }

  test("every registration guard refuses without changing any state") {
    val clock = newManualClock()
    val protocol = newProtocol(clock)
    protocol.registerShuffle(firstShuffleId, partitionCount)
    val key = producerKey()
    assert(protocol.registerStream(key, creditLimitBytes), "the ledger must open")

    val shufflesBefore = protocol.registeredShuffleIds
    val streamsBefore = protocol.registeredStreams
    val creditBefore = protocol.availableCreditBytes(key)
    val eventsBefore = observedBackpressureEvents()

    val refusals: Seq[(String, () => Any)] = Seq(
      ("a negative shuffle id", () => protocol.registerShuffle(-1, partitionCount)),
      ("a zero partition count", () => protocol.registerShuffle(firstShuffleId + 1, 0)),
      ("a negative partition count", () => protocol.registerShuffle(firstShuffleId + 1, -1)),
      ("a null stream key", () => protocol.registerStream(null, creditLimitBytes)),
      ("a stream key naming a negative shuffle",
        () => protocol.registerStream(
          BackpressureStreamKey.forProducer(-1, mapId, taskAttemptId, partitionId, consumerId),
          creditLimitBytes)),
      ("a stream key naming a negative partition",
        () => protocol.registerStream(
          BackpressureStreamKey.forProducer(firstShuffleId, mapId, taskAttemptId, -1, consumerId),
          creditLimitBytes)),
      ("a zero credit limit", () => protocol.registerStream(producerKey(partition = 1), 0L)),
      ("a negative credit limit", () => protocol.registerStream(producerKey(partition = 1), -1L)))

    refusals.foreach { case (description, attempt) =>
      intercept[IllegalArgumentException] {
        attempt()
      }
    }

    assert(protocol.registeredShuffleIds === shufflesBefore,
      s"no refused registration may add a shuffle, but the registry went from " +
        s"${shufflesBefore.mkString("[", ", ", "]")} to " +
        s"${protocol.registeredShuffleIds.mkString("[", ", ", "]")}")
    assert(protocol.registeredStreams === streamsBefore,
      s"no refused registration may open a ledger, but the streams went from " +
        s"${streamsBefore.mkString("[", ", ", "]")} to " +
        s"${protocol.registeredStreams.mkString("[", ", ", "]")}")
    assert(protocol.availableCreditBytes(key) === creditBefore,
      s"no refused registration may disturb an open ledger's credit, but it went from " +
        s"${creditBefore} to ${protocol.availableCreditBytes(key)}")
    assert(observedBackpressureEvents() === eventsBefore,
      s"a malformed call is a programming error rather than backpressure, so no event may be " +
        s"recorded, but the counter went from ${eventsBefore} to ${observedBackpressureEvents()}")
    assert(protocol.egressBytes === 0L && protocol.ingressBytes === 0L,
      s"no refused call may charge the link, but egress read ${protocol.egressBytes} and ingress " +
        s"${protocol.ingressBytes}")
  }

  test("every sequence and total guard refuses without changing any state") {
    val clock = newManualClock()
    val protocol = newProtocol(clock)
    val key = producerKey()
    val receiveKey = consumerKey()
    protocol.registerShuffle(firstShuffleId, partitionCount)
    assert(protocol.registerStream(key, creditLimitBytes), "the producer ledger must open")
    assert(protocol.registerStream(receiveKey, creditLimitBytes), "the consumer ledger must open")
    assert(protocol.tryAdmit(key, blockBytes, 0L), "one block is admitted so there is a window")

    val outstandingBefore = protocol.outstandingBytes(key)
    val creditBefore = protocol.availableCreditBytes(key)
    val sentBefore = protocol.sentBytes(key)
    val chargedBefore = protocol.highestChargedSequenceNumber(key)
    val windowBefore = protocol.unacknowledgedWindow(key)
    val egressBefore = protocol.egressBytes
    val ingressBefore = protocol.ingressBytes
    val replayedBefore = protocol.replayedBytes(key)
    val eventsBefore = observedBackpressureEvents()

    val refusals: Seq[(String, () => Any)] = Seq(
      ("a negative sequence number offered for admission",
        () => protocol.tryAdmit(key, blockBytes, -1L)),
      ("a negative sequence number offered for a replay",
        () => protocol.tryAdmitReplay(key, blockBytes, -1L)),
      ("a negative sequence number offered on receipt",
        () => protocol.onDataReceived(receiveKey, -1L, blockBytes)),
      ("a negative announced block total",
        () => protocol.onStreamTermination(receiveKey, -1L)))

    refusals.foreach { case (description, attempt) =>
      intercept[IllegalArgumentException] {
        attempt()
      }
    }

    assert(protocol.outstandingBytes(key) === outstandingBefore &&
        protocol.availableCreditBytes(key) === creditBefore &&
        protocol.sentBytes(key) === sentBefore,
      s"no refused call may charge or release credit, but outstanding went from " +
        s"${outstandingBefore} to ${protocol.outstandingBytes(key)}, credit from ${creditBefore} " +
        s"to ${protocol.availableCreditBytes(key)} and sent from ${sentBefore} to " +
        s"${protocol.sentBytes(key)}")
    assert(protocol.highestChargedSequenceNumber(key) === chargedBefore,
      s"no refused call may advance the charged sequence cursor, but it went from " +
        s"${chargedBefore} to ${protocol.highestChargedSequenceNumber(key)}")
    assert(protocol.unacknowledgedWindow(key) === windowBefore,
      s"no refused call may widen the unacknowledged window, but it went from ${windowBefore} to " +
        s"${protocol.unacknowledgedWindow(key)}")
    assert(protocol.replayedBytes(key) === replayedBefore,
      s"no refused replay may be counted as replayed volume, but it went from ${replayedBefore} " +
        s"to ${protocol.replayedBytes(key)}")
    assert(protocol.egressBytes === egressBefore && protocol.ingressBytes === ingressBefore,
      s"no refused call may charge the link, but egress went from ${egressBefore} to " +
        s"${protocol.egressBytes} and ingress from ${ingressBefore} to ${protocol.ingressBytes}")
    assert(observedBackpressureEvents() === eventsBefore,
      s"no refused call may record a backpressure event, but the counter went from " +
        s"${eventsBefore} to ${observedBackpressureEvents()}")
    assert(protocol.isStreamRegistered(key) && protocol.isStreamRegistered(receiveKey),
      "both ledgers must still be open after every refusal")
  }
}
