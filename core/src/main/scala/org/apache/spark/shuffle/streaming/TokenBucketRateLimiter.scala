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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{CONFIG, COUNT, MAX_SIZE, NUM_BLOCKS, NUM_BYTES, VALUE}
import org.apache.spark.network.shuffle.protocol.streaming.DataBlockMessage
import org.apache.spark.util.{Clock, SystemClock}

/**
 * A non-blocking token bucket that paces streaming shuffle egress on a producer executor.
 *
 * Acquisition never parks, sleeps or waits for a peer: it is consulted from Netty event-loop
 * threads, where blocking would stall every channel the loop serves, so a refusal is returned to
 * the caller instead. A refused block is held and retried rather than dropped, and that refusal is
 * also the signal the spill decision reacts to.
 *
 * The rate a limiter enforces is the operator's administered cap, held to
 * [[TokenBucketRateLimiter.BANDWIDTH_CEILING_PERCENT]] of the link and divided by the number of
 * shuffles the executor is producing for, so a share moves as that concurrency moves. An absent cap
 * selects an unlimited limiter that admits everything without reading the clock.
 *
 * @param initialCapacityBytes bucket size in bytes, that is the maximum burst; must be positive
 * @param initialRefillBytesPerSecond token rate in bytes per second; must be positive
 * @param clock time source for token accrual, so accrual is a function of this clock alone and
 *     no reading of wall time enters the rate
 * @param debugEnabled the value of spark.shuffle.streaming.debug, read once by the caller and
 *     held immutably here.
 * @param ceiling the executor-wide bucket every charge made against this one is also charged
 *     against, or None for a limiter that answers for itself alone.
 */
private[spark] class TokenBucketRateLimiter(
    initialCapacityBytes: Long,
    initialRefillBytesPerSecond: Long,
    clock: Clock = new SystemClock,
    debugEnabled: Boolean = false,
    ceiling: Option[TokenBucketRateLimiter] = None) extends Logging {

  require(initialCapacityBytes > 0L,
    "The streaming shuffle token bucket capacity must be positive but was " +
      s"$initialCapacityBytes.")
  require(initialRefillBytesPerSecond > 0L,
    "The streaming shuffle token bucket refill rate must be positive but was " +
      s"$initialRefillBytesPerSecond bytes/s.")

  /** Whether this limiter enforces no rate at all. */
  private val unlimited: Boolean =
    initialRefillBytesPerSecond >= TokenBucketRateLimiter.UNLIMITED_BYTES_PER_SECOND

  /**
   * The live state of the bucket: the tokens it holds, the instant up to which accrual has already
   * been credited, and the pacing parameters those two were measured against.
   */
  private val state: AtomicReference[TokenBucketRateLimiter.BucketState] =
    new AtomicReference(
      new TokenBucketRateLimiter.BucketState(initialCapacityBytes, clock.nanoTime(),
        initialCapacityBytes, initialRefillBytesPerSecond))

  /** Requests refused because they asked for more than the bucket could ever hold. */
  private val oversizedRequests = new AtomicLong(0L)

  /** Latch ensuring the oversized-request warning is emitted at most once per limiter. */
  private val oversizedLogged = new AtomicBoolean(false)

  /**
   * Requests this limiter refused, whatever refused them: its own tokens or the executor-wide
   * ceiling behind it.
   */
  private val refusals = new AtomicLong(0L)

  // At most one line is logged per limiter, at construction, and nothing is ever logged per
  // acquisition attempt: a limiter is consulted once per outbound block, so a per attempt line
  // would exhaust the per executor log budget within seconds.
  if (debugEnabled) {
    if (unlimited) {
      logDebug(log"Created an unlimited streaming shuffle rate limiter; every egress request is " +
        log"admitted without consulting the clock")
    } else {
      logDebug(log"Created a streaming shuffle rate limiter with burst " +
        log"${MDC(MAX_SIZE, initialCapacityBytes)} bytes and refill rate " +
        log"${MDC(NUM_BYTES, initialRefillBytesPerSecond)} bytes/s")
    }
  }

  /** The bucket size in force right now. */
  def capacityBytes: Long = state.get().capacityBytes

  /** The token rate in force right now. */
  def refillBytesPerSecond: Long = state.get().refillBytesPerSecond

  /**
   * The largest request this limiter can ever admit, which a caller frames to rather than discovers
   * by refusal.
   */
  def maxAcquirableBytes: Long = {
    if (unlimited) {
      Long.MaxValue
    } else {
      ceiling.map(c => math.min(capacityBytes, c.maxAcquirableBytes)).getOrElse(capacityBytes)
    }
  }

  /**
   * Requests this limiter refused, by its own tokens or by the executor-wide ceiling behind it.
   *
   * One refused request is counted here once, and a request the ceiling refused is counted a second
   * time on the ceiling's own limiter, since that is where it was turned away. The two totals are
   * therefore per-level readings of the same events and must not be added together: this one is the
   * authoritative count of what one shuffle was refused, and the ceiling's is the authoritative
   * count of what the executor was refused across all of its shuffles.
   */
  def refusalCount: Long = refusals.get()

  /** Number of requests refused for asking more than the bucket's whole capacity. */
  def oversizedRequestCount: Long = oversizedRequests.get()

  /**
   * Attempts to charge `bytes` tokens against the bucket.
   *
   * @param bytes the number of bytes the caller wants to send; must not be negative.
   * @return true if the tokens were charged and the caller may send now, false if the request
   *     is over limit and the caller must hold the data and retry
   * @throws IllegalArgumentException when `bytes` is negative
   */
  def tryAcquire(bytes: Long): Boolean = {
    require(bytes >= 0L, s"Acquired bytes must be non-negative but was $bytes.")
    if (unlimited) {
      // Unlimited fast path: no clock read, no arithmetic and no compare-and-set, because there is
      // no rate to enforce.
      true
    } else if (bytes == 0L) {
      true
    } else if (!chargeBucket(bytes)) {
      refusals.incrementAndGet()
      false
    } else if (!chargeCeiling(bytes)) {
      // This shuffle's own share had room but the executor's administered ceiling did not, so the
      // tokens just taken are given back.
      refund(bytes)
      refusals.incrementAndGet()
      false
    } else {
      true
    }
  }

  /**
   * Charges the executor-wide ceiling, if this limiter has one.
   *
   * @param bytes bytes already charged against this bucket
   * @return true if the ceiling admitted them, false if it refused
   */
  private def chargeCeiling(bytes: Long): Boolean = ceiling.forall(_.tryAcquire(bytes))

  /**
   * Returns tokens charged for a request that a later gate in the same admission refused.
   *
   * @param bytes the tokens to return; non-positive returns nothing
   */
  private[streaming] def refund(bytes: Long): Unit = {
    if (!unlimited && bytes > 0L) {
      var attempting = true
      while (attempting) {
        val observed = state.get()
        val restored = math.min(observed.capacityBytes,
          TokenBucketRateLimiter.saturatingAdd(observed.tokens, bytes))
        attempting = !state.compareAndSet(observed,
          new TokenBucketRateLimiter.BucketState(restored, observed.lastRefillNanos,
            observed.capacityBytes, observed.refillBytesPerSecond))
      }
    }
  }

  /** The tokens the bucket holds right now, accrual included. */
  def availableTokens: Long = {
    if (unlimited) {
      Long.MaxValue
    } else {
      val own = refill(state.get(), clock.nanoTime()).tokens
      // The smaller of the two, because a charge has to clear both gates: reporting this bucket's
      // tokens alone would answer "how much may I send" with a figure the executor's ceiling would
      // refuse.
      ceiling.map(c => math.min(own, c.availableTokens)).getOrElse(own)
    }
  }

  /**
   * How long until this bucket holds `bytes` tokens, in milliseconds, or zero if it already does.
   *
   * @param bytes the number of tokens the caller wants; must be non-negative
   */
  def millisUntilAvailable(bytes: Long): Long = {
    require(bytes >= 0L,
      s"The streaming shuffle token request must be non-negative but was $bytes.")
    if (unlimited || bytes == 0L) {
      0L
    } else {
      val observed = refill(state.get(), clock.nanoTime())
      val ownWait = if (bytes > observed.capacityBytes) {
        Long.MaxValue
      } else {
        val shortfall = bytes - observed.tokens
        if (shortfall <= 0L) {
          0L
        } else {
          val rate = observed.refillBytesPerSecond
          math.max(1L,
            (shortfall * TokenBucketRateLimiter.MILLIS_PER_SECOND + rate - 1L) / rate)
        }
      }
      // The longer of the two waits, because the charge clears the later of the two gates.
      ceiling.map(c => math.max(ownWait, c.millisUntilAvailable(bytes))).getOrElse(ownWait)
    }
  }

  /** Whether this limiter admits everything at no cost. */
  def isUnlimited: Boolean = unlimited

  /** Refills the bucket to its capacity and restarts accrual from the current time. */
  def reset(): Unit = {
    // Refills to the capacity currently in force rather than the one this limiter was constructed
    // with, so resetting after a redistribution does not silently reinstate an old rate.
    val observed = state.get()
    state.set(new TokenBucketRateLimiter.BucketState(observed.capacityBytes, clock.nanoTime(),
      observed.capacityBytes, observed.refillBytesPerSecond))
  }

  /** The compare-and-set refill-and-charge loop behind [[tryAcquire]] for a bounded bucket. */
  private def chargeBucket(bytes: Long): Boolean = {
    var acquired = false
    var attempting = true
    while (attempting) {
      val observed = state.get()
      if (bytes > observed.capacityBytes) {
        // A request larger than the whole bucket is refused outright, and refused without charging
        // anything.
        recordOversizedRequest(bytes, observed.capacityBytes)
        attempting = false
      } else {
        val refreshed = refill(observed, clock.nanoTime())
        if (refreshed.tokens < bytes) {
          // Over limit, and deliberately publishing nothing: accrual is derived from the elapsed
          // time since the recorded refill point, so the tokens minted above are not lost, and a
          // refused caller adds no compare-and-set traffic to the hot path.
          attempting = false
        } else {
          // The full requested amount is debited.
          val next = new TokenBucketRateLimiter.BucketState(refreshed.tokens - bytes,
            refreshed.lastRefillNanos, refreshed.capacityBytes, refreshed.refillBytesPerSecond)
          if (state.compareAndSet(observed, next)) {
            acquired = true
            attempting = false
          }
          // A failed compare-and-set means another acquirer published first, so re-read and retry.
        }
      }
    }
    acquired
  }

  /** Records a request that asked for more than the bucket could ever hold. */
  private def recordOversizedRequest(bytes: Long, capacity: Long): Unit = {
    oversizedRequests.incrementAndGet()
    if (oversizedLogged.compareAndSet(false, true)) {
      logWarning(log"Refusing a streaming shuffle egress request of " +
        log"${MDC(NUM_BYTES, bytes)} bytes because it exceeds the whole token-bucket capacity of " +
        log"${MDC(MAX_SIZE, capacity)} bytes. Every bucket is sized to hold at least one " +
        log"maximum-sized encoded frame, so this indicates the caller is framing blocks larger " +
        log"than the streaming protocol permits; further such refusals are counted but not logged")
    }
  }

  /**
   * Republishes this bucket at a new token rate, resizing its burst allowance to match.
   *
   * @param newRefillBytesPerSecond the token rate to adopt; must be positive
   * @return true if a new rate was published, false if this limiter is unlimited, the rate was
   *     non-positive, or the bucket already paced at exactly this rate
   */
  def updateRate(newRefillBytesPerSecond: Long): Boolean = {
    if (unlimited || newRefillBytesPerSecond <= 0L) {
      false
    } else {
      val newCapacity = TokenBucketRateLimiter.burstCapacityBytes(newRefillBytesPerSecond)
      var published = false
      var attempting = true
      while (attempting) {
        val observed = state.get()
        if (observed.refillBytesPerSecond == newRefillBytesPerSecond &&
            observed.capacityBytes == newCapacity) {
          // Already pacing at exactly this rate.
          attempting = false
        } else {
          val refreshed = refill(observed, clock.nanoTime())
          val next = new TokenBucketRateLimiter.BucketState(
            math.min(refreshed.tokens, newCapacity), refreshed.lastRefillNanos,
            newCapacity, newRefillBytesPerSecond)
          if (state.compareAndSet(observed, next)) {
            published = true
            attempting = false
          }
        }
      }
      published
    }
  }

  /**
   * Credits the tokens earned since `observed` was recorded, discarding any surplus above the
   * capacity.
   *
   * @param observed the snapshot to advance
   * @param nowNanos the current monotonic time in nanoseconds, as reported by the injected
   *     clock
   * @return the refilled snapshot, or `observed` when no whole token was earned
   */
  private def refill(
      observed: TokenBucketRateLimiter.BucketState,
      nowNanos: Long): TokenBucketRateLimiter.BucketState = {
    val elapsedNanos = nowNanos - observed.lastRefillNanos
    if (elapsedNanos <= 0L) {
      observed
    } else {
      // The rate and the capacity are taken from the snapshot being advanced, never from a field of
      // the limiter, so accrual is always measured against the parameters that snapshot was
      // published with even if a redistribution lands mid-computation.
      val minted = TokenBucketRateLimiter.tokensEarned(
        elapsedNanos, observed.refillBytesPerSecond)
      if (minted <= 0L) {
        observed
      } else {
        // The deficit form of the surplus-discard rule.
        val headroom = observed.capacityBytes - observed.tokens
        if (minted >= headroom) {
          new TokenBucketRateLimiter.BucketState(observed.capacityBytes, nowNanos,
            observed.capacityBytes, observed.refillBytesPerSecond)
        } else {
          new TokenBucketRateLimiter.BucketState(observed.tokens + minted, nowNanos,
            observed.capacityBytes, observed.refillBytesPerSecond)
        }
      }
    }
  }
}

/** Constants, arithmetic and factories for [[TokenBucketRateLimiter]]. */
private[spark] object TokenBucketRateLimiter extends Logging {

  /** The token rate that selects the unlimited fast path. */
  val UNLIMITED_BYTES_PER_SECOND: Long = Long.MaxValue

  /**
   * Share of the administered link capacity, as declared by
   * spark.shuffle.streaming.maxBandwidthMBps, that streaming shuffle egress may occupy in aggregate
   * across every shuffle an executor is serving.
   */
  val BANDWIDTH_CEILING_PERCENT: Long = 80L

  val PERCENT_SCALE: Long = 100L

  /** Bytes in one MiB, the unit in which the operator declares the bandwidth cap. */
  val BYTES_PER_MIB: Long = 1024L * 1024L

  /**
   * Largest number of bytes the streaming wire protocol will ever put on the network for one block,
   * framing included, which is also the floor for a bucket's capacity.
   */
  val MAX_ENCODED_FRAME_BYTES: Long = DataBlockMessage.MAX_ENCODED_FRAME_BYTES.toLong

  /** Seconds of sustained throughput a bucket may bank as burst allowance. */
  val BURST_SECONDS: Long = 1L

  /** Milliseconds in one second; accrual is computed in milliseconds. */
  val MILLIS_PER_SECOND: Long = 1000L

  /** Nanoseconds in one second, the divisor that turns a monotonic interval into earned tokens. */
  val NANOS_PER_SECOND: Long = 1000000000L

  /**
   * Builds the executor's egress budget, out of which each shuffle's limiter is handed.
   *
   * @param conf the executor's configuration; the cap and the debug gate are read once here and
   *     never consulted again, which is what makes an executor restart the only way to change
   *     either
   * @param clock time source handed to every limiter the budget creates, so accrual across the
   *     whole budget advances from one reading rather than from wall time
   * @param concurrencySource asks the coordinator how many streaming shuffles this executor is
   *     producing for, answering `None` when the question cannot be put
   * @return a budget that paces to the declared capacity, or one that paces nothing when no
   *     capacity is declared
   */
  def executorBudget(
      conf: SparkConf,
      clock: Clock = new SystemClock,
      concurrencySource: () => Option[Int] = () => None): ExecutorEgressBudget = {
    val debug = conf.get(config.SHUFFLE_STREAMING_DEBUG)
    val cap = conf.get(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)
    if (debug) {
      cap match {
        case Some(maxBandwidthMBps) =>
          val firstShare = applyLinkCapacityCeiling(perShuffleBytesPerSecond(maxBandwidthMBps, 1))
          logInfo(log"Streaming shuffle egress budget derived from the link capacity " +
            log"${MDC(CONFIG, config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS.key)}=" +
            log"${MDC(VALUE, maxBandwidthMBps)} MB/s, held to " +
            log"${MDC(NUM_BYTES, BANDWIDTH_CEILING_PERCENT)}% of it and divided among the " +
            log"shuffles this executor produces for: while it produces for one, that one paces " +
            log"at ${MDC(MAX_SIZE, firstShare)} bytes/s")
        case None =>
          logInfo(log"Streaming shuffle egress is uncapped because " +
            log"${MDC(CONFIG, config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS.key)} is unset")
      }
    }
    new ExecutorEgressBudget(cap, clock, debug, concurrencySource)
  }

  /**
   * Builds a limiter that enforces no rate, admitting every request without reading the clock.
   *
   * @param clock time source handed to the limiter; unused on the acquisition path
   * @param debugEnabled the value of spark.shuffle.streaming.debug, threaded through so that
   *     the limiter's single construction diagnostic stays governed by that key alone
   */
  def unlimited(
      clock: Clock = new SystemClock,
      debugEnabled: Boolean = false): TokenBucketRateLimiter = {
    new TokenBucketRateLimiter(
      UNLIMITED_BYTES_PER_SECOND, UNLIMITED_BYTES_PER_SECOND, clock, debugEnabled)
  }

  /**
   * Splits the operator's declared link capacity evenly across the shuffles this executor is
   * serving.
   *
   * @param maxBandwidthMBps the administered link capacity in MB/s; a non-positive value is
   *     floored at one MB/s so that a directly constructed limiter can still make progress, even
   *     though the configuration entry itself rejects such a value
   * @param numConcurrentShuffles active shuffles on this executor; zero or less is treated as
   *     one, because a producer that is streaming is by definition serving at least one shuffle
   * @return the per shuffle share in bytes per second, never less than one byte per second
   */
  def perShuffleBytesPerSecond(maxBandwidthMBps: Int, numConcurrentShuffles: Int): Long = {
    val shuffles = math.max(1L, numConcurrentShuffles.toLong)
    val administeredBytesPerSecond = math.max(1L, maxBandwidthMBps.toLong) * BYTES_PER_MIB
    math.max(1L, administeredBytesPerSecond / shuffles)
  }

  /**
   * Holds a rate to [[BANDWIDTH_CEILING_PERCENT]] of itself, which is the second of the two steps
   * [[apply]] composes.
   *
   * Multiplying before dividing, so the result is the exact eighty percent of the input rather than
   * eighty percent of it rounded down to a whole hundred bytes per second first. The rounding was
   * immaterial in size -- sixty bytes per second at a one MB/s cap -- but it made the ceiling
   * describe itself inaccurately, and a share that is reported as eighty percent should be eighty
   * percent.
   *
   * Overflow is guarded rather than avoided by rounding: the product only exceeds a Long above
   * roughly 1.15e17 bytes per second, which no administered cap can reach because the configuration
   * entry is an Int of MB/s and its largest value is 2.25e15 bytes per second, and the branch below
   * keeps the method total for every non-negative Long regardless. The result is floored at one
   * byte per second so that a very small cap throttles rather than halts.
   *
   * @param bytesPerSecond the rate to hold down
   * @return the paced rate, never less than one byte per second
   */
  def applyLinkCapacityCeiling(bytesPerSecond: Long): Long = {
    val ceiled =
      if (bytesPerSecond <= Long.MaxValue / BANDWIDTH_CEILING_PERCENT) {
        (bytesPerSecond * BANDWIDTH_CEILING_PERCENT) / PERCENT_SCALE
      } else {
        // Unreachable from any configured cap; kept so the arithmetic cannot wrap for any caller.
        (bytesPerSecond / PERCENT_SCALE) * BANDWIDTH_CEILING_PERCENT
      }
    math.max(1L, ceiled)
  }

  /**
   * Sizes the burst allowance for a given token rate.
   *
   * @param refillBytesPerSecond the token rate the bucket will refill at
   * @return the bucket size in bytes, at least [[MAX_ENCODED_FRAME_BYTES]]
   */
  def burstCapacityBytes(refillBytesPerSecond: Long): Long = {
    math.max(MAX_ENCODED_FRAME_BYTES, saturatingMultiply(refillBytesPerSecond, BURST_SECONDS))
  }

  /**
   * Multiplies two non-negative Longs, saturating at Long.MaxValue instead of wrapping.
   *
   * @param a multiplicand; a non-positive value yields zero, since neither an elapsed interval
   *     nor a byte rate can be negative
   * @param b multiplier, interpreted the same way
   * @return a * b, or Long.MaxValue when that product does not fit in a Long
   */
  def saturatingMultiply(a: Long, b: Long): Long = {
    if (a <= 0L || b <= 0L) {
      0L
    } else {
      val product = a * b
      if (product / a != b) Long.MaxValue else product
    }
  }

  /**
   * Whole tokens earned by `elapsedNanos` of accrual at `bytesPerSecond`.
   *
   * @param elapsedNanos monotonic interval to credit; a non-positive value earns nothing
   * @param bytesPerSecond the token rate to credit it at; a non-positive rate earns nothing
   * @return tokens earned, never negative
   */
  def tokensEarned(elapsedNanos: Long, bytesPerSecond: Long): Long = {
    if (elapsedNanos <= 0L || bytesPerSecond <= 0L) {
      0L
    } else {
      val wholeSeconds = elapsedNanos / NANOS_PER_SECOND
      val remainderNanos = elapsedNanos - wholeSeconds * NANOS_PER_SECOND
      saturatingAdd(
        saturatingMultiply(wholeSeconds, bytesPerSecond),
        saturatingMultiply(remainderNanos, bytesPerSecond) / NANOS_PER_SECOND)
    }
  }

  /** `a + b` for non-negative operands, saturating at `Long.MaxValue` instead of wrapping. */
  def saturatingAdd(a: Long, b: Long): Long = {
    val sum = a + b
    if (sum < 0L) Long.MaxValue else sum
  }

  /**
   * An immutable snapshot of a bucket: the tokens it holds, and the instant up to which accrual has
   * been credited.
   *
   * @param tokens tokens currently held, always within [0, capacityBytes]
   * @param lastRefillNanos monotonic clock reading, in nanoseconds, up to which accrual is
   *     credited.
   * @param capacityBytes bucket size this snapshot was measured against
   * @param refillBytesPerSecond token rate this snapshot accrues at
   */
  private[streaming] class BucketState(
      val tokens: Long,
      val lastRefillNanos: Long,
      val capacityBytes: Long,
      val refillBytesPerSecond: Long)

  /**
   * The executor's whole streaming shuffle egress allowance, divided among the shuffles it is
   * currently producing for.
   *
   * @param maxBandwidthMBps the operator's declared egress cap in MB/s, or None for unlimited.
   * @param clock time source handed to every limiter this budget creates, injected for
   *     determinism
   * @param concurrencySource asks the coordinator how many streaming shuffles this executor is
   *     currently producing for, answering `None` when the question cannot be put.
   */
  class ExecutorEgressBudget(
      maxBandwidthMBps: Option[Int],
      clock: Clock = new SystemClock,
      debugEnabled: Boolean = false,
      concurrencySource: () => Option[Int] = () => None)
    extends Logging {

    /** Guards the registry and the redistribution pass. */
    private val lock = new Object

    /** The executor-wide bucket every per-shuffle charge is also charged against. */
    private val aggregate: TokenBucketRateLimiter = maxBandwidthMBps match {
      case Some(cap) =>
        val ceilingRate = applyLinkCapacityCeiling(
          TokenBucketRateLimiter.perShuffleBytesPerSecond(cap, 1))
        new TokenBucketRateLimiter(
          burstCapacityBytes(ceilingRate), ceilingRate, clock, debugEnabled)
      case None =>
        unlimited(clock, debugEnabled)
    }

    /** Live limiters by shuffle id. */
    private val limiters = new mutable.HashMap[Int, TokenBucketRateLimiter]()

    private var reportedConcurrency = 0

    /**
     * The limiter pacing one shuffle's egress on this executor, creating it on first use.
     *
     * @param shuffleId shuffle to pace
     * @return the limiter for that shuffle, never null and never shared with another shuffle
     */
    def limiterFor(shuffleId: Int): TokenBucketRateLimiter = lock.synchronized {
      val limiter = limiters.getOrElseUpdate(shuffleId, newLimiter())
      redistribute()
      limiter
    }

    /**
     * Retires a shuffle's limiter, returning its share to the shuffles that remain.
     *
     * @param shuffleId shuffle whose limiter should be retired
     * @return true if a limiter was present and retired
     */
    def release(shuffleId: Int): Boolean = {
      val removed = lock.synchronized {
        val present = limiters.remove(shuffleId).isDefined
        if (present) {
          redistribute()
        }
        present
      }
      if (removed) {
        // Outside the lock, because the question may cross the network.
        refreshConcurrency()
      }
      removed
    }

    /**
     * Re-reads the coordinator's view of this executor's concurrency and republishes the shares if
     * it moved the divisor.
     *
     * @return true if the divisor moved and limiters were republished
     */
    def refreshConcurrency(): Boolean = {
      // Read outside the lock.
      concurrencySource().exists(observeConcurrency)
    }

    /**
     * Records the executor-scoped concurrency the coordinator reports, redistributing if it moves
     * the divisor.
     *
     * @param numConcurrentShuffles concurrency reported for this executor; non-positive is
     *     ignored
     * @return true if the divisor moved and limiters were republished
     */
    def observeConcurrency(numConcurrentShuffles: Int): Boolean = lock.synchronized {
      if (numConcurrentShuffles > 0 && numConcurrentShuffles != reportedConcurrency) {
        reportedConcurrency = numConcurrentShuffles
        redistribute()
        true
      } else {
        false
      }
    }

    /**
     * The divisor currently applied to the operator's cap: the larger of the local limiter count
     * and the concurrency the coordinator last reported, never less than one.
     */
    def divisor: Int = lock.synchronized(currentDivisor)

    /**
     * The token rate every live limiter is currently pacing at, or None when egress is uncapped.
     */
    def currentShareBytesPerSecond: Option[Long] = lock.synchronized {
      maxBandwidthMBps.map(currentShare)
    }

    /**
     * Drops every limiter and forgets the reported concurrency, returning the budget to the state a
     * freshly constructed one would report.
     */
    def reset(): Unit = lock.synchronized {
      limiters.clear()
      reportedConcurrency = 0
      // The shared ceiling outlives individual limiters, so returning the budget to a freshly
      // constructed state has to refill it too; leaving it drained would pace the next measurement
      // against tokens the previous one spent.
      aggregate.reset()
    }

    /** Divisor of the cap. */
    private def currentDivisor: Int = math.max(1, math.max(limiters.size, reportedConcurrency))

    /** Per-shuffle paced share for a given cap. */
    private def currentShare(cap: Int): Long = {
      applyLinkCapacityCeiling(
        TokenBucketRateLimiter.perShuffleBytesPerSecond(cap, currentDivisor))
    }

    /** Builds a limiter at the share currently in force. */
    private def newLimiter(): TokenBucketRateLimiter = maxBandwidthMBps match {
      case Some(cap) =>
        val share = currentShare(cap)
        // Handed the shared ceiling, so this limiter paces its own share AND contributes to the
        // executor's aggregate being paced.
        new TokenBucketRateLimiter(
          burstCapacityBytes(share), share, clock, debugEnabled, Some(aggregate))
      case None =>
        unlimited(clock, debugEnabled)
    }

    /**
     * The executor-wide ceiling bucket, exposed for the pacing evidence it carries rather than to
     * be charged directly: a caller charges a shuffle's own limiter, which charges this one.
     */
    def aggregateLimiter: TokenBucketRateLimiter = aggregate

    /** The rate the executor's aggregate egress is paced at, or None when egress is uncapped. */
    def aggregateBytesPerSecond: Option[Long] = maxBandwidthMBps.map { cap =>
      applyLinkCapacityCeiling(TokenBucketRateLimiter.perShuffleBytesPerSecond(cap, 1))
    }

    /** Requests the executor-wide ceiling refused, across every shuffle. */
    def aggregateRefusalCount: Long = aggregate.refusalCount

    /** Republishes every live limiter at the share the current divisor implies. */
    private def redistribute(): Unit = {
      maxBandwidthMBps.foreach { cap =>
        val share = currentShare(cap)
        var republished = 0
        limiters.valuesIterator.foreach { limiter =>
          if (limiter.updateRate(share)) {
            republished += 1
          }
        }
        // Gated like every other verbose path in the subsystem: a redistribution happens on every
        // registration and departure, so an ungated line here would scale with task churn.
        if (republished > 0 && debugEnabled) {
          logDebug(log"Redistributed streaming shuffle egress across " +
            log"${MDC(COUNT, currentDivisor)} concurrent shuffles: each of " +
            log"${MDC(NUM_BLOCKS, limiters.size)} live limiters now paces at " +
            log"${MDC(NUM_BYTES, share)} bytes/s")
        }
      }
    }
  }
}
