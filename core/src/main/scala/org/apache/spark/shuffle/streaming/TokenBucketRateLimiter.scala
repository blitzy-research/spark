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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{CONFIG, COUNT, MAX_SIZE, NUM_BYTES, VALUE}
import org.apache.spark.util.{Clock, SystemClock}

/**
 * A non-blocking token bucket that paces streaming shuffle egress on a producer executor.
 *
 * This class owns exactly one of the three backpressure layers the streaming shuffle applies:
 * rate limiting. The other two -- application level credit derived from consumer acknowledgements,
 * and TCP level throttling by toggling channel autoRead -- belong to the backpressure protocol and
 * to the streaming client handler respectively, and are deliberately not implemented here. Keeping
 * the layers apart is what lets each of them be reasoned about, and tested, on its own.
 *
 * The bucket is the canonical two parameter formulation. It has a bucket size, [[capacityBytes]],
 * which is the burst a producer may emit after an idle period, and a token rate,
 * [[refillBytesPerSecond]], which is the sustained throughput the producer is allowed. One token
 * is one byte. Tokens accrue into the bucket as time passes, at the token rate; once the bucket is
 * full any surplus is discarded, so an idle producer banks at most one bucket of credit rather
 * than an unbounded amount. A request arriving when the bucket does not hold enough tokens is over
 * limit, and is refused.
 *
 * A refusal is a signal, not an error. When [[tryAcquire]] returns false the caller keeps the
 * block it was about to send and retries later; that hold is precisely what drives the spill
 * decision in the memory spill manager and the transition into the throttled state in the
 * backpressure protocol, which increments the backpressureEvents counter on every such entry.
 * Callers must therefore treat false as ordinary flow control, and must never turn it into a task
 * failure.
 *
 * Being non-blocking is a hard requirement rather than an optimisation. [[tryAcquire]] is called
 * from Netty event-loop threads, and a parked event-loop thread stalls every channel multiplexed
 * onto it, so this class holds no lock and performs no wait, no sleep and no awaiting of any kind.
 * The refill-and-charge sequence is published with a single compare-and-set over an immutable
 * state snapshot, and a refused request publishes nothing at all: accrual is a pure function of
 * the elapsed time since the recorded refill point, so leaving the state untouched loses no tokens
 * and keeps refusals off the contended path.
 *
 * All time is read through the injected clock, never through the system clock directly, so that a
 * test can freeze or advance time and observe exact token arithmetic instead of racing a real
 * clock. Accrual is measured in milliseconds: that keeps the elapsed-time multiplication far away
 * from Long overflow for every rate derivable from the operator's MB/s setting, and the bucket's
 * burst allowance is at least one second of tokens, which makes sub-millisecond accrual
 * irrelevant -- a refusal inside the same millisecond is simply the throttle signal the protocol
 * expects.
 *
 * An oversized request, one asking for more than the whole bucket can ever hold, is granted as
 * soon as the bucket is full and drains it completely, instead of being refused forever. That rule
 * matters only as a safety net: the factory in the companion object floors the capacity at one
 * maximum sized block, so every legal block fits, and a producer can never be deadlocked by a
 * request the bucket is structurally unable to satisfy.
 *
 * Instances are cheap, and every method is safe to call concurrently from any number of threads.
 *
 * @param capacityBytes bucket size in bytes, that is the maximum burst; must be positive
 * @param refillBytesPerSecond token rate in bytes per second; must be positive.
 *                             [[TokenBucketRateLimiter.UNLIMITED_BYTES_PER_SECOND]] selects the
 *                             unlimited fast path, in which every request is admitted at no cost
 * @param clock time source for token accrual, injected so that tests are deterministic
 */
private[spark] class TokenBucketRateLimiter(
    val capacityBytes: Long,
    val refillBytesPerSecond: Long,
    clock: Clock = new SystemClock) extends Logging {

  require(capacityBytes > 0L,
    s"The streaming shuffle token bucket capacity must be positive but was $capacityBytes.")
  require(refillBytesPerSecond > 0L,
    "The streaming shuffle token bucket refill rate must be positive but was " +
      s"$refillBytesPerSecond bytes/s.")

  /**
   * Whether this limiter enforces no rate at all. Derived once from the sentinel token rate so
   * that the hot path is a single field read rather than a comparison against a bare number.
   */
  private val unlimited: Boolean =
    refillBytesPerSecond >= TokenBucketRateLimiter.UNLIMITED_BYTES_PER_SECOND

  /**
   * The live state of the bucket: the tokens it holds, and the instant up to which accrual has
   * already been credited. Held as an immutable snapshot behind an AtomicReference so that a
   * refill and a charge are published together, atomically, without taking a lock.
   *
   * The bucket starts full, which lets a producer emit one burst straight away rather than waiting
   * out a refill interval before its very first block.
   */
  private val state: AtomicReference[TokenBucketRateLimiter.BucketState] =
    new AtomicReference(
      new TokenBucketRateLimiter.BucketState(capacityBytes, clock.getTimeMillis()))

  // Exactly one line is logged per limiter, at construction, and nothing is ever logged per
  // acquisition attempt: a limiter is consulted once per outbound block, so a per attempt line
  // would exhaust the per executor log budget within seconds.
  if (unlimited) {
    logDebug(log"Created an unlimited streaming shuffle rate limiter; every egress request is " +
      log"admitted without consulting the clock")
  } else {
    logDebug(log"Created a streaming shuffle rate limiter with burst " +
      log"${MDC(MAX_SIZE, capacityBytes)} bytes and refill rate " +
      log"${MDC(NUM_BYTES, refillBytesPerSecond)} bytes/s")
  }

  /**
   * Attempts to charge `bytes` tokens against the bucket.
   *
   * Never blocks, never parks the calling thread and never throws, so it is safe to call from a
   * Netty event-loop thread. A false result is flow control rather than failure: the caller should
   * keep the data it was about to send and try again later, which is exactly the pressure the
   * spill manager and the backpressure protocol react to.
   *
   * @param bytes the number of bytes the caller wants to send. A non-positive value is admitted
   *              without charge, so callers need no special case for an empty payload
   * @return true if the tokens were charged and the caller may send now, false if the request is
   *         over limit and the caller must hold the data and retry
   */
  def tryAcquire(bytes: Long): Boolean = {
    if (unlimited) {
      // Unlimited fast path, tested before anything else: no clock read, no arithmetic and no
      // compare-and-set, because there is no rate to enforce. This is the state an operator gets
      // by leaving spark.shuffle.streaming.maxBandwidthMBps unset.
      true
    } else if (bytes <= 0L) {
      true
    } else {
      chargeBucket(bytes)
    }
  }

  /**
   * The tokens the bucket holds right now, accrual included.
   *
   * This is an observer: it computes the refill that a [[tryAcquire]] call would see, yet
   * publishes nothing, so polling it can never perturb pacing. An unlimited limiter reports
   * Long.MaxValue, the honest answer to "how much may I send" when no rate is enforced.
   */
  def availableTokens: Long = {
    if (unlimited) {
      Long.MaxValue
    } else {
      refill(state.get(), clock.getTimeMillis()).tokens
    }
  }

  /**
   * Whether this limiter admits everything at no cost. The fallback policy and the backpressure
   * protocol use it to skip pacing bookkeeping entirely when egress is uncapped.
   */
  def isUnlimited: Boolean = unlimited

  /**
   * Refills the bucket to its capacity and restarts accrual from the current time.
   *
   * Nothing on the streaming shuffle path calls this, because pacing state is meant to evolve only
   * through [[tryAcquire]]. It exists so that a caller can return a limiter to a known state
   * between measurements, in the spirit of the reset hook the static metric sources expose. This
   * is useful in tests.
   */
  def reset(): Unit = {
    state.set(new TokenBucketRateLimiter.BucketState(capacityBytes, clock.getTimeMillis()))
  }

  /**
   * The lock-free refill-and-charge loop behind [[tryAcquire]] for a bounded bucket.
   *
   * Two acquirers can never jointly consume more than the bucket holds, because every charge is
   * published by a compare-and-set from the exact snapshot the charge was computed against; a
   * racing thread invalidates that snapshot and forces a recomputation.
   */
  private def chargeBucket(bytes: Long): Boolean = {
    // A request larger than the bucket can never be met by `bytes` tokens, so it is met by a full
    // bucket instead. Without this rule such a request would be refused on every single attempt
    // for as long as the producer lived.
    val required = math.min(bytes, capacityBytes)
    var acquired = false
    var attempting = true
    while (attempting) {
      val observed = state.get()
      val refreshed = refill(observed, clock.getTimeMillis())
      if (refreshed.tokens < required) {
        // Over limit, and deliberately publishing nothing: accrual is derived from the elapsed
        // time since the recorded refill point, so the tokens minted above are not lost, and a
        // refused caller adds no compare-and-set traffic to the hot path.
        attempting = false
      } else {
        // math.min stops the bucket going negative when an oversized request drains it.
        val charge = math.min(bytes, refreshed.tokens)
        val next = new TokenBucketRateLimiter.BucketState(
          refreshed.tokens - charge, refreshed.lastRefillMillis)
        if (state.compareAndSet(observed, next)) {
          acquired = true
          attempting = false
        }
        // A failed compare-and-set means another acquirer published first, so re-read and retry.
        // The loop always makes progress, because every failure implies another thread succeeded.
      }
    }
    acquired
  }

  /**
   * Credits the tokens earned since `observed` was recorded, discarding any surplus above the
   * capacity.
   *
   * Pure: it returns a new snapshot, or `observed` itself when nothing changed, and never touches
   * the shared state. Two guards keep it honest. Elapsed time that is zero or negative -- which a
   * wall clock can report when it is adjusted backwards -- mints nothing and never rewinds the
   * accrual point. Elapsed time too short to earn a whole token likewise leaves the accrual point
   * alone, so the fraction is carried into the next call instead of being discarded over and over.
   *
   * @param observed the snapshot to advance
   * @param nowMillis the current time in milliseconds, as reported by the injected clock
   * @return the refilled snapshot, or `observed` when no whole token was earned
   */
  private def refill(
      observed: TokenBucketRateLimiter.BucketState,
      nowMillis: Long): TokenBucketRateLimiter.BucketState = {
    val elapsedMillis = nowMillis - observed.lastRefillMillis
    if (elapsedMillis <= 0L) {
      observed
    } else {
      val minted = TokenBucketRateLimiter.saturatingMultiply(
        elapsedMillis, refillBytesPerSecond) / TokenBucketRateLimiter.MILLIS_PER_SECOND
      if (minted <= 0L) {
        observed
      } else {
        // The deficit form of the surplus-discard rule. Comparing the minted tokens against the
        // remaining headroom, rather than adding first and clamping afterwards, means the addition
        // can never overflow, while clamping to the capacity is exactly the classic rule that
        // tokens arriving at a full bucket are thrown away.
        val headroom = capacityBytes - observed.tokens
        if (minted >= headroom) {
          new TokenBucketRateLimiter.BucketState(capacityBytes, nowMillis)
        } else {
          new TokenBucketRateLimiter.BucketState(observed.tokens + minted, nowMillis)
        }
      }
    }
  }
}

/**
 * Constants, arithmetic and factories for [[TokenBucketRateLimiter]].
 *
 * This object is where configuration becomes arithmetic. The factory reads
 * spark.shuffle.streaming.maxBandwidthMBps exactly once and hands the derived numbers to a limiter
 * that holds them immutably, which is what makes "configuration changes require an executor
 * restart" true by construction rather than by convention: no path exists by which a running
 * limiter can observe a changed value, so there is no dynamic reconfiguration to design.
 *
 * Two formulas are fixed by contract.
 *
 *  - The per shuffle share of the operator's cap is maxBandwidthMBps * 1 MiB divided by the number
 *    of concurrent shuffles. That divisor comes from the streaming shuffle coordinator, which keeps
 *    the executor scoped registry of active shuffles, because no existing Spark API reports how
 *    many shuffles an executor is currently serving.
 *  - The share is then held to 80 percent of the administered link capacity. Administered means the
 *    operator declared cap and nothing else: this subsystem probes neither the operating system nor
 *    the network for a real capacity, so none is invented here. Applying the ceiling to the share
 *    rather than to the cap is what makes the ceiling bite, because the aggregate of N concurrent
 *    limiters is then 80 percent of the cap, which leaves exactly the headroom the fallback policy
 *    relies on when it watches for link saturation.
 *
 * An unset cap means unlimited, never zero. The distinction is load bearing: reading absence as
 * zero would silently stop all egress, so absence is routed to a limiter whose fast path admits
 * everything at no cost.
 */
private[spark] object TokenBucketRateLimiter extends Logging {

  /**
   * The token rate that selects the unlimited fast path.
   *
   * A named sentinel is used in preference to a second constructor parameter so that the hot path
   * stays a single boolean field read, and Long.MaxValue is chosen rather than 0 or -1 so that the
   * value is self describing even if it is ever read without this documentation: a bucket that
   * refills at Long.MaxValue bytes per second is, for every practical purpose, uncapped.
   */
  val UNLIMITED_BYTES_PER_SECOND: Long = Long.MaxValue

  /** Share of the administered link capacity that streaming shuffle egress may occupy. */
  val BANDWIDTH_CEILING_PERCENT: Long = 80L

  /** Divisor that turns a percentage into a fraction with integer arithmetic. */
  val PERCENT_SCALE: Long = 100L

  /** Bytes in one MiB, the unit in which the operator declares the bandwidth cap. */
  val BYTES_PER_MIB: Long = 1024L * 1024L

  /**
   * Largest block the streaming wire protocol will ever put on the network, which is also the
   * floor for a bucket's capacity. The value mirrors the block cap enforced by the protocol's data
   * block message; it is restated here rather than imported so that this class stays free of any
   * dependency on the network module.
   */
  val MAX_BLOCK_SIZE_BYTES: Long = 2L * 1024L * 1024L

  /** Seconds of sustained throughput a bucket may bank as burst allowance. */
  val BURST_SECONDS: Long = 1L

  /** Milliseconds in one second; accrual is computed in milliseconds. */
  val MILLIS_PER_SECOND: Long = 1000L

  /**
   * Builds the limiter that paces one shuffle's egress on this executor.
   *
   * The bandwidth cap is an optional configuration entry, and its absence is the unlimited state:
   * absence is deliberately not encoded as zero or as a negative sentinel, so it is matched
   * explicitly here and routed to [[unlimited]].
   *
   * @param conf the executor's configuration, read once and never consulted again
   * @param numConcurrentShuffles active shuffles on this executor, as reported by the streaming
   *                              shuffle coordinator; a value of zero or less is treated as one
   * @param clock time source handed to the limiter, injected so that tests are deterministic
   * @return a bounded limiter when the cap is set, an unlimited one when it is not
   */
  def apply(
      conf: SparkConf,
      numConcurrentShuffles: Int,
      clock: Clock = new SystemClock): TokenBucketRateLimiter = {
    val debug = conf.get(config.SHUFFLE_STREAMING_DEBUG)
    conf.get(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS) match {
      case Some(maxBandwidthMBps) =>
        val share = perShuffleBytesPerSecond(maxBandwidthMBps, numConcurrentShuffles)
        val paced = applyLinkCapacityCeiling(share)
        val burst = burstCapacityBytes(paced)
        if (debug) {
          logInfo(log"Streaming shuffle egress limiter derived from " +
            log"${MDC(CONFIG, config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS.key)}=" +
            log"${MDC(VALUE, maxBandwidthMBps)} MB/s shared across " +
            log"${MDC(COUNT, numConcurrentShuffles)} concurrent shuffles: refill " +
            log"${MDC(NUM_BYTES, paced)} bytes/s, burst ${MDC(MAX_SIZE, burst)} bytes")
        }
        new TokenBucketRateLimiter(burst, paced, clock)
      case None =>
        if (debug) {
          logInfo(log"Streaming shuffle egress is uncapped because " +
            log"${MDC(CONFIG, config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS.key)} is unset")
        }
        unlimited(clock)
    }
  }

  /**
   * Builds a limiter that enforces no rate, admitting every request without reading the clock.
   *
   * This is the shape an operator gets by leaving the bandwidth cap unset, and it is also the
   * shape a caller should ask for when it wants the pacing layer present but inert.
   *
   * @param clock time source handed to the limiter; unused on the acquisition path
   */
  def unlimited(clock: Clock = new SystemClock): TokenBucketRateLimiter = {
    new TokenBucketRateLimiter(UNLIMITED_BYTES_PER_SECOND, UNLIMITED_BYTES_PER_SECOND, clock)
  }

  /**
   * Splits the operator's declared egress cap evenly across the shuffles this executor is serving.
   *
   * @param maxBandwidthMBps the declared cap in MB/s; a non-positive value is floored at one MB/s
   *                         so that a directly constructed limiter can still make progress, even
   *                         though the configuration entry itself already rejects such values
   * @param numConcurrentShuffles active shuffles on this executor; zero or less is treated as one,
   *                              because a producer that is streaming is by definition serving at
   *                              least one shuffle
   * @return the per shuffle share in bytes per second, never less than one byte per second
   */
  def perShuffleBytesPerSecond(maxBandwidthMBps: Int, numConcurrentShuffles: Int): Long = {
    val shuffles = math.max(1L, numConcurrentShuffles.toLong)
    val administeredBytesPerSecond = math.max(1L, maxBandwidthMBps.toLong) * BYTES_PER_MIB
    math.max(1L, administeredBytesPerSecond / shuffles)
  }

  /**
   * Holds a rate to [[BANDWIDTH_CEILING_PERCENT]] of the administered link capacity.
   *
   * Dividing before multiplying keeps the arithmetic overflow free for every non-negative Long,
   * which multiplying first would not: a rate near Long.MaxValue times eighty wraps. The cost is a
   * rounding error below one hundred bytes per second, which is immaterial against a cap expressed
   * in MB/s, and the result is floored at one byte per second so that a very small cap throttles
   * rather than halts.
   *
   * @param bytesPerSecond the rate to hold down
   * @return the paced rate, never less than one byte per second
   */
  def applyLinkCapacityCeiling(bytesPerSecond: Long): Long = {
    math.max(1L, (bytesPerSecond / PERCENT_SCALE) * BANDWIDTH_CEILING_PERCENT)
  }

  /**
   * Sizes the burst allowance for a given token rate.
   *
   * The allowance is one second of sustained throughput, floored at one maximum sized block. That
   * floor is a liveness guarantee rather than a tuning choice: a bucket smaller than a single legal
   * block could otherwise refuse that block forever, stalling the producer permanently.
   *
   * @param refillBytesPerSecond the token rate the bucket will refill at
   * @return the bucket size in bytes, at least [[MAX_BLOCK_SIZE_BYTES]]
   */
  def burstCapacityBytes(refillBytesPerSecond: Long): Long = {
    math.max(MAX_BLOCK_SIZE_BYTES, saturatingMultiply(refillBytesPerSecond, BURST_SECONDS))
  }

  /**
   * Multiplies two non-negative Longs, saturating at Long.MaxValue instead of wrapping.
   *
   * The refill computation multiplies an elapsed millisecond count by a byte rate, and both can be
   * large, so an unguarded product could wrap negative and hand out tokens that were never earned.
   * Saturation is the correct failure mode here: a saturated product always exceeds the bucket's
   * remaining headroom, so the bucket simply fills, which is precisely what an enormous elapsed
   * interval ought to produce. Overflow is detected with the division identity that
   * java.lang.Math.multiplyExact uses, but without throwing, so the hot path stays free of both
   * allocation and exceptions.
   *
   * @param a multiplicand; a non-positive value yields zero, since neither an elapsed interval nor
   *          a byte rate can be negative
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
   * An immutable snapshot of a bucket: the tokens it holds, and the instant up to which accrual
   * has been credited.
   *
   * Snapshots are swapped by compare-and-set, which compares references rather than values, so
   * this stays a plain class on purpose: no case class synthetics are needed, no equality is
   * defined, and every instance is distinct.
   *
   * @param tokens tokens currently held, always within [0, capacityBytes]
   * @param lastRefillMillis clock reading, in milliseconds, up to which accrual is credited
   */
  private[streaming] class BucketState(val tokens: Long, val lastRefillMillis: Long)
}
