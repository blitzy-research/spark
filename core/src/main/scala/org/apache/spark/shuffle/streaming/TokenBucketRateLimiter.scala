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
 * clock. Accrual is measured against the clock's <em>monotonic</em> reading rather than its
 * wall-clock one, and that is a correctness requirement rather than a preference: a wall clock can
 * be stepped by an administrator or slewed by an NTP daemon, and either direction is harmful here.
 * Stepped backwards it freezes accrual for the length of the step, so a producer is paced at zero
 * while the operator's cap says otherwise; stepped forwards it mints a burst nobody earned, so the
 * cap is exceeded by exactly the size of the jump. A monotonic source measures the interval that
 * actually elapsed and is distorted by neither. The injected clock exposes both readings, and the
 * manual clock used in tests derives its monotonic reading from the millisecond time a test sets,
 * so advancing a manual clock still advances accrual exactly.
 *
 * Nanosecond arithmetic is kept overflow-safe by construction rather than by assuming intervals
 * stay small: the elapsed interval is split into whole seconds and a sub-second remainder before it
 * meets the token rate, each product saturates instead of wrapping, and the sum is immediately
 * clamped to the bucket's remaining headroom. A bucket left idle for hours therefore fills, which
 * is right, and never reports a wrapped or negative token count, which is what an unguarded
 * multiplication would eventually do.
 *
 * An oversized request, one asking for more than the whole bucket can ever hold, is refused
 * outright and charged nothing. Admitting it against a full bucket and debiting only the capacity
 * is what would let the surplus bytes travel entirely unpaced, defeating the cap the operator
 * configured. Refusing cannot deadlock a producer either, because the factory in the companion
 * object floors the capacity at one maximum sized encoded frame -- payload plus the framing the
 * protocol adds -- so every legal block fits whole, and a request that does not fit is a framing
 * defect in the caller rather than a bucket the caller must wait out.
 *
 * Instances are cheap, and every method is safe to call concurrently from any number of threads.
 *
 * The rate this class enforces is derived, not declared, and it is not derived here. The operator
 * declares an administered link capacity through spark.shuffle.streaming.maxBandwidthMBps, and
 * [[TokenBucketRateLimiter.ExecutorEgressBudget]] turns that capacity into each limiter's token
 * rate by dividing it across the shuffles the executor is producing for and then holding the result
 * to 80 percent of the declared capacity. A limiter is therefore never built at a rate of its own
 * choosing and never keeps a rate the divisor has moved past. See
 * [[TokenBucketRateLimiter.executorBudget]] for that arithmetic and for the single definition of
 * the configured value that both the configuration entry and this class speak in terms of.
 *
 * @param capacityBytes bucket size in bytes, that is the maximum burst; must be positive
 * @param refillBytesPerSecond token rate in bytes per second; must be positive.
 *                             [[TokenBucketRateLimiter.UNLIMITED_BYTES_PER_SECOND]] selects the
 *                             unlimited fast path, in which every request is admitted at no cost
 * @param clock time source for token accrual, so accrual is a function of this clock alone and no
 *              reading of wall time enters the rate
 * @param debugEnabled the value of spark.shuffle.streaming.debug, read once by the caller and held
 *                     immutably here. It is the sole authority over the one diagnostic this class
 *                     emits, so an operator who leaves that key at its default of false sees
 *                     nothing from the limiter even when logging is globally set to DEBUG
 */
private[spark] class TokenBucketRateLimiter(
    initialCapacityBytes: Long,
    initialRefillBytesPerSecond: Long,
    clock: Clock = new SystemClock,
    debugEnabled: Boolean = false) extends Logging {

  require(initialCapacityBytes > 0L,
    "The streaming shuffle token bucket capacity must be positive but was " +
      s"$initialCapacityBytes.")
  require(initialRefillBytesPerSecond > 0L,
    "The streaming shuffle token bucket refill rate must be positive but was " +
      s"$initialRefillBytesPerSecond bytes/s.")

  /**
   * Whether this limiter enforces no rate at all. Derived once from the sentinel token rate so
   * that the hot path is a single field read rather than a comparison against a bare number.
   *
   * This is immutable even though the rate is not, because a limiter never crosses between the
   * capped and the uncapped world: which one it inhabits follows from whether the operator set a
   * bandwidth cap at all, and that is read once per executor. [[updateRate]] therefore has nothing
   * to do on an unlimited limiter, and the hot path keeps its single boolean read.
   */
  private val unlimited: Boolean =
    initialRefillBytesPerSecond >= TokenBucketRateLimiter.UNLIMITED_BYTES_PER_SECOND

  /**
   * The live state of the bucket: the tokens it holds, the instant up to which accrual has already
   * been credited, and the pacing parameters those two were measured against. Held as an immutable
   * snapshot behind an AtomicReference so that a refill, a charge and a rate change are each
   * published atomically, without taking a lock.
   *
   * The bucket starts full, which lets a producer emit one burst straight away rather than waiting
   * out a refill interval before its very first block.
   */
  private val state: AtomicReference[TokenBucketRateLimiter.BucketState] =
    new AtomicReference(
      new TokenBucketRateLimiter.BucketState(initialCapacityBytes, clock.nanoTime(),
        initialCapacityBytes, initialRefillBytesPerSecond))

  /**
   * Requests refused because they asked for more than the bucket could ever hold. Counted rather
   * than merely logged, so that a caller framing blocks larger than the protocol permits is
   * visible to a test and to an operator instead of being inferred from a stalled producer.
   */
  private val oversizedRequests = new AtomicLong(0L)

  /** Latch ensuring the oversized-request warning is emitted at most once per limiter. */
  private val oversizedLogged = new AtomicBoolean(false)

  // At most one line is logged per limiter, at construction, and nothing is ever logged per
  // acquisition attempt: a limiter is consulted once per outbound block, so a per attempt line
  // would exhaust the per executor log budget within seconds. Even that single line is gated on
  // spark.shuffle.streaming.debug, so it is that key -- and not the logging framework's global
  // level -- that decides whether this subsystem emits verbose diagnostics at all.
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

  /**
   * The bucket size in force right now. Read from the live snapshot rather than from a constructor
   * field, so it stays truthful after the executor's egress budget has been redistributed.
   */
  def capacityBytes: Long = state.get().capacityBytes

  /**
   * The token rate in force right now. Read from the live snapshot for the same reason as
   * [[capacityBytes]].
   */
  def refillBytesPerSecond: Long = state.get().refillBytesPerSecond

  /**
   * The largest request this limiter can ever admit, which a caller frames to rather than discovers
   * by refusal.
   *
   * A request for more than the whole bucket can never be satisfied however long the caller waits,
   * so [[tryAcquire]] refuses it outright instead of admitting it against a full bucket and
   * debiting only the capacity. Publishing the ceiling is what turns that refusal from a mystery
   * into a framing rule: a caller sizing its blocks by this figure is never refused for size, and a
   * caller that is refused knows exactly which figure it exceeded. An unlimited limiter has no
   * ceiling at all.
   *
   * Read from the live snapshot, so it stays truthful after the executor's egress budget has been
   * redistributed and the bucket resized.
   */
  def maxAcquirableBytes: Long = if (unlimited) Long.MaxValue else capacityBytes

  /**
   * Number of requests refused for asking more than the bucket's whole capacity. A non-zero value
   * means a caller is framing blocks larger than the streaming protocol permits, since the bucket
   * is always sized to hold at least one maximum-sized encoded frame.
   */
  def oversizedRequestCount: Long = oversizedRequests.get()

  /**
   * Attempts to charge `bytes` tokens against the bucket.
   *
   * Does not park or block the calling thread, so it is safe to call from a Netty event-loop
   * thread. It raises nothing of its own; the only throwable it can surface comes from the injected
   * `Clock`. A false result is flow control rather than failure: the caller should keep the data it
   * was about to send and try again later, which is exactly the pressure the spill manager and the
   * backpressure protocol react to.
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
      refill(state.get(), clock.nanoTime()).tokens
    }
  }

  /**
   * How long until this bucket holds `bytes` tokens, in milliseconds, or zero if it already does.
   *
   * A caller that has been refused needs to know when to come back, and computing that here is the
   * difference between resuming as soon as pacing permits and resuming whenever some unrelated
   * event happens to provoke another attempt. The figure is derived from the shortfall and the
   * refill rate, so it is exactly the wait the bucket's own arithmetic implies rather than a
   * guessed interval, and it is rounded up so that the caller never wakes a fraction of a
   * millisecond early and is refused a second time for it.
   *
   * A request larger than the bucket can ever hold reports [[Long.MaxValue]]: no amount of waiting
   * would satisfy it, and reporting a finite wait would invite an endless retry. An unlimited
   * limiter reports zero, because nothing is ever withheld.
   *
   * This is an observer: like [[availableTokens]] it computes the refill a [[tryAcquire]] would
   * see and publishes nothing, so polling it can never perturb pacing.
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
      if (bytes > observed.capacityBytes) {
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
    }
  }

  /**
   * Whether this limiter admits everything at no cost. Read by [[StreamingShuffleWriter]] to skip
   * its pacing bookkeeping entirely -- the retry loop, the deferral accounting and the diagnostic
   * -- when egress is uncapped, which is the default configuration.
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
    // Refills to the capacity currently in force rather than the one this limiter was constructed
    // with, so resetting after a redistribution does not silently reinstate an old rate.
    val observed = state.get()
    state.set(new TokenBucketRateLimiter.BucketState(observed.capacityBytes, clock.nanoTime(),
      observed.capacityBytes, observed.refillBytesPerSecond))
  }

  /**
   * The lock-free refill-and-charge loop behind [[tryAcquire]] for a bounded bucket.
   *
   * Two acquirers can never jointly consume more than the bucket holds, because every charge is
   * published by a compare-and-set from the exact snapshot the charge was computed against; a
   * racing thread invalidates that snapshot and forces a recomputation.
   */
  private def chargeBucket(bytes: Long): Boolean = {
    var acquired = false
    var attempting = true
    while (attempting) {
      val observed = state.get()
      if (bytes > observed.capacityBytes) {
        // A request larger than the whole bucket is refused outright, and refused without charging
        // anything. The alternative -- admitting it against a full bucket and debiting only the
        // capacity -- is what let the surplus bytes travel entirely unpaced, defeating the cap the
        // operator configured. Refusal is safe here rather than a livelock risk, because every
        // bucket is sized to hold at least one maximum-sized encoded frame
        // ([[TokenBucketRateLimiter.burstCapacityBytes]]), so a legal block always fits and a
        // request that does not fit is a framing defect in the caller, reported as such.
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
          // The full requested amount is debited. `refreshed.tokens >= bytes` was just established,
          // so the result cannot go negative and no clamping is needed -- and none is wanted, since
          // clamping is precisely how bytes escaped being charged for.
          val next = new TokenBucketRateLimiter.BucketState(refreshed.tokens - bytes,
            refreshed.lastRefillNanos, refreshed.capacityBytes, refreshed.refillBytesPerSecond)
          if (state.compareAndSet(observed, next)) {
            acquired = true
            attempting = false
          }
          // A failed compare-and-set means another acquirer published first, so re-read and retry.
          // The bucket as a whole always advances, because every failure implies another thread
          // succeeded; an individual caller can still be made to retry under contention.
        }
      }
    }
    acquired
  }

  /**
   * Records a request that asked for more than the bucket could ever hold.
   *
   * The count is always incremented; the log line is emitted at most once per limiter, because a
   * caller that frames one oversized block almost certainly frames every block that way, and a line
   * per attempt would exhaust the per-executor log budget in seconds.
   */
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
   * This exists because the per-shuffle share of the operator's bandwidth cap is
   * `maxBandwidthMBps / numConcurrentShuffles`, and that divisor changes as shuffles start and
   * finish on the executor. A rate fixed at construction is therefore wrong the moment a second
   * shuffle begins: the first limiter would go on pacing itself against the whole cap while the
   * second paced itself against half of it, so their aggregate would exceed the cap the operator
   * set. [[TokenBucketRateLimiter.ExecutorEgressBudget]] closes that gap by calling this on every
   * live limiter whenever membership changes.
   *
   * Accrual earned under the previous rate is credited first, before the new rate is published, so
   * that tokens a producer has already waited for are honoured rather than silently re-scaled.
   * Tokens are then clamped to the new capacity, which is the same surplus-discard rule the refill
   * path applies: a bucket that shrinks cannot keep holding more than it can hold.
   *
   * Does not park or block, and raises nothing of its own. An unlimited limiter has no rate to
   * redistribute, so the call is a no-op there and reports `false`.
   *
   * @param newRefillBytesPerSecond the token rate to adopt; must be positive
   * @return true if a new rate was published, false if this limiter is unlimited, the rate was
   *         non-positive, or the bucket already paced at exactly this rate
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
          // Already pacing at exactly this rate. Publishing an identical snapshot would only add
          // compare-and-set traffic and reset nothing, so the redistribution pass costs nothing
          // for the limiters whose share did not move.
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
   * Pure: it returns a new snapshot, or `observed` itself when nothing changed, and never touches
   * the shared state. Two guards keep it honest. An elapsed interval that is zero or negative mints
   * nothing and never rewinds the accrual point -- negative is not expected from a monotonic
   * source, but the platform guarantee is "always increasing" rather than "provably monotonic", so
   * the guard stays. An interval too short to earn a whole token likewise leaves the accrual point
   * alone, so the fraction is carried into the next call instead of being discarded over and over;
   * at nanosecond resolution that carry matters for every rate an operator can configure.
   *
   * @param observed the snapshot to advance
   * @param nowNanos the current monotonic time in nanoseconds, as reported by the injected clock
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
        // The deficit form of the surplus-discard rule. Comparing the minted tokens against the
        // remaining headroom, rather than adding first and clamping afterwards, means the addition
        // can never overflow, while clamping to the capacity is exactly the classic rule that
        // tokens arriving at a full bucket are thrown away.
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

/**
 * Constants, arithmetic and factories for [[TokenBucketRateLimiter]].
 *
 * This object is where configuration becomes arithmetic. The factory reads
 * spark.shuffle.streaming.maxBandwidthMBps exactly once and hands the derived numbers to a limiter
 * that holds them immutably, which is what makes "configuration changes require an executor
 * restart" true by construction rather than by convention: no path exists by which a running
 * limiter can observe a changed value, so there is no dynamic reconfiguration to design.
 *
 * What the configured value means. spark.shuffle.streaming.maxBandwidthMBps declares the
 * ADMINISTERED LINK CAPACITY of the executor, not the rate streaming is allowed to reach. That one
 * definition is stated identically by the configuration entry itself, so an operator reading either
 * place is told the same thing. Administered means the operator declared figure and nothing else:
 * this subsystem probes neither the operating system nor the network for a real capacity, so
 * none is invented here.
 *
 * Two formulas turn that capacity into a token rate, and both are fixed by contract.
 *
 *  - The per shuffle share of the declared capacity is maxBandwidthMBps * 1 MiB divided by the
 *    number of concurrent shuffles. That divisor comes from the streaming shuffle coordinator,
 *    which keeps the executor scoped registry of active shuffles, because no existing Spark API
 *    reports how many shuffles an executor is currently serving.
 *  - The share is then held to 80 percent, which is streaming shuffle's ceiling on the declared
 *    capacity. Applying the ceiling to the share rather than to the capacity is what makes the
 *    ceiling bite, because the aggregate of N concurrent limiters is then 80 percent of the
 *    declared capacity, which leaves exactly the headroom the fallback policy relies on when it
 *    watches for link saturation.
 *
 * Composing the two gives the effective refill rate of a single shuffle, which is the number an
 * operator should expect to observe:
 *
 * {{{
 *   refillBytesPerSecond = (0.8 * maxBandwidthMBps * 1 MiB) / numConcurrentShuffles
 * }}}
 *
 * An unset link capacity means unlimited, never zero. The distinction is load bearing: reading
 * absence as zero would silently stop all egress, so absence is routed to a limiter whose fast path
 * admits everything at no cost.
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

  /**
   * Share of the administered link capacity, as declared by
   * spark.shuffle.streaming.maxBandwidthMBps, that streaming shuffle egress may occupy in
   * aggregate across every shuffle an executor is serving. The remaining 20 percent is the headroom
   * the fallback policy watches, which is why the ceiling is part of the contract rather than a
   * tuning knob and why no configuration key exposes it.
   */
  val BANDWIDTH_CEILING_PERCENT: Long = 80L

  /** Divisor that turns a percentage into a fraction with integer arithmetic. */
  val PERCENT_SCALE: Long = 100L

  /** Bytes in one MiB, the unit in which the operator declares the bandwidth cap. */
  val BYTES_PER_MIB: Long = 1024L * 1024L

  /**
   * Largest number of bytes the streaming wire protocol will ever put on the network for one block,
   * framing included, which is also the floor for a bucket's capacity.
   *
   * The value is read from the protocol's own data block message rather than restated here, and
   * that matters for correctness rather than tidiness. A bucket paces network bytes, so its
   * capacity must be able to hold the largest complete <em>encoded frame</em> -- payload plus the
   * thirty bytes of framing the protocol adds. A floor of the payload cap alone would leave the
   * largest legal frame unrepresentable, and a bucket that cannot represent a request can only
   * refuse it forever or admit it while charging for part of it. Importing the constant costs
   * nothing: core already declares `spark-network-shuffle` as a compile dependency, exactly as
   * `IndexShuffleBlockResolver` relies on when it imports from the same module.
   */
  val MAX_ENCODED_FRAME_BYTES: Long = DataBlockMessage.MAX_ENCODED_FRAME_BYTES.toLong

  /** Seconds of sustained throughput a bucket may bank as burst allowance. */
  val BURST_SECONDS: Long = 1L

  /** Milliseconds in one second; accrual is computed in milliseconds. */
  val MILLIS_PER_SECOND: Long = 1000L

  /**
   * Nanoseconds in one second, the divisor that turns a monotonic interval into earned tokens.
   *
   * Accrual is measured in nanoseconds because that is the unit the clock's monotonic reading comes
   * in, and converting it down to milliseconds first would throw away every sub-millisecond
   * interval -- which, on a path consulted once per outbound block, is most of them.
   */
  val NANOS_PER_SECOND: Long = 1000000000L

  /**
   * Builds the executor's egress budget, out of which each shuffle's limiter is handed.
   *
   * The configured value is the administered link capacity, so the token rate each of this budget's
   * limiters paces at is
   * `(BANDWIDTH_CEILING_PERCENT / 100) * maxBandwidthMBps * 1 MiB / numConcurrentShuffles` bytes
   * per second -- the composition of [[perShuffleBytesPerSecond]] and [[applyLinkCapacityCeiling]],
   * in that order. An operator who declares a 1000 MB/s link and is running two shuffles should
   * therefore expect each to refill at 400 MB/s and the pair to occupy 800 MB/s, which is the 80
   * percent ceiling holding across the executor rather than per stream.
   *
   * A budget rather than a limiter is what this returns, and that is the whole point: the divisor
   * is a property of the executor, so no single limiter can own it. [[ExecutorEgressBudget]] hands
   * out one limiter per shuffle and republishes every live limiter's share whenever the divisor
   * moves, which is what makes the aggregate obey the cap instead of each limiter obeying it alone.
   *
   * The link capacity is an optional configuration entry, and its absence is the unlimited state:
   * absence is deliberately not encoded as zero or as a negative sentinel, so it is carried as an
   * absent option into the budget, which routes it to [[unlimited]].
   *
   * @param conf the executor's configuration; the cap and the debug gate are read once here and
   *             never consulted again, which is what makes an executor restart the only way to
   *             change either
   * @param clock time source handed to every limiter the budget creates, so accrual across the
   *              whole budget advances from one reading rather than from wall time
   * @param concurrencySource asks the coordinator how many streaming shuffles this executor is
   *                          producing for, answering `None` when the question cannot be put
   * @return a budget that paces to the declared capacity, or one that paces nothing when no
   *         capacity is declared
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
   * This is the shape an operator gets by leaving the link capacity unset, and it is also the
   * shape a caller should ask for when it wants the pacing layer present but inert.
   *
   * @param clock time source handed to the limiter; unused on the acquisition path
   * @param debugEnabled the value of spark.shuffle.streaming.debug, threaded through so that the
   *                     limiter's single construction diagnostic stays governed by that key alone
   */
  def unlimited(
      clock: Clock = new SystemClock,
      debugEnabled: Boolean = false): TokenBucketRateLimiter = {
    new TokenBucketRateLimiter(
      UNLIMITED_BYTES_PER_SECOND, UNLIMITED_BYTES_PER_SECOND, clock, debugEnabled)
  }

  /**
   * Splits the operator's declared link capacity evenly across the shuffles this executor is
   * serving. This is the first of the two steps [[apply]] composes, and on its own it does NOT
   * apply the 80 percent ceiling; [[applyLinkCapacityCeiling]] does that to the result.
   *
   * @param maxBandwidthMBps the administered link capacity in MB/s; a non-positive value is floored
   *                         at one MB/s so that a directly constructed limiter can still make
   *                         progress, even though the configuration entry itself rejects such a
   *                         value
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
   * Holds a rate to [[BANDWIDTH_CEILING_PERCENT]] of itself, which is the second of the two steps
   * [[apply]] composes. Its input is a per shuffle share of the administered link capacity, so the
   * output is that share of the 80 percent streaming shuffle is permitted to occupy.
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
   * The allowance is one second of sustained throughput, floored at one maximum sized encoded
   * frame. That floor is a liveness and accounting guarantee rather than a tuning choice: a bucket
   * smaller than a single legal frame could neither admit that frame nor charge for all of it, so
   * it would either stall the producer permanently or let the surplus bytes travel uncharged.
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
   * Whole tokens earned by `elapsedNanos` of accrual at `bytesPerSecond`.
   *
   * The interval is split into whole seconds and a sub-second remainder before either meets the
   * rate, which is what keeps the arithmetic exact over the entire range a caller can present. The
   * naive form -- multiply the whole nanosecond interval by the rate and then divide -- overflows a
   * `Long` after roughly ninety seconds at a hundred megabytes per second, and a bucket that has
   * simply been idle while its shuffle waited on a slow consumer can easily be idle for longer than
   * that. Split, the remainder term is bounded by one second of nanoseconds and the seconds term is
   * a small number, so both stay in range for every rate derivable from the operator's MB/s
   * setting.
   *
   * Both products saturate rather than wrap, so an interval or rate beyond even that range yields
   * `Long.MaxValue` -- which the caller immediately clamps to the bucket's headroom, meaning the
   * extreme case fills the bucket. That is the correct answer for a long idle period, and it is
   * reached without the caller having to reason about overflow at all.
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

  /**
   * `a + b` for non-negative operands, saturating at `Long.MaxValue` instead of wrapping.
   *
   * Used only where a wrapped sum would become a small or negative token count and therefore a
   * silently wrong pacing decision; the saturated value is always clamped by the caller.
   */
  def saturatingAdd(a: Long, b: Long): Long = {
    val sum = a + b
    if (sum < 0L) Long.MaxValue else sum
  }

  /**
   * An immutable snapshot of a bucket: the tokens it holds, and the instant up to which accrual
   * has been credited.
   *
   * Snapshots are swapped by compare-and-set, which compares references rather than values, so
   * this stays a plain class on purpose: no case class synthetics are needed, no equality is
   * defined, and every instance is distinct.
   *
   * The pacing parameters travel inside the snapshot rather than beside it, in fields of the
   * limiter. That is what makes a rate change safe on a live bucket: redistributing the executor's
   * egress budget publishes a new capacity and a new token rate through the very same
   * compare-and-set that publishes a charge, so an acquirer can never observe a half-applied
   * change -- tokens minted at one rate but measured against another capacity, say. Holding them in
   * mutable fields would make that impossible to guarantee without a lock on the hot path.
   *
   * @param tokens tokens currently held, always within [0, capacityBytes]
   * @param lastRefillNanos monotonic clock reading, in nanoseconds, up to which accrual is
   *                        credited. Monotonic rather than wall-clock, because an interval is the
   *                        only quantity accrual needs and it is the one quantity a clock
   *                        adjustment must not be able to distort
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
   * <b>Why this exists.</b> The contract fixes the per-shuffle token rate at
   * `maxBandwidthMBps / numConcurrentShuffles`. A limiter that reads that divisor once, when it is
   * built, is correct only until the executor starts producing for a second shuffle: the first
   * limiter goes on pacing against the whole cap while the second paces against half of it, so the
   * two together admit one and a half times the cap -- and after the eighty percent link-capacity
   * ceiling is applied to each, a hundred and twenty percent of it. Every additional shuffle widens
   * the overrun. Owning the limiters collectively, and republishing every live limiter's share
   * whenever membership changes, is what makes the aggregate obey the cap rather than each limiter
   * obeying it in isolation.
   *
   * <b>The divisor.</b> Two views of concurrency exist and neither alone is trustworthy. This
   * executor knows how many limiters it holds, which is authoritative for shuffles it has already
   * begun serving but lags a registration still in flight. The coordinator knows how many shuffles
   * the executor has registered producers for -- `numConcurrentShufflesFor(executorId)`, the
   * executor-scoped count, not the cluster-wide one -- which lags a shuffle that has been retired
   * locally but not yet reaped. The larger of the two is used, because over-estimating concurrency
   * paces conservatively and can only keep the aggregate under the cap, whereas under-estimating it
   * is exactly the overrun this class exists to prevent.
   *
   * <b>Cost.</b> Redistribution takes a short lock, but membership changes once per shuffle rather
   * than once per block, and [[TokenBucketRateLimiter.tryAcquire]] never touches this class at all:
   * a producer holds its limiter directly and the redistribution reaches it through a
   * compare-and-set. The Netty egress path therefore stays lock-free.
   *
   * A single instance belongs to the executor's streaming shuffle manager, which reads the cap once
   * at construction. That is what keeps "configuration changes require an executor restart" true by
   * construction: no path exists by which a running budget can observe a changed cap.
   *
   * @param maxBandwidthMBps the operator's declared egress cap in MB/s, or None for unlimited.
   *                         Absence means unlimited, never zero
   * @param clock time source handed to every limiter this budget creates, injected for determinism
   * @param concurrencySource asks the coordinator how many streaming shuffles this executor is
   *                          currently producing for, answering `None` when the question cannot be
   *                          put. Supplied as a function rather than as an endpoint reference so
   *                          that no transport type reaches this class, and so the question can be
   *                          answered without one
   */
  class ExecutorEgressBudget(
      maxBandwidthMBps: Option[Int],
      clock: Clock = new SystemClock,
      debugEnabled: Boolean = false,
      concurrencySource: () => Option[Int] = () => None)
    extends Logging {

    /** Guards the registry and the redistribution pass. Never held across a blocking call. */
    private val lock = new Object

    /** Live limiters by shuffle id. Mutated only under [[lock]]. */
    private val limiters = new mutable.HashMap[Int, TokenBucketRateLimiter]()

    /** Most recent executor-scoped concurrency the coordinator reported. Guarded by [[lock]]. */
    private var reportedConcurrency = 0

    /**
     * The limiter pacing one shuffle's egress on this executor, creating it on first use.
     *
     * Admitting a shuffle changes the divisor, so every live limiter is republished before the
     * caller receives its own. The returned limiter is therefore already pacing at the share that
     * accounts for it.
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
     * Idempotent: releasing a shuffle that holds no limiter changes nothing, so a manager may call
     * this from `unregisterShuffle` without tracking whether a limiter was ever created.
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
        // Outside the lock, because the question may cross the network. The removal's verdict is
        // what this method reports; whether the refresh moved the divisor is the refresh's
        // business.
        refreshConcurrency()
      }
      removed
    }

    /**
     * Re-reads the coordinator's view of this executor's concurrency and republishes the shares if
     * it moved the divisor.
     *
     * <b>Why a release is the moment that needs this and an admission is not.</b> The divisor is
     * the larger of the local limiter count and the count the coordinator last reported, so a
     * reported value can only ever be too high, never too low -- and a value that is too high paces
     * every live limiter below its true share for as long as it is believed. An admission already
     * carries a fresh count: the reply to a producer registration reports it, and the caller feeds
     * it straight in through [[observeConcurrency]], so asking again there would spend a round trip
     * on a number the executor already has. A release carries nothing. Dropping the third of three
     * limiters leaves the local count at two and the reported count at three, and nothing else
     * would ever correct it until the executor happened to register another producer.
     *
     * The question is put outside the redistribution lock, because it may cross the network and
     * this class documents that its lock is never held across a blocking call. Nothing is lost by
     * answering late: the divisor stays conservatively high until the answer arrives.
     *
     * @return true if the divisor moved and limiters were republished
     */
    def refreshConcurrency(): Boolean = {
      // Read outside the lock. A failure to reach the coordinator is answered with None by the
      // source itself, and None leaves the divisor exactly as it was.
      concurrencySource().exists(observeConcurrency)
    }

    /**
     * Records the executor-scoped concurrency the coordinator reports, redistributing if it moves
     * the divisor.
     *
     * The reply to a producer registration carries this value, so a producer feeds it straight in
     * and the executor's pacing tracks the coordinator's view without a second round trip. A stale
     * high reading only paces more conservatively, so it is safe to apply immediately.
     *
     * @param numConcurrentShuffles concurrency reported for this executor; non-positive is ignored
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
     * The aggregate of all live limiters is this multiplied by the number of them, which is
     * what a test asserts against the operator's cap.
     */
    def currentShareBytesPerSecond: Option[Long] = lock.synchronized {
      maxBandwidthMBps.map(currentShare)
    }

    /**
     * Drops every limiter and forgets the reported concurrency, returning the budget to the state a
     * freshly constructed one would report.
     *
     * Called at executor shutdown, once both transports are closed, so that nothing is retired
     * while a live channel could still be charging against it. Also what lets a suite reuse one
     * budget across cases.
     */
    def reset(): Unit = lock.synchronized {
      limiters.clear()
      reportedConcurrency = 0
    }

    /** Divisor of the cap. Callers must hold [[lock]]. */
    private def currentDivisor: Int = math.max(1, math.max(limiters.size, reportedConcurrency))

    /** Per-shuffle paced share for a given cap. Callers must hold [[lock]]. */
    private def currentShare(cap: Int): Long = {
      applyLinkCapacityCeiling(
        TokenBucketRateLimiter.perShuffleBytesPerSecond(cap, currentDivisor))
    }

    /** Builds a limiter at the share currently in force. Callers must hold [[lock]]. */
    private def newLimiter(): TokenBucketRateLimiter = maxBandwidthMBps match {
      case Some(cap) =>
        val share = currentShare(cap)
        new TokenBucketRateLimiter(burstCapacityBytes(share), share, clock, debugEnabled)
      case None =>
        unlimited(clock, debugEnabled)
    }

    /**
     * Republishes every live limiter at the share the current divisor implies. Callers must hold
     * [[lock]].
     *
     * Uncapped egress needs no redistribution, because an unlimited limiter enforces no rate and
     * [[TokenBucketRateLimiter.updateRate]] is a no-op on it. Limiters already pacing at the new
     * share are left untouched by that same call, so a pass that changes nothing publishes nothing.
     */
    private def redistribute(): Unit = {
      maxBandwidthMBps.foreach { cap =>
        val share = currentShare(cap)
        var republished = 0
        limiters.valuesIterator.foreach { limiter =>
          if (limiter.updateRate(share)) {
            republished += 1
          }
        }
        // Gated like every other verbose path in the subsystem: a redistribution happens on
        // every registration and departure, so an ungated line here would scale with task churn.
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
