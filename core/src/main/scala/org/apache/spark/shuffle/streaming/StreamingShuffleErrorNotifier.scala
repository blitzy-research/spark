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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{COUNT, ERROR, NUM_FAILURES, SHUFFLE_ID}
import org.apache.spark.internal.config.SHUFFLE_STREAMING_DEBUG
import org.apache.spark.shuffle.FetchFailedException

/**
 * A first-error-wins bridge that carries a failure observed on an asynchronous I/O thread across
 * to the task thread that is waiting for streaming shuffle input.
 *
 * The streaming shuffle reader consumes blocks delivered by Netty event-loop threads. A decode
 * failure, a checksum mismatch surfaced while framing, or a channel level exception is therefore
 * observed on an I/O thread and not on the task thread. Without a bridge such a failure is simply
 * swallowed and the task blocks forever on input that is never going to arrive, because
 * ShuffleReader offers no lifecycle hook on which a deferred error could be surfaced: its only
 * member is read(), and stop() is deliberately left commented out in that trait.
 *
 * This class is that bridge. Whoever observes a failure calls [[setError]] from whichever thread
 * observed it; the task thread calls [[throwIfError]] on every iterator advance inside read(), so
 * the failure is re-thrown there, where Spark's task-failure machinery can act on it.
 *
 * The policy implemented here has four parts.
 *
 *  - First error wins. The first throwable handed to [[setError]] becomes the permanent root cause
 *    and is never displaced, so the reported failure is always the earliest one, which is the one
 *    that actually explains the cascade.
 *  - A fetch failure is never lost. A [[FetchFailedException]] is the one throwable whose identity
 *    the scheduler acts upon, so it is latched in its own slot in addition to taking part in the
 *    first-error race. See "Fetch failures" below for why that slot exists and what is done with
 *    it.
 *  - Later throwables are attached to the root cause as suppressed exceptions, up to a bounded
 *    and de-duplicated number of them, so a follow-on failure -- a channel close provoked by the
 *    original decode failure, say -- is retained for diagnosis without obscuring the root cause
 *    and without letting retained heap grow with the size of the cascade. Reports beyond the bound
 *    are counted rather than retained; see [[droppedFailureCount]].
 *  - Bookkeeping failures are contained. [[setError]] absorbs any non-fatal problem it hits while
 *    recording, because letting an exception escape a Netty handler callback would tear down the
 *    channel pipeline instead of failing the task cleanly. A fatal `Error` is deliberately left to
 *    propagate: containing one would hide a JVM-level failure the executor must act on.
 *
 * <b>Fetch failures.</b> [[FetchFailedException]] is the sanctioned signal that makes the
 * scheduler recompute an upstream stage, and it is the mechanism by which streaming shuffle
 * recovers from producer loss without any scheduler modification. Two properties of that exception
 * govern the handling here. First, it is a checked exception, so it is not caught by a match on
 * RuntimeException and must be recognised explicitly. Second, its constructor registers itself on
 * the task context, which is what lets the executor report a fetch failure even when user code has
 * wrapped the exception. That registration is thread confined: TaskContext.get() reads a thread
 * local populated only on the task thread, so an exception constructed on a Netty event-loop
 * thread registers nothing at all. This notifier exists precisely because failures are observed
 * on those threads, so it must complete the registration itself, on the task thread, inside
 * [[throwIfError]]. Doing so is what keeps the executor's hidden-fetch-failure path viable and is
 * why the root cause may be re-thrown with its type intact rather than having to be flattened.
 *
 * Bounded retention. The number of failures reported for one stream is not under this executor's
 * control: a peer sending malformed frames produces a fresh throwable per frame, and a wedged
 * channel can produce one per read. Retaining every one of them would grow the heap in proportion
 * to how badly a peer misbehaves, and each retention would take the monitor inside
 * `Throwable.addSuppressed` on a Netty event-loop thread. This class therefore retains at most
 * [[StreamingShuffleErrorNotifier.MaxSuppressedFailures]] failures, and only failures that are
 * distinct -- keyed on throwable class together with a length-capped prefix of the message, so a
 * peer repeating one malformed frame contributes one entry rather than thousands. Everything beyond
 * that is counted in [[droppedFailureCount]] and discarded. The count is never lost: the first drop
 * attaches one marker exception to the root cause, and [[throwIfError]] names the count in the
 * failure it raises. `addSuppressed` is therefore invoked at most
 * `MaxSuppressedFailures + 1` times over the entire life of a notifier, no matter how many failures
 * arrive, which bounds both the retained heap and the total time any I/O thread spends in that
 * monitor.
 *
 * Concurrency: first-error publication is a compare-and-set on an atomic reference, and the rest of
 * the state is atomics plus one concurrent set. This class declares no monitor of its own and does
 * not park, sleep or wait, because [[setError]] runs on Netty event-loop threads. It is lock-free
 * as a system -- a thread that loses a compare-and-set retries against a state another thread has
 * already advanced -- which is not the same as a guarantee that an individual caller cannot be made
 * to retry. Once the retention cap is reached, the reporting path is a volatile read, one atomic
 * increment and a return, with no allocation, no hashing and no monitor. The one monitor reachable
 * from here belongs to the JDK, inside `Throwable.addSuppressed`, and it guards a bounded critical
 * section that never waits on another thread's I/O. Every method is safe to call concurrently, any
 * number of times.
 *
 * Log volume: the two diagnostics this class writes describe bookkeeping rather than failure -- the
 * failure itself is reported by whoever re-throws it -- and they are raised once per reported
 * throwable, which under a cascade is once per channel. They are therefore gated on
 * spark.shuffle.streaming.debug, so that key alone decides whether the streaming subsystem emits
 * verbose diagnostics, and the log budget holds even when the logging framework is globally set to
 * DEBUG. The value is read once and held immutably, because the streaming shuffle has no dynamic
 * reconfiguration.
 *
 * @param shuffleId the shuffle this notifier belongs to, carried into log lines as MDC context
 * @param debugEnabled the value of spark.shuffle.streaming.debug. Use the secondary constructor to
 *                     derive it from a `SparkConf`; it defaults to false so that a caller with no
 *                     configuration to hand still gets the quiet, budget-respecting behaviour
 */
private[spark] class StreamingShuffleErrorNotifier(
    shuffleId: Int,
    debugEnabled: Boolean = false) extends Logging {

  /**
   * Builds a notifier whose verbose diagnostics are governed by the configuration, which is how
   * production code should construct one: the flag is resolved here, once, so that no code on the
   * reporting path ever consults the configuration again.
   *
   * @param shuffleId the shuffle this notifier belongs to
   * @param conf the executor's configuration, read exactly once
   */
  def this(shuffleId: Int, conf: SparkConf) =
    this(shuffleId, conf.get(SHUFFLE_STREAMING_DEBUG))

  /**
   * The first failure reported for the stream, or null while no failure has been reported. A raw
   * null is used in preference to an Option so that the reporting path is a single compareAndSet
   * with no allocation.
   */
  private val firstError = new AtomicReference[Throwable](null)

  /**
   * De-duplication keys for the failures already retained as suppressed exceptions. Membership is
   * only ever added while the retention cap has not been observed as reached, so this set holds at
   * most [[StreamingShuffleErrorNotifier.MaxSuppressedFailures]] keys plus, transiently, one per
   * thread that raced that observation -- a bound set by the size of the I/O thread pool, not by
   * the number of failures a peer can provoke. Each key is itself length-capped, so a peer cannot
   * inflate memory through an enormous exception message either.
   */
  private val suppressedKeys = ConcurrentHashMap.newKeySet[String]()

  /** How many failures are currently retained as suppressed exceptions on the root cause. */
  private val suppressedRetained = new AtomicInteger(0)

  /** How many failures were observed but neither retained nor de-duplicated into a retained one. */
  private val suppressedDropped = new AtomicLong(0L)

  /** Guards the single marker exception that records that dropping has begun. */
  private val overflowMarked = new AtomicBoolean(false)

  /** Guards the single log line that names the retained and dropped failure counts. */
  private val droppedFailuresReported = new AtomicBoolean(false)

  /**
   * The first [[FetchFailedException]] reported for the stream, or null if none has been. This is
   * deliberately a slot of its own rather than a test applied to [[firstError]], because the two
   * answer different questions and the arrival order of concurrent reports is not controllable: a
   * channel-close IOException provoked by producer loss can easily win the first-error race
   * against the fetch failure that explains it. Latching the fetch failure separately means the
   * scheduler-facing signal survives every arrival order, so an upstream stage is recomputed
   * whenever a fetch failure was observed at all, not only when it happened to be observed first.
   */
  private val firstFetchFailure = new AtomicReference[FetchFailedException](null)

  /**
   * Total number of failures reported through [[setError]], including the first. Recorded so that
   * the true size of a cascade remains visible even though only a bounded prefix of it is retained.
   */
  private val reportedFailures = new AtomicLong(0L)

  /**
   * Records a failure observed anywhere on the streaming shuffle path.
   *
   * Safe to call from any thread, any number of times. It does not park or block, and it contains
   * every non-fatal problem it meets while recording, because the callers are Netty event-loop
   * threads: they must not be parked, and a bookkeeping exception must not escape into them. A
   * fatal `Error` is not contained and propagates to the caller.
   *
   * The first call establishes the root cause, and no later call ever displaces it. A later call
   * attaches its throwable to that root cause as a suppressed exception provided the throwable is
   * distinct from the ones already retained and the retention cap has not been reached; otherwise
   * it is counted in [[droppedFailureCount]] and discarded. Either way the root cause itself is
   * left untouched.
   *
   * Independently of the first-error race, a [[FetchFailedException]] is latched in its own slot so
   * that the scheduler-facing signal is preserved whatever order reports arrive in.
   *
   * @param t the failure to record. Passing null is a caller defect, but it is tolerated by
   *          substituting a synthetic cause rather than by dropping the report, since dropping it
   *          would leave the task thread blocked forever on input that will never arrive -- the
   *          exact outcome this class exists to prevent.
   */
  def setError(t: Throwable): Unit = {
    val reported = if (t != null) {
      t
    } else {
      new SparkException(s"Streaming shuffle $shuffleId reported a failure with no throwable")
    }
    reportedFailures.incrementAndGet()
    // Latch the fetch failure before racing for the root cause, so that a fetch failure is
    // recorded as such even if it loses that race to a consequence of the same producer loss.
    reported match {
      case fetchFailed: FetchFailedException =>
        firstFetchFailure.compareAndSet(null, fetchFailed)
      case _ =>
    }
    if (firstError.compareAndSet(null, reported)) {
      if (debugEnabled) {
        logDebug(log"Recorded the first failure for streaming shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)}", reported)
      }
    } else {
      // A later failure never displaces the root cause. The whole block is guarded by NonFatal so
      // that a bookkeeping problem can never escape onto an I/O thread; a fatal error is
      // deliberately left to propagate.
      try {
        val rootCause = firstError.get()
        if (rootCause != null) {
          retainOrCount(rootCause, reported)
        }
      } catch {
        case NonFatal(bookkeepingFailure) =>
          if (debugEnabled) {
            logDebug(log"Could not attach a suppressed failure for streaming shuffle " +
              log"${MDC(SHUFFLE_ID, shuffleId)}: ${MDC(ERROR, bookkeepingFailure)}")
          }
      }
    }
  }

  /**
   * Either retains a later failure as a suppressed exception on the root cause, or counts it and
   * discards it.
   *
   * The order of the tests below is what makes the flood case constant-time. The cheapest and most
   * selective check comes first, so once the cap is reached a report costs one atomic read plus one
   * atomic increment: no key is built, nothing is hashed, and `addSuppressed` is not called, so the
   * monitor inside it is never taken again.
   *
   * A retention slot is reserved with an atomic increment before `addSuppressed` runs, and released
   * if the reservation overshot. Reserving first rather than checking first is what keeps the bound
   * exact when several I/O threads report at the same instant.
   */
  private def retainOrCount(rootCause: Throwable, reported: Throwable): Unit = {
    if (rootCause eq reported) {
      // Throwable.addSuppressed rejects self-suppression with IllegalArgumentException. This is not
      // a dropped failure -- it is the root cause being reported a second time -- so it is not
      // counted either.
    } else if (suppressedRetained.get() >= StreamingShuffleErrorNotifier.MaxSuppressedFailures) {
      countDropped(rootCause)
    } else if (!suppressedKeys.add(suppressionKey(reported))) {
      // An identical failure is already retained, which is the common case under a peer repeating
      // one malformed frame. Counting it keeps the volume visible without keeping the object.
      suppressedDropped.incrementAndGet()
    } else if (suppressedRetained.incrementAndGet() >
        StreamingShuffleErrorNotifier.MaxSuppressedFailures) {
      suppressedRetained.decrementAndGet()
      countDropped(rootCause)
    } else {
      rootCause.addSuppressed(reported)
    }
  }

  /**
   * Counts a failure that will not be retained, and on the very first such failure attaches one
   * marker exception to the root cause so that a reader of the stack trace learns that dropping
   * began at all. The marker is attached exactly once, which is why `addSuppressed` is called at
   * most `MaxSuppressedFailures + 1` times per notifier.
   */
  private def countDropped(rootCause: Throwable): Unit = {
    suppressedDropped.incrementAndGet()
    if (overflowMarked.compareAndSet(false, true)) {
      rootCause.addSuppressed(new SparkException(
        s"Streaming shuffle $shuffleId retained " +
          s"${StreamingShuffleErrorNotifier.MaxSuppressedFailures} distinct failures for " +
          "diagnosis; further failures are counted rather than retained and the total is " +
          "reported with the task failure"))
      // Gated like every other verbose path in this subsystem, so that an operator who leaves the
      // streaming debug key at its default gets no debug output from this notifier at all. The
      // retained total still reaches the task failure through the marker attached above, which is
      // the record that matters and is not conditional on any log setting.
      if (debugEnabled) {
        logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} reached its " +
          log"suppressed-failure retention limit of " +
          log"${MDC(COUNT, StreamingShuffleErrorNotifier.MaxSuppressedFailures)}; further " +
          log"failures will be counted only")
      }
    }
  }

  /**
   * The de-duplication key for a failure: its concrete type together with a length-capped prefix of
   * its message.
   *
   * The type alone would collapse genuinely different problems that happen to share a class, which
   * is common for IOException. The full message would defeat de-duplication whenever a peer
   * varies one number in it, and -- because the message can be built from bytes the peer chose --
   * would let that peer decide how much memory each key occupies. A capped prefix keeps the key
   * both discriminating and bounded.
   */
  private def suppressionKey(t: Throwable): String = {
    val message = t.getMessage
    val bounded = if (message == null) {
      ""
    } else if (message.length > StreamingShuffleErrorNotifier.MaxSuppressionKeyMessageChars) {
      message.substring(0, StreamingShuffleErrorNotifier.MaxSuppressionKeyMessageChars)
    } else {
      message
    }
    t.getClass.getName + ":" + bounded
  }

  /**
   * How many distinct failures are currently retained as suppressed exceptions on the root cause,
   * never more than [[StreamingShuffleErrorNotifier.MaxSuppressedFailures]].
   */
  def suppressedFailureCount: Int = suppressedRetained.get()

  /**
   * How many failures were observed after the first but neither retained nor merged into a retained
   * one, whether because they duplicated a retained failure or because the retention cap was
   * already reached. Reported with the task failure so that the volume of a flood remains visible
   * even though the individual throwables are not kept.
   */
  def droppedFailureCount: Long = suppressedDropped.get()

  /**
   * Whether a failure has been recorded. Non-blocking, and safe to poll from any thread as often
   * as the caller likes; the streaming reader polls it on the hot path between blocks.
   */
  def hasError: Boolean = firstError.get() != null

  /**
   * The recorded root cause, or None while no failure has been recorded. Any failures reported
   * after the first are reachable from the returned throwable through Throwable#getSuppressed.
   */
  def error: Option[Throwable] = Option(firstError.get())

  /**
   * The first [[FetchFailedException]] recorded for the stream, or None if no fetch failure has
   * been reported. Present whether or not the fetch failure won the first-error race, which is
   * what makes the scheduler-facing signal independent of report ordering.
   */
  def fetchFailure: Option[FetchFailedException] = Option(firstFetchFailure.get())

  /**
   * Total number of failures reported through [[setError]], including the first and including those
   * whose throwables were not retained.
   */
  def reportedFailureCount: Long = reportedFailures.get()

  /**
   * Re-throws the recorded failure, if there is one, on the calling thread. The streaming shuffle
   * reader calls this on every iterator advance inside read(), which is what converts an
   * asynchronous I/O failure into an ordinary synchronous task failure. Doing nothing when no
   * failure has been recorded makes the call cheap enough to sit on the hot path.
   *
   * Before anything is thrown, any recorded [[FetchFailedException]] is registered on the calling
   * thread's task context. This completes a registration that the exception's own constructor could
   * not perform, because it ran on an I/O thread where TaskContext.get() is null; see the "Fetch
   * failures" note on this class. The registration is an idempotent assignment, so
   * repeating it on every iterator advance costs nothing and is safe. Its effect is that the
   * executor reports a FetchFailed task-end reason -- and therefore the unmodified scheduler
   * recomputes the upstream stage -- whether the exception that finally propagates is the fetch
   * failure itself or something that merely accompanied it.
   *
   * The original type is then preserved wherever re-throwing it unchanged is safe. A fetch failure
   * is re-thrown exactly as recorded, never wrapped, so that every existing matcher on its type
   * continues to recognise it. An unchecked exception is re-thrown as it stands so that downstream
   * matchers still see its exact type, and an Error is re-thrown as it stands so that a fatal
   * condition is never masked or downgraded. Only a remaining checked exception is wrapped in a
   * SparkException naming the shuffle, with the original attached as the cause.
   */
  def throwIfError(): Unit = {
    val recorded = firstError.get()
    if (recorded != null) {
      // Re-assert the scheduler-facing signal on the task thread. This must happen before the
      // throw and independently of which throwable is about to propagate, because the executor
      // decides between a FetchFailed reason and an ordinary ExceptionFailure by consulting the
      // task context rather than by inspecting the exception it catches.
      val fetchFailed = firstFetchFailure.get()
      if (fetchFailed != null) {
        Option(TaskContext.get()).foreach(_.setFetchFailed(fetchFailed))
      }
      reportDroppedFailuresOnce()
      recorded match {
        // Checked, so it must be matched ahead of the generic checked branch below; wrapping it
        // there would erase the type the scheduler-facing path is built on.
        case producerLoss: FetchFailedException => throw producerLoss
        case unchecked: RuntimeException => throw unchecked
        case fatal: Error => throw fatal
        case checked =>
          throw new SparkException(
            s"Streaming shuffle $shuffleId failed while reading pipelined map output " +
              s"(${checked.getClass.getName})$droppedFailureSuffix", checked)
      }
    }
  }

  /**
   * A parenthetical naming the failures that were counted rather than retained, or the empty string
   * when none were. Appended to the wrapped failure so that the volume of a flood survives into the
   * message even though the individual throwables were discarded.
   */
  private def droppedFailureSuffix: String = {
    val dropped = suppressedDropped.get()
    if (dropped > 0L) {
      s"; $dropped further failure(s) were counted but not retained"
    } else {
      ""
    }
  }

  /**
   * Emits one line naming the retained and dropped failure counts, at most once per notifier.
   *
   * This exists because an unchecked exception and an Error are re-thrown verbatim, so that
   * downstream matchers keep seeing their exact type -- which means the dropped count cannot be
   * folded into their message the way it is folded into a wrapped one. One line per failed stream
   * is emitted instead, which is negligible against the log-volume budget and is not gated behind
   * debug logging, because an operator diagnosing a failed stream needs to know that the stack
   * trace they are reading is a bounded sample rather than the whole story.
   */
  private def reportDroppedFailuresOnce(): Unit = {
    if (suppressedDropped.get() > 0L && droppedFailuresReported.compareAndSet(false, true)) {
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} retained " +
        log"${MDC(COUNT, suppressedRetained.get())} distinct failures for diagnosis and counted " +
        log"${MDC(NUM_FAILURES, suppressedDropped.get())} further failures without retaining them")
    }
  }

  /**
   * Clears the recorded failure, together with all suppression bookkeeping, and returns this
   * notifier to its initial, empty state. This is useful in tests, which reuse a single notifier
   * across cases and need a deterministic starting point. Production code has no reason to call it:
   * a stream that has failed stays failed for the remaining lifetime of the task.
   *
   * The root cause is cleared last, so a concurrent [[setError]] either sees the old root cause and
   * accounts against the old bookkeeping, or sees none and installs itself as the new root cause.
   * Neither outcome can leave a retained failure that the counters do not know about.
   */
  def reset(): Unit = {
    firstFetchFailure.set(null)
    reportedFailures.set(0L)
    suppressedKeys.clear()
    suppressedRetained.set(0)
    suppressedDropped.set(0L)
    overflowMarked.set(false)
    droppedFailuresReported.set(false)
    firstError.set(null)
  }
}

/**
 * Retention limits for the streaming shuffle error bridge.
 *
 * Both values are deliberately fixed rather than configurable. They bound diagnostic retention, not
 * behaviour, so exposing them as configuration would add an operator-facing knob whose only effect
 * is how much of a failure cascade appears in one stack trace -- and it would add a value an
 * operator could raise to reintroduce the unbounded growth these limits exist to prevent.
 */
private[spark] object StreamingShuffleErrorNotifier {

  /**
   * Most distinct failures retained as suppressed exceptions on a single root cause.
   *
   * Eight is chosen because a genuine cascade is short: a decode failure provokes a channel close
   * which provokes a read failure, and a handful of entries covers that comfortably while keeping a
   * printed stack trace readable. A peer able to provoke thousands of failures gains nothing beyond
   * the eighth.
   */
  val MaxSuppressedFailures: Int = 8

  /**
   * Most characters of an exception message included in a de-duplication key.
   *
   * An exception message on this path can be built from bytes a peer chose, so the key length
   * must not be. A prefix of this size still distinguishes genuinely different problems, because
   * the part of a message that names the failure comes first and the part that varies -- an
   * offset, a length, a sequence number -- comes later.
   */
  val MaxSuppressionKeyMessageChars: Int = 256
}
