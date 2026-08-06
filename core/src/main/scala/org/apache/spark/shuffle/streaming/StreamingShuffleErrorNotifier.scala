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
 * A first-error-wins bridge that carries a failure observed on an asynchronous I/O thread across to
 * the task thread that is waiting for streaming shuffle input. Whoever observes a failure calls
 * [[setError]] from whichever thread observed it, and the task thread calls [[throwIfError]] on
 * every iterator advance inside `read()`, which is where the failure becomes an ordinary task
 * failure. Without the bridge the failure is swallowed and the task waits forever, because
 * `ShuffleReader` offers no lifecycle hook on which a deferred error could be surfaced.
 *
 * The earliest failure reported is the permanent diagnostic root and is never displaced: it is the
 * one that explains the cascade, and later failures are attached to it as suppressed exceptions, up
 * to a bounded and de-duplicated number, the rest being counted rather than retained.
 *
 * Propagation precedence differs from that, deliberately. A recorded [[FetchFailedException]] is
 * what propagates whenever one exists, even when it lost the first-error race to a consequence of
 * the producer loss it describes, because it is the one type the scheduler and every matcher act
 * upon; the diagnostic root is then attached to it as a suppressed exception so nothing is lost. An
 * `Error` is never displaced by it, since a fatal condition must not be downgraded to a recoverable
 * one.
 *
 * [[setError]] absorbs any non-fatal problem it hits while recording, because an exception escaping
 * a Netty callback tears down the channel pipeline instead of failing the task cleanly. A fatal
 * `Error` is left to propagate.
 *
 * @param shuffleId the shuffle this notifier belongs to, carried into log lines as MDC context
 * @param debugEnabled the value of spark.shuffle.streaming.debug
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

  /** The first failure reported for the stream, or null while no failure has been reported. */
  private val firstError = new AtomicReference[Throwable](null)

  /** De-duplication keys for the failures already retained as suppressed exceptions. */
  private val suppressedKeys = ConcurrentHashMap.newKeySet[String]()

  /** How many failures are currently retained as suppressed exceptions on the root cause. */
  private val suppressedRetained = new AtomicInteger(0)

  /** How many failures were observed but neither retained nor de-duplicated into a retained one. */
  private val suppressedDropped = new AtomicLong(0L)

  private val overflowMarked = new AtomicBoolean(false)

  /** Guards the single log line that names the retained and dropped failure counts. */
  private val droppedFailuresReported = new AtomicBoolean(false)

  /** The first [[FetchFailedException]] reported for the stream, or null if none has been. */
  private val firstFetchFailure = new AtomicReference[FetchFailedException](null)

  /** Guards the single attachment of the root cause onto a preferred fetch failure. */
  private val rootCauseAttached = new AtomicBoolean(false)

  /** Total number of failures reported through [[setError]], including the first. */
  private val reportedFailures = new AtomicLong(0L)

  /**
   * Records a failure observed anywhere on the streaming shuffle path.
   *
   * @param t the failure to record.
   */
  def setError(t: Throwable): Unit = {
    val reported = if (t != null) {
      t
    } else {
      new SparkException(s"Streaming shuffle $shuffleId reported a failure with no throwable")
    }
    reportedFailures.incrementAndGet()
    // Latch the fetch failure before racing for the root cause, so that a fetch failure is recorded
    // as such even if it loses that race to a consequence of the same producer loss.
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
      // A later failure never displaces the root cause.
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
   */
  private def retainOrCount(rootCause: Throwable, reported: Throwable): Unit = {
    if (rootCause eq reported) {
      // Throwable.addSuppressed rejects self-suppression with IllegalArgumentException.
    } else if (suppressedRetained.get() >= StreamingShuffleErrorNotifier.MaxSuppressedFailures) {
      countDropped(rootCause)
    } else if (!suppressedKeys.add(suppressionKey(reported))) {
      // An identical failure is already retained, which is the common case under a peer repeating
      // one malformed frame.
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
   * began at all.
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
      // streaming debug key at its default gets no debug output from this notifier at all.
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
   * already reached.
   */
  def droppedFailureCount: Long = suppressedDropped.get()

  /** Whether a failure has been recorded. */
  def hasError: Boolean = firstError.get() != null

  /** The recorded root cause, or None while no failure has been recorded. */
  def error: Option[Throwable] = Option(firstError.get())

  /**
   * The first [[FetchFailedException]] recorded for the stream, or None if no fetch failure has
   * been reported.
   */
  def fetchFailure: Option[FetchFailedException] = Option(firstFetchFailure.get())

  /**
   * Total number of failures reported through [[setError]], including the first and including those
   * whose throwables were not retained.
   */
  def reportedFailureCount: Long = reportedFailures.get()

  /**
   * Re-throws the recorded failure, if there is one, on the calling thread, which is what turns an
   * asynchronous I/O failure into an ordinary synchronous task failure.
   *
   * Any recorded [[FetchFailedException]] is registered on the calling thread's task context first.
   * The exception's own constructor could not do it: registration reads a thread local that only
   * the task thread has, so an exception constructed on an I/O thread registers nothing. The
   * assignment is idempotent, so repeating it on every iterator advance costs nothing.
   */
  def throwIfError(): Unit = {
    val recorded = firstError.get()
    if (recorded != null) {
      // Re-assert the scheduler-facing signal on the task thread.
      val fetchFailed = firstFetchFailure.get()
      if (fetchFailed != null) {
        Option(TaskContext.get()).foreach(_.setFetchFailed(fetchFailed))
      }
      reportDroppedFailuresOnce()
      recorded match {
        // Matched first, and ahead of the fetch-failure preference below: a fatal condition is
        // never displaced by a recoverable one.
        case fatal: Error => throw fatal
        case rootCause if fetchFailed != null =>
          attachRootCauseOnce(fetchFailed, rootCause)
          throw fetchFailed
        case unchecked: RuntimeException => throw unchecked
        case checked =>
          throw new SparkException(
            s"Streaming shuffle $shuffleId failed while reading pipelined map output: " +
              s"${StreamingShuffleErrorNotifier.describe(checked)}$droppedFailureSuffix", checked)
      }
    }
  }

  /** Attaches the root cause to a preferred fetch failure, once, and never fatally. */
  private def attachRootCauseOnce(fetchFailed: FetchFailedException, rootCause: Throwable): Unit = {
    if ((rootCause ne fetchFailed) && rootCauseAttached.compareAndSet(false, true)) {
      try {
        fetchFailed.addSuppressed(rootCause)
      } catch {
        case NonFatal(bookkeepingFailure) =>
          if (debugEnabled) {
            logDebug(log"Could not attach the root cause to the fetch failure of streaming " +
              log"shuffle ${MDC(SHUFFLE_ID, shuffleId)}: ${MDC(ERROR, bookkeepingFailure)}")
          }
      }
    }
  }

  /**
   * A parenthetical naming the failures that were counted rather than retained, or the empty string
   * when none were.
   */
  private def droppedFailureSuffix: String = {
    val dropped = suppressedDropped.get()
    if (dropped > 0L) {
      s"; $dropped further failure(s) were counted but not retained"
    } else {
      ""
    }
  }

  /** Emits one line naming the retained and dropped failure counts, at most once per notifier. */
  private def reportDroppedFailuresOnce(): Unit = {
    if (suppressedDropped.get() > 0L && droppedFailuresReported.compareAndSet(false, true)) {
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} retained " +
        log"${MDC(COUNT, suppressedRetained.get())} distinct failures for diagnosis and counted " +
        log"${MDC(NUM_FAILURES, suppressedDropped.get())} further failures without retaining them")
    }
  }

  /**
   * Clears the recorded failure, together with all suppression bookkeeping, and returns this
   * notifier to its initial, empty state.
   */
  def reset(): Unit = {
    firstFetchFailure.set(null)
    reportedFailures.set(0L)
    suppressedKeys.clear()
    suppressedRetained.set(0)
    suppressedDropped.set(0L)
    overflowMarked.set(false)
    droppedFailuresReported.set(false)
    rootCauseAttached.set(false)
    firstError.set(null)
  }
}

/** Retention limits for the streaming shuffle error bridge. */
private[spark] object StreamingShuffleErrorNotifier {

  /** Most distinct failures retained as suppressed exceptions on a single root cause. */
  val MaxSuppressedFailures: Int = 8

  /** Most characters of an exception message included in a de-duplication key. */
  val MaxSuppressionKeyMessageChars: Int = 256

  /**
   * The operator-facing rendering of a failure: its own message, or its type when it has none.
   *
   * @param cause the failure to render; a null is rendered as an explicit absence rather than
   *     as the word "null", because a report that reads "null" tells an operator nothing
   */
  def describe(cause: Throwable): String = {
    if (cause == null) {
      "no cause was reported"
    } else {
      val message = cause.getMessage
      if (message != null && message.trim.nonEmpty) message.trim else cause.getClass.getName
    }
  }
}
