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

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{ERROR, SHUFFLE_ID}

/**
 * A lock-free, first-error-wins bridge that carries a failure observed on an asynchronous I/O
 * thread across to the task thread that is waiting for streaming shuffle input.
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
 * The policy implemented here has three parts.
 *
 *  - First error wins. The first throwable handed to [[setError]] becomes the permanent root cause
 *    and is never displaced, so the reported failure is always the earliest one, which is the one
 *    that actually explains the cascade.
 *  - Later throwables are attached to that root cause as suppressed exceptions, so a follow-on
 *    failure -- a channel close provoked by the original decode failure, say -- is retained for
 *    diagnosis without obscuring the root cause.
 *  - Bookkeeping never throws. [[setError]] absorbs any non-fatal problem it hits while recording,
 *    because letting an exception escape a Netty handler callback would tear down the channel
 *    pipeline instead of failing the task cleanly.
 *
 * Concurrency: the entire state of this class is a single AtomicReference, mutated only through
 * compareAndSet and set. It holds no lock, contains no synchronized block, and performs no sleep
 * and no blocking wait, because [[setError]] runs on Netty event-loop threads that must never
 * park. The one monitor reachable from here belongs to the JDK, inside Throwable.addSuppressed,
 * and it guards a bounded critical section that never waits on another thread's I/O. Every method
 * is safe to call concurrently, any number of times.
 *
 * @param shuffleId the shuffle this notifier belongs to, carried into log lines as MDC context
 */
private[spark] class StreamingShuffleErrorNotifier(shuffleId: Int) extends Logging {

  /**
   * The whole state of this notifier: the first failure reported for the stream, or null while no
   * failure has been reported. A raw null is used in preference to an Option so that the reporting
   * path is a single compareAndSet with no allocation.
   */
  private val firstError = new AtomicReference[Throwable](null)

  /**
   * Records a failure observed anywhere on the streaming shuffle path.
   *
   * Safe to call from any thread, any number of times, and guaranteed both never to block and
   * never to throw. Both guarantees are load bearing, because the callers are Netty event-loop
   * threads: they must not be parked, and an exception must not escape into them.
   *
   * The first call establishes the root cause. Every later call attaches its throwable to that
   * root cause as a suppressed exception, leaving the root cause itself untouched.
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
    if (firstError.compareAndSet(null, reported)) {
      logDebug(log"Recorded the first failure for streaming shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)}", reported)
    } else {
      // A later failure never displaces the root cause; attaching it as suppressed keeps the whole
      // cascade visible in the eventual stack trace. Throwable.addSuppressed rejects
      // self-suppression with IllegalArgumentException, so that case is screened out explicitly,
      // and the block is guarded by NonFatal so that a bookkeeping problem can never escape onto
      // an I/O thread. A fatal error is deliberately left to propagate.
      try {
        val rootCause = firstError.get()
        if (rootCause != null && (rootCause ne reported)) {
          rootCause.addSuppressed(reported)
        }
      } catch {
        case NonFatal(bookkeepingFailure) =>
          logDebug(log"Could not attach a suppressed failure for streaming shuffle " +
            log"${MDC(SHUFFLE_ID, shuffleId)}: ${MDC(ERROR, bookkeepingFailure)}")
      }
    }
  }

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
   * Re-throws the recorded failure, if there is one, on the calling thread. The streaming shuffle
   * reader calls this on every iterator advance inside read(), which is what converts an
   * asynchronous I/O failure into an ordinary synchronous task failure. Doing nothing when no
   * failure has been recorded makes the call cheap enough to sit on the hot path.
   *
   * The original type is preserved wherever re-throwing it unchanged is safe. An unchecked
   * exception is re-thrown as it stands so that downstream matchers still see its exact type, and
   * an Error is re-thrown as it stands so that a fatal condition is never masked or downgraded.
   * Anything else is wrapped in a SparkException naming the shuffle, with the original attached as
   * the cause. Wrapping is safe for the scheduler-facing case: FetchFailedException registers
   * itself on the TaskContext when it is constructed (SPARK-19276), so the executor still reports
   * a fetch failure and the upstream stage is still recomputed even when the exception it sees has
   * been wrapped.
   */
  def throwIfError(): Unit = {
    val recorded = firstError.get()
    if (recorded != null) {
      recorded match {
        case unchecked: RuntimeException => throw unchecked
        case fatal: Error => throw fatal
        case checked =>
          throw new SparkException(
            s"Streaming shuffle $shuffleId failed while reading pipelined map output " +
              s"(${checked.getClass.getName})", checked)
      }
    }
  }

  /**
   * Clears the recorded failure and returns this notifier to its initial, empty state. This is
   * useful in tests, which reuse a single notifier across cases and need a deterministic starting
   * point. Production code has no reason to call it: a stream that has failed stays failed for
   * the remaining lifetime of the task.
   */
  def reset(): Unit = firstError.set(null)
}
