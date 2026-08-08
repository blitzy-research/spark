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
import java.util.concurrent.atomic.AtomicLong

import scala.util.control.NonFatal

import com.codahale.metrics.{Counter, Gauge, MetricRegistry}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CLASS_NAME, ERROR}
import org.apache.spark.metrics.source.Source

/**
 * One live owner of streaming shuffle buffer memory, as seen by the buffer-utilisation gauge.
 *
 * Utilisation is an executor-wide quantity that no single owner can compute on its own, so an owner
 * contributes only the two numbers it actually knows -- how many bytes it is holding, and the
 * budget those bytes are measured against -- and the gauge sums the live contributions and derives
 * the percentage itself. That division of labour is what makes the exported value describe the
 * whole executor rather than whichever owner happened to publish last, and it is what lets an owner
 * leave without disturbing the contribution of an owner that is still holding buffers.
 *
 * Both methods are read from the metrics reporting thread, so an implementation must answer
 * without blocking: no lock that a producer can hold across I/O, no allocation proportional to the
 * buffered data, and no dereference of state that may not exist yet. An implementation that cannot
 * answer cheaply should register itself only once it can.
 *
 * Neither method may raise, and an implementation that does is not trusted to be the exception:
 * the gauge leaves such an owner out of the reading entirely -- both of its numbers, not just the
 * one that failed -- counts the omission on
 * `StreamingShuffleMetricsSource.unreadableContributorReadCount` and reports the first of them, so
 * that one defective owner degrades its own contribution rather than the executor's reading or the
 * reporting pass of every configured sink.
 */
private[spark] trait StreamingShuffleBufferUtilizationContributor {

  /** Bytes this owner is currently holding in its streaming shuffle buffers. */
  def contributedBufferedBytes: Long

  /** The buffer budget this owner's bytes are measured against, in bytes. */
  def contributedBudgetBytes: Long
}

/**
 * Dropwizard metric source for the streaming shuffle subsystem, publishing exactly four metrics
 * under the `shuffle.streaming` namespace: `bufferUtilizationPercent` (gauge),
 * `spillCount`, `backpressureEvents` and `partialReadInvalidations` (counters).
 *
 * Utilisation is a gauge computed when it is read, by summing the buffered bytes and budgets of the
 * registered [[StreamingShuffleBufferUtilizationContributor]]s, so concurrent shuffles on one
 * executor aggregate instead of overwriting one another and the write path performs no metric work.
 * The namespace lives in `sourceName` alone and must never be repeated in a metric name, which
 * would export a doubled prefix.
 */
private[spark] object StreamingShuffleMetricsSource extends Source with Logging {

  private val PERCENT_SCALE: Long = 100L

  /** The metric namespace for the streaming shuffle subsystem. */
  override val sourceName: String = "shuffle.streaming"

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  /** The buffer owners currently holding streaming shuffle memory on this executor. */
  private val bufferUtilizationContributors =
    ConcurrentHashMap.newKeySet[StreamingShuffleBufferUtilizationContributor]()

  /**
   * Contributor reads the gauge had to abandon because the owner raised something.
   *
   * Kept as an internal reading rather than a fifth metric: the exported set is exactly the four
   * metrics the telemetry contract names, and this condition is a defect in an owner rather than a
   * property of the shuffle. It exists so that a skipped contribution is countable and assertable
   * instead of silent, and so the one log line the gauge emits about it is emitted exactly once.
   */
  private val unreadableContributorReads = new AtomicLong(0L)

  // Dropwizard gauge reporting executor-wide utilisation of the streaming shuffle buffer budget as
  // a percentage.
  metricRegistry.register(MetricRegistry.name("bufferUtilizationPercent"),
    new Gauge[Long] {
      override def getValue: Long = bufferUtilizationPercent
    })

  /** Tracks the total number of spill events performed by the streaming shuffle spill manager. */
  val METRIC_SPILL_COUNT: Counter = metricRegistry.counter(MetricRegistry.name("spillCount"))

  /**
   * Tracks the total number of transitions into a throttled state observed by the backpressure
   * protocol.
   */
  val METRIC_BACKPRESSURE_EVENTS: Counter =
    metricRegistry.counter(MetricRegistry.name("backpressureEvents"))

  /**
   * Tracks the total number of partial-read invalidations performed by the streaming shuffle
   * reader.
   */
  val METRIC_PARTIAL_READ_INVALIDATIONS: Counter =
    metricRegistry.counter(MetricRegistry.name("partialReadInvalidations"))

  /**
   * Adds a buffer owner to the executor-wide utilisation aggregate.
   *
   * @param contributor the buffer owner to start counting
   */
  def registerBufferUtilizationContributor(
      contributor: StreamingShuffleBufferUtilizationContributor): Unit = {
    if (contributor != null) {
      bufferUtilizationContributors.add(contributor)
    }
  }

  /**
   * Removes a buffer owner from the executor-wide utilisation aggregate, subtracting exactly that
   * owner's contribution and leaving every other owner's intact.
   *
   * @param contributor the buffer owner to stop counting
   */
  def unregisterBufferUtilizationContributor(
      contributor: StreamingShuffleBufferUtilizationContributor): Unit = {
    if (contributor != null) {
      bufferUtilizationContributors.remove(contributor)
    }
  }

  /** How many buffer owners are currently counted, for diagnostics and assertions. */
  def bufferUtilizationContributorCount: Int = bufferUtilizationContributors.size()

  /**
   * Executor-wide buffer utilisation as a percentage of the aggregate budget, computed here rather
   * than cached, which is exactly what the `bufferUtilizationPercent` gauge reports.
   *
   * The buffered bytes and the budgets of every registered owner are summed independently and the
   * percentage is taken from the two totals, so a busy shuffle and an idle one on the same
   * executor produce one honest executor-wide figure instead of two competing ones. Both sums
   * saturate rather than wrap, a negative contribution is read as zero, and an empty registry or a
   * zero total budget answers zero, so the gauge is total: there is no input for which it throws
   * and no sentinel value it can return in place of a percentage.
   *
   * The value is not clamped at 100. A momentarily over-budget allocation stays visible to
   * operators instead of being masked, which is the whole point of watching this gauge.
   *
   * An owner that cannot answer is skipped rather than allowed to fail the read, which is what
   * makes the totality claim above true of any registry and not merely of a well-behaved one. The
   * trait asks implementations to answer cheaply and without blocking, and the one contributor
   * production registers does exactly that; but this method is called from the metrics reporting
   * thread on behalf of every sink, so one defective owner must not be able to take the whole
   * executor's utilisation reading -- or the sink's whole reporting pass -- down with it.
   */
  def bufferUtilizationPercent: Long = {
    var bufferedBytes = 0L
    var budgetBytes = 0L
    val contributors = bufferUtilizationContributors.iterator()
    while (contributors.hasNext) {
      val contributor = contributors.next()
      try {
        // Both answers are taken before either is added, so an owner that answers its buffered
        // bytes and then fails on its budget contributes NEITHER. Adding as we went would have left
        // a numerator standing against no denominator of its own, which does not merely lose that
        // owner's contribution: it inflates the executor-wide percentage by measuring its bytes
        // against everyone else's budget.
        val ownerBufferedBytes = contributor.contributedBufferedBytes
        val ownerBudgetBytes = contributor.contributedBudgetBytes
        bufferedBytes = saturatingAdd(bufferedBytes, ownerBufferedBytes)
        budgetBytes = saturatingAdd(budgetBytes, ownerBudgetBytes)
      } catch {
        case NonFatal(e) =>
          reportUnreadableContributor(contributor, e)
      }
    }
    percentOf(bufferedBytes, budgetBytes)
  }

  /**
   * Counts one abandoned contributor read and reports the first of them.
   *
   * Reported once and only once for the life of the process, because the gauge is read on every
   * reporting pass of every configured sink: a line per read would turn one defective owner into a
   * log flood measured in the executor's log budget, and the second occurrence tells an operator
   * nothing the first did not. The running count remains available through
   * [[unreadableContributorReadCount]] for anyone who needs to know it kept happening.
   *
   * @param contributor the owner that could not be read
   * @param failure what it raised
   */
  private def reportUnreadableContributor(
      contributor: StreamingShuffleBufferUtilizationContributor,
      failure: Throwable): Unit = {
    if (unreadableContributorReads.incrementAndGet() == 1L) {
      logWarning(log"A streaming shuffle buffer utilisation contributor of type " +
        log"${MDC(CLASS_NAME, contributor.getClass.getName)} could not be read and was left out " +
        log"of the executor-wide reading: ${MDC(ERROR, failure)}. The gauge reports the owners " +
        log"that could be read; further occurrences are counted but not logged")
    }
  }

  /**
   * How many contributor reads the gauge has abandoned, for diagnostics and assertions. Zero on a
   * healthy executor, and the reading to consult if `bufferUtilizationPercent` looks lower than the
   * bytes an operator believes are held.
   */
  def unreadableContributorReadCount: Long = unreadableContributorReads.get()

  /**
   * Resets the values of all metrics to zero. This is useful in tests.
   *
   * Each counter is driven back to zero by decrementing it by its own current count, which is
   * the reset idiom already used by the static metric sources in this codebase. The utilisation
   * registry is emptied as well, so that a caller sees the same state a freshly initialised
   * process would report. Emptying it detaches any owner that is still holding buffers, which is
   * harmless in the tests this method exists for and is the reason it is not called in service.
   *
   * The abandoned-read count is cleared for the same reason the counters are: it is part of the
   * state a freshly initialised process would report, and a caller establishing a clean baseline
   * must not inherit an earlier caller's defective owner.
   */
  def reset(): Unit = {
    METRIC_SPILL_COUNT.dec(METRIC_SPILL_COUNT.getCount())
    METRIC_BACKPRESSURE_EVENTS.dec(METRIC_BACKPRESSURE_EVENTS.getCount())
    METRIC_PARTIAL_READ_INVALIDATIONS.dec(METRIC_PARTIAL_READ_INVALIDATIONS.getCount())
    bufferUtilizationContributors.clear()
    unreadableContributorReads.set(0L)
  }

  // The three increment helpers below ignore non-positive arguments.

  // Each counter is reached through a named helper rather than by the subsystem touching the
  // registry itself, so every call site names the event it is counting, no call site can invent a
  // metric name, and a decrement is impossible: these three counters are lifetime running totals.
  def incrementSpillCount(n: Long): Unit = if (n > 0L) METRIC_SPILL_COUNT.inc(n)
  def incrementBackpressureEvents(n: Long): Unit = if (n > 0L) METRIC_BACKPRESSURE_EVENTS.inc(n)
  def incrementPartialReadInvalidations(n: Long): Unit =
    if (n > 0L) METRIC_PARTIAL_READ_INVALIDATIONS.inc(n)

  /**
   * Adds two byte counts, treating a negative contribution as zero and saturating at
   * `Long.MaxValue` instead of wrapping.
   */
  private def saturatingAdd(runningTotal: Long, contribution: Long): Long = {
    val sum = runningTotal + math.max(0L, contribution)
    if (sum < 0L) Long.MaxValue else sum
  }

  /**
   * Expresses `bufferedBytes` as a percentage of `budgetBytes`, without overflowing for any pair of
   * non-negative inputs.
   */
  private def percentOf(bufferedBytes: Long, budgetBytes: Long): Long = {
    if (bufferedBytes <= 0L || budgetBytes <= 0L) {
      0L
    } else if (bufferedBytes <= Long.MaxValue / PERCENT_SCALE) {
      bufferedBytes * PERCENT_SCALE / budgetBytes
    } else {
      bufferedBytes / math.max(1L, budgetBytes / PERCENT_SCALE)
    }
  }

  // There is deliberately no setter for the utilisation gauge.
}
