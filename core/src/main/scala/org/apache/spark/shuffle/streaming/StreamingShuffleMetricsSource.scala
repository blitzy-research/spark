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

import com.codahale.metrics.{Counter, Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

/** One live owner of streaming shuffle buffer memory, as seen by the buffer-utilisation gauge. */
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
private[spark] object StreamingShuffleMetricsSource extends Source {

  private val PERCENT_SCALE: Long = 100L

  /** The metric namespace for the streaming shuffle subsystem. */
  override val sourceName: String = "shuffle.streaming"

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  /** The buffer owners currently holding streaming shuffle memory on this executor. */
  private val bufferUtilizationContributors =
    ConcurrentHashMap.newKeySet[StreamingShuffleBufferUtilizationContributor]()

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
   */
  def bufferUtilizationPercent: Long = {
    var bufferedBytes = 0L
    var budgetBytes = 0L
    val contributors = bufferUtilizationContributors.iterator()
    while (contributors.hasNext) {
      val contributor = contributors.next()
      bufferedBytes = saturatingAdd(bufferedBytes, contributor.contributedBufferedBytes)
      budgetBytes = saturatingAdd(budgetBytes, contributor.contributedBudgetBytes)
    }
    percentOf(bufferedBytes, budgetBytes)
  }

  /** Resets the values of all metrics to zero. */
  def reset(): Unit = {
    METRIC_SPILL_COUNT.dec(METRIC_SPILL_COUNT.getCount())
    METRIC_BACKPRESSURE_EVENTS.dec(METRIC_BACKPRESSURE_EVENTS.getCount())
    METRIC_PARTIAL_READ_INVALIDATIONS.dec(METRIC_PARTIAL_READ_INVALIDATIONS.getCount())
    bufferUtilizationContributors.clear()
  }

  // The three increment helpers below ignore non-positive arguments.

  // clients can use these to avoid classloader issues with the codahale classes
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
