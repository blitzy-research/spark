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

import com.codahale.metrics.{Counter, Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

/**
 * Dropwizard metric source for the streaming shuffle subsystem.
 *
 * The streaming shuffle path publishes exactly four metrics, which together form its
 * operator-facing telemetry contract:
 *
 *  - `shuffle.streaming.bufferUtilizationPercent` (gauge) -- the utilisation of the streaming
 *    shuffle buffer budget, as a percentage. A gauge is the correct instrument here because
 *    utilisation is an instantaneous level rather than a cumulative total, and the value is
 *    computed on read so that the producer write path never pays for a metric computation.
 *  - `shuffle.streaming.spillCount` (counter) -- the number of spill events performed to keep
 *    the buffer budget within its configured threshold.
 *  - `shuffle.streaming.backpressureEvents` (counter) -- the number of transitions into a
 *    throttled state, whether caused by exhausted consumer credit or by a refused rate-limiter
 *    acquisition.
 *  - `shuffle.streaming.partialReadInvalidations` (counter) -- the number of atomic,
 *    per-producer partial-read invalidations performed after a producer failure.
 *
 * Name composition. This source declares its `sourceName` as "shuffle.streaming" and registers
 * the four metrics under their bare names. MetricsSystem prefixes a source with the application
 * and executor identifiers, and Dropwizard then appends the per-metric name, so the exported
 * names are `<application id>.<executor id>.shuffle.streaming.<metric name>`. The
 * "shuffle.streaming" prefix therefore lives in `sourceName` alone and must never be repeated
 * in an individual metric name, which would export a doubled prefix such as
 * `shuffle.streaming.shuffle.streaming.spillCount`.
 *
 * Registration. This source is published through the static source list that MetricsSystem
 * registers when it starts, on the driver and on every executor alike. Because registration is
 * automatic, the sinks that are already configured -- notably the JMX sink -- export all four
 * metrics with no additional configuration, and the subsystem needs no bespoke metrics agent,
 * sink or UI surface of its own.
 *
 * Telemetry budget. Every update is lock-free and none of them is per-record. The counters are
 * backed by Dropwizard's striped adder and the gauge by a single atomic cell, so publishing a
 * value costs one uncontended add or one plain store. Counters are advanced on discrete events
 * only -- a spill occurring, a transition into a throttled state, an invalidation being
 * performed -- and never once per record or once per block, while the gauge is sampled by the
 * reporting sink rather than recomputed by the writer. That is how the sub-1% CPU telemetry
 * budget is met. This object holds no lock, performs no blocking call and never sleeps, so it
 * is equally safe to call from a task thread and from a network event-loop thread.
 *
 * As a JVM singleton this object accumulates values for the lifetime of the process. Callers
 * that need a clean baseline, such as tests asserting on absolute metric values, must first
 * call `reset()`.
 */
private[spark] object StreamingShuffleMetricsSource extends Source {

  /**
   * The metric namespace for the streaming shuffle subsystem. MetricsSystem places it after the
   * application and executor identifiers, and the bare metric names are appended after it, so
   * this value supplies the shared "shuffle.streaming" prefix exactly once.
   */
  override val sourceName: String = "shuffle.streaming"

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  /**
   * Holder for the most recently published buffer utilisation percentage. An atomic cell keeps
   * publication lock-free on the producer side and reduces the gauge itself to a plain read,
   * which is what allows the gauge to be evaluated on the reporting thread rather than on the
   * shuffle write path. Declared before the gauge below so that the initialisation order of
   * this object is unambiguous.
   */
  private val bufferUtilization: AtomicLong = new AtomicLong(0L)

  // Dropwizard gauge reporting the utilisation of the streaming shuffle buffer budget as a
  // percentage. The value is computed on read: the gauge returns whatever the owning component
  // last published through setBufferUtilizationPercent, so the write path does no metric work
  // and the sampling interval is decided entirely by the reporting sink.
  // Values are expected in the range [0, 100]. The published value is reported verbatim and is
  // deliberately neither clamped nor coerced, so that a momentarily over-budget allocation
  // stays visible to operators instead of being masked at 100, and so that a negative
  // out-of-band sentinel remains available to the publisher.
  metricRegistry.register(MetricRegistry.name("bufferUtilizationPercent"),
    new Gauge[Long] {
      override def getValue: Long = bufferUtilization.get()
    })

  /**
   * Tracks the total number of spill events performed by the streaming shuffle spill manager.
   * Incremented once per spill event, never once per evicted partition and never per record.
   */
  val METRIC_SPILL_COUNT: Counter = metricRegistry.counter(MetricRegistry.name("spillCount"))

  /**
   * Tracks the total number of transitions into a throttled state observed by the backpressure
   * protocol. Incremented once per transition, so a stream that stays throttled is counted
   * once rather than continuously.
   */
  val METRIC_BACKPRESSURE_EVENTS: Counter =
    metricRegistry.counter(MetricRegistry.name("backpressureEvents"))

  /**
   * Tracks the total number of partial-read invalidations performed by the streaming shuffle
   * reader. Incremented once per atomic, per-producer invalidation, not once per discarded
   * block, so the count reports failed producers rather than discarded bytes.
   */
  val METRIC_PARTIAL_READ_INVALIDATIONS: Counter =
    metricRegistry.counter(MetricRegistry.name("partialReadInvalidations"))

  /**
   * Resets the values of all metrics to zero. This is useful in tests.
   *
   * Each counter is driven back to zero by decrementing it by its own current count, which is
   * the reset idiom already used by the static metric sources in this codebase. The buffer
   * utilisation gauge is returned to its initial value as well, so that a caller sees the same
   * state a freshly initialised process would report.
   */
  def reset(): Unit = {
    METRIC_SPILL_COUNT.dec(METRIC_SPILL_COUNT.getCount())
    METRIC_BACKPRESSURE_EVENTS.dec(METRIC_BACKPRESSURE_EVENTS.getCount())
    METRIC_PARTIAL_READ_INVALIDATIONS.dec(METRIC_PARTIAL_READ_INVALIDATIONS.getCount())
    bufferUtilization.set(0L)
  }

  // The three increment helpers below ignore non-positive arguments. These are monotonic event
  // counters, so a negative increment would silently run an operator-facing metric backwards,
  // and a zero increment is pure overhead; rejecting both keeps the exported series monotonic
  // without costing a legitimate caller anything. reset() drives the Dropwizard handles
  // directly rather than going through these helpers, so it is unaffected by the guard.

  // clients can use these to avoid classloader issues with the codahale classes
  def incrementSpillCount(n: Long): Unit = if (n > 0L) METRIC_SPILL_COUNT.inc(n)
  def incrementBackpressureEvents(n: Long): Unit = if (n > 0L) METRIC_BACKPRESSURE_EVENTS.inc(n)
  def incrementPartialReadInvalidations(n: Long): Unit =
    if (n > 0L) METRIC_PARTIAL_READ_INVALIDATIONS.inc(n)
  def setBufferUtilizationPercent(v: Long): Unit = bufferUtilization.set(v)
}
