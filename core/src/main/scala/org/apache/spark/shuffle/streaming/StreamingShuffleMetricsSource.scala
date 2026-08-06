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
 */
private[spark] trait StreamingShuffleBufferUtilizationContributor {

  /**
   * Bytes this owner is currently holding in its streaming shuffle buffers. Must be non-negative;
   * a negative answer is treated as zero rather than allowed to distort the executor-wide sum.
   */
  def contributedBufferedBytes: Long

  /**
   * The buffer budget this owner's bytes are measured against, in bytes. Must be non-negative; an
   * owner reporting a zero budget contributes nothing to the denominator.
   */
  def contributedBudgetBytes: Long
}

/**
 * Dropwizard metric source for the streaming shuffle subsystem.
 *
 * The streaming shuffle path publishes exactly four metrics, which together form its
 * operator-facing telemetry contract:
 *
 *  - `shuffle.streaming.bufferUtilizationPercent` (gauge) -- executor-wide utilisation of the
 *    streaming shuffle buffer budget, as a percentage. A gauge is the correct instrument here
 *    because utilisation is an instantaneous level rather than a cumulative total, and the value
 *    is genuinely computed when the gauge is read: the gauge sums the buffered bytes and the
 *    budgets of every registered [[StreamingShuffleBufferUtilizationContributor]] and divides,
 *    so the write path performs no metric work at all and concurrent shuffles on one executor are
 *    aggregated rather than overwriting one another.
 *  - `shuffle.streaming.spillCount` (counter) -- the number of spill events performed to keep
 *    the buffer budget within its configured threshold. Exactly two kinds of disk write are
 *    counted, because both are the same condition -- the configured buffer percentage was not
 *    enough for what a producer was holding, so bytes went to local disk to keep the bound: an
 *    eviction the budget forced, and a block written straight to disk because the allowance could
 *    not admit it to memory at all. Counting both is what keeps this series consistent with the
 *    volume on `TaskMetrics.diskBytesSpilled`, so that a spill rate and an average spill size are
 *    computable rather than a path producing volume with no event behind it. The one disk write
 *    that is deliberately '''not''' counted is the end-of-stream flush that makes a producer's
 *    retained window durable: that is a routine step of a successful streaming map task rather
 *    than a symptom of pressure, so counting it here would advance this series once per task at a
 *    fraction of a percent of the budget and leave an operator unable to tell a healthy shuffle
 *    from a struggling one, or to measure a spill rate against the configured threshold at all.
 *    That event is reported by `MemorySpillManager.durabilityFlushCount`, which counts non-empty
 *    durability eviction passes, and in the producing task's own summary instead, and its volume
 *    still reaches the standard spill accumulators on
 *    `TaskMetrics`.
 *  - `shuffle.streaming.backpressureEvents` (counter) -- the number of transitions into a
 *    throttled state, whether caused by exhausted consumer credit or by a refused rate-limiter
 *    acquisition.
 *  - `shuffle.streaming.partialReadInvalidations` (counter) -- the number of atomic,
 *    per-producer partial-read invalidations performed after a producer failure.
 *
 * Name composition. This source declares its `sourceName` as "shuffle.streaming" and registers
 * the four metrics under their bare names. `MetricsSystem.buildRegistryName` prefixes a source
 * with the application and executor identifiers only when both are available, so there are two
 * exported forms and an operator may see either:
 *
 *  - `<spark.metrics.namespace or spark.app.id>.<spark.executor.id>.shuffle.streaming.<metric>`
 *    for the driver and executor instances, which are the instances that set both identifiers; and
 *  - the bare fallback `shuffle.streaming.<metric>` when the namespace or the executor id is
 *    absent, which is the case for other instance types such as the standalone master and worker.
 *
 * Either way the "shuffle.streaming" prefix lives in `sourceName` alone and must never be repeated
 * in an individual metric name, which would export a doubled prefix such as
 * `shuffle.streaming.shuffle.streaming.spillCount`.
 *
 * Executor-wide aggregation of buffer utilisation. Utilisation is deliberately not published as a
 * percentage by anyone. A single scalar cell written by each owner in turn would be a
 * last-writer-wins race with an operator-facing consequence rather than a merely cosmetic one: an
 * owner holding almost nothing, or one publishing zero as it closes, would erase the reading of a
 * sibling sitting at the spill threshold, and the gauge would report calm during exactly the
 * condition it exists to expose.
 *
 * What production actually registers is exactly one contributor: the
 * [[MemorySpillManager.ExecutorBufferQuota]] that every spill manager on the executor shares.
 * Registering the shared allowance rather than each spill manager is what makes the reading
 * executor-wide without counting one budget many times, and it withdraws when the last spill
 * manager on the executor closes.
 *
 * ==What the numerator is==
 *
 * The numerator is every byte of streaming shuffle buffer held anywhere on this executor, in
 * '''both directions''', and the denominator is the whole of that one allowance. There is one
 * aggregate quota, not one per direction, because `spark.shuffle.streaming.bufferSizePercent` is a
 * promise about an executor rather than about a role: two independent allowances of the same
 * percentage would sum to twice the configured percentage on any executor doing both at once, which
 * is every executor in a multi-stage job. So the quota charges four ownership categories against
 * one ceiling -- producer framing and buffered blocks, consumer received frames, transient framing
 * copies, and per-stream metadata -- and this gauge reports their total.
 *
 * The read side therefore registers '''nothing of its own''', and that is the reason: a consumer's
 * received bytes are already in this numerator, charged as the consumer category of the same
 * quota. A second contributor for the read side would count those bytes twice and add the same
 * allowance to the denominator a second time, so an executor genuinely at its spill point would
 * read as roughly half of it the moment one reader registered.
 *
 * ==What the range is==
 *
 * Reservation is admission-controlled: the quota refuses any request that would carry the total
 * past the allowance, so no byte counted here was ever granted beyond the ceiling and a reading
 * above 100 cannot arise from admitted reservations. The arithmetic nevertheless applies no clamp
 * -- see [[bufferUtilizationPercent]] -- because a clamp would mask an accounting defect rather
 * than prevent one, and a gauge that reads 100 while an executor is over budget would be reporting
 * calm in exactly the condition it exists to expose.
 *
 * The gauge sums numerators and sums denominators across every registered contributor and divides
 * once, rather than reading a single cell, so the reading is independent of the order owners happen
 * to run in and an owner that finishes withdraws its contribution rather than overwriting
 * everybody else's.
 *
 * Registration. This source is published through the static source list, which `MetricsSystem`
 * registers when it starts -- on the driver and on every executor alike -- provided static sources
 * are enabled. They are enabled by default, and `spark.metrics.staticSources.enabled=false` turns
 * the whole static list off, this source along with the code-generation and Hive catalogue sources.
 * Registering a source is not the same thing as exporting it: reaching an operator additionally
 * requires a sink, and every sink is opt-in through `metrics.properties`. In particular the JMX
 * sink, which is how these four metrics are intended to be read, ships commented out in
 * `conf/metrics.properties.template` and has to be enabled there -- for example
 * `*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink` -- before the metrics appear in an MBean
 * browser. What this subsystem does guarantee is that it needs no metrics agent, sink or UI surface
 * of its own: once a sink an operator already runs is configured, all four metrics travel over it.
 *
 * Telemetry budget. No update is per-record or per-block, and none of them blocks on another
 * thread. The counters are backed by Dropwizard's striped adder, so advancing one costs a single
 * uncontended add, and they are advanced on discrete events only -- a spill occurring, a transition
 * into a throttled state, an invalidation being performed. The gauge costs the write path nothing
 * at all: it is computed on the reporting thread by walking a registry that a buffer owner joins
 * once and leaves once, so its cost is borne by the sink at whatever interval the sink samples.
 * This object declares no monitor of its own and never sleeps; joining and leaving the contributor
 * registry enters the concurrent set's own brief per-bin synchronization, which is off every hot
 * path, so both a task thread and a network event-loop thread may call anything here.
 *
 * As a JVM singleton this object accumulates values for the lifetime of the process, so a caller
 * needing a clean baseline of absolute values must first call `reset()`.
 *
 * There is deliberately no setter for the gauge, for the last-writer-wins reason set out above:
 * `bufferUtilizationPercent` is driven only by registering a contributor. Registration is keyed on
 * the contributor's identity rather than its value, so two owners reporting the same two counts are
 * two contributions and both are summed.
 */
private[spark] object StreamingShuffleMetricsSource extends Source {

  /** Divisor and multiplier of the utilisation percentage. */
  private val PERCENT_SCALE: Long = 100L

  /**
   * The metric namespace for the streaming shuffle subsystem. MetricsSystem places it after the
   * application and executor identifiers, and the bare metric names are appended after it, so
   * this value supplies the shared "shuffle.streaming" prefix exactly once.
   */
  override val sourceName: String = "shuffle.streaming"

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  /**
   * The buffer owners currently holding streaming shuffle memory on this executor.
   *
   * A set rather than a single cell, because an executor runs one owner per concurrently streaming
   * task and the gauge has to describe all of them at once. Membership is what makes a
   * contribution live: an owner adds itself when it starts holding bytes and removes itself when
   * it releases them, so a departing owner subtracts exactly its own contribution and can never
   * zero a peer's. The set is a concurrent one and is only ever added to, removed from and walked,
   * so an owner publishes its contribution without blocking on another thread and the gauge never
   * blocks a producer; joining and leaving enter the set's own brief per-bin synchronization, which
   * happens once per owner rather than on any hot path.
   */
  private val bufferUtilizationContributors =
    ConcurrentHashMap.newKeySet[StreamingShuffleBufferUtilizationContributor]()

  // Dropwizard gauge reporting executor-wide utilisation of the streaming shuffle buffer budget as
  // a percentage. The value is computed when the gauge is read, by aggregating the registered
  // contributions, so the shuffle write path does no metric work and the sampling interval is
  // decided entirely by the reporting sink.
  metricRegistry.register(MetricRegistry.name("bufferUtilizationPercent"),
    new Gauge[Long] {
      override def getValue: Long = bufferUtilizationPercent
    })

  /**
   * Tracks the total number of spill events performed by the streaming shuffle spill manager.
   * Incremented once per spill event, never once per evicted partition and never per record.
   *
   * Both pressure-driven disk writes are counted -- an eviction the budget forced, and a block
   * written straight to disk because the buffer allowance could not admit it -- and the
   * end-of-stream durability flush, which `MemorySpillManager.durabilityFlushCount` reports as
   * non-empty durability eviction passes, is not. See the namespace documentation above for why
   * counting exactly those two, and not the flush, is what gives this series its meaning.
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
   * Adds a buffer owner to the executor-wide utilisation aggregate. Idempotent: registering the
   * same owner twice leaves it counted once, so a defensive second call cannot double count its
   * bytes. A null owner is ignored rather than allowed to poison the registry.
   *
   * An owner should register only once its two contributed values can be answered cheaply and
   * without dereferencing state that may not exist yet, because the gauge reads them from the
   * metrics reporting thread.
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
   * owner's contribution and leaving every other owner's intact. Idempotent, so it is safe on a
   * cleanup path that may run more than once, and safe to call for an owner that never registered.
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
   * The value is not clamped at 100, and in service it does not need to be: the quota behind the
   * only contributor production registers admits no reservation that would carry its total past the
   * allowance, so every byte in the numerator was granted inside the ceiling and an admitted state
   * reads between zero and a hundred. The clamp is omitted so that a contributor which somehow
   * reported bytes it was never granted -- an accounting defect, not an admitted state -- stays
   * visible to operators instead of being masked at exactly 100, which is the whole point of
   * watching this gauge.
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

  /**
   * Resets the values of all metrics to zero. This is useful in tests.
   *
   * Each counter is driven back to zero by decrementing it by its own current count, which is
   * the reset idiom already used by the static metric sources in this codebase. The utilisation
   * registry is emptied as well, so that a caller sees the same state a freshly initialised
   * process would report. Emptying it detaches any owner that is still holding buffers, which is
   * harmless in the tests this method exists for and is the reason it is not called in service.
   */
  def reset(): Unit = {
    METRIC_SPILL_COUNT.dec(METRIC_SPILL_COUNT.getCount())
    METRIC_BACKPRESSURE_EVENTS.dec(METRIC_BACKPRESSURE_EVENTS.getCount())
    METRIC_PARTIAL_READ_INVALIDATIONS.dec(METRIC_PARTIAL_READ_INVALIDATIONS.getCount())
    bufferUtilizationContributors.clear()
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

  /**
   * Adds two byte counts, treating a negative contribution as zero and saturating at
   * `Long.MaxValue` instead of wrapping. Wrapping would turn an implausibly large total into a
   * negative one and thence into a nonsense percentage, so it is ruled out arithmetically rather
   * than assumed away.
   */
  private def saturatingAdd(runningTotal: Long, contribution: Long): Long = {
    val sum = runningTotal + math.max(0L, contribution)
    if (sum < 0L) Long.MaxValue else sum
  }

  /**
   * Expresses `bufferedBytes` as a percentage of `budgetBytes`, without overflowing for any pair
   * of non-negative inputs. The straightforward product is used whenever it is provably safe, and
   * the divisor is scaled down instead for the extreme totals where it is not. The result is
   * reported verbatim and is deliberately not clamped at 100, for the reason given on
   * [[bufferUtilizationPercent]]: admission already keeps an admitted reading inside the range, so
   * a clamp could only ever hide an accounting defect.
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

  // There is deliberately no setter for the utilisation gauge. Buffer utilisation is aggregated
  // exclusively from the registered contributors, because a single shared setter is what allowed
  // one manager to overwrite the executor-wide reading with its own local view. A caller that needs
  // the gauge to read a chosen value registers a contributor reporting that value; production has
  // exactly one such contributor, and any stand-in belongs with the code that needs it rather than
  // shipped here.
}
