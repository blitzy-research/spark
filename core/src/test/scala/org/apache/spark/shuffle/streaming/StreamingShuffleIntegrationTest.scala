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

import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.{FetchFailed, LocalSparkContext, ShuffleDependency, SparkConf, SparkContext, SparkEnv, SparkFunSuite, TaskContext, TaskFailedReason, TestUtils}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_ENABLED, SHUFFLE_STREAMING_SPILL_THRESHOLD, STAGE_MAX_CONSECUTIVE_ATTEMPTS, TASK_MAX_FAILURES}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.util.{LongAccumulator, ManualClock}

/**
 * End-to-end integration tests for the streaming shuffle, run across REAL separate executor JVMs.
 *
 * ==Why a cluster master and not `local[n]`==
 *
 * Every context UNDER TEST is started on `local-cluster[2,1,1024]`. That is not a stylistic
 * preference: in local mode the executor deliberately skips shuffle manager initialisation, because
 * the driver's instance already exists, so ONE `StreamingShuffleManager` serves both the producer
 * and the consumer role and the coordinator can only ever take the register path. A cluster master
 * gives two genuine executor JVMs, each of which builds a manager of its own and must resolve the
 * driver's coordinator endpoint by name -- the driver-registers / executor-resolves rendezvous,
 * real cross-JVM block transfer, and real cross-executor failure injection. `SharedSparkContext`
 * cannot serve any of this because it hardcodes its master, so `LocalSparkContext` is the only
 * route.
 *
 * The throwaway sort-based BASELINE contexts are a separate matter, and their master is chosen per
 * scenario for a stated reason. The first scenario's baseline runs on the same cluster master,
 * because that scenario compares elapsed time and two runs on different masters would be measuring
 * the master. Every other baseline runs on a local master, because those scenarios compare only the
 * output set, which the dataset fixes and the master cannot change; running them locally keeps the
 * suite inside its budget without weakening a single assertion.
 *
 * ==The five scenarios, and the one assertion they share==
 *
 * The feature names five integration scenarios and this suite carries all five: a one hundred
 * mebibyte, ten partition shuffle measured against the sort-based baseline; a producer failure
 * mid-shuffle; a consumer held at fifty percent of the producer's rate until spill triggers; a
 * network partition that fires the five second producer timeout and stands streaming down; and five
 * concurrent shuffles arbitrating for one executor's buffer allowance.
 *
 * Every one of them ends in the same assertion, because it is the property that makes the whole
 * feature safe: '''EVERY PATH TERMINATES IN A WORKING SHUFFLE.''' No configuration, no failure and
 * no resource condition may leave a job without a functioning shuffle implementation. So each
 * scenario asserts that its job COMPLETES and that its output is exactly what stock sort-based
 * shuffle produces from the same input -- compared as a SET, because a shuffle promises nothing
 * about ordering within a partition and comparing sequences would fail for a reason that is not a
 * defect.
 *
 * ==Determinism, which is a hard requirement here and not an aspiration==
 *
 * This suite must be runnable repeatedly with zero flakiness, and that is met by construction
 * rather than by retrying. The retrying variant of `test` that the base suite offers is
 * deliberately not used anywhere: retrying a case hides the order dependence and the timing
 * assumption that the gate exists to find. Concretely:
 *
 *  - Nothing sleeps. Elapsed time is an advance of an injected [[ManualClock]], so the five second
 *    producer timeout is asserted at exactly 4999 ms and exactly 5000 ms rather than somewhere
 *    near five seconds, and waits on background work go through the shared bounded barriers.
 *  - Nothing is random unless it is seeded, and the dataset is generated on the executors from
 *    partition and record indices, so a failure can be replayed rather than chased.
 *  - The injected faults are conditioned on the task attempt number, so exactly one attempt of
 *    exactly one map partition fails, and its retry always succeeds.
 *  - [[StreamingShuffleMetricsSource]] is a JVM singleton whose counters outlive a case, so
 *    `beforeEach` returns it, and the process-scoped buffer allowance, to a known state.
 *
 * ==What is asserted about latency, and what is not==
 *
 * The feature's target is a thirty to fifty percent latency reduction. That figure is an
 * '''acceptance target measured by `StreamingShufflePerformanceBenchmark`''', not a continuous
 * integration gate: this repository asserts no performance threshold anywhere, and a suite that
 * did would fail on a loaded machine for a reason that is not a defect. So the first scenario
 * measures both paths, reports the observed reduction against the named target window, and asserts
 * what can be asserted without a timing assumption -- that both runs completed, that the
 * measurement is well formed, and that the two outputs are identical.
 *
 * Each case is comfortably inside the twenty minute default per-test timeout that
 * `SparkFunSuite` imposes, including the hundred mebibyte case and the five concurrent shuffles.
 */
class StreamingShuffleIntegrationTest
  extends SparkFunSuite
  with LocalSparkContext
  with StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  /** Two real executor JVMs, one core and one gibibyte each. */
  private val ClusterMaster: String = "local-cluster[2,1,1024]"

  /** Executors the cluster master promises, waited for so scheduling is never a race. */
  private val ExecutorCount: Int = 2

  private val ExecutorStartupTimeoutMillis: Long = 60000L

  private val ListenerDrainTimeoutMillis: Long = 60000L

  /** The shuffle width the feature names: ten reduce partitions. */
  private val PartitionCount: Int = DefaultPartitionCount

  /** The dataset the feature names: one hundred mebibytes. */
  private val LargeDatasetBytes: Long = TargetDatasetBytes

  /**
   * A smaller dataset for the failure scenarios, whose subject is recovery rather than volume.
   *
   * Two mebibytes over ten partitions is roughly eight hundred records per partition, which is
   * what makes a crash injected part way through a partition a genuine MID-write failure: records
   * have been framed and streamed before the producer dies, and the iterator is nowhere near
   * exhausted.
   */
  private val ModestDatasetBytes: Long = 2L * 1024L * 1024L

  /** A middling dataset for the slow-consumer scenario, large enough to press the allowance. */
  private val SlowConsumerDatasetBytes: Long = TargetDatasetBytes / 4L

  /** The map partition whose first attempt is made to die, and how far into it the crash lands. */
  private val CrashingMapPartition: Int = 3

  private val RecordsBeforeProducerCrash: Int = 64

  /**
   * Task failures the scheduler may absorb.
   *
   * One injected crash needs one retry, and the allowance is set above that rather than at it so
   * that the case measures recovery instead of measuring how close the injection came to the
   * limit.
   */
  private val MaxTaskFailures: Int = 4

  /** Concurrent shuffles the fifth scenario runs, as the feature specifies. */
  private val ConcurrentShuffleCount: Int = StressConcurrentShuffles

  /**
   * Widths of the five concurrent shuffles, ascending and all distinct.
   *
   * Distinct widths are what make arbitration observable at all: reduce partition count is one of
   * the two keys the feature names, so five shuffles of equal width could not show that the key is
   * being used.
   */
  private val ConcurrentPartitionCounts: Seq[Int] = Seq(2, 4, 6, 8, 10)

  private val ConcurrentJobTimeoutMillis: Long = 600000L

  // Fixture identities for the in-process parts. Named rather than inlined so that a failure
  // message can say which stream or which consumer it is talking about.

  private val SpillPayloadBytes: Int = 4096

  /**
   * Blocks the injected allowance can hold.
   *
   * Ten, so that the eighty percent trigger falls on a whole number of blocks and the arithmetic
   * every assertion below makes is exact rather than approximate.
   */
  private val SpillBudgetBlocks: Long = 10L

  /** Bound on the produce-and-acknowledge loop, so a broken fixture fails rather than spins. */
  private val SpillRoundLimit: Int = 32

  private val SpillConsumerId: String = "streaming-shuffle-integration-consumer"

  private val SpillTaskAttemptId: Long = 7L

  private val TimeoutShuffleId: Int = 0

  private val TimeoutMapId: Long = 0L

  private val TimeoutTaskAttemptId: Long = 0L

  private val TimeoutPartitionId: Int = 0

  private val CreditLimitBytes: Long = 8L * 1024L * 1024L

  private val ArbitrationBandwidthMBps: Int = 1000

  private val ArbitrationBudgetBytes: Long = 1000L

  private val ArbitrationLowUtilizationBytes: Long = 100L

  private val ArbitrationHighUtilizationBytes: Long = 900L

  private val ArbitrationBlockBytes: Long = 4096L

  private val ArbitrationMapId: Long = 0L

  private val ArbitrationTaskAttemptId: Long = 0L

  private val ArbitrationPartitionId: Int = 0

  private val ArbitrationNumMaps: Int = 1

  override def beforeEach(): Unit = {
    super.beforeEach()
    // The metrics source is a JVM singleton whose counters survive from one case into the next, and
    // the streaming buffer allowance is likewise process scoped. Both are returned to a known state
    // here: a counter assertion that started from whatever ran before it is exactly the order
    // dependence the zero-flakiness gate exists to prevent.
    resetStreamingShuffleMetrics()
    MemorySpillManager.resetSharedStateForTesting()
  }

  // Shared fixtures for the cluster scenarios. Every closure handed to Spark below is built inside
  // one of these methods and references NOTHING on this suite, so no task closure ever drags a
  // non-serializable suite instance onto an executor.

  /**
   * A streaming configuration on the cluster master, with the two scheduler allowances a failure
   * scenario needs.
   *
   * Recovery from a lost or stood-down map output is by resubmission, and a fetch failure is
   * reported per map output that cannot be found, so a stage may legitimately be resubmitted once
   * per void partition. Both allowances are stated explicitly rather than relied upon, which is
   * what makes each case's premise visible instead of incidental.
   *
   * @param appName application name, so a failure names the case it came from
   * @return the configuration, with streaming selected and its behaviour gate open
   */
  private def resilientStreamingConf(appName: String): SparkConf = {
    withLocalMaster(streamingConf(), appName, ClusterMaster)
      .set(TASK_MAX_FAILURES, MaxTaskFailures)
      .set(STAGE_MAX_CONSECUTIVE_ATTEMPTS, 2 * PartitionCount + 4)
  }

  /**
   * Asserts that the live environment really is running the streaming manager.
   *
   * Every comparison in this suite is only evidence about streaming shuffle if the run genuinely
   * went through the streaming manager class, so that is established before anything is compared
   * rather than assumed from the configuration.
   */
  private def assertStreamingManagerInService(): Unit = {
    val manager = SparkEnv.get.shuffleManager
    assert(manager.isInstanceOf[StreamingShuffleManager],
      "spark.shuffle.manager=streaming must have produced a StreamingShuffleManager in the live " +
        s"environment, but the driver holds ${manager.getClass.getName}")
    assert(sc.getConf.get(SHUFFLE_MANAGER) === StreamingShuffleManager.SHORT_NAME,
      "the application must have selected the streaming manager by its short name, but selected " +
        s"'${sc.getConf.get(SHUFFLE_MANAGER)}'")
    assert(sc.getConf.get(SHUFFLE_STREAMING_ENABLED),
      s"${SHUFFLE_STREAMING_ENABLED.key} must be set true explicitly, because it defaults to " +
        "false and a closed gate would have every call delegated to the sort-based manager")
  }

  /** The dataset the feature names, grouped by key: the workload every scenario shuffles. */
  private def groupedLargeDataset(
      context: SparkContext,
      numPartitions: Int,
      totalBytes: Long): RDD[(Int, Iterable[String])] = {
    largeDataset(context, numPartitions, totalBytes).groupByKey(numPartitions)
  }

  /**
   * The same workload with a producer crash injected part way through one map partition.
   *
   * <b>Why this is a genuine mid-write producer failure.</b> The map function is fused into the map
   * stage, so a streaming writer PULLS records through it: by the time the sixty-fifth record of
   * the chosen partition is reached, blocks have already been framed, checksummed and streamed, and
   * the iterator is nowhere near exhausted. Throwing there loses a producer in exactly the state
   * the feature's producer-failure flow is specified for.
   *
   * <b>Why it is deterministic.</b> The fault is conditioned on the attempt number, so attempt zero
   * of exactly one partition always fails and its retry always succeeds. Nothing depends on timing,
   * on which executor the partition landed on, or on how far a concurrent task had progressed.
   *
   * @param context live context to build on
   * @param numPartitions partitions the shuffle produces
   * @param totalBytes approximate size of the generated dataset
   * @param failingPartition map partition whose first attempt dies
   * @param recordsBeforeCrash records that partition streams before the producer is lost
   * @return the grouped RDD, not yet computed
   */
  private def groupedCrashingDataset(
      context: SparkContext,
      numPartitions: Int,
      totalBytes: Long,
      failingPartition: Int,
      recordsBeforeCrash: Int): RDD[(Int, Iterable[String])] = {
    largeDataset(context, numPartitions, totalBytes)
      .mapPartitionsWithIndex { (partitionIndex, records) =>
        records.zipWithIndex.map { case (record, index) =>
          if (partitionIndex == failingPartition && index == recordsBeforeCrash &&
              TaskContext.get().attemptNumber() == 0) {
            throw new IllegalStateException(
              s"Injected streaming shuffle producer crash in partition $partitionIndex after " +
                s"$recordsBeforeCrash record(s)")
          }
          record
        }
      }
      .groupByKey(numPartitions)
  }

  /**
   * The same workload with one fallback condition driven on the executor, mid-write.
   *
   * The condition has to be driven ON the executor, because the policy a writer consults is
   * executor scoped and a driver-side instance is a different object; and the shuffle id has to
   * reach the closure, which is why the holder is mutable and is filled in after the graph exists
   * and before the action submits it. Whether the trip actually took effect is reported back
   * through an accumulator, because in cluster mode the executor's policy is not otherwise visible
   * from the driver and a placement-dependent probe job would be a race.
   *
   * @param context live context to build on
   * @param numPartitions partitions the shuffle produces
   * @param totalBytes approximate size of the generated dataset
   * @param holder carries the condition and, once the caller has set it, the shuffle id
   * @param observedTrips counts the map tasks that saw their executor's policy latched
   * @return the grouped RDD, not yet computed
   */
  private def groupedTrippingDataset(
      context: SparkContext,
      numPartitions: Int,
      totalBytes: Long,
      holder: MidWriteTripHolder,
      observedTrips: LongAccumulator): RDD[(Int, Iterable[String])] = {
    largeDataset(context, numPartitions, totalBytes)
      .mapPartitions { records =>
        records.zipWithIndex.map { case (record, index) =>
          if (index == 0) {
            holder.trip()
            SparkEnv.get.shuffleManager match {
              case manager: StreamingShuffleManager =>
                if (manager.streamingFallbackPolicy.hasTripped) {
                  observedTrips.add(1L)
                }
              case _ =>
            }
          }
          record
        }
      }
      .groupByKey(numPartitions)
  }

  /**
   * Reduces a grouped shuffle to a comparable set of per-key digests.
   *
   * A hundred mebibytes of values cannot be collected to the driver twice, so the comparison is on
   * a digest rather than on the values themselves -- but it is still a SET comparison over every
   * key, and each element carries the group's total value length as well as its order-independent
   * content hash, so a missing record, a duplicated record and a corrupted record are all
   * differences that the comparison reports.
   *
   * The reduce side walks each group TWICE, once for the length and once for the hash. That is not
   * incidental: it is the deterministic expression of a consumer running at half the rate its
   * producer frames at, which is what the slow-consumer scenario needs and what makes the same
   * digest usable as the baseline for every other scenario.
   *
   * @param shuffled the grouped output of a shuffle
   * @return one digest per key
   */
  private def digestOf(shuffled: RDD[(Int, Iterable[String])]): Set[(Int, Int, Int)] = {
    shuffled.map { case (key, values) =>
      val ordered = values.toSeq.sorted
      val totalLength = ordered.foldLeft(0)((running, value) => running + value.length)
      val contentHash = ordered.foldLeft(0)((running, value) => running * 31 + value.hashCode)
      (key, totalLength, contentHash)
    }.collect().toSet
  }

  /**
   * Runs the dataset through sort-based shuffle in a context of its own and returns its digest with
   * the wall time the run took.
   *
   * The context is created and stopped inside this method, so the caller must hold none of its own:
   * Spark permits one per JVM. Capturing the baseline before the context under test is started is
   * the cheaper of the two ways to satisfy that.
   *
   * @param numPartitions partitions the shuffle produces
   * @param totalBytes approximate size of the generated dataset
   * @param master master URL for the throwaway context
   * @return the baseline digest and the nanoseconds the run took
   */
  private def sortBaselineDigest(
      numPartitions: Int,
      totalBytes: Long,
      master: String): (Set[(Int, Int, Int)], Long) = {
    val conf = withLocalMaster(
      sortBaselineConf(), "streaming-shuffle-integration-sort-baseline", master)
    val baselineContext = new SparkContext(conf)
    try {
      if (master.startsWith("local-cluster")) {
        TestUtils.waitUntilExecutorsUp(baselineContext, ExecutorCount, ExecutorStartupTimeoutMillis)
      }
      timed(digestOf(groupedLargeDataset(baselineContext, numPartitions, totalBytes)))
    } finally {
      baselineContext.stop()
    }
  }

  /**
   * Runs a body and reports how long it took, in nanoseconds.
   *
   * Wall time, deliberately and only here: a latency comparison is the one measurement in this
   * suite that is about real elapsed time rather than about a bound, and it is reported rather than
   * asserted on.
   *
   * @param body the work to measure
   * @tparam T whatever the body returns
   * @return the body's result and the nanoseconds it took
   */
  private def timed[T](body: => T): (T, Long) = {
    val startedAt = System.nanoTime()
    val result = body
    (result, System.nanoTime() - startedAt)
  }

  /**
   * The reduction in elapsed time, as a whole percentage of the baseline.
   *
   * Negative when the measured path was slower, which is reported as it stands rather than clamped:
   * a regression that showed up as zero would be indistinguishable from parity.
   *
   * @param baselineNanos elapsed time of the sort-based run
   * @param measuredNanos elapsed time of the streaming run
   * @return the reduction as a percentage of the baseline
   */
  private def latencyReductionPercent(baselineNanos: Long, measuredNanos: Long): Long = {
    require(baselineNanos > 0L,
      s"the baseline must have taken measurable time but was $baselineNanos ns")
    (baselineNanos - measuredNanos) * PercentScale / baselineNanos
  }

  /**
   * What each executor publishes under the streaming shuffle namespace, gathered from inside tasks.
   *
   * In cluster mode the counters an executor advances live in the executor's own JVM, so this
   * is the only way to observe them. The probe deliberately asserts on properties that hold for
   * EVERY executor it reaches -- the exact metric name set -- rather than on a count that would
   * depend on which executor the probe happened to land on.
   *
   * @param context live context to probe through
   * @param probePartitions tasks to spread across the executors
   * @return one observation per distinct executor reached: its id, its metric names, and its
   *         partial-read invalidation count
   */
  private def executorMetricObservations(
      context: SparkContext,
      probePartitions: Int): Seq[(String, Set[String], Long)] = {
    context.parallelize(0 until probePartitions, probePartitions).mapPartitions { _ =>
      val names = StreamingShuffleMetricsSource.metricRegistry.getNames.asScala.toSet
      val invalidations = StreamingShuffleMetricsSource.METRIC_PARTIAL_READ_INVALIDATIONS.getCount
      Iterator.single((SparkEnv.get.executorId, names, invalidations))
    }.collect().toSeq.distinct
  }

  /**
   * Asserts that streaming spill accounting reuses Spark's own accumulators and adds none of its
   * own.
   *
   * A parallel counter is the failure mode this guards against: it would make the streaming path
   * invisible to every existing Spark observability surface while looking, from inside the
   * subsystem, as though spill were being reported.
   */
  private def assertNoParallelSpillCounters(): Unit = {
    val accessors = classOf[TaskMetrics].getMethods.toSeq.map(method => method.getName)
    val streamingSpecific =
      accessors.filter(name => name.toLowerCase(Locale.ROOT).contains("streaming"))
    assert(streamingSpecific.isEmpty,
      "TaskMetrics must carry no streaming-specific accumulator, because a parallel counter " +
        "would make the streaming path invisible to every existing surface, but found " +
        s"$streamingSpecific")
    Seq("memoryBytesSpilled", "diskBytesSpilled", "peakExecutionMemory").foreach { accessor =>
      assert(accessors.contains(accessor),
        s"TaskMetrics must expose $accessor, which is the accumulator the streaming path reuses")
    }
    assert(streamingShuffleMetricNames() === MetricNames.toSet,
      "exactly the four documented streaming metrics may exist, and no spill-specific fifth, but " +
        s"the registry holds ${streamingShuffleMetricNames().toSeq.sorted.mkString(", ")}")
  }

  // ---------------------------------------------------------------------------------------------
  // The numeric contract every scenario below is written against.
  //
  // Pinned in one place, and first, because each of the five scenarios is only evidence about the
  // feature if the figure it exercises is the figure the feature specifies. A scenario that walked
  // utilisation up to a threshold which had quietly moved would still pass while proving nothing,
  // and this case is what turns that into a failure with the moved constant named. It needs no
  // context, so it costs nothing and runs first.
  // ---------------------------------------------------------------------------------------------

  test("the numeric contract these scenarios exercise is the one the feature specifies") {
    // Scenario shape.
    assert(LargeDatasetBytes === 100L * 1024L * 1024L,
      s"the named dataset is one hundred mebibytes, but the suite reads $LargeDatasetBytes bytes")
    assert(PartitionCount === 10,
      s"the named shuffle width is ten reduce partitions, but the suite reads $PartitionCount")
    assert(ConcurrentShuffleCount === 5,
      s"the named concurrency is five shuffles, but the suite reads $ConcurrentShuffleCount")
    assert(PerTestTimeoutMinutes === 20,
      s"the default per-test budget is twenty minutes, but the suite reads $PerTestTimeoutMinutes")

    // Memory: the allowance, its range, the per-partition formula and the eviction trigger.
    assert(DefaultBufferSizePercent === 20 && MinBufferSizePercent === 1 &&
      MaxBufferSizePercent === 50,
      s"the buffer allowance defaults to 20 percent within [1, 50], but the suite reads " +
        s"$DefaultBufferSizePercent within [$MinBufferSizePercent, $MaxBufferSizePercent]")
    assert(DefaultSpillThresholdPercent === 80 && MinSpillThresholdPercent === 50 &&
      MaxSpillThresholdPercent === 95,
      s"the spill trigger defaults to 80 percent within [50, 95], but the suite reads " +
        s"$DefaultSpillThresholdPercent within " +
        s"[$MinSpillThresholdPercent, $MaxSpillThresholdPercent]")
    // The contracted formula, stated as the feature states it: the aggregate allowance is that
    // percentage of executor memory, and one partition's share is the aggregate divided by the
    // partition count. Computed over a round gibibyte so the arithmetic is checkable by eye.
    val executorMemoryBytes = 1024L * 1024L * 1024L
    assert(aggregateBudgetBytes(executorMemoryBytes, DefaultBufferSizePercent) ===
      exactPercentageOf(executorMemoryBytes, DefaultBufferSizePercent),
      "the aggregate allowance must be exactly the configured percentage of executor memory")
    val aggregateBytes = aggregateBudgetBytes(executorMemoryBytes, DefaultBufferSizePercent)
    assert(perPartitionBudgetBytes(
      executorMemoryBytes, DefaultBufferSizePercent, PartitionCount) ===
      aggregateBytes / PartitionCount.toLong,
      "the per-partition allowance must be (executorMemory * bufferPercent) / numPartitions")
    assert(SpillPollIntervalMillis === 100L,
      s"the threshold is polled every 100 ms, but the suite reads $SpillPollIntervalMillis ms")
    assert(ReclamationDeadlineMillis === 100L,
      s"reclamation completes within 100 ms of an acknowledgement, but the suite reads " +
        s"$ReclamationDeadlineMillis ms")

    // Wire and pacing.
    assert(MaxBlockSizeBytes === 2097152,
      s"a block is capped at two mebibytes, 2097152 bytes, but the suite reads $MaxBlockSizeBytes")
    assert(TransportModuleName === "shuffle-streaming",
      s"the transport module is named shuffle-streaming, but the suite reads $TransportModuleName")
    assert(BandwidthCeilingPercent === 80L,
      s"egress is held to 80 percent of the administered capacity, but the suite reads " +
        s"$BandwidthCeilingPercent percent")
    assert(LinkSaturationTripPercent === 90L,
      s"saturation trips above 90 percent of the link, but the suite reads " +
        s"$LinkSaturationTripPercent percent")
    assert(BandwidthCeilingPercent != LinkSaturationTripPercent,
      "the egress ceiling and the saturation trip are DIFFERENT constants, and conflating them " +
        "would either throttle to the trip point or trip at the throttle point")

    // Liveness, degradation and retry. Each bound is pinned together with the value one millisecond
    // short of it, because "the timer fires" and "the timer does not fire early" are two claims.
    assert(ProducerConnectionTimeoutMillis === 5000L &&
      JustBeforeProducerTimeoutMillis === 4999L,
      s"the producer connection timeout is 5000 ms with a non-firing boundary at 4999 ms, but " +
        s"the suite reads $ProducerConnectionTimeoutMillis and " +
        s"$JustBeforeProducerTimeoutMillis")
    assert(HeartbeatIntervalMillis === ProducerConnectionTimeoutMillis,
      "a heartbeat is emitted at the cadence the producer detector expects, so a producer with " +
        "nothing to send stays alive by heartbeating alone")
    assert(ConsumerLivenessTimeoutMillis === 10000L &&
      JustBeforeConsumerLivenessMillis === 9999L,
      s"the consumer liveness window is 10000 ms with a non-firing boundary at 9999 ms, but the " +
        s"suite reads $ConsumerLivenessTimeoutMillis and $JustBeforeConsumerLivenessMillis")
    assert(ConsumerSlownessRatio === 2.0d,
      s"a consumer is slow at twice the producer's time per byte, but the suite reads " +
        s"$ConsumerSlownessRatio")
    assert(SustainedSlownessWindowMillis === 60000L && SustainedSlownessTripMillis === 60001L &&
      JustBeforeSustainedSlownessMillis === 59999L,
      s"sustained slowness trips STRICTLY beyond 60000 ms, so at 60001 ms and not at 59999 ms, " +
        s"but the suite reads $SustainedSlownessWindowMillis, $SustainedSlownessTripMillis and " +
        s"$JustBeforeSustainedSlownessMillis")
    assert(MaxRetryAttempts === 5,
      s"retransmission is attempted at most five times, but the suite reads $MaxRetryAttempts")
    assert(RetryBaseBackoffMillis === 1000L,
      s"the backoff ladder starts at one second, but the suite reads $RetryBaseBackoffMillis ms")
    assert(RetryBackoffLadderMillis === Seq(1000L, 2000L, 4000L, 8000L, 16000L),
      s"the ladder doubles from one second over five attempts, but the suite reads " +
        s"${RetryBackoffLadderMillis.mkString(", ")}")

    // Observability and the error surface.
    assert(MetricNames.size === 4,
      s"exactly four metrics exist and there is no fifth, but the suite reads ${MetricNames.size}")
    assert(MetricNames.toSet === Set(BufferUtilizationMetricName, SpillCountMetricName,
      BackpressureEventsMetricName, PartialReadInvalidationsMetricName),
      s"the four metrics are the documented ones, but the suite reads " +
        s"${MetricNames.mkString(", ")}")
    assert(streamingShuffleMetricNames() === MetricNames.toSet,
      "and the live registry must publish exactly those four, but publishes " +
        s"${streamingShuffleMetricNames().toSeq.sorted.mkString(", ")}")
    assert(AuthorizedErrorConditions.size === 3,
      s"exactly three error conditions are authorized, but the suite reads " +
        s"${AuthorizedErrorConditions.size}")
    assert(AuthorizedErrorConditions.forall(_.startsWith(StreamingShuffleConditionPrefix)),
      "every authorized condition must carry the feature's prefix, but the set is " +
        s"${AuthorizedErrorConditions.toSeq.sorted.mkString(", ")}")
    assert(StreamingShuffleSqlState === "XXKST",
      s"the conditions carry SQLSTATE XXKST, but the suite reads $StreamingShuffleSqlState")

    // And the accumulators the spill scenario measures against are Spark's own.
    assertNoParallelSpillCounters()
  }

  // ---------------------------------------------------------------------------------------------
  // Scenario 1: a 100 MB, 10 partition shuffle measured against the sort-based baseline, with the
  // 30% latency reduction target reported.
  // ---------------------------------------------------------------------------------------------

  test("a 100 MB 10 partition shuffle streams correctly and reports its 30% latency target") {
    // The baseline runs first, in a context of its own, on the SAME master as the run under test --
    // otherwise the two elapsed times would not be comparable and the reported figure would be
    // measuring the master rather than the shuffle.
    val (baseline, baselineNanos) =
      sortBaselineDigest(PartitionCount, LargeDatasetBytes, ClusterMaster)
    val expectedRecords = PartitionCount * recordsPerPartitionFor(PartitionCount, LargeDatasetBytes)
    assert(baseline.size === expectedRecords,
      s"the sort-based baseline must produce one group per distinct key, $expectedRecords in " +
        s"all, but produced ${baseline.size}")

    sc = new SparkContext(resilientStreamingConf("streaming-shuffle-integration-hundred-mib"))
    TestUtils.waitUntilExecutorsUp(sc, ExecutorCount, ExecutorStartupTimeoutMillis)
    assertStreamingManagerInService()

    val (observed, streamingNanos) =
      timed(digestOf(groupedLargeDataset(sc, PartitionCount, LargeDatasetBytes)))

    // The overarching assertion first: the job completed and its output is exactly the baseline's.
    val megabytes = LargeDatasetBytes / BytesPerMebibyte
    assertNoDataLoss(observed, baseline,
      s"a $PartitionCount partition streaming shuffle of $megabytes MB")
    assert(observed.size === expectedRecords,
      s"the streaming run must produce the same $expectedRecords groups, but produced " +
        s"${observed.size}")

    // The measurement is asserted to be WELL FORMED, and the reduction is reported against the
    // named target window. The percentage itself is deliberately not a gate: the feature's 30 to 50
    // percent figure is an acceptance target measured by StreamingShufflePerformanceBenchmark, this
    // repository asserts no performance threshold anywhere, and a suite that did would fail on a
    // loaded machine for a reason that is not a defect.
    assert(baselineNanos > 0L, "the sort-based baseline must have taken measurable time")
    assert(streamingNanos > 0L, "the streaming run must have taken measurable time")
    assert(MinLatencyReductionPercent === 30,
      s"the acceptance target's lower bound is 30 percent, but the suite reads " +
        s"$MinLatencyReductionPercent")
    assert(MaxLatencyReductionPercent === 50,
      s"the acceptance target's upper bound is 50 percent, but the suite reads " +
        s"$MaxLatencyReductionPercent")
    assert(MinLatencyReductionPercent < MaxLatencyReductionPercent,
      "the target window must be an interval, or no measurement could ever fall inside it")
    // A pure check of the arithmetic the target is judged by, so that the figure reported below
    // means what the feature says it means: half the time is a fifty percent reduction, and a
    // slower run reports a negative reduction rather than a clamped zero.
    assert(latencyReductionPercent(1000L, 500L) === 50L,
      "halving the elapsed time must be reported as a 50 percent reduction")
    assert(latencyReductionPercent(1000L, 2000L) === -100L,
      "a run twice as slow must be reported as a negative reduction rather than as parity")

    val reduction = latencyReductionPercent(baselineNanos, streamingNanos)
    logInfo(s"Streaming shuffle of ${LargeDatasetBytes / BytesPerMebibyte} MB across " +
      s"$PartitionCount partitions took ${TimeUnit.NANOSECONDS.toMillis(streamingNanos)} ms " +
      s"against a sort-based baseline of ${TimeUnit.NANOSECONDS.toMillis(baselineNanos)} ms, " +
      s"a reduction of $reduction percent against the acceptance target of " +
      s"$MinLatencyReductionPercent to $MaxLatencyReductionPercent percent")
  }

  // ---------------------------------------------------------------------------------------------
  // Scenario 2: a producer failure mid-shuffle, recovered by the UNMODIFIED scheduler.
  // ---------------------------------------------------------------------------------------------

  test("a producer failure mid shuffle is recovered and the output is still the baseline") {
    // There is NO scheduler edit to test. The whole of the streaming subsystem's interface to stage
    // recomputation is the pre-existing FetchFailedException, whose conversion to a fetch-failure
    // reason the task framework already performs, so what is asserted here is that recovery
    // happened through Spark's own machinery and cost a bounded number of resubmissions.
    val (baseline, _) = sortBaselineDigest(PartitionCount, ModestDatasetBytes, "local[2]")
    assert(baseline.nonEmpty, "the sort-based baseline must produce output to compare against")

    sc = new SparkContext(resilientStreamingConf("streaming-shuffle-integration-producer-failure"))
    TestUtils.waitUntilExecutorsUp(sc, ExecutorCount, ExecutorStartupTimeoutMillis)
    assertStreamingManagerInService()

    val recorder = new StreamingShuffleJobRecorder
    sc.addSparkListener(recorder)
    val shuffled = groupedCrashingDataset(sc, PartitionCount, ModestDatasetBytes,
      CrashingMapPartition, RecordsBeforeProducerCrash)
    val shuffleId = shuffled.dependencies.head
      .asInstanceOf[ShuffleDependency[Int, String, _]].shuffleId
    assert(shuffleId >= 0, "the workload must have registered a shuffle for the failure to be in")

    val observed = try {
      digestOf(shuffled)
    } finally {
      sc.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
      sc.removeSparkListener(recorder)
    }

    // The overarching assertion: losing a producer part way through its write costs throughput,
    // never correctness and never the job.
    assertNoDataLoss(observed, baseline,
      "a streaming shuffle whose producer died mid-write and was recomputed")
    assert(observed.size === baseline.size,
      s"recovery must reproduce every group exactly once, but produced ${observed.size} against " +
        s"the baseline's ${baseline.size}")

    // The injected crash really happened, and it was counted as a task failure rather than
    // swallowed somewhere inside the subsystem.
    val injected = recorder.countedFailures.filter { reason =>
      reason.toErrorString.contains("Injected streaming shuffle producer crash")
    }
    assert(injected.nonEmpty,
      "the injected producer crash must have been reported as a task failure, yet the run " +
        s"counted only ${recorder.countedFailures.size} failure(s): " +
        recorder.countedFailures.take(3).map(_.toErrorString).mkString("[", ", ", "]"))

    // Every fetch failure the run reported, if it reported any, names THIS shuffle. Recovery took
    // the sanctioned signal rather than a mechanism of the subsystem's own invention, and a fetch
    // failure naming some other shuffle would mean the invalidation was not per-producer.
    recorder.fetchFailures.foreach { fetchFailed =>
      assert(fetchFailed.shuffleId === shuffleId,
        s"a fetch failure must name the shuffle whose producer was lost, $shuffleId, but named " +
          s"${fetchFailed.shuffleId}")
      assert(fetchFailed.mapIndex >= 0 || fetchFailed.mapIndex === -1,
        s"a fetch failure must carry a map index or the documented unknown marker, but carried " +
          s"${fetchFailed.mapIndex}")
    }

    // Recovery was bounded. Three routes exist and all three are sanctioned -- a rewritten attempt,
    // a withdrawn registration resubmitted per void partition, or a fetch failure the scheduler
    // answers by recomputing -- so the bound states what all three guarantee rather than picking
    // one: at most one resubmission per map partition. The failure mode this rules out is a stage
    // resubmitted over and over until the stage-attempt limit aborts the job.
    assert(recorder.maxStageSubmissions <= 1 + PartitionCount,
      s"a lost producer must be recovered within one resubmission per map partition, yet the " +
        s"busiest stage of a $PartitionCount partition shuffle was submitted " +
        s"${recorder.maxStageSubmissions} time(s), with ${recorder.fetchFailures.size} fetch " +
        "failure(s) reported")
    assert(recorder.taskEndCount > PartitionCount.toLong,
      "the run must have ended more tasks than the map stage has partitions, because the failed " +
        s"attempt is one of them, but ended only ${recorder.taskEndCount}")

    // The failure path introduced no counter of its own: every executor still publishes exactly the
    // four documented metrics, invalidations included.
    val observations = executorMetricObservations(sc, 2 * ExecutorCount * PartitionCount)
    assert(observations.nonEmpty, "the metric probe must have run on at least one executor")
    observations.foreach { case (executorId, names, invalidations) =>
      assert(names === MetricNames.toSet,
        s"executor $executorId must publish exactly the four documented streaming metrics, but " +
          s"published ${names.toSeq.sorted.mkString(", ")}")
      assert(invalidations >= 0L,
        s"executor $executorId reported a negative partial-read invalidation count, " +
          s"$invalidations, which an operator-facing event series can never be")
    }
    assertNoParallelSpillCounters()
  }

  // ---------------------------------------------------------------------------------------------
  // Scenario 3: a consumer held at 50% of the producer's rate until spill triggers.
  // ---------------------------------------------------------------------------------------------

  test("a consumer at 50% of the producer rate triggers spill onto the existing accumulators") {
    // The scenario has two halves, and they answer two different questions.
    //
    // The cluster half answers "does a job survive a consumer that cannot keep up?" It runs the
    // shuffle in the most spill-prone posture the configuration permits -- the buffer allowance at
    // its documented minimum of one percent and the spill threshold at its documented minimum of
    // fifty -- with a reduce side that walks every group twice, so it consumes at half the rate the
    // map side frames at. The answer must be yes, with output identical to sort-based shuffle.
    //
    // The in-process half answers "when spill does happen, is it counted and accounted correctly?"
    // A cluster run cannot answer that: the counters an executor advances live in the executor's
    // JVM and its task metrics are aggregated asynchronously, so the volumes and the event count
    // would be asserted against whatever happened to have been reported by the time the driver
    // looked. Driving the spill manager directly against an injected allowance and a frozen clock
    // makes the threshold arithmetic exact and the assertions total.
    val (baseline, _) = sortBaselineDigest(PartitionCount, SlowConsumerDatasetBytes, "local[2]")
    assert(baseline.nonEmpty, "the sort-based baseline must produce output to compare against")

    val conf = withLocalMaster(
      streamingConfWithOverrides(
        bufferSizePercent = MinBufferSizePercent,
        spillThreshold = MinSpillThresholdPercent),
      "streaming-shuffle-integration-slow-consumer", ClusterMaster)
      .set(TASK_MAX_FAILURES, MaxTaskFailures)
      .set(STAGE_MAX_CONSECUTIVE_ATTEMPTS, 2 * PartitionCount + 4)
    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, ExecutorCount, ExecutorStartupTimeoutMillis)
    assertStreamingManagerInService()
    assert(sc.getConf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT) === MinBufferSizePercent,
      s"the run must hold the minimum buffer allowance of $MinBufferSizePercent percent, but " +
        s"holds ${sc.getConf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)}")
    assert(sc.getConf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD) === MinSpillThresholdPercent,
      s"the run must hold the minimum spill threshold of $MinSpillThresholdPercent percent, but " +
        s"holds ${sc.getConf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD)}")

    val recorder = new StreamingShuffleJobRecorder
    sc.addSparkListener(recorder)
    val observed = try {
      digestOf(groupedLargeDataset(sc, PartitionCount, SlowConsumerDatasetBytes))
    } finally {
      sc.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
      sc.removeSparkListener(recorder)
    }
    assertNoDataLoss(observed, baseline,
      "a streaming shuffle whose consumer ran at half the producer's rate")
    assert(recorder.taskEndCount >= 2L * PartitionCount.toLong,
      "both stages of the shuffle must have ended every one of their tasks, which is " +
        s"${2 * PartitionCount} at least, but only ${recorder.taskEndCount} ended")
    // The spill really happened, and it reached the driver through Spark's OWN accumulators rather
    // than through any channel of the subsystem's invention. It is forced by arithmetic and not by
    // luck: a one percent allowance over a gibibyte executor leaves a few hundred kibibytes per
    // reduce partition against megabytes of data per partition, and a streaming map task makes its
    // retained window durable before it finishes in any case, so bytes reach local disk on every
    // non-empty streaming shuffle.
    assert(recorder.diskBytesSpilled > 0L,
      "bytes must have reached local disk and been reported on TaskMetrics.diskBytesSpilled, but " +
        s"the run reported ${recorder.diskBytesSpilled}; a streaming shuffle whose consumer runs " +
        "at half rate under a one percent buffer allowance cannot have held everything in memory")
    assert(recorder.peakExecutionMemory > 0L,
      "the high-water mark of the streaming reservation must be reported on " +
        s"TaskMetrics.peakExecutionMemory, but the run reported ${recorder.peakExecutionMemory}")
    assert(recorder.memoryBytesSpilled >= 0L,
      s"memory spill volume can never run backwards, but read ${recorder.memoryBytesSpilled}")
    logInfo(s"Slow-consumer streaming shuffle reported ${recorder.memoryBytesSpilled} byte(s) of " +
      s"memory spill and ${recorder.diskBytesSpilled} byte(s) of disk spill on the existing task " +
      s"metric accumulators, with a peak execution memory of ${recorder.peakExecutionMemory} bytes")

    // The in-process half. The allowance is sized so that the eighty percent trigger falls on a
    // whole number of blocks, which is what makes every figure below exact rather than approximate.
    val blockCharge = SpillPayloadBytes.toLong + MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES
    val budgetBytes = SpillBudgetBlocks * blockCharge
    val unifiedMemoryBytes = budgetBytes * PercentScale / DefaultBufferSizePercent.toLong
    val quota = new MemorySpillManager.ExecutorBufferQuota(
      DefaultBufferSizePercent, DefaultSpillThresholdPercent, () => unifiedMemoryBytes)
    val clock: ManualClock = newManualClock()
    val spillConf = streamingConfWithOverrides()
    val context = newTaskContext(
      sc.env, partitionId = 0, taskAttemptId = SpillTaskAttemptId, numPartitions = 1)
    // Read through the trait's accessors, so the fixture binds to the contract a production
    // component is handed rather than to the implementation's constructor fields.
    val taskMemoryManager = (context: TaskContext).taskMemoryManager()
    val manager = new MemorySpillManager(
      taskMemoryManager, spillConf, clock, Some(quota), autoPoll = false)
    try {
      manager.registerPartitionCount(1)
      manager.registerCleanup(context)
      assert(manager.registerConsumer(SpillConsumerId),
        "a live producer must admit the consumer whose acknowledgements pace it")
      assert(manager.totalBudgetBytes === budgetBytes,
        s"the injected allowance must hold exactly $SpillBudgetBlocks blocks, $budgetBytes " +
          s"bytes, but reported ${manager.totalBudgetBytes}")
      val expectedTrigger = budgetBytes * DefaultSpillThresholdPercent / PercentScale
      assert(manager.spillThresholdBytes === expectedTrigger,
        s"the trigger must sit at $DefaultSpillThresholdPercent percent of the allowance, so at " +
          s"$expectedTrigger bytes, but sits at ${manager.spillThresholdBytes}")
      assert(observedSpillCount() === 0L, "beforeEach must have reset the spill counter")

      // The consumer at exactly fifty percent: two blocks framed per round, one acknowledged. The
      // retained window therefore grows by one block per round, which is what walks utilisation up
      // to the threshold with no timing assumption and no sleeping anywhere.
      var round = 0
      while (manager.bufferUtilizationPercent < DefaultSpillThresholdPercent.toLong &&
          round < SpillRoundLimit) {
        val firstSequence = 2L * round.toLong
        Seq(firstSequence, firstSequence + 1L).foreach { sequenceNumber =>
          assert(manager.bufferBlock(0, sequenceNumber, payloadOfLength(sequenceNumber,
            SpillPayloadBytes)),
            s"block $sequenceNumber was refused, but the allowance is sized to admit it")
        }
        manager.acknowledge(SpillConsumerId, 0, round.toLong)
        round += 1
      }
      assert(round < SpillRoundLimit,
        s"the half-rate consumer must have walked utilisation to $DefaultSpillThresholdPercent " +
          s"percent within $SpillRoundLimit rounds, but stopped at " +
          s"${manager.bufferUtilizationPercent} percent")
      assert(manager.bufferUtilizationPercent === DefaultSpillThresholdPercent.toLong,
        s"utilisation must land exactly on the $DefaultSpillThresholdPercent percent trigger, " +
          s"but reads ${manager.bufferUtilizationPercent} percent")
      assert(manager.bufferedBytes === manager.spillThresholdBytes,
        s"the retained window must be exactly the trigger volume, but holds " +
          s"${manager.bufferedBytes} bytes against a trigger of ${manager.spillThresholdBytes}")

      assert(manager.maybeSpill(),
        "reaching the threshold must evict at least one partition rather than keep buffering")
      assert(manager.spillCount >= 1L,
        s"the component must count the spill event, but counted ${manager.spillCount}")
      assert(observedSpillCount() >= 1L,
        s"shuffle.streaming.spillCount must advance on a threshold spill, but reads " +
          s"${observedSpillCount()}")
      assert(manager.memoryBytesSpilled > 0L, "bytes must have left memory")
      assert(manager.diskBytesSpilled > 0L, "and must have been committed to local disk")
      assert(manager.peakMemoryBytes > 0L, "and the high-water mark must have been recorded")
      assert(manager.allSpilledBlocks.nonEmpty, "the evicted blocks must be retained on disk")

      // The volumes land on the EXISTING accumulators, reported once, at closure.
      val metrics = (context: TaskContext).taskMetrics()
      assert(metrics.memoryBytesSpilled === 0L,
        "nothing may be reported before the consumer closes, because reporting per eviction " +
          "would put a per-record cost on the data path")
      context.markTaskCompleted(None)
      assert(manager.isClosed, "task completion must close the consumer")
      assert(metrics.memoryBytesSpilled === manager.memoryBytesSpilled,
        s"spilled memory must be reported on TaskMetrics.memoryBytesSpilled, but the accumulator " +
          s"reads ${metrics.memoryBytesSpilled} against ${manager.memoryBytesSpilled}")
      assert(metrics.diskBytesSpilled === manager.diskBytesSpilled,
        s"committed bytes must be reported on TaskMetrics.diskBytesSpilled, but the accumulator " +
          s"reads ${metrics.diskBytesSpilled} against ${manager.diskBytesSpilled}")
      assert(metrics.peakExecutionMemory === manager.peakMemoryBytes,
        s"the peak reservation must be reported on TaskMetrics.peakExecutionMemory, but the " +
          s"accumulator reads ${metrics.peakExecutionMemory} against ${manager.peakMemoryBytes}")
      assertNoParallelSpillCounters()
    } finally {
      manager.close()
      val leaked = taskMemoryManager.cleanUpAllAllocatedMemory()
      assert(leaked === 0L,
        s"a closed spill manager left $leaked byte(s) of execution memory acquired; buffered " +
          "bytes must be released on close whatever the outcome of the task")
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Scenario 4: a network partition fires the 5 s producer timeout and stands streaming down.
  // ---------------------------------------------------------------------------------------------

  test("a network partition fires the 5 s timeout and the job still completes on sort") {
    val (baseline, _) = sortBaselineDigest(PartitionCount, ModestDatasetBytes, "local[2]")
    assert(baseline.nonEmpty, "the sort-based baseline must produce output to compare against")

    // First, the detection, exact and on an injected clock. A partitioned network is silence, and
    // silence for the connection timeout is what the reader turns into a partial-read invalidation
    // and a fetch failure. Both halves of the boundary are asserted, because "the timer fired" and
    // "the timer did not fire early" are different claims and a suite owes both.
    val clock: ManualClock = newManualClock()
    val injector = newFaultInjector(clock)
    val protocolConf = streamingConfWithOverrides()
    val budget = TokenBucketRateLimiter.executorBudget(protocolConf, clock)
    val protocol = new BackpressureProtocol(protocolConf, null, budget, clock)
    try {
      assert(ProducerConnectionTimeoutMillis === 5000L,
        s"the producer connection timeout is five seconds, but the suite reads " +
          s"$ProducerConnectionTimeoutMillis ms")
      val key = BackpressureStreamKey.forProducer(
        TimeoutShuffleId, TimeoutMapId, TimeoutTaskAttemptId, TimeoutPartitionId)
      protocol.registerShuffle(TimeoutShuffleId, PartitionCount)
      assert(protocol.registerStream(key, CreditLimitBytes), "the credit ledger must open")
      injector.partitionNetwork()
      assert(injector.networkPartitioned, "the fixture must report the link as partitioned")
      assert(!protocol.isProducerTimedOut(key),
        "a stream that has just opened has heard from its peer and cannot be timed out")

      advanceJustBeforeProducerTimeout(clock)
      assert(protocol.millisSinceInbound(key) === Some(JustBeforeProducerTimeoutMillis),
        s"the detector must see exactly the $JustBeforeProducerTimeoutMillis ms the clock advanced")
      assert(!protocol.isProducerTimedOut(key),
        s"the timeout must NOT fire at $JustBeforeProducerTimeoutMillis ms")
      assert(protocol.timedOutProducerStreams.isEmpty,
        "and no stream may be reported as having lost its producer one millisecond early")
      clock.advance(1L)
      assert(protocol.isProducerTimedOut(key),
        s"the timeout must fire at exactly $ProducerConnectionTimeoutMillis ms")
      assert(protocol.timedOutProducerStreams === Seq(key),
        "and the whole-set scan must apply the same definition as the single-stream predicate")

      injector.expireProducerConnection()
      assert(injector.fireCount(
        StreamingShuffleFaultScenario.ConnectionTimeoutDuringTransfer) === 1,
        "the injected connection timeout must be tallied exactly once")
      assert(protocol.isProducerTimedOut(key),
        "and further silence must leave the producer judged lost rather than revive it")
    } finally {
      injector.healNetwork()
      injector.disarmAll()
      protocol.reset()
    }

    // Then the consequence, end to end on real executor JVMs: an unusable link stands streaming
    // down and the job still completes on the sort-based delegate. The condition is driven ON the
    // executor, from inside a map function running while the streaming writer pulls records through
    // it, so the trip is a genuine mid-write trip through the policy the production code consults.
    sc = new SparkContext(resilientStreamingConf("streaming-shuffle-integration-partition"))
    TestUtils.waitUntilExecutorsUp(sc, ExecutorCount, ExecutorStartupTimeoutMillis)
    assertStreamingManagerInService()
    val manager = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]
    assert(manager.streamingFallbackPolicy.streamingActive,
      "streaming must be in service before the condition is driven, or the case would be proving " +
        "nothing about degradation")

    val holder = new MidWriteTripHolder(StreamingShuffleFallbackReason.NetworkSaturation)
    val observedTrips = sc.longAccumulator("streamingShuffleObservedTrips")
    val shuffled =
      groupedTrippingDataset(sc, PartitionCount, ModestDatasetBytes, holder, observedTrips)
    // Set after the graph exists and before the action, because task closures are serialized when
    // the job is submitted rather than when the RDD is defined.
    holder.shuffleId = shuffled.dependencies.head
      .asInstanceOf[ShuffleDependency[Int, String, _]].shuffleId
    assert(holder.shuffleId >= 0, "the workload must have registered a shuffle to stand down")

    val recorder = new StreamingShuffleJobRecorder
    sc.addSparkListener(recorder)
    val observed = try {
      digestOf(shuffled)
    } finally {
      sc.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
      sc.removeSparkListener(recorder)
    }

    assertNoDataLoss(observed, baseline,
      "a job whose map stage stood streaming down mid-write because the link was unusable")
    assert(observedTrips.value >= 1L,
      "at least one map task must have observed its own executor's policy latched, yet the run " +
        s"reported ${observedTrips.value}; the condition is executor scoped, so an " +
        "accumulator is how the driver learns that the trip took effect where it matters")
    assert(recorder.maxStageSubmissions <= 1 + PartitionCount,
      s"a stand-down must be recovered within one resubmission per map partition, yet the " +
        s"busiest stage of a $PartitionCount partition shuffle was submitted " +
        s"${recorder.maxStageSubmissions} time(s), with ${recorder.fetchFailures.size} fetch " +
        "failure(s) reported")
    assert(sc.getConf.get(SHUFFLE_MANAGER) === StreamingShuffleManager.SHORT_NAME,
      "the application never changed manager class: degradation is delegation inside the " +
        "streaming manager, not a different manager being selected")
  }

  // ---------------------------------------------------------------------------------------------
  // Scenario 5: five concurrent shuffles arbitrating for one executor's buffer allowance.
  // ---------------------------------------------------------------------------------------------

  test("five concurrent shuffles all complete and arbitrate on partition count and volume") {
    // Baselines first, one per width, each in a context of its own -- Spark permits one context per
    // JVM, so every baseline is captured before the context under test is started.
    assert(ConcurrentPartitionCounts.size === ConcurrentShuffleCount,
      s"the scenario runs $ConcurrentShuffleCount concurrent shuffles, but " +
        s"${ConcurrentPartitionCounts.size} widths were declared")
    assert(ConcurrentPartitionCounts.distinct.size === ConcurrentPartitionCounts.size,
      "the widths must be distinct, or arbitration on partition count could not be observed")
    val baselines = ConcurrentPartitionCounts.map { numPartitions =>
      sortBaselineGroupedOutput(numPartitions = numPartitions)
    }
    baselines.zip(ConcurrentPartitionCounts).foreach { case (baseline, numPartitions) =>
      assert(baseline.nonEmpty,
        s"the sort-based baseline of the $numPartitions partition shuffle must produce output")
    }

    sc = new SparkContext(resilientStreamingConf("streaming-shuffle-integration-concurrent"))
    TestUtils.waitUntilExecutorsUp(sc, ExecutorCount, ExecutorStartupTimeoutMillis)
    assertStreamingManagerInService()

    // The barrier is what makes the concurrency real: without it the first job would usually finish
    // before the last started, and a case meant to exercise contention would exercise nothing.
    val results = runConcurrently(
      ConcurrentShuffleCount,
      "streaming-shuffle-integration-concurrent",
      ConcurrentJobTimeoutMillis) { index =>
      groupedOutputAsSet(sc, ConcurrentPartitionCounts(index))
    }
    assert(results.size === ConcurrentShuffleCount,
      s"every one of the $ConcurrentShuffleCount concurrent jobs must have returned a result, " +
        s"but ${results.size} did")
    results.zip(ConcurrentPartitionCounts).zip(baselines).foreach {
      case ((observedOutput, numPartitions), baseline) =>
        assertNoDataLoss(observedOutput, baseline,
          s"one of $ConcurrentShuffleCount concurrent streaming shuffles, $numPartitions " +
            "partitions wide")
    }

    // Arbitration, driven deterministically over the same five widths. The coordinator is the
    // executor-scoped registry of active shuffles and it is the SOLE source of the token bucket's
    // divisor: no existing Spark API reports how many shuffles an executor is serving.
    val clock: ManualClock = newManualClock()
    val conf = streamingConfWithOverrides(maxBandwidthMBps = Some(ArbitrationBandwidthMBps))
    val coordinator = new StreamingShuffleCoordinator(sc.env.rpcEnv, conf, clock)
    val budget = TokenBucketRateLimiter.executorBudget(
      conf, clock, () => Some(coordinator.numConcurrentShuffles))
    val protocol = new BackpressureProtocol(conf, coordinator, budget, clock)
    try {
      assert(coordinator.activeShuffleIds.isEmpty, "the registry starts empty")
      assert(coordinator.numConcurrentShuffles === 1,
        "yet the count is clamped to one, which is what makes the bucket's division always safe")

      ConcurrentPartitionCounts.zipWithIndex.foreach { case (numPartitions, shuffleId) =>
        assert(coordinator.registerShuffle(
          shuffleId, numPartitions, ArbitrationNumMaps, ProtocolVersion).isDefined,
          s"shuffle $shuffleId must be admitted to the registry at the current wire revision")
        protocol.registerShuffle(shuffleId, numPartitions)
      }
      assert(coordinator.activeShuffleIds === ConcurrentPartitionCounts.indices,
        s"all $ConcurrentShuffleCount shuffles must be registered, but the registry holds " +
          s"${coordinator.activeShuffleIds.mkString(", ")}")
      assert(coordinator.numConcurrentShuffles === ConcurrentShuffleCount,
        s"the registry must report $ConcurrentShuffleCount concurrent shuffles")
      assert(protocol.numConcurrentShuffles === ConcurrentShuffleCount,
        "and the protocol must report the coordinator's count rather than inventing one")
      assert(budget.divisor === ConcurrentShuffleCount,
        s"$ConcurrentShuffleCount live limiters must divide the cap $ConcurrentShuffleCount ways")
      assert(budget.currentShareBytesPerSecond ===
        Some(refillBytesPerSecond(ArbitrationBandwidthMBps, ConcurrentShuffleCount)),
        s"each share must be the contract's ($BandwidthCeilingPercent percent of the cap) " +
          s"divided by $ConcurrentShuffleCount, but was " +
          s"${budget.currentShareBytesPerSecond}")
      assert(!budget.observeConcurrency(0),
        "a non-positive count is ignored outright rather than allowed to divide by zero")
      assert(budget.divisor === ConcurrentShuffleCount,
        "leaving the divisor at the coordinator's answer, which is its only source")

      // Utilisation is aggregated ACROSS the concurrent shuffles: the buffered bytes and the
      // budgets are summed independently and the percentage is taken from the two totals, so one
      // busy shuffle and four idle ones produce a single honest figure rather than five competing
      // ones.
      ConcurrentPartitionCounts.indices.foreach { shuffleId =>
        protocol.reportBufferUtilization(
          shuffleId, ArbitrationLowUtilizationBytes, ArbitrationBudgetBytes)
      }
      val lowPercent = ArbitrationLowUtilizationBytes * PercentScale / ArbitrationBudgetBytes
      assert(protocol.aggregateBufferUtilizationPercent === lowPercent,
        s"the aggregate over $ConcurrentShuffleCount equally loaded shuffles must be $lowPercent " +
          s"percent, but read ${protocol.aggregateBufferUtilizationPercent}")
      assert(!protocol.isSpillRequired,
        s"nothing may be asked to evict below the $DefaultSpillThresholdPercent percent threshold")
      ConcurrentPartitionCounts.indices.foreach { shuffleId =>
        protocol.reportBufferUtilization(
          shuffleId, ArbitrationHighUtilizationBytes, ArbitrationBudgetBytes)
      }
      val highPercent = ArbitrationHighUtilizationBytes * PercentScale / ArbitrationBudgetBytes
      assert(protocol.aggregateBufferUtilizationPercent === highPercent,
        s"the aggregate must follow every participating shuffle, but read " +
          s"${protocol.aggregateBufferUtilizationPercent} rather than $highPercent percent")
      assert(protocol.isSpillRequired,
        s"an executor at $highPercent percent of its allowance is over the threshold and " +
          "somebody must yield")

      // Pending volume, made to differ by width so that both arbitration keys are exercised: the
      // widest shuffle holds the most bytes, the narrowest the fewest.
      ConcurrentPartitionCounts.indices.foreach { shuffleId =>
        val key = BackpressureStreamKey.forProducer(
          shuffleId, ArbitrationMapId, ArbitrationTaskAttemptId, ArbitrationPartitionId)
        assert(protocol.registerStream(key, CreditLimitBytes),
          s"the credit ledger of shuffle $shuffleId must open")
        val pending = (shuffleId + 1).toLong * ArbitrationBlockBytes
        assert(protocol.tryAdmit(key, pending, 0L),
          s"shuffle $shuffleId must be admitted its block: the cap is generous and its credit is " +
            "untouched, so a refusal here would be a defect rather than backpressure")
        assert(protocol.outstandingBytes(key) === pending,
          s"shuffle $shuffleId must hold $pending unacknowledged byte(s), but holds " +
            s"${protocol.outstandingBytes(key)}")
      }

      val order = protocol.arbitrationOrder
      assert(order.size === ConcurrentShuffleCount,
        s"arbitration must rank every one of the $ConcurrentShuffleCount concurrent shuffles, " +
          s"but ranked ${order.size}")
      order.foreach { demand =>
        assert(demand.numPartitions === ConcurrentPartitionCounts(demand.shuffleId),
          s"the demand of shuffle ${demand.shuffleId} must carry its registered width " +
            s"${ConcurrentPartitionCounts(demand.shuffleId)}, but carried ${demand.numPartitions}")
        assert(demand.pendingBytes === (demand.shuffleId + 1).toLong * ArbitrationBlockBytes,
          s"the demand of shuffle ${demand.shuffleId} must carry its pending volume, but carried " +
            s"${demand.pendingBytes}")
        assert(demand.streamCount === 1,
          s"shuffle ${demand.shuffleId} registered one stream, but its demand reports " +
            s"${demand.streamCount}")
      }
      assert(protocol.yieldOrder === ConcurrentPartitionCounts.indices.reverse,
        "the order must be the most demanding first, keyed on pending volume and then on reduce " +
          s"partition count, but was ${protocol.yieldOrder.mkString(", ")}")

      // The anti-starvation rule, which is the whole point of arbitrating rather than evicting
      // arbitrarily: the least demanding shuffle is exempt, so a narrow shuffle sharing an executor
      // with a very wide one is never the one asked to give way and can always finish.
      val smallest = ConcurrentPartitionCounts.indices.head
      val largest = ConcurrentPartitionCounts.indices.last
      assert(protocol.guaranteedShuffleId === Some(smallest),
        s"the narrowest, least loaded shuffle $smallest must be the exempt one, but " +
          s"${protocol.guaranteedShuffleId} was")
      assert(!protocol.shouldYield(smallest),
        s"shuffle $smallest must never be asked to yield, or it could be taxed on every pass and " +
          "never finish")
      assert(protocol.shouldYield(largest),
        s"the widest, most loaded shuffle $largest must be the one asked to yield first")
    } finally {
      protocol.reset()
      coordinator.reset()
    }
  }
}

/**
 * Records what the scheduler did with a job, so that recovery can be asserted rather than assumed.
 *
 * Three things are captured because three different claims are made about them. Task failure
 * reasons say whether an injected fault was counted or silently swallowed. Fetch failures say
 * whether recovery took the one scheduler-facing signal this subsystem uses, and which shuffle it
 * named -- a fetch failure naming another shuffle would mean invalidation was not per-producer.
 * Stage submission counts bound the cost of recovery, which is what separates a recomputation from
 * a stage resubmitted until the attempt limit aborts the job.
 *
 * Spill volumes are accumulated from the task metrics the listener is handed, which is the read
 * path an operator's own tooling uses, so observing them here proves the streaming path reports
 * through Spark's existing accumulators and not through a channel of its own.
 *
 * Every accessor and every callback is synchronized: the listener bus delivers on its own thread
 * while the test body reads from the task thread.
 */
private class StreamingShuffleJobRecorder extends SparkListener {

  private val failures = new mutable.ArrayBuffer[TaskFailedReason]()

  private val fetches = new mutable.ArrayBuffer[FetchFailed]()

  private val submissions = mutable.Map.empty[Int, Int]

  private var tasksEnded: Long = 0L

  private var memorySpilledTotal: Long = 0L

  private var diskSpilledTotal: Long = 0L

  private var peakMemoryHighWater: Long = 0L

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = synchronized {
    val stageId = stageSubmitted.stageInfo.stageId
    submissions(stageId) = submissions.getOrElse(stageId, 0) + 1
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    tasksEnded += 1L
    val metrics: TaskMetrics = taskEnd.taskMetrics
    if (metrics != null) {
      memorySpilledTotal += metrics.memoryBytesSpilled
      diskSpilledTotal += metrics.diskBytesSpilled
      peakMemoryHighWater = math.max(peakMemoryHighWater, metrics.peakExecutionMemory)
    }
    taskEnd.reason match {
      case fetchFailed: FetchFailed =>
        fetches += fetchFailed
        failures += fetchFailed
      case failed: TaskFailedReason =>
        failures += failed
      case _ =>
    }
  }

  def countedFailures: Seq[TaskFailedReason] = synchronized(failures.toSeq)

  def fetchFailures: Seq[FetchFailed] = synchronized(fetches.toSeq)

  def maxStageSubmissions: Int = synchronized {
    if (submissions.isEmpty) 0 else submissions.values.max
  }

  def taskEndCount: Long = synchronized(tasksEnded)

  def memoryBytesSpilled: Long = synchronized(memorySpilledTotal)

  def diskBytesSpilled: Long = synchronized(diskSpilledTotal)

  def peakExecutionMemory: Long = synchronized(peakMemoryHighWater)
}
