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

import java.lang.management.{BufferPoolMXBean, ManagementFactory}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TestUtils}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT,
  SHUFFLE_STREAMING_ENABLED, SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS,
  SHUFFLE_STREAMING_SPILL_THRESHOLD}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

/**
 * Comparative benchmark of the streaming shuffle against the sort-based baseline it exists to beat.
 *
 * The feature names one reference workload -- a 100 MB `groupByKey` across ten partitions -- and
 * three acceptance targets against it: a 30 to 50 percent latency reduction, under 10 percent
 * memory overhead, and a spill rate under 5 percent. This object runs that workload twice, once on
 * sort-based shuffle and once on streaming shuffle, and emits a single comparative report covering
 * latency, memory, spill and bandwidth.
 *
 * ==It reports; it does not gate==
 *
 * Every figure is recorded for a human to judge, and nothing here enforces a threshold. That is
 * a decision, not an omission: this repository holds roughly ninety-four on-demand benchmark
 * classes and not one of them enforces a performance bound, because a percentage measured on a
 * loaded machine would fail for a reason that is not a defect. The acceptance targets are
 * printed beside the measurements so the judgement can be made, and no measurement can fail
 * the run.
 *
 * ==Why this is an object and not a suite==
 *
 * `BenchmarkBase` extends no test base class, so a benchmark cannot be a suite; it is an object
 * whose inherited `main` drives `runBenchmarkSuite`. The context is therefore built and stopped
 * here rather than by a suite fixture, and it is stopped in a `finally` because the harness's
 * `main` calls `afterAll` only on the path where nothing threw.
 *
 * ==Why both cases run on the same cluster master==
 *
 * Both cases run on `local-cluster[2,1,1024]`, and both on the SAME master, because two elapsed
 * times taken on different masters would be comparing the masters rather than the shuffles. A
 * local master would be worse than merely unfaithful: in local mode the executor skips shuffle
 * manager initialisation, because the driver's instance already exists, so one manager would serve
 * both the producer and the consumer role and no block would cross a JVM boundary. Two real
 * executor JVMs give the driver-registers / executor-resolves rendezvous and genuine
 * cross-executor transfer of retained output. The DAG scheduler remains unmodified and still starts
 * the reduce stage after the map stage finishes.
 *
 * ==What is measured, and where each figure comes from==
 *
 * Latency is wall-clock time taken around the job alone, so cluster start-up is charged to neither
 * case, and the harness times the same runs independently. Memory is sampled inside executor tasks:
 * full JVM heap, native buffer pools, and every category of the aggregate streaming quota are read
 * together. Spark's existing `TaskMetrics.peakExecutionMemory` is printed beside that full
 * footprint instead of being mistaken for it. Spill and bandwidth come from the existing task
 * accumulators read through a `SparkListener`, the same read path an operator's own tooling uses.
 * The four `shuffle.streaming` metrics are also sampled on the executors, and the registry backing
 * them is reset as the case switches so that each case reports its own reading.
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to
 *      "benchmarks/StreamingShufflePerformanceBenchmark-results.txt".
 *
 *   where <this class> is
 *   org.apache.spark.shuffle.streaming.StreamingShufflePerformanceBenchmark
 *
 *   An optional first argument overrides the master both cases run on, for use where executor
 *   processes cannot be started, for example
 *   build/sbt "core/Test/runMain <this class> local[8]"
 * }}}
 */
object StreamingShufflePerformanceBenchmark extends BenchmarkBase with StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  // ---------------------------------------------------------------------------------------------
  // The workload. Every value the feature names is taken from the shared fixtures rather than
  // restated, so this benchmark and the suites cannot drift apart on what "the reference workload"
  // means.
  // ---------------------------------------------------------------------------------------------

  /** The shuffle width the feature names: ten reduce partitions. */
  private val PartitionCount: Int = DefaultPartitionCount

  /** The dataset the feature names: one hundred mebibytes. */
  private val DatasetBytes: Long = TargetDatasetBytes

  /** Records the dataset carries, which is what the harness rates each case against. */
  private val RecordCount: Long =
    PartitionCount.toLong * recordsPerPartitionFor(PartitionCount, DatasetBytes).toLong

  /** Name of the scenario, printed by the harness above its comparison table. */
  private val ScenarioName: String = "Streaming shuffle versus sort-based shuffle"

  /** CPU-bound comparison that isolates shuffle coordination beneath deterministic compute. */
  private val CpuScenarioName: String = "CPU-bound shuffle coordination"

  /** Name of the comparison table, derived so it can never disagree with the workload above. */
  private val ComparisonName: String =
    s"groupByKey, ${DatasetBytes / BytesPerMebibyte} MB over $PartitionCount partitions"

  /** Work items and deterministic mixing rounds applied on both sides of the CPU-bound shuffle. */
  private val CpuWorkItems: Int = PartitionCount * 8192

  private val CpuMixRounds: Int = 64

  private val CpuComparisonName: String =
    s"deterministic CPU mix, $CpuWorkItems records over $PartitionCount partitions"

  // ---------------------------------------------------------------------------------------------
  // The environment. Both cases share one master, for the reason set out in this object's
  // documentation, and the shape below is the one the integration suite established for the very
  // same 100 MB measurement.
  // ---------------------------------------------------------------------------------------------

  /** Two real executor JVMs, one core and one gibibyte each. */
  private val ClusterMaster: String = "local-cluster[2,1,1024]"

  /** Prefix that identifies a master which starts genuine executor processes. */
  private val ClusterMasterPrefix: String = "local-cluster"

  /** Executors the cluster master promises, waited for so scheduling is never a race. */
  private val ExecutorCount: Int = 2

  /** How long an executor is given to register before the wait is abandoned. */
  private val ExecutorStartupTimeoutMillis: Long = 60000L

  /** How long the listener bus is given to deliver a run's events before the drain gives up. */
  private val ListenerDrainTimeoutMillis: Long = 60000L

  /**
   * Measured iterations per case, over and above the harness's unmeasured warm-up run.
   *
   * Three, matching the shuffle checksum benchmark, which is enough for a best-of to shed the worst
   * of the machine's own noise without turning an on-demand run into an overnight one.
   */
  private val MeasuredIterations: Int = 3

  /** Label of the sort-based case, which is the report's reference point. */
  private val BaselineCaseName: String = "sort-based shuffle (baseline)"

  /** Label of the streaming case. */
  private val StreamingCaseName: String = "streaming shuffle"

  /** Application name of the baseline context, so a failure names the case it came from. */
  private val BaselineAppName: String = "streaming-shuffle-benchmark-sort-baseline"

  /** Application name of the streaming context. */
  private val StreamingAppName: String = "streaming-shuffle-benchmark-streaming"

  private val CpuBaselineAppName: String = "streaming-shuffle-benchmark-cpu-sort"

  private val CpuStreamingAppName: String = "streaming-shuffle-benchmark-cpu-streaming"

  // ---------------------------------------------------------------------------------------------
  // The acceptance targets, and the report's own presentation constants. The latency window comes
  // from the shared fixtures; the other two targets exist nowhere else in the tree, so they are
  // named here rather than left as bare numbers inside a string.
  // ---------------------------------------------------------------------------------------------

  /** Ceiling of the reported memory overhead, as a whole percentage of the baseline. */
  private val MaxMemoryOverheadPercent: Int = 10

  /** Improvement window the CPU-bound acceptance target names. */
  private val MinCpuBoundImprovementPercent: Int = 5

  private val MaxCpuBoundImprovementPercent: Int = 10

  /**
   * The workload shapes memory overhead is reported at, narrowest first.
   *
   * <b>Why more than one.</b> The streaming path's buffer allowance is
   * `(executorMemory * bufferSizePercent) / numPartitions`, so a shuffle's width is the very
   * quantity the overhead depends on -- and the sort-based path's peak execution memory at a narrow
   * shape is close to nothing, which makes an overhead expressed as a percentage of it enormous
   * however small the absolute difference. One shape therefore cannot support a claim about memory
   * overhead in either direction: a favourable width would let compliance be overstated, and an
   * unfavourable one would report thousands of percent for a few mebibytes. Three widths, each with
   * its absolute figures printed beside its percentage, is what makes the reading honest.
   *
   * The reference width comes first in the acceptance verdict because it is the shape the feature
   * names; the other two are reported beside it, never instead of it.
   */
  private val NarrowPartitionCount: Int = 2

  private val WidePartitionCount: Int = 200

  /**
   * Dataset size the two additional shapes run at.
   *
   * A small fraction of the reference size, because what those shapes are measured for is the
   * relationship between shuffle width and buffer allowance rather than throughput at volume, and a
   * shape that took as long as the reference workload would double the cost of an on-demand
   * benchmark for information the reference shape already carries.
   */
  private val ShapeProbeBytes: Long = 8L * 1024L * 1024L

  /** Ceiling of the reported spill rate, as a whole percentage of the bytes the shuffle moved. */
  private val MaxSpillRatePercent: Int = 5

  /** Width the report pads its labels to, so the figures line up in a column. */
  private val LabelWidth: Int = 52

  /** Rule the report separates its sections with. */
  private val ReportRule: String = "-" * 96

  /** Heading the report opens with. */
  private val ReportHeading: String = "Streaming shuffle acceptance report"

  /** What the report prints when the live environment cannot name its shuffle manager. */
  private val UnknownManagerName: String = "unavailable"

  /** What the report prints for an absent bandwidth cap, which means uncapped and never zero. */
  private val UncappedBandwidth: String = "unset, which means uncapped egress"

  /** Milliseconds in a second, used by the bandwidth arithmetic. */
  private val MillisPerSecond: Long = TimeUnit.SECONDS.toMillis(1L)

  /** Tenths of a percent in a whole, the fixed-point scale every percentage is computed in. */
  private val TenthsPerWhole: Long = 1000L

  /** The reading a case carries before it has run, so no accessor ever answers with a null. */
  private val EmptyTelemetry: StreamingTelemetry =
    new StreamingTelemetry(0L, 0L, 0L, 0L, 0)

  private val EmptyMemoryFootprint: MemoryFootprint =
    new MemoryFootprint(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)

  /** Sampling interval for executor memory while grouped output is consumed. */
  private val MemorySampleInterval: Int = 256

  /** Representative CPU accounting shape for telemetry source-on/source-off comparison. */
  private val TelemetryCpuWorkItems: Int = 500000

  private val TelemetryCpuSampleInterval: Int = 4096

  private val TelemetryCpuSamples: Int = 5

  /**
   * Metric operations the source-on loop performs at each sampled event.
   *
   * One gauge read plus three counter increments. Named rather than inlined because the per-event
   * cost is the total divided by OPERATIONS, not by passes: dividing by passes would report the
   * cost of four operations under the label of one and understate it fourfold.
   */
  private val TelemetryOpsPerSampledEvent: Int = 4

  private val MaxTelemetryCpuOverheadPercent: Int = 1

  /** One representative producer contribution for source-on gauge reads. */
  private val TelemetryCpuContributor: StreamingShuffleBufferUtilizationContributor =
    new StreamingShuffleBufferUtilizationContributor {
      override def contributedBufferedBytes: Long = BytesPerMebibyte
      override def contributedBudgetBytes: Long = 4L * BytesPerMebibyte
    }

  /** Scope and availability of the executor-side streaming telemetry in this report. */
  private val TelemetryScopeNote: Seq[String] = Seq(
    "  Note: the four metrics are sampled inside the workload tasks and merged once per executor.",
    "  A zero spillCount is therefore an executor-side zero. If no executor sample is available,",
    "  pressure-spill attribution is printed as unavailable rather than as a misleading zero.")

  /**
   * What the ordinary stage-boundary comparison can attribute without claiming scheduler changes.
   *
   * The DAG scheduler remains an absolute preservation zone and submits a reduce stage only after
   * its map stage finishes. This benchmark therefore measures the implementation that exists:
   * framing, checksumming, retained-output publication and transport against sort, index
   * publication and ordinary block fetch. It reports the thirty to fifty percent project target
   * beside that measurement, but never explains a result using overlap the workload cannot have.
   */
  private val LatencyAttributionNote: Seq[String] = Seq(
    "  Note: the DAG scheduler is unmodified and starts reduce tasks after the map stage finishes.",
    "  This comparison measures framing, checksumming, retained-output publication and transport",
    "  against sort, index publication and ordinary block fetch. It claims no map/reduce overlap.")

  /** The standing reminder that this file measures and does not gate. */
  private val TargetsNote: Seq[String] = Seq(
    "  Note: every figure above is reported for a human to judge. This benchmark enforces no",
    "  threshold, in keeping with every other benchmark in this repository.")

  // ---------------------------------------------------------------------------------------------
  // Live state. Spark permits one context per JVM, so the two cases cannot hold one each; the
  // harness runs every iteration of one case before touching the next, which is what makes a single
  // cell sufficient.
  // ---------------------------------------------------------------------------------------------

  /** The context serving the case currently running, if one is up. */
  private var activeContext: Option[SparkContext] = None

  /** The case the active context was built for, so that a switch of case is detected. */
  private var activeCase: Option[CaseObservation] = None

  // ---------------------------------------------------------------------------------------------
  // The comparison.
  // ---------------------------------------------------------------------------------------------

  /**
   * Runs the whole comparison: the sort-based baseline, then streaming, then the report.
   *
   * The two cases are handed to the harness as ordinary cases, so the table it prints carries its
   * own best, mean, standard deviation, rate and relative columns for both. The report emitted
   * afterwards adds the dimensions the harness knows nothing about -- full memory, spill,
   * bandwidth, executor telemetry and telemetry CPU cost -- and restates latency beside the
   * acceptance windows.
   *
   * @param mainArgs program arguments; an optional first element overrides the master both cases
   *                 run on, which is what lets the comparison still be taken in an environment
   *                 that cannot start executor processes
   */
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val master = masterFrom(mainArgs)
    val baseline = new CaseObservation(
      BaselineCaseName, withLocalMaster(sortBaselineConf(), BaselineAppName, master), master)
    // BOTH keys, set through their typed entries by the shared fixture: spark.shuffle.manager
    // selects the manager class and spark.shuffle.streaming.enabled opens its behaviour gate. With
    // the gate left at its default of false the streaming manager would forward every call to the
    // sort-based manager it holds internally, and this benchmark would be comparing sort with sort.
    val streaming = new CaseObservation(
      StreamingCaseName, withLocalMaster(streamingConf(), StreamingAppName, master), master)
    val cpuBaseline = new CaseObservation(
      BaselineCaseName, withLocalMaster(sortBaselineConf(), CpuBaselineAppName, master), master)
    val cpuStreaming = new CaseObservation(
      StreamingCaseName, withLocalMaster(streamingConf(), CpuStreamingAppName, master), master)
    try {
      runBenchmark(ScenarioName) {
        val benchmark =
          new Benchmark(ComparisonName, RecordCount, MeasuredIterations, output = output)
        benchmark.addCase(BaselineCaseName) { _ =>
          runCase(baseline)
        }
        benchmark.addCase(StreamingCaseName) { _ =>
          runCase(streaming)
        }
        benchmark.run()
      }
      runBenchmark(CpuScenarioName) {
        val benchmark =
          new Benchmark(CpuComparisonName, CpuWorkItems.toLong, MeasuredIterations, output = output)
        benchmark.addCase(BaselineCaseName) { _ =>
          runCpuCase(cpuBaseline)
        }
        benchmark.addCase(StreamingCaseName) { _ =>
          runCpuCase(cpuStreaming)
        }
        benchmark.run()
      }
      stopActiveContext()
      val telemetryCpu = measureTelemetryCpuOverhead()
      emitReport(master, baseline, streaming, cpuBaseline, cpuStreaming, telemetryCpu)
    } finally {
      // Unconditional, so neither a failure in a case nor a failure while reporting can leave a
      // live context behind. The harness's own main reaches afterAll only when nothing threw.
      stopActiveContext()
    }
  }

  /**
   * Final safety net for the context, run by the harness once the suite has returned.
   *
   * Stopping is idempotent, so this costs nothing on the ordinary path and still covers a context
   * that some future path started outside the block above.
   */
  override def afterAll(): Unit = {
    stopActiveContext()
  }

  /**
   * The master both cases run on.
   *
   * Both take the same one: two elapsed times measured on different masters would be comparing the
   * masters. An override is honoured exactly as given, and a blank argument is treated as no
   * argument rather than as a request for a nameless master.
   *
   * @param mainArgs program arguments as the harness passed them
   * @return the master URL
   */
  private def masterFrom(mainArgs: Array[String]): String = {
    mainArgs.headOption.map(argument => argument.trim).filter(_.nonEmpty).getOrElse(ClusterMaster)
  }

  /**
   * Runs the reference workload once for a case and records what it cost.
   *
   * The context is obtained before the clock starts, so a case's first iteration does not charge
   * cluster start-up to the shuffle. Timing the job alone is also what makes this object's figures
   * agree with the harness's for every iteration rather than only for the measured ones, since the
   * iteration that pays for start-up is precisely the warm-up the harness discards.
   *
   * @param observation the case to run and to record against
   */
  private def runCase(observation: CaseObservation): Unit = {
    val context = contextFor(observation)
    observation.recorder.beginWindow()
    val startedAt = System.nanoTime()
    val workload = observeGroupedWorkload(groupedReferenceWorkload(context))
    val elapsedNanos = System.nanoTime() - startedAt
    // Task-end events arrive on the listener bus's own thread, so the bus is drained before the
    // accumulated figures are read; otherwise the tasks of the run just finished might not be in
    // them yet.
    drainListenerBus(context)
    val metrics = observation.recorder.finishWindow()
    observation.observeRun(elapsedNanos, workload, metrics)
    observation.observeShapeFootprint(shapeName(PartitionCount), workload.memory)
    if (observation.runCount == 1) {
      // Once per case, on the harness's warm-up iteration, and in this case's own context: closing
      // the recorder window after each shape prevents its task metrics from contaminating the
      // reference comparison. They are deliberately not timed -- the latency comparison is the
      // reference workload's alone, and adding shapes to it would compare different workloads.
      Seq(NarrowPartitionCount, WidePartitionCount).foreach { partitions =>
        observation.recorder.beginWindow()
        val shape = observeGroupedWorkload(
          largeDataset(context, partitions, ShapeProbeBytes).groupByKey(partitions))
        drainListenerBus(context)
        observation.recorder.finishWindow()
        observation.observeShapeFootprint(shapeName(partitions), shape.memory)
      }
    }
  }

  /** Runs the representative CPU-bound shuffle once and records its isolated task-metric window. */
  private def runCpuCase(observation: CaseObservation): Unit = {
    val context = contextFor(observation)
    observation.recorder.beginWindow()
    val startedAt = System.nanoTime()
    val workload = observeGroupedWorkload(cpuBoundWorkload(context))
    val elapsedNanos = System.nanoTime() - startedAt
    drainListenerBus(context)
    observation.observeRun(elapsedNanos, workload, observation.recorder.finishWindow())
  }

  /**
   * Consumes grouped output on the executors and returns compact executor-side observations.
   *
   * Sampling inside the result tasks makes both telemetry and full memory readings belong to the
   * executor JVMs that ran the streaming writer and reader rather than to the driver.
   */
  private def observeGroupedWorkload(grouped: RDD[_]): WorkloadObservation = {
    val observations = grouped.mapPartitions { records =>
      val sampler = new ExecutorMemorySampler
      var groups = 0L
      sampler.sample()
      while (records.hasNext) {
        records.next()
        groups += 1L
        if (groups % MemorySampleInterval.toLong == 0L) {
          sampler.sample()
        }
      }
      sampler.sample()
      Iterator.single(new ExecutorWorkloadObservation(
        SparkEnv.get.executorId, groups, telemetrySnapshot(), sampler.observed))
    }.collect().toSeq
    aggregateExecutorObservations(observations)
  }

  /** Reduces per-partition observations to one case reading without double-counting an executor. */
  private def aggregateExecutorObservations(
      observations: Seq[ExecutorWorkloadObservation]): WorkloadObservation = {
    val byExecutor = observations.groupBy(_.executorId)
    val telemetry = byExecutor.values.foldLeft(EmptyTelemetry) { (total, samples) =>
      total.plus(samples.map(_.telemetry).reduce(_.max(_)))
    }
    val memory = byExecutor.values.foldLeft(EmptyMemoryFootprint) { (total, samples) =>
      total.plus(samples.map(_.memory).reduce(_.max(_)))
    }
    new WorkloadObservation(observations.map(_.groupsProduced).sum, telemetry, memory)
  }

  /**
   * The label the report prints for one workload shape.
   *
   * @param partitions the shape's map and reduce width
   * @return the label, which is also the key a case's shape peaks are held under
   */
  private def shapeName(partitions: Int): String = {
    if (partitions == PartitionCount) {
      s"$partitions partitions, ${DatasetBytes / BytesPerMebibyte} MiB (reference)"
    } else {
      s"$partitions partitions, ${ShapeProbeBytes / BytesPerMebibyte} MiB"
    }
  }

  /**
   * The workload the feature names: one hundred mebibytes grouped by key across ten partitions.
   *
   * The dataset is generated on the executors from their own partition and record indices rather
   * than shipped from the driver, so a hundred mebibytes never enters the driver heap and the input
   * is byte-for-byte identical for both cases. Counting the groups is the action, because it forces
   * the whole shuffle -- every partition is read and every value deserialized -- without pulling
   * the result back to the driver.
   *
   * @param context the live context to build on
   * @return the grouped dataset, not yet computed
   */
  private def groupedReferenceWorkload(context: SparkContext): RDD[(Int, Iterable[String])] = {
    largeDataset(context, PartitionCount, DatasetBytes).groupByKey(PartitionCount)
  }

  /** CPU-heavy shuffle with identical deterministic compute on the sort and streaming paths. */
  private def cpuBoundWorkload(context: SparkContext): RDD[(Int, Long)] = {
    context.parallelize(0 until CpuWorkItems, PartitionCount)
      .map { value =>
        (value % PartitionCount, cpuMix(value.toLong, CpuMixRounds))
      }
      .groupByKey(PartitionCount)
      .mapValues { values =>
        values.iterator.foldLeft(0L) { (combined, value) =>
          combined ^ cpuMix(value, CpuMixRounds)
        }
      }
  }

  /** Deterministic integer mixing used to make the CPU-bound case substantial and reproducible. */
  private def cpuMix(seed: Long, rounds: Int): Long = {
    var mixed = seed ^ 0x9e3779b97f4a7c15L
    var round = 0
    while (round < rounds) {
      mixed ^= mixed >>> 30
      mixed *= 0xbf58476d1ce4e5b9L
      mixed ^= mixed >>> 27
      mixed *= 0x94d049bb133111ebL
      mixed ^= mixed >>> 31
      round += 1
    }
    mixed
  }

  // ---------------------------------------------------------------------------------------------
  // Context lifecycle.
  // ---------------------------------------------------------------------------------------------

  /**
   * The context for a case, started on first use and reused for that case's later iterations.
   *
   * Swapping happens here rather than around `benchmark.run()` because the harness owns the loop:
   * it runs every iteration of one case before touching the next, so the first iteration of a case
   * is exactly the moment its context is needed and the previous case's is not. Reuse matters for
   * the measurement, not merely for the clock: a context that stays up keeps its executors warm, so
   * the measured iterations compare two shuffles rather than two cluster start-ups.
   *
   * @param observation the case whose context is wanted
   * @return a live context configured for that case
   */
  private def contextFor(observation: CaseObservation): SparkContext = {
    activeContext match {
      case Some(context) if activeCase.exists(current => current eq observation) => context
      case _ =>
        stopActiveContext()
        // The metrics source is a JVM singleton, so its counters carry from one case into the next.
        // Resetting at the switch is what makes each case's telemetry a reading of its own.
        resetStreamingShuffleMetrics()
        val context = new SparkContext(observation.conf)
        context.addSparkListener(observation.recorder)
        awaitExecutors(context, observation.master)
        observation.recordManagerInService(shuffleManagerClassName())
        activeContext = Some(context)
        activeCase = Some(observation)
        context
    }
  }

  /**
   * Waits for the executors a cluster master promises, so scheduling is never a race.
   *
   * Guarded on the master because the wait counts registered executors against the driver, and a
   * local master registers none: applying it there would time out after a minute for a reason that
   * has nothing to do with the shuffle.
   *
   * @param context the context that has just been started
   * @param master the master it was started on
   */
  private def awaitExecutors(context: SparkContext, master: String): Unit = {
    if (master.startsWith(ClusterMasterPrefix)) {
      TestUtils.waitUntilExecutorsUp(context, ExecutorCount, ExecutorStartupTimeoutMillis)
    }
  }

  /**
   * Stops whatever context is live and forgets it, leaving the JVM free to start the next one.
   *
   * Idempotent, so it is safe from the completion path and the failure path alike. The listener is
   * deliberately not detached first: stopping the context flushes its bus, and detaching before the
   * flush would discard the very task-end events the last run's figures are drawn from.
   */
  private def stopActiveContext(): Unit = {
    activeContext.foreach(context => context.stop())
    activeContext = None
    activeCase = None
  }

  /**
   * Waits for the listener bus to deliver everything a run queued.
   *
   * A timeout is allowed to fail the run. Reporting figures from a listener window that did not
   * finish draining would present incomplete task metrics as a measurement, which is less useful
   * than stopping with the actual cause.
   *
   * @param context the context whose bus is drained
   */
  private def drainListenerBus(context: SparkContext): Unit = {
    context.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
  }

  /**
   * Simple name of the shuffle manager the live driver environment holds.
   *
   * Reported so that the comparison is evidence about the manager which actually served the run,
   * rather than about the configuration that asked for one: a streaming case whose behaviour gate
   * was closed would delegate every call to the sort-based manager and would otherwise be
   * indistinguishable, in the report, from a genuine streaming measurement.
   *
   * @return the manager's simple class name, or a placeholder when no environment can name one
   */
  private def shuffleManagerClassName(): String = {
    val env = SparkEnv.get
    if (env == null || env.shuffleManager == null) {
      UnknownManagerName
    } else {
      env.shuffleManager.getClass.getSimpleName
    }
  }

  /**
   * The four `shuffle.streaming` metrics as they stand, read through the metrics registry.
   *
   * Taken inside each result task rather than once on the driver, because every executor owns its
   * own registry and the case switch resets only the current JVM. The three counters are cumulative
   * across the case. The utilisation gauge is computed when read, so each result partition samples
   * it while that executor is still serving or consuming the workload.
   *
   * @return the snapshot
   */
  private def telemetrySnapshot(): StreamingTelemetry = {
    new StreamingTelemetry(
      observedBufferUtilizationPercent(),
      observedSpillCount(),
      observedBackpressureEvents(),
      observedPartialReadInvalidations(),
      1)
  }

  /** Measures source-on/source-off CPU cost without putting a threshold in the run. */
  private def measureTelemetryCpuOverhead(): TelemetryCpuObservation = {
    measuredTelemetryCpuSample(sourceEnabled = false)
    measuredTelemetryCpuSample(sourceEnabled = true)
    resetStreamingShuffleMetrics()
    val disabledSamples = new mutable.ArrayBuffer[Long]()
    val enabledSamples = new mutable.ArrayBuffer[Long]()
    var checksum = 0L
    var sample = 0
    var available = true
    while (sample < TelemetryCpuSamples && available) {
      val ordered = if (sample % 2 == 0) {
        Seq(false, true)
      } else {
        Seq(true, false)
      }
      ordered.foreach { enabled =>
        measuredTelemetryCpuSample(enabled) match {
          case Some((elapsed, observed)) =>
            if (enabled) enabledSamples += elapsed else disabledSamples += elapsed
            checksum ^= observed
          case None =>
            available = false
        }
      }
      resetStreamingShuffleMetrics()
      sample += 1
    }
    if (available && disabledSamples.nonEmpty && enabledSamples.nonEmpty) {
      new TelemetryCpuObservation(
        Some(medianLong(disabledSamples.toSeq)),
        Some(medianLong(enabledSamples.toSeq)),
        checksum)
    } else {
      new TelemetryCpuObservation(None, None, checksum)
    }
  }

  /** One CPU sample with representative gauge ownership installed outside the measured window. */
  private def measuredTelemetryCpuSample(sourceEnabled: Boolean): Option[(Long, Long)] = {
    if (sourceEnabled) {
      StreamingShuffleMetricsSource.registerBufferUtilizationContributor(TelemetryCpuContributor)
    }
    try {
      measuredThreadCpuNanos(telemetryCpuLoop(sourceEnabled))
    } finally {
      if (sourceEnabled) {
        StreamingShuffleMetricsSource.unregisterBufferUtilizationContributor(
          TelemetryCpuContributor)
      }
    }
  }

  /** Representative CPU work with sparse metric updates matching the source's event-level use. */
  private def telemetryCpuLoop(sourceEnabled: Boolean): Long = {
    var item = 0
    var checksum = 0L
    var localEvents = 0L
    while (item < TelemetryCpuWorkItems) {
      checksum ^= cpuMix(item.toLong, CpuMixRounds)
      if (item % TelemetryCpuSampleInterval == 0) {
        localEvents += 1L
        if (sourceEnabled) {
          checksum ^= StreamingShuffleMetricsSource.bufferUtilizationPercent
          StreamingShuffleMetricsSource.incrementSpillCount(1L)
          StreamingShuffleMetricsSource.incrementBackpressureEvents(1L)
          StreamingShuffleMetricsSource.incrementPartialReadInvalidations(1L)
        }
      }
      item += 1
    }
    checksum ^ localEvents
  }

  /** Current-thread CPU time around one body, or unavailable when the JVM cannot expose it. */
  private def measuredThreadCpuNanos(body: => Long): Option[(Long, Long)] = {
    try {
      val bean = ManagementFactory.getThreadMXBean
      if (!bean.isCurrentThreadCpuTimeSupported) {
        None
      } else {
        if (!bean.isThreadCpuTimeEnabled) {
          bean.setThreadCpuTimeEnabled(true)
        }
        val startedAt = bean.getCurrentThreadCpuTime
        val result = body
        val elapsed = bean.getCurrentThreadCpuTime - startedAt
        if (startedAt < 0L || elapsed <= 0L) None else Some(elapsed -> result)
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  /**
   * Metric operations the source-on loop priced: the per-operation denominator.
   *
   * @return gauge reads plus counter increments performed across the whole loop
   */
  private def telemetryOperationCount: Long = {
    val sampledEvents =
      (TelemetryCpuWorkItems + TelemetryCpuSampleInterval - 1) / TelemetryCpuSampleInterval
    sampledEvents.toLong * TelemetryOpsPerSampledEvent.toLong
  }

  /**
   * Cost of one metric operation, in nanoseconds, from the source-on/source-off difference.
   *
   * @param differenceNanos source-on CPU time less source-off CPU time
   * @return nanoseconds per metric operation, floored at zero because a negative difference means
   *         the two arms were indistinguishable rather than that telemetry saved time
   */
  private def telemetryNanosPerOperation(differenceNanos: Long): Long = {
    val operations = telemetryOperationCount
    if (operations <= 0L || differenceNanos <= 0L) 0L else differenceNanos / operations
  }

  /** Lower-median reading, used so one noisy CPU sample cannot dominate the report. */
  private def medianLong(values: Seq[Long]): Long = {
    val ordered = values.sorted
    ordered((ordered.size - 1) / 2)
  }


  // ---------------------------------------------------------------------------------------------
  // The report. One section per dimension, so that each is readable on its own and a reader can
  // find the figure they came for without reading the rest.
  // ---------------------------------------------------------------------------------------------

  /**
   * Emits the comparative report over the harness's result stream and to the console.
   *
   * @param master the master both cases ran on
   * @param baseline the sort-based case
   * @param streaming the streaming case
   * @param cpuBaseline sort-based CPU-bound case
   * @param cpuStreaming streaming CPU-bound case
   * @param telemetryCpu source-on/source-off CPU accounting
   */
  private def emitReport(
      master: String,
      baseline: CaseObservation,
      streaming: CaseObservation,
      cpuBaseline: CaseObservation,
      cpuStreaming: CaseObservation,
      telemetryCpu: TelemetryCpuObservation): Unit = {
    emit(
      workloadSection(master, baseline, streaming) ++
        activationSection(baseline, streaming) ++
        latencySection(baseline, streaming) ++
        cpuBoundSection(cpuBaseline, cpuStreaming) ++
        memorySection(baseline, streaming) ++
        spillSection(baseline, streaming) ++
        writeAmplificationSection(baseline, streaming) ++
        bandwidthSection(baseline, streaming) ++
        telemetrySection(baseline, streaming) ++
        telemetryCpuSection(telemetryCpu) ++
        Seq(ReportRule))
  }

  /**
   * What was run, how much of it, and how many times.
   *
   * @param master the master both cases ran on
   * @param baseline the sort-based case
   * @param streaming the streaming case
   * @return the section's lines
   */
  private def workloadSection(
      master: String,
      baseline: CaseObservation,
      streaming: CaseObservation): Seq[String] = {
    Seq(
      "",
      ReportRule,
      ReportHeading,
      ReportRule,
      row("workload", ComparisonName),
      row("dataset bytes", s"$DatasetBytes across $PartitionCount partitions"),
      row("records", s"$RecordCount, about $RecordValueLength bytes of value each"),
      row("master, shared by both cases", master),
      row("runs recorded, warm-up included",
        s"${baseline.runCount} baseline, ${streaming.runCount} streaming"),
      row("groups produced per run",
        s"${baseline.groupsProduced} baseline, ${streaming.groupsProduced} streaming"))
  }

  /**
   * What each case actually asked Spark for, read back through the typed configuration entries.
   *
   * The two-tier activation model is why this is worth printing. `spark.shuffle.manager` selects
   * which manager class is instantiated and `spark.shuffle.streaming.enabled` gates that class's
   * behaviour, so a streaming case whose gate was left at its default of false would delegate every
   * service-provider call to the sort-based manager and the comparison would silently be sort
   * against sort. Both keys are shown, together with the manager class the live driver environment
   * held once each context was up, so the reader can see the activation rather than assume it.
   *
   * The bandwidth entry is optional and its ABSENCE is the unlimited state, never zero, so it is
   * rendered as such instead of being shown as a number that would misdescribe it.
   *
   * @param baseline the sort-based case
   * @param streaming the streaming case
   * @return the section's lines
   */
  private def activationSection(
      baseline: CaseObservation,
      streaming: CaseObservation): Seq[String] = {
    val streamingConfiguration = streaming.conf
    Seq(
      "",
      "Activation, baseline / streaming",
      row(SHUFFLE_MANAGER.key,
        s"${baseline.conf.get(SHUFFLE_MANAGER)} / ${streamingConfiguration.get(SHUFFLE_MANAGER)}"),
      row(SHUFFLE_STREAMING_ENABLED.key,
        s"${baseline.conf.get(SHUFFLE_STREAMING_ENABLED)} / " +
          s"${streamingConfiguration.get(SHUFFLE_STREAMING_ENABLED)}"),
      row(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT.key,
        s"${streamingConfiguration.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)} percent of " +
          "executor memory"),
      row(SHUFFLE_STREAMING_SPILL_THRESHOLD.key,
        s"${streamingConfiguration.get(SHUFFLE_STREAMING_SPILL_THRESHOLD)} percent utilisation"),
      row(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS.key,
        streamingConfiguration.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)
          .map(cap => s"$cap MB/s declared link capacity")
          .getOrElse(UncappedBandwidth)),
      row("shuffle manager in service",
        s"${baseline.managerInService} / ${streaming.managerInService}"))
  }

  /**
   * Latency, and the reduction the feature is judged on.
   *
   * The best run of each case is compared rather than the mean, because the best is the run least
   * polluted by whatever else the machine was doing, and both cases are given the same treatment.
   *
   * @param baseline the sort-based case
   * @param streaming the streaming case
   * @return the section's lines
   */
  private def latencySection(
      baseline: CaseObservation,
      streaming: CaseObservation): Seq[String] = {
    val reduction = reductionTenths(baseline.bestNanos, streaming.bestNanos)
    val meanReduction = reductionTenths(baseline.meanNanos, streaming.meanNanos)
    val worstReduction = reductionTenths(baseline.worstNanos, streaming.worstNanos)
    Seq(
      "",
      "Latency, wall clock around the job alone, cluster start-up excluded",
      row(baseline.caseName, elapsedDescription(baseline)),
      row(streaming.caseName, elapsedDescription(streaming)),
      row("reduction on best time", s"${renderTenths(reduction)} percent"),
      row("reduction on mean time", s"${renderTenths(meanReduction)} percent"),
      row("reduction on worst time", s"${renderTenths(worstReduction)} percent"),
      row("every run, sort-based", runSamples(baseline)),
      row("every run, streaming", runSamples(streaming)),
      row("acceptance target, NOT MEASURED HERE",
        s"$MinLatencyReductionPercent to $MaxLatencyReductionPercent percent reduction")) ++
      LatencyAttributionNote
  }

  /**
   * CPU-bound comparison, with identical deterministic compute wrapped around both shuffle paths.
   *
   * The same work items and mixing rounds are used in both cases, so the elapsed-time difference
   * isolates the scheduler and shuffle-coordination work left once deterministic application CPU is
   * held constant. Like every other section, this reports the target and does not enforce it.
   *
   * @param baseline the sort-based CPU-bound case
   * @param streaming the streaming CPU-bound case
   * @return the section's lines
   */
  private def cpuBoundSection(
      baseline: CaseObservation,
      streaming: CaseObservation): Seq[String] = {
    val bestImprovement = reductionTenths(baseline.bestNanos, streaming.bestNanos)
    val meanImprovement = reductionTenths(baseline.meanNanos, streaming.meanNanos)
    Seq(
      "",
      "CPU-bound workload, deterministic compute plus shuffle coordination",
      row("workload", CpuComparisonName),
      row(baseline.caseName, elapsedDescription(baseline)),
      row(streaming.caseName, elapsedDescription(streaming)),
      row("improvement on best time", s"${renderTenths(bestImprovement)} percent"),
      row("improvement on mean time", s"${renderTenths(meanImprovement)} percent"),
      row("every run, sort-based", runSamples(baseline)),
      row("every run, streaming", runSamples(streaming)),
      row("acceptance target",
        s"$MinCpuBoundImprovementPercent to $MaxCpuBoundImprovementPercent percent improvement"),
      row("read this way", "both paths execute the same integer mixing before and after"),
      row("", "the shuffle, so the difference is coordination beneath fixed CPU work"))
  }

  /**
   * Every run of a case as a list of milliseconds, in the order the runs happened.
   *
   * Printed because a best and a mean describe a distribution only if its width is visible. A
   * reduction that fell short across every run and one that fell short because a single run was
   * slow call for different responses, and only the samples distinguish them.
   *
   * @param observation the case whose runs are listed
   * @return the runs, comma separated, in milliseconds
   */
  private def runSamples(observation: CaseObservation): String = {
    val samples = observation.elapsedNanosSamples.map(nanos => millisOf(nanos).toString)
    if (samples.isEmpty) "no run recorded" else s"${samples.mkString(", ")} ms"
  }

  /**
   * Full executor memory, sampled in the tasks that consume each workload.
   *
   * Heap and native buffer-pool use are sampled together, then merged as a high-water mark per
   * executor and summed across represented executors. The streaming quota and its four ownership
   * categories are reported beside that footprint but are not added to it: those bytes already
   * live in heap or native memory. Spark's task execution-memory peak remains useful, so it is
   * printed as a separate, deliberately narrower reading.
   *
   * @param baseline the sort-based case
   * @param streaming the streaming case
   * @return the section's lines
   */
  private def memorySection(
      baseline: CaseObservation,
      streaming: CaseObservation): Seq[String] = {
    val baselineMemory = baseline.memoryFootprint
    val streamingMemory = streaming.memoryFootprint
    val referenceShape = shapeName(PartitionCount)
    val shapes = streaming.measuredShapes.filter { shape =>
      baseline.shapeFootprint(shape).totalBytes > 0L ||
        streaming.shapeFootprint(shape).totalBytes > 0L
    }
    val components = Seq(
      "heap used" -> (baselineMemory.heapBytes, streamingMemory.heapBytes),
      "native buffer pools" -> (baselineMemory.nativeBytes, streamingMemory.nativeBytes),
      "aggregate streaming quota" ->
        (baselineMemory.aggregateQuotaBytes, streamingMemory.aggregateQuotaBytes),
      "quota: producer" -> (baselineMemory.producerBytes, streamingMemory.producerBytes),
      "quota: consumer" -> (baselineMemory.consumerBytes, streamingMemory.consumerBytes),
      "quota: transient" -> (baselineMemory.transientBytes, streamingMemory.transientBytes),
      "quota: metadata" -> (baselineMemory.metadataBytes, streamingMemory.metadataBytes))
    val componentRows = components.flatMap { case (label, (baselineBytes, streamingBytes)) =>
      Seq(
        row(s"  $label", s"$baselineBytes / $streamingBytes bytes"),
        row("    overhead", memoryOverheadDescription(baselineBytes, streamingBytes)))
    }
    val perShape = shapes.flatMap { shape =>
      val shapeBaseline = baseline.shapeFootprint(shape)
      val shapeStreaming = streaming.shapeFootprint(shape)
      Seq(
        row(s"  $shape",
          s"${shapeBaseline.totalBytes} / ${shapeStreaming.totalBytes} bytes"),
        row("    overhead",
          memoryOverheadDescription(shapeBaseline.totalBytes, shapeStreaming.totalBytes)),
        row("    streaming quota", quotaDescription(shapeStreaming)))
    }
    Seq(
      "",
      "Memory, executor-side heap plus native buffer-pool high-water marks",
      row(baseline.caseName, footprintDescription(baselineMemory)),
      row(streaming.caseName, footprintDescription(streamingMemory)),
      row("full-footprint overhead",
        memoryOverheadDescription(baselineMemory.totalBytes, streamingMemory.totalBytes)),
      row("TaskMetrics peakExecutionMemory",
        s"${baseline.peakExecutionMemory} / ${streaming.peakExecutionMemory} bytes"),
      row("acceptance target", s"under $MaxMemoryOverheadPercent percent full-footprint overhead"),
      "  component high-water marks, baseline / streaming:") ++
      componentRows ++
      Seq("  by workload shape, baseline / streaming:") ++
      perShape ++
      Seq(
        row("  verdict shape", referenceShape),
        row("  read this way", "heap plus native is the full footprint; quota rows are an"),
        row("", "ownership breakdown already included in that footprint, not extra bytes"),
        row("", "to add again. Shape probes use isolated task-metric windows."))
  }

  /** One full executor footprint rendered without hiding its heap and native constituents. */
  private def footprintDescription(footprint: MemoryFootprint): String = {
    s"${footprint.totalBytes} bytes = ${footprint.heapBytes} heap + " +
      s"${footprint.nativeBytes} native"
  }

  /** Aggregate quota and its independently sampled ownership-category high-water marks. */
  private def quotaDescription(footprint: MemoryFootprint): String = {
    s"${footprint.aggregateQuotaBytes} aggregate; producer ${footprint.producerBytes}, " +
      s"consumer ${footprint.consumerBytes}, transient ${footprint.transientBytes}, " +
      s"metadata ${footprint.metadataBytes} bytes"
  }

  /** Relative and absolute memory change, with an explicit zero-baseline interpretation. */
  private def memoryOverheadDescription(baselineBytes: Long, streamingBytes: Long): String = {
    val difference = streamingBytes - baselineBytes
    if (baselineBytes <= 0L) {
      s"baseline zero; $difference bytes absolute"
    } else {
      val overhead = percentTenths(difference, baselineBytes)
      s"${renderTenths(overhead)} percent, $difference bytes absolute"
    }
  }

  /**
   * Spill, taken from Spark's own spill accumulators and from no counter of this feature's own,
   * reported split by cause.
   *
   * <b>Why one rate was not enough, and why this is a reporting correction rather than a softened
   * target.</b> Disk bytes over shuffle bytes written answers "how much of what we moved went
   * through the disk", and on the streaming path the honest answer is "most of it" -- but almost
   * none of that is spill in the sense the acceptance target means. Two different things reach
   * `diskBytesSpilled`:
   *
   *  - '''Spill under pressure.''' Buffer utilisation met the configured threshold, or an
   * allocation
   *    needed room, so resident blocks were evicted. This is the condition the under-five-percent
   *    target is about, and `shuffle.streaming.spillCount` counts exactly its events.
   *  - '''The end-of-stream durability flush.''' Every successful streaming map task ends by making
   *    its still-unacknowledged output durable, because the unmodified DAG scheduler submits no
   *    reduce task until the map stage has finished: the consumers of that output do not exist yet,
   *    and the executor-scoped block resolver serves them from spilled segments once they do. Those
   *    bytes are the streaming path's counterpart to the shuffle files the sort-based path writes
   * for
   *    exactly the same reason -- and the sort path's own write is accounted as
   *    `shuffleBytesWritten`, never as spill, which is why the baseline reads zero here while
   * having
   *    written every byte of its output to the same disk. Comparing the two totals as though they
   *    measured the same thing is a category error, and it is the reason a single rate reported
   *    sixty-three percent for a run in which nothing was ever under memory pressure.
   *
   * The combined rate is still reported first and unchanged, because it is the true cost of the
   * disk on this path and an operator sizing local storage needs it. What is added is the
   * attribution, so that the figure the target names can be read off rather than inferred -- and so
   * that a `spillCount` of zero beside a large disk volume reads as the explanation it is instead
   * of as a contradiction.
   *
   * @param baseline the sort-based case
   * @param streaming the streaming case
   * @return the section's lines
   */
  private def spillSection(
      baseline: CaseObservation,
      streaming: CaseObservation): Seq[String] = {
    Seq(
      "",
      "Spill, from TaskMetrics memoryBytesSpilled and diskBytesSpilled",
      row(baseline.caseName, spillDescription(baseline)),
      row(streaming.caseName, spillDescription(streaming)),
      row("disk bytes over shuffle bytes written, all causes",
        s"${renderTenths(spillRateTenths(baseline))} percent / " +
          s"${renderTenths(spillRateTenths(streaming))} percent"),
      row("threshold-driven spill events (spillCount)",
        s"${spillCountDescription(baseline)} / ${spillCountDescription(streaming)}"),
      row("spill rate under pressure, the target's figure",
        s"${pressureSpillRateDescription(baseline)} / " +
          pressureSpillRateDescription(streaming)),
      row("acceptance target", s"under $MaxSpillRatePercent percent under pressure")) ++
      spillAttribution(streaming)
  }

  /**
   * The note that explains which of the two rates above the reader should act on.
   *
   * Conditional, because the two cases it distinguishes call for different readings and a note that
   * covered both would say neither. With no threshold-driven event the whole disk volume is the
   * durability flush and there is nothing to tune; with events present the volume is a mixture this
   * benchmark cannot split further, and saying so is more useful than implying it can.
   *
   * @param streaming the streaming case
   * @return the note's lines
   */
  private def spillAttribution(streaming: CaseObservation): Seq[String] = {
    if (!streaming.telemetry.available) {
      Seq(
        row("attribution",
          "unavailable: no executor telemetry sample was returned by the workload."),
        row("", "TaskMetrics disk volume remains valid, but a driver-local zero is not"),
        row("", "substituted for the missing executor spill-event attribution."))
    } else if (streaming.telemetry.spillCount == 0L && streaming.diskBytesSpilled > 0L) {
      Seq(
        row("attribution",
          "no threshold-driven spill event occurred, so every disk byte above is the"),
        row("", "end-of-stream durability flush of retained output. That flush is what lets"),
        row("", "reduce tasks read this output at all, since the unmodified scheduler starts"),
        row("", "them only after the map stage has finished, and it is the counterpart of the"),
        row("", "shuffle files the sort case wrote and reported as shuffleBytesWritten."))
    } else if (streaming.telemetry.spillCount > 0L) {
      Seq(
        row("attribution",
          s"${streaming.telemetry.spillCount} threshold-driven spill event(s) occurred, so the"),
        row("", "disk volume above mixes memory pressure with the end-of-stream durability"),
        row("", "flush. The pressure rate is bounded above by the combined rate; the four"),
        row("", "streaming metrics below carry the event counts."))
    } else {
      Seq(row("attribution", "no disk bytes were written on either path."))
    }
  }

  /** Executor-side spill-event count, never a driver-local zero when no sample was returned. */
  private def spillCountDescription(observation: CaseObservation): String = {
    if (observation.telemetry.available) observation.telemetry.spillCount.toString
    else "unavailable"
  }

  /** Pressure-spill rate qualified by whether executor-side event attribution was available. */
  private def pressureSpillRateDescription(observation: CaseObservation): String = {
    pressureSpillRateTenths(observation)
      .map(tenths => s"${renderTenths(tenths)} percent")
      .getOrElse("unavailable")
  }

  /**
   * Write amplification: records the producing side wrote against records the consuming side read.
   *
   * <b>Why this belongs in the report.</b> A shuffle abandoned part way through and produced again
   * costs its records twice, and nothing else in this report would show it: the latency section
   * would simply read slower, and the spill section would read larger, with no indication that the
   * extra work was the same work done twice. Records written against records read is the direct
   * reading, and it is taken from the very reporters the sort-based path populates, so both cases
   * are measured the same way.
   *
   * On a healthy run of either path the two figures agree exactly: every record written is read
   * once. Written above read means some producer's output was discarded and produced again -- a
   * retried map task, or a stage the unmodified scheduler recomputed after a fetch failure. Read
   * above written would mean a reduce task ran more than once over the same output, which a retried
   * reduce task does.
   *
   * @param baseline the sort-based case
   * @param streaming the streaming case
   * @return the section's lines
   */
  private def writeAmplificationSection(
      baseline: CaseObservation,
      streaming: CaseObservation): Seq[String] = {
    Seq(
      "",
      "Write amplification, from the shuffle write and read record reporters",
      row(baseline.caseName, amplificationDescription(baseline)),
      row(streaming.caseName, amplificationDescription(streaming)),
      row("read this way", "written and read agree exactly on a run in which nothing was"),
      row("", "produced twice; written above read is output discarded and produced"),
      row("", "again, by a retried map task or a recomputed stage"))
  }

  /**
   * One case's records written, records read and the amplification between them.
   *
   * @param observation the case to describe
   * @return the description, records and percentage together
   */
  private def amplificationDescription(observation: CaseObservation): String = {
    val written = observation.shuffleRecordsWritten
    val read = observation.shuffleRecordsRead
    val amplification = percentTenths(written - read, read)
    s"$written written, $read read, ${renderTenths(amplification)} percent amplification"
  }

  /**
   * Bandwidth, derived from the REMOTE shuffle bytes the accumulators report over the best run.
   *
   * <b>Why remote bytes and not total.</b> `ShuffleReadMetrics.totalBytesRead` is the sum of local
   * and remote bytes, and a local read never touches a link -- it is a file the same executor
   * wrote, or a buffer the same executor holds. Labelling that sum "bandwidth" overstates the
   * link's load by however much of the shuffle stayed on one host, which on a two-executor cluster
   * is a large fraction and on a `local` master is all of it, where a "bandwidth" figure would be
   * reported for a run that put nothing on any wire at all. `remoteBytesRead` is the part that
   * crossed a link, so it is the only part a bandwidth figure may be computed from. Both are
   * printed, so the local share is visible rather than folded away.
   *
   * Byte totals are divided by the number of runs recorded before the rate is taken, because the
   * accumulators are cumulative over every run of a case while the elapsed time is one run's.
   *
   * The byte figures are what the accumulators report, which is bytes after shuffle compression,
   * and that is the right basis for a bandwidth figure: what a link carries is the compressed
   * stream, not the dataset it was built from.
   *
   * @param baseline the sort-based case
   * @param streaming the streaming case
   * @return the section's lines
   */
  private def bandwidthSection(
      baseline: CaseObservation,
      streaming: CaseObservation): Seq[String] = {
    Seq(
      "",
      "Bandwidth, REMOTE shuffle bytes per second of the best run",
      row(baseline.caseName, bandwidthDescription(baseline)),
      row(streaming.caseName, bandwidthDescription(streaming)),
      row("shuffle bytes written per run",
        s"${perRun(baseline.shuffleBytesWritten, baseline.runCount)} / " +
          s"${perRun(streaming.shuffleBytesWritten, streaming.runCount)}"),
      row("remote bytes read per run, the wire figure",
        s"${perRun(baseline.shuffleRemoteBytesRead, baseline.runCount)} / " +
          s"${perRun(streaming.shuffleRemoteBytesRead, streaming.runCount)}"),
      row("local plus remote read per run, NOT the wire",
        s"${perRun(baseline.shuffleTotalBytesRead, baseline.runCount)} / " +
          s"${perRun(streaming.shuffleTotalBytesRead, streaming.runCount)}"),
      row("shuffle records written per run",
        s"${perRun(baseline.shuffleRecordsWritten, baseline.runCount)} / " +
          s"${perRun(streaming.shuffleRecordsWritten, streaming.runCount)}"),
      row("shuffle records read per run",
        s"${perRun(baseline.shuffleRecordsRead, baseline.runCount)} / " +
          s"${perRun(streaming.shuffleRecordsRead, streaming.runCount)}"),
      row("fetch wait time per run, ms",
        s"${perRun(baseline.fetchWaitTime, baseline.runCount)} / " +
          s"${perRun(streaming.fetchWaitTime, streaming.runCount)}"))
  }

  /**
   * The four `shuffle.streaming` metrics, plus the bookkeeping that qualifies the figures above.
   *
   * @param baseline the sort-based case
   * @param streaming the streaming case
   * @return the section's lines
   */
  private def telemetrySection(
      baseline: CaseObservation,
      streaming: CaseObservation): Seq[String] = {
    val baselineTelemetry = baseline.telemetry
    val streamingTelemetry = streaming.telemetry
    Seq(
      "",
      "Streaming telemetry, the four shuffle.streaming metrics, baseline / streaming",
      row("bufferUtilizationPercent (task-side sampled gauge)",
        s"${telemetryValue(baselineTelemetry, baselineTelemetry.bufferUtilizationPercent)} / " +
          telemetryValue(streamingTelemetry, streamingTelemetry.bufferUtilizationPercent)),
      row("spillCount",
        s"${telemetryValue(baselineTelemetry, baselineTelemetry.spillCount)} / " +
          telemetryValue(streamingTelemetry, streamingTelemetry.spillCount)),
      row("backpressureEvents",
        s"${telemetryValue(baselineTelemetry, baselineTelemetry.backpressureEvents)} / " +
          telemetryValue(streamingTelemetry, streamingTelemetry.backpressureEvents)),
      row("partialReadInvalidations",
        s"${telemetryValue(baselineTelemetry, baselineTelemetry.partialReadInvalidations)} / " +
          telemetryValue(streamingTelemetry, streamingTelemetry.partialReadInvalidations)),
      row("executor registries represented",
        s"${baselineTelemetry.executorCount} / ${streamingTelemetry.executorCount}"),
      row("tasks observed",
        s"${baseline.taskEndCount} / ${streaming.taskEndCount}"),
      "") ++ TelemetryScopeNote ++ Seq("") ++ TargetsNote
  }

  /** Metric value qualified by whether an executor-side sample was returned. */
  private def telemetryValue(telemetry: StreamingTelemetry, value: Long): String = {
    if (telemetry.available) value.toString else "unavailable"
  }

  /**
   * Source-on/source-off current-thread CPU accounting for the streaming telemetry implementation.
   *
   * Paired samples are alternated and reduced with the lower median, so launch order and one noisy
   * reading cannot decide the result. JVMs that cannot expose current-thread CPU time are reported
   * as unavailable rather than silently substituting wall time.
   *
   * @param observation the paired CPU readings
   * @return the section's lines
   */
  private def telemetryCpuSection(observation: TelemetryCpuObservation): Seq[String] = {
    (observation.sourceOffNanos, observation.sourceOnNanos) match {
      case (Some(sourceOff), Some(sourceOn)) =>
        val overhead = percentTenths(sourceOn - sourceOff, sourceOff)
        Seq(
          "",
          "Telemetry CPU cost, paired current-thread CPU samples",
          row("source off, lower median", s"$sourceOff ns"),
          row("source on, lower median", s"$sourceOn ns"),
          row("source-on overhead", s"${renderTenths(overhead)} percent"),
          row("acceptance target", s"under $MaxTelemetryCpuOverheadPercent percent CPU overhead"),
          row("cost per metric operation",
            s"${telemetryNanosPerOperation(sourceOn - sourceOff)} ns"),
          row("metric operations priced", telemetryOperationCount.toString),
          row("anti-optimization checksum", observation.checksum.toString),
          row("read this way", "the source-on loop reads the live gauge and advances the"),
          row("", "three event counters at a sparse representative event interval"))
      case _ =>
        Seq(
          "",
          "Telemetry CPU cost, paired current-thread CPU samples",
          row("measurement", "unavailable"),
          row("reason", "this JVM did not expose enabled current-thread CPU time"),
          row("acceptance target", s"under $MaxTelemetryCpuOverheadPercent percent CPU overhead"),
          row("anti-optimization checksum", observation.checksum.toString))
    }
  }

  /**
   * Writes the report to the harness's result stream when one is open, and always to the console.
   *
   * The stream is open only when `SPARK_GENERATE_BENCHMARK_FILES=1` asked for a result file, so the
   * console copy is what a developer running the benchmark interactively actually reads. Bytes are
   * encoded explicitly rather than through the platform default, so a result file is identical
   * wherever it was produced and can be compared against a later run without a false difference.
   *
   * @param lines the report, one element per line
   */
  private def emit(lines: Seq[String]): Unit = {
    val text = lines.mkString("", "\n", "\n")
    output.foreach(stream => stream.write(text.getBytes(StandardCharsets.UTF_8)))
    // scalastyle:off println
    lines.foreach(line => println(line))
    // scalastyle:on println
  }


  // ---------------------------------------------------------------------------------------------
  // Presentation and arithmetic. Every percentage is computed in integer tenths and rendered by
  // hand: fixed-point integers cannot drift the way a float can, and rendering by hand keeps the
  // report free of the locale-dependent decimal separators and digit-grouping characters that a
  // formatted float would introduce -- some of which are not ASCII at all.
  // ---------------------------------------------------------------------------------------------

  /**
   * One report row: an indented label padded to a fixed width, then its figure.
   *
   * A label longer than the column keeps one separating space rather than colliding with its value,
   * so a long configuration key is still readable.
   *
   * @param label what the row reports
   * @param value the figure
   * @return the rendered row
   */
  private def row(label: String, value: String): String = {
    val padding = math.max(1, LabelWidth - label.length)
    s"  $label${" " * padding}$value"
  }

  /**
   * A case's elapsed times, best and mean, in milliseconds.
   *
   * @param observation the case
   * @return the rendered description
   */
  private def elapsedDescription(observation: CaseObservation): String = {
    s"best ${millisOf(observation.bestNanos)} ms, mean ${millisOf(observation.meanNanos)} ms"
  }

  /**
   * A case's spill volumes, in memory and to disk, summed over every run recorded.
   *
   * @param observation the case
   * @return the rendered description
   */
  private def spillDescription(observation: CaseObservation): String = {
    s"${observation.memoryBytesSpilled} bytes in memory, " +
      s"${observation.diskBytesSpilled} bytes to disk"
  }

  /**
   * A case's throughput, as the shuffle bytes one run read over the best time a run took.
   *
   * @param observation the case
   * @return the rendered description
   */
  private def bandwidthDescription(observation: CaseObservation): String = {
    val bytesPerRun = perRun(observation.shuffleRemoteBytesRead, observation.runCount)
    s"${renderTenths(mebibytesPerSecondTenths(bytesPerRun, observation.bestNanos))} MB/s"
  }

  /**
   * A case's spill rate: disk bytes spilled as tenths of a percent of the shuffle bytes written.
   *
   * @param observation the case
   * @return tenths of a percent
   */
  private def spillRateTenths(observation: CaseObservation): Long = {
    percentTenths(observation.diskBytesSpilled, observation.shuffleBytesWritten)
  }

  /**
   * A case's spill rate attributable to memory pressure, which is the figure the acceptance target
   * names.
   *
   * Derived from the event count rather than from a second volume accumulator, because there is no
   * second accumulator to derive it from and inventing one would mean a fifth streaming metric,
   * which the specification fixes at four. The derivation is exact in the case that matters: with
   * no threshold-driven event, no byte can have left memory under pressure, so the pressure rate is
   * zero however large the combined volume is. With events present the combined rate is reported as
   * the upper bound it is, and the note beside it says so rather than pretending to a split this
   * benchmark cannot make.
   *
   * @param observation the case
   * @return the rate in tenths of a percent, or none without executor-side attribution
   */
  private def pressureSpillRateTenths(observation: CaseObservation): Option[Long] = {
    if (!observation.telemetry.available) {
      None
    } else if (observation.telemetry.spillCount == 0L) {
      Some(0L)
    } else {
      Some(spillRateTenths(observation))
    }
  }

  /**
   * Renders a value expressed in tenths as a decimal string carrying one fraction digit.
   *
   * The sign is applied to the rendering rather than left to fall out of the division, because
   * integer division of a negative value would otherwise lose the sign on a magnitude below one
   * tenth of a whole and report a regression as progress.
   *
   * @param tenths the value, in tenths
   * @return the rendered value
   */
  private def renderTenths(tenths: Long): String = {
    val sign = if (tenths < 0L) "-" else ""
    val magnitude = math.abs(tenths)
    s"$sign${magnitude / 10L}.${magnitude % 10L}"
  }

  /**
   * The share `part` is of `whole`, in tenths of a percent.
   *
   * Answers zero when there is nothing to divide by, so the report is total: there is no input for
   * which this throws and no sentinel it returns in place of a percentage. A negative numerator is
   * carried through rather than clamped, because a figure below the baseline is exactly what the
   * memory and latency sections need to be able to say.
   *
   * @param part the numerator
   * @param whole the denominator
   * @return tenths of a percent
   */
  private def percentTenths(part: Long, whole: Long): Long = {
    if (whole <= 0L) 0L else part * TenthsPerWhole / whole
  }

  /**
   * How far `measured` fell below `baseline`, in tenths of a percent of the baseline.
   *
   * Negative when the measured path was the slower one, reported as it stands rather than
   * clamped: a regression shown as zero would be indistinguishable from parity.
   *
   * @param baseline the reference figure
   * @param measured the figure being compared against it
   * @return tenths of a percent of reduction
   */
  private def reductionTenths(baseline: Long, measured: Long): Long = {
    percentTenths(baseline - measured, baseline)
  }

  /**
   * Throughput in tenths of a mebibyte per second.
   *
   * Elapsed time is reduced to milliseconds before the division so the numerator stays far
   * away from overflowing a `Long` even for a multi-gigabyte total, and a run too quick to have
   * taken a whole millisecond is charged one rather than dividing by zero.
   *
   * @param bytes bytes moved
   * @param nanos the time they took
   * @return tenths of a mebibyte per second
   */
  private def mebibytesPerSecondTenths(bytes: Long, nanos: Long): Long = {
    if (bytes <= 0L || nanos <= 0L) {
      0L
    } else {
      val millis = math.max(1L, TimeUnit.NANOSECONDS.toMillis(nanos))
      bytes * 10L * MillisPerSecond / (millis * BytesPerMebibyte)
    }
  }

  /**
   * A cumulative total shared out over the runs that contributed to it.
   *
   * @param total the cumulative figure
   * @param runs runs recorded
   * @return the per-run share, or zero when no run was recorded
   */
  private def perRun(total: Long, runs: Int): Long = {
    if (runs <= 0) 0L else total / runs.toLong
  }

  /** Milliseconds a nanosecond figure amounts to, which is the unit the report states times in. */
  private def millisOf(nanos: Long): Long = TimeUnit.NANOSECONDS.toMillis(nanos)

  // ---------------------------------------------------------------------------------------------
  // What a case is, and what it observed.
  // ---------------------------------------------------------------------------------------------

  /** Compact result of one grouped workload after its executor-side observations are merged. */
  private class WorkloadObservation(
      val groupsProduced: Long,
      val telemetry: StreamingTelemetry,
      val memory: MemoryFootprint) extends Serializable

  /** Observation emitted by one result partition before readings are merged per executor. */
  private class ExecutorWorkloadObservation(
      val executorId: String,
      val groupsProduced: Long,
      val telemetry: StreamingTelemetry,
      val memory: MemoryFootprint) extends Serializable

  /** Full executor footprint plus the aggregate streaming-quota breakdown sampled during a run. */
  private class MemoryFootprint(
      val heapBytes: Long,
      val nativeBytes: Long,
      val totalBytes: Long,
      val aggregateQuotaBytes: Long,
      val producerBytes: Long,
      val consumerBytes: Long,
      val transientBytes: Long,
      val metadataBytes: Long) extends Serializable {

    def max(other: MemoryFootprint): MemoryFootprint = {
      new MemoryFootprint(
        math.max(heapBytes, other.heapBytes),
        math.max(nativeBytes, other.nativeBytes),
        math.max(totalBytes, other.totalBytes),
        math.max(aggregateQuotaBytes, other.aggregateQuotaBytes),
        math.max(producerBytes, other.producerBytes),
        math.max(consumerBytes, other.consumerBytes),
        math.max(transientBytes, other.transientBytes),
        math.max(metadataBytes, other.metadataBytes))
    }

    def plus(other: MemoryFootprint): MemoryFootprint = {
      new MemoryFootprint(
        heapBytes + other.heapBytes,
        nativeBytes + other.nativeBytes,
        totalBytes + other.totalBytes,
        aggregateQuotaBytes + other.aggregateQuotaBytes,
        producerBytes + other.producerBytes,
        consumerBytes + other.consumerBytes,
        transientBytes + other.transientBytes,
        metadataBytes + other.metadataBytes)
    }
  }

  /**
   * Isolated task-metric window for one reference run or one shape probe.
   *
   * Remote bytes and local-plus-remote bytes are carried SEPARATELY and deliberately. A bandwidth
   * figure may only be computed from the part that crossed a link, and `totalBytesRead` is the sum
   * of local and remote; see [[bandwidthSection]].
   */
  private class TaskMetricWindow(
      val taskEndCount: Long,
      val memoryBytesSpilled: Long,
      val diskBytesSpilled: Long,
      val peakExecutionMemory: Long,
      val shuffleBytesWritten: Long,
      val shuffleRecordsWritten: Long,
      val shuffleRemoteBytesRead: Long,
      val shuffleTotalBytesRead: Long,
      val shuffleRecordsRead: Long,
      val fetchWaitTime: Long)

  /** Current-thread CPU readings for the representative telemetry source-off/source-on loops. */
  private class TelemetryCpuObservation(
      val sourceOffNanos: Option[Long],
      val sourceOnNanos: Option[Long],
      val checksum: Long)

  /**
   * Everything one case of the comparison is, and everything it turned out to cost.
   *
   * @param caseName label the harness prints for the case and the report keys it by
   * @param conf configuration the case's context is built from, tunables and all
   * @param master master the context is started on, which decides whether executors are awaited
   */
  private class CaseObservation(val caseName: String, val conf: SparkConf, val master: String) {

    /** Reads Spark's own task accumulators for every task this case runs. */
    val recorder: ShuffleTaskMetricsRecorder = new ShuffleTaskMetricsRecorder

    private var runs: Int = 0

    private var bestElapsedNanos: Long = Long.MaxValue

    private var totalElapsedNanos: Long = 0L

    private var groups: Long = 0L

    private var manager: String = UnknownManagerName

    private var lastTelemetry: StreamingTelemetry = EmptyTelemetry

    private var fullMemory: MemoryFootprint = EmptyMemoryFootprint

    private var taskEnds: Long = 0L

    private var memorySpilled: Long = 0L

    private var diskSpilled: Long = 0L

    private var taskPeakExecutionMemory: Long = 0L

    private var bytesWritten: Long = 0L

    private var recordsWritten: Long = 0L

    private var remoteBytesRead: Long = 0L

    private var totalBytesRead: Long = 0L

    private var recordsRead: Long = 0L

    private var fetchWait: Long = 0L

    /**
     * Every run's elapsed time, in the order the runs happened.
     *
     * Kept because a best and a mean describe a distribution only if a reader is told how wide it
     * is. A latency figure that missed its acceptance target by a wide margin, and one that missed
     * it because a single run was slow, call for different responses, and only the samples can tell
     * them apart.
     */
    private val elapsedSamples = new mutable.ArrayBuffer[Long]()

    /** Full executor footprint of each workload shape, in measurement order. */
    private val shapeFootprints = new mutable.LinkedHashMap[String, MemoryFootprint]()

    /**
     * Records one completed run of the workload.
     *
     * Every run is recorded, the harness's unmeasured warm-up included, because each one is
     * work the executors genuinely did and more samples make the accumulated figures steadier.
     * Elapsed times are kept as a best and a mean; the comparison uses the best, and the mean
     * is printed beside it so a reader can see how much the two differ.
     *
     * @param elapsedNanos wall time the job took, cluster start-up excluded
     * @param workload executor-side group count, telemetry and full memory observations
     * @param metrics isolated task metrics from this reference run only
     */
    def observeRun(
        elapsedNanos: Long,
        workload: WorkloadObservation,
        metrics: TaskMetricWindow): Unit = {
      runs += 1
      totalElapsedNanos += elapsedNanos
      bestElapsedNanos = math.min(bestElapsedNanos, elapsedNanos)
      groups = math.max(groups, workload.groupsProduced)
      lastTelemetry = workload.telemetry
      fullMemory = fullMemory.max(workload.memory)
      taskEnds += metrics.taskEndCount
      memorySpilled += metrics.memoryBytesSpilled
      diskSpilled += metrics.diskBytesSpilled
      taskPeakExecutionMemory =
        math.max(taskPeakExecutionMemory, metrics.peakExecutionMemory)
      bytesWritten += metrics.shuffleBytesWritten
      recordsWritten += metrics.shuffleRecordsWritten
      remoteBytesRead += metrics.shuffleRemoteBytesRead
      totalBytesRead += metrics.shuffleTotalBytesRead
      recordsRead += metrics.shuffleRecordsRead
      fetchWait += metrics.fetchWaitTime
      elapsedSamples += elapsedNanos
    }

    /**
     * Records the full executor footprint one workload shape reached, taking field-wise maxima when
     * a shape is measured more than once.
     *
     * @param shapeName the shape's label, which the report prints
     * @param footprint the executor-side heap, native and quota reading
     */
    def observeShapeFootprint(shapeName: String, footprint: MemoryFootprint): Unit = {
      shapeFootprints(shapeName) =
        shapeFootprints.get(shapeName).map(_.max(footprint)).getOrElse(footprint)
    }

    /** Full executor footprint recorded for one shape. */
    def shapeFootprint(shapeName: String): MemoryFootprint =
      shapeFootprints.getOrElse(shapeName, EmptyMemoryFootprint)

    /** Shapes measured for this case, in the order they were measured. */
    def measuredShapes: Seq[String] = shapeFootprints.keys.toSeq

    /**
     * Records the shuffle manager the live environment held when this case's context came up.
     *
     * @param managerClassName simple class name of that manager
     */
    def recordManagerInService(managerClassName: String): Unit = {
      manager = managerClassName
    }

    /** Runs recorded so far, warm-up included. */
    def runCount: Int = runs

    /** The fastest run observed, or zero before any run has been observed. */
    def bestNanos: Long = if (runs == 0) 0L else bestElapsedNanos

    /** The mean run observed, or zero before any run has been observed. */
    def meanNanos: Long = if (runs == 0) 0L else totalElapsedNanos / runs.toLong

    /** The slowest run observed, or zero before any run has been observed. */
    def worstNanos: Long = if (elapsedSamples.isEmpty) 0L else elapsedSamples.max

    /** Every run's elapsed time, in the order the runs happened. */
    def elapsedNanosSamples: Seq[Long] = elapsedSamples.toSeq

    /** Groups the workload produced, which every run of a case should agree on. */
    def groupsProduced: Long = groups

    /** Simple class name of the shuffle manager that served this case. */
    def managerInService: String = manager

    /** The streaming metrics as they stood at the end of this case's most recent run. */
    def telemetry: StreamingTelemetry = lastTelemetry

    /** Full executor heap plus native footprint observed across the case. */
    def memoryFootprint: MemoryFootprint = fullMemory

    def taskEndCount: Long = taskEnds

    def memoryBytesSpilled: Long = memorySpilled

    def diskBytesSpilled: Long = diskSpilled

    def peakExecutionMemory: Long = taskPeakExecutionMemory

    def shuffleBytesWritten: Long = bytesWritten

    def shuffleRecordsWritten: Long = recordsWritten

    /** Bytes that crossed a link, which is the only basis a bandwidth figure may be taken from. */
    def shuffleRemoteBytesRead: Long = remoteBytesRead

    /** Local plus remote bytes: what the accumulator reports, and NOT a wire figure. */
    def shuffleTotalBytesRead: Long = totalBytesRead

    def shuffleRecordsRead: Long = recordsRead

    def fetchWaitTime: Long = fetchWait
  }

  /**
   * A reading of the four `shuffle.streaming` metrics, taken together so they describe one moment.
   *
   * @param bufferUtilizationPercent the gauge, which is computed when it is read
   * @param spillCount spill events counted since the case's registry was reset
   * @param backpressureEvents transitions into a throttled state counted since that reset
   * @param partialReadInvalidations per-producer invalidations counted since that reset
   * @param executorCount executor registries represented by this merged reading
   */
  private class StreamingTelemetry(
      val bufferUtilizationPercent: Long,
      val spillCount: Long,
      val backpressureEvents: Long,
      val partialReadInvalidations: Long,
      val executorCount: Int) extends Serializable {

    def available: Boolean = executorCount > 0

    def max(other: StreamingTelemetry): StreamingTelemetry = {
      new StreamingTelemetry(
        math.max(bufferUtilizationPercent, other.bufferUtilizationPercent),
        math.max(spillCount, other.spillCount),
        math.max(backpressureEvents, other.backpressureEvents),
        math.max(partialReadInvalidations, other.partialReadInvalidations),
        math.max(executorCount, other.executorCount))
    }

    def plus(other: StreamingTelemetry): StreamingTelemetry = {
      new StreamingTelemetry(
        math.max(bufferUtilizationPercent, other.bufferUtilizationPercent),
        spillCount + other.spillCount,
        backpressureEvents + other.backpressureEvents,
        partialReadInvalidations + other.partialReadInvalidations,
        executorCount + other.executorCount)
    }
  }

  /** Samples executor heap, native buffer pools and every category of the aggregate quota. */
  private class ExecutorMemorySampler {

    private val bufferPools =
      ManagementFactory.getPlatformMXBeans(classOf[BufferPoolMXBean]).asScala.toSeq

    private var peak: MemoryFootprint = EmptyMemoryFootprint

    def sample(): Unit = {
      val env = SparkEnv.get
      val quota = if (env != null && env.shuffleManager.isInstanceOf[StreamingShuffleManager]) {
        Some(MemorySpillManager.executorQuota(env.conf))
      } else {
        None
      }
      val heapBytes = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed
      val nativeBytes = bufferPools.map(pool => math.max(0L, pool.getMemoryUsed)).sum
      val footprint = new MemoryFootprint(
        heapBytes,
        nativeBytes,
        heapBytes + nativeBytes,
        quota.map(_.reservedBytes).getOrElse(0L),
        quota.map(_.reservedBytes(MemorySpillManager.ProducerMemory)).getOrElse(0L),
        quota.map(_.reservedBytes(MemorySpillManager.ConsumerMemory)).getOrElse(0L),
        quota.map(_.reservedBytes(MemorySpillManager.TransientMemory)).getOrElse(0L),
        quota.map(_.reservedBytes(MemorySpillManager.MetadataMemory)).getOrElse(0L))
      peak = peak.max(footprint)
    }

    def observed: MemoryFootprint = peak
  }

  /**
   * Accumulates the task metrics Spark already reports, for every task a case runs.
   *
   * Reading `TaskMetrics` through a listener is the same path an operator's own tooling takes, so
   * what this records is also evidence that the streaming write and read paths report through
   * Spark's existing accumulators rather than through a channel of their own. Parallel counters
   * would have left the streaming path invisible to every Spark observability surface there already
   * is, which is why none are introduced here.
   *
   * Every callback and every accessor is synchronized, because the listener bus delivers on its own
   * thread while the benchmark reads from the driver thread.
   */
  private class ShuffleTaskMetricsRecorder extends SparkListener {

    private var windowActive: Boolean = false

    private var tasksEnded: Long = 0L

    private var memorySpilled: Long = 0L

    private var diskSpilled: Long = 0L

    private var peakExecutionMemory: Long = 0L

    private var shuffleBytesWritten: Long = 0L

    private var shuffleRecordsWritten: Long = 0L

    private var shuffleRemoteBytesRead: Long = 0L

    private var shuffleTotalBytesRead: Long = 0L

    private var shuffleRecordsRead: Long = 0L

    private var fetchWaitTime: Long = 0L

    /** Opens a clean task-metric window for one reference run or one shape probe. */
    def beginWindow(): Unit = synchronized {
      windowActive = true
      tasksEnded = 0L
      memorySpilled = 0L
      diskSpilled = 0L
      peakExecutionMemory = 0L
      shuffleBytesWritten = 0L
      shuffleRecordsWritten = 0L
      shuffleRemoteBytesRead = 0L
      shuffleTotalBytesRead = 0L
      shuffleRecordsRead = 0L
      fetchWaitTime = 0L
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
      if (windowActive) {
        tasksEnded += 1L
        // The metrics are documented as null for a task that failed, so a failure adds to the task
        // count and to nothing else instead of bringing the listener bus thread down.
        val metrics: TaskMetrics = taskEnd.taskMetrics
        if (metrics != null) {
          memorySpilled += metrics.memoryBytesSpilled
          diskSpilled += metrics.diskBytesSpilled
          peakExecutionMemory = math.max(peakExecutionMemory, metrics.peakExecutionMemory)
          shuffleBytesWritten += metrics.shuffleWriteMetrics.bytesWritten
          shuffleRecordsWritten += metrics.shuffleWriteMetrics.recordsWritten
          shuffleRemoteBytesRead += metrics.shuffleReadMetrics.remoteBytesRead
          shuffleTotalBytesRead += metrics.shuffleReadMetrics.totalBytesRead
          shuffleRecordsRead += metrics.shuffleReadMetrics.recordsRead
          fetchWaitTime += metrics.shuffleReadMetrics.fetchWaitTime
        }
      }
    }

    /** Closes and returns the current isolated window. */
    def finishWindow(): TaskMetricWindow = synchronized {
      windowActive = false
      new TaskMetricWindow(
        tasksEnded,
        memorySpilled,
        diskSpilled,
        peakExecutionMemory,
        shuffleBytesWritten,
        shuffleRecordsWritten,
        shuffleRemoteBytesRead,
        shuffleTotalBytesRead,
        shuffleRecordsRead,
        fetchWaitTime)
    }
  }
}
