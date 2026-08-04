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

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TestUtils}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_ENABLED, SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, SHUFFLE_STREAMING_SPILL_THRESHOLD}
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
 * both the producer and the consumer role and no block would cross a JVM boundary -- which is
 * exactly the transfer the streaming path exists to overlap with map-side work. Two real executor
 * JVMs give the driver-registers / executor-resolves rendezvous and genuine cross-executor
 * streaming.
 *
 * ==What is measured, and where each figure comes from==
 *
 * Latency is wall-clock time taken around the job alone, so cluster start-up is charged to neither
 * case, and the harness times the same runs independently. Memory and spill come from Spark's
 * EXISTING task accumulators -- `TaskMetrics.peakExecutionMemory`, `memoryBytesSpilled` and
 * `diskBytesSpilled` -- read through a `SparkListener`, which is the same read path an operator's
 * own tooling uses; counters of this feature's own would have left the streaming path invisible to
 * every Spark observability surface there already is. Bandwidth is derived from the shuffle bytes
 * those same accumulators report over the elapsed time. The four `shuffle.streaming` metrics
 * supplement the picture, and the registry backing them is reset as the case switches so that each
 * case reports its own reading.
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

  /** Name of the comparison table, derived so it can never disagree with the workload above. */
  private val ComparisonName: String =
    s"groupByKey, ${DatasetBytes / BytesPerMebibyte} MB over $PartitionCount partitions"

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

  // ---------------------------------------------------------------------------------------------
  // The acceptance targets, and the report's own presentation constants. The latency window comes
  // from the shared fixtures; the other two targets exist nowhere else in the tree, so they are
  // named here rather than left as bare numbers inside a string.
  // ---------------------------------------------------------------------------------------------

  /** Ceiling of the reported memory overhead, as a whole percentage of the baseline. */
  private val MaxMemoryOverheadPercent: Int = 10

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
  private val EmptyTelemetry: StreamingTelemetry = new StreamingTelemetry(0L, 0L, 0L, 0L)

  /** Why a driver-side reading of the four streaming metrics can be zero and still be correct. */
  private val TelemetryScopeNote: Seq[String] = Seq(
    "  Note: the four metrics are read from THIS JVM's registry. Under a cluster master the",
    "  streaming writer and reader run inside the executor JVMs, whose own registries export the",
    "  same four metrics per executor over whatever sink an operator has configured, so a driver",
    "  side reading of zero is expected and is not evidence that nothing streamed.")

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

  /** Listener-bus drains that timed out, reported so an incomplete reading is never silent. */
  private var listenerDrainTimeouts: Int = 0

  // ---------------------------------------------------------------------------------------------
  // The comparison.
  // ---------------------------------------------------------------------------------------------

  /**
   * Runs the whole comparison: the sort-based baseline, then streaming, then the report.
   *
   * The two cases are handed to the harness as ordinary cases, so the table it prints carries its
   * own best, mean, standard deviation, rate and relative columns for both. The report emitted
   * afterwards adds the three dimensions the harness knows nothing about -- memory, spill and
   * bandwidth -- and restates latency beside the acceptance window.
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
        emitReport(master, baseline, streaming)
      }
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
    val startedAt = System.nanoTime()
    val groupsProduced = groupedReferenceWorkload(context).count()
    val elapsedNanos = System.nanoTime() - startedAt
    // Task-end events arrive on the listener bus's own thread, so the bus is drained before the
    // accumulated figures are read; otherwise the tasks of the run just finished might not be in
    // them yet.
    drainListenerBus(context)
    observation.observeRun(elapsedNanos, groupsProduced, telemetrySnapshot())
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
   * A timeout is counted and reported rather than thrown. It would mean one iteration's figures are
   * incomplete, which is worth knowing and is printed, but it is not a reason to abandon a
   * measurement that has already been taken -- and a benchmark that died because a bus was slow
   * would be failing for something other than the shuffle it set out to measure.
   *
   * @param context the context whose bus is drained
   */
  private def drainListenerBus(context: SparkContext): Unit = {
    try {
      context.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
    } catch {
      case NonFatal(_) => listenerDrainTimeouts += 1
    }
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
   * Taken after every iteration rather than once at the end, because the registry is reset when the
   * case switches: a reading taken only at the end would report streaming's figures for both cases.
   * The three counters are cumulative across the case. The utilisation gauge is computed when it is
   * read, so a sample taken once the job has finished is expected to be zero and says nothing about
   * what the gauge showed while records were in flight.
   *
   * @return the snapshot
   */
  private def telemetrySnapshot(): StreamingTelemetry = {
    new StreamingTelemetry(
      observedBufferUtilizationPercent(),
      observedSpillCount(),
      observedBackpressureEvents(),
      observedPartialReadInvalidations())
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
   */
  private def emitReport(
      master: String,
      baseline: CaseObservation,
      streaming: CaseObservation): Unit = {
    emit(
      workloadSection(master, baseline, streaming) ++
        activationSection(baseline, streaming) ++
        latencySection(baseline, streaming) ++
        memorySection(baseline, streaming) ++
        spillSection(baseline, streaming) ++
        bandwidthSection(baseline, streaming) ++
        telemetrySection(baseline, streaming) ++
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
    Seq(
      "",
      "Latency, wall clock around the job alone, cluster start-up excluded",
      row(baseline.caseName, elapsedDescription(baseline)),
      row(streaming.caseName, elapsedDescription(streaming)),
      row("reduction on best time", s"${renderTenths(reduction)} percent"),
      row("acceptance target",
        s"$MinLatencyReductionPercent to $MaxLatencyReductionPercent percent reduction"))
  }

  /**
   * Memory, taken from the peak execution memory Spark's own accumulator already reports.
   *
   * A high-water mark across tasks rather than a sum: peak execution memory is already a per-task
   * peak, so adding peaks that never coexisted would report a total no executor ever held.
   *
   * @param baseline the sort-based case
   * @param streaming the streaming case
   * @return the section's lines
   */
  private def memorySection(
      baseline: CaseObservation,
      streaming: CaseObservation): Seq[String] = {
    val baselinePeak = baseline.recorder.peakExecutionMemory
    val streamingPeak = streaming.recorder.peakExecutionMemory
    val overhead = percentTenths(streamingPeak - baselinePeak, baselinePeak)
    Seq(
      "",
      "Memory, peak execution memory high water mark across tasks",
      row(baseline.caseName, s"$baselinePeak bytes"),
      row(streaming.caseName, s"$streamingPeak bytes"),
      row("overhead", s"${renderTenths(overhead)} percent"),
      row("acceptance target", s"under $MaxMemoryOverheadPercent percent overhead"))
  }

  /**
   * Spill, taken from Spark's own spill accumulators and from no counter of this feature's own.
   *
   * The rate is disk bytes spilled as a share of the shuffle bytes written, which is the reading
   * that answers "how much of what we moved had to go through the disk on the way".
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
      row("spill rate, disk bytes over shuffle bytes written",
        s"${renderTenths(spillRateTenths(baseline))} percent / " +
          s"${renderTenths(spillRateTenths(streaming))} percent"),
      row("acceptance target", s"under $MaxSpillRatePercent percent"))
  }

  /**
   * Bandwidth, derived from the shuffle bytes the accumulators report over the best elapsed time.
   *
   * Byte totals are divided by the number of runs recorded before the rate is taken, because the
   * accumulators are cumulative over every run of a case while the elapsed time is one run's.
   *
   * The byte figures are what the accumulators report, which is bytes ON THE WIRE and therefore
   * bytes after shuffle compression. They are consequently far smaller than the dataset, whose
   * values are highly compressible, and that is the right basis for a bandwidth figure: what a link
   * carries is the compressed stream, not the dataset it was built from.
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
      "Bandwidth, shuffle bytes on the wire per second of the best run",
      row(baseline.caseName, bandwidthDescription(baseline)),
      row(streaming.caseName, bandwidthDescription(streaming)),
      row("shuffle bytes written per run",
        s"${perRun(baseline.recorder.shuffleBytesWritten, baseline.runCount)} / " +
          s"${perRun(streaming.recorder.shuffleBytesWritten, streaming.runCount)}"),
      row("shuffle bytes read per run",
        s"${perRun(baseline.recorder.shuffleBytesRead, baseline.runCount)} / " +
          s"${perRun(streaming.recorder.shuffleBytesRead, streaming.runCount)}"),
      row("shuffle records written per run",
        s"${perRun(baseline.recorder.shuffleRecordsWritten, baseline.runCount)} / " +
          s"${perRun(streaming.recorder.shuffleRecordsWritten, streaming.runCount)}"),
      row("shuffle records read per run",
        s"${perRun(baseline.recorder.shuffleRecordsRead, baseline.runCount)} / " +
          s"${perRun(streaming.recorder.shuffleRecordsRead, streaming.runCount)}"),
      row("fetch wait time per run, ms",
        s"${perRun(baseline.recorder.fetchWaitTime, baseline.runCount)} / " +
          s"${perRun(streaming.recorder.fetchWaitTime, streaming.runCount)}"))
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
      row("bufferUtilizationPercent (live gauge, post job)",
        s"${baselineTelemetry.bufferUtilizationPercent} / " +
          s"${streamingTelemetry.bufferUtilizationPercent}"),
      row("spillCount",
        s"${baselineTelemetry.spillCount} / ${streamingTelemetry.spillCount}"),
      row("backpressureEvents",
        s"${baselineTelemetry.backpressureEvents} / ${streamingTelemetry.backpressureEvents}"),
      row("partialReadInvalidations",
        s"${baselineTelemetry.partialReadInvalidations} / " +
          s"${streamingTelemetry.partialReadInvalidations}"),
      row("tasks observed",
        s"${baseline.recorder.taskEndCount} / ${streaming.recorder.taskEndCount}"),
      row("listener bus drain timeouts", listenerDrainTimeouts.toString),
      "") ++ TelemetryScopeNote ++ Seq("") ++ TargetsNote
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
    s"${observation.recorder.memoryBytesSpilled} bytes in memory, " +
      s"${observation.recorder.diskBytesSpilled} bytes to disk"
  }

  /**
   * A case's throughput, as the shuffle bytes one run read over the best time a run took.
   *
   * @param observation the case
   * @return the rendered description
   */
  private def bandwidthDescription(observation: CaseObservation): String = {
    val bytesPerRun = perRun(observation.recorder.shuffleBytesRead, observation.runCount)
    s"${renderTenths(mebibytesPerSecondTenths(bytesPerRun, observation.bestNanos))} MB/s"
  }

  /**
   * A case's spill rate: disk bytes spilled as tenths of a percent of the shuffle bytes written.
   *
   * @param observation the case
   * @return tenths of a percent
   */
  private def spillRateTenths(observation: CaseObservation): Long = {
    percentTenths(observation.recorder.diskBytesSpilled, observation.recorder.shuffleBytesWritten)
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

    /**
     * Records one completed run of the workload.
     *
     * Every run is recorded, the harness's unmeasured warm-up included, because each one is
     * work the executors genuinely did and more samples make the accumulated figures steadier.
     * Elapsed times are kept as a best and a mean; the comparison uses the best, and the mean
     * is printed beside it so a reader can see how much the two differ.
     *
     * @param elapsedNanos wall time the job took, cluster start-up excluded
     * @param producedGroups groups the job produced, kept as evidence that it did the work
     * @param telemetry the four streaming metrics as they stood when the run finished
     */
    def observeRun(
        elapsedNanos: Long,
        producedGroups: Long,
        telemetry: StreamingTelemetry): Unit = {
      runs += 1
      totalElapsedNanos += elapsedNanos
      bestElapsedNanos = math.min(bestElapsedNanos, elapsedNanos)
      groups = math.max(groups, producedGroups)
      lastTelemetry = telemetry
    }

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

    /** Groups the workload produced, which every run of a case should agree on. */
    def groupsProduced: Long = groups

    /** Simple class name of the shuffle manager that served this case. */
    def managerInService: String = manager

    /** The streaming metrics as they stood at the end of this case's most recent run. */
    def telemetry: StreamingTelemetry = lastTelemetry
  }

  /**
   * A reading of the four `shuffle.streaming` metrics, taken together so they describe one moment.
   *
   * @param bufferUtilizationPercent the gauge, which is computed when it is read
   * @param spillCount spill events counted since the case's registry was reset
   * @param backpressureEvents transitions into a throttled state counted since that reset
   * @param partialReadInvalidations per-producer invalidations counted since that reset
   */
  private class StreamingTelemetry(
      val bufferUtilizationPercent: Long,
      val spillCount: Long,
      val backpressureEvents: Long,
      val partialReadInvalidations: Long)

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

    private var tasksEnded: Long = 0L

    private var memorySpilledTotal: Long = 0L

    private var diskSpilledTotal: Long = 0L

    private var peakMemoryHighWater: Long = 0L

    private var shuffleBytesWrittenTotal: Long = 0L

    private var shuffleRecordsWrittenTotal: Long = 0L

    private var shuffleBytesReadTotal: Long = 0L

    private var shuffleRecordsReadTotal: Long = 0L

    private var fetchWaitTimeTotal: Long = 0L

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
      tasksEnded += 1L
      // The metrics are documented as null for a task that failed, so a failure adds to the task
      // count and to nothing else instead of bringing the listener bus thread down.
      val metrics: TaskMetrics = taskEnd.taskMetrics
      if (metrics != null) {
        memorySpilledTotal += metrics.memoryBytesSpilled
        diskSpilledTotal += metrics.diskBytesSpilled
        // A high-water mark rather than a sum: peak execution memory is already a per-task peak, so
        // adding peaks that never coexisted would report a total no executor ever held.
        peakMemoryHighWater = math.max(peakMemoryHighWater, metrics.peakExecutionMemory)
        shuffleBytesWrittenTotal += metrics.shuffleWriteMetrics.bytesWritten
        shuffleRecordsWrittenTotal += metrics.shuffleWriteMetrics.recordsWritten
        shuffleBytesReadTotal += metrics.shuffleReadMetrics.totalBytesRead
        shuffleRecordsReadTotal += metrics.shuffleReadMetrics.recordsRead
        fetchWaitTimeTotal += metrics.shuffleReadMetrics.fetchWaitTime
      }
    }

    /** Task-end events seen, whether or not they carried metrics. */
    def taskEndCount: Long = synchronized(tasksEnded)

    /** In-memory bytes spilled, summed over every task of every run. */
    def memoryBytesSpilled: Long = synchronized(memorySpilledTotal)

    /** On-disk bytes spilled, summed over every task of every run. */
    def diskBytesSpilled: Long = synchronized(diskSpilledTotal)

    /** The highest per-task peak execution memory any task reported. */
    def peakExecutionMemory: Long = synchronized(peakMemoryHighWater)

    /** Shuffle bytes written, summed over every task of every run. */
    def shuffleBytesWritten: Long = synchronized(shuffleBytesWrittenTotal)

    /** Shuffle records written, summed over every task of every run. */
    def shuffleRecordsWritten: Long = synchronized(shuffleRecordsWrittenTotal)

    /** Shuffle bytes read, local and remote together, summed over every task of every run. */
    def shuffleBytesRead: Long = synchronized(shuffleBytesReadTotal)

    /** Shuffle records read, summed over every task of every run. */
    def shuffleRecordsRead: Long = synchronized(shuffleRecordsReadTotal)

    /** Milliseconds tasks spent blocked on remote shuffle input, summed over every run. */
    def fetchWaitTime: Long = synchronized(fetchWaitTimeTotal)
  }
}

