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
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TaskContext, TaskContextImpl,
  TestUtils}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.{SHUFFLE_COMPRESS, SHUFFLE_MANAGER,
  SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_ENABLED,
  SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, SHUFFLE_STREAMING_SPILL_THRESHOLD}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

/**
 * Comparative benchmark of the streaming shuffle against the sort-based baseline it exists to beat.
 *
 * The reference workload is a 100 MiB (104,857,600 bytes) `groupByKey` across ten partitions, run
 * once on each path, alongside a CPU-bound comparison and a producer/consumer overlap measurement.
 *
 * This is a report, not a gate: like every benchmark in this project it records comparative figures
 * and asserts no threshold, so the latency, memory and spill acceptance targets are read off the
 * report rather than enforced by a test run. Spill is reported split by cause, because the disk
 * volume of a healthy streaming run is dominated by the end-of-stream durability flush that a
 * successful map task always performs -- the streaming counterpart of the files the sort path
 * writes as `shuffleBytesWritten` -- while the under-five-percent target concerns only eviction
 * under memory pressure, which is what `shuffle.streaming.spillCount` counts.
 */
object StreamingShufflePerformanceBenchmark extends BenchmarkBase with StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  private val PartitionCount: Int = DefaultPartitionCount

  private val DatasetBytes: Long = TargetDatasetBytes

  private val RecordCount: Long =
    PartitionCount.toLong * recordsPerPartitionFor(PartitionCount, DatasetBytes).toLong

  private val ScenarioName: String = "Streaming shuffle versus sort-based shuffle"

  /** CPU-bound comparison that isolates shuffle coordination beneath deterministic compute. */
  private val CpuScenarioName: String = "CPU-bound shuffle coordination"

  private val ComparisonName: String =
    s"groupByKey, ${DatasetBytes / BytesPerMebibyte} MB over $PartitionCount partitions"

  /** Work items and deterministic mixing rounds applied on both sides of the CPU-bound shuffle. */
  private val CpuWorkItems: Int = PartitionCount * 8192

  private val CpuMixRounds: Int = 64

  private val CpuComparisonName: String =
    s"deterministic CPU mix, $CpuWorkItems records over $PartitionCount partitions"

  private val ClusterMaster: String = "local-cluster[2,1,1024]"

  private val ClusterMasterPrefix: String = "local-cluster"

  private val ExecutorCount: Int = 2

  private val ExecutorStartupTimeoutMillis: Long = 60000L

  private val ListenerDrainTimeoutMillis: Long = 60000L

  private val MeasuredIterations: Int = 3

  private val OverlapScenarioName: String = "Producer/consumer overlap on the streaming path"

  private val OverlapMaster: String = "local[4]"

  private val OverlapPartitions: Int = 2

  private val OverlapRecords: Int = 8000000

  private val OverlapJoinTimeoutMillis: Long = 180000L

  private val OverlapMapId: Long = 0L

  private val OverlapAttemptBase: Long = 9200L

  private val OverlapAttemptStride: Long = 2L

  private val OverlapAppName: String = "streaming-shuffle-benchmark-overlap"

  private val OverlapComparisonName: String =
    s"$OverlapRecords records streamed to one consumer"

  private val OverlapLiveCaseName: String = "consumer attached during production"

  private val OverlapRetainedCaseName: String = "consumer attached after production"

  private val BaselineCaseName: String = "sort-based shuffle (baseline)"

  private val StreamingCaseName: String = "streaming shuffle"

  private val BaselineAppName: String = "streaming-shuffle-benchmark-sort-baseline"

  private val StreamingAppName: String = "streaming-shuffle-benchmark-streaming"

  private val CpuBaselineAppName: String = "streaming-shuffle-benchmark-cpu-sort"

  private val CpuStreamingAppName: String = "streaming-shuffle-benchmark-cpu-streaming"

  private val MaxMemoryOverheadPercent: Int = 10

  /** Improvement window the CPU-bound acceptance target names. */
  private val MinCpuBoundImprovementPercent: Int = 5

  private val MaxCpuBoundImprovementPercent: Int = 10

  private val NarrowPartitionCount: Int = 2

  private val WidePartitionCount: Int = 200

  private val ShapeProbeBytes: Long = 8L * 1024L * 1024L

  private val MaxSpillRatePercent: Int = 5

  private val LabelWidth: Int = 52

  private val ReportRule: String = "-" * 96

  private val ReportHeading: String = "Streaming shuffle acceptance report"

  private val UnknownManagerName: String = "unavailable"

  private val UncappedBandwidth: String = "unset, which means uncapped egress"

  private val MillisPerSecond: Long = TimeUnit.SECONDS.toMillis(1L)

  private val TenthsPerWhole: Long = 1000L

  private val EmptyTelemetry: StreamingTelemetry =
    new StreamingTelemetry(0L, 0L, 0L, 0L, 0)

  private val EmptyMemoryFootprint: MemoryFootprint =
    new MemoryFootprint(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)

  private val MemorySampleInterval: Int = 256

  private val TelemetryCpuWorkItems: Int = 500000

  private val TelemetryCpuSampleInterval: Int = 4096

  private val TelemetryCpuSamples: Int = 5

  private val TelemetryOpsPerSampledEvent: Int = 4

  private val MaxTelemetryCpuOverheadPercent: Int = 1

  private val TelemetryCpuContributor: StreamingShuffleBufferUtilizationContributor =
    new StreamingShuffleBufferUtilizationContributor {
      override def contributedBufferedBytes: Long = BytesPerMebibyte
      override def contributedBudgetBytes: Long = 4L * BytesPerMebibyte
    }

  private val TelemetryScopeNote: Seq[String] = Seq(
    "  Note: the four metrics are sampled inside the workload tasks and merged once per executor.",
    "  A zero spillCount is therefore an executor-side zero. If no executor sample is available,",
    "  pressure-spill attribution is printed as unavailable rather than as a misleading zero.")

  private val LatencyAttributionNote: Seq[String] = Seq(
    "  Note: the DAG scheduler is unmodified and starts reduce tasks after the map stage finishes.",
    "  This comparison measures framing, checksumming, retained-output publication and transport",
    "  against sort, index publication and ordinary block fetch. It claims no map/reduce overlap.",
    "  The overlap the subsystem does deliver is measured on its own, in the section below.")

  /** What the overlap comparison establishes, and what it must not be read as establishing. */
  private val OverlapAttributionNote: Seq[String] = Seq(
    "  Note: both arms stream the same volume through the same manager, writer, reader, rendezvous",
    "  and transport, on one context, and differ only in when the consumer attaches. The reduction",
    "  is the value of the overlap on the path the shuffle abstraction owns. It is NOT a scheduled",
    "  job's latency and may not be added to the figure above: a scheduled job attaches its",
    "  consumer only once the map stage has finished, which nothing inside this boundary may move.")

  private val TargetsNote: Seq[String] = Seq(
    "  Note: every figure above is reported for a human to judge. This benchmark enforces no",
    "  threshold, in keeping with every other benchmark in this repository.")

  private var activeContext: Option[SparkContext] = None

  private var activeCase: Option[CaseObservation] = None

  private var nextOverlapAttemptId: Long = OverlapAttemptBase

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val master = masterFrom(mainArgs)
    val baseline = new CaseObservation(
      BaselineCaseName, withLocalMaster(sortBaselineConf(), BaselineAppName, master), master)
    // BOTH keys, set through their typed entries by the shared fixture: spark.shuffle.manager
    // selects the manager class and spark.shuffle.streaming.enabled opens its behaviour gate.
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
      val overlapContext = new CaseObservation(
        OverlapScenarioName,
        withLocalMaster(overlapConf(), OverlapAppName, OverlapMaster),
        OverlapMaster)
      val overlap = new OverlapObservation
      require(contextFor(overlapContext) != null, "the overlap context must be live")
      runBenchmark(OverlapScenarioName) {
        val benchmark = new Benchmark(OverlapComparisonName, OverlapRecords.toLong,
          MeasuredIterations, output = output)
        benchmark.addCase(OverlapLiveCaseName) { _ =>
          overlap.observeLive(measureOverlapPath(overlapContext, attachDuringProduction = true))
        }
        benchmark.addCase(OverlapRetainedCaseName) { _ =>
          overlap.observeRetained(
            measureOverlapPath(overlapContext, attachDuringProduction = false))
        }
        benchmark.run()
      }
      stopActiveContext()
      val telemetryCpu = measureTelemetryCpuOverhead()
      emitReport(master, baseline, streaming, cpuBaseline, cpuStreaming, telemetryCpu,
        overlapContext, overlap)
    } finally {
      stopActiveContext()
    }
  }

  override def afterAll(): Unit = {
    stopActiveContext()
  }

  private def masterFrom(mainArgs: Array[String]): String = {
    mainArgs.headOption.map(argument => argument.trim).filter(_.nonEmpty).getOrElse(ClusterMaster)
  }

  /** Runs the reference workload once for a case and records what it cost. */
  private def runCase(observation: CaseObservation): Unit = {
    val context = contextFor(observation)
    observation.recorder.beginWindow()
    val startedAt = System.nanoTime()
    val workload = observeGroupedWorkload(groupedReferenceWorkload(context))
    val elapsedNanos = System.nanoTime() - startedAt
    drainListenerBus(context)
    val metrics = observation.recorder.finishWindow()
    observation.observeRun(elapsedNanos, workload, metrics)
    observation.observeShapeFootprint(shapeName(PartitionCount), workload.memory)
    if (observation.runCount == 1) {
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

  private def runCpuCase(observation: CaseObservation): Unit = {
    val context = contextFor(observation)
    observation.recorder.beginWindow()
    val startedAt = System.nanoTime()
    val workload = observeGroupedWorkload(cpuBoundWorkload(context))
    val elapsedNanos = System.nanoTime() - startedAt
    drainListenerBus(context)
    observation.observeRun(elapsedNanos, workload, observation.recorder.finishWindow())
  }

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

  private def shapeName(partitions: Int): String = {
    if (partitions == PartitionCount) {
      s"$partitions partitions, ${DatasetBytes / BytesPerMebibyte} MiB (reference)"
    } else {
      s"$partitions partitions, ${ShapeProbeBytes / BytesPerMebibyte} MiB"
    }
  }

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

  private def overlapConf(): SparkConf = {
    streamingConf().set(SHUFFLE_COMPRESS, false)
  }

  /**
   * Streams one overlap run and returns what it cost and what it moved.
   *
   * @param attachDuringProduction whether the consumer is started at the first block or only
   *     once production has finished
   */
  private def measureOverlapPath(
      observation: CaseObservation,
      attachDuringProduction: Boolean): OverlapRun = {
    val context = contextFor(observation)
    val manager = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]
    // A shuffle of its own per run, so no run inherits another's registration state.
    val dependency = shuffleDependencyFor(context, context.getConf,
      numPartitions = OverlapPartitions, numRecords = 1)
    val handle = dependency.shuffleHandle.asInstanceOf[StreamingShuffleHandle[Int, Int, Int]]
    val attemptId = nextOverlapAttemptId
    nextOverlapAttemptId += OverlapAttemptStride
    val writerContext = newTaskContext(context.env, partitionId = 0,
      taskAttemptId = attemptId, numPartitions = OverlapPartitions)
    val readerContext = newTaskContext(context.env, partitionId = 0,
      taskAttemptId = attemptId + 1L, numPartitions = OverlapPartitions)

    val writerReady = new CountDownLatch(1)
    val productionFinished = new CountDownLatch(1)
    val consumptionFinished = new CountDownLatch(1)
    val producing = new AtomicBoolean(true)
    val produced = new AtomicInteger(0)
    val consumed = new AtomicInteger(0)
    val consumedDuringProduction = new AtomicInteger(0)
    val consumptionDrained = new AtomicBoolean(false)
    val writerHandle = new AtomicReference[StreamingShuffleWriter[Int, Int, Int]](null)
    val producerFailure = new AtomicReference[Throwable](null)
    val consumerFailure = new AtomicReference[Throwable](null)
    // Each half stamps the instant it stopped working, so the elapsed window below closes when the
    // two halves finished rather than when the settlement that follows them finished.
    val producerFinishedAt = new AtomicLong(0L)
    val consumerFinishedAt = new AtomicLong(0L)

    val producer = new Thread(() => {
      try {
        TaskContext.setTaskContext(writerContext)
        val writer = manager
          .getWriter[Int, Int](handle, OverlapMapId, writerContext,
            writerContext.taskMetrics.shuffleWriteMetrics)
          .asInstanceOf[StreamingShuffleWriter[Int, Int, Int]]
        writerHandle.set(writer)
        writerReady.countDown()
        writer.write(overlapRecords(produced))
        producing.set(false)
        productionFinished.countDown()
        // The end of stream is signalled by `write` itself, so the consumer can finish before this
        // producer stops.
        consumptionDrained.set(
          consumptionFinished.await(OverlapJoinTimeoutMillis, TimeUnit.MILLISECONDS))
        writer.stop(success = true)
      } catch {
        case failure: Throwable => producerFailure.set(failure)
      } finally {
        producing.set(false)
        writerReady.countDown()
        productionFinished.countDown()
        producerFinishedAt.set(System.nanoTime())
        TaskContext.unset()
      }
    }, "streaming-shuffle-benchmark-overlap-producer")

    val consumer = new Thread(() => {
      try {
        TaskContext.setTaskContext(readerContext)
        val records = manager
          .getReader[Int, Int](handle, 0, Int.MaxValue, 0, 1, readerContext,
            readerContext.taskMetrics.createTempShuffleReadMetrics())
          .read()
        while (records.hasNext) {
          records.next()
          consumed.incrementAndGet()
          if (producing.get()) {
            consumedDuringProduction.incrementAndGet()
          }
        }
      } catch {
        case failure: Throwable => consumerFailure.set(failure)
      } finally {
        consumptionFinished.countDown()
        consumerFinishedAt.set(System.nanoTime())
        TaskContext.unset()
      }
    }, "streaming-shuffle-benchmark-overlap-consumer")

    val startedAt = System.nanoTime()
    // The shared settlement owns both halves.
    runBoundedThreadedScenario(
      threads = Seq(producer, consumer),
      taskContexts = Seq(writerContext, readerContext),
      description = "an overlap run",
      joinTimeoutMillis = OverlapJoinTimeoutMillis,
      releaseWaits = () => {
        writerReady.countDown()
        productionFinished.countDown()
        consumptionFinished.countDown()
      }) {
      producer.start()
      if (attachDuringProduction) {
        requireOverlapPrecondition(writerReady, "the producer to have been given a writer")
      } else {
        requireOverlapPrecondition(productionFinished, "production to have finished")
      }
      consumer.start()
    }
    val elapsedNanos = math.max(producerFinishedAt.get(), consumerFinishedAt.get()) - startedAt
    requireOverlapRunSucceeded(producerFailure, consumerFailure, consumptionDrained.get())
    val run = overlapRunOf(elapsedNanos, writerHandle.get(), writerContext, produced, consumed,
      consumedDuringProduction)
    // The registration is released last: after both task contexts have completed, so it outlives
    // every reservation taken against it, and only on the success path, so a cleanup failure can
    // never mask the failure that caused it.
    manager.unregisterShuffle(handle.shuffleId)
    run
  }

  private def overlapRecords(produced: AtomicInteger): Iterator[Product2[Int, Int]] = {
    new Iterator[Product2[Int, Int]] {
      override def hasNext: Boolean = produced.get() < OverlapRecords

      override def next(): Product2[Int, Int] = {
        val index = produced.getAndIncrement()
        (index * OverlapPartitions, index)
      }
    }
  }

  /** Fails the run rather than reporting a measurement that did not happen. */
  private def requireOverlapRunSucceeded(
      producerFailure: AtomicReference[Throwable],
      consumerFailure: AtomicReference[Throwable],
      consumptionDrained: Boolean): Unit = {
    if (producerFailure.get() != null) {
      throw new IllegalStateException(
        "the overlap producer failed to stream its output", producerFailure.get())
    }
    if (consumerFailure.get() != null) {
      throw new IllegalStateException(
        "the overlap consumer failed to read its partition", consumerFailure.get())
    }
    if (!consumptionDrained) {
      throw new IllegalStateException(
        s"an overlap run's producer waited ${OverlapJoinTimeoutMillis} ms for its consumer to " +
          s"finish reading and the wait expired, so the durable figure would describe a window " +
          s"this measurement created by stopping early rather than one its consumer left")
    }
  }

  private def requireOverlapPrecondition(latch: CountDownLatch, awaited: String): Unit = {
    val opened = latch.await(OverlapJoinTimeoutMillis, TimeUnit.MILLISECONDS)
    if (!opened) {
      throw new IllegalStateException(
        s"an overlap run waited ${OverlapJoinTimeoutMillis} ms for $awaited before attaching its " +
          s"consumer and the wait expired, so the attachment instant this case is defined by " +
          s"never happened")
    }
  }

  private def overlapRunOf(
      elapsedNanos: Long,
      writer: StreamingShuffleWriter[Int, Int, Int],
      writerContext: TaskContextImpl,
      produced: AtomicInteger,
      consumed: AtomicInteger,
      consumedDuringProduction: AtomicInteger): OverlapRun = {
    val metrics = writerContext.taskMetrics
    new OverlapRun(
      elapsedNanos,
      produced.get().toLong,
      consumed.get().toLong,
      consumedDuringProduction.get().toLong,
      if (writer == null) 0L else writer.blocksStreamed,
      metrics.shuffleWriteMetrics.bytesWritten,
      metrics.diskBytesSpilled,
      if (writer == null) 0L else writer.spillsObserved,
      if (writer == null) None else writer.standDownDescription)
  }

  /** The context for a case, started on first use and reused for that case's later iterations. */
  private def contextFor(observation: CaseObservation): SparkContext = {
    activeContext match {
      case Some(context) if activeCase.exists(current => current eq observation) => context
      case _ =>
        stopActiveContext()
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

  private def awaitExecutors(context: SparkContext, master: String): Unit = {
    if (master.startsWith(ClusterMasterPrefix)) {
      TestUtils.waitUntilExecutorsUp(context, ExecutorCount, ExecutorStartupTimeoutMillis)
    }
  }

  private def stopActiveContext(): Unit = {
    activeContext.foreach(context => context.stop())
    activeContext = None
    activeCase = None
  }

  private def drainListenerBus(context: SparkContext): Unit = {
    context.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
  }

  private def shuffleManagerClassName(): String = {
    val env = SparkEnv.get
    if (env == null || env.shuffleManager == null) {
      UnknownManagerName
    } else {
      env.shuffleManager.getClass.getSimpleName
    }
  }

  /** The four `shuffle.streaming` metrics as they stand, read through the metrics registry. */
  private def telemetrySnapshot(): StreamingTelemetry = {
    new StreamingTelemetry(
      observedBufferUtilizationPercent(),
      observedSpillCount(),
      observedBackpressureEvents(),
      observedPartialReadInvalidations(),
      1)
  }

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

  private def telemetryOperationCount: Long = {
    val sampledEvents =
      (TelemetryCpuWorkItems + TelemetryCpuSampleInterval - 1) / TelemetryCpuSampleInterval
    sampledEvents.toLong * TelemetryOpsPerSampledEvent.toLong
  }

  private def telemetryNanosPerOperation(differenceNanos: Long): Long = {
    val operations = telemetryOperationCount
    if (operations <= 0L || differenceNanos <= 0L) 0L else differenceNanos / operations
  }

  private def medianLong(values: Seq[Long]): Long = {
    val ordered = values.sorted
    ordered((ordered.size - 1) / 2)
  }

  private def emitReport(
      master: String,
      baseline: CaseObservation,
      streaming: CaseObservation,
      cpuBaseline: CaseObservation,
      cpuStreaming: CaseObservation,
      telemetryCpu: TelemetryCpuObservation,
      overlapContext: CaseObservation,
      overlap: OverlapObservation): Unit = {
    emit(
      workloadSection(master, baseline, streaming) ++
        activationSection(baseline, streaming) ++
        latencySection(baseline, streaming) ++
        overlapSection(overlapContext, overlap) ++
        cpuBoundSection(cpuBaseline, cpuStreaming) ++
        memorySection(baseline, streaming) ++
        spillSection(baseline, streaming) ++
        writeAmplificationSection(baseline, streaming) ++
        bandwidthSection(baseline, streaming) ++
        telemetrySection(baseline, streaming) ++
        telemetryCpuSection(telemetryCpu) ++
        Seq(ReportRule))
  }

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
   * The overlap itself: the same volume delivered to a consumer attached during production, against
   * the same volume delivered to a consumer attached after it.
   */
  private def overlapSection(
      contextCase: CaseObservation,
      overlap: OverlapObservation): Seq[String] = {
    val liveRuns = overlap.liveRuns
    val retainedRuns = overlap.retainedRuns
    val bestReduction =
      reductionTenths(overlap.bestNanos(retainedRuns), overlap.bestNanos(liveRuns))
    val meanReduction =
      reductionTenths(overlap.meanNanos(retainedRuns), overlap.meanNanos(liveRuns))
    Seq(
      "",
      "Producer/consumer overlap, one volume delivered two ways on the streaming path",
      row("workload", OverlapComparisonName),
      row("master, shared by both arms", OverlapMaster),
      row("shuffle manager in service", contextCase.managerInService),
      row("processors available to this JVM",
        Runtime.getRuntime.availableProcessors.toString),
      row("shuffle compression", "off, so pipelining is not hidden behind a codec's buffering"),
      row(OverlapLiveCaseName, overlapElapsedDescription(overlap, liveRuns)),
      row(OverlapRetainedCaseName, overlapElapsedDescription(overlap, retainedRuns)),
      row("reduction on best time", s"${renderTenths(bestReduction)} percent"),
      row("reduction on mean time", s"${renderTenths(meanReduction)} percent"),
      row(s"every run, $OverlapLiveCaseName", overlap.samples(liveRuns)),
      row(s"every run, $OverlapRetainedCaseName", overlap.samples(retainedRuns)),
      row("records read while the producer was producing",
        s"${overlap.peak(liveRuns, _.recordsConsumedDuringProduction)} attached during, " +
          s"${overlap.peak(retainedRuns, _.recordsConsumedDuringProduction)} attached after, " +
          s"of $OverlapRecords"),
      row("blocks streamed per run",
        s"${overlap.peak(liveRuns, _.blocksStreamed)} attached during, " +
          s"${overlap.peak(retainedRuns, _.blocksStreamed)} attached after"),
      row("bytes streamed per run",
        s"${overlap.peak(liveRuns, _.streamedBytes)} attached during, " +
          s"${overlap.peak(retainedRuns, _.streamedBytes)} attached after"),
      row("bytes made durable at the producer's stop",
        s"${overlap.trough(liveRuns, _.durableBytes)} attached during, " +
          s"${overlap.trough(retainedRuns, _.durableBytes)} attached after"),
      row("spill events observed per run",
        s"${overlap.peak(liveRuns, _.spillsObserved)} attached during, " +
          s"${overlap.peak(retainedRuns, _.spillsObserved)} attached after"),
      row("every record delivered on both arms", overlap.deliveredEverything.toString),
      row("stand-downs observed",
        if (overlap.standDowns.isEmpty) "none" else overlap.standDowns.mkString("; "))) ++
      OverlapAttributionNote
  }

  private def overlapElapsedDescription(
      overlap: OverlapObservation,
      runs: Seq[OverlapRun]): String = {
    if (runs.isEmpty) {
      "no run recorded"
    } else {
      s"best ${millisOf(overlap.bestNanos(runs))} ms, mean ${millisOf(overlap.meanNanos(runs))} " +
        s"ms over ${runs.size} run(s)"
    }
  }

  /**
   * CPU-bound comparison, with identical deterministic compute wrapped around both shuffle paths.
   *
   * @param baseline the sort-based CPU-bound case
   * @param streaming the streaming CPU-bound case
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

  private def runSamples(observation: CaseObservation): String = {
    val samples = observation.elapsedNanosSamples.map(nanos => millisOf(nanos).toString)
    if (samples.isEmpty) "no run recorded" else s"${samples.mkString(", ")} ms"
  }

  /** Full executor memory, sampled in the tasks that consume each workload. */
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

  private def spillCountDescription(observation: CaseObservation): String = {
    if (observation.telemetry.available) observation.telemetry.spillCount.toString
    else "unavailable"
  }

  private def pressureSpillRateDescription(observation: CaseObservation): String = {
    pressureSpillRateTenths(observation)
      .map(tenths => s"${renderTenths(tenths)} percent")
      .getOrElse("unavailable")
  }

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

  private def amplificationDescription(observation: CaseObservation): String = {
    val written = observation.shuffleRecordsWritten
    val read = observation.shuffleRecordsRead
    val amplification = percentTenths(written - read, read)
    s"$written written, $read read, ${renderTenths(amplification)} percent amplification"
  }

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

  private def telemetryValue(telemetry: StreamingTelemetry, value: Long): String = {
    if (telemetry.available) value.toString else "unavailable"
  }

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

  private def emit(lines: Seq[String]): Unit = {
    val text = lines.mkString("", "\n", "\n")
    output.foreach(stream => stream.write(text.getBytes(StandardCharsets.UTF_8)))
    // scalastyle:off println
    lines.foreach(line => println(line))
    // scalastyle:on println
  }

  private def row(label: String, value: String): String = {
    val padding = math.max(1, LabelWidth - label.length)
    s"  $label${" " * padding}$value"
  }

  private def elapsedDescription(observation: CaseObservation): String = {
    s"best ${millisOf(observation.bestNanos)} ms, mean ${millisOf(observation.meanNanos)} ms"
  }

  private def spillDescription(observation: CaseObservation): String = {
    s"${observation.memoryBytesSpilled} bytes in memory, " +
      s"${observation.diskBytesSpilled} bytes to disk"
  }

  private def bandwidthDescription(observation: CaseObservation): String = {
    val bytesPerRun = perRun(observation.shuffleRemoteBytesRead, observation.runCount)
    s"${renderTenths(mebibytesPerSecondTenths(bytesPerRun, observation.bestNanos))} MB/s"
  }

  private def spillRateTenths(observation: CaseObservation): Long = {
    percentTenths(observation.diskBytesSpilled, observation.shuffleBytesWritten)
  }

  /**
   * A case's spill rate attributable to memory pressure, which is the figure the acceptance target
   * names.
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

  private def renderTenths(tenths: Long): String = {
    val sign = if (tenths < 0L) "-" else ""
    val magnitude = math.abs(tenths)
    s"$sign${magnitude / 10L}.${magnitude % 10L}"
  }

  private def percentTenths(part: Long, whole: Long): Long = {
    if (whole <= 0L) 0L else part * TenthsPerWhole / whole
  }

  private def reductionTenths(baseline: Long, measured: Long): Long = {
    percentTenths(baseline - measured, baseline)
  }

  private def mebibytesPerSecondTenths(bytes: Long, nanos: Long): Long = {
    if (bytes <= 0L || nanos <= 0L) {
      0L
    } else {
      val millis = math.max(1L, TimeUnit.NANOSECONDS.toMillis(nanos))
      bytes * 10L * MillisPerSecond / (millis * BytesPerMebibyte)
    }
  }

  private def perRun(total: Long, runs: Int): Long = {
    if (runs <= 0) 0L else total / runs.toLong
  }

  private def millisOf(nanos: Long): Long = TimeUnit.NANOSECONDS.toMillis(nanos)

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

  /** Isolated task-metric window for one reference run or one shape probe. */
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
   * One overlap run: what it delivered, what it cost, and what it had to make durable.
   *
   * @param elapsedNanos wall time from just before the producer started to both halves finished
   * @param recordsConsumed records the consumer read, which must equal the above
   * @param recordsConsumedDuringProduction records read before production finished, which is
   *     how much overlap this run actually contained
   * @param streamedBytes bytes the writer reported on the task's shuffle write accumulator
   * @param durableBytes bytes that reached disk, reported on the task's spill accumulator
   */
  private class OverlapRun(
      val elapsedNanos: Long,
      val recordsProduced: Long,
      val recordsConsumed: Long,
      val recordsConsumedDuringProduction: Long,
      val blocksStreamed: Long,
      val streamedBytes: Long,
      val durableBytes: Long,
      val spillsObserved: Long,
      val standDownDescription: Option[String])

  /** Both arms of the overlap comparison, accumulated as the harness runs them. */
  private class OverlapObservation {

    private val live = new mutable.ArrayBuffer[OverlapRun]()

    private val retained = new mutable.ArrayBuffer[OverlapRun]()

    def observeLive(run: OverlapRun): Unit = {
      live += run
    }

    def observeRetained(run: OverlapRun): Unit = {
      retained += run
    }

    def liveRuns: Seq[OverlapRun] = live.toSeq

    def retainedRuns: Seq[OverlapRun] = retained.toSeq

    def bestNanos(runs: Seq[OverlapRun]): Long = {
      if (runs.isEmpty) 0L else runs.map(_.elapsedNanos).min
    }

    def meanNanos(runs: Seq[OverlapRun]): Long = {
      if (runs.isEmpty) 0L else runs.map(_.elapsedNanos).sum / runs.size.toLong
    }

    def samples(runs: Seq[OverlapRun]): String = {
      if (runs.isEmpty) {
        "no run recorded"
      } else {
        s"${runs.map(run => millisOf(run.elapsedNanos).toString).mkString(", ")} ms"
      }
    }

    def peak(runs: Seq[OverlapRun], figure: OverlapRun => Long): Long = {
      if (runs.isEmpty) 0L else runs.map(figure).max
    }

    def trough(runs: Seq[OverlapRun], figure: OverlapRun => Long): Long = {
      if (runs.isEmpty) 0L else runs.map(figure).min
    }

    def deliveredEverything: Boolean = {
      val runs = live ++ retained
      runs.nonEmpty && runs.forall { run =>
        run.recordsProduced == OverlapRecords.toLong && run.recordsConsumed == run.recordsProduced
      }
    }

    def standDowns: Seq[String] = (live ++ retained).flatMap(_.standDownDescription).toSeq
  }

  /** Everything one case of the comparison is, and everything it turned out to cost. */
  private class CaseObservation(val caseName: String, val conf: SparkConf, val master: String) {

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

    /** Every run's elapsed time, in the order the runs happened. */
    private val elapsedSamples = new mutable.ArrayBuffer[Long]()

    private val shapeFootprints = new mutable.LinkedHashMap[String, MemoryFootprint]()

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

    def observeShapeFootprint(shapeName: String, footprint: MemoryFootprint): Unit = {
      shapeFootprints(shapeName) =
        shapeFootprints.get(shapeName).map(_.max(footprint)).getOrElse(footprint)
    }

    def shapeFootprint(shapeName: String): MemoryFootprint =
      shapeFootprints.getOrElse(shapeName, EmptyMemoryFootprint)

    def measuredShapes: Seq[String] = shapeFootprints.keys.toSeq

    def recordManagerInService(managerClassName: String): Unit = {
      manager = managerClassName
    }

    def runCount: Int = runs

    def bestNanos: Long = if (runs == 0) 0L else bestElapsedNanos

    def meanNanos: Long = if (runs == 0) 0L else totalElapsedNanos / runs.toLong

    def worstNanos: Long = if (elapsedSamples.isEmpty) 0L else elapsedSamples.max

    def elapsedNanosSamples: Seq[Long] = elapsedSamples.toSeq

    def groupsProduced: Long = groups

    def managerInService: String = manager

    def telemetry: StreamingTelemetry = lastTelemetry

    def memoryFootprint: MemoryFootprint = fullMemory

    def taskEndCount: Long = taskEnds

    def memoryBytesSpilled: Long = memorySpilled

    def diskBytesSpilled: Long = diskSpilled

    def peakExecutionMemory: Long = taskPeakExecutionMemory

    def shuffleBytesWritten: Long = bytesWritten

    def shuffleRecordsWritten: Long = recordsWritten

    def shuffleRemoteBytesRead: Long = remoteBytesRead

    def shuffleTotalBytesRead: Long = totalBytesRead

    def shuffleRecordsRead: Long = recordsRead

    def fetchWaitTime: Long = fetchWait
  }

  /**
   * A reading of the four `shuffle.streaming` metrics, taken together so they describe one moment.
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

  /** Accumulates the task metrics Spark already reports, for every task a case runs. */
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
