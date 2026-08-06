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
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.matching.Regex

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{LogEvent, Logger => Log4jLogger, LoggerContext, StringLayout}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.impl.Log4jLogEvent
import org.apache.logging.log4j.message.SimpleMessage

import org.apache.spark.{FetchFailed, HashPartitioner, LocalSparkContext, ShuffleDependency,
  SparkConf, SparkContext, SparkFunSuite, TaskContext, TaskFailedReason}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.LogKeys.{BYTE_SIZE, COUNT, DELAY, DURATION, MAX_SIZE, MIN_SIZE,
  NEW_VALUE, NUM_BLOCKS, NUM_BYTES, NUM_CHUNKS, NUM_CONCURRENT_WRITER, NUM_EVENTS, NUM_FAILURES,
  NUM_ITERATIONS, NUM_RECORDS_READ, NUM_REQUESTS, NUM_RETRIES, NUM_ROWS, NUM_TASKS, OLD_VALUE,
  PERCENT, RECORDS, THRESHOLD, TOTAL, TOTAL_TIME, VALUE}
import org.apache.spark.internal.config.{LISTENER_BUS_EVENT_QUEUE_CAPACITY, SHUFFLE_MANAGER,
  SHUFFLE_STREAMING_ENABLED, SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, STAGE_MAX_CONSECUTIVE_ATTEMPTS,
  TASK_MAX_FAILURES, UNSAFE_EXCEPTION_ON_MEMORY_LEAK}
import org.apache.spark.internal.config.Status.{MAX_RETAINED_JOBS, MAX_RETAINED_STAGES,
  MAX_RETAINED_TASKS_PER_STAGE}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{JobSucceeded, SparkListener, SparkListenerJobEnd,
  SparkListenerJobStart, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.util.{Clock, Utils}

/** The five-minute continuous streaming shuffle stress workload. */
class StreamingShuffleStressSuite
  extends SparkFunSuite
  with LocalSparkContext
  with StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  private val MaxTaskFailures: Int = 4

  private val StressMaster: String = s"local[$StressConcurrentTasks,$MaxTaskFailures]"

  private val BaselineMaster: String = "local[2]"

  private val ShuffleWidths: Seq[Int] = Seq(2, 4, 6, 8, 10)

  private val MapTasksPerIteration: Int = ShuffleWidths.sum

  private val InjectedFailuresPerIteration: Int =
    MapTasksPerIteration * StressFailureInjectionPercent / 100

  private val RecordsPerPartition: Int = 1024

  private val RecordsBeforeInjectedFailure: Int = RecordsPerPartition / 2

  private val FailureSelectionSeed: Long = 4242L

  /** Prefix every injected failure's message carries. */
  private val InjectedFailureMarker: String = "Injected streaming shuffle stress failure"

  private val NoIteration: Int = -1

  private val NoShuffleIndex: Int = -1

  private val UnfiredKeysReported: Int = 12

  private val InjectionKeyPattern: Regex =
    """\[iteration=-?\d+ shuffle=-?\d+ partition=\d+\]""".r

  private val MinIterations: Int = 8

  private val ThroughputWarmupIterations: Int = 1

  /** Hard ceiling on the loop, so a clock that never advances fails rather than spins forever. */
  private val IterationGuardLimit: Int = 8192

  private val IterationTimeoutMillis: Long = 180000L

  private val ListenerDrainTimeoutMillis: Long = 120000L

  private val DeadlineOverrunToleranceMillis: Long = 1000L

  private val ListenerQueueCapacity: Int = 200000

  private val RetainedJobCount: Int = 20

  private val RetainedStageCount: Int = 20

  private val RetainedTasksPerStage: Int = 200

  private val NanosPerSecond: Long = 1000000000L

  private val MillisPerHour: Long = 3600000L

  private val WireEvidenceWidth: Int = ShuffleWidths.max

  private val BoundedBandwidthMBps: Int = 1

  private val PacingProbeShuffleId: Int = 424242

  private val PacingProbeAttempts: Int = 4

  private val LogVolumeBudgetBytesPerHour: Long = 10L * 1024L * 1024L

  private val DuplicateRecordsPerInjectedFailure: Long = RecordsBeforeInjectedFailure.toLong

  private val DuplicateRecordsPerStageRetry: Long =
    ShuffleWidths.max.toLong * RecordsPerPartition.toLong

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Both of these are JVM-global and outlive a case by design: the metric source is an object,
    // and the buffer allowance is scoped to the executor rather than to a task.
    resetStreamingShuffleMetrics()
    MemorySpillManager.resetSharedStateForTesting()
  }

  override def afterEach(): Unit = {
    try {
      super.afterEach()
    } finally {
      MemorySpillManager.resetSharedStateForTesting()
      resetStreamingShuffleMetrics()
    }
  }

  /** The configuration the workload runs under. */
  private def stressConf(appName: String): SparkConf = {
    withLocalMaster(
      streamingConfWithOverrides(
        bufferSizePercent = DefaultBufferSizePercent,
        spillThreshold = DefaultSpillThresholdPercent,
        maxBandwidthMBps = None,
        debug = false,
        enabled = true),
      appName,
      StressMaster)
      .set(TASK_MAX_FAILURES, MaxTaskFailures)
      .set(STAGE_MAX_CONSECUTIVE_ATTEMPTS, 2 * ShuffleWidths.max + 4)
      .set(UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      .set(LISTENER_BUS_EVENT_QUEUE_CAPACITY, ListenerQueueCapacity)
      .set(MAX_RETAINED_JOBS, RetainedJobCount)
      .set(MAX_RETAINED_STAGES, RetainedStageCount)
      .set(MAX_RETAINED_TASKS_PER_STAGE, RetainedTasksPerStage)
  }

  private def boundedBandwidthConf(appName: String): SparkConf = {
    stressConf(appName)
      .set(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, BoundedBandwidthMBps)
  }

  /** The workload's dataset, optionally with a producer failure injected into chosen partitions. */
  private def stressDataset(
      context: SparkContext,
      numPartitions: Int,
      failingPartitions: Set[Int],
      iteration: Int = NoIteration,
      shuffleIndex: Int = NoShuffleIndex): RDD[(Int, String)] = {
    require(numPartitions > 0, s"numPartitions must be positive but was $numPartitions")
    val records = RecordsPerPartition
    val failAfter = math.min(RecordsBeforeInjectedFailure, records - 1)
    val messagesByPartition: Map[Int, String] = failingPartitions.iterator.map { partitionIndex =>
      partitionIndex -> (s"$InjectedFailureMarker " +
        s"${injectionKey(iteration, shuffleIndex, partitionIndex)} after $failAfter record(s)")
    }.toMap
    context.parallelize(0 until numPartitions, numPartitions).flatMap { partitionIndex =>
      (0 until records).iterator.map { recordIndex =>
        if (recordIndex == failAfter && messagesByPartition.contains(partitionIndex) &&
            TaskContext.get().attemptNumber() == 0 &&
            TaskContext.get().stageAttemptNumber() == 0) {
          throw new IllegalStateException(messagesByPartition(partitionIndex))
        }
        (partitionIndex * records + recordIndex, deterministicValue(partitionIndex, recordIndex))
      }
    }
  }

  /** The streaming block registry behind a manager's published resolver, if it has one. */
  private def streamingResolverOf(
      manager: StreamingShuffleManager): Option[StreamingShuffleBlockResolver] = {
    manager.shuffleBlockResolver match {
      case router: StreamingShuffleBlockRouter => Some(router.streamingResolver)
      case _ => None
    }
  }

  private def observedInjectionKeys(recorder: StreamingShuffleStressRecorder): Set[String] = {
    recorder.countedFailureReports.iterator
      .filter(report => report.attemptNumber == 0 &&
        report.description.contains(InjectedFailureMarker))
      .flatMap(report => InjectionKeyPattern.findFirstIn(report.description))
      .toSet
  }

  private def injectionKey(iteration: Int, shuffleIndex: Int, partitionIndex: Int): String = {
    s"[iteration=$iteration shuffle=$shuffleIndex partition=$partitionIndex]"
  }

  private def groupedStressDataset(
      context: SparkContext,
      numPartitions: Int,
      failingPartitions: Set[Int],
      iteration: Int = NoIteration,
      shuffleIndex: Int = NoShuffleIndex): RDD[(Int, Iterable[String])] = {
    stressDataset(context, numPartitions, failingPartitions, iteration, shuffleIndex)
      .groupByKey(new HashPartitioner(numPartitions))
  }

  /** Reduces a grouped shuffle to a comparable set of per-key digests. */
  private def digestOf(shuffled: RDD[(Int, Iterable[String])]): Set[(Int, Int, Int)] = {
    shuffled.map { case (key, values) =>
      val ordered = values.toSeq.sorted
      val totalLength = ordered.foldLeft(0)((running, value) => running + value.length)
      val contentHash = ordered.foldLeft(0)((running, value) => running * 31 + value.hashCode)
      (key, totalLength, contentHash)
    }.collect().toSet
  }

  private def sortBaselineDigests(): Seq[Set[(Int, Int, Int)]] = {
    val conf = withLocalMaster(
      sortBaselineConf(), "streaming-shuffle-stress-sort-baseline", BaselineMaster)
    val baselineContext = new SparkContext(conf)
    try {
      ShuffleWidths.map { numPartitions =>
        digestOf(groupedStressDataset(baselineContext, numPartitions, Set.empty))
      }
    } finally {
      baselineContext.stop()
    }
  }

  private def sortBaselineDigest(numPartitions: Int): Set[(Int, Int, Int)] = {
    val conf = withLocalMaster(
      sortBaselineConf(), s"streaming-shuffle-stress-sort-baseline-$numPartitions", BaselineMaster)
    val baselineContext = new SparkContext(conf)
    try {
      digestOf(groupedStressDataset(baselineContext, numPartitions, Set.empty))
    } finally {
      baselineContext.stop()
    }
  }

  private def failingPartitionsFor(iteration: Int): Seq[Set[Int]] = {
    val doomed = selectFailingTasks(
      MapTasksPerIteration, StressFailureInjectionPercent, FailureSelectionSeed + iteration.toLong)
    val offsets = ShuffleWidths.scanLeft(0)((running, width) => running + width)
    ShuffleWidths.indices.map { shuffleIndex =>
      val start = offsets(shuffleIndex)
      val end = start + ShuffleWidths(shuffleIndex)
      doomed.filter(index => index >= start && index < end).map(index => index - start)
    }
  }

  private def isStreamingShuffle(shuffled: RDD[_]): Boolean = {
    shuffled.dependencies.headOption.exists {
      case dependency: ShuffleDependency[_, _, _] =>
        dependency.shuffleHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]]
      case _ => false
    }
  }

  private def assertStreamingManagerInService(context: SparkContext): Unit = {
    val manager = context.env.shuffleManager
    assert(manager.isInstanceOf[StreamingShuffleManager],
      "spark.shuffle.manager=streaming must have produced a StreamingShuffleManager in the live " +
        s"environment, but the driver holds ${manager.getClass.getName}")
    assert(context.getConf.get(SHUFFLE_MANAGER) === StreamingShuffleManager.SHORT_NAME,
      "the application must have selected the streaming manager by its short name, but selected " +
        s"'${context.getConf.get(SHUFFLE_MANAGER)}'")
    assert(context.getConf.get(SHUFFLE_STREAMING_ENABLED),
      s"${SHUFFLE_STREAMING_ENABLED.key} must be set true explicitly, because it defaults to " +
        "false and a closed gate would have every call delegated to the sort-based manager")
    assert(context.getConf.get(UNSAFE_EXCEPTION_ON_MEMORY_LEAK),
      s"${UNSAFE_EXCEPTION_ON_MEMORY_LEAK.key} must be set on the configuration the executor " +
        "reads, or a retained task reservation would pass silently instead of failing the task")
  }

  private def throughputRecordsPerSecond(recordCount: Long, elapsedNanos: Long): Long = {
    require(recordCount >= 0L, s"recordCount must be non-negative but was $recordCount")
    require(elapsedNanos > 0L, s"elapsedNanos must be positive but was $elapsedNanos")
    recordCount * NanosPerSecond / elapsedNanos
  }

  private def remainingIterationTimeoutMillis(
      nowMillis: Long,
      deadlineMillis: Long): Long = {
    math.max(0L, math.min(IterationTimeoutMillis, deadlineMillis - nowMillis))
  }

  private def medianOf(values: Seq[Long]): Long = {
    require(values.nonEmpty, "a median needs at least one sample")
    val ordered = values.sorted
    ordered((ordered.size - 1) / 2)
  }

  private def degradationPercent(before: Long, after: Long): Long = {
    require(before > 0L, s"the earlier figure must be positive but was $before")
    (before - after) * 100L / before
  }

  private def logBytesPerHour(bytes: Long, elapsedMillis: Long): Long = {
    require(bytes >= 0L, s"bytes must be non-negative but was $bytes")
    require(elapsedMillis > 0L, s"elapsedMillis must be positive but was $elapsedMillis")
    bytes * MillisPerHour / elapsedMillis
  }

  private def duplicateWriteAllowance(
      injectedFailures: Long,
      retriedStageAttempts: Long): Long = {
    require(injectedFailures >= 0L,
      s"injectedFailures must be non-negative but was $injectedFailures")
    require(retriedStageAttempts >= 0L,
      s"retriedStageAttempts must be non-negative but was $retriedStageAttempts")
    injectedFailures * DuplicateRecordsPerInjectedFailure +
      retriedStageAttempts * DuplicateRecordsPerStageRetry
  }

  private def logRecordFor(message: String, thrown: Throwable): LogEvent = {
    Log4jLogEvent.newBuilder()
      .setLoggerName(s"${StreamingShuffleLoggerName}OracleProbe")
      .setLevel(Level.INFO)
      .setMessage(new SimpleMessage(message))
      .setThrown(thrown)
      .setThreadName(Thread.currentThread().getName)
      .setTimeMillis(System.currentTimeMillis())
      .build()
  }

  private def measuringStreamingLogVolume[T](
      appender: StreamingShuffleLogVolumeAppender)(body: => T): T = {
    val root = LogManager.getRootLogger.asInstanceOf[Log4jLogger]
    appender.start()
    root.addAppender(appender)
    try {
      body
    } finally {
      root.removeAppender(appender)
      appender.stop()
    }
  }

  test("the stress workload's numeric contract is the one the feature specifies") {
    assert(StressDurationMillis === 5L * 60L * 1000L,
      s"the workload runs for five minutes, but the suite reads $StressDurationMillis ms")
    assert(StressConcurrentTasks === 10,
      s"the workload runs ten concurrent tasks, but the suite reads $StressConcurrentTasks")
    assert(StressConcurrentShuffles === 5,
      s"the workload runs five concurrent shuffles, but the suite reads $StressConcurrentShuffles")
    assert(StressFailureInjectionPercent === 10,
      "the workload fails ten percent of its tasks, but the suite reads " +
        s"$StressFailureInjectionPercent percent")
    assert(MaxThroughputDegradationPercent === 5,
      "throughput may degrade by under five percent, but the suite reads " +
        s"$MaxThroughputDegradationPercent percent")
    assert(PerTestTimeoutMinutes === 20,
      s"the base suite allows twenty minutes per test, but the suite reads $PerTestTimeoutMinutes")
    assert(StressDurationMillis * 2L < PerTestTimeoutMinutes.toLong * 60L * 1000L,
      "the five minute workload must leave generous headroom inside the twenty minute per-test " +
        "timeout, because the baseline, the recovery of every injected failure and the teardown " +
        "all have to fit alongside it")

    assert(StressMaster === s"local[$StressConcurrentTasks,$MaxTaskFailures]",
      "ten task slots is what ten concurrent tasks means, and the failure allowance has to be in " +
        "the master because local[N] pins it to one whatever the configuration says, but the " +
        s"master is '$StressMaster'")
    assert(StressMaster === "local[10,4]",
      s"spelled out, the master must be local[10,4], but it is '$StressMaster'")

    assert(ShuffleWidths.size === StressConcurrentShuffles,
      s"one width per concurrent shuffle is needed, but ${ShuffleWidths.size} were declared")
    assert(ShuffleWidths.distinct.size === ShuffleWidths.size,
      "the widths must be distinct, or arbitration keyed on reduce partition count could not be " +
        s"observed at all; they are ${ShuffleWidths.mkString(", ")}")
    assert(ShuffleWidths.forall(width => width > 0),
      s"every width must be positive, but they are ${ShuffleWidths.mkString(", ")}")
    assert(MapTasksPerIteration === 30,
      s"the five widths must sum to thirty, but they sum to $MapTasksPerIteration")
    assert(MapTasksPerIteration >= StressConcurrentTasks,
      s"an iteration must offer at least $StressConcurrentTasks runnable map tasks, or ten " +
        s"could never be in flight together, but it offers $MapTasksPerIteration")
    assert(InjectedFailuresPerIteration === 3,
      s"ten percent of thirty tasks is three, but the suite derives $InjectedFailuresPerIteration")
    assert(
      InjectedFailuresPerIteration * 100 === MapTasksPerIteration * StressFailureInjectionPercent,
      s"$InjectedFailuresPerIteration of $MapTasksPerIteration tasks must be exactly " +
        s"$StressFailureInjectionPercent percent, with no rounding either way")
    assert(RecordsBeforeInjectedFailure > 0 && RecordsBeforeInjectedFailure < RecordsPerPartition,
      "the injected failure must land after the first record and before the last, or it would " +
        s"not be a mid-write failure; it lands at $RecordsBeforeInjectedFailure of " +
        s"$RecordsPerPartition")
    assert(MaxTaskFailures > 2,
      "one injected fault needs one retry, and the allowance must sit above that rather than at " +
        s"it, but it is $MaxTaskFailures")

    val firstDraw = failingPartitionsFor(0)
    val repeatDraw = failingPartitionsFor(0)
    assert(firstDraw === repeatDraw,
      "the same iteration must select the same tasks every time, or a stress failure could not " +
        s"be replayed; it selected $firstDraw and then $repeatDraw")
    val drawSizes = (0 until 20).map(iteration => failingPartitionsFor(iteration).map(_.size).sum)
    assert(drawSizes.forall(size => size === InjectedFailuresPerIteration),
      s"every iteration must select exactly $InjectedFailuresPerIteration task(s), but the first " +
        s"twenty selected ${drawSizes.distinct.mkString(", ")}")
    val distinctDraws = (0 until 20).map(iteration => failingPartitionsFor(iteration)).distinct
    assert(distinctDraws.size > 1,
      "the selection must move with the iteration rather than doom the same tasks for the whole " +
        s"run, but the first twenty iterations produced ${distinctDraws.size} distinct " +
        "selection(s)")
    failingPartitionsFor(0).zip(ShuffleWidths).foreach { case (doomed, width) =>
      assert(doomed.forall(index => index >= 0 && index < width),
        s"a doomed partition index must fall inside its own $width partition shuffle, but the " +
          s"selection was $doomed")
    }

    assert(throughputRecordsPerSecond(2000L, NanosPerSecond) === 2000L,
      "two thousand records in one second is two thousand records per second")
    assert(throughputRecordsPerSecond(1000L, NanosPerSecond / 2L) === 2000L,
      "one thousand records in half a second is also two thousand records per second")
    assert(medianOf(Seq(3L, 1L, 2L)) === 2L, "the median of an odd sample is its middle value")
    assert(medianOf(Seq(4L, 1L, 3L, 2L)) === 2L,
      "the median of an even sample takes the lower middle, so the answer is an observation")
    assert(medianOf(Seq(7L)) === 7L, "the median of one observation is that observation")
    assert(degradationPercent(1000L, 950L) === 5L,
      "a fall from a thousand to nine hundred and fifty is a five percent degradation")
    assert(degradationPercent(1000L, 1000L) === 0L, "holding level is no degradation at all")
    assert(degradationPercent(1000L, 1200L) === -20L,
      "getting faster must report a negative degradation rather than a clamped zero")
    assert(degradationPercent(1000L, 960L) < MaxThroughputDegradationPercent.toLong,
      "a four percent fall is inside the bound the workload applies")
    assert(degradationPercent(1000L, 940L) >= MaxThroughputDegradationPercent.toLong,
      "a six percent fall is outside it, so the bound is a bound rather than a formality")

    val probePeaks = new StreamingResourcePeaks(None, () => None, () => 0L)
    assert(probePeaks.reservedBytes === 0L,
      s"an unsampled mark must read zero, but read ${probePeaks.reservedBytes}")
    assert(probePeaks.registeredProducers === 0 && probePeaks.retainedFiles === 0 &&
      probePeaks.servingProducers === 0,
      s"every unsampled mark must read zero, but they read ${probePeaks.describe}")
    probePeaks.raiseFor(registeredProducers = 3, retainedFiles = 2, reservedByteCount = 4096L,
      servingProducers = 5)
    assert(probePeaks.registeredProducers === 3 && probePeaks.retainedFiles === 2 &&
      probePeaks.reservedBytes === 4096L && probePeaks.servingProducers === 5,
      s"a first reading must set every mark, but they read ${probePeaks.describe}")
    probePeaks.raiseFor(registeredProducers = 1, retainedFiles = 1, reservedByteCount = 1L,
      servingProducers = 1)
    assert(probePeaks.registeredProducers === 3 && probePeaks.retainedFiles === 2 &&
      probePeaks.reservedBytes === 4096L && probePeaks.servingProducers === 5,
      s"a smaller reading must not lower a high-water mark, but they read ${probePeaks.describe}")
    probePeaks.raiseFor(registeredProducers = 9, retainedFiles = 8, reservedByteCount = 8192L,
      servingProducers = 7)
    assert(probePeaks.registeredProducers === 9 && probePeaks.retainedFiles === 8 &&
      probePeaks.reservedBytes === 8192L && probePeaks.servingProducers === 7,
      s"a larger reading must raise every mark, but they read ${probePeaks.describe}")
    assert(remainingIterationTimeoutMillis(0L, IterationTimeoutMillis * 2L) ===
      IterationTimeoutMillis,
      "an iteration with ample remaining time must retain its ordinary timeout")
    assert(remainingIterationTimeoutMillis(900L, 1000L) === 100L,
      "an iteration near the deadline must be bounded by the hundred milliseconds remaining")
    assert(remainingIterationTimeoutMillis(1000L, 1000L) === 0L,
      "an iteration at the deadline must receive no time and must not start")
    assert(remainingIterationTimeoutMillis(1001L, 1000L) === 0L,
      "an expired workload must not produce a negative timeout")

    assert(MetricNames.size === 4,
      s"the subsystem publishes four metrics, but the suite reads ${MetricNames.size}")
    assert(streamingShuffleMetricNames() === MetricNames.toSet,
      s"the registry must hold exactly ${MetricNames.mkString(", ")}, but holds " +
        s"${streamingShuffleMetricNames().toSeq.sorted.mkString(", ")}")

    assert(LogVolumeBudgetBytesPerHour === 10L * 1024L * 1024L,
      s"the budget is ten mebibytes an hour, but the suite reads $LogVolumeBudgetBytesPerHour")
    assert(MillisPerHour === 3600000L, s"an hour is 3600000 ms, but the suite reads $MillisPerHour")
    assert(logBytesPerHour(LogVolumeBudgetBytesPerHour, MillisPerHour) ===
      LogVolumeBudgetBytesPerHour,
      "a full hour of the budget must normalise to exactly the budget")
    assert(logBytesPerHour(LogVolumeBudgetBytesPerHour / 12L, StressDurationMillis) <=
      LogVolumeBudgetBytesPerHour,
      "a twelfth of the budget over the five minute run must normalise to inside the budget, " +
        "but normalised to " +
        s"${logBytesPerHour(LogVolumeBudgetBytesPerHour / 12L, StressDurationMillis)}")
    assert(logBytesPerHour(LogVolumeBudgetBytesPerHour / 12L + 1024L, StressDurationMillis) >
      LogVolumeBudgetBytesPerHour,
      "a kibibyte more than a twelfth must normalise to outside it, or the budget would not be " +
        "enforced at the boundary")
    assert(logBytesPerHour(1L, MillisPerHour) < LogVolumeBudgetBytesPerHour,
      "one byte an hour is inside the budget, so the comparison is the right way round")
    val oracleProbe = new StreamingShuffleLogVolumeAppender(StreamingShuffleLoggerName)
    val probeMessage = "streaming shuffle log volume oracle probe"
    val probeThrowable = new IllegalStateException("streaming shuffle log volume oracle stack")
    oracleProbe.append(logRecordFor(probeMessage, probeThrowable))
    assert(oracleProbe.eventCount === 1L,
      s"the oracle must have measured the one record it was handed, not ${oracleProbe.eventCount}")
    assert(oracleProbe.unrenderedEventCount === 0L,
      "the running configuration must expose a layout for the oracle to render through, or the " +
        "measurement is an approximation wearing a rendered measurement's name")
    val messageOnlyBytes = probeMessage.getBytes(StandardCharsets.UTF_8).length.toLong
    assert(oracleProbe.byteCount > messageOnlyBytes,
      s"a rendered line with a stack trace attached must cost more than its ${messageOnlyBytes} " +
        s"byte message, but the oracle measured ${oracleProbe.byteCount}; a measurement that " +
        "omitted the layout's own fields and the throwable would report exactly the message")
    assert(oracleProbe.byteCount >
        messageOnlyBytes + probeThrowable.getStackTrace.length.toLong,
      "and it must account for the stack trace itself rather than a fixed allowance, so it must " +
        s"exceed one byte per frame of it, but measured ${oracleProbe.byteCount} for " +
        s"${probeThrowable.getStackTrace.length} frame(s)")

    assert(DuplicateRecordsPerInjectedFailure === (RecordsPerPartition / 2).toLong,
      "an injected fault dies half way through its records, so that is what its retry writes " +
        s"again, but the suite reads $DuplicateRecordsPerInjectedFailure")
    assert(DuplicateRecordsPerStageRetry === ShuffleWidths.max.toLong * RecordsPerPartition.toLong,
      "a recomputed map stage writes at most the widest shuffle's whole output again, but the " +
        s"suite reads $DuplicateRecordsPerStageRetry")
    assert(duplicateWriteAllowance(injectedFailures = 3L, retriedStageAttempts = 0L) ===
      3L * DuplicateRecordsPerInjectedFailure,
      "with no stage recomputed the allowance is the injection's own cost and nothing more")
    assert(duplicateWriteAllowance(injectedFailures = 0L, retriedStageAttempts = 2L) ===
      2L * DuplicateRecordsPerStageRetry,
      "and with no injected fault it is the recomputations' cost and nothing more")
    assert(duplicateWriteAllowance(injectedFailures = 0L, retriedStageAttempts = 0L) === 0L,
      "a run that neither injected a fault nor recomputed a stage must write nothing twice")
  }

  test("a streaming shuffle puts bytes on the wire and its consumers acknowledge them") {
    val baseline = sortBaselineDigest(WireEvidenceWidth)

    sc = new SparkContext(stressConf("streaming-shuffle-wire-evidence"))
    assertStreamingManagerInService(sc)
    val manager = sc.env.shuffleManager.asInstanceOf[StreamingShuffleManager]

    val shuffled = groupedStressDataset(sc, WireEvidenceWidth, Set.empty)
    assert(isStreamingShuffle(shuffled),
      "the shuffle must have been registered on the streaming path, or nothing below is evidence " +
        "about streaming shuffle at all")
    assertNoDataLoss(digestOf(shuffled), baseline, "the wire-evidence streaming shuffle")

    val listener = manager.boundStreamingListener
    assert(listener.isDefined,
      "the executor must hold a serving listener after a streaming shuffle, since it is the " +
        "only route to a producer's retained output once its task has ended")
    val streamedBytes = listener.get.streamedBytes
    val streamedBlocks = listener.get.streamedBlocks
    val acknowledgedBlocks = listener.get.acknowledgedBlocks
    val acceptedSessions = listener.get.acceptedSessions

    assert(acceptedSessions > 0L,
      "a consumer must have opened an egress session against a producer on this executor, but " +
        s"$acceptedSessions were accepted, which means no reduce task ever reached a producer")
    assert(streamedBlocks > 0L,
      "the transport must have carried data blocks, but the executor put " +
        s"$streamedBlocks block(s) on the wire across $acceptedSessions session(s), which is a " +
        "dormant transport rather than a streaming one")
    assert(streamedBytes > 0L,
      s"$streamedBlocks block(s) were written, so their bytes must be accounted for, but the " +
        s"executor reports $streamedBytes byte(s) on the wire")
    assert(streamedBytes >= streamedBlocks,
      s"$streamedBytes byte(s) cannot carry $streamedBlocks block(s), since every block costs at " +
        "least its framing")
    assert(acknowledgedBlocks > 0L,
      "a consumer must have acknowledged what it consumed, because an acknowledgement is what " +
        s"frees the producer's buffers, but $acknowledgedBlocks came back for $streamedBlocks " +
        "block(s) sent")
    assert(listener.get.malformedFrameCount === 0L,
      "no frame the router could not handle may have crossed the wire, but " +
        s"${listener.get.malformedFrameCount} did")
    logInfo(log"Streaming shuffle wire evidence: ${MDC(NUM_BYTES, streamedBytes)} byte(s) in " +
      log"${MDC(NUM_BLOCKS, streamedBlocks)} block(s) over " +
      log"${MDC(COUNT, acceptedSessions)} session(s), with " +
      log"${MDC(NUM_CHUNKS, acknowledgedBlocks)} acknowledgement(s)")
  }

  test("a bounded bandwidth streaming shuffle paces its egress and still produces the baseline") {
    val baseline = sortBaselineDigest(WireEvidenceWidth)

    sc = new SparkContext(boundedBandwidthConf("streaming-shuffle-bounded-bandwidth"))
    assertStreamingManagerInService(sc)
    assert(sc.getConf.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS).contains(BoundedBandwidthMBps),
      s"the run must declare a link capacity of $BoundedBandwidthMBps MB/s, or the pacing layer " +
        s"stays on its unlimited fast path, but it declared " +
        s"${sc.getConf.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)}")
    val manager = sc.env.shuffleManager.asInstanceOf[StreamingShuffleManager]

    val shuffled = groupedStressDataset(sc, WireEvidenceWidth, Set.empty)
    assert(isStreamingShuffle(shuffled),
      "a declared bandwidth cap must not stop a shuffle being streamed; it paces the streaming " +
        "rather than declining it")
    assertNoDataLoss(digestOf(shuffled), baseline, "the bandwidth-bounded streaming shuffle")

    val pacing = manager.egressPacing
    val flow = manager.flowControl
    val expectedCeiling = TokenBucketRateLimiter.applyLinkCapacityCeiling(
      TokenBucketRateLimiter.perShuffleBytesPerSecond(BoundedBandwidthMBps, 1))
    assert(pacing.aggregateBytesPerSecond.contains(expectedCeiling),
      s"the executor's aggregate must be paced at $expectedCeiling bytes/s, which is " +
        s"${TokenBucketRateLimiter.BANDWIDTH_CEILING_PERCENT} percent of the declared capacity, " +
        s"but it reports ${pacing.aggregateBytesPerSecond}")
    assert(pacing.currentShareBytesPerSecond.exists(share => share <= expectedCeiling),
      "a shuffle's own share must sit inside the executor's aggregate rather than beside it, but " +
        s"the share is ${pacing.currentShareBytesPerSecond} against a ceiling of $expectedCeiling")
    assert(!pacing.aggregateLimiter.isUnlimited,
      "a declared capacity must produce a bounded aggregate, not the unlimited fast path")

    assert(flow.egressBytes > 0L,
      "the bytes this executor put on the wire must have been admitted through the flow-control " +
        s"protocol, but it accounted for ${flow.egressBytes} of them")
    assert(flow.throttledStreamCount === 0,
      "no stream may still be throttled once every task of the run has ended, but " +
        s"${flow.throttledStreamCount} still are")
    assert(observedBackpressureEvents() === flow.backpressureEventCount,
      s"the backpressure metric read ${observedBackpressureEvents()} against the " +
        s"${flow.backpressureEventCount} transition(s) the protocol counted; a throttle an " +
        "operator cannot see is a throttle they cannot act on")

    // And the ceiling is a real bucket, exercised rather than inferred -- but exercised on a budget
    // of this case's own, not on the one the executor's writers charge.
    val probeBudget = TokenBucketRateLimiter.executorBudget(sc.getConf)
    assert(probeBudget.aggregateBytesPerSecond === pacing.aggregateBytesPerSecond,
      "the probe budget must pace to the same ceiling as the live one or its refusals say " +
        s"nothing about production: the probe reports " +
        s"${probeBudget.aggregateBytesPerSecond} against the live " +
        s"${pacing.aggregateBytesPerSecond}")
    val liveAggregateRefusalsBefore = pacing.aggregateRefusalCount
    val liveDivisorBefore = pacing.divisor
    val limiter = probeBudget.limiterFor(PacingProbeShuffleId)
    val block = limiter.maxAcquirableBytes
    assert(block > 0L && block < Long.MaxValue,
      s"a bounded limiter must publish a finite framing ceiling, but it published $block")
    var admitted = 0L
    var refused = false
    var attempts = 0
    while (!refused && attempts <= PacingProbeAttempts) {
      attempts += 1
      if (limiter.tryAcquire(block)) {
        admitted += block
      } else {
        refused = true
      }
    }
    assert(refused,
      s"a request of $block byte(s) -- the whole bucket -- must have been refused within " +
        s"$PacingProbeAttempts attempt(s), but $attempts of them were all admitted, which would " +
        "mean the executor's allowance is not being charged at all")
    assert(limiter.refusalCount > 0L,
      "the limiter must record the refusal that stopped the drain, or a paced producer would be " +
        "indistinguishable from one that was never held back")
    assert(probeBudget.aggregateRefusalCount > 0L ||
        limiter.availableTokens < limiter.capacityBytes,
      "the refusal must have come from a bucket with something spent, whether this shuffle's own " +
        "or the executor's ceiling behind it")
    assert(pacing.divisor === liveDivisorBefore,
      s"the probe must not have entered the live budget's divisor, but it moved from " +
        s"$liveDivisorBefore to ${pacing.divisor}")
    assert(pacing.aggregateRefusalCount === liveAggregateRefusalsBefore,
      "the probe must not have spent the live executor's allowance, but the live aggregate's " +
        s"refusals moved from $liveAggregateRefusalsBefore to ${pacing.aggregateRefusalCount}")
    val refusedWait = limiter.millisUntilAvailable(block)
    assert(refusedWait > 0L && refusedWait < Long.MaxValue,
      s"a refused caller must be told a finite wait but was told $refusedWait: zero schedules no " +
        "wake-up and the block it holds would never leave, while an unbounded wait would mean " +
        "the bucket can never admit a block this size however long the caller waits")
    assert(!manager.degradationPolicy.hasTripped,
      "pacing is flow control rather than a degradation, so a bounded run must not stand " +
        s"streaming down, but it did because ${manager.degradationPolicy.trippedReason}")
    logInfo(log"Streaming shuffle bounded-bandwidth run paced at " +
      log"${MDC(MAX_SIZE, expectedCeiling)} bytes/s aggregate and " +
      log"${MDC(MIN_SIZE, pacing.currentShareBytesPerSecond)} per shuffle, accounted " +
      log"${MDC(NUM_BYTES, flow.egressBytes)} egress byte(s); an isolated budget at the same " +
      log"ceiling admitted ${MDC(BYTE_SIZE, admitted)} byte(s) in " +
      log"${MDC(NUM_REQUESTS, attempts)} attempt(s) before being refused, and recorded " +
      log"${MDC(COUNT, limiter.refusalCount)} refusal(s) answered with a wait of " +
      log"${MDC(DELAY, refusedWait)} ms")
  }

  test("a five minute continuous streaming shuffle workload holds throughput and leaks nothing",
      StreamingShuffleStressTest) {
    val baselines = sortBaselineDigests()
    baselines.zip(ShuffleWidths).foreach { case (baseline, width) =>
      assert(baseline.size === width * RecordsPerPartition,
        s"the sort-based baseline of the $width partition shuffle must produce one group per " +
          s"distinct key, ${width * RecordsPerPartition} in all, but produced ${baseline.size}")
    }

    sc = new SparkContext(stressConf("streaming-shuffle-stress"))
    assertStreamingManagerInService(sc)
    val manager = sc.env.shuffleManager.asInstanceOf[StreamingShuffleManager]
    val memoryQuota = MemorySpillManager.executorQuota(sc.getConf)
    val diskQuota = MemorySpillManager.executorDiskQuota(sc.getConf, memoryQuota)

    val recorder = new StreamingShuffleStressRecorder
    sc.addSparkListener(recorder)

    val preflight = groupedStressDataset(sc, ShuffleWidths.head, Set.empty)
    assert(isStreamingShuffle(preflight),
      "the pre-flight shuffle must have been registered on the streaming path, but its handle is " +
        "not a StreamingShuffleHandle, which means the manager declined to stream it")
    assertNoDataLoss(digestOf(preflight), baselines.head, "the streaming pre-flight shuffle")

    // The five minute budget, as a deadline read from a clock.
    val clock: Clock = wallClock()
    val startedAtMillis = clock.getTimeMillis()
    val deadlineMillis = startedAtMillis + StressDurationMillis
    val recordsPerIteration = MapTasksPerIteration.toLong * RecordsPerPartition.toLong
    val throughputSamples = new mutable.ArrayBuffer[Long]()
    val plannedInjectionKeys = new mutable.HashSet[String]()
    val resourcePeaks = new StreamingResourcePeaks(
      streamingResolverOf(manager),
      () => manager.boundStreamingListener,
      () => MemorySpillManager.executorQuota(sc.getConf).reservedBytes)
    var iterations = 0
    var injectedFailures = 0L
    var requiredIterationMillis = 1L
    var remainingMillis = deadlineMillis - clock.getTimeMillis()

    val logVolume = new StreamingShuffleLogVolumeAppender(StreamingShuffleLoggerName)
    measuringStreamingLogVolume(logVolume) {
      while (remainingMillis >= requiredIterationMillis && iterations < IterationGuardLimit) {
        val iteration = iterations
        val doomedPerShuffle = failingPartitionsFor(iteration)
        injectedFailures += doomedPerShuffle.map(_.size).sum.toLong
        doomedPerShuffle.iterator.zipWithIndex.foreach { case (doomed, shuffleIndex) =>
          doomed.foreach { partitionIndex =>
            plannedInjectionKeys += injectionKey(iteration, shuffleIndex, partitionIndex)
          }
        }
        // Wall time, and only for measurement.
        val iterationStartedAtNanos = System.nanoTime()
        val iterationTimeoutMillis =
          remainingIterationTimeoutMillis(clock.getTimeMillis(), deadlineMillis)
        assert(iterationTimeoutMillis > 0L,
          s"iteration $iteration must not start without workload time remaining")
        // Five jobs released from one barrier, so the five shuffles are genuinely concurrent rather
        // than merely consecutive: without the barrier the first would usually finish before the
        // last began, and an iteration meant to exercise contention would exercise nothing.
        val outcomes = runConcurrently(
            StressConcurrentShuffles,
            s"streaming-shuffle-stress-iteration-$iteration",
            iterationTimeoutMillis) { shuffleIndex =>
          val shuffled = groupedStressDataset(
            sc, ShuffleWidths(shuffleIndex), doomedPerShuffle(shuffleIndex), iteration,
            shuffleIndex)
          val digest = digestOf(shuffled)
          resourcePeaks.sample()
          digest
        }
        resourcePeaks.sample()
        val iterationNanos = System.nanoTime() - iterationStartedAtNanos

        assert(outcomes.size === StressConcurrentShuffles,
          s"iteration $iteration must have completed all $StressConcurrentShuffles of its " +
            s"concurrent shuffles, but ${outcomes.size} returned a result")
        outcomes.zip(ShuffleWidths).zip(baselines).foreach {
          case ((observed, width), baseline) =>
            assertNoDataLoss(observed, baseline,
              s"iteration $iteration of the stress workload, its $width partition shuffle")
        }
        assert(iterationNanos > 0L,
          s"iteration $iteration must have taken measurable time, but the clock reported " +
            s"$iterationNanos ns")
        val deliveredRecords = outcomes.iterator.map(_.size.toLong).sum
        assert(deliveredRecords === recordsPerIteration,
          s"iteration $iteration must deliver all $recordsPerIteration unique records, but " +
            s"delivered $deliveredRecords")
        throughputSamples += throughputRecordsPerSecond(deliveredRecords, iterationNanos)
        val elapsedIterationMillis =
          math.max(1L, TimeUnit.NANOSECONDS.toMillis(iterationNanos))
        requiredIterationMillis = math.min(
          IterationTimeoutMillis, math.max(1L, elapsedIterationMillis * 2L))
        iterations += 1
        remainingMillis = deadlineMillis - clock.getTimeMillis()
      }
    }

    assert(iterations < IterationGuardLimit,
      s"the loop stopped at its guard of $IterationGuardLimit iteration(s) rather than at the " +
        "five minute deadline, which means the clock never advanced past it")
    assert(iterations >= MinIterations,
      s"the half-against-half throughput comparison needs at least $MinIterations iterations to " +
        s"mean anything, but the five minute budget only bought $iterations; each iteration is " +
        "therefore doing far more work than this workload intends")
    val finishedAtMillis = clock.getTimeMillis()
    val elapsedMillis = finishedAtMillis - startedAtMillis
    val unspentMillis = math.max(0L, deadlineMillis - finishedAtMillis)
    val overrunMillis = math.max(0L, finishedAtMillis - deadlineMillis)
    assert(unspentMillis < requiredIterationMillis,
      s"the workload stopped with $unspentMillis ms unused even though the next bounded " +
        s"iteration needed only $requiredIterationMillis ms")
    assert(overrunMillis <= DeadlineOverrunToleranceMillis,
      s"the five minute workload overran its deadline by $overrunMillis ms, above the " +
        s"$DeadlineOverrunToleranceMillis ms scheduling allowance")

    sc.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
    sc.removeSparkListener(recorder)

    assert(recorder.peakConcurrentTasks >= StressConcurrentTasks,
      s"$StressConcurrentTasks tasks must have been in flight together at some point in the run, " +
        s"but the peak observed across $iterations iteration(s) was " +
        s"${recorder.peakConcurrentTasks}")
    assert(recorder.peakConcurrentJobs >= StressConcurrentShuffles,
      s"$StressConcurrentShuffles shuffles must have been active together, since that is what " +
        "exercises cross-shuffle utilisation aggregation and arbitration at all, but the peak " +
        s"observed was ${recorder.peakConcurrentJobs}")
    assert(recorder.taskEndCount > 0L, "the run must have ended some tasks for a listener to read")

    val mapTasksSubmitted = iterations.toLong * MapTasksPerIteration.toLong
    assert(injectedFailures === iterations.toLong * InjectedFailuresPerIteration.toLong,
      s"the run must have injected $InjectedFailuresPerIteration failure(s) into each of its " +
        s"$iterations iteration(s), but injected $injectedFailures in total")
    assert(injectedFailures * 100L === mapTasksSubmitted * StressFailureInjectionPercent.toLong,
      s"$injectedFailures of $mapTasksSubmitted map tasks must be exactly " +
        s"$StressFailureInjectionPercent percent of them, with no rounding either way")
    val plannedKeys = plannedInjectionKeys.toSet
    assert(plannedKeys.size.toLong === injectedFailures,
      s"each of the $injectedFailures planned injection(s) must be distinct, but the plan " +
        s"produced ${plannedKeys.size} distinct key(s), which would mean two injections claimed " +
        "the same iteration, shuffle and partition")
    val observedKeys = observedInjectionKeys(recorder)
    val unfiredKeys = plannedKeys.diff(observedKeys)
    assert(unfiredKeys.isEmpty,
      s"every one of the $injectedFailures planned injection(s) must have been observed as a " +
        s"first-attempt task failure, but ${unfiredKeys.size} never fired: " +
        s"${unfiredKeys.toSeq.sorted.take(UnfiredKeysReported).mkString(", ")}" +
        (if (unfiredKeys.size > UnfiredKeysReported) ", ..." else ""))
    val unplannedKeys = observedKeys.diff(plannedKeys)
    assert(unplannedKeys.isEmpty,
      "no failure may carry an injection key this run did not plan, since that would mean the " +
        s"accounting is matching something other than its own injections, but observed: " +
        s"${unplannedKeys.toSeq.sorted.take(UnfiredKeysReported).mkString(", ")}")
    assert(recorder.failedJobCount === 0,
      "every job must have completed despite the injected failures, but " +
        s"${recorder.failedJobCount} failed outright, which would mean a failure went unrecovered")
    assert(recorder.succeededJobCount >= iterations.toLong * StressConcurrentShuffles.toLong,
      s"all ${iterations.toLong * StressConcurrentShuffles.toLong} shuffle jobs must have " +
        s"succeeded, but only ${recorder.succeededJobCount} did")
    assert(recorder.fetchFailures.forall(
      failure => failure.shuffleId >= 0 && failure.reduceId >= 0),
      "every fetch failure must name a real shuffle and reduce partition, but one reported " +
        s"${recorder.fetchFailures.find(f => f.shuffleId < 0 || f.reduceId < 0)}")

    val samples = throughputSamples.toSeq
    assert(samples.size === iterations,
      s"one throughput sample per iteration is needed, but $iterations iteration(s) produced " +
        s"${samples.size} sample(s)")
    val postWarmupSamples = samples.drop(ThroughputWarmupIterations)
    assert(postWarmupSamples.size === iterations - ThroughputWarmupIterations,
      s"excluding $ThroughputWarmupIterations warm-up iteration(s) from $iterations must leave " +
        s"${iterations - ThroughputWarmupIterations} delivered-throughput samples, but left " +
        s"${postWarmupSamples.size}")
    val firstHalf = postWarmupSamples.take(postWarmupSamples.size / 2)
    val secondHalf = postWarmupSamples.drop(postWarmupSamples.size / 2)
    assert(firstHalf.nonEmpty && secondHalf.nonEmpty,
      s"both halves of the run must hold samples, but they hold ${firstHalf.size} and " +
        s"${secondHalf.size}")
    val firstMedian = medianOf(firstHalf)
    val secondMedian = medianOf(secondHalf)
    assert(firstMedian > 0L,
      s"the first half of the run must have measurable throughput, but its median was $firstMedian")
    val medianDegradation = degradationPercent(firstMedian, secondMedian)
    val firstMean = firstHalf.sum / firstHalf.size.toLong
    val secondMean = secondHalf.sum / secondHalf.size.toLong

    assert(medianDegradation < MaxThroughputDegradationPercent.toLong,
      s"throughput must not decay by $MaxThroughputDegradationPercent percent or more under " +
        s"sustained load, but post-warm-up delivered-record throughput fell $medianDegradation " +
        s"percent, from $firstMedian to $secondMedian record(s) per elapsed second across " +
        s"$iterations iteration(s)")
    logInfo(log"Streaming shuffle stress workload ran " +
      log"${MDC(NUM_ITERATIONS, iterations)} iteration(s) of " +
      log"${MDC(NUM_CONCURRENT_WRITER, StressConcurrentShuffles)} concurrent shuffle(s) over " +
      log"${MDC(TOTAL_TIME, TimeUnit.MILLISECONDS.toSeconds(elapsedMillis))} s, injecting " +
      log"${MDC(NUM_FAILURES, injectedFailures)} failure(s) into " +
      log"${MDC(NUM_TASKS, mapTasksSubmitted)} map task(s) and observing " +
      log"${MDC(COUNT, recorder.countedFailures.size)} counted failure(s) of which " +
      log"${MDC(NUM_RETRIES, recorder.fetchFailures.size)} were fetch failures")
    logInfo(log"Streaming shuffle stress workload median throughput moved from " +
      log"${MDC(OLD_VALUE, firstMedian)} to ${MDC(NEW_VALUE, secondMedian)} record(s) per " +
      log"elapsed second, a degradation of ${MDC(PERCENT, medianDegradation)} percent against a " +
      log"bound of ${MDC(THRESHOLD, MaxThroughputDegradationPercent)} percent, with half means " +
      log"of ${MDC(MIN_SIZE, firstMean)} and ${MDC(MAX_SIZE, secondMean)} record(s) per second " +
      log"and a peak of ${MDC(NUM_TASKS, recorder.peakConcurrentTasks)} concurrent task(s); the " +
      log"bound excludes ${MDC(TOTAL, ThroughputWarmupIterations)} warm-up iteration(s) and is " +
      log"applied to delivered records per elapsed wall second")

    val servingListener = manager.boundStreamingListener
    assert(servingListener.isDefined,
      "the executor must hold a serving listener after five minutes of streaming shuffles, since " +
        "it is the only route to a producer's retained output once its task has ended")
    val streamedBytes = servingListener.get.streamedBytes
    val streamedBlocks = servingListener.get.streamedBlocks
    val acknowledgedBlocks = servingListener.get.acknowledgedBlocks
    val acceptedSessions = servingListener.get.acceptedSessions
    assert(acceptedSessions > 0L,
      s"consumers must have opened egress sessions across $iterations iteration(s) of " +
        s"$StressConcurrentShuffles concurrent shuffle(s), but $acceptedSessions were accepted")
    assert(streamedBlocks > 0L,
      s"the transport must have carried data blocks, but $streamedBlocks left this executor over " +
        s"$acceptedSessions session(s), which is a dormant transport rather than a streaming one")
    assert(streamedBytes >= streamedBlocks,
      s"$streamedBytes byte(s) cannot carry $streamedBlocks block(s), since every block costs at " +
        "least its framing")
    assert(acknowledgedBlocks > 0L,
      "consumers must have acknowledged what they consumed, because an acknowledgement is what " +
        s"frees a producer's buffers, but $acknowledgedBlocks came back for $streamedBlocks sent")

    val preflightRecords = ShuffleWidths.head.toLong * RecordsPerPartition.toLong
    val uniqueRecords = iterations.toLong * recordsPerIteration + preflightRecords
    val recordsWritten = recorder.shuffleWriteRecords
    val recordsRead = recorder.shuffleReadRecords
    assert(recordsWritten >= uniqueRecords,
      s"every one of the $uniqueRecords record(s) this run produced must appear on the shuffle " +
        s"write reporter, but it reports $recordsWritten")
    assert(recordsRead >= uniqueRecords,
      s"every record must have been read out again, but the shuffle read reporter accounts for " +
        s"$recordsRead of $uniqueRecords")
    assert(recordsRead <= recordsWritten,
      s"more records were read ($recordsRead) than were ever written ($recordsWritten), which " +
        "cannot happen and means one of the two reporters is being populated incorrectly")
    val duplicateWrites = recordsWritten - uniqueRecords
    val allowedDuplicates =
      duplicateWriteAllowance(injectedFailures, recorder.retriedStageAttempts)
    assert(duplicateWrites <= allowedDuplicates,
      s"$duplicateWrites record(s) were written more than once, above the $allowedDuplicates " +
        s"that $injectedFailures injected failure(s) and ${recorder.retriedStageAttempts} stage " +
        s"resubmission(s) account for; a duplicate the feature's own recovery cannot explain " +
        "means a shuffle already under way was abandoned and produced again")
    logInfo(log"Streaming shuffle stress workload streamed " +
      log"${MDC(NUM_BYTES, streamedBytes)} byte(s) in ${MDC(NUM_BLOCKS, streamedBlocks)} " +
      log"block(s) over ${MDC(COUNT, acceptedSessions)} session(s) with " +
      log"${MDC(NUM_CHUNKS, acknowledgedBlocks)} acknowledgement(s), wrote " +
      log"${MDC(RECORDS, recordsWritten)} record(s) and read " +
      log"${MDC(NUM_RECORDS_READ, recordsRead)} for ${MDC(NUM_ROWS, uniqueRecords)} unique, a " +
      log"duplicate write of ${MDC(VALUE, duplicateWrites)} against an allowance of " +
      log"${MDC(THRESHOLD, allowedDuplicates)} from " +
      log"${MDC(NUM_RETRIES, recorder.retriedStageAttempts)} stage resubmission(s)")

    val logBytes = logVolume.byteCount
    val hourlyLogBytes = logBytesPerHour(logBytes, elapsedMillis)
    assert(logVolume.unrenderedEventCount === 0L,
      s"${logVolume.unrenderedEventCount} of ${logVolume.eventCount} record(s) could not be " +
        "rendered through any configured layout, so the byte count below is an approximation " +
        "rather than the encoded size the budget is stated in")
    assert(logVolume.eventCount > 0L,
      "five minutes of five concurrent shuffles must have produced at least one record from the " +
        "subsystem, so a reading of zero means the measurement was attached to nothing and the " +
        "budget below would pass whatever the subsystem logged")
    assert(hourlyLogBytes < LogVolumeBudgetBytesPerHour,
      s"the subsystem emitted $logBytes rendered byte(s) in ${logVolume.eventCount} record(s) " +
        s"over $elapsedMillis ms, which is $hourlyLogBytes byte(s) an hour against a budget of " +
        s"$LogVolumeBudgetBytesPerHour with debug logging off; a record made once per map output " +
        "or once per shuffle has to be bounded on a rolling window rather than emitted every time")
    logInfo(log"Streaming shuffle stress workload emitted " +
      log"${MDC(NUM_EVENTS, logVolume.eventCount)} log record(s) costing " +
      log"${MDC(NUM_BYTES, logBytes)} rendered byte(s) over " +
      log"${MDC(DURATION, elapsedMillis)} ms, which is ${MDC(BYTE_SIZE, hourlyLogBytes)} " +
      log"byte(s) an hour against a budget of " +
      log"${MDC(THRESHOLD, LogVolumeBudgetBytesPerHour)}; " +
      log"${MDC(COUNT, logVolume.foreignEventCount)} record(s) logged by something other than " +
      log"the subsystem were excluded and " +
      log"${MDC(VALUE, logVolume.unrenderedEventCount)} could not be rendered through a " +
      log"configured layout")

    assert(recorder.peakExecutionMemory > 0L,
      "the streaming path must report peak execution memory on the existing task metric " +
        "accumulator, or the whole run would be invisible to every Spark observability surface, " +
        s"but the high water mark across the run was ${recorder.peakExecutionMemory}")
    if (recorder.memoryBytesSpilled > 0L) {
      assert(recorder.diskBytesSpilled > 0L,
        s"${recorder.memoryBytesSpilled} byte(s) of memory were released by spilling, so their " +
          "bytes must appear on the disk-spill accumulator, but it read " +
          s"${recorder.diskBytesSpilled}")
    }
    if (observedSpillCount() > 0L) {
      assert(recorder.diskBytesSpilled > 0L,
        s"${observedSpillCount()} spill event(s) were counted, so their volume must appear on " +
          "the existing disk-spill accumulator rather than only on the streaming counter, but " +
          "the accumulator read zero")
    }
    assert(streamingShuffleMetricNames() === MetricNames.toSet,
      "the subsystem must publish exactly its four documented metrics, so that spill volume is " +
        s"reported through task metrics alone, but the registry holds " +
        s"${streamingShuffleMetricNames().toSeq.sorted.mkString(", ")}")
    val invalidations = observedPartialReadInvalidations()
    if (recorder.fetchFailures.nonEmpty) {
      assert(invalidations >= 1L,
        s"${recorder.fetchFailures.size} fetch failure(s) were reported, so at least one " +
          s"partial-read invalidation must have been counted, but the counter read $invalidations")
    } else {
      assert(invalidations === 0L,
        s"no fetch failure was reported, so nothing can have invalidated a partial read, yet the " +
          s"counter read $invalidations")
    }
    assert(invalidations <= recorder.taskEndCount,
      s"an invalidation is a consumer giving up on one producer, so a run of " +
        s"${recorder.taskEndCount} task(s) cannot have counted $invalidations of them; a counter " +
        "advancing more often than that is counting something other than what it names")
    assert(observedBackpressureEvents() === manager.flowControl.backpressureEventCount,
      s"the backpressure metric read ${observedBackpressureEvents()} against the " +
        s"${manager.flowControl.backpressureEventCount} throttling transition(s) the protocol " +
        "counted; the metric and the transition it names must move together")

    // ---------------------------------------------------------------------------------------------
    // Zero retained resources, measured on the resources this subsystem actually allocates, on top
    // of the machine-enforced leak check.

    assert(resourcePeaks.registeredProducers > 0,
      "the workload must have registered streaming producers in the block registry, since that " +
        "is how streamed output is reachable by a reduce task at all, but the peak across " +
        s"$iterations iteration(s) was ${resourcePeaks.registeredProducers}: " +
        s"${resourcePeaks.describe}")
    assert(resourcePeaks.reservedBytes > 0L,
      "the workload must have reserved buffer memory against the executor allowance, since that " +
        "is the memory streaming shuffle exists to use, but the peak reservation across " +
        s"$iterations iteration(s) was ${resourcePeaks.reservedBytes} byte(s): " +
        s"${resourcePeaks.describe}")
    assert(resourcePeaks.servingProducers > 0,
      "the serving listener must have held producer sessions, since that is what a consumer's " +
        s"channel is routed through, but the peak was ${resourcePeaks.servingProducers}: " +
        s"${resourcePeaks.describe}")
    assert(recorder.peakExecutionMemory > 0L,
      "task-managed memory must have been acquired by the streaming path, but the peak read " +
        s"${recorder.peakExecutionMemory}")

    // Second half: task-scoped resources are already gone, while the context is still up.
    assert(manager.flowControl.awaitDataPlaneIdle(
      BackpressureProtocol.DATA_PLANE_SHUTDOWN_TIMEOUT_MS),
      "every accepted data-plane task must settle before task-scoped ownership is inspected")
    val leakFailures = recorder.countedFailures.filter(
      reason => reason.toErrorString.contains("Managed memory leak"))
    assert(leakFailures.isEmpty,
      s"${leakFailures.size} task(s) failed the managed-memory-leak check that this run arms " +
        "explicitly, which means task memory was left acquired at task completion")
    assert(observedBufferUtilizationPercent() === 0L,
      "no buffer may survive task completion, so the executor-wide buffer utilisation gauge must " +
        "read zero once every task has ended, but it reads " +
        s"${observedBufferUtilizationPercent()} percent")
    val producerBytes = memoryQuota.reservedBytes(MemorySpillManager.ProducerMemory)
    val consumerBytes = memoryQuota.reservedBytes(MemorySpillManager.ConsumerMemory)
    val transientBytes = memoryQuota.reservedBytes(MemorySpillManager.TransientMemory)
    val metadataBytes = memoryQuota.reservedBytes(MemorySpillManager.MetadataMemory)
    assert(producerBytes === 0L && consumerBytes === 0L && transientBytes === 0L,
      s"task cleanup must return producer, consumer and transient reservations, but producer=" +
        s"$producerBytes, consumer=$consumerBytes and transient=$transientBytes byte(s) remain")
    assert(memoryQuota.reservedBytes === metadataBytes,
      s"before manager shutdown only retained-output routing metadata may remain charged, but " +
        s"${memoryQuota.reservedBytes} total byte(s) differ from $metadataBytes metadata byte(s)")
    assert(MemorySpillManager.registeredConsumerIdentityCount === 0,
      s"task cleanup must unregister every logical consumer identity, but " +
        s"${MemorySpillManager.registeredConsumerIdentityCount} remain")
    assert(manager.activeStreamingConsumerRoutes.isEmpty,
      s"task cleanup must release every consumer route, but " +
        s"${manager.activeStreamingConsumerRoutes.size} remain active")

    sc.stop()
    // Retained streamed output is the one streaming resource DESIGNED to outlive its producing
    // task, so it is the one whose release cannot be inferred from the leak check.
    streamingResolverOf(manager).foreach { resolver =>
      assert(resolver.registeredProducerCount === 0,
        "no producer registration may survive the manager being stopped, but " +
          s"${resolver.registeredProducerCount} of a peak " +
          s"${resourcePeaks.registeredProducers} remain registered")
      assert(resolver.retainedFileCount === 0,
        "no spill file whose deletion this registry took over may survive the manager being " +
          s"stopped, but ${resolver.retainedFileCount} of a peak ${resourcePeaks.retainedFiles} " +
          "remain on disk")
      assert(resolver.isStopped,
        "the streaming block registry must have been stopped with the manager, but reports that " +
          "it is still running, which would mean its retained output has no release boundary left")
    }
    assert(manager.boundStreamingListener.forall(listener => listener.producerCount === 0),
      "no producer, and therefore no channel and no retained streamed output, may survive the " +
        "manager being stopped, but the serving listener still holds " +
        s"${manager.boundStreamingListener.map(_.producerCount).getOrElse(0)}")
    assert(manager.boundStreamingListener.forall(listener => listener.malformedFrameCount === 0L),
      "a five minute run of the wire protocol must not have produced a single frame the router " +
        "could not handle, but it produced " +
        s"${manager.boundStreamingListener.map(_.malformedFrameCount).getOrElse(0L)}")
    assert(manager.retainedStreamingProducerCount === 0,
      s"manager shutdown must release every retained producer, but the resolver still owns " +
        s"${manager.retainedStreamingProducerCount}")
    assert(diskQuota.reservedBytes === 0L && diskQuota.reservedFileCount === 0,
      s"manager shutdown must unlink every retained spill file, but the disk quota still owns " +
        s"${diskQuota.reservedBytes} byte(s) across ${diskQuota.reservedFileCount} file(s)")
    assert(memoryQuota.reservedBytes === 0L,
      s"manager shutdown must leave the aggregate heap quota empty, but " +
        s"${memoryQuota.reservedBytes} byte(s) remain")

    // Every transport retains and awaits the exact event-loop future it owns.
    assert(manager.streamingTransportsTerminated,
      "manager shutdown must complete every owned transport event-loop termination future")
    // Reported rather than asserted, deliberately.
    logInfo("Streaming shuffle stress workload left " +
      s"${manager.boundStreamingListener.map(_.unroutableFrameCount).getOrElse(0L)} unroutable " +
      s"frame(s) and ${manager.boundStreamingListener.map(_.retiredConsumerCount).getOrElse(0L)} " +
      s"retired consumer(s) behind, with ${recorder.memoryBytesSpilled} byte(s) spilled in " +
      s"memory and ${recorder.diskBytesSpilled} on disk across ${recorder.taskEndCount} task(s), " +
      s"and ${observedSpillCount()} spill event(s) counted")
  }
}

/**
 * High-water marks of the streaming resources a run actually allocates.
 *
 * @param streamingResolver the streaming block registry, absent only if the manager declined to
 *     stream at all, which the run asserts against separately
 * @param servingListener reads the serving listener on each sample rather than capturing it,
 *     because the listener is bound when the transport server starts and a sample taken before that
 *     must read absence rather than a stale value
 * @param reservedBytes reads the bytes currently reserved against the executor-wide buffer
 *     allowance, which is the one streaming resource measured as a volume rather than as a count
 */
private class StreamingResourcePeaks(
    streamingResolver: Option[StreamingShuffleBlockResolver],
    servingListener: () => Option[StreamingShuffleListener],
    reservedBytes: () => Long) {

  private var peakRegisteredProducers: Int = 0

  private var peakRetainedFiles: Int = 0

  private var peakReservedBytes: Long = 0L

  private var peakServingProducers: Int = 0

  def sample(): Unit = synchronized {
    streamingResolver.foreach { resolver =>
      peakRegisteredProducers = math.max(peakRegisteredProducers, resolver.registeredProducerCount)
      peakRetainedFiles = math.max(peakRetainedFiles, resolver.retainedFileCount)
    }
    peakReservedBytes = math.max(peakReservedBytes, reservedBytes())
    servingListener().foreach { listener =>
      peakServingProducers = math.max(peakServingProducers, listener.producerCount)
    }
  }

  /**
   * Raises the marks against one reading supplied directly, bypassing every live source.
   *
   * @param reservedByteCount reserved buffer bytes to offer the mark
   */
  def raiseFor(
      registeredProducers: Int,
      retainedFiles: Int,
      reservedByteCount: Long,
      servingProducers: Int): Unit = synchronized {
    peakRegisteredProducers = math.max(peakRegisteredProducers, registeredProducers)
    peakRetainedFiles = math.max(peakRetainedFiles, retainedFiles)
    peakReservedBytes = math.max(peakReservedBytes, reservedByteCount)
    peakServingProducers = math.max(peakServingProducers, servingProducers)
  }

  def registeredProducers: Int = synchronized(peakRegisteredProducers)

  def retainedFiles: Int = synchronized(peakRetainedFiles)

  def reservedBytes: Long = synchronized(peakReservedBytes)

  def servingProducers: Int = synchronized(peakServingProducers)

  def describe: String = synchronized {
    s"$peakRegisteredProducers registered producer(s), $peakRetainedFiles retained spill " +
      s"file(s), $peakReservedBytes reserved buffer byte(s), $peakServingProducers session(s)"
  }
}

/**
 * One task failure the scheduler counted, as the attempt it happened on and the text it reported.
 */
private case class CountedTaskFailure(attemptNumber: Int, description: String)

private object CountedTaskFailure {

  val UnknownAttempt: Int = -1
}

/**
 * Records what the scheduler did with the stress workload, so that its properties are observed
 * rather than assumed.
 */
private class StreamingShuffleStressRecorder extends SparkListener {

  private val failures = new mutable.ArrayBuffer[TaskFailedReason]()

  private val failureReports = new mutable.ArrayBuffer[CountedTaskFailure]()

  private val fetches = new mutable.ArrayBuffer[FetchFailed]()

  private var activeTasks: Int = 0

  private var peakTasks: Int = 0

  private var activeJobs: Int = 0

  private var peakJobs: Int = 0

  private var tasksEnded: Long = 0L

  private var jobsSucceeded: Long = 0L

  private var jobsFailed: Long = 0L

  private var memorySpilledTotal: Long = 0L

  private var diskSpilledTotal: Long = 0L

  private var peakMemoryHighWater: Long = 0L

  private var shuffleRecordsWritten: Long = 0L

  private var shuffleRecordsRead: Long = 0L

  private var stageAttemptsRetried: Long = 0L

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = synchronized {
    if (stageSubmitted.stageInfo.attemptNumber() > 0) {
      stageAttemptsRetried += 1L
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    activeJobs += 1
    peakJobs = math.max(peakJobs, activeJobs)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    activeJobs -= 1
    jobEnd.jobResult match {
      case JobSucceeded => jobsSucceeded += 1L
      case _ => jobsFailed += 1L
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    activeTasks += 1
    peakTasks = math.max(peakTasks, activeTasks)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    activeTasks -= 1
    tasksEnded += 1L
    val metrics: TaskMetrics = taskEnd.taskMetrics
    if (metrics != null) {
      memorySpilledTotal += metrics.memoryBytesSpilled
      diskSpilledTotal += metrics.diskBytesSpilled
      peakMemoryHighWater = math.max(peakMemoryHighWater, metrics.peakExecutionMemory)
      shuffleRecordsWritten += metrics.shuffleWriteMetrics.recordsWritten
      shuffleRecordsRead += metrics.shuffleReadMetrics.recordsRead
    }
    taskEnd.reason match {
      case fetchFailed: FetchFailed =>
        fetches += fetchFailed
        failures += fetchFailed
        recordFailureReport(taskEnd, fetchFailed)
      case failed: TaskFailedReason =>
        failures += failed
        recordFailureReport(taskEnd, failed)
      case _ =>
    }
  }

  private def recordFailureReport(
      taskEnd: SparkListenerTaskEnd,
      reason: TaskFailedReason): Unit = {
    val attempt = if (taskEnd.taskInfo != null) {
      taskEnd.taskInfo.attemptNumber
    } else {
      CountedTaskFailure.UnknownAttempt
    }
    failureReports += CountedTaskFailure(attempt, reason.toErrorString)
  }

  def peakConcurrentTasks: Int = synchronized(peakTasks)

  def peakConcurrentJobs: Int = synchronized(peakJobs)

  def countedFailures: Seq[TaskFailedReason] = synchronized(failures.toSeq)

  def countedFailureReports: Seq[CountedTaskFailure] = synchronized(failureReports.toSeq)

  def fetchFailures: Seq[FetchFailed] = synchronized(fetches.toSeq)

  def taskEndCount: Long = synchronized(tasksEnded)

  def succeededJobCount: Long = synchronized(jobsSucceeded)

  def failedJobCount: Long = synchronized(jobsFailed)

  def memoryBytesSpilled: Long = synchronized(memorySpilledTotal)

  def diskBytesSpilled: Long = synchronized(diskSpilledTotal)

  def peakExecutionMemory: Long = synchronized(peakMemoryHighWater)

  def shuffleWriteRecords: Long = synchronized(shuffleRecordsWritten)

  def shuffleReadRecords: Long = synchronized(shuffleRecordsRead)

  def retriedStageAttempts: Long = synchronized(stageAttemptsRetried)
}

/**
 * Measures how much log volume the streaming shuffle subsystem emits, without retaining any of it.
 *
 * @param loggerNamePrefix only records logged beneath this name are measured
 */
private class StreamingShuffleLogVolumeAppender(loggerNamePrefix: String)
  extends AbstractAppender("streamingShuffleLogVolume", null, null, true, Property.EMPTY_ARRAY) {

  private val events = new AtomicLong(0L)

  private val bytes = new AtomicLong(0L)

  private val foreignEvents = new AtomicLong(0L)

  private val unrenderedEvents = new AtomicLong(0L)

  private val layouts: Seq[StringLayout] = {
    LogManager.getContext(false) match {
      case context: LoggerContext =>
        context.getConfiguration.getAppenders.values().asScala.toSeq.flatMap { appender =>
          Option(appender.getLayout).collect { case layout: StringLayout => layout }
        }
      case _ => Seq.empty
    }
  }

  override def append(event: LogEvent): Unit = {
    val loggerName = Option(event.getLoggerName).getOrElse("")
    if (!loggerName.startsWith(loggerNamePrefix)) {
      foreignEvents.incrementAndGet()
    } else {
      events.incrementAndGet()
      bytes.addAndGet(renderedBytesOf(event))
    }
  }

  private def renderedBytesOf(event: LogEvent): Long = {
    val immutable = event.toImmutable
    val rendered = layouts.flatMap { layout =>
      try {
        Option(layout.toSerializable(immutable))
      } catch {
        case NonFatal(_) => None
      }
    }
    if (rendered.isEmpty) {
      unrenderedEvents.incrementAndGet()
      approximateBytesOf(immutable)
    } else {
      rendered.map(text => text.getBytes(StandardCharsets.UTF_8).length.toLong).max
    }
  }

  private def approximateBytesOf(event: LogEvent): Long = {
    val message = Option(event.getMessage).map(_.getFormattedMessage).getOrElse("")
    val thrown = Option(event.getThrown)
      .map(throwable => Utils.exceptionString(throwable))
      .getOrElse("")
    (message.getBytes(StandardCharsets.UTF_8).length +
      thrown.getBytes(StandardCharsets.UTF_8).length).toLong
  }

  def eventCount: Long = events.get()

  def byteCount: Long = bytes.get()

  def unrenderedEventCount: Long = unrenderedEvents.get()

  def foreignEventCount: Long = foreignEvents.get()
}
