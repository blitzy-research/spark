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
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.scalatest.concurrent.Eventually._
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import org.apache.spark.{FetchFailed, HashPartitioner, LocalSparkContext, ShuffleDependency,
  SparkConf, SparkContext, SparkEnv, SparkFunSuite, TaskContext, TaskContextImpl, TaskFailedReason,
  TestUtils}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.LogKeys.{BYTE_SIZE, COUNT, DURATION, MAX_SIZE, MEMORY_SIZE,
  MIN_SIZE, NUM_BLOCKS, NUM_BYTES, NUM_FAILURES, NUM_PARTITIONS, NUM_RECORDS_READ, NUM_RETRIES,
  PERCENT, REASON, RECORDS, TIME_UNITS, TOTAL}
import org.apache.spark.internal.config.{SHUFFLE_COMPRESS, SHUFFLE_MANAGER,
  SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_ENABLED,
  SHUFFLE_STREAMING_SPILL_THRESHOLD, STAGE_MAX_CONSECUTIVE_ATTEMPTS, TASK_MAX_FAILURES}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{JobSucceeded, SparkListener, SparkListenerJobEnd,
  SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.{CollectionAccumulator, LongAccumulator, ManualClock, RpcUtils}

/**
 * End-to-end integration tests for the streaming shuffle, run across real separate executor JVMs so
 * that every block crosses a socket rather than a local shortcut. The first scenario is the
 * reference workload: a 100 MiB (104,857,600 bytes) shuffle across ten partitions, measured against
 * the sort-based baseline.
 */
class StreamingShuffleIntegrationTest
  extends SparkFunSuite
  with LocalSparkContext
  with StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  private val ClusterMaster: String = "local-cluster[2,1,1024]"

  private val ExecutorCount: Int = 2

  private val ExecutorStartupTimeoutMillis: Long = 60000L

  private val OverlapPartitions: Int = 2

  private val OverlapRecordCount: Int = 1000000

  private val OverlapBlocksBeforeWait: Long = 2L

  private val OverlapDurableFractionDivisor: Long = 10L

  private val OverlapMapId: Long = 0L

  private val OverlapWriterAttemptId: Long = 9100L

  private val OverlapReaderAttemptId: Long = 9101L

  private val OverlapWaitTimeoutMillis: Long = 30000L

  private val OverlapJoinTimeoutMillis: Long = 120000L

  private val OverlapStartupTimeout: Span = 60.seconds

  private val OverlapPollInterval: Span = 20.milliseconds

  private val LiveFaultRecordCount: Int = 1000000

  private val LiveFaultBlocksBeforeWait: Long = 1L

  private val SlowConsumerRecordCount: Int = 3000000

  private val LiveFaultConditionTimeout: Span = 90.seconds

  private val ListenerDrainTimeoutMillis: Long = 60000L

  private val PartitionCount: Int = DefaultPartitionCount

  private val LargeDatasetBytes: Long = TargetDatasetBytes

  private val SlowConsumerDatasetBytes: Long = TargetDatasetBytes

  private val VerticalPartitions: Int = 4

  private val VerticalDatasetBytes: Long = 8L * BytesPerMebibyte

  private val FailurePartitionCount: Int = 2

  private val FaultInjectionDatasetBytes: Long = TargetDatasetBytes

  private val BlocksPerConsumerPermit: Long = 2L

  private val ConsumerPermitPollLimit: Int = 4

  private val ConsumerPermitPollMillis: Long = 10L

  private val PacedRecordWindow: Long = 512L

  private val PartitionHoldMillis: Long = ProducerConnectionTimeoutMillis + 2000L

  private val PartitionSweepMillis: Long = 100L

  private val PartitionCloseAttempts: Int = 30

  private val PartitionFaultReason: String =
    "an integration scenario partitioned every streaming shuffle link"

  private val RecordsBeforeProducerLoss: Int = 16

  private val MaxTaskFailures: Int = 4

  private val ConcurrentShuffleCount: Int = StressConcurrentShuffles

  private val ConcurrentPartitionCounts: Seq[Int] = Seq(2, 4, 6, 8, 10)

  private val ConcurrentJobTimeoutMillis: Long = 600000L

  private val ModestDatasetBytes: Long = 2L * 1024L * 1024L

  // Fixture identities for the in-process parts.

  private val SpillPayloadBytes: Int = 4096

  private val SpillBudgetBlocks: Long = 10L

  /** Bound on the produce-and-acknowledge loop, so a broken fixture fails rather than spins. */
  private val SpillRoundLimit: Int = 32

  private val SpillConsumerId: String = "streaming-shuffle-integration-consumer"

  private val SpillTaskAttemptId: Long = 7L

  private val CreditLimitBytes: Long = 8L * 1024L * 1024L

  // Fixture figures for the deterministic arbitration half.

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
    // the streaming buffer allowance is likewise process scoped.
    resetStreamingShuffleMetrics()
    MemorySpillManager.resetSharedStateForTesting()
  }

  override def afterEach(): Unit = {
    try {
      super.afterEach()
    } finally {
      resetStreamingShuffleMetrics()
      MemorySpillManager.resetSharedStateForTesting()
    }
  }

  private def resilientStreamingConf(appName: String): SparkConf = {
    withLocalMaster(streamingConf(), appName, ClusterMaster)
      .set(TASK_MAX_FAILURES, MaxTaskFailures)
      .set(STAGE_MAX_CONSECUTIVE_ATTEMPTS, 2 * PartitionCount + 4)
  }

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

  private def liveStreamingPair(
      appName: String,
      numPartitions: Int,
      overrides: SparkConf => SparkConf = identity): (
      StreamingShuffleManager,
      StreamingShuffleHandle[Int, Int, Int],
      TaskContextImpl,
      TaskContextImpl) = {
    val conf = overrides(streamingConf().set(SHUFFLE_COMPRESS, false))
    sc = new SparkContext(withLocalMaster(conf, appName, "local[4]"))
    assertStreamingManagerInService()
    val manager = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]
    val dependency =
      shuffleDependencyFor(sc, sc.getConf, numPartitions = numPartitions, numRecords = 1)
    val handle = dependency.shuffleHandle.asInstanceOf[StreamingShuffleHandle[Int, Int, Int]]
    val writerContext = newTaskContext(sc.env, partitionId = 0,
      taskAttemptId = OverlapWriterAttemptId, numPartitions = numPartitions)
    val readerContext = newTaskContext(sc.env, partitionId = 0,
      taskAttemptId = OverlapReaderAttemptId, numPartitions = numPartitions)
    (manager, handle, writerContext, readerContext)
  }

  private def liveFaultRecords(
      produced: AtomicInteger,
      recordCount: Int,
      numPartitions: Int,
      blocksBeforeWait: Long,
      blocksStreamed: () => Long,
      awaitConsumer: () => Unit): Iterator[Product2[Int, Int]] = {
    new Iterator[Product2[Int, Int]] {
      private var waited = false

      override def hasNext: Boolean = produced.get() < recordCount

      override def next(): Product2[Int, Int] = {
        if (!waited && blocksStreamed() >= blocksBeforeWait) {
          waited = true
          awaitConsumer()
        }
        val index = produced.getAndIncrement()
        (index * numPartitions, index)
      }
    }
  }

  private def fetchFailureIn(failure: Throwable): Option[FetchFailed] = {
    var current = failure
    var found: Option[FetchFailed] = None
    while (current != null && found.isEmpty) {
      found = current match {
        case fetchFailed: FetchFailedException =>
          fetchFailed.toTaskFailedReason match {
            case reason: FetchFailed => Some(reason)
            case _ => None
          }
        case _ => None
      }
      current = current.getCause
    }
    found
  }

  private def groupedLargeDataset(
      context: SparkContext,
      numPartitions: Int,
      totalBytes: Long): RDD[(Int, Iterable[String])] = {
    largeDataset(context, numPartitions, totalBytes).groupByKey(numPartitions)
  }

  private def groupedChainedDataset(
      context: SparkContext,
      numPartitions: Int,
      totalBytes: Long): RDD[(Int, Iterable[String])] = {
    largeDataset(context, numPartitions, totalBytes)
      .partitionBy(new HashPartitioner(numPartitions))
      .groupByKey(numPartitions)
  }

  private def lazilyConsumedDataset(
      context: SparkContext,
      numPartitions: Int,
      totalBytes: Long): RDD[(Int, String)] = {
    largeDataset(context, numPartitions, totalBytes)
      .partitionBy(new HashPartitioner(numPartitions))
  }
  private def severedTransportDigest(
      context: SparkContext,
      numPartitions: Int,
      totalBytes: Long,
      holder: LiveFaultHolder,
      severedProducers: LongAccumulator): (Set[(Int, Int, Int)], Int) = {
    val shuffled = largeDataset(context, numPartitions, totalBytes)
      .partitionBy(new HashPartitioner(numPartitions))
    holder.shuffleId = shuffled.dependencies.head
      .asInstanceOf[ShuffleDependency[Int, String, _]].shuffleId
    assert(holder.shuffleId >= 0, "the workload must have registered a shuffle to sever")
    val digest = shuffled
      .mapPartitionsWithIndex { (partitionIndex, received) =>
        severedProducers.add(holder.applyOnReduceSide(partitionIndex, 0).toLong)
        val groups = new mutable.HashMap[Int, mutable.ArrayBuffer[String]]()
        while (received.hasNext) {
          val record = received.next()
          groups.getOrElseUpdate(record._1, new mutable.ArrayBuffer[String]()) += record._2
        }
        groups.iterator.map { group =>
          val ordered = group._2.toSeq.sorted
          val totalLength = ordered.foldLeft(0)((running, value) => running + value.length)
          val contentHash = ordered.foldLeft(0)((running, value) => running * 31 + value.hashCode)
          (group._1, totalLength, contentHash)
        }
      }
      .collect()
      .toSet
    (digest, holder.shuffleId)
  }

  private def faultInjectingRead(
      shuffled: RDD[(Int, String)],
      recordsBeforeFault: Int,
      probe: ReduceSideFaultProbe): RDD[(Int, String)] = {
    val fault = probe.fault
    val egressObserved = probe.egressBytesObserved
    val recordsObserved = probe.recordsConsumedBeforeFault
    val faultsApplied = probe.faultsApplied
    val faultEffect = probe.faultEffect
    val faultOffers = probe.faultOffers
    val managerPresent = probe.managerPresent
    val firstStageAttempts = probe.firstStageAttempts
    val threshold = recordsBeforeFault
    shuffled.mapPartitionsWithIndex { (partitionIndex, records) =>
      val context = TaskContext.get()
      if (context.attemptNumber() != 0) {
        records
      } else {
        var consumed = 0
        var acted = false
        records.map { record =>
          consumed += 1
          if (!acted && consumed >= threshold) {
            acted = true
            faultOffers.add(1L)
            firstStageAttempts.add(if (context.stageAttemptNumber() == 0) 1L else 0L)
            StreamingShuffleReduceSideFault.streamingManager().foreach { manager =>
              managerPresent.add(1L)
              recordsObserved.add(consumed.toLong)
              manager.boundStreamingListener.foreach { listener =>
                egressObserved.add(listener.streamedBytes)
              }
              StreamingShuffleReduceSideFault.injectOncePerExecutor {
                faultsApplied.add(1L)
                faultEffect.add(fault.applyTo(manager).toLong)
              }
            }
          }
          record
        }
      }
    }
  }

  private def pacedRead(
      shuffled: RDD[(Int, String)],
      probe: ConsumerPacingProbe): RDD[(Int, String)] = {
    val recordsConsumed = probe.recordsConsumed
    val permitsEarned = probe.permitsEarned
    val permitsGranted = probe.permitsGranted
    val blocksObserved = probe.blocksObserved
    val blocksPerPermit = BlocksPerConsumerPermit
    val pollLimit = ConsumerPermitPollLimit
    val pollMillis = ConsumerPermitPollMillis
    val pacedWindow = PacedRecordWindow
    shuffled.mapPartitions { records =>
      val idle = new CountDownLatch(1)
      var permits = 0L
      var baselineBlocks = -1L
      var lastBlocks = 0L
      var consumed = 0L
      var earned = 0L
      var granted = 0L
      var highestBlocks = 0L
      def streamedBlocks(): Long = {
        StreamingShuffleReduceSideFault.streamingManager()
          .flatMap(_.boundStreamingListener)
          .map(_.streamedBlocks)
          .getOrElse(0L)
      }
      val paced = records.map { record =>
        if (consumed < pacedWindow) {
          if (permits <= 0L) {
            var polls = 0
            while (permits <= 0L && polls < pollLimit) {
              val blocks = streamedBlocks()
              highestBlocks = math.max(highestBlocks, blocks)
              if (baselineBlocks < 0L) {
                baselineBlocks = blocks
                lastBlocks = blocks
              }
              val fresh = (blocks - lastBlocks) / blocksPerPermit
              if (fresh > 0L) {
                permits += fresh
                earned += fresh
                lastBlocks += fresh * blocksPerPermit
              } else {
                polls += 1
                idle.await(pollMillis, TimeUnit.MILLISECONDS)
              }
            }
            if (permits <= 0L) {
              permits = 1L
              granted += 1L
            }
          }
          permits -= 1L
        }
        consumed += 1L
        record
      }
      paced ++ new Iterator[(Int, String)] {
        private var reported = false
        override def hasNext: Boolean = {
          if (!reported) {
            reported = true
            recordsConsumed.add(consumed)
            permitsEarned.add(earned)
            permitsGranted.add(granted)
            blocksObserved.add(highestBlocks)
          }
          false
        }
        override def next(): (Int, String) = throw new NoSuchElementException
      }
    }
  }

  private def digestOf(shuffled: RDD[(Int, Iterable[String])]): Set[(Int, Int, Int)] = {
    shuffled.map { case (key, values) =>
      val ordered = values.toSeq.sorted
      val totalLength = ordered.foldLeft(0)((running, value) => running + value.length)
      val contentHash = ordered.foldLeft(0)((running, value) => running * 31 + value.hashCode)
      (key, totalLength, contentHash)
    }.collect().toSet
  }

  private def keyedDigestOf(shuffled: RDD[(Int, String)]): Set[(Int, Int, Int)] = {
    shuffled.map { case (key, value) => (key, value.length, value.hashCode) }.collect().toSet
  }

  private def sortBaselineKeyedDigest(
      numPartitions: Int,
      totalBytes: Long): Set[(Int, Int, Int)] = {
    val conf = withLocalMaster(
      sortBaselineConf(), "streaming-shuffle-integration-sort-baseline", "local[2]")
    val baselineContext = new SparkContext(conf)
    try {
      keyedDigestOf(lazilyConsumedDataset(baselineContext, numPartitions, totalBytes))
    } finally {
      baselineContext.stop()
    }
  }

  private def streamingShuffleStoodDown(shuffled: RDD[_]): Boolean = {
    shuffled.dependencies.headOption.collect {
      case dependency: ShuffleDependency[_, _, _]
        if dependency.shuffleHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]] =>
        dependency.shuffleHandle.asInstanceOf[StreamingShuffleHandle[_, _, _]]
    }.exists { handle =>
      val coordinator = RpcUtils.makeDriverRef(
        StreamingShuffleCoordinator.ENDPOINT_NAME, sc.getConf, sc.env.rpcEnv)
      coordinator.askSync[StreamingShuffleFallbackState](
        GetStreamingShuffleFallbackState(handle.shuffleId, handle.capabilityToken)).fallenBack
    }
  }

  private def isStreamingKeyedShuffle(shuffled: RDD[_]): Boolean = {
    shuffled.dependencies.headOption.exists {
      case dependency: ShuffleDependency[_, _, _] =>
        dependency.shuffleHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]]
      case _ => false
    }
  }

  private def groupedOutputAsSet(
      context: SparkContext,
      numPartitions: Int,
      observedDivisors: CollectionAccumulator[java.lang.Integer]): Set[(Int, Seq[String])] = {
    groupByKeyWorkload(context, numPartitions)
      .mapValues(values => values.toSeq.sorted)
      .mapPartitions { groups =>
        StreamingShuffleReduceSideFault.streamingManager().foreach { manager =>
          observedDivisors.add(Integer.valueOf(manager.egressPacing.divisor))
        }
        groups
      }
      .collect()
      .toSet
  }

  private def failureCountingAccumulator(context: SparkContext, name: String): LongAccumulator = {
    val accumulator = new LongAccumulator
    accumulator.register(context, name = Some(name), countFailedValues = true)
    accumulator
  }

  private def newReduceSideFaultProbe(
      context: SparkContext,
      fault: StreamingShuffleReduceSideFault,
      label: String): ReduceSideFaultProbe = {
    ReduceSideFaultProbe(
      fault,
      failureCountingAccumulator(context, s"streamingShuffleEgressBytes-$label"),
      failureCountingAccumulator(context, s"streamingShuffleRecordsBeforeFault-$label"),
      failureCountingAccumulator(context, s"streamingShuffleFaultsApplied-$label"),
      failureCountingAccumulator(context, s"streamingShuffleFaultEffect-$label"),
      failureCountingAccumulator(context, s"streamingShuffleFaultOffers-$label"),
      failureCountingAccumulator(context, s"streamingShuffleManagerPresent-$label"),
      failureCountingAccumulator(context, s"streamingShuffleFirstStageAttempts-$label"))
  }

  private def sortBaselineDigest(
      numPartitions: Int,
      totalBytes: Long,
      master: String,
      chained: Boolean = false): (Set[(Int, Int, Int)], Long) = {
    val conf = withLocalMaster(
      sortBaselineConf(), "streaming-shuffle-integration-sort-baseline", master)
    val baselineContext = new SparkContext(conf)
    try {
      if (master.startsWith("local-cluster")) {
        TestUtils.waitUntilExecutorsUp(baselineContext, ExecutorCount, ExecutorStartupTimeoutMillis)
      }
      val shuffled = if (chained) {
        groupedChainedDataset(baselineContext, numPartitions, totalBytes)
      } else {
        groupedLargeDataset(baselineContext, numPartitions, totalBytes)
      }
      timed(digestOf(shuffled))
    } finally {
      baselineContext.stop()
    }
  }

  private def timed[T](body: => T): (T, Long) = {
    val startedAt = System.nanoTime()
    val result = body
    (result, System.nanoTime() - startedAt)
  }

  private def latencyReductionPercent(baselineNanos: Long, measuredNanos: Long): Long = {
    require(baselineNanos > 0L,
      s"the baseline must have taken measurable time but was $baselineNanos ns")
    (baselineNanos - measuredNanos) * PercentScale / baselineNanos
  }

  private def executorMetricObservations(
      context: SparkContext,
      probePartitions: Int): Seq[ExecutorMetricObservation] = {
    context.parallelize(0 until probePartitions, probePartitions).mapPartitions { _ =>
      val names = StreamingShuffleMetricsSource.metricRegistry.getNames.asScala.toSet
      Iterator.single(ExecutorMetricObservation(
        SparkEnv.get.executorId,
        names,
        StreamingShuffleMetricsSource.METRIC_PARTIAL_READ_INVALIDATIONS.getCount,
        StreamingShuffleMetricsSource.METRIC_SPILL_COUNT.getCount,
        StreamingShuffleMetricsSource.METRIC_BACKPRESSURE_EVENTS.getCount))
    }.collect().toSeq.distinct
  }

  private def executorFallbackObservations(
      context: SparkContext,
      probePartitions: Int): Seq[(String, Option[StreamingShuffleFallbackReason])] = {
    context.parallelize(0 until probePartitions, probePartitions).mapPartitions { _ =>
      val reason = SparkEnv.get.shuffleManager match {
        case manager: StreamingShuffleManager => manager.streamingFallbackPolicy.trippedReason
        case _ => None
      }
      Iterator.single(SparkEnv.get.executorId -> reason)
    }.collect().toSeq.distinct
  }

  private def probedExecutorMetrics(
      context: SparkContext,
      probePartitions: Int): Seq[ExecutorMetricObservation] = {
    val observations = executorMetricObservations(context, probePartitions)
    assert(observations.nonEmpty, "the metric probe must have run on at least one executor")
    observations.foreach { observation =>
      assert(observation.metricNames === MetricNames.toSet,
        s"executor ${observation.executorId} must publish exactly the four documented streaming " +
          s"metrics, but published ${observation.metricNames.toSeq.sorted.mkString(", ")}")
    }
    observations
  }

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

  test("the numeric contract these scenarios exercise is the one the feature specifies") {
    assert(LargeDatasetBytes === 100L * 1024L * 1024L,
      s"the named dataset is one hundred mebibytes, but the suite reads $LargeDatasetBytes bytes")
    assert(PartitionCount === 10,
      s"the named shuffle width is ten reduce partitions, but the suite reads $PartitionCount")
    assert(ConcurrentShuffleCount === 5,
      s"the named concurrency is five shuffles, but the suite reads $ConcurrentShuffleCount")
    assert(PerTestTimeoutMinutes === 20,
      s"the default per-test budget is twenty minutes, but the suite reads $PerTestTimeoutMinutes")

    assert(DefaultBufferSizePercent === 20 && MinBufferSizePercent === 1 &&
      MaxBufferSizePercent === 50,
      s"the buffer allowance defaults to 20 percent within [1, 50], but the suite reads " +
        s"$DefaultBufferSizePercent within [$MinBufferSizePercent, $MaxBufferSizePercent]")
    assert(DefaultSpillThresholdPercent === 80 && MinSpillThresholdPercent === 50 &&
      MaxSpillThresholdPercent === 95,
      s"the spill trigger defaults to 80 percent within [50, 95], but the suite reads " +
        s"$DefaultSpillThresholdPercent within " +
        s"[$MinSpillThresholdPercent, $MaxSpillThresholdPercent]")
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

    assert(ProducerConnectionTimeoutMillis === 5000L &&
      JustBeforeProducerTimeoutMillis === 4999L,
      s"the producer connection timeout is 5000 ms with a non-firing boundary at 4999 ms, but " +
        s"the suite reads $ProducerConnectionTimeoutMillis and " +
        s"$JustBeforeProducerTimeoutMillis")
    assert(HeartbeatIntervalMillis < ProducerConnectionTimeoutMillis &&
      HeartbeatIntervalMillis * BackpressureProtocol.HEARTBEAT_SAFETY_DIVISOR <=
        ProducerConnectionTimeoutMillis,
      s"a heartbeat is emitted every $HeartbeatIntervalMillis ms, which must leave " +
        s"${BackpressureProtocol.HEARTBEAT_SAFETY_DIVISOR - 1L} lost heartbeat(s) of margin " +
        s"inside the $ProducerConnectionTimeoutMillis ms producer detector")
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

    assertNoParallelSpillCounters()
  }

  test("a 100 MB 10 partition shuffle streams correctly and reports its 30% latency target") {
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

    val megabytes = LargeDatasetBytes / BytesPerMebibyte
    assertNoDataLoss(observed, baseline,
      s"a $PartitionCount partition streaming shuffle of $megabytes MB")
    assert(observed.size === expectedRecords,
      s"the streaming run must produce the same $expectedRecords groups, but produced " +
        s"${observed.size}")

    // The measurement is asserted to be WELL FORMED, and the reduction is reported beside the named
    // target window.
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
    assert(latencyReductionPercent(1000L, 500L) === 50L,
      "halving the elapsed time must be reported as a 50 percent reduction")
    assert(latencyReductionPercent(1000L, 2000L) === -100L,
      "a run twice as slow must be reported as a negative reduction rather than as parity")

    val reduction = latencyReductionPercent(baselineNanos, streamingNanos)
    logInfo(log"Streaming shuffle of " +
      log"${MDC(NUM_BYTES, LargeDatasetBytes / BytesPerMebibyte)} MB across " +
      log"${MDC(NUM_PARTITIONS, PartitionCount)} partitions took " +
      log"${MDC(DURATION, TimeUnit.NANOSECONDS.toMillis(streamingNanos))} ms against a " +
      log"sort-based baseline of " +
      log"${MDC(TIME_UNITS, TimeUnit.NANOSECONDS.toMillis(baselineNanos))} ms, a reduction of " +
      log"${MDC(PERCENT, reduction)} percent against the acceptance target of " +
      log"${MDC(MIN_SIZE, MinLatencyReductionPercent)} to " +
      log"${MDC(MAX_SIZE, MaxLatencyReductionPercent)} percent")
  }

  test("a consumer attached during production is served live and nothing is materialised whole") {
    val overlapConf = streamingConf().set(SHUFFLE_COMPRESS, false)
    sc = new SparkContext(
      withLocalMaster(overlapConf, "streaming-shuffle-integration-overlap", "local[4]"))
    assertStreamingManagerInService()
    val manager = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]

    val dependency = shuffleDependencyFor(sc, sc.getConf,
      numPartitions = OverlapPartitions, numRecords = 1)
    val handle = dependency.shuffleHandle.asInstanceOf[StreamingShuffleHandle[Int, Int, Int]]

    val writerContext = newTaskContext(sc.env, partitionId = 0,
      taskAttemptId = OverlapWriterAttemptId, numPartitions = OverlapPartitions)
    val readerContext = newTaskContext(sc.env, partitionId = 0,
      taskAttemptId = OverlapReaderAttemptId, numPartitions = OverlapPartitions)

    val consumerConsumedOne = new CountDownLatch(1)
    val producerFinished = new AtomicBoolean(false)
    val overlapWaitSatisfied = new AtomicBoolean(false)
    val consumedDuringProduction = new AtomicInteger(0)
    val produced = new AtomicInteger(0)
    val consumed = new AtomicInteger(0)
    val consumerFinishedReading = new CountDownLatch(1)
    val consumerDrained = new AtomicBoolean(false)
    val blocksAtWait = new AtomicLong(0L)
    val recordsAtWait = new AtomicInteger(0)
    val writerHandle = new AtomicReference[StreamingShuffleWriter[Int, Int, Int]](null)
    val producerFailure = new AtomicReference[Throwable](null)
    val consumerFailure = new AtomicReference[Throwable](null)

    val producer = new Thread(() => {
      try {
        TaskContext.setTaskContext(writerContext)
        val writer = manager
          .getWriter[Int, Int](handle, OverlapMapId, writerContext,
            writerContext.taskMetrics.shuffleWriteMetrics)
          .asInstanceOf[StreamingShuffleWriter[Int, Int, Int]]
        writerHandle.set(writer)
        var waited = false
        val records = new Iterator[Product2[Int, Int]] {
          override def hasNext: Boolean = produced.get() < OverlapRecordCount

          override def next(): Product2[Int, Int] = {
            if (!waited && writer.blocksStreamed >= OverlapBlocksBeforeWait) {
              waited = true
              blocksAtWait.set(writer.blocksStreamed)
              recordsAtWait.set(produced.get())
              overlapWaitSatisfied.set(
                consumerConsumedOne.await(OverlapWaitTimeoutMillis, TimeUnit.MILLISECONDS))
            }
            val index = produced.getAndIncrement()
            (index * OverlapPartitions, index)
          }
        }
        writer.write(records)
        producerFinished.set(true)
        consumerDrained.set(
          consumerFinishedReading.await(OverlapWaitTimeoutMillis, TimeUnit.MILLISECONDS))
        writer.stop(success = true)
      } catch {
        case failure: Throwable =>
          producerFinished.set(true)
          producerFailure.set(failure)
      } finally {
        TaskContext.unset()
      }
    }, "streaming-shuffle-overlap-producer")

    val consumer = new Thread(() => {
      try {
        TaskContext.setTaskContext(readerContext)
        val reader = manager.getReader[Int, Int](handle, 0, Int.MaxValue, 0, 1, readerContext,
          readerContext.taskMetrics.createTempShuffleReadMetrics())
        val records = reader.read()
        while (records.hasNext) {
          records.next()
          consumed.incrementAndGet()
          if (!producerFinished.get()) {
            consumedDuringProduction.incrementAndGet()
          }
          consumerConsumedOne.countDown()
        }
      } catch {
        case failure: Throwable => consumerFailure.set(failure)
      } finally {
        consumerConsumedOne.countDown()
        consumerFinishedReading.countDown()
        TaskContext.unset()
      }
    }, "streaming-shuffle-overlap-consumer")

    // The shared settlement owns both halves: it joins them inside the budget and, only if that
    // expires, releases the two latches this case's producer parks on, interrupts whatever that did
    // not free, re-joins inside a bounded grace, and completes the task contexts afterwards rather
    // than before.
    runBoundedThreadedScenario(
      threads = Seq(producer, consumer),
      taskContexts = Seq(writerContext, readerContext),
      description = "the overlap case",
      joinTimeoutMillis = OverlapJoinTimeoutMillis,
      releaseWaits = () => {
        consumerConsumedOne.countDown()
        consumerFinishedReading.countDown()
      }) {
      producer.start()
      eventually(timeout(OverlapStartupTimeout), interval(OverlapPollInterval)) {
        assert(writerHandle.get() != null || producerFailure.get() != null,
          "the producer must have been given a writer, or have failed trying")
      }
      assert(producerFailure.get() == null,
        s"the producer must have been given a streaming writer, but failed with " +
          s"${producerFailure.get()}")
      consumer.start()
    }

    assert(producerFailure.get() == null,
      s"the producer must have streamed its output without failing, but raised " +
        s"${producerFailure.get()}")
    assert(consumerFailure.get() == null,
      s"the consumer must have read its partition without failing, but raised " +
        s"${consumerFailure.get()}")

    assert(overlapWaitSatisfied.get(),
      s"the producer waited ${OverlapWaitTimeoutMillis} ms after ${blocksAtWait.get()} block(s) " +
        s"and ${recordsAtWait.get()} record(s) for the consumer to consume one, and the consumer " +
        s"did not, so no record reached a consumer while the producer was still producing; " +
        s"${consumed.get()} record(s) were read in all")
    assert(consumedDuringProduction.get() > 0,
      s"and the count of records read before the producer finished must be positive, but was " +
        s"${consumedDuringProduction.get()} of ${consumed.get()} read")

    assert(produced.get() === OverlapRecordCount,
      s"the producer must have offered every one of the $OverlapRecordCount records, but offered " +
        s"${produced.get()}")
    assert(consumed.get() === OverlapRecordCount,
      s"and the consumer must have read every one of them, but read ${consumed.get()}")

    val streamedBytes = writerContext.taskMetrics.shuffleWriteMetrics.bytesWritten
    val spilledBytes = writerContext.taskMetrics.diskBytesSpilled
    assert(streamedBytes > 0L,
      "the producer must have streamed bytes for the comparison below to mean anything")
    assert(consumerDrained.get(),
      "the consumer must have finished reading before the producer stopped, or the comparison " +
        "below is measuring a consumer that fell behind rather than one that kept pace")
    assert(spilledBytes * OverlapDurableFractionDivisor < streamedBytes,
      s"a map output served to a consumer that kept pace must not be written to disk whole, nor " +
        s"anything close to it: $spilledBytes of $streamedBytes streamed byte(s) reached disk, " +
        s"which is more than one ${OverlapDurableFractionDivisor}th of the output")
    logInfo(log"Streaming shuffle overlap: " +
      log"${MDC(NUM_RECORDS_READ, consumedDuringProduction.get())} of " +
      log"${MDC(RECORDS, consumed.get())} record(s) were read while the producer was still " +
      log"producing -- the producer paused after ${MDC(NUM_BLOCKS, blocksAtWait.get())} block(s) " +
      log"and ${MDC(COUNT, recordsAtWait.get())} record(s) -- and " +
      log"${MDC(MEMORY_SIZE, spilledBytes)} of ${MDC(BYTE_SIZE, streamedBytes)} streamed byte(s) " +
      log"needed to be made durable at the stop")
  }

  // ---------------------------------------------------------------------------------------------
  // Scenario 1c: the production scheduled vertical, and the barrier it runs into, both OBSERVED.

  test("a scheduled job is carried by the streaming path and its stage barrier is measured") {
    val baseline = sortBaselineKeyedDigest(VerticalPartitions, VerticalDatasetBytes)
    assert(baseline.nonEmpty, "the sort-based baseline must have produced records to compare with")

    sc = new SparkContext(resilientStreamingConf("streaming-shuffle-integration-vertical"))
    TestUtils.waitUntilExecutorsUp(sc, ExecutorCount, ExecutorStartupTimeoutMillis)
    assertStreamingManagerInService()
    val recorder = new StreamingShuffleJobRecorder
    sc.addSparkListener(recorder)

    val shuffled = lazilyConsumedDataset(sc, VerticalPartitions, VerticalDatasetBytes)
    assert(isStreamingKeyedShuffle(shuffled),
      "the driver must have registered this shuffle on the streaming path, or the run below says " +
        "nothing about streaming")
    val observed = keyedDigestOf(shuffled)

    assertNoDataLoss(observed, baseline,
      s"a scheduled $VerticalPartitions partition streaming shuffle")

    assert(recorder.succeededJobCount === 1L && recorder.failedJobCount === 0L,
      s"the vertical must have run as exactly one successful job, but the recorder saw " +
        s"${recorder.succeededJobCount} succeeded and ${recorder.failedJobCount} failed")
    assert(recorder.fetchFailures.isEmpty,
      s"no fetch failure may have occurred, or the output was reached by recomputation rather " +
        s"than by streaming: ${recorder.fetchFailures}")
    assert(recorder.maxStageSubmissions === 1,
      s"no stage may have been submitted twice, but the busiest was submitted " +
        s"${recorder.maxStageSubmissions} time(s)")
    assert(!streamingShuffleStoodDown(shuffled),
      "the shuffle must still be streaming, or the vertical fell back and measured sort")

    val producing = recorder.producingStageIds
    val consuming = recorder.consumingStageIds
    assert(producing.size === 1 && consuming.size === 1,
      s"the vertical must be exactly one producing stage and one consuming stage, but the write " +
        s"reporter moved in stages $producing and the read reporter in stages $consuming")
    val mapStageId = producing.head
    val reduceStageId = consuming.head
    assert(mapStageId != reduceStageId,
      s"the producing and consuming halves must be different stages, but both were $mapStageId")
    assert(recorder.bytesWrittenByStage(mapStageId) > 0L,
      "the streaming writer must have reported its bytes onto the task's shuffle write metrics, " +
        "or every existing Spark observability surface is blank for this shuffle")
    assert(recorder.bytesReadByStage(reduceStageId) > 0L,
      "and the streaming reader must have reported its bytes onto the task's shuffle read metrics")

    // 3.
    val mapCompletedAt = recorder.completionTimeOf(mapStageId).getOrElse(
      fail(s"the scheduler must have recorded a completion time for map stage $mapStageId"))
    val reduceSubmittedAt = recorder.submissionTimeOf(reduceStageId).getOrElse(
      fail(s"the scheduler must have recorded a submission time for reduce stage $reduceStageId"))
    assert(reduceSubmittedAt >= mapCompletedAt,
      s"the reduce stage was submitted at $reduceSubmittedAt and the map stage completed at " +
        s"$mapCompletedAt: a consuming stage submitted before its producing stage finished would " +
        "mean the scheduler had changed, which this feature is forbidden to arrange and this " +
        "assertion exists to notice")

    assert(recorder.memoryBytesSpilled > 0L,
      s"a scheduled vertical whose reduce stage starts after its map stage must have moved its " +
        s"retained output out of task memory, and that must appear on " +
        s"TaskMetrics.memoryBytesSpilled, which reads ${recorder.memoryBytesSpilled}")
    assert(recorder.diskBytesSpilled > 0L,
      s"and its committed volume must appear on TaskMetrics.diskBytesSpilled, which reads " +
        s"${recorder.diskBytesSpilled}")

    logInfo(log"Scheduled streaming vertical: map stage ${MDC(COUNT, mapStageId)} wrote " +
      log"${MDC(BYTE_SIZE, recorder.bytesWrittenByStage(mapStageId))} byte(s) and reduce stage " +
      log"${MDC(NUM_PARTITIONS, reduceStageId)} read " +
      log"${MDC(NUM_BYTES, recorder.bytesReadByStage(reduceStageId))} byte(s); " +
      log"${MDC(MEMORY_SIZE, recorder.diskBytesSpilled)} byte(s) were made durable so the reduce " +
      log"stage could read them -- whatever of that the producers had not already secured while " +
      log"framing is what their stops wrote; the reduce stage was submitted " +
      log"${MDC(TIME_UNITS, reduceSubmittedAt - mapCompletedAt)} ms after the map stage " +
      log"completed, which is the stage barrier this feature may not move")
  }

  test("scenario 2 in the live path: a producer lost under a reading consumer is invalidated") {
    val (manager, handle, writerContext, readerContext) =
      liveStreamingPair("streaming-shuffle-integration-live-producer-loss", OverlapPartitions)
    val invalidationsBefore = observedPartialReadInvalidations()

    val consumerConsumedOne = new CountDownLatch(1)
    val produced = new AtomicInteger(0)
    val consumed = new AtomicInteger(0)
    val consumerWasAttached = new AtomicBoolean(false)
    val writerHandle = new AtomicReference[StreamingShuffleWriter[Int, Int, Int]](null)
    val writeFailure = new AtomicReference[Throwable](null)
    val producerFailure = new AtomicReference[Throwable](null)
    val consumerFailure = new AtomicReference[Throwable](null)
    val producerStopped = new AtomicBoolean(false)

    val producer = new Thread(() => {
      try {
        TaskContext.setTaskContext(writerContext)
        val writer = manager
          .getWriter[Int, Int](handle, OverlapMapId, writerContext,
            writerContext.taskMetrics.shuffleWriteMetrics)
          .asInstanceOf[StreamingShuffleWriter[Int, Int, Int]]
        writerHandle.set(writer)
        val records = liveFaultRecords(produced, LiveFaultRecordCount, OverlapPartitions,
          LiveFaultBlocksBeforeWait, () => writer.blocksStreamed,
          () => {
            consumerWasAttached.set(
              consumerConsumedOne.await(OverlapWaitTimeoutMillis, TimeUnit.MILLISECONDS))
            throw new IllegalStateException("Injected live streaming shuffle producer loss")
          })
        try {
          writer.write(records)
        } catch {
          case failure: Throwable => writeFailure.set(failure)
        }
        writer.stop(success = false)
        producerStopped.set(true)
      } catch {
        case failure: Throwable => producerFailure.set(failure)
      } finally {
        TaskContext.unset()
      }
    }, "streaming-shuffle-live-loss-producer")

    val consumer = new Thread(() => {
      try {
        TaskContext.setTaskContext(readerContext)
        val reader = manager.getReader[Int, Int](handle, 0, Int.MaxValue, 0, 1, readerContext,
          readerContext.taskMetrics.createTempShuffleReadMetrics())
        val records = reader.read()
        while (records.hasNext) {
          records.next()
          consumed.incrementAndGet()
          consumerConsumedOne.countDown()
        }
      } catch {
        case failure: Throwable => consumerFailure.set(failure)
      } finally {
        consumerConsumedOne.countDown()
        TaskContext.unset()
      }
    }, "streaming-shuffle-live-loss-consumer")

    runBoundedThreadedScenario(
      threads = Seq(producer, consumer),
      taskContexts = Seq(writerContext, readerContext),
      description = "the live producer-loss case",
      joinTimeoutMillis = OverlapJoinTimeoutMillis,
      releaseWaits = () => consumerConsumedOne.countDown()) {
      producer.start()
      eventually(timeout(OverlapStartupTimeout), interval(OverlapPollInterval)) {
        assert(writerHandle.get() != null || producerFailure.get() != null,
          "the producer must have been given a writer, or have failed trying")
      }
      assert(producerFailure.get() == null,
        s"the producer must have been given a streaming writer, but failed with " +
          s"${producerFailure.get()}")
      consumer.start()
    }

    assert(producerFailure.get() == null,
      s"the producer half must have failed only where the fault was injected, but raised " +
        s"${producerFailure.get()}")
    assert(producerStopped.get(),
      "the lost producer must have been stopped unsuccessfully, because that withdrawal is the " +
        "production cleanup path this case exists to drive")

    assert(writeFailure.get() != null,
      "the injected producer loss must have escaped the write, but the write returned normally " +
        s"after ${produced.get()} record(s)")
    assert(writeFailure.get().toString.contains("Injected live streaming shuffle producer loss") ||
      Option(writeFailure.get().getCause).exists(
        _.toString.contains("Injected live streaming shuffle producer loss")),
      s"the write must have failed with the INJECTED loss rather than with something else, but " +
        s"failed with ${writeFailure.get()}")
    assert(consumerWasAttached.get(),
      s"a consumer must have consumed a record before the producer was lost, or there were no " +
        s"partial reads to invalidate; the producer waited $OverlapWaitTimeoutMillis ms after " +
        s"${produced.get()} record(s) and ${consumed.get()} were read")
    assert(consumed.get() > 0,
      s"and the count of records the consumer read before the loss must be positive, but was " +
        s"${consumed.get()}")

    assert(consumerFailure.get() != null,
      s"a consumer whose producer was lost mid-stream must fail rather than complete with the " +
        s"${consumed.get()} record(s) it happened to have received")
    val fetchFailed = fetchFailureIn(consumerFailure.get())
    assert(fetchFailed.isDefined,
      s"the consumer must have raised FetchFailedException, the sanctioned recomputation signal, " +
        s"but raised ${consumerFailure.get()}")
    assert(fetchFailed.get.shuffleId === handle.shuffleId,
      s"the fetch failure must name the shuffle whose producer was lost, ${handle.shuffleId}, " +
        s"but named ${fetchFailed.get.shuffleId}")
    assert(fetchFailed.get.reduceId === 0,
      s"and must name the reduce partition that was reading, 0, but named " +
        s"${fetchFailed.get.reduceId}")
    assert(observedPartialReadInvalidations() > invalidationsBefore,
      s"the production reader must have counted its partial-read invalidation on " +
        s"shuffle.streaming.partialReadInvalidations, which read $invalidationsBefore before the " +
        s"loss and ${observedPartialReadInvalidations()} after")
    logInfo(log"Live producer loss: ${MDC(NUM_RECORDS_READ, consumed.get())} record(s) had been " +
      log"read from a producer that offered ${MDC(RECORDS, produced.get())} before it was lost, " +
      log"and the consumer answered with ${MDC(REASON, fetchFailed.get.message)}")
  }

  test("scenario 3 in the live path: a consumer that falls behind paces the producer onto disk") {
    val (manager, handle, writerContext, readerContext) = liveStreamingPair(
      "streaming-shuffle-integration-live-slow-consumer", OverlapPartitions,
      conf => conf
        .set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, MinBufferSizePercent)
        .set(SHUFFLE_STREAMING_SPILL_THRESHOLD, MinSpillThresholdPercent))

    val consumerConsumedOne = new CountDownLatch(1)
    val produced = new AtomicInteger(0)
    val consumed = new AtomicInteger(0)
    val consumerWasAttached = new AtomicBoolean(false)
    val writerHandle = new AtomicReference[StreamingShuffleWriter[Int, Int, Int]](null)
    val producerFailure = new AtomicReference[Throwable](null)
    val consumerFailure = new AtomicReference[Throwable](null)
    val producerFinished = new AtomicBoolean(false)
    val consumedDuringProduction = new AtomicInteger(0)
    val spillWitnessed = new AtomicBoolean(false)

    // "Fell behind" as a condition on the SUBSYSTEM rather than on a clock: the producer's own
    // eviction count and the task's own spill accumulator.
    def producerSpilled(): Boolean = {
      val writer = writerHandle.get()
      writer != null &&
        (writer.spillsObserved > 0L || writerContext.taskMetrics.diskBytesSpilled > 0L)
    }

    val producer = new Thread(() => {
      try {
        TaskContext.setTaskContext(writerContext)
        val writer = manager
          .getWriter[Int, Int](handle, OverlapMapId, writerContext,
            writerContext.taskMetrics.shuffleWriteMetrics)
          .asInstanceOf[StreamingShuffleWriter[Int, Int, Int]]
        writerHandle.set(writer)
        writer.write(liveFaultRecords(produced, SlowConsumerRecordCount, OverlapPartitions,
          LiveFaultBlocksBeforeWait, () => writer.blocksStreamed,
          () => consumerWasAttached.set(
            consumerConsumedOne.await(OverlapWaitTimeoutMillis, TimeUnit.MILLISECONDS))))
        producerFinished.set(true)
        writer.stop(success = true)
      } catch {
        case failure: Throwable =>
          producerFinished.set(true)
          producerFailure.set(failure)
      } finally {
        TaskContext.unset()
      }
    }, "streaming-shuffle-live-slow-producer")

    val consumer = new Thread(() => {
      try {
        TaskContext.setTaskContext(readerContext)
        val reader = manager.getReader[Int, Int](handle, 0, Int.MaxValue, 0, 1, readerContext,
          readerContext.taskMetrics.createTempShuffleReadMetrics())
        val records = reader.read()
        var withheld = false
        while (records.hasNext) {
          records.next()
          consumed.incrementAndGet()
          if (!producerFinished.get()) {
            consumedDuringProduction.incrementAndGet()
          }
          consumerConsumedOne.countDown()
          if (!withheld) {
            withheld = true
            eventually(timeout(LiveFaultConditionTimeout), interval(OverlapPollInterval)) {
              assert(producerSpilled() || producerFinished.get(),
                s"the producer must have spilled or finished while the consumer withheld its " +
                  s"acknowledgements, but has streamed ${Option(writerHandle.get())
                    .map(_.blocksStreamed).getOrElse(0L)} block(s) with " +
                  s"${writerContext.taskMetrics.diskBytesSpilled} byte(s) on disk")
            }
            spillWitnessed.set(producerSpilled())
          }
        }
      } catch {
        case failure: Throwable => consumerFailure.set(failure)
      } finally {
        consumerConsumedOne.countDown()
        TaskContext.unset()
      }
    }, "streaming-shuffle-live-slow-consumer")

    runBoundedThreadedScenario(
      threads = Seq(producer, consumer),
      taskContexts = Seq(writerContext, readerContext),
      description = "the live slow-consumer case",
      joinTimeoutMillis = OverlapJoinTimeoutMillis,
      releaseWaits = () => consumerConsumedOne.countDown()) {
      producer.start()
      eventually(timeout(OverlapStartupTimeout), interval(OverlapPollInterval)) {
        assert(writerHandle.get() != null || producerFailure.get() != null,
          "the producer must have been given a writer, or have failed trying")
      }
      assert(producerFailure.get() == null,
        s"the producer must have been given a streaming writer, but failed with " +
          s"${producerFailure.get()}")
      consumer.start()
    }

    assert(producerFailure.get() == null,
      s"a producer whose consumer fell behind must stream to completion rather than fail, but " +
        s"raised ${producerFailure.get()}")
    assert(consumerFailure.get() == null,
      s"and the consumer that fell behind must still have read its partition, but raised " +
        s"${consumerFailure.get()}")
    assert(consumerWasAttached.get() && consumedDuringProduction.get() > 0,
      s"the consumer must have been reading from a live producer for its slowness to have paced " +
        s"anything, but read ${consumedDuringProduction.get()} record(s) before the producer " +
        "finished")

    // The specified answer happened, on the live data path, driven by a consumer that stopped
    // acknowledging rather than by a fixture calling the spill manager.
    val streamedBytes = writerContext.taskMetrics.shuffleWriteMetrics.bytesWritten
    val spilledBytes = writerContext.taskMetrics.diskBytesSpilled
    assert(streamedBytes > 0L,
      "the producer must have streamed bytes for the comparison below to mean anything")
    assert(spilledBytes > 0L,
      s"a consumer held behind a producer under the minimum $MinBufferSizePercent percent " +
        s"allowance must have driven the retained window onto local disk, and the volume must be " +
        s"reported on Spark's OWN TaskMetrics.diskBytesSpilled, which reads $spilledBytes")
    assert(2L * spilledBytes > streamedBytes,
      s"a producer whose consumer stopped acknowledging reclaims nothing, so the MAJORITY of " +
        s"what it streamed must have been made durable, yet only $spilledBytes of $streamedBytes " +
        s"streamed byte(s) reached disk, which is the profile of a consumer that kept pace and " +
        s"not of one that fell behind")

    assert(produced.get() === SlowConsumerRecordCount,
      s"the producer must have offered every one of the $SlowConsumerRecordCount records, but " +
        s"offered ${produced.get()}")
    assert(consumed.get() === SlowConsumerRecordCount,
      s"and the consumer that fell behind must still have read every one of them, but read " +
        s"${consumed.get()}")
    logInfo(log"Live half-rate consumer: " +
      log"${MDC(NUM_RECORDS_READ, consumedDuringProduction.get())} of " +
      log"${MDC(RECORDS, consumed.get())} record(s) were read while the producer was still " +
      log"producing; withholding acknowledgements moved ${MDC(MEMORY_SIZE, spilledBytes)} of " +
      log"${MDC(BYTE_SIZE, streamedBytes)} streamed byte(s) to disk, and the eviction was " +
      log"observed while the producer was still running: ${MDC(TOTAL, spillWitnessed.get())}")
  }

  test("scenario 4 in the live path: a severed link is silence the reader turns into a failure") {
    val (manager, handle, writerContext, readerContext) =
      liveStreamingPair("streaming-shuffle-integration-live-partition", OverlapPartitions)
    val invalidationsBefore = observedPartialReadInvalidations()

    val consumerConsumedOne = new CountDownLatch(1)
    val produced = new AtomicInteger(0)
    val consumed = new AtomicInteger(0)
    val consumerWasAttached = new AtomicBoolean(false)
    val linkSevered = new AtomicBoolean(false)
    val writerHandle = new AtomicReference[StreamingShuffleWriter[Int, Int, Int]](null)
    val producerFailure = new AtomicReference[Throwable](null)
    val consumerFailure = new AtomicReference[Throwable](null)

    val producer = new Thread(() => {
      try {
        TaskContext.setTaskContext(writerContext)
        val writer = manager
          .getWriter[Int, Int](handle, OverlapMapId, writerContext,
            writerContext.taskMetrics.shuffleWriteMetrics)
          .asInstanceOf[StreamingShuffleWriter[Int, Int, Int]]
        writerHandle.set(writer)
        val records = liveFaultRecords(produced, LiveFaultRecordCount, OverlapPartitions,
          LiveFaultBlocksBeforeWait, () => writer.blocksStreamed,
          () => {
            consumerWasAttached.set(
              consumerConsumedOne.await(OverlapWaitTimeoutMillis, TimeUnit.MILLISECONDS))
            // The link goes, and NOTHING else does.
            val server = manager.boundStreamingServer
            assert(server.isDefined,
              "a producer that has streamed blocks must have bound a serving socket to stream them")
            server.foreach(_.close())
            linkSevered.set(true)
          })
        try {
          writer.write(records)
        } catch {
          case failure: Throwable => producerFailure.set(failure)
        }
        writer.stop(success = producerFailure.get() == null)
      } catch {
        case failure: Throwable => producerFailure.set(failure)
      } finally {
        TaskContext.unset()
      }
    }, "streaming-shuffle-live-partition-producer")

    val consumer = new Thread(() => {
      try {
        TaskContext.setTaskContext(readerContext)
        val reader = manager.getReader[Int, Int](handle, 0, Int.MaxValue, 0, 1, readerContext,
          readerContext.taskMetrics.createTempShuffleReadMetrics())
        val records = reader.read()
        while (records.hasNext) {
          records.next()
          consumed.incrementAndGet()
          consumerConsumedOne.countDown()
        }
      } catch {
        case failure: Throwable => consumerFailure.set(failure)
      } finally {
        consumerConsumedOne.countDown()
        TaskContext.unset()
      }
    }, "streaming-shuffle-live-partition-consumer")

    runBoundedThreadedScenario(
      threads = Seq(producer, consumer),
      taskContexts = Seq(writerContext, readerContext),
      description = "the live severed-link case",
      joinTimeoutMillis = OverlapJoinTimeoutMillis,
      releaseWaits = () => consumerConsumedOne.countDown()) {
      producer.start()
      eventually(timeout(OverlapStartupTimeout), interval(OverlapPollInterval)) {
        assert(writerHandle.get() != null || producerFailure.get() != null,
          "the producer must have been given a writer, or have failed trying")
      }
      assert(producerFailure.get() == null,
        s"the producer must have been given a streaming writer, but failed with " +
          s"${producerFailure.get()}")
      consumer.start()
    }

    assert(linkSevered.get(),
      "the case must have closed the producer's serving socket, or it partitioned nothing")
    assert(consumerWasAttached.get() && consumed.get() > 0,
      s"a consumer must have been reading over the link before it was severed, but read " +
        s"${consumed.get()} record(s)")

    assert(consumerFailure.get() != null,
      s"a consumer whose link died mid-stream must fail rather than complete with the " +
        s"${consumed.get()} record(s) it had already received")
    val fetchFailed = fetchFailureIn(consumerFailure.get())
    assert(fetchFailed.isDefined,
      s"the consumer must have raised FetchFailedException so the unmodified scheduler can " +
        s"recompute the upstream stage, but raised ${consumerFailure.get()}")
    assert(fetchFailed.get.shuffleId === handle.shuffleId,
      s"the fetch failure must name the partitioned shuffle, ${handle.shuffleId}, but named " +
        s"${fetchFailed.get.shuffleId}")
    assert(observedPartialReadInvalidations() > invalidationsBefore,
      s"and the invalidation must have been counted on " +
        s"shuffle.streaming.partialReadInvalidations, which read $invalidationsBefore before the " +
        s"link died and ${observedPartialReadInvalidations()} after")
    logInfo(log"Live network partition: the link was severed after " +
      log"${MDC(NUM_RECORDS_READ, consumed.get())} record(s) had been read of " +
      log"${MDC(RECORDS, produced.get())} offered, and the consumer answered with " +
      log"${MDC(REASON, fetchFailed.get.message)}")
  }

  test("a producer failure mid shuffle is recovered and the output is still the baseline") {
    val baseline = sortBaselineKeyedDigest(PartitionCount, FaultInjectionDatasetBytes)
    assert(baseline.nonEmpty, "the sort-based baseline must produce output to compare against")

    sc = new SparkContext(resilientStreamingConf("streaming-shuffle-integration-producer-failure"))
    TestUtils.waitUntilExecutorsUp(sc, ExecutorCount, ExecutorStartupTimeoutMillis)
    assertStreamingManagerInService()

    val recorder = new StreamingShuffleJobRecorder
    sc.addSparkListener(recorder)
    val fault = new WithdrawRetainedOutputFault
    val probe = newReduceSideFaultProbe(sc, fault, "producer-loss")
    val shuffled = lazilyConsumedDataset(sc, PartitionCount, FaultInjectionDatasetBytes)
    assert(isStreamingKeyedShuffle(shuffled),
      "the shuffle must have been registered on the streaming path, or the fault below would be " +
        "injected into a sort-based read and this scenario would prove nothing about streaming")
    val shuffleId = shuffled.dependencies.head
      .asInstanceOf[ShuffleDependency[Int, String, _]].shuffleId
    assert(shuffleId >= 0, "the workload must have registered a shuffle for the failure to be in")
    fault.shuffleId = shuffleId

    val read = faultInjectingRead(
      shuffled, RecordsBeforeProducerLoss, probe)
    val observed = try {
      keyedDigestOf(read)
    } finally {
      sc.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
      sc.removeSparkListener(recorder)
    }

    assertNoDataLoss(observed, baseline,
      "a streaming shuffle whose live producer connection failed and was recomputed")
    assert(observed.size === baseline.size,
      s"recovery must reproduce every group exactly once, but produced ${observed.size} against " +
        s"the baseline's ${baseline.size}")

    assert(probe.faultOffers.value >= 1L,
      "at least one consumer must have reached the injection point, which means receiving " +
        s"$RecordsBeforeProducerLoss record(s) on its first attempt, but none did: " +
        probe.describe)
    assert(probe.managerPresent.value >= 1L,
      "the consumers that reached the injection point must have found the streaming manager in " +
        "service on their executor, or the fault was offered to a sort-based read: " +
        probe.describe)
    assert(probe.recordsConsumedBeforeFault.value >= RecordsBeforeProducerLoss.toLong,
      s"a faulting consumer must have received at least $RecordsBeforeProducerLoss record(s) " +
        "before acting, so that receipt is partial rather than absent, but: " + probe.describe)
    assert(probe.egressBytesObserved.value > 0L,
      "the producing side must already have put bytes on the wire when the fault landed, or the " +
        "loss would not have interrupted a stream at all, but: " + probe.describe)

    val invalidations = executorMetricObservations(sc, 2 * ExecutorCount * PartitionCount)
      .map(_.partialReadInvalidations)
    assert(invalidations.nonEmpty, "the metric probe must have run on at least one executor")
    assert(invalidations.sum > 0L,
      "losing a producer mid-drain must have invalidated the partial reads sourced from it, so " +
        "shuffle.streaming.partialReadInvalidations must be positive somewhere in the " +
        s"application, but the executors reported ${invalidations.mkString(", ")}")

    assert(recorder.fetchFailures.nonEmpty,
      "the lost producer must have been reported to the scheduler as a fetch failure, since that " +
        "is the whole of this subsystem's interface to stage recomputation, but the run reported " +
        s"none across ${recorder.countedFailures.size} counted failure(s): " +
        recorder.countedFailures.take(3).map(_.toErrorString).mkString("[", ", ", "]"))
    recorder.fetchFailures.foreach { fetchFailed =>
      assert(fetchFailed.shuffleId === shuffleId,
        s"a fetch failure must name the shuffle whose producer was lost, $shuffleId, but named " +
          s"${fetchFailed.shuffleId}")
      assert(fetchFailed.mapIndex >= 0 || fetchFailed.mapIndex === -1,
        s"a fetch failure must carry a map index or the documented unknown marker, but carried " +
          s"${fetchFailed.mapIndex}")
    }
    assert(!recorder.countedFailures.exists(
      _.toErrorString.contains("Injected streaming shuffle producer crash")),
      "the live-connection scenario must not be satisfied by an ordinary map exception")

    val replacedAttempts = recorder.taskEndCount - 2L * PartitionCount.toLong
    val recoveredByRetry = replacedAttempts >= 1L
    val recoveredByInvalidation = recorder.fetchFailures.nonEmpty
    assert(recoveredByRetry || recoveredByInvalidation,
      s"recovery must be evidenced by one of the two sanctioned routes, but the run ended " +
        s"${recorder.taskEndCount} task(s) against the ${2 * PartitionCount} a clean run needs " +
        s"and reported no fetch failure at all")

    assert(recorder.maxStageSubmissions <= 1 + PartitionCount,
      s"a lost producer must be recovered within one resubmission per map partition, yet the " +
        s"busiest stage of a $FailurePartitionCount partition shuffle was submitted " +
        s"${recorder.maxStageSubmissions} time(s), with ${recorder.fetchFailures.size} fetch " +
        "failure(s) reported")
    assert(recorder.taskEndCount > FailurePartitionCount.toLong,
      "the run must have ended more tasks than the map stage has partitions, because the failed " +
        s"attempt is one of them, but ended only ${recorder.taskEndCount}")

    val observations = probedExecutorMetrics(sc, 2 * ExecutorCount * PartitionCount)
    val clusterInvalidations = observations.map(_.partialReadInvalidations).sum
    if (recoveredByInvalidation) {
      assert(clusterInvalidations >= 1L,
        s"${recorder.fetchFailures.size} fetch failure(s) were reported, so at least one " +
          s"partial-read invalidation must have been counted across the cluster, but the " +
          s"executors reported $clusterInvalidations")
    } else {
      assert(clusterInvalidations === 0L,
        s"no fetch failure was reported, so no partial read can have been invalidated, yet the " +
          s"executors counted $clusterInvalidations")
    }
    assert(observations.exists(_.partialReadInvalidations > 0L),
      "the executor that lost the live producer connection must increment partialReadInvalidations")
    assertNoParallelSpillCounters()
  }

  test("a consumer at 50% of the producer rate triggers spill onto the existing accumulators") {
    // The scenario has two halves, and they answer two different questions.
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
    val pacing = ConsumerPacingProbe(
      failureCountingAccumulator(sc, "streamingShuffleRecordsConsumed"),
      failureCountingAccumulator(sc, "streamingShufflePermitsEarned"),
      failureCountingAccumulator(sc, "streamingShufflePermitsGranted"),
      failureCountingAccumulator(sc, "streamingShuffleBlocksObserved"))
    val shuffled = lazilyConsumedDataset(sc, PartitionCount, SlowConsumerDatasetBytes)
    assert(isStreamingKeyedShuffle(shuffled),
      "the shuffle must have been registered on the streaming path, or the pacing below would be " +
        "restraining a sort-based fetch and this scenario would prove nothing about streaming")
    val observed = try {
      keyedDigestOf(pacedRead(shuffled, pacing))
    } finally {
      sc.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
      sc.removeSparkListener(recorder)
    }
    assertNoDataLoss(observed, baseline,
      "a streaming shuffle whose consumer ran at half the producer's rate")
    val pacedDatasetRecords =
      PartitionCount.toLong * recordsPerPartitionFor(PartitionCount, SlowConsumerDatasetBytes)
    assert(pacing.recordsConsumed.value >= pacedDatasetRecords,
      s"every record of the dataset must have been pulled off the reader's own iterator by the " +
        s"paced loop, which is $pacedDatasetRecords records, but the executors paced " +
        s"${pacing.recordsConsumed.value}")
    assert(recorder.taskEndCount >= 2L * PartitionCount.toLong,
      "both stages of the shuffle must have ended every one of their tasks, which is " +
        s"${2 * PartitionCount} at least, but only ${recorder.taskEndCount} ended")

    assert(pacing.recordsConsumed.value > 0L,
      "the paced consumers must have taken records through the permit ledger, but reported " +
        s"${pacing.recordsConsumed.value}, which means the reduce side never ran the pacing at all")
    assert(pacing.blocksObserved.value > 0L,
      "the producing side must have been putting blocks on the wire while the pacing was " +
        "happening, or half its rate would be half of nothing, but the highest reading was " +
        s"${pacing.blocksObserved.value}")
    assert(pacing.permitsEarned.value > 0L,
      s"permits must have been EARNED from an observed advance of $BlocksPerConsumerPermit " +
        "block(s), which is what makes the ratio one against real production, but " +
        s"${pacing.permitsEarned.value} were earned against " +
        s"${pacing.permitsGranted.value} granted by the bounded fall-through")
    assert(recorder.diskBytesSpilled > 0L,
      "bytes must have reached local disk and been reported on TaskMetrics.diskBytesSpilled, but " +
        s"the run reported ${recorder.diskBytesSpilled}; a streaming shuffle whose consumer runs " +
        "at half rate under a one percent buffer allowance cannot have held everything in memory")
    assert(recorder.peakExecutionMemory > 0L,
      "the high-water mark of the streaming reservation must be reported on " +
        s"TaskMetrics.peakExecutionMemory, but the run reported ${recorder.peakExecutionMemory}")
    assert(recorder.memoryBytesSpilled >= 0L,
      s"memory spill volume can never run backwards, but read ${recorder.memoryBytesSpilled}")
    probedExecutorMetrics(sc, 2 * ExecutorCount * PartitionCount)
    logInfo(log"Slow-consumer streaming shuffle reported " +
      log"${MDC(NUM_BYTES, recorder.memoryBytesSpilled)} byte(s) of memory spill and " +
      log"${MDC(MEMORY_SIZE, recorder.diskBytesSpilled)} byte(s) of disk spill on the existing " +
      log"task metric accumulators, with a peak execution memory of " +
      log"${MDC(MAX_SIZE, recorder.peakExecutionMemory)} bytes")

    val blockCharge = SpillPayloadBytes.toLong + MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES
    val budgetBytes = SpillBudgetBlocks * blockCharge
    val executorMemoryBytes = budgetBytes * PercentScale / DefaultBufferSizePercent.toLong
    val quota = new MemorySpillManager.ExecutorBufferQuota(
      DefaultBufferSizePercent, DefaultSpillThresholdPercent, () => executorMemoryBytes)
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

  test("a network partition fires the 5 s timeout and the job still completes on sort") {
    // A partition is not a lost producer, and the difference is what this scenario exists to
    // establish.
    val baseline = sortBaselineKeyedDigest(PartitionCount, ModestDatasetBytes)
    assert(baseline.nonEmpty, "the sort-based baseline must produce output to compare against")
    val saturationBaseline = sortBaselineKeyedDigest(PartitionCount, FaultInjectionDatasetBytes)
    assert(saturationBaseline.nonEmpty,
      "the sort-based baseline of the saturation half must produce output to compare against")
    assert(ProducerConnectionTimeoutMillis === 5000L,
      "the producer connection timeout this scenario relies on is five seconds, but the suite " +
        s"reads $ProducerConnectionTimeoutMillis ms")

    sc = new SparkContext(resilientStreamingConf("streaming-shuffle-integration-partition"))
    TestUtils.waitUntilExecutorsUp(sc, ExecutorCount, ExecutorStartupTimeoutMillis)
    assertStreamingManagerInService()

    val severHolder = new LiveFaultHolder(StreamingShuffleFaultScenario.NetworkPartition)
    val severedProducers = sc.longAccumulator("streamingShuffleSeveredProducers")
    val severRecorder = new StreamingShuffleJobRecorder
    sc.addSparkListener(severRecorder)
    val (severedOutput, severedShuffleId) = try {
      severedTransportDigest(
        sc, PartitionCount, ModestDatasetBytes, severHolder, severedProducers)
    } finally {
      sc.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
      sc.removeSparkListener(severRecorder)
    }

    assertNoDataLoss(severedOutput, baseline,
      "a job whose streaming transport was partitioned and then recomputed")
    assert(severedOutput.size === baseline.size,
      s"recovery must reproduce every group exactly once, but produced ${severedOutput.size} " +
        s"against the baseline's ${baseline.size}")
    assert(severedProducers.value >= 1L || severRecorder.fetchFailures.nonEmpty,
      "the partition must have reached the run whose output was compared, yet it withdrew " +
        s"${severedProducers.value} generation(s) and produced " +
        s"${severRecorder.fetchFailures.size} fetch failure(s)")
    severRecorder.fetchFailures.foreach { fetchFailed =>
      assert(fetchFailed.shuffleId === severedShuffleId,
        s"a fetch failure must name the shuffle whose transport was severed, $severedShuffleId, " +
          s"but named ${fetchFailed.shuffleId}")
    }
    assert(severRecorder.maxStageSubmissions <= 1 + PartitionCount,
      s"a partition must be recovered within one resubmission per map partition, yet the busiest " +
        s"stage of a $PartitionCount partition shuffle was submitted " +
        s"${severRecorder.maxStageSubmissions} time(s), with " +
        s"${severRecorder.fetchFailures.size} fetch failure(s) reported")
    logInfo(log"A partitioned streaming transport withdrew " +
      log"${MDC(COUNT, severedProducers.value)} producer generation(s) and cost " +
      log"${MDC(NUM_FAILURES, severRecorder.fetchFailures.size)} fetch failure(s) across " +
      log"${MDC(NUM_RETRIES, severRecorder.maxStageSubmissions)} submission(s) of the busiest " +
      log"stage")

    resetSparkContext()
    LiveFaultHolder.reset()
    sc = new SparkContext(resilientStreamingConf("streaming-shuffle-integration-saturation"))
    TestUtils.waitUntilExecutorsUp(sc, ExecutorCount, ExecutorStartupTimeoutMillis)
    assertStreamingManagerInService()
    val manager = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]
    assert(manager.streamingFallbackPolicy.streamingActive,
      "streaming must be in service before the link is broken, or the case would be proving " +
        "nothing about a streaming shuffle's response to one")

    val recorder = new StreamingShuffleJobRecorder
    sc.addSparkListener(recorder)
    val fault = new PartitionEveryLinkFault(
      PartitionFaultReason, PartitionHoldMillis, PartitionSweepMillis, PartitionCloseAttempts)
    val probe = newReduceSideFaultProbe(sc, fault, "network-partition")
    val shuffled = lazilyConsumedDataset(sc, PartitionCount, FaultInjectionDatasetBytes)
    assert(isStreamingKeyedShuffle(shuffled),
      "the shuffle must have been registered on the streaming path, or breaking the link would " +
        "break a sort-based fetch and this scenario would prove nothing about streaming")
    val shuffleId = shuffled.dependencies.head
      .asInstanceOf[ShuffleDependency[Int, String, _]].shuffleId
    assert(shuffleId >= 0, "the workload must have registered a shuffle for the link to serve")

    val read = faultInjectingRead(
      shuffled, RecordsBeforeProducerLoss, probe)
    val observed = try {
      keyedDigestOf(read)
    } finally {
      sc.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
      sc.removeSparkListener(recorder)
    }

    assertNoDataLoss(observed, saturationBaseline,
      "a streaming shuffle whose links were all broken mid-drain")

    assert(probe.faultOffers.value >= 1L,
      "at least one consumer must have reached the injection point, but none did: " +
        probe.describe)
    assert(probe.faultsApplied.value <= ExecutorCount.toLong,
      s"at most $ExecutorCount executor(s) may have broken their links -- the injection is " +
        s"latched one per executor JVM -- but ${probe.faultsApplied.value} did: " +
        probe.describe)
    assert(probe.faultsApplied.value >= 1L || recorder.fetchFailures.nonEmpty,
      "an executor must have broken its links, yet none reported doing so and the run recorded " +
        s"no fetch failure either: ${probe.describe}")
    assert(probe.recordsConsumedBeforeFault.value >= RecordsBeforeProducerLoss.toLong,
      s"a consumer must have received at least $RecordsBeforeProducerLoss record(s) across the " +
        "link before breaking it, but: " + probe.describe)
    assert(probe.egressBytesObserved.value > 0L,
      "the link must have been carrying streaming shuffle output when it was broken, but: " +
        probe.describe)
    assert(probe.faultEffect.value > 0L || recorder.fetchFailures.nonEmpty,
      "at least one live consumer channel must have been closed, or the transport was idle and " +
        "nothing was partitioned, but: " + probe.describe)
    assert(PartitionHoldMillis > ProducerConnectionTimeoutMillis,
      s"the partition must be held for longer than the $ProducerConnectionTimeoutMillis ms " +
        "producer-liveness bound, or a reconnecting consumer would cross it before the bound " +
        s"could expire, but it is held for $PartitionHoldMillis ms")

    val servingListener = manager.boundStreamingListener
    servingListener.foreach { listener =>
      assert(listener.faultedChannelCloseCount >= 0L,
        "the channel-global close counter must be readable after the run, whatever it reads on " +
          "this particular executor")
      assert(listener.malformedFrameCount === 0L,
        "breaking a link must not have produced a frame the router could not handle, but the " +
          s"driver-side listener saw ${listener.malformedFrameCount}")
    }

    val invalidations = executorMetricObservations(sc, 2 * ExecutorCount * PartitionCount)
      .map(_.partialReadInvalidations)
    assert(invalidations.nonEmpty, "the metric probe must have run on at least one executor")
    assert(invalidations.sum > 0L,
      "a consumer whose link went silent past the five-second bound must have invalidated the " +
        "partial reads it had taken across it, so " +
        "shuffle.streaming.partialReadInvalidations must be positive somewhere in the " +
        s"application, but the executors reported ${invalidations.mkString(", ")}")

    recorder.fetchFailures.foreach { fetchFailed =>
      assert(fetchFailed.shuffleId === shuffleId,
        s"a fetch failure must name the shuffle whose link was broken, $shuffleId, but named " +
          s"${fetchFailed.shuffleId}")
    }
    assert(recorder.maxStageSubmissions <= 1 + PartitionCount,
      s"a broken link must be recovered within one resubmission per map partition, yet the " +
        s"busiest stage of a $PartitionCount partition shuffle was submitted " +
        s"${recorder.maxStageSubmissions} time(s), with ${recorder.fetchFailures.size} fetch " +
        "failure(s) reported")
    assert(recorder.failedJobCount === 0,
      "the job must have completed despite the partition, but " +
        s"${recorder.failedJobCount} failed outright")

    assert(sc.getConf.get(SHUFFLE_MANAGER) === StreamingShuffleManager.SHORT_NAME,
      "degradation is delegation inside the streaming manager, not a different manager being " +
        "selected, so the configured manager must be unchanged")
    assertNoParallelSpillCounters()
  }

  test("five concurrent shuffles all complete and arbitrate on partition count and volume") {
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

    val conf = withLocalMaster(
      streamingConfWithOverrides(
        bufferSizePercent = MinBufferSizePercent,
        spillThreshold = MinSpillThresholdPercent,
        maxBandwidthMBps = Some(16)),
      "streaming-shuffle-integration-concurrent", ClusterMaster)
      .set(TASK_MAX_FAILURES, MaxTaskFailures)
      .set(STAGE_MAX_CONSECUTIVE_ATTEMPTS, 2 * PartitionCount + 4)
    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, ExecutorCount, ExecutorStartupTimeoutMillis)
    assertStreamingManagerInService()

    // <b>The concurrent jobs OBSERVE the arbitration, rather than running beside a fixture that
    // observes nothing about them.</b> Arbitration is executor-scoped state held by the one
    // protocol instance an executor's manager owns, so the only way an asserted job can bear
    // witness to it is from inside the job: each of the five workloads reads its own executor's
    // live protocol from within its map stage and reports what it saw through accumulators.
    val observations = sc.longAccumulator("streamingShuffleArbitrationObservations")
    val arbitrations = sc.longAccumulator("streamingShuffleArbitrationsSeen")
    val ownDemands = sc.longAccumulator("streamingShuffleOwnDemandsSeen")
    val exemptions = sc.longAccumulator("streamingShuffleExemptionsSeen")
    val disagreements = sc.longAccumulator("streamingShuffleArbitrationDisagreements")

    // The barrier is what makes the concurrency real: without it the first job would usually finish
    // before the last started, and a case meant to exercise contention would exercise nothing.
    val liveManager = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]
    val liveCoordinator = liveManager.boundCoordinator
    assert(liveCoordinator.isDefined,
      "the driver must host the coordinator, since that is where the active-shuffle registry " +
        "lives and what supplies numConcurrentShuffles to every executor's rate limiter")
    val liveRegistrySamples = new StreamingShuffleRegistrySamples
    val observedExecutorDivisors = new CollectionAccumulator[java.lang.Integer]
    observedExecutorDivisors.register(
      sc, name = Some("streamingShuffleExecutorDivisors"), countFailedValues = true)

    val results = runConcurrently(
      ConcurrentShuffleCount,
      "streaming-shuffle-integration-concurrent",
      ConcurrentJobTimeoutMillis) { index =>
      val output = groupedOutputAsSet(
        sc, ConcurrentPartitionCounts(index), observedExecutorDivisors)
      liveRegistrySamples.sample(liveCoordinator.get)
      output
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

    assert(liveRegistrySamples.peakConcurrentShuffles === ConcurrentShuffleCount,
      s"all $ConcurrentShuffleCount shuffles must have been registered in the production " +
        s"registry at once, but its peak was ${liveRegistrySamples.peakConcurrentShuffles}; that " +
        "count is what every executor's token bucket divides its share by, so a lower peak means " +
        "the shuffles never contended for the cap")
    assert(liveRegistrySamples.widestObservedSet.size === ConcurrentShuffleCount,
      s"one sample must have seen all $ConcurrentShuffleCount shuffles together, but the widest " +
        s"held ${liveRegistrySamples.widestObservedSet.toSeq.sorted.mkString(", ")}")
    assert(liveRegistrySamples.observedWidths === ConcurrentPartitionCounts.toSet,
      s"the registry must have held the five distinct widths this scenario submitted, " +
        s"${ConcurrentPartitionCounts.mkString(", ")}, but held " +
        s"${liveRegistrySamples.observedWidths.toSeq.sorted.mkString(", ")}")

    val reportedDivisors = observedExecutorDivisors.value.asScala.map(_.intValue()).toSeq
    assert(reportedDivisors.nonEmpty,
      "at least one reduce task must have reported its executor's egress divisor, but none did, " +
        "which means no reduce task ran on the streaming path")
    assert(reportedDivisors.max > 1,
      "an executor serving concurrent shuffles must have divided its bandwidth share more than " +
        s"one way, but the largest divisor any reduce task reported was ${reportedDivisors.max} " +
        s"across ${reportedDivisors.size} report(s)")
    assert(reportedDivisors.max <= ConcurrentShuffleCount,
      s"no executor may divide its share more than $ConcurrentShuffleCount ways, since that is " +
        s"how many shuffles exist, but one reported ${reportedDivisors.max}")
    assert(reportedDivisors.forall(_ >= 1),
      "a divisor is clamped to at least one so the division is always safe, but a reduce task " +
        s"reported ${reportedDivisors.filter(_ < 1).mkString(", ")}")

    // Arbitration, driven deterministically over the same five widths.
    val clock: ManualClock = newManualClock()
    val arbitrationConf =
      streamingConfWithOverrides(maxBandwidthMBps = Some(ArbitrationBandwidthMBps))
    val coordinator = new StreamingShuffleCoordinator(sc.env.rpcEnv, arbitrationConf, clock)
    val budget = TokenBucketRateLimiter.executorBudget(
      arbitrationConf, clock, () => Some(coordinator.numConcurrentShuffles))
    val protocol = new BackpressureProtocol(arbitrationConf, coordinator, budget, clock)
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

/** Executor-local one-shot gate for the live network-partition injection. */
private object StreamingShuffleIntegrationFaultGate {

  private val partitioned = new AtomicBoolean(false)

  /** Pauses the current channels once in this executor JVM. */
  def partitionChannelsOnce(manager: StreamingShuffleManager): Int = {
    if (partitioned.compareAndSet(false, true)) {
      val paused = manager.pauseInboundStreamingChannels()
      if (paused <= 0) {
        partitioned.set(false)
      }
      paused
    } else {
      0
    }
  }
}

/** What the production active-shuffle registry held while the concurrent jobs were running. */
private class StreamingShuffleRegistrySamples {

  private var peakCount: Int = 0

  private var widest: Set[Int] = Set.empty

  private var widths: Set[Int] = Set.empty

  def sample(coordinator: StreamingShuffleCoordinator): Unit = synchronized {
    val ids = coordinator.activeShuffleIds.toSet
    if (ids.size > widest.size) {
      widest = ids
    }
    peakCount = math.max(peakCount, ids.size)
    widths ++= ids.flatMap(id => coordinator.registeredNumPartitions(id))
  }

  def peakConcurrentShuffles: Int = synchronized(peakCount)

  def widestObservedSet: Set[Int] = synchronized(widest)

  def observedWidths: Set[Int] = synchronized(widths)
}

/** The readings a paced consumer reports back. */
private case class ConsumerPacingProbe(
    recordsConsumed: LongAccumulator,
    permitsEarned: LongAccumulator,
    permitsGranted: LongAccumulator,
    blocksObserved: LongAccumulator)

/** A live fault a reduce-side closure can apply to the executor it is running on. */
private sealed trait StreamingShuffleReduceSideFault extends Serializable {

  def applyTo(manager: StreamingShuffleManager): Int
}

/** Withdraws one shuffle's retained producer output from this executor's block registry. */
private class WithdrawRetainedOutputFault extends StreamingShuffleReduceSideFault {

  @volatile var shuffleId: Int = StreamingShuffleReduceSideFault.NO_SHUFFLE

  override def applyTo(manager: StreamingShuffleManager): Int = {
    if (shuffleId < 0) {
      0
    } else {
      StreamingShuffleReduceSideFault.streamingResolver(manager)
        .map(_.removeShuffle(shuffleId))
        .getOrElse(0)
    }
  }
}

/** Closes every consumer channel this executor is currently serving. */
private class PartitionEveryLinkFault(
    val reason: String,
    val holdMillis: Long,
    val sweepMillis: Long,
    val closeAttempts: Int)
  extends StreamingShuffleReduceSideFault {

  override def applyTo(manager: StreamingShuffleManager): Int = {
    val listener = manager.boundStreamingListener
    if (listener.isEmpty) {
      0
    } else {
      val closed = closeUntilSomethingCloses(listener.get)
      holdPartitionOpen(listener.get)
      closed
    }
  }

  /** Closes served channels until at least one closes, within a bounded number of attempts. */
  private def closeUntilSomethingCloses(listener: StreamingShuffleListener): Int = {
    val idle = new CountDownLatch(1)
    var closed = 0
    var attempts = 0
    while (closed == 0 && attempts < closeAttempts) {
      closed += listener.faultEveryServedChannel(reason)
      if (closed == 0) {
        attempts += 1
        idle.await(sweepMillis, TimeUnit.MILLISECONDS)
      }
    }
    closed
  }

  private def holdPartitionOpen(listener: StreamingShuffleListener): Unit = {
    val sweeper = new Thread(() => {
      val idle = new CountDownLatch(1)
      val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(holdMillis)
      while (System.nanoTime() < deadline) {
        idle.await(sweepMillis, TimeUnit.MILLISECONDS)
        listener.faultEveryServedChannel(reason)
      }
    }, "streaming-shuffle-integration-partition-sweeper")
    sweeper.setDaemon(true)
    sweeper.start()
  }
}

private object StreamingShuffleReduceSideFault {

  val NO_SHUFFLE: Int = -1

  /** Whether this executor JVM has already injected a fault. */
  private val injected = new AtomicBoolean(false)

  def injectOncePerExecutor(body: => Unit): Unit = {
    if (injected.compareAndSet(false, true)) {
      body
    }
  }

  def streamingManager(): Option[StreamingShuffleManager] = {
    SparkEnv.get.shuffleManager match {
      case manager: StreamingShuffleManager => Some(manager)
      case _ => None
    }
  }

  def streamingResolver(
      manager: StreamingShuffleManager): Option[StreamingShuffleBlockResolver] = {
    manager.shuffleBlockResolver match {
      case router: StreamingShuffleBlockRouter => Some(router.streamingResolver)
      case _ => None
    }
  }
}

/**
 * The accumulators a reduce-side fault reports itself through.
 *
 * @param egressBytesObserved bytes this executor had streamed when the fault landed
 * @param firstStageAttempts how many of those belonged to the stage's FIRST attempt, which is
 *     the only attempt that injects
 */
private case class ReduceSideFaultProbe(
    fault: StreamingShuffleReduceSideFault,
    egressBytesObserved: LongAccumulator,
    recordsConsumedBeforeFault: LongAccumulator,
    faultsApplied: LongAccumulator,
    faultEffect: LongAccumulator,
    faultOffers: LongAccumulator,
    managerPresent: LongAccumulator,
    firstStageAttempts: LongAccumulator) {

  def describe: String = {
    s"${faultOffers.value} offer(s), ${managerPresent.value} on the streaming manager, " +
      s"${firstStageAttempts.value} in the first stage attempt, ${faultsApplied.value} applied, " +
      s"${recordsConsumedBeforeFault.value} record(s) received, " +
      s"${egressBytesObserved.value} byte(s) streamed, effect ${faultEffect.value}"
  }
}

/**
 * Records what the scheduler did with a job, so that recovery can be asserted rather than assumed.
 */
private class StreamingShuffleJobRecorder extends SparkListener {

  private val failures = new mutable.ArrayBuffer[TaskFailedReason]()

  private val fetches = new mutable.ArrayBuffer[FetchFailed]()

  private val submissions = mutable.Map.empty[Int, Int]

  private var tasksEnded: Long = 0L

  private var memorySpilledTotal: Long = 0L

  private var diskSpilledTotal: Long = 0L

  private var peakMemoryHighWater: Long = 0L

  private var jobsFailed: Long = 0L

  private var jobsSucceeded: Long = 0L

  private var crashRemoteBlocks: Long = 0L

  private var crashRemoteBytes: Long = 0L

  private var crashLocalBlocks: Long = 0L

  private var crashLocalBytes: Long = 0L

  // When each stage was submitted and when it completed, taken from the scheduler's own stage
  // information rather than from a clock this listener reads.
  private val submittedAtMs = mutable.Map.empty[Int, Long]

  private val completedAtMs = mutable.Map.empty[Int, Long]

  private val writtenByStage = mutable.Map.empty[Int, Long]

  private val readByStage = mutable.Map.empty[Int, Long]

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = synchronized {
    val stageId = stageSubmitted.stageInfo.stageId
    submissions(stageId) = submissions.getOrElse(stageId, 0) + 1
    stageSubmitted.stageInfo.submissionTime.foreach { at =>
      if (!submittedAtMs.contains(stageId)) {
        submittedAtMs(stageId) = at
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = synchronized {
    val stageId = stageCompleted.stageInfo.stageId
    stageCompleted.stageInfo.completionTime.foreach(at => completedAtMs(stageId) = at)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    jobEnd.jobResult match {
      case JobSucceeded => jobsSucceeded += 1L
      case _ => jobsFailed += 1L
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    tasksEnded += 1L
    val metrics: TaskMetrics = taskEnd.taskMetrics
    if (metrics != null) {
      memorySpilledTotal += metrics.memoryBytesSpilled
      diskSpilledTotal += metrics.diskBytesSpilled
      peakMemoryHighWater = math.max(peakMemoryHighWater, metrics.peakExecutionMemory)
      val stageId = taskEnd.stageId
      writtenByStage(stageId) =
        writtenByStage.getOrElse(stageId, 0L) + metrics.shuffleWriteMetrics.bytesWritten
      readByStage(stageId) = readByStage.getOrElse(stageId, 0L) +
        metrics.shuffleReadMetrics.remoteBytesRead + metrics.shuffleReadMetrics.localBytesRead
    }
    taskEnd.reason match {
      case fetchFailed: FetchFailed =>
        fetches += fetchFailed
        failures += fetchFailed
      case failed: TaskFailedReason =>
        if (metrics != null &&
            failed.toErrorString.contains("Injected streaming shuffle producer crash")) {
          crashRemoteBlocks += metrics.shuffleReadMetrics.remoteBlocksFetched
          crashRemoteBytes += metrics.shuffleReadMetrics.remoteBytesRead
          crashLocalBlocks += metrics.shuffleReadMetrics.localBlocksFetched
          crashLocalBytes += metrics.shuffleReadMetrics.localBytesRead
        }
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

  def failedJobCount: Long = synchronized(jobsFailed)

  def succeededJobCount: Long = synchronized(jobsSucceeded)

  def memoryBytesSpilled: Long = synchronized(memorySpilledTotal)

  def diskBytesSpilled: Long = synchronized(diskSpilledTotal)

  def peakExecutionMemory: Long = synchronized(peakMemoryHighWater)

  def producingStageIds: Seq[Int] = synchronized {
    writtenByStage.filter(_._2 > 0L).keys.toSeq.sorted
  }

  def consumingStageIds: Seq[Int] = synchronized {
    readByStage.filter(_._2 > 0L).keys.toSeq.sorted
  }

  def bytesWrittenByStage(stageId: Int): Long = synchronized(writtenByStage.getOrElse(stageId, 0L))

  def bytesReadByStage(stageId: Int): Long = synchronized(readByStage.getOrElse(stageId, 0L))

  def submissionTimeOf(stageId: Int): Option[Long] = synchronized(submittedAtMs.get(stageId))

  def completionTimeOf(stageId: Int): Option[Long] = synchronized(completedAtMs.get(stageId))

  def remoteBlocksBeforeInjectedProducerCrash: Long = synchronized(crashRemoteBlocks)

  def remoteBytesBeforeInjectedProducerCrash: Long = synchronized(crashRemoteBytes)

  def localBlocksBeforeInjectedProducerCrash: Long = synchronized(crashLocalBlocks)

  def localBytesBeforeInjectedProducerCrash: Long = synchronized(crashLocalBytes)
}

/**
 * What one executor published under the streaming shuffle namespace when a probe task reached it.
 */
private case class ExecutorMetricObservation(
    executorId: String,
    metricNames: Set[String],
    partialReadInvalidations: Long,
    spillCount: Long,
    backpressureEvents: Long)
