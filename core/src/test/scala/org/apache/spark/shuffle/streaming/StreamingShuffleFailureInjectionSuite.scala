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

import java.io.{ByteArrayOutputStream, File, IOException, RandomAccessFile}
import java.util.Locale
import java.util.concurrent.{Callable, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.{FetchFailed, HashPartitioner, LocalSparkContext, ShuffleDependency,
  SparkConf, SparkContext, SparkEnv, SparkFunSuite, SparkThrowable, SparkThrowableHelper,
  TaskContext, TaskContextImpl, TaskFailedReason, TestUtils}
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.internal.LogKeys.{CLASS_NAME, COUNT, NUM_RECORDS_READ, RECORDS}
import org.apache.spark.internal.config.{SHUFFLE_COMPRESS, SHUFFLE_MANAGER,
  SHUFFLE_STREAMING_ENABLED, STAGE_MAX_CONSECUTIVE_ATTEMPTS, TASK_MAX_FAILURES}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.network.shuffle.protocol.streaming.{DataBlockMessage,
  RetransmitRequestMessage, StreamingShuffleChecksum, StreamingShuffleMessage,
  StreamTerminationMessage}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{JobSucceeded, SparkListener, SparkListenerJobEnd,
  SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.{BlockManagerId, DiskBlockObjectWriter, ShuffleBlockId,
  TempShuffleBlockId}
import org.apache.spark.util.{Clock, LongAccumulator, ManualClock, ThreadUtils}

/**
 * Failure injection across the ten conditions the streaming shuffle is specified to survive, each
 * one proven to lose nothing.
 */
class StreamingShuffleFailureInjectionSuite
  extends SparkFunSuite
  with LocalSparkContext
  with StreamingShuffleTestHelper
  with StreamingShuffleHadoopCredentialIsolation {

  import StreamingShuffleFaultScenario._
  import StreamingShuffleTestHelper._

  private val PartitionCount: Int = DefaultPartitionCount

  private val LiveDatasetBytes: Long = TargetDatasetBytes / 4L

  private val SpillingDatasetBytes: Long = TargetDatasetBytes

  private val RecordsBeforeLiveFault: Int = 16

  private val RecordsBeforeMapFailure: Int = 256

  private val InjectedMapFailureMarker: String = "Injected streaming shuffle live map failure"

  private val LivePartitionHoldMillis: Long = ProducerConnectionTimeoutMillis + 2000L

  private val BriefStallMillis: Long = ProducerConnectionTimeoutMillis / 2L

  private val ExtendedStallMillis: Long = ConsumerLivenessTimeoutMillis + 2000L

  private val ClusterMaster: String = "local-cluster[2,1,1024]"

  private val ExecutorCount: Int = 2

  private val ExecutorStartupTimeoutMillis: Long = 60000L

  private val MaxTaskFailures: Int = 4

  private val ShuffleId: Int = 0

  private val MapId: Long = 0L

  private val PartitionId: Int = 0

  private val FirstSequence: Long = 0L

  private val ConsumerId: String = "streaming-shuffle-failure-injection-consumer"

  private val PayloadBytes: Int = 1024

  private val RoomyExecutorMemoryBytes: Long = 64L * 1024L * 1024L

  private val WindowBlockCount: Int = 4

  private val ConcurrentProducerCount: Int = 3

  private val LiveLossRecordCount: Int = 1000000

  private val LiveLossPartitions: Int = 2

  private val LiveLossMapId: Long = 0L

  private val LiveLossWriterAttemptId: Long = 8100L

  private val LiveLossReaderAttemptId: Long = 8101L

  private val LiveLossWaitMillis: Long = 30000L

  private val LiveLossJoinMillis: Long = 120000L

  private val LiveLossFailureMessage: String =
    "Injected streaming shuffle producer loss under a reading consumer"

  private val LiveFaultDatasetBytes: Long = 8L * 1024L * 1024L

  private val LiveFaultKeyCount: Int = 128

  private val ConcurrentFailingMapPartitions: Set[Int] = Set(0, 2, 5)

  private val ExtendedDowntimeMillis: Long = ConsumerLivenessTimeoutMillis * 6L

  private val ShortPauseMillis: Long = SpillPollIntervalMillis * 3L

  /**
   * The records every production reader fixture streams, and the oracle its read is compared to.
   */
  private val FixtureRecords: Seq[(Int, Int)] = (1 to 24).map(value => (value, value * 7))

  private val FixtureCoordinatorEpoch: Long = 11L

  private val FixtureCapabilityToken: String = "streaming-shuffle-failure-injection-token"

  private val FixtureProducerAttemptId: Long = 400L

  private val FixtureProducerBasePort: Int = 17000

  private val FixtureFullRangeStart: Int = 0

  private val FixtureFullRangeEnd: Int = Int.MaxValue

  /** Map index of a fixture's first producer generation, which is also its read order. */
  private val FixtureFirstMapIndex: Int = 0

  /** Map id of a fixture's first producer generation, which its blocks are stamped with. */
  private val FixtureFirstMapId: Long = 0L

  private val InjectedDatasetBytes: Long = 2L * 1024L * 1024L

  private val CrashingMapPartition: Int = 3

  private val RecordsBeforeInjectedFault: Int = 64

  private val NarrowestBufferSizePercent: Int = 1

  private val LowestSpillThresholdPercent: Int = 50

  /** The in-job stall the garbage-collection scenario injects. */
  private val ConsumerStallMillis: Long = 2000L

  private val ConcurrentlySeveredPartitions: Set[Int] = Set(2, 5, 7)

  private val openManagers = mutable.ArrayBuffer.empty[MemorySpillManager]

  private val openMemoryManagers = mutable.ArrayBuffer.empty[TaskMemoryManager]

  private var taskAttemptCounter: Long = 0L

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Both of these are process-scoped and outlive a test: the metrics source is a JVM singleton
    // whose counters accumulate, and the buffer allowance is derived once per executor.
    resetStreamingShuffleMetrics()
    MemorySpillManager.resetSharedStateForTesting()
    LiveFaultHolder.reset()
    LiveStreamingFault.resetInjection()
    openManagers.clear()
    openMemoryManagers.clear()
  }

  override def afterEach(): Unit = {
    try {
      openManagers.foreach(manager => manager.close())
      openMemoryManagers.foreach { memoryManager =>
        val leaked = memoryManager.cleanUpAllAllocatedMemory()
        assert(leaked === 0L,
          s"A closed streaming shuffle buffer store left $leaked bytes of execution memory " +
            "acquired; buffered bytes must be released whatever the outcome of the task")
      }
    } finally {
      openManagers.clear()
      openMemoryManagers.clear()
      MemorySpillManager.resetSharedStateForTesting()
      LiveFaultHolder.reset()
      LiveStreamingFault.resetInjection()
      super.afterEach()
      resetStreamingShuffleMetrics()
      MemorySpillManager.resetSharedStateForTesting()
    }
  }

  /** A memory consumer whose only purpose is to be a spill trigger other than the store itself. */
  private class SpillTriggerConsumer(memoryManager: TaskMemoryManager)
    extends MemoryConsumer(memoryManager, MemoryMode.ON_HEAP) {

    override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
  }

  /** A buffer store whose allocator can be made to grant strictly less than it was asked for. */
  private class GrantCappingSpillManager(
      memoryManager: TaskMemoryManager,
      conf: SparkConf,
      clock: Clock,
      quota: MemorySpillManager.ExecutorBufferQuota)
    extends MemorySpillManager(memoryManager, conf, clock, Some(quota), autoPoll = false) {

    private val grantCeiling = new AtomicLong(Long.MaxValue)

    private val requestedBytes = new AtomicLong(0L)

    private val grantedBytes = new AtomicLong(0L)

    def capGrantsAt(bytes: Long): Unit = grantCeiling.set(bytes)

    def lastRequestedBytes: Long = requestedBytes.get()

    def lastGrantedBytes: Long = grantedBytes.get()

    override def acquireMemory(size: Long): Long = {
      requestedBytes.set(size)
      val granted = super.acquireMemory(math.min(size, grantCeiling.get()))
      grantedBytes.set(granted)
      granted
    }
  }

  /**
   * A spill writer that commits a fixed number of blocks and then fails the device.
   *
   * @param failAfterWrites raw-byte writes that succeed before the device fails
   */
  private class FailingSpillWriter(
      spillFile: File,
      serializerManager: SerializerManager,
      serializerInstance: SerializerInstance,
      bufferSizeBytes: Int,
      writeMetrics: ShuffleWriteMetrics,
      spillBlockId: TempShuffleBlockId,
      failAfterWrites: Int)
    extends DiskBlockObjectWriter(spillFile, serializerManager, serializerInstance, bufferSizeBytes,
      syncWrites = false, writeMetrics, spillBlockId) {

    private val writesAttempted = new AtomicInteger(0)

    private val reverted = new AtomicBoolean(false)

    override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
      if (writesAttempted.incrementAndGet() > failAfterWrites) {
        throw new IOException(
          s"Injected device failure writing block ${writesAttempted.get()} of ${spillBlockId.name}")
      }
      super.write(kvBytes, offs, len)
    }

    override def revertPartialWritesAndClose(): File = {
      reverted.set(true)
      super.revertPartialWritesAndClose()
    }

    def wasReverted: Boolean = reverted.get()

    def writeAttempts: Int = writesAttempted.get()
  }

  /**
   * A buffer store whose spill files are written through a device that fails part way through.
   *
   * @param failAfterWrites blocks committed before the device fails
   */
  private class FailingDeviceSpillManager(
      memoryManager: TaskMemoryManager,
      conf: SparkConf,
      clock: Clock,
      quota: MemorySpillManager.ExecutorBufferQuota,
      failAfterWrites: Int)
    extends MemorySpillManager(memoryManager, conf, clock, Some(quota), autoPoll = false) {

    private val writers = mutable.ArrayBuffer.empty[FailingSpillWriter]

    override private[streaming] def newSpillWriter(
        blockId: TempShuffleBlockId,
        file: File,
        serializer: SerializerInstance,
        bufferSizeBytes: Int,
        metrics: ShuffleWriteMetrics): DiskBlockObjectWriter = synchronized {
      val writer = new FailingSpillWriter(file, SparkEnv.get.serializerManager, serializer,
        bufferSizeBytes, metrics, blockId, failAfterWrites)
      writers += writer
      writer
    }

    def openedWriters: Seq[FailingSpillWriter] = synchronized(writers.toSeq)
  }

  /** Collects every task-failure reason a run produces. */
  private class TaskDisturbanceRecorder extends SparkListener {

    private val reasons = mutable.ArrayBuffer.empty[String]

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      taskEnd.reason match {
        case failure: TaskFailedReason => synchronized(reasons += failure.toErrorString)
        case _ =>
      }
      ()
    }

    def failureReasons: Seq[String] = synchronized(reasons.toSeq)
  }

  /** Records how every job of an application ended. */
  private class JobOutcomeRecorder extends SparkListener {

    private val succeeded = new AtomicInteger(0)

    private val failed = new AtomicInteger(0)

    private val failures = mutable.ArrayBuffer.empty[String]

    private val taskFailures = mutable.ArrayBuffer.empty[TaskFailedReason]

    private val fetchFailed = mutable.ArrayBuffer.empty[FetchFailed]

    private val submissions = mutable.Map.empty[Int, Int]

    private var memorySpilled: Long = 0L

    private var diskSpilled: Long = 0L

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      jobEnd.jobResult match {
        case JobSucceeded =>
          succeeded.incrementAndGet()
        case other =>
          synchronized(failures += other.toString)
          failed.incrementAndGet()
      }
      ()
    }

    override def onStageSubmitted(
        stageSubmitted: SparkListenerStageSubmitted): Unit = synchronized {
      val stageId = stageSubmitted.stageInfo.stageId
      submissions(stageId) = submissions.getOrElse(stageId, 0) + 1
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
      val metrics: TaskMetrics = taskEnd.taskMetrics
      if (metrics != null) {
        memorySpilled += metrics.memoryBytesSpilled
        diskSpilled += metrics.diskBytesSpilled
      }
      taskEnd.reason match {
        case fetch: FetchFailed =>
          fetchFailed += fetch
          taskFailures += fetch
        case failed: TaskFailedReason =>
          taskFailures += failed
        case _ =>
      }
    }

    def succeededJobCount: Int = succeeded.get()

    def failedJobCount: Int = failed.get()

    def failureDescriptions: Seq[String] = synchronized(failures.toSeq)

    def countedFailures: Seq[TaskFailedReason] = synchronized(taskFailures.toSeq)

    def fetchFailures: Seq[FetchFailed] = synchronized(fetchFailed.toSeq)

    def maxStageSubmissions: Int = synchronized {
      if (submissions.isEmpty) 0 else submissions.values.max
    }

    def memoryBytesSpilled: Long = synchronized(memorySpilled)

    def diskBytesSpilled: Long = synchronized(diskSpilled)
  }

  private val ListenerDrainTimeoutMillis: Long = DefaultAwaitTimeoutMillis

  private def payload(seed: Long): Array[Byte] = payloadOfLength(seed, PayloadBytes)

  private val BlockChargeBytes: Long =
    PayloadBytes.toLong + MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES

  private val SpillTriggeringExecutorMemoryBytes: Long = {
    val allowance = BlockChargeBytes * (WindowBlockCount + 1).toLong
    allowance * PercentScale / DefaultBufferSizePercent.toLong
  }

  private def memoryManagerOf(context: TaskContext): TaskMemoryManager = context.taskMemoryManager()

  private def newTrackedTaskContext(): TaskContextImpl = {
    taskAttemptCounter += 1L
    val context = newTaskContext(
      sc.env,
      partitionId = PartitionId,
      taskAttemptId = taskAttemptCounter,
      numPartitions = PartitionCount)
    openMemoryManagers += memoryManagerOf(context)
    context
  }

  private def newQuota(
      executorMemoryBytes: Long,
      bufferSizePercent: Int = DefaultBufferSizePercent,
      spillThresholdPercent: Int = DefaultSpillThresholdPercent
  ): MemorySpillManager.ExecutorBufferQuota = {
    new MemorySpillManager.ExecutorBufferQuota(
      bufferSizePercent, spillThresholdPercent, () => executorMemoryBytes)
  }

  private def newBufferStore(
      context: TaskContextImpl,
      clock: Clock,
      quota: MemorySpillManager.ExecutorBufferQuota,
      partitions: Int = PartitionCount,
      conf: SparkConf = streamingConfWithOverrides()): MemorySpillManager = {
    track(new MemorySpillManager(
      memoryManagerOf(context), conf, clock, Some(quota), autoPoll = false), context, partitions)
  }

  /**
   * Prepares a store for use and registers it for closure, whatever kind of store it is.
   *
   * @tparam T the store's concrete type, so a fixture's own accessors survive the call
   */
  private def track[T <: MemorySpillManager](
      manager: T,
      context: TaskContextImpl,
      partitions: Int): T = {
    manager.registerPartitionCount(partitions)
    manager.registerCleanup(context)
    openManagers += manager
    manager
  }

  private def bufferWindow(
      manager: MemorySpillManager,
      partitionId: Int,
      count: Int,
      firstSequence: Long = FirstSequence): Seq[(Long, Array[Byte])] = {
    (0 until count).map { index =>
      val sequenceNumber = firstSequence + index.toLong
      val data = payload(sequenceNumber)
      assert(manager.bufferBlock(partitionId, sequenceNumber, data),
        s"block $sequenceNumber of partition $partitionId must be admitted to build the window")
      sequenceNumber -> data
    }
  }

  private def spillFilesOf(manager: MemorySpillManager): Seq[File] =
    manager.allSpilledBlocks.map(record => record.file).distinct

  /** A degradation policy with streaming in service and an injected clock. */
  private def activePolicy(clock: Clock): StreamingShuffleFallbackPolicy = {
    val policy = new StreamingShuffleFallbackPolicy(streamingConfWithOverrides(), clock)
    assert(policy.streamingActive, "a fixture policy must start with streaming in service")
    policy
  }

  private def liveStreamingManager(): StreamingShuffleManager = {
    SparkEnv.get.shuffleManager match {
      case manager: StreamingShuffleManager => manager
      case other => fail(
        s"the application must be running the streaming manager but was running ${other.getClass}")
    }
  }

  private def startStreamingApplication(
      appName: String,
      master: String = "local[2,4]",
      conf: SparkConf = streamingConfWithOverrides(
        bufferSizePercent = MinBufferSizePercent,
        spillThreshold = MinSpillThresholdPercent)): Unit = {
    sc = new SparkContext(withLocalMaster(conf, appName, master).set(SHUFFLE_COMPRESS, false))
    assert(sc.getConf.get(SHUFFLE_MANAGER) === StreamingShuffleManager.SHORT_NAME,
      "the application must have selected the streaming shuffle manager")
    assert(sc.getConf.get(SHUFFLE_STREAMING_ENABLED),
      "the behaviour gate must be open so the streaming path is the one under test")
  }

  private def startLiveStreamingApplication(
      appName: String,
      conf: SparkConf = streamingConf()): Unit = {
    startStreamingApplication(
      appName,
      ClusterMaster,
      conf
        .set(TASK_MAX_FAILURES, MaxTaskFailures)
        .set(STAGE_MAX_CONSECUTIVE_ATTEMPTS, 2 * PartitionCount + 4))
    TestUtils.waitUntilExecutorsUp(sc, ExecutorCount, ExecutorStartupTimeoutMillis)
  }

  private def runRecorded[T](description: String)(body: => T): T = {
    val recorder = new JobOutcomeRecorder
    sc.addSparkListener(recorder)
    val result = try {
      body
    } finally {
      sc.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
      sc.removeSparkListener(recorder)
    }
    assert(recorder.failedJobCount === 0,
      s"no job of $description may fail, yet ${recorder.failedJobCount} did: " +
        recorder.failureDescriptions.mkString("[", ", ", "]"))
    assert(recorder.succeededJobCount >= 1,
      s"$description must have completed at least one job, but the scheduler declared none")
    result
  }

  // ---------------------------------------------------------------------------------------------
  // Live fault injection into the compared job.

  /**
   * The dataset the live half shuffles: real records, sized so blocks are genuinely outstanding.
   */
  private def keyedWorkload(
      context: SparkContext,
      numPartitions: Int,
      datasetBytes: Long): RDD[(Int, String)] = {
    largeDataset(context, numPartitions, datasetBytes)
  }

  private def liveWorkload(
      context: SparkContext,
      failingMapPartitions: Set[Int] = Set.empty,
      chainedShuffles: Boolean = false,
      datasetBytes: Long = LiveDatasetBytes): RDD[(Int, String)] = {
    val doomed = failingMapPartitions
    val marker = InjectedMapFailureMarker
    // Hoisted onto the driver's stack rather than read from the suite inside the closure.
    val failAfter = RecordsBeforeMapFailure
    val base = keyedWorkload(context, PartitionCount, datasetBytes)
    val withFailures = if (doomed.isEmpty) {
      base
    } else {
      base.mapPartitionsWithIndex { (partitionIndex, records) =>
        if (!doomed.contains(partitionIndex) || TaskContext.get().attemptNumber() != 0) {
          records
        } else {
          var seen = 0
          records.map { record =>
            seen += 1
            if (seen == failAfter) {
              throw new IllegalStateException(s"$marker in map partition $partitionIndex")
            }
            record
          }
        }
      }
    }
    val partitioned = withFailures.partitionBy(new HashPartitioner(PartitionCount))
    if (chainedShuffles) {
      partitioned.map(record => record).partitionBy(new HashPartitioner(PartitionCount))
    } else {
      partitioned
    }
  }

  private def liveDigestOf(shuffled: RDD[(Int, String)]): Set[(Int, Int, Int)] = {
    shuffled.map { case (key, value) => (key, value.length, value.hashCode) }.collect().toSet
  }

  private def liveSortBaseline(datasetBytes: Long = LiveDatasetBytes): Set[(Int, Int, Int)] = {
    val baselineContext = new SparkContext(withLocalMaster(
      sortBaselineConf(), "streaming-shuffle-live-sort-baseline", "local[2]"))
    val baseline = try {
      liveDigestOf(liveWorkload(baselineContext, datasetBytes = datasetBytes))
    } finally {
      baselineContext.stop()
    }
    assert(baseline.nonEmpty, "the sort-based baseline must produce output to compare against")
    baseline
  }

  private def runLiveFault(
      scenario: StreamingShuffleFaultScenario,
      baseline: Set[(Int, Int, Int)],
      fault: LiveStreamingFault,
      failingMapPartitions: Set[Int] = Set.empty,
      chainedShuffles: Boolean = false,
      datasetBytes: Long = LiveDatasetBytes): LiveFaultRun = {
    val probe = LiveFaultProbe(
      liveFaultAccumulator(s"offers-${scenario.faultName}"),
      liveFaultAccumulator(s"receipt-${scenario.faultName}"),
      liveFaultAccumulator(s"egress-${scenario.faultName}"),
      liveFaultAccumulator(s"applied-${scenario.faultName}"),
      liveFaultAccumulator(s"effect-${scenario.faultName}"))
    val recorder = new JobOutcomeRecorder
    sc.addSparkListener(recorder)
    val shuffled = liveWorkload(sc, failingMapPartitions, chainedShuffles, datasetBytes)
    val dependency = shuffled.dependencies.headOption match {
      case Some(shuffle: ShuffleDependency[_, _, _]) => shuffle
      case other => fail(
        s"the workload carrying ${scenario.faultName} must depend on a shuffle but depended on " +
          s"$other")
    }
    assert(dependency.shuffleHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
      s"the shuffle carrying ${scenario.faultName} must have been registered on the streaming " +
        "path, or the fault would be injected into a sort-based read and this scenario would " +
        s"prove nothing about streaming, but it carried a ${dependency.shuffleHandle.getClass}")
    fault.bindShuffle(dependency.shuffleId)
    val observed = try {
      liveDigestOf(injectingRead(shuffled, fault, probe))
    } finally {
      sc.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
      sc.removeSparkListener(recorder)
    }

    assert(observed.nonEmpty,
      s"the streaming run under ${scenario.faultName} produced nothing at all, so the job did " +
        "not complete in any useful sense")
    assertNoDataLoss(observed, baseline, s"a live streaming shuffle under ${scenario.faultName}")
    assert(recorder.failedJobCount === 0,
      s"every job must have completed despite ${scenario.faultName}, but " +
        s"${recorder.failedJobCount} failed outright, which would mean the fault went unrecovered")
    assert(!sc.isStopped,
      s"the application must still be live after ${scenario.faultName}, because a fault costs " +
        "throughput and never the job")

    assert(probe.offers.value >= 1L,
      s"a consumer must have reached the injection point under ${scenario.faultName}, which " +
        s"means receiving $RecordsBeforeLiveFault record(s) on its first attempt, but none did: " +
        probe.describe)
    assert(probe.receipt.value >= RecordsBeforeLiveFault.toLong,
      s"receipt must be partial rather than absent when ${scenario.faultName} lands, but: " +
        probe.describe)
    assert(probe.egress.value > 0L,
      s"the producing side must already have put bytes on the wire when ${scenario.faultName} " +
        "lands, or the fault would interrupt nothing: " + probe.describe)
    LiveFaultRun(probe, recorder, observed, dependency.shuffleId)
  }

  /**
   * What one live-fault run leaves behind for a scenario to assert on.
   *
   * @param shuffleId the shuffle the fault was bound to
   */
  private case class LiveFaultRun(
      probe: LiveFaultProbe,
      recorder: JobOutcomeRecorder,
      observed: Set[(Int, Int, Int)],
      shuffleId: Int) {

    def fetchFailuresForShuffle: Seq[FetchFailed] =
      recorder.fetchFailures.filter(_.shuffleId == shuffleId)

    def injectedMapFailures: Seq[TaskFailedReason] =
      recorder.countedFailures.filter(_.toErrorString.contains(InjectedMapFailureMarker))
  }

  private def injectingRead(
      shuffled: RDD[(Int, String)],
      fault: LiveStreamingFault,
      probe: LiveFaultProbe): RDD[(Int, String)] = {
    val offers = probe.offers
    val receipt = probe.receipt
    val egress = probe.egress
    val applied = probe.applied
    val effect = probe.effect
    val threshold = RecordsBeforeLiveFault
    shuffled.mapPartitions { records =>
      if (TaskContext.get().attemptNumber() != 0) {
        records
      } else {
        var consumed = 0
        var acted = false
        records.map { record =>
          consumed += 1
          if (!acted && consumed >= threshold) {
            acted = true
            offers.add(1L)
            LiveStreamingFault.streamingManager().foreach { manager =>
              receipt.add(consumed.toLong)
              manager.boundStreamingListener.foreach(listener => egress.add(listener.streamedBytes))
              LiveStreamingFault.injectOncePerExecutor {
                applied.add(1L)
                effect.add(fault.applyTo(manager).toLong)
              }
            }
          }
          record
        }
      }
    }
  }

  private def liveFaultAccumulator(name: String): LongAccumulator = {
    val accumulator = new LongAccumulator
    accumulator.register(sc, name = Some(s"streamingShuffleLive-$name"), countFailedValues = true)
    accumulator
  }

  /**
   * Discards every block accepted from one producer, atomically, and reports the fetch failure that
   * makes the unmodified scheduler recompute the upstream stage.
   *
   * @param recordsOffered records the producer offered before it died
   */
  private case class LiveProducerLossOutcome(
      shuffleId: Int,
      recordsOffered: Int,
      recordsRead: Int,
      consumerWasReading: Boolean,
      producerStopped: Boolean,
      writeFailure: Option[Throwable],
      consumerFailure: Option[Throwable],
      invalidationDelta: Long)

  private def loseProducerUnderReadingConsumer(
      recordCount: Int = LiveLossRecordCount): LiveProducerLossOutcome = {
    val manager = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]
    val handle = shuffleDependencyFor(sc, sc.getConf, numPartitions = LiveLossPartitions,
      numRecords = 1).shuffleHandle.asInstanceOf[StreamingShuffleHandle[Int, Int, Int]]
    val writerContext = newTaskContext(sc.env, partitionId = 0,
      taskAttemptId = LiveLossWriterAttemptId, numPartitions = LiveLossPartitions)
    val readerContext = newTaskContext(sc.env, partitionId = 0,
      taskAttemptId = LiveLossReaderAttemptId, numPartitions = LiveLossPartitions)

    val consumerConsumedOne = new CountDownLatch(1)
    val produced = new AtomicInteger(0)
    val consumed = new AtomicInteger(0)
    val consumerWasReading = new AtomicBoolean(false)
    val producerStopped = new AtomicBoolean(false)
    val writerHandle = new AtomicReference[StreamingShuffleWriter[Int, Int, Int]](null)
    val writeFailure = new AtomicReference[Throwable](null)
    val producerFailure = new AtomicReference[Throwable](null)
    val consumerFailure = new AtomicReference[Throwable](null)
    val invalidationsBefore = observedPartialReadInvalidations()

    val producer = new Thread(() => {
      try {
        TaskContext.setTaskContext(writerContext)
        val writer = manager
          .getWriter[Int, Int](handle, LiveLossMapId, writerContext,
            writerContext.taskMetrics.shuffleWriteMetrics)
          .asInstanceOf[StreamingShuffleWriter[Int, Int, Int]]
        writerHandle.set(writer)
        val records = new Iterator[Product2[Int, Int]] {
          private var waited = false

          override def hasNext: Boolean = produced.get() < recordCount

          override def next(): Product2[Int, Int] = {
            if (!waited && writer.blocksStreamed >= 1L) {
              waited = true
              consumerWasReading.set(
                consumerConsumedOne.await(LiveLossWaitMillis, TimeUnit.MILLISECONDS))
              throw new IllegalStateException(LiveLossFailureMessage)
            }
            val index = produced.getAndIncrement()
            (index * LiveLossPartitions, index)
          }
        }
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
    }, "streaming-shuffle-injected-loss-producer")

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
    }, "streaming-shuffle-injected-loss-consumer")

    // The shared settlement owns the whole lifecycle of these two threads: it joins them inside the
    // budget, and on expiry releases the latch either half may be parked on, interrupts whatever
    // that did not free, re-joins inside a bounded grace, and only THEN completes the task contexts
    // that hold this pair's execution-memory reservations.
    runBoundedThreadedScenario(
      threads = Seq(producer, consumer),
      taskContexts = Seq(writerContext, readerContext),
      description = "the injected producer loss under a reading consumer",
      joinTimeoutMillis = LiveLossJoinMillis,
      releaseWaits = () => consumerConsumedOne.countDown()) {
      producer.start()
      eventually(timeout(60.seconds), interval(20.milliseconds)) {
        assert(writerHandle.get() != null || producerFailure.get() != null,
          "the producer must have been given a streaming writer, or have failed trying")
      }
      assert(producerFailure.get() == null,
        s"the producer must have been given a streaming writer, but failed with " +
          s"${producerFailure.get()}")
      consumer.start()
    }
    assert(producerFailure.get() == null,
      s"the producer half must have failed only where the fault was injected, but raised " +
        s"${producerFailure.get()}")

    LiveProducerLossOutcome(
      shuffleId = handle.shuffleId,
      recordsOffered = produced.get(),
      recordsRead = consumed.get(),
      consumerWasReading = consumerWasReading.get(),
      producerStopped = producerStopped.get(),
      writeFailure = Option(writeFailure.get()),
      consumerFailure = Option(consumerFailure.get()),
      invalidationDelta = observedPartialReadInvalidations() - invalidationsBefore)
  }

  private def assertProductionInvalidation(loss: LiveProducerLossOutcome): Unit = {
    assert(loss.writeFailure.exists(failure =>
      failure.toString.contains(LiveLossFailureMessage) ||
        Option(failure.getCause).exists(_.toString.contains(LiveLossFailureMessage))),
      s"the injected loss must have escaped the production write, but the write ended with " +
        s"${loss.writeFailure} after ${loss.recordsOffered} record(s)")
    assert(loss.producerStopped,
      "the lost producer must have been stopped unsuccessfully, which is the production " +
        "withdrawal path a failed task takes")
    assert(loss.consumerWasReading && loss.recordsRead > 0,
      s"a consumer must have consumed from the producer before it went, or there were no partial " +
        s"reads to invalidate; ${loss.recordsRead} record(s) were read of " +
        s"${loss.recordsOffered} offered")
    assert(loss.consumerFailure.isDefined,
      s"the consumer must fail rather than present the ${loss.recordsRead} record(s) it happened " +
        "to hold as a complete partition")
    val reason = loss.consumerFailure.flatMap(fetchFailureIn)
    assert(reason.isDefined,
      s"the production reader must have raised FetchFailedException, the one signal the " +
        s"unmodified scheduler answers by recomputing, but raised ${loss.consumerFailure}")
    assert(reason.get.shuffleId === loss.shuffleId,
      s"the fetch failure must name the shuffle whose producer was lost, ${loss.shuffleId}, but " +
        s"named ${reason.get.shuffleId}")
    assert(loss.invalidationDelta >= 1L,
      s"the production reader must have counted its partial-read invalidation on " +
        s"shuffle.streaming.partialReadInvalidations, but the counter moved by " +
        s"${loss.invalidationDelta}")
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

  private def recordingBufferTaken(bytes: Array[Byte]): RecordingStreamingManagedBuffer = {
    val buffer = recordingBuffer(bytes)
    buffer.retain()
    buffer
  }

  private def discardFramesAndEscalate(
      buffers: Seq[RecordingStreamingManagedBuffer],
      survivors: Seq[RecordingStreamingManagedBuffer],
      reduceId: Int,
      shuffleId: Int = ShuffleId): Nothing = {
    val survivorReferencesBefore = survivors.map(buffer => buffer.outstandingReferences)
    buffers.foreach { buffer =>
      buffer.release()
      ()
    }
    assert(buffers.forall(buffer => buffer.outstandingReferences == 0),
      "every frame from the failed producer must be released by the one invalidation, not some " +
        s"of them: outstanding references ${buffers.map(_.outstandingReferences).mkString(", ")}")
    assert(survivors.map(buffer => buffer.outstandingReferences) == survivorReferencesBefore,
      "invalidating one producer must leave every other producer's frames exactly as they were, " +
        "otherwise a reduce task would discard data it is entitled to keep")
    StreamingShuffleMetricsSource.incrementPartialReadInvalidations(1L)
    throw new FetchFailedException(
      null.asInstanceOf[BlockManagerId],
      shuffleId,
      MapId,
      MapId.toInt,
      reduceId,
      s"Injected streaming shuffle producer loss for shuffle $shuffleId partition $reduceId")
  }

  /**
   * A REAL consumer -- production reader, production consumer handler, production flow control --
   * reading from producers a test connector opens, so that a producer loss is invalidated by
   * production code rather than modelled by this suite.
   *
   * @param numMaps producer generations the coordinator offers, each feeding the same reduce
   *     partition; more than one is what makes "an invalidation reaches exactly one producer"
   *     assertable at all
   */
  private class ProducerLossFixture(
      numMaps: Int = 1,
      reducePartition: Int = PartitionId,
      records: Seq[(Int, Int)] = FixtureRecords) {

    val conf: SparkConf = streamingConf()
    val clock: ManualClock = newManualClock()
    val dependency: ShuffleDependency[Int, Int, Int] =
      shuffleDependencyFor(sc, conf, PartitionCount, numRecords = records.size)
    val shuffleId: Int = dependency.shuffleId
    val handle: StreamingShuffleHandle[Int, Int, Int] = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId, dependency, PartitionCount, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
      FixtureCoordinatorEpoch, FixtureCapabilityToken)
    val coordinator: StreamingShuffleCoordinator =
      new StreamingShuffleCoordinator(sc.env.rpcEnv, conf, clock)
    val backpressure: BackpressureProtocol = new BackpressureProtocol(
      conf, coordinator, TokenBucketRateLimiter.executorBudget(conf, clock), clock)
    val fallbackPolicy: StreamingShuffleFallbackPolicy =
      new StreamingShuffleFallbackPolicy(conf, clock)
    val connector: StreamingShuffleTestProducerConnector =
      new StreamingShuffleTestProducerConnector()
    val coordinatorRef: StreamingShuffleTestCoordinatorRef =
      new StreamingShuffleTestCoordinatorRef(conf, clock)
    val context: TaskContextImpl = newTrackedTaskContext()
    val readMetrics: RecordingStreamingShuffleReadMetrics =
      new RecordingStreamingShuffleReadMetrics

    val producers: Seq[StreamingShuffleProducerLocation] = {
      require(numMaps > 0, s"a consumer must be offered at least one producer but was $numMaps")
      (0 until numMaps).map { mapIndex =>
        StreamingShuffleProducerLocation(
          executorId = s"failure-injection-producer-$mapIndex",
          host = "failure-injection-producer-host",
          port = FixtureProducerBasePort + mapIndex,
          mapId = mapIndex.toLong,
          mapIndex = mapIndex,
          taskAttemptId = FixtureProducerAttemptId + mapIndex,
          blockManagerId = BlockManagerId(s"failure-injection-producer-$mapIndex",
            "failure-injection-block-manager", FixtureProducerBasePort + 1000 + mapIndex))
      }
    }

    coordinatorRef.answerLookupWith(Some(StreamingShuffleProducerLocations(
      shuffleId = shuffleId,
      locations = producers,
      numPartitions = PartitionCount,
      numMaps = numMaps,
      completedMapIndexes = producers.indices.toSet,
      protocolVersion = StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
      coordinatorEpoch = FixtureCoordinatorEpoch,
      fallback = StreamingShuffleFallbackState())))

    val reader: StreamingShuffleReader[Int, Int] = new StreamingShuffleReader[Int, Int](
      handle, FixtureFullRangeStart, FixtureFullRangeEnd, reducePartition, reducePartition + 1,
      context, readMetrics, conf,
      StreamingShuffleReaderContext(coordinatorRef, backpressure, fallbackPolicy, connector,
        sc.env.serializerManager),
      clock)

    def payloadOf(mapId: Long): Array[Byte] = {
      val bytes = new ByteArrayOutputStream()
      val wrapped = sc.env.serializerManager.wrapStream(
        ShuffleBlockId(shuffleId, mapId, reducePartition), bytes)
      val serialized = dependency.serializer.newInstance().serializeStream(wrapped)
      records.foreach { record =>
        serialized.writeKey(record._1)
        serialized.writeValue(record._2)
      }
      serialized.close()
      bytes.toByteArray
    }

    def blocksOf(mapId: Long, blocks: Int = WindowBlockCount): Seq[DataBlockMessage] = {
      val payload = payloadOf(mapId)
      assert(payload.nonEmpty, "a partition payload must carry bytes to be cut into blocks")
      val chunkSize = math.max(1, (payload.length + blocks - 1) / blocks)
      payload.grouped(chunkSize).toSeq.zipWithIndex.map { chunk =>
        dataBlock(shuffleId, mapId, reducePartition, chunk._2.toLong, chunk._1)
      }
    }

    def terminatorOf(mapId: Long, totalBlocks: Long): StreamTerminationMessage =
      streamTermination(shuffleId, mapId, reducePartition, totalBlocks)

    def streamOf(mapIndex: Int): StreamingShuffleTestProducerStream = {
      val open = connector.streams.filter(stream => stream.location.mapIndex == mapIndex)
      assert(open.size === 1,
        s"exactly one channel must be open to the producer of map index $mapIndex but " +
          s"${open.size} were, among " +
          connector.streams.map(stream => stream.location.mapIndex).mkString(", "))
      open.head
    }

    def loseProducer(mapIndex: Int): Unit = {
      val stream = streamOf(mapIndex)
      stream.closeChannel()
      stream.handler.channelInactive(stream.client)
      // The transition is applied off the event loop, exactly as an arriving frame is, so the loss
      // it records exists only once that work has run.
      stream.awaitDataPlane(s"the channel-inactive transition of map index $mapIndex")
      assert(stream.handler.isProducerLost,
        s"the production handler bound to map index $mapIndex must have observed the loss, or " +
          "nothing has been injected")
    }

    /** The records every producer of this fixture streams, and the oracle a read is compared to. */
    val streamedRecords: Seq[(Int, Int)] = records

    def readAll(streamed: Iterator[Product2[Int, Int]]): Seq[(Int, Int)] =
      streamed.map(record => (record._1, record._2)).toSeq

    def creditLedgersOf(mapId: Long): Seq[BackpressureStreamKey] =
      backpressure.registeredStreams.filter(key =>
        key.partitionId == reducePartition && key.mapId == mapId)

    def reducePartitionId: Int = reducePartition

    /** Releases everything this fixture holds, whatever the case did with it. */
    def release(): Unit = {
      context.markTaskCompleted(None)
      backpressure.reset()
      coordinator.reset()
    }
  }

  /**
   * Runs a body against a production reader fixture and always releases it.
   *
   * @param fixture the fixture to run against
   */
  private def withProducerLoss[T](
      fixture: ProducerLossFixture)(body: ProducerLossFixture => T): T = {
    try {
      body(fixture)
    } finally {
      fixture.release()
    }
  }

  private def fetchFailedReasonOf(failure: FetchFailedException): FetchFailed =
    failure.toTaskFailedReason.asInstanceOf[FetchFailed]

  private def withTaskContext[T](context: TaskContextImpl)(body: => T): T = {
    TaskContext.setTaskContext(context)
    try {
      body
    } finally {
      TaskContext.unset()
    }
  }

  test("a producer crash during write invalidates partial reads and loses nothing") {
    val baseline = liveSortBaseline()
    startLiveStreamingApplication("streaming-shuffle-producer-crash")

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    val notifier = new StreamingShuffleErrorNotifier(ShuffleId, sc.getConf)
    val context = newTrackedTaskContext()

    injector.crashProducer()
    assert(injector.isArmed(ProducerCrashDuringWrite),
      "the producer crash must be armed before the consumer's timer is examined")

    val openedAtMillis = clock.getTimeMillis()
    advanceJustBeforeProducerTimeout(clock)
    val justBefore = clock.getTimeMillis() - openedAtMillis
    assert(justBefore === JustBeforeProducerTimeoutMillis,
      s"the fixture must stop one millisecond short of the deadline but stopped at $justBefore")
    assert(justBefore < ProducerConnectionTimeoutMillis,
      s"$justBefore ms must not reach the ${ProducerConnectionTimeoutMillis} ms producer " +
        "timeout,  because a timer that fires early is as wrong as one that never fires")

    clock.advance(ProducerConnectionTimeoutMillis - JustBeforeProducerTimeoutMillis)
    val elapsed = clock.getTimeMillis() - openedAtMillis
    assert(elapsed === ProducerConnectionTimeoutMillis,
      s"the deadline must be reached at exactly ${ProducerConnectionTimeoutMillis} ms, " +
        s"not $elapsed")

    val loss = loseProducerUnderReadingConsumer()
    assertProductionInvalidation(loss)
    logInfo(log"Live producer loss under a reading consumer: " +
      log"${MDC(NUM_RECORDS_READ, loss.recordsRead)} record(s) had been read of " +
      log"${MDC(RECORDS, loss.recordsOffered)} offered, the reader counted " +
      log"${MDC(COUNT, loss.invalidationDelta)} invalidation(s), and answered with " +
      log"${MDC(CLASS_NAME, loss.consumerFailure.map(_.getClass.getName).getOrElse("nothing"))}")

    val accepted = (0 until WindowBlockCount).map { index =>
      val buffer = recordingBuffer(payload(index.toLong))
      buffer.retain()
      buffer
    }
    val bridged = withTaskContext(context) {
      intercept[FetchFailedException] {
        discardFramesAndEscalate(accepted, Seq.empty, PartitionId)
      }
    }
    assertBuffersReleasedExactlyOnce(accepted)
    assert(context.fetchFailed.contains(bridged),
      "the fetch failure must have registered itself with the task context in its own " +
        "constructor  per SPARK-19276, which is what stops user code from hiding it")
    assert(bridged.toTaskFailedReason.toErrorString.contains(ShuffleId.toString),
      "the failure must convert to a fetch-failure reason naming the shuffle, because that " +
        "conversion is the entire interface to stage recomputation")
    notifier.setError(bridged)
    assertPublishedFailure[FetchFailedException](notifier, "a lost producer")
    assert(notifier.fetchFailure.contains(bridged),
      "the notifier must surface the fetch failure as such, so the task thread re-throws the " +
        "exception the scheduler recognises rather than a wrapper")
    val rethrown = intercept[FetchFailedException](notifier.throwIfError())
    assert(rethrown eq bridged,
      "throwIfError must re-throw the very failure the I/O thread published, on the task thread")

    val run = runLiveFault(
      ProducerCrashDuringWrite, baseline, new LiveProducerLoss, failingMapPartitions = Set(1))
    assert(run.injectedMapFailures.nonEmpty,
      "a write must have been interrupted part way along, which is what makes this a crash " +
        s"DURING write, but none of the ${run.recorder.countedFailures.size} task failure(s) the " +
        "driver saw carried the injected map-side loss")
    assert(run.fetchFailuresForShuffle.nonEmpty,
      s"losing a producer mid-drain must have raised a fetch failure against shuffle " +
        s"${run.shuffleId}, because that conversion is the entire interface to recomputation, " +
        s"but the driver saw ${run.recorder.fetchFailures.size} fetch failure(s), none naming it")
    assert(run.recorder.maxStageSubmissions >= 2,
      "the upstream stage must have been resubmitted, because recovering a lost producer means " +
        s"recomputing it, but no stage was submitted more than " +
        s"${run.recorder.maxStageSubmissions} time(s)")
  }

  test("a consumer crash during read retains the unacknowledged window and loses nothing") {
    val baseline = liveSortBaseline()
    startLiveStreamingApplication("streaming-shuffle-consumer-crash")

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    val context = newTrackedTaskContext()
    val store = newBufferStore(
      context, clock, newQuota(SpillTriggeringExecutorMemoryBytes), partitions = 1)
    assert(store.registerConsumer(ConsumerId), "the consumer must be admitted before it stops")

    val window = bufferWindow(store, PartitionId, WindowBlockCount)
    assert(store.bufferedBytes > 0L, "the window must hold bytes for the watchdog to have work")

    injector.crashConsumer()
    assert(injector.isArmed(ConsumerCrashDuringRead),
      "the consumer crash must be armed before the writer's watchdog is examined")

    val openedAtMillis = clock.getTimeMillis()
    advanceJustBeforeConsumerLivenessTimeout(clock)
    val justBefore = clock.getTimeMillis() - openedAtMillis
    assert(justBefore === JustBeforeConsumerLivenessMillis,
      s"the fixture must stop one millisecond short of the window but stopped at $justBefore")
    assert(justBefore < ConsumerLivenessTimeoutMillis,
      s"$justBefore ms must not reach the ${ConsumerLivenessTimeoutMillis} ms acknowledgement " +
        "window, so nothing may be treated as missing yet")
    assert(store.retainedBlockCount(PartitionId) === WindowBlockCount,
      "the whole window must still be retained before the watchdog fires, since a block released " +
        "early can never be retransmitted")

    clock.advance(ConsumerLivenessTimeoutMillis - JustBeforeConsumerLivenessMillis)
    assert(clock.getTimeMillis() - openedAtMillis === ConsumerLivenessTimeoutMillis,
      "the watchdog's deadline must be reached exactly at the contracted window")

    val utilisation = store.bufferUtilizationPercent
    assert(utilisation >= DefaultSpillThresholdPercent.toLong,
      s"the fixture must have crossed the ${DefaultSpillThresholdPercent} percent spill trigger " +
        s"for the retention path to be the one under test, but utilisation was " +
        s"$utilisation percent")
    val spillsBefore = store.spillCount
    assert(store.maybeSpill(),
      "a window above the spill trigger whose consumer has gone must be evicted to disk, never " +
        "dropped, because the bytes are still owed to a consumer that may return")
    assert(store.spillCount === spillsBefore + 1L,
      s"the eviction must be counted once, but the count moved to ${store.spillCount}")
    assert(store.diskBytesSpilled > 0L,
      "spilled volume must land on Spark's own disk-spill accounting rather than a private counter")
    window.foreach { case (sequenceNumber, expected) =>
      val servable = store.retainedPayload(PartitionId, sequenceNumber)
      assert(servable.isDefined,
        s"block $sequenceNumber must remain servable from spill for the retransmission " +
          "that follows")
      assert(servable.get.sameElements(expected),
        s"block $sequenceNumber must be servable byte for byte, or a retransmission would repair " +
          "corruption with corruption")
    }

    assert(RetryBaseBackoffMillis === 1000L,
      s"the retry ladder must start at one second but starts at $RetryBaseBackoffMillis ms")
    assert(MaxRetryAttempts === 5,
      s"the ladder must stop after five attempts but allows $MaxRetryAttempts")
    assert(RetryBackoffLadderMillis === Seq(1000L, 2000L, 4000L, 8000L, 16000L),
      s"the ladder must double from one second, but was $RetryBackoffLadderMillis")
    val ladderStartMillis = clock.getTimeMillis()
    val readings = advanceThroughRetryBackoff(clock)
    assert(readings.length === MaxRetryAttempts,
      s"walking the ladder must take exactly $MaxRetryAttempts steps but took ${readings.length}")
    assert(readings.last - ladderStartMillis === RetryBackoffLadderMillis.sum,
      "the ladder's total delay must be the sum of its rungs, so a reconnection is bounded")

    assert(ConsumerSlownessRatio === 2.0d,
      s"the shortfall that arms the window must be two-fold but was $ConsumerSlownessRatio")
    assert(SustainedSlownessWindowMillis === 60000L,
      s"the window must be sixty seconds but was $SustainedSlownessWindowMillis ms")
    assert(JustBeforeSustainedSlownessMillis === 59999L,
      s"the near miss must be 59999 ms but was $JustBeforeSustainedSlownessMillis")
    assert(SustainedSlownessTripMillis === 60001L,
      s"the trip must be 60001 ms but was $SustainedSlownessTripMillis")
    val slownessPolicy = activePolicy(clock)
    val producerRate = 4000.0d
    val stalledRate = producerRate / (2.0d * ConsumerSlownessRatio)
    slownessPolicy.recordProducerThroughput(ShuffleId, producerRate, 0L)
    slownessPolicy.recordConsumerThroughput(ShuffleId, stalledRate, 0L)
    assert(slownessPolicy.slownessArmedSinceMs(ShuffleId).contains(0L),
      "the window must arm on the first sample that shows the shortfall, since that is when the " +
        "clock the degradation is measured against starts")
    slownessPolicy.recordProducerThroughput(ShuffleId, producerRate,
      JustBeforeSustainedSlownessMillis)
    slownessPolicy.recordConsumerThroughput(ShuffleId, stalledRate,
      JustBeforeSustainedSlownessMillis)
    assert(slownessPolicy.streamingActive,
      s"$JustBeforeSustainedSlownessMillis ms of shortfall is not more than " +
        s"$SustainedSlownessWindowMillis ms, so streaming must still be in service")
    slownessPolicy.recordProducerThroughput(ShuffleId, producerRate, SustainedSlownessTripMillis)
    slownessPolicy.recordConsumerThroughput(ShuffleId, stalledRate, SustainedSlownessTripMillis)
    assert(slownessPolicy.hasTripped,
      s"$SustainedSlownessTripMillis ms of shortfall must stand streaming down")
    assert(slownessPolicy.trippedReason.contains(StreamingShuffleFallbackReason.ConsumerTooSlow),
      s"the latched reason must be consumer slowness but was ${slownessPolicy.trippedReason}")

    val run = runLiveFault(ConsumerCrashDuringRead, baseline, new LiveConsumerLoss())
    val consumerLosses = run.recorder.countedFailures.filter(
      failure => failure.toErrorString.contains(LiveStreamingFault.CONSUMER_LOSS_MARKER))
    assert(consumerLosses.nonEmpty,
      "a consuming task must actually have died mid-read, or the retry this scenario is about " +
        s"was never taken: the driver saw ${run.recorder.countedFailures.size} task failure(s), " +
        "none of them the injected consumer loss")
    assert(consumerLosses.size <= ExecutorCount,
      s"the loss must be injected at most once per executor, so at most $ExecutorCount consuming " +
        s"task(s) may die of it, but ${consumerLosses.size} did -- an injection that re-armed on " +
        "every attempt would never converge")
  }

  test("a network partition trips the fallback policy and the job still completes") {
    val baseline = liveSortBaseline()
    startLiveStreamingApplication("streaming-shuffle-network-partition")

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    injector.partitionNetwork()
    assert(injector.networkPartitioned, "the partition must be in force before it is evaluated")

    val policy = activePolicy(clock)
    val openedAtMillis = clock.getTimeMillis()
    advanceJustBeforeProducerTimeout(clock)
    assert(clock.getTimeMillis() - openedAtMillis === JustBeforeProducerTimeoutMillis,
      "the fixture must stop one millisecond short of the connection timeout")
    assert(policy.streamingActive,
      "a partition that has not yet outlasted the connection timeout must not stand streaming down")
    clock.advance(ProducerConnectionTimeoutMillis - JustBeforeProducerTimeoutMillis)

    assert(policy.streamingActive,
      "producer silence is not the link-utilisation fallback condition, so the standalone policy " +
        "must not claim NetworkSaturation without a capacity sample")

    val sortConstructors = classOf[SortShuffleManager].getConstructors
    assert(sortConstructors.length === 1,
      s"SortShuffleManager must expose one constructor but exposes ${sortConstructors.length}")
    assert(sortConstructors.head.getParameterCount === 1,
      "SortShuffleManager's constructor must take exactly the configuration, so the fallback " +
        s"target is byte for byte the one Spark ships, but it takes " +
        s"${sortConstructors.head.getParameterCount} arguments")

    val run = runLiveFault(
      NetworkPartition, baseline, new LiveLinkPartition(LivePartitionHoldMillis))
    assert(run.fetchFailuresForShuffle.nonEmpty,
      s"an unusable link must have timed out into a fetch failure against shuffle " +
        s"${run.shuffleId}, or nothing about the partition reached the scheduler: the driver saw " +
        s"${run.recorder.fetchFailures.size} fetch failure(s) and none named it")
    assert(run.recorder.maxStageSubmissions >= 2,
      "recovering across a partition means recomputing, so a stage must have been submitted more " +
        s"than once, but the most any was submitted is ${run.recorder.maxStageSubmissions}")

    val manager = liveStreamingManager()
    driveFallbackReason(
      manager.streamingFallbackPolicy, StreamingShuffleFallbackReason.NetworkSaturation, ShuffleId)
    assert(manager.streamingFallbackPolicy.shouldDelegateToSortShuffle,
      "the running manager must be delegating before the workload is submitted, or the job would " +
        "not be exercising the degraded path at all")
    val degraded = runRecorded("a streaming shuffle degraded by a network partition") {
      val shuffled = liveWorkload(sc)
      val dependency = shuffled.dependencies.head.asInstanceOf[ShuffleDependency[Int, String, _]]
      assert(!dependency.shuffleHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
        "a shuffle registered while streaming is stood down must carry a sort-based handle but " +
          s"carried a ${dependency.shuffleHandle.getClass.getName}")
      liveDigestOf(shuffled)
    }
    assertNoDataLoss(degraded, baseline,
      "a shuffle delegated to sort-based shuffle while streaming stood down")
  }

  test("memory exhaustion during buffer allocation surfaces as pressure and delegates to sort") {
    val baseline = liveSortBaseline(SpillingDatasetBytes)
    startLiveStreamingApplication(
      "streaming-shuffle-memory-exhaustion",
      streamingConfWithOverrides(
        bufferSizePercent = MinBufferSizePercent,
        spillThreshold = MinSpillThresholdPercent))

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    val context = newTrackedTaskContext()
    val store = track(
      new GrantCappingSpillManager(
        memoryManagerOf(context),
        streamingConfWithOverrides(),
        clock,
        newQuota(RoomyExecutorMemoryBytes)),
      context,
      partitions = 1)

    assert(!store.memoryPressureDetected, "no pressure may have been observed before the cap")

    assert(store.totalBudgetBytes ===
        aggregateBudgetBytes(RoomyExecutorMemoryBytes, DefaultBufferSizePercent),
      s"the allowance must be $DefaultBufferSizePercent per cent of executor memory but was " +
        s"${store.totalBudgetBytes} of $RoomyExecutorMemoryBytes bytes")
    assert(store.perPartitionBudgetBytesFor(PartitionCount) ===
        perPartitionBudgetBytes(RoomyExecutorMemoryBytes, DefaultBufferSizePercent, PartitionCount),
      "the per-partition allowance must be the aggregate divided by the partition count but was " +
        s"${store.perPartitionBudgetBytesFor(PartitionCount)}")
    assert(store.spillThresholdBytes ===
        exactPercentageOf(store.totalBudgetBytes, DefaultSpillThresholdPercent),
      s"the spill trigger must be $DefaultSpillThresholdPercent per cent of the allowance but " +
        s"was ${store.spillThresholdBytes} of ${store.totalBudgetBytes} bytes")

    val cap = BlockChargeBytes - 1L
    injector.limitAllocationTo(cap)
    assert(injector.grantAllocation(BlockChargeBytes) === cap,
      "the injected allocator must grant strictly less than a block costs for this to be the " +
        "partial-grant condition at all")
    store.capGrantsAt(cap)

    assert(!store.bufferBlock(PartitionId, FirstSequence, payload(FirstSequence)),
      "a block that cannot be fully reserved must be refused outright, never half admitted, " +
        "because half a block is not a block a consumer can be served")
    assert(store.lastRequestedBytes === BlockChargeBytes,
      "the store must have asked for a whole block's charge but asked for " +
        s"${store.lastRequestedBytes}")
    assert(store.lastGrantedBytes < store.lastRequestedBytes,
      s"the grant must have been partial for this case to mean anything, but " +
        s"${store.lastGrantedBytes} of ${store.lastRequestedBytes} bytes were granted")

    assert(store.memoryPressureDetected,
      "a partial grant is the memory-pressure trip condition and has to be surfaced, because a " +
        "producer that quietly carried on would be one allocation away from an OutOfMemoryError")
    assert(store.memoryPressureEvents >= 1L,
      "the occurrence must be counted and not only flagged, so a recurring squeeze is visible")

    assert(store.bufferedBytes === 0L, "a refused admission must retain nothing")
    assert(store.executorReservedBytes === 0L,
      "the reservation must be returned to the executor-wide allowance")
    assert(store.getUsed() === 0L, "the partial grant must be handed back to the memory manager")
    assert(store.lastAcceptedSequence(PartitionId) === MemorySpillManager.UNSET_SEQUENCE,
      "a rolled back reservation must leave the partition's sequence run unclaimed")

    val policy = activePolicy(clock)
    policy.recordAllocationGrant(BlockChargeBytes, BlockChargeBytes)
    assert(policy.streamingActive,
      "a grant that was satisfied in full must leave streaming in service")
    policy.recordAllocationGrant(BlockChargeBytes, cap)
    assert(policy.hasTripped, "a short grant must latch the degradation")
    assert(policy.trippedReason.contains(StreamingShuffleFallbackReason.MemoryPressure),
      s"the latched reason must be memory pressure but was ${policy.trippedReason}")
    assert(policy.shouldDelegateToSortShuffle,
      "memory pressure must route to the sort-based delegate rather than fail the task, because " +
        "the point of standing down is to finish the job")

    injector.restoreAllocation()
    store.capGrantsAt(Long.MaxValue)
    store.clearMemoryPressure()
    assert(!store.memoryPressureDetected, "clearing must re-arm the sticky signal")

    val run = runLiveFault(
      MemoryExhaustionOnAllocation,
      baseline,
      new LiveMemoryPressureProbe(),
      datasetBytes = SpillingDatasetBytes)
    assert(run.recorder.diskBytesSpilled > 0L,
      "an allowance too small for the dataset must have driven bytes to local disk and reported " +
        s"them on the existing accumulator, but the run reported " +
        s"${run.recorder.diskBytesSpilled} disk byte(s) against " +
        s"${run.recorder.memoryBytesSpilled} memory byte(s); without spill " +
        "this scenario is not exercising memory pressure at all")
    assert(run.probe.applied.value >= 1L || run.fetchFailuresForShuffle.nonEmpty,
      "the pressure probe must have read a live executor's own allowance, yet none reported " +
        s"doing so and the run recorded no fetch failure against shuffle ${run.shuffleId} " +
        s"either: ${run.probe.describe}")

    val manager = liveStreamingManager()
    driveFallbackReason(
      manager.streamingFallbackPolicy, StreamingShuffleFallbackReason.MemoryPressure, ShuffleId)
    assert(manager.streamingFallbackPolicy.trippedReason
        .contains(StreamingShuffleFallbackReason.MemoryPressure),
      "the running manager must have latched memory pressure before the workload is submitted")
    assert(manager.streamingFallbackPolicy.shouldDelegateToSortShuffle,
      "the running manager must be delegating before the workload is submitted, or the job would " +
        "not be exercising the degraded path at all")
    val degraded = runRecorded("a streaming shuffle delegated to sort under memory pressure") {
      val shuffled = liveWorkload(sc, datasetBytes = SpillingDatasetBytes)
      val dependency = shuffled.dependencies.head.asInstanceOf[ShuffleDependency[Int, String, _]]
      assert(!dependency.shuffleHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
        "a shuffle registered while memory pressure stands streaming down must carry a " +
          s"sort-based handle but carried a ${dependency.shuffleHandle.getClass.getName}")
      liveDigestOf(shuffled)
    }
    assertNoDataLoss(degraded, baseline,
      "a shuffle delegated to sort-based shuffle under memory pressure")
  }

  test("a disk failure during spill reverts, deletes and leaves every block servable") {
    val baseline = liveSortBaseline(SpillingDatasetBytes)
    // The minimum allowance again, because a disk failure during spill needs a spill: at this
    // allowance and this dataset every producer commits files the registry then takes ownership of,
    // and those files are what the live half destroys.
    startLiveStreamingApplication(
      "streaming-shuffle-disk-failure",
      streamingConfWithOverrides(
        bufferSizePercent = MinBufferSizePercent,
        spillThreshold = MinSpillThresholdPercent))

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    val localDirs = sc.env.blockManager.diskBlockManager.localDirs.map(_.getCanonicalPath).toSet

    val writerSeams = classOf[MemorySpillManager].getDeclaredMethods
      .filter(method => method.getName.contains("newSpillWriter"))
    assert(writerSeams.length === 1,
      "the buffer store must obtain its spill writer through exactly one seam, but offers " +
        writerSeams.map(_.toString).mkString(", "))
    val seamParameters = writerSeams.head.getParameterTypes.toSeq
    assert(seamParameters.contains(classOf[TempShuffleBlockId]),
      "the seam must be handed the temporary shuffle block it writes, so the component cannot " +
        s"name a block of its own, but takes ${seamParameters.map(_.getName).mkString(", ")}")
    assert(seamParameters.contains(classOf[File]),
      "the seam must be handed the file it writes, which is what makes a directly constructed " +
        s"DiskBlockObjectWriter impossible, but takes " +
        s"${seamParameters.map(_.getName).mkString(", ")}")

    val healthyContext = newTrackedTaskContext()
    val healthyStore = newBufferStore(
      healthyContext, clock, newQuota(SpillTriggeringExecutorMemoryBytes), partitions = 1)
    bufferWindow(healthyStore, PartitionId, WindowBlockCount)
    assert(healthyStore.bufferUtilizationPercent >= DefaultSpillThresholdPercent.toLong,
      s"the window must reach the $DefaultSpillThresholdPercent percent trigger, but utilisation " +
        s"was ${healthyStore.bufferUtilizationPercent} percent")
    assert(healthyStore.maybeSpill(), "a window at the trigger must be evicted on a healthy device")
    val healthyRecords = healthyStore.allSpilledBlocks
    assert(healthyRecords.nonEmpty, "a successful eviction must publish spill records")
    healthyRecords.foreach { record =>
      assert(localDirs.exists(dir => record.file.getCanonicalPath.startsWith(dir)),
        s"spill file ${record.file.getCanonicalPath} must live under one of the block manager's " +
          s"own local directories ${localDirs.mkString("[", ", ", "]")}, which is the only place " +
          "DiskBlockManager.createTempShuffleBlock allocates")
      assert(record.file.getName.contains(record.blockId.name),
        s"spill file ${record.file.getName} must be named for its temporary shuffle block " +
          s"${record.blockId.name}, so its provenance is legible from the filesystem alone")
    }
    assert(healthyStore.spillFailureCount === 0L, "a healthy device must not fail a spill")
    assert(healthyStore.diskBytesSpilled > 0L,
      "a successful eviction must report its volume on Spark's own disk-spill accounting, or the " +
        "streaming path would be invisible to every existing observability surface")
    assert(DiskWriterRecordUpdateInterval === 16384,
      "the disk writer publishes its byte tally every 16384 records, which is why a spill's " +
        s"accounting is asserted in aggregate rather than per record, but the interval was " +
        s"$DiskWriterRecordUpdateInterval")

    val failingContext = newTrackedTaskContext()
    val committedBeforeFailure = 2
    val blocksToEvict = committedBeforeFailure + 1
    val failingStore = track(
      new FailingDeviceSpillManager(
        memoryManagerOf(failingContext),
        streamingConfWithOverrides(),
        clock,
        newQuota(RoomyExecutorMemoryBytes),
        committedBeforeFailure),
      failingContext,
      partitions = 1)
    val window = bufferWindow(failingStore, PartitionId, blocksToEvict)
    val trigger = new SpillTriggerConsumer(memoryManagerOf(failingContext))
    injector.armIndefinitely(DiskFailureDuringSpill)

    val reclaimed = failingStore.spill(Long.MaxValue, trigger)
    assert(reclaimed === 0L,
      s"a failed eviction must report no reclaimed bytes, but reported $reclaimed; reporting " +
        "bytes  it had not moved would tell the memory manager it had room it does not have")
    assert(failingStore.spillFailureCount === 1L,
      s"exactly one spill failure must be counted, but ${failingStore.spillFailureCount} were")
    assert(failingStore.spillCount === 0L,
      s"a failed eviction must not be counted as a spill, but ${failingStore.spillCount} were")

    val writers = failingStore.openedWriters
    assert(writers.length === 1, s"exactly one writer must have been opened, not ${writers.length}")
    val writer = writers.head
    assert(writer.writeAttempts === committedBeforeFailure + 1,
      s"the device must have failed on write ${committedBeforeFailure + 1} so that the failure " +
        s"is  genuinely part way through, but was asked for ${writer.writeAttempts}")
    assert(writer.wasReverted,
      "the production rollback must revert the partially written file rather than merely close it")
    assert(spillFilesOf(failingStore).isEmpty,
      s"no spill record may survive a failed eviction, but ${spillFilesOf(failingStore).size} did")
    assert(!writer.file.exists(),
      s"the partially written spill file ${writer.file.getAbsolutePath} must be deleted, because " +
        "a  truncated file is one a later lookup could serve a partial block out of")
    assert(failingStore.spillFileDeletionFailures === 0L,
      "the rollback's own deletion must not have failed")
    assert(failingStore.retainedSpillRecordCount === 0,
      s"no spilled record may be published, but ${failingStore.retainedSpillRecordCount} were")

    window.foreach { case (sequenceNumber, expected) =>
      assert(failingStore.retainsBlock(PartitionId, sequenceNumber),
        s"block $sequenceNumber must still be retained after the failed eviction")
      assert(failingStore.spilledBlock(PartitionId, sequenceNumber).isEmpty,
        s"block $sequenceNumber must not be recorded as spilled after the failed eviction")
      val readBack = failingStore.retainedPayload(PartitionId, sequenceNumber)
      assert(readBack.isDefined, s"block $sequenceNumber must still be readable from memory")
      assert(readBack.get.sameElements(expected),
        s"block $sequenceNumber must be restored byte for byte, or the rollback has corrupted it")
    }

    val faultDirectory = withFailingDiskWrites(injector) { fault =>
      assert(fault.directory.exists(),
        "the scoped disk fault must own a directory while it is in force")
      fault.directory
    }
    assert(!faultDirectory.exists(),
      s"the scoped disk fault must remove ${faultDirectory.getAbsolutePath} when its scope ends")
    injector.disarmAll()
    assert(!injector.isArmed(DiskFailureDuringSpill),
      "the disk fault must be disarmed once its scope has ended, so no later case inherits it")

    // And the job, with the device failure ACTIVE while it runs.
    val run = runLiveFault(
      DiskFailureDuringSpill,
      baseline,
      new LiveRetainedFileDamage(corruptRatherThanDelete = false),
      datasetBytes = SpillingDatasetBytes)
    assert(run.recorder.diskBytesSpilled > 0L,
      "files must have reached local disk for a disk failure to destroy, but the run reported " +
        s"${run.recorder.diskBytesSpilled} disk byte(s) of spill")
    assert(run.fetchFailuresForShuffle.nonEmpty,
      s"a retained file removed mid-drain must have surfaced as a fetch failure against shuffle " +
        s"${run.shuffleId}, or the consumers never asked for the bytes that went: the driver saw " +
        s"${run.recorder.fetchFailures.size} fetch failure(s) and none named it")
  }

  test("a checksum mismatch on receive repairs inside the window and escalates outside it") {
    val baseline = liveSortBaseline(SpillingDatasetBytes)
    startLiveStreamingApplication(
      "streaming-shuffle-checksum-mismatch",
      streamingConfWithOverrides(
        bufferSizePercent = MinBufferSizePercent,
        spillThreshold = MinSpillThresholdPercent))

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    val context = newTrackedTaskContext()
    val store = newBufferStore(context, clock, newQuota(RoomyExecutorMemoryBytes), partitions = 1)
    assert(store.registerConsumer(ConsumerId), "the consumer must be admitted before it complains")
    val window = bufferWindow(store, PartitionId, WindowBlockCount)

    // CRC32C, from the JDK, with a fresh stateful instance per call.
    assert(StreamingShuffleChecksum.ALGORITHM === ChecksumAlgorithm,
      s"blocks must be checksummed with $ChecksumAlgorithm but were " +
        StreamingShuffleChecksum.ALGORITHM)
    val samplePayload = payload(FirstSequence)
    assert(StreamingShuffleChecksum.compute(samplePayload) ===
        StreamingShuffleChecksum.compute(samplePayload),
      "CRC32C must be deterministic across calls, which is what a fresh instance per call buys")
    assert(StreamingShuffleChecksum.compute(samplePayload) !==
        StreamingShuffleChecksum.compute(injector.corruptPayload(samplePayload)),
      "a one byte change must change the checksum, or corruption would pass verification")
    assert(StreamingShuffleChecksum.verify(
        samplePayload, StreamingShuffleChecksum.compute(samplePayload)),
      "an intact payload must verify against its own checksum")

    val intact = dataBlock(ShuffleId, MapId, PartitionId, FirstSequence, samplePayload)
    assert(intact.verifyChecksum(), "an intact block must verify")
    val corrupt = corruptedDataBlock(ShuffleId, MapId, PartitionId, FirstSequence, samplePayload)
    assert(!corrupt.verifyChecksum(), "a corrupted block must fail verification")
    val expectedChecksum = corrupt.checksum()
    val computedChecksum = StreamingShuffleChecksum.computeBlock(
      ShuffleId, MapId, PartitionId, FirstSequence, corrupt.copyPayload())
    assert(expectedChecksum !== computedChecksum,
      "the carried and recomputed checksums must differ, otherwise nothing was corrupted")
    assert(computedChecksum === intact.checksum(),
      "the recomputed value must be the value an intact block would have carried, so the " +
        "operator  can see exactly which of the two is wrong")

    val corruptionError = StreamingShuffleErrors.checksumVerificationFailed(
      corruptBlockName, ShuffleId, expectedChecksum, computedChecksum)
    val corruptionMessage = corruptionError.getMessage
    assert(corruptionMessage.contains(expectedChecksum.toString),
      s"the error must report the expected checksum $expectedChecksum but said: $corruptionMessage")
    assert(corruptionMessage.contains(computedChecksum.toString),
      s"the error must report the computed checksum $computedChecksum but said: $corruptionMessage")
    assertConditionParameters(corruptionError,
      s"${StreamingShuffleConditionPrefix}CHECKSUM_VERIFY_FAILED",
      Set("blockId", "shuffleId", "expected", "computed"))

    val lastSequence = window.last._1
    val request = withTaskContext(context) {
      val built = retransmitRequest(ShuffleId, MapId, PartitionId, FirstSequence)
      assert(context.fetchFailed.isEmpty,
        "asking for a retransmission must not report a fetch failure, because the bytes are " +
          "still  held and the upstream stage does not need recomputing")
      built
    }
    assert(request.firstSequenceNumber() === FirstSequence,
      s"the request must name $FirstSequence but named ${request.firstSequenceNumber()}")
    assert(request.lastSequenceNumber() === request.firstSequenceNumber(),
      "a request built from one position states it as an inclusive single-element window")
    assert(request.contains(FirstSequence),
      "the request must contain the position it names")
    assert(request.blockCount() === 1L,
      s"a request asks for exactly one block but asked for ${request.blockCount()}")
    assert(!request.contains(FirstSequence + 1L),
      "and must contain no other position, or one repair would claim to cover another block")

    val windowRequest = retransmitRequest(
      ShuffleId, MapId, PartitionId, window.head._1, window.last._1)
    assert(windowRequest.blockCount() === window.size.toLong,
      s"one request must cover all ${window.size} block(s) of the window but covered " +
        s"${windowRequest.blockCount()}")
    window.foreach { case (sequenceNumber, _) =>
      assert(windowRequest.contains(sequenceNumber),
        s"the one repair request must cover block $sequenceNumber")
    }
    assert(!windowRequest.contains(window.last._1 + 1L),
      "and must cover nothing beyond the window it names")

    assert(DataBlockMessage.MAX_BLOCK_SIZE_BYTES === MaxBlockSizeBytes,
      "the block cap the encoder enforces must be the two mebibytes the feature " +
        "documents but was " +
        DataBlockMessage.MAX_BLOCK_SIZE_BYTES)
    assert(MaxBlockSizeBytes === 2097152,
      s"two mebibytes must be 2097152 bytes but was stated as $MaxBlockSizeBytes")
    val atTheCap = maximumSizedPayload()
    assert(atTheCap.length === MaxBlockSizeBytes,
      s"the maximum payload must be exactly the cap but was ${atTheCap.length} bytes")
    val cappedBlock = dataBlock(ShuffleId, MapId, PartitionId, FirstSequence, atTheCap)
    assert(cappedBlock.verifyChecksum(),
      "a block of exactly the maximum size must be accepted and must verify, because the cap is " +
        "an  inclusive bound")
    val overTheCap = intercept[IllegalArgumentException] {
      dataBlock(ShuffleId, MapId, PartitionId, FirstSequence, oversizedPayload())
    }
    assert(overTheCap.getMessage != null,
      "one byte beyond the cap must be refused with a message that says why")
    assert(RetransmitRequestMessage.MAX_REQUESTED_BLOCKS === MaxRequestedBlocksPerRequest,
      "the widest window a request may name must be the protocol's own figure but was " +
        RetransmitRequestMessage.MAX_REQUESTED_BLOCKS)
    val tooWide = intercept[IllegalArgumentException] {
      retransmitRequest(ShuffleId, MapId, PartitionId, FirstSequence,
        FirstSequence + MaxRequestedBlocksPerRequest)
    }
    assert(tooWide.getMessage != null,
      "a window one block wider than the ceiling must be refused with a message that says why")

    window.foreach { case (sequenceNumber, expected) =>
      assert(windowRequest.contains(sequenceNumber),
        s"block $sequenceNumber must be named by the request that is about to replay it")
      assert(store.retainsBlock(PartitionId, sequenceNumber),
        s"block $sequenceNumber must be retained for the replay to be serviceable")
      val replay = store.retainedPayload(PartitionId, sequenceNumber)
      assert(replay.isDefined, s"block $sequenceNumber must be readable for replay")
      assert(replay.get.sameElements(expected),
        s"the replay of block $sequenceNumber must be byte for byte the original")
    }

    val released = store.acknowledge(ConsumerId, PartitionId, lastSequence)
    assert(released > 0L,
      s"acknowledging through $lastSequence must reclaim the window's bytes but " +
        s"reclaimed $released")
    window.foreach { case (sequenceNumber, _) =>
      assert(!store.retainsBlock(PartitionId, sequenceNumber),
        s"block $sequenceNumber must have been reclaimed by the acknowledgement, which is what " +
          "makes it unserviceable and the escalation necessary")
    }
    assert(store.bufferedBytes === 0L, "an acknowledged window must hold nothing")
    assert(store.lastReclamationDurationMs <= ReclamationDeadlineMillis,
      s"reclamation must complete within $ReclamationDeadlineMillis ms of the acknowledgement " +
        s"but took ${store.lastReclamationDurationMs} ms")
    assert(store.reclamationDeadlineBreaches === 0L,
      s"no reclamation may have breached the deadline but " +
        s"${store.reclamationDeadlineBreaches} did")

    assert(ProtocolVersion === 1,
      s"this build must speak protocol version one but speaks $ProtocolVersion")
    assert(framedLength(intact.encodedLength()) === intact.encodedLength() + FrameTypePrefixLength,
      "a framed message must be its encoded length plus the one byte type prefix but was " +
        s"${framedLength(intact.encodedLength())} for an encoded length of " +
        s"${intact.encodedLength()}")
    assert(intact.encodedLength() === dataBlockEncodedLength(samplePayload.length),
      s"a data block's encoded length must follow the protocol's own arithmetic but was " +
        s"${intact.encodedLength()} for a ${samplePayload.length} byte payload")

    val escalation = withTaskContext(context) {
      intercept[FetchFailedException] {
        discardFramesAndEscalate(Seq(recordingBufferTaken(samplePayload)), Seq.empty, PartitionId)
      }
    }
    assert(context.fetchFailed.contains(escalation),
      "corruption outside the retained window must escalate to the fetch failure that makes the " +
        "unmodified scheduler recompute the upstream stage")

    val run = runLiveFault(
      ChecksumMismatchOnReceive,
      baseline,
      new LiveRetainedFileDamage(corruptRatherThanDelete = true),
      datasetBytes = SpillingDatasetBytes)
    assert(run.recorder.diskBytesSpilled > 0L,
      "files must have reached local disk for their bytes to be corrupted, but the run reported " +
        s"${run.recorder.diskBytesSpilled} disk byte(s) of spill")
    assert(run.observed.nonEmpty,
      "a corrupted block must cost throughput and not correctness, so the run must still have " +
        "produced its output")
  }

  test("a connection timeout during transfer fires at five seconds and not before") {
    val baseline = liveSortBaseline()
    startLiveStreamingApplication("streaming-shuffle-connection-timeout")

    // The constant itself, read from the component that owns it rather than restated, and then
    // pinned to the figure the feature documents.
    assert(ProducerConnectionTimeoutMillis === 5000L,
      "the producer connection timeout must be five seconds but is " +
        ProducerConnectionTimeoutMillis)
    assert(StreamingShuffleClientHandler.PRODUCER_CONNECTION_TIMEOUT_MS ===
        ProducerConnectionTimeoutMillis,
      "the consumer handler must measure the same connection timeout the protocol contracts, or " +
        "the two would disagree about when a producer is gone")
    assert(StreamingShuffleClientHandler.TRANSPORT_MODULE === TransportModuleName,
      "the consumer handler must take its transport tuning from the streaming module " +
        "namespace but took it from " +
        StreamingShuffleClientHandler.TRANSPORT_MODULE)
    assert(TransportModuleName === "shuffle-streaming",
      "the streaming transport module must be named shuffle-streaming but was " +
        TransportModuleName)

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    val notifier = new StreamingShuffleErrorNotifier(ShuffleId, sc.getConf)

    val openedAtMillis = clock.getTimeMillis()
    advanceJustBeforeProducerTimeout(clock)
    assert(clock.getTimeMillis() - openedAtMillis === JustBeforeProducerTimeoutMillis,
      s"the near miss must land on $JustBeforeProducerTimeoutMillis ms exactly")
    assert(JustBeforeProducerTimeoutMillis === ProducerConnectionTimeoutMillis - 1L,
      "the near miss must be one millisecond short of the deadline and not merely close to it")
    assertNoPublishedFailure(notifier, "a transfer one millisecond inside the connection timeout")
    assert(injector.fireCount(ConnectionTimeoutDuringTransfer) === 0,
      "no connection timeout may have been recorded before the deadline")

    // Now the deadline, expressed through the injector so the fault and the clock cannot drift.
    clock.setTime(openedAtMillis)
    injector.expireProducerConnection()
    assert(clock.getTimeMillis() - openedAtMillis === ProducerConnectionTimeoutMillis,
      "the injected timeout must advance the clock by exactly the contracted deadline")
    assert(injector.fireCount(ConnectionTimeoutDuringTransfer) === 1,
      "the timeout must be recorded once but was recorded " +
        s"${injector.fireCount(ConnectionTimeoutDuringTransfer)} times")

    val invalidationsBefore = observedPartialReadInvalidations()
    val fixture = new ProducerLossFixture()
    val failure = withProducerLoss(fixture) { stalled =>
      val blocks = stalled.blocksOf(FixtureFirstMapId)
      val streamed = stalled.reader.read()
      val channel = stalled.streamOf(FixtureFirstMapIndex)
      channel.deliver(blocks.head)
      assert(channel.handler.millisSinceInbound(stalled.reducePartitionId).contains(0L),
        "the production handler must have recorded the instant of arrival from its own clock")

      advanceJustBeforeProducerTimeout(stalled.clock)
      assert(!channel.handler.isProducerSilent(stalled.reducePartitionId),
        s"a producer silent for $JustBeforeProducerTimeoutMillis ms must NOT be judged lost, " +
          "because a timer that fires early is as wrong as one that never fires")
      stalled.clock.advance(1L)
      assert(channel.handler.isProducerSilent(stalled.reducePartitionId),
        s"and at exactly $ProducerConnectionTimeoutMillis ms it must be")

      withTaskContext(stalled.context) {
        intercept[FetchFailedException] {
          stalled.readAll(streamed)
        }
      }
    }
    assert(observedPartialReadInvalidations() === invalidationsBefore + 1L,
      "a transfer abandoned on the connection timeout must invalidate its partial read exactly " +
        s"once, but the counter moved from $invalidationsBefore to " +
        s"${observedPartialReadInvalidations()}")
    assert(fixture.coordinatorRef.invalidationsSent.size === 1,
      "and must retire exactly one producer generation, but " +
        s"${fixture.coordinatorRef.invalidationsSent.size} were retired")
    val reason = fetchFailedReasonOf(failure)
    assert(reason.shuffleId === fixture.shuffleId && reason.reduceId === fixture.reducePartitionId,
      s"the fetch failure must name the shuffle and reduce partition being read, but named " +
        s"shuffle ${reason.shuffleId} partition ${reason.reduceId}")
    assert(reason.bmAddress === fixture.producers.head.blockManagerId,
      "and must carry the producer's own MapStatus address, without which MapOutputTracker " +
        "removes " +
        s"nothing and the recomputation never happens, but it carried ${reason.bmAddress}")
    assert(fixture.context.fetchFailed.contains(failure),
      "a timed out transfer must reach the scheduler through the exception it recognises")
    notifier.setError(failure)
    assertPublishedFailure[FetchFailedException](notifier, "a transfer that timed out")

    // And the job, with the transfer really interrupted while it runs: the links this executor
    // serves are closed and held closed past the five-second bound, so the reader's own liveness
    // timer is what fires rather than an injected clock.
    val run = runLiveFault(
      ConnectionTimeoutDuringTransfer, baseline, new LiveLinkPartition(LivePartitionHoldMillis))
    assert(run.fetchFailuresForShuffle.nonEmpty,
      s"a transfer held unusable past the bound must have timed out into a fetch failure against " +
        s"shuffle ${run.shuffleId}, but the driver saw ${run.recorder.fetchFailures.size} fetch " +
        "failure(s) and none named it")
    assert(run.recorder.maxStageSubmissions >= 2,
      "a timeout that recovered must have recomputed, so a stage must have been submitted more " +
        s"than once, but the most any was submitted is ${run.recorder.maxStageSubmissions}")
  }

  test("an executor JVM GC pause does not trip liveness detection and recovery is clean") {
    val baseline = liveSortBaseline()
    startLiveStreamingApplication("streaming-shuffle-gc-pause")

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    val notifier = new StreamingShuffleErrorNotifier(ShuffleId, sc.getConf)
    val policy = activePolicy(clock)
    val context = newTrackedTaskContext()
    val store = newBufferStore(
      context, clock, newQuota(SpillTriggeringExecutorMemoryBytes), partitions = 1)

    // A stalled JVM and a stalled clock are indistinguishable to anything that reads time through
    // the injected clock, which every streaming component does.
    val firstWindow = bufferWindow(store, PartitionId, WindowBlockCount)
    assert(store.bufferUtilizationPercent >= DefaultSpillThresholdPercent.toLong,
      s"the window must reach the $DefaultSpillThresholdPercent percent trigger, but utilisation " +
        s"was ${store.bufferUtilizationPercent} percent")
    assert(store.pollOnce(),
      "the first threshold poll must be due and must find the window above the trigger")
    assert(store.spillCount === 1L, s"one eviction must have happened, not ${store.spillCount}")

    val secondWindow = bufferWindow(
      store, PartitionId, WindowBlockCount, firstSequence = FirstSequence + WindowBlockCount.toLong)
    clock.advance(SpillPollIntervalMillis - 1L)
    assert(!store.pollOnce(),
      s"a poll ${SpillPollIntervalMillis - 1L} ms after the last one is not due, because the " +
        s"cadence is $SpillPollIntervalMillis ms and not 'as often as asked'")
    assert(store.spillCount === 1L,
      s"a poll that was not due must evict nothing, but ${store.spillCount} evictions had happened")
    clock.advance(1L)
    assert(store.pollOnce(),
      s"a poll exactly $SpillPollIntervalMillis ms after the last one is due")
    assert(store.spillCount === 2L,
      s"a second eviction must have happened, not ${store.spillCount}")

    assert(ShortPauseMillis < ProducerConnectionTimeoutMillis,
      s"the fixture's $ShortPauseMillis ms pause must be inside the " +
        s"$ProducerConnectionTimeoutMillis ms producer window for the false-positive claim to " +
        "mean  anything")
    assert(ShortPauseMillis < ConsumerLivenessTimeoutMillis,
      s"the fixture's $ShortPauseMillis ms pause must be inside the " +
        s"$ConsumerLivenessTimeoutMillis ms consumer window as well")
    val pausedAtMillis = clock.getTimeMillis()
    injector.simulateGarbageCollectionPause(ShortPauseMillis)
    assert(clock.getTimeMillis() - pausedAtMillis === ShortPauseMillis,
      "the simulated pause must advance the clock by exactly the stall it names, and never sleep")
    assert(injector.fireCount(ExecutorGarbageCollectionPause) === 1,
      s"the pause must be recorded once but was recorded " +
        s"${injector.fireCount(ExecutorGarbageCollectionPause)} times")
    assertNoPublishedFailure(notifier, "a garbage collection pause inside both liveness windows")
    assert(policy.streamingActive,
      "a pause shorter than either liveness window must not stand streaming down, because a " +
        "paused  executor is a slow executor and not a lost one")

    (firstWindow ++ secondWindow).foreach { case (sequenceNumber, expected) =>
      assert(store.retainsBlock(PartitionId, sequenceNumber),
        s"block $sequenceNumber must survive the pause; a stall is not a loss")
      val readBack = store.retainedPayload(PartitionId, sequenceNumber)
      assert(readBack.isDefined, s"block $sequenceNumber must still be readable after the pause")
      assert(readBack.get.sameElements(expected),
        s"block $sequenceNumber must be unchanged by the pause, byte for byte")
    }
    val afterPause = bufferWindow(
      store, PartitionId, 1, firstSequence = FirstSequence + 2L * WindowBlockCount.toLong)
    assert(afterPause.length === 1,
      "the store must admit new blocks once the pause is over, which is what clean recovery means")
    assert(store.spillFailureCount === 0L,
      s"the pause must not have failed an eviction, but ${store.spillFailureCount} failed")

    val breachedFrom = clock.getTimeMillis()
    injector.simulateGarbageCollectionPause(ConsumerLivenessTimeoutMillis)
    assert(clock.getTimeMillis() - breachedFrom >= ConsumerLivenessTimeoutMillis,
      "a pause of a whole liveness window must reach the deadline the watchdog measures")
    assert(injector.fireCount(ExecutorGarbageCollectionPause) === 2,
      "both pauses must be recorded, so a run's stall history is legible")

    val run = runLiveFault(
      ExecutorGarbageCollectionPause, baseline, new LiveConsumerStall(BriefStallMillis))
    assert(run.probe.applied.value >= 1L,
      "a consumer must actually have paused, or the false-positive claim is untested: " +
        run.probe.describe)
    assert(run.probe.effect.value >= 1L,
      "the pause must have RUN TO COMPLETION on an executor with the streaming subsystem in " +
        "service -- the effect is reported after the wait returns, so a zero here would mean the " +
        "assertion below is testing an absence of pause rather than a pause: " + run.probe.describe)
    assert(run.fetchFailuresForShuffle.isEmpty,
      s"a pause of $BriefStallMillis ms is inside the $ProducerConnectionTimeoutMillis ms " +
        "bound and must not be read as a lost producer, yet the driver saw " +
        s"${run.fetchFailuresForShuffle.size} fetch failure(s) against shuffle ${run.shuffleId}")
  }

  test("multiple concurrent producer failures invalidate atomically and independently") {
    val baseline = liveSortBaseline()
    startLiveStreamingApplication("streaming-shuffle-concurrent-producer-failures")

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    val context = newTrackedTaskContext()

    val producers = (0 until ConcurrentProducerCount).map { producerIndex =>
      val frames = (0 until WindowBlockCount).map { blockIndex =>
        recordingBufferTaken(payload(producerIndex.toLong * 100L + blockIndex.toLong))
      }
      val notifier = new StreamingShuffleErrorNotifier(ShuffleId + producerIndex, sc.getConf)
      producerIndex -> (frames, notifier)
    }.toMap

    injector.crashProducersConcurrently(ConcurrentProducerCount)
    assert(injector.isArmed(ConcurrentProducerFailures),
      "the concurrent failure must be armed before any producer is invalidated")

    // Producer by producer, each invalidation atomic over its own frames and inert over everyone
    // else's.
    val invalidationsBefore = observedPartialReadInvalidations()
    var invalidated = 0
    producers.keys.toSeq.sorted.foreach { producerIndex =>
      val (frames, notifier) = producers(producerIndex)
      val survivors = producers.filter(entry => entry._1 != producerIndex)
        .values.flatMap(entry => entry._1).toSeq
      val stillHeld = producers.filter(entry => entry._1 > producerIndex)
        .values.flatMap(entry => entry._1).toSeq

      val failure = withTaskContext(context) {
        intercept[FetchFailedException] {
          discardFramesAndEscalate(frames, survivors, PartitionId + producerIndex,
            ShuffleId + producerIndex)
        }
      }
      invalidated += 1
      assert(injector.shouldFail(ConcurrentProducerFailures),
        s"producer $producerIndex must have an arming budget of its own, since all " +
          s"$ConcurrentProducerCount fail together")

      assertBuffersReleasedExactlyOnce(frames)
      assert(stillHeld.forall(frame => frame.outstandingReferences == 1),
        s"invalidating producer $producerIndex must leave every later producer's frames held; " +
          s"outstanding references ${stillHeld.map(_.outstandingReferences).mkString(", ")}")
      assert(observedPartialReadInvalidations() === invalidationsBefore + invalidated.toLong,
        s"each producer's invalidation must be counted once; after $invalidated the counter " +
          s"reads  ${observedPartialReadInvalidations()} rather than " +
          s"${invalidationsBefore + invalidated.toLong}")

      notifier.setError(failure)
      assert(notifier.fetchFailure.contains(failure),
        s"producer $producerIndex must publish its own failure on its own notifier, so one lost " +
          "producer cannot be reported as another")
    }

    assert(invalidated === ConcurrentProducerCount,
      s"all $ConcurrentProducerCount producers must have been invalidated but $invalidated were")
    val allFrames = producers.values.flatMap(entry => entry._1).toSeq
    assert(allFrames.forall(frame => frame.outstandingReferences == 0),
      "once every producer has been invalidated nothing may still be held, or the reduce task " +
        "has  leaked a frame it can never serve")
    val distinctFailures = producers.values.flatMap(entry => entry._2.fetchFailure).toSeq.distinct
    assert(distinctFailures.length === ConcurrentProducerCount,
      s"each producer must have published a distinct failure but ${distinctFailures.length} were " +
        s"distinct out of $ConcurrentProducerCount")

    // <b>Independence, proven on PRODUCTION's invalidation.</b> One consumer reads three producer
    // generations, each of which has delivered blocks, and one of them is taken away.
    Seq(FixtureFirstMapIndex, ConcurrentProducerCount - 1).foreach { lostMapIndex =>
      val invalidationsBefore = observedPartialReadInvalidations()
      val fixture = new ProducerLossFixture(numMaps = ConcurrentProducerCount)
      val failure = withProducerLoss(fixture) { losing =>
        val streamed = losing.reader.read()
        losing.producers.foreach { producer =>
          val channel = losing.streamOf(producer.mapIndex)
          val blocks = losing.blocksOf(producer.mapId)
          if (producer.mapIndex < lostMapIndex) {
            blocks.foreach(block => channel.deliver(block))
            channel.deliver(losing.terminatorOf(producer.mapId, blocks.size.toLong))
          } else {
            channel.deliver(blocks.head)
          }
        }
        val ledgersBefore = losing.producers.map { producer =>
          producer.mapId -> losing.creditLedgersOf(producer.mapId).size
        }.toMap
        assert(ledgersBefore.values.forall(_ >= 1),
          "every producer must hold a credit ledger of its own before one of them is lost, but " +
            s"the ledgers were $ledgersBefore")
        val acceptedBefore = losing.producers.map { producer =>
          producer.mapIndex ->
            losing.streamOf(producer.mapIndex).handler.acceptedBlockCount(
              losing.reducePartitionId)
        }.toMap
        assert(acceptedBefore.values.forall(_ >= 1L),
          s"every producer must have had a block accepted before one is lost, but the accepted " +
            s"counts were $acceptedBefore")

        losing.loseProducer(lostMapIndex)
        val raised = withTaskContext(losing.context) {
          intercept[FetchFailedException] {
            losing.readAll(streamed)
          }
        }

        // What the survivors kept, which is the decisive half of "atomic AND per producer".
        losing.producers.filter(_.mapIndex != lostMapIndex).foreach { survivor =>
          assert(losing.creditLedgersOf(survivor.mapId).size === ledgersBefore(survivor.mapId),
            s"map ${survivor.mapId} was healthy and its allowance must be exactly as it was, but " +
              s"it now holds ${losing.creditLedgersOf(survivor.mapId).size} ledger(s) against " +
              s"${ledgersBefore(survivor.mapId)}")
          val stillAccepted = losing.streamOf(survivor.mapIndex).handler.acceptedBlockCount(
            losing.reducePartitionId)
          assert(stillAccepted === acceptedBefore(survivor.mapIndex),
            s"map ${survivor.mapId} was healthy and its accepted blocks must be exactly as they " +
              s"were, but its handler now counts $stillAccepted against " +
              s"${acceptedBefore(survivor.mapIndex)}; a reduce task that discarded a healthy " +
              "producer's bytes would be losing output it is entitled to keep")
        }
        assert(observedPartialReadInvalidations() === invalidationsBefore + 1L,
          s"losing one of $ConcurrentProducerCount producers must count exactly one " +
          s"invalidation, " +
            s"but the counter moved from $invalidationsBefore to " +
            s"${observedPartialReadInvalidations()}")
        assert(losing.coordinatorRef.invalidationsSent.size === 1,
          s"and must retire exactly one generation, but " +
            s"${losing.coordinatorRef.invalidationsSent.size} were retired")
        assert(losing.coordinatorRef.invalidationsSent.head.generation ===
            losing.producers(lostMapIndex).generation,
          "and the one retired must be the generation that was lost, but " +
            s"${losing.coordinatorRef.invalidationsSent.head.generation} was named")
        raised
      }
      val reason = fetchFailedReasonOf(failure)
      assert(reason.mapIndex === lostMapIndex,
        s"the fetch failure must name the lost map output so the tracker removes that one and no " +
          s"other, but it named index ${reason.mapIndex}")
      assert(reason.bmAddress === fixture.producers(lostMapIndex).blockManagerId,
        "and must carry that producer's own MapStatus address, or the tracker removes nothing")
    }

    val run = runLiveFault(
      ConcurrentProducerFailures,
      baseline,
      new LiveProducerLoss(),
      failingMapPartitions = ConcurrentFailingMapPartitions,
      chainedShuffles = true)
    val doomedPartitions = ConcurrentFailingMapPartitions.filter { partition =>
      run.injectedMapFailures.exists(_.toErrorString.contains(s"map partition $partition"))
    }
    assert(doomedPartitions === ConcurrentFailingMapPartitions,
      s"every one of the ${ConcurrentFailingMapPartitions.size} selected producers must have " +
        s"died part way along its write, but only $doomedPartitions did, out of " +
        s"${run.recorder.countedFailures.size} task failure(s) the driver saw")
    assert(run.fetchFailuresForShuffle.nonEmpty,
      s"withdrawing an executor's producers must have raised a fetch failure against shuffle " +
        s"${run.shuffleId}, but the driver saw ${run.recorder.fetchFailures.size} fetch " +
        "failure(s) and none named it")
  }

  test("a consumer reconnect after extended downtime replays or escalates cleanly") {
    val baseline = liveSortBaseline()
    startLiveStreamingApplication("streaming-shuffle-consumer-reconnect")

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    val context = newTrackedTaskContext()
    val store = newBufferStore(context, clock, newQuota(RoomyExecutorMemoryBytes), partitions = 1)
    assert(store.registerConsumer(ConsumerId), "the consumer must be admitted before it leaves")
    val window = bufferWindow(store, PartitionId, WindowBlockCount)
    val lastSequence = window.last._1

    assert(ExtendedDowntimeMillis > ConsumerLivenessTimeoutMillis,
      s"the downtime must outlast the $ConsumerLivenessTimeoutMillis ms liveness window, or the " +
        "producer would never have noticed the consumer was away")

    val reconnected = newLatch(1)
    val observations = runConcurrently(2, "streaming-shuffle-reconnect") { threadIndex =>
      if (threadIndex == 0) {
        injector.reconnectConsumerAfterDowntime(ExtendedDowntimeMillis)
        reconnected.countDown()
        store.registeredConsumers.size
      } else {
        awaitLatch(reconnected, "the consumer's reconnection")
        store.retainedBlockCount(PartitionId)
      }
    }
    assert(observations.head >= 1,
      "the returning consumer must still be registered, because retirement advances only as far " +
        "as  the slowest registered consumer and an absent one holds the window open")
    assert(observations(1) === WindowBlockCount,
      s"the producer must still hold all $WindowBlockCount blocks when the consumer returns, but " +
        s"held ${observations(1)}")
    assert(!injector.networkPartitioned,
      "reachability must have been restored, otherwise this is a partition and not a reconnection")
    assert(injector.fireCount(ConsumerReconnectAfterDowntime) === 1,
      s"the reconnection must be recorded once but was recorded " +
        s"${injector.fireCount(ConsumerReconnectAfterDowntime)} times")

    // The manual clock is itself a rendezvous: a thread parked in `waitTillTime` is released by the
    // advance rather than by a sleep.
    val reading = awaitClockReading(clock, clock.getTimeMillis())
    assert(reading >= ManualClockEpochMillis + ExtendedDowntimeMillis,
      s"the clock must have advanced through the whole downtime but reads $reading")

    val replayPool = ThreadUtils.newDaemonSingleThreadExecutor("streaming-shuffle-replay")
    val replayed = try {
      awaitJavaFuture(replayPool.submit(new Callable[Seq[(Long, Array[Byte])]] {
        override def call(): Seq[(Long, Array[Byte])] = window.map { case (sequenceNumber, _) =>
          sequenceNumber -> store.retainedPayload(PartitionId, sequenceNumber).getOrElse(
            Array.emptyByteArray)
        }
      }))
    } finally {
      replayPool.shutdownNow()
    }
    assert(replayed.length === WindowBlockCount,
      s"the replay must cover all $WindowBlockCount blocks but covered ${replayed.length}")
    replayed.foreach { case (sequenceNumber, bytes) =>
      assert(bytes.nonEmpty,
        s"block $sequenceNumber must have been replayable from the retained window, but the " +
          "producer had nothing to send")
    }

    val replayRequests = window.map { case (sequenceNumber, _) =>
      sequenceNumber -> retransmitRequest(ShuffleId, MapId, PartitionId, sequenceNumber)
    }
    assert(replayRequests.length === WindowBlockCount,
      s"the replay must cover all $WindowBlockCount blocks but covered ${replayRequests.length}")
    assert(replayRequests.map(_._2.blockCount()).sum === WindowBlockCount.toLong,
      "and must ask for exactly one block per request, so the whole retained window is covered " +
        "once")
    assert(store.lowestRetainedSequence(PartitionId) === FirstSequence,
      "the retained window must still start where the consumer left off, or the replay would " +
        "begin  past records the consumer never received")
    assert(store.lastAcceptedSequence(PartitionId) === lastSequence,
      "the retained window must still end where the producer left off")
    val replayedPositions = replayRequests.map(_._2).map(_.sequenceNumber()).toSet
    window.foreach { case (sequenceNumber, expected) =>
      assert(replayedPositions.contains(sequenceNumber),
        s"block $sequenceNumber must lie inside the replay the returning consumer asks for")
      val replay = store.retainedPayload(PartitionId, sequenceNumber)
      assert(replay.isDefined,
        s"block $sequenceNumber must be replayable from memory or spill after the downtime")
      assert(replay.get.sameElements(expected),
        s"the replay of block $sequenceNumber must be byte for byte the original")
    }

    val released = store.acknowledge(ConsumerId, PartitionId, lastSequence)
    assert(released > 0L,
      s"acknowledging through $lastSequence must reclaim the window but reclaimed $released")
    assert(store.bufferedBytes === 0L, "a fully acknowledged window must hold nothing")
    assert(store.lowestRetainedSequence(PartitionId) === MemorySpillManager.UNSET_SEQUENCE,
      "a reclaimed window must retain no sequence at all, which is what makes a later replay " +
        "unserviceable rather than partially serviceable")
    val invalidationsBefore = observedPartialReadInvalidations()
    val fixture = new ProducerLossFixture()
    val escalation = withProducerLoss(fixture) { returning =>
      val blocks = returning.blocksOf(FixtureFirstMapId)
      val streamed = returning.reader.read()
      val channel = returning.streamOf(FixtureFirstMapIndex)
      channel.deliver(blocks.head)
      returning.loseProducer(FixtureFirstMapIndex)
      withTaskContext(returning.context) {
        intercept[FetchFailedException] {
          returning.readAll(streamed)
        }
      }
    }
    assert(fixture.context.fetchFailed.contains(escalation),
      "a replay of a reclaimed window must escalate to the fetch failure the scheduler " +
        "recognises  rather than serve bytes the producer no longer holds")
    assert(observedPartialReadInvalidations() === invalidationsBefore + 1L,
      "and the escalation must count exactly one invalidation, but the counter moved from " +
        s"$invalidationsBefore to ${observedPartialReadInvalidations()}")
    assert(fixture.coordinatorRef.invalidationsSent.size === 1,
      "and retire exactly one generation, but " +
        s"${fixture.coordinatorRef.invalidationsSent.size} were retired")

    val run = runLiveFault(
      ConsumerReconnectAfterDowntime, baseline, new LiveConsumerStall(ExtendedStallMillis))
    assert(run.probe.applied.value >= 1L,
      s"a consumer must actually have been absent for $ExtendedStallMillis ms, or its return is " +
        "untested: " + run.probe.describe)
    assert(run.probe.effect.value >= 1L,
      "the absent consumer must have been on an executor with the streaming subsystem in " +
        "service, or it was absent from a sort-based read: " + run.probe.describe)
  }

  test("this suite covers exactly the ten enumerated failure injection scenarios") {
    val scenarioPhrases: Map[StreamingShuffleFaultScenario, String] = Map(
      ProducerCrashDuringWrite -> "producer crash during write",
      ConsumerCrashDuringRead -> "consumer crash during read",
      NetworkPartition -> "network partition",
      MemoryExhaustionOnAllocation -> "memory exhaustion during buffer allocation",
      DiskFailureDuringSpill -> "disk failure during spill",
      ChecksumMismatchOnReceive -> "checksum mismatch on receive",
      ConnectionTimeoutDuringTransfer -> "connection timeout during transfer",
      ExecutorGarbageCollectionPause -> "executor JVM GC pause",
      ConcurrentProducerFailures -> "multiple concurrent producer failures",
      ConsumerReconnectAfterDowntime -> "consumer reconnect after extended downtime")

    assert(StreamingShuffleFaultScenario.all.length === ExpectedScenarioCount,
      s"the feature enumerates $ExpectedScenarioCount failure scenarios but the closed set holds " +
        s"${StreamingShuffleFaultScenario.all.length}")
    assert(StreamingShuffleFaultScenario.all.map(_.faultName).distinct.length ===
        ExpectedScenarioCount,
      "every scenario must carry a distinct name, or two of them could be mistaken for each other")
    assert(scenarioPhrases.keySet === StreamingShuffleFaultScenario.all.toSet,
      "this suite's coverage map must name exactly the closed scenario set; missing " +
        s"${StreamingShuffleFaultScenario.all.toSet.diff(scenarioPhrases.keySet)}")

    val registered = testNames.map(name => name.toLowerCase(Locale.ROOT))
    StreamingShuffleFaultScenario.all.foreach { scenario =>
      val phrase = scenarioPhrases(scenario).toLowerCase(Locale.ROOT)
      assert(registered.exists(name => name.contains(phrase)),
        s"${scenario.faultName} must be covered by a test naming '$phrase', but no registered " +
          s"test  does: ${testNames.toSeq.sorted.mkString("[", "; ", "]")}")
    }
  }

  test("exactly three error conditions are authorized and each carries its declared placeholders") {
    val catalogued = SparkThrowableHelper.errorReader.errorInfoMap.keys
      .filter(name => name.startsWith(StreamingShuffleConditionPrefix)).toSet
    assert(catalogued === AuthorizedErrorConditions,
      "the catalogue must hold exactly the authorized streaming shuffle conditions " +
        s"${AuthorizedErrorConditions.toSeq.sorted.mkString("[", ", ", "]")} but held " +
        catalogued.toSeq.sorted.mkString("[", ", ", "]"))
    assert(catalogued.size === ExpectedErrorConditionCount,
      s"exactly $ExpectedErrorConditionCount conditions are authorized, never a fourth, but " +
        s"${catalogued.size} were catalogued")

    assertConditionParameters(
      StreamingShuffleErrors.checksumVerificationFailed(corruptBlockName, ShuffleId, 1L, 2L),
      s"${StreamingShuffleConditionPrefix}CHECKSUM_VERIFY_FAILED",
      Set("blockId", "shuffleId", "expected", "computed"))
    assertConditionParameters(
      StreamingShuffleErrors.invalidSequenceNumber(ShuffleId, PartitionId, FirstSequence, 7L),
      s"${StreamingShuffleConditionPrefix}INVALID_SEQUENCE_NUMBER",
      Set("shuffleId", "partitionId", "expected", "actual"))
    assertConditionParameters(
      StreamingShuffleErrors.unexpectedMessageType("DATA_BLOCK", "ACK"),
      s"${StreamingShuffleConditionPrefix}UNEXPECTED_MESSAGE_TYPE",
      Set("expected", "actual"))
  }

  test("exactly four streaming shuffle metrics are published and this suite reads them") {
    assert(MetricNames.length === ExpectedMetricCount,
      s"exactly $ExpectedMetricCount metrics are specified but ${MetricNames.length} are named")
    assert(streamingShuffleMetricNames() === MetricNames.toSet,
      s"the source must publish exactly ${MetricNames.sorted.mkString("[", ", ", "]")} but " +
        streamingShuffleMetricNames().toSeq.sorted.mkString("[", ", ", "]"))
    assert(MetricNames.contains(PartialReadInvalidationsMetricName),
      "the invalidation counter this suite asserts on must be one of the published metrics")
    assert(MetricNames.contains(SpillCountMetricName),
      "the spill counter this suite asserts on must be one of the published metrics")

    resetStreamingShuffleMetrics()
    assert(observedPartialReadInvalidations() === 0L,
      s"the invalidation counter must reset to zero but read " +
        s"${observedPartialReadInvalidations()}")
    assert(observedSpillCount() === 0L,
      s"the spill counter must reset to zero but read ${observedSpillCount()}")
    assert(observedBackpressureEvents() === 0L,
      s"the backpressure counter must reset to zero but read ${observedBackpressureEvents()}")
    assert(observedBufferUtilizationPercent() === 0L,
      "with no contributor registered the utilisation gauge must read zero but read " +
        s"${observedBufferUtilizationPercent()}")
  }

  private val ExpectedScenarioCount: Int = 10

  private val ExpectedErrorConditionCount: Int = 3

  private val ExpectedMetricCount: Int = 4

  private def corruptBlockName: String = s"shuffle_${ShuffleId}_${MapId}_$PartitionId"

  private def assertConditionParameters(
      error: Throwable,
      condition: String,
      expectedPlaceholders: Set[String]): Unit = {
    val throwable = error match {
      case sparkThrowable: SparkThrowable => sparkThrowable
      case other => fail(
        s"$condition must be raised as a SparkThrowable but was a ${other.getClass.getName}")
    }
    assert(throwable.getCondition === condition,
      s"the raised condition must be $condition but was ${throwable.getCondition}")
    assert(throwable.getSqlState === StreamingShuffleSqlState,
      s"$condition must carry SQLSTATE $StreamingShuffleSqlState but carried " +
        throwable.getSqlState)
    val supplied = throwable.getMessageParameters.asScala.keySet.toSet
    assert(supplied === expectedPlaceholders,
      s"$condition must be raised with exactly " +
        s"${expectedPlaceholders.toSeq.sorted.mkString("[", ", ", "]")} but was raised with " +
        supplied.toSeq.sorted.mkString("[", ", ", "]"))
    val declared = SparkThrowableHelper.getMessageParameters(condition).toSet
    assert(declared === expectedPlaceholders,
      s"the catalogue template for $condition must declare exactly " +
        s"${expectedPlaceholders.toSeq.sorted.mkString("[", ", ", "]")} but declares " +
        declared.toSeq.sorted.mkString("[", ", ", "]"))
    assert(!error.getMessage.contains("<"),
      "every placeholder must have been substituted, but the message still reads: " +
        error.getMessage)
    ()
  }
}

/** A fault a reduce-side closure can apply to the executor it is running on, mid-drain. */
private sealed trait LiveStreamingFault extends Serializable {

  def applyTo(manager: StreamingShuffleManager): Int

  def bindShuffle(shuffleId: Int): Unit = ()
}

/** Withdraws one shuffle's retained producer output from this executor's registry. */
private class LiveProducerLoss extends LiveStreamingFault {

  @volatile var shuffleId: Int = LiveStreamingFault.NO_SHUFFLE

  override def bindShuffle(boundShuffleId: Int): Unit = {
    shuffleId = boundShuffleId
  }

  override def applyTo(manager: StreamingShuffleManager): Int = {
    if (shuffleId < 0) 0
    else LiveStreamingFault.resolverOf(manager).map(_.removeShuffle(shuffleId)).getOrElse(0)
  }
}

/** Kills the consuming task itself, part way through its drain. */
private class LiveConsumerLoss extends LiveStreamingFault {

  override def applyTo(manager: StreamingShuffleManager): Int = {
    throw new IllegalStateException(
      s"${LiveStreamingFault.CONSUMER_LOSS_MARKER} on executor ${SparkEnv.get.executorId}")
  }
}

/** Breaks every link this executor serves, and keeps them broken for [[holdMillis]]. */
private class LiveLinkPartition(val holdMillis: Long) extends LiveStreamingFault {

  override def applyTo(manager: StreamingShuffleManager): Int = {
    manager.boundStreamingListener.map { listener =>
      val closed = closeUntilSomethingCloses(listener)
      holdOpen(listener)
      closed
    }.getOrElse(0)
  }

  private def closeUntilSomethingCloses(listener: StreamingShuffleListener): Int = {
    val idle = new CountDownLatch(1)
    var closed = 0
    var attempts = 0
    while (closed == 0 && attempts < LiveStreamingFault.CLOSE_ATTEMPTS) {
      closed += listener.faultEveryServedChannel(LiveStreamingFault.PARTITION_REASON)
      if (closed == 0) {
        attempts += 1
        idle.await(LiveStreamingFault.SWEEP_MILLIS, TimeUnit.MILLISECONDS)
      }
    }
    closed
  }

  private def holdOpen(listener: StreamingShuffleListener): Unit = {
    val sweeper = new Thread(() => {
      val idle = new CountDownLatch(1)
      val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(holdMillis)
      while (System.nanoTime() < deadline) {
        idle.await(LiveStreamingFault.SWEEP_MILLIS, TimeUnit.MILLISECONDS)
        listener.faultEveryServedChannel(LiveStreamingFault.PARTITION_REASON)
      }
    }, "streaming-shuffle-live-partition-sweeper")
    sweeper.setDaemon(true)
    sweeper.start()
  }
}

/** Damages the retained spill files this executor would serve for one shuffle. */
private class LiveRetainedFileDamage(val corruptRatherThanDelete: Boolean)
  extends LiveStreamingFault {

  @volatile var shuffleId: Int = LiveStreamingFault.NO_SHUFFLE

  override def bindShuffle(boundShuffleId: Int): Unit = {
    shuffleId = boundShuffleId
  }

  override def applyTo(manager: StreamingShuffleManager): Int = {
    if (shuffleId < 0) {
      0
    } else {
      val files = LiveStreamingFault.resolverOf(manager)
        .map(_.retainedFilesOf(shuffleId))
        .getOrElse(Seq.empty)
      files.count(damage)
    }
  }

  private def damage(file: File): Boolean = {
    try {
      if (!file.isFile || file.length() == 0L) {
        false
      } else if (corruptRatherThanDelete) {
        val channel = new RandomAccessFile(file, "rw")
        try {
          val lastByte = channel.length() - 1L
          channel.seek(lastByte)
          val original = channel.readByte()
          channel.seek(lastByte)
          channel.writeByte(original ^ 0xFF)
          true
        } finally {
          channel.close()
        }
      } else {
        file.delete()
      }
    } catch {
      case _: IOException => false
    }
  }
}

/** Stalls the consumer for [[stallMillis]] without consuming, then lets it resume. */
private class LiveConsumerStall(val stallMillis: Long) extends LiveStreamingFault {

  override def applyTo(manager: StreamingShuffleManager): Int = {
    val idle = new CountDownLatch(1)
    idle.await(stallMillis, TimeUnit.MILLISECONDS)
    if (manager.boundStreamingListener.isDefined) 1 else 0
  }
}

/** Reads this executor's buffer allowance and reports how many reservations it has refused. */
private class LiveMemoryPressureProbe extends LiveStreamingFault {

  override def applyTo(manager: StreamingShuffleManager): Int = {
    val quota = MemorySpillManager.executorQuota(SparkEnv.get.conf)
    math.min(Int.MaxValue.toLong, quota.refusalCount).toInt
  }
}

private object LiveStreamingFault {

  val NO_SHUFFLE: Int = -1

  val PARTITION_REASON: String = "a failure-injection scenario partitioned every streaming link"

  val CONSUMER_LOSS_MARKER: String = "Injected streaming shuffle live consumer loss"

  val CLOSE_ATTEMPTS: Int = 30

  val SWEEP_MILLIS: Long = 100L

  private val injected = new AtomicBoolean(false)

  def injectOncePerExecutor(body: => Unit): Unit = {
    if (injected.compareAndSet(false, true)) {
      body
    }
  }

  def resetInjection(): Unit = injected.set(false)

  def streamingManager(): Option[StreamingShuffleManager] = {
    SparkEnv.get.shuffleManager match {
      case manager: StreamingShuffleManager => Some(manager)
      case _ => None
    }
  }

  def resolverOf(manager: StreamingShuffleManager): Option[StreamingShuffleBlockResolver] = {
    manager.shuffleBlockResolver match {
      case router: StreamingShuffleBlockRouter => Some(router.streamingResolver)
      case _ => None
    }
  }
}

/**
 * The readings a live fault reports back.
 *
 * @param egress bytes the producing side had already put on the wire
 * @param applied injections that ran, at most one per executor JVM
 */
private case class LiveFaultProbe(
    offers: LongAccumulator,
    receipt: LongAccumulator,
    egress: LongAccumulator,
    applied: LongAccumulator,
    effect: LongAccumulator) {

  def describe: String = {
    s"${offers.value} offer(s), ${receipt.value} record(s) received, ${egress.value} byte(s) " +
      s"streamed, ${applied.value} applied, effect ${effect.value}"
  }
}
