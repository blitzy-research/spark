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

import java.io.File
import java.io.IOException
import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable

import org.scalatest.matchers.should.Matchers

import org.apache.spark.{SharedSparkContext, SparkConf, SparkEnv, SparkFunSuite,
  SparkIllegalArgumentException, TaskContext, TaskContextImpl}
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.internal.config.{CPUS_PER_TASK, EXECUTOR_CORES,
  SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_SPILL_THRESHOLD}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.storage.{BlockId, DiskBlockObjectWriter, TempShuffleBlockId,
  UnrecognizedBlockId}
import org.apache.spark.util.{Clock, Utils}

/**
 * Unit coverage of [[MemorySpillManager]], the bounded and spillable buffer budget the streaming
 * shuffle producer holds its unacknowledged window in.
 */
class MemorySpillManagerSuite
  extends SparkFunSuite
    with SharedSparkContext
    with Matchers
    with StreamingShuffleTestHelper {

  private val roomyMemoryBytes: Long = 4000000L

  private val tightMemoryBytes: Long = 59500L

  private val awkwardMemoryBytes: Long = 4000037L

  private val reducePartitions: Int = 8

  private val payloadBytes: Int = 1024

  private val bufferPercent: Int = StreamingShuffleTestHelper.DefaultBufferSizePercent

  private val spillPercent: Int = StreamingShuffleTestHelper.DefaultSpillThresholdPercent

  private val percentScale: Long = StreamingShuffleTestHelper.PercentScale

  private val pollIntervalMs: Long = StreamingShuffleTestHelper.SpillPollIntervalMillis

  private val reclamationDeadlineMs: Long = StreamingShuffleTestHelper.ReclamationDeadlineMillis

  private val clockEpochMs: Long = StreamingShuffleTestHelper.ManualClockEpochMillis

  private val maxBlockPayloadBytes: Int = StreamingShuffleTestHelper.MaxBlockSizeBytes

  private val blockCharge: Long =
    payloadBytes.toLong + MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES

  private val consumerId: String = "streaming-shuffle-consumer"

  private val openManagers = new mutable.ArrayBuffer[MemorySpillManager]()

  private val openMemoryManagers = new mutable.ArrayBuffer[TaskMemoryManager]()

  private var taskAttemptCounter: Long = 0L

  override def beforeEach(): Unit = {
    super.beforeEach()
    // The metrics source is a JVM singleton whose counters outlive a test, and the executor-wide
    // buffer allowance is likewise process scoped.
    resetStreamingShuffleMetrics()
    MemorySpillManager.resetSharedStateForTesting()
    openManagers.clear()
    openMemoryManagers.clear()
  }

  override def afterEach(): Unit = {
    try {
      // Closing is what hands buffered bytes back to the task memory manager, and the figure below
      // is the machine check that it really did.
      openManagers.foreach(manager => manager.close())
      openMemoryManagers.foreach { memoryManager =>
        val leaked = memoryManager.cleanUpAllAllocatedMemory()
        assert(leaked === 0L,
          s"A closed MemorySpillManager left $leaked bytes of execution memory acquired; " +
            "buffered bytes must be released on close, whatever the outcome of the task")
      }
    } finally {
      openManagers.clear()
      openMemoryManagers.clear()
      MemorySpillManager.resetSharedStateForTesting()
      super.afterEach()
    }
  }

  private def newQuota(
      executorMemoryBytes: Long,
      bufferSizePercent: Int = bufferPercent,
      spillThresholdPercent: Int = spillPercent): MemorySpillManager.ExecutorBufferQuota = {
    new MemorySpillManager.ExecutorBufferQuota(
      bufferSizePercent, spillThresholdPercent, () => executorMemoryBytes)
  }

  private def newTrackedTaskContext(): TaskContextImpl = {
    taskAttemptCounter += 1L
    val context = newTaskContext(
      sc.env,
      partitionId = 0,
      taskAttemptId = taskAttemptCounter,
      numPartitions = reducePartitions)
    openMemoryManagers += memoryManagerOf(context)
    context
  }

  private def memoryManagerOf(context: TaskContext): TaskMemoryManager = context.taskMemoryManager()

  private def metricsOf(context: TaskContext): TaskMetrics = context.taskMetrics()

  private def attemptIdOf(context: TaskContext): Long = context.taskAttemptId()

  private def newManager(
      context: TaskContextImpl,
      clock: Clock,
      quota: MemorySpillManager.ExecutorBufferQuota,
      conf: SparkConf = streamingConfWithOverrides(),
      diskQuota: Option[MemorySpillManager.ExecutorDiskQuota] = None): MemorySpillManager = {
    val manager = new MemorySpillManager(
      memoryManagerOf(context), conf, clock, Some(quota), autoPoll = false,
      diskQuotaOverride = diskQuota)
    manager.registerPartitionCount(reducePartitions)
    manager.registerCleanup(context)
    openManagers += manager
    manager
  }

  /**
   * A spill manager over the '''production''' executor allowance, with no injected quota.
   *
   * @param context the task the consumer's memory and cleanup belong to
   * @return the manager, already told its reduce partition count and registered for cleanup
   */
  private def newProductionManager(
      context: TaskContextImpl,
      conf: SparkConf = streamingConfWithOverrides()): MemorySpillManager = {
    val manager = new MemorySpillManager(
      memoryManagerOf(context), conf, newManualClock(), quotaOverride = None, autoPoll = false)
    manager.registerPartitionCount(reducePartitions)
    manager.registerCleanup(context)
    openManagers += manager
    manager
  }

  private def bufferBlocks(manager: MemorySpillManager, partitionId: Int, count: Int): Unit = {
    val lastAccepted = manager.lastAcceptedSequence(partitionId)
    val firstSequence =
      if (lastAccepted == MemorySpillManager.UNSET_SEQUENCE) 0L else lastAccepted + 1L
    (0 until count).foreach { offset =>
      val sequenceNumber = firstSequence + offset
      val payload = payloadOfLength(sequenceNumber, payloadBytes)
      assert(manager.bufferBlock(partitionId, sequenceNumber, payload),
        s"Partition $partitionId sequence $sequenceNumber was refused, but the fixture's " +
          "allowance is sized to admit it")
    }
  }

  private def spillFilesOf(manager: MemorySpillManager): Seq[java.io.File] =
    manager.allSpilledBlocks.map(record => record.file).distinct

  /**
   * A memory consumer whose only purpose is to be a spill trigger other than the manager itself.
   */
  private class NoopMemoryConsumer(memoryManager: TaskMemoryManager)
    extends MemoryConsumer(memoryManager, MemoryMode.ON_HEAP) {

    override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
  }

  /** A spill manager that records what its inherited memory operations were asked to do. */
  private class RecordingSpillManager(
      memoryManager: TaskMemoryManager,
      conf: SparkConf,
      clock: Clock,
      quota: MemorySpillManager.ExecutorBufferQuota)
    extends MemorySpillManager(memoryManager, conf, clock, Some(quota), autoPoll = false) {

    private val insideSpill = new AtomicBoolean(false)

    private val acquisitions = new AtomicInteger(0)

    private val acquisitionsInsideSpill = new AtomicInteger(0)

    private val grantCeiling = new AtomicLong(Long.MaxValue)

    def capGrantsAt(bytes: Long): Unit = grantCeiling.set(bytes)

    def acquisitionCount: Int = acquisitions.get()

    def acquisitionsDuringSpill: Int = acquisitionsInsideSpill.get()

    override def acquireMemory(size: Long): Long = {
      acquisitions.incrementAndGet()
      if (insideSpill.get()) {
        acquisitionsInsideSpill.incrementAndGet()
      }
      super.acquireMemory(math.min(size, grantCeiling.get()))
    }

    override def spill(size: Long, trigger: MemoryConsumer): Long = {
      insideSpill.set(true)
      try {
        super.spill(size, trigger)
      } finally {
        insideSpill.set(false)
      }
    }
  }

  /**
   * A clock that advances a fixed step on every reading.
   *
   * @param startTimeMillis the reading the clock begins at
   * @param stepMillis milliseconds added after every reading
   */
  private class SteppingClock(startTimeMillis: Long, stepMillis: Long) extends Clock {

    private val current = new AtomicLong(startTimeMillis)

    override def getTimeMillis(): Long = current.getAndAdd(stepMillis)

    override def nanoTime(): Long = TimeUnit.MILLISECONDS.toNanos(current.get())

    override def waitTillTime(targetTime: Long): Long = current.get()
  }

  test("the streaming spill keys carry their specified defaults") {
    val defaults = new SparkConf(false)
    assert(defaults.get(SHUFFLE_STREAMING_SPILL_THRESHOLD) === 80,
      "The spill threshold must default to 80 percent of the buffer allowance")
    assert(defaults.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT) === 20,
      "The buffer size percent must default to 20 percent of executor memory")
    assert(spillPercent === 80, "The suite must exercise the specified default spill threshold")
    assert(bufferPercent === 20, "The suite must exercise the specified default buffer share")
  }

  test("the spill threshold accepts its documented range and refuses everything outside it") {
    Seq(50, 80, 95).foreach { accepted =>
      val conf = new SparkConf(false).set(SHUFFLE_STREAMING_SPILL_THRESHOLD, accepted)
      assert(conf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD) === accepted,
        s"$accepted is inside the documented range [50, 95] and must be accepted")
    }
    Seq(0, 49, 96, 100).foreach { rejected =>
      val conf = new SparkConf(false).set(SHUFFLE_STREAMING_SPILL_THRESHOLD, rejected)
      val failure = intercept[SparkIllegalArgumentException] {
        conf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD)
      }
      assert(failure.getCondition === "INVALID_CONF_VALUE.REQUIREMENT",
        s"A rejected spill threshold must be reported through the existing condition, " +
          s"but $rejected produced ${failure.getCondition}")
      assert(failure.getMessage.contains("The spill threshold must be in [50, 95]."),
        s"The rejection of $rejected must quote the documented range")
    }
  }

  test("the buffer size percent accepts its documented range and refuses everything outside it") {
    Seq(1, 20, 50).foreach { accepted =>
      val conf = new SparkConf(false).set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, accepted)
      assert(conf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT) === accepted,
        s"$accepted is inside the documented range [1, 50] and must be accepted")
    }
    Seq(0, 51, 100).foreach { rejected =>
      val conf = new SparkConf(false).set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, rejected)
      val failure = intercept[SparkIllegalArgumentException] {
        conf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)
      }
      assert(failure.getCondition === "INVALID_CONF_VALUE.REQUIREMENT",
        s"A rejected buffer size percent must reuse the existing condition, but $rejected " +
          s"produced ${failure.getCondition}")
      assert(failure.getMessage.contains("The buffer size percent must be in [1, 50]."),
        s"The rejection of $rejected must quote the documented range")
    }
  }

  test("a manager refuses to be constructed on an out-of-range spill threshold") {
    val context = newTrackedTaskContext()
    val invalid = streamingConfWithOverrides().set(SHUFFLE_STREAMING_SPILL_THRESHOLD, 96)
    val failure = intercept[SparkIllegalArgumentException] {
      new MemorySpillManager(
        memoryManagerOf(context),
        invalid,
        newManualClock(),
        Some(newQuota(roomyMemoryBytes)),
        autoPoll = false)
    }
    assert(failure.getCondition === "INVALID_CONF_VALUE.REQUIREMENT",
      s"Construction must fail through the existing condition, not ${failure.getCondition}")
  }

  test("the polling cadence and the reclamation bound are both 100 ms") {
    assert(MemorySpillManager.POLL_INTERVAL_MS === 100L,
      "The threshold must be polled on the contracted 100 ms cadence")
    assert(MemorySpillManager.RECLAMATION_DEADLINE_MS === 100L,
      "Reclamation must be held to the contracted 100 ms bound")
    assert(pollIntervalMs === MemorySpillManager.POLL_INTERVAL_MS,
      "The suite must drive the cadence the production component actually uses")
    assert(reclamationDeadlineMs === MemorySpillManager.RECLAMATION_DEADLINE_MS,
      "The suite must measure against the production reclamation bound")
    assert(MemorySpillManager.MAX_BLOCK_PAYLOAD_BYTES === 2097152,
      "A block payload is capped at 2 MB so that a stream pipelines rather than stalling")
    assert(maxBlockPayloadBytes === MemorySpillManager.MAX_BLOCK_PAYLOAD_BYTES,
      "The block cap must be single sourced from the wire protocol")
    assert(MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES > 0L,
      "A retained block costs more than its payload, and the budget has to be charged for it")
  }

  test("the buffer allowance divides the configured share of executor memory by partitions") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))

    val expectedTotal = StreamingShuffleTestHelper.aggregateBudgetBytes(
      roomyMemoryBytes, bufferPercent)
    assert(manager.totalBudgetBytes === expectedTotal,
      s"The aggregate allowance must be $bufferPercent percent of the injected region")
    assert(manager.spillThresholdBytes ===
        StreamingShuffleTestHelper.exactPercentageOf(expectedTotal, spillPercent),
      s"Eviction must trigger at $spillPercent percent of the aggregate allowance")
    assert(manager.numPartitions === reducePartitions,
      "The registered reduce partition count must be the divisor of the per-partition allowance")
    assert(manager.perPartitionBudgetBytes === expectedTotal / reducePartitions,
      "The per-partition allowance must be the aggregate divided by the partition count")
    assert(manager.perPartitionBudgetBytes === StreamingShuffleTestHelper.perPartitionBudgetBytes(
      roomyMemoryBytes, bufferPercent, reducePartitions),
      "The per-partition allowance must equal the specified (memory * percent) / numPartitions")
    assert(manager.perPartitionBudgetBytesFor(1) === manager.totalBudgetBytes,
      "A single-partition shuffle may use the whole aggregate allowance")
    assert(manager.maxAdmissiblePayloadBytes <= maxBlockPayloadBytes.toLong,
      "The advertised payload allowance may never exceed the protocol's own block cap")
    assert(manager.executorReservedBytes === 0L, "An idle producer reserves nothing")
    assert(manager.bufferUtilizationPercent === 0L, "An idle producer reports zero utilisation")
  }

  test("the buffer allowance takes the product before the division, not after") {
    val expectedTotal = StreamingShuffleTestHelper.aggregateBudgetBytes(
      awkwardMemoryBytes, bufferPercent)
    val expectedTrigger = StreamingShuffleTestHelper.exactPercentageOf(expectedTotal, spillPercent)

    // Anti-vacuity guards.
    val dividedFirstTotal = awkwardMemoryBytes / percentScale * bufferPercent
    assert(expectedTotal !== dividedFirstTotal,
      s"The awkward fixture must make the two orderings disagree, yet both give $expectedTotal; " +
        s"pick a memory figure whose remainder modulo $percentScale is non-zero")
    val dividedFirstTrigger = expectedTotal / percentScale * spillPercent
    assert(expectedTrigger !== dividedFirstTrigger,
      s"The derived trigger must also distinguish the orderings, yet both give $expectedTrigger")

    val manager =
      newManager(newTrackedTaskContext(), newManualClock(), newQuota(awkwardMemoryBytes))
    assert(manager.totalBudgetBytes === expectedTotal,
      s"The aggregate allowance must be the specified ($awkwardMemoryBytes * $bufferPercent) / " +
        s"$percentScale = $expectedTotal, not the $dividedFirstTotal a division taken first " +
        s"would give, yet it was ${manager.totalBudgetBytes}")
    assert(manager.spillThresholdBytes === expectedTrigger,
      s"The spill trigger must be the specified $spillPercent percent of that allowance, " +
        s"$expectedTrigger, not $dividedFirstTrigger, yet it was ${manager.spillThresholdBytes}")
    assert(manager.perPartitionBudgetBytes === expectedTotal / reducePartitions,
      s"and the per-partition allowance must divide the exact aggregate, yet it was " +
        s"${manager.perPartitionBudgetBytes}")

    assert(MemorySpillManager.percentageOf(awkwardMemoryBytes, 0) === 0L,
      "Zero percent of anything is zero")
    assert(MemorySpillManager.percentageOf(0L, bufferPercent) === 0L,
      "Any percentage of nothing is nothing")
    Seq(StreamingShuffleTestHelper.MinBufferSizePercent,
        bufferPercent,
        StreamingShuffleTestHelper.MaxBufferSizePercent,
        spillPercent,
        percentScale.toInt).foreach { percent =>
      assert(MemorySpillManager.percentageOf(awkwardMemoryBytes, percent) ===
          StreamingShuffleTestHelper.exactPercentageOf(awkwardMemoryBytes, percent),
        s"percentageOf must be exact at $percent percent of $awkwardMemoryBytes")
    }
    assert(MemorySpillManager.percentageOf(Long.MaxValue, StreamingShuffleTestHelper
        .MaxBufferSizePercent) ===
        StreamingShuffleTestHelper.exactPercentageOf(Long.MaxValue,
          StreamingShuffleTestHelper.MaxBufferSizePercent),
      "The largest representable region must still yield its exact percentage")
    assert(MemorySpillManager.percentageOf(Long.MaxValue,
        StreamingShuffleTestHelper.MaxBufferSizePercent) > 0L,
      "A wrapped product would report a negative allowance, which reads as permanent pressure")
  }

  // -----------------------------------------------------------------------------------------------
  // The 100 ms threshold polling cadence.
  test("threshold polling evaluates buffer utilisation on a 100 ms cadence") {
    val clock = newManualClock()
    val manager = newManager(newTrackedTaskContext(), clock, newQuota(tightMemoryBytes))

    assert(manager.perPartitionBudgetBytes >= blockCharge,
      "The tight fixture must still admit one block per partition")
    assert(manager.perPartitionBudgetBytes < 2L * blockCharge,
      "The tight fixture must admit no more than one block per partition")
    (0 until reducePartitions).foreach(partitionId => bufferBlocks(manager, partitionId, 1))
    assert(manager.executorReservedBytes === reducePartitions * blockCharge,
      "Every admitted block must be charged against the executor-wide allowance")
    assert(manager.executorReservedBytes === manager.spillThresholdBytes,
      "The tight fixture must land the reservation exactly on the spill trigger")
    assert(manager.spillCount === 0L, "Buffering alone must never evict")

    assert(manager.pollOnce(), "The first poll taken at the threshold must evict")
    assert(manager.spillCount === 1L, "One poll at the threshold is one eviction event")
    assert(manager.executorReservedBytes < manager.spillThresholdBytes,
      "An eviction must take the reservation back below the trigger it fired at")

    val drained = (0 until reducePartitions).filter(id => manager.bufferedBytesFor(id) == 0L)
    assert(drained === Seq(0),
      s"A tie in footprint and access time must be broken by ascending partition id, but " +
        s"the drained partitions were $drained")

    bufferBlocks(manager, drained.head, 1)
    assert(manager.bufferedBytes === manager.spillThresholdBytes,
      "The refill must put producer payload and retained-block overhead back on the trigger")
    assert(manager.executorReservedBytes ===
      manager.spillThresholdBytes + MemorySpillManager.SPILLED_RECORD_METADATA_BYTES,
      "The one durable record from the first eviction must stay charged above producer bytes")

    assert(!manager.pollOnce(), "A poll taken with no time elapsed must be suppressed")
    assert(manager.spillCount === 1L, "A suppressed poll must not evict")

    clock.advance(pollIntervalMs - 1L)
    assert(!manager.pollOnce(), "A poll one millisecond short of the cadence must be suppressed")
    assert(manager.spillCount === 1L, "A suppressed poll must not evict")

    clock.advance(1L)
    assert(manager.pollOnce(), "A poll on the cadence boundary must evaluate the threshold")
    assert(manager.spillCount === 2L, "The second due poll at the threshold is a second event")
  }

  test("maybeSpill evaluates the threshold immediately, ignoring the poll cadence") {
    val clock = newManualClock()
    val manager = newManager(newTrackedTaskContext(), clock, newQuota(tightMemoryBytes))
    (0 until reducePartitions).foreach(partitionId => bufferBlocks(manager, partitionId, 1))

    assert(manager.pollOnce(), "The first poll at the threshold must evict")
    val drained = (0 until reducePartitions).filter(id => manager.bufferedBytesFor(id) == 0L)
    bufferBlocks(manager, drained.head, 1)
    assert(!manager.pollOnce(), "The cadence gate must suppress a poll taken too soon")
    assert(manager.spillCount === 1L, "A suppressed poll must not evict")

    assert(manager.maybeSpill(), "maybeSpill must not be subject to the polling cadence")
    assert(manager.spillCount === 2L, "An immediate evaluation at the threshold is an event")
  }

  test("a poll below the spill threshold evicts nothing however often it is taken") {
    val clock = newManualClock()
    val manager = newManager(newTrackedTaskContext(), clock, newQuota(roomyMemoryBytes))
    bufferBlocks(manager, 0, 4)
    assert(manager.executorReservedBytes < manager.spillThresholdBytes,
      "The roomy fixture must stay well below the trigger with four blocks buffered")

    assert(!manager.pollOnce(), "A poll below the threshold must not evict")
    (1 to 10).foreach { _ =>
      clock.advance(pollIntervalMs)
      assert(!manager.pollOnce(), "A due poll below the threshold must still not evict")
    }
    assert(manager.spillCount === 0L, "Nothing below the threshold is an eviction event")
    assert(observedSpillCount() === 0L, "The published counter must agree with the component")
    assert(manager.bufferedBytes === 4L * blockCharge,
      "Buffered bytes must survive a poll that found nothing to do")
    assert(manager.allSpilledBlocks.isEmpty, "Nothing may reach local disk below the threshold")
  }

  test("eviction considers the largest buffered partitions first") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    bufferBlocks(manager, 5, 1)
    bufferBlocks(manager, 3, 3)
    bufferBlocks(manager, 7, 2)

    assert(manager.bufferedBytesFor(3) === 3L * blockCharge, "Partition 3 must hold three blocks")
    assert(manager.bufferedBytesFor(7) === 2L * blockCharge, "Partition 7 must hold two blocks")
    assert(manager.bufferedBytesFor(5) === blockCharge, "Partition 5 must hold one block")
    assert(manager.spillSelectionOrder === Seq(3, 7, 5),
      "Eviction must consider the largest buffered footprint first")
    assert(!manager.spillSelectionOrder.contains(0),
      "A partition holding nothing in memory is not an eviction candidate at all")
  }

  test("eviction breaks footprint ties least recently used first") {
    val clock = newManualClock()
    val manager = newManager(newTrackedTaskContext(), clock, newQuota(roomyMemoryBytes))
    bufferBlocks(manager, 6, 1)
    clock.advance(pollIntervalMs)
    bufferBlocks(manager, 2, 1)

    assert(manager.bufferedBytesFor(6) === manager.bufferedBytesFor(2),
      "The two partitions must hold identical footprints for the tie-break to be the subject")
    assert(manager.spillSelectionOrder === Seq(6, 2),
      "A footprint tie must be broken least recently used first, ahead of the identifier")

    clock.advance(pollIntervalMs)
    assert(manager.retainedPayload(6, 0L).isDefined,
      "A buffered block must be readable back for retransmission")
    assert(manager.spillSelectionOrder === Seq(2, 6),
      "Reading a partition must move it to the back of the eviction order")
  }

  test("eviction breaks footprint and access-time ties by ascending partition id") {
    val clock = newManualClock()
    val manager = newManager(newTrackedTaskContext(), clock, newQuota(roomyMemoryBytes))
    // The clock never moves while the blocks are admitted, so every partition carries the same
    // access stamp and the same footprint.
    Seq(7, 1, 4).foreach(partitionId => bufferBlocks(manager, partitionId, 2))

    val captured = manager.spillSelectionOrder
    assert(captured === Seq(1, 4, 7),
      "A tie in both footprint and access time must be broken by ascending partition id")

    // Stability across reads, stated as a comparison of a captured order against a later one rather
    // than of the order against itself -- which would hold however unstable the order was.
    clock.advance(pollIntervalMs)
    val diagnostics = Seq(
      manager.bufferedBytes,
      manager.bufferedBytesFor(4),
      manager.bufferUtilizationPercent,
      manager.spillCount,
      manager.diskBytesSpilled,
      manager.memoryBytesSpilled,
      manager.retainedBlockCount(4).toLong,
      manager.allSpilledBlocks.size.toLong,
      manager.executorReservedBytes,
      manager.peakMemoryBytes)
    assert(diagnostics.forall(_ >= 0L),
      s"Every diagnostic accessor must answer a sane figure, yet they reported $diagnostics")
    assert(manager.spillSelectionOrder === captured,
      s"The published order must be unchanged by a clock advance and by ten diagnostic reads, " +
        s"yet it moved from $captured to ${manager.spillSelectionOrder}")
    assert(manager.spillSelectionOrder === captured,
      "Reading the order must not be a use of the partitions it names")
  }

  test("the spill callback evicts in the published selection order") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))
    assert(memoryManagerOf(context).getTungstenMemoryMode === MemoryMode.ON_HEAP,
      "The streaming buffer is an on-heap consumer, so the fixture must be on-heap too")

    bufferBlocks(manager, 2, 1)
    bufferBlocks(manager, 5, 3)
    bufferBlocks(manager, 6, 2)
    assert(manager.spillSelectionOrder === Seq(5, 6, 2),
      "The order must be largest buffered footprint first")

    val freed = manager.spill(blockCharge, trigger)
    assert(freed === 3L * blockCharge,
      "The callback must report exactly the bytes that left memory, which is a whole partition")
    assert(manager.bufferedBytesFor(5) === 0L, "The first candidate must have left memory")
    assert(manager.spilledBlocks(5).size === 3, "Its blocks must be retained as durable records")
    assert(manager.bufferedBytesFor(6) === 2L * blockCharge, "The runner-up must be untouched")
    assert(manager.spilledBlocks(6).isEmpty, "The runner-up must not have reached disk")
    assert(manager.bufferedBytesFor(2) === blockCharge, "The last candidate must be untouched")
    assert(manager.spillSelectionOrder === Seq(6, 2),
      "A partition with nothing left in memory must drop out of the order")
  }

  test("a block the allowance cannot hold is admitted straight to disk instead") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    assert(manager.durableAdmissionCount === 0L, "Nothing may have bypassed the buffer yet")

    val payload = payloadOfLength(41L, payloadBytes)
    assert(manager.admitDurably(0, 0L, payload),
      "A block the producer could not buffer must still be admitted, on disk")
    assert(manager.durableAdmissionCount === 1L,
      "The write-through must be counted, so an operator sees the allowance was met and spilled " +
        "past rather than silently absorbed")
    assert(manager.bufferedBytes === 0L,
      "A written-through block occupies no memory at all, which is the whole point of it")
    assert(manager.diskBytesSpilled > 0L, "and its payload must have reached local disk")
    assert(manager.spillCount === 1L,
      "and it must be counted as the spill event it is: the allowance could not hold the block " +
        "and the block went to local disk, which is the condition the pressure signal reports. " +
        "Publishing the volume without the event would let an operator watch disk spilling climb " +
        "while the spill count stayed at zero")
    assert(manager.retainedBlockCount(0) === 1,
      "It is still part of the retransmission window, because a consumer may yet ask for it")
    assert(manager.spilledBlocks(0).map(_.sequenceNumber) === Seq(0L),
      "and it must be recorded under exactly the sequence number it was offered under")
    assert(manager.retainedPayload(0, 0L).map(_.toSeq) === Some(payload.toSeq),
      "A written-through block must read back byte for byte, or a retransmission would corrupt")

    assert(manager.lastAcceptedSequence(0) === 0L,
      "A write-through must advance the partition's numbering exactly as a buffered block does")
    bufferBlocks(manager, 0, 1)
    assert(manager.lastAcceptedSequence(0) === 1L,
      "and the next buffered block must continue that same run")
    assert(manager.bufferedBytes === blockCharge,
      "That next block is a buffered one, so it is the only thing charged to the allowance")

    val replayed = intercept[IllegalArgumentException](manager.admitDurably(0, 1L, payload))
    assert(replayed.getMessage.contains("gap-free ascending run"),
      s"A sequence already accepted must be refused as a numbering violation, not ${replayed}")
    val skipped = intercept[IllegalArgumentException](manager.admitDurably(0, 3L, payload))
    assert(skipped.getMessage.contains("expected sequence 2"),
      s"A skipped sequence must name the position it expected, yet it said ${skipped.getMessage}")

    intercept[IllegalArgumentException](manager.admitDurably(0, 2L, Array.emptyByteArray))
    assert(manager.lastAcceptedSequence(0) === 1L,
      "A rejected offer must leave the partition's numbering exactly where it was")
    assert(manager.durableAdmissionCount === 1L,
      "and must not be counted as a write-through")

    assert(manager.admitDurably(0, 2L, payload),
      "The partition must still accept its next sequence after three refused offers")
    assert(manager.durableAdmissionCount === 2L, "which is the second real write-through")
    assert(manager.retainedBlockCount(0) === 3,
      "and the window must now hold all three blocks, however each of them got there")
  }

  test("an eviction under way is published rather than left to be inferred") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))
    assert(!manager.evictionInFlight, "An idle store has no eviction under way")

    bufferBlocks(manager, 0, 2)
    assert(!manager.evictionInFlight, "Nor has one that is merely holding blocks")
    assert(manager.spill(Long.MaxValue, trigger) === 2L * blockCharge,
      "The pressure callback must evict what it holds")
    assert(!manager.evictionInFlight,
      "and must publish the flag as clear once the write has landed, so a deferring producer is " +
        "never left waiting on a reclamation that has already completed")
    assert(manager.spillCount === 1L, "Exactly one eviction event must have been counted")
  }

  test("the executor task-slot count is read from the configuration, never from the machine") {
    val declared = streamingConfWithOverrides()
      .set(EXECUTOR_CORES, 8)
      .set(CPUS_PER_TASK, 2)
    assert(MemorySpillManager.executorTaskSlots(declared) === 4,
      "Eight declared cores at two CPUs per task is four slots")

    val localMaster = streamingConfWithOverrides().set("spark.master", "local[6]")
    assert(MemorySpillManager.executorTaskSlots(localMaster) === 6,
      "A local master's thread count is the declared core count for this purpose")

    val bothDeclared = streamingConfWithOverrides()
      .set("spark.master", "local[6]")
      .set(EXECUTOR_CORES, 3)
    assert(MemorySpillManager.executorTaskSlots(bothDeclared) === 6,
      "The larger of the two declarations wins, so neither can understate the concurrency")

    val oversubscribed = streamingConfWithOverrides()
      .set(EXECUTOR_CORES, 1)
      .set(CPUS_PER_TASK, 4)
    assert(MemorySpillManager.executorTaskSlots(oversubscribed) === 1,
      "A task wider than the executor still leaves one slot, never zero, because zero would " +
        "divide by nothing when the framing share is derived")

    // A configuration with no master at all reads as the default "local", which declares one thread
    // and therefore one slot.
    val undeclared = streamingConfWithOverrides()
    assert(!undeclared.contains("spark.master") && !undeclared.contains(EXECUTOR_CORES.key),
      "The fixture must declare neither a master nor a core count for this case to be the one " +
        "being exercised")
    assert(MemorySpillManager.executorTaskSlots(undeclared) === 1,
      "An undeclared master reads as local, which is one thread and therefore one slot")

    val cluster = streamingConfWithOverrides().set("spark.master", "spark://host:7077")
    assert(MemorySpillManager.executorTaskSlots(cluster) ===
        math.max(1, Runtime.getRuntime.availableProcessors()),
      "With a cluster master and no declared cores the processor count is the only estimate left")

    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes),
      conf = declared)
    assert(manager.concurrentTaskSlots === 4,
      "A manager must hold the slot count its own configuration implies")
  }

  test("the spill callback declines a reclamation it triggered itself") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    bufferBlocks(manager, 0, 2)

    assert(manager.spill(Long.MaxValue, manager) === 0L,
      "A self-triggered reclamation must be declined rather than served")
    assert(manager.bufferedBytes === 2L * blockCharge, "A declined request must free nothing")
    assert(manager.spillCount === 0L, "A declined request is not an eviction event")
  }

  test("an acknowledgement reclaims buffered memory within the 100 ms bound") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    assert(manager.registerConsumer(consumerId), "A live producer must admit a consumer")
    bufferBlocks(manager, 4, 3)
    assert(manager.bufferedBytes === 3L * blockCharge, "Three blocks must be held in memory")
    assert(manager.executorReservedBytes === manager.bufferedBytes,
      "Held bytes and reserved bytes must agree, so admission and the allowance cannot drift")

    val releasedPrefix = manager.acknowledge(consumerId, 4, 1L)
    assert(releasedPrefix === 2L * blockCharge,
      "An acknowledgement must release exactly the prefix it covers")
    assert(manager.bufferedBytesFor(4) === blockCharge, "The unacknowledged block must be retained")
    assert(manager.lastReclamationDurationMs <= reclamationDeadlineMs,
      s"Reclamation must complete inside the ${reclamationDeadlineMs} ms bound, but took " +
        s"${manager.lastReclamationDurationMs} ms")
    // The clock never moved, which is the precise form of "measured on the injected clock" -- a
    // component reading wall time here would report something other than zero.
    assert(manager.lastReclamationDurationMs === 0L,
      "Reclamation latency must be measured on the injected clock, not on wall time")
    assert(manager.reclamationDeadlineBreaches === 0L, "No breach may be recorded for a held clock")

    val releasedRest = manager.acknowledge(consumerId, 4, 2L)
    assert(releasedRest === blockCharge, "The final acknowledgement must release the last block")
    assert(manager.bufferedBytes === 0L, "A fully acknowledged partition must hold no memory")
    assert(manager.executorReservedBytes === 0L,
      "Released bytes must go back to the executor-wide allowance as well as to the task")
    assert(manager.getUsed() === 0L, "Released bytes must go back to the task memory manager")
    assert(manager.reclamationDeadlineBreaches === 0L, "Still no breach may be recorded")
  }

  test("a reclamation that overruns the 100 ms bound is measured and counted") {
    val stepMs = 150L
    val clock = new SteppingClock(clockEpochMs, stepMs)
    val manager = newManager(newTrackedTaskContext(), clock, newQuota(roomyMemoryBytes))
    assert(manager.registerConsumer(consumerId), "A live producer must admit a consumer")
    bufferBlocks(manager, 1, 2)

    val released = manager.acknowledge(consumerId, 1, 1L)
    assert(released === 2L * blockCharge, "The acknowledgement must still release its whole prefix")
    // Every reading of this clock is a step later than the one before it, so any measurement at all
    // spans at least one step and a step of 150 ms cannot fit inside a 100 ms bound.
    assert(manager.lastReclamationDurationMs >= stepMs,
      s"A measurement spanning at least one ${stepMs} ms step must be at least that wide")
    assert(manager.lastReclamationDurationMs % stepMs === 0L,
      "The measurement must be a whole number of clock steps, so it came from the injected clock")
    assert(manager.reclamationDeadlineBreaches === 1L,
      "An overrun must be counted once, so a latency symptom is visible without being enforced")
    assert(manager.bufferedBytes === 0L,
      "An overrun is recorded, never enforced: the memory is still released")
  }

  test("an acknowledgement releases memory and never a spilled segment") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))
    assert(manager.registerConsumer(consumerId), "A live producer must admit a consumer")
    bufferBlocks(manager, 0, 2)
    assert(manager.spill(Long.MaxValue, trigger) === 2L * blockCharge,
      "Both buffered blocks must leave memory for local disk")
    val files = spillFilesOf(manager)
    assert(files.nonEmpty, "An eviction must produce at least one spill file")

    bufferBlocks(manager, 0, 1)
    val released = manager.acknowledge(consumerId, 0, 2L)
    assert(released === blockCharge,
      "Only the in-memory block has memory to give back: the evicted pair gave theirs at eviction")
    assert(manager.bufferedBytes === 0L, "The in-memory half of the window must be released")
    assert(files.forall(file => file.exists()),
      "Map output on local disk must stay readable, because a later reduce attempt is entitled " +
        "to read it again")
    assert(manager.spilledBlocks(0).size === 2,
      "A durable record is map output rather than a retransmission buffer and outlives an ack")
  }

  test("an acknowledgement is refused unless registered, advancing and within what was sent") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    bufferBlocks(manager, 0, 1)

    assert(manager.acknowledge("a-consumer-that-never-subscribed", 0, 0L) === 0L,
      "An unregistered party must not be able to retire anything")
    assert(manager.rejectedAcknowledgementCount === 1L, "The refusal must be counted")

    assert(manager.registerConsumer(consumerId), "A live producer must admit a consumer")
    assert(manager.acknowledge(consumerId, 0, Long.MaxValue) === 0L,
      "A position beyond what was actually sent must retire nothing at all")
    assert(manager.rejectedAcknowledgementCount === 2L, "That refusal must be counted too")
    assert(manager.bufferedBytes === blockCharge, "A refused acknowledgement must free nothing")

    assert(manager.acknowledge(consumerId, 0, 0L) === blockCharge,
      "A registered, advancing, in-range acknowledgement must release its prefix")
    assert(manager.acknowledge(consumerId, 0, 0L) === 0L,
      "A replayed acknowledgement must be inert rather than release anything twice")
    assert(manager.rejectedAcknowledgementCount === 3L, "The replay must be counted as refused")
  }

  test("memory is retired only once every registered consumer has confirmed receipt") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    val slowConsumerId = "streaming-shuffle-slow-consumer"
    assert(manager.registerConsumer(consumerId), "The first consumer must be admitted")
    assert(manager.registerConsumer(slowConsumerId), "The second consumer must be admitted")
    bufferBlocks(manager, 0, 2)

    assert(manager.acknowledge(consumerId, 0, 1L) === 0L,
      "Retirement must not advance past the slowest registered consumer")
    assert(manager.bufferedBytes === 2L * blockCharge, "Nothing may be released while one lags")
    assert(manager.consumerPosition(consumerId, 0) === Some(1L),
      "The fast consumer's own position must still be recorded")
    assert(manager.consumerPosition(slowConsumerId, 0).isEmpty,
      "The slow consumer must have acknowledged nothing")

    assert(manager.acknowledge(slowConsumerId, 0, 1L) === 2L * blockCharge,
      "The last consumer to confirm receipt is the one whose acknowledgement releases the memory")
    assert(manager.bufferedBytes === 0L, "The confirmed prefix must be released in full")
  }

  test("unregistering a stale consumer immediately releases the prefix a live peer confirmed") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    val liveConsumer = "attempt-41-partitions-0-0"
    val staleConsumer = "attempt-42-partitions-0-0"
    assert(manager.registerConsumer(liveConsumer), "The live consumer must be admitted")
    assert(manager.registerConsumer(staleConsumer), "The stale consumer must initially be admitted")
    bufferBlocks(manager, 0, 3)

    assert(manager.acknowledge(liveConsumer, 0, 2L) === 0L,
      "The stale cursor must pin the prefix until that consumer is finally unregistered")
    assert(manager.retainedBlockCount(0) === 3,
      "All three blocks must remain while the stale cursor is registered")
    assert(manager.unregisterConsumer(staleConsumer) === 3L * blockCharge,
      "Removing the stale cursor must reclaim the live consumer's confirmed prefix immediately")
    assert(manager.retainedBlockCount(0) === 0,
      "No block may remain pinned by an identity that has been unregistered")
    assert(manager.registeredConsumers === Set(liveConsumer),
      "Final unregistration must remove exactly the stale identity")
    assert(MemorySpillManager.registeredConsumerIdentityCount === 1,
      "The executor-wide identity ledger must release the stale identity too")
  }

  test("consumer cursor registration is capped per store and across the executor") {
    val first = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    val second = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    val third = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    val perStoreCap = MemorySpillManager.MAX_REGISTERED_CONSUMERS_PER_STORE
    val executorCap = MemorySpillManager.MAX_REGISTERED_CONSUMER_IDENTITIES

    (0 until perStoreCap).foreach { index =>
      assert(first.registerConsumer(s"attempt-$index-partitions-0-0"),
        s"The first store must admit identity $index through its exact cap")
    }
    assert(first.registeredConsumers.size === perStoreCap)
    assert(!first.registerConsumer(s"attempt-$perStoreCap-partitions-0-0"),
      "A store must refuse the first identity beyond its cursor cap")
    assert(first.rejectedConsumerRegistrationCount === 1L)
    assert(MemorySpillManager.registeredConsumerIdentityCount === perStoreCap)

    (perStoreCap until executorCap).foreach { index =>
      assert(second.registerConsumer(s"attempt-$index-partitions-0-0"),
        s"The second store must admit identity $index through the executor-wide cap")
    }
    assert(MemorySpillManager.registeredConsumerIdentityCount === executorCap)
    assert(!third.registerConsumer(s"attempt-$executorCap-partitions-0-0"),
      "A new identity must be refused once the executor-wide unique-id cap is full")
    assert(third.rejectedConsumerRegistrationCount === 1L)

    val existingIdentity = "attempt-0-partitions-0-0"
    assert(third.registerConsumer(existingIdentity),
      "An already charged identity must remain usable by another producer at the unique-id cap")
    assert(MemorySpillManager.registeredConsumerIdentityCount === executorCap,
      "Reference counting an existing identity must not consume another unique-id slot")
    assert(third.unregisterConsumer(existingIdentity) === 0L,
      "Unregistering a cursor with no retained partition releases no bytes")
    assert(MemorySpillManager.registeredConsumerIdentityCount === executorCap,
      "The original producer's reference must keep the shared identity charged")

    assert(first.releaseRetainedConsumerState() === perStoreCap,
      "The resolver-side final release must drop every cursor owned by the first store")
    assert(MemorySpillManager.registeredConsumerIdentityCount === executorCap - perStoreCap,
      "Only the second store's identities may remain after the first store is released")
    assert(second.releaseRetainedConsumerState() === perStoreCap,
      "The second store must release the remaining executor identities")
    assert(MemorySpillManager.registeredConsumerIdentityCount === 0,
      "The executor-wide identity ledger must return to zero after final release")
  }

  test("a large acknowledged prefix is reclaimed in bounded batches") {
    val manager = newManager(
      newTrackedTaskContext(), newManualClock(), newQuota(64L * 1024L * 1024L))
    val batchSize = MemorySpillManager.MAX_RECLAIMED_BLOCKS_PER_BATCH
    val blockCount = 2 * batchSize + 17
    assert(manager.registerConsumer(consumerId), "The consumer must be admitted")
    bufferBlocks(manager, 0, blockCount)
    assert(manager.retainedBlockCount(0) === blockCount)

    val firstBatch = manager.acknowledge(consumerId, 0, blockCount - 1L)
    assert(firstBatch === batchSize.toLong * blockCharge,
      "The acknowledgement-facing call must retire exactly one bounded batch")
    assert(manager.retainedBlockCount(0) === blockCount - batchSize,
      "The remainder must stay charged until the bounded worker batches run")
    assert(manager.pendingReclamationCount === 1,
      "One coalesced partition request must represent the remaining prefix")
    assert(manager.reclamationBatchCount === 1L,
      "The acknowledgement-facing call must run one reclamation batch")

    val remainder = manager.drainPendingReclamationForTesting()
    assert(remainder === (blockCount - batchSize).toLong * blockCharge,
      "The deterministic worker seam must release the whole queued remainder")
    assert(manager.retainedBlockCount(0) === 0 && manager.bufferedBytes === 0L,
      "All acknowledged blocks and their memory charge must be gone after the worker drains")
    assert(manager.pendingReclamationCount === 0,
      "The partition must leave no queued reclamation after its final batch")
    assert(manager.reclamationBatchCount === 3L,
      "Two full batches and one final partial batch must service the acknowledged prefix")
    assert(manager.lastReclamationDurationMs === 0L,
      "The end-to-end duration must use the held test clock and stay inside the 100 ms bound")
    assert(manager.reclamationDeadlineBreaches === 0L)
  }

  test("spill volume lands on the existing task metrics accumulators exactly once") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))
    bufferBlocks(manager, 0, 4)

    val freed = manager.spill(blockCharge, trigger)
    assert(freed === 4L * blockCharge, "The whole partition must leave memory")
    assert(manager.memoryBytesSpilled === freed, "The component must account what left memory")
    assert(manager.diskBytesSpilled > 0L, "Bytes must have been committed to local disk")
    assert(manager.peakMemoryBytes > 0L, "The high-water mark of the reservation must be recorded")

    val metrics = metricsOf(context)
    assert(metrics.memoryBytesSpilled === 0L, "Nothing may be reported before the consumer closes")

    context.markTaskCompleted(None)
    assert(manager.isClosed, "Task completion must close the consumer")
    assert(metrics.memoryBytesSpilled === manager.memoryBytesSpilled,
      "Spilled memory must be reported on TaskMetrics.memoryBytesSpilled")
    assert(metrics.diskBytesSpilled === manager.diskBytesSpilled,
      "Committed bytes must be reported on TaskMetrics.diskBytesSpilled")
    assert(metrics.peakExecutionMemory === manager.peakMemoryBytes,
      "The peak reservation must be reported on TaskMetrics.peakExecutionMemory")

    manager.close()
    assert(metrics.memoryBytesSpilled === manager.memoryBytesSpilled,
      "A second close must not report spilled memory twice")
    assert(metrics.diskBytesSpilled === manager.diskBytesSpilled,
      "A second close must not report committed bytes twice")
    assert(metrics.peakExecutionMemory === manager.peakMemoryBytes,
      "A second close must not report the peak reservation twice")
  }

  test("streaming spill accounting introduces no parallel task metric") {
    val accessors = classOf[TaskMetrics].getMethods.toSeq.map(method => method.getName)
    val streamingSpecific = accessors.filter(name => name.toLowerCase(Locale.ROOT).contains(
      "streaming"))
    assert(streamingSpecific.isEmpty,
      "TaskMetrics must carry no streaming-specific accumulator, because a parallel counter " +
        "would make the streaming path invisible to every existing surface, but found " +
        s"$streamingSpecific")

    Seq("memoryBytesSpilled", "diskBytesSpilled", "peakExecutionMemory").foreach { accessor =>
      assert(accessors.contains(accessor),
        s"TaskMetrics must expose $accessor, which is the accumulator the streaming path reuses")
    }
  }

  test("a spill uses fresh metrics and never inflates the task's shuffle write metrics") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))
    bufferBlocks(manager, 0, 3)
    assert(manager.spill(Long.MaxValue, trigger) > 0L, "The partition must leave memory")
    context.markTaskCompleted(None)

    val writeMetrics = metricsOf(context).shuffleWriteMetrics
    assert(writeMetrics.bytesWritten === 0L,
      "A spill must not be counted as shuffle output: it is a buffer eviction, not a map write")
    assert(writeMetrics.recordsWritten === 0L, "A spill must not inflate shuffle records written")
    assert(writeMetrics.writeTime === 0L, "A spill must not inflate shuffle write time")
    assert(metricsOf(context).diskBytesSpilled > 0L,
      "The spilled volume must still be visible, on the accumulator that means exactly that")
  }

  test("the spill callback releases memory without ever acquiring any") {
    val context = newTrackedTaskContext()
    val manager = new RecordingSpillManager(
      memoryManagerOf(context),
      streamingConfWithOverrides(),
      newManualClock(),
      newQuota(roomyMemoryBytes))
    manager.registerPartitionCount(reducePartitions)
    manager.registerCleanup(context)
    openManagers += manager
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))

    bufferBlocks(manager, 0, 3)
    assert(manager.acquisitionCount >= 3,
      "Every admission must reserve through the inherited acquisition, so the recording is real")

    val freed = manager.spill(Long.MaxValue, trigger)
    assert(freed === 3L * blockCharge,
      "The callback must free every buffered byte it was asked for")
    assert(manager.acquisitionsDuringSpill === 0,
      "acquireMemory must never be called from spill, which the MemoryConsumer contract " +
        s"documents as a deadlock, but it was called ${manager.acquisitionsDuringSpill} times")
    assert(manager.bufferedBytes === 0L, "Every buffered byte must have left memory")
    assert(manager.getUsed() === 0L, "The freed bytes must have gone back to the memory manager")
  }

  test("a partial memory grant is surfaced as memory pressure rather than tolerated") {
    val context = newTrackedTaskContext()
    val manager = new RecordingSpillManager(
      memoryManagerOf(context),
      streamingConfWithOverrides(),
      newManualClock(),
      newQuota(roomyMemoryBytes))
    manager.registerPartitionCount(reducePartitions)
    manager.registerCleanup(context)
    openManagers += manager

    assert(!manager.memoryPressureDetected, "No pressure has been observed yet")
    manager.capGrantsAt(blockCharge - 1L)
    assert(!manager.bufferBlock(0, 0L, payloadOfLength(0L, payloadBytes)),
      "A block that cannot be fully reserved must be refused outright, never half admitted")
    assert(manager.memoryPressureDetected,
      "A partial grant is the memory-pressure trip condition and has to reach the fallback policy")
    assert(manager.memoryPressureEvents >= 1L, "The occurrence must be counted, not only flagged")

    assert(manager.bufferedBytes === 0L, "A refused admission must retain nothing")
    assert(manager.executorReservedBytes === 0L,
      "The reservation must be returned to the executor-wide allowance")
    assert(manager.getUsed() === 0L, "The partial grant must be handed back to the memory manager")
    assert(manager.retainedBlockCount(0) === 0, "No record of the refused block may survive")
    assert(manager.lastAcceptedSequence(0) === MemorySpillManager.UNSET_SEQUENCE,
      "A rolled back reservation must leave the partition's sequence run unclaimed")

    manager.clearMemoryPressure()
    assert(!manager.memoryPressureDetected, "Clearing must re-arm the sticky signal")
    assert(manager.memoryPressureEvents >= 1L, "Clearing must not erase the cumulative tally")

    manager.capGrantsAt(Long.MaxValue)
    assert(manager.bufferBlock(0, 0L, payloadOfLength(0L, payloadBytes)),
      "The same block must be admitted once the allocator can satisfy it in full")
    assert(manager.bufferedBytes === blockCharge, "The admitted block must be charged in full")
  }

  test("a block larger than the per-partition allowance is refused rather than retried forever") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(tightMemoryBytes))
    val allowance = manager.maxAdmissiblePayloadBytes
    assert(allowance > 0L, "The tight fixture must still be able to admit one framed block")

    assert(manager.bufferBlock(0, 0L, payloadOfLength(0L, allowance.toInt)),
      "A payload no larger than the advertised allowance must always fit an empty partition")
    assert(!manager.bufferBlock(1, 0L, payloadOfLength(1L, allowance.toInt + 1)),
      "A payload beyond the per-partition allowance must be refused permanently")
    assert(manager.memoryPressureDetected,
      "A permanently inadmissible block must raise memory pressure so the shuffle can degrade")
    assert(manager.bufferedBytesFor(1) === 0L, "The refused partition must hold nothing")
  }

  test("an empty or oversized payload is rejected at the boundary") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    intercept[IllegalArgumentException] {
      manager.bufferBlock(0, 0L, Array.emptyByteArray)
    }
    intercept[IllegalArgumentException] {
      manager.bufferBlock(0, 0L, payloadOfLength(0L, maxBlockPayloadBytes + 1))
    }
    bufferBlocks(manager, 0, 1)
    intercept[IllegalArgumentException] {
      manager.bufferBlock(0, 2L, payloadOfLength(2L, payloadBytes))
    }
    intercept[IllegalArgumentException] {
      manager.bufferBlock(0, 0L, payloadOfLength(0L, payloadBytes))
    }
    assert(manager.retainedBlockCount(0) === 1, "Only the one legitimate block may be retained")
  }

  test("spill writes temporary shuffle blocks through the block manager's disk writer") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))
    val payloads = (0 until 3).map(index => payloadOfLength(index.toLong, payloadBytes + index))
    payloads.zipWithIndex.foreach { case (payload, index) =>
      assert(manager.bufferBlock(0, index.toLong, payload),
        s"Block $index must be admitted by the roomy fixture")
    }
    assert(manager.spill(Long.MaxValue, trigger) > 0L, "The partition must leave memory")

    val records = manager.spilledBlocks(0)
    assert(records.size === payloads.size, "Every evicted block must leave a durable record")
    val localDirs =
      sc.env.blockManager.diskBlockManager.localDirs.toSeq.map(dir => dir.getCanonicalPath)
    records.foreach { record =>
      assert(record.blockId.name.startsWith("temp_shuffle_"),
        "A spill segment must be allocated as a temporary shuffle block, but was " +
          s"${record.blockId}")
      assert(record.file.exists(), s"The spill file of ${record.blockId} must exist on disk")
      val parent = record.file.getParentFile.getCanonicalPath
      assert(localDirs.exists(dir => parent.startsWith(dir)),
        s"A spill file must live in one of the executor's local directories, but $parent does not")
      assert(manager.spillFile(record.blockId) === Some(record.file),
        "A retained segment must be locatable by the block name it was allocated under")
      assert(record.length > 0L, "A committed segment must have a length")
      assert(record.offset + record.length <= record.file.length(),
        "A committed segment must lie inside the file it names")
    }

    assert(records.map(record => record.file).distinct.size === 1,
      "One eviction of one partition must write exactly one file")
    records.indices.tail.foreach { index =>
      val previous = records(index - 1)
      val current = records(index)
      assert(previous.sequenceNumber < current.sequenceNumber,
        "Durable records must be retained in ascending sequence order")
      assert(previous.offset + previous.length === current.offset,
        "Individually committed segments must be contiguous and must not overlap")
    }

    payloads.zipWithIndex.foreach { case (payload, index) =>
      val record = records(index)
      assert(record.payloadLength === payload.length,
        "The payload length must be recorded from the block rather than derived from the file")
      val readBack = manager.retainedPayload(0, index.toLong)
      assert(readBack.isDefined, s"Block $index must still be replayable from its spill segment")
      assert(readBack.get.sameElements(payload),
        s"Block $index must read back byte for byte from local disk")
    }
  }

  test("spill reuses the existing block identity namespace and adds no BlockId subtype") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))
    bufferBlocks(manager, 0, 2)
    assert(manager.spill(Long.MaxValue, trigger) > 0L, "The partition must leave memory")

    val records = manager.allSpilledBlocks
    assert(records.nonEmpty, "There must be durable records to inspect")
    records.foreach { record =>
      assert(BlockId(record.blockId.name) === record.blockId,
        s"${record.blockId.name} must round-trip through the existing block identity parser")
      assert(BlockId(record.blockId.name).getClass === classOf[TempShuffleBlockId],
        s"${record.blockId.name} must parse back as a temporary shuffle block")
    }

    intercept[UnrecognizedBlockId] {
      BlockId("streaming_shuffle_0_0_0")
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Cleanup.

  test("task completion releases every buffer and unlinks every spill file") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))
    bufferBlocks(manager, 0, 2)
    assert(manager.spill(Long.MaxValue, trigger) > 0L, "The partition must leave memory")
    bufferBlocks(manager, 1, 2)
    val files = spillFilesOf(manager)
    assert(files.nonEmpty, "There must be spill files to unlink")
    assert(files.forall(file => file.exists()), "Those files must exist before the task completes")
    assert(manager.getUsed() === 2L * blockCharge, "The in-memory blocks must still be charged")
    assert(!manager.spillFilesTransferred, "This consumer must still own its files")

    context.markTaskCompleted(Some(new IllegalStateException("injected task failure")))
    assert(manager.isClosed, "Task completion must close the consumer")
    assert(manager.bufferedBytes === 0L, "Every buffered byte must be released")
    assert(manager.getUsed() === 0L, "Every byte must go back to the task memory manager")
    assert(manager.executorReservedBytes === 0L, "Every byte must go back to the allowance")
    assert(files.forall(file => !file.exists()),
      "A consumer that still owned its spill files must unlink all of them, because the output " +
        "of a failed producer is going away in any case")
    assert(manager.spillFileDeletionFailures === 0L, "No spill file may be left behind on disk")
    assert(!manager.servesRetainedOutput, "A closed owning consumer serves nothing")
  }

  test("cleanup registration is idempotent and completing a closed consumer never throws") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    manager.registerCleanup(context)
    manager.registerCleanup(context)
    bufferBlocks(manager, 3, 1)

    manager.close()
    assert(manager.isClosed, "An explicit close must close the consumer")
    assert(manager.getUsed() === 0L, "An explicit close must release the buffers")

    // A listener that threw here would fail the very task it exists to clean up after, so the
    // listener has to be idempotent as well as unconditional.
    context.markTaskCompleted(None)
    assert(manager.getUsed() === 0L, "Completion after an explicit close must release nothing more")
    assert(manager.bufferedBytes === 0L, "The consumer must remain empty")
    assert(metricsOf(context).memoryBytesSpilled === 0L,
      "Nothing was spilled, so nothing may be reported on the spill accumulators")
    assert(!manager.bufferBlock(0, 0L, payloadOfLength(0L, payloadBytes)),
      "A closed consumer must refuse admission rather than buffer into released memory")
  }

  test("task completion listeners run in reverse registration order") {
    val context = newTrackedTaskContext()
    val observed = new mutable.ArrayBuffer[String]()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    bufferBlocks(manager, 0, 1)

    // Registered after the consumer's own cleanup, so it has to run BEFORE it: listeners are held
    // on a stack, which makes teardown last-in-first-out and therefore makes registration order the
    // thing that decides what is released first.
    context.addTaskCompletionListener[Unit] { _ =>
      val stage = if (manager.isClosed) "buffers-already-released" else "buffers-still-held"
      observed += stage
    }
    context.markTaskCompleted(None)

    assert(observed.toSeq === Seq("buffers-still-held"),
      "The later registration must run first and therefore observe buffers not yet released")
    assert(manager.isClosed, "The earlier registration must still have run")
    assert(manager.getUsed() === 0L, "Both listeners having run, nothing may remain acquired")
  }

  test("handing spill files to the block resolver keeps them past an owning close") {
    val shuffleId = 11
    val mapId = 7L
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))
    val resolver = new StreamingShuffleBlockResolver(streamingConfWithOverrides())
    try {
      bufferBlocks(manager, 0, 2)
      assert(manager.spill(Long.MaxValue, trigger) > 0L, "The partition must leave memory")
      assert(resolver.registerProducer(shuffleId, mapId, attemptIdOf(context), manager),
        "A live producer must be registrable with the resolver")

      val files = manager.releaseSpillFileOwnership()
      assert(files.nonEmpty, "The hand-off must report the files whose deletion it transfers")
      assert(manager.spillFilesTransferred, "Ownership must be recorded as transferred")
      assert(resolver.retainProducerOutput(shuffleId, mapId, attemptIdOf(context), files),
        "The resolver must accept the files of the generation it has registered")

      manager.close()
      assert(manager.isClosed, "The consumer must be closed")
      assert(manager.getUsed() === 0L, "Buffered memory must still be released on close")
      assert(files.forall(file => file.exists()),
        "A detached close must leave every file in place for the resolver to serve")
      assert(manager.servesRetainedOutput,
        "A closed consumer whose files were handed on must go on answering for them")

      assert(resolver.removeShuffle(shuffleId) === 1, "Unregistering the shuffle must drop it")
      assert(files.forall(file => !file.exists()),
        "The resolver must unlink the files it took ownership of when the shuffle is unregistered")
    } finally {
      resolver.stop()
    }
  }

  test("the streaming shuffle namespace publishes exactly the four specified metrics") {
    val published = streamingShuffleMetricNames()
    assert(published === StreamingShuffleTestHelper.MetricNames.toSet,
      s"The namespace must publish exactly the four specified metrics, but published $published")
    assert(published.size === 4, "Exactly four metrics, and no fifth")
    assert(published.contains(StreamingShuffleTestHelper.BufferUtilizationMetricName),
      "The buffer utilisation gauge must be published")
    assert(published.contains(StreamingShuffleTestHelper.SpillCountMetricName),
      "The spill counter must be published")
    assert(StreamingShuffleMetricsSource.sourceName ===
      StreamingShuffleTestHelper.MetricsSourceName,
      "The namespace must be the one the documentation and the operator both expect")

    val sources = streamingShuffleSources(sc.env.metricsSystem)
    assert(sources.size === 1,
      s"The streaming namespace must resolve to exactly one registered source, got ${sources.size}")
    assert(sources.head.sourceName === StreamingShuffleTestHelper.MetricsSourceName,
      "The registered source must carry the streaming shuffle namespace")
  }

  test("the spill counter advances once per eviction event, not per partition or per block") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))
    assert(observedSpillCount() === 0L, "The reset in beforeEach must leave a clean baseline")
    bufferBlocks(manager, 0, 2)
    bufferBlocks(manager, 1, 2)

    val freed = manager.spill(2L * blockCharge + 1L, trigger)
    assert(freed === 4L * blockCharge, "Both candidates must have left memory in whole partitions")
    assert(manager.spillCount === 1L, "Two partitions in one reclamation is one eviction event")
    assert(observedSpillCount() === 1L,
      s"The published counter must advance once per event, but read ${observedSpillCount()}")
    assert(manager.durabilityFlushCount === 0L,
      "A reclamation forced by the budget is not an end-of-stream durability flush")

    assert(manager.memoryBytesSpilled === freed, "The volume that left memory must be reported")
    assert(manager.diskBytesSpilled > 0L, "The volume committed to local disk must be reported")

    assert(manager.lastSpillDurationMs === 0L,
      "Spill latency must be measured on the injected clock rather than on wall time")
  }

  test("the end-of-stream durability flush is counted apart from a pressure spill") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    bufferBlocks(manager, 2, 3)

    val moved = manager.spillAllRetained()
    assert(moved === 3L * blockCharge, "The flush must move every retained byte to local disk")
    assert(manager.durabilityFlushCount === 1L, "The flush must be counted as the event it is")
    assert(manager.spillCount === 0L,
      "A routine end-of-stream flush must not appear in the pressure signal")
    assert(observedSpillCount() === 0L, "Nor may it appear on the published pressure counter")
    assert(manager.diskBytesSpilled > 0L,
      "The bytes really did reach local disk, so the volume is reported as an eviction's is")
    assert(manager.memoryBytesSpilled === moved, "The volume that left memory must be reported")
    assert(manager.bufferedBytes === 0L, "Nothing may be left in memory after a flush")
  }

  test("a block written straight to disk under buffer pressure is counted as a spill event") {
    val partitionId = 3
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(tightMemoryBytes))
    assert(observedSpillCount() === 0L, "The reset in beforeEach must leave a clean baseline")
    val payload = payloadOfLength(7L, payloadBytes)

    assert(manager.admitDurably(partitionId, 0L, payload),
      "A block the allowance cannot hold must still be retained, on local disk")

    assert(manager.spillCount === 1L,
      "A pressure-induced direct-to-disk admission must be counted as one spill event")
    assert(observedSpillCount() === 1L,
      s"The published counter must advance with it, but read ${observedSpillCount()}")
    assert(manager.durableAdmissionCount === 1L,
      "The breakdown must record that the event took the direct-to-disk route")
    assert(manager.durabilityFlushCount === 0L,
      "A pressure admission is not an end-of-stream durability flush")

    val committedBytes = manager.allSpilledBlocks.map(record => record.length).sum
    assert(committedBytes > 0L, "The block must have been committed to local disk")
    assert(manager.diskBytesSpilled === committedBytes,
      "The committed segment must be reported as disk spill volume")
    assert(manager.memoryBytesSpilled === 0L,
      "Nothing left memory, because these bytes were never charged to it")
    assert(manager.bufferedBytes === 0L, "A direct admission must charge the buffer budget nothing")

    assert(manager.retainsBlock(partitionId, 0L), "The block must be retained and servable")
    assert(manager.retainedPayloadLength(partitionId, 0L).contains(payload.length),
      "A directly admitted block must be readable at its full payload length")

    val metrics = metricsOf(context)
    context.markTaskCompleted(None)
    assert(metrics.diskBytesSpilled === committedBytes,
      "The same volume must reach TaskMetrics.diskBytesSpilled")
    assert(metrics.memoryBytesSpilled === 0L,
      "No memory volume may be manufactured by a write that never occupied memory")
  }

  test("spill files contain a bounded number of segments and remain charged until deletion") {
    val diskQuota = new MemorySpillManager.ExecutorDiskQuota(
      () => 64L * 1024L * 1024L, fileLimit = 2)
    val manager = newManager(
      newTrackedTaskContext(),
      newManualClock(),
      newQuota(roomyMemoryBytes),
      diskQuota = Some(diskQuota))
    val blockCount = MemorySpillManager.MAX_SPILL_SEGMENTS_PER_FILE + 1
    bufferBlocks(manager, 0, blockCount)

    assert(manager.spillAllRetained() === blockCount.toLong * blockCharge)
    val records = manager.spilledBlocks(0)
    val files = records.groupBy(_.file)
    assert(files.size === 2,
      "One block beyond the segment ceiling must be written to a second spill file")
    assert(files.values.forall(_.size <= MemorySpillManager.MAX_SPILL_SEGMENTS_PER_FILE),
      "No spill file may contain more than the bounded segment count")
    assert(manager.reservedDiskFiles === 2 && diskQuota.reservedFileCount === 2,
      "Both committed files must remain charged while their retained output is live")
    assert(manager.reservedDiskBytes === files.keys.map(_.length()).sum,
      "The disk quota must reconcile conservative reservations to committed file lengths")

    manager.close()
    assert(diskQuota.reservedFileCount === 0 && diskQuota.reservedBytes === 0L,
      "Deleting task-owned spill files must return both disk quota dimensions")
    assert(files.keys.forall(file => !file.exists()),
      "Closing a store that retained ownership must unlink every bounded spill file")
  }

  test("disk quota refusal rolls back partial batches and signals MemoryPressure") {
    val diskQuota = new MemorySpillManager.ExecutorDiskQuota(
      () => 64L * 1024L * 1024L, fileLimit = 1)
    val manager = newManager(
      newTrackedTaskContext(),
      newManualClock(),
      newQuota(roomyMemoryBytes),
      diskQuota = Some(diskQuota))
    val blockCount = MemorySpillManager.MAX_SPILL_SEGMENTS_PER_FILE + 1
    bufferBlocks(manager, 0, blockCount)
    val retainedBefore = manager.bufferedBytes

    assert(manager.spillAllRetained() === 0L,
      "A file-ceiling refusal must not publish a partially durable retained window")
    assert(manager.diskPressureDetected,
      "The refusal must expose the executor-wide disk-pressure diagnostic")
    assert(manager.memoryPressureDetected,
      "Disk exhaustion is the documented MemoryPressure fallback condition, not a fifth reason")
    assert(manager.memoryPressureEvents > 0L)
    assert(manager.allSpilledBlocks.isEmpty,
      "Every earlier batch must be rolled back when a later bounded file is refused")
    assert(manager.retainedBlockCount(0) === blockCount && manager.bufferedBytes === retainedBefore,
      "Every detached block must return to memory so no output is lost")
    assert(diskQuota.reservedFileCount === 0 && diskQuota.reservedBytes === 0L,
      "Rollback must return the failed batch and every already committed file reservation")
  }

  test("recurring-condition log records are bounded per executor, not per manager instance") {
    // Two managers, which is what an executor running two streaming map tasks has.
    val firstClock = newManualClock()
    val secondClock = newManualClock()
    val firstContext = newTrackedTaskContext()
    val secondContext = newTrackedTaskContext()
    val first = newManager(firstContext, firstClock, newQuota(roomyMemoryBytes))
    val second = newManager(secondContext, secondClock, newQuota(roomyMemoryBytes))
    val flushes = MemorySpillManager.durabilityFlushLogAggregator
    assert(flushes.occurrenceCount === 0L,
      "The reset in beforeEach must leave the executor's aggregation window clean")

    bufferBlocks(first, 0, 2)
    assert(first.spillAllRetained() === 2L * blockCharge,
      "The first task's flush must move every retained byte to local disk")
    assert(flushes.occurrenceCount === 1L, "The first flush must be recorded on the executor")
    assert(flushes.unreportedCount === 0L,
      "The first occurrence is the one that reports, so nothing is yet standing unreported")

    bufferBlocks(second, 0, 3)
    assert(second.spillAllRetained() === 3L * blockCharge,
      "The second task's flush must move every retained byte to local disk")
    assert(flushes.occurrenceCount === 2L,
      "A second manager's flush must be recorded on the SAME executor-wide aggregation")
    assert(flushes.unreportedCount === 1L,
      "Inside the window the second task's flush must be aggregated rather than reported, which " +
        "is exactly what a per-instance gate could not do")
    assert(second.durabilityFlushCount === 1L,
      "Each manager still counts its own flushes; only the reporting is shared")

    assert(flushes.volumeBytes === 5L * blockCharge,
      s"The aggregation must carry both tasks' volume, but carried ${flushes.volumeBytes}")

    secondClock.advance(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)
    bufferBlocks(second, 1, 1)
    assert(second.spillAllRetained() === blockCharge, "The refilled partition must flush again")
    assert(flushes.occurrenceCount === 3L, "The third flush must be recorded")
    assert(flushes.unreportedCount === 0L,
      "A record admitted past the window must clear the occurrences it accounted for")

    val evictions = MemorySpillManager.spillLogAggregator
    assert(evictions.occurrenceCount === 0L,
      "No eviction has occurred, so the eviction aggregation must be untouched by the flushes")
    val trigger = new NoopMemoryConsumer(memoryManagerOf(firstContext))
    bufferBlocks(first, 1, 2)
    assert(first.spill(blockCharge, trigger) > 0L, "The partition must leave memory")
    assert(evictions.occurrenceCount === 1L, "The eviction must be recorded on its own aggregation")
    assert(evictions.unreportedCount === 0L,
      "An independent condition's first occurrence must report, whatever the flushes did")

    MemorySpillManager.resetSharedStateForTesting()
    assert(flushes.occurrenceCount === 0L && flushes.unreportedCount === 0L,
      "The shared-state reset must return the executor's aggregation to its initial state")
    assert(evictions.occurrenceCount === 0L,
      "Every aggregation must be reset, not only the one a test happened to name")
  }

  test("spill latency is measured on the injected clock") {
    val stepMs = 150L
    val context = newTrackedTaskContext()
    val manager = newManager(context, new SteppingClock(clockEpochMs, stepMs),
      newQuota(roomyMemoryBytes))
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))
    bufferBlocks(manager, 0, 2)

    assert(manager.spill(blockCharge, trigger) > 0L, "The partition must leave memory")
    assert(manager.lastSpillDurationMs >= stepMs,
      s"A measurement spanning at least one ${stepMs} ms step must be at least that wide")
    assert(manager.lastSpillDurationMs % stepMs === 0L,
      "The measurement must be a whole number of clock steps, so it came from the injected clock")
  }

  test("buffer utilisation is reported as a percentage of the executor allowance") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    assert(manager.bufferUtilizationPercent === 0L, "An idle producer must report zero utilisation")

    bufferBlocks(manager, 0, 40)
    val expected = manager.executorReservedBytes * percentScale / manager.totalBudgetBytes
    assert(manager.bufferUtilizationPercent === expected,
      "Utilisation must be the executor-wide reservation as a percentage of the whole allowance")
    assert(manager.bufferUtilizationPercent > 0L, "A loaded producer must report a live figure")
    assert(manager.bufferUtilizationPercent < spillPercent.toLong,
      "This fixture must stay below the threshold, so utilisation must read below it too")

    val contributor = installBufferUtilization(blockCharge, blockCharge * 4L)
    try {
      assert(observedBufferUtilizationPercent() === 25L,
        s"A quarter-full allowance must read 25, but the gauge read " +
          s"${observedBufferUtilizationPercent()}")
    } finally {
      removeBufferUtilization(contributor)
    }
  }

  test("the published utilisation gauge is fed by the production executor allowance") {
    assert(StreamingShuffleMetricsSource.bufferUtilizationContributorCount === 0,
      "beforeEach resets the source and discards the shared allowance, so nothing is registered")
    assert(observedBufferUtilizationPercent() === 0L,
      "With no contributor registered the gauge must read zero rather than throw or guess")

    val conf = streamingConfWithOverrides()
    val manager = newProductionManager(newTrackedTaskContext(), conf)
    bufferBlocks(manager, 0, 1)
    assert(StreamingShuffleMetricsSource.bufferUtilizationContributorCount === 1,
      "The production path must register exactly one contributor: the shared executor allowance")

    val quota = MemorySpillManager.executorQuota(conf)
    assert(quota.totalBytes === manager.totalBudgetBytes,
      "The manager and the gauge must measure against one budget rather than two")
    assert(quota.contributedBudgetBytes === manager.totalBudgetBytes,
      "The gauge's denominator must be that same executor-wide budget")
    assert(manager.executorReservedBytes >= blockCharge,
      "A buffered block must be visible in the shared reservation the gauge reads")
    assert(quota.contributedBufferedBytes === manager.executorReservedBytes,
      "The gauge's numerator must be the bytes the executor's producers are actually holding")
    assert(observedBufferUtilizationPercent() === manager.bufferUtilizationPercent,
      "The published gauge and the manager must report one utilisation from one allowance")

    val reservedBefore = quota.reservedBytes
    val utilisationBefore = observedBufferUtilizationPercent()
    val heldBytes = quota.totalBytes / 4L - reservedBefore
    assert(heldBytes > 0L,
      s"The derived allowance of ${quota.totalBytes} bytes must be roomy enough to lend a quarter")
    assert(quota.tryReserve(heldBytes), "A quarter of a near-empty allowance must be reservable")
    try {
      val expected = quota.reservedBytes * percentScale / quota.totalBytes
      assert(observedBufferUtilizationPercent() === expected,
        s"The gauge must report the registered allowance's own arithmetic: expected $expected " +
          s"but read ${observedBufferUtilizationPercent()}")
      assert(observedBufferUtilizationPercent() > utilisationBefore,
        "Lending out a quarter of the allowance must move the published gauge upwards")
      assert(StreamingShuffleMetricsSource.bufferUtilizationContributorCount === 1,
        "Reserving must not add a contributor; the allowance was already the one being counted")
    } finally {
      quota.release(heldBytes)
    }
    assert(observedBufferUtilizationPercent() === utilisationBefore,
      "Releasing must return the gauge to the reservation the producers still hold")

    manager.close()
    MemorySpillManager.resetSharedStateForTesting()
    assert(StreamingShuffleMetricsSource.bufferUtilizationContributorCount === 0,
      "Discarding the shared allowance must unregister it from the gauge")
    assert(observedBufferUtilizationPercent() === 0L,
      "With the allowance withdrawn the gauge must read zero again")
  }

  /**
   * A spill writer that succeeds for a fixed number of blocks and then fails the device.
   *
   * @param failAfterWrites how many raw-byte writes succeed before the device fails
   */
  private class FailingSpillWriter(
      file: java.io.File,
      serializerManager: SerializerManager,
      serializerInstance: SerializerInstance,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetricsReporter,
      blockId: BlockId,
      failAfterWrites: Int)
    extends DiskBlockObjectWriter(file, serializerManager, serializerInstance, bufferSize,
      syncWrites = false, writeMetrics, blockId) {

    private val writesAttempted = new AtomicInteger(0)
    private val reverted = new AtomicBoolean(false)

    override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
      if (writesAttempted.incrementAndGet() > failAfterWrites) {
        throw new IOException(
          s"Simulated device failure writing block ${writesAttempted.get()} of $blockId")
      }
      super.write(kvBytes, offs, len)
    }

    override def revertPartialWritesAndClose(): java.io.File = {
      reverted.set(true)
      super.revertPartialWritesAndClose()
    }

    def wasReverted: Boolean = reverted.get()

    def writeAttempts: Int = writesAttempted.get()
  }

  /**
   * A spill manager whose spill files are written through a device that fails part way through.
   *
   * @param failAfterWrites how many blocks are committed before the device fails
   */
  private class FailingDeviceSpillManager(
      taskMemoryManager: TaskMemoryManager,
      conf: SparkConf,
      clock: Clock,
      quota: MemorySpillManager.ExecutorBufferQuota,
      failAfterWrites: Int)
    extends MemorySpillManager(taskMemoryManager, conf, clock, Some(quota), autoPoll = false) {

    private val writers = new mutable.ArrayBuffer[FailingSpillWriter]()

    override private[streaming] def newSpillWriter(
        blockId: TempShuffleBlockId,
        file: java.io.File,
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

  test("the scoped disk fault owns the directory it breaks and always cleans up after itself") {
    // A test fixture whose failure mode is to leave process-wide state behind is worth testing in
    // its own right, because what it leaves behind is charged to whichever case runs next.
    val injector = new StreamingShuffleFaultInjector(newManualClock())
    try {
      val faultMethods = classOf[StreamingShuffleFaultInjector].getMethods
        .filter(_.getName == "failDiskWrites")
      assert(faultMethods.length === 1,
        "the fixture must offer exactly one way to arm a disk fault, but offers " +
          faultMethods.map(_.toString).mkString(", "))
      assert(faultMethods.head.getParameterCount === 0,
        "arming a disk fault must take no argument, so it can only ever break its own directory, " +
          s"but takes ${faultMethods.head.getParameterCount}")
      val sparkLocalDirs = sc.env.blockManager.diskBlockManager.localDirs.map(_.getCanonicalPath)

      // 2.
      val brokenDirectory = withFailingDiskWrites(injector) { fault =>
        assert(fault.directory.isDirectory,
          s"${fault.directory.getAbsolutePath} must exist for the duration of the scope")
        assert(!sparkLocalDirs.exists(local =>
            fault.directory.getCanonicalPath.startsWith(local)),
          s"${fault.directory.getCanonicalPath} must not be inside one of Spark's own local " +
            s"directories ${sparkLocalDirs.mkString(", ")}")
        assert(fault.isEffective ===
            injector.isArmed(StreamingShuffleFaultScenario.DiskFailureDuringSpill),
          "the scenario must be armed if and only if the permission change took effect")
        if (fault.isEffective) {
          assert(!fault.directory.canWrite,
            "an effective fault must leave its directory unwritable")
          val blocked = new java.io.File(fault.directory, "spill-attempt")
          val created = try {
            blocked.createNewFile()
          } catch {
            case _: java.io.IOException => false
          }
          assert(!created,
            s"an effective fault must make ${blocked.getAbsolutePath} impossible to create")
        }
        fault.directory
      }
      assert(!brokenDirectory.exists(),
        s"${brokenDirectory.getAbsolutePath} must be removed once the scope has ended")
      assert(!injector.isArmed(StreamingShuffleFaultScenario.DiskFailureDuringSpill),
        "the scenario must be disarmed once the scope has ended")

      // 3.
      var directoryFromFailedBody: java.io.File = null
      val raised = intercept[IllegalStateException] {
        withFailingDiskWrites(injector) { fault =>
          directoryFromFailedBody = fault.directory
          throw new IllegalStateException("the body failed part way through")
        }
      }
      assert(raised.getMessage.contains("the body failed part way through"),
        "the body's own failure must propagate rather than being replaced by a cleanup failure")
      assert(directoryFromFailedBody != null && !directoryFromFailedBody.exists(),
        s"${directoryFromFailedBody} must be removed after a body that raised")
      assert(!injector.isArmed(StreamingShuffleFaultScenario.DiskFailureDuringSpill),
        "the scenario must be disarmed after a body that raised")

      // 4.
      val fault = injector.failDiskWrites()
      val reused = fault.directory
      fault.close()
      assert(!reused.exists(), "the first close must remove the directory")
      assert(reused.mkdirs(), "the name must be free for something else to take")
      fault.close()
      assert(reused.isDirectory,
        s"a second close must not remove ${reused.getAbsolutePath} a second time")
      Utils.deleteRecursively(reused)
      assert(!injector.isArmed(StreamingShuffleFaultScenario.DiskFailureDuringSpill),
        "no scenario may remain armed once every fixture has been closed")
    } finally {
      injector.disarmAll()
    }
  }

  test("a spill that fails part way through reverts, deletes and keeps every block servable") {
    val context = newTrackedTaskContext()
    val clock = newManualClock()
    val committedBeforeFailure = 2
    val blockCount = 3
    val manager = new FailingDeviceSpillManager(memoryManagerOf(context),
      streamingConfWithOverrides(), clock, newQuota(roomyMemoryBytes), committedBeforeFailure)
    manager.registerPartitionCount(reducePartitions)
    manager.registerCleanup(context)
    openManagers += manager
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))

    bufferBlocks(manager, 0, blockCount)
    val payloads = (0L until blockCount.toLong).map { sequence =>
      sequence -> manager.retainedPayload(0, sequence).getOrElse(
        fail(s"block $sequence must be buffered before the eviction"))
    }.toMap
    val sequenceClaimBefore = manager.lastAcceptedSequence(0)
    val bufferedBefore = manager.bufferedBytes
    assert(bufferedBefore > 0L, "the partition must hold bytes for the eviction to detach")
    assert(sequenceClaimBefore === (blockCount - 1).toLong,
      s"the partition's sequence claim must stand at ${blockCount - 1} but is $sequenceClaimBefore")

    val reclaimed = manager.spill(Long.MaxValue, trigger)
    assert(reclaimed === 0L,
      s"a failed eviction must report no reclaimed bytes, but reported $reclaimed")

    assert(manager.spillFailureCount === 1L,
      s"exactly one spill failure must be counted, but ${manager.spillFailureCount} were")
    assert(manager.spillCount === 0L,
      s"a failed eviction must not be counted as a spill, but ${manager.spillCount} were")

    val writers = manager.openedWriters
    assert(writers.size === 1, s"exactly one writer must have been opened, not ${writers.size}")
    val writer = writers.head
    assert(writer.writeAttempts === committedBeforeFailure + 1,
      s"the device must have failed on write ${committedBeforeFailure + 1}, but was asked for " +
        s"${writer.writeAttempts}")
    assert(writer.wasReverted,
      "the production rollback must revert the partially written file rather than merely close it")

    assert(spillFilesOf(manager).isEmpty,
      s"no spill record may survive a failed eviction, but ${spillFilesOf(manager).size} did")
    assert(!writer.file.exists(),
      s"the partially written spill file ${writer.file.getAbsolutePath} must be deleted")
    assert(manager.spillFileDeletionFailures === 0L,
      "the rollback's own deletion must not have failed")
    assert(manager.retainedSpillRecordCount === 0,
      s"no spilled record may be published, but ${manager.retainedSpillRecordCount} were")

    payloads.foreach { case (sequence, expected) =>
      assert(manager.retainsBlock(0, sequence),
        s"block $sequence must still be retained after the failed eviction")
      assert(manager.spilledBlock(0, sequence).isEmpty,
        s"block $sequence must not be recorded as spilled after the failed eviction")
      val readBack = manager.retainedPayload(0, sequence)
      assert(readBack.isDefined, s"block $sequence must still be readable")
      assert(java.util.Arrays.equals(expected, readBack.get),
        s"block $sequence read back different bytes after the failed eviction")
    }
    assert(manager.bufferedBytes === bufferedBefore,
      s"every detached byte must be back in memory: $bufferedBefore before the failure, " +
        s"${manager.bufferedBytes} after")
    assert(manager.lastAcceptedSequence(0) === sequenceClaimBefore,
      s"the partition's sequence claim must be restored to $sequenceClaimBefore, but is " +
        manager.lastAcceptedSequence(0))

    val nextSequence = sequenceClaimBefore + 1L
    assert(manager.bufferBlock(0, nextSequence, payloadOfLength(nextSequence, payloadBytes)),
      s"the partition must still admit sequence $nextSequence after the failed eviction")
  }

  test("a spill failure names the block and the failure class, never a path or a stack trace") {
    val context = newTrackedTaskContext()
    val manager = new FailingDeviceSpillManager(memoryManagerOf(context),
      streamingConfWithOverrides(debug = true), newManualClock(), newQuota(roomyMemoryBytes),
      failAfterWrites = 0)
    manager.registerPartitionCount(reducePartitions)
    manager.registerCleanup(context)
    openManagers += manager
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))

    bufferBlocks(manager, 0, 2)
    bufferBlocks(manager, 1, 2)

    val appender = new LogAppender("streaming shuffle spill failure diagnostics")
    withLogAppender(appender) {
      assert(manager.spill(Long.MaxValue, trigger) === 0L,
        "an eviction whose every write fails must report no reclaimed bytes")
    }
    assert(manager.spillFailureCount === 2L,
      s"both partitions must have failed to spill, but ${manager.spillFailureCount} did")
    assert(manager.openedWriters.size === 2,
      s"one writer per partition must have been opened, not ${manager.openedWriters.size}")

    val records = appender.loggingEvents
      .filter(_.getMessage.getFormattedMessage.contains("Failed to evict streaming shuffle"))
    assert(records.size === 2,
      s"both failures must still be reported -- sanitising a diagnostic must not silence it -- " +
        s"but ${records.size} record(s) named the eviction failure")

    records.foreach { record =>
      assert(record.getThrown === null,
        s"no spill-failure record may carry a throwable, yet one carried " +
          s"${Option(record.getThrown).map(_.getClass.getName).getOrElse("none")}")
    }

    val forbidden = manager.openedWriters.flatMap { writer =>
      Seq(writer.file.getAbsolutePath, writer.file.getParent)
    } ++ SparkEnv.get.blockManager.diskBlockManager.localDirs.toSeq.map(_.getAbsolutePath)
    records.foreach { record =>
      val rendered = record.getMessage.getFormattedMessage
      forbidden.foreach { path =>
        assert(!rendered.contains(path),
          s"a spill-failure record disclosed the local path $path: $rendered")
      }
    }

    val blockNames = manager.openedWriters.map(_.file.getName)
    blockNames.foreach { blockName =>
      assert(records.exists(_.getMessage.getFormattedMessage.contains(blockName)),
        s"the destination block $blockName must still be named, or the record identifies nothing")
    }
    assert(records.forall(_.getMessage.getFormattedMessage.contains(classOf[IOException].getName)),
      "and every record must name the failure by class, which is the actionable half of a trace")
  }

  test("a spill failure is never observed on a healthy device and nothing is left behind") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))
    bufferBlocks(manager, 0, 3)
    assert(manager.spill(Long.MaxValue, trigger) > 0L, "The partition must leave memory")

    assert(manager.spillFailureCount === 0L, "A healthy device must not fail a spill")
    assert(manager.spillFileDeletionFailures === 0L, "No spill file may be left undeletable")
    manager.allSpilledBlocks.foreach { record =>
      val readBack = manager.retainedPayload(record.partitionId, record.sequenceNumber)
      assert(readBack.isDefined,
        s"Every published record must be fully readable, but ${record.blockId} was not")
      assert(readBack.get.length === record.payloadLength,
        "A published record must read back at exactly the payload length it recorded")
    }
    assert(manager.retainedSpillRecordCount === 3,
      "Every evicted block must be counted against the retained-record bound")
  }

  test("a block the buffer allowance cannot hold is admitted straight to local disk") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(tightMemoryBytes))
    val allowance = manager.maxAdmissiblePayloadBytes
    val oversized = payloadOfLength(0L, allowance.toInt + 1)
    assert(!manager.bufferBlock(0, 0L, oversized),
      "A payload beyond the per-partition allowance must be refused by the buffering path, or " +
        "the durable path below would not be the only way to admit it")
    manager.clearMemoryPressure()
    val spillsBefore = manager.spillCount
    val memoryBefore = manager.memoryBytesSpilled
    val diskBefore = manager.diskBytesSpilled
    val bufferedBefore = manager.bufferedBytes

    assert(manager.admitDurably(0, 0L, oversized),
      "A block the allowance cannot hold must be admitted straight to disk rather than refused")

    assert(manager.durableAdmissionCount === 1L,
      s"The direct admission must be counted apart from an eviction, but the count was " +
        s"${manager.durableAdmissionCount}")
    assert(manager.spillCount === spillsBefore + 1L,
      s"The allowance could not hold the block and the block reached local disk, which is the " +
        s"same condition an eviction answers, so the pressure signal must advance with it -- but " +
        s"spillCount went from ${spillsBefore} to ${manager.spillCount}")
    assert(manager.memoryBytesSpilled === memoryBefore,
      s"Nothing left memory, so memoryBytesSpilled must not move, but it went from " +
        s"${memoryBefore} to ${manager.memoryBytesSpilled}")
    val committed = manager.spilledBlock(0, 0L).map(_.length).getOrElse(0L)
    assert(committed >= oversized.length.toLong,
      s"The committed segment must be at least the payload it carries -- the disk writer's own " +
        s"wrapping accounts for the difference -- but ${committed} was recorded for " +
        s"${oversized.length} byte(s)")
    assert(manager.diskBytesSpilled === diskBefore + committed,
      s"The bytes really did reach local disk, so diskBytesSpilled must carry exactly the " +
        s"committed segment, but it went from ${diskBefore} to ${manager.diskBytesSpilled} for a " +
        s"segment of ${committed}")
    assert(manager.bufferedBytes === bufferedBefore,
      s"A durably admitted block charges the buffer budget nothing, but the buffered tally went " +
        s"from ${bufferedBefore} to ${manager.bufferedBytes}")

    assert(manager.retainsBlock(0, 0L), "The block must be retained and servable")
    assert(manager.spilledBlock(0, 0L).isDefined,
      "The block must be published in the spilled store, exactly as an evicted one is")
    assert(manager.retainedPayload(0, 0L).map(_.toSeq) === Some(oversized.toSeq),
      "The block must read back byte for byte, or a consumer could not reassemble the partition")
    assert(manager.retainedPayloadLength(0, 0L) === Some(oversized.length),
      "The retained length must be the payload's own length")
    assert(manager.lastAcceptedSequence(0) === 0L,
      s"The sequence must be claimed by the admission, but the run stopped at " +
        s"${manager.lastAcceptedSequence(0)}")
    assert(manager.servesRetainedOutput,
      "A producer holding a durably admitted block serves retained output")

    intercept[IllegalArgumentException] {
      manager.admitDurably(0, 0L, oversized)
    }
    intercept[IllegalArgumentException] {
      manager.admitDurably(0, 2L, oversized)
    }
    assert(manager.durableAdmissionCount === 1L,
      "A refused admission must not be counted as a durable admission")
    assert(manager.retainedBlockCount(0) === 1,
      s"Only the one legitimate block may be retained, but ${manager.retainedBlockCount(0)} were")
  }

  test("a durable admission whose write fails publishes nothing and unclaims its sequence") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    val payload = payloadOfLength(7L, payloadBytes)
    val diskBefore = manager.diskBytesSpilled
    val bufferedBefore = manager.bufferedBytes

    val localDirs = sc.env.blockManager.diskBlockManager.localDirs
    val displaced = localDirs.map(dir => (dir, new File(dir.getPath + ".displaced")))
    try {
      displaced.foreach { case (dir, aside) =>
        assert(dir.renameTo(aside), s"The suite must be able to move ${dir} aside")
        assert(aside.isDirectory, s"${aside} must be the real local directory")
        java.nio.file.Files.write(dir.toPath, Array[Byte](0))
        assert(dir.isFile, s"${dir} must now be a regular file so no subdirectory can exist")
      }

      assert(!manager.admitDurably(0, 0L, payload),
        "A durable admission that cannot reach local disk must report failure rather than " +
          "claiming to have retained anything")
    } finally {
      displaced.foreach { case (dir, aside) =>
        if (dir.isFile) {
          assert(dir.delete(), s"The placeholder at ${dir} must be removable")
        }
        assert(aside.renameTo(dir), s"The real local directory must be restored to ${dir}")
      }
    }
    assert(sc.env.blockManager.diskBlockManager.localDirs.forall(_.isDirectory),
      "Local storage must be intact again before anything else runs")

    assert(manager.spillFailureCount === 0L,
      "The disk guard must refuse before a spill writer is opened, so no write failure is counted")
    assert(manager.diskPressureDetected && manager.memoryPressureDetected,
      "An unusable local directory must route through the documented MemoryPressure condition")
    assert(manager.durableAdmissionCount === 0L,
      "A failed write is not a durable admission and must not be counted as one")
    assert(manager.diskBytesSpilled === diskBefore,
      s"Nothing reached disk, so diskBytesSpilled must not move, but it went from ${diskBefore} " +
        s"to ${manager.diskBytesSpilled}")
    assert(manager.bufferedBytes === bufferedBefore,
      "A failed durable admission must charge the buffer budget nothing")
    assert(!manager.retainsBlock(0, 0L),
      "Nothing may be published for a block whose write failed")
    assert(manager.retainedBlockCount(0) === 0,
      s"No record may be retained, but ${manager.retainedBlockCount(0)} were")
    assert(manager.lastAcceptedSequence(0) === MemorySpillManager.UNSET_SEQUENCE,
      s"The sequence must be unclaimed by the rollback, but the run reported " +
        s"${manager.lastAcceptedSequence(0)}")

    assert(manager.admitDurably(0, 0L, payload),
      "The same block must be admissible again once local storage is healthy")
    assert(manager.durableAdmissionCount === 1L,
      "The retry must be counted exactly once")
    assert(manager.retainedPayload(0, 0L).map(_.toSeq) === Some(payload.toSeq),
      "The retried block must read back byte for byte")
  }

  test("the framing share is sized per task slot") {
    val processors = Runtime.getRuntime.availableProcessors()
    val cases: Seq[(String, SparkConf, Int)] = Seq(
      ("a declared core count", new SparkConf(false).set("spark.executor.cores", "4"), 4),
      ("a declared count with two cpus per task",
        new SparkConf(false).set("spark.executor.cores", "4").set("spark.task.cpus", "2"), 2),
      ("a local master, which leaves the core key at its default",
        new SparkConf(false).set("spark.master", "local[3]"), 3),
      ("a local master with two cpus per task",
        new SparkConf(false).set("spark.master", "local[4]").set("spark.task.cpus", "2"), 2),
      ("both sources, where the larger wins",
        new SparkConf(false).set("spark.master", "local[5]").set("spark.executor.cores", "2"), 5),
      ("a coarse-grained executor, whose true count is unreadable from configuration",
        new SparkConf(false).set("spark.master", "spark://host:7077"), processors),
      ("more cpus per task than cores, which must still leave one slot",
        new SparkConf(false).set("spark.executor.cores", "1").set("spark.task.cpus", "4"), 1))
    cases.foreach { case (description, conf, expected) =>
      assert(MemorySpillManager.executorTaskSlots(conf) === expected,
        s"With $description the divisor must be $expected, but was " +
          s"${MemorySpillManager.executorTaskSlots(conf)}")
    }

    val fourSlots = streamingConfWithOverrides().set("spark.executor.cores", "4")
    val oneSlot = streamingConfWithOverrides().set("spark.executor.cores", "1")
    val wide = newManager(newTrackedTaskContext(), newManualClock(),
      newQuota(roomyMemoryBytes), fourSlots)
    val narrow = newManager(newTrackedTaskContext(), newManualClock(),
      newQuota(roomyMemoryBytes), oneSlot)
    assert(wide.concurrentTaskSlots === 4 && narrow.concurrentTaskSlots === 1,
      s"Each manager must hold the count derived from its own configuration, but they held " +
        s"${wide.concurrentTaskSlots} and ${narrow.concurrentTaskSlots}")
    assert(wide.maxAdmissiblePayloadBytes <= narrow.maxAdmissiblePayloadBytes,
      s"An executor believing it runs four concurrent tasks must not admit a larger block than " +
        s"one believing it runs a single task, but it admitted " +
        s"${wide.maxAdmissiblePayloadBytes} against ${narrow.maxAdmissiblePayloadBytes}")
  }

  test("an eviction is in flight only between detaching and publishing") {
    // The publish stage reads the injected clock while the eviction is still in flight and the
    // monitor is still held, which is the one instant from which the flag can be observed
    // deterministically -- no sleep, no second thread and no race.
    val observations = new mutable.ArrayBuffer[Boolean]()
    val managerRef = new AtomicReference[MemorySpillManager](null)
    val observingClock = new Clock {
      private val current = new AtomicLong(clockEpochMs)
      override def getTimeMillis(): Long = {
        val manager = managerRef.get()
        if (manager != null) {
          observations.synchronized {
            observations += manager.evictionInFlight
          }
        }
        current.get()
      }
      override def nanoTime(): Long = TimeUnit.MILLISECONDS.toNanos(current.get())
      override def waitTillTime(targetTime: Long): Long = current.get()
    }
    val context = newTrackedTaskContext()
    val manager = newManager(context, observingClock, newQuota(roomyMemoryBytes))
    managerRef.set(manager)
    bufferBlocks(manager, 0, 3)
    assert(!manager.evictionInFlight,
      "No eviction may be in flight before one is asked for")
    observations.synchronized(observations.clear())

    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))
    assert(manager.spill(Long.MaxValue, trigger) > 0L, "The partition must leave memory")

    assert(observations.synchronized(observations.contains(true)),
      s"The flag must read true while the reclamation is between its detach and publish stages, " +
        s"but every reading inside it was false: " +
        s"${observations.synchronized(observations.mkString("[", ", ", "]"))}")
    assert(!manager.evictionInFlight,
      "The flag must be cleared by the publish stage, so a later producer is not made to wait " +
        "for an eviction that has finished")
    assert(manager.spillCount === 1L,
      s"Exactly one eviction event may be counted, but ${manager.spillCount} were")
  }

  test("concurrent reclamation accounts every byte exactly once") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    val partitions = 4
    val blocksPerPartition = 6
    (0 until partitions).foreach(partitionId =>
      bufferBlocks(manager, partitionId, blocksPerPartition))
    val totalBlocks = partitions * blocksPerPartition
    val chargedBytes = totalBlocks.toLong * blockCharge
    assert(manager.bufferedBytes === chargedBytes,
      s"The fixture must have charged ${chargedBytes} bytes, but held ${manager.bufferedBytes}")

    val racers = (1 to 3).map { index =>
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          var attempts = 0
          while (attempts < 50 && manager.bufferedBytes > 0L) {
            attempts += 1
            manager.maybeSpill()
          }
        }
      }, s"streaming-shuffle-spill-racer-$index")
      thread.setDaemon(true)
      thread
    }
    racers.foreach(_.start())
    val flushed = manager.spillAllRetained()
    racers.foreach(_.join(TimeUnit.SECONDS.toMillis(60L)))
    racers.foreach(thread =>
      assert(!thread.isAlive, s"${thread.getName} must have finished rather than being left alive"))

    assert(manager.bufferedBytes === 0L,
      s"Every buffered byte must have left memory, but ${manager.bufferedBytes} remained")
    assert(manager.memoryBytesSpilled === chargedBytes,
      s"Exactly the bytes that were charged may be reported as spilled from memory -- no more, " +
        s"which would be a double count, and no less -- but ${manager.memoryBytesSpilled} of " +
        s"${chargedBytes} was reported")
    assert(flushed + manager.bufferedBytes <= chargedBytes,
      s"No reclamation may report freeing more than was ever charged, but the durability flush " +
        s"alone reported ${flushed} of ${chargedBytes}")
    val payloadsOnDisk = manager.allSpilledBlocks.map(_.payloadLength.toLong).sum
    val committedOnDisk = manager.allSpilledBlocks.map(_.length).sum
    assert(payloadsOnDisk === totalBlocks.toLong * payloadBytes.toLong,
      s"Every payload must reach disk exactly once, but ${payloadsOnDisk} byte(s) of payload " +
        s"recorded for ${totalBlocks} block(s) of ${payloadBytes}")
    assert(manager.diskBytesSpilled === committedOnDisk,
      s"Disk accounting must be exactly the committed segments, with nothing counted twice, but " +
        s"${manager.diskBytesSpilled} was reported against ${committedOnDisk} committed")
    assert(manager.retainedSpillRecordCount === totalBlocks,
      s"Every block must be retained exactly once, but ${manager.retainedSpillRecordCount} " +
        s"record(s) exist for ${totalBlocks} block(s)")
    assert(manager.spillFailureCount === 0L,
      s"No reclamation may have failed, but ${manager.spillFailureCount} did")
    (0 until partitions).foreach { partitionId =>
      assert(manager.retainedBlockCount(partitionId) === blocksPerPartition,
        s"Partition $partitionId must retain ${blocksPerPartition} block(s), but retained " +
          s"${manager.retainedBlockCount(partitionId)}")
      (0 until blocksPerPartition).foreach { sequence =>
        assert(manager.retainsBlock(partitionId, sequence.toLong),
          s"Partition $partitionId sequence $sequence must still be servable")
      }
    }
    assert(!manager.evictionInFlight,
      "No eviction may be left in flight once every racer has finished")
  }

  test("every block admission guard refuses without changing any state") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    bufferBlocks(manager, 0, 1)
    val payload = payloadOfLength(1L, payloadBytes)
    val oversized = payloadOfLength(2L, maxBlockPayloadBytes + 1)

    val bufferedBefore = manager.bufferedBytes
    val reservedBefore = manager.executorReservedBytes
    val retainedBefore = manager.retainedBlockCount(0)
    val cursorBefore = manager.lastAcceptedSequence(0)
    val diskBefore = manager.diskBytesSpilled
    val filesBefore = spillFilesOf(manager).size
    val durableBefore = manager.durableAdmissionCount
    val spillsBefore = manager.spillCount

    val refusals: Seq[(String, () => Any)] = Seq(
      ("a negative partition id offered to the buffering path", () => manager.bufferBlock(-1, 1L,
        payload)),
      ("a negative sequence number offered to the buffering path", () => manager.bufferBlock(0, -1L,
        payload)),
      ("a null payload offered to the buffering path", () => manager.bufferBlock(0, 1L, null)),
      ("an empty payload offered to the buffering path", () => manager.bufferBlock(0, 1L,
        Array.emptyByteArray)),
      ("a payload past the protocol's block size offered to the buffering path",
        () => manager.bufferBlock(0, 1L, oversized)),
      ("a negative partition id offered to the durable path", () => manager.admitDurably(-1, 1L,
        payload)),
      ("a negative sequence number offered to the durable path", () => manager.admitDurably(0, -1L,
        payload)),
      ("a null payload offered to the durable path", () => manager.admitDurably(0, 1L, null)),
      ("an empty payload offered to the durable path", () => manager.admitDurably(0, 1L,
        Array.emptyByteArray)),
      ("a payload past the protocol's block size offered to the durable path",
        () => manager.admitDurably(0, 1L, oversized)),
      ("a non-positive partition count", () => manager.registerPartitionCount(0)),
      ("a negative partition count", () => manager.registerPartitionCount(-1)),
      ("a partition count past the tracked ceiling",
        () => manager.registerPartitionCount(MemorySpillManager.MAX_TRACKED_PARTITIONS + 1)),
      ("a partition count conflicting with the one already registered",
        () => manager.registerPartitionCount(reducePartitions + 1)),
      ("a non-positive divisor for the per-partition budget",
        () => manager.perPartitionBudgetBytesFor(0)),
      ("a null task context for cleanup registration", () => manager.registerCleanup(null)),
      ("a null spill file offered for a lease", () => manager.acquireSpillFileLease(null)),
      ("a null spill file offered for a lease release", () => manager.releaseSpillFileLease(null)))

    refusals.foreach { case (description, attempt) =>
      intercept[IllegalArgumentException] {
        attempt()
      }
    }

    assert(manager.bufferedBytes === bufferedBefore,
      s"No refused call may charge the buffer budget, but the tally went from ${bufferedBefore} " +
        s"to ${manager.bufferedBytes}")
    assert(manager.executorReservedBytes === reservedBefore,
      s"No refused call may charge the executor's shared allowance, but it went from " +
        s"${reservedBefore} to ${manager.executorReservedBytes}")
    assert(manager.retainedBlockCount(0) === retainedBefore,
      s"No refused call may retain a block, but the count went from ${retainedBefore} to " +
        s"${manager.retainedBlockCount(0)}")
    assert(manager.lastAcceptedSequence(0) === cursorBefore,
      s"No refused call may advance a partition's sequence cursor, but it went from " +
        s"${cursorBefore} to ${manager.lastAcceptedSequence(0)}")
    assert(manager.diskBytesSpilled === diskBefore && spillFilesOf(manager).size === filesBefore,
      "No refused call may write or allocate a spill file")
    assert(manager.durableAdmissionCount === durableBefore && manager.spillCount === spillsBefore,
      "No refused call may advance a spill or durable-admission counter")
    assert(manager.numPartitions === reducePartitions,
      s"A refused partition count must leave the registered one in place, but the manager " +
        s"reports ${manager.numPartitions}")
    assert(!manager.memoryPressureDetected,
      "A malformed call is a programming error rather than memory pressure, so it must not raise " +
        "the signal that stands streaming down")
  }

  test("every consumer and acknowledgement guard refuses without changing any state") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    assert(manager.registerConsumer(consumerId), "A live producer must admit a consumer")
    bufferBlocks(manager, 0, 2)

    val bufferedBefore = manager.bufferedBytes
    val consumersBefore = manager.registeredConsumers
    val refusalsBefore = manager.rejectedAcknowledgementCount
    val positionBefore = manager.consumerPosition(consumerId, 0)

    val refusals: Seq[(String, () => Any)] = Seq(
      ("a null consumer identity offered for registration", () => manager.registerConsumer(null)),
      ("an empty consumer identity offered for registration", () => manager.registerConsumer("")),
      ("a null consumer identity offered for unregistration",
        () => manager.unregisterConsumer(null)),
      ("an empty consumer identity offered for unregistration",
        () => manager.unregisterConsumer("")),
      ("a null consumer identity offered for an acknowledgement",
        () => manager.acknowledge(null, 0, 0L)),
      ("an empty consumer identity offered for an acknowledgement",
        () => manager.acknowledge("", 0, 0L)),
      ("a negative partition id offered for an acknowledgement",
        () => manager.acknowledge(consumerId, -1, 0L)))

    refusals.foreach { case (description, attempt) =>
      intercept[IllegalArgumentException] {
        attempt()
      }
    }

    assert(manager.registeredConsumers === consumersBefore,
      s"No refused call may change the registered consumers, but they went from " +
        s"${consumersBefore.mkString("[", ", ", "]")} to " +
        s"${manager.registeredConsumers.mkString("[", ", ", "]")}")
    assert(manager.bufferedBytes === bufferedBefore,
      s"No refused acknowledgement may release memory, but the tally went from ${bufferedBefore} " +
        s"to ${manager.bufferedBytes}")
    assert(manager.consumerPosition(consumerId, 0) === positionBefore,
      s"No refused acknowledgement may advance a consumer's position, but it went from " +
        s"${positionBefore} to ${manager.consumerPosition(consumerId, 0)}")
    assert(manager.rejectedAcknowledgementCount === refusalsBefore,
      s"A malformed acknowledgement is refused at the boundary and is not the refused-in-range " +
        s"case the counter records, but the counter went from ${refusalsBefore} to " +
        s"${manager.rejectedAcknowledgementCount}")
    assert(manager.lastAcceptedSequence(0) === 1L,
      s"No refused call may disturb the admitted run, which must still end at 1, but it ended at " +
        s"${manager.lastAcceptedSequence(0)}")
  }
}
