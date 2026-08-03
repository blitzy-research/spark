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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.collection.mutable

import org.scalatest.matchers.should.Matchers

import org.apache.spark.{SharedSparkContext, SparkConf, SparkFunSuite, SparkIllegalArgumentException, TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.{SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_SPILL_THRESHOLD}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.storage.{BlockId, TempShuffleBlockId, UnrecognizedBlockId}
import org.apache.spark.util.Clock

/**
 * Unit coverage of [[MemorySpillManager]], the bounded and spillable buffer budget that the
 * streaming shuffle producer holds its unacknowledged window in.
 *
 * Four behaviours are contracted for this component and every one of them is time sensitive: the
 * 100 ms threshold polling cadence, the order in which buffered partitions are evicted, the 100 ms
 * bound a consumer acknowledgement's reclamation is held to, and the spill accounting that has to
 * land on Spark's own task accumulators rather than on counters of its own. Nothing in this suite
 * sleeps and nothing waits on wall time: every timing assertion is made by injecting a clock the
 * test body controls, which is what makes a 100 ms cadence assertable in microseconds and -- the
 * part a sleeping test cannot do at all -- lets the suite prove that a timer does NOT fire one
 * millisecond early.
 *
 * Two fixtures carry the whole suite, and both derive the buffer allowance from an injected
 * executor-memory figure rather than from the host JVM's heap. That is deliberate: the production
 * budget is a percentage of the executor's on-heap unified memory region, so a suite that let the
 * real region decide would assert against whatever heap the test runner happened to be given.
 * Passing an explicit figure makes every arithmetic assertion below exact on any machine.
 *
 *  - the roomy fixture has enough allowance that admission never refuses, which is what the
 *    eviction-order, accounting, plumbing and lifecycle cases need;
 *  - the tight fixture admits exactly one block per partition and reaches the spill trigger on the
 *    last of them, so the threshold is met with no slack to reason about.
 *
 * Every manager is built with the executor's shared threshold ticker withheld. The ticker is a
 * daemon thread that drives the very method these tests drive by hand, and leaving it running would
 * make the cadence a race rather than a measurement.
 *
 * The zero-leak requirement is machine checked rather than asserted by inspection. Each test gets
 * its own task memory manager, and `afterEach` closes the consumer and then asks that memory
 * manager how much execution memory the task still holds: a non-zero answer is a leak in the
 * component under test. This is the same property the test JVM enforces globally through
 * `spark.unsafe.exceptionOnMemoryLeak`, made local so that a failure names the test that caused it.
 */
class MemorySpillManagerSuite
  extends SparkFunSuite
    with SharedSparkContext
    with Matchers
    with StreamingShuffleTestHelper {

  // The two injected executor-memory figures. Chosen so that the derived budgets are exact
  // integers: the production arithmetic divides by a hundred before multiplying by the percentage,
  // so a figure that is a clean multiple of a hundred loses nothing to truncation.
  //
  // Roomy: 4,000,000 -> allowance 800,000 bytes, spill trigger 640,000, per-partition 100,000.
  private val roomyMemoryBytes: Long = 4000000L

  // Tight: 59,500 -> allowance 11,900 bytes (ten block charges), spill trigger 9,520 (eight of
  // them), per-partition 1,487 (one). The eighth admitted block therefore meets the threshold
  // exactly, which is the cheapest possible way to arm it.
  private val tightMemoryBytes: Long = 59500L

  private val reducePartitions: Int = 8

  private val payloadBytes: Int = 1024

  private val bufferPercent: Int = StreamingShuffleTestHelper.DefaultBufferSizePercent

  private val spillPercent: Int = StreamingShuffleTestHelper.DefaultSpillThresholdPercent

  private val percentScale: Long = StreamingShuffleTestHelper.PercentScale

  private val pollIntervalMs: Long = StreamingShuffleTestHelper.SpillPollIntervalMillis

  private val reclamationDeadlineMs: Long = StreamingShuffleTestHelper.ReclamationDeadlineMillis

  private val clockEpochMs: Long = StreamingShuffleTestHelper.ManualClockEpochMillis

  private val maxBlockPayloadBytes: Int = StreamingShuffleTestHelper.MaxBlockSizeBytes

  // What one test block actually costs the budget. Never the bare payload length: a block is
  // charged its payload plus the per-block overhead, and that charge is the figure every accounting
  // path in the component reserves, releases and reports.
  private val blockCharge: Long =
    payloadBytes.toLong + MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES

  private val consumerId: String = "streaming-shuffle-consumer"

  // Consumers and task memory managers opened by the test currently running. Cleared in beforeEach
  // and drained in afterEach, so one test can never be charged for another's memory.
  private val openManagers = new mutable.ArrayBuffer[MemorySpillManager]()

  private val openMemoryManagers = new mutable.ArrayBuffer[TaskMemoryManager]()

  private var taskAttemptCounter: Long = 0L

  override def beforeEach(): Unit = {
    super.beforeEach()
    // The metrics source is a JVM singleton whose counters outlive a test, and the executor-wide
    // buffer allowance is likewise process scoped. Both are returned to a known state here: a
    // counter assertion that started from whatever ran before it is exactly the order dependence
    // the zero-flakiness gate exists to prevent.
    resetStreamingShuffleMetrics()
    MemorySpillManager.resetSharedStateForTesting()
    openManagers.clear()
    openMemoryManagers.clear()
  }

  override def afterEach(): Unit = {
    try {
      // Closing is what hands buffered bytes back to the task memory manager, and the figure below
      // is the machine check that it really did. `cleanUpAllAllocatedMemory` answers with the
      // execution memory the task still held, so a non-zero answer is a leak rather than a
      // shortcoming of the assertion.
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

  // -----------------------------------------------------------------------------------------------
  // Fixtures.
  // -----------------------------------------------------------------------------------------------

  /**
   * A buffer allowance over an injected executor-memory figure.
   *
   * @param executorMemoryBytes size of the memory region the allowance is carved from
   * @param bufferSizePercent share of that region the allowance occupies
   * @param spillThresholdPercent share of the allowance at which eviction is triggered
   * @return the allowance, unregistered with any metrics gauge and unshared with any other test
   */
  private def newQuota(
      executorMemoryBytes: Long,
      bufferSizePercent: Int = bufferPercent,
      spillThresholdPercent: Int = spillPercent): MemorySpillManager.ExecutorBufferQuota = {
    new MemorySpillManager.ExecutorBufferQuota(
      bufferSizePercent, spillThresholdPercent, () => executorMemoryBytes)
  }

  /**
   * A task context carrying a memory manager of its own, tracked so that `afterEach` can prove the
   * consumer built on it released everything it took.
   *
   * @return the context, with a task attempt id unique within this suite
   */
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

  /**
   * The memory manager of a task context, taken through the interface that declares the accessor so
   * that the call site does not depend on how the implementation happens to expose it.
   *
   * @param context the context to read
   * @return that task's memory manager
   */
  private def memoryManagerOf(context: TaskContext): TaskMemoryManager = context.taskMemoryManager()

  /**
   * The task metrics of a task context, taken through the interface that declares the accessor.
   *
   * @param context the context to read
   * @return that task's metrics, carrying the accumulators the streaming path reports on
   */
  private def metricsOf(context: TaskContext): TaskMetrics = context.taskMetrics()

  /**
   * The attempt identifier of a task context, taken through the interface that declares it. This is
   * the generation a producer registers its spill files under.
   *
   * @param context the context to read
   * @return that task's attempt identifier
   */
  private def attemptIdOf(context: TaskContext): Long = context.taskAttemptId()

  /**
   * A spill manager over an injected allowance, with the executor's shared threshold ticker
   * withheld so the polling cadence stays the test body's to drive.
   *
   * @param context the task the consumer's memory and cleanup belong to
   * @param clock the time source every cadence, ordering and latency assertion is made against
   * @param quota the allowance to draw on
   * @param conf the configuration to read the streaming keys from
   * @return the manager, already told its reduce partition count and registered for cleanup
   */
  private def newManager(
      context: TaskContextImpl,
      clock: Clock,
      quota: MemorySpillManager.ExecutorBufferQuota,
      conf: SparkConf = streamingConfWithOverrides()): MemorySpillManager = {
    val manager = new MemorySpillManager(
      memoryManagerOf(context), conf, clock, Some(quota), autoPoll = false)
    manager.registerPartitionCount(reducePartitions)
    manager.registerCleanup(context)
    openManagers += manager
    manager
  }

  /**
   * Buffers blocks into one partition, continuing that partition's gap-free ascending run.
   *
   * @param manager the consumer to admit into
   * @param partitionId the reduce partition to admit into
   * @param count how many blocks to admit
   */
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

  /**
   * Every spill file a consumer currently retains, de-duplicated.
   *
   * @param manager the consumer to inspect
   * @return the distinct backing files of its retained spill records
   */
  private def spillFilesOf(manager: MemorySpillManager): Seq[java.io.File] =
    manager.allSpilledBlocks.map(record => record.file).distinct

  /**
   * A memory consumer whose only purpose is to be a spill trigger other than the manager itself.
   *
   * [[MemorySpillManager.spill]] declines a reclamation request it triggered itself -- the guard
   * Spark's own spillable collections use, so that a consumer cannot recurse into its own eviction
   * mid-reservation. A suite that wants the callback to do work therefore has to hand it a
   * different consumer, which is what this is. It never holds memory and never spills anything.
   *
   * @param memoryManager the task memory manager to register against
   */
  private class NoopMemoryConsumer(memoryManager: TaskMemoryManager)
    extends MemoryConsumer(memoryManager, MemoryMode.ON_HEAP) {

    override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
  }

  /**
   * A spill manager that records what its inherited memory operations were asked to do.
   *
   * Two clauses of the `MemoryConsumer` contract are only observable from inside those operations.
   * The first is that `spill` must never call `acquireMemory`, which the interface documents as a
   * deadlock rather than merely discouraging. The second is that `acquireMemory` may grant strictly
   * LESS than it was asked for, which the production code has to surface as memory pressure instead
   * of quietly tolerating. Both inherited methods are public and non-final, so overriding them is
   * what makes each clause assertable without altering one line of the component under test.
   *
   * @param memoryManager the task memory manager to reserve from
   * @param conf the configuration to read the streaming keys from
   * @param clock the injected time source
   * @param quota the buffer allowance to draw on
   */
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

    /**
     * Caps what every subsequent acquisition may obtain, which is how a partial grant is produced
     * without having to exhaust the host JVM's heap.
     *
     * @param bytes the most any single acquisition may be granted
     */
    def capGrantsAt(bytes: Long): Unit = grantCeiling.set(bytes)

    /** How many times the inherited acquisition was called, from anywhere. */
    def acquisitionCount: Int = acquisitions.get()

    /** How many of those calls came from inside the spill callback. Must always be zero. */
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
   * A frozen manual clock proves that a duration is measured on the injected time source, because
   * the measurement must then come out at exactly zero. It cannot prove the converse -- that a real
   * overrun is detected -- because a test body cannot advance a clock in the middle of a
   * synchronous call it is itself making. Stepping on read closes that gap with neither a sleep nor
   * a second thread: any measurement spanning two or more readings is then at least one step wide.
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

  // -----------------------------------------------------------------------------------------------
  // Configuration and budget arithmetic. The five streaming keys are CONSUMED through their typed
  // entries here and never re-declared: each key may be declared exactly once in the JVM, and the
  // validators that police their documented ranges belong to those declarations.
  // -----------------------------------------------------------------------------------------------

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
      // The validator belongs to the typed entry, so the value is refused when it is READ rather
      // than when it is set. The condition is the one Spark already has for a configuration
      // requirement: the streaming shuffle adds no catalogue entry of its own for this.
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
    // Configuration is read once, at construction, and held immutably. That is what makes
    // "configuration changes require an executor restart" true by construction, and it is also why
    // an out-of-range value can never reach the buffer arithmetic.
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

    val expectedTotal = roomyMemoryBytes / percentScale * bufferPercent
    assert(manager.totalBudgetBytes === expectedTotal,
      s"The aggregate allowance must be $bufferPercent percent of the injected region")
    assert(manager.spillThresholdBytes === expectedTotal / percentScale * spillPercent,
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

  // -----------------------------------------------------------------------------------------------
  // The 100 ms threshold polling cadence. Driven entirely by advancing an injected manual clock, so
  // every assertion below is exact and none of them takes a hundred milliseconds to make.
  // -----------------------------------------------------------------------------------------------

  test("threshold polling evaluates buffer utilisation on a 100 ms cadence") {
    val clock = newManualClock()
    val manager = newManager(newTrackedTaskContext(), clock, newQuota(tightMemoryBytes))

    // The tight allowance admits exactly one block per partition, so filling every partition puts
    // the executor's reservation exactly on the spill trigger with nothing left over.
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

    // The first poll of a manager's life is always due, because no poll has been taken yet.
    assert(manager.pollOnce(), "The first poll taken at the threshold must evict")
    assert(manager.spillCount === 1L, "One poll at the threshold is one eviction event")
    assert(manager.executorReservedBytes < manager.spillThresholdBytes,
      "An eviction must take the reservation back below the trigger it fired at")

    // Exactly one partition was evicted, and it is the lowest identifier: every partition held one
    // block of identical size and was last touched at the same frozen instant, so the footprint and
    // the access stamp both tie and the identifier is what makes the order total.
    val drained = (0 until reducePartitions).filter(id => manager.bufferedBytesFor(id) == 0L)
    assert(drained === Seq(0),
      s"A tie in footprint and access time must be broken by ascending partition id, but " +
        s"the drained partitions were $drained")

    // Back to the threshold, continuing the drained partition's sequence run.
    bufferBlocks(manager, drained.head, 1)
    assert(manager.executorReservedBytes === manager.spillThresholdBytes,
      "The refill must put the reservation back on the trigger")

    // Not due: no time at all has passed since the poll above.
    assert(!manager.pollOnce(), "A poll taken with no time elapsed must be suppressed")
    assert(manager.spillCount === 1L, "A suppressed poll must not evict")

    // Still not due one millisecond short of the interval. This is the assertion that a sleeping
    // test cannot make at all, and it is the difference between measuring a cadence and hoping.
    clock.advance(pollIntervalMs - 1L)
    assert(!manager.pollOnce(), "A poll one millisecond short of the cadence must be suppressed")
    assert(manager.spillCount === 1L, "A suppressed poll must not evict")

    // Due on the interval exactly.
    clock.advance(1L)
    assert(manager.pollOnce(), "A poll on the cadence boundary must evaluate the threshold")
    assert(manager.spillCount === 2L, "The second due poll at the threshold is a second event")
  }

  test("maybeSpill evaluates the threshold immediately, ignoring the poll cadence") {
    val clock = newManualClock()
    val manager = newManager(newTrackedTaskContext(), clock, newQuota(tightMemoryBytes))
    (0 until reducePartitions).foreach(partitionId => bufferBlocks(manager, partitionId, 1))

    // Consume the cadence, then arm the threshold again without letting the clock move.
    assert(manager.pollOnce(), "The first poll at the threshold must evict")
    val drained = (0 until reducePartitions).filter(id => manager.bufferedBytesFor(id) == 0L)
    bufferBlocks(manager, drained.head, 1)
    assert(!manager.pollOnce(), "The cadence gate must suppress a poll taken too soon")
    assert(manager.spillCount === 1L, "A suppressed poll must not evict")

    // The cadence gates pollOnce and nothing else: an owner that needs the threshold evaluated now
    // has a way to say so, which is what lets an admission needing room act without waiting.
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

  // -----------------------------------------------------------------------------------------------
  // Eviction order: largest buffered footprint first, ties broken least recently used first and
  // then by ascending partition id. The order is published, so it is asserted directly rather than
  // inferred from which partitions happen to end up on disk.
  // -----------------------------------------------------------------------------------------------

  test("eviction considers the largest buffered partitions first") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    // Distinct footprints, so the footprint alone decides and neither tie-break can interfere.
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

    // Reading a block back counts as a use of its partition, because a partition being actively
    // retransmitted is a poor thing to evict. The order must therefore flip.
    clock.advance(pollIntervalMs)
    assert(manager.retainedPayload(6, 0L).isDefined,
      "A buffered block must be readable back for retransmission")
    assert(manager.spillSelectionOrder === Seq(2, 6),
      "Reading a partition must move it to the back of the eviction order")
  }

  test("eviction breaks footprint and access-time ties by ascending partition id") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    // The clock never moves, so every partition carries the same access stamp and the same
    // footprint. The identifier is the only discriminator left, and it is what makes the order
    // total and therefore reproducible from one run to the next.
    Seq(7, 1, 4).foreach(partitionId => bufferBlocks(manager, partitionId, 2))

    assert(manager.spillSelectionOrder === Seq(1, 4, 7),
      "A tie in both footprint and access time must be broken by ascending partition id")
    assert(manager.spillSelectionOrder === manager.spillSelectionOrder,
      "The published order must be stable across reads, so a diagnostic cannot perturb it")
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

    // Ask for one block's worth. Whole partitions are evicted, so the first candidate goes in full
    // and nothing behind it is touched.
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

  test("the spill callback declines a reclamation it triggered itself") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    bufferBlocks(manager, 0, 2)

    // The guard Spark's own spillable collections use: a consumer must not recurse into its own
    // eviction while it is mid-reservation.
    assert(manager.spill(Long.MaxValue, manager) === 0L,
      "A self-triggered reclamation must be declined rather than served")
    assert(manager.bufferedBytes === 2L * blockCharge, "A declined request must free nothing")
    assert(manager.spillCount === 0L, "A declined request is not an eviction event")
  }

  // -----------------------------------------------------------------------------------------------
  // Reclamation latency. An acknowledgement is what frees producer memory, and it is contracted to
  // do so within 100 ms. The bound is measured on the injected clock, so a frozen clock has to
  // produce exactly zero and a clock that moves has to produce a whole number of its own steps.
  // -----------------------------------------------------------------------------------------------

  test("an acknowledgement reclaims buffered memory within the 100 ms bound") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    assert(manager.registerConsumer(consumerId), "A live producer must admit a consumer")
    bufferBlocks(manager, 4, 3)
    assert(manager.bufferedBytes === 3L * blockCharge, "Three blocks must be held in memory")
    assert(manager.executorReservedBytes === manager.bufferedBytes,
      "Held bytes and reserved bytes must agree, so admission and the allowance cannot drift")

    // Acknowledging through sequence one retires a prefix of exactly two blocks and no more.
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

    // A third block, still in memory, so the acknowledgement spans both halves of the window.
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

    // A fast consumer cannot free a block a slower sibling has yet to receive, so retirement waits.
    assert(manager.acknowledge(consumerId, 0, 1L) === 0L,
      "Retirement must not advance past the slowest registered consumer")
    assert(manager.bufferedBytes === 2L * blockCharge, "Nothing may be released while one lags")
    assert(manager.consumerPosition(consumerId, 0) === Some(1L),
      "The fast consumer's own position must still be recorded")
    assert(manager.consumerPosition(slowConsumerId, 0).isEmpty,
      "The slow consumer must have acknowledged nothing")

    // Once the laggard catches up the whole prefix becomes retirable in one step.
    assert(manager.acknowledge(slowConsumerId, 0, 1L) === 2L * blockCharge,
      "The last consumer to confirm receipt is the one whose acknowledgement releases the memory")
    assert(manager.bufferedBytes === 0L, "The confirmed prefix must be released in full")
  }

  // -----------------------------------------------------------------------------------------------
  // Spill accounting. Volumes have to land on the accumulators Spark already has, because those are
  // what the web UI, the history server, the event log and the metrics REST API read. A parallel
  // counter would make the streaming path invisible to every one of them.
  // -----------------------------------------------------------------------------------------------

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
    // Nothing reaches the accumulators until the consumer is released, because the report is made
    // once at closure rather than on every eviction. Reporting per eviction is what would put a
    // per-record cost on the data path.
    assert(metrics.memoryBytesSpilled === 0L, "Nothing may be reported before the consumer closes")

    context.markTaskCompleted(None)
    assert(manager.isClosed, "Task completion must close the consumer")
    assert(metrics.memoryBytesSpilled === manager.memoryBytesSpilled,
      "Spilled memory must be reported on TaskMetrics.memoryBytesSpilled")
    assert(metrics.diskBytesSpilled === manager.diskBytesSpilled,
      "Committed bytes must be reported on TaskMetrics.diskBytesSpilled")
    assert(metrics.peakExecutionMemory === manager.peakMemoryBytes,
      "The peak reservation must be reported on TaskMetrics.peakExecutionMemory")

    // Closing again must not double count. These are cumulative accumulators, so a second report
    // would overstate every surface that reads them.
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

    // The three accumulators the streaming path does report on are the ones that already exist.
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

    // The disk writer reports bytes, records and write time to whatever reporter it is handed, so a
    // spill that reused the task's own shuffle-write reporter would count spilled bytes a second
    // time as shuffle-written bytes. A freshly allocated reporter is what keeps the two apart.
    val writeMetrics = metricsOf(context).shuffleWriteMetrics
    assert(writeMetrics.bytesWritten === 0L,
      "A spill must not be counted as shuffle output: it is a buffer eviction, not a map write")
    assert(writeMetrics.recordsWritten === 0L, "A spill must not inflate shuffle records written")
    assert(writeMetrics.writeTime === 0L, "A spill must not inflate shuffle write time")
    assert(metricsOf(context).diskBytesSpilled > 0L,
      "The spilled volume must still be visible, on the accumulator that means exactly that")
  }

  // -----------------------------------------------------------------------------------------------
  // The two MemoryConsumer clauses that are only observable from inside the inherited operations:
  // spill must never acquire, and a partial grant must be surfaced rather than tolerated.
  // -----------------------------------------------------------------------------------------------

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
    // The interface documents acquiring from inside the callback as a deadlock: the task memory
    // manager runs the callback with its own monitor held, so a thread that reached back for memory
    // there would wait on the monitor it is already holding against.
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
    // The allocator now grants strictly less than a block costs, which is exactly what
    // `MemoryConsumer.acquireMemory` is documented as being allowed to do and what Spark's own
    // spillable collections treat as the signal to spill.
    manager.capGrantsAt(blockCharge - 1L)
    assert(!manager.bufferBlock(0, 0L, payloadOfLength(0L, payloadBytes)),
      "A block that cannot be fully reserved must be refused outright, never half admitted")
    assert(manager.memoryPressureDetected,
      "A partial grant is the memory-pressure trip condition and has to reach the fallback policy")
    assert(manager.memoryPressureEvents >= 1L, "The occurrence must be counted, not only flagged")

    // Nothing was retained and nothing leaked: the partial grant went straight back, and the
    // sequence run was left unclaimed so the block the producer legitimately retries is not then
    // rejected as a duplicate.
    assert(manager.bufferedBytes === 0L, "A refused admission must retain nothing")
    assert(manager.executorReservedBytes === 0L,
      "The reservation must be returned to the executor-wide allowance")
    assert(manager.getUsed() === 0L, "The partial grant must be handed back to the memory manager")
    assert(manager.retainedBlockCount(0) === 0, "No record of the refused block may survive")
    assert(manager.lastAcceptedSequence(0) === MemorySpillManager.UNSET_SEQUENCE,
      "A rolled back reservation must leave the partition's sequence run unclaimed")

    // Clearing re-arms the signal for a policy that has already acted on it, without erasing the
    // cumulative history that same policy may still audit.
    manager.clearMemoryPressure()
    assert(!manager.memoryPressureDetected, "Clearing must re-arm the sticky signal")
    assert(manager.memoryPressureEvents >= 1L, "Clearing must not erase the cumulative tally")

    // With the cap lifted the very same block is admitted, so the refusal really was the budget's
    // doing and not a defect in the block.
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
    // One byte more can never be made to fit by any amount of eviction, so it is refused rather
    // than retried forever, and the refusal raises the signal that routes the shuffle to fallback.
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
    // A gap or a duplicate would let a later acknowledgement retire bytes that were never charged,
    // or charge one block twice, so the gap-free ascending run is enforced rather than documented.
    bufferBlocks(manager, 0, 1)
    intercept[IllegalArgumentException] {
      manager.bufferBlock(0, 2L, payloadOfLength(2L, payloadBytes))
    }
    intercept[IllegalArgumentException] {
      manager.bufferBlock(0, 0L, payloadOfLength(0L, payloadBytes))
    }
    assert(manager.retainedBlockCount(0) === 1, "Only the one legitimate block may be retained")
  }

  // -----------------------------------------------------------------------------------------------
  // Spill plumbing. Files come from the disk block manager as temporary SHUFFLE blocks and are
  // written through the block manager's own disk writer, so the sync-write setting, the serializer
  // manager wiring and the compression codec are all the sanctioned ones.
  // -----------------------------------------------------------------------------------------------

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
      // A temporary SHUFFLE block, never a temporary LOCAL one: these bytes may be read back over
      // the shuffle transport, so their compression has to be governed by spark.shuffle.compress
      // rather than by the spill-specific codec.
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

    // One file for one partition eviction, with every block committed on its own so the segments
    // are contiguous, non-overlapping and each independently decodable.
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

    // Byte-exactness on the way back proves both halves of the sanctioned path were used: a writer
    // constructed directly would not be wrapped by the block manager's serializer manager, and the
    // bytes read back through it would then not be the block that was spilled.
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
      // The identity round-trips through the existing parser, which recognises only the block kinds
      // that already exist. The hierarchy is sealed, so a new kind could not be declared outside
      // its own file, and this is the check that none was smuggled in by name either.
      assert(BlockId(record.blockId.name) === record.blockId,
        s"${record.blockId.name} must round-trip through the existing block identity parser")
      assert(BlockId(record.blockId.name).getClass === classOf[TempShuffleBlockId],
        s"${record.blockId.name} must parse back as a temporary shuffle block")
    }

    // A name outside the existing namespace is not parseable, which is what "no new subtype" means
    // in practice: the block manager's storage interface contract is left exactly as it was.
    intercept[UnrecognizedBlockId] {
      BlockId("streaming_shuffle_0_0_0")
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Cleanup. The consumer registers release on task completion, which runs on success, on failure
  // and on cancellation alike -- and is the mechanism by which the zero-leak requirement is met.
  // -----------------------------------------------------------------------------------------------

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

    // A failed task completes too, and the listener is registered precisely so that the release
    // happens whatever the outcome.
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
    // Registered defensively more than once, which must install one listener rather than three: a
    // second release would drive the memory manager's balance negative.
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

      // The producing task ends. Its buffered memory cannot survive that, but its spill files must,
      // because a consumer subscribing afterwards is exactly who they exist for.
      manager.close()
      assert(manager.isClosed, "The consumer must be closed")
      assert(manager.getUsed() === 0L, "Buffered memory must still be released on close")
      assert(files.forall(file => file.exists()),
        "A detached close must leave every file in place for the resolver to serve")
      assert(manager.servesRetainedOutput,
        "A closed consumer whose files were handed on must go on answering for them")

      // The resolver unlinks at shuffle unregistration, which is one of the boundaries the feature
      // specifies -- rather than at the arbitrary moment a producing task happened to finish.
      assert(resolver.removeShuffle(shuffleId) === 1, "Unregistering the shuffle must drop it")
      assert(files.forall(file => !file.exists()),
        "The resolver must unlink the files it took ownership of when the shuffle is unregistered")
    } finally {
      resolver.stop()
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Telemetry. Spill frequency, volume and latency all have to be reported, and the reporting has
  // to stay off the data path: one lock-free increment per eviction EVENT, never one per partition
  // and never one per record, which is how the sub-one-percent telemetry budget is met.
  // -----------------------------------------------------------------------------------------------

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

    // Reached the way the metrics system itself reaches it, so what is proven is that registration
    // works rather than merely that the object exists.
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

    // Ask for more than the first candidate holds, so one event evicts two partitions and four
    // blocks between them. A counter advanced per partition would read two here, and one advanced
    // per block would read four.
    val freed = manager.spill(2L * blockCharge + 1L, trigger)
    assert(freed === 4L * blockCharge, "Both candidates must have left memory in whole partitions")
    assert(manager.spillCount === 1L, "Two partitions in one reclamation is one eviction event")
    assert(observedSpillCount() === 1L,
      s"The published counter must advance once per event, but read ${observedSpillCount()}")
    assert(manager.durabilityFlushCount === 0L,
      "A reclamation forced by the budget is not an end-of-stream durability flush")

    // Volume, on the component's own tallies as well as on the accumulators.
    assert(manager.memoryBytesSpilled === freed, "The volume that left memory must be reported")
    assert(manager.diskBytesSpilled > 0L, "The volume committed to local disk must be reported")

    // Latency, measured on the injected clock: a held clock has to produce exactly zero.
    assert(manager.lastSpillDurationMs === 0L,
      "Spill latency must be measured on the injected clock rather than on wall time")
  }

  test("the end-of-stream durability flush is counted apart from a pressure spill") {
    val manager = newManager(newTrackedTaskContext(), newManualClock(), newQuota(roomyMemoryBytes))
    bufferBlocks(manager, 2, 3)

    val moved = manager.spillAllRetained()
    assert(moved === 3L * blockCharge, "The flush must move every retained byte to local disk")
    assert(manager.durabilityFlushCount === 1L, "The flush must be counted as the event it is")
    // Every successful streaming map task ends with a flush. Counting it as a spill would show an
    // operator a spill on every healthy task at a fraction of a percent of the budget, and the
    // pressure signal would then measure nothing at all.
    assert(manager.spillCount === 0L,
      "A routine end-of-stream flush must not appear in the pressure signal")
    assert(observedSpillCount() === 0L, "Nor may it appear on the published pressure counter")
    assert(manager.diskBytesSpilled > 0L,
      "The bytes really did reach local disk, so the volume is reported as an eviction's is")
    assert(manager.memoryBytesSpilled === moved, "The volume that left memory must be reported")
    assert(manager.bufferedBytes === 0L, "Nothing may be left in memory after a flush")
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

    // The published gauge is computed on read from whatever allowances are registered with it, so a
    // contributor installed here reports exactly what an operator would observe over JMX.
    val contributor = installBufferUtilization(blockCharge, blockCharge * 4L)
    try {
      assert(observedBufferUtilizationPercent() === 25L,
        s"A quarter-full allowance must read 25, but the gauge read " +
          s"${observedBufferUtilizationPercent()}")
    } finally {
      removeBufferUtilization(contributor)
    }
  }

  test("a spill failure is never observed on a healthy device and nothing is left behind") {
    val context = newTrackedTaskContext()
    val manager = newManager(context, newManualClock(), newQuota(roomyMemoryBytes))
    val trigger = new NoopMemoryConsumer(memoryManagerOf(context))
    bufferBlocks(manager, 0, 3)
    assert(manager.spill(Long.MaxValue, trigger) > 0L, "The partition must leave memory")

    assert(manager.spillFailureCount === 0L, "A healthy device must not fail a spill")
    assert(manager.spillFileDeletionFailures === 0L, "No spill file may be left undeletable")
    // Only committed segments are ever published, which is the observable consequence of rolling a
    // partially written file back rather than serving anything out of it.
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
}
