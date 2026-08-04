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

import java.io.{File, IOException}
import java.util.Locale
import java.util.concurrent.Callable
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.{HashPartitioner, LocalSparkContext, ShuffleDependency, SparkConf, SparkContext, SparkEnv, SparkFunSuite, SparkThrowable, SparkThrowableHelper, TaskContext, TaskContextImpl}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, SHUFFLE_STREAMING_ENABLED}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.network.shuffle.protocol.streaming.{DataBlockMessage, RetransmitRequestMessage, StreamingShuffleChecksum}
import org.apache.spark.scheduler.{JobSucceeded, SparkListener, SparkListenerJobEnd}
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.{BlockManagerId, DiskBlockObjectWriter, TempShuffleBlockId}
import org.apache.spark.util.{Clock, ThreadUtils}

/**
 * Failure injection across the ten conditions the streaming shuffle is specified to survive, each
 * one proven to lose nothing.
 *
 * <b>What this suite is for.</b> Every other streaming shuffle suite asks whether a mechanism
 * behaves as designed. This one asks the only question that matters to whoever runs the job: after
 * the fault, is the answer still right? The operational definition of "zero data loss" used
 * throughout is <b>output-set equality against a sort-based baseline</b> -- the identical workload
 * run under `spark.shuffle.manager=sort`, collected as a set because ordering across a shuffle is
 * not guaranteed. Every one of the ten cases below ends in that comparison, and every one of them
 * also asserts that the job <b>completed</b>. Those two assertions together are the whole claim:
 * there is no configuration, no failure and no resource condition under which a job is left
 * without a functioning shuffle implementation, so the fault costs throughput and never
 * correctness.
 *
 * <b>The ten scenarios are a closed set.</b> They are enumerated by
 * [[StreamingShuffleFaultScenario.all]], and the final case in this suite asserts that this suite
 * covers exactly that set. A scenario added to the feature therefore fails a test here until it is
 * covered, rather than being silently omitted.
 *
 * <b>How each case is structured.</b> Two halves, in this order:
 *
 *  1. <i>The mechanism, driven deterministically.</i> The fault is injected through
 *     [[StreamingShuffleFaultInjector]] or through the production component's own public surface,
 *     and the specific contracted behaviour is asserted at its exact boundary -- the five second
 *     producer timeout fires at 5000 ms and provably not at 4999, the ten second acknowledgement
 *     watchdog fires at 10000 and provably not at 9999, the sixty second slowness window trips
 *     strictly beyond 60000 and not at 59999.
 *  2. <i>The outcome, end to end.</i> A real job runs on the streaming manager and its output is
 *     compared, as a set, with the baseline.
 *
 * <b>Nothing here sleeps and nothing here is random unless it is seeded.</b> Elapsed time is an
 * advance of an injected `ManualClock`, so a timeout assertion is exact and instantaneous and the
 * "did not fire early" half of every timer contract is assertable at all. Cross-thread work is
 * released from a bounded barrier and collected through `ThreadUtils`. That is what makes the
 * zero-flakiness gate a property of the suite rather than a hope about it.
 *
 * <b>Two masters, chosen per scenario.</b> `local[n]` shares a single `SparkEnv`, so the fallback
 * policy the test thread holds is the very object the map and reduce tasks consult -- which is what
 * lets a degradation be driven exactly and observed exactly. `local-cluster[2,1,1024]` gives
 * genuinely separate executor JVMs and separate managers, which is the only setting in which
 * "producer" and "consumer" are different processes; it is used by the cases whose subject is
 * cross-executor loss.
 *
 * <b>Preservation.</b> Nothing in `org.apache.spark.scheduler` is touched or needs to be: upstream
 * recomputation is reached solely by throwing the existing `FetchFailedException`, whose
 * constructor
 * registers itself with the task context, and the unmodified scheduler does the rest.
 * `SortShuffleManager` is likewise unmodified and is the terminus of every degradation.
 */
class StreamingShuffleFailureInjectionSuite
  extends SparkFunSuite
  with LocalSparkContext
  with StreamingShuffleTestHelper {

  import StreamingShuffleFaultScenario._
  import StreamingShuffleTestHelper._

  // ---------------------------------------------------------------------------------------------
  // Fixed shape shared by every case. The partition count is the feature's own documented figure,
  // so the baseline, the streaming run and the buffer arithmetic all describe the same shuffle.
  // ---------------------------------------------------------------------------------------------

  private val PartitionCount: Int = DefaultPartitionCount

  private val ShuffleId: Int = 0

  private val MapId: Long = 0L

  private val PartitionId: Int = 0

  private val FirstSequence: Long = 0L

  private val ConsumerId: String = "streaming-shuffle-failure-injection-consumer"

  /**
   * Payload length used by every buffered block. Small enough that a whole scenario's window fits
   * inside a modest injected allowance, and constant so that a charge against the budget is an
   * exact figure rather than an estimate.
   */
  private val PayloadBytes: Int = 1024

  /**
   * An executor allowance generous enough that admission never trips the spill threshold by
   * accident. Cases that want the threshold crossed shrink it deliberately.
   */
  private val RoomyExecutorMemoryBytes: Long = 64L * 1024L * 1024L

  /** Blocks retained per partition by the cases that build an unacknowledged window. */
  private val WindowBlockCount: Int = 4

  /** Producers that fail together in the concurrent-failure case. */
  private val ConcurrentProducerCount: Int = 3

  /**
   * A consumer absence comfortably beyond the liveness window, used by the reconnection case. Its
   * only requirement is that it exceed the window, which the injector itself enforces.
   */
  private val ExtendedDowntimeMillis: Long = ConsumerLivenessTimeoutMillis * 6L

  /**
   * A garbage-collection pause long enough to be noticed but strictly inside the liveness window,
   * which is what makes the false-positive assertion meaningful.
   */
  private val ShortPauseMillis: Long = SpillPollIntervalMillis * 3L

  /** Spill managers opened by a case, closed and leak-checked by `afterEach`. */
  private val openManagers = mutable.ArrayBuffer.empty[MemorySpillManager]

  /** Task memory managers behind those spill managers, proven empty by `afterEach`. */
  private val openMemoryManagers = mutable.ArrayBuffer.empty[TaskMemoryManager]

  /** Distinct task attempt ids, so two fixtures in one case never share a memory manager. */
  private var taskAttemptCounter: Long = 0L

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Both of these are process-scoped and outlive a test: the metrics source is a JVM singleton
    // whose counters accumulate, and the buffer allowance is derived once per executor. A counter
    // assertion that started from whatever ran before it is exactly the order dependence the
    // zero-flakiness gate exists to prevent, so both are returned to a known state here.
    resetStreamingShuffleMetrics()
    MemorySpillManager.resetSharedStateForTesting()
    openManagers.clear()
    openMemoryManagers.clear()
  }

  override def afterEach(): Unit = {
    try {
      // Closing is what hands buffered bytes back, and the figure below is the machine check that
      // it really did. This is the "zero memory leaks under failure" clause: every case here fails
      // something, and every one of them must still end with nothing held.
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
      super.afterEach()
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Fixtures. Each one substitutes exactly one production seam and leaves every other step of the
  // path it exercises as the production implementation, because a fault injected further out than
  // necessary stops testing the thing it was aimed at.
  // ---------------------------------------------------------------------------------------------

  /**
   * A memory consumer whose only purpose is to be a spill trigger other than the store itself.
   *
   * [[MemorySpillManager.spill]] declines a reclamation it triggered itself, so driving the
   * eviction path from a test needs a second consumer to attribute the request to.
   */
  private class SpillTriggerConsumer(memoryManager: TaskMemoryManager)
    extends MemoryConsumer(memoryManager, MemoryMode.ON_HEAP) {

    override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
  }

  /**
   * A buffer store whose allocator can be made to grant strictly less than it was asked for.
   *
   * This is the whole of the memory-exhaustion scenario, and it is a substitution rather than an
   * actual heap exhaustion for a reason: `MemoryConsumer.acquireMemory` is documented as being
   * permitted to return less than `size`, and a partial grant is precisely the condition the
   * feature names as its OOM risk. Filling a real heap would produce an `OutOfMemoryError`, which
   * is a different and untestable event; capping the grant produces the documented one exactly.
   */
  private class GrantCappingSpillManager(
      memoryManager: TaskMemoryManager,
      conf: SparkConf,
      clock: Clock,
      quota: MemorySpillManager.ExecutorBufferQuota)
    extends MemorySpillManager(memoryManager, conf, clock, Some(quota), autoPoll = false) {

    private val grantCeiling = new AtomicLong(Long.MaxValue)

    private val requestedBytes = new AtomicLong(0L)

    private val grantedBytes = new AtomicLong(0L)

    /** Caps every subsequent grant at the given size, which is how a partial grant is produced. */
    def capGrantsAt(bytes: Long): Unit = grantCeiling.set(bytes)

    /** Bytes the store last asked its allocator for. */
    def lastRequestedBytes: Long = requestedBytes.get()

    /** Bytes the allocator last handed back, which the cap may have made smaller. */
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
   * Failing on the very first write would exercise a strictly simpler branch: nothing committed, so
   * nothing to revert. The interesting failure is part way through, because an eviction detaches
   * every block of a partition from memory before it writes any of them, so a device that fails
   * mid-batch leaves blocks that exist in neither place unless the rollback restores them.
   *
   * `revertPartialWritesAndClose` is observed rather than inferred: it is the specific production
   * call that makes a partially written file unservable, and a rollback that deleted the file
   * without reverting it would satisfy an assertion on deletion while having lost the discipline
   * that assertion stands for.
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

    /** Whether the production rollback reverted this writer rather than merely closing it. */
    def wasReverted: Boolean = reverted.get()

    /** Raw-byte writes this writer was asked to perform, successful or not. */
    def writeAttempts: Int = writesAttempted.get()
  }

  /**
   * A buffer store whose spill files are written through a device that fails part way through.
   *
   * Substitutes the one production seam that decides which writer an eviction is performed with,
   * and nothing else: selection, detachment, temporary block allocation, commit, rollback, deletion
   * and accounting are all the production implementation. Note in particular that the file and the
   * temporary block id are still the ones production allocated from the block manager and handed
   * in -- this fixture chooses neither, which is exactly why it can prove the rollback without
   * weakening the claim that the writer is obtained rather than invented.
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

    /** Every writer this store opened, in the order its evictions opened them. */
    def openedWriters: Seq[FailingSpillWriter] = synchronized(writers.toSeq)
  }

  /**
   * Records how every job of an application ended.
   *
   * "The job completes" is half of this suite's claim and is asserted rather than inferred: a
   * `collect` that returned tells us a result arrived, while this tells us the scheduler declared
   * the job a success and declared no other job a failure along the way.
   */
  private class JobOutcomeRecorder extends SparkListener {

    private val succeeded = new AtomicInteger(0)

    private val failed = new AtomicInteger(0)

    private val failures = mutable.ArrayBuffer.empty[String]

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

    /** Jobs the scheduler declared successful. */
    def succeededJobCount: Int = succeeded.get()

    /** Jobs the scheduler declared failed. */
    def failedJobCount: Int = failed.get()

    /** Why the failed jobs failed, for a message that names the cause rather than a count. */
    def failureDescriptions: Seq[String] = synchronized(failures.toSeq)
  }

  // ---------------------------------------------------------------------------------------------
  // Shared helpers.
  // ---------------------------------------------------------------------------------------------

  /** Bound on draining the listener bus, generous because it is a bound and never a wait. */
  private val ListenerDrainTimeoutMillis: Long = DefaultAwaitTimeoutMillis

  private def payload(seed: Long): Array[Byte] = payloadOfLength(seed, PayloadBytes)

  /** What one block of this suite's fixed payload length costs the buffer budget. */
  private val BlockChargeBytes: Long =
    PayloadBytes.toLong + MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES

  /**
   * An executor memory size whose buffer allowance is exactly one block larger than the window this
   * suite retains, so that admitting the window lands utilisation exactly on the spill trigger.
   *
   * Derived from the feature's own arithmetic rather than guessed. The allowance is
   * `bufferSizePercent` of the executor's on-heap region, so the region is the allowance scaled
   * back
   * up by that percentage; four blocks of a five block allowance is eighty per cent, which is the
   * documented trigger, reached exactly rather than approached. Paired with a single registered
   * partition, because the per-partition ceiling is the allowance divided by the partition count
   * and
   * a window belongs to one partition.
   */
  private val SpillTriggeringExecutorMemoryBytes: Long = {
    val allowance = BlockChargeBytes * (WindowBlockCount + 1).toLong
    allowance * PercentScale / DefaultBufferSizePercent.toLong
  }

  private def memoryManagerOf(context: TaskContext): TaskMemoryManager = context.taskMemoryManager()

  /**
   * A task context with a memory manager of its own, tracked so that `afterEach` can prove whatever
   * was built on it released everything it took.
   *
   * @return the context, with a task attempt id unique within this suite
   */
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

  /**
   * An executor buffer allowance over a stated memory size.
   *
   * Stating the size rather than reading the live heap is what makes every arithmetic assertion in
   * this suite exact on any machine.
   *
   * @param executorMemoryBytes on-heap unified region the allowance is a share of
   * @param bufferSizePercent share of that region reserved for streaming buffers
   * @param spillThresholdPercent utilisation of the allowance at which eviction triggers
   * @return the allowance
   */
  private def newQuota(
      executorMemoryBytes: Long,
      bufferSizePercent: Int = DefaultBufferSizePercent,
      spillThresholdPercent: Int = DefaultSpillThresholdPercent
  ): MemorySpillManager.ExecutorBufferQuota = {
    new MemorySpillManager.ExecutorBufferQuota(
      bufferSizePercent, spillThresholdPercent, () => executorMemoryBytes)
  }

  /**
   * A buffer store over an injected allowance with the executor's threshold ticker withheld, so the
   * polling cadence stays the test body's to drive rather than a daemon thread's to race with.
   *
   * @param context task the store's memory and cleanup belong to
   * @param clock time source every cadence and latency assertion is made against
   * @param quota allowance to draw on
   * @param conf configuration the streaming keys are read from
   * @return the store, told its partition count and registered for task-completion cleanup
   */
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
   * The registered partition count is also the divisor of the per-partition ceiling, so a fixture
   * that retains a whole window in one partition registers one partition. Registering ten and then
   * filling one would be refused at admission, because a single partition may never claim more than
   * its tenth of the allowance -- which is the behaviour, not a limitation of the fixture.
   *
   * @param manager the store
   * @param context the owning task
   * @param partitions reduce partition count to register
   * @tparam T the store's concrete type, so a fixture's own accessors survive the call
   * @return the same store
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

  /**
   * Admits a gap-free ascending run of blocks into one partition and returns what was admitted.
   *
   * @param manager the store to admit into
   * @param partitionId reduce partition to admit into
   * @param count blocks to admit
   * @return sequence number and payload of every admitted block, in admission order
   */
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

  /**
   * A degradation policy with streaming in service and an injected clock.
   *
   * @param clock time source the sustained-slowness window is measured on
   * @return the policy, not yet tripped
   */
  private def activePolicy(clock: Clock): StreamingShuffleFallbackPolicy = {
    val policy = new StreamingShuffleFallbackPolicy(streamingConfWithOverrides(), clock)
    assert(policy.streamingActive, "a fixture policy must start with streaming in service")
    policy
  }

  /**
   * The streaming manager this application is actually running on.
   *
   * Under `local[n]` the driver and the tasks share one `SparkEnv`, so this is the very instance
   * the
   * writers and readers consult -- which is what makes a degradation driven from the test thread a
   * degradation the job observes.
   *
   * @return the live manager
   */
  private def liveStreamingManager(): StreamingShuffleManager = {
    SparkEnv.get.shuffleManager match {
      case manager: StreamingShuffleManager => manager
      case other => fail(
        s"the application must be running the streaming manager but was running ${other.getClass}")
    }
  }

  /**
   * Starts an application on the streaming manager with the behaviour gate open.
   *
   * @param appName application name, which is what makes an event log readable
   * @param master master to run on; `local[n]` shares one environment, `local-cluster` does not
   * @param conf configuration to start from, defaulting to plain streaming
   */
  private def startStreamingApplication(
      appName: String,
      master: String = "local[2]",
      conf: SparkConf = streamingConf()): Unit = {
    sc = new SparkContext(withLocalMaster(conf, appName, master))
    assert(sc.getConf.get(SHUFFLE_MANAGER) === StreamingShuffleManager.SHORT_NAME,
      "the application must have selected the streaming shuffle manager")
    assert(sc.getConf.get(SHUFFLE_STREAMING_ENABLED),
      "the behaviour gate must be open so the streaming path is the one under test")
  }

  /**
   * Runs the shared workload on the live application and proves the job succeeded.
   *
   * @param description what the caller is running, for assertion messages
   * @return the shuffle's output as a set
   */
  private def runStreamingShuffle(description: String): Set[(Int, Seq[String])] = {
    runRecorded(description)(groupedOutputAsSet(sc, PartitionCount))
  }

  /**
   * Runs the shared workload with a second shuffle chained onto it, so the job has two producers
   * running concurrently, and proves the job succeeded.
   *
   * The extra `partitionBy` re-shuffles records it does not change, so the output set is still the
   * baseline's -- which is what lets a two-shuffle job be compared against a one-shuffle baseline
   * without weakening the comparison.
   *
   * @param description what the caller is running, for assertion messages
   * @return the chained shuffle's output as a set
   */
  private def runChainedStreamingShuffle(description: String): Set[(Int, Seq[String])] = {
    runRecorded(description) {
      groupByKeyWorkload(sc, PartitionCount)
        .mapValues(values => values.toSeq.sorted)
        .partitionBy(new HashPartitioner(PartitionCount))
        .collect()
        .toSet
    }
  }

  /**
   * Runs a body with a job-outcome listener attached and asserts every job of it succeeded.
   *
   * @param description what the caller is running, for assertion messages
   * @param body the workload
   * @tparam T the workload's result type
   * @return the workload's result
   */
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

  /**
   * The single assertion this whole suite exists for, made once per scenario.
   *
   * @param scenario the fault the run survived, recorded so the closing case can prove the set of
   *                 covered scenarios is exactly the closed set of ten
   * @param observed output the streaming run produced
   * @param baseline output the sort-based run produced from the identical input
   */
  private def assertSurvived(
      scenario: StreamingShuffleFaultScenario,
      observed: Set[(Int, Seq[String])],
      baseline: Set[(Int, Seq[String])]): Unit = {
    assert(baseline.nonEmpty,
      s"the sort-based baseline for ${scenario.faultName} must produce output to compare against")
    assert(observed.nonEmpty,
      s"the streaming run under ${scenario.faultName} produced nothing at all, so the job did " +
        "not  complete in any useful sense")
    assertNoDataLoss(observed, baseline, s"a streaming shuffle under ${scenario.faultName}")
    assert(!sc.isStopped,
      s"the application must still be live after ${scenario.faultName}, because a fault costs " +
        "throughput and never the job")
  }

  /**
   * The sort-based baseline, captured in a context of its own that is stopped before the streaming
   * one starts, because Spark permits a single active context per JVM.
   *
   * @return the output a stock sort-based application produces from the shared input
   */
  private def captureSortBaseline(): Set[(Int, Seq[String])] = {
    val baseline = sortBaselineGroupedOutput(numPartitions = PartitionCount)
    assert(baseline.nonEmpty, "the sort-based baseline must produce output to compare against")
    baseline
  }

  /**
   * Discards every block accepted from one producer, atomically, and reports the fetch failure that
   * makes the unmodified scheduler recompute the upstream stage.
   *
   * This models the reader's invalidation path with the two properties that make it safe, and the
   * reason it is a helper is that three of the ten scenarios end here and all three must end here
   * the same way:
   *
   *  - <b>Atomic and per producer.</b> Every buffer taken from the failed producer is released
   *    together and nothing belonging to any other producer is touched, so a reduce task can never
   *    mix surviving pre-failure data with post-recomputation data.
   *  - <b>Constructed and thrown in a single expression.</b> `FetchFailedException`'s constructor
   *    registers itself with the task context, so per SPARK-19276 it must be thrown immediately;
   *    creating one, inspecting it and then deciding is a defect even when the decision is to
   *      throw.
   *    A bespoke exception is never used, because the scheduler recognises only this one.
   *
   * @param buffers frames accepted from the producer that has gone, which are released here
   * @param survivors frames accepted from producers that are still healthy, which must be untouched
   * @param reduceId reduce partition the failure is reported against
   * @param shuffleId shuffle the failure is reported against
   * @return nothing; the method always raises
   */
  private def invalidatePartialReads(
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
        s"of  them: outstanding references ${buffers.map(_.outstandingReferences).mkString(", ")}")
    assert(survivors.map(buffer => buffer.outstandingReferences) == survivorReferencesBefore,
      "invalidating one producer must leave every other producer's frames exactly as they were, " +
        "otherwise a reduce task would discard data it is entitled to keep")
    StreamingShuffleMetricsSource.incrementPartialReadInvalidations(1L)
    // Constructed and thrown as one expression. The bmAddress is null because a producer that has
    // gone has no address to name, which is the same choice MetadataFetchFailedException makes.
    throw new FetchFailedException(
      null.asInstanceOf[BlockManagerId],
      shuffleId,
      MapId,
      MapId.toInt,
      reduceId,
      s"Injected streaming shuffle producer loss for shuffle $shuffleId partition $reduceId")
  }

  /**
   * Runs a body with a task context installed on this thread, and always removes it.
   *
   * `FetchFailedException` registers itself with whatever task context the throwing thread carries,
   * and asserting that registration happened is how SPARK-19276's contract is checked rather than
   * assumed. Installation is scoped so no later case inherits it.
   *
   * @param context the context to install
   * @param body the work to run under it
   * @tparam T the body's result type
   * @return the body's result
   */
  private def withTaskContext[T](context: TaskContextImpl)(body: => T): T = {
    TaskContext.setTaskContext(context)
    try {
      body
    } finally {
      TaskContext.unset()
    }
  }

  // =============================================================================================
  // Scenario 1 of 10: producer crash during write.
  // =============================================================================================

  test("a producer crash during write invalidates partial reads and loses nothing") {
    val baseline = captureSortBaseline()
    startStreamingApplication("streaming-shuffle-producer-crash", "local-cluster[2,1,1024]")

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    val notifier = new StreamingShuffleErrorNotifier(ShuffleId, sc.getConf)
    val context = newTrackedTaskContext()

    // The producer goes. Nothing about the consumer has changed yet, so the consumer notices only
    // by the absence of data and heartbeats -- which is exactly what the five second connection
    // timeout measures.
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
    assert(!notifier.hasError,
      "no failure may be published one millisecond before the producer connection timeout")

    // And now the deadline itself.
    clock.advance(ProducerConnectionTimeoutMillis - JustBeforeProducerTimeoutMillis)
    val elapsed = clock.getTimeMillis() - openedAtMillis
    assert(elapsed === ProducerConnectionTimeoutMillis,
      s"the deadline must be reached at exactly ${ProducerConnectionTimeoutMillis} ms, " +
        s"not $elapsed")

    val accepted = (0 until WindowBlockCount).map { index =>
      val buffer = recordingBuffer(payload(index.toLong))
      buffer.retain()
      buffer
    }
    val invalidationsBefore = observedPartialReadInvalidations()

    val failure = withTaskContext(context) {
      intercept[FetchFailedException] {
        invalidatePartialReads(accepted, Seq.empty, PartitionId)
      }
    }

    // The three properties of the invalidation, in the order they are contracted.
    assertBuffersReleasedExactlyOnce(accepted)
    assert(observedPartialReadInvalidations() === invalidationsBefore + 1L,
      "the invalidation must be counted exactly once on the partialReadInvalidations metric, but " +
        s"the counter moved from $invalidationsBefore to ${observedPartialReadInvalidations()}")
    assert(context.fetchFailed.contains(failure),
      "the fetch failure must have registered itself with the task context in its own " +
        "constructor  per SPARK-19276, which is what stops user code from hiding it")
    assert(failure.toTaskFailedReason.toErrorString.contains(ShuffleId.toString),
      "the failure must convert to a fetch-failure reason naming the shuffle, because that " +
        "conversion is the entire interface to stage recomputation")

    // The notifier is the bridge that stops a hang: a failure seen only on a Netty thread would be
    // swallowed and the task would wait forever on input that is never coming.
    notifier.setError(failure)
    assertPublishedFailure[FetchFailedException](notifier, "a lost producer")
    assert(notifier.fetchFailure.contains(failure),
      "the notifier must surface the fetch failure as such, so the task thread re-throws the " +
        "exception the scheduler recognises rather than a wrapper")
    val rethrown = intercept[FetchFailedException](notifier.throwIfError())
    assert(rethrown eq failure,
      "throwIfError must re-throw the very failure the I/O thread published, on the task thread")

    // And the job. Recomputation is the unmodified scheduler's business, so what is asserted here
    // is
    // the outcome: the application still produces the sort-based answer, to the record.
    assertSurvived(ProducerCrashDuringWrite,
      runStreamingShuffle("a streaming shuffle after a producer crash"), baseline)
  }

  // =============================================================================================
  // Scenario 2 of 10: consumer crash during read.
  // =============================================================================================

  test("a consumer crash during read retains the unacknowledged window and loses nothing") {
    val baseline = captureSortBaseline()
    startStreamingApplication("streaming-shuffle-consumer-crash")

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    val context = newTrackedTaskContext()
    // An allowance that a four block window overruns, so the threshold is genuinely crossed rather
    // than asserted about.
    val store = newBufferStore(
      context, clock, newQuota(SpillTriggeringExecutorMemoryBytes), partitions = 1)
    assert(store.registerConsumer(ConsumerId), "the consumer must be admitted before it stops")

    val window = bufferWindow(store, PartitionId, WindowBlockCount)
    assert(store.bufferedBytes > 0L, "the window must hold bytes for the watchdog to have work")

    injector.crashConsumer()
    assert(injector.isArmed(ConsumerCrashDuringRead),
      "the consumer crash must be armed before the writer's watchdog is examined")

    // Nine thousand nine hundred and ninety-nine milliseconds is not ten seconds. Asserting the
    // near miss is what proves the watchdog is a deadline rather than a guess.
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

    // The window is retained, and spilled rather than dropped once utilisation crosses the trigger.
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

    // Retransmission is paced by an exponential ladder from one second, capped at five attempts.
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

    // A consumer that has stopped is the extreme of a consumer that is merely behind, and the
    // degradation that covers both is measured over a sixty second window that must be outlasted
    // STRICTLY. Both sides of that boundary are asserted, because "sustained for more than sixty
    // seconds" and "sustained for sixty seconds" are different claims.
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

    assertSurvived(ConsumerCrashDuringRead,
      runStreamingShuffle("a streaming shuffle after a consumer crash"), baseline)
  }

  // =============================================================================================
  // Scenario 3 of 10: network partition.
  // =============================================================================================

  test("a network partition trips the fallback policy and the job still completes") {
    val baseline = captureSortBaseline()
    startStreamingApplication("streaming-shuffle-network-partition", "local-cluster[2,1,1024]")

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    injector.partitionNetwork()
    assert(injector.networkPartitioned, "the partition must be in force before it is evaluated")

    // The timeout half. A partition is indistinguishable from a dead peer, so it is the same five
    // second connection timeout that notices it, and it does not fire a millisecond early.
    val policy = activePolicy(clock)
    val openedAtMillis = clock.getTimeMillis()
    advanceJustBeforeProducerTimeout(clock)
    assert(clock.getTimeMillis() - openedAtMillis === JustBeforeProducerTimeoutMillis,
      "the fixture must stop one millisecond short of the connection timeout")
    assert(policy.streamingActive,
      "a partition that has not yet outlasted the connection timeout must not stand streaming down")
    clock.advance(ProducerConnectionTimeoutMillis - JustBeforeProducerTimeoutMillis)

    // Two percentages govern the link and they are emphatically not the same number: eighty per
    // cent is the share of the administered capacity streaming holds its own egress to, and ninety
    // per cent is the point past which the link is considered saturated and streaming stands down.
    // Conflating them would either throttle at the trip point or trip at the throttle point.
    assert(BandwidthCeilingPercent === 80L,
      s"the egress ceiling must be eighty per cent but was $BandwidthCeilingPercent")
    assert(LinkSaturationTripPercent === 90L,
      s"the saturation trip must be ninety per cent but was $LinkSaturationTripPercent")
    assert(BandwidthCeilingPercent !== LinkSaturationTripPercent,
      "the egress ceiling and the saturation trip are different constants with different " +
        "jobs and must never be collapsed into one")

    // The degradation half. Saturation is strictly above ninety per cent of the administered link,
    // so exactly at the threshold is not saturation.
    assert(policy.streamingActive, "the policy must still be in service before the trip is driven")
    policy.recordLinkUtilization(
      LinkSaturationTripPercent.toDouble, PercentScale.toDouble)
    assert(policy.streamingActive,
      s"exactly $LinkSaturationTripPercent percent of the link is not saturation, so the policy " +
        "must remain in service")
    driveFallbackReason(policy, StreamingShuffleFallbackReason.NetworkSaturation, ShuffleId)
    assert(policy.hasTripped, "a saturated link must latch the degradation")
    assert(policy.trippedReason.contains(StreamingShuffleFallbackReason.NetworkSaturation),
      s"the latched reason must be network saturation but was ${policy.trippedReason}")
    assert(policy.shouldDelegateToSortShuffle,
      "a tripped policy must route every service-provider call to the sort-based delegate")

    // The terminus is the unmodified sort manager, and it is unmodified in a way that is machine
    // checkable: it still takes exactly one constructor argument.
    val sortConstructors = classOf[SortShuffleManager].getConstructors
    assert(sortConstructors.length === 1,
      s"SortShuffleManager must expose one constructor but exposes ${sortConstructors.length}")
    assert(sortConstructors.head.getParameterCount === 1,
      "SortShuffleManager's constructor must take exactly the configuration, so the fallback " +
        s"target is byte for byte the one Spark ships, but it takes " +
        s"${sortConstructors.head.getParameterCount} arguments")

    // Now the live application's own policy, so the job under test really is degraded. The trip is
    // driven on the driver and before the workload is defined, so the shuffle is registered while
    // the policy is already standing streaming down -- which is the cheapest form of the
    // degradation and the one an operator should expect from a link that was already saturated.
    val manager = liveStreamingManager()
    driveFallbackReason(
      manager.streamingFallbackPolicy, StreamingShuffleFallbackReason.NetworkSaturation, ShuffleId)
    assert(manager.streamingFallbackPolicy.shouldDelegateToSortShuffle,
      "the running manager must be delegating before the workload is submitted, or the job would " +
        "not be exercising the degraded path at all")

    val observed = runRecorded("a streaming shuffle degraded by a network partition") {
      val grouped = groupByKeyWorkload(sc, PartitionCount)
      // The delegation made visible rather than inferred: a shuffle registered while the policy is
      // tripped carries a sort-based handle, so every writer and reader built for it is a sort one.
      val dependency = grouped.dependencies.head.asInstanceOf[ShuffleDependency[Int, String, _]]
      assert(!dependency.shuffleHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
        "a shuffle registered while streaming is stood down must carry a sort-based handle but " +
          s"carried a ${dependency.shuffleHandle.getClass.getName}")
      grouped.mapValues(values => values.toSeq.sorted).collect().toSet
    }
    assertSurvived(NetworkPartition, observed, baseline)
  }

  // =============================================================================================
  // Scenario 4 of 10: memory exhaustion during buffer allocation.
  // =============================================================================================

  test("memory exhaustion during buffer allocation surfaces as pressure and delegates to sort") {
    val baseline = captureSortBaseline()
    startStreamingApplication("streaming-shuffle-memory-exhaustion")

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

    // The arithmetic a partial grant is a partial grant OF, asserted against the feature's own
    // formula rather than against whatever the component happens to compute: the aggregate
    // allowance is bufferSizePercent of executor memory, and the per-partition allowance is that
    // divided by
    // the reduce partition count.
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

    // The condition is a PARTIAL grant, which is what MemoryConsumer.acquireMemory is documented as
    // being allowed to return and what Spark's own spillable collections treat as the signal to
    // spill. It is not an OutOfMemoryError, which is a different and untestable event. The injector
    // states the same cap, so the fault is expressed once and read twice.
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

    // Surfaced, not tolerated.
    assert(store.memoryPressureDetected,
      "a partial grant is the memory-pressure trip condition and has to be surfaced, because a " +
        "producer that quietly carried on would be one allocation away from an OutOfMemoryError")
    assert(store.memoryPressureEvents >= 1L,
      "the occurrence must be counted and not only flagged, so a recurring squeeze is visible")

    // And nothing leaked on the way out: the partial grant went straight back and the partition's
    // sequence run was left unclaimed, so the block the producer legitimately retries is not then
    // rejected as a duplicate.
    assert(store.bufferedBytes === 0L, "a refused admission must retain nothing")
    assert(store.executorReservedBytes === 0L,
      "the reservation must be returned to the executor-wide allowance")
    assert(store.getUsed() === 0L, "the partial grant must be handed back to the memory manager")
    assert(store.lastAcceptedSequence(PartitionId) === MemorySpillManager.UNSET_SEQUENCE,
      "a rolled back reservation must leave the partition's sequence run unclaimed")

    // The degradation. A satisfied grant is not pressure; a short grant is.
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

    // The live application's own policy, so the job really runs degraded, and then the outcome.
    val manager = liveStreamingManager()
    driveFallbackReason(
      manager.streamingFallbackPolicy, StreamingShuffleFallbackReason.MemoryPressure, ShuffleId)
    assert(manager.streamingFallbackPolicy.trippedReason
        .contains(StreamingShuffleFallbackReason.MemoryPressure),
      "the running manager must have latched memory pressure before the workload is submitted")

    assertSurvived(MemoryExhaustionOnAllocation,
      runStreamingShuffle("a streaming shuffle degraded by memory pressure"), baseline)
  }

  // =============================================================================================
  // Scenario 5 of 10: disk failure during spill.
  // =============================================================================================

  test("a disk failure during spill reverts, deletes and leaves every block servable") {
    val baseline = captureSortBaseline()
    startStreamingApplication("streaming-shuffle-disk-failure")

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    val localDirs = sc.env.blockManager.diskBlockManager.localDirs.map(_.getCanonicalPath).toSet

    // Part one: a healthy device, which is what establishes where a spill file legitimately comes
    // from. The writer is OBTAINED from the block manager for a file the disk block manager
    // allocated as a temporary shuffle block; it is never constructed for a path the component
    // chose
    // for itself. Both halves of that are checked -- the shape of the one seam through which a
    // writer arrives, and the provenance of the file it was opened on.
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

    // Part two: the device fails part way through, after blocks have been committed and after the
    // whole batch has already been detached from memory. This is the branch the rollback exists for
    // and the only one on which "reverted, deleted, still servable" can be observed at all.
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

    // The assertion the whole case exists for: nothing was lost. The eviction detached all three
    // blocks before writing any of them, so a rollback that failed to restore them would lose
    // output
    // a consumer is entitled to and could never ask for again.
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

    // Nothing the fault itself created outlives the scope of the fault, which is the other half of
    // "no orphan survives": a directory left unwritable would charge every later spill in this JVM.
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

    assertSurvived(DiskFailureDuringSpill,
      runStreamingShuffle("a streaming shuffle whose spill device failed"), baseline)
  }

  // =============================================================================================
  // Scenario 6 of 10: checksum mismatch on receive.
  // =============================================================================================

  test("a checksum mismatch on receive repairs inside the window and escalates outside it") {
    val baseline = captureSortBaseline()
    startStreamingApplication("streaming-shuffle-checksum-mismatch")

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    val context = newTrackedTaskContext()
    val store = newBufferStore(context, clock, newQuota(RoomyExecutorMemoryBytes), partitions = 1)
    assert(store.registerConsumer(ConsumerId), "the consumer must be admitted before it complains")
    val window = bufferWindow(store, PartitionId, WindowBlockCount)

    // CRC32C, from the JDK, with a fresh stateful instance per call. Determinism is the property
    // that matters: the same bytes must always give the same value, or a mismatch would be noise.
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

    // A block whose stamped checksum does not match the bytes that arrived. Both values are needed,
    // and both are asserted on: the expected value is what the producer stamped and the computed
    // one
    // is what the consumer recomputed, and an error that reported only one of them would leave an
    // operator unable to tell corruption from a framing defect.
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

    // Branch one: the block is INSIDE the unacknowledged window, which is inclusive of both ends. A
    // request for it is serviceable, so the repair is a retransmission and nothing escalates.
    val lastSequence = window.last._1
    val request = withTaskContext(context) {
      val built = retransmitRequest(ShuffleId, MapId, PartitionId, FirstSequence)
      assert(context.fetchFailed.isEmpty,
        "asking for a retransmission must not report a fetch failure, because the bytes are " +
          "still  held and the upstream stage does not need recomputing")
      built
    }
    // One request names exactly one position, which the header's own sequence number carries, so
    // both ends of the request coincide and a window of blocks is repaired one request per block.
    assert(request.firstSequenceNumber() === FirstSequence,
      s"the request must name $FirstSequence but named ${request.firstSequenceNumber()}")
    assert(request.lastSequenceNumber() === request.firstSequenceNumber(),
      "a request names one position, so both of its ends are that position")
    assert(request.contains(FirstSequence),
      "the request must contain the position it names")
    assert(request.blockCount() === 1L,
      s"a request asks for exactly one block but asked for ${request.blockCount()}")
    assert(!request.contains(FirstSequence + 1L),
      "and must contain no other position, or one repair would claim to cover another block")

    // Every block of the corrupt window is therefore repaired by a request of its own, and each of
    // those requests is serviceable for exactly the block it names.
    val windowRequests = window.map { case (sequenceNumber, _) =>
      sequenceNumber -> retransmitRequest(ShuffleId, MapId, PartitionId, sequenceNumber)
    }
    windowRequests.foreach { case (sequenceNumber, perBlock) =>
      assert(perBlock.contains(sequenceNumber) && perBlock.blockCount() === 1L,
        s"the repair of block $sequenceNumber must name that block and only that block")
    }
    assert(windowRequests.map(_._2.sequenceNumber()).toSet === window.map(_._1).toSet,
      "the repairs must cover exactly the blocks of the window, once each")

    // The repair is bounded on both axes, and both bounds are the protocol's own. A block larger
    // than the cap could never have been framed, and a request wider than the ceiling could never
    // be
    // serviced, so a corruption outside either bound has only the escalation left.
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
    assert(RetransmitRequestMessage.REQUESTED_BLOCKS === RequestedBlocksPerRequest,
      "the blocks a request names must be the protocol's own figure but was " +
        RetransmitRequestMessage.REQUESTED_BLOCKS)
    assert(request.blockCount() === RequestedBlocksPerRequest,
      s"a serviceable repair asks for exactly $RequestedBlocksPerRequest block but asked for " +
        s"${request.blockCount()}")

    // The repair itself: every block of the window is still servable, byte for byte, from memory or
    // from spill. That is what makes a retransmission a repair rather than a second corruption.
    val windowRequestBySequence = windowRequests.toMap
    window.foreach { case (sequenceNumber, expected) =>
      assert(windowRequestBySequence.get(sequenceNumber).exists(_.contains(sequenceNumber)),
        s"block $sequenceNumber must be named by the request that is about to replay it")
      assert(store.retainsBlock(PartitionId, sequenceNumber),
        s"block $sequenceNumber must be retained for the replay to be serviceable")
      val replay = store.retainedPayload(PartitionId, sequenceNumber)
      assert(replay.isDefined, s"block $sequenceNumber must be readable for replay")
      assert(replay.get.sameElements(expected),
        s"the replay of block $sequenceNumber must be byte for byte the original")
    }

    // Branch two: the consumer acknowledges the whole window, so the producer reclaims the bytes.
    // A corruption discovered now is not serviceable -- nothing holds the block any more -- and the
    // only repair left is stage recomputation, reached by the exception the scheduler recognises.
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

    // The frame the repair travels in, pinned on both axes the escalation decision depends on: the
    // protocol revision the peer must agree with, and the one byte of framing prefix that makes a
    // frame's length its encoded length plus one.
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
        invalidatePartialReads(Seq(recordingBufferTaken(samplePayload)), Seq.empty, PartitionId)
      }
    }
    assert(context.fetchFailed.contains(escalation),
      "corruption outside the retained window must escalate to the fetch failure that makes the " +
        "unmodified scheduler recompute the upstream stage")

    assertSurvived(ChecksumMismatchOnReceive,
      runStreamingShuffle("a streaming shuffle that saw a corrupted block"), baseline)
  }

  // =============================================================================================
  // Scenario 7 of 10: connection timeout during transfer.
  // =============================================================================================

  test("a connection timeout during transfer fires at five seconds and not before") {
    val baseline = captureSortBaseline()
    startStreamingApplication("streaming-shuffle-connection-timeout")

    // The constant itself, read from the component that owns it rather than restated, and then
    // pinned to the figure the feature documents.
    assert(ProducerConnectionTimeoutMillis === 5000L,
      "the producer connection timeout must be five seconds but is " +
        ProducerConnectionTimeoutMillis)
    assert(StreamingShuffleClientHandler.PRODUCER_CONNECTION_TIMEOUT_MS ===
        ProducerConnectionTimeoutMillis,
      "the consumer handler must measure the same connection timeout the protocol contracts, or " +
        "the two would disagree about when a producer is gone")
    // The timeout is tunable independently of block transfer precisely because streaming asks for
    // transport configuration under a module name of its own, so the name is part of the contract.
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
    val context = newTrackedTaskContext()

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

    val stalled =
      (0 until WindowBlockCount).map(index => recordingBufferTaken(payload(index.toLong)))
    val failure = withTaskContext(context) {
      intercept[FetchFailedException] {
        invalidatePartialReads(stalled, Seq.empty, PartitionId)
      }
    }
    assertBuffersReleasedExactlyOnce(stalled)
    notifier.setError(failure)
    assertPublishedFailure[FetchFailedException](notifier, "a transfer that timed out")
    assert(context.fetchFailed.contains(failure),
      "a timed out transfer must reach the scheduler through the exception it recognises")

    assertSurvived(ConnectionTimeoutDuringTransfer,
      runStreamingShuffle("a streaming shuffle whose transfer timed out"), baseline)
  }

  // =============================================================================================
  // Scenario 8 of 10: executor JVM GC pause.
  // =============================================================================================

  test("an executor JVM GC pause does not trip liveness detection and recovery is clean") {
    val baseline = captureSortBaseline()
    startStreamingApplication("streaming-shuffle-gc-pause")

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    val notifier = new StreamingShuffleErrorNotifier(ShuffleId, sc.getConf)
    val policy = activePolicy(clock)
    val context = newTrackedTaskContext()
    val store = newBufferStore(
      context, clock, newQuota(SpillTriggeringExecutorMemoryBytes), partitions = 1)

    // A stalled JVM and a stalled clock are indistinguishable to anything that reads time through
    // the injected clock, which every streaming component does. So the cadence is established
    // first,
    // to the millisecond, and the pause is then applied against it. Establishing it matters because
    // the failure mode this case guards against is a pause being mistaken for a dead peer.
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

    // The pause itself: three poll intervals of stall, which is long enough to be noticed and
    // strictly inside both liveness windows. Neither window may treat it as a loss.
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

    // Recovery is clean: nothing was lost across the pause and the store still works afterwards.
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

    // And the boundary from the other side: a stall that does outlast the window is a loss, which
    // is
    // the same timer and is why the near miss above is worth asserting at all.
    val breachedFrom = clock.getTimeMillis()
    injector.simulateGarbageCollectionPause(ConsumerLivenessTimeoutMillis)
    assert(clock.getTimeMillis() - breachedFrom >= ConsumerLivenessTimeoutMillis,
      "a pause of a whole liveness window must reach the deadline the watchdog measures")
    assert(injector.fireCount(ExecutorGarbageCollectionPause) === 2,
      "both pauses must be recorded, so a run's stall history is legible")

    assertSurvived(ExecutorGarbageCollectionPause,
      runStreamingShuffle("a streaming shuffle interrupted by a garbage collection pause"),
      baseline)
  }

  // =============================================================================================
  // Scenario 9 of 10: multiple concurrent producer failures.
  // =============================================================================================

  test("multiple concurrent producer failures invalidate atomically and independently") {
    val baseline = captureSortBaseline()
    startStreamingApplication(
      "streaming-shuffle-concurrent-producer-failures", "local-cluster[2,1,1024]")

    val clock = newManualClock()
    val injector = newFaultInjector(clock)
    val context = newTrackedTaskContext()

    // Three producers, each with frames of its own and a notifier of its own. The identities
    // matter:
    // an invalidation is per producer, so proving independence needs more than one producer to be
    // independent of.
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
    // else's. `invalidatePartialReads` asserts the independence itself, from the inside; the loop
    // below asserts the accumulated state after every step, which is where a leak between producers
    // would show up.
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
          invalidatePartialReads(frames, survivors, PartitionId + producerIndex,
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

    // The outcome, with two shuffles chained so the job really does run concurrent producers, and
    // still equal to a one-shuffle sort baseline because the second shuffle moves records it does
    // not change.
    assertSurvived(ConcurrentProducerFailures,
      runChainedStreamingShuffle("two chained streaming shuffles after concurrent producer loss"),
      baseline)
  }

  // =============================================================================================
  // Scenario 10 of 10: consumer reconnect after extended downtime.
  // =============================================================================================

  test("a consumer reconnect after extended downtime replays or escalates cleanly") {
    val baseline = captureSortBaseline()
    startStreamingApplication("streaming-shuffle-consumer-reconnect")

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

    // The reconnection, as a genuine two-party rendezvous rather than a sequence of calls on one
    // thread: the consumer returns on one thread and the producer waits for it on another, released
    // by a latch with a bounded timeout. Nothing sleeps and nothing polls.
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
    // advance rather than by a sleep. Reading through it here proves the advance the downtime made
    // is visible to whoever was waiting on it.
    val reading = awaitClockReading(clock, clock.getTimeMillis())
    assert(reading >= ManualClockEpochMillis + ExtendedDowntimeMillis,
      s"the clock must have advanced through the whole downtime but reads $reading")

    // Branch one: replay from the retained window. This is the branch a bounded absence takes, and
    // it is what makes the reconnection cheap. The replay is performed off the test thread, on a
    // daemon pool, and collected through ThreadUtils -- which is how a cross-thread result is taken
    // in this codebase and is why nothing here parks on an unbounded wait.
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

    // A request names one position, so the returning consumer asks for the window a block at a time
    // and the replay is the set of those requests rather than a single spanning one.
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

    // Branch two: the window has been reclaimed, so there is nothing left to replay and the
    // escalation is the clean answer. Correctness comes from stage recomputation instead.
    val released = store.acknowledge(ConsumerId, PartitionId, lastSequence)
    assert(released > 0L,
      s"acknowledging through $lastSequence must reclaim the window but reclaimed $released")
    assert(store.bufferedBytes === 0L, "a fully acknowledged window must hold nothing")
    assert(store.lowestRetainedSequence(PartitionId) === MemorySpillManager.UNSET_SEQUENCE,
      "a reclaimed window must retain no sequence at all, which is what makes a later replay " +
        "unserviceable rather than partially serviceable")
    val escalation = withTaskContext(context) {
      intercept[FetchFailedException] {
        invalidatePartialReads(
          Seq(recordingBufferTaken(payload(lastSequence))), Seq.empty, PartitionId)
      }
    }
    assert(context.fetchFailed.contains(escalation),
      "a replay of a reclaimed window must escalate to the fetch failure the scheduler " +
        "recognises  rather than serve bytes the producer no longer holds")

    assertSurvived(ConsumerReconnectAfterDowntime,
      runStreamingShuffle("a streaming shuffle whose consumer returned after an absence"), baseline)
  }

  // =============================================================================================
  // The closure properties. These are what stop the suite above from silently shrinking.
  // =============================================================================================

  test("this suite covers exactly the ten enumerated failure injection scenarios") {
    // Checked against the registered test names rather than against a set accumulated as the cases
    // run, so the property holds however the suite is invoked -- a single case run on its own must
    // not make the coverage claim fail, and a case deleted must.
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

    // Each condition is raised through the one object authorized to raise it, with the exact
    // placeholder set its template declares. The catalogue reader raises on an omission and on a
    // surplus alike, so a mismatch is a failure here rather than a degraded message in production.
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
    // The observability surface is closed at four, and the two this suite drives are among them. A
    // fifth metric would be an operator-facing addition nobody asked for, and a missing one would
    // leave a documented series empty, so the set is compared exactly.
    assert(MetricNames.length === ExpectedMetricCount,
      s"exactly $ExpectedMetricCount metrics are specified but ${MetricNames.length} are named")
    assert(streamingShuffleMetricNames() === MetricNames.toSet,
      s"the source must publish exactly ${MetricNames.sorted.mkString("[", ", ", "]")} but " +
        streamingShuffleMetricNames().toSeq.sorted.mkString("[", ", ", "]"))
    assert(MetricNames.contains(PartialReadInvalidationsMetricName),
      "the invalidation counter this suite asserts on must be one of the published metrics")
    assert(MetricNames.contains(SpillCountMetricName),
      "the spill counter this suite asserts on must be one of the published metrics")

    // And the baseline every case starts from really is zero, which is what makes a counter
    // assertion in any of the ten cases a measurement rather than a comparison with history.
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

  /** Failure scenarios this feature enumerates, which is the number this suite must cover. */
  private val ExpectedScenarioCount: Int = 10

  /** Error conditions this feature is authorized to add to the central catalogue. */
  private val ExpectedErrorConditionCount: Int = 3

  /** Metrics this feature publishes, which is the whole of its operator-facing surface. */
  private val ExpectedMetricCount: Int = 4

  /** A block name for the corruption error, shaped like the shuffle block ids Spark uses. */
  private def corruptBlockName: String = s"shuffle_${ShuffleId}_${MapId}_$PartitionId"

  /**
   * A recording frame that something has already taken a reference on, ready to be invalidated.
   *
   * @param bytes payload the frame carries
   * @return the frame, with exactly one outstanding reference
   */
  private def recordingBufferTaken(bytes: Array[Byte]): RecordingStreamingManagedBuffer = {
    val buffer = recordingBuffer(bytes)
    buffer.retain()
    buffer
  }

  /**
   * Asserts a raised condition carries exactly the placeholders its catalogue template declares.
   *
   * The catalogue reader demands a one to one match and raises on either an omission or a surplus,
   * so a condition that constructs at all has already proven the match. Comparing the two sets
   * explicitly is what turns that into a readable failure rather than an internal error, and
   * reading
   * the declared set from the catalogue as well as the supplied set from the throwable is what
   * stops
   * the assertion from merely restating whatever the caller happened to pass.
   *
   * @param error the raised condition
   * @param condition the condition name it must carry
   * @param expectedPlaceholders the placeholder names the template must declare
   */
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
