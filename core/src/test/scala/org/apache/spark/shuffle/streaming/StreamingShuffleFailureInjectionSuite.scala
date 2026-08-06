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
 * <b>How each case is structured.</b> Each case has two coordinated views of the same fault:
 *
 *  1. <i>The boundary, driven deterministically.</i> The fault is injected through
 *     [[StreamingShuffleFaultInjector]] or through the production component's own public surface,
 *     and the specific contracted behaviour is asserted at its exact boundary -- the five second
 *     producer timeout fires at 5000 ms and provably not at 4999, the ten second acknowledgement
 *     watchdog fires at 10000 and provably not at 9999, the sixty second slowness window trips
 *     strictly beyond 60000 and not at 59999.
 *  2. <i>The outcome, end to end, with the fault ACTIVE.</i> A real job runs on the streaming
 *     manager and its output is compared, as a set, with the baseline -- and the fault is applied
 *     to that job's own producers, links, files or consumers while it is running, from inside a
 *     reduce-side closure that has already taken delivery of records. This is what makes the
 *     comparison mean something: a clean job compared after a fault that happened somewhere else
 *     proves only that a clean job works. Each fault acts through a production seam and adds no
 *     branch to production code -- withdrawing retained output is the registry teardown
 *     `unregisterShuffle` performs, breaking links is the channel-global close the subsystem
 *     performs when a frame impugns a connection, damaging a retained spill file damages exactly
 *     the bytes [[StreamingShuffleBlockResolver]] would have served, and a consumer loss is a
 *     reduce task that dies. That the fault then LANDED is asserted from what the DRIVER saw --
 *     fetch failures naming the shuffle, stage resubmissions, injected task failures, spill
 *     volumes -- because a task that injects may be killed once the stage it disturbed is
 *     abandoned, and a killed task's accumulator updates are discarded.
 *
 * <b>Nothing here sleeps and nothing here is random unless it is seeded.</b> Elapsed time is an
 * advance of an injected `ManualClock`, so a timeout assertion is exact and instantaneous and the
 * "did not fire early" half of every timer contract is assertable at all. Cross-thread work is
 * released from a bounded barrier and collected through `ThreadUtils`. That is what makes the
 * zero-flakiness gate a property of the suite rather than a hope about it.
 *
 * <b>Every live half runs on separate executor JVMs.</b> `local-cluster[2,1,1024]` rather than
 * `local[n]`, and not as a matter of taste: in local mode the driver IS the executor, so a "remote"
 * read is a read from the same heap -- no channel is opened, no producer registry is separate from
 * the consumer's, and the once-per-executor injection latch would be shared with the driver and
 * with every later scenario in the suite. Two executor JVMs give each scenario a real transport to
 * break, a real registry to withdraw from, and a fresh latch of its own. Degradation is still
 * driven from the test thread and still observed exactly, because `registerShuffle` runs on the
 * DRIVER: a policy stood down there is one that every shuffle registered afterwards observes.
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

  /**
   * Size of the dataset the live half of every scenario shuffles.
   *
   * A quarter of the feature's target size, chosen against two opposing costs. It has to be large
   * enough that a single reduce partition cannot fit inside a consumer's credit window, because a
   * shuffle delivered in full before the first record is pulled leaves a reduce-side fault with
   * nothing to interrupt. It has to be small enough that ten scenarios, each capturing a baseline
   * and running a streaming job, remain a suite rather than an afternoon.
   */
  private val LiveDatasetBytes: Long = TargetDatasetBytes / 4L

  /**
   * Size of the dataset the three spill-dependent scenarios shuffle, which is the feature's own
   * target size.
   *
   * Chosen by arithmetic rather than by taste, because those scenarios are meaningless without
   * spill. An executor of [[ClusterMaster]]'s size gives the unified memory manager roughly 434
   * MiB, so the minimum buffer allowance of one percent is roughly 4.3 MiB for the whole executor.
   * At this dataset a map task holds about a tenth of it, ten MiB, which cannot fit and therefore
   * spills; at [[LiveDatasetBytes]] it holds 2.5 MiB, which fits and does not. Memory exhaustion, a
   * disk failure during spill and a corrupted retained block all need files on disk to act on, so
   * they pay for the larger dataset and the other seven do not.
   */
  private val SpillingDatasetBytes: Long = TargetDatasetBytes

  /**
   * Records a consumer receives before a live fault is applied.
   *
   * Small, because its only job is to make receipt POSITIVE and egress POSITIVE: the assertions
   * read both back and require them. Waiting longer would only shrink the window of outstanding
   * blocks the fault is meant to interrupt.
   */
  private val RecordsBeforeLiveFault: Int = 16

  /** How far into a doomed map partition an injected crash lands. Mid-write, deliberately. */
  private val RecordsBeforeMapFailure: Int = 256

  /** Prefix an injected map-side failure carries, so only this suite's own failures are counted. */
  private val InjectedMapFailureMarker: String = "Injected streaming shuffle live map failure"

  /**
   * How long a live link partition is held unusable.
   *
   * Longer than the five-second producer-liveness bound, because a link that heals inside that
   * bound is one the reader legitimately reconnects across without ever applying its timeout.
   */
  private val LivePartitionHoldMillis: Long = ProducerConnectionTimeoutMillis + 2000L

  /**
   * How long a consumer stall lasts when the scenario means "a pause that must NOT be a loss".
   *
   * Comfortably inside the five-second producer-liveness bound, because the claim being tested is
   * that an ordinary pause does not trip liveness detection.
   */
  private val BriefStallMillis: Long = ProducerConnectionTimeoutMillis / 2L

  /**
   * How long a consumer stall lasts when the scenario means "extended downtime".
   *
   * Past the consumer-liveness window, so the producer's retention and replay are what carry the
   * stream across it rather than luck.
   */
  private val ExtendedStallMillis: Long = ConsumerLivenessTimeoutMillis + 2000L

  /**
   * Master every live half runs on: two separate executor JVMs, one core each.
   *
   * Not a stylistic choice. In `local[n]` the driver IS the executor, so a "remote" read is a read
   * from the same heap: no channel is opened, no producer registry is separate from the consumer's,
   * and the once-per-JVM injection latch is shared with the driver and with every later scenario in
   * the suite. Two executor JVMs give each scenario a real transport to break, a real registry to
   * withdraw from, and a fresh latch per scenario.
   */
  private val ClusterMaster: String = "local-cluster[2,1,1024]"

  /** Executors the live halves wait for, which is the count [[ClusterMaster]] asks for. */
  private val ExecutorCount: Int = 2

  /** How long a live half waits for its executors before declaring the environment at fault. */
  private val ExecutorStartupTimeoutMillis: Long = 60000L

  /**
   * Attempts a task gets before its stage is abandoned, and the reason it is stated rather than
   * defaulted: every live scenario provokes at least one task failure ON PURPOSE, so a run that
   * inherited a lower ceiling from the environment would fail for want of retries rather than for
   * want of recovery.
   */
  private val MaxTaskFailures: Int = 4

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
   * Records the live producer-loss driver offers before the producer dies.
   *
   * Blocks are capped at two mebibytes of payload, so a dataset that fits in a single block would
   * reach the driver's pause only after its last record, the one moment a mid-write fault must
   * not be applied at. A million pairs is several blocks, so the pause falls inside the iterator.
   */
  private val LiveLossRecordCount: Int = 1000000

  /**
   * Declared partitions for the live producer-loss driver.
   *
   * Two, with every key a multiple of two, so all of the output lands in partition zero and one
   * consumer of `[0, 1)` reads the whole of it. A consumer reading only part could be served from
   * the retained window alone and would prove nothing about a partial read.
   */
  private val LiveLossPartitions: Int = 2

  private val LiveLossMapId: Long = 0L

  private val LiveLossWriterAttemptId: Long = 8100L

  private val LiveLossReaderAttemptId: Long = 8101L

  /**
   * How long the live driver's producer waits for its consumer to have consumed a record.
   *
   * Generous, because the wait expiring is the FAILURE mode of the driver rather than its normal
   * path: a producer that is genuinely being read from returns in microseconds.
   */
  private val LiveLossWaitMillis: Long = 30000L

  /** How long each half of the live driver is given to finish once both are running. */
  private val LiveLossJoinMillis: Long = 120000L

  /** Text the live driver's injected producer loss carries, so a run names its own fault. */
  private val LiveLossFailureMessage: String =
    "Injected streaming shuffle producer loss under a reading consumer"

  /** Data moved by the exact end-to-end workload each fault is injected into. */
  private val LiveFaultDatasetBytes: Long = 8L * 1024L * 1024L

  /** Keys retained in the live workload's digest, keeping correctness cheap to collect. */
  private val LiveFaultKeyCount: Int = 128

  /**
   * Map partitions whose producers die together in the live half of the concurrent-failure case.
   *
   * As many as [[ConcurrentProducerCount]], and spread rather than adjacent so the failures land on
   * both executors instead of clustering on whichever one took the first block of partitions.
   */
  private val ConcurrentFailingMapPartitions: Set[Int] = Set(0, 2, 5)

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

  /**
   * The records every production reader fixture streams, and the oracle its read is compared to.
   *
   * Distinct keys with values derived from them, so a read that dropped, duplicated or reordered a
   * record produces a set that differs from this one -- which is what makes "the fault cost
   * nothing" an assertion about the very unit of work the fault was injected into.
   */
  private val FixtureRecords: Seq[(Int, Int)] = (1 to 24).map(value => (value, value * 7))

  /** Coordinator epoch the fixtures' rendezvous answers carry. */
  private val FixtureCoordinatorEpoch: Long = 11L

  /** Capability token the fixtures' handles carry. */
  private val FixtureCapabilityToken: String = "streaming-shuffle-failure-injection-token"

  /** Task attempt id of the fixtures' first producer generation; later ones follow it. */
  private val FixtureProducerAttemptId: Long = 400L

  /** Port of the fixtures' first producer endpoint; later ones follow it. */
  private val FixtureProducerBasePort: Int = 17000

  /** The whole map range, which is what a reduce task of an unsplit stage requests. */
  private val FixtureFullRangeStart: Int = 0

  private val FixtureFullRangeEnd: Int = Int.MaxValue

  /** Map index of a fixture's first producer generation, which is also its read order. */
  private val FixtureFirstMapIndex: Int = 0

  /** Map id of a fixture's first producer generation, which its blocks are stamped with. */
  private val FixtureFirstMapId: Long = 0L

  /**
   * The volume every injected job shuffles.
   *
   * Two mebibytes over ten partitions, which is what gives a fault somewhere to land: see
   * [[groupedOutput]] for why five records could not.
   */
  private val InjectedDatasetBytes: Long = 2L * 1024L * 1024L

  /** The partition whose first attempt carries an injected fault. */
  private val CrashingMapPartition: Int = 3

  /** How far into that partition the fault lands, so it is genuinely part way through. */
  private val RecordsBeforeInjectedFault: Int = 64

  /** The narrowest allowance the configuration admits, used to make spill certain. */
  private val NarrowestBufferSizePercent: Int = 1

  /** The lowest spill trigger the configuration admits, used with it. */
  private val LowestSpillThresholdPercent: Int = 50

  /**
   * The in-job stall the garbage-collection scenario injects.
   *
   * Chosen strictly inside both liveness windows -- the five second producer window and the ten
   * second consumer window -- because the claim is that a pause of this size is not a loss. A stall
   * that reached either window would be asserting the opposite, and the assertion that a stall past
   * the window IS treated as a loss is made on the injected clock where it costs nothing.
   */
  private val ConsumerStallMillis: Long = 2000L

  /** The map partitions the concurrent-failure scenario severs, which must be more than one. */
  private val ConcurrentlySeveredPartitions: Set[Int] = Set(2, 5, 7)

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
    // The live-fault claims are JVM scoped too, for the reason the claims exist at all, so they are
    // returned here as well: a case whose fault had already been claimed by the case before it
    // would compare a run that saw nothing.
    LiveFaultHolder.reset()
    LiveStreamingFault.resetInjection()
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
      LiveFaultHolder.reset()
      LiveStreamingFault.resetInjection()
      super.afterEach()
      resetStreamingShuffleMetrics()
      MemorySpillManager.resetSharedStateForTesting()
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
  /**
   * Collects every task-failure reason a run produces.
   *
   * This is the driver-visible trace of a fault that failed something, and it exists because an
   * accumulator cannot be that trace: Spark discards the accumulator updates of a failed task, so a
   * fault whose whole purpose is to fail a task reports nothing through one. A fetch failure is
   * captured for the same reason from the other direction -- the task that reports it is a consumer
   * that was left reading a withdrawn producer, and it added nothing itself.
   */
  private class TaskDisturbanceRecorder extends SparkListener {

    private val reasons = mutable.ArrayBuffer.empty[String]

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      taskEnd.reason match {
        case failure: TaskFailedReason => synchronized(reasons += failure.toErrorString)
        case _ =>
      }
      ()
    }

    /** Every failure reason this run reported, in the order the driver learned of them. */
    def failureReasons: Seq[String] = synchronized(reasons.toSeq)
  }

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

    /**
     * Every task outcome the driver saw, which is the only reliable place to read one.
     *
     * A task that injects a fault may be KILLED once the fault it caused abandons its stage, and a
     * killed task's accumulator updates are discarded, so an injecting task cannot be trusted to
     * report on itself. What the driver saw is not subject to that, which is why every "the fault
     * landed" assertion in this suite reads from here.
     */
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

    /** Jobs the scheduler declared successful. */
    def succeededJobCount: Int = succeeded.get()

    /** Jobs the scheduler declared failed. */
    def failedJobCount: Int = failed.get()

    /** Why the failed jobs failed, for a message that names the cause rather than a count. */
    def failureDescriptions: Seq[String] = synchronized(failures.toSeq)

    /** Every task failure, whatever its shape. */
    def countedFailures: Seq[TaskFailedReason] = synchronized(taskFailures.toSeq)

    /** The fetch failures, which are the streaming reader's route to stage recomputation. */
    def fetchFailures: Seq[FetchFailed] = synchronized(fetchFailed.toSeq)

    /** The most times any stage was submitted, so recomputation is read rather than inferred. */
    def maxStageSubmissions: Int = synchronized {
      if (submissions.isEmpty) 0 else submissions.values.max
    }

    /** Bytes the run reported on the existing memory-spill accumulator. */
    def memoryBytesSpilled: Long = synchronized(memorySpilled)

    /** Bytes the run reported on the existing disk-spill accumulator. */
    def diskBytesSpilled: Long = synchronized(diskSpilled)
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
   * `bufferSizePercent` of the configured executor memory, so the memory size is the allowance
   * scaled back up by that percentage; four blocks of a five block allowance is eighty per cent,
   * which is the documented trigger, reached exactly rather than approached. Paired with a single
   * registered partition, because the per-partition ceiling is the allowance divided by the
   * partition count and a window belongs to one partition.
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
   * @param executorMemoryBytes configured executor memory the allowance is a share of
   * @param bufferSizePercent share of that memory reserved for streaming buffers
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

  /**
   * Starts the application every live half runs on: two executor JVMs, retries stated explicitly.
   *
   * The two settings are what make an intentional fault survivable rather than fatal. Every live
   * scenario provokes at least one task failure, and a fetch failure additionally costs the stage a
   * submission, so a run that took the environment's defaults could be abandoned for having used up
   * its attempts while the recovery it is testing was working perfectly.
   *
   * @param appName application name, which is what makes an event log readable
   * @param conf configuration to start from, defaulting to plain streaming
   */
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


  // ---------------------------------------------------------------------------------------------
  // Live fault injection into the compared job.
  //
  // Every scenario in this suite has two halves and they answer different questions. The precision
  // half drives one component directly against an injected clock and an injected allowance, which
  // is what makes a threshold, a release count or a message shape EXACT -- a cluster reading,
  // aggregated asynchronously, could never pin those down. The live half runs the real workload
  // through the real writer, reader, transport and spill path WITH THE FAULT ACTIVE, and asserts
  // both that the fault was active and that the output is still the sort-based baseline's.
  //
  // The live half is what makes "zero data loss" a claim about the subsystem rather than about a
  // simulation beside it. Before it existed, every scenario compared a CLEAN job against the
  // baseline, so a fault that never reached production would have passed all ten.
  //
  // Two rules the live half obeys throughout, both learned the hard way:
  //
  //  - Nothing is asserted on a reading only the injecting task could report. When the scheduler
  //    abandons a stage attempt it KILLS the attempts still running, and a killed task's
  // accumulator    updates are never merged, whatever the accumulator was registered to count. A
  // fault that    destroys its own injector therefore cannot reliably report itself, so what is
  // asserted is the    driver's own evidence -- the executors' metric counters, the scheduler's
  // failure reasons, the    job outcome -- plus readings taken BEFORE the destructive act.
  //
  //  - Every injection is bounded and self-limiting, so the recomputation it provokes runs clean
  // and    the job converges. The bound is a one-shot latch in the executor's own JVM, which also
  // removes    any dependence on which partition was scheduled where.
  // ---------------------------------------------------------------------------------------------

  /**
   * The dataset the live half shuffles: real records, sized so blocks are genuinely outstanding.
   *
   * The five-pair fixture the precision halves use cannot serve here. A reduce-side fault has to
   * land while a consumer has received some records and is still owed more, and five records across
   * ten partitions leaves nothing outstanding to interrupt.
   *
   * @param context live context to build on
   * @param numPartitions map and reduce width of the shuffle
   * @return the key-value dataset, not yet computed
   */
  private def keyedWorkload(
      context: SparkContext,
      numPartitions: Int,
      datasetBytes: Long): RDD[(Int, String)] = {
    largeDataset(context, numPartitions, datasetBytes)
  }

  /**
   * The workload as a shuffle whose reduce side is consumed LAZILY, so a fault can be injected
   * while data is genuinely in flight.
   *
   * <b>Why not `groupByKey`.</b> Egress happens during the REDUCE stage, because the unmodified DAG
   * scheduler starts no reduce task until the map stage has finished: the map stage retains its
   * output and the reduce tasks then drain it over the transport. A fault that has to land while
   * blocks are outstanding must therefore be injected from the reduce side, and `groupByKey` cannot
   * host one -- its reduce side inserts every record into an `ExternalAppendOnlyMap` before
   * yielding a single group, so by the time anything downstream runs the input has already been
   * drained. `partitionBy` asks for no aggregator and no key ordering, so its reduce-side iterator
   * is consumed one record at a time, exactly as the streaming reader hands them over.
   *
   * @param context live context to build on
   * @param failingMapPartitions map partitions whose first attempt dies, possibly empty
   * @param chainedShuffles whether a second shuffle is chained on, so the job runs two
   * concurrently.                        The extra `partitionBy` moves records it does not change,
   * so the digest                        is still the one-shuffle baseline's -- which is what lets
   * a two-shuffle                        job be compared against a one-shuffle baseline without
   * weakening the                        comparison
   * @param datasetBytes size of the dataset to shuffle
   * @return the partitioned RDD, not yet computed
   */
  private def liveWorkload(
      context: SparkContext,
      failingMapPartitions: Set[Int] = Set.empty,
      chainedShuffles: Boolean = false,
      datasetBytes: Long = LiveDatasetBytes): RDD[(Int, String)] = {
    val doomed = failingMapPartitions
    val marker = InjectedMapFailureMarker
    // Hoisted onto the driver's stack rather than read from the suite inside the closure. A closure
    // that reads a suite member captures the suite, and a suite is not serialisable, so the job
    // would fail to submit rather than fail the way this fixture intends.
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
            // Thrown part way along the iterator, which is what makes it a mid-write loss: a
            // streaming writer PULLS records through this function, so blocks have already been
            // framed, checksummed and admitted by the time the producer dies.
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
      // The `map` is load bearing rather than decorative: `partitionBy` returns the receiver
      // unchanged when it already carries an equal partitioner, so asking for the same partitioning
      // twice registers ONE shuffle. Dropping the partitioner first is what makes the second
      // `partitionBy` a second shuffle, and mapping each record to itself is what keeps the digest
      // identical to the one-shuffle baseline's.
      partitioned.map(record => record).partitionBy(new HashPartitioner(PartitionCount))
    } else {
      partitioned
    }
  }

  /**
   * The digest of a lazily consumed shuffle: every record as its key, value length and value hash.
   *
   * A record lost, duplicated or truncated changes the set, so comparing two of these is the
   * operational definition of zero data loss for this shape of shuffle.
   *
   * @param shuffled the shuffle to drain
   * @return one entry per record
   */
  private def liveDigestOf(shuffled: RDD[(Int, String)]): Set[(Int, Int, Int)] = {
    shuffled.map { case (key, value) => (key, value.length, value.hashCode) }.collect().toSet
  }

  /**
   * The same dataset through sort-based shuffle, digested the same way, in a context of its own.
   *
   * Captured before the application under test exists, because Spark permits one context per JVM.
   *
   * @param datasetBytes size of the dataset to shuffle, which must match the streaming run's
   * @return the baseline digest
   */
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

  /**
   * Runs the live workload with one reduce-side fault active and asserts it lost nothing.
   *
   * @param scenario the enumerated scenario being exercised, for assertion messages
   * @param baseline the sort-based digest the streaming output must equal
   * @param fault the fault to apply on the executor, after receipt is positive
   * @param failingMapPartitions map partitions whose first attempt dies, for the map-side scenarios
   * @param chainedShuffles whether the job runs two shuffles rather than one
   * @param datasetBytes size of the dataset to shuffle, which must match the baseline's
   * @return the run, so a caller can make scenario-specific assertions on what the driver saw
   */
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
    // Supplied after the graph exists and before the action submits it, which is the only window in
    // which the shuffle id is known and the closure carrying the fault has not yet been serialised.
    fault.bindShuffle(dependency.shuffleId)
    val observed = try {
      liveDigestOf(injectingRead(shuffled, fault, probe))
    } finally {
      sc.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
      sc.removeSparkListener(recorder)
    }

    // Zero data loss, which is the whole point, asserted on output produced WHILE the fault was
    // active rather than by a clean job run afterwards.
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

    // The fault was offered to a LIVE stream. Both readings are taken before the destructive act,
    // so they arrive whatever becomes of the task that took them.
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
   * @param probe readings the injecting tasks reported
   * @param recorder what the DRIVER saw, which is where "the fault landed" is read from
   * @param observed the output digest, already compared against the baseline
   * @param shuffleId the shuffle the fault was bound to
   */
  private case class LiveFaultRun(
      probe: LiveFaultProbe,
      recorder: JobOutcomeRecorder,
      observed: Set[(Int, Int, Int)],
      shuffleId: Int) {

    /**
     * Fetch failures the driver saw against this run's shuffle.
     *
     * Filtered by shuffle id rather than counted wholesale, because a scenario claims recovery of
     * ITS shuffle and a run with two shuffles must not satisfy that claim with the other one's
     * failure.
     */
    def fetchFailuresForShuffle: Seq[FetchFailed] =
      recorder.fetchFailures.filter(_.shuffleId == shuffleId)

    /** Task failures whose description names this suite's injected map-side loss. */
    def injectedMapFailures: Seq[TaskFailedReason] =
      recorder.countedFailures.filter(_.toErrorString.contains(InjectedMapFailureMarker))
  }

  /**
   * Wraps a lazily consumed shuffle so that one fault is applied per executor, mid-drain.
   *
   * @param shuffled the shuffle to read
   * @param fault the fault to apply
   * @param probe the readings to report
   * @return the mapped RDD, not yet computed
   */
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

  /**
   * An accumulator whose updates survive the failure of the task that made them.
   *
   * `sc.longAccumulator` discards a failed task's updates, and in a fault-injection suite the task
   * whose readings matter is precisely the one that may then fail.
   *
   * @param name the accumulator's name
   * @return the accumulator
   */
  private def liveFaultAccumulator(name: String): LongAccumulator = {
    val accumulator = new LongAccumulator
    accumulator.register(sc, name = Some(s"streamingShuffleLive-$name"), countFailedValues = true)
    accumulator
  }

  /**
   * Discards every block accepted from one producer, atomically, and reports the fetch failure that
   * makes the unmodified scheduler recompute the upstream stage.
   *
   * @param shuffleId shuffle the pair used
   * @param recordsOffered records the producer offered before it died
   * @param recordsRead records the consumer had read when it died
   * @param consumerWasReading whether the producer's pause was released by the consumer having
   *                           consumed, rather than by the wait expiring
   * @param producerStopped whether the lost producer's writer was stopped unsuccessfully
   * @param writeFailure what escaped the production write, if anything did
   * @param consumerFailure what the production reader raised, if it raised anything
   * @param invalidationDelta movement on `shuffle.streaming.partialReadInvalidations` over the run
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

  /**
   * Loses a producer under a consumer that is reading from it, and returns what the PRODUCTION
   * reader did about it.
   *
   * <b>Why this exists.</b> The scenario this suite opens with is a producer crash during a write,
   * and the only place that fault is distinguishable from an ordinary task retry is where a
   * consumer is attached to a producer that has not finished producing. Under the unmodified DAG
   * scheduler a reduce task is never submitted while its map task runs, so the pair is built here
   * directly from the live manager, as the scheduler would hand it out, and the production writer,
   * reader, coordinator and transport are left to do the whole of the work. Nothing about the
   * detection, the discard, the counter or the escalation is modelled.
   *
   * The producer pauses once a block has gone out, waits for the consumer to have consumed one,
   * and then dies inside its own iterator; its task is then stopped unsuccessfully, which is the
   * production withdrawal path. The consumer is an ordinary streaming reader, reading the whole map
   * output because every key is a multiple of the partition count.
   *
   * @param recordCount records the producer offers before the fault, sized so the pause below is
   *                    reached well inside the iterator rather than at its last record
   * @return what the two halves observed, for the calling case to assert against
   */
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

    try {
      producer.start()
      eventually(timeout(60.seconds), interval(20.milliseconds)) {
        assert(writerHandle.get() != null || producerFailure.get() != null,
          "the producer must have been given a streaming writer, or have failed trying")
      }
      assert(producerFailure.get() == null,
        s"the producer must have been given a streaming writer, but failed with " +
          s"${producerFailure.get()}")
      consumer.start()
      producer.join(LiveLossJoinMillis)
      consumer.join(LiveLossJoinMillis)
    } finally {
      writerContext.markTaskCompleted(None)
      readerContext.markTaskCompleted(None)
    }
    assert(!producer.isAlive && !consumer.isAlive,
      "both halves must have finished inside the join budget, or the case timed out rather than " +
        "observed anything")
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

  /**
   * Asserts everything a lost producer under a reading consumer contracts, on the production path.
   *
   * @param loss what the live pair observed
   */
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

  /**
   * Walks a failure's cause chain down to the fetch-failure REASON inside it, if there is one.
   *
   * The reason rather than the exception, because the reason is what the task framework hands the
   * scheduler and therefore what recomputation turns on; and the chain rather than the outermost
   * type, because a failure raised inside an iterator can surface wrapped.
   *
   * @param failure the throwable a live half recorded
   * @return the fetch-failure reason in its chain, if the chain holds one
   */
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

  /**
   * Asserts the two properties a partial-read invalidation must have, over frames THIS CASE
   * arranged, and escalates the way the reader escalates.
   *
   * <b>What this is, and what it is not.</b> It is not the production reader, and it does not claim
   * to be one: the reader's own invalidation is driven for real by
   * [[loseProducerUnderReadingConsumer]] above, and again end to end by
   * `StreamingShuffleIntegrationTest`, "scenario 2 in the live path". What this helper does is
   * assert the two properties of a discard over frame sets a case has arranged deliberately --
   * several producers at once, a reclaimed window, a stalled transfer -- which is the only way to
   * state the independence property at all, since a live reader is handed one producer set and not
   * several:
   *
   * '''One substitution only.''' The producer connector and the coordinator reference are test
   * doubles because a reduce task cannot otherwise be given a producer to lose inside a single JVM.
   * The reader, its handlers, its credit ledgers, its checksum verification, its buffer accounting,
   * its invalidation and its error notifier are all production instances.
   *
   * @param numMaps producer generations the coordinator offers, each feeding the same reduce
   *                partition; more than one is what makes "an invalidation reaches exactly one
   *                producer" assertable at all
   * @param reducePartition the partition this consumer reads
   * @param records the key-value pairs each producer streams, which are also the oracle the read is
   *                compared against
   */
  /**
   * A recording frame with exactly one outstanding reference, which is what a frame a consumer has
   * accepted looks like.
   *
   * @param bytes payload the frame carries
   * @return the frame, with exactly one outstanding reference
   */
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
   * A REAL consumer -- production reader, production consumer handler, production flow control --
   * reading from producers a test connector opens, so that a producer loss is invalidated by
   * production code rather than modelled by this suite.
   *
   * '''Why this replaced a helper that did the invalidation itself.''' The previous shape of this
   * suite released the buffers, incremented `partialReadInvalidations` and threw
   * `FetchFailedException` from a test method. Every assertion that followed was then an assertion
   * about that method: the metric moved because the test moved it, the exception carried the fields
   * the test passed, and the release happened because the test released. Production's invalidation
   * path -- which decides WHEN a producer is lost, WHAT belongs to it, and whether anything else is
   * disturbed -- was never executed. What is here instead is the production path, driven at the one
   * seam a suite legitimately owns: the transport. Blocks are delivered through the transport's own
   * request dispatcher, the peer's socket is taken away, and everything after that is production's
   * decision, including the metric and the exception.
   *
   * '''One substitution only.''' The producer connector and the coordinator reference are test
   * doubles because a reduce task cannot otherwise be given a producer to lose inside a single JVM.
   * The reader, its handlers, its credit ledgers, its checksum verification, its buffer accounting,
   * its invalidation and its error notifier are all production instances.
   *
   * @param numMaps producer generations the coordinator offers, each feeding the same reduce
   *                partition; more than one is what makes "an invalidation reaches exactly one
   *                producer" assertable at all
   * @param reducePartition the partition this consumer reads
   * @param records the key-value pairs each producer streams, which are also the oracle the read is
   *                compared against
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

    /** Every producer generation offered, in map-index order, which is also read order. */
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

    /**
     * The payload bytes one producer streams for this partition.
     *
     * Wrapped for the block id the reader unwraps with, once for the whole partition, because a
     * codec's framing spans the blocks the payload is cut into.
     *
     * @param mapId producing map task, whose identity the wrapping and the checksums bind to
     * @return the wrapped bytes
     */
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

    /**
     * One producer's partition payload cut into the run of blocks it would put on the wire.
     *
     * @param mapId producing map task each block is stamped with
     * @param blocks how many blocks to cut the payload into
     * @return the blocks, in sequence order
     */
    def blocksOf(mapId: Long, blocks: Int = WindowBlockCount): Seq[DataBlockMessage] = {
      val payload = payloadOf(mapId)
      assert(payload.nonEmpty, "a partition payload must carry bytes to be cut into blocks")
      val chunkSize = math.max(1, (payload.length + blocks - 1) / blocks)
      payload.grouped(chunkSize).toSeq.zipWithIndex.map { chunk =>
        dataBlock(shuffleId, mapId, reducePartition, chunk._2.toLong, chunk._1)
      }
    }

    /**
     * The orderly end-of-stream one producer sends once it has framed everything.
     *
     * @param mapId producing map task
     * @param totalBlocks blocks the producer says it sent
     * @return the termination frame
     */
    def terminatorOf(mapId: Long, totalBlocks: Long): StreamTerminationMessage =
      streamTermination(shuffleId, mapId, reducePartition, totalBlocks)

    /**
     * The open channel to one producer, identified by the map index it produces.
     *
     * @param mapIndex map index whose channel to take
     * @return the stream
     */
    def streamOf(mapIndex: Int): StreamingShuffleTestProducerStream = {
      val open = connector.streams.filter(stream => stream.location.mapIndex == mapIndex)
      assert(open.size === 1,
        s"exactly one channel must be open to the producer of map index $mapIndex but " +
          s"${open.size} were, among " +
          connector.streams.map(stream => stream.location.mapIndex).mkString(", "))
      open.head
    }

    /**
     * Takes one producer's socket away, exactly as a lost executor does.
     *
     * The channel is closed and the handler is told the channel went inactive, which is the pair of
     * events a peer's disappearance produces. Production decides what that means; nothing here
     * touches a buffer, a counter or an exception.
     *
     * @param mapIndex map index of the producer to lose
     */
    def loseProducer(mapIndex: Int): Unit = {
      val stream = streamOf(mapIndex)
      stream.closeChannel()
      stream.handler.channelInactive(stream.client)
      // The transition is applied off the event loop, exactly as an arriving frame is, so the loss
      // it records exists only once that work has run. Waiting here is what makes the injection an
      // injection rather than a race the fixture would sometimes win.
      stream.awaitDataPlane(s"the channel-inactive transition of map index $mapIndex")
      assert(stream.handler.isProducerLost,
        s"the production handler bound to map index $mapIndex must have observed the loss, or " +
          "nothing has been injected")
    }

    /** The records every producer of this fixture streams, and the oracle a read is compared to. */
    val streamedRecords: Seq[(Int, Int)] = records

    /**
     * Drains a read into the pairs it yielded.
     *
     * @param streamed the reader's iterator
     * @return the records it produced, in the order it produced them
     */
    def readAll(streamed: Iterator[Product2[Int, Int]]): Seq[(Int, Int)] =
      streamed.map(record => (record._1, record._2)).toSeq

    /**
     * The credit ledgers this consumer holds for one producer generation.
     *
     * A ledger's identity includes the generation, so a multi-producer read holds one per producer
     * per partition -- which is what lets a case assert that a healthy peer's allowance was left
     * exactly as it was while a lost producer's was released.
     *
     * @param mapId the producing map task whose ledgers to select
     * @return its registered ledgers
     */
    def creditLedgersOf(mapId: Long): Seq[BackpressureStreamKey] =
      backpressure.registeredStreams.filter(key =>
        key.partitionId == reducePartition && key.mapId == mapId)

    /** The reduce partition this consumer reads, which a fetch failure must name. */
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
   * @param body the work
   * @tparam T the body's result type
   * @return the body's result
   */
  private def withProducerLoss[T](
      fixture: ProducerLossFixture)(body: ProducerLossFixture => T): T = {
    try {
      body(fixture)
    } finally {
      fixture.release()
    }
  }

  /**
   * The scheduler-facing reason a production fetch failure converts to.
   *
   * Read through the conversion rather than off the exception, because the conversion is the entire
   * interface to stage recomputation and its fields are what the scheduler acts on.
   *
   * @param failure the failure production raised
   * @return the fetch-failure reason
   */
  private def fetchFailedReasonOf(failure: FetchFailedException): FetchFailed =
    failure.toTaskFailedReason.asInstanceOf[FetchFailed]

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
    val baseline = liveSortBaseline()
    startLiveStreamingApplication("streaming-shuffle-producer-crash")

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

    // And now the deadline itself.
    clock.advance(ProducerConnectionTimeoutMillis - JustBeforeProducerTimeoutMillis)
    val elapsed = clock.getTimeMillis() - openedAtMillis
    assert(elapsed === ProducerConnectionTimeoutMillis,
      s"the deadline must be reached at exactly ${ProducerConnectionTimeoutMillis} ms, " +
        s"not $elapsed")

    // THE invalidation, performed by the PRODUCTION reader rather than modelled. A real consumer
    // reads from a real producer over the real transport, the producer dies inside its own iterator
    // while the consumer holds partial reads, and everything after that -- detecting the loss,
    // discarding every block accepted from it, counting the invalidation and raising the one signal
    // the unmodified scheduler answers -- is done by the subsystem itself.
    val loss = loseProducerUnderReadingConsumer()
    assertProductionInvalidation(loss)
    logInfo(log"Live producer loss under a reading consumer: " +
      log"${MDC(NUM_RECORDS_READ, loss.recordsRead)} record(s) had been read of " +
      log"${MDC(RECORDS, loss.recordsOffered)} offered, the reader counted " +
      log"${MDC(COUNT, loss.invalidationDelta)} invalidation(s), and answered with " +
      log"${MDC(CLASS_NAME, loss.consumerFailure.map(_.getClass.getName).getOrElse("nothing"))}")

    // The notifier is the bridge that stops a hang: a failure seen only on a Netty thread would be
    // swallowed and the task would wait forever on input that is never coming. It is asserted over
    // a fetch failure raised here rather than over the live one above, because the bridge's
    // contract is about WHICH failure propagates on which thread and holds however the loss was
    // detected -- and because the live reader publishes on its own notifier, not on this one.
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

    // And the job, with the loss ACTIVE while it runs rather than repaired beforehand. Two things
    // happen to the producing side of this one job: one map partition dies part way along its write
    // on its first attempt, and one executor's retained output for the shuffle is withdrawn while
    // consumers are mid-drain. Recomputation is the unmodified scheduler's business, so what is
    // asserted is the outcome plus the driver's own evidence that the loss was really taken.
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

  // =============================================================================================
  // Scenario 2 of 10: consumer crash during read.
  // =============================================================================================

  test("a consumer crash during read retains the unacknowledged window and loses nothing") {
    val baseline = liveSortBaseline()
    startLiveStreamingApplication("streaming-shuffle-consumer-crash")

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

    // And the job, with the loss ACTIVE while it runs: a consuming task dies part way through its
    // drain, having already taken delivery of records and acknowledged them, so its retry has to be
    // served from what the producer retained rather than from anything the dead attempt held.
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

  // =============================================================================================
  // Scenario 3 of 10: network partition.
  // =============================================================================================

  test("a network partition trips the fallback policy and the job still completes") {
    val baseline = liveSortBaseline()
    startLiveStreamingApplication("streaming-shuffle-network-partition")

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

    assert(policy.streamingActive,
      "producer silence is not the link-utilisation fallback condition, so the standalone policy " +
        "must not claim NetworkSaturation without a capacity sample")

    // The terminus is the unmodified sort manager, and it is unmodified in a way that is machine
    // checkable: it still takes exactly one constructor argument.
    val sortConstructors = classOf[SortShuffleManager].getConstructors
    assert(sortConstructors.length === 1,
      s"SortShuffleManager must expose one constructor but exposes ${sortConstructors.length}")
    assert(sortConstructors.head.getParameterCount === 1,
      "SortShuffleManager's constructor must take exactly the configuration, so the fallback " +
        s"target is byte for byte the one Spark ships, but it takes " +
        s"${sortConstructors.head.getParameterCount} arguments")

    // Now a real partition, in a real job, while it runs: every link this executor serves is closed
    // and HELD closed for longer than the five-second producer-liveness bound, because one close is
    // not a partition -- a consumer reconnects across it immediately and the bound never expires.
    // The producers stay registered and keep their output throughout, which is what distinguishes a
    // partition from a loss: a later attempt reconnects and finds the output waiting.
    val run = runLiveFault(
      NetworkPartition, baseline, new LiveLinkPartition(LivePartitionHoldMillis))
    assert(run.fetchFailuresForShuffle.nonEmpty,
      s"an unusable link must have timed out into a fetch failure against shuffle " +
        s"${run.shuffleId}, or nothing about the partition reached the scheduler: the driver saw " +
        s"${run.recorder.fetchFailures.size} fetch failure(s) and none named it")
    assert(run.recorder.maxStageSubmissions >= 2,
      "recovering across a partition means recomputing, so a stage must have been submitted more " +
        s"than once, but the most any was submitted is ${run.recorder.maxStageSubmissions}")

    // And the degradation the policy is for, made visible rather than inferred. Registration
    // happens on the DRIVER, so a policy stood down there is one every shuffle registered
    // afterwards observes: the handle it carries is a sort-based one, and every writer and reader
    // built for it is therefore a sort one.
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

  // =============================================================================================
  // Scenario 4 of 10: memory exhaustion during buffer allocation.
  // =============================================================================================

  test("memory exhaustion during buffer allocation surfaces as pressure and delegates to sort") {
    val baseline = liveSortBaseline(SpillingDatasetBytes)
    // The buffer allowance held at its documented minimum and the spill threshold at its lowest, so
    // this job's producers genuinely cannot hold their partitions in memory. The pressure is
    // applied by the CONFIGURATION rather than by a stub, which is what makes the path that refuses
    // the production allocation path.
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

    // And the job, run under the pressure rather than beside it: the allowance this application
    // holds cannot fit a partition of this dataset, so admission refuses and the production spill
    // path is what carries the shuffle. The probe reports how many reservations its executor
    // actually refused, which is the difference between a scenario that ran under memory pressure
    // and one that merely declared it.
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
    // Either the probe reported its reading, or the attempt that took it did not survive to report
    // anything -- and one of the two must hold, which is what keeps this able to fail. A task that
    // dies of a fetch failure carries NO accumulator payload at all: `FetchFailed` has no field for
    // one, so `countFailedValues` cannot rescue it. Under an allowance this small the read that
    // reaches the injection point is exactly the read that then fails, and the readings above come
    // from the stage attempt that followed, whose tasks number from zero again and find this
    // executor's one-shot latch already taken. A run that never reached the probe at all leaves
    // neither trace.
    assert(run.probe.applied.value >= 1L || run.fetchFailuresForShuffle.nonEmpty,
      "the pressure probe must have read a live executor's own allowance, yet none reported " +
        s"doing so and the run recorded no fetch failure against shuffle ${run.shuffleId} " +
        s"either: ${run.probe.describe}")

    // And the degradation, on the running application's own policy. The live run above absorbed its
    // pressure the way the specification orders the two responses -- spill first, stand down only
    // when an allocation cannot be met at all -- so it drove bytes to disk and went on streaming,
    // which is why the stand-down is driven here rather than read off that run. Driven exactly as
    // the network-saturation scenario drives its own condition, through the production trip: a
    // reservation for one whole block granted nothing, which is the OOM risk the condition names.
    // The policy is the DRIVER's, which is the one every later registration observes, so the handle
    // asserted below is one this manager chose and not one this case built.
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
      // The delegation made visible rather than inferred: registration happens on the DRIVER, so a
      // shuffle registered while the policy is stood down carries a sort-based handle and every
      // writer and reader built for it is a sort one.
      val dependency = shuffled.dependencies.head.asInstanceOf[ShuffleDependency[Int, String, _]]
      assert(!dependency.shuffleHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
        "a shuffle registered while memory pressure stands streaming down must carry a " +
          s"sort-based handle but carried a ${dependency.shuffleHandle.getClass.getName}")
      liveDigestOf(shuffled)
    }
    assertNoDataLoss(degraded, baseline,
      "a shuffle delegated to sort-based shuffle under memory pressure")
  }

  // =============================================================================================
  // Scenario 5 of 10: disk failure during spill.
  // =============================================================================================

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

    // And the job, with the device failure ACTIVE while it runs. The bytes destroyed are the exact
    // bytes the resolver would have served: files a producer committed and the registry took
    // ownership of, removed from under it mid-drain, so the resolver's own error handling is what
    // runs and the consumers reading it recover by recomputation.
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

  // =============================================================================================
  // Scenario 6 of 10: checksum mismatch on receive.
  // =============================================================================================

  test("a checksum mismatch on receive repairs inside the window and escalates outside it") {
    val baseline = liveSortBaseline(SpillingDatasetBytes)
    // The minimum allowance a third time, for the same reason and to a different end: the live half
    // corrupts a byte of a committed spill file rather than removing it, so what the consumer meets
    // is a block that arrives and fails verification.
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
    // A request built from one position is the inclusive single-element window a corrupt block
    // calls for, so both of its ends are that position.
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

    // A run of corrupt blocks is repaired by ONE request naming the whole closed interval, which is
    // what makes a multi-block repair atomic: the producer charges one attempt and arms one backoff
    // for it, so no sibling position can be deferred behind another's pause.
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
    assert(RetransmitRequestMessage.MAX_REQUESTED_BLOCKS === MaxRequestedBlocksPerRequest,
      "the widest window a request may name must be the protocol's own figure but was " +
        RetransmitRequestMessage.MAX_REQUESTED_BLOCKS)
    val tooWide = intercept[IllegalArgumentException] {
      retransmitRequest(ShuffleId, MapId, PartitionId, FirstSequence,
        FirstSequence + MaxRequestedBlocksPerRequest)
    }
    assert(tooWide.getMessage != null,
      "a window one block wider than the ceiling must be refused with a message that says why")

    // The repair itself: every block of the window is still servable, byte for byte, from memory or
    // from spill. That is what makes a retransmission a repair rather than a second corruption.
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
        discardFramesAndEscalate(Seq(recordingBufferTaken(samplePayload)), Seq.empty, PartitionId)
      }
    }
    assert(context.fetchFailed.contains(escalation),
      "corruption outside the retained window must escalate to the fetch failure that makes the " +
        "unmodified scheduler recompute the upstream stage")

    // And the job, with real corruption in real bytes while it runs. A byte at the end of every
    // retained file for this shuffle is flipped in place, so the file still exists and still reads
    // and what fails is verification -- repair inside the unacknowledged window where the producer
    // still holds the block, escalation outside it where it does not. Either way the answer comes
    // out whole, which is the only claim that matters.
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

  // =============================================================================================
  // Scenario 7 of 10: connection timeout during transfer.
  // =============================================================================================

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

    // The consequence, driven through PRODUCTION on a real read. A producer delivers a block and
    // then goes silent -- no further block, no heartbeat, no orderly end of stream, which is
    // exactly what a stalled transfer looks like to a consumer. Production decides when that
    // silence has lasted long enough, what to discard, what to tell the driver and what to raise;
    // and the near-miss half is asserted on the same real handler, so "the timer did not fire
    // early" is a claim about production rather than about a constant.
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

  // =============================================================================================
  // Scenario 8 of 10: executor JVM GC pause.
  // =============================================================================================

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

    // And the job, with a real pause in a real consumer: a consuming task stops consuming for a
    // period comfortably INSIDE the five-second bound and then carries on. This is the one scenario
    // whose claim is negative -- an ordinary pause must not be mistaken for a loss -- so the
    // assertion is the absence of a fetch failure, which is the only way a mistaken loss could
    // reach the scheduler.
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

  // =============================================================================================
  // Scenario 9 of 10: multiple concurrent producer failures.
  // =============================================================================================

  test("multiple concurrent producer failures invalidate atomically and independently") {
    val baseline = liveSortBaseline()
    startLiveStreamingApplication("streaming-shuffle-concurrent-producer-failures")

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
    // else's. `discardFramesAndEscalate` asserts the independence from the inside, and the loop
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
    // generations, each of which has delivered blocks, and one of them is taken away. What belongs
    // to the lost generation and what does not is production's judgement, not this suite's: the
    // credit ledgers of the survivors, the counter, and the single invalidation message naming one
    // generation are all read back after production has decided. A reduce task that mixed a lost
    // producer's discarded bytes with a healthy one's retained bytes would be the defect this
    // asserts against, and it is unreachable from a single-producer fixture.
    //
    // The loop runs twice, losing a different generation each time, because a suite that only ever
    // loses the first producer cannot tell "the invalidation is per producer" from "the
    // invalidation happens to name the first one".
    Seq(FixtureFirstMapIndex, ConcurrentProducerCount - 1).foreach { lostMapIndex =>
      val invalidationsBefore = observedPartialReadInvalidations()
      val fixture = new ProducerLossFixture(numMaps = ConcurrentProducerCount)
      val failure = withProducerLoss(fixture) { losing =>
        val streamed = losing.reader.read()
        // Read order is map-index order, so the producers AHEAD of the lost one are completed --
        // every block and an orderly end of stream -- and the rest deliver a first block and stop.
        // That shape is what makes the case terminating as well as meaningful: the reader consumes
        // the completed producers, arrives at the severed one and has to decide, rather than
        // parking on an incomplete producer it reaches first and waiting for a block that will
        // never come. Every producer still has accepted bytes for the invalidation to be atomic
        // about, and a credit ledger of its own to be left alone.
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

        // What the survivors kept, which is the decisive half of "atomic AND per producer". Their
        // credit ledgers are still held and their accepted blocks are still counted: the reader's
        // own task-completion cleanup owns releasing a ledger, and it has not run, so anything
        // missing here would be an invalidation that reached past the generation it named.
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

    // And the job, with concurrent losses ACTIVE while it runs and two shuffles chained so the
    // application really does carry concurrent producers. Three map partitions die part way along
    // their writes on their first attempts, and the withdrawal then takes every producer one
    // executor holds for the shuffle in a single act -- which is what makes the invalidation
    // concurrent rather than serial.
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

  // =============================================================================================
  // Scenario 10 of 10: consumer reconnect after extended downtime.
  // =============================================================================================

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

    // Branch two: the window has been reclaimed, so there is nothing left to replay.
    val released = store.acknowledge(ConsumerId, PartitionId, lastSequence)
    assert(released > 0L,
      s"acknowledging through $lastSequence must reclaim the window but reclaimed $released")
    assert(store.bufferedBytes === 0L, "a fully acknowledged window must hold nothing")
    assert(store.lowestRetainedSequence(PartitionId) === MemorySpillManager.UNSET_SEQUENCE,
      "a reclaimed window must retain no sequence at all, which is what makes a later replay " +
        "unserviceable rather than partially serviceable")
    // What a consumer that comes back to an unserviceable window actually meets, driven through
    // PRODUCTION: it asks, nothing can answer, and production -- not this suite -- decides that the
    // producer is gone, discards what it had accepted, counts the invalidation, retires the
    // generation and raises the one exception the scheduler recognises. The producer-side half
    // above established that the bytes are no longer held; this is the consumer-side consequence of
    // the same state.
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

    // And the job, with a real absence in a real consumer: a consuming task stops consuming for
    // longer than the ten-second acknowledgement window and then returns. What carries the stream
    // across it is the producer's retention and replay, or -- where the window has moved on --
    // escalation and recomputation. The scenario claims either is clean, so what is asserted is
    // that the absence really happened and the answer still came out whole.
    val run = runLiveFault(
      ConsumerReconnectAfterDowntime, baseline, new LiveConsumerStall(ExtendedStallMillis))
    assert(run.probe.applied.value >= 1L,
      s"a consumer must actually have been absent for $ExtendedStallMillis ms, or its return is " +
        "untested: " + run.probe.describe)
    assert(run.probe.effect.value >= 1L,
      "the absent consumer must have been on an executor with the streaming subsystem in " +
        "service, or it was absent from a sort-based read: " + run.probe.describe)
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

/**
 * A fault a reduce-side closure can apply to the executor it is running on, mid-drain.
 *
 * Each one acts through the production path it names, and none of them adds a branch to production
 * code: withdrawing retained output is the registry teardown `unregisterShuffle` performs; breaking
 * links is the channel-global close the subsystem performs when a frame impugns a connection;
 * deleting or corrupting a retained spill file damages exactly the bytes the resolver would have
 * served, so the resolver's own error handling is what runs.
 */
private sealed trait LiveStreamingFault extends Serializable {

  /**
   * Applies this fault on the executor running the closure.
   *
   * @param manager the streaming manager in service on this executor
   * @return a count describing what the fault reached, reported to the driver as a diagnostic
   */
  def applyTo(manager: StreamingShuffleManager): Int

  /**
   * Tells this fault which shuffle it is about to interrupt.
   *
   * Called on the driver once the graph exists and before the action submits it, which is the only
   * window in which the id is known and the closure has not yet been serialised. Faults that do not
   * name a shuffle ignore it.
   *
   * @param shuffleId the shuffle the run registered
   */
  def bindShuffle(shuffleId: Int): Unit = ()
}

/**
 * Withdraws one shuffle's retained producer output from this executor's registry.
 *
 * Every block this executor served for that shuffle becomes unobtainable, exactly as it would had
 * the JVM holding it died, so the consumers draining it invalidate per producer and raise the fetch
 * failure the unmodified scheduler answers by recomputing.
 */
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

/**
 * Kills the consuming task itself, part way through its drain.
 *
 * This is the consumer's half of a loss, and it is the only one of these faults whose subject is
 * the task that applies it: a reduce task that has received some of its input and acknowledged some
 * of it dies, so the producer is left holding an unacknowledged window that the RETRY must be
 * served from. The retry is what the scenario is really about -- it either replays the retained
 * window or escalates to recomputation, and either way the answer has to come out whole.
 *
 * The throw is unconditional because the injection site already guarantees it happens once per
 * executor and only on a first attempt, so the retry meets a latch already taken and the job
 * converges.
 */
private class LiveConsumerLoss extends LiveStreamingFault {

  override def applyTo(manager: StreamingShuffleManager): Int = {
    throw new IllegalStateException(
      s"${LiveStreamingFault.CONSUMER_LOSS_MARKER} on executor ${SparkEnv.get.executorId}")
  }
}

/**
 * Breaks every link this executor serves, and keeps them broken for [[holdMillis]].
 *
 * Producers stay REGISTERED and keep their output, which is what distinguishes a partition from a
 * lost producer: consumers fall silent and time out, and a later attempt reconnects and finds the
 * output waiting. One close is not a partition -- the consumer reconnects immediately and the
 * five-second liveness bound never expires -- so the link is held unusable for longer than that
 * bound by a bounded daemon sweep, and the first close retries until it finds a channel, because
 * whether one exists at the exact instant of injection is a scheduling accident rather than a
 * property of the run.
 */
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

/**
 * Damages the retained spill files this executor would serve for one shuffle.
 *
 * Two shapes, and they exercise two different production error paths against the same bytes.
 * Deleting a file is a disk that has lost the data the resolver committed to it, so the read fails
 * outright. Overwriting a file's bytes leaves the read succeeding and the CRC32C failing, which is
 * the corruption path -- repair inside the unacknowledged window, escalation outside it.
 *
 * Damaging the files rather than simulating the failure is what makes this live: these are the
 * exact bytes [[StreamingShuffleBlockResolver]] hands to a consumer once the producing task has
 * gone.
 */
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
        // One byte, flipped in place at the very END of the file. The file still exists and still
        // reads, so what fails is the checksum and not the I/O -- which is the distinction this
        // fault exists to draw. The last byte rather than the first because the fault lands after a
        // consumer has already taken delivery of some records: damage to the bytes it has already
        // been handed would change nothing it is going to read, and this scenario is about a block
        // that arrives corrupted rather than one that arrived intact.
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

/**
 * Stalls the consumer for [[stallMillis]] without consuming, then lets it resume.
 *
 * A stalled consumer is what a GC pause and an extended consumer downtime both look like from the
 * producer's side: acknowledgements stop arriving while the stream stays open. A stall shorter than
 * the liveness bound must not be mistaken for a loss; one longer than it must be survived by
 * retention and replay. The wait is a bounded `await` on a latch nothing counts down, which is the
 * idiom the reader itself uses for a poll window -- nothing here sleeps for an interval it then
 * treats as having elapsed.
 */
private class LiveConsumerStall(val stallMillis: Long) extends LiveStreamingFault {

  override def applyTo(manager: StreamingShuffleManager): Int = {
    val idle = new CountDownLatch(1)
    idle.await(stallMillis, TimeUnit.MILLISECONDS)
    if (manager.boundStreamingListener.isDefined) 1 else 0
  }
}

/**
 * Reads this executor's buffer allowance and reports how many reservations it has refused.
 *
 * The fault here is not applied by this object at all -- it is applied by the CONFIGURATION the
 * scenario runs under, which holds the buffer allowance at its documented minimum so that a shuffle
 * of this size cannot fit. What this reports is whether the executor's own quota actually refused,
 * which is the difference between a scenario that ran under memory pressure and one that merely
 * declared it.
 */
private class LiveMemoryPressureProbe extends LiveStreamingFault {

  override def applyTo(manager: StreamingShuffleManager): Int = {
    val quota = MemorySpillManager.executorQuota(SparkEnv.get.conf)
    math.min(Int.MaxValue.toLong, quota.refusalCount).toInt
  }
}

private object LiveStreamingFault {

  /** Shuffle id a fault carries before the driver has supplied one. Matches no real shuffle. */
  val NO_SHUFFLE: Int = -1

  /** Operator-facing reason a partition fault carries into each channel's diagnostic. */
  val PARTITION_REASON: String = "a failure-injection scenario partitioned every streaming link"

  /** Phrase an injected consumer loss carries, so only this suite's own losses are counted. */
  val CONSUMER_LOSS_MARKER: String = "Injected streaming shuffle live consumer loss"

  /** Attempts the partition fault makes to find a channel to close before giving up. */
  val CLOSE_ATTEMPTS: Int = 30

  /** Interval between close attempts and between sweeps. */
  val SWEEP_MILLIS: Long = 100L

  /**
   * Whether this executor JVM has already injected a fault.
   *
   * One `object` per JVM, so one injection per executor, which is the whole budget needed: every
   * fault acts on the executor it runs on, so letting each executor inject once covers every
   * executor that holds producers or serves links, without depending on which partition was
   * scheduled where. Latching it is what makes the job converge, because the recomputation the
   * fault provokes meets a latch already taken.
   */
  private val injected = new AtomicBoolean(false)

  /**
   * Runs the injection if this executor has not injected yet.
   *
   * @param body the injection, run at most once per JVM
   */
  def injectOncePerExecutor(body: => Unit): Unit = {
    if (injected.compareAndSet(false, true)) {
      body
    }
  }

  /**
   * Returns the per-executor claim, so one case's injection cannot silence the next case's.
   *
   * The latch above is per JVM, and under `local[n]` the driver IS the executor -- so without this
   * every case after the first would run its workload with its fault never applied and assert
   * against a run that saw nothing. Called from the suite's `beforeEach` and `afterEach`, beside
   * the other process-scoped state returned there.
   */
  def resetInjection(): Unit = injected.set(false)

  /** The streaming manager in service on this JVM, or absent on the sort-based path. */
  def streamingManager(): Option[StreamingShuffleManager] = {
    SparkEnv.get.shuffleManager match {
      case manager: StreamingShuffleManager => Some(manager)
      case _ => None
    }
  }

  /** The streaming block registry behind a manager's published resolver, if it has one. */
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
 * The first three are taken BEFORE the destructive act, so they arrive whatever becomes of the task
 * that took them; the last two describe the act itself and are diagnostics only, because a task the
 * fault destroys may be killed before its updates are merged.
 *
 * @param offers consumers that reached the injection point
 * @param receipt records those consumers had already received
 * @param egress bytes the producing side had already put on the wire
 * @param applied injections that ran, at most one per executor JVM
 * @param effect what those injections reached
 */
private case class LiveFaultProbe(
    offers: LongAccumulator,
    receipt: LongAccumulator,
    egress: LongAccumulator,
    applied: LongAccumulator,
    effect: LongAccumulator) {

  /** Every reading, rendered for a failure message. */
  def describe: String = {
    s"${offers.value} offer(s), ${receipt.value} record(s) received, ${egress.value} byte(s) " +
      s"streamed, ${applied.value} applied, effect ${effect.value}"
  }
}
