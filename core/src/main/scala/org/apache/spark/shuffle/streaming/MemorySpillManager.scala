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

import java.io.{BufferedInputStream, ByteArrayOutputStream, File, FileInputStream, InputStream}
import java.nio.file.Files
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ScheduledExecutorService,
  TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{BLOCK_ID, BYTE_SIZE, CLASS_NAME, COUNT, DURATION,
  FILE_NAME, MAX_SIZE, MEMORY_SIZE, NUM_BYTES, NUM_EVENTS, NUM_PARTITIONS, NUM_SKIPPED,
  PARTITION_ID, PATH, REASON, THREAD_NAME, THRESHOLD, TIMEOUT}
import org.apache.spark.internal.config.{CPUS_PER_TASK, EXECUTOR_CORES, EXECUTOR_MEMORY,
  SHUFFLE_FILE_BUFFER_SIZE, SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_DEBUG,
  SHUFFLE_STREAMING_SPILL_THRESHOLD}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.network.shuffle.protocol.streaming.DataBlockMessage
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.serializer.{DummySerializerInstance, SerializerInstance, SerializerManager}
import org.apache.spark.storage.{BlockManager, DiskBlockManager, DiskBlockObjectWriter,
  TempShuffleBlockId}
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * Owns the bounded, spillable buffer budget of the streaming shuffle producer.
 *
 * The streaming shuffle path holds already-framed blocks in memory between the moment they are
 * produced and the moment the consumer acknowledges them, because an acknowledged block is the
 * only block whose *memory* may be discarded. That window is what makes retransmission possible,
 * and it is also what makes a hard memory bound mandatory. This class is that bound: it admits
 * blocks against an explicit budget, evicts to local disk when utilisation crosses the configured
 * threshold, and releases memory the instant an acknowledgement arrives.
 *
 * Two lifetimes meet here, and keeping them apart is the class's other job. Buffered bytes belong
 * to the producing task: they are acquired from its memory manager and have to be returned when it
 * ends. Spilled segments belong to the shuffle: they are ordinary files in the local directories,
 * and the reduce side reads them after the map stage has finished, because that is the only time
 * the scheduler lets it read at all. So an acknowledgement releases memory and never a segment, and
 * a successful producer hands its files to the executor-scoped block resolver -- which unlinks them
 * at generation withdrawal, at shuffle unregistration or at its own shutdown -- rather than
 * deleting output that a later attempt of a reduce task is entitled to read again.
 *
 * Extension by subclassing only. This class extends [[MemoryConsumer]] and overrides its abstract
 * `spill(long, MemoryConsumer)` callback. That single decision is what buys the streaming path
 * Spark's existing memory accounting, its existing cross-consumer pressure arbitration and its
 * existing task-level leak detection for free, without a line of change to `MemoryManager`,
 * `UnifiedMemoryManager`, `MemoryConsumer` or `TaskMemoryManager` -- all of which are preservation
 * zones. Registration with the task memory manager happens simply by being a `MemoryConsumer`
 * subclass, so there is nothing to wire.
 *
 * What a block costs. A block is charged its payload length plus
 * [[MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES]], and that charge -- never the bare payload length
 * -- is what every accounting path reserves, releases and reports. The overhead folds together the
 * streaming protocol's own framing, read from `DataBlockMessage` so that the payload cap and the
 * encoded-frame cap are single-sourced rather than restated per layer, and the JVM cost of
 * retaining a block as an object in a queue. Charging the payload alone would let a stream of small
 * blocks grow the heap while reported utilisation stayed near zero, which is precisely the bound
 * this class exists to enforce.
 *
 * Budget derivation, and why it is executor scoped. The aggregate allowance is
 * `executorMemory * bufferSizePercent / 100` and the per-partition allowance is that aggregate
 * divided by the reduce partition count, which is exactly the contracted
 * `(executorMemory * bufferPercent) / numPartitions` with `bufferPercent` read as the percentage it
 * is named for. `executorMemory` is `spark.executor.memory`, the configured figure the property is
 * documented against, and it is the '''same''' figure the consumer side of a shuffle is bounded by
 * -- one property with one basis. A configuration value rather than a live reading of the memory
 * manager, which also means the allowance can be derived, and the utilisation gauge read, without
 * dereferencing `SparkEnv`.
 *
 * Crucially, that allowance is held by one [[MemorySpillManager.ExecutorBufferQuota]] shared by
 * every instance on the executor, not recomputed per instance. An executor runs many tasks at once,
 * so an instance that enforced the full `bufferSizePercent` on its own would let `n` concurrent
 * producers hold `n * bufferSizePercent` of the heap between them and the contracted bound would be
 * a per-task bound wearing an executor-wide name. Admission therefore reserves against the shared
 * quota with a single atomic compare-and-set, and the reported utilisation gauge is likewise
 * executor scoped, because that is the figure the spill threshold is meaningfully compared against.
 *
 * Deferred environment access. Every [[SparkEnv]] dereference in this class sits behind a
 * `lazy val`, and that is load bearing rather than stylistic. On the driver the shuffle manager is
 * initialised strictly before the memory manager, so a component constructed from the streaming
 * shuffle manager's constructor observes a null `SparkEnv.get.memoryManager`; the block manager is
 * likewise not valid until its own `initialize()` has run. Deferring to first use -- which always
 * happens inside a task, where both are live -- is the same remedy the block manager itself
 * adopted for this hazard. An eager dereference here would be a guaranteed
 * `NullPointerException` on the driver.
 *
 * Threshold polling. Utilisation is compared against `spark.shuffle.streaming.spillThreshold` on a
 * 100 ms cadence measured with the injected [[Clock]]. The cadence is driven by one shared daemon
 * ticker per executor -- not one thread per task -- which every instance constructed with
 * `autoPoll` enabled joins and which it leaves again on [[close]]. Relying on the owner to call
 * [[pollOnce]] would have left the contracted 100 ms cadence unmet on any path that stopped
 * calling, for instance a producer already blocked waiting for room, which is exactly when the
 * threshold check matters most. The ticker is a plain driver of the same public [[pollOnce]] the
 * owner may still call: the cadence gate suppresses work in between either way, and [[maybeSpill]]
 * evaluates the threshold immediately when the cadence is not wanted. Passing `autoPoll = false`
 * withholds the instance from the ticker so the whole component stays deterministic under an
 * injected clock, which is what makes the polling cadence, the eviction order and the reclamation
 * latency assertable without a single timing-dependent test.
 *
 * Eviction order. On a trip the largest buffered partitions are evicted first, ties broken
 * least-recently-used first and then by ascending partition id so the order is total and
 * reproducible. [[spillSelectionOrder]] exposes precisely the order eviction will follow.
 *
 * Spill mechanics. Each evicted partition is written to its own temporary shuffle block obtained
 * from `DiskBlockManager.createTempShuffleBlock()` through a `DiskBlockObjectWriter` obtained from
 * `BlockManager.getDiskWriter`, and every block is committed individually so that each one lands
 * as an independently decodable unit with an exact offset and length. The resulting
 * [[MemorySpillManager.SpilledBlock]] records carry everything a block resolver needs to serve or
 * retransmit a single spilled block. Two consequences of using the sanctioned block-manager path
 * are worth stating plainly: the bytes on disk are wrapped by `SerializerManager` exactly as every
 * other Spark spill file is, so a reader must unwrap them the same way; and their compression is
 * governed by `spark.shuffle.compress` rather than by the spill-specific codec, which is precisely
 * why a temporary *shuffle* block is used and not a temporary local one.
 *
 * Accounting. Spilled volumes are reported on Spark's existing accumulators -- `memoryBytesSpilled`
 * `diskBytesSpilled` and `peakExecutionMemory` on [[org.apache.spark.executor.TaskMetrics]] -- so
 * the streaming path shows up on every observability surface Spark already has: the UI, the
 * history server, the event log and the metrics REST API. No parallel counter is introduced. The
 * streaming-specific spill *frequency* counter and the buffer-utilisation gauge on
 * [[StreamingShuffleMetricsSource]] complement those accumulators, they do not replace them. The
 * disk writer is handed a freshly allocated [[ShuffleWriteMetrics]] rather than the task's own
 * shuffle-write reporter, because `DiskBlockObjectWriter` reports bytes, records and write time to
 * whatever reporter it is given and reusing the task's would double count spilled bytes as
 * shuffle-written bytes.
 *
 * Concurrency. All buffer bookkeeping is guarded by one private monitor, and every decision that
 * must be indivisible is taken inside it: the lifecycle check, the sequence check, the retained
 * block count check, the per-partition ceiling check and the shared-quota reservation are one
 * critical section, so two producers can never both observe room and both take it. Two invariants
 * govern every path. First, `acquireMemory` and `freeMemory` are never called while the monitor is
 * held: `TaskMemoryManager` runs spill callbacks with its own monitor held, so a thread holding
 * this monitor while waiting for the task memory manager could deadlock against a thread holding
 * the task memory manager while waiting for this monitor, and reserving strictly outside breaks
 * that cycle by construction. Second, disk I/O is never performed while the monitor is held:
 * eviction runs as three stages -- select and detach under the monitor, write with the monitor
 * released, publish or re-attach under the monitor -- because holding it across a write would make
 * the contracted 100 ms reclamation deadline unmeetable, an acknowledgement needing the same
 * monitor.
 *
 * Lifecycle. The instance is open until [[close]], and closure is decided under the monitor in the
 * same step that admission is refused, so a block can never be admitted into a buffer whose memory
 * has already been released. Bytes reserved but not yet committed are owned by the admitting thread
 * and released by it when it finds the instance closed; bytes already committed are owned by the
 * instance and released exactly once by [[close]], which frees its own precise tally rather than
 * whatever the memory manager happens to report. A residual reconciliation against the memory
 * manager still runs, but only when no admission is in flight, so it can never race a release.
 *
 * Sequence discipline. A partition's blocks form one gap-free ascending run. The first block
 * admitted for a partition establishes the origin at whatever non-negative sequence it carries and
 * every later block must be exactly its predecessor plus one; a duplicate or a gap is rejected as a
 * caller defect rather than accepted. Prefix acknowledgement depends on this: an acknowledgement
 * retires every block at or below a position, so a gap would retire bytes that were never charged
 * and a duplicate would charge the same block twice. The watermark survives a partition emptying,
 * which is why an empty partition's bookkeeping is retained rather than discarded -- the number of
 * entries is bounded by the reduce partition count either way.
 *
 * Cleanup. Release is registered on `TaskContext.addTaskCompletionListener`, so buffered memory is
 * freed on success, on failure and on cancellation alike. This is the mechanism by which the
 * zero-leak requirement is met, and it is machine checked: the test JVM runs with
 * `spark.unsafe.exceptionOnMemoryLeak` enabled, so a retained reservation fails the build rather
 * than passing silently. What becomes of the spill files at that moment depends on whether this
 * instance still owns them: a producer that succeeded hands them to the executor-scoped block
 * resolver first and [[close]] then leaves every one in place, while a producer that failed still
 * owns them and [[close]] unlinks them all. Files are reference counted either way rather than
 * unlinked the instant their owner is done with them, because a block resolver hands out lazily
 * opened file segments: a reader takes a lease with [[acquireSpillFileLease]] and drops it with
 * [[releaseSpillFileLease]], and an unlink waits on the last lease.
 *
 * Configuration is read once here and held immutably, which is what makes "configuration changes
 * require an executor restart" true by construction rather than by documentation.
 *
 * Example use from the streaming shuffle producer:
 *
 * {{{
 *   val spillManager = new MemorySpillManager(context.taskMemoryManager(), conf)
 *   spillManager.registerPartitionCount(handle.numPartitions)
 *   spillManager.registerCleanup(context)
 *   spillManager.registerConsumer(consumerExecutorId)
 *   ...
 *   if (!spillManager.bufferBlock(partitionId, sequenceNumber, blockBytes)) {
 *     // Budget exhausted and nothing left to evict: hold the block and retry, or fall back.
 *   }
 *   spillManager.pollOnce()
 *   ...
 *   spillManager.acknowledge(consumerExecutorId, partitionId, consumerPosition)
 * }}}
 *
 * @param taskMemoryManager the task memory manager this consumer reserves execution memory from
 * @param conf the configuration to read the streaming shuffle buffer keys from, exactly once
 * @param clock the time source used for the polling cadence, for last-access ordering and for
 *              latency measurement; injectable so that every timing-sensitive behaviour of this
 *              class can be driven deterministically from a test
 * @param quotaOverride an explicit executor buffer quota to draw from instead of the shared one;
 *                      supplied only by tests, which must be able to exercise admission against a
 *                      budget of their own choosing without depending on the host JVM's heap
 * @param autoPoll whether to join the executor's shared 100 ms threshold ticker; disabled by tests
 *                 that drive [[pollOnce]] themselves so that no background thread perturbs what
 *                 they are measuring
 * @param diskQuotaOverride explicit executor disk allowance supplied only by quota-bound tests
 */
private[spark] class MemorySpillManager(
    taskMemoryManager: TaskMemoryManager,
    conf: SparkConf,
    clock: Clock = new SystemClock,
    quotaOverride: Option[MemorySpillManager.ExecutorBufferQuota] = None,
    autoPoll: Boolean = true,
    diskQuotaOverride: Option[MemorySpillManager.ExecutorDiskQuota] = None)
  extends MemoryConsumer(taskMemoryManager, MemoryMode.ON_HEAP) with Logging {

  import MemorySpillManager._

  // Configuration. Read once, held immutably. The two percentage keys are range validated by their
  // own ConfigEntry definitions, so an out-of-range value is rejected here at read time with
  // INVALID_CONF_VALUE.REQUIREMENT and never reaches the arithmetic below.

  // Held for diagnostics only. The percentages that actually size the budget are read once by the
  // executor-scoped quota, because the budget is shared and must be derived once per executor
  // rather than once per instance; see [[MemorySpillManager.ExecutorBufferQuota]].
  private val spillThresholdPercent: Int = conf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD)

  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  /**
   * Map tasks that can run at once in this JVM, which is the second divisor of a producer's framing
   * share. See [[MemorySpillManager.executorTaskSlots]] for why the buffer arithmetic needs it: the
   * allowance is executor wide, so a share sized for one task is claimed once per task slot.
   *
   * Read from the configuration at construction and held immutably, so it is a constant for the
   * life of the executor rather than a reading that moves with load.
   */
  val concurrentTaskSlots: Int = MemorySpillManager.executorTaskSlots(conf)

  // Matches the sizing the sort-based spill path uses for its own disk writers, so streaming spill
  // files are buffered identically to every other spill file this executor produces.
  private val fileBufferSizeBytes: Int = conf.get(SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024

  // Deferred environment access. See the class comment: none of these may be dereferenced during
  // construction, because the shuffle manager that owns this component is built before the memory
  // manager exists on the driver.

  private lazy val blockManager: BlockManager = SparkEnv.get.blockManager

  private lazy val diskBlockManager: DiskBlockManager = blockManager.diskBlockManager

  /**
   * The executor's serializer manager, which owns the compression and encryption a spill file is
   * wrapped in.
   *
   * It is read here for one purpose only: a block read back out of a spill segment has to be
   * unwrapped by exactly the manager, and with exactly the block id, that `DiskBlockObjectWriter`
   * wrapped it with on the way in. Taking it from the block manager rather than from `SparkEnv`
   * directly keeps every deferred environment access in this component behind the same single
   * dereference.
   */
  private lazy val serializerManager: SerializerManager = blockManager.serializerManager

  /**
   * The serializer handed to every spill writer, and deliberately the no-op one.
   *
   * Spilled blocks are written as raw bytes through `DiskBlockObjectWriter.write(Array[Byte],
   * ...)` because they are already framed, so no object is ever serialized here. That makes the
   * choice of serializer look immaterial, and it is not: `DiskBlockObjectWriter.open()` calls
   * `serializerInstance.serializeStream(...)` on the compression stream, and a real serializer --
   * `JavaSerializerInstance` being the default -- writes its own stream preamble at that moment.
   * The writer re-opens on the first write after every `commitAndGet`, so a real serializer would
   * inject that preamble ahead of *every* committed segment, and the bytes read back would no
   * longer be the framed block that was spilled. Corruption would be silent: each segment would
   * decode as a protocol message whose header was really a serialization magic number.
   *
   * The no-op instance's `serializeStream` returns a pass-through that forwards only `flush` and
   * `close`, so the segment on disk is byte-for-byte the payload that was handed in, wrapped by
   * nothing but the `SerializerManager` compression the block id selects. This is exactly what the
   * sort-based spill path does for the same reason -- see `ShuffleExternalSorter` and
   * `UnsafeSorterSpillWriter`, both of which pass this same instance.
   *
   * It is a plain `val` rather than a lazy one because it dereferences no part of `SparkEnv`, which
   * also removes one deferred environment access from the spill path.
   */
  private val serializerInstance: SerializerInstance = DummySerializerInstance.INSTANCE

  /**
   * The executor-wide buffer allowance this instance draws from. Shared by every instance on the
   * executor unless a test supplied one of its own, so that `n` concurrent producers cannot hold
   * `n * bufferSizePercent` of the heap between them. Deferred like every other environment
   * dereference, because deriving the budget reads the memory manager.
   */
  private lazy val quota: ExecutorBufferQuota =
    quotaOverride.getOrElse(MemorySpillManager.executorQuota(conf))

  /** Executor-wide disk allowance shared by every streaming producer in this JVM. */
  private lazy val diskQuota: ExecutorDiskQuota =
    diskQuotaOverride.getOrElse(MemorySpillManager.executorDiskQuota(conf, quota))

  // Mutable state. Everything in this block is guarded by `lock` except the atomics, which are
  // deliberately lock free so that a diagnostic read never contends with a producer.

  private val lock = new Object()

  private val partitionBuffers = new mutable.HashMap[Int, PartitionBuffer]()

  private var bufferedMemoryBytes = 0L

  // Bytes reserved from the shared quota and from the task memory manager for admissions that have
  // been granted room but have not yet been committed into a partition queue. Guarded by `lock`.
  // These bytes belong to the admitting thread, not to this instance, which is what lets closure
  // release its own committed tally without ever double-releasing an admission in flight.
  private var pendingReservedBytes = 0L

  // Number of admissions currently between "granted room" and "committed or rolled back". Guarded
  // by `lock`. Closure consults it to decide whether a residual reconciliation against the memory
  // manager is safe: with no admission in flight, and with no further admission possible once the
  // instance is closed, nothing can be acquired after the reconciliation reads.
  private var inFlightAdmissions = 0

  // Whether an eviction is between its select-and-detach stage and its publish-or-re-attach stage.
  // Guarded by `lock`. Only one eviction runs at a time, so a second caller observing this simply
  // declines rather than planning an overlapping eviction of partitions already being written.
  //
  // A caller that needs room, or that needs every byte out of memory, must *wait* for that eviction
  // rather than conclude from it that nothing can be freed: the detached bytes have already left
  // every partition's own tally, so nothing is selectable while it runs, yet they are still counted
  // in the aggregate until the publish stage lands. `lock.notifyAll()` in the publish stage and
  // [[awaitEvictionQuiescence]] on this side are what turn that window from a false "budget
  // exhausted" verdict into a short wait for the reclamation that is already under way.
  private var evictionInProgress = false

  // Outstanding reader leases per spill file, and the set of files whose last record has been
  // retired. A file is unlinked only when it is retired and holds no lease. Both guarded by `lock`.
  private val spillFileLeases = new mutable.HashMap[File, Int]()

  private val retiredSpillFiles = new mutable.HashSet[File]()

  // O(1) index between a spill file and the temporary block identity it was allocated under, in
  // both directions. Every record of one eviction of one partition is written into a single file
  // under a single identity, so the two are in bijection and one entry stands for the file. Both
  // guarded by `lock`. Their purpose is to answer "which producer owns this block name" with one
  // probe per producer rather than a walk of every producer's every retained segment, whose cost
  // grew with how much the executor had spilled while the request paying for it stayed fixed.
  private val spillFilesByBlockId = new mutable.HashMap[TempShuffleBlockId, File]()

  private val spillFileBlockIds = new mutable.HashMap[File, TempShuffleBlockId]()

  // Retained spill records across every partition. Guarded by `lock`, and separately charged to the
  // aggregate metadata category for as long as they remain reachable.
  private var spilledRecordCount = 0

  private var spilledMetadataReservedBytes = 0L

  // Acknowledged position per registered consumer, per partition. Guarded by `lock`, because a
  // position and the memory retirement it authorises must be decided in the same indivisible step.
  // Only a consumer present here may acknowledge anything, and retirement never advances past the
  // slowest entry, so a fast consumer cannot free memory holding a block a slower sibling has yet
  // to receive. Positions outlive an owning task whose files were handed on, because the
  // acknowledgement that retires the last of them can arrive after the task has ended.
  private val consumerPositions = new mutable.HashMap[String, mutable.HashMap[Int, Long]]()

  // Partitions whose acknowledged prefix extends beyond the most recent bounded reclamation batch.
  // Guarded by `lock`. The set makes queueing idempotent; the queue preserves fair arrival order.
  private val pendingReclamationPartitions = new mutable.ArrayDeque[Int]()
  private val pendingReclamationSet = new mutable.HashSet[Int]()
  private val reclamationRequestedAtMs = new mutable.HashMap[Int, Long]()

  // At most one entry for this manager may sit on the executor-scoped dispatch queue.
  private val reclamationDispatchQueued = new AtomicBoolean(false)

  private val reclamationBatchTotal = new AtomicLong(0L)

  private var lastPollTimeMs = -1L

  private val partitionCount = new AtomicInteger(UNREGISTERED_PARTITION_COUNT)

  private val memoryPressure = new AtomicBoolean(false)

  private val diskPressure = new AtomicBoolean(false)

  private val memoryPressureCount = new AtomicLong(0L)

  private val permanentRefusalCount = new AtomicLong(0L)

  private val permanentRefusalLogged = new AtomicBoolean(false)

  private val memoryBytesSpilledTotal = new AtomicLong(0L)

  private val diskBytesSpilledTotal = new AtomicLong(0L)

  private val spillCountTotal = new AtomicLong(0L)

  // End-of-stream durability flushes, counted apart from threshold-driven evictions. See
  // [[durabilityFlushCount]] for why the two must not share a counter.
  private val durabilityFlushTotal = new AtomicLong(0L)

  // Blocks written straight to disk because the allowance could not admit them to memory. Counted
  // apart from both eviction counters: nothing was reclaimed and no threshold was crossed, so this
  // is neither a spill event nor a durability flush. See [[durableAdmissionCount]].
  private val durableAdmissionTotal = new AtomicLong(0L)

  private val spillFailureTotal = new AtomicLong(0L)

  private val peakMemoryBytesSeen = new AtomicLong(0L)

  private val lastSpillMs = new AtomicLong(0L)

  private val lastReclamationMs = new AtomicLong(0L)

  // The lifecycle flag. Written only inside `lock`, in the same critical section that refuses
  // admission, so a block can never be admitted into a buffer whose memory has already been
  // released. Held as an atomic purely so that the [[isClosed]] diagnostic and the shared ticker
  // can read it without contending with a producer; the mutual exclusion that makes it correct
  // comes from `lock`, never from the atomic.
  private val closed = new AtomicBoolean(false)

  // Whether the spill files this instance produced have been handed to the executor-scoped block
  // resolver. Guarded by `lock`, in the same critical section that decides what [[close]] deletes,
  // so a hand-off can never race the deletion it exists to prevent.
  //
  // This flag is how retained output outlives the task that produced it. Buffered bytes cannot:
  // they are task-managed execution memory, and the executor frees -- and with
  // `spark.unsafe.exceptionOnMemoryLeak` enabled, fails the task over -- anything still acquired
  // when a task completes. Spilled bytes can, because they are ordinary files in the local
  // directories, so a successful map output makes its unacknowledged window durable and transfers
  // the files rather than trying to keep memory alive past the platform's own boundary.
  private var spillFilesDetached = false

  // Bytes reserved for a producer's framing scratch -- the accumulators a serialization stream
  // writes into before its bytes are cut into blocks. Guarded by `lock`. Held separately from
  // `bufferedMemoryBytes` on purpose: both are charged to the same executor-wide quota, so the
  // configured buffer percentage bounds the two together, but only buffered blocks can be evicted,
  // and mixing scratch into the evictable tally would have the eviction planner promise room it
  // cannot free.
  private var scratchReservedBytes = 0L

  private val metricsReported = new AtomicBoolean(false)

  private val cleanupRegistered = new AtomicBoolean(false)

  // The task that owns this consumer, retained from the first cleanup registration. An owner is
  // entitled to close this consumer from a thread that is not the task thread -- a network event
  // loop completing a stream, for instance -- and `TaskContext.get()` answers nothing there.
  // Retaining the owning context makes the spill accounting independent of which thread performs
  // the close, which is what stops an off-task close from silently discarding it.
  private val ownerContext = new AtomicReference[TaskContext](null)

  private val reclamationBreachTotal = new AtomicLong(0L)

  private val rejectedAcknowledgements = new AtomicLong(0L)

  private val rejectedConsumerRegistrations = new AtomicLong(0L)

  private val spillFileDeletionFailureTotal = new AtomicLong(0L)

  // Each recurring condition this class reports is aggregated through the EXECUTOR-scoped gate of
  // the same name on the companion object, never through a field of this instance. One of these
  // managers exists per streaming map task, so an instance-scoped gate would admit one
  // default-level record per task and make the executor's log volume a function of its task count;
  // see [[MemorySpillManager.ExecutorLogAggregator]] for the full argument. The aggregators quote
  // executor-wide totals for the same reason, so a record standing in for several tasks describes
  // the executor rather than whichever task happened to win the gate.

  /**
   * Per-partition buffer state: the in-memory blocks still awaiting acknowledgement, the blocks an
   * eviction has detached and is currently writing, the durable records of the blocks already on
   * disk, the three byte tallies, the sequence watermarks and the last-access stamp that breaks
   * eviction ties. Every queue is kept in ascending sequence-number order, and every spilled block
   * precedes every in-memory block for the same partition, so a monotonically advancing consumer
   * position always retires a prefix of the memory.
   *
   * `spilledBlockRecords` outlives acknowledgement, unlike the other two queues: the segments it
   * names are this map task's output on local disk, and they stay readable until their owner
   * unlinks them.
   *
   * `spillingBlocks` is owned exclusively by the eviction in progress. Their payloads are still on
   * the heap and still charged, so they are still counted; an acknowledgement covering them only
   * advances [[acknowledgedThroughSequence]] and leaves the eviction to release them when it
   * publishes, which is what keeps a concurrent acknowledgement and a concurrent write from both
   * releasing the same bytes.
   */
  private class PartitionBuffer(val partitionId: Int) {
    val memoryBlocks = new mutable.ArrayDeque[BufferedBlock]()
    val spillingBlocks = new mutable.ArrayDeque[BufferedBlock]()
    val spilledBlockRecords = new mutable.ArrayDeque[SpilledBlock]()

    /** Charges of [[memoryBlocks]]: the only bytes an eviction can free right now. */
    var bufferedBytes: Long = 0L

    /** Charges of [[spillingBlocks]]: on the heap, charged, but not currently evictable. */
    var spillingBytes: Long = 0L

    /** Charges reserved for admissions to this partition that have not yet been committed. */
    var pendingBytes: Long = 0L

    var lastAccessTimeMs: Long = 0L

    /**
     * Sequence number of the most recently admitted block, or [[UNSET_SEQUENCE]] before the first
     * admission. Retained when the partition empties, so that a stream which drains completely and
     * then continues is still held to a gap-free run.
     */
    var lastAcceptedSequence: Long = UNSET_SEQUENCE

    /** Highest acknowledged position, used to drop records an eviction was still writing. */
    var acknowledgedThroughSequence: Long = UNSET_SEQUENCE

    /** Every byte of heap attributable to this partition, whichever stage it is in. */
    def memoryBytes: Long = bufferedBytes + spillingBytes + pendingBytes

    /** Blocks retained in any form, which is what [[MAX_RETAINED_BLOCKS_PER_PARTITION]] bounds. */
    def retainedBlockCount: Int =
      memoryBlocks.size + spillingBlocks.size + spilledBlockRecords.size

    def isEmpty: Boolean = retainedBlockCount == 0 && pendingBytes == 0L
  }

  // Budget and registration

  /**
   * Records the reduce partition count of the shuffle this consumer serves, which is the divisor
   * of the per-partition allowance. The count is supplied after construction rather than through
   * the constructor so that the constructor keeps the shape every `MemoryConsumer` subclass in
   * this codebase has, and because the producer learns the count from its shuffle handle only
   * after this component already exists.
   *
   * The count is registered exactly once and is immutable thereafter. Repeating the same value is
   * idempotent and safe from any thread; offering a different one throws, because the count is the
   * divisor of the per-partition ceiling and changing it after blocks are already buffered would
   * silently move a ceiling that admission has already been judged against -- raising it would
   * retroactively legalise an over-allocation, and lowering it would leave partitions holding more
   * than the ceiling they are now measured by. Until it is registered the count is one, which makes
   * the per-partition allowance equal to the aggregate allowance: the correct degenerate answer for
   * a single-partition shuffle.
   *
   * @param count the number of reduce partitions; must be positive
   * @throws IllegalArgumentException if the count is not positive, or if a different count has
   *                                 already been registered
   */
  def registerPartitionCount(count: Int): Unit = {
    require(count > 0, s"numPartitions must be positive, but was $count")
    // The registered count is also the exclusive upper bound on the partition ids this instance
    // will admit, so it has to be a bound rather than a number the caller chooses freely.
    require(count <= MAX_TRACKED_PARTITIONS,
      s"numPartitions must not exceed $MAX_TRACKED_PARTITIONS, but was $count")
    if (!partitionCount.compareAndSet(UNREGISTERED_PARTITION_COUNT, count)) {
      val registered = partitionCount.get()
      require(registered == count,
        s"numPartitions is already registered as $registered and cannot be changed to $count; " +
          "the reduce partition count is the divisor of the per-partition buffer ceiling and is " +
          "fixed for the lifetime of a shuffle")
    }
  }

  /**
   * The reduce partition count this consumer divides its aggregate allowance by, or one while no
   * count has been registered.
   */
  def numPartitions: Int = math.max(1, partitionCount.get())

  /** Whether a reduce partition count has been registered. */
  def partitionCountRegistered: Boolean =
    partitionCount.get() != UNREGISTERED_PARTITION_COUNT

  /**
   * The exclusive upper bound on the partition ids this instance will admit: the registered reduce
   * partition count once one is registered, and [[MemorySpillManager.MAX_TRACKED_PARTITIONS]] until
   * then.
   *
   * Without a bound a caller could name unbounded distinct partitions and allocate a buffer, three
   * deques and a map entry for each, none of which is payload and none of which the byte budget
   * would ever notice. Bounding it keeps per-partition metadata proportional to a real shuffle
   * rather than to whatever the caller happens to name, and it is deliberately not the same figure
   * as the budget divisor: an unregistered count divides the allowance as one, but restricting an
   * unregistered producer to partition zero would refuse work that is perfectly legitimate.
   */
  def partitionDomainBound: Int = {
    val registered = partitionCount.get()
    if (registered == UNREGISTERED_PARTITION_COUNT) MAX_TRACKED_PARTITIONS else registered
  }

  /**
   * The aggregate streaming buffer allowance in bytes, that is `bufferSizePercent` of configured
   * executor memory. This ceiling is hard, it is shared by every instance on the executor and by
   * every consumer on it, and admission never exceeds it -- not even to let a single block through.
   */
  def totalBudgetBytes: Long = quota.totalBytes

  /**
   * Bytes currently reserved against [[totalBudgetBytes]] by every instance on this executor. This,
   * and not this instance's own tally, is what the spill threshold is compared against.
   */
  def executorReservedBytes: Long = quota.reservedBytes

  /**
   * The utilisation level in bytes at which eviction is triggered, that is `spillThreshold`
   * percent of [[totalBudgetBytes]].
   */
  def spillThresholdBytes: Long = quota.spillTriggerBytes

  /**
   * The per-partition allowance for an arbitrary partition count, implementing the contracted
   * `(executorMemory * bufferPercent) / numPartitions` -- with `bufferPercent` read as a
   * percentage, so the product is taken before the division by a hundred; see
   * [[MemorySpillManager.percentageOf]] -- without requiring a registration first.
   *
   * @param partitions the partition count to divide the aggregate allowance by; must be positive
   */
  def perPartitionBudgetBytesFor(partitions: Int): Long = {
    require(partitions > 0, s"partitions must be positive, but was $partitions")
    math.max(1L, totalBudgetBytes / partitions)
  }

  /** The per-partition allowance for the registered partition count. */
  def perPartitionBudgetBytes: Long = perPartitionBudgetBytesFor(numPartitions)

  /**
   * The largest block payload that can actually be admitted, in bytes: the smaller of the streaming
   * protocol's own block cap and what the per-partition allowance leaves once the per-block
   * overhead is charged.
   *
   * A producer frames to this figure rather than to the protocol cap alone. Framing to the protocol
   * cap when the per-partition allowance is smaller would produce a block that no amount of
   * eviction can ever make room for, and admission would refuse it forever; framing to this figure
   * guarantees that a block which fits an empty partition is always admissible. Zero means the
   * configured budget cannot accommodate even one block for one partition, in which case streaming
   * is not viable for the shuffle at all and the caller must fall back.
   */
  def maxAdmissiblePayloadBytes: Long = {
    val allowance = perPartitionBudgetBytes - PER_BLOCK_OVERHEAD_BYTES
    math.max(0L, math.min(MAX_BLOCK_PAYLOAD_BYTES.toLong, allowance))
  }

  // Inspection. Every accessor below is a pure read; none of them disturbs the eviction order, so
  // a test or a diagnostic may call them freely without perturbing what it is measuring.

  /** Total bytes currently held in memory across all partitions, awaiting acknowledgement. */
  def bufferedBytes: Long = lock.synchronized(bufferedMemoryBytes)

  /**
   * Bytes currently held in memory for one partition.
   *
   * @param partitionId the reduce partition to report on
   */
  def bufferedBytesFor(partitionId: Int): Long = lock.synchronized {
    partitionBuffers.get(partitionId).map(_.memoryBytes).getOrElse(0L)
  }

  /**
   * Live executor-wide buffer utilisation as a percentage of [[totalBudgetBytes]], which is exactly
   * the value published to the `shuffle.streaming.bufferUtilizationPercent` gauge.
   *
   * Executor scoped rather than instance scoped on purpose: the budget is shared, so an instance's
   * own share of it says nothing about how close the executor is to the spill threshold, and it is
   * the threshold this figure exists to be compared against.
   *
   * An idle executor answers zero without consulting the memory manager at all, which keeps this
   * accessor callable before the first admission and, in particular, after [[close]] -- when the
   * environment may no longer be able to answer. A non-zero reservation implies some admission has
   * already succeeded, so the budget is necessarily already derived by the time it is divided by.
   */
  def bufferUtilizationPercent: Long = {
    val reserved = quota.reservedBytes
    if (reserved <= 0L) 0L else reserved * PERCENT_SCALE / quota.totalBytes
  }

  /**
   * Number of '''threshold or pressure driven''' spill events, counted once per event and not once
   * per partition. This is exactly the figure published to `shuffle.streaming.spillCount`.
   *
   * Two paths advance it, because both are the same condition -- the configured buffer percentage
   * was not enough for what this producer was holding, so bytes went to local disk to keep the
   * bound:
   *
   *  - an eviction the budget forced, because the threshold was met, because an admission needed
   *    room, or because the task memory manager asked for memory back; and
   *  - a block [[admitDurably]] wrote straight to disk, because the allowance could not admit it to
   *    memory at all. [[durableAdmissionCount]] reports how many of these events took that route.
   *
   * Counting both is what keeps event and volume accounting describing the same spill: every byte
   * either path puts on local disk reaches `diskBytesSpilled`, so a path that produced volume
   * without an event would let an operator observe disk spilling at a spill count of zero, and
   * neither a spill rate nor an average spill size could be computed.
   *
   * The end-of-stream durability flush is deliberately '''not''' counted here, and separating that
   * out is what makes this metric mean anything. Every successful streaming map task ends by making
   * its retained window durable, so a shared counter would advance once per task whatever the
   * utilisation: an operator watching it would see a spill on every healthy task at a fraction of a
   * percent of the budget, could not tell that from genuine pressure, and could not measure a spill
   * rate against the configured threshold at all. [[durabilityFlushCount]] reports that event
   * instead, counted once per non-empty pass, and its volume still reaches the ordinary spill
   * accumulators because those bytes really did reach disk.
   */
  def spillCount: Long = spillCountTotal.get()

  /**
   * Number of durability eviction passes that moved bytes, counted once per non-empty pass.
   *
   * A flush is not a spill in the flow-control sense: it is how a producer converts the window a
   * consumer has not yet acknowledged into something the executor-scoped block resolver can still
   * serve after the task's memory has gone. It is driven by the end of a stream rather than by
   * utilisation, and it is a pass count rather than a task count: [[spillAllRetained]] re-plans
   * until memory is empty, so a task whose window was already acknowledged or already durable
   * contributes nothing here, and one whose blocks are retired concurrently can contribute more
   * than one. Volumes reach the ordinary spill accumulators on
   * [[org.apache.spark.executor.TaskMetrics]] exactly as an eviction's do -- the bytes really did
   * reach local disk. Only the *event count* is kept apart, so that `shuffle.streaming.spillCount`
   * remains a pressure signal.
   */
  def durabilityFlushCount: Long = durabilityFlushTotal.get()

  /**
   * Number of blocks admitted straight to local disk by [[admitDurably]] because the buffer
   * allowance could not hold them.
   *
   * A non-zero reading says a producer met the allowance's ceiling and spilled rather than failing,
   * which is the specified behaviour and not an error -- but it is also the signal that the framing
   * arithmetic and the configured percentage are tight for the shuffle's width, so it is in
   * the producing task's summary.
   *
   * This is a '''breakdown''' of [[spillCount]], not a series beside it: every durable admission is
   * also a spill event, because the allowance could not hold the block and the block went to local
   * disk to keep the bound. What this figure adds is which route the event took -- straight to disk
   * rather than by evicting something already resident -- which is what tells an operator that the
   * shuffle is too wide for its configured percentage rather than merely busy. The volume reaches
   * `diskBytesSpilled` either way; `memoryBytesSpilled` does not move on this route, because no
   * memory was reclaimed by writing bytes that were never in the tally.
   */
  def durableAdmissionCount: Long = durableAdmissionTotal.get()

  /**
   * Number of eviction attempts that failed, for instance because the local disk rejected the
   * write.
   *
   * A failure never discards a buffered block: the partial write is rolled back and the blocks are
   * re-attached, so the attempt degrades to a refused admission and the caller decides what that
   * costs. A producer that has not yet consumed a record stands the shuffle down; one already
   * producing writes the block durably instead, and only a block that can reach neither memory nor
   * disk fails the attempt so the map stage is recomputed.
   */
  def spillFailureCount: Long = spillFailureTotal.get()

  /** Bytes that have left memory through eviction, matching `TaskMetrics.memoryBytesSpilled`. */
  def memoryBytesSpilled: Long = memoryBytesSpilledTotal.get()

  /**
   * Bytes this consumer has committed to local disk, matching `TaskMetrics.diskBytesSpilled`.
   *
   * Every route to disk is counted: a threshold or pressure driven eviction, an end-of-stream
   * durability flush, and a block admitted straight to disk by [[admitDurably]] because the buffer
   * allowance could not hold it. The event counts are split across [[spillCount]],
   * [[durabilityFlushCount]] and [[durableAdmissionCount]]; the volume is not.
   */
  def diskBytesSpilled: Long = diskBytesSpilledTotal.get()

  /** High-water mark of the execution memory reserved by this consumer. */
  def peakMemoryBytes: Long = peakMemoryBytesSeen.get()

  /** Elapsed milliseconds, measured on the injected clock, of the most recent eviction event. */
  def lastSpillDurationMs: Long = lastSpillMs.get()

  /**
   * Retained spill records across every partition, which is the figure
   * [[MemorySpillManager.MAX_RETAINED_SPILL_RECORDS_TOTAL]] bounds.
   */
  def retainedSpillRecordCount: Int = lock.synchronized(spilledRecordCount)

  /** Aggregate heap charged for durable spill-record metadata retained by this store. */
  def retainedSpillMetadataBytes: Long = lock.synchronized(spilledMetadataReservedBytes)

  /** Executor-wide retained-output disk byte ceiling. */
  def diskQuotaBytes: Long = diskQuota.totalBytes

  /** Disk bytes currently reserved by pending and committed streaming spill files. */
  def reservedDiskBytes: Long = diskQuota.reservedBytes

  /** Executor-wide retained-output spill-file ceiling. */
  def diskFileQuota: Int = diskQuota.fileLimit

  /** Spill files currently reserved or committed across the executor. */
  def reservedDiskFiles: Int = diskQuota.reservedFileCount

  /** Elapsed milliseconds, measured on the injected clock, of the most recent reclamation. */
  def lastReclamationDurationMs: Long = lastReclamationMs.get()

  /** Reclamations that exceeded the 100 ms target. Counted, never enforced. */
  def reclamationDeadlineBreaches: Long = reclamationBreachTotal.get()

  /** Spill files this consumer failed to remove. Counted so a leak on local disk is visible. */
  def spillFileDeletionFailures: Long = spillFileDeletionFailureTotal.get()

  /**
   * Whether a buffer reservation has been refused or only partially granted since the flag was
   * last cleared. This is the memory-pressure trip condition the streaming shuffle fallback policy
   * consumes to decide that streaming can no longer be sustained and sort-based shuffle must take
   * over. It is sticky on purpose: a single brush with exhaustion must remain visible to a policy
   * that samples it, rather than being lost between samples.
   */
  def memoryPressureDetected: Boolean = memoryPressure.get()

  /** Whether the retained-output disk byte, file or physical-headroom bound refused a write. */
  def diskPressureDetected: Boolean = diskPressure.get()

  /** Total number of refused or partially granted reservations observed. */
  def memoryPressureEvents: Long = memoryPressureCount.get()

  /**
   * Clears the sticky memory-pressure flag so a policy that has already acted on it can re-arm.
   * The cumulative [[memoryPressureEvents]] tally is not reset, so history is never lost.
   */
  def clearMemoryPressure(): Unit = memoryPressure.set(false)

  /** Whether this consumer has been closed and will refuse any further admission. */
  def isClosed: Boolean = closed.get()

  /**
   * Whether this store can still serve the output it retained, which is a different question from
   * whether it can still accept a block.
   *
   * The two lifetimes this class straddles are why the questions differ. Memory belongs to the
   * producing task: it is acquired from that task's memory manager and has to be returned when the
   * task ends. Spilled segments belong to the shuffle: they are ordinary files in the local
   * directories, and the reduce side reads them after the map stage has finished, which is the only
   * time the scheduler lets it read at all. So a closed store whose files have been handed to the
   * block resolver goes on answering lookups, registrations and acknowledgements over those files,
   * whereas a closed store that still owned its files has already unlinked them and can answer
   * nothing.
   */
  def servesRetainedOutput: Boolean = lock.synchronized(servesRetainedOutputLocked)

  /** [[servesRetainedOutput]] for a caller already holding `lock`. */
  private def servesRetainedOutputLocked: Boolean = !closed.get() || spillFilesDetached

  /**
   * The exact order in which eviction will consider partitions: largest evictable footprint first,
   * ties broken least-recently-used first and then by ascending partition id. Partitions holding
   * nothing that can be evicted right now are omitted -- either because they hold nothing in
   * memory, or because an eviction already detached what they held and is writing it.
   */
  def spillSelectionOrder: Seq[Int] = lock.synchronized(selectionOrderLocked)

  /**
   * The sequence number of the most recently admitted block for one partition, or
   * [[MemorySpillManager.UNSET_SEQUENCE]] if none has been admitted. The next block admitted for
   * that partition must carry exactly this value plus one.
   *
   * @param partitionId the reduce partition to report on
   */
  def lastAcceptedSequence(partitionId: Int): Long = lock.synchronized {
    partitionBuffers.get(partitionId).map(_.lastAcceptedSequence).getOrElse(UNSET_SEQUENCE)
  }

  /**
   * Blocks retained in any form for one partition -- in memory, mid-eviction or on disk -- which is
   * the figure [[MemorySpillManager.MAX_RETAINED_BLOCKS_PER_PARTITION]] bounds.
   *
   * @param partitionId the reduce partition to report on
   */
  def retainedBlockCount(partitionId: Int): Int = lock.synchronized {
    partitionBuffers.get(partitionId).map(_.retainedBlockCount).getOrElse(0)
  }

  /**
   * The blocks of one partition still held in memory awaiting acknowledgement, in ascending
   * sequence order, including any an eviction has detached and is currently writing.
   *
   * These are the in-memory portion of the retransmission window; the blocks already evicted to
   * disk but not yet acknowledged are the rest of it and are reported by [[spilledBlocks]]. A
   * corrupted or lost block can be replayed from memory while it is recorded here, and from its
   * spill record once it has been evicted. Only a block absent from both requires the upstream
   * stage to be recomputed.
   *
   * @param partitionId the reduce partition to report on
   */
  def unacknowledgedBlocks(partitionId: Int): Seq[BufferedBlock] = lock.synchronized {
    partitionBuffers.get(partitionId)
      .map(buffer => (buffer.spillingBlocks.toList ++ buffer.memoryBlocks.toList)
        .sortBy(_.sequenceNumber))
      .getOrElse(Nil)
  }

  /**
   * The blocks of one partition that were evicted to disk and are still awaiting acknowledgement,
   * in ascending sequence order. Together with [[unacknowledgedBlocks]] these make up the
   * retransmission window. Each record carries the block id, the backing file and the exact segment
   * bounds, which is everything a block resolver needs to serve or retransmit it from disk.
   *
   * @param partitionId the reduce partition to report on
   */
  def spilledBlocks(partitionId: Int): Seq[SpilledBlock] = lock.synchronized {
    partitionBuffers.get(partitionId).map(_.spilledBlockRecords.toList).getOrElse(Nil)
  }

  /** Every currently retained spilled block, across all partitions. */
  def allSpilledBlocks: Seq[SpilledBlock] = lock.synchronized {
    partitionBuffers.values.flatMap(_.spilledBlockRecords.toList).toList
  }

  /**
   * Looks up a single in-memory block for retransmission. Unlike the bulk inspectors this counts
   * as a use of the partition and refreshes its last-access stamp, so a partition being actively
   * retransmitted is not the first one chosen for eviction.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param sequenceNumber the sequence number of the block to look up
   */
  def bufferedBlock(partitionId: Int, sequenceNumber: Long): Option[BufferedBlock] = {
    lock.synchronized {
      partitionBuffers.get(partitionId).flatMap { buffer =>
        val found = buffer.memoryBlocks.find(_.sequenceNumber == sequenceNumber)
          .orElse(buffer.spillingBlocks.find(_.sequenceNumber == sequenceNumber))
        if (found.isDefined) {
          buffer.lastAccessTimeMs = clock.getTimeMillis()
        }
        found
      }
    }
  }

  /**
   * Looks up a single spilled block for retransmission from disk.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param sequenceNumber the sequence number of the block to look up
   */
  def spilledBlock(partitionId: Int, sequenceNumber: Long): Option[SpilledBlock] = {
    lock.synchronized {
      partitionBuffers.get(partitionId)
        .flatMap(_.spilledBlockRecords.find(_.sequenceNumber == sequenceNumber))
    }
  }

  /**
   * Whether one block is still retained in any form, and therefore still replayable.
   *
   * This is the cheap eligibility test a producer needs before it promises a replay: it consults
   * both halves of the retransmission window -- memory and disk -- without materialising a single
   * byte, so a request spanning a whole range can be validated in full before any part of it is
   * emitted. Unlike [[bufferedBlock]] it does not count as a use of the partition, because asking
   * whether a block could be replayed is not the same as replaying it and must not reorder
   * eviction.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param sequenceNumber the sequence number of the block to test
   */
  def retainsBlock(partitionId: Int, sequenceNumber: Long): Boolean = lock.synchronized {
    partitionBuffers.get(partitionId).exists { buffer =>
      buffer.memoryBlocks.exists(_.sequenceNumber == sequenceNumber) ||
        buffer.spillingBlocks.exists(_.sequenceNumber == sequenceNumber) ||
        buffer.spilledBlockRecords.exists(_.sequenceNumber == sequenceNumber)
    }
  }

  /**
   * The payload size of one retained block, without materialising it.
   *
   * A producer pacing a replay needs each block's size in order to charge it against a credit
   * window and a rate limiter, and it needs that number *before* deciding whether to queue the
   * block at all. Obtaining it from [[retainedPayload]] would read the segment off disk and
   * decompress it -- a disk round trip and an allocation of up to the protocol's block cap, per
   * block, discarded immediately -- so a bounded replay of a long window would spend more work
   * measuring blocks than sending them. Both halves of the retained window already know the
   * answer: a buffered block carries its bytes, and a spilled record carries the payload length
   * recorded when it was written.
   *
   * Like [[retainsBlock]], and unlike [[bufferedBlock]], this does not count as a use of the
   * partition: asking how large a block is must not reorder which partition is evicted next.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param sequenceNumber the sequence number of the block to measure
   * @return the payload length in bytes, or `None` if the block is no longer retained
   */
  def retainedPayloadLength(partitionId: Int, sequenceNumber: Long): Option[Int] = {
    lock.synchronized {
      partitionBuffers.get(partitionId).flatMap { buffer =>
        buffer.memoryBlocks.find(_.sequenceNumber == sequenceNumber).map(_.length)
          .orElse(buffer.spillingBlocks.find(_.sequenceNumber == sequenceNumber).map(_.length))
          .orElse(buffer.spilledBlockRecords.find(_.sequenceNumber == sequenceNumber)
            .map(_.payloadLength))
      }
    }
  }

  /**
   * The lowest sequence number still retained for one partition, or
   * [[MemorySpillManager.UNSET_SEQUENCE]] when nothing is retained.
   *
   * The retained window of a partition is always a contiguous run, because admission is dense and
   * retirement always retires a prefix, so this value and [[lastAcceptedSequence]] bound the whole
   * replayable range. A producer answering a retransmission request compares the request against
   * these two bounds rather than probing block by block, which is what lets it refuse an
   * unserviceable range atomically instead of discovering the gap half way through a replay.
   *
   * @param partitionId the reduce partition to report on
   */
  def lowestRetainedSequence(partitionId: Int): Long = lock.synchronized {
    partitionBuffers.get(partitionId).map { buffer =>
      val candidates =
        buffer.spilledBlockRecords.iterator.map(_.sequenceNumber) ++
          buffer.spillingBlocks.iterator.map(_.sequenceNumber) ++
          buffer.memoryBlocks.iterator.map(_.sequenceNumber)
      if (candidates.isEmpty) UNSET_SEQUENCE else candidates.min
    }.getOrElse(UNSET_SEQUENCE)
  }

  /**
   * The payload bytes of one retained block, read from memory when it is still there and from its
   * spill segment when it has been evicted.
   *
   * This is the single authoritative source of a block's bytes on the producing side, and it exists
   * so that no other component has to hold a second copy of a payload this manager has already
   * charged against the buffer budget. A producer framing a block for the wire -- whether for its
   * first transmission or for a replay -- asks here, so the bytes that leave the executor are
   * always the bytes the budget admitted.
   *
   * The memory path is preferred and refreshes the partition's access stamp through
   * [[bufferedBlock]], because a partition actively being read is a poor eviction candidate. The
   * disk path is deliberate about three things. It takes a lease first, so the file cannot be
   * unlinked underneath the read; it reads exactly the committed segment, because each block was
   * committed on its own and is therefore independently decodable; and it unwraps the segment
   * through `SerializerManager` with the block's own id, because that is how
   * `DiskBlockObjectWriter` wrapped it on the way out, and reading raw bytes from a compressed or
   * encrypted spill file would return something that is not the payload at all.
   *
   * A block that is retained in neither place yields `None`. That is not an error here: it is the
   * honest answer that the bytes were acknowledged and released, which the caller must translate
   * into whatever recovery its own protocol prescribes.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param sequenceNumber the sequence number of the block to read
   * @return the payload bytes, or `None` if the block is no longer retained or could not be read
   */
  def retainedPayload(partitionId: Int, sequenceNumber: Long): Option[Array[Byte]] = {
    bufferedBlock(partitionId, sequenceNumber).map(_.data)
      .orElse(spilledBlock(partitionId, sequenceNumber).flatMap(readSpilledPayload))
  }

  /**
   * Reads one committed spill segment back into its original payload bytes.
   *
   * The segment's recorded length is its length *on disk*, which is not the payload's length: with
   * `spark.shuffle.compress` on -- and it is on by default -- the committed bytes are compressed.
   * The unwrapped stream is therefore read to its end rather than to a precomputed size, and the
   * result is bounded by the protocol's own two mebibyte block cap, so a corrupt or mislabelled
   * segment cannot be turned into an unbounded allocation. The stream is positioned and limited
   * exactly as `ExternalSorter` positions and limits its own spill segments, so a streaming spill
   * file is read back by the same idiom as every other Spark spill file.
   *
   * The lease is acquired before the file is touched and released in a `finally`, so neither a read
   * failure nor an eviction racing the read can leave the file pinned. A failure to read is
   * reported and answered with `None` rather than thrown: the caller's protocol already has to
   * handle a block that cannot be replayed, and turning an I/O fault into that same outcome keeps
   * one recovery path instead of two.
   */
  private def readSpilledPayload(segment: SpilledBlock): Option[Array[Byte]] = {
    if (!acquireSpillFileLease(segment.file)) {
      if (debugEnabled) {
        logDebug(log"Streaming shuffle cannot lease spill file " +
          log"${MDC(FILE_NAME, segment.file.getName)} to replay block " +
          log"${MDC(COUNT, segment.sequenceNumber)} of partition " +
          log"${MDC(PARTITION_ID, segment.partitionId)}")
      }
      None
    } else {
      try {
        val raw = new FileInputStream(segment.file)
        val payload = try {
          // Positioned through the channel rather than by skipping, because a skip may legally
          // move fewer bytes than asked and a partial skip would decode the wrong segment.
          raw.getChannel().position(segment.offset)
          val bounded = new BufferedInputStream(
            new LimitedInputStream(raw, segment.length), fileBufferSizeBytes)
          val unwrapped = serializerManager.wrapStream(segment.blockId, bounded)
          try {
            readBoundedPayload(unwrapped)
          } finally {
            unwrapped.close()
          }
        } finally {
          raw.close()
        }
        if (debugEnabled) {
          logDebug(log"Streaming shuffle replayed ${MDC(NUM_BYTES, payload.length)} byte(s) of " +
            log"partition ${MDC(PARTITION_ID, segment.partitionId)} block " +
            log"${MDC(COUNT, segment.sequenceNumber)} from spill file " +
            log"${MDC(FILE_NAME, segment.file.getName)}")
        }
        Some(payload)
      } catch {
        case NonFatal(e) =>
          logWarning(log"Streaming shuffle could not replay partition " +
            log"${MDC(PARTITION_ID, segment.partitionId)} block " +
            log"${MDC(COUNT, segment.sequenceNumber)} from spill file " +
            log"${MDC(FILE_NAME, segment.file.getName)}", e)
          None
      } finally {
        releaseSpillFileLease(segment.file)
      }
    }
  }

  /**
   * Reads a decoded spill segment to its end, refusing anything larger than a protocol block.
   *
   * One extra byte beyond the cap is requested deliberately: reaching the cap exactly is legal, and
   * only a read that still has bytes left at that point proves the segment does not hold a single
   * block. Refusing is a genuine failure rather than a truncation, because a truncated payload
   * would fail its checksum at the consumer and be blamed on the network instead of on this file.
   */
  private def readBoundedPayload(source: InputStream): Array[Byte] = {
    val cap = DataBlockMessage.MAX_BLOCK_SIZE_BYTES
    val collected = new ByteArrayOutputStream(math.min(cap, DEFAULT_REPLAY_BUFFER_BYTES))
    val chunk = new Array[Byte](DEFAULT_REPLAY_BUFFER_BYTES)
    var read = source.read(chunk)
    while (read >= 0) {
      if (collected.size() + read > cap) {
        throw new IllegalStateException(s"a streaming shuffle spill segment decoded to more than " +
          s"the protocol block cap of $cap byte(s), so it does not hold a single block")
      }
      collected.write(chunk, 0, read)
      read = source.read(chunk)
    }
    collected.toByteArray
  }

  /**
   * A diagnostic rendering built only from state this consumer already holds. The derived budget is
   * deliberately absent: reading it would force the lazy memory-manager dereference, and a
   * `toString` -- which a logging framework or a debugger may evaluate anywhere, including on the
   * driver before the memory manager exists -- must never be the thing that triggers it.
   */
  override def toString: String = {
    s"MemorySpillManager(mode=${getMode()}, numPartitions=$numPartitions, " +
      s"bufferedBytes=$bufferedBytes, utilizationPercent=$bufferUtilizationPercent, " +
      s"spillCount=$spillCount, memoryBytesSpilled=$memoryBytesSpilled, " +
      s"diskBytesSpilled=$diskBytesSpilled, closed=$isClosed)"
  }

  // Admission

  /**
   * Admits one already-framed block into the producer buffer, reserving its bytes from the task
   * memory manager first.
   *
   * The block is retained until the consumer acknowledges it, because until then it may have to be
   * retransmitted. Ownership of the array passes to this consumer: it is retained by reference and
   * never copied, so a caller must not mutate or reuse the array it hands over. Avoiding the copy
   * is deliberate, since this is the producer hot path and a copy here would double the cost of
   * every block.
   *
   * Sequence numbers must form one gap-free ascending run per partition, and that is enforced
   * rather than merely documented: the first block admitted for a partition establishes the origin
   * and every later one must be exactly its predecessor plus one. A duplicate or a gap is a caller
   * defect and throws, because prefix acknowledgement would otherwise charge a block twice or
   * retire bytes that were never charged.
   *
   * @param partitionId the reduce partition the block belongs to; must be non-negative
   * @param sequenceNumber the block's position in the partition's stream; must be non-negative, and
   *                       must be exactly one greater than the partition's previously admitted
   *                       sequence number once the partition has admitted anything
   * @param data the block payload: non-empty, and at most
   *             [[MemorySpillManager.MAX_BLOCK_PAYLOAD_BYTES]] bytes. The budget it occupies is
   *             that length plus [[MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES]], because the block
   *             is retained as a JVM object and leaves as a framed wire message, and neither cost
   *             is free. A payload no larger than [[maxAdmissiblePayloadBytes]] is always
   *             admissible into an empty partition; a larger one can never be, and is refused
   *             rather than retried forever
   * @return true when the block was admitted; false when the budget cannot accommodate it even
   *         after eviction, or when this instance is closed. A false answer that was caused by the
   *         budget also raises the memory-pressure signal, which is the producer's cue to hold the
   *         block and retry, or to let the fallback policy route the shuffle to sort-based shuffle
   * @throws IllegalArgumentException if the payload is empty, oversized, or carries a sequence
   *                                 number that is not the partition's next expected one
   */
  def bufferBlock(partitionId: Int, sequenceNumber: Long, data: Array[Byte]): Boolean = {
    require(partitionId >= 0, s"partitionId must be non-negative, but was $partitionId")
    require(sequenceNumber >= 0L, s"sequenceNumber must be non-negative, but was $sequenceNumber")
    require(data != null, "A streaming shuffle block payload must not be null")
    // An empty block carries nothing a consumer can use, yet it would still occupy a queue slot, a
    // sequence number and the per-block overhead. Refusing it at the boundary keeps the accounting
    // honest and surfaces the framing defect at its source instead of billing the budget for it.
    require(data.length > 0,
      s"A streaming shuffle block payload must not be empty, but partition $partitionId " +
        s"sequence $sequenceNumber carried none")
    require(data.length <= MAX_BLOCK_PAYLOAD_BYTES,
      s"A streaming shuffle block payload may not exceed $MAX_BLOCK_PAYLOAD_BYTES bytes, but " +
        s"partition $partitionId sequence $sequenceNumber carried ${data.length} bytes")

    // Every block costs something, however small its payload, so there is no free admission path to
    // short-circuit: the reservation is always attempted and the budget always charged.
    val required = data.length.toLong + PER_BLOCK_OVERHEAD_BYTES
    // Read outside `lock`, and deliberately so: this forces the lazily derived budget, whose
    // derivation synchronizes on the memory manager, and this class never holds its own monitor
    // while touching that one.
    val partitionCeiling = perPartitionBudgetBytes

    var admitted = false
    var settled = false
    var attempt = 0
    while (!settled) {
      attempt += 1
      val outcome = lock.synchronized {
        reserveLocked(partitionId, sequenceNumber, required, partitionCeiling)
      }
      outcome match {
        case AdmissionReserved(previousSequence) =>
          // `acquireMemory` is called with no monitor held. `TaskMemoryManager` runs spill
          // callbacks with its own monitor held, so acquiring while holding ours could deadlock
          // against a thread holding the task memory manager and waiting for ours.
          val granted = acquireMemory(required)
          if (granted >= required) {
            // The array is adopted, not copied, which is the ownership contract this method
            // documents. A second copy would be the difference between the configured buffer
            // percentage being a bound and being half of one: the copy is what the budget charges
            // for, so the original would be an equal quantity of retained payload that nothing
            // accounts for. The producer relinquishes the array at this call and the store is from
            // here on the single owned representation of the block -- egress reads it back through
            // [[retainedPayload]] rather than holding a reference of its own.
            if (commitReservation(partitionId, sequenceNumber, data, required)) {
              recordPeakMemory()
              admitted = true
            } else {
              // Closed between reservation and commit. These bytes were never added to this
              // instance's tally, so this thread owns them and this release is the only one they
              // get -- which is exactly why closure frees its own tally and not whatever the memory
              // manager happens to report.
              freeMemory(required)
            }
            settled = true
          } else {
            // `acquireMemory` answers with the amount it was actually able to grant, which may be
            // less than the request. Spark's own spillable collections treat exactly that outcome
            // as the signal to spill, and this class does the same: hand the partial grant straight
            // back so no other consumer is starved by memory this one cannot use, publish the
            // pressure signal the fallback policy watches, evict, and retry.
            if (granted > 0L) {
              freeMemory(granted)
            }
            rollbackReservation(partitionId, sequenceNumber, previousSequence, required)
            recordMemoryPressure(required, granted)
            if (attempt >= MAX_ADMISSION_ATTEMPTS || !reclaimRoom(required)) {
              settled = true
            }
          }

        case AdmissionNeedsRoom =>
          // Transient: the budget is full right now. Evict and re-evaluate. A final refusal raises
          // the memory-pressure signal, because a producer that cannot buffer is precisely the
          // condition under which the fallback policy must route the shuffle to sort-based shuffle.
          if (attempt >= MAX_ADMISSION_ATTEMPTS || !reclaimRoom(required)) {
            recordMemoryPressure(required, 0L)
            settled = true
          }

        case AdmissionRefused(reason) =>
          // Permanent for this block: no amount of eviction will make it admissible. Signal
          // pressure so the shuffle degrades to sort rather than retrying an impossible admission.
          recordMemoryPressure(required, 0L)
          recordPermanentRefusal(partitionId, required, reason)
          settled = true

        case AdmissionClosed =>
          settled = true

        case AdmissionOutOfSequence(expected) =>
          throw new IllegalArgumentException(
            s"Streaming shuffle blocks for one partition must form a gap-free ascending run, so " +
              s"partition $partitionId expected sequence $expected but was offered " +
              s"$sequenceNumber; a duplicate would be charged twice and a gap would let a later " +
              "acknowledgement retire bytes that were never charged")
      }
    }
    admitted
  }

  /**
   * Admits one block by writing it straight to local disk, charging the buffer budget nothing.
   *
   * '''Why this exists.''' The buffer allowance is a bound, and a bound that is reached has to have
   * an answer other than failure. [[bufferBlock]] evicts, waits out a concurrent eviction and
   * retries, but every one of those routes reclaims memory *this* producer occupies -- and the
   * occupant may be the framing scratch of the other tasks sharing the executor, which no eviction
   * this task can run will ever return. A producer in that position has framed a block it must
   * account for and cannot buffer, and the specification's answer is to spill rather than to fail:
   * every path has to terminate in a working shuffle. So the block skips memory altogether and goes
   * to the same temporary shuffle block, through the same writer, as an evicted one.
   *
   * '''Why the result is indistinguishable from an eviction.''' The record published here is the
   * record eviction publishes, registered in the same spilled store under the same lock, so every
   * read path -- [[retainsBlock]], [[retainedPayload]], [[retainedPayloadLength]],
   * [[lowestRetainedSequence]], [[spilledBlock]] and the resolver behind them -- answers for it
   * exactly as it answers for a block that was buffered first and evicted later. A consumer cannot
   * tell the difference, which is the property that makes this safe: the map output stays complete
   * and stays reassemblable, so no hybrid of streamed and sort-written output is ever produced.
   *
   * '''Why it counts as a spill.''' Being unable to admit a block to memory and putting it on local
   * disk instead is exactly what the buffer threshold exists to prevent, so a successful admission
   * here advances [[spillCount]] and the exported `shuffle.streaming.spillCount` counter through
   * the same [[recordSpillEvent]] seam an eviction uses, as well as `diskBytesSpilled`. Publishing
   * the volume without the event would let an operator watch disk spilling climb while the spill
   * counter stayed at zero, which makes both a spill rate and an average spill size uncomputable.
   * [[durableAdmissionCount]] remains as the breakdown of how many of those events took this route.
   *
   * '''What is still refused.''' Everything that no amount of disk can fix. Closure, a partition
   * outside the admissible domain, a payload outside the protocol's framing rules, and both
   * retained-metadata ceilings are checked exactly as [[bufferBlock]] checks them, because each of
   * those bounds something other than memory. What is *not* checked is the per-partition allowance
   * and the executor-wide quota -- being unable to satisfy them is precisely the condition this
   * method answers. An out-of-order sequence is still a programming error and still raises, so the
   * gap-free ascending run every consumer depends on is enforced on this path too.
   *
   * '''What a caller owes this method: a non-degrading report.''' Because this is spill behaviour
   * and not a fallback condition, the pressure that led here must be published through
   * `BackpressureProtocol.reportDurableSpillAdmission`, which counts the occurrence and leaves the
   * subsystem flowing. It must '''not''' be published through
   * `BackpressureProtocol.reportBufferAllocationFailure`, which latches the second graceful
   * degradation reason and records that streaming should yield to the sort-based implementation:
   * retaining the block here and carrying on streaming while that reason stands would leave the
   * executor claiming streaming is unsustainable and continuing anyway. A caller that genuinely
   * cannot proceed -- one for which even this method returns false -- fails its attempt instead,
   * and the map stage is recomputed.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param sequenceNumber the block's position in that partition's gap-free ascending run
   * @param data the framed payload, adopted rather than copied for the duration of the write
   * @return true when the block is durably retained and servable; false when it was refused or the
   *         write failed, in which case nothing was published and the sequence is unclaimed
   */
  def admitDurably(partitionId: Int, sequenceNumber: Long, data: Array[Byte]): Boolean = {
    require(partitionId >= 0, s"partitionId must be non-negative, but was $partitionId")
    require(sequenceNumber >= 0L, s"sequenceNumber must be non-negative, but was $sequenceNumber")
    require(data != null, "A streaming shuffle block payload must not be null")
    require(data.length > 0,
      s"A streaming shuffle block payload must not be empty, but partition $partitionId " +
        s"sequence $sequenceNumber carried none")
    require(data.length <= MAX_BLOCK_PAYLOAD_BYTES,
      s"A streaming shuffle block payload may not exceed $MAX_BLOCK_PAYLOAD_BYTES bytes, but " +
        s"partition $partitionId sequence $sequenceNumber carried ${data.length} bytes")

    lock.synchronized(claimDurableSequenceLocked(partitionId, sequenceNumber)) match {
      case None =>
        false
      case Some(previousSequence) =>
        // Written with no monitor held: the write is a filesystem operation of unbounded duration,
        // and holding `lock` across it would stall every admission, acknowledgement and eviction on
        // the executor for as long as the disk took.
        val block = BufferedBlock(partitionId, sequenceNumber, data)
        spillPartitionBlocks(partitionId, Seq(block)) match {
          case Some(records) =>
            val committedBytes = lock.synchronized {
              publishDurableAdmissionLocked(partitionId, records)
            }
            diskBytesSpilledTotal.addAndGet(committedBytes)
            durableAdmissionTotal.incrementAndGet()
            // Counted as the spill event it is. The block reached local disk because the allowance
            // could not hold it, which is the same condition an eviction answers and the same
            // condition `shuffle.streaming.spillCount` exists to report, so the event count and the
            // disk volume this path produces describe one spill rather than disagreeing about
            // whether one happened. `durableAdmissionCount` remains the breakdown of how many of
            // those events took this route rather than evicting something already resident.
            recordSpillEvent()
            true
          case None =>
            // `spillPartitionBlocks` has already reported the failure, rolled the file back and
            // unlinked it. All that is left is to unclaim the sequence, so the producer's own
            // recovery -- or a later attempt at the same block -- is not rejected as a duplicate.
            lock.synchronized {
              rollbackDurableSequenceLocked(partitionId, sequenceNumber, previousSequence)
            }
            false
        }
    }
  }

  /**
   * Claims one sequence number for a durable admission, or declines it. Must hold `lock`.
   *
   * Returns the sequence the partition had accepted before this claim, which is what a failed write
   * restores. Every check here bounds something a disk cannot supply; the two allowance checks of
   * [[reserveLocked]] are deliberately absent.
   */
  private def claimDurableSequenceLocked(
      partitionId: Int,
      sequenceNumber: Long): Option[Long] = {
    val existing = partitionBuffers.get(partitionId)
    val lastAccepted = existing.map(_.lastAcceptedSequence).getOrElse(UNSET_SEQUENCE)
    val expected = if (lastAccepted == UNSET_SEQUENCE) sequenceNumber else lastAccepted + 1L
    if (closed.get() || partitionId >= partitionDomainBound) {
      None
    } else if (sequenceNumber != expected) {
      throw new IllegalArgumentException(
        s"Streaming shuffle blocks for one partition must form a gap-free ascending run, so " +
          s"partition $partitionId expected sequence $expected but was offered $sequenceNumber " +
          "for a durable admission")
    } else if (spilledRecordCount >= MAX_RETAINED_SPILL_RECORDS_TOTAL) {
      diskPressure.set(true)
      memoryPressure.set(true)
      memoryPressureCount.incrementAndGet()
      None
    } else if (existing.exists(_.retainedBlockCount >= MAX_RETAINED_BLOCKS_PER_PARTITION)) {
      diskPressure.set(true)
      memoryPressure.set(true)
      memoryPressureCount.incrementAndGet()
      None
    } else {
      val buffer = partitionBuffers.getOrElseUpdate(partitionId, new PartitionBuffer(partitionId))
      buffer.lastAcceptedSequence = sequenceNumber
      Some(lastAccepted)
    }
  }

  /**
   * Publishes the records of a completed durable admission and reports their committed length.
   * Must hold `lock`.
   *
   * Deliberately identical to the published half of [[publishEvictionLocked]] minus everything to
   * do with memory: no buffered tally moves, because these bytes were never in the tally, and
   * `memoryBytesSpilled` is not advanced, because no memory was freed by writing them. Their volume
   * is real disk output and reaches `diskBytesSpilled` like every other spilled byte.
   *
   * A partition whose buffer vanished while the write was in flight -- only closure does that --
   * cannot publish, so the files are retired rather than left unnameable on local disk.
   */
  private def publishDurableAdmissionLocked(
      partitionId: Int,
      records: Seq[SpilledBlock]): Long = {
    partitionBuffers.get(partitionId) match {
      case Some(buffer) if !closed.get() =>
        buffer.spilledBlockRecords ++= records
        spilledRecordCount += records.size
        spilledMetadataReservedBytes += records.size.toLong * SPILLED_RECORD_METADATA_BYTES
        records.foreach { record =>
          spillFilesByBlockId.update(record.blockId, record.file)
          spillFileBlockIds.update(record.file, record.blockId)
        }
        buffer.lastAccessTimeMs = clock.getTimeMillis()
        records.foldLeft(0L)((acc, record) => acc + record.length)
      case _ =>
        quota.release(records.size.toLong * SPILLED_RECORD_METADATA_BYTES, MetadataMemory)
        records.map(_.file).distinct.foreach { file =>
          if (retireSpillFileLocked(file)) {
            deleteSpillFile(file)
          }
        }
        0L
    }
  }

  /**
   * Restores the accepted sequence of a partition after a durable admission failed to write. Must
   * hold `lock`.
   *
   * Conditional, exactly as [[rollbackReservation]] is: the watermark is restored only while it
   * still names this attempt, so a concurrent admission that has already moved it past this one is
   * never clobbered.
   */
  private def rollbackDurableSequenceLocked(
      partitionId: Int,
      sequenceNumber: Long,
      previousSequence: Long): Unit = {
    partitionBuffers.get(partitionId).foreach { buffer =>
      if (buffer.lastAcceptedSequence == sequenceNumber) {
        buffer.lastAcceptedSequence = previousSequence
      }
    }
  }

  /**
   * Decides one admission attempt and, when it succeeds, takes the room for it.
   *
   * Every check that must be indivisible lives here, and the shared-quota reservation is taken last
   * so that a refusal never has to be undone. Must be called while holding `lock`; that is what
   * makes the lifecycle check, the sequence check, the retained-count check, the per-partition
   * ceiling check and the executor-wide reservation one atomic decision rather than five racing
   * ones.
   */
  private def reserveLocked(
      partitionId: Int,
      sequenceNumber: Long,
      required: Long,
      partitionCeiling: Long): AdmissionOutcome = {
    val existing = partitionBuffers.get(partitionId)
    val lastAccepted = existing.map(_.lastAcceptedSequence).getOrElse(UNSET_SEQUENCE)
    val expected = if (lastAccepted == UNSET_SEQUENCE) sequenceNumber else lastAccepted + 1L
    val partitionBytes = existing.map(_.memoryBytes).getOrElse(0L)
    val retained = existing.map(_.retainedBlockCount).getOrElse(0)
    if (closed.get()) {
      // Read here rather than before the call so that refusal and closure are the same indivisible
      // step: a block admitted after closure had released its buffers would leak and would make the
      // memory manager's balance disagree with this instance's.
      AdmissionClosed
    } else if (partitionId >= partitionDomainBound) {
      // The partition-id domain is bounded, so no caller can invent partitions to allocate
      // uncharged per-partition metadata under. This is what makes the retention comment below
      // true: the number of tracked partitions really is bounded by the reduce partition count.
      AdmissionRefused(s"partition $partitionId is outside the admissible range " +
        s"[0, $partitionDomainBound)")
    } else if (spilledRecordCount >= MAX_RETAINED_SPILL_RECORDS_TOTAL) {
      // Every retained spill record is heap this instance no longer charges for, because eviction
      // returned the whole charge to the quota, and a record outlives acknowledgement because the
      // segment it names is map output rather than a retransmission buffer. Bounding their number
      // across partitions is therefore the only thing standing between a large map output and
      // unbounded metadata; reaching it raises pressure and routes the shuffle to fallback.
      AdmissionRefused(s"this consumer already retains $spilledRecordCount spilled block records " +
        s"across all partitions, the maximum of $MAX_RETAINED_SPILL_RECORDS_TOTAL")
    } else if (sequenceNumber != expected) {
      AdmissionOutOfSequence(expected)
    } else if (required > partitionCeiling) {
      // No eviction can ever help: the block alone exceeds the partition's entire allowance. The
      // previous behaviour let the first block of an empty partition through regardless, which
      // silently raised the per-partition ceiling to the aggregate one for any producer that framed
      // larger than its share. A producer that respects `maxAdmissiblePayloadBytes` never sees
      // this.
      AdmissionRefused(s"a block charging $required bytes cannot fit a per-partition allowance " +
        s"of $partitionCeiling bytes; frame to at most $maxAdmissiblePayloadBytes payload bytes")
    } else if (retained >= MAX_RETAINED_BLOCKS_PER_PARTITION) {
      // The per-block charge bounds how much payload one partition can hold, but a spilled block's
      // retained record is heap this class does not charge for and holds for the shuffle's lifetime
      // rather than until acknowledgement, so the block count is bounded too.
      AdmissionRefused(s"partition $partitionId already retains $retained blocks in memory " +
        s"and on disk, the maximum of $MAX_RETAINED_BLOCKS_PER_PARTITION")
    } else if (partitionBytes + required > partitionCeiling) {
      AdmissionNeedsRoom
    } else if (!quota.tryReserve(required)) {
      // The executor-wide ceiling is absolute: aggregate buffer usage never exceeds
      // `bufferSizePercent` of executor memory, however many producers are running at once.
      AdmissionNeedsRoom
    } else {
      val buffer = partitionBuffers.getOrElseUpdate(partitionId, new PartitionBuffer(partitionId))
      buffer.pendingBytes += required
      buffer.lastAcceptedSequence = sequenceNumber
      pendingReservedBytes += required
      inFlightAdmissions += 1
      AdmissionReserved(lastAccepted)
    }
  }

  /**
   * Publishes a reserved block into its partition queue, or hands the reservation back when the
   * instance was closed while the reservation was in flight.
   *
   * @return true when the block was published and its bytes became this instance's to release;
   *         false when the instance is closed, in which case the caller must free the memory it
   *         acquired because nothing else will
   */
  private def commitReservation(
      partitionId: Int,
      sequenceNumber: Long,
      data: Array[Byte],
      bytes: Long): Boolean = {
    val now = clock.getTimeMillis()
    lock.synchronized {
      inFlightAdmissions -= 1
      pendingReservedBytes -= bytes
      val maybeBuffer = partitionBuffers.get(partitionId)
      if (closed.get() || maybeBuffer.isEmpty) {
        maybeBuffer.foreach(buffer => buffer.pendingBytes -= bytes)
        quota.release(bytes)
        false
      } else {
        val buffer = maybeBuffer.get
        buffer.pendingBytes -= bytes
        buffer.memoryBlocks += BufferedBlock(partitionId, sequenceNumber, data)
        buffer.bufferedBytes += bytes
        buffer.lastAccessTimeMs = now
        bufferedMemoryBytes += bytes
        true
      }
    }
  }

  /**
   * Undoes a reservation whose memory acquisition failed, restoring the sequence watermark so the
   * block the producer will legitimately retry is not then rejected as a duplicate.
   *
   * The watermark is restored conditionally, only while it still names this attempt, so a
   * concurrent admission that has already moved it past this one is never clobbered.
   */
  private def rollbackReservation(
      partitionId: Int,
      sequenceNumber: Long,
      previousSequence: Long,
      bytes: Long): Unit = {
    lock.synchronized {
      inFlightAdmissions -= 1
      pendingReservedBytes -= bytes
      partitionBuffers.get(partitionId).foreach { buffer =>
        buffer.pendingBytes -= bytes
        if (buffer.lastAcceptedSequence == sequenceNumber) {
          buffer.lastAcceptedSequence = previousSequence
        }
      }
      quota.release(bytes)
    }
  }

  // Threshold polling and eviction

  /**
   * Performs one cadence-gated utilisation check, evicting if the threshold is met.
   *
   * Driven on the contracted 100 ms cadence by the executor's shared ticker whenever this instance
   * was constructed with `autoPoll` enabled, and callable by the owner from its own thread as often
   * as it likes besides. The 100 ms gate, measured with the injected clock, suppresses the check in
   * between either way, so which calls do work is a function of the injected clock rather than of
   * how often the caller happens to ask.
   *
   * @return true when this call evicted at least one partition
   */
  def pollOnce(): Boolean = {
    val now = clock.getTimeMillis()
    val due = lock.synchronized {
      if (lastPollTimeMs < 0L || now - lastPollTimeMs >= POLL_INTERVAL_MS) {
        lastPollTimeMs = now
        true
      } else {
        false
      }
    }
    due && maybeSpill()
  }

  /**
   * Evaluates buffer utilisation against the configured threshold immediately, ignoring the polling
   * cadence, and evicts when it is met or exceeded.
   *
   * The threshold is evaluated against the executor-wide reservation, because that is what the
   * budget bounds, but the amount this instance sheds is capped by what it actually holds. An
   * instance holding nothing therefore does no work however loaded its neighbours are, while an
   * instance holding everything sheds the whole overage -- which is the behaviour that makes a
   * shared budget converge instead of oscillating.
   *
   * @return true when this call evicted at least one partition
   */
  def maybeSpill(): Boolean = {
    val buffered = bufferedBytes
    val threshold = spillThresholdBytes
    val reserved = quota.reservedBytes
    if (closed.get() || buffered <= 0L || reserved < threshold) {
      false
    } else {
      // Free at least the overage so utilisation drops back below the threshold. Whole partitions
      // are evicted, so in practice rather more than the overage is reclaimed, which is what stops
      // the trigger from re-arming on the very next block.
      val overage = math.max(1L, reserved - threshold)
      if (debugEnabled) {
        logDebug(log"Streaming shuffle spill threshold met: executor reservation " +
          log"${MDC(NUM_BYTES, reserved)} bytes against a threshold of " +
          log"${MDC(THRESHOLD, threshold)} bytes (${MDC(COUNT, spillThresholdPercent)} percent " +
          log"of ${MDC(MAX_SIZE, totalBudgetBytes)} bytes); this instance holds " +
          log"${MDC(MEMORY_SIZE, buffered)} bytes")
      }
      evictAndRelease(math.min(buffered, overage)) > 0L
    }
  }

  /**
   * Reserves budget for a producer's framing scratch, which is memory it holds but cannot spill.
   *
   * A streaming producer needs a serialization accumulator per active partition before it has any
   * block to admit, and those arrays are as real as the blocks cut out of them. Charging them here
   * is what makes the configured percentage of executor memory a bound on everything the subsystem
   * holds rather than on the half of it that happens to be evictable: the reservation is taken from
   * the same executor-wide quota as a buffered block and acquired from the same task memory
   * manager, so an executor whose scratch alone approaches the budget refuses further blocks
   * and degrades exactly as it would under block pressure.
   *
   * Eviction is attempted before refusing, because scratch and blocks compete for one budget and
   * the blocks are the half that can be moved to disk.
   *
   * @param bytes the reservation being requested; a non-positive request is a no-op that succeeds
   * @return true when the reservation was granted and the caller may allocate
   */
  def reserveScratch(bytes: Long): Boolean = {
    if (bytes <= 0L) {
      true
    } else {
      var attempt = 0
      var granted = false
      var settled = false
      while (!settled) {
        attempt += 1
        val reserved = lock.synchronized {
          if (closed.get()) {
            false
          } else if (quota.tryReserve(bytes)) {
            scratchReservedBytes += bytes
            true
          } else {
            false
          }
        }
        if (reserved) {
          // Acquired with no monitor of ours held, for the same reason block admission does it that
          // way: the task memory manager runs spill callbacks under its own monitor.
          val acquired = acquireMemory(bytes)
          if (acquired >= bytes) {
            granted = true
          } else {
            if (acquired > 0L) {
              freeMemory(acquired)
            }
            lock.synchronized {
              scratchReservedBytes -= bytes
              quota.release(bytes)
            }
            recordMemoryPressure(bytes, acquired)
          }
          settled = granted || attempt >= MAX_ADMISSION_ATTEMPTS
        } else if (attempt >= MAX_ADMISSION_ATTEMPTS || !reclaimRoom(bytes)) {
          recordMemoryPressure(bytes, 0L)
          settled = true
        }
      }
      granted
    }
  }

  /**
   * Returns a scratch reservation. Bounded by what is actually outstanding, so a caller that
   * releases twice cannot credit the budget with memory it never held.
   *
   * @param bytes the reservation being returned
   * @return the number of bytes actually released
   */
  def releaseScratch(bytes: Long): Long = {
    if (bytes <= 0L) {
      0L
    } else {
      val released = lock.synchronized {
        val amount = math.min(bytes, scratchReservedBytes)
        if (amount > 0L) {
          scratchReservedBytes -= amount
          quota.release(amount)
        }
        amount
      }
      if (released > 0L) {
        freeMemory(released)
      }
      released
    }
  }

  /** Bytes currently reserved for framing scratch. */
  def scratchBytes: Long = lock.synchronized(scratchReservedBytes)

  /**
   * Forces every byte still held in memory out to disk, whatever the utilisation.
   *
   * This is not a spill in the flow-control sense and is not driven by the threshold: it is how a
   * producer makes its unacknowledged window durable before the memory holding it is taken away.
   * Task-managed execution memory does not survive task completion -- the executor frees it and,
   * with `spark.unsafe.exceptionOnMemoryLeak` enabled, fails the task over anything still acquired
   * -- so a block that is only in memory when a map task ends is a block no consumer can ever be
   * sent again. Writing it to a spill file converts it into something the executor-scoped block
   * resolver can serve for as long as the shuffle is registered.
   *
   * Acknowledged blocks are not written, because retirement has already released them; only what
   * remains retained is, which is exactly the window a reconnecting consumer may ask for.
   *
   * @return bytes moved to disk by this call, which is zero when nothing was held in memory
   */
  def spillAllRetained(): Long = {
    var moved = 0L
    var passes = 0
    var held = bufferedBytes
    // Repeated until memory is empty rather than attempted once. A single pass leaves bytes behind
    // in two ordinary situations, and in both the caller would report a retained window as
    // unreachable while it was in fact being written to disk: a reclamation started by the
    // threshold poller is mid-flight, so nothing is selectable and the aggregate still counts its
    // bytes; or a
    // consumer's acknowledgement retired blocks between the tally being read and the plan being
    // built. `evictAndRelease` waits out the former, and this loop re-reads the tally so the latter
    // simply converges. Bounded by a pass count so a device that keeps failing its writes ends the
    // loop instead of spinning on it.
    while (!closed.get() && held > 0L && passes < MAX_DURABILITY_FLUSH_PASSES) {
      passes += 1
      val freed = evictAndRelease(held, durabilityFlush = true)
      moved += freed
      held = bufferedBytes
      if (freed <= 0L && held > 0L) {
        // Nothing left memory and nothing is being reclaimed, so another pass would plan the same
        // empty eviction. Stop and let the caller report what is still resident.
        passes = MAX_DURABILITY_FLUSH_PASSES
      }
    }
    moved
  }

  /**
   * Releases memory on behalf of another consumer under pressure, which is the callback
   * `TaskMemoryManager` invokes when the task as a whole cannot satisfy an allocation.
   *
   * The guard is the one Spark's own spillable collections use. A request that this consumer
   * triggered itself is declined, because a consumer must not recurse into its own eviction while
   * it is mid-reservation, and a request against a memory mode this consumer does not manage is
   * likewise declined. `acquireMemory` is never called anywhere in this path: acquiring memory from
   * inside a spill callback is the documented deadlock on this interface.
   *
   * @param size the number of bytes the task memory manager is trying to reclaim
   * @param trigger the consumer whose allocation provoked the reclamation
   * @return the number of bytes actually released, and zero when nothing could be released
   */
  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    if (trigger != this && taskMemoryManager.getTungstenMemoryMode == MemoryMode.ON_HEAP) {
      val freed = evictToDisk(size, durabilityFlush = false)
      if (freed <= 0L) {
        0L
      } else {
        // Released with no buffer monitor held. This thread already owns the task memory manager's
        // monitor -- it is the thread that called into us -- so the release is reentrant and cannot
        // block, and holding none of our own monitors keeps the lock-ordering invariant intact.
        releaseReclaimedBytes(freed)
        freed
      }
    } else {
      0L
    }
  }

  /**
   * Evicts to disk and then hands the freed bytes back to both budgets.
   *
   * '''Why a nil result is retried rather than believed.''' Exactly one eviction runs at a time and
   * the threshold poller runs on a thread of its own, so a caller that needs room can arrive while
   * a reclamation is already in flight. In that window nothing is selectable, because the detached
   * bytes have left every partition's own tally, so a single pass returns zero -- which a caller
   * would otherwise read as an exhausted budget and answer with a refused admission, a refused
   * scratch reservation, or a retained window reported as not durable while it was being written to
   * disk. Waiting for the reclamation already under way and then re-planning distinguishes a short
   * wait from an exhausted budget. The wait is bounded, so a genuinely exhausted budget still
   * refuses, and a genuinely slow device still refuses rather than blocking forever.
   *
   * Never called from [[spill]]: that callback runs with the task memory manager's own monitor held
   * by this thread, and parking there could hold it against a thread this one is waiting for.
   *
   * @param bytesToFree the number of bytes the caller needs to leave memory
   * @return the number of bytes that actually left memory, across every pass this call made
   */
  private def evictAndRelease(bytesToFree: Long, durabilityFlush: Boolean = false): Long = {
    var freed = evictToDisk(bytesToFree, durabilityFlush)
    var passes = 1
    while (freed <= 0L && passes < MAX_EVICTION_PASSES && !closed.get() &&
        awaitEvictionQuiescence() && bufferedBytes > 0L) {
      passes += 1
      freed = evictToDisk(bytesToFree, durabilityFlush)
    }
    if (freed > 0L) {
      releaseReclaimedBytes(freed)
    }
    freed
  }

  /**
   * Waits, bounded, for any eviction in flight to publish what it wrote.
   *
   * Bounded by a slice count rather than by a deadline, so the wait terminates on a held clock as
   * well as on a running one -- this class takes its clock by injection precisely so that a suite
   * can stop it, and a loop that only consulted the clock would never end there. The monitor is
   * released by `wait`, which is what lets the evicting thread reach its publish stage; that stage
   * wakes every waiter, so the common case costs one notification rather than the whole budget.
   *
   * An interrupt ends the wait immediately and is re-asserted rather than swallowed: the task
   * thread is the usual caller and Spark cancels a task by interrupting it.
   *
   * @return true when no eviction is in flight any more, false when one outlasted the wait
   */
  private def awaitEvictionQuiescence(): Boolean = lock.synchronized {
    var slices = 0
    while (evictionInProgress && !closed.get() && slices < EVICTION_QUIESCENCE_SLICES) {
      slices += 1
      try {
        lock.wait(EVICTION_QUIESCENCE_SLICE_MS)
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          slices = EVICTION_QUIESCENCE_SLICES
      }
    }
    !evictionInProgress
  }

  /**
   * Whether a reclamation is between its detach stage and its publish stage right now.
   *
   * Published so that a producer deciding whether to defer a refused admission can consult the fact
   * itself instead of inferring it from which partitions happen to be selectable.
   */
  def evictionInFlight: Boolean = lock.synchronized(evictionInProgress)

  /**
   * Tries to make `bytesToFree` bytes of room, and reports whether retrying a reservation is worth
   * it.
   *
   * '''Why this is not just "did I free anything".''' The budget is executor wide and reclamation
   * is concurrent, so room can appear without this call having freed a byte -- the threshold
   * poller's eviction publishes, or a sibling producer's acknowledgement retires its window.
   * Answering "no progress" there would refuse a reservation at the very moment the budget was
   * being freed, so the shared allowance is sampled either side of the attempt and a reduction
   * anywhere in it counts as progress just as this instance's own eviction does.
   *
   * @param bytesToFree the room the caller needs
   * @return true when room was reclaimed, by this call or by another party, and the caller should
   *         re-attempt its reservation
   */
  private def reclaimRoom(bytesToFree: Long): Boolean = {
    val reservedBefore = quota.reservedBytes
    val freed = evictAndRelease(bytesToFree)
    freed > 0L || quota.reservedBytes < reservedBefore
  }

  /**
   * Returns bytes that have left this instance's buffers to both budgets they were taken from: the
   * executor-wide streaming quota and the task memory manager.
   *
   * Both releases are mandatory and neither is optional. Releasing only the memory manager would
   * leave the shared quota permanently consumed, so the executor would stop admitting long before
   * its heap was full; releasing only the quota would leak execution memory and fail the task-level
   * leak check. Must be called with no monitor of this class held, because the memory manager call
   * is exactly the one that must never be made while holding it.
   */
  private def releaseReclaimedBytes(bytes: Long): Unit = {
    if (bytes > 0L) {
      quota.release(bytes)
      freeMemory(bytes)
    }
  }

  /**
   * Evicts buffered partitions to local disk until at least `bytesToFree` bytes have left memory or
   * no candidate remains, and returns how many bytes left memory.
   *
   * The caller, not this method, returns the freed bytes to the memory manager. Keeping the two
   * separate is what lets the [[spill]] callback report the released amount to `TaskMemoryManager`
   * while still performing the release outside this class's own monitor.
   *
   * @param bytesToFree the number of bytes the caller needs to leave memory
   * @param durabilityFlush whether this eviction is the end-of-stream flush that makes a retained
   *                        window durable rather than a reclamation the budget forced. The
   *                        mechanics are identical; what differs is the event counter it advances,
   *                        and therefore whether it appears in the `shuffle.streaming.spillCount`
   *                        pressure signal. See [[spillCount]] and [[durabilityFlushCount]]
   */
  private def evictToDisk(bytesToFree: Long, durabilityFlush: Boolean): Long = {
    if (bytesToFree <= 0L) {
      0L
    } else {
      val startTimeMs = clock.getTimeMillis()
      // Stage one, monitor held: choose the victims and detach their blocks. Detaching makes those
      // blocks the exclusive property of this eviction, so a concurrent producer cannot append to a
      // queue that is being written out and a concurrent acknowledgement cannot retire a block
      // whose bytes this eviction is about to account for.
      val plan = lock.synchronized(planEvictionLocked(bytesToFree))
      if (plan.isEmpty) {
        0L
      } else {
        // Stage two, monitor released: write. Holding the monitor across a disk write would make
        // the contracted 100 ms reclamation deadline unmeetable, because an acknowledgement needs
        // the same monitor and would queue behind an arbitrarily slow device. The blocks are
        // detached, so releasing the monitor here is safe by construction rather than by hope.
        val written = plan.map { case (partitionId, blocks) =>
          (partitionId, blocks, spillPartitionBlocks(partitionId, blocks))
        }
        // Stage three, monitor held again: publish what was written, re-attach what was not, and
        // account for exactly the bytes that actually left memory.
        val result = lock.synchronized(publishEvictionLocked(written))
        result.filesToDelete.foreach(deleteSpillFile)
        if (result.freedBytes > 0L) {
          // The volumes are accounted the same way whichever event this was, because the bytes
          // really did leave memory for local disk in both cases and the feature specifies that
          // they appear on the existing spill accumulators. Only the event count is split.
          memoryBytesSpilledTotal.addAndGet(result.freedBytes)
          diskBytesSpilledTotal.addAndGet(result.committedDiskBytes)
          val elapsedMs = clock.getTimeMillis() - startTimeMs
          lastSpillMs.set(elapsedMs)
          if (durabilityFlush) {
            durabilityFlushTotal.incrementAndGet()
            reportDurabilityFlush(result, elapsedMs)
          } else {
            // One increment per eviction event, never one per partition and never one per block,
            // which is what keeps the telemetry cost off the data path.
            recordSpillEvent()
            reportThresholdEviction(result, elapsedMs)
          }
        }
        result.freedBytes
      }
    }
  }

  /**
   * Reports one reclamation the budget forced, through the executor's own aggregation window.
   *
   * Spilling under pressure is a recurring condition, not an incident: once a workload is over its
   * budget every eviction would produce a line, a producer can evict many times a second, and an
   * executor runs one producer per map task. The default-level record is therefore bounded to one
   * line per window '''for the whole executor''' and carries the executor-wide cumulative totals
   * plus how many evictions it stands in for, so an operator loses no information about volume --
   * only the per-event granularity, which the streaming debug key restores. This is what keeps the
   * executor inside its log-volume budget, whatever its task count, without ever going silent about
   * spilling.
   */
  private def reportThresholdEviction(result: EvictionResult, elapsedMs: Long): Unit = {
    spillLogAggregator.record(clock.getTimeMillis(), result.freedBytes) match {
      case Some(summary) =>
        logInfo(log"Streaming shuffle evicted " +
          log"${MDC(NUM_PARTITIONS, result.evictedPartitions)} buffered partitions holding " +
          log"${MDC(BYTE_SIZE, Utils.bytesToString(result.freedBytes))} to local disk in " +
          log"${MDC(DURATION, elapsedMs)} ms, committing " +
          log"${MDC(NUM_BYTES, result.committedDiskBytes)} bytes " +
          log"(${MDC(COUNT, summary.occurrences)} evictions and " +
          log"${MDC(MEMORY_SIZE, summary.volumeBytes)} bytes spilled on this executor so far, " +
          log"${MDC(NUM_SKIPPED, summary.unreported)} evictions not reported individually)")
      case None =>
        if (debugEnabled) {
          logInfo(log"Streaming shuffle evicted " +
            log"${MDC(NUM_PARTITIONS, result.evictedPartitions)} buffered partitions " +
            log"holding ${MDC(BYTE_SIZE, Utils.bytesToString(result.freedBytes))} to local " +
            log"disk in ${MDC(DURATION, elapsedMs)} ms")
        }
    }
  }

  /**
   * Reports one end-of-stream durability flush, through a window of its own.
   *
   * Worded as what it is -- retained output being made durable so a consumer can still be served
   * after this task's memory has gone -- rather than as a spill, because describing a routine step
   * of every successful map task as memory pressure is what makes a spill signal unreadable. A
   * window of its own, so that a burst of flushes cannot silence the first genuine eviction, or the
   * reverse.
   *
   * The window is the '''executor's''', and for this condition that is not a refinement but the
   * whole point: a flush happens once per successful streaming map task, so an instance-scoped
   * window would put one default-level record in the log for every task of every map stage --
   * precisely the output the aggregation exists to bound. Aggregated executor-wide, a thousand
   * healthy tasks contribute one line and a count of the flushes it stands in for.
   */
  private def reportDurabilityFlush(result: EvictionResult, elapsedMs: Long): Unit = {
    durabilityFlushLogAggregator.record(clock.getTimeMillis(), result.freedBytes) match {
      case Some(summary) =>
        logInfo(log"Streaming shuffle made the retained output of " +
          log"${MDC(NUM_PARTITIONS, result.evictedPartitions)} partitions durable, moving " +
          log"${MDC(BYTE_SIZE, Utils.bytesToString(result.freedBytes))} to local disk in " +
          log"${MDC(DURATION, elapsedMs)} ms and committing " +
          log"${MDC(NUM_BYTES, result.committedDiskBytes)} bytes " +
          log"(${MDC(COUNT, summary.occurrences)} flushes moving " +
          log"${MDC(MEMORY_SIZE, summary.volumeBytes)} bytes on this executor so far, " +
          log"${MDC(NUM_SKIPPED, summary.unreported)} not reported individually)")
      case None =>
        if (debugEnabled) {
          logInfo(log"Streaming shuffle made the retained output of " +
            log"${MDC(NUM_PARTITIONS, result.evictedPartitions)} partitions durable, moving " +
            log"${MDC(BYTE_SIZE, Utils.bytesToString(result.freedBytes))} to local disk in " +
            log"${MDC(DURATION, elapsedMs)} ms")
        }
    }
  }

  /**
   * Chooses eviction victims and detaches their in-memory blocks, in selection order, until at
   * least `bytesToFree` bytes have been planned or no candidate remains. Must be called while
   * holding `lock`.
   *
   * Detached bytes stay charged and stay counted, because the payloads are still on the heap; what
   * changes is only which stage owns them. Exactly one eviction is in flight at a time, so a second
   * caller observing one in progress declines rather than planning an overlapping write of
   * partitions that are already being drained.
   *
   * @return the detached blocks per partition, in the order they should be written
   */
  private def planEvictionLocked(bytesToFree: Long): Seq[(Int, List[BufferedBlock])] = {
    if (closed.get() || evictionInProgress) {
      Nil
    } else {
      val plan = new mutable.ArrayBuffer[(Int, List[BufferedBlock])]()
      var plannedBytes = 0L
      val now = clock.getTimeMillis()
      val candidates = selectionOrderLocked.iterator
      while (candidates.hasNext && plannedBytes < bytesToFree) {
        val partitionId = candidates.next()
        partitionBuffers.get(partitionId).foreach { buffer =>
          val blocks = buffer.memoryBlocks.toList
          if (blocks.nonEmpty) {
            // The charge, not the payload length: what leaves memory is exactly what was reserved
            // on admission, so eviction and admission cannot drift apart.
            val chargedBytes = blocks.foldLeft(0L)((acc, block) => acc + block.chargeBytes)
            buffer.memoryBlocks.clear()
            buffer.spillingBlocks ++= blocks
            buffer.bufferedBytes -= chargedBytes
            buffer.spillingBytes += chargedBytes
            buffer.lastAccessTimeMs = now
            plan += ((partitionId, blocks))
            plannedBytes += chargedBytes
          }
        }
      }
      if (plan.nonEmpty) {
        evictionInProgress = true
      }
      plan.toSeq
    }
  }

  /**
   * Re-attaches or retires every block a planned eviction detached, and reports exactly what left
   * memory. Must be called while holding `lock`.
   *
   * Three outcomes are handled. A partition that was written out has every durable record
   * published, whether or not a consumer acknowledged it while the write was in flight, because a
   * segment on disk is map output rather than a retransmission buffer; a partition that could not
   * be written has its unacknowledged blocks returned to the head of its queue, still servable, so
   * a failing disk costs the shuffle its fast path and never its data; and a partition belonging to
   * an instance closed mid-write releases nothing at all, because closure has already released
   * those bytes and a second release would drive the memory manager's balance negative.
   */
  private def publishEvictionLocked(
      written: Seq[(Int, List[BufferedBlock], Option[Seq[SpilledBlock]])]): EvictionResult = {
    var freedBytes = 0L
    var committedDiskBytes = 0L
    var evictedPartitions = 0
    val filesToDelete = new mutable.ArrayBuffer[File]()
    val instanceClosed = closed.get()
    val now = clock.getTimeMillis()
    written.foreach { case (partitionId, blocks, outcome) =>
      val chargedBytes = blocks.foldLeft(0L)((acc, block) => acc + block.chargeBytes)
      val spilledFiles = outcome.map(_.map(_.file).distinct).getOrElse(Nil)
      partitionBuffers.get(partitionId) match {
        case Some(buffer) if !instanceClosed =>
          val detached = blocks.map(_.sequenceNumber).toSet
          buffer.spillingBlocks.filterInPlace(block => !detached.contains(block.sequenceNumber))
          buffer.spillingBytes -= chargedBytes
          bufferedMemoryBytes -= chargedBytes
          freedBytes += chargedBytes
          val watermark = buffer.acknowledgedThroughSequence
          outcome match {
            case Some(records) =>
              // Every written record is published, including one an acknowledgement covered while
              // the write was in flight. A durable segment is map output rather than a
              // retransmission buffer: acknowledgement releases the memory a block occupied, never
              // the copy on disk that a later reduce attempt has to be able to read.
              buffer.spilledBlockRecords ++= records
              spilledRecordCount += records.size
              spilledMetadataReservedBytes +=
                records.size.toLong * SPILLED_RECORD_METADATA_BYTES
              records.foreach { record =>
                spillFilesByBlockId.update(record.blockId, record.file)
                spillFileBlockIds.update(record.file, record.blockId)
              }
              buffer.lastAccessTimeMs = now
              committedDiskBytes += records.foldLeft(0L)((acc, record) => acc + record.length)
              evictedPartitions += 1
              if (records.isEmpty) {
                // No record names the file, so nothing could ever locate it again; unlinking it
                // here is what keeps an unnameable file from being left on local disk.
                spilledFiles.foreach { file =>
                  if (retireSpillFileLocked(file)) {
                    filesToDelete += file
                  }
                }
              }
            case None =>
              val survivors = blocks.filter(_.sequenceNumber > watermark)
              if (survivors.nonEmpty) {
                val restoredBytes = survivors.foldLeft(0L)((acc, block) => acc + block.chargeBytes)
                buffer.memoryBlocks.prependAll(survivors)
                buffer.bufferedBytes += restoredBytes
                bufferedMemoryBytes += restoredBytes
                freedBytes -= restoredBytes
              }
          }
        case _ =>
          outcome.foreach(records =>
            quota.release(records.size.toLong * SPILLED_RECORD_METADATA_BYTES, MetadataMemory))
          spilledFiles.foreach(filesToDelete += _)
      }
    }
    evictionInProgress = false
    // Woken with the monitor held, which is the only safe moment: a waiter re-tests
    // `evictionInProgress` under the same monitor, so it cannot miss this transition and cannot
    // observe a half-published eviction. This is what lets a producer that needs room, or a
    // producer making its retained window durable, wait out an eviction another thread is running
    // instead of reading the aggregate tally mid-flight and declaring the budget exhausted.
    lock.notifyAll()
    EvictionResult(freedBytes, committedDiskBytes, evictedPartitions, filesToDelete.toSeq)
  }

  /**
   * Orders the partitions eviction can actually reclaim from: largest evictable footprint first,
   * ties broken least-recently-used first and then by ascending partition id. Must be called while
   * holding `lock`.
   *
   * A partition whose blocks another eviction already detached is omitted, because its bytes cannot
   * be reclaimed twice and planning it again would double count the reclamation.
   */
  private def selectionOrderLocked: Seq[Int] = {
    partitionBuffers.values
      .filter(buffer => buffer.bufferedBytes > 0L && buffer.spillingBlocks.isEmpty)
      .toSeq
      .sortBy(buffer => (-buffer.bufferedBytes, buffer.lastAccessTimeMs, buffer.partitionId))
      .map(_.partitionId)
  }

  /**
   * Writes one partition's in-memory blocks to a temporary shuffle block on local disk.
   *
   * A temporary *shuffle* block is used rather than a temporary local one because these files may
   * be read back over the shuffle transport, so their compression must be governed by
   * `spark.shuffle.compress`. The writer is obtained from the block manager rather than constructed
   * directly, which is what keeps the sync-write setting and the serializer manager wiring in one
   * place. Every block is committed on its own so that each lands as an independently decodable
   * unit with exact segment bounds.
   *
   * A non-fatal failure -- including one raised by the temporary block allocation itself, which is
   * performed inside the boundary for exactly that reason -- is logged, counted, rolled back and
   * reported as `None`, so a failing disk costs the shuffle its streaming fast path and nothing
   * else. A fatal error is not contained and propagates.
   *
   * @return the durable records for the blocks, or `None` when the partition could not be written
   */
  /**
   * Opens the writer one spill file is written through.
   *
   * A single-expression delegation to the block manager, extracted as its own member for one
   * reason: it is the only point at which a device failure part way through an eviction can be
   * produced deterministically, and the rollback that such a failure triggers is a correctness
   * guarantee rather than a nicety. A partially written spill file that were left in place, or
   * whose already-detached blocks were not restored, would either serve a consumer a truncated
   * block or lose the block outright -- and both are invisible on a healthy device, which is the
   * only device a test can otherwise arrange. Making the directory unwritable fails the
   * *allocation* instead, several statements earlier, and so exercises a different branch than the
   * one the rollback lives on.
   *
   * `private[streaming]` rather than public: it widens nothing outside this package, adds no
   * behaviour of its own, and every production caller reaches it through the one call site below.
   *
   * @param blockId temporary shuffle block the spill file was allocated as
   * @param file the spill file itself
   * @param serializer serializer instance the writer is opened with, which for a raw-byte spill is
   *                   the dummy instance that performs no serialization
   * @param bufferSizeBytes write buffer size the writer is opened with
   * @param metrics reporter the writer publishes its own byte and record tallies to, which is
   *                deliberately not the task's shuffle-write reporter
   * @return the writer, which the caller owns and must close, revert or delete
   */
  private[streaming] def newSpillWriter(
      blockId: TempShuffleBlockId,
      file: File,
      serializer: SerializerInstance,
      bufferSizeBytes: Int,
      metrics: ShuffleWriteMetrics): DiskBlockObjectWriter = {
    blockManager.getDiskWriter(blockId, file, serializer, bufferSizeBytes, metrics)
  }

  private def spillPartitionBlocks(
      partitionId: Int,
      blocks: Seq[BufferedBlock]): Option[Seq[SpilledBlock]] = {
    if (blocks.isEmpty) {
      return Some(Seq.empty)
    }
    val metadataBytes = blocks.size.toLong * SPILLED_RECORD_METADATA_BYTES
    if (!quota.tryReserve(metadataBytes, MetadataMemory)) {
      recordMemoryPressure(metadataBytes, 0L)
      return None
    }
    val batches = new mutable.ArrayBuffer[Seq[BufferedBlock]]()
    val batch = new mutable.ArrayBuffer[BufferedBlock](MAX_SPILL_SEGMENTS_PER_FILE)
    var batchBytes = 0L
    blocks.foreach { block =>
      val wouldExceedBytes =
        batch.nonEmpty && batchBytes + block.data.length.toLong > MAX_SPILL_FILE_PAYLOAD_BYTES
      if (batch.size >= MAX_SPILL_SEGMENTS_PER_FILE || wouldExceedBytes) {
        batches += batch.toSeq
        batch.clear()
        batchBytes = 0L
      }
      batch += block
      batchBytes += block.data.length.toLong
    }
    if (batch.nonEmpty) {
      batches += batch.toSeq
    }

    val records = new mutable.ArrayBuffer[SpilledBlock](blocks.size)
    val committedFiles = new mutable.ArrayBuffer[File](batches.size)
    var failed = false
    val iterator = batches.iterator
    while (iterator.hasNext && !failed) {
      spillBlockBatch(partitionId, iterator.next()) match {
        case Some(written) =>
          records ++= written
          written.headOption.foreach(record => committedFiles += record.file)
        case None =>
          failed = true
      }
    }
    if (failed) {
      committedFiles.distinct.foreach(deleteSpillFile)
      quota.release(metadataBytes, MetadataMemory)
      None
    } else {
      Some(records.toSeq)
    }
  }

  /** Writes one bounded group of blocks into one quota-reserved spill file. */
  private def spillBlockBatch(
      partitionId: Int,
      blocks: Seq[BufferedBlock]): Option[Seq[SpilledBlock]] = {
    val rawBytes = blocks.foldLeft(0L)((total, block) => total + block.data.length.toLong)
    val expandedBytes = saturatingMultiply(rawBytes, DISK_RESERVATION_MULTIPLIER)
    val estimatedBytes =
      if (expandedBytes > Long.MaxValue - DISK_RESERVATION_OVERHEAD_BYTES) {
        Long.MaxValue
      } else {
        expandedBytes + DISK_RESERVATION_OVERHEAD_BYTES
      }
    val reservation = diskQuota.tryReserve(estimatedBytes) match {
      case Some(granted) => granted
      case None =>
        recordDiskPressure(estimatedBytes,
          s"the executor disk quota of ${diskQuota.totalBytes} bytes or " +
            s"${diskQuota.fileLimit} files is fully committed")
        return None
    }
    // A freshly allocated reporter, never the task's shuffle-write reporter. DiskBlockObjectWriter
    // mutates whichever reporter it receives, so sharing the task reporter would double count.
    val spillMetrics = new ShuffleWriteMetrics
    var file: File = null
    var spillBlockId: TempShuffleBlockId = null
    var writer: DiskBlockObjectWriter = null
    var succeeded = false
    val allocated = try {
      Some(diskBlockManager.createTempShuffleBlock())
    } catch {
      case NonFatal(e) =>
        recordDiskPressure(estimatedBytes,
          s"the selected local directory could not allocate a spill path: " +
            s"${e.getClass.getSimpleName}")
        reservation.cancel()
        None
    }
    if (allocated.isEmpty) {
      return None
    }
    val (blockId, tempFile) = allocated.get
    spillBlockId = blockId
    file = tempFile
    try {
      if (!diskQuota.hasUsableSpace(tempFile, estimatedBytes)) {
        recordDiskPressure(estimatedBytes,
          s"the selected local directory has less than $MIN_LOCAL_DISK_HEADROOM_BYTES bytes of " +
            "headroom beyond this write")
        return None
      }
      writer = newSpillWriter(blockId, tempFile, serializerInstance, fileBufferSizeBytes,
        spillMetrics)
      val records = new mutable.ArrayBuffer[SpilledBlock](blocks.size)
      blocks.foreach { block =>
        writer.write(block.data, 0, block.data.length)
        writer.recordWritten()
        val segment = writer.commitAndGet()
        records += SpilledBlock(partitionId, block.sequenceNumber, blockId, tempFile,
          segment.offset, segment.length, block.data.length)
      }
      writer.close()
      writer = null
      val actualBytes = tempFile.length()
      if (!reservation.commit(tempFile, actualBytes)) {
        recordDiskPressure(actualBytes,
          s"the committed file would exceed the executor disk quota of ${diskQuota.totalBytes} " +
            "bytes")
        None
      } else {
        succeeded = true
        Some(records.toSeq)
      }
    } catch {
      case NonFatal(e) =>
        spillFailureTotal.incrementAndGet()
        reportSpillFailure(partitionId, spillBlockId, file, e)
        None
    } finally {
      if (!succeeded) {
        if (writer != null) {
          Utils.tryLogNonFatalError {
            writer.revertPartialWritesAndClose()
          }
        }
        if (file != null) {
          deleteSpillFile(file)
        }
        reservation.cancel()
      }
    }
  }

  /** Reports one bounded spill-write failure without exposing local paths at default log level. */
  private def reportSpillFailure(
      partitionId: Int,
      spillBlockId: TempShuffleBlockId,
      file: File,
      failure: Throwable): Unit = {
    spillFailureLogAggregator.record(clock.getTimeMillis()) match {
      case Some(summary) =>
        val destination =
          if (spillBlockId == null) "an unallocated spill block" else spillBlockId.name
        logError(log"Failed to evict streaming shuffle partition " +
          log"${MDC(PARTITION_ID, partitionId)} to spill block " +
          log"${MDC(BLOCK_ID, destination)}: ${MDC(CLASS_NAME, failure.getClass.getName)} " +
          log"(${MDC(COUNT, summary.occurrences)} spill failures on this executor so far, " +
          log"${MDC(NUM_SKIPPED, summary.unreported)} not reported individually)")
      case None =>
        if (debugEnabled) {
          val target = if (file == null) "an unallocated spill file" else file.getAbsolutePath
          logError(log"Failed to evict streaming shuffle partition " +
            log"${MDC(PARTITION_ID, partitionId)} to spill file ${MDC(PATH, target)}", failure)
        }
    }
  }

  // Acknowledgement-driven reclamation

  /**
   * Admits a consumer to the acknowledgement protocol for this producer.
   *
   * Only a registered consumer may acknowledge, and retirement advances only as far as the slowest
   * registered consumer, so registering a consumer that never acknowledges holds the retransmission
   * window open rather than closing it early. That asymmetry is what makes it safe for the egress
   * handler to admit a consumer as it subscribes: admitting a party can only ever hold the window
   * open for longer, never release a byte, so no identity a peer chooses for itself can cause its
   * output to be discarded before the consumers entitled to it have confirmed receipt. Holding the
   * window open for a consumer that has fallen silent is not a leak either but the specified
   * behaviour -- the unacknowledged window is retained, spilled under pressure and replayed on
   * reconnection, and a consumer that never returns is escalated by the producer's own
   * retransmission budget rather than by discarding its data.
   *
   * Idempotent: re-registering a consumer already present keeps the position it has reached, so a
   * reconnecting consumer does not rewind the window it has already advanced.
   *
   * New identities are admitted only while both the per-store cursor cap and the executor-wide
   * unique-identity cap have room. The latter is reference counted, so one reduce task may register
   * with many map outputs without being counted as many identities, while identity churn cannot
   * create permanent cursor state without bound.
   *
   * A consumer may be admitted after the producing task has finished, and normally is: the
   * scheduler starts no reduce task before its map stage completes, so almost every consumer
   * subscribes to a store that has already released its memory and handed its files to the block
   * resolver. Registration is therefore governed by whether this store still serves its output --
   * [[servesRetainedOutput]] -- rather than by whether the producing task is still running. A store
   * that owns its files and has been closed serves nothing, because those files are gone.
   *
   * @param consumerId identity of the consumer, as the producer knows it; must be non-empty
   * @return true when the consumer is now registered, false when this store no longer serves or a
   *         consumer-registration cap is full
   */
  def registerConsumer(consumerId: String): Boolean = {
    require(consumerId != null && consumerId.nonEmpty, "consumerId must be non-empty")
    lock.synchronized {
      if (!servesRetainedOutputLocked) {
        false
      } else if (consumerPositions.contains(consumerId)) {
        true
      } else if (consumerPositions.size >= MAX_REGISTERED_CONSUMERS_PER_STORE ||
          !MemorySpillManager.acquireConsumerIdentity(consumerId)) {
        rejectedConsumerRegistrations.incrementAndGet()
        false
      } else {
        consumerPositions.put(consumerId, new mutable.HashMap[Int, Long]())
        true
      }
    }
  }

  /**
   * Removes a consumer from the acknowledgement protocol and retires whatever its departure makes
   * retirable.
   *
   * A consumer that has failed for good must be removed, because until it is, the minimum position
   * across registered consumers cannot advance past whatever it last acknowledged and the window it
   * pinned can never be reclaimed. Removing the last consumer leaves the window pinned rather than
   * releasing it wholesale: with no consumer left, nothing has confirmed receipt of anything, and
   * discarding on that basis is precisely the destructive behaviour this protocol prevents.
   *
   * @param consumerId the consumer to remove; must be non-empty
   * @return the number of bytes the departure released back to the memory manager
   */
  def unregisterConsumer(consumerId: String): Long = {
    require(consumerId != null && consumerId.nonEmpty, "consumerId must be non-empty")
    val (removed, partitions) = lock.synchronized {
      val removed = consumerPositions.remove(consumerId).isDefined
      val partitions =
        if (!removed || consumerPositions.isEmpty) Nil else partitionBuffers.keys.toList
      (removed, partitions)
    }
    if (removed) {
      MemorySpillManager.releaseConsumerIdentity(consumerId)
    }
    partitions.foldLeft(0L)((freed, partitionId) => freed + requestReclamation(partitionId))
  }

  /** The consumers currently entitled to acknowledge. */
  def registeredConsumers: Set[String] = lock.synchronized(consumerPositions.keySet.toSet)

  /** Consumer registrations refused by the per-store or executor-wide identity cap. */
  def rejectedConsumerRegistrationCount: Long = rejectedConsumerRegistrations.get()

  /**
   * Releases every retained consumer cursor when the block resolver drops this producer.
   *
   * A successful map task detaches its spill files and closes its task-owned memory while keeping
   * these cursors alive for post-task consumers. The resolver is therefore the final owner and must
   * release the cursor registrations when the generation is superseded, the shuffle is unregistered
   * or the resolver stops.
   *
   * @return the number of consumer identities released
   */
  private[streaming] def releaseRetainedConsumerState(): Int = {
    var metadataBytes = 0L
    val consumers = lock.synchronized {
      val snapshot = consumerPositions.keys.toSeq
      consumerPositions.clear()
      pendingReclamationPartitions.clear()
      pendingReclamationSet.clear()
      reclamationRequestedAtMs.clear()
      reclamationDispatchQueued.set(false)
      if (closed.get() && spillFilesDetached) {
        metadataBytes = spilledMetadataReservedBytes
        spilledMetadataReservedBytes = 0L
        partitionBuffers.values.foreach(_.spilledBlockRecords.clear())
        spilledRecordCount = 0
      }
      snapshot
    }
    consumers.foreach(MemorySpillManager.releaseConsumerIdentity)
    if (metadataBytes > 0L) {
      quota.release(metadataBytes, MetadataMemory)
    }
    consumers.size
  }

  /**
   * The position a consumer has acknowledged for a partition, or `None` when it has acknowledged
   * nothing there -- which is deliberately distinct from having acknowledged sequence number zero.
   *
   * @param consumerId the consumer to report on
   * @param partitionId the reduce partition to report on
   */
  def consumerPosition(consumerId: String, partitionId: Int): Option[Long] = lock.synchronized {
    consumerPositions.get(consumerId).flatMap(_.get(partitionId))
  }

  /**
   * Number of acknowledgements refused because the acknowledging party was not registered, because
   * the position did not advance that party's own, or because it named a block this producer never
   * sent.
   */
  def rejectedAcknowledgementCount: Long = rejectedAcknowledgements.get()

  /**
   * Records that a consumer has received everything in a partition up to and including
   * `throughSequenceNumber`, and releases the buffer memory that makes releasable -- which is what
   * a consumer acknowledgement means: those bytes reached their consumer, so the producer need not
   * hold them in memory against a retransmission request.
   *
   * Three checks stand between an acknowledgement and the buffers it would retire, and all three
   * are evaluated under the same monitor that performs the retirement:
   *
   *  - the consumer must be registered, so an unregistered party cannot retire anything;
   *  - the position must strictly advance that consumer's own previous position for the partition,
   *    so a replayed acknowledgement is inert and a late, lower one can never un-retire what a
   *    higher one already covered;
   *  - the position may not exceed the highest sequence number this producer has actually admitted
   *    for the partition, so a position of `Long.MaxValue` -- or any other value plucked from the
   *    air -- retires exactly the blocks that were really sent and not one more.
   *
   * Retirement then advances to the *minimum* position across every registered consumer, so memory
   * is released only once every consumer entitled to it has confirmed receipt. Because a
   * partition's spilled blocks always precede its in-memory blocks, and both queues are ordered
   * by sequence number, a monotonically advancing retirement position always retires a prefix.
   *
   * What an acknowledgement releases is *memory*, and only memory. A block that had already been
   * evicted gave its bytes back at eviction time and leaves nothing here to release: its segment on
   * disk is map output, not a retransmission buffer, and stays readable until the block resolver
   * unlinks it at generation withdrawal, at shuffle unregistration or at its own shutdown.
   * Unlinking it on acknowledgement would destroy output a later attempt of the same reduce task is
   * entitled to read, which is the re-readability Spark's recovery model assumes of map output.
   *
   * Per-acknowledgement work is bounded. The call retires at most
   * [[MemorySpillManager.MAX_RECLAIMED_BLOCKS_PER_BATCH]] blocks under the store lock and
   * dispatches any remaining prefix to the executor-scoped reclaimer in equally bounded batches.
   * The achieved end-to-end latency is recorded on [[lastReclamationDurationMs]], and a breach is
   * counted on [[reclamationDeadlineBreaches]] and logged.
   *
   * @param consumerId identity of the acknowledging consumer; must be non-empty and registered
   * @param partitionId the reduce partition being acknowledged; must be non-negative
   * @param throughSequenceNumber the highest sequence number the consumer has received
   * @return the number of bytes released back to the memory manager, and zero when the
   *         acknowledgement was refused or released nothing
   */
  def acknowledge(consumerId: String, partitionId: Int, throughSequenceNumber: Long): Long = {
    require(consumerId != null && consumerId.nonEmpty, "consumerId must be non-empty")
    require(partitionId >= 0, s"partitionId must be non-negative, but was $partitionId")
    val accepted = lock.synchronized {
      // A store that has handed its files to the resolver still accepts acknowledgements, and must:
      // its consumers do almost all of their acknowledging after the producing task has ended, and
      // refusing them there would leave every cursor frozen at the position it held when the task
      // finished -- so a reconnecting consumer would be replayed output it had already consumed.
      // Such an acknowledgement releases no memory, because there is none left to release; what it
      // advances is the cursor that bounds replay.
      servesRetainedOutputLocked &&
        recordAcknowledgementLocked(consumerId, partitionId, throughSequenceNumber)
    }
    if (!accepted) {
      rejectedAcknowledgements.incrementAndGet()
      0L
    } else {
      requestReclamation(partitionId)
    }
  }

  /**
   * Validates an acknowledgement and, if it stands, records the consumer's new position. Must be
   * called while holding `lock`, because the verdict is only meaningful when it is atomic with
   * recording the position that establishes the next verdict.
   */
  private def recordAcknowledgementLocked(
      consumerId: String,
      partitionId: Int,
      throughSequenceNumber: Long): Boolean = {
    consumerPositions.get(consumerId).exists { positions =>
      // The highest sequence number admitted for the partition is the high-water mark of what was
      // really sent. It is retained across retirement precisely so that this bound survives a
      // partition draining completely.
      val sentHighWater =
        partitionBuffers.get(partitionId).map(_.lastAcceptedSequence).getOrElse(UNSET_SEQUENCE)
      val advances = positions.get(partitionId).forall(throughSequenceNumber > _)
      val withinHighWater =
        throughSequenceNumber >= 0L && sentHighWater != UNSET_SEQUENCE &&
          throughSequenceNumber <= sentHighWater
      if (advances && withinHighWater) {
        positions.put(partitionId, throughSequenceNumber)
        true
      } else {
        false
      }
    }
  }

  /**
   * The position through which a partition may be retired: the minimum position acknowledged across
   * every registered consumer.
   *
   * Answers [[MemorySpillManager.UNSET_SEQUENCE]] -- which retires nothing -- when any registered
   * consumer has acknowledged nothing for the partition, and when no consumer is registered at all,
   * because in neither case has every entitled party confirmed receipt of anything.
   *
   * Must be called while holding `lock`.
   */
  private def retirementPositionLocked(partitionId: Int): Long = {
    if (consumerPositions.isEmpty) {
      UNSET_SEQUENCE
    } else {
      var minimum = Long.MaxValue
      var pinned = false
      val iterator = consumerPositions.valuesIterator
      while (!pinned && iterator.hasNext) {
        iterator.next().get(partitionId) match {
          case Some(position) => if (position < minimum) minimum = position
          case None => pinned = true
        }
      }
      if (pinned) UNSET_SEQUENCE else minimum
    }
  }

  /**
   * Starts reclamation for one partition and performs the first bounded batch synchronously.
   *
   * The acknowledgement-facing call can therefore retire the common small prefix immediately while
   * never scanning or removing more than [[MAX_RECLAIMED_BLOCKS_PER_BATCH]] entries under the store
   * lock. Any remainder is queued once for the executor-scoped reclamation worker.
   */
  private def requestReclamation(partitionId: Int): Long = {
    lock.synchronized {
      reclamationRequestedAtMs.getOrElseUpdate(partitionId, clock.getTimeMillis())
    }
    val (freedBytes, hasMore) = reclaimBatch(partitionId)
    if (hasMore) {
      queueReclamation(partitionId)
    }
    freedBytes
  }

  /**
   * Releases at most one bounded prefix of one partition's memory.
   *
   * Spilled segments are deliberately untouched: they are durable map output whose lifetime belongs
   * to the shuffle. The acknowledgement watermark still advances before any in-memory block is
   * removed, so an eviction already in flight cannot publish a block this batch retired.
   *
   * @return bytes released by this batch and whether another eligible in-memory block remains
   */
  private def reclaimBatch(partitionId: Int): (Long, Boolean) = {
    var memoryFreed = 0L
    var hasMore = false
    var requestedAtMs = clock.getTimeMillis()
    lock.synchronized {
      requestedAtMs = reclamationRequestedAtMs.getOrElseUpdate(partitionId, requestedAtMs)
      val position = retirementPositionLocked(partitionId)
      partitionBuffers.get(partitionId).foreach { buffer =>
        if (position > buffer.acknowledgedThroughSequence) {
          buffer.acknowledgedThroughSequence = position
        }
        var retiredBlocks = 0
        while (retiredBlocks < MAX_RECLAIMED_BLOCKS_PER_BATCH &&
            buffer.memoryBlocks.nonEmpty &&
            buffer.memoryBlocks.head.sequenceNumber <= position) {
          val retired = buffer.memoryBlocks.removeHead()
          val bytes = retired.chargeBytes
          buffer.bufferedBytes -= bytes
          bufferedMemoryBytes -= bytes
          memoryFreed += bytes
          retiredBlocks += 1
        }
        hasMore = buffer.memoryBlocks.nonEmpty &&
          buffer.memoryBlocks.head.sequenceNumber <= position
        buffer.lastAccessTimeMs = clock.getTimeMillis()
      }
      if (!hasMore) {
        reclamationRequestedAtMs.remove(partitionId)
      }
    }
    if (memoryFreed > 0L) {
      releaseReclaimedBytes(memoryFreed)
    }
    reclamationBatchTotal.incrementAndGet()
    if (!hasMore) {
      recordReclamationCompletion(partitionId, requestedAtMs)
    }
    (memoryFreed, hasMore)
  }

  /** Adds one partition to this manager's coalesced asynchronous reclamation queue. */
  private def queueReclamation(partitionId: Int): Unit = {
    val added = lock.synchronized {
      if (pendingReclamationSet.add(partitionId)) {
        pendingReclamationPartitions.append(partitionId)
        true
      } else {
        false
      }
    }
    if (added) {
      scheduleReclamationDispatch()
    }
  }

  /** Queues this manager once on the executor-scoped dispatcher when background work is enabled. */
  private def scheduleReclamationDispatch(): Unit = {
    if (autoPoll && reclamationDispatchQueued.compareAndSet(false, true)) {
      MemorySpillManager.enqueueForReclamation(this)
    }
  }

  /** Runs one queued partition batch on the executor-scoped reclamation worker. */
  private def runScheduledReclamationBatch(): Unit = {
    reclamationDispatchQueued.set(false)
    if (!closed.get()) {
      drainOnePendingReclamationBatch()
      if (lock.synchronized(pendingReclamationPartitions.nonEmpty)) {
        scheduleReclamationDispatch()
      }
    }
  }

  /** Drains one queued partition batch, re-queueing it when another bounded prefix remains. */
  private def drainOnePendingReclamationBatch(): Long = {
    val partition = lock.synchronized {
      if (pendingReclamationPartitions.nonEmpty) {
        val next = pendingReclamationPartitions.removeHead()
        pendingReclamationSet.remove(next)
        Some(next)
      } else {
        None
      }
    }
    partition.map { partitionId =>
      val (freedBytes, hasMore) = reclaimBatch(partitionId)
      if (hasMore) {
        queueReclamation(partitionId)
      }
      freedBytes
    }.getOrElse(0L)
  }

  /**
   * Deterministically drains queued batches when the background worker is disabled by a test.
   *
   * @return bytes released by the batches this call ran
   */
  private[streaming] def drainPendingReclamationForTesting(): Long = {
    var total = 0L
    var continue = true
    while (continue) {
      val pending = lock.synchronized(pendingReclamationPartitions.nonEmpty)
      if (pending) {
        total += drainOnePendingReclamationBatch()
      } else {
        continue = false
      }
    }
    total
  }

  /** Partitions awaiting an asynchronous reclamation batch. */
  private[streaming] def pendingReclamationCount: Int =
    lock.synchronized(pendingReclamationPartitions.size)

  /** Bounded reclamation batches run by this producer store. */
  private[streaming] def reclamationBatchCount: Long = reclamationBatchTotal.get()

  /** Records end-to-end latency from acknowledgement acceptance to the final batch. */
  private def recordReclamationCompletion(partitionId: Int, requestedAtMs: Long): Unit = {
    val elapsedMs = math.max(0L, clock.getTimeMillis() - requestedAtMs)
    lastReclamationMs.set(elapsedMs)
    if (elapsedMs > RECLAMATION_DEADLINE_MS) {
      reclamationBreachTotal.incrementAndGet()
      reclamationLogAggregator.record(clock.getTimeMillis()) match {
        case Some(summary) =>
          logWarning(log"Streaming shuffle buffer reclamation for partition " +
            log"${MDC(PARTITION_ID, partitionId)} took ${MDC(DURATION, elapsedMs)} ms, exceeding " +
            log"the ${MDC(THRESHOLD, RECLAMATION_DEADLINE_MS)} ms reclamation bound " +
            log"(${MDC(COUNT, summary.occurrences)} breaches on this executor so far, " +
            log"${MDC(NUM_SKIPPED, summary.unreported)} not reported individually)")
        case None =>
          if (debugEnabled) {
            logInfo(log"Streaming shuffle buffer reclamation for partition " +
              log"${MDC(PARTITION_ID, partitionId)} took ${MDC(DURATION, elapsedMs)} ms")
          }
      }
    }
  }

  // Lifecycle

  /**
   * Registers release on task completion, so buffers are freed and spill files removed whether the
   * task succeeds, fails or is cancelled. Idempotent: only the first call registers a listener, so
   * a producer may call it defensively without risking a double release.
   *
   * @param context the task context whose completion release is bound to
   */
  def registerCleanup(context: TaskContext): Unit = {
    require(context != null, "context must not be null")
    // Retained before the listener is installed and independently of whether this call is the one
    // that installs it, so the owning task is known from the earliest possible moment. First
    // registration wins: a consumer belongs to exactly one task for its whole life.
    ownerContext.compareAndSet(null, context)
    if (cleanupRegistered.compareAndSet(false, true)) {
      context.addTaskCompletionListener[Unit](completed => closeInternal(Option(completed)))
    }
  }

  /**
   * Releases every buffer and removes every spill file. Idempotent and safe to call from any
   * thread. Each spill-file removal absorbs its own non-fatal failure, but a failure reported by
   * the memory manager or by metric publication propagates to the caller. Registered task cleanup
   * calls this, so an explicit call is only needed by an owner that is finished with this consumer
   * before its task is.
   *
   * The retained owning task context is preferred over the calling thread's, because an owner may
   * legitimately close this consumer from a thread that carries no task context -- or, worse, one
   * that carries a different task's -- and the spill accounting belongs to the task that did the
   * spilling either way.
   */
  def close(): Unit =
    closeInternal(Option(ownerContext.get()).orElse(Option(TaskContext.get())))

  /**
   * The single release path. The task context is passed in rather than looked up so that the
   * completion listener reports against the context it was registered on.
   */
  private def closeInternal(context: Option[TaskContext]): Unit = {
    val filesToDelete = new mutable.ArrayBuffer[File]()
    var accountedBytes = 0L
    var metadataBytesToRelease = 0L
    var admissionsInFlight = 0
    var consumerIdsToRelease = Seq.empty[String]
    // The lifecycle flip and the tally capture are one indivisible step, taken under the same
    // monitor that refuses admission. Once this block returns, no further admission can be reserved
    // and no further eviction can be planned, so the captured tally is final.
    val firstClose = lock.synchronized {
      if (closed.compareAndSet(false, true)) {
        // Scratch is released with the blocks, and through the same two budgets, so a producer that
        // failed before returning its accumulators cannot leave the quota short.
        accountedBytes = bufferedMemoryBytes + scratchReservedBytes
        scratchReservedBytes = 0L
        admissionsInFlight = inFlightAdmissions
        // Files are deleted here only while this instance still owns them. Once
        // [[releaseSpillFileOwnership]] has handed them to the block resolver they must survive,
        // because the whole reason for the hand-off is that a consumer may still have to be served
        // from them after the producing task has gone; the resolver unlinks them instead.
        val ownsFiles = !spillFilesDetached
        partitionBuffers.values.foreach { buffer =>
          buffer.memoryBlocks.clear()
          buffer.spillingBlocks.clear()
          if (ownsFiles) {
            buffer.spilledBlockRecords.foreach(record => filesToDelete += record.file)
            buffer.spilledBlockRecords.clear()
          }
          buffer.bufferedBytes = 0L
          buffer.spillingBytes = 0L
        }
        // In-memory blocks always go, because the memory behind them is being returned. The segment
        // records survive a detached close, and that is what makes the hand-off worth performing:
        // the resolver locates a spilled block through this instance's records, so discarding them
        // would leave files on disk that nothing could name and no consumer could ever be served
        // from -- the files would outlive the task and be useless, which is the worst of both.
        if (ownsFiles) {
          metadataBytesToRelease = spilledMetadataReservedBytes
          spilledMetadataReservedBytes = 0L
          partitionBuffers.clear()
          spilledRecordCount = 0
        }
        bufferedMemoryBytes = 0L
        // Every spill file this instance still owns goes, leased or not. A lease is held by
        // a reader of bytes this task produced, and while this instance owns its files no such
        // reader may outlive the task, so an outstanding lease at this point is a bug in the reader
        // rather than a reason to leak a file onto local disk.
        //
        // A detached close keeps all four structures instead. Leases, retirements and the two
        // name indexes are how a spilled block is found and how it is stopped from being unlinked
        // while it is being read, and those are exactly the operations that must keep working after
        // this task has gone; the resolver performs the deletion at its own boundary.
        if (ownsFiles) {
          filesToDelete ++= spillFileLeases.keys
          filesToDelete ++= retiredSpillFiles
          spillFileLeases.clear()
          retiredSpillFiles.clear()
          spillFilesByBlockId.clear()
          spillFileBlockIds.clear()
        }
        // The consumer registry survives a detached close and goes with an owning one. Each
        // consumer's position is where its replay would resume from, so discarding it at task
        // completion would make every reconnecting consumer look like one that had never been
        // served -- and it is precisely after task completion that consumers do their reading. An
        // owning close has nothing left to replay, so its registry is only heap.
        if (ownsFiles) {
          consumerIdsToRelease = consumerPositions.keys.toSeq
          consumerPositions.clear()
        }
        pendingReclamationPartitions.clear()
        pendingReclamationSet.clear()
        reclamationRequestedAtMs.clear()
        reclamationDispatchQueued.set(false)
        true
      } else {
        false
      }
    }
    if (firstClose) {
      // Release precisely this instance's own committed tally, from both budgets. Bytes that were
      // reserved but not yet committed are not in this tally: they belong to the admitting thread,
      // which releases them itself on finding the instance closed, so neither side double releases.
      if (accountedBytes > 0L) {
        quota.release(accountedBytes)
        freeMemory(accountedBytes)
      }
      if (metadataBytesToRelease > 0L) {
        quota.release(metadataBytesToRelease, MetadataMemory)
      }
      // Residual reconciliation, and only when nothing was in flight. The memory manager's view is
      // the one the task-level leak detector checks, so a drift in this class's own bookkeeping
      // would otherwise fail the task; with no admission in flight, and none possible now that the
      // instance is closed, nothing can be acquired after this read and the release cannot race
      // one.
      if (admissionsInFlight == 0) {
        val residual = getUsed()
        if (residual > 0L) {
          logWarning(log"Streaming shuffle buffer accounting left " +
            log"${MDC(NUM_BYTES, residual)} bytes reserved after releasing " +
            log"${MDC(MEMORY_SIZE, accountedBytes)} accounted bytes; releasing the remainder")
          freeMemory(residual)
        }
      }
      MemorySpillManager.deregisterFromPolling(this)
      consumerIdsToRelease.foreach(MemorySpillManager.releaseConsumerIdentity)
      filesToDelete.distinct.foreach(deleteSpillFile)
      reportTaskMetrics(context)
      if (debugEnabled) {
        logInfo(log"Released streaming shuffle buffers after " +
          log"${MDC(COUNT, spillCountTotal.get())} evictions and " +
          log"${MDC(NUM_EVENTS, durabilityFlushTotal.get())} durability flushes totalling " +
          log"${MDC(BYTE_SIZE, Utils.bytesToString(memoryBytesSpilledTotal.get()))}")
      }
    }
  }

  // Spill file leases. A block resolver serves a spilled block by handing out a lazily opened file
  // segment, so the file must outlive the record that named it: a consumer acknowledgement can
  // retire the last record referring to a file at any moment, and unlinking it underneath a reader
  // that has not opened it yet is a time-of-check-to-time-of-use race that surfaces as a missing
  // spill file rather than as anything diagnosable. Reference counting closes it: a reader leases
  // the file for as long as it may open it, and the file is unlinked only once it is both retired
  // and unleased.

  /**
   * Hands the spill files this instance produced to the executor-scoped block resolver.
   *
   * Called by a producer whose map task succeeded, once its unacknowledged window has been made
   * durable. From this call onwards [[close]] frees this instance's memory as it always did but
   * unlinks none of its files, so the resolver can serve them to a consumer that subscribes or
   * reconnects after the producing task has gone. The resolver owns the deletion from then on and
   * performs it when the generation is superseded, when the shuffle is unregistered, or when the
   * resolver itself stops -- which is precisely the "acknowledgement, shuffle unregistration or
   * generation invalidation" boundary the feature specifies, rather than the arbitrary moment a
   * producing task happens to finish.
   *
   * Idempotent, and safe to call on a closed instance: a second call reports the same file set and
   * changes nothing. The set is a snapshot of distinct files, so a caller may keep it after this
   * instance is gone.
   *
   * @return every spill file this instance produced, whose deletion the caller now owns
   */
  def releaseSpillFileOwnership(): Seq[File] = {
    lock.synchronized {
      spillFilesDetached = true
      // Both maps are consulted, and the retired set with them: a file may hold nothing but
      // acknowledged segments and still be referenced by an in-flight lease, and one that no record
      // names is still this instance's to hand over rather than to leak.
      (spillFileBlockIds.keys.toSeq ++ spillFileLeases.keys.toSeq ++ retiredSpillFiles.toSeq)
        .distinct
    }
  }

  /** Whether the spill files of this instance are now owned by the block resolver. */
  def spillFilesTransferred: Boolean = lock.synchronized(spillFilesDetached)

  /**
   * Takes a reader lease on a spill file, so that it will not be unlinked while the lease is held.
   *
   * @param file the spill file to lease
   * @return true when the lease was granted and the file may be opened; false when the file has
   *         already been retired or this instance is closed, in which case the caller must not open
   *         it and must instead treat the block as no longer servable from disk
   */
  def acquireSpillFileLease(file: File): Boolean = {
    require(file != null, "file must not be null")
    lock.synchronized {
      // A closed instance grants no lease while it still owns its files, because it has already
      // unlinked them. Once ownership has been handed to the block resolver the files are still
      // there and serving them is the entire purpose of the hand-off, so a detached close keeps
      // granting. A retired file is refused in either case: nothing retained names it any more.
      if (!servesRetainedOutputLocked || retiredSpillFiles.contains(file)) {
        false
      } else {
        spillFileLeases.update(file, spillFileLeases.getOrElse(file, 0) + 1)
        true
      }
    }
  }

  /**
   * Drops a reader lease taken by [[acquireSpillFileLease]], unlinking the file if it was retired
   * while the lease was held. Calling it more than once for one lease is refused rather than
   * allowed to drive the count negative, because an over-release would unlink a file another reader
   * is still entitled to open.
   *
   * @param file the spill file whose lease is being dropped
   */
  def releaseSpillFileLease(file: File): Unit = {
    require(file != null, "file must not be null")
    val deletable = lock.synchronized {
      spillFileLeases.get(file) match {
        case Some(count) if count > 1 =>
          spillFileLeases.update(file, count - 1)
          false
        case Some(_) =>
          spillFileLeases.remove(file)
          // A detached file is the resolver's to unlink, at generation invalidation, shuffle
          // unregistration or resolver shutdown. Deleting it here on the last lease release would
          // take it away from every consumer that has not asked for it yet.
          !spillFilesDetached && retiredSpillFiles.contains(file)
        case None =>
          logWarning(log"Streaming shuffle spill file ${MDC(FILE_NAME, file.getName)} was " +
            log"released without an outstanding lease; ignoring")
          false
      }
    }
    if (deletable) {
      deleteSpillFile(file)
      lock.synchronized(retiredSpillFiles.remove(file))
    }
  }

  /**
   * The spill file allocated under a temporary block identity, if this consumer still retains a
   * record in it.
   *
   * Answered from an index, so a caller holding a block name learns whether this consumer owns it
   * in constant time instead of by traversing every segment this consumer has spilled. A file whose
   * every record has been retired is not answered: nothing retained names it, and a reader arriving
   * for it is arriving for bytes their consumer has already acknowledged.
   *
   * @param blockId the temporary shuffle block identity a spill file was allocated under
   */
  def spillFile(blockId: TempShuffleBlockId): Option[File] =
    lock.synchronized(spillFilesByBlockId.get(blockId))

  /** Outstanding reader leases on a spill file, for diagnostics and for tests. */
  def spillFileLeaseCount(file: File): Int = lock.synchronized(spillFileLeases.getOrElse(file, 0))

  /** Whether a spill file has been retired and is awaiting only the release of its last lease. */
  def spillFileRetired(file: File): Boolean = lock.synchronized(retiredSpillFiles.contains(file))

  /**
   * Marks a spill file as no longer referenced by any retained record. Must be called while holding
   * `lock`.
   *
   * @return true when the file may be unlinked immediately, which is the case only when no reader
   *         lease is outstanding; false when a lease still holds it, in which case the last release
   *         of that lease performs the unlink
   */
  private def retireSpillFileLocked(file: File): Boolean = {
    // Retirement means no retained record names this file any more, so it must stop being findable
    // by name. Doing it here rather than at each call site is what keeps the index from outliving
    // the records it indexes, whichever path retires them.
    spillFileBlockIds.remove(file).foreach(blockId => spillFilesByBlockId.remove(blockId))
    if (spillFileLeases.contains(file)) {
      retiredSpillFiles.add(file)
      false
    } else {
      retiredSpillFiles.remove(file)
      true
    }
  }

  /**
   * Publishes spill volume and peak reservation onto Spark's existing task accumulators, exactly
   * once. Reusing `memoryBytesSpilled`, `diskBytesSpilled` and `peakExecutionMemory` rather than
   * introducing parallel counters is what makes the streaming path visible on the UI, the history
   * server, the event log and the metrics REST API without a single change to any of them.
   */
  private def reportTaskMetrics(context: Option[TaskContext]): Unit = {
    // The retained owning context is the fallback, and the report is claimed only once a context
    // is actually in hand: claiming it first would let a close performed off the task thread mark
    // the accounting as reported and then discard it, which is exactly how spilled bytes go
    // missing from a task's metrics.
    context.orElse(Option(ownerContext.get())).foreach { taskContext =>
      if (metricsReported.compareAndSet(false, true)) {
        val memorySpilled = memoryBytesSpilledTotal.get()
        val diskSpilled = diskBytesSpilledTotal.get()
        val peak = peakMemoryBytesSeen.get()
        val metrics = taskContext.taskMetrics()
        if (memorySpilled > 0L) {
          metrics.incMemoryBytesSpilled(memorySpilled)
        }
        if (diskSpilled > 0L) {
          metrics.incDiskBytesSpilled(diskSpilled)
        }
        if (peak > 0L) {
          metrics.incPeakExecutionMemory(peak)
        }
      }
    }
  }

  // Telemetry and disk helpers

  /**
   * Counts one spill event, on this instance's tally and on the exported counter alike.
   *
   * '''Why a single seam.''' A spill event and the disk volume it produces are two halves of one
   * observation, and an operator diagnosing capacity reads them together: a spill rate is events
   * over time and an average spill size is volume over events. Every path that puts pressure-driven
   * bytes on local disk therefore has to advance both, and routing all of them through one method
   * is what makes that structural rather than a convention each call site has to remember. Two
   * paths reach here -- an eviction the budget forced, and a block [[admitDurably]] wrote straight
   * to disk because the allowance could not admit it at all -- and both are the same condition
   * from an operator's point of view: the configured buffer percentage was not enough for what the
   * producer was holding, so bytes went to disk to keep the bound.
   *
   * The end-of-stream durability flush deliberately does not come here; see [[spillCount]] for why
   * counting a routine step of every successful map task would destroy the signal. Its volume still
   * reaches the ordinary spill accumulators, because those bytes really did reach local disk.
   *
   * One increment per event, never one per partition and never one per block, so the telemetry cost
   * stays off the data path.
   */
  private def recordSpillEvent(): Unit = {
    spillCountTotal.incrementAndGet()
    StreamingShuffleMetricsSource.incrementSpillCount(1L)
  }

  /** Advances the peak-reservation high-water mark without taking a lock. */
  private def recordPeakMemory(): Unit = {
    val current = getUsed()
    var settled = false
    while (!settled) {
      val observed = peakMemoryBytesSeen.get()
      settled = current <= observed || peakMemoryBytesSeen.compareAndSet(observed, current)
    }
  }

  /**
   * Publishes the memory-pressure trip condition. The first occurrence is reported at warning level
   * because it is the moment streaming stopped being sustainable; later ones are gated behind the
   * streaming debug key so that sustained pressure cannot flood the executor log.
   */
  private def recordMemoryPressure(requested: Long, granted: Long): Unit = {
    memoryPressureCount.incrementAndGet()
    if (memoryPressure.compareAndSet(false, true)) {
      logWarning(log"Streaming shuffle buffer reservation of " +
        log"${MDC(MEMORY_SIZE, requested)} bytes could not be satisfied, only " +
        log"${MDC(NUM_BYTES, granted)} bytes being available; signalling memory pressure so the " +
        log"streaming shuffle can fall back to sort-based shuffle if it persists")
    } else if (debugEnabled) {
      logInfo(log"Streaming shuffle buffer reservation of ${MDC(MEMORY_SIZE, requested)} bytes " +
        log"could not be satisfied, only ${MDC(NUM_BYTES, granted)} bytes being available")
    }
  }

  /**
   * Publishes retained-output disk exhaustion as the existing MemoryPressure fallback condition.
   *
   * The fallback contract has exactly four reasons and local disk exhaustion is not a fifth one.
   * It is resource pressure preventing a buffer from being retained, so it deliberately sets the
   * same sticky memory-pressure signal the policy already maps to `MemoryPressure`.
   */
  private def recordDiskPressure(requested: Long, reason: String): Unit = {
    diskPressure.set(true)
    memoryPressure.set(true)
    memoryPressureCount.incrementAndGet()
    diskPressureLogAggregator.record(clock.getTimeMillis(), requested) match {
      case Some(summary) =>
        logWarning(log"Streaming shuffle refused ${MDC(NUM_BYTES, requested)} retained-output " +
          log"disk byte(s): ${MDC(REASON, reason)}. Streaming will yield to sort-based shuffle " +
          log"before exhausting local storage " +
          log"(${MDC(COUNT, summary.occurrences)} disk-pressure refusals totalling " +
          log"${MDC(MEMORY_SIZE, summary.volumeBytes)} requested byte(s) on this executor, " +
          log"${MDC(NUM_SKIPPED, summary.unreported)} not reported individually)")
      case None =>
        if (debugEnabled) {
          logInfo(log"Streaming shuffle refused ${MDC(NUM_BYTES, requested)} retained-output " +
            log"disk byte(s): ${MDC(REASON, reason)}")
        }
    }
  }

  /**
   * Reports a refusal that no eviction can ever reverse.
   *
   * Reported at warning level exactly once and at debug level thereafter. A permanent refusal
   * repeats for every subsequent block of the offending partition, so logging each one at warning
   * would blow the contracted log-volume budget on a single misconfigured shuffle -- and the first
   * occurrence already carries every fact an operator needs.
   */
  private def recordPermanentRefusal(partitionId: Int, required: Long, reason: String): Unit = {
    val total = permanentRefusalCount.incrementAndGet()
    if (permanentRefusalLogged.compareAndSet(false, true)) {
      logWarning(log"Streaming shuffle refused a block for partition " +
        log"${MDC(PARTITION_ID, partitionId)} charging ${MDC(NUM_BYTES, required)} bytes and no " +
        log"eviction can make it admissible: ${MDC(REASON, reason)}. Further occurrences are " +
        log"logged only with the streaming shuffle debug key enabled")
    } else if (debugEnabled) {
      logInfo(log"Streaming shuffle refused a block for partition " +
        log"${MDC(PARTITION_ID, partitionId)} charging ${MDC(NUM_BYTES, required)} bytes: " +
        log"${MDC(REASON, reason)} (${MDC(COUNT, total)} such refusals so far)")
    }
  }

  /** Removes a spill file. A non-fatal failure is logged rather than escalated. */
  private def deleteSpillFile(file: File): Unit = {
    try {
      val deleted = Files.deleteIfExists(file.toPath)
      MemorySpillManager.releaseDiskQuota(file)
      if (!deleted && debugEnabled) {
        logInfo(log"Streaming shuffle spill file ${MDC(FILE_NAME, file.getName)} was already " +
          log"removed")
      }
    } catch {
      case NonFatal(e) =>
        spillFileDeletionFailureTotal.incrementAndGet()
        // Cleanup walks every retained spill file of every task, so one undeletable directory
        // produces one line per file per task. Bounding it executor-wide is what stops a cleanup
        // path from being noisier than the work it is cleaning up after, and the default-level
        // line names the file rather than its absolute path so the executor's storage layout stays
        // out of ordinary logs.
        spillFileDeletionLogAggregator.record(clock.getTimeMillis()) match {
          case Some(summary) =>
            logWarning(log"Failed to delete streaming shuffle spill file " +
              log"${MDC(FILE_NAME, file.getName)}: ${MDC(CLASS_NAME, e.getClass.getName)} " +
              log"(${MDC(COUNT, summary.occurrences)} deletion failures on this executor so far, " +
              log"${MDC(NUM_SKIPPED, summary.unreported)} not reported individually)")
          case None =>
            if (debugEnabled) {
              logWarning(log"Failed to delete streaming shuffle spill file " +
                log"${MDC(PATH, file.getAbsolutePath)}", e)
            }
        }
    }
  }

  // Joined to the executor's shared threshold ticker before cleanup is registered, and after every
  // field above is initialised, because the ticker thread may call `pollOnce` the instant this
  // returns. Withheld when `autoPoll` is disabled, which is how a test keeps the cadence its own.
  if (autoPoll) {
    MemorySpillManager.registerForPolling(this)
  }

  // Registered last, and deliberately so. `addTaskCompletionListener` invokes its listener
  // immediately when the task has already completed, so this has to be the final statement of the
  // constructor body: by the time the listener can possibly run, every field above is initialised.
  // A consumer built outside a task registers explicitly through `registerCleanup` instead.
  Option(TaskContext.get()).foreach(registerCleanup)
}

/**
 * Constants, the executor-scoped shared budget, the shared threshold ticker and the durable record
 * types of the streaming shuffle spill manager.
 */
private[spark] object MemorySpillManager extends Logging {

  /** Divisor and multiplier for every percentage in this component. */
  val PERCENT_SCALE: Long = 100L

  /**
   * `value * percent / PERCENT_SCALE`, computed exactly and without overflow.
   *
   * The feature specifies the buffer allowance as `(executorMemory * bufferPercent) /
   * numPartitions`, with the percentage read as a hundredth, so the aggregate this returns must be
   * the product divided by a hundred and not the quotient multiplied by the percentage. The two
   * differ: dividing first discards `value % 100` before the multiplication and so understates the
   * allowance by up to `percent - 1` bytes, which is small in absolute terms but is a systematic
   * bias rather than a rounding, and it makes every allowance derived from a heap that is not a
   * clean multiple of a hundred slightly smaller than the one the operator configured.
   *
   * Computing the product directly is not an option either -- a heap size multiplied by a
   * percentage can exceed `Long.MaxValue` for an implausibly large but representable input, and a
   * wrapped product would collapse the allowance to a negative number, which reads as permanent
   * memory pressure. So the product is split: the whole hundreds are divided first, where the
   * division is exact and cannot lose anything, and the remainder -- strictly less than a hundred
   * -- is multiplied and divided on its own, where `99 * 100` cannot overflow anything. The sum is
   * the exact value of `value * percent / 100` for every non-negative input.
   *
   * @param value the quantity to take a percentage of; must be non-negative
   * @param percent the percentage to take, as a whole number of hundredths; must be non-negative
   * @return the exact percentage, truncated towards zero exactly once at the end
   */
  def percentageOf(value: Long, percent: Int): Long = {
    require(value >= 0L, s"value must be non-negative, but was $value")
    require(percent >= 0, s"percent must be non-negative, but was $percent")
    val wholeHundreds = value / PERCENT_SCALE * percent.toLong
    val remainder = (value % PERCENT_SCALE) * percent.toLong / PERCENT_SCALE
    wholeHundreds + remainder
  }

  /** Multiplies two non-negative byte counts, saturating instead of wrapping. */
  def saturatingMultiply(left: Long, right: Long): Long = {
    if (left <= 0L || right <= 0L) {
      0L
    } else if (left > Long.MaxValue / right) {
      Long.MaxValue
    } else {
      left * right
    }
  }

  /**
   * Sentinel meaning that no reduce partition count has been registered yet. Distinct from one, so
   * that a legitimate single-partition registration is told apart from the degenerate default.
   */
  val UNREGISTERED_PARTITION_COUNT: Int = 0

  /**
   * Sentinel meaning that a partition has admitted no block yet, so its next admission establishes
   * the origin of its sequence run. Deliberately not `-1`, which is a value a caller could
   * otherwise conflate with a real predecessor of sequence zero.
   */
  val UNSET_SEQUENCE: Long = Long.MinValue

  /**
   * Attempts one admission makes before answering no: one against the budget as it stands, and one
   * more after evicting. A third would not help -- the second attempt already ran after the
   * eviction the first provoked -- and would turn a refusal into an unbounded retry loop on the hot
   * path.
   */
  val MAX_ADMISSION_ATTEMPTS: Int = 2

  /**
   * Eviction passes one reclamation request makes before it accepts that nothing can be freed.
   *
   * More than one is needed because exactly one eviction runs at a time and the threshold poller
   * runs on its own thread: a request arriving mid-reclamation plans nothing, and believing that
   * first empty plan would turn a reclamation in progress into a refused reservation. Two is enough
   * -- the second pass runs after the first has waited the in-flight eviction out -- and a bound is
   * what keeps a device that fails every write from being retried forever.
   */
  val MAX_EVICTION_PASSES: Int = 2

  /**
   * Passes the end-of-stream durability flush makes before it reports what is still resident.
   *
   * Each pass re-reads the live tally, so a flush converges past both a reclamation that was
   * already in flight and an acknowledgement that retired blocks between the tally being read and
   * the plan
   * being built. Four passes covers those interleavings with room to spare while still terminating.
   */
  val MAX_DURABILITY_FLUSH_PASSES: Int = 4

  /**
   * One wait slice, in milliseconds, spent waiting for an eviction in flight to publish.
   *
   * Short, because the wait ends on a notification in the ordinary case and the slice only bounds
   * how long a lost notification could cost.
   */
  val EVICTION_QUIESCENCE_SLICE_MS: Long = 10L

  /**
   * Slices a caller waits for an eviction in flight before giving up on it.
   *
   * Counted rather than timed so the wait terminates under an injected clock that does not advance.
   * Fifty slices of ten milliseconds bounds the wait at half a second, which is far longer than a
   * spill write of the sizes this budget admits and far shorter than any task timeout.
   */
  val EVICTION_QUIESCENCE_SLICES: Int = 50

  /**
   * Hard bound on the blocks one partition may retain in memory, mid-eviction and on disk together.
   *
   * The per-block charge already bounds how much *payload* a partition can hold, but a spilled
   * block's retained record is heap this class does not charge for, so a stream of small blocks
   * that spilled and went unacknowledged would grow that metadata without limit. Reaching the bound
   * raises memory pressure, which routes the shuffle to sort-based fallback rather than stalling
   * it.
   */
  val MAX_RETAINED_BLOCKS_PER_PARTITION: Int = 4096

  /**
   * Transfer buffer used when a spilled block is decoded back into its payload.
   *
   * Sixty-four kibibytes is the usual compromise for a streaming copy: large enough that a two
   * mebibyte block costs a few dozen reads rather than thousands, small enough that the buffer
   * itself is never a meaningful allocation. It is not configurable, because it is a copy buffer
   * rather than a budget: nothing about a job's behaviour depends on its size.
   */
  val DEFAULT_REPLAY_BUFFER_BYTES: Int = 64 * 1024

  /**
   * The largest reduce partition count this consumer will track, and so the exclusive upper bound
   * on the partition ids it will admit while no count has been registered. Spark's own shuffle
   * partition ceiling is of this order, and the bound exists to keep per-partition metadata
   * proportional to a real shuffle rather than to whatever a caller names.
   */
  val MAX_TRACKED_PARTITIONS: Int = 1 << 20

  /**
   * Most logical consumers one retained producer store may hold cursors for at once.
   *
   * This matches the producer handler's live-session ceiling. A reconnect with the same identity is
   * idempotent and consumes no additional entry; identity churn beyond the ceiling is refused
   * before it can pin another reclamation cursor.
   */
  val MAX_REGISTERED_CONSUMERS_PER_STORE: Int = 4096

  /**
   * Most distinct logical consumer identities admitted across the executor.
   *
   * The quota is shared across every producer store and reference-counted, so the same reduce task
   * may register with many map outputs while a peer cannot manufacture an unbounded succession of
   * identities across them. Twice the per-store ceiling leaves room for reconnect overlap and for
   * concurrent shuffles without turning identity churn into permanent executor state.
   */
  val MAX_REGISTERED_CONSUMER_IDENTITIES: Int =
    2 * MAX_REGISTERED_CONSUMERS_PER_STORE

  /**
   * The largest number of retained spill records one consumer may accumulate across every
   * partition.
   *
   * [[MAX_RETAINED_BLOCKS_PER_PARTITION]] bounds one partition; this bounds their sum, which is
   * what a producer streaming many partitions to a consumer that has stopped acknowledging would
   * otherwise grow without limit. At the 2 MiB block cap it stands for 512 GiB of spilled payload,
   * so it constrains only a stream that has already stopped making progress.
   */
  val MAX_RETAINED_SPILL_RECORDS_TOTAL: Int = 65536

  /** Most committed spill files the streaming subsystem may retain on one executor. */
  val MAX_RETAINED_SPILL_FILES: Int = 4096

  /** Most independently committed block segments written into one spill file. */
  val MAX_SPILL_SEGMENTS_PER_FILE: Int = 64

  /** Largest raw payload volume grouped into one spill file. */
  val MAX_SPILL_FILE_PAYLOAD_BYTES: Long = 128L * 1024L * 1024L

  /**
   * Conservative expansion factor reserved before compression and encryption write a spill file.
   */
  val DISK_RESERVATION_MULTIPLIER: Long = 2L

  /** Fixed per-file headroom included in every pre-write disk reservation. */
  val DISK_RESERVATION_OVERHEAD_BYTES: Long = 64L * 1024L

  /** Disk allowance as a multiple of the configured aggregate streaming heap allowance. */
  val DISK_TO_MEMORY_QUOTA_MULTIPLIER: Long = 8L

  /** Absolute ceiling on streaming-retained disk per executor. */
  val MAX_EXECUTOR_DISK_QUOTA_BYTES: Long = 8L * 1024L * 1024L * 1024L

  /** Share of currently usable local-disk space streaming may reserve. */
  val LOCAL_DISK_QUOTA_PERCENT: Int = 10

  /** Free space left untouched on a selected local directory before a spill write begins. */
  val MIN_LOCAL_DISK_HEADROOM_BYTES: Long = 64L * 1024L * 1024L

  /** Heap charged for one durable spill record and its collection slot. */
  val SPILLED_RECORD_METADATA_BYTES: Long = 128L

  /** Name of the single daemon thread that drives the threshold cadence for the whole executor. */
  val POLLER_THREAD_NAME: String = "streaming-shuffle-spill-poller"

  /** Name of the one executor-scoped worker that drains bounded reclamation batches. */
  val RECLAIMER_THREAD_NAME: String = "streaming-shuffle-reclaimer"

  /**
   * The bound, in milliseconds, within which the executor's threshold ticker must be gone once
   * [[shutdownExecutorPoller]] has asked it to stop.
   *
   * Generous against the work involved -- a round is a comparison per registered instance, and the
   * registry is emptied before the request -- so reaching this bound means a round is genuinely
   * wedged rather than merely in flight, which is worth a warning. Short enough that an executor
   * shutting down is not held up by it.
   */
  val POLLER_SHUTDOWN_TIMEOUT_MS: Long = 5000L

  /**
   * Cadence of the buffer-utilisation check, in milliseconds. [[MemorySpillManager.pollOnce]]
   * suppresses checks that arrive sooner than this.
   */
  val POLL_INTERVAL_MS: Long = 100L

  /** Blocks one synchronous or asynchronous reclamation batch may retire under the store lock. */
  val MAX_RECLAIMED_BLOCKS_PER_BATCH: Int = 256

  /** Delay between executor-scoped reclamation dispatch rounds. */
  val RECLAMATION_DISPATCH_INTERVAL_MS: Long = 1L

  /** Managers one dispatch round services before yielding to the scheduler. */
  val MAX_RECLAMATION_MANAGERS_PER_ROUND: Int = 64

  /**
   * The bound, in milliseconds, within which buffer reclamation must complete after a consumer
   * acknowledgement. Per-ack work is capped and any remaining prefix is dispatched to the bounded
   * executor-scoped reclaimer, so the event-loop-facing call is never proportional to the retained
   * window.
   */
  val RECLAMATION_DEADLINE_MS: Long = 100L

  /**
   * Width, in milliseconds, of the window within which one recurring condition is reported at
   * default level at most once.
   *
   * A minute bounds each condition at sixty records an hour however often it recurs, which is what
   * keeps a producer that is spilling continuously, or writing to a failing disk, from choosing the
   * executor's log volume. It is deliberately not configurable: it bounds diagnostics rather than
   * behaviour, and an operator who needs per-event granularity has the streaming debug key.
   */
  val LOG_AGGREGATION_WINDOW_MS: Long = 60000L

  /**
   * Rate gate for one recurring, default-level log record.
   *
   * Every condition this class guards is one that recurs as often as the data path runs -- an
   * eviction, a spill failure, a reclamation overrun, an undeletable spill file -- so reporting
   * each occurrence would make the log volume a function of throughput. A gate admits the first
   * occurrence immediately and then at most one per window, and it hands the admitted report the
   * number of occurrences it stands in for, so the aggregate record loses volume information about
   * nothing: only per-event granularity, which the streaming debug key restores.
   *
   * Non-blocking and driven by the owner's clock, so it is safe to consult from any thread and its
   * bound advances with that clock rather than with wall time.
   *
   * @param windowMs width of the reporting window in milliseconds
   */
  class LogAggregationGate(windowMs: Long) {

    /** Earliest time a report may be made. Starts in the past so the first call reports. */
    private val nextReportTimeMs = new AtomicLong(Long.MinValue)

    /** Occurrences since the last report, which the next report accounts for and then clears. */
    private val unreportedOccurrences = new AtomicLong(0L)

    /**
     * Decides whether the caller should report this occurrence at default level.
     *
     * @param nowMs the current time, from the owner's clock
     * @return the number of occurrences that went unreported since the last report, when the
     *         caller should report; `None` when the caller should stay silent or fall back to a
     *         debug-level record
     */
    def admit(nowMs: Long): Option[Long] = {
      val due = nextReportTimeMs.get()
      if (nowMs >= due && nextReportTimeMs.compareAndSet(due, nextWindowEnd(nowMs))) {
        Some(unreportedOccurrences.getAndSet(0L))
      } else {
        unreportedOccurrences.incrementAndGet()
        None
      }
    }

    /** How many occurrences are currently unreported, for diagnostics and assertions. */
    def unreportedCount: Long = unreportedOccurrences.get()

    /**
     * Returns the gate to its initial state, so the next call reports and carries no occurrence
     * over.
     *
     * Nothing in service calls this, and the reason is the same reason the window is not
     * configurable: the bound belongs to the executor's lifetime, and resetting it mid-service
     * would restore exactly the unbounded output the gate exists to prevent. It exists so that one
     * test suite cannot inherit another's open window, which for an executor-scoped gate is the
     * difference between an assertion and a coin toss.
     */
    private[streaming] def reset(): Unit = {
      nextReportTimeMs.set(Long.MinValue)
      unreportedOccurrences.set(0L)
    }

    /**
     * End of the window opening at `nowMs`, saturating instead of wrapping. A wrapped deadline
     * would land in the past and disable the gate outright, so it is ruled out arithmetically.
     */
    private def nextWindowEnd(nowMs: Long): Long = {
      val end = nowMs + windowMs
      if (end < nowMs) Long.MaxValue else end
    }
  }

  /**
   * What an admitted executor-scoped record has to say about the occurrences it stands in for.
   *
   * @param occurrences how many times the condition has occurred on this executor, this one
   *                    included
   * @param volumeBytes cumulative bytes those occurrences moved, where the condition has a volume;
   *                    zero for conditions that do not
   * @param unreported how many occurrences went unreported since the last admitted record
   */
  private[streaming] case class ExecutorLogSummary(
      occurrences: Long,
      volumeBytes: Long,
      unreported: Long)

  /**
   * An executor-scoped aggregator for one recurring, default-level log record: a
   * [[LogAggregationGate]] paired with the cumulative totals the admitted record quotes.
   *
   * '''Why the scope has to be the executor and not the instance.''' The log-volume budget this
   * subsystem is held to -- under 10 MB per hour per executor with debug logging off -- is measured
   * per executor, but one [[MemorySpillManager]] exists per streaming map task and one
   * [[StreamingShuffleWriter]] per attempt. A gate owned by an instance admits its own first
   * occurrence immediately, whatever the executor has already reported, so a stage of a thousand
   * map tasks emits a thousand default-level records for a condition whose bound is one a minute.
   * The gate was never wrong; its scope was. Hoisting both the gate and the totals it quotes here
   * makes the bound what it claims to be -- at most one default-level record per condition per
   * window per executor, whatever the task count -- and the occurrences every silent task
   * contributed are carried in the count the next admitted record reports, so no volume information
   * is lost, only per-event granularity, which the streaming debug key restores.
   *
   * '''Why the totals move here too.''' A record standing in for occurrences from several tasks
   * cannot honestly quote the running totals of whichever task happened to win the gate: an
   * operator would read "3 evictions so far" beside "900 not reported individually". Accumulating
   * the totals alongside the gate makes the quoted figures describe the same population as the
   * gate does.
   *
   * Non-blocking and driven by the caller's clock, so it is safe to consult from a task thread, a
   * network event-loop thread or the executor's shared ticker alike.
   *
   * @param windowMs width of the reporting window in milliseconds
   */
  private[streaming] class ExecutorLogAggregator(windowMs: Long) {

    private val gate = new LogAggregationGate(windowMs)

    private val occurrences = new AtomicLong(0L)

    private val volume = new AtomicLong(0L)

    /**
     * Records one occurrence on this executor and decides whether the caller should report it at
     * default level.
     *
     * @param nowMs the current time, from the caller's clock
     * @param volumeBytes bytes this occurrence moved, or zero for a condition without a volume
     * @return the executor-wide figures the caller should quote, when the caller should report;
     *         `None` when it should stay silent or fall back to a debug-level record
     */
    def record(nowMs: Long, volumeBytes: Long = 0L): Option[ExecutorLogSummary] = {
      val totalOccurrences = occurrences.incrementAndGet()
      val totalVolume =
        if (volumeBytes > 0L) volume.addAndGet(volumeBytes) else volume.get()
      gate.admit(nowMs).map { unreported =>
        ExecutorLogSummary(totalOccurrences, totalVolume, unreported)
      }
    }

    /** How many times this condition has occurred on this executor, for assertions. */
    def occurrenceCount: Long = occurrences.get()

    /** Cumulative bytes those occurrences moved, for assertions. */
    def volumeBytes: Long = volume.get()

    /** How many occurrences are currently unreported, for assertions. */
    def unreportedCount: Long = gate.unreportedCount

    /** Returns the aggregator to its initial state. See [[LogAggregationGate.reset]]. */
    private[streaming] def reset(): Unit = {
      gate.reset()
      occurrences.set(0L)
      volume.set(0L)
    }
  }

  /**
   * The largest block payload this manager will admit, in bytes.
   *
   * The bound is on payload bytes, not on the encoded frame: a frame carrying a maximum-sized
   * payload is larger by the protocol's framing overhead, which
   * `DataBlockMessage.MAX_ENCODED_FRAME_BYTES` states. Read from the wire protocol's own data block
   * message rather than restated, so the bound this manager enforces and the bound a receiver is
   * entitled to reject against are one value. Payloads are capped so that a stream pipelines rather
   * than arriving in a few large stalls, and the cap is checked on admission as well as on the wire
   * so that a mis-framed block is caught at its source.
   */
  val MAX_BLOCK_PAYLOAD_BYTES: Int = DataBlockMessage.MAX_BLOCK_SIZE_BYTES

  /**
   * Conservative JVM cost, in bytes, of retaining one block in a partition's queue: the payload
   * array's own object header and length field, the [[MemorySpillManager.BufferedBlock]] wrapper,
   * the queue slot that refers to it and the [[MemorySpillManager.SpilledBlock]] record it may
   * later become. Rounded up to a round number because the point is to stop a stream of tiny
   * blocks from escaping accounting, not to model the heap exactly.
   */
  val RETAINED_BLOCK_OVERHEAD_BYTES: Long = 128L

  /**
   * Bytes charged against the budget for every retained block over and above its payload.
   *
   * Payload bytes and retained bytes are not the same resource, and charging only the payload is
   * how a buffer grows while utilisation reads near zero. Two costs are folded in here. The first
   * is the protocol's own framing, [[DataBlockMessage.FRAMING_OVERHEAD_BYTES]], because the bytes
   * this manager holds are destined for the network and it is the framed size that the egress
   * limiter and the link both see. The second is [[RETAINED_BLOCK_OVERHEAD_BYTES]], the JVM cost of
   * retaining a block at all, which dominates the payload for a small block.
   *
   * Both are deliberately conservative: over-charging shrinks the effective buffer slightly,
   * whereas under-charging breaks the memory bound the subsystem exists to guarantee.
   */
  val PER_BLOCK_OVERHEAD_BYTES: Long =
    DataBlockMessage.FRAMING_OVERHEAD_BYTES.toLong + RETAINED_BLOCK_OVERHEAD_BYTES

  /**
   * One kind of heap charged to the executor-wide streaming allowance.
   *
   * The category does not create a second ceiling. It is a diagnostic and ownership boundary inside
   * the one ceiling, so a consumer cannot release producer bytes and a queue cannot release a
   * decoder's payload. The sum of all four categories is always
   * [[ExecutorBufferQuota.reservedBytes]].
   */
  private[streaming] sealed trait MemoryCharge

  /** Retained producer payloads and the fixed framing scratch that creates them. */
  private[streaming] case object ProducerMemory extends MemoryCharge

  /** Decoded payloads retained by consumer hand-off queues and readers. */
  private[streaming] case object ConsumerMemory extends MemoryCharge

  /** Temporary frame copies held from encoding until a channel write completes. */
  private[streaming] case object TransientMemory extends MemoryCharge

  /** Queue nodes, credit windows, retained spill records and other bounded ledgers. */
  private[streaming] case object MetadataMemory extends MemoryCharge

  /** Quota owner of each committed spill file, including test-local quota instances. */
  private val diskQuotaOwners = new ConcurrentHashMap[String, ExecutorDiskQuota]()

  /**
   * Largest number of bytes one block can occupy on the wire, framing included, taken from the
   * protocol. Published here so that a producer sizing its framing against this manager's bound and
   * a limiter sizing its burst against the same bound read one value.
   */
  val MAX_ENCODED_FRAME_BYTES: Int = DataBlockMessage.MAX_ENCODED_FRAME_BYTES

  // Executor-scoped shared budget. One instance per JVM, and therefore per executor, because the
  // contracted bound is a fraction of *executor* memory: an executor runs many tasks at once, so a
  // budget derived per instance would let `n` concurrent producers hold `n * bufferSizePercent` of
  // the heap between them and the bound would be a per-task bound wearing an executor-wide name.

  /**
   * The streaming buffer allowance every streaming shuffle participant on one executor reserves
   * from, in '''both''' directions.
   *
   * ==One allowance, not two==
   *
   * `spark.shuffle.streaming.bufferSizePercent` is a promise about one executor: no more than that
   * percentage of its memory will be held in streaming shuffle buffers. Producer-side framing and
   * buffered blocks and consumer-side received frames are all streaming shuffle buffers on the same
   * heap, so they are all charged '''here''', against one ceiling, through one compare-and-set.
   * Two independent allowances of the same percentage -- one per direction -- would have summed to
   * twice the configured percentage on any executor doing both at once, which is every executor in
   * a multi-stage job, and the promise would have been unenforceable in exactly the situation it
   * exists for. The two directions are tallied separately for observability only; the '''bound'''
   * is the single `reserved` cell below.
   *
   * ==The basis==
   *
   * The percentage is taken of the '''configured executor memory''' -- `spark.executor.memory` --
   * which is the basis the property is documented and specified against, and the same basis on both
   * sides of a shuffle. It is derived once, on first use, and held immutably thereafter, which is
   * what makes "configuration changes require an executor restart" true by construction. It is also
   * why this class needs nothing from a live `SparkEnv`: the figure is a configuration value, so
   * the utilisation gauge can be read from the metrics thread at any time.
   *
   * Reservation is one atomic compare-and-set against the ceiling, so two participants on different
   * tasks -- or on opposite sides of the same shuffle -- can never both observe room for the same
   * bytes. Refusal on the producer side is the memory-pressure condition the fallback policy
   * consumes; refusal on the consumer side is flow control the consumer repairs by asking for the
   * position again.
   *
   * @param bufferSizePercent percentage of configured executor memory the allowance occupies
   * @param spillThresholdPercent percentage of the allowance at which eviction is triggered
   * @param executorMemoryProvider supplies the configured executor memory in bytes; injected so the
   *                               allowance is a function of this argument rather than of whatever
   *                               heap the host JVM happens to have been given
   */
  class ExecutorBufferQuota(
      bufferSizePercent: Int,
      spillThresholdPercent: Int,
      executorMemoryProvider: () => Long)
    extends StreamingShuffleBufferUtilizationContributor {

    // The bound. Every byte held in a streaming shuffle buffer anywhere on this executor, in either
    // direction, is counted here and nowhere else.
    private val reserved = new AtomicLong(0L)

    private val refusals = new AtomicLong(0L)

    private val producerReserved = new AtomicLong(0L)

    private val consumerReserved = new AtomicLong(0L)

    private val transientReserved = new AtomicLong(0L)

    private val metadataReserved = new AtomicLong(0L)

    /**
     * Configured executor memory in bytes, floored at one byte so the utilisation arithmetic can
     * never divide by zero. Sampled exactly once, on first use.
     */
    lazy val executorMemoryBytes: Long = math.max(1L, executorMemoryProvider())

    /**
     * The aggregate allowance in bytes: exactly `bufferSizePercent` of configured executor memory,
     * as the feature specifies it. [[MemorySpillManager.percentageOf]] computes the product before
     * the division without ever forming a product that could overflow, so a memory size that is not
     * a clean multiple of a hundred yields the allowance the operator configured rather than one
     * systematically a few bytes short of it.
     *
     * There is no floor above that percentage, and deliberately so. Raising the allowance to the
     * size of one legal frame would hand a very small executor more than the percentage it
     * configured -- at the minimum percentage of a one-mebibyte executor, twice the executor's
     * entire memory -- which is the same "allowances summing past the configured percent" defect
     * this single aggregate quota exists to remove. An executor whose configured percentage cannot
     * admit one frame refuses the reservation, and that refusal stands streaming down under
     * [[StreamingShuffleFallbackReason.MemoryPressure]] to sort-based shuffle rather than quietly
     * exceeding the ceiling the operator set. The one-byte floor below is only so the utilisation
     * arithmetic cannot divide by zero; it is not a usable allowance.
     */
    lazy val totalBytes: Long =
      math.max(1L, percentageOf(executorMemoryBytes, bufferSizePercent))

    /** The utilisation level, in bytes, at which eviction is triggered. */
    lazy val spillTriggerBytes: Long =
      math.max(1L, percentageOf(totalBytes, spillThresholdPercent))

    /** Bytes currently reserved across every participant drawing on this allowance. */
    def reservedBytes: Long = reserved.get()

    /** Bytes not yet committed to any streaming allocation. */
    def availableBytes: Long = math.max(0L, totalBytes - reservedBytes)

    /** Reservations refused because the allowance was exhausted. */
    def refusalCount: Long = refusals.get()

    /** Bytes one ownership category currently holds inside the shared allowance. */
    def reservedBytes(charge: MemoryCharge): Long = counterFor(charge).get()

    /**
     * Bytes this allowance is currently lending out, as reported to the executor-wide
     * `shuffle.streaming.bufferUtilizationPercent` gauge.
     *
     * Both directions, because both are charged against [[contributedBudgetBytes]]: reporting only
     * one of them would put part of the numerator against the whole of the denominator and read low
     * by exactly the share it omitted. One relaxed atomic read, so the metrics reporting thread
     * never waits on a buffer monitor.
     */
    override def contributedBufferedBytes: Long = reservedBytes

    /**
     * The allowance this contribution is measured against. Answered from the memoised total, so
     * reading the gauge never forces the lazy derivation more than once.
     */
    override def contributedBudgetBytes: Long = totalBytes

    /**
     * Reserves `bytes` of producer-side buffer against the allowance, atomically and without
     * blocking.
     *
     * @param bytes the number of bytes to reserve; must be positive
     * @return true when the reservation was taken and the caller now owns those bytes; false when
     *         it would have exceeded the allowance, in which case nothing was reserved
     */
    def tryReserve(bytes: Long): Boolean = {
      tryReserve(bytes, ProducerMemory)
    }

    /**
     * Reserves `bytes` for one ownership category against the same aggregate allowance.
     *
     * The aggregate compare-and-set is performed first, so two categories racing can never both
     * claim the last bytes. The category counter is advanced only after that claim succeeds; no
     * caller can release the reservation before this method returns it.
     */
    def tryReserve(bytes: Long, charge: MemoryCharge): Boolean = {
      require(bytes > 0L, s"A quota reservation must be positive, but was $bytes")
      require(charge != null, "A quota reservation category must not be null")
      val ceiling = totalBytes
      var granted = false
      var settled = false
      while (!settled) {
        val current = reserved.get()
        if (bytes > ceiling || current > ceiling - bytes) {
          refusals.incrementAndGet()
          settled = true
        } else if (reserved.compareAndSet(current, current + bytes)) {
          counterFor(charge).addAndGet(bytes)
          granted = true
          settled = true
        }
      }
      granted
    }

    /**
     * Returns `bytes` of producer-side buffer to the allowance. Floored at zero rather than allowed
     * to go negative, so that a bookkeeping defect degrades to a conservatively smaller allowance
     * instead of silently manufacturing headroom the executor does not have.
     *
     * @param bytes the number of bytes to return; must not be negative
     */
    def release(bytes: Long): Unit = {
      release(bytes, ProducerMemory)
    }

    /**
     * Returns up to `bytes` owned by one category, never touching another category's
     * reservation.
     */
    def release(bytes: Long, charge: MemoryCharge): Unit = {
      require(bytes >= 0L, s"A quota release must not be negative, but was $bytes")
      require(charge != null, "A quota release category must not be null")
      val category = counterFor(charge)
      var released = 0L
      var categorySettled = bytes == 0L
      while (!categorySettled) {
        val current = category.get()
        released = math.min(current, bytes)
        categorySettled = category.compareAndSet(current, current - released)
      }
      var settled = released == 0L
      while (!settled) {
        val current = reserved.get()
        settled = reserved.compareAndSet(current, math.max(0L, current - released))
      }
    }

    /** Counter belonging to one ownership category. */
    private def counterFor(charge: MemoryCharge): AtomicLong = charge match {
      case ProducerMemory => producerReserved
      case ConsumerMemory => consumerReserved
      case TransientMemory => transientReserved
      case MetadataMemory => metadataReserved
    }
  }

  /**
   * Executor-wide disk byte and file allowance for retained streaming output.
   *
   * A reservation is taken before a temp file is allocated. It carries a conservative estimate
   * while the writer is open, then is committed to the file's actual length or cancelled. Committed
   * files remain charged until the component that really unlinks them calls [[release]], including
   * files whose ownership moved from a task to the executor-scoped block resolver.
   */
  class ExecutorDiskQuota(
      byteLimitProvider: () => Long,
      val fileLimit: Int = MAX_RETAINED_SPILL_FILES) {

    require(byteLimitProvider != null, "The disk-quota byte provider must not be null")
    require(fileLimit > 0, s"The disk-quota file limit must be positive, but was $fileLimit")

    private var bytesReserved = 0L
    private var filesReserved = 0
    private var refusals = 0L
    private val committedFiles = new mutable.HashMap[String, Long]()

    /** Maximum bytes retained by streaming output on this executor. */
    lazy val totalBytes: Long = math.max(
      DataBlockMessage.MAX_ENCODED_FRAME_BYTES.toLong, byteLimitProvider())

    /** Bytes currently held by pending writes and committed files. */
    def reservedBytes: Long = synchronized(bytesReserved)

    /** Pending and committed spill files currently counted against the file ceiling. */
    def reservedFileCount: Int = synchronized(filesReserved)

    /** Reservations refused by either the byte or file ceiling. */
    def refusalCount: Long = synchronized(refusals)

    /**
     * Takes room for one future file before any filesystem allocation occurs.
     *
     * @param estimatedBytes conservative upper bound for the write
     * @return a reservation the caller must commit or cancel, or `None` when either ceiling binds
     */
    def tryReserve(estimatedBytes: Long): Option[DiskReservation] = synchronized {
      require(estimatedBytes > 0L,
        s"A disk reservation must be positive, but was $estimatedBytes")
      val byteHeadroom = totalBytes - bytesReserved
      if (filesReserved >= fileLimit || estimatedBytes > byteHeadroom) {
        refusals += 1L
        None
      } else {
        bytesReserved += estimatedBytes
        filesReserved += 1
        Some(new DiskReservation(estimatedBytes))
      }
    }

    /**
     * Whether the selected local directory can accept this write while preserving emergency
     * headroom for Spark's other spill and shuffle paths.
     *
     * The directory check is separate from the free-space reading. `File.getUsableSpace` may report
     * the containing filesystem's capacity even when the selected path has disappeared or become a
     * regular file, and treating that as writable would open the spill writer only to fail after
     * allocation. Refusing before that point is what routes device loss through MemoryPressure.
     */
    def hasUsableSpace(file: File, requestedBytes: Long): Boolean = {
      require(file != null, "The spill file must not be null")
      require(requestedBytes >= 0L,
        s"Requested local-disk bytes must not be negative, but was $requestedBytes")
      val parent = file.getAbsoluteFile.getParentFile
      val usable = if (parent == null) 0L else parent.getUsableSpace
      parent != null && parent.isDirectory && parent.canWrite &&
        usable > MIN_LOCAL_DISK_HEADROOM_BYTES &&
        requestedBytes <= usable - MIN_LOCAL_DISK_HEADROOM_BYTES
    }

    /** Releases a committed file after it has actually been removed. Idempotent by file path. */
    def release(file: File): Long = synchronized {
      if (file == null) {
        0L
      } else {
        val key = fileKey(file)
        committedFiles.remove(key) match {
          case Some(bytes) =>
            bytesReserved = math.max(0L, bytesReserved - bytes)
            filesReserved = math.max(0, filesReserved - 1)
            diskQuotaOwners.remove(key, this)
            bytes
          case None =>
            0L
        }
      }
    }

    /** Whether this quota currently accounts for the given file. */
    def contains(file: File): Boolean =
      file != null && synchronized(committedFiles.contains(fileKey(file)))

    /** Clears all process-scoped state for an isolated test. */
    private[streaming] def resetForTesting(): Unit = synchronized {
      bytesReserved = 0L
      filesReserved = 0
      refusals = 0L
      committedFiles.keys.foreach(key => diskQuotaOwners.remove(key, this))
      committedFiles.clear()
    }

    /** One pending file reservation. */
    final class DiskReservation private[streaming] (val estimatedBytes: Long) {

      private[streaming] var active = true

      /** Reconciles the estimate to one committed file's actual length. */
      def commit(file: File, actualBytes: Long): Boolean =
        ExecutorDiskQuota.this.commit(this, file, actualBytes)

      /** Returns a reservation whose file was never committed. */
      def cancel(): Unit = ExecutorDiskQuota.this.cancel(this)
    }

    private def commit(
        reservation: DiskReservation,
        file: File,
        actualBytes: Long): Boolean = synchronized {
      require(reservation != null, "The disk reservation must not be null")
      require(file != null, "The committed spill file must not be null")
      require(actualBytes >= 0L,
        s"Committed spill bytes must not be negative, but was $actualBytes")
      if (!reservation.active) {
        false
      } else {
        val delta = actualBytes - reservation.estimatedBytes
        val key = fileKey(file)
        if (committedFiles.contains(key) ||
            (delta > 0L && delta > totalBytes - bytesReserved)) {
          cancelLocked(reservation)
          refusals += 1L
          false
        } else {
          bytesReserved += delta
          committedFiles.update(key, actualBytes)
          diskQuotaOwners.put(key, this)
          reservation.active = false
          true
        }
      }
    }

    private def cancel(reservation: DiskReservation): Unit = synchronized {
      require(reservation != null, "The disk reservation must not be null")
      cancelLocked(reservation)
    }

    private def cancelLocked(reservation: DiskReservation): Unit = {
      if (reservation.active) {
        bytesReserved = math.max(0L, bytesReserved - reservation.estimatedBytes)
        filesReserved = math.max(0, filesReserved - 1)
        reservation.active = false
      }
    }

    private def fileKey(file: File): String = file.getAbsoluteFile.toPath.normalize().toString
  }

  // The executor-scoped aggregators, one per recurring condition a spill manager reports. Each is
  // independent, so a flood of one condition never masks the first occurrence of another, and each
  // is driven by its caller's clock so the bound is deterministic under an injected clock. They are
  // deliberately `val`s on this object rather than fields of an instance: see
  // [[ExecutorLogAggregator]] for why instance scope cannot enforce a per-executor log budget.
  private[streaming] val spillLogAggregator = new ExecutorLogAggregator(LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val durabilityFlushLogAggregator =
    new ExecutorLogAggregator(LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val spillFailureLogAggregator =
    new ExecutorLogAggregator(LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val diskPressureLogAggregator =
    new ExecutorLogAggregator(LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val reclamationLogAggregator =
    new ExecutorLogAggregator(LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val spillFileDeletionLogAggregator =
    new ExecutorLogAggregator(LOG_AGGREGATION_WINDOW_MS)

  /** Every aggregator above, for the bulk reset the test seam performs. */
  private val logAggregators: Seq[ExecutorLogAggregator] = Seq(spillLogAggregator,
    durabilityFlushLogAggregator, spillFailureLogAggregator, diskPressureLogAggregator,
    reclamationLogAggregator, spillFileDeletionLogAggregator)

  // Guarded by this object's monitor. Held as `var` rather than as a lazy val because they must be
  // discardable -- see [[resetExecutorState]] -- and because the configuration they are derived
  // from is only available once an instance is constructed.
  private var sharedQuota: ExecutorBufferQuota = null

  private var sharedDiskQuota: ExecutorDiskQuota = null

  private var poller: ScheduledExecutorService = null

  private var reclaimer: ScheduledExecutorService = null

  private val pollTargets = ConcurrentHashMap.newKeySet[MemorySpillManager]()

  private val reclamationTargets = new ConcurrentLinkedQueue[MemorySpillManager]()

  /** Reference count per stable logical consumer identity, guarded by this object's monitor. */
  private val consumerIdentityRefCounts = new mutable.HashMap[String, Int]()

  /**
   * Claims one reference to a stable consumer identity against the executor-wide unique-id cap.
   *
   * Existing identities are always admitted and merely increment their reference count, which is
   * what allows one reduce task to read every map output without consuming one unique-id slot per
   * producer. A new identity is admitted only while the unique-id map remains below its cap.
   */
  private def acquireConsumerIdentity(consumerId: String): Boolean = synchronized {
    consumerIdentityRefCounts.get(consumerId) match {
      case Some(references) =>
        consumerIdentityRefCounts.update(consumerId, references + 1)
        true
      case None if consumerIdentityRefCounts.size < MAX_REGISTERED_CONSUMER_IDENTITIES =>
        consumerIdentityRefCounts.put(consumerId, 1)
        true
      case None =>
        false
    }
  }

  /** Releases one producer store's reference to a stable consumer identity. */
  private def releaseConsumerIdentity(consumerId: String): Unit = synchronized {
    consumerIdentityRefCounts.get(consumerId).foreach { references =>
      if (references <= 1) {
        consumerIdentityRefCounts.remove(consumerId)
      } else {
        consumerIdentityRefCounts.update(consumerId, references - 1)
      }
    }
  }

  /** Distinct consumer identities currently charged to the executor-wide cap. */
  private[streaming] def registeredConsumerIdentityCount: Int = synchronized {
    consumerIdentityRefCounts.size
  }

  /**
   * The executor's shared buffer allowance, created on first use from the given configuration.
   *
   * The one allowance every streaming shuffle participant on this executor draws on, producers and
   * consumers alike, so that the configured percentage bounds the executor rather than bounding
   * each direction separately and summing to twice itself. Every caller after the first receives
   * the allowance the first one created.
   *
   * @param conf the configuration to read the buffer percentage, the spill threshold and the
   *             executor memory from, exactly once per executor
   */
  def executorQuota(conf: SparkConf): ExecutorBufferQuota = synchronized {
    if (sharedQuota == null) {
      sharedQuota = new ExecutorBufferQuota(
        conf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT),
        conf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD),
        // No floor above the configured percentage. An allowance raised to the size of one legal
        // frame would grant a very small executor more than the percentage it configured -- on a
        // one-mebibyte executor at the minimum percentage it would grant twice the executor's whole
        // memory -- which is exactly the "two allowances summing past the configured percent"
        // defect this single aggregate quota exists to remove. An executor too small to admit one
        // frame therefore refuses the reservation, and that refusal takes the specified
        // MemoryPressure fallback to sort-based shuffle, which is how every other unsatisfiable
        // reservation already ends. Memory is bounded by configuration, without exception.
        () => configuredExecutorMemoryBytes(conf))
      // Counted by the utilisation gauge for as long as the allowance exists. Registering the
      // allowance rather than each instance is what makes the gauge executor-wide without double
      // counting a shared budget: the numerator and the denominator both come from the one object
      // that owns them.
      StreamingShuffleMetricsSource.registerBufferUtilizationContributor(sharedQuota)
    }
    sharedQuota
  }

  /**
   * The executor's shared retained-output disk allowance.
   *
   * The byte ceiling is the smallest of three independent bounds: eight aggregate heap allowances,
   * ten percent of the local directories' currently usable space, and eight gibibytes. The first
   * keeps disk proportional to the configured streaming footprint, the second leaves ninety percent
   * of local storage to Spark's ordinary shuffle and spill paths, and the third prevents a very
   * large executor from turning this optional subsystem into an unbounded disk tenant.
   */
  def executorDiskQuota(
      conf: SparkConf,
      memoryQuota: ExecutorBufferQuota): ExecutorDiskQuota = synchronized {
    require(conf != null, "The Spark configuration must not be null")
    require(memoryQuota != null, "The executor memory quota must not be null")
    if (sharedDiskQuota == null) {
      sharedDiskQuota = new ExecutorDiskQuota(() => {
        val memoryBound = math.min(
          MAX_EXECUTOR_DISK_QUOTA_BYTES,
          saturatingMultiply(memoryQuota.totalBytes, DISK_TO_MEMORY_QUOTA_MULTIPLIER))
        val localBound = percentageOf(localDiskUsableBytes(), LOCAL_DISK_QUOTA_PERCENT)
        val usableBound = if (localBound > 0L) localBound else memoryBound
        math.min(memoryBound, usableBound)
      })
    }
    sharedDiskQuota
  }

  /** Releases the quota ownership of a spill file after any component has actually unlinked it. */
  private[streaming] def releaseDiskQuota(file: File): Long = {
    if (file == null) {
      0L
    } else {
      val key = file.getAbsoluteFile.toPath.normalize().toString
      val owner = diskQuotaOwners.get(key)
      if (owner == null) 0L else owner.release(file)
    }
  }

  /**
   * The number of map tasks that can run at once in this JVM, floored at one.
   *
   * '''Why the streaming buffer arithmetic needs this.''' The buffer allowance is executor wide,
   * and a producer's framing scratch -- one serialization accumulator per active partition -- is
   * the half of that allowance which cannot be spilled. Dividing the allowance by the partition
   * count alone therefore bounds what *one* task holds and not what the executor holds in total:
   * `slots` tasks each framing to a share sized for a single task will between them claim `slots`
   * times that share, and at an entirely ordinary four task slots that is the whole allowance with
   * no room left for a single buffered block. Sizing the framing share per slot is what makes the
   * configured percentage a bound on the executor rather than on one task of it.
   *
   * '''Where the answer comes from, and why three sources are needed.''' Two configuration keys
   * carry it, and neither covers every master: `spark.executor.cores` is set when a profile
   * or an operator declares it, while a plain `local[n]` master encodes the count in the master
   * string and leaves that key at its default of one. The larger of the two is taken so that
   * whichever one is authoritative wins, and the result is divided by `spark.task.cpus`, which is
   * what actually converts cores into concurrent tasks.
   *
   * A coarse-grained executor can carry neither. Its true core count arrives as the `numCores`
   * *argument* to `SparkEnv.createExecutorEnv` -- passed on from the backend's `--cores` -- and is
   * handed to the memory manager as a plain constructor parameter that nothing exposes, so it is
   * unreadable from here; meanwhile `spark.executor.cores` stays at its default and the master is
   * not a local pattern, which between them read as a single task slot. Believing that is what
   * makes the framing share four times too large on such an executor, and four concurrent tasks
   * then claim the whole allowance -- the exact failure this arithmetic exists to prevent.
   *
   * So when the configuration is silent, the processor count is used instead. That is not a guess
   * invented here: it is precisely what Spark's own `MemoryManager` does with the identical unknown
   * when it sizes the default page -- `if (numCores > 0) numCores else availableProcessors` -- and
   * coarse-grained executor is normally given the cores of the machine it was placed on. The
   * substitution is applied only when neither key spoke, so a declared count is never overridden,
   * and over-estimating is the safe direction: a larger divisor reserves less framing scratch,
   * which costs block size rather than correctness, while under-estimating exhausts the allowance.
   *
   * Read once per component and held immutably, like every other configuration value in this
   * subsystem, so a configuration change takes effect on executor restart and nowhere else.
   *
   * @param conf the configuration to derive the count from
   */
  def executorTaskSlots(conf: SparkConf): Int = {
    val cpusPerTask = math.max(1, conf.get(CPUS_PER_TASK))
    val declaredExecutorCores =
      if (conf.contains(EXECUTOR_CORES.key)) math.max(0, conf.get(EXECUTOR_CORES)) else 0
    val localTaskThreads = math.max(0,
      SparkContext.numDriverCores(conf.get("spark.master", "local"), conf))
    val declaredCores = math.max(declaredExecutorCores, localTaskThreads)
    val cores =
      if (declaredCores > 0) declaredCores else Runtime.getRuntime.availableProcessors()
    math.max(1, math.max(1, cores) / cpusPerTask)
  }

  /**
   * The configured executor memory in bytes, which is the basis the buffer allowance is a
   * percentage of.
   *
   * Configuration rather than the live heap, deliberately. `spark.executor.memory` is the figure an
   * operator sizes the allowance against and the figure the documented formula
   * `(executorMemory * bufferSizePercent) / numPartitions` refers to, so reading it here makes the
   * allowance a function of what was asked for rather than of whatever heap a particular JVM -- a
   * test JVM, a driver running in local mode -- happens to have been given. It also means the
   * figure needs no live `SparkEnv`, so the utilisation gauge can be read from the metrics thread.
   *
   * Saturating multiplication, because the entry is a size in mebibytes and an absurd configured
   * value must degrade to an effectively unbounded basis rather than wrap negative and read as
   * permanent memory pressure.
   */
  private def configuredExecutorMemoryBytes(conf: SparkConf): Long = {
    val megabytes = math.max(1L, conf.get(EXECUTOR_MEMORY))
    if (megabytes > Long.MaxValue / (1024L * 1024L)) Long.MaxValue else megabytes * 1024L * 1024L
  }

  /** Usable bytes across the executor's configured local directories, with saturating addition. */
  private def localDiskUsableBytes(): Long = {
    val directories = SparkEnv.get.blockManager.diskBlockManager.localDirs
    directories.foldLeft(0L) { (total, directory) =>
      val usable = math.max(0L, directory.getUsableSpace)
      val sum = total + usable
      if (sum < 0L) Long.MaxValue else sum
    }
  }

  /**
   * Joins an instance to the executor's shared threshold ticker, starting the ticker on first use.
   *
   * One daemon thread serves the whole executor. A thread per instance would put one per concurrent
   * task on a machine that may run dozens, purely to evaluate a comparison every 100 ms.
   */
  private def registerForPolling(manager: MemorySpillManager): Unit = {
    pollTargets.add(manager)
    synchronized {
      if (poller == null) {
        poller = ThreadUtils.newDaemonSingleThreadScheduledExecutor(POLLER_THREAD_NAME)
        poller.scheduleWithFixedDelay(new Runnable {
          override def run(): Unit = pollRegisteredTargets()
        }, POLL_INTERVAL_MS, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS)
      }
    }
  }

  /** Removes an instance from the shared ticker. Idempotent. */
  private def deregisterFromPolling(manager: MemorySpillManager): Unit = {
    pollTargets.remove(manager)
  }

  /**
   * Queues one manager for a bounded reclamation batch and starts the shared worker on first use.
   *
   * Each manager guards its own queueing with an atomic flag, so this executor-wide queue contains
   * at most one dispatch entry per manager however many acknowledgements arrive before it runs.
   */
  private def enqueueForReclamation(manager: MemorySpillManager): Unit = {
    reclamationTargets.add(manager)
    synchronized {
      if (reclaimer == null) {
        reclaimer = ThreadUtils.newDaemonSingleThreadScheduledExecutor(RECLAIMER_THREAD_NAME)
        reclaimer.scheduleWithFixedDelay(new Runnable {
          override def run(): Unit = dispatchReclamation()
        }, 0L, RECLAMATION_DISPATCH_INTERVAL_MS, TimeUnit.MILLISECONDS)
      }
    }
  }

  /** Drains a bounded number of manager batches before yielding the shared worker thread. */
  private def dispatchReclamation(): Unit = {
    var dispatched = 0
    var manager = reclamationTargets.poll()
    while (manager != null && dispatched < MAX_RECLAMATION_MANAGERS_PER_ROUND) {
      try {
        manager.runScheduledReclamationBatch()
      } catch {
        case NonFatal(e) =>
          logWarning(log"Streaming shuffle asynchronous reclamation failed for one producer", e)
      }
      dispatched += 1
      manager = reclamationTargets.poll()
    }
  }

  /**
   * Stops the executor's shared threshold ticker and reclamation worker, then waits within a
   * bounded deadline for both threads to be gone.
   *
   * <b>Why this exists and why it is the manager that calls it.</b> The ticker is created lazily by
   * the first instance that needs polling and is deliberately executor-scoped, because the budget
   * it evaluates is shared across every concurrent map task -- so no individual instance may shut
   * it down, and none does: a closing instance only removes itself from [[pollTargets]]. That
   * leaves exactly one owner able to end it, the component whose lifetime the executor's streaming
   * subsystem has: [[StreamingShuffleManager]], from its `stop`. Without this call the thread named
   * [[POLLER_THREAD_NAME]] outlives every context that ever streamed a shuffle, which in a
   * long-lived JVM that creates and stops contexts -- a test suite, a notebook kernel, a session
   * server -- accumulates one thread per context.
   *
   * The registry is emptied before the executor is shut down so that a round already in flight
   * finds nothing to poll rather than touching an instance whose environment is being torn down.
   * Termination is then awaited, because `shutdownNow` is a request: returning before the thread is
   * gone would let a caller report a clean shutdown while the thread it asked to stop is still
   * running, which is precisely the leak this method exists to close. The wait is bounded, and a
   * straggler is reported rather than thrown -- this runs during shutdown, where an exception would
   * abandon the rest of an orderly release over a thread the JVM is about to reclaim.
   *
   * Idempotent, and safe to call on an executor that never polled anything: with no ticker created
   * there is nothing to stop and nothing to wait for.
   *
   * @return true when neither worker is running by the time this returns
   */
  def shutdownExecutorPoller(): Boolean = {
    val running = synchronized {
      val currentPoller = poller
      val currentReclaimer = reclaimer
      poller = null
      reclaimer = null
      Seq(
        (currentPoller, POLLER_THREAD_NAME),
        (currentReclaimer, RECLAIMER_THREAD_NAME))
    }
    pollTargets.clear()
    reclamationTargets.clear()
    running.forall { case (executor, threadName) =>
      executor == null || shutdownWorker(executor, threadName)
    }
  }

  /** Stops and awaits one executor-scoped daemon worker. */
  private def shutdownWorker(executor: ScheduledExecutorService, threadName: String): Boolean = {
    executor.shutdownNow()
    val terminated = try {
      executor.awaitTermination(POLLER_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        executor.isTerminated
    }
    if (!terminated) {
      logWarning(log"The streaming shuffle worker ${MDC(THREAD_NAME, threadName)} did not " +
        log"terminate within ${MDC(TIMEOUT, POLLER_SHUTDOWN_TIMEOUT_MS)} ms of being asked to stop")
    }
    terminated
  }

  /**
   * Drives one cadence tick across every registered instance.
   *
   * A single instance must never be able to stop the executor's ticker: a closed instance is
   * dropped quietly, and one that throws -- because its environment has gone away, say -- is
   * dropped with a warning, leaving every other producer on the executor still polled.
   */
  private def pollRegisteredTargets(): Unit = {
    val iterator = pollTargets.iterator()
    while (iterator.hasNext) {
      val manager = iterator.next()
      try {
        if (manager.isClosed) {
          pollTargets.remove(manager)
        } else {
          manager.pollOnce()
        }
      } catch {
        case NonFatal(e) =>
          pollTargets.remove(manager)
          logWarning(log"Streaming shuffle buffer threshold poll failed and the buffer was " +
            log"withdrawn from the executor's poller; its owner may still poll it directly", e)
      }
    }
  }

  /**
   * Discards the executor-scoped allowance, empties the ticker's registry and returns every
   * executor-scoped log aggregator to its initial state, so the next instance derives a fresh
   * budget and reports its first occurrence of each condition.
   *
   * Nothing on an executor calls this: there the allowance is meant to outlive every individual
   * task, and discarding it under a live producer would unaccount memory that is still held; and
   * the log bound is a property of the executor's lifetime, so resetting it per task would restore
   * exactly the unbounded output the aggregation removes. In a suite, by contrast, executor-scoped
   * state that survives from one test into the next is the order dependence the zero-flakiness gate
   * exists to prevent.
   */
  private[streaming] def resetSharedStateForTesting(): Unit = synchronized {
    if (sharedQuota != null) {
      StreamingShuffleMetricsSource.unregisterBufferUtilizationContributor(sharedQuota)
    }
    sharedQuota = null
    if (sharedDiskQuota != null) {
      sharedDiskQuota.resetForTesting()
    }
    sharedDiskQuota = null
    diskQuotaOwners.clear()
    pollTargets.clear()
    reclamationTargets.clear()
    consumerIdentityRefCounts.clear()
    logAggregators.foreach(aggregator => aggregator.reset())
    StreamingShuffleWriter.resetLogAggregationForTesting()
    StreamingShuffleServerHandler.resetLogAggregationForTesting()
    StreamingShuffleClientHandler.resetLogAggregationForTesting()
  }

  // Internal admission and eviction outcomes. Modelled as types rather than as booleans so that the
  // several distinguishable ways an admission can fail -- transiently, permanently, because the
  // instance closed, or because the caller broke the sequence contract -- cannot be conflated.

  /** Outcome of one attempt to reserve room for a block. */
  private sealed trait AdmissionOutcome

  /**
   * Room was taken. The previous sequence watermark is carried so that a rollback can restore
   * exactly what the reservation advanced, rather than guessing at it.
   */
  private case class AdmissionReserved(previousSequence: Long) extends AdmissionOutcome

  /** The budget is full right now; eviction may make room. */
  private case object AdmissionNeedsRoom extends AdmissionOutcome

  /** No eviction can ever make this block admissible; the caller must fall back. */
  private case class AdmissionRefused(reason: String) extends AdmissionOutcome

  /** The instance is closed and admits nothing further. */
  private case object AdmissionClosed extends AdmissionOutcome

  /** The block breaks the partition's gap-free ascending sequence run. */
  private case class AdmissionOutOfSequence(expected: Long) extends AdmissionOutcome

  /**
   * What one eviction actually achieved, once every write either published or was rolled back.
   *
   * @param freedBytes bytes that left memory, and therefore must be returned to both budgets
   * @param committedDiskBytes bytes committed to local disk
   * @param evictedPartitions partitions successfully written out
   * @param filesToDelete spill files to unlink, collected here so the unlink happens outside the
   *                      buffer monitor
   */
  private case class EvictionResult(
      freedBytes: Long,
      committedDiskBytes: Long,
      evictedPartitions: Int,
      filesToDelete: Seq[File])

  /**
   * A framed block held in memory, awaiting consumer acknowledgement. Membership of this set is the
   * in-memory portion of the retransmission window: a block recorded here can be replayed from
   * memory, and one that has been evicted can be replayed from its spill record instead. Only a
   * block that is in neither requires the upstream stage to be recomputed.
   *
   * The payload held here is the manager's own copy, taken on admission, so a later write by the
   * producer to the array it handed over cannot change what is checksummed, retransmitted or
   * spilled.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param sequenceNumber the block's position in that partition's stream
   * @param data the framed block payload
   */
  case class BufferedBlock(partitionId: Int, sequenceNumber: Long, data: Array[Byte]) {

    /** Payload length in bytes, which is what goes on the wire as the block's body. */
    def length: Int = data.length

    /**
     * Bytes this block occupies against the buffer budget: its payload plus
     * [[PER_BLOCK_OVERHEAD_BYTES]]. This, and never [[length]], is the figure every accounting path
     * reserves, releases and reports, so admission, eviction, acknowledgement and closure can never
     * disagree about how much a block cost.
     */
    def chargeBytes: Long = data.length.toLong + PER_BLOCK_OVERHEAD_BYTES
  }

  /**
   * A framed block that was evicted to local disk and is still awaiting acknowledgement.
   *
   * The four location fields are precisely what a block resolver needs to serve or retransmit a
   * single spilled block, and they are stable for as long as the record is retained. The bytes at
   * that location were written through the block manager's disk writer, so they are wrapped by
   * `SerializerManager` exactly as every other Spark spill file is, and a reader must unwrap them
   * the same way; each block was committed on its own, so each segment is independently decodable.
   *
   * <b>Two lengths, and they are not the same number.</b> `length` measures the segment on disk,
   * which with `spark.shuffle.compress` on -- its default -- is the compressed size and carries no
   * usable relation to the payload. `payloadLength` is the size of the block as it goes on the
   * wire, recorded from the block itself at the instant it was written rather than derived from
   * the file afterwards. Keeping it is what lets a producer answer "how large is this block" while
   * pacing a replay without reading and decompressing the segment to find out -- a read that would
   * cost a whole disk round trip and a two mebibyte allocation per queued block, for a number it
   * already knew when it spilled it.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param sequenceNumber the block's position in that partition's stream
   * @param blockId the temporary shuffle block that backs the file
   * @param file the file the segment lives in
   * @param offset the segment's start offset within that file
   * @param length the segment's length in bytes, as committed to disk
   * @param payloadLength the block's payload size in bytes, as it goes on the wire
   */
  case class SpilledBlock(
      partitionId: Int,
      sequenceNumber: Long,
      blockId: TempShuffleBlockId,
      file: File,
      offset: Long,
      length: Long,
      payloadLength: Int)
}
