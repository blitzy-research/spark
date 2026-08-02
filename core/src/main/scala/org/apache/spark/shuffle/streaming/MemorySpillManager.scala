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
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{BLOCK_ID, BYTE_SIZE, CLASS_NAME, COUNT, DURATION,
  FILE_NAME, MAX_SIZE, MEMORY_SIZE, NUM_BYTES, NUM_PARTITIONS, NUM_SKIPPED, PARTITION_ID, PATH,
  REASON, THRESHOLD}
import org.apache.spark.internal.config.{SHUFFLE_FILE_BUFFER_SIZE,
  SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_DEBUG,
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
 * only block whose bytes may be discarded. That window is what makes retransmission possible, and
 * it is also what makes a hard memory bound mandatory. This class is that bound: it admits blocks
 * against an explicit budget, evicts to local disk when utilisation crosses the configured
 * threshold, and releases memory the instant an acknowledgement arrives.
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
 * `onHeapUnifiedMemory * bufferSizePercent / 100` and the per-partition allowance is that aggregate
 * divided by the reduce partition count, which is exactly the contracted
 * `(executorMemory * bufferPercent) / numPartitions`. The on-heap unified region is recovered from
 * the callable public surface of `MemoryManager` as
 * `maxOnHeapStorageMemory + onHeapExecutionMemoryUsed`, because `UnifiedMemoryManager` defines
 * `maxOnHeapStorageMemory` as `maxHeapMemory - onHeapExecutionMemoryUsed`; the two therefore sum
 * back to the region size, and both accessors are read inside a single `synchronized` block on the
 * memory manager's own monitor so the pair is one consistent observation rather than two that a
 * concurrent acquisition can perturb between. Reading it that way is deliberate: `getMaxMemory`,
 * `maxHeapMemory` and `RESERVED_SYSTEM_MEMORY_BYTES` are all private to the memory package and are
 * not callable from here, and reaching for them would have meant widening a preservation zone.
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
 * Cleanup. Release is registered on `TaskContext.addTaskCompletionListener`, so buffers are freed
 * and spill files deleted on success, on failure and on cancellation alike. This is the mechanism
 * by which the zero-leak requirement is met, and it is machine checked: the test JVM runs with
 * `spark.unsafe.exceptionOnMemoryLeak` enabled, so a retained reservation fails the build rather
 * than passing silently. Spill files are reference counted rather than deleted the instant their
 * last record is retired, because a block resolver hands out lazily opened file segments: a reader
 * takes a lease with [[acquireSpillFileLease]] and drops it with [[releaseSpillFileLease]], and the
 * file is unlinked only once it is retired and unleased. [[close]] is the bounded backstop and
 * unlinks unconditionally, because no reader may outlive the task that produced the bytes.
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
 */
private[spark] class MemorySpillManager(
    taskMemoryManager: TaskMemoryManager,
    conf: SparkConf,
    clock: Clock = new SystemClock,
    quotaOverride: Option[MemorySpillManager.ExecutorBufferQuota] = None,
    autoPoll: Boolean = true)
  extends MemoryConsumer(taskMemoryManager, MemoryMode.ON_HEAP) with Logging {

  import MemorySpillManager._

  // ----------------------------------------------------------------------------------------------
  // Configuration. Read once, held immutably. The two percentage keys are range validated by their
  // own ConfigEntry definitions, so an out-of-range value is rejected here at read time with
  // INVALID_CONF_VALUE.REQUIREMENT and never reaches the arithmetic below.
  // ----------------------------------------------------------------------------------------------

  // Held for diagnostics only. The percentages that actually size the budget are read once by the
  // executor-scoped quota, because the budget is shared and must be derived once per executor
  // rather than once per instance; see [[MemorySpillManager.ExecutorBufferQuota]].
  private val spillThresholdPercent: Int = conf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD)

  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  // Matches the sizing the sort-based spill path uses for its own disk writers, so streaming spill
  // files are buffered identically to every other spill file this executor produces.
  private val fileBufferSizeBytes: Int = conf.get(SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024

  // ----------------------------------------------------------------------------------------------
  // Deferred environment access. See the class comment: none of these may be dereferenced during
  // construction, because the shuffle manager that owns this component is built before the memory
  // manager exists on the driver.
  // ----------------------------------------------------------------------------------------------

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

  // ----------------------------------------------------------------------------------------------
  // Mutable state. Everything in this block is guarded by `lock` except the atomics, which are
  // deliberately lock free so that a diagnostic read never contends with a producer.
  // ----------------------------------------------------------------------------------------------

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

  // Retained spill records across every partition. Guarded by `lock`. Evicting a block returns its
  // whole charge -- payload and per-block overhead alike -- to the quota while its record stays on
  // the heap, so the record count is the one thing the byte budget stops bounding. Tracked as a
  // running total rather than derived, because admission consults it on the hot path.
  private var spilledRecordCount = 0

  // Acknowledged position per registered consumer, per partition. Guarded by `lock`, because a
  // position and the retirement it authorises must be decided in the same indivisible step. Only a
  // consumer present here may acknowledge anything, and retirement never advances past the slowest
  // entry, so a fast consumer cannot discard bytes a slower sibling has yet to receive.
  private val consumerPositions = new mutable.HashMap[String, mutable.HashMap[Int, Long]]()

  private var lastPollTimeMs = -1L

  private val partitionCount = new AtomicInteger(UNREGISTERED_PARTITION_COUNT)

  private val memoryPressure = new AtomicBoolean(false)

  private val memoryPressureCount = new AtomicLong(0L)

  private val permanentRefusalCount = new AtomicLong(0L)

  private val permanentRefusalLogged = new AtomicBoolean(false)

  private val memoryBytesSpilledTotal = new AtomicLong(0L)

  private val diskBytesSpilledTotal = new AtomicLong(0L)

  private val spillCountTotal = new AtomicLong(0L)

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

  private val spillFileDeletionFailureTotal = new AtomicLong(0L)

  // One aggregation gate per recurring condition that would otherwise be reported once per event.
  // Each gate is independent, so a flood of one condition never masks the first occurrence of
  // another, and every gate is driven by the injected clock so the bound is deterministic in tests.
  private val spillLogGate = new LogAggregationGate(LOG_AGGREGATION_WINDOW_MS)

  private val spillFailureLogGate = new LogAggregationGate(LOG_AGGREGATION_WINDOW_MS)

  private val reclamationLogGate = new LogAggregationGate(LOG_AGGREGATION_WINDOW_MS)

  private val spillFileDeletionLogGate = new LogAggregationGate(LOG_AGGREGATION_WINDOW_MS)

  /**
   * Per-partition buffer state: the in-memory blocks still awaiting acknowledgement, the blocks an
   * eviction has detached and is currently writing, the blocks already on disk and still awaiting
   * acknowledgement, the three byte tallies, the sequence watermarks and the last-access stamp that
   * breaks eviction ties. Every queue is kept in ascending sequence-number order, and every spilled
   * block precedes every in-memory block for the same partition, so a monotonically advancing
   * consumer position always retires a prefix.
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

  // ----------------------------------------------------------------------------------------------
  // Budget and registration
  // ----------------------------------------------------------------------------------------------

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
   * The aggregate streaming buffer allowance in bytes, that is `bufferSizePercent` of the on-heap
   * unified memory region. This ceiling is hard, it is shared by every instance on the executor,
   * and admission never exceeds it -- not even to let a single block through.
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
   * `(executorMemory * bufferPercent) / numPartitions` without requiring a registration first.
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

  // ----------------------------------------------------------------------------------------------
  // Inspection. Every accessor below is a pure read; none of them disturbs the eviction order, so
  // a test or a diagnostic may call them freely without perturbing what it is measuring.
  // ----------------------------------------------------------------------------------------------

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

  /** Number of eviction events performed, counted once per event and not once per partition. */
  def spillCount: Long = spillCountTotal.get()

  /**
   * Number of eviction attempts that failed, for instance because the local disk rejected the
   * write. A failure degrades to a refused admission and thence to sort-based fallback; it never
   * discards a buffered block.
   */
  def spillFailureCount: Long = spillFailureTotal.get()

  /** Bytes that have left memory through eviction, matching `TaskMetrics.memoryBytesSpilled`. */
  def memoryBytesSpilled: Long = memoryBytesSpilledTotal.get()

  /** Bytes committed to local disk by eviction, matching `TaskMetrics.diskBytesSpilled`. */
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

  // ----------------------------------------------------------------------------------------------
  // Admission
  // ----------------------------------------------------------------------------------------------

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
            if (attempt >= MAX_ADMISSION_ATTEMPTS || evictAndRelease(required) <= 0L) {
              settled = true
            }
          }

        case AdmissionNeedsRoom =>
          // Transient: the budget is full right now. Evict and re-evaluate. A final refusal raises
          // the memory-pressure signal, because a producer that cannot buffer is precisely the
          // condition under which the fallback policy must route the shuffle to sort-based shuffle.
          if (attempt >= MAX_ADMISSION_ATTEMPTS || evictAndRelease(required) <= 0L) {
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
      // returned the whole charge to the quota. Bounding their number across partitions is
      // therefore the only thing standing between a consumer that stopped acknowledging and
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
      // retained record is heap this class does not charge for, so the block count is bounded too.
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

  // ----------------------------------------------------------------------------------------------
  // Threshold polling and eviction
  // ----------------------------------------------------------------------------------------------

  /**
   * Performs one cadence-gated utilisation check, evicting if the threshold is met.
   *
   * Driven on the contracted 100 ms cadence by the executor's shared ticker whenever this instance
   * was constructed with `autoPoll` enabled, and callable by the owner from its own thread as often
   * as it likes besides. The 100 ms gate, measured with the injected clock, suppresses the check in
   * between either way, so a test that withholds the instance from the ticker can advance a manual
   * clock and observe exactly which calls do work.
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
        } else if (attempt >= MAX_ADMISSION_ATTEMPTS || evictAndRelease(bytes) <= 0L) {
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
    val held = bufferedBytes
    if (closed.get() || held <= 0L) {
      0L
    } else {
      evictAndRelease(held)
    }
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
      val freed = evictToDisk(size)
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

  /** Evicts to disk and then hands the freed bytes back to both budgets. */
  private def evictAndRelease(bytesToFree: Long): Long = {
    val freed = evictToDisk(bytesToFree)
    if (freed > 0L) {
      releaseReclaimedBytes(freed)
    }
    freed
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
   */
  private def evictToDisk(bytesToFree: Long): Long = {
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
          memoryBytesSpilledTotal.addAndGet(result.freedBytes)
          diskBytesSpilledTotal.addAndGet(result.committedDiskBytes)
          spillCountTotal.incrementAndGet()
          // One increment per eviction event, never one per partition and never one per block,
          // which is what keeps the telemetry cost off the data path.
          StreamingShuffleMetricsSource.incrementSpillCount(1L)
          val elapsedMs = clock.getTimeMillis() - startTimeMs
          lastSpillMs.set(elapsedMs)
          // Spilling is a recurring condition, not an incident: once a workload is over its budget
          // every eviction would produce a line, and a producer can evict many times a second. The
          // default-level record is therefore bounded to one line per aggregation window and
          // carries the cumulative totals plus how many evictions it stands in for, so an operator
          // loses no information about volume -- only the per-event granularity, which the
          // streaming debug key restores. This is what keeps the executor inside its log-volume
          // budget without ever going silent about spilling.
          spillLogGate.admit(clock.getTimeMillis()) match {
            case Some(unreportedEvents) =>
              logInfo(log"Streaming shuffle evicted " +
                log"${MDC(NUM_PARTITIONS, result.evictedPartitions)} buffered partitions holding " +
                log"${MDC(BYTE_SIZE, Utils.bytesToString(result.freedBytes))} to local disk in " +
                log"${MDC(DURATION, elapsedMs)} ms, committing " +
                log"${MDC(NUM_BYTES, result.committedDiskBytes)} bytes " +
                log"(${MDC(COUNT, spillCountTotal.get())} evictions and " +
                log"${MDC(MEMORY_SIZE, memoryBytesSpilledTotal.get())} bytes spilled so far, " +
                log"${MDC(NUM_SKIPPED, unreportedEvents)} evictions not reported individually)")
            case None =>
              if (debugEnabled) {
                logInfo(log"Streaming shuffle evicted " +
                  log"${MDC(NUM_PARTITIONS, result.evictedPartitions)} buffered partitions " +
                  log"holding ${MDC(BYTE_SIZE, Utils.bytesToString(result.freedBytes))} to local " +
                  log"disk in ${MDC(DURATION, elapsedMs)} ms")
              }
          }
        }
        result.freedBytes
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
   * Three outcomes are handled. A partition that was written out has its durable records published,
   * minus any the consumer acknowledged while the write was in flight; a partition that could not
   * be written has its blocks returned to the head of its queue, still servable, so a failing disk
   * costs the shuffle its fast path and never its data; and a partition belonging to an instance
   * that was closed mid-write releases nothing at all, because closure has already released those
   * bytes and a second release would drive the memory manager's balance negative.
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
              // An acknowledgement that landed while the write was in flight only advanced the
              // watermark; dropping the records it covers is what retires them here, and a file
              // covering nothing that survives is unlinked immediately rather than leaked.
              val survivors = records.filter(_.sequenceNumber > watermark)
              buffer.spilledBlockRecords ++= survivors
              spilledRecordCount += survivors.size
              survivors.foreach { record =>
                spillFilesByBlockId.update(record.blockId, record.file)
                spillFileBlockIds.update(record.file, record.blockId)
              }
              buffer.lastAccessTimeMs = now
              committedDiskBytes += records.foldLeft(0L)((acc, record) => acc + record.length)
              evictedPartitions += 1
              if (survivors.isEmpty) {
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
          spilledFiles.foreach(filesToDelete += _)
      }
    }
    evictionInProgress = false
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
  private def spillPartitionBlocks(
      partitionId: Int,
      blocks: Seq[BufferedBlock]): Option[Seq[SpilledBlock]] = {
    // A freshly allocated ShuffleWriteMetrics, never the task's own shuffle-write reporter.
    // DiskBlockObjectWriter reports bytes, records and write time to whatever reporter it is
    // handed, so reusing the task's would double count spilled bytes as shuffle-written bytes.
    // Spilled volume belongs on the spill accumulators, which this class reports separately.
    val spillMetrics = new ShuffleWriteMetrics
    var file: File = null
    // Held outside the try so that the failure report can name the destination by its block id even
    // when the failure was the allocation itself.
    var spillBlockId: TempShuffleBlockId = null
    var writer: DiskBlockObjectWriter = null
    var succeeded = false
    try {
      // Allocated inside the failure boundary, not before it. Local scratch allocation is itself a
      // filesystem operation and fails for exactly the reasons a write does -- a full disk, an
      // unwritable directory, a device error -- so leaving it outside would let a disk failure
      // escape uncounted, past the rollback, and out of an eviction that has already detached
      // blocks.
      val (blockId, tempFile) = diskBlockManager.createTempShuffleBlock()
      spillBlockId = blockId
      file = tempFile
      writer = blockManager.getDiskWriter(blockId, tempFile, serializerInstance,
        fileBufferSizeBytes, spillMetrics)
      val records = new mutable.ArrayBuffer[SpilledBlock](blocks.size)
      blocks.foreach { block =>
        // Raw bytes: the payload is already framed, so re-serializing it as a key/value pair would
        // be pure overhead. `recordWritten` keeps the writer's record tally honest, since the raw
        // byte overload deliberately does not advance it.
        writer.write(block.data, 0, block.data.length)
        writer.recordWritten()
        val segment = writer.commitAndGet()
        records += SpilledBlock(partitionId, block.sequenceNumber, blockId, tempFile,
          segment.offset, segment.length)
      }
      writer.close()
      succeeded = true
      Some(records.toSeq)
    } catch {
      case NonFatal(e) =>
        val failures = spillFailureTotal.incrementAndGet()
        // A failing disk fails every eviction, which makes this the most prolific of the recurring
        // conditions, so it is bounded exactly like the success path. Two deliberate choices keep
        // the bounded line both safe and useful: the destination is named by its temporary block
        // id rather than by its path, so an executor's local storage layout is not written into a
        // default-level record, and the cause is named by its exception class, because the
        // exception's own message routinely embeds that same path. The full path and the complete
        // stack trace are emitted together under the streaming debug key, which is where an
        // operator investigating repeated spill failures should look.
        spillFailureLogGate.admit(clock.getTimeMillis()) match {
          case Some(unreportedFailures) =>
            val destination =
              if (spillBlockId == null) "an unallocated spill block" else spillBlockId.name
            logError(log"Failed to evict streaming shuffle partition " +
              log"${MDC(PARTITION_ID, partitionId)} to spill block " +
              log"${MDC(BLOCK_ID, destination)}: ${MDC(CLASS_NAME, e.getClass.getName)} " +
              log"(${MDC(COUNT, failures)} spill failures so far, " +
              log"${MDC(NUM_SKIPPED, unreportedFailures)} not reported individually)")
          case None =>
            if (debugEnabled) {
              val target = if (file == null) "an unallocated spill file" else file.getAbsolutePath
              logError(log"Failed to evict streaming shuffle partition " +
                log"${MDC(PARTITION_ID, partitionId)} to spill file ${MDC(PATH, target)}", e)
            }
        }
        None
    } finally {
      if (!succeeded) {
        // Roll the file back to its last committed position and then remove it outright: nothing
        // from a partially written spill file may ever be served.
        if (writer != null) {
          Utils.tryLogNonFatalError {
            writer.revertPartialWritesAndClose()
          }
        }
        if (file != null) {
          deleteSpillFile(file)
        }
      }
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Acknowledgement-driven reclamation
  // ----------------------------------------------------------------------------------------------

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
   * @param consumerId identity of the consumer, as the producer knows it; must be non-empty
   */
  def registerConsumer(consumerId: String): Unit = {
    require(consumerId != null && consumerId.nonEmpty, "consumerId must be non-empty")
    lock.synchronized {
      if (!closed.get()) {
        consumerPositions.getOrElseUpdate(consumerId, new mutable.HashMap[Int, Long]())
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
    val partitions = lock.synchronized {
      if (consumerPositions.remove(consumerId).isEmpty || consumerPositions.isEmpty) {
        Nil
      } else {
        partitionBuffers.keys.toList
      }
    }
    partitions.foldLeft(0L)((freed, partitionId) => freed + reclaim(partitionId))
  }

  /** The consumers currently entitled to acknowledge. */
  def registeredConsumers: Set[String] = lock.synchronized(consumerPositions.keySet.toSet)

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
   * `throughSequenceNumber`, and retires whatever that makes retirable -- which is what a consumer
   * acknowledgement means: those bytes have been received and need no longer be retained for
   * retransmission.
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
   * Retirement then advances to the *minimum* position across every registered consumer, so a block
   * is released only once every consumer entitled to it has confirmed receipt. In-memory blocks
   * give their bytes straight back to the memory manager. Blocks that had already been evicted gave
   * theirs back at eviction time, so all that remains for them is to delete the backing file, and
   * that happens only once no retained record still refers to it and no reader holds a lease on it.
   * Because a partition's spilled blocks always precede its in-memory blocks, and both queues are
   * ordered by sequence number, a monotonically advancing retirement position always retires a
   * prefix.
   *
   * The whole operation is synchronous and does no work proportional to anything but the retired
   * prefix, which is what keeps it inside the 100 ms reclamation target. The target is measured
   * rather than enforced: the achieved latency is recorded on [[lastReclamationDurationMs]], and a
   * breach is counted on [[reclamationDeadlineBreaches]] and logged.
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
    if (closed.get()) {
      0L
    } else {
      val accepted = lock.synchronized {
        recordAcknowledgementLocked(consumerId, partitionId, throughSequenceNumber)
      }
      if (!accepted) {
        rejectedAcknowledgements.incrementAndGet()
        0L
      } else {
        reclaim(partitionId)
      }
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

  private def reclaim(partitionId: Int): Long = {
    val startTimeMs = clock.getTimeMillis()
    var memoryFreed = 0L
    val filesToDelete = new mutable.ArrayBuffer[File]()
    lock.synchronized {
      // The retirement position is the minimum across every registered consumer, recomputed here
      // rather than passed in, so that a departing consumer and an advancing one reach the same
      // decision through the same code and neither can retire on the strength of its own position
      // alone.
      val position = retirementPositionLocked(partitionId)
      partitionBuffers.get(partitionId).foreach { buffer =>
        // Record the watermark before retiring anything. Blocks an eviction has already detached
        // are deliberately not touched here: they are the eviction's to account for, and this
        // watermark is how it learns they no longer need publishing. Advancing monotonically means
        // a late, lower acknowledgement can never un-retire what a higher one already covered.
        if (position > buffer.acknowledgedThroughSequence) {
          buffer.acknowledgedThroughSequence = position
        }
        while (buffer.memoryBlocks.nonEmpty &&
            buffer.memoryBlocks.head.sequenceNumber <= position) {
          val retired = buffer.memoryBlocks.removeHead()
          val bytes = retired.chargeBytes
          buffer.bufferedBytes -= bytes
          bufferedMemoryBytes -= bytes
          memoryFreed += bytes
        }
        val releasedFiles = new mutable.ArrayBuffer[File]()
        while (buffer.spilledBlockRecords.nonEmpty &&
            buffer.spilledBlockRecords.head.sequenceNumber <= position) {
          releasedFiles += buffer.spilledBlockRecords.removeHead().file
          spilledRecordCount -= 1
        }
        if (releasedFiles.nonEmpty) {
          // A file is retired once no retained record still refers to it, and unlinked only once it
          // is also unleased: a block resolver may be holding a lazily opened segment on it, and
          // unlinking underneath that reader is precisely the race the lease exists to close.
          val stillReferenced = buffer.spilledBlockRecords.map(_.file).toSet
          releasedFiles.distinct.filterNot(stillReferenced.contains).foreach { file =>
            if (retireSpillFileLocked(file)) {
              filesToDelete += file
            }
          }
        }
        buffer.lastAccessTimeMs = clock.getTimeMillis()
        // The partition's bookkeeping is deliberately retained even when it holds nothing. It
        // carries the sequence watermark that keeps the stream gap-free, and discarding it would
        // let a partition that drained completely re-admit a sequence number it had already
        // accepted. The number of entries is bounded by the reduce partition count either way.
      }
    }
    if (memoryFreed > 0L) {
      releaseReclaimedBytes(memoryFreed)
    }
    filesToDelete.foreach(deleteSpillFile)
    val elapsedMs = clock.getTimeMillis() - startTimeMs
    lastReclamationMs.set(elapsedMs)
    if (elapsedMs > RECLAMATION_DEADLINE_MS) {
      val breaches = reclamationBreachTotal.incrementAndGet()
      // Reclamation runs once per acknowledgement, so a machine that is merely slow breaches this
      // bound on nearly every acknowledgement. Bounding the record keeps a latency symptom from
      // becoming a log-volume problem, and the running breach count preserves the one thing an
      // operator actually needs from the suppressed lines: how often it is happening.
      reclamationLogGate.admit(clock.getTimeMillis()) match {
        case Some(unreportedBreaches) =>
          logWarning(log"Streaming shuffle buffer reclamation for partition " +
            log"${MDC(PARTITION_ID, partitionId)} took ${MDC(DURATION, elapsedMs)} ms, exceeding " +
            log"the ${MDC(THRESHOLD, RECLAMATION_DEADLINE_MS)} ms reclamation bound " +
            log"(${MDC(COUNT, breaches)} breaches so far, " +
            log"${MDC(NUM_SKIPPED, unreportedBreaches)} not reported individually)")
        case None =>
          if (debugEnabled) {
            logInfo(log"Streaming shuffle buffer reclamation for partition " +
              log"${MDC(PARTITION_ID, partitionId)} took ${MDC(DURATION, elapsedMs)} ms")
          }
      }
    }
    memoryFreed
  }

  // ----------------------------------------------------------------------------------------------
  // Lifecycle
  // ----------------------------------------------------------------------------------------------

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
    var admissionsInFlight = 0
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
        // The consumer registry is this instance's heap too, and once closed no acknowledgement can
        // retire anything, so keeping positions alive would serve nothing but the leak detector.
        consumerPositions.clear()
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
      filesToDelete.distinct.foreach(deleteSpillFile)
      reportTaskMetrics(context)
      if (debugEnabled) {
        logInfo(log"Released streaming shuffle buffers after " +
          log"${MDC(COUNT, spillCountTotal.get())} evictions totalling " +
          log"${MDC(BYTE_SIZE, Utils.bytesToString(memoryBytesSpilledTotal.get()))}")
      }
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Spill file leases. A block resolver serves a spilled block by handing out a lazily opened file
  // segment, so the file must outlive the record that named it: a consumer acknowledgement can
  // retire the last record referring to a file at any moment, and unlinking it underneath a reader
  // that has not opened it yet is a time-of-check-to-time-of-use race that surfaces as a missing
  // spill file rather than as anything diagnosable. Reference counting closes it: a reader leases
  // the file for as long as it may open it, and the file is unlinked only once it is both retired
  // and unleased.
  // ----------------------------------------------------------------------------------------------

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
      // granting. A retired file is refused in either case: every record in it was acknowledged.
      val servable = !closed.get() || spillFilesDetached
      if (!servable || retiredSpillFiles.contains(file)) {
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

  // ----------------------------------------------------------------------------------------------
  // Telemetry and disk helpers
  // ----------------------------------------------------------------------------------------------

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
      if (!Files.deleteIfExists(file.toPath) && debugEnabled) {
        logInfo(log"Streaming shuffle spill file ${MDC(FILE_NAME, file.getName)} was already " +
          log"removed")
      }
    } catch {
      case NonFatal(e) =>
        val failures = spillFileDeletionFailureTotal.incrementAndGet()
        // Cleanup walks every retained spill file, so one undeletable directory produces one line
        // per file. Bounding it is what stops a cleanup path from being noisier than the work it
        // is cleaning up after, and the default-level line names the file rather than its
        // absolute path so the executor's storage layout stays out of ordinary logs.
        spillFileDeletionLogGate.admit(clock.getTimeMillis()) match {
          case Some(unreportedFailures) =>
            logWarning(log"Failed to delete streaming shuffle spill file " +
              log"${MDC(FILE_NAME, file.getName)}: ${MDC(CLASS_NAME, e.getClass.getName)} " +
              log"(${MDC(COUNT, failures)} deletion failures so far, " +
              log"${MDC(NUM_SKIPPED, unreportedFailures)} not reported individually)")
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
   * Hard bound on the blocks one partition may retain in memory, mid-eviction and on disk together.
   *
   * The per-block charge already bounds how much *payload* a partition can hold, but a spilled
   * block's retained record is heap this class does not charge for, so a stream of small blocks
   * that spilled and went unacknowledged would grow that metadata without limit. Reaching the bound
   * raises memory pressure, which routes the shuffle to sort-based fallback rather than stalling
   * it.
   */
  val MAX_RETAINED_BLOCKS_PER_PARTITION: Int = 8192

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
   * The largest number of retained spill records one consumer may accumulate across every
   * partition.
   *
   * [[MAX_RETAINED_BLOCKS_PER_PARTITION]] bounds one partition; this bounds their sum, which is
   * what a producer streaming many partitions to a consumer that has stopped acknowledging would
   * otherwise grow without limit. At the 2 MiB block cap it stands for 512 GiB of spilled payload,
   * so it constrains only a stream that has already stopped making progress.
   */
  val MAX_RETAINED_SPILL_RECORDS_TOTAL: Int = 262144

  /** Name of the single daemon thread that drives the threshold cadence for the whole executor. */
  val POLLER_THREAD_NAME: String = "streaming-shuffle-spill-poller"

  /**
   * Cadence of the buffer-utilisation check, in milliseconds. [[MemorySpillManager.pollOnce]]
   * suppresses checks that arrive sooner than this.
   */
  val POLL_INTERVAL_MS: Long = 100L

  /**
   * The bound, in milliseconds, within which buffer reclamation must complete after a consumer
   * acknowledgement. Exceeding it is logged rather than enforced, because failing a task over a
   * reclamation that was merely slow would trade a latency problem for a correctness one.
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
   * Lock-free and driven by the owner's clock, so it is safe to consult from any thread and its
   * bound is exactly reproducible in a test that advances a manual clock.
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
     * End of the window opening at `nowMs`, saturating instead of wrapping. A wrapped deadline
     * would land in the past and disable the gate outright, so it is ruled out arithmetically.
     */
    private def nextWindowEnd(nowMs: Long): Long = {
      val end = nowMs + windowMs
      if (end < nowMs) Long.MaxValue else end
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
   * Largest number of bytes one block can occupy on the wire, framing included, taken from the
   * protocol. Published here so that a producer sizing its framing against this manager's bound and
   * a limiter sizing its burst against the same bound read one value.
   */
  val MAX_ENCODED_FRAME_BYTES: Int = DataBlockMessage.MAX_ENCODED_FRAME_BYTES

  // -----------------------------------------------------------------------------------------------
  // Executor-scoped shared budget. One instance per JVM, and therefore per executor, because the
  // contracted bound is a fraction of *executor* memory: an executor runs many tasks at once, so a
  // budget derived per instance would let `n` concurrent producers hold `n * bufferSizePercent` of
  // the heap between them and the bound would be a per-task bound wearing an executor-wide name.
  // -----------------------------------------------------------------------------------------------

  /**
   * The streaming buffer allowance every [[MemorySpillManager]] on one executor reserves from.
   *
   * The allowance is derived once, on first use, from a single consistent observation of the
   * on-heap unified memory region, and held immutably thereafter -- which is what makes
   * "configuration changes require an executor restart" true by construction. Reservation is one
   * atomic compare-and-set against that ceiling, so two producers on different tasks can never both
   * observe room for the same bytes; refusal is the memory-pressure condition the fallback policy
   * consumes.
   *
   * @param bufferSizePercent percentage of the on-heap unified region the allowance occupies
   * @param spillThresholdPercent percentage of the allowance at which eviction is triggered
   * @param unifiedMemoryProvider supplies the size of the on-heap unified memory region; injected
   *                              so that a test can exercise admission against a budget of its own
   *                              choosing without depending on the host JVM's heap
   */
  class ExecutorBufferQuota(
      bufferSizePercent: Int,
      spillThresholdPercent: Int,
      unifiedMemoryProvider: () => Long)
    extends StreamingShuffleBufferUtilizationContributor {

    private val reserved = new AtomicLong(0L)

    private val refusals = new AtomicLong(0L)

    /**
     * Size of the on-heap unified memory region, floored at one byte so the utilisation arithmetic
     * can never divide by zero. Sampled exactly once, on first use.
     */
    lazy val unifiedMemoryBytes: Long = math.max(1L, unifiedMemoryProvider())

    /**
     * The aggregate allowance in bytes. Dividing before multiplying keeps the product away from
     * overflow for any conceivable heap while losing under one hundred bytes of precision, which is
     * irrelevant at this scale.
     */
    lazy val totalBytes: Long =
      math.max(1L, unifiedMemoryBytes / PERCENT_SCALE * bufferSizePercent)

    /** The utilisation level, in bytes, at which eviction is triggered. */
    lazy val spillTriggerBytes: Long =
      math.max(1L, totalBytes / PERCENT_SCALE * spillThresholdPercent)

    /** Bytes currently reserved across every instance drawing on this allowance. */
    def reservedBytes: Long = reserved.get()

    /** Reservations refused because the allowance was exhausted. */
    def refusalCount: Long = refusals.get()

    /**
     * Bytes this allowance is currently lending out, as reported to the executor-wide
     * `shuffle.streaming.bufferUtilizationPercent` gauge.
     *
     * One relaxed atomic read, so the metrics reporting thread never waits on a buffer monitor, and
     * the figure is the allowance's own -- the gauge sums the registered allowances rather than
     * letting the last writer overwrite an executor-wide slot.
     */
    override def contributedBufferedBytes: Long = reservedBytes

    /**
     * The allowance this contribution is measured against. Answered from the memoised total, so
     * reading the gauge never forces the lazy derivation and therefore never dereferences
     * [[org.apache.spark.SparkEnv]] from the metrics thread once the allowance has been used at
     * all.
     */
    override def contributedBudgetBytes: Long = totalBytes

    /**
     * Reserves `bytes` against the allowance, atomically and without blocking.
     *
     * @param bytes the number of bytes to reserve; must be positive
     * @return true when the reservation was taken and the caller now owns those bytes; false when
     *         it would have exceeded the allowance, in which case nothing was reserved
     */
    def tryReserve(bytes: Long): Boolean = {
      require(bytes > 0L, s"A quota reservation must be positive, but was $bytes")
      val ceiling = totalBytes
      var granted = false
      var settled = false
      while (!settled) {
        val current = reserved.get()
        if (current + bytes > ceiling) {
          refusals.incrementAndGet()
          settled = true
        } else if (reserved.compareAndSet(current, current + bytes)) {
          granted = true
          settled = true
        }
      }
      granted
    }

    /**
     * Returns `bytes` to the allowance. Floored at zero rather than allowed to go negative, so that
     * a bookkeeping defect degrades to a conservatively smaller allowance instead of silently
     * manufacturing headroom the executor does not have.
     *
     * @param bytes the number of bytes to return; must not be negative
     */
    def release(bytes: Long): Unit = {
      require(bytes >= 0L, s"A quota release must not be negative, but was $bytes")
      var settled = bytes == 0L
      while (!settled) {
        val current = reserved.get()
        settled = reserved.compareAndSet(current, math.max(0L, current - bytes))
      }
    }
  }

  // Guarded by this object's monitor. Held as `var` rather than as a lazy val because a test must
  // be able to discard them, and because the configuration they are derived from is only available
  // once an instance is constructed.
  private var sharedQuota: ExecutorBufferQuota = null

  private var poller: ScheduledExecutorService = null

  private val pollTargets = ConcurrentHashMap.newKeySet[MemorySpillManager]()

  /**
   * The executor's shared buffer allowance, created on first use from the given configuration.
   *
   * @param conf the configuration to read the buffer and threshold percentages from, exactly once
   *             per executor; every later caller receives the allowance the first one created
   */
  def executorQuota(conf: SparkConf): ExecutorBufferQuota = synchronized {
    if (sharedQuota == null) {
      sharedQuota = new ExecutorBufferQuota(
        conf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT),
        conf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD),
        () => onHeapUnifiedMemoryBytes())
      // Counted by the utilisation gauge for as long as the allowance exists. Registering the
      // allowance rather than each instance is what makes the gauge executor-wide without double
      // counting a shared budget: the numerator and the denominator both come from the one object
      // that owns them.
      StreamingShuffleMetricsSource.registerBufferUtilizationContributor(sharedQuota)
    }
    sharedQuota
  }

  /**
   * Size of the on-heap unified memory region, recovered from the callable public surface of the
   * memory manager as `maxOnHeapStorageMemory + onHeapExecutionMemoryUsed`.
   *
   * Both accessors synchronize on the memory manager's own monitor, so both are read inside one
   * `synchronized` block on that same monitor: the pair is then a single consistent observation
   * rather than two independent ones a concurrent execution-memory acquisition can perturb between.
   * Java monitors are reentrant, so nesting the accessors' own synchronisation inside ours is safe.
   */
  private def onHeapUnifiedMemoryBytes(): Long = {
    val manager = SparkEnv.get.memoryManager
    val (executionUsed, storageHeadroom) = manager.synchronized {
      (manager.onHeapExecutionMemoryUsed, manager.maxOnHeapStorageMemory)
    }
    val sum = storageHeadroom + executionUsed
    // Saturating addition. A production memory manager reports a real heap size, but a manager that
    // reports an effectively unbounded headroom would otherwise wrap the sum negative and collapse
    // the budget to one byte, which would look like permanent memory pressure rather than like the
    // unbounded budget it actually is.
    if (storageHeadroom > 0L && executionUsed > 0L && sum < 0L) Long.MaxValue else math.max(1L, sum)
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
   * Discards the executor-scoped allowance and empties the ticker's registry, so that a test can
   * exercise a fresh budget. Only a test calls this: on an executor the allowance is meant to
   * outlive every individual task.
   */
  private[streaming] def resetSharedStateForTesting(): Unit = synchronized {
    if (sharedQuota != null) {
      StreamingShuffleMetricsSource.unregisterBufferUtilizationContributor(sharedQuota)
    }
    sharedQuota = null
    pollTargets.clear()
  }

  // -----------------------------------------------------------------------------------------------
  // Internal admission and eviction outcomes. Modelled as types rather than as booleans so that the
  // several distinguishable ways an admission can fail -- transiently, permanently, because the
  // instance closed, or because the caller broke the sequence contract -- cannot be conflated.
  // -----------------------------------------------------------------------------------------------

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
   * @param partitionId the reduce partition the block belongs to
   * @param sequenceNumber the block's position in that partition's stream
   * @param blockId the temporary shuffle block that backs the file
   * @param file the file the segment lives in
   * @param offset the segment's start offset within that file
   * @param length the segment's length in bytes, as committed
   */
  case class SpilledBlock(
      partitionId: Int,
      sequenceNumber: Long,
      blockId: TempShuffleBlockId,
      file: File,
      offset: Long,
      length: Long)
}
