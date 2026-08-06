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
  PARTITION_ID, REASON, THREAD_NAME, THRESHOLD, TIMEOUT}
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
 * Owns the bounded, spillable buffer budget of the streaming shuffle producer: it admits framed
 * blocks against an explicit budget, evicts the largest buffered partitions in least-recently-used
 * order to local disk once utilisation crosses the configured threshold, and releases memory as
 * soon as an acknowledgement arrives -- an acknowledged block being the only block whose memory may
 * be discarded, since the unacknowledged window is what makes retransmission possible.
 *
 * Two lifetimes meet here. Buffered bytes belong to the producing task and are returned when it
 * ends; spilled segments belong to the shuffle and are read after the map stage finishes, so an
 * acknowledgement releases memory and never a segment, and a successful producer hands its files to
 * the executor-scoped block resolver rather than deleting output a later reduce attempt may read.
 *
 * Extension by subclassing only: this class extends [[MemoryConsumer]] and overrides its abstract
 * `spill(long, MemoryConsumer)`, which buys Spark's existing memory accounting, cross-consumer
 * pressure arbitration and task-level leak detection without a line of change to `MemoryManager`,
 * `UnifiedMemoryManager`, `MemoryConsumer` or `TaskMemoryManager`. Spilled volumes are reported on
 * the task's existing spill accumulators rather than on counters of this subsystem's own.
 *
 * Concurrency. One private monitor guards all buffer bookkeeping, and every decision that must be
 * indivisible is taken inside it, so two producers can never both observe room and both take it.
 * Two invariants govern every path. `acquireMemory` and `freeMemory` are never called while that
 * monitor is held: `TaskMemoryManager` runs spill callbacks with its own monitor held, so acquiring
 * under ours could deadlock against a thread holding the task memory manager and waiting for ours.
 * And disk I/O is never performed while the monitor is held: eviction runs as three stages --
 * select and detach under the monitor, write with it released, publish or re-attach under it again
 * -- because holding it across a write would make the 100 ms reclamation bound unmeetable, an
 * acknowledgement needing the same monitor.
 *
 * @param taskMemoryManager the task memory manager this consumer reserves execution memory from
 * @param conf the configuration to read the streaming shuffle buffer keys from, exactly once
 * @param clock the time source used for the polling cadence, for last-access ordering and for
 *     latency measurement; injectable so that every timing-sensitive behaviour of this class can be
 *     driven deterministically from a test
 * @param quotaOverride an explicit executor buffer quota to draw from instead of the shared
 *     one; supplied only by tests, which must be able to exercise admission against a budget of
 *     their own choosing without depending on the host JVM's heap
 * @param autoPoll whether to join the executor's shared 100 ms threshold ticker; disabled by
 *     tests that drive [[pollOnce]] themselves so that no background thread perturbs what they are
 *     measuring
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

  // Configuration.

  // Held for diagnostics only.
  private val spillThresholdPercent: Int = conf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD)

  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  /**
   * Map tasks that can run at once in this JVM, which is the second divisor of a producer's framing
   * share.
   */
  val concurrentTaskSlots: Int = MemorySpillManager.executorTaskSlots(conf)

  // Matches the sizing the sort-based spill path uses for its own disk writers, so streaming spill
  // files are buffered identically to every other spill file this executor produces.
  private val fileBufferSizeBytes: Int = conf.get(SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024

  // Deferred environment access.

  private lazy val blockManager: BlockManager = SparkEnv.get.blockManager

  private lazy val diskBlockManager: DiskBlockManager = blockManager.diskBlockManager

  /**
   * The executor's serializer manager, which owns the compression and encryption a spill file is
   * wrapped in.
   */
  private lazy val serializerManager: SerializerManager = blockManager.serializerManager

  /** The serializer handed to every spill writer, and deliberately the no-op one. */
  private val serializerInstance: SerializerInstance = DummySerializerInstance.INSTANCE

  /** The executor-wide buffer allowance this instance draws from. */
  private lazy val quota: ExecutorBufferQuota =
    quotaOverride.getOrElse(MemorySpillManager.executorQuota(conf))

  private lazy val diskQuota: ExecutorDiskQuota =
    diskQuotaOverride.getOrElse(MemorySpillManager.executorDiskQuota(conf, quota))

  // Mutable state.

  private val lock = new Object()

  private val partitionBuffers = new mutable.HashMap[Int, PartitionBuffer]()

  private var bufferedMemoryBytes = 0L

  // Bytes reserved from the shared quota and from the task memory manager for admissions that have
  // been granted room but have not yet been committed into a partition queue.
  private var pendingReservedBytes = 0L

  // Number of admissions currently between "granted room" and "committed or rolled back".
  private var inFlightAdmissions = 0

  // Whether an eviction is between its select-and-detach stage and its publish-or-re-attach stage.
  private var evictionInProgress = false

  // Outstanding reader leases per spill file, and the set of files whose last record has been
  // retired.
  private val spillFileLeases = new mutable.HashMap[File, Int]()

  private val retiredSpillFiles = new mutable.HashSet[File]()

  // O(1) index between a spill file and the temporary block identity it was allocated under, in
  // both directions.
  private val spillFilesByBlockId = new mutable.HashMap[TempShuffleBlockId, File]()

  private val spillFileBlockIds = new mutable.HashMap[File, TempShuffleBlockId]()

  // Retained spill records across every partition.
  private var spilledRecordCount = 0

  private var spilledMetadataReservedBytes = 0L

  // Acknowledged position per registered consumer, per partition.
  private val consumerPositions = new mutable.HashMap[String, mutable.HashMap[Int, Long]]()

  // Partitions whose acknowledged prefix extends beyond the most recent bounded reclamation batch.
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

  // End-of-stream durability flushes, counted apart from threshold-driven evictions.
  private val durabilityFlushTotal = new AtomicLong(0L)

  // Blocks written straight to disk because the allowance could not admit them to memory.
  private val durableAdmissionTotal = new AtomicLong(0L)

  private val spillFailureTotal = new AtomicLong(0L)

  private val peakMemoryBytesSeen = new AtomicLong(0L)

  private val lastSpillMs = new AtomicLong(0L)

  private val lastReclamationMs = new AtomicLong(0L)

  // The lifecycle flag.
  private val closed = new AtomicBoolean(false)

  // Whether the spill files this instance produced have been handed to the executor-scoped block
  // resolver.
  private var spillFilesDetached = false

  // Bytes reserved for a producer's framing scratch -- the accumulators a serialization stream
  // writes into before its bytes are cut into blocks.
  private var scratchReservedBytes = 0L

  private val metricsReported = new AtomicBoolean(false)

  private val cleanupRegistered = new AtomicBoolean(false)

  // The task that owns this consumer, retained from the first cleanup registration.
  private val ownerContext = new AtomicReference[TaskContext](null)

  private val reclamationBreachTotal = new AtomicLong(0L)

  private val rejectedAcknowledgements = new AtomicLong(0L)

  private val rejectedConsumerRegistrations = new AtomicLong(0L)

  private val spillFileDeletionFailureTotal = new AtomicLong(0L)

  // Each recurring condition this class reports is aggregated through the EXECUTOR-scoped gate of
  // the same name on the companion object, never through a field of this instance.

  /**
   * Per-partition buffer state: the in-memory blocks still awaiting acknowledgement, the blocks an
   * eviction has detached and is currently writing, the durable records of the blocks already on
   * disk, the three byte tallies, the sequence watermarks and the last-access stamp that breaks
   * eviction ties.
   */
  private class PartitionBuffer(val partitionId: Int) {
    val memoryBlocks = new mutable.ArrayDeque[BufferedBlock]()
    val spillingBlocks = new mutable.ArrayDeque[BufferedBlock]()
    val spilledBlockRecords = new mutable.ArrayDeque[SpilledBlock]()

    /** Charges of [[memoryBlocks]]: the only bytes an eviction can free right now. */
    var bufferedBytes: Long = 0L

    var spillingBytes: Long = 0L

    /** Charges reserved for admissions to this partition that have not yet been committed. */
    var pendingBytes: Long = 0L

    var lastAccessTimeMs: Long = 0L

    /**
     * Sequence number of the most recently admitted block, or [[UNSET_SEQUENCE]] before the first
     * admission.
     */
    var lastAcceptedSequence: Long = UNSET_SEQUENCE

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
   * Records the reduce partition count of the shuffle this consumer serves, which is the divisor of
   * the per-partition allowance.
   *
   * @param count the number of reduce partitions; must be positive
   * @throws IllegalArgumentException if the count is not positive, or if a different count has
   *     already been registered
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
   */
  def partitionDomainBound: Int = {
    val registered = partitionCount.get()
    if (registered == UNREGISTERED_PARTITION_COUNT) MAX_TRACKED_PARTITIONS else registered
  }

  /**
   * The aggregate streaming buffer allowance in bytes, that is `bufferSizePercent` of configured
   * executor memory.
   */
  def totalBudgetBytes: Long = quota.totalBytes

  /** Bytes currently reserved against [[totalBudgetBytes]] by every instance on this executor. */
  def executorReservedBytes: Long = quota.reservedBytes

  /**
   * The utilisation level in bytes at which eviction is triggered, that is `spillThreshold` percent
   * of [[totalBudgetBytes]].
   */
  def spillThresholdBytes: Long = quota.spillTriggerBytes

  /**
   * The per-partition allowance for an arbitrary partition count, implementing the contracted
   * `(executorMemory * bufferPercent) / numPartitions` -- with `bufferPercent` read as a
   * percentage, so the product is taken before the division by a hundred; see
   * [[MemorySpillManager.percentageOf]] -- without requiring a registration first.
   *
   * @param partitions the partition count to divide the aggregate allowance by; must be
   *     positive
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
   */
  def maxAdmissiblePayloadBytes: Long = {
    val allowance = perPartitionBudgetBytes - PER_BLOCK_OVERHEAD_BYTES
    math.max(0L, math.min(MAX_BLOCK_PAYLOAD_BYTES.toLong, allowance))
  }

  // Inspection.

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
   */
  def bufferUtilizationPercent: Long = {
    val reserved = quota.reservedBytes
    if (reserved <= 0L) 0L else reserved * PERCENT_SCALE / quota.totalBytes
  }

  /**
   * Number of '''threshold or pressure driven''' spill events, counted once per event and not once
   * per partition.
   */
  def spillCount: Long = spillCountTotal.get()

  /** Number of durability eviction passes that moved bytes, counted once per non-empty pass. */
  def durabilityFlushCount: Long = durabilityFlushTotal.get()

  /**
   * Number of blocks admitted straight to local disk by [[admitDurably]] because the buffer
   * allowance could not hold them.
   */
  def durableAdmissionCount: Long = durableAdmissionTotal.get()

  /**
   * Number of eviction attempts that failed, for instance because the local disk rejected the
   * write.
   */
  def spillFailureCount: Long = spillFailureTotal.get()

  /** Bytes that have left memory through eviction, matching `TaskMetrics.memoryBytesSpilled`. */
  def memoryBytesSpilled: Long = memoryBytesSpilledTotal.get()

  /** Bytes this consumer has committed to local disk, matching `TaskMetrics.diskBytesSpilled`. */
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

  /** Reclamations that exceeded the 100 ms target. */
  def reclamationDeadlineBreaches: Long = reclamationBreachTotal.get()

  /** Spill files this consumer failed to remove. */
  def spillFileDeletionFailures: Long = spillFileDeletionFailureTotal.get()

  /**
   * Whether a buffer reservation has been refused or only partially granted since the flag was last
   * cleared.
   */
  def memoryPressureDetected: Boolean = memoryPressure.get()

  /** Whether the retained-output disk byte, file or physical-headroom bound refused a write. */
  def diskPressureDetected: Boolean = diskPressure.get()

  /** Total number of refused or partially granted reservations observed. */
  def memoryPressureEvents: Long = memoryPressureCount.get()

  /** Clears the sticky memory-pressure flag so a policy that has already acted on it can re-arm. */
  def clearMemoryPressure(): Unit = memoryPressure.set(false)

  /** Whether this consumer has been closed and will refuse any further admission. */
  def isClosed: Boolean = closed.get()

  /**
   * Whether this store can still serve the output it retained, which is a different question from
   * whether it can still accept a block.
   */
  def servesRetainedOutput: Boolean = lock.synchronized(servesRetainedOutputLocked)

  /** [[servesRetainedOutput]] for a caller already holding `lock`. */
  private def servesRetainedOutputLocked: Boolean = !closed.get() || spillFilesDetached

  /**
   * The exact order in which eviction will consider partitions: largest evictable footprint first,
   * ties broken least-recently-used first and then by ascending partition id.
   */
  def spillSelectionOrder: Seq[Int] = lock.synchronized(selectionOrderLocked)

  /**
   * The sequence number of the most recently admitted block for one partition, or
   * [[MemorySpillManager.UNSET_SEQUENCE]] if none has been admitted.
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
   * in ascending sequence order.
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
   * Looks up a single in-memory block for retransmission.
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
   * @param partitionId the reduce partition the block belongs to
   * @param sequenceNumber the sequence number of the block to read
   * @return the payload bytes, or `None` if the block is no longer retained or could not be
   *     read
   */
  def retainedPayload(partitionId: Int, sequenceNumber: Long): Option[Array[Byte]] = {
    bufferedBlock(partitionId, sequenceNumber).map(_.data)
      .orElse(spilledBlock(partitionId, sequenceNumber).flatMap(readSpilledPayload))
  }

  /** Reads one committed spill segment back into its original payload bytes. */
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
          // Positioned through the channel rather than by skipping, because a skip may legally move
          // fewer bytes than asked and a partial skip would decode the wrong segment.
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

  /** Reads a decoded spill segment to its end, refusing anything larger than a protocol block. */
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

  /** A diagnostic rendering built only from state this consumer already holds. */
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
   * @param partitionId the reduce partition the block belongs to; must be non-negative
   * @param sequenceNumber the block's position in the partition's stream; must be non-negative,
   *     and must be exactly one greater than the partition's previously admitted sequence number
   *     once the partition has admitted anything
   * @param data the block payload: non-empty, and at most
   *     [[MemorySpillManager.MAX_BLOCK_PAYLOAD_BYTES]] bytes.
   * @return true when the block was admitted; false when the budget cannot accommodate it even
   *     after eviction, or when this instance is closed.
   * @throws IllegalArgumentException if the payload is empty, oversized, or carries a sequence
   *     number that is not the partition's next expected one
   */
  def bufferBlock(partitionId: Int, sequenceNumber: Long, data: Array[Byte]): Boolean = {
    require(partitionId >= 0, s"partitionId must be non-negative, but was $partitionId")
    require(sequenceNumber >= 0L, s"sequenceNumber must be non-negative, but was $sequenceNumber")
    require(data != null, "A streaming shuffle block payload must not be null")
    // An empty block carries nothing a consumer can use, yet it would still occupy a queue slot, a
    // sequence number and the per-block overhead.
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
          // `acquireMemory` is called with no monitor held.
          val granted = acquireMemory(required)
          if (granted >= required) {
            // The array is adopted, not copied, which is the ownership contract this method
            // documents.
            if (commitReservation(partitionId, sequenceNumber, data, required)) {
              recordPeakMemory()
              admitted = true
            } else {
              // Closed between reservation and commit.
              freeMemory(required)
            }
            settled = true
          } else {
            // `acquireMemory` answers with the amount it was actually able to grant, which may be
            // less than the request.
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
          // Transient: the budget is full right now.
          if (attempt >= MAX_ADMISSION_ATTEMPTS || !reclaimRoom(required)) {
            recordMemoryPressure(required, 0L)
            settled = true
          }

        case AdmissionRefused(reason) =>
          // Permanent for this block: no amount of eviction will make it admissible.
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
   * @param partitionId the reduce partition the block belongs to
   * @param sequenceNumber the block's position in that partition's gap-free ascending run
   * @param data the framed payload, adopted rather than copied for the duration of the write
   * @return true when the block is durably retained and servable; false when it was refused or
   *     the write failed, in which case nothing was published and the sequence is unclaimed
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
            // Counted as the spill event it is.
            recordSpillEvent()
            true
          case None =>
            // `spillPartitionBlocks` has already reported the failure, rolled the file back and
            // unlinked it.
            lock.synchronized {
              rollbackDurableSequenceLocked(partitionId, sequenceNumber, previousSequence)
            }
            false
        }
    }
  }

  /** Claims one sequence number for a durable admission, or declines it. */
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

  /** Publishes the records of a completed durable admission and reports their committed length. */
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

  /** Restores the accepted sequence of a partition after a durable admission failed to write. */
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

  /** Decides one admission attempt and, when it succeeds, takes the room for it. */
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
      // uncharged per-partition metadata under.
      AdmissionRefused(s"partition $partitionId is outside the admissible range " +
        s"[0, $partitionDomainBound)")
    } else if (spilledRecordCount >= MAX_RETAINED_SPILL_RECORDS_TOTAL) {
      // Every retained spill record is heap this instance no longer charges for, because eviction
      // returned the whole charge to the quota, and a record outlives acknowledgement because the
      // segment it names is map output rather than a retransmission buffer.
      AdmissionRefused(s"this consumer already retains $spilledRecordCount spilled block records " +
        s"across all partitions, the maximum of $MAX_RETAINED_SPILL_RECORDS_TOTAL")
    } else if (sequenceNumber != expected) {
      AdmissionOutOfSequence(expected)
    } else if (required > partitionCeiling) {
      // No eviction can ever help: the block alone exceeds the partition's entire allowance.
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
   *     false when the instance is closed, in which case the caller must free the memory it
   *     acquired because nothing else will
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
   * @return true when this call evicted at least one partition
   */
  def maybeSpill(): Boolean = {
    val buffered = bufferedBytes
    val threshold = spillThresholdBytes
    val reserved = quota.reservedBytes
    if (closed.get() || buffered <= 0L || reserved < threshold) {
      false
    } else {
      // Free at least the overage so utilisation drops back below the threshold.
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
   * @param bytes the reservation being requested; a non-positive request is a no-op that
   *     succeeds
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
   * Returns a scratch reservation.
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
   * @return bytes moved to disk by this call, which is zero when nothing was held in memory
   */
  def spillAllRetained(): Long = {
    var moved = 0L
    var passes = 0
    var held = bufferedBytes
    // Repeated until memory is empty rather than attempted once.
    while (!closed.get() && held > 0L && passes < MAX_DURABILITY_FLUSH_PASSES) {
      passes += 1
      val freed = evictAndRelease(held, durabilityFlush = true)
      moved += freed
      held = bufferedBytes
      if (freed <= 0L && held > 0L) {
        // Nothing left memory and nothing is being reclaimed, so another pass would plan the same
        // empty eviction.
        passes = MAX_DURABILITY_FLUSH_PASSES
      }
    }
    moved
  }

  /**
   * Makes up to `maxBytes` of the retained window durable ahead of the end of the task, without
   * waiting for the buffer threshold and without counting as a pressure spill.
   *
   * @param maxBytes the most this call may move out of memory; a non-positive figure is a no-op
   * @return bytes moved to disk by this call, which is zero when nothing was held in memory
   */
  def flushRetainedForDurability(maxBytes: Long): Long = {
    if (closed.get() || maxBytes <= 0L) {
      0L
    } else {
      val held = bufferedBytes
      if (held <= 0L) {
        0L
      } else {
        evictAndRelease(math.min(held, maxBytes), durabilityFlush = true)
      }
    }
  }

  /**
   * Releases memory on behalf of another consumer under pressure, which is the callback
   * `TaskMemoryManager` invokes when the task as a whole cannot satisfy an allocation.
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
        // Released with no buffer monitor held.
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

  /** Whether a reclamation is between its detach stage and its publish stage right now. */
  def evictionInFlight: Boolean = lock.synchronized(evictionInProgress)

  /**
   * Tries to make `bytesToFree` bytes of room, and reports whether retrying a reservation is worth
   * it.
   *
   * @param bytesToFree the room the caller needs
   * @return true when room was reclaimed, by this call or by another party, and the caller
   *     should re-attempt its reservation
   */
  private def reclaimRoom(bytesToFree: Long): Boolean = {
    val reservedBefore = quota.reservedBytes
    val freed = evictAndRelease(bytesToFree)
    freed > 0L || quota.reservedBytes < reservedBefore
  }

  /**
   * Returns bytes that have left this instance's buffers to both budgets they were taken from: the
   * executor-wide streaming quota and the task memory manager.
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
   * @param bytesToFree the number of bytes the caller needs to leave memory
   * @param durabilityFlush whether this eviction is the end-of-stream flush that makes a
   *     retained window durable rather than a reclamation the budget forced.
   */
  private def evictToDisk(bytesToFree: Long, durabilityFlush: Boolean): Long = {
    if (bytesToFree <= 0L) {
      0L
    } else {
      val startTimeMs = clock.getTimeMillis()
      // Stage one, monitor held: choose the victims and detach their blocks.
      val plan = lock.synchronized(planEvictionLocked(bytesToFree))
      if (plan.isEmpty) {
        0L
      } else {
        // Stage two, monitor released: write.
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
          // they appear on the existing spill accumulators.
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

  /** Reports one reclamation the budget forced, through the executor's own aggregation window. */
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

  /** Reports one end-of-stream durability flush, through a window of its own. */
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
   * least `bytesToFree` bytes have been planned or no candidate remains.
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
   * memory.
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
              // the write was in flight.
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
    // observe a half-published eviction.
    lock.notifyAll()
    EvictionResult(freedBytes, committedDiskBytes, evictedPartitions, filesToDelete.toSeq)
  }

  /**
   * Orders the partitions eviction can actually reclaim from: largest evictable footprint first,
   * ties broken least-recently-used first and then by ascending partition id.
   */
  private def selectionOrderLocked: Seq[Int] = {
    partitionBuffers.values
      .filter(buffer => buffer.bufferedBytes > 0L && buffer.spillingBlocks.isEmpty)
      .toSeq
      .sortBy(buffer => (-buffer.bufferedBytes, buffer.lastAccessTimeMs, buffer.partitionId))
      .map(_.partitionId)
  }

  /**
   * Opens the writer one spill file is written through.
   *
   * @param blockId temporary shuffle block the spill file was allocated as
   * @param file the spill file itself
   * @param serializer serializer instance the writer is opened with, which for a raw-byte spill
   *     is the dummy instance that performs no serialization
   * @param bufferSizeBytes write buffer size the writer is opened with
   * @param metrics reporter the writer publishes its own byte and record tallies to, which is
   *     deliberately not the task's shuffle-write reporter
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

  /**
   * Writes one partition's in-memory blocks to a temporary shuffle block on local disk. A temporary
   * *shuffle* block rather than a local one, because these files are read back over the shuffle
   * transport and so their compression must be governed by `spark.shuffle.compress`. Each block is
   * committed on its own, so every one lands as an independently decodable unit with exact bounds.
   *
   * A non-fatal failure -- including one raised by the temporary block allocation itself -- is
   * logged, counted, rolled back and reported as `None`, so a failing disk costs the shuffle its
   * streaming fast path and nothing else. A fatal error propagates.
   *
   * @return the durable records for the blocks, or `None` when the partition could not be written
   */
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
    // A freshly allocated reporter, never the task's shuffle-write reporter.
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
        reportSpillFailure(partitionId, spillBlockId, e)
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

  /**
   * Reports one bounded spill-write failure without ever disclosing where the executor keeps its
   * local directories or what its call stack looks like.
   *
   * @param partitionId the reduce partition whose eviction failed
   * @param spillBlockId the generated destination block, or `null` when allocation itself
   *     failed
   * @param failure what went wrong, reported by class rather than by message or trace
   */
  private def reportSpillFailure(
      partitionId: Int,
      spillBlockId: TempShuffleBlockId,
      failure: Throwable): Unit = {
    val destination =
      if (spillBlockId == null) "an unallocated spill block" else spillBlockId.name
    spillFailureLogAggregator.record(clock.getTimeMillis()) match {
      case Some(summary) =>
        logError(log"Failed to evict streaming shuffle partition " +
          log"${MDC(PARTITION_ID, partitionId)} to spill block " +
          log"${MDC(BLOCK_ID, destination)}: ${MDC(CLASS_NAME, failure.getClass.getName)} " +
          log"(${MDC(COUNT, summary.occurrences)} spill failures on this executor so far, " +
          log"${MDC(NUM_SKIPPED, summary.unreported)} not reported individually)")
      case None =>
        if (debugEnabled) {
          logError(log"Failed to evict streaming shuffle partition " +
            log"${MDC(PARTITION_ID, partitionId)} to spill block " +
            log"${MDC(BLOCK_ID, destination)}: ${MDC(CLASS_NAME, failure.getClass.getName)}")
        }
    }
  }

  // Acknowledgement-driven reclamation

  /**
   * Admits a consumer to the acknowledgement protocol for this producer.
   *
   * @param consumerId identity of the consumer, as the producer knows it; must be non-empty
   * @return true when the consumer is now registered, false when this store no longer serves or
   *     a consumer-registration cap is full
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

  /** How many consumers are currently entitled to acknowledge. */
  def registeredConsumerCount: Int = lock.synchronized(consumerPositions.size)

  /** Consumer registrations refused by the per-store or executor-wide identity cap. */
  def rejectedConsumerRegistrationCount: Long = rejectedConsumerRegistrations.get()

  /**
   * Releases every retained consumer cursor when the block resolver drops this producer.
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
   * @param consumerId identity of the acknowledging consumer; must be non-empty and registered
   * @param partitionId the reduce partition being acknowledged; must be non-negative
   * @param throughSequenceNumber the highest sequence number the consumer has received
   * @return the number of bytes released back to the memory manager, and zero when the
   *     acknowledgement was refused or released nothing
   */
  def acknowledge(consumerId: String, partitionId: Int, throughSequenceNumber: Long): Long = {
    require(consumerId != null && consumerId.nonEmpty, "consumerId must be non-empty")
    require(partitionId >= 0, s"partitionId must be non-negative, but was $partitionId")
    val accepted = lock.synchronized {
      // A store that has handed its files to the resolver still accepts acknowledgements, and must:
      // its consumers do almost all of their acknowledging after the producing task has ended, and
      // refusing them there would leave every cursor frozen at the position it held when the task
      // finished -- so a reconnecting consumer would be replayed output it had already consumed.
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

  /** Validates an acknowledgement and, if it stands, records the consumer's new position. */
  private def recordAcknowledgementLocked(
      consumerId: String,
      partitionId: Int,
      throughSequenceNumber: Long): Boolean = {
    consumerPositions.get(consumerId).exists { positions =>
      // The highest sequence number admitted for the partition is the high-water mark of what was
      // really sent.
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

  /** Starts reclamation for one partition and performs the first bounded batch synchronously. */
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
   * task succeeds, fails or is cancelled.
   *
   * @param context the task context whose completion release is bound to
   */
  def registerCleanup(context: TaskContext): Unit = {
    require(context != null, "context must not be null")
    // Retained before the listener is installed and independently of whether this call is the one
    // that installs it, so the owning task is known from the earliest possible moment.
    ownerContext.compareAndSet(null, context)
    if (cleanupRegistered.compareAndSet(false, true)) {
      context.addTaskCompletionListener[Unit](completed => closeInternal(Option(completed)))
    }
  }

  /** Releases every buffer and removes every spill file. */
  def close(): Unit =
    closeInternal(Option(ownerContext.get()).orElse(Option(TaskContext.get())))

  /** The single release path. */
  private def closeInternal(context: Option[TaskContext]): Unit = {
    val filesToDelete = new mutable.ArrayBuffer[File]()
    var accountedBytes = 0L
    var metadataBytesToRelease = 0L
    var admissionsInFlight = 0
    var consumerIdsToRelease = Seq.empty[String]
    // The lifecycle flip and the tally capture are one indivisible step, taken under the same
    // monitor that refuses admission.
    val firstClose = lock.synchronized {
      if (closed.compareAndSet(false, true)) {
        // Scratch is released with the blocks, and through the same two budgets, so a producer that
        // failed before returning its accumulators cannot leave the quota short.
        accountedBytes = bufferedMemoryBytes + scratchReservedBytes
        scratchReservedBytes = 0L
        admissionsInFlight = inFlightAdmissions
        // Files are deleted here only while this instance still owns them.
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
        // In-memory blocks always go, because the memory behind them is being returned.
        if (ownsFiles) {
          metadataBytesToRelease = spilledMetadataReservedBytes
          spilledMetadataReservedBytes = 0L
          partitionBuffers.clear()
          spilledRecordCount = 0
        }
        bufferedMemoryBytes = 0L
        // Every spill file this instance still owns goes, leased or not.
        if (ownsFiles) {
          filesToDelete ++= spillFileLeases.keys
          filesToDelete ++= retiredSpillFiles
          spillFileLeases.clear()
          retiredSpillFiles.clear()
          spillFilesByBlockId.clear()
          spillFileBlockIds.clear()
        }
        // The consumer registry survives a detached close and goes with an owning one.
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
      // Release precisely this instance's own committed tally, from both budgets.
      if (accountedBytes > 0L) {
        quota.release(accountedBytes)
        freeMemory(accountedBytes)
      }
      if (metadataBytesToRelease > 0L) {
        quota.release(metadataBytesToRelease, MetadataMemory)
      }
      // Residual reconciliation, and only when nothing was in flight.
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

  // Spill file leases.

  /**
   * Hands the spill files this instance produced to the executor-scoped block resolver.
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
   *     already been retired or this instance is closed, in which case the caller must not open it
   *     and must instead treat the block as no longer servable from disk
   */
  def acquireSpillFileLease(file: File): Boolean = {
    require(file != null, "file must not be null")
    lock.synchronized {
      // A closed instance grants no lease while it still owns its files, because it has already
      // unlinked them.
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
   * while the lease was held.
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
          // unregistration or resolver shutdown.
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
   * @param blockId the temporary shuffle block identity a spill file was allocated under
   */
  def spillFile(blockId: TempShuffleBlockId): Option[File] =
    lock.synchronized(spillFilesByBlockId.get(blockId))

  /** Outstanding reader leases on a spill file, for diagnostics and for tests. */
  def spillFileLeaseCount(file: File): Int = lock.synchronized(spillFileLeases.getOrElse(file, 0))

  /** Whether a spill file has been retired and is awaiting only the release of its last lease. */
  def spillFileRetired(file: File): Boolean = lock.synchronized(retiredSpillFiles.contains(file))

  /**
   * Marks a spill file as no longer referenced by any retained record.
   *
   * @return true when the file may be unlinked immediately, which is the case only when no
   *     reader lease is outstanding; false when a lease still holds it, in which case the last
   *     release of that lease performs the unlink
   */
  private def retireSpillFileLocked(file: File): Boolean = {
    // Retirement means no retained record names this file any more, so it must stop being findable
    // by name.
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
   * once.
   */
  private def reportTaskMetrics(context: Option[TaskContext]): Unit = {
    // The retained owning context is the fallback, and the report is claimed only once a context is
    // actually in hand: claiming it first would let a close performed off the task thread mark the
    // accounting as reported and then discard it, which is exactly how spilled bytes go missing
    // from a task's metrics.
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

  /** Counts one spill event, on this instance's tally and on the exported counter alike. */
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

  /** Publishes the memory-pressure trip condition. */
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

  /** Reports a refusal that no eviction can ever reverse. */
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

  /** Removes a spill file. */
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
        // produces one line per file per task.
        spillFileDeletionLogAggregator.record(clock.getTimeMillis()) match {
          case Some(summary) =>
            logWarning(log"Failed to delete streaming shuffle spill file " +
              log"${MDC(FILE_NAME, file.getName)}: ${MDC(CLASS_NAME, e.getClass.getName)} " +
              log"(${MDC(COUNT, summary.occurrences)} deletion failures on this executor so far, " +
              log"${MDC(NUM_SKIPPED, summary.unreported)} not reported individually)")
          case None =>
            if (debugEnabled) {
              logWarning(log"Failed to delete streaming shuffle spill file " +
                log"${MDC(FILE_NAME, file.getName)}: " +
                log"${MDC(CLASS_NAME, e.getClass.getName)}")
            }
        }
    }
  }

  // Joined to the executor's shared threshold ticker before cleanup is registered, and after every
  // field above is initialised, because the ticker thread may call `pollOnce` the instant this
  // returns.
  if (autoPoll) {
    MemorySpillManager.registerForPolling(this)
  }

  // Registered last, and deliberately so.
  Option(TaskContext.get()).foreach(registerCleanup)
}

/**
 * Constants, the executor-scoped shared budget, the shared threshold ticker and the durable record
 * types of the streaming shuffle spill manager.
 */
private[spark] object MemorySpillManager extends Logging {

  val PERCENT_SCALE: Long = 100L

  /**
   * `value * percent / PERCENT_SCALE`, computed exactly and without overflow.
   *
   * @param value the quantity to take a percentage of; must be non-negative
   * @param percent the percentage to take, as a whole number of hundredths; must be
   *     non-negative
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

  /** Sentinel meaning that no reduce partition count has been registered yet. */
  val UNREGISTERED_PARTITION_COUNT: Int = 0

  /**
   * Sentinel meaning that a partition has admitted no block yet, so its next admission establishes
   * the origin of its sequence run.
   */
  val UNSET_SEQUENCE: Long = Long.MinValue

  /**
   * Attempts one admission makes before answering no: one against the budget as it stands, and one
   * more after evicting.
   */
  val MAX_ADMISSION_ATTEMPTS: Int = 2

  /** Eviction passes one reclamation request makes before it accepts that nothing can be freed. */
  val MAX_EVICTION_PASSES: Int = 2

  /** Passes the end-of-stream durability flush makes before it reports what is still resident. */
  val MAX_DURABILITY_FLUSH_PASSES: Int = 4

  /** One wait slice, in milliseconds, spent waiting for an eviction in flight to publish. */
  val EVICTION_QUIESCENCE_SLICE_MS: Long = 10L

  /** Slices a caller waits for an eviction in flight before giving up on it. */
  val EVICTION_QUIESCENCE_SLICES: Int = 50

  /**
   * Hard bound on the blocks one partition may retain in memory, mid-eviction and on disk together.
   */
  val MAX_RETAINED_BLOCKS_PER_PARTITION: Int = 4096

  /** Transfer buffer used when a spilled block is decoded back into its payload. */
  val DEFAULT_REPLAY_BUFFER_BYTES: Int = 64 * 1024

  /**
   * The largest reduce partition count this consumer will track, and so the exclusive upper bound
   * on the partition ids it will admit while no count has been registered.
   */
  val MAX_TRACKED_PARTITIONS: Int = 1 << 20

  /** Most logical consumers one retained producer store may hold cursors for at once. */
  val MAX_REGISTERED_CONSUMERS_PER_STORE: Int = 4096

  /** Most distinct logical consumer identities admitted across the executor. */
  val MAX_REGISTERED_CONSUMER_IDENTITIES: Int =
    2 * MAX_REGISTERED_CONSUMERS_PER_STORE

  /**
   * The largest number of retained spill records one consumer may accumulate across every
   * partition.
   */
  val MAX_RETAINED_SPILL_RECORDS_TOTAL: Int = 65536

  val MAX_RETAINED_SPILL_FILES: Int = 4096

  val MAX_SPILL_SEGMENTS_PER_FILE: Int = 64

  val MAX_SPILL_FILE_PAYLOAD_BYTES: Long = 128L * 1024L * 1024L

  /**
   * Conservative expansion factor reserved before compression and encryption write a spill file.
   */
  val DISK_RESERVATION_MULTIPLIER: Long = 2L

  val DISK_RESERVATION_OVERHEAD_BYTES: Long = 64L * 1024L

  val DISK_TO_MEMORY_QUOTA_MULTIPLIER: Long = 8L

  val MAX_EXECUTOR_DISK_QUOTA_BYTES: Long = 8L * 1024L * 1024L * 1024L

  val LOCAL_DISK_QUOTA_PERCENT: Int = 10

  /** Free space left untouched on a selected local directory before a spill write begins. */
  val MIN_LOCAL_DISK_HEADROOM_BYTES: Long = 64L * 1024L * 1024L

  val SPILLED_RECORD_METADATA_BYTES: Long = 128L

  /** Name of the single daemon thread that drives the threshold cadence for the whole executor. */
  val POLLER_THREAD_NAME: String = "streaming-shuffle-spill-poller"

  val RECLAIMER_THREAD_NAME: String = "streaming-shuffle-reclaimer"

  /**
   * The bound, in milliseconds, within which the executor's threshold ticker must be gone once
   * [[shutdownExecutorPoller]] has asked it to stop.
   */
  val POLLER_SHUTDOWN_TIMEOUT_MS: Long = 5000L

  /** Cadence of the buffer-utilisation check, in milliseconds. */
  val POLL_INTERVAL_MS: Long = 100L

  /** Blocks one synchronous or asynchronous reclamation batch may retire under the store lock. */
  val MAX_RECLAIMED_BLOCKS_PER_BATCH: Int = 256

  val RECLAMATION_DISPATCH_INTERVAL_MS: Long = 1L

  /** Managers one dispatch round services before yielding to the scheduler. */
  val MAX_RECLAMATION_MANAGERS_PER_ROUND: Int = 64

  /**
   * The bound, in milliseconds, within which buffer reclamation must complete after a consumer
   * acknowledgement.
   */
  val RECLAMATION_DEADLINE_MS: Long = 100L

  /**
   * Width, in milliseconds, of the window within which one recurring condition is reported at
   * default level at most once.
   */
  val LOG_AGGREGATION_WINDOW_MS: Long = 60000L

  /**
   * Rate gate for one recurring, default-level log record.
   *
   * @param windowMs width of the reporting window in milliseconds
   */
  class LogAggregationGate(windowMs: Long) {

    /** Earliest time a report may be made. */
    private val nextReportTimeMs = new AtomicLong(Long.MinValue)

    /** Occurrences since the last report, which the next report accounts for and then clears. */
    private val unreportedOccurrences = new AtomicLong(0L)

    /**
     * Decides whether the caller should report this occurrence at default level.
     *
     * @param nowMs the current time, from the owner's clock
     * @return the number of occurrences that went unreported since the last report, when the
     *     caller should report; `None` when the caller should stay silent or fall back to a
     *     debug-level record
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
     */
    private[streaming] def reset(): Unit = {
      nextReportTimeMs.set(Long.MinValue)
      unreportedOccurrences.set(0L)
    }

    /** End of the window opening at `nowMs`, saturating instead of wrapping. */
    private def nextWindowEnd(nowMs: Long): Long = {
      val end = nowMs + windowMs
      if (end < nowMs) Long.MaxValue else end
    }
  }

  /**
   * What an admitted executor-scoped record has to say about the occurrences it stands in for.
   *
   * @param occurrences how many times the condition has occurred on this executor, this one
   *     included
   * @param volumeBytes cumulative bytes those occurrences moved, where the condition has a
   *     volume; zero for conditions that do not
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
     *     `None` when it should stay silent or fall back to a debug-level record
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

    /** Returns the aggregator to its initial state. */
    private[streaming] def reset(): Unit = {
      gate.reset()
      occurrences.set(0L)
      volume.set(0L)
    }
  }

  /** The largest block payload this manager will admit, in bytes. */
  val MAX_BLOCK_PAYLOAD_BYTES: Int = DataBlockMessage.MAX_BLOCK_SIZE_BYTES

  /**
   * Conservative JVM cost, in bytes, of retaining one block in a partition's queue: the payload
   * array's own object header and length field, the [[MemorySpillManager.BufferedBlock]] wrapper,
   * the queue slot that refers to it and the [[MemorySpillManager.SpilledBlock]] record it may
   * later become.
   */
  val RETAINED_BLOCK_OVERHEAD_BYTES: Long = 128L

  /** Bytes charged against the budget for every retained block over and above its payload. */
  val PER_BLOCK_OVERHEAD_BYTES: Long =
    DataBlockMessage.FRAMING_OVERHEAD_BYTES.toLong + RETAINED_BLOCK_OVERHEAD_BYTES

  /** One kind of heap charged to the executor-wide streaming allowance. */
  private[streaming] sealed trait MemoryCharge

  /** Retained producer payloads and the fixed framing scratch that creates them. */
  private[streaming] case object ProducerMemory extends MemoryCharge

  /** Decoded payloads retained by consumer hand-off queues and readers. */
  private[streaming] case object ConsumerMemory extends MemoryCharge

  /** Temporary frame copies held from encoding until a channel write completes. */
  private[streaming] case object TransientMemory extends MemoryCharge

  /** Queue nodes, credit windows, retained spill records and other bounded ledgers. */
  private[streaming] case object MetadataMemory extends MemoryCharge

  private val diskQuotaOwners = new ConcurrentHashMap[String, ExecutorDiskQuota]()

  /**
   * Largest number of bytes one block can occupy on the wire, framing included, taken from the
   * protocol.
   */
  val MAX_ENCODED_FRAME_BYTES: Int = DataBlockMessage.MAX_ENCODED_FRAME_BYTES

  // Executor-scoped shared budget.

  /**
   * The streaming buffer allowance every streaming shuffle participant on one executor reserves
   * from, in '''both''' directions.
   *
   * @param bufferSizePercent percentage of configured executor memory the allowance occupies
   * @param spillThresholdPercent percentage of the allowance at which eviction is triggered
   * @param executorMemoryProvider supplies the configured executor memory in bytes; injected so
   *     the allowance is a function of this argument rather than of whatever heap the host JVM
   *     happens to have been given
   */
  class ExecutorBufferQuota(
      bufferSizePercent: Int,
      spillThresholdPercent: Int,
      executorMemoryProvider: () => Long)
    extends StreamingShuffleBufferUtilizationContributor {

    // The bound.
    private val reserved = new AtomicLong(0L)

    private val refusals = new AtomicLong(0L)

    private val producerReserved = new AtomicLong(0L)

    private val consumerReserved = new AtomicLong(0L)

    private val transientReserved = new AtomicLong(0L)

    private val metadataReserved = new AtomicLong(0L)

    /**
     * Configured executor memory in bytes, floored at one byte so the utilisation arithmetic can
     * never divide by zero.
     */
    lazy val executorMemoryBytes: Long = math.max(1L, executorMemoryProvider())

    /**
     * The aggregate allowance in bytes: exactly `bufferSizePercent` of configured executor memory,
     * as the feature specifies it.
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
     */
    override def contributedBufferedBytes: Long = reservedBytes

    /** The allowance this contribution is measured against. */
    override def contributedBudgetBytes: Long = totalBytes

    /**
     * Reserves `bytes` of producer-side buffer against the allowance, atomically and without
     * blocking.
     *
     * @param bytes the number of bytes to reserve; must be positive
     * @return true when the reservation was taken and the caller now owns those bytes; false
     *     when it would have exceeded the allowance, in which case nothing was reserved
     */
    def tryReserve(bytes: Long): Boolean = {
      tryReserve(bytes, ProducerMemory)
    }

    /** Reserves `bytes` for one ownership category against the same aggregate allowance. */
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
     * Returns `bytes` of producer-side buffer to the allowance.
     *
     * @param bytes the number of bytes to return; must not be negative
     */
    def release(bytes: Long): Unit = {
      release(bytes, ProducerMemory)
    }

    /**
     * Returns up to `bytes` owned by one category, never touching another category's reservation.
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

  /** Executor-wide disk byte and file allowance for retained streaming output. */
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
     * @return a reservation the caller must commit or cancel, or `None` when either ceiling
     *     binds
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

    /** Releases a committed file after it has actually been removed. */
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

  // The executor-scoped aggregators, one per recurring condition a spill manager reports.
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

  private val logAggregators: Seq[ExecutorLogAggregator] = Seq(spillLogAggregator,
    durabilityFlushLogAggregator, spillFailureLogAggregator, diskPressureLogAggregator,
    reclamationLogAggregator, spillFileDeletionLogAggregator)

  // Guarded by this object's monitor.
  private var sharedQuota: ExecutorBufferQuota = null

  private var sharedDiskQuota: ExecutorDiskQuota = null

  private var poller: ScheduledExecutorService = null

  private var reclaimer: ScheduledExecutorService = null

  private val pollTargets = ConcurrentHashMap.newKeySet[MemorySpillManager]()

  private val reclamationTargets = new ConcurrentLinkedQueue[MemorySpillManager]()

  /** Reference count per stable logical consumer identity, guarded by this object's monitor. */
  private val consumerIdentityRefCounts = new mutable.HashMap[String, Int]()

  /** Claims one reference to a stable consumer identity against the executor-wide unique-id cap. */
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
   * @param conf the configuration to read the buffer percentage, the spill threshold and the
   *     executor memory from, exactly once per executor
   */
  def executorQuota(conf: SparkConf): ExecutorBufferQuota = synchronized {
    if (sharedQuota == null) {
      sharedQuota = new ExecutorBufferQuota(
        conf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT),
        conf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD),
        // No floor above the configured percentage.
        () => configuredExecutorMemoryBytes(conf))
      // Counted by the utilisation gauge for as long as the allowance exists.
      StreamingShuffleMetricsSource.registerBufferUtilizationContributor(sharedQuota)
    }
    sharedQuota
  }

  /** The executor's shared retained-output disk allowance. */
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

  /** Removes an instance from the shared ticker. */
  private def deregisterFromPolling(manager: MemorySpillManager): Unit = {
    pollTargets.remove(manager)
  }

  /**
   * Queues one manager for a bounded reclamation batch and starts the shared worker on first use.
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

  /** Drives one cadence tick across every registered instance. */
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

  // Internal admission and eviction outcomes.

  /** Outcome of one attempt to reserve room for a block. */
  private sealed trait AdmissionOutcome

  /** Room was taken. */
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
   * @param filesToDelete spill files to unlink, collected here so the unlink happens outside
   *     the buffer monitor
   */
  private case class EvictionResult(
      freedBytes: Long,
      committedDiskBytes: Long,
      evictedPartitions: Int,
      filesToDelete: Seq[File])

  /**
   * A framed block held in memory, awaiting consumer acknowledgement.
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
     * [[PER_BLOCK_OVERHEAD_BYTES]].
     */
    def chargeBytes: Long = data.length.toLong + PER_BLOCK_OVERHEAD_BYTES
  }

  /**
   * A framed block that was evicted to local disk and is still awaiting acknowledgement.
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
