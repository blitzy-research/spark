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
import java.nio.file.Files
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{BYTE_SIZE, COUNT, DURATION, FILE_NAME, MEMORY_SIZE,
  NUM_BYTES, NUM_PARTITIONS, PARTITION_ID, THRESHOLD}
import org.apache.spark.internal.config.{SHUFFLE_FILE_BUFFER_SIZE,
  SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_DEBUG,
  SHUFFLE_STREAMING_SPILL_THRESHOLD}
import org.apache.spark.memory.{MemoryConsumer, MemoryManager, MemoryMode, TaskMemoryManager}
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.{BlockManager, DiskBlockManager, DiskBlockObjectWriter,
  TempShuffleBlockId}
import org.apache.spark.util.{Clock, SystemClock, Utils}

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
 * Budget derivation. The aggregate allowance is `onHeapUnifiedMemory * bufferSizePercent / 100`
 * and the per-partition allowance is that aggregate divided by the reduce partition count, which
 * is exactly the contracted `(executorMemory * bufferPercent) / numPartitions`. The on-heap
 * unified region is recovered from the callable public surface of [[MemoryManager]] as
 * `maxOnHeapStorageMemory + onHeapExecutionMemoryUsed`, because `UnifiedMemoryManager` defines
 * `maxOnHeapStorageMemory` as `maxHeapMemory - onHeapExecutionMemoryUsed`; the two therefore sum
 * back to the region size. Reading it that way is deliberate: `getMaxMemory`, `maxHeapMemory` and
 * `RESERVED_SYSTEM_MEMORY_BYTES` are all private to the memory package and are not callable from
 * here, and reaching for them would have meant widening a preservation zone.
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
 * 100 ms cadence measured with the injected [[Clock]]. There is no daemon thread and no sleep: the
 * owner drives [[pollOnce]] from whatever thread it already runs on, the cadence gate suppresses
 * work in between, and [[maybeSpill]] evaluates the threshold immediately when the cadence is not
 * wanted. That keeps the whole component deterministic under an injected clock, which is what
 * makes the polling cadence, the eviction order and the reclamation latency assertable without a
 * single timing-dependent test.
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
 * Concurrency. All buffer bookkeeping is guarded by one private monitor. One invariant governs
 * every path: `acquireMemory` and `freeMemory` are never called while that monitor is held.
 * `TaskMemoryManager` runs spill callbacks with its own monitor held, so a thread that held this
 * monitor while waiting for the task memory manager could deadlock against a thread that held the
 * task memory manager while waiting for this monitor. Reserving and releasing strictly outside the
 * monitor breaks that cycle by construction. Disk I/O, in contrast, is performed with the monitor
 * held on purpose: a spill is a rare, heavy barrier and serialising producers against it is the
 * correct behaviour, not a bottleneck to optimise away.
 *
 * Cleanup. Release is registered on `TaskContext.addTaskCompletionListener`, so buffers are freed
 * and spill files deleted on success, on failure and on cancellation alike. This is the mechanism
 * by which the zero-leak requirement is met, and it is machine checked: the test JVM runs with
 * `spark.unsafe.exceptionOnMemoryLeak` enabled, so a retained reservation fails the build rather
 * than passing silently.
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
 *   ...
 *   if (!spillManager.bufferBlock(partitionId, sequenceNumber, blockBytes)) {
 *     // Budget exhausted and nothing left to evict: hold the block and retry, or fall back.
 *   }
 *   spillManager.pollOnce()
 *   ...
 *   spillManager.acknowledge(partitionId, consumerPosition)
 * }}}
 *
 * @param taskMemoryManager the task memory manager this consumer reserves execution memory from
 * @param conf the configuration to read the streaming shuffle buffer keys from, exactly once
 * @param clock the time source used for the polling cadence, for last-access ordering and for
 *              latency measurement; injectable so that every timing-sensitive behaviour of this
 *              class can be driven deterministically from a test
 */
private[spark] class MemorySpillManager(
    taskMemoryManager: TaskMemoryManager,
    conf: SparkConf,
    clock: Clock = new SystemClock)
  extends MemoryConsumer(taskMemoryManager, MemoryMode.ON_HEAP) with Logging {

  import MemorySpillManager._

  // ----------------------------------------------------------------------------------------------
  // Configuration. Read once, held immutably. The two percentage keys are range validated by their
  // own ConfigEntry definitions, so an out-of-range value is rejected here at read time with
  // INVALID_CONF_VALUE.REQUIREMENT and never reaches the arithmetic below.
  // ----------------------------------------------------------------------------------------------

  private val spillThresholdPercent: Int = conf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD)

  private val bufferSizePercent: Int = conf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)

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

  private lazy val memoryManager: MemoryManager = SparkEnv.get.memoryManager

  // Only ever used to construct a DiskBlockObjectWriter. Spilled blocks are written as raw bytes
  // rather than as serialized key/value pairs, because they are already framed, so no object is
  // ever handed to this instance.
  private lazy val serializerInstance: SerializerInstance = SparkEnv.get.serializer.newInstance()

  /**
   * Size of the on-heap unified memory region, recovered from the callable public surface of the
   * memory manager. Floored at one byte so that the utilisation arithmetic can never divide by
   * zero. The two accessors are separately synchronized, so a concurrent execution-memory
   * acquisition can perturb the sum; that is immaterial because the value is sampled once, on
   * first use, and then held immutably as the budget for the lifetime of this consumer.
   */
  private lazy val onHeapUnifiedMemoryBytes: Long = {
    val manager = memoryManager
    val executionUsed = manager.onHeapExecutionMemoryUsed
    val storageHeadroom = manager.maxOnHeapStorageMemory
    val sum = storageHeadroom + executionUsed
    // Saturating addition. A production memory manager reports a real heap size, but a manager that
    // reports an effectively unbounded headroom would otherwise wrap the sum negative and collapse
    // the budget to one byte, which would look like permanent memory pressure rather than like the
    // unbounded budget it actually is.
    val saturated = if (storageHeadroom > 0L && executionUsed > 0L && sum < 0L) {
      Long.MaxValue
    } else {
      sum
    }
    math.max(1L, saturated)
  }

  /**
   * The aggregate streaming buffer allowance in bytes. Dividing before multiplying keeps the
   * product away from overflow for any conceivable heap while losing under one hundred bytes of
   * precision, which is irrelevant at this scale.
   */
  private lazy val aggregateBudgetBytes: Long =
    math.max(1L, onHeapUnifiedMemoryBytes / PERCENT_SCALE * bufferSizePercent)

  /**
   * The utilisation level, in bytes, at which eviction is triggered. Derived from the aggregate
   * allowance so that the configured percentage means the same thing whatever the heap size.
   */
  private lazy val spillTriggerBytes: Long =
    math.max(1L, aggregateBudgetBytes / PERCENT_SCALE * spillThresholdPercent)

  // ----------------------------------------------------------------------------------------------
  // Mutable state. Everything in this block is guarded by `lock` except the atomics, which are
  // deliberately lock free so that a diagnostic read never contends with a producer.
  // ----------------------------------------------------------------------------------------------

  private val lock = new Object()

  private val partitionBuffers = new mutable.HashMap[Int, PartitionBuffer]()

  private var bufferedMemoryBytes = 0L

  private var lastPollTimeMs = -1L

  private val partitionCount = new AtomicInteger(1)

  private val memoryPressure = new AtomicBoolean(false)

  private val memoryPressureCount = new AtomicLong(0L)

  private val memoryBytesSpilledTotal = new AtomicLong(0L)

  private val diskBytesSpilledTotal = new AtomicLong(0L)

  private val spillCountTotal = new AtomicLong(0L)

  private val spillFailureTotal = new AtomicLong(0L)

  private val peakMemoryBytesSeen = new AtomicLong(0L)

  private val lastSpillMs = new AtomicLong(0L)

  private val lastReclamationMs = new AtomicLong(0L)

  private val closed = new AtomicBoolean(false)

  private val metricsReported = new AtomicBoolean(false)

  private val cleanupRegistered = new AtomicBoolean(false)

  /**
   * Per-partition buffer state: the in-memory blocks still awaiting acknowledgement, the blocks
   * already evicted to disk and still awaiting acknowledgement, the in-memory byte tally and the
   * last-access stamp that breaks eviction ties. Both queues are kept in ascending sequence-number
   * order, and every spilled block precedes every in-memory block for the same partition, so a
   * monotonically advancing consumer position always retires a prefix.
   */
  private class PartitionBuffer(val partitionId: Int) {
    val memoryBlocks = new mutable.ArrayDeque[BufferedBlock]()
    val spilledBlockRecords = new mutable.ArrayDeque[SpilledBlock]()
    var memoryBytes: Long = 0L
    var lastAccessTimeMs: Long = 0L

    def isEmpty: Boolean = memoryBlocks.isEmpty && spilledBlockRecords.isEmpty
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
   * Idempotent and safe to call from any thread. Until it is called the count is one, which makes
   * the per-partition allowance equal to the aggregate allowance -- the correct degenerate answer
   * for a single-partition shuffle.
   *
   * @param count the number of reduce partitions; must be positive
   */
  def registerPartitionCount(count: Int): Unit = {
    require(count > 0, s"numPartitions must be positive, but was $count")
    partitionCount.set(count)
  }

  /** The reduce partition count this consumer divides its aggregate allowance by. */
  def numPartitions: Int = partitionCount.get()

  /**
   * The aggregate streaming buffer allowance in bytes, that is `bufferSizePercent` of the on-heap
   * unified memory region. This ceiling is hard: admission never exceeds it, not even to let a
   * single block through.
   */
  def totalBudgetBytes: Long = aggregateBudgetBytes

  /**
   * The utilisation level in bytes at which eviction is triggered, that is `spillThreshold`
   * percent of [[totalBudgetBytes]].
   */
  def spillThresholdBytes: Long = spillTriggerBytes

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
   * Live buffer utilisation as a percentage of [[totalBudgetBytes]], which is exactly the value
   * published to the `shuffle.streaming.bufferUtilizationPercent` gauge.
   *
   * An empty buffer answers zero without consulting the memory manager at all, which keeps this
   * accessor callable before the first admission and, in particular, after [[close]] -- when the
   * environment may no longer be able to answer.
   */
  def bufferUtilizationPercent: Long = {
    val buffered = bufferedBytes
    if (buffered <= 0L) 0L else buffered * PERCENT_SCALE / totalBudgetBytes
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

  /** Wall-clock duration of the most recent eviction event, in milliseconds. */
  def lastSpillDurationMs: Long = lastSpillMs.get()

  /** Wall-clock duration of the most recent acknowledgement-driven reclamation, in milliseconds. */
  def lastReclamationDurationMs: Long = lastReclamationMs.get()

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
   * The exact order in which eviction will consider partitions: largest in-memory footprint first,
   * ties broken least-recently-used first and then by ascending partition id. Partitions holding
   * nothing in memory are omitted, because evicting them would free nothing.
   */
  def spillSelectionOrder: Seq[Int] = lock.synchronized(selectionOrderLocked)

  /**
   * The blocks of one partition still held in memory awaiting acknowledgement, in ascending
   * sequence order. This is the retransmission window: a corrupted or lost block inside it can be
   * replayed from memory, whereas one outside it is gone and must be recovered by recomputing the
   * upstream stage.
   *
   * @param partitionId the reduce partition to report on
   */
  def unacknowledgedBlocks(partitionId: Int): Seq[BufferedBlock] = lock.synchronized {
    partitionBuffers.get(partitionId).map(_.memoryBlocks.toList).getOrElse(Nil)
  }

  /**
   * The blocks of one partition that were evicted to disk and are still awaiting acknowledgement,
   * in ascending sequence order. Each record carries the block id, the backing file and the exact
   * segment bounds, which is everything a block resolver needs to serve it.
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
   * @param partitionId the reduce partition the block belongs to; must be non-negative
   * @param sequenceNumber the block's position in the partition's stream; must be non-negative and
   *                       is expected to increase monotonically per partition
   * @param data the framed block payload, at most [[MemorySpillManager.MAX_BLOCK_SIZE_BYTES]] bytes
   * @return true when the block was admitted; false when the budget cannot accommodate it even
   *         after eviction, which is the producer's cue to hold the block and retry, or to let the
   *         fallback policy route the shuffle to sort-based shuffle
   */
  def bufferBlock(partitionId: Int, sequenceNumber: Long, data: Array[Byte]): Boolean = {
    require(partitionId >= 0, s"partitionId must be non-negative, but was $partitionId")
    require(sequenceNumber >= 0L, s"sequenceNumber must be non-negative, but was $sequenceNumber")
    require(data != null, "A streaming shuffle block payload must not be null")
    require(data.length <= MAX_BLOCK_SIZE_BYTES,
      s"A streaming shuffle block may not exceed $MAX_BLOCK_SIZE_BYTES bytes, but partition " +
        s"$partitionId sequence $sequenceNumber carried ${data.length} bytes")
    if (closed.get()) {
      false
    } else {
      val required = data.length.toLong
      // A zero-length block occupies no budget, so it is admitted without troubling the memory
      // manager at all. Short-circuit evaluation is what keeps `reserve` out of that path.
      if (required == 0L || reserve(partitionId, required)) {
        admit(partitionId, sequenceNumber, data, required)
        true
      } else {
        false
      }
    }
  }

  /**
   * Reserves `required` bytes for a partition, evicting and retrying once if the first attempt is
   * refused or only partially satisfied.
   */
  private def reserve(partitionId: Int, required: Long): Boolean = {
    if (!makeRoom(partitionId, required)) {
      false
    } else {
      val granted = acquireMemory(required)
      if (granted >= required) {
        true
      } else {
        // `acquireMemory` answers with the amount it was actually able to grant, which may be less
        // than the request. Spark's own spillable collections treat exactly that outcome as the
        // signal to spill, and this class does the same: hand the partial grant straight back so no
        // other consumer is starved by memory this one cannot use, publish the pressure signal the
        // fallback policy watches, evict our own buffers, and retry precisely once.
        if (granted > 0L) {
          freeMemory(granted)
        }
        recordMemoryPressure(required, granted)
        if (evictAndRelease(required) <= 0L) {
          false
        } else {
          val retried = acquireMemory(required)
          if (retried >= required) {
            true
          } else {
            if (retried > 0L) {
              freeMemory(retried)
            }
            false
          }
        }
      }
    }
  }

  /**
   * Ensures the two budget ceilings can accommodate `required` bytes, evicting once if they cannot.
   * Evaluated before any reservation is attempted, so the streaming budget is enforced by this
   * class itself and not merely inherited from whatever the memory manager happens to grant.
   */
  private def makeRoom(partitionId: Int, required: Long): Boolean = {
    val aggregateCeiling = totalBudgetBytes
    val partitionCeiling = perPartitionBudgetBytes
    val admittedImmediately = lock.synchronized {
      admissibleLocked(partitionId, required, aggregateCeiling, partitionCeiling)
    }
    if (admittedImmediately) {
      true
    } else {
      // Evict enough to accommodate the block, then re-evaluate exactly once. A second refusal is
      // final: there is nothing left to give back and the honest answer to the producer is no.
      evictAndRelease(required)
      lock.synchronized {
        admissibleLocked(partitionId, required, aggregateCeiling, partitionCeiling)
      }
    }
  }

  /** Evaluates both budget ceilings. Must be called while holding `lock`. */
  private def admissibleLocked(
      partitionId: Int,
      required: Long,
      aggregateCeiling: Long,
      partitionCeiling: Long): Boolean = {
    // The aggregate ceiling is absolute: buffer usage never exceeds `bufferSizePercent` of executor
    // memory, so a block that cannot fit inside it is refused outright, and that refusal is itself
    // the memory-pressure signal which routes the shuffle to sort-based fallback.
    val aggregateOk = bufferedMemoryBytes + required <= aggregateCeiling
    // The per-partition allowance subdivides that ceiling, so it carries one deliberate relaxation:
    // an empty partition always admits one block even when the block alone exceeds its share.
    // Without it a producer could stall forever on an indivisible block whenever the reduce
    // partition count made a partition's share smaller than a single framed block, and because the
    // relaxation stays inside the aggregate ceiling it cannot breach the memory bound.
    val partitionBytes = partitionBuffers.get(partitionId).map(_.memoryBytes).getOrElse(0L)
    val partitionOk = partitionBytes == 0L || partitionBytes + required <= partitionCeiling
    aggregateOk && partitionOk
  }

  /** Records an admitted block. The reservation has already succeeded by the time this runs. */
  private def admit(
      partitionId: Int,
      sequenceNumber: Long,
      data: Array[Byte],
      bytes: Long): Unit = {
    val now = clock.getTimeMillis()
    lock.synchronized {
      val buffer = partitionBuffers.getOrElseUpdate(partitionId, new PartitionBuffer(partitionId))
      buffer.memoryBlocks += BufferedBlock(partitionId, sequenceNumber, data)
      buffer.memoryBytes += bytes
      buffer.lastAccessTimeMs = now
      bufferedMemoryBytes += bytes
    }
    recordPeakMemory()
    publishUtilization()
  }

  // ----------------------------------------------------------------------------------------------
  // Threshold polling and eviction
  // ----------------------------------------------------------------------------------------------

  /**
   * Performs one cadence-gated utilisation check, evicting if the threshold is met.
   *
   * The owner calls this from its own thread as often as it likes; the 100 ms gate, measured with
   * the injected clock, suppresses the check in between. There is no background thread and nothing
   * ever sleeps, so a test can advance a manual clock and observe exactly which calls do work.
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
    if (closed.get() || buffered <= 0L || buffered < spillThresholdBytes) {
      false
    } else {
      // Free at least the overage so utilisation drops back below the threshold. Whole partitions
      // are evicted, so in practice rather more than the overage is reclaimed, which is what stops
      // the trigger from re-arming on the very next block.
      evictAndRelease(math.max(1L, buffered - spillThresholdBytes)) > 0L
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
        freeMemory(freed)
        publishUtilization()
        freed
      }
    } else {
      0L
    }
  }

  /** Evicts to disk and then hands the freed bytes back to the memory manager. */
  private def evictAndRelease(bytesToFree: Long): Long = {
    val freed = evictToDisk(bytesToFree)
    if (freed > 0L) {
      freeMemory(freed)
      publishUtilization()
    }
    freed
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
      var freedBytes = 0L
      var committedDiskBytes = 0L
      var evictedPartitions = 0
      // The monitor is held across the disk writes on purpose. An eviction is a rare, heavy memory
      // barrier, and serialising producers and acknowledgements against it is the correct
      // behaviour: it stops a partition from growing, or from being retired, halfway through being
      // written out. The invariant that matters is preserved -- no memory manager call is made from
      // inside this block.
      lock.synchronized {
        val candidates = selectionOrderLocked.iterator
        while (candidates.hasNext && freedBytes < bytesToFree) {
          val partitionId = candidates.next()
          partitionBuffers.get(partitionId).foreach { buffer =>
            val blocks = buffer.memoryBlocks.toList
            if (blocks.nonEmpty) {
              spillPartitionBlocks(partitionId, blocks) match {
                case Some(records) =>
                  val payloadBytes = blocks.foldLeft(0L)((acc, block) => acc + block.length)
                  buffer.memoryBlocks.clear()
                  buffer.memoryBytes -= payloadBytes
                  buffer.spilledBlockRecords ++= records
                  buffer.lastAccessTimeMs = clock.getTimeMillis()
                  bufferedMemoryBytes -= payloadBytes
                  freedBytes += payloadBytes
                  committedDiskBytes += records.foldLeft(0L)((acc, rec) => acc + rec.length)
                  evictedPartitions += 1
                case None =>
                  // This partition could not be written out. Its blocks stay in memory, untouched
                  // and still servable, and the next candidate is tried instead. Nothing is ever
                  // discarded on a spill failure, so a failing disk degrades to fallback rather
                  // than to data loss.
              }
            }
          }
        }
      }
      if (freedBytes > 0L) {
        memoryBytesSpilledTotal.addAndGet(freedBytes)
        diskBytesSpilledTotal.addAndGet(committedDiskBytes)
        spillCountTotal.incrementAndGet()
        // One increment per eviction event, never one per partition and never one per block, which
        // is what keeps the telemetry cost off the data path.
        StreamingShuffleMetricsSource.incrementSpillCount(1L)
        val elapsedMs = clock.getTimeMillis() - startTimeMs
        lastSpillMs.set(elapsedMs)
        logInfo(log"Streaming shuffle evicted ${MDC(NUM_PARTITIONS, evictedPartitions)} buffered " +
          log"partitions holding ${MDC(BYTE_SIZE, Utils.bytesToString(freedBytes))} to local " +
          log"disk in ${MDC(DURATION, elapsedMs)} ms, committing " +
          log"${MDC(NUM_BYTES, committedDiskBytes)} bytes " +
          log"(${MDC(COUNT, spillCountTotal.get())} evictions so far)")
      }
      freedBytes
    }
  }

  /**
   * Orders the partitions that currently hold memory: largest first, ties broken
   * least-recently-used first and then by ascending partition id. Must be called while holding
   * `lock`.
   */
  private def selectionOrderLocked: Seq[Int] = {
    partitionBuffers.values
      .filter(_.memoryBytes > 0L)
      .toSeq
      .sortBy(buffer => (-buffer.memoryBytes, buffer.lastAccessTimeMs, buffer.partitionId))
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
   * Never throws. A failure is logged, counted, rolled back and reported as `None`, so a failing
   * disk costs the shuffle its streaming fast path and nothing else.
   *
   * @return the durable records for the blocks, or `None` when the partition could not be written
   */
  private def spillPartitionBlocks(
      partitionId: Int,
      blocks: Seq[BufferedBlock]): Option[Seq[SpilledBlock]] = {
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()
    // A freshly allocated ShuffleWriteMetrics, never the task's own shuffle-write reporter.
    // DiskBlockObjectWriter reports bytes, records and write time to whatever reporter it is
    // handed, so reusing the task's would double count spilled bytes as shuffle-written bytes.
    // Spilled volume belongs on the spill accumulators, which this class reports separately.
    val spillMetrics = new ShuffleWriteMetrics
    var writer: DiskBlockObjectWriter = null
    var succeeded = false
    try {
      writer = blockManager.getDiskWriter(blockId, file, serializerInstance, fileBufferSizeBytes,
        spillMetrics)
      val records = new mutable.ArrayBuffer[SpilledBlock](blocks.size)
      blocks.foreach { block =>
        // Raw bytes: the payload is already framed, so re-serializing it as a key/value pair would
        // be pure overhead. `recordWritten` keeps the writer's record tally honest, since the raw
        // byte overload deliberately does not advance it.
        writer.write(block.data, 0, block.data.length)
        writer.recordWritten()
        val segment = writer.commitAndGet()
        records += SpilledBlock(partitionId, block.sequenceNumber, blockId, file, segment.offset,
          segment.length)
      }
      writer.close()
      succeeded = true
      Some(records.toSeq)
    } catch {
      case NonFatal(e) =>
        spillFailureTotal.incrementAndGet()
        logError(log"Failed to evict streaming shuffle partition " +
          log"${MDC(PARTITION_ID, partitionId)} to spill file ${MDC(FILE_NAME, file)}", e)
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
        deleteSpillFile(file)
      }
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Acknowledgement-driven reclamation
  // ----------------------------------------------------------------------------------------------

  /**
   * Retires every block of a partition up to and including `throughSequenceNumber`, which is what a
   * consumer acknowledgement means: those bytes have been received and need no longer be retained
   * for retransmission.
   *
   * In-memory blocks give their bytes straight back to the memory manager. Blocks that had already
   * been evicted gave theirs back at eviction time, so all that remains for them is to delete the
   * backing file, and that happens only once no retained record still refers to it. Because a
   * partition's spilled blocks always precede its in-memory blocks, and both queues are ordered by
   * sequence number, a monotonically advancing consumer position always retires a prefix.
   *
   * The whole operation is synchronous and does no work proportional to anything but the retired
   * prefix, which is how the 100 ms reclamation bound is met; the achieved latency is recorded on
   * [[lastReclamationDurationMs]] and a breach is logged.
   *
   * @param partitionId the reduce partition being acknowledged; must be non-negative
   * @param throughSequenceNumber the highest sequence number the consumer has received
   * @return the number of bytes released back to the memory manager
   */
  def acknowledge(partitionId: Int, throughSequenceNumber: Long): Long = {
    require(partitionId >= 0, s"partitionId must be non-negative, but was $partitionId")
    val startTimeMs = clock.getTimeMillis()
    var memoryFreed = 0L
    val filesToDelete = new mutable.ArrayBuffer[File]()
    lock.synchronized {
      partitionBuffers.get(partitionId).foreach { buffer =>
        while (buffer.memoryBlocks.nonEmpty &&
            buffer.memoryBlocks.head.sequenceNumber <= throughSequenceNumber) {
          val retired = buffer.memoryBlocks.removeHead()
          val bytes = retired.length.toLong
          buffer.memoryBytes -= bytes
          bufferedMemoryBytes -= bytes
          memoryFreed += bytes
        }
        val releasedFiles = new mutable.ArrayBuffer[File]()
        while (buffer.spilledBlockRecords.nonEmpty &&
            buffer.spilledBlockRecords.head.sequenceNumber <= throughSequenceNumber) {
          releasedFiles += buffer.spilledBlockRecords.removeHead().file
        }
        if (releasedFiles.nonEmpty) {
          val stillReferenced = buffer.spilledBlockRecords.map(_.file).toSet
          filesToDelete ++= releasedFiles.distinct.filterNot(stillReferenced.contains)
        }
        buffer.lastAccessTimeMs = clock.getTimeMillis()
        if (buffer.isEmpty) {
          partitionBuffers.remove(partitionId)
        }
      }
    }
    if (memoryFreed > 0L) {
      freeMemory(memoryFreed)
    }
    filesToDelete.foreach(deleteSpillFile)
    publishUtilization()
    val elapsedMs = clock.getTimeMillis() - startTimeMs
    lastReclamationMs.set(elapsedMs)
    if (elapsedMs > RECLAMATION_DEADLINE_MS) {
      logWarning(log"Streaming shuffle buffer reclamation for partition " +
        log"${MDC(PARTITION_ID, partitionId)} took ${MDC(DURATION, elapsedMs)} ms, exceeding the " +
        log"${MDC(THRESHOLD, RECLAMATION_DEADLINE_MS)} ms reclamation bound")
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
    if (cleanupRegistered.compareAndSet(false, true)) {
      context.addTaskCompletionListener[Unit](completed => closeInternal(Option(completed)))
    }
  }

  /**
   * Releases every buffer and removes every spill file. Idempotent, safe to call from any thread,
   * and guaranteed not to throw. Registered task cleanup calls this, so an explicit call is only
   * needed by an owner that is finished with this consumer before its task is.
   */
  def close(): Unit = closeInternal(Option(TaskContext.get()))

  /**
   * The single release path. The task context is passed in rather than looked up so that the
   * completion listener reports against the context it was registered on.
   */
  private def closeInternal(context: Option[TaskContext]): Unit = {
    if (closed.compareAndSet(false, true)) {
      val filesToDelete = new mutable.ArrayBuffer[File]()
      lock.synchronized {
        partitionBuffers.values.foreach { buffer =>
          buffer.memoryBlocks.clear()
          buffer.spilledBlockRecords.foreach(record => filesToDelete += record.file)
          buffer.spilledBlockRecords.clear()
          buffer.memoryBytes = 0L
        }
        partitionBuffers.clear()
        bufferedMemoryBytes = 0L
      }
      // Release whatever the memory manager still believes is reserved, rather than this class's
      // own tally. The memory manager's view is the one the task-level leak detector checks, so
      // freeing exactly that amount guarantees a zero balance even if bookkeeping had drifted.
      val reserved = getUsed()
      if (reserved > 0L) {
        freeMemory(reserved)
      }
      filesToDelete.distinct.foreach(deleteSpillFile)
      // The buffer is empty now, so utilisation is zero by definition. Publishing the constant
      // avoids re-deriving the budget, which after task completion may no longer be derivable.
      StreamingShuffleMetricsSource.setBufferUtilizationPercent(0L)
      reportTaskMetrics(context)
      if (debugEnabled) {
        logInfo(log"Released streaming shuffle buffers after " +
          log"${MDC(COUNT, spillCountTotal.get())} evictions totalling " +
          log"${MDC(BYTE_SIZE, Utils.bytesToString(memoryBytesSpilledTotal.get()))}")
      }
    }
  }

  /**
   * Publishes spill volume and peak reservation onto Spark's existing task accumulators, exactly
   * once. Reusing `memoryBytesSpilled`, `diskBytesSpilled` and `peakExecutionMemory` rather than
   * introducing parallel counters is what makes the streaming path visible on the UI, the history
   * server, the event log and the metrics REST API without a single change to any of them.
   */
  private def reportTaskMetrics(context: Option[TaskContext]): Unit = {
    if (metricsReported.compareAndSet(false, true)) {
      val memorySpilled = memoryBytesSpilledTotal.get()
      val diskSpilled = diskBytesSpilledTotal.get()
      val peak = peakMemoryBytesSeen.get()
      context.foreach { taskContext =>
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

  /** Refreshes the buffer-utilisation gauge. One atomic store, so it is safe on any thread. */
  private def publishUtilization(): Unit = {
    StreamingShuffleMetricsSource.setBufferUtilizationPercent(bufferUtilizationPercent)
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
        log"${MDC(MEMORY_SIZE, requested)} bytes was granted only " +
        log"${MDC(NUM_BYTES, granted)} bytes; signalling memory pressure so the streaming " +
        log"shuffle can fall back to sort-based shuffle if it persists")
    } else if (debugEnabled) {
      logInfo(log"Streaming shuffle buffer reservation of ${MDC(MEMORY_SIZE, requested)} bytes " +
        log"was granted only ${MDC(NUM_BYTES, granted)} bytes")
    }
  }

  /** Removes a spill file. Never throws: a file that cannot be deleted is logged, not escalated. */
  private def deleteSpillFile(file: File): Unit = {
    try {
      if (!Files.deleteIfExists(file.toPath) && debugEnabled) {
        logInfo(log"Streaming shuffle spill file ${MDC(FILE_NAME, file)} was already removed")
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to delete streaming shuffle spill file " +
          log"${MDC(FILE_NAME, file)}", e)
    }
  }

  // Registered last, and deliberately so. `addTaskCompletionListener` invokes its listener
  // immediately when the task has already completed, so this has to be the final statement of the
  // constructor body: by the time the listener can possibly run, every field above is initialised.
  // A consumer built outside a task registers explicitly through `registerCleanup` instead.
  Option(TaskContext.get()).foreach(registerCleanup)
}

/**
 * Constants and the durable record types of the streaming shuffle spill manager.
 */
private[spark] object MemorySpillManager {

  /** Divisor and multiplier for every percentage in this component. */
  val PERCENT_SCALE: Long = 100L

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
   * The largest framed block this manager will admit, in bytes. Blocks are capped so that a stream
   * pipelines rather than arriving in a few large stalls, and the cap is asserted here as well as
   * on the wire so that a mis-framed block is caught at its source.
   */
  val MAX_BLOCK_SIZE_BYTES: Int = 2 * 1024 * 1024

  /**
   * A framed block held in memory, awaiting consumer acknowledgement. Membership of this set is
   * what defines the retransmission window: a block still recorded here can be replayed from
   * memory, whereas one that has left it can only be recovered by recomputing the upstream stage.
   *
   * The payload is retained by reference and never copied, so a producer must treat the array as
   * having been handed over.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param sequenceNumber the block's position in that partition's stream
   * @param data the framed block payload
   */
  case class BufferedBlock(partitionId: Int, sequenceNumber: Long, data: Array[Byte]) {

    /** Payload length in bytes, which is exactly the budget the block occupies. */
    def length: Int = data.length
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
