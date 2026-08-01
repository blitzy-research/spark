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

import java.io.{IOException, OutputStream}
import java.util.Arrays

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{COUNT, DURATION, MAX_ATTEMPTS, MEMORY_SIZE, NUM_BLOCKS,
  NUM_BYTES, NUM_PARTITIONS, PARTITION_ID, REASON, RECORDS, SHUFFLE_ID, TASK_ATTEMPT_ID, THRESHOLD,
  TIMEOUT, VALUE}
import org.apache.spark.network.shuffle.protocol.streaming.DataBlockMessage
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{SerializationStream, SerializerInstance}
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.util.{Clock, SystemClock}

/**
 * The streaming shuffle collaborators a [[StreamingShuffleWriter]] is built on, bundled into one
 * value.
 *
 * The bundle exists for a reason that is worth stating rather than leaving to be inferred. Passing
 * the six collaborators individually, alongside the five arguments every shuffle writer needs, puts
 * the constructor at thirteen parameters, and scalastyle's `argcount` check caps a parameter list
 * constructors included -- at ten. Bundling is therefore not a stylistic preference but the
 * mechanism by which this writer stays inside a machine-enforced limit, and it buys two further
 * properties: the manager assembles the streaming subsystem once and hands the same shape to every
 * writer it creates, and a test can substitute one collaborator without restating the rest.
 *
 * Every member is supplied by [[StreamingShuffleManager]], which owns their lifetimes. The writer
 * borrows them for the duration of one map task and never constructs, replaces or closes anything
 * whose lifetime is wider than its own task.
 *
 * Scoping is deliberately mixed, and the difference matters to anyone extending this code:
 *
 *  - `spillManager` and `serverHandler` are per map task. The spill manager binds its memory to one
 *    task's [[org.apache.spark.memory.TaskMemoryManager]] and registers release on that task's
 *    completion; the server handler holds per-channel state and assigns the sequence numbers of one
 *    producer. Sharing either across concurrent map tasks of the same shuffle would interleave two
 *    producers' sequence numbers on one stream, which the receiving reader would reject as a
 *    sequence violation.
 *  - `backpressure`, `rateLimiter` and `fallbackPolicy` are executor scoped, because each of them
 *    exists precisely to reason across the shuffles running concurrently on one executor: buffer
 *    utilisation is aggregated, the egress budget is divided by the number of concurrent shuffles,
 *    and a fallback trip stands the whole executor's streaming path down.
 *  - `errorNotifier` is per map task, for the same reason the handler is. What it bridges is a
 *    failure observed on the Netty threads of one producer's channel, so sharing one notifier
 *    between concurrent map tasks of the same shuffle would let a failure in one of them be
 *    re-thrown by another, replacing a task's own diagnosis with a stranger's.
 *
 * Producer/consumer rendezvous is deliberately absent from this bundle. Announcing a producer to
 * [[StreamingShuffleCoordinator]] is an RPC against a driver endpoint, and the component that knows
 * whether it is running on a driver or an executor, and therefore how to reach that endpoint, is
 * [[StreamingShuffleManager]], which already holds both the map id and the task context at the
 * moment it creates a writer. Keeping the RPC there rather than here leaves this writer free of
 * `RpcEnv`, which is what allows the whole of the consumer-failure flow to be driven by a manual
 * clock in a test with no live Spark cluster at all.
 *
 * @param backpressure executor-wide flow control: credit, liveness timers and arbitration
 * @param rateLimiter executor-wide egress pacing, consulted here only for its framing ceiling
 * @param spillManager this task's buffer budget, retention window and disk spill
 * @param serverHandler this task's egress path: framing, checksums, pacing and the wire
 * @param fallbackPolicy the four graceful-degradation trip conditions
 * @param errorNotifier the first-error-wins bridge from Netty threads to the task thread
 */
private[spark] case class StreamingShuffleWriterComponents(
    backpressure: BackpressureProtocol,
    rateLimiter: TokenBucketRateLimiter,
    spillManager: MemorySpillManager,
    serverHandler: StreamingShuffleServerHandler,
    fallbackPolicy: StreamingShuffleFallbackPolicy,
    errorNotifier: StreamingShuffleErrorNotifier)

/**
 * Writes a map task's output by streaming it to its consumers instead of materialising it on disk.
 *
 * This is the producer half of the streaming shuffle. Where
 * [[org.apache.spark.shuffle.sort.SortShuffleWriter]] sorts a task's records, writes them to a
 * local data file and publishes an index so that reduce tasks may fetch them once the whole map
 * stage has finished, this writer frames records into small blocks and pushes them to the consumers
 * as they are produced. That is the entire point of the subsystem: it removes the materialisation
 * barrier, so reduce-side work overlaps map-side work rather than waiting for it.
 *
 * ==Wire contract==
 *
 * The contract this writer establishes with [[StreamingShuffleReader]] is deliberately simple, and
 * the reader depends on every clause of it:
 *
 *  - Each reduce partition receives '''one continuous serialization stream''', produced by a single
 *    [[SerializationStream]] opened at the first record for that partition and closed once. Blocks
 *    are consecutive byte ranges cut out of that one stream, never independently deserialisable
 *    units. A consumer concatenates a partition's payloads in ascending sequence order and
 *    deserialises the concatenation exactly once.
 *  - Sequence numbers are '''dense and ascending from zero''' in a partition. Both the retention
 *    window and the receiver's reassembly depend on there being no gaps, so a gap is a protocol
 *    violation rather than something to be tolerated.
 *  - No payload exceeds [[DataBlockMessage.MAX_BLOCK_SIZE_BYTES]]. The cap is what makes pipelining
 *    possible: a consumer may begin work on a block while later blocks are still in flight.
 *  - Payload bytes are '''not compressed and not encrypted by this writer'''. Compression would
 *    place a codec frame boundary in the middle of the byte range a block carries, which would
 *    defeat the incremental consumption the cap exists to enable; confidentiality on the wire
 *    belongs to the transport, which the streaming module configures under its own
 *    `spark.shuffle-streaming.io.*` namespace.
 *  - Every partition ends with an explicit end-of-stream signal, emitted even when the partition is
 *    empty. Without it a consumer could not tell a finished producer from a producer that has
 *    stopped responding, and would wait out the five-second producer timeout on every successful
 *    shuffle.
 *
 * ==Division of labour==
 *
 * This writer decides '''what''' leaves and '''when''' it may be produced. It does not implement
 * mechanics its collaborators already own, and duplicating them would be a defect rather than
 * defence in depth:
 *
 *  - [[MemorySpillManager]] owns the memory budget. Every block is offered to it before it goes to
 *    egress, so aggregate buffered bytes are bounded by the configured percentage of executor
 *    memory, spill happens at the configured threshold, and the retention window that makes
 *    retransmission possible is maintained in one place. Because it is a
 *    [[org.apache.spark.memory.MemoryConsumer]], Spark's existing accounting and its memory-leak
 *    detection cover the streaming path for free.
 *  - [[StreamingShuffleServerHandler]] owns framing, CRC32C computation, flush ordering, pacing and
 *    the wire. In particular '''it''' charges the token bucket and '''it''' feeds acknowledgements
 *    and heartbeats to the backpressure protocol from the Netty thread that observes them. This
 *    writer therefore never calls `tryAcquire` and never reports an acknowledgement: doing so would
 *    debit the executor's egress budget twice for the same bytes, halving effective bandwidth and
 *    inflating the throttle count. The one thing the writer does read from the limiter is its
 *    framing ceiling, which is exactly the use that API prescribes -- a producer that frames to the
 *    ceiling is never refused for size.
 *  - [[BackpressureProtocol]] owns credit, the acknowledgement and liveness timers and arbitration
 *    between concurrent shuffles. The writer reports its buffer utilisation into it and reads its
 *    verdicts back.
 *  - [[StreamingShuffleFallbackPolicy]] owns graceful degradation. The writer reports the two
 *    conditions only a producer can observe -- a buffer reservation that eviction could not rescue,
 *    and its own share of the administered link -- and obeys a trip by standing down.
 *
 * ==Threading==
 *
 * Every method of this class runs on the task thread, and nothing in it is safe to call from
 * anywhere else. That is no accident: [[ShuffleWriteMetricsReporter]] documents that it does not
 * synchronise, so the standard task metrics may only be touched from a single thread. The writer
 * consequently never lets a Netty thread near the reporter. Progress made on I/O threads is
 * published by reading the server handler's monotonic counters on the task thread and passing the
 * increments from there, and failures raised on I/O threads reach the task by way of
 * [[StreamingShuffleErrorNotifier]], which is re-checked at every block boundary. Without that
 * bridge a failure observed by Netty would be swallowed and the task would wait for input that is
 * never coming.
 *
 * ==Time==
 *
 * All elapsed-time decisions -- the maintenance cadence, the heartbeat interval, the consumer
 * liveness window and the replay backoff -- read the injected [[Clock]] and never `Thread.sleep`.
 * The writer schedules work at deadlines instead of waiting for them, so a test drives the whole of
 * FR-9's consumer-failure flow with a manual clock and no sleeps, which is what makes that flow
 * deterministic rather than flaky. The one exception is `System.nanoTime`, used solely to measure
 * the writer's own service time for [[ShuffleWriteMetricsReporter.incWriteTime]], mirroring
 * `SortShuffleWriter`.
 *
 * @param handle the streaming registration produced by `registerShuffle`
 * @param mapId this map task's identifier, as the scheduler knows it
 * @param context this task's context, the source of both attempt attributes and cleanup
 * @param writeMetrics the reporter supplied by [[org.apache.spark.shuffle.ShuffleWriteProcessor]],
 *                     which is this task's `shuffleWriteMetrics`
 * @param conf the executor's configuration, read once here and then held immutably
 * @param components the streaming collaborators, owned by [[StreamingShuffleManager]]
 * @param clock the time source for every elapsed-time decision
 * @tparam K the shuffle key type
 * @tparam V the shuffle value type
 * @tparam C the combiner type of the underlying dependency, unused by a streaming producer because
 *           map-side combination is a materialising operation
 */
private[spark] class StreamingShuffleWriter[K, V, C](
    handle: StreamingShuffleHandle[K, V, C],
    mapId: Long,
    context: TaskContext,
    writeMetrics: ShuffleWriteMetricsReporter,
    conf: SparkConf,
    components: StreamingShuffleWriterComponents,
    clock: Clock = new SystemClock)
  extends ShuffleWriter[K, V] with Logging {

  import StreamingShuffleWriter._

  // ---------------------------------------------------------------------------------------------
  // Collaborators, unpacked once so that the hot path reads a field rather than a case class member
  // ---------------------------------------------------------------------------------------------

  private val backpressure: BackpressureProtocol = components.backpressure
  private val rateLimiter: TokenBucketRateLimiter = components.rateLimiter
  private val spillManager: MemorySpillManager = components.spillManager
  private val serverHandler: StreamingShuffleServerHandler = components.serverHandler
  private val fallbackPolicy: StreamingShuffleFallbackPolicy = components.fallbackPolicy
  private val errorNotifier: StreamingShuffleErrorNotifier = components.errorNotifier

  // ---------------------------------------------------------------------------------------------
  // Shuffle shape, taken from the handle
  // ---------------------------------------------------------------------------------------------

  private val dep = handle.dependency

  private val shuffleId: Int = handle.shuffleId

  /**
   * The reduce partition count, as the handle recorded it at registration.
   *
   * The handle's count and the dependency's partitioner must agree: the first is the divisor
   * of every buffer ceiling and the second decides which partition a record belongs to. A mismatch
   * would let a record be routed to a partition that has no budget, so it is refused here rather
   * than discovered as a corrupt accounting reading much later.
   */
  private val declaredPartitions: Int = {
    val fromPartitioner = dep.partitioner.numPartitions
    require(handle.numPartitions == fromPartitioner,
      s"Streaming shuffle $shuffleId was registered for ${handle.numPartitions} partitions but " +
        s"its partitioner reports $fromPartitioner; the registered count is the divisor of the " +
        "per-partition buffer ceiling and must match the partitioner exactly")
    fromPartitioner
  }

  /**
   * The partition count used as a divisor and as a registration argument, floored at one.
   *
   * A shuffle with no reduce partitions writes nothing, but the buffer arithmetic still divides by
   * this figure and the spill manager still refuses a non-positive count, so the degenerate case is
   * normalised here once instead of guarded at every division. No record can reach a partition
   * that does not exist: `appendRecord` bounds-checks against [[declaredPartitions]], not against
   * this.
   */
  private val partitionDivisor: Int = math.max(1, declaredPartitions)

  // ---------------------------------------------------------------------------------------------
  // Configuration, read exactly once (G5). Holding these immutably is what makes "streaming shuffle
  // configuration changes require an executor restart" true by construction rather than by promise,
  // and is why no dynamic reconfiguration path exists or is needed.
  // ---------------------------------------------------------------------------------------------

  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  private val bufferSizePercent: Int = conf.get(config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)

  private val spillThresholdPercent: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)

  // ---------------------------------------------------------------------------------------------
  // Task attributes
  // ---------------------------------------------------------------------------------------------

  /**
   * The flush ordering this task's blocks are queued under.
   *
   * This is the whole of what "prioritise shuffle traffic over speculative task execution" can mean
   * in a system that marks no packets anywhere: blocks of an original attempt are flushed ahead of
   * blocks of a retry or a speculative copy, using the attempt attributes `TaskContext` publishes.
   * No OS-level or network-level quality of service is configured anywhere in this repository, and
   * none is attempted here.
   */
  private val egressPriority: StreamingShuffleServerHandler.EgressPriority =
    StreamingShuffleServerHandler.EgressPriority(
      context.stageId(),
      context.stageAttemptNumber(),
      context.taskAttemptId(),
      context.attemptNumber())

  /**
   * The block manager, dereferenced eagerly.
   *
   * A writer is only ever built on a task thread, long after `SparkEnv` and the memory manager
   * exist, so this is safe here in exactly the way it is safe at `SortShuffleWriter`'s equivalent
   * line. The constructor-ordering hazard that forbids eager `SparkEnv` access belongs to the
   * manager, which is built while `SparkEnv` is still being assembled.
   */
  private val blockManager = SparkEnv.get.blockManager

  /**
   * The one serializer instance every partition's stream is built from.
   *
   * A single instance shared by concurrently open streams is the established shuffle-writer
   * precedent: `BypassMergeSortShuffleWriter` opens one writer per reduce partition from one
   * `SerializerInstance`. It is safe here for the same reason it is safe there -- all of them are
   * driven from one thread -- and it avoids paying the per-instance construction cost, which for
   * Kryo is substantial, once per partition.
   */
  private lazy val serializerInstance: SerializerInstance = dep.serializer.newInstance()

  // -------------------------------------------------------------------------------------------
  // Memory budget (FR-2, FR-5)
  // -------------------------------------------------------------------------------------------

  /**
   * The largest block payload this writer will ever frame, in bytes.
   *
   * Three ceilings apply and the smallest wins, because a block that exceeds any one of them can
   * never be sent:
   *
   *  - the protocol's own cap, [[DataBlockMessage.MAX_BLOCK_SIZE_BYTES]], which is what makes
   *    pipelining possible and is enforced on both encode and decode;
   *  - a [[StreamingShuffleWriter.TARGET_PIPELINE_DEPTH]] share of the per-partition buffer
   *    allowance, once the per-block retention overhead is charged. See below for why a share
   *    rather than the whole allowance;
   *  - what the per-partition allowance leaves outright, published by
   *    [[MemorySpillManager.maxAdmissiblePayloadBytes]]. This is a hard admissibility ceiling:
   *    framing above it would produce a block that no amount of eviction could ever make room for;
   *  - the token bucket's capacity, published by [[TokenBucketRateLimiter.maxAcquirableBytes]] and
   *    measured in framed bytes, so the framing overhead is subtracted from it. A producer that
   *    frames to this ceiling is never refused for size, which is precisely the use that API
   *    prescribes. An unlimited limiter reports `Long.MaxValue` and imposes nothing.
   *
   * The per-partition allowance behind the middle two ceilings is `(executorMemory *
   * bufferSizePercent / 100) / numPartitions`, computed by the spill manager, which also owns the
   * reservation that enforces it.
   *
   * Why a share of the allowance and not all of it. Framing one block to a partition's entire
   * allowance gives that partition a pipeline depth of exactly one: the block being filled IS the
   * whole allowance, so the moment every partition holds one block the aggregate reservation is
   * 100% of the budget by construction, independently of how much data the stage produces. That has
   * three bad consequences. Utilisation would sit permanently above the `spillThreshold`, making
   * spill the steady state rather than the exceptional condition it is specified to be; every block
   * after the first for each partition could only be admitted by first evicting one, so throughput
   * would be governed by disk rather than by the network; and a producer would have no retention
   * window to retransmit from, because the only block it holds is the one it has not sent yet.
   * Reserving depth instead means a partition can hold one block being filled plus a retained
   * window awaiting acknowledgement, which is the pipelining the protocol's block cap exists to
   * enable.
   *
   * Zero is a legitimate answer: it means the configured budget cannot accommodate even one block
   * for one partition, so streaming is not viable for this shuffle and the memory-pressure trip
   * condition is the correct response.
   *
   * Resolved lazily because it reaches through `SparkEnv` into the memory manager, and forced at
   * the head of [[write]] so that the derivation is logged once per task and any failure surfaces
   * before a single record has been buffered.
   */
  private lazy val blockPayloadCapacity: Int = {
    val protocolCeiling = DataBlockMessage.MAX_BLOCK_SIZE_BYTES.toLong
    // A share of the allowance, so a partition holds a block being filled plus a retained window.
    val pipelineCeiling = spillManager.perPartitionBudgetBytes / TARGET_PIPELINE_DEPTH -
      MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES
    // The hard admissibility ceiling: never frame a block eviction could not make room for.
    val budgetCeiling = math.min(pipelineCeiling, spillManager.maxAdmissiblePayloadBytes)
    // An unlimited limiter reports Long.MaxValue, so the subtraction below cannot overflow.
    val paceCeiling = rateLimiter.maxAcquirableBytes - DataBlockMessage.FRAMING_OVERHEAD_BYTES
    val resolved = math.min(protocolCeiling, math.min(budgetCeiling, paceCeiling))
    if (resolved <= 0L) {
      // Trip condition 2: the reservation this shuffle needs can never be satisfied, so streaming
      // is stood down for the executor and the retry of this task lands on the sort-based path.
      fallbackPolicy.recordAllocationGrant(
        protocolCeiling + DataBlockMessage.FRAMING_OVERHEAD_BYTES, math.max(0L, resolved))
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} cannot frame a block: " +
        log"the buffer budget of ${MDC(MEMORY_SIZE, spillManager.totalBudgetBytes)} bytes leaves " +
        log"${MDC(NUM_BYTES, budgetCeiling)} bytes per block across " +
        log"${MDC(NUM_PARTITIONS, partitionDivisor)} partitions, and egress pacing allows " +
        log"${MDC(THRESHOLD, paceCeiling)} bytes; falling back to sort-based shuffle")
      throw new SparkException(s"Streaming shuffle $shuffleId cannot frame a block within a " +
        s"per-partition buffer allowance of ${spillManager.perPartitionBudgetBytes} bytes; " +
        s"increase ${config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT.key} or reduce the partition " +
        "count. The task will be retried on the sort-based shuffle path.")
    }
    resolved.toInt
  }

  // -------------------------------------------------------------------------------------------
  // Mutable state. Every field below is touched only from the task thread.
  // -------------------------------------------------------------------------------------------

  /**
   * Per-partition egress state, indexed by reduce partition id, allocated lazily on first use.
   *
   * An array rather than a map: the partition id is already a dense index, so this is the cheapest
   * possible lookup on the per-record path, and [[activePartitions]] keeps iteration proportional
   * to the partitions this task actually produced rather than to the partition count of the
   * shuffle.
   */
  private val partitionStates = new Array[PartitionEgressState](declaredPartitions)

  /** Ids of the partitions [[partitionStates]] holds a state for, in first-touch order. */
  private val activePartitions = new mutable.ArrayBuffer[Int](
    math.min(math.max(declaredPartitions, 1), INITIAL_ACTIVE_PARTITION_CAPACITY))

  /**
   * Bytes of payload streamed per reduce partition.
   *
   * Allocated eagerly and zero-filled so that [[getPartitionLengths]] is never `null`, including
   * for a task that fails before writing anything.
   */
  private val partitionLengths: Array[Long] = new Array[Long](math.max(0, declaredPartitions))

  private var initialized: Boolean = false

  private var finished: Boolean = false

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var serializationReleased: Boolean = false

  private var egressReleased: Boolean = false

  private var mapStatus: MapStatus = null

  private var recordsAppended: Long = 0L

  private var recordsSinceMaintenance: Int = 0

  private var blocksStreamedTotal: Long = 0L

  private var payloadBytesStreamedTotal: Long = 0L

  private var lastMaintenanceMillis: Long = 0L

  private var lastHeartbeatMillis: Long = 0L

  private var throughputWindowOpenedMillis: Long = 0L

  private var throughputWindowBaseBytes: Long = 0L

  private var admissionRetries: Long = 0L

  private var consumerStalls: Long = 0L

  private var replayAttemptsTotal: Long = 0L

  private var spillsObservedTotal: Long = 0L

  private var reclamationBreaches: Long = 0L

  private var memoryPressureBrushes: Long = 0L

  // Monotonic high-water marks of what has already been forwarded to the task's metrics reporter.
  // Only the increment since the last publication is forwarded, which is what lets progress made on
  // an I/O thread be reported from the task thread without ever double-counting.
  private var publishedBytesWritten: Long = 0L

  private var publishedRecordsWritten: Long = 0L

  // -------------------------------------------------------------------------------------------
  // ShuffleWriter contract
  // -------------------------------------------------------------------------------------------

  /**
   * Streams this task's records to their consumers.
   *
   * The loop is deliberately flat: append, and at a block boundary cut, admit, hand to egress and
   * check for anything that has happened on an I/O thread since. Maintenance -- acknowledgement
   * drainage, utilisation reporting, spill polling, heartbeats and the consumer-liveness check --
   * is amortised behind a 100 ms cadence measured on the injected clock, so a shuffle of tiny
   * records pays for it once every hundred milliseconds rather than once per record.
   *
   * @param records this task's output, in the order the upstream operator produced it
   * @throws IOException if a serialization stream or the underlying channel fails
   */
  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // One writer streams one map task's output exactly once. Refusing a second call is not
    // pedantry: every partition's serialization stream has been closed and its end-of-stream
    // signalled by the time the first call returns, so appending to it would discard the records
    // silently and produce a shuffle that is short of data with nothing anywhere reporting it.
    if (finished) {
      throw new IllegalStateException(s"Streaming shuffle $shuffleId map $mapId has already " +
        "written its output; a shuffle writer streams one record iterator exactly once.")
    }
    try {
      initializeStreaming()
      while (records.hasNext) {
        val record = records.next()
        appendRecord(record._1, record._2)
      }
      finishAllPartitions()
      finished = true
    } catch {
      case NonFatal(e) =>
        // A failure already observed on an I/O thread is the more precise diagnosis of what went
        // wrong, so it wins, and this one is attached to it as suppressed so that nothing is lost.
        // When there is no such failure this exception propagates exactly as it stands rather than
        // being round-tripped through the notifier: the notifier wraps a checked exception -- which
        // SparkException is -- in a message worded for the consuming side, which would replace a
        // precise producer-side diagnosis with a misleading one.
        if (errorNotifier.hasError) {
          errorNotifier.setError(e)
          errorNotifier.throwIfError()
        }
        throw e
    }
  }

  /**
   * Closes this writer.
   *
   * On success the returned status is always non-empty, because
   * [[org.apache.spark.shuffle.ShuffleWriteProcessor]] dereferences it unconditionally. It is a
   * placeholder in the sense that it describes bytes that were streamed rather than bytes that are
   * resident in a local data file, and it exists so that the scheduler's bookkeeping is satisfied
   * without implying materialised output. It is produced by [[MapStatus.apply]] because `MapStatus`
   * is a sealed trait, so no new subtype could be declared here even if one were wanted.
   *
   * A second call is a no-op that returns `None`. That is not defensive padding: the caller invokes
   * `stop(success = true)` on the happy path and `stop(success = false)` from its catch block, so a
   * writer without the guard would release its resources twice.
   *
   * @param success whether the map task completed successfully
   * @return the map status on success, `None` otherwise
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      return None
    }
    stopping = true
    val startedAtNanos = System.nanoTime()
    try {
      if (success) {
        // A best-effort, non-blocking last pass: push whatever is queued, apply whatever has been
        // acknowledged, and publish the final counters. Waiting for consumers here would hold the
        // task thread, and a consumer that never arrives is recovered by the reader failing the
        // fetch and the unmodified scheduler recomputing this stage.
        drainWithoutWaiting()
        publishWriteMetrics()
        mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
        logStreamingSummary()
        Option(mapStatus)
      } else {
        // The task has failed, so nothing retained for retransmission can ever be asked for again.
        // Release it now rather than at task completion: this is the path FR-2 requires to free all
        // buffers and delete all spill files.
        releaseEgressResources()
        closeSpillState()
        None
      }
    } finally {
      closeSerializationResources()
      writeMetrics.incWriteTime(System.nanoTime() - startedAtNanos)
    }
  }

  /**
   * Bytes of payload streamed per reduce partition.
   *
   * Never `null`: the array is allocated and zero-filled at construction, so a task that fails
   * before producing anything still reports a well-formed, all-zero length vector.
   */
  override def getPartitionLengths(): Array[Long] = partitionLengths


  // -------------------------------------------------------------------------------------------
  // Initialisation
  // -------------------------------------------------------------------------------------------

  /**
   * Prepares the streaming path, exactly once per writer.
   *
   * The order of the calls below is load-bearing and is documented rather than merely observed:
   *
   *  1. The reduce partition count is registered with the spill manager first, because it is the
   *     divisor of every buffer ceiling. Reading a budget before registering it would divide by the
   *     unregistered default of one and return an allowance an order of magnitude too generous.
   *  2. The default acknowledgement consumer is registered next. That identity enjoys no privilege
   *     in the retention protocol: without registration it may not acknowledge, and nothing would
   *     ever be released.
   *  3. The spill manager's own cleanup is registered before this writer's, because task completion
   *     listeners are invoked in reverse registration order. Registering this writer's listener
   *     second means it runs first, so egress is released while the buffers it referenced are still
   *     alive rather than after they have been freed.
   *  4. The shuffle is registered with the backpressure protocol before any utilisation is
   *     reported, because reports for an unregistered shuffle are discarded.
   *  5. The attempt's flush ordering is registered with the handler before the first block is
   *     queued, so that no block is ever queued under the default ordering.
   *  6. The protocol version the handle was registered under is checked explicitly. This is trip
   *     condition 4, detected by an explicit compatibility check rather than inferred from a decode
   *     failure, so a rolling upgrade degrades deterministically.
   *  7. The buffer budget is forced last, because it reaches through `SparkEnv` and may refuse.
   */
  private def initializeStreaming(): Unit = {
    if (!initialized) {
      initialized = true
      spillManager.registerPartitionCount(partitionDivisor)
      spillManager.registerConsumer(MemorySpillManager.DEFAULT_CONSUMER_ID)
      spillManager.registerCleanup(context)
      context.addTaskCompletionListener[Unit](_ => releaseOnTaskCompletion())
      backpressure.registerShuffle(shuffleId, partitionDivisor)
      serverHandler.registerTaskAttempt(egressPriority)
      if (!fallbackPolicy.checkProtocolVersion(handle.protocolVersion)) {
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} was registered under " +
          log"protocol version ${MDC(VALUE, handle.protocolVersion)}, which this executor cannot " +
          log"speak; streaming has been stood down for this executor")
      }
      val nowMillis = clock.getTimeMillis()
      lastMaintenanceMillis = nowMillis
      lastHeartbeatMillis = nowMillis
      throughputWindowOpenedMillis = nowMillis
      // Forces the derivation, so it either succeeds and is logged once, or refuses before a single
      // record has been buffered and while there is still nothing to unwind.
      val capacity = blockPayloadCapacity
      verifyBudgetContract()
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
        log"${MDC(TASK_ATTEMPT_ID, mapId)} streams ${MDC(NUM_PARTITIONS, declaredPartitions)} " +
        log"partitions with a buffer budget of " +
        log"${MDC(MEMORY_SIZE, spillManager.totalBudgetBytes)} bytes " +
        log"(${MDC(VALUE, bufferSizePercent)}% of executor memory), " +
        log"${MDC(NUM_BYTES, spillManager.perPartitionBudgetBytes)} bytes per partition, " +
        log"spilling at ${MDC(THRESHOLD, spillThresholdPercent)}% and framing blocks of at most " +
        log"${MDC(COUNT, capacity)} bytes")
    }
  }

  /**
   * Checks that the allowance the spill manager publishes really is the specified quotient.
   *
   * The formula `(executorMemory * bufferSizePercent / 100) / numPartitions` is a requirement of
   * the feature, not an implementation detail of whoever computes it, so the one component that
   * consumes the quotient verifies it rather than assuming it. The floor of one byte the spill
   * manager applies to a degenerate quotient is accounted for, because a budget smaller than the
   * partition count legitimately rounds to zero before the floor is applied.
   */
  private def verifyBudgetContract(): Unit = {
    val expected = math.max(1L, spillManager.totalBudgetBytes / partitionDivisor)
    val published = spillManager.perPartitionBudgetBytes
    require(published == expected,
      s"The streaming shuffle per-partition buffer allowance must be the aggregate budget of " +
        s"${spillManager.totalBudgetBytes} bytes divided by $partitionDivisor partitions, i.e. " +
        s"$expected bytes, but $published bytes were published")
  }

  // -------------------------------------------------------------------------------------------
  // Record path
  // -------------------------------------------------------------------------------------------

  /**
   * Appends one record to its partition's stream, emitting blocks as boundaries are crossed.
   *
   * Both arguments are widened to `Any`, which is what `DiskBlockObjectWriter` does for the same
   * reason: [[SerializationStream.writeKey]] needs a `ClassTag`, and no `ClassTag` for the abstract
   * key and value types of a shuffle is in scope anywhere on this path.
   *
   * The partition index is bounds-checked. A `Partitioner` is contractually obliged to return an
   * index within range, but a user-supplied one may not, and an out-of-range index would otherwise
   * be charged to a partition that has no budget and no consumer.
   */
  private def appendRecord(key: Any, value: Any): Unit = {
    val partitionId = dep.partitioner.getPartition(key)
    if (partitionId < 0 || partitionId >= declaredPartitions) {
      throw new SparkException(s"Partitioner ${dep.partitioner.getClass.getName} returned " +
        s"partition $partitionId for shuffle $shuffleId, which declares $declaredPartitions " +
        "partitions; a partitioner must return an index in [0, numPartitions)")
    }
    val state = stateFor(partitionId)
    state.stream.writeKey(key)
    state.stream.writeValue(value)
    state.recordsAppended += 1L
    recordsAppended += 1L
    recordsSinceMaintenance += 1
    if (state.accumulator.size >= blockPayloadCapacity) {
      // Flushing pushes whatever the serializer is holding into the accumulator, so the boundary is
      // measured against bytes that really exist rather than against bytes the serializer might
      // still be buffering. The stream itself is never closed here: one partition is one continuous
      // serialization stream, and closing it per block would emit a fresh header into every block.
      state.stream.flush()
      emitFullBlocks(state)
    }
    if (recordsSinceMaintenance >= MAINTENANCE_RECORD_INTERVAL) {
      // A shuffle of tiny records may go a long time between block boundaries, and heartbeats,
      // acknowledgement drainage and the liveness check must not wait for one. The clock is only
      // read once per interval, so the amortised cost per record is negligible.
      maintainStreams()
    }
  }

  /** Returns the egress state of one partition, opening its stream on first use. */
  private def stateFor(partitionId: Int): PartitionEgressState = {
    val existing = partitionStates(partitionId)
    if (existing != null) {
      existing
    } else {
      val state = new PartitionEgressState(partitionId, blockPayloadCapacity)
      state.stream = serializerInstance.serializeStream(state.accumulator)
      partitionStates(partitionId) = state
      activePartitions += partitionId
      // A stream must be registered before any of its blocks are admitted: admission for an
      // unregistered stream fails open, which would leave this partition's egress unaccounted and
      // therefore unpaced.
      backpressure.registerStream(shuffleId, partitionId, spillManager.perPartitionBudgetBytes)
      state
    }
  }

  /** Cuts and emits every whole block the accumulator now holds. */
  private def emitFullBlocks(state: PartitionEgressState): Unit = {
    while (state.accumulator.size >= blockPayloadCapacity) {
      emitBlock(state, blockPayloadCapacity)
    }
  }

  /**
   * Cuts `length` bytes off the front of a partition's accumulator and streams them as one block.
   *
   * The order of the two hand-offs is the memory bound. The block is offered to the spill manager
   * first, so aggregate buffered bytes are checked, spill is triggered if the threshold has been
   * crossed and the retention window is extended, all before the bytes are queued for the wire.
   * Reversing the order would let bytes leave that no budget had ever admitted.
   *
   * The sequence number is asserted rather than trusted. Both the spill manager's retention window
   * and the handler's egress queue number a partition's blocks densely from zero, and the receiving
   * reader reassembles on that assumption, so a divergence is a genuine protocol violation -- the
   * signature of a server handler shared between two concurrent producers of the same shuffle --
   * and is reported as one instead of silently corrupting a stream.
   */
  private def emitBlock(state: PartitionEgressState, length: Int): Unit = {
    val sequenceNumber = state.nextSequenceNumber
    // Only the framing, admission and hand-off are charged as shuffle write time, and only these.
    // Timing the whole of write() would charge the upstream operator's compute time to shuffle
    // write, and the sort-based path is careful about exactly this: `DiskBlockObjectWriter` charges
    // write time around its commit rather than around each record's serialization, and
    // `SortShuffleWriter` charges it around its sorter's teardown. The maintenance and telemetry
    // hooks below are deliberately outside the timed region, because they are neither.
    val startedAtNanos = System.nanoTime()
    val assigned = try {
      val payload = state.accumulator.take(length)
      admitBlock(state, sequenceNumber, payload)
      serverHandler.enqueueBlock(state.partitionId, payload)
    } finally {
      writeMetrics.incWriteTime(System.nanoTime() - startedAtNanos)
    }
    if (assigned != sequenceNumber) {
      throw StreamingShuffleErrors.invalidSequenceNumber(
        shuffleId, state.partitionId, sequenceNumber, assigned)
    }
    state.nextSequenceNumber = sequenceNumber + 1L
    state.payloadBytesStreamed += length.toLong
    partitionLengths(state.partitionId) += length.toLong
    payloadBytesStreamedTotal += length.toLong
    blocksStreamedTotal += 1L
    if (debugEnabled) {
      logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, state.partitionId)} streamed block " +
        log"${MDC(VALUE, sequenceNumber)} of ${MDC(NUM_BYTES, length)} bytes")
    }
    // A block boundary is the natural point to notice everything that has happened elsewhere since
    // the last one: an I/O failure, a fallback trip, an acknowledgement, a stalled consumer.
    errorNotifier.throwIfError()
    maintainStreams()
    checkFallbackPolicy()
  }

  /**
   * Reserves budget for one block, evicting and retrying until either the reservation succeeds or a
   * round demonstrably frees nothing.
   *
   * A refusal from [[MemorySpillManager.bufferBlock]] already means eviction was attempted and
   * could not free enough, so the recovery here does the only three things that can change that
   * verdict without waiting: push queued bytes towards the wire, apply whatever the consumer has
   * acknowledged since, and force an eviction pass. None of them sleeps, which is why the whole
   * path is deterministic under a manual clock.
   *
   * Why the loop is driven by observed progress rather than by a fixed round count. A refusal is
   * raised against the *per-partition* allowance, but eviction chooses its victim executor-wide, in
   * largest-first order. Those are not the same partition: while some other partition is larger, an
   * eviction pass frees that one instead, and the partition actually waiting for room is untouched.
   * A fixed number of rounds therefore fails whenever the number of larger partitions happens to
   * exceed it -- not because the budget is exhausted, but because relief kept landing elsewhere.
   * Retrying while rounds keep freeing bytes converges instead: this partition is at its own
   * ceiling, so it is by definition among the largest, and every pass shrinks whichever partition
   * was ahead of it, so it is selected within at most one pass per larger partition. Termination is
   * bounded twice over -- by the barren-round tolerance below, and by the fact that the budget
   * holds a finite number of evictable blocks -- so a genuinely exhausted budget is still diagnosed
   * promptly rather than retried forever.
   *
   * Why an in-flight eviction is not counted against that tolerance. Eviction is not this thread's
   * exclusive activity: the spill manager polls on its own contracted cadence, and the task memory
   * manager invokes the spill callback from whichever thread is under pressure. While such a write
   * is in flight the manager offers no candidate for the affected partition and plans no new
   * eviction, so a round can free nothing at the very moment memory is actively being reclaimed.
   * Counting that as exhaustion would fail a task that was about to have room. Those rounds are
   * therefore deferred instead, up to [[StreamingShuffleWriter.MAX_EVICTION_DEFERRALS]], which
   * bounds the wait so a stalled evictor cannot spin this thread indefinitely.
   *
   * When the tolerance is exhausted the reservation has genuinely been prevented, which is trip
   * condition 2 exactly as specified -- memory pressure preventing a buffer allocation -- so the
   * fallback policy is told, streaming stands down for the executor, and this task fails so that
   * its retry lands on the sort-based path. Failing here is what makes the memory bound real: the
   * alternative would be to stream bytes no budget admitted.
   */
  /**
   * True when the spill manager has detached some partition's blocks for a write that has not
   * landed yet, which is what another thread's eviction looks like from the outside.
   *
   * Detected rather than asked, because the manager exposes no in-flight flag: a partition that
   * holds memory bytes yet is absent from the eviction candidate order has by definition had those
   * blocks detached by an eviction already under way. Only partitions this writer actually opened a
   * stream for are examined, so the scan costs nothing on a shuffle whose partitions are mostly
   * untouched, and it runs only on a round that freed nothing -- never on the record path.
   */
  private def evictionInFlight(): Boolean = {
    val candidates = spillManager.spillSelectionOrder.toSet
    var partitionId = 0
    var detached = false
    while (partitionId < partitionStates.length && !detached) {
      if (partitionStates(partitionId) != null &&
        !candidates.contains(partitionId) &&
        spillManager.bufferedBytesFor(partitionId) > 0L) {
        detached = true
      }
      partitionId += 1
    }
    detached
  }

  private def admitBlock(
      state: PartitionEgressState,
      sequenceNumber: Long,
      payload: Array[Byte]): Unit = {
    var admitted = spillManager.bufferBlock(state.partitionId, sequenceNumber, payload)
    var round = 0
    var barrenRounds = 0
    var deferrals = 0
    while (!admitted && barrenRounds < MAX_ADMISSION_RECOVERY_ROUNDS) {
      // Measured before the pass so that "did this round free anything" is an observation rather
      // than an assumption about which partition eviction chose.
      val partitionBytesBefore = spillManager.bufferedBytesFor(state.partitionId)
      val aggregateBytesBefore = spillManager.bufferedBytes
      round += 1
      admissionRetries += 1L
      serverHandler.flushPending()
      drainAcknowledgements()
      if (spillManager.maybeSpill()) {
        recordSpillObserved()
      }
      admitted = spillManager.bufferBlock(state.partitionId, sequenceNumber, payload)
      if (!admitted) {
        val freedHere = partitionBytesBefore - spillManager.bufferedBytesFor(state.partitionId)
        val freedOverall = aggregateBytesBefore - spillManager.bufferedBytes
        if (freedHere > 0L || freedOverall > 0L) {
          // Relief landed somewhere. Whichever partition it landed on is now smaller, which moves
          // this one closer to the front of the eviction order.
          barrenRounds = 0
        } else if (deferrals < MAX_EVICTION_DEFERRALS && evictionInFlight()) {
          // Memory is actively being reclaimed by another thread; this round freeing nothing says
          // nothing about whether the budget is exhausted. Wait for that write to land instead of
          // failing a task that is about to have room.
          deferrals += 1
          // Hand the CPU to whichever thread is doing the reclaiming. Spinning without yielding
          // starves the very eviction being waited for, which is not a theoretical concern: an
          // unyielded run of these rounds was measured completing in about 5 ms while the eviction
          // it was waiting for landed 17 ms later, so the wait expired before the work it existed
          // to wait for could possibly finish. This is a yield and not a sleep, so the loop stays
          // free of wall-clock timing and remains deterministic under an injected clock.
          Thread.`yield`()
        } else {
          barrenRounds += 1
        }
      }
    }
    if (!admitted) {
      val requested = payload.length.toLong + MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES
      fallbackPolicy.recordAllocationGrant(requested, 0L)
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not reserve " +
        log"${MDC(MEMORY_SIZE, requested)} bytes for partition " +
        log"${MDC(PARTITION_ID, state.partitionId)} after ${MDC(MAX_ATTEMPTS, round)} eviction " +
        log"attempts, holding ${MDC(NUM_BYTES, spillManager.bufferedBytes)} of " +
        log"${MDC(THRESHOLD, spillManager.totalBudgetBytes)} budgeted bytes; falling back to " +
        log"sort-based shuffle")
      throw new SparkException(s"Streaming shuffle $shuffleId could not reserve $requested bytes " +
        s"for partition ${state.partitionId} even after eviction; streaming shuffle has been " +
        "stood down on this executor and the task will be retried on the sort-based path.")
    }
    if (round > 0 && spillManager.memoryPressureDetected) {
      // Pressure that eviction rescued is a brush, not a prevented allocation, so it is counted and
      // the sticky flag is re-armed rather than escalated. Only a refusal above trips the policy.
      memoryPressureBrushes += 1L
      spillManager.clearMemoryPressure()
      if (debugEnabled) {
        logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} recovered a buffer " +
          log"reservation for partition ${MDC(PARTITION_ID, state.partitionId)} after " +
          log"${MDC(COUNT, round)} eviction attempts")
      }
    }
  }


  // -------------------------------------------------------------------------------------------
  // Maintenance: flow control, spill, liveness and the consumer-failure flow (FR-3, FR-5, FR-9)
  // -------------------------------------------------------------------------------------------

  /**
   * Runs the periodic streaming duties, at most once per [[MAINTENANCE_INTERVAL_MS]].
   *
   * Everything here is proportional to the number of partitions this task has touched rather than
   * to the number of records it has written, and it is behind a clock gate, so the per-record cost
   * of flow control, spill polling, liveness and telemetry is amortised to nothing. That is how the
   * feature's sub-one-percent telemetry overhead is met: no counter in this method is per-record.
   *
   * The record counter is reset on every call, gate or no gate, so the caller's cheap integer test
   * remains the thing that decides when the clock is read.
   */
  private def maintainStreams(): Unit = {
    recordsSinceMaintenance = 0
    val nowMillis = clock.getTimeMillis()
    if (nowMillis - lastMaintenanceMillis >= MAINTENANCE_INTERVAL_MS) {
      lastMaintenanceMillis = nowMillis
      drainAcknowledgements()
      backpressure.reportBufferUtilization(
        shuffleId, spillManager.bufferedBytes, spillManager.totalBudgetBytes)
      pollSpill()
      sendHeartbeatsIfDue(nowMillis)
      handleConsumerStalls(nowMillis)
      reportProducerThroughput(nowMillis)
      // Lets the protocol advance its own timers even when no message has arrived, which is what
      // makes an acknowledgement gap observable rather than merely inferable.
      backpressure.pollOnce()
      publishWriteMetrics()
    }
  }

  /**
   * Applies whatever consumers have acknowledged, releasing the buffers those acknowledgements
   * free.
   *
   * Acknowledgements arrive on a Netty thread, which records them against the handler's stream
   * state; they are applied to the memory budget here, on the task thread, because releasing memory
   * is a change to the task's own reservation. The latency of the release is confirmed back to the
   * backpressure protocol, which is the component that can see both ends of the 100 ms reclamation
   * bound: the acknowledgement opened the window on an I/O thread and this call closes it.
   */
  private def drainAcknowledgements(): Unit = {
    var index = 0
    while (index < activePartitions.length) {
      val state = partitionStates(activePartitions(index))
      val position = serverHandler.acknowledgedPosition(state.partitionId)
      if (position >= 0L && position > state.appliedAckPosition) {
        val released = spillManager.acknowledge(state.partitionId, position)
        state.appliedAckPosition = position
        // Progress means the stream is healthy again, so the replay budget is restored in full
        // rather than left partly spent for an unrelated later failure to inherit.
        state.replayAttempts = 0
        state.nextReplayAtMillis = 0L
        state.stallReported = false
        backpressure.confirmReclamation(shuffleId, state.partitionId).foreach { latencyMillis =>
          if (latencyMillis > RECLAMATION_DEADLINE_MS) {
            reclamationBreaches += 1L
            if (reclamationBreaches == 1L) {
              logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} freed buffers for " +
                log"partition ${MDC(PARTITION_ID, state.partitionId)} " +
                log"${MDC(DURATION, latencyMillis)} ms after the acknowledgement freeing them, " +
                log"outside the ${MDC(THRESHOLD, RECLAMATION_DEADLINE_MS)} ms bound")
            }
          }
        }
        if (debugEnabled && released > 0L) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
            log"${MDC(PARTITION_ID, state.partitionId)} released ${MDC(NUM_BYTES, released)} " +
            log"bytes on acknowledgement through ${MDC(VALUE, position)}")
        }
      }
      index += 1
    }
  }

  /**
   * Gives the spill manager its polling opportunity and honours the executor-wide spill verdict.
   *
   * Two triggers, and both are required. The first is this task's own utilisation, polled on the
   * cadence the spill manager enforces internally. The second is the aggregate reading across every
   * shuffle registered on this executor: a task whose own buffers are comfortable must still yield
   * when the executor as a whole has crossed the threshold, which is precisely the cross-shuffle
   * monitoring the backpressure protocol exists to provide.
   */
  private def pollSpill(): Unit = {
    if (spillManager.pollOnce()) {
      recordSpillObserved()
    } else if (backpressure.isSpillRequired && spillManager.maybeSpill()) {
      recordSpillObserved()
    }
  }

  /** Notices that a spill happened, for this writer's own diagnostics and log. */
  private def recordSpillObserved(): Unit = {
    val observed = spillManager.spillCount
    if (observed > spillsObservedTotal) {
      spillsObservedTotal = observed
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} spilled to disk, now at " +
        log"${MDC(COUNT, observed)} spills holding ${MDC(NUM_BYTES, spillManager.bufferedBytes)} " +
        log"of ${MDC(THRESHOLD, spillManager.totalBudgetBytes)} budgeted bytes")
    }
  }

  /**
   * Sends a producer heartbeat on every active stream when one is due.
   *
   * The five-second liveness bound the feature specifies is enforced at application level by this
   * timer, not by TCP keep-alive: the transport exposes keep-alive as a boolean only, and the JDK
   * offers no way to set the keep-alive interval, so the operating system's own interval is
   * whatever the host is configured for. Enabling keep-alive for the streaming module and
   * heartbeating here are complementary, and only the latter is bounded at five seconds.
   */
  private def sendHeartbeatsIfDue(nowMillis: Long): Unit = {
    if (nowMillis - lastHeartbeatMillis >= HEARTBEAT_INTERVAL_MS) {
      lastHeartbeatMillis = nowMillis
      var index = 0
      while (index < activePartitions.length) {
        val state = partitionStates(activePartitions(index))
        if (!state.terminated) {
          serverHandler.sendHeartbeat(state.partitionId)
        }
        index += 1
      }
    }
  }

  /**
   * Implements the consumer-failure flow.
   *
   * A stream is stalled when it holds unacknowledged blocks and has seen no acknowledgement
   * progress for the ten-second liveness window. The response is exactly the specified sequence:
   * retain the unacknowledged window, spill it once utilisation has crossed the threshold, and
   * replay it when the consumer comes back, with an exponential backoff from one second and at most
   * five attempts.
   *
   * Retention needs no action here, and saying why matters: the window is retained by construction,
   * both in the spill manager -- which releases a block only when every registered consumer has
   * acknowledged past it -- and in the handler, which keeps a written block until it is
   * acknowledged. The only decisions left are when to spill and when to replay.
   */
  private def handleConsumerStalls(nowMillis: Long): Unit = {
    val stalled = serverHandler.stalledPartitions
    if (stalled.nonEmpty) {
      if (spillManager.bufferUtilizationPercent >= spillThresholdPercent.toLong) {
        if (spillManager.maybeSpill()) {
          recordSpillObserved()
        }
      }
      stalled.foreach(partitionId => replayUnacknowledgedWindow(partitionId, nowMillis))
    }
  }

  /**
   * Replays one stalled stream's unacknowledged window, or escalates when the budget is spent.
   *
   * Replay is bounded to the unacknowledged window and nothing wider, which is what keeps the
   * zero-data-loss guarantee compatible with releasing memory on acknowledgement: bytes that have
   * been acknowledged are gone precisely because they are known to have arrived. The handler serves
   * the replay from memory or from spill, whichever holds the block.
   *
   * When the attempts are exhausted the consumer is treated as lost. The task is failed by way of
   * the error notifier so that the earliest failure wins over this one if an I/O thread already had
   * a more precise diagnosis. Failing the task is the correct escalation and the only one available
   * here: `FetchFailedException` is the reader's signal, and constructing one on the producer side
   * would mark a fetch failure against a task that is not fetching.
   */
  private def replayUnacknowledgedWindow(partitionId: Int, nowMillis: Long): Unit = {
    if (partitionId >= 0 && partitionId < declaredPartitions) {
      val state = partitionStates(partitionId)
      if (state != null) {
        if (!state.stallReported) {
          state.stallReported = true
          consumerStalls += 1L
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} saw no acknowledgement " +
            log"from the consumer of partition ${MDC(PARTITION_ID, partitionId)} for " +
            log"${MDC(TIMEOUT, CONSUMER_LIVENESS_TIMEOUT_MS)} ms, holding " +
            log"${MDC(NUM_BYTES, serverHandler.unacknowledgedBytes)} unacknowledged bytes")
        }
        if (nowMillis >= state.nextReplayAtMillis) {
          if (state.replayAttempts >= MAX_REPLAY_ATTEMPTS) {
            escalateConsumerLoss(state)
          } else {
            state.replayAttempts += 1
            replayAttemptsTotal += 1L
            state.nextReplayAtMillis = nowMillis + backoffMillisFor(state.replayAttempts)
            val first = math.max(0L, state.appliedAckPosition + 1L)
            val last = state.nextSequenceNumber - 1L
            if (last >= first) {
              val replayed = serverHandler.retransmit(partitionId, first, last)
              logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} replayed " +
                log"${MDC(NUM_BLOCKS, replayed)} unacknowledged blocks of partition " +
                log"${MDC(PARTITION_ID, partitionId)} on attempt " +
                log"${MDC(COUNT, state.replayAttempts)} of " +
                log"${MDC(MAX_ATTEMPTS, MAX_REPLAY_ATTEMPTS)}")
            } else {
              // Nothing is outstanding for this stream, so there is nothing to replay; pushing the
              // queue is still worth doing, because a block may be waiting on pacing rather than on
              // the consumer.
              serverHandler.flushPending()
            }
          }
        }
      }
    }
  }

  /** Fails the task after a lost consumer has exhausted its replay budget. */
  private def escalateConsumerLoss(state: PartitionEgressState): Unit = {
    val failure = new SparkException(s"Streaming shuffle $shuffleId lost the consumer of " +
      s"partition ${state.partitionId}: no acknowledgement advanced past " +
      s"${state.appliedAckPosition} after $MAX_REPLAY_ATTEMPTS retransmission attempts. The task " +
      "is failed so that the unmodified scheduler recomputes it.")
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} failing map " +
      log"${MDC(TASK_ATTEMPT_ID, mapId)} because the consumer of partition " +
      log"${MDC(PARTITION_ID, state.partitionId)} did not return after " +
      log"${MDC(MAX_ATTEMPTS, MAX_REPLAY_ATTEMPTS)} retransmission attempts", failure)
    // The same discipline as write()'s catch, and for the same reason: an earlier I/O failure is
    // the better diagnosis and wins with this one attached to it, and otherwise this exception
    // propagates exactly as constructed instead of being re-worded by the notifier's wrapper.
    if (errorNotifier.hasError) {
      errorNotifier.setError(failure)
      errorNotifier.throwIfError()
    }
    throw failure
  }

  /**
   * Returns the replay backoff for one attempt: one second doubled per attempt, capped.
   *
   * The cap exists so that the fifth attempt of a stream is still made inside a task's lifetime
   * rather than at an interval that grows without bound.
   */
  private def backoffMillisFor(attempt: Int): Long = {
    val shift = math.min(math.max(attempt - 1, 0), MAX_BACKOFF_SHIFT)
    math.min(REPLAY_BASE_BACKOFF_MS << shift, MAX_REPLAY_BACKOFF_MS)
  }

  /**
   * Reports this producer's own egress rate, and its share of the administered link.
   *
   * Only the producer side is reported, and the omission is deliberate. A producer can infer a
   * consumption rate from the acknowledgements it receives, but it cannot distinguish a consumer
   * that is slow from a consumer that has not started, and standing streaming down for a reduce
   * task that has not been scheduled yet would be a false positive that costs every job its fast
   * path. The consumer rate is therefore reported by the consumer, in [[StreamingShuffleReader]],
   * which is the only place it can be measured rather than guessed.
   *
   * The link-utilisation report is the producer's own trip condition. Egress is paced to a fraction
   * of the administered capacity, so exceeding ninety percent of it means the administered figure
   * and the pacing budget disagree, and streaming should stand down until they are reconciled. When
   * no bandwidth cap is administered the reading is not evaluable and nothing is reported.
   */
  private def reportProducerThroughput(nowMillis: Long): Unit = {
    val elapsedMillis = nowMillis - throughputWindowOpenedMillis
    if (elapsedMillis >= THROUGHPUT_WINDOW_MS) {
      val bytes = math.max(0L, payloadBytesStreamedTotal - throughputWindowBaseBytes)
      val bytesPerSecond = (bytes * MILLIS_PER_SECOND) / elapsedMillis
      throughputWindowOpenedMillis = nowMillis
      throughputWindowBaseBytes = payloadBytesStreamedTotal
      fallbackPolicy.recordProducerThroughput(shuffleId, bytesPerSecond.toDouble, nowMillis)
      if (!rateLimiter.isUnlimited) {
        fallbackPolicy.recordLinkUtilization(bytesPerSecond.toDouble)
      }
      if (debugEnabled) {
        logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} produced " +
          log"${MDC(NUM_BYTES, bytesPerSecond)} bytes per second over the last " +
          log"${MDC(DURATION, elapsedMillis)} ms")
      }
    }
  }

  /**
   * Stands down cleanly when graceful degradation has tripped.
   *
   * Every trip routes to the same terminus -- delegation to the unmodified sort-based shuffle --
   * and the way a running map task reaches that terminus is to fail so that its retry is served by
   * the delegate the manager has by then switched to. That is why this throws rather than returning
   * a verdict: there is no correct way to finish a streaming shuffle whose streaming has been
   * withdrawn, and every path must still terminate in a working shuffle.
   *
   * A trip observed after the last block has been terminated is ignored, because at that point the
   * task's output is complete and failing it would discard correct work for no benefit.
   */
  private def checkFallbackPolicy(): Unit = {
    if (!finished && fallbackPolicy.hasTripped) {
      val reason = fallbackPolicy.trippedReason
      val description = reason.map(_.description).getOrElse("an unrecorded condition")
      val name = reason.map(_.toString).getOrElse("Unknown")
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} is standing down at map " +
        log"${MDC(TASK_ATTEMPT_ID, mapId)} because ${MDC(REASON, description)}; the task will be " +
        log"retried on the sort-based shuffle path")
      throw new SparkException(s"Streaming shuffle $shuffleId stopped streaming because " +
        s"$description ($name). The shuffle manager now delegates to sort-based shuffle, so the " +
        "retry of this task will use it.")
    }
  }


  // -------------------------------------------------------------------------------------------
  // End of stream
  // -------------------------------------------------------------------------------------------

  /**
   * Closes every partition's stream and signals end of stream on all of them.
   *
   * Partitions this task produced no record for are terminated too, and that is not a formality: a
   * consumer with no data and no terminator cannot tell a producer that finished from a producer
   * that stopped responding, so it would wait out the five-second producer timeout, invalidate its
   * partial reads and force a stage recomputation on every otherwise successful shuffle. An empty
   * stream's terminator legitimately reports zero blocks.
   */
  private def finishAllPartitions(): Unit = {
    var index = 0
    while (index < activePartitions.length) {
      finishPartition(partitionStates(activePartitions(index)))
      index += 1
    }
    val startedAtNanos = System.nanoTime()
    var partitionId = 0
    while (partitionId < declaredPartitions) {
      if (partitionStates(partitionId) == null) {
        serverHandler.terminateStream(partitionId)
      }
      partitionId += 1
    }
    serverHandler.flushPending()
    writeMetrics.incWriteTime(System.nanoTime() - startedAtNanos)
    drainAcknowledgements()
    publishWriteMetrics()
  }

  /**
   * Flushes and closes one partition's serialization stream, emits its tail and terminates it.
   *
   * The stream is closed before the tail is cut, because closing is what makes the serializer emit
   * whatever trailing state it has been holding. Only after that is the accumulator's remainder
   * known to be the complete end of the partition's byte range.
   */
  private def finishPartition(state: PartitionEgressState): Unit = {
    if (!state.closed) {
      state.stream.flush()
      state.stream.close()
      state.closed = true
      emitFullBlocks(state)
      val remaining = state.accumulator.size
      if (remaining > 0) {
        emitBlock(state, remaining)
      }
      state.accumulator.release()
    }
    if (!state.terminated) {
      serverHandler.terminateStream(state.partitionId)
      state.terminated = true
    }
  }

  /**
   * Pushes and applies whatever can be pushed and applied without waiting for anybody.
   *
   * A failure here is logged rather than raised. At this point the task's records have all been
   * framed, admitted and queued, and whether the last of them reached a consumer is a question only
   * the consumer can answer: it detects a producer that went quiet by its own five-second timeout
   * and recovers by failing the fetch, which the unmodified scheduler resolves by recomputing this
   * stage. Converting a flush failure into a map-task failure here would duplicate that detection
   * less accurately.
   */
  private def drainWithoutWaiting(): Unit = {
    try {
      serverHandler.flushPending()
      drainAcknowledgements()
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not complete its " +
          log"final egress pass for map ${MDC(TASK_ATTEMPT_ID, mapId)}; consumers detect the " +
          log"gap and recover by failing their fetch", e)
    }
  }

  // -------------------------------------------------------------------------------------------
  // Telemetry (FR-7)
  // -------------------------------------------------------------------------------------------

  /**
   * Publishes progress onto the task's standard shuffle-write metrics.
   *
   * Wiring the existing reporter is what makes a streaming shuffle visible on the Web UI, in the
   * history server, in the event log and through the metrics REST API without a single change to
   * any of them; omitting it would leave every one of those surfaces blank for streaming shuffles.
   * Only the increment since the previous publication is forwarded, so this may be called as often
   * as convenient without any risk of double counting, and the reporter -- which documents that it
   * does not synchronise -- is only ever touched from the task thread.
   *
   * Bytes are counted as payload bytes rather than as framed bytes, so that the figure agrees with
   * both [[getPartitionLengths]] and the sizes carried by the map status. The protocol's framing
   * overhead stays visible through [[bytesWrittenToChannel]].
   *
   * Write time is not published here. It is charged where it is incurred, around block framing and
   * hand-off and around teardown, which keeps it free of the upstream operator's compute time.
   *
   * Spill volume is deliberately absent. It is published onto `memoryBytesSpilled`,
   * `diskBytesSpilled` and `peakExecutionMemory` by [[MemorySpillManager]], which writes to a fresh
   * reporter of its own so that spilled bytes are never also counted as shuffle-written bytes. The
   * four streaming metrics are likewise absent, because the spill manager and the backpressure
   * protocol already increment them at the moments they occur; incrementing them again here would
   * double every reading an operator sees.
   */
  private def publishWriteMetrics(): Unit = {
    if (payloadBytesStreamedTotal > publishedBytesWritten) {
      writeMetrics.incBytesWritten(payloadBytesStreamedTotal - publishedBytesWritten)
      publishedBytesWritten = payloadBytesStreamedTotal
    }
    if (recordsAppended > publishedRecordsWritten) {
      writeMetrics.incRecordsWritten(recordsAppended - publishedRecordsWritten)
      publishedRecordsWritten = recordsAppended
    }
  }

  /** Emits the one summary line a successful streaming map task contributes to the executor log. */
  private def logStreamingSummary(): Unit = {
    logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
      log"${MDC(TASK_ATTEMPT_ID, mapId)} streamed ${MDC(RECORDS, recordsAppended)} records as " +
      log"${MDC(NUM_BLOCKS, blocksStreamedTotal)} blocks totalling " +
      log"${MDC(NUM_BYTES, payloadBytesStreamedTotal)} payload bytes " +
      log"(${MDC(MEMORY_SIZE, serverHandler.bytesWrittenToChannel)} bytes on the wire) across " +
      log"${MDC(NUM_PARTITIONS, activePartitions.length)} partitions, with " +
      log"${MDC(COUNT, spillsObservedTotal)} spills, " +
      log"${MDC(VALUE, consumerStalls)} consumer stalls and " +
      log"${MDC(MAX_ATTEMPTS, replayAttemptsTotal)} replay attempts")
  }

  // -------------------------------------------------------------------------------------------
  // Cleanup. Every release below is idempotent, because it may be reached from stop() and from task
  // completion, and because a map task may call stop() twice.
  // -------------------------------------------------------------------------------------------

  /**
   * Releases everything this writer owns, on success, on failure and on cancellation alike.
   *
   * Registered on the task-completion listener because that is the only hook that fires in all
   * three cases; the reader's half of the subsystem has no choice about this, since its trait
   * exposes no `stop`, and the writer follows the same discipline so that the two halves release
   * identically. Because completion listeners run in reverse registration order and this one is
   * registered after the spill manager's, it runs first: egress is released while the buffers it
   * referenced are still alive.
   *
   * A failure to release is logged and swallowed. Throwing from a completion listener would replace
   * whatever failure the task is already reporting with a less informative one, and the memory this
   * writer holds is reclaimed by the spill manager's own listener regardless.
   */
  private def releaseOnTaskCompletion(): Unit = {
    try {
      closeSerializationResources()
      releaseEgressResources()
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not release map " +
          log"${MDC(TASK_ATTEMPT_ID, mapId)} on task completion", e)
    }
  }

  /**
   * Closes every serialization stream and frees every accumulator.
   *
   * Freeing the accumulators matters as much as closing the streams: each one holds a byte array
   * sized to a block, so leaving them referenced would retain a multiple of the block size per
   * partition for as long as anything holds this writer. The test JVM enforces zero retained task
   * memory, so this is machine-checked rather than merely intended.
   */
  private def closeSerializationResources(): Unit = {
    if (!serializationReleased) {
      serializationReleased = true
      var index = 0
      while (index < activePartitions.length) {
        val state = partitionStates(activePartitions(index))
        try {
          if (!state.closed) {
            state.closed = true
            state.stream.close()
          }
        } catch {
          case NonFatal(e) =>
            logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not close the " +
              log"serialization stream of partition ${MDC(PARTITION_ID, state.partitionId)}", e)
        }
        state.accumulator.release()
        index += 1
      }
    }
  }

  /** Releases the handler's queued and retained blocks, and its channel state. */
  private def releaseEgressResources(): Unit = {
    if (!egressReleased) {
      egressReleased = true
      try {
        serverHandler.releaseAll()
      } catch {
        case NonFatal(e) =>
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not free egress " +
            log"state for map ${MDC(TASK_ATTEMPT_ID, mapId)}", e)
      }
    }
  }

  /**
   * Frees every buffer and deletes every spill file, for a task that has already failed.
   *
   * Reached only from the unsuccessful branch of [[stop]]. Nothing retained for retransmission can
   * ever be asked for again once the task has failed, so waiting for task completion to release it
   * would hold executor memory and disk for no purpose. On the successful branch this is
   * deliberately not called: the spill manager's own completion listener releases it, which keeps
   * the retention window alive for the short remainder of the task in case a consumer asks for a
   * replay.
   */
  private def closeSpillState(): Unit = {
    try {
      spillManager.close()
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not release buffers " +
          log"and spill files for map ${MDC(TASK_ATTEMPT_ID, mapId)}", e)
    }
  }

  // -------------------------------------------------------------------------------------------
  // Inspection. Pure reads, exposed so that a suite can assert on what this writer did without
  // reaching into its internals or observing it through a live cluster.
  // -------------------------------------------------------------------------------------------

  /** The largest block payload this writer will frame, in bytes. */
  def blockPayloadCapacityBytes: Int = blockPayloadCapacity

  /** The aggregate buffer budget this writer streams within, in bytes. */
  def totalBufferBudgetBytes: Long = spillManager.totalBudgetBytes

  /** The per-partition buffer allowance, in bytes. */
  def perPartitionBufferBudgetBytes: Long = spillManager.perPartitionBudgetBytes

  /** Records appended to a partition stream by this writer. */
  def recordsStreamed: Long = recordsAppended

  /** Blocks handed to egress by this writer. */
  def blocksStreamed: Long = blocksStreamedTotal

  /** Payload bytes handed to egress by this writer, framing excluded. */
  def payloadBytesStreamed: Long = payloadBytesStreamedTotal

  /** Framed bytes the handler has written to the channel on this writer's behalf. */
  def bytesWrittenToChannel: Long = serverHandler.bytesWrittenToChannel

  /** Times a buffer reservation had to be retried after an eviction pass. */
  def admissionRetryCount: Long = admissionRetries

  /** Streams that crossed the consumer-liveness window without acknowledgement progress. */
  def consumerStallCount: Long = consumerStalls

  /** Retransmission attempts made in response to a stalled consumer. */
  def replayAttemptCount: Long = replayAttemptsTotal

  /** Spills observed by this writer, as counted by the spill manager. */
  def spillsObserved: Long = spillsObservedTotal

  /** Buffer reservations that eviction rescued rather than prevented. */
  def memoryPressureBrushCount: Long = memoryPressureBrushes

  /** Reclamations confirmed outside the 100 ms bound. */
  def reclamationDeadlineBreaches: Long = reclamationBreaches

  /** Ids of the partitions this writer streamed at least one record to, in first-touch order. */
  def streamedPartitions: Seq[Int] = activePartitions.toSeq

  /**
   * The next sequence number one partition will be assigned, which is also the number of blocks
   * already streamed for it.
   */
  def nextSequenceNumberFor(partitionId: Int): Long = {
    if (partitionId < 0 || partitionId >= declaredPartitions) {
      0L
    } else {
      val state = partitionStates(partitionId)
      if (state == null) 0L else state.nextSequenceNumber
    }
  }

  /** Whether [[stop]] has already been entered. */
  def isStopped: Boolean = stopping

  /** The map status produced on success, or `None` before a successful stop. */
  def producedMapStatus: Option[MapStatus] = Option(mapStatus)

  // -------------------------------------------------------------------------------------------
  // Internal state types
  // -------------------------------------------------------------------------------------------

  /**
   * Everything this writer tracks for one reduce partition.
   *
   * A mutable holder rather than an immutable value, and deliberately so: it is read and updated on
   * the per-record path, it is confined to the task thread, and copying it per record would
   * allocate once per record for no benefit at all.
   *
   * @param partitionId the reduce partition this state belongs to
   * @param initialCapacityBytes the accumulator's starting size, which is the block size, so that a
   *                             partition that fills exactly one block never grows its buffer
   */
  private final class PartitionEgressState(val partitionId: Int, initialCapacityBytes: Int) {

    /** Serialized bytes produced for this partition and not yet cut into a block. */
    val accumulator: BlockAccumulator = new BlockAccumulator(initialCapacityBytes)

    /**
     * The one serialization stream of this partition.
     *
     * Assigned immediately after construction by the only factory that creates a state, and never
     * reassigned. It is a `var` purely because the stream is built over the accumulator this object
     * owns, so the accumulator has to exist first.
     */
    var stream: SerializationStream = _

    /** The sequence number the next block of this partition will carry. */
    var nextSequenceNumber: Long = 0L

    /** Records appended to this partition's stream. */
    var recordsAppended: Long = 0L

    /** Payload bytes streamed for this partition, framing excluded. */
    var payloadBytesStreamed: Long = 0L

    /** The highest acknowledged position already applied to the buffer budget. */
    var appliedAckPosition: Long = NOTHING_APPLIED

    /** Retransmission attempts spent on this stall, reset by any acknowledgement progress. */
    var replayAttempts: Int = 0

    /** The clock reading at which the next retransmission attempt becomes due. */
    var nextReplayAtMillis: Long = 0L

    /** Whether this stall was already reported, so it is logged once and not per poll. */
    var stallReported: Boolean = false

    /** Whether the serialization stream has been closed. */
    var closed: Boolean = false

    /** Whether end of stream has been signalled for this partition. */
    var terminated: Boolean = false
  }

  /**
   * A growable byte buffer that a serialization stream writes into and blocks are cut out of.
   *
   * This exists rather than a `ByteArrayOutputStream` because the operation the writer needs is not
   * "give me everything": it is "detach the first N bytes and keep the rest", so that a record
   * which straddles a block boundary is split at the boundary instead of forcing the boundary to
   * move. A `ByteArrayOutputStream` can only be read whole and reset whole, which would either lose
   * the remainder or copy it twice.
   *
   * `close` and `flush` are no-ops on purpose. A serialization stream closes the stream it was
   * given, and losing the trailing bytes a serializer emits while closing would truncate the last
   * block of every partition.
   *
   * Not thread safe, and never touched from anywhere but the task thread.
   *
   * @param initialCapacityBytes the starting size, floored so that a tiny block size does not cause
   *                             repeated growth on the first few records
   */
  private final class BlockAccumulator(initialCapacityBytes: Int) extends OutputStream {

    private var buffer: Array[Byte] =
      new Array[Byte](math.max(MIN_ACCUMULATOR_BYTES, math.max(1, initialCapacityBytes)))

    private var count: Int = 0

    /** Bytes currently held. */
    def size: Int = count

    /** The current capacity, exposed for diagnostics and for tests of the growth policy. */
    def capacity: Int = buffer.length

    override def write(oneByte: Int): Unit = {
      ensureCapacity(1)
      buffer(count) = oneByte.toByte
      count += 1
    }

    override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
      if (length < 0 || offset < 0 || offset > bytes.length - length) {
        throw new IndexOutOfBoundsException(
          s"offset $offset and length $length do not fit an array of ${bytes.length} bytes")
      }
      if (length > 0) {
        ensureCapacity(length)
        System.arraycopy(bytes, offset, buffer, count, length)
        count += length
      }
    }

    /** No-op: the accumulator has nowhere to flush to. */
    override def flush(): Unit = {}

    /**
     * No-op: closing must not discard the buffer, because a serialization stream closes the stream
     * it was handed and the bytes it emits while closing are the tail of the partition.
     */
    override def close(): Unit = {}

    /**
     * Detaches the first `length` bytes and shifts the remainder to the front.
     *
     * The returned array is always freshly allocated and never aliases this accumulator's buffer or
     * any array it shares with another cut. A block's payload outlives the cut that produced it --
     * the retention window holds it for possible retransmission and the egress queue holds it until
     * it is written -- so handing back a shared array would give two retained blocks one identity.
     * The zero-length case is included in that guarantee rather than special-cased out of it: it
     * costs one empty allocation on a path the writer never takes, and it means no caller has to
     * know that one length is different from the others.
     *
     * @param length bytes to detach; must not exceed [[size]]
     * @return the detached bytes, freshly allocated and owned by the caller
     */
    def take(length: Int): Array[Byte] = {
      require(length >= 0 && length <= count,
        s"cannot take $length bytes from an accumulator holding $count")
      if (length == 0) {
        new Array[Byte](0)
      } else {
        val block = new Array[Byte](length)
        System.arraycopy(buffer, 0, block, 0, length)
        val remaining = count - length
        if (remaining > 0) {
          System.arraycopy(buffer, length, buffer, 0, remaining)
        }
        count = remaining
        block
      }
    }

    /** Drops the buffer so it can be collected, leaving the accumulator empty and reusable. */
    def release(): Unit = {
      buffer = EMPTY_PAYLOAD
      count = 0
    }

    /**
     * Grows the buffer so that `additional` more bytes fit, doubling to amortise the copies.
     *
     * The arithmetic is done in `Long` because a partition's accumulator can legitimately approach
     * the array size limit when one record is enormous, and `count + additional` would silently
     * become negative at that point, turning an out-of-memory condition into a corrupt copy.
     */
    private def ensureCapacity(additional: Int): Unit = {
      val required = count.toLong + additional.toLong
      if (required > buffer.length.toLong) {
        if (required > MAX_ACCUMULATOR_BYTES.toLong) {
          throw new IOException(s"A single streaming shuffle record needs $required bytes, which " +
            s"exceeds the largest buffer that can be allocated ($MAX_ACCUMULATOR_BYTES bytes)")
        }
        val grown = math.min(
          MAX_ACCUMULATOR_BYTES.toLong, math.max(buffer.length.toLong * 2L, required))
        buffer = Arrays.copyOf(buffer, grown.toInt)
      }
    }
  }
}

/**
 * Constants of the streaming shuffle producer.
 *
 * The timing figures are the feature's own, restated here as named values so that the write path
 * reads as prose and a test can assert against the same names the implementation uses. Where a
 * figure is also a constant of a collaborator, it is taken from that collaborator rather than
 * repeated, so that the two can never drift apart.
 */
private[spark] object StreamingShuffleWriter {

  /** Cadence of the maintenance pass: flow control, spill polling, liveness and telemetry. */
  val MAINTENANCE_INTERVAL_MS: Long = 100L

  /**
   * Records between two consecutive maintenance opportunities.
   *
   * A shuffle of very small records may go a long time between block boundaries, and the clock has
   * to be consulted anyway; testing an integer counter first keeps the per-record cost of that at a
   * single comparison.
   */
  val MAINTENANCE_RECORD_INTERVAL: Int = 1024

  /** The bound within which buffers must be released after an acknowledgement frees them. */
  val RECLAMATION_DEADLINE_MS: Long = BackpressureProtocol.RECLAMATION_DEADLINE_MS

  /** Interval at which a producer heartbeat is sent on every active stream. */
  val HEARTBEAT_INTERVAL_MS: Long = StreamingShuffleServerHandler.PRODUCER_HEARTBEAT_INTERVAL_MS

  /** Acknowledgement gap after which a consumer is treated as unresponsive. */
  val CONSUMER_LIVENESS_TIMEOUT_MS: Long =
    StreamingShuffleServerHandler.CONSUMER_LIVENESS_TIMEOUT_MS

  /** First retransmission backoff; each further attempt doubles it. */
  val REPLAY_BASE_BACKOFF_MS: Long = StreamingShuffleServerHandler.RETRANSMIT_INITIAL_BACKOFF_MS

  /** Retransmission attempts allowed per stream before the consumer is declared lost. */
  val MAX_REPLAY_ATTEMPTS: Int = StreamingShuffleServerHandler.MAX_RETRANSMIT_ATTEMPTS

  /** Largest doubling applied to the backoff, which is the last attempt's shift. */
  val MAX_BACKOFF_SHIFT: Int = math.max(0, MAX_REPLAY_ATTEMPTS - 1)

  /** Ceiling on the retransmission backoff, so the last attempt still falls inside a task. */
  val MAX_REPLAY_BACKOFF_MS: Long = REPLAY_BASE_BACKOFF_MS << MAX_BACKOFF_SHIFT

  /**
   * Consecutive eviction passes that free nothing at all before a refused buffer reservation is
   * treated as prevented.
   *
   * This is a tolerance for barren rounds, not a cap on total rounds: a pass that frees bytes is
   * making progress towards the refusing partition even when it freed them from a different one, so
   * only a run of passes that free nothing proves the budget is genuinely exhausted. Three absorbs
   * the case where egress or acknowledgement drainage needs a moment to change the verdict, and is
   * few enough that a truly exhausted budget is diagnosed promptly rather than after a long, futile
   * loop.
   */
  val MAX_ADMISSION_RECOVERY_ROUNDS: Int = 3

  /**
   * Recovery rounds that may be deferred while another thread's eviction is still in flight.
   *
   * A deferral is not idle: the round it covers still pushes queued bytes towards the wire, applies
   * whatever the consumer acknowledged, and yields the CPU to the thread doing the reclaiming, so
   * waiting and working are the same act. The bound exists only so that an evictor which never
   * completes -- a wedged disk, a starved thread -- cannot spin the task thread forever; reaching
   * it hands the verdict back to the barren-round tolerance, which fails the task and degrades to
   * sort-based shuffle exactly as specified.
   *
   * Sized from measurement rather than taste. A spill write of a few kilobytes was observed landing
   * roughly 17 ms after it began, while sixty-odd rounds that did not yield burned through their
   * whole allowance in about 5 ms -- fast enough to declare the budget exhausted while it was
   * actively being freed. A thousand yielding rounds spans that write with a wide margin on a busy
   * executor and still terminates promptly when nothing is going to arrive.
   */
  val MAX_EVICTION_DEFERRALS: Int = 1024

  /**
   * Blocks a partition's buffer allowance is sized to hold concurrently.
   *
   * A block is framed to this share of the allowance rather than to the whole of it, so that a
   * partition can hold one block being filled plus a retained window awaiting acknowledgement. Four
   * places steady-state utilisation for an evenly loaded shuffle at roughly a quarter of the buffer
   * budget, which keeps ordinary operation clear of the `spillThreshold` -- spill then means the
   * consumer has genuinely fallen behind, which is what the threshold is specified to detect --
   * while still leaving a window deep enough to retransmit from. Raising it would shrink blocks and
   * add framing overhead per byte; lowering it to one would collapse the pipeline, pin utilisation
   * at 100% and make disk, not the network, the limit on throughput.
   */
  val TARGET_PIPELINE_DEPTH: Int = 4

  /** Window over which the producer's egress rate is averaged before it is reported. */
  val THROUGHPUT_WINDOW_MS: Long = 1000L

  /** Milliseconds in a second, named so the rate arithmetic reads as a rate. */
  val MILLIS_PER_SECOND: Long = 1000L

  /** Starting size of the per-partition active-set list. */
  val INITIAL_ACTIVE_PARTITION_CAPACITY: Int = 16

  /** Floor on an accumulator's capacity, so a small block size does not cause repeated growth. */
  val MIN_ACCUMULATOR_BYTES: Int = 4096

  /**
   * Largest accumulator this writer will allocate.
   *
   * Eight bytes below `Int.MaxValue`, which is the conventional headroom for an array header on the
   * JVM: requesting the theoretical maximum fails on some implementations before it fails on any.
   */
  val MAX_ACCUMULATOR_BYTES: Int = Int.MaxValue - 8

  /** The sentinel for a stream no acknowledgement has been applied to yet. */
  val NOTHING_APPLIED: Long = -1L

  /**
   * The empty array a released accumulator parks its buffer reference on.
   *
   * Sharing one instance for this is safe in a way that sharing it for a detached payload would not
   * be: nothing is ever written through it, because any write to a released accumulator grows the
   * buffer by copying it first, and a zero-length array has no element to mutate in any case.
   */
  val EMPTY_PAYLOAD: Array[Byte] = new Array[Byte](0)
}

