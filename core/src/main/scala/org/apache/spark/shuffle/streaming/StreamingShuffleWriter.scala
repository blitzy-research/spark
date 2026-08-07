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

import java.io.{ByteArrayInputStream, File, InputStream, IOException, OutputStream,
  SequenceInputStream}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{CONFIG, COUNT, DESCRIPTION, DURATION, EPOCH, FILE_NAME,
  INDEX, MAP_ID, MAX_ATTEMPTS, MAX_SIZE, MEMORY_SIZE, NUM_BLOCKS, NUM_BYTES, NUM_EVENTS,
  NUM_PARTITIONS, NUM_SKIPPED, NUM_TASKS, PARTITION_ID, REASON, RECORDS, SHUFFLE_ID,
  TASK_ATTEMPT_ID, THRESHOLD, TIMEOUT, VALUE}
import org.apache.spark.network.shuffle.protocol.streaming.DataBlockMessage
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.{Clock, SystemClock}

/**
 * The streaming shuffle collaborators a [[StreamingShuffleWriter]] is built on, bundled into one
 * value.
 *
 * @param backpressure executor-wide flow control: credit, liveness timers and arbitration
 * @param rateLimiter executor-wide egress pacing, consulted here only for its framing ceiling
 * @param spillManager this task's buffer budget, retention window and disk spill
 * @param blockResolver the executor-scoped registry that publishes this task's retained output,
 *     so that egress and replay can read a block whether it is in memory or spilled
 * @param serverHandler this task's egress path: framing, checksums, pacing and the wire
 * @param fallbackPolicy the four graceful-degradation trip conditions
 * @param errorNotifier the first-error-wins bridge from Netty threads to the task thread
 * @param coordinatorGateway the driver-facing operations only a producer can perform: turning a
 *     local fallback trip into a shuffle-wide decision, and reporting that this map output has been
 *     streamed to completion.
 */
private[spark] case class StreamingShuffleWriterComponents(
    backpressure: BackpressureProtocol,
    rateLimiter: TokenBucketRateLimiter,
    spillManager: MemorySpillManager,
    blockResolver: StreamingShuffleBlockResolver,
    serverHandler: StreamingShuffleServerHandler,
    fallbackPolicy: StreamingShuffleFallbackPolicy,
    errorNotifier: StreamingShuffleErrorNotifier,
    coordinatorGateway: StreamingShuffleCoordinatorGateway)

/**
 * Writes a map task's output by streaming it to its consumers instead of materialising it on disk.
 *
 * Records are serialized into per-partition buffers whose aggregate is the share of executor memory
 * `spark.shuffle.streaming.bufferSizePercent` allows, divided by the reduce partition count. A
 * buffer is cut into blocks of at most the protocol's block size, each checksummed, and offered to
 * the egress handler once flow control and the pacing bucket admit it. A block stays retained until
 * its consumer acknowledges it, and that retained window is what a retransmission is served from;
 * eviction to local disk keeps the window inside the budget.
 *
 * Flush ordering is the whole of what "prioritising shuffle traffic over speculative execution"
 * means here: this writer orders its own egress using the attempt attributes `TaskContext` already
 * exposes. Nothing marks a packet and no network-level quality of service is configured.
 *
 * `stop(success = true)` returns a placeholder [[MapStatus]] wrapped in `Some`, because the shared
 * write path dereferences the result unconditionally; the status satisfies the scheduler's
 * bookkeeping without claiming materialised output. `stop(success = false)` withdraws the
 * generation and releases everything this attempt holds.
 *
 * Threading. The shuffle metrics reporters are documented as single-threaded, so no reporter is
 * ever called from a Netty thread: the handler publishes plain counters and this writer forwards
 * them on the task thread. A failure observed on an I/O thread reaches this task through
 * [[StreamingShuffleErrorNotifier]].
 *
 * When streaming is not viable for an attempt -- the operator gate is off, a trip condition holds,
 * or the shuffle has already stood down -- the whole record iterator is handed to the sort-based
 * writer built by `sortShuffleWriter`, so the attempt completes on the unmodified path.
 *
 * @param handle the streaming registration produced by `registerShuffle`
 * @param mapId this map task's identifier, as the scheduler knows it
 * @param context this task's context, the source of both attempt attributes and cleanup
 * @param writeMetrics the reporter supplied by
 *     [[org.apache.spark.shuffle.ShuffleWriteProcessor]], which is this task's
 *     `shuffleWriteMetrics`
 * @param conf the executor's configuration, read once here and then held immutably
 * @param components the streaming collaborators, owned by [[StreamingShuffleManager]]
 * @param sortShuffleWriter builds the sort-based writer for this same handle, map id, task
 *     context and reporter.
 * @param clock the time source for every elapsed-time decision, the shuffle write time reported
 *     to the metrics reporter included
 * @tparam K the shuffle key type
 * @tparam V the shuffle value type
 * @tparam C the combiner type of the underlying dependency, unused by a streaming producer
 *     because map-side combination is a materialising operation
 */
private[spark] class StreamingShuffleWriter[K, V, C](
    handle: StreamingShuffleHandle[K, V, C],
    mapId: Long,
    context: TaskContext,
    writeMetrics: ShuffleWriteMetricsReporter,
    conf: SparkConf,
    components: StreamingShuffleWriterComponents,
    sortShuffleWriter: () => ShuffleWriter[K, V],
    clock: Clock = new SystemClock)
  extends ShuffleWriter[K, V] with Logging {

  import StreamingShuffleWriter._

  /** The ledger identity of one partition of this map output, as seen from the sending end. */
  private def producerKey(partitionId: Int): BackpressureStreamKey =
    BackpressureStreamKey.forProducer(
      shuffleId, mapId, context.taskAttemptId(), partitionId)

  // Collaborators, unpacked once so that the hot path reads a field rather than a case class member

  private val backpressure: BackpressureProtocol = components.backpressure
  private val rateLimiter: TokenBucketRateLimiter = components.rateLimiter
  private val spillManager: MemorySpillManager = components.spillManager

  private val blockResolver: StreamingShuffleBlockResolver = components.blockResolver

  private val serverHandler: StreamingShuffleServerHandler = components.serverHandler
  private val fallbackPolicy: StreamingShuffleFallbackPolicy = components.fallbackPolicy
  private val errorNotifier: StreamingShuffleErrorNotifier = components.errorNotifier

  private val coordinatorGateway: StreamingShuffleCoordinatorGateway =
    components.coordinatorGateway

  // Shuffle shape, taken from the handle

  private val dep = handle.dependency

  private val shuffleId: Int = handle.shuffleId

  // The egress handler keys the very same ledgers this writer registers -- it is the component that
  // reports acknowledgements and replays against them -- so the two must agree on the producing
  // identity exactly.
  require(serverHandler.shuffleId == shuffleId && serverHandler.mapId == mapId &&
    serverHandler.taskAttemptId == context.taskAttemptId(),
    s"The streaming shuffle egress handler was built for shuffle ${serverHandler.shuffleId} map " +
      s"${serverHandler.mapId} attempt ${serverHandler.taskAttemptId} but this writer produces " +
      s"shuffle $shuffleId map $mapId attempt ${context.taskAttemptId()}.")

  /** The reduce partition count, as the handle recorded it at registration. */
  private val declaredPartitions: Int = {
    val fromPartitioner = dep.partitioner.numPartitions
    require(handle.numPartitions == fromPartitioner,
      s"Streaming shuffle $shuffleId was registered for ${handle.numPartitions} partitions but " +
        s"its partitioner reports $fromPartitioner; the registered count is the divisor of the " +
        "per-partition buffer ceiling and must match the partitioner exactly")
    fromPartitioner
  }

  /** The partition count used as a divisor and as a registration argument, floored at one. */
  private val partitionDivisor: Int = math.max(1, declaredPartitions)

  // Configuration, read exactly once (G5).

  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  private val bufferSizePercent: Int = conf.get(config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)

  private val spillThresholdPercent: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)

  // Task attributes

  /** The flush ordering this task's blocks are queued under. */
  private val egressPriority: StreamingShuffleServerHandler.EgressPriority =
    StreamingShuffleServerHandler.EgressPriority(
      context.stageId(),
      context.stageAttemptNumber(),
      context.taskAttemptId(),
      context.attemptNumber())

  /** The block manager, dereferenced eagerly. */
  private val blockManager = SparkEnv.get.blockManager

  /** The one serializer instance every partition's stream is built from. */
  private lazy val serializerInstance: SerializerInstance = dep.serializer.newInstance()

  /** The compression and encryption wrapper both ends of a streamed partition must agree on. */
  private val serializerManager: SerializerManager = SparkEnv.get.serializerManager

  /** The largest block payload this writer will ever frame, in bytes. */
  private lazy val derivedBlockPayloadCapacity: Int = {
    val protocolCeiling = DataBlockMessage.MAX_BLOCK_SIZE_BYTES.toLong
    // A share of the allowance, so a partition holds a block being filled plus a retained window,
    // and a share per task slot, so the executor-wide budget bounds the executor and not one task.
    val pipelineCeiling = spillManager.perPartitionBudgetBytes / framingShareDivisor -
      MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES
    // The hard admissibility ceiling: never frame a block eviction could not make room for.
    val budgetCeiling = math.min(pipelineCeiling, spillManager.maxAdmissiblePayloadBytes)
    // An unlimited limiter reports Long.MaxValue, so the subtraction below cannot overflow.
    val paceCeiling = rateLimiter.maxAcquirableBytes - DataBlockMessage.FRAMING_OVERHEAD_BYTES
    val resolved = math.min(protocolCeiling, math.min(budgetCeiling, paceCeiling))
    if (resolved <= 0L) 0 else resolved.toInt
  }

  /**
   * The framing capacity this attempt actually uses: the derived one, unless a smaller one had to
   * be negotiated to fit the allowance.
   */
  private def blockPayloadCapacity: Int =
    if (negotiatedCapacityBytes > 0) negotiatedCapacityBytes else derivedBlockPayloadCapacity

  /**
   * The divisor applied to a partition's allowance to obtain one block's framing capacity: the
   * pipeline depth this writer reserves, multiplied by the number of map tasks that can hold a
   * framing accumulator at the same time in this JVM.
   */
  private def framingShareDivisor: Long =
    TARGET_PIPELINE_DEPTH.toLong * math.max(1, spillManager.concurrentTaskSlots).toLong

  /**
   * The whole framing reservation this attempt takes before it consumes a record: one accumulator's
   * worth of heap for every partition the shuffle declares.
   */
  private def framingEnvelopeBytes: Long =
    accumulatorFootprintBytes * declaredPartitions.toLong

  /** Base credit-ledger metadata reserved before records can open any partition stream. */
  private def producerLedgerEnvelopeBytes: Long =
    BackpressureProtocol.STREAM_LEDGER_BASE_BYTES * declaredPartitions.toLong

  // Mutable state.

  /** Per-partition egress state, indexed by reduce partition id, allocated lazily on first use. */
  private val partitionStates = new Array[PartitionEgressState](declaredPartitions)

  /** Ids of the partitions [[partitionStates]] holds a state for, in first-touch order. */
  private val activePartitions = new mutable.ArrayBuffer[Int](
    math.min(math.max(declaredPartitions, 1), INITIAL_ACTIVE_PARTITION_CAPACITY))

  /** Bytes of payload streamed per reduce partition. */
  private val partitionLengths: Array[Long] = new Array[Long](math.max(0, declaredPartitions))

  private var initialized: Boolean = false

  private var finished: Boolean = false

  /** The trip condition that stood this attempt's streaming down mid-write, once one has. */
  private var standDownReason: Option[StreamingShuffleStandDownCause] = None

  /** Operator-facing description of why streaming stood down mid-write, or `null` if it did not. */
  private var standDownDetail: String = null

  /** Whether this writer's release has been registered on the task's completion listener. */
  private var cleanupInstalled: Boolean = false

  /**
   * The sort-based writer this attempt handed itself to, if streaming turned out not to be viable
   * for it.
   */
  private var sortDelegate: ShuffleWriter[K, V] = null

  /**
   * The shuffle-wide stand-down this attempt has observed while producing, if it has observed one.
   */
  private var pendingDegradation: Option[StreamingShuffleWriter.MidStreamDegradation] = None

  /** Bytes of the up-front framing reservation that no partition has drawn yet. */
  private var framingEnvelopeRemaining: Long = 0L

  private var producerLedgerMetadataRemaining: Long = 0L

  /** How far the stop protocol has progressed. */
  private var stopState: StopState = StopState.NotEntered

  private var serializationReleased: Boolean = false

  private var egressReleased: Boolean = false

  private var streamsCompleted: Boolean = false

  private var streamsUnregistered: Boolean = false

  private var retainedOutputSecured: Boolean = false

  private var mapStatus: MapStatus = null

  private var recordsAppended: Long = 0L

  private var recordsSinceMaintenance: Int = 0

  private var blocksStreamedTotal: Long = 0L

  private var payloadBytesStreamedTotal: Long = 0L

  private var lastMaintenanceMillis: Long = 0L

  private var lastHeartbeatMillis: Long = 0L

  private var lastCoordinatorHeartbeatMillis: Long = 0L

  private var throughputWindowOpenedMillis: Long = 0L

  private var throughputWindowBaseBytes: Long = 0L

  private var admissionRetries: Long = 0L

  private var consumerStalls: Long = 0L

  private var replayAttemptsTotal: Long = 0L

  private var spillsObservedTotal: Long = 0L

  // Instant of the last pipelined durability pass, seeded from the injected clock when streaming is
  // initialised.
  private var lastRetainedDurabilityMillis: Long = 0L

  private var retainedDurabilityBytesTotal: Long = 0L

  private var retainedDurabilityPassesTotal: Long = 0L

  private var reclamationBreaches: Long = 0L

  private var memoryPressureBrushes: Long = 0L

  // Blocks this attempt could not buffer and wrote straight to local disk instead.
  private var durableAdmissionsObserved: Long = 0L

  // The framing capacity negotiated down to fit the allowance, or zero while the derived one
  // stands.
  private var negotiatedCapacityBytes: Int = 0

  // Monotonic high-water marks of what has already been forwarded to the task's metrics reporter.
  private var publishedBytesWritten: Long = 0L

  private var publishedRecordsWritten: Long = 0L

  // ShuffleWriter contract

  /**
   * Streams this task's records to their consumers.
   *
   * @param records this task's output, in the order the upstream operator produced it
   * @throws IOException if a serialization stream or the underlying channel fails
   */
  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // One writer streams one map task's output exactly once.
    if (finished) {
      throw new IllegalStateException(s"Streaming shuffle $shuffleId map $mapId has already " +
        "written its output; a shuffle writer streams one record iterator exactly once.")
    }
    // Settled before a single record is consumed, so a refusal can still hand the iterator on
    // intact.
    if (!prepareStreaming()) {
      sortDelegate.write(records)
      finished = true
      return
    }
    try {
      initializeStreaming()
      // The loop yields as soon as a shuffle-wide stand-down has been observed, because from that
      // moment on the correct thing to do with the remaining records is to hand them, together with
      // the ones already consumed, to the sort-based writer.
      while (records.hasNext && pendingDegradation.isEmpty) {
        val record = records.next()
        appendRecord(record._1, record._2)
      }
      pendingDegradation match {
        case Some(degradation) => finishByDegrading(degradation, records)
        case None => finishAllPartitions()
      }
      finished = true
    } catch {
      case signal: StreamingShuffleWriter.StandDownSignal =>
        // Not a failure, and that distinction is the whole of it: this attempt stops streaming and
        // then COMPLETES, so no task attempt is consumed and a master that permits one failure --
        // which a plain local[n] forces -- cannot abort the job because streaming stood down.
        completeStandDown(signal)
      case NonFatal(e) =>
        // A failure already observed on an I/O thread is the more precise diagnosis of what went
        // wrong, so it wins, and this one is attached to it as suppressed so that nothing is lost.
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
   * @param success whether the map task completed successfully
   * @return the map status on success, `None` otherwise
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    // A delegated attempt is the sort-based writer's from end to end, its stop protocol included:
    // the map status, the partition lengths and the cleanup all belong to it, and running any of
    // this writer's own stop sequence alongside would release state it never created and report
    // write time the delegate has already reported.
    if (sortDelegate != null) {
      return sortDelegate.stop(success)
    }
    val recoveringFromSuccess = !success && stopState == StopState.Succeeded
    if (stopState != StopState.NotEntered && !recoveringFromSuccess) {
      return None
    }
    stopState = StopState.Running
    val startedAtNanos = clock.nanoTime()
    try {
      if (success && standDownReason.isDefined) {
        // A stand-down attempt has already released everything it held and withdrawn its generation
        // from every owner of it, at the instant it stood down.
        publishWriteMetrics()
        mapStatus = voidMapStatus()
        stopState = StopState.Succeeded
        Option(mapStatus)
      } else if (success) {
        try {
          val status = completeSuccessfully()
          stopState = StopState.Succeeded
          status
        } catch {
          case NonFatal(e) =>
            // The sequence did not finish, so this generation's output is not reachable in the way
            // a published map status would promise.
            releaseAfterFailure("the producing map task could not complete its successful stop")
            stopState = StopState.Failed
            throw e
        }
      } else {
        if (recoveringFromSuccess) {
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
            log"${MDC(TASK_ATTEMPT_ID, mapId)} reported success and is now failing; its streamed " +
            log"output is withdrawn, because a consumer must not read the output of an attempt " +
            log"that did not succeed")
        }
        releaseAfterFailure("the producing map task failed")
        stopState = StopState.Failed
        None
      }
    } finally {
      closeSerializationResources()
      writeMetrics.incWriteTime(clock.nanoTime() - startedAtNanos)
    }
  }

  /**
   * Turns a mid-write trip into a completed task whose output is withdrawn.
   *
   * @param signal the trip that stood this attempt down, carrying the condition and its
   *     description
   */
  private def completeStandDown(signal: StreamingShuffleWriter.StandDownSignal): Unit = {
    standDownReason = Some(signal.reason)
    standDownDetail = signal.detail
    retireVoidGeneration(s"streaming stood down because ${signal.detail}")
    publishWriteMetrics()
    finished = true
    errorNotifier.error.foreach { latched =>
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
        log"${MDC(TASK_ATTEMPT_ID, mapId)} had also observed a failure on an I/O thread before " +
        log"it stood down; the attempt still completes, because the stage is recomputed either " +
        log"way and failing would consume a task attempt for no benefit", latched)
    }
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
      log"${MDC(TASK_ATTEMPT_ID, mapId)} stood streaming down after " +
      log"${MDC(NUM_BLOCKS, blocksStreamedTotal)} block(s) and " +
      log"${MDC(RECORDS, recordsAppended)} record(s) because " +
      log"${MDC(REASON, signal.detail)}; the attempt completes with its output withdrawn, so no " +
      log"task failure is counted and the map stage is recomputed on the sort-based path")
  }

  /**
   * Retires this generation and releases everything it holds, for an attempt that will not stream.
   *
   * @param reason operator-facing context, recorded with the withdrawal on the driver and
   *     locally
   */
  private def retireVoidGeneration(reason: String): Unit = {
    // The framing reservation and the accumulators first, because they are this task's execution
    // memory and the withdrawal that follows does not cover them.
    closeSerializationResources()
    releaseAfterFailure(reason)
  }

  /**
   * The status a stood-down attempt reports: one that no consumer may skip.
   *
   * @return the void status, always non-empty and never skippable
   */
  private def voidMapStatus(): MapStatus = {
    val unskippableLengths = new Array[Long](partitionLengths.length)
    var partitionId = 0
    while (partitionId < unskippableLengths.length) {
      unskippableLengths(partitionId) = math.max(1L, partitionLengths(partitionId))
      partitionId += 1
    }
    MapStatus(blockManager.shuffleServerId, unskippableLengths, mapId)
  }

  /**
   * Runs the successful stop sequence and produces the map status.
   *
   * @return the placeholder map status, always non-empty
   */
  private def completeSuccessfully(): Option[MapStatus] = {
    val delivered = drainBeforeStopping()
    val durable = secureRetainedOutput()
    if (!durable && (spillManager.diskPressureDetected || spillManager.memoryPressureDetected)) {
      val requested = math.max(1L, spillManager.bufferedBytes)
      backpressure.reportBufferAllocationFailure(producerKey(0))
      standDownForMemoryPressure(requested, 0L)
      val signal = new StreamingShuffleWriter.StandDownSignal(
        fallbackPolicy.trippedReason.getOrElse(StreamingShuffleFallbackReason.MemoryPressure),
        s"the retained-output ceiling could not make ${spillManager.bufferedBytes} byte(s) " +
          "durable without exceeding executor memory or local-disk quotas")
      completeStandDown(signal)
      mapStatus = voidMapStatus()
      return Option(mapStatus)
    }
    requireOutputRecoverable(delivered, durable)
    requireNoLatchedFailure()
    completeProducerStreams()
    unregisterProducerStreams()
    publishWriteMetrics()
    val refusal = requirePublishableGeneration()
    if (refusal.isDefined) {
      return refusal
    }
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
    serverHandler.markProducerTaskComplete()
    logStreamingSummary()
    Option(mapStatus)
  }

  /**
   * Asks the driver to accept this generation's completion and settles whether its output may be
   * published, answering with the status to report instead when it may not.
   *
   * @return `None` when this generation may publish its ordinary status, or the status to
   *     report instead when it may not
   * @throws org.apache.spark.SparkException when the generation was refused without a
   *     shuffle-wide fallback, so that the unmodified scheduler recomputes this map output
   */
  private def requirePublishableGeneration(): Option[MapStatus] = {
    reportMapOutputComplete() match {
      case Some(true) => None
      case None =>
        // Nothing is known, so nothing is authorised.
        val signal = new StreamingShuffleWriter.StandDownSignal(
          StreamingShuffleStandDownCause.ProducerUnavailable,
          s"the driver could not confirm the completion of map index ${context.partitionId()} " +
            s"attempt ${context.taskAttemptId()}, so nothing is known about whether it still " +
            "holds this producer generation and its output must not be published as readable")
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
          log"${MDC(TASK_ATTEMPT_ID, mapId)} could not confirm its completion with the driver; " +
          log"its output is withdrawn rather than published, because an answer that never " +
          log"arrived is what a withdrawal, a supersession or a stand-down racing this stop also " +
          log"looks like, and the map stage is recomputed instead")
        completeStandDown(signal)
        mapStatus = voidMapStatus()
        Option(mapStatus)
      case Some(false) if fallbackPolicy.shuffleHasFallenBack(shuffleId) =>
        // Read across the whole closed set, for the reason every other stand-down here reads it
        // that way: the shuffle-wide verdict may be a structural decline, and re-labelling it as
        // one of the four specified conditions would put a measurement nobody took into the record.
        val reason: StreamingShuffleStandDownCause =
          fallbackPolicy.knownShuffleFallback(shuffleId).flatMap(_.cause)
            .orElse(fallbackPolicy.trippedReason)
            .getOrElse(StreamingShuffleStandDownCause.ProducerUnavailable)
        val signal = new StreamingShuffleWriter.StandDownSignal(reason,
          s"the driver refused the completion of map index ${context.partitionId()} attempt " +
            s"${context.taskAttemptId()} because a shuffle-wide fallback had retired its " +
            "producer generation, so this attempt reports a withdrawn output rather than one a " +
            "delegated reduce task would be told to read")
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
          log"${MDC(TASK_ATTEMPT_ID, mapId)} finished streaming into a shuffle-wide fallback: " +
          log"its completion was refused, so it reports the void status and its output is " +
          log"recomputed on the sort-based path")
        completeStandDown(signal)
        mapStatus = voidMapStatus()
        Option(mapStatus)
      case Some(false) =>
        val failure = new SparkException(s"Streaming shuffle $shuffleId refused the completion " +
          s"of map index ${context.partitionId()} attempt ${context.taskAttemptId()}: the driver " +
          "no longer holds that producer generation, so it has been superseded by a newer " +
          "attempt or invalidated by a consumer. No map status is published for it and the task " +
          "is failed so that the unmodified scheduler recomputes the output.")
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} failing map " +
          log"${MDC(TASK_ATTEMPT_ID, mapId)} at publication because the driver refused its " +
          log"completion report", failure)
        throw failure
    }
  }

  /** Bytes of payload streamed per reduce partition. */
  override def getPartitionLengths(): Array[Long] = {
    if (sortDelegate != null) sortDelegate.getPartitionLengths() else partitionLengths
  }

  // Initialisation

  /**
   * Settles whether this attempt streams at all, and hands it to the sort-based writer if it does
   * not.
   *
   * @return true when this attempt streams, false when it has been handed to the sort-based
   *     writer
   */
  private def prepareStreaming(): Boolean = {
    if (sortDelegate != null) {
      return false
    }
    requireStreamableDependency()
    // Installed before anything below can take memory, so that every path out of this method --
    // streaming, delegating, or an unexpected throw -- has a hook that returns what was taken.
    installCleanupListeners()
    if (!fallbackPolicy.checkProtocolVersion(handle.protocolVersion)) {
      return degradeToSortShuffle(
        StreamingShuffleFallbackReason.ProtocolVersionMismatch,
        s"this executor cannot speak the protocol version ${handle.protocolVersion} that " +
          s"shuffle $shuffleId was registered under")
    }
    if (fallbackPolicy.hasTripped || fallbackPolicy.shuffleHasFallenBack(shuffleId)) {
      // The cause is read from whatever actually stood streaming down, across the whole closed set
      // rather than only the four specified fallback conditions: a shuffle stood down by a
      // structural decline is stood down just as firmly, and re-declaring it as one of the four
      // would put a measurement nobody took into the record.
      val cause: StreamingShuffleStandDownCause = fallbackPolicy.trippedReason
        .orElse(fallbackPolicy.knownShuffleFallback(shuffleId).flatMap(_.cause))
        .getOrElse(StreamingShuffleStandDownCause.ProducerUnavailable)
      return degradeToSortShuffle(cause,
        s"streaming had already been stood down because ${cause.description}")
    }
    // Registered before the budget is read, because the partition count is the divisor of every
    // buffer ceiling; reading a budget first would divide by the unregistered default of one.
    spillManager.registerPartitionCount(partitionDivisor)
    val capacity = blockPayloadCapacity
    if (capacity <= 0) {
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} cannot frame a block: the " +
        log"buffer budget of ${MDC(MEMORY_SIZE, spillManager.totalBudgetBytes)} bytes leaves " +
        log"${MDC(NUM_BYTES, spillManager.perPartitionBudgetBytes)} bytes per partition across " +
        log"${MDC(NUM_PARTITIONS, partitionDivisor)} partitions and " +
        log"${MDC(COUNT, spillManager.concurrentTaskSlots)} concurrent task slot(s); increase " +
        log"${MDC(CONFIG, config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT.key)} or reduce the " +
        log"partition count")
      return degradeToSortShuffle(
        StreamingShuffleFallbackReason.MemoryPressure,
        s"the buffer budget of ${spillManager.totalBudgetBytes} bytes cannot frame one block for " +
          s"one of $partitionDivisor partitions across ${spillManager.concurrentTaskSlots} " +
          "concurrent task slot(s)")
    }
    val ledgerEnvelope = producerLedgerEnvelopeBytes
    if (!backpressure.tryReserveMetadataQuota(ledgerEnvelope)) {
      backpressure.reportBufferAllocationFailure(producerKey(0))
      fallbackPolicy.recordAllocationGrant(ledgerEnvelope, 0L)
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not reserve " +
        log"${MDC(MEMORY_SIZE, ledgerEnvelope)} bytes of producer-ledger metadata for " +
        log"${MDC(NUM_PARTITIONS, declaredPartitions)} partitions within the executor-wide " +
        log"streaming allowance")
      return degradeToSortShuffle(StreamingShuffleFallbackReason.MemoryPressure,
        s"credit-ledger metadata for $declaredPartitions partitions could not be taken from the " +
          s"executor-wide streaming allowance of ${spillManager.totalBudgetBytes} bytes")
    }
    producerLedgerMetadataRemaining = ledgerEnvelope
    val envelope = reserveFramingEnvelope(capacity)
    if (envelope <= 0L) {
      // Reached only when even one minimum accumulator per partition cannot be taken, which is a
      // budget too small for this shuffle's width rather than a moment of pressure.
      fallbackPolicy.recordAllocationGrant(framingEnvelopeBytes, 0L)
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not reserve framing " +
        log"scratch for ${MDC(NUM_PARTITIONS, declaredPartitions)} partitions even at " +
        log"${MDC(MEMORY_SIZE, StreamingShuffleWriter.MIN_ACCUMULATOR_BYTES)} bytes each, " +
        log"holding ${MDC(NUM_BYTES, spillManager.bufferedBytes)} buffered and " +
        log"${MDC(THRESHOLD, spillManager.scratchBytes)} scratch bytes of " +
        log"${MDC(MAX_SIZE, spillManager.totalBudgetBytes)} budgeted")
      return degradeToSortShuffle(
        StreamingShuffleFallbackReason.MemoryPressure,
        s"framing scratch for $declaredPartitions partitions could not be taken from a buffer " +
          s"budget of ${spillManager.totalBudgetBytes} bytes at any admissible block size")
    }
    framingEnvelopeRemaining = envelope
    true
  }

  /**
   * Takes this attempt's whole framing reservation, shrinking the block size until it fits.
   *
   * @param derivedCapacity the block size the budget arithmetic asked for
   * @return the reservation actually taken, or zero when even the floor was refused
   */
  private def reserveFramingEnvelope(derivedCapacity: Int): Long = {
    var candidate = derivedCapacity
    var taken = 0L
    var settled = false
    while (!settled) {
      negotiatedCapacityBytes = candidate
      val envelope = framingEnvelopeBytes
      if (spillManager.reserveScratch(envelope)) {
        taken = envelope
        settled = true
      } else if (candidate <= StreamingShuffleWriter.MIN_ACCUMULATOR_BYTES) {
        // The floor was refused, so there is nothing smaller to ask for.
        negotiatedCapacityBytes = 0
        settled = true
      } else {
        val next = math.max(StreamingShuffleWriter.MIN_ACCUMULATOR_BYTES, candidate / 2)
        if (debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not reserve " +
            log"${MDC(MEMORY_SIZE, envelope)} bytes of framing scratch for " +
            log"${MDC(NUM_PARTITIONS, declaredPartitions)} partitions; retrying with a block " +
            log"size of ${MDC(NUM_BYTES, next)} bytes")
        }
        candidate = next
      }
    }
    taken
  }

  /**
   * Hands this whole attempt to the sort-based writer, after standing the shuffle down for every
   * participant.
   *
   * @param cause the trip condition to record, one of the four specified fallback conditions
   * @param detail operator-facing context, recorded with the declaration on the driver
   * @return false always, so a caller can `return degradeToSortShuffle(...)` and read as a
   *     decision
   */
  private def degradeToSortShuffle(
      cause: StreamingShuffleStandDownCause,
      detail: String): Boolean = {
    val state = coordinatorGateway.declareFallback(shuffleId, cause,
      s"$detail, observed by the streaming producer of map $mapId attempt " +
        s"${context.taskAttemptId()}")
    if (fallbackPolicy.observeShuffleFallback(shuffleId, state)) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} stood streaming down for every " +
        log"participant at epoch ${MDC(EPOCH, state.declaredAtEpoch)}: " +
        log"${MDC(REASON, state.reasonName)}")
    }
    // Marked as announced so that a later fire-and-forget stand-down on this executor does not
    // repeat an ask this one has already made and had answered.
    fallbackPolicy.claimFallbackAnnouncement(shuffleId)
    if (!state.fallenBack && !fallbackPolicy.shuffleHasFallenBack(shuffleId)) {
      throw new SparkException(s"Streaming shuffle $shuffleId cannot stream map $mapId attempt " +
        s"${context.taskAttemptId()} because $detail, and could not stand the shuffle down for " +
        "every participant, so this attempt fails and is retried rather than being written by a " +
        "second shuffle implementation while the rest of the shuffle keeps streaming.")
    }
    // Every streaming owner of this attempt is retired BEFORE the delegate exists, and the order is
    // the point: from the moment `sortDelegate` is non-null this writer forwards its whole contract
    // to it -- `stop` included -- so anything still published at that instant is published for the
    // life of the executor with nothing left that would ever retire it.
    withdrawPrePublishedOwnership(detail)
    sortDelegate = sortShuffleWriter()
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
      log"${MDC(TASK_ATTEMPT_ID, mapId)} will not stream because ${MDC(REASON, detail)}; the " +
      log"shuffle has stood streaming down for every participant, this generation has been " +
      log"retired from every owner of it on this executor, and the sort-based shuffle writes " +
      log"this map output, so this task completes rather than failing")
    false
  }

  /** Prepares the streaming path, exactly once per writer. */
  private def initializeStreaming(): Unit = {
    if (!initialized) {
      initialized = true
      try {
        publishStreamingProducer()
      } catch {
        case NonFatal(e) =>
          releaseAfterFailure("its producer could not be initialised")
          throw e
      }
    }
  }

  /**
   * The publishing half of [[initializeStreaming]], separated so that the unwind has one subject.
   */
  private def publishStreamingProducer(): Unit = {
    spillManager.registerPartitionCount(partitionDivisor)
    // The verdict is acted upon rather than discarded.
    if (!blockResolver.registerProducer(shuffleId, mapId, context.taskAttemptId(), spillManager)) {
      throw new SparkException(s"Streaming shuffle $shuffleId could not publish the retained " +
        s"output of map $mapId attempt ${context.taskAttemptId()}: either a newer generation of " +
        "that map output is registered or this executor has stopped streaming, so this attempt " +
        "must not stream and is failed so that its retry is served correctly.")
    }
    // Consumers can legitimately subscribe while this map task is live but before its first call to
    // `write`.
    serverHandler.registerPendingRetainedConsumers()
    installCleanupListeners()
    backpressure.registerShuffle(shuffleId, partitionDivisor)
    serverHandler.registerTaskAttempt(egressPriority)
    val nowMillis = clock.getTimeMillis()
    lastMaintenanceMillis = nowMillis
    lastHeartbeatMillis = nowMillis
    lastCoordinatorHeartbeatMillis = nowMillis
    throughputWindowOpenedMillis = nowMillis
    lastRetainedDurabilityMillis = nowMillis
    // Forces the derivation, so it either succeeds and is logged once, or refuses before a single
    // record has been buffered.
    val capacity = blockPayloadCapacity
    verifyBudgetContract()
    // Bounded per executor rather than emitted per map task: see [[budgetLogAggregator]] for why a
    // per-task record is a log volume proportional to the workload's task rate.
    val entry = log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
      log"${MDC(TASK_ATTEMPT_ID, mapId)} streams ${MDC(NUM_PARTITIONS, declaredPartitions)} " +
      log"partitions with a buffer budget of " +
      log"${MDC(MEMORY_SIZE, spillManager.totalBudgetBytes)} bytes " +
      log"(${MDC(VALUE, bufferSizePercent)}% of executor memory), " +
      log"${MDC(NUM_BYTES, spillManager.perPartitionBudgetBytes)} bytes per partition, " +
      log"spilling at ${MDC(THRESHOLD, spillThresholdPercent)}% and framing blocks of at most " +
      log"${MDC(COUNT, capacity)} bytes"
    StreamingShuffleWriter.budgetLogAggregator.record(clock.getTimeMillis()) match {
      case Some(summary) =>
        logInfo(entry + log"; this executor has started " +
          log"${MDC(NUM_TASKS, summary.occurrences)} streaming map task(s), " +
          log"${MDC(NUM_SKIPPED, summary.unreported)} of whose identical report(s) were " +
          log"suppressed to keep the executor inside its log budget")
      case None =>
        // Under the feature's own key the record is restored exactly as it was, at the level it was
        // emitted at.
        if (debugEnabled) {
          logInfo(entry)
        }
    }
  }

  /**
   * Asserts the one property of a dependency that streaming can never express, before any output
   * state exists.
   */
  private def requireStreamableDependency(): Unit = {
    require(!dep.mapSideCombine,
      s"Streaming shuffle $shuffleId declares map-side combine, which the streaming path cannot " +
        "express: a combined value is only correct once every record for its key has been seen, " +
        "so such a dependency must be registered on the sort-based path instead of being streamed.")
  }

  /** Checks that the allowance the spill manager publishes really is the specified quotient. */
  private def verifyBudgetContract(): Unit = {
    val expected = math.max(1L, spillManager.totalBudgetBytes / partitionDivisor)
    val published = spillManager.perPartitionBudgetBytes
    require(published == expected,
      s"The streaming shuffle per-partition buffer allowance must be the aggregate budget of " +
        s"${spillManager.totalBudgetBytes} bytes divided by $partitionDivisor partitions, i.e. " +
        s"$expected bytes, but $published bytes were published")
  }

  // Record path

  /** Appends one record to its partition's stream, emitting blocks as boundaries are crossed. */
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
    if (recordsSinceMaintenance >= MAINTENANCE_RECORD_INTERVAL) {
      // A shuffle of tiny records may go a long time between block boundaries, and heartbeats,
      // acknowledgement drainage and the liveness check must not wait for one.
      maintainStreams()
    }
  }

  /** The heap one partition's framing accumulator occupies, and therefore what it is charged. */
  private def accumulatorFootprintBytes: Long =
    math.max(MIN_ACCUMULATOR_BYTES, math.max(1, blockPayloadCapacity)).toLong

  /** Returns the egress state of one partition, opening its stream on first use. */
  private def stateFor(partitionId: Int): PartitionEgressState = {
    val existing = partitionStates(partitionId)
    if (existing != null) {
      existing
    } else {
      val scratchBytes = accumulatorFootprintBytes
      val usesFramingEnvelope = framingEnvelopeRemaining >= scratchBytes
      if (!usesFramingEnvelope && !spillManager.reserveScratch(scratchBytes)) {
        backpressure.reportBufferAllocationFailure(producerKey(partitionId))
        standDownForMemoryPressure(scratchBytes, 0L)
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not reserve " +
          log"${MDC(MEMORY_SIZE, scratchBytes)} bytes of framing scratch for partition " +
          log"${MDC(PARTITION_ID, partitionId)}, holding " +
          log"${MDC(NUM_BYTES, spillManager.bufferedBytes)} buffered and " +
          log"${MDC(THRESHOLD, spillManager.scratchBytes)} scratch bytes of " +
          log"${MDC(MAX_SIZE, spillManager.totalBudgetBytes)} budgeted; standing the shuffle " +
          log"down so it is recomputed on the sort-based path")
        // The same non-failing terminus as every other mid-write trip: this attempt completes with
        // its output withdrawn rather than failing, so no task attempt is consumed.
        throw new StreamingShuffleWriter.StandDownSignal(
          fallbackPolicy.trippedReason.getOrElse(StreamingShuffleFallbackReason.MemoryPressure),
          s"framing scratch of $scratchBytes bytes could not be reserved for partition " +
            s"$partitionId, so that partition cannot be framed at all")
      }
      val streamKey = producerKey(partitionId)
      val metadataBytes = BackpressureProtocol.STREAM_LEDGER_BASE_BYTES
      if (producerLedgerMetadataRemaining < metadataBytes ||
          !backpressure.registerStreamWithReservedMetadata(
            streamKey, spillManager.perPartitionBudgetBytes)) {
        if (!usesFramingEnvelope) {
          spillManager.releaseScratch(scratchBytes)
        }
        backpressure.reportBufferAllocationFailure(streamKey)
        standDownForMemoryPressure(metadataBytes, 0L)
        throw new StreamingShuffleWriter.StandDownSignal(
          fallbackPolicy.trippedReason.getOrElse(StreamingShuffleFallbackReason.MemoryPressure),
          s"credit-ledger metadata of $metadataBytes bytes could not be reserved for partition " +
            s"$partitionId, so that partition cannot be streamed within the executor budget")
      }
      producerLedgerMetadataRemaining -= metadataBytes
      if (usesFramingEnvelope) {
        framingEnvelopeRemaining -= scratchBytes
      }
      val state = new PartitionEgressState(partitionId, blockPayloadCapacity)
      state.scratchReservedBytes = scratchBytes
      partitionStates(partitionId) = state
      activePartitions += partitionId
      // Register before the serializer is constructed.
      state.stream = serializerInstance.serializeStream(
        serializerManager.wrapStream(ShuffleBlockId(shuffleId, mapId, partitionId),
          state.accumulator))
      state
    }
  }

  /** Streams one detached fixed segment as a block. */
  private def emitBlock(state: PartitionEgressState, payload: Array[Byte]): Unit = {
    val length = payload.length
    val sequenceNumber = state.nextSequenceNumber
    // Only the framing, admission and hand-off are charged as shuffle write time, and only these.
    val startedAtNanos = clock.nanoTime()
    val assigned = try {
      admitBlock(state, sequenceNumber, payload)
      serverHandler.enqueueBlock(state.partitionId, length)
    } finally {
      writeMetrics.incWriteTime(clock.nanoTime() - startedAtNanos)
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
   */
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
          // Relief landed somewhere.
          barrenRounds = 0
        } else if (deferrals < MAX_EVICTION_DEFERRALS && spillManager.evictionInFlight) {
          // Memory is actively being reclaimed by another thread; this round freeing nothing says
          // nothing about whether the budget is exhausted.
          deferrals += 1
          // Hand the CPU to whichever thread is doing the reclaiming.
          Thread.`yield`()
        } else {
          barrenRounds += 1
        }
      }
    }
    if (!admitted) {
      val requested = payload.length.toLong + MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES
      // Reported through the *non-degrading* transport signal rather than through
      // [[BackpressureProtocol.reportBufferAllocationFailure]], which would declare the subsystem
      // degraded from inside an accounting call.
      backpressure.reportDurableSpillAdmission(producerKey(state.partitionId))
      // The fallback policy is told, unconditionally, because reaching this line *is* the
      // specification's second trip condition: "memory pressure prevents buffer allocation".
      fallbackPolicy.recordAllocationGrant(requested, 0L)
      // The sticky signal is consumed by the report so that a later rescue is judged on its own
      // evidence rather than on this one; the policy's own verdict is latched and unaffected.
      spillManager.clearMemoryPressure()
      if (!spillManager.admitDurably(state.partitionId, sequenceNumber, payload)) {
        if (spillManager.diskPressureDetected || spillManager.memoryPressureDetected) {
          backpressure.reportBufferAllocationFailure(producerKey(state.partitionId))
          standDownForMemoryPressure(requested, 0L)
          throw new StreamingShuffleWriter.StandDownSignal(
            fallbackPolicy.trippedReason.getOrElse(
              StreamingShuffleFallbackReason.MemoryPressure),
            s"the retained-output ceiling refused $requested bytes for partition " +
              s"${state.partitionId}; streaming yielded before exhausting executor memory or " +
              "local disk")
        }
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not reserve " +
          log"${MDC(MEMORY_SIZE, requested)} bytes for partition " +
          log"${MDC(PARTITION_ID, state.partitionId)} after ${MDC(MAX_ATTEMPTS, round)} eviction " +
          log"attempts, holding ${MDC(NUM_BYTES, spillManager.bufferedBytes)} of " +
          log"${MDC(THRESHOLD, spillManager.totalBudgetBytes)} budgeted bytes, and could not " +
          log"write it to local disk either")
        // Nowhere left to put the block.
        throw new SparkException(s"Streaming shuffle $shuffleId could not retain $requested " +
          s"bytes for partition ${state.partitionId} in memory or on local disk; the map output " +
          "of this attempt is incomplete and the attempt cannot continue.")
      }
      durableAdmissionsObserved += 1L
      reportDurableAdmission(state.partitionId, requested, round)
    } else if (round > 0 && spillManager.memoryPressureDetected) {
      // Pressure that eviction rescued is a brush, not a prevented allocation, so it is counted and
      // the sticky flag is re-armed rather than escalated.
      memoryPressureBrushes += 1L
      spillManager.clearMemoryPressure()
      if (debugEnabled) {
        logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} recovered a buffer " +
          log"reservation for partition ${MDC(PARTITION_ID, state.partitionId)} after " +
          log"${MDC(COUNT, round)} eviction attempts")
      }
    }
  }

  /**
   * Reports that a block met the buffer allowance and went to local disk instead of into memory.
   */
  private def reportDurableAdmission(partitionId: Int, requestedBytes: Long, rounds: Int): Unit = {
    StreamingShuffleWriter.durableAdmissionLogAggregator
        .record(clock.getTimeMillis(), requestedBytes) match {
      case Some(summary) =>
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} met its buffer allowance " +
          log"of ${MDC(THRESHOLD, spillManager.totalBudgetBytes)} bytes and wrote a " +
          log"${MDC(MEMORY_SIZE, requestedBytes)} byte block of partition " +
          log"${MDC(PARTITION_ID, partitionId)} straight to local disk after " +
          log"${MDC(MAX_ATTEMPTS, rounds)} eviction attempt(s); the shuffle continues on the " +
          log"streaming path (${MDC(COUNT, summary.occurrences)} such blocks totalling " +
          log"${MDC(NUM_BYTES, summary.volumeBytes)} bytes on this executor so far, " +
          log"${MDC(NUM_SKIPPED, summary.unreported)} not reported individually)")
      case None =>
        if (debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} wrote a " +
            log"${MDC(MEMORY_SIZE, requestedBytes)} byte block of partition " +
            log"${MDC(PARTITION_ID, partitionId)} straight to local disk " +
            log"(${MDC(COUNT, durableAdmissionsObserved)} so far in this task)")
        }
    }
  }

  /** Runs the periodic streaming duties, at most once per [[MAINTENANCE_INTERVAL_MS]]. */
  private def maintainStreams(): Unit = {
    recordsSinceMaintenance = 0
    val nowMillis = clock.getTimeMillis()
    if (nowMillis - lastMaintenanceMillis >= MAINTENANCE_INTERVAL_MS) {
      lastMaintenanceMillis = nowMillis
      drainAcknowledgements()
      // Both figures are executor-wide, and they have to be.
      backpressure.reportBufferUtilization(
        shuffleId, spillManager.executorReservedBytes, spillManager.totalBudgetBytes)
      pollSpill()
      // After the threshold poll and before the liveness duties: pressure has first claim on the
      // eviction machinery, and securing unclaimed output must not delay a heartbeat.
      flushRetainedOutputAhead(nowMillis)
      sendHeartbeatsIfDue(nowMillis)
      handleConsumerStalls(nowMillis)
      reportProducerThroughput(nowMillis)
      // Lets the protocol advance its own timers even when no message has arrived, which is what
      // makes an acknowledgement gap observable rather than merely inferable.
      backpressure.pollOnce()
      publishWriteMetrics()
      // Last, because this is the one maintenance duty that can stand this producer down, and a
      // stand-down raises.
      standDownIfRetired(nowMillis)
    }
  }

  /**
   * Refreshes this generation's registration with the coordinator, on the refresh cadence.
   *
   * @param nowMillis the caller's own clock reading, so the cadence is measured once per pass
   * @return what the driver reported, or `None` when it was not asked on this pass or could not
   *     be reached.
   */
  private def refreshCoordinatorRegistration(
      nowMillis: Long): Option[StreamingShuffleProducerLiveness] = {
    val elapsedMillis = nowMillis - lastCoordinatorHeartbeatMillis
    if (elapsedMillis < COORDINATOR_HEARTBEAT_INTERVAL_MS) {
      None
    } else {
      lastCoordinatorHeartbeatMillis = nowMillis
      val generation = StreamingShuffleProducerGeneration(context.partitionId(), mapId,
        context.taskAttemptId())
      coordinatorGateway.heartbeatProducer(shuffleId, generation)
    }
  }

  /** Fails this task when the driver no longer holds its producer generation. */
  private def standDownIfRetired(nowMillis: Long): Unit = {
    if (!finished) {
      refreshCoordinatorRegistration(nowMillis).foreach { liveness =>
        if (!liveness.live) {
          // Two retirements reach here and they demand different answers.
          if (fallbackPolicy.shuffleHasFallenBack(shuffleId)) {
            // Across the whole closed set, for the same reason the pre-flight check above reads it
            // that way: the shuffle-wide verdict may be a structural decline, and re-labelling it
            // as one of the four specified conditions would record a measurement nobody took.
            val reason: StreamingShuffleStandDownCause =
              fallbackPolicy.knownShuffleFallback(shuffleId).flatMap(_.cause)
                .orElse(fallbackPolicy.trippedReason)
                .getOrElse(StreamingShuffleStandDownCause.ProducerUnavailable)
            logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} standing down map " +
              log"${MDC(TASK_ATTEMPT_ID, mapId)} because a shuffle-wide fallback retired its " +
              log"producer generation at epoch ${MDC(EPOCH, liveness.coordinatorEpoch)}")
            throw new StreamingShuffleWriter.StandDownSignal(reason,
              s"a shuffle-wide fallback retired the producer generation of map index " +
                s"${context.partitionId()} attempt ${context.taskAttemptId()} at epoch " +
                s"${liveness.coordinatorEpoch}")
          }
          val failure = new SparkException(s"Streaming shuffle $shuffleId no longer holds the " +
            s"producer generation of map index ${context.partitionId()} attempt " +
            s"${context.taskAttemptId()} at epoch ${liveness.coordinatorEpoch}: it has been " +
            "superseded or invalidated by a consumer. The task is failed so that the unmodified " +
            "scheduler recomputes it.")
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} failing map " +
            log"${MDC(TASK_ATTEMPT_ID, mapId)} because the coordinator no longer holds its " +
            log"producer generation at epoch ${MDC(EPOCH, liveness.coordinatorEpoch)}", failure)
          throw failure
        }
      }
    }
  }

  /** Reconciles this writer's view of what every consumer has acknowledged. */
  private def drainAcknowledgements(): Unit = {
    var index = 0
    while (index < activePartitions.length) {
      val state = partitionStates(activePartitions(index))
      // The minimum across every subscribed consumer, so this advances only when the slowest of
      // them has advanced -- which is exactly when retained bytes can have been released.
      val position = serverHandler.acknowledgedPosition(state.partitionId)
      if (position >= 0L && position > state.appliedAckPosition) {
        state.appliedAckPosition = position
        // Progress means the stream is healthy again, so the replay budget is restored in full
        // rather than left partly spent for an unrelated later failure to inherit.
        state.replayAttempts = 0
        state.nextReplayAtMillis = 0L
        state.stallReported = false
        backpressure.confirmReclamation(producerKey(state.partitionId)).foreach { latencyMillis =>
          if (latencyMillis > RECLAMATION_DEADLINE_MS) {
            reclamationBreaches += 1L
            // Bounded on the executor's window, not on this task's first breach.
            StreamingShuffleWriter.reclamationBreachLogAggregator
                .record(clock.getTimeMillis(), latencyMillis) match {
              case Some(summary) =>
                logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} freed buffers for " +
                  log"partition ${MDC(PARTITION_ID, state.partitionId)} " +
                  log"${MDC(DURATION, latencyMillis)} ms after the acknowledgement freeing them, " +
                  log"outside the ${MDC(THRESHOLD, RECLAMATION_DEADLINE_MS)} ms bound " +
                  log"(${MDC(NUM_EVENTS, reclamationBreaches)} breach(es) in this task, " +
                  log"${MDC(COUNT, summary.occurrences)} on this executor, " +
                  log"${MDC(NUM_SKIPPED, summary.unreported)} not reported individually)")
              case None =>
                if (debugEnabled) {
                  logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} freed buffers " +
                    log"for partition ${MDC(PARTITION_ID, state.partitionId)} " +
                    log"${MDC(DURATION, latencyMillis)} ms after the acknowledgement freeing " +
                    log"them, outside the ${MDC(THRESHOLD, RECLAMATION_DEADLINE_MS)} ms bound")
                }
            }
          }
        }
        if (debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
            log"${MDC(PARTITION_ID, state.partitionId)} is retaining " +
            log"${MDC(NUM_BLOCKS, spillManager.retainedBlockCount(state.partitionId))} block(s) " +
            log"after every consumer acknowledged through ${MDC(VALUE, position)}")
        }
      }
      index += 1
    }
  }

  /**
   * Gives the spill manager its polling opportunity and honours the executor-wide spill verdict.
   */
  private def pollSpill(): Unit = {
    if (!spillManager.pollOnce() && backpressure.shouldYield(shuffleId)) {
      spillManager.maybeSpill()
    }
    // Observed unconditionally, and not only when one of the calls above returned that it had just
    // evicted something. This writer is not the only thing that can spill its own buffers: the
    // executor's spill poller evaluates the threshold on its own cadence, and the memory manager
    // invokes the `MemoryConsumer` spill callback under pressure, so an eviction of this task's
    // buffers can complete on a thread that never enters this method. Reporting only self-triggered
    // evictions therefore left the writer claiming "0 spill events" in its summary for a task whose
    // buffers had just been evicted -- a figure an operator would read as "no pressure occurred" --
    // and left `spillsObserved` unusable as a pressure signal. The observation is a single atomic
    // read compared against a local high-water mark on a path that already runs once per
    // maintenance pass rather than once per record, so making it unconditional costs nothing.
    recordSpillObserved()
  }

  /**
   * Secures output that no consumer has come for, in bounded slices, while records are still being
   * produced.
   *
   * @param nowMillis the current time, already read by the maintenance pass
   */
  private def flushRetainedOutputAhead(nowMillis: Long): Unit = {
    if (finished || standDownReason.isDefined || spillManager.isClosed ||
        spillManager.registeredConsumerCount > 0 ||
        nowMillis - lastRetainedDurabilityMillis < RETAINED_DURABILITY_INTERVAL_MS) {
      return
    }
    lastRetainedDurabilityMillis = nowMillis
    val moved = spillManager.flushRetainedForDurability(RETAINED_DURABILITY_BYTES_PER_PASS)
    if (moved > 0L) {
      retainedDurabilityBytesTotal += moved
      retainedDurabilityPassesTotal += 1L
      if (debugEnabled) {
        logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
          log"${MDC(TASK_ATTEMPT_ID, mapId)} secured ${MDC(NUM_BYTES, moved)} unclaimed byte(s) " +
          log"while producing, ${MDC(MEMORY_SIZE, retainedDurabilityBytesTotal)} byte(s) over " +
          log"${MDC(COUNT, retainedDurabilityPassesTotal)} pass(es); no consumer has subscribed, " +
          log"so this is the write the stop would otherwise perform in one burst")
      }
    }
  }

  /** Notices that a spill happened, for this writer's own diagnostics and log. */
  private def recordSpillObserved(): Unit = {
    val observed = spillManager.spillCount
    if (observed > spillsObservedTotal) {
      spillsObservedTotal = observed
      // The spill manager already records every eviction at default level on its own bounded
      // window, with the cumulative totals and the number of evictions each record stands in for,
      // so this line can only duplicate it -- once per observation, on a path that runs as often as
      // the producer spills.
      if (debugEnabled) {
        logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} spilled to disk, now at " +
          log"${MDC(COUNT, observed)} spills holding " +
          log"${MDC(NUM_BYTES, spillManager.bufferedBytes)} of " +
          log"${MDC(THRESHOLD, spillManager.totalBudgetBytes)} budgeted bytes")
      }
    }
  }

  /** Sends a producer heartbeat on every active stream when one is due. */
  private def sendHeartbeatsIfDue(nowMillis: Long): Unit = {
    if (nowMillis - lastHeartbeatMillis >= HEARTBEAT_INTERVAL_MS) {
      lastHeartbeatMillis = nowMillis
      var index = 0
      while (index < activePartitions.length) {
        val state = partitionStates(activePartitions(index))
        if (!state.terminated || serverHandler.pendingBlocksFor(state.partitionId) > 0L ||
            spillManager.retainedBlockCount(state.partitionId) > 0) {
          serverHandler.sendHeartbeat(state.partitionId)
        }
        index += 1
      }
    }
  }

  /** Implements the consumer-failure flow. */
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

  /** Replays one stalled stream's unacknowledged window, or escalates when the budget is spent. */
  private def replayUnacknowledgedWindow(partitionId: Int, nowMillis: Long): Unit = {
    if (partitionId >= 0 && partitionId < declaredPartitions) {
      val state = partitionStates(partitionId)
      if (state != null) {
        if (!state.stallReported) {
          state.stallReported = true
          consumerStalls += 1L
          // The per-partition latch above still governs how often the condition is *counted* --
          // once per stall rather than once per maintenance pass -- but the record itself is
          // bounded on the executor's window.
          StreamingShuffleWriter.consumerStallLogAggregator
              .record(clock.getTimeMillis()) match {
            case Some(summary) =>
              logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} saw no " +
                log"acknowledgement from the consumer of partition " +
                log"${MDC(PARTITION_ID, partitionId)} for " +
                log"${MDC(TIMEOUT, CONSUMER_LIVENESS_TIMEOUT_MS)} ms, holding " +
                log"${MDC(NUM_BYTES, serverHandler.unacknowledgedBytes)} unacknowledged bytes " +
                log"(${MDC(NUM_EVENTS, consumerStalls)} stall(s) in this task, " +
                log"${MDC(COUNT, summary.occurrences)} on this executor, " +
                log"${MDC(NUM_SKIPPED, summary.unreported)} not reported individually)")
            case None =>
              if (debugEnabled) {
                logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} saw no " +
                  log"acknowledgement from the consumer of partition " +
                  log"${MDC(PARTITION_ID, partitionId)} for " +
                  log"${MDC(TIMEOUT, CONSUMER_LIVENESS_TIMEOUT_MS)} ms, holding " +
                  log"${MDC(NUM_BYTES, serverHandler.unacknowledgedBytes)} unacknowledged bytes")
              }
          }
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
              // Up to five attempts for every partition whose consumer is stalled, so the
              // per-attempt record is detail.
              if (debugEnabled) {
                logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} replayed " +
                  log"${MDC(NUM_BLOCKS, replayed)} unacknowledged blocks of partition " +
                  log"${MDC(PARTITION_ID, partitionId)} on attempt " +
                  log"${MDC(COUNT, state.replayAttempts)} of " +
                  log"${MDC(MAX_ATTEMPTS, MAX_REPLAY_ATTEMPTS)}")
              }
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

  /** Returns the replay backoff for one attempt: one second doubled per attempt, capped. */
  private def backoffMillisFor(attempt: Int): Long = {
    val shift = math.min(math.max(attempt - 1, 0), MAX_BACKOFF_SHIFT)
    math.min(REPLAY_BASE_BACKOFF_MS << shift, MAX_REPLAY_BACKOFF_MS)
  }

  /** Reports this producer's own egress rate, and its share of the administered link. */
  private def reportProducerThroughput(nowMillis: Long): Unit = {
    val elapsedMillis = nowMillis - throughputWindowOpenedMillis
    if (elapsedMillis >= THROUGHPUT_WINDOW_MS) {
      val bytes = math.max(0L, payloadBytesStreamedTotal - throughputWindowBaseBytes)
      val bytesPerSecond = (bytes * MILLIS_PER_SECOND) / elapsedMillis
      throughputWindowOpenedMillis = nowMillis
      throughputWindowBaseBytes = payloadBytesStreamedTotal
      fallbackPolicy.recordProducerThroughput(shuffleId, bytesPerSecond.toDouble, nowMillis)
      if (!rateLimiter.isUnlimited) {
        fallbackPolicy.recordLinkUtilization(backpressure.egressBytesPerSecond.toDouble)
      }
      if (debugEnabled) {
        logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} produced " +
          log"${MDC(NUM_BYTES, bytesPerSecond)} bytes per second over the last " +
          log"${MDC(DURATION, elapsedMillis)} ms")
      }
    }
  }

  /**
   * Turns a buffer reservation this executor could not satisfy into a shuffle-wide stand-down.
   *
   * @param requestedBytes the reservation that was asked for, in bytes
   * @param grantedBytes what was actually granted, which is what makes the sample a short grant
   */
  private def standDownForMemoryPressure(requestedBytes: Long, grantedBytes: Long): Unit = {
    fallbackPolicy.recordAllocationGrant(requestedBytes, grantedBytes)
    // The verdict is not consulted here, and deliberately: every caller of this method raises
    // immediately afterwards, so this attempt is failing rather than delegating and no second
    // implementation of the shuffle can be published whatever the coordinator answered.
    val unusedVerdict = declareShuffleFallback(fallbackPolicy.trippedReason
      .orElse(Some(StreamingShuffleFallbackReason.MemoryPressure)))
    if (unusedVerdict.exists(_.fallenBack) && debugEnabled) {
      logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} stood down for every " +
        log"participant before failing map ${MDC(TASK_ATTEMPT_ID, mapId)} for memory pressure")
    }
  }

  /** Stands down cleanly when graceful degradation has tripped. */
  private def checkFallbackPolicy(): Unit = {
    // What reaches here, and what does not.
    val remoteFallback = fallbackPolicy.shuffleHasFallenBack(shuffleId)
    if (!finished && (fallbackPolicy.hasTripped || remoteFallback)) {
      // Read across the whole closed set of causes, not only the four specified fallback
      // conditions: a verdict this executor learned from the driver may be a structural decline,
      // and this producer has to stand down for it just as firmly and report it as what it was.
      val reason: Option[StreamingShuffleStandDownCause] = fallbackPolicy.trippedReason
        .orElse(fallbackPolicy.knownShuffleFallback(shuffleId).flatMap(_.cause))
      val description = reason.map(_.description).getOrElse("an unrecorded condition")
      val name = reason.map(_.toString).getOrElse("Unknown")
      // Declared only for a condition observed here.
      val confirmed =
        if (remoteFallback) true else declareShuffleFallback(reason).exists(_.fallenBack)
      if (!confirmed) {
        throw new SparkException(
          s"Streaming shuffle $shuffleId observed $description at map $mapId but the shuffle " +
            "could not be stood down for every participant, so this attempt must not finish " +
            "through the sort-based writer: doing so would publish sort-based output for a " +
            "shuffle whose other participants are still streaming. Failing the attempt so that " +
            "it is retried; the retry re-attempts the declaration.")
      }
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} is standing down at map " +
        log"${MDC(TASK_ATTEMPT_ID, mapId)} because ${MDC(REASON, description)}; this attempt " +
        log"withdraws its streamed output and finishes the same map output through the " +
        log"sort-based shuffle, so no retry is needed")
      // Recorded, not raised.
      pendingDegradation = Some(StreamingShuffleWriter.MidStreamDegradation(description, name))
    }
  }

  /**
   * Finishes this attempt through the sort-based writer after streaming stood down mid-production.
   *
   * @param degradation the stand-down that was observed, for the log and the delegate's
   *     diagnosis
   * @param remaining the records this attempt had not yet consumed, handed on intact
   */
  private def finishByDegrading(
      degradation: StreamingShuffleWriter.MidStreamDegradation,
      remaining: Iterator[Product2[K, V]]): Unit = {
    // Closing every stream first is what makes the byte range of each partition complete, and it is
    // done for every active partition before any of them is read back so that a failure part way
    // through cannot leave one partition closed and another still open.
    val tails = new Array[Array[Byte]](declaredPartitions)
    var index = 0
    while (index < activePartitions.length) {
      val state = partitionStates(activePartitions(index))
      tails(state.partitionId) = closeForDegradation(state)
      index += 1
    }
    val reason = s"${degradation.description} (${degradation.name})"
    withdrawGeneration(s"streaming stood down mid-production because $reason, and this map " +
      "output is being rewritten by the sort-based shuffle")
    // The withdrawal above is what makes the refusal below safe: it has already retired this
    // generation everywhere, so a consumer that had acknowledged part of this output can no longer
    // read any of it and will recompute rather than mix a surviving prefix with a new attempt's
    // output.
    firstUnreconstructableBlock().foreach { case (partitionId, sequenceNumber) =>
      val refusal = new SparkException(
        StreamingShuffleWriter.unreconstructableBlockMessage(
          shuffleId, mapId, partitionId, sequenceNumber))
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
        log"${MDC(TASK_ATTEMPT_ID, mapId)} cannot rewrite its output through the sort-based " +
        log"shuffle after ${MDC(REASON, reason)}: block ${MDC(COUNT, sequenceNumber)} " +
        log"of partition ${MDC(PARTITION_ID, partitionId)} was acknowledged and released. The " +
        log"attempt fails with its output already withdrawn, so the map stage is recomputed on " +
        log"the sort-based path from its first record", refusal)
      throw refusal
    }
    val reconstructed = activePartitions.toSeq.sorted.iterator.flatMap { partitionId =>
      degradedRecords(partitionStates(partitionId), tails(partitionId))
    }
    sortDelegate = sortShuffleWriter()
    try {
      sortDelegate.write(reconstructed ++ remaining)
    } finally {
      // Whether the delegate succeeded or not, the streaming state has served its purpose: the
      // reconstruction has either been consumed or abandoned, and nothing may ask for it again.
      closeSerializationResources()
      closeSpillState()
      releaseEgressResources()
    }
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
      log"${MDC(TASK_ATTEMPT_ID, mapId)} rewrote ${MDC(RECORDS, recordsAppended)} already " +
      log"streamed record(s) and the remainder of its input through the sort-based shuffle after " +
      log"${MDC(REASON, reason)}; the attempt completes rather than being retried")
  }

  /**
   * The first block of this attempt's streamed output that can no longer be read back, if any.
   *
   * @return the partition and the sequence number of the first block that cannot be read back,
   *     or `None` when every framed block of every active partition is still retained
   */
  private def firstUnreconstructableBlock(): Option[(Int, Long)] = {
    var index = 0
    var missing = Option.empty[(Int, Long)]
    while (index < activePartitions.length && missing.isEmpty) {
      val partitionId = activePartitions(index)
      val framed = partitionStates(partitionId).nextSequenceNumber
      if (framed > 0L) {
        val lowest = spillManager.lowestRetainedSequence(partitionId)
        val highest = spillManager.lastAcceptedSequence(partitionId)
        if (lowest != 0L) {
          // Either a prefix was acknowledged and released, or nothing is retained at all.
          missing = Some((partitionId, 0L))
        } else if (highest < framed - 1L) {
          missing = Some((partitionId, highest + 1L))
        }
      }
      index += 1
    }
    missing
  }

  /**
   * Closes one partition's serialization stream and detaches whatever the accumulator still holds.
   *
   * @param state the partition to close
   * @return the trailing bytes of that partition's stream, or `null` when it ended on a block
   *     boundary and there are none
   */
  private def closeForDegradation(state: PartitionEgressState): Array[Byte] = {
    if (state.closed) {
      null
    } else {
      // Claim closure before invoking the serializer.
      state.closed = true
      state.stream.flush()
      state.stream.close()
      state.accumulator.takeRemaining()
    }
  }

  /**
   * The key-value pairs one partition's streamed bytes hold, read back in the order they went in.
   *
   * @param state the partition whose blocks are to be read back
   * @param tail the trailing bytes [[closeForDegradation]] detached, or `null` if there were
   *     none
   * @return the partition's records
   */
  private def degradedRecords(
      state: PartitionEgressState,
      tail: Array[Byte]): Iterator[Product2[K, V]] = {
    val partitionId = state.partitionId
    val admitted = (0L until state.nextSequenceNumber).iterator.map { sequenceNumber =>
      new ByteArrayInputStream(spillManager.retainedPayload(partitionId, sequenceNumber).getOrElse {
        // The backstop for a gap the window bounds cannot see; [[firstUnreconstructableBlock]] has
        // already refused every gap they can, before the delegate existed.
        throw new SparkException(StreamingShuffleWriter.unreconstructableBlockMessage(
          shuffleId, mapId, partitionId, sequenceNumber))
      }): InputStream
    }
    val segments = if (tail == null) admitted else admitted ++ Iterator.single(
      new ByteArrayInputStream(tail): InputStream)
    val unwrapped = serializerManager.wrapStream(ShuffleBlockId(shuffleId, mapId, partitionId),
      new SequenceInputStream(segments.asJavaEnumeration))
    serializerInstance.deserializeStream(unwrapped).asKeyValueIterator.map {
      case (key, value) => (key.asInstanceOf[K], value.asInstanceOf[V])
    }
  }

  /** Turns this executor's fallback trip into a decision the whole shuffle agrees on. */
  private def declareShuffleFallback(
      cause: Option[StreamingShuffleStandDownCause]): Option[StreamingShuffleFallbackState] = {
    cause.map { tripped =>
      // A verdict already latched is the same answer the coordinator would give, so it is reported
      // rather than re-asked: the claim keeps the wire cost at one ask per shuffle per executor
      // however many participants observe the condition, and the cached state is what every later
      // caller reads.
      if (fallbackPolicy.claimFallbackAnnouncement(shuffleId)) {
        val state = coordinatorGateway.declareFallback(shuffleId, tripped,
          s"observed by the streaming producer of map $mapId attempt ${context.taskAttemptId()}")
        // Cached on the way back, so every other streaming component of this shuffle on this
        // executor -- including a reduce task reading a different shuffle's output alongside it --
        // learns the verdict without an ask of its own.
        if (fallbackPolicy.observeShuffleFallback(shuffleId, state)) {
          logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} stood streaming down for " +
            log"every participant at epoch ${MDC(EPOCH, state.declaredAtEpoch)}: " +
            log"${MDC(REASON, state.reasonName)}")
        }
        state
      } else {
        fallbackPolicy.knownShuffleFallback(shuffleId)
          .getOrElse(StreamingShuffleFallbackState())
      }
    }
  }

  /**
   * Reports that this map output has been streamed in full, so that a consumer can tell a map stage
   * that has finished from one that has not begun.
   *
   * @return `Some(true)` when the driver applied the report, `Some(false)` when it refused it,
   *     and `None` when the driver could not be asked
   */
  private def reportMapOutputComplete(): Option[Boolean] = {
    val generation = StreamingShuffleProducerGeneration(
      context.partitionId(), mapId, context.taskAttemptId())
    val applied = coordinatorGateway.completeProducer(shuffleId, generation)
    if (applied.contains(true) && debugEnabled) {
      logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} recorded map " +
        log"${MDC(MAP_ID, mapId)} index ${MDC(INDEX, context.partitionId())} as streamed to " +
        log"completion")
    }
    applied
  }

  // End of stream

  /** Closes every partition's stream and signals end of stream on all of them. */
  private def finishAllPartitions(): Unit = {
    var index = 0
    while (index < activePartitions.length) {
      finishPartition(partitionStates(activePartitions(index)))
      index += 1
    }
    val startedAtNanos = clock.nanoTime()
    // Every declared partition is terminated in one pass: those this task produced records for,
    // whose blocks were emitted just above, and those it produced nothing for, whose terminator
    // legitimately reports zero blocks.
    serverHandler.terminateStreams(0 until declaredPartitions)
    serverHandler.flushPending()
    writeMetrics.incWriteTime(clock.nanoTime() - startedAtNanos)
    drainAcknowledgements()
    publishWriteMetrics()
  }

  /** Flushes and closes one partition's serialization stream, emits its tail and marks it ended. */
  private def finishPartition(state: PartitionEgressState): Unit = {
    if (!state.closed) {
      // Claim closure before invoking the serializer for the same reason as
      // [[closeForDegradation]]: its final write may trigger a stand-down and re-enter cleanup.
      state.closed = true
      state.stream.flush()
      state.stream.close()
      val tail = state.accumulator.takeRemaining()
      if (tail != null) {
        emitBlock(state, tail)
      }
      state.accumulator.release()
      if (state.scratchReservedBytes > 0L) {
        spillManager.releaseScratch(state.scratchReservedBytes)
        state.scratchReservedBytes = 0L
      }
    }
    if (!state.terminated) {
      // The local ledger is told here; the wire terminator is requested for every partition at once
      // by the caller, after this loop has emitted the last block of each of them.
      backpressure.onStreamTermination(producerKey(state.partitionId), state.nextSequenceNumber)
      state.terminated = true
    }
  }

  /**
   * Pushes everything still owed to a consumer, waiting up to a bound for pacing to allow it.
   *
   * @return true when every subscribed consumer has been sent everything it was queued
   */
  private def drainBeforeStopping(): Boolean = {
    var delivered = false
    try {
      val deadlineMillis = clock.getTimeMillis() + FINAL_DRAIN_TIMEOUT_MS
      var remaining = FINAL_DRAIN_TIMEOUT_MS
      // Bounded by slice count as well as by the deadline.
      var slices = 0
      delivered = serverHandler.awaitDrain(math.min(remaining, MAINTENANCE_INTERVAL_MS))
      while (!delivered && remaining > 0L && slices < MAX_DRAIN_SLICES) {
        slices += 1
        val nowMillis = clock.getTimeMillis()
        drainAcknowledgements()
        sendHeartbeatsIfDue(nowMillis)
        // The coordinator's liveness window is the same length as this drain's deadline, so a drain
        // that runs to its deadline without refreshing would have this producer's address reaped at
        // exactly the moment its last blocks were being delivered.
        refreshCoordinatorRegistration(nowMillis)
        handleConsumerStalls(nowMillis)
        backpressure.pollOnce()
        errorNotifier.throwIfError()
        remaining = deadlineMillis - clock.getTimeMillis()
        if (remaining > 0L) {
          delivered = serverHandler.awaitDrain(math.min(remaining, MAINTENANCE_INTERVAL_MS))
        }
      }
      drainAcknowledgements()
      if (!delivered) {
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
          log"${MDC(TASK_ATTEMPT_ID, mapId)} still owed " +
          log"${MDC(NUM_BYTES, serverHandler.pendingBytes)} framed byte(s) after waiting " +
          log"${MDC(TIMEOUT, FINAL_DRAIN_TIMEOUT_MS)} ms for egress; those blocks stay retained " +
          log"and are replayed when their consumer asks for them")
      }
    } catch {
      case NonFatal(e) =>
        // Swallowed here and not lost: the drain is best-effort, so its own failure must not
        // pre-empt the durability check that follows, and anything the notifier latched is raised
        // by [[requireNoLatchedFailure]] before this attempt may report success.
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not complete its " +
          log"final egress pass for map ${MDC(TASK_ATTEMPT_ID, mapId)}; the blocks stay retained " +
          log"and consumers recover by asking for them or by failing their fetch", e)
    }
    delivered
  }

  /**
   * Makes whatever of this map output is still unacknowledged durable, and hands its files to the
   * block resolver.
   *
   * @return true when every retained byte is durable and owned by the resolver
   */
  private def secureRetainedOutput(): Boolean = {
    if (retainedOutputSecured) {
      return spillManager.spillFilesTransferred && spillManager.bufferedBytes == 0L
    }
    retainedOutputSecured = true
    try {
      val movedBytes = spillManager.spillAllRetained()
      val stillBuffered = spillManager.bufferedBytes
      val files = spillManager.releaseSpillFileOwnership()
      val accepted =
        blockResolver.retainProducerOutput(shuffleId, mapId, context.taskAttemptId(), files)
      if (!accepted) {
        discardDetachedFiles(files)
      }
      if (debugEnabled && (movedBytes > 0L || files.nonEmpty)) {
        logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
          log"${MDC(MAP_ID, mapId)} made ${MDC(NUM_BYTES, movedBytes)} retained byte(s) durable " +
          log"across ${MDC(COUNT, files.size)} spill file(s); transfer accepted: " +
          log"${MDC(VALUE, accepted)}")
      }
      if (stillBuffered > 0L) {
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
          log"${MDC(TASK_ATTEMPT_ID, mapId)} still held " +
          log"${MDC(NUM_BYTES, stillBuffered)} retained byte(s) in memory after being asked to " +
          log"make them durable; those bytes cannot outlive this task")
      }
      accepted && stillBuffered == 0L
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not make the " +
          log"retained output of map ${MDC(TASK_ATTEMPT_ID, mapId)} durable", e)
        false
    }
  }

  /** Unlinks spill files whose ownership was released and then refused. */
  private def discardDetachedFiles(files: Seq[File]): Unit = {
    files.foreach { file =>
      try {
        if (file.exists() && !file.delete()) {
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not delete the " +
            log"detached spill file ${MDC(FILE_NAME, file.getName)} of map " +
            log"${MDC(TASK_ATTEMPT_ID, mapId)}")
        }
      } catch {
        case NonFatal(e) =>
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not delete the " +
            log"detached spill file ${MDC(FILE_NAME, file.getName)} of map " +
            log"${MDC(TASK_ATTEMPT_ID, mapId)}", e)
      }
    }
  }

  /**
   * Fails a successful stop whose output would not be reachable by a consumer.
   *
   * @param delivered whether the final drain wrote everything that was queued, for the
   *     diagnosis
   * @param durable whether the retained window is reachable: on disk and owned by the resolver
   */
  private def requireOutputRecoverable(delivered: Boolean, durable: Boolean): Unit = {
    if (!durable) {
      val queued = serverHandler.pendingBytes
      val buffered = spillManager.bufferedBytes
      throw new SparkException(s"Streaming shuffle $shuffleId map $mapId could not complete: its " +
        s"retained output is not reachable by a consumer (final drain delivered everything " +
        s"queued: $delivered, $queued byte(s) still queued for egress, $buffered byte(s) still " +
        "buffered in task memory), so the attempt must be retried on a fresh generation.")
    }
  }

  /**
   * Raises whatever the error notifier latched, before this attempt is allowed to report success.
   */
  private def requireNoLatchedFailure(): Unit = errorNotifier.throwIfError()

  /** Reports every producer stream of this map output complete on the local ledger. */
  private def completeProducerStreams(): Unit = {
    if (!streamsCompleted) {
      streamsCompleted = true
      var index = 0
      while (index < activePartitions.length) {
        val state = partitionStates(activePartitions(index))
        backpressure.onStreamTermination(producerKey(state.partitionId), state.nextSequenceNumber)
        index += 1
      }
    }
  }

  /** Unregisters every producer stream this writer registered. */
  private def unregisterProducerStreams(): Unit = {
    if (!streamsUnregistered) {
      streamsUnregistered = true
      var index = 0
      while (index < activePartitions.length) {
        val partitionId = activePartitions(index)
        try {
          backpressure.unregisterStream(producerKey(partitionId))
        } catch {
          case NonFatal(e) =>
            logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not unregister " +
              log"the producer ledger of partition ${MDC(PARTITION_ID, partitionId)}", e)
        }
        index += 1
      }
    }
  }

  /** Publishes progress onto the task's standard shuffle-write metrics. */
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

  /**
   * Emits the summary a successful streaming map task contributes to the executor log, bounded per
   * executor rather than per task.
   */
  private def logStreamingSummary(): Unit = {
    val entry = log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
      log"${MDC(TASK_ATTEMPT_ID, mapId)} streamed ${MDC(RECORDS, recordsAppended)} records as " +
      log"${MDC(NUM_BLOCKS, blocksStreamedTotal)} blocks totalling " +
      log"${MDC(NUM_BYTES, payloadBytesStreamedTotal)} payload bytes across " +
      log"${MDC(NUM_PARTITIONS, activePartitions.length)} partitions, with " +
      log"${MDC(COUNT, spillsObservedTotal)} spill events, " +
      log"${MDC(NUM_EVENTS, spillManager.durabilityFlushCount)} durability flushes, " +
      log"${MDC(NUM_SKIPPED, durableAdmissionsObserved)} blocks written straight to disk, " +
      log"${MDC(VALUE, consumerStalls)} consumer stalls and " +
      log"${MDC(MAX_ATTEMPTS, replayAttemptsTotal)} replay attempts"
    StreamingShuffleWriter.summaryLogAggregator
      .record(clock.getTimeMillis(), payloadBytesStreamedTotal) match {
      case Some(summary) =>
        logInfo(entry + log"; this executor has now streamed " +
          log"${MDC(MEMORY_SIZE, summary.volumeBytes)} payload byte(s) across " +
          log"${MDC(NUM_TASKS, summary.occurrences)} map task(s), " +
          log"${MDC(MAX_SIZE, summary.unreported)} of whose summaries were suppressed to keep " +
          log"the executor inside its log budget")
      case None =>
        // At the level it was emitted at, under the feature's own key.
        if (debugEnabled) {
          logInfo(entry)
        }
    }
    logEgressCeilings()
    logProtocolAnomalies()
  }

  /** Reports the egress ceilings this map task's consumers ran into, when any of them did. */
  private def logEgressCeilings(): Unit = {
    val refusedSessions = serverHandler.refusedSessionCount
    val untrackedConsumers = serverHandler.untrackedConsumerCount
    val deferredBlocks = serverHandler.deferredBlockCount
    val owedBlocks = serverHandler.owedBlockCount
    if (refusedSessions > 0L || untrackedConsumers > 0L || deferredBlocks > 0L || owedBlocks > 0L) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
        log"${MDC(TASK_ATTEMPT_ID, mapId)} applied its egress ceilings: " +
        log"${MDC(COUNT, refusedSessions)} channel(s) refused past the session ceiling, " +
        log"${MDC(VALUE, untrackedConsumers)} consumer identity(ies) left untracked, " +
        log"${MDC(NUM_BLOCKS, deferredBlocks)} block reference(s) deferred onto an owed run and " +
        log"${MDC(MAX_SIZE, owedBlocks)} still owed at this point")
    }
  }

  /** Reports the protocol anomalies this map task's egress observed, when it observed any. */
  private def logProtocolAnomalies(): Unit = {
    val anomalies = Seq(
      ("acknowledgement(s) refused as out of range", serverHandler.refusedAckCount),
      ("duplicate acknowledgement(s)", serverHandler.duplicateAckCount),
      ("retained block(s) no longer servable", serverHandler.unservableBlockCount),
      ("block(s) replayed on request", serverHandler.retransmittedBlockCount),
      ("session(s) resumed after a reconnect", serverHandler.resumedSessionCount),
      ("session(s) retired for silence", serverHandler.expiredSessionCount),
      ("consumer(s) retired for silence", serverHandler.expiredConsumerCount),
      ("channel(s) lost while still owing bytes", serverHandler.lossyChannelClosureCount),
      ("misaddressed frame(s)", serverHandler.misaddressedMessageCount))
      .filter(_._2 > 0L)
    if (anomalies.nonEmpty) {
      val rendered = anomalies.map { case (label, count) => s"$count $label" }.mkString(", ")
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
        log"${MDC(TASK_ATTEMPT_ID, mapId)} egress observed ${MDC(DESCRIPTION, rendered)} across " +
        log"${MDC(COUNT, serverHandler.liveConsumerCount)} connected and " +
        log"${MDC(VALUE, serverHandler.trackedConsumerCount)} tracked consumer(s)")
    }
  }

  // Cleanup.

  /**
   * Registers the two releases that must run whatever becomes of this task, exactly once.
   *
   * Task-completion listeners run in reverse registration order, so the spill manager's is
   * installed first and this writer's second, which makes this writer's run first: egress is
   * released while the buffers it referenced are still alive. Both are registered here, from
   * preparation rather than from publication, because the framing reservation is taken before
   * anything is published -- a task killed in between would otherwise hold executor memory with
   * nothing registered to return it. Both registrations are idempotent.
   */
  private def installCleanupListeners(): Unit = {
    if (!cleanupInstalled) {
      cleanupInstalled = true
      spillManager.registerCleanup(context)
      context.addTaskCompletionListener[Unit](_ => releaseOnTaskCompletion())
    }
  }

  /** Releases everything this writer owns, on success, on failure and on cancellation alike. */
  private def releaseOnTaskCompletion(): Unit = {
    try {
      // Completion before release, and both before the ledgers are dropped: a stream reported
      // complete and then unregistered leaves the protocol with no entry at all, which is what the
      // arbitration wants, whereas unregistering first would discard the completion silently.
      completeProducerStreams()
      closeSerializationResources()
      releaseEgressResources()
      unregisterProducerStreams()
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not release map " +
          log"${MDC(TASK_ATTEMPT_ID, mapId)} on task completion", e)
    }
  }

  /** Closes every serialization stream and frees every accumulator. */
  private def closeSerializationResources(): Unit = {
    if (!serializationReleased) {
      serializationReleased = true
      if (framingEnvelopeRemaining > 0L) {
        spillManager.releaseScratch(framingEnvelopeRemaining)
        framingEnvelopeRemaining = 0L
      }
      if (producerLedgerMetadataRemaining > 0L) {
        backpressure.releaseMetadataQuota(producerLedgerMetadataRemaining)
        producerLedgerMetadataRemaining = 0L
      }
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
        if (state.scratchReservedBytes > 0L) {
          spillManager.releaseScratch(state.scratchReservedBytes)
          state.scratchReservedBytes = 0L
        }
        index += 1
      }
    }
  }

  /**
   * Releases the handler's queued references and its channel state -- but only once nothing it
   * could still serve remains published.
   */
  private def releaseEgressResources(): Unit = {
    if (!egressReleased && !(retainedOutputSecured && serverHandler.servesRetainedOutput)) {
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
   * Hands back everything the manager published on this attempt's behalf, before it is delegated.
   *
   * @param detail operator-facing context, recorded with the withdrawal on the driver and
   *     locally
   */
  private def withdrawPrePublishedOwnership(detail: String): Unit = {
    val reason = s"streaming was stood down before its first record because $detail"
    // Retires the driver's producer address, this executor's routing entry, the retained-output
    // registration and the handler's sessions -- every owner of the generation, through the one
    // operation that knows how to retire all of them.
    withdrawGeneration(reason)
    // The buffers and the framing scratch this attempt reserved.
    closeSerializationResources()
    closeSpillState()
    // The shuffle's share of the egress allowance, returned only once no stream of it remains
    // registered on this executor.
    guardedRelease("return this shuffle's share of the executor's egress allowance") {
      if (backpressure.streamCount(shuffleId) == 0) {
        // The guard is what makes this safe for a concurrent attempt of the same shuffle: the share
        // is returned only when this executor holds no ledger of it, so there is by construction no
        // ledger for this call to drop and no other attempt's credit to take away.
        backpressure.unregisterShuffle(shuffleId)
        if (debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} returned the executor " +
            log"state of map ${MDC(TASK_ATTEMPT_ID, mapId)} before delegating to the sort-based " +
            log"writer, no stream ledger of it remaining registered here")
        }
      }
    }
    logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} withdrew every streaming owner " +
      log"of map ${MDC(TASK_ATTEMPT_ID, mapId)} before handing the attempt to the sort-based " +
      log"writer, so no consumer can resolve a producer this attempt will never serve")
  }

  /**
   * Runs one release step, reporting rather than propagating a failure.
   *
   * @param what the step being attempted, named for the diagnostic
   * @param release the step
   */
  private def guardedRelease(what: String)(release: => Unit): Unit = {
    try {
      release
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not " +
          log"${MDC(REASON, what)} for map ${MDC(TASK_ATTEMPT_ID, mapId)}", e)
    }
  }

  /**
   * Retires this generation and frees everything it holds, for an attempt that will not succeed.
   *
   * @param reason operator-facing context, recorded with the withdrawal on the driver and
   *     locally
   */
  private def releaseAfterFailure(reason: String): Unit = {
    withdrawGeneration(reason)
    unregisterProducerStreams()
    // The buffers are this task's execution memory, so releasing them is the one part of the
    // withdrawal only the task thread may do.
    closeSpillState()
  }

  /**
   * Retires this generation from every owner of it on this executor: the driver's producer address,
   * the routing entry, the retained-output registration and the handler's sessions.
   *
   * Withdrawal always precedes adopting the sort-based delegate. A generation still published while
   * another writer produces the same map output is one a consumer can resolve and read from, so the
   * order is what stops a reduce task from mixing streamed bytes with delegated output.
   */
  private def withdrawGeneration(reason: String): Unit = {
    try {
      coordinatorGateway.invalidateProducer(shuffleId,
        StreamingShuffleProducerGeneration(context.partitionId(), mapId, context.taskAttemptId()),
        StreamingShuffleInvalidationReason.IncompleteStream, reason)
      serverHandler.withdrawGeneration(reason)
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not withdraw the " +
          log"producer generation of map ${MDC(TASK_ATTEMPT_ID, mapId)}", e)
    }
  }

  /** Frees every buffer and deletes every spill file, for an attempt that will not succeed. */
  private def closeSpillState(): Unit = {
    try {
      spillManager.close()
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not release buffers " +
          log"and spill files for map ${MDC(TASK_ATTEMPT_ID, mapId)}", e)
    }
  }

  // Inspection.

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

  /**
   * Bytes this writer secured to local disk while it was still producing, because no consumer had
   * subscribed to take them.
   */
  def retainedDurabilityBytesAheadOfStop: Long = retainedDurabilityBytesTotal

  /** Pipelined durability passes this writer performed while producing. */
  def retainedDurabilityPassCount: Long = retainedDurabilityPassesTotal

  /** Streams that crossed the consumer-liveness window without acknowledgement progress. */
  def consumerStallCount: Long = consumerStalls

  /** Retransmission attempts made in response to a stalled consumer. */
  def replayAttemptCount: Long = replayAttemptsTotal

  /**
   * Spill events observed by this writer, as counted by the spill manager: evictions the budget
   * forced and blocks written straight to disk because the allowance could not admit them.
   */
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

  /** Whether [[stop]] has already been entered, in any of its outcomes. */
  def isStopped: Boolean = stopState != StopState.NotEntered

  /** The map status produced on success, or `None` before a successful stop. */
  def producedMapStatus: Option[MapStatus] = Option(mapStatus)

  /** What stood this attempt's streaming down mid-write, if anything did. */
  def standDownFallbackReason: Option[StreamingShuffleStandDownCause] = standDownReason

  /** Operator-facing description of why streaming stood down mid-write, if it did. */
  def standDownDescription: Option[String] = Option(standDownDetail)

  // Internal state types

  /**
   * Everything this writer tracks for one reduce partition.
   *
   * @param partitionId the reduce partition this state belongs to
   * @param initialCapacityBytes the accumulator's starting size, which is the block size, so
   *     that a partition that fills exactly one block never grows its buffer
   */
  private final class PartitionEgressState(val partitionId: Int, initialCapacityBytes: Int) {

    /**
     * Serialized bytes produced for this partition and not yet handed off as a complete segment.
     */
    val accumulator: BlockAccumulator =
      new BlockAccumulator(initialCapacityBytes, payload => emitBlock(this, payload))

    /** The one serialization stream of this partition. */
    var stream: SerializationStream = _

    var nextSequenceNumber: Long = 0L

    var recordsAppended: Long = 0L

    /** Payload bytes streamed for this partition, framing excluded. */
    var payloadBytesStreamed: Long = 0L

    var appliedAckPosition: Long = NOTHING_APPLIED

    /** Retransmission attempts spent on this stall, reset by any acknowledgement progress. */
    var replayAttempts: Int = 0

    /** The clock reading at which the next retransmission attempt becomes due. */
    var nextReplayAtMillis: Long = 0L

    /** Whether this stall was already reported, so it is logged once and not per poll. */
    var stallReported: Boolean = false

    /** Bytes reserved from the buffer budget for this partition's accumulator. */
    var scratchReservedBytes: Long = 0L

    var closed: Boolean = false

    var terminated: Boolean = false
  }

  /**
   * A fixed-segment output stream that emits a block at the instant its segment fills.
   *
   * @param segmentCapacityBytes exact payload capacity of one emitted block
   * @param emitFullSegment callback that adopts each full segment before another is allocated
   */
  private final class BlockAccumulator(
      segmentCapacityBytes: Int,
      emitFullSegment: Array[Byte] => Unit) extends OutputStream {

    require(segmentCapacityBytes > 0,
      s"A streaming shuffle accumulator segment must be positive, but was $segmentCapacityBytes")
    require(emitFullSegment != null, "The full-segment callback must not be null")

    private var buffer: Array[Byte] = new Array[Byte](segmentCapacityBytes)

    private var count: Int = 0

    /** Bytes currently held. */
    def size: Int = count

    /** The fixed segment capacity, exposed for diagnostics and tests of the segmentation policy. */
    def capacity: Int = segmentCapacityBytes

    override def write(oneByte: Int): Unit = {
      buffer(count) = oneByte.toByte
      count += 1
      emitIfFull()
    }

    override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
      if (length < 0 || offset < 0 || offset > bytes.length - length) {
        throw new IndexOutOfBoundsException(
          s"offset $offset and length $length do not fit an array of ${bytes.length} bytes")
      }
      var sourceOffset = offset
      var remaining = length
      while (remaining > 0) {
        val copied = math.min(remaining, segmentCapacityBytes - count)
        System.arraycopy(bytes, sourceOffset, buffer, count, copied)
        count += copied
        sourceOffset += copied
        remaining -= copied
        emitIfFull()
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
     * Detaches the bounded tail after the serialization stream has been closed.
     *
     * @return the tail owned by the caller, or `null` when the stream ended on a block boundary
     */
    def takeRemaining(): Array[Byte] = {
      if (count == 0) {
        null
      } else {
        val tail = new Array[Byte](count)
        System.arraycopy(buffer, 0, tail, 0, count)
        buffer = EMPTY_PAYLOAD
        count = 0
        tail
      }
    }

    /** Drops the buffer so it can be collected, leaving the accumulator empty and reusable. */
    def release(): Unit = {
      buffer = EMPTY_PAYLOAD
      count = 0
    }

    /**
     * Hands off a full segment, then allocates the next one against the standing scratch charge.
     */
    private def emitIfFull(): Unit = {
      if (count == segmentCapacityBytes) {
        val full = buffer
        buffer = EMPTY_PAYLOAD
        count = 0
        // The next segment is allocated whether the hand-off returned or raised.
        try {
          emitFullSegment(full)
        } finally {
          buffer = new Array[Byte](segmentCapacityBytes)
        }
      }
    }
  }
}

/** Constants of the streaming shuffle producer. */
private[spark] object StreamingShuffleWriter {

  /**
   * A shuffle-wide stand-down observed by a producer that had already consumed records.
   *
   * @param description the operator-facing reason, as the fallback reason describes itself
   * @param name the reason's stable name, for a message that has to be matched rather than read
   */
  final case class MidStreamDegradation(description: String, name: String)

  /** How far a writer's stop protocol has progressed. */
  sealed trait StopState

  /** The states of a writer's stop protocol. */
  object StopState {

    /** No stop has begun, so the requested sequence runs. */
    case object NotEntered extends StopState

    /** A stop is executing; a nested call is a no-op rather than a recursion. */
    case object Running extends StopState

    /** A successful stop completed and produced a map status. */
    case object Succeeded extends StopState

    /** Everything the writer held has been released; no further stop can change anything. */
    case object Failed extends StopState
  }

  /**
   * Raised inside a write to stop streaming without failing the task.
   *
   * @param reason the trip condition observed, one of the four the feature specifies
   * @param detail operator-facing description of what was observed
   */
  private[streaming] class StandDownSignal(
      val reason: StreamingShuffleStandDownCause,
      val detail: String)
    extends Exception(detail, null, false, false)

  /**
   * The one explanation of a reconstruction that cannot be completed, shared by the check that runs
   * before the sort-based delegate exists and by the per-block backstop inside the reconstruction
   * itself.
   *
   * @param shuffleId the shuffle whose map output cannot be rewritten
   * @param mapId the map output that cannot be rewritten
   * @param partitionId the reduce partition holding the block that cannot be read back
   * @param sequenceNumber the sequence number of that block
   * @return the refusal message
   */
  private[streaming] def unreconstructableBlockMessage(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      sequenceNumber: Long): String = {
    s"Streaming shuffle $shuffleId cannot finish map $mapId through the sort-based shuffle " +
      s"because block $sequenceNumber of partition $partitionId is no longer retained: a " +
      "consumer had already acknowledged it, which released it. This attempt is failed, with its " +
      "streamed output already withdrawn from every owner of it, so the map output is recomputed."
  }

  /** Executor-scoped aggregation of the "block written straight to local disk" report. */
  private[streaming] val durableAdmissionLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /**
   * Executor-scoped aggregation of the two records every streaming map task would otherwise emit:
   * the budget it starts with, and the summary of what it streamed.
   */
  private[streaming] val budgetLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val summaryLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /** Executor-scoped aggregation of the two recurring producer-side stall records. */
  private[streaming] val reclamationBreachLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val consumerStallLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /** Returns the executor-scoped log aggregation above to its initial state. */
  private[streaming] def resetLogAggregationForTesting(): Unit = {
    durableAdmissionLogAggregator.reset()
    budgetLogAggregator.reset()
    summaryLogAggregator.reset()
    reclamationBreachLogAggregator.reset()
    consumerStallLogAggregator.reset()
  }

  val MAINTENANCE_INTERVAL_MS: Long = 100L

  /** Records between two consecutive maintenance opportunities. */
  val MAINTENANCE_RECORD_INTERVAL: Int = 1024

  /** The bound within which buffers must be released after an acknowledgement frees them. */
  val RECLAMATION_DEADLINE_MS: Long = BackpressureProtocol.RECLAMATION_DEADLINE_MS

  val HEARTBEAT_INTERVAL_MS: Long = StreamingShuffleServerHandler.PRODUCER_HEARTBEAT_INTERVAL_MS

  /** Interval at which a producer refreshes its registration with the coordinator. */
  val COORDINATOR_HEARTBEAT_INTERVAL_MS: Long =
    StreamingShuffleCoordinator.PRODUCER_CONNECTION_TIMEOUT_MS

  /** Acknowledgement gap after which a consumer is treated as unresponsive. */
  val CONSUMER_LIVENESS_TIMEOUT_MS: Long =
    StreamingShuffleServerHandler.CONSUMER_LIVENESS_TIMEOUT_MS

  /**
   * How long a retained block may sit unclaimed before the producer starts making it durable rather
   * than waiting for its own stop to do it.
   */
  val RETAINED_DURABILITY_INTERVAL_MS: Long = 5L * MAINTENANCE_INTERVAL_MS

  /** Most this producer moves out of memory in one pipelined durability pass. */
  val RETAINED_DURABILITY_BYTES_PER_PASS: Long = 8L * MemorySpillManager.MAX_BLOCK_PAYLOAD_BYTES

  /** Longest the successful stop waits for paced egress to release what is still queued. */
  val FINAL_DRAIN_TIMEOUT_MS: Long = CONSUMER_LIVENESS_TIMEOUT_MS

  /** The most maintenance slices the final drain will take. */
  val MAX_DRAIN_SLICES: Int = (FINAL_DRAIN_TIMEOUT_MS / MAINTENANCE_INTERVAL_MS).toInt + 1

  /** First retransmission backoff; each further attempt doubles it. */
  val REPLAY_BASE_BACKOFF_MS: Long = StreamingShuffleServerHandler.RETRANSMIT_INITIAL_BACKOFF_MS

  /** Retransmission attempts allowed per stream before the consumer is declared lost. */
  val MAX_REPLAY_ATTEMPTS: Int = StreamingShuffleServerHandler.MAX_RETRANSMIT_ATTEMPTS

  val MAX_BACKOFF_SHIFT: Int = math.max(0, MAX_REPLAY_ATTEMPTS - 1)

  /** Ceiling on the retransmission backoff, so the last attempt still falls inside a task. */
  val MAX_REPLAY_BACKOFF_MS: Long = REPLAY_BASE_BACKOFF_MS << MAX_BACKOFF_SHIFT

  /**
   * Consecutive eviction passes that free nothing at all before a refused buffer reservation is
   * treated as prevented.
   */
  val MAX_ADMISSION_RECOVERY_ROUNDS: Int = 3

  /** Recovery rounds that may be deferred while another thread's eviction is still in flight. */
  val MAX_EVICTION_DEFERRALS: Int = 1024

  /** Blocks a partition's buffer allowance is sized to hold concurrently. */
  val TARGET_PIPELINE_DEPTH: Int = 4

  /** Window over which the producer's egress rate is averaged before it is reported. */
  val THROUGHPUT_WINDOW_MS: Long = 1000L

  /** Milliseconds in a second, named so the rate arithmetic reads as a rate. */
  val MILLIS_PER_SECOND: Long = 1000L

  val INITIAL_ACTIVE_PARTITION_CAPACITY: Int = 16

  /** Minimum accounting charge for one accumulator, even when its fixed segment is smaller. */
  val MIN_ACCUMULATOR_BYTES: Int = 4096

  val NOTHING_APPLIED: Long = -1L

  /** The empty array a released accumulator parks its buffer reference on. */
  val EMPTY_PAYLOAD: Array[Byte] = new Array[Byte](0)
}
