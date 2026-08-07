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

import java.io.InputStream
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable
import scala.concurrent.duration.DurationLong
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.netty.channel.{Channel, EventLoopGroup}

import org.apache.spark.{Aggregator, InterruptibleIterator, SecurityManager, SparkConf, SparkEnv,
  TaskContext}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{BLOCK_ID, CHECKSUM, COUNT, EPOCH, ERROR, EXECUTOR_ID,
  HOST_PORT, MAP_ID, MAX_ATTEMPTS, NUM_BLOCKS, NUM_BYTES, NUM_PARTITIONS, NUM_SKIPPED, NUM_TASKS,
  PARTITION_ID, PROTOCOL_VERSION, REASON, SHUFFLE_ID, STATUS, THRESHOLD, TIMEOUT, TOTAL, VALUE,
  VERSION_NUM}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient,
  TransportClientBootstrap, TransportClientFactory}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol.streaming.{DataBlockMessage,
  StreamingShuffleChecksum, StreamingShuffleMessage}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.rpc.{RpcEndpointRef, RpcTimeout, RpcTimeoutException}
import org.apache.spark.serializer.{DeserializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.streaming.StreamingShuffleClientHandler.{BlockReceived,
  ProducerLost, StreamCompleted}
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.{Clock, CompletionIterator, SystemClock}
import org.apache.spark.util.collection.ExternalSorter

/**
 * The consumer half of a streaming shuffle: a [[ShuffleReader]] that consumes a reduce partition
 * directly from the executors producing it, block by block as blocks arrive, instead of waiting for
 * the map output to be materialised on disk and then fetching whole partitions of it.
 *
 * `read()` resolves the producers of the partition from the coordinator, opens a streaming channel
 * to each, and returns an iterator that yields records as blocks arrive. A block's CRC32C is
 * verified before any of its records becomes visible, and consumption progress is acknowledged as
 * it advances -- an acknowledgement being what releases the producer's retained memory. Spark's
 * standard shuffle-read metrics are reported through the reporter this reader is given, so the
 * streaming path is visible on every existing observability surface.
 *
 * Failure is handled at exactly two levels. Corruption of a block still inside the producer's
 * retained window is repaired by asking for a replay of it. Anything else -- a producer silent past
 * the liveness bound, corruption of a position the producer has already reclaimed, a stream that
 * ended short of the total it announced -- causes every block accepted from that producer to be
 * discarded atomically, so no reduce task can mix pre-failure bytes with recomputed ones, and then
 * raises [[FetchFailedException]], which is the sanctioned signal that has the unmodified scheduler
 * recompute the upstream stage.
 *
 * Threading and cleanup. Blocks are delivered by Netty event-loop threads while the iterator is
 * advanced on the task thread, so a failure observed on an I/O thread is carried across by
 * [[StreamingShuffleErrorNotifier]] and re-thrown here; without that bridge the task would wait
 * forever on input that will never arrive. [[ShuffleReader]] exposes no `stop`, so every release --
 * buffers, channels, ledgers, spill state -- is registered on the task-completion listener, which
 * is the one hook that fires on success, on failure and on cancellation alike.
 *
 * @param handle registration state for the shuffle being read, produced by `registerShuffle`
 * @param startMapIndex first map index to read, inclusive.
 * @param endMapIndex map index one past the last to read; `Int.MaxValue` for the whole range
 * @param startPartition first reduce partition to read, inclusive
 * @param endPartition reduce partition one past the last to read
 * @param context the task context of the reduce task performing the read
 * @param readMetrics reporter Spark's standard shuffle-read metrics are accumulated on.
 * @param conf the executor's configuration, read once here and held immutably
 * @param streamingContext the collaborators this reader shares with the rest of the subsystem
 * @param clock the clock every timer in this class reads, so each one advances with that clock
 *     rather than with wall time and nothing here sleeps
 */
private[spark] class StreamingShuffleReader[K, C](
    handle: StreamingShuffleHandle[K, _, C],
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    conf: SparkConf,
    streamingContext: StreamingShuffleReaderContext,
    clock: Clock = new SystemClock)
  extends ShuffleReader[K, C] with Logging {

  import StreamingShuffleReader._

  private val dep = handle.dependency
  private val shuffleId: Int = handle.shuffleId
  private val capabilityToken: String = handle.capabilityToken

  // Collaborators.
  private val coordinatorRef: RpcEndpointRef = streamingContext.coordinatorRef
  private val backpressure: BackpressureProtocol = streamingContext.backpressure
  private val fallbackPolicy: StreamingShuffleFallbackPolicy = streamingContext.fallbackPolicy
  private val connector: StreamingShuffleProducerConnector = streamingContext.connector
  private val serializerManager: SerializerManager = streamingContext.serializerManager

  /**
   * The bridge that carries a failure observed on this reader's Netty threads to this reader's task
   * thread.
   */
  private val errorNotifier: StreamingShuffleErrorNotifier =
    new StreamingShuffleErrorNotifier(shuffleId, conf)

  // Configuration is read exactly once, here, and held immutably for the lifetime of the reader.
  private val streamingEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_ENABLED)
  private val spillThresholdPercent: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)
  private val maxBandwidthMbps: Option[Int] = conf.get(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)
  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  /** The bounded deadline every coordinator ask this reader makes is subject to. */
  private val coordinatorTimeout: RpcTimeout = {
    val configured = conf.getTimeAsMs(StreamingShuffleServerHandler.CONNECTION_TIMEOUT_KEY,
      s"${StreamingShuffleServerHandler.CONNECTION_TIMEOUT_SECONDS}s")
    new RpcTimeout(math.max(MIN_COORDINATOR_TIMEOUT_MS, configured).millis,
      StreamingShuffleServerHandler.CONNECTION_TIMEOUT_KEY)
  }

  /** The consumer-side budget, which is the executor's and not this reader's. */
  private val bufferBudgetBytes: Long = backpressure.receiveQuotaBytes

  /**
   * Per-partition allowance, following the specified `(executorMemory * bufferPercent) /
   * numPartitions`.
   */
  private val perStreamCreditBytes: Long =
    math.max(DataBlockMessage.MAX_ENCODED_FRAME_BYTES.toLong,
      bufferBudgetBytes / math.max(1, handle.numPartitions))

  /**
   * The point at which this reader stops accepting blocks for partitions it is not currently
   * draining.
   */
  private val stashHighWaterBytes: Long =
    math.max(DataBlockMessage.MAX_ENCODED_FRAME_BYTES.toLong,
      perStreamCreditBytes / PERCENT_SCALE * spillThresholdPercent)

  /** Partitions this reduce task is responsible for, ascending, so consumption order is stable. */
  private val partitionIds: Seq[Int] = startPartition until endPartition

  // Mutable state.
  private val streamsLock = new Object
  private val producerStreams = new mutable.ArrayBuffer[ProducerStream]

  /** The ledger identities this reader opened, so cleanup releases exactly what it claimed. */
  private val registeredStreams = new mutable.HashSet[BackpressureStreamKey]

  /** This reader's consumer session identity, announced to every producer it streams from. */
  private val consumerId: String =
    s"attempt-${context.taskAttemptId()}-partitions-$startPartition-$endPartition"

  private val cleanedUp = new AtomicBoolean(false)

  /** Released by cleanup so that no bounded wait in this reader can outlive the task. */
  private val quiesce = new CountDownLatch(1)

  /** Bytes this reader is holding in decoded-but-unconsumed blocks. */
  private val stashedBytes = new AtomicLong(0L)

  private var invalidations: Int = 0

  /** Blocks accepted and bytes accepted, for the throughput samples the fallback policy needs. */
  private var acceptedBlocks: Long = 0L
  private var acceptedBytes: Long = 0L

  /**
   * Cumulative repair activity on this reduce task, and the two windows that bound its reporting.
   */
  private var checksumFailures: Long = 0L
  private var replayRequests: Long = 0L
  private val checksumFailureLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)
  private val replayRequestLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  // Tier two of the two-tier activation model.
  require(streamingEnabled,
    s"${config.SHUFFLE_STREAMING_ENABLED.key} is false, so no streaming shuffle reader may be " +
      "constructed. The streaming shuffle manager delegates to the sort-based manager instead.")

  require(startMapIndex >= 0 && endMapIndex >= startMapIndex,
    s"Invalid map index range [$startMapIndex, $endMapIndex) for shuffle $shuffleId.")

  require(startPartition >= 0 && endPartition >= startPartition,
    s"Invalid reduce partition range [$startPartition, $endPartition) for shuffle $shuffleId.")

  // Registered before any channel is opened, so that a failure between here and the first block
  // still releases everything.
  context.addTaskCompletionListener[Unit](_ => releaseResources())

  if (debugEnabled) {
    logDebug(log"Streaming shuffle reader for shuffle ${MDC(SHUFFLE_ID, shuffleId)} partitions " +
      log"[${MDC(PARTITION_ID, startPartition)}, ${MDC(VALUE, endPartition)}) holds a budget of " +
      log"${MDC(NUM_BYTES, bufferBudgetBytes)} byte(s), ${MDC(THRESHOLD, perStreamCreditBytes)} " +
      log"byte(s) of credit per stream, and an administered bandwidth cap of " +
      log"${MDC(COUNT, maxBandwidthMbps.map(_.toString).getOrElse("unlimited"))} MB/s")
  }

  /** Deserializer for every stream this reader opens; the task thread is its only user. */
  private lazy val serializerInstance: SerializerInstance = dep.serializer.newInstance()

  /**
   * Reads this reduce task's partitions, streaming them from the producers that are still writing
   * them.
   *
   * @return the combined key-value pairs for this reduce task
   * @throws FetchFailedException if a producer is lost, announces an incompatible protocol,
   *     ends a stream short, or delivers a block that cannot be repaired.
   */
  override def read(): Iterator[Product2[K, C]] = {
    // A failure latched on an I/O thread before the task thread got here must surface now, not be
    // overwritten by whatever the first fetch attempt happens to hit.
    checkForFailure(startPartition)

    // Makes this shuffle visible to the executor-wide ledger, which is what lets the protocol
    // arbitrate between concurrent shuffles and what the token bucket's refill rate is divided by.
    backpressure.registerShuffle(shuffleId, handle.numPartitions)
    // Consumer and producer draw on ONE executor-wide quota, and a consumer's received frames are
    // charged to it exactly as a producer's buffered blocks are -- as the consumer category of the
    // same aggregate reservation, alongside transient framing copies and per-stream metadata.

    // Deduplicated before anything is claimed or opened, so that the credit split, the streams and
    // the reduce input all describe the same set of producers.
    val locations = acceptedGenerations(rendezvous())
    // Claimed only once the producing generations are known, because a generation is part of a
    // stream's identity and therefore of the allowance being claimed.
    registerStreams(locations)
    val streams = locations.map(openProducerStream)
    requestInProgressBlocks(streams)

    // Lazy from here down: one producer at a time, one partition at a time, one block at a time.
    val recordIter: Iterator[(Any, Any)] = streams.iterator.flatMap(stream => stream.records)

    // Standard Spark shuffle-read accounting, populated on the task thread.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator is required for task cancellation to be observed promptly.
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val resultIter: Iterator[Product2[K, C]] = combineAndSort(interruptibleIter)

    // Streams are open by now, so a producer lost while this read was being assembled is converted
    // before the notifier is consulted -- otherwise the transport error latched alongside the loss
    // would be raised here, one step before the iterator's own conversion could run.
    checkForFailure(startPartition)

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // The aggregator or the sorter may have consumed the interruptible iterator above, so wrap
        // the result again to keep cancellation responsive while the caller drains it.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }

  /**
   * Applies the dependency's aggregation and ordering to the streamed records.
   *
   * The two `dep.mapSideCombine` branches below are unreachable on the streaming path, because a
   * dependency that asks for map-side combining is declined when the shuffle is registered and is
   * served by the sort-based shuffle instead. They are retained so that this method stays
   * structurally identical to `BlockStoreShuffleReader.read`, which is what makes the two readers
   * comparable line by line and keeps a future change to the shared read shape from having to be
   * reasoned about twice.
   */
  private def combineAndSort(records: Iterator[(Any, Any)]): Iterator[Product2[K, C]] = {
    if (dep.keyOrdering.isDefined) {
      val sorter: ExternalSorter[K, _, C] = if (dep.aggregator.isDefined) {
        if (dep.mapSideCombine) {
          new ExternalSorter[K, C, C](context,
            Option(new Aggregator[K, C, C](identity,
              dep.aggregator.get.mergeCombiners,
              dep.aggregator.get.mergeCombiners)),
            ordering = Some(dep.keyOrdering.get), serializer = dep.serializer)
        } else {
          new ExternalSorter[K, Nothing, C](context,
            dep.aggregator.asInstanceOf[Option[Aggregator[K, Nothing, C]]],
            ordering = Some(dep.keyOrdering.get), serializer = dep.serializer)
        }
      } else {
        new ExternalSorter[K, C, C](context, ordering = Some(dep.keyOrdering.get),
          serializer = dep.serializer)
      }
      sorter.insertAllAndUpdateMetrics(records.asInstanceOf[Iterator[(K, Nothing)]])
    } else if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // The values arriving are already combiners.
        val combinedKeyValuesIterator = records.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // The value type is unknown here, and need not be known: the dependency guarantees the
        // aggregator converts it to the combined type C.
        val keyValuesIterator = records.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      records.asInstanceOf[Iterator[(K, C)]]
    }
  }

  /**
   * The one producer generation per map index that this consumer may read.
   *
   * @param offered producer locations as the coordinator ordered them, newest attempts included
   * @return one location per map index, the newest attempt of each, in map-index order
   */
  private def acceptedGenerations(
      offered: Seq[StreamingShuffleProducerLocation]): Seq[StreamingShuffleProducerLocation] = {
    // Re-sorted explicitly: grouping loses the coordinator's map-index ordering, and that ordering
    // is what makes a streaming read's record order match the sort-based baseline's.
    val accepted = offered.groupBy(_.mapIndex).values.map(_.maxBy(_.taskAttemptId)).toSeq
      .sortBy(_.mapIndex)
    val superseded = offered.size - accepted.size
    if (superseded > 0) {
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} was offered " +
        log"${MDC(COUNT, superseded)} producer generation(s) of map indexes it was also offered " +
        log"a newer attempt of; reading only the newest attempt of each map index so that no map " +
        log"output is concatenated into this reduce input twice")
    }
    accepted
  }

  /**
   * Claims a credit allowance for every stream this task will read: one per producer generation,
   * per partition.
   */
  private def registerStreams(locations: Seq[StreamingShuffleProducerLocation]): Unit = {
    // One partition's allowance is shared between the generations feeding it, so making the ledger
    // per producer sharpens the identity without loosening the bound: the aggregate this reader may
    // hold is the same partition allowance it always was, however many producers serve it.
    val perProducerCreditBytes = math.max(
      DataBlockMessage.MAX_ENCODED_FRAME_BYTES.toLong,
      perStreamCreditBytes / math.max(1, locations.size))
    locations.foreach { location =>
      partitionIds.foreach { partitionId =>
        val key = consumerKey(location, partitionId)
        if (backpressure.registerStream(key, perProducerCreditBytes)) {
          streamsLock.synchronized {
            registeredStreams.add(key)
          }
        } else {
          backpressure.reportBufferAllocationFailure(key)
          val detail =
            s"a consumer of partitions [$startPartition, $endPartition) could not reserve " +
              s"${BackpressureProtocol.STREAM_LEDGER_BASE_BYTES} bytes of credit-ledger metadata"
          val declared =
            declareShuffleFallback(StreamingShuffleFallbackReason.MemoryPressure, detail)
          throw new FetchFailedException(
            null,
            shuffleId,
            UNKNOWN_MAP_ID,
            location.mapIndex,
            partitionId,
            s"Streaming shuffle $shuffleId could not register the consumer stream for map " +
              s"${location.mapId}, partition $partitionId within the executor-wide memory " +
              s"budget. The shuffle fallback state is ${declared.condition}.")
        }
      }
    }
  }

  /** The ledger identity of one partition arriving from one producer generation. */
  private def consumerKey(
      location: StreamingShuffleProducerLocation,
      partitionId: Int): BackpressureStreamKey =
    BackpressureStreamKey.forConsumer(
      shuffleId, location.mapId, location.taskAttemptId, partitionId, consumerId)

  /** Resolves the producers currently streaming this partition range. */
  private def rendezvous(): Seq[StreamingShuffleProducerLocation] = {
    // The executor's own verdict is consulted before the driver's.
    checkLocalFallback(UNKNOWN_MAP_INDEX)
    val waitStartMillis = clock.getTimeMillis()
    var attempts = 0
    var resolved: Option[StreamingShuffleProducerLocations] = None
    var lastReply: Option[StreamingShuffleProducerLocations] = None
    while (resolved.isEmpty && attempts < COORDINATOR_LOOKUP_MAX_ATTEMPTS && !cleanedUp.get()) {
      checkForFailure(startPartition)
      val reply = lookupProducers()
      // The shuffle-wide verdict is honoured before the addresses are, and on every attempt rather
      // than once: a fallback declared while this consumer was polling withdraws the streaming path
      // from under it, and continuing to poll for a producer that may no longer register would burn
      // the whole lookup budget before recovering.
      reply.foreach(checkShuffleFallback)
      lastReply = reply.orElse(lastReply)
      resolved = reply.filter(rendezvousSatisfied)
      if (resolved.isEmpty) {
        attempts += 1
        if (attempts < COORDINATOR_LOOKUP_MAX_ATTEMPTS) {
          awaitQuietly(COORDINATOR_LOOKUP_INTERVAL_MS)
        }
      }
    }
    readMetrics.incFetchWaitTime(math.max(0L, clock.getTimeMillis() - waitStartMillis))
    resolved match {
      case Some(reply) =>
        validateRendezvous(reply)
        val served = servedLocations(reply)
        if (debugEnabled) {
          // The completion count is reported alongside the producer count because together they are
          // the only honest answer to "did this read overlap its map stage": a reply in which every
          // declared map index has already streamed to completion is a read of retained output,
          // while any shortfall is a read that genuinely overlaps production.
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} resolved " +
            log"${MDC(COUNT, served.size)} live producer(s) of the " +
            log"${MDC(NUM_TASKS, expectedMapCount(reply))} map task(s) it reads, out of " +
            log"${MDC(TOTAL, reply.numMaps)} the stage declared, for partitions " +
            log"[${MDC(PARTITION_ID, startPartition)}, ${MDC(VALUE, endPartition)}) after " +
            log"${MDC(MAX_ATTEMPTS, attempts + 1)} lookup attempt(s); the map stage had " +
            log"${MDC(STATUS, if (reply.mapStageComplete) "finished" else "not finished")} " +
            log"producing, so this is a read of " +
            log"${MDC(REASON, if (reply.mapStageComplete) "retained output" else "output in " +
              "progress")}")
        }
        served
      case None =>
        // The declaration is what makes this terminate.
        val declared = declareShuffleFallback(
          StreamingShuffleStandDownCause.ProducerUnavailable,
          s"a consumer of partitions [$startPartition, $endPartition) could not resolve every " +
            s"producer after $attempts lookup attempt(s)")
        throw new FetchFailedException(null, shuffleId, UNKNOWN_MAP_ID, UNKNOWN_MAP_INDEX,
          startPartition,
          unresolvedRendezvousMessage(lastReply, attempts, declared.fallenBack))
    }
  }

  /**
   * Fails the read when this executor has itself stood streaming down.
   *
   * @param mapIndex the map index to blame, or [[StreamingShuffleReader.UNKNOWN_MAP_INDEX]]
   *     when the failure belongs to the shuffle rather than to one map output
   */
  private def checkLocalFallback(mapIndex: Int): Unit = {
    val known = fallbackPolicy.knownShuffleFallback(shuffleId)
    val state = known match {
      case Some(latched) => Some(latched)
      // The condition the policy actually latched, never a stand-in for it: a trip always carries
      // its reason, so a default here could only ever record a condition nobody observed.
      case None => fallbackPolicy.trippedReason.map { reason =>
        declareShuffleFallback(reason,
          s"a consumer of partitions [$startPartition, $endPartition) observed the condition " +
            "on its own executor")
      }
    }
    state.filter(_.fallenBack).foreach { latched =>
      val description = latched.condition
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} stood streaming down at " +
        log"epoch ${MDC(EPOCH, latched.declaredAtEpoch)} because ${MDC(REASON, description)}; " +
        log"failing this fetch so that the upstream stage is recomputed on the sort-based path")
      throw new FetchFailedException(null, shuffleId, UNKNOWN_MAP_ID, mapIndex, startPartition,
        s"Streaming shuffle $shuffleId stood streaming down at epoch " +
          s"${latched.declaredAtEpoch} because $description. Its partial streaming output is " +
          "invalid, so the upstream stage is recomputed on the sort-based shuffle path.")
    }
  }

  /**
   * Runs [[ProducerStream.convertSignalledProducerLoss]] on every producer this read has opened.
   */
  private def convertSignalledProducerLossEverywhere(): Unit = {
    if (errorNotifier.hasError) {
      streamsLock.synchronized(producerStreams.toSeq)
        .foreach(stream => stream.convertSignalledProducerLoss())
    }
  }

  /**
   * The single failure funnel of this reader: every consult of the notifier goes through here.
   *
   * Three things happen, in an order that is the whole of why this method exists.
   *
   * First, every producer loss the I/O threads have already signalled is converted, on this thread,
   * into the atomic per-producer invalidation plus the [[FetchFailedException]] that the unmodified
   * scheduler recovers from. That has to precede the notifier, because a channel failure is
   * reported twice by design -- as a marker on the hand-off queue and as a raw throwable on the
   * notifier -- and only the marker can be turned into a fetch failure.
   *
   * Second, a failure still latched with no fetch failure to show for it is escalated to one
   * anyway. A marker is not guaranteed to arrive: the hand-off queue can be full when the loss is
   * signalled, a second failure on an already-signalled partition raises no further marker, and a
   * failure observed on a channel callback before this reader has bound the stream names no
   * partition at all. Those paths used to leave a bare `SparkException` on the notifier, which
   * propagated as an ordinary task failure -- so the scheduler counted it against
   * `spark.task.maxFailures` and aborted the job instead of recomputing the upstream stage that
   * produced the unreadable bytes. Escalating here restores the guarantee that every unrecoverable
   * producer-side condition, corruption and lost sequence included, terminates in a working
   * shuffle.
   *
   * Third, and only if neither of the two above found anything to raise, the notifier is consulted
   * as before, so a failure that belongs to nothing this reader opened -- a rendezvous error,
   * say -- still surfaces with its own type intact.
   *
   * Cheap on the hot path: when no failure has been recorded the whole method is one volatile read.
   *
   * @param awaitedPartition the partition whose input the caller is waiting for, used to attribute
   *                         a failure that names no partition of its own
   * @param opening the producer whose channel this reader is in the middle of opening, when it is
   *                in the middle of opening one. Present only from [[openProducerStream]], and it
   *                is what lets a failure observed inside the connect window -- before the stream
   *                exists to attribute it to -- still name the producer it belongs to
   */
  private def checkForFailure(
      awaitedPartition: Int,
      opening: Option[StreamingShuffleProducerLocation] = None): Unit = {
    if (errorNotifier.hasError) {
      convertSignalledProducerLossEverywhere()
      escalateLatchedFailure(awaitedPartition, opening)
    }
    errorNotifier.throwIfError()
  }

  /**
   * Escalates a latched failure that no producer-loss marker accounted for into a fetch failure.
   *
   * Does nothing unless a non-fatal failure is latched and none of it is a fetch failure already,
   * so the common paths -- a converted marker, an explicit fetch failure raised by this thread --
   * reach this method having nothing to do.
   *
   * <b>Every failure a reader latches is a fetch-side failure, so every one of them leaves here as
   * a fetch failure.</b> This notifier belongs to one reader, and the only things that can latch a
   * failure on it are the client handlers this reader opened and [[raiseFetchFailure]] -- so a
   * throwable found here is, by construction, a fault of the producer side of a shuffle read. That
   * is the same invariant the classic path relies on, where `ShuffleBlockFetcherIterator` routes
   * every fetch-side `IOException` and every corruption through a fetch failure and nothing else.
   * Letting one propagate as it stood made the scheduler count it against `spark.task.maxFailures`
   * rather than recompute the stage that produced the unreadable bytes, and under a master whose
   * task budget is a single attempt -- which `local[N]` is, through
   * `SparkContext.MAX_LOCAL_TASK_FAILURES` -- one connection reset observed on a consumer channel
   * aborted the whole job. The escalation below therefore has three arms and no fall-through.
   *
   * The first arm is the ordinary one. The stream that is failed is the one whose handler reported
   * the failure, identified by the handler's own escalation count, so the fetch failure names the
   * producer that actually broke and the coordinator invalidates that producer's generation and no
   * other. When no handler admits to it, the first stream that has not finished is failed instead,
   * because that is the producer whose input this read is still waiting for.
   *
   * The second arm covers the connect window, which is where the abort above came from. A handler
   * is bound to its channel before the connector decides whether to publish it, so a reset observed
   * inside that window is latched by a handler this reader never receives: [[connectOnce]] declines
   * it and closes it, and `producerStreams` is still empty when the next attempt consults the
   * notifier. The producer being opened is named directly in that case, which is both the honest
   * attribution and the one that recovers -- the generation is invalidated and the fetch failure
   * carries the producer's own `MapStatus` address, so the tracker removes that one dead map output
   * and the scheduler recomputes that one map task.
   *
   * The third arm is the backstop for a failure this read can attribute to no producer at all --
   * one observed after the task-completion listener has released every stream, say. It names none,
   * exactly as the coordinator-unreachable and stood-down paths already do and as
   * `FetchFailedException` itself sanctions, and recovery is then the ordinary resubmission of the
   * upstream stage rather than the removal of one map output.
   *
   * A fatal `Error` is never escalated, because a fetch failure is a recoverable condition and a
   * fatal one must not be downgraded into it. It is left to [[StreamingShuffleErrorNotifier]],
   * which re-throws it ahead of every other consideration.
   *
   * [[ProducerStream.failProducer]], [[failOpeningProducer]] and [[raiseUnattributedFetchFailure]]
   * all return `Nothing`: they discard whatever was taken from the producer, record the
   * invalidation where there is a producer to invalidate, and throw. This method therefore either
   * throws or returns having found nothing it may escalate.
   *
   * @param awaitedPartition the partition whose input the caller is waiting for
   * @param opening the producer whose channel is being opened, when one is
   */
  private def escalateLatchedFailure(
      awaitedPartition: Int,
      opening: Option[StreamingShuffleProducerLocation]): Unit = {
    val latched = errorNotifier.error
    // An empty slot and a fatal Error are both excluded by this one test: there is nothing to
    // escalate in the first case and nothing that may be escalated in the second.
    if (latched.exists(NonFatal(_)) && errorNotifier.fetchFailure.isEmpty) {
      val detail = latched.map(describeFailure)
        .getOrElse("a streaming shuffle failure was reported without a cause")
      val cause = latched.orNull
      val streams = streamsLock.synchronized(producerStreams.toSeq)
      val reporter = streams.find(_.hasReportedFailure).orElse(streams.find(!_.isDrained))
      (reporter, opening) match {
        case (Some(stream), _) =>
          val partitionId = stream.firstUnfinishedPartition.getOrElse(
            if (stream.readsPartition(awaitedPartition)) awaitedPartition else startPartition)
          stream.failProducer(partitionId, StreamingShuffleInvalidationReason.ProtocolViolation,
            detail, cause)
        case (None, Some(location)) =>
          failOpeningProducer(location, detail, cause)
        case (None, None) =>
          raiseUnattributedFetchFailure(awaitedPartition, detail, cause)
      }
    }
  }

  /**
   * Fails the producer whose channel was being opened when a failure was observed on that channel.
   *
   * The two steps are the ones the exhausted-attempt arm of [[openProducerStream]] takes, and they
   * are taken for the same reasons. The invalidation withdraws the producer generation, so the
   * recomputation this fetch failure asks for lands on a fresh map attempt instead of on the same
   * unreachable output, and it keeps the most common invalidation an operator can hit -- a producer
   * that cannot be streamed from at all -- visible on
   * `shuffle.streaming.partialReadInvalidations`. Nothing is discarded first because nothing was
   * ever taken: a channel that failed while it was being opened delivered no block to this read.
   *
   * @param location the producer being opened, whose `MapStatus` address and map index are what let
   *                 `MapOutputTracker` remove the exact dead map output
   * @param detail the operator-facing rendering of the latched failure
   * @param cause the latched failure itself, attached so the recomputation is diagnosable
   */
  private def failOpeningProducer(
      location: StreamingShuffleProducerLocation,
      detail: String,
      cause: Throwable): Nothing = {
    recordInvalidation(location, StreamingShuffleInvalidationReason.ConnectionTimeout, detail)
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} lost producer " +
      log"${MDC(EXECUTOR_ID, location.executorId)} at ${MDC(HOST_PORT, location.hostPort)} while " +
      log"opening its channel: ${MDC(REASON, detail)}; the upstream stage will be recomputed")
    raiseFetchFailure(location, startPartition,
      s"Streaming shuffle producer ${location.hostPort} for shuffle $shuffleId map " +
        s"${location.mapId} was lost while its channel was being opened: $detail.", cause)
  }

  /**
   * Raises a fetch failure for a latched failure that belongs to no producer this read can name.
   *
   * The shape is [[raiseFetchFailure]]'s, and for the same reasons: the failure is handed to the
   * notifier before it is thrown, so first-error-wins decides what propagates and
   * [[StreamingShuffleErrorNotifier.throwIfError]] re-asserts the fetch failure on this thread's
   * task context -- which is what the executor consults when it chooses between a FetchFailed and
   * an ExceptionFailure reason. The trailing throw is what makes the `Nothing` result honest, and
   * it is a real safety net besides, because `setError` deliberately absorbs any non-fatal problem
   * it meets while recording.
   *
   * What differs is that no producer is named: the address is null and the map identity is the
   * documented unknown sentinel, which is the shape `FetchFailedException` sanctions for a failure
   * that cannot be attributed to one map output, and which the coordinator-unreachable and
   * stood-down paths in this class already use. Recovery is then the plain resubmission of the
   * upstream stage.
   *
   * @param partitionId the partition whose input this read was waiting for
   * @param detail the operator-facing rendering of the latched failure
   * @param cause the latched failure itself, attached to the fetch failure
   */
  private def raiseUnattributedFetchFailure(
      partitionId: Int,
      detail: String,
      cause: Throwable): Nothing = {
    val attributed = if (servesPartition(partitionId)) partitionId else startPartition
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} failed reading partition " +
      log"${MDC(PARTITION_ID, attributed)} and can attribute the failure to no producer it " +
      log"opened: ${MDC(REASON, detail)}; the upstream stage will be recomputed")
    val failure = new FetchFailedException(null, shuffleId, UNKNOWN_MAP_ID, UNKNOWN_MAP_INDEX,
      attributed,
      s"Streaming shuffle $shuffleId could not read partitions " +
        s"[$startPartition, $endPartition) and could attribute the failure to no producer it " +
        s"opened: $detail. The upstream stage is recomputed so that this read is served from " +
        "output this consumer can reach.", cause)
    errorNotifier.setError(failure)
    errorNotifier.throwIfError()
    throw failure
  }

  /**
   * The operator-facing rendering of a failure: its message, or its type when it carries none.
   *
   * A typed streaming shuffle condition puts everything an operator needs -- the condition name,
   * the shuffle and partition, the numbers that disagree and the SQLSTATE -- in its message, so
   * the message is what a failure report has to carry. A throwable with no message at all is
   * rendered by its class name, because an empty report would be worse than a terse one.
   */
  private def describeFailure(cause: Throwable): String = {
    val message = cause.getMessage
    if (message != null && message.nonEmpty) message else cause.getClass.getName
  }

  /**
   * Declares a shuffle-wide fallback on the coordinator and caches the verdict it latched.
   *
   * @param cause what is being declared -- one of the four documented fallback conditions, or a
   *     structural decline the streaming protocol cannot serve
   * @param detail operator-facing context recorded alongside the verdict
   * @return the verdict in force after the declaration, empty when it could not be made
   */
  private def declareShuffleFallback(
      cause: StreamingShuffleStandDownCause,
      detail: String): StreamingShuffleFallbackState = {
    fallbackPolicy.knownShuffleFallback(shuffleId) match {
      case Some(latched) => latched
      case None =>
        val declared = try {
          coordinatorRef.askSync[Any](DeclareStreamingShuffleFallback(
            shuffleId, capabilityToken, cause.toString, detail), coordinatorTimeout) match {
            case state: StreamingShuffleFallbackState => state
            case other =>
              logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} received " +
                log"${MDC(VALUE, describe(other))} for a stand-down declaration; treating the " +
                log"shuffle as still streaming and relying on the fetch failure alone")
              StreamingShuffleFallbackState()
          }
        } catch {
          case NonFatal(e) =>
            logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not declare " +
              log"a fallback for ${MDC(REASON, cause.description)}; the reduce side still " +
              log"recovers through its fetch failure", e)
            StreamingShuffleFallbackState()
        }
        fallbackPolicy.observeShuffleFallback(shuffleId, declared)
        declared
    }
  }

  /**
   * Asks the driver to withdraw this shuffle's streamed map output so that its map stage is
   * recomputed, and reports whether the withdrawal was '''confirmed'''.
   *
   * @param detail free text describing what could not be resolved
   * @return true when the driver confirmed that no streamed map output of this shuffle remains
   */
  private def withdrawStreamedMapOutput(detail: String): Boolean = {
    try {
      coordinatorRef.askSync[Any](
        InvalidateStreamingShuffleMapOutput(shuffleId, capabilityToken, detail),
        coordinatorTimeout) match {
        case withdrawn: java.lang.Boolean => withdrawn.booleanValue()
        case other =>
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} received " +
            log"${MDC(VALUE, describe(other))} for a streamed map output invalidation; the " +
            log"recomputation cannot be treated as arranged")
          false
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not have its " +
          log"streamed map output withdrawn, so the reduce side recovers only through its own " +
          log"fetch failure", e)
        false
    }
  }

  /**
   * Whether a lookup reply names a producer this consumer can open a stream to for every map output
   * it is entitled to read.
   */
  private def rendezvousSatisfied(reply: StreamingShuffleProducerLocations): Boolean = {
    servedLocations(reply).map(_.mapIndex).toSet.size >= expectedMapCount(reply)
  }

  /** The producers of this read's own map range, out of everything the coordinator named. */
  private def servedLocations(
      reply: StreamingShuffleProducerLocations): Seq[StreamingShuffleProducerLocation] = {
    if (servesFullMapRange(startMapIndex, endMapIndex)) {
      reply.locations
    } else {
      reply.locations.filter(location =>
        location.mapIndex >= startMapIndex && location.mapIndex < endMapIndex)
    }
  }

  /**
   * The map indexes this read would select from one coordinator reply, in ascending order.
   *
   * @param reply a coordinator reply to select from
   * @return the map indexes this read is entitled to, ascending
   */
  private[streaming] def servedMapIndexesOf(
      reply: StreamingShuffleProducerLocations): Seq[Int] = {
    servedLocations(reply).map(_.mapIndex).distinct.sorted
  }

  /** Map outputs this read must be able to resolve before it may begin. */
  private def expectedMapCount(reply: StreamingShuffleProducerLocations): Int = {
    val first = math.max(0, startMapIndex)
    val last = math.min(reply.numMaps, math.max(endMapIndex, 0))
    math.max(0, last - first)
  }

  /**
   * The map indexes this consumer must be able to reach a producer for.
   *
   * @param numMaps map outputs the shuffle declared
   * @return the indexes that must be resolvable, empty for a stage that declared no map tasks
   */
  private def requiredMapIndexes(numMaps: Int): Range =
    math.max(0, startMapIndex) until math.min(endMapIndex, math.max(0, numMaps))

  /** Whether a resolved producer belongs to the map-index window this consumer asked for. */
  private def inRequestedMapRange(location: StreamingShuffleProducerLocation): Boolean =
    location.mapIndex >= startMapIndex && location.mapIndex < endMapIndex

  /** Fails the fetch when the shuffle has stood streaming down for every participant. */
  private def checkShuffleFallback(reply: StreamingShuffleProducerLocations): Unit = {
    if (reply.fallback.fallenBack) {
      val fallback = reply.fallback
      val description = fallback.cause.map(_.description).getOrElse(fallback.reasonName)
      // Cached before the fetch is failed, so a producer of this shuffle running on this executor
      // stands down on its next block boundary rather than waiting to be declined by the driver.
      fallbackPolicy.observeShuffleFallback(shuffleId, fallback)
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} stood streaming down for " +
        log"every participant at epoch ${MDC(EPOCH, reply.fallback.declaredAtEpoch)} because " +
        log"${MDC(REASON, description)}; failing this fetch so that the upstream stage is " +
        log"recomputed on the sort-based shuffle path")
      throw new FetchFailedException(null, shuffleId, UNKNOWN_MAP_ID, UNKNOWN_MAP_INDEX,
        startPartition,
        s"Streaming shuffle $shuffleId stood streaming down for every participant at epoch " +
          s"${reply.fallback.declaredAtEpoch} because $description. Its partial streaming output " +
          "is invalid, so the upstream stage is recomputed on the sort-based shuffle path.")
    }
  }

  /**
   * Explains an exhausted rendezvous in terms of what the coordinator last said, so that an
   * operator can tell a map stage that never registered from one that registered only in part.
   */
  private def unresolvedRendezvousMessage(
      lastReply: Option[StreamingShuffleProducerLocations],
      attempts: Int,
      recomputed: Boolean): String = {
    val spanMs = attempts * COORDINATOR_LOOKUP_INTERVAL_MS
    val observed = lastReply match {
      case Some(reply) =>
        val located = servedLocations(reply).map(_.mapIndex).toSet.size
        // Whether more output was still expected is what separates the two diagnoses that share
        // this message: a rendezvous that ran out of attempts while producers were still arriving
        // is a pacing problem, whereas one that ran out after the stage had finished or stood down
        // is a liveness or registration problem, and no further waiting would have helped.
        val outlook = if (reply.mapOutputsPending) {
          "map output was still expected at that point"
        } else {
          "no further map output was expected at that point"
        }
        s"the coordinator last named a live producer for $located of the " +
          s"${expectedMapCount(reply)} map task(s) this read covers, out of ${reply.numMaps} the " +
          s"stage declared, ${reply.completedMapIndexes.size} of which had streamed to " +
          s"completion, at epoch ${reply.coordinatorEpoch}, and $outlook"
      case None => "the shuffle had no registration at all"
    }
    val recovery = if (recomputed) {
      "Its streamed map output has been withdrawn, so the upstream stage is recomputed and this " +
        "read is served from the recomputed output."
    } else {
      "Its streamed map output could NOT be withdrawn, so the recomputation of the upstream " +
        "stage is not confirmed and this read recovers only through the fetch failure itself."
    }
    s"Streaming shuffle $shuffleId could not resolve every producer of partitions " +
      s"[$startPartition, $endPartition) after $attempts lookup attempt(s) spanning $spanMs ms: " +
      s"$observed. $recovery"
  }

  /** One coordinator lookup. */
  private def lookupProducers(): Option[StreamingShuffleProducerLocations] = {
    val request =
      LookupStreamingShuffleProducers(shuffleId, capabilityToken, startPartition, endPartition)
    val reply = try {
      coordinatorRef.askSync[Any](request, coordinatorTimeout)
    } catch {
      // Translated on the spot rather than retried.
      case e: RpcTimeoutException =>
        throw coordinatorUnreachable("resolve the live producers of", e)
    }
    reply match {
      case Some(locations: StreamingShuffleProducerLocations) => Some(locations)
      case None => None
      case other =>
        throw StreamingShuffleErrors.unexpectedMessageType(
          s"Option[${classOf[StreamingShuffleProducerLocations].getSimpleName}]", describe(other))
    }
  }

  /**
   * The fetch failure raised when the coordinator did not answer within the stated deadline.
   *
   * @param operation what the reader was asking the coordinator to do, for the message
   * @param cause the timeout, retained so the operator sees which deadline expired
   * @return the exception to throw, never thrown here so that the caller's `throw` is visible
   */
  private def coordinatorUnreachable(operation: String, cause: Throwable): FetchFailedException = {
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not reach the " +
      log"coordinator within ${MDC(TIMEOUT, coordinatorTimeout.duration.toMillis)} ms; failing " +
      log"this fetch so that the upstream stage is recomputed", cause)
    new FetchFailedException(null, shuffleId, UNKNOWN_MAP_ID, UNKNOWN_MAP_INDEX, startPartition,
      s"Streaming shuffle $shuffleId could not reach its coordinator to $operation partitions " +
        s"[$startPartition, $endPartition) within " +
        s"${coordinatorTimeout.duration.toMillis} ms (bounded by " +
        s"${StreamingShuffleServerHandler.CONNECTION_TIMEOUT_KEY}).", cause)
  }

  /**
   * Confirms that the producer generation just resolved partitions its output the way this consumer
   * expects and speaks a protocol this build understands.
   */
  private def validateRendezvous(reply: StreamingShuffleProducerLocations): Unit = {

    if (!StreamingShuffleMessage.isCompatible(reply.protocolVersion)) {
      fallbackPolicy.checkProtocolVersion(reply.protocolVersion)
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} producers announced " +
        log"protocol version ${MDC(PROTOCOL_VERSION, reply.protocolVersion)} but this build " +
        log"speaks ${MDC(VERSION_NUM, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)}; " +
        log"falling back to the " +
        log"sort-based shuffle and recomputing the upstream stage")
      // Declared as well as tripped locally, for the same reason exhaustion is: the fetch failure
      // recovers this reduce attempt, and only the withdrawal of the streamed map output makes the
      // recomputation land on the sort-based path instead of on producers that would announce the
      // very same version again.
      declareShuffleFallback(
        StreamingShuffleFallbackReason.ProtocolVersionMismatch,
        s"producers announced protocol version ${reply.protocolVersion} but this build speaks " +
          s"${StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION}")
      throw new FetchFailedException(null, shuffleId, UNKNOWN_MAP_ID, UNKNOWN_MAP_INDEX,
        startPartition,
        s"Incompatible streaming shuffle protocol version for shuffle $shuffleId: producers " +
          s"announced ${reply.protocolVersion} but this build speaks " +
          s"${StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION}.")
    }
    if (reply.numPartitions != handle.numPartitions) {
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} producers registered " +
        log"${MDC(NUM_PARTITIONS, reply.numPartitions)} partition(s) but this consumer holds a " +
        log"handle for ${MDC(COUNT, handle.numPartitions)}; the streams cannot be reconciled")
      // A partition-count disagreement, detected by an explicit check rather than inferred from a
      // parse error, means the two ends of the shuffle do not agree about how many partitions it
      // has, so no partition range this consumer could name is servable from those producers.
      declareShuffleFallback(
        StreamingShuffleStandDownCause.UnsupportedReadShape,
        s"producers registered ${reply.numPartitions} partition(s) but a consumer holds a handle " +
          s"for ${handle.numPartitions}")
      throw new FetchFailedException(null, shuffleId, UNKNOWN_MAP_ID, UNKNOWN_MAP_INDEX,
        startPartition,
        s"Streaming shuffle $shuffleId partition count disagreement: producers registered " +
          s"${reply.numPartitions} but this consumer expects ${handle.numPartitions}.")
    }
    // Recorded per stream, because the ledger is keyed by stream and it is the ledger that reports
    // a version disagreement to the fallback policy for streams already in flight.
    reply.locations.foreach { location =>
      partitionIds.foreach { partitionId =>
        backpressure.observeProtocolVersion(
          consumerKey(location, partitionId), reply.protocolVersion)
      }
    }
    if (reply.coordinatorEpoch != handle.coordinatorEpoch && debugEnabled) {
      logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} resolved producers at epoch " +
        log"${MDC(COUNT, reply.coordinatorEpoch)} against a handle registered at epoch " +
        log"${MDC(VALUE, handle.coordinatorEpoch)}")
    }
  }

  /**
   * Opens one channel to one producer, retrying a refused connection a bounded number of times
   * before the producer is declared lost.
   */
  private def openProducerStream(location: StreamingShuffleProducerLocation): ProducerStream = {
    var attempt = 1
    var opened: Option[ProducerStream] = None
    // Time spent pausing between attempts is time the reduce task waited on its input, so it is
    // accumulated and reported to the standard fetch-wait metric in one call, the way the
    // rendezvous reports its own polling.
    var backoffWaitMillis = 0L
    while (opened.isEmpty && !cleanedUp.get() &&
        attempt <= BackpressureProtocol.MAX_RETRY_ATTEMPTS) {
      // A failure already latched on an I/O thread outranks a further attempt: spending the rest of
      // the budget on a read that has failed only delays the fetch failure it has already earned.
      // Through the funnel, because the producers opened before this one are already streaming: a
      // corruption or a lost sequence observed on one of them while this connect was in progress
      // has to be raised as the fetch failure that recomputes its map task, not as a bare
      // exception that fails this attempt and asks for nothing to be recomputed. The producer being
      // opened is named for the funnel, because a failure observed inside the connect window is
      // latched by a handler this reader never receives -- so there is no stream to attribute it
      // to, and this location is the only honest attribution available.
      checkForFailure(startPartition, Some(location))
      opened = connectOnce(location)
      if (opened.isEmpty && attempt < BackpressureProtocol.MAX_RETRY_ATTEMPTS) {
        val backoffMillis = BackpressureProtocol.retryBackoffMillis(attempt)
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not open a channel " +
          log"to producer ${MDC(EXECUTOR_ID, location.executorId)} at " +
          log"${MDC(HOST_PORT, location.hostPort)} for map ${MDC(MAP_ID, location.mapId)} " +
          log"(attempt ${MDC(MAX_ATTEMPTS, attempt)} of " +
          log"${MDC(COUNT, BackpressureProtocol.MAX_RETRY_ATTEMPTS)}); retrying in " +
          log"${MDC(TIMEOUT, backoffMillis)} ms")
        val pauseStartMillis = clock.getTimeMillis()
        awaitQuietly(backoffMillis)
        backoffWaitMillis += math.max(0L, clock.getTimeMillis() - pauseStartMillis)
      }
      attempt += 1
    }
    if (backoffWaitMillis > 0L) {
      readMetrics.incFetchWaitTime(backoffWaitMillis)
    }
    opened match {
      case Some(stream) =>
        stream
      case None if cleanedUp.get() =>
        // This consumer withdrew, which is not evidence against the producer.
        raiseFetchFailure(location, startPartition,
          s"The streaming shuffle consumer of shuffle $shuffleId partitions " +
            s"[$startPartition, $endPartition) was released while opening a channel to " +
            s"${location.hostPort}.")
      case None =>
        // Through `recordInvalidation` rather than straight to `invalidateProducer`, because this
        // arm is an invalidation like every other and has to be counted like one.
        recordInvalidation(location, StreamingShuffleInvalidationReason.ConnectionTimeout,
          "the consumer could not open a streaming channel")
        raiseFetchFailure(location, startPartition,
          s"Could not open a streaming shuffle channel to ${location.hostPort} for shuffle " +
            s"$shuffleId map ${location.mapId} in " +
            s"${BackpressureProtocol.MAX_RETRY_ATTEMPTS} attempt(s), each bounded at " +
            s"${StreamingShuffleClientHandler.PRODUCER_CONNECTION_TIMEOUT_MS} ms.")
    }
  }

  /**
   * One connection attempt: builds this producer's handler, offers it to the connector, and either
   * registers the resulting stream or releases the handler again.
   *
   * @param location the producer to reach
   * @return the registered stream, or `None` if no channel could be established
   */
  private def connectOnce(location: StreamingShuffleProducerLocation): Option[ProducerStream] = {
    // The partition range is handed to the handler because it is the *authenticated* request: a
    // frame naming a partition outside it allocates nothing and is dropped, so the state a remote
    // producer can provoke on this consumer is bounded by what this task asked for rather than by
    // what the producer chooses to send.
    val handler = new StreamingShuffleClientHandler(conf, shuffleId, location.mapId,
      location.taskAttemptId, consumerId, startPartition, endPartition, backpressure, errorNotifier,
      clock)
    connector.connect(location, handler) match {
      case Some(client) =>
        val stream = new ProducerStream(location, handler, client)
        streamsLock.synchronized {
          producerStreams.append(stream)
        }
        if (debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} opened a channel to " +
            log"producer ${MDC(EXECUTOR_ID, location.executorId)} at " +
            log"${MDC(HOST_PORT, location.hostPort)} for map ${MDC(MAP_ID, location.mapId)}")
        }
        Some(stream)
      case None =>
        handler.close()
        None
    }
  }

  /** Issues the in-progress block request for every partition on every producer. */
  private def requestInProgressBlocks(streams: Seq[ProducerStream]): Unit = {
    streams.foreach { stream =>
      partitionIds.foreach { partitionId =>
        val requested = stream.handler.sendHeartbeatIfDue(partitionId)
        if (requested && debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} requested in-progress " +
            log"blocks for partition ${MDC(PARTITION_ID, partitionId)} from " +
            log"${MDC(HOST_PORT, stream.location.hostPort)}")
        }
      }
    }
  }

  /** Set once, so a sustained condition is reported once per read rather than every 100 ms. */
  private var slownessReported: Boolean = false

  /** Set once, so crossing the consumer-side stash threshold is reported once per read. */
  private var stashPressureReported: Boolean = false

  /**
   * Feeds the flow-control machinery the two things only a consumer can measure, on the cadence the
   * protocol owns.
   */
  private def sampleFlowControl(): Unit = {
    if (backpressure.isPollDue) {
      val throttled = backpressure.pollOnce()
      var producerRate = 0L
      var consumerRate = 0L
      val sampled = streamsLock.synchronized(registeredStreams.toSeq)
      sampled.foreach { key =>
        producerRate += backpressure.producerRateBytesPerSecond(key)
        consumerRate += backpressure.consumerRateBytesPerSecond(key)
      }
      fallbackPolicy.recordProducerThroughput(shuffleId, producerRate.toDouble)
      fallbackPolicy.recordConsumerThroughput(shuffleId, consumerRate.toDouble)
      // Measured ingress, not a summed ledger rate.
      fallbackPolicy.recordLinkUtilization(backpressure.ingressBytesPerSecond.toDouble)

      val sustainedSlow = sampled.exists(backpressure.isConsumerSustainedSlow)
      if ((sustainedSlow || backpressure.isLinkSaturated) && !slownessReported) {
        slownessReported = true
        // Both rates are qualified as this consumer's own observations, and the saturation figure
        // is named against the administered cap it is a ratio of. Neither qualifier is decoration:
        // the producer rate is summed from the ledgers of the streams THIS consumer registered, so
        // a producer streaming healthily to a peer reads as 0 B/s here, and the saturation ratio
        // exceeds 100% whenever more is moving than the configured cap allows -- which is a real
        // condition and deliberately not clamped. An unqualified line invited the reading that the
        // producer had stopped and that a percentage above 100 was a bug.
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} is degrading: consumer " +
          log"rate ${MDC(NUM_BYTES, consumerRate)} B/s against producer rate " +
          log"${MDC(VALUE, producerRate)} B/s, both as this consumer observes them on its own " +
          log"streams -- a producer rate of 0 B/s means no producer progress has been reported " +
          log"to this consumer rather than an idle producer -- at " +
          log"${MDC(THRESHOLD, backpressure.linkSaturationPercent)}% of the administered " +
          log"bandwidth cap, which reads above 100% when more is moving than the cap allows, " +
          log"with ${MDC(COUNT, throttled)} throttled stream(s). The fallback policy decides " +
          log"whether to yield to the sort-based shuffle")
      }
      if (stashedBytes.get() >= stashHighWaterBytes && !stashPressureReported) {
        stashPressureReported = true
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} consumer is holding " +
          log"${MDC(NUM_BYTES, stashedBytes.get())} byte(s), at or above its " +
          log"${MDC(THRESHOLD, stashHighWaterBytes)} byte threshold; the receive window will " +
          log"close until the reduce task catches up")
      }
    }
  }

  /** Tells the coordinator that a producer generation must not be handed to another consumer. */
  private def invalidateProducer(
      location: StreamingShuffleProducerLocation,
      reason: StreamingShuffleInvalidationReason,
      detail: String): Unit = {
    try {
      // Bounded by the same deadline as every other coordinator ask, and for a sharper reason here:
      // this runs while a fetch failure is already being raised, so an unbounded wait would hold
      // the task thread inside its own failure path for the generic two-minute default.
      val epoch = coordinatorRef.askSync[Any](InvalidateStreamingShuffleProducer(
        shuffleId, capabilityToken, location.generation, reason, detail), coordinatorTimeout)
      if (debugEnabled) {
        logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} invalidated producer " +
          log"${MDC(EXECUTOR_ID, location.executorId)} map ${MDC(MAP_ID, location.mapId)} for " +
          log"${MDC(REASON, reason.code)}; the coordinator advanced to epoch " +
          log"${MDC(VALUE, epoch)}")
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not tell the " +
          log"coordinator to invalidate producer ${MDC(EXECUTOR_ID, location.executorId)} map " +
          log"${MDC(MAP_ID, location.mapId)} for ${MDC(REASON, reason.code)}", e)
    }
  }

  /** Counts one partial-read invalidation on the executor's metrics and reports it upstream. */
  private def recordInvalidation(
      location: StreamingShuffleProducerLocation,
      reason: StreamingShuffleInvalidationReason,
      detail: String): Unit = {
    invalidations += 1
    StreamingShuffleMetricsSource.incrementPartialReadInvalidations(1L)
    invalidateProducer(location, reason, detail)
  }

  /** The block-manager identity of a producer, for the fetch-failure report. */
  private def blockManagerIdOf(location: StreamingShuffleProducerLocation): BlockManagerId = {
    location.blockManagerId
  }

  /**
   * Records a fully populated fetch failure against a named producer and raises it here, on the
   * calling thread, as the single failure of this read.
   *
   * @param location the producer the failure is attributed to, whose `MapStatus` address and
   *     map index are what let `MapOutputTracker` remove the exact dead map output
   * @param partitionId the reduce partition being read when the failure was detected
   * @param message the failure text, which is what an operator reads in the task-end reason
   * @param cause the underlying failure, or null when the message is the whole story
   */
  private def raiseFetchFailure(
      location: StreamingShuffleProducerLocation,
      partitionId: Int,
      message: String,
      cause: Throwable = null): Nothing = {
    val failure = new FetchFailedException(blockManagerIdOf(location), shuffleId, location.mapId,
      location.mapIndex, partitionId, message, cause)
    errorNotifier.setError(failure)
    errorNotifier.throwIfError()
    throw failure
  }

  /** Whether a partition belongs to this reduce task's range. */
  private def servesPartition(partitionId: Int): Boolean = {
    partitionId >= startPartition && partitionId < endPartition
  }

  /** A bounded wait that reads no clock and never calls `Thread.sleep`. */
  private def awaitQuietly(millis: Long): Unit = {
    if (millis > 0L) {
      quiesce.await(millis, TimeUnit.MILLISECONDS)
    }
  }

  /** Releases everything this reader acquired. */
  private def releaseResources(): Unit = {
    if (cleanedUp.compareAndSet(false, true)) {
      // Frees any bounded wait in flight before anything else, so cleanup cannot be delayed by one.
      quiesce.countDown()
      val (streams, ledgers) = streamsLock.synchronized {
        val streamSnapshot = producerStreams.toSeq
        val ledgerSnapshot = registeredStreams.toSeq
        producerStreams.clear()
        registeredStreams.clear()
        (streamSnapshot, ledgerSnapshot)
      }
      var released = 0L
      streams.foreach { stream =>
        released += stream.close()
      }
      ledgers.foreach { key =>
        backpressure.unregisterStream(key)
      }
      stashedBytes.set(0L)
      if (debugEnabled) {
        logDebug(log"Streaming shuffle reader for shuffle ${MDC(SHUFFLE_ID, shuffleId)} released " +
          log"${MDC(COUNT, streams.size)} channel(s) holding ${MDC(NUM_BYTES, released)} byte(s) " +
          log"after accepting ${MDC(NUM_BLOCKS, acceptedBlocks)} block(s) and " +
          log"${MDC(VALUE, acceptedBytes)} byte(s) with " +
          log"${MDC(THRESHOLD, invalidations)} invalidation(s)")
      }
    }
  }

  /** Renders an unexpected RPC reply for a diagnostic without trusting its `toString`. */
  private def describe(reply: Any): String = {
    if (reply == null) "null" else reply.getClass.getName
  }

  /** Consumer-side state for one partition of one producer. */
  private final class PartitionInbox(val partitionId: Int) {

    /** Verified-on-dequeue blocks waiting to be turned into records, in arrival order. */
    val blocks = new mutable.Queue[DataBlockMessage]

    /** Payload bytes currently queued here. */
    var bufferedBytes: Long = 0L

    /** The sequence number the next block for this partition must carry. */
    var nextSequenceNumber: Long = FIRST_SEQUENCE_NUMBER

    var consumedBlocks: Long = 0L

    var terminated: Boolean = false

    var announcedTotalBlocks: Long = StreamingShuffleClientHandler.NO_BLOCK_TOTAL

    /** When this reader first started waiting on this partition, read from the injected clock. */
    var awaitStartedMillis: Long = NO_TIMESTAMP
  }

  /**
   * One producer's worth of consumer-side state: the channel, the handler that owns it, the
   * per-partition inboxes, and the deserialization streams opened over them.
   */
  private final class ProducerStream(
      val location: StreamingShuffleProducerLocation,
      val handler: StreamingShuffleClientHandler,
      val client: TransportClient) {

    /** Whether this producer is this very executor, which decides local or remote accounting. */
    private val producerIsLocal: Boolean =
      Option(SparkEnv.get).map(_.executorId).contains(location.executorId)

    private val inboxes = new mutable.HashMap[Int, PartitionInbox]
    private val openStreams = new mutable.ArrayBuffer[DeserializationStream]
    private val closed = new AtomicBoolean(false)

    /** Blocks this producer delivered for partitions this task does not read. */
    private var foreignBlocksDiscarded: Long = 0L

    /** The ledger identity of one partition arriving from *this* producer generation. */
    private[streaming] def ledgerKey(partitionId: Int): BackpressureStreamKey =
      consumerKey(location, partitionId)

    /** Whether this producer's handler has reported a failure of its own. */
    def hasReportedFailure: Boolean = handler.reportedEscalationCount > 0L

    /** Whether every partition of this producer has ended, so nothing further is expected of it. */
    def isDrained: Boolean = firstUnfinishedPartition.isEmpty

    /** The lowest partition of this producer whose stream has not ended, if any. */
    def firstUnfinishedPartition: Option[Int] =
      partitionIds.find(partitionId => !inboxOf(partitionId).terminated)

    /** Whether this reduce task reads the given partition at all. */
    def readsPartition(partitionId: Int): Boolean =
      StreamingShuffleReader.this.servesPartition(partitionId)

    /** The records this producer contributes, partition by partition in ascending order. */
    def records: Iterator[(Any, Any)] = {
      partitionIds.iterator.flatMap(partitionId => new PartitionRecordIterator(this, partitionId))
    }

    /** The inbox for one partition, created on first use. */
    def inboxOf(partitionId: Int): PartitionInbox = {
      inboxes.getOrElseUpdate(partitionId, new PartitionInbox(partitionId))
    }

    /** Opens the record stream for one partition over the given payload stream. */
    def openRecords(partitionId: Int, payload: InputStream): Iterator[(Any, Any)] = {
      if (!handler.authenticatedTransportBound) {
        throw new SecurityException(
          s"Streaming shuffle $shuffleId map ${location.mapId} partition $partitionId refuses " +
            "deserialization because its producer channel did not complete Spark authentication.")
      }
      val blockId = ShuffleBlockId(shuffleId, location.mapId, partitionId)
      val wrapped = serializerManager.wrapStream(blockId, payload)
      val deserializationStream = serializerInstance.deserializeStream(wrapped)
      openStreams.append(deserializationStream)
      deserializationStream.asKeyValueIterator
    }

    /** The next verified block for one partition, or `None` at the orderly end of its stream. */
    def nextBlock(partitionId: Int): Option[DataBlockMessage] = {
      val inbox = inboxOf(partitionId)
      if (inbox.awaitStartedMillis == NO_TIMESTAMP) {
        inbox.awaitStartedMillis = clock.getTimeMillis()
      }
      var result: Option[DataBlockMessage] = None
      var finished = false
      while (!finished) {
        // Producer loss is converted into a fetch failure before the notifier is consulted, so a
        // transport error latched alongside the loss can never be what this task raises.
        convertSignalledProducerLoss(partitionId)
        checkForFailure(partitionId)
        checkFallbackWhileReading(partitionId)
        if (inbox.blocks.nonEmpty) {
          result = Some(acceptNext(inbox))
          finished = true
        } else if (inbox.terminated) {
          verifyStreamComplete(inbox)
          finished = true
        } else {
          pumpOnce(partitionId)
        }
      }
      result
    }

    /** Fails this read when streaming has stood down while this iterator was consuming. */
    private def checkFallbackWhileReading(partitionId: Int): Unit = {
      val latched = fallbackPolicy.knownShuffleFallback(shuffleId).orElse {
        // The latched reason and nothing substituted for it, exactly as at rendezvous.
        fallbackPolicy.trippedReason.map { reason =>
          declareShuffleFallback(reason,
            s"a consumer reading partition $partitionId observed the condition on its own " +
              "executor while streaming")
        }
      }
      latched.filter(_.fallenBack).foreach { state =>
        val description = state.cause.map(_.description).getOrElse(state.reasonName)
        // StaleEpoch, because that is what a fallback makes of every generation registered before
        // it: the coordinator has advanced the shuffle past the epoch this producer belongs to and
        // retired it, so the invalidation being reported is the epoch and not a fault of the
        // producer's own.
        failProducer(partitionId, StreamingShuffleInvalidationReason.StaleEpoch,
          s"streaming shuffle $shuffleId stood streaming down for every participant at epoch " +
            s"${state.declaredAtEpoch} because $description")
      }
    }

    /**
     * One poll window: renew the in-progress request, take at most one inbound event, and charge
     * the wait to fetch wait time so "fetch wait" keeps its usual meaning on this path.
     */
    private def pumpOnce(awaitedPartition: Int): Unit = {
      // The protocol owns the heartbeat interval, so this is a no-op unless one is actually due --
      // which is how the log and telemetry budgets survive a long wait.
      handler.sendHeartbeatIfDue(awaitedPartition)
      val waitStartMillis = clock.getTimeMillis()
      val event = handler.poll(POLL_WINDOW_MS)
      readMetrics.incFetchWaitTime(math.max(0L, clock.getTimeMillis() - waitStartMillis))
      event match {
        case Some(BlockReceived(block)) =>
          enqueue(block)
        case Some(StreamCompleted(completedPartition, totalBlocks)) =>
          markTerminated(completedPartition, totalBlocks)
        case Some(ProducerLost(lostPartition, reason, cause, invalidation)) =>
          val attributedPartition =
            if (servesPartition(lostPartition)) lostPartition else awaitedPartition
          // The attribution travels on the marker rather than being assumed here: only the handler
          // can tell a silent channel from a producer that contradicted its own end of stream, and
          // the two reach an operator's invalidation telemetry as different facts.
          failProducer(attributedPartition, invalidation, reason, cause)
        case None =>
          checkProducerLiveness(awaitedPartition)
      }
      sampleFlowControl()
    }

    /** Accepts one arrived block into its inbox, or discards it if it is not ours. */
    private def enqueue(block: DataBlockMessage): Unit = {
      val partitionId = block.partitionId()
      val payloadBytes = block.payloadLength().toLong
      if (!servesPartition(partitionId)) {
        foreignBlocksDiscarded += 1L
        if (foreignBlocksDiscarded == 1L) {
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} received a block for " +
            log"partition ${MDC(PARTITION_ID, partitionId)} from " +
            log"${MDC(HOST_PORT, location.hostPort)}, which this task does not read; it is " +
            log"discarded and deliberately not acknowledged, because the producer must keep it " +
            log"for the consumer entitled to it")
        } else if (debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} discarded block " +
            log"${MDC(COUNT, foreignBlocksDiscarded)} for a partition outside the range this " +
            log"task reads")
        }
      } else {
        val inbox = inboxOf(partitionId)
        inbox.blocks.enqueue(block)
        inbox.bufferedBytes += payloadBytes
        stashedBytes.addAndGet(payloadBytes)
        acceptedBlocks += 1L
        acceptedBytes += payloadBytes
        if (producerIsLocal) {
          readMetrics.incLocalBlocksFetched(1)
          readMetrics.incLocalBytesRead(payloadBytes)
        } else {
          readMetrics.incRemoteBlocksFetched(1)
          readMetrics.incRemoteBytesRead(payloadBytes)
        }
        if (debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} accepted block " +
            log"${MDC(COUNT, block.sequenceNumber())} of partition " +
            log"${MDC(PARTITION_ID, partitionId)} carrying ${MDC(NUM_BYTES, payloadBytes)} " +
            log"byte(s) from ${MDC(HOST_PORT, location.hostPort)}")
        }
      }
    }

    /** Records the orderly end of one partition's stream, taking the first total and keeping it. */
    private def markTerminated(completedPartition: Int, totalBlocks: Long): Unit = {
      if (servesPartition(completedPartition)) {
        val inbox = inboxOf(completedPartition)
        inbox.terminated = true
        if (inbox.announcedTotalBlocks == StreamingShuffleClientHandler.NO_BLOCK_TOTAL) {
          inbox.announcedTotalBlocks = totalBlocks
        }
        if (debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
            log"${MDC(PARTITION_ID, completedPartition)} ended after " +
            log"${MDC(NUM_BLOCKS, totalBlocks)} announced block(s) from " +
            log"${MDC(HOST_PORT, location.hostPort)}")
        }
      } else if (debugEnabled) {
        logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} ignored the end of " +
          log"partition ${MDC(PARTITION_ID, completedPartition)}, which this task does not read")
      }
    }

    /**
     * Applies the five-second producer connection timeout.
     *
     * Three readings are combined, because no single one covers every failure. The handler reports
     * a channel that went inactive. The handler and the protocol together report a partition that
     * has gone quiet after having delivered something. Neither covers the case that matters most --
     * a producer that delivers nothing at all, as after a crash or a network partition -- so the
     * elapsed time since this reader started waiting is measured here, against the most recent
     * inbound activity anywhere on the channel so that a producer busy with another partition is
     * never mistaken for a dead one.
     *
     * <b>Each reading reports its own interval.</b> The silence that reaches an operator is the
     * silence the handler measured when it reached the verdict, carried out of it on a
     * [[StreamingShuffleClientHandler.ProducerSilence]] rather than re-derived here. Re-deriving it
     * was wrong in exactly the case that matters: a producer declared silent by the flow-control
     * ledger has, by definition, delivered nothing this reader's per-partition reading could
     * measure, so that reading answered zero and the diagnostic asserted a silence of 0 ms to be
     * past a 5000 ms bound -- contradicting itself and withholding the one number that separates a
     * dead producer from a mis-tuned timeout.
     *
     * The three readings are taken before the notifier is consulted. Reached only from an empty
     * poll window, so the hand-off queue holds nothing at this point and a loss signalled on it has
     * already been applied; what remains is to prefer the reading that names a lost producer over a
     * throwable that merely accompanied one.
     */
    private def checkProducerLiveness(awaitedPartition: Int): Unit = {
      if (handler.isProducerLost) {
        failProducer(awaitedPartition, StreamingShuffleInvalidationReason.ConnectionTimeout,
          s"the producer channel became inactive " +
            s"${handler.producerLostElapsedMillis.getOrElse(0L)} ms ago")
      } else {
        handler.producerSilence(awaitedPartition) match {
          case Some(silence) =>
            failProducer(awaitedPartition, StreamingShuffleInvalidationReason.ConnectionTimeout,
              silence.describe(awaitedPartition))
          case None =>
            val inbox = inboxOf(awaitedPartition)
            val idleMillis = millisSinceAnyInbound.getOrElse {
              if (inbox.awaitStartedMillis == NO_TIMESTAMP) {
                0L
              } else {
                math.max(0L, clock.getTimeMillis() - inbox.awaitStartedMillis)
              }
            }
            if (idleMillis >= StreamingShuffleClientHandler.PRODUCER_CONNECTION_TIMEOUT_MS) {
              failProducer(awaitedPartition, StreamingShuffleInvalidationReason.ConnectionTimeout,
                s"nothing arrived on the channel for $idleMillis ms, past the " +
                  s"${StreamingShuffleClientHandler.PRODUCER_CONNECTION_TIMEOUT_MS} ms " +
                  "connection timeout")
            }
            // Consulted last, so that a producer loss this poll window revealed is raised as the
            // fetch failure it is rather than as whatever transport error accompanied it.
            checkForFailure(awaitedPartition)
        }
      }
    }

    /**
     * Milliseconds since anything at all arrived on this channel, across every partition the
     * handler has observed, or `None` while nothing has ever arrived.
     */
    private def millisSinceAnyInbound: Option[Long] = {
      val samples =
        handler.observedPartitionIds.flatMap(partitionId => handler.millisSinceInbound(partitionId))
      if (samples.isEmpty) None else Some(samples.min)
    }

    /** Takes the next block off an inbox and makes it fit to be read. */
    private def acceptNext(inbox: PartitionInbox): DataBlockMessage = {
      val block = dequeueHead(inbox)
      val expected = inbox.nextSequenceNumber
      val actual = block.sequenceNumber()
      if (actual != expected) {
        // A gap or a reordering means the stream this consumer is reading is not the stream the
        // producer sent.
        discardAll()
        recordInvalidation(location, StreamingShuffleInvalidationReason.ProtocolViolation,
          s"partition ${inbox.partitionId} expected sequence number $expected but received $actual")
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
          log"${MDC(PARTITION_ID, inbox.partitionId)} expected sequence number " +
          log"${MDC(COUNT, expected)} but received ${MDC(VALUE, actual)} from " +
          log"${MDC(HOST_PORT, location.hostPort)}")
        throw StreamingShuffleErrors.invalidSequenceNumber(
          shuffleId, inbox.partitionId, expected, actual)
      }
      val verified = verifyOrRepair(inbox, block)
      inbox.nextSequenceNumber = expected + 1L
      inbox.consumedBlocks += 1L
      verified
    }

    /** Removes the head block of an inbox, keeping both byte accounts in step. */
    private def dequeueHead(inbox: PartitionInbox): DataBlockMessage = {
      releaseFromInbox(inbox, inbox.blocks.dequeue())
    }

    /**
     * Takes one specific sequence number out of an inbox, wherever it is sitting in the queue.
     *
     * @return the extracted block, or `None` when the inbox does not hold that sequence number
     */
    private def dequeueBySequence(
        inbox: PartitionInbox,
        sequenceNumber: Long): Option[DataBlockMessage] = {
      inbox.blocks.dequeueFirst(_.sequenceNumber() == sequenceNumber)
        .map(block => releaseFromInbox(inbox, block))
    }

    /** Rewinds the byte accounting of one block that has just left an inbox. */
    private def releaseFromInbox(
        inbox: PartitionInbox,
        block: DataBlockMessage): DataBlockMessage = {
      val payloadBytes = block.payloadLength().toLong
      inbox.bufferedBytes = math.max(0L, inbox.bufferedBytes - payloadBytes)
      stashedBytes.addAndGet(-payloadBytes)
      block
    }

    /** Verifies a block's CRC32C and, when it fails, repairs it if repair is still possible. */
    private def verifyOrRepair(
        inbox: PartitionInbox,
        block: DataBlockMessage): DataBlockMessage = {
      var candidate = block
      var verified: Option[DataBlockMessage] = None
      var attempt = 0
      while (verified.isEmpty) {
        if (candidate.verifyChecksum()) {
          verified = Some(candidate)
        } else {
          val sequenceNumber = candidate.sequenceNumber()
          val computed = StreamingShuffleChecksum.computeBlock(candidate.shuffleId(),
            candidate.mapId(), candidate.partitionId(), sequenceNumber, candidate.copyPayload())
          val blockName = ShuffleBlockId(shuffleId, location.mapId, inbox.partitionId).name
          val retainedByProducer = sequenceNumber > handler.acknowledgedPosition(inbox.partitionId)
          val withinLedgerWindow = backpressure.isWithinUnacknowledgedWindow(
            ledgerKey(inbox.partitionId), sequenceNumber)
          checksumFailures += 1L
          checksumFailureLogGate.admit(clock.getTimeMillis()) match {
            case Some(unreported) =>
              logWarning(log"Streaming shuffle block ${MDC(BLOCK_ID, blockName)} failed CRC32C " +
                log"verification: the producer sent " +
                log"${MDC(CHECKSUM, candidate.checksum())} and this consumer computed " +
                log"${MDC(VALUE, computed)}. Retained by the producer: " +
                log"${MDC(REASON, retainedByProducer)}, ledger window: " +
                log"${MDC(THRESHOLD, withinLedgerWindow)} " +
                log"(${MDC(COUNT, checksumFailures)} failure(s) on this task, " +
                log"${MDC(NUM_SKIPPED, unreported)} not reported individually)")
            case None =>
              if (debugEnabled) {
                logDebug(log"Streaming shuffle block ${MDC(BLOCK_ID, blockName)} failed CRC32C " +
                  log"verification; computed ${MDC(VALUE, computed)}")
              }
          }
          attempt += 1
          val repairable = retainedByProducer &&
            attempt <= BackpressureProtocol.MAX_RETRY_ATTEMPTS &&
            !backpressure.isRetryExhausted(ledgerKey(inbox.partitionId))
          if (repairable) {
            // The candidate is replaced only if a replay actually arrives.
            requestReplay(inbox, sequenceNumber, attempt, computed, blockName).foreach { replayed =>
              candidate = replayed
            }
          } else {
            discardAll()
            recordInvalidation(location, StreamingShuffleInvalidationReason.ChecksumMismatch,
              s"block $blockName failed verification and could not be replayed")
            raiseFetchFailure(location, inbox.partitionId,
              s"Streaming shuffle block $blockName failed CRC32C verification after $attempt " +
                s"attempt(s) and cannot be replayed, so the upstream stage must be recomputed.",
              StreamingShuffleErrors.checksumVerificationFailed(
                blockName, shuffleId, candidate.checksum(), computed))
          }
        }
      }
      verified.get
    }

    /** Asks for one block to be sent again and waits for it, without sleeping. */
    private def requestReplay(
        inbox: PartitionInbox,
        sequenceNumber: Long,
        attempt: Int,
        computed: Long,
        blockName: String): Option[DataBlockMessage] = {
      val requested =
        handler.requestRetransmission(inbox.partitionId, sequenceNumber, sequenceNumber)
      val backoffMillis = backpressure.nextRetryBackoffMillis(ledgerKey(inbox.partitionId))
        .getOrElse(BackpressureProtocol.retryBackoffMillis(attempt))
      replayRequests += 1L
      replayRequestLogGate.admit(clock.getTimeMillis()) match {
        case Some(unreported) =>
          logWarning(log"Streaming shuffle requested a replay of block " +
            log"${MDC(BLOCK_ID, blockName)} (attempt ${MDC(MAX_ATTEMPTS, attempt)} of " +
            log"${MDC(COUNT, BackpressureProtocol.MAX_RETRY_ATTEMPTS)}, accepted: " +
            log"${MDC(REASON, requested)}, backoff ${MDC(TIMEOUT, backoffMillis)} ms; " +
            log"${MDC(NUM_BLOCKS, replayRequests)} replay(s) asked for on this task, " +
            log"${MDC(NUM_SKIPPED, unreported)} not reported individually)")
        case None =>
          if (debugEnabled) {
            logDebug(log"Streaming shuffle requested a replay of block " +
              log"${MDC(BLOCK_ID, blockName)} on attempt ${MDC(MAX_ATTEMPTS, attempt)}")
          }
      }
      var windows = math.max(1L, backoffMillis / POLL_WINDOW_MS)
      var replayed: Option[DataBlockMessage] = None
      while (replayed.isEmpty && windows > 0L && !cleanedUp.get()) {
        pumpOnce(inbox.partitionId)
        replayed = dequeueBySequence(inbox, sequenceNumber)
        windows -= 1L
      }
      if (replayed.isEmpty && debugEnabled) {
        logDebug(log"Streaming shuffle saw no replay of block ${MDC(BLOCK_ID, blockName)} within " +
          log"${MDC(TIMEOUT, backoffMillis)} ms; the computed checksum was " +
          log"${MDC(CHECKSUM, computed)}")
      }
      replayed
    }

    /** Confirms that a terminated stream delivered everything it announced. */
    private def verifyStreamComplete(inbox: PartitionInbox): Unit = {
      val announced = if (inbox.announcedTotalBlocks !=
          StreamingShuffleClientHandler.NO_BLOCK_TOTAL) {
        inbox.announcedTotalBlocks
      } else {
        handler.announcedBlockCount(inbox.partitionId)
          .orElse(backpressure.announcedBlockCount(ledgerKey(inbox.partitionId)))
          .getOrElse(StreamingShuffleClientHandler.NO_BLOCK_TOTAL)
      }
      if (announced != StreamingShuffleClientHandler.NO_BLOCK_TOTAL &&
          announced != inbox.consumedBlocks) {
        discardAll()
        recordInvalidation(location, StreamingShuffleInvalidationReason.IncompleteStream,
          s"partition ${inbox.partitionId} announced $announced block(s) but delivered " +
            s"${inbox.consumedBlocks}")
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
          log"${MDC(PARTITION_ID, inbox.partitionId)} announced " +
          log"${MDC(NUM_BLOCKS, announced)} block(s) but delivered " +
          log"${MDC(COUNT, inbox.consumedBlocks)} from ${MDC(HOST_PORT, location.hostPort)}")
        raiseFetchFailure(location, inbox.partitionId,
          s"Streaming shuffle $shuffleId partition ${inbox.partitionId} ended after " +
            s"${inbox.consumedBlocks} block(s) but $announced were announced.")
      } else if (debugEnabled) {
        logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
          log"${MDC(PARTITION_ID, inbox.partitionId)} completed with " +
          log"${MDC(NUM_BLOCKS, inbox.consumedBlocks)} block(s) consumed")
      }
    }

    /** Acknowledges consumption up to and including one block. */
    def acknowledge(partitionId: Int, sequenceNumber: Long): Unit = {
      if (sequenceNumber >= FIRST_SEQUENCE_NUMBER) {
        val advanced = handler.acknowledge(partitionId, sequenceNumber)
        if (advanced && debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} acknowledged position " +
            log"${MDC(COUNT, sequenceNumber)} of partition ${MDC(PARTITION_ID, partitionId)} to " +
            log"${MDC(HOST_PORT, location.hostPort)}")
        }
      }
    }

    /** The incompatible protocol version an I/O thread saw on this channel, if any. */
    private def incompatibleVersion: Option[Byte] = handler.incompatibleProtocolVersion

    /**
     * Discards, atomically and in one step, every block this consumer has accepted from this
     * producer, then closes the channel that could deliver more.
     *
     * @return the payload bytes released
     */
    def discardAll(): Long = synchronized {
      val released = discardBuffered()
      closeOpenStreams()
      // Through the connector, which owns the channel and is therefore the only party that may
      // decide whether releasing this handler releases the socket beneath it.
      connector.release(handler, client)
      released
    }

    /** Empties every inbox and rewinds the reader's byte accounting. */
    private def discardBuffered(): Long = {
      var released = 0L
      inboxes.valuesIterator.foreach { inbox =>
        released += inbox.bufferedBytes
        inbox.blocks.clear()
        inbox.bufferedBytes = 0L
      }
      stashedBytes.addAndGet(-released)
      released
    }

    /** Closes every record stream opened over this producer, tolerating a stream already closed. */
    private def closeOpenStreams(): Unit = {
      openStreams.foreach { stream =>
        try {
          stream.close()
        } catch {
          case NonFatal(e) =>
            if (debugEnabled) {
              logDebug(log"Streaming shuffle ignored a failure closing a record stream for " +
                log"shuffle ${MDC(SHUFFLE_ID, shuffleId)}: ${MDC(ERROR, e.getMessage)}")
            }
        }
      }
      openStreams.clear()
    }

    /**
     * Declares this producer lost, invalidates everything taken from it, and raises the one signal
     * the scheduler understands.
     *
     * @param cause the transport-level throwable that revealed the loss, when the handler
     *     carried one on its marker.
     */
    def failProducer(
        partitionId: Int,
        reason: StreamingShuffleInvalidationReason,
        detail: String,
        cause: Throwable = null): Nothing = {
      val versionCause = incompatibleVersion
      val released = discardAll()
      val attributedReason = if (versionCause.isDefined) {
        StreamingShuffleInvalidationReason.ProtocolViolation
      } else {
        reason
      }
      recordInvalidation(location, attributedReason, detail)
      val message = versionCause match {
        case Some(version) =>
          fallbackPolicy.checkProtocolVersion(version)
          // The same declaration the rendezvous makes when it reads an unspeakable version, for the
          // same reason and with the same effect.
          declareShuffleFallback(
            StreamingShuffleFallbackReason.ProtocolVersionMismatch,
            s"a producer streamed protocol version $version but this build speaks " +
              s"${StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION}")
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} saw protocol version " +
            log"${MDC(PROTOCOL_VERSION, version)} from ${MDC(HOST_PORT, location.hostPort)}, " +
            log"which this build cannot read; discarded ${MDC(NUM_BYTES, released)} byte(s) and " +
            log"fell back to the sort-based shuffle")
          s"Streaming shuffle producer ${location.hostPort} announced protocol version " +
            s"$version but this build speaks " +
            s"${StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION}; $detail."
        case None =>
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} invalidated partial " +
            log"reads of partition ${MDC(PARTITION_ID, partitionId)} from " +
            log"${MDC(HOST_PORT, location.hostPort)}: ${MDC(REASON, detail)}. Discarded " +
            log"${MDC(NUM_BYTES, released)} byte(s); the upstream stage will be recomputed")
          s"Streaming shuffle producer ${location.hostPort} for shuffle $shuffleId map " +
            s"${location.mapId} was lost: $detail."
      }
      raiseFetchFailure(location, partitionId, message, cause)
    }

    /**
     * Converts a producer loss the I/O threads have already signalled into the typed failure the
     * scheduler understands, before any raw transport error can be raised in its place.
     */
    def convertSignalledProducerLoss(awaitedPartition: Int): Unit = {
      if (errorNotifier.hasError || handler.isProducerLost) {
        var remaining = StreamingShuffleClientHandler.INBOUND_QUEUE_CAPACITY
        var draining = true
        while (draining && remaining > 0) {
          remaining -= 1
          handler.poll() match {
            case Some(BlockReceived(block)) => enqueue(block)
            case Some(StreamCompleted(completed, totalBlocks)) =>
              markTerminated(completed, totalBlocks)
            case Some(ProducerLost(lost, reason, cause, invalidation)) =>
              val attributed = if (servesPartition(lost)) lost else awaitedPartition
              failProducer(attributed, invalidation, reason, cause)
            case None => draining = false
          }
        }
      }
    }

    /** The same conversion for a caller that is not awaiting any particular partition. */
    def convertSignalledProducerLoss(): Unit = {
      partitionIds.find(partitionId => !inboxOf(partitionId).terminated)
        .foreach(partitionId => convertSignalledProducerLoss(partitionId))
    }

    /**
     * Releases this producer's channel, handler and buffers exactly once.
     *
     * @return the payload bytes released
     */
    def close(): Long = {
      if (closed.compareAndSet(false, true)) {
        // `discardAll` releases the handler through the connector, which closes the channel when it
        // is this handler's alone and leaves it open when it is shared.
        discardAll()
      } else {
        0L
      }
    }
  }

  /**
   * The payload of one partition of one producer, presented to the deserializer as a single
   * `InputStream`.
   */
  private final class BlockPayloadStream(stream: ProducerStream, partitionId: Int)
    extends InputStream {

    /** A read-only view of the block being consumed, or null between blocks. */
    private var current: ByteBuffer = null

    /** Sequence number of the block being consumed, which is what its acknowledgement names. */
    private var currentSequenceNumber: Long = NO_SEQUENCE_NUMBER

    /** Set once the partition's stream has ended and every block has been consumed. */
    private var exhausted: Boolean = false

    /** Whether this partition has any bytes at all, blocking until that is known. */
    def hasAvailableBytes: Boolean = ensureAvailable()

    override def read(): Int = {
      if (ensureAvailable()) {
        current.get() & UNSIGNED_BYTE_MASK
      } else {
        END_OF_STREAM
      }
    }

    override def read(target: Array[Byte], offset: Int, length: Int): Int = {
      if (target == null) {
        throw new NullPointerException("The target array must not be null.")
      } else if (offset < 0 || length < 0 || length > target.length - offset) {
        throw new IndexOutOfBoundsException(
          s"Cannot read $length byte(s) at offset $offset of a ${target.length} byte array.")
      } else if (length == 0) {
        0
      } else if (!ensureAvailable()) {
        END_OF_STREAM
      } else {
        // Bounded by what the current block still holds, so one call never spans two blocks and the
        // acknowledgement for a block is always issued exactly when that block is finished.
        val taken = math.min(length, current.remaining())
        current.get(target, offset, taken)
        taken
      }
    }

    override def available(): Int = if (current == null) 0 else current.remaining()

    /** Releases the block in hand and refuses to pull another. */
    override def close(): Unit = {
      releaseCurrent()
      exhausted = true
    }

    /** Ensures a byte is available to be read, pulling blocks until one is or the stream ends. */
    private def ensureAvailable(): Boolean = {
      while (!exhausted && (current == null || !current.hasRemaining)) {
        releaseCurrent()
        stream.nextBlock(partitionId) match {
          case Some(block) =>
            current = block.payloadBuffer()
            currentSequenceNumber = block.sequenceNumber()
          case None =>
            exhausted = true
        }
      }
      !exhausted && current != null && current.hasRemaining
    }

    /** Drops the block in hand and acknowledges it, which is what frees it on the producer. */
    private def releaseCurrent(): Unit = {
      if (current != null) {
        val consumed = currentSequenceNumber
        current = null
        currentSequenceNumber = NO_SEQUENCE_NUMBER
        stream.acknowledge(partitionId, consumed)
      }
    }
  }

  /** The records of one partition of one producer. */
  private final class PartitionRecordIterator(stream: ProducerStream, partitionId: Int)
    extends Iterator[(Any, Any)] {

    private var initialized: Boolean = false
    private var delegate: Iterator[(Any, Any)] = Iterator.empty

    override def hasNext: Boolean = {
      stream.convertSignalledProducerLoss(partitionId)
      checkForFailure(partitionId)
      initialize()
      delegate.hasNext
    }

    override def next(): (Any, Any) = {
      stream.convertSignalledProducerLoss(partitionId)
      checkForFailure(partitionId)
      initialize()
      if (!delegate.hasNext) {
        throw new NoSuchElementException(
          s"No further streamed records for shuffle $shuffleId partition $partitionId from " +
            s"${stream.location.hostPort}")
      }
      delegate.next()
    }

    /** Opens the record stream on first demand, or settles for an empty one when empty. */
    private def initialize(): Unit = {
      if (!initialized) {
        initialized = true
        val payload = new BlockPayloadStream(stream, partitionId)
        if (payload.hasAvailableBytes) {
          delegate = stream.openRecords(partitionId, payload)
        } else {
          payload.close()
          if (debugEnabled) {
            logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
              log"${MDC(PARTITION_ID, partitionId)} from " +
              log"${MDC(HOST_PORT, stream.location.hostPort)} carried no bytes")
          }
        }
      }
    }
  }
}

/** Constants and predicates shared by [[StreamingShuffleReader]] and its collaborators. */
private[spark] object StreamingShuffleReader {

  val PERCENT_SCALE: Long = 100L

  /** How long one blocking poll of the consumer queue waits. */
  val POLL_WINDOW_MS: Long = BackpressureProtocol.POLL_INTERVAL_MS

  val COORDINATOR_LOOKUP_INTERVAL_MS: Long = POLL_WINDOW_MS

  /** How many lookups the reader will attempt before declaring the producers unreachable. */
  val COORDINATOR_LOOKUP_MAX_ATTEMPTS: Int = math.max(1,
    (BackpressureProtocol.CONSUMER_LIVENESS_TIMEOUT_MS / COORDINATOR_LOOKUP_INTERVAL_MS).toInt)

  /** Floor on the coordinator ask deadline, in milliseconds. */
  val MIN_COORDINATOR_TIMEOUT_MS: Long = 1000L

  /** How long the production connector waits for its transport to release, in milliseconds. */
  val CONNECTOR_SHUTDOWN_TIMEOUT_MS: Long = BackpressureProtocol.ACK_TIMEOUT_MS

  /**
   * Most channel identities the production connector remembers as having gone inactive before their
   * handler was published.
   */
  val MAX_EARLY_INACTIVE_CHANNELS: Int = 4096

  /**
   * How many times a handler will try to join a channel its consumer already holds before opening
   * one of its own.
   */
  val MAX_SHARE_JOIN_ATTEMPTS: Int = 4

  val FIRST_SEQUENCE_NUMBER: Long = 0L

  val NO_SEQUENCE_NUMBER: Long = -1L

  val NO_TIMESTAMP: Long = -1L

  /** What `FetchFailedException` is given when no producer can be named, as its own doc allows. */
  val UNKNOWN_MAP_ID: Long = -1L

  /** What `FetchFailedException` is given for a map index a producer location cannot supply. */
  val UNKNOWN_MAP_INDEX: Int = -1

  val END_OF_STREAM: Int = -1

  /** Mask that promotes a signed byte to the unsigned value `InputStream.read` must return. */
  val UNSIGNED_BYTE_MASK: Int = 0xFF

  /**
   * Whether a map range is one streaming can serve, which is any well-formed window.
   *
   * @param startMapIndex first map index requested, inclusive
   * @param endMapIndex map index one past the last requested
   * @return true when the window is well formed
   */
  def servesMapRange(startMapIndex: Int, endMapIndex: Int): Boolean = {
    startMapIndex >= 0 && endMapIndex >= startMapIndex
  }

  /**
   * Whether a map range covers every map output of its shuffle.
   *
   * @param startMapIndex first map index requested, inclusive
   * @param endMapIndex map index one past the last requested
   * @return true when the window covers every map output the shuffle can have
   */
  def servesFullMapRange(startMapIndex: Int, endMapIndex: Int): Boolean = {
    startMapIndex == 0 && endMapIndex == Int.MaxValue
  }
}

/**
 * The collaborators a [[StreamingShuffleReader]] shares with the rest of the streaming subsystem.
 *
 * @param coordinatorRef reference to the driver's [[StreamingShuffleCoordinator]] endpoint,
 *     through which a consumer resolves live producer endpoints.
 * @param backpressure the executor-wide credit ledger and flow-control state machine.
 * @param fallbackPolicy the executor-wide policy that decides when streaming yields to the
 *     sort-based shuffle.
 * @param connector opens channels to producers.
 * @param serializerManager applies the same compression and encryption wrapping to a streamed
 *     payload that the classic path applies to a fetched block
 */
private[spark] case class StreamingShuffleReaderContext(
    coordinatorRef: RpcEndpointRef,
    backpressure: BackpressureProtocol,
    fallbackPolicy: StreamingShuffleFallbackPolicy,
    connector: StreamingShuffleProducerConnector,
    serializerManager: SerializerManager)

/** Opens consumer-side channels to streaming shuffle producers. */
private[spark] trait StreamingShuffleProducerConnector {

  /**
   * Opens a channel to one producer with the given handler bound to it.
   *
   * @param location the producer to reach
   * @param handler the consumer handler to bind; it becomes the owner of the returned client
   * @return the connected client, or `None` if it could not be established
   */
  def connect(
      location: StreamingShuffleProducerLocation,
      handler: StreamingShuffleClientHandler): Option[TransportClient]

  /**
   * Gives up one handler's participation in the channel [[connect]] returned for it.
   *
   * @param handler the handler giving up its participation
   * @param client the channel [[connect]] returned for that handler
   */
  def release(handler: StreamingShuffleClientHandler, client: TransportClient): Unit = {
    handler.close()
    if (client.isActive()) {
      client.close()
    }
  }

  /** Releases any process-wide resource this connector holds. */
  def close(): Unit
}

/**
 * The production [[StreamingShuffleProducerConnector]]: a Spark transport client factory on a
 * transport configuration scoped to the streaming module.
 *
 * @param conf the executor configuration the streaming transport namespace is read from
 * @param clock the time source the shutdown deadline is measured on, injected for the same
 *     reason the reader's is: a suite that has to observe a straggling channel must be able to
 *     reach the deadline by advancing a clock rather than by waiting out the full window
 */
private[spark] class NettyStreamingShuffleProducerConnector(
    conf: SparkConf,
    clock: Clock = new SystemClock)
  extends RpcHandler with StreamingShuffleProducerConnector with Logging {

  /** Security material used by both transport configuration and client bootstraps. */
  private val securityManager: SecurityManager =
    Option(SparkEnv.get).map(_.securityManager).getOrElse(new SecurityManager(conf))

  private val transportConf: TransportConf =
    StreamingShuffleServerHandler.streamingTransportConf(conf, security = Some(securityManager))

  private val transportContext: TransportContext = new TransportContext(transportConf, this)

  /** The bootstraps every channel this connector opens completes before a frame is exchanged. */
  private val clientBootstraps: java.util.List[TransportClientBootstrap] =
    StreamingShuffleServerHandler.streamingClientBootstraps(
      conf, transportConf, Some(securityManager))

  private val clientFactory: TransportClientFactory =
    transportContext.createClientFactory(clientBootstraps)

  /** The consumer handler that owns each live channel, keyed by the channel's own identity. */
  private val handlers = new ConcurrentHashMap[String, ChannelParticipants]()

  /** The channels available for a handler to join, keyed by consumer and producer endpoint. */
  private val shareable = new ConcurrentHashMap[String, ChannelShare]()

  /** The same shares, keyed by channel identity rather than by endpoint. */
  private val sharesByChannel = new ConcurrentHashMap[String, ChannelShare]()

  /** The claim on the connection currently being created on this thread. */
  private val connecting = new ThreadLocal[ConnectionClaim]()

  /** Every connection currently being created, across all connecting threads. */
  private val claims = ConcurrentHashMap.newKeySet[ConnectionClaim]()

  /** Channels that went inactive before their handler could be published, by channel identity. */
  private val earlyInactiveChannels = ConcurrentHashMap.newKeySet[String]()

  /** Channels closed here because their ownership could not be settled. */
  private val declinedConnections = new AtomicInteger(0)

  /** Handlers that joined a channel this consumer already held instead of opening one. */
  private val sharedJoins = new AtomicLong(0L)

  /** Terminal callbacks replayed at binding time because they arrived before it. */
  private val earlyTerminalCallbacks = new AtomicInteger(0)

  /** The unmanaged client of each live channel, keyed the same way as [[handlers]]. */
  private val clients = new ConcurrentHashMap[String, TransportClient]()

  /** Received data frames that arrived on a channel no handler could be found for. */
  private val unboundFrames = new AtomicLong(0L)

  /** Channels that were still open when the shutdown deadline passed. */
  private val unreleasedChannelCount = new AtomicInteger(0)

  /** Whether the transport event-loop termination future completed during [[close]]. */
  private val transportTerminatedOnClose = new AtomicBoolean(false)

  /** Client-factory event-loop group retained after its last channel leaves the registries. */
  private val transportEventLoopGroup = new AtomicReference[EventLoopGroup]()

  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  private val closed = new AtomicBoolean(false)

  /**
   * Opens one channel to one producer, as a single ownership transition.
   *
   * @param location the producer to reach
   * @param handler the consumer handler that will own the channel
   * @return the client, or `None` when no channel could be established or kept
   */
  override def connect(
      location: StreamingShuffleProducerLocation,
      handler: StreamingShuffleClientHandler): Option[TransportClient] = {
    if (closed.get()) {
      logWarning(log"Refusing to open a streaming shuffle channel to " +
        log"${MDC(HOST_PORT, location.hostPort)} because this connector is closed")
      None
    } else {
      // A channel this consumer already holds to this producer's executor serves this handler too,
      // and joining it costs no socket, no handshake and no accept on the far side.
      joinShare(location, handler).orElse(openShare(location, handler))
    }
  }

  /**
   * Joins a handler to a channel this consumer already holds to this producer's executor.
   *
   * @param location the producer to reach
   * @param handler the handler to bind
   * @return the shared channel, or `None` when this consumer holds none that can be joined
   */
  private def joinShare(
      location: StreamingShuffleProducerLocation,
      handler: StreamingShuffleClientHandler): Option[TransportClient] = {
    val key = shareKey(location, handler)
    var attempts = 0
    var joined: Option[TransportClient] = None
    var keepLooking = true
    while (keepLooking && attempts < StreamingShuffleReader.MAX_SHARE_JOIN_ATTEMPTS) {
      attempts += 1
      val share = shareable.get(key)
      if (share == null) {
        keepLooking = false
      } else if (!share.client.isActive() || closed.get()) {
        // A dead channel must not be handed to a handler that would then wait out its whole
        // connection timeout on it.
        shareable.remove(key, share)
      } else if (reserveShare(share)) {
        // The claim is held from here on, and it is what makes everything below safe: a concurrent
        // release cannot take the count to zero while this claim stands, so the participant set and
        // the receive window this handler is about to be bound to cannot be withdrawn underneath
        // it.
        share.participants.add(handler)
        if (!share.client.isActive() || closed.get()) {
          // The channel died inside the join.
          share.participants.remove(handler)
          shareable.remove(key, share)
          releaseClaim(share)
        } else {
          // The channel's window, not this handler's own: a socket carrying several producers is
          // closed while ANY of them is throttled and reopened only when none is, so a joiner has
          // to adopt the shared gate before it can be given a reason to throttle.
          handler.joinReadGate(share.readGate)
          // Announced on this thread, exactly as a freshly created channel's subscription is, so
          // the in-progress request for this producer is issued once the handler is reachable.
          handler.channelActive(share.client)
          sharedJoins.incrementAndGet()
          if (debugEnabled) {
            logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, handler.shuffleId)} joined map " +
              log"${MDC(MAP_ID, handler.mapId)} to the channel this consumer already holds to " +
              log"${MDC(HOST_PORT, location.hostPort)}, now carrying " +
              log"${MDC(COUNT, share.participants.size)} producer(s) on " +
              log"${MDC(NUM_BLOCKS, share.claimCount)} claim(s)")
          }
          joined = Some(share.client)
          keepLooking = false
        }
      } else {
        // The share is sealed: its last participant has left, or it has been withdrawn as unusable.
        shareable.remove(key, share)
      }
    }
    joined
  }

  /**
   * Takes a place on a shared channel, as one step with whatever a subclass needs to observe.
   *
   * @param share the channel being joined
   * @return true when a place was reserved and the caller may publish into the share
   */
  protected def reserveShare(share: ChannelShare): Boolean = share.join()

  /**
   * Opens a new channel for a handler and publishes it as this consumer's share for that endpoint.
   *
   * @param location the producer to reach
   * @param handler the handler to bind
   * @return the new channel, or `None` when it could not be established
   */
  private def openShare(
      location: StreamingShuffleProducerLocation,
      handler: StreamingShuffleClientHandler): Option[TransportClient] = {
    {
      val claim = new ConnectionClaim(handler)
      connecting.set(claim)
      claims.add(claim)
      try {
        val client = createTransportClient(location.host, location.port)
        rememberTransportEventLoop(client)
        // The share is published by the binding, and only once the binding has succeeded: a share
        // for a channel that was then declined would hand a further handler a socket nothing owns.
        if (bind(client, handler, claim, shareKey(location, handler)).isDefined) {
          // The transport raises channelActive from the pipeline before the client is returned,
          // which is earlier than the binding above on a channel that connected before this thread
          // resumed.
          handler.channelActive(client)
          Some(client)
        } else {
          None
        }
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          logWarning(log"Interrupted while connecting a streaming shuffle channel to " +
            log"${MDC(HOST_PORT, location.hostPort)}")
          None
        case NonFatal(e) =>
          logWarning(log"Failed to connect a streaming shuffle channel to " +
            log"${MDC(HOST_PORT, location.hostPort)}: ${MDC(ERROR, e.getMessage)}")
          None
      } finally {
        claims.remove(claim)
        connecting.remove()
        // With no connection being created, no remembered channel identity can ever be consumed by
        // a binding, so the record is swept here rather than being left to a timestamp or a reaper.
        if (claims.isEmpty()) {
          earlyInactiveChannels.clear()
        }
      }
    }
  }

  /**
   * Opens one socket to one producer, authenticating it if the application is authenticated.
   *
   * @param host producer host to reach
   * @param port producer streaming port to reach
   * @return the authenticated, unmanaged client
   */
  protected def createTransportClient(host: String, port: Int): TransportClient =
    clientFactory.createUnmanagedClient(host, port)

  /**
   * Pauses inbound traffic on every live channel without closing it.
   *
   * @return the number of active channels that were paused
   */
  private[streaming] def pauseInboundTraffic(): Int = {
    var paused = 0
    clients.values().asScala.foreach { client =>
      val channel = client.getChannel
      if (channel != null && channel.isActive && channel.config().isAutoRead) {
        channel.config().setAutoRead(false)
        paused += 1
      }
    }
    paused
  }

  /**
   * Releases the transport this connector owns, once, and does not return until it is released or a
   * bounded deadline has passed.
   */
  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      // Cancelled before anything is released, and before the registries are read.
      claims.asScala.foreach(_.cancel())
      val bound = handlers.size()
      // Every participant of every channel, because a channel now carries the handlers of one
      // consumer rather than one handler.
      handlers.values().asScala.foreach { participants =>
        participants.retire().foreach { handler =>
          try {
            handler.close(releaseChannel = false)
          } catch {
            case NonFatal(e) =>
              logWarning(log"A streaming shuffle consumer handler failed to close: " +
                log"${MDC(ERROR, e.getMessage)}")
          }
        }
      }
      handlers.clear()
      // Sealed before the maps are emptied, so a connecting thread that already holds a share
      // cannot take a claim on a channel this method is about to close.
      sharesByChannel.values().asScala.foreach(_.seal())
      sharesByChannel.clear()
      shareable.clear()
      sharesByChannel.values().asScala.foreach(_.retire())
      sharesByChannel.clear()
      val channels = liveChannels()
      val group = Option(transportEventLoopGroup.get())
      val straggling = closeChannels(channels)
      clients.clear()
      clientFactory.close()
      transportContext.close()
      val terminated = awaitEventLoopTermination(group)
      transportTerminatedOnClose.set(terminated)
      unreleasedChannelCount.set(straggling)
      if (straggling > 0 || !terminated) {
        logWarning(log"Streaming shuffle producer connector for module " +
          log"${MDC(VALUE, transportConf.getModuleName())} did not release cleanly: " +
          log"${MDC(COUNT, straggling)} of ${MDC(NUM_TASKS, channels.size)} channel(s) were " +
          log"still open after " +
          log"${MDC(TIMEOUT, StreamingShuffleReader.CONNECTOR_SHUTDOWN_TIMEOUT_MS)} ms and the " +
          log"transport event loop terminated: ${MDC(STATUS, terminated)}")
      } else if (debugEnabled) {
        logDebug(log"Streaming shuffle producer connector for module " +
          log"${MDC(VALUE, transportConf.getModuleName())} released " +
          log"${MDC(COUNT, bound)} channel(s) after " +
          log"${MDC(NUM_BLOCKS, unboundFrames.get())} unbound frame(s)")
      }
      earlyInactiveChannels.clear()
    }
  }

  /** How many channels this connector could not confirm closed, once it has stopped. */
  def unreleasedChannels: Int = unreleasedChannelCount.get()

  /** Whether this connector's transport event-loop group completed termination during close. */
  private[streaming] def transportResourcesTerminated: Boolean =
    transportTerminatedOnClose.get()

  /**
   * Received frames that arrived on a channel this connector could not route, since it was created.
   */
  private[streaming] def unboundFrameCount: Long = unboundFrames.get()

  /**
   * Channels this connector opened and then closed itself because their ownership could not be
   * settled -- the connector was closing while the channel was being created.
   */
  def declinedConnectionCount: Int = declinedConnections.get()

  /**
   * Terminal callbacks that arrived before their channel's handler was published and were replayed
   * to that handler at binding time.
   */
  def earlyTerminalCallbackCount: Int = earlyTerminalCallbacks.get()

  /** Whether this connector still holds a registration for any channel. */
  def boundChannelCount: Int = handlers.size()

  /**
   * Retains the exact event-loop group this connector must await after its final channel is gone.
   */
  private def rememberTransportEventLoop(client: TransportClient): Unit = {
    Option(client.getChannel()).flatMap(channel => Option(channel.eventLoop()))
      .flatMap(loop => Option(loop.parent())).foreach { group =>
        val existing = transportEventLoopGroup.get()
        val retained = if (existing == null) {
          transportEventLoopGroup.compareAndSet(null, group)
          transportEventLoopGroup.get()
        } else {
          existing
        }
        if (!(retained eq group)) {
          throw new IllegalStateException(
            "One streaming shuffle connector received channels from multiple event-loop groups.")
        }
      }
  }

  /** The channels this connector still holds, snapshotted before the factory is closed. */
  private def liveChannels(): Seq[Channel] = {
    clients.values().asScala.toSeq.flatMap(client => Option(client.getChannel()))
  }

  /**
   * Closes every channel and waits, within one shared deadline, for each to confirm it.
   *
   * @param channels the channels to release
   * @return how many were still open when the deadline passed
   */
  private def closeChannels(channels: Seq[Channel]): Int = {
    channels.foreach { channel =>
      try {
        channel.close()
      } catch {
        case NonFatal(e) =>
          logWarning(log"A streaming shuffle consumer channel failed to close: " +
            log"${MDC(ERROR, e.getMessage)}")
      }
    }
    val deadline =
      clock.getTimeMillis() + StreamingShuffleReader.CONNECTOR_SHUTDOWN_TIMEOUT_MS
    var straggling = 0
    channels.foreach { channel =>
      val remaining = deadline - clock.getTimeMillis()
      val confirmed = try {
        if (remaining <= 0L) {
          !channel.isOpen
        } else {
          channel.closeFuture().await(remaining, TimeUnit.MILLISECONDS) || !channel.isOpen
        }
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          !channel.isOpen
        case NonFatal(_) =>
          !channel.isOpen
      }
      if (!confirmed) {
        straggling += 1
      }
    }
    straggling
  }

  /**
   * Waits, within the same bound, for the transport event loop to finish winding down.
   *
   * @param group the event loop group the channels ran on, if any
   * @return whether the group confirmed termination
   */
  private def awaitEventLoopTermination(group: Option[EventLoopGroup]): Boolean = {
    group match {
      case None => true
      case Some(loops) =>
        try {
          loops.awaitTermination(StreamingShuffleReader.CONNECTOR_SHUTDOWN_TIMEOUT_MS,
            TimeUnit.MILLISECONDS) ||
            loops.isTerminated
        } catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
            loops.isTerminated
          case NonFatal(_) =>
            loops.isTerminated
        }
    }
  }

  /**
   * A streaming consumer channel serves no chunked stream, so the manager offered here is an empty
   * one.
   */
  private val emptyStreamManager: StreamManager = new OneForOneStreamManager()

  override def getStreamManager(): StreamManager = emptyStreamManager

  override def receive(client: TransportClient, message: ByteBuffer): Unit = {
    dispatchTo(client, message)(_.receive(client, message))
  }

  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    dispatchTo(client, message)(_.receive(client, message, callback))
  }

  /** Forwards a channel becoming active, and stays quiet when there is nothing to forward it to. */
  override def channelActive(client: TransportClient): Unit = {
    participantsOf(client) match {
      case handlers if handlers.nonEmpty =>
        handlers.foreach(_.channelActive(client))
      case _ =>
        if (debugEnabled) {
          logDebug(log"A streaming shuffle channel to " +
            log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))} became active " +
            log"before its consumer handler was published; the connecting thread announces the " +
            log"subscription itself once the binding completes")
        }
    }
  }

  /** Forwards the loss of a channel and forgets it. */
  override def channelInactive(client: TransportClient): Unit = {
    val key = channelKeyOf(client)
    clients.remove(key)
    // The share goes with the channel, so no further handler can be given a socket that has just
    // died and then wait out its whole connection timeout on it.
    forgetShare(client)
    Option(handlers.get(key)) match {
      case Some(participants) =>
        // Retired before it is unregistered, and in that order: retirement is what refuses a
        // reservation another thread may be holding, so doing it first means no joiner can publish
        // itself into a set this method is about to abandon.
        val held = participants.retire()
        handlers.remove(key, participants)
        // Every participant is told, because a lost channel is lost for all of them, and each has
        // to convert that loss into the fetch failure that recomputes its own map task.
        held.foreach(_.channelInactive(client))
      case None =>
        recordEarlyInactive(key)
        currentlyConnecting.foreach(_.channelInactive(client))
    }
  }

  /**
   * Remembers that one channel went inactive before its handler was published.
   *
   * @param channelKey identity of the channel that went inactive
   */
  private def recordEarlyInactive(channelKey: String): Unit = {
    if (!claims.isEmpty() &&
        earlyInactiveChannels.size() < StreamingShuffleReader.MAX_EARLY_INACTIVE_CHANNELS) {
      earlyInactiveChannels.add(channelKey)
    }
  }

  /** Forwards a channel level failure, and stays quiet when there is nothing to forward it to. */
  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    participantsOf(client) match {
      case handlers if handlers.nonEmpty =>
        handlers.foreach(_.exceptionCaught(cause, client))
      case _ =>
        if (debugEnabled) {
          logDebug(log"A streaming shuffle channel to " +
            log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))} failed with " +
            log"${MDC(ERROR, cause.getMessage)} with no consumer handler bound to it; the " +
            log"connect path or the channel's own release reports this failure")
        }
    }
  }

  /** The handler that owns one channel, if that channel can still be answered for. */
  private def handlerFor(
      client: TransportClient,
      message: ByteBuffer): Option[StreamingShuffleClientHandler] = {
    Option(handlers.get(channelKeyOf(client)))
      .flatMap(participants => participants.routeFor(message))
      .orElse(currentlyConnecting.filter(handler => handler.routes(message)))
  }

  /**
   * Every handler bound to one channel, for a callback that concerns the channel rather than a
   * frame.
   */
  private def participantsOf(client: TransportClient): Seq[StreamingShuffleClientHandler] = {
    Option(handlers.get(channelKeyOf(client))) match {
      case Some(participants) => participants.snapshot()
      case None => currentlyConnecting.toSeq
    }
  }

  /** The key under which a handler's consumer shares a channel to one producer endpoint. */
  private def shareKey(
      location: StreamingShuffleProducerLocation,
      handler: StreamingShuffleClientHandler): String =
    s"${handler.consumerId}@${location.hostPort}"

  /**
   * Withdraws the share of a channel that has gone, so no further handler can join a dead socket.
   *
   * @param client the channel that has gone
   */
  private def forgetShare(client: TransportClient): Unit = {
    val share = sharesByChannel.remove(channelKeyOf(client))
    if (share != null) {
      shareable.remove(share.shareKey, share)
    }
  }

  /** Gives up one handler's participation, closing the channel only when it was the last. */
  override def release(
      handler: StreamingShuffleClientHandler,
      client: TransportClient): Unit = {
    val key = channelKeyOf(client)
    val share = sharesByChannel.get(key)
    val participants = if (share != null) share.participants else handlers.get(key)
    val wasParticipant = participants != null && participants.remove(handler)
    // The handler is released whether or not it was still registered: a channel lost earlier
    // already removed the whole participant set, and the handler still holds receive quota that
    // must come back.
    handler.close(releaseChannel = false)
    if (wasParticipant && share != null) {
      releaseClaim(share)
    }
  }

  /**
   * Whether this connector still holds a joinable share for one channel.
   *
   * @param client the channel to ask about
   * @return true when a further handler of the same consumer could still join it
   */
  private[streaming] def isShareJoinable(client: TransportClient): Boolean = {
    val share = sharesByChannel.get(channelKeyOf(client))
    share != null && share.isJoinable
  }

  /**
   * How many claims stand on one channel, which is how many participants must leave before it is
   * released.
   *
   * @param client the channel to ask about
   * @return the outstanding claim count, or zero when this connector holds no share for it
   */
  private[streaming] def channelClaimCount(client: TransportClient): Int = {
    val share = sharesByChannel.get(channelKeyOf(client))
    if (share == null) 0 else share.claimCount
  }

  /** Handlers joined to a channel this consumer already held, since this connector was created. */
  private[streaming] def sharedJoinCount: Long = sharedJoins.get()

  /** Stable snapshot of the live consumer routes and the real transport clients carrying them. */
  private[streaming] def activeRoutes:
      Seq[(StreamingShuffleClientHandler, TransportClient)] = {
    handlers.entrySet().asScala.toSeq.flatMap { entry =>
      val client = clients.get(entry.getKey)
      if (client == null) {
        Nil
      } else {
        entry.getValue.snapshot().map(handler => handler -> client)
      }
    }
  }

  /** Routes one received frame to the handler that owns the channel it arrived on. */
  private def dispatchTo(client: TransportClient, message: ByteBuffer)(
      action: StreamingShuffleClientHandler => Unit): Unit = {
    handlerFor(client, message) match {
      case Some(handler) =>
        action(handler)
      case None =>
        val unbound = unboundFrames.incrementAndGet()
        if (unbound == 1L) {
          logWarning(log"A streaming shuffle data frame arrived from " +
            log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))} on a channel " +
            log"with no consumer handler bound to it. Further occurrences are counted but " +
            log"not logged")
        }
    }
  }

  /** The claim of the connection this thread is in the middle of creating, if any. */
  private def currentClaim: Option[ConnectionClaim] = Option(connecting.get())

  /** The handler of the connection this thread is in the middle of creating, if any. */
  private def currentlyConnecting: Option[StreamingShuffleClientHandler] =
    currentClaim.map(_.handler)

  /**
   * Completes one connection's ownership transition, or declines it.
   *
   * @param client the transport client the factory returned
   * @param handler the consumer handler that owns it
   * @param claim the claim registered before the client was created
   * @param sharePublishKey the key the channel's share is published under
   * @return the published share when the channel is owned, `None` when it has been declined and
   *     closed
   */
  private def bind(
      client: TransportClient,
      handler: StreamingShuffleClientHandler,
      claim: ConnectionClaim,
      sharePublishKey: String): Option[ChannelShare] = {
    val key = channelKeyOf(client)
    if (StreamingShuffleServerHandler.authenticatedPrincipal(client).isEmpty) {
      declineConnection(client, "the channel did not complete Spark authentication")
      None
    } else if (closed.get() || claim.isCancelled) {
      declineConnection(client, "the connector closed while the channel was being created")
      None
    } else {
      // A participant set rather than a single handler, because further handlers of this same
      // consumer join this channel instead of opening one of their own.
      val participants = new ChannelParticipants(handler)
      // The channel's receive window is this handler's own to begin with, because a frame can reach
      // the handler on this very thread before the share exists; every handler that joins later
      // adopts the same gate, so the socket obeys the union of their intents rather than the last
      // one to state an intent.
      val share = new ChannelShare(
        client, sharePublishKey, key, participants, handler.currentReadGate)
      handlers.put(key, participants)
      clients.put(key, client)
      sharesByChannel.put(key, share)
      if (closed.get() || claim.isCancelled) {
        withdrawChannel(share)
        declineConnection(client, "the connector closed as the channel was being published")
        None
      } else if (earlyInactiveChannels.remove(key)) {
        // The channel died before it could be published.
        earlyTerminalCallbacks.incrementAndGet()
        withdrawChannel(share)
        if (debugEnabled) {
          logDebug(log"A streaming shuffle channel to " +
            log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))} went inactive " +
            log"before its consumer handler was published; the callback is replayed on the " +
            log"connecting thread so the reduce task learns of the loss at once")
        }
        handler.channelInactive(client)
        None
      } else {
        shareable.put(sharePublishKey, share)
        Some(share)
      }
    }
  }

  /**
   * Withdraws one channel from every registry that names it, without closing it.
   *
   * @param share the share to withdraw
   */
  private def withdrawChannel(share: ChannelShare): Unit = {
    share.seal()
    shareable.remove(share.shareKey, share)
    handlers.remove(share.channelKey, share.participants)
    clients.remove(share.channelKey)
    sharesByChannel.remove(share.channelKey, share)
  }

  /**
   * Gives one claim back and releases the channel when it was the last.
   *
   * @param share the share whose claim is being given back
   * @return true when this call released the channel
   */
  private def releaseClaim(share: ChannelShare): Boolean = {
    if (share.leave()) {
      shareable.remove(share.shareKey, share)
      handlers.remove(share.channelKey, share.participants)
      clients.remove(share.channelKey)
      sharesByChannel.remove(share.channelKey, share)
      val client = share.client
      if (client.isActive()) {
        client.close()
      }
      if (debugEnabled) {
        logDebug(log"Streaming shuffle released the consumer channel to " +
          log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))} after its last " +
          log"producer finished")
      }
      true
    } else {
      false
    }
  }

  /**
   * Closes a channel this connector has decided not to own, and says why.
   *
   * @param client the client whose channel is being given up
   * @param reason operator-facing context for the diagnostic
   */
  private def declineConnection(client: TransportClient, reason: String): Unit = {
    declinedConnections.incrementAndGet()
    logWarning(log"Closing a streaming shuffle channel to " +
      log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))} because " +
      log"${MDC(REASON, reason)}")
    try {
      Option(client.getChannel()).foreach(_.close())
      client.close()
    } catch {
      case NonFatal(e) =>
        logWarning(log"A declined streaming shuffle channel did not close cleanly: " +
          log"${MDC(ERROR, e.getMessage)}")
    }
  }

  /**
   * One connection whose creation has begun and whose ownership has not yet been settled.
   *
   * @param handler the consumer handler that will own the channel
   */
  private final class ConnectionClaim(val handler: StreamingShuffleClientHandler) {

    private val cancelled = new AtomicBoolean(false)

    /** Whether [[close]] has decided this connection must not be published. */
    def isCancelled: Boolean = cancelled.get()

    /** Marks this connection as one the connecting thread must give up rather than publish. */
    def cancel(): Unit = cancelled.set(true)
  }

  /**
   * The handlers bound to one channel, and the routing of a frame to the one that owns it.
   *
   * @param initial the handler this channel was opened for, so the set is never momentarily
   *     empty
   */
  protected final class ChannelParticipants(initial: StreamingShuffleClientHandler) {

    private val byProducer = new ConcurrentHashMap[(Int, Long), StreamingShuffleClientHandler]()

    private val lifecycle = new Object()

    private var retired: Boolean = false

    byProducer.put((initial.shuffleId, initial.mapId), initial)

    /** Binds one more handler so frames for its producer can be routed to it. */
    def add(handler: StreamingShuffleClientHandler): Unit = lifecycle.synchronized {
      byProducer.put((handler.shuffleId, handler.mapId), handler)
    }

    /**
     * Unbinds one handler, value-qualified so a handler already replaced by a later attempt of the
     * same map task cannot unbind the one that replaced it.
     *
     * @return true when this handler was still bound, which is what tells its caller to give
     *     the corresponding claim back exactly once
     */
    def remove(handler: StreamingShuffleClientHandler): Boolean = lifecycle.synchronized {
      byProducer.remove((handler.shuffleId, handler.mapId), handler)
    }

    /**
     * Retires this channel outright and returns everything it held, for a lost channel and for the
     * connector's own shutdown.
     */
    def retire(): Seq[StreamingShuffleClientHandler] = lifecycle.synchronized {
      retired = true
      val held = byProducer.values().asScala.toSeq
      byProducer.clear()
      held
    }

    /** Whether this channel has been retired outright. */
    def isRetired: Boolean = lifecycle.synchronized(retired)

    /** How many handlers are bound to this channel. */
    def size: Int = byProducer.size()

    /** The handlers bound now, as a stable sequence a callback can be fanned out over. */
    def snapshot(): Seq[StreamingShuffleClientHandler] = byProducer.values().asScala.toSeq

    /** The handler a frame belongs to, by the producer its header names. */
    def routeFor(message: ByteBuffer): Option[StreamingShuffleClientHandler] = {
      try {
        val shuffleId = StreamingShuffleMessage.peekShuffleId(message)
        val mapId = StreamingShuffleMessage.peekMapId(message)
        Option(byProducer.get((shuffleId, mapId)))
      } catch {
        case NonFatal(_) =>
          // A frame too short or too malformed to name a producer cannot be routed.
          val held = snapshot()
          if (held.size == 1) Some(held.head) else None
      }
    }
  }

  /**
   * One physical channel, everything bound to it, and the claim count that decides its lifetime.
   *
   * @param client the channel being shared
   * @param shareKey the key this share is published under, so it can be withdrawn without a
   *     scan
   * @param channelKey the identity of the channel, matching the routing registries
   * @param participants the handlers bound to this channel
   * @param readGate the receive window of this channel, shared by every participant
   */
  protected final class ChannelShare(
      val client: TransportClient,
      val shareKey: String,
      val channelKey: String,
      val participants: ChannelParticipants,
      val readGate: StreamingShuffleChannelReadGate) {

    /** Outstanding claims on this channel. */
    private var claims: Int = 1

    /** Whether the share has been sealed, after which no further claim may be taken. */
    private var sealedOff: Boolean = false

    /**
     * Takes one claim, or refuses because this channel can no longer be joined.
     *
     * @return true when a claim was taken and the caller must give it back exactly once
     */
    def join(): Boolean = synchronized {
      if (sealedOff || claims <= 0) {
        false
      } else {
        claims += 1
        true
      }
    }

    /**
     * Gives a claim back, and says whether it was the last one.
     *
     * @return true when this call took the claim count to zero and thereby sealed the share
     */
    def leave(): Boolean = synchronized {
      if (claims <= 0) {
        false
      } else {
        claims -= 1
        if (claims == 0) {
          sealedOff = true
          true
        } else {
          false
        }
      }
    }

    /**
     * Refuses every further join without waiting for the participants to leave.
     *
     * @return true when this call was the one that sealed the share
     */
    def seal(): Boolean = synchronized {
      val wasJoinable = !sealedOff && claims > 0
      claims = 0
      sealedOff = true
      wasJoinable
    }

    /** Seals the share and hands back everything the channel still carried. */
    def retire(): Seq[StreamingShuffleClientHandler] = {
      seal()
      participants.retire()
    }

    /** Whether this share can still be joined. */
    def isJoinable: Boolean = synchronized(!sealedOff && claims > 0)

    /** How many claims are outstanding on this channel. */
    def claimCount: Int = synchronized(math.max(0, claims))
  }

  /** The identity of one channel. */
  private def channelKeyOf(client: TransportClient): String = {
    val channel = client.getChannel()
    if (channel == null) String.valueOf(client.getSocketAddress()) else channel.id().asLongText()
  }
}
