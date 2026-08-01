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
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.util.control.NonFatal

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInitializer, ChannelOption, EventLoopGroup}
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.ByteToMessageDecoder

import org.apache.spark.{Aggregator, InterruptibleIterator, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{BLOCK_ID, CHECKSUM, COUNT, ERROR, EXECUTOR_ID, HOST_PORT, MAP_ID, MAX_ATTEMPTS, NUM_BLOCKS, NUM_BYTES, NUM_PARTITIONS, PARTITION_ID, PROTOCOL_VERSION, REASON, SHUFFLE_ID, THRESHOLD, TIMEOUT, VALUE}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.protocol.streaming.{DataBlockMessage, StreamingShuffleChecksum, StreamingShuffleMessage, StreamingShuffleMessageType}
import org.apache.spark.network.util.{IOMode, NettyUtils, TransportConf}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.serializer.{DeserializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.streaming.StreamingShuffleClientHandler.{BlockReceived, ProducerLost, StreamCompleted}
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.{Clock, CompletionIterator, SystemClock}
import org.apache.spark.util.collection.ExternalSorter

/**
 * The consumer half of a streaming shuffle: a [[ShuffleReader]] that consumes a reduce partition
 * from producer executors while the map stage is still producing it, instead of waiting for the map
 * output to be materialised on disk and then fetching it.
 *
 * <b>What this reader replaces, and what it deliberately does not.</b> The classic path -- see
 * `BlockStoreShuffleReader` -- learns block locations from `MapOutputTracker` and pulls finished
 * files. This reader instead learns <i>live producer endpoints</i> from
 * [[StreamingShuffleCoordinator]], because a producer that has not finished has no file to name and
 * no entry a location tracker could hold. `MapOutputTracker` is not modified, not consulted and not
 * extended by this class; neither is the DAG scheduler, the task scheduler, the executor lifecycle,
 * the block manager storage contract, or `SortShuffleManager`, which stays byte for byte the
 * production-stable fallback. Everything new lives in this package.
 *
 * <b>The shape of the read.</b> `read()` performs, in order:
 *  - a rendezvous with the coordinator, which answers with the producers currently streaming the
 *    requested partition range together with the partition count and the protocol version they
 *    registered;
 *  - one channel per producer, opened through [[StreamingShuffleProducerConnector]] with a
 *    [[StreamingShuffleClientHandler]] attached, on a transport configuration scoped to the
 *    `shuffle-streaming` module so that `spark.shuffle-streaming.io.*` tunes streaming alone;
 *  - an in-progress block request per partition, expressed as the protocol's own heartbeat, which
 *    is what tells a producer which partitions this consumer wants and that it is alive;
 *  - a lazy iterator that yields records as blocks arrive. Nothing buffers a whole partition: a
 *    block becomes records, the records are consumed, the block is acknowledged, and the
 *    acknowledgement is what lets the producer reclaim the memory that block occupied.
 *
 * <b>Thread model, which is not negotiable.</b> Netty event-loop threads only ever <i>enqueue</i>,
 * inside [[StreamingShuffleClientHandler]]. This class runs on the task thread, and it is the task
 * thread that verifies checksums, enforces sequence integrity, updates counters, calls the metrics
 * reporters and throws. `ShuffleReadMetricsReporter` documents itself as single-threaded, so
 * reporting from an I/O thread would be a data race; and a `FetchFailedException` raised on an I/O
 * thread would never reach the scheduler. The one thing an I/O thread may hand across is a failure,
 * and it does so through [[StreamingShuffleErrorNotifier]]: first error wins, later ones are
 * suppressed onto it, and the task thread re-throws it. Without that bridge a channel failure would
 * be swallowed and the task would block for ever on input that will never arrive, which is why
 * `errorNotifier.throwIfError()` is called on every single iterator advance.
 *
 * <b>Cleanup.</b> The `ShuffleReader` trait exposes `read()` and nothing else -- its `stop()` is
 * commented out upstream -- so this class declares no `stop()`. Every resource it acquires is
 * released from a listener registered on `TaskContext.addTaskCompletionListener` at construction
 * time, which runs on success, on failure and on cancellation alike. That is strictly safer than a
 * `stop()` a caller may forget, and it is machine-checked: the test JVM runs with
 * `spark.unsafe.exceptionOnMemoryLeak=true`, so a retained buffer fails the suite outright.
 *
 * <b>Failure semantics.</b> Producer loss -- no data and no heartbeat for
 * [[StreamingShuffleClientHandler.PRODUCER_CONNECTION_TIMEOUT_MS]] -- causes every block already
 * accepted from that producer to be discarded together, atomically and per producer, so a reduce
 * task can never mix surviving pre-failure data with post-recomputation data. The invalidation is
 * counted on [[StreamingShuffleMetricsSource]], reported to the coordinator so the dead generation
 * is not handed out again, and then converted into a `FetchFailedException`. That exception is the
 * <i>entire</i> interface to stage recomputation: the framework turns it into a fetch-failure
 * reason and the unmodified DAG scheduler resubmits the upstream stage. It is always thrown as a
 * single expression, never stored in a variable, because its constructor already calls
 * `TaskContext.setFetchFailed` (SPARK-19276).
 *
 * <b>Corruption.</b> Every block's CRC32C is verified before any record from it becomes visible.
 * A mismatch inside the window this producer still retains -- that is, at a sequence number this
 * consumer has not yet acknowledged to it -- is repaired by asking for a replay of exactly that
 * block, with exponential backoff from one second over at most
 * [[BackpressureProtocol.MAX_RETRY_ATTEMPTS]] attempts. A mismatch outside that window cannot be
 * repaired, because the acknowledgement that moved the window is precisely what let the producer
 * free the bytes, so it escalates to `FetchFailedException` and correctness is recovered by
 * recomputation. Either way no corrupt record is ever handed to user code.
 *
 * <b>Determinism.</b> All timing goes through the injected [[Clock]] and through bounded numbers of
 * poll windows. There is no `Thread.sleep` and no direct `System.currentTimeMillis`, so the
 * behaviour of every timer in this class is reproducible under a manual clock.
 *
 * @param handle registration state for the shuffle being read, produced by `registerShuffle`
 * @param startMapIndex first map index to read, inclusive. Streaming serves whole map ranges only
 *                      (see [[StreamingShuffleReader.servesFullMapRange]]), because a live producer
 *                      location names a map id and a task attempt id but no map index
 * @param endMapIndex map index one past the last to read
 * @param startPartition first reduce partition to read, inclusive
 * @param endPartition reduce partition one past the last to read
 * @param context the task context of the reduce task performing the read
 * @param readMetrics reporter Spark's standard shuffle-read metrics are accumulated on. Populating
 *                    it is what makes streaming shuffles visible on the Web UI, the history server
 *                    and the REST API with no user-interface change whatsoever
 * @param conf the executor's configuration, read once here and held immutably
 * @param streamingContext the collaborators this reader shares with the rest of the subsystem
 * @param clock the clock every timer in this class reads, injected so tests need no sleeps
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
  extends ShuffleReader[K, C]
  with StreamingShuffleBufferUtilizationContributor
  with Logging {

  import StreamingShuffleReader._

  private val dep = handle.dependency
  private val shuffleId: Int = handle.shuffleId
  private val capabilityToken: String = handle.capabilityToken

  // Collaborators. They are shared executor-wide, which is why they arrive rather than being built:
  // the backpressure ledger has to see every concurrent shuffle in order to arbitrate between them,
  // and the fallback policy has to remember a trip across tasks for the kill switch to mean
  // anything.
  private val coordinatorRef: RpcEndpointRef = streamingContext.coordinatorRef
  private val backpressure: BackpressureProtocol = streamingContext.backpressure
  private val fallbackPolicy: StreamingShuffleFallbackPolicy = streamingContext.fallbackPolicy
  private val errorNotifier: StreamingShuffleErrorNotifier = streamingContext.errorNotifier
  private val connector: StreamingShuffleProducerConnector = streamingContext.connector
  private val serializerManager: SerializerManager = streamingContext.serializerManager

  // Configuration is read exactly once, here, and held immutably for the lifetime of the reader.
  // That is what makes "configuration changes require an executor restart" true by construction
  // and removes any need for a dynamic reconfiguration path, which is explicitly out of scope.
  private val streamingEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_ENABLED)
  private val bufferSizePercent: Int = conf.get(config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)
  private val spillThresholdPercent: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)
  private val maxBandwidthMbps: Option[Int] = conf.get(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)
  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)
  private val executorMemoryMib: Long = conf.get(config.EXECUTOR_MEMORY)

  /**
   * Aggregate consumer-side budget: the same `bufferSizePercent` of executor memory the producer
   * side is bounded by, so that the two halves of one shuffle are sized from one number rather than
   * from two that could drift apart.
   */
  private val bufferBudgetBytes: Long =
    math.max(DataBlockMessage.MAX_ENCODED_FRAME_BYTES.toLong,
      executorMemoryMib * BYTES_PER_MIB / PERCENT_SCALE * bufferSizePercent)

  /**
   * Per-partition allowance, following the specified `(executorMemory * bufferPercent) /
   * numPartitions`. Floored at one maximum-size frame so that a shuffle with very many partitions
   * still has room for the largest legal block, which is the smallest unit that can make progress.
   */
  private val perStreamCreditBytes: Long =
    math.max(DataBlockMessage.MAX_ENCODED_FRAME_BYTES.toLong,
      bufferBudgetBytes / math.max(1, handle.numPartitions))

  /**
   * The point at which this reader stops accepting blocks for partitions it is not currently
   * draining. Beyond it the reader consumes strictly head of line, which lets the client handler's
   * own high-water marks turn `autoRead` off and push the pressure back to the producer rather than
   * growing consumer heap. Expressed as `spillThreshold` of the credit for one stream, so the
   * consumer trips at the same utilisation the producer spills at.
   */
  private val stashHighWaterBytes: Long =
    math.max(DataBlockMessage.MAX_ENCODED_FRAME_BYTES.toLong,
      perStreamCreditBytes / PERCENT_SCALE * spillThresholdPercent)

  /** Partitions this reduce task is responsible for, ascending, so consumption order is stable. */
  private val partitionIds: Seq[Int] = startPartition until endPartition

  // Mutable state. Everything here is touched by the task thread; the lock exists solely because
  // the completion listener may run from a different thread than the one inside read() when a task
  // is cancelled, and a half-closed stream list would leak.
  private val streamsLock = new Object
  private val producerStreams = new mutable.ArrayBuffer[ProducerStream]
  private val registeredPartitions = new mutable.HashSet[Int]
  private val cleanedUp = new AtomicBoolean(false)

  /**
   * Released by cleanup so that no bounded wait in this reader can outlive the task. Only ever
   * awaited, never counted down for its own sake, which is what makes it a wait primitive that
   * reads no clock.
   */
  private val quiesce = new CountDownLatch(1)

  /**
   * Bytes this reader is holding in decoded-but-unconsumed blocks. Kept in an atomic rather than
   * under `streamsLock` because the metrics reporting thread reads it through
   * [[contributedBufferedBytes]] and must never block behind the task thread to do so.
   */
  private val stashedBytes = new AtomicLong(0L)

  /** Producer invalidations this reader has performed, mirrored onto the metrics source. */
  private var invalidations: Int = 0

  /** Blocks accepted and bytes accepted, for the throughput samples the fallback policy needs. */
  private var acceptedBlocks: Long = 0L
  private var acceptedBytes: Long = 0L

  // Tier two of the two-tier activation model. Tier one -- spark.shuffle.manager=streaming --
  // decides which manager class exists at all; this gate decides whether that manager streams or
  // delegates every call to the sort-based manager it holds internally. A reader must therefore
  // never be constructed while the gate is closed, and saying so here makes the model checkable
  // rather than merely documented.
  require(streamingEnabled,
    s"${config.SHUFFLE_STREAMING_ENABLED.key} is false, so no streaming shuffle reader may be " +
      "constructed. The streaming shuffle manager delegates to the sort-based manager instead.")

  require(servesFullMapRange(startMapIndex, endMapIndex),
    s"Streaming shuffle reads whole map ranges only, but [$startMapIndex, $endMapIndex) was " +
      s"requested for shuffle $shuffleId. A live producer location carries a map id and a task " +
      "attempt id but no map index, so a narrowed range cannot be honoured; the manager must " +
      "delegate such a read to the sort-based shuffle.")

  require(startPartition >= 0 && endPartition >= startPartition,
    s"Invalid reduce partition range [$startPartition, $endPartition) for shuffle $shuffleId.")

  // Registered before any channel is opened, so that a failure between here and the first block
  // still releases everything. Listeners run on success, failure and cancellation alike.
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
   * Bytes held in decoded-but-unconsumed blocks, answered without a lock so that the metrics
   * reporting thread never waits on the task thread.
   */
  override def contributedBufferedBytes: Long = math.max(0L, stashedBytes.get())

  /** The consumer-side budget those bytes are measured against. */
  override def contributedBudgetBytes: Long = bufferBudgetBytes

  /**
   * Reads this reduce task's partitions, streaming them from the producers that are still writing
   * them.
   *
   * The returned iterator is lazy and bounded: it holds at most the blocks the backpressure ledger
   * has granted credit for, and every block it releases is acknowledged so the producer can reclaim
   * the corresponding memory. Aggregation and ordering are applied exactly as the classic
   * block-store reader applies them, which is what makes the output of a streaming shuffle equal to
   * the output of the sort-based baseline record for record.
   *
   * @return the combined key-value pairs for this reduce task
   * @throws FetchFailedException if a producer is lost, announces an incompatible protocol, ends a
   *                             stream short, or delivers a block that cannot be repaired. The
   *                             unmodified DAG scheduler recomputes the upstream stage in response
   */
  override def read(): Iterator[Product2[K, C]] = {
    // A failure latched on an I/O thread before the task thread got here must surface now, not be
    // overwritten by whatever the first fetch attempt happens to hit.
    errorNotifier.throwIfError()

    // Makes this shuffle visible to the executor-wide ledger, which is what lets the protocol
    // arbitrate between concurrent shuffles and what the token bucket's refill rate is divided by.
    backpressure.registerShuffle(shuffleId, handle.numPartitions)
    registerStreams()
    StreamingShuffleMetricsSource.registerBufferUtilizationContributor(this)

    val locations = rendezvous()
    val streams = locations.map(openProducerStream)
    requestInProgressBlocks(streams)

    // Lazy from here down: one producer at a time, one partition at a time, one block at a time.
    val recordIter: Iterator[(Any, Any)] = streams.iterator.flatMap(stream => stream.records)

    // Standard Spark shuffle-read accounting, populated on the task thread. Without these calls the
    // Web UI, the history server and the REST API would report nothing for a streaming shuffle.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator is required for task cancellation to be observed promptly.
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val resultIter: Iterator[Product2[K, C]] = combineAndSort(interruptibleIter)

    errorNotifier.throwIfError()

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
   * Deliberately identical in structure to the classic block-store reader's own combine-and-sort
   * step. Any divergence here would make a streaming shuffle produce a different result set from
   * the sort-based baseline, which the failure-injection suites assert equality against.
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
   * Claims a credit allowance for each partition this task reads.
   *
   * Only partitions this call actually created are remembered, so cleanup releases exactly what it
   * claimed and leaves a concurrent reader of the same partition -- a speculative attempt, for
   * instance -- holding its own allowance untouched.
   */
  private def registerStreams(): Unit = {
    partitionIds.foreach { partitionId =>
      if (backpressure.registerStream(shuffleId, partitionId, perStreamCreditBytes)) {
        streamsLock.synchronized {
          registeredPartitions.add(partitionId)
        }
      }
    }
  }

  /**
   * Resolves the producers currently streaming this partition range.
   *
   * Reduce tasks in the classic path learn block locations from `MapOutputTracker`; a streaming
   * consumer needs live endpoints instead, and the coordinator is the only component that knows
   * them. An empty answer is not an error while a registration is still propagating, so the lookup
   * is retried over a bounded number of intervals; the whole wait is charged to fetch wait time, so
   * "fetch wait" keeps its usual meaning on this path. Exhausting the bound means no producer can
   * be reached, which is a fetch failure and is recovered by recomputing the upstream stage.
   *
   * A reply that names no producer at all is <b>not</b> a failure: a map stage with no tasks
   * legitimately produces an empty partition, and the read must then yield an empty iterator rather
   * than time out.
   */
  private def rendezvous(): Seq[StreamingShuffleProducerLocation] = {
    val waitStartMillis = clock.getTimeMillis()
    var attempts = 0
    var resolved: Option[StreamingShuffleProducerLocations] = None
    while (resolved.isEmpty && attempts < COORDINATOR_LOOKUP_MAX_ATTEMPTS && !cleanedUp.get()) {
      errorNotifier.throwIfError()
      resolved = lookupProducers()
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
        if (debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} resolved " +
            log"${MDC(COUNT, reply.locations.size)} live producer(s) for partitions " +
            log"[${MDC(PARTITION_ID, startPartition)}, ${MDC(VALUE, endPartition)}) after " +
            log"${MDC(MAX_ATTEMPTS, attempts + 1)} lookup attempt(s)")
        }
        reply.locations
      case None =>
        throw new FetchFailedException(null, shuffleId, UNKNOWN_MAP_ID, UNKNOWN_MAP_INDEX,
          startPartition,
          s"No live streaming shuffle producer was registered for shuffle $shuffleId partitions " +
            s"[$startPartition, $endPartition) after $attempts lookup attempt(s) spanning " +
            s"${attempts * COORDINATOR_LOOKUP_INTERVAL_MS} ms. Recomputing the upstream stage is " +
            "the recovery, because a producer that never registered cannot be streamed from.")
    }
  }

  /**
   * One coordinator lookup.
   *
   * The reply is matched on shape rather than cast, so a coordinator answering with something this
   * reader does not understand is reported as the typed condition it is instead of surfacing as a
   * class-cast exception from somewhere further down.
   */
  private def lookupProducers(): Option[StreamingShuffleProducerLocations] = {
    val request =
      LookupStreamingShuffleProducers(shuffleId, capabilityToken, startPartition, endPartition)
    coordinatorRef.askSync[Any](request) match {
      case Some(locations: StreamingShuffleProducerLocations) => Some(locations)
      case None => None
      case other =>
        throw StreamingShuffleErrors.unexpectedMessageType(
          s"Option[${classOf[StreamingShuffleProducerLocations].getSimpleName}]", describe(other))
    }
  }

  /**
   * Confirms that the producer generation just resolved partitions its output the way this consumer
   * expects and speaks a protocol this build understands.
   *
   * The version check is explicit -- `isCompatible` on the version the coordinator reports -- and
   * never inferred from a parse failure, which is what lets a mismatch trip the documented fallback
   * with the right reason attached. A trip routes to the same terminus as the operator kill switch:
   * the streaming manager delegates to the unmodified sort-based manager, so the retry that follows
   * the fetch failure raised here runs on the sort-based path and the job still completes.
   */
  private def validateRendezvous(reply: StreamingShuffleProducerLocations): Unit = {
    if (!StreamingShuffleMessage.isCompatible(reply.protocolVersion)) {
      fallbackPolicy.checkProtocolVersion(reply.protocolVersion)
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} producers announced " +
        log"protocol version ${MDC(PROTOCOL_VERSION, reply.protocolVersion)} but this build " +
        log"speaks " +
        log"${MDC(VALUE, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)}; falling back to the " +
        log"sort-based shuffle and recomputing the upstream stage")
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
      throw new FetchFailedException(null, shuffleId, UNKNOWN_MAP_ID, UNKNOWN_MAP_INDEX,
        startPartition,
        s"Streaming shuffle $shuffleId partition count disagreement: producers registered " +
          s"${reply.numPartitions} but this consumer expects ${handle.numPartitions}.")
    }
    // Recorded per partition because the ledger is keyed by stream, and it is the ledger that
    // reports a version disagreement to the fallback policy for streams already in flight.
    partitionIds.foreach { partitionId =>
      backpressure.observeProtocolVersion(shuffleId, partitionId, reply.protocolVersion)
    }
    if (reply.coordinatorEpoch != handle.coordinatorEpoch && debugEnabled) {
      logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} resolved producers at epoch " +
        log"${MDC(COUNT, reply.coordinatorEpoch)} against a handle registered at epoch " +
        log"${MDC(VALUE, handle.coordinatorEpoch)}")
    }
  }

  /**
   * Opens one channel to one producer and attaches a consumer handler to it.
   *
   * The handler is per producer, because it owns that producer's sequence expectation, its
   * acknowledgement position and its receive window. The channel is created through the injected
   * connector so that a test can exercise every path in this class without a socket.
   */
  private def openProducerStream(location: StreamingShuffleProducerLocation): ProducerStream = {
    val handler =
      new StreamingShuffleClientHandler(conf, shuffleId, backpressure, errorNotifier, clock)
    // Populated from a Netty thread the moment a frame arrives stamped with a version this build
    // cannot read. An int carries the byte so the sentinel cannot collide with a real version.
    val observedVersion = new AtomicInteger(NO_OBSERVED_VERSION)
    val connected = connector.connect(location, handler,
      version => observedVersion.compareAndSet(NO_OBSERVED_VERSION, version.toInt))
    connected match {
      case Some(channel) =>
        val stream = new ProducerStream(location, handler, channel, observedVersion)
        streamsLock.synchronized {
          producerStreams.append(stream)
        }
        if (debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} opened a channel to " +
            log"producer ${MDC(EXECUTOR_ID, location.executorId)} at " +
            log"${MDC(HOST_PORT, location.hostPort)} for map ${MDC(MAP_ID, location.mapId)}")
        }
        stream
      case None =>
        handler.close()
        invalidateProducer(location, StreamingShuffleInvalidationReason.ConnectionTimeout,
          "the consumer could not open a streaming channel")
        throw new FetchFailedException(blockManagerIdOf(location), shuffleId, location.mapId,
          UNKNOWN_MAP_INDEX, startPartition,
          s"Could not open a streaming shuffle channel to ${location.hostPort} for shuffle " +
            s"$shuffleId map ${location.mapId} within " +
            s"${StreamingShuffleClientHandler.PRODUCER_CONNECTION_TIMEOUT_MS} ms.")
    }
  }

  /**
   * Issues the in-progress block request for every partition on every producer.
   *
   * This is what makes the feature a streaming one: the request is sent while the map stage is
   * still producing, so reduce-side work overlaps map-side work instead of waiting behind a
   * materialisation barrier. The protocol's heartbeat carries it, which means one message type
   * serves both purposes -- it names the partition this consumer wants and it proves the consumer
   * is alive -- and the producer needs no extra request type to learn either.
   */
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
   *
   * The reader does not decide to fall back -- [[StreamingShuffleFallbackPolicy]] does -- so this
   * method reports and does not judge: producer and consumer throughput for the "consumer sustained
   * two times slower for more than sixty seconds" condition, observed link usage for the
   * "saturation above ninety percent" condition, and the consumer's own buffered bytes so the
   * executor-wide utilisation gauge and the spill decision see the whole picture rather than the
   * producer's half. When no bandwidth cap is administered the policy treats the utilisation sample
   * as unevaluable, which is exactly right: absence of a cap means unlimited, never zero.
   */
  private def sampleFlowControl(): Unit = {
    if (backpressure.isPollDue) {
      val throttled = backpressure.pollOnce()
      var producerRate = 0L
      var consumerRate = 0L
      partitionIds.foreach { partitionId =>
        producerRate += backpressure.producerRateBytesPerSecond(shuffleId, partitionId)
        consumerRate += backpressure.consumerRateBytesPerSecond(shuffleId, partitionId)
      }
      fallbackPolicy.recordProducerThroughput(shuffleId, producerRate.toDouble)
      fallbackPolicy.recordConsumerThroughput(shuffleId, consumerRate.toDouble)
      fallbackPolicy.recordLinkUtilization(producerRate.toDouble)
      backpressure.reportBufferUtilization(shuffleId, stashedBytes.get(), bufferBudgetBytes)

      val sustainedSlow = partitionIds.exists { partitionId =>
        backpressure.isConsumerSustainedSlow(shuffleId, partitionId)
      }
      if ((sustainedSlow || backpressure.isLinkSaturated) && !slownessReported) {
        slownessReported = true
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} is degrading: consumer " +
          log"rate ${MDC(NUM_BYTES, consumerRate)} B/s against producer rate " +
          log"${MDC(VALUE, producerRate)} B/s at " +
          log"${MDC(THRESHOLD, backpressure.linkSaturationPercent)}% link saturation with " +
          log"${MDC(COUNT, throttled)} throttled stream(s). The fallback policy decides whether " +
          log"to yield to the sort-based shuffle")
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

  /**
   * Tells the coordinator that a producer generation must not be handed to another consumer.
   *
   * Best effort on purpose. The read is failing regardless, and a coordinator that cannot be
   * reached must not replace the fetch failure about to be raised -- which the scheduler knows how
   * to recover from -- with an RPC error it does not.
   */
  private def invalidateProducer(
      location: StreamingShuffleProducerLocation,
      reason: StreamingShuffleInvalidationReason,
      detail: String): Unit = {
    try {
      val epoch = coordinatorRef.askSync[Any](InvalidateStreamingShuffleProducer(
        shuffleId, capabilityToken, location.generation, reason, detail))
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

  /**
   * Counts one partial-read invalidation on the executor's metrics and reports it upstream.
   *
   * Exactly one increment per invalidated producer, which is what makes
   * `shuffle.streaming.partialReadInvalidations` readable as "how many times a consumer threw away
   * what it had already accepted and asked for the upstream stage again".
   */
  private def recordInvalidation(
      location: StreamingShuffleProducerLocation,
      reason: StreamingShuffleInvalidationReason,
      detail: String): Unit = {
    invalidations += 1
    StreamingShuffleMetricsSource.incrementPartialReadInvalidations(1L)
    invalidateProducer(location, reason, detail)
  }

  /**
   * The block manager identity of a producer, for the fetch-failure report.
   *
   * `FetchFailedException` documents that this may be null, and `MetadataFetchFailedException`
   * passes null itself; this reader has the identity whenever it resolved a location, so it passes
   * it, and passes null only where no producer could be named at all.
   */
  private def blockManagerIdOf(location: StreamingShuffleProducerLocation): BlockManagerId = {
    BlockManagerId(location.executorId, location.host, location.port, topologyInfo = None)
  }

  /** Whether a partition belongs to this reduce task's range. */
  private def servesPartition(partitionId: Int): Boolean = {
    partitionId >= startPartition && partitionId < endPartition
  }

  /**
   * A bounded wait that reads no clock and never calls `Thread.sleep`.
   *
   * The latch is never counted down for its own sake, so the timeout is the wait; cleanup counts it
   * down, which is what stops a cancelled task from sitting out the remainder of an interval.
   */
  private def awaitQuietly(millis: Long): Unit = {
    if (millis > 0L) {
      quiesce.await(millis, TimeUnit.MILLISECONDS)
    }
  }

  /**
   * Releases everything this reader acquired.
   *
   * Registered on `TaskContext.addTaskCompletionListener` at construction, so it runs on success,
   * on failure and on cancellation alike, and it is idempotent by compare-and-set because a
   * listener may run alongside a stream that is closing for its own reasons. The connector is
   * deliberately not closed here: its event loop is executor-scoped and owned by
   * `StreamingShuffleManager`, which closes it in `stop()`. Neither is the shuffle unregistered
   * from the backpressure ledger, because another task on this executor may still be reading it;
   * that too belongs to the manager.
   */
  private def releaseResources(): Unit = {
    if (cleanedUp.compareAndSet(false, true)) {
      // Frees any bounded wait in flight before anything else, so cleanup cannot be delayed by one.
      quiesce.countDown()
      StreamingShuffleMetricsSource.unregisterBufferUtilizationContributor(this)
      val (streams, partitions) = streamsLock.synchronized {
        val streamSnapshot = producerStreams.toSeq
        val partitionSnapshot = registeredPartitions.toSeq
        producerStreams.clear()
        registeredPartitions.clear()
        (streamSnapshot, partitionSnapshot)
      }
      var released = 0L
      streams.foreach { stream =>
        released += stream.close()
      }
      partitions.foreach { partitionId =>
        backpressure.unregisterStream(shuffleId, partitionId)
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


  /**
   * Consumer-side state for one partition of one producer.
   *
   * Touched only by the task thread. `bufferedBytes` mirrors the sum of the queued payload lengths
   * so that the executor-wide utilisation gauge can be answered without walking the queue.
   */
  private final class PartitionInbox(val partitionId: Int) {

    /** Verified-on-dequeue blocks waiting to be turned into records, in arrival order. */
    val blocks = new mutable.Queue[DataBlockMessage]

    /** Payload bytes currently queued here. */
    var bufferedBytes: Long = 0L

    /** The sequence number the next block for this partition must carry. */
    var nextSequenceNumber: Long = FIRST_SEQUENCE_NUMBER

    /** Blocks handed on to the record stream so far. */
    var consumedBlocks: Long = 0L

    /** Whether the producer has announced the orderly end of this partition's stream. */
    var terminated: Boolean = false

    /** Blocks the producer says it sent, or [[StreamingShuffleClientHandler.NO_BLOCK_TOTAL]]. */
    var announcedTotalBlocks: Long = StreamingShuffleClientHandler.NO_BLOCK_TOTAL

    /** When this reader first started waiting on this partition, read from the injected clock. */
    var awaitStartedMillis: Long = NO_TIMESTAMP
  }

  /**
   * One producer's worth of consumer-side state: the channel, the handler that owns it, the
   * per-partition inboxes, and the deserialization streams opened over them.
   *
   * <b>Why the state is per producer.</b> A channel carries every partition this consumer asked
   * that producer for, tagged by the partition id in each frame's header, so the reader
   * demultiplexes on the task thread. Sequence expectation, acknowledgement position and receive
   * window all belong to the producer, not to the shuffle, which is why one handler serves one
   * channel and no more.
   *
   * <b>Head-of-line consumption is deliberate.</b> Partitions are drained in ascending order, so
   * blocks for a partition that is not being drained wait in its inbox. That waiting is bounded
   * twice: by the credit the ledger grants each stream, and by the handler's own queue high-water
   * marks, which close the receive window and push the pressure back to the producer rather than
   * growing consumer heap without limit.
   */
  private final class ProducerStream(
      val location: StreamingShuffleProducerLocation,
      val handler: StreamingShuffleClientHandler,
      val channel: Channel,
      observedVersion: AtomicInteger) {

    /** Whether this producer is this very executor, which decides local or remote accounting. */
    private val producerIsLocal: Boolean =
      Option(SparkEnv.get).map(_.executorId).contains(location.executorId)

    private val inboxes = new mutable.HashMap[Int, PartitionInbox]
    private val openStreams = new mutable.ArrayBuffer[DeserializationStream]
    private val closed = new AtomicBoolean(false)

    /**
     * The records this producer contributes, partition by partition in ascending order.
     *
     * Lazy throughout: no channel is read until the caller asks for a record, and no more than one
     * block per partition is decoded at a time.
     */
    def records: Iterator[(Any, Any)] = {
      partitionIds.iterator.flatMap(partitionId => new PartitionRecordIterator(this, partitionId))
    }

    /** The inbox for one partition, created on first use. */
    def inboxOf(partitionId: Int): PartitionInbox = {
      inboxes.getOrElseUpdate(partitionId, new PartitionInbox(partitionId))
    }

    /**
     * Opens the record stream for one partition over the given payload stream.
     *
     * The payload is wrapped exactly as the classic path wraps a fetched shuffle block --
     * `SerializerManager.wrapStream` for the same `ShuffleBlockId` -- so compression and encryption
     * are applied identically on both paths. That is the contract the streaming writer honours: a
     * partition's payload bytes, concatenated in sequence order across the blocks of one producer,
     * are exactly the bytes that wrapping produces. Wrapping once per (producer, partition) rather
     * than once per block is what makes the concatenation legal, since a codec's framing spans
     * blocks.
     */
    def openRecords(partitionId: Int, payload: InputStream): Iterator[(Any, Any)] = {
      val blockId = ShuffleBlockId(shuffleId, location.mapId, partitionId)
      val wrapped = serializerManager.wrapStream(blockId, payload)
      val deserializationStream = serializerInstance.deserializeStream(wrapped)
      openStreams.append(deserializationStream)
      deserializationStream.asKeyValueIterator
    }

    /**
     * The next verified block for one partition, or `None` at the orderly end of its stream.
     *
     * Blocks until a block arrives, the stream ends, or a failure is detected. Every exit other
     * than a block is either an empty answer for a completed stream or a thrown failure; there is
     * no path on which this returns without the caller knowing which of the two happened.
     */
    def nextBlock(partitionId: Int): Option[DataBlockMessage] = {
      val inbox = inboxOf(partitionId)
      if (inbox.awaitStartedMillis == NO_TIMESTAMP) {
        inbox.awaitStartedMillis = clock.getTimeMillis()
      }
      var result: Option[DataBlockMessage] = None
      var finished = false
      while (!finished) {
        errorNotifier.throwIfError()
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

    /**
     * One poll window: renew the in-progress request, take at most one inbound event, and charge
     * the wait to fetch wait time so "fetch wait" keeps its usual meaning on this path.
     *
     * Everything here runs on the task thread. The Netty threads behind the handler only enqueue.
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
        case Some(ProducerLost(lostPartition, reason)) =>
          val attributedPartition =
            if (servesPartition(lostPartition)) lostPartition else awaitedPartition
          failProducer(attributedPartition, StreamingShuffleInvalidationReason.ConnectionTimeout,
            reason)
        case None =>
          checkProducerLiveness(awaitedPartition)
      }
      sampleFlowControl()
    }

    /**
     * Accepts one arrived block into its inbox, or discards it if it is not ours.
     *
     * A block for a partition outside this task's range is acknowledged before being dropped, so
     * the producer reclaims it rather than retaining it for a consumer that will never read it.
     * That is a routing quirk, not a protocol violation, so it is never reported as an unexpected
     * message type.
     *
     * Byte and block counts are recorded here, on arrival, because that is when the bytes crossed
     * the network. Locality is decided from the producer's executor id, so a producer that happens
     * to be this executor is accounted local exactly as the classic path would account it.
     */
    private def enqueue(block: DataBlockMessage): Unit = {
      val partitionId = block.partitionId()
      val payloadBytes = block.payloadLength().toLong
      if (!servesPartition(partitionId)) {
        handler.acknowledge(partitionId, block.sequenceNumber())
        if (debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} discarded a block for " +
            log"partition ${MDC(PARTITION_ID, partitionId)} outside the range this task reads")
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

    /**
     * Records the orderly end of one partition's stream.
     *
     * A total of zero is a valid reading and means the partition was empty; the read must then
     * yield an empty iterator rather than wait out the connection timeout, which is exactly what an
     * empty inbox plus a terminated flag produces.
     */
    private def markTerminated(completedPartition: Int, totalBlocks: Long): Unit = {
      if (servesPartition(completedPartition)) {
        val inbox = inboxOf(completedPartition)
        inbox.terminated = true
        inbox.announcedTotalBlocks = totalBlocks
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
     */
    private def checkProducerLiveness(awaitedPartition: Int): Unit = {
      errorNotifier.throwIfError()
      if (handler.isProducerLost) {
        failProducer(awaitedPartition, StreamingShuffleInvalidationReason.ConnectionTimeout,
          s"the producer channel became inactive " +
            s"${handler.producerLostElapsedMillis.getOrElse(0L)} ms ago")
      } else if (handler.isProducerSilent(awaitedPartition)) {
        failProducer(awaitedPartition, StreamingShuffleInvalidationReason.ConnectionTimeout,
          s"partition $awaitedPartition received nothing for " +
            s"${handler.millisSinceInbound(awaitedPartition).getOrElse(0L)} ms, past the " +
            s"${StreamingShuffleClientHandler.PRODUCER_CONNECTION_TIMEOUT_MS} ms connection " +
            "timeout")
      } else {
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
              s"${StreamingShuffleClientHandler.PRODUCER_CONNECTION_TIMEOUT_MS} ms connection " +
              "timeout")
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

    /**
     * Takes the next block off an inbox and makes it fit to be read.
     *
     * Two gates stand between a queued block and a visible record, and both are applied here, on
     * the task thread, before any byte of the block is handed on: the sequence number must be
     * exactly the one expected, and the CRC32C must match. Neither can be deferred, because a
     * record that has already been handed to user code cannot be recalled.
     */
    private def acceptNext(inbox: PartitionInbox): DataBlockMessage = {
      val block = dequeueHead(inbox)
      val expected = inbox.nextSequenceNumber
      val actual = block.sequenceNumber()
      if (actual != expected) {
        // A gap or a reordering means the stream this consumer is reading is not the stream the
        // producer sent. Everything already accepted from this producer goes, and the typed
        // condition is raised rather than a generic failure, so the diagnostic names both
        // positions.
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
      val block = inbox.blocks.dequeue()
      val payloadBytes = block.payloadLength().toLong
      inbox.bufferedBytes = math.max(0L, inbox.bufferedBytes - payloadBytes)
      stashedBytes.addAndGet(-payloadBytes)
      block
    }

    /**
     * Verifies a block's CRC32C and, when it fails, repairs it if repair is still possible.
     *
     * <b>The window.</b> A producer frees the bytes of a block when the consumer acknowledges it,
     * so a block can only be replayed while it is still unacknowledged to <i>this</i> producer. The
     * handler's acknowledgement position for the partition is therefore the authoritative lower
     * bound of the repairable window; the protocol ledger's own view is consulted too, but it
     * aggregates every producer of the partition and so is the coarser of the two.
     *
     * <b>Inside the window</b> a replay of exactly that one block is requested -- an inclusive
     * single-element window -- and awaited over a bounded number of poll windows derived from the
     * protocol's exponential backoff, which starts at one second and allows at most
     * [[BackpressureProtocol.MAX_RETRY_ATTEMPTS]] attempts. Nothing sleeps: the wait is spent
     * polling for the replay, so a replay that arrives early is consumed immediately.
     *
     * <b>Outside the window, or once the attempts are spent</b>, the bytes are gone and no message
     * can bring them back, so the failure escalates to a `FetchFailedException` carrying the typed
     * checksum condition as its cause. Stage recomputation then restores correctness. This is the
     * resolution of the tension between "retransmit on corruption" and "reclaim on
     * acknowledgement": repair inside the window, recompute outside it, and no corrupt record is
     * ever visible either way.
     *
     * One honest note on how often the repair path fires. The client handler verifies a block as it
     * arrives and refuses to enqueue a corrupt one, requesting the replay itself; a mismatch
     * observed here is therefore corruption that happened after that check, and the producer's
     * replay of an already-accepted position is suppressed by the handler as a duplicate. The path
     * is retained because the protocol mandates it and because it is exactly right for a mismatch
     * on a block the handler never accepted, and it terminates in recomputation rather than in a
     * silent hang.
     */
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
            candidate.partitionId(), sequenceNumber, candidate.copyPayload())
          val blockName = ShuffleBlockId(shuffleId, location.mapId, inbox.partitionId).name
          val retainedByProducer = sequenceNumber > handler.acknowledgedPosition(inbox.partitionId)
          val withinLedgerWindow = backpressure.isWithinUnacknowledgedWindow(
            shuffleId, inbox.partitionId, sequenceNumber)
          logWarning(log"Streaming shuffle block ${MDC(BLOCK_ID, blockName)} failed CRC32C " +
            log"verification: the producer sent ${MDC(CHECKSUM, candidate.checksum())} and this " +
            log"consumer computed ${MDC(VALUE, computed)}. Retained by the producer: " +
            log"${MDC(REASON, retainedByProducer)}, ledger window: " +
            log"${MDC(THRESHOLD, withinLedgerWindow)}")
          attempt += 1
          val repairable = retainedByProducer &&
            attempt <= BackpressureProtocol.MAX_RETRY_ATTEMPTS &&
            !backpressure.isRetryExhausted(shuffleId, inbox.partitionId)
          if (repairable) {
            // The candidate is replaced only if a replay actually arrives. Keeping the corrupt one
            // otherwise sends the loop round to spend another attempt, or to escalate once they are
            // spent, so there is no path on which a corrupt block becomes records.
            requestReplay(inbox, sequenceNumber, attempt, computed, blockName).foreach { replayed =>
              candidate = replayed
            }
          } else {
            discardAll()
            recordInvalidation(location, StreamingShuffleInvalidationReason.ChecksumMismatch,
              s"block $blockName failed verification and could not be replayed")
            throw new FetchFailedException(blockManagerIdOf(location), shuffleId, location.mapId,
              UNKNOWN_MAP_INDEX, inbox.partitionId,
              s"Streaming shuffle block $blockName failed CRC32C verification after $attempt " +
                s"attempt(s) and cannot be replayed, so the upstream stage must be recomputed.",
              StreamingShuffleErrors.checksumVerificationFailed(
                blockName, shuffleId, candidate.checksum(), computed))
          }
        }
      }
      verified.get
    }

    /**
     * Asks for one block to be sent again and waits for it, without sleeping.
     *
     * The wait is a bounded number of poll windows whose count is derived from the protocol's own
     * backoff for the stream, falling back to the standard exponential schedule from one second
     * when the protocol has no opinion. Expressing the delay as a count of poll windows rather than
     * as a clock deadline is what keeps the behaviour identical under a manual clock, which is what
     * makes the reader suites deterministic without a single sleep.
     */
    private def requestReplay(
        inbox: PartitionInbox,
        sequenceNumber: Long,
        attempt: Int,
        computed: Long,
        blockName: String): Option[DataBlockMessage] = {
      val requested =
        handler.requestRetransmission(inbox.partitionId, sequenceNumber, sequenceNumber)
      val backoffMillis = backpressure.nextRetryBackoffMillis(shuffleId, inbox.partitionId)
        .getOrElse(BackpressureProtocol.retryBackoffMillis(attempt))
      logWarning(log"Streaming shuffle requested a replay of block ${MDC(BLOCK_ID, blockName)} " +
        log"(attempt ${MDC(MAX_ATTEMPTS, attempt)} of " +
        log"${MDC(COUNT, BackpressureProtocol.MAX_RETRY_ATTEMPTS)}, accepted: " +
        log"${MDC(REASON, requested)}, backoff ${MDC(TIMEOUT, backoffMillis)} ms)")
      var windows = math.max(1L, backoffMillis / POLL_WINDOW_MS)
      var replayed: Option[DataBlockMessage] = None
      while (replayed.isEmpty && windows > 0L && !cleanedUp.get()) {
        pumpOnce(inbox.partitionId)
        if (inbox.blocks.nonEmpty && inbox.blocks.head.sequenceNumber() == sequenceNumber) {
          replayed = Some(dequeueHead(inbox))
        }
        windows -= 1L
      }
      if (replayed.isEmpty && debugEnabled) {
        logDebug(log"Streaming shuffle saw no replay of block ${MDC(BLOCK_ID, blockName)} within " +
          log"${MDC(TIMEOUT, backoffMillis)} ms; the computed checksum was " +
          log"${MDC(CHECKSUM, computed)}")
      }
      replayed
    }

    /**
     * Confirms that a terminated stream delivered everything it announced.
     *
     * A stream that ends short is the one failure a checksum cannot catch, because every block that
     * did arrive was intact. Comparing the announced total against what was consumed is what turns
     * a truncated partition into a fetch failure instead of a silently short result.
     */
    private def verifyStreamComplete(inbox: PartitionInbox): Unit = {
      val announced = handler.announcedBlockCount(inbox.partitionId)
        .orElse(backpressure.announcedBlockCount(shuffleId, inbox.partitionId))
        .getOrElse(inbox.announcedTotalBlocks)
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
        throw new FetchFailedException(blockManagerIdOf(location), shuffleId, location.mapId,
          UNKNOWN_MAP_INDEX, inbox.partitionId,
          s"Streaming shuffle $shuffleId partition ${inbox.partitionId} ended after " +
            s"${inbox.consumedBlocks} block(s) but $announced were announced.")
      } else if (debugEnabled) {
        logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
          log"${MDC(PARTITION_ID, inbox.partitionId)} completed with " +
          log"${MDC(NUM_BLOCKS, inbox.consumedBlocks)} block(s) consumed")
      }
    }

    /**
     * Acknowledges consumption up to and including one block.
     *
     * This is the moment the producer may free that block's memory -- the protocol requires the
     * reclamation to complete within a hundred milliseconds of it -- and it is also what re-opens
     * the receive window this consumer closed by falling behind. Acknowledging on consumption
     * rather than on arrival is the whole point: it is the reduce task's progress that releases
     * producer memory, so a slow consumer throttles a fast producer instead of drowning in its
     * output.
     */
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

    /** The incompatible protocol version a Netty thread saw on this channel, if any. */
    private def incompatibleVersion: Option[Byte] = {
      val observed = observedVersion.get()
      if (observed == NO_OBSERVED_VERSION) None else Some(observed.toByte)
    }

    /**
     * Discards, atomically and in one step, every block this consumer has accepted from this
     * producer, then closes the channel that could deliver more.
     *
     * Atomic and per producer is the requirement, and it is what stops a reduce task from ever
     * mixing data that survived a failure with data produced by the recomputation that follows it.
     * The channel is closed as part of the discard, because a block arriving after the discard
     * would be exactly the mixture the discard exists to prevent.
     *
     * @return the payload bytes released
     */
    def discardAll(): Long = synchronized {
      val released = discardBuffered()
      closeOpenStreams()
      handler.close()
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
     * The `FetchFailedException` is constructed and thrown as a single expression, never assigned:
     * its constructor calls `TaskContext.setFetchFailed`, so creating one and then deciding not to
     * throw it would poison the task (SPARK-19276). An explicitly detected protocol version
     * mismatch is preferred as the cause when one was seen, because a version disagreement must
     * trip the fallback policy rather than be reported as a timeout.
     */
    def failProducer(
        partitionId: Int,
        reason: StreamingShuffleInvalidationReason,
        detail: String): Nothing = {
      val versionCause = incompatibleVersion
      val released = discardAll()
      val attributedReason = if (versionCause.isDefined) {
        StreamingShuffleInvalidationReason.ProtocolViolation
      } else {
        reason
      }
      recordInvalidation(location, attributedReason, detail)
      versionCause match {
        case Some(version) =>
          fallbackPolicy.checkProtocolVersion(version)
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} saw protocol version " +
            log"${MDC(PROTOCOL_VERSION, version)} from ${MDC(HOST_PORT, location.hostPort)}, " +
            log"which this build cannot read; discarded ${MDC(NUM_BYTES, released)} byte(s) and " +
            log"fell back to the sort-based shuffle")
          throw new FetchFailedException(blockManagerIdOf(location), shuffleId, location.mapId,
            UNKNOWN_MAP_INDEX, partitionId,
            s"Streaming shuffle producer ${location.hostPort} announced protocol version " +
              s"$version but this build speaks " +
              s"${StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION}; $detail.")
        case None =>
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} invalidated partial " +
            log"reads of partition ${MDC(PARTITION_ID, partitionId)} from " +
            log"${MDC(HOST_PORT, location.hostPort)}: ${MDC(REASON, detail)}. Discarded " +
            log"${MDC(NUM_BYTES, released)} byte(s); the upstream stage will be recomputed")
          throw new FetchFailedException(blockManagerIdOf(location), shuffleId, location.mapId,
            UNKNOWN_MAP_INDEX, partitionId,
            s"Streaming shuffle producer ${location.hostPort} for shuffle $shuffleId map " +
              s"${location.mapId} was lost: $detail.")
      }
    }

    /**
     * Releases this producer's channel, handler and buffers exactly once.
     *
     * Reached from the task-completion listener, so it runs on success, on failure and on
     * cancellation alike. `handler.close()` drains its queue and closes the channel; the channel is
     * closed again here only because doing so is idempotent and because a handler that never saw a
     * channel active would otherwise leave the socket to the garbage collector.
     *
     * @return the payload bytes released
     */
    def close(): Long = {
      if (closed.compareAndSet(false, true)) {
        val released = discardAll()
        if (channel.isOpen()) {
          channel.close()
        }
        released
      } else {
        0L
      }
    }
  }


  /**
   * The payload of one partition of one producer, presented to the deserializer as a single
   * `InputStream`.
   *
   * <b>Why a stream and not a block iterator.</b> A record may straddle a block boundary -- blocks
   * are capped at two megabytes and records are not -- and a compression codec's framing spans
   * blocks too. Concatenating the payloads in sequence order behind one stream is therefore the
   * only presentation the deserializer can consume, and it is exactly the presentation the classic
   * path gives it for a fetched block.
   *
   * <b>Where the pipelining lives.</b> `read` pulls the next block only when the deserializer has
   * consumed the previous one, so the reduce task's own progress is what draws data across the
   * network. That is the mechanism by which reduce-side work overlaps map-side work.
   *
   * <b>Where the acknowledgement lives.</b> A block is acknowledged the moment its last byte has
   * been handed over, never when it arrives. Acknowledging on consumption is what makes the flow
   * control real: it releases the producer's memory for that block and re-opens the receive window
   * this consumer closed by falling behind.
   */
  private final class BlockPayloadStream(stream: ProducerStream, partitionId: Int)
    extends InputStream {

    /** A read-only view of the block being consumed, or null between blocks. */
    private var current: ByteBuffer = null

    /** Sequence number of the block being consumed, which is what its acknowledgement names. */
    private var currentSequenceNumber: Long = NO_SEQUENCE_NUMBER

    /** Set once the partition's stream has ended and every block has been consumed. */
    private var exhausted: Boolean = false

    /**
     * Whether this partition has any bytes at all, blocking until that is known.
     *
     * Exists so that an empty partition never reaches the deserializer: some serializers read a
     * stream header eagerly and would fail on empty input, which is why the classic path skips
     * zero-length blocks rather than opening a stream over them. An orderly end of stream
     * announcing zero blocks therefore yields an empty iterator here, not a failure and not a
     * timeout.
     */
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

    /**
     * Releases the block in hand and refuses to pull another.
     *
     * Reached both when the deserializer finishes -- Spark's `asKeyValueIterator` closes its stream
     * on exhaustion -- and from the reader's own cleanup, so it must be safe twice.
     */
    override def close(): Unit = {
      releaseCurrent()
      exhausted = true
    }

    /**
     * Ensures a byte is available to be read, pulling blocks until one is or the stream ends.
     *
     * A zero-length block is not a special case: the loop acknowledges it and moves on, which is
     * what makes an empty block harmless rather than a source of a spurious end of stream.
     */
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

  /**
   * The records of one partition of one producer.
   *
   * Two responsibilities, and no others. First, it defers opening the deserialization stream until
   * a record is actually asked for, and skips opening one at all for a partition that turns out to
   * be empty. Second, it calls `errorNotifier.throwIfError()` on <i>every</i> advance, which is the
   * mechanism that converts a failure latched on a Netty thread into a thrown exception on the task
   * thread. Without that call a channel-level failure would leave this iterator waiting for input
   * that can never arrive, and the task would hang rather than fail -- the worst available outcome.
   */
  private final class PartitionRecordIterator(stream: ProducerStream, partitionId: Int)
    extends Iterator[(Any, Any)] {

    private var initialized: Boolean = false
    private var delegate: Iterator[(Any, Any)] = Iterator.empty

    override def hasNext: Boolean = {
      errorNotifier.throwIfError()
      initialize()
      delegate.hasNext
    }

    override def next(): (Any, Any) = {
      errorNotifier.throwIfError()
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

/**
 * Constants and predicates shared by [[StreamingShuffleReader]] and its collaborators.
 *
 * Every wire-derived value here is computed from the protocol's own public constants rather than
 * written out as a number, so that a change upstream cannot leave a stale literal behind. The
 * arithmetic is checked at class initialisation against `FRAMING_OVERHEAD_BYTES`, which is the one
 * figure the protocol publishes that depends on all of the others.
 */
private[spark] object StreamingShuffleReader {

  /** Denominator for every percentage this subsystem is configured with. */
  val PERCENT_SCALE: Long = 100L

  /** Bytes in a mebibyte, the unit executor memory is configured in. */
  val BYTES_PER_MIB: Long = 1024L * 1024L

  /**
   * How long one blocking poll of the consumer queue waits.
   *
   * Deliberately the protocol's own poll interval, so that every timer the reader evaluates -- the
   * five-second connection timeout, the heartbeat interval, the hundred-millisecond reclamation
   * deadline -- is examined on the cadence the protocol expects and not on a second one.
   */
  val POLL_WINDOW_MS: Long = BackpressureProtocol.POLL_INTERVAL_MS

  /** How long the reader waits between coordinator lookups while a registration propagates. */
  val COORDINATOR_LOOKUP_INTERVAL_MS: Long = POLL_WINDOW_MS

  /**
   * How many lookups the reader will attempt before declaring the producers unreachable.
   *
   * Bounded by the consumer liveness timeout, so a producer that never registers is reported as a
   * fetch failure on the same time scale as one that registers and then goes quiet.
   */
  val COORDINATOR_LOOKUP_MAX_ATTEMPTS: Int = math.max(1,
    (BackpressureProtocol.CONSUMER_LIVENESS_TIMEOUT_MS / COORDINATOR_LOOKUP_INTERVAL_MS).toInt)

  /** Sequence numbers count from zero, one per data block, per partition stream. */
  val FIRST_SEQUENCE_NUMBER: Long = 0L

  /** Sentinel for "no block in hand", distinct from every legal sequence number. */
  val NO_SEQUENCE_NUMBER: Long = -1L

  /** Sentinel for "not yet measured", distinct from every legal clock reading. */
  val NO_TIMESTAMP: Long = -1L

  /** Sentinel for "no incompatible version seen", outside the range of a byte. */
  val NO_OBSERVED_VERSION: Int = Int.MinValue

  /** What `FetchFailedException` is given when no producer can be named, as its own doc allows. */
  val UNKNOWN_MAP_ID: Long = -1L

  /** What `FetchFailedException` is given for a map index a producer location cannot supply. */
  val UNKNOWN_MAP_INDEX: Int = -1

  /** `InputStream` end-of-stream marker. */
  val END_OF_STREAM: Int = -1

  /** Mask that promotes a signed byte to the unsigned value `InputStream.read` must return. */
  val UNSIGNED_BYTE_MASK: Int = 0xFF

  /**
   * Framed length of every fixed-size control message.
   *
   * Derived, not asserted: the type prefix, the shared header, and the single long each of
   * acknowledgement, heartbeat, retransmit request and stream termination carries as its body.
   * Those four are indistinguishable by length -- which is exactly why this reader discriminates on
   * the type byte and never on a length.
   */
  val CONTROL_FRAME_LENGTH: Int = StreamingShuffleMessage.FRAME_TYPE_PREFIX_LENGTH +
    StreamingShuffleMessage.HEADER_ENCODED_LENGTH + java.lang.Long.BYTES

  /**
   * Offset, within a framed data block, of the big-endian int that gives the payload length.
   *
   * The type prefix, then the shared header, then the block's CRC32C, which is a long.
   */
  val PAYLOAD_LENGTH_FRAME_OFFSET: Int = StreamingShuffleMessage.FRAME_TYPE_PREFIX_LENGTH +
    StreamingShuffleMessage.HEADER_ENCODED_LENGTH + java.lang.Long.BYTES

  /** Fewest bytes that must be buffered before the type byte and the version can be read. */
  val MIN_FRAME_PEEK_LENGTH: Int = StreamingShuffleMessage.FRAME_TYPE_PREFIX_LENGTH + 1

  /** How long the connector's event loop is given to wind down once it is closed. */
  val EVENT_LOOP_SHUTDOWN_TIMEOUT_MS: Long = 1000L

  /** Pipeline name of the frame decoder, so a channel's pipeline reads the same at every site. */
  val FRAME_DECODER_HANDLER_NAME: String = "streamingShuffleFrameDecoder"

  /** Pipeline name of the consumer handler, which sits immediately after the decoder. */
  val CONSUMER_HANDLER_NAME: String = "streamingShuffleConsumer"

  // The one place the derivations above can be checked against a figure the protocol publishes: a
  // data block's framing overhead is everything before the payload, which is the payload length
  // offset plus the width of the length prefix itself. If the protocol's framing ever changes, this
  // fails at class initialisation instead of mis-framing a stream at run time.
  require(PAYLOAD_LENGTH_FRAME_OFFSET + java.lang.Integer.BYTES ==
      DataBlockMessage.FRAMING_OVERHEAD_BYTES,
    s"The streaming shuffle frame layout changed: a payload length at offset " +
      s"$PAYLOAD_LENGTH_FRAME_OFFSET plus ${java.lang.Integer.BYTES} byte(s) does not " +
      s"account for the ${DataBlockMessage.FRAMING_OVERHEAD_BYTES} byte(s) of framing " +
      "overhead the protocol reports.")

  /**
   * Whether a map range can be served by streaming.
   *
   * A live producer location names a map id and a task attempt id, but not a map index, so a
   * narrowed range -- as adaptive execution produces when it coalesces or splits -- cannot be
   * honoured without inventing an index mapping that the coordinator does not have. Streaming
   * therefore serves whole ranges only, and the manager delegates anything narrower to the
   * sort-based reader, which is a fallback and not a failure.
   *
   * @param startMapIndex first map index requested, inclusive
   * @param endMapIndex map index one past the last requested
   * @return true when the request covers every map output of the shuffle
   */
  def servesFullMapRange(startMapIndex: Int, endMapIndex: Int): Boolean = {
    startMapIndex == 0 && endMapIndex == Int.MaxValue
  }
}

/**
 * The collaborators a [[StreamingShuffleReader]] shares with the rest of the streaming subsystem.
 *
 * They travel together in one value for a concrete reason: the reader already takes the seven
 * arguments the `ShuffleManager` service-provider interface hands it, plus its configuration and
 * its clock, and scalastyle caps a parameter list -- constructors included -- at ten. Bundling the
 * shared collaborators keeps the reader inside that limit without hiding anything, and it gives
 * `StreamingShuffleManager` a single value to build once per executor and hand to every reader.
 *
 * Every member is shared executor-wide and outlives any one reader, which is why none of them is
 * closed by the reader's task-completion listener.
 *
 * @param coordinatorRef reference to the driver's [[StreamingShuffleCoordinator]] endpoint, through
 *                       which a consumer resolves live producer endpoints. This is the streaming
 *                       counterpart of the classic path's `MapOutputTracker` lookup, and it exists
 *                       because a producer that has not finished has no materialised output a
 *                       location tracker could describe
 * @param backpressure the executor-wide credit ledger and flow-control state machine. Shared so it
 *                     can arbitrate between concurrent shuffles and so the token bucket's refill
 *                     rate has a meaningful count of them to divide by
 * @param fallbackPolicy the executor-wide policy that decides when streaming yields to the
 *                       sort-based shuffle. Shared so a trip is remembered across tasks
 * @param errorNotifier the first-error-wins bridge from Netty threads to the task thread
 * @param connector opens channels to producers. Injected so that a test can drive every path in the
 *                  reader without a socket, and owned by the manager, which closes it in `stop()`
 * @param serializerManager applies the same compression and encryption wrapping to a streamed
 *                          payload that the classic path applies to a fetched block
 */
private[spark] case class StreamingShuffleReaderContext(
    coordinatorRef: RpcEndpointRef,
    backpressure: BackpressureProtocol,
    fallbackPolicy: StreamingShuffleFallbackPolicy,
    errorNotifier: StreamingShuffleErrorNotifier,
    connector: StreamingShuffleProducerConnector,
    serializerManager: SerializerManager)

/**
 * Opens consumer-side channels to streaming shuffle producers.
 *
 * Abstract for two reasons. It keeps every socket concern out of the reader, which is what lets the
 * reader's own suites exercise producer loss, corruption repair, sequence violations and orderly
 * completion deterministically, with no port and no timing. And it puts channel construction in one
 * place, so the pipeline every streaming consumer runs -- frame decoder, then
 * [[StreamingShuffleClientHandler]] -- cannot drift between call sites.
 *
 * <b>Ownership.</b> An implementation may hold process-wide resources such as an event loop, so it
 * is owned by whoever built it -- `StreamingShuffleManager` -- and closed when that owner stops. A
 * reader never closes a connector; it closes only the channels it opened.
 */
private[spark] trait StreamingShuffleProducerConnector {

  /**
   * Opens a channel to one producer with the given handler attached.
   *
   * @param location the producer to reach
   * @param handler the consumer handler to attach; it becomes the owner of the returned channel
   * @param onIncompatibleVersion invoked, from a Netty thread, with the protocol version of the
   *                              first frame this build cannot read. It must not block. An
   *                              implementation reports a version this way instead of failing to
   *                              parse, so that a mismatch is detected explicitly and can trip the
   *                              documented fallback with the right reason attached
   * @return the connected channel, or `None` if it could not be established
   */
  def connect(
      location: StreamingShuffleProducerLocation,
      handler: StreamingShuffleClientHandler,
      onIncompatibleVersion: Byte => Unit): Option[Channel]

  /** Releases any process-wide resource this connector holds. Must be idempotent. */
  def close(): Unit
}

/**
 * The production [[StreamingShuffleProducerConnector]]: a Netty client bootstrap on a transport
 * configuration scoped to the streaming module.
 *
 * <b>Why a module of its own.</b> Requesting configuration for
 * [[StreamingShuffleClientHandler.TRANSPORT_MODULE]] yields an independent
 * `spark.shuffle-streaming.io.*` namespace, so thread counts, buffer sizes, retry behaviour and TCP
 * keepalive can be tuned for streaming without perturbing the block transfer service that every
 * other shuffle depends on. No shared transport class is modified to achieve this: `TransportConf`
 * and `SparkTransportConf` are consumed exactly as they stand.
 *
 * <b>Keepalive.</b> Enabled from the module's own configuration, which is the only keepalive
 * control the platform exposes -- the JDK offers no keepalive <i>interval</i> as a socket option.
 * The five-second liveness bound the protocol promises is therefore enforced at application level
 * by the heartbeat timer, and OS keepalive is a second, coarser safety net beneath it.
 *
 * @param conf the executor configuration the streaming transport namespace is read from
 */
private[spark] class NettyStreamingShuffleProducerConnector(conf: SparkConf)
  extends StreamingShuffleProducerConnector with Logging {

  import StreamingShuffleReader._

  private val transportConf: TransportConf = SparkTransportConf.fromSparkConf(
    conf, StreamingShuffleClientHandler.TRANSPORT_MODULE, numUsableCores = 0)

  private val ioMode: IOMode = IOMode.valueOf(transportConf.ioMode())

  private val workerGroup: EventLoopGroup = NettyUtils.createEventLoop(ioMode,
    transportConf.clientThreads(), s"${StreamingShuffleClientHandler.TRANSPORT_MODULE}-client")

  private val closed = new AtomicBoolean(false)

  override def connect(
      location: StreamingShuffleProducerLocation,
      handler: StreamingShuffleClientHandler,
      onIncompatibleVersion: Byte => Unit): Option[Channel] = {
    if (closed.get()) {
      logWarning(log"Refusing to open a streaming shuffle channel to " +
        log"${MDC(HOST_PORT, location.hostPort)} because this connector is closed")
      None
    } else {
      val bootstrap = new Bootstrap()
      bootstrap.group(workerGroup)
        .channel(NettyUtils.getClientChannelClass(ioMode))
        .option(ChannelOption.TCP_NODELAY, java.lang.Boolean.TRUE)
        .option(ChannelOption.SO_KEEPALIVE,
          java.lang.Boolean.valueOf(transportConf.enableTcpKeepAlive()))
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
          Integer.valueOf(transportConf.connectionCreationTimeoutMs()))
        // The handler owns the receive window from here on: it turns autoRead off when the consumer
        // falls behind and on again when an acknowledgement advances. Starting it on is what lets
        // the first block arrive.
        .option(ChannelOption.AUTO_READ, java.lang.Boolean.TRUE)
      if (transportConf.receiveBuf() > 0) {
        bootstrap.option(ChannelOption.SO_RCVBUF, Integer.valueOf(transportConf.receiveBuf()))
      }
      if (transportConf.sendBuf() > 0) {
        bootstrap.option(ChannelOption.SO_SNDBUF, Integer.valueOf(transportConf.sendBuf()))
      }
      bootstrap.handler(new ChannelInitializer[SocketChannel] {
        override def initChannel(channel: SocketChannel): Unit = {
          channel.pipeline()
            .addLast(FRAME_DECODER_HANDLER_NAME,
              new StreamingShuffleFrameDecoder(onIncompatibleVersion))
            .addLast(CONSUMER_HANDLER_NAME, handler)
        }
      })
      awaitConnection(bootstrap, location)
    }
  }

  /**
   * Waits for one connection attempt, bounded by the module's connection creation timeout.
   *
   * Called on the task thread, never on an event loop, so blocking here is legitimate. An
   * interruption is honoured rather than swallowed: the flag is restored and the attempt reported
   * as a failure, which the reader converts into a fetch failure like any other.
   */
  private def awaitConnection(
      bootstrap: Bootstrap,
      location: StreamingShuffleProducerLocation): Option[Channel] = {
    val timeoutMillis = transportConf.connectionCreationTimeoutMs().toLong
    try {
      val future = bootstrap.connect(location.host, location.port)
      if (future.await(timeoutMillis) && future.isSuccess) {
        Some(future.channel())
      } else {
        future.cancel(false)
        logWarning(log"Could not connect a streaming shuffle channel to " +
          log"${MDC(HOST_PORT, location.hostPort)} within ${MDC(TIMEOUT, timeoutMillis)} ms")
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
    }
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      workerGroup.shutdownGracefully(0L, EVENT_LOOP_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      logDebug(log"Streaming shuffle producer connector for module " +
        log"${MDC(VALUE, transportConf.getModuleName())} shut its event loop down")
    }
  }
}

/**
 * Turns the bytes of a streaming shuffle channel into whole protocol messages.
 *
 * <b>Why a decoder of our own.</b> The producer writes each message as its type byte followed by
 * its encoded body, with no length prefix, so a frame's extent has to be derived from its type:
 * every control message is fixed width, and a data block's width is its framing overhead plus the
 * payload length its header declares. Deriving the extent this way -- rather than guessing from
 * what happened to be readable -- is what makes the four fixed-width control messages, which are
 * all exactly the same length, distinguishable at all.
 *
 * <b>Why the version is peeked and not parsed.</b> The version sits immediately after the type byte
 * and is read without consuming, so an incompatible peer is reported as a version mismatch rather
 * than as a malformed frame or an unknown type. That distinction matters: only an explicit mismatch
 * can trip the documented fallback with the right reason, and the protocol publishes
 * `peekProtocolVersion` precisely so that no caller has to infer it.
 *
 * Runs on a Netty event-loop thread and therefore does nothing but frame, report and hand on. It
 * never verifies a checksum, never touches a metric and never throws a fetch failure; the reader
 * does all three on the task thread.
 *
 * @param onIncompatibleVersion invoked with the version of the first unreadable frame, after which
 *                              the channel is closed
 */
private[spark] class StreamingShuffleFrameDecoder(onIncompatibleVersion: Byte => Unit)
  extends ByteToMessageDecoder {

  import StreamingShuffleReader._

  override protected def decode(
      ctx: ChannelHandlerContext,
      in: ByteBuf,
      out: java.util.List[AnyRef]): Unit = {
    val readable = in.readableBytes()
    if (readable >= MIN_FRAME_PEEK_LENGTH) {
      val version = in.getByte(in.readerIndex() + StreamingShuffleMessage.FRAME_TYPE_PREFIX_LENGTH)
      if (!StreamingShuffleMessage.isCompatible(version)) {
        // Nothing in this stream can be trusted once the version is wrong, so the remaining bytes
        // are dropped rather than parsed, and the reader is told which version it was.
        onIncompatibleVersion(version)
        in.skipBytes(readable)
        ctx.close()
      } else {
        val messageType = resolveType(in.getByte(in.readerIndex()))
        frameLengthOf(messageType, in, readable).foreach { frameLength =>
          if (readable >= frameLength) {
            // Exactly one frame: the protocol's decoder rejects a buffer with bytes left over,
            // which is what makes a framing error impossible to mistake for a payload.
            out.add(StreamingShuffleMessage.Decoder.fromByteBuffer(
              in.readSlice(frameLength).nioBuffer()))
          }
        }
      }
    }
  }

  /**
   * Resolves the type byte through the protocol's own discriminator.
   *
   * An unknown id is the typed condition it looks like, raised as such so that the diagnostic names
   * the byte instead of blaming whatever the body failed to parse as.
   */
  private def resolveType(typeByte: Byte): StreamingShuffleMessageType = {
    try {
      StreamingShuffleMessageType.fromId(typeByte)
    } catch {
      case _: IllegalArgumentException =>
        throw StreamingShuffleErrors.unexpectedMessageType(
          "a known streaming shuffle message type", s"message type id $typeByte")
    }
  }

  /**
   * The framed length of the message at the head of the buffer, or `None` while too few bytes are
   * buffered to tell.
   *
   * @throws IllegalArgumentException by way of the typed unexpected-message condition when a data
   *                                 block declares a payload outside the protocol's own bounds
   */
  private def frameLengthOf(
      messageType: StreamingShuffleMessageType,
      in: ByteBuf,
      readable: Int): Option[Int] = {
    if (messageType == StreamingShuffleMessageType.DATA_BLOCK) {
      if (readable < PAYLOAD_LENGTH_FRAME_OFFSET + java.lang.Integer.BYTES) {
        None
      } else {
        val payloadLength = in.getInt(in.readerIndex() + PAYLOAD_LENGTH_FRAME_OFFSET)
        if (payloadLength < 0 || payloadLength > DataBlockMessage.MAX_BLOCK_SIZE_BYTES) {
          throw StreamingShuffleErrors.unexpectedMessageType(
            s"a data block of at most ${DataBlockMessage.MAX_BLOCK_SIZE_BYTES} payload byte(s)",
            s"a data block declaring $payloadLength payload byte(s)")
        }
        Some(DataBlockMessage.FRAMING_OVERHEAD_BYTES + payloadLength)
      }
    } else {
      Some(CONTROL_FRAME_LENGTH)
    }
  }
}
