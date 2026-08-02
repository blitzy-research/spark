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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.concurrent.duration.DurationLong
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.netty.channel.{Channel, EventLoopGroup}

import org.apache.spark.{Aggregator, InterruptibleIterator, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{BLOCK_ID, CHECKSUM, COUNT, EPOCH, ERROR, EXECUTOR_ID, HOST_PORT, MAP_ID, MAX_ATTEMPTS, NUM_BLOCKS, NUM_BYTES, NUM_PARTITIONS, NUM_SKIPPED, NUM_TASKS, PARTITION_ID, PROTOCOL_VERSION, REASON, SHUFFLE_ID, STATUS, THRESHOLD, TIMEOUT, VALUE, VERSION_NUM}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient, TransportClientBootstrap, TransportClientFactory}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol.streaming.{DataBlockMessage, StreamingShuffleChecksum, StreamingShuffleMessage}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.rpc.{RpcEndpointRef, RpcTimeout, RpcTimeoutException}
import org.apache.spark.serializer.{DeserializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.streaming.StreamingShuffleClientHandler.{BlockReceived, ProducerLost, StreamCompleted}
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.{Clock, CompletionIterator, SystemClock}
import org.apache.spark.util.collection.ExternalSorter

/**
 * The consumer half of a streaming shuffle: a [[ShuffleReader]] that consumes a reduce partition
 * directly from the executors producing it, block by block as blocks arrive, instead of waiting for
 * the map output to be materialised on disk and then fetching whole partitions of it.
 *
 * <b>Producers need not have finished, and the log says whether they had.</b> This reader
 * rendezvouses on <i>registered</i> producers rather than on completed ones -- a producer registers
 * when it starts producing, not when it stops -- so a partition whose map tasks are still running
 * is read as it is produced, and no step of `read()` waits for the map stage to complete. Whether
 * that capability is exercised is decided elsewhere and deliberately not here: task submission
 * belongs to the DAG scheduler, which this feature may not modify, and the unmodified scheduler
 * submits a reduce stage only once its map stage reports available output. For a map stage whose
 * tasks all run once, this reader therefore consumes retained output from producers that have
 * already finished, and what it saves is the index-and-fetch round trip and the whole-partition
 * buffering rather than the overlap. Because the rendezvous reply carries the completion set, which
 * of the two actually happened is recorded rather than left to be assumed.
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
 * <b>Corruption.</b> Every block's CRC32C is verified before any record from it becomes visible. A
 * mismatch inside the window this producer still retains -- that is, at a sequence number this
 * consumer has not yet acknowledged to it -- is repaired by asking for a replay of exactly that
 * block, with exponential backoff from one second over at most
 * [[BackpressureProtocol.MAX_RETRY_ATTEMPTS]] attempts. A mismatch outside that window cannot be
 * repaired, because the acknowledgement that moved the window is precisely what let the producer
 * free the bytes, so it escalates to `FetchFailedException` and correctness is recovered by
 * recomputation. Either way no corrupt record is ever handed to user code.
 *
 * <b>Determinism.</b> All timing goes through the injected [[Clock]] and through bounded numbers of
 * poll windows. Nothing sleeps and nothing reads the JVM's own clock directly, anywhere in this
 * file -- the transport connector below takes a clock for the same reason -- so the behaviour of
 * every timer on the consumer side is reproducible under a manual clock.
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
 * @param clock the clock every timer in this class reads, so each one advances with that clock
 *              rather than with wall time and nothing here sleeps
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

  // Collaborators. They are shared executor-wide, which is why they arrive rather than being built:
  // the backpressure ledger has to see every concurrent shuffle in order to arbitrate between them,
  // and the fallback policy has to remember a trip across tasks for the kill switch to mean
  // anything.
  private val coordinatorRef: RpcEndpointRef = streamingContext.coordinatorRef
  private val backpressure: BackpressureProtocol = streamingContext.backpressure
  private val fallbackPolicy: StreamingShuffleFallbackPolicy = streamingContext.fallbackPolicy
  private val connector: StreamingShuffleProducerConnector = streamingContext.connector
  private val serializerManager: SerializerManager = streamingContext.serializerManager

  /**
   * The bridge that carries a failure observed on this reader's Netty threads to this reader's task
   * thread.
   *
   * <b>Why it is built here rather than shared.</b> Every other collaborator is shared because
   * it exists in order to reason across the tasks running on one executor. This one is the exact
   * opposite: it exists to carry one task's failure to that same task, and it is first-error-wins
   * and permanent by design -- the first throwable it latches is never displaced, which is what
   * makes the reported failure the one that explains a cascade. Those two properties do not
   * compose. A notifier shared executor-wide latches the first failure any reader ever observes and
   * then re-throws it from `throwIfError()` for every reader that runs afterwards: readers of
   * unrelated shuffles that are streaming perfectly, and retry attempts of the very task that
   * failed. One producer loss would stand the executor's whole streaming path down until the
   * executor restarted, and it would do so while reporting a stranger's diagnosis -- a
   * `FetchFailedException` naming a producer and a partition that have nothing to do with the task
   * being failed, which the scheduler would act upon by recomputing the wrong upstream stage.
   *
   * A per-reader notifier has none of that reach. It is created when this reader is created, it is
   * reachable only from this reader and from the client handlers this reader opens, and it becomes
   * garbage when the task ends -- so the absence of a production reset stops being a defect and
   * becomes the correct design: there is nothing to reset, because nothing outlives the failure's
   * owner. [[StreamingShuffleErrorNotifier.reset]] therefore stays a test affordance rather than a
   * production requirement.
   */
  private val errorNotifier: StreamingShuffleErrorNotifier =
    new StreamingShuffleErrorNotifier(shuffleId, conf)

  // Configuration is read exactly once, here, and held immutably for the lifetime of the reader.
  // That is what makes "configuration changes require an executor restart" true by construction
  // and removes any need for a dynamic reconfiguration path, which is explicitly out of scope.
  private val streamingEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_ENABLED)
  private val spillThresholdPercent: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)
  private val maxBandwidthMbps: Option[Int] = conf.get(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)
  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  /**
   * The bounded deadline every coordinator ask this reader makes is subject to.
   *
   * <b>Why not the default.</b> `askSync` without a timeout uses `spark.rpc.askTimeout`, falling
   * back to `spark.network.timeout` and thus to two minutes. A rendezvous that waited two minutes
   * on an unresponsive driver would have this reader claim a five-second producer bound while
   * actually delivering a hundred-and-twenty-second one, and every guarantee downstream of it --
   * partial-read invalidation, upstream recomputation, the fallback -- would be late by the same
   * margin. The deadline is therefore stated rather than inherited.
   *
   * <b>Why this key.</b> The subsystem already publishes one connection deadline, the transport's
   * `spark.shuffle-streaming.io.connectionTimeout`, and the coordinator ask is part of establishing
   * the same connection. Deriving the ask deadline from that key gives an operator one lever for
   * both instead of two that can disagree, and it makes the hint in a timeout message name a key
   * that actually exists and actually governs the wait. Floored at
   * [[StreamingShuffleReader.MIN_COORDINATOR_TIMEOUT_MS]], so a very small transport setting cannot
   * make every rendezvous fail before the driver has had a chance to answer.
   */
  private val coordinatorTimeout: RpcTimeout = {
    val configured = conf.getTimeAsMs(StreamingShuffleServerHandler.CONNECTION_TIMEOUT_KEY,
      s"${StreamingShuffleServerHandler.CONNECTION_TIMEOUT_SECONDS}s")
    new RpcTimeout(math.max(MIN_COORDINATOR_TIMEOUT_MS, configured).millis,
      StreamingShuffleServerHandler.CONNECTION_TIMEOUT_KEY)
  }

  /**
   * The consumer-side budget, which is the executor's and not this reader's.
   *
   * <b>Why it is read and not computed.</b> A reader that derived `bufferSizePercent` of executor
   * memory for itself would be joined by every other reduce task in the same JVM deriving the same
   * figure, and by every producer and partition each of them read from, so the aggregate consumer
   * heap would be the configured percentage multiplied by a number nobody chose. The budget is
   * therefore owned by [[BackpressureProtocol]], which is constructed once per executor and shared
   * by every reader and every consumer handler, and which charges each admitted frame against it.
   * Every consumer of every shuffle on this executor is bounded by this one number.
   *
   * It is still the same percentage of the same executor memory the producer side is bounded by,
   * read from the same configuration entry, so the two halves of a streaming shuffle continue to be
   * sized from one number rather than from two that could drift apart.
   */
  private val bufferBudgetBytes: Long = backpressure.receiveQuotaBytes

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

  /**
   * The ledger identities this reader opened, so cleanup releases exactly what it claimed.
   *
   * Keyed by stream rather than by partition. One reduce partition is fed by one stream per map
   * output, so a partition is not a stream: keying by partition alone would have this reader claim
   * a single allowance for data arriving from every producer at once, and would have its cleanup
   * close a ledger a concurrent reader of the same partition -- a speculative attempt of this very
   * task -- was still using.
   */
  private val registeredStreams = new mutable.HashSet[BackpressureStreamKey]

  /**
   * This reader's consumer session identity, announced to every producer it streams from.
   *
   * The task attempt id is allocated from one monotonically increasing per-application counter, so
   * it names this attempt uniquely across the whole application; the partition range distinguishes
   * two readers of the same attempt. A reduce task that fails and is retried is therefore a new
   * session with a new receive window, which is what stops it inheriting the credit accounting --
   * or the retained-output cursor -- of the session it replaced. Inheriting that cursor is what
   * would lose data, because it would have the new attempt resume past blocks it never read.
   *
   * Within one attempt the value is fixed, and it travels on every heartbeat this reader's handlers
   * send. That is what makes it the identity a producer keys its retained-output cursor and its
   * per-consumer credit ledger by, and therefore what lets a reconnected channel resume where the
   * previous one stopped rather than start again from the first block. A socket-derived identity
   * could do neither, because it changes with the socket.
   */
  private val consumerId: String =
    s"attempt-${context.taskAttemptId()}-partitions-$startPartition-$endPartition"

  private val cleanedUp = new AtomicBoolean(false)

  /**
   * Released by cleanup so that no bounded wait in this reader can outlive the task. Only ever
   * awaited, never counted down for its own sake, which is what makes it a wait primitive that
   * reads no clock.
   */
  private val quiesce = new CountDownLatch(1)

  /**
   * Bytes this reader is holding in decoded-but-unconsumed blocks.
   *
   * An atomic rather than a field under `streamsLock` because the two sides of the count are on
   * different threads: a network event-loop thread adds as it stashes a decoded block, and the task
   * thread subtracts as it consumes one. Neither may wait on the other -- parking an event-loop
   * thread stalls every channel sharing it -- so the counter has to be lock-free rather than merely
   * convenient.
   */
  private val stashedBytes = new AtomicLong(0L)

  /** Producer invalidations this reader has performed, mirrored onto the metrics source. */
  private var invalidations: Int = 0

  /** Blocks accepted and bytes accepted, for the throughput samples the fallback policy needs. */
  private var acceptedBlocks: Long = 0L
  private var acceptedBytes: Long = 0L

  /**
   * Cumulative repair activity on this reduce task, and the two windows that bound its reporting.
   *
   * Both conditions recur per block rather than per task, and the retry budget is per block too, so
   * a stream that is corrupt rather than merely unlucky produces one report per block and up to
   * [[BackpressureProtocol.MAX_RETRY_ATTEMPTS]] more for its replays. That is the log volume of the
   * shuffle itself, which is the one thing a diagnostic must never be. Each condition therefore
   * reports the first occurrence immediately and then at most one aggregate per window, and each
   * aggregate carries both the running total and the number of occurrences it stands in for, so the
   * volume is stated even when the individual events are not. Per-block identity remains available
   * behind the streaming debug key, and the terminal escalation -- a block that could not be
   * repaired at all -- is never gated, because there is exactly one of those per task.
   *
   * The gates are per reader, not per executor, matching where the condition is observed and who
   * owns the recovery. That bounds an executor at one report per window per concurrently running
   * reduce task, which is bounded by its core count rather than by the size of the shuffle.
   */
  private var checksumFailures: Long = 0L
  private var replayRequests: Long = 0L
  private val checksumFailureLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)
  private val replayRequestLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

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
    // The consumer side deliberately contributes nothing to `bufferUtilizationPercent`. That gauge
    // reports the producer buffer budget the spill threshold is evaluated against, and the receive
    // quota this side holds is a different budget entirely: adding it would put unrelated bytes in
    // the numerator and unrelated capacity in the denominator, so a producer genuinely at its
    // eighty percent spill point would read as roughly forty the moment one idle reader started.
    // The spill manager's executor-shared quota is therefore the gauge's sole owner. What the
    // consumer side holds is observable in its own right through the protocol's receive-quota
    // accessors and through the backpressure-event counter.

    // Deduplicated before anything is claimed or opened, so that the credit split, the streams
    // and the reduce input all describe the same set of producers.
    val locations = acceptedGenerations(rendezvous())
    // Claimed only once the producing generations are known, because a generation is part of a
    // stream's identity and therefore of the allowance being claimed.
    registerStreams(locations)
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

    // Streams are open by now, so a producer lost while this read was being assembled is converted
    // before the notifier is consulted -- otherwise the transport error latched alongside the loss
    // would be raised here, one step before the iterator's own conversion could run.
    convertSignalledProducerLossEverywhere()
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
   * The one producer generation per map index that this consumer may read.
   *
   * A logical map output has exactly one accepted generation. A map index can be produced more than
   * once -- speculative execution and task retries both do it -- and every attempt streams the
   * whole of that index's output rather than a share of it, so reading two attempts of one index
   * would concatenate the same records into the reduce input twice. That is a wrong answer returned
   * silently, not a failure, which makes it the most expensive thing this reader could get wrong.
   * The generation to read is the newest attempt offered: the coordinator lets a higher task
   * attempt id supersede a lower one for the same map index, and a superseded attempt is precisely
   * the one whose output the scheduler has abandoned.
   *
   * On any consistent reply this is the identity function, because the coordinator holds at most
   * one producer per map index. It is applied regardless, because the two costs are not comparable:
   * the check is one grouping of a list as long as the map count, while being wrong duplicates rows
   * in a result no assertion downstream would question. A coordinator that ever offered two
   * generations of one index -- through a defect, or through a reply a consumer of a different
   * vintage reads -- must not be able to turn that into duplicated output here.
   *
   * Applied before allowances are claimed and before any stream is opened, so the credit split, the
   * open channels and the records read all describe the same set of producers.
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
   *
   * Registration happens after rendezvous rather than before it, and it has to: a stream's identity
   * includes the producer generation, and the generations serving this partition range are exactly
   * what rendezvous discovers. Registering by partition beforehand would have claimed one allowance
   * for the data of every map task at once, leaving the receive windows of concurrent producers
   * indistinguishable from each other.
   *
   * Only the ledgers this call actually opened are remembered, so cleanup releases exactly what it
   * claimed and leaves a concurrent reader -- a speculative attempt of this same task, for instance
   * -- holding its own allowance untouched.
   */
  private def registerStreams(locations: Seq[StreamingShuffleProducerLocation]): Unit = {
    // One partition's allowance is shared between the generations feeding it, so making the ledger
    // per producer sharpens the identity without loosening the bound: the aggregate this reader may
    // hold is the same partition allowance it always was, however many producers serve it. Floored
    // at one maximum-size frame, because a window too small for the largest legal block could never
    // make progress at all.
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
   * A reply that names no producer at all is <b>not</b> automatically a failure, and it is not
   * automatically success either. The declared map-stage cardinality decides which it is: a stage
   * that declared no map tasks legitimately produces an empty partition and the read yields an
   * empty iterator, while a stage that declared map tasks whose producers have not registered yet
   * is simply not ready and is polled again. Reading the second as the first is how a streaming
   * consumer silently returns nothing for data that was on its way, so the two are distinguished on
   * the cardinality the coordinator publishes rather than inferred from the size of a list.
   */
  private def rendezvous(): Seq[StreamingShuffleProducerLocation] = {
    // The executor's own verdict is consulted before the driver's. A trip observed here -- by this
    // reader's own executor, for any of the four conditions -- is as binding as one the coordinator
    // has latched, and asking the driver first would open a channel this executor has already
    // decided not to use.
    checkLocalFallback(UNKNOWN_MAP_INDEX)
    val waitStartMillis = clock.getTimeMillis()
    var attempts = 0
    var resolved: Option[StreamingShuffleProducerLocations] = None
    var lastReply: Option[StreamingShuffleProducerLocations] = None
    while (resolved.isEmpty && attempts < COORDINATOR_LOOKUP_MAX_ATTEMPTS && !cleanedUp.get()) {
      errorNotifier.throwIfError()
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
        if (debugEnabled) {
          // The completion count is reported alongside the producer count because together they are
          // the only honest answer to "did this read overlap its map stage": a reply in which every
          // declared map index has already streamed to completion is a read of retained output,
          // while any shortfall is a read that genuinely overlaps production.
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} resolved " +
            log"${MDC(COUNT, reply.locations.size)} live producer(s) of " +
            log"${MDC(NUM_TASKS, reply.numMaps)} declared map task(s) for partitions " +
            log"[${MDC(PARTITION_ID, startPartition)}, ${MDC(VALUE, endPartition)}) after " +
            log"${MDC(MAX_ATTEMPTS, attempts + 1)} lookup attempt(s); the map stage had " +
            log"${MDC(STATUS, if (reply.mapStageComplete) "finished" else "not finished")} " +
            log"producing, so this is a read of " +
            log"${MDC(REASON, if (reply.mapStageComplete) "retained output" else "output in " +
              "progress")}")
        }
        reply.locations
      case None =>
        // The declaration is what makes this terminate. A fetch failure on its own resubmits the
        // reduce stage against a map stage the tracker still reports as available, so the next
        // attempt reaches this same unresolvable rendezvous and the one after that, until the
        // stage-attempt limit aborts the job. Declaring the fallback has the coordinator withdraw
        // the shuffle's streamed map output, so the scheduler resubmits the *map* stage and the
        // manager serves its new attempts from the sort-based delegate: one recomputation, then a
        // job that completes. This is also the terminus for a consumer that arrives after every
        // producer of its shuffle has been reaped for silence -- a producer whose task has ended
        // cannot be streamed from, however complete its output was, so the only correct answer is
        // to read a map stage recomputed onto the sort-based path.
        val declared = declareShuffleFallback(StreamingShuffleFallbackReason.ConsumerTooSlow,
          s"a consumer of partitions [$startPartition, $endPartition) could not resolve every " +
            s"producer after $attempts lookup attempt(s)")
        throw new FetchFailedException(null, shuffleId, UNKNOWN_MAP_ID, UNKNOWN_MAP_INDEX,
          startPartition, unresolvedRendezvousMessage(lastReply, attempts, declared))
    }
  }

  /**
   * Fails the read when this executor has itself stood streaming down.
   *
   * [[StreamingShuffleFallbackPolicy]] holds two answers and both are consulted, because they cover
   * different failures. `shuffleHasFallenBack` is the coordinator's latched, shuffle-wide verdict
   * as this executor last learned it. `hasTripped` is this executor's own observation of one of the
   * four trip conditions, which no other participant may have seen yet -- and which therefore has
   * to be propagated by whoever noticed it, since none of them is privileged.
   *
   * Propagation is a declaration to the coordinator, claimed through the policy's own one-shot so
   * that the wire cost is one ask per shuffle per executor rather than one per task. It is not
   * optional: without it this reader would fail its fetch while the map stage stayed registered as
   * streamed, and every retry would fail the same way.
   *
   * @param mapIndex the map index to blame, or [[StreamingShuffleReader.UNKNOWN_MAP_INDEX]] when
   *                 the failure belongs to the shuffle rather than to one map output
   */
  private def checkLocalFallback(mapIndex: Int): Unit = {
    val known = fallbackPolicy.knownShuffleFallback(shuffleId)
    val state = known match {
      case Some(latched) => Some(latched)
      case None if fallbackPolicy.hasTripped =>
        val reason =
          fallbackPolicy.trippedReason.getOrElse(StreamingShuffleFallbackReason.ConsumerTooSlow)
        Some(declareShuffleFallback(reason,
          s"a consumer of partitions [$startPartition, $endPartition) observed the condition " +
            "on its own executor"))
      case None => None
    }
    state.filter(_.fallenBack).foreach { latched =>
      val description = latched.reason.map(_.description).getOrElse(latched.reasonName)
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
   *
   * For the two places that consult the notifier without awaiting any one partition. Cheap when
   * nothing has failed -- one volatile read per open producer and no allocation beyond the snapshot
   * -- and it is the snapshot that makes taking the lock necessary: the release listener may clear
   * the buffer from the task-completion thread while this walks it.
   */
  private def convertSignalledProducerLossEverywhere(): Unit = {
    if (errorNotifier.hasError) {
      streamsLock.synchronized(producerStreams.toSeq)
        .foreach(stream => stream.convertSignalledProducerLoss())
    }
  }

  /**
   * Declares a shuffle-wide fallback on the coordinator and caches the verdict it latched.
   *
   * Two things make this safe to call from a failure path. The ask is bounded by the same explicit
   * deadline as every other coordinator ask, so declaring cannot hold a failing task for the
   * generic RPC default; and every failure is absorbed, because the caller is already raising a
   * fetch failure and must not have it replaced by an RPC error the scheduler cannot recover from.
   * An unreachable coordinator therefore costs the extra recomputation the declaration would have
   * avoided, and never correctness.
   *
   * The claim is taken through the policy so that concurrent readers and writers of one shuffle on
   * one executor produce a single ask; when the claim is refused the verdict already cached is
   * returned instead, which is the same answer the coordinator would have given.
   *
   * @param reason which of the four documented conditions is being declared
   * @param detail operator-facing context recorded alongside the verdict
   * @return the verdict in force after the declaration, empty when it could not be made
   */
  private def declareShuffleFallback(
      reason: StreamingShuffleFallbackReason,
      detail: String): StreamingShuffleFallbackState = {
    fallbackPolicy.knownShuffleFallback(shuffleId) match {
      case Some(latched) => latched
      case None =>
        val declared = try {
          coordinatorRef.askSync[Any](DeclareStreamingShuffleFallback(
            shuffleId, capabilityToken, reason.toString, detail), coordinatorTimeout) match {
            case state: StreamingShuffleFallbackState => state
            case other =>
              logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} received " +
                log"${MDC(VALUE, describe(other))} for a fallback declaration; treating the " +
                log"shuffle as still streaming and relying on the fetch failure alone")
              StreamingShuffleFallbackState()
          }
        } catch {
          case NonFatal(e) =>
            logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not declare " +
              log"a fallback for ${MDC(REASON, reason.description)}; the reduce side still " +
              log"recovers through its fetch failure", e)
            StreamingShuffleFallbackState()
        }
        fallbackPolicy.observeShuffleFallback(shuffleId, declared)
        declared
    }
  }

  /**
   * Whether a lookup reply names a producer this consumer can open a stream to for every map output
   * it is entitled to read.
   *
   * Readiness is defined by what this consumer can actually read, which is a producer it holds an
   * address for -- not by what the coordinator happens to know. A completion record is deliberately
   * not accepted in place of an address: a completion says only that some producer finished
   * streaming that map index, and a consumer that treated it as sufficient would stop polling for
   * an index it can reach nobody for and hand back an iterator missing that index's records.
   * That is silent data loss, whereas continuing to poll costs at worst one more lookup and then a
   * bounded fetch failure, which the unmodified scheduler resolves by recomputing the upstream
   * stage.
   *
   * Requiring the address costs nothing on the happy path, because the coordinator holds one
   * producer per map index, exempts a producer from reaping once it has reported completion, and
   * withdraws a completion whenever the producer that earned it goes -- so a completion never
   * outlives the location that backs it.
   *
   * A stage that declared no map tasks is accounted for by definition, which is precisely the
   * legitimately-empty case, and a stage that has stood streaming down never reaches here because
   * [[checkShuffleFallback]] has already failed the fetch.
   */
  private def rendezvousSatisfied(reply: StreamingShuffleProducerLocations): Boolean = {
    reply.locations.map(_.mapIndex).toSet.size >= reply.numMaps
  }

  /**
   * Fails the fetch when the shuffle has stood streaming down for every participant.
   *
   * Graceful degradation is a shuffle-wide decision, taken by whichever participant first observed
   * a trip condition and latched by the coordinator. Once taken, the streaming output of this
   * shuffle is invalid: its producers have been retired so that none can re-register, and the
   * reduce side must read a map stage recomputed onto the sort-based path instead. There is
   * therefore no address this consumer could usefully be handed, and the recovery is the one the
   * classic path already has -- a fetch failure, which the unmodified scheduler resolves by
   * resubmitting the upstream stage, whose new attempts the manager serves from its sort-based
   * delegate.
   *
   * Raised before any producer stream is opened, so nothing has been consumed that would need
   * invalidating; a fallback observed after streams are open is handled on the iterator path, where
   * partial reads do have to be discarded first.
   */
  private def checkShuffleFallback(reply: StreamingShuffleProducerLocations): Unit = {
    if (reply.fallback.fallenBack) {
      val fallback = reply.fallback
      val description = fallback.reason.map(_.description).getOrElse(fallback.reasonName)
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
      declared: StreamingShuffleFallbackState): String = {
    val spanMs = attempts * COORDINATOR_LOOKUP_INTERVAL_MS
    val observed = lastReply match {
      case Some(reply) =>
        val located = reply.locations.map(_.mapIndex).toSet.size
        // Whether more output was still expected is what separates the two diagnoses that share
        // this message: a rendezvous that ran out of attempts while producers were still arriving
        // is a pacing problem, whereas one that ran out after the stage had finished or stood down
        // is a liveness or registration problem, and no further waiting would have helped.
        val outlook = if (reply.mapOutputsPending) {
          "map output was still expected at that point"
        } else {
          "no further map output was expected at that point"
        }
        s"the coordinator last named a live producer for $located of ${reply.numMaps} declared " +
          s"map task(s), ${reply.completedMapIndexes.size} of which had streamed to completion, " +
          s"at epoch ${reply.coordinatorEpoch}, and $outlook"
      case None => "the shuffle had no registration at all"
    }
    val recovery = if (declared.fallenBack) {
      s"The shuffle has stood streaming down at epoch ${declared.declaredAtEpoch}, so its " +
        "streamed map output has been withdrawn and the upstream stage is recomputed on the " +
        "sort-based shuffle path."
    } else {
      "Recomputing the upstream stage is the recovery, because map output that never " +
        "registered a producer cannot be streamed from."
    }
    s"Streaming shuffle $shuffleId could not resolve every producer of partitions " +
      s"[$startPartition, $endPartition) after $attempts lookup attempt(s) spanning $spanMs ms: " +
      s"$observed. $recovery"
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
    val reply = try {
      coordinatorRef.askSync[Any](request, coordinatorTimeout)
    } catch {
      // Translated on the spot rather than retried. A driver that has not answered within the
      // subsystem's own connection deadline is exactly the bounded-failure case this reader
      // promises to report quickly, and polling an unresponsive coordinator for the rest of the
      // lookup budget is the delay the deadline exists to prevent. The classic path behaves the
      // same way -- a map-output fetch that times out fails the task rather than looping.
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
   * A shuffle-wide failure with no producer to blame, so it names none: the address is null and the
   * map identity is the documented unknown sentinel, which is the shape `FetchFailedException`
   * itself sanctions for a failure that cannot be attributed to one map output. Recovery is the
   * ordinary one -- the unmodified scheduler resubmits the stage -- and if the coordinator is
   * unreachable because it has stood this shuffle down, the retry reads the sort-based output the
   * recomputation produces.
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
        log"speaks ${MDC(VERSION_NUM, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)}; " +
        log"falling back to the " +
        log"sort-based shuffle and recomputing the upstream stage")
      // Declared as well as tripped locally, for the same reason exhaustion is: the fetch failure
      // recovers this reduce attempt, and only the withdrawal of the streamed map output makes the
      // recomputation land on the sort-based path instead of on producers that would announce the
      // very same version again.
      declareShuffleFallback(StreamingShuffleFallbackReason.ProtocolVersionMismatch,
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
      // A partition-count disagreement is a compatibility failure between the two ends of one
      // shuffle, detected by an explicit check rather than inferred from a parse error, which is
      // precisely what the documented version-mismatch condition covers. Declaring it under that
      // reason keeps the set of four trip conditions closed -- a fifth would be a fifth way for
      // streaming to be unavailable -- while still standing the shuffle down for every
      // participant, which is the only outcome that makes the recomputation reconcilable.
      declareShuffleFallback(StreamingShuffleFallbackReason.ProtocolVersionMismatch,
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
   *
   * <b>A producer whose task has finished is the ordinary case, not a failure.</b> Every consumer
   * of a map output connects after that output's task has ended, because the DAG scheduler submits
   * a reduce stage only once its map stage has finished. The subsystem is built for exactly that:
   * the serving endpoint is [[StreamingShuffleListener]], which is scoped to the executor rather
   * than to a task and outlives every map task that registers with it, and the bytes it serves are
   * the retained files [[StreamingShuffleBlockResolver]] owns, which outlive their task by the same
   * design. A completed producer is therefore connectable and streamable, and a connect is expected
   * to succeed on the happy path.
   *
   * <b>Why a refusal is retried rather than escalated at once.</b> The retained output this
   * consumer needs is served by an executor that is doing other work: its transport can be
   * momentarily out of accept capacity, its host briefly unreachable, or its serving handler
   * part-way through registration. Escalating the first refusal would recompute a map task whose
   * output is intact and seconds away from being servable, so the attempt is repeated on the
   * schedule every streaming retry uses -- [[BackpressureProtocol.MAX_RETRY_ATTEMPTS]] attempts
   * paced by [[BackpressureProtocol.retryBackoffMillis]], one second doubling to sixteen. The pause
   * is [[awaitQuietly]], so it calls no `Thread.sleep` and a completing task cuts it short rather
   * than sitting out the remainder of an interval.
   *
   * <b>Resumption belongs to the identity, not to this method.</b> The consumer identity announced
   * on every heartbeat is stable for the life of this task attempt, so a producer that has already
   * retained blocks for it answers a reconnected channel from the cursor that identity holds
   * instead of from the beginning. Nothing is replayed here because a refused connect delivered
   * nothing to replay; what a reconnection resumes is decided where the cursor lives.
   *
   * Once the attempts are spent, a failure to connect means what it says: the peer is unreachable.
   * It tells the coordinator to invalidate that producer generation, which withdraws the
   * generation's completion record along with it, and it names the producer's own `MapStatus`
   * address and map index, so the tracker removes that one dead map output and the scheduler
   * recomputes that one map task. If the recomputed producer can be streamed from, the retry
   * streams; if no producer of the shuffle can be resolved at all, the rendezvous exhausts and
   * declares a shuffle-wide fallback, and the retry after that runs on the sort-based path. Every
   * branch terminates in a completed job.
   */
  private def openProducerStream(location: StreamingShuffleProducerLocation): ProducerStream = {
    var attempt = 1
    var opened: Option[ProducerStream] = None
    // Time spent pausing between attempts is time the reduce task waited on its input, so it is
    // accumulated and reported to the standard fetch-wait metric in one call, the way the
    // rendezvous reports its own polling. Only the pauses are counted: a connect that succeeds
    // first time adds nothing, so the happy path's accounting is exactly what it was.
    var backoffWaitMillis = 0L
    while (opened.isEmpty && !cleanedUp.get() &&
        attempt <= BackpressureProtocol.MAX_RETRY_ATTEMPTS) {
      // A failure already latched on an I/O thread outranks a further attempt: spending the rest of
      // the budget on a read that has failed only delays the fetch failure it has already earned.
      errorNotifier.throwIfError()
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
        // This consumer withdrew, which is not evidence against the producer. Invalidating the
        // generation here would withdraw an intact map output and recompute a task for nothing, so
        // the generation is left alone and only this read fails.
        raiseFetchFailure(location, startPartition,
          s"The streaming shuffle consumer of shuffle $shuffleId partitions " +
            s"[$startPartition, $endPartition) was released while opening a channel to " +
            s"${location.hostPort}.")
      case None =>
        invalidateProducer(location, StreamingShuffleInvalidationReason.ConnectionTimeout,
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
   * The handler is per producer, because it owns that producer's sequence expectation, its
   * acknowledgement position and its receive window, and a fresh one is built per attempt. A
   * refused handler is closed at once, which returns whatever receive quota it charged to the
   * executor's shared budget; reusing a closed one would leave that quota accounted to a channel
   * that no longer exists. There is nothing to carry over, because a refusal delivered no bytes.
   *
   * The channel is created through the injected connector, so every path in this class is reachable
   * without depending on a particular transport being available.
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

  /**
   * Issues the in-progress block request for every partition on every producer.
   *
   * "In progress" is a statement about what the request does not require, and that is what makes
   * the feature a streaming one: it is issued without waiting for the map stage to report complete
   * output, and a producer answers it with whatever it has cut so far rather than only with a
   * finished partition. Whether any producer really is still producing when it arrives is decided
   * by the scheduler that submitted this task and not here; when one is, reduce-side work overlaps
   * map-side work, and when none is, the same request reads retained output without the index
   * lookup and whole-partition buffering the classic path needs. The request is not conditioned on
   * which case holds, because both are served by the same exchange.
   *
   * The protocol's heartbeat carries it, which means one message type serves both purposes -- it
   * names the partition this consumer wants and it proves the consumer is alive -- and the producer
   * needs no extra request type to learn either.
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
   * two times slower for more than sixty seconds" condition, and observed link usage for the
   * "saturation above ninety percent" condition. When no bandwidth cap is administered the policy
   * treats the utilisation sample as unevaluable, which is exactly right: absence of a cap means
   * unlimited, never zero.
   *
   * Buffer utilisation is deliberately not among the things reported. That reading is per shuffle
   * and last-writer-wins, its numerator is the producer buffer budget the spill threshold is
   * evaluated against, and this side holds a different budget entirely -- so a reader reporting its
   * own stash would not add to the picture, it would replace the producer's half of it, and a
   * producer genuinely at its eighty percent spill point would read as whatever the reader happened
   * to be holding. The invariant is stated once where this reader registers the shuffle, and this
   * is the method that would otherwise break it.
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
      // Measured ingress, not a summed ledger rate. A ledger rate is what this consumer has already
      // been paced to, so comparing it against the administered capacity would report successful
      // pacing as saturation; and it is per stream, whereas the link is shared by every concurrent
      // shuffle on this executor. The protocol measures what actually arrived, executor-wide, over
      // a stable interval, and that is the only honest numerator for a saturation ratio.
      fallbackPolicy.recordLinkUtilization(backpressure.ingressBytesPerSecond.toDouble)

      val sustainedSlow = sampled.exists(backpressure.isConsumerSustainedSlow)
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
   *
   * The coordinator is told rather than the producer, and that is not a shortcut: the generation's
   * other owners are on the producer's executor, this consumer has no channel to any of them, and
   * inventing one would let any peer retire another executor's output. Telling the one authority
   * both ends already talk to is what makes the invalidation reach them -- the producer refreshes
   * its registration on the connection-timeout cadence, discovers on its next refresh that the
   * driver no longer holds its generation, and retires its routing entry, its retained output, its
   * sessions and its spill files itself through the one withdrawal operation that owns all four.
   */
  private def invalidateProducer(
      location: StreamingShuffleProducerLocation,
      reason: StreamingShuffleInvalidationReason,
      detail: String): Unit = {
    try {
      // Bounded by the same deadline as every other coordinator ask, and for a sharper reason
      // here: this runs while a fetch failure is already being raised, so an unbounded wait would
      // hold the task thread inside its own failure path for the generic two-minute default.
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
   * The block-manager identity of a producer, for the fetch-failure report.
   *
   * It is the identity the producer's own `MapStatus` carries, taken from the location the
   * coordinator published, and it is emphatically '''not''' synthesized from the streaming
   * endpoint. The distinction is the whole point of the field.
   * `MapOutputTracker.unregisterMapOutput` removes a map output only when the address in the
   * failure equals the address in the recorded status, and a streaming producer serves blocks from
   * an ephemeral listener on a port that is nothing like its block manager's. An identity built
   * from that listener therefore matches nothing, the dead output stays registered, and the stage
   * recomputation the fetch failure exists to trigger never happens -- a fetch failure that
   * silently recovers nothing.
   *
   * `FetchFailedException` documents that this may be null, and `MetadataFetchFailedException`
   * passes null itself; this reader has the identity whenever it resolved a location, so it passes
   * it, and passes null only where no producer could be named at all.
   */
  private def blockManagerIdOf(location: StreamingShuffleProducerLocation): BlockManagerId = {
    location.blockManagerId
  }

  /**
   * Records a fully populated fetch failure against a named producer and raises it here, on the
   * calling thread, as the single failure of this read.
   *
   * <b>Why every producer-attributed failure goes through this one door.</b> A streaming read is
   * driven from the task thread but fed by Netty threads, so two failures can exist at once: one
   * latched asynchronously on the notifier, and one the task thread is about to raise. If the task
   * thread simply threw its own, three things would go wrong. The earliest failure -- the one that
   * actually explains the cascade -- would be discarded in favour of a later symptom. The notifier
   * would never learn of the fetch failure, so it would hold no scheduler-facing signal, and a
   * subsequent `throwIfError()` on any other advance of this read would surface some unrelated
   * channel exception in its place. And the choice of which failure a task reports would depend on
   * a race between an I/O thread and the task thread rather than on the protocol.
   *
   * Handing the exception to the notifier first removes all three. First-error-wins then decides
   * what propagates, and [[StreamingShuffleErrorNotifier.throwIfError]] re-asserts the fetch
   * failure on this thread's task context before it throws anything at all -- so the executor
   * reports a FetchFailed task-end reason, and the unmodified scheduler recomputes the upstream
   * stage, whichever of the two throwables finally propagates. The one that does not propagate is
   * retained as a suppressed exception on the one that does, so no diagnosis is lost.
   *
   * <b>Why assigning the exception is safe here.</b> `FetchFailedException`'s constructor registers
   * itself on the task context (SPARK-19276), so constructing one and then deciding not to raise it
   * would poison a task that went on to succeed. This method never makes that decision: the value
   * it constructs is either thrown by `throwIfError()` or thrown by the line after it. The trailing
   * throw is what makes the method's `Nothing` result type honest, and it is also a real safety
   * net, because [[StreamingShuffleErrorNotifier.setError]] deliberately absorbs any non-fatal
   * problem it hits while recording.
   *
   * @param location the producer the failure is attributed to, whose `MapStatus` address and map
   *                 index are what let `MapOutputTracker` remove the exact dead map output
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
      val client: TransportClient) {

    /** Whether this producer is this very executor, which decides local or remote accounting. */
    private val producerIsLocal: Boolean =
      Option(SparkEnv.get).map(_.executorId).contains(location.executorId)

    private val inboxes = new mutable.HashMap[Int, PartitionInbox]
    private val openStreams = new mutable.ArrayBuffer[DeserializationStream]
    private val closed = new AtomicBoolean(false)

    /**
     * Blocks this producer delivered for partitions this task does not read. Touched only by the
     * task thread, so a plain counter is enough, and it exists so that a misrouting is a number an
     * operator can see rather than a line per discarded block.
     */
    private var foreignBlocksDiscarded: Long = 0L

    /** The ledger identity of one partition arriving from *this* producer generation. */
    private[streaming] def ledgerKey(partitionId: Int): BackpressureStreamKey =
      consumerKey(location, partitionId)

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
        // Producer loss is converted into a fetch failure before the notifier is consulted, so a
        // transport error latched alongside the loss can never be what this task raises.
        convertSignalledProducerLoss(partitionId)
        errorNotifier.throwIfError()
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

    /**
     * Fails this read when streaming has stood down while this iterator was consuming.
     *
     * Evaluated at every block boundary of every partition, which is the only place this iterator
     * can either wait or make a block visible, and therefore the only place a decision taken while
     * it was blocked can be acted upon without either delivering data the shuffle has invalidated
     * or waiting out a producer timeout for blocks that will never be sent.
     *
     * Both of the policy's answers are consulted, exactly as at rendezvous: the coordinator's
     * latched verdict, and this executor's own observation of a trip condition -- which is declared
     * to the coordinator so the recomputation that follows lands on the sort-based path.
     *
     * The failure goes through [[failProducer]] rather than raising directly, so that everything
     * already taken from this producer is discarded atomically first. A fallback that left a
     * consumer holding half a stream would be the one situation in which streamed and recomputed
     * data could be mixed in one reduce task's input.
     */
    private def checkFallbackWhileReading(partitionId: Int): Unit = {
      val latched = fallbackPolicy.knownShuffleFallback(shuffleId).orElse {
        if (fallbackPolicy.hasTripped) {
          val reason =
            fallbackPolicy.trippedReason.getOrElse(StreamingShuffleFallbackReason.ConsumerTooSlow)
          Some(declareShuffleFallback(reason,
            s"a consumer reading partition $partitionId observed the condition on its own " +
              "executor while streaming"))
        } else {
          None
        }
      }
      latched.filter(_.fallenBack).foreach { state =>
        val description = state.reason.map(_.description).getOrElse(state.reasonName)
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
        case Some(ProducerLost(lostPartition, reason, cause)) =>
          val attributedPartition =
            if (servesPartition(lostPartition)) lostPartition else awaitedPartition
          failProducer(attributedPartition, StreamingShuffleInvalidationReason.ConnectionTimeout,
            reason, cause)
        case None =>
          checkProducerLiveness(awaitedPartition)
      }
      sampleFlowControl()
    }

    /**
     * Accepts one arrived block into its inbox, or discards it if it is not ours.
     *
     * <b>A block for a partition outside this task's range is dropped and never acknowledged.</b>
     * Acknowledging it was both contradictory and wrong. Contradictory, because the handler refuses
     * an acknowledgement for a partition this consumer never asked for, so the call could only ever
     * return false. Wrong, because an acknowledgement is the one message that advances a producer's
     * retained-window cursor and frees the buffers behind it, and the cursor it would advance
     * belongs to the reduce task that really does read that partition -- so a reducer confirming
     * another reducer's data is a reducer able to make that data unreadable. The producer loses
     * nothing by the silence: it keeps the block for the consumer that is entitled to it and
     * releases it on that consumer's own acknowledgement.
     *
     * Arriving at all is nevertheless an anomaly, since a producer sends a partition only to the
     * consumers that subscribed to it, so the first one is reported at warning level and counted.
     * The count is reported rather than the individual blocks, because a misrouting affects every
     * block of a partition and one line per block would be one line per two megabytes.
     *
     * Byte and block counts are recorded here, on arrival, because that is when the bytes crossed
     * the network. Locality is decided from the producer's executor id, so a producer that happens
     * to be this executor is accounted local exactly as the classic path would account it.
     */
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
        // Consulted last, so that a producer loss this poll window revealed is raised as the fetch
        // failure it is rather than as whatever transport error accompanied it.
        errorNotifier.throwIfError()
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
      releaseFromInbox(inbox, inbox.blocks.dequeue())
    }

    /**
     * Takes one specific sequence number out of an inbox, wherever it is sitting in the queue.
     *
     * <b>Why the position cannot be assumed.</b> Blocks are appended to an inbox in arrival order,
     * and a replay is requested only after the corrupt copy has already been taken off the head. By
     * the time the replayed copy arrives, the blocks the producer sent after the corrupt one may
     * well be queued ahead of it, because they were never lost -- only corrupted in one copy. A
     * replay of sequence n therefore commonly lands behind n+1, and inspecting only the head would
     * never see it: the repair would time out, the attempt budget would be spent, and a block that
     * did arrive intact would escalate to a stage recomputation.
     *
     * Extraction preserves the relative order of everything else, so the blocks left behind are
     * still in ascending sequence order and the caller's sequence gate continues to hold. A
     * duplicate copy of an already accepted position never reaches an inbox at all: the client
     * handler admits a below-expected sequence only while that position is quarantined, and the
     * quarantine is released when the replay is accepted.
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
     * arrives and refuses to enqueue a corrupt one, requesting the replay itself, so a mismatch
     * observed here is corruption that happened after that check. The repair is nevertheless able
     * to complete: asking the handler for a replay <i>quarantines</i> that position, and a
     * quarantined position is admitted when it arrives again instead of being discarded as a
     * duplicate. Without that the replay could never be observed here and this path could only ever
     * end in recomputation, which would make the retransmission the protocol mandates unreachable
     * for exactly the case it was written for.
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

    /**
     * Asks for one block to be sent again and waits for it, without sleeping.
     *
     * The wait is a bounded number of poll windows whose count is derived from the protocol's own
     * backoff for the stream, falling back to the standard exponential schedule from one second
     * when the protocol has no opinion. Expressing the delay as a count of poll windows rather than
     * as a clock deadline is what keeps the behaviour identical whichever clock is supplied, and
     * what keeps this path free of a sleep.
     *
     * The replay is looked for by sequence number rather than at the head of the inbox, because the
     * blocks that followed the corrupt one were not lost and are commonly queued ahead of it; see
     * [[dequeueBySequence]].
     */
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

    /**
     * Confirms that a terminated stream delivered everything it announced.
     *
     * A stream that ends short is the one failure a checksum cannot catch, because every block that
     * did arrive was intact. Comparing the announced total against what was consumed is what turns
     * a truncated partition into a fetch failure instead of a silently short result.
     */
    private def verifyStreamComplete(inbox: PartitionInbox): Unit = {
      val announced = handler.announcedBlockCount(inbox.partitionId)
        .orElse(backpressure.announcedBlockCount(ledgerKey(inbox.partitionId)))
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
        raiseFetchFailure(location, inbox.partitionId,
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

    /**
     * The incompatible protocol version an I/O thread saw on this channel, if any.
     *
     * Read from the handler, which is the one component that sees every frame's version byte before
     * anything is decoded from it, so there is a single authority for the answer and no callback to
     * keep in step with it.
     */
    private def incompatibleVersion: Option[Byte] = handler.incompatibleProtocolVersion

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
     * Order matters and is fixed: the discard happens first, so no byte taken from this producer
     * can still be reached by the time anything is reported, and only then is the failure raised.
     * That is what makes the invalidation atomic with respect to the failure -- a consumer can
     * never be left holding part of a stream whose producer has been declared lost, which is the
     * one situation in which streamed and recomputed data could be mixed in one reduce task's
     * input.
     *
     * The failure itself is raised through [[raiseFetchFailure]], which records it on the notifier
     * before throwing so that the scheduler-facing signal is asserted on this thread and is the
     * signal that propagates. An explicitly detected protocol version mismatch is preferred as the
     * reported cause when one was seen, because a version disagreement must trip the fallback
     * policy rather than be reported as a timeout.
     *
     * @param cause the transport-level throwable that revealed the loss, when the handler carried
     *              one on its marker. Attached to the fetch failure so the recomputation is
     *              diagnosable without having to correlate two exceptions.
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
          // same reason and with the same effect. Discovering the mismatch mid-stream rather than
          // at registration changes when it is learned, not what it means: every producer of this
          // shuffle speaks the version this one just announced, so a fetch failure alone would
          // recompute the upstream stage into producers that announce it again. Latching the trip
          // locally governs only this executor, and the log line below would otherwise be claiming
          // a fallback that no other participant had been told about.
          declareShuffleFallback(StreamingShuffleFallbackReason.ProtocolVersionMismatch,
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
     *
     * This is the ordering that makes producer loss recoverable. A channel failure is reported
     * twice by design -- as a marker on the hand-off queue, and as a raw throwable on the notifier
     * -- and only the first of those can be turned into an atomic per-producer invalidation plus a
     * fetch failure, because only the task thread can see everything this read has taken.
     * Consulting the notifier first therefore raised a transport error out of a situation the
     * scheduler could have recovered from, and the reduce attempt failed without its upstream stage
     * ever being recomputed.
     *
     * Nothing is done unless a failure has actually been observed, so the cost on the hot path is
     * one volatile read. When one has, every event already queued is processed in arrival order,
     * which is what keeps a producer that finished streaming and then dropped its channel from
     * being treated as lost: its terminations are ahead of its marker on the queue and are applied
     * first. The drain is bounded by the queue's capacity and never waits.
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
            case Some(ProducerLost(lost, reason, cause)) =>
              val attributed = if (servesPartition(lost)) lost else awaitedPartition
              failProducer(attributed, StreamingShuffleInvalidationReason.ConnectionTimeout,
                reason, cause)
            case None => draining = false
          }
        }
      }
    }

    /**
     * The same conversion for a caller that is not awaiting any particular partition.
     *
     * Attributed to the first partition this producer has not ended, because that is the partition
     * whose input a fetch failure is actually about. When every partition has ended there is
     * nothing left for this producer to lose and the call does nothing, which is what stops a
     * channel that failed while closing from failing a read that had already received everything.
     */
    def convertSignalledProducerLoss(): Unit = {
      partitionIds.find(partitionId => !inboxOf(partitionId).terminated)
        .foreach(partitionId => convertSignalledProducerLoss(partitionId))
    }

    /**
     * Releases this producer's channel, handler and buffers exactly once.
     *
     * Reached from the task-completion listener, so it runs on success, on failure and on
     * cancellation alike. `handler.close()` drains its queue and closes the channel; the client is
     * closed again here only because doing so is idempotent and because a handler that never saw a
     * channel active would otherwise leave the socket to the garbage collector. The client is an
     * unpooled one, so closing it releases this connection and no other reader's.
     *
     * @return the payload bytes released
     */
    def close(): Long = {
      if (closed.compareAndSet(false, true)) {
        val released = discardAll()
        if (client.isActive()) {
          client.close()
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
   * network. Against a producer that is still producing, that is the mechanism by which reduce-side
   * work overlaps map-side work; against retained output it is the mechanism by which a partition
   * larger than the receive window is consumed without ever being held whole.
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
      stream.convertSignalledProducerLoss(partitionId)
      errorNotifier.throwIfError()
      initialize()
      delegate.hasNext
    }

    override def next(): (Any, Any) = {
      stream.convertSignalledProducerLoss(partitionId)
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
 * Nothing here is wire-derived. Frame sizes, header lengths and framing overheads belong to the
 * protocol classes and are read from them at the point of use, never restated as a literal on this
 * side; the reader's own constants are its timing and retry bounds, its sentinel sequence numbers
 * and map identifiers, and one range predicate. The timings that must agree with the flow-control
 * subsystem are taken from [[BackpressureProtocol]] rather than written out again, for the same
 * reason: a change there must not leave a stale number here.
 */
private[spark] object StreamingShuffleReader {

  /** Denominator for every percentage this subsystem is configured with. */
  val PERCENT_SCALE: Long = 100L

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

  /**
   * Floor on the coordinator ask deadline, in milliseconds.
   *
   * The deadline is derived from a transport key an operator may set to anything, and a rendezvous
   * that timed out before the driver could plausibly answer would turn a healthy job into a
   * recomputation loop. One second is well below the subsystem's five-second bound and well above
   * the round trip to a driver that is merely busy.
   */
  val MIN_COORDINATOR_TIMEOUT_MS: Long = 1000L

  /**
   * How long the production connector waits for its transport to release, in milliseconds.
   *
   * The same five seconds every other deadline in this subsystem is measured in, so an executor
   * shutting down cannot be held for longer by streaming shuffle than a consumer would be held
   * waiting for a producer. It bounds the whole release rather than each channel individually.
   */
  val CONNECTOR_SHUTDOWN_TIMEOUT_MS: Long = BackpressureProtocol.ACK_TIMEOUT_MS

  /** Sequence numbers count from zero, one per data block, per partition stream. */
  val FIRST_SEQUENCE_NUMBER: Long = 0L

  /** Sentinel for "no block in hand", distinct from every legal sequence number. */
  val NO_SEQUENCE_NUMBER: Long = -1L

  /** Sentinel for "not yet measured", distinct from every legal clock reading. */
  val NO_TIMESTAMP: Long = -1L

  /** What `FetchFailedException` is given when no producer can be named, as its own doc allows. */
  val UNKNOWN_MAP_ID: Long = -1L

  /** What `FetchFailedException` is given for a map index a producer location cannot supply. */
  val UNKNOWN_MAP_INDEX: Int = -1

  /** `InputStream` end-of-stream marker. */
  val END_OF_STREAM: Int = -1

  /** Mask that promotes a signed byte to the unsigned value `InputStream.read` must return. */
  val UNSIGNED_BYTE_MASK: Int = 0xFF

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
 * One collaborator is deliberately <i>not</i> here: the first-error-wins notifier. Each reader
 * builds its own, because that bridge is scoped to one task attempt rather than to the executor.
 * See the field that builds it in [[StreamingShuffleReader]] for why a shared one would re-throw a
 * stranger's failure at every later reader on this executor.
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
 * @param connector opens channels to producers. Injected so no transport type reaches the reader's
 *                  own logic, and owned by the manager, which closes it in `stop()`
 * @param serializerManager applies the same compression and encryption wrapping to a streamed
 *                          payload that the classic path applies to a fetched block
 */
private[spark] case class StreamingShuffleReaderContext(
    coordinatorRef: RpcEndpointRef,
    backpressure: BackpressureProtocol,
    fallbackPolicy: StreamingShuffleFallbackPolicy,
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
   * Opens a channel to one producer with the given handler bound to it.
   *
   * The handler is an `RpcHandler`, so an implementation must route the channel's inbound one-way
   * messages to it and must raise its `channelActive`, `channelInactive` and `exceptionCaught`
   * callbacks. An incompatible protocol revision is not reported through this method: the handler
   * peeks every frame's version before decoding it and publishes the first unreadable one, so there
   * is one authority for the answer rather than a callback to keep in step with it.
   *
   * @param location the producer to reach
   * @param handler the consumer handler to bind; it becomes the owner of the returned client
   * @return the connected client, or `None` if it could not be established
   */
  def connect(
      location: StreamingShuffleProducerLocation,
      handler: StreamingShuffleClientHandler): Option[TransportClient]

  /** Releases any process-wide resource this connector holds. Must be idempotent. */
  def close(): Unit
}

/**
 * The production [[StreamingShuffleProducerConnector]]: a Spark transport client factory on a
 * transport configuration scoped to the streaming module.
 *
 * <b>Why the Spark transport and not a bootstrap of our own.</b> The producer side of this protocol
 * is an `RpcHandler` installed on a `TransportContext`, so every streaming frame travels as the
 * body of a one-way message on Spark's own transport. Building the consumer side on the same
 * transport is what makes the two ends one protocol: the length prefix, the message encoding, the
 * optional encryption and the frame accounting are the transport's in both directions, and a
 * hand-rolled client pipeline could only ever be a second implementation of them to keep in step.
 * It also means this connector inherits, rather than reimplements, everything the platform already
 * does for a shuffle connection -- connection creation timeouts, idle detection, pooled allocation
 * and the bootstrap hooks through which authentication and TLS are installed.
 *
 * <b>Why this class is itself the `RpcHandler`.</b> A `TransportContext` is constructed with one
 * handler, while a reduce task needs one handler per producer, because sequence expectation,
 * acknowledgement position and receive window all belong to a producer generation rather than to a
 * shuffle. This connector therefore holds a single context and demultiplexes: it registers the
 * per-producer handler against the channel as the connection is created and forwards every callback
 * the transport makes to the handler that owns that channel. Doing it here rather than in a class
 * of its own keeps the mapping in the one place that creates and destroys the channels it is keyed
 * by, so a handler cannot outlive its registration or vice versa.
 *
 * <b>Why the clients are unmanaged.</b> `createClient` pools connections per peer, which would have
 * two reduce tasks reading from the same executor share one channel -- and with it one handler, one
 * sequence expectation and one receive window. `createUnmanagedClient` gives each producer stream a
 * connection of its own, which is what makes per-producer accounting correct and what lets a reader
 * close its own connection at task completion without disturbing another task's.
 *
 * <b>Why a module of its own.</b> The configuration comes from
 * `StreamingShuffleServerHandler.streamingTransportConf`, the same builder the producer side uses,
 * which yields an independent `spark.shuffle-streaming.io.*` namespace with OS keepalive enabled.
 * Thread counts, buffer sizes and retry behaviour can therefore be tuned for streaming without
 * perturbing the block transfer service that every other shuffle depends on, and producer and
 * consumer cannot be configured differently. No shared transport class is modified to achieve any
 * of it: `TransportContext`, `TransportConf` and `SparkTransportConf` are consumed exactly as they
 * stand.
 *
 * <b>Keepalive.</b> OS keepalive is the coarse safety net beneath the protocol's own timer, never a
 * substitute for it: the platform exposes keepalive as a boolean and the JDK offers no keepalive
 * <i>interval</i> as a socket option, so the five-second liveness bound this subsystem promises is
 * enforced at application level by the heartbeat.
 *
 * @param conf the executor configuration the streaming transport namespace is read from
 * @param clock the time source the shutdown deadline is measured on, injected for the same reason
 *              the reader's is: a suite that has to observe a straggling channel must be able to
 *              reach the deadline by advancing a clock rather than by waiting out the full window
 */
private[spark] class NettyStreamingShuffleProducerConnector(
    conf: SparkConf,
    clock: Clock = new SystemClock)
  extends RpcHandler with StreamingShuffleProducerConnector with Logging {

  private val transportConf: TransportConf =
    StreamingShuffleServerHandler.streamingTransportConf(conf)

  private val transportContext: TransportContext = new TransportContext(transportConf, this)

  /**
   * The bootstraps every channel this connector opens completes before a frame is exchanged.
   *
   * Empty only when the application itself has authentication disabled. When it is enabled this
   * carries the platform's own auth handshake, which is what makes the channel's identity the
   * capability that admits a consumer to a producer's output. It matters more here than it does for
   * a block fetch: a streaming frame's payload goes straight into Spark's deserialization, and the
   * per-block CRC32C detects corruption rather than forgery, so the channel is the only place the
   * question "may this peer send me bytes to deserialize" can be answered. `createClientFactory`
   * runs them synchronously on the connecting thread, so a client is handed back already
   * authenticated -- there is no window in which an unauthenticated channel could deliver anything.
   */
  private val clientBootstraps: java.util.List[TransportClientBootstrap] =
    StreamingShuffleServerHandler.streamingClientBootstraps(conf, transportConf)

  private val clientFactory: TransportClientFactory =
    transportContext.createClientFactory(clientBootstraps)

  /**
   * The consumer handler that owns each live channel, keyed by the channel's own identity.
   *
   * Keyed by channel rather than by producer, because a producer that is reconnected to gets a new
   * channel and must not inherit the receive window of the channel it replaced. The entry is placed
   * while the connection is being created and removed when the channel goes inactive, so a callback
   * can never reach a handler whose reader has finished with it.
   */
  private val handlers = new ConcurrentHashMap[String, StreamingShuffleClientHandler]()

  /**
   * The handler of the connection currently being created on this thread.
   *
   * The transport builds the pipeline before it hands the client back, so there is no point at
   * which a caller could register a handler against a channel that is guaranteed not to have
   * delivered anything yet. Publishing the handler on the connecting thread closes that window: the
   * registering bootstrap below runs synchronously, on this thread, inside `createUnmanagedClient`,
   * and a frame arriving before it finds the handler here. A thread local rather than a field
   * because several reduce tasks share one connector and connect concurrently.
   */
  private val connecting = new ThreadLocal[StreamingShuffleClientHandler]()

  /**
   * The unmanaged client of each live channel, keyed the same way as [[handlers]].
   *
   * Held because every client this connector creates is unmanaged and therefore absent from the
   * factory's connection pool: closing the factory would not close them, so shutting down cleanly
   * requires this connector to know which sockets it opened. The entry's life is exactly the
   * handler's -- placed as the connection is created, removed when the channel goes inactive -- so
   * this map cannot pin a channel the reader has already finished with.
   */
  private val clients = new ConcurrentHashMap[String, TransportClient]()

  /** Channels whose frames arrived before any handler was bound to them. Diagnostic only. */
  private val unboundFrames = new AtomicLong(0L)

  /** Channels that were still open when the shutdown deadline passed. Zero until [[close]] runs. */
  private val unreleasedChannelCount = new AtomicInteger(0)

  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  private val closed = new AtomicBoolean(false)

  override def connect(
      location: StreamingShuffleProducerLocation,
      handler: StreamingShuffleClientHandler): Option[TransportClient] = {
    if (closed.get()) {
      logWarning(log"Refusing to open a streaming shuffle channel to " +
        log"${MDC(HOST_PORT, location.hostPort)} because this connector is closed")
      None
    } else {
      connecting.set(handler)
      try {
        val client = clientFactory.createUnmanagedClient(location.host, location.port)
        bind(client, handler)
        // The transport raises channelActive from the pipeline before the client is returned, which
        // is earlier than the binding above on a channel that connected before this thread resumed.
        // Announcing here as well is idempotent -- the producer treats a repeat subscription as a
        // no-op -- and it is what guarantees the in-progress request is issued exactly once per
        // connection however the two orderings interleave.
        handler.channelActive(client)
        Some(client)
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
        connecting.remove()
      }
    }
  }

  /**
   * Releases the transport this connector owns, once, and does not return until it is released or a
   * bounded deadline has passed.
   *
   * The order is forced by what each step depends on. The handlers go first, so nothing is still
   * trying to write when the socket beneath it disappears. The channels go next and are *awaited*:
   * every client this connector opened is unmanaged and therefore invisible to
   * `TransportClientFactory.close`, so closing the factory alone would leave them open, and
   * `Channel.close` is asynchronous, so requesting it without awaiting the close future is a
   * request rather than a release. The factory goes third, which asks its event loop to wind down.
   * The context goes last, releasing the encoder, decoder and any TLS material.
   *
   * <b>Why the event loop is awaited, and why through the channel.</b>
   * `TransportClientFactory.close` calls `shutdownGracefully` and returns immediately, so on its
   * own it proves nothing about the threads it asked to stop -- and an executor that reported clean
   * shutdown while a transport thread still held a direct buffer would misattribute the leak to
   * whatever ran next. The group is reached through `Channel.eventLoop().parent()`, which is the
   * public route from a channel this connector created to the group it was created on; the factory
   * does not expose the group, and no shared transport class is modified to make it do so.
   *
   * <b>Evidence rather than an exception.</b> This runs inside `StreamingShuffleManager.stop()`,
   * usually while the executor is shutting down. Throwing there would abort an orderly shutdown
   * over a resource the JVM is about to reclaim anyway, so a straggler is reported instead: counted
   * on [[unreleasedChannels]] for the owner to read, and logged as a warning naming the module, how
   * many channels did not close and whether the event loop terminated. Silence is the one outcome
   * that is not acceptable, because a leak nobody records is a leak nobody fixes.
   */
  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      val bound = handlers.size()
      handlers.values().asScala.foreach { handler =>
        try {
          handler.close()
        } catch {
          case NonFatal(e) =>
            logWarning(log"A streaming shuffle consumer handler failed to close: " +
              log"${MDC(ERROR, e.getMessage)}")
        }
      }
      handlers.clear()
      val channels = liveChannels()
      val group = channels.flatMap(channel => Option(channel.eventLoop())
        .flatMap(loop => Option(loop.parent()))).headOption
      val straggling = closeChannels(channels)
      clients.clear()
      clientFactory.close()
      transportContext.close()
      val terminated = awaitEventLoopTermination(group)
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
    }
  }

  /**
   * How many channels this connector could not confirm closed, once it has stopped.
   *
   * Zero before [[close]] runs and zero after a clean release. A non-zero reading is the evidence
   * the owner's shutdown path reports; it is deliberately a count rather than a flag, because the
   * number of sockets left behind is what distinguishes one slow peer from a systematic leak.
   */
  def unreleasedChannels: Int = unreleasedChannelCount.get()

  /** The channels this connector still holds, snapshotted before the factory is closed. */
  private def liveChannels(): Seq[Channel] = {
    clients.values().asScala.toSeq.flatMap(client => Option(client.getChannel()))
  }

  /**
   * Closes every channel and waits, within one shared deadline, for each to confirm it.
   *
   * The deadline is shared rather than per channel so that shutdown is bounded by
   * [[CONNECTOR_SHUTDOWN_TIMEOUT_MS]] however many channels are open: a per-channel wait would
   * multiply the bound by the number of producers a reduce task happened to be reading from.
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
   * `None` -- no channel was ever opened, so there is no group to wait for -- counts as terminated,
   * because a group that never ran cannot be leaking a thread.
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
   * one. The transport dereferences it unconditionally when a stream request arrives, so a real,
   * empty instance -- answering "no such stream" -- is the correct response to a request this
   * subsystem never invites.
   */
  private val emptyStreamManager: StreamManager = new OneForOneStreamManager()

  override def getStreamManager(): StreamManager = emptyStreamManager

  override def receive(client: TransportClient, message: ByteBuffer): Unit = {
    dispatchTo(client)(_.receive(client, message))
  }

  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    dispatchTo(client)(_.receive(client, message, callback))
  }

  override def channelActive(client: TransportClient): Unit = {
    dispatchTo(client)(_.channelActive(client))
  }

  /**
   * Forwards the loss of a channel and forgets it.
   *
   * The registration is removed here and only here, so a handler is reachable for exactly as long
   * as the channel it owns can deliver anything.
   */
  override def channelInactive(client: TransportClient): Unit = {
    val key = channelKeyOf(client)
    clients.remove(key)
    val handler = Option(handlers.remove(key)).orElse(currentlyConnecting)
    handler.foreach(_.channelInactive(client))
  }

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    dispatchTo(client)(_.exceptionCaught(cause, client))
  }

  /**
   * Routes one transport callback to the handler that owns the channel it arrived on.
   *
   * A callback for a channel with no handler is counted rather than raised. It can only happen if a
   * producer sends before this consumer has subscribed, which the producer side does not do, and
   * the consequence is a frame the sequence-gap repair will ask for again -- whereas raising here
   * would fail a reduce task over a frame nobody asked for.
   */
  private def dispatchTo(client: TransportClient)(
      action: StreamingShuffleClientHandler => Unit): Unit = {
    Option(handlers.get(channelKeyOf(client))).orElse(currentlyConnecting) match {
      case Some(handler) =>
        action(handler)
      case None =>
        val unbound = unboundFrames.incrementAndGet()
        if (unbound == 1L) {
          logWarning(log"A streaming shuffle frame arrived from " +
            log"${MDC(HOST_PORT, String.valueOf(client.getSocketAddress()))} on a channel " +
            log"with no consumer handler bound to it. Further occurrences are counted but " +
            log"not logged")
        }
    }
  }

  /** The handler of the connection this thread is in the middle of creating, if any. */
  private def currentlyConnecting: Option[StreamingShuffleClientHandler] =
    Option(connecting.get())

  /** Binds one handler to one channel for the life of that channel. */
  private def bind(client: TransportClient, handler: StreamingShuffleClientHandler): Unit = {
    val key = channelKeyOf(client)
    handlers.put(key, handler)
    clients.put(key, client)
  }

  /**
   * The identity of one channel.
   *
   * Netty's channel id is unique for the life of the JVM, so it distinguishes a reconnection from
   * the connection it replaced even when both name the same peer -- which a socket address alone
   * would not.
   */
  private def channelKeyOf(client: TransportClient): String = {
    val channel = client.getChannel()
    if (channel == null) String.valueOf(client.getSocketAddress()) else channel.id().asLongText()
  }
}
