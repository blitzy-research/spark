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
 * The bundle exists for a reason that is worth stating rather than leaving to be inferred. Passing
 * the six collaborators individually, alongside the five arguments every shuffle writer needs, puts
 * the constructor at thirteen parameters, and scalastyle's `argcount` check caps a parameter list
 * constructors included -- at ten. Bundling is therefore not a stylistic preference but the
 * mechanism by which this writer stays inside a machine-enforced limit, and it buys two further
 * properties: the manager assembles the streaming subsystem once and hands the same shape to every
 * writer it creates, and one collaborator can be substituted without restating the rest.
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
 *  - `blockResolver` is executor scoped, and deliberately so: it is the registry through which a
 *    map output's retained blocks outlive the task that produced them. A reduce task may still be
 *    reading a partition long after the producing task has finished, so the component that owns the
 *    binding from a map output to its retained bytes cannot itself be per task.
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
 * moment it creates a writer.
 *
 * Two driver-facing operations are nonetheless the writer's own, and they arrive as
 * `coordinatorGateway` rather than as an `RpcEndpointRef`. Only the writer observes that this map
 * output has been streamed in full, and only the writer observes a fallback trip while producing,
 * so only the writer can report either at the moment it becomes true. Taking them as an abstraction
 * rather than as a transport keeps this writer free of `RpcEnv`, so the whole of the
 * consumer-failure and graceful-degradation flow is driven by a clock and an interface rather than
 * by a live cluster.
 *
 * @param backpressure executor-wide flow control: credit, liveness timers and arbitration
 * @param rateLimiter executor-wide egress pacing, consulted here only for its framing ceiling
 * @param spillManager this task's buffer budget, retention window and disk spill
 * @param blockResolver the executor-scoped registry that publishes this task's retained output, so
 *                      that egress and replay can read a block whether it is in memory or spilled
 * @param serverHandler this task's egress path: framing, checksums, pacing and the wire
 * @param fallbackPolicy the four graceful-degradation trip conditions
 * @param errorNotifier the first-error-wins bridge from Netty threads to the task thread
 * @param coordinatorGateway the driver-facing operations only a producer can perform: turning a
 *                           local fallback trip into a shuffle-wide decision, and reporting that
 *                           this map output has been streamed to completion. Executor scoped,
 *                           because it holds nothing but the shuffle's capability token and a
 *                           reference to the driver endpoint
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
 * This is the producer half of the streaming shuffle. Where
 * [[org.apache.spark.shuffle.sort.SortShuffleWriter]] sorts a task's records, writes them to a
 * local data file and publishes an index so that reduce tasks may fetch them once the whole map
 * stage has finished, this writer frames records into small blocks and pushes each block to the
 * consumers subscribed to its partition as soon as it is cut. Nothing waits for the task's last
 * record, and nothing reaches disk on the path a subscribed consumer keeps up with.
 *
 * ==Where the overlap comes from, and what bounds it==
 *
 * This writer streams to whichever consumers are subscribed, and it does so through one code path
 * regardless of when they subscribed: a block is cut, admitted to the retained store, enqueued on
 * the egress handler and drained to every subscribed session. Nothing in that path asks whether the
 * producing task is still running, which is why the same drain serves a consumer that attached
 * during production and one that attached afterwards, and why the amount of this output that
 * reaches disk is decided by what consumers took rather than by any decision made here.
 *
 * '''That a subscribed consumer is served live is a machine-checked fact.''' Twice over, at two
 * levels. `StreamingShuffleWriterSuite`, "a subscribed consumer is served while the map task is
 * still producing": a consumer attached before `write` begins receives decoded data frames from
 * inside the record iterator, strictly before the last record is handed over, and the producer
 * accounts those bytes as written to a live channel. And end to end, through the production
 * manager, reader, coordinator rendezvous and transport, `StreamingShuffleIntegrationTest`, "a
 * consumer attached during production is served live and nothing is materialised whole": the
 * producer there PAUSES until the consumer has consumed a record, so an implementation that
 * delivered only at the stop could never reach its own stop and the wait would expire. That case
 * also bounds the durability claim numerically -- a consumer that kept pace leaves under a tenth
 * of the streamed bytes to be made durable. Deferring delivery to the stop fails both tests.
 *
 * '''What decides whether a consumer is subscribed is not this subsystem.''' Task submission
 * belongs to the DAG scheduler and to task scheduling, and both are absolute preservation zones for
 * this feature -- AAP 0.2.1 and 0.2.2 forbid modifying the DAG scheduler, the task lifecycle or
 * task scheduling algorithms, and AAP 0.8.2 Tier 1 restates the prohibition file by file. That
 * scheduler submits a stage only once every parent stage reports its output available. So for a
 * shuffle whose map tasks each run once, the reduce tasks reading it are submitted after the map
 * stage has finished, and no consumer is subscribed while this writer runs. What the streaming path
 * removes in that configuration is the sort, the index-and-fetch round trip and the reduce side's
 * whole-partition materialisation. No change confined to the shuffle abstraction -- which AAP 0.2.1
 * fixes as the entire modification scope -- can make the scheduler submit a consumer earlier, so
 * that remaining gap is a scheduler property and is recorded here rather than worked around.
 *
 * Two consequences follow, and both are asserted rather than assumed -- see
 * `StreamingShuffleWriterSuite`, "disk is untouched while a producer streams within its budget":
 *
 *  - '''While this writer runs, nothing of a within-budget map output reaches disk.''' Blocks live
 *    in the retained store and are evicted only when utilisation crosses the configured spill
 *    threshold, which is the specified memory model rather than a materialisation.
 *  - '''The one unavoidable write is at the stop, and only of what no consumer took.''' Buffered
 *    blocks are task-managed execution memory, which the executor reclaims when the task ends, so
 *    output that must outlive its producer has to be somewhere that outlives a task. A partition
 *    whose consumer kept pace has nothing retained and nothing written; a partition nobody read is
 *    written whole, because the alternative is to lose it.
 *
 * The overlap is exercised whenever a consumer is in fact attached: a reduce attempt reading while
 * a superseded or speculative map attempt is still producing, a consumer resuming after a channel
 * loss, and every shuffle under a scheduler that submits consumers earlier. In that case this
 * writer streams to it with no disk involved at all, and the durability step at the stop writes
 * only what was not taken.
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
 *  - Payload bytes are '''compressed and encrypted exactly once per partition''', by the single
 *    `SerializerManager.wrapStream` this writer opens for that partition's `ShuffleBlockId`. A
 *    codec frame therefore straddles block boundaries freely, which is legal precisely because no
 *    block is decoded on its own: the consumer concatenates first and unwraps once. Wrapping per
 *    block instead would be the thing that breaks, since a codec's framing spans blocks.
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
 * deterministic rather than flaky. There is no exception to it, the service time reported to
 * [[ShuffleWriteMetricsReporter.incWriteTime]] included: that is measured with the clock's own
 * monotonic reading rather than by calling the JVM's nanosecond timer directly, so the figure an
 * operator reads out of task metrics comes from the same source as every other timing here.
 *
 * @param handle the streaming registration produced by `registerShuffle`
 * @param mapId this map task's identifier, as the scheduler knows it
 * @param context this task's context, the source of both attempt attributes and cleanup
 * @param writeMetrics the reporter supplied by [[org.apache.spark.shuffle.ShuffleWriteProcessor]],
 *                     which is this task's `shuffleWriteMetrics`
 * @param conf the executor's configuration, read once here and then held immutably
 * @param components the streaming collaborators, owned by [[StreamingShuffleManager]]
 * @param sortShuffleWriter builds the sort-based writer for this same handle, map id, task context
 *                          and reporter. Invoked at most once, and only before the first record has
 *                          been consumed, when [[prepareStreaming]] finds streaming unviable for
 *                          this attempt: the whole record iterator is then handed to it so the
 *                          attempt completes on the unmodified sort-based path instead of failing
 * @param clock the time source for every elapsed-time decision, the shuffle write time reported
 *              to the metrics reporter included
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
    sortShuffleWriter: () => ShuffleWriter[K, V],
    clock: Clock = new SystemClock)
  extends ShuffleWriter[K, V] with Logging {

  import StreamingShuffleWriter._

  /**
   * The ledger identity of one partition of this map output, as seen from the sending end.
   *
   * The producing generation -- this map id together with this task attempt id -- is part of the
   * identity because a speculative copy or a retry of the same map task produces the same shuffle
   * and the same partitions, and is nonetheless a separate flow with its own window of
   * unacknowledged bytes. Keying without it would let a stale attempt's completion release the live
   * ledger of the attempt that superseded it.
   */
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
  // identity exactly. Checked rather than documented, because a silent disagreement would surface
  // only as a stream that is never paced and a replay that is always refused: admission for an
  // unregistered stream fails open, so nothing would fail loudly and the memory bound would simply
  // stop applying. Placed after the identity fields above, because a check that reads a field
  // before its initialiser has run compares against a zero and passes for the wrong reason.
  require(serverHandler.shuffleId == shuffleId && serverHandler.mapId == mapId &&
    serverHandler.taskAttemptId == context.taskAttemptId(),
    s"The streaming shuffle egress handler was built for shuffle ${serverHandler.shuffleId} map " +
      s"${serverHandler.mapId} attempt ${serverHandler.taskAttemptId} but this writer produces " +
      s"shuffle $shuffleId map $mapId attempt ${context.taskAttemptId()}.")

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
   * normalised here once instead of guarded at every division. No record can reach a partition that
   * does not exist: `appendRecord` bounds-checks against [[declaredPartitions]], not against this.
   */
  private val partitionDivisor: Int = math.max(1, declaredPartitions)

  // Configuration, read exactly once (G5). Holding these immutably is what makes "streaming shuffle
  // configuration changes require an executor restart" true by construction rather than by promise,
  // and is why no dynamic reconfiguration path exists or is needed.

  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  private val bufferSizePercent: Int = conf.get(config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)

  private val spillThresholdPercent: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)

  // Task attributes

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

  /**
   * The compression and encryption wrapper both ends of a streamed partition must agree on.
   *
   * This is not an optimisation, it is the framing contract. `StreamingShuffleReader` wraps the
   * concatenation of a partition's block payloads with `SerializerManager.wrapStream` for the same
   * [[org.apache.spark.storage.ShuffleBlockId]] before deserialising it, so a producer that
   * serialised straight into its accumulator would hand the consumer a raw stream to decompress --
   * which fails immediately with `spark.shuffle.compress` at its default of true, and fails
   * silently in the worse case where a codec happens to tolerate the leading bytes. Wrapping here,
   * under the same block identity, is what makes the two symmetric: the same
   * `spark.shuffle.compress` decision, the same codec, and the same I/O encryption key on both
   * sides, derived rather than agreed.
   */
  private val serializerManager: SerializerManager = SparkEnv.get.serializerManager

  // Memory budget (FR-2, FR-5)

  /**
   * The largest block payload this writer will ever frame, in bytes.
   *
   * Four ceilings apply and the smallest wins, because a block that exceeds any one of them can
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
   * '''Why the pipeline depth is multiplied by the executor's task-slot count.''' The per-partition
   * allowance is the aggregate budget divided by the partition count and nothing else, which is the
   * quotient this feature specifies -- but the budget itself is executor wide, and this writer is
   * one of as many concurrent map tasks as the executor has task slots. A framing accumulator is
   * held for as long as its partition is producing and cannot be spilled, so dividing only by the
   * partition count gives every task a share sized as though it were the only one: `slots` tasks
   * each holding `numPartitions * perPartitionAllowance / depth` claim `slots / depth` of the whole
   * allowance in accumulators alone, which at four task slots and a depth of four is 100% of it
   * with nothing left for a single buffered block. Including the slot count makes the aggregate
   * framing reservation `1 / depth` of the budget no matter how many tasks run, which leaves the
   * remaining `(depth - 1) / depth` for blocks -- and blocks, unlike accumulators, can be evicted
   * to disk.
   *
   * Zero is a legitimate answer, and is not an error: it means the configured budget cannot
   * accommodate even one block for one partition, so streaming is not viable for this shuffle on
   * this executor. [[prepareStreaming]] reads it before a single record has been consumed and
   * degrades the whole attempt to the sort-based writer, which is the terminus this feature
   * specifies for the memory-pressure trip condition.
   *
   * Resolved lazily because it reaches through `SparkEnv` into the memory manager, and forced by
   * [[prepareStreaming]] so that the derivation is logged once per task and any refusal is acted on
   * before a single record has been buffered.
   */
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
   * The framing capacity this attempt actually uses: the derived one, unless a smaller one had
   * to be negotiated to fit the allowance.
   *
   * [[prepareStreaming]] settles this once, before the first record, and it does not move again --
   * so every accumulator this task allocates, every block it cuts and every share it draws speak
   * about the same number, which keeps the framing reservation exact rather than optimistic.
   */
  private def blockPayloadCapacity: Int =
    if (negotiatedCapacityBytes > 0) negotiatedCapacityBytes else derivedBlockPayloadCapacity

  /**
   * The divisor applied to a partition's allowance to obtain one block's framing capacity: the
   * pipeline depth this writer reserves, multiplied by the number of map tasks that can hold a
   * framing accumulator at the same time in this JVM.
   *
   * Both factors are constants of the executor rather than readings that move with load, so the
   * capacity a task frames to is stable for its whole life -- which is what lets the framing
   * reservation be taken once, up front, instead of partition by partition as records arrive.
   */
  private def framingShareDivisor: Long =
    TARGET_PIPELINE_DEPTH.toLong * math.max(1, spillManager.concurrentTaskSlots).toLong

  /**
   * The whole framing reservation this attempt takes before it consumes a record: one accumulator's
   * worth of heap for every partition the shuffle declares.
   *
   * Sized for every declared partition rather than for the partitions this task turns out to touch,
   * because the point of taking it up front is that no later reservation can be refused. A task
   * that writes to a subset of the partitions therefore holds a little more than it needs for as
   * long as it runs, and that is the deliberate trade: reserving lazily at each partition's first
   * record would instead let an ordinary wide shuffle fail a map task part way through, with the
   * records it had already consumed unrecoverable and its retry the only remedy.
   */
  private def framingEnvelopeBytes: Long =
    accumulatorFootprintBytes * declaredPartitions.toLong

  /** Base credit-ledger metadata reserved before records can open any partition stream. */
  private def producerLedgerEnvelopeBytes: Long =
    BackpressureProtocol.STREAM_LEDGER_BASE_BYTES * declaredPartitions.toLong

  // Mutable state. Every field below is touched only from the task thread.

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

  /**
   * The trip condition that stood this attempt's streaming down mid-write, once one has.
   *
   * Latched rather than thrown onwards, because a mid-write trip is a degradation and not a
   * failure: it withdraws this attempt's void output and completes the task, so the map stage is
   * recomputed on the sort-based path through an ordinary fetch failure rather than by consuming a
   * task attempt. `None` for every attempt that streamed to completion and for every attempt that
   * delegated before consuming a record, which are the two ordinary outcomes.
   */
  private var standDownReason: Option[StreamingShuffleStandDownCause] = None

  /** Operator-facing description of why streaming stood down mid-write, or `null` if it did not. */
  private var standDownDetail: String = null

  /**
   * Whether this writer's release has been registered on the task's completion listener.
   *
   * Registration is idempotent because it is made from the earliest point at which this writer can
   * hold anything -- [[prepareStreaming]], before the framing reservation is taken -- and again
   * from [[publishStreamingProducer]], the point at which the release has work to do. Installing it
   * only at the later of the two would leave a window in which a reservation existed with no hook
   * to return it, and a task killed inside that window would report a retained-memory failure
   * instead of the cancellation that caused it.
   */
  private var cleanupInstalled: Boolean = false

  /**
   * The sort-based writer this attempt handed itself to, if streaming turned out not to be viable
   * for it.
   *
   * Set by [[degradeToSortShuffle]] '''before the first record is consumed''' and never afterwards,
   * which is the whole point of it: the record iterator can be passed on intact, so the attempt
   * produces a complete map output through the unmodified sort-based path and succeeds. Every
   * member of the writer contract consults this first and forwards to it, so the delegation is
   * total rather than partial -- an attempt whose records were split between two shuffle
   * implementations would produce output no reduce-side read path could reassemble.
   */
  private var sortDelegate: ShuffleWriter[K, V] = null

  /**
   * The shuffle-wide stand-down this attempt has observed while producing, if it has observed one.
   *
   * Recorded rather than raised, which is the whole of how runtime degradation was made independent
   * of the retry budget. [[checkFallbackPolicy]] runs at every block boundary and can only set
   * this; the record loop in [[write]] reads it and hands the attempt to [[finishByDegrading]],
   * which finishes the map output through the sort-based writer. Raising instead would make the
   * delegation depend on a retry, and with `spark.task.maxFailures` at one -- which a plain
   * `local[n]` master forces, and which is a legitimate cluster setting besides -- there is no
   * retry to depend on, so a condition specified to degrade would have aborted the stage.
   */
  private var pendingDegradation: Option[StreamingShuffleWriter.MidStreamDegradation] = None

  /**
   * Bytes of the up-front framing reservation that no partition has drawn yet.
   *
   * [[prepareStreaming]] reserves one accumulator's worth for every declared partition and this
   * counts down as partitions open theirs, so [[stateFor]] never has to ask the budget for room
   * mid-write. Whatever is left when the writer closes is returned along with the drawn shares, so
   * the total released always equals the total reserved.
   */
  private var framingEnvelopeRemaining: Long = 0L

  /** Pre-reserved producer-ledger metadata not yet transferred to an opened stream. */
  private var producerLedgerMetadataRemaining: Long = 0L

  /**
   * How far the stop protocol has progressed.
   *
   * A single "already stopping" flag cannot express the sequence [[stop]] has to survive, which is
   * why this is a state; the protocol and the reason each state is distinguished are documented
   * there rather than here.
   */
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

  private var reclamationBreaches: Long = 0L

  private var memoryPressureBrushes: Long = 0L

  // Blocks this attempt could not buffer and wrote straight to local disk instead. Counted so the
  // summary can report that the allowance was met and spilled past rather than silently absorbed.
  private var durableAdmissionsObserved: Long = 0L

  // The framing capacity negotiated down to fit the allowance, or zero while the derived one
  // stands. Settled by [[prepareStreaming]] before the first record and never changed afterwards.
  // Zero rather than a negative sentinel deliberately: zero is also this field's value before its
  // initialiser runs, so a read from anywhere earlier than that can only mean "use the derived
  // capacity" and can never be mistaken for a negotiated one.
  private var negotiatedCapacityBytes: Int = 0

  // Monotonic high-water marks of what has already been forwarded to the task's metrics reporter.
  // Only the increment since the last publication is forwarded, which is what lets progress made on
  // an I/O thread be reported from the task thread without ever double-counting.
  private var publishedBytesWritten: Long = 0L

  private var publishedRecordsWritten: Long = 0L

  // ShuffleWriter contract

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
    // Settled before a single record is consumed, so a refusal can still hand the iterator on
    // intact. Everything after this point assumes streaming is viable for this attempt.
    if (!prepareStreaming()) {
      sortDelegate.write(records)
      finished = true
      return
    }
    try {
      initializeStreaming()
      // The loop yields as soon as a shuffle-wide stand-down has been observed, because from that
      // moment on the correct thing to do with the remaining records is to hand them, together with
      // the ones already consumed, to the sort-based writer. Nothing is discarded: the tail of this
      // iterator has not been touched, and everything ahead of it is reconstructed from the
      // retained blocks it was framed into.
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
        // Recovery is reached the way the specification requires, through an ordinary fetch failure
        // that the unmodified scheduler resolves. See [[completeStandDown]].
        completeStandDown(signal)
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
   * <b>Why the guard is a state and not a flag.</b> The authoritative caller invokes
   * `stop(success = true)` on the happy path and `stop(success = false)` from a catch block that
   * covers everything after the writer was created -- which includes a failure raised *by* the
   * successful stop and a failure raised *after* it returned. A single "already stopping" boolean
   * answers all of those the same way, and that answer is wrong for two of them. A success stop
   * that threw part way through would have the failure cleanup refused, leaving a generation
   * published that no consumer can read to completion; and a task that failed after a successful
   * stop would leave its output reachable although the attempt did not succeed. So the states are
   * distinguished, and each combination has exactly one correct response:
   *
   *  - not entered: run the requested sequence.
   *  - running: a re-entrant call. Unreachable through the shuffle write path, and answered as a
   *    no-op rather than by recursing into a sequence that is already executing.
   *  - succeeded, asked to succeed again: a no-op. The status has been produced and republishing it
   *    would report the same bytes twice.
   *  - succeeded, asked to fail: run the failure cleanup. The attempt is failing after all, so its
   *    output must stop being reachable through every owner of it.
   *  - failed: a no-op. Everything has been released and no further call can change anything.
   *
   * A success stop that raises performs the failure cleanup itself before propagating, so the
   * guarantee does not depend on the caller making the second call at all.
   *
   * @param success whether the map task completed successfully
   * @return the map status on success, `None` otherwise
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    // A delegated attempt is the sort-based writer's from end to end, its stop protocol included:
    // the map status, the partition lengths and the cleanup all belong to it, and running any of
    // this writer's own stop sequence alongside would release state it never created and report
    // write time the delegate has already reported.
    //
    // Nothing of this writer's is left unreleased by forwarding, and that is a property of where
    // the delegation is decided rather than of this branch: [[degradeToSortShuffle]] retires this
    // generation from every owner of it -- routing entry, producer address, retained output,
    // sessions, buffers and spill files -- before it builds the delegate, so by the time a stop
    // arrives there is no streaming state in existence for it to clean up.
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
        // from every owner of it, at the instant it stood down. What remains is to report a status,
        // and the ordinary successful sequence must NOT run to produce it: draining, securing
        // durability and reporting completion to the coordinator would all publish output that the
        // shuffle-wide verdict has declared unreadable, and telling a consumer that void output is
        // complete is the one thing a producer must never do.
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
            // a published map status would promise. Retiring it here rather than relying on the
            // caller's own catch block is what makes the protocol self-contained: the caller is
            // not obliged to make the second call, and the flag-based guard this replaces would
            // have refused it in any case.
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
   * '''What this does, in the order it has to be done.''' The generation is retired from every
   * owner of it first -- the driver's registry, this executor's routing table, the retained-output
   * registry and the handler's sessions -- so that from this instant no consumer can be handed an
   * address for output the shuffle-wide verdict has declared unreadable. Everything this attempt
   * held is then released, because the records it consumed will be produced again by a different
   * writer and holding memory or disk for them serves nobody. What was genuinely streamed is still
   * published to the task's write metrics, so an operator sees the work that was done rather than a
   * silent gap.
   *
   * '''What it deliberately does not do.''' It does not finish the open partitions. Terminating a
   * stream needs the very scratch a memory-pressure trip has just been refused, and there is
   * nothing to terminate for: the withdrawal above already made every block unreachable. It does
   * not report the output complete to the coordinator either -- a consumer must never be told that
   * void output is complete.
   *
   * '''Why a latched I/O failure is logged rather than raised.''' Completing is strictly better
   * than failing even then. The void status leads to the same recomputation either way, so raising
   * only consume a task attempt -- and consuming a task attempt is the precise defect this method
   * exists to remove. The failure is logged with its cause so nothing is lost from the record.
   *
   * @param signal the trip that stood this attempt down, carrying the condition and its description
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
   * Shared by the two ways an attempt stops streaming without failing -- a preflight delegation and
   * a mid-write stand-down -- because both owe exactly the same unwind and neither can rely on
   * [[stop]] to perform it: a delegated stop returns through the sort-based writer, and a
   * stand-down stop takes the branch that produces the void status. Every step is idempotent, so
   * the task-completion listener that follows finds nothing left to do.
   *
   * @param reason operator-facing context, recorded with the withdrawal on the driver and locally
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
   * '''Why every partition reports a non-zero size, and why that is not a lie of consequence.''' A
   * block fetcher is entitled to skip a partition whose reported size is zero -- `MapStatus`
   * documents exactly that -- and a stood-down attempt has consumed records whose partitions it may
   * never have streamed a byte for, because they were still buffered when the trip fired. Reporting
   * those partitions as empty would have a reducer skip this map output silently and produce a
   * result short of data with nothing anywhere reporting it, which is the one outcome worse than an
   * aborted job. Reporting at least one byte for every declared partition makes every reducer ASK
   * for this output, and every such ask fails, because the output was withdrawn from every owner of
   * it before this status existed. The fetch failure is the truth; the byte count is only what
   * guarantees the question gets asked.
   *
   * The true streamed byte counts stay available through [[getPartitionLengths]], which describes
   * bytes that were actually written and is not consulted by the shared write path for a resolver
   * that is not index-based.
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
   * Separate from [[stop]] so that the state protocol there reads as a protocol rather than as one
   * interleaved with the sequence it governs. Each step is documented at its own definition; what
   * this method owns is the order, and every adjacency in it is load bearing:
   *
   *  1. The final drain first, because it is the last chance for paced egress to release what is
   *     still queued, and everything after it depends on knowing what is left.
   *  2. Durability next, over exactly what the drain could not deliver. This task's memory is about
   *     to stop existing, so the retained window is written to disk and its files handed to the
   *     executor-scoped resolver; that transfer, not the moment this task happens to finish, is
   *     what bounds the lifetime of retained output. A map output every consumer already
   *     acknowledged has nothing retained, so this step writes nothing at all.
   *
   *     '''How large that window is depends entirely on what consumers took, and is not decided
   *     here.''' Every acknowledgement releases what it covers the moment it arrives, so a producer
   *     whose consumer kept pace reaches this step holding a fraction of its output -- which
   *     `StreamingShuffleIntegrationTest` asserts against a live consumer -- while a producer no
   *     consumer ever subscribed to reaches it holding all of it, because a shuffle's reduce tasks
   *     are submitted only after its map stage completes and that ordering belongs to the scheduler
   *     (AAP 0.2.1, 0.2.2 and 0.8.2 Tier 1 forbid touching it). The same suite measures that
   *     ordering on an ordinary scheduled job rather than assuming it. What is decided here is only
   *     that whatever remains is made durable before a status promises a consumer can read it:
   *     bringing that write forward on a cadence of its own would spill below the configured
   *     threshold, which is the one thing AAP 0.9.4 fixes about spill behaviour.
   *  3. The reachability requirement immediately after, because it is the one condition under which
   *     publishing a status would be a promise this producer cannot keep.
   *  4. Stream completion before the status, so that a consumer which reads the completion set and
   *     then asks the ledger how many blocks to expect gets an answer rather than an unterminated
   *     stream.
   *  5. The completion report after the drain and '''immediately before''' the status is built, so
   *     that the coordinator's completion set never claims output this writer has not finished
   *     framing, and -- the load-bearing half -- so that the driver's answer is the last thing this
   *     sequence learns before it publishes. A refusal means the driver has disowned this
   *     generation, and an ordinary status published after that would re-register output a
   *     shuffle-wide withdrawal has just removed. See [[requirePublishableGeneration]].
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
   * '''The race this closes.''' A shuffle-wide stand-down withdraws the streamed map output of a
   * shuffle and retires every generation of it, and it does both on the driver. A map task in its
   * final drain cannot be stopped by that -- the drain runs inside a successful stop, where raising
   * would abandon output that is about to be made durable -- so the two can genuinely overlap. If
   * this writer then published an ordinary map status, the withdrawal would be undone by the very
   * attempt it was meant to invalidate: the map stage would report itself available again, the
   * recomputation would not happen, and the delegated reduce stage would retry against output no
   * owner will serve. The completion report is the barrier, because the coordinator applies it only
   * while the generation is still registered and not retired, so its refusal is the driver's own
   * statement that this output has been disowned -- taken here, after everything else in the stop
   * sequence, so that nothing can intervene between the answer and the publication it authorises.
   *
   * '''The two refusals, and why they end differently.''' A refusal accompanied by a shuffle-wide
   * fallback is a degradation, so it ends as every other trip does: the attempt completes with the
   * void status, its output withdrawn, and no task attempt is consumed. A refusal without one is a
   * zombie generation -- superseded by a newer attempt, or invalidated by a consumer that has
   * already raised the fetch failure which recomputes the stage -- and the honest answer there is
   * to fail, exactly as [[standDownIfRetired]] fails it when its own pass finds the same thing.
   *
   * '''An unreachable driver is not a refusal.''' Nothing is known in that case, and withholding a
   * map output on the strength of nothing would fail a task whose output is perfectly readable. It
   * is recorded and the status is published, which is the same direction every other unreachable
   * answer in this subsystem is resolved in.
   *
   * @return `None` when this generation may publish its ordinary status, or the status to report
   *         instead when it may not
   * @throws org.apache.spark.SparkException when the generation was refused without a shuffle-wide
   *         fallback, so that the unmodified scheduler recomputes this map output
   */
  private def requirePublishableGeneration(): Option[MapStatus] = {
    reportMapOutputComplete() match {
      case Some(true) => None
      case None =>
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
          log"${MDC(TASK_ATTEMPT_ID, mapId)} could not confirm its completion with the driver, " +
          log"so its map status is published on the strength of a local success; nothing is " +
          log"known about the driver's view of this generation, and a lost report is recovered " +
          log"by the consumer's own poll and timeout")
        None
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

  /**
   * Bytes of payload streamed per reduce partition.
   *
   * Never `null`: the array is allocated and zero-filled at construction, so a task that fails
   * before producing anything still reports a well-formed, all-zero length vector. An attempt that
   * degraded to the sort-based writer reports that writer's lengths, because they describe the
   * bytes that were actually written.
   */
  override def getPartitionLengths(): Array[Long] = {
    if (sortDelegate != null) sortDelegate.getPartitionLengths() else partitionLengths
  }


  // Initialisation

  /**
   * Settles whether this attempt streams at all, and hands it to the sort-based writer if it does
   * not.
   *
   * '''Why this exists, and why it runs before the first record.''' Graceful degradation is
   * specified to end in delegation to the sort-based shuffle without failing the job: every path
   * must terminate in a working shuffle. A running map task cannot honour that once it has consumed
   * records, because the records it has already framed exist only as serialized blocks and cannot
   * be re-driven into another writer -- so a refusal found mid-write leaves failing the attempt as
   * the only correct answer, and a failure is not a delegation. And with `spark.task.maxFailures`
   * at one -- which is what a plain `local[n]` master forces, and a legitimate cluster setting
   * besides -- that failure aborts the stage and the job.
   *
   * Every condition that can forbid streaming is therefore settled here, before a single record has
   * been consumed and before anything has been published to an owner outside this writer, where the
   * remedy is a total delegation that loses nothing: the iterator is passed on intact and the
   * attempt succeeds with a complete map output written by the unmodified sort-based path.
   *
   * The four conditions, in the order they are cheapest to answer:
   *
   *  1. '''Protocol version.''' Trip condition 4, detected by an explicit compatibility check
   *     rather than inferred from a decode failure, so a rolling upgrade degrades predictably.
   *  2. '''A verdict already reached.''' Either this executor has latched a trip of its own or this
   *     shuffle has been stood down somewhere else. Both mean the same thing here.
   *  3. '''Framing viability.''' A capacity of zero means the configured budget cannot accommodate
   *     even one block for one partition, so there is nothing to stream with.
   *  4. '''The framing reservation.''' Taken in full, once, for every declared partition. This is
   *     trip condition 2 -- memory pressure preventing a buffer allocation -- and taking the whole
   *     reservation here is what makes it a condition this method can act on rather than one
   *     [[stateFor]] would meet part way through the record loop.
   *
   * @return true when this attempt streams, false when it has been handed to the sort-based writer
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
      // would put a measurement nobody took into the record. `ProducerUnavailable` is the terminus
      // for the one case that reaches here with nothing latched at all, because a producer that
      // cannot learn why it must not stream is a producer no consumer can rely on.
      val cause: StreamingShuffleStandDownCause = fallbackPolicy.trippedReason
        .orElse(fallbackPolicy.knownShuffleFallback(shuffleId).flatMap(_.cause))
        .getOrElse(StreamingShuffleStandDownCause.ProducerUnavailable)
      return degradeToSortShuffle(cause,
        s"streaming had already been stood down because ${cause.description}")
    }
    // Registered before the budget is read, because the partition count is the divisor of every
    // buffer ceiling; reading a budget first would divide by the unregistered default of one.
    // Idempotent for the same count, so the registration the publishing step makes is unaffected.
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
      // budget too small for this shuffle's width rather than a moment of pressure. Reported to the
      // policy as trip 2 so the degradation below is attributed to memory pressure in the telemetry
      // an operator consults, and safe to declare here because it happens before this attempt has
      // consumed a record.
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
   * '''Why shrinking beats declining.''' The reservation is the one allocation a streaming producer
   * cannot spill its way out of -- an open partition needs an accumulator to frame into -- so it is
   * taken before the first record, whole. But its size is a *preference*: the block size it is
   * derived from trades pipelining against footprint, and a smaller block still streams correctly.
   * Declining instead of shrinking is what makes the difference matter, because declining means
   * standing the whole shuffle down: the verdict is shuffle-wide, so every map task of the shuffle
   * has to take the sort-based path, including siblings already streaming that can no longer be
   * asked to change their minds. Trading block size for a reservation that fits keeps the shuffle
   * on one path and keeps every sibling's output readable.
   *
   * Halving is used rather than a computed fit because the budget is shared and moves under this
   * task: a size computed from a reading taken now can be refused a moment later, whereas each
   * halving both lowers the request and re-reads what is available. The floor is one minimum
   * accumulator per partition, which is the smallest reservation that can frame at all.
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
        // The floor was refused, so there is nothing smaller to ask for. The negotiation is undone
        // so that the capacity this writer reports is the one it derived rather than the one it
        // failed to reserve, which is what the degradation message and the telemetry describe.
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
   * '''The declaration is not optional and it comes first.''' One map output written by the
   * sort-based writer while its siblings stream is output no reduce-side read path can reassemble,
   * so a per-task decision has to become a shuffle-wide one before the delegate may be used. The
   * coordinator latches the verdict, invalidates the partial streaming output, retires every live
   * producer so none can re-register, and answers every later participant with the verdict rather
   * than with an address; the map outputs that had already streamed are then recomputed on the
   * sort-based path through an ordinary fetch failure, which the unmodified scheduler resolves
   * without failing the job.
   *
   * '''The verdict is asked for directly rather than through [[declareShuffleFallback]].''' That
   * method claims the announcement once per shuffle per executor, which is right for a
   * fire-and-forget stand-down but wrong here, because this caller needs the *answer*: every map
   * task of the shuffle running on this executor reaches the same refusal at the same moment, only
   * one of them wins the claim, and the others would then read a cached verdict its ask had not yet
   * recorded and conclude that the shuffle could not be stood down -- failing attempts that were in
   * fact standing down correctly. The coordinator latches the verdict, so a second ask returns the
   * state the first one established instead of declaring a second fallback.
   *
   * If the declaration cannot be made -- the driver is unreachable, say -- the shuffle is left
   * exactly as it was and this attempt fails instead, leaving the scheduler to apply its configured
   * retry policy rather than leaving a contradiction behind. That is the same choice
   * [[StreamingShuffleManager]] makes when it cannot publish a producer.
   *
   * '''The generation is withdrawn before the delegate is adopted, and that ordering is the whole
   * point.''' Two registrations exist before this writer does -- the routing entry this executor's
   * listener holds and the producer address the driver publishes -- and both are made by
   * [[StreamingShuffleManager]] on the way to constructing this writer, because a consumer must be
   * routable the instant it can resolve an address. A delegation that left them standing would
   * leave a producer a consumer can resolve, connect to, and then wait on until its own five-second
   * detector fires, for an attempt that will never send it a byte: the sort-based writer publishes
   * this map output, so the streaming address is not merely unnecessary but wrong. Withdrawing
   * through [[releaseAfterFailure]] is what makes the unwind complete rather than partial -- one
   * operation retires the driver registration, the routing entry, the retained-output registration
   * and the handler's sessions, in that order -- and it is idempotent, so the task-completion
   * listener that follows changes nothing. The undrawn framing reservation goes back with it,
   * because the delegated [[stop]] returns through the sort-based writer and never reaches the
   * `finally` that would otherwise have returned it.
   *
   * '''This generation is retired before the delegate is built, and that is not optional.'''
   * [[StreamingShuffleManager]] publishes two owners of this generation before this writer
   * exists -- a routing entry in the executor's streaming listener, and a producer address on the
   * driver -- because a consumer must be able to reach a producer the instant it resolves its
   * address. Neither of them is this writer's to leave behind once it has decided not to stream: a
   * routing entry that survives is a producer a consumer can resolve, connect to and then wait on
   * until its own five-second detector fires, and this executor would hold one per delegated map
   * output until the shuffle was unregistered. The unwind therefore runs here, before the delegate
   * is built, through [[releaseAfterFailure]] -- the single operation that retires every owner of a
   * generation at once and releases the buffers and spill files behind it. Deferring it to [[stop]]
   * would not do: a delegated attempt's stop belongs entirely to the sort-based writer, so this
   * writer's own stop sequence is deliberately not run at all.
   *
   * '''The generation is withdrawn before the delegate is adopted, and that ordering is the whole
   * point.''' Two registrations exist before this writer does -- the routing entry this executor's
   * listener holds and the producer address the driver publishes -- and both are made by
   * [[StreamingShuffleManager]] on the way to constructing this writer, because a consumer must be
   * routable the instant it can resolve an address. A delegation that left them standing would
   * leave a producer a consumer can resolve, connect to, and then wait on until its own five-second
   * detector fires, for an attempt that will never send it a byte: the sort-based writer publishes
   * this map output, so the streaming address is not merely unnecessary but wrong. Withdrawing
   * through [[releaseAfterFailure]] is what makes the unwind complete rather than partial -- one
   * operation retires the driver registration, the routing entry, the retained-output registration
   * and the handler's sessions, in that order -- and it is idempotent, so the task-completion
   * listener that follows changes nothing. The undrawn framing reservation goes back with it,
   * because the delegated [[stop]] returns through the sort-based writer and never reaches the
   * `finally` that would otherwise have returned it.
   *
   * @param reason the trip condition to record, one of the four the feature specifies
   * @param detail operator-facing context, recorded with the declaration on the driver
   * @return false always, so a caller can `return degradeToSortShuffle(...)` and read as a decision
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
    // life of the executor with nothing left that would ever retire it. See
    // [[withdrawPrePublishedOwnership]] for what those owners are and why the manager cannot retire
    // them itself.
    withdrawPrePublishedOwnership(detail)
    sortDelegate = sortShuffleWriter()
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
      log"${MDC(TASK_ATTEMPT_ID, mapId)} will not stream because ${MDC(REASON, detail)}; the " +
      log"shuffle has stood streaming down for every participant, this generation has been " +
      log"retired from every owner of it on this executor, and the sort-based shuffle writes " +
      log"this map output, so this task completes rather than failing")
    false
  }


  /**
   * Prepares the streaming path, exactly once per writer.
   *
   * The order of the calls below is load-bearing and is documented rather than merely observed:
   *
   *  1. The reduce partition count is registered with the spill manager first, because it is the
   *     divisor of every buffer ceiling. Reading a budget before registering it would divide by the
   *     unregistered default of one and return an allowance an order of magnitude too generous.
   *  2. This map output's retained blocks are published to the executor-scoped resolver next, under
   *     this task attempt as their generation. That publication is what lets egress and replay read
   *     a block from wherever it presently lives -- memory or a spill segment -- instead of the
   *     egress path holding a second, unaccounted copy of every block in flight. No acknowledging
   *     consumer is registered here, and deliberately not: retirement advances to the *minimum*
   *     position across every registered consumer, so a placeholder identity that never
   *     acknowledges would hold the whole retained window open, and one advanced on the real
   *     consumers' behalf would merely lag them and would freeze the moment this task finished --
   *     while the reduce tasks it is streaming to are still reading. Consumers register themselves
   *     as they subscribe, which is the only identity that can honestly release anything.
   *  3. The spill manager's own cleanup is registered before this writer's, because task completion
   *     listeners are invoked in reverse registration order. Registering this writer's listener
   *     second means it runs first, so egress is released while the buffers it referenced are still
   *     alive rather than after they have been freed.
   *  4. The shuffle is registered with the backpressure protocol before any utilisation is
   *     reported, because reports for an unregistered shuffle are discarded.
   *  5. The attempt's flush ordering is registered with the handler before the first block is
   *     queued, so that no block is ever queued under the default ordering.
   *  6. The buffer budget is forced last, because it reaches through `SparkEnv` and may refuse.
   *
   * Everything that can forbid streaming this shuffle at all is settled by [[prepareStreaming]]
   * before any of it, so that a refusal costs nothing, unwinds nothing and degrades to the
   * sort-based writer rather than failing the attempt.
   *
   * <b>Transactional.</b> Steps 2 onwards each publish something an owner outside this writer can
   * see, and any of the steps after them can still refuse: the publication can be declined, the
   * budget derivation reaches through `SparkEnv`, and the quotient check is an assertion on a
   * contract. A writer that left a partial initialisation behind would leave this generation
   * published and routed while holding no buffers and streaming nothing -- a producer a consumer
   * can resolve and open a channel to, and then wait on until its own detector fires. So the whole
   * body either completes or is unwound: the failure path is the same single operation the
   * unsuccessful stop uses, so exactly one piece of code knows how to retire a generation, and it
   * retires every owner rather than the subset this method happened to reach.
   *
   * Two registrations are deliberately '''not''' unwound, and both are correct to keep:
   *
   *  - the two task-completion listeners, because `TaskContext` exposes no way to withdraw one and
   *    neither needs withdrawing -- each is idempotent and each fires on a task that failed exactly
   *    as it does on one that succeeded, releasing whatever is left;
   *  - the shuffle's registration with the backpressure protocol, because that registration is
   *    shuffle scoped rather than task scoped and is shared with every other map task of the same
   *    shuffle running on this executor. Unwinding it here would strip the partition count and the
   *    utilisation budget out from under those tasks. It is released when the shuffle is
   *    unregistered, which is the boundary that owns it.
   */
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
   *
   * Everything here either publishes this generation to an owner outside this writer or derives a
   * value that binds memory to the task. Nothing here is idempotent by itself, and nothing here is
   * retried: the caller unwinds the whole of it on any refusal.
   */
  private def publishStreamingProducer(): Unit = {
    spillManager.registerPartitionCount(partitionDivisor)
    // The verdict is acted upon rather than discarded. A declined publication means one of exactly
    // two things, and neither permits this attempt to stream: a newer generation of this map output
    // is registered, so this one is a superseded attempt whose partial output must never reach a
    // consumer; or the executor has stopped its streaming subsystem, so no retained block of this
    // attempt could ever be served. Discovering it here costs one refused task whose retry the
    // manager serves correctly, whereas discovering it at the successful stop -- where the hand-off
    // of retained files is refused -- would mean a whole map task's output had already been
    // streamed to consumers that must not receive it.
    if (!blockResolver.registerProducer(shuffleId, mapId, context.taskAttemptId(), spillManager)) {
      throw new SparkException(s"Streaming shuffle $shuffleId could not publish the retained " +
        s"output of map $mapId attempt ${context.taskAttemptId()}: either a newer generation of " +
        "that map output is registered or this executor has stopped streaming, so this attempt " +
        "must not stream and is failed so that its retry is served correctly.")
    }
    // Consumers can legitimately subscribe while this map task is live but before its first call to
    // `write`. Their session and subscriber slots are already bounded; now that the retained store
    // is published, settle its independent cursor cap before this writer can admit or fan out a
    // block.
    serverHandler.registerPendingRetainedConsumers()
    installCleanupListeners()
    backpressure.registerShuffle(shuffleId, partitionDivisor)
    serverHandler.registerTaskAttempt(egressPriority)
    val nowMillis = clock.getTimeMillis()
    lastMaintenanceMillis = nowMillis
    lastHeartbeatMillis = nowMillis
    lastCoordinatorHeartbeatMillis = nowMillis
    throughputWindowOpenedMillis = nowMillis
    // Forces the derivation, so it either succeeds and is logged once, or refuses before a single
    // record has been buffered.
    val capacity = blockPayloadCapacity
    verifyBudgetContract()
    // Bounded per executor rather than emitted per map task: see [[budgetLogAggregator]] for why a
    // per-task record is a log volume proportional to the workload's task rate. The admitted record
    // states how many tasks it stands in for; the per-task form is restored by the debug key.
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
        // emitted at. That is this subsystem's established convention for opted-in detail -- the
        // same one the egress handler's own construction record uses -- and it matters: routing
        // opt-in detail to `logDebug` would additionally require the logging framework to be at
        // DEBUG, so an operator who set the streaming debug key and expected the per-task line back
        // would get nothing.
        if (debugEnabled) {
          logInfo(entry)
        }
    }
  }

  /**
   * Asserts the one property of a dependency that streaming can never express, before any output
   * state exists.
   *
   * <b>Map-side combine.</b> A dependency that asks for map-side combine cannot be streamed, and
   * the reason is semantic rather than a limitation of this implementation: a combined value for a
   * key is only correct once every record for that key has been seen, so a producer that emitted
   * records as it produced them would be emitting uncombined values under a contract that promises
   * combined ones. The reduce side would then apply `mergeCombiners` to values that were never
   * combiners, and the result would differ from Spark's shuffle contract without anything failing.
   * Such a dependency is therefore never streamed at all: [[StreamingShuffleManager]] declines the
   * streaming registration and hands it to the sort-based path, where map-side combine is
   * implemented correctly by `ExternalSorter`. This requirement states that obligation at the
   * boundary that depends on it, so a manager that ever stopped honouring it fails loudly here
   * instead of producing quietly wrong output.
   *
   * Deliberately a `require` and not a degradation: a manager that handed such a dependency to this
   * writer has a defect, and quietly writing it on the sort-based path would hide the defect rather
   * than reporting it. Every condition that is a legitimate runtime state instead of a defect is
   * handled by [[prepareStreaming]], which degrades rather than raising.
   */
  private def requireStreamableDependency(): Unit = {
    require(!dep.mapSideCombine,
      s"Streaming shuffle $shuffleId declares map-side combine, which the streaming path cannot " +
        "express: a combined value is only correct once every record for its key has been seen, " +
        "so such a dependency must be registered on the sort-based path instead of being streamed.")
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

  // Record path

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
    if (recordsSinceMaintenance >= MAINTENANCE_RECORD_INTERVAL) {
      // A shuffle of tiny records may go a long time between block boundaries, and heartbeats,
      // acknowledgement drainage and the liveness check must not wait for one. The clock is only
      // read once per interval, so the amortised cost per record is negligible.
      maintainStreams()
    }
  }

  /**
   * The heap one partition's framing accumulator occupies, and therefore what it is charged.
   *
   * The accumulator itself is exactly one fixed-size block segment. A segment smaller than the JVM
   * accounting floor is deliberately over-charged to that floor, which keeps the aggregate bound
   * conservative without forcing a small negotiated block to allocate a larger backing array.
   */
  private def accumulatorFootprintBytes: Long =
    math.max(MIN_ACCUMULATOR_BYTES, math.max(1, blockPayloadCapacity)).toLong

  /**
   * Returns the egress state of one partition, opening its stream on first use.
   *
   * Two things happen here that are worth naming, because both are contracts rather than details.
   *
   * The accumulator's memory is charged to the spill manager <b>before</b> it is allocated. An
   * accumulator is one block's worth of heap per active partition, held for as long as the
   * partition is producing, and leaving it uncharged would mean the configured buffer percentage
   * bounded only the blocks cut out of these arrays and not the arrays themselves -- so the
   * executor could hold substantially more than the operator asked it to.
   *
   * <b>The charge is drawn from a reservation this attempt already holds, so it cannot be
   * refused.</b> [[prepareStreaming]] takes one accumulator's worth for every declared partition up
   * front, before the first record is consumed, and this draws its share from that. The reason is
   * not efficiency but correctness of the degradation contract: a reservation taken here, part way
   * through the record loop, can be refused, and an attempt that has already consumed records
   * cannot then be handed to another writer -- so a refusal here could only fail the task, and
   * failing the task is not the delegation to sort-based shuffle that graceful degradation is
   * specified to be. Taking the whole reservation ahead of the first record is what makes that
   * condition a total, loss-free delegation. Every partition id this method is reached with is
   * inside the declared domain, so the envelope always covers the charge; the fresh reservation
   * below is the arithmetic that keeps the charge correct if the envelope is ever exhausted rather
   * than a reachable branch.
   *
   * The serialization stream is built over `SerializerManager.wrapStream`, not over the raw
   * accumulator, so that the bytes this partition streams are compressed and encrypted exactly as
   * the reader expects to find them.
   */
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
        // its output withdrawn rather than failing, so no task attempt is consumed. See
        // [[checkFallbackPolicy]] for why a counted failure is not an option here.
        // Memory pressure, and not a stand-in for it: a framing reservation this executor could
        // not satisfy IS the specified condition "memory pressure prevents buffer allocation",
        // observed right here. The policy's own latched reason is preferred only because it may
        // already name the condition that caused this one.
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
      // Register before the serializer is constructed. Some serializers emit a header from their
      // constructor, and the fixed-segment accumulator can hand a full segment to egress from that
      // write; even the first segment must therefore have a credit and replay ledger already open.
      state.stream = serializerInstance.serializeStream(
        serializerManager.wrapStream(ShuffleBlockId(shuffleId, mapId, partitionId),
          state.accumulator))
      state
    }
  }

  /**
   * Streams one detached fixed segment as a block.
   *
   * The order of the two hand-offs is the memory bound. The block is offered to the spill manager
   * first, so aggregate buffered bytes are checked, spill is triggered if the threshold has been
   * crossed and the retention window is extended, all before the bytes are queued for the wire.
   * Reversing the order would let bytes leave that no budget had ever admitted.
   *
   * There is exactly one owned copy of a block's payload, and after the admission below it is the
   * store's. The array cut from the accumulator is adopted rather than copied, and the handler is
   * given the block's <i>size</i> rather than its bytes -- it reads them back from the store when
   * it frames them for a particular consumer. That is what makes the budget a real bound: a second
   * copy on the egress path would be an equal quantity of retained payload that nothing charges
   * for, and it would make a block replayable while it sat in memory but not once it had been
   * evicted. Nothing here touches `payload` after the admission returns.
   *
   * The sequence number is asserted rather than trusted. Both the spill manager's retention window
   * and the handler's egress queue number a partition's blocks densely from zero, and the receiving
   * reader reassembles on that assumption, so a divergence is a genuine protocol violation -- the
   * signature of a server handler shared between two concurrent producers of the same shuffle --
   * and is reported as one instead of silently corrupting a stream.
   *
   * <b>This side's numbering is committed only by a successful hand-off.</b>
   * `state.nextSequenceNumber` advances after both the admission and the egress offer have returned
   * and the assertion has passed, so a refused block leaves this partition's numbering where it was
   * and the retry of the task starts from a consistent position. The admission is deliberately not
   * rolled back when the egress offer refuses: the only way it refuses is a protocol violation,
   * which fails this task, and a failing task withdraws the whole generation -- routing, retained
   * output, spill files and sessions together -- so there is nothing left for a rollback of one
   * block to protect.
   */
  private def emitBlock(state: PartitionEgressState, payload: Array[Byte]): Unit = {
    val length = payload.length
    val sequenceNumber = state.nextSequenceNumber
    // Only the framing, admission and hand-off are charged as shuffle write time, and only these.
    // Timing the whole of write() would charge the upstream operator's compute time to shuffle
    // write, and the sort-based path is careful about exactly this: `DiskBlockObjectWriter` charges
    // write time around its commit rather than around each record's serialization, and
    // `SortShuffleWriter` charges it around its sorter's teardown. The maintenance and telemetry
    // hooks below are deliberately outside the timed region, because they are neither.
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
   *
   * A refusal from [[MemorySpillManager.bufferBlock]] already means eviction was attempted and
   * could not free enough, so the recovery here does the only three things that can change that
   * verdict without waiting: push queued bytes towards the wire, apply whatever the consumer has
   * acknowledged since, and force an eviction pass. None of them sleeps, so the whole path advances
   * with the injected clock rather than with wall time.
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
   * fallback policy is told and streaming stands down for the executor. The block is not lost with
   * it: it goes durably to local disk, so the map output this attempt has produced stays complete
   * and the attempt is stood down in place at the next block boundary rather than failed. That is
   * what makes the memory bound real without discarding output: the alternative would be to stream
   * bytes no budget admitted.
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
          // Relief landed somewhere. Whichever partition it landed on is now smaller, which moves
          // this one closer to the front of the eviction order.
          barrenRounds = 0
        } else if (deferrals < MAX_EVICTION_DEFERRALS && spillManager.evictionInFlight) {
          // Memory is actively being reclaimed by another thread; this round freeing nothing says
          // nothing about whether the budget is exhausted. Wait for that write to land instead of
          // failing a task that is about to have room.
          deferrals += 1
          // Hand the CPU to whichever thread is doing the reclaiming. A run of these rounds that
          // does not yield can burn its whole allowance faster than a spill write completes, and so
          // declare the budget exhausted while it is actively being freed; yielding makes the wait
          // outlast the work it is waiting for. This is a yield and not a sleep, so the loop stays
          // free of wall-clock timing and advances with the injected clock.
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
      // degraded from inside an accounting call. The output itself is never abandoned here: the
      // specified answer to a full buffer is to spill, so the block skips memory and goes straight
      // to local disk, the map output stays complete, and the spill manager counts that write as
      // the spill event it is -- on `shuffle.streaming.spillCount` as well as on `diskBytesSpilled`
      // -- so an operator never sees this path produce disk volume with no spill behind it.
      backpressure.reportDurableSpillAdmission(producerKey(state.partitionId))
      // The fallback policy is told, unconditionally, because reaching this line *is* the
      // specification's second trip condition: "memory pressure prevents buffer allocation". Every
      // recovery round this writer is allowed has already run -- pending frames were flushed,
      // acknowledgements drained, eviction invoked, an in-flight eviction waited out -- and the
      // reservation is still refused. Whether any of those rounds happened to find resident
      // bytes to evict does not change the fact reported here: the executor's buffer allowance
      // cannot admit this block, and streaming that continues past it is streaming that will keep
      // failing to allocate.
      //
      // This was previously gated on `spillManager.bufferedBytes > 0 || memoryBytesSpilled > 0`,
      // on the reasoning that an allowance which was never available to this attempt is
      // "structural rather than a shortage" and should keep the stream its consumers are already
      // reading. That reasoning made the documented graceful degradation inert in exactly the
      // situation it exists for: a budget wholly committed by other tasks on the executor, or a
      // block the configured percentage can never hold, reports nothing resident and nothing
      // spilled -- so the condition FR-10 names most directly was the one condition the policy was
      // never told about. An operator saw buffer utilisation at ninety-nine per cent, a stream of
      // pressure warnings and repeated map-stage recomputation while the policy reported that
      // nothing had tripped. There is no reading of the specification under which a refused
      // allocation is not a refused allocation, so there is no gate here.
      //
      // Zero granted, because the reservation was refused outright: a grant of nothing against a
      // request for something is what a short grant means at its limit. Reporting it trips the
      // policy; the next block boundary reads that trip in `checkFallbackPolicy` and stands this
      // attempt down through the in-place degradation path, which reconstructs the complete map
      // output through the sort-based writer without failing the task, while the coordinator
      // withdraws the streamed output shuffle-wide so no consumer is left reading a path this
      // executor has abandoned.
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
        // Nowhere left to put the block. Unlike a full budget this is not a condition another
        // arrangement of memory could absorb, so it is raised: the attempt has produced incomplete
        // output and only recomputation can replace it.
        throw new SparkException(s"Streaming shuffle $shuffleId could not retain $requested " +
          s"bytes for partition ${state.partitionId} in memory or on local disk; the map output " +
          "of this attempt is incomplete and the attempt cannot continue.")
      }
      durableAdmissionsObserved += 1L
      reportDurableAdmission(state.partitionId, requested, round)
    } else if (round > 0 && spillManager.memoryPressureDetected) {
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

  /**
   * Reports that a block met the buffer allowance and went to local disk instead of into memory.
   *
   * Bounded through the '''executor-scoped''' aggregation window, not through a per-task first
   * occurrence. The condition recurs for every block once the allowance is met, a wide shuffle cuts
   * thousands of them, and -- the part a per-task bound cannot answer -- an executor runs one of
   * these writers per map task, so "the first occurrence of this task" is one default-level record
   * per task and thence a log volume that scales with the stage's task count. That is the volume
   * this feature may not produce. Aggregated executor-wide, a whole stage in this condition
   * contributes one record a minute, carrying the executor-wide total, the bytes it moved and how
   * many occurrences it stands in for; the record is what tells an operator to look, and a task's
   * own summary still reports how much of its output took that route. The streaming debug key
   * restores the per-block detail.
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


  // Maintenance: flow control, spill, liveness and the consumer-failure flow (FR-3, FR-5, FR-9)

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
   *
   * <b>What happens to each of these duties when the task ends.</b> This method runs on the task
   * thread and therefore stops when the task does, while a consumer's right to be served does not,
   * so it is worth being explicit about where each obligation goes rather than leaving the boundary
   * to be inferred:
   *
   *  - Utilisation reporting, spill polling and buffer reclamation manage task-managed execution
   *    memory. That memory is reclaimed by the executor at task completion -- and, with
   *    `spark.unsafe.exceptionOnMemoryLeak` enabled, its retention fails the task -- so these
   *    duties cannot outlive the task because the resource they manage cannot either.
   *  - Heartbeats and the consumer-liveness check exist to keep an open streaming channel healthy.
   *    The channel belongs to this map attempt and closes with it. They continue through the final
   *    drain, including for streams already marked terminated while anything is still owed, which
   *    is what stops a consumer that is mid-delivery from declaring a healthy producer dead.
   *  - Retransmission of the unacknowledged window is the one obligation that genuinely outlives
   *    the task, and it is transferred rather than abandoned. [[secureRetainedOutput]] writes the
   *    window to disk and hands the files to the executor-scoped
   *    [[StreamingShuffleBlockResolver]], which from then on locates and serves those segments and
   *    unlinks them when the generation is superseded, when the shuffle is unregistered, or when it
   *    stops. The owner of retained output after this task is therefore the resolver, not this
   *    writer, and the resolver's lifetime is the executor's.
   *  - Ledger state is not left behind: [[completeProducerStreams]] reports every stream complete
   *    and [[unregisterProducerStreams]] removes it, both from the task-completion listener, so the
   *    executor-wide protocol keeps no entry for a task that has finished.
   */
  private def maintainStreams(): Unit = {
    recordsSinceMaintenance = 0
    val nowMillis = clock.getTimeMillis()
    if (nowMillis - lastMaintenanceMillis >= MAINTENANCE_INTERVAL_MS) {
      lastMaintenanceMillis = nowMillis
      drainAcknowledgements()
      // Both figures are executor-wide, and they have to be. The budget is one allowance shared by
      // every spill manager on the executor, so pairing it with this task's own buffered bytes
      // would divide a task-local numerator by an executor-wide denominator and under-report
      // utilisation by roughly the number of map tasks running concurrently -- exactly when the
      // spill threshold matters most. Reporting the reservation against the shared quota also makes
      // the report idempotent: every concurrent map task of every shuffle publishes the same pair,
      // so a later report overwriting an earlier one changes nothing and the reading is
      // executor-wide by construction rather than by whichever task reported last.
      backpressure.reportBufferUtilization(
        shuffleId, spillManager.executorReservedBytes, spillManager.totalBudgetBytes)
      pollSpill()
      sendHeartbeatsIfDue(nowMillis)
      handleConsumerStalls(nowMillis)
      reportProducerThroughput(nowMillis)
      // Lets the protocol advance its own timers even when no message has arrived, which is what
      // makes an acknowledgement gap observable rather than merely inferable.
      backpressure.pollOnce()
      publishWriteMetrics()
      // Last, because this is the one maintenance duty that can stand this producer down, and a
      // stand-down raises. Nothing above it is skipped as a result.
      standDownIfRetired(nowMillis)
    }
  }

  /**
   * Refreshes this generation's registration with the coordinator, on the refresh cadence.
   *
   * The refresh is an obligation rather than an optimisation. The coordinator reaps a producer it
   * has not heard from inside its liveness window, so a map task that streams for longer than that
   * window without refreshing would have its own address withdrawn from under the consumers that
   * are reading it -- and they would then recompute a map stage that was working perfectly well.
   *
   * The same ask is also the only way a driver-side decision about this generation reaches this
   * executor. A consumer that timed out invalidates the generation at the driver, a newer attempt
   * supersedes it there, a shuffle-wide fallback retires it there, and this subsystem has no
   * driver-to-executor channel through which any of that could be pushed. Asking is what makes
   * those decisions actionable here, which is why the answer is returned rather than discarded.
   *
   * The refresh rides the maintenance pass, so its cadence is the maintenance cadence. That is
   * sufficient rather than merely convenient: the interval is the connection timeout and the
   * coordinator's window is twice it, so one missed pass is absorbed by construction.
   *
   * Gated on the cadence and on nothing else. In particular it is deliberately not gated on this
   * writer having finished framing, because the final drain runs after framing has finished and is
   * exactly the phase in which the registration most needs to stay alive: the drain's deadline is
   * the same length as the coordinator's liveness window.
   *
   * @param nowMillis the caller's own clock reading, so the cadence is measured once per pass
   * @return what the driver reported, or `None` when it was not asked on this pass or could not be
   *         reached. `None` never means "not live"
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

  /**
   * Fails this task when the driver no longer holds its producer generation.
   *
   * A retired generation must not keep producing. Its output has either been superseded by a newer
   * attempt or already declared invalid, its consumers have been told to recompute, and every byte
   * it streams from here on is work whose result nothing will read. Failing is the recovery the
   * platform already has: the unmodified scheduler retries the task, and the unsuccessful stop that
   * follows retires this generation from every owner on this executor through the one withdrawal
   * operation.
   *
   * An unreachable driver is deliberately not treated as retirement. Nothing is known in that case,
   * and standing down on the strength of nothing would abandon a shuffle that is healthy everywhere
   * else; a producer that has genuinely gone is handled by the coordinator's own liveness window.
   *
   * Reached from the maintenance pass alone, and never from the final drain. That drain runs inside
   * a successful stop, where raising would abandon output that is about to be made durable and
   * readable, so the drain refreshes the registration and ignores the answer. A generation retired
   * during the drain is not stood down here -- it is settled at the end of the stop sequence
   * instead, by [[requirePublishableGeneration]], which asks the driver to accept this generation's
   * completion immediately before the map status is built and refuses to publish an ordinary status
   * when the driver refuses the report. That is the same fact this method acts on, learned through
   * the same registry, at the one instant where acting on it can still keep a withdrawn output from
   * being re-registered; putting it there rather than in the drain is what lets the drain finish
   * making its output durable without publishing it.
   */
  private def standDownIfRetired(nowMillis: Long): Unit = {
    if (!finished) {
      refreshCoordinatorRegistration(nowMillis).foreach { liveness =>
        if (!liveness.live) {
          // Two retirements reach here and they demand different answers. One is a shuffle-wide
          // FALLBACK, which is a degradation: this attempt completes with its output withdrawn so
          // that no task attempt is consumed, exactly as every other trip does. The other is a
          // generation SUPERSEDED or invalidated by a consumer, which is a zombie attempt whose
          // failure is both correct and harmless -- the winning attempt owns the map index, and the
          // consumer that invalidated the generation has already raised the fetch failure that
          // recomputes the stage.
          if (fallbackPolicy.shuffleHasFallenBack(shuffleId)) {
            // Across the whole closed set, for the same reason the pre-flight check above reads
            // it that way: the shuffle-wide verdict may be a structural decline, and re-labelling
            // it as one of the four specified conditions would record a measurement nobody took.
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

  /**
   * Reconciles this writer's view of what every consumer has acknowledged.
   *
   * The bytes an acknowledgement frees are released where the acknowledgement is authenticated --
   * inside the egress handler, against the session that sent it -- and not here. That is a
   * correctness requirement rather than a preference: releasing on the task thread would mean
   * releasing on behalf of an identity that did not acknowledge anything, and retirement advances
   * to the minimum position across every registered consumer precisely so that no such proxy can
   * authorise the discard of bytes a slower consumer has not yet received.
   *
   * What is left for the task thread is what only the task thread can do: notice that the slowest
   * consumer has advanced, restore the replay budget that a stall had spent, and close the 100 ms
   * reclamation window on the backpressure protocol -- the one component that sees both ends of it,
   * since the acknowledgement opened it on an I/O thread.
   */
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
            // Bounded on the executor's window, not on this task's first breach. A breach is
            // per acknowledgement, an executor runs one of these writers per map task, and a
            // first-occurrence latch per task is therefore one default-level record per task --
            // a volume set by the workload's task rate rather than by anything this feature does.
            // The per-task tally is still reported by this attempt's own summary and exposed for
            // assertions; the debug key restores the per-breach line.
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
   *
   * Two triggers, and both are required. The first is this task's own utilisation, polled on the
   * cadence the spill manager enforces internally. The second is the aggregate reading across every
   * shuffle registered on this executor: a task whose own buffers are comfortable must still yield
   * when the executor as a whole has crossed the threshold, which is precisely the cross-shuffle
   * monitoring the backpressure protocol exists to provide.
   *
   * <b>Only the second trigger is arbitrated.</b> The first is this subsystem's own contract -- the
   * spill threshold is evaluated against the executor's one aggregate streaming buffer reservation,
   * every category of it, and eviction at that point is unconditional -- so no arbitration may
   * stand in front of it. The second exists to make one
   * shuffle yield on account of another's demand, and that is exactly the question arbitration
   * answers: `shouldYield` places the concurrent shuffles in order of how much of the executor's
   * buffered egress each is responsible for and exempts the least demanding one, so a small shuffle
   * sharing an executor with a very large one is never asked to yield on every pass and starved.
   * Gating the first trigger on it instead would be strictly wrong: `shouldYield` is false when
   * there is only one shuffle, by design, so a single-shuffle executor would stop spilling
   * altogether at the very moment the threshold said it must.
   */
  private def pollSpill(): Unit = {
    if (spillManager.pollOnce()) {
      recordSpillObserved()
    } else if (backpressure.shouldYield(shuffleId) && spillManager.maybeSpill()) {
      recordSpillObserved()
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
      // the producer spills. The writer's view of the same event is therefore debug detail.
      if (debugEnabled) {
        logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} spilled to disk, now at " +
          log"${MDC(COUNT, observed)} spills holding " +
          log"${MDC(NUM_BYTES, spillManager.bufferedBytes)} of " +
          log"${MDC(THRESHOLD, spillManager.totalBudgetBytes)} budgeted bytes")
      }
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
   *
   * A terminated stream is still heartbeated while it holds anything unacknowledged, and that is a
   * correctness requirement rather than thoroughness. Termination says this producer will frame no
   * further block; it says nothing about whether the blocks already framed have arrived. A consumer
   * still waiting for retained blocks measures producer liveness on its own five-second timer, so
   * falling silent the moment the local stream was marked terminated would have that consumer
   * declare a perfectly healthy producer dead, invalidate its partial reads and force a stage
   * recomputation -- for output that was sitting in the retained window ready to be replayed. The
   * liveness service therefore runs until the window is empty or the task is over.
   */
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
          // The per-partition latch above still governs how often the condition is *counted* --
          // once per stall rather than once per maintenance pass -- but the record itself is
          // bounded on the executor's window. A stall is per reduce partition and this writer
          // exists per map task, so a latch per partition is one default-level record per
          // partition per task: on a wide shuffle whose consumers are slow, the loudest record
          // this subsystem can produce.
          // The executor-wide figure is what tells an operator that consumers are behind, and this
          // attempt's own summary still reports how many of its partitions stalled.
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
              // per-attempt record is detail. The stall itself is already reported at warning
              // level, the aggregate reaches an operator in this task's summary, and an exhausted
              // budget escalates with a warning of its own.
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
   * The link-utilisation report is the producer's own trip condition, and the figure reported for
   * it is deliberately not this writer's own rate. Saturation is a property of the link, and the
   * link belongs to the executor: one map task's egress systematically understates a link shared by
   * every concurrent shuffle, so a policy fed that figure could never observe a saturated link at
   * all. The protocol measures executor-wide egress -- framed wire bytes across every stream --
   * over a stable interval, and that measurement is what the trip condition is evaluated against.
   * When no bandwidth cap is administered the reading is not evaluable and nothing is reported.
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
   * Reached from the three places a streaming producer can be refused the memory it trades for
   * latency: a budget that cannot accommodate even one block for one partition, the framing scratch
   * a partition needs before its first record, and the admission of a block after eviction has been
   * attempted and could not free room. All three are the second of the four specified fallback
   * conditions, and every caller raises immediately afterwards, because there is no correct way to
   * continue streaming bytes that cannot be held.
   *
   * Two participants are told here, and each owns a different part of the outcome:
   *
   *  - [[StreamingShuffleFallbackPolicy]] owns the '''decision'''. Recording a reservation that was
   *    short granted is what latches the memory-pressure trip on this executor.
   *  - the coordinator owns the '''scope'''. This is the step the raise alone cannot supply, and
   *    leaving it out is what made a refused allocation a local decision with global consequences:
   *    a task that merely failed would have its retry served by the sort-based delegate here while
   *    every other executor kept streaming the same shuffle, which is two producers of one output
   *    and two incompatible reduce-side read paths. Declaring makes the transition shuffle-wide
   *    before anything is delegated or retried, and the policy's own one-shot keeps the wire cost
   *    at one ask per shuffle per executor however many tasks observe the condition.
   *
   * A third participant, [[BackpressureProtocol]], owns the '''record of why''' and is told by the
   * two callers that have a partition to name rather than here: its record is per stream, and the
   * budget derivation runs before any stream exists. Without that record the protocol's degradation
   * reasons would show no memory-pressure entry for a shuffle that stood down precisely because a
   * buffer could not be allocated, and the condition would be invisible in the very telemetry an
   * operator consults to understand the fallback.
   *
   * The reason declared is whatever the policy latched, which is memory pressure for every path
   * that reaches here; the explicit fallback covers the case where a different condition had
   * already latched first, in which case that condition is the one the whole shuffle agrees on.
   *
   * @param requestedBytes the reservation that was asked for, in bytes
   * @param grantedBytes what was actually granted, which is what makes the sample a short grant
   */
  private def standDownForMemoryPressure(requestedBytes: Long, grantedBytes: Long): Unit = {
    fallbackPolicy.recordAllocationGrant(requestedBytes, grantedBytes)
    // The verdict is not consulted here, and deliberately: every caller of this method raises
    // immediately afterwards, so this attempt is failing rather than delegating and no second
    // implementation of the shuffle can be published whatever the coordinator answered. The retry
    // consults the verdict through [[StreamingShuffleManager]], which delegates only once it is
    // confirmed.
    val unusedVerdict = declareShuffleFallback(fallbackPolicy.trippedReason
      .orElse(Some(StreamingShuffleFallbackReason.MemoryPressure)))
    if (unusedVerdict.exists(_.fallenBack) && debugEnabled) {
      logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} stood down for every " +
        log"participant before failing map ${MDC(TASK_ATTEMPT_ID, mapId)} for memory pressure")
    }
  }

  /**
   * Stands down cleanly when graceful degradation has tripped.
   *
   * Every trip routes to the same terminus -- the unmodified sort-based shuffle -- and a running
   * map task that has already consumed records cannot reach it by delegating, because the
   * records it framed exist only as serialized blocks and cannot be re-driven into another writer.
   * So the map output has to be produced again, and the only question is by what mechanism.
   *
   * '''It must not be by failing this attempt.''' A task that throws reports an ordinary task
   * failure, which counts towards `spark.task.maxFailures`; at a limit of one -- which a plain
   * `local[n]` master forces, and which is a legitimate cluster setting besides -- that single
   * failure aborts the stage and the job. Graceful degradation that aborts the job is not graceful
   * degradation, and "every path terminates in a working shuffle" would be false for the very
   * conditions the feature exists to survive.
   *
   * So this raises a private signal that [[write]] converts into a clean completion
   * ([[completeStandDown]]). The attempt stops streaming, withdraws its void output from every
   * owner of it, and succeeds. Recomputation is then reached by the mechanism the feature names and
   * the rest of this subsystem already relies on: a consumer that can find none of the withdrawn
   * output raises an ordinary fetch failure, which does NOT count towards task failures, and the
   * unmodified scheduler resubmits the map stage -- where the latched verdict routes every attempt
   * to the sort-based writer. That the void status reports a non-zero size for every partition is
   * what makes this airtight rather than merely likely; see [[voidMapStatus]].
   *
   * A trip observed after the last block has been terminated is ignored, because at that point the
   * task's output is complete and discarding correct work would benefit nobody.
   */
  private def checkFallbackPolicy(): Unit = {
    // What reaches here, and what does not. Pressure that eviction *rescued* does not: a partial
    // grant the spill manager reversed is ordinary flow control, counted as a brush and nothing
    // more, and it must not cost a job its fast path. Pressure that eviction could not rescue does:
    // a reservation still refused after every recovery round is the specified second trip
    // condition, it is reported to the policy at that point by [[admitBlock]], and it is read here.
    // Standing down mid-production is safe because it is not a failure: the degradation below
    // reconstructs the complete map output through the sort-based writer in place, and the
    // shuffle-wide declaration withdraws the streamed output so no consumer is left reading a path
    // this executor has abandoned. Pre-flight memory pressure keeps its place too: a task that
    // cannot frame at all stands the shuffle down before it consumes a record, where the delegation
    // is total and nothing has been produced that a recomputation would have to replace.
    //
    // Two independent facts, and both must stand this producer down. The local latch is this
    // executor's own verdict on whether streaming is sustainable here; the cached shuffle-wide
    // verdict is the decision taken for this shuffle everywhere, which may have been established by
    // a trip on an entirely different executor. Consulting only the first would leave this producer
    // streaming a shuffle whose consumers have already been told to stop reading it, which is
    // precisely the split-brain the fallback protocol exists to prevent. Both are local reads, and
    // both are deliberately read as booleans: this method runs at every block boundary, so the
    // overwhelmingly common answer -- neither has fired -- costs one volatile load and one map
    // membership test and allocates nothing. The verdict itself is fetched only once standing down
    // is certain, where an allocation no longer matters.
    val remoteFallback = fallbackPolicy.shuffleHasFallenBack(shuffleId)
    if (!finished && (fallbackPolicy.hasTripped || remoteFallback)) {
      // Read across the whole closed set of causes, not only the four specified fallback
      // conditions: a verdict this executor learned from the driver may be a structural decline,
      // and this producer has to stand down for it just as firmly and report it as what it was.
      val reason: Option[StreamingShuffleStandDownCause] = fallbackPolicy.trippedReason
        .orElse(fallbackPolicy.knownShuffleFallback(shuffleId).flatMap(_.cause))
      val description = reason.map(_.description).getOrElse("an unrecorded condition")
      val name = reason.map(_.toString).getOrElse("Unknown")
      // Declared only for a condition observed here. A verdict this executor learned from elsewhere
      // is already latched at the coordinator, so re-declaring it would be an ask that changes
      // nothing.
      //
      // <b>The verdict has to be CONFIRMED before this attempt rewrites anything.</b> Finishing
      // through the sort-based writer publishes sort-based output for a shuffle whose other
      // producers may still be streaming, and whose consumers may still be reading the streamed
      // path -- one shuffle with two implementations of the same output. That is only safe once the
      // coordinator has latched the decision for every participant and withdrawn the streamed map
      // output the rewrite replaces. A declaration that did not take effect therefore leaves this
      // attempt streaming and raises, so the scheduler retries it and the retry re-attempts the
      // declaration; a retry costs throughput, whereas mixing two implementations costs
      // correctness.
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
      // Recorded, not raised. The record loop acts on it at the next iteration, which is where both
      // halves of the map output -- the records already framed and the untouched tail of the
      // iterator -- can be handed to one sort-based writer.
      pendingDegradation = Some(StreamingShuffleWriter.MidStreamDegradation(description, name))
    }
  }

  /**
   * Finishes this attempt through the sort-based writer after streaming stood down mid-production.
   *
   * '''Why this exists.''' Graceful degradation is specified to end in delegation to the sort-based
   * shuffle without failing the job, and two of the four trip conditions -- a consumer sustained at
   * half the producer's rate, and link saturation -- are runtime observations that cannot be
   * settled before the first record. Failing the attempt and relying on its retry is not a
   * delegation: it needs a retry budget that `spark.task.maxFailures=1` does not provide, and it
   * discards correct work even when it does. This method makes the degradation independent of the
   * retry budget by reconstructing the complete map output in place.
   *
   * '''How the output is made complete.''' Every record this attempt has consumed was written into
   * its partition's serialization stream and framed into blocks that the retained store still
   * holds, in memory or in a spill segment. Closing each stream makes the serializer emit its
   * trailing state, after which a partition's blocks plus the accumulator's remainder are, byte for
   * byte, the stream that a reader would have unwrapped. Deserializing that byte range yields
   * exactly the key-value pairs that went into it, and the tail of the record iterator has not been
   * touched at all -- so the concatenation of the two is precisely this map task's input, once, in
   * order.
   *
   * '''The order of the four steps is load-bearing.''' The generation is withdrawn before anything
   * is rewritten, so no consumer is offered output that is being replaced. The sort writer then
   * consumes the reconstruction lazily, so a partition's bytes are read back one block at a time
   * rather than all at once. Only when it has finished are the buffers and spill files released,
   * because releasing them earlier would delete the very bytes being read. Write metrics for the
   * streamed half are deliberately never published, so the delegate's own accounting is the only
   * accounting this attempt contributes and nothing is counted twice.
   *
   * '''The one case this cannot repair, and why it is settled before the delegate exists.''' A
   * block a consumer has both received and acknowledged has been released -- that is what the
   * acknowledgement is for -- and no producer can reproduce it. Whenever a consumer really is
   * attached during production, which is the case this whole subsystem is built for, that is not a
   * hypothetical: one advancing acknowledgement is enough to make the reconstruction short. So the
   * reconstruction is proved possible '''before''' anything is handed to the sort-based writer, by
   * [[firstUnreconstructableBlock]], rather than being discovered part way through it. The
   * difference matters twice over: a delegate that has already written half a map output when the
   * gap is found leaves the attempt failing with a partially written output behind it, and the
   * failure would arrive from inside another writer's `write` where it reads as that writer's fault
   * rather than as the exact block this one cannot replay.
   *
   * When the proof fails, the recovery is the one the platform already has and this method's own
   * first step has already begun: the generation is withdrawn from every owner of it, which
   * atomically invalidates whatever a consumer had partially consumed and makes every later fetch
   * of it a fetch failure, and the attempt then fails so that the unmodified scheduler recomputes
   * the map output -- served, because the shuffle has stood streaming down, by the sort-based path
   * from the first record. The per-block check inside [[degradedRecords]] remains as a backstop for
   * anything the two window bounds cannot see, and both name the exact block, so neither can be
   * mistaken for a generic failure.
   *
   * @param degradation the stand-down that was observed, for the log and the delegate's diagnosis
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
    // output. Proving the reconstruction complete here, between that withdrawal and the delegate,
    // is therefore the last point at which failing costs nothing that was not already invalid.
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
   * '''Why two window bounds rather than a probe per block.''' A partition's retained window is
   * always a contiguous run -- admission is dense and retirement always retires a prefix, which
   * [[MemorySpillManager.lowestRetainedSequence]] documents -- so the run
   * `[lowestRetainedSequence, lastAcceptedSequence]` is the whole replayable range, and comparing
   * it against the range this writer framed settles reconstructability for a partition in two
   * constant-time reads. Probing every sequence number would ask the store for a payload it may
   * have to read from disk, which is precisely the work this check exists to avoid starting.
   *
   * Two gaps are therefore visible here. A released prefix, which is what an advancing consumer
   * acknowledgement produces, shows as a retained window that no longer starts at zero; the first
   * block that cannot be read back is then sequence zero itself. A window that stops short of the
   * last block framed shows as a highest retained sequence below it, and the first missing block is
   * the one after the highest. Anything neither bound can see -- a block that was framed but never
   * admitted at all -- is left to the per-block check in [[degradedRecords]].
   *
   * @return the partition and the sequence number of the first block that cannot be read back, or
   *         `None` when every framed block of every active partition is still retained
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
          // Either a prefix was acknowledged and released, or nothing is retained at all. In both
          // cases the first block a reconstruction would ask for is the first one ever framed.
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
   * The remainder is taken whole rather than framed into a block, because it is about to be
   * deserialized in this JVM and never sent: framing it would charge the buffer budget, occupy a
   * sequence number and queue a reference for consumers that are no longer being served. Its size
   * is bounded by one block's capacity plus whatever the serializer emits while closing.
   *
   * @param state the partition to close
   * @return the trailing bytes of that partition's stream, or `null` when it ended on a block
   *         boundary and there are none
   */
  private def closeForDegradation(state: PartitionEgressState): Array[Byte] = {
    if (state.closed) {
      null
    } else {
      // Claim closure before invoking the serializer. Closing may flush a full segment, and that
      // callback may itself stand the shuffle down; cleanup must not then close this stream again
      // against an accumulator whose last segment was already detached.
      state.closed = true
      state.stream.flush()
      state.stream.close()
      state.accumulator.takeRemaining()
    }
  }

  /**
   * The key-value pairs one partition's streamed bytes hold, read back in the order they went in.
   *
   * Read lazily, one block at a time, through a `SequenceInputStream` so that a partition wider
   * than the buffer budget is not materialised in heap to be rewritten. The byte range is unwrapped
   * with `SerializerManager.wrapStream` under the same `ShuffleBlockId` the writer wrapped it with,
   * which is what makes compression and encryption round-trip.
   *
   * @param state the partition whose blocks are to be read back
   * @param tail the trailing bytes [[closeForDegradation]] detached, or `null` if there were none
   * @return the partition's records
   */
  private def degradedRecords(
      state: PartitionEgressState,
      tail: Array[Byte]): Iterator[Product2[K, V]] = {
    val partitionId = state.partitionId
    val admitted = (0L until state.nextSequenceNumber).iterator.map { sequenceNumber =>
      new ByteArrayInputStream(spillManager.retainedPayload(partitionId, sequenceNumber).getOrElse {
        // The backstop for a gap the window bounds cannot see; [[firstUnreconstructableBlock]] has
        // already refused every gap they can, before the delegate existed. Same message either way,
        // so an operator reads one explanation and a test asserts one string.
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

  /**
   * Turns this executor's fallback trip into a decision the whole shuffle agrees on.
   *
   * Standing this task down is necessary but not sufficient. The trip that was observed here is a
   * property of this executor, while the shuffle it was serving is produced and consumed by many
   * executors at once; leaving them streaming would mean the recomputation this task's failure
   * triggers is served by the sort-based path on one executor and by the streaming path on the
   * others, which is two producers of the same output. The declaration is what makes the decision
   * shuffle-wide: the coordinator latches it, invalidates the partial streaming output, retires
   * every live producer so none can re-register, and answers every later participant with the
   * verdict rather than with an address.
   *
   * It is claimed through the policy, so the ask happens once per shuffle per executor rather than
   * once per task -- every writer and reader of this shuffle running here observes the same latch
   * at the same moment. And it happens '''before''' the exception is thrown, because the exception
   * is what ends this task, and a declaration deferred until after it would race the retry it
   * exists to redirect.
   */
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
   * A consumer resolving no producer for a map index cannot otherwise distinguish "this index was
   * never produced" from "its producer has finished and gone", and the two demand opposite
   * behaviour: keep waiting, or stop. Reporting completion here -- on the successful stop, after
   * the final drain, once every partition has been terminated -- is what supplies the distinction,
   * and doing it only on success is what keeps it honest, because a failed task's output is exactly
   * what a consumer must not be told is complete.
   *
   * '''The answer is a precondition of publishing, not a courtesy.''' The driver applies the report
   * only while this generation is still the registered one and has not been retired, so a refusal
   * is the one authoritative statement that this attempt's output has been disowned -- by a
   * shuffle-wide stand-down, by a newer attempt, or by a consumer that invalidated it. Publishing
   * an ordinary map status after a refusal would re-register output the driver has just withdrawn,
   * which is why [[completeSuccessfully]] asks for this answer immediately before it builds that
   * status and acts on it. An unreachable driver is a different answer again and is not a refusal:
   * nothing is known, and the consumer's own poll and timeout recover a report that was lost.
   *
   * @return `Some(true)` when the driver applied the report, `Some(false)` when it refused it, and
   *         `None` when the driver could not be asked
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
    val startedAtNanos = clock.nanoTime()
    // Every declared partition is terminated in one pass: those this task produced records for,
    // whose blocks were emitted just above, and those it produced nothing for, whose terminator
    // legitimately reports zero blocks. One call rather than one per partition is what keeps
    // finishing linear in the partition count -- a terminator has to be followed by an egress
    // drain, and requesting one drain per partition would repeatedly schedule and revisit the same
    // ready sessions for identical wire output.
    serverHandler.terminateStreams(0 until declaredPartitions)
    serverHandler.flushPending()
    writeMetrics.incWriteTime(clock.nanoTime() - startedAtNanos)
    drainAcknowledgements()
    publishWriteMetrics()
  }

  /**
   * Flushes and closes one partition's serialization stream, emits its tail and marks it ended.
   *
   * The stream is closed before the tail is cut, because closing is what makes the serializer emit
   * whatever trailing state it has been holding. Only after that is the accumulator's remainder
   * known to be the complete end of the partition's byte range.
   *
   * The partition's framing share is returned here rather than at the end of the task, and the
   * difference matters under pressure. A closed stream with a released accumulator will never frame
   * again, so its share is dead weight from this point on -- and finishing walks the partitions in
   * turn, each one emitting a tail block that needs room in the very allowance those shares occupy.
   * Holding all of them until the task ends makes the last partitions of a wide shuffle compete
   * with the framing scratch of partitions that finished long before. Returning each share as its
   * partition ends lowers the peak an executor ever holds, and the release is idempotent, so
   * [[closeSerializationResources]] still accounts for whatever a failed task never reached.
   */
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
      // by the caller, after this loop has emitted the last block of each of them. Both are needed:
      // the ledger is what a stalled-consumer decision and the final drain read, and one that never
      // learns a stream ended keeps treating its unacknowledged window as a stream still in flight.
      backpressure.onStreamTermination(producerKey(state.partitionId), state.nextSequenceNumber)
      state.terminated = true
    }
  }

  /**
   * Pushes everything still owed to a consumer, waiting up to a bound for pacing to allow it.
   *
   * A single non-blocking pass is not enough here and the difference is output, not latency. Egress
   * is rate limited, so a block admitted in the last moments of a map task is very often still
   * queued when this method is reached; one attempt at pushing it would leave it queued, the task
   * would report success, and the reduce side would wait out its producer-liveness detector for
   * bytes that were never sent. Waiting gives pacing the chance to release them, bounded so that a
   * consumer which has stopped reading cannot hold this task thread indefinitely.
   *
   * What is '''not''' lost when the bound is reached is the point of the retained store. A block
   * offered for egress is admitted to that store first and is retired only once every subscribed
   * consumer has acknowledged it, so anything still queued here remains durably held -- in memory
   * or in a spill segment -- and is replayed in full when its consumer subscribes or asks for it.
   * The bound therefore costs a replay, never a byte, which is what makes reporting success honest.
   *
   * A failure is logged rather than raised, for the same reason: the bytes are retained, and a
   * consumer that receives nothing recovers through its own detector and the stage recomputation
   * the unmodified scheduler performs. Converting it into a map-task failure here would duplicate
   * that detection less accurately. What the caller must not do is *skip* the durability step on
   * the strength of a successful drain, which is why the verdict is returned rather than swallowed.
   *
   * The wait is taken in slices with a maintenance pass between them, and that is what keeps the
   * liveness service running through it. A single blocking wait would leave a consumer that is
   * still receiving retained blocks without a producer heartbeat for the whole of the drain window
   * -- longer than its own five-second detector -- so the very wait that exists to deliver its
   * bytes would provoke it to declare the producer dead. Acknowledgements arrive on the transport's
   * own threads throughout and need nothing from this loop; heartbeats and replay service do.
   *
   * A failure surfaced on an I/O thread ends the wait rather than being waited out, and it is
   * reported as a failed drain rather than raised. Raising it would fail a map task whose output is
   * durably retained and therefore perfectly readable -- the channel failed, not the output -- and
   * the caller's durability requirement is the check that decides whether success is honest.
   *
   * @return true when every subscribed consumer has been sent everything it was queued
   */
  private def drainBeforeStopping(): Boolean = {
    var delivered = false
    try {
      val deadlineMillis = clock.getTimeMillis() + FINAL_DRAIN_TIMEOUT_MS
      var remaining = FINAL_DRAIN_TIMEOUT_MS
      // Bounded by slice count as well as by the deadline. The deadline alone is a bound only while
      // the clock advances, and this writer takes its clock by injection precisely so that a suite
      // can hold it still; counting slices makes the loop terminate either way.
      var slices = 0
      delivered = serverHandler.awaitDrain(math.min(remaining, MAINTENANCE_INTERVAL_MS))
      while (!delivered && remaining > 0L && slices < MAX_DRAIN_SLICES) {
        slices += 1
        val nowMillis = clock.getTimeMillis()
        drainAcknowledgements()
        sendHeartbeatsIfDue(nowMillis)
        // The coordinator's liveness window is the same length as this drain's deadline, so a drain
        // that runs to its deadline without refreshing would have this producer's address reaped at
        // exactly the moment its last blocks were being delivered. The answer is deliberately
        // ignored here: see [[standDownIfRetired]] for why a retirement must not raise inside a
        // successful stop, and where it surfaces instead.
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
   * '''How much this writes is decided by the consumers, not by this method.''' What it makes
   * durable is the retained window, and a block leaves that window as soon as the consumer reading
   * it acknowledges it. So this is not a materialisation of the map output: a partition whose
   * consumer kept pace has nothing retained by the time the task stops, and this step writes zero
   * bytes and registers zero files for it. It writes a whole partition only in the case where
   * nothing consumed it while it was being produced -- which is the case the unmodified scheduler
   * produces for a map stage that runs once, and the reason the honest description of streaming in
   * that configuration is the removal of the index-and-fetch round trip rather than overlap.
   *
   * The step still runs unconditionally, and deliberately so even when it has nothing to write. The
   * transfer of an empty file set is accepted on the same terms as a full one, which leaves the
   * resolver holding a registration for this generation; a consumer that reconnects afterwards then
   * receives a typed answer about a block that no longer exists instead of finding no producer at
   * all, and recovers through a fetch failure and recomputation. Skipping the step to save a
   * round trip would trade that for silence.
   *
   * What is durable rather than resident is not a design preference either. Buffered blocks live in
   * task-managed execution memory, which the executor reclaims at task completion and -- with
   * `spark.unsafe.exceptionOnMemoryLeak` enabled, as the test envelope sets it -- fails the task
   * over if anything still holds. So "keep it in memory until the consumer acknowledges it" is not
   * an option that was passed over for a cheaper one: it is not available at all. Spill files,
   * being ordinary files in the local directories, are.
   *
   * Three steps in this order, and the order is the guarantee:
   *
   * 1. Force whatever remains of the retained window to disk. Only unacknowledged blocks are still
   *    retained, so this writes exactly what a reconnecting consumer may ask for, nothing that has
   *    already been retired, and nothing at all when everything has been.
   * 2. Detach file ownership from this task's spill manager, so that its task-completion listener
   *    frees memory as it always did but unlinks none of the files.
   * 3. Register the files with the executor-scoped [[StreamingShuffleBlockResolver]], which unlinks
   *    them when the generation is superseded, when the shuffle is unregistered, or when the
   *    resolver stops -- the boundary the feature specifies.
   *
   * A refused transfer means the resolver no longer holds this generation's registration: either a
   * newer attempt of the same map has replaced it, or the executor is shutting the subsystem down.
   * Either way this output must not be served, and nothing else will delete the files now that they
   * are detached, so they are unlinked here. Refusal is reported rather than hidden, because the
   * caller uses it to decide whether reporting success would be honest.
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

  /**
   * Unlinks spill files whose ownership was released and then refused.
   *
   * Reached only when [[StreamingShuffleBlockResolver.retainProducerOutput]] declines the transfer,
   * which it does exactly when this generation is no longer the registered one. Its output is
   * invalid to serve and nothing else holds a reference to these files, so the releasing side
   * unlinks them rather than leaving them for the application directory sweep.
   */
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
   * Reporting success publishes a map status, and a map status is a promise that every reduce
   * partition of this output can be read. The promise is kept by exactly one thing: the retained
   * window is reachable -- either nothing is retained because every registered consumer has
   * acknowledged everything, or what is retained is on disk and owned by the executor-scoped
   * resolver. That is precisely what `durable` reports. When it is false the promise cannot be
   * kept, and a map status published anyway becomes a fetch failure against a producer the
   * scheduler believes succeeded, which is the one failure shape that recovers slowly and
   * confusingly.
   *
   * Failing here instead hands the scheduler the ordinary case it already handles well: a map task
   * that did not succeed, retried as a fresh attempt.
   *
   * <b>Why the test is durability alone and not "neither delivered nor durable".</b> The weaker
   * disjunction is vacuous in the case that matters most. A map stage whose reduce stage has not
   * started yet has no subscribed consumer, so there is nothing queued, so the drain reports itself
   * satisfied -- `delivered` is true while not one byte has reached anybody. Accepting that as half
   * of the test would let exactly the shuffle whose output is entirely un-received skip the
   * durability requirement. Conversely every way `durable` can be false is genuinely unrecoverable:
   * bytes that could not be evicted die with this task's memory, and a refused transfer means this
   * generation is no longer the registered one, so its output must not be served at all. The drain
   * result is therefore reported in the diagnosis rather than used to excuse the failure.
   *
   * Nothing is released here. This raises from inside the successful stop sequence, and that
   * sequence retires the generation and frees everything it holds on its way out -- see [[stop]]
   * for why the protocol performs the failure cleanup itself rather than leaving it to the caller.
   * Releasing here as well would duplicate an operation whose single owner is the point of it.
   *
   * @param delivered whether the final drain wrote everything that was queued, for the diagnosis
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
   *
   * '''Why one more check, after everything else has passed.''' Every failure the egress path
   * observes arrives on a Netty thread, long after the call that issued the write returned, and the
   * notifier exists precisely to carry it across to the task thread. The write loop consults it on
   * every block and the final drain consults it on every pass -- but the drain's own pass is
   * best-effort by design and swallows what it catches, so the last thing to fail is the one thing
   * that could go unreported. Without this call a channel failure could be logged on an I/O thread
   * and a successful map status published anyway, which is the single worst outcome available: the
   * scheduler would record the attempt as done and would learn otherwise only when a reduce task
   * failed its fetch, one stage later and with a diagnosis pointing at the wrong task.
   *
   * '''Why it is placed after the durability requirement rather than before it.''' Both refuse the
   * same publication, so only their diagnoses differ, and [[requireOutputRecoverable]] gives the
   * more actionable one: it names how many bytes are unreachable and why. A latched channel failure
   * is frequently the *cause* of the unreachability it would otherwise mask, so letting the
   * specific message win and keeping this as the backstop reads better in a log than the reverse.
   *
   * Nothing is released here, for the reason [[requireOutputRecoverable]] documents: raising from
   * inside the successful stop sequence hands the work to that sequence's own catch, which retires
   * the generation and frees everything it holds exactly once.
   */
  private def requireNoLatchedFailure(): Unit = errorNotifier.throwIfError()

  /**
   * Reports every producer stream of this map output complete on the local ledger.
   *
   * [[finishPartition]] already reports each stream as it is terminated, so on the ordinary path
   * this finds nothing left to do. It exists for the partitions this task produced no record for:
   * those have no [[PartitionEgressState]], are terminated directly against the handler, and would
   * otherwise leave the ledger believing a stream that ended is still in flight.
   *
   * Idempotent, so it may be called from more than one place in the stop sequence.
   */
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

  /**
   * Unregisters every producer stream this writer registered.
   *
   * Registration is not free bookkeeping: a registered stream holds a credit ledger, an
   * unacknowledged window and the utilisation contribution that the cross-shuffle arbitration
   * reads. A writer that registered and never unregistered would leave that ledger on the
   * executor-wide protocol for the life of the executor, one entry per reduce partition per map
   * task, and the arbitration would keep weighing shuffles whose tasks ended long ago.
   *
   * Called from the task-completion listener, which is what makes it cover success, failure and
   * cancellation with a single call site rather than three that can drift apart. Idempotent, so the
   * listener firing after an explicit release changes nothing.
   */
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

  // Telemetry (FR-7)

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

  /**
   * Emits the summary a successful streaming map task contributes to the executor log, bounded per
   * executor rather than per task.
   *
   * <b>Why the bytes-on-the-wire figure is gone from here.</b> It was structurally always zero, and
   * reporting a figure that cannot be anything else invites exactly the misreading it invited: that
   * the transport had carried nothing. This method runs inside the writer's `stop`, which is the
   * end of the *map* task -- and the unmodified DAG scheduler submits no reduce task until the
   * whole map stage has finished, so at this instant no consumer has subscribed and none can have
   * been sent a byte. The egress this producer's output really does receive happens afterwards,
   * through the executor-scoped `StreamingShuffleListener`, and it is measured there: see
   * `StreamingShuffleListener.streamedBytes`, which accumulates across producers and survives their
   * deregistration precisely so that "did this executor stream anything" is answerable after the
   * fact rather than at the one instant the answer is guaranteed to be no.
   *
   * What remains here is what this task alone can answer for: what it produced, how it was framed,
   * and which bounds it met while producing it.
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
        // At the level it was emitted at, under the feature's own key. See the budget record above.
        if (debugEnabled) {
          logInfo(entry)
        }
    }
    logEgressCeilings()
    logProtocolAnomalies()
  }

  /**
   * Reports the egress ceilings this map task's consumers ran into, when any of them did.
   *
   * Conditional on purpose, and the condition is the whole design of the line: every figure it
   * carries is zero for a shuffle whose consumers kept up, so emitting it unconditionally would add
   * a line per map task saying nothing. A non-zero figure, by contrast, is the only way an operator
   * learns that a bound engaged -- that a channel was refused, that a consumer identity went
   * untracked, or that a queue ceiling deferred references onto an owed run -- and each of those is
   * a bounded response to remotely induced pressure rather than a fault. What the numbers diagnose
   * is the pressure, not the bound: deferrals climbing while output is not moving points at a
   * consumer that has stopped acknowledging, and refused channels point at a peer opening
   * connections it never uses.
   */
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

  /**
   * Reports the protocol anomalies this map task's egress observed, when it observed any.
   *
   * These counters are the producer's side of every consumer behaviour the protocol tolerates
   * rather than trusts: an acknowledgement refused because it reached past what was sent, a
   * retained block a consumer asked for that could no longer be served, a session resumed after a
   * reconnect, a duplicate acknowledgement, a session or a consumer retired for silence, a session
   * superseded by the same consumer reconnecting, a consumer identity that changed mid-session, a
   * frame addressed to a stream this handler does not serve, and a block replayed on request.
   * Individually each is a handled case; together, and counted, they are how an operator
   * distinguishes a slow consumer from a broken one, and a reconnecting consumer from a peer
   * probing the port.
   *
   * The anomalies are rendered as a list of only those that occurred rather than as a fixed set of
   * figures, so the line says what happened instead of mostly saying that nothing did, and it is
   * suppressed entirely when nothing did. The two consumer counts are carried as context whenever
   * the line fires, because every figure above is per-consumer in origin and meaningless without
   * knowing how many there were.
   */
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

  // Cleanup. Every release below is idempotent, because it may be reached from stop() and from task
  // completion, and because a map task may call stop() twice.

  /**
   * Registers the two releases that must run whatever becomes of this task, exactly once.
   *
   * '''Order matters and is the reason both are registered here rather than wherever each is
   * needed.''' Completion listeners run in reverse registration order, so the spill manager's is
   * installed first and this writer's second, which makes this writer's run first: egress is
   * released while the buffers it referenced are still alive. Registering them apart would leave
   * that ordering to whichever call site happened to run first.
   *
   * '''Why this is called from [[prepareStreaming]] and not only from
   * [[publishStreamingProducer]].''' The framing reservation is taken during preparation, before
   * anything is published, so publication is too late to be the moment a release hook first exists.
   * A task killed between the reservation and the publication would then hold executor memory with
   * nothing registered to return it, and the test JVM -- which fails any task that retains task
   * memory -- would report a leak rather than the cancellation that caused it. Both registrations
   * are idempotent, so calling this from both points installs one of each.
   */
  private def installCleanupListeners(): Unit = {
    if (!cleanupInstalled) {
      cleanupInstalled = true
      spillManager.registerCleanup(context)
      context.addTaskCompletionListener[Unit](_ => releaseOnTaskCompletion())
    }
  }

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

  /**
   * Closes every serialization stream and frees every accumulator.
   *
   * Freeing the accumulators matters as much as closing the streams: each one holds a byte array
   * sized to a block, so leaving them referenced would retain a multiple of the block size per
   * partition for as long as anything holds this writer. The test JVM enforces zero retained task
   * memory, so this is machine-checked rather than merely intended.
   *
   * Each accumulator's budget reservation is returned as its array is dropped, and in that order:
   * the charge exists to bound the allocation, so releasing it before the array had gone would
   * briefly let another partition allocate against room that was still occupied.
   *
   * The undrawn remainder of the up-front framing reservation is returned too. A task that wrote to
   * a subset of the shuffle's partitions never draws the shares of the rest, and those bytes are as
   * real a charge against the executor's allowance as the drawn ones -- so the total released here
   * always equals the total [[prepareStreaming]] reserved, whichever partitions the task touched.
   */
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
   *
   * The condition is the point of this method. A successful map task hands its retained window to
   * the executor-scoped resolver and stays registered with the executor's single streaming
   * listener, because a consumer is entitled to read this output after the task has ended -- both
   * one resuming a stream it was already being served and one attaching for the first time, which
   * under the unmodified scheduler is the ordinary case, since it starts no reduce task until the
   * whole map stage has finished. Releasing here on the successful path would clear the
   * per-partition stream records, and with them the termination state such a consumer needs in
   * order to be told the stream ended, so a shuffle that had produced its output perfectly well
   * would still never complete.
   *
   * Both terms are required, and each is owned by the component that can answer it: this writer
   * knows whether it completed the hand-off at all, and the handler knows whether what it would
   * serve is still published to it. Failure, cancellation, a refused hand-off and a superseded
   * generation each fail one term or the other, and in every one of those cases the handler can
   * serve nothing, so its queues and sessions must go.
   *
   * Retiring a handler that is deliberately left alive is not this writer's business. The executor
   * scoped listener drops it when the shuffle is unregistered or when a newer generation of the
   * same map replaces it, which is the same boundary that retires the bytes behind it.
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
   * <b>Why this exists at all.</b> A streaming producer is published to owners outside this writer
   * *before* the writer is constructed, because a consumer must be able to resolve and reach a
   * producer from the instant the driver hands out its address. Three of those owners survive the
   * writer:
   *
   *  - '''this executor's routing table''', which the single streaming listener consults to find
   * the    handler that serves a map output;
   *  - '''the driver's producer registry''', which is the address a reduce task resolves;
   *  - '''this shuffle's share of the executor's egress allowance''', which is a divisor of every
   *    other live shuffle's token rate.
   *
   * A delegated attempt reaches none of the paths that retire them. `prepareStreaming` refuses
   * before the first record, so `initializeStreaming` never runs and its task-completion listener
   * is never registered; and from the moment `sortDelegate` is set, `stop` forwards to the delegate
   * rather than running this writer's own stop sequence. Without this method the attempt would
   * therefore leave a routable producer address behind that answers no consumer, and an egress
   * divisor permanently one too high -- pacing every shuffle that outlives it below its true share.
   *
   * <b>Why the writer and not the manager.</b> The manager publishes these owners and can unwind
   * them when it is the one that refuses, but the decision this method serves is taken inside the
   * writer, after construction, on state only the writer can read -- the protocol version of the
   * handle, the latched fallback verdict, and whether the buffer budget can frame a single block
   * for this shuffle's width. Handing the writer an explicit obligation to withdraw what was
   * published for it is what keeps that decision and its unwind in one place, rather than splitting
   * the ownership of a generation across two components.
   *
   * <b>What is deliberately kept.</b> The backpressure registration is shuffle-scoped rather than
   * task-scoped and is shared with every other map task of the same shuffle on this executor, so it
   * is released only when no stream of the shuffle remains here -- which is exactly the condition
   * checked below. The two task-completion listeners the spill manager may hold are kept because
   * they are idempotent, fire on any outcome, and releasing memory twice is not something a
   * delegated attempt should have to be careful about.
   *
   * Contained rather than propagating: this runs on the path that keeps a task alive by delegating,
   * and failing it would turn a successful sort-based map output into a failed task.
   *
   * @param detail operator-facing context, recorded with the withdrawal on the driver and locally
   */
  private def withdrawPrePublishedOwnership(detail: String): Unit = {
    val reason = s"streaming was stood down before its first record because $detail"
    // Retires the driver's producer address, this executor's routing entry, the retained-output
    // registration and the handler's sessions -- every owner of the generation, through the one
    // operation that knows how to retire all of them.
    withdrawGeneration(reason)
    // The buffers and the framing scratch this attempt reserved. Reached here rather than left to
    // the spill manager's own completion listener because a delegated attempt has no streaming
    // state to keep alive for a late consumer, and holding executor memory for a producer that will
    // never stream a byte would shrink the budget the sibling shuffles are pacing themselves
    // against.
    closeSerializationResources()
    closeSpillState()
    // The shuffle's share of the egress allowance, returned only once no stream of it remains
    // registered on this executor. A sibling map task still draining its unacknowledged window is
    // still occupying the link, and dropping its ledgers would leave those bytes unpaced; that
    // share is returned by `unregisterShuffle` when the shuffle itself ends.
    guardedRelease("return this shuffle's share of the executor's egress allowance") {
      if (backpressure.streamCount(shuffleId) == 0) {
        val dropped = backpressure.unregisterShuffle(shuffleId)
        if (debugEnabled) {
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} returned the executor " +
            log"state of map ${MDC(TASK_ATTEMPT_ID, mapId)} before delegating to the sort-based " +
            log"writer, dropping ${MDC(COUNT, dropped)} stream ledger(s)")
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
   * Every caller is unwinding state on a path whose whole purpose is to keep a task alive -- either
   * by delegating it to the sort-based writer or by finishing a failure that is already being
   * handled -- so a bookkeeping step that fails must not become the outcome. The step is named in
   * the diagnostic so that a partial unwind can be attributed.
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
   * The two steps below are one unit and are always taken together, which is why they have a name
   * rather than being written out at each call site: the withdrawal stops every owner -- the driver
   * registry, this executor's routing table, the retained-output registry and the handler's own
   * sessions -- from offering this generation, and the close returns the memory and the disk it
   * holds. Both halves are idempotent, which is what lets every caller reach it in any order: the
   * unsuccessful stop, a success stop that could not finish, a failed initialisation, and the
   * pre-flight degradation that hands the whole attempt to the sort-based writer.
   *
   * That last caller is the one worth naming, because its attempt does not fail: it succeeds, with
   * a complete map output written by the unmodified sort-based path. What will not succeed is this
   * '''generation''', and the distinction is the point -- a generation that is never going to
   * stream must stop being reachable exactly as a failed one must, or a consumer will resolve its
   * address and wait on a producer that has already handed its work to another writer.
   *
   * @param reason operator-facing context, recorded with the withdrawal on the driver and locally
   */
  private def releaseAfterFailure(reason: String): Unit = {
    withdrawGeneration(reason)
    unregisterProducerStreams()
    // The buffers are this task's execution memory, so releasing them is the one part of the
    // withdrawal only the task thread may do.
    closeSpillState()
  }

  /**
   * Retires this generation from every owner of it on this executor.
   *
   * Reached only through [[releaseAfterFailure]], and so only for a generation that will not
   * stream. A successful streamed map output stays published and routed for as long as its
   * consumers may read it, which outlives this task: it is retired when the shuffle is
   * unregistered, when a newer generation of the same map supersedes it, or when this executor's
   * manager stops -- never when the producing task happens to finish.
   *
   * A failed attempt's output, by contrast, is not merely unneeded but wrong to serve, because it
   * is partial by definition; and a delegated attempt's generation has no output at all, because it
   * was retired before the first record was consumed. Delegating to the handler rather than
   * unregistering the resolver here is what makes the retirement complete in both cases: this
   * writer holds the retained-output owner but not the routing table, so unregistering what it has
   * would leave a routing entry behind for the whole life of the executor.
   *
   * The driver is told first, and it has to be. It owns the address a consumer resolves and is the
   * one owner this executor cannot retire for itself; withdrawing it before the local owners go
   * means no consumer is handed an address that is about to refuse it. Waiting for the driver's own
   * liveness window to reap the registration instead would leave that address on offer for the
   * whole of that window.
   *
   * Contained rather than propagating, because this runs while a failure is already being handled.
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

  /**
   * Frees every buffer and deletes every spill file, for an attempt that will not succeed.
   *
   * Reached only through [[releaseAfterFailure]]. Nothing retained for retransmission can ever be
   * asked for again once the attempt has failed, so waiting for task completion to release it would
   * hold executor memory and disk for no purpose. On the successful path this is deliberately not
   * called: the spill manager's own completion listener releases it, which keeps the retention
   * window alive for the short remainder of the task in case a consumer asks for a replay.
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

  // Inspection. Pure reads, exposed so that what this writer did is observable without reaching
  // into its internals or standing up a live cluster to watch it.

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

  /**
   * Spill events observed by this writer, as counted by the spill manager: evictions the budget
   * forced and blocks written straight to disk because the allowance could not admit them. The
   * task's own summary reports how many took the second route.
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

  /**
   * What stood this attempt's streaming down mid-write, if anything did.
   *
   * Non-empty means the attempt stopped streaming and completed anyway: its output was withdrawn
   * and the map stage is recomputed on the sort-based path through an ordinary fetch failure, with
   * no task failure counted against `spark.task.maxFailures`. The value is one of the four
   * specified fallback conditions or a structural decline, and it is reported as whichever it was.
   */
  def standDownFallbackReason: Option[StreamingShuffleStandDownCause] = standDownReason

  /** Operator-facing description of why streaming stood down mid-write, if it did. */
  def standDownDescription: Option[String] = Option(standDownDetail)

  // Internal state types

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

    /**
     * Serialized bytes produced for this partition and not yet handed off as a complete segment.
     *
     * The callback runs synchronously from the serializer's `OutputStream.write` call. A record
     * larger than one block is therefore emitted block by block while it is being serialized rather
     * than being materialized as one growable array before the writer notices a boundary.
     */
    val accumulator: BlockAccumulator =
      new BlockAccumulator(initialCapacityBytes, payload => emitBlock(this, payload))

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

    /**
     * Bytes reserved from the buffer budget for this partition's accumulator.
     *
     * Held on the state rather than recomputed at release time so that exactly what was reserved is
     * what is returned, even if the block capacity were ever derived differently between the two
     * moments.
     */
    var scratchReservedBytes: Long = 0L

    /** Whether the serialization stream has been closed. */
    var closed: Boolean = false

    /** Whether end of stream has been signalled for this partition. */
    var terminated: Boolean = false
  }

  /**
   * A fixed-segment output stream that emits a block at the instant its segment fills.
   *
   * A growable accumulator cannot enforce the executor quota: one serialized record can be much
   * larger than a block, and a serializer writes that record before returning control to the record
   * loop. Growing until that return materializes the whole record, while cutting blocks afterwards
   * repeatedly shifts its remainder. Fixed segments remove both defects. The backing array never
   * grows, a full array is handed to the retained store without copying, and a write spanning many
   * blocks advances through the caller's source array once.
   *
   * `close` and `flush` are no-ops on purpose. The serializer owns their timing; a full segment is
   * already emitted by `write`, and the bounded tail is detached only after the serializer has
   * emitted its closing bytes.
   *
   * Not thread safe, and never touched from anywhere but the task thread.
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
     * A full segment can never be present here: it is emitted synchronously by the write that
     * filled it. The only copy left in this design is therefore one final copy of less than one
     * block, never one copy per block and never a shift of an arbitrarily large remainder.
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
        // The next segment is allocated whether the hand-off returned or raised. The callback
        // admits the block, and admission is exactly where a stand-down or an unretainable block is
        // raised from -- so on that failure path the accumulator would have been left holding the
        // empty sentinel, and the serialization stream's own closing flush, which runs while the
        // attempt is being finished through the sort-based writer, would have struck a zero-length
        // array. That surfaced as an ArrayIndexOutOfBoundsException logged as "could not close the
        // serialization stream" and swallowed, hiding the condition that really stopped the write.
        // Allocated after the sentinel rather than before the hand-off so that only one segment is
        // ever held against the standing scratch charge.
        try {
          emitFullSegment(full)
        } finally {
          buffer = new Array[Byte](segmentCapacityBytes)
        }
      }
    }
  }
}

/**
 * Constants of the streaming shuffle producer.
 *
 * The timing figures are the feature's own, restated here as named values so that the write path
 * reads as prose and every consumer of a figure names it rather than repeating its digits. Where a
 * figure is also a constant of a collaborator, it is taken from that collaborator rather than
 * repeated, so that the two can never drift apart.
 */
private[spark] object StreamingShuffleWriter {

  /**
   * A shuffle-wide stand-down observed by a producer that had already consumed records.
   *
   * Carried as a value rather than as an exception because the response to it is to finish the map
   * output through the sort-based writer, not to fail: an exception would have made the delegation
   * conditional on a retry budget that may be one attempt deep. Both renderings the log and the
   * delegate's diagnosis need are captured at the moment of observation, because the policy's
   * latched verdict is a shared object that another task may advance afterwards.
   *
   * @param description the operator-facing reason, as the fallback reason describes itself
   * @param name the reason's stable name, for a message that has to be matched rather than read
   */
  final case class MidStreamDegradation(description: String, name: String)

  /**
   * How far a writer's stop protocol has progressed.
   *
   * Sealed and enumerated rather than encoded in booleans because the responses the protocol owes
   * are not a product of independent flags: see [[StreamingShuffleWriter.stop]] for the five
   * (state, requested outcome) combinations and the single correct response to each.
   */
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
   * '''A control signal, not a failure, and the difference is the point.''' It is raised at the two
   * places a mid-write trip is detected -- a block boundary and a partition's first framing
   * reservation -- and is caught by [[StreamingShuffleWriter.write]] alone, which turns it into a
   * completed task whose output has been withdrawn. It never leaves the writer, so nothing outside
   * this class can mistake it for a task failure, and `spark.task.maxFailures` is never charged for
   * a graceful degradation.
   *
   * Deliberately '''not''' a `scala.util.control.ControlThrowable`: that type is excluded by
   * `NonFatal`, so every enclosing `case NonFatal(e)` in this subsystem would let it escape as
   * though it were fatal. It is an ordinary exception caught by type, ahead of the `NonFatal`
   * handler that follows it. Neither a stack trace nor suppression is recorded, because it carries
   * a decision rather than a diagnosis and is raised on a hot path.
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
   * One builder rather than two literals so that an operator reads a single explanation whichever
   * check refused, and so that the two can never drift into describing the same condition
   * differently. It names the exact block, because "a block was released" and "the shuffle failed"
   * demand completely different investigations.
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

  /**
   * Executor-scoped aggregation of the "block written straight to local disk" report.
   *
   * On this object rather than in a writer, because the log-volume budget is per executor and an
   * executor runs one writer per map task: a bound expressed per writer admits one default-level
   * record per task, which is a log volume proportional to the stage's task count rather than the
   * one-per-window bound the aggregation exists to provide. Reusing the spill manager's aggregator
   * type keeps every recurring condition in the subsystem bounded by the same mechanism and window,
   * so an operator reads one aggregation convention rather than several.
   */
  private[streaming] val durableAdmissionLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /**
   * Executor-scoped aggregation of the two records every streaming map task would otherwise emit:
   * the budget it starts with, and the summary of what it streamed.
   *
   * <b>Why these two had to be bounded.</b> They are per task, not per event, which reads as
   * inherently modest -- and is exactly why they were the largest single contributor to this
   * subsystem's log volume. An executor's task rate is set by the workload, not by this feature, so
   * two records per task is a volume proportional to throughput: at a few hundred bytes each, an
   * ordinary five map tasks per second per executor already exceeds the ten megabytes an hour this
   * feature is allowed, and a task-dense workload exceeds it by more than an order of magnitude.
   * Nothing about either record is per-record or per-block, so no amount of tuning inside a task
   * helps; the count of tasks is the problem, and a per-executor window is the only bound that
   * addresses it.
   *
   * <b>What is not lost.</b> Both records describe a condition that is identical across the tasks
   * of a stage -- the same budget, derived from the same configuration, and a summary whose
   * interesting figures are cumulative rather than per task. The admitted record therefore quotes
   * the executor's running totals and states how many tasks it stands in for, so the aggregate says
   * strictly more about the executor than any single task's line did. Per-task granularity is
   * restored in full by `spark.shuffle.streaming.debug`, which is the contract the whole subsystem
   * uses for detail.
   *
   * Two aggregators rather than one, because a stage's start records must not silence its
   * summaries: a shared window would let whichever record arrived first in a window suppress the
   * other for the rest of it, and the summary is the one an operator reads to learn what actually
   * happened.
   */
  private[streaming] val budgetLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val summaryLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /**
   * Returns the executor-scoped log aggregation above to its initial state.
   *
   * Reached only through [[MemorySpillManager.resetSharedStateForTesting]], so that a suite has one
   * call to make rather than one per component, and never called in service for the reason set out
   * there: the bound belongs to the executor's lifetime.
   */
  /**
   * Executor-scoped aggregation of the two recurring producer-side stall records.
   *
   * Both were bounded per task or per partition, which is not a bound on an executor: a reclamation
   * breach is per acknowledgement and a stall is per reduce partition, and one of these writers
   * exists per map task, so the default-level volume of either scaled with the stage's task count
   * multiplied by its partition count. Neither is a condition an operator acts on line by line --
   * "consumers on this executor are behind" is the whole of the actionable content -- so an
   * executor-wide record quoting the running total, the per-task figure and how many occurrences it
   * stands in for says strictly more than a per-task line did. The streaming debug key restores the
   * individual records.
   *
   * Separate aggregators, because a run of stalls must not silence the first reclamation breach:
   * the first says consumers are slow, the second says this executor's own reclamation is, and they
   * send an operator to look at different things.
   */
  private[streaming] val reclamationBreachLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] val consumerStallLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private[streaming] def resetLogAggregationForTesting(): Unit = {
    durableAdmissionLogAggregator.reset()
    budgetLogAggregator.reset()
    summaryLogAggregator.reset()
    reclamationBreachLogAggregator.reset()
    consumerStallLogAggregator.reset()
  }

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

  /**
   * Interval at which a producer refreshes its registration with the coordinator.
   *
   * Half the coordinator's liveness window by construction, because it is the connection timeout
   * and the window is twice it. One missed refresh is therefore tolerated before the registration
   * is reaped, which is what stops a single slow ask from withdrawing a healthy producer's address.
   */
  val COORDINATOR_HEARTBEAT_INTERVAL_MS: Long =
    StreamingShuffleCoordinator.PRODUCER_CONNECTION_TIMEOUT_MS

  /** Acknowledgement gap after which a consumer is treated as unresponsive. */
  val CONSUMER_LIVENESS_TIMEOUT_MS: Long =
    StreamingShuffleServerHandler.CONSUMER_LIVENESS_TIMEOUT_MS

  /**
   * Longest the successful stop waits for paced egress to release what is still queued.
   *
   * Set to the consumer-liveness window, which is the interval a consumer itself tolerates without
   * progress before concluding that its producer is gone. Waiting longer than the peer's own
   * patience achieves nothing; waiting less would abandon egress that pacing was about to allow.
   */
  val FINAL_DRAIN_TIMEOUT_MS: Long = CONSUMER_LIVENESS_TIMEOUT_MS

  /**
   * The most maintenance slices the final drain will take.
   *
   * One more than the deadline divided by the maintenance cadence, so the count never cuts a drain
   * short that the deadline would have allowed. It exists as a second, clock-independent bound: the
   * writer's clock is injected, and a held clock would otherwise make the deadline unreachable.
   */
  val MAX_DRAIN_SLICES: Int = (FINAL_DRAIN_TIMEOUT_MS / MAINTENANCE_INTERVAL_MS).toInt + 1

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
   * Sized so that the bound is a backstop rather than the thing that decides the outcome. Rounds
   * that yield are paced by the scheduler and not by a spin, so a thousand of them outlast a spill
   * write of ordinary size by a wide margin even on a busy executor, while still terminating
   * promptly when nothing is going to arrive. A count small enough to be reached by a healthy
   * evictor would turn a transient reclamation into a task failure, which is the failure mode this
   * number exists to prevent.
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

  /** Minimum accounting charge for one accumulator, even when its fixed segment is smaller. */
  val MIN_ACCUMULATOR_BYTES: Int = 4096

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
