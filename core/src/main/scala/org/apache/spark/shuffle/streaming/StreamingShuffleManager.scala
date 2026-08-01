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

import java.net.InetSocketAddress
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{Channel, ChannelInitializer, ChannelOption, EventLoopGroup}
import io.netty.channel.socket.SocketChannel

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{CLASS_NAME, CONFIG, COUNT, EPOCH, EXECUTOR_ID, HOST_PORT,
  MAP_ID, NUM_PARTITIONS, REASON, SHUFFLE_ID, TASK_ATTEMPT_ID, THRESHOLD, VALUE}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.network.shuffle.protocol.streaming.StreamingShuffleMessage
import org.apache.spark.network.util.{IOMode, NettyUtils, TransportConf}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.shuffle.{FetchFailedException, ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId, ShuffleBlockId, ShuffleMergedBlockId, TempShuffleBlockId}
import org.apache.spark.util.Utils

/**
 * A [[ShuffleManager]] that streams map output straight from producer executors to consumer
 * executors instead of materialising it on local disk first, and that falls back to the unmodified
 * sort-based shuffle whenever streaming cannot be sustained.
 *
 * This is the service-provider entry point of the streaming shuffle subsystem and the one component
 * that wires every other component in this package together. It is reached exactly the way any
 * pluggable shuffle manager is reached: the short name `streaming` resolves through
 * `ShuffleManager.getShuffleManagerClassName` to this class, and `ShuffleManager.create` then
 * instantiates it reflectively. That is why the constructor shape below is not a matter of taste --
 * the `ShuffleManager` trait documents that a manager is instantiated by `SparkEnv` with a
 * `SparkConf` and a boolean `isDriver`, and the reflective instantiation supplies exactly those two
 * arguments.
 *
 * ==Coexistence strategy==
 *
 * The sort-based shuffle is not replaced, wrapped in a way that changes it, or modified in any
 * respect. `SortShuffleManager` remains the default (`spark.shuffle.manager` still defaults to
 * `sort`), remains byte-for-byte unchanged, and remains the fallback. This manager simply holds an
 * instance of it and delegates to it. The coexistence is realised on four axes:
 *
 *  - '''Selection (tier one).''' `spark.shuffle.manager=streaming` chooses which manager
 *    '''class''' is instantiated. Both existing short names, and the default, still resolve to
 *    `SortShuffleManager`, so an operator who changes nothing observes nothing.
 *  - '''Behaviour (tier two).''' `spark.shuffle.streaming.enabled` -- `false` by default -- gates
 *    the '''behaviour''' of this class. While the gate is closed, every one of the six
 *    service-provider methods forwards verbatim to the internally held `SortShuffleManager`, so
 *    selecting this class with the gate closed is indistinguishable from selecting the sort-based
 *    manager. That is the operator kill switch: streaming can be turned off at runtime without
 *    changing the manager class and without redeploying a different jar.
 *  - '''Degradation.''' Tier two is also the single terminus of every fallback condition
 *    [[StreamingShuffleFallbackPolicy]] recognises -- a consumer sustained at 2x slower than its
 *    producer for more than 60 seconds, memory pressure that prevents a buffer reservation, link
 *    utilisation above 90% of the administered capacity, and a producer/consumer protocol version
 *    mismatch -- plus every infrastructure failure this manager detects itself. A trip never fails
 *    a job; it routes the next writer and the next reader to the sort-based delegate. '''Every
 *    path through this class therefore terminates in a working shuffle.'''
 *  - '''Push-merge.''' `ShuffleWriteProcessor` decides whether to initiate a push-based merge by
 *    pattern-matching on `shuffleBlockResolver`: it pushes only for an `IndexShuffleBlockResolver`
 *    and takes a no-op branch for anything else. While the gate is open this manager exposes a
 *    resolver that is deliberately '''not''' of that type, so streaming declines push-merge with no
 *    edit to that file at all; while the gate is closed it exposes the delegate's own index
 *    resolver, so push-merge behaves exactly as it always has.
 *
 * Nothing outside this package is modified to achieve any of it. The scheduler is untouched: a
 * streaming reader that loses its producer reports the loss by throwing the existing
 * `FetchFailedException`, and the unmodified `DAGScheduler` recomputes the upstream stage in
 * response. `SparkEnv` is untouched: because the shuffle manager is initialised after `SparkEnv`'s
 * constructor has completed, the rendezvous endpoint registers itself on the already-live `RpcEnv`.
 * `ShuffleWriteProcessor`, `shuffle/metrics.scala`, `TaskMetrics`, the memory manager, the block
 * manager and the shared transport layer are all consumed exactly as they stand.
 *
 * ==Initialisation order==
 *
 * On the driver this constructor runs '''before''' the memory manager exists --
 * `SparkContext` calls `initializeShuffleManager()` and only then `initializeMemoryManager(...)` --
 * and the block manager is likewise not valid until its own `initialize()` has run. Nothing here,
 * and nothing this class builds eagerly, may therefore dereference `SparkEnv.get.memoryManager` or
 * `SparkEnv.get.blockManager`: every such derivation is deferred to a `lazy val` or to the body of
 * `getWriter`/`getReader`, both of which run on a task thread long after both managers exist.
 * `SparkEnv.rpcEnv`, by contrast, is a constructor parameter of `SparkEnv` and so is fully live
 * here, which is what lets the rendezvous endpoint self-register.
 *
 * In local mode there is exactly one instance of this class -- the driver's, built with
 * `isDriver = true` -- because `Executor` skips shuffle manager initialisation when it is local.
 * That single instance serves both the producer role and the consumer role, and it reaches the
 * coordinator through `setupEndpoint` rather than through a driver lookup.
 *
 * ==Producer lifetime, and why the fallback is reached rather than avoided==
 *
 * A streaming producer lives exactly as long as the map task that owns it. Its buffers belong to
 * that task's memory manager, its spill files are deleted when that task's buffer owner closes, and
 * this class closes its egress listener and withdraws its rendezvous registration on the same task
 * completion listener. That lifetime is not an accident of the implementation but the condition on
 * which the memory guarantees rest: nothing a streaming producer holds may outlive the task that is
 * accounted for it, which is what makes the zero-leak requirement machine-checkable.
 *
 * The consequence is worth stating plainly, because it is easy to mistake for a defect. A consumer
 * can only be served while its producer's task is still running. With the scheduler untouched -- as
 * it must be -- a reduce stage is submitted only once its map stage has finished, so in an ordinary
 * job the producers are already gone by the time the consumers look for them. The read then fails,
 * this class records the failure, withdraws the shuffle from the rendezvous so that a
 * recomputation's map tasks are declined cluster wide, and closes the gate. The recomputation the
 * scheduler was going to perform anyway is written and read by the sort-based path, and the job
 * completes with correct results. That is the designed outcome, it costs one stage recomputation,
 * and it is the reason the guarantee above holds unconditionally: the streaming path is an
 * optimisation that is taken when a consumer is present to take it, never a dependency the job's
 * correctness rests on.
 *
 * ==Configuration==
 *
 * All five streaming properties are read once, here, and held immutably. That is what makes
 * "configuration changes require an executor restart" a property of the code rather than a note in
 * the documentation, and it is why no dynamic reconfiguration path exists or is needed.
 *
 * @param conf the Spark configuration; the five `spark.shuffle.streaming.*` properties are read
 *             from it exactly once, at construction
 * @param isDriver whether this process is the driver. It decides whether the rendezvous endpoint is
 *                 registered here or merely looked up, and it is supplied by the reflective
 *                 instantiation in `ShuffleManager.create`
 */
private[spark] class StreamingShuffleManager(conf: SparkConf, isDriver: Boolean)
  extends ShuffleManager with Logging {

  import StreamingShuffleManager._

  // ----------------------------------------------------------------------------------------------
  // Configuration. Read exactly once, held immutably, never re-read. The two percentage keys and
  // the bandwidth key are validated by their own ConfigEntry definitions, so an out-of-range value
  // is rejected here at read time with INVALID_CONF_VALUE.REQUIREMENT and never reaches any
  // arithmetic below.
  // ----------------------------------------------------------------------------------------------

  /** Tier two: whether the streaming behaviour of this manager is active at all. */
  private val streamingEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_ENABLED)

  /** Percentage of executor memory all streaming buffers of one executor may occupy together. */
  private val bufferSizePercent: Int = conf.get(config.SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)

  /** Buffer utilisation percentage at which buffered partitions are evicted to local disk. */
  private val spillThresholdPercent: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)

  /**
   * The administered link capacity in MB/s, or `None` when none is administered.
   *
   * `None` means '''unlimited''' -- emphatically not zero. It is kept as an `Option` rather than
   * flattened onto a sentinel precisely so that no arithmetic below can mistake absence for a
   * capacity of nothing.
   */
  private val maxBandwidthMbps: Option[Int] = conf.get(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)

  /** Whether the verbose diagnostics of this manager are emitted. Off by default. */
  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  // ----------------------------------------------------------------------------------------------
  // The fallback delegate and the gate.
  // ----------------------------------------------------------------------------------------------

  /**
   * The unmodified sort-based shuffle manager, held as the delegation target for the closed gate,
   * for every fallback condition and for every handle this manager did not register itself.
   *
   * Constructed eagerly, and safely so: `SortShuffleManager` takes exactly one argument and its
   * constructor touches only the configuration and creates its own resolver. It reaches neither the
   * memory manager nor the block manager, so building it in this constructor is sound on the driver
   * as well as on an executor.
   */
  private val sortShuffleManager: SortShuffleManager = new SortShuffleManager(conf)

  /**
   * Evaluates the four specified conditions under which streaming must step aside.
   *
   * Shared by every writer and every reader this manager hands out, because a trip has to be
   * remembered across tasks for the degradation to mean anything: a condition observed by one task
   * routes every later task on this executor to the sort-based delegate.
   */
  private[streaming] val fallbackPolicy: StreamingShuffleFallbackPolicy =
    new StreamingShuffleFallbackPolicy(conf)

  /**
   * Set once when an '''infrastructure''' failure makes streaming unusable in this process -- the
   * rendezvous endpoint could not be reached, an egress listener could not be bound, a producer
   * registration was refused, or a collaborator could not be built.
   *
   * Kept here rather than pushed into [[StreamingShuffleFallbackPolicy]] on purpose: that class
   * models exactly the four conditions the feature specifies, and widening it with a fifth reason
   * would blur a specified contract. The effect is identical, because this flag is consulted by the
   * same gate.
   */
  private val streamingStoodDown = new AtomicBoolean(false)

  /** Guards [[stop]] so that it is idempotent however many times it is called. */
  private val stopped = new AtomicBoolean(false)

  /**
   * Handles minted by [[registerShuffle]], keyed by shuffle id.
   *
   * Retained for one reason that cannot be met another way: [[unregisterShuffle]] receives only a
   * shuffle id, while every coordinator operation on a shuffle must present the capability token
   * that was minted when it was registered. The handle is where that token lives.
   */
  private val streamingShuffles =
    new ConcurrentHashMap[Int, StreamingShuffleHandle[_, _, _]]()

  /**
   * Reduce-stage attempt numbers, per shuffle, on which a streaming read did not deliver.
   *
   * This is the bookkeeping behind the one guarantee that has to hold unconditionally: that a job
   * whose streaming shuffle cannot be read still finishes. The scheduler's response to a failed
   * shuffle read is to recompute the map stage, and recomputation on its own would reproduce the
   * same streaming attempt and the same failure until the stage's consecutive-attempt allowance ran
   * out and the job was aborted. Counting the attempts that failed is what lets the subsystem stop
   * after the first one and let the recomputation take the sort-based path, so the recomputation
   * the scheduler was already going to perform is also the one that succeeds.
   *
   * Attempt numbers rather than a plain count, so that the several reduce tasks of one stage
   * attempt -- which all fail together, for the same reason -- are counted as the one event they
   * are.
   */
  private val streamingReadFailures =
    new ConcurrentHashMap[Int, java.util.Set[Integer]]()

  // ----------------------------------------------------------------------------------------------
  // The mandatory shuffle block resolver, chosen at construction time.
  // ----------------------------------------------------------------------------------------------

  /**
   * Serves the blocks the streaming path spilled to local disk, or `None` while the gate is closed
   * and nothing streaming can exist.
   *
   * Built eagerly, and safely so: its constructor reads the configuration and reaches the block
   * manager only through a `lazy val`, so it is sound to build on the driver before the block
   * manager is valid.
   */
  private val streamingBlockResolver: Option[StreamingShuffleBlockResolver] =
    if (streamingEnabled) Some(new StreamingShuffleBlockResolver(conf)) else None

  /**
   * The resolver this manager publishes, which is mandatory: the `ShuffleManager` trait declares it
   * abstract and warns that a custom manager must co-operate with the External Shuffle Service
   * through it.
   *
   * The choice is made once, here, and it is the load-bearing decision of the whole coexistence
   * strategy, because `ShuffleWriteProcessor` pattern-matches on this very value:
   *
   *  - '''Gate closed''' -- the delegate's own `IndexShuffleBlockResolver` is published unchanged.
   *    `ShuffleWriteProcessor` matches its index branch, so push-based merge is initiated exactly
   *    as it is for the sort-based manager and behaviour is indistinguishable from sort.
   *  - '''Gate open''' -- a [[StreamingShuffleDelegatingBlockResolver]] is published. It is
   *    deliberately '''not''' an `IndexShuffleBlockResolver`, so `ShuffleWriteProcessor` takes its
   *    no-op branch and streaming declines push-merge without a single edit to that file. It is a
   *    delegating resolver rather than the bare streaming one because the block manager routes
   *    '''every''' shuffle-block fetch through this value: a shuffle that fell back to sort-based
   *    shuffle -- because the gate was flipped, because the policy tripped, or because it was
   *    registered before streaming was reachable -- has index-based output that only the delegate's
   *    resolver can serve, and refusing to serve it would turn a graceful degradation into a failed
   *    fetch.
   *
   * The explicit type annotation is required rather than decorative. The two branches have
   * different static types, so without it inference would compute a least upper bound and could
   * silently publish a narrower or wider type than the trait member promises.
   *
   * A `val` is correct even though a fallback may trip mid-execution: a trip changes which writer
   * and which reader are handed out, never the identity of the resolver, and skipping push-merge is
   * always safe because push-merge is an optimisation. The value is never `null` and neither branch
   * throws, because a resolver that misbehaved here would break the sort-based path as well.
   */
  override val shuffleBlockResolver: ShuffleBlockResolver = streamingBlockResolver match {
    case Some(resolver) =>
      new StreamingShuffleDelegatingBlockResolver(resolver, sortShuffleManager.shuffleBlockResolver)
    case None =>
      sortShuffleManager.shuffleBlockResolver
  }

  // ----------------------------------------------------------------------------------------------
  // Streaming wiring. Every member below is lazy, so a manager whose gate is closed never touches
  // the RpcEnv, never registers an endpoint, never starts a thread and never opens a socket: it
  // costs precisely one SortShuffleManager and one closed gate.
  //
  // Each lazy member that owns a releasable resource also publishes it to a volatile field, so that
  // stop() can release what was created without forcing what was not -- forcing the rendezvous from
  // stop() would register an endpoint while shutting down.
  // ----------------------------------------------------------------------------------------------

  /** The driver's own coordinator instance, or `null` on an executor and before first use. */
  @volatile private var driverCoordinator: StreamingShuffleCoordinator = null

  /** The producer connector, or `null` until a reader has needed one. */
  @volatile private var openedConnector: StreamingShuffleProducerConnector = null

  /** The egress event loop, or `null` until a producer has needed one. */
  @volatile private var openedEgressEventLoop: EventLoopGroup = null

  /**
   * The producer/consumer rendezvous, resolved on first use of the streaming path.
   *
   * This replicates, inside this package, the driver-versus-executor shape `SparkEnv` uses for its
   * own named endpoints, and replicating it here is exactly why `SparkEnv` needs no modification:
   * this manager is constructed after `SparkEnv`'s constructor has completed, so the `RpcEnv` it
   * hands over is live and the endpoint can register itself on it. On the driver the endpoint
   * instance is created and registered under a fixed name; on an executor only a reference to the
   * driver's endpoint is resolved, and no instance is ever constructed there.
   */
  private lazy val rendezvous: StreamingShuffleRendezvous = {
    val rpcEnv = SparkEnv.get.rpcEnv
    val endpoint = if (isDriver) new StreamingShuffleCoordinator(rpcEnv, conf) else null
    // Published before the registration call, because the by-name argument below reads this field
    // on the driver and because stop() must be able to find the instance without forcing this val.
    driverCoordinator = endpoint
    // The fourth argument is by name and is evaluated on the driver only, so `endpoint` being null
    // on an executor is never observed by it.
    val ref = StreamingShuffleCoordinator.registerOrLookupEndpoint(rpcEnv, conf, isDriver, endpoint)
    StreamingShuffleRendezvous(endpoint, ref)
  }

  /**
   * The executor-wide egress rate limiter, shared by every streaming shuffle on this executor.
   *
   * One limiter per executor rather than one per shuffle, because the specified refill rate is the
   * administered link capacity divided by the number of shuffles the executor is concurrently
   * serving: a per-shuffle limiter could not express a budget that is shared. The divisor is not
   * known until the first producer registers, so the limiter starts on its single-shuffle share --
   * the most conservative rate it can have -- and is re-rated from the count the coordinator
   * reports on every producer registration. When no capacity is administered the limiter is
   * unlimited and costs a single boolean read per acquisition.
   */
  private lazy val rateLimiter: TokenBucketRateLimiter =
    TokenBucketRateLimiter(conf, numConcurrentShuffles = 1)

  /**
   * The executor-wide backpressure ledger, shared by every streaming writer and reader here.
   *
   * Shared deliberately: the protocol arbitrates between the concurrent shuffles of one executor on
   * partition count and pending volume, and a per-shuffle ledger could not see the shuffles it is
   * meant to arbitrate against. It is handed the driver's coordinator instance when there is one --
   * in local mode there is -- and `null` on an executor, which it handles by falling back to the
   * shuffles registered with it locally.
   */
  private lazy val backpressure: BackpressureProtocol =
    new BackpressureProtocol(conf, rendezvous.endpoint, rateLimiter)

  /**
   * Opens consumer-side channels to producers. Owned here, because a connector holds a process-wide
   * event loop; a reader only ever closes the channels it opened, never the connector.
   */
  private lazy val producerConnector: StreamingShuffleProducerConnector = {
    val connector = new NettyStreamingShuffleProducerConnector(conf)
    openedConnector = connector
    connector
  }

  /**
   * Transport configuration for the streaming module's own `spark.shuffle-streaming.io.*`
   * namespace.
   *
   * Asking for a module of its own is what lets thread counts, buffer sizes, backlog and TCP
   * keepalive be tuned for streaming without perturbing the block transfer service every other
   * shuffle depends on. No shared transport class is modified to obtain it.
   */
  private lazy val egressTransportConf: TransportConf =
    StreamingShuffleServerHandler.streamingTransportConf(conf)

  /** The transport mode the streaming module is configured for, resolved once. */
  private lazy val egressIoMode: IOMode = IOMode.valueOf(egressTransportConf.ioMode())

  /**
   * The event loop every producer-side egress listener of this executor shares.
   *
   * One group per executor, not per task: a group per map task would multiply Netty threads by the
   * task concurrency. A thread count of zero is passed straight through, which is Netty's own
   * "derive it from the available processors" default and matches how the block transfer server
   * sizes itself.
   */
  private lazy val egressEventLoop: EventLoopGroup = {
    val group = NettyUtils.createEventLoop(
      IOMode.valueOf(egressTransportConf.ioMode()),
      egressTransportConf.serverThreads(),
      EGRESS_THREAD_PREFIX)
    openedEgressEventLoop = group
    group
  }

  // ----------------------------------------------------------------------------------------------
  // One construction-time line, so that an operator reading an executor log can tell which tier
  // state applies without enabling anything. Exactly one line per JVM keeps the subsystem inside
  // its budget of under ten megabytes of log per hour per executor with debug off by default.
  // ----------------------------------------------------------------------------------------------

  if (streamingEnabled) {
    logInfo(log"Streaming shuffle is enabled: map output will be pipelined to consumers with " +
      log"${MDC(VALUE, bufferSizePercent)}% of executor memory reserved for buffers, spilling at " +
      log"${MDC(THRESHOLD, spillThresholdPercent)}% utilisation, egress paced to " +
      log"${MDC(COUNT, maxBandwidthMbps.map(_.toString).getOrElse(UNLIMITED_BANDWIDTH))} MB/s, " +
      log"and automatic fallback to ${MDC(CLASS_NAME, SORT_MANAGER_NAME)} whenever streaming " +
      log"cannot be sustained")
  } else {
    logInfo(log"Streaming shuffle is selected but not enabled, so every shuffle service-provider " +
      log"call is delegated verbatim to ${MDC(CLASS_NAME, SORT_MANAGER_NAME)}; set " +
      log"${MDC(CONFIG, config.SHUFFLE_STREAMING_ENABLED.key)} to true to activate streaming")
  }


  // ----------------------------------------------------------------------------------------------
  // The tier-two gate.
  // ----------------------------------------------------------------------------------------------

  /**
   * Whether the streaming path may be used for the next writer or reader handed out.
   *
   * Three reads of three fields and no allocation, no lock and no clock, because this is consulted
   * at the top of every service-provider call:
   *
   *  - `streamingEnabled` is the operator kill switch, read once at construction.
   *  - `streamingStoodDown` is this manager's own verdict, set when the infrastructure the
   *    streaming path needs could not be established on this executor.
   *  - `fallbackPolicy.hasTripped` is the verdict of the four specified conditions.
   *
   * Once any of the three closes the gate it stays closed for the lifetime of the manager, which is
   * deliberate: a subsystem that oscillated between streaming and sort would produce shuffles whose
   * halves were written by different implementations far more often than one that stands down and
   * stays down. Reopening it requires an executor restart, which is exactly the operational
   * contract the configuration keys advertise.
   */
  private def streamingActive: Boolean =
    streamingEnabled && !streamingStoodDown.get() && !fallbackPolicy.hasTripped

  // ----------------------------------------------------------------------------------------------
  // ShuffleManager: the six service-provider members.
  // ----------------------------------------------------------------------------------------------

  /**
   * Registers a shuffle on the driver, which is where the decision to stream a given shuffle is
   * actually taken.
   *
   * Whether the returned handle is a [[StreamingShuffleHandle]] is what every later decision keys
   * on, so registration is the one place that has to be conservative: a shuffle is registered for
   * streaming only when the gate is open '''and''' the coordinator issues a grant. The coordinator
   * declines a registration whose protocol version this build cannot speak, whose partition count
   * contradicts an existing registration, or which would exceed the number of shuffles it is
   * willing to track -- and every decline lands on the sort-based path, which is the same terminus
   * as the kill switch.
   *
   * The grant carries the capability token every later coordinator interaction must present and the
   * epoch that identifies this registration, both of which travel to the writer and the reader
   * inside the handle. The handle is retained here as well, because `unregisterShuffle` receives
   * only a shuffle id and needs the token to authorise the removal.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (!streamingActive) {
      // The kill switch, or a policy that has already tripped. Delegating verbatim is what makes
      // the gated-off manager indistinguishable from the sort-based one.
      sortShuffleManager.registerShuffle(shuffleId, dependency)
    } else {
      val numPartitions = dependency.partitioner.numPartitions
      streamingRegistration(shuffleId, numPartitions) match {
        case Some(grant) =>
          val handle = new StreamingShuffleHandle(shuffleId, dependency, numPartitions,
            PROTOCOL_VERSION, grant.coordinatorEpoch, grant.capabilityToken)
          streamingShuffles.put(shuffleId, handle)
          logInfo(log"Registered shuffle ${MDC(SHUFFLE_ID, shuffleId)} for streaming across " +
            log"${MDC(NUM_PARTITIONS, numPartitions)} partitions at coordinator epoch " +
            log"${MDC(EPOCH, grant.coordinatorEpoch)}")
          handle
        case None =>
          // A declined registration is not an error: the shuffle simply runs on the sort-based
          // path, which is the whole point of keeping that path reachable at all times.
          logInfo(log"Shuffle ${MDC(SHUFFLE_ID, shuffleId)} was not granted a streaming " +
            log"registration and will use ${MDC(CLASS_NAME, SORT_MANAGER_NAME)}")
          sortShuffleManager.registerShuffle(shuffleId, dependency)
      }
    }
  }

  /**
   * Returns the writer a map task will use.
   *
   * Called on a task thread, which is what makes it the right place to build everything that needs
   * a live memory manager or block manager: by the time a task runs, both exist on the driver and
   * on an executor alike. That is the whole of the answer to the initialisation-order hazard
   * described on this class.
   *
   * A streaming writer is returned only for a [[StreamingShuffleHandle]] and only when the whole
   * producer-side apparatus could be established: a per-task buffer owner registered with the
   * memory manager, an egress listener bound to an ephemeral local port, a producer registration
   * accepted by the coordinator, and the task's buffers published to the block resolver so that
   * anything spilled remains servable. If any of that fails the executor stands down and this task
   * -- and every later one -- uses the sort-based writer.
   */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    if (!streamingActive) {
      sortShuffleManager.getWriter(handle, mapId, context, metrics)
    } else {
      handle match {
        case streamingHandle: StreamingShuffleHandle[K @unchecked, V @unchecked, _] =>
          streamingWriter(streamingHandle, mapId, context, metrics)
            .getOrElse(sortShuffleManager.getWriter(streamingHandle, mapId, context, metrics))
        case other =>
          // A shuffle registered while the gate was closed keeps the writer it was registered for.
          sortShuffleManager.getWriter(other, mapId, context, metrics)
      }
    }
  }

  /**
   * Returns the reader a reduce task will use.
   *
   * This is the seven-argument form, and it is the only one this class may override: the
   * five-argument overload is `final` on the trait and forwards to this one, so overriding this
   * single method covers both call shapes.
   *
   * When the gate is closed every argument is forwarded verbatim to the sort-based delegate. That
   * is correct even for a shuffle that was registered for streaming: the placeholder map status the
   * streaming writer publishes carries the real per-partition byte counts, so the delegate's reader
   * attempts a fetch, that fetch legitimately fails, and the unmodified scheduler recomputes the
   * map stage -- which, with the gate now closed, produces sort-based output the delegate can read.
   * Every path terminates in a working shuffle.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    if (!streamingActive) {
      sortShuffleManager.getReader(
        handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
    } else {
      handle match {
        case streamingHandle: StreamingShuffleHandle[K @unchecked, _, C @unchecked] =>
          streamingReader(
              streamingHandle, startMapIndex, endMapIndex, startPartition, endPartition,
              context, metrics)
            .getOrElse(sortShuffleManager.getReader(streamingHandle, startMapIndex, endMapIndex,
              startPartition, endPartition, context, metrics))
        case other =>
          sortShuffleManager.getReader(
            other, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
      }
    }
  }

  /**
   * Forgets a shuffle, releasing everything either path retained for it.
   *
   * Idempotent and total: it never throws, and it always cleans both paths. Both properties matter
   * because this is called from the context cleaner rather than from a task, so an exception here
   * would be logged far from the shuffle that caused it, and because a shuffle may have been
   * registered on one path and read on the other after a mid-execution fallback.
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    val handle = streamingShuffles.remove(shuffleId)
    if (handle != null) {
      Utils.tryLogNonFatalError {
        // Only the driver holds the coordinator's state, and only the driver ever registered this
        // shuffle, so only the driver has anything to remove.
        if (isDriver) {
          val coordinator = driverCoordinator
          if (coordinator != null) {
            coordinator.unregisterShuffle(shuffleId, handle.capabilityToken)
          }
        }
      }
      Utils.tryLogNonFatalError {
        val released = backpressure.unregisterShuffle(shuffleId)
        if (debugEnabled) {
          logInfo(log"Released ${MDC(COUNT, released)} bytes of streaming backpressure state for " +
            log"shuffle ${MDC(SHUFFLE_ID, shuffleId)}")
        }
      }
      Utils.tryLogNonFatalError {
        fallbackPolicy.forgetShuffle(shuffleId)
      }
      streamingReadFailures.remove(shuffleId)
      Utils.tryLogNonFatalError {
        streamingBlockResolver.foreach { resolver =>
          val dropped = resolver.removeShuffle(shuffleId)
          if (debugEnabled) {
            logInfo(log"Dropped ${MDC(COUNT, dropped)} streaming producer registrations for " +
              log"shuffle ${MDC(SHUFFLE_ID, shuffleId)}")
          }
        }
      }
    }
    // Always asked of the delegate as well: a shuffle registered while the gate was closed, or one
    // whose writers fell back per task, has sort-based state that only the delegate can release.
    Utils.tryLogNonFatalError {
      sortShuffleManager.unregisterShuffle(shuffleId)
    }
    true
  }

  /**
   * Releases everything this manager owns, exactly once, without ever throwing.
   *
   * Guarded by an [[java.util.concurrent.atomic.AtomicBoolean]] because it is reachable from both
   * `SparkEnv.stop` and an explicit shutdown, and every step is wrapped individually so that a
   * failure to release one resource cannot strand the rest. Nothing here forces a lazy member into
   * existence: each releasable resource was published to a volatile field when it was created, so a
   * manager that never streamed releases nothing and registers nothing on its way out.
   */
  override def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      Utils.tryLogNonFatalError {
        val connector = openedConnector
        if (connector != null) {
          connector.close()
        }
      }
      Utils.tryLogNonFatalError {
        val group = openedEgressEventLoop
        if (group != null) {
          group.shutdownGracefully(
            EGRESS_SHUTDOWN_QUIET_PERIOD_MS, EGRESS_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        }
      }
      Utils.tryLogNonFatalError {
        // Only the driver ever registered the endpoint, so only the driver stops it. Stopping the
        // endpoint through its own lifecycle method is what lets the coordinator release its
        // reaping timer on the thread it owns, and it needs no message round trip on a shutdown
        // path.
        val coordinator = driverCoordinator
        if (isDriver && coordinator != null) {
          coordinator.stop()
        }
      }
      Utils.tryLogNonFatalError {
        shuffleBlockResolver.stop()
      }
      Utils.tryLogNonFatalError {
        // The delegate's own stop() closes its resolver, which is idempotent, so stopping both the
        // published resolver and the delegate is safe in either order.
        sortShuffleManager.stop()
      }
      streamingShuffles.clear()
    }
  }


  // ----------------------------------------------------------------------------------------------
  // Streaming construction helpers. Each one is total: it either returns a working streaming
  // component or it returns nothing, having already released whatever it had built and recorded
  // why. No helper propagates a failure to its caller, which is what makes every service-provider
  // method above able to fall through to the sort-based delegate instead of failing a task.
  // ----------------------------------------------------------------------------------------------

  /**
   * Asks the coordinator to admit a shuffle to the streaming path.
   *
   * Answers `None` for every reason a shuffle should not stream, and treats a coordinator that
   * cannot be reached as one of them: an unreachable coordinator stands the executor down, because
   * no streaming shuffle can rendezvous without it, and there is no value in rediscovering that
   * once per shuffle.
   */
  private def streamingRegistration(
      shuffleId: Int,
      numPartitions: Int): Option[StreamingShuffleRegistrationGrant] = {
    try {
      val coordinator = rendezvous.endpoint
      if (coordinator == null) {
        // registerShuffle is a driver-side call in every deployment mode, local mode included, so
        // reaching here means a process that does not host the coordinator is registering a
        // shuffle. The sort-based path handles it correctly, so this is a downgrade and not a
        // failure.
        logWarning(log"Shuffle ${MDC(SHUFFLE_ID, shuffleId)} cannot be registered for streaming " +
          log"because this process does not host the streaming shuffle coordinator")
        None
      } else {
        coordinator.registerShuffle(shuffleId, numPartitions, PROTOCOL_VERSION)
      }
    } catch {
      case NonFatal(e) =>
        standDownStreaming(shuffleId, s"the streaming shuffle coordinator is unreachable: $e")
        None
    }
  }

  /**
   * Builds the producer half for one map task, or answers `None` so the caller uses the delegate.
   *
   * The collaborators split cleanly by lifetime, and that split is the reason the subsystem stays
   * inside its memory budget. Per task: the buffer owner, which must be a memory consumer of that
   * task's memory manager so Spark's own accounting and leak detection apply; the egress handler,
   * which holds per-channel state and is explicitly not shareable; and the error notifier, which
   * bridges one task's I/O threads to that one task's thread. Per executor: the backpressure ledger
   * and the rate limiter, both of which exist precisely to arbitrate '''between''' the concurrent
   * shuffles of an executor and could not do so if they were per task.
   */
  private def streamingWriter[K, V, C](
      handle: StreamingShuffleHandle[K, V, C],
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): Option[ShuffleWriter[K, V]] = {
    val shuffleId = handle.shuffleId
    streamingBlockResolver match {
      case None =>
        None
      case Some(resolver) =>
        try {
          val errorNotifier = new StreamingShuffleErrorNotifier(shuffleId, conf)
          val spillManager = new MemorySpillManager(context.taskMemoryManager(), conf)
          val serverHandler = new StreamingShuffleServerHandler(
            conf, shuffleId, backpressure, rateLimiter, errorNotifier)
          if (startProducerEgress(handle, mapId, context, spillManager, serverHandler, resolver)) {
            val components = StreamingShuffleWriterComponents(
              backpressure, rateLimiter, spillManager, serverHandler, fallbackPolicy, errorNotifier)
            Some(new StreamingShuffleWriter[K, V, C](
              handle, mapId, context, metrics, conf, components))
          } else {
            // The buffer owner is the only collaborator that has already taken a resource from the
            // memory manager, so it is the only one that must be closed on this path. Closing it is
            // what keeps the zero-leak guarantee true for a producer that never started.
            spillManager.close()
            None
          }
        } catch {
          case NonFatal(e) =>
            standDownStreaming(shuffleId,
              s"the streaming producer for map $mapId could not be started: $e")
            None
        }
    }
  }

  /**
   * Builds the consumer half for one reduce task, or answers `None` so the caller uses the
   * delegate.
   *
   * The reader is handed the executor-wide ledger, the shared connector and this task's own error
   * notifier. It is not handed a coordinator instance, only a reference: a reduce task discovers
   * producers by asking the driver, which is the same rendezvous shape the map output tracker uses
   * and the reason no scheduler or `SparkEnv` code needs to change.
   */
  private def streamingReader[K, C](
      handle: StreamingShuffleHandle[K, _, C],
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): Option[ShuffleReader[K, C]] = {
    val shuffleId = handle.shuffleId
    try {
      if (!producersAvailable(handle, startPartition, endPartition)) {
        // The rendezvous holds nothing for this shuffle, so there is nothing to stream from. That
        // is not an error and it is not a reason to fail the task: it is the signal that this
        // shuffle has to be read the other way, and recording it is what makes the recomputation
        // the scheduler is about to perform produce output the other way as well.
        recordStreamingReadFailure(shuffleId, handle.capabilityToken, context.stageAttemptNumber(),
          "the rendezvous holds no live producer for this shuffle")
        None
      } else {
        val errorNotifier = new StreamingShuffleErrorNotifier(shuffleId, conf)
        val streamingContext = StreamingShuffleReaderContext(
          rendezvous.ref,
          backpressure,
          fallbackPolicy,
          errorNotifier,
          producerConnector,
          SparkEnv.get.serializerManager)
        val reader = new StreamingShuffleReader[K, C](
          handle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          metrics,
          conf,
          streamingContext)
        Some(new FallbackRecordingShuffleReader[K, C](
          reader, shuffleId, handle.capabilityToken, context.stageAttemptNumber()))
      }
    } catch {
      case NonFatal(e) =>
        standDownStreaming(shuffleId, s"a streaming shuffle consumer could not be created: $e")
        None
    }
  }

  /**
   * Whether the rendezvous currently holds a producer this consumer could read.
   *
   * Asked before a streaming reader is built rather than left to the reader's own polling loop,
   * because the two questions are different. The reader polls to wait for producers that have not
   * announced themselves yet, which is the right behaviour once it has been decided that this
   * shuffle is being streamed. This asks whether that decision is still true at all -- and it is
   * the question that has to be answered here, because only this class can act on a negative answer
   * by handing back the sort-based reader instead of failing the task.
   *
   * A single lookup costs one small round trip per reduce task and is dwarfed by the read it gates.
   */
  private def producersAvailable(
      handle: StreamingShuffleHandle[_, _, _],
      startPartition: Int,
      endPartition: Int): Boolean = {
    val request = LookupStreamingShuffleProducers(
      handle.shuffleId, handle.capabilityToken, startPartition, endPartition)
    // Typed as Any because the reply is an Option, and an Option's element type is erased: matching
    // on the payload is what turns an unexpected reply into an attributable mismatch here rather
    // than a class cast somewhere further down.
    rendezvous.ref.askSync[Any](request) match {
      case Some(locations: StreamingShuffleProducerLocations) => locations.locations.nonEmpty
      case _ => false
    }
  }

  /**
   * Records that a streaming read of one shuffle did not deliver on one reduce-stage attempt, and
   * withdraws the shuffle from the streaming path once enough attempts have failed.
   *
   * The threshold is one attempt, and that is a deliberate choice rather than an arbitrary one. By
   * the time a streaming read reports failure, the reader has already exhausted its own rendezvous
   * polling and its own retransmission window, so a second whole-stage attempt would repeat work
   * that has already been tried and would spend one more of the stage's limited consecutive
   * attempts. Standing down after the first spends none of them.
   *
   * Withdrawal happens at the rendezvous before it happens locally, and the order matters: the
   * coordinator is the only state every executor of the application consults, so a shuffle it no
   * longer knows cannot be streamed by any producer anywhere. Without that step a stand-down would
   * be local to this JVM, the recomputed map tasks on other executors would stream again, and the
   * recomputation would fail for exactly the same reason as the attempt before it.
   */
  private def recordStreamingReadFailure(
      shuffleId: Int,
      capabilityToken: String,
      stageAttemptNumber: Int,
      reason: String): Unit = {
    val attempts = streamingReadFailures.computeIfAbsent(
      shuffleId, _ => ConcurrentHashMap.newKeySet[Integer]())
    attempts.add(Integer.valueOf(stageAttemptNumber))
    if (attempts.size() >= MAX_FAILED_STREAMING_READ_STAGE_ATTEMPTS) {
      Utils.tryLogNonFatalError {
        rendezvous.ref.askSync[Boolean](UnregisterStreamingShuffle(shuffleId, capabilityToken))
      }
      standDownStreaming(shuffleId,
        s"a streaming shuffle read did not deliver on reduce stage attempt $stageAttemptNumber " +
          s"($reason), so the recomputation the scheduler performs will use sort-based shuffle")
    } else if (debugEnabled) {
      logInfo(log"Streaming read of shuffle ${MDC(SHUFFLE_ID, shuffleId)} failed on " +
        log"${MDC(COUNT, attempts.size())} reduce stage attempts so far: ${MDC(REASON, reason)}")
    }
  }

  /**
   * Wraps a streaming reader so that a failed shuffle read is observed by the manager that made it.
   *
   * The wrapper exists because the failure that matters cannot be seen from where it is thrown. A
   * reduce task that cannot read its input raises a fetch failure, the scheduler recomputes the map
   * stage, and neither the reader nor the scheduler is in a position to decide that the next
   * attempt should be written differently. This manager is, so the failure is routed to it on its
   * way past.
   *
   * Both the call and the iteration are covered, because a shuffle read is lazy: the failure may
   * surface when the iterator is created or at any point while it is being drained. The exception
   * is always rethrown unchanged, so the scheduler sees exactly the fetch failure it would have
   * seen and its own recovery is untouched.
   */
  private class FallbackRecordingShuffleReader[K, C](
      delegate: ShuffleReader[K, C],
      shuffleId: Int,
      capabilityToken: String,
      stageAttemptNumber: Int)
    extends ShuffleReader[K, C] {

    override def read(): Iterator[Product2[K, C]] = {
      val underlying = recording(delegate.read())
      new Iterator[Product2[K, C]] {
        override def hasNext: Boolean = recording(underlying.hasNext)

        override def next(): Product2[K, C] = recording(underlying.next())
      }
    }

    /** Runs `body`, routing a fetch failure to the manager before letting it continue upward. */
    private def recording[T](body: => T): T = {
      try {
        body
      } catch {
        case failure: FetchFailedException =>
          recordStreamingReadFailure(shuffleId, capabilityToken, stageAttemptNumber,
            Option(failure.getMessage).getOrElse(failure.getClass.getName))
          throw failure
      }
    }
  }

  /**
   * Publishes one map task as a reachable streaming producer, and arranges for it to stop being
   * one.
   *
   * Three things have to become true together for a consumer to be able to read this task, and this
   * method is the only place all three happen: an egress listener is accepting connections, the
   * task's buffers are reachable through the published block resolver so that anything spilled is
   * still servable, and the coordinator knows the address so a reduce task can find it. If any of
   * the three fails, all three are undone before returning, because a half-published producer is
   * worse than an unpublished one -- a consumer would find an address it could not read.
   *
   * The teardown is registered on the task-completion listener rather than in a `stop()` hook,
   * because it must run on success, on failure and on cancellation alike. That is the same
   * mechanism the reader uses, and for the same reason: it is the only hook that is guaranteed to
   * run.
   */
  private def startProducerEgress(
      handle: StreamingShuffleHandle[_, _, _],
      mapId: Long,
      context: TaskContext,
      spillManager: MemorySpillManager,
      serverHandler: StreamingShuffleServerHandler,
      resolver: StreamingShuffleBlockResolver): Boolean = {
    val shuffleId = handle.shuffleId
    val taskAttemptId = context.taskAttemptId()
    val channel = bindEgressChannel(shuffleId, mapId, serverHandler)
    val port = channel.localAddress() match {
      case address: InetSocketAddress => address.getPort
      case _ => EGRESS_UNKNOWN_PORT
    }
    var published = false
    var started = false
    try {
      if (port <= 0) {
        logWarning(log"The streaming egress listener for shuffle ${MDC(SHUFFLE_ID, shuffleId)} " +
          log"map ${MDC(MAP_ID, mapId)} bound to an address without a reachable port")
      } else if (!resolver.registerProducer(shuffleId, mapId, taskAttemptId, spillManager)) {
        // A newer attempt of the same map task already owns the registration. Declining is correct:
        // a stale or speculative attempt must never take over a live producer.
        logInfo(log"A newer attempt already owns the streaming producer registration for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)}, so attempt " +
          log"${MDC(TASK_ATTEMPT_ID, taskAttemptId)} will use " +
          log"${MDC(CLASS_NAME, SORT_MANAGER_NAME)}")
      } else {
        published = true
        val location = StreamingShuffleProducerLocation(
          SparkEnv.get.executorId, advertisedHost(), port, mapId, taskAttemptId)
        val registration = rendezvous.ref.askSync[StreamingShuffleProducerRegistration](
          RegisterStreamingShuffleProducer(shuffleId, handle.capabilityToken, location,
            handle.numPartitions, handle.protocolVersion, System.currentTimeMillis()))
        if (!registration.accepted) {
          logInfo(log"The streaming shuffle coordinator declined the producer registration for " +
            log"shuffle ${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)} at epoch " +
            log"${MDC(EPOCH, registration.coordinatorEpoch)}")
        } else if (!fallbackPolicy.checkProtocolVersion(registration.protocolVersion)) {
          // Recording the mismatch is what trips the documented version-mismatch fallback with the
          // right reason attached, rather than leaving it to be inferred from a parse failure
          // later.
          logWarning(log"The streaming shuffle coordinator speaks protocol version " +
            log"${MDC(VALUE, registration.protocolVersion)}, which this executor cannot read")
        } else {
          // The divisor of the egress refill formula is only knowable from the coordinator, so the
          // limiter is re-rated here, on the one path that has just learned it.
          updateRateFromConcurrency(registration.numConcurrentShuffles)
          context.addTaskCompletionListener[Unit] { _ =>
            releaseProducer(shuffleId, mapId, taskAttemptId, channel, serverHandler, resolver)
          }
          started = true
          if (debugEnabled) {
            logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
              log"${MDC(MAP_ID, mapId)} is serving consumers from " +
              log"${MDC(HOST_PORT, location.hostPort)} on executor " +
              log"${MDC(EXECUTOR_ID, location.executorId)} across " +
              log"${MDC(COUNT, registration.numConcurrentShuffles)} concurrent shuffles")
          }
        }
      }
    } catch {
      case NonFatal(e) =>
        standDownStreaming(shuffleId,
          s"the streaming producer for map $mapId could not be published: $e")
    }
    if (!started) {
      // Undo in the reverse order of publication, so that at no instant is an address discoverable
      // for a producer whose buffers have already been withdrawn.
      if (published) {
        Utils.tryLogNonFatalError {
          resolver.unregisterProducer(shuffleId, mapId, taskAttemptId)
        }
      }
      Utils.tryLogNonFatalError {
        serverHandler.releaseAll()
      }
      Utils.tryLogNonFatalError {
        channel.close()
      }
    }
    started
  }

  /**
   * The host a consumer should dial to reach a producer on this process.
   *
   * The block manager's own identity is preferred over the local host name, because that identity
   * is the address Spark already publishes for block transfer and is therefore known to be
   * reachable from every peer: it honours the local-address and advertised-address settings an
   * operator may have configured, whereas the raw host name need not resolve on the other side of
   * the connection. Reached from a task thread only, so the block manager is always valid by the
   * time it is read, and guarded anyway so that an unexpected shape degrades to the host name
   * rather than failing a task.
   */
  private def advertisedHost(): String = {
    try {
      val env = SparkEnv.get
      val blockManager = if (env == null) null else env.blockManager
      val identity = if (blockManager == null) null else blockManager.blockManagerId
      if (identity == null) Utils.localHostName() else identity.host
    } catch {
      case NonFatal(_) =>
        Utils.localHostName()
    }
  }

  /**
   * Binds the egress listener one map task's consumers connect to.
   *
   * An ephemeral port on every interface, because the address is published to the coordinator
   * rather than configured, and one listener per producing task attempt, because the egress handler
   * holds per-channel state and is deliberately not shareable. That last point is why a second
   * connection is refused rather than accepted: sharing one handler across two channels would
   * interleave two consumers' sequence numbers, and a refused consumer recovers through the
   * documented fetch-failure path, whereas a corrupted stream does not.
   *
   * The pipeline is the frame decoder followed by the handler, which is the same two-stage shape
   * the consumer side uses. The decoder is what makes a protocol-version mismatch an explicit,
   * reported event instead of a parse failure. TCP keepalive comes from the streaming module's own
   * transport namespace, so enabling it here cannot perturb the block transfer service.
   */
  private def bindEgressChannel(
      shuffleId: Int,
      mapId: Long,
      serverHandler: StreamingShuffleServerHandler): Channel = {
    val accepted = new AtomicBoolean(false)
    val bootstrap = new ServerBootstrap()
    bootstrap.group(egressEventLoop)
      .channel(NettyUtils.getServerChannelClass(egressIoMode))
      .option(ChannelOption.SO_REUSEADDR, java.lang.Boolean.TRUE)
      .childOption(ChannelOption.TCP_NODELAY, java.lang.Boolean.TRUE)
      .childOption(ChannelOption.SO_KEEPALIVE,
        java.lang.Boolean.valueOf(egressTransportConf.enableTcpKeepAlive()))
      .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(channel: SocketChannel): Unit = {
          if (accepted.compareAndSet(false, true)) {
            channel.pipeline()
              .addLast(EGRESS_DECODER_NAME,
                new StreamingShuffleFrameDecoder(version => onIncompatibleEgressVersion(
                  shuffleId, mapId, version)))
              .addLast(EGRESS_HANDLER_NAME, serverHandler)
          } else {
            if (debugEnabled) {
              logInfo(log"Refusing a second consumer channel for shuffle " +
                log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)}, because one egress " +
                log"handler serves exactly one channel")
            }
            channel.close()
          }
        }
      })
    // Binding an ephemeral local port completes without a network round trip, so waiting for it on
    // the task thread costs nothing measurable and gives the port the coordinator has to be told.
    bootstrap.bind(new InetSocketAddress(EGRESS_EPHEMERAL_PORT)).sync().channel()
  }

  /**
   * Records that a consumer sent a frame this build cannot read.
   *
   * Invoked from a Netty thread by the frame decoder, so it must neither block nor throw. Feeding
   * the version to the policy is what turns an unreadable frame into the documented
   * version-mismatch fallback, with this executor standing down rather than retrying a conversation
   * it cannot hold.
   */
  private def onIncompatibleEgressVersion(shuffleId: Int, mapId: Long, version: Byte): Unit = {
    Utils.tryLogNonFatalError {
      if (!fallbackPolicy.checkProtocolVersion(version)) {
        logWarning(log"A consumer of shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
          log"${MDC(MAP_ID, mapId)} sent protocol version ${MDC(VALUE, version)}, which this " +
          log"executor cannot read")
      }
    }
  }

  /**
   * Withdraws one map task's producer, releasing every resource it held.
   *
   * Runs on task completion whatever the outcome was, and never throws, because it is the last
   * thing that will ever run for this producer: an exception escaping here would strand a socket, a
   * Netty handler's retained buffers and a resolver registration for the lifetime of the executor.
   * Each step is therefore wrapped on its own.
   */
  private def releaseProducer(
      shuffleId: Int,
      mapId: Long,
      taskAttemptId: Long,
      channel: Channel,
      serverHandler: StreamingShuffleServerHandler,
      resolver: StreamingShuffleBlockResolver): Unit = {
    Utils.tryLogNonFatalError {
      resolver.unregisterProducer(shuffleId, mapId, taskAttemptId)
    }
    Utils.tryLogNonFatalError {
      serverHandler.releaseAll()
    }
    Utils.tryLogNonFatalError {
      channel.close()
    }
    if (debugEnabled) {
      logInfo(log"Withdrew the streaming producer for shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
        log"${MDC(MAP_ID, mapId)} attempt ${MDC(TASK_ATTEMPT_ID, taskAttemptId)}")
    }
  }

  /**
   * Re-rates the shared egress limiter from the number of shuffles this executor is now serving.
   *
   * The specified refill rate is the administered link capacity divided by the number of concurrent
   * shuffles, held to a ceiling of a fixed percentage of that capacity. The divisor changes as
   * shuffles come and go, and only the coordinator knows it, so it is applied here -- on the one
   * path that has just been told. When no capacity is administered the limiter is unlimited and
   * there is nothing to re-rate, which is why `None` must never be read as zero.
   */
  private def updateRateFromConcurrency(numConcurrentShuffles: Int): Unit = {
    maxBandwidthMbps.foreach { administeredMbps =>
      val share =
        TokenBucketRateLimiter.perShuffleBytesPerSecond(administeredMbps, numConcurrentShuffles)
      val paced = TokenBucketRateLimiter.applyLinkCapacityCeiling(share)
      if (rateLimiter.updateRate(paced) && debugEnabled) {
        logInfo(log"Streaming shuffle egress re-rated to ${MDC(VALUE, paced)} bytes/s, being " +
          log"${MDC(CONFIG, config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS.key)}=" +
          log"${MDC(THRESHOLD, administeredMbps)} MB/s shared across " +
          log"${MDC(COUNT, numConcurrentShuffles)} concurrent shuffles")
      }
    }
  }

  /**
   * Closes the gate for the remaining life of this manager, once, with the reason recorded.
   *
   * This is the terminus for every failure the streaming infrastructure of '''this''' executor
   * suffers, as distinct from the four workload conditions the fallback policy owns. Keeping the
   * two apart matters: the policy's reasons are diagnosable properties of a workload, whereas these
   * are properties of a host, and conflating them would make the policy's telemetry misleading.
   * Both arrive at the same place -- delegation to the sort-based manager -- which is the single
   * terminus the design requires.
   */
  private def standDownStreaming(shuffleId: Int, reason: String): Unit = {
    if (streamingStoodDown.compareAndSet(false, true)) {
      logWarning(log"Standing down streaming shuffle on this process from shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} onwards and delegating to " +
        log"${MDC(CLASS_NAME, SORT_MANAGER_NAME)}: ${MDC(REASON, reason)}")
    } else if (debugEnabled) {
      logInfo(log"Streaming shuffle was already stood down when shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} reported ${MDC(REASON, reason)}")
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Seams the package's own suites observe. Every one is a read of state this manager already
  // keeps, so none of them can perturb what it reports, and none widens the public surface: the
  // whole class is private[spark] and these are private[streaming].
  // ----------------------------------------------------------------------------------------------

  /** Whether the streaming path is currently in use, which is the gate the suites assert on. */
  private[streaming] def isStreamingActive: Boolean = streamingActive

  /** Whether the tier-two kill switch is open at all, independent of any later verdict. */
  private[streaming] def isStreamingEnabled: Boolean = streamingEnabled

  /** Whether this manager withdrew from streaming because its own infrastructure failed. */
  private[streaming] def hasStoodDown: Boolean = streamingStoodDown.get()

  /** Whether `stop()` has run, which every idempotence assertion needs. */
  private[streaming] def isStopped: Boolean = stopped.get()

  /** The sort-based manager every gated-off and fallen-back call is delegated to. */
  private[streaming] def sortDelegate: SortShuffleManager = sortShuffleManager

  /** The resolver that serves spilled streaming blocks, or `None` while the gate is closed. */
  private[streaming] def streamingResolver: Option[StreamingShuffleBlockResolver] =
    streamingBlockResolver

  /** Shuffle ids currently registered for streaming, in ascending order. */
  private[streaming] def registeredStreamingShuffleIds: Seq[Int] =
    streamingShuffles.keySet().asScala.toSeq.sorted

  /** The streaming handle registered for a shuffle, if it has one. */
  private[streaming] def streamingHandleFor(
      shuffleId: Int): Option[StreamingShuffleHandle[_, _, _]] =
    Option(streamingShuffles.get(shuffleId))

  /** The coordinator instance this process hosts, which is only ever the driver's. */
  private[streaming] def hostedCoordinator: Option[StreamingShuffleCoordinator] =
    Option(driverCoordinator)

  /** The buffer budget percentage in force, read once at construction. */
  private[streaming] def configuredBufferSizePercent: Int = bufferSizePercent

  /** The spill threshold percentage in force, read once at construction. */
  private[streaming] def configuredSpillThresholdPercent: Int = spillThresholdPercent

  /** The administered egress capacity, where `None` means unlimited and never zero. */
  private[streaming] def configuredMaxBandwidthMbps: Option[Int] = maxBandwidthMbps

  /** Whether verbose streaming logging is on, which is off unless an operator asks for it. */
  private[streaming] def configuredDebugEnabled: Boolean = debugEnabled
}


/**
 * Constants and helper types of [[StreamingShuffleManager]].
 *
 * Everything here is `private[spark]`, so nothing in this file appears on a public surface and the
 * binary-compatibility gate needs no exclusion for any of it.
 */
private[spark] object StreamingShuffleManager {

  /**
   * The value of `spark.shuffle.manager` that selects this manager.
   *
   * Held here so that the short-name table, this class and the package's suites all name it once
   * rather than repeating a string literal that could drift apart.
   */
  val SHORT_NAME: String = "streaming"

  /**
   * The wire protocol version this build speaks, taken from the protocol itself rather than
   * restated.
   *
   * Every handle carries it, every producer registration declares it, and a peer that answers with
   * a different one trips the documented version-mismatch fallback.
   */
  val PROTOCOL_VERSION: Byte = StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION

  /** Rendered in place of a bandwidth figure when no link capacity is administered. */
  val UNLIMITED_BANDWIDTH: String = "unlimited"

  /** Name of the sort-based manager, used in the log lines that explain a delegation. */
  val SORT_MANAGER_NAME: String = classOf[SortShuffleManager].getName

  /** Thread-name prefix of the shared producer-side egress event loop. */
  val EGRESS_THREAD_PREFIX: String =
    s"${StreamingShuffleServerHandler.TRANSPORT_MODULE_NAME}-egress"

  /** Pipeline name of the producer-side frame decoder. */
  val EGRESS_DECODER_NAME: String = "streaming-shuffle-egress-decoder"

  /** Pipeline name of the producer-side egress handler. */
  val EGRESS_HANDLER_NAME: String = "streaming-shuffle-egress-handler"

  /**
   * Port requested when binding an egress listener.
   *
   * Zero asks the operating system for an ephemeral port, which is correct because the address is
   * published to the coordinator rather than configured: a fixed port could not accommodate the
   * several map tasks an executor runs concurrently.
   */
  val EGRESS_EPHEMERAL_PORT: Int = 0

  /** Reported when a bound channel has no inet address, so no producer can be published. */
  val EGRESS_UNKNOWN_PORT: Int = -1

  /**
   * Quiet period, in milliseconds, of the egress event loop's graceful shutdown.
   *
   * Zero, because by the time this manager stops every producing task has already completed and
   * released its channel, so there is no in-flight work worth waiting to quiesce.
   */
  val EGRESS_SHUTDOWN_QUIET_PERIOD_MS: Long = 0L

  /**
   * Upper bound, in milliseconds, on the egress event loop's graceful shutdown.
   *
   * Bounded rather than unbounded so that a wedged Netty thread cannot hold up executor shutdown.
   */
  val EGRESS_SHUTDOWN_TIMEOUT_MS: Long = 1000L

  /**
   * Failed reduce-stage attempts a shuffle is allowed before streaming is withdrawn from it.
   *
   * One, because by the time a streaming read reports failure the reader has already exhausted its
   * own rendezvous polling and its own retransmission window: a second whole-stage attempt would
   * repeat work that has been tried and would spend another of the stage's limited consecutive
   * attempts, whereas standing down after the first spends none of them and lets the recomputation
   * the scheduler is already performing be the one that succeeds.
   */
  val MAX_FAILED_STREAMING_READ_STAGE_ATTEMPTS: Int = 1

  /**
   * The producer/consumer rendezvous of one process.
   *
   * Two fields rather than one because the driver and an executor need different things from the
   * same endpoint. Both need the reference, which is how a producer announces itself and how a
   * consumer discovers producers. Only the driver has the instance, and only it needs one: it is
   * what admits shuffles to the streaming path, what reports the concurrency the egress refill
   * formula divides by, and -- in local mode, where one process is both roles -- what the
   * backpressure ledger arbitrates with.
   *
   * @param endpoint the coordinator this process hosts, or `null` on an executor
   * @param ref a reference to the driver's coordinator, which every process has
   */
  case class StreamingShuffleRendezvous(
      endpoint: StreamingShuffleCoordinator,
      ref: RpcEndpointRef)
}

/**
 * The resolver [[StreamingShuffleManager]] publishes while the streaming path is reachable.
 *
 * It exists because publishing either resolver on its own would be wrong. Publishing the streaming
 * resolver alone would strand a shuffle that fell back to the sort-based path: the block manager
 * routes '''every''' shuffle-block fetch through the manager's resolver, and index-based output can
 * only be read by the resolver that wrote it, so a graceful degradation would turn into a hard
 * fetch failure. Publishing the index-based resolver alone would strand a streaming producer's
 * spilled blocks for the same reason in the other direction.
 *
 * So both are published, behind a router that sends each identity to the one resolver that can
 * answer it. The router asks the streaming resolver first only when it actually holds a producer
 * for the block, which keeps the decision a registry lookup rather than a guess, and sends
 * everything else -- including every identity the streaming path never produces -- to the delegate.
 *
 * The one property this class must have, and the reason it is not simply a subclass of either
 * resolver, is that it is '''not''' an `IndexShuffleBlockResolver`. `ShuffleWriteProcessor` matches
 * on the manager's resolver type to decide whether to initiate push-based merge, and its no-op
 * branch is exactly how streaming declines to participate in merge without a single edit to that
 * file. A subclass of the index resolver would silently opt streaming back in.
 *
 * @param streaming serves blocks a streaming producer spilled to local disk
 * @param delegate serves everything the sort-based path wrote, and everything else
 */
private[streaming] class StreamingShuffleDelegatingBlockResolver(
    streaming: StreamingShuffleBlockResolver,
    delegate: ShuffleBlockResolver) extends ShuffleBlockResolver {

  /**
   * Whether the streaming resolver holds a live producer for the map task that owns this identity.
   *
   * Only the three identities a streaming producer can own are considered, and each is checked
   * against the registry rather than assumed: a `ShuffleBlockId` and a `ShuffleBlockBatchId` name a
   * map task directly, and a `TempShuffleBlockId` is a spill file, which only the streaming
   * resolver ever serves by name. Every other identity -- merged blocks, index blocks, data blocks,
   * broadcast and RDD blocks -- belongs to the delegate.
   */
  private def servedByStreaming(blockId: BlockId): Boolean = blockId match {
    case shuffleBlock: ShuffleBlockId =>
      streaming.producerFor(shuffleBlock.shuffleId, shuffleBlock.mapId).isDefined
    case batch: ShuffleBlockBatchId =>
      streaming.producerFor(batch.shuffleId, batch.mapId).isDefined
    case _: TempShuffleBlockId =>
      !streaming.isStopped
    case _ =>
      false
  }

  /**
   * Routes a block request to whichever resolver produced the block.
   *
   * The routing decision is taken once, before either resolver is asked, so a block is never
   * fetched twice and a failure is never masked by a second attempt against a resolver that could
   * not have held it. That matters for attribution: a missing streaming block must surface as a
   * streaming failure, because that is what drives the fallback, and a missing sort block must
   * surface as the ordinary fetch failure the scheduler already knows how to recover from.
   */
  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]] = None): ManagedBuffer = {
    if (servedByStreaming(blockId)) {
      streaming.getBlockData(blockId, dirs)
    } else {
      delegate.getBlockData(blockId, dirs)
    }
  }

  /**
   * Lists the blocks of one map task, from whichever resolver holds them.
   *
   * Used by the External Shuffle Service to delete a removed executor's files, which is precisely
   * the co-operation the shuffle manager contract asks a custom manager to preserve. A streaming
   * producer contributes its spill files, and the delegate contributes the index and data files of
   * a map task that took the sort-based path, so a mixed shuffle is cleaned up completely rather
   * than partially.
   */
  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    val streamingBlocks = if (streaming.producerFor(shuffleId, mapId).isDefined) {
      streaming.getBlocksForShuffle(shuffleId, mapId)
    } else {
      Seq.empty
    }
    streamingBlocks ++ delegate.getBlocksForShuffle(shuffleId, mapId)
  }

  /**
   * Serves a push-merged block, which only the delegate can ever have produced.
   *
   * Streaming declines push-based merge by construction, so a merged identity can only have come
   * from a shuffle that took the sort-based path, and the delegate is the resolver that wrote it.
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    delegate.getMergedBlockData(blockId, dirs)
  }

  /**
   * Serves push-merged block metadata, which only the delegate can ever have produced.
   */
  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    delegate.getMergedBlockMeta(blockId, dirs)
  }

  /**
   * Stops both resolvers, independently, and never throws.
   *
   * Independently because a failure to release one must not strand the other, and safely re-entrant
   * because the manager stops both this resolver and the sort-based delegate that owns the second
   * of them -- so the delegate's resolver is stopped twice by design, which its implementation
   * tolerates.
   */
  override def stop(): Unit = {
    Utils.tryLogNonFatalError {
      streaming.stop()
    }
    Utils.tryLogNonFatalError {
      delegate.stop()
    }
  }
}
