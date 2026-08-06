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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.DurationLong
import scala.util.control.NonFatal

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CLASS_NAME, CONFIG, CONFIG2, COUNT, EXECUTOR_ID,
  HOST_PORT, MAP_ID, NUM_BYTES, NUM_EVENTS, NUM_PARTITIONS, NUM_SKIPPED, NUM_TASKS, REASON,
  SHUFFLE_ID, STATUS, TASK_ATTEMPT_ID, VALUE}
import org.apache.spark.internal.config
import org.apache.spark.internal.config.{NETWORK_AUTH_ENABLED, SHUFFLE_STREAMING_DEBUG,
  SHUFFLE_STREAMING_ENABLED}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.TransportClient
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.network.shuffle.protocol.streaming.StreamingShuffleMessage
import org.apache.spark.rpc.{RpcEndpointRef, RpcTimeout}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader,
  ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId, ShuffleBlockId,
  ShuffleMergedBlockId, TempShuffleBlockId}
import org.apache.spark.util.{Clock, SystemClock}

/**
 * The `ShuffleManager` of the streaming shuffle subsystem: the single entry point through which
 * every other class in this package is reached.
 *
 * ==How it is selected==
 *
 * `spark.shuffle.manager=streaming` resolves to this class through the short-name table in the
 * `ShuffleManager` companion object, which is the whole of the selection mechanism; the manager is
 * then instantiated reflectively with the `(SparkConf, isDriver: Boolean)` constructor the trait
 * documents. `sort` and `tungsten-sort` are untouched and `sort` remains the default, so an
 * application that does not name this class never loads a line of it.
 *
 * ==Activation, and why every gate exists==
 *
 * Selecting the class is tier one. Tier two is `spark.shuffle.streaming.enabled`, which defaults to
 * `false` and gates the <i>behaviour</i> of the selected class: while it is off, every
 * service-provider call is forwarded verbatim to an internally held, unmodified
 * [[SortShuffleManager]], so behaviour is indistinguishable from sort-based shuffle. That is not
 * redundancy. It gives an operator a kill switch that does not require editing the manager class
 * and restarting into a different implementation, and -- more importantly -- it gives all four
 * documented graceful-degradation conditions a single terminus. Whether streaming is stood down by
 * an operator, by a consumer that fell too far behind, by memory pressure, by link saturation or by
 * a protocol version disagreement, the outcome is the same: this manager delegates, and the
 * sort-based path serves the shuffle.
 *
 * Spark authentication is the mandatory trust gate above those two selection keys. Streaming moves
 * serialized records directly into a remote executor's deserializer and accepts acknowledgements
 * that release retained output, so enabling the streaming key while `spark.authenticate` is false
 * still delegates every call to sort-based shuffle. The transport builders independently refuse to
 * create an unauthenticated streaming channel, and the handlers independently reject one, so a
 * configuration mistake cannot turn the data plane into an unauthenticated deserialization surface.
 *
 * The consequence worth stating plainly is that <b>no configuration, failure or resource condition
 * leaves a job without a working shuffle</b>. Delegation is available because the delegate is
 * constructed unconditionally, in this constructor, before any streaming component exists.
 *
 * ==What activation buys, and what it cannot==
 *
 * An operator turning tier two on should know what the subsystem can and cannot deliver on this
 * platform, so it is recorded here rather than implied.
 *
 * The streaming path removes the map side's materialisation of shuffle output and the reduce side's
 * index-and-fetch round trip and whole-partition buffering. A block is framed, checksummed and
 * pushed the moment it is cut, and a subscribed consumer turns it into records without any of it
 * reaching disk.
 *
 * What activation does <i>not</i> do is change when consumers exist. Task submission belongs to the
 * DAG scheduler and to the task scheduler, both of which this feature may not modify at all, and
 * the unmodified scheduler submits a stage only once every parent stage reports its output
 * available. For a map stage whose tasks each run once, its reduce tasks are therefore submitted
 * after the last map task has finished, no consumer is subscribed while any of them produces, and
 * what those reduce tasks read is retained output served by [[StreamingShuffleBlockResolver]]
 * rather than a live stream. Producer/consumer overlap is exercised only where a consumer really is
 * attached -- a reconnecting consumer asking for a replay of the window it has not acknowledged,
 * and any subscriber a scheduler that submitted consumers earlier would provide. Both halves of the
 * subsystem are built for that case and take it whenever it arises; neither pretends to create it.
 *
 * ==What is consulted before streaming is used==
 *
 * Three surfaces are consulted, because each knows something the others cannot.
 *
 *  - The driver's answer, carried on [[StreamingShuffleRegistrationGrant]] when a shuffle is
 *    registered and on the fallback state a lookup reports afterwards. It is the only shuffle-wide,
 *    cluster-wide verdict.
 *  - This executor's own [[StreamingShuffleFallbackPolicy]], which holds both the kill switch and
 *    any trip condition observed locally. A trip observed here is as binding as one the driver has
 *    latched, and consulting it first avoids building components this executor has already decided
 *    not to use.
 *  - The shape of the request itself. Two dependencies cannot be streamed at all: one that asks for
 *    map-side combining, and a read that narrows the map range. Both are declined here rather than
 *    failed later.
 *
 * ==Component ownership and scope==
 *
 * Scope is not uniform, and getting it wrong is the difference between a working subsystem and a
 * subtly broken one, so each is stated.
 *
 *  - Executor scoped, one per JVM, built here: the fallback policy, the block resolver, the token
 *    bucket, the backpressure ledger and the producer connector. Each of these exists precisely in
 *    order to reason across the tasks and shuffles sharing one executor -- the ledger holds the
 *    single executor-wide consumer receive quota and is the consumer side's only buffer-utilisation
 *    gauge contributor, the bucket divides one egress budget, the policy remembers a trip across
 *    tasks, and the resolver owns retained map output that must outlive the task that produced it.
 *    Building two of any of them would double a budget that is supposed to be shared.
 *  - Driver scoped, one per application: the [[StreamingShuffleCoordinator]] endpoint. It is
 *    registered here on the driver and resolved by name on every executor.
 *  - Task scoped, one per map task: the error notifier, the spill manager, the server handler and
 *    the ephemeral listener that handler is installed behind. A shared handler would interleave two
 *    producers' sequence numbers on one stream; a shared notifier would re-throw one task's failure
 *    at another.
 *
 * ==Which resolver is exposed, and why it depends on the kill switch==
 *
 * `shuffleBlockResolver` is consulted by the block manager to serve local shuffle blocks, and by
 * `ShuffleWriteProcessor` to decide whether push-based merge applies. Both consult <i>this</i>
 * manager's resolver, never the delegate's, so the choice matters.
 *
 *  - Kill switch off: the delegate's own `IndexShuffleBlockResolver` is exposed unchanged. The
 *    push-merge branch therefore fires exactly as it does under sort-based shuffle, which is what
 *    makes "indistinguishable when disabled" true of this method too.
 *  - Streaming enabled: a router is exposed that serves a block from
 *    [[StreamingShuffleBlockResolver]] when that resolver holds the producer of the block, and from
 *    the delegate's index resolver otherwise. The second half is not hypothetical: a dependency
 *    declined for map-side combining is written by the sort-based writer inside a streaming-enabled
 *    application, and its blocks have to be servable. The router is deliberately not an
 *    `IndexShuffleBlockResolver` subtype, so push-based merge declines for the whole application
 *    while streaming is enabled -- which is the documented posture, since streaming neither extends
 *    nor interoperates with push-based merge.
 *
 * ==Initialization order==
 *
 * `SparkEnv` publishes itself before calling `initializeShuffleManager()`, on the driver and on an
 * executor alike, so `SparkEnv.get` is live in this constructor and no environment object has to be
 * threaded in. Components that dereference the environment are nonetheless built lazily, on first
 * use, for two reasons: a driver that never runs a streaming shuffle should not pay for a transport
 * client factory, and a lazily built component cannot be caught out by a future reordering of that
 * call. The one exception is the coordinator endpoint, which is registered eagerly on the driver:
 * an executor resolves it by name, and a name registered only on first use is a race an executor
 * can lose.
 *
 * ==Thread safety and shutdown==
 *
 * Every method is safe to call concurrently. Per-shuffle state is one concurrent map; the lazily
 * built components are initialized under Scala's own lazy-val latch. [[stop]] is idempotent: the
 * first caller wins a compare-and-set and closes what was actually created, in reverse dependency
 * order, containing and logging each non-fatal failure so one stubborn component cannot prevent the
 * rest from being released.
 *
 * @param conf the executor's or driver's configuration, read once here and held immutably, which is
 *             what makes "a configuration change requires a restart" true by construction
 * @param isDriver whether this JVM is the driver, which decides whether the coordinator endpoint is
 *                 hosted here or merely referenced
 */
private[spark] class StreamingShuffleManager(conf: SparkConf, isDriver: Boolean)
  extends ShuffleManager with Logging {

  import StreamingShuffleManager._

  // Configuration, read exactly once.

  private val streamingRequested: Boolean = conf.get(SHUFFLE_STREAMING_ENABLED)
  private val authenticationEnabled: Boolean = conf.get(NETWORK_AUTH_ENABLED)
  private val streamingEnabled: Boolean = streamingRequested && authenticationEnabled
  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  // Whether this application runs with the External Shuffle Service. Read here, once, because it is
  // a structural exclusion rather than a runtime condition: the service serves blocks from files an
  // executor left behind, and a streaming shuffle leaves none it could read. See declineReason.
  private val externalShuffleServiceEnabled: Boolean = conf.get(config.SHUFFLE_SERVICE_ENABLED)

  /**
   * Whether this JVM builds any streaming state at all.
   *
   * The two flags above are separate readings with one structural consequence, so they are combined
   * once, here, rather than at each of the places that would otherwise have to remember both. An
   * application running with the External Shuffle Service is in exactly the posture the kill switch
   * produces: no coordinator endpoint is hosted, no streaming resolver is published, and therefore
   * no streaming handle can be minted -- the sort-based delegate owns every shuffle end to end, and
   * leaves precisely the index and data files the service already knows how to serve. Applying the
   * exclusion structurally, and not merely as a registration-time refusal, is what makes that true
   * of the resolver too: `ShuffleWriteProcessor` inspects the published resolver, and publishing a
   * streaming one for an application that will never stream is a difference the service could
   * observe.
   *
   * [[declineReason]] still reads the two flags apart, so the reason an operator is given names the
   * setting that actually excluded the shuffle.
   */
  private val streamingActive: Boolean = streamingEnabled && !externalShuffleServiceEnabled

  // The delegate. Constructed unconditionally and first, so that delegation is available from the
  // moment this object exists -- including from the constructor of anything below it.

  private val sortShuffleManager: SortShuffleManager = new SortShuffleManager(conf)

  /** Executor-wide degradation policy: the kill switch plus every locally observed trip. */
  private val fallbackPolicy: StreamingShuffleFallbackPolicy =
    new StreamingShuffleFallbackPolicy(conf)

  /**
   * The degradation policy every service-provider method on this manager routes on.
   *
   * Exposed read-only at package scope for one reason: the four trip conditions are observed by the
   * writer, the reader and the flow-control protocol running on this executor, every one of which
   * holds '''this''' instance. Whether a trip actually changes what `registerShuffle`, `getWriter`
   * and `getReader` hand back can therefore only be established by tripping the policy those
   * methods consult, and no other route to it exists from outside this class. The instance is
   * created once and never replaced, so the reference a caller reads is the reference the routing
   * uses.
   */
  private[streaming] def degradationPolicy: StreamingShuffleFallbackPolicy = fallbackPolicy

  private val stopped = new AtomicBoolean(false)

  /**
   * Wall clock, used for the one timestamp this class produces.
   *
   * Held as a field rather than read inline so that this class follows the same discipline as
   * every other component in the package, and deliberately not exposed as a constructor
   * parameter: the service-provider contract fixes the constructor at `(SparkConf, Boolean)`,
   * and a third parameter -- even a defaulted one -- would remove the two-argument constructor
   * the reflective factory looks for, making this manager impossible to instantiate.
   */
  private val clock: Clock = new SystemClock

  /**
   * Capability token of each shuffle registered by this JVM, keyed by shuffle id.
   *
   * Held because a token is minted on the driver when a shuffle is registered and is required again
   * when the same shuffle is unregistered, and the handle that carried it to the executors is not
   * handed back. Only the driver populates this map, which is also why an executor's
   * `unregisterShuffle` has nothing streaming to do beyond releasing local state.
   */
  private val capabilityTokens = new ConcurrentHashMap[Integer, String]()

  // Executor-scoped and driver-scoped collaborators.

  /**
   * The driver's coordinator instance, present only on the driver and only while streaming is
   * enabled. Registered eagerly, because executors resolve this endpoint by name and a name that
   * appears only on first use is a race an executor can lose.
   */
  private val driverCoordinator: Option[StreamingShuffleCoordinator] = {
    if (isDriver && streamingActive) {
      Some(new StreamingShuffleCoordinator(SparkEnv.get.rpcEnv, conf))
    } else {
      None
    }
  }

  /**
   * Reference to the coordinator endpoint: the local instance on the driver, a lookup elsewhere.
   *
   * Lazy because an executor must not reach the driver while this manager is being constructed --
   * a manager is built on every executor whether or not that executor ever streams a shuffle -- and
   * eagerly forced on the driver by the initialiser below, which is where the endpoint name is
   * actually published.
   */
  private lazy val coordinatorRef: RpcEndpointRef = {
    val rpcEnv = SparkEnv.get.rpcEnv
    StreamingShuffleCoordinator.registerOrLookupEndpoint(rpcEnv, conf, isDriver,
      driverCoordinator.getOrElse(new StreamingShuffleCoordinator(rpcEnv, conf)))
  }

  // Publishes the coordinator's endpoint name on the driver at construction time, which is what
  // makes the claim on `driverCoordinator` above true. Constructing the coordinator instance is not
  // the same thing as registering its name: registration happens inside `coordinatorRef`, and on
  // the driver nothing else forces it, because every driver-side call goes to the instance
  // directly. Left deferred, the name would first appear when the driver happened to need a
  // *reference* -- while the first thing an executor does with a streaming shuffle is build a
  // coordinator gateway from exactly that name. That is a race an executor loses with
  // RpcEndpointNotFoundException, and it is invisible in local mode, where driver and executor
  // share one manager and the driver's own lookup registers the name on the way past.
  driverCoordinator.foreach(_ => coordinatorRef)

  /**
   * Executor-scoped owner of retained map output. Built eagerly when streaming is enabled, because
   * [[shuffleBlockResolver]] must answer from the moment this manager exists and this class
   * dereferences no environment object while constructing.
   */
  private val streamingBlockResolver: Option[StreamingShuffleBlockResolver] = {
    if (streamingActive) Some(new StreamingShuffleBlockResolver(conf)) else None
  }

  /**
   * The executor's whole egress allowance, divided among the shuffles it is producing for.
   *
   * <b>One limiter per shuffle, not one limiter for the executor.</b> The contract fixes the
   * per-shuffle token rate at `maxBandwidthMBps / numConcurrentShuffles`. A single shared limiter
   * set to that share is the same arithmetic applied to the wrong subject: it caps the executor's
   * entire egress at one shuffle's worth, so an executor producing for three shuffles admits a
   * third of the cap in total rather than the whole of it, and every one of those shuffles is paced
   * at a third of what it is owed. Holding a limiter per shuffle, and republishing every live
   * limiter's share whenever the divisor moves, is what makes the division real in both directions
   * -- the aggregate obeys the cap and no shuffle is throttled below its share.
   *
   * Lazy, and its concurrency source lazier still: the divisor's second input is read from the
   * driver, and an executor must not reach the driver while its shuffle manager is being
   * constructed. Nothing here asks anything until a share is actually being returned.
   */
  private lazy val egressBudget: TokenBucketRateLimiter.ExecutorEgressBudget =
    TokenBucketRateLimiter.executorBudget(conf, clock, () => executorConcurrency())

  @volatile private var backpressureBuilt: Boolean = false

  /**
   * The executor's single credit ledger, liveness timer set and consumer receive quota. The
   * coordinator instance is passed when this JVM hosts one and `null` otherwise, which the protocol
   * documents and handles: an executor arbitrates over the shuffles registered locally instead.
   *
   * The flag above is what lets the release paths know whether there is any flow-control state to
   * return without forcing this value, since `unregisterShuffle` reaches every executor of a
   * streaming-enabled application including those that never streamed a byte.
   */
  private lazy val backpressure: BackpressureProtocol = {
    val protocol = new BackpressureProtocol(conf, driverCoordinator.orNull, egressBudget)
    backpressureBuilt = true
    protocol
  }

  /**
   * This executor's egress pacing, read-only and at package scope, for the same reason
   * [[degradationPolicy]] is exposed: the writers and the flow-control protocol on this executor
   * all charge '''this''' budget, so whether the administered bandwidth cap is actually reached by
   * pacing -- rather than by the fallback policy standing streaming down once the link has already
   * been overrun -- can only be established by reading the very budget they charge.
   *
   * Deliberately not forced: reading it builds the budget, which is harmless, but a caller on the
   * shutdown path should consult [[flowControlBuilt]] first rather than construct state in order
   * to ask whether any exists.
   */
  private[streaming] def egressPacing: TokenBucketRateLimiter.ExecutorEgressBudget = egressBudget

  /**
   * This executor's flow-control protocol, read-only and at package scope. It carries the
   * throttled stream count and the backpressure-event count, which are the two readings that
   * distinguish a paced producer from one that was never held back at all.
   */
  private[streaming] def flowControl: BackpressureProtocol = backpressure

  /** Whether any flow-control state exists yet, so a caller can ask without creating it. */
  private[streaming] def flowControlBuilt: Boolean = backpressureBuilt

  /**
   * The window that bounds this manager's per-shuffle registration record.
   *
   * An instance field is enough: there is one manager per driver and per executor, so this
   * instance's lifetime is the process's. The aggregation type and window are the subsystem's
   * shared ones, so an operator reads one convention for every bounded record the feature makes.
   */
  private val registrationLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  @volatile private var listenerBound: Boolean = false

  /**
   * The executor's one streaming shuffle listener, together with the transport it owns.
   *
   * One per executor, not one per map task, and the reason is twofold. A bound transport server
   * creates its own boss and worker event-loop groups, so a server per task would multiply threads
   * and listening sockets by the task count for a subsystem that needs a single port. More
   * decisively, the DAG scheduler -- an absolute preservation zone -- submits a reduce stage only
   * once its map stage has finished, so a listener whose lifetime were a map task's would always be
   * gone before the first consumer could connect. Binding here makes the serving endpoint live as
   * long as this manager does, which is what lets it serve output that already outlives its task
   * because [[StreamingShuffleBlockResolver]] retains it.
   *
   * Lazy, so an executor that never produces a streaming shuffle never binds a port; and the flag
   * above is what lets [[stop]] know whether there is anything to close without forcing the lazy
   * value and binding a port purely in order to release it.
   */
  private lazy val streamingListener:
      (StreamingShuffleListener, TransportContext, StreamingShuffleListener.BoundServer) = {
    val bound = StreamingShuffleListener.bind(conf)
    listenerBound = true
    bound
  }

  @volatile private var connectorCreated: Boolean = false

  /**
   * The executor's single consumer-side connector. It holds an event loop, so [[stop]] closes it.
   */
  private lazy val producerConnector: NettyStreamingShuffleProducerConnector = {
    val connector = new NettyStreamingShuffleProducerConnector(conf)
    connectorCreated = true
    connector
  }

  /** Bounded deadline for every coordinator ask this manager or its writers make. */
  private lazy val coordinatorTimeout: RpcTimeout = {
    val configured = conf.getTimeAsMs(StreamingShuffleServerHandler.CONNECTION_TIMEOUT_KEY,
      s"${StreamingShuffleServerHandler.CONNECTION_TIMEOUT_SECONDS}s")
    new RpcTimeout(math.max(MIN_COORDINATOR_TIMEOUT_MS, configured).millis,
      StreamingShuffleServerHandler.CONNECTION_TIMEOUT_KEY)
  }

  /**
   * The collaborators every reduce task on this executor shares. Built once; the error notifier is
   * deliberately absent, because each reader builds its own so that one task's failure is never
   * re-thrown at another.
   */
  private lazy val readerContext: StreamingShuffleReaderContext = StreamingShuffleReaderContext(
    coordinatorRef = coordinatorRef,
    backpressure = backpressure,
    fallbackPolicy = fallbackPolicy,
    connector = producerConnector,
    serializerManager = serializerManager)

  private def serializerManager: SerializerManager = SparkEnv.get.serializerManager

  if (debugEnabled) {
    logInfo(log"StreamingShuffleManager started on " +
      log"${MDC(CLASS_NAME, if (isDriver) "the driver" else "an executor")} with " +
      log"${MDC(CONFIG, SHUFFLE_STREAMING_ENABLED.key)}=${MDC(VALUE, streamingEnabled)} and " +
      log"${MDC(CONFIG2, config.SHUFFLE_SERVICE_ENABLED.key)}=" +
      log"${MDC(STATUS, externalShuffleServiceEnabled)}, so streaming state is " +
      log"${MDC(REASON, if (streamingActive) "built" else "not built")} in this JVM")
  }

  // ShuffleManager service-provider interface.

  /**
   * The resolver the block manager serves local shuffle blocks from, and the one
   * `ShuffleWriteProcessor` inspects when deciding whether push-based merge applies.
   *
   * See the class comment for why the choice is made on the kill switch and what each branch means
   * for push-based merge.
   */
  override val shuffleBlockResolver: ShuffleBlockResolver = streamingBlockResolver match {
    case Some(streaming) =>
      new StreamingShuffleBlockRouter(streaming, sortShuffleManager.shuffleBlockResolver)
    case None =>
      sortShuffleManager.shuffleBlockResolver
  }

  /**
   * Registers a shuffle on the driver and decides, once and for the whole shuffle, whether it will
   * be streamed.
   *
   * The decision is taken here rather than per task because it must be the same on every executor:
   * a shuffle half of whose map tasks streamed and half of which wrote sorted files has two
   * incompatible reduce-side read paths. Returning a [[StreamingShuffleHandle]] is the commitment,
   * and the handle carries the registration state -- partition count, protocol version, coordinator
   * epoch and capability token -- to both ends of the shuffle inside the task binary.
   *
   * Any other outcome returns whatever the delegate would have returned, which is why a declined
   * shuffle is not merely correct but literally identical to one in a sort-based application.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    declineReason(dependency) match {
      case Some(reason) =>
        if (debugEnabled) {
          logDebug(log"Streaming shuffle declined shuffle ${MDC(SHUFFLE_ID, shuffleId)}: " +
            log"${MDC(REASON, reason)}; it will use the sort-based shuffle")
        }
        sortShuffleManager.registerShuffle(shuffleId, dependency)
      case None =>
        val numPartitions = dependency.partitioner.numPartitions
        val numMaps = dependency.rdd.partitions.length
        grantFor(shuffleId, numPartitions, numMaps) match {
          case Some(grant) =>
            capabilityTokens.put(shuffleId, grant.capabilityToken)
            // Bounded on a window rather than emitted per shuffle, for the same reason the
            // coordinator's own registration record is: `registerShuffle` runs on the driver once
            // per shuffle, so this record's volume is the application's shuffle count, and a
            // workload that submits several shuffles a second would spend the log budget restating
            // a configuration that has not changed. The admitted record says how many registrations
            // it stands in for; the per-shuffle form is on the debug key.
            val entry = log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} registered with " +
              log"${MDC(NUM_PARTITIONS, numPartitions)} partition(s) across " +
              log"${MDC(NUM_TASKS, numMaps)} map task(s) at epoch " +
              log"${MDC(COUNT, grant.coordinatorEpoch)}"
            registrationLogAggregator.record(clock.getTimeMillis()) match {
              case Some(summary) =>
                logInfo(entry + log"; ${MDC(NUM_EVENTS, summary.occurrences)} streaming " +
                  log"shuffle(s) registered here, " +
                  log"${MDC(NUM_SKIPPED, summary.unreported)} of them not reported " +
                  log"individually so that the log budget is kept")
              case None =>
                // At the level it was emitted at, under the feature's own key, so that setting the
                // streaming debug key restores the per-shuffle record without also having to move
                // the logging framework to DEBUG.
                if (debugEnabled) {
                  logInfo(entry)
                }
            }
            new StreamingShuffleHandle(shuffleId, dependency, numPartitions,
              StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, grant.coordinatorEpoch,
              grant.capabilityToken)
          case None =>
            sortShuffleManager.registerShuffle(shuffleId, dependency)
        }
    }
  }

  /**
   * Builds the producer half for one map task, or delegates.
   *
   * Everything task scoped is built here, in an order chosen so that nothing is created on a path
   * that turns out not to stream: the handler and its ephemeral listener come first because the
   * listener's bound port is part of the producer's published address, the coordinator is asked to
   * publish that address next, and only an accepted registration goes on to bind memory to the task
   * and construct the writer.
   */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      case streaming: StreamingShuffleHandle[K @unchecked, V @unchecked, _]
          if streamingAvailableFor(streaming) =>
        newStreamingWriter(streaming, mapId, context, metrics)
      case _ =>
        sortShuffleManager.getWriter(handle, mapId, context, metrics)
    }
  }

  /**
   * Builds the consumer half for one reduce task, or delegates.
   *
   * Only the seven-argument form is overridden. The five-argument overload is `final` in the trait
   * and already delegates here with the whole map range, so overriding it is neither possible nor
   * necessary.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    handle match {
      case streaming: StreamingShuffleHandle[K @unchecked, _, C @unchecked]
          if streamingAvailableFor(streaming) =>
        // Any map range, narrowed or whole. A producer registration carries the map INDEX with the
        // map id, so the reader selects the registrations of the range it was asked for and waits
        // only for those; `[0, Int.MaxValue)` selects everything, so the whole-range case needs no
        // branch of its own. Declining a narrowed range -- which adaptive execution produces
        // routinely when it coalesces or splits a stage -- stood the entire shuffle down for an
        // ordinary, correct request, and recorded the decline as a protocol-version mismatch that
        // had not occurred.
        new StreamingShuffleReader[K, C](streaming, startMapIndex, endMapIndex, startPartition,
          endPartition, context, metrics, conf, readerContext)
      case _ =>
        sortShuffleManager.getReader(handle, startMapIndex, endMapIndex, startPartition,
          endPartition, context, metrics)
    }
  }

  /**
   * Releases everything this JVM holds for one shuffle.
   *
   * Every owner is told, unconditionally and in the order that keeps local disk honest. The
   * resolver goes first, because it is the only thing that unlinks retained spill files and a
   * shuffle whose files are never unlinked is a permanent local-disk leak. The coordinator is told
   * next so that no consumer can resolve a producer of a shuffle that no longer exists. The
   * executor's flow-control state is returned after that, and the order there is load bearing in
   * its own right: returning the shuffle's share of the egress allowance re-reads the coordinator's
   * count of this executor's shuffles, and doing it before the coordinator has forgotten this one
   * would read a divisor that still included it. The delegate is told last and always, because a
   * declined dependency of this shuffle id may have been written by the sort-based writer.
   *
   * This method reaches every JVM of the application, driver and executors alike, because the block
   * manager's storage endpoint routes the cleaner's removal to each of them. That is what makes it
   * the right place to return executor-scoped state, and it is why nothing here forces a lazy
   * collaborator: an executor that never streamed must not build a flow-control protocol purely in
   * order to release one.
   *
   * @return the delegate's verdict, since it is the only participant whose bookkeeping the caller
   *         can act upon
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    // Routing stops before the files are unlinked, and the order is load bearing: a frame routed to
    // a handler whose retained store had already lost its bytes would be answered as an unservable
    // block rather than as the "producer is gone" the consumer needs to see.
    if (listenerBound) {
      guard(s"stop routing producers of shuffle $shuffleId") {
        val withdrawn = streamingListener._1.deregisterShuffle(shuffleId)
        if (withdrawn > 0 && debugEnabled) {
          logDebug(log"Streaming shuffle stopped routing ${MDC(COUNT, withdrawn)} producer(s) " +
            log"of shuffle ${MDC(SHUFFLE_ID, shuffleId)}")
        }
      }
    }
    // Each routed generation has already withdrawn its own retained output, because withdrawing the
    // routing entry is one step of the single operation that does both. This sweep is what reaches
    // the generations that were never routed -- an announcement the driver declined, a hand-off
    // that was refused -- so it is a completeness pass rather than the primary path, and it is
    // expected to release nothing in the ordinary case.
    streamingBlockResolver.foreach { resolver =>
      guard(s"release retained output of shuffle $shuffleId") {
        val released = resolver.removeShuffle(shuffleId)
        if (released > 0 && debugEnabled) {
          logDebug(log"Streaming shuffle unlinked ${MDC(COUNT, released)} retained spill file(s) " +
            log"of shuffle ${MDC(SHUFFLE_ID, shuffleId)}")
        }
      }
    }
    Option(capabilityTokens.remove(shuffleId)).foreach { token =>
      driverCoordinator.foreach { coordinator =>
        guard(s"unregister shuffle $shuffleId with the coordinator") {
          coordinator.unregisterShuffle(shuffleId, token)
        }
      }
    }
    if (backpressureBuilt) {
      releaseShuffleStreamingState(shuffleId, "the shuffle was unregistered")
    }
    guard(s"forget the degradation state of shuffle $shuffleId") {
      fallbackPolicy.forgetShuffle(shuffleId)
    }
    sortShuffleManager.unregisterShuffle(shuffleId)
  }

  /**
   * Shuts the subsystem down exactly once, releasing only what was actually built.
   *
   * The order is the reverse of the dependency order, so nothing is released while something that
   * uses it is still live: consumer channels, then the serving listener, then the executor's
   * flow-control state, then retained producer output, then the rendezvous endpoint, then the
   * delegate. Every step is contained, because a shutdown that abandoned the remaining steps on the
   * first failure would leak precisely the resources this method exists to release.
   */
  override def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      if (connectorCreated) {
        guard("close the streaming producer connector") {
          producerConnector.close()
          val unreleased = producerConnector.unreleasedChannels
          if (unreleased > 0) {
            logWarning(log"Streaming shuffle left ${MDC(COUNT, unreleased)} consumer channel(s) " +
              log"unclosed at shutdown; their sockets are released by the event loop's own " +
              log"termination")
          }
        }
      }
      // The listener goes before the retained output it serves, so no consumer can be mid-request
      // against a store that is being torn down.
      closeStreamingListener()
      // Both transports are closed, so no new frame can submit work. Stop the executor-wide
      // data-plane stripes before releasing the ledgers and retained output their accepted tasks
      // may still reference.
      guard("stop the executor's streaming data-plane workers") {
        BackpressureProtocol.shutdownDataPlaneWorkers()
      }
      // After both transports, because a limiter that is being retired must not be one a live
      // channel is still charging against. `stopped` is already set, so the releases below ask the
      // driver nothing: a divisor that will never pace another byte is not worth a round trip, and
      // a driver that has already gone is not worth a warning per shuffle.
      if (backpressureBuilt) {
        guard("return the executor's streaming flow-control state") {
          val released = backpressure.registeredShuffleIds
          released.foreach(backpressure.unregisterShuffle)
          egressBudget.reset()
          if (released.nonEmpty && debugEnabled) {
            logDebug(log"Streaming shuffle returned the executor state of " +
              log"${MDC(COUNT, released.size)} shuffle(s) at shutdown")
          }
        }
      }
      // Through the exposed resolver rather than the streaming one directly, so that whichever
      // resolver this manager published is the one that gets stopped. Both are idempotent, and the
      // sort delegate's own stop below reaches its resolver again harmlessly.
      guard("stop the shuffle block resolver")(shuffleBlockResolver.stop())
      driverCoordinator.foreach { _ =>
        guard("stop the streaming shuffle coordinator") {
          coordinatorRef.askSync[Boolean](StopStreamingShuffleCoordinator, coordinatorTimeout)
        }
      }
      guard("stop the sort-based shuffle delegate")(sortShuffleManager.stop())
      // The executor's buffer threshold ticker is created by whichever map task first needed
      // polling and is deliberately executor-scoped, so no individual spill manager may end it --
      // and none does. This is the one owner whose lifetime matches the ticker's, so it is stopped
      // here, after every producer has been released and can therefore no longer ask to be polled.
      guard("stop the executor's streaming buffer threshold ticker") {
        MemorySpillManager.shutdownExecutorPoller()
      }
    }
  }

  // Inspection. Pure reads of components this manager owns, exposed so that what a running executor
  // decided is observable without reaching into internals and without standing up a second copy of
  // the subsystem to reason about.

  /**
   * The four graceful-degradation trip conditions, as this executor observes them.
   *
   * Exposed because the policy is the one component of the subsystem whose state is a decision
   * rather than a measurement: it is what every service-provider call consults before choosing
   * between the streaming path and the sort-based delegate. A caller may read it to learn what this
   * executor concluded, and may report an observation to it -- which is exactly what a running task
   * does, since the writer and the reader are handed this very instance.
   */
  def streamingFallbackPolicy: StreamingShuffleFallbackPolicy = fallbackPolicy

  /**
   * The executor's serving listener, if one has been bound.
   *
   * `None` rather than a forced binding: the listener is created lazily so that an executor which
   * never produces a streaming shuffle never opens a port, and a reader that forced it would open
   * one purely in order to be observed.
   */
  def boundStreamingListener: Option[StreamingShuffleListener] = {
    if (listenerBound) Some(streamingListener._1) else None
  }

  /**
   * The coordinator this manager hosts, present only on the driver.
   *
   * Absent on an executor by construction: an executor resolves a reference to the driver's
   * endpoint rather than hosting one, so there is no local instance to hand back and returning a
   * proxy would misrepresent which side owns the registry.
   *
   * Intended for diagnostics and tests. The registry is what supplies `numConcurrentShuffles` to
   * every rate limiter in the application, so a test asserting that concurrent shuffles genuinely
   * arbitrate against one another has to read the registry those jobs registered with -- not a
   * fresh instance, which would answer questions about itself.
   */
  def boundCoordinator: Option[StreamingShuffleCoordinator] = driverCoordinator

  /**
   * The executor's bound serving socket, if one has been bound.
   *
   * Exposed at package visibility for exactly one reason, and no production path reads it: making a
   * live producer UNREACHABLE without withdrawing it requires closing the socket its consumers are
   * attached to, and that is what a network partition looks like from a consumer's side -- channels
   * die with no end-of-stream and no invalidation, leaving only silence for the reader's connection
   * timeout to interpret. A partition driven any other way would be a fixture asserting against
   * itself. [[stop]] releases this same server through the lazy triple, so nothing here changes the
   * lifecycle; the accessor only observes it.
   */
  private[streaming] def boundStreamingServer: Option[StreamingShuffleListener.BoundServer] = {
    if (listenerBound) Some(streamingListener._3) else None
  }

  /**
   * Consumer routes currently carried by this executor's streaming connector.
   *
   * A snapshot rather than the connector itself, so diagnostics and integration fault tests can
   * observe or close a real live route without gaining ownership of the connector's registries.
   * Empty until a reduce task has actually opened a streaming producer channel.
   */
  private[streaming] def activeStreamingConsumerRoutes:
      Seq[(StreamingShuffleClientHandler, TransportClient)] = {
    if (connectorCreated) producerConnector.activeRoutes else Nil
  }

  /**
   * Pauses inbound traffic on every consumer channel this executor currently owns.
   *
   * This is a package-scoped fault-injection seam for end-to-end validation of the production
   * connection-timeout path. It changes the actual Netty channels rather than a fallback-policy
   * flag, and it is deliberately non-sticky: task cleanup closes the paused channels and a retry
   * opens fresh ones with auto-read enabled.
   *
   * @return the number of live channels whose inbound traffic was paused
   */
  private[streaming] def pauseInboundStreamingChannels(): Int = {
    if (connectorCreated) producerConnector.pauseInboundTraffic() else 0
  }

  /** Registered streaming shuffles on this driver, clamped to one on executor-only managers. */
  private[streaming] def registeredStreamingShuffleCount: Int =
    driverCoordinator.map(_.numConcurrentShuffles).getOrElse(1)

  /** Retained producer generations still owned by this executor's streaming block resolver. */
  private[streaming] def retainedStreamingProducerCount: Int =
    streamingBlockResolver.map(_.registeredProducerCount).getOrElse(0)

  /**
   * Whether every Netty event-loop group this manager created has completed termination.
   *
   * Each transport retains and awaits its own termination future. This read is therefore a
   * deterministic ownership assertion, not a scan of unrelated JVM threads by name.
   */
  private[streaming] def streamingTransportsTerminated: Boolean = {
    val listenerTerminated = !listenerBound || streamingListener._3.isTerminated
    val connectorTerminated =
      !connectorCreated || producerConnector.transportResourcesTerminated
    listenerTerminated && connectorTerminated
  }

  // Gating.

  /**
   * Why this dependency cannot be streamed, or `None` when it can.
   *
   * Map-side combining is the one structural exclusion. A combining writer has to accumulate every
   * record of a partition before it can emit the combined value for a key, which is precisely the
   * materialisation barrier the streaming path exists to remove; there is nothing to pipeline until
   * the last record has been seen. [[StreamingShuffleWriter]] carries a `require` on the same
   * condition, so a future change that stopped honouring this would fail the task loudly rather
   * than stream something the reader cannot reassemble.
   */
  private def declineReason(dependency: ShuffleDependency[_, _, _]): Option[String] = {
    if (!streamingRequested) {
      Some(s"${SHUFFLE_STREAMING_ENABLED.key} is false")
    } else if (externalShuffleServiceEnabled) {
      // The External Shuffle Service serves a shuffle block by reading the index and data files an
      // executor left on local disk, from a process that outlives that executor. Streaming has
      // neither: this subsystem's resolver is JVM-local -- it answers from the retained window and
      // the spill segments of a live producer, which the service cannot see and would not know how
      // to interpret -- and the producer address a consumer resolves names an executor that the
      // service exists to make dispensable. So a streaming shuffle under an enabled service is a
      // shuffle whose blocks the service cannot serve, and every consumer routed to it would fail.
      // Declining at REGISTRATION is what makes that safe rather than merely detected: no streaming
      // handle is created for the application at all, so the sort-based delegate owns the shuffle
      // end to end and its files are exactly what the service already knows how to serve.
      Some(s"${config.SHUFFLE_SERVICE_ENABLED.key} is true, and the External Shuffle Service " +
        "cannot serve a streaming shuffle's blocks")
    } else if (!authenticationEnabled) {
      // The streaming data plane carries serialised records, so it must be authenticated before any
      // byte of it reaches deserialisation. Declining at registration is what makes that structural
      // rather than merely checked: with authentication off no streaming handle exists at all.
      Some(s"${NETWORK_AUTH_ENABLED.key} is false; streaming requires authenticated transport")
    } else if (fallbackPolicy.hasTripped) {
      Some(fallbackPolicy.trippedReason.map(_.description).getOrElse("streaming has stood down"))
    } else if (dependency.mapSideCombine) {
      Some("the dependency asks for map-side combining, which cannot be pipelined")
    } else if (!isDriver) {
      Some("a shuffle may only be registered from the driver")
    } else {
      None
    }
  }

  /**
   * Whether a shuffle already registered for streaming may still be streamed on this executor.
   *
   * All surfaces are consulted cheapest first, and a verdict learned from the driver is cached
   * locally so that the next task on this executor answers without an RPC.
   *
   * <b>A local trip is declared before it is acted upon.</b> That is the whole difference between
   * this method answering correctly and answering locally. `hasTripped` is this executor's own
   * observation of one of the four conditions, and no other participant need have seen it;
   * answering `false` on the strength of it alone would have this executor's tasks written and read
   * by the sort-based delegate while every other executor kept streaming the same shuffle, which is
   * one shuffle with two producers of the same output and two incompatible reduce-side read paths.
   * So the trip is turned into a shuffle-wide decision first, and the decision -- not the local
   * observation -- is what withdraws streaming.
   *
   * The kill switch is the one condition that is not declared, because it is not an observation. It
   * is an operator turning streaming off, the closed set of fallback conditions has no member for
   * it, and it cannot split a shuffle in practice: `registerShuffle` runs on the driver, so a
   * streaming handle exists only for an application whose configuration enabled streaming, and that
   * configuration is the one every executor of it receives.
   *
   * <b>The protocol-version check belongs here, before anything is published.</b> Whether this
   * build can speak the version a shuffle was registered under is decidable from the handle alone:
   * it needs no budget, no coordinator round trip and no writer. Answering it here rather than
   * inside the writer's own preflight is what keeps a version mismatch from publishing a routing
   * entry and a producer address that the very next step would have to withdraw. The writer keeps
   * its own copy of the check, because it must remain correct on its own terms for the conditions
   * that genuinely cannot be decided until it exists, and a shuffle whose ownership is published
   * must always have a way to hand that ownership back -- see
   * [[StreamingShuffleWriter.withdrawPrePublishedOwnership]].
   */
  private def streamingAvailableFor(handle: StreamingShuffleHandle[_, _, _]): Boolean = {
    val shuffleId = handle.shuffleId
    if (fallbackPolicy.shuffleHasFallenBack(shuffleId)) {
      false
    } else if (fallbackPolicy.killSwitchEngaged) {
      false
    } else if (!fallbackPolicy.checkProtocolVersion(handle.protocolVersion)) {
      requireConfirmedFallback(handle, StreamingShuffleFallbackReason.ProtocolVersionMismatch,
        s"this executor cannot speak the protocol version ${handle.protocolVersion} that " +
          s"shuffle $shuffleId was registered under")
      false
    } else if (fallbackPolicy.hasTripped) {
      // The condition the policy actually latched, never a stand-in for it: a trip always carries
      // its cause, so substituting a default here could only ever record a condition that was not
      // the one observed. A trip whose cause is somehow absent still declines the task -- the
      // verdict is what matters to this decision -- it simply declares nothing it cannot name.
      // The declaration it does make has to be CONFIRMED before any delegation follows it, so it
      // routes through the confirming form rather than declaring and hoping.
      fallbackPolicy.trippedReason.foreach { reason =>
        requireConfirmedFallback(handle, reason,
          "the executor stood streaming down before it served this task")
      }
      false
    } else {
      val declared = guardOption(s"read the fallback state of shuffle $shuffleId") {
        gatewayFor(handle).fallbackState(shuffleId)
      }
      declared match {
        case Some(state) if state.fallenBack =>
          fallbackPolicy.observeShuffleFallback(shuffleId, state)
          false
        case _ => true
      }
    }
  }

  /**
   * Turns a locally observed trip into a '''confirmed''' shuffle-wide verdict, or fails this task.
   *
   * The distinction is why this method exists rather than a bare declaration. Answering
   * "do not stream" on the strength of a local observation alone would have this executor's tasks
   * written and read by the sort-based delegate while every other executor kept streaming the same
   * shuffle -- one shuffle with two producers of the same output and two incompatible reduce-side
   * read paths, which is the split brain the fallback protocol exists to prevent. Delegation is
   * therefore legitimate only once the coordinator has latched the verdict for every participant
   * and withdrawn the streamed map output that the sort-based path is about to replace.
   *
   * When the declaration cannot be made -- an unreachable driver, a refused token, an output that
   * could not be withdrawn -- there is no safe local answer, so this raises. Failing is the
   * conservative outcome: the scheduler retries the task, the retry re-declares, and until
   * one succeeds every participant is still streaming one implementation of one shuffle. Silently
   * delegating instead would trade a retry for corrupt output.
   *
   * @param handle the shuffle whose verdict is being established
   * @param reason the condition observed on this executor
   * @param detail what was observed, for the operator-facing record
   * @throws SparkException when the verdict could not be established for every participant
   */
  private def requireConfirmedFallback(
      handle: StreamingShuffleHandle[_, _, _],
      reason: StreamingShuffleFallbackReason,
      detail: String): Unit = {
    val state = declareFallbackFor(handle, reason, detail)
    if (!state.fallenBack) {
      throw new SparkException(
        s"Streaming shuffle ${handle.shuffleId} observed ${reason.description} on this executor " +
          s"($detail) but the shuffle could not be stood down for every participant, so this " +
          "task cannot be served by the sort-based shuffle without risking two implementations " +
          "of one shuffle. Failing the task so that it is retried; the retry re-attempts the " +
          "declaration.")
    }
  }

  /** Registers a shuffle with the driver's coordinator, or explains why it could not be. */
  private def grantFor(
      shuffleId: Int,
      numPartitions: Int,
      numMaps: Int): Option[StreamingShuffleRegistrationGrant] = {
    val granted = driverCoordinator.flatMap { coordinator =>
      guardOption(s"register shuffle $shuffleId with the coordinator") {
        coordinator.registerShuffle(shuffleId, numPartitions, numMaps,
          StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)
      }.flatten
    }
    granted.filter { grant =>
      val usable = !grant.fallback.fallenBack
      if (!usable) {
        fallbackPolicy.observeShuffleFallback(shuffleId, grant.fallback)
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} was registered while the " +
          log"shuffle was already stood down for " +
          log"${MDC(REASON, grant.fallback.reasonName)}; using the sort-based shuffle")
      }
      usable
    }
  }

  /**
   * The driver-facing gateway for one shuffle, built per call because it holds nothing but that
   * shuffle's capability token, the endpoint reference and the ask deadline.
   *
   * The token is taken from the handle rather than from this manager's registration map. The map is
   * populated by `registerShuffle`, which runs on the driver only, so an executor's copy of it is
   * always empty; a gateway built from it would present no token and be refused by every check the
   * coordinator applies. The handle is the trusted channel the token actually arrives on.
   */
  private def gatewayFor(
      handle: StreamingShuffleHandle[_, _, _]): StreamingShuffleCoordinatorGateway = {
    new RpcStreamingShuffleCoordinatorGateway(coordinatorRef, handle.capabilityToken,
      coordinatorTimeout)
  }

  /**
   * Stands one shuffle down for every participant, and caches the verdict the coordinator latched.
   *
   * A declaration already latched is not repeated: the cached verdict is the same answer the
   * coordinator would give, so the wire cost of a fallback stays at one ask per shuffle per
   * executor however many tasks observe the condition.
   *
   * Failure is absorbed rather than propagated, and the verdict is '''returned''' rather than
   * assumed, because those are two halves of one contract. A caller is about to delegate to the
   * sort based path, and it may only do so once the transition is shuffle-wide; an unreachable or
   * refusing driver therefore has to be visible to it as a declaration that did not take effect,
   * not as an exception and not as a silent success.
   *
   * @return the verdict in force after the declaration; `fallenBack` is false when it could not be
   *         made, in which case the caller must not delegate
   */
  private def declareFallbackFor(
      handle: StreamingShuffleHandle[_, _, _],
      cause: StreamingShuffleStandDownCause,
      detail: String): StreamingShuffleFallbackState = {
    standDown(handle, detail) { (gateway, shuffleId) =>
      gateway.declareFallback(shuffleId, cause, detail)
    }
  }

  /**
   * Stands a shuffle down for every participant '''without naming a fallback condition''', for a
   * request the streaming read or write path structurally cannot serve.
   *
   * The consequences are identical to a fallback declaration -- the coordinator withdraws the
   * shuffle's streamed map output, so a consumer raises an ordinary fetch failure, the unmodified
   * scheduler recomputes the map stage, and the sort-based delegate serves the new attempts -- and
   * only the recorded verdict differs. No condition is asserted, because none was observed. Using a
   * member of the closed four-condition set here instead would report a measurement nobody took.
   *
   * @param handle the shuffle to withdraw from streaming
   * @param detail what was asked for and why streaming declined it
   * @return the verdict in force after the withdrawal; `fallenBack` is false when it could not be
   *         made, in which case the caller must not delegate
   */
  private def withdrawStreamingFor(
      handle: StreamingShuffleHandle[_, _, _],
      detail: String): StreamingShuffleFallbackState = {
    standDown(handle, detail) { (gateway, shuffleId) =>
      gateway.withdrawStreaming(shuffleId, detail)
    }
  }

  /**
   * The shared body of [[declareFallbackFor]] and [[withdrawStreamingFor]].
   *
   * Shared because everything except the verdict the gateway is asked to record is identical: the
   * already-latched short circuit, the absorbed transport failure, the report of a declaration that
   * did not take effect, and the release of this executor's share of the egress allowance.
   *
   * @param handle the shuffle being stood down
   * @param detail what was observed or asked for, used only in the unconfirmed-declaration record
   * @param declare sends the verdict to the driver and answers with the state in force
   */
  private def standDown(
      handle: StreamingShuffleHandle[_, _, _],
      detail: String)(
      declare: (StreamingShuffleCoordinatorGateway, Int) => StreamingShuffleFallbackState)
    : StreamingShuffleFallbackState = {
    val shuffleId = handle.shuffleId
    fallbackPolicy.knownShuffleFallback(shuffleId) match {
      case Some(latched) => latched
      case None =>
        val state = guardOption(s"stand shuffle $shuffleId down for every participant") {
          declare(gatewayFor(handle), shuffleId)
        }.getOrElse(StreamingShuffleFallbackState())
        // Only the unconfirmed case is reported here; a confirmed one is reported by the policy as
        // it records the verdict, and logging both would say the same thing twice per shuffle.
        if (!fallbackPolicy.observeShuffleFallback(shuffleId, state) && !state.fallenBack) {
          logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could not be stood down " +
            log"for every participant after ${MDC(REASON, detail)}; the shuffle keeps streaming " +
            log"and the local participant fails so that it is retried rather than served by a " +
            log"second implementation")
        }
        // A confirmed stand-down means no further participant of this shuffle will stream on this
        // executor, so its share of the egress allowance goes back to the shuffles that are still
        // using the link -- but only once no stream of it remains registered here. A producer
        // draining its unacknowledged window is still occupying that link, and dropping its ledgers
        // would leave those bytes unpaced and unaccounted for; a share held by a draining producer
        // is returned by `unregisterShuffle` when the shuffle itself ends.
        if (state.fallenBack && backpressureBuilt && backpressure.streamCount(shuffleId) == 0) {
          releaseShuffleStreamingState(shuffleId, "the shuffle stood streaming down")
        }
        state
    }
  }

  // Task-scoped construction.

  /**
   * Builds the producer half of one map task, falling back if the coordinator declines it.
   *
   * The resolver is matched rather than asserted upon. It is present exactly when streaming is
   * enabled, and [[streamingAvailableFor]] answers `true` only when the kill switch is disengaged
   * -- that is, only when streaming is enabled -- so the absent case is unreachable. It is
   * nonetheless written out, because a writer without a resolver could not retain its output at
   * all, and delegating is a strictly safer answer to an unreachable state than an assertion that
   * fails a task. No fallback is declared on that branch and none is needed: it cannot be reached,
   * and the closed set of conditions has no member for "this executor holds no retained-output
   * owner".
   */
  private def newStreamingWriter[K, V, C](
      handle: StreamingShuffleHandle[K, V, C],
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    streamingBlockResolver match {
      case Some(resolver) => streamingWriterOn(resolver, handle, mapId, context, metrics)
      case None => sortShuffleManager.getWriter(handle, mapId, context, metrics)
    }
  }

  /**
   * Builds the producer half against the executor's retained-output owner, transactionally.
   *
   * Two registrations are published before the writer exists, and both are visible to owners
   * outside this method: the routing entry the executor's listener holds, and the producer address
   * the driver's coordinator publishes. Everything after them can still refuse -- the announcement
   * can be declined, the buffer budget reaches through `SparkEnv`, and the writer's own
   * construction derives sizes from it -- so either the writer is returned or every registration
   * made on the way to it is unwound. A partial registration left behind is not inert: it is a
   * producer a consumer can resolve, connect to and then wait on until its own detector fires.
   *
   * The unwind is driven through [[StreamingShuffleServerHandler.withdrawGeneration]] rather than
   * by removing the routing entry here, so exactly one piece of code knows how to retire a
   * generation and it retires every owner of it -- routing, retained output, spill files and
   * sessions -- rather than the subset this method happened to create. The handler is asked
   * directly rather than looked up by key, because withdrawing '''this''' handler is what is
   * wanted: under `spark.shuffle.useOldFetchProtocol` a map id is the partition index instead of
   * the task attempt id, so a lookup can find a different attempt's handler under the same key.
   *
   * '''One refusal is not answered here, and deliberately.''' A writer that is constructed and then
   * finds at its own preflight that it cannot stream -- an incompatible protocol version, a verdict
   * already latched, a budget that cannot frame one block, a framing reservation that cannot be met
   * -- hands the whole attempt to the sort-based writer instead of failing it. That decision
   * belongs to the writer because only it knows it has not yet consumed a record.
   *
   * <b>The writer inherits an obligation, and it is an explicit one.</b> The three registrations
   * made here -- the egress share, the routing entry and the driver's producer address -- outlive
   * this method, so the writer that is returned owns them. Every condition that can be decided
   * without a writer is decided before them, in [[streamingAvailableFor]]; the ones that cannot are
   * decided by the writer's own preflight, which runs before its first record and may still stand
   * this attempt down. When it does, it is required to hand all three back through
   * [[StreamingShuffleWriter.withdrawPrePublishedOwnership]] before it assigns its sort delegate --
   * because from that instant the writer forwards its whole contract, `stop` included, to the
   * delegate, and anything still published then is published for the life of the executor with
   * nothing left that would retire it. The invariant this method establishes therefore holds across
   * that path too: either a streaming producer is reachable or no registration for it exists, and
   * never both a reachable address and a sort-based writer producing the output that address would
   * serve.
   */
  private def streamingWriterOn[K, V, C](
      resolver: StreamingShuffleBlockResolver,
      handle: StreamingShuffleHandle[K, V, C],
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val shuffleId = handle.shuffleId
    val taskAttemptId = context.taskAttemptId()
    val notifier = new StreamingShuffleErrorNotifier(shuffleId, conf)
    val gateway = gatewayFor(handle)
    // Bound before the handler is built, because the handler takes the routing table as the one
    // owner of a generation it cannot otherwise reach: its own withdrawal retires the routing
    // entry, the retained output and its sessions together, in that order.
    val (listener, _, server) = streamingListener
    // This shuffle's own share of the executor's egress allowance, never a limiter shared with
    // another shuffle. Resolving it here does two things at once: it admits this shuffle to the
    // divisor and it republishes every other live shuffle's share, so the first block this map task
    // frames is already paced at a rate that accounts for it.
    val limiter = egressBudget.limiterFor(shuffleId)
    // The partition count travels from the handle, which is the shuffle-wide registration every
    // participant shares, so the handler's partition domain is fixed before it can receive a frame
    // and is the same figure on every executor.
    val serverHandler = new StreamingShuffleServerHandler(conf, shuffleId, mapId, taskAttemptId,
      handle.numPartitions, resolver, listener, backpressure, limiter, notifier, fallbackPolicy)
    // Registered before the producer is announced, so the executor's listener can route a frame the
    // instant a consumer acts on the address. Deliberately NOT released on task completion: a
    // successful map task's output is precisely the output no consumer has read yet, and the
    // listener is the only way to reach it. Release happens when the output does, and always
    // through the one operation that retires every owner at once --
    // [[StreamingShuffleServerHandler.withdrawGeneration]] -- whether it is reached from a failed
    // task's stop, from a generation the driver retired, from unregisterShuffle or from this
    // manager's own shutdown.
    listener.register(shuffleId, mapId, serverHandler)
    val announcement = announceProducer(handle, mapId, context, server)
    announcement.filter(_.accepted) match {
      case Some(registration) =>
        try {
          applyConcurrency(registration.numConcurrentShuffles)
          val components = StreamingShuffleWriterComponents(
            backpressure = backpressure,
            rateLimiter = limiter,
            spillManager = new MemorySpillManager(context.taskMemoryManager(), conf),
            blockResolver = resolver,
            serverHandler = serverHandler,
            fallbackPolicy = fallbackPolicy,
            errorNotifier = notifier,
            coordinatorGateway = gateway)
          // The sort-based writer is supplied as a factory rather than built here, so it is
          // constructed only when the streaming writer degrades before consuming a record -- a
          // framing reservation that cannot be met, no viable framing envelope at all, a protocol
          // version the peer cannot speak, or a fallback verdict already taken for this shuffle.
          // Delegating at that point loses nothing, because nothing has been produced. Building it
          // eagerly would instead register a map task with the delegate's own bookkeeping for every
          // streaming shuffle that never uses it.
          //
          // The coexistence contract that goes with the factory. Both registrations made above are
          // still in force when the writer decides, so a writer that reached for this factory
          // while leaving them in place would leave this executor holding a routing entry and the
          // driver holding an address for a generation that has handed its work to the sort-based
          // writer -- one such orphan per delegated map output, alive until the shuffle was
          // unregistered, and each of them a producer a consumer can resolve, connect to and then
          // wait on until its own detector fires. The writer is therefore obliged to retire this
          // generation through `StreamingShuffleServerHandler.withdrawGeneration` -- the single
          // cross-owner withdrawal, reached the same way this method's own unwind reaches it --
          // before it invokes this factory, and `StreamingShuffleWriter.degradeToSortShuffle`
          // does exactly that.
          new StreamingShuffleWriter[K, V, C](handle, mapId, context, metrics, conf, components,
            () => sortShuffleManager.getWriter[K, V](handle, mapId, context, metrics))
        } catch {
          case NonFatal(e) =>
            withdrawAnnouncedProducer(gateway, serverHandler, handle, mapId, context,
              "its producer half could not be constructed")
            throw e
        }
      case None =>
        declinedProducerWriter(announcement, serverHandler, handle, mapId, context, metrics)
    }
  }

  /**
   * Answers a producer announcement the coordinator would not publish.
   *
   * Nothing streamed and nothing was published on the driver, so only this executor's own routing
   * entry has to be unwound; it is withdrawn through the single operation for the reason given on
   * [[streamingWriterOn]].
   *
   * What is then owed is a shuffle that still works, and there are two ways to owe it depending on
   * what the refusal meant:
   *
   *  - a '''stale generation''' -- superseded by a later attempt, or retired -- is one zombie task,
   *    not an unavailable shuffle. The generation that owns the map index is streaming perfectly
   *    well, so standing the shuffle down would abandon streaming everywhere because a superseded
   *    attempt happened to still be running. This attempt therefore fails, and the live one carries
   *    on.
   *  - '''anything else''' says streaming is not available for this shuffle: it has already stood
   *    down, its registration is gone, it is at its producer ceiling, or the announcement did not
   *    reach the driver at all. Delegating this one map task before that is agreed is the split
   *    this finding is about -- one map output written by the sort-based writer while its siblings
   *    stream, which no reduce-side read path can reassemble -- so the transition is confirmed with
   *    the coordinator first, and the delegate is returned only if the confirmation took effect. A
   *    confirmation that could not be made leaves the shuffle exactly as it was and fails this
   *    attempt, which is a retry rather than a contradiction.
   */
  private def declinedProducerWriter[K, V, C](
      announcement: Option[StreamingShuffleProducerRegistration],
      serverHandler: StreamingShuffleServerHandler,
      handle: StreamingShuffleHandle[K, V, C],
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val shuffleId = handle.shuffleId
    guard(s"retire the unpublished producer of shuffle $shuffleId map $mapId") {
      serverHandler.withdrawGeneration("the producer address could not be published")
    }
    if (announcement.exists(_.generationStale)) {
      throw new SparkException(s"Streaming shuffle $shuffleId will not publish map $mapId " +
        s"attempt ${context.taskAttemptId()}, because that generation no longer owns its map " +
        "index: it has been superseded or retired. The attempt that owns it continues to " +
        "stream, so this one fails rather than standing the shuffle down.")
    }
    // A producer whose address cannot be published can never be resolved by a consumer, so the two
    // ends of this shuffle have no rendezvous. That is a structural decline and it is declared as
    // one: none of the four specified fallback conditions was observed here, and reporting one of
    // them -- this was previously declared as a consumer held at 2x behind for a minute -- would
    // send an operator to tune a throughput measurement that this path never takes. The reader
    // declares the same cause when it cannot resolve a producer, which is the same failure seen
    // from the other end.
    val latched = declareFallbackFor(handle, StreamingShuffleStandDownCause.ProducerUnavailable,
      s"the producer address of map $mapId could not be published")
    if (!latched.fallenBack) {
      throw new SparkException(s"Streaming shuffle $shuffleId could not publish the producer " +
        s"address of map $mapId and could not stand the shuffle down for every participant, so " +
        "this attempt fails and is retried rather than being written by a second shuffle " +
        "implementation while the rest of the shuffle keeps streaming.")
    }
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
      log"${MDC(MAP_ID, mapId)} could not publish its producer address; the shuffle has stood " +
      log"streaming down for every participant and the sort-based shuffle writes this map output")
    sortShuffleManager.getWriter(handle, mapId, context, metrics)
  }

  /**
   * Unwinds a producer that was announced and then could not be built.
   *
   * The driver is told first, and in that order for the same reason a failing writer tells it
   * first: it owns the address a consumer resolves and is the one owner this executor cannot retire
   * for itself, so withdrawing it before the local owners go means no consumer is handed an address
   * that is about to refuse it. Each step is contained, because this runs while a failure is
   * already being propagated and must not replace it.
   */
  private def withdrawAnnouncedProducer(
      gateway: StreamingShuffleCoordinatorGateway,
      serverHandler: StreamingShuffleServerHandler,
      handle: StreamingShuffleHandle[_, _, _],
      mapId: Long,
      context: TaskContext,
      reason: String): Unit = {
    val shuffleId = handle.shuffleId
    guard(s"withdraw the announced producer of shuffle $shuffleId map $mapId") {
      gateway.invalidateProducer(shuffleId,
        StreamingShuffleProducerGeneration(context.partitionId(), mapId, context.taskAttemptId()),
        StreamingShuffleInvalidationReason.IncompleteStream, reason)
    }
    guard(s"retire the local owners of shuffle $shuffleId map $mapId") {
      serverHandler.withdrawGeneration(reason)
    }
  }

  /**
   * Publishes one producer's address so that consumers can find it.
   *
   * The address has two halves that are emphatically not interchangeable, and both are published.
   * `host`/`port` address the ephemeral streaming listener this map task has just bound, which is
   * where blocks are streamed from. `blockManagerId` is the identity this map output's `MapStatus`
   * carries, which is what `MapOutputTracker` matches when a fetch failure asks it to remove the
   * output; an identity synthesised from the streaming listener would match nothing and would
   * silently defeat every recomputation the subsystem depends upon.
   *
   * The reply is handed back whole rather than filtered on acceptance, because a refusal is not one
   * outcome but two: the caller has to tell a superseded generation, which must fail, from a
   * shuffle that cannot be streamed, which must stand down. `None` means the ask did not reach the
   * driver, which the caller treats as the second of those.
   *
   * @return the coordinator's reply, or `None` when the driver could not be reached
   */
  private def announceProducer(
      handle: StreamingShuffleHandle[_, _, _],
      mapId: Long,
      context: TaskContext,
      server: StreamingShuffleListener.BoundServer):
      Option[StreamingShuffleProducerRegistration] = {
    val env = SparkEnv.get
    val blockManagerId = env.blockManager.shuffleServerId
    val location = StreamingShuffleProducerLocation(
      executorId = env.executorId,
      host = blockManagerId.host,
      port = server.getPort,
      mapId = mapId,
      mapIndex = context.partitionId(),
      taskAttemptId = context.taskAttemptId(),
      blockManagerId = blockManagerId)
    val request = RegisterStreamingShuffleProducer(handle.shuffleId, handle.capabilityToken,
      location, handle.numPartitions, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
      clock.getTimeMillis())
    val registration = guardOption(s"announce a producer of shuffle ${handle.shuffleId}") {
      coordinatorRef.askSync[StreamingShuffleProducerRegistration](request, coordinatorTimeout)
    }
    registration.foreach { reply =>
      if (reply.accepted && debugEnabled) {
        logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, handle.shuffleId)} published producer " +
          log"${MDC(EXECUTOR_ID, env.executorId)} map ${MDC(MAP_ID, mapId)} attempt " +
          log"${MDC(TASK_ATTEMPT_ID, context.taskAttemptId())} at " +
          log"${MDC(HOST_PORT, location.hostPort)}")
      }
    }
    registration
  }

  /**
   * Feeds the coordinator's count of this executor's concurrent shuffles to the egress budget,
   * which republishes every live limiter's share if the count moves the divisor.
   *
   * The per-shuffle rate is the administered link capacity divided by that count and held to 80% of
   * it, so the count is the one input that must move every rate at once. Handing the number to the
   * budget rather than computing a rate here is what keeps that arithmetic in one place: the budget
   * owns the divisor, the ceiling and the redistribution, and this method owns only the
   * observation.
   *
   * Called on a producer registration reply, which carries the count, so the ordinary path costs no
   * extra round trip.
   *
   * @param numConcurrentShuffles the count the coordinator reported; a non-positive value is
   *                              ignored by the budget rather than treated as a divisor
   */
  private def applyConcurrency(numConcurrentShuffles: Int): Unit = {
    if (egressBudget.observeConcurrency(numConcurrentShuffles) && debugEnabled) {
      val share = egressBudget.currentShareBytesPerSecond
      logDebug(log"Streaming shuffle egress is divided " +
        log"${MDC(COUNT, egressBudget.divisor)} way(s) at " +
        log"${MDC(NUM_BYTES, share.map(_.toString).getOrElse("unlimited"))} byte(s)/s per " +
        log"shuffle after the coordinator reported " +
        log"${MDC(VALUE, numConcurrentShuffles)} concurrent shuffle(s) on this executor")
    }
  }

  /**
   * How many streaming shuffles this executor is currently producing for, or `None` when the answer
   * cannot be had.
   *
   * Handed to the egress budget as a closure, so the budget never touches `RpcEnv` and the question
   * can be answered without one. Two routes, because this class is both the driver's
   * manager and every executor's: the driver reads its own coordinator instance, and an executor
   * asks the endpoint. The request names no shuffle, so no shuffle's capability token authorises it
   * and none is presented; the coordinator validates the executor id itself and answers the safe
   * divisor of one for an id it cannot read.
   *
   * `None` leaves the divisor exactly as it was, which is the conservative outcome. The divisor is
   * the larger of this answer and the executor's own limiter count, so a missing answer can only
   * leave the pacing where the local count already puts it -- never above the cap.
   */
  private def executorConcurrency(): Option[Int] = {
    if (stopped.get()) {
      // A shutdown returns every share it holds, and asking once per release would spend a round
      // trip -- and log a warning once the driver has gone -- on a divisor that will never pace
      // another byte.
      None
    } else {
      guardOption("read this executor's streaming shuffle concurrency") {
        val executorId = SparkEnv.get.executorId
        driverCoordinator match {
          case Some(coordinator) => coordinator.numConcurrentShufflesFor(executorId)
          case None =>
            coordinatorRef.askSync[Int](GetStreamingShuffleConcurrency(executorId),
              coordinatorTimeout)
        }
      }
    }
  }

  /**
   * Returns the executor-scoped streaming state one shuffle holds: its credit ledgers, its
   * contribution to the aggregate utilisation reading, and its share of the executor's egress
   * allowance.
   *
   * The share is the half that is easy to overlook and the half that compounds. The per-shuffle
   * token rate is the administered cap divided by the number of shuffles this executor is producing
   * for, so a shuffle that has ended while still holding a limiter keeps that divisor too high and
   * paces every shuffle outliving it below its true share -- permanently, because nothing else ever
   * revisits the division. Ledgers left behind are the same defect in the other currency: they keep
   * a finished shuffle in the arbitration order and in the utilisation numerator forever.
   *
   * Contained rather than propagated, because every caller is either finishing a shuffle or
   * shutting down and neither has a recovery for a failed bookkeeping step that is better than
   * proceeding.
   *
   * @param shuffleId shuffle whose executor-scoped state is being returned
   * @param occasion what prompted the release, for the diagnostic
   */
  private def releaseShuffleStreamingState(shuffleId: Int, occasion: String): Unit = {
    guard(s"return the executor's streaming state for shuffle $shuffleId") {
      val dropped = backpressure.unregisterShuffle(shuffleId)
      if (debugEnabled) {
        logDebug(log"Streaming shuffle returned the executor state of shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} because ${MDC(REASON, occasion)}, dropping " +
          log"${MDC(COUNT, dropped)} stream ledger(s); egress is now divided " +
          log"${MDC(VALUE, egressBudget.divisor)} way(s)")
      }
    }
  }

  /**
   * Releases the executor's streaming listener, if one was ever bound.
   *
   * Guarded by the flag rather than by touching the lazy value, because dereferencing it here would
   * bind a port for the sole purpose of closing it. Every producer handler is released first, so no
   * consumer is left holding a session against a pipeline that is about to go away.
   */
  private def closeStreamingListener(): Unit = {
    if (listenerBound) {
      val (listener, transportContext, server) = streamingListener
      guard("release the executor's streaming producer handlers")(listener.releaseAll())
      guard("close the executor's streaming listener")(server.close())
      guard("close the executor's streaming transport context")(transportContext.close())
    }
  }

  // Failure containment.

  /**
   * Runs one shutdown or bookkeeping step, logging any non-fatal failure instead of propagating it.
   *
   * Every caller is either releasing a resource or recording a decision, and in both cases an
   * exception would replace a recoverable situation with an unrecoverable one -- an abandoned
   * shutdown leaks, and a failed bookkeeping step fails a task that had already succeeded. A fatal
   * `Error` is deliberately allowed to propagate.
   */
  private def guard(operation: String)(body: => Unit): Unit = {
    try {
      body
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle could not ${MDC(REASON, operation)}", e)
    }
  }

  /** As [[guard]], but reporting the value the step produced, or `None` when it failed. */
  private def guardOption[T](operation: String)(body: => T): Option[T] = {
    try {
      Some(body)
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle could not ${MDC(REASON, operation)}", e)
        None
    }
  }
}

/**
 * Serves a local shuffle block from whichever resolver owns it.
 *
 * <b>Why a router is needed at all.</b> A streaming-enabled application does not stream every
 * shuffle. A dependency that asks for map-side combining is written by the sort-based writer, as
 * is every shuffle written after streaming has stood down. Those blocks are requested from the
 * manager's resolver like any others, so a manager that exposed only the streaming resolver would
 * refuse to serve output it had itself arranged to be written.
 *
 * <b>How ownership is decided.</b> By asking the streaming resolver whether it owns the producer of
 * the block, never by catching a failure from it. A producer registers with that resolver exactly
 * when it retains streamed output, so the question has a definite answer for every shuffle block
 * identity, and asking it costs one lookup under a monitor that is never held while disk or network
 * is touched. Temporary shuffle block identities are streaming's own -- they name spill files this
 * subsystem created -- so they are routed to the streaming resolver without a producer lookup.
 *
 * <b>What it deliberately is not: an index resolver.</b> `ShuffleWriteProcessor` selects
 * push-based merge by pattern matching on the manager's resolver type, so a router that presented
 * itself as an `IndexShuffleBlockResolver` would switch push-based merge back on for streamed
 * writes and then push a data file that streaming never materialises. Declining to be one is the
 * specified posture -- streaming neither extends nor interoperates with push-based merge, it
 * declines to participate -- and it is why the sort delegate's own resolver is not simply published
 * in place of this router. Three unchanged behaviours are consequently unavailable to the
 * <i>whole</i> application while streaming is enabled, and every one of them degrades rather than
 * failing: push-based merge declines for every shuffle; `FallbackStorage` logs an
 * unsupported-resolver warning and copies nothing to fallback storage; and checksum-based
 * corruption <i>diagnosis</i> answers `UNKNOWN_ISSUE`, which the fetch side already handles because
 * it treats diagnosis as best effort. Ordinary shuffle reads and ordinary shuffle writes are
 * unaffected.
 *
 * <b>What it deliberately is not, either: a migration resolver.</b> Streamed output is not
 * migratable. It is a live retransmission window bound to the executor that produced it, addressed
 * by no index and no data file, so no other executor could serve it and no decommissioning
 * executor could hand it over. Presenting a `MigratableResolver` would therefore advertise a
 * capability the streaming path does not have, and `BlockManagerDecommissioner` documents its own
 * requirement as "an Indexed based shuffle resolver" rather than merely a migratable one -- which
 * this router is deliberately not, for the reason given above. Declining the mixin is consequently
 * the honest posture, and it is safe because every caller that reaches for migration support
 * degrades rather than failing a job:
 *
 *  - `BlockManager.migratableResolver` is a `lazy val`, so nothing forces the cast until migration
 *    is actually attempted, which requires `spark.decommission.enabled` -- off by default.
 *  - `BlockManagerDecommissioner`'s shuffle-migration thread wraps its refresh in a `NonFatal`
 *    catch that logs the error and stops shuffle migration, leaving RDD-block migration and the
 *    job itself untouched.
 *  - `BlockManager.putBlockDataAsStream` already catches the `ClassCastException` and raises the
 *    existing typed `unexpectedShuffleBlockWithUnsupportedResolverError`, which names the manager
 *    and the block instead of surfacing a raw cast failure.
 *  - `FallbackStorage.copy` is only ever reached after a successful `getStoredShuffles`, so it is
 *    unreachable once the migration thread has stood down.
 *
 * Three unchanged behaviours are therefore unavailable to a streaming-enabled application, and each
 * of them degrades: push-based merge declines, fallback storage copies nothing, and decommission
 * shuffle-block migration stops with a logged error. All three are restored the moment streaming is
 * gated off, because the sort delegate's own `IndexShuffleBlockResolver` -- which is an
 * `IndexShuffleBlockResolver` and hence a `MigratableResolver` -- is then published unchanged.
 *
 * @param streaming the streaming resolver, which owns retained streamed output
 * @param sorted the sort-based delegate's index resolver, which owns materialised sorted output
 */
private[spark] class StreamingShuffleBlockRouter(
    streaming: StreamingShuffleBlockResolver,
    sorted: ShuffleBlockResolver)
  extends ShuffleBlockResolver with Logging {

  /**
   * The streaming resolver this router delegates streamed blocks to.
   *
   * Exposed for diagnostics and tests. The manager's `shuffleBlockResolver` is typed as the shared
   * `ShuffleBlockResolver` contract, so a caller holding the manager cannot otherwise reach the
   * streaming registry's own readings -- how many producers are registered, and how many spill
   * files have outlived their producing task -- which is what a test proving allocation and release
   * needs.
   */
  def streamingResolver: StreamingShuffleBlockResolver = streaming

  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    resolverFor(blockId).getBlockData(blockId, dirs)
  }

  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    val streamed = streaming.getBlocksForShuffle(shuffleId, mapId)
    if (streamed.nonEmpty) streamed else sorted.getBlocksForShuffle(shuffleId, mapId)
  }

  /**
   * Merged blocks are never streaming's. Push-based merge does not run while this router is the
   * manager's resolver, so any request that arrives belongs to output written before streaming was
   * enabled and is answered by the delegate.
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    sorted.getMergedBlockData(blockId, dirs)
  }

  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    sorted.getMergedBlockMeta(blockId, dirs)
  }

  /**
   * Stops both resolvers. The streaming one goes first, because only it holds leases on
   * files it must unlink, and neither failure is allowed to prevent the other from being released.
   */
  override def stop(): Unit = {
    try {
      streaming.stop()
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle could not stop its block resolver", e)
    } finally {
      sorted.stop()
    }
  }

  /**
   * The resolver that owns a block identity, decided by asking rather than by failing.
   *
   * The default arm matters as much as the specific ones. Index, data, checksum, push and
   * merged-chunk identities are all sort-based artefacts that the streaming resolver never
   * allocated; sending one there would produce a refusal that reads to an operator as a corrupted
   * shuffle, in place of the delegate's own accurate answer for an identity it does understand.
   */
  private def resolverFor(blockId: BlockId): ShuffleBlockResolver = blockId match {
    case ShuffleBlockId(shuffleId, mapId, _) => routeByProducer(shuffleId, mapId)
    case ShuffleBlockBatchId(shuffleId, mapId, _, _) => routeByProducer(shuffleId, mapId)
    // A spill file asked for by name is streaming's alone: only the streaming resolver allocates
    // temporary shuffle blocks for retained output, and the delegate holds no record of one.
    case _: TempShuffleBlockId => streaming
    case _ => sorted
  }

  private def routeByProducer(shuffleId: Int, mapId: Long): ShuffleBlockResolver = {
    if (streaming.producerFor(shuffleId, mapId).isDefined) streaming else sorted
  }

  override def toString: String =
    s"StreamingShuffleBlockRouter(streaming=$streaming, sorted=$sorted)"
}

/**
 * Constants of the streaming shuffle manager.
 */
private[spark] object StreamingShuffleManager {

  /**
   * The value of `spark.shuffle.manager` that selects this manager.
   *
   * Declared here, next to the class it names, and referenced by the short-name table in
   * `ShuffleManager` rather than repeated as a literal there, so that the selector an operator
   * writes and the class it resolves to cannot drift apart.
   */
  val SHORT_NAME: String = "streaming"

  /**
   * Floor on the coordinator ask deadline. A deadline shorter than a second would make an ordinary
   * driver pause look like an unreachable driver, and every one of these asks is on a path that
   * recovers by failing a task.
   */
  val MIN_COORDINATOR_TIMEOUT_MS: Long = 1000L

}
