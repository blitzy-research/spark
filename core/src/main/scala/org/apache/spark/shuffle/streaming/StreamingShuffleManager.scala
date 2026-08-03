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
import org.apache.spark.internal.LogKeys.{CLASS_NAME, CONFIG, COUNT, EXECUTOR_ID, HOST_PORT,
  MAP_ID, NUM_BYTES, NUM_PARTITIONS, NUM_TASKS, REASON, SHUFFLE_ID, TASK_ATTEMPT_ID, VALUE}
import org.apache.spark.internal.config.{SHUFFLE_STREAMING_DEBUG, SHUFFLE_STREAMING_ENABLED}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.server.TransportServer
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.network.shuffle.protocol.streaming.StreamingShuffleMessage
import org.apache.spark.rpc.{RpcEndpointRef, RpcTimeout}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{MigratableResolver, ShuffleBlockInfo, ShuffleBlockResolver,
  ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter,
  ShuffleWriteMetricsReporter, ShuffleWriter}
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
 * ==Two-tier activation, and why there are two keys==
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
 * attached -- a reduce attempt reading while a superseded or speculative map attempt still produces
 * -- and would be exercised throughout under a scheduler that submitted consumers earlier. Both
 * halves of the subsystem are built for that case and take it whenever it arises; neither pretends
 * to create it.
 *
 * ==What is consulted before streaming is used==
 *
 * Three surfaces are consulted, because each knows something the others cannot.
 *
 *  - The driver's answer, carried on [[StreamingShuffleRegistrationGrant]] when a shuffle is
 *    registered and on the fallback state a lookup reports afterwards. It is the only
 *    shuffle-wide, cluster-wide verdict.
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
 * call. The one exception is the coordinator endpoint, which is registered eagerly on the
 * driver: an executor resolves it by name, and a name registered only on first use is a race
 * an executor can lose.
 *
 * ==Thread safety and shutdown==
 *
 * Every method is safe to call concurrently. Per-shuffle state is one concurrent map; the lazily
 * built components are initialized under Scala's own lazy-val latch. [[stop]] is idempotent: the
 * first caller wins a compare-and-set and closes what was actually created, in reverse dependency
 * order, containing and logging each non-fatal failure so one stubborn component cannot prevent
 * the rest from being released.
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

  private val streamingEnabled: Boolean = conf.get(SHUFFLE_STREAMING_ENABLED)
  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  // The delegate. Constructed unconditionally and first, so that delegation is available from the
  // moment this object exists -- including from the constructor of anything below it.

  private val sortShuffleManager: SortShuffleManager = new SortShuffleManager(conf)

  /** Executor-wide degradation policy: the kill switch plus every locally observed trip. */
  private val fallbackPolicy: StreamingShuffleFallbackPolicy =
    new StreamingShuffleFallbackPolicy(conf)

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
    if (isDriver && streamingEnabled) {
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
    if (streamingEnabled) Some(new StreamingShuffleBlockResolver(conf)) else None
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

  @volatile private var listenerBound: Boolean = false

  /**
   * The executor's one streaming shuffle listener, together with the transport it owns.
   *
   * One per executor, not one per map task, and the reason is twofold. A `TransportServer` always
   * creates its own boss and worker event loop groups, so a server per task would multiply threads
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
      (StreamingShuffleListener, TransportContext, TransportServer) = {
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
      log"${MDC(CONFIG, SHUFFLE_STREAMING_ENABLED.key)}=${MDC(VALUE, streamingEnabled)}")
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
            logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} registered with " +
              log"${MDC(NUM_PARTITIONS, numPartitions)} partition(s) across " +
              log"${MDC(NUM_TASKS, numMaps)} map task(s) at epoch " +
              log"${MDC(COUNT, grant.coordinatorEpoch)}")
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
        if (StreamingShuffleReader.servesFullMapRange(startMapIndex, endMapIndex)) {
          new StreamingShuffleReader[K, C](streaming, startMapIndex, endMapIndex, startPartition,
            endPartition, context, metrics, conf, readerContext)
        } else {
          // A narrowed map range cannot be streamed: a live producer location names a map id and a
          // task attempt id but no map index, so there is no way to serve "map indexes 3 to 7" from
          // producers that are still running. Declining is therefore forced. Standing the shuffle
          // down before delegating is what makes the delegation useful rather than merely legal: it
          // has the coordinator withdraw the streamed map output, so the sort-based reader below
          // reports a missing map output, the scheduler recomputes the map stage on the sort-based
          // path, and the narrowed read is served from materialised files.
          declareFallbackFor(streaming, StreamingShuffleFallbackReason.ConsumerTooSlow,
            s"a consumer requested the narrowed map range [$startMapIndex, $endMapIndex), which " +
              "streaming cannot serve")
          sortShuffleManager.getReader(handle, startMapIndex, endMapIndex, startPartition,
            endPartition, context, metrics)
        }
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
    }
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
    if (!streamingEnabled) {
      Some(s"${SHUFFLE_STREAMING_ENABLED.key} is false")
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
   */
  private def streamingAvailableFor(handle: StreamingShuffleHandle[_, _, _]): Boolean = {
    val shuffleId = handle.shuffleId
    if (fallbackPolicy.shuffleHasFallenBack(shuffleId)) {
      false
    } else if (fallbackPolicy.killSwitchEngaged) {
      false
    } else if (fallbackPolicy.hasTripped) {
      declareFallbackFor(handle,
        fallbackPolicy.trippedReason.getOrElse(StreamingShuffleFallbackReason.ConsumerTooSlow),
        "the executor stood streaming down before it served this task")
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
      reason: StreamingShuffleFallbackReason,
      detail: String): StreamingShuffleFallbackState = {
    val shuffleId = handle.shuffleId
    fallbackPolicy.knownShuffleFallback(shuffleId) match {
      case Some(latched) => latched
      case None =>
        val state = guardOption(s"stand shuffle $shuffleId down for every participant") {
          gatewayFor(handle).declareFallback(shuffleId, reason, detail)
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
    val serverHandler = new StreamingShuffleServerHandler(conf, shuffleId, mapId, taskAttemptId,
      resolver, listener, backpressure, limiter, notifier)
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
          // constructed only in the one case that needs it: a producer whose framing reservation
          // cannot be met, which degrades the whole attempt before its first record and therefore
          // loses nothing. Building it eagerly would register a map task with the delegate's own
          // bookkeeping for every streaming shuffle that never uses it.
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
    // The closed set of four conditions has no member for "the coordinator would not publish this
    // producer", and adding a fifth is forbidden by design, so the generic condition carries the
    // decision and the detail carries the truth -- which is the same convention the reader uses for
    // a rendezvous it cannot resolve.
    val latched = declareFallbackFor(handle, StreamingShuffleFallbackReason.ConsumerTooSlow,
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
      server: TransportServer): Option[StreamingShuffleProducerRegistration] = {
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
 * it treats diagnosis as best effort. Ordinary shuffle reads, ordinary shuffle writes and
 * peer-to-peer block migration are all unaffected.
 *
 * <b>What it deliberately is: a migration resolver.</b> `BlockManager` reaches migration support by
 * casting the manager's resolver to `MigratableResolver` without testing the type first, so a
 * router that did not mix it in would turn every executor decommission and every shuffle-block
 * stream upload into a `ClassCastException`. Every member is therefore forwarded to the sort
 * delegate, which is where all migratable output lives. Streamed output is never migratable -- it
 * is a live retransmission window bound to the executor that produced it, addressed by no index and
 * no data file -- so it is correctly invisible to the migrator rather than being offered and then
 * failing half way through a migration.
 *
 * @param streaming the streaming resolver, which owns retained streamed output
 * @param sorted the sort-based delegate's index resolver, which owns materialised sorted output
 */
private[spark] class StreamingShuffleBlockRouter(
    streaming: StreamingShuffleBlockResolver,
    sorted: ShuffleBlockResolver)
  extends ShuffleBlockResolver with MigratableResolver with Logging {

  /**
   * The delegate seen as a migration resolver, which is what every migration member forwards to.
   *
   * Derived by asking rather than by casting, so that a delegate which is not migratable degrades
   * to "nothing here is migratable" instead of failing at the first decommission. With
   * [[SortShuffleManager]] as the delegate the answer is always present, because its resolver is an
   * `IndexShuffleBlockResolver` and that class is itself a `MigratableResolver`.
   */
  private val migratable: Option[MigratableResolver] = sorted match {
    case resolver: MigratableResolver => Some(resolver)
    case _ => None
  }

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

  // MigratableResolver. Every member forwards to the sort delegate, because streamed output is not
  // migratable and must stay invisible to the migrator.

  /**
   * The shuffles stored locally in a form that can be migrated, which is exactly the sort-owned
   * ones. A streamed shuffle contributes nothing, and that is the correct answer rather than an
   * omission: its retained bytes are a retransmission window that only the producing executor can
   * serve, so listing it would have the decommissioner attempt a migration that cannot succeed.
   */
  override def getStoredShuffles(): Seq[ShuffleBlockInfo] = {
    migratable.map(resolver => resolver.getStoredShuffles()).getOrElse(Seq.empty)
  }

  /** Records that one shuffle must not be migrated, with the delegate that owns that decision. */
  override def addShuffleToSkip(shuffleId: Int): Unit = {
    migratable.foreach(resolver => resolver.addShuffleToSkip(shuffleId))
  }

  /**
   * Accepts a migrated shuffle block as a stream. The identities that arrive here are sort-based
   * artefacts -- an index or data block of a map output being moved off a decommissioning executor
   * -- so the delegate is the only component that can place them. A delegate that cannot accept
   * them is reported rather than papered over with a callback that would discard the bytes.
   */
  override def putShuffleBlockAsStream(
      blockId: BlockId,
      serializerManager: SerializerManager): StreamCallbackWithID = {
    migratable
      .map(resolver => resolver.putShuffleBlockAsStream(blockId, serializerManager))
      .getOrElse(throw new UnsupportedOperationException(
        s"Streaming shuffle cannot accept migrated block $blockId, because the sort-based " +
          s"delegate resolver ${sorted.getClass.getName} does not support block migration."))
  }

  /** The buffers of one map output being migrated, from the delegate that materialised them. */
  override def getMigrationBlocks(
      shuffleBlockInfo: ShuffleBlockInfo): List[(BlockId, ManagedBuffer)] = {
    migratable
      .map(resolver => resolver.getMigrationBlocks(shuffleBlockInfo))
      .getOrElse(List.empty)
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
