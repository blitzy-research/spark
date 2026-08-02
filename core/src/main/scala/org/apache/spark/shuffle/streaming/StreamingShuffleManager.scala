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

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CLASS_NAME, CONFIG, COUNT, EXECUTOR_ID, HOST_PORT,
  MAP_ID, NUM_BYTES, NUM_PARTITIONS, NUM_TASKS, REASON, SHUFFLE_ID, TASK_ATTEMPT_ID, VALUE}
import org.apache.spark.internal.config.{SHUFFLE_STREAMING_DEBUG, SHUFFLE_STREAMING_ENABLED}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.server.TransportServer
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.network.shuffle.protocol.streaming.StreamingShuffleMessage
import org.apache.spark.rpc.{RpcEndpointRef, RpcTimeout}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{ShuffleBlockResolver, ShuffleHandle, ShuffleManager,
  ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
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

  // ---------------------------------------------------------------------------------------------
  // Configuration, read exactly once.
  // ---------------------------------------------------------------------------------------------

  private val streamingEnabled: Boolean = conf.get(SHUFFLE_STREAMING_ENABLED)
  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  // ---------------------------------------------------------------------------------------------
  // The delegate. Constructed unconditionally and first, so that delegation is available from the
  // moment this object exists -- including from the constructor of anything below it.
  // ---------------------------------------------------------------------------------------------

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

  // ---------------------------------------------------------------------------------------------
  // Executor-scoped and driver-scoped collaborators.
  // ---------------------------------------------------------------------------------------------

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
   * The executor's single egress pacer. Its refill rate is re-derived whenever the coordinator
   * reports a different number of concurrent shuffles, which is the divisor the rate is defined by.
   */
  private lazy val rateLimiter: TokenBucketRateLimiter =
    TokenBucketRateLimiter(conf, INITIAL_CONCURRENT_SHUFFLES)

  /**
   * The executor's single credit ledger, liveness timer set and consumer receive quota. The
   * coordinator instance is passed when this JVM hosts one and `null` otherwise, which the protocol
   * documents and handles: an executor arbitrates over the shuffles registered locally instead.
   */
  private lazy val backpressure: BackpressureProtocol =
    new BackpressureProtocol(conf, driverCoordinator.orNull, rateLimiter)

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

  // ---------------------------------------------------------------------------------------------
  // ShuffleManager service-provider interface.
  // ---------------------------------------------------------------------------------------------

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
          if streamingAvailableFor(streaming.shuffleId) =>
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
          if streamingAvailableFor(streaming.shuffleId) =>
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
   * All three owners are told, unconditionally and in the order that keeps local disk honest. The
   * resolver goes first, because it is the only thing that unlinks retained spill files and a
   * shuffle whose files are never unlinked is a permanent local-disk leak. The coordinator is told
   * next so that no consumer can resolve a producer of a shuffle that no longer exists. The
   * delegate is told last and always, because a declined dependency of this shuffle id may have
   * been written by the sort-based writer.
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
    guard(s"forget the degradation state of shuffle $shuffleId") {
      fallbackPolicy.forgetShuffle(shuffleId)
    }
    sortShuffleManager.unregisterShuffle(shuffleId)
  }

  /**
   * Shuts the subsystem down exactly once, releasing only what was actually built.
   *
   * The order is the reverse of the dependency order, so nothing is released while something that
   * uses it is still live: consumer channels, then retained producer output, then the rendezvous
   * endpoint, then the delegate. Every step is contained, because a shutdown that abandoned the
   * remaining steps on the first failure would leak precisely the resources this method exists to
   * release.
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

  // ---------------------------------------------------------------------------------------------
  // Gating.
  // ---------------------------------------------------------------------------------------------

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
   * All three surfaces are consulted, cheapest first, and a verdict learned from the driver is
   * cached locally so that the next task on this executor answers without an RPC.
   */
  private def streamingAvailableFor(shuffleId: Int): Boolean = {
    if (fallbackPolicy.shouldDelegateToSortShuffle) {
      false
    } else if (fallbackPolicy.shuffleHasFallenBack(shuffleId)) {
      false
    } else {
      val declared = guardOption(s"read the fallback state of shuffle $shuffleId") {
        gatewayFor(shuffleId).fallbackState(shuffleId)
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
   */
  private def gatewayFor(shuffleId: Int): StreamingShuffleCoordinatorGateway = {
    new RpcStreamingShuffleCoordinatorGateway(coordinatorRef,
      Option(capabilityTokens.get(shuffleId)).getOrElse(""), coordinatorTimeout)
  }

  /** Stands one shuffle down for every participant, and caches the verdict locally. */
  private def declareFallbackFor(
      handle: StreamingShuffleHandle[_, _, _],
      reason: StreamingShuffleFallbackReason,
      detail: String): Unit = {
    val gateway = new RpcStreamingShuffleCoordinatorGateway(coordinatorRef, handle.capabilityToken,
      coordinatorTimeout)
    val state = gateway.declareFallback(handle.shuffleId, reason, detail)
    fallbackPolicy.observeShuffleFallback(handle.shuffleId, state)
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, handle.shuffleId)} stood down for " +
      log"${MDC(REASON, detail)}; the sort-based shuffle serves it from here on")
  }

  // ---------------------------------------------------------------------------------------------
  // Task-scoped construction.
  // ---------------------------------------------------------------------------------------------

  /**
   * Builds the producer half of one map task, falling back if the coordinator declines it.
   *
   * The resolver is matched rather than asserted upon. It is present exactly when streaming is
   * enabled, which is also what `streamingAvailableFor` requires, so its absence is unreachable --
   * but a writer without a resolver could not retain its output, and delegating is a strictly safer
   * answer to an unreachable state than an assertion that fails a task.
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

  /** Builds the producer half against the executor's retained-output owner. */
  private def streamingWriterOn[K, V, C](
      resolver: StreamingShuffleBlockResolver,
      handle: StreamingShuffleHandle[K, V, C],
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val shuffleId = handle.shuffleId
    val taskAttemptId = context.taskAttemptId()
    val notifier = new StreamingShuffleErrorNotifier(shuffleId, conf)
    val serverHandler = new StreamingShuffleServerHandler(conf, shuffleId, mapId, taskAttemptId,
      resolver, backpressure, rateLimiter, notifier)
    val (listener, _, server) = streamingListener
    // Registered before the producer is announced, so the executor's listener can route a frame the
    // instant a consumer acts on the address. Deliberately NOT released on task completion: a
    // successful map task's output is precisely the output no consumer has read yet, and the
    // listener is the only way to reach it. Release happens when the output does -- on generation
    // withdrawal, on unregisterShuffle, or when this manager stops.
    listener.register(shuffleId, mapId, serverHandler)
    announceProducer(handle, mapId, context, server) match {
      case Some(registration) =>
        applyConcurrency(registration.numConcurrentShuffles)
        val components = StreamingShuffleWriterComponents(
          backpressure = backpressure,
          rateLimiter = rateLimiter,
          spillManager = new MemorySpillManager(context.taskMemoryManager(), conf),
          blockResolver = resolver,
          serverHandler = serverHandler,
          fallbackPolicy = fallbackPolicy,
          errorNotifier = notifier,
          coordinatorGateway = new RpcStreamingShuffleCoordinatorGateway(coordinatorRef,
            handle.capabilityToken, coordinatorTimeout))
        new StreamingShuffleWriter[K, V, C](handle, mapId, context, metrics, conf, components)
      case None =>
        // Nothing else has been built, and in particular no memory has been bound to this task, so
        // withdrawing the routing entry is the whole of the cleanup delegation needs.
        listener.deregister(shuffleId, mapId)
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
          log"${MDC(MAP_ID, mapId)} could not publish its producer address; the sort-based " +
          log"shuffle writes this map output")
        sortShuffleManager.getWriter(handle, mapId, context, metrics)
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
    registration.filter { reply =>
      if (reply.accepted && debugEnabled) {
        logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, handle.shuffleId)} published producer " +
          log"${MDC(EXECUTOR_ID, env.executorId)} map ${MDC(MAP_ID, mapId)} attempt " +
          log"${MDC(TASK_ATTEMPT_ID, context.taskAttemptId())} at " +
          log"${MDC(HOST_PORT, location.hostPort)}")
      }
      reply.accepted
    }
  }

  /**
   * Re-derives the egress refill rate from the number of shuffles this executor is now serving.
   *
   * The rate is defined as the administered link capacity divided by that count and held to 80% of
   * it, so the count changing is the one thing that must move the rate. Doing it here, on the
   * coordinator's own answer, is what keeps the divisor a measured value rather than a guess.
   */
  private def applyConcurrency(numConcurrentShuffles: Int): Unit = {
    conf.get(org.apache.spark.internal.config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS).foreach {
      maxBandwidthMBps =>
        val share =
          TokenBucketRateLimiter.perShuffleBytesPerSecond(maxBandwidthMBps, numConcurrentShuffles)
        val paced = TokenBucketRateLimiter.applyLinkCapacityCeiling(share)
        if (rateLimiter.updateRate(paced) && debugEnabled) {
          logDebug(log"Streaming shuffle egress refill rate is now " +
            log"${MDC(NUM_BYTES, paced)} byte(s)/s across " +
            log"${MDC(COUNT, numConcurrentShuffles)} concurrent shuffle(s)")
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

  // ---------------------------------------------------------------------------------------------
  // Failure containment.
  // ---------------------------------------------------------------------------------------------

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
 * <b>What this deliberately is not.</b> Not an `IndexShuffleBlockResolver`, and not a
 * `MigratableResolver`. `ShuffleWriteProcessor` selects push-based merge by pattern matching on the
 * manager's resolver type, so a router that presented itself as an index resolver would switch
 * push-based merge back on for streamed writes -- exactly the behavioural surprise the streaming
 * resolver is documented to avoid. The price is that push-based merge declines for the whole
 * application while streaming is enabled -- the documented posture: streaming neither extends
 * nor interoperates with push-based merge, it declines to participate.
 *
 * @param streaming the streaming resolver, which owns retained streamed output
 * @param sorted the sort-based delegate's index resolver, which owns materialised sorted output
 */
private[spark] class StreamingShuffleBlockRouter(
    streaming: StreamingShuffleBlockResolver,
    sorted: ShuffleBlockResolver)
  extends ShuffleBlockResolver with Logging {

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
   * The concurrency divisor assumed before any coordinator has reported one.
   *
   * One, because a JVM about to build its first limiter is by definition serving at most one
   * shuffle, and because assuming fewer concurrent shuffles than there are yields a larger initial
   * refill rate that the first coordinator answer corrects downward at once. Assuming more would
   * throttle the first shuffle of every application for no reason.
   */
  val INITIAL_CONCURRENT_SHUFFLES: Int = 1

  /**
   * Floor on the coordinator ask deadline. A deadline shorter than a second would make an ordinary
   * driver pause look like an unreachable driver, and every one of these asks is on a path that
   * recovers by failing a task.
   */
  val MIN_COORDINATOR_TIMEOUT_MS: Long = 1000L
}
