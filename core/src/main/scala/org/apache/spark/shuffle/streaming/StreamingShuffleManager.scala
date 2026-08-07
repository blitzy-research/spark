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
 * Selection is tier one: `spark.shuffle.manager=streaming` resolves to this class through the
 * short-name table in the `ShuffleManager` companion object, which is the whole of the mechanism,
 * and the class is then instantiated reflectively with the `(SparkConf, isDriver: Boolean)`
 * constructor the trait documents. `sort` and `tungsten-sort` are untouched and `sort` remains the
 * default, so applications using the default sort manager do not instantiate the streaming manager
 * or route shuffle service-provider calls through it.
 *
 * Tier two is `spark.shuffle.streaming.enabled`, which defaults to `false` and gates the
 * <i>behaviour</i> of the selected class: while it is off, every service-provider call is forwarded
 * verbatim to an internally held, unmodified [[SortShuffleManager]]. That is not redundancy. It
 * gives an operator a kill switch that needs no change of manager class, and it gives all four
 * graceful-degradation conditions one terminus -- whether streaming stands down for an operator, a
 * consumer that fell too far behind, memory pressure, link saturation or a protocol disagreement,
 * this manager delegates and the sort-based path serves the shuffle.
 *
 * Spark authentication is a mandatory gate above both keys. Streaming moves serialized records
 * straight into a remote executor's deserializer and honours acknowledgements that release retained
 * output, so with `spark.authenticate` false every call is delegated even when the streaming key is
 * on; the transport builders and the handlers refuse an unauthenticated streaming channel
 * independently, so a misconfiguration cannot expose the data plane.
 *
 * The consequence worth stating plainly is that <b>no configuration, failure or resource condition
 * leaves a job without a working shuffle</b>: the delegate is constructed unconditionally, in this
 * constructor, before any streaming component exists.
 *
 * What activation cannot change is <i>when</i> consumers exist. The scheduler is unmodified, and it
 * submits a stage only once every parent stage reports its output available, so for a map stage
 * whose tasks each run once the reduce tasks start after the last map task finished: what they read
 * is retained output served by [[StreamingShuffleBlockResolver]] over the streaming transport, and
 * producer/consumer overlap is a capability the subsystem takes whenever a consumer really is
 * attached -- a reconnecting consumer replaying its unacknowledged window, or a reduce attempt
 * reading while a superseded or speculative map attempt still produces -- rather than an outcome
 * this class can manufacture. There is one egress path either way, with the same credit, checksums
 * and acknowledgements; only the arrival time differs.
 *
 * @param conf the executor's or driver's configuration, read once here and held immutably,
 *     which is what makes "a configuration change requires a restart" true by construction
 * @param isDriver whether this JVM is the driver, which decides whether the coordinator
 *     endpoint is hosted here or merely referenced
 */
private[spark] class StreamingShuffleManager(conf: SparkConf, isDriver: Boolean)
  extends ShuffleManager with Logging {

  import StreamingShuffleManager._

  // Configuration, read exactly once.

  private val streamingRequested: Boolean = conf.get(SHUFFLE_STREAMING_ENABLED)
  private val authenticationEnabled: Boolean = conf.get(NETWORK_AUTH_ENABLED)
  private val streamingEnabled: Boolean = streamingRequested && authenticationEnabled
  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  // Whether this application runs with the External Shuffle Service.
  private val externalShuffleServiceEnabled: Boolean = conf.get(config.SHUFFLE_SERVICE_ENABLED)

  /** Whether this JVM builds any streaming state at all. */
  private val streamingActive: Boolean = streamingEnabled && !externalShuffleServiceEnabled

  // Reported once per JVM, at construction, when an operator asked for streaming and a
  // configuration-level condition means it can never happen.
  //
  // This exists because the decline is otherwise completely silent and self-consistent: the
  // streaming manager is instantiated, its metrics source registers, four MBeans appear, and every
  // one of them reads a perfectly plausible 0 forever. Nothing in the metrics surface says "this
  // never activated", so an operator whose telemetry is flat has no way to tell a healthy idle
  // executor from a configuration that excluded streaming before the first shuffle.
  //
  // Warning level, once per JVM, and only for the two conditions that are properties of the
  // configuration rather than of a workload: authentication being off, and the External Shuffle
  // Service being on. The kill switch is deliberately not reported -- an operator who set it to
  // false asked for exactly this -- nor is map-side combining, which is a property of an individual
  // dependency and would fire on ordinary jobs, nor a fallback trip, which is already reported
  // where it is observed. Emitting at construction rather than per declined shuffle is what bounds
  // the volume at one line per executor for the life of the process.
  if (streamingRequested && !streamingActive) {
    val exclusion = if (!authenticationEnabled) {
      log"${MDC(CONFIG, NETWORK_AUTH_ENABLED.key)} is false and the streaming data plane carries " +
        log"serialized records, so it requires an authenticated transport"
    } else {
      log"${MDC(CONFIG, config.SHUFFLE_SERVICE_ENABLED.key)} is true and the External Shuffle " +
        log"Service cannot serve a streaming shuffle's blocks"
    }
    logWarning(log"Streaming shuffle was requested by " +
      log"${MDC(CONFIG2, SHUFFLE_STREAMING_ENABLED.key)} but cannot be used: " + exclusion +
      log". Every shuffle is served by the sort-based shuffle manager, and the " +
      log"shuffle.streaming metrics stay at zero for the life of this process")
  }

  // The delegate. Constructed unconditionally and first, so that delegation is available from the
  // moment this object exists -- including from the constructor of anything below it.

  private val sortShuffleManager: SortShuffleManager = new SortShuffleManager(conf)

  private val fallbackPolicy: StreamingShuffleFallbackPolicy =
    new StreamingShuffleFallbackPolicy(conf)

  /** The degradation policy every service-provider method on this manager routes on. */
  private[streaming] def degradationPolicy: StreamingShuffleFallbackPolicy = fallbackPolicy

  private val stopped = new AtomicBoolean(false)

  /** Wall clock, used for the one timestamp this class produces. */
  private val clock: Clock = new SystemClock

  /** Capability token of each shuffle registered by this JVM, keyed by shuffle id. */
  private val capabilityTokens = new ConcurrentHashMap[Integer, String]()

  // Executor-scoped and driver-scoped collaborators.

  /**
   * The driver's coordinator instance, present only on the driver and only while streaming is
   * enabled.
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
   */
  private lazy val coordinatorRef: RpcEndpointRef = {
    val rpcEnv = SparkEnv.get.rpcEnv
    StreamingShuffleCoordinator.registerOrLookupEndpoint(rpcEnv, conf, isDriver,
      driverCoordinator.getOrElse(new StreamingShuffleCoordinator(rpcEnv, conf)))
  }

  // Publishes the coordinator's endpoint name on the driver at construction time, which is what
  // makes the claim on `driverCoordinator` above true.
  driverCoordinator.foreach(_ => coordinatorRef)

  /** Executor-scoped owner of retained map output. */
  private val streamingBlockResolver: Option[StreamingShuffleBlockResolver] = {
    if (streamingActive) Some(new StreamingShuffleBlockResolver(conf)) else None
  }

  /** The executor's whole egress allowance, divided among the shuffles it is producing for. */
  private lazy val egressBudget: TokenBucketRateLimiter.ExecutorEgressBudget =
    TokenBucketRateLimiter.executorBudget(conf, clock, () => executorConcurrency())

  @volatile private var backpressureBuilt: Boolean = false

  /** The executor's single credit ledger, liveness timer set and consumer receive quota. */
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
   */
  private[streaming] def egressPacing: TokenBucketRateLimiter.ExecutorEgressBudget = egressBudget

  /** This executor's flow-control protocol, read-only and at package scope. */
  private[streaming] def flowControl: BackpressureProtocol = backpressure

  /** Whether any flow-control state exists yet, so a caller can ask without creating it. */
  private[streaming] def flowControlBuilt: Boolean = backpressureBuilt

  /** The window that bounds this manager's per-shuffle registration record. */
  private val registrationLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  @volatile private var listenerBound: Boolean = false

  /** The executor's one streaming shuffle listener, together with the transport it owns. */
  private lazy val streamingListener:
      (StreamingShuffleListener, TransportContext, StreamingShuffleListener.BoundServer) = {
    val bound = StreamingShuffleListener.bind(conf)
    listenerBound = true
    bound
  }

  @volatile private var connectorCreated: Boolean = false

  /** The executor's single consumer-side connector. */
  private lazy val producerConnector: NettyStreamingShuffleProducerConnector = {
    val connector = new NettyStreamingShuffleProducerConnector(conf)
    connectorCreated = true
    connector
  }

  private lazy val coordinatorTimeout: RpcTimeout = {
    val configured = conf.getTimeAsMs(StreamingShuffleServerHandler.CONNECTION_TIMEOUT_KEY,
      s"${StreamingShuffleServerHandler.CONNECTION_TIMEOUT_SECONDS}s")
    new RpcTimeout(math.max(MIN_COORDINATOR_TIMEOUT_MS, configured).millis,
      StreamingShuffleServerHandler.CONNECTION_TIMEOUT_KEY)
  }

  /** The collaborators every reduce task on this executor shares. */
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
            // a configuration that has not changed.
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

  /** Builds the producer half for one map task, or delegates. */
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

  /** Builds the consumer half for one reduce task, or delegates. */
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
        // Any map range, narrowed or whole.
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
   * @return the delegate's verdict, since it is the only participant whose bookkeeping the
   *     caller can act upon
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
    // routing entry is one step of the single operation that does both.
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

  /** Shuts the subsystem down exactly once, releasing only what was actually built. */
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
      // Both transports are closed, so no new frame can submit work.
      guard("stop the executor's streaming data-plane workers") {
        BackpressureProtocol.shutdownDataPlaneWorkers()
      }
      // After both transports, because a limiter that is being retired must not be one a live
      // channel is still charging against.
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
      // resolver this manager published is the one that gets stopped.
      guard("stop the shuffle block resolver")(shuffleBlockResolver.stop())
      driverCoordinator.foreach { _ =>
        guard("stop the streaming shuffle coordinator") {
          coordinatorRef.askSync[Boolean](StopStreamingShuffleCoordinator, coordinatorTimeout)
        }
      }
      guard("stop the sort-based shuffle delegate")(sortShuffleManager.stop())
      // The executor's buffer threshold ticker is created by whichever map task first needed
      // polling and is deliberately executor-scoped, so no individual spill manager may end it --
      // and none does.
      guard("stop the executor's streaming buffer threshold ticker") {
        MemorySpillManager.shutdownExecutorPoller()
      }
    }
  }

  // Inspection.

  /** The four graceful-degradation trip conditions, as this executor observes them. */
  def streamingFallbackPolicy: StreamingShuffleFallbackPolicy = fallbackPolicy

  /** The executor's serving listener, if one has been bound. */
  def boundStreamingListener: Option[StreamingShuffleListener] = {
    if (listenerBound) Some(streamingListener._1) else None
  }

  /** The coordinator this manager hosts, present only on the driver. */
  def boundCoordinator: Option[StreamingShuffleCoordinator] = driverCoordinator

  /** The executor's bound serving socket, if one has been bound. */
  private[streaming] def boundStreamingServer: Option[StreamingShuffleListener.BoundServer] = {
    if (listenerBound) Some(streamingListener._3) else None
  }

  /** Consumer routes currently carried by this executor's streaming connector. */
  private[streaming] def activeStreamingConsumerRoutes:
      Seq[(StreamingShuffleClientHandler, TransportClient)] = {
    if (connectorCreated) producerConnector.activeRoutes else Nil
  }

  /**
   * Pauses inbound traffic on every consumer channel this executor currently owns.
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

  /** Whether every Netty event-loop group this manager created has completed termination. */
  private[streaming] def streamingTransportsTerminated: Boolean = {
    val listenerTerminated = !listenerBound || streamingListener._3.isTerminated
    val connectorTerminated =
      !connectorCreated || producerConnector.transportResourcesTerminated
    listenerTerminated && connectorTerminated
  }

  // Gating.

  /** Why this dependency cannot be streamed, or `None` when it can. */
  private def declineReason(dependency: ShuffleDependency[_, _, _]): Option[String] = {
    if (!streamingRequested) {
      Some(s"${SHUFFLE_STREAMING_ENABLED.key} is false")
    } else if (externalShuffleServiceEnabled) {
      // The External Shuffle Service serves a shuffle block by reading the index and data files an
      // executor left on local disk, from a process that outlives that executor.
      Some(s"${config.SHUFFLE_SERVICE_ENABLED.key} is true, and the External Shuffle Service " +
        "cannot serve a streaming shuffle's blocks")
    } else if (!authenticationEnabled) {
      // The streaming data plane carries serialised records, so it must be authenticated before any
      // byte of it reaches deserialisation.
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

  /** Whether a shuffle already registered for streaming may still be streamed on this executor. */
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
      // the one observed.
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
   */
  private def gatewayFor(
      handle: StreamingShuffleHandle[_, _, _]): StreamingShuffleCoordinatorGateway = {
    new RpcStreamingShuffleCoordinatorGateway(coordinatorRef, handle.capabilityToken,
      coordinatorTimeout)
  }

  /**
   * Stands one shuffle down for every participant, and caches the verdict the coordinator latched.
   *
   * @return the verdict in force after the declaration; `fallenBack` is false when it could not
   *     be made, in which case the caller must not delegate
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
   * @param handle the shuffle to withdraw from streaming
   * @param detail what was asked for and why streaming declined it
   * @return the verdict in force after the withdrawal; `fallenBack` is false when it could not
   *     be made, in which case the caller must not delegate
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
   * @param handle the shuffle being stood down
   * @param detail what was observed or asked for, used only in the unconfirmed-declaration
   *     record
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
        // using the link -- but only once no stream of it remains registered here.
        if (state.fallenBack && backpressureBuilt && backpressure.streamCount(shuffleId) == 0) {
          releaseShuffleStreamingState(shuffleId, "the shuffle stood streaming down")
        }
        state
    }
  }

  // Task-scoped construction.

  /** Builds the producer half of one map task, falling back if the coordinator declines it. */
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

  /** Builds the producer half against the executor's retained-output owner, transactionally. */
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
    // another shuffle.
    val limiter = egressBudget.limiterFor(shuffleId)
    // The partition count travels from the handle, which is the shuffle-wide registration every
    // participant shares, so the handler's partition domain is fixed before it can receive a frame
    // and is the same figure on every executor.
    val serverHandler = new StreamingShuffleServerHandler(conf, shuffleId, mapId, taskAttemptId,
      handle.numPartitions, resolver, listener, backpressure, limiter, notifier, fallbackPolicy)
    // Registered before the producer is announced, so the executor's listener can route a frame the
    // instant a consumer acts on the address.
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

  /** Answers a producer announcement the coordinator would not publish. */
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
    // ends of this shuffle have no rendezvous. That is a structural decline rather than one of the
    // four throughput or resource conditions, and it is declared as one so an operator is not sent
    // to tune a measurement this path never takes. The reader declares the same cause when it
    // cannot resolve a producer, which is the same failure seen from the other end.
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

  /** Unwinds a producer that was announced and then could not be built. */
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
   * @param numConcurrentShuffles the count the coordinator reported; a non-positive value is
   *     ignored by the budget rather than treated as a divisor
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

  /** Releases the executor's streaming listener, if one was ever bound. */
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
 * @param streaming the streaming resolver, which owns retained streamed output
 * @param sorted the sort-based delegate's index resolver, which owns materialised sorted output
 */
private[spark] class StreamingShuffleBlockRouter(
    streaming: StreamingShuffleBlockResolver,
    sorted: ShuffleBlockResolver)
  extends ShuffleBlockResolver with Logging {

  /** The streaming resolver this router delegates streamed blocks to. */
  def streamingResolver: StreamingShuffleBlockResolver = streaming

  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    resolverFor(blockId).getBlockData(blockId, dirs)
  }

  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    val streamed = streaming.getBlocksForShuffle(shuffleId, mapId)
    if (streamed.nonEmpty) streamed else sorted.getBlocksForShuffle(shuffleId, mapId)
  }

  /** Merged blocks are never streaming's. */
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

  /** Stops both resolvers. */
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

  /** The resolver that owns a block identity, decided by asking rather than by failing. */
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

/** Constants of the streaming shuffle manager. */
private[spark] object StreamingShuffleManager {

  /** The value of `spark.shuffle.manager` that selects this manager. */
  val SHORT_NAME: String = "streaming"

  /** Floor on the coordinator ask deadline. */
  val MIN_COORDINATOR_TIMEOUT_MS: Long = 1000L

}
