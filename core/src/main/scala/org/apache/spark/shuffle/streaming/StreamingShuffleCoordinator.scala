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

import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging, LogKeys, MessageWithContext}
import org.apache.spark.internal.LogKeys.{COUNT, EPOCH, EXECUTOR_ID, HOST_PORT, MAP_ID, NUM_PARTITIONS, REASON, SHUFFLE_ID, TIME_UNITS, TIMEOUT}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.util.{Clock, RpcUtils, SystemClock, ThreadUtils, Utils}

/**
 * Address of a live streaming shuffle producer, as a pure value that is safe to ship inside an
 * RPC reply.
 *
 * Deliberately free of anything live: no channel, no `SparkConf`, no `RpcEndpointRef` and no
 * clock. A consumer turns this into a streaming connection itself, which keeps the coordinator
 * free of transport state and keeps every message in this family cheap to serialize.
 *
 * @param executorId id of the executor hosting the producer, used for the executor-scoped
 *                   concurrency count that the token bucket divides by
 * @param host host name or address on which the producer serves streaming blocks
 * @param port port on which the producer serves streaming blocks
 * @param mapId map task id whose output this producer streams
 */
private[spark] case class StreamingShuffleProducerLocation(
    executorId: String,
    host: String,
    port: Int,
    mapId: Long) {

  /** Host and port rendered the way Spark renders every other endpoint address. */
  def hostPort: String = s"$host:$port"
}

/**
 * Reply to a consumer lookup: every producer currently known to be streaming a shuffle, together
 * with the registration state a consumer needs in order to talk to them.
 *
 * `numPartitions` and `protocolVersion` let a consumer confirm that the producer generation it is
 * about to read from speaks the same protocol and partitions its output the same way it does, so
 * a mismatch trips the documented fallback rather than surfacing as a parse failure.
 * `coordinatorEpoch` lets a consumer recognize that it is holding locations from a stale producer
 * generation after an upstream stage has been recomputed.
 *
 * @param shuffleId shuffle these locations belong to
 * @param locations live producer locations, ordered by map id so that callers and tests observe a
 *                  deterministic sequence
 * @param numPartitions reduce partition count registered for this shuffle
 * @param protocolVersion streaming wire-protocol version registered for this shuffle
 * @param coordinatorEpoch epoch of this shuffle's producer generation
 */
private[spark] case class StreamingShuffleProducerLocations(
    shuffleId: Int,
    locations: Seq[StreamingShuffleProducerLocation],
    numPartitions: Int,
    protocolVersion: Byte,
    coordinatorEpoch: Long)

/**
 * Reply to a producer registration.
 *
 * A rejected registration is reported as `accepted = false` rather than as a thrown exception,
 * because every rejection reason -- an unsupported protocol version, or a partition count that
 * conflicts with the one already registered for the shuffle -- is a condition under which the
 * streaming path must degrade gracefully to sort-based shuffle. Failing the ask instead would
 * fail the task and defeat the zero-regression guarantee. `protocolVersion` always carries the
 * version the coordinator itself speaks, so a rejected caller learns what it should have sent.
 *
 * @param shuffleId shuffle the registration referred to
 * @param accepted whether the coordinator accepted the producer
 * @param coordinatorEpoch epoch of the shuffle's producer generation, or
 *                         [[StreamingShuffleCoordinator.NO_EPOCH]] when no epoch was assigned
 * @param protocolVersion protocol version the coordinator speaks
 * @param numConcurrentShuffles active shuffle count, which is the divisor of the token-bucket
 *                              refill rate and is never less than one
 */
private[spark] case class StreamingShuffleProducerRegistration(
    shuffleId: Int,
    accepted: Boolean,
    coordinatorEpoch: Long,
    protocolVersion: Byte,
    numConcurrentShuffles: Int)

/**
 * Base type of every message handled by [[StreamingShuffleCoordinator]].
 *
 * The family follows the message idiom of the map output tracker: a sealed trait, `case class`
 * requests carrying only primitives and immutable values, and a `case object` for the stop
 * signal. Every member is `private[spark]`, so none of them widens a public surface and none
 * needs a binary-compatibility exclusion.
 */
private[spark] sealed trait StreamingShuffleCoordinatorMessage

/**
 * Announces that an executor has begun streaming the output of one map task.
 *
 * @param shuffleId shuffle being streamed
 * @param location where the producer can be reached
 * @param numPartitions reduce partition count the producer streams to
 * @param protocolVersion streaming wire-protocol version the producer speaks
 * @param timestampMs producer-side wall clock at send time. Supplied by the caller, exactly as
 *                    the heartbeat message of the streaming wire protocol supplies it, and used
 *                    only for producer-to-coordinator clock-skew diagnostics. Liveness itself is
 *                    always judged by the coordinator's own injected clock, so a skewed or
 *                    hostile caller can neither extend nor shorten its own liveness window.
 */
private[spark] case class RegisterStreamingShuffleProducer(
    shuffleId: Int,
    location: StreamingShuffleProducerLocation,
    numPartitions: Int,
    protocolVersion: Byte,
    timestampMs: Long) extends StreamingShuffleCoordinatorMessage

/**
 * Asks for the producers currently streaming a shuffle, on behalf of a reduce task that consumes
 * partitions `[startPartition, endPartition)`.
 *
 * The reply is an `Option`: `None` means the shuffle has no registration yet, which is the normal
 * answer while a consumer polls an in-progress map stage rather than an error. Every producer of
 * a shuffle streams every reduce partition, so the range is not used to filter the reply; it is
 * validated against the registered partition count, which turns a mis-derived range into an
 * immediate, attributable failure instead of a silent partial read.
 *
 * @param shuffleId shuffle to look up
 * @param startPartition first reduce partition the consumer will read, inclusive
 * @param endPartition last reduce partition the consumer will read, exclusive
 */
private[spark] case class LookupStreamingShuffleProducers(
    shuffleId: Int,
    startPartition: Int,
    endPartition: Int) extends StreamingShuffleCoordinatorMessage

/**
 * Refreshes the liveness of one registered producer. The reply is `false` when the coordinator
 * holds no registration for the producer, which tells a survivor of a coordinator restart or of
 * a reaping decision that it must register again before consumers can find it.
 *
 * @param shuffleId shuffle the producer streams
 * @param mapId map task id of the producer
 * @param timestampMs producer-side wall clock at send time, used only for skew diagnostics
 */
private[spark] case class HeartbeatStreamingShuffleProducer(
    shuffleId: Int,
    mapId: Long,
    timestampMs: Long) extends StreamingShuffleCoordinatorMessage

/**
 * Reports that a producer generation is dead, so that the coordinator stops handing its address
 * out and advances the shuffle's epoch. Sent by a consumer that has just performed a partial-read
 * invalidation, and answered with the epoch that is current after the invalidation.
 *
 * This is the driver-side counterpart of consumer-side partial-read invalidation: the consumer
 * discards the bytes and raises a fetch failure so the unmodified scheduler recomputes the
 * upstream stage, while the coordinator forgets the dead address so the recomputed producer is
 * the only one a retry can find.
 *
 * @param shuffleId shuffle the producer streamed
 * @param mapId map task id of the dead producer
 * @param reason human-readable cause, recorded in the coordinator log
 */
private[spark] case class InvalidateStreamingShuffleProducer(
    shuffleId: Int,
    mapId: Long,
    reason: String) extends StreamingShuffleCoordinatorMessage

/**
 * Drops all state for a shuffle. Answered with `true` when state was present and dropped, and
 * with `false` when the shuffle was unknown, so unregistering twice is harmless.
 *
 * @param shuffleId shuffle whose state should be dropped
 */
private[spark] case class UnregisterStreamingShuffle(shuffleId: Int)
  extends StreamingShuffleCoordinatorMessage

/**
 * Asks how many streaming shuffles are concurrently active on one executor. The answer is the
 * divisor of the token-bucket refill rate, `maxBandwidthMBps * 1 MiB / numConcurrentShuffles`,
 * and is never less than one so that the division is always safe.
 *
 * @param executorId executor asking on its own behalf
 */
private[spark] case class GetStreamingShuffleConcurrency(executorId: String)
  extends StreamingShuffleCoordinatorMessage

/**
 * Self-addressed request that drives one pass of stale-producer reaping. Posted by the
 * coordinator's own timer so that reaping is processed on the endpoint thread like every other
 * message, and answered with the number of producers reaped.
 */
private[spark] case object ReapStaleStreamingShuffleProducers
  extends StreamingShuffleCoordinatorMessage

/**
 * Stops the coordinator endpoint, following the stop-message precedent of the map output
 * tracker: the endpoint replies first and stops itself afterwards.
 */
private[spark] case object StopStreamingShuffleCoordinator
  extends StreamingShuffleCoordinatorMessage

/**
 * One registered producer together with the moment the coordinator last saw it alive.
 *
 * `lastSeenMs` is always a reading of the coordinator's own injected clock, never a caller
 * supplied timestamp, which is what makes reaping deterministic under a manual clock and immune
 * to clock skew between hosts.
 *
 * @param location address of the producer
 * @param lastSeenMs coordinator clock reading at the last registration or heartbeat
 */
private[spark] case class StreamingShuffleProducerEntry(
    location: StreamingShuffleProducerLocation,
    lastSeenMs: Long)

/**
 * Everything the coordinator knows about one active streaming shuffle.
 *
 * The type is an immutable value on purpose. Each state is published into a `ConcurrentHashMap`
 * and every mutation replaces it wholesale inside an atomic `compute`, so a reader outside
 * message processing -- the rate limiter asking for the concurrency count, or a test asserting on
 * the registry -- always observes an internally consistent snapshot without taking a lock and
 * without any field needing to be volatile.
 *
 * @param numPartitions reduce partition count registered for the shuffle
 * @param protocolVersion streaming wire-protocol version registered for the shuffle
 * @param coordinatorEpoch epoch of the current producer generation, advanced whenever a producer
 *                         is invalidated or reaped
 * @param producers live producers keyed by map task id
 */
private[spark] case class StreamingShuffleState(
    numPartitions: Int,
    protocolVersion: Byte,
    coordinatorEpoch: Long,
    producers: Map[Long, StreamingShuffleProducerEntry])

/**
 * Rendezvous point and active-shuffle registry for the streaming shuffle subsystem.
 *
 * Why this component exists. In the classic path a reduce task learns where to fetch from by
 * asking `MapOutputTracker`, and it may only do so once the map stage has completed. A streaming
 * reduce task instead needs the address of a producer that is still running, and no such
 * rendezvous exists in Spark. This endpoint supplies it: producers announce themselves as they
 * begin streaming, consumers look them up while the map stage is in flight, and dead producer
 * generations are forgotten so that a retry after stage recomputation cannot be handed a stale
 * address. It also supplies `numConcurrentShuffles`, the divisor of the token-bucket refill rate
 * `maxBandwidthMBps * 1 MiB / numConcurrentShuffles`, which no existing Spark API reports.
 *
 * Coexistence with the classic path. This registry is entirely separate from map-output tracking
 * and shares no state with it; `MapOutputTracker` is read as a precedent and is never touched.
 * Sort-based shuffle remains the default and the fallback, and nothing here is reachable unless
 * an operator selects the streaming shuffle manager, so a cluster that has not opted in never
 * constructs this endpoint at all.
 *
 * How it attaches to a running Spark. `SparkEnv` takes its `RpcEnv` as a constructor parameter,
 * and the shuffle manager is created afterwards by a separate `initializeShuffleManager()` call,
 * so by the time the streaming shuffle manager builds this coordinator the `RpcEnv` is already
 * live and the endpoint can register itself on it. That is the entire reason `SparkEnv` needs no
 * modification for this feature. Use
 * [[StreamingShuffleCoordinator.registerOrLookupEndpoint]] to get either a registration on the
 * driver or a reference to the driver's registration on an executor.
 *
 * Initialisation hazard that this class deliberately avoids. On the driver the shuffle manager is
 * created before the memory manager and before the block manager is valid, so anything reachable
 * from this constructor that dereferenced `SparkEnv.get.memoryManager` or
 * `SparkEnv.get.blockManager` would read `null`. Neither is referenced here at all: the only
 * `SparkEnv`-derived value this class needs is the `RpcEnv` handed to it, which is safe.
 *
 * Local mode. In local mode there is exactly one shuffle manager instance, created with
 * `isDriver = true`, and it serves the producer role and the consumer role from the same JVM. The
 * registration path is therefore always `rpcEnv.setupEndpoint` and never a driver lookup, and
 * every operation is available as a direct method call as well as over RPC, so a producer
 * registration immediately followed by a consumer lookup on the very same instance works without
 * a remote peer existing.
 *
 * Thread safety. This is a [[ThreadSafeRpcEndpoint]], so messages are processed one at a time
 * with a happens-before relationship between them and no field would need to be volatile for the
 * message path alone. The registry is nevertheless a `ConcurrentHashMap` of immutable state
 * values mutated through atomic `compute`, because it is also read and written from outside
 * message processing: the shuffle manager registers a shuffle directly on the driver, and the
 * rate limiter asks for the concurrency count from the producer's own threads.
 *
 * Determinism. Every liveness decision reads the injected [[Clock]]. There is no `Thread.sleep`
 * and no direct call to `System.currentTimeMillis()` or `System.nanoTime()` anywhere in this
 * file, so a suite can drive reaping to the millisecond with a manual clock instead of waiting on
 * wall-clock time.
 *
 * Log volume. Shuffle-level events -- registration, unregistration, invalidation and reaping --
 * are logged at info level and are O(number of shuffles). Producer-level events -- individual
 * producer registrations, heartbeats, lookups and concurrency queries -- are O(number of map and
 * reduce tasks) and are therefore emitted only when `spark.shuffle.streaming.debug` is enabled.
 * That split is what keeps the subsystem inside its log budget with debug off by default.
 *
 * Telemetry. This component publishes none of the four streaming shuffle metrics. Each of them is
 * owned by exactly one component -- buffer utilisation and spill count by the spill manager,
 * backpressure events by the backpressure protocol, and partial-read invalidations by the reader
 * that performs them -- and incrementing any of them from here would double count in local mode,
 * where the coordinator and the reader share one JVM and therefore one metric registry.
 *
 * @param rpcEnv the [[RpcEnv]] this endpoint is registered to
 * @param conf the Spark configuration, read once here so that a configuration change requires an
 *             executor restart rather than taking effect mid-flight
 * @param clock clock used for every liveness decision; injected so that reaping is deterministic
 *              under test
 */
private[spark] class StreamingShuffleCoordinator(
    override val rpcEnv: RpcEnv,
    conf: SparkConf,
    clock: Clock = new SystemClock)
  extends ThreadSafeRpcEndpoint with Logging {

  logDebug("init") // force eager creation of logger

  // Read once at construction and held immutably: the streaming shuffle subsystem has no dynamic
  // reconfiguration, so every value it depends on is fixed for the lifetime of the JVM.
  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  // A producer that has not been seen for longer than this is treated as gone. The window is one
  // full liveness bound, so a producer heartbeating at the connection-timeout cadence may miss a
  // single heartbeat without being reaped.
  private val livenessTimeoutMs: Long = StreamingShuffleCoordinator.PRODUCER_LIVENESS_TIMEOUT_MS

  // Active streaming shuffles, keyed by shuffle id. See StreamingShuffleState for why the values
  // are immutable and why every mutation goes through an atomic compute.
  private val shuffleStates = new ConcurrentHashMap[Int, StreamingShuffleState]()

  // Monotonic source of coordinator epochs. The first epoch handed out is 1, so NO_EPOCH (0) is
  // unambiguously "no epoch was assigned" and every real epoch satisfies the non-negativity
  // requirement of the streaming shuffle handle.
  private val epochCounter = new AtomicLong(StreamingShuffleCoordinator.NO_EPOCH)

  // Timer that drives stale-producer reaping. Started in onStart and cancelled in onStop, in the
  // manner of the driver's heartbeat receiver. The executor is created lazily by the JDK, so an
  // endpoint that is constructed but never started spawns no thread.
  private var reaperTask: ScheduledFuture[_] = null

  private val reaperThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("streaming-shuffle-coordinator-reaper")

  /**
   * Arms the reaping timer. The timer does not touch the registry itself; it posts a message to
   * this endpoint so that reaping is processed on the endpoint thread along with every other
   * message. `self` is resolved inside the scheduled action rather than here, because an endpoint
   * reference is only valid once `onStart` has been entered and becomes `null` again after
   * `onStop`.
   */
  override def onStart(): Unit = {
    reaperTask = reaperThread.scheduleAtFixedRate(
      () => Utils.tryLogNonFatalError {
        Option(self).foreach(_.ask[Int](ReapStaleStreamingShuffleProducers))
      },
      StreamingShuffleCoordinator.REAPER_INTERVAL_MS,
      StreamingShuffleCoordinator.REAPER_INTERVAL_MS,
      TimeUnit.MILLISECONDS)
  }

  /**
   * Cancels the reaping timer, shuts its thread down and drops all registry state. Runs on a
   * clean stop and on a failed one alike, so no registration and no timer thread outlives the
   * endpoint.
   */
  override def onStop(): Unit = {
    if (reaperTask != null) {
      reaperTask.cancel(true)
      reaperTask = null
    }
    reaperThread.shutdownNow()
    val dropped = shuffleStates.size()
    shuffleStates.clear()
    logInfo(log"StreamingShuffleCoordinator dropped ${MDC(COUNT, dropped)} active streaming " +
      log"shuffle registrations on stop")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterStreamingShuffleProducer(
        shuffleId, location, numPartitions, protocolVersion, timestampMs) =>
      // registerProducer traces the registration itself, including the producer address, so only
      // the caller's clock skew is worth recording here.
      logSkewIfDebug(shuffleId, timestampMs)
      context.reply(registerProducer(shuffleId, location, numPartitions, protocolVersion))

    case LookupStreamingShuffleProducers(shuffleId, startPartition, endPartition) =>
      // Checked before the call so that a malformed range is reported to the caller without
      // routing through the endpoint's error handler.
      if (startPartition < 0 || endPartition < startPartition) {
        context.sendFailure(new IllegalArgumentException(
          s"Invalid reduce partition range [$startPartition, $endPartition) requested for " +
            s"streaming shuffle $shuffleId"))
      } else {
        logRequestIfDebug(log"a producer lookup", shuffleId, context)
        context.reply(lookupProducers(shuffleId, startPartition, endPartition))
      }

    case HeartbeatStreamingShuffleProducer(shuffleId, mapId, timestampMs) =>
      logSkewIfDebug(shuffleId, timestampMs)
      context.reply(heartbeatProducer(shuffleId, mapId))

    case InvalidateStreamingShuffleProducer(shuffleId, mapId, reason) =>
      context.reply(invalidateProducer(shuffleId, mapId, reason))

    case UnregisterStreamingShuffle(shuffleId) =>
      context.reply(unregisterShuffle(shuffleId))

    case GetStreamingShuffleConcurrency(executorId) =>
      val concurrency = numConcurrentShufflesFor(executorId)
      if (debugEnabled) {
        logDebug(log"Streaming shuffle concurrency for executor " +
          log"${MDC(EXECUTOR_ID, executorId)} is ${MDC(COUNT, concurrency)}")
      }
      context.reply(concurrency)

    case ReapStaleStreamingShuffleProducers =>
      context.reply(reapStaleProducers())

    case StopStreamingShuffleCoordinator =>
      logInfo("StreamingShuffleCoordinator stopped!")
      context.reply(true)
      stop()
  }

  /**
   * Registers a streaming shuffle and returns the epoch stamped on its first producer
   * generation, or `None` when the coordinator declines the shuffle.
   *
   * Called directly by the streaming shuffle manager on the driver, from `registerShuffle`, which
   * stamps the returned epoch onto the shuffle handle it hands to the scheduler. Registration is
   * idempotent: registering the same shuffle again with the same partition count and protocol
   * version returns the epoch already assigned rather than allocating a new one, and never
   * inflates the active-shuffle count.
   *
   * A declined registration is reported as `None` rather than thrown, so the manager can fall
   * back to sort-based shuffle instead of failing the job. A shuffle is declined when it asks for
   * a protocol version this coordinator does not speak, or when it contradicts a partition count
   * already registered under the same shuffle id.
   *
   * @param shuffleId shuffle to register
   * @param numPartitions reduce partition count of the shuffle; must be positive
   * @param protocolVersion streaming wire-protocol version the caller speaks
   * @return the shuffle's coordinator epoch, or `None` if the shuffle was declined
   */
  def registerShuffle(shuffleId: Int, numPartitions: Int, protocolVersion: Byte): Option[Long] = {
    require(numPartitions > 0,
      s"numPartitions must be positive for shuffle $shuffleId, but was $numPartitions")
    if (protocolVersion != StreamingShuffleCoordinator.CURRENT_PROTOCOL_VERSION) {
      logWarning(log"Declining streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)}: " +
        log"${MDC(REASON, protocolMismatchReason(protocolVersion))}")
      None
    } else {
      var created = false
      val state = shuffleStates.compute(shuffleId,
        (_: Int, existing: StreamingShuffleState) =>
          if (existing == null) {
            created = true
            StreamingShuffleState(numPartitions, protocolVersion,
              epochCounter.incrementAndGet(), Map.empty)
          } else {
            existing
          })
      if (state.numPartitions != numPartitions) {
        logWarning(log"Declining streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)}: " +
          log"${MDC(REASON, partitionMismatchReason(numPartitions, state.numPartitions))}")
        None
      } else {
        if (created) {
          logInfo(log"Registered streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} with " +
            log"${MDC(NUM_PARTITIONS, numPartitions)} reduce partitions at epoch " +
            log"${MDC(EPOCH, state.coordinatorEpoch)}")
        } else if (debugEnabled) {
          // Info-level logging stays proportional to the number of shuffles, so a repeated,
          // idempotent registration is recorded only under the debug gate.
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} is already registered at " +
            log"epoch ${MDC(EPOCH, state.coordinatorEpoch)}; reusing it")
        }
        Some(state.coordinatorEpoch)
      }
    }
  }

  /**
   * Registers, or re-registers, one producer of a shuffle and answers with the state the producer
   * needs: whether it was accepted, the epoch of its generation, the protocol version the
   * coordinator speaks and the current active-shuffle count.
   *
   * The shuffle is created on demand when it is not registered yet, using the partition count and
   * protocol version the producer supplies. That is what lets a producer register and a consumer
   * look it up on a single instance in local mode, and what makes the path robust to a producer
   * that reaches the coordinator before the manager's own `registerShuffle` call has.
   *
   * Registration is idempotent per map task: registering the same map id again replaces the
   * previous entry and refreshes its liveness, which is exactly the behaviour a recomputed or
   * speculated map attempt needs, and it can never inflate either the producer count or the
   * active-shuffle count.
   *
   * @param shuffleId shuffle the producer streams
   * @param location where the producer can be reached
   * @param numPartitions reduce partition count the producer streams to; must be positive
   * @param protocolVersion streaming wire-protocol version the producer speaks
   * @return the registration outcome, never `null`
   */
  def registerProducer(
      shuffleId: Int,
      location: StreamingShuffleProducerLocation,
      numPartitions: Int,
      protocolVersion: Byte): StreamingShuffleProducerRegistration = {
    require(location != null, s"The producer location must not be null for shuffle $shuffleId.")
    require(numPartitions > 0,
      s"numPartitions must be positive for shuffle $shuffleId, but was $numPartitions")
    val expected = StreamingShuffleCoordinator.CURRENT_PROTOCOL_VERSION
    if (protocolVersion != expected) {
      logWarning(log"Declining streaming shuffle producer for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, location.mapId)}: " +
        log"${MDC(REASON, protocolMismatchReason(protocolVersion))}")
      declinedRegistration(shuffleId)
    } else {
      val entry = StreamingShuffleProducerEntry(location, clock.getTimeMillis())
      val state = shuffleStates.compute(shuffleId,
        (_: Int, existing: StreamingShuffleState) =>
          if (existing == null) {
            StreamingShuffleState(numPartitions, protocolVersion,
              epochCounter.incrementAndGet(), Map(location.mapId -> entry))
          } else if (existing.numPartitions != numPartitions) {
            // Left untouched so that a contradictory registration cannot corrupt a live shuffle;
            // the mismatch is turned into a rejection below.
            existing
          } else {
            existing.copy(producers = existing.producers.updated(location.mapId, entry))
          })
      if (state.numPartitions != numPartitions) {
        logWarning(log"Declining streaming shuffle producer for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, location.mapId)}: " +
          log"${MDC(REASON, partitionMismatchReason(numPartitions, state.numPartitions))}")
        declinedRegistration(shuffleId)
      } else {
        if (debugEnabled) {
          logDebug(log"Registered streaming shuffle producer for shuffle " +
            log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, location.mapId)} on executor " +
            log"${MDC(EXECUTOR_ID, location.executorId)} at " +
            log"${MDC(HOST_PORT, location.hostPort)}")
        }
        StreamingShuffleProducerRegistration(shuffleId, accepted = true,
          state.coordinatorEpoch, expected, numConcurrentShuffles)
      }
    }
  }

  /**
   * Refreshes the liveness of one producer against the coordinator's own clock.
   *
   * @param shuffleId shuffle the producer streams
   * @param mapId map task id of the producer
   * @return `true` when a registration for the producer exists and was refreshed, `false` when
   *         the coordinator holds none and the producer must register again
   */
  def heartbeatProducer(shuffleId: Int, mapId: Long): Boolean = {
    val nowMs = clock.getTimeMillis()
    val state = shuffleStates.computeIfPresent(shuffleId,
      (_: Int, existing: StreamingShuffleState) =>
        existing.producers.get(mapId) match {
          case Some(entry) =>
            existing.copy(producers = existing.producers.updated(mapId,
              entry.copy(lastSeenMs = nowMs)))
          case None =>
            existing
        })
    val live = state != null && state.producers.contains(mapId)
    if (debugEnabled) {
      if (live) {
        logDebug(log"Refreshed the liveness of the streaming shuffle producer for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)}")
      } else {
        logDebug(log"Ignoring a heartbeat for the unknown streaming shuffle producer of " +
          log"shuffle ${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)}")
      }
    }
    live
  }

  /**
   * Returns every producer currently streaming a shuffle, or `None` when the shuffle has no
   * registration yet.
   *
   * `None` is the normal answer to a consumer polling an in-progress map stage, not an error, so a
   * caller is expected to retry rather than to fail. Producers are returned ordered by map id, so
   * repeated calls and test assertions observe a stable sequence.
   *
   * @param shuffleId shuffle to look up
   * @param startPartition first reduce partition the consumer will read, inclusive
   * @param endPartition last reduce partition the consumer will read, exclusive
   * @return the live producer locations plus the shuffle's registration state, or `None`
   */
  def lookupProducers(
      shuffleId: Int,
      startPartition: Int,
      endPartition: Int): Option[StreamingShuffleProducerLocations] = {
    require(startPartition >= 0,
      s"startPartition must be non-negative for shuffle $shuffleId, but was $startPartition")
    require(endPartition >= startPartition,
      s"endPartition $endPartition must not precede startPartition $startPartition for " +
        s"shuffle $shuffleId")
    val state = shuffleStates.get(shuffleId)
    if (state == null) {
      None
    } else {
      // Every producer of a shuffle streams every reduce partition, so the range does not filter
      // the reply. It is validated instead, because a range outside the registered partition
      // count can only come from a mis-derived request and must not be answered silently.
      require(endPartition <= state.numPartitions,
        s"endPartition $endPartition exceeds the ${state.numPartitions} reduce partitions " +
          s"registered for streaming shuffle $shuffleId")
      Some(StreamingShuffleProducerLocations(shuffleId, orderedLocations(state),
        state.numPartitions, state.protocolVersion, state.coordinatorEpoch))
    }
  }

  /**
   * Forgets one producer generation and advances the shuffle's epoch, so that no later lookup can
   * hand the dead address out and every consumer holding an older epoch can tell that its
   * locations are stale.
   *
   * @param shuffleId shuffle the producer streamed
   * @param mapId map task id of the dead producer
   * @param reason human-readable cause, recorded in the log
   * @return the shuffle's epoch after the invalidation, or [[StreamingShuffleCoordinator.NO_EPOCH]]
   *         when the shuffle is unknown
   */
  def invalidateProducer(shuffleId: Int, mapId: Long, reason: String): Long = {
    var epoch = StreamingShuffleCoordinator.NO_EPOCH
    var invalidated = false
    shuffleStates.computeIfPresent(shuffleId,
      (_: Int, existing: StreamingShuffleState) =>
        if (existing.producers.contains(mapId)) {
          invalidated = true
          epoch = epochCounter.incrementAndGet()
          existing.copy(coordinatorEpoch = epoch, producers = existing.producers - mapId)
        } else {
          epoch = existing.coordinatorEpoch
          existing
        })
    if (invalidated) {
      logInfo(log"Invalidated streaming shuffle producer for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)}; shuffle advanced to epoch " +
        log"${MDC(EPOCH, epoch)}: ${MDC(REASON, reason)}")
    } else if (debugEnabled) {
      logDebug(log"Ignoring invalidation of unknown streaming shuffle producer for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)}: ${MDC(REASON, reason)}")
    }
    epoch
  }

  /**
   * Drops all state for a shuffle. Driven from the streaming shuffle manager's `unregisterShuffle`
   * and idempotent: unregistering an unknown shuffle changes nothing and answers `false`.
   *
   * @param shuffleId shuffle whose state should be dropped
   * @return `true` when state was present and dropped
   */
  def unregisterShuffle(shuffleId: Int): Boolean = {
    val removed = shuffleStates.remove(shuffleId)
    if (removed == null) {
      if (debugEnabled) {
        logDebug(log"Ignoring unregistration of unknown streaming shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)}")
      }
      false
    } else {
      logInfo(log"Unregistered streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} holding " +
        log"${MDC(COUNT, removed.producers.size)} registered producers")
      true
    }
  }

  /**
   * Performs one pass of stale-producer reaping and returns how many producers were reaped.
   *
   * A producer that the coordinator has not seen for longer than the liveness window is dropped
   * and its shuffle's epoch is advanced, which is the driver-side half of producer-failure
   * handling: consumers stop being handed the dead address, and any consumer still holding it
   * detects the failure on its own connection timeout.
   *
   * The shuffle itself is kept even once its last producer has been reaped, because a recomputed
   * map stage will register into it again and because dropping it would make the active-shuffle
   * count -- and therefore the rate-limiter divisor -- oscillate during recovery. Shuffle state is
   * dropped only by [[unregisterShuffle]].
   *
   * Reaping is normally driven by this endpoint's own timer. It is a public method so that a test
   * can drive it directly against a manual clock instead of waiting on wall-clock time.
   *
   * @return the number of producers reaped in this pass
   */
  def reapStaleProducers(): Int = {
    val nowMs = clock.getTimeMillis()
    var reaped = 0
    shuffleStates.keySet().asScala.toSeq.foreach { shuffleId =>
      // Staleness is decided inside the atomic update, so a producer that heartbeats concurrently
      // is never reaped on the strength of an older snapshot. The dropped producers are logged
      // afterwards to keep the update itself free of expensive work.
      var dropped: Seq[StreamingShuffleProducerEntry] = Nil
      var epoch = StreamingShuffleCoordinator.NO_EPOCH
      shuffleStates.computeIfPresent(shuffleId,
        (_: Int, existing: StreamingShuffleState) => {
          val (live, stale) = existing.producers.partition {
            case (_, entry) => nowMs - entry.lastSeenMs <= livenessTimeoutMs
          }
          if (stale.isEmpty) {
            existing
          } else {
            dropped = stale.values.toSeq.sortBy(_.location.mapId)
            epoch = epochCounter.incrementAndGet()
            existing.copy(coordinatorEpoch = epoch, producers = live)
          }
        })
      dropped.foreach { entry =>
        logWarning(log"Reaped streaming shuffle producer for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, entry.location.mapId)} on executor " +
          log"${MDC(EXECUTOR_ID, entry.location.executorId)}: last seen " +
          log"${MDC(TIME_UNITS, nowMs - entry.lastSeenMs)} ms ago exceeds the liveness timeout " +
          log"${MDC(TIMEOUT, livenessTimeoutMs)} ms, so the shuffle advanced to epoch " +
          log"${MDC(EPOCH, epoch)}")
      }
      reaped += dropped.size
    }
    reaped
  }

  /**
   * Number of streaming shuffles currently registered, clamped to at least one.
   *
   * This is the sole source of the divisor in the token-bucket refill rate
   * `maxBandwidthMBps * 1 MiB / numConcurrentShuffles`. Clamping the answer here rather than at
   * every call site is what guarantees the division is always safe, including in the window
   * before the first shuffle has registered.
   */
  def numConcurrentShuffles: Int = math.max(1, shuffleStates.size())

  /**
   * Number of streaming shuffles that one executor is concurrently producing for, clamped to at
   * least one.
   *
   * This is the executor-scoped view of the registry, and it is the value an executor's rate
   * limiter should divide its bandwidth budget by: an executor producing for one shuffle is not
   * throttled on account of shuffles it takes no part in.
   *
   * @param executorId executor to count for
   * @return the number of shuffles for which the executor has a registered producer, never less
   *         than one
   */
  def numConcurrentShufflesFor(executorId: String): Int = {
    require(executorId != null, "The executor id must not be null.")
    val active = shuffleStates.values().asScala.count { state =>
      state.producers.values.exists(_.location.executorId == executorId)
    }
    math.max(1, active)
  }

  /** Ids of every registered streaming shuffle, ascending, for diagnostics and assertions. */
  def activeShuffleIds: Seq[Int] = shuffleStates.keySet().asScala.toSeq.sorted

  /**
   * Live producers of one shuffle, ordered by map id, or an empty sequence when the shuffle has
   * no registration. Unlike [[lookupProducers]] this performs no range validation and returns no
   * registration state, so it is the accessor to use for diagnostics and assertions.
   *
   * @param shuffleId shuffle to read
   * @return the producer locations, ordered by map id
   */
  def producersFor(shuffleId: Int): Seq[StreamingShuffleProducerLocation] = {
    val state = shuffleStates.get(shuffleId)
    if (state == null) {
      Nil
    } else {
      orderedLocations(state)
    }
  }

  /**
   * Current epoch of a shuffle's producer generation, or `None` when the shuffle has no
   * registration. Real epochs start at one, so a returned value is always positive.
   *
   * @param shuffleId shuffle to read
   * @return the shuffle's coordinator epoch
   */
  def epochFor(shuffleId: Int): Option[Long] = {
    Option(shuffleStates.get(shuffleId)).map(_.coordinatorEpoch)
  }

  /**
   * Drops every registration and rewinds the epoch counter, so that the coordinator reports what
   * a freshly constructed one would. This is useful in tests, which need a clean registry between
   * cases without paying to rebuild an endpoint.
   */
  def reset(): Unit = {
    shuffleStates.clear()
    epochCounter.set(StreamingShuffleCoordinator.NO_EPOCH)
  }

  /** Producer locations of a shuffle, ordered by map id so every reply is deterministic. */
  private def orderedLocations(
      state: StreamingShuffleState): Seq[StreamingShuffleProducerLocation] = {
    state.producers.values.map(_.location).toSeq.sortBy(_.mapId)
  }

  /**
   * Negative registration answer. It always reports the protocol version the coordinator speaks,
   * so a declined caller learns what it should have sent, and the current concurrency count, so
   * that a caller falling back still has a well-formed reply to read.
   */
  private def declinedRegistration(shuffleId: Int): StreamingShuffleProducerRegistration = {
    StreamingShuffleProducerRegistration(shuffleId, accepted = false,
      epochFor(shuffleId).getOrElse(StreamingShuffleCoordinator.NO_EPOCH),
      StreamingShuffleCoordinator.CURRENT_PROTOCOL_VERSION, numConcurrentShuffles)
  }

  /** Explains a protocol-version rejection in operator-facing terms. */
  private def protocolMismatchReason(protocolVersion: Byte): String = {
    s"protocol version $protocolVersion is not version " +
      s"${StreamingShuffleCoordinator.CURRENT_PROTOCOL_VERSION}, which the coordinator speaks"
  }

  /** Explains a partition-count rejection in operator-facing terms. */
  private def partitionMismatchReason(requested: Int, registered: Int): String = {
    s"$requested reduce partitions were requested but the shuffle is registered with $registered"
  }

  /**
   * Debug-level trace of one request, mirroring the map output tracker endpoint's logging helper:
   * it reads the sender address and composes the message with `+`, which is how a multi-part
   * message stays inside the line budget. The message fragment is taken by name so that a request
   * costs nothing to trace while debug logging is off.
   */
  private def logRequestIfDebug(
      msg: => MessageWithContext,
      shuffleId: Int,
      context: RpcCallContext): Unit = {
    if (debugEnabled) {
      logDebug(log"Asked to handle " +
        msg +
        log" for shuffle ${MDC(SHUFFLE_ID, shuffleId)} from " +
        log"${MDC(HOST_PORT, senderHostPort(context))}")
    }
  }

  /**
   * Debug-level record of the skew between a caller's clock and the coordinator's. Liveness never
   * depends on the caller's stamp, so this is pure diagnostics for an operator investigating
   * timeout behaviour across hosts; a positive value means the caller's clock trails.
   */
  private def logSkewIfDebug(shuffleId: Int, timestampMs: Long): Unit = {
    if (debugEnabled) {
      logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} caller clock trails the " +
        log"coordinator by ${MDC(TIME_UNITS, clock.getTimeMillis() - timestampMs)} ms")
    }
  }

  /**
   * Sender address of a request, or "local" when the request carries none. A message that never
   * left the JVM, or one sent from an `RpcEnv` without a server, can arrive with a null sender
   * address, so it is read defensively instead of dereferenced.
   */
  private def senderHostPort(context: RpcCallContext): String = {
    val address = context.senderAddress
    if (address == null) "local" else address.hostPort
  }
}

/**
 * Fixed names, protocol constants and the registration helper for
 * [[StreamingShuffleCoordinator]].
 *
 * The endpoint name is a constant for the same reason the map output tracker's is: the driver
 * registers the endpoint under it and every executor resolves the endpoint by it, so the two
 * sides must agree on one literal.
 */
private[spark] object StreamingShuffleCoordinator extends Logging {

  /**
   * Name the coordinator endpoint is registered under on the driver and looked up by on every
   * executor.
   */
  val ENDPOINT_NAME: String = "StreamingShuffleCoordinator"

  /**
   * Streaming shuffle wire-protocol version this build speaks.
   *
   * A registration that asks for any other version is declined, which is how the documented
   * "producer/consumer version mismatch" fallback condition is detected by an explicit
   * compatibility check rather than inferred from a parse failure. The value is positive, as the
   * streaming shuffle handle requires of the version it carries.
   */
  val CURRENT_PROTOCOL_VERSION: Byte = 1

  /**
   * Bound within which a producer failure is detected, in milliseconds. It is also the cadence at
   * which a producer is expected to heartbeat the coordinator, which is why one missed heartbeat
   * is tolerated before the liveness window expires.
   */
  val PRODUCER_CONNECTION_TIMEOUT_MS: Long = 5000L

  /**
   * Time without a heartbeat after which a producer registration is reaped, in milliseconds. This
   * is the liveness bound of the streaming shuffle protocol.
   */
  val PRODUCER_LIVENESS_TIMEOUT_MS: Long = 10000L

  /**
   * Cadence at which the coordinator polls for stale producers, in milliseconds. Polling at the
   * connection-timeout cadence bounds the extra delay a reaping decision can add to the liveness
   * window by one poll interval.
   */
  val REAPER_INTERVAL_MS: Long = PRODUCER_CONNECTION_TIMEOUT_MS

  /**
   * Epoch value meaning "no epoch has been assigned". Real epochs start at one, so this can never
   * collide with a live generation, and it is non-negative, so it is safe to carry anywhere an
   * epoch is expected.
   */
  val NO_EPOCH: Long = 0L

  /**
   * Registers the coordinator endpoint on the driver, or resolves a reference to the driver's
   * endpoint from an executor.
   *
   * This replicates, inside the streaming shuffle package, the driver-versus-executor shape that
   * `SparkEnv` uses for its own named endpoints. Replicating it here rather than calling into
   * `SparkEnv` is precisely why `SparkEnv` needs no modification for this feature: the shuffle
   * manager is created after `SparkEnv`'s constructor has completed, so the `RpcEnv` it hands over
   * is already live and this endpoint can register itself on it.
   *
   * `endpointCreator` is taken by name, so an executor never constructs a coordinator instance;
   * only the driver does. In local mode there is a single shuffle manager, created with
   * `isDriver = true`, so the `setupEndpoint` branch is the only one taken and the instance the
   * driver holds serves both the producer role and the consumer role from one JVM.
   *
   * {{{
   *   // In StreamingShuffleManager, which holds the instance only on the driver:
   *   private val coordinator: Option[StreamingShuffleCoordinator] =
   *     if (isDriver) Some(new StreamingShuffleCoordinator(rpcEnv, conf)) else None
   *   private val coordinatorRef: RpcEndpointRef =
   *     StreamingShuffleCoordinator.registerOrLookupEndpoint(rpcEnv, conf, isDriver,
   *       coordinator.get)
   * }}}
   *
   * @param rpcEnv the live `RpcEnv` to register on or resolve through
   * @param conf configuration supplying the driver host and port for an executor-side lookup
   * @param isDriver whether this process is the driver
   * @param endpointCreator creates the endpoint; evaluated on the driver only
   * @return a reference to the coordinator endpoint
   */
  def registerOrLookupEndpoint(
      rpcEnv: RpcEnv,
      conf: SparkConf,
      isDriver: Boolean,
      endpointCreator: => StreamingShuffleCoordinator): RpcEndpointRef = {
    if (isDriver) {
      logInfo(log"Registering ${MDC(LogKeys.ENDPOINT_NAME, ENDPOINT_NAME)}")
      rpcEnv.setupEndpoint(ENDPOINT_NAME, endpointCreator)
    } else {
      RpcUtils.makeDriverRef(ENDPOINT_NAME, conf, rpcEnv)
    }
  }
}

