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

import java.nio.charset.StandardCharsets
import java.security.{MessageDigest, SecureRandom}
import java.util.{Base64, HashMap, LinkedHashSet, TreeMap}
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.immutable.SortedMap
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{MapOutputTrackerMaster, SparkConf, SparkEnv}
import org.apache.spark.internal.{config, Logging, LogKeys, MessageWithContext}
import org.apache.spark.internal.LogKeys.{CLASS_NAME, COUNT, DESCRIPTION, ELAPSED_TIME, EPOCH,
  EXECUTOR_ID, HOST, HOST_PORT, INDEX, MAP_ID, MAX_SIZE, NEW_VALUE, NUM_EVENTS, NUM_PARTITIONS,
  NUM_SKIPPED, NUM_TASKS, REASON, SHUFFLE_ID, STATUS, TASK_ATTEMPT_ID, TIME_UNITS, TIMEOUT}
import org.apache.spark.network.shuffle.protocol.streaming.StreamingShuffleMessage
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, RpcTimeout,
  ThreadSafeRpcEndpoint}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{Clock, RpcUtils, SystemClock, ThreadUtils, Utils}

/**
 * Immutable identity of one streaming shuffle producer generation.
 *
 * @param mapIndex index of the map task within its stage, which is the logical identity a later
 *     attempt supersedes on
 * @param mapId map task id whose output the producer streams
 * @param taskAttemptId task attempt id of the producer, unique across the application and
 *     increasing with each attempt of the same map task
 */
private[spark] case class StreamingShuffleProducerGeneration(
    mapIndex: Int,
    mapId: Long,
    taskAttemptId: Long)

/**
 * Address of a live streaming shuffle producer, as a pure value that is safe to ship inside an RPC
 * reply.
 *
 * @param executorId id of the executor hosting the producer, used for the executor-scoped
 *     concurrency count that the token bucket divides by
 * @param host host name or address on which the producer serves streaming blocks.
 * @param port port on which the producer serves streaming blocks
 * @param mapId map task id whose output this producer streams
 * @param mapIndex index of the map task within its stage, which is the identity
 *     `MapOutputTracker` removes a map output by.
 * @param taskAttemptId task attempt id of the producer; together with `mapId` this is the
 *     producer's generation identity
 * @param blockManagerId block-manager identity this producer's `MapStatus` carries.
 * @param registrationEpoch coordinator epoch at which this producer was published, stamped by
 *     the coordinator and left at [[StreamingShuffleCoordinator.NO_EPOCH]] by the producer that
 *     builds the location.
 */
private[spark] case class StreamingShuffleProducerLocation(
    executorId: String,
    host: String,
    port: Int,
    mapId: Long,
    mapIndex: Int,
    taskAttemptId: Long,
    blockManagerId: BlockManagerId,
    registrationEpoch: Long = StreamingShuffleCoordinator.NO_EPOCH) {

  /** Host and port rendered the way Spark renders every other endpoint address. */
  def hostPort: String = s"$host:$port"

  /** Generation identity of this producer, which is what every mutation path compares. */
  def generation: StreamingShuffleProducerGeneration =
    StreamingShuffleProducerGeneration(mapIndex, mapId, taskAttemptId)
}

/**
 * Declared shape of a shuffle's map stage, and how much of it has been streamed to completion.
 *
 * @param numMaps number of map tasks the stage declared, as the driver knows it.
 * @param completedMaps task attempt id that reported completion, keyed by map index.
 */
private[spark] case class StreamingShuffleMapStage(
    numMaps: Int,
    completedMaps: Map[Int, Long] = Map.empty) {

  /** Whether every map output the stage declared has been streamed to completion. */
  def complete: Boolean = completedMaps.size >= numMaps

  /** Map indexes whose output has been streamed to completion, for a consumer's progress check. */
  def completedIndexes: Set[Int] = completedMaps.keySet

  /** This stage with one map output recorded complete by the attempt that produced it. */
  def withCompleted(mapIndex: Int, taskAttemptId: Long): StreamingShuffleMapStage = {
    copy(completedMaps = completedMaps.updated(mapIndex, taskAttemptId))
  }

  /**
   * This stage with one map output's completion withdrawn, but only when the attempt named is the
   * one that recorded it.
   */
  def withoutCompleted(mapIndex: Int, taskAttemptId: Long): StreamingShuffleMapStage = {
    if (completedBy(mapIndex, taskAttemptId)) {
      copy(completedMaps = completedMaps - mapIndex)
    } else {
      this
    }
  }

  /** Whether this exact attempt is the one recorded as having completed this map index. */
  def completedBy(mapIndex: Int, taskAttemptId: Long): Boolean = {
    completedMaps.get(mapIndex).contains(taskAttemptId)
  }

  /** This stage with every completion withdrawn, which is what a shuffle-wide fallback does. */
  def cleared: StreamingShuffleMapStage = {
    if (completedMaps.isEmpty) this else copy(completedMaps = Map.empty)
  }
}

/**
 * Whether a shuffle has stood streaming down for every participant, and why.
 *
 * @param reasonName name of the latched [[StreamingShuffleStandDownCause]], or
 *     [[StreamingShuffleCoordinator.NO_FALLBACK_REASON]] while streaming is in force
 * @param declaredAtEpoch coordinator epoch the shuffle advanced to when the fallback was
 *     declared, or [[StreamingShuffleCoordinator.NO_EPOCH]] while streaming is in force.
 * @param detail the declaring component's own sanitised account of the condition, or
 *     [[StreamingShuffleCoordinator.NO_INVALIDATION_DETAIL]] when it gave none
 */
private[spark] case class StreamingShuffleFallbackState(
    reasonName: String = StreamingShuffleCoordinator.NO_FALLBACK_REASON,
    declaredAtEpoch: Long = StreamingShuffleCoordinator.NO_EPOCH,
    detail: String = StreamingShuffleCoordinator.NO_INVALIDATION_DETAIL) {

  /** Whether the shuffle has stood streaming down for every participant. */
  def fallenBack: Boolean = reasonName != StreamingShuffleCoordinator.NO_FALLBACK_REASON

  /**
   * The latched cause, resolved against this build's closed set, or `None` while streaming is in
   * force.
   */
  def cause: Option[StreamingShuffleStandDownCause] =
    StreamingShuffleStandDownCause.fromName(reasonName)

  /** The latched cause when, and only when, it is one of the four specified fallback conditions. */
  def reason: Option[StreamingShuffleFallbackReason] =
    StreamingShuffleFallbackReason.fromName(reasonName)

  /**
   * The operator-facing account of this verdict: the declarer's own detail when it gave one, this
   * build's prose for the latched cause when it did not, and the bare name only for a name this
   * build cannot resolve.
   */
  def condition: String = {
    if (detail != null && detail.nonEmpty &&
        detail != StreamingShuffleCoordinator.NO_INVALIDATION_DETAIL) {
      detail
    } else if (reasonName == StreamingShuffleFallbackReason.UNAVAILABLE_NAME) {
      StreamingShuffleFallbackReason.UNAVAILABLE_DESCRIPTION
    } else {
      cause.map(_.description).getOrElse(reasonName)
    }
  }
}

/**
 * Reply to a consumer lookup: every producer currently known to be streaming a shuffle, together
 * with the registration state a consumer needs in order to talk to them.
 *
 * @param shuffleId shuffle these locations belong to
 * @param locations live producer locations, ordered by map index so that callers and tests
 *     observe a deterministic sequence, and at most one per map index because that is the identity
 *     the registry holds a producer under.
 * @param numPartitions reduce partition count registered for this shuffle
 * @param numMaps number of map tasks the stage declared; zero is a legitimately empty map stage
 * @param completedMapIndexes map indexes whose output a live producer has streamed to
 *     completion
 * @param protocolVersion streaming wire-protocol version registered for this shuffle
 * @param coordinatorEpoch epoch of this shuffle's producer generation
 * @param fallback whether, and why, this shuffle has stood streaming down for every participant
 */
private[spark] case class StreamingShuffleProducerLocations(
    shuffleId: Int,
    locations: Seq[StreamingShuffleProducerLocation],
    numPartitions: Int,
    numMaps: Int,
    completedMapIndexes: Set[Int],
    protocolVersion: Byte,
    coordinatorEpoch: Long,
    fallback: StreamingShuffleFallbackState = StreamingShuffleFallbackState()) {

  /**
   * Whether every map output this stage declared has been streamed to completion, so a consumer
   * that has drained what it holds may stop expecting more.
   */
  def mapStageComplete: Boolean = completedMapIndexes.size >= numMaps

  /** Whether map output this consumer has not yet been offered is still expected. */
  def mapOutputsPending: Boolean = !mapStageComplete && !fallback.fallenBack
}

/**
 * Reply to a producer registration.
 *
 * @param shuffleId shuffle the registration referred to
 * @param accepted whether the coordinator accepted the producer
 * @param coordinatorEpoch epoch that is current for the shuffle once the registration has been
 *     processed, or [[StreamingShuffleCoordinator.NO_EPOCH]] when no epoch was assigned.
 * @param protocolVersion protocol version the coordinator speaks
 * @param numConcurrentShuffles number of shuffles the registering executor is concurrently
 *     producing for.
 * @param generationStale set on a rejection when the registration was refused because the
 *     registering generation is no longer the one that owns its map index -- it has been superseded
 *     by a later attempt, or retired.
 */
private[spark] case class StreamingShuffleProducerRegistration(
    shuffleId: Int,
    accepted: Boolean,
    coordinatorEpoch: Long,
    protocolVersion: Byte,
    numConcurrentShuffles: Int,
    generationStale: Boolean = false)

/**
 * Reply to a producer heartbeat.
 *
 * @param live whether a registration for the heartbeating generation exists and was refreshed
 * @param coordinatorEpoch epoch that is current for the shuffle, or
 *     [[StreamingShuffleCoordinator.NO_EPOCH]] when the shuffle is unknown
 */
private[spark] case class StreamingShuffleProducerLiveness(live: Boolean, coordinatorEpoch: Long)

/**
 * What the driver receives when it registers a shuffle: the epoch of the shuffle's first producer
 * generation and the capability token that authorizes every later operation on it.
 *
 * @param coordinatorEpoch epoch stamped on the shuffle's first producer generation
 * @param capabilityToken opaque bearer token authorizing operations on this shuffle
 * @param fallback whether, and why, this shuffle has already stood streaming down.
 */
private[spark] case class StreamingShuffleRegistrationGrant(
    coordinatorEpoch: Long,
    capabilityToken: String,
    fallback: StreamingShuffleFallbackState = StreamingShuffleFallbackState()) {

  /** Renders the grant without disclosing the credential it carries. */
  override def toString: String =
    s"StreamingShuffleRegistrationGrant(coordinatorEpoch=$coordinatorEpoch, " +
      s"capabilityToken=${StreamingShuffleCoordinator.REDACTED_TOKEN}, fallback=$fallback)"
}

/** Base type of every message handled by [[StreamingShuffleCoordinator]]. */
private[spark] sealed trait StreamingShuffleCoordinatorMessage {

  /** The rendering a capability token takes wherever a member of this family renders itself. */
  protected def redactedToken: String = StreamingShuffleCoordinator.REDACTED_TOKEN
}

/**
 * Announces that an executor has begun streaming the output of one map task.
 *
 * @param shuffleId shuffle being streamed
 * @param capabilityToken token issued when the shuffle was registered on the driver, without
 *     which the registration is refused
 * @param location where the producer can be reached, and which generation it is
 * @param numPartitions reduce partition count the producer streams to
 * @param protocolVersion streaming wire-protocol version the producer speaks
 * @param timestampMs producer-side milliseconds at send time.
 */
private[spark] case class RegisterStreamingShuffleProducer(
    shuffleId: Int,
    capabilityToken: String,
    location: StreamingShuffleProducerLocation,
    numPartitions: Int,
    protocolVersion: Byte,
    timestampMs: Long) extends StreamingShuffleCoordinatorMessage {

  /** Renders the request without disclosing the credential it carries. */
  override def toString: String =
    s"RegisterStreamingShuffleProducer(shuffleId=$shuffleId, " +
      s"capabilityToken=$redactedToken, location=$location, numPartitions=$numPartitions, " +
      s"protocolVersion=$protocolVersion, timestampMs=$timestampMs)"
}

/**
 * Asks for the producers currently streaming a shuffle, on behalf of a reduce task that consumes
 * partitions `[startPartition, endPartition)`.
 *
 * @param shuffleId shuffle to look up
 * @param capabilityToken token issued when the shuffle was registered on the driver, without
 *     which the lookup is refused.
 * @param startPartition first reduce partition the consumer will read, inclusive
 * @param endPartition last reduce partition the consumer will read, exclusive
 */
private[spark] case class LookupStreamingShuffleProducers(
    shuffleId: Int,
    capabilityToken: String,
    startPartition: Int,
    endPartition: Int) extends StreamingShuffleCoordinatorMessage {

  /** Renders the request without disclosing the credential it carries. */
  override def toString: String =
    s"LookupStreamingShuffleProducers(shuffleId=$shuffleId, " +
      s"capabilityToken=$redactedToken, startPartition=$startPartition, endPartition=$endPartition)"
}

/**
 * Refreshes the liveness of one registered producer generation.
 *
 * @param shuffleId shuffle the producer streams
 * @param capabilityToken token issued when the shuffle was registered on the driver, without
 *     which the heartbeat is refused
 * @param generation generation identity of the heartbeating producer
 * @param timestampMs producer-side milliseconds at send time, used only for skew diagnostics
 */
private[spark] case class HeartbeatStreamingShuffleProducer(
    shuffleId: Int,
    capabilityToken: String,
    generation: StreamingShuffleProducerGeneration,
    timestampMs: Long) extends StreamingShuffleCoordinatorMessage {

  /** Renders the request without disclosing the credential it carries. */
  override def toString: String =
    s"HeartbeatStreamingShuffleProducer(shuffleId=$shuffleId, " +
      s"capabilityToken=$redactedToken, generation=$generation, timestampMs=$timestampMs)"
}

/**
 * Why a producer generation was invalidated, as a closed set of codes rather than as free text.
 *
 * @param code the stable, operator-facing token recorded in the coordinator log
 */
private[spark] sealed abstract class StreamingShuffleInvalidationReason(val code: String) {

  /** The code itself, so that rendering a reason can never widen what reaches a log record. */
  override def toString: String = code
}

/**
 * The closed set of invalidation reasons, together with the only two ways to obtain one from
 * untrusted input.
 */
private[spark] object StreamingShuffleInvalidationReason {

  /** No data or heartbeat arrived from the producer inside the connection-timeout window. */
  case object ConnectionTimeout
    extends StreamingShuffleInvalidationReason("CONNECTION_TIMEOUT")

  /** A block failed its CRC32C check and could not be repaired by retransmission. */
  case object ChecksumMismatch extends StreamingShuffleInvalidationReason("CHECKSUM_MISMATCH")

  /** The producer sent something the streaming wire protocol does not allow. */
  case object ProtocolViolation extends StreamingShuffleInvalidationReason("PROTOCOL_VIOLATION")

  /** The stream ended short of the blocks that had been announced. */
  case object IncompleteStream extends StreamingShuffleInvalidationReason("INCOMPLETE_STREAM")

  /** The consumer was holding locations from a producer generation that has been superseded. */
  case object StaleEpoch extends StreamingShuffleInvalidationReason("STALE_EPOCH")

  /** The reported cause was absent or is not one this build knows. */
  case object Unknown extends StreamingShuffleInvalidationReason("UNKNOWN")

  /** Every reason this build knows, in the order they are documented above. */
  val values: Seq[StreamingShuffleInvalidationReason] = Seq(
    ConnectionTimeout, ChecksumMismatch, ProtocolViolation, IncompleteStream, StaleEpoch, Unknown)

  /**
   * Resolves a code to the reason it names, answering [[Unknown]] for anything else.
   *
   * @param code the code to resolve
   */
  def fromCode(code: String): StreamingShuffleInvalidationReason = {
    values.find(_.code == code).getOrElse(Unknown)
  }

  /**
   * Normalises a reason that arrived over the wire back onto one of the constants declared here.
   *
   * @param reason the reason as received, which may be `null`
   */
  def sanitize(reason: StreamingShuffleInvalidationReason): StreamingShuffleInvalidationReason = {
    if (reason == null) Unknown else fromCode(reason.code)
  }
}

/**
 * Reports that a producer generation is dead, so that the coordinator stops handing its address out
 * and advances the shuffle's epoch.
 *
 * @param shuffleId shuffle the producer streamed
 * @param capabilityToken token issued when the shuffle was registered on the driver, without
 *     which the invalidation is refused
 * @param generation generation identity of the dead producer, as carried by the location the
 *     consumer read from
 * @param reason cause of the invalidation, drawn from the closed set in
 *     [[StreamingShuffleInvalidationReason]] so that a peer cannot author driver log content
 * @param detail optional human detail.
 */
private[spark] case class InvalidateStreamingShuffleProducer(
    shuffleId: Int,
    capabilityToken: String,
    generation: StreamingShuffleProducerGeneration,
    reason: StreamingShuffleInvalidationReason,
    detail: String = null) extends StreamingShuffleCoordinatorMessage {

  /** Renders the request without disclosing the credential it carries. */
  override def toString: String =
    s"InvalidateStreamingShuffleProducer(shuffleId=$shuffleId, " +
      s"capabilityToken=$redactedToken, generation=$generation, reason=$reason, detail=$detail)"
}

/**
 * Drops all state for a shuffle.
 *
 * @param shuffleId shuffle whose state should be dropped
 * @param capabilityToken token issued when the shuffle was registered on the driver, without
 *     which the unregistration is refused
 */
private[spark] case class UnregisterStreamingShuffle(shuffleId: Int, capabilityToken: String)
  extends StreamingShuffleCoordinatorMessage {

  /** Renders the request without disclosing the credential it carries. */
  override def toString: String =
    s"UnregisterStreamingShuffle(shuffleId=$shuffleId, capabilityToken=$redactedToken)"
}

/**
 * Reports that one producer has finished streaming its whole map output, so that a consumer can
 * tell a map stage that is complete from one that has not started.
 *
 * @param shuffleId shuffle the producer streamed
 * @param capabilityToken token issued when the shuffle was registered on the driver
 * @param generation generation identity of the producer reporting completion, whose map index
 *     is the identity the completion set is keyed by
 */
private[spark] case class CompleteStreamingShuffleProducer(
    shuffleId: Int,
    capabilityToken: String,
    generation: StreamingShuffleProducerGeneration)
  extends StreamingShuffleCoordinatorMessage {

  /** Renders the request without disclosing the credential it carries. */
  override def toString: String =
    s"CompleteStreamingShuffleProducer(shuffleId=$shuffleId, " +
      s"capabilityToken=$redactedToken, generation=$generation)"
}

/**
 * Declares that a shuffle must stand streaming down for every participant, and why.
 *
 * @param shuffleId shuffle that must stand streaming down
 * @param capabilityToken token issued when the shuffle was registered on the driver, without
 *     which the declaration is refused: forcing a shuffle onto the sort-based path is a decision no
 *     stranger may drive
 * @param reasonName name of the [[StreamingShuffleStandDownCause]] that was observed, which is
 *     either one of the four specified fallback conditions or a structural decline
 * @param detail free text describing the observation, sanitized before it reaches any log
 *     record
 */
private[spark] case class DeclareStreamingShuffleFallback(
    shuffleId: Int,
    capabilityToken: String,
    reasonName: String,
    detail: String = null) extends StreamingShuffleCoordinatorMessage {

  /** Renders the request without disclosing the credential it carries. */
  override def toString: String =
    s"DeclareStreamingShuffleFallback(shuffleId=$shuffleId, " +
      s"capabilityToken=$redactedToken, reasonName=$reasonName, detail=$detail)"
}

/**
 * Asks the driver to withdraw a shuffle's streamed map output so that its map stage is recomputed,
 * '''without''' standing streaming down.
 *
 * @param shuffleId shuffle whose streamed map output must be recomputed
 * @param capabilityToken token issued when the shuffle was registered on the driver, without
 *     which the request is refused: forcing a recomputation is not a stranger's decision to take
 * @param detail free text describing what could not be resolved, sanitized before it is logged
 */
private[spark] case class InvalidateStreamingShuffleMapOutput(
    shuffleId: Int,
    capabilityToken: String,
    detail: String = null) extends StreamingShuffleCoordinatorMessage {

  /** Renders the request without disclosing the credential it carries. */
  override def toString: String =
    s"InvalidateStreamingShuffleMapOutput(shuffleId=$shuffleId, " +
      s"capabilityToken=$redactedToken, detail=$detail)"
}

/**
 * Asks whether a shuffle has stood streaming down, so that a participant can decide between the
 * streaming path and the sort-based one before it commits any state to either.
 *
 * @param shuffleId shuffle to ask about
 * @param capabilityToken token issued when the shuffle was registered on the driver
 */
private[spark] case class GetStreamingShuffleFallbackState(
    shuffleId: Int,
    capabilityToken: String) extends StreamingShuffleCoordinatorMessage {

  /** Renders the request without disclosing the credential it carries. */
  override def toString: String =
    s"GetStreamingShuffleFallbackState(shuffleId=$shuffleId, capabilityToken=$redactedToken)"
}

/**
 * Asks how many streaming shuffles are concurrently active on one executor.
 *
 * @param executorId executor asking on its own behalf
 */
private[spark] case class GetStreamingShuffleConcurrency(executorId: String)
  extends StreamingShuffleCoordinatorMessage

/** Self-addressed request that drives one pass of stale-producer reaping. */
private[spark] case object ReapStaleStreamingShuffleProducers
  extends StreamingShuffleCoordinatorMessage

/**
 * Stops the coordinator endpoint, following the stop-message precedent of the map output tracker:
 * the endpoint replies first and stops itself afterwards.
 */
private[spark] case object StopStreamingShuffleCoordinator
  extends StreamingShuffleCoordinatorMessage

/**
 * One registered producer together with the moment the coordinator last saw it alive.
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
 * @param numPartitions reduce partition count registered for the shuffle
 * @param mapStage declared cardinality of the shuffle's map stage together with the map outputs
 *     streamed to completion so far, which is what lets a consumer tell a stage that has finished
 *     from one that has not begun
 * @param protocolVersion streaming wire-protocol version registered for the shuffle
 * @param coordinatorEpoch epoch of the current producer generation, advanced whenever a
 *     producer is invalidated, replaced or reaped
 * @param capabilityToken token that authorizes operations on this shuffle.
 * @param producers live producers keyed and incrementally ordered by map task id
 * @param retiredAttempts highest task attempt id that has been invalidated or reaped, per map
 *     task id.
 * @param producerExecutors reference count of live producers per executor id, maintained
 *     incrementally by [[withProducer]] and [[withoutProducer]].
 * @param fallback whether, and why, this shuffle has stood streaming down for every
 *     participant.
 * @param lastActivityMs coordinator clock reading at the last accepted operation on the
 *     shuffle.
 */
private[spark] case class StreamingShuffleState(
    numPartitions: Int,
    mapStage: StreamingShuffleMapStage,
    protocolVersion: Byte,
    coordinatorEpoch: Long,
    capabilityToken: String,
    producers: SortedMap[Int, StreamingShuffleProducerEntry],
    retiredAttempts: Map[Int, Long] = Map.empty,
    producerExecutors: Map[String, Int] = Map.empty,
    fallback: StreamingShuffleFallbackState = StreamingShuffleFallbackState(),
    producerTimeouts: Map[Int, Int] = Map.empty,
    producerTimeoutTotal: Int = 0,
    lastActivityMs: Long = 0L) {

  /** Whether the executor has at least one live producer of this shuffle. */
  def hasProducerOn(executorId: String): Boolean = producerExecutors.contains(executorId)

  /** Whether this shuffle has stood streaming down for every participant. */
  def hasFallenBack: Boolean = fallback.fallenBack

  /** The registered generation of one map index, or `None` when it has no live producer. */
  def generationOf(mapIndex: Int): Option[StreamingShuffleProducerGeneration] =
    producers.get(mapIndex).map(_.location.generation)

  /**
   * Whether a generation has already been invalidated or reaped and must therefore never be
   * published again.
   */
  def isRetired(generation: StreamingShuffleProducerGeneration): Boolean =
    retiredAttempts.get(generation.mapIndex).exists(generation.taskAttemptId <= _)

  /**
   * How many registered producer generations of one map output a consumer has invalidated because
   * the producer stopped answering inside the connection-timeout window.
   */
  def producerTimeoutsOf(mapIndex: Int): Int = producerTimeouts.getOrElse(mapIndex, 0)

  /** This state with one more producer-liveness invalidation recorded against a map index. */
  def withProducerTimeout(mapIndex: Int): StreamingShuffleState = {
    copy(
      producerTimeouts = producerTimeouts.updated(mapIndex, producerTimeoutsOf(mapIndex) + 1),
      producerTimeoutTotal = producerTimeoutTotal + 1)
  }

  /** This state with one producer published, keeping the executor index exact. */
  def withProducer(entry: StreamingShuffleProducerEntry): StreamingShuffleState = {
    val mapIndex = entry.location.mapIndex
    val previous = producers.get(mapIndex)
    val released = previous match {
      case Some(older) => releaseExecutor(producerExecutors, older.location.executorId)
      case None => producerExecutors
    }
    val withdrawn = previous
      .filterNot(_.location.taskAttemptId == entry.location.taskAttemptId)
      .fold(mapStage) { older =>
        mapStage.withoutCompleted(mapIndex, older.location.taskAttemptId)
      }
    copy(
      producers = producers.updated(mapIndex, entry),
      producerExecutors = retainExecutor(released, entry.location.executorId),
      mapStage = withdrawn)
  }

  /**
   * This state with the producer of one map task dropped, keeping the executor index exact and
   * withdrawing whatever completion that exact generation had earned.
   */
  def withoutProducer(mapIndex: Int): StreamingShuffleState = {
    producers.get(mapIndex) match {
      case Some(previous) =>
        copy(
          producers = producers - mapIndex,
          producerExecutors = releaseExecutor(producerExecutors, previous.location.executorId),
          mapStage = mapStage.withoutCompleted(mapIndex, previous.location.taskAttemptId))
      case None => this
    }
  }

  /**
   * This state with one map output recorded as streamed to completion by the attempt that produced
   * it.
   */
  def withCompletedMap(mapIndex: Int, taskAttemptId: Long): StreamingShuffleState = {
    copy(mapStage = mapStage.withCompleted(mapIndex, taskAttemptId))
  }

  /**
   * This state with a shuffle-wide fallback latched, every completion withdrawn and the shuffle
   * advanced to `epoch`.
   */
  def withFallback(
      reasonName: String,
      epoch: Long,
      detail: String = StreamingShuffleCoordinator.NO_INVALIDATION_DETAIL)
    : StreamingShuffleState = {
    if (fallback.fallenBack) {
      this
    } else {
      copy(
        fallback = StreamingShuffleFallbackState(reasonName, epoch, detail),
        mapStage = mapStage.cleared,
        coordinatorEpoch = epoch)
    }
  }

  /** This state with a generation recorded as retired. */
  def withRetiredGeneration(
      generation: StreamingShuffleProducerGeneration): StreamingShuffleState = {
    if (isRetired(generation)) {
      this
    } else {
      copy(retiredAttempts =
        retiredAttempts.updated(generation.mapIndex, generation.taskAttemptId))
    }
  }

  /** The executor index with one producer added for an executor. */
  private def retainExecutor(
      index: Map[String, Int],
      executorId: String): Map[String, Int] = {
    index.updated(executorId, index.getOrElse(executorId, 0) + 1)
  }

  /** The executor index with one producer removed for an executor, dropping empty entries. */
  private def releaseExecutor(
      index: Map[String, Int],
      executorId: String): Map[String, Int] = {
    val remaining = index.getOrElse(executorId, 1) - 1
    if (remaining <= 0) index - executorId else index.updated(executorId, remaining)
  }

  /** Renders the registry entry without disclosing the credential it holds. */
  override def toString: String =
    s"StreamingShuffleState(numPartitions=$numPartitions, mapStage=$mapStage, " +
      s"protocolVersion=$protocolVersion, coordinatorEpoch=$coordinatorEpoch, " +
      s"capabilityToken=${StreamingShuffleCoordinator.REDACTED_TOKEN}, " +
      s"producers=${producers.size}, retiredAttempts=${retiredAttempts.size}, " +
      s"producerExecutors=${producerExecutors.size}, fallback=$fallback, " +
      s"producerTimeouts=${producerTimeouts.size}/$producerTimeoutTotal, " +
      s"lastActivityMs=$lastActivityMs)"
}

/**
 * Rendezvous point and active-shuffle registry for the streaming shuffle subsystem, hosted on the
 * driver as a named [[ThreadSafeRpcEndpoint]] and reached from executors by reference.
 *
 * A reduce task on the classic path learns block locations from the map-output tracker; a streaming
 * reader instead needs a live producer endpoint, and that is what this registry answers. It also
 * reports how many shuffles an executor is producing for, which is the divisor the egress budget is
 * split by and which no existing API exposes. It shares no state with map-output tracking, and
 * sort-based shuffle neither reads nor writes it.
 *
 * A lookup is answered from the registered producers, and registration happens when a producer
 * begins streaming rather than when it finishes, so a consumer that asks mid-production and one
 * that asks afterwards are answered by the same path; the reply carries the completion set so a
 * reader records which of the two it got instead of assuming either. Which of them calls is decided
 * by task submission, and the scheduler that decides it is unmodified.
 *
 * Every operation on a shuffle must present the capability token minted at registration, because
 * this endpoint is reachable by every peer of the application and a shuffle id is a small guessable
 * integer. The token is a bearer credential: it is compared in constant time and never written to a
 * log or a `toString`.
 *
 * @param rpcEnv the [[RpcEnv]] this endpoint is registered to
 * @param conf the Spark configuration, read once here so that a configuration change requires
 *     an executor restart rather than taking effect mid-flight
 * @param clock clock used for every liveness decision; injected so that reaping is
 *     deterministic under test
 */
private[spark] class StreamingShuffleCoordinator(
    override val rpcEnv: RpcEnv,
    conf: SparkConf,
    clock: Clock = new SystemClock)
  extends ThreadSafeRpcEndpoint with Logging {

  // Read once at construction and held immutably: the streaming shuffle subsystem has no dynamic
  // reconfiguration, so every value it depends on is fixed for the lifetime of the JVM.
  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  // The windows that bound this endpoint's two per-shuffle records.
  private val registrationLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private val unregistrationLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /** The window bounding the record made when a producer generation is invalidated or retired. */
  private val invalidationLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  // A producer that has not been seen for longer than this is treated as gone.
  private val livenessTimeoutMs: Long = StreamingShuffleCoordinator.PRODUCER_LIVENESS_TIMEOUT_MS

  /** Shuffles whose repeated producer-liveness losses have already been reported once. */
  private val repeatedTimeoutsReported: java.util.Set[Int] =
    ConcurrentHashMap.newKeySet[Int]()

  /**
   * Reports one shuffle registration change at default level at most once per window, and otherwise
   * only under the debug key.
   *
   * @param entry the record to make
   * @param aggregator the window this record is bounded by
   */
  private def reportRegistrationBounded(
      entry: => MessageWithContext,
      aggregator: MemorySpillManager.ExecutorLogAggregator): Unit = {
    aggregator.record(clock.getTimeMillis()) match {
      case Some(summary) =>
        logInfo(entry + log"; ${MDC(NUM_EVENTS, summary.occurrences)} such change(s) so far, " +
          log"${MDC(NUM_SKIPPED, summary.unreported)} of them not reported individually so that " +
          log"the driver stays inside its log budget")
      case None =>
        // At the level it was emitted at, under the feature's own key, so that setting the
        // streaming debug key restores the per-shuffle record without also reconfiguring the log
        // framework.
        if (debugEnabled) {
          logInfo(entry)
        }
    }
  }

  // Active streaming shuffles, keyed by shuffle id.
  private val shuffleStates = new ConcurrentHashMap[Int, StreamingShuffleState]()

  /** Shuffle ids with at least one producer on each executor. */
  private val executorShuffles = new ConcurrentHashMap[String, java.util.Set[Int]]()

  /** One deadline per live incomplete producer, plus one per empty non-fallback shuffle. */
  private val expiryIndex = new StreamingShuffleCoordinator.ExpiryIndex

  // Monotonic source of coordinator epochs.
  private val epochCounter = new AtomicLong(StreamingShuffleCoordinator.NO_EPOCH)

  // Count of operations refused because they were unauthorized, malformed or over a cap.
  private val deniedOperations = new AtomicLong(0L)

  // Stand-downs whose publication barrier had to remove a map status published after the
  // withdrawal. Counted rather than only logged, because it is the one observable trace of a race
  // between a producer's stop sequence and a shuffle-wide withdrawal.
  private val withdrawalBarriers = new AtomicLong(0L)

  // Timer that drives stale-producer reaping.
  private var reaperTask: ScheduledFuture[_] = null

  private val reaperThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("streaming-shuffle-coordinator-reaper")

  // Earliest coordinator-clock reading at which the next rejection may be reported at default
  // level, and how many rejections have gone unreported since the last one that was.
  private val nextRejectionReportMs = new AtomicLong(Long.MinValue)

  private val unreportedRejections = new AtomicLong(0L)

  private val rejectionTotal = new AtomicLong(0L)

  /** Arms the reaping timer. */
  override def onStart(): Unit = {
    reaperTask = reaperThread.scheduleAtFixedRate(
      () => Utils.tryLogNonFatalError {
        Option(self).foreach(_.ask[Int](ReapStaleStreamingShuffleProducers))
      },
      StreamingShuffleCoordinator.REAPER_INTERVAL_MS,
      StreamingShuffleCoordinator.REAPER_INTERVAL_MS,
      TimeUnit.MILLISECONDS)
  }

  /** Cancels the reaping timer, shuts its thread down and drops all registry state. */
  override def onStop(): Unit = {
    if (reaperTask != null) {
      reaperTask.cancel(true)
      reaperTask = null
    }
    reaperThread.shutdownNow()
    val dropped = shuffleStates.size()
    shuffleStates.clear()
    executorShuffles.clear()
    expiryIndex.clear()
    repeatedTimeoutsReported.clear()
    logInfo(log"StreamingShuffleCoordinator dropped ${MDC(COUNT, dropped)} active streaming " +
      log"shuffle registrations on stop")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterStreamingShuffleProducer(
        shuffleId, capabilityToken, location, numPartitions, protocolVersion, timestampMs) =>
      // registerProducer traces the registration itself, including the producer address, so only
      // the caller's clock skew is worth recording here.
      logSkewIfDebug(shuffleId, timestampMs)
      // Every field arrived from a peer.
      validateProducerRegistration(shuffleId, location, numPartitions) match {
        case Some(reason) =>
          recordPeerRejection(log"a malformed streaming shuffle producer registration for " +
            log"shuffle ${MDC(SHUFFLE_ID, shuffleId)} from " +
            log"${MDC(HOST_PORT, senderHostPort(context))}", reason)
          context.reply(declinedRegistration(shuffleId, claimedExecutorId(location)))
        case None =>
          context.reply(registerProducer(shuffleId, capabilityToken,
            bindToSender(shuffleId, location, context), numPartitions, protocolVersion))
      }

    case LookupStreamingShuffleProducers(
        shuffleId, capabilityToken, startPartition, endPartition) =>
      // Checked before the call so that a malformed range is reported to the caller without routing
      // through the endpoint's error handler.
      if (startPartition < 0 || endPartition < startPartition) {
        context.sendFailure(new IllegalArgumentException(
          s"Invalid reduce partition range [$startPartition, $endPartition) requested for " +
            s"streaming shuffle $shuffleId"))
      } else {
        logRequestIfDebug(log"a producer lookup", shuffleId, context)
        context.reply(lookupProducers(shuffleId, capabilityToken, startPartition, endPartition))
      }

    case HeartbeatStreamingShuffleProducer(
        shuffleId, capabilityToken, generation, timestampMs) =>
      logSkewIfDebug(shuffleId, timestampMs)
      validateGeneration(shuffleId, generation) match {
        case Some(reason) =>
          recordPeerRejection(log"a malformed streaming shuffle producer heartbeat for shuffle " +
            log"${MDC(SHUFFLE_ID, shuffleId)} from " +
            log"${MDC(HOST_PORT, senderHostPort(context))}", reason)
          // Reported as not live, which is the same answer an unknown generation gets, so a
          // defective producer stops assuming consumers can reach it.
          context.reply(StreamingShuffleProducerLiveness(live = false, currentEpoch(shuffleId)))
        case None =>
          context.reply(heartbeatProducer(shuffleId, capabilityToken, generation))
      }

    case InvalidateStreamingShuffleProducer(
        shuffleId, capabilityToken, generation, reason, detail) =>
      validateGeneration(shuffleId, generation) match {
        case Some(violation) =>
          recordPeerRejection(log"a malformed streaming shuffle producer invalidation for " +
            log"shuffle ${MDC(SHUFFLE_ID, shuffleId)} from " +
            log"${MDC(HOST_PORT, senderHostPort(context))}", violation)
          context.reply(currentEpoch(shuffleId))
        case None =>
          context.reply(
            invalidateProducer(shuffleId, capabilityToken, generation, reason, detail))
      }

    case UnregisterStreamingShuffle(shuffleId, capabilityToken) =>
      context.reply(unregisterShuffle(shuffleId, capabilityToken))

    case CompleteStreamingShuffleProducer(shuffleId, capabilityToken, generation) =>
      validateGeneration(shuffleId, generation) match {
        case Some(violation) =>
          recordPeerRejection(log"a malformed streaming shuffle producer completion for shuffle " +
            log"${MDC(SHUFFLE_ID, shuffleId)} from " +
            log"${MDC(HOST_PORT, senderHostPort(context))}", violation)
          context.reply(false)
        case None =>
          context.reply(completeProducer(shuffleId, capabilityToken, generation))
      }

    case DeclareStreamingShuffleFallback(shuffleId, capabilityToken, reasonName, detail) =>
      // The cause is resolved against this build's closed set before anything is stored or logged.
      StreamingShuffleStandDownCause.fromName(reasonName) match {
        case Some(cause) if shuffleId >= 0 =>
          context.reply(declareFallback(shuffleId, capabilityToken, cause, detail))
        case Some(_) =>
          recordPeerRejection(log"a streaming shuffle fallback declaration from " +
            log"${MDC(HOST_PORT, senderHostPort(context))}",
            s"the shuffle id $shuffleId is negative")
          context.reply(StreamingShuffleFallbackState())
        case None =>
          recordPeerRejection(log"a streaming shuffle fallback declaration for shuffle " +
            log"${MDC(SHUFFLE_ID, shuffleId)} from " +
            log"${MDC(HOST_PORT, senderHostPort(context))}",
            "the stand-down cause is not one this build recognises")
          context.reply(StreamingShuffleFallbackState())
      }

    case InvalidateStreamingShuffleMapOutput(shuffleId, capabilityToken, detail) =>
      if (shuffleId < 0) {
        recordPeerRejection(log"a streaming shuffle map output invalidation from " +
          log"${MDC(HOST_PORT, senderHostPort(context))}",
          s"the shuffle id $shuffleId is negative")
        context.reply(false)
      } else {
        logRequestIfDebug(log"a streamed map output invalidation", shuffleId, context)
        val withdrawn = invalidateStreamedMapOutput(shuffleId, capabilityToken)
        if (withdrawn) {
          logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} will recompute its map " +
            log"stage after a consumer reported that it could not be read: " +
            log"${MDC(REASON, StreamingShuffleCoordinator.sanitizeDetail(detail))}")
        }
        context.reply(withdrawn)
      }

    case GetStreamingShuffleFallbackState(shuffleId, capabilityToken) =>
      if (shuffleId < 0) {
        recordPeerRejection(log"a streaming shuffle fallback query from " +
          log"${MDC(HOST_PORT, senderHostPort(context))}",
          s"the shuffle id $shuffleId is negative")
        context.reply(StreamingShuffleFallbackState())
      } else {
        logRequestIfDebug(log"a shuffle fallback query", shuffleId, context)
        context.reply(fallbackStateFor(shuffleId, capabilityToken))
      }

    case GetStreamingShuffleConcurrency(executorId) =>
      // The executor id is peer supplied, so it is validated before it is compared against stored
      // ids or written to a log record.
      identifierRejection("executor id", executorId) match {
        case Some(rejection) =>
          recordPeerRejection(log"a streaming shuffle concurrency query", rejection)
          context.reply(1)
        case None =>
          val concurrency = numConcurrentShufflesFor(executorId)
          if (debugEnabled) {
            logDebug(log"Streaming shuffle concurrency for executor " +
              log"${MDC(EXECUTOR_ID, executorId)} is ${MDC(COUNT, concurrency)}")
          }
          context.reply(concurrency)
      }

    case ReapStaleStreamingShuffleProducers =>
      // Maintenance, so it is accepted only from this JVM.
      if (isLocalSender(context)) {
        context.reply(reapStaleProducers())
      } else {
        denyRemoteControl("a reaping pass", context)
        context.sendFailure(new SecurityException(
          "Streaming shuffle reaping may only be requested from the driver JVM"))
      }

    case StopStreamingShuffleCoordinator =>
      // Lifecycle, so it is accepted only from this JVM.
      if (isLocalSender(context)) {
        logInfo("StreamingShuffleCoordinator stopped!")
        context.reply(true)
        stop()
      } else {
        denyRemoteControl("a coordinator stop", context)
        context.sendFailure(new SecurityException(
          "The streaming shuffle coordinator may only be stopped from the driver JVM"))
      }
  }

  /**
   * Registers a streaming shuffle and returns the epoch stamped on its first producer generation,
   * or `None` when the coordinator declines the shuffle.
   *
   * @param shuffleId shuffle to register
   * @param numPartitions reduce partition count of the shuffle; must be positive
   * @param numMaps number of map tasks in the producing stage; must be non-negative, and zero
   *     is a legitimately empty map stage
   * @param protocolVersion streaming wire-protocol version the caller speaks
   * @return the shuffle's epoch, capability token and fallback state, or `None` if the shuffle
   *     was declined
   */
  def registerShuffle(
      shuffleId: Int,
      numPartitions: Int,
      numMaps: Int,
      protocolVersion: Byte): Option[StreamingShuffleRegistrationGrant] = {
    require(numPartitions > 0,
      s"numPartitions must be positive for shuffle $shuffleId, but was $numPartitions")
    require(numMaps >= 0,
      s"numMaps must be non-negative for shuffle $shuffleId, but was $numMaps")
    if (protocolVersion != StreamingShuffleCoordinator.CURRENT_PROTOCOL_VERSION) {
      recordRegistrationRejection(shuffleId, None, protocolMismatchReason(protocolVersion))
      None
    } else {
      var created = false
      var overCap = false
      val nowMs = clock.getTimeMillis()
      val state = shuffleStates.compute(shuffleId,
        (_: Int, existing: StreamingShuffleState) => {
          val updated = if (existing == null) {
            // The cap is evaluated inside the atomic update so that the decision and the insertion
            // it guards cannot be separated.
            if (shuffleStates.size() >= StreamingShuffleCoordinator.MAX_REGISTERED_SHUFFLES) {
              overCap = true
              null
            } else {
              created = true
              StreamingShuffleState(numPartitions, StreamingShuffleMapStage(numMaps),
                protocolVersion, epochCounter.incrementAndGet(),
                StreamingShuffleCoordinator.newCapabilityToken(), SortedMap.empty,
                lastActivityMs = nowMs)
            }
          } else {
            existing.copy(lastActivityMs = nowMs)
          }
          if (updated != null) {
            refreshEmptyShuffleExpiry(shuffleId, updated)
          }
          updated
        })
      if (overCap) {
        // The registry is full, so nothing was inserted and there is no state to read below.
        deniedOperations.incrementAndGet()
        logWarning(log"Declining streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)}: the " +
          log"coordinator already holds ${MDC(COUNT, shuffleStates.size())} registered streaming " +
          log"shuffles, " +
          log"which is its maximum of " +
          log"${MDC(MAX_SIZE, StreamingShuffleCoordinator.MAX_REGISTERED_SHUFFLES)}; the shuffle " +
          log"will fall back to sort-based shuffle")
        None
      } else if (state.numPartitions != numPartitions) {
        recordRegistrationRejection(shuffleId, None,
          partitionMismatchReason(numPartitions, state.numPartitions))
        None
      } else if (state.mapStage.numMaps != numMaps) {
        // A re-registration that contradicts the recorded cardinality would make every consumer's
        // completion test meaningless, so it is declined rather than accommodated, exactly as a
        // contradictory partition count is.
        recordRegistrationRejection(shuffleId, None,
          mapCountMismatchReason(numMaps, state.mapStage.numMaps))
        None
      } else {
        if (created) {
          // Bounded on a window rather than emitted per shuffle.
          reportRegistrationBounded(
            log"Registered streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} with " +
              log"${MDC(NUM_PARTITIONS, numPartitions)} reduce partitions and " +
              log"${MDC(NUM_TASKS, numMaps)} map task(s) at epoch " +
              log"${MDC(EPOCH, state.coordinatorEpoch)}",
            registrationLogAggregator)
        } else if (debugEnabled) {
          // Info-level logging stays proportional to the number of shuffles, so a repeated,
          // idempotent registration is recorded only under the debug gate.
          logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} is already registered at " +
            log"epoch ${MDC(EPOCH, state.coordinatorEpoch)}; reusing it")
        }
        // The token of an idempotent re-registration is the one already held, never a fresh one, so
        // a handle minted by an earlier call keeps working and a caller cannot rotate another
        // caller's token out from under it.
        Some(StreamingShuffleRegistrationGrant(state.coordinatorEpoch, state.capabilityToken,
          state.fallback))
      }
    }
  }

  /**
   * Registers, or re-registers, one producer of a shuffle and answers with the state the producer
   * needs: whether it was accepted, the epoch of its generation, the protocol version the
   * coordinator speaks and the current active-shuffle count.
   *
   * @param shuffleId shuffle the producer streams; must be non-negative
   * @param location where the producer can be reached, and which generation it is; must carry a
   *     well-formed executor id, host, port and map id
   * @param numPartitions reduce partition count the producer streams to; must be positive
   * @param protocolVersion streaming wire-protocol version the producer speaks
   * @return the registration outcome, never `null`; `accepted = false` for a declined
   *     registration
   */
  def registerProducer(
      shuffleId: Int,
      capabilityToken: String,
      location: StreamingShuffleProducerLocation,
      numPartitions: Int,
      protocolVersion: Byte): StreamingShuffleProducerRegistration = {
    validateProducerRegistration(shuffleId, location, numPartitions).foreach { reason =>
      throw new IllegalArgumentException(
        s"Invalid streaming shuffle producer registration for shuffle $shuffleId: $reason")
    }
    val generation = location.generation
    val expected = StreamingShuffleCoordinator.CURRENT_PROTOCOL_VERSION
    val rejection = registrationRejection(shuffleId, location, numPartitions, protocolVersion)
    if (rejection.isDefined) {
      // The map id is rendered only once the location itself is known to be well formed, so a
      // rejected registration never causes an unvalidated field to be read for a log record, and
      // the report is bounded so a retrying peer cannot choose the driver's log volume.
      recordRegistrationRejection(shuffleId, mapIdIfValid(location), rejection.get)
      declinedRegistration(shuffleId, location.executorId)
    } else {
      val nowMs = clock.getTimeMillis()
      // The admission decision and the publication happen in one atomic update, and the outcome is
      // read out afterwards rather than recomputed, so that a concurrent registration of another
      // generation can never be observed between the two.
      var declineReason: Option[String] = None
      var denied = false
      var known = false
      // Distinguishes "this attempt is a zombie" from "this shuffle cannot be streamed", which are
      // the two rejections the caller has to answer in opposite ways.
      var generationStale = false
      var epoch = StreamingShuffleCoordinator.NO_EPOCH
      // computeIfPresent, never compute: a producer registration must not be able to create a
      // shuffle's state.
      shuffleStates.computeIfPresent(shuffleId,
        (_: Int, existing: StreamingShuffleState) => {
          known = true
          val registered = existing.generationOf(generation.mapIndex)
          if (unauthorizedFor(existing, capabilityToken)) {
            // Declined without disclosing anything else about the shuffle -- not its epoch, not its
            // partition count -- so an unauthorized caller learns only that it was refused.
            declineReason = Some(unauthorizedReason)
            denied = true
            existing
          } else if (existing.numPartitions != numPartitions) {
            epoch = existing.coordinatorEpoch
            // Left untouched so that a contradictory registration cannot corrupt a live shuffle;
            // the mismatch is turned into a rejection below.
            declineReason =
              Some(partitionMismatchReason(numPartitions, existing.numPartitions))
            existing
          } else if (existing.hasFallenBack) {
            epoch = existing.coordinatorEpoch
            // Once a shuffle has stood streaming down, every later producer of it must take the
            // sort-based path, including the producers a recomputation of the map stage creates.
            declineReason = Some(fallbackDeclaredReason(existing.fallback))
            existing
          } else if (existing.isRetired(generation)) {
            epoch = existing.coordinatorEpoch
            declineReason = Some(retiredGenerationReason(generation))
            generationStale = true
            existing
          } else if (registered.exists(_.taskAttemptId > generation.taskAttemptId)) {
            epoch = existing.coordinatorEpoch
            declineReason =
              Some(supersededGenerationReason(generation, registered.get.taskAttemptId))
            generationStale = true
            existing
          } else if (registered.isEmpty &&
              existing.producers.size >= StreamingShuffleCoordinator.MAX_PRODUCERS_PER_SHUFFLE) {
            epoch = existing.coordinatorEpoch
            // A producer that is already registered may always refresh itself, so only a new
            // producer is refused at the ceiling: reaching it degrades further producers to
            // sort-based shuffle instead of disturbing the ones already streaming.
            declineReason = Some(producerCapReason)
            denied = true
            existing
          } else {
            // A strictly newer generation taking a map task's registration over makes the address
            // every consumer already holds for that map task stale, so the shuffle's epoch advances
            // with the replacement and a consumer comparing epochs can tell that its locations
            // predate the current generation.
            val replacesOlder = registered.exists(_.taskAttemptId < generation.taskAttemptId)
            epoch = if (replacesOlder) {
              epochCounter.incrementAndGet()
            } else {
              existing.coordinatorEpoch
            }
            // An accepted registration is activity, so it defers the empty-state eviction that
            // would otherwise drop a registration whose producers were all reaped.
            val entry = StreamingShuffleProducerEntry(stamp(location, epoch), nowMs)
            val published = existing
              .withProducer(entry)
              .copy(lastActivityMs = nowMs)
            val updated =
              if (replacesOlder) published.copy(coordinatorEpoch = epoch) else published
            producerPublished(shuffleId, existing, updated, entry)
            updated
          }
        })
      if (!known) {
        declineReason = Some(unregisteredShuffleReason)
        denied = true
      }
      declineReason match {
        case Some(reason) =>
          if (denied) {
            // Refusals a peer can provoke deliberately -- a wrong token, an unregistered shuffle, a
            // shuffle already at its producer ceiling -- are counted as denied operations and
            // explained only under the debug gate, so the report itself is never an amplifier.
            denyOperation("a producer registration", shuffleId, reason)
          } else {
            recordRegistrationRejection(shuffleId, Some(generation.mapId), reason)
          }
          declinedRegistration(shuffleId, location.executorId, generationStale)
        case None =>
          if (debugEnabled) {
            logDebug(log"Registered streaming shuffle producer for shuffle " +
              log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, generation.mapId)} attempt " +
              log"${MDC(TASK_ATTEMPT_ID, generation.taskAttemptId)} on executor " +
              log"${MDC(EXECUTOR_ID, location.executorId)} at " +
              log"${MDC(HOST_PORT, location.hostPort)} in epoch ${MDC(EPOCH, epoch)}")
          }
          // The count is read after publication, so an executor registering its first producer of a
          // shuffle already sees that shuffle counted, and it is scoped to the registering executor
          // because that is what its own token bucket divides by.
          StreamingShuffleProducerRegistration(shuffleId, accepted = true, epoch, expected,
            numConcurrentShufflesFor(location.executorId))
      }
    }
  }

  /**
   * Refreshes the liveness of one producer generation against the coordinator's own clock.
   *
   * @param shuffleId shuffle the producer streams; must be non-negative
   * @param generation generation identity of the heartbeating producer
   * @return whether the generation is registered and was refreshed, together with the epoch
   *     that is current for the shuffle
   */
  def heartbeatProducer(
      shuffleId: Int,
      capabilityToken: String,
      generation: StreamingShuffleProducerGeneration): StreamingShuffleProducerLiveness = {
    validateGeneration(shuffleId, generation).foreach { reason =>
      throw new IllegalArgumentException(
        s"Invalid streaming shuffle producer heartbeat for shuffle $shuffleId: $reason")
    }
    val nowMs = clock.getTimeMillis()
    var live = false
    var unauthorized = false
    var epoch = StreamingShuffleCoordinator.NO_EPOCH
    shuffleStates.computeIfPresent(shuffleId,
      (_: Int, existing: StreamingShuffleState) => {
        if (unauthorizedFor(existing, capabilityToken)) {
          // Neither refreshed nor answered with the shuffle's epoch: a caller without the token can
          // learn nothing about the shuffle by heartbeating at it.
          unauthorized = true
          existing
        } else {
          epoch = existing.coordinatorEpoch
          existing.producers.get(generation.mapIndex) match {
            case Some(entry) if entry.location.generation == generation =>
              live = true
              // Republished through the same path every other registration takes, so the executor
              // index has exactly one maintainer, and an accepted heartbeat also defers the
              // empty-state eviction.
              val refreshed = entry.copy(lastSeenMs = nowMs)
              val updated = existing.withProducer(refreshed).copy(lastActivityMs = nowMs)
              producerPublished(shuffleId, existing, updated, refreshed)
              updated
            case _ =>
              existing
          }
        }
      })
    if (unauthorized) {
      denyOperation("a producer heartbeat", shuffleId, unauthorizedReason)
      return StreamingShuffleProducerLiveness(live = false, StreamingShuffleCoordinator.NO_EPOCH)
    }
    if (debugEnabled) {
      if (live) {
        logDebug(log"Refreshed the liveness of the streaming shuffle producer for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, generation.mapId)} attempt " +
          log"${MDC(TASK_ATTEMPT_ID, generation.taskAttemptId)}")
      } else {
        logDebug(log"Ignoring a heartbeat for the unregistered streaming shuffle producer " +
          log"generation of shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
          log"${MDC(MAP_ID, generation.mapId)} attempt " +
          log"${MDC(TASK_ATTEMPT_ID, generation.taskAttemptId)}")
      }
    }
    StreamingShuffleProducerLiveness(live, epoch)
  }

  /**
   * Returns every producer currently streaming a shuffle, or `None` when the shuffle has no
   * registration yet.
   *
   * @param shuffleId shuffle to look up
   * @param startPartition first reduce partition the consumer will read, inclusive
   * @param endPartition last reduce partition the consumer will read, exclusive
   * @return the live producer locations plus the shuffle's registration state, or `None`
   */
  def lookupProducers(
      shuffleId: Int,
      capabilityToken: String,
      startPartition: Int,
      endPartition: Int): Option[StreamingShuffleProducerLocations] = {
    require(startPartition >= 0,
      s"startPartition must be non-negative for shuffle $shuffleId, but was $startPartition")
    require(endPartition >= startPartition,
      s"endPartition $endPartition must not precede startPartition $startPartition for " +
        s"shuffle $shuffleId")
    var unauthorized = false
    val nowMs = clock.getTimeMillis()
    // An authorized lookup counts as activity, so this is a compute rather than a plain get.
    val state = shuffleStates.computeIfPresent(shuffleId,
      (_: Int, existing: StreamingShuffleState) =>
        if (unauthorizedFor(existing, capabilityToken)) {
          unauthorized = true
          existing
        } else {
          val updated = existing.copy(lastActivityMs = nowMs)
          refreshEmptyShuffleExpiry(shuffleId, updated)
          updated
        })
    if (state == null) {
      None
    } else if (unauthorized) {
      // Answered as `None` rather than as a distinguishable failure, so that an unauthorized caller
      // cannot use the shape of the reply to learn whether a given shuffle id exists.
      denyOperation("a producer lookup", shuffleId, "the capability token does not match")
      None
    } else {
      // Every producer of a shuffle streams every reduce partition, so the range does not filter
      // the reply.
      require(endPartition <= state.numPartitions,
        s"endPartition $endPartition exceeds the ${state.numPartitions} reduce partitions " +
          s"registered for streaming shuffle $shuffleId")
      Some(StreamingShuffleProducerLocations(shuffleId, orderedLocations(state),
        state.numPartitions, state.mapStage.numMaps, state.mapStage.completedIndexes,
        state.protocolVersion, state.coordinatorEpoch, state.fallback))
    }
  }

  /**
   * Records that one producer has streamed its whole map output to completion.
   *
   * @param shuffleId shuffle the producer streamed; must be non-negative
   * @param capabilityToken token issued when the shuffle was registered on the driver
   * @param generation generation identity of the producer reporting completion; its map index
   *     is what the completion set is keyed by, and is validated with the rest of the generation
   *     because it keys a map that lives as long as the shuffle
   * @return `true` when the report was applied
   */
  def completeProducer(
      shuffleId: Int,
      capabilityToken: String,
      generation: StreamingShuffleProducerGeneration): Boolean = {
    validateGeneration(shuffleId, generation).foreach { violation =>
      throw new IllegalArgumentException(
        s"Invalid streaming shuffle producer completion for shuffle $shuffleId: $violation")
    }
    val mapIndex = generation.mapIndex
    var unauthorized = false
    var applied = false
    val nowMs = clock.getTimeMillis()
    shuffleStates.computeIfPresent(shuffleId,
      (_: Int, existing: StreamingShuffleState) => {
        if (unauthorizedFor(existing, capabilityToken)) {
          unauthorized = true
          existing
        } else if (existing.isRetired(generation) ||
            !existing.generationOf(generation.mapIndex).contains(generation)) {
          existing
        } else {
          applied = true
          expiryIndex.cancel(
            StreamingShuffleCoordinator.ProducerExpiry(shuffleId, generation))
          existing.withCompletedMap(mapIndex, generation.taskAttemptId)
            .copy(lastActivityMs = nowMs)
        }
      })
    if (unauthorized) {
      denyOperation("a producer completion report", shuffleId, unauthorizedReason)
      false
    } else {
      if (debugEnabled) {
        val verdict = if (applied) "Recorded" else "Ignored"
        logDebug(log"${MDC(STATUS, verdict)} a streaming shuffle producer completion for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, generation.mapId)} index " +
          log"${MDC(INDEX, mapIndex)} attempt ${MDC(TASK_ATTEMPT_ID, generation.taskAttemptId)}")
      }
      applied
    }
  }

  /**
   * Declares that a shuffle must stand streaming down for every participant, and answers with the
   * state that is in force once the declaration has been processed.
   *
   * @param shuffleId shuffle that must stand streaming down; must be non-negative
   * @param capabilityToken token issued when the shuffle was registered on the driver
   * @param cause what was observed -- one of the four specified fallback conditions, or a
   *     structural decline -- recorded by its stable name
   * @param detail free text describing the observation, sanitized before it reaches a log
   *     record
   * @return the fallback state in force once the declaration has been processed
   */
  def declareFallback(
      shuffleId: Int,
      capabilityToken: String,
      cause: StreamingShuffleStandDownCause,
      detail: String = null): StreamingShuffleFallbackState = {
    require(cause != null, s"a stand-down cause is required for streaming shuffle $shuffleId")
    latchStandDown(shuffleId, capabilityToken, cause.toString, cause.description, detail)
  }

  /**
   * Latches one shuffle-wide stand-down under a record name this build is willing to store.
   *
   * @param reasonName the record name, already one this build recognises
   * @param reasonDescription prose for that name, used in the one operator record this emits
   */
  private def latchStandDown(
      shuffleId: Int,
      capabilityToken: String,
      reasonName: String,
      reasonDescription: String,
      detail: String): StreamingShuffleFallbackState = {
    require(shuffleId >= 0,
      s"shuffleId must be non-negative for a streaming shuffle fallback, but was $shuffleId")
    require(StreamingShuffleFallbackReason.isDeclarable(reasonName),
      s"$reasonName is not a streaming shuffle stand-down record this build will store for " +
        s"shuffle $shuffleId")
    val recordedDetail = StreamingShuffleCoordinator.sanitizeDetail(detail)
    // AUTHORIZATION IS THE FIRST STEP, and it is first because the step after it mutates state
    // outside this registry.
    val resolved = Option(shuffleStates.get(shuffleId))
    if (resolved.isEmpty) {
      // An unknown shuffle has no state to stand down, and creating some would let any peer mint a
      // registry entry for any integer it named.
      if (debugEnabled) {
        logDebug(log"Ignoring a streaming shuffle fallback declaration naming an unknown shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)}: ${MDC(NEW_VALUE, reasonName)}")
      }
      return StreamingShuffleFallbackState()
    }
    if (resolved.exists(state => unauthorizedFor(state, capabilityToken))) {
      denyOperation("a shuffle fallback declaration", shuffleId, unauthorizedReason)
      return StreamingShuffleFallbackState()
    }
    // The withdrawal is a PRECONDITION of the transition rather than a step after it, and that
    // ordering is the whole of what makes a latched verdict actionable.
    val alreadyStoodDown = resolved.exists(_.hasFallenBack)
    val withdrawn = alreadyStoodDown || invalidateStreamedMapOutput(shuffleId)
    var unauthorized = false
    var declared = false
    var droppedProducers = 0
    var epoch = StreamingShuffleCoordinator.NO_EPOCH
    var state = StreamingShuffleFallbackState()
    val nowMs = clock.getTimeMillis()
    if (!withdrawn) {
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} was NOT stood down for " +
        log"${MDC(REASON, reasonDescription)} (${MDC(DESCRIPTION, recordedDetail)}): its " +
        log"streamed map output could not be withdrawn from the tracker, so a stand-down would " +
        log"leave participants delegating to the sort-based path while the map stage still " +
        log"reported streamed output as available. The shuffle keeps streaming and the " +
        log"participant that observed the condition fails so that it is retried")
      return StreamingShuffleFallbackState()
    }
    shuffleStates.computeIfPresent(shuffleId,
      (_: Int, existing: StreamingShuffleState) => {
        if (unauthorizedFor(existing, capabilityToken)) {
          // The comparison is made a second time, and inside the transition rather than beside it,
          // because the state authorized above is not necessarily the state being mutated here: a
          // shuffle can be unregistered and re-registered in the interval, and a re-registration
          // mints a new token.
          unauthorized = true
          existing
        } else if (existing.hasFallenBack) {
          state = existing.fallback
          val updated = existing.copy(lastActivityMs = nowMs)
          refreshEmptyShuffleExpiry(shuffleId, updated)
          updated
        } else {
          declared = true
          droppedProducers = existing.producers.size
          epoch = epochCounter.incrementAndGet()
          state = StreamingShuffleFallbackState(reasonName, epoch, recordedDetail)
          // Every live producer is dropped and its generation retired in the same update that
          // latches the cause, so there is no interleaving in which a producer survives the
          // declaration or re-registers immediately after it.
          val updated = existing.producers.values
            .foldLeft(existing) { (accumulated, entry) =>
              accumulated.withoutProducer(entry.location.mapIndex)
                .withRetiredGeneration(entry.location.generation)
            }
            .withFallback(reasonName, epoch, recordedDetail)
            .copy(lastActivityMs = nowMs)
          allProducersRemoved(shuffleId, existing, updated)
          updated
        }
      })
    if (unauthorized) {
      denyOperation("a shuffle fallback declaration", shuffleId, unauthorizedReason)
      StreamingShuffleFallbackState()
    } else if (declared) {
      // The publication barrier, and the reason the withdrawal is performed twice.
      sealWithdrawalAfterLatch(shuffleId, epoch)
      // One record per shuffle for the life of the application, because the state latches.
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} has stood streaming down " +
        log"for every participant at epoch ${MDC(EPOCH, epoch)}: " +
        log"${MDC(REASON, reasonDescription)} (${MDC(DESCRIPTION, recordedDetail)}). " +
        log"${MDC(COUNT, droppedProducers)} live producer(s) were invalidated; the shuffle " +
        log"will be recomputed on the sort-based path")
      state
    } else {
      if (debugEnabled) {
        val ignoredBecause =
          if (state.fallenBack) "a shuffle that had already stood down" else "an unknown shuffle"
        logDebug(log"Ignoring a streaming shuffle fallback declaration naming " +
          log"${MDC(REASON, ignoredBecause)} for shuffle ${MDC(SHUFFLE_ID, shuffleId)}: " +
          log"${MDC(NEW_VALUE, reasonName)}")
      }
      state
    }
  }

  /**
   * Closes the interval between the withdrawal that precedes a stand-down and the latch itself, by
   * withdrawing once more now that no generation of the shuffle can stream or complete.
   *
   * @param shuffleId the shuffle that has just latched a stand-down
   * @param epoch the epoch the latch advanced to, for the record only
   */
  private[streaming] def sealWithdrawalAfterLatch(shuffleId: Int, epoch: Long): Unit = {
    val available = registeredMapOutputCount(shuffleId)
    if (available > 0) {
      withdrawalBarriers.incrementAndGet()
      val outcome = if (invalidateStreamedMapOutput(shuffleId)) {
        "is confirmed"
      } else {
        "could NOT be confirmed, so a delegated reduce attempt may need one further fetch failure"
      }
      // Reported at warning level because it is evidence of the race rather than routine: an
      // operator reading it learns that a map status was published while the shuffle was standing
      // down, which is exactly the condition an unexplained extra stage attempt would come from.
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} had " +
        log"${MDC(COUNT, available)} map output(s) registered when it latched its stand-down at " +
        log"epoch ${MDC(EPOCH, epoch)}, published while the withdrawal was in progress; the " +
        log"withdrawal was therefore taken again and ${MDC(STATUS, outcome)}")
    } else if (debugEnabled) {
      logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} held no registered map output " +
        log"when it latched its stand-down at epoch ${MDC(EPOCH, epoch)}, so nothing was " +
        log"published inside the withdrawal interval")
    }
  }

  /**
   * How many map outputs of a shuffle the driver's tracker currently reports as available.
   *
   * @param shuffleId the shuffle to count
   * @return the number of registered map outputs, or zero when it cannot be established
   */
  private def registeredMapOutputCount(shuffleId: Int): Int = {
    try {
      Option(SparkEnv.get).map(_.mapOutputTracker) match {
        case Some(master: MapOutputTrackerMaster) if master.containsShuffle(shuffleId) =>
          master.getNumAvailableOutputs(shuffleId)
        case _ => 0
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"The registered map output of streaming shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} could not be counted", e)
        0
    }
  }

  /**
   * Withdraws every map output of a shuffle from the driver's map-output tracker, and reports
   * whether the withdrawal is '''confirmed'''.
   *
   * @param shuffleId the shuffle whose streamed map output is no longer valid to read
   * @return true when no streamed map output of this shuffle remains registered
   */
  private def invalidateStreamedMapOutput(shuffleId: Int): Boolean = {
    try {
      Option(SparkEnv.get).map(_.mapOutputTracker) match {
        case Some(master: MapOutputTrackerMaster) if master.containsShuffle(shuffleId) =>
          master.unregisterAllMapAndMergeOutput(shuffleId)
          logInfo(log"Withdrew every registered map output of streaming shuffle " +
            log"${MDC(SHUFFLE_ID, shuffleId)} so that its map stage is recomputed")
          true
        case _ =>
          if (debugEnabled) {
            logDebug(log"No driver map-output registration to withdraw for streaming shuffle " +
              log"${MDC(SHUFFLE_ID, shuffleId)}")
          }
          true
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"The map output of streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} could " +
          log"not be withdrawn from the tracker, so nothing may act as though its map stage will " +
          log"be recomputed", e)
        false
    }
  }

  /**
   * Withdraws a shuffle's streamed map output so that its map stage is recomputed, '''without'''
   * standing streaming down.
   *
   * @param shuffleId shuffle whose streamed map output must be recomputed; must be non-negative
   * @param capabilityToken token issued when the shuffle was registered on the driver
   * @return true when no streamed map output of this shuffle remains registered, so the caller
   *     may raise a fetch failure knowing the map stage will be recomputed
   */
  def invalidateStreamedMapOutput(shuffleId: Int, capabilityToken: String): Boolean = {
    require(shuffleId >= 0,
      s"shuffleId must be non-negative for a streamed map output invalidation, but was $shuffleId")
    val existing = Option(shuffleStates.get(shuffleId))
    if (existing.exists(state => unauthorizedFor(state, capabilityToken))) {
      denyOperation("a streamed map output invalidation", shuffleId, unauthorizedReason)
      false
    } else if (existing.isEmpty) {
      // Nothing is registered, so nothing streamed can be read and there is nothing to withdraw.
      true
    } else {
      val withdrawn = invalidateStreamedMapOutput(shuffleId)
      if (withdrawn) {
        var retired = 0
        val nowMs = clock.getTimeMillis()
        shuffleStates.computeIfPresent(shuffleId,
          (_: Int, state: StreamingShuffleState) => {
            retired = state.producers.size
            state.producers.values
              .foldLeft(state) { (accumulated, entry) =>
                accumulated.withoutProducer(entry.location.mapIndex)
                  .withRetiredGeneration(entry.location.generation)
              }
              .copy(lastActivityMs = nowMs)
          })
        logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} withdrew its streamed map " +
          log"output and retired ${MDC(COUNT, retired)} live producer generation(s) so that the " +
          log"map stage is recomputed; streaming remains in force for the recomputed attempts")
      }
      withdrawn
    }
  }

  /**
   * The fallback state in force for a shuffle, or the streaming-in-force value when the shuffle has
   * no registration or the caller cannot present its token.
   *
   * @param shuffleId shuffle to read
   * @param capabilityToken token issued when the shuffle was registered on the driver
   * @return the fallback state in force
   */
  def fallbackStateFor(
      shuffleId: Int,
      capabilityToken: String): StreamingShuffleFallbackState = {
    val state = shuffleStates.get(shuffleId)
    if (state == null) {
      StreamingShuffleFallbackState()
    } else if (unauthorizedFor(state, capabilityToken)) {
      denyOperation("a shuffle fallback query", shuffleId, unauthorizedReason)
      StreamingShuffleFallbackState()
    } else {
      state.fallback
    }
  }

  /**
   * Forgets one producer generation and advances the shuffle's epoch, so that no later lookup can
   * hand the dead address out and every consumer holding an older epoch can tell that its locations
   * are stale.
   *
   * @param shuffleId shuffle the producer streamed; must be non-negative
   * @param generation generation identity of the dead producer
   * @param reason cause of the invalidation; `null` and anything this build does not recognise
   *     are recorded as [[StreamingShuffleInvalidationReason.Unknown]]
   * @return the shuffle's epoch after the invalidation, or
   *     [[StreamingShuffleCoordinator.NO_EPOCH]] when the shuffle is unknown
   */
  def invalidateProducer(
      shuffleId: Int,
      capabilityToken: String,
      generation: StreamingShuffleProducerGeneration,
      reason: StreamingShuffleInvalidationReason,
      detail: String = null): Long = {
    validateGeneration(shuffleId, generation).foreach { violation =>
      throw new IllegalArgumentException(
        s"Invalid streaming shuffle producer invalidation for shuffle $shuffleId: $violation")
    }
    val recordedReason = StreamingShuffleInvalidationReason.sanitize(reason).code
    // Caller-supplied detail is rendered once, here, and only ever reaches a log record through
    // this rendering: control characters become spaces so a peer cannot forge a log line, and the
    // result is length-capped so it cannot choose the driver's log volume.
    val recordedDetail = StreamingShuffleCoordinator.sanitizeDetail(detail)
    var epoch = StreamingShuffleCoordinator.NO_EPOCH
    var known = false
    var unauthorized = false
    var invalidated = false
    var newlyRetired = false
    var registeredAttemptId = Option.empty[Long]
    // Whether this invalidation is a producer-liveness timeout, which is the only kind the per-map
    // and shuffle-wide tallies count.
    val timedOut =
      StreamingShuffleInvalidationReason.sanitize(reason) ==
        StreamingShuffleInvalidationReason.ConnectionTimeout
    shuffleStates.computeIfPresent(shuffleId,
      (_: Int, existing: StreamingShuffleState) => {
        known = true
        if (unauthorizedFor(existing, capabilityToken)) {
          // Recomputation is expensive and this is the message that triggers it, so it is the one
          // operation a stranger must never be able to drive: without the token the generation is
          // neither retired nor invalidated and the epoch is not disclosed.
          unauthorized = true
          existing
        } else if (existing.generationOf(generation.mapIndex).contains(generation)) {
          invalidated = true
          epoch = epochCounter.incrementAndGet()
          val removed = existing.producers(generation.mapIndex)
          val withdrawn = existing.withoutProducer(generation.mapIndex)
            .withRetiredGeneration(generation)
            .copy(coordinatorEpoch = epoch)
          if (timedOut) {
            withdrawn.withProducerTimeout(generation.mapIndex)
          } else {
            withdrawn
          }
        } else {
          epoch = existing.coordinatorEpoch
          newlyRetired = !existing.isRetired(generation)
          registeredAttemptId = existing.generationOf(generation.mapIndex).map(_.taskAttemptId)
          existing.withRetiredGeneration(generation)
        }
      })
    if (unauthorized) {
      denyOperation("a producer invalidation", shuffleId, unauthorizedReason)
      return StreamingShuffleCoordinator.NO_EPOCH
    }
    if (invalidated) {
      // Bounded on a rolling window, because one of these is made per lost producer and a workload
      // losing a tenth of its tasks would otherwise spend the whole executor log budget on them.
      reportRegistrationBounded(
        log"Invalidated streaming shuffle producer for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, generation.mapId)} attempt " +
          log"${MDC(TASK_ATTEMPT_ID, generation.taskAttemptId)}; shuffle advanced to epoch " +
          log"${MDC(EPOCH, epoch)}: ${MDC(REASON, recordedReason)} " +
          log"(${MDC(DESCRIPTION, recordedDetail)})",
        invalidationLogAggregator)
    } else if (newlyRetired && registeredAttemptId.isDefined) {
      // Made once per generation, because a repeat of the same invalidation no longer advances the
      // retirement record and therefore falls through to the debug branch.
      reportRegistrationBounded(
        log"Retired the superseded streaming shuffle producer generation of shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, generation.mapId)} attempt " +
          log"${MDC(TASK_ATTEMPT_ID, generation.taskAttemptId)} without disturbing the " +
          log"registered attempt ${MDC(NEW_VALUE, registeredAttemptId.get)} at epoch " +
          log"${MDC(EPOCH, epoch)}: " +
          log"${MDC(REASON, recordedReason)}",
        invalidationLogAggregator)
    } else if (newlyRetired) {
      reportRegistrationBounded(
        log"Retired the unregistered streaming shuffle producer generation of shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, generation.mapId)} attempt " +
          log"${MDC(TASK_ATTEMPT_ID, generation.taskAttemptId)} at epoch ${MDC(EPOCH, epoch)}: " +
          log"${MDC(REASON, recordedReason)}",
        invalidationLogAggregator)
    } else if (debugEnabled) {
      val cause = if (known) "already retired generation" else "unknown shuffle"
      logDebug(log"Ignoring an invalidation naming an ${MDC(REASON, cause)} for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, generation.mapId)} attempt " +
        log"${MDC(TASK_ATTEMPT_ID, generation.taskAttemptId)}: ${MDC(NEW_VALUE, recordedReason)}")
    }
    epoch
  }

  /**
   * Producer-liveness invalidations recorded against one map output of a shuffle, or zero when the
   * shuffle is unknown.
   *
   * @param shuffleId shuffle to ask about
   * @param mapIndex map output to ask about
   * @return invalidations recorded for that map output
   */
  def producerTimeoutCount(shuffleId: Int, mapIndex: Int): Int = {
    val state = shuffleStates.get(shuffleId)
    if (state == null) 0 else state.producerTimeoutsOf(mapIndex)
  }

  /**
   * Producer-liveness invalidations recorded across one whole shuffle, or zero when the shuffle is
   * unknown.
   *
   * @param shuffleId shuffle to ask about
   * @return invalidations recorded for that shuffle
   */
  def producerTimeoutTotal(shuffleId: Int): Int = {
    val state = shuffleStates.get(shuffleId)
    if (state == null) 0 else state.producerTimeoutTotal
  }

  /**
   * Drops all state for a shuffle.
   *
   * @param shuffleId shuffle whose state should be dropped
   * @return `true` when state was present and dropped
   */
  def unregisterShuffle(shuffleId: Int, capabilityToken: String): Boolean = {
    var unauthorized = false
    var removed = false
    var producerCount = 0
    // A conditional removal, not an unconditional one: the token is checked against the very state
    // that is about to be dropped.
    shuffleStates.computeIfPresent(shuffleId,
      (_: Int, existing: StreamingShuffleState) =>
        if (unauthorizedFor(existing, capabilityToken)) {
          unauthorized = true
          existing
        } else {
          removed = true
          producerCount = existing.producers.size
          shuffleRemoved(shuffleId, existing)
          null
        })
    if (unauthorized) {
      denyOperation("a shuffle unregistration", shuffleId, "the capability token does not match")
      false
    } else if (!removed) {
      if (debugEnabled) {
        logDebug(log"Ignoring unregistration of unknown streaming shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)}")
      }
      false
    } else {
      // The repeated-timeout marker is scoped to the registration it describes, so it goes with it.
      repeatedTimeoutsReported.remove(shuffleId)
      // On a window of its own, so that a burst of registrations cannot silence the withdrawals
      // that balance them and leave a reader inferring a registered count that never comes down.
      reportRegistrationBounded(
        log"Unregistered streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} holding " +
          log"${MDC(COUNT, producerCount)} registered producers",
        unregistrationLogAggregator)
      true
    }
  }

  /**
   * Reaps only producer generations whose indexed deadlines have elapsed.
   *
   * @return dropped entries, retained-state epoch, and idle duration when the shuffle was
   *     evicted
   */
  private def reapExpiredProducerTargets(
      shuffleId: Int,
      targets: Seq[StreamingShuffleCoordinator.ProducerExpiry],
      nowMs: Long): (Seq[StreamingShuffleProducerEntry], Long, Option[Long]) = {
    val dropped = ArrayBuffer.empty[StreamingShuffleProducerEntry]
    var epoch = StreamingShuffleCoordinator.NO_EPOCH
    var evictedIdleMs = Option.empty[Long]
    shuffleStates.computeIfPresent(shuffleId,
      (_: Int, existing: StreamingShuffleState) => {
        var updated = existing
        targets.foreach { target =>
          updated.producers.get(target.generation.mapIndex) match {
            case Some(entry) if entry.location.generation == target.generation &&
                !updated.mapStage.completedBy(
                  entry.location.mapIndex, entry.location.taskAttemptId) =>
              val deadline = StreamingShuffleCoordinator.deadlineAfter(
                entry.lastSeenMs, livenessTimeoutMs)
              if (deadline < nowMs) {
                val before = updated
                updated = updated.withoutProducer(entry.location.mapIndex)
                  .withRetiredGeneration(entry.location.generation)
                producerRemoved(shuffleId, before, updated, entry)
                dropped += entry
              } else {
                // A heartbeat refreshed the state after the old deadline was polled.
                refreshProducerExpiry(shuffleId, updated, entry)
              }
            case _ =>
              // The generation was completed, replaced or removed after this target was indexed.
          }
        }
        val emptyDeadline = StreamingShuffleCoordinator.deadlineAfter(
          updated.lastActivityMs, StreamingShuffleCoordinator.EMPTY_STATE_TTL_MS)
        if (updated.producers.isEmpty && !updated.hasFallenBack && emptyDeadline < nowMs) {
          evictedIdleMs = Some(math.max(0L, nowMs - updated.lastActivityMs))
          shuffleRemoved(shuffleId, updated)
          null
        } else {
          if (dropped.nonEmpty) {
            epoch = epochCounter.incrementAndGet()
            updated = updated.copy(coordinatorEpoch = epoch)
          }
          refreshEmptyShuffleExpiry(shuffleId, updated)
          updated
        }
      })
    (dropped.toSeq, epoch, evictedIdleMs)
  }

  /** Evicts one indexed empty shuffle if its refreshed activity deadline is still elapsed. */
  private def evictExpiredEmptyShuffle(shuffleId: Int, nowMs: Long): Option[Long] = {
    var evictedIdleMs = Option.empty[Long]
    shuffleStates.computeIfPresent(shuffleId,
      (_: Int, existing: StreamingShuffleState) => {
        val deadline = StreamingShuffleCoordinator.deadlineAfter(
          existing.lastActivityMs, StreamingShuffleCoordinator.EMPTY_STATE_TTL_MS)
        if (existing.producers.isEmpty && !existing.hasFallenBack && deadline < nowMs) {
          evictedIdleMs = Some(math.max(0L, nowMs - existing.lastActivityMs))
          shuffleRemoved(shuffleId, existing)
          null
        } else {
          // An authorized lookup may have refreshed an empty state after its old target was polled.
          refreshEmptyShuffleExpiry(shuffleId, existing)
          existing
        }
      })
    evictedIdleMs
  }

  /** Reports one idle-shuffle eviction under the streaming debug gate. */
  private def reportEmptyShuffleEviction(shuffleId: Int, idleMs: Long): Unit = {
    if (debugEnabled) {
      logDebug(log"Evicted the registration of streaming shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)}: it held no live producer and had seen no accepted " +
        log"operation for ${MDC(ELAPSED_TIME, idleMs)} ms, beyond the " +
        log"${MDC(TIMEOUT, StreamingShuffleCoordinator.EMPTY_STATE_TTL_MS)} ms time-to-live")
    }
  }

  /**
   * Performs one pass of stale-producer reaping and returns how many producers were reaped.
   *
   * @return the number of producers reaped in this pass
   */
  def reapStaleProducers(): Int = {
    val nowMs = clock.getTimeMillis()
    var reaped = 0
    var evictedShuffles = 0
    val producerExpiries = ArrayBuffer.empty[StreamingShuffleCoordinator.ProducerExpiry]
    val emptyExpiries = ArrayBuffer.empty[StreamingShuffleCoordinator.EmptyShuffleExpiry]
    expiryIndex.pollExpired(nowMs).foreach {
      case producer: StreamingShuffleCoordinator.ProducerExpiry =>
        producerExpiries += producer
      case empty: StreamingShuffleCoordinator.EmptyShuffleExpiry =>
        emptyExpiries += empty
    }
    producerExpiries.groupBy(_.shuffleId).foreach { case (shuffleId, targets) =>
      val (dropped, epoch, evictedIdleMs) =
        reapExpiredProducerTargets(shuffleId, targets.toSeq, nowMs)
      evictedIdleMs.foreach { idleMs =>
        evictedShuffles += 1
        reportEmptyShuffleEviction(shuffleId, idleMs)
      }
      if (dropped.nonEmpty) {
        // One record per reaping pass and per shuffle, never one per producer.
        val stalestMs = dropped.map(entry => nowMs - entry.lastSeenMs).max
        val outcome = if (epoch == StreamingShuffleCoordinator.NO_EPOCH) {
          log"and the abandoned shuffle registration was evicted"
        } else {
          log"so the shuffle advanced to epoch ${MDC(EPOCH, epoch)}"
        }
        logWarning(log"Reaped ${MDC(COUNT, dropped.size)} streaming shuffle producers of shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)}: the stalest was last seen " +
          log"${MDC(TIME_UNITS, stalestMs)} ms ago, exceeding the liveness timeout " +
          log"${MDC(TIMEOUT, livenessTimeoutMs)} ms, " + outcome)
        if (debugEnabled) {
          dropped.foreach { entry =>
            logDebug(log"Reaped streaming shuffle producer for shuffle " +
              log"${MDC(SHUFFLE_ID, shuffleId)} map " +
              log"${MDC(MAP_ID, entry.location.mapId)} on executor " +
              log"${MDC(EXECUTOR_ID, entry.location.executorId)}, last seen " +
              log"${MDC(TIME_UNITS, nowMs - entry.lastSeenMs)} ms ago")
          }
        }
      }
      reaped += dropped.size
    }
    emptyExpiries.foreach { target =>
      evictExpiredEmptyShuffle(target.shuffleId, nowMs).foreach { idleMs =>
        evictedShuffles += 1
        reportEmptyShuffleEviction(target.shuffleId, idleMs)
      }
    }
    if (evictedShuffles > 0 && debugEnabled) {
      logDebug(log"One reaping pass evicted ${MDC(COUNT, evictedShuffles)} empty streaming " +
        log"shuffle registrations")
    }
    reaped
  }

  /**
   * Number of streaming shuffles currently registered anywhere in the cluster, clamped to at least
   * one.
   */
  def numConcurrentShuffles: Int = math.max(1, shuffleStates.size())

  /**
   * Number of streaming shuffles that one executor is concurrently producing for, clamped to at
   * least one.
   *
   * @param executorId executor to count for; must not be null
   * @return the number of shuffles for which the executor has a registered producer, never less
   *     than one
   */
  def numConcurrentShufflesFor(executorId: String): Int = {
    require(executorId != null, "The executor id must not be null.")
    math.max(1, indexedShuffleCountFor(executorId))
  }

  /** Ids of every registered streaming shuffle, ascending, for diagnostics and assertions. */
  def activeShuffleIds: Seq[Int] = shuffleStates.keySet().asScala.toSeq.sorted

  /**
   * Reduce partition count registered for one shuffle, or `None` when the shuffle has no
   * registration.
   *
   * @param shuffleId shuffle to read
   * @return the registered width
   */
  def registeredNumPartitions(shuffleId: Int): Option[Int] = {
    Option(shuffleStates.get(shuffleId)).map(_.numPartitions)
  }

  /**
   * Live producers of one shuffle, ordered by map id, or an empty sequence when the shuffle has no
   * registration.
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
   * registration.
   *
   * @param shuffleId shuffle to read
   * @return the shuffle's coordinator epoch
   */
  def epochFor(shuffleId: Int): Option[Long] = {
    Option(shuffleStates.get(shuffleId)).map(_.coordinatorEpoch)
  }

  /**
   * Drops every registration and rewinds the epoch counter, so that the coordinator reports what a
   * freshly constructed one would.
   */
  def reset(): Unit = {
    shuffleStates.clear()
    executorShuffles.clear()
    expiryIndex.clear()
    repeatedTimeoutsReported.clear()
    epochCounter.set(StreamingShuffleCoordinator.NO_EPOCH)
  }

  /** Adds one shuffle to an executor's constant-time concurrency index. */
  private def addExecutorShuffle(executorId: String, shuffleId: Int): Unit = {
    executorShuffles.compute(executorId,
      (_: String, existing: java.util.Set[Int]) => {
        val active = if (existing == null) ConcurrentHashMap.newKeySet[Int]() else existing
        active.add(shuffleId)
        active
      })
  }

  /** Removes one shuffle from an executor's concurrency index and drops an empty nested set. */
  private def removeExecutorShuffle(executorId: String, shuffleId: Int): Unit = {
    executorShuffles.computeIfPresent(executorId,
      (_: String, existing: java.util.Set[Int]) => {
        existing.remove(shuffleId)
        if (existing.isEmpty) null else existing
      })
  }

  /** Applies only the executor memberships that one producer replacement can have changed. */
  private def reconcileExecutorShuffles(
      shuffleId: Int,
      before: StreamingShuffleState,
      after: StreamingShuffleState,
      candidates: Seq[String]): Unit = {
    candidates.distinct.foreach { executorId =>
      val wasActive = before.hasProducerOn(executorId)
      val isActive = after.hasProducerOn(executorId)
      if (!wasActive && isActive) {
        addExecutorShuffle(executorId, shuffleId)
      } else if (wasActive && !isActive) {
        removeExecutorShuffle(executorId, shuffleId)
      }
    }
  }

  /** Refreshes or cancels the one liveness deadline belonging to a producer generation. */
  private def refreshProducerExpiry(
      shuffleId: Int,
      state: StreamingShuffleState,
      entry: StreamingShuffleProducerEntry): Unit = {
    val target =
      StreamingShuffleCoordinator.ProducerExpiry(shuffleId, entry.location.generation)
    if (state.mapStage.completedBy(
        entry.location.mapIndex, entry.location.taskAttemptId)) {
      expiryIndex.cancel(target)
    } else {
      expiryIndex.schedule(target,
        StreamingShuffleCoordinator.deadlineAfter(entry.lastSeenMs, livenessTimeoutMs))
    }
  }

  /** Maintains the one eviction deadline belonging to an empty, non-fallback shuffle. */
  private def refreshEmptyShuffleExpiry(
      shuffleId: Int,
      state: StreamingShuffleState): Unit = {
    val target = StreamingShuffleCoordinator.EmptyShuffleExpiry(shuffleId)
    if (state.producers.isEmpty && !state.hasFallenBack) {
      expiryIndex.schedule(target, StreamingShuffleCoordinator.deadlineAfter(
        state.lastActivityMs, StreamingShuffleCoordinator.EMPTY_STATE_TTL_MS))
    } else {
      expiryIndex.cancel(target)
    }
  }

  /** Reconciles all indexes after one producer has been inserted, refreshed or replaced. */
  private def producerPublished(
      shuffleId: Int,
      before: StreamingShuffleState,
      after: StreamingShuffleState,
      entry: StreamingShuffleProducerEntry): Unit = {
    val previous = before.producers.get(entry.location.mapIndex)
    previous.foreach { old =>
      expiryIndex.cancel(
        StreamingShuffleCoordinator.ProducerExpiry(shuffleId, old.location.generation))
    }
    reconcileExecutorShuffles(shuffleId, before, after,
      previous.map(_.location.executorId).toSeq :+ entry.location.executorId)
    refreshProducerExpiry(shuffleId, after, entry)
    refreshEmptyShuffleExpiry(shuffleId, after)
  }

  /** Reconciles all indexes after one producer has been removed. */
  private def producerRemoved(
      shuffleId: Int,
      before: StreamingShuffleState,
      after: StreamingShuffleState,
      entry: StreamingShuffleProducerEntry): Unit = {
    reconcileExecutorShuffles(
      shuffleId, before, after, Seq(entry.location.executorId))
    expiryIndex.cancel(
      StreamingShuffleCoordinator.ProducerExpiry(shuffleId, entry.location.generation))
    refreshEmptyShuffleExpiry(shuffleId, after)
  }

  /** Drops every producer-derived index entry while retaining the shuffle's resulting state. */
  private def allProducersRemoved(
      shuffleId: Int,
      before: StreamingShuffleState,
      after: StreamingShuffleState): Unit = {
    before.producerExecutors.keys.foreach(removeExecutorShuffle(_, shuffleId))
    before.producers.valuesIterator.foreach { entry =>
      expiryIndex.cancel(
        StreamingShuffleCoordinator.ProducerExpiry(shuffleId, entry.location.generation))
    }
    refreshEmptyShuffleExpiry(shuffleId, after)
  }

  /** Drops every secondary index entry belonging to a shuffle that is leaving the registry. */
  private def shuffleRemoved(shuffleId: Int, state: StreamingShuffleState): Unit = {
    state.producerExecutors.keys.foreach(removeExecutorShuffle(_, shuffleId))
    state.producers.valuesIterator.foreach { entry =>
      expiryIndex.cancel(
        StreamingShuffleCoordinator.ProducerExpiry(shuffleId, entry.location.generation))
    }
    expiryIndex.cancel(StreamingShuffleCoordinator.EmptyShuffleExpiry(shuffleId))
  }

  /** Number of bounded expiry records, exposed for deterministic index tests. */
  private[spark] def expiryEntryCount: Int = expiryIndex.size

  /** Number of indexed shuffles for an executor before the public minimum-of-one rule. */
  private[spark] def indexedShuffleCountFor(executorId: String): Int = {
    val active = executorShuffles.get(executorId)
    if (active == null) 0 else active.size()
  }

  /**
   * Whether an operation on `state` presenting `presented` must be refused as unauthorized.
   *
   * @param state atomically-read state the operation would act on
   * @param presented token the caller supplied
   * @return `true` when the operation must be refused
   */
  private def unauthorizedFor(state: StreamingShuffleState, presented: String): Boolean = {
    !StreamingShuffleCoordinator.tokenMatches(state.capabilityToken, presented)
  }

  /**
   * Number of operations refused because they were unauthorized, named an unregistered shuffle, or
   * exceeded a registry cap.
   */
  def deniedOperationCount: Long = deniedOperations.get()

  /**
   * Number of stand-downs whose publication barrier found a map status registered after the
   * withdrawal and removed it.
   */
  def withdrawalBarrierCount: Long = withdrawalBarriers.get()

  /**
   * Whether a request originated inside this JVM rather than from a peer.
   *
   * @param context call context of the request being served
   * @return `true` when the request came from this JVM
   */
  private def isLocalSender(context: RpcCallContext): Boolean = {
    val sender = context.senderAddress
    sender == null || sender == rpcEnv.address
  }

  /**
   * Records a refused per-shuffle operation.
   *
   * @param operation short description of what was attempted
   * @param shuffleId shuffle the attempt named
   * @param cause why it was refused; must not contain caller-supplied text
   */
  private def denyOperation(operation: String, shuffleId: Int, cause: String): Unit = {
    deniedOperations.incrementAndGet()
    if (debugEnabled) {
      logDebug(log"Refused ${MDC(STATUS, operation)} for streaming shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)}: ${MDC(REASON, cause)}")
    }
  }

  /**
   * Records a refused lifecycle or maintenance message that arrived from outside this JVM.
   *
   * @param operation short description of what was attempted
   * @param context call context of the refused request
   */
  private def denyRemoteControl(operation: String, context: RpcCallContext): Unit = {
    deniedOperations.incrementAndGet()
    logWarning(log"Refused ${MDC(STATUS, operation)} of the streaming shuffle coordinator " +
      log"requested by ${MDC(HOST_PORT, senderHostPort(context))}: the operation is " +
      log"accepted only " +
      log"from the driver JVM")
  }

  private def orderedLocations(
      state: StreamingShuffleState): Seq[StreamingShuffleProducerLocation] = {
    state.producers.valuesIterator.map(_.location).toSeq
  }

  /**
   * Negative registration answer.
   *
   * @param shuffleId shuffle the declined registration referred to
   * @param executorId executor the declined registration came from, or `null` when the request
   *     was too malformed to name one
   * @param generationStale whether the refusal was because the registering generation no longer
   *     owns its map index, which is the one rejection a producer must answer by failing rather
   *     than by standing its whole shuffle down
   */
  private def declinedRegistration(
      shuffleId: Int,
      executorId: String,
      generationStale: Boolean = false): StreamingShuffleProducerRegistration = {
    val concurrency = if (isBlank(executorId)) 1 else numConcurrentShufflesFor(executorId)
    StreamingShuffleProducerRegistration(shuffleId, accepted = false, currentEpoch(shuffleId),
      StreamingShuffleCoordinator.CURRENT_PROTOCOL_VERSION, concurrency, generationStale)
  }

  /**
   * Epoch that is current for a shuffle, or [[StreamingShuffleCoordinator.NO_EPOCH]] when the
   * shuffle is unknown.
   */
  private def currentEpoch(shuffleId: Int): Long = {
    epochFor(shuffleId).getOrElse(StreamingShuffleCoordinator.NO_EPOCH)
  }

  /** The location as it should be published, stamped with the epoch it is being published in. */
  private def stamp(
      location: StreamingShuffleProducerLocation,
      epoch: Long): StreamingShuffleProducerLocation = {
    location.copy(registrationEpoch = epoch)
  }

  /**
   * Checks every caller-asserted field of a producer registration, answering `None` when all of
   * them are well formed and `Some(reason)` with an operator-facing explanation otherwise.
   *
   * @param shuffleId shuffle the registration refers to
   * @param location location the caller asserts
   * @param numPartitions reduce partition count the caller asserts
   * @return the first violation found, or `None`
   */
  private def validateProducerRegistration(
      shuffleId: Int,
      location: StreamingShuffleProducerLocation,
      numPartitions: Int): Option[String] = {
    if (shuffleId < 0) {
      Some(s"the shuffle id $shuffleId is negative")
    } else if (location == null) {
      Some("the producer location is missing")
    } else if (isBlank(location.executorId)) {
      Some("the executor id is empty")
    } else if (isBlank(location.host)) {
      Some("the host is empty")
    } else if (location.port < StreamingShuffleCoordinator.MIN_PORT ||
        location.port > StreamingShuffleCoordinator.MAX_PORT) {
      Some(s"the port ${location.port} is outside the range " +
        s"${StreamingShuffleCoordinator.MIN_PORT}-${StreamingShuffleCoordinator.MAX_PORT}")
    } else if (location.mapIndex < 0) {
      Some(s"the map index ${location.mapIndex} is negative")
    } else if (location.blockManagerId == null) {
      Some("the producer block-manager identity is missing")
    } else if (numPartitions <= 0) {
      Some(s"the reduce partition count $numPartitions is not positive")
    } else {
      validateGeneration(shuffleId, location.generation)
    }
  }

  /**
   * Checks the caller-asserted generation identity of a producer mutation, answering `None` when it
   * is well formed and `Some(reason)` otherwise.
   *
   * @param shuffleId shuffle the mutation refers to
   * @param generation generation the caller asserts
   * @return the first violation found, or `None`
   */
  private def validateGeneration(
      shuffleId: Int,
      generation: StreamingShuffleProducerGeneration): Option[String] = {
    if (shuffleId < 0) {
      Some(s"the shuffle id $shuffleId is negative")
    } else if (generation == null) {
      Some("the producer generation is missing")
    } else if (generation.mapIndex < 0) {
      Some(s"the map index ${generation.mapIndex} is negative")
    } else if (generation.mapId < 0) {
      Some(s"the map id ${generation.mapId} is negative")
    } else if (generation.taskAttemptId < 0) {
      Some(s"the task attempt id ${generation.taskAttemptId} is negative")
    } else {
      None
    }
  }

  /** The location with its host replaced by the address the request actually arrived from. */
  private def bindToSender(
      shuffleId: Int,
      location: StreamingShuffleProducerLocation,
      context: RpcCallContext): StreamingShuffleProducerLocation = {
    val address = context.senderAddress
    if (address == null || isBlank(address.host) || address.host == location.host) {
      location
    } else {
      logWarning(log"Binding the streaming shuffle producer of shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, location.mapId)} to the address its " +
        log"registration arrived from: it claimed host ${MDC(HOST, location.host)} but the " +
        log"request came from ${MDC(HOST_PORT, address.hostPort)}")
      val bound = location.blockManagerId match {
        case null => null
        case id if id.host == address.host => id
        case id => BlockManagerId(id.executorId, address.host, id.port, id.topologyInfo)
      }
      location.copy(host = address.host, blockManagerId = bound)
    }
  }

  /** Executor id a registration claims, or `null` when it is too malformed to name one. */
  private def claimedExecutorId(location: StreamingShuffleProducerLocation): String = {
    if (location == null) null else location.executorId
  }

  /** Whether a caller-supplied string is absent or carries no non-whitespace character. */
  private def isBlank(value: String): Boolean = value == null || value.isBlank

  /** Why an operation presenting the wrong capability token is refused. */
  private def unauthorizedReason: String = "the capability token does not match"

  /** Why an operation naming a shuffle the coordinator holds no registration for is refused. */
  private def unregisteredShuffleReason: String =
    "the shuffle is not registered with the coordinator"

  /** Why a new producer is refused once its shuffle is already holding the maximum it may. */
  private def producerCapReason: String =
    s"the shuffle already holds the maximum of " +
      s"${StreamingShuffleCoordinator.MAX_PRODUCERS_PER_SHUFFLE} producers"

  /** Explains a protocol-version rejection in operator-facing terms. */
  private def protocolMismatchReason(protocolVersion: Byte): String = {
    s"protocol version $protocolVersion is not version " +
      s"${StreamingShuffleCoordinator.CURRENT_PROTOCOL_VERSION}, which the coordinator speaks"
  }

  /** Explains a map-stage cardinality rejection in operator-facing terms. */
  private def mapCountMismatchReason(requested: Int, registered: Int): String = {
    s"it declared $requested map task(s) but the shuffle is registered with $registered"
  }

  /** Why a producer registration is declined once its shuffle has stood streaming down. */
  private def fallbackDeclaredReason(fallback: StreamingShuffleFallbackState): String = {
    s"the shuffle stood streaming down at epoch ${fallback.declaredAtEpoch} because " +
      s"${fallback.condition}, so its producers " +
      "must use the sort-based shuffle"
  }

  /** Explains a partition-count rejection in operator-facing terms. */
  private def partitionMismatchReason(requested: Int, registered: Int): String = {
    s"$requested reduce partitions were requested but the shuffle is registered with $registered"
  }

  /** Explains the rejection of a generation that has already been invalidated or reaped. */
  private def retiredGenerationReason(
      generation: StreamingShuffleProducerGeneration): String = {
    s"task attempt ${generation.taskAttemptId} of map ${generation.mapId} has already been " +
      "invalidated or reaped, so it can no longer be published to consumers"
  }

  /** Explains the rejection of a generation that a newer attempt has already superseded. */
  private def supersededGenerationReason(
      generation: StreamingShuffleProducerGeneration,
      registeredAttemptId: Long): String = {
    s"task attempt ${generation.taskAttemptId} of map ${generation.mapId} is older than the " +
      s"registered attempt $registeredAttemptId"
  }

  // Peer input validation.

  /**
   * Validates one peer-supplied registration in full, returning the first reason it must be
   * declined for or `None` when every field is well formed.
   *
   * @param shuffleId shuffle the registration refers to
   * @param location address the producer supplied
   * @param numPartitions reduce partition count the producer supplied
   * @param protocolVersion wire-protocol version the producer supplied
   */
  private def registrationRejection(
      shuffleId: Int,
      location: StreamingShuffleProducerLocation,
      numPartitions: Int,
      protocolVersion: Byte): Option[String] = {
    if (shuffleId < 0) {
      Some("the shuffle id was negative")
    } else if (numPartitions <= 0) {
      Some(s"the reduce partition count was $numPartitions, which is not positive")
    } else {
      locationRejection(location)
        .orElse {
          if (protocolVersion != StreamingShuffleCoordinator.CURRENT_PROTOCOL_VERSION) {
            Some(protocolMismatchReason(protocolVersion))
          } else {
            None
          }
        }
    }
  }

  /**
   * Validates a peer-supplied producer address, returning the first reason it must be declined for
   * or `None` when it is well formed.
   *
   * @param location the address to validate, which may be `null`
   */
  private def locationRejection(
      location: StreamingShuffleProducerLocation): Option[String] = {
    if (location == null) {
      Some("the producer address was absent")
    } else {
      identifierRejection("executor id", location.executorId)
        .orElse(identifierRejection("host", location.host))
        .orElse {
          val minPort = StreamingShuffleCoordinator.MIN_PORT
          val maxPort = StreamingShuffleCoordinator.MAX_PORT
          if (location.port < minPort || location.port > maxPort) {
            Some(s"the port was outside the legal range [$minPort, $maxPort]")
          } else if (location.mapId < 0L) {
            Some("the map id was negative")
          } else if (location.mapIndex < 0) {
            // The map index is what a consumer names in a fetch failure, so a negative one would
            // reach `MapOutputTracker` and match nothing, silently defeating the recomputation the
            // failure exists to trigger.
            Some("the map index was negative")
          } else if (location.blockManagerId == null) {
            Some("the block-manager identity was absent")
          } else {
            // The block-manager identity is stored, handed to consumers and rendered into fetch
            // failures for as long as the producer lives, so its caller-asserted strings pass the
            // same length and control-character checks as the streaming address does.
            identifierRejection("block-manager executor id", location.blockManagerId.executorId)
              .orElse(identifierRejection("block-manager host", location.blockManagerId.host))
          }
        }
    }
  }

  /**
   * Validates one peer-supplied identifier -- an executor id or a host name -- returning the first
   * reason it must be rejected for or `None` when it is acceptable.
   *
   * @param name what the identifier is, used to compose the rejection reason
   * @param value the identifier as received, which may be `null`
   */
  private def identifierRejection(name: String, value: String): Option[String] = {
    val maxLength = StreamingShuffleCoordinator.MAX_IDENTIFIER_LENGTH
    if (value == null) {
      Some(s"the $name was absent")
    } else if (value.isEmpty) {
      Some(s"the $name was empty")
    } else if (value.length > maxLength) {
      // The length is a number, so reporting it discloses nothing and tells an operator exactly
      // what is wrong.
      Some(s"the $name was ${value.length} characters long, exceeding the $maxLength character " +
        "maximum")
    } else if (value.exists(isLineBreaking)) {
      Some(s"the $name contained a control or line-separator character")
    } else {
      None
    }
  }

  /**
   * Whether a character could end a line in a rendered log record.
   *
   * @param character the character to judge
   * @return true when the character must not reach a log sink verbatim
   */
  private def isLineBreaking(character: Char): Boolean = {
    val category = Character.getType(character)
    Character.isISOControl(character) || category == Character.LINE_SEPARATOR ||
      category == Character.PARAGRAPH_SEPARATOR
  }

  /** The map id of a location, but only when the location as a whole is well formed. */
  private def mapIdIfValid(location: StreamingShuffleProducerLocation): Option[Long] = {
    if (locationRejection(location).isEmpty) Some(location.mapId) else None
  }

  /**
   * Records a declined registration at default level, bounded so that a peer retrying a rejected
   * registration cannot choose the driver's log volume.
   *
   * @param shuffleId shuffle the declined registration referred to
   * @param mapId map id of the declined producer, when it was well formed enough to render
   * @param reason why it was declined, composed entirely of this class's own text
   */
  private def recordRegistrationRejection(
      shuffleId: Int,
      mapId: Option[Long],
      reason: String): Unit = {
    val producer = mapId match {
      case Some(id) => log" map ${MDC(MAP_ID, id)}"
      case None => log""
    }
    recordPeerRejection(log"a streaming shuffle registration for shuffle " +
      log"${MDC(SHUFFLE_ID, shuffleId)}" + producer, reason)
  }

  /**
   * Reports one rejected peer request at default level if the bound allows it, and otherwise counts
   * it so that the next report can account for it.
   *
   * @param subject what was rejected, composed by the caller from constants and numbers only
   * @param reason why it was rejected
   */
  private def recordPeerRejection(subject: MessageWithContext, reason: String): Unit = {
    val total = rejectionTotal.incrementAndGet()
    val nowMs = clock.getTimeMillis()
    val due = nextRejectionReportMs.get()
    val window = StreamingShuffleCoordinator.REJECTION_REPORT_WINDOW_MS
    if (nowMs >= due && nextRejectionReportMs.compareAndSet(due, nextWindowEnd(nowMs, window))) {
      val unreported = unreportedRejections.getAndSet(0L)
      logWarning(log"Declining " + subject + log": ${MDC(REASON, reason)} " +
        log"(${MDC(COUNT, total)} peer requests declined so far, " +
        log"${MDC(NUM_SKIPPED, unreported)} not reported individually)")
    } else {
      unreportedRejections.incrementAndGet()
      if (debugEnabled) {
        logDebug(log"Declining " + subject + log": ${MDC(REASON, reason)}")
      }
    }
  }

  /**
   * End of the report window opening at `nowMs`, saturating instead of wrapping so that a wrapped
   * deadline can never land in the past and disable the bound.
   */
  private def nextWindowEnd(nowMs: Long, windowMs: Long): Long = {
    val end = nowMs + windowMs
    if (end < nowMs) Long.MaxValue else end
  }

  /** How many peer requests this coordinator has declined, for diagnostics and assertions. */
  def declinedPeerRequests: Long = rejectionTotal.get()

  /**
   * Debug-level trace of one request, mirroring the map output tracker endpoint's logging helper:
   * it reads the sender address and composes the message with `+`, which is how a multi-part
   * message stays inside the line budget.
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

  /** Debug-level record of the skew between a caller's clock and the coordinator's. */
  private def logSkewIfDebug(shuffleId: Int, timestampMs: Long): Unit = {
    if (debugEnabled) {
      logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} caller clock trails the " +
        log"coordinator by ${MDC(TIME_UNITS, clock.getTimeMillis() - timestampMs)} ms")
    }
  }

  /** Sender address of a request, or "local" when the request carries none. */
  private def senderHostPort(context: RpcCallContext): String = {
    val address = context.senderAddress
    if (address == null) "local" else address.hostPort
  }
}

/**
 * Fixed names, protocol constants and the registration helper for [[StreamingShuffleCoordinator]].
 */
private[spark] object StreamingShuffleCoordinator extends Logging {

  /** One bounded target tracked by the coordinator's deadline index. */
  private sealed trait ExpiryTarget {
    def shuffleId: Int
  }

  /** Liveness deadline for one exact producer generation. */
  private case class ProducerExpiry(
      shuffleId: Int,
      generation: StreamingShuffleProducerGeneration) extends ExpiryTarget

  /** Idle-eviction deadline for one empty shuffle registration. */
  private case class EmptyShuffleExpiry(shuffleId: Int) extends ExpiryTarget

  /** Mutable deadline index with one entry per target. */
  private final class ExpiryIndex {

    private val targetsByDeadline =
      new TreeMap[java.lang.Long, LinkedHashSet[ExpiryTarget]]()
    private val deadlineByTarget =
      new HashMap[ExpiryTarget, java.lang.Long]()

    def schedule(target: ExpiryTarget, deadlineMs: Long): Unit = synchronized {
      val deadline = java.lang.Long.valueOf(deadlineMs)
      val previous = deadlineByTarget.put(target, deadline)
      if (previous != null) {
        removeFromBucket(target, previous)
      }
      var bucket = targetsByDeadline.get(deadline)
      if (bucket == null) {
        bucket = new LinkedHashSet[ExpiryTarget]()
        targetsByDeadline.put(deadline, bucket)
      }
      bucket.add(target)
    }

    def cancel(target: ExpiryTarget): Unit = synchronized {
      val deadline = deadlineByTarget.remove(target)
      if (deadline != null) {
        removeFromBucket(target, deadline)
      }
    }

    def pollExpired(nowMs: Long): Seq[ExpiryTarget] = synchronized {
      val expired = ArrayBuffer.empty[ExpiryTarget]
      while (!targetsByDeadline.isEmpty &&
          targetsByDeadline.firstKey().longValue() < nowMs) {
        val entry = targetsByDeadline.pollFirstEntry()
        entry.getValue.asScala.foreach { target =>
          val indexed = deadlineByTarget.get(target)
          if (indexed != null && indexed == entry.getKey) {
            deadlineByTarget.remove(target)
            expired += target
          }
        }
      }
      expired.toSeq
    }

    def size: Int = synchronized {
      deadlineByTarget.size()
    }

    def clear(): Unit = synchronized {
      targetsByDeadline.clear()
      deadlineByTarget.clear()
    }

    private def removeFromBucket(
        target: ExpiryTarget,
        deadline: java.lang.Long): Unit = {
      val bucket = targetsByDeadline.get(deadline)
      if (bucket != null) {
        bucket.remove(target)
        if (bucket.isEmpty) {
          targetsByDeadline.remove(deadline)
        }
      }
    }
  }

  /** Overflow-safe deadline used by both liveness and empty-state expiry. */
  private def deadlineAfter(startMs: Long, intervalMs: Long): Long = {
    if (startMs > Long.MaxValue - intervalMs) Long.MaxValue else startMs + intervalMs
  }

  /**
   * Name the coordinator endpoint is registered under on the driver and looked up by on every
   * executor.
   */
  val ENDPOINT_NAME: String = "StreamingShuffleCoordinator"

  /** What a capability token renders as, everywhere one would otherwise be rendered. */
  val REDACTED_TOKEN: String = "<redacted>"

  /** Streaming shuffle wire-protocol version this build speaks. */
  val CURRENT_PROTOCOL_VERSION: Byte = StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION

  /** Bound within which a producer failure is detected, in milliseconds. */
  val PRODUCER_CONNECTION_TIMEOUT_MS: Long = 5000L

  /** Time without a heartbeat after which a producer registration is reaped, in milliseconds. */
  val PRODUCER_LIVENESS_TIMEOUT_MS: Long = 10000L

  /** Cadence at which the coordinator polls for stale producers, in milliseconds. */
  val REAPER_INTERVAL_MS: Long = PRODUCER_CONNECTION_TIMEOUT_MS

  /** Epoch value meaning "no epoch has been assigned". */
  val NO_EPOCH: Long = 0L

  /** Reason name held by a [[StreamingShuffleFallbackState]] while streaming is still in force. */
  val NO_FALLBACK_REASON: String = ""

  /**
   * Verdict name recorded when a shuffle has stood streaming down without any of the four
   * graceful-degradation conditions having been observed.
   *
   * @see [[StreamingShuffleCoordinator.withdrawStreaming]]
   */
  val WITHDRAWN_WITHOUT_CONDITION: String = "(withdrawn-without-condition)"

  /** Operator-facing prose for [[WITHDRAWN_WITHOUT_CONDITION]]. */
  val WITHDRAWN_WITHOUT_CONDITION_DESCRIPTION: String =
    "streaming was withdrawn for a request the streaming shuffle cannot serve; none of the four " +
      "graceful-degradation conditions was observed"

  /**
   * Longest peer-supplied identifier -- executor id or host name -- the coordinator will accept.
   */
  val MAX_IDENTIFIER_LENGTH: Int = 256

  /** Lowest port a producer may advertise. */
  val MIN_PORT: Int = 1

  val MAX_PORT: Int = 65535

  /**
   * Window, in milliseconds, within which at most one declined peer request is reported at default
   * level.
   */
  val REJECTION_REPORT_WINDOW_MS: Long = 60000L

  /** Width of a capability token in bytes before Base64 encoding. */
  val CAPABILITY_TOKEN_BYTES: Int = 32

  /** Largest number of streaming shuffles the coordinator will hold registrations for. */
  val MAX_REGISTERED_SHUFFLES: Int = 4096

  /** Largest number of distinct producers a single shuffle may hold registrations for. */
  val MAX_PRODUCERS_PER_SHUFFLE: Int = 65536

  /** How long a shuffle registration that holds no live producer survives before it is evicted. */
  val EMPTY_STATE_TTL_MS: Long = 10L * PRODUCER_LIVENESS_TIMEOUT_MS

  /** Longest caller-supplied invalidation detail that is ever written to the log, in characters. */
  val MAX_INVALIDATION_DETAIL_CHARS: Int = 200

  val NO_INVALIDATION_DETAIL: String = "(no detail)"

  // Source of capability tokens.
  private val tokenSource = new SecureRandom()

  /**
   * Mints a 256-bit capability token from a cryptographically strong source and encodes it with
   * URL-safe, unpadded Base64 for stable transport. The token remains a bearer credential and must
   * never be logged.
   *
   * @return a fresh token, which no practical sequence of calls repeats
   */
  private[streaming] def newCapabilityToken(): String = {
    val bytes = new Array[Byte](CAPABILITY_TOKEN_BYTES)
    tokenSource.nextBytes(bytes)
    Base64.getUrlEncoder.withoutPadding.encodeToString(bytes)
  }

  /**
   * Compares a presented token against the registered one without leaking, through timing, how much
   * of a guess was correct.
   *
   * @param registered token held for the shuffle
   * @param presented token supplied by the caller
   * @return `true` only when both are present and equal
   */
  private[streaming] def tokenMatches(registered: String, presented: String): Boolean = {
    if (registered == null || presented == null || registered.isEmpty || presented.isEmpty) {
      false
    } else {
      val registeredBytes = registered.getBytes(StandardCharsets.UTF_8)
      val presentedBytes = presented.getBytes(StandardCharsets.UTF_8)
      registeredBytes.length == presentedBytes.length &&
        MessageDigest.isEqual(registeredBytes, presentedBytes)
    }
  }

  /**
   * Renders caller-supplied detail text so that it is safe to place in a log record.
   *
   * @param detail caller-supplied text, possibly null, hostile or unbounded
   * @return a single-line, length-capped rendering that is always safe to log
   */
  private[streaming] def sanitizeDetail(detail: String): String = {
    if (detail == null || detail.isEmpty) {
      NO_INVALIDATION_DETAIL
    } else {
      val builder = new StringBuilder(math.min(detail.length, MAX_INVALIDATION_DETAIL_CHARS) + 3)
      var index = 0
      while (index < detail.length && builder.length < MAX_INVALIDATION_DETAIL_CHARS) {
        val character = detail.charAt(index)
        // Anything that is not printable becomes a space.
        val category = Character.getType(character)
        if (Character.isISOControl(character) || category == Character.LINE_SEPARATOR ||
            category == Character.PARAGRAPH_SEPARATOR) {
          builder.append(' ')
        } else {
          builder.append(character)
        }
        index += 1
      }
      if (index < detail.length) {
        builder.append("...")
      }
      val sanitized = builder.toString.trim
      if (sanitized.isEmpty) NO_INVALIDATION_DETAIL else sanitized
    }
  }

  /**
   * Registers the coordinator endpoint on the driver, or resolves a reference to the driver's
   * endpoint from an executor.
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

/**
 * The driver-facing operations a task-scoped streaming shuffle component needs, as an abstraction
 * rather than an `RpcEndpointRef`.
 */
private[spark] trait StreamingShuffleCoordinatorGateway {

  /**
   * Declares that a shuffle must stand streaming down for every participant, and answers with the
   * state in force once the declaration has been processed.
   *
   * @param shuffleId shuffle that must stand streaming down
   * @param cause what was observed -- one of the four specified fallback conditions, or a
   *     structural decline
   * @param detail free text describing the observation
   * @return the fallback state in force, which is what the caller must act on
   */
  def declareFallback(
      shuffleId: Int,
      cause: StreamingShuffleStandDownCause,
      detail: String): StreamingShuffleFallbackState

  /**
   * Withdraws a shuffle from streaming without naming a fallback condition, for a request the
   * streaming path structurally cannot serve, and answers with the state in force once the
   * withdrawal has been processed.
   *
   * @param shuffleId shuffle to withdraw from streaming
   * @param detail free text stating what was asked for and why streaming declined it
   * @return the fallback state in force, which is what the caller must act on
   */
  def withdrawStreaming(shuffleId: Int, detail: String): StreamingShuffleFallbackState

  /**
   * The fallback state in force for a shuffle.
   *
   * @param shuffleId shuffle to ask about
   * @return the fallback state in force
   */
  def fallbackState(shuffleId: Int): StreamingShuffleFallbackState

  /**
   * Withdraws a shuffle's streamed map output so that its map stage is recomputed, without standing
   * streaming down, and answers whether the withdrawal is confirmed.
   *
   * @param shuffleId shuffle whose streamed map output must be recomputed
   * @param detail free text describing what could not be resolved
   * @return true when no streamed map output of this shuffle remains registered, so the caller
   *     may raise a fetch failure knowing the map stage will be recomputed; false when the driver
   *     could not be reached or refused, in which case the caller must say so rather than promise a
   *     recomputation that will not happen
   */
  def invalidateStreamedMapOutput(shuffleId: Int, detail: String): Boolean

  /**
   * Reports that one producer has streamed its whole map output to completion, and learns whether
   * the driver accepted the report.
   *
   * @param shuffleId shuffle the producer streamed
   * @param generation generation identity of the reporting producer, whose map index is what
   *     the completion set is keyed by
   * @return `Some(true)` when the coordinator applied the report, `Some(false)` when it refused
   *     it, and `None` when the driver could not be asked
   */
  def completeProducer(
      shuffleId: Int,
      generation: StreamingShuffleProducerGeneration): Option[Boolean]

  /**
   * Refreshes one producer generation's liveness, and learns whether the driver still holds it.
   *
   * @param shuffleId shuffle this producer streams
   * @param generation generation identity of the heartbeating producer
   * @return the liveness the driver reports, or `None` when the driver could not be reached and
   *     nothing is therefore known.
   */
  def heartbeatProducer(
      shuffleId: Int,
      generation: StreamingShuffleProducerGeneration): Option[StreamingShuffleProducerLiveness]

  /**
   * Withdraws one producer generation from the driver's registry.
   *
   * @param shuffleId shuffle this producer streamed
   * @param generation generation identity being withdrawn
   * @param reason cause of the withdrawal, from the closed set the driver logs
   * @param detail free text for the diagnosis, sanitised and length-capped by the driver
   * @return the shuffle's epoch after the withdrawal, or
   *     [[StreamingShuffleCoordinator.NO_EPOCH]] when the driver could not be reached
   */
  def invalidateProducer(
      shuffleId: Int,
      generation: StreamingShuffleProducerGeneration,
      reason: StreamingShuffleInvalidationReason,
      detail: String): Long
}

/**
 * The production [[StreamingShuffleCoordinatorGateway]]: asks the driver's coordinator endpoint,
 * presenting the capability token the shuffle was registered with.
 *
 * @param coordinatorRef reference to the driver's [[StreamingShuffleCoordinator]] endpoint
 * @param capabilityToken token minted when the shuffle was registered on the driver
 * @param timeout how long any one ask is given before it is abandoned; bounded by the producer
 *     connection timeout so that a wedged driver cannot hold a task thread longer than the
 *     subsystem's own liveness window
 * @param clock source of the wall-clock stamp a heartbeat carries.
 */
private[spark] class RpcStreamingShuffleCoordinatorGateway(
    coordinatorRef: RpcEndpointRef,
    capabilityToken: String,
    timeout: RpcTimeout,
    clock: Clock = new SystemClock)
  extends StreamingShuffleCoordinatorGateway with Logging {

  /** Cumulative unusable answers from the driver, and the window that bounds reporting them. */
  private val unusableAnswers = new AtomicLong(0L)
  private val unusableAnswerLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /**
   * Emits one report about an unusable answer through the window, or accounts it and stays quiet.
   */
  private def reportUnusableAnswer(entry: => MessageWithContext, cause: Throwable): Unit = {
    val occurrences = unusableAnswers.incrementAndGet()
    unusableAnswerLogGate.admit(clock.getTimeMillis()) match {
      case Some(unreported) =>
        val message = entry +
          log". ${MDC(COUNT, occurrences)} request(s) have not been answered usably, " +
          log"${MDC(NUM_SKIPPED, unreported)} of them not reported individually"
        if (cause == null) logWarning(message) else logWarning(message, cause)
      case None =>
    }
  }

  override def declareFallback(
      shuffleId: Int,
      cause: StreamingShuffleStandDownCause,
      detail: String): StreamingShuffleFallbackState = {
    declareVerdict(cause.toString, shuffleId, detail)
  }

  override def withdrawStreaming(
      shuffleId: Int,
      detail: String): StreamingShuffleFallbackState = {
    declareVerdict(StreamingShuffleCoordinator.WITHDRAWN_WITHOUT_CONDITION, shuffleId, detail)
  }

  /** Sends one stand-down verdict, whichever entry point produced it. */
  private def declareVerdict(
      verdictName: String,
      shuffleId: Int,
      detail: String): StreamingShuffleFallbackState = {
    val operation = "a streaming shuffle fallback declaration"
    askAny(operation, shuffleId,
      DeclareStreamingShuffleFallback(shuffleId, capabilityToken, verdictName, detail)) match {
      case Some(state: StreamingShuffleFallbackState) => state
      case answer => unexpected(operation, shuffleId, answer, StreamingShuffleFallbackState())
    }
  }

  override def fallbackState(shuffleId: Int): StreamingShuffleFallbackState = {
    val operation = "a streaming shuffle fallback query"
    askAny(operation, shuffleId,
      GetStreamingShuffleFallbackState(shuffleId, capabilityToken)) match {
      case Some(state: StreamingShuffleFallbackState) => state
      case answer => unexpected(operation, shuffleId, answer, StreamingShuffleFallbackState())
    }
  }

  override def invalidateStreamedMapOutput(shuffleId: Int, detail: String): Boolean = {
    val operation = "a streaming shuffle map output invalidation"
    val request = InvalidateStreamingShuffleMapOutput(shuffleId, capabilityToken, detail)
    askAny(operation, shuffleId, request) match {
      case Some(withdrawn: java.lang.Boolean) => withdrawn.booleanValue()
      // An unreachable or unusable driver is reported as "not withdrawn", which is the safe
      // direction: the caller then states that the recomputation is unconfirmed rather than
      // promising one that may not happen.
      case answer => unexpected(operation, shuffleId, answer, false)
    }
  }

  override def completeProducer(
      shuffleId: Int,
      generation: StreamingShuffleProducerGeneration): Option[Boolean] = {
    val operation = "a streaming shuffle producer completion report"
    askAny(operation, shuffleId,
      CompleteStreamingShuffleProducer(shuffleId, capabilityToken, generation)) match {
      case Some(applied: java.lang.Boolean) => Some(applied.booleanValue())
      // An unreachable driver and an unrecognised answer both mean nothing is known, and neither
      // may be reported as a refusal: a refusal makes the caller withhold its map output, which is
      // right for a generation the driver has disowned and wrong for one that could not be asked.
      case answer => unexpected(operation, shuffleId, answer, None)
    }
  }

  override def invalidateProducer(
      shuffleId: Int,
      generation: StreamingShuffleProducerGeneration,
      reason: StreamingShuffleInvalidationReason,
      detail: String): Long = {
    val operation = "a streaming shuffle producer invalidation"
    val request = InvalidateStreamingShuffleProducer(shuffleId, capabilityToken, generation, reason,
      detail)
    askAny(operation, shuffleId, request) match {
      case Some(epoch: java.lang.Long) => epoch.longValue()
      case answer =>
        unexpected(operation, shuffleId, answer, StreamingShuffleCoordinator.NO_EPOCH)
    }
  }

  override def heartbeatProducer(
      shuffleId: Int,
      generation: StreamingShuffleProducerGeneration)
    : Option[StreamingShuffleProducerLiveness] = {
    val operation = "a streaming shuffle producer heartbeat"
    val request = HeartbeatStreamingShuffleProducer(shuffleId, capabilityToken, generation,
      clock.getTimeMillis())
    askAny(operation, shuffleId, request) match {
      case Some(liveness: StreamingShuffleProducerLiveness) => Some(liveness)
      // An unreachable driver and an unrecognised answer mean the same thing here -- nothing is
      // known -- and both must be reported as nothing known rather than as not live.
      case answer => unexpected(operation, shuffleId, answer, None)
    }
  }

  /** One bounded ask, answering `None` for anything that goes wrong. */
  private def askAny(
      operation: String,
      shuffleId: Int,
      message: StreamingShuffleCoordinatorMessage): Option[Any] = {
    try {
      Some(coordinatorRef.askSync[Any](message, timeout))
    } catch {
      case NonFatal(cause) =>
        reportUnusableAnswer(log"${MDC(STATUS, operation)} for streaming shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} did not reach the driver; the local decision stands " +
          log"and recovery proceeds without it", cause)
        None
    }
  }

  /** Class name of a reply, or the literal `null`, for a diagnostic that must never itself fail. */
  private def describe(answer: Any): String = {
    if (answer == null) "null" else answer.getClass.getName
  }

  /** Reports a reply this build cannot read and answers with the conservative value instead. */
  private def unexpected[T](
      operation: String,
      shuffleId: Int,
      answer: Option[Any],
      fallbackValue: T): T = {
    answer.foreach { other =>
      reportUnusableAnswer(log"Ignoring the coordinator's answer to " +
        log"${MDC(STATUS, operation)} for streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)}: it is " +
        log"a ${MDC(CLASS_NAME, describe(other))}, which this build does not recognise as an " +
        log"answer to that request", null)
    }
    fallbackValue
  }
}
