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
import java.util.Base64
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{MapOutputTrackerMaster, SparkConf, SparkEnv}
import org.apache.spark.internal.{config, Logging, LogKeys, MessageWithContext}
import org.apache.spark.internal.LogKeys.{CLASS_NAME, COUNT, DESCRIPTION, ELAPSED_TIME, EPOCH,
  EXECUTOR_ID, HOST, HOST_PORT, INDEX, MAP_ID, MAX_ATTEMPTS, MAX_SIZE, NEW_VALUE, NUM_EVENTS,
  NUM_PARTITIONS, NUM_SKIPPED, NUM_TASKS, REASON, SHUFFLE_ID, STATUS, TASK_ATTEMPT_ID, THRESHOLD,
  TIME_UNITS, TIMEOUT}
import org.apache.spark.network.shuffle.protocol.streaming.StreamingShuffleMessage
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, RpcTimeout,
  ThreadSafeRpcEndpoint}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{Clock, RpcUtils, SystemClock, ThreadUtils, Utils}

/**
 * Immutable identity of one streaming shuffle producer generation.
 *
 * <b>Logical identity and physical identity are different things, and both are carried here.</b>
 * `mapIndex` is the logical one: the position of the map task inside its stage, which every attempt
 * of that task shares and which `MapOutputTracker` removes a map output by. `mapId` and
 * `taskAttemptId` are the physical one: which attempt produced the bytes.
 *
 * Keeping them apart is what makes supersession work. `ShuffleMapTask` passes
 * `context.taskAttemptId()` as `mapId`, so two attempts of one map task carry two different map
 * ids, and a registry keyed by map id would hold both of them as unrelated producers of unrelated
 * output -- leaving a consumer to read the same map index twice. Keyed by `mapIndex`, a later
 * attempt takes the earlier one's place, which is what a retry and a speculative copy are. (Under
 * `spark.shuffle.useOldFetchProtocol` the map id is the partition index instead, so a map-id key
 * would silently change meaning with that setting; `mapIndex` never does.)
 *
 * The attempt id completes the identity: Spark allocates it from a single monotonic counter per
 * application, so no two attempts of any task ever share one, and a later attempt always carries a
 * larger value than the attempt it replaces.
 *
 * Carrying the three as one value rather than as loose fields is deliberate. Every mutation of a
 * producer registration -- replacing it, refreshing its liveness, or removing it -- compares the
 * caller's generation against the registered one with a single `==`, so no call site can compare
 * one part of the identity and forget the others, which is precisely the defect that lets a stale
 * attempt overwrite, refresh, or invalidate its own replacement.
 *
 * @param mapIndex index of the map task within its stage, which is the logical identity a later
 *                 attempt supersedes on
 * @param mapId map task id whose output the producer streams
 * @param taskAttemptId task attempt id of the producer, unique across the application and
 *                      increasing with each attempt of the same map task
 */
private[spark] case class StreamingShuffleProducerGeneration(
    mapIndex: Int,
    mapId: Long,
    taskAttemptId: Long)

/**
 * Address of a live streaming shuffle producer, as a pure value that is safe to ship inside an
 * RPC reply.
 *
 * Deliberately free of anything live: no channel, no `SparkConf`, no `RpcEndpointRef` and no
 * clock. A consumer turns this into a streaming connection itself, which keeps the coordinator
 * free of transport state and keeps every message in this family cheap to serialize.
 *
 * The location carries the full generation identity of the producer, so a consumer that must
 * later heartbeat against it or invalidate it names the exact generation it read from rather than
 * a map task id that a replacement attempt may since have taken over.
 *
 * @param executorId id of the executor hosting the producer, used for the executor-scoped
 *                   concurrency count that the token bucket divides by
 * @param host host name or address on which the producer serves streaming blocks. A producer
 *             asserts this itself, so the coordinator binds it to the address the request
 *             actually arrived from before publishing it to consumers
 * @param port port on which the producer serves streaming blocks
 * @param mapId map task id whose output this producer streams
 * @param mapIndex index of the map task within its stage, which is the identity
 *                 `MapOutputTracker` removes a map output by. It is carried alongside `mapId`
 *                 rather than derived from it because the two are different numbering schemes --
 *                 `mapId` is allocated per shuffle across the whole application while `mapIndex`
 *                 is the partition index inside one map stage -- and a fetch failure that names
 *                 the wrong one leaves the dead output registered, so the recomputation the
 *                 failure is supposed to trigger never happens
 * @param taskAttemptId task attempt id of the producer; together with `mapId` this is the
 *                      producer's generation identity
 * @param blockManagerId block-manager identity this producer's `MapStatus` carries. It is not
 *                       interchangeable with `host`/`port` above: those address the ephemeral
 *                       streaming listener, whereas this is the identity `MapOutputTracker`
 *                       matches when a fetch failure asks it to remove a map output. Carrying
 *                       both is what lets a consumer name the producer exactly rather than
 *                       synthesising an identity from the streaming endpoint, which would never
 *                       match and would silently defeat upstream recomputation
 * @param registrationEpoch coordinator epoch at which this producer was published, stamped by the
 *                          coordinator and left at [[StreamingShuffleCoordinator.NO_EPOCH]] by
 *                          the producer that builds the location. It is informational: comparing
 *                          it against the shuffle epoch carried by
 *                          [[StreamingShuffleProducerLocations]] tells a consumer whether the
 *                          producer it holds predates the shuffle's most recent generation
 *                          change. It deliberately takes no part in generation comparison, for
 *                          the reason documented on
 *                          [[StreamingShuffleCoordinator.invalidateProducer]]
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
 * This exists because a consumer cannot otherwise tell two entirely different situations apart. A
 * lookup that names no producer may mean "this map stage has no tasks, so the partition is
 * legitimately empty" or it may mean "the map tasks have not registered yet, so keep polling", and
 * a consumer that reads the first meaning into the second silently returns an empty iterator for
 * data that was on its way. Recording the declared cardinality makes the distinction explicit
 * rather than inferred: `numMaps == 0` is the empty stage, and anything else with fewer completed
 * map outputs than `numMaps` is still in progress.
 *
 * Completion is recorded per map index and qualified by the task attempt that reported it, so a
 * completion cannot outlive the generation that earned it. Whenever a producer is invalidated its
 * completion record goes with it, which is what stops a consumer arriving after a producer's death
 * from being told that the map output it needs is already complete. Failing back to "still
 * pending", and from there to a bounded fetch failure, costs a recomputation; treating a dead
 * producer's output as complete would cost correctness.
 *
 * Completion is also what exempts a producer from liveness reaping, and the asymmetry is the whole
 * of the reason this record is kept here rather than inferred. A producer stops heartbeating the
 * moment its map task ends, and its map task ends before any consumer of its output is scheduled,
 * because the DAG scheduler submits a reduce stage only once its map stage has finished. Reaping a
 * silent-but-complete producer would therefore withdraw, on a ten-second timer, exactly the
 * addresses every consumer of the shuffle is about to ask for. Its retained output remains servable
 * regardless -- the executor's streaming listener outlives the task and serves it from the files
 * [[StreamingShuffleBlockResolver]] owns -- so silence after completion carries no information and
 * must not be acted upon. Silence *before* completion still does, and is still reaped: a producer
 * that goes quiet mid-stream really has failed, and its partial stream must be withdrawn.
 *
 * @param numMaps number of map tasks the stage declared, as the driver knows it. Never negative;
 *                zero is the legitimately empty map stage
 * @param completedMaps task attempt id that reported completion, keyed by map index. Bounded by
 *                      `numMaps`, so it cannot grow beyond the stage it describes
 */
private[spark] case class StreamingShuffleMapStage(
    numMaps: Int,
    completedMaps: Map[Int, Long] = Map.empty) {

  /** Whether every map output the stage declared has been streamed to completion. */
  def complete: Boolean = completedMaps.size >= numMaps

  /** Map indexes whose output has been streamed to completion, for a consumer's progress check. */
  def completedIndexes: Set[Int] = completedMaps.keySet

  /**
   * This stage with one map output recorded complete by the attempt that produced it. A later
   * attempt of the same map index replaces the record, so the attempt held is always the newest
   * one to have finished.
   */
  def withCompleted(mapIndex: Int, taskAttemptId: Long): StreamingShuffleMapStage = {
    copy(completedMaps = completedMaps.updated(mapIndex, taskAttemptId))
  }

  /**
   * This stage with one map output's completion withdrawn, but only when the attempt named is the
   * one that recorded it. Qualifying the withdrawal by attempt is what stops a straggling older
   * attempt's death from withdrawing the completion its replacement has already earned.
   */
  def withoutCompleted(mapIndex: Int, taskAttemptId: Long): StreamingShuffleMapStage = {
    if (completedBy(mapIndex, taskAttemptId)) {
      copy(completedMaps = completedMaps - mapIndex)
    } else {
      this
    }
  }

  /**
   * Whether this exact attempt is the one recorded as having completed this map index.
   *
   * Deliberately the same condition [[withoutCompleted]] withdraws on, and expressed once so the
   * two cannot drift: what exempts a producer from reaping and what a withdrawal removes are then
   * the same fact by construction rather than by coincidence.
   */
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
 * Graceful degradation is only correct if it is agreed. A trip observed on one executor stands that
 * executor's streaming path down, but the shuffle it was serving is being produced and consumed by
 * many executors at once, and a shuffle in which some participants stream while others do not is a
 * shuffle with two producers of the same data. The fallback therefore has to be a property of the
 * shuffle, held where every participant can see it, and the coordinator is the only component that
 * every participant already talks to.
 *
 * The state latches on the first declaration and never clears while the shuffle lives, for the same
 * reason the executor-local policy latches: oscillating back into streaming mid-shuffle would mean
 * two implementations owning the same output. Every reply that a participant reads -- registration,
 * producer lookup and the direct query -- carries this value, so no participant can act on a stale
 * belief that streaming is still in force.
 *
 * The reason travels as the stable name of a [[StreamingShuffleFallbackReason]] rather than as the
 * object, so that a value arriving over RPC is normalised back onto this build's own closed set
 * before it is stored or logged and a peer can neither author a driver log record nor smuggle an
 * unknown condition into the registry.
 *
 * @param reasonName name of the latched [[StreamingShuffleFallbackReason]], or
 *                   [[StreamingShuffleCoordinator.NO_FALLBACK_REASON]] while streaming is in force
 * The declared detail travels with the reason for a reason of its own. The set of conditions is
 * closed at four members, so a declaration whose true condition is narrower than any of them has to
 * pick the member it belongs under -- and the member's own prose would then be the only thing an
 * operator ever saw, stating a measurement that was never taken. Carrying the declarer's own
 * sentence alongside the name keeps the name a stable machine identifier while the prose an
 * operator reads is the one the declaring component actually meant.
 *
 * @param reasonName name of the latched [[StreamingShuffleFallbackReason]], or
 *                   [[StreamingShuffleCoordinator.NO_FALLBACK_REASON]] while streaming is in force
 * @param declaredAtEpoch coordinator epoch the shuffle advanced to when the fallback was declared,
 *                        or [[StreamingShuffleCoordinator.NO_EPOCH]] while streaming is in force.
 *                        A consumer holding locations from an earlier epoch can tell from this that
 *                        everything it holds predates the decision
 * @param detail the declaring component's own sanitised account of the condition, or
 *               [[StreamingShuffleCoordinator.NO_INVALIDATION_DETAIL]] when it gave none
 */
private[spark] case class StreamingShuffleFallbackState(
    reasonName: String = StreamingShuffleCoordinator.NO_FALLBACK_REASON,
    declaredAtEpoch: Long = StreamingShuffleCoordinator.NO_EPOCH,
    detail: String = StreamingShuffleCoordinator.NO_INVALIDATION_DETAIL) {

  /** Whether the shuffle has stood streaming down for every participant. */
  def fallenBack: Boolean = reasonName != StreamingShuffleCoordinator.NO_FALLBACK_REASON

  /**
   * The latched condition, resolved against this build's closed set, or `None` while streaming is
   * in force. Resolution can only fail for a name this build does not know, which the coordinator
   * refuses to store in the first place.
   */
  def reason: Option[StreamingShuffleFallbackReason] =
    StreamingShuffleFallbackReason.fromName(reasonName)

  /**
   * The operator-facing account of this verdict: the declarer's own detail when it gave one, this
   * build's prose for the latched member when it did not, and the bare name only for a name this
   * build cannot resolve.
   *
   * Preferring the detail is what stops a declaration from being reported as a condition nobody
   * measured. Three of the four members name a resource condition -- a throughput ratio held for a
   * minute, a refused buffer reservation, a share of an administered link -- so rendering one of
   * them for a declaration that observed none of those things tells an operator to go and tune
   * something that was never the matter.
   */
  def condition: String = {
    if (detail != null && detail.nonEmpty &&
        detail != StreamingShuffleCoordinator.NO_INVALIDATION_DETAIL) {
      detail
    } else {
      reason.map(_.description).getOrElse(reasonName)
    }
  }
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
 * The reply is deliberately '''not''' just a list of addresses. A list alone cannot answer the one
 * question a streaming consumer has to answer before it decides it is finished -- "is there more
 * map output still coming?" -- because the same empty list means "nothing to come" for a map stage
 * with no tasks and "everything still to come" for one whose tasks have not registered yet. The
 * declared cardinality and the completion set answer it explicitly, and the fallback state answers
 * the other question a consumer must not guess at: whether streaming is still in force for this
 * shuffle at all.
 *
 * @param shuffleId shuffle these locations belong to
 * @param locations live producer locations, ordered by map index so that callers and tests observe
 *                  a deterministic sequence, and at most one per map index because that is the
 *                  identity the registry holds a producer under. Each one carries the generation
 *                  identity of the producer it names, which is what a consumer echoes back when it
 *                  invalidates that producer
 * @param numPartitions reduce partition count registered for this shuffle
 * @param numMaps number of map tasks the stage declared; zero is a legitimately empty map stage
 * @param completedMapIndexes map indexes whose output a live producer has streamed to completion
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

  /**
   * Whether map output this consumer has not yet been offered is still expected.
   *
   * This is the predicate that distinguishes "poll again" from "there is genuinely nothing here",
   * and it is deliberately conservative in both directions: a stage that declared no tasks is never
   * pending, and a stage that has stood streaming down is never pending either, because its
   * recovery is a fetch failure and a recomputation rather than another lookup.
   */
  def mapOutputsPending: Boolean = !mapStageComplete && !fallback.fallenBack
}

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
 * @param coordinatorEpoch epoch that is current for the shuffle once the registration has been
 *                         processed, or [[StreamingShuffleCoordinator.NO_EPOCH]] when no epoch was
 *                         assigned. It is reported on a rejection too, so a producer that is
 *                         declined because its generation has been superseded still learns which
 *                         generation the coordinator now considers current
 * @param protocolVersion protocol version the coordinator speaks
 * @param numConcurrentShuffles number of shuffles the registering executor is concurrently
 *                              producing for. This is the divisor of that executor's token-bucket
 *                              refill rate, so it is deliberately executor-scoped rather than
 *                              cluster-wide: an executor is never throttled on account of shuffles
 *                              it takes no part in. Never less than one, so the division is always
 *                              safe
 * @param generationStale set on a rejection when the registration was refused because the
 *                        registering generation is no longer the one that owns its map index -- it
 *                        has been superseded by a later attempt, or retired. It is what separates
 *                        the two rejections a producer must answer differently: a stale generation
 *                        is a single zombie attempt, whose correct outcome is to fail so that the
 *                        live attempt's output is the one used, while every other rejection says
 *                        streaming is not available for the shuffle at all, whose correct outcome
 *                        is a shuffle-wide stand-down onto the sort-based path. Conflating them
 *                        lets a superseded attempt stand a healthy shuffle down. Always `false` on
 *                        an accepted registration, and deliberately not a reason string: a rejected
 *                        caller learns only what it must act on
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
 * `live` is `false` whenever the coordinator holds no registration for the exact generation that
 * heartbeated -- because it was never registered, because it was reaped or invalidated, or because
 * a later attempt of the same map task has taken the registration over. In every one of those
 * cases the producer must stop assuming consumers can find it, and `coordinatorEpoch` tells it
 * which generation the shuffle is on now, so a producer that lost its registration to a newer
 * attempt can distinguish that from a coordinator that has forgotten the shuffle entirely.
 *
 * @param live whether a registration for the heartbeating generation exists and was refreshed
 * @param coordinatorEpoch epoch that is current for the shuffle, or
 *                         [[StreamingShuffleCoordinator.NO_EPOCH]] when the shuffle is unknown
 */
private[spark] case class StreamingShuffleProducerLiveness(live: Boolean, coordinatorEpoch: Long)

/**
 * What the driver receives when it registers a shuffle: the epoch of the shuffle's first
 * producer generation and the capability token that authorizes every later operation on it.
 *
 * The token exists because an `RpcEnv` endpoint is reachable by every peer of the application,
 * and a shuffle id is a small guessable integer. Knowing an id therefore cannot be what
 * authorizes a caller to replace a producer address, refresh a liveness window, invalidate a
 * generation or drop a registration. The coordinator mints this token on the driver, hands it
 * back only to the shuffle manager that asked, and the manager stamps it onto the
 * [[StreamingShuffleHandle]] so that it reaches executors inside the serialized task binary --
 * the same trusted channel that already carries the shuffle dependency itself. A peer that never
 * received a task for the shuffle therefore never learns the token.
 *
 * @param coordinatorEpoch epoch stamped on the shuffle's first producer generation
 * @param capabilityToken opaque bearer token authorizing operations on this shuffle
 * @param fallback whether, and why, this shuffle has already stood streaming down. Reported on the
 *                 grant so that a manager re-registering a shuffle that fell back earlier -- which
 *                 is exactly what the recomputation a fallback forces looks like -- delegates the
 *                 whole shuffle to sort-based shuffle instead of streaming it a second time
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

/**
 * Base type of every message handled by [[StreamingShuffleCoordinator]].
 *
 * The family follows the message idiom of the map output tracker: a sealed trait, `case class`
 * requests carrying only primitives and immutable values, and a `case object` for the stop
 * signal. Every member is `private[spark]`, so none of them widens a public surface and none
 * needs a binary-compatibility exclusion.
 */
private[spark] sealed trait StreamingShuffleCoordinatorMessage {

  /**
   * The rendering a capability token takes wherever a member of this family renders itself.
   *
   * Held on the base type rather than restated in each member, so that the family cannot acquire a
   * message whose token rendering differs from its siblings', and so that the substitute is one
   * value to change rather than nine.
   */
  protected def redactedToken: String = StreamingShuffleCoordinator.REDACTED_TOKEN
}

/**
 * Announces that an executor has begun streaming the output of one map task.
 *
 * The location carries the producer's generation identity, so a stale or speculative attempt can
 * never take the registration of the attempt that replaced it: the coordinator compares the two
 * generations and declines the older one instead of overwriting the newer.
 *
 * Every field of the location is caller-asserted, so the coordinator validates all of them and
 * binds the claimed host to the address the request arrived from before publishing it. See
 * [[StreamingShuffleCoordinator.registerProducer]].
 *
 * @param shuffleId shuffle being streamed
 * @param capabilityToken token issued when the shuffle was registered on the driver, without
 *                        which the registration is refused
 * @param location where the producer can be reached, and which generation it is
 * @param numPartitions reduce partition count the producer streams to
 * @param protocolVersion streaming wire-protocol version the producer speaks
 * @param timestampMs producer-side milliseconds at send time. Supplied by the caller, exactly as
 *                    the heartbeat message of the streaming wire protocol supplies it, and used
 *                    only for producer-to-coordinator clock-skew diagnostics. Liveness itself is
 *                    always judged by the coordinator's own injected clock, so a skewed or
 *                    hostile caller can neither extend nor shorten its own liveness window.
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
 * The reply is an `Option`: `None` means the shuffle has no registration yet, which is the normal
 * answer while a consumer polls an in-progress map stage rather than an error. Every producer of
 * a shuffle streams every reduce partition, so the range is not used to filter the reply; it is
 * validated against the registered partition count, which turns a mis-derived range into an
 * immediate, attributable failure instead of a silent partial read.
 *
 * @param shuffleId shuffle to look up
 * @param capabilityToken token issued when the shuffle was registered on the driver, without
 *                        which the lookup is refused. A lookup discloses live producer addresses,
 *                        so it is authorized exactly as strictly as a mutation.
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
 * Refreshes the liveness of one registered producer generation. The reply reports
 * `live = false` when the coordinator holds no registration for that exact generation, which
 * tells a survivor of a coordinator restart or of a reaping decision that it must register again
 * before consumers can find it, and tells a superseded attempt that it must stop streaming.
 *
 * The generation is named in full rather than by map task id alone, so a stale attempt cannot
 * keep the registration of the attempt that replaced it alive.
 *
 * A heartbeat identifies the producer by generation as well as by map id, and refreshes nothing
 * unless the generation matches exactly. Without that condition a superseded attempt could keep the
 * registration of the attempt that replaced it alive -- or, once the replacement had itself been
 * reaped, resurrect the appearance of liveness for output nobody is producing.
 *
 * @param shuffleId shuffle the producer streams
 * @param capabilityToken token issued when the shuffle was registered on the driver, without
 *                        which the heartbeat is refused
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
 * Most invalidations are raised by a consumer, and the set is named for the conditions a consumer
 * can distinguish. A producer withdrawing its own failed generation reports through the same set,
 * because it is describing the same fact from the other end -- a stream that ends short of what it
 * announced -- and one vocabulary is what keeps the driver's log readable.
 *
 * The cause of an invalidation crosses a trust boundary: it is chosen by a peer executor and it is
 * recorded in the driver's log. Free text would make that peer the author of a driver log record,
 * which is two problems at once -- a peer could embed a line break and forge a second, plausible
 * looking record around it, and a peer could send an arbitrarily long string and multiply the
 * driver's log volume by whatever factor it liked. A closed set of short codes removes both
 * problems by construction rather than by escaping: every code that can ever be logged is a
 * compile-time constant declared here, so there is no untrusted text to escape in the first place.
 *
 * The set is deliberately coarse. It names the conditions a consumer can actually distinguish and
 * an operator can actually act on, and nothing finer -- per-block detail belongs in the consumer's
 * own log, next to the block, not in a driver-side summary.
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

  /**
   * The stream ended short of the blocks that had been announced. Reported by a consumer that saw
   * the stream close early, and by a producer withdrawing its own generation after its map task
   * failed, which is the same fact observed from the producing end.
   */
  case object IncompleteStream extends StreamingShuffleInvalidationReason("INCOMPLETE_STREAM")

  /** The consumer was holding locations from a producer generation that has been superseded. */
  case object StaleEpoch extends StreamingShuffleInvalidationReason("STALE_EPOCH")

  /**
   * The reported cause was absent or is not one this build knows. Reported rather than rejected,
   * because an invalidation must still take effect: forgetting a dead producer is what lets a
   * recomputed one be found, and that must not depend on the two peers agreeing about vocabulary.
   */
  case object Unknown extends StreamingShuffleInvalidationReason("UNKNOWN")

  /** Every reason this build knows, in the order they are documented above. */
  val values: Seq[StreamingShuffleInvalidationReason] = Seq(
    ConnectionTimeout, ChecksumMismatch, ProtocolViolation, IncompleteStream, StaleEpoch, Unknown)

  /**
   * Resolves a code to the reason it names, answering [[Unknown]] for anything else. This is the
   * only supported way to turn a string into a reason, which is what keeps a string from ever
   * becoming one.
   *
   * @param code the code to resolve
   */
  def fromCode(code: String): StreamingShuffleInvalidationReason = {
    values.find(_.code == code).getOrElse(Unknown)
  }

  /**
   * Normalises a reason that arrived over the wire back onto one of the constants declared here.
   *
   * Resolving by code rather than trusting the received object is what makes the guarantee hold
   * even against a peer that is not this build: whatever arrives, what gets recorded is one of the
   * constants above, whose code is short, fixed and free of control characters.
   *
   * @param reason the reason as received, which may be `null`
   */
  def sanitize(reason: StreamingShuffleInvalidationReason): StreamingShuffleInvalidationReason = {
    if (reason == null) Unknown else fromCode(reason.code)
  }
}

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
 * The generation is named in full, so an invalidation raised by a consumer that was reading an
 * older attempt cannot remove the replacement attempt that a recomputed map stage has since
 * registered. An invalidated generation is also remembered as retired, so it can never register
 * again even if the process behind it is still running.
 *
 * @param shuffleId shuffle the producer streamed
 * @param capabilityToken token issued when the shuffle was registered on the driver, without
 *                        which the invalidation is refused
 * @param generation generation identity of the dead producer, as carried by the location the
 *                   consumer read from
 * @param reason cause of the invalidation, drawn from the closed set in
 *               [[StreamingShuffleInvalidationReason]] so that a peer cannot author driver log
 *               content
 * @param detail optional human detail. Sanitized and length-capped before it is logged, never
 *               written to the log as received.
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
 * Drops all state for a shuffle. Answered with `true` when state was present and dropped, and
 * with `false` when the shuffle was unknown, so unregistering twice is harmless.
 *
 * @param shuffleId shuffle whose state should be dropped
 * @param capabilityToken token issued when the shuffle was registered on the driver, without
 *                        which the unregistration is refused
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
 * Answered with `true` when the report was applied. It is refused, and answered `false`, when the
 * reporting generation is not the one registered -- a straggling older attempt cannot claim its
 * replacement's completion -- and when the generation has already been invalidated or reaped, whose
 * output a consumer has been told to stop expecting.
 *
 * @param shuffleId shuffle the producer streamed
 * @param capabilityToken token issued when the shuffle was registered on the driver
 * @param generation generation identity of the producer reporting completion, whose map index is
 *                   the identity the completion set is keyed by
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
 * This is the message that turns an executor-local fallback trip into an agreed, shuffle-wide
 * decision. Without it a trip observed by one participant leaves the others streaming, and a
 * shuffle in which some participants stream while others do not has two producers of the same
 * output. Declaring is idempotent and latching: the first declaration is the one recorded, and
 * every later one is answered with the state already held.
 *
 * The reason travels as the stable name of a [[StreamingShuffleFallbackReason]] and is resolved
 * against this build's own closed set before anything is stored or logged, so a peer can neither
 * introduce a condition this build does not know nor choose the text of a driver log record.
 *
 * Answered with the [[StreamingShuffleFallbackState]] in force once the declaration has been
 * processed, which is the state the caller must act on -- never the one it proposed.
 *
 * @param shuffleId shuffle that must stand streaming down
 * @param capabilityToken token issued when the shuffle was registered on the driver, without which
 *                        the declaration is refused: forcing a shuffle onto the sort-based path is
 *                        a decision no stranger may drive
 * @param reasonName name of the [[StreamingShuffleFallbackReason]] that was observed
 * @param detail free text describing the observation, sanitized before it reaches any log record
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
 * Asks whether a shuffle has stood streaming down, so that a participant can decide between the
 * streaming path and the sort-based one before it commits any state to either.
 *
 * Answered with the [[StreamingShuffleFallbackState]] in force, and with the streaming-in-force
 * value for a shuffle the coordinator does not know: an unknown shuffle has not fallen back, and
 * answering that way keeps an unauthorized or mistimed query from being distinguishable from a
 * shuffle that simply has no registration yet.
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
 * supplied timestamp. One clock decides every expiry, so reaping is immune to skew between hosts
 * and a producer cannot extend its own lease by misreporting the time.
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
 * and every mutation replaces it wholesale inside an atomic `compute`, so a reader outside message
 * processing -- the rate limiter asking for the concurrency count, for instance -- observes one
 * whole state or another and never a half-applied one, without taking a lock and without any field
 * needing to be volatile. The registry traversal around it is weakly consistent, as
 * `ConcurrentHashMap`'s is; it is each state that is atomic, not the set of them.
 *
 * @param numPartitions reduce partition count registered for the shuffle
 * @param mapStage declared cardinality of the shuffle's map stage together with the map outputs
 *                 streamed to completion so far, which is what lets a consumer tell a stage that
 *                 has finished from one that has not begun
 * @param protocolVersion streaming wire-protocol version registered for the shuffle
 * @param coordinatorEpoch epoch of the current producer generation, advanced whenever a producer
 *                         is invalidated, replaced or reaped
 * @param capabilityToken token that authorizes operations on this shuffle. Held here rather than
 *                        in a second map so that a token check and the mutation it guards read
 *                        the same atomically-published snapshot, which is what makes the check
 *                        free of a check-then-act window.
 * @param producers live producers keyed by map task id
 * @param retiredAttempts highest task attempt id that has been invalidated or reaped, per map task
 *                        id. A generation at or below the recorded attempt is never published
 *                        again, which is what stops a producer that a consumer has already
 *                        declared dead from re-registering and being handed out afresh. It holds
 *                        at most one entry per map task and is dropped with the shuffle, so it is
 *                        bounded by the same quantity the producer map is
 * @param producerExecutors reference count of live producers per executor id, maintained
 *                          incrementally by [[withProducer]] and [[withoutProducer]]. It exists so
 *                          that the executor-scoped concurrency count -- which every producer
 *                          registration answers with -- costs one map lookup per shuffle instead
 *                          of a scan of every producer, keeping registration linear in the number
 *                          of map tasks rather than quadratic
 * @param fallback whether, and why, this shuffle has stood streaming down for every participant.
 *                 Held here, on the one value every participant reads through, so the decision is
 *                 agreed rather than made independently on each executor
 * @param lastActivityMs coordinator clock reading at the last accepted operation on the shuffle.
 *                       A shuffle that holds no producers and has seen nothing for longer than
 *                       the empty-state time-to-live is evicted, so a registry entry cannot
 *                       outlive its usefulness and inflate the concurrency divisor forever.
 */
private[spark] case class StreamingShuffleState(
    numPartitions: Int,
    mapStage: StreamingShuffleMapStage,
    protocolVersion: Byte,
    coordinatorEpoch: Long,
    capabilityToken: String,
    producers: Map[Int, StreamingShuffleProducerEntry],
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

  /**
   * The registered generation of one map index, or `None` when it has no live producer.
   *
   * Keyed by map index rather than by map id because the map index is the logical output a
   * consumer needs exactly one producer of, and because two attempts of one map task carry two
   * different map ids.
   */
  def generationOf(mapIndex: Int): Option[StreamingShuffleProducerGeneration] =
    producers.get(mapIndex).map(_.location.generation)

  /**
   * Whether a generation has already been invalidated or reaped and must therefore never be
   * published again. An attempt id at or below the map index's recorded high-water mark is retired,
   * so a generation that was retired is retired for good while the shuffle lives -- and so is every
   * attempt of that map index older than it, which is what stops a straggler from re-registering
   * behind the attempt that replaced it.
   */
  def isRetired(generation: StreamingShuffleProducerGeneration): Boolean =
    retiredAttempts.get(generation.mapIndex).exists(generation.taskAttemptId <= _)

  /**
   * How many registered producer generations of one map output a consumer has invalidated because
   * the producer stopped answering inside the connection-timeout window.
   *
   * Counted per map index, because the map index is the logical output that a recomputation
   * replaces: a second entry for one index means a second attempt of the same output was also lost,
   * which is the difference between one transient loss and a recomputation that is not converging.
   * Only invalidations that actually withdrew a live registration are counted, so a consumer that
   * retries an invalidation of a generation already gone cannot inflate the figure.
   */
  def producerTimeoutsOf(mapIndex: Int): Int = producerTimeouts.getOrElse(mapIndex, 0)

  /**
   * This state with one more producer-liveness invalidation recorded against a map index.
   *
   * The running total is carried as its own field rather than summed from the map on demand,
   * because it is read on the invalidation path and a wide shuffle holds one entry per map task.
   */
  def withProducerTimeout(mapIndex: Int): StreamingShuffleState = {
    copy(
      producerTimeouts = producerTimeouts.updated(mapIndex, producerTimeoutsOf(mapIndex) + 1),
      producerTimeoutTotal = producerTimeoutTotal + 1)
  }

  /**
   * This state with one producer published, keeping the executor index exact. Replacing the
   * producer of a map index moves the index entry from the previous producer's executor to the new
   * one, so the index never has to be recomputed from the producer map.
   *
   * <b>A replacement withdraws the completion its predecessor earned.</b> The completion set
   * records, per map index, the attempt that streamed it in full, and that record is only
   * meaningful while that attempt is the registered producer: once a newer attempt takes the map
   * index over, the executor-scoped block resolver has superseded the older attempt's retained
   * files too, so the output the completion refers to is no longer servable. Leaving the record in
   * place would tell a consumer that a map index is complete while the only producer it can reach
   * is one that has not finished -- and, worse, would leave a completion with no producer at all if
   * that newer attempt were later invalidated. Withdrawing here keeps one invariant true
   * everywhere: a recorded completion always belongs to the registered generation of its map index.
   */
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
   * withdrawing whatever completion that exact generation had earned. Dropping a map task that has
   * no producer changes nothing.
   *
   * The completion is withdrawn here rather than at each call site because every path that removes
   * a producer -- invalidation by a consumer, reaping by the driver's own timer -- must withdraw
   * it: a producer that is gone can no longer serve the output it once finished streaming, and a
   * consumer told otherwise would return an incomplete result instead of failing its fetch. The
   * withdrawal is qualified by the attempt id inside [[StreamingShuffleMapStage.withoutCompleted]],
   * so a straggling older attempt's removal cannot withdraw its replacement's completion.
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
   *
   * Latching is first-writer-wins: a state that has already fallen back is returned unchanged, so
   * the reason recorded is the first condition observed rather than the last, and a second
   * declaration cannot advance the epoch or re-clear a completion set that has already been
   * cleared.
   *
   * The completion set is cleared because a fallback invalidates the streamed output itself, not
   * merely the decision to keep streaming: the reduce side must recompute the map stage onto the
   * sort-based path, and a completion record surviving the declaration would tell a consumer that
   * output it must no longer read is still available.
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

  /**
   * This state with a generation recorded as retired. The record is a high-water mark, so
   * retiring an older attempt than the one already recorded changes nothing.
   */
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

  /**
   * Renders the registry entry without disclosing the credential it holds.
   *
   * This value is the most exposed of the token-bearing ones, because it is the coordinator's own
   * per-shuffle record: it is interpolated into diagnostics about the shuffle it describes and it
   * is the natural thing to render when the registry is dumped. Producer entries are summarised by
   * count rather than listed, because a registry of a wide shuffle holds one per map task and a
   * diagnostic that expanded them all would be unusable as well as long.
   */
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
 * Rendezvous point and active-shuffle registry for the streaming shuffle subsystem.
 *
 * In the classic path a reduce task learns where to fetch from by asking `MapOutputTracker`, and it
 * may only do so once the map stage has completed. A streaming
 * reduce task instead needs the address of a producer that is still running, and no such
 * rendezvous exists in Spark. This endpoint supplies it: producers announce themselves as they
 * begin streaming, consumers look them up while the map stage is in flight, and dead producer
 * generations are forgotten so that a retry after stage recomputation cannot be handed a stale
 * address. It also supplies `numConcurrentShuffles`, the divisor of the token-bucket refill rate
 * `maxBandwidthMBps * 1 MiB / numConcurrentShuffles`, which no existing Spark API reports.
 *
 * ==What "while the map stage is in flight" means here==
 *
 * A lookup is answered from the set of *registered* producers, and registration happens when a
 * producer begins streaming rather than when it finishes. So a consumer that asks mid-production is
 * answered with a live address on the spot -- there is one lookup path, and it serves a consumer
 * that asks during production and one that asks afterwards identically. Nothing in this endpoint
 * waits for a map stage to complete, and nothing in it distinguishes the two callers.
 *
 * What differs between them is only *when* they call, and that is decided by task submission, which
 * belongs to the DAG scheduler. The scheduler is an absolute preservation zone for this feature --
 * AAP 0.2.1 and 0.2.2 forbid modifying the DAG scheduler or the task lifecycle, and AAP 0.8.2 Tier
 * 1 restates it -- and the unmodified scheduler submits a reduce stage only once its map stage
 * reports available output. At an ordinary stage boundary the lookups served here therefore name
 * producers whose task has ended, and the retained output of those producers is served from the
 * block resolver over the same wire protocol, by the same handler, as a live one. The reply carries
 * the completion set so that a reader *records* which of the two it got rather than assuming
 * either. See `StreamingShuffleWriter` and `StreamingShuffleServerHandler` for the same bound
 * stated from the producing side, and `StreamingShuffleWriterSuite`'s "a subscribed consumer is
 * served while the map task is still producing" for the machine-checked form of it.
 *
 * Coexistence with the classic path. This registry is entirely separate from map-output tracking
 * and shares no state with it. Sort-based shuffle remains the default and the fallback, and
 * nothing here is reachable unless
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
 * Generation identity. A producer is identified by its map task id together with its task attempt
 * id, never by the map task id alone. Every mutation -- registering, heartbeating, invalidating --
 * names the full generation and is compared against the registered one atomically, so a stale or
 * speculative attempt can neither overwrite the attempt that replaced it, nor refresh its
 * liveness, nor invalidate it. A generation that has been invalidated or reaped is additionally
 * remembered as retired for as long as the shuffle lives, so a producer whose process is still
 * running after a consumer declared it dead cannot re-register and be handed out again. Each
 * mutation answers with the epoch that is current afterwards, so a caller whose generation was
 * refused always learns which generation the coordinator now considers live.
 *
 * Trust boundary. Every field of a registration arrives from a peer, so all of them are validated
 * before anything is published, and the claimed host is bound to the address the request actually
 * arrived from. That binding is what stops a peer from directing consumers at an endpoint of its
 * choosing, and it follows the precedent of executor registration in
 * `CoarseGrainedSchedulerBackend`, which likewise prefers the address the RPC layer reports over
 * the one the caller asserts. A malformed registration is declined rather than failed, so a
 * defective or hostile peer degrades that one shuffle to sort-based shuffle instead of failing a
 * job.
 *
 * Thread safety. This is a [[ThreadSafeRpcEndpoint]], so messages are processed one at a time
 * with a happens-before relationship between them and no field would need to be volatile for the
 * message path alone. The registry is nevertheless a `ConcurrentHashMap` of immutable state
 * values mutated through atomic `compute`, because it is also read and written from outside
 * message processing: the shuffle manager registers a shuffle directly on the driver, and the
 * rate limiter asks for the concurrency count from the producer's own threads.
 *
 * Determinism. Every liveness decision reads the injected [[Clock]], and so does the one stamp
 * this file originates rather than receives -- the wall-clock reading a producer heartbeat carries,
 * emitted by [[RpcStreamingShuffleCoordinatorGateway]] below. Nothing here sleeps and nothing reads
 * the JVM's clock directly, so every expiry in this file trips at exactly its bound rather than
 * somewhere near it.
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

  // Read once at construction and held immutably: the streaming shuffle subsystem has no dynamic
  // reconfiguration, so every value it depends on is fixed for the lifetime of the JVM. This flag
  // is read first because it is the single authority over every verbose line this endpoint writes:
  // no diagnostic is emitted anywhere in this class without consulting it, so an operator who
  // leaves spark.shuffle.streaming.debug at its default of false sees nothing from the coordinator
  // even when the logging framework is globally set to DEBUG. It is also why no eager debug line is
  // written at construction, unlike the map output tracker's endpoint: materialising the logger
  // early is not worth an unconditional line charged against the log-volume budget, and such a line
  // cannot be gated on this flag anyway because it would have to precede this very declaration.
  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  // The windows that bound this endpoint's two per-shuffle records. One coordinator serves the
  // whole application, so a record per shuffle is a volume the workload chooses rather than one
  // this endpoint controls; the same aggregation type and window the rest of the subsystem uses
  // keeps the convention single, and instance scope is sufficient here because there is exactly one
  // instance.
  private val registrationLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  private val unregistrationLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  /**
   * The window bounding the record made when a producer generation is invalidated or retired.
   *
   * <b>Why this one needs bounding at all.</b> A producer invalidation is a consequential event --
   * it is what has an upstream stage recomputed -- so the temptation is to record every one. Its
   * frequency, though, is a property of how often tasks fail rather than of how many distinct
   * things an operator needs to know: a workload losing a tenth of its producers emits one of these
   * per failed task, which at ten tasks a second is enough on its own to exhaust the whole
   * ten-mebibyte-an-hour budget this subsystem is held to. Bounding it on the same rolling window
   * as every other repeated record keeps the first invalidation in each window at its own level,
   * carries the count of the rest, and restores each of them verbatim under the debug key.
   *
   * A window of its own rather than the registration or unregistration window, because an
   * invalidation and a registration are different events and a burst of one must not be able to
   * silence the other.
   */
  private val invalidationLogAggregator =
    new MemorySpillManager.ExecutorLogAggregator(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  // A producer that has not been seen for longer than this is treated as gone. The window is one
  // full liveness bound, so a producer heartbeating at the connection-timeout cadence may miss a
  // single heartbeat without being reaped.
  private val livenessTimeoutMs: Long = StreamingShuffleCoordinator.PRODUCER_LIVENESS_TIMEOUT_MS

  // How many consecutive attempts of one stage the scheduler will make before it aborts the job.
  // Read here, once, because it is the budget every producer-liveness invalidation of this shuffle
  // spends: each one fails a reduce attempt and costs one recomputation. Read rather than assumed,
  // so an operator who raises their own tolerance for recomputation raises streaming's with it --
  // which is exactly what a long-running stress workload does.
  private val stageAttemptAllowance: Int = conf.get(config.STAGE_MAX_CONSECUTIVE_ATTEMPTS)

  private val perMapProducerTimeoutTolerance: Int =
    StreamingShuffleCoordinator.perMapProducerTimeoutTolerance(stageAttemptAllowance)

  private val shuffleProducerTimeoutTolerance: Int =
    StreamingShuffleCoordinator.shuffleProducerTimeoutTolerance(stageAttemptAllowance)

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
        // streaming debug key restores the per-shuffle record without also reconfiguring the
        // log framework.
        if (debugEnabled) {
          logInfo(entry)
        }
    }
  }

  // Active streaming shuffles, keyed by shuffle id. See StreamingShuffleState for why the values
  // are immutable and why every mutation goes through an atomic compute.
  private val shuffleStates = new ConcurrentHashMap[Int, StreamingShuffleState]()

  // Monotonic source of coordinator epochs. The first epoch handed out is 1, so NO_EPOCH (0) is
  // unambiguously "no epoch was assigned" and every real epoch satisfies the non-negativity
  // requirement of the streaming shuffle handle.
  private val epochCounter = new AtomicLong(StreamingShuffleCoordinator.NO_EPOCH)

  // Count of operations refused because they were unauthorized, malformed or over a cap. Exposed
  // for tests and for operators correlating a fallback with a rejected registration. A counter
  // rather than a log line per event, because an attacker controls the event rate and per-event
  // logging is itself an amplification channel; the individual denials are logged under the debug
  // gate only.
  private val deniedOperations = new AtomicLong(0L)

  // Timer that drives stale-producer reaping. Started in onStart and cancelled in onStop, in the
  // manner of the driver's heartbeat receiver. The executor is created lazily by the JDK, so an
  // endpoint that is constructed but never started spawns no thread.
  private var reaperTask: ScheduledFuture[_] = null

  private val reaperThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("streaming-shuffle-coordinator-reaper")

  // Earliest coordinator-clock reading at which the next rejection may be reported at default
  // level, and how many rejections have gone unreported since the last one that was.
  //
  // Rejections are driven entirely by peers, and a peer that is misconfigured or hostile can retry
  // a rejected registration as fast as it likes. Reporting every rejection would therefore let a
  // peer choose the driver's log volume, which is precisely the amplification the bound exists to
  // remove. Starting the deadline in the past makes the first rejection of the process always
  // reportable, so a genuine misconfiguration is still visible immediately.
  private val nextRejectionReportMs = new AtomicLong(Long.MinValue)

  private val unreportedRejections = new AtomicLong(0L)

  private val rejectionTotal = new AtomicLong(0L)

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
        shuffleId, capabilityToken, location, numPartitions, protocolVersion, timestampMs) =>
      // registerProducer traces the registration itself, including the producer address, so only
      // the caller's clock skew is worth recording here.
      logSkewIfDebug(shuffleId, timestampMs)
      // Every field arrived from a peer. A violation is answered with a declined registration
      // rather than a failed ask, because declining degrades that producer to sort-based shuffle
      // while a failure would fail its task, and the caller's own count is reported back so that a
      // declined producer still has a well-formed reply to read.
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
      // Checked before the call so that a malformed range is reported to the caller without
      // routing through the endpoint's error handler.
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
      // The reason is resolved against this build's closed set before anything is stored or
      // logged. A name this build does not know is refused rather than recorded as "unknown",
      // because standing a whole shuffle down is too consequential to do on the strength of a
      // condition this build cannot reason about, and because an unrecognised name is far more
      // likely to be a forged or skewed message than a real observation.
      StreamingShuffleFallbackReason.fromName(reasonName) match {
        case Some(reason) if shuffleId >= 0 =>
          context.reply(declareFallback(shuffleId, capabilityToken, reason, detail))
        case Some(_) =>
          recordPeerRejection(log"a streaming shuffle fallback declaration from " +
            log"${MDC(HOST_PORT, senderHostPort(context))}",
            s"the shuffle id $shuffleId is negative")
          context.reply(StreamingShuffleFallbackState())
        case None =>
          recordPeerRejection(log"a streaming shuffle fallback declaration for shuffle " +
            log"${MDC(SHUFFLE_ID, shuffleId)} from " +
            log"${MDC(HOST_PORT, senderHostPort(context))}",
            "the fallback reason is not one this build recognises")
          context.reply(StreamingShuffleFallbackState())
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
      // ids or written to a log record. A malformed id is answered with the safe divisor rather
      // than with a failure, because this value only scales a rate limiter: answering one throttles
      // the caller to its full single-shuffle share, which is a conservative answer, never a
      // dangerous one.
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
      // Maintenance, so it is accepted only from this JVM. The coordinator's own timer posts it
      // through `self`, which is a local send, while a remote peer replaying it could otherwise
      // drive unbounded reaping passes on the endpoint thread and starve real requests.
      if (isLocalSender(context)) {
        context.reply(reapStaleProducers())
      } else {
        denyRemoteControl("a reaping pass", context)
        context.sendFailure(new SecurityException(
          "Streaming shuffle reaping may only be requested from the driver JVM"))
      }

    case StopStreamingShuffleCoordinator =>
      // Lifecycle, so it is accepted only from this JVM. Honouring it from any peer would let a
      // single message shut the rendezvous down for the whole application, which is a denial of
      // service that no capability token can be scoped to authorize.
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
   * This is the only operation that creates registry state, and it is reachable only from the
   * driver's own shuffle manager -- it is deliberately not an RPC message. Every remote operation
   * acts on state this method already created, and must present the capability token this method
   * minted. That asymmetry is what stops a peer from populating the registry with arbitrary
   * shuffle ids, and it is why the number of registered shuffles has a ceiling that only
   * driver-side code can approach.
   *
   * The declared map-stage cardinality is recorded here and nowhere else, because the driver is the
   * only participant that knows it before any map task has run. It is what later lets a consumer
   * distinguish a map stage that has finished from one that has not started, so registering without
   * it would leave every consumer guessing from an empty producer list.
   *
   * @param shuffleId shuffle to register
   * @param numPartitions reduce partition count of the shuffle; must be positive
   * @param numMaps number of map tasks in the producing stage; must be non-negative, and zero is a
   *                legitimately empty map stage
   * @param protocolVersion streaming wire-protocol version the caller speaks
   * @return the shuffle's epoch, capability token and fallback state, or `None` if the shuffle was
   *         declined
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
        (_: Int, existing: StreamingShuffleState) =>
          if (existing == null) {
            // The cap is evaluated inside the atomic update so that the decision and the insertion
            // it guards cannot be separated. `size()` reads the map's counters without taking a
            // bin lock, so consulting it here neither deadlocks nor blocks another registration.
            if (shuffleStates.size() >= StreamingShuffleCoordinator.MAX_REGISTERED_SHUFFLES) {
              overCap = true
              null
            } else {
              created = true
              StreamingShuffleState(numPartitions, StreamingShuffleMapStage(numMaps),
                protocolVersion, epochCounter.incrementAndGet(),
                StreamingShuffleCoordinator.newCapabilityToken(), Map.empty,
                lastActivityMs = nowMs)
            }
          } else {
            existing.copy(lastActivityMs = nowMs)
          })
      if (overCap) {
        // The registry is full, so nothing was inserted and there is no state to read below. This
        // is reported at warning level rather than under the debug gate and without aggregation,
        // because only the driver registers shuffles: the rate is bounded by the application's own
        // behaviour, never by a peer's, and an application large enough to reach the ceiling needs
        // to be told. Declining rather than throwing is what keeps the job running -- the shuffle
        // falls back to sort-based shuffle, which is the subsystem's guaranteed degradation path.
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
          // Bounded on a window rather than emitted per shuffle. One record per shuffle reads as
          // modest, and is, for a job with a handful of stage boundaries -- but the driver hosts
          // one coordinator for the whole application, so this record's volume is the application's
          // shuffle count, and a workload that submits several shuffles a second spends the log
          // budget on registrations alone. The admitted record states how many registrations it
          // stands in for; the per-shuffle form is on the debug key.
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
        // The token of an idempotent re-registration is the one already held, never a fresh one,
        // so a handle minted by an earlier call keeps working and a caller cannot rotate another
        // caller's token out from under it. The fallback state travels with it, so a manager
        // re-registering a shuffle that already stood streaming down learns to delegate the whole
        // shuffle rather than streaming it a second time.
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
   * The shuffle must already be registered. This operation never creates registry state: a
   * producer that names an unregistered shuffle is declined rather than accommodated. Creating
   * state here would mean that any peer able to reach the endpoint could mint a permanent entry
   * for any integer it chose, and every such entry would be scanned by the reaper forever and
   * counted in the concurrency divisor that paces every other shuffle on the executor.
   *
   * Registration is idempotent per generation: registering the same attempt of the same map task
   * again replaces the previous entry and refreshes its liveness, which is exactly the behaviour a
   * producer that re-announces itself needs, and it can never inflate either the producer count or
   * the active-shuffle count.
   *
   * Generation ordering is enforced, and this is the property that keeps a recomputed or
   * speculated map task correct. A registration is declined when a strictly newer attempt of the
   * same map task is already registered, so a straggling older attempt cannot take the
   * registration back, and it is declined when the attempt has already been invalidated or reaped,
   * so a producer that a consumer declared dead cannot be handed out again. Both comparisons
   * happen inside the atomic update that would publish the entry, so no interleaving of concurrent
   * registrations can defeat them.
   *
   * Every field of `location` is caller-asserted, and every one of them can arrive from a peer
   * executor over RPC. This is the subsystem's trust boundary and it is enforced here, in the
   * single funnel into the registry: a structurally invalid registration from an in-process caller
   * is a programming error and is thrown, while anything a peer can choose -- an over-long or
   * control-character-bearing identifier, an unusable port, a protocol or partition mismatch -- is
   * declined, so a peer can neither place an unvalidated address where a consumer would later be
   * handed it nor place unvalidated text where the driver would later log it. Declining degrades
   * that producer to sort-based shuffle, whereas failing the ask would fail the task and defeat the
   * zero-regression guarantee. The RPC entry point additionally binds the claimed host to the
   * address the request arrived from before calling this method.
   *
   * @param shuffleId shuffle the producer streams; must be non-negative
   * @param location where the producer can be reached, and which generation it is; must carry a
   *                 well-formed executor id, host, port and map id
   * @param numPartitions reduce partition count the producer streams to; must be positive
   * @param protocolVersion streaming wire-protocol version the producer speaks
   * @return the registration outcome, never `null`; `accepted = false` for a declined registration
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
      // shuffle's state. Registrations are created only by driver-side registerShuffle, which is
      // what keeps the registry cap enforceable against a peer that names shuffle ids in a loop,
      // and what guarantees there is always a capability token for a producer to be checked
      // against. A registration naming an unregistered shuffle is declined below.
      shuffleStates.computeIfPresent(shuffleId,
        (_: Int, existing: StreamingShuffleState) => {
          known = true
          val registered = existing.generationOf(generation.mapIndex)
          if (unauthorizedFor(existing, capabilityToken)) {
            // Declined without disclosing anything else about the shuffle -- not its epoch, not
            // its partition count -- so an unauthorized caller learns only that it was refused.
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
            // Declining here is what makes that unavoidable: a producer whose registration is
            // refused has no consumer to stream to, so its manager degrades it to the delegate, and
            // the shuffle converges on one implementation instead of two.
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
            // predate the current generation. A first insertion invalidates nothing a consumer
            // holds, and a re-registration of the generation already registered changes nothing at
            // all, so both leave the epoch where it is: an epoch that moved on every
            // re-announcement would make staleness detection meaningless.
            val replacesOlder = registered.exists(_.taskAttemptId < generation.taskAttemptId)
            epoch = if (replacesOlder) {
              epochCounter.incrementAndGet()
            } else {
              existing.coordinatorEpoch
            }
            // An accepted registration is activity, so it defers the empty-state eviction that
            // would otherwise drop a registration whose producers were all reaped.
            val published = existing
              .withProducer(StreamingShuffleProducerEntry(stamp(location, epoch), nowMs))
              .copy(lastActivityMs = nowMs)
            if (replacesOlder) published.copy(coordinatorEpoch = epoch) else published
          }
        })
      if (!known) {
        declineReason = Some(unregisteredShuffleReason)
        denied = true
      }
      declineReason match {
        case Some(reason) =>
          if (denied) {
            // Refusals a peer can provoke deliberately -- a wrong token, an unregistered shuffle,
            // a shuffle already at its producer ceiling -- are counted as denied operations and
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
          // The count is read after publication, so an executor registering its first producer of
          // a shuffle already sees that shuffle counted, and it is scoped to the registering
          // executor because that is what its own token bucket divides by.
          StreamingShuffleProducerRegistration(shuffleId, accepted = true, epoch, expected,
            numConcurrentShufflesFor(location.executorId))
      }
    }
  }

  /**
   * Refreshes the liveness of one producer generation against the coordinator's own clock.
   *
   * The refresh is applied only when the registered generation is exactly the one that
   * heartbeated. A heartbeat from an attempt that has been superseded, invalidated or reaped
   * therefore cannot keep its replacement alive, and is answered as not live so that the
   * superseded producer learns to stop.
   *
   * @param shuffleId shuffle the producer streams; must be non-negative
   * @param generation generation identity of the heartbeating producer
   * @return whether the generation is registered and was refreshed, together with the epoch that
   *         is current for the shuffle
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
          // Neither refreshed nor answered with the shuffle's epoch: a caller without the token
          // can learn nothing about the shuffle by heartbeating at it.
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
              existing.withProducer(entry.copy(lastSeenMs = nowMs))
                .copy(lastActivityMs = nowMs)
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
   * `None` is the normal answer to a consumer polling an in-progress map stage, not an error, so a
   * caller is expected to retry rather than to fail. Producers are returned ordered by map index,
   * so repeated calls and test assertions observe a stable sequence.
   *
   * The reply carries the declared map-stage cardinality, the map outputs streamed to completion so
   * far and the shuffle's fallback state alongside the addresses, because a list of addresses
   * cannot on its own tell a consumer whether more output is coming. A snapshot naming no producer
   * is ambiguous -- an empty map stage and a map stage that has not registered yet look identical
   * -- and answering that ambiguity by returning an empty iterator would lose data silently.
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
    // An authorized lookup counts as activity, so this is a compute rather than a plain get. A
    // consumer polling a shuffle whose producers have all been reaped -- exactly what it does while
    // it waits for an upstream stage to be recomputed -- is proof that the shuffle is still in use.
    // Without this, a recomputation lasting longer than the empty-state time-to-live would have the
    // registration evicted from under it, and the recomputed producer would then be refused as
    // belonging to no registered shuffle. An unauthorized caller cannot reach this refresh, so it
    // is not a way to keep a stranger's registration alive.
    val state = shuffleStates.computeIfPresent(shuffleId,
      (_: Int, existing: StreamingShuffleState) =>
        if (unauthorizedFor(existing, capabilityToken)) {
          unauthorized = true
          existing
        } else {
          existing.copy(lastActivityMs = nowMs)
        })
    if (state == null) {
      None
    } else if (unauthorized) {
      // Answered as `None` rather than as a distinguishable failure, so that an unauthorized
      // caller cannot use the shape of the reply to learn whether a given shuffle id exists.
      denyOperation("a producer lookup", shuffleId, "the capability token does not match")
      None
    } else {
      // Every producer of a shuffle streams every reduce partition, so the range does not filter
      // the reply. It is validated instead, because a range outside the registered partition
      // count can only come from a mis-derived request and must not be answered silently.
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
   * This is the positive half of the map-stage progress signal. A consumer that finds no producer
   * for a map index needs to know whether that index was ever produced, and the coordinator is the
   * only component both ends talk to, so the report lands here. It is applied only when the
   * reporting generation is exactly the one registered, so a straggling older attempt cannot claim
   * the completion its replacement earned, and only while the generation has not been retired,
   * because a generation a consumer has declared dead must not be resurrected as complete.
   *
   * A completion is withdrawn again whenever its producer is invalidated or reaped -- see
   * [[StreamingShuffleState.withoutProducer]] -- so the set never claims that output which is no
   * longer servable is available. That asymmetry is deliberate: over-reporting completion loses
   * data, while under-reporting it costs at worst one recomputation.
   *
   * @param shuffleId shuffle the producer streamed; must be non-negative
   * @param capabilityToken token issued when the shuffle was registered on the driver
   * @param generation generation identity of the producer reporting completion; its map index is
   *                   what the completion set is keyed by, and is validated with the rest of the
   *                   generation because it keys a map that lives as long as the shuffle
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
   * This is the mechanism by which graceful degradation becomes an agreed decision rather than a
   * per-executor opinion. Three things happen atomically, and all three are necessary:
   *
   *  - the reason latches, first-writer-wins, so every participant subsequently reads the same
   *    verdict and the condition reported is the first one observed rather than the last;
   *  - every completion record is withdrawn and every live producer is dropped and retired, which
   *    invalidates the partial streaming output and stops any producer -- including one a
   *    recomputation creates -- from re-registering to stream the rest of it;
   *  - the shuffle's epoch advances, so a consumer holding locations from before the decision can
   *    tell that everything it holds predates it.
   *
   * Dropping the producers is what forces the recomputation onto the sort-based path. A consumer
   * that can no longer resolve a producer raises a fetch failure, the unmodified scheduler
   * resubmits the map stage, and the manager -- which reads the latched state here -- serves the
   * new attempts from its delegate. No scheduler, executor or sort-based class is involved in
   * arranging that.
   *
   * An unknown shuffle is answered with the streaming-in-force value and changes nothing: there is
   * no state to stand down, and creating some would let any peer mint a registry entry for any
   * integer it named.
   *
   * @param shuffleId shuffle that must stand streaming down; must be non-negative
   * @param capabilityToken token issued when the shuffle was registered on the driver
   * @param reason condition that was observed, recorded by its stable name
   * @param detail free text describing the observation, sanitized before it reaches a log record
   * @return the fallback state in force once the declaration has been processed
   */
  def declareFallback(
      shuffleId: Int,
      capabilityToken: String,
      reason: StreamingShuffleFallbackReason,
      detail: String = null): StreamingShuffleFallbackState = {
    require(shuffleId >= 0,
      s"shuffleId must be non-negative for a streaming shuffle fallback, but was $shuffleId")
    require(reason != null, s"a fallback reason is required for streaming shuffle $shuffleId")
    val recordedDetail = StreamingShuffleCoordinator.sanitizeDetail(detail)
    var unauthorized = false
    var declared = false
    var droppedProducers = 0
    var epoch = StreamingShuffleCoordinator.NO_EPOCH
    var state = StreamingShuffleFallbackState()
    val nowMs = clock.getTimeMillis()
    shuffleStates.computeIfPresent(shuffleId,
      (_: Int, existing: StreamingShuffleState) => {
        if (unauthorizedFor(existing, capabilityToken)) {
          // Standing a shuffle down forces a stage recomputation across the whole application, so
          // it is precisely the operation a stranger must never drive. Nothing is disclosed either.
          unauthorized = true
          existing
        } else if (existing.hasFallenBack) {
          state = existing.fallback
          existing.copy(lastActivityMs = nowMs)
        } else {
          declared = true
          droppedProducers = existing.producers.size
          epoch = epochCounter.incrementAndGet()
          state = StreamingShuffleFallbackState(reason.toString, epoch, recordedDetail)
          // Every live producer is dropped and its generation retired in the same update that
          // latches the reason, so there is no interleaving in which a producer survives the
          // declaration or re-registers immediately after it.
          existing.producers.values
            .foldLeft(existing) { (accumulated, entry) =>
              accumulated.withoutProducer(entry.location.mapIndex)
                .withRetiredGeneration(entry.location.generation)
            }
            .withFallback(reason.toString, epoch, recordedDetail)
            .copy(lastActivityMs = nowMs)
        }
      })
    if (unauthorized) {
      denyOperation("a shuffle fallback declaration", shuffleId, unauthorizedReason)
      StreamingShuffleFallbackState()
    } else if (declared) {
      // One record per shuffle for the life of the application, because the state latches. This is
      // a decision an operator must be told about without having to enable anything, since it is
      // the difference between the fast path and the guaranteed one.
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} has stood streaming down " +
        log"for every participant at epoch ${MDC(EPOCH, epoch)}: " +
        log"${MDC(REASON, reason.description)} (${MDC(DESCRIPTION, recordedDetail)}). " +
        log"${MDC(COUNT, droppedProducers)} live producer(s) were invalidated; the shuffle " +
        log"will be recomputed on the sort-based path")
      // Outside the registry update, because it reaches a different component and must not run
      // under this endpoint's own compute. This is the step that makes "recomputed on the
      // sort-based path" true rather than merely intended -- see the method's own note.
      invalidateStreamedMapOutput(shuffleId)
      state
    } else {
      if (debugEnabled) {
        val cause =
          if (state.fallenBack) "a shuffle that had already stood down" else "an unknown shuffle"
        logDebug(log"Ignoring a streaming shuffle fallback declaration naming " +
          log"${MDC(REASON, cause)} for shuffle ${MDC(SHUFFLE_ID, shuffleId)}: " +
          log"${MDC(NEW_VALUE, reason.toString)}")
      }
      state
    }
  }

  /**
   * Withdraws every map output of a shuffle from the driver's map-output tracker.
   *
   * Called exactly once per shuffle, on the declaration that latches its fallback, and it is what
   * turns the declaration into an actual recomputation. Without it a fallback is a decision nobody
   * can act on: the streaming map outputs stay registered, so the map stage still reports itself
   * available; a consumer's fetch failure therefore resubmits only the reduce stage, which fails
   * the same way on its next attempt and the next, until the stage-attempt limit aborts the job.
   * There is no per-map blame that would do instead, because a fallback invalidates the whole
   * stage's streamed output rather than one task's.
   *
   * With it, the arithmetic works out on its own through machinery that already exists: the outputs
   * are gone, so the map stage reports itself unavailable, so the scheduler resubmits it with every
   * partition missing, and the manager -- which reads the latched state -- serves those new
   * attempts from its sort-based delegate. No scheduler, executor or sort-based class is modified,
   * and this calls only the tracker's own public withdrawal method.
   *
   * Runs on the driver, where the tracker is the master instance; on anything else, and on a
   * shuffle the tracker does not know, it does nothing. Every failure is absorbed: a declaration
   * that has already latched must not be undone by a tracker that could not be reached, and the
   * participants have already been told to stand down.
   *
   * @param shuffleId the shuffle whose streamed map output is no longer valid to read
   */
  private def invalidateStreamedMapOutput(shuffleId: Int): Unit = {
    try {
      Option(SparkEnv.get).map(_.mapOutputTracker) match {
        case Some(master: MapOutputTrackerMaster) if master.containsShuffle(shuffleId) =>
          master.unregisterAllMapAndMergeOutput(shuffleId)
          logInfo(log"Withdrew every registered map output of streaming shuffle " +
            log"${MDC(SHUFFLE_ID, shuffleId)} so that its map stage is recomputed on the " +
            log"sort-based path")
        case _ =>
          if (debugEnabled) {
            logDebug(log"No driver map-output registration to withdraw for streaming shuffle " +
              log"${MDC(SHUFFLE_ID, shuffleId)}")
          }
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} stood streaming down but " +
          log"its map output could not be withdrawn from the tracker; the reduce side recovers " +
          log"through its own fetch failures", e)
    }
  }

  /**
   * The fallback state in force for a shuffle, or the streaming-in-force value when the shuffle has
   * no registration or the caller cannot present its token.
   *
   * Answering an unauthorized or unknown query the same way an in-force shuffle is answered is
   * deliberate: it keeps the shape of the reply from disclosing whether a given shuffle id exists,
   * and it is the safe direction to be wrong in, because a participant told "streaming is in force"
   * still has every other gate -- registration, lookup, and its own executor-local policy -- ahead
   * of it.
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
   * hand the dead address out and every consumer holding an older epoch can tell that its
   * locations are stale.
   *
   * The registration is removed only when the registered generation is exactly the one named, so a
   * consumer that timed out against an attempt which has since been replaced cannot remove its
   * replacement. The named generation is recorded as retired either way, which is what stops a
   * producer whose process is still running -- the ordinary case after a consumer-side connection
   * timeout -- from re-registering and being handed out again. Retiring a generation that is not
   * the registered one is deliberately harmless: retirement is a per-attempt high-water mark, so it
   * can never block the newer attempt a recomputed map stage produces.
   *
   * The epoch takes no part in the comparison, and that is deliberate: it is shuffle-scoped and
   * advances whenever any producer of the shuffle is invalidated or reaped, so requiring a caller's
   * epoch to match would reject a legitimate invalidation of producer A merely because unrelated
   * producer B had been reaped in the meantime. The task attempt id is the identity that is
   * immutable for the life of a generation, so it is the identity that is compared, and the epoch
   * is returned so the caller learns the truth.
   *
   * The reason is normalised back onto one of the constants of
   * [[StreamingShuffleInvalidationReason]] before it is recorded, so what reaches the driver's log
   * is always a short, fixed token of this build's own choosing and never content a peer authored.
   *
   * ==How this reaches the producer executor's own owners==
   *
   * This method withdraws the driver's registration, which is what it alone owns. The generation's
   * other owners live on the producer executor -- its inbound routing entry, its retained output,
   * its egress sessions and the spill files behind them -- and no driver-to-executor channel exists
   * in this subsystem through which they could be told. They are reached instead by the producer
   * asking: a streaming writer refreshes its registration on the connection-timeout cadence, the
   * removal here makes the very next refresh answer "not live", and the writer turns that answer
   * into a task failure whose unsuccessful stop retires every local owner through the one
   * withdrawal operation. Convergence is bounded by one heartbeat interval, and the ordering is
   * safe in the meantime: the address has already gone, so no consumer can be handed it, and the
   * retirement record stops the producer re-registering the address it is about to withdraw.
   *
   * @param shuffleId shuffle the producer streamed; must be non-negative
   * @param generation generation identity of the dead producer
   * @param reason cause of the invalidation; `null` and anything this build does not recognise are
   *               recorded as [[StreamingShuffleInvalidationReason.Unknown]]
   * @return the shuffle's epoch after the invalidation, or [[StreamingShuffleCoordinator.NO_EPOCH]]
   *         when the shuffle is unknown
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
    // Producer-liveness accounting, read out of the same atomic update that records it so the
    // decision below is taken on a consistent view rather than on a re-read that another
    // invalidation could have moved.
    val timedOut =
      StreamingShuffleInvalidationReason.sanitize(reason) ==
        StreamingShuffleInvalidationReason.ConnectionTimeout
    var mapTimeouts = 0
    var shuffleTimeouts = 0
    var declaredMaps = 0
    var alreadyStoodDown = false
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
          val withdrawn = existing.withoutProducer(generation.mapIndex)
            .withRetiredGeneration(generation)
            .copy(coordinatorEpoch = epoch)
          val counted = if (timedOut) {
            withdrawn.withProducerTimeout(generation.mapIndex)
          } else {
            withdrawn
          }
          mapTimeouts = counted.producerTimeoutsOf(generation.mapIndex)
          shuffleTimeouts = counted.producerTimeoutTotal
          declaredMaps = counted.mapStage.numMaps
          alreadyStoodDown = counted.hasFallenBack
          counted
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
      // The window carries the count of the ones it withheld, and the debug key restores each of
      // them verbatim.
      reportRegistrationBounded(
        log"Invalidated streaming shuffle producer for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, generation.mapId)} attempt " +
          log"${MDC(TASK_ATTEMPT_ID, generation.taskAttemptId)}; shuffle advanced to epoch " +
          log"${MDC(EPOCH, epoch)}: ${MDC(REASON, recordedReason)} " +
          log"(${MDC(DESCRIPTION, recordedDetail)})",
        invalidationLogAggregator)
    } else if (newlyRetired && registeredAttemptId.isDefined) {
      // Made once per generation, because a repeat of the same invalidation no longer advances the
      // retirement record and therefore falls through to the debug branch. That bound is what keeps
      // a consumer that retries an invalidation from amplifying the log; the window above is what
      // keeps a workload losing many DISTINCT generations from doing the same.
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
    if (invalidated && timedOut && !alreadyStoodDown) {
      escalateProducerTimeouts(shuffleId, capabilityToken, generation.mapIndex, mapTimeouts,
        shuffleTimeouts, declaredMaps)
    }
    epoch
  }

  /**
   * Stands a shuffle down once producer-liveness invalidations show that recomputing it on the
   * streaming path is not converging.
   *
   * ==Why this exists==
   *
   * A consumer that receives nothing from a registered producer inside the connection timeout
   * invalidates that producer and raises a fetch failure, and the unmodified scheduler answers by
   * recomputing the upstream map output. That is the specified producer-failure flow and it is
   * correct: the reduce side never mixes surviving pre-failure data with post-recomputation data,
   * and every completing run produces exactly the output sort-based shuffle would have.
   *
   * What that flow lacks on its own is a '''bound'''. Nothing about a recomputation changes the
   * condition that caused the timeout, so a shuffle whose producers cannot sustain the stream --
   * because the executor is starved of buffer budget, of threads, or of both -- loses a producer,
   * recomputes, loses one again, and repeats until `spark.stage.maxConsecutiveAttempts` is spent
   * and the scheduler aborts the job. An abort is the one outcome graceful degradation exists to
   * make impossible: the feature's guarantee is that no configuration and no resource condition
   * leaves a job without a working shuffle, and sort-based shuffle is always a working shuffle.
   *
   * Neither of the two rate-based trip conditions can rescue that case, and for a structural reason
   * rather than a tuning one. The consumer-slowness condition is sustained over sixty seconds,
   * while the connection timeout fires at five, so the producer is invalidated and the reduce
   * attempt is gone long before sixty seconds of evidence exists. And the memory-pressure trip is
   * deliberately confined to a reservation that was actually '''prevented''': a full buffer whose
   * block reached local disk instead is answered by spilling, which is the specified answer and
   * which keeps a producer streaming output its consumers are already reading. So this is a third
   * observation, made where it can be observed at all -- on the driver, the one participant that
   * sees every consumer's invalidations across every attempt of the stage.
   *
   * ==The two bounds, and why they are derived rather than chosen==
   *
   * Both are expressed in the operator's own currency, `spark.stage.maxConsecutiveAttempts`, so the
   * subsystem never spends more of the scheduler's budget than the scheduler has:
   *
   *  - '''per map output''' ([[perMapProducerTimeoutTolerance]]): two generations of one map index
   *    lost to a timeout means the recomputation of that output was lost the same way the original
   *    was. That is the specific signal, and it is the one that fires first in practice, because a
   *    recomputed producer is the one a consumer is still waiting on.
   *  - '''per shuffle''' ([[shuffleProducerTimeoutTolerance]]): losses spread evenly across map
   *    indices would satisfy no per-index bound while still spending an attempt each, so the
   *    shuffle-wide count is bounded one below the attempt allowance. That is the safety net that
   *    makes termination unconditional rather than probable.
   *
   * Neither is a hair trigger: a healthy streaming shuffle records '''no''' producer-liveness
   * invalidations at all, so this can only fire where the alternative was repeated recomputation.
   *
   * ==Why the reason recorded is consumer slowness==
   *
   * The four fallback conditions are a closed set, deliberately, and a fifth would be a fifth way
   * to abandon the fast path. Of the four, this is a producer and consumer that could not be kept
   * in step -- which is what the consumer-slowness condition names -- rather than memory, link
   * capacity or protocol disagreement. The reader already declares the same reason when it cannot
   * resolve a producer at all, for the same reason, and the detail recorded here states the counts
   * and the derivation so the record is never ambiguous about which observation was made.
   *
   * @param shuffleId shuffle whose producers keep being lost
   * @param capabilityToken token the invalidation presented, already validated by the caller
   * @param mapIndex map output whose producer was just invalidated
   * @param mapTimeouts producer-liveness invalidations recorded against that map index
   * @param shuffleTimeouts producer-liveness invalidations recorded across the whole shuffle
   * @param declaredMaps map outputs the shuffle declared, reported so the record is diagnosable
   */
  private def escalateProducerTimeouts(
      shuffleId: Int,
      capabilityToken: String,
      mapIndex: Int,
      mapTimeouts: Int,
      shuffleTimeouts: Int,
      declaredMaps: Int): Unit = {
    val perMapExceeded = mapTimeouts >= perMapProducerTimeoutTolerance
    val shuffleExceeded = shuffleTimeouts >= shuffleProducerTimeoutTolerance
    if (perMapExceeded || shuffleExceeded) {
      // Deliberately terse, and ordered with the facts first. The detail is sanitized and capped
      // at MAX_INVALIDATION_DETAIL_CHARS before it reaches a log record, so prose placed ahead of
      // the counts and of the key they derive from would be what survived. The reasoning belongs in
      // this method's own documentation, to which no cap applies.
      val observation = if (perMapExceeded) {
        s"$mapTimeouts producer(s) of map index $mapIndex hit the " +
          s"${StreamingShuffleCoordinator.PRODUCER_CONNECTION_TIMEOUT_MS} ms connection timeout, " +
          s"at or past the per-map tolerance $perMapProducerTimeoutTolerance"
      } else {
        s"$shuffleTimeouts producer(s) of $declaredMaps map output(s) hit the " +
          s"${StreamingShuffleCoordinator.PRODUCER_CONNECTION_TIMEOUT_MS} ms connection timeout, " +
          s"at or past the shuffle-wide tolerance $shuffleProducerTimeoutTolerance"
      }
      declareFallback(shuffleId, capabilityToken,
        StreamingShuffleFallbackReason.ConsumerTooSlow,
        s"$observation derived from " +
          s"${config.STAGE_MAX_CONSECUTIVE_ATTEMPTS.key}=$stageAttemptAllowance; yielding to " +
          "sort-based shuffle")
    } else if (debugEnabled) {
      logDebug(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} has lost " +
        log"${MDC(COUNT, mapTimeouts)} producer generation(s) of map index " +
        log"${MDC(INDEX, mapIndex)} and ${MDC(NUM_EVENTS, shuffleTimeouts)} across the shuffle, " +
        log"both inside the tolerances of ${MDC(THRESHOLD, perMapProducerTimeoutTolerance)} per " +
        log"map output and ${MDC(MAX_ATTEMPTS, shuffleProducerTimeoutTolerance)} per shuffle; " +
        log"recomputation continues on the streaming path")
    }
  }

  /**
   * Producer-liveness invalidations recorded against one map output of a shuffle, or zero when the
   * shuffle is unknown. Exposed so a suite can observe the tally without being handed the registry
   * entry, which would hand it the capability token as well.
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

  /** Producer-liveness invalidations tolerated per map output before the shuffle stands down. */
  def producerTimeoutTolerancePerMap: Int = perMapProducerTimeoutTolerance

  /** Producer-liveness invalidations tolerated across a shuffle before it stands down. */
  def producerTimeoutToleranceForShuffle: Int = shuffleProducerTimeoutTolerance

  /**
   * Drops all state for a shuffle. Driven from the streaming shuffle manager's `unregisterShuffle`
   * and idempotent: unregistering an unknown shuffle changes nothing and answers `false`.
   *
   * @param shuffleId shuffle whose state should be dropped
   * @return `true` when state was present and dropped
   */
  def unregisterShuffle(shuffleId: Int, capabilityToken: String): Boolean = {
    var unauthorized = false
    var removed = false
    var producerCount = 0
    // A conditional removal, not an unconditional one: the token is checked against the very state
    // that is about to be dropped. Returning null from the remapping function is how a
    // ConcurrentHashMap entry is removed atomically, so the check and the removal are one step.
    // `computeIfPresent` answers null both for an absent key and for a removed one, so removal is
    // recorded by a flag rather than inferred from the return value.
    shuffleStates.computeIfPresent(shuffleId,
      (_: Int, existing: StreamingShuffleState) =>
        if (unauthorizedFor(existing, capabilityToken)) {
          unauthorized = true
          existing
        } else {
          removed = true
          producerCount = existing.producers.size
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
   * Performs one pass of stale-producer reaping and returns how many producers were reaped.
   *
   * A producer that the coordinator has not seen for longer than the liveness window <b>and has
   * not reported its map output complete</b> is dropped and its shuffle's epoch is advanced, which
   * is the driver-side half of producer-failure handling: consumers stop being handed the dead
   * address, and any consumer still holding it detects the failure on its own connection timeout.
   * The reaped generation is also recorded as retired, so an unresponsive producer that recovers
   * cannot re-register the very generation consumers have been told to stop expecting; only a newer
   * attempt may register.
   *
   * The completion exemption is not a relaxation of liveness; it is what makes liveness measure the
   * right thing. A producer heartbeats only while its map task runs, and the DAG scheduler starts
   * no reduce task until the whole map stage has finished, so <i>every</i> producer of a completed
   * map stage is silent by the time its first consumer looks it up. Reaping on that silence would
   * withdraw every address the shuffle is about to be read through, turning the feature into a race
   * against this timer; and it would withdraw them needlessly, because the output stays servable
   * from the executor's streaming listener and the files [[StreamingShuffleBlockResolver]] owns.
   * A completed producer that has genuinely gone -- its executor lost, its port unreachable -- is
   * still removed, by the two mechanisms that can actually tell: a consumer's failed connect
   * invalidates that generation and withdraws its completion with it, and the platform's own
   * map-output removal on executor loss retires the `MapStatus` the writer published. Silence
   * before completion is untouched and is still reaped, because a producer that goes quiet
   * mid-stream really has failed and its partial stream must not be offered to anyone.
   *
   * The shuffle itself is kept once its last producer has been reaped, because a recomputed map
   * stage will register into it again and because dropping it immediately would make the
   * active-shuffle count -- and therefore the rate-limiter divisor -- oscillate during recovery. It
   * is evicted only after it has additionally been idle for the empty-state time-to-live, which is
   * an order of magnitude longer than the liveness window, so an ordinary recovery never races it.
   * A shuffle that has stood streaming down is never evicted at all: its latched verdict has to
   * outlive the producers that standing down retired, so only [[unregisterShuffle]] drops it.
   *
   * Reaping is normally driven by this endpoint's own timer. It is public so that a round can also
   * be forced directly, against the clock the endpoint was given, rather than only on the cadence.
   *
   * @return the number of producers reaped in this pass
   */
  def reapStaleProducers(): Int = {
    val nowMs = clock.getTimeMillis()
    var reaped = 0
    var evictedShuffles = 0
    // The scan is bounded by the registry cap, and the registry cap is enforceable because only
    // driver-side registration creates entries. Empty states are evicted below, so the set this
    // iterates shrinks back to the genuinely active shuffles instead of accumulating one entry per
    // shuffle id any caller ever named.
    shuffleStates.keySet().asScala.toSeq.foreach { shuffleId =>
      // Staleness is decided inside the atomic update, so a producer that heartbeats concurrently
      // is never reaped on the strength of an older snapshot. The dropped producers are logged
      // afterwards to keep the update itself free of expensive work.
      var dropped: Seq[StreamingShuffleProducerEntry] = Nil
      var epoch = StreamingShuffleCoordinator.NO_EPOCH
      var evicted = false
      var idleMs = 0L
      shuffleStates.computeIfPresent(shuffleId,
        (_: Int, existing: StreamingShuffleState) => {
          val stale = existing.producers.filter {
            case (_, entry) =>
              nowMs - entry.lastSeenMs > livenessTimeoutMs &&
                !existing.mapStage.completedBy(
                  entry.location.mapIndex, entry.location.taskAttemptId)
          }
          val idleFor = nowMs - existing.lastActivityMs
          if (stale.size == existing.producers.size &&
              idleFor > StreamingShuffleCoordinator.EMPTY_STATE_TTL_MS &&
              !existing.hasFallenBack) {
            // A shuffle that holds no live producer and has seen no accepted operation for longer
            // than the time-to-live is dropped outright. Returning null removes the entry, which is
            // what keeps the registry, the reaper's own scan and the concurrency divisor bounded
            // over the lifetime of a long-running application.
            //
            // A shuffle that has stood streaming down is exempt, and the exemption is load-bearing
            // rather than cautious. Standing down drops every producer by design, so a fallen-back
            // shuffle looks exactly like an abandoned one to this branch; evicting it would erase
            // the latched verdict, and the very next producer of that shuffle -- the recomputed map
            // task the fallback exists to force onto the sort-based path -- would be admitted to
            // stream again. The verdict has to outlive the producers it retired, so only
            // [[unregisterShuffle]] may drop it. The registry stays bounded because a fallen-back
            // shuffle holds no producers, is counted by no executor's concurrency divisor, and is
            // unregistered along with every other shuffle when the application cleans it up.
            dropped = stale.values.toSeq.sortBy(_.location.mapIndex)
            evicted = true
            idleMs = idleFor
            null
          } else if (stale.isEmpty) {
            existing
          } else {
            dropped = stale.values.toSeq.sortBy(_.location.mapIndex)
            epoch = epochCounter.incrementAndGet()
            // Each reaped generation is retired as well as dropped, so a producer that is merely
            // unresponsive rather than dead cannot re-register the generation a consumer has
            // already been told to stop expecting. Dropping goes through withoutProducer so that
            // the executor index stays exact.
            dropped
              .foldLeft(existing) { (state, entry) =>
                state.withoutProducer(entry.location.mapIndex)
                  .withRetiredGeneration(entry.location.generation)
              }
              .copy(coordinatorEpoch = epoch)
          }
        })
      if (evicted) {
        // Counted here rather than inside the atomic update, so that the update itself stays free
        // of anything but the state transition. An eviction is not a failure -- it is the registry
        // reclaiming a shuffle nobody has touched for ten liveness windows -- so it is reported
        // under the debug gate, aggregated per pass, and the idle interval that justified it is
        // reported with it because that interval is the only evidence an operator has that the
        // time-to-live is set sensibly for their workload.
        evictedShuffles += 1
        if (debugEnabled) {
          logDebug(log"Evicted the registration of streaming shuffle " +
            log"${MDC(SHUFFLE_ID, shuffleId)}: it held no live producer and had seen no accepted " +
            log"operation for ${MDC(ELAPSED_TIME, idleMs)} ms, beyond the " +
            log"${MDC(TIMEOUT, StreamingShuffleCoordinator.EMPTY_STATE_TTL_MS)} ms " +
            log"time-to-live")
        }
      }
      if (dropped.nonEmpty) {
        // One record per reaping pass and per shuffle, never one per producer. A pass can reap
        // every producer of a shuffle at once -- that is exactly what a lost executor looks like --
        // so a per-producer record would make a single node failure the largest log event the
        // driver ever writes. The aggregated record carries what an operator needs to act: how many
        // went, the staleness of the one silent longest, the bound they missed and the epoch the
        // shuffle advanced to. Per-producer identity, which is the part that scales with the
        // cluster, is available under the streaming debug key.
        val stalestMs = dropped.map(entry => nowMs - entry.lastSeenMs).max
        logWarning(log"Reaped ${MDC(COUNT, dropped.size)} streaming shuffle producers of shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)}: the stalest was last seen " +
          log"${MDC(TIME_UNITS, stalestMs)} ms ago, exceeding the liveness timeout " +
          log"${MDC(TIMEOUT, livenessTimeoutMs)} ms, so the shuffle advanced to epoch " +
          log"${MDC(EPOCH, epoch)}")
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
    if (evictedShuffles > 0 && debugEnabled) {
      logDebug(log"One reaping pass evicted ${MDC(COUNT, evictedShuffles)} empty streaming " +
        log"shuffle registrations")
    }
    reaped
  }

  /**
   * Number of streaming shuffles currently registered anywhere in the cluster, clamped to at least
   * one.
   *
   * This is a diagnostic reading, not a throttling input. The divisor of the token-bucket refill
   * rate is always the executor-scoped count from [[numConcurrentShufflesFor]], because bandwidth
   * is an executor-local resource and an executor must not be throttled on account of shuffles
   * running elsewhere. That rate is `(0.8 * maxBandwidthMBps * 1 MiB) / numConcurrentShuffles`, in
   * which `maxBandwidthMBps` is the administered link capacity and the 0.8 is streaming shuffle's
   * ceiling on it; see [[TokenBucketRateLimiter.executorBudget]] for the one definition of that
   * contract.
   * Clamping the answer here rather than at every call site is what guarantees the division is
   * always safe, including in the window before the first shuffle has registered.
   */
  def numConcurrentShuffles: Int = math.max(1, shuffleStates.size())

  /**
   * Number of streaming shuffles that one executor is concurrently producing for, clamped to at
   * least one.
   *
   * This is the executor-scoped view of the registry, and it is the value an executor's rate
   * limiter divides its bandwidth budget by: an executor producing for one shuffle is not throttled
   * on account of shuffles it takes no part in. It is also the count reported by every producer
   * registration reply, so a producer never has to ask a second question to size its bucket.
   *
   * The answer costs one map lookup per registered shuffle, because each shuffle state carries a
   * per-executor producer index maintained as producers come and go. Counting producers instead
   * would make each registration linear in the number of map tasks already registered, and the map
   * stage as a whole quadratic.
   *
   * @param executorId executor to count for; must not be null
   * @return the number of shuffles for which the executor has a registered producer, never less
   *         than one
   */
  def numConcurrentShufflesFor(executorId: String): Int = {
    require(executorId != null, "The executor id must not be null.")
    val active = shuffleStates.values().asScala.count(_.hasProducerOn(executorId))
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

  /**
   * Whether an operation on `state` presenting `presented` must be refused as unauthorized.
   *
   * Named as a predicate and used at every gate, so that each call site reads as the question it is
   * asking rather than as a restatement of the comparison mechanism, and so that the constant-time
   * comparison can never be accidentally replaced by `==` at one site out of six.
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
   * exceeded a registry cap. Monotonic for the lifetime of the endpoint.
   */
  def deniedOperationCount: Long = deniedOperations.get()

  /**
   * Whether a request originated inside this JVM rather than from a peer.
   *
   * Used to confine lifecycle and maintenance messages to the driver. A local send leaves the
   * sender address either unset or equal to this endpoint's own `RpcEnv` address, while a message
   * from any other process carries that process's address, so the two are distinguishable without
   * any new transport state or configuration.
   *
   * @param context call context of the request being served
   * @return `true` when the request came from this JVM
   */
  private def isLocalSender(context: RpcCallContext): Boolean = {
    val sender = context.senderAddress
    sender == null || sender == rpcEnv.address
  }

  /**
   * Records a refused per-shuffle operation. The counter always advances; the explanation is
   * written only under the debug gate, because the rate of refusals is attacker-controlled and
   * logging one line per refusal would itself be an amplification channel.
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
   * Records a refused lifecycle or maintenance message that arrived from outside this JVM. Logged
   * at warning level rather than under the debug gate, because a remote peer attempting to stop or
   * to drive the coordinator is a security-relevant event an operator should see, and it cannot be
   * provoked by ordinary streaming traffic.
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
    state.producers.values.map(_.location).toSeq.sortBy(_.mapIndex)
  }

  /**
   * Negative registration answer. It always reports the protocol version the coordinator speaks,
   * so a declined caller learns what it should have sent; the epoch that is current for the
   * shuffle, so a caller whose generation was superseded learns which one is live; and the
   * concurrency count of the caller's own executor, so that a caller falling back still has a
   * well-formed, correctly scoped reply to read.
   *
   * @param shuffleId shuffle the declined registration referred to
   * @param executorId executor the declined registration came from, or `null` when the request was
   *                   too malformed to name one
   * @param generationStale whether the refusal was because the registering generation no longer
   *                        owns its map index, which is the one rejection a producer must answer by
   *                        failing rather than by standing its whole shuffle down
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
   * shuffle is unknown. Every reply that reports an epoch reads it through here, so no caller ever
   * receives a null-shaped answer for an unknown shuffle.
   */
  private def currentEpoch(shuffleId: Int): Long = {
    epochFor(shuffleId).getOrElse(StreamingShuffleCoordinator.NO_EPOCH)
  }

  /**
   * The location as it should be published, stamped with the epoch it is being published in.
   *
   * The stamp is informational for consumers, so it is applied in exactly one place: the entry a
   * producer registration publishes.
   */
  private def stamp(
      location: StreamingShuffleProducerLocation,
      epoch: Long): StreamingShuffleProducerLocation = {
    location.copy(registrationEpoch = epoch)
  }

  /**
   * Checks every caller-asserted field of a producer registration, answering `None` when all of
   * them are well formed and `Some(reason)` with an operator-facing explanation otherwise.
   *
   * There is one checker rather than one per entry point, so that the RPC path -- which declines a
   * malformed registration in order to degrade gracefully -- and the direct path -- which throws,
   * because a malformed direct call is a defect in Spark itself -- can never disagree about what
   * "well formed" means.
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
   * is well formed and `Some(reason)` otherwise. Shared by registration, heartbeat and
   * invalidation, so all three agree on what a nameable generation is.
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

  /**
   * The location with its host replaced by the address the request actually arrived from.
   *
   * A producer asserts its own host, and that assertion is what consumers are told to connect to,
   * so it is bound to the authenticated sender rather than trusted: otherwise a peer could point
   * every consumer of a shuffle at an endpoint of its choosing. The port is left as claimed,
   * because a producer serves streaming blocks on its own listener rather than on the RPC port, and
   * the executor id is left as claimed because it scopes nothing but the claimant's own bandwidth
   * divisor.
   *
   * The block-manager identity is bound to the same address, and for the same reason: a consumer
   * renders it into the fetch failure that asks `MapOutputTracker` to remove a map output, so a
   * producer allowed to assert an arbitrary host there could name another executor's output. The
   * executor id inside it is left as claimed, exactly as the streaming location's is, because
   * `MapOutputTracker` matches on the whole identity and a mismatched one simply matches nothing.
   *
   * A request that carries no sender address never crossed a network -- a direct in-JVM ask, or an
   * `RpcEnv` without a server, which is the local-mode case -- so there is nothing to bind it to
   * and the claim stands.
   */
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

  /** Explains a protocol-version rejection in operator-facing terms. */
  /**
   * Why an operation presenting the wrong capability token is refused. Deliberately identical for
   * every operation and free of any caller-supplied text, so a refusal discloses nothing beyond
   * the fact of the refusal.
   */
  private def unauthorizedReason: String = "the capability token does not match"

  /**
   * Why an operation naming a shuffle the coordinator holds no registration for is refused. A
   * producer whose shuffle was never registered, or whose registration has already been dropped,
   * degrades to sort-based shuffle rather than creating registry state of its own.
   */
  private def unregisteredShuffleReason: String =
    "the shuffle is not registered with the coordinator"

  /** Why a new producer is refused once its shuffle is already holding the maximum it may. */
  private def producerCapReason: String =
    s"the shuffle already holds the maximum of " +
      s"${StreamingShuffleCoordinator.MAX_PRODUCERS_PER_SHUFFLE} producers"

  private def protocolMismatchReason(protocolVersion: Byte): String = {
    s"protocol version $protocolVersion is not version " +
      s"${StreamingShuffleCoordinator.CURRENT_PROTOCOL_VERSION}, which the coordinator speaks"
  }

  /** Explains a map-stage cardinality rejection in operator-facing terms. */
  private def mapCountMismatchReason(requested: Int, registered: Int): String = {
    s"it declared $requested map task(s) but the shuffle is registered with $registered"
  }

  /**
   * Why a producer registration is declined once its shuffle has stood streaming down. The epoch of
   * the decision is named so that an operator reading the producer's log can line the refusal up
   * against the driver's single fallback record.
   */
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

  // Peer input validation. Every helper below returns a reason composed entirely of this class's
  // own constants and of numeric properties of the offending value -- never the value itself, and
  // never any fragment of it. That is the whole point: a rejection record has to be safe to write
  // even though the thing it is about is not safe to write.

  /**
   * Validates one peer-supplied registration in full, returning the first reason it must be
   * declined for or `None` when every field is well formed.
   *
   * Order matters: the location is checked before the protocol version, so that a registration
   * carrying both a malformed address and a bad version is reported for the address. The address is
   * the field a later log record or a later consumer reply would read, so it is the one worth
   * naming.
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
   * A stored address is handed to consumers and written to log records for as long as the producer
   * lives, so it is validated once, here, at the moment it enters the registry, rather than
   * defensively at every point that later reads it.
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
   * Three properties are required, and each of them exists to close a specific hole. It must be
   * present and non-empty, because an absent identity cannot be matched against a stored one and
   * would silently widen an executor-scoped count into a global one. It must be no longer than the
   * declared maximum, because a peer choosing the length of a string the driver stores per producer
   * and writes per record is a peer choosing the driver's memory and log volume. And it must
   * contain no control character, because a log record is line oriented: a carriage return or a
   * line feed in a rendered value lets a peer end the driver's record early and forge a second,
   * entirely plausible one after it. Rejecting control characters outright is safer than escaping,
   * because it removes the question of whether every downstream sink escapes identically.
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
      // what is wrong. The offending text itself is never rendered.
      Some(s"the $name was ${value.length} characters long, exceeding the $maxLength character " +
        "maximum")
    } else if (value.exists(Character.isISOControl)) {
      Some(s"the $name contained a control character")
    } else {
      None
    }
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
   * The first rejection is always reported, because the first one is the one that tells an operator
   * something is misconfigured. Later ones inside the same window are counted and, when the
   * streaming debug key is on, recorded individually as well.
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
   * What a capability token renders as, everywhere one would otherwise be rendered.
   *
   * '''Why every token-bearing value needs this.''' The token is the whole of the coordinator's
   * authorization: a peer holding it may replace a producer address, refresh a liveness window,
   * invalidate a generation, force a shuffle onto the sort-based path, or drop a registration
   * outright. A `case class` gets a generated `toString` that renders every field, and these
   * values reach renderings nobody writes by hand -- Spark's own RPC layer logs a message it
   * cannot deliver or does not recognise in full, an `Option` or a collection of them lands in a
   * `require` diagnostic, and a state value is interpolated into a log record about the shuffle it
   * describes. A credential printed in any of those has left the trusted channel it was designed
   * never to leave, and it stays in the log for as long as the log does.
   *
   * So the presence of a token is reported and its value never is. The substitute is a fixed
   * string, deliberately not a hash or a prefix: a stable derivation of a secret is still a
   * distinguisher for it, and a prefix is a head start for whoever is guessing the rest. It
   * matches the rendering `StreamingShuffleHandle` already uses, so the one credential in this
   * subsystem reads the same wherever it is mentioned.
   */
  val REDACTED_TOKEN: String = "<redacted>"

  /**
   * Streaming shuffle wire-protocol version this build speaks.
   *
   * A registration that asks for any other version is declined, which is how the documented
   * "producer/consumer version mismatch" fallback condition is detected by an explicit
   * compatibility check rather than inferred from a parse failure. The value is positive, as the
   * streaming shuffle handle requires of the version it carries.
   *
   * Delegated to the wire protocol's own constant rather than restated as a literal. The version
   * the coordinator arbitrates registrations by and the version the protocol stamps into every
   * frame header must be the same number by construction: were they two literals, bumping one and
   * missing the other would leave the coordinator admitting producers whose frames it could not
   * accept, which is the precise failure the compatibility check exists to prevent.
   */
  val CURRENT_PROTOCOL_VERSION: Byte = StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION

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
   * Fewest producer-liveness invalidations of one map output that may stand a shuffle down.
   *
   * Two, and it cannot sensibly be one: a single loss is the ordinary producer-failure flow, which
   * the specification answers by invalidating the partial reads and recomputing the upstream stage,
   * and standing a shuffle down for it would abandon the fast path for a fault the feature is
   * designed to absorb. Two is the first count that says something a single loss cannot -- that the
   * recomputation was lost the same way the original was.
   */
  val MIN_PRODUCER_TIMEOUT_TOLERANCE: Int = 2

  /**
   * Producer-liveness invalidations tolerated for one map output before a shuffle stands down.
   *
   * Derived from the scheduler's own consecutive-attempt allowance, less two: one attempt is the
   * original and one is left for the sort-based recomputation the stand-down routes to, so the
   * subsystem never spends the last attempt on a path that has already failed twice. Floored at
   * [[MIN_PRODUCER_TIMEOUT_TOLERANCE]], so an installation that has tightened the allowance to one
   * or zero cannot reduce this to a value that trips on the first ordinary producer failure.
   *
   * @param stageAttemptAllowance value of `spark.stage.maxConsecutiveAttempts`
   * @return invalidations tolerated per map output
   */
  def perMapProducerTimeoutTolerance(stageAttemptAllowance: Int): Int =
    math.max(MIN_PRODUCER_TIMEOUT_TOLERANCE, stageAttemptAllowance - 2)

  /**
   * Producer-liveness invalidations tolerated across a whole shuffle before it stands down.
   *
   * One below the scheduler's consecutive-attempt allowance, because each invalidation fails a
   * reduce attempt and costs one recomputation: at the allowance less one the scheduler still has
   * an attempt in hand, and that attempt is the one the sort-based delegate completes the job on.
   * This is the bound that makes termination unconditional, since losses spread one per map index
   * satisfy no per-index bound while spending an attempt each. Never below the per-map tolerance,
   * so the two bounds can never be ordered the wrong way round.
   *
   * @param stageAttemptAllowance value of `spark.stage.maxConsecutiveAttempts`
   * @return invalidations tolerated per shuffle
   */
  def shuffleProducerTimeoutTolerance(stageAttemptAllowance: Int): Int =
    math.max(perMapProducerTimeoutTolerance(stageAttemptAllowance), stageAttemptAllowance - 1)

  /**
   * Epoch value meaning "no epoch has been assigned". Real epochs start at one, so this can never
   * collide with a live generation, and it is non-negative, so it is safe to carry anywhere an
   * epoch is expected.
   */
  val NO_EPOCH: Long = 0L

  /**
   * Reason name held by a [[StreamingShuffleFallbackState]] while streaming is still in force.
   *
   * The empty string rather than a word such as "None", so that no legal
   * [[StreamingShuffleFallbackReason]] name can ever collide with it: every member of that closed
   * set renders as its non-empty case-object name, so "has fallen back" is exactly "the recorded
   * name is not empty" and needs no second boolean to be kept consistent with it.
   */
  val NO_FALLBACK_REASON: String = ""

  /**
   * Longest peer-supplied identifier -- executor id or host name -- the coordinator will accept.
   *
   * Chosen to sit comfortably above every identifier Spark itself produces (an executor id is a
   * small integer or a container id, and a host name is bounded at 253 characters by DNS) while
   * still bounding what a single producer registration can cost the driver in retained memory and
   * in log bytes. It is a limit on a hostile or misconfigured peer, not on a legitimate one.
   */
  val MAX_IDENTIFIER_LENGTH: Int = 256

  /** Lowest port a producer may advertise. Zero means "any port", which is never a real address. */
  val MIN_PORT: Int = 1

  /** Highest port a producer may advertise. */
  val MAX_PORT: Int = 65535

  /**
   * Window, in milliseconds, within which at most one declined peer request is reported at default
   * level.
   *
   * A minute bounds the cost of rejection reporting at sixty records an hour however hard a peer
   * retries, which keeps a peer from being able to choose the driver's log volume while still
   * surfacing a genuine misconfiguration within a minute of it appearing.
   */
  val REJECTION_REPORT_WINDOW_MS: Long = 60000L

  /**
   * Width of a capability token in bytes before Base64 encoding.
   *
   * 256 bits, matching the strength Spark already uses for its own authentication secret. A token
   * is a bearer credential checked by an endpoint a peer can call in a loop, so the only safe
   * design is one where guessing is infeasible rather than merely slow.
   */
  val CAPABILITY_TOKEN_BYTES: Int = 32

  /**
   * Largest number of streaming shuffles the coordinator will hold registrations for.
   *
   * A ceiling is what makes the reaper's per-pass work and the concurrency divisor bounded. It is
   * far above what a real application registers concurrently, because it is a backstop against
   * runaway driver-side registration rather than a tuning knob: the actual defence against registry
   * poisoning is that only driver-side `registerShuffle` creates state at all.
   */
  val MAX_REGISTERED_SHUFFLES: Int = 4096

  /**
   * Largest number of distinct producers a single shuffle may hold registrations for.
   *
   * Generous on purpose -- a wide map stage legitimately has very many map tasks streaming at once
   * -- and it never blocks a producer that is already registered from refreshing itself, so
   * reaching the ceiling degrades new registrations only. Producer registration is already gated by
   * the shuffle's capability token, which makes this a second line of defence rather than the
   * first.
   */
  val MAX_PRODUCERS_PER_SHUFFLE: Int = 65536

  /**
   * How long a shuffle registration that holds no live producer survives before it is evicted.
   *
   * Comfortably longer than the liveness window, so an ordinary gap between one producer generation
   * being reaped and the recomputed generation registering never costs a shuffle its registration.
   * Its purpose is to stop a registration whose job has moved on from occupying the registry, and
   * the concurrency divisor, for the remaining lifetime of the application.
   */
  val EMPTY_STATE_TTL_MS: Long = 10L * PRODUCER_LIVENESS_TIMEOUT_MS

  /**
   * Longest caller-supplied invalidation detail that is ever written to the log, in characters.
   * Anything longer is truncated with an ellipsis, which bounds what one message can contribute to
   * the driver's log volume.
   */
  val MAX_INVALIDATION_DETAIL_CHARS: Int = 200

  /** Placeholder logged in place of an absent or entirely unprintable invalidation detail. */
  val NO_INVALIDATION_DETAIL: String = "(no detail)"

  // Source of capability tokens. One per JVM: the JDK's implementation is thread-safe and reseeds
  // itself, and tokens are minted only when a shuffle is registered on the driver, so this is never
  // on a hot path.
  private val tokenSource = new SecureRandom()

  /**
   * Mints a capability token: 256 bits from a cryptographically strong source, rendered with the
   * URL-safe, unpadded Base64 alphabet so the value is log-safe and comparison-safe.
   *
   * The width matters. A token is a bearer credential for one shuffle, checked by an endpoint a
   * peer can call in a loop, so it must be infeasible to guess rather than merely awkward to guess.
   *
   * @return a fresh token, never equal to a previously issued one in any practical sense
   */
  private[streaming] def newCapabilityToken(): String = {
    val bytes = new Array[Byte](CAPABILITY_TOKEN_BYTES)
    tokenSource.nextBytes(bytes)
    Base64.getUrlEncoder.withoutPadding.encodeToString(bytes)
  }

  /**
   * Compares a presented token against the registered one without leaking, through timing, how
   * much of a guess was correct.
   *
   * `MessageDigest.isEqual` is the JDK's constant-time comparison and is used rather than
   * `String.equals`, which returns as soon as it finds a differing character and would therefore
   * let a caller recover a token one character at a time by measuring reply latency. A null or
   * empty token on either side never matches, and a length difference is resolved before the
   * comparison, which is the one distinction the primitive cannot hide anyway.
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
   * Two independent hazards are removed. Every character that could terminate or structure a log
   * record -- carriage return, line feed, tab and any other control character -- is replaced with a
   * single space, so a caller cannot forge an additional log line or inject a fake severity. The
   * result is then truncated to [[MAX_INVALIDATION_DETAIL_CHARS]], so a caller cannot use one
   * message to consume an unbounded amount of the driver's log budget. A null, empty or entirely
   * unprintable detail becomes [[NO_INVALIDATION_DETAIL]] rather than the string "null".
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
        // Anything that is not printable becomes a space. Testing the character's category rather
        // than enumerating carriage return and line feed also covers vertical tab, form feed, the
        // Unicode line and paragraph separators and the C1 controls, every one of which a log
        // viewer may treat as a record boundary.
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

/**
 * The driver-facing operations a task-scoped streaming shuffle component needs, as an abstraction
 * rather than an `RpcEndpointRef`.
 *
 * Two of the subsystem's contracts cannot be honoured from inside one task without reaching the
 * driver. A fallback observed by one participant has to become a decision the whole shuffle agrees
 * on, or the shuffle ends up with two producers of the same output; and a producer that has
 * streamed its whole map output has to say so, or a consumer cannot tell a map stage that has
 * finished from one that has not begun. Both are therefore reachable from a writer, and the writer
 * is where they are observed.
 *
 * The abstraction exists so that reaching the driver does not put `RpcEnv` inside a writer. The
 * production implementation is [[RpcStreamingShuffleCoordinatorGateway]]; the whole of the
 * consumer-failure and graceful-degradation flow can therefore be driven by a substitute with a
 * manual clock and no live Spark cluster, which is what makes those flows deterministic to exercise
 * rather than dependent on a socket.
 *
 * Implementations are shared executor-wide and must be safe to call from several task threads at
 * once. Every method must answer rather than throw: these calls sit on the recovery path of a
 * component that already has a failure in hand, so an implementation that threw would replace a
 * diagnosed failure with an undiagnosed one.
 */
private[spark] trait StreamingShuffleCoordinatorGateway {

  /**
   * Declares that a shuffle must stand streaming down for every participant, and answers with the
   * state in force once the declaration has been processed.
   *
   * @param shuffleId shuffle that must stand streaming down
   * @param reason condition that was observed
   * @param detail free text describing the observation
   * @return the fallback state in force, which is what the caller must act on
   */
  def declareFallback(
      shuffleId: Int,
      reason: StreamingShuffleFallbackReason,
      detail: String): StreamingShuffleFallbackState

  /**
   * The fallback state in force for a shuffle.
   *
   * @param shuffleId shuffle to ask about
   * @return the fallback state in force
   */
  def fallbackState(shuffleId: Int): StreamingShuffleFallbackState

  /**
   * Reports that one producer has streamed its whole map output to completion.
   *
   * @param shuffleId shuffle the producer streamed
   * @param generation generation identity of the reporting producer, whose map index is what the
   *                   completion set is keyed by
   * @return `true` when the coordinator applied the report
   */
  def completeProducer(
      shuffleId: Int,
      generation: StreamingShuffleProducerGeneration): Boolean

  /**
   * Refreshes one producer generation's liveness, and learns whether the driver still holds it.
   *
   * Both halves are load bearing. The coordinator reaps a producer that has not been heard from
   * inside its liveness window, so a map task that streams for longer than that window without
   * heartbeating would have its own address withdrawn from under its consumers. And a generation
   * the driver has retired -- superseded by a newer attempt, invalidated by a consumer that timed
   * out, or retired by a shuffle-wide fallback -- can only learn that by asking, because this
   * subsystem has no driver-to-executor channel. The answer is therefore how a driver-side
   * invalidation reaches the producer executor's own owners of that generation.
   *
   * @param shuffleId shuffle this producer streams
   * @param generation generation identity of the heartbeating producer
   * @return the liveness the driver reports, or `None` when the driver could not be reached and
   *         nothing is therefore known. `None` must never be read as "not live": a producer that
   *         stood itself down on an unreachable driver would abandon a shuffle that is perfectly
   *         healthy everywhere else
   */
  def heartbeatProducer(
      shuffleId: Int,
      generation: StreamingShuffleProducerGeneration): Option[StreamingShuffleProducerLiveness]

  /**
   * Withdraws one producer generation from the driver's registry.
   *
   * The driver holds the address a consumer resolves, and it is the one owner of a generation that
   * a producer executor cannot retire for itself. A failing producer therefore says so, rather than
   * letting the registration be reaped: reaping takes a full liveness window, and every consumer
   * that resolves the shuffle inside that window is handed an address that will refuse it and then
   * has to spend its own connection timeout discovering as much.
   *
   * @param shuffleId shuffle this producer streamed
   * @param generation generation identity being withdrawn
   * @param reason cause of the withdrawal, from the closed set the driver logs
   * @param detail free text for the diagnosis, sanitised and length-capped by the driver
   * @return the shuffle's epoch after the withdrawal, or
   *         [[StreamingShuffleCoordinator.NO_EPOCH]] when the driver could not be reached
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
 * <b>Why the token is held here.</b> It reaches an executor inside the serialized shuffle handle,
 * which is the same trusted channel that already carries the shuffle dependency, and the manager
 * hands it to this gateway once. Keeping it in one place means no task-scoped component has to
 * carry it, and no call site can forget to present it.
 *
 * <b>Why every failure is absorbed.</b> Each of these asks is made by a component that is already
 * handling a failure or finishing a task, and none of them has a recovery for an unreachable driver
 * that is better than the one already in progress. A failed fallback declaration leaves the local
 * latch in force, so this executor still stands down; a failed completion report leaves a consumer
 * to poll, time out and recover by recomputation. Both are strictly safer than propagating an
 * exception out of a cleanup path, so failures are logged and swallowed by design rather than by
 * oversight.
 *
 * @param coordinatorRef reference to the driver's [[StreamingShuffleCoordinator]] endpoint
 * @param capabilityToken token minted when the shuffle was registered on the driver
 * @param timeout how long any one ask is given before it is abandoned; bounded by the producer
 *                connection timeout so that a wedged driver cannot hold a task thread longer than
 *                the subsystem's own liveness window
 * @param clock source of the wall-clock stamp a heartbeat carries. Wall clock, and deliberately so:
 *              the stamp exists to be compared against the driver's own reading of the same kind,
 *              so a monotonic figure from this JVM would be meaningless on the other side of the
 *              wire. Injected rather than read from the JVM so that the one value this gateway
 *              originates is as controllable in a suite as every other time source in the subsystem
 */
private[spark] class RpcStreamingShuffleCoordinatorGateway(
    coordinatorRef: RpcEndpointRef,
    capabilityToken: String,
    timeout: RpcTimeout,
    clock: Clock = new SystemClock)
  extends StreamingShuffleCoordinatorGateway with Logging {

  /**
   * Cumulative unusable answers from the driver, and the window that bounds reporting them.
   *
   * A producer refreshes its registration on a timer, so an unreachable or skewed driver is not one
   * event but one event every few seconds for the life of every task on this executor. The local
   * decision stands either way -- that is the whole point of absorbing these failures -- so the
   * report is a diagnostic, and a diagnostic that recurs on a timer has to be aggregated or it
   * becomes the thing an operator sees instead of the cause.
   */
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
      reason: StreamingShuffleFallbackReason,
      detail: String): StreamingShuffleFallbackState = {
    val operation = "a streaming shuffle fallback declaration"
    askAny(operation, shuffleId,
      DeclareStreamingShuffleFallback(shuffleId, capabilityToken, reason.toString, detail)) match {
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

  override def completeProducer(
      shuffleId: Int,
      generation: StreamingShuffleProducerGeneration): Boolean = {
    val operation = "a streaming shuffle producer completion report"
    askAny(operation, shuffleId,
      CompleteStreamingShuffleProducer(shuffleId, capabilityToken, generation)) match {
      case Some(applied: java.lang.Boolean) => applied.booleanValue()
      case answer => unexpected(operation, shuffleId, answer, false)
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

  /**
   * One bounded ask, answering `None` for anything that goes wrong.
   *
   * Every caller here is already handling a failure or finishing a task, so an unreachable driver
   * must not become a second exception on top of the first. The reply is handed back unexamined and
   * matched on shape by the caller, so a coordinator answering something this build does not
   * understand is reported as the version skew it is instead of surfacing as a class-cast exception
   * from a cleanup path.
   */
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
