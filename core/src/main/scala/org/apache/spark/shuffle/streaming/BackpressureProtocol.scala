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
import java.security.MessageDigest
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, ConcurrentSkipListMap,
  RejectedExecutionException, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{COUNT, MAX_ATTEMPTS, MAX_SIZE, NUM_BYTES, NUM_EVENTS,
  NUM_SKIPPED, PARTITION_ID, PERCENT, PROTOCOL_VERSION, REASON, SHUFFLE_ID, THRESHOLD, VERSION_NUM}
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, HeartbeatMessage,
  RetransmitRequestMessage, StreamingShuffleMessage, StreamingShuffleMessageType,
  StreamTerminationMessage}
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

/**
 * The state a streaming shuffle stream, or the executor's whole streaming shuffle, is in as far as
 * flow control is concerned.
 *
 * @param name stable, ASCII-only label used in logs and in assertions
 * @param severity rank within the order above; higher is more severe
 */
private[spark] sealed abstract class BackpressureState(val name: String, val severity: Int) {
  override def toString: String = name
}

private[spark] object BackpressureState {

  /** Data is moving: the consumer has credit outstanding and the rate limiter is admitting. */
  case object Flowing extends BackpressureState("Flowing", 0)

  /**
   * Egress is being held back, either because the consumer's credit is exhausted or because the
   * token bucket refused an acquisition.
   */
  case object Throttled extends BackpressureState("Throttled", 1)

  /**
   * Aggregate buffer utilisation has reached `spark.shuffle.streaming.spillThreshold`, so buffered
   * partitions must be evicted to disk.
   */
  case object Spilling extends BackpressureState("Spilling", 2)

  /** Streaming cannot be sustained and the sort-based path must take over. */
  case object Degraded extends BackpressureState("Degraded", 3)

  /** Every state, in ascending severity, so a caller can enumerate them without a match. */
  val values: Seq[BackpressureState] = Seq(Flowing, Throttled, Spilling, Degraded)
}

/**
 * Why the streaming path can no longer be sustained.
 *
 * @param code stable, ASCII-only identifier suitable for a log line or an assertion
 */
private[spark] sealed abstract class BackpressureDegradationReason(val code: String) {
  override def toString: String = code
}

private[spark] object BackpressureDegradationReason {

  /**
   * The consumer has been at least twice as slow as the producer, continuously, for longer than the
   * sustained-slowness window.
   */
  case object ConsumerSustainedSlowness
    extends BackpressureDegradationReason("consumerSustainedSlowness")

  /** A buffer allocation could not be satisfied even after spilling, so streaming risks an OOM. */
  case object BufferAllocationFailure
    extends BackpressureDegradationReason("bufferAllocationFailure")

  /** Observed egress has saturated the administered link capacity beyond its safe ceiling. */
  case object LinkSaturation extends BackpressureDegradationReason("linkSaturation")

  /** A peer sent a message stamped with a wire revision this executor does not speak. */
  case object ProtocolVersionMismatch
    extends BackpressureDegradationReason("protocolVersionMismatch")

  /** Every reason, in a stable order, so a caller can enumerate them without a match. */
  val values: Seq[BackpressureDegradationReason] = Seq(
    ConsumerSustainedSlowness, BufferAllocationFailure, LinkSaturation, ProtocolVersionMismatch)
}

/**
 * Identity of one streaming shuffle stream: the flow of blocks from one producer generation of one
 * shuffle to one consumer session reading one of its reduce partitions.
 *
 * @param role which end of the flow this ledger accounts for
 * @param shuffleId shuffle the stream belongs to
 * @param mapId map output being streamed
 * @param taskAttemptId task attempt id of the producing generation
 * @param partitionId reduce partition the stream feeds
 * @param consumerId identity of the consumer session reading that partition
 */
private[spark] case class BackpressureStreamKey(
    role: BackpressureStreamRole,
    shuffleId: Int,
    mapId: Long,
    taskAttemptId: Long,
    partitionId: Int,
    consumerId: String) {

  /**
   * The identity this stream's heartbeats declare on the wire, or nothing when it declares none.
   */
  def consumerToken: Long = role match {
    case BackpressureStreamRole.Consumer => BackpressureStreamKey.consumerTokenOf(consumerId)
    case _ => HeartbeatMessage.NO_CONSUMER_TOKEN
  }

  override def toString: String =
    s"$role stream for shuffle $shuffleId map $mapId attempt $taskAttemptId partition " +
      s"$partitionId consumer $consumerId"
}

/** Which end of a stream a ledger accounts for. */
private[spark] sealed abstract class BackpressureStreamRole(val name: String) {
  override def toString: String = name
}

private[spark] object BackpressureStreamRole {

  /** The sending end: charges on send, releases on the acknowledgement it receives. */
  case object Producer extends BackpressureStreamRole("producer")

  /** The receiving end: charges on receive, releases on the acknowledgement it emits. */
  case object Consumer extends BackpressureStreamRole("consumer")

  val values: Seq[BackpressureStreamRole] = Seq(Producer, Consumer)
}

/** Constructors for [[BackpressureStreamKey]]. */
private[spark] object BackpressureStreamKey {

  /**
   * Placeholder consumer identity, used for a producer-side ledger opened before any consumer has
   * subscribed -- the interval between a writer framing its first block and a reduce task
   * announcing itself on a channel.
   */
  val ANY_CONSUMER: String = "*"

  /**
   * Reduces a consumer session identity to the fixed-width token a heartbeat declares, derived once
   * per consumer session rather than per heartbeat.
   *
   * A fixed-width token keeps a heartbeat's encoded length constant, so no field of this protocol
   * has a length a peer chooses. It is a cryptographic digest rather than a string hash because a
   * producer keys a replay cursor by it, and two sessions sharing a cursor would resume one of them
   * past output it never received: sixty-three digest bits make that collision negligible at any
   * supported scale, though not impossible, whereas a 32-bit hash collides at a rate a large reduce
   * side reaches in practice.
   *
   * @param consumerId the consumer session identity, stable for the life of one reduce task
   *     attempt
   * @return the token to declare, always positive
   */
  def consumerTokenOf(consumerId: String): Long = {
    require(consumerId != null && consumerId.nonEmpty,
      "The streaming shuffle consumer session id must not be empty.")
    val digest = MessageDigest.getInstance("SHA-256")
      .digest(consumerId.getBytes(StandardCharsets.UTF_8))
    var accumulated = 0L
    var index = 0
    while (index < java.lang.Long.BYTES) {
      accumulated = (accumulated << java.lang.Byte.SIZE) | (digest(index) & 0xffL)
      index += 1
    }
    // The sign bit is cleared rather than the value shifted right, so all sixty three bits that
    // remain still come from the digest.
    val bounded = accumulated & Long.MaxValue
    if (bounded == HeartbeatMessage.NO_CONSUMER_TOKEN) 1L else bounded
  }

  /** A key for the sending end of a stream. */
  def forProducer(
      shuffleId: Int,
      mapId: Long,
      taskAttemptId: Long,
      partitionId: Int,
      consumerId: String = ANY_CONSUMER): BackpressureStreamKey =
    BackpressureStreamKey(
      BackpressureStreamRole.Producer, shuffleId, mapId, taskAttemptId, partitionId, consumerId)

  /** A key for the receiving end of a stream. */
  def forConsumer(
      shuffleId: Int,
      mapId: Long,
      taskAttemptId: Long,
      partitionId: Int,
      consumerId: String): BackpressureStreamKey =
    BackpressureStreamKey(
      BackpressureStreamRole.Consumer, shuffleId, mapId, taskAttemptId, partitionId, consumerId)
}

/**
 * How much of the executor's buffered egress one shuffle is responsible for, which is the input to
 * priority arbitration between concurrent shuffles.
 *
 * @param shuffleId the shuffle being described
 * @param numPartitions reduce partitions registered for it, the coarse measure of its footprint
 * @param pendingBytes bytes it currently holds unacknowledged across all of its streams
 * @param streamCount streams of this shuffle currently registered on this executor
 */
private[spark] case class BackpressureShuffleDemand(
    shuffleId: Int,
    numPartitions: Int,
    pendingBytes: Long,
    streamCount: Int)

/**
 * Application-level flow control for the streaming shuffle: the credit ledger, the liveness timers
 * and the arbitration between concurrent shuffles on one executor.
 *
 * Three layers of backpressure act on the streaming path and this class owns exactly one of them,
 * the application-level credit a consumer's acknowledgements restore. Pacing belongs to
 * [[TokenBucketRateLimiter]], consulted here through its non-blocking `tryAcquire`, and TCP-level
 * throttling belongs to the client handler, which reads [[hasCredit]] but is the only party to
 * touch a channel. A refusal from any layer is expressed the same way -- the caller keeps the data
 * and retries -- and that is the pressure the spill manager reacts to.
 *
 * Timing. The bound this class enforces is [[BackpressureProtocol.ACK_TIMEOUT_MS]]: a peer that has
 * neither acknowledged nor heartbeated within it is treated as gone. Heartbeats are emitted at
 * [[BackpressureProtocol.HEARTBEAT_INTERVAL_MS]], a third of that bound, so a healthy peer may lose
 * two of them and still be judged alive. This is the application-level heartbeat, distinct from the
 * transport's OS-level TCP keepalive, which the streaming module enables but whose period the JDK
 * does not expose. No time value travels on the wire: a heartbeat carries a position and an
 * identity only, and liveness is measured from the local instant at which a frame arrived, because
 * two executors need not agree on the wall clock.
 *
 * Concurrency. This class is consulted from Netty event-loop threads and from task threads, so
 * every field is an atomic or a concurrent collection and the hot paths perform no blocking I/O,
 * never sleep and never wait for a peer to make progress; the compare-and-set retries and the short
 * critical sections around ledger lifecycle are bounded by local work alone. No shuffle metrics
 * reporter is ever called from here, because those are documented as single-threaded and belong to
 * the task thread.
 *
 * What this class detects but does not do: reaching the spill threshold and tripping a degradation
 * condition are both observed here, because this is where acknowledgement rates, the pacing bucket
 * and the message headers are seen. The spill manager evicts and the fallback policy delegates, so
 * nothing here both measures and reacts.
 *
 * @param conf the executor's configuration; the spill threshold and the debug gate are read
 *     from it once, at construction
 * @param coordinator the streaming shuffle rendezvous endpoint, which supplies the set of
 *     active shuffles and the concurrency count that the egress budget is divided by.
 * @param egressBudget the executor's whole egress allowance, from which this protocol resolves
 *     the one limiter that paces each registered shuffle.
 * @param clock time source for every liveness and rate decision, so each timer trips at exactly
 *     its bound rather than at whatever wall time happens to be
 * @param aggregateQuotaOverride the executor-wide heap allowance shared by producer buffers,
 *     consumer payloads, queue and ledger metadata and transient frame copies, or null to use the
 *     one the executor already holds
 */
private[spark] class BackpressureProtocol(
    conf: SparkConf,
    coordinator: StreamingShuffleCoordinator,
    egressBudget: TokenBucketRateLimiter.ExecutorEgressBudget,
    clock: Clock = new SystemClock,
    aggregateQuotaOverride: MemorySpillManager.ExecutorBufferQuota = null)
  extends Logging {

  require(conf != null, "The Spark configuration must not be null.")
  require(egressBudget != null, "The streaming shuffle egress budget must not be null.")
  require(clock != null, "The streaming shuffle clock must not be null.")
  private val aggregateQuota: MemorySpillManager.ExecutorBufferQuota =
    Option(aggregateQuotaOverride).getOrElse(MemorySpillManager.executorQuota(conf))

  /** Submits data-plane work to the executor-wide bounded worker stripes. */
  def executeDataPlane(owner: AnyRef, task: Runnable): Boolean = {
    require(owner != null, "The streaming shuffle data-plane owner must not be null.")
    require(task != null, "The streaming shuffle data-plane task must not be null.")
    BackpressureProtocol.executeDataPlane(owner, task)
  }

  /** Waits until the shared data-plane workers have no accepted task left to settle. */
  def awaitDataPlaneIdle(timeoutMs: Long): Boolean =
    BackpressureProtocol.awaitDataPlaneIdle(timeoutMs)

  // Read once at construction and held immutably.
  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  // Aggregate buffer utilisation, as a percentage, at which buffered partitions must be evicted.
  private val spillThreshold: Int = conf.get(config.SHUFFLE_STREAMING_SPILL_THRESHOLD)

  // The credit ledger: one entry per (shuffleId, partitionId) stream.
  private val streams =
    new ConcurrentHashMap[BackpressureStreamKey, BackpressureProtocol.StreamLedger]()

  // Reduce partition count per registered shuffle, which is one of the two arbitration keys.
  private val shufflePartitionCounts = new ConcurrentHashMap[Int, Integer]()

  // Buffered bytes and the budget they are measured against, per registered shuffle, as reported by
  // whichever component owns those buffers.
  private val shuffleBufferedBytes = new ConcurrentHashMap[Int, java.lang.Long]()

  private val shuffleBudgetBytes = new ConcurrentHashMap[Int, java.lang.Long]()

  // The limiter pacing each registered shuffle's egress, cached here so the admission path never
  // touches the budget's lock.
  private val shuffleLimiters = new ConcurrentHashMap[Int, TokenBucketRateLimiter]()

  // Reservations refused because the executor-wide budget was exhausted.
  private val receiveQuotaRefusals = new AtomicLong(0L)

  // Whether the executor-wide consumer budget is currently in an episode of exhaustion.
  private val receiveQuotaThrottled = new AtomicBoolean(false)

  // Degradation reasons observed so far.
  private val degradations =
    ConcurrentHashMap.newKeySet[BackpressureDegradationReason]()

  // Transitions into the throttled state observed by this protocol instance.
  private val throttleTransitions = new AtomicLong(0L)

  // Rate gate for the default-level throttling record.
  private val throttleLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  // Rate gate for the impossible-acknowledgement record.
  private val impossibleAckLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  // Acknowledgement-driven reclamations that were not confirmed inside the 100 ms bound.
  private val reclamationBreaches = new AtomicLong(0L)

  // Latch ensuring each degradation reason is reported at most once per protocol instance.
  private val reportedDegradations =
    ConcurrentHashMap.newKeySet[BackpressureDegradationReason]()

  // Blocks a producer could not buffer and retained on local disk instead.
  private val durableSpillAdmissions = new AtomicLong(0L)

  // Rate gate for the default-level durable-admission record.
  private val durableSpillLogGate =
    new MemorySpillManager.LogAggregationGate(MemorySpillManager.LOG_AGGREGATION_WINDOW_MS)

  // Latch for the wire-revision warning.
  private val versionMismatchReported = new AtomicBoolean(false)

  // Instant of the last pollOnce(), so that a caller driving the protocol on a timer can ask
  // whether the next poll is due instead of keeping that bookkeeping itself.
  private val lastPollMillis = new AtomicLong(clock.getTimeMillis())

  // Acknowledgements naming a position this side has never charged.
  private val impossibleAckPositions = new AtomicLong(0L)

  // Frames whose shuffle and partition contradicted the stream they were delivered as.
  private val misaddressedFrames = new AtomicLong(0L)

  // Measured link usage in each direction.
  private val egressWindow = new BackpressureProtocol.RateWindow(clock)

  private val ingressWindow = new BackpressureProtocol.RateWindow(clock)

  /**
   * The link capacity the operator declared, in bytes per second, or zero when none was declared.
   */
  private val declaredLinkCapacityBytesPerSecond: Long =
    conf.get(config.SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)
      .map(mbps => BackpressureProtocol.saturatingMultiply(
        math.max(0L, mbps.toLong), TokenBucketRateLimiter.BYTES_PER_MIB))
      .getOrElse(0L)

  /**
   * Registers a shuffle with the protocol, recording the reduce partition count that arbitration
   * and the aggregate utilisation reading are computed from.
   *
   * @param shuffleId the shuffle to register; must be non-negative
   * @param numPartitions its reduce partition count; must be positive
   */
  def registerShuffle(shuffleId: Int, numPartitions: Int): Unit = {
    require(shuffleId >= 0, s"The shuffle id must be non-negative but was $shuffleId.")
    require(numPartitions > 0,
      s"The partition count of shuffle $shuffleId must be positive but was $numPartitions.")
    shufflePartitionCounts.put(shuffleId, Integer.valueOf(numPartitions))
    shuffleBufferedBytes.putIfAbsent(shuffleId, java.lang.Long.valueOf(0L))
    shuffleBudgetBytes.putIfAbsent(shuffleId, java.lang.Long.valueOf(0L))
    // Claims this shuffle's share of the executor's egress allowance, which republishes every other
    // live shuffle's share because the divisor has just moved.
    resolveLimiter(shuffleId)
    if (debugEnabled) {
      logDebug(log"Registered shuffle ${MDC(SHUFFLE_ID, shuffleId)} with the streaming shuffle " +
        log"backpressure protocol across ${MDC(COUNT, numPartitions)} partitions")
    }
  }

  /**
   * Drops a shuffle and every stream belonging to it, releasing all of the credit those streams
   * held together with every byte of metadata their ledgers had reserved from the executor's
   * aggregate allowance.
   *
   * @param shuffleId the shuffle to drop
   * @return how many streams were dropped with it
   */
  def unregisterShuffle(shuffleId: Int): Int = {
    shufflePartitionCounts.remove(shuffleId)
    shuffleBufferedBytes.remove(shuffleId)
    shuffleBudgetBytes.remove(shuffleId)
    // Returns this shuffle's share to the shuffles that remain.
    shuffleLimiters.remove(shuffleId)
    egressBudget.release(shuffleId)
    // The keys are snapshotted first and each one is then dropped through closeAndRemove, so a
    // ledger is closed by the same indivisible step that unlinks it. Dropping a shuffle is a
    // shuffle-wide decision that overrides the per-stream owner count, and closing is what returns
    // the ledger's metadata reservations to the executor's aggregate allowance.
    var dropped = 0
    streams.keySet().asScala.toSeq.foreach { key =>
      if (key.shuffleId == shuffleId && closeAndRemove(key)) {
        dropped += 1
      }
    }
    if (dropped > 0) {
      logInfo(log"Streaming shuffle backpressure protocol dropped " +
        log"${MDC(COUNT, dropped)} stream(s) of shuffle ${MDC(SHUFFLE_ID, shuffleId)}")
    }
    dropped
  }

  /**
   * Unlinks one stream's ledger and closes it in the same indivisible step, which is the only way
   * a ledger may leave [[streams]]: `close()` is the sole path that returns the ledger's base and
   * per-entry metadata reservations to the executor's aggregate allowance, so unlinking without it
   * would charge those bytes for the lifetime of the process.
   *
   * @param key identity of the stream whose ledger is being dropped
   * @return true if a ledger was found under this key and closed
   */
  private def closeAndRemove(key: BackpressureStreamKey): Boolean = {
    var closed = false
    streams.compute(key, (_, existing) => {
      if (existing != null) {
        // close() is idempotent and the ledger refuses -- and rolls back -- every charge attempted
        // after it, so a holder that still has this reference can neither leak nor double-release.
        existing.close()
        closed = true
      }
      null
    })
    closed
  }

  /**
   * How many streams of one shuffle are registered here; zero for a shuffle this protocol has never
   * seen and zero for one whose streams have all left.
   */
  def streamCount(shuffleId: Int): Int = {
    var count = 0
    val keys = streams.keySet().iterator()
    while (keys.hasNext) {
      if (keys.next().shuffleId == shuffleId) {
        count += 1
      }
    }
    count
  }

  /**
   * The limiter pacing one shuffle's egress, resolved from the cache without taking any lock.
   *
   * @param shuffleId shuffle whose pacing is being charged
   * @return that shuffle's limiter, never null and never shared with another shuffle
   */
  private def limiterFor(shuffleId: Int): TokenBucketRateLimiter = {
    val cached = shuffleLimiters.get(shuffleId)
    if (cached != null) cached else resolveLimiter(shuffleId)
  }

  /**
   * Claims a shuffle's share of the executor's egress allowance and caches the limiter pacing it.
   *
   * @param shuffleId shuffle to allocate a share to
   * @return the limiter that now paces it
   */
  private def resolveLimiter(shuffleId: Int): TokenBucketRateLimiter = {
    val limiter = egressBudget.limiterFor(shuffleId)
    val existing = shuffleLimiters.putIfAbsent(shuffleId, limiter)
    if (existing != null) existing else limiter
  }

  /** Ids of every shuffle registered here, ascending, so a reading is always deterministic. */
  def registeredShuffleIds: Seq[Int] = shufflePartitionCounts.keySet().asScala.toSeq.sorted

  /** How many shuffles are registered here. */
  def numRegisteredShuffles: Int = shufflePartitionCounts.size()

  /**
   * Reduce partition count registered for a shuffle, or `None` when the shuffle is unknown here.
   */
  def partitionCountOf(shuffleId: Int): Option[Int] =
    Option(shufflePartitionCounts.get(shuffleId)).map(_.intValue())

  /**
   * The divisor the executor's egress budget is split by, as reported by the coordinator, clamped
   * to at least one.
   */
  def numConcurrentShuffles: Int = {
    if (coordinator == null) {
      math.max(1, numRegisteredShuffles)
    } else {
      coordinator.numConcurrentShuffles
    }
  }

  /** The shuffles whose buffers make up this executor's aggregate utilisation, ascending. */
  def concurrentShuffleIds: Seq[Int] = {
    val local = registeredShuffleIds
    if (coordinator == null || local.isEmpty) {
      local
    } else {
      val active = coordinator.activeShuffleIds.toSet
      val shared = local.filter(active.contains)
      if (shared.isEmpty) local else shared
    }
  }

  /**
   * Opens the credit ledger for one stream.
   *
   * @param key identity of the stream, including producer generation and consumer session
   * @param creditLimitBytes bytes the producer may hold unacknowledged; must be positive
   * @return true when this caller acquired an ownership claim, whether it opened or retained
   *     the ledger; false when the aggregate metadata quota had no room for a new ledger
   */
  def registerStream(key: BackpressureStreamKey, creditLimitBytes: Long): Boolean = {
    registerStreamInternal(key, creditLimitBytes, baseMetadataReserved = false)
  }

  /** Opens or retains a stream using one base-metadata charge the caller reserved in advance. */
  private[streaming] def registerStreamWithReservedMetadata(
      key: BackpressureStreamKey,
      creditLimitBytes: Long): Boolean = {
    registerStreamInternal(key, creditLimitBytes, baseMetadataReserved = true)
  }

  private def registerStreamInternal(
      key: BackpressureStreamKey,
      creditLimitBytes: Long,
      baseMetadataReserved: Boolean): Boolean = {
    require(key != null, "The streaming shuffle stream key must not be null.")
    require(key.shuffleId >= 0, s"The shuffle id must be non-negative but was ${key.shuffleId}.")
    require(key.partitionId >= 0,
      s"The partition id must be non-negative but was ${key.partitionId}.")
    require(creditLimitBytes > 0L,
      s"The credit limit of $key must be positive but was $creditLimitBytes.")
    // compute() serializes registration and unregistration of this key.
    var claimed = false
    var opened = false
    streams.compute(key, (_, existing) => {
      if (existing != null) {
        existing.retain()
        if (baseMetadataReserved) {
          aggregateQuota.release(
            BackpressureProtocol.STREAM_LEDGER_BASE_BYTES, MemorySpillManager.MetadataMemory)
        }
        claimed = true
        existing
      } else if (baseMetadataReserved || aggregateQuota.tryReserve(
          BackpressureProtocol.STREAM_LEDGER_BASE_BYTES, MemorySpillManager.MetadataMemory)) {
        opened = true
        claimed = true
        new BackpressureProtocol.StreamLedger(
          key, creditLimitBytes, clock.getTimeMillis(), aggregateQuota)
      } else {
        null
      }
    })
    if (opened && debugEnabled) {
      logDebug(log"Opened a streaming shuffle credit ledger for shuffle " +
        log"${MDC(SHUFFLE_ID, key.shuffleId)} partition " +
        log"${MDC(PARTITION_ID, key.partitionId)} with " +
        log"${MDC(NUM_BYTES, creditLimitBytes)} bytes of credit")
    }
    claimed
  }

  /**
   * Releases one owner's claim on a stream's ledger, closing it -- and with it every byte of credit
   * it held -- once the last owner has let go.
   *
   * @param key identity of the stream whose ledger is being released
   * @return true if this call closed the ledger
   */
  def unregisterStream(key: BackpressureStreamKey): Boolean = {
    if (key == null) {
      false
    } else {
      var closed = false
      streams.compute(key, (_, existing) => {
        if (existing == null) {
          null
        } else if (existing.releaseOwner()) {
          closed = true
          existing.close()
          null
        } else {
          existing
        }
      })
      closed
    }
  }

  /** Whether a ledger is open for this stream. */
  def isStreamRegistered(key: BackpressureStreamKey): Boolean =
    streams.containsKey(key)

  /** How many streams currently hold a ledger. */
  def streamCount: Int = streams.size()

  /**
   * Every registered stream, ordered by shuffle id and then partition id, so that a reading of the
   * ledger set is deterministic and an assertion can compare it directly.
   */
  def registeredStreams: Seq[BackpressureStreamKey] = {
    streams.keySet().asScala.toSeq.sortBy(key => (key.shuffleId, key.partitionId))
  }

  /**
   * Asks whether a block may be sent now, and charges it against both the consumer's credit and the
   * pacing bucket when the answer is yes.
   *
   * @param key the stream the block belongs to
   * @param bytes encoded size of the block; a non-positive size is admitted without charge
   * @param sequenceNumber position of the block within its partition's stream; must be
   *     non-negative
   * @return true if the caller may send the block now, false if it must hold it and retry
   */
  def tryAdmit(
      key: BackpressureStreamKey,
      bytes: Long,
      sequenceNumber: Long): Boolean = {
    require(sequenceNumber >= 0L,
      s"The sequence number must be non-negative but was $sequenceNumber.")
    val ledger = streams.get(key)
    if (ledger == null) {
      true
    } else if (bytes <= 0L) {
      // An empty block consumes neither credit nor tokens, so it can never be the cause of
      // backpressure and must not be recorded in a window that exists to be retransmitted.
      true
    } else if (!ledger.hasCreditFor(bytes)) {
      enterThrottled(ledger, BackpressureProtocol.THROTTLE_CAUSE_CREDIT)
      false
    } else if (!limiterFor(key.shuffleId).tryAcquire(bytes)) {
      enterThrottled(ledger, BackpressureProtocol.THROTTLE_CAUSE_RATE)
      false
    } else if (ledger.recordSent(sequenceNumber, bytes)) {
      recordEgress(bytes)
      true
    } else {
      limiterFor(key.shuffleId).refund(bytes)
      enterThrottled(ledger, BackpressureProtocol.THROTTLE_CAUSE_MEMORY)
      false
    }
  }

  /** Accumulates bytes this executor handed to the wire. */
  private def recordEgress(bytes: Long): Unit = egressWindow.record(bytes)

  /** Accumulates bytes this executor took off the wire. */
  private def recordIngress(bytes: Long): Unit = ingressWindow.record(bytes)

  /**
   * Measured egress over the last completed interval, in bytes per second, across every stream on
   * this executor.
   */
  def egressBytesPerSecond: Long = egressWindow.bytesPerSecond

  /**
   * Measured ingress over the last completed interval, in bytes per second, across every stream on
   * this executor.
   */
  def ingressBytesPerSecond: Long = ingressWindow.bytesPerSecond

  /** Bytes admitted to the wire since this protocol was created. */
  def egressBytes: Long = egressWindow.total

  /** Bytes taken off the wire since this protocol was created. */
  def ingressBytes: Long = ingressWindow.total

  /** The administered link capacity in bytes per second, or `None` when none was declared. */
  def declaredLinkCapacity: Option[Long] =
    if (declaredLinkCapacityBytesPerSecond > 0L) Some(declaredLinkCapacityBytesPerSecond) else None

  /** Frames that were delivered as a stream whose shuffle or partition they did not name. */
  def misaddressedFrameCount: Long = misaddressedFrames.get()

  /** Whether the producer may send anything at all to this consumer right now. */
  def hasCredit(key: BackpressureStreamKey): Boolean = {
    val ledger = streams.get(key)
    ledger == null || ledger.isTerminated || ledger.hasCredit
  }

  /**
   * Bytes of credit still available to this stream, or `Long.MaxValue` for a stream with no ledger,
   * which is the honest answer to "how much may I send" when no allowance is being tracked.
   */
  def availableCreditBytes(key: BackpressureStreamKey): Long = {
    val ledger = streams.get(key)
    if (ledger == null) Long.MaxValue else ledger.availableCredit
  }

  /** Bytes this stream has sent but not yet had acknowledged; zero for an unknown stream. */
  def outstandingBytes(key: BackpressureStreamKey): Long = {
    val ledger = streams.get(key)
    if (ledger == null) 0L else ledger.outstandingBytes
  }

  /** The credit limit this stream was opened with, or zero for an unknown stream. */
  def creditLimitBytes(key: BackpressureStreamKey): Long = {
    val ledger = streams.get(key)
    if (ledger == null) 0L else ledger.creditLimitBytes
  }

  /** Bytes this stream has sent in total, acknowledged or not; zero for an unknown stream. */
  def sentBytes(key: BackpressureStreamKey): Long = {
    val ledger = streams.get(key)
    if (ledger == null) 0L else ledger.sentBytes
  }

  /** Bytes of this stream the consumer has acknowledged; zero for an unknown stream. */
  def acknowledgedBytes(key: BackpressureStreamKey): Long = {
    val ledger = streams.get(key)
    if (ledger == null) 0L else ledger.acknowledgedBytes
  }

  /**
   * Applies a consumer acknowledgement, which is the single event that frees producer memory and
   * lifts backpressure.
   *
   * @param key identity of the stream being acknowledged
   * @param consumerPosition highest block sequence number the consumer has consumed, or
   *     [[BackpressureProtocol.NOTHING_ACKNOWLEDGED]] when it has consumed nothing
   * @return bytes released by this acknowledgement, zero if it advanced nothing or was refused
   */
  def onAck(key: BackpressureStreamKey, consumerPosition: Long): Long =
    tryAcknowledge(key, consumerPosition).getOrElse(0L)

  /**
   * Applies a consumer acknowledgement, reporting a refusal distinctly from a no-op.
   *
   * @return bytes released, or `None` when the acknowledgement was refused as impossible
   */
  def tryAcknowledge(key: BackpressureStreamKey, consumerPosition: Long): Option[Long] = {
    val ledger = streams.get(key)
    if (ledger == null) {
      Some(0L)
    } else {
      val nowMillis = clock.getTimeMillis()
      ledger.applyAck(consumerPosition, nowMillis) match {
        case None =>
          val refusals = impossibleAckPositions.incrementAndGet()
          impossibleAckLogGate.admit(nowMillis) match {
            case Some(unreported) =>
              logWarning(log"Refusing a streaming shuffle acknowledgement for shuffle " +
                log"${MDC(SHUFFLE_ID, key.shuffleId)} partition " +
                log"${MDC(PARTITION_ID, key.partitionId)} that named position " +
                log"${MDC(COUNT, consumerPosition)} beyond the highest position charged, " +
                log"${MDC(MAX_SIZE, ledger.highestChargedSequenceNumber)} " +
                log"(${MDC(NUM_EVENTS, refusals)} refusal(s) on this executor, " +
                log"${MDC(NUM_SKIPPED, unreported)} not reported individually)")
            case None =>
              if (debugEnabled) {
                logDebug(log"Refusing a streaming shuffle acknowledgement for shuffle " +
                  log"${MDC(SHUFFLE_ID, key.shuffleId)} partition " +
                  log"${MDC(PARTITION_ID, key.partitionId)} that named position " +
                  log"${MDC(COUNT, consumerPosition)}")
              }
          }
          None
        case Some(released) =>
          if (released > 0L || ledger.hasCredit) {
            // Credit has been restored, so an episode of credit-driven throttling is over.
            leaveThrottled(ledger)
          }
          if (debugEnabled && released > 0L) {
            logDebug(log"Acknowledgement released ${MDC(NUM_BYTES, released)} bytes of streaming " +
              log"shuffle credit for shuffle ${MDC(SHUFFLE_ID, key.shuffleId)} partition " +
              log"${MDC(PARTITION_ID, key.partitionId)}")
          }
          Some(released)
      }
    }
  }

  /** Acknowledgements refused because they named a position this side had never charged. */
  def impossibleAckPositionCount: Long = impossibleAckPositions.get()

  /**
   * The highest sequence number this stream has charged, and therefore the highest position an
   * acknowledgement for it may name.
   */
  def highestChargedSequenceNumber(key: BackpressureStreamKey): Long = {
    val ledger = streams.get(key)
    if (ledger == null) BackpressureProtocol.NO_SEQUENCE else ledger.highestChargedSequenceNumber
  }

  /**
   * Applies an acknowledgement that arrived on the wire.
   *
   * @param key identity of the stream the frame arrived for
   * @param ack the acknowledgement received; a null message releases nothing
   * @return bytes released by this acknowledgement
   */
  def onAck(key: BackpressureStreamKey, ack: AckMessage): Long =
    tryAcknowledge(key, ack).getOrElse(0L)

  /**
   * Applies an acknowledgement that arrived on the wire, reporting a refusal distinctly from a
   * no-op.
   *
   * @return bytes released, or `None` when the frame was refused
   */
  def tryAcknowledge(key: BackpressureStreamKey, ack: AckMessage): Option[Long] = {
    if (ack == null || !addresses(key, ack)) {
      None
    } else {
      tryAcknowledge(key, ack.consumerPosition())
    }
  }

  /**
   * Whether a frame's stream identity -- shuffle, producer map, and reduce partition -- matches the
   * stream it was delivered as.
   */
  private def addresses(key: BackpressureStreamKey, message: StreamingShuffleMessage): Boolean = {
    val matches = key != null && message.shuffleId() == key.shuffleId &&
      message.mapId() == key.mapId && message.partitionId() == key.partitionId
    if (!matches) {
      misaddressedFrames.incrementAndGet()
    }
    matches
  }

  /**
   * Highest block sequence number the consumer of this stream has acknowledged, or
   * [[BackpressureProtocol.NOTHING_ACKNOWLEDGED]] when it has acknowledged nothing and for an
   * unknown stream.
   */
  def acknowledgedPosition(key: BackpressureStreamKey): Long = {
    val ledger = streams.get(key)
    if (ledger == null) BackpressureProtocol.NOTHING_ACKNOWLEDGED else ledger.acknowledgedPosition
  }

  /**
   * Confirms that the buffers freed by the most recent acknowledgement have been released, and
   * reports how long that took.
   *
   * @return the observed latency in milliseconds, or `None` when no acknowledgement is awaiting
   *     confirmation for this stream
   */
  def confirmReclamation(key: BackpressureStreamKey): Option[Long] = {
    val ledger = streams.get(key)
    if (ledger == null) {
      None
    } else {
      val latency = ledger.confirmReclamation(clock.getTimeMillis())
      latency.foreach { observed =>
        if (observed > BackpressureProtocol.RECLAMATION_DEADLINE_MS) {
          reclamationBreaches.incrementAndGet()
        }
      }
      latency
    }
  }

  /**
   * Whether an acknowledgement for this stream is still awaiting a reclamation confirmation and the
   * 100 ms bound has already elapsed.
   */
  def isReclamationOverdue(key: BackpressureStreamKey): Boolean = {
    val ledger = streams.get(key)
    ledger != null && ledger.isReclamationOverdue(clock.getTimeMillis(),
      BackpressureProtocol.RECLAMATION_DEADLINE_MS)
  }

  /** Reclamations confirmed outside the 100 ms bound, across every stream of this protocol. */
  def reclamationDeadlineBreaches: Long = reclamationBreaches.get()

  /**
   * Inclusive bounds of the window of blocks this stream has sent but not had acknowledged, or
   * `None` when nothing is outstanding.
   */
  def unacknowledgedWindow(key: BackpressureStreamKey): Option[(Long, Long)] = {
    val ledger = streams.get(key)
    if (ledger == null) None else ledger.unacknowledgedWindow
  }

  /** How many blocks this stream has sent but not had acknowledged. */
  def unacknowledgedBlockCount(key: BackpressureStreamKey): Int = {
    val ledger = streams.get(key)
    if (ledger == null) 0 else ledger.unacknowledgedBlockCount
  }

  /** Whether one block is still inside this stream's unacknowledged window. */
  def isWithinUnacknowledgedWindow(
      key: BackpressureStreamKey,
      sequenceNumber: Long): Boolean = {
    val ledger = streams.get(key)
    ledger != null && ledger.containsUnacknowledged(sequenceNumber)
  }

  /**
   * Whether a retransmission request can be served in full.
   *
   * @param request the retransmission request received; a null request is never serviceable
   */
  def canServeRetransmit(
      key: BackpressureStreamKey,
      request: RetransmitRequestMessage): Boolean = {
    if (request == null || !addresses(key, request)) {
      false
    } else {
      val ledger = streams.get(key)
      ledger != null &&
        ledger.retainedCountWithin(
          request.firstSequenceNumber(), request.lastSequenceNumber()) == request.blockCount()
    }
  }

  /**
   * Records an inbound retransmission request and reports whether it can be served.
   *
   * @return true if the producer may replay every block the request names
   */
  def onRetransmitRequest(
      key: BackpressureStreamKey,
      request: RetransmitRequestMessage): Boolean = {
    if (request == null || !addresses(key, request)) {
      false
    } else {
      val ledger = streams.get(key)
      if (ledger != null) {
        ledger.recordInbound(clock.getTimeMillis())
      }
      val serviceable = ledger != null && canServeRetransmit(key, request)
      if (serviceable) {
        val attempt = ledger.recordRetransmitAttempt()
        if (debugEnabled) {
          logDebug(log"Serving a streaming shuffle retransmission of " +
            log"${MDC(COUNT, request.blockCount())} block(s) for shuffle " +
            log"${MDC(SHUFFLE_ID, request.shuffleId())} partition " +
            log"${MDC(PARTITION_ID, request.partitionId())} on attempt " +
            log"${MDC(MAX_ATTEMPTS, attempt)}")
        }
      }
      serviceable
    }
  }

  /**
   * The pause a caller should observe before the next replay of this stream, or `None` when the
   * attempt budget is spent and the failure must be escalated instead of retried.
   */
  def nextRetryBackoffMillis(key: BackpressureStreamKey): Option[Long] = {
    val ledger = streams.get(key)
    if (ledger == null) {
      Some(BackpressureProtocol.RETRY_BASE_BACKOFF_MS)
    } else {
      val attempt = ledger.retransmitAttempts
      if (attempt >= BackpressureProtocol.MAX_RETRY_ATTEMPTS) {
        None
      } else {
        Some(BackpressureProtocol.retryBackoffMillis(attempt + 1))
      }
    }
  }

  /** Replay attempts already made for this stream since it last made progress. */
  def retransmitAttempts(key: BackpressureStreamKey): Int = {
    val ledger = streams.get(key)
    if (ledger == null) 0 else ledger.retransmitAttempts
  }

  /**
   * Whether this stream has spent its five replay attempts, so that the next failure must be
   * escalated to a fetch failure rather than retried.
   */
  def isRetryExhausted(key: BackpressureStreamKey): Boolean = {
    val ledger = streams.get(key)
    ledger != null && ledger.retransmitAttempts >= BackpressureProtocol.MAX_RETRY_ATTEMPTS
  }

  /**
   * Asks whether a *retained* block may be replayed now, charging the pacing bucket and the replay
   * accounting when the answer is yes.
   *
   * @param key identity of the stream being repaired
   * @param sequenceNumber position of the replayed block
   * @param bytes encoded size of the replayed block on the wire
   * @return true if the caller may write the replay now, false if it must hold it and retry
   */
  def tryAdmitReplay(
      key: BackpressureStreamKey,
      bytes: Long,
      sequenceNumber: Long): Boolean = {
    require(sequenceNumber >= 0L,
      s"The sequence number must be non-negative but was $sequenceNumber.")
    val ledger = streams.get(key)
    if (ledger == null) {
      true
    } else if (bytes <= 0L) {
      true
    } else if (!limiterFor(key.shuffleId).tryAcquire(bytes)) {
      enterThrottled(ledger, BackpressureProtocol.THROTTLE_CAUSE_RATE)
      false
    } else if (ledger.recordReplay(sequenceNumber, bytes)) {
      recordEgress(bytes)
      true
    } else {
      limiterFor(key.shuffleId).refund(bytes)
      enterThrottled(ledger, BackpressureProtocol.THROTTLE_CAUSE_MEMORY)
      false
    }
  }

  /** Bytes this stream has replayed to repair losses; zero for an unknown stream. */
  def replayedBytes(key: BackpressureStreamKey): Long = {
    val ledger = streams.get(key)
    if (ledger == null) 0L else ledger.replayedBytes
  }

  /**
   * Records inbound data on a stream, which is both consumer-side progress and proof that the
   * producer is alive.
   *
   * @param key the stream the block arrived on
   * @param sequenceNumber position of the block within the stream; must be non-negative
   * @param bytes encoded size of the block; a non-positive size records activity but no volume
   */
  def onDataReceived(
      key: BackpressureStreamKey,
      sequenceNumber: Long,
      bytes: Long): Boolean = {
    require(sequenceNumber >= 0L,
      s"The sequence number must be non-negative but was $sequenceNumber.")
    val ledger = streams.get(key)
    val recorded =
      ledger == null || ledger.recordReceived(sequenceNumber, bytes, clock.getTimeMillis())
    // Recorded whether or not a ledger exists, because the link carried these bytes regardless of
    // whether this protocol had been told about the stream that carried them.
    recordIngress(bytes)
    recorded
  }

  /**
   * Records the arrival of a block the receiver is going to discard.
   *
   * @param key identity of the stream the frame arrived for
   * @param bytes encoded size of the block being discarded
   */
  def onDiscardedData(key: BackpressureStreamKey, bytes: Long): Unit = {
    val ledger = streams.get(key)
    if (ledger != null) {
      ledger.recordInbound(clock.getTimeMillis())
    }
    recordIngress(bytes)
  }

  /** Records that a heartbeat was received for a stream. */
  def onHeartbeat(key: BackpressureStreamKey): Unit = {
    val ledger = streams.get(key)
    if (ledger != null) {
      ledger.recordHeartbeat(clock.getTimeMillis(), BackpressureProtocol.NO_TIMESTAMP)
    }
  }

  /**
   * Records a heartbeat that arrived on the wire, judging liveness from the local instant of
   * arrival.
   *
   * @param heartbeat the heartbeat received; a null message records nothing
   */
  def onHeartbeat(key: BackpressureStreamKey, heartbeat: HeartbeatMessage): Unit = {
    if (heartbeat != null && addresses(key, heartbeat)) {
      val ledger = streams.get(key)
      if (ledger != null) {
        val arrivedAtMillis = clock.getTimeMillis()
        ledger.recordHeartbeat(arrivedAtMillis, arrivedAtMillis)
      }
    }
  }

  /**
   * The local instant at which the most recent heartbeat of this stream arrived, or `None` when
   * none has.
   */
  def remoteHeartbeatTimestamp(key: BackpressureStreamKey): Option[Long] = {
    val ledger = streams.get(key)
    if (ledger == null) None else ledger.remoteHeartbeatTimestamp
  }

  /**
   * Whether this stream is due to emit a heartbeat, that is whether the last one it sent is older
   * than [[BackpressureProtocol.HEARTBEAT_INTERVAL_MS]].
   */
  def shouldSendHeartbeat(key: BackpressureStreamKey): Boolean = {
    val ledger = streams.get(key)
    ledger != null && !ledger.isTerminated &&
      ledger.isHeartbeatDue(clock.getTimeMillis(), BackpressureProtocol.HEARTBEAT_INTERVAL_MS)
  }

  /**
   * Builds the heartbeat to send for a stream, positioned at the <b>next</b> block position this
   * side of the stream will use. The clock is read only to restart this side's own interval; the
   * message itself carries no time value.
   *
   * @param nextPosition next block position this side of the stream will use; clamped at zero,
   *     because the message type refuses a negative sequence number and a side that has not started
   *     announces zero, which the resume handshake reads as "serve me from the beginning"
   * @return the heartbeat to send, or `None` for a stream with no ledger
   */
  def heartbeatFor(key: BackpressureStreamKey, nextPosition: Long): Option[HeartbeatMessage] = {
    val ledger = streams.get(key)
    if (ledger == null) {
      None
    } else {
      ledger.recordHeartbeatSent(clock.getTimeMillis())
      // The position is clamped because the message type refuses a negative sequence number, and a
      // side that has not started announces zero rather than a sentinel.
      Some(new HeartbeatMessage(key.shuffleId, key.mapId, key.partitionId,
        math.max(0L, nextPosition), key.consumerToken))
    }
  }

  /**
   * Records that a producer has announced the orderly end of a stream.
   *
   * @param key the stream that has finished
   * @param totalBlocks blocks the producer claims to have sent; must be non-negative, and zero
   *     is the valid announcement of an empty partition
   */
  def onStreamTermination(key: BackpressureStreamKey, totalBlocks: Long): Unit = {
    require(totalBlocks >= 0L,
      s"The total block count must be non-negative but was $totalBlocks.")
    val ledger = streams.get(key)
    if (ledger != null) {
      ledger.recordTermination(totalBlocks, clock.getTimeMillis())
      // One line per stream at completion is O(reduce partitions), the same order as the
      // coordinator's shuffle-level logging, so it stays inside the log budget with debug off.
      if (debugEnabled) {
        logDebug(log"Streaming shuffle stream for shuffle ${MDC(SHUFFLE_ID, key.shuffleId)} " +
          log"partition ${MDC(PARTITION_ID, key.partitionId)} terminated after " +
          log"${MDC(COUNT, totalBlocks)} block(s)")
      }
    }
  }

  /**
   * Records an orderly end-of-stream that arrived on the wire.
   *
   * @param termination the terminator received; a null message records nothing
   */
  def onStreamTermination(
      key: BackpressureStreamKey,
      termination: StreamTerminationMessage): Unit = {
    if (termination != null && addresses(key, termination)) {
      onStreamTermination(key, termination.totalBlocks())
    }
  }

  /** Whether a producer has announced the orderly end of this stream. */
  def isStreamTerminated(key: BackpressureStreamKey): Boolean = {
    val ledger = streams.get(key)
    ledger != null && ledger.isTerminated
  }

  /**
   * Blocks the producer of this stream claims to have sent, or `None` when it has not yet announced
   * the end of the stream.
   */
  def announcedBlockCount(key: BackpressureStreamKey): Option[Long] = {
    val ledger = streams.get(key)
    if (ledger == null) None else ledger.announcedBlockCount
  }

  /**
   * Applies one inbound control message to the ledger, dispatching on its concrete type.
   *
   * @param message the message received; a null message is unhandled
   * @return the type that was applied, or `None` when nothing was
   */
  def onControlMessage(
      key: BackpressureStreamKey,
      message: StreamingShuffleMessage): Option[StreamingShuffleMessageType] = {
    if (message == null || !addresses(key, message)) {
      None
    } else if (!observeProtocolVersion(key, message.protocolVersion())) {
      None
    } else {
      message match {
        case ack: AckMessage =>
          onAck(key, ack)
          Some(StreamingShuffleMessageType.ACK)
        case heartbeat: HeartbeatMessage =>
          onHeartbeat(key, heartbeat)
          Some(StreamingShuffleMessageType.HEARTBEAT)
        case request: RetransmitRequestMessage =>
          onRetransmitRequest(key, request)
          Some(StreamingShuffleMessageType.RETRANSMIT_REQUEST)
        case termination: StreamTerminationMessage =>
          onStreamTermination(key, termination)
          Some(StreamingShuffleMessageType.STREAM_TERMINATION)
        case _ =>
          None
      }
    }
  }

  /**
   * Whether the producer of this stream has fallen silent for longer than the five-second
   * connection timeout, which is the detection the reader turns into a partial-read invalidation
   * and a fetch failure.
   */
  def isProducerTimedOut(key: BackpressureStreamKey): Boolean = {
    val ledger = streams.get(key)
    ledger != null && isProducerTimedOut(ledger, clock.getTimeMillis())
  }

  /**
   * Whether the consumer of this stream has failed to acknowledge for longer than the ten-second
   * liveness window, which is the detection the writer turns into retention, spill and replay of
   * the unacknowledged window.
   */
  def isConsumerTimedOut(key: BackpressureStreamKey): Boolean = {
    val ledger = streams.get(key)
    ledger != null && isConsumerTimedOut(ledger, clock.getTimeMillis())
  }

  /**
   * Every stream whose producer has fallen silent past the five-second bound, in a deterministic
   * order.
   */
  def timedOutProducerStreams: Seq[BackpressureStreamKey] = {
    val nowMillis = clock.getTimeMillis()
    streams.values().asScala
      .filter(ledger => isProducerTimedOut(ledger, nowMillis))
      .map(_.key)
      .toSeq
      .sortBy(key => (key.shuffleId, key.partitionId))
  }

  /** Every stream whose consumer has stopped acknowledging past the ten-second bound. */
  def timedOutConsumerStreams: Seq[BackpressureStreamKey] = {
    val nowMillis = clock.getTimeMillis()
    streams.values().asScala
      .filter(ledger => isConsumerTimedOut(ledger, nowMillis))
      .map(_.key)
      .toSeq
      .sortBy(key => (key.shuffleId, key.partitionId))
  }

  /**
   * Milliseconds since anything at all was received on this stream, or `None` for an unknown
   * stream.
   */
  def millisSinceInbound(key: BackpressureStreamKey): Option[Long] = {
    val ledger = streams.get(key)
    if (ledger == null) None else Some(ledger.millisSinceInbound(clock.getTimeMillis()))
  }

  /** Milliseconds since this stream's consumer last advanced its position. */
  def millisSinceAck(key: BackpressureStreamKey): Option[Long] = {
    val ledger = streams.get(key)
    if (ledger == null) None else Some(ledger.millisSinceAck(clock.getTimeMillis()))
  }

  /**
   * Publishes the buffered bytes and the budget one participating shuffle sees, so that utilisation
   * can be read without this class reaching into any shuffle's memory accounting.
   *
   * @param shuffleId the shuffle reporting
   * @param bufferedBytes bytes currently held against the allowance the reporter measures
   *     against
   * @param budgetBytes that allowance
   */
  def reportBufferUtilization(shuffleId: Int, bufferedBytes: Long, budgetBytes: Long): Unit = {
    if (shufflePartitionCounts.containsKey(shuffleId)) {
      shuffleBufferedBytes.put(shuffleId, java.lang.Long.valueOf(math.max(0L, bufferedBytes)))
      shuffleBudgetBytes.put(shuffleId, java.lang.Long.valueOf(math.max(0L, budgetBytes)))
    }
  }

  /**
   * Executor-wide buffer utilisation across the concurrent shuffles, as a percentage of their
   * aggregate budget.
   */
  def aggregateBufferUtilizationPercent: Long = {
    var bufferedBytes = 0L
    var budgetBytes = 0L
    concurrentShuffleIds.foreach { shuffleId =>
      bufferedBytes =
        BackpressureProtocol.saturatingAdd(bufferedBytes, readLongEntry(shuffleBufferedBytes,
          shuffleId))
      budgetBytes =
        BackpressureProtocol.saturatingAdd(budgetBytes, readLongEntry(shuffleBudgetBytes,
          shuffleId))
    }
    BackpressureProtocol.percentOf(bufferedBytes, budgetBytes)
  }

  /** The configured utilisation percentage at which buffered partitions must be evicted. */
  def spillThresholdPercent: Int = spillThreshold

  /** Whether aggregate utilisation has reached the configured spill threshold. */
  def isSpillRequired: Boolean = aggregateBufferUtilizationPercent >= spillThreshold.toLong

  /**
   * The concurrent shuffles ordered by how much of the executor's buffered egress each is
   * responsible for, most demanding first.
   */
  def arbitrationOrder: Seq[BackpressureShuffleDemand] = {
    // One pass over the ledger set folds every stream into its shuffle's totals, so the cost is
    // linear in the number of streams rather than quadratic in shuffles times streams. Plain
    // mutable maps suffice because they never escape this method.
    val pending = mutable.Map.empty[Int, Long]
    val counts = mutable.Map.empty[Int, Int]
    val ledgers = streams.values().iterator()
    while (ledgers.hasNext) {
      val ledger = ledgers.next()
      val shuffleId = ledger.key.shuffleId
      pending(shuffleId) = BackpressureProtocol.saturatingAdd(
        pending.getOrElse(shuffleId, 0L), ledger.outstandingBytes)
      counts(shuffleId) = counts.getOrElse(shuffleId, 0) + 1
    }
    concurrentShuffleIds.map { shuffleId =>
      BackpressureShuffleDemand(
        shuffleId,
        partitionCountOf(shuffleId).getOrElse(0),
        pending.getOrElse(shuffleId, 0L),
        counts.getOrElse(shuffleId, 0))
    }.sortBy { demand =>
      // Negating the two descending keys keeps the whole comparison in one total ordering, and the
      // ascending shuffle id breaks every remaining tie, so the result never depends on the order
      // the concurrent map happened to enumerate.
      (-demand.pendingBytes, -demand.numPartitions, demand.shuffleId)
    }
  }

  /** The arbitration order reduced to shuffle ids, the one that must yield first leading. */
  def yieldOrder: Seq[Int] = arbitrationOrder.map(_.shuffleId)

  /**
   * The one shuffle this arbitration never asks to yield while the executor is over its spill
   * threshold, namely the least demanding of the concurrent shuffles.
   */
  def guaranteedShuffleId: Option[Int] = yieldOrder.lastOption

  /** Whether one shuffle should yield buffer space on this pass. */
  def shouldYield(shuffleId: Int): Boolean = {
    isSpillRequired && guaranteedShuffleId.exists(_ != shuffleId) &&
      shufflePartitionCounts.containsKey(shuffleId)
  }

  /**
   * Bytes per second the producer of this stream has sustained since the stream opened, or zero
   * while less than a millisecond has elapsed and for an unknown stream.
   */
  def producerRateBytesPerSecond(key: BackpressureStreamKey): Long = {
    val ledger = streams.get(key)
    if (ledger == null) 0L else ledger.producerRate(clock.getTimeMillis())
  }

  /** Bytes per second the consumer of this stream has acknowledged since the stream opened. */
  def consumerRateBytesPerSecond(key: BackpressureStreamKey): Long = {
    val ledger = streams.get(key)
    if (ledger == null) 0L else ledger.consumerRate(clock.getTimeMillis())
  }

  /** Whether this stream's consumer is at least twice as slow as its producer right now. */
  def isConsumerSlow(key: BackpressureStreamKey): Boolean = {
    val ledger = streams.get(key)
    ledger != null && ledger.isConsumerSlow(BackpressureProtocol.CONSUMER_SLOWNESS_RATIO)
  }

  /**
   * Whether this stream's consumer has been at least twice as slow as its producer continuously for
   * longer than sixty seconds, which is the first of the four conditions under which streaming
   * steps aside.
   *
   * Asking the question records an observation, and [[pollOnce]] records one for every ledger on
   * its own timer, so a stream is normally observed twice in the same interval. That composes
   * safely and deliberately: the recorder latches the instant slowness began and clears the latch
   * only when the stream stops being slow, so a second observation inside one interval can neither
   * move the latch forward nor change the verdict this method returns.
   */
  def isConsumerSustainedSlow(key: BackpressureStreamKey): Boolean = {
    val ledger = streams.get(key)
    if (ledger == null) {
      false
    } else {
      val sustained = ledger.observeSlowness(
        clock.getTimeMillis(),
        BackpressureProtocol.CONSUMER_SLOWNESS_RATIO,
        BackpressureProtocol.SUSTAINED_SLOWNESS_WINDOW_MS)
      if (sustained) {
        latchDegradation(BackpressureDegradationReason.ConsumerSustainedSlowness)
      }
      sustained
    }
  }

  // The consumer's share of the executor-wide aggregate quota.

  /**
   * The aggregate allowance consumers share with every other streaming allocation on the executor.
   */
  def receiveQuotaBytes: Long = aggregateQuota.totalBytes

  /** Bytes of that allowance currently held by consumers anywhere on this executor. */
  def reservedReceiveQuotaBytes: Long =
    aggregateQuota.reservedBytes(MemorySpillManager.ConsumerMemory)

  /** Bytes held by every streaming allocation category on this executor. */
  def aggregateReservedBytes: Long = aggregateQuota.reservedBytes

  /** Bytes still available to every streaming allocation category on this executor. */
  def aggregateAvailableBytes: Long = aggregateQuota.availableBytes

  /** How many consumer-side reservations have been refused because the allowance was exhausted. */
  def receiveQuotaRefusalCount: Long = receiveQuotaRefusals.get()

  /** Whether the executor-wide aggregate allowance is fully committed right now. */
  def receiveQuotaExhausted: Boolean = aggregateQuota.availableBytes <= 0L

  /**
   * Reserves consumer-side heap for one frame, or refuses it.
   *
   * @param bytes payload bytes the caller is about to retain; must not be negative
   * @return true when the bytes were charged, false when the executor's allowance is exhausted
   * @throws IllegalArgumentException when `bytes` is negative
   */
  def tryReserveReceiveQuota(bytes: Long): Boolean = {
    require(bytes >= 0L, s"Reserved receive bytes must be non-negative but was $bytes.")
    if (bytes == 0L) {
      true
    } else if (aggregateQuota.tryReserve(bytes, MemorySpillManager.ConsumerMemory)) {
      true
    } else {
      receiveQuotaRefusals.incrementAndGet()
      // Counted as a throttle rather than latched as a degradation: an exhausted aggregate
      // allowance is flow control working as designed.
      enterReceiveQuotaThrottle()
      false
    }
  }

  /**
   * Returns consumer-side heap to the executor's allowance.
   *
   * @param bytes payload bytes the caller has finished with; must not be negative
   * @throws IllegalArgumentException when `bytes` is negative
   */
  def releaseReceiveQuota(bytes: Long): Unit = {
    require(bytes >= 0L, s"Released receive bytes must be non-negative but was $bytes.")
    if (bytes > 0L) {
      aggregateQuota.release(bytes, MemorySpillManager.ConsumerMemory)
      // The episode ends here, so the next refusal counts a fresh edge.
      receiveQuotaThrottled.set(false)
    }
  }

  /** Reserves transient frame-copy bytes inside the executor's aggregate allowance. */
  def tryReserveTransientQuota(bytes: Long): Boolean = {
    require(bytes >= 0L, s"Reserved transient bytes must be non-negative but was $bytes.")
    bytes == 0L ||
      aggregateQuota.tryReserve(bytes, MemorySpillManager.TransientMemory)
  }

  /** Returns transient frame-copy bytes to the executor's aggregate allowance. */
  def releaseTransientQuota(bytes: Long): Unit = {
    require(bytes >= 0L, s"Released transient bytes must be non-negative but was $bytes.")
    aggregateQuota.release(bytes, MemorySpillManager.TransientMemory)
  }

  /** Transient frame-copy bytes currently in flight across this executor. */
  def reservedTransientQuotaBytes: Long =
    aggregateQuota.reservedBytes(MemorySpillManager.TransientMemory)

  /** Reserves queue or ledger metadata inside the executor's aggregate allowance. */
  def tryReserveMetadataQuota(bytes: Long): Boolean = {
    require(bytes >= 0L, s"Reserved metadata bytes must be non-negative but was $bytes.")
    bytes == 0L ||
      aggregateQuota.tryReserve(bytes, MemorySpillManager.MetadataMemory)
  }

  /** Returns queue or ledger metadata to the executor's aggregate allowance. */
  def releaseMetadataQuota(bytes: Long): Unit = {
    require(bytes >= 0L, s"Released metadata bytes must be non-negative but was $bytes.")
    aggregateQuota.release(bytes, MemorySpillManager.MetadataMemory)
  }

  /** Queue and ledger metadata bytes currently held across this executor. */
  def reservedMetadataQuotaBytes: Long =
    aggregateQuota.reservedBytes(MemorySpillManager.MetadataMemory)

  /**
   * Counts the edge into an episode of executor-wide allowance exhaustion seen from the consumer
   * side.
   */
  private def enterReceiveQuotaThrottle(): Unit = {
    if (receiveQuotaThrottled.compareAndSet(false, true)) {
      throttleTransitions.incrementAndGet()
      StreamingShuffleMetricsSource.incrementBackpressureEvents(1L)
      logInfo(log"Streaming shuffle throttled every consumer on this executor: the shared " +
        log"aggregate budget of ${MDC(MAX_SIZE, aggregateQuota.totalBytes)} byte(s) is fully " +
        log"committed after ${MDC(COUNT, receiveQuotaRefusals.get())} refusal(s). Blocks refused " +
        log"here are asked for again, so nothing is lost; the reduce side is behind its producers")
    }
  }

  /**
   * Records that a streaming buffer could not be allocated even after spilling '''and that no
   * durable retention was possible either''', which is the second condition under which streaming
   * steps aside: continuing would risk exhausting the heap rather than merely slowing the job down.
   *
   * @param key the stream whose allocation could not be satisfied
   */
  def reportBufferAllocationFailure(key: BackpressureStreamKey): Unit = {
    latchDegradation(BackpressureDegradationReason.BufferAllocationFailure)
    if (debugEnabled) {
      logDebug(log"Streaming shuffle buffer allocation failed for shuffle " +
        log"${MDC(SHUFFLE_ID, key.shuffleId)} partition ${MDC(PARTITION_ID, key.partitionId)}")
    }
  }

  /**
   * Records that a block met the buffer allowance and was retained on local disk instead of in
   * memory, without declaring the subsystem degraded.
   *
   * @param key the stream whose block was retained on disk rather than in memory
   */
  def reportDurableSpillAdmission(key: BackpressureStreamKey): Unit = {
    val occurrence = durableSpillAdmissions.incrementAndGet()
    durableSpillLogGate.admit(clock.getTimeMillis()) match {
      case Some(unreportedAdmissions) =>
        logInfo(log"Streaming shuffle met its buffer allowance for shuffle " +
          log"${MDC(SHUFFLE_ID, key.shuffleId)} and retained a block of partition " +
          log"${MDC(PARTITION_ID, key.partitionId)} on local disk instead of in memory " +
          log"(${MDC(COUNT, occurrence)} such block(s) so far, " +
          log"${MDC(NUM_SKIPPED, unreportedAdmissions)} not reported individually). Streaming " +
          log"continues: spilling is the specified answer to a full buffer, so the map output " +
          log"stays complete and no participant stands down")
      case None =>
        if (debugEnabled) {
          logDebug(log"Streaming shuffle retained a block of shuffle " +
            log"${MDC(SHUFFLE_ID, key.shuffleId)} partition " +
            log"${MDC(PARTITION_ID, key.partitionId)} on local disk " +
            log"(${MDC(COUNT, occurrence)} so far)")
        }
    }
  }

  /**
   * Blocks retained on local disk because the buffer allowance was met, across every stream this
   * protocol serves.
   */
  def durableSpillAdmissionCount: Long = durableSpillAdmissions.get()

  /**
   * How saturated the administered link is, as a percentage of the capacity the operator declared.
   */
  def linkSaturationPercent: Long = {
    if (declaredLinkCapacityBytesPerSecond <= 0L) {
      0L
    } else {
      BackpressureProtocol.percentOf(
        math.max(egressBytesPerSecond, ingressBytesPerSecond),
        declaredLinkCapacityBytesPerSecond)
    }
  }

  /**
   * Whether the link is saturated beyond ninety percent, which is the third condition under which
   * streaming steps aside.
   */
  def isLinkSaturated: Boolean = {
    val saturated = linkSaturationPercent > BackpressureProtocol.LINK_SATURATION_PERCENT
    if (saturated) {
      latchDegradation(BackpressureDegradationReason.LinkSaturation)
    }
    saturated
  }

  /**
   * Checks a wire revision seen on a message against the one this executor speaks, which is the
   * fourth condition under which streaming steps aside.
   *
   * @return true if a message stamped with this revision may be applied
   */
  def observeProtocolVersion(key: BackpressureStreamKey, version: Byte): Boolean = {
    if (StreamingShuffleMessage.isCompatible(version)) {
      true
    } else {
      latchDegradation(BackpressureDegradationReason.ProtocolVersionMismatch)
      // Warned once and then only under the debug gate.
      if (versionMismatchReported.compareAndSet(false, true)) {
        logWarning(log"Streaming shuffle message for shuffle ${MDC(SHUFFLE_ID, key.shuffleId)} " +
          log"partition ${MDC(PARTITION_ID, key.partitionId)} was refused because the peer " +
          log"speaks wire revision ${MDC(PROTOCOL_VERSION, version)} and this executor speaks " +
          log"${MDC(VERSION_NUM, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)}")
      } else if (debugEnabled) {
        logDebug(log"Streaming shuffle message for shuffle ${MDC(SHUFFLE_ID, key.shuffleId)} " +
          log"partition ${MDC(PARTITION_ID, key.partitionId)} was refused at wire revision " +
          log"${MDC(PROTOCOL_VERSION, version)}")
      }
      false
    }
  }

  /** Whether any of the four degradation conditions has been observed. */
  def isDegraded: Boolean = !degradations.isEmpty

  /**
   * Every degradation condition observed so far, in the stable order the reasons are declared in,
   * so a log line or an assertion is never at the mercy of set iteration order.
   */
  def degradationReasons: Seq[BackpressureDegradationReason] =
    BackpressureDegradationReason.values.filter(degradations.contains)

  /** Forgets every degradation condition. */
  def clearDegradation(): Unit = {
    degradations.clear()
    reportedDegradations.clear()
  }

  /**
   * The state of the executor's streaming shuffle as a whole: the most severe state any of its
   * streams is in.
   */
  def state: BackpressureState = {
    if (isDegraded) {
      BackpressureState.Degraded
    } else if (isSpillRequired) {
      BackpressureState.Spilling
    } else if (throttledStreamCount > 0) {
      BackpressureState.Throttled
    } else {
      BackpressureState.Flowing
    }
  }

  /** The state of one stream, with the executor-wide overlays applied. */
  def streamState(key: BackpressureStreamKey): BackpressureState = {
    val ledger = streams.get(key)
    if (isDegraded) {
      BackpressureState.Degraded
    } else if (isSpillRequired) {
      BackpressureState.Spilling
    } else if (ledger != null && ledger.isThrottled) {
      BackpressureState.Throttled
    } else {
      BackpressureState.Flowing
    }
  }

  /** How many streams are currently held back by credit or by pacing. */
  def throttledStreamCount: Int = streams.values().asScala.count(_.isThrottled)

  /** Whether this particular stream is currently held back. */
  def isStreamThrottled(key: BackpressureStreamKey): Boolean = {
    val ledger = streams.get(key)
    ledger != null && ledger.isThrottled
  }

  /** Transitions into the throttled state counted by this protocol instance. */
  def backpressureEventCount: Long = throttleTransitions.get()

  /**
   * Re-evaluates every stream and reports how many changed state, which is what a caller driving
   * the protocol on a timer at the hundred-millisecond cadence calls.
   *
   * @return the number of streams whose effective state differed from the state they were in
   *     before
   */
  def pollOnce(): Int = {
    val nowMillis = clock.getTimeMillis()
    lastPollMillis.set(nowMillis)
    var changed = 0
    val ledgers = streams.values().iterator()
    while (ledgers.hasNext) {
      val ledger = ledgers.next()
      val wasThrottled = ledger.isThrottled
      // Credit may have been restored by an acknowledgement processed on a network thread that had
      // no reason to consult the pacing layer, so the flowing transition is confirmed here.
      if (wasThrottled && !ledger.isTerminated && ledger.hasCredit) {
        leaveThrottled(ledger)
      }
      val sustained = ledger.observeSlowness(
        nowMillis,
        BackpressureProtocol.CONSUMER_SLOWNESS_RATIO,
        BackpressureProtocol.SUSTAINED_SLOWNESS_WINDOW_MS)
      if (sustained) {
        latchDegradation(BackpressureDegradationReason.ConsumerSustainedSlowness)
      }
      if (ledger.isReclamationOverdue(nowMillis, BackpressureProtocol.RECLAMATION_DEADLINE_MS)) {
        reclamationBreaches.incrementAndGet()
        ledger.forgetReclamationDeadline()
      }
      if (wasThrottled != ledger.isThrottled) {
        changed += 1
      }
    }
    changed
  }

  /**
   * Records that one stream has entered a throttled state for a reason only its handler can see.
   *
   * @param key identity of the stream that has stopped making progress
   * @param cause short, non-sensitive description of why, used only in the first report per
   *     stream
   * @return true if this call was the edge, false for a stream already throttled or unknown
   */
  def noteThrottled(key: BackpressureStreamKey, cause: String): Boolean = {
    val ledger = streams.get(key)
    if (ledger == null || ledger.isThrottled) {
      false
    } else {
      enterThrottled(ledger, cause)
      true
    }
  }

  /**
   * Records that one stream has resumed, closing the episode [[noteThrottled]] opened.
   *
   * @param key identity of the stream that has resumed
   * @return true if this call was the edge, false for a stream already flowing or unknown
   */
  def noteResumed(key: BackpressureStreamKey): Boolean = {
    val ledger = streams.get(key)
    if (ledger == null || !ledger.isThrottled) {
      false
    } else {
      leaveThrottled(ledger)
      true
    }
  }

  /**
   * Whether the next poll is due, that is whether at least a hundred milliseconds have elapsed
   * since the last one.
   */
  def isPollDue: Boolean =
    clock.getTimeMillis() - lastPollMillis.get() >= BackpressureProtocol.POLL_INTERVAL_MS

  /** Instant of the most recent poll, as read from the injected clock. */
  def lastPollTimeMillis: Long = lastPollMillis.get()

  /**
   * Drops every ledger, every utilisation contribution, every registration and every latched
   * degradation, and rewinds the local counters, so that this protocol reports what a freshly
   * constructed one would.
   */
  def reset(): Unit = {
    // Each ledger is closed by the step that unlinks it, because clearing the map on its own would
    // discard the only references that can return their metadata reservations to the executor's
    // aggregate allowance -- which is shared process-wide, so the charge would outlive this
    // protocol and be read as another component's utilisation.
    streams.keySet().asScala.toSeq.foreach(closeAndRemove)
    streams.clear()
    shufflePartitionCounts.clear()
    shuffleBufferedBytes.clear()
    shuffleBudgetBytes.clear()
    degradations.clear()
    reportedDegradations.clear()
    versionMismatchReported.set(false)
    throttleTransitions.set(0L)
    reclamationBreaches.set(0L)
    durableSpillAdmissions.set(0L)
    lastPollMillis.set(clock.getTimeMillis())
  }

  override def toString: String = {
    s"BackpressureProtocol(state=$state, streams=$streamCount, " +
      s"shuffles=$numRegisteredShuffles, utilizationPercent=$aggregateBufferUtilizationPercent, " +
      s"spillThresholdPercent=$spillThreshold, backpressureEvents=${throttleTransitions.get()})"
  }

  /** Moves a stream into the throttled state, counting the transition exactly once per episode. */
  private def enterThrottled(
      ledger: BackpressureProtocol.StreamLedger,
      cause: String): Unit = {
    if (ledger.enterThrottled()) {
      throttleTransitions.incrementAndGet()
      StreamingShuffleMetricsSource.incrementBackpressureEvents(1L)
      // Default-level reporting is bounded by a window rather than by stream identity.
      throttleLogGate.admit(clock.getTimeMillis()) match {
        case Some(unreportedEpisodes) =>
          logInfo(log"Streaming shuffle throttled a stream of shuffle " +
            log"${MDC(SHUFFLE_ID, ledger.key.shuffleId)} because ${MDC(REASON, cause)}, holding " +
            log"${MDC(NUM_BYTES, ledger.outstandingBytes)} unacknowledged byte(s) " +
            log"(${MDC(COUNT, throttleTransitions.get())} throttling episode(s) so far, " +
            log"${MDC(NUM_SKIPPED, unreportedEpisodes)} not reported individually)")
        case None =>
      }
      if (debugEnabled && ledger.markThrottleReported()) {
        logDebug(log"Streaming shuffle throttled shuffle " +
          log"${MDC(SHUFFLE_ID, ledger.key.shuffleId)} partition " +
          log"${MDC(PARTITION_ID, ledger.key.partitionId)} because ${MDC(REASON, cause)}")
      }
    }
  }

  /** Moves a stream back into the flowing state. */
  private def leaveThrottled(ledger: BackpressureProtocol.StreamLedger): Unit = {
    if (ledger.leaveThrottled() && debugEnabled) {
      logDebug(log"Streaming shuffle resumed shuffle ${MDC(SHUFFLE_ID, ledger.key.shuffleId)} " +
        log"partition ${MDC(PARTITION_ID, ledger.key.partitionId)} with " +
        log"${MDC(NUM_BYTES, ledger.availableCredit)} byte(s) of credit")
    }
  }

  /** Records a degradation reason and reports it at most once for the lifetime of this protocol. */
  private def latchDegradation(reason: BackpressureDegradationReason): Unit = {
    degradations.add(reason)
    if (reportedDegradations.add(reason)) {
      logInfo(log"Streaming shuffle observed a degradation condition, which is a reason a " +
        log"shuffle yields to sort-based shuffle: ${MDC(REASON, reason.code)}. " +
        log"Aggregate buffer utilisation is " +
        log"${MDC(PERCENT, aggregateBufferUtilizationPercent)}% against a threshold of " +
        log"${MDC(THRESHOLD, spillThreshold)}% across " +
        log"${MDC(COUNT, numRegisteredShuffles)} shuffle(s)")
    }
  }

  /** Whether a producer has been silent for the five-second bound or longer. */
  private def isProducerTimedOut(
      ledger: BackpressureProtocol.StreamLedger,
      nowMillis: Long): Boolean = {
    !ledger.isTerminated &&
      ledger.millisSinceInbound(nowMillis) >= BackpressureProtocol.ACK_TIMEOUT_MS
  }

  /** Whether a consumer has stopped acknowledging for the ten-second bound or longer. */
  private def isConsumerTimedOut(
      ledger: BackpressureProtocol.StreamLedger,
      nowMillis: Long): Boolean = {
    !ledger.isTerminated && ledger.outstandingBytes > 0L &&
      ledger.millisSinceAck(nowMillis) >= BackpressureProtocol.CONSUMER_LIVENESS_TIMEOUT_MS
  }

  /** Reads a byte count out of one of the per-shuffle maps, answering zero for an absent entry. */
  private def readLongEntry(source: ConcurrentHashMap[Int, java.lang.Long], key: Int): Long = {
    val value = source.get(key)
    if (value == null) 0L else value.longValue()
  }
}

/**
 * Constants, pure helpers and the per-stream ledger of the streaming shuffle backpressure protocol.
 */
private[spark] object BackpressureProtocol {

  /** Worker stripes shared by every streaming producer and consumer in one executor JVM. */
  val DATA_PLANE_WORKER_THREADS: Int =
    math.max(2, math.min(8, Runtime.getRuntime.availableProcessors()))

  /** Pending tasks allowed on each worker stripe before the data plane fails closed. */
  val DATA_PLANE_TASKS_PER_STRIPE: Int = 1024

  val DATA_PLANE_SHUTDOWN_TIMEOUT_MS: Long = 10000L

  private val dataPlaneLock = new Object()

  @volatile private var dataPlaneWorkers: DataPlaneWorkers = null

  /** Queue element whose executor-wide admission can be settled before it starts. */
  private trait CancellableDataPlaneTask extends Runnable {
    def cancel(): Unit
  }

  /** Submits one task to the stable stripe selected by its owner. */
  private def executeDataPlane(owner: AnyRef, task: Runnable): Boolean =
    currentDataPlaneWorkers.execute(owner, task)

  /** Waits until the shared worker stripes have no accepted task left to run. */
  private def awaitDataPlaneIdle(timeoutMs: Long): Boolean = {
    val workers = dataPlaneWorkers
    workers == null || workers.awaitIdle(timeoutMs)
  }

  /** Stops and awaits the executor-wide data-plane workers. */
  def shutdownDataPlaneWorkers(): Unit = {
    val workers = dataPlaneLock.synchronized {
      val current = dataPlaneWorkers
      dataPlaneWorkers = null
      current
    }
    if (workers != null) {
      workers.shutdown()
    }
  }

  private def currentDataPlaneWorkers: DataPlaneWorkers = {
    var current = dataPlaneWorkers
    if (current == null || current.isStopped) {
      dataPlaneLock.synchronized {
        current = dataPlaneWorkers
        if (current == null || current.isStopped) {
          current = new DataPlaneWorkers
          dataPlaneWorkers = current
        }
      }
    }
    current
  }

  /** A fixed set of bounded single-thread stripes. */
  private final class DataPlaneWorkers {

    private val stopped = new AtomicBoolean(false)
    private val acceptedTasks = new AtomicInteger(0)
    private val idleLock = new ReentrantLock()
    private val becameIdle = idleLock.newCondition()

    private val stripes: Array[ThreadPoolExecutor] =
      Array.tabulate(DATA_PLANE_WORKER_THREADS) { stripe =>
        new ThreadPoolExecutor(
          1,
          1,
          0L,
          TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue[Runnable](DATA_PLANE_TASKS_PER_STRIPE),
          ThreadUtils.namedThreadFactory(s"streaming-shuffle-data-plane-$stripe"),
          new ThreadPoolExecutor.AbortPolicy())
      }

    def isStopped: Boolean = stopped.get()

    def execute(owner: AnyRef, task: Runnable): Boolean = {
      if (stopped.get()) {
        false
      } else {
        val tracked = new TrackedDataPlaneTask(task)
        acceptedTasks.incrementAndGet()
        val stripe = Math.floorMod(System.identityHashCode(owner), stripes.length)
        try {
          stripes(stripe).execute(tracked)
          true
        } catch {
          case _: RejectedExecutionException =>
            tracked.cancel()
            false
        }
      }
    }

    def awaitIdle(timeoutMs: Long): Boolean = {
      var remainingNanos =
        TimeUnit.MILLISECONDS.toNanos(math.max(0L, timeoutMs))
      idleLock.lock()
      try {
        while (acceptedTasks.get() > 0 && remainingNanos > 0L) {
          try {
            remainingNanos = becameIdle.awaitNanos(remainingNanos)
          } catch {
            case _: InterruptedException =>
              Thread.currentThread().interrupt()
              return false
          }
        }
        acceptedTasks.get() == 0
      } finally {
        idleLock.unlock()
      }
    }

    def shutdown(): Unit = {
      if (stopped.compareAndSet(false, true)) {
        stripes.foreach { stripe =>
          stripe.shutdownNow().asScala.foreach {
            case tracked: CancellableDataPlaneTask => tracked.cancel()
            case _ => ()
          }
        }
      }
      awaitIdle(DATA_PLANE_SHUTDOWN_TIMEOUT_MS)
      stripes.foreach { stripe =>
        try {
          stripe.awaitTermination(DATA_PLANE_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        } catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
        }
      }
    }

    private def taskSettled(): Unit = {
      if (acceptedTasks.decrementAndGet() == 0) {
        idleLock.lock()
        try {
          becameIdle.signalAll()
        } finally {
          idleLock.unlock()
        }
      }
    }

    private final class TrackedDataPlaneTask(delegate: Runnable)
      extends CancellableDataPlaneTask {

      private val settled = new AtomicBoolean(false)

      override def run(): Unit = {
        try {
          delegate.run()
        } finally {
          settle()
        }
      }

      def cancel(): Unit = settle()

      private def settle(): Unit = {
        if (settled.compareAndSet(false, true)) {
          taskSettled()
        }
      }
    }
  }

  /**
   * Producer-liveness bound: a stream that receives nothing at all for this long is treated as
   * having lost its producer.
   */
  val ACK_TIMEOUT_MS: Long = 5000L

  /**
   * How many heartbeat intervals fit inside the liveness bound, and therefore how many consecutive
   * heartbeats may be lost before a healthy peer is declared gone.
   */
  val HEARTBEAT_SAFETY_DIVISOR: Long = 3L

  /**
   * Interval at which a stream emits a heartbeat, strictly below the liveness bound it refreshes.
   */
  val HEARTBEAT_INTERVAL_MS: Long = math.max(1L, ACK_TIMEOUT_MS / HEARTBEAT_SAFETY_DIVISOR)

  /**
   * Consumer-liveness bound: a consumer that has bytes outstanding and acknowledges none of them
   * for this long is treated as gone, which is what makes the writer retain, spill and later replay
   * its unacknowledged window.
   */
  val CONSUMER_LIVENESS_TIMEOUT_MS: Long = 10000L

  /** The bound within which the buffers freed by an acknowledgement must actually be released. */
  val RECLAMATION_DEADLINE_MS: Long = 100L

  val POLL_INTERVAL_MS: Long = 100L

  /**
   * How long a consumer must remain at least [[CONSUMER_SLOWNESS_RATIO]] times slower than its
   * producer, continuously, before streaming is judged unsustainable for the workload.
   */
  val SUSTAINED_SLOWNESS_WINDOW_MS: Long = 60000L

  /** The factor by which a consumer must lag its producer to count as slow. */
  val CONSUMER_SLOWNESS_RATIO: Long = 2L

  val LINK_SATURATION_PERCENT: Long = 90L

  val PERCENT_SCALE: Long = 100L

  val STREAM_LEDGER_BASE_BYTES: Long = 512L

  /** Conservative heap charge for one sorted unacknowledged-window entry. */
  val STREAM_LEDGER_ENTRY_BYTES: Long = 64L

  val MAX_LEDGER_ENTRIES_PER_STREAM: Int = 4096

  /** Milliseconds in one second; every rate this protocol reports is per second. */
  val MILLIS_PER_SECOND: Long = 1000L

  /** A monotonically accumulating byte counter that publishes a rate once per stable interval. */
  private[streaming] final class RateWindow(clock: Clock) {

    private val bytesTotal = new AtomicLong(0L)
    private val openedMillis = new AtomicLong(clock.getTimeMillis())
    private val baseBytes = new AtomicLong(0L)
    private val ratePerSecond = new AtomicLong(0L)

    /** Adds observed bytes, publishing a new rate when the interval has run its length. */
    def record(bytes: Long): Unit = {
      if (bytes > 0L) {
        val total = bytesTotal.addAndGet(bytes)
        val nowMillis = clock.getTimeMillis()
        val opened = openedMillis.get()
        val elapsedMillis = nowMillis - opened
        if (elapsedMillis >= SATURATION_SAMPLE_WINDOW_MS &&
            openedMillis.compareAndSet(opened, nowMillis)) {
          val delta = math.max(0L, total - baseBytes.getAndSet(total))
          ratePerSecond.set(saturatingMultiply(delta, MILLIS_PER_SECOND) / elapsedMillis)
        }
      }
    }

    /** The rate published by the last completed interval. */
    def bytesPerSecond: Long = {
      val elapsedMillis = clock.getTimeMillis() - openedMillis.get()
      if (elapsedMillis >= 2L * SATURATION_SAMPLE_WINDOW_MS) 0L else ratePerSecond.get()
    }

    /** Bytes observed since this window was created. */
    def total: Long = bytesTotal.get()
  }

  /** The interval over which link usage is measured before a rate is derived from it. */
  val SATURATION_SAMPLE_WINDOW_MS: Long = 1000L

  /** The pause before the first replay attempt, which each further attempt doubles. */
  val RETRY_BASE_BACKOFF_MS: Long = 1000L

  /** Replay attempts permitted for one stream before a failure must be escalated. */
  val MAX_RETRY_ATTEMPTS: Int = 5

  /** The acknowledged position of a stream whose consumer has consumed nothing. */
  val NOTHING_ACKNOWLEDGED: Long = AckMessage.NOTHING_CONSUMED

  /** Sentinel for a timestamp that has never been recorded. */
  val NO_TIMESTAMP: Long = Long.MinValue

  /** Sentinel for a sequence number that has never been recorded. */
  val NO_SEQUENCE: Long = Long.MinValue

  val NO_BLOCK_TOTAL: Long = -1L

  /** Reason recorded when a stream is throttled because its consumer's credit is exhausted. */
  val THROTTLE_CAUSE_CREDIT: String = "consumer credit is exhausted"

  /** Reason recorded when a stream is throttled because the pacing bucket refused it. */
  val THROTTLE_CAUSE_RATE: String = "the egress rate limit was reached"

  val THROTTLE_CAUSE_MEMORY: String = "the executor streaming memory allowance was reached"

  /**
   * The pause before a given replay attempt: one second doubled once per attempt, so the five
   * permitted attempts span one, two, four, eight and sixteen seconds.
   *
   * @param attempt the attempt about to be made, counted from one
   * @return the pause in milliseconds
   */
  def retryBackoffMillis(attempt: Int): Long = {
    val bounded = math.min(math.max(1, attempt), MAX_RETRY_ATTEMPTS)
    RETRY_BASE_BACKOFF_MS << (bounded - 1)
  }

  /**
   * Adds two byte counts, treating a negative contribution as zero and saturating at
   * `Long.MaxValue` rather than wrapping.
   */
  def saturatingAdd(runningTotal: Long, contribution: Long): Long = {
    val sum = runningTotal + math.max(0L, contribution)
    if (sum < 0L) Long.MaxValue else sum
  }

  /** Multiplies two non-negative values, saturating at `Long.MaxValue` rather than wrapping. */
  def saturatingMultiply(left: Long, right: Long): Long = {
    if (left <= 0L || right <= 0L) {
      0L
    } else if (left > Long.MaxValue / right) {
      Long.MaxValue
    } else {
      left * right
    }
  }

  /**
   * Expresses `part` as a percentage of `whole` without overflowing for any pair of non-negative
   * inputs.
   */
  def percentOf(part: Long, whole: Long): Long = {
    if (part <= 0L || whole <= 0L) {
      0L
    } else if (part <= Long.MaxValue / PERCENT_SCALE) {
      (part * PERCENT_SCALE) / whole
    } else {
      part / math.max(1L, whole / PERCENT_SCALE)
    }
  }

  /**
   * The credit ledger of one streaming shuffle stream: what has been sent, what has been
   * acknowledged, when the peer was last heard from, and whether the stream is currently held back.
   *
   * @param key identity of the stream this ledger belongs to
   * @param creditLimitBytes bytes the producer may hold unacknowledged at once
   * @param openedAtMillis instant the stream opened, from the protocol's clock, and the origin
   *     every rate and every liveness interval is measured from
   */
  private[streaming] class StreamLedger(
      val key: BackpressureStreamKey,
      val creditLimitBytes: Long,
      openedAtMillis: Long,
      aggregateQuota: MemorySpillManager.ExecutorBufferQuota) {

    // Sequence number to encoded size, for every block sent and not yet acknowledged.
    private val unacknowledged = new ConcurrentSkipListMap[java.lang.Long, java.lang.Long]()

    // Sum of the window's sizes, maintained alongside it so that the hot-path credit check is one
    // atomic read rather than a walk of the window.
    private val outstanding = new AtomicLong(0L)

    // Owners of this ledger.
    private val owners = new AtomicInteger(1)

    private val sentTotal = new AtomicLong(0L)

    // Replayed bytes are counted apart from original output.
    private val replayedTotal = new AtomicLong(0L)

    private val acknowledgedTotal = new AtomicLong(0L)

    private val receivedTotal = new AtomicLong(0L)

    private val highestSent = new AtomicLong(NO_SEQUENCE)

    private val highestReceived = new AtomicLong(NO_SEQUENCE)

    private val ackedPosition = new AtomicLong(NOTHING_ACKNOWLEDGED)

    // Every instant below is a reading of the protocol's clock, never a value a peer sent, so no
    // liveness decision can be distorted by clock skew between two hosts.
    private val lastInboundMillis = new AtomicLong(openedAtMillis)

    private val lastAckMillis = new AtomicLong(openedAtMillis)

    private val lastHeartbeatReceivedMillis = new AtomicLong(openedAtMillis)

    private val lastHeartbeatSentMillis = new AtomicLong(openedAtMillis)

    // Local instant at which the peer's last heartbeat arrived. Nothing here is read from a peer:
    // a heartbeat carries no time value, so arrival is the only sound measure across two clocks.
    private val remoteHeartbeatMillis = new AtomicLong(NO_TIMESTAMP)

    // Instant at which the most recent acknowledgement freed buffers, and therefore the origin of
    // the 100 ms reclamation bound.
    private val reclamationOpenedMillis = new AtomicLong(NO_TIMESTAMP)

    private val lastReclamationLatency = new AtomicLong(NO_TIMESTAMP)

    // Instant at which the consumer was first observed to be lagging by the slowness ratio, or the
    // sentinel when it is not lagging now.
    private val slownessSinceMillis = new AtomicLong(NO_TIMESTAMP)

    private val retransmitAttemptCount = new AtomicInteger(0)

    private val throttled = new AtomicBoolean(false)

    private val throttleReported = new AtomicBoolean(false)

    private val terminated = new AtomicBoolean(false)

    private val announcedBlocks = new AtomicLong(NO_BLOCK_TOTAL)

    private val closed = new AtomicBoolean(false)

    /** Records one further owner of this ledger, reporting the resulting owner count. */
    def retain(): Int = owners.incrementAndGet()

    /**
     * Withdraws one owner, reporting whether that was the last one and the ledger may be discarded.
     */
    def releaseOwner(): Boolean = owners.decrementAndGet() <= 0

    /** Releases every metadata reservation this ledger owns. */
    def close(): Unit = {
      if (closed.compareAndSet(false, true)) {
        val iterator = unacknowledged.entrySet().iterator()
        while (iterator.hasNext) {
          val entry = iterator.next()
          if (unacknowledged.remove(entry.getKey, entry.getValue)) {
            outstanding.addAndGet(-entry.getValue.longValue())
            aggregateQuota.release(
              STREAM_LEDGER_ENTRY_BYTES, MemorySpillManager.MetadataMemory)
          }
        }
        aggregateQuota.release(STREAM_LEDGER_BASE_BYTES, MemorySpillManager.MetadataMemory)
      }
    }

    /** Owners currently holding this ledger. */
    def ownerCount: Int = math.max(0, owners.get())

    /** Bytes charged to this stream and not yet released. */
    def outstandingBytes: Long = outstanding.get()

    /** Bytes of original output sent, replays excluded, which is the producer's true volume. */
    def sentBytes: Long = sentTotal.get()

    /** Bytes re-sent to repair a loss or a corruption, counted apart from original output. */
    def replayedBytes: Long = replayedTotal.get()

    /** Bytes acknowledged in total, which is the numerator of the consumer's rate. */
    def acknowledgedBytes: Long = acknowledgedTotal.get()

    /** Bytes received in total, for a ledger held by the consuming side of a stream. */
    def receivedBytes: Long = receivedTotal.get()

    /** Credit still available, never negative. */
    def availableCredit: Long = math.max(0L, creditLimitBytes - outstanding.get())

    /** Whether any credit at all remains. */
    def hasCredit: Boolean = outstanding.get() < creditLimitBytes

    /** Whether a block of this size fits in the credit that remains. */
    def hasCreditFor(bytes: Long): Boolean = {
      val used = outstanding.get()
      bytes <= math.max(0L, creditLimitBytes - used) || used <= 0L
    }

    /** Highest position the consumer has acknowledged, or the nothing-consumed sentinel. */
    def acknowledgedPosition: Long = ackedPosition.get()

    /** Highest sequence number this side has sent, or the sentinel when it has sent nothing. */
    def highestSentSequenceNumber: Long = highestSent.get()

    /** Highest sequence number this side has received, or the sentinel when it has none. */
    def highestReceivedSequenceNumber: Long = highestReceived.get()

    /**
     * The highest position this side has charged to the window, and therefore the highest position
     * an acknowledgement for this stream can possibly name.
     */
    def highestChargedSequenceNumber: Long =
      math.max(highestSent.get(), highestReceived.get())

    /** Blocks still in the window. */
    def unacknowledgedBlockCount: Int = unacknowledged.size()

    /** Whether one block is still in the window and can therefore still be replayed. */
    def containsUnacknowledged(sequenceNumber: Long): Boolean =
      unacknowledged.containsKey(java.lang.Long.valueOf(sequenceNumber))

    /**
     * How many blocks the window still retains within the inclusive range `[first, last]`.
     *
     * @param first inclusive lower bound
     * @param last inclusive upper bound; a value below `first` yields zero
     */
    def retainedCountWithin(first: Long, last: Long): Int = {
      if (last < first) {
        0
      } else {
        unacknowledged.subMap(
          java.lang.Long.valueOf(first), true, java.lang.Long.valueOf(last), true).size()
      }
    }

    /** Inclusive bounds of the window, or `None` when nothing is outstanding. */
    def unacknowledgedWindow: Option[(Long, Long)] = {
      val first = unacknowledged.firstEntry()
      val last = unacknowledged.lastEntry()
      if (first == null || last == null) {
        None
      } else {
        Some((first.getKey.longValue(), last.getKey.longValue()))
      }
    }

    /**
     * Records that a block of original output has been sent, charging it to the window and to the
     * credit.
     */
    def recordSent(sequenceNumber: Long, bytes: Long): Boolean = {
      require(sequenceNumber >= 0L,
        s"The sequence number must be non-negative but was $sequenceNumber.")
      require(bytes >= 0L, s"The block size must be non-negative but was $bytes.")
      val charged = charge(sequenceNumber, bytes)
      if (charged) {
        sentTotal.addAndGet(bytes)
        advance(highestSent, sequenceNumber)
      }
      charged
    }

    /** Records that a retained block has been re-sent to repair a loss or a corruption. */
    def recordReplay(sequenceNumber: Long, bytes: Long): Boolean = {
      require(sequenceNumber >= 0L,
        s"The sequence number must be non-negative but was $sequenceNumber.")
      require(bytes >= 0L, s"The block size must be non-negative but was $bytes.")
      val charged = charge(sequenceNumber, bytes)
      if (charged) {
        replayedTotal.addAndGet(bytes)
        advance(highestSent, sequenceNumber)
      }
      charged
    }

    /**
     * Applies an acknowledgement, releasing every retained block at or below the acknowledged
     * position and reporting the bytes that release freed.
     *
     * @return bytes released, or `None` when the position was impossible and nothing was
     *     applied
     */
    def applyAck(consumerPosition: Long, nowMillis: Long): Option[Long] = {
      if (consumerPosition > highestChargedSequenceNumber) {
        None
      } else {
        recordInbound(nowMillis)
        Some(releaseUpTo(consumerPosition, nowMillis))
      }
    }

    private def releaseUpTo(consumerPosition: Long, nowMillis: Long): Long = {
      if (consumerPosition < 0L) {
        0L
      } else if (!advance(ackedPosition, consumerPosition)) {
        0L
      } else {
        val released = releaseThrough(consumerPosition)
        acknowledgedTotal.addAndGet(released)
        lastAckMillis.set(nowMillis)
        // Progress means the stream is healthy, so the replay budget is restored in full rather
        // than left partly spent for a later, unrelated failure to inherit.
        retransmitAttemptCount.set(0)
        if (released > 0L) {
          reclamationOpenedMillis.set(nowMillis)
        }
        released
      }
    }

    /** Confirms release of the buffers the last acknowledgement freed, reporting its latency. */
    def confirmReclamation(nowMillis: Long): Option[Long] = {
      val opened = reclamationOpenedMillis.getAndSet(NO_TIMESTAMP)
      if (opened == NO_TIMESTAMP) {
        None
      } else {
        val latency = math.max(0L, nowMillis - opened)
        lastReclamationLatency.set(latency)
        Some(latency)
      }
    }

    /** Whether a release is still awaited and the bound has already elapsed. */
    def isReclamationOverdue(nowMillis: Long, deadlineMs: Long): Boolean = {
      val opened = reclamationOpenedMillis.get()
      opened != NO_TIMESTAMP && nowMillis - opened > deadlineMs
    }

    /** Abandons the pending reclamation bound, after a breach has been counted for it. */
    def forgetReclamationDeadline(): Unit = reclamationOpenedMillis.set(NO_TIMESTAMP)

    /** Latency of the last confirmed reclamation, or `None` when none has been confirmed. */
    def lastReclamationLatencyMillis: Option[Long] = {
      val observed = lastReclamationLatency.get()
      if (observed == NO_TIMESTAMP) None else Some(observed)
    }

    /** Records inbound activity of any kind, which is what keeps a producer judged alive. */
    def recordInbound(nowMillis: Long): Unit = advance(lastInboundMillis, nowMillis)

    /**
     * Records a received block: its volume, its position, the activity it evidences -- and the
     * receive credit it consumes.
     */
    def recordReceived(sequenceNumber: Long, bytes: Long, nowMillis: Long): Boolean = {
      require(sequenceNumber >= 0L,
        s"The sequence number must be non-negative but was $sequenceNumber.")
      require(bytes >= 0L, s"The block size must be non-negative but was $bytes.")
      val charged = charge(sequenceNumber, bytes)
      if (charged) {
        if (bytes > 0L) {
          receivedTotal.addAndGet(bytes)
        }
        advance(highestReceived, sequenceNumber)
        recordInbound(nowMillis)
      }
      charged
    }

    /** Records a received heartbeat. */
    def recordHeartbeat(nowMillis: Long, remoteTimestampMs: Long): Unit = {
      advance(lastHeartbeatReceivedMillis, nowMillis)
      if (remoteTimestampMs != NO_TIMESTAMP) {
        remoteHeartbeatMillis.set(remoteTimestampMs)
      }
      recordInbound(nowMillis)
    }

    /** Records that a heartbeat has been emitted, restarting the interval. */
    def recordHeartbeatSent(nowMillis: Long): Unit = advance(lastHeartbeatSentMillis, nowMillis)

    /** Whether the heartbeat interval has elapsed since the last one was emitted. */
    def isHeartbeatDue(nowMillis: Long, intervalMs: Long): Boolean =
      nowMillis - lastHeartbeatSentMillis.get() >= intervalMs

    /** The local instant at which the peer's last heartbeat arrived, if it has sent one. */
    def remoteHeartbeatTimestamp: Option[Long] = {
      val observed = remoteHeartbeatMillis.get()
      if (observed == NO_TIMESTAMP) None else Some(observed)
    }

    /** Instant of the last received heartbeat, from this executor's clock. */
    def lastHeartbeatMillis: Long = lastHeartbeatReceivedMillis.get()

    /** Milliseconds since anything at all arrived on this stream. */
    def millisSinceInbound(nowMillis: Long): Long =
      math.max(0L, nowMillis - lastInboundMillis.get())

    /** Milliseconds since the consumer last advanced its position. */
    def millisSinceAck(nowMillis: Long): Long = math.max(0L, nowMillis - lastAckMillis.get())

    /** Records the orderly end of the stream, which retires both liveness timers. */
    def recordTermination(totalBlocks: Long, nowMillis: Long): Unit = {
      announcedBlocks.set(totalBlocks)
      terminated.set(true)
      recordInbound(nowMillis)
    }

    /** Whether the producer has announced the end of this stream. */
    def isTerminated: Boolean = terminated.get()

    /** Blocks the producer announced, or `None` before it has announced anything. */
    def announcedBlockCount: Option[Long] =
      if (terminated.get()) Some(announcedBlocks.get()) else None

    /** Counts a replay attempt and reports which attempt it is, counted from one. */
    def recordRetransmitAttempt(): Int = retransmitAttemptCount.incrementAndGet()

    /** Replay attempts made since the stream last made progress. */
    def retransmitAttempts: Int = retransmitAttemptCount.get()

    /** Bytes per second of original output this stream's producer has sustained since it opened. */
    def producerRate(nowMillis: Long): Long = ratePerSecond(sentTotal.get(), nowMillis)

    /** Bytes per second this stream's consumer has acknowledged since it opened. */
    def consumerRate(nowMillis: Long): Long = ratePerSecond(acknowledgedTotal.get(), nowMillis)

    /** Whether the consumer is lagging its producer by at least `ratio`. */
    def isConsumerSlow(ratio: Long): Boolean = {
      val sent = sentTotal.get()
      sent > 0L && outstanding.get() > 0L &&
        sent >= saturatingMultiply(acknowledgedTotal.get(), ratio)
    }

    /**
     * Re-observes the instantaneous lag, maintains the latch that records when it began, and
     * reports whether it has now persisted for longer than `windowMs`.
     */
    def observeSlowness(nowMillis: Long, ratio: Long, windowMs: Long): Boolean = {
      if (terminated.get() || !isConsumerSlow(ratio)) {
        slownessSinceMillis.set(NO_TIMESTAMP)
        false
      } else {
        val since = slownessSinceMillis.get()
        if (since == NO_TIMESTAMP) {
          slownessSinceMillis.compareAndSet(NO_TIMESTAMP, nowMillis)
          false
        } else {
          nowMillis - since > windowMs
        }
      }
    }

    /** Instant at which the current episode of lag began, if one is in progress. */
    def slownessSince: Option[Long] = {
      val since = slownessSinceMillis.get()
      if (since == NO_TIMESTAMP) None else Some(since)
    }

    /** Whether this stream is currently held back. */
    def isThrottled: Boolean = throttled.get()

    /** Moves into the throttled state, reporting true only on the edge. */
    def enterThrottled(): Boolean = throttled.compareAndSet(false, true)

    /** Moves out of the throttled state, reporting true only on the edge. */
    def leaveThrottled(): Boolean = throttled.compareAndSet(true, false)

    /** Claims the single log line this stream is allowed for its throttling episodes. */
    def markThrottleReported(): Boolean = throttleReported.compareAndSet(false, true)

    override def toString: String = {
      s"StreamLedger($key, outstanding=${outstanding.get()}, credit=$creditLimitBytes, " +
        s"acked=${ackedPosition.get()}, throttled=${throttled.get()}, " +
        s"terminated=${terminated.get()})"
    }

    /**
     * Charges one block to the window, keeping the outstanding total exactly equal to the sum of
     * the window's sizes.
     */
    private def charge(sequenceNumber: Long, bytes: Long): Boolean = {
      if (closed.get()) {
        return false
      }
      val key = java.lang.Long.valueOf(sequenceNumber)
      val value = java.lang.Long.valueOf(bytes)
      val existing = unacknowledged.get(key)
      if (existing != null) {
        if (unacknowledged.replace(key, existing, value)) {
          outstanding.addAndGet(bytes - existing.longValue())
          true
        } else {
          charge(sequenceNumber, bytes)
        }
      } else if (unacknowledged.size() >= MAX_LEDGER_ENTRIES_PER_STREAM) {
        false
      } else if (!aggregateQuota.tryReserve(
          STREAM_LEDGER_ENTRY_BYTES, MemorySpillManager.MetadataMemory)) {
        false
      } else {
        val raced = unacknowledged.putIfAbsent(key, value)
        if (raced == null) {
          outstanding.addAndGet(bytes)
          if (closed.get() && unacknowledged.remove(key, value)) {
            outstanding.addAndGet(-bytes)
            aggregateQuota.release(
              STREAM_LEDGER_ENTRY_BYTES, MemorySpillManager.MetadataMemory)
            false
          } else {
            true
          }
        } else {
          aggregateQuota.release(
            STREAM_LEDGER_ENTRY_BYTES, MemorySpillManager.MetadataMemory)
          if (unacknowledged.replace(key, raced, value)) {
            outstanding.addAndGet(bytes - raced.longValue())
            true
          } else {
            charge(sequenceNumber, bytes)
          }
        }
      }
    }

    /** Drains every retained block at or below `position`, returning the bytes freed. */
    private def releaseThrough(position: Long): Long = {
      var released = 0L
      var draining = true
      while (draining) {
        val entry = unacknowledged.firstEntry()
        if (entry == null || entry.getKey.longValue() > position) {
          draining = false
        } else if (unacknowledged.remove(entry.getKey, entry.getValue)) {
          val bytes = entry.getValue.longValue()
          released += bytes
          outstanding.addAndGet(-bytes)
          aggregateQuota.release(STREAM_LEDGER_ENTRY_BYTES, MemorySpillManager.MetadataMemory)
        }
      }
      released
    }

    /** Raises a monotonic cell to `candidate`, reporting whether it moved. */
    private def advance(cell: AtomicLong, candidate: Long): Boolean = {
      var current = cell.get()
      while (candidate > current) {
        if (cell.compareAndSet(current, candidate)) {
          return true
        }
        current = cell.get()
      }
      false
    }

    /** Bytes per second, from a total and the interval since the stream opened. */
    private def ratePerSecond(bytes: Long, nowMillis: Long): Long = {
      val elapsedMillis = nowMillis - openedAtMillis
      if (bytes <= 0L || elapsedMillis <= 0L) {
        0L
      } else {
        saturatingMultiply(bytes, MILLIS_PER_SECOND) / elapsedMillis
      }
    }
  }
}
