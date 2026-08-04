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

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.util.control.NonFatal

import _root_.io.netty.channel.DefaultChannelId
import _root_.io.netty.channel.embedded.EmbeddedChannel
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SharedSparkContext, SparkConf, SparkException, SparkFunSuite, TaskContextImpl}
import org.apache.spark.internal.config.{SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, SHUFFLE_STREAMING_SPILL_THRESHOLD}
import org.apache.spark.network.client.{TransportClient, TransportResponseHandler}
import org.apache.spark.network.protocol.OneWayMessage
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, DataBlockMessage, HeartbeatMessage, RetransmitRequestMessage, StreamingShuffleChecksum, StreamingShuffleMessage, StreamingShuffleMessageType, StreamTerminationMessage}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{FetchFailedException, ShuffleWriter}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{Clock, ManualClock}

/**
 * Unit tests for [[StreamingShuffleWriter]], the producer half of the streaming shuffle.
 *
 * The four cases the feature specification names -- buffer allocation with per-partition memory
 * tracking, the spill trigger at the 80% threshold with timing validation, checksum generation, and
 * producer-failure cleanup with resource reclamation -- each have a test of their own below. The
 * remainder covers the service-provider obligations a streaming writer must honour so the
 * unmodified shuffle write path works: a non-empty map status on success, a double-stop guard, a
 * well-formed partition length vector, write metrics reported from the task thread alone, the
 * consumer-failure flow's windows and backoff, intra-subsystem flush ordering, and the
 * memory-pressure escalation a partial allocation grant produces.
 *
 * <b>Determinism.</b> Nothing here sleeps or waits on wall time: every window the writer and its
 * collaborators enforce is measured against an injected [[ManualClock]], so a timing assertion is a
 * statement about a clock reading the test itself advanced. The one exception is the write-time
 * assertion, which needs a monotonic source in order to observe a non-zero elapsed interval and
 * therefore builds its harness on a real clock; it asserts only that the interval is positive,
 * never how large it is.
 *
 * <b>Why the fixture is assembled by hand.</b> [[StreamingShuffleManager]] normally binds an
 * ephemeral transport server and rendezvouses with a driver endpoint before handing a writer back,
 * none of which is needed to exercise the writer and all of which would make every assertion depend
 * on a live cluster. The collaborators are therefore constructed directly, with two seams doing the
 * heavy lifting: [[MemorySpillManager.ExecutorBufferQuota]] takes the executor-memory figure as a
 * function, so the buffer arithmetic is a function of this file's own constants rather than of
 * whatever heap the test JVM was given; and the two driver-facing abstractions the writer depends
 * upon -- [[StreamingShuffleCoordinatorGateway]] and [[StreamingShuffleRouteRegistry]] -- are
 * interfaces, so recording doubles over them make the failure and degradation flow drivable.
 *
 * A partition with no subscribed consumer queues nothing, which is exactly the state a map task
 * runs in under the unmodified scheduler, so a writer driven here retains every block it frames and
 * makes it durable at the stop. That is the path these tests exercise.
 */
class StreamingShuffleWriterSuite
  extends SparkFunSuite
    with SharedSparkContext
    with Matchers
    with PrivateMethodTester
    with StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  private val defaultPartitions = 4

  /**
   * Executor memory the default fixture derives its buffer budget from.
   *
   * Stated here rather than sampled from the JVM so that every expected figure in this suite is
   * arithmetic over a constant. Sixty-four mebibytes at the default twenty percent leaves a budget
   * comfortably larger than the framing reservation four partitions need, which is what keeps the
   * default fixture on the streaming path instead of degrading it for want of memory.
   */
  private val defaultExecutorMemoryBytes = 64L * 1024L * 1024L

  private val defaultMapId = 7L

  private val defaultTaskAttemptId = 21L

  private val capabilityToken = "streaming-shuffle-writer-suite-token"

  private val consumerId = "streaming-shuffle-writer-suite-consumer"

  private val declaredEpoch = 1L

  private val protocolShuffleId = 3


  /**
   * A working sort-based delegate that records what it was handed.
   *
   * The fixture's default delegate throws, so that a test which expected to stream cannot pass by
   * silently degrading. That default makes the *successful* fallback path unreachable, and it is
   * the successful path that owes the cleanup: the streaming attempt has to withdraw everything it
   * published before the delegate takes over. So a test about delegation supplies this instead --
   * a real `ShuffleWriter` that consumes records, answers `stop` with a real `MapStatus`, and
   * records both -- which turns "the writer degraded" from an exception the test catches into a
   * state the test can interrogate.
   */
  private class WorkingSortShuffleDelegate extends ShuffleWriter[Int, Int] {

    private val consumed = new mutable.ArrayBuffer[(Int, Int)]()
    private var stopped: Option[Boolean] = None
    private val lengths = Array.fill(defaultPartitions)(1L)

    override def write(records: Iterator[Product2[Int, Int]]): Unit = synchronized {
      records.foreach(record => consumed += ((record._1, record._2)))
    }

    override def stop(success: Boolean): Option[MapStatus] = synchronized {
      stopped = Some(success)
      if (success) {
        Some(MapStatus(BlockManagerId("sort-delegate", "sort-delegate-host", 7337), lengths,
          defaultTaskAttemptId))
      } else {
        None
      }
    }

    override def getPartitionLengths(): Array[Long] = lengths.clone()

    /** Every record the delegate was handed, in the order the streaming writer passed them on. */
    def consumedRecords: Seq[(Int, Int)] = synchronized(consumed.toSeq)

    /** The success flag the delegate's own stop was called with, or `None` if it never was. */
    def stopOutcome: Option[Boolean] = synchronized(stopped)
  }

  /**
   * One consumer channel attached to the producer under test, as the transport attaches one.
   *
   * This is the seam the producer-side security cases need. A `StreamingShuffleServerHandler` built
   * without a channel can be asserted about only through its own counters; with one attached, the
   * frames it actually emits can be decoded and the frames a hostile consumer would send can be
   * delivered. Every assertion about emitted integrity, about acknowledgement validation and about
   * the session and subscription ceilings is therefore a statement about bytes rather than about
   * bookkeeping.
   *
   * An `EmbeddedChannel` with a channel id of its own, because Netty's embedded channel shares one
   * singleton id across every instance and the handler keys its sessions by channel identity -- two
   * consumers on the shared id would be one session, which is precisely what the fan-out cases must
   * be able to tell apart.
   *
   * @param handler the producer handler this channel is attached to
   * @param consumerId the identity this consumer declares in its heartbeats
   */
  private class ProducerConsumerChannel(
      val handler: StreamingShuffleServerHandler,
      val consumerId: String) {

    val channel: EmbeddedChannel = new EmbeddedChannel(DefaultChannelId.newInstance())

    val client: TransportClient =
      new TransportClient(channel, new TransportResponseHandler(channel))

    /** Announces this consumer on the channel, which is what the transport's callback does. */
    def activate(): Unit = handler.channelActive(client)

    /**
     * Delivers one frame to the producer exactly as the transport would.
     *
     * @param message the frame to deliver
     */
    def deliver(message: StreamingShuffleMessage): Unit = {
      val framed = message.toByteBuffer()
      val bytes = new Array[Byte](framed.remaining())
      framed.duplicate().get(bytes)
      handler.receive(client, ByteBuffer.wrap(bytes))
    }

    /**
     * Delivers arbitrary bytes, bypassing every message constructor.
     *
     * The only way to reach the producer's pre-decode path: a hostile peer does not use our
     * constructors, and the frames it sends are the ones this method sends.
     *
     * @param bytes the raw frame, exactly as it would arrive off the socket
     */
    def deliverRaw(bytes: Array[Byte]): Unit = {
      handler.receive(client, ByteBuffer.wrap(bytes.clone()))
    }

    /**
     * Subscribes this consumer to one partition from a given position, through a real heartbeat.
     *
     * A heartbeat *is* the subscription and the resume handshake in this protocol, so subscribing
     * through one rather than by reaching into the handler is what makes these cases exercise the
     * production path.
     *
     * @param shuffleId the shuffle being read
     * @param mapId the map output being read
     * @param partitionId the reduce partition to subscribe to
     * @param nextPosition the next block position this consumer expects
     */
    def subscribe(shuffleId: Int, mapId: Long, partitionId: Int, nextPosition: Long = 0L): Unit = {
      deliver(new HeartbeatMessage(shuffleId, mapId, partitionId, nextPosition,
        ManualClockEpochMillis, consumerId))
    }

    /** Acknowledges consumption through the given position, through a real ack frame. */
    def acknowledge(shuffleId: Int, mapId: Long, partitionId: Int, position: Long): Unit = {
      deliver(new AckMessage(shuffleId, mapId, partitionId, position + 1L, position))
    }

    /**
     * Every frame the producer has written since this was last called, decoded in write order.
     *
     * A flush is attempted only while the channel is open, because the producer legitimately closes
     * a channel it has refused -- an identity change, a session ceiling -- and flushing a closed
     * embedded channel raises rather than answering. Whatever was written before the close is still
     * readable, and reading it is exactly how a refusal is shown to have served nothing.
     */
    def drainOutbound(): Seq[StreamingShuffleMessage] = {
      if (channel.isOpen) {
        channel.flushOutbound()
      }
      val decoded = mutable.ArrayBuffer.empty[StreamingShuffleMessage]
      var written = channel.readOutbound[AnyRef]()
      while (written != null) {
        written match {
          case oneWay: OneWayMessage =>
            decoded += StreamingShuffleMessage.Decoder.fromByteBuffer(oneWay.body().nioByteBuffer())
          case _ => ()
        }
        written = channel.readOutbound[AnyRef]()
      }
      decoded.toSeq
    }

    /** Every data block the producer has written since the outbound queue was last drained. */
    def drainBlocks(): Seq[DataBlockMessage] = drainOutbound().collect {
      case block: DataBlockMessage => block
    }

    /** Closes the channel under the producer, which is what a consumer going away looks like. */
    def close(): Unit = {
      handler.channelInactive(client)
      // Discards whatever is still queued and releases it, so a test that closes a channel with
      // frames on it does not leave Netty buffers for the leak detector to find.
      channel.finishAndReleaseAll()
    }
  }

  /**
   * Reduce partitions the degradation cases declare.
   *
   * Deliberately narrow. A mid-production stand-down is only observable at a block boundary, and a
   * boundary is reached once one partition has accumulated a whole block's worth of serialized
   * bytes; concentrating the input into two partitions is what makes that happen for a record count
   * a test can afford.
   */
  private val DegradationPartitions = 2

  /**
   * The executor-memory figure the degradation cases carve their budget from.
   *
   * One mebibyte rather than the suite default, for the same reason the partition count is narrow:
   * the framing capacity is a share of the per-partition allowance, so a small allowance means a
   * small block, and a small block means a boundary is crossed early in the record stream.
   */
  private val DegradationExecutorMemoryBytes = 1024L * 1024L

  /**
   * Records the degradation cases feed.
   *
   * Enough that several block boundaries are crossed in both partitions even with shuffle
   * compression on, so the stand-down is certainly observed while records remain unconsumed --
   * which is what makes the case a statement about reconstructing a partly produced output rather
   * than about the pre-flight delegation.
   */
  private val DegradationRecords = 200000

  /**
   * Records the consumer-failure cases stream to a live subscribed consumer.
   *
   * Chosen against the narrow, small-budget shape those cases use, so that the writer cuts several
   * blocks per partition -- a window of one block cannot express a partial replay or a repair range
   * -- and so that the retained window outgrows the buffer allowance and is partly evicted to disk,
   * which is what lets a replay be asserted to cover a spill segment as well as memory.
   */
  private val StreamedRecordsPerConsumerCase = 120000

  /**
   * Records the pre-flight cases feed.
   *
   * Small, and deliberately so: a refusal settled before the first record is settled whatever the
   * input size, and every one of those cases asserts that the delegate received the input in its
   * entirety -- which is a comparison of two sequences, so it wants to be a sequence a failure
   * message can be read against.
   */
  private val PreflightRecords = 5000

  /**
   * Records the direct-to-disk case feeds into a single partition.
   *
   * Sized like the consumer-failure cases and for the same reason: several blocks have to be cut
   * against a small allowance, because a case that cut one block would be a statement about one
   * admission rather than about the route a run of them takes. Shuffle compression is on, so the
   * count has to be generous relative to the block size the budget yields.
   */
  private val DirectDiskRecords = StreamedRecordsPerConsumerCase

  /**
   * A partition count no plausible budget can frame a block for.
   *
   * Paired with the smallest admissible buffer percentage, so that the per-partition allowance is a
   * handful of bytes and the derived block capacity is zero however many task slots the executor
   * running this suite happens to report.
   */
  private val UnframeablePartitions = 512

  /** The smallest buffer share the configuration entry admits. */
  private val MinimumBufferSizePercent = 1

  /** The largest buffer share the configuration entry admits. */
  private val MaximumBufferSizePercent = 50

  /**
   * The executor-memory figure the framing-negotiation cases carve their budget from.
   *
   * Wide on purpose. Halving can only lower a reservation while the halved block size is still
   * above the accumulator floor, so the derived capacity has to be comfortably above twice that
   * floor -- and the derivation divides by the executor's task slot count, which is a property of
   * the machine this suite runs on rather than of the fixture. A quarter of a gibibyte at the
   * maximum share keeps the derived capacity above the floor for any slot count a JVM is likely to
   * report.
   */
  private val NegotiationExecutorMemoryBytes = 256L * 1024L * 1024L

  /**
   * A protocol version this build cannot speak.
   *
   * Compatibility requires exact equality with the current revision, so the next value up is
   * incompatible by construction and stays incompatible when the revision is bumped.
   */
  private val IncompatibleProtocolVersion: Byte = (ProtocolVersion + 1).toByte

  /**
   * Records every driver-facing operation a producer performs, and answers all of them.
   *
   * A test double over a production interface rather than a stand-in for missing code: the writer
   * takes its two driver-facing operations as [[StreamingShuffleCoordinatorGateway]] precisely so
   * that the consumer-failure and graceful-degradation flows can be driven without an `RpcEnv`.
   * Every method answers rather than throws, which is the contract the interface documents, and the
   * fallback verdict is latched exactly as the coordinator latches it, so a second declaration
   * returns the state the first one established.
   */
  private class RecordingCoordinatorGateway(refuseStandDown: Boolean = false)
    extends StreamingShuffleCoordinatorGateway {

    private val declared = new mutable.ArrayBuffer[StreamingShuffleFallbackReason]()
    private val invalidated = new mutable.ArrayBuffer[StreamingShuffleInvalidationReason]()
    private val completed = new mutable.ArrayBuffer[StreamingShuffleProducerGeneration]()
    private val heartbeats = new mutable.ArrayBuffer[StreamingShuffleProducerGeneration]()
    private var latched: StreamingShuffleFallbackState = StreamingShuffleFallbackState()

    override def declareFallback(
        shuffleId: Int,
        reason: StreamingShuffleFallbackReason,
        detail: String): StreamingShuffleFallbackState = synchronized {
      declared += reason
      // A coordinator that cannot be reached, or that declines, is the one case in which a producer
      // may not use the sort-based delegate: a map output written by a second implementation while
      // its siblings stream is output no reduce-side read path can reassemble. Answering with a
      // state that has not fallen back is exactly what that looks like to the producer.
      if (refuseStandDown) {
        StreamingShuffleFallbackState()
      } else {
        if (!latched.fallenBack) {
          latched = StreamingShuffleFallbackState(reason.toString, declaredEpoch)
        }
        latched
      }
    }

    override def fallbackState(shuffleId: Int): StreamingShuffleFallbackState =
      synchronized(latched)

    override def completeProducer(
        shuffleId: Int,
        generation: StreamingShuffleProducerGeneration): Boolean = synchronized {
      completed += generation
      true
    }

    override def heartbeatProducer(
        shuffleId: Int,
        generation: StreamingShuffleProducerGeneration)
      : Option[StreamingShuffleProducerLiveness] = synchronized {
      heartbeats += generation
      Some(StreamingShuffleProducerLiveness(live = true, coordinatorEpoch = declaredEpoch))
    }

    override def invalidateProducer(
        shuffleId: Int,
        generation: StreamingShuffleProducerGeneration,
        reason: StreamingShuffleInvalidationReason,
        detail: String): Long = synchronized {
      invalidated += reason
      declaredEpoch
    }

    def declaredFallbacks: Seq[StreamingShuffleFallbackReason] = synchronized(declared.toSeq)

    def invalidations: Seq[StreamingShuffleInvalidationReason] = synchronized(invalidated.toSeq)

    def completions: Seq[StreamingShuffleProducerGeneration] = synchronized(completed.toSeq)

    def heartbeatedGenerations: Seq[StreamingShuffleProducerGeneration] =
      synchronized(heartbeats.toSeq)
  }

  /**
   * Records the routing a producer installs and withdraws on its own executor.
   *
   * The registry exists so that the handler can retire one generation's routing entry without
   * holding the table that owns it. Recording the calls is what lets a cleanup assertion prove the
   * retirement happened rather than infer it -- and holding the LIVE set as well as the withdrawal
   * log is what lets an assertion say the executor's routing returned to the state it started in,
   * which a withdrawal log on its own cannot: a generation withdrawn twice and installed three
   * times has a withdrawal recorded and a route still live.
   *
   * The live set mirrors the executor listener's own, whose count is `producerCount`.
   */
  private class RecordingRouteRegistry(
      private val listener: Option[StreamingShuffleListener] = None)
    extends StreamingShuffleRouteRegistry {

    private val withdrawn = new mutable.ArrayBuffer[(Int, Long)]()

    private val routed = new mutable.HashMap[(Int, Long), StreamingShuffleServerHandler]()

    private val live = new mutable.LinkedHashSet[(Int, Long)]()

    /**
     * Installs one generation's routing entry, as the manager installs it before the writer exists.
     *
     * Not an override: the install side of routing is the executor listener's `register`, which is
     * deliberately not part of the narrow interface a handler is given -- a handler may retire its
     * own route and may not create one.
     *
     * Modelling the table rather than only counting calls is what makes a withdrawal assertion mean
     * something: a `withdrawRoute` that answered `true` unconditionally would report success for a
     * table it had never touched, so the entry has to be present for the removal to be observable
     * as a removal. Mirroring the manager's call here is also what gives [[liveRouteCount]] a
     * baseline to be stated against, and what forwards the registration to a real listener when one
     * was supplied.
     */
    def installRoute(shuffleId: Int, mapId: Long, handler: StreamingShuffleServerHandler): Unit =
      synchronized {
        routed((shuffleId, mapId)) = handler
        live += ((shuffleId, mapId))
        listener.foreach(_.register(shuffleId, mapId, handler))
      }

    override def withdrawRoute(
        shuffleId: Int,
        mapId: Long,
        handler: StreamingShuffleServerHandler): Boolean = synchronized {
      withdrawn += ((shuffleId, mapId))
      // Conditional on this handler still being the entry, exactly as the production table is: a
      // straggling older generation must not be able to unroute the attempt that replaced it.
      routed.get((shuffleId, mapId)) match {
        case Some(installed) if installed eq handler =>
          routed.remove((shuffleId, mapId))
          live -= ((shuffleId, mapId))
          // `forall` on the empty case: with no real listener behind this fixture the withdrawal is
          // recorded and reported successful, which is what the handler's own bookkeeping expects;
          // with one behind it, the answer reported is the production table's own.
          listener.forall(_.withdrawRoute(shuffleId, mapId, handler))
        case _ => false
      }
    }

    /** Shuffle and map pairs whose routing entry was withdrawn, in withdrawal order. */
    def withdrawals: Seq[(Int, Long)] = synchronized(withdrawn.toSeq)

    /** Whether a generation is still routable through this table. */
    def isRouted(shuffleId: Int, mapId: Long): Boolean =
      synchronized(routed.contains((shuffleId, mapId)))

    /** Routing entries still installed, which a clean teardown must leave empty. */
    def liveRoutes: Seq[(Int, Long)] = synchronized(live.toSeq)

    /** How many routing entries are still installed, the figure a baseline is stated against. */
    def liveRouteCount: Int = synchronized(live.size)

    /**
     * How many producers the real routing table is serving, when this fixture has one behind it.
     *
     * Reported separately from [[liveRouteCount]] rather than instead of it, so a test can assert
     * that the production table and the fixture's own mirror agree. They agree only if the
     * withdrawal actually removed the entry, which is the part [[liveRouteCount]] alone cannot
     * prove: the fixture's mirror would return to baseline even if `withdrawRoute` were a no-op.
     */
    def routedProducerCount: Option[Int] = listener.map(_.producerCount)

    /** Whether the real routing table would route a frame naming this generation. */
    def routedServes(shuffleId: Int, mapId: Long): Option[Boolean] =
      listener.map(_.serves(shuffleId, mapId))

    /**
     * Releases the real routing table, if there is one.
     *
     * The listener starts an upkeep sweep on its first registration, so a fixture that registered
     * with one owns stopping it; `releaseAll` is what stops it and is a no-op on a table that is
     * already empty.
     */
    def close(): Unit = listener.foreach(_.releaseAll())
  }

  /**
   * Stands in for the sort-based writer a degraded attempt is handed to.
   *
   * It records what it was asked to do rather than doing it, because what the degradation case has
   * to establish is not that the sort-based writer works -- it is unmodified and has its own
   * suite -- but that the streaming half handed the whole attempt over and retired itself first.
   * `stop` therefore answers `None`: no assertion reads the status, and every one reads the calls.
   */
  private class RecordingStopSortWriter extends ShuffleWriter[Int, Int] {

    private val stopped = new mutable.ArrayBuffer[Boolean]()
    private var written = 0

    override def write(records: Iterator[Product2[Int, Int]]): Unit = synchronized {
      written += records.size
    }

    override def stop(success: Boolean): Option[MapStatus] = synchronized {
      stopped += success
      None
    }

    override def getPartitionLengths(): Array[Long] = Array.emptyLongArray

    /** Records the delegate consumed, which is how "the iterator was handed on intact" is read. */
    def writtenRecords: Int = synchronized(written)

    /** The success flag of every stop this delegate was given, in order. */
    def stopCalls: Seq[Boolean] = synchronized(stopped.toSeq)
  }

  /**
   * One writer under test together with everything it was built from.
   *
   * The collaborators are reachable individually because most of the contracts this suite asserts
   * are joint properties of the writer and one collaborator -- the budget it streams within, the
   * eviction it observes, the priority it registered -- and reading them from the components bundle
   * the writer itself was handed is what makes the assertion about the object under test rather
   * than about a second copy of it.
   */
  private class WriterHarness(
      val writer: StreamingShuffleWriter[Int, Int, Int],
      val writerConf: SparkConf,
      val context: TaskContextImpl,
      val metrics: RecordingStreamingShuffleWriteMetrics,
      val handle: StreamingShuffleHandle[Int, Int, Int],
      val components: StreamingShuffleWriterComponents,
      val gateway: RecordingCoordinatorGateway,
      val routes: RecordingRouteRegistry,
      val quota: MemorySpillManager.ExecutorBufferQuota,
      val clock: Clock,
      val sortDelegate: Option[WorkingSortShuffleDelegate] = None) {

    def spillManager: MemorySpillManager = components.spillManager

    def serverHandler: StreamingShuffleServerHandler = components.serverHandler

    def blockResolver: StreamingShuffleBlockResolver = components.blockResolver

    def backpressure: BackpressureProtocol = components.backpressure

    def rateLimiter: TokenBucketRateLimiter = components.rateLimiter

    def fallbackPolicy: StreamingShuffleFallbackPolicy = components.fallbackPolicy

    def errorNotifier: StreamingShuffleErrorNotifier = components.errorNotifier

    def shuffleId: Int = handle.shuffleId

    def spillFiles(): Seq[File] = spillManager.allSpilledBlocks.map(_.file).distinct

    /**
     * Reserves everything the executor's buffer allowance has left, so that the next admission this
     * writer attempts cannot be satisfied from memory by any amount of eviction.
     *
     * The reservation is taken from the shared allowance directly rather than through this task's
     * spill manager, which is what makes it unevictable: the bytes belong to no partition of this
     * producer, so there is nothing for eviction to select, exactly as another task's framing
     * scratch on the same executor would be. Halved on refusal for the same reason the writer
     * halves its own framing request -- the allowance is shared and moves -- so the reservation
     * converges instead of failing on an arithmetic change.
     *
     * @return the number of bytes reserved, which the caller must release
     */
    def reserveRemainingAllowance(): Long = {
      var request = quota.totalBytes - quota.reservedBytes
      var taken = 0L
      while (request > 0L && taken == 0L) {
        if (quota.tryReserve(request)) {
          taken = request
        } else {
          request /= 2L
        }
      }
      taken
    }

    /**
     * Completes the task the way the executor would, then releases anything a failed test
     * left behind.
     *
     * Marking the context complete is the honest teardown, because that is what fires the
     * task-completion listeners the writer registered, and those listeners are the mechanism by
     * which "no buffer, channel or spill file survives task completion" is met.
     *
     * <b>A failure raised by those listeners fails the test.</b> It is captured rather than thrown
     * immediately only so that the emergency releases below still run -- a fixture that held
     * executor memory would fail whichever test ran next instead of this one -- and it is re-thrown
     * afterwards. Swallowing it would let a broken completion listener pass every cleanup case in
     * this suite, which is precisely the mechanism those cases exist to check.
     *
     * The explicit releases are belt and braces for a test that failed before the writer had
     * registered anything, and for one whose listeners raised; every one of them is idempotent, and
     * a failure from any of them is attached to the completion failure rather than replacing it.
     */
    def close(): Unit = {
      // The primary failure is the one the task completion listeners raised, and it is the one that
      // must reach the test. Swallowing it hid exactly what this fixture exists to observe: a
      // listener that threw, a fatal error, and the memory-leak detection the test JVM raises from
      // `markTaskCompleted` when a task ends holding execution memory. Every one of those presented
      // as a passing test.
      //
      // So the primary is captured and rethrown, the belt-and-braces releases still all run -- each
      // is idempotent and a test that failed early may have left any of them undone -- and anything
      // *they* raise is attached to the primary as suppressed rather than replacing it. `NonFatal`
      // for the secondary releases and `Throwable` for the primary is deliberate: a secondary
      // bookkeeping failure must not displace a primary diagnosis, while a fatal error raised by
      // the task's own completion must never be turned into a passing test.
      var primary: Throwable = null
      try {
        context.markTaskCompleted(None)
      } catch {
        case failure: Throwable => primary = failure
      }
      Seq[() => Unit](
        () => serverHandler.releaseAll(),
        () => spillManager.close(),
        () => blockResolver.stop()
      ).foreach { release =>
        try {
          release()
        } catch {
          case NonFatal(secondary) =>
            if (primary == null) {
              primary = secondary
            } else if (primary ne secondary) {
              primary.addSuppressed(secondary)
            }
        }
      }
      if (primary != null) {
        throw primary
      }
    }
  }

  /**
   * One consumer attached to the producer under test, over an embedded channel.
   *
   * The mirror of the reader suite's producer-side fixture, and it exists for the same reason: the
   * writer's egress path is `Channel.write` followed by `Channel.flush`, so an embedded channel is
   * the one way to observe exactly those writes -- through the protocol's own decoder, so that a
   * frame this fixture reports is a frame a real consumer could read -- without a port, a thread
   * pool or a timing assumption. A mock would let an assertion pass that the real encoder rejects.
   *
   * A consumer becomes visible to a producer by sending a control frame naming the partition it
   * wants, exactly as the production client handler does from its own `channelActive`. There is no
   * separate subscription call, on the wire or in this fixture.
   *
   * @param handler the producer's egress handler this consumer is attached to
   * @param channel the embedded channel the frames travel on
   * @param client the transport client the producer knows this consumer by
   */
  private class ConsumerAttachment(
      val handler: StreamingShuffleServerHandler,
      val channel: EmbeddedChannel,
      val client: TransportClient) {

    /**
     * Subscribes this consumer to one partition, from the position it has consumed through.
     *
     * @param shuffleId the shuffle the producer serves
     * @param mapId the map output the producer serves
     * @param partitionId the reduce partition to subscribe to
     * @param consumedThrough the highest sequence number already consumed, or -1 for none
     */
    def subscribe(
        shuffleId: Int,
        mapId: Long,
        partitionId: Int,
        consumedThrough: Long = AckMessage.NOTHING_CONSUMED): Unit = {
      // A heartbeat states the NEXT position its sender expects, so a consumer that has taken
      // nothing announces zero. Built through the protocol's own constructor so the frame this
      // fixture pushes in is byte-for-byte the one the production client handler sends.
      val frame = new HeartbeatMessage(shuffleId, mapId, partitionId, consumedThrough + 1L,
        ManualClockEpochMillis)
      handler.receive(client, frame.toByteBuffer())
    }

    /**
     * Every frame the producer has written since this was last called, decoded in write order.
     *
     * Reading through the protocol's single decoding entry point rather than by inspecting bytes is
     * what makes an assertion on the result meaningful.
     */
    def drainOutbound(): Seq[StreamingShuffleMessage] = {
      val decoded = mutable.ArrayBuffer.empty[StreamingShuffleMessage]
      var written = channel.readOutbound[AnyRef]()
      while (written != null) {
        written match {
          case oneWay: OneWayMessage =>
            decoded += StreamingShuffleMessage.Decoder.fromByteBuffer(oneWay.body().nioByteBuffer())
          case _ => ()
        }
        written = channel.readOutbound[AnyRef]()
      }
      decoded.toSeq
    }

    /** Closes the channel under the consumer, which is what a dead reduce task looks like. */
    def close(): Unit = {
      if (channel.isOpen) {
        channel.close().syncUninterruptibly()
      }
    }
  }

  /**
   * Attaches one consumer to a producer and announces its channel, as the transport would.
   *
   * @param harness the producer under test
   * @return the attachment, whose channel the caller closes
   */
  private def attachConsumer(harness: WriterHarness): ConsumerAttachment = {
    val channel = new EmbeddedChannel()
    val client = new TransportClient(channel, new TransportResponseHandler(channel))
    harness.serverHandler.channelActive(client)
    new ConsumerAttachment(harness.serverHandler, channel, client)
  }

  /**
   * A consumer channel over an embedded Netty channel, driving a producer handler as the wire
   * would.
   *
   * An `EmbeddedChannel` rather than a mock for the same reason the reader suite uses one: every
   * frame a producer emits goes out through `Channel.writeAndFlush` as a transport message, so an
   * embedded channel is the only way to observe exactly what a consumer would receive, in the order
   * it would receive it, without a socket. A mock would let an assertion pass that the real encoder
   * would reject.
   *
   * The identity is supplied rather than derived from the channel, which is precisely the property
   * the consumer-failure flow depends upon: a reconnection arrives on a new channel and must be
   * recognised as the same consumer, so a second instance of this class carrying the same identity
   * is what a reconnection looks like to a producer.
   *
   * @param handler the producer handler this consumer is reading from
   * @param consumerId the consumer session identity every frame carries
   */
  private class ConsumerChannel(
      handler: StreamingShuffleServerHandler,
      val consumerId: String) {

    val channel: EmbeddedChannel = new EmbeddedChannel()
    val client: TransportClient =
      new TransportClient(channel, new TransportResponseHandler(channel))
    private var outboundSequence = 0L

    handler.channelActive(client)

    /**
     * Subscribes this consumer to one partition, which is what an in-progress block request is.
     *
     * @param partitionId the reduce partition to read
     * @param timestampMs the clock reading the heartbeat is stamped with
     */
    def subscribe(partitionId: Int, timestampMs: Long): Unit = {
      deliver(new HeartbeatMessage(handler.shuffleId, handler.mapId, partitionId,
        nextOutboundSequence(), timestampMs, consumerId))
    }

    /**
     * Acknowledges consumption up to a position, which is what releases producer memory.
     *
     * @param partitionId the reduce partition being acknowledged
     * @param position the highest data-block sequence number consumed
     */
    def acknowledge(partitionId: Int, position: Long): Unit = {
      deliver(new AckMessage(handler.shuffleId, handler.mapId, partitionId,
        nextOutboundSequence(), position))
    }

    /** Hands one frame to the producer handler exactly as the transport would. */
    def deliver(message: StreamingShuffleMessage): Unit = {
      val framed = message.toByteBuffer()
      val bytes = new Array[Byte](framed.remaining())
      framed.duplicate().get(bytes)
      handler.receive(client, ByteBuffer.wrap(bytes))
    }

    /** Every frame this consumer has been sent since the last call, decoded in write order. */
    def drainInbound(): Seq[StreamingShuffleMessage] = {
      val received = new mutable.ArrayBuffer[StreamingShuffleMessage]()
      var next = channel.readOutbound[Object]()
      while (next != null) {
        next match {
          case oneWay: OneWayMessage =>
            received +=
              StreamingShuffleMessage.Decoder.fromByteBuffer(oneWay.body().nioByteBuffer())
          case _ =>
        }
        next = channel.readOutbound[Object]()
      }
      received.toSeq
    }

    /** The data blocks among the frames this consumer has been sent, in write order. */
    def drainInboundBlocks(): Seq[DataBlockMessage] = drainInbound().collect {
      case block: DataBlockMessage => block
    }

    /** Closes the channel under the producer, which is what a consumer crash looks like. */
    def crash(): Unit = {
      channel.close().syncUninterruptibly()
      handler.channelInactive(client)
    }

    /**
     * Closes the channel the way a reduce task that has finished reading does.
     *
     * The same machinery as [[crash]], deliberately: a channel closing under a producer is a
     * channel closing, however it came about. It is named separately because what distinguishes the
     * two at the producer is not the manner of the close but what the session still owed when it
     * happened, and a fixture that hid that behind two different call sequences would be asserting
     * on its own bookkeeping rather than on the producer's classification.
     */
    def disconnect(): Unit = crash()

    private def nextOutboundSequence(): Long = {
      outboundSequence += 1L
      outboundSequence
    }
  }

  /**
   * A sort-based writer that records what it was handed.
   *
   * Installed in place of the fixture's loud default when a case is about to assert on degradation
   * rather than on streaming. It is the delegate's own accounting that matters there -- which
   * records reached it, how many times it was written to, and what status it reports -- so it
   * records exactly those and computes lengths the same way the shuffle's partitioner does.
   *
   * @param numPartitions reduce partitions the shuffle declares
   */
  private class RecordingSortShuffleWriter(numPartitions: Int) extends ShuffleWriter[Int, Int] {

    private val received = new mutable.ArrayBuffer[(Int, Int)]()
    private val writeCalls = new AtomicInteger(0)
    private val lengths = new Array[Long](numPartitions)
    private var stopped = false

    override def write(records: Iterator[Product2[Int, Int]]): Unit = {
      writeCalls.incrementAndGet()
      records.foreach { record =>
        received += ((record._1, record._2))
        lengths(math.floorMod(record._1, numPartitions)) += 1L
      }
    }

    override def stop(success: Boolean): Option[MapStatus] = {
      if (stopped) {
        None
      } else {
        stopped = true
        if (success) {
          Some(MapStatus(BlockManagerId("sort-delegate", "delegate-host", 7077), lengths, 0L))
        } else {
          None
        }
      }
    }

    override def getPartitionLengths(): Array[Long] = lengths

    /** Every record handed to this delegate, in the order it arrived. */
    def recordsWritten: Seq[(Int, Int)] = received.toSeq

    /** How many times anything wrote to this delegate; one write is a complete delegation. */
    def writeCallCount: Int = writeCalls.get()
  }

  /**
   * The parts of a streaming registration a case may need to vary.
   *
   * Bundled rather than passed as two more fixture parameters, because they belong together: both
   * describe the registration this producer was granted rather than the budget it streams within.
   * A case that varies neither gets a current protocol version and a fresh recording gateway, which
   * is what every case that is not about the registration wants.
   *
   * @param protocolVersion the version the shuffle was registered under, which the pre-flight
   *                        compatibility check reads
   * @param gateway the driver-facing gateway the producer declares and withdraws through
   */
  private case class RegistrationFixture(
      protocolVersion: Byte = ProtocolVersion,
      gateway: RecordingCoordinatorGateway = new RecordingCoordinatorGateway)

  /**
   * Assembles one writer and every collaborator it needs.
   *
   * @param numPartitions reduce partitions the shuffle declares, which is the divisor of every
   *                      buffer ceiling and must agree with the dependency's partitioner
   * @param executorMemoryBytes the executor-memory figure the buffer budget is carved from,
   *                            supplied as a constant so every expected figure is arithmetic
   *                            rather than a reading of the test JVM's heap
   * @param bufferSizePercent share of that figure the aggregate buffer allowance occupies
   * @param spillThreshold utilisation at which eviction triggers
   * @param taskAttemptId attempt this writer produces under
   * @param attemptNumber attempt of this task; a non-zero value marks a speculative or retried
   *                      attempt, which is what the flush-ordering case varies
   * @param protocolVersion revision the shuffle was registered under; a value this build cannot
   *                        speak is trip condition 4 and makes the writer degrade at its preflight
   * @param delegate a ready-made sort-based writer this attempt hands itself to if it cannot
   *                 stream, for a case that holds the instance it wants to interrogate
   * @param maxBandwidthMBps egress cap for this executor, absent -- and therefore unlimited -- by
   *                         default. A cap makes the token bucket refuse, which is how a case that
   *                         needs blocks to accumulate in the egress queue arranges it without
   *                         reaching into Netty's write water marks
   * @param routeTable the executor's real routing table, absent by default. Supplying one makes the
   *                   fixture's routing calls reach production code, so a test can assert against
   *                   the table's own producer count instead of against a mirror of it; it is not
   *                   the default because the table starts an upkeep thread on first registration,
   *                   and a case asserting a manual clock's timing wants no second thread in it
   * @param sortWriterFactory the same delegate supplied as a factory, for a case that asserts the
   *                          writer builds its delegate exactly once and only when it degrades
   * @param registration the registration this producer was granted
   * @param recordingSortDelegate a working recording delegate, which is what makes the *successful*
   *                              fallback path -- and the cleanup it owes -- observable rather than
   *                              merely unreachable
   * @param clock time source every collaborator reads, manual by default
   * @return the assembled fixture, whose `close()` the caller owns
   *
   * All three delegate parameters reach the same seam and only one may be supplied at a time; with
   * none, the delegate raises, so a fixture that degraded unexpectedly is a loud failure rather
   * than a silent change of subject.
   */
  // Over the parameter limit, and deliberately: every one of these is an injected figure that a
  // case asserts arithmetic against, so collapsing them into a bag would hide exactly the numbers
  // the assertions are stated on. Every one has a default, so a caller names only what it varies.
  // The same waiver is taken by `ShuffleBlockFetcherIteratorSuite` for its own fixture builder.
  // scalastyle:off argcount
  private def newHarness(
      numPartitions: Int = defaultPartitions,
      executorMemoryBytes: Long = defaultExecutorMemoryBytes,
      bufferSizePercent: Int = DefaultBufferSizePercent,
      spillThreshold: Int = DefaultSpillThresholdPercent,
      taskAttemptId: Long = defaultTaskAttemptId,
      attemptNumber: Int = 0,
      protocolVersion: Byte = ProtocolVersion,
      delegate: Option[ShuffleWriter[Int, Int]] = None,
      maxBandwidthMBps: Option[Int] = None,
      routeTable: Option[StreamingShuffleListener] = None,
      sortWriterFactory: Option[() => ShuffleWriter[Int, Int]] = None,
      registration: RegistrationFixture = RegistrationFixture(),
      recordingSortDelegate: Option[WorkingSortShuffleDelegate] = None,
      clock: Clock = new ManualClock(ManualClockEpochMillis)): WriterHarness = {
    val mapId = defaultMapId
    val writerConf = streamingConfWithOverrides(
      bufferSizePercent = bufferSizePercent,
      spillThreshold = spillThreshold)
    val context = newTaskContext(
      sc.env,
      taskAttemptId = taskAttemptId,
      attemptNumber = attemptNumber,
      numPartitions = numPartitions)
    val metrics = new RecordingStreamingShuffleWriteMetrics
    val dependency = shuffleDependencyFor(sc, writerConf, numPartitions)
    val handle = new StreamingShuffleHandle[Int, Int, Int](
      dependency.shuffleId, dependency, numPartitions, registration.protocolVersion, declaredEpoch,
      capabilityToken)
    // The executor-memory figure is injected rather than sampled, which is the whole reason this
    // suite can assert the contracted quotient exactly. The JVM-wide shared quota is deliberately
    // not used: it is derived once per JVM from the real heap and would make every budget assertion
    // depend on whichever suite happened to construct it first.
    val quota = new MemorySpillManager.ExecutorBufferQuota(
      bufferSizePercent, spillThreshold, () => executorMemoryBytes)
    // autoPoll = false so the 100 ms cadence is driven by this suite's clock through pollOnce()
    // rather than by the executor's shared ticker, which would make a timing assertion a race.
    val spillManager = new MemorySpillManager(
      context.taskMemoryManager, writerConf, clock, Some(quota), autoPoll = false)
    // The one-argument constructor deliberately: the writer registers its own retained output, so a
    // root producer is not needed, and leaving it unset means a lookup answers from the
    // registration alone rather than falling back to a producer supplied at construction.
    val blockResolver = new StreamingShuffleBlockResolver(writerConf)
    val egressBudget = new TokenBucketRateLimiter.ExecutorEgressBudget(
      maxBandwidthMBps = maxBandwidthMBps, clock = clock)
    // No coordinator in this process: the protocol documents and guards that case, falling back to
    // the shuffles registered locally when it asks how many an executor is serving.
    val backpressure = new BackpressureProtocol(writerConf, null, egressBudget, clock)
    val rateLimiter = egressBudget.limiterFor(handle.shuffleId)
    val errorNotifier = new StreamingShuffleErrorNotifier(handle.shuffleId, writerConf)
    val routes = new RecordingRouteRegistry(routeTable)
    val serverHandler = new StreamingShuffleServerHandler(writerConf, handle.shuffleId, mapId,
      taskAttemptId, blockResolver, routes, backpressure, rateLimiter, errorNotifier, clock)
    // Mirrors what the manager does on the way to constructing this writer, and in its order: the
    // routing entry is installed once the handler exists and before the producer is announced, so
    // that a consumer acting on the published address reaches a handler rather than nothing. The
    // fixture has to make this call because the harness stands in for the manager here -- and a
    // registration that never happened cannot be asserted to have been withdrawn.
    routes.installRoute(handle.shuffleId, mapId, serverHandler)
    val fallbackPolicy = new StreamingShuffleFallbackPolicy(writerConf, clock)
    val gateway = registration.gateway
    val components = StreamingShuffleWriterComponents(backpressure, rateLimiter, spillManager,
      blockResolver, serverHandler, fallbackPolicy, errorNotifier, gateway)
    // By default a fixture that degraded would silently stop testing the streaming path, so the
    // delegate is a loud failure rather than a working writer: every test that expects streaming
    // therefore proves it. A test that is *about* delegation supplies a real recording writer
    // instead, which is what makes the successful fallback path -- and the cleanup it owes --
    // observable rather than merely unreachable.
    val suppliedDelegate: Option[() => ShuffleWriter[Int, Int]] = recordingSortDelegate match {
      case Some(recording) => Some(() => recording)
      case None => sortWriterFactory.orElse(delegate.map(ready => () => ready))
    }
    val sortDelegate: () => ShuffleWriter[Int, Int] = suppliedDelegate.getOrElse { () =>
      throw new SparkException("The streaming shuffle writer fixture degraded to the " +
        "sort-based writer, so the streaming path under test was never exercised.")
    }
    val writer = new StreamingShuffleWriter[Int, Int, Int](handle, mapId, context, metrics,
      writerConf, components, sortDelegate, clock)
    new WriterHarness(writer, writerConf, context, metrics, handle, components, gateway, routes,
      quota, clock, recordingSortDelegate)
  }
  // scalastyle:on argcount

  /**
   * Runs a body against a fresh fixture and closes it, whatever the body does.
   *
   * Every test uses this rather than closing by hand, because a fixture that outlives a failing
   * test holds executor memory and spill files, and the memory-leak detection the test JVM runs
   * with turns that into a failure in whichever test happens to run next.
   *
   * ==What happens to a teardown failure==
   *
   * [[WriterHarness.close]] raises rather than swallows, and this decides what a raise means. When
   * the body already failed, its exception is the more precise diagnosis and stays the one
   * reported, with the teardown failure attached as suppressed so nothing is lost. When the body
   * succeeded, a teardown failure IS the finding -- a task-completion listener that throws means
   * the cleanup mechanism this suite exists to verify has broken -- so it is the failure reported.
   * Either way no failure is silently discarded, which is what a harness that logged and moved on
   * would do.
   *
   * @param harness the fixture to use and then close
   * @param body the test body
   * @tparam T the body's result type
   * @return the body's result
   */
  private def withHarness[T](harness: WriterHarness)(body: WriterHarness => T): T = {
    var bodyFailure: Throwable = null
    try {
      val result = body(harness)
      harness.close()
      result
    } catch {
      case failure: Throwable =>
        bodyFailure = failure
        try {
          harness.close()
        } catch {
          case teardown: Throwable => failure.addSuppressed(teardown)
        }
        throw failure
    }
  }

  /**
   * Serves one partition's whole egress queue to one consumer, in as many rounds as it takes.
   *
   * One drain is not enough and cannot be made enough: a drain hands a channel only what pacing and
   * the executor-wide framing budget allow at that instant, so a single round observes an arbitrary
   * prefix of the queue. A test that read one round and compared its size to a producer-side total
   * would fail or pass according to how the budget happened to fall, which is the definition of a
   * flaky assertion. Rounds continue while either the last round produced something or the producer
   * still reports the partition as queued, and stop as soon as neither holds.
   *
   * @param handler the producer whose queue is being served
   * @param consumer the consumer channel to read from
   * @param partitionId the partition whose blocks are collected
   * @param maxRounds a bound so that a producer which never drains fails the caller rather than
   *                  hanging it
   * @return every block of that partition that reached the channel, in arrival order
   */
  private def drainAllBlocks(
      handler: StreamingShuffleServerHandler,
      consumer: ProducerConsumerChannel,
      partitionIds: Seq[Int],
      maxRounds: Int = 256): Map[Int, Seq[DataBlockMessage]] = {
    val collected = mutable.HashMap.empty[Int, mutable.ArrayBuffer[DataBlockMessage]]
    var rounds = 0
    var progressing = true
    while (progressing && rounds < maxRounds) {
      handler.flushPending()
      // Every partition is collected in the one pass, and it has to be: a channel carries one
      // interleaved stream of frames, so reading it while keeping only one partition's blocks
      // discards the others irrecoverably and a later pass for them would find an empty channel.
      val batch = consumer.drainBlocks()
      batch.foreach { block =>
        collected.getOrElseUpdate(block.partitionId(), mutable.ArrayBuffer.empty) += block
      }
      progressing = batch.nonEmpty || partitionIds.exists(handler.pendingBlocksFor(_) > 0L)
      rounds += 1
    }
    assert(rounds < maxRounds,
      s"The egress queues of partitions ${partitionIds.mkString(", ")} did not empty in " +
        s"$maxRounds drain round(s)")
    partitionIds.map(partitionId =>
      partitionId -> collected.get(partitionId).map(_.toSeq).getOrElse(Seq.empty)).toMap
  }

  /**
   * Fills a fixture's buffers, round-robin across its partitions, until utilisation reaches the
   * given percentage of the aggregate allowance.
   *
   * Utilisation is checked before every admission rather than after, so the loop stops as soon as
   * the target is met and never relies on eviction to make room. The bound on the number of
   * admissions is a guard against an arithmetic change turning this into an unbounded loop, and it
   * fails loudly rather than silently returning a half-filled buffer.
   *
   * @param spillManager the store to fill
   * @param nextSequence the next sequence number to admit for each partition, indexed by partition
   * id and advanced in place, so a caller may fill, evict and fill again
   * without
   * breaking the gap-free ascending run each partition requires
   * @param targetPercent utilisation, as a percentage of the aggregate allowance, to reach
   * @param blockBytes payload size of each admitted block
   * @return the number of blocks this call admitted
   */
  private def fillToUtilization(
      spillManager: MemorySpillManager,
      nextSequence: Array[Long],
      targetPercent: Long,
      blockBytes: Int): Int = {
    val admissionLimit = 4096
    var admissions = 0
    var partitionId = 0
    while (spillManager.bufferUtilizationPercent < targetPercent && admissions < admissionLimit) {
      val payload = payloadOfLength(admissions.toLong, blockBytes)
      val admitted = spillManager.bufferBlock(partitionId, nextSequence(partitionId), payload)
      assert(admitted,
        s"Admission of block ${nextSequence(partitionId)} to partition $partitionId was refused " +
          s"at ${spillManager.bufferUtilizationPercent}% utilisation, below the $targetPercent% " +
          s"this fixture is filling to; buffered ${spillManager.bufferedBytes} of " +
          s"${spillManager.totalBudgetBytes} budgeted bytes")
      nextSequence(partitionId) += 1L
      admissions += 1
      partitionId = (partitionId + 1) % nextSequence.length
    }
    assert(admissions < admissionLimit,
      s"Filling to $targetPercent% utilisation took $admissions admissions of $blockBytes bytes " +
        "without reaching the target, which means the budget arithmetic has changed; buffered " +
        s"${spillManager.bufferedBytes} of ${spillManager.totalBudgetBytes} budgeted bytes")
    admissions
  }

  /**
   * Admits one small block to the retained store and offers it for egress at a given priority.
   *
   * Both halves are required and in this order, because the handler refuses to enqueue a block the
   * retained store does not hold: that refusal is what stops a handler shared between two producers
   * from sending bytes nobody admitted.
   *
   * @param harness the fixture whose store and handler are used
   * @param partitionId the reduce partition the block belongs to
   * @param priority the egress priority to queue it at
   * @return the sequence number the block was assigned
   */
  private def admitAndEnqueue(
      harness: WriterHarness,
      partitionId: Int,
      priority: StreamingShuffleServerHandler.EgressPriority): Long = {
    val sequenceNumber = harness.spillManager.lastAcceptedSequence(partitionId) match {
      case MemorySpillManager.UNSET_SEQUENCE => 0L
      case last => last + 1L
    }
    val payload = payloadOfLength(sequenceNumber, 512)
    assert(harness.spillManager.bufferBlock(partitionId, sequenceNumber, payload),
      s"the retained store must accept block $sequenceNumber of partition $partitionId")
    harness.serverHandler.enqueueBlock(partitionId, payload.length, priority)
  }

  test("buffer allocation with partition memory tracking") {
    val partitions = 8
    // Not a clean multiple of a hundred, and deliberately so: a round figure gives the same answer
    // whether the percentage is applied before or after the division by a hundred, so it cannot
    // tell the specified `(executorMemory * bufferPercent) / 100` apart from a truncating
    // `executorMemory / 100 * bufferPercent`. The 137 byte remainder makes the two disagree, and
    // every expectation below is computed by the helper in `BigInt` -- an independent evaluation of
    // the specified expression rather than a transcription of the implementation's ordering.
    val executorMemory = 128L * 1024L * 1024L + 137L
    withHarness(newHarness(numPartitions = partitions, executorMemoryBytes = executorMemory)) {
      harness =>
        val spillManager = harness.spillManager
        spillManager.registerPartitionCount(partitions)

        assert(DefaultBufferSizePercent === 20,
          "The specified default share of executor memory reserved for streaming buffers is 20%")
        val expectedTotal = aggregateBudgetBytes(executorMemory, DefaultBufferSizePercent)
        assert(expectedTotal !== executorMemory / PercentScale * DefaultBufferSizePercent.toLong,
          "The fixture must make the two orderings of the percentage disagree, or the assertions " +
            "below cannot tell a truncating implementation from a correct one")
        assert(spillManager.totalBudgetBytes === expectedTotal,
          s"The aggregate buffer allowance must be $DefaultBufferSizePercent% of the " +
            s"$executorMemory byte executor memory region, i.e. $expectedTotal bytes")
        assert(harness.writer.totalBufferBudgetBytes === expectedTotal,
          "The writer must stream within the same aggregate allowance the spill manager publishes")

        val expectedPerPartition =
          perPartitionBudgetBytes(executorMemory, DefaultBufferSizePercent, partitions)
        assert(spillManager.perPartitionBudgetBytes === expectedPerPartition,
          s"The per-partition allowance must be the contracted quotient, i.e. " +
            s"$expectedPerPartition bytes across $partitions partitions")
        assert(harness.writer.perPartitionBufferBudgetBytes === expectedPerPartition,
          "The writer must divide the aggregate allowance by exactly the declared partition count")
        assert(expectedPerPartition * partitions.toLong <= expectedTotal,
          "The per-partition allowances must not sum to more than the aggregate allowance")

        val blockBytes = 64 * 1024
        val perBlockCharge = blockBytes.toLong + MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES
        assert(spillManager.bufferBlock(0, 0L, payloadOfLength(1L, blockBytes)),
          "The first block of partition 0 must be admissible into an empty allowance")
        assert(spillManager.bufferBlock(1, 0L, payloadOfLength(2L, blockBytes)),
          "The first block of partition 1 must be admissible into an empty allowance")
        assert(spillManager.bufferedBytesFor(0) === perBlockCharge,
          s"Partition 0 must be charged its payload plus the per-block overhead, i.e. " +
            s"$perBlockCharge bytes")
        assert(spillManager.bufferedBytesFor(1) === spillManager.bufferedBytesFor(0),
          "Two partitions holding one identically sized block each must be charged identically")
        assert(spillManager.bufferedBytesFor(2) === 0L,
          "A partition that holds nothing must be charged nothing")
        assert(spillManager.bufferedBytes ===
            spillManager.bufferedBytesFor(0) + spillManager.bufferedBytesFor(1),
          "The aggregate reservation must be the sum of the per-partition reservations")
        assert(spillManager.retainedBlockCount(0) === 1,
          "Partition 0 must retain exactly the one block it was handed")

        val nextSequence = Array.fill(partitions)(0L)
        nextSequence(0) = 1L
        nextSequence(1) = 1L
        fillToUtilization(spillManager, nextSequence, DefaultSpillThresholdPercent.toLong,
          blockBytes)
        assert(spillManager.executorReservedBytes <= expectedTotal,
          s"Aggregate buffer usage of ${spillManager.executorReservedBytes} bytes must never " +
            s"exceed the $expectedTotal byte allowance")
        assert(spillManager.bufferedBytes <= expectedTotal,
          "Buffered bytes must never exceed the aggregate allowance")
        (0 until partitions).foreach { partitionId =>
          assert(spillManager.bufferedBytesFor(partitionId) <= expectedPerPartition,
            s"Partition $partitionId held ${spillManager.bufferedBytesFor(partitionId)} bytes, " +
              s"more than its $expectedPerPartition byte allowance")
        }

        intercept[IllegalArgumentException] {
          spillManager.perPartitionBudgetBytesFor(0)
        }
        intercept[IllegalArgumentException] {
          new StreamingShuffleHandle[Int, Int, Int](harness.shuffleId, harness.handle.dependency, 0,
            ProtocolVersion, declaredEpoch, capabilityToken)
        }
    }
  }

  test("spill trigger at 80% with timing validation") {
    val partitions = 2
    val executorMemory = 2L * 1024L * 1024L
    val bufferPercent = 50
    val clock = newManualClock()
    withHarness(newHarness(numPartitions = partitions, executorMemoryBytes = executorMemory,
        bufferSizePercent = bufferPercent, clock = clock)) { harness =>
      val spillManager = harness.spillManager
      spillManager.registerPartitionCount(partitions)

      assert(DefaultSpillThresholdPercent === 80,
        "The specified default utilisation at which streaming shuffle spills is 80%")
      assert(SpillPollIntervalMillis === 100L,
        "The specified threshold polling cadence is 100 ms")
      assert(ReclamationDeadlineMillis === 100L,
        "The specified buffer reclamation deadline is 100 ms after an acknowledgement")
      val total = spillManager.totalBudgetBytes
      assert(total === aggregateBudgetBytes(executorMemory, bufferPercent),
        s"The aggregate allowance must be $bufferPercent% of $executorMemory bytes")
      assert(spillManager.spillThresholdBytes ===
          exactPercentageOf(total, DefaultSpillThresholdPercent),
        s"The spill trigger must be $DefaultSpillThresholdPercent% of the $total byte allowance")
      assert(spillManager.spillCount === 0L, "Nothing may have spilled before anything is buffered")

      val blockBytes = 64 * 1024
      val nextSequence = Array.fill(partitions)(0L)
      fillToUtilization(spillManager, nextSequence, DefaultSpillThresholdPercent.toLong, blockBytes)
      assert(spillManager.bufferUtilizationPercent >= DefaultSpillThresholdPercent.toLong,
        "The fixture must have driven utilisation to the spill threshold before polling")

      assert(spillManager.pollOnce(),
        "The first threshold poll must be due and must evict at or above the spill threshold")
      assert(spillManager.spillCount === 1L, "Exactly one threshold spill event must be counted")
      assert(spillManager.bufferUtilizationPercent < DefaultSpillThresholdPercent.toLong,
        "Eviction must bring utilisation back below the spill threshold")
      assert(spillManager.allSpilledBlocks.nonEmpty,
        "Eviction must have written at least one block to local disk")
      assert(spillManager.spillSelectionOrder.forall(id => id >= 0 && id < partitions),
        "The eviction order must only name partitions of this shuffle")
      assert(spillManager.lastSpillDurationMs >= 0L,
        "The achieved spill latency must be recorded rather than left unset")

      // The cadence is measured on the injected clock: no work inside 100 ms, work at exactly 100.
      // Refilling can itself evict, because an admission that meets a partition's ceiling reclaims
      // room rather than being refused, so the baseline is read after the refill; what the cadence
      // governs is which POLLS do work, and that is what is asserted.
      fillToUtilization(spillManager, nextSequence, DefaultSpillThresholdPercent.toLong, blockBytes)
      val spillsBeforeCadence = spillManager.spillCount
      assert(!spillManager.pollOnce(),
        "A poll taken without advancing the clock is inside the 100 ms cadence and must do nothing")
      assert(spillManager.spillCount === spillsBeforeCadence,
        "A suppressed poll must not count a spill event")
      clock.advance(SpillPollIntervalMillis - 1L)
      assert(!spillManager.pollOnce(),
        s"A poll at ${SpillPollIntervalMillis - 1L} ms is still inside the cadence and must do " +
          "nothing")
      assert(spillManager.spillCount === spillsBeforeCadence,
        "A poll one millisecond early must not spill")
      clock.advance(1L)
      assert(spillManager.pollOnce(),
        s"A poll at exactly $SpillPollIntervalMillis ms must be due and must evict")
      assert(spillManager.spillCount === spillsBeforeCadence + 1L,
        "The due poll must count exactly one further spill event")

      assert(spillManager.registerConsumer(consumerId),
        "A store that still serves its retained output must admit a consumer")
      val freshSequence = nextSequence(0)
      assert(spillManager.bufferBlock(0, freshSequence, payloadOfLength(99L, blockBytes)),
        "A block admitted after eviction must be resident in memory")
      nextSequence(0) += 1L
      val released = spillManager.acknowledge(consumerId, 0, freshSequence)
      assert(released > 0L,
        s"Acknowledging through sequence $freshSequence must release the memory it held")
      assert(spillManager.lastReclamationDurationMs <= ReclamationDeadlineMillis,
        s"Reclamation took ${spillManager.lastReclamationDurationMs} ms, outside the " +
          s"$ReclamationDeadlineMillis ms bound")
      assert(spillManager.reclamationDeadlineBreaches === 0L,
        "No reclamation may be recorded as having overrun its deadline")

      // The MemoryConsumer pressure callback. A request this consumer triggered itself is declined,
      // which is the guard Spark's own spillable collections use against recursing into an eviction
      // while mid-reservation, and a request from any other consumer is honoured in full.
      val pressureSequence = nextSequence(0)
      assert(spillManager.bufferBlock(0, pressureSequence, payloadOfLength(101L, blockBytes)),
        "A block must be resident before the pressure callback is exercised")
      nextSequence(0) += 1L
      val bufferedBeforePressure = spillManager.bufferedBytes
      assert(bufferedBeforePressure > 0L, "The store must hold memory to be asked to release")
      spillManager.spill()
      assert(spillManager.bufferedBytes === bufferedBeforePressure,
        "A spill request a consumer triggered itself must be declined, because a consumer must " +
          "not recurse into its own eviction while it is mid-reservation")
      val neighbourQuota = new MemorySpillManager.ExecutorBufferQuota(
        bufferPercent, DefaultSpillThresholdPercent, () => executorMemory)
      val neighbour = new MemorySpillManager(harness.context.taskMemoryManager, harness.writerConf,
        clock, Some(neighbourQuota), autoPoll = false)
      try {
        assert(spillManager.spill(Long.MaxValue, neighbour) > 0L,
          "A pressure request from another consumer must release memory to disk")
        assert(spillManager.bufferedBytes < bufferedBeforePressure,
          "The honoured pressure request must have moved bytes out of memory")
      } finally {
        neighbour.close()
      }
      assert(spillManager.spillFailureCount === 0L, "No spill may have failed")
    }
  }

  test("checksum generation") {
    assert(ChecksumAlgorithm === "CRC32C", "The specified block checksum algorithm is CRC32C")
    assert(MaxBlockSizeBytes === 2097152,
      "The specified block payload cap is 2 MB, i.e. 2097152 bytes")
    assert(ProtocolVersion === 1.toByte, "The current streaming protocol version is 1")

    val payload = payloadOfLength(7L, 4096)
    val plain = StreamingShuffleChecksum.compute(payload)
    assert(StreamingShuffleChecksum.verify(payload, plain),
      "A payload must verify against the checksum computed over it")
    assert(!StreamingShuffleChecksum.verify(payload, plain + 1L),
      "A payload must not verify against a checksum that is not its own")
    assert(StreamingShuffleChecksum.compute(payload) === plain,
      "The checksum helper must be repeatable, which it is because it uses a fresh CRC32C per call")

    // The block checksum binds the bytes to the stream identity, so a block delivered against the
    // wrong partition fails verification even with its payload intact.
    val block = dataBlock(protocolShuffleId, defaultMapId, 0, 0L, payload)
    assert(block.verifyChecksum(), "A correctly stamped block must verify")
    assert(block.checksum() ===
        blockChecksum(protocolShuffleId, defaultMapId, 0, 0L, payload),
      "A block's stamp must be the identity-bound CRC32C the producer computes")
    assert(block.checksum() !==
        blockChecksum(protocolShuffleId, defaultMapId, 1, 0L, payload),
      "A block's stamp must cover its partition, so the same bytes on another stream differ")
    assert(block.checksum() !==
        blockChecksum(protocolShuffleId, defaultMapId, 0, 1L, payload),
      "A block's stamp must cover its sequence number")
    assert(!corruptedDataBlock(protocolShuffleId, defaultMapId, 0, 0L, payload)
        .verifyChecksum(),
      "A block whose stamp was corrupted must fail verification")

    val atCap = maximumSizedPayload(11L)
    assert(atCap.length === MaxBlockSizeBytes, "The boundary payload must be exactly at the cap")
    val capped = DataBlockMessage.withComputedChecksum(
      protocolShuffleId, defaultMapId, 0, 0L, atCap)
    assert(capped.payloadLength() === MaxBlockSizeBytes,
      s"A payload of exactly $MaxBlockSizeBytes bytes must be accepted")
    assert(capped.verifyChecksum(), "A maximum sized block must carry a verifiable checksum")
    assert(capped.encodedLength() === dataBlockEncodedLength(MaxBlockSizeBytes),
      "A data block's encoded length must be its payload plus the protocol's framing overhead")
    assert(framedLength(capped.encodedLength()) ===
        capped.encodedLength() + FrameTypePrefixLength,
      "A framed message is always its encoded length plus the one-byte type discriminator")
    val oversized = oversizedPayload(11L)
    assert(oversized.length === MaxBlockSizeBytes + 1,
      "The rejecting payload must be exactly one byte over the cap")
    intercept[IllegalArgumentException] {
      DataBlockMessage.withComputedChecksum(
        protocolShuffleId, defaultMapId, 0, 0L, oversized)
    }

    // Three of the five message types encode to the same number of bytes, so a discriminator must
    // read the framing type byte or the concrete class and never the length.
    val fixedMessages = Seq(
      ack(protocolShuffleId, defaultMapId, 0, 0L, 0L),
      retransmitRequest(protocolShuffleId, defaultMapId, 0, 0L, 0L),
      streamTermination(protocolShuffleId, defaultMapId, 0, 1L))
    assert(fixedMessages.forall(_.encodedLength() == FixedMessageEncodedLength),
      s"Every fixed-size streaming message must encode to $FixedMessageEncodedLength bytes")
    assert(fixedMessages.map(typeOf).distinct.size === fixedMessages.size,
      "Fixed-size messages of equal length must still be distinguished by their type discriminator")

    // Every block the writer frames respects the cap and carries a verifiable identity-bound stamp.
    val partitions = 4
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      harness.writer.write(deterministicRecords(400, seed = 4L, keySpace = 48).iterator)
      assert(harness.writer.blocksStreamed > 0L, "The writer must have framed at least one block")
      assert(harness.writer.blockPayloadCapacityBytes > 0,
        "The writer must have derived a positive framing capacity")
      assert(harness.writer.blockPayloadCapacityBytes <= MaxBlockSizeBytes,
        "The writer must never frame above the protocol's block payload cap")
      var verified = 0
      harness.writer.streamedPartitions.foreach { partitionId =>
        val blocks = harness.writer.nextSequenceNumberFor(partitionId)
        assert(blocks > 0L, s"A streamed partition $partitionId must have framed a block")
        var sequence = 0L
        while (sequence < blocks) {
          val retained = harness.spillManager.retainedPayload(partitionId, sequence)
          assert(retained.isDefined,
            s"Block $sequence of partition $partitionId must still be retained before the stop")
          val bytes = retained.get
          assert(bytes.length <= harness.writer.blockPayloadCapacityBytes,
            s"Block $sequence of partition $partitionId carried ${bytes.length} bytes, above the " +
              s"${harness.writer.blockPayloadCapacityBytes} byte framing capacity")
          val framed = DataBlockMessage.withComputedChecksum(
            harness.shuffleId, defaultMapId, partitionId, sequence, bytes)
          assert(framed.verifyChecksum(),
            s"Block $sequence of partition $partitionId must carry a verifiable CRC32C")
          assert(framed.checksum() ===
              blockChecksum(harness.shuffleId, defaultMapId, partitionId, sequence, bytes),
            s"Block $sequence of partition $partitionId must be stamped through the shared helper")
          verified += 1
          sequence += 1L
        }
      }
      assert(verified > 0, "At least one framed block must have been checksum verified")
    }
  }

  test("every block the producer puts on the wire carries the retained payload and its checksum") {
    // What the wire carries is the only thing a consumer can verify, so it is the only thing worth
    // asserting here. Rebuilding a `DataBlockMessage` from the retained store and then checking
    // that message proves the checksum helper is self-consistent -- which the protocol suite
    // already establishes -- and says nothing at all about the frames the producer emitted: a
    // producer that stamped the wrong partition, skipped a sequence number, framed above the cap or
    // shipped a payload other than the one it retained would pass that check unchanged. So a real
    // consumer channel is attached, the frames the production egress path actually wrote are
    // decoded off it, and every field is asserted against the store the producer served them from.
    val partitions = 2
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      val consumer = new ProducerConsumerChannel(harness.serverHandler, consumerId)
      consumer.activate()
      (0 until partitions).foreach(consumer.subscribe(harness.shuffleId, defaultMapId, _))
      assert(harness.serverHandler.sessionCount === 1,
        "The subscribing consumer must hold exactly one session, keyed by its channel")
      assert(harness.serverHandler.subscriptionCount === partitions,
        s"The consumer must be subscribed to all $partitions partitions")

      harness.writer.write(deterministicRecords(3000, seed = 9L, keySpace = 32).iterator)
      harness.serverHandler.flushPending()
      val frames = consumer.drainOutbound()
      assert(frames.nonEmpty, "The producer must have written frames to the subscribed consumer")

      val blocks = frames.collect { case block: DataBlockMessage => block }
      assert(blocks.nonEmpty, "The producer must have put at least one data block on the wire")
      assert(harness.serverHandler.blocksWrittenToChannel === blocks.size.toLong,
        s"The handler counted ${harness.serverHandler.blocksWrittenToChannel} blocks written but " +
          s"${blocks.size} reached the channel")

      blocks.foreach { block =>
        assert(block.shuffleId() === harness.shuffleId,
          s"A block on the wire carried shuffle ${block.shuffleId()}, not ${harness.shuffleId}")
        assert(block.mapId() === defaultMapId,
          s"A block on the wire carried map ${block.mapId()}, not $defaultMapId")
        assert(block.partitionId() >= 0 && block.partitionId() < partitions,
          s"A block on the wire carried partition ${block.partitionId()}, outside 0 until " +
            s"$partitions")
        assert(block.verifyChecksum(),
          s"Block ${block.sequenceNumber()} of partition ${block.partitionId()} failed its own " +
            "checksum, so the bytes on the wire are not the bytes the checksum was computed over")
        assert(block.payloadLength() <= harness.writer.blockPayloadCapacityBytes,
          s"Block ${block.sequenceNumber()} of partition ${block.partitionId()} carried " +
            s"${block.payloadLength()} bytes, above the writer's framing capacity of " +
            s"${harness.writer.blockPayloadCapacityBytes}")
        assert(block.payloadLength() <= MaxBlockSizeBytes,
          s"Block ${block.sequenceNumber()} of partition ${block.partitionId()} carried " +
            s"${block.payloadLength()} bytes, above the protocol's ${MaxBlockSizeBytes} byte cap")
        assert(framedLength(block.encodedLength()) <=
            StreamingShuffleServerHandler.MAX_FRAMED_BYTES,
          "A framed block must fit the protocol's maximum frame")
        // The identity binding, computed independently over the payload that arrived: a block whose
        // stamp covered different identity fields than the ones it carries fails here.
        assert(block.checksum() === blockChecksum(harness.shuffleId, defaultMapId,
            block.partitionId(), block.sequenceNumber(), block.copyPayload()),
          s"Block ${block.sequenceNumber()} of partition ${block.partitionId()} was not stamped " +
            "with the identity-bound CRC32C the protocol specifies")
        // And the payload is the retained one, byte for byte. This is the assertion that ties the
        // wire to the store: the producer serves a consumer from the retention window, so a frame
        // whose bytes differ from the retained bytes is a frame no retransmission could reproduce.
        val retained = harness.spillManager.retainedPayload(
          block.partitionId(), block.sequenceNumber())
        assert(retained.isDefined,
          s"Block ${block.sequenceNumber()} of partition ${block.partitionId()} was written to " +
            "the wire but is not in the retention window, so it could never be retransmitted")
        assert(java.util.Arrays.equals(retained.get, block.copyPayload()),
          s"Block ${block.sequenceNumber()} of partition ${block.partitionId()} put bytes on the " +
            "wire that differ from the bytes it retained")
      }

      // Sequence numbers are the consumer's ordering contract: contiguous from zero, once each.
      blocks.groupBy(_.partitionId()).foreach { case (partitionId, forPartition) =>
        val sequences = forPartition.map(_.sequenceNumber())
        assert(sequences === sequences.distinct,
          s"Partition $partitionId put a sequence number on the wire more than once: " +
            sequences.mkString(", "))
        assert(sequences === sequences.sorted,
          s"Partition $partitionId wrote its blocks out of order: ${sequences.mkString(", ")}")
        assert(sequences === (0L until sequences.size.toLong).toSeq,
          s"Partition $partitionId's stream is not contiguous from zero: " +
            sequences.mkString(", "))
      }

      // Every declared partition is terminated, and the total each terminator announces is the
      // number of blocks that partition actually committed -- which is what the consumer's
      // completeness check compares its own count against.
      val terminations = frames.collect { case end: StreamTerminationMessage => end }
      assert(terminations.map(_.partitionId()).sorted === (0 until partitions).toSeq,
        s"Every declared partition must be terminated exactly once, but " +
          s"${terminations.map(_.partitionId()).sorted.mkString(", ")} were")
      terminations.foreach { end =>
        assert(end.totalBlocks() ===
            harness.writer.nextSequenceNumberFor(end.partitionId()),
          s"Partition ${end.partitionId()} announced ${end.totalBlocks()} block(s) but committed " +
            harness.writer.nextSequenceNumberFor(end.partitionId()))
        assert(end.totalBlocks() === blocks.count(_.partitionId() == end.partitionId()).toLong,
          s"Partition ${end.partitionId()} announced ${end.totalBlocks()} block(s) but " +
            s"${blocks.count(_.partitionId() == end.partitionId())} reached the wire")
      }

      // The discriminator is read from the frame's type byte, never inferred from its length: an
      // acknowledgement, a heartbeat, a retransmission request and a terminator all encode to the
      // same number of bytes, so a decoder that guessed from length would mis-route three of them.
      assert(frames.forall(frame => typeOf(frame) === messageTypeOf(frame)),
        "Every decoded frame must report the type its discriminator carries")
      assert(terminations.forall(end =>
          typeOf(end) === StreamingShuffleMessageType.STREAM_TERMINATION),
        "A terminator decoded off the wire must be discriminated as a terminator")
      assert(blocks.forall(block => typeOf(block) === StreamingShuffleMessageType.DATA_BLOCK),
        "A data block decoded off the wire must be discriminated as a data block")
      consumer.close()
    }
  }

  test("producer failure cleanup and resource reclamation") {
    val partitions = 4
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      harness.writer.write(deterministicRecords(512, seed = 3L, keySpace = 64).iterator)
      assert(harness.writer.recordsStreamed === 512L, "Every record handed over must be streamed")

      // Make the retained window durable so that the cleanup has files to unlink as well as memory
      // to release. The no-argument MemoryConsumer seam is deliberately not used here: it triggers
      // itself, and a self-triggered request is declined by design.
      val movedBytes = harness.spillManager.spillAllRetained()
      assert(movedBytes > 0L, "Making the retained window durable must move bytes to disk")
      val files = harness.spillFiles()
      assert(files.nonEmpty, "The durability flush must have produced at least one spill file")
      assert(files.forall(_.exists()), "Every spill file must exist before the failure")

      // ==The buffers production owns, and what the failure must do to them==
      //
      // The buffers worth measuring here are the ones the RESOLVER creates: `getBlockData` builds a
      // `ManagedBuffer` over a spill segment, and the party that reads it -- a consumer, or the
      // external shuffle service -- owns it from then on. That is a production-owned lifecycle, so
      // the exact count of what exists before the failure is recorded, and the fact that every one
      // of them stops being obtainable afterwards is what proves the failure released rather than
      // orphaned them. Nothing here wraps a payload in a buffer of the test's own and then retains
      // and releases it, because that would only show that the test can count its own calls.
      val servedBlocks = harness.blockResolver.getBlocksForShuffle(harness.shuffleId, defaultMapId)
      assert(servedBlocks.nonEmpty,
        "The durable window must be reachable through the resolver before the failure")
      assert(servedBlocks.size === servedBlocks.distinct.size,
        s"Each spill file must be named exactly once, yet the resolver reported $servedBlocks")
      val servedBytes = servedBlocks.map { blockId =>
        val buffer = harness.blockResolver.getBlockData(blockId)
        try {
          assert(buffer.size() > 0L,
            s"The resolver's buffer for $blockId must carry bytes, or a consumer reading it " +
              "would conclude the producer had nothing for it")
          val body = buffer.nioByteBuffer()
          assert(body.remaining().toLong === buffer.size(),
            s"The buffer for $blockId must expose exactly the bytes it reports")
          buffer.size()
        } finally {
          // Released by the reader, which is this test standing in for a consumer. Release must not
          // unlink the segment: a later attempt of the same reduce task is entitled to read it
          // again, which is the re-readability Spark's recovery model assumes of map output.
          buffer.release()
        }
      }
      assert(servedBytes.sum > 0L, "The resolver must have served bytes for the durable window")
      assert(harness.blockResolver.getBlockData(servedBlocks.head).size() === servedBytes.head,
        "and a second reader must still be served the same bytes, because releasing a buffer " +
          "cannot destroy map output")
      // The retained window itself is production's, and its exact size is known: one record per
      // block the writer framed.
      val framedBlocks = harness.writer.streamedPartitions
        .map(harness.writer.nextSequenceNumberFor).sum
      assert(framedBlocks === harness.writer.blocksStreamed,
        "Every framed block must be accounted in exactly one partition's numbering")
      assert(harness.spillManager.allSpilledBlocks.size.toLong === framedBlocks,
        s"The durable window must hold exactly the $framedBlocks block(s) the writer framed, yet " +
          s"it holds ${harness.spillManager.allSpilledBlocks.size}")
      // What the retention window is actually holding, read from the production store, so that the
      // release assertions after the failure are about bytes production acquired. Wrapping copies
      // of these payloads in the suite's own recording buffer and then retaining and releasing them
      // here would count the suite's own calls: the writer never sees such an object, so a writer
      // that released nothing at all would leave that count exactly as it stands.
      val retainedBlocks = harness.writer.streamedPartitions.flatMap { partitionId =>
        (0L until harness.writer.nextSequenceNumberFor(partitionId)).map((partitionId, _))
      }
      assert(retainedBlocks.nonEmpty, "The retention window must be holding at least one block")
      assert(retainedBlocks.forall { case (partitionId, sequence) =>
          harness.spillManager.retainedPayload(partitionId, sequence).isDefined
        },
        "Every committed block must be reachable in the retention window before the failure")
      val retainedBytesBefore = retainedBlocks.map { case (partitionId, sequence) =>
        harness.spillManager.retainedPayloadLength(partitionId, sequence).getOrElse(0)
      }.sum
      assert(retainedBytesBefore > 0,
        "The retention window must hold bytes before the failure, or its release proves nothing")

      val status = harness.writer.stop(success = false)
      assert(status.isEmpty, "An unsuccessful stop must report no map status")
      assert(harness.writer.isStopped, "The writer must record that it has been stopped")
      assert(harness.writer.producedMapStatus.isEmpty,
        "A failed attempt must never publish a map status")

      assert(harness.gateway.invalidations.contains(
          StreamingShuffleInvalidationReason.IncompleteStream),
        "A failing producer must withdraw its generation from the driver as an incomplete stream")
      assert(harness.routes.withdrawals.contains((harness.shuffleId, defaultMapId)),
        "A failing producer must withdraw its own executor's routing entry")
      assert(harness.blockResolver.registeredGeneration(harness.shuffleId, defaultMapId).isEmpty,
        "A failing producer's generation must no longer be registered with the resolver")
      assert(harness.blockResolver.producerFor(harness.shuffleId, defaultMapId).isEmpty,
        "A failing producer's retained output must no longer be reachable through the resolver")
      assert(harness.blockResolver.registeredProducerCount === 0,
        "No producer may remain registered once the only generation has been withdrawn")

      assert(harness.spillManager.isClosed, "The buffer store must be closed by the failure path")
      assert(harness.spillManager.bufferedBytes === 0L, "No buffered byte may survive the failure")
      assert(harness.spillManager.scratchBytes === 0L,
        "No framing scratch reservation may survive the failure")
      assert(harness.spillManager.executorReservedBytes === 0L,
        "The executor-wide allowance must have every byte of this task's reservation back")
      assert(harness.spillManager.getUsed() === 0L,
        s"The memory consumer still holds ${harness.spillManager.getUsed()} bytes of task memory")
      assert(files.forall(file => !file.exists()),
        s"Spill files survived the failure: ${files.filter(_.exists()).mkString(", ")}")
      assert(harness.spillManager.spillFileDeletionFailures === 0L,
        "No spill file deletion may have failed")
      assert(harness.blockResolver.getBlocksForShuffle(harness.shuffleId, defaultMapId).isEmpty,
        "and not one of the blocks the resolver served before the failure may still be named")
      assert(harness.spillManager.allSpilledBlocks.isEmpty,
        "nor may any durable record survive, so the exact window that existed is exactly gone")
      harness.writer.streamedPartitions.foreach { partitionId =>
        assert(harness.spillManager.retainedBlockCount(partitionId) === 0,
          s"Partition $partitionId must retain no block of any kind after the failure")
      }
      // Every buffer the resolver had handed out is now unobtainable, which is the release: a
      // resolver that kept serving withdrawn output would be serving bytes no attempt owns.
      servedBlocks.foreach { blockId =>
        val failure = intercept[Exception](harness.blockResolver.getBlockData(blockId))
        assert(failure != null,
          s"Requesting withdrawn block $blockId must be refused rather than answered")
      }

      // Every block the retention window was holding has gone with it, read back through the same
      // production accessor that proved they were there.
      assert(retainedBlocks.forall { case (partitionId, sequence) =>
          harness.spillManager.retainedPayload(partitionId, sequence).isEmpty
        },
        "No block may remain reachable in the retention window after the attempt failed")
      assert(harness.writer.streamedPartitions.forall(harness.spillManager.retainedBlockCount(_)
          == 0),
        "No partition may retain a block after the attempt failed")

      // The producer never manufactures the reader's failure signal.
      assert(harness.errorNotifier.fetchFailure.isEmpty,
        "A producer must not construct a fetch failure, which belongs to the reading side")
    }
  }

  test("every resource production held is released exactly once, whichever way the task ends") {
    // The specification's "zero memory leaks under failure" and "no buffer, channel or spill file
    // survives task completion" are claims about resources *production* acquired, so they can only
    // be proved against production's own ledgers. Wrapping a copy of a retained payload in a
    // recording buffer, retaining it and releasing it establishes only that the test can count its
    // own calls -- the writer never sees that object, and a writer that leaked every byte it held
    // would leave the assertion untouched.
    //
    // So this drives all three ways a task can end -- a successful stop, a failing stop, and a
    // cancellation that never stops the writer at all -- and after each one reads the ledgers the
    // production code keeps: the task's own memory consumer, the executor-wide buffer allowance,
    // the executor-wide framing budget the egress path reserves against, the retention window, the
    // spill files on disk and the handler's sessions. Exactly once is then established by releasing
    // a second time and asserting nothing moved and no failure was counted, because a release that
    // ran twice would either double-count on the shared ledgers or fail deleting an absent file.
    val partitions = 4
    Seq("a successful stop", "a failing stop", "a cancellation with no stop at all").foreach {
      ending =>
      // The framing budget is JVM-wide, so the baseline is established rather than assumed. A
      // sibling suite's leftover reservation would otherwise read as this task's leak.
      StreamingShuffleServerHandler.EgressFramingBudget.resetForTesting()
      val harness = newHarness(numPartitions = partitions)
      try {
        val consumer = new ProducerConsumerChannel(harness.serverHandler, consumerId)
        consumer.activate()
        (0 until partitions).foreach(consumer.subscribe(harness.shuffleId, defaultMapId, _))
        harness.writer.write(deterministicRecords(1500, seed = 10L, keySpace = 40).iterator)
        harness.serverHandler.flushPending()
        // Real frames were written, so the framing budget was really exercised, and the payloads
        // written are really the retained ones. Draining the channel is what lets the write
        // listeners the egress path attached run, which is how the budget is returned.
        val blocks = consumer.drainBlocks()
        assert(blocks.nonEmpty, s"[$ending] the producer must have written blocks to the consumer")

        // Read before anything is made durable, because a spill is itself a release of buffered
        // memory: the precondition being established is that production acquired task execution
        // memory at all, and asserting it after the flush would assert it of the wrong instant.
        assert(harness.spillManager.getUsed() > 0L,
          s"[$ending] the task must hold execution memory once it has streamed, or the release " +
            "assertions below prove nothing")
        assert(harness.spillManager.executorReservedBytes > 0L,
          s"[$ending] the executor-wide allowance must be carrying this task's reservation")
        // Durable retention, so the cleanup has files to unlink as well as memory to free.
        assert(harness.spillManager.spillAllRetained() > 0L,
          s"[$ending] making the retained window durable must move bytes to disk")
        val files = harness.spillFiles()
        assert(files.nonEmpty, s"[$ending] the durability flush must have produced a spill file")
        assert(files.forall(_.exists()), s"[$ending] every spill file must exist before the ending")
        assert(harness.serverHandler.sessionCount === 1,
          s"[$ending] the consumer must hold a session before the ending")

        ending match {
          case "a successful stop" =>
            assert(harness.writer.stop(success = true).isDefined,
              s"[$ending] a successful stop must report a map status")
          case "a failing stop" =>
            assert(harness.writer.stop(success = false).isEmpty,
              s"[$ending] a failing stop must report no map status")
          case _ => // Nothing: a cancelled task never reaches its writer's stop at all.
        }
        // Task completion, which is the executor's own teardown and the only hook the reader and
        // the spill manager have. On the success path this is what releases the retention window;
        // on the cancellation path it is the only thing that releases anything.
        harness.context.markTaskCompleted(None)
        consumer.close()

        assert(harness.spillManager.isClosed,
          s"[$ending] the buffer store must be closed once the task has ended")
        assert(harness.spillManager.bufferedBytes === 0L,
          s"[$ending] no buffered byte may survive task completion")
        assert(harness.spillManager.scratchBytes === 0L,
          s"[$ending] no framing scratch reservation may survive task completion")
        assert(harness.spillManager.executorReservedBytes === 0L,
          s"[$ending] the executor-wide allowance must have every reserved byte back")
        assert(harness.spillManager.getUsed() === 0L,
          s"[$ending] the memory consumer still holds ${harness.spillManager.getUsed()} bytes of " +
            "task execution memory")
        assert(harness.spillManager.spillFileDeletionFailures === 0L,
          s"[$ending] no spill file deletion may have failed")
        // Spill files are the one resource whose release is *transferred* rather than performed at
        // task completion, and only on the successful path: a map output a consumer has not
        // finished reading outlives its producing task, so the block resolver becomes the owner and
        // unlinks the files at its own boundary. Asserting deletion at task completion for every
        // ending would therefore assert a contract the subsystem deliberately does not have -- and
        // asserting nothing would let a genuinely orphaned file pass. Both halves of the transfer
        // are checked instead: the files survive with an owner, and that owner really unlinks them.
        if (ending == "a successful stop") {
          assert(harness.spillManager.spillFilesTransferred,
            s"[$ending] a successful attempt must hand its spill files to the block resolver")
          assert(files.forall(_.exists()),
            s"[$ending] a successful attempt's spill files must survive for its readers")
          assert(harness.blockResolver.registeredGeneration(harness.shuffleId, defaultMapId)
              .contains(defaultTaskAttemptId),
            s"[$ending] the resolver must be the registered owner of the surviving spill files")
          assert(harness.blockResolver.unregisterProducer(harness.shuffleId, defaultMapId,
              defaultTaskAttemptId),
            s"[$ending] retiring the generation must take effect")
          assert(files.forall(file => !file.exists()),
            s"[$ending] the resolver left spill files behind when it retired the generation: " +
              files.filter(_.exists()).mkString(", "))
        } else {
          assert(!harness.spillManager.spillFilesTransferred,
            s"[$ending] an attempt that did not succeed must not hand its spill files on")
          assert(files.forall(file => !file.exists()),
            s"[$ending] spill files survived: ${files.filter(_.exists()).mkString(", ")}")
          // A registration may still name this generation -- retiring it belongs to the failing
          // stop or to the manager, never to task completion, because a successful map output has
          // to stay registered after its task ends. What must be true of an attempt that did not
          // succeed is that nothing can be served through it: the store is closed and every block
          // has gone with it, so a consumer that resolved this producer is refused rather than
          // handed a read it could only ever complete part of.
          assert((0 until partitions).forall(harness.spillManager.retainedBlockCount(_) == 0),
            s"[$ending] no partition may retain a block once the task has ended")
          assert(blocks.forall(block => harness.spillManager
              .retainedPayload(block.partitionId(), block.sequenceNumber()).isEmpty),
            s"[$ending] no block written to the wire may remain servable once the task has ended")
        }
        assert(harness.serverHandler.sessionCount === 0,
          s"[$ending] no consumer session may survive task completion")
        assert(harness.serverHandler.pendingBytes === 0L,
          s"[$ending] no queued egress byte may survive task completion")
        assert(StreamingShuffleServerHandler.EgressFramingBudget.inFlight === 0L,
          s"[$ending] the executor-wide framing budget still holds " +
            s"${StreamingShuffleServerHandler.EgressFramingBudget.inFlight} bytes, so a framing " +
            "copy was reserved and never returned")

        // Exactly once, not merely at least once. A second release of every kind must move nothing:
        // an over-release would show on the shared ledgers, and a second deletion of an unlinked
        // file would be counted as a deletion failure.
        val deletionFailuresBefore = harness.spillManager.spillFileDeletionFailures
        harness.context.markTaskCompleted(None)
        harness.spillManager.close()
        harness.serverHandler.releaseAll()
        assert(harness.spillManager.executorReservedBytes === 0L,
          s"[$ending] a repeated release must not push the executor allowance below zero")
        assert(harness.spillManager.getUsed() === 0L,
          s"[$ending] a repeated release must not over-free the task's execution memory")
        assert(harness.spillManager.spillFileDeletionFailures === deletionFailuresBefore,
          s"[$ending] a repeated release attempted to delete a spill file a second time")
        assert(StreamingShuffleServerHandler.EgressFramingBudget.inFlight === 0L,
          s"[$ending] a repeated release must leave the framing budget at zero")
      } finally {
        harness.close()
        StreamingShuffleServerHandler.EgressFramingBudget.resetForTesting()
      }
    }
  }

  test("a subscribed consumer is served while the map task is still producing") {
    // This is the overlap claim, and it is a claim about the code path rather than about
    // scheduling.
    // A block leaves for a subscribed consumer at the moment it is enqueued -- inside `write`, on
    // the task thread, with the record iterator not yet exhausted -- and not at the stop. There is
    // exactly one egress path: a consumer that attached mid-production and one that attaches after
    // the producing task has gone are served by the same code, and only the arrival time differs.
    //
    // What decides that arrival time is the DAG scheduler's stage submission order, which submits a
    // reduce stage only once its map stage reports availability. That order is an ABSOLUTE
    // preservation zone -- AAP 0.2.1 and 0.2.2 forbid modifying the DAG scheduler or the task
    // lifecycle, and AAP 0.8.2 Tier 1 restates it -- so nothing confined to the `ShuffleManager`
    // boundary can make an ordinary reduce task attach earlier. What this test settles is the half
    // that IS inside the boundary: when a consumer is attached, it is served live, and no part of
    // the producer's egress waits for the map task to end.
    //
    // ==Why the fixture is shaped this way==
    //
    // A block is cut when a partition's framing accumulator reaches the derived framing capacity,
    // and that capacity is a share of the buffer budget -- so a fixture on a large budget frames
    // one enormous block per partition and reaches its boundary only in the trailing flush, which
    // would make this test vacuous. One partition and a small executor-memory figure put the
    // boundary well inside the iteration. The figures are injected rather than sampled, so the
    // arrangement is arithmetic and not a property of the machine: a 4 MiB executor figure at the
    // default 20% gives an 838,860 byte budget, one partition takes all of it, and a quarter of
    // that -- the pipeline-depth share -- is the roughly 209 KiB block this shuffle frames to.
    val partitions = 1
    withHarness(newHarness(
        numPartitions = partitions,
        executorMemoryBytes = 4L * 1024L * 1024L)) { harness =>
      val consumer = attachConsumer(harness)
      try {
        consumer.subscribe(harness.shuffleId, defaultMapId, partitionId = 0)
        assert(harness.serverHandler.subscriptionCount === 1,
          "the consumer's control frame must have opened exactly one subscription, so a delivery " +
            "failure below cannot be mistaken for a subscription that never happened")
        // The subscription alone delivers nothing but its handshake: no block has been produced.
        val onSubscribe = consumer.drainOutbound()
        assert(!onSubscribe.exists(_.isInstanceOf[DataBlockMessage]),
          s"a subscription cannot deliver data that does not exist yet, yet it wrote $onSubscribe")

        // Observed from inside the iterator the writer is consuming, which is the only vantage
        // point from which "while the map task is still producing" is a statement about time rather
        // than about the order of lines in a test. At each record handed over, the closure reads
        // what the producer has framed and takes whatever the consumer's channel now holds.
        val blockCutAtRecord = new AtomicInteger(-1)
        val firstDeliveryAtRecord = new AtomicInteger(-1)
        val blocksDeliveredDuringWrite = new AtomicInteger(0)
        val handedOver = new AtomicInteger(0)
        val observed = deterministicRecords(60000, seed = 17L, keySpace = 40).iterator.map {
          record =>
            if (harness.writer.blocksStreamed > 0L) {
              blockCutAtRecord.compareAndSet(-1, handedOver.get())
            }
            val delivered = consumer.drainOutbound().count(_.isInstanceOf[DataBlockMessage])
            if (delivered > 0) {
              blocksDeliveredDuringWrite.addAndGet(delivered)
              firstDeliveryAtRecord.compareAndSet(-1, handedOver.get())
            }
            handedOver.incrementAndGet()
            record
        }
        harness.writer.write(observed)

        assert(handedOver.get() === 60000,
          "the writer must have consumed the whole iterator, so the observations below cover the " +
            "entire production of this map output")
        assert(blockCutAtRecord.get() > 0,
          "a block must have been framed before the iterator was exhausted, or this fixture is " +
            "measuring the trailing flush rather than production")
        assert(blocksDeliveredDuringWrite.get() > 0,
          s"and at least one of them must have reached the consumer before the last record: a " +
            s"producer that delivered nothing until its stop would be a materialising producer " +
            s"whatever its internals were called, yet the first delivery was at record " +
            s"${firstDeliveryAtRecord.get()} of ${handedOver.get()}")
        assert(firstDeliveryAtRecord.get() > 0 &&
            firstDeliveryAtRecord.get() < handedOver.get(),
          s"delivery began at record ${firstDeliveryAtRecord.get()}, which must be strictly " +
            s"inside the ${handedOver.get()} records this task produced")
        assert(blockCutAtRecord.get() <= firstDeliveryAtRecord.get(),
          "and framing must precede delivery, which is what makes the delivery this task's own " +
            "output rather than something the fixture left behind")

        // The stop is not what made the output reachable for this consumer: it already holds part
        // of it, and the producer accounts those bytes as written to a live channel.
        assert(harness.serverHandler.bytesWrittenToChannel > 0L,
          "the producer must account the delivered bytes as written to a consumer's channel")
        assert(harness.serverHandler.subscriptionCount === 1,
          "over exactly the one subscription this test opened")
        assert(harness.writer.stop(success = true).isDefined,
          "and the successful stop must still produce a status")
      } finally {
        consumer.close()
      }
    }
  }

  test("disk is untouched while a producer streams within its budget") {
    // The claim under test is where materialisation happens, and it has two halves.
    //
    // While a producer streams, a map output that fits its buffer allowance touches disk NOWHERE.
    // Blocks are cut, admitted to the retained store and drained to whichever consumers are
    // subscribed, and eviction happens only when utilisation crosses the configured spill
    // threshold.
    // A writer that wrote through to disk as it went would be a materialising writer with extra
    // steps, and this half is what distinguishes the two.
    //
    // The one write that does happen is at the stop, and only of what no consumer took. That write
    // is not a design preference: buffered blocks are task-managed execution memory, which the
    // executor reclaims when the task ends, so output that must outlive its producer has to live
    // somewhere that outlives a task. Which is why the SECOND half of this test measures the write
    // against what was consumed rather than against the size of the output.
    val partitions = 4
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      val spillManager = harness.spillManager

      harness.writer.write(deterministicRecords(320, seed = 31L, keySpace = 40).iterator)

      // First half. The write is complete, the stop has not run, and nothing is on disk.
      assert(harness.writer.blocksStreamed > 0L, "the fixture must have framed blocks to retain")
      assert(spillManager.bufferedBytes > 0L,
        "and must be holding them, since no consumer acknowledged anything")
      assert(spillManager.bufferUtilizationPercent < DefaultSpillThresholdPercent.toLong,
        s"the fixture must stay below the ${DefaultSpillThresholdPercent}% spill threshold, or " +
          "this test would be measuring eviction rather than the absence of it")
      assert(spillManager.spillCount === 0L,
        "no eviction may have occurred below the threshold")
      assert(spillManager.diskBytesSpilled === 0L,
        "and not one byte of a within-budget map output may have reached disk while streaming")
      assert(spillManager.allSpilledBlocks.isEmpty, "so there is no spilled block at all")
      assert(harness.spillFiles().isEmpty, "and no spill file exists on the local disk")
      assert(spillManager.durableAdmissionCount === 0L,
        "nor may any block have bypassed the buffer and been written straight through")

      // Second half. The stop is the one place the retained window becomes durable, and what it
      // writes is what was retained -- which is everything only because nothing consumed it.
      val retainedChargeBeforeStop = spillManager.bufferedBytes
      val retainedBlocksBeforeStop =
        (0 until partitions).map(spillManager.retainedBlockCount).sum.toLong
      assert(retainedBlocksBeforeStop > 0L, "the retained window must hold blocks to be written")
      assert(harness.writer.stop(success = true).isDefined,
        "the successful stop must produce a status")
      // Measured on the charge, which is the resource the bound is expressed in and the one the
      // budget accounts: exactly what the window held left memory, no more and no less. The bytes
      // that land on the device are the same blocks through a serialization stream, so their count
      // is framing-dependent and only their existence is asserted here.
      assert(spillManager.memoryBytesSpilled === retainedChargeBeforeStop,
        s"the stop must have moved exactly the $retainedChargeBeforeStop retained byte(s) out of " +
          s"memory, yet it moved ${spillManager.memoryBytesSpilled}")
      assert(spillManager.diskBytesSpilled > 0L, "and must have committed them to local disk")
      assert(spillManager.durabilityFlushCount === 1L,
        "as one durability flush rather than as a threshold eviction, which is the distinction " +
          "between making output recoverable and running out of buffer")
      assert(spillManager.spillCount === 0L,
        "so the operator-visible spill count still reports no eviction under pressure")
      assert(spillManager.bufferedBytes === 0L,
        "and must hold nothing afterwards, because task memory cannot outlive the task")
      assert(spillManager.spillFilesTransferred,
        "file ownership must have moved to the executor-scoped resolver, which is what lets the " +
          "output outlive its producer without the task leaking anything")
      assert(harness.blockResolver.getBlocksForShuffle(harness.shuffleId, defaultMapId).nonEmpty,
        "and the resolver must be able to serve the output to a consumer that arrives later")
    }

    // The complement, which is what makes the measurement above a statement about CONSUMPTION
    // rather than about the stop: a retained window a consumer has taken is not written at all.
    // Driven against the retained store directly, because a subscribed consumer needs a live
    // channel and the claim here is about the store's own accounting.
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      val spillManager = harness.spillManager
      spillManager.registerPartitionCount(partitions)
      assert(spillManager.registerConsumer(consumerId),
        "a store that still serves its retained output must admit a consumer")
      val blockBytes = 1024
      val blockCharge = blockBytes.toLong + MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES
      val admitted = 6
      (0 until admitted).foreach { sequenceNumber =>
        assert(spillManager.bufferBlock(0, sequenceNumber.toLong,
            payloadOfLength(sequenceNumber.toLong, blockBytes)),
          s"block $sequenceNumber must be admitted below the threshold")
      }
      assert(spillManager.retainedBlockCount(0) === admitted, "every block must be retained")
      assert(spillManager.diskBytesSpilled === 0L, "and none of them may have touched disk")

      // An acknowledgement through the third block retires that prefix, exactly as a consumer's
      // acknowledgement does on the live path -- and gives its memory straight back rather than
      // writing it anywhere.
      val consumedThrough = 2L
      val consumedBlocks = (consumedThrough + 1L).toInt
      val released = spillManager.acknowledge(consumerId, 0, consumedThrough)
      assert(released === consumedBlocks.toLong * blockCharge,
        s"acknowledging through $consumedThrough must release the $consumedBlocks block(s) it " +
          s"covers, yet it released $released byte(s)")
      val stillRetained = admitted - consumedBlocks
      assert(spillManager.retainedBlockCount(0) === stillRetained,
        s"an acknowledgement through $consumedThrough must leave $stillRetained block(s) retained")
      assert(spillManager.diskBytesSpilled === 0L,
        "a consumed block leaves through memory reclamation, never through a disk write")

      val madeDurable = spillManager.spillAllRetained()
      assert(madeDurable === stillRetained.toLong * blockCharge,
        s"the durability step must move only the $stillRetained unacknowledged block(s) out of " +
          s"memory, yet it moved $madeDurable byte(s)")

      // And this is the claim itself, stated per block rather than in bytes so that no serializer
      // framing enters it: the sequence numbers a consumer acknowledged are absent from disk
      // altogether. Their bytes were released, never written.
      val durableSequences = spillManager.spilledBlocks(0).map(_.sequenceNumber)
      assert(durableSequences === (consumedBlocks until admitted).map(_.toLong),
        s"only the unacknowledged sequence numbers may be on disk, yet the durable set was " +
          s"$durableSequences")
      assert(spillManager.spilledBlocks(0).map(_.payloadLength.toLong).sum ===
          stillRetained.toLong * blockBytes.toLong,
        "and they must carry exactly their own payloads, so nothing a consumer took was written")
    }
  }

  test("a producer handed to the sort-based writer withdraws every owner it published") {
    // What this proves is an ownership hand-off, not the existence of a delegate. The manager
    // publishes four owners of a producer generation *before* the writer exists -- the shuffle's
    // share of the executor's egress allowance, this executor's routing entry, the driver's
    // producer address and the retained-output registration -- and the writer is the only party
    // that learns the attempt is not going to stream. If it assigns the delegate without retiring
    // them, a reduce task resolves a producer address that answers, routes a frame to a handler
    // whose task is writing through the sort-based path instead, and waits out the full producer
    // timeout for output that will never be streamed. Asserting only that the delegate was reached
    // would report that arrangement as a pass, so every owner is published here through the same
    // production calls the manager makes and every one of them is asserted retired afterwards.
    val partitions = 4
    val delegate = new WorkingSortShuffleDelegate
    withHarness(newHarness(numPartitions = partitions,
        recordingSortDelegate = Some(delegate))) { harness =>
      harness.routes.installRoute(harness.shuffleId, defaultMapId, harness.serverHandler)
      harness.backpressure.registerShuffle(harness.shuffleId, partitions)
      assert(harness.blockResolver.registerProducer(harness.shuffleId, defaultMapId,
          defaultTaskAttemptId, harness.spillManager),
        "The retained-output registration must take effect before the attempt is stood down")
      // A subscribed consumer, so the handler holds a session as well as a route. A degraded
      // attempt that left one behind would keep a channel open on output nobody will produce.
      val consumer = new ProducerConsumerChannel(harness.serverHandler, consumerId)
      consumer.activate()
      consumer.subscribe(harness.shuffleId, defaultMapId, 0)
      assert(harness.serverHandler.sessionCount === 1,
        "The subscribing consumer must hold a session before the attempt is stood down")

      // Preconditions, so that the assertions below are about removals rather than about absences
      // that were always true.
      assert(harness.routes.isRouted(harness.shuffleId, defaultMapId),
        "The routing entry must be installed before the attempt is stood down")
      assert(harness.blockResolver.producerFor(harness.shuffleId, defaultMapId).isDefined,
        "The retained output must be reachable before the attempt is stood down")
      assert(harness.backpressure.registeredShuffleIds.contains(harness.shuffleId),
        "The shuffle must hold its egress share before the attempt is stood down")

      // The trip itself, through the production observation rather than a flag: a reservation the
      // executor could only partly grant is trip condition two, memory pressure.
      harness.fallbackPolicy.recordAllocationGrant(requestedBytes = 1 << 20, grantedBytes = 0L)
      assert(harness.fallbackPolicy.hasTripped,
        "A refused allocation must trip the fallback policy before the write begins")

      val records = deterministicRecords(192, seed = 6L, keySpace = 24)
      harness.writer.write(records.iterator)

      // The delegate really wrote the output, and it received all of it: a writer that consumed
      // part of the iterator before delegating would show up here as a short record list.
      assert(delegate.consumedRecords === records,
        "The sort-based delegate must receive the whole record iterator, in order")
      assert(harness.writer.recordsStreamed === 0L,
        "A delegated attempt must not have streamed a record of its own")
      assert(harness.writer.blocksStreamed === 0L,
        "A delegated attempt must not have framed a block of its own")

      // Every published owner retired, and the retirement of each asserted as a removal.
      assert(harness.gateway.invalidations.contains(
          StreamingShuffleInvalidationReason.IncompleteStream),
        "The driver's producer address must be withdrawn before the delegate takes over")
      assert(harness.routes.withdrawals.contains((harness.shuffleId, defaultMapId)),
        "This executor's routing entry must be withdrawn before the delegate takes over")
      assert(!harness.routes.isRouted(harness.shuffleId, defaultMapId),
        "No routing entry may survive for a generation that will never stream")
      assert(harness.blockResolver.producerFor(harness.shuffleId, defaultMapId).isEmpty,
        "No retained output may remain reachable for a generation that will never stream")
      assert(harness.blockResolver.registeredGeneration(harness.shuffleId, defaultMapId).isEmpty,
        "No generation may remain registered with the resolver after delegation")
      assert(harness.serverHandler.sessionCount === 0,
        "No consumer session may survive delegation")
      assert(harness.serverHandler.subscriptionCount === 0,
        "No subscription may survive delegation")
      assert(!harness.backpressure.registeredShuffleIds.contains(harness.shuffleId),
        "The shuffle's share of the executor's egress allowance must be returned on delegation")
      assert(harness.backpressure.streamCount(harness.shuffleId) === 0,
        "No stream ledger may survive delegation")

      // Buffers, framing scratch and spill files, all of which a delegated attempt has no use for.
      assert(harness.spillManager.isClosed,
        "The buffer store must be closed once the attempt has been delegated")
      assert(harness.spillManager.bufferedBytes === 0L,
        "No buffered byte may be held for an attempt that will never stream")
      assert(harness.spillManager.scratchBytes === 0L,
        "No framing scratch may be held for an attempt that will never stream")
      assert(harness.spillManager.executorReservedBytes === 0L,
        "The executor-wide allowance must have every reserved byte back after delegation")
      assert(harness.spillManager.getUsed() === 0L,
        s"The memory consumer still holds ${harness.spillManager.getUsed()} bytes after delegation")
      assert(harness.spillFiles().isEmpty, "No spill file may survive delegation")

      // The stop protocol belongs to the delegate from end to end, and the writer must not run its
      // own alongside: a second withdrawal or a second map status would double-report the attempt.
      val invalidationsBeforeStop = harness.gateway.invalidations.size
      val status = harness.writer.stop(success = true)
      assert(delegate.stopOutcome === Some(true),
        "The delegate's own stop must be the one that runs")
      assert(status.isDefined, "A delegated attempt must still report a map status on success")
      assert(status.get.location === BlockManagerId("sort-delegate", "sort-delegate-host", 7337),
        "The map status a delegated attempt reports must be the delegate's own")
      assert(harness.writer.getPartitionLengths().toSeq === delegate.getPartitionLengths().toSeq,
        "The partition lengths a delegated attempt reports must be the delegate's own")
      assert(harness.gateway.invalidations.size === invalidationsBeforeStop,
        "A delegated attempt must not withdraw its generation a second time on stop")
      assert(harness.gateway.completions.isEmpty,
        "A delegated attempt must never report a streamed generation as complete")
      assert(harness.writer.producedMapStatus.isEmpty,
        "A delegated attempt must not record a streaming map status of its own")
      consumer.close()
    }
  }

  test("a successful stop returns a non-empty map status honouring the non-zero size invariant") {
    val partitions = 4
    // A real routing table, so this case can also settle the other half of the withdrawal
    // asymmetry. A failed or delegated attempt must return the executor's routing to its baseline;
    // a SUCCESSFUL one must not, because a successful map task's output is precisely the output no
    // consumer has read yet and routing is the only way to reach it. Both halves being asserted is
    // what stops a fix for either becoming an unconditional withdrawal.
    val routeTable = new StreamingShuffleListener(
      streamingConfWithOverrides(), new ManualClock(ManualClockEpochMillis))
    withHarness(newHarness(numPartitions = partitions, routeTable = Some(routeTable))) { harness =>
      harness.writer.write(deterministicRecords(256, seed = 5L, keySpace = 32).iterator)
      val status = harness.writer.stop(success = true)
      // ShuffleWriteProcessor dereferences this unconditionally, outside its own isDefined guard,
      // so returning None on success would throw before the task could report anything.
      assert(status.isDefined,
        "A successful stop must return a non-empty map status, because the shared shuffle write " +
          "path dereferences it unconditionally")
      val mapStatus = status.get
      assert(mapStatus.mapId === defaultMapId,
        "The placeholder map status must carry the map id this writer produced")
      assert(mapStatus.location === sc.env.blockManager.shuffleServerId,
        "The placeholder map status must name this executor's shuffle server, which is the " +
          "identity MapOutputTracker matches when a fetch failure asks it to remove the output")
      val lengths = harness.writer.getPartitionLengths()
      assert(lengths.exists(_ > 0L),
        "The fixture must have streamed bytes to at least one partition")
      lengths.indices.foreach { partitionId =>
        if (lengths(partitionId) > 0L) {
          // MapStatus documents this as necessary for correctness, because block fetchers are
          // allowed to skip a partition whose reported size is zero.
          assert(mapStatus.getSizeForBlock(partitionId) > 0L,
            s"Partition $partitionId carried ${lengths(partitionId)} bytes, so its reported size " +
              "must be non-zero or a block fetcher is entitled to skip it")
        } else {
          assert(mapStatus.getSizeForBlock(partitionId) === 0L,
            s"Partition $partitionId carried nothing, so its reported size must be zero")
        }
      }
      assert(harness.writer.producedMapStatus.contains(mapStatus),
        "The writer must retain the status it produced")
      assert(harness.gateway.completions.exists(_.mapId == defaultMapId),
        "A successful producer must report its map output complete to the coordinator")
      assertNoPublishedFailure(harness.errorNotifier, "A successful streaming map task")

      // The asymmetry, stated on the routing table itself.
      assert(harness.routes.withdrawals.isEmpty,
        "A successful attempt must withdraw nothing, yet it withdrew " +
          s"${harness.routes.withdrawals}")
      assert(harness.routes.routedProducerCount === Some(1),
        "and the executor must still be routing its output, because no consumer has read it")
      assert(harness.routes.routedServes(harness.shuffleId, defaultMapId) === Some(true),
        "so a frame naming this generation still reaches the handler holding the output")
      harness.context.markTaskCompleted(None)
      assert(harness.routes.routedProducerCount === Some(1),
        "and task completion must not withdraw it either -- the route outlives the task that " +
          "made it, and is released with the output rather than with the attempt")
      assert(harness.serverHandler.servesRetainedOutput,
        "which is only true while the generation still serves its retained output")
    }
  }

  test("a second stop is a no-op that returns None") {
    withHarness(newHarness()) { harness =>
      harness.writer.write(deterministicRecords(64, seed = 6L, keySpace = 16).iterator)
      assert(harness.writer.stop(success = true).isDefined, "The first stop must succeed")
      val bytesAfterFirst = harness.metrics.bytesWritten
      val recordsAfterFirst = harness.metrics.recordsWritten
      // The shared write path calls stop(success = true) and then, from its catch block,
      // stop(success = false); a repeat of either must not report the same bytes twice or delete
      // anything twice.
      assert(harness.writer.stop(success = true).isEmpty,
        "A repeated successful stop must be a no-op returning None")
      assert(harness.metrics.bytesWritten === bytesAfterFirst,
        "A repeated stop must not republish written bytes")
      assert(harness.metrics.recordsWritten === recordsAfterFirst,
        "A repeated stop must not republish written records")
      // Failing after a successful stop withdraws the output, because a consumer must not read the
      // output of an attempt that did not succeed.
      assert(harness.writer.stop(success = false).isEmpty,
        "Failing after a successful stop must return None")
      assert(harness.spillManager.isClosed,
        "Failing after a successful stop must release everything the attempt held")
      assert(harness.writer.stop(success = false).isEmpty,
        "A repeated unsuccessful stop must be a no-op returning None")
      intercept[IllegalStateException] {
        harness.writer.write(Iterator.empty)
      }
    }
  }

  test("getPartitionLengths reports one non-null length per declared partition") {
    val partitions = 5
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      val before = harness.writer.getPartitionLengths()
      assert(before != null, "The partition length vector must never be null, even before a write")
      assert(before.length === partitions,
        s"The partition length vector must have one entry per declared partition, i.e. $partitions")
      assert(before.forall(_ == 0L), "Nothing may be reported as written before a write")

      harness.writer.write(deterministicRecords(200, seed = 7L, keySpace = 40).iterator)
      val after = harness.writer.getPartitionLengths()
      assert(after != null, "The partition length vector must never be null after a write")
      assert(after.length === partitions,
        "A write must not change the length of the partition length vector")
      assert(after.forall(_ >= 0L), "No partition may report a negative number of bytes")
      assert(after.sum === harness.writer.payloadBytesStreamed,
        s"The reported lengths must sum to the ${harness.writer.payloadBytesStreamed} payload " +
          "bytes this writer streamed")
      harness.writer.streamedPartitions.foreach { partitionId =>
        assert(after(partitionId) > 0L,
          s"Partition $partitionId was streamed to, so it must report bytes")
      }
      assert(harness.writer.stop(success = true).isDefined, "The stop must succeed")
      assert(harness.writer.getPartitionLengths().sameElements(after),
        "The stop must not change the lengths the write reported")
    }
  }

  test("write metrics reach the task thread only and are not inflated by a spill") {
    // A real clock, not a manual one: the elapsed write interval is measured with nanoTime, and a
    // clock the test never advances would legitimately report zero. Only the sign is asserted.
    val harness = newHarness(clock = wallClock())
    try {
      val records = deterministicRecords(512, seed = 8L, keySpace = 64)
      harness.writer.write(records.iterator)
      assert(harness.writer.stop(success = true).isDefined, "The stop must succeed")
      // Completing the task is what fires the listeners that publish spill volume, so the task's
      // own accumulators are read after it.
      harness.close()

      val metrics = harness.metrics
      assert(metrics.recordsWritten === records.size.toLong,
        s"The write reporter must have been told about all ${records.size} records, or the Web " +
          "UI, the history server and the metrics REST API stay blank for streaming shuffles")
      assert(metrics.bytesWritten === harness.writer.payloadBytesStreamed,
        "The write reporter must count payload bytes, exactly as the partition lengths do")
      assert(metrics.writeTime > 0L,
        "The write reporter must have been told about a positive elapsed write interval")
      assert(metrics.bytesDecremented === 0L,
        "Nothing on the streaming write path decrements written bytes")
      assert(metrics.recordsDecremented === 0L,
        "Nothing on the streaming write path decrements written records")
      assertSingleThreadedReporting(metrics.foreignThreadCallCount,
        "The streaming shuffle write reporter")
      assert(metrics.reportingThread.contains(Thread.currentThread()),
        "Every reporter call must arrive on the task thread; a Netty event-loop thread may only " +
          "enqueue, because the reporter contract permits implementations not to synchronize")

      // Spill volume lands on the accumulators Spark already has, and never on the task's own
      // shuffle-write reporter, which the spill path must not touch at all.
      val taskMetrics = harness.context.taskMetrics
      assert(taskMetrics.diskBytesSpilled > 0L,
        "Making the retained window durable must be reported on the existing diskBytesSpilled " +
          "accumulator rather than on a parallel counter of the streaming subsystem's own")
      assert(taskMetrics.peakExecutionMemory > 0L,
        "The peak buffer reservation must be reported on the existing peakExecutionMemory " +
          "accumulator")
      assert(taskMetrics.shuffleWriteMetrics.bytesWritten === 0L,
        "The spill path must write to a fresh ShuffleWriteMetrics of its own; a non-zero reading " +
          "here would mean spilled bytes were also counted as shuffle-written bytes")
      assert(metrics.bytesWritten === harness.writer.payloadBytesStreamed,
        s"A spill of ${taskMetrics.diskBytesSpilled} bytes must not inflate the " +
          s"${metrics.bytesWritten} payload bytes reported as written")
    } finally {
      harness.close()
    }
  }

  test("the consumer failure flow arms at ten seconds and backs off exponentially") {
    assert(ProducerConnectionTimeoutMillis === 5000L,
      "The specified producer connection timeout is 5 s")
    assert(JustBeforeProducerTimeoutMillis === 4999L,
      "The non-firing boundary of the producer timeout is 4999 ms")
    assert(ConsumerLivenessTimeoutMillis === 10000L,
      "The specified consumer liveness window is 10 s")
    assert(JustBeforeConsumerLivenessMillis === 9999L,
      "The non-firing boundary of the consumer liveness window is 9999 ms")
    assert(RetryBaseBackoffMillis === 1000L, "The specified retry backoff starts at 1 s")
    assert(MaxRetryAttempts === 5, "The specified retry budget is 5 attempts")
    assert(RetryBackoffLadderMillis === Seq(1000L, 2000L, 4000L, 8000L, 16000L),
      s"The backoff ladder must double from one second for five attempts, but was " +
        s"${RetryBackoffLadderMillis.mkString(", ")}")
    assert(StreamingShuffleWriter.CONSUMER_LIVENESS_TIMEOUT_MS === ConsumerLivenessTimeoutMillis,
      "The writer must enforce the same consumer liveness window the protocol defines")
    assert(StreamingShuffleWriter.REPLAY_BASE_BACKOFF_MS === RetryBaseBackoffMillis,
      "The writer must replay on the same base backoff the protocol defines")
    assert(StreamingShuffleWriter.MAX_REPLAY_ATTEMPTS === MaxRetryAttempts,
      "The writer must bound replay at the same attempt count the protocol defines")
    assert(StreamingShuffleWriter.MAX_REPLAY_BACKOFF_MS === 16000L,
      "The last rung of the ladder is 16 s, which is where the writer's backoff is capped")

    val clock = newManualClock()
    withHarness(newHarness(clock = clock)) { harness =>
      val backpressure = harness.backpressure
      val key = BackpressureStreamKey.forProducer(harness.shuffleId, defaultMapId,
        defaultTaskAttemptId, partitionId = 0, consumerId = consumerId)
      assert(backpressure.registerStream(key, MaxEncodedFrameBytes.toLong),
        "Registering a producer ledger for the first time must open it")
      val blockBytes = 4096L
      assert(backpressure.tryAdmit(key, blockBytes, 0L),
        "A block within credit must be admitted for sending")
      assert(backpressure.outstandingBytes(key) === blockBytes,
        "An admitted block's bytes must be outstanding until they are acknowledged")

      advanceJustBeforeConsumerLivenessTimeout(clock)
      assert(!backpressure.isConsumerTimedOut(key),
        s"The consumer liveness window must not fire at $JustBeforeConsumerLivenessMillis ms")
      clock.advance(1L)
      assert(backpressure.isConsumerTimedOut(key),
        s"The consumer liveness window must fire at exactly $ConsumerLivenessTimeoutMillis ms")
      assert(backpressure.timedOutConsumerStreams.contains(key),
        "A timed out stream must be reported in the set the writer polls")

      // The unacknowledged window is retained, inclusive at both ends, and is the only thing that
      // may be replayed; a request outside it can never be served.
      assert(backpressure.unacknowledgedWindow(key).contains((0L, 0L)),
        "A single-element unacknowledged window is valid and inclusive at both ends")
      assert(backpressure.unacknowledgedBlockCount(key) === 1,
        "Exactly the one unacknowledged block must be counted")
      assert(backpressure.isWithinUnacknowledgedWindow(key, 0L),
        "The one block sent must be inside the unacknowledged window")
      assert(!backpressure.isWithinUnacknowledgedWindow(key, 1L),
        "A block that was never sent cannot be inside the unacknowledged window")
      assert(backpressure.canServeRetransmit(key,
          retransmitRequest(harness.shuffleId, defaultMapId, 0, 0L, 0L)),
        "A retransmission scoped to the unacknowledged window must be serviceable")
      assert(!backpressure.canServeRetransmit(key,
          retransmitRequest(harness.shuffleId, defaultMapId, 0, 1L, 1L)),
        "A retransmission outside the unacknowledged window must be refused, because those bytes " +
          "were released the moment the consumer acknowledged them")

      assert(backpressure.tryAcknowledge(key, 0L).isDefined,
        "An acknowledgement inside the sent range must be applied rather than refused")
      assert(backpressure.outstandingBytes(key) === 0L,
        "An acknowledgement must release the bytes it covers")
      assert(!backpressure.isConsumerTimedOut(key),
        "A stream with nothing outstanding is idle rather than timed out")

      // Retention plus spill: the unacknowledged window survives eviction, moving to disk instead
      // of being discarded, which is what makes replay possible without a stage recomputation.
      val spillManager = harness.spillManager
      spillManager.registerPartitionCount(defaultPartitions)
      val nextSequence = Array.fill(defaultPartitions)(0L)
      fillToUtilization(spillManager, nextSequence, DefaultSpillThresholdPercent.toLong, 64 * 1024)
      assert(spillManager.maybeSpill(),
        "Utilisation at or above the threshold must evict the largest buffered partitions")
      val spilled = spillManager.allSpilledBlocks
      assert(spilled.nonEmpty, "Eviction must have moved blocks to disk")
      spilled.foreach { block =>
        assert(spillManager.retainsBlock(block.partitionId, block.sequenceNumber),
          s"Block ${block.sequenceNumber} of partition ${block.partitionId} must still be " +
            "retained after eviction, because an unacknowledged block is replayable from disk")
        assert(spillManager.spilledBlock(block.partitionId, block.sequenceNumber).isDefined,
          s"Block ${block.sequenceNumber} of partition ${block.partitionId} must be locatable on " +
            "disk after eviction")
      }
    }
  }

  test("a consumer that stops acknowledging is replayed by the writer, on the backoff ladder") {
    // The FR-9 flow, driven through the components that implement it rather than through the
    // protocol and the retained store directly. A live consumer subscribes, takes blocks, and then
    // stops acknowledging; the writer's own maintenance pass is what must notice, and what it must
    // do is retain and REPLAY -- never discard, and never fail the task while the budget lasts.
    //
    // The clock is advanced from inside the record iterator, because the writer's maintenance runs
    // on its own cadence during `write` and there is no other way to place a clock reading inside
    // it. Nothing sleeps: every window below is a reading of the injected clock.
    // The executor-memory figure is small on purpose. A stall is only detectable once bytes are
    // OUTSTANDING to the consumer, and bytes only become outstanding when a block is cut -- so a
    // fixture whose framing capacity is larger than its whole output frames one block at the very
    // end and has nothing outstanding while the liveness window elapses. A 256 KiB figure gives a
    // roughly 13 KiB block, so blocks start leaving within the first couple of thousand records and
    // the window elapses against a consumer that is genuinely behind.
    val partitions = 1
    withHarness(newHarness(
        numPartitions = partitions,
        executorMemoryBytes = 256L * 1024L)) { harness =>
      val clock = harness.clock.asInstanceOf[ManualClock]
      val consumer = attachConsumer(harness)
      try {
        consumer.subscribe(harness.shuffleId, defaultMapId, partitionId = 0)
        assert(harness.serverHandler.subscriptionCount === 1,
          "The consumer must be subscribed before the producer starts, so the stall is a stall " +
            "rather than an absence")

        // Each element advances the clock by one millisecond, so the writer's maintenance cadence
        // is crossed roughly once per thousand records and the liveness window exactly once.
        // The instant at which each successive replay attempt first becomes visible is recorded, so
        // the gaps between them can be compared against the specified ladder afterwards.
        val stepMillis = 1L
        val recordCount = 30000
        val firstSeenAtAttempt = mutable.HashMap.empty[Long, Long]
        val maintenanceGranularityMs =
          StreamingShuffleWriter.MAINTENANCE_RECORD_INTERVAL.toLong * stepMillis
        val observed = deterministicRecords(recordCount, seed = 23L, keySpace = 40).iterator.map {
          record =>
            clock.advance(stepMillis)
            val attempts = harness.writer.replayAttemptCount
            if (attempts > 0L && !firstSeenAtAttempt.contains(attempts)) {
              firstSeenAtAttempt(attempts) = clock.getTimeMillis() - ManualClockEpochMillis
            }
            record
        }
        harness.writer.write(observed)

        // The stall was noticed, reported once, and answered with replay rather than with failure.
        assert(harness.writer.consumerStallCount === 1L,
          s"The stall must be reported exactly once per stalled stream, yet it was reported " +
            s"${harness.writer.consumerStallCount} time(s)")
        assert(harness.writer.replayAttemptCount === MaxRetryAttempts.toLong,
          s"The writer must spend exactly the specified $MaxRetryAttempts replay attempts over a " +
            s"stall this long and no more, yet it made ${harness.writer.replayAttemptCount}")
        assert(harness.errorNotifier.error.isEmpty,
          s"No failure may be latched while the replay budget lasts, yet " +
            s"${harness.errorNotifier.error} was")

        // The first replay cannot have happened before the liveness window elapsed, and the window
        // is measured from the last acknowledgement progress -- of which there was none, so from
        // the subscription. The maintenance cadence is the observation's resolution, which is why
        // the upper bound is that cadence and not zero.
        val firstReplayAt = firstSeenAtAttempt(1L)
        assert(firstReplayAt >= ConsumerLivenessTimeoutMillis,
          s"A replay observed at $firstReplayAt ms would be inside the " +
            s"$ConsumerLivenessTimeoutMillis ms liveness window, which must not fire early")
        assert(firstReplayAt < ConsumerLivenessTimeoutMillis + maintenanceGranularityMs,
          s"and one observed at $firstReplayAt ms would be more than one " +
            s"$maintenanceGranularityMs ms maintenance pass late, which would mean something " +
            "other than the window governed it")

        // ==Exponential backoff, measured==
        //
        // The gap between successive attempts must be at least the ladder's rung for that
        // attempt, which is what "exponential backoff starting at one second" means. Observation
        // happens once per record and the writer acts once per maintenance pass, so each measured
        // gap can overrun its rung by up to one pass -- but it can never fall short of it, and that
        // is the direction a writer replaying too eagerly would fail in.
        (2L to MaxRetryAttempts.toLong).foreach { attempt =>
          val rung = RetryBackoffLadderMillis((attempt - 2L).toInt)
          val gap = firstSeenAtAttempt(attempt) - firstSeenAtAttempt(attempt - 1L)
          assert(gap >= rung,
            s"Attempt $attempt followed attempt ${attempt - 1} after only $gap ms, inside its " +
              s"$rung ms rung of the ladder ${RetryBackoffLadderMillis.mkString(", ")}")
          assert(gap < rung + 2L * maintenanceGranularityMs,
            s"Attempt $attempt followed attempt ${attempt - 1} after $gap ms, far beyond its " +
              s"$rung ms rung, so something other than the ladder governed the wait")
        }
        val totalLadderMillis = RetryBackoffLadderMillis.take(MaxRetryAttempts - 1).sum
        assert(firstSeenAtAttempt(MaxRetryAttempts.toLong) - firstReplayAt >= totalLadderMillis,
          s"Five attempts cannot be spent in less than the ${totalLadderMillis} ms the ladder's " +
            "first four rungs require, which is what stops a stalled consumer from being " +
            "retransmitted at the maintenance cadence")

        // Retained rather than discarded: the window the consumer never acknowledged is still
        // replayable, from memory or from spill, and the consumer really did receive replays.
        assert(harness.serverHandler.unacknowledgedBytes > 0L,
          "The unacknowledged window must still be owed to the consumer")
        assert(harness.serverHandler.acknowledgedPosition(0) === AckMessage.NOTHING_CONSUMED,
          "and its acknowledged position must still be the nothing-consumed sentinel")
        val delivered = consumer.drainOutbound().collect { case block: DataBlockMessage => block }
        assert(delivered.nonEmpty, "The consumer must have received blocks")
        val deliveredSequences = delivered.map(_.sequenceNumber())
        assert(deliveredSequences.distinct.size < deliveredSequences.size,
          s"At least one block must have arrived more than once, which is what a replay IS, yet " +
            s"the consumer saw each of ${deliveredSequences.distinct.size} block(s) exactly once")
        assert(delivered.forall(_.verifyChecksum()),
          "and every replayed frame must verify, because a replay is the same bytes and not a " +
            "re-framing of different ones")

        // A stop after all that still succeeds: a stalled consumer is a degradation, not a failure.
        assert(harness.writer.stop(success = true).isDefined,
          "The attempt must still complete successfully, because its output is intact")
      } finally {
        consumer.close()
      }
    }
  }

  test("the writer detects a silent consumer at the ten second boundary and not before") {
    // The behavioural half of the FR-9 window: a real writer streams to a real subscribed consumer
    // that never acknowledges, and the producer's own stall detector is asked at 9,999 ms and again
    // at 10,000 ms. Nothing sleeps; the boundary is a clock reading this test advances.
    val clock = newManualClock()
    withHarness(newHarness(numPartitions = DegradationPartitions,
        executorMemoryBytes = DegradationExecutorMemoryBytes, clock = clock)) { harness =>
      val consumer = new ConsumerChannel(harness.serverHandler, consumerId)
      consumer.subscribe(partitionId = 0, timestampMs = clock.getTimeMillis())
      assert(harness.serverHandler.liveConsumerCount === 1,
        "the in-progress block request must have opened exactly one consumer session")

      harness.writer.write(deterministicRecords(StreamedRecordsPerConsumerCase, seed = 81L,
        keySpace = 8).iterator)
      val delivered = consumer.drainInboundBlocks()
      assert(delivered.nonEmpty,
        "a subscribed consumer must actually receive streamed blocks, otherwise the stall this " +
          "case measures would be a stall of nothing")
      assert(harness.serverHandler.unacknowledgedBlockCount > 0,
        "every delivered block must remain unacknowledged, because this consumer never " +
          "acknowledged anything")

      advanceJustBeforeConsumerLivenessTimeout(clock)
      assert(!harness.serverHandler.isConsumerStalled(0),
        s"the producer must not call its consumer stalled at $JustBeforeConsumerLivenessMillis ms")
      assert(harness.serverHandler.stalledPartitions.isEmpty,
        "no partition may be reported stalled one millisecond short of the window")

      clock.advance(1L)
      assert(harness.serverHandler.isConsumerStalled(0),
        s"the producer must call its consumer stalled at exactly " +
          s"$ConsumerLivenessTimeoutMillis ms of acknowledgement silence")
      assert(harness.serverHandler.stalledPartitions === Seq(0),
        s"exactly the silent partition must be reported stalled, but " +
          s"${harness.serverHandler.stalledPartitions.mkString(", ")} was")

      // An acknowledgement disarms it, which is the other half of the contract: the detector
      // measures silence, not elapsed time.
      consumer.acknowledge(partitionId = 0, position = delivered.last.sequenceNumber())
      assert(!harness.serverHandler.isConsumerStalled(0),
        "an acknowledgement must disarm the stall detector, because progress means the consumer " +
          "is alive")
      assert(harness.serverHandler.acknowledgedPosition(0) === delivered.last.sequenceNumber(),
        "the acknowledged position must have advanced to the block the consumer named")
      consumer.crash()
    }
  }

  test("an unacknowledged window survives a consumer crash and is replayed on reconnection") {
    // FR-9 end to end through the real components: a consumer subscribes, receives blocks, crashes
    // without acknowledging, and a new channel carrying the SAME identity returns. The producer
    // must recognise it, resume it, and re-deliver from the retained window -- from memory and from
    // a spill segment alike, since eviction is forced in between.
    val clock = newManualClock()
    withHarness(newHarness(numPartitions = DegradationPartitions,
        executorMemoryBytes = DegradationExecutorMemoryBytes, clock = clock)) { harness =>
      val first = new ConsumerChannel(harness.serverHandler, consumerId)
      first.subscribe(partitionId = 0, timestampMs = clock.getTimeMillis())
      harness.writer.write(deterministicRecords(StreamedRecordsPerConsumerCase, seed = 82L,
        keySpace = 8).iterator)
      val original = first.drainInboundBlocks()
      assert(original.nonEmpty, "the first session must have received blocks to replay later")
      val originalSequences = original.map(_.sequenceNumber())

      // Eviction, so that at least part of the replay is served from disk. A block on disk is fully
      // serviceable and a memory-only view of the window would refuse it, which is exactly the
      // mistake this assertion exists to catch. The write above already evicts under this budget;
      // the explicit pass below is what makes the case independent of that arithmetic.
      harness.spillManager.maybeSpill()
      val spilled = harness.spillManager.allSpilledBlocks.map(_.sequenceNumber).toSet
      assert(spilled.nonEmpty,
        "the retained window must be at least partly on disk for the replay to cover a spill " +
          "segment as well as memory")

      first.crash()
      assert(harness.serverHandler.liveConsumerCount === 0,
        "a crashed consumer must leave no live session behind")
      assert(harness.serverHandler.unacknowledgedBlockCount > 0,
        "the unacknowledged window must be retained across the crash rather than discarded: that " +
          "retention is what makes a reconnection a resume instead of a recomputation")

      clock.advance(RetryBaseBackoffMillis)
      val resumed = new ConsumerChannel(harness.serverHandler, consumerId)
      resumed.subscribe(partitionId = 0, timestampMs = clock.getTimeMillis())
      assert(harness.serverHandler.resumedSessionCount >= 1L,
        s"the returning consumer must be recognised as the same session and resumed, but the " +
          s"producer counted ${harness.serverHandler.resumedSessionCount} resume(s)")
      val replayed = resumed.drainInboundBlocks().map(_.sequenceNumber())
      assert(replayed.nonEmpty,
        "a resumed consumer must be re-sent the window it never acknowledged")
      assert(replayed.forall(originalSequences.contains),
        s"a replay may only re-send blocks the original session was sent, but replayed " +
          s"${replayed.mkString(", ")} against ${originalSequences.mkString(", ")}")
      assert(replayed === replayed.sorted,
        s"replayed blocks must arrive in sequence order, but arrived as ${replayed.mkString(", ")}")
      if (spilled.nonEmpty) {
        assert(replayed.exists(spilled.contains),
          s"at least one replayed block must have come from a spill segment, but the replay " +
            s"covered ${replayed.mkString(", ")} while ${spilled.mkString(", ")} were on disk")
      }
      resumed.crash()
    }
  }

  test("replay is paced on the one second ladder and escalates after five attempts") {
    // The retry budget and its pacing, driven through the producer that owns them rather than
    // restated from a constant. A request inside the backoff is deferred, a request at the boundary
    // is serviced, and the sixth attempt escalates through the notifier -- which is the escalation
    // the failure protocol prescribes once backoff is exhausted.
    val clock = newManualClock()
    withHarness(newHarness(numPartitions = DegradationPartitions,
        executorMemoryBytes = DegradationExecutorMemoryBytes, clock = clock)) { harness =>
      val consumer = new ConsumerChannel(harness.serverHandler, consumerId)
      consumer.subscribe(partitionId = 0, timestampMs = clock.getTimeMillis())
      harness.writer.write(deterministicRecords(StreamedRecordsPerConsumerCase, seed = 83L,
        keySpace = 8).iterator)
      val delivered = consumer.drainInboundBlocks()
      assert(delivered.size >= 2,
        s"the ladder needs a window of at least two blocks to repair, but only ${delivered.size} " +
          "were delivered")
      val target = delivered.head.sequenceNumber()

      RetryBackoffLadderMillis.zipWithIndex.foreach { case (backoffMillis, index) =>
        val attempt = index + 1
        assert(harness.serverHandler.retransmit(0, target, target) > 0,
          s"repair attempt $attempt must be serviced, because the previous backoff has elapsed")
        assert(harness.serverHandler.retransmit(0, target, target) === 0,
          s"a second request inside the backoff of attempt $attempt must be deferred rather than " +
            "serviced, which is what paces the repair")
        clock.advance(backoffMillis - 1L)
        assert(harness.serverHandler.retransmit(0, target, target) === 0,
          s"a request one millisecond short of the ${backoffMillis} ms backoff of attempt " +
            s"$attempt must still be deferred")
        clock.advance(1L)
      }

      assert(harness.serverHandler.retransmit(0, target, target) === 0,
        s"the request after $MaxRetryAttempts attempts must be refused rather than serviced")
      val escalated = harness.errorNotifier.error
      assert(escalated.isDefined,
        s"an exhausted repair budget must escalate through the notifier so the producing task " +
          s"fails and stage recomputation recovers the output")
      assert(escalated.get.getMessage.contains("retransmission budget"),
        s"the escalation must name the exhausted budget so an operator can act on it; got " +
          s"'${escalated.get.getMessage}'")
      consumer.crash()
    }
  }

  test("a channel closing with nothing owed is not reported as a lost one") {
    // The classification a producer applies to every closing channel, asserted on both of its arms.
    // It matters because a reduce task closes its channel when it has finished reading, so a close
    // per consumer is the expected end of every healthy shuffle: an executor that described each of
    // them as a lost channel would put its log volume in direct proportion to the width of its
    // shuffles and would train an operator to ignore the warnings that do mean something.
    //
    // Blocks are admitted and offered directly rather than produced by a record loop, because what
    // is under test is what a session owed at the instant it closed, and that has to be established
    // exactly rather than approximately.
    val clock = newManualClock()
    withHarness(newHarness(clock = clock)) { harness =>
      harness.spillManager.registerPartitionCount(defaultPartitions)
      assert(harness.blockResolver.registerProducer(harness.shuffleId, defaultMapId,
          defaultTaskAttemptId, harness.spillManager),
        "the fixture must be able to publish its retained output before offering a block")
      val priority = StreamingShuffleServerHandler.EgressPriority(
        stageId = 0, stageAttemptNumber = 0, taskAttemptId = defaultTaskAttemptId,
        attemptNumber = 0)

      /**
       * Subscribes one consumer and has exactly one block delivered to it.
       *
       * @param identity the consumer session identity
       * @return the consumer and the sequence number it was sent
       */
      def deliverOneBlockTo(identity: String): (ConsumerChannel, Long) = {
        val consumer = new ConsumerChannel(harness.serverHandler, identity)
        consumer.subscribe(partitionId = 0, timestampMs = clock.getTimeMillis())
        consumer.drainInbound()
        val sequenceNumber = admitAndEnqueue(harness, partitionId = 0, priority = priority)
        val delivered = consumer.drainInboundBlocks().map(_.sequenceNumber())
        assert(delivered === Seq(sequenceNumber),
          s"consumer $identity must have been sent block $sequenceNumber but was sent " +
            s"${delivered.mkString(", ")}")
        (consumer, sequenceNumber)
      }

      assert(harness.serverHandler.lossyChannelClosureCount === 0L,
        "a producer that has lost nothing must report no lost channel")

      // Arm 1: a consumer that closes still owing the block it was sent. This is the condition the
      // failure protocol exists for, and it keeps its warning.
      val (stalled, stalledSequence) = deliverOneBlockTo(s"$consumerId-stalled")
      assert(harness.serverHandler.unacknowledgedBytes > 0L,
        "the stalled session must owe bytes at the instant it closes, or the arm under test is " +
          "not the arm being reached")
      assert(harness.serverHandler.sessionCount === 1,
        "the close must have a session to classify: a close with no session skips the " +
          "classification altogether and would make a zero reading meaningless")
      stalled.crash()
      assert(harness.serverHandler.lossyChannelClosureCount === 1L,
        "a channel that closes owing bytes must be counted as a lost one, but the producer " +
          s"counted ${harness.serverHandler.lossyChannelClosureCount}")
      assert(harness.serverHandler.sessionCount === 0 &&
          harness.serverHandler.liveConsumerCount === 0,
        "the closed channel's session must be dropped, because that channel will never carry " +
          "anything again")
      assert(harness.spillManager.retainsBlock(0, stalledSequence),
        "the bytes a lost channel never acknowledged must stay in the retained store, because " +
          "retention is what makes a reconnection a resume rather than a recomputation")
      assert(harness.serverHandler.unacknowledgedBlockCount > 0,
        "the retained window must survive the loss of the channel that owed it")

      // Arm 1 again, on a second channel: the log is aggregated through a window, the COUNT is not.
      // A reading of one after two lost channels is an aggregation gate leaking into a metric.
      val (secondStalled, _) = deliverOneBlockTo(s"$consumerId-stalled-again")
      assert(harness.serverHandler.unacknowledgedBytes > 0L,
        "the second stalled session must also owe bytes when it closes")
      secondStalled.crash()
      assert(harness.serverHandler.lossyChannelClosureCount === 2L,
        "every lost channel must be counted even though only some are logged individually, but " +
          s"the producer counted ${harness.serverHandler.lossyChannelClosureCount}")

      // Arm 2: a consumer that acknowledges everything it was sent and then closes. Nothing owed,
      // nothing queued: this is the orderly end of a healthy read and must not be counted at all.
      val (finished, finishedSequence) = deliverOneBlockTo(s"$consumerId-finished")
      finished.acknowledge(partitionId = 0, position = finishedSequence)
      assert(harness.serverHandler.acknowledgedPosition(0) === finishedSequence,
        s"the finished consumer must have acknowledged through $finishedSequence but it " +
          s"reports ${harness.serverHandler.acknowledgedPosition(0)}")
      assert(harness.serverHandler.unacknowledgedBytes === 0L,
        "a fully acknowledged session owes nothing")
      assert(harness.serverHandler.pendingBytes === 0L,
        "a fully drained session has nothing queued either")
      assert(harness.serverHandler.sessionCount === 1,
        "the orderly close must also have a session to classify")
      finished.disconnect()
      assert(harness.serverHandler.lossyChannelClosureCount === 2L,
        "an orderly close owing nothing must leave the lost-channel count untouched, but it rose " +
          s"to ${harness.serverHandler.lossyChannelClosureCount}")
      assert(harness.serverHandler.sessionCount === 0,
        "the orderly close must still drop its session, which is what shows the quiet arm was " +
          "taken rather than the classification skipped")
      assert(harness.errorNotifier.error.isEmpty,
        "no channel closing -- owing or not -- may fail the producing task")
    }
  }

  test("a broken task completion listener fails the test rather than being logged away") {
    // The fixture's own teardown is asserted on here, because every cleanup case in this suite
    // rests on it: if close() swallowed a completion-listener failure, a writer that never released
    // its buffers would pass all of them. A listener registered after the writer's own therefore
    // raises, and the fixture must propagate it.
    val harness = newHarness()
    harness.context.addTaskCompletionListener[Unit] { _ =>
      throw new IllegalStateException("a deliberately broken task completion listener")
    }
    val failure = intercept[Throwable] {
      harness.close()
    }
    assert(failure.getMessage.contains("broken task completion listener") ||
        failure.getSuppressed.exists(_.getMessage.contains("broken task completion listener")) ||
        Option(failure.getCause).exists(_.getMessage.contains("broken task completion listener")),
      s"the fixture must surface the listener's failure rather than logging it; got " +
        s"'${failure.getMessage}'")
    assert(harness.context.isCompleted(),
      "the task must still have been completed, so the listeners that do work still ran")
    assert(harness.spillFiles().forall(!_.exists()),
      "the emergency release must still have run, so a raising listener cannot leak a spill file")
  }

  test("every condition that forbids streaming is settled before the first record") {
    // The pre-flight. Graceful degradation is specified to end in delegation without failing the
    // job, and a running attempt cannot honour that once it has consumed records -- the records it
    // has already framed exist only as serialized blocks. So every condition that can be settled
    // ahead of the first record is settled there, where the remedy is a TOTAL delegation that loses
    // nothing: the iterator is handed on untouched and the attempt succeeds with a complete map
    // output written by the unmodified sort-based path.
    //
    // Each row below is one of those conditions. What is asserted for all of them is the same
    // property, which is what makes this a statement about the pre-flight rather than about five
    // unrelated branches: the delegate wrote everything, this writer streamed nothing at all, and
    // nothing was published to any owner outside the writer that would then have to be withdrawn.
    def assertTotalDelegation(
        condition: String,
        partitions: Int,
        build: RecordingSortShuffleWriter => WriterHarness,
        expectedReason: StreamingShuffleFallbackReason,
        prepare: WriterHarness => Unit,
        alsoAssert: WriterHarness => Unit = _ => ()): Unit = {
      val delegate = new RecordingSortShuffleWriter(partitions)
      withHarness(build(delegate)) { fixture =>
        prepare(fixture)
        val records = deterministicRecords(PreflightRecords, seed = 93L, keySpace = 16)
        fixture.writer.write(records.iterator)

        assert(delegate.writeCallCount === 1,
          s"$condition: the attempt must be finished by exactly one sort-based write, but the " +
            s"delegate was written to ${delegate.writeCallCount} time(s)")
        assert(delegate.recordsWritten === records,
          s"$condition: the delegate must receive this task's input in its original order and " +
            s"entirety, but received ${delegate.recordsWritten.size} of ${records.size} records")
        assert(fixture.writer.recordsStreamed === 0L && fixture.writer.blocksStreamed === 0L &&
            fixture.writer.payloadBytesStreamed === 0L,
          s"$condition: a refusal settled before the first record streams nothing at all, but " +
            s"${fixture.writer.recordsStreamed} record(s) and " +
            s"${fixture.writer.blocksStreamed} block(s) were streamed")
        assert(fixture.gateway.declaredFallbacks.contains(expectedReason),
          s"$condition: the stand-down must be declared shuffle-wide as $expectedReason so every " +
            s"other participant takes the same path, but the gateway saw " +
            s"${fixture.gateway.declaredFallbacks.mkString(", ")}")
        // Withdrawn exactly once, and that is the contract rather than an accident. The manager
        // publishes three owners of this generation BEFORE the writer exists -- the shuffle's share
        // of the executor's egress allowance, this executor's routing entry and the driver's
        // producer address -- so a refusal settled inside the writer inherits the obligation to
        // hand all three back before it assigns its delegate. Nothing streamed, so the withdrawal
        // discards no output; what omitting it would leave behind is a producer address a consumer
        // can still resolve, connect to and then wait out its full timeout on.
        assert(fixture.gateway.invalidations ===
            Seq(StreamingShuffleInvalidationReason.IncompleteStream),
          s"$condition: the generation published for this attempt before it existed must be " +
            s"withdrawn exactly once before the sort-based delegate takes over, but the gateway " +
            s"saw ${fixture.gateway.invalidations.mkString(", ")}")
        assert(!fixture.serverHandler.servesRetainedOutput,
          s"$condition: no retained output may be published by an attempt that never streamed")
        assert(fixture.spillManager.bufferedBytes === 0L &&
            fixture.spillManager.allSpilledBlocks.isEmpty,
          s"$condition: no buffer may be held and no spill file written by an attempt that never " +
            s"framed a block")

        // The writer contract belongs to the delegate from end to end, its stop protocol included.
        val status = fixture.writer.stop(success = true)
        assert(status.isDefined,
          s"$condition: a delegated attempt must still report a map status, so the unmodified " +
            "shuffle write path's unconditional dereference succeeds")
        assert(status.get.location === BlockManagerId("sort-delegate", "delegate-host", 7077),
          s"$condition: the status must be the delegate's own, because the delegate wrote the " +
            s"output; got ${status.get.location}")
        assert(fixture.writer.getPartitionLengths() eq delegate.getPartitionLengths(),
          s"$condition: the partition lengths must be the delegate's, because they describe the " +
            "bytes that were actually written")
        assert(fixture.metrics.bytesWritten === 0L && fixture.metrics.recordsWritten === 0L,
          s"$condition: a delegated attempt must publish none of its own write metrics, or the " +
            "task would report roughly twice the output that exists")
        assert(fixture.errorNotifier.error.isEmpty && fixture.errorNotifier.fetchFailure.isEmpty,
          s"$condition: a delegation is not a failure, so nothing may be latched for the task " +
            "thread to re-throw")
        alsoAssert(fixture)
      }
    }

    // 1. A protocol version this build cannot speak. Detected by an explicit compatibility check on
    // the version the shuffle was registered under, never inferred from a decode failure, so a
    // rolling upgrade degrades predictably instead of misreading bytes.
    assertTotalDelegation(
      condition = "a protocol version mismatch",
      partitions = DegradationPartitions,
      build = delegate => newHarness(
        numPartitions = DegradationPartitions,
        executorMemoryBytes = DegradationExecutorMemoryBytes,
        sortWriterFactory = Some(() => delegate),
        registration = RegistrationFixture(protocolVersion = IncompatibleProtocolVersion)),
      expectedReason = StreamingShuffleFallbackReason.ProtocolVersionMismatch,
      prepare = fixture => assert(
        !fixture.fallbackPolicy.checkProtocolVersion(fixture.handle.protocolVersion),
        "the fixture's handle must carry a version this build refuses, or the row proves nothing"))

    // 2. A trip this executor has already latched. The verdict is executor-wide, so a shuffle
    // starting after it must not begin streaming at all.
    assertTotalDelegation(
      condition = "a trip already latched on this executor",
      partitions = DegradationPartitions,
      build = delegate => newHarness(
        numPartitions = DegradationPartitions,
        executorMemoryBytes = DegradationExecutorMemoryBytes,
        sortWriterFactory = Some(() => delegate)),
      expectedReason = StreamingShuffleFallbackReason.NetworkSaturation,
      prepare = fixture => {
        fixture.fallbackPolicy.recordLinkUtilization(
          usedBytesPerSecond = 990.0d, capacityBytesPerSecond = 1000.0d)
        assert(fixture.fallbackPolicy.hasTripped,
          "the fixture's policy must have latched a trip before the first record")
      })

    // 3. A shuffle stood down by some OTHER executor. The latch above is local; this verdict is the
    // shuffle's, and a producer that consulted only its own latch would keep streaming a shuffle
    // whose consumers had already been told to stop reading it.
    assertTotalDelegation(
      condition = "a shuffle already stood down elsewhere",
      partitions = DegradationPartitions,
      build = delegate => newHarness(
        numPartitions = DegradationPartitions,
        executorMemoryBytes = DegradationExecutorMemoryBytes,
        sortWriterFactory = Some(() => delegate)),
      expectedReason = StreamingShuffleFallbackReason.ConsumerTooSlow,
      prepare = fixture => {
        fixture.fallbackPolicy.observeShuffleFallback(fixture.shuffleId,
          StreamingShuffleFallbackState(
            StreamingShuffleFallbackReason.ConsumerTooSlow.toString, declaredEpoch))
        assert(fixture.fallbackPolicy.shuffleHasFallenBack(fixture.shuffleId),
          "the fixture must know the shuffle has stood down before the first record")
      })

    // 4. A budget that cannot frame one block for one partition. Zero capacity is a legitimate
    // answer rather than an error: it says streaming is not viable for a shuffle this wide on an
    // executor this size, which is trip condition 2 exactly as specified.
    assertTotalDelegation(
      condition = "a budget too small to frame a single block",
      partitions = UnframeablePartitions,
      build = delegate => newHarness(
        numPartitions = UnframeablePartitions,
        executorMemoryBytes = DegradationExecutorMemoryBytes,
        bufferSizePercent = MinimumBufferSizePercent,
        sortWriterFactory = Some(() => delegate)),
      expectedReason = StreamingShuffleFallbackReason.MemoryPressure,
      prepare = fixture => {
        // Registered first because the partition count is the divisor of every ceiling, which is
        // the same order the writer's own pre-flight uses; reading the capacity before it would
        // divide by the unregistered default of one and report an allowance an order of magnitude
        // too generous -- and would freeze that wrong figure, since the derivation is memoised.
        fixture.spillManager.registerPartitionCount(UnframeablePartitions)
        assert(fixture.writer.blockPayloadCapacityBytes === 0,
          s"the fixture's budget of ${fixture.spillManager.totalBudgetBytes} bytes across " +
            s"$UnframeablePartitions partitions must leave no room for a block, but the writer " +
            s"derived a capacity of ${fixture.writer.blockPayloadCapacityBytes} bytes")
      })

    // 5. A framing reservation that cannot be taken even at its floor. Distinct from row 4: a block
    // would fit, but one minimum accumulator per declared partition cannot be held alongside what
    // the executor's allowance is already committed to.
    assertTotalDelegation(
      condition = "a framing reservation refused even at the floor",
      partitions = defaultPartitions,
      build = delegate => newHarness(
        numPartitions = defaultPartitions,
        executorMemoryBytes = NegotiationExecutorMemoryBytes,
        bufferSizePercent = MaximumBufferSizePercent,
        sortWriterFactory = Some(() => delegate)),
      expectedReason = StreamingShuffleFallbackReason.MemoryPressure,
      prepare = fixture => {
        fixture.spillManager.registerPartitionCount(defaultPartitions)
        assert(fixture.writer.blockPayloadCapacityBytes > 0,
          "this row must have room for a block, or it is row 4 over again rather than a refused " +
            "reservation")
        // The whole allowance is committed to scratch, which is the half of the budget that cannot
        // be spilled -- so no arrangement of eviction can make room and every candidate size
        // down to the floor is refused.
        assert(fixture.spillManager.reserveScratch(fixture.spillManager.totalBudgetBytes),
          "the fixture must be able to commit the whole allowance, or the refusal under test is " +
            "not the refusal being reached")
      },
      alsoAssert = fixture => {
        assert(fixture.fallbackPolicy.trippedReason.contains(
            StreamingShuffleFallbackReason.MemoryPressure),
          s"a reservation prevented outright is trip condition 2 and must be reported to the " +
            s"policy as such, but the policy reports ${fixture.fallbackPolicy.trippedReason}")
        fixture.spillManager.releaseScratch(fixture.spillManager.totalBudgetBytes)
      })
  }

  test("the framing reservation is negotiated down rather than declining to stream") {
    // The other arm of the same reservation. Its size is a PREFERENCE -- the block size trades
    // pipelining against footprint, and a smaller block still streams correctly -- so a reservation
    // that does not fit is retried smaller rather than refused. The difference matters because
    // refusing stands the whole shuffle down, including siblings already streaming that can no
    // longer be asked to change their minds; trading block size keeps the shuffle on one path.
    //
    // The control fixture is asserted first, so that the negotiated figure below is known to be a
    // negotiation rather than the derivation this budget would have produced anyway.
    val derivedCapacity = withHarness(newHarness(
        numPartitions = defaultPartitions,
        executorMemoryBytes = NegotiationExecutorMemoryBytes,
        bufferSizePercent = MaximumBufferSizePercent)) { control =>
      control.spillManager.registerPartitionCount(defaultPartitions)
      control.writer.blockPayloadCapacityBytes
    }
    assert(derivedCapacity / 2 > StreamingShuffleWriter.MIN_ACCUMULATOR_BYTES,
      s"this case needs a derived capacity whose half is still above the " +
        s"${StreamingShuffleWriter.MIN_ACCUMULATOR_BYTES} byte accumulator floor, or halving " +
        s"cannot lower the reservation at all; the fixture derived $derivedCapacity bytes, and " +
        "fixture's executor memory must be widened if the arithmetic has changed")

    val expectedCapacity = derivedCapacity / 2
    val expectedEnvelope = expectedCapacity.toLong * defaultPartitions
    withHarness(newHarness(
        numPartitions = defaultPartitions,
        executorMemoryBytes = NegotiationExecutorMemoryBytes,
        bufferSizePercent = MaximumBufferSizePercent)) { fixture =>
      fixture.spillManager.registerPartitionCount(defaultPartitions)
      assert(fixture.writer.blockPayloadCapacityBytes === derivedCapacity,
        "the fixture must derive the same capacity as the control, or the arithmetic below is " +
          "about two different budgets")
      // Exactly the halved envelope is left free, so the derived reservation cannot be taken and
      // the first halving fits precisely. Nothing about this is approximate: a fixture that left a
      // little more would pass whether the writer negotiated or not.
      val committed = fixture.spillManager.totalBudgetBytes - expectedEnvelope
      assert(committed > 0L, "the fixture must have budget left to commit")
      assert(fixture.spillManager.reserveScratch(committed),
        "the fixture must be able to commit all but the halved envelope")

      val records = deterministicRecords(PreflightRecords, seed = 94L, keySpace = 16)
      var envelopeHeldAtFirstRecord = 0L
      val observing = records.iterator.map { record =>
        if (envelopeHeldAtFirstRecord == 0L) {
          // Sampled at the first record, because each partition's share is returned as that
          // partition finishes: after the loop the reservation has been given back and the peak is
          // no longer observable.
          envelopeHeldAtFirstRecord = fixture.spillManager.scratchBytes
        }
        record
      }
      fixture.writer.write(observing)

      // The negotiated size is the observable: the fixture's loud default delegate would have
      // raised had the writer declined instead, so reaching this line at all is half the assertion.
      assert(fixture.writer.blockPayloadCapacityBytes === expectedCapacity,
        s"the writer must stream at the halved capacity of $expectedCapacity bytes rather than " +
          s"declining, but reports ${fixture.writer.blockPayloadCapacityBytes}")
      assert(envelopeHeldAtFirstRecord === committed + expectedEnvelope,
        s"the whole negotiated envelope must be held before the first record, so no later " +
          s"reservation can be refused: expected ${committed + expectedEnvelope} bytes held but " +
          s"$envelopeHeldAtFirstRecord were")
      assert(fixture.writer.recordsStreamed === records.size.toLong,
        s"every record must have been streamed, but ${fixture.writer.recordsStreamed} of " +
          s"${records.size} were")
      assert(fixture.gateway.declaredFallbacks.isEmpty,
        s"a reservation negotiated down must not stand the shuffle down, but the gateway " +
          s"saw ${fixture.gateway.declaredFallbacks.mkString(", ")}")
      assert(!fixture.fallbackPolicy.hasTripped,
        "a negotiated reservation is not memory pressure: nothing was prevented")

      val status = fixture.writer.stop(success = true)
      assert(status.isDefined && status.get.location === sc.env.blockManager.shuffleServerId,
        s"the status must be this streaming writer's own rather than a delegate's, but was " +
          s"${status.map(_.location)}")
      assert(fixture.writer.blocksStreamed > 0L && fixture.writer.payloadBytesStreamed > 0L,
        s"the negotiated capacity must have framed real blocks, but " +
          s"${fixture.writer.blocksStreamed} were cut")
      fixture.spillManager.releaseScratch(committed)
    }
  }

  test("a block the allowance cannot hold goes straight to disk and the shuffle keeps streaming") {
    // What a full buffer allowance costs, and what it must NOT cost. The specified answer to a full
    // buffer is to spill, so a block that cannot be held skips memory and is written straight to
    // local disk: the map output stays complete and the shuffle keeps the streaming path it is
    // already committed to. Standing the shuffle down here would be wrong twice over -- it would
    // tell consumers to read output this producer has already streamed from a sort-based path that
    // has none of it, and a task that fails to force the whole map stage to be recomputed takes the
    // job with it wherever retries are unavailable.
    //
    // A single reduce partition, deliberately. Each partition's framing share is returned as that
    // partition finishes, so on a wider shuffle the share released by the first partition to end
    // makes room for the last block of the second -- which is the peak-lowering behaviour finishing
    // is supposed to have, and which would make "every block took the disk route" false for a
    // reason that has nothing to do with the branch under test.
    //
    // The control fixture goes first, because "every block took the disk route" says nothing unless
    // the same records under the same shape would otherwise have gone to memory.
    val controlRecords = deterministicRecords(DirectDiskRecords, seed = 96L, keySpace = 8)
    withHarness(newHarness(numPartitions = 1,
        executorMemoryBytes = DegradationExecutorMemoryBytes)) { control =>
      control.writer.write(controlRecords.iterator)
      control.writer.stop(success = true)
      assert(control.writer.blocksStreamed > 1L,
        s"the control fixture must cut more than one block, or the fixture below is a " +
          s"statement about a single admission; it cut ${control.writer.blocksStreamed}")
      assert(control.spillManager.durableAdmissionCount === 0L,
        s"an unconstrained allowance must admit every block to memory, but " +
          s"${control.spillManager.durableAdmissionCount} went straight to disk")
    }

    withHarness(newHarness(numPartitions = 1,
        executorMemoryBytes = DegradationExecutorMemoryBytes)) { fixture =>
      var committed = 0L
      val records = deterministicRecords(DirectDiskRecords, seed = 96L, keySpace = 8)
      val constraining = records.iterator.map { record =>
        if (committed == 0L) {
          // Committed at the first record, which is after the framing reservation has been taken
          // and before any block has been cut. What is left of the allowance goes to scratch -- the
          // half of the budget that cannot be spilled -- so no arrangement of eviction can make
          // room and every admission is refused rather than merely delayed.
          val remaining = fixture.spillManager.totalBudgetBytes - fixture.spillManager.scratchBytes
          assert(remaining > 0L, "the fixture must have allowance left to commit")
          assert(fixture.spillManager.bufferedBytes === 0L,
            "nothing may be buffered yet, or eviction could free room and the refusal under test " +
              "would be a delay instead")
          assert(fixture.spillManager.reserveScratch(remaining),
            "the fixture must be able to commit what is left of the allowance")
          committed = remaining
        }
        record
      }
      fixture.writer.write(constraining)
      val status = fixture.writer.stop(success = true)

      assert(status.isDefined && status.get.location === sc.env.blockManager.shuffleServerId,
        s"the attempt must complete on the streaming path with its own status, but reported " +
          s"${status.map(_.location)}")
      assert(fixture.writer.recordsStreamed === records.size.toLong,
        s"every record must still have been streamed, but ${fixture.writer.recordsStreamed} of " +
          s"${records.size} were")
      assert(fixture.spillManager.durableAdmissionCount === fixture.writer.blocksStreamed,
        s"every one of the ${fixture.writer.blocksStreamed} block(s) must have taken the disk " +
          s"route, but ${fixture.spillManager.durableAdmissionCount} did")
      assert(fixture.writer.blocksStreamed > 1L,
        s"the fixture must have cut more than one block, so the disk route is asserted over a " +
          s"of admissions rather than over one; it cut ${fixture.writer.blocksStreamed}")

      // Every spill event this fixture counted is a direct admission and nothing else: no eviction
      // ran, because there was never anything resident to evict. The event is still counted: the
      // configured percentage could not hold the block and the block went to local disk, which is
      // the condition `shuffle.streaming.spillCount` exists to report, and publishing the volume
      // without the event would let an operator watch disk spilling climb at a spill count of zero.
      assert(fixture.spillManager.spillCount === fixture.spillManager.durableAdmissionCount,
        s"every spill event here must be a direct admission, but " +
          s"${fixture.spillManager.spillCount} event(s) were counted against " +
          s"${fixture.spillManager.durableAdmissionCount} admission(s)")
      assert(fixture.spillManager.memoryBytesSpilled === 0L,
        s"no memory was reclaimed, so no memory may be reported as spilled, but " +
          s"${fixture.spillManager.memoryBytesSpilled} bytes were")
      assert(fixture.spillManager.diskBytesSpilled > 0L,
        "the bytes really did reach local disk, so they must reach the disk accumulator")
      assert(fixture.spillManager.spillFailureCount === 0L,
        s"every direct admission must have succeeded, but " +
          s"${fixture.spillManager.spillFailureCount} failed")

      // The recovery rounds were spent before the disk route was taken, which is what makes the
      // direct admission a last resort rather than a first choice.
      assert(fixture.writer.admissionRetryCount >=
          StreamingShuffleWriter.MAX_ADMISSION_RECOVERY_ROUNDS.toLong *
            fixture.writer.blocksStreamed,
        s"each refused admission must exhaust its ${
          StreamingShuffleWriter.MAX_ADMISSION_RECOVERY_ROUNDS} recovery round(s) before going " +
          s"disk, but only ${fixture.writer.admissionRetryCount} retries were spent over " +
          s"${fixture.writer.blocksStreamed} block(s)")
      assert(fixture.writer.memoryPressureBrushCount === 0L,
        s"nothing was rescued by eviction, so nothing may be counted as a brush with pressure, " +
          s"but ${fixture.writer.memoryPressureBrushCount} were")

      // Visible in the flow-control telemetry, and deliberately absent from the fallback policy --
      // which is why it is reported through the non-degrading signal rather than as an allocation
      // failure. A met allowance is answered by spilling, so declaring the subsystem degraded here
      // would record that streaming should yield while this producer deliberately carries on.
      assert(fixture.backpressure.durableSpillAdmissionCount ===
          fixture.spillManager.durableAdmissionCount,
        s"every block that took the disk route must be visible in the backpressure telemetry an " +
          s"operator consults, but it counted " +
          s"${fixture.backpressure.durableSpillAdmissionCount} against " +
          s"${fixture.spillManager.durableAdmissionCount} admission(s)")
      assert(!fixture.backpressure.isDegraded && fixture.backpressure.degradationReasons.isEmpty,
        s"and nothing may declare the subsystem degraded for an allowance answered by spilling, " +
          s"yet it holds ${fixture.backpressure.degradationReasons.mkString(", ")}")
      assert(!fixture.fallbackPolicy.hasTripped,
        "a full buffer is answered by spilling, not by standing the shuffle down: the verdict is " +
          "shuffle-wide and this producer has already streamed output no sort-based path has")
      assert(fixture.gateway.declaredFallbacks.isEmpty,
        s"no stand-down may be declared for a block that reached disk, but the gateway saw " +
          s"${fixture.gateway.declaredFallbacks.mkString(", ")}")
      assert(fixture.errorNotifier.error.isEmpty,
        "a block that reached disk is not a failure")
      fixture.spillManager.releaseScratch(committed)
    }
  }

  test("a stand down that cannot be agreed fails the attempt instead of writing it twice") {
    // The one case in which a producer may NOT use the sort-based delegate. The verdict is
    // shuffle-wide, so a map output written by a second implementation while its siblings stream
    // is output no reduce-side read path can reassemble. If the coordinator cannot be reached, or
    // declines, the shuffle is therefore left exactly as it was and the attempt fails -- which is a
    // retry rather than a contradiction, and is the same choice the manager makes when it cannot
    // publish a producer.
    val delegate = new RecordingSortShuffleWriter(DegradationPartitions)
    val refusing = new RecordingCoordinatorGateway(refuseStandDown = true)
    withHarness(newHarness(
        numPartitions = DegradationPartitions,
        executorMemoryBytes = DegradationExecutorMemoryBytes,
        sortWriterFactory = Some(() => delegate),
        registration = RegistrationFixture(
          protocolVersion = IncompatibleProtocolVersion, gateway = refusing))) { fixture =>
      val records = deterministicRecords(PreflightRecords, seed = 95L, keySpace = 16)
      val refused = intercept[SparkException] {
        fixture.writer.write(records.iterator)
      }

      assert(refused.getMessage.contains("could not stand the shuffle down"),
        s"the failure must name what could not be agreed, so an operator reads it as a " +
          s"that was refused rather than as a streaming fault; got '${refused.getMessage}'")
      assert(refusing.declaredFallbacks ===
          Seq(StreamingShuffleFallbackReason.ProtocolVersionMismatch),
        s"the stand-down must have been attempted exactly once before the attempt failed, but " +
          s"gateway saw ${refusing.declaredFallbacks.mkString(", ")}")
      assert(delegate.writeCallCount === 0,
        s"the sort-based delegate must not be used while the rest of the shuffle streams, " +
          s"but it was written to ${delegate.writeCallCount} time(s)")
      assert(fixture.writer.recordsStreamed === 0L && fixture.writer.blocksStreamed === 0L,
        "a refusal settled before the first record streams nothing either way")
      assert(!fixture.fallbackPolicy.shuffleHasFallenBack(fixture.shuffleId),
        "the shuffle must be left exactly as it was, so the retry of this attempt is free to " +
          "reach whatever verdict the coordinator then reports")

      // The failure stop is the writer's own, because no delegate was installed, and it must leave
      // nothing behind: the retry lands on a fresh writer.
      assert(fixture.writer.stop(success = false).isEmpty,
        "a failed attempt reports no map status")
      assert(fixture.spillManager.bufferedBytes === 0L && fixture.spillManager.scratchBytes === 0L,
        s"a refused attempt must hold no buffer and no framing scratch, but held " +
          s"${fixture.spillManager.bufferedBytes} and ${fixture.spillManager.scratchBytes} bytes")
      assert(fixture.spillFiles().isEmpty,
        "a refused attempt cannot have written a spill file")
    }
  }

  test("a replay request never lets a block overtake a position the consumer is still owed") {
    // The invariant the whole retransmission protocol rests on: for one partition and one consumer,
    // the first delivery of each position is strictly ascending. It was violated by the two paths
    // that queue a block racing each other -- a replay request or a subscription, serviced on the
    // event-loop thread that delivered it, against the guarded drain loop paying the same owed run
    // down -- because taking a position off the run and queueing it are two steps and the egress
    // order is decided by the ticket taken in the second. One block leaving ahead of its
    // predecessor is enough: the consumer quarantines the position, asks for the run again, and the
    // producer's honest end-of-stream is then refused against a position the consumer never
    // reached, reported as a lost producer and paid for with a recomputation of the whole map
    // stage.
    withHarness(newHarness(numPartitions = 1,
        executorMemoryBytes = 4L * 1024L * 1024L)) { harness =>
      val consumer = attachConsumer(harness)
      try {
        consumer.subscribe(harness.shuffleId, defaultMapId, partitionId = 0)
        val firstDeliveryOrder = mutable.ArrayBuffer.empty[Long]
        val seen = mutable.HashSet.empty[Long]
        // A replay of everything delivered so far is asked for from inside the write, over and
        // over,
        // so the owed run is being paid down by the drain at the same time as production is adding
        // to it -- which is exactly the interleaving that used to reorder egress.
        val ordered = deterministicRecords(60000, seed = 23L, keySpace = 40)
        val records = ordered.iterator.map { record =>
          consumer.drainOutbound().foreach {
            case block: DataBlockMessage =>
              if (seen.add(block.sequenceNumber())) {
                firstDeliveryOrder += block.sequenceNumber()
              }
            case _ => ()
          }
          val highest = harness.writer.blocksStreamed - 1L
          if (highest > 0L) {
            harness.serverHandler.receive(consumer.client,
              new RetransmitRequestMessage(harness.shuffleId, defaultMapId, 0, 0L, highest)
                .toByteBuffer())
          }
          record
        }
        harness.writer.write(records)
        harness.writer.stop(success = true)
        consumer.drainOutbound().foreach {
          case block: DataBlockMessage =>
            if (seen.add(block.sequenceNumber())) {
              firstDeliveryOrder += block.sequenceNumber()
            }
          case _ => ()
        }

        assert(firstDeliveryOrder.size > 1,
          s"the fixture must deliver more than one block for an ordering claim to mean anything, " +
            s"but it delivered ${firstDeliveryOrder.size}")
        val outOfOrder = firstDeliveryOrder.toSeq.sliding(2)
          .collect { case Seq(previous: Long, next: Long) if next <= previous => (previous, next) }
          .toSeq
        assert(outOfOrder.isEmpty,
          s"every position must reach the consumer for the first time in ascending order, but " +
            s"${outOfOrder.mkString(", ")} arrived out of sequence out of " +
            s"${firstDeliveryOrder.size} first deliveries")
        assert(firstDeliveryOrder.head === 0L,
          s"and the run must start at the first block, but it started at " +
            s"${firstDeliveryOrder.head}")
      } finally {
        consumer.close()
      }
    }
  }

  test("a stream ends only after every block a consumer is owed has been delivered") {
    // A terminator states a total, and the consumer reconciles what it received against that total
    // and closes the stream. Emitting it while the consumer is still owed blocks therefore promises
    // an end that has not happened: the drain pays the owed run down afterwards, the blocks arrive
    // at a stream that has already ended, and the consumer -- which cannot tell an obsolete repair
    // from a producer contradicting its own end of stream -- reports the producer lost and the
    // upstream stage is recomputed although every byte of it was correct. Readiness is therefore
    // read from the owed run as well as from the queue.
    withHarness(newHarness(numPartitions = 1,
        executorMemoryBytes = 4L * 1024L * 1024L)) { harness =>
      val consumer = attachConsumer(harness)
      try {
        consumer.subscribe(harness.shuffleId, defaultMapId, partitionId = 0)
        val outbound = mutable.ArrayBuffer.empty[String]
        def collectOutbound(): Unit = consumer.drainOutbound().foreach {
          case block: DataBlockMessage => outbound += s"block ${block.sequenceNumber()}"
          case end: StreamTerminationMessage => outbound += s"end of stream ${end.totalBlocks()}"
          case _ => ()
        }
        // Replays asked for from inside the write keep an owed run alive right up to the moment the
        // stream finishes, which is the only window in which the ordering can be got wrong.
        val ordered = deterministicRecords(30000, seed = 71L, keySpace = 40)
        val records = ordered.iterator.map { record =>
          collectOutbound()
          val highest = harness.writer.blocksStreamed - 1L
          if (highest > 0L) {
            harness.serverHandler.receive(consumer.client,
              new RetransmitRequestMessage(harness.shuffleId, defaultMapId, 0, 0L, highest)
                .toByteBuffer())
          }
          record
        }
        harness.writer.write(records)
        harness.writer.stop(success = true)
        collectOutbound()

        val terminators = outbound.zipWithIndex.filter(_._1.startsWith("end of stream"))
        assert(terminators.size === 1,
          s"the partition must be ended exactly once, but the consumer saw " +
            s"${terminators.size}: ${outbound.mkString(", ")}")
        val afterTermination = outbound.drop(terminators.head._2 + 1)
        assert(afterTermination.isEmpty,
          s"and nothing may follow the end of stream, but " +
            s"${afterTermination.size} frame(s) did: ${afterTermination.mkString(", ")}")
      } finally {
        consumer.close()
      }
    }
  }

  test("an admission refused after eviction has spilled trips the memory-pressure fallback") {
    // The second specified trip condition, on the path the whole subsystem actually takes. Memory
    // pressure that eviction *rescues* is ordinary flow control and must not cost the job its fast
    // path -- that is the direct-to-disk case above, where nothing was ever resident and no
    // spilling could have helped. This is the other half: output IS resident, eviction runs and
    // moves bytes to disk, and the reservation is still refused. That is "memory pressure prevents
    // buffer allocation", it is the Spilling-to-Degraded transition the state machine names, and
    // leaving it unreported made the documented degradation inert exactly when it was needed --
    // ninety-nine percent buffer utilisation and dozens of spill events with the policy reporting
    // that nothing had tripped, while the job absorbed repeated map-stage recomputation instead of
    // yielding.
    val delegate = new RecordingSortShuffleWriter(DegradationPartitions)
    val harness = newHarness(
      numPartitions = DegradationPartitions,
      executorMemoryBytes = DegradationExecutorMemoryBytes,
      spillThreshold = 50,
      sortWriterFactory = Some(() => delegate))
    withHarness(harness) { fixture =>
      val records = deterministicRecords(DegradationRecords, seed = 71L, keySpace = 64)
      var reservedBytes = 0L
      // The squeeze begins only once eviction has genuinely moved bytes out of memory, so the
      // condition under test is a refusal that spilling could not repair rather than an allowance
      // that was never there. Everything resident is evicted first and the allowance is then taken
      // whole, which is the state an executor reaches when its other tasks claim the budget while
      // this one is mid-stream: spilling has already happened, and there is nothing left for
      // another round of it to free.
      val squeezing = records.iterator.map { record =>
        if (reservedBytes == 0L && fixture.spillManager.memoryBytesSpilled > 0L) {
          fixture.spillManager.spillAllRetained()
          reservedBytes = fixture.reserveRemainingAllowance()
          assert(reservedBytes > 0L,
            "the fixture must be able to take what eviction released, or the admissions below " +
              "would simply succeed and the refusal under test would never happen")
        }
        record
      }
      try {
        fixture.writer.write(squeezing)
      } finally {
        if (reservedBytes > 0L) {
          fixture.quota.release(reservedBytes)
        }
      }

      assert(fixture.spillManager.memoryBytesSpilled > 0L,
        "eviction must have moved bytes out of memory, or this case is the direct-to-disk " +
          "one and proves nothing about a refusal spilling could not repair")
      assert(fixture.spillManager.durableAdmissionCount > 0L,
        "an admission must have been refused after those rounds, or nothing reported pressure")
      assert(fixture.fallbackPolicy.hasTripped,
        "a reservation refused after eviction had spilled must trip the fallback policy")
      assert(fixture.fallbackPolicy.trippedReason
          .contains(StreamingShuffleFallbackReason.MemoryPressure),
        s"and it must trip as memory pressure, but it tripped as " +
          s"${fixture.fallbackPolicy.trippedReason}")
      assert(fixture.gateway.declaredFallbacks.contains(
          StreamingShuffleFallbackReason.MemoryPressure),
        s"the trip must be declared shuffle-wide so every participant stands down, but the " +
          s"gateway saw ${fixture.gateway.declaredFallbacks.mkString(", ")}")
      // Degradation, not failure: the attempt finishes its own map output through the sort-based
      // writer, which is what makes "zero regression for a memory-bound workload" a completed job
      // rather than a retry budget.
      assert(delegate.writeCallCount === 1,
        s"the attempt must be finished by exactly one sort-based write, but the delegate was " +
          s"written to ${delegate.writeCallCount} time(s)")
      assert(delegate.recordsWritten.groupBy(identity).map(entry => (entry._1, entry._2.size)) ===
          records.groupBy(identity).map(entry => (entry._1, entry._2.size)),
        "the sort-based writer must receive this map task's input exactly, every duplicate " +
          "preserved: a record lost or repeated here is a data loss no reduce task could detect")
      val status = fixture.writer.stop(success = true)
      assert(status.isDefined,
        "a degraded attempt must still report a map status for the write path's dereference")
    }
  }

  test("a runtime stand down finishes the map output through sort rather than failing the task") {
    // The property under test is the one graceful degradation is specified to have and which a
    // failure cannot have: the attempt completes. Two of the four trip conditions -- a consumer
    // sustained at half the producer's rate, and link saturation -- are runtime observations that
    // cannot be settled before the first record, so the only way they can terminate in delegation
    // rather than in a job failure is for the running attempt to rewrite its own output. Nothing
    // here is retried, and nothing here relies on a retry budget existing.
    val delegate = new RecordingSortShuffleWriter(DegradationPartitions)
    val harness = newHarness(
      numPartitions = DegradationPartitions,
      executorMemoryBytes = DegradationExecutorMemoryBytes,
      sortWriterFactory = Some(() => delegate))
    withHarness(harness) { fixture =>
      val records = deterministicRecords(DegradationRecords, seed = 71L, keySpace = 64)
      // The trip is applied from inside the iterator, after the first record has been consumed, so
      // that it is observed while production is under way rather than before it starts. Link
      // saturation is used because it needs no clock: a utilisation above the trip percentage is
      // enough on its own.
      var tripped = false
      val tripping = records.iterator.map { record =>
        if (!tripped) {
          tripped = true
          fixture.fallbackPolicy.recordLinkUtilization(
            usedBytesPerSecond = 990.0d, capacityBytesPerSecond = 1000.0d)
          assert(fixture.fallbackPolicy.hasTripped,
            "the link saturation condition must trip the policy the writer consults")
        }
        record
      }

      fixture.writer.write(tripping)

      assert(delegate.writeCallCount === 1,
        s"the attempt must have been finished by exactly one sort-based write, but the delegate " +
          s"was written to ${delegate.writeCallCount} time(s); zero means the stand-down was " +
          "never observed mid-production and this case proved nothing")
      assert(delegate.recordsWritten.size === records.size,
        s"the sort-based writer must receive every record of this map task's input, but received " +
          s"${delegate.recordsWritten.size} of ${records.size}")
      assert(delegate.recordsWritten.groupBy(identity).map(entry => (entry._1, entry._2.size)) ===
          records.groupBy(identity).map(entry => (entry._1, entry._2.size)),
        "the reconstructed records must be the input exactly, with every duplicate preserved: a " +
          "record lost or repeated here is a data loss no reduce task could detect")
      assert(fixture.gateway.declaredFallbacks.contains(
          StreamingShuffleFallbackReason.NetworkSaturation),
        s"the trip must have been declared shuffle-wide so every other participant stands down " +
          s"too, but the gateway saw ${fixture.gateway.declaredFallbacks.mkString(", ")}")
      assert(fixture.gateway.invalidations.contains(
          StreamingShuffleInvalidationReason.IncompleteStream),
        s"the streamed generation must be withdrawn before it is rewritten, so no consumer is " +
          s"offered output being replaced, but the gateway saw " +
          s"${fixture.gateway.invalidations.mkString(", ")}")
      assert(!fixture.serverHandler.servesRetainedOutput,
        "the withdrawn generation must serve nothing further")

      // The writer contract is answered by the delegate from this point on, which is what makes the
      // delegation total rather than partial.
      val status = fixture.writer.stop(success = true)
      assert(status.isDefined, "a degraded attempt must still report a map status, so that the " +
        "unmodified shuffle write path's unconditional dereference succeeds")
      assert(status.get.location === BlockManagerId("sort-delegate", "delegate-host", 7077),
        s"the status must be the delegate's own, because the delegate wrote the output; got " +
          s"${status.get.location}")
      assert(fixture.writer.getPartitionLengths() eq delegate.getPartitionLengths(),
        "the partition lengths must be the delegate's, because they describe the bytes that were " +
          "actually written")
      assert(fixture.errorNotifier.fetchFailure.isEmpty,
        "a producer that degraded must not have latched a fetch failure: a fetch failure is the " +
          "reading side's signal")

      // No double accounting. The streamed half's bytes and records are never published, so the
      // delegate's own accounting is the only accounting this attempt contributes -- otherwise a
      // degraded task would report roughly twice the bytes it actually wrote.
      assert(fixture.metrics.bytesWritten === 0L,
        s"a degraded attempt must publish none of the bytes it streamed, but published " +
          s"${fixture.metrics.bytesWritten}; the sort delegate accounts for the output that exists")
      assert(fixture.metrics.recordsWritten === 0L,
        s"a degraded attempt must publish none of the records it streamed, but published " +
          s"${fixture.metrics.recordsWritten}")
    }
  }

  test("a runtime stand down releases every buffer and spill file it had retained") {
    // Degradation is the one path that both retains streamed blocks and then abandons them, so the
    // release is asserted separately from the record-level correctness above. Nothing may survive
    // it: the bytes have been rewritten by the delegate and can never be asked for again.
    val delegate = new RecordingSortShuffleWriter(DegradationPartitions)
    val harness = newHarness(
      numPartitions = DegradationPartitions,
      executorMemoryBytes = DegradationExecutorMemoryBytes,
      sortWriterFactory = Some(() => delegate))
    withHarness(harness) { fixture =>
      val records = deterministicRecords(DegradationRecords, seed = 72L, keySpace = 64)
      var tripped = false
      fixture.writer.write(records.iterator.map { record =>
        if (!tripped) {
          tripped = true
          fixture.fallbackPolicy.recordLinkUtilization(990.0d, 1000.0d)
        }
        record
      })
      assert(delegate.writeCallCount === 1, "the case requires that degradation actually happened")

      assert(fixture.spillManager.bufferedBytes === 0L,
        s"a degraded attempt must hold no buffered bytes, but held " +
          s"${fixture.spillManager.bufferedBytes}")
      assert(fixture.spillManager.retainedBlockCount(0) === 0,
        "a degraded attempt must retain no block of any partition")
      fixture.spillFiles().foreach { file =>
        assert(!file.exists(),
          s"the spill file ${file.getName} must have been deleted when the attempt degraded, " +
            "because nothing can ask for those bytes again")
      }
      assert(fixture.routes.withdrawals.nonEmpty,
        "the routing entry of the withdrawn generation must have been retired on this executor")
    }
  }

  test("a consumer that stops acknowledging is replayed five times and then escalated") {
    // The first half of the consumer-failure flow, driven end to end through the real writer and
    // the real egress path rather than through the ledger APIs the two of them use. The ledger
    // assertions establish that a window can be measured and that a range can be tested against
    // it; they cannot establish that the writer notices a stalled consumer, that it spills the
    // window, that it retransmits, that it waits the contracted backoff between attempts, that it
    // makes exactly five of them, or that it fails the task afterwards -- and every one of those is
    // a decision the writer alone makes.
    //
    // Determinism comes from the clock being driven by record consumption. The iterator handed to
    // the writer advances the injected clock and drains the consumer's channel as it is read, so
    // the whole interleaving -- production, pacing, stall detection, spill, replay, escalation --
    // is a function of how far the writer has got, with nothing sleeping and nothing racing.
    //
    // One partition, and deliberately: the attempt budget and the stall report are per stream, so a
    // second partition would contribute attempts of its own and turn an exact assertion about five
    // into an inexact one about "at least five", which is precisely the imprecision that let a
    // replay ladder go unverified in the first place.
    val clock = newManualClock()
    val startedAtMillis = clock.getTimeMillis()
    // A deliberately small executor-memory figure, because the framing capacity is derived from it
    // and the capacity decides when a block is cut. At the default figure one block holds more than
    // this test produces in total, so nothing would be committed until the writer had finished its
    // iterator -- and a consumer cannot stall on output that has not been sent yet. A small budget
    // cuts blocks of a few kilobytes instead, so the stall detector, the spill, the replay ladder
    // and the escalation are all reached inside the record loop, where maintenance runs.
    withHarness(newHarness(numPartitions = 1, clock = clock,
        executorMemoryBytes = 96L * 1024L)) { harness =>
      val consumer = new ProducerConsumerChannel(harness.serverHandler, consumerId)
      consumer.activate()
      consumer.subscribe(harness.shuffleId, defaultMapId, 0)
      assert(harness.writer.blockPayloadCapacityBytes < 32 * 1024,
        "The fixture must cut small blocks for this drive, but frames " +
          s"${harness.writer.blockPayloadCapacityBytes} byte blocks")

      var ticks = 0
      def tick(): Unit = {
        ticks += 1
        clock.advance(500L)
        // Every frame is taken off the channel and none is ever confirmed. Taking them matters:
        // leaving them would let the socket's outbound buffer, rather than the protocol, decide
        // when production stops. Never confirming them is what presents this consumer exactly as
        // the specification describes a lost one -- connected, reading, silent.
        consumer.drainBlocks()
      }
      val paced = deterministicRecords(20000, seed = 12L, keySpace = 32).iterator.zipWithIndex
        .map { case (record, index) =>
          if (index % 128 == 0) {
            tick()
          }
          record: Product2[Int, Int]
        }

      // Captured rather than merely intercepted, so that a drive which fails to reach the
      // escalation reports the state it did reach. A bare `intercept` would say only that no
      // exception arrived, which is the least informative possible description of a flow with a
      // stall detector, a backoff ladder and an attempt budget in it.
      val escalation = try {
        harness.writer.write(paced)
        fail("The writer must escalate a lost consumer, but the write completed. Drive state: " +
          s"ticks=$ticks, clockAdvanced=${clock.getTimeMillis() - startedAtMillis} ms, " +
          s"blocksStreamed=${harness.writer.blocksStreamed}, " +
          s"blocksWritten=${harness.serverHandler.blocksWrittenToChannel}, " +
          s"stalls=${harness.writer.consumerStallCount}, " +
          s"replays=${harness.writer.replayAttemptCount}, " +
          s"retransmitted=${harness.serverHandler.retransmittedBlockCount}, " +
          s"unacknowledged=${harness.serverHandler.unacknowledgedBlockCount}, " +
          s"spills=${harness.writer.spillsObserved}, " +
          s"stalledNow=${harness.serverHandler.stalledPartitions.mkString("[", ",", "]")}")
      } catch {
        case failure: SparkException => failure
      }
      assert(ticks > 0, "The paced iterator must have driven the clock")
      assert(escalation.getMessage.contains("lost the consumer of partition 0"),
        "The escalation must name the partition whose consumer was lost, but said: " +
          escalation.getMessage)
      assert(escalation.getMessage.contains(s"after $MaxRetryAttempts retransmission attempts"),
        "The escalation must report the exhausted attempt budget, but said: " +
          escalation.getMessage)

      // Detected once, at the ten-second window, and reported once however many times the
      // maintenance pass ran afterwards.
      assert(harness.writer.consumerStallCount === 1L,
        s"Exactly one stall must be reported, but ${harness.writer.consumerStallCount} were")
      // The budget was spent in full and never exceeded. A writer that escalated early or that
      // retried without limit would both show up here.
      assert(harness.writer.replayAttemptCount === MaxRetryAttempts.toLong,
        s"Exactly $MaxRetryAttempts replay attempts must be made, but " +
          s"${harness.writer.replayAttemptCount} were")
      // And they were real retransmissions rather than bookkeeping: bytes went back on the wire.
      assert(harness.serverHandler.retransmittedBlockCount > 0L,
        "A replay must put blocks back on the consumer's channel")
      assert(harness.serverHandler.acknowledgedPosition(0) === -1L,
        "The lost consumer must have no acknowledged position at all")

      // The window was spilled rather than dropped while the consumer was being waited for, which
      // is the middle step of the specified sequence and the reason a lost consumer costs disk
      // rather than a recomputed stage.
      assert(harness.writer.spillsObserved > 0L,
        "The unacknowledged window must be spilled while the consumer is being waited for")
      val files = harness.spillFiles()
      assert(files.nonEmpty && files.forall(_.exists()),
        "The spilled window must be on disk when the escalation is raised")

      // And nothing was lost in the meantime: an escalation fails the task so the scheduler can
      // recompute it, and every block the stream committed is still retained when it does.
      val committed = harness.writer.nextSequenceNumberFor(0)
      assert(committed > 0L, "The stalled stream must have committed blocks")
      assert(harness.spillManager.retainedBlockCount(0) === committed.toInt,
        s"All $committed committed block(s) must still be retained, but " +
          s"${harness.spillManager.retainedBlockCount(0)} are")

      // The producer escalates by failing its own task and never by manufacturing the reader's
      // signal, which belongs to a task that is fetching. That the escalation is not a fetch
      // failure is settled by its type rather than by a further assertion: `SparkException` and
      // `FetchFailedException` are unrelated, so testing for one is a check the compiler rejects as
      // vacuous. What is worth asserting is that nothing constructed one on the side.
      assert(harness.errorNotifier.fetchFailure.isEmpty,
        "A producer must not construct a fetch failure")
      consumer.close()
    }
  }

  test("a reconnecting consumer is served the retained window from spill, not a recomputation") {
    // The second half of the consumer-failure flow, and the half that decides whether a lost
    // consumer costs a stage recomputation: "resume on reconnection, retransmit unacknowledged
    // blocks from spill or memory". Asserting it through the ledger APIs -- that a window is
    // retained and that a range is within it -- establishes only that the bookkeeping is
    // self-consistent. What has to be true is that a consumer which comes back on a new channel is
    // handed the same bytes again, and the only way to see that is to take them off a channel.
    val partitions = 2
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      val first = new ProducerConsumerChannel(harness.serverHandler, consumerId)
      first.activate()
      (0 until partitions).foreach(first.subscribe(harness.shuffleId, defaultMapId, _))
      harness.writer.write(deterministicRecords(2400, seed = 11L, keySpace = 24).iterator)

      // Everything this consumer was sent and never confirmed, taken off the wire so that the
      // comparison after the reconnection is against bytes that genuinely crossed a channel.
      // Drained in rounds rather than once, because pacing and the executor framing budget both
      // bound how much egress a single drain can hand to a channel.
      val delivered = drainAllBlocks(harness.serverHandler, first, 0 until partitions)
      assert(delivered.values.forall(_.nonEmpty),
        "Every subscribed partition must have delivered at least one block")
      delivered.foreach { case (partitionId, blocks) =>
        val committed = harness.writer.nextSequenceNumberFor(partitionId)
        assert(blocks.size.toLong === committed,
          s"Partition $partitionId committed $committed block(s) but ${blocks.size} reached the " +
            "consumer")
      }
      val deliveredPayloads = delivered.flatMap { case (partitionId, blocks) =>
        blocks.map(block => (partitionId, block.sequenceNumber()) -> block.copyPayload())
      }
      assert(harness.serverHandler.unacknowledgedBlockCount === deliveredPayloads.size,
        s"All ${deliveredPayloads.size} delivered block(s) must be unacknowledged, but " +
          s"${harness.serverHandler.unacknowledgedBlockCount} are")

      // The consumer goes away without acknowledging anything. Its bytes must survive: that is the
      // whole difference between a reconnection and a recomputation.
      first.close()
      assert(harness.serverHandler.sessionCount === 0,
        "The lost consumer's session must be dropped when its channel goes")
      delivered.foreach { case (partitionId, blocks) =>
        val retained = harness.spillManager.retainedBlockCount(partitionId)
        assert(retained === blocks.size,
          s"Partition $partitionId must still retain all ${blocks.size} unconfirmed block(s) but " +
            s"retains $retained")
      }

      // Made durable, so the replay below is provably served from disk rather than from a buffer
      // that happened not to have been evicted yet.
      assert(harness.spillManager.spillAllRetained() > 0L,
        "Making the unacknowledged window durable must move bytes to disk")
      val files = harness.spillFiles()
      assert(files.nonEmpty && files.forall(_.exists()),
        "The unacknowledged window must be on disk before the reconnection")
      deliveredPayloads.keys.foreach { case (partitionId, sequence) =>
        assert(harness.spillManager.spilledBlock(partitionId, sequence).isDefined,
          s"Block $sequence of partition $partitionId must be locatable on disk")
      }

      // The reconnection: the same logical consumer, a new channel, resuming from the position it
      // had reached -- which is nothing, because it acknowledged nothing.
      val reconnected = new ProducerConsumerChannel(harness.serverHandler, consumerId)
      reconnected.activate()
      (0 until partitions).foreach(
        reconnected.subscribe(harness.shuffleId, defaultMapId, _, nextPosition = 0L))
      assert(harness.serverHandler.sessionCount === 1,
        "The reconnecting consumer must hold exactly one session")
      assert(harness.serverHandler.resumedSessionCount >= 1L,
        "The producer must record that a known consumer resumed on a new channel")
      // The queue is served over several drains, because pacing and the executor framing budget
      // both apply to a replay exactly as they apply to a first delivery.
      val replayed = drainAllBlocks(harness.serverHandler, reconnected, 0 until partitions)

      // The same bytes, block for block, verified independently on arrival: this is what "zero data
      // loss" means for a consumer that came back, and it is a statement about the wire.
      replayed.foreach { case (partitionId, blocks) =>
        assert(blocks.size === delivered(partitionId).size,
          s"Partition $partitionId must replay all ${delivered(partitionId).size} retained " +
            s"block(s) but replayed ${blocks.size}")
        assert(blocks.map(_.sequenceNumber()).sorted ===
            delivered(partitionId).map(_.sequenceNumber()).sorted,
          s"Partition $partitionId's replay must cover exactly its unacknowledged window")
        blocks.foreach { block =>
          val label = s"block ${block.sequenceNumber()} of partition $partitionId"
          assert(block.verifyChecksum(), s"Replayed $label failed its own checksum")
          val original = deliveredPayloads.get((partitionId, block.sequenceNumber()))
          assert(original.isDefined,
            s"Replayed $label was never delivered the first time")
          assert(java.util.Arrays.equals(original.get, block.copyPayload()),
            s"Replayed $label carried different bytes than its first delivery, so the retained " +
              "window did not survive the reconnection intact")
          assert(block.checksum() === blockChecksum(harness.shuffleId, defaultMapId, partitionId,
              block.sequenceNumber(), block.copyPayload()),
            s"Replayed $label was not stamped for its own stream identity")
        }
      }

      // And the window closes normally once the returning consumer confirms it, which is what frees
      // the producer's memory and unlinks the spill files behind it.
      replayed.foreach { case (partitionId, blocks) =>
        val lastSequence = blocks.map(_.sequenceNumber()).max
        reconnected.acknowledge(harness.shuffleId, defaultMapId, partitionId, lastSequence)
        assert(harness.serverHandler.acknowledgedPosition(partitionId) === lastSequence,
          s"The producer must record the returning consumer's acknowledgement of partition " +
            s"$partitionId at $lastSequence")
      }
      reconnected.close()
    }
  }

  test("the writer never raises a fetch failure and never touches channel auto-read") {
    val writerClass = classOf[StreamingShuffleWriter[Int, Int, Int]]
    val referencedTypes = writerClass.getDeclaredFields.map(_.getType.getName).toSet ++
      writerClass.getDeclaredMethods.flatMap { method =>
        method.getReturnType.getName +: method.getParameterTypes.map(_.getName).toSeq
      }.toSet
    assert(!referencedTypes.contains(classOf[FetchFailedException].getName),
      s"The streaming writer must not mention ${classOf[FetchFailedException].getName}: a fetch " +
        "failure is the reading side's signal, and constructing one on the producer side would " +
        "mark a fetch failure against a task that is not fetching")
    assert(!referencedTypes.exists(_.startsWith("io.netty")),
      s"The streaming writer must not name a Netty type, but named " +
        s"${referencedTypes.filter(_.startsWith("io.netty")).mkString(", ")}; channel level flow " +
        "control belongs to the two channel handlers alone")
    val autoReadMethods = writerClass.getDeclaredMethods
      .map(_.getName)
      .filter(name => name.contains("setAutoRead") || name.contains("autoRead"))
    assert(autoReadMethods.isEmpty,
      s"The streaming writer must not touch channel auto-read, but declares " +
        s"${autoReadMethods.mkString(", ")}; toggling it belongs to the consumer side handler")

    withHarness(newHarness()) { harness =>
      harness.writer.write(deterministicRecords(128, seed = 10L, keySpace = 24).iterator)
      assert(harness.writer.stop(success = true).isDefined, "The stop must succeed")
      assert(harness.errorNotifier.fetchFailure.isEmpty,
        "A streaming producer must never latch a fetch failure")
      assertNoPublishedFailure(harness.errorNotifier, "A complete streaming map task")
    }
  }

  test("flush ordering prefers the original attempt over a speculative one") {
    // The shared task context fixture hardcodes attempt number zero, which is why the flush
    // ordering case builds its context directly instead.
    assert(fakeTaskContext(sc).attemptNumber() === 0,
      "The shared task context fixture reports attempt number zero, so it cannot express a " +
        "speculative attempt")

    withHarness(newHarness(taskAttemptId = 31L, attemptNumber = 0)) { original =>
      withHarness(newHarness(taskAttemptId = 32L, attemptNumber = 2)) { speculative =>
        assert(original.serverHandler.taskPriority ===
            StreamingShuffleServerHandler.DEFAULT_PRIORITY,
          "Before a task attempt registers, egress runs at the default ordering")

        original.writer.write(deterministicRecords(64, seed = 11L, keySpace = 16).iterator)
        speculative.writer.write(deterministicRecords(64, seed = 11L, keySpace = 16).iterator)

        val originalPriority = original.serverHandler.taskPriority
        val speculativePriority = speculative.serverHandler.taskPriority
        assert(originalPriority.attemptNumber === 0,
          "The original attempt must register the attempt number its task context reports")
        assert(speculativePriority.attemptNumber === 2,
          "A retried or speculative attempt must register its own non-zero attempt number")
        assert(!originalPriority.speculative,
          "An attempt whose number is zero is the original and is flushed first")
        assert(speculativePriority.speculative,
          "An attempt whose number is above zero is a retry or a speculative copy")
        assert(originalPriority.taskAttemptId === 31L,
          "Egress ordering must carry the task attempt id the context reports")
        assert(speculativePriority.taskAttemptId === 32L,
          "Two attempts of one map task must be ordered as two distinct generations")
        // This is intra-subsystem flush ordering and nothing else: no operating system or network
        // level quality of service marking is configured anywhere in this repository.
        assert(StreamingShuffleServerHandler.TRANSPORT_MODULE_NAME === TransportModuleName,
          s"Streaming egress must be tuned under its own $TransportModuleName transport module")
      }
    }
  }

  test("flush order puts an original attempt on the wire ahead of a speculative one") {
    // The attributes asserted above are the INPUT to the ordering; this is the ordering itself,
    // observed as the sequence of frames that actually reached a consumer's channel. The production
    // comparator only decides anything when two blocks are queued at once, so the fixture has to
    // arrange that -- and it arranges it the way production does, by emptying the egress token
    // bucket so that pacing refuses and blocks accumulate. Nothing here reaches into Netty's water
    // marks or into the comparator itself; the only thing read is the order frames came out in.
    val partitions = 2
    val speculativePartition = 0
    val originalPartition = 1
    val blocksPerPartition = 3
    val blockBytes = 512
    withHarness(newHarness(numPartitions = partitions, maxBandwidthMBps = Some(1))) { harness =>
      val clock = harness.clock.asInstanceOf[ManualClock]
      val spillManager = harness.spillManager
      spillManager.registerPartitionCount(partitions)
      // Registered directly rather than by running the writer, because a completed write terminates
      // every partition's stream and a terminated stream accepts no further block. This is the same
      // registration the writer performs, made by hand so the test owns what is queued and when.
      assert(harness.blockResolver.registerProducer(
          harness.shuffleId, defaultMapId, defaultTaskAttemptId, spillManager),
        "The retained store must be registered, or the handler has nothing to frame blocks from")

      val consumer = attachConsumer(harness)
      try {
        consumer.subscribe(harness.shuffleId, defaultMapId, speculativePartition)
        consumer.subscribe(harness.shuffleId, defaultMapId, originalPartition)
        assert(harness.serverHandler.subscriptionCount === 2,
          "Both partitions must be subscribed on the one channel, so both compete for one queue")

        // Empty the bucket through the limiter's own public acquisition, which is exactly what the
        // egress path calls: from here on production's pacing refuses every block.
        val availableBefore = harness.rateLimiter.availableTokens
        assert(availableBefore > 0L, "A capped bucket must start with tokens to be emptied of")
        assert(harness.rateLimiter.tryAcquire(availableBefore),
          "Draining the whole burst allowance must be admitted exactly once")
        assert(harness.rateLimiter.availableTokens === 0L,
          "and must leave the bucket empty, which is the condition that makes blocks queue")

        val producingStageId = harness.context.stageId
        val speculativePriority = StreamingShuffleServerHandler.EgressPriority(
          producingStageId, 0, defaultTaskAttemptId + 1L, 2)
        val originalPriority = StreamingShuffleServerHandler.EgressPriority(
          producingStageId, 0, defaultTaskAttemptId, 0)
        assert(speculativePriority.speculative && !originalPriority.speculative,
          "The fixture must pit a speculative attempt against an original one")

        // Enqueued speculative FIRST and interleaved, so neither enqueue order nor partition order
        // can produce the expected result by accident. A partition's own blocks all carry one
        // priority, as they do in production where one handler serves one attempt, which is what
        // keeps a partition's blocks in sequence whichever attempt wins the flush.
        (0 until blocksPerPartition).foreach { index =>
          Seq(speculativePartition -> speculativePriority,
              originalPartition -> originalPriority).foreach { case (partitionId, priority) =>
            val sequence = index.toLong
            assert(spillManager.bufferBlock(partitionId, sequence,
                payloadOfLength(partitionId.toLong * 100L + sequence, blockBytes)),
              s"Block $sequence of partition $partitionId must be admitted to the retained store")
            assert(harness.serverHandler.enqueueBlock(partitionId, blockBytes, priority) ===
                sequence,
              s"The handler must number partition $partitionId's block $index densely from zero")
          }
        }
        assert(consumer.drainOutbound().isEmpty,
          "Not one block may have left while the bucket was empty, or the queue this test is " +
            "about was never populated")
        val queuedNow = harness.serverHandler.pendingBlocksFor(speculativePartition) +
          harness.serverHandler.pendingBlocksFor(originalPartition)
        assert(queuedNow === (2 * blocksPerPartition).toLong,
          s"All ${2 * blocksPerPartition} blocks must be queued at once, because the comparator " +
            s"decides nothing about a queue that never holds two blocks, yet $queuedNow were")

        // Refill and let production flush. One pass, so the order below is the order the comparator
        // produced and not an artefact of several passes.
        clock.advance(5000L)
        assert(harness.serverHandler.flushPending() > 0L,
          "A refilled bucket must let the queued blocks leave")
        val order = consumer.drainOutbound().collect {
          case block: DataBlockMessage => (block.partitionId(), block.sequenceNumber())
        }
        val expected =
          (0 until blocksPerPartition).map(index => (originalPartition, index.toLong)) ++
            (0 until blocksPerPartition).map(index => (speculativePartition, index.toLong))
        assert(order === expected,
          s"Every block of the original attempt must reach the wire before any block of the " +
            s"speculative copy, and each partition's blocks in sequence, yet the channel saw " +
            s"$order rather than $expected")
        assert(harness.serverHandler.pendingBlocksFor(speculativePartition) === 0L &&
            harness.serverHandler.pendingBlocksFor(originalPartition) === 0L,
          "and nothing may be left queued afterwards")
      } finally {
        consumer.close()
      }
    }
  }

  test("queued original output drains before queued speculative output") {
    // The behavioural half of the QoS statement. Two priorities are queued against ONE consumer
    // channel that cannot drain while it is closed for writing, so both are still queued when the
    // channel opens again; the order they then leave in is the property under test. Per-partition
    // sequence order must survive it, because a consumer rejects an out-of-order stream.
    val clock = newManualClock()
    withHarness(newHarness(clock = clock, maxBandwidthMBps = Some(1))) { harness =>
      // The retained output has to be registered before a block may be offered for egress: the
      // handler refuses to send bytes no store admitted, which is what stops a handler shared
      // between two producers from streaming output that is not its own. The writer does this in
      // its own preparation; this case registers it directly because it drives the queue rather
      // than the record loop.
      harness.spillManager.registerPartitionCount(defaultPartitions)
      assert(harness.blockResolver.registerProducer(harness.shuffleId, defaultMapId,
          defaultTaskAttemptId, harness.spillManager),
        "the fixture must be able to publish its retained output before offering a block")
      val consumer = new ConsumerChannel(harness.serverHandler, consumerId)
      consumer.subscribe(partitionId = 0, timestampMs = clock.getTimeMillis())
      consumer.subscribe(partitionId = 1, timestampMs = clock.getTimeMillis())
      consumer.drainInbound()

      // Exhausting the egress bucket is what holds the queue: a paced producer queues a block it
      // cannot yet send, so both priorities are still queued when the bucket refills below. Pacing
      // rather than channel writability, because an embedded channel accepts every write at once.
      val available = harness.rateLimiter.availableTokens
      assert(available > 0L, "a capped bucket must start with a burst allowance to drain")
      assert(harness.rateLimiter.tryAcquire(available),
        "draining the whole burst allowance in one acquisition must succeed")
      assert(!harness.rateLimiter.tryAcquire(1L),
        "the bucket must refuse once its allowance is spent, which is what holds the queue")

      val original = StreamingShuffleServerHandler.EgressPriority(
        stageId = 0, stageAttemptNumber = 0, taskAttemptId = 41L, attemptNumber = 0)
      val speculative = StreamingShuffleServerHandler.EgressPriority(
        stageId = 0, stageAttemptNumber = 0, taskAttemptId = 42L, attemptNumber = 2)
      assert(!original.speculative, "attempt zero is the original attempt")
      assert(speculative.speculative, "a non-zero attempt number is a retry or a speculative copy")

      // The speculative block is queued FIRST, so a queue that merely preserved arrival order would
      // fail this case; only a priority ordering can put the original ahead of it.
      val speculativeSequence = admitAndEnqueue(harness, partitionId = 1, priority = speculative)
      val originalSequence = admitAndEnqueue(harness, partitionId = 0, priority = original)
      val secondOriginal = admitAndEnqueue(harness, partitionId = 0, priority = original)

      assert(consumer.drainInboundBlocks().isEmpty,
        "nothing may reach the consumer while the bucket is empty, otherwise the ordering below " +
          "would be an ordering of writes that had already happened")
      clock.advance(1000L)
      harness.serverHandler.flushPending()
      val drained = consumer.drainInboundBlocks()
      assert(drained.size === 3,
        s"all three queued blocks must drain once the channel is writable again, but " +
          s"${drained.size} did")
      val partitions = drained.map(_.partitionId())
      assert(partitions.take(2) === Seq(0, 0),
        s"the original attempt's blocks must drain before the speculative attempt's, but the " +
          s"order was ${partitions.mkString(", ")}")
      assert(partitions.last === 1,
        s"the speculative attempt's block must drain last, but the order was " +
          s"${partitions.mkString(", ")}")
      val originalOrder = drained.filter(_.partitionId() == 0).map(_.sequenceNumber())
      assert(originalOrder === Seq(originalSequence, secondOriginal),
        s"per-partition sequence order must survive the priority ordering, but partition 0 " +
          s"drained as ${originalOrder.mkString(", ")} rather than " +
          s"${Seq(originalSequence, secondOriginal).mkString(", ")}")
      assert(drained.map(_.sequenceNumber()).contains(speculativeSequence),
        "the speculative attempt's block must still be delivered, only later: priority orders " +
          "egress, it never drops it")
      consumer.crash()
    }
  }

  test("a partial memory grant is surfaced as memory pressure") {
    val executorMemory = 2L * 1024L * 1024L
    withHarness(newHarness(numPartitions = 2, executorMemoryBytes = executorMemory,
        bufferSizePercent = 1)) { harness =>
      val spillManager = harness.spillManager
      spillManager.registerPartitionCount(2)
      assert(!spillManager.memoryPressureDetected,
        "No pressure may be reported before a reservation has been refused")
      // acquireMemory may grant less than it was asked for, so a reservation the allowance cannot
      // satisfy is refused outright rather than partially honoured and silently tolerated.
      val beyondAllowance = spillManager.totalBudgetBytes + 1L
      assert(!spillManager.reserveScratch(beyondAllowance),
        s"A reservation of $beyondAllowance bytes exceeds the " +
          s"${spillManager.totalBudgetBytes} byte allowance and must be refused")
      assert(spillManager.memoryPressureDetected,
        "A refused reservation must raise the memory-pressure signal the fallback policy consumes")
      assert(spillManager.memoryPressureEvents > 0L, "The pressure event must be counted")
      assert(spillManager.scratchBytes === 0L,
        "A refused reservation must leave nothing reserved")
      assert(spillManager.getUsed() === 0L,
        "A refused reservation must leave no task memory acquired")
      spillManager.clearMemoryPressure()
      assert(!spillManager.memoryPressureDetected, "The pressure signal must be clearable")

      // Escalation: a short grant is trip condition two, and it stands the shuffle down rather than
      // being tolerated in silence.
      val fallbackPolicy = harness.fallbackPolicy
      assert(!fallbackPolicy.hasTripped, "The policy must not have tripped before it is told")
      fallbackPolicy.recordAllocationGrant(beyondAllowance, 0L)
      assert(fallbackPolicy.hasTripped, "A short allocation grant must trip the fallback policy")
      assert(fallbackPolicy.trippedReason
          .contains(StreamingShuffleFallbackReason.MemoryPressure),
        s"A short grant must be recorded as memory pressure, but was " +
          s"${fallbackPolicy.trippedReason}")
      assert(fallbackPolicy.shouldDelegateToSortShuffle,
        "A tripped policy must route the shuffle to the sort-based writer, which is unmodified")
      val untripped = new StreamingShuffleFallbackPolicy(harness.writerConf, harness.clock)
      untripped.recordAllocationGrant(0L, 0L)
      assert(!untripped.hasTripped,
        "A reservation of zero bytes cannot be short granted and must not trip anything")
      assert(untripped.unevaluableSampleCount === 1L,
        "A request for nothing must be counted as un-evaluable rather than ignored")
    }
  }

  test("a pre-flight degradation retires this generation before it uses the sort delegate") {
    // Graceful degradation has to be total. The manager publishes two owners of a producer
    // generation before the writer exists -- a routing entry on this executor and an address on the
    // driver -- so an attempt that then decides not to stream must retire them before it hands its
    // records to the sort-based writer. Leaving them in place would leave one orphan per delegated
    // map output, each of them an address a consumer can resolve, connect to, and then wait on
    // until its own five-second detector fires; and because a delegated attempt's stop belongs
    // entirely to the delegate, no later lifecycle hook of this writer would ever clean them up.
    val delegate = new RecordingStopSortWriter
    withHarness(newHarness(delegate = Some(delegate))) { harness =>
      val writer = harness.writer
      // Tripping the policy is the cheapest of the four pre-flight refusals to arrange and the one
      // that does not depend on the buffer arithmetic: a short allocation grant is trip condition
      // two, and prepare reads the latch before it reads anything else about memory.
      harness.fallbackPolicy.recordAllocationGrant(1024L, 0L)
      assert(harness.fallbackPolicy.hasTripped,
        "the fixture must have tripped the policy, or the writer would simply stream")
      assert(!harness.spillManager.isClosed, "and the attempt must still hold its buffer state")

      writer.write(Iterator((1, 10), (2, 20), (3, 30)))

      // The iterator was handed on intact, which is what makes the degradation loss-free.
      assert(delegate.writtenRecords === 3,
        s"every record must reach the sort-based writer, but it received " +
          s"${delegate.writtenRecords}")
      assert(harness.gateway.declaredFallbacks.nonEmpty,
        "and the shuffle must have stood streaming down for every participant first, because one " +
          "sort-written map output among streaming siblings is unreadable")

      // The retirement. Each of the three owners is asserted separately, because each was published
      // by a different component and a partial withdrawal is exactly the defect being pinned.
      assert(harness.routes.withdrawals === Seq((harness.shuffleId, defaultMapId)),
        s"this executor's routing entry must have been withdrawn exactly once, but the registry " +
          s"recorded ${harness.routes.withdrawals}")
      assert(harness.gateway.invalidations ===
          Seq(StreamingShuffleInvalidationReason.IncompleteStream),
        s"the driver must have been told to drop this producer's address, but it recorded " +
          s"${harness.gateway.invalidations}")
      assert(!harness.serverHandler.withdrawGeneration("a second withdrawal must find nothing"),
        "the single cross-owner withdrawal must already have run, so a second call finds it done")
      assert(!harness.serverHandler.servesRetainedOutput,
        "and the handler must serve no retained output, because none was ever published")
      assert(harness.spillManager.isClosed,
        "the buffer and spill state must be released too, which is also what removes this task " +
          "from the executor's hundred-millisecond threshold poller")
      assert(harness.spillFiles().isEmpty, "and no spill file may survive a degraded attempt")

      // The stop protocol belongs to the delegate from here on, and there is nothing of this
      // writer's left for it to release.
      assert(writer.stop(success = true).isEmpty,
        "a delegated stop is answered by the delegate, whose status this fixture does not mint")
      assert(delegate.stopCalls === Seq(true),
        s"and the delegate must have been the one stopped, but it recorded ${delegate.stopCalls}")
      assert(writer.getPartitionLengths() === delegate.getPartitionLengths(),
        "the partition lengths must likewise come from the writer that produced the output")
      assert(harness.routes.withdrawals.size === 1,
        "and the stop must not repeat a withdrawal the degradation already performed")
    }
  }

  test("a block the allowance cannot admit goes to disk and nothing stands the shuffle down") {
    // The cross-component contract behind "spill rather than fail". A buffer allowance that has
    // been MET is answered by retaining the block on local disk, and no part of the subsystem may
    // treat that as a reason to stop streaming: the fallback verdict is shuffle-wide, so declaring
    // one here would tell consumers to read output this producer has already streamed from a
    // sort-based path that has none of it. This case pins all three components at once -- the
    // writer keeps streaming, the spill manager retains the block durably, and the backpressure
    // protocol stays out of its Degraded state -- because the defect this replaces was precisely a
    // disagreement between them: the writer reported a fallback-triggering allocation failure and
    // then carried on regardless.
    //
    // The budget is sized so that the framing reservation fits and almost nothing is left over:
    // two partitions of a 4% allowance over a one-megabyte executor. The remainder is then
    // reserved directly from the shared allowance, which is what makes the refusal unevictable --
    // those bytes belong to no partition of this producer, exactly as another task's framing
    // scratch would not.
    val executorMemory = 1024L * 1024L
    withHarness(newHarness(numPartitions = 2, executorMemoryBytes = executorMemory,
        bufferSizePercent = 4)) { harness =>
      val spillManager = harness.spillManager
      val backpressure = harness.backpressure
      val recordBound = 400000
      var reservedBytes = 0L
      var offered = 0
      // Records are offered until the durable path has been taken, so the case is bounded by the
      // behaviour it is asserting rather than by a record count that a change to the framing
      // arithmetic would silently invalidate. Values are byte-swapped indices rather than the
      // indices themselves, so the serialized stream compresses poorly and a block boundary is
      // reached in thousands of records instead of millions.
      val records = new Iterator[Product2[Int, Int]] {
        override def hasNext: Boolean =
          offered < recordBound && spillManager.durableAdmissionCount == 0L

        override def next(): Product2[Int, Int] = {
          if (offered == 0) {
            // Taken after the writer's own framing reservation, which is made before the first
            // record is consumed, so the producer is fully committed to streaming by the time the
            // allowance runs out. Reserving earlier would be the pre-flight memory-pressure
            // condition instead, which correctly degrades the whole attempt.
            reservedBytes = harness.reserveRemainingAllowance()
            assert(reservedBytes > 0L,
              "the fixture must be able to take what the framing reservation left, or the " +
                "admission below would simply succeed; the buffer arithmetic has changed")
          }
          val record = (offered, scala.util.hashing.byteswap32(offered))
          offered += 1
          record
        }
      }
      try {
        harness.writer.write(records)
      } finally {
        if (reservedBytes > 0L) {
          harness.quota.release(reservedBytes)
        }
      }

      assert(spillManager.durableAdmissionCount > 0L,
        s"A block that the allowance cannot admit must be written straight to local disk, but " +
          s"after $offered records none was; buffered ${spillManager.bufferedBytes} of " +
          s"${spillManager.totalBudgetBytes} budgeted bytes with " +
          s"${spillManager.executorReservedBytes} reserved across the executor")
      assert(spillManager.allSpilledBlocks.nonEmpty,
        "and the retained block must be reachable on disk, because a consumer reads it exactly " +
          "as it reads an evicted one")
      assert(harness.writer.getPartitionLengths().sum > 0L,
        "the map output must be complete: a durably retained block is streamed output like any " +
          "other and is counted in the partition lengths")

      // The signal the writer used. It counts the pressure and reports it, and it does NOT latch a
      // degradation reason, which is the whole distinction being pinned here.
      assert(backpressure.durableSpillAdmissionCount === spillManager.durableAdmissionCount,
        s"every durable admission must be reported to the protocol exactly once, but it saw " +
          s"${backpressure.durableSpillAdmissionCount} and the store made " +
          s"${spillManager.durableAdmissionCount}")
      assert(!backpressure.isDegraded,
        "a met buffer allowance must not degrade the subsystem, but it latched " +
          s"${backpressure.degradationReasons.mkString(", ")}")
      assert(backpressure.degradationReasons.isEmpty,
        "no degradation reason at all, because spilling is the specified answer to a full buffer")
      assert(backpressure.state != BackpressureState.Degraded,
        s"and the executor's flow-control state must not be Degraded, but it was " +
          s"${backpressure.state.name}")

      // And nothing anywhere decided the shuffle should stop streaming.
      assert(!harness.fallbackPolicy.hasTripped,
        s"the fallback policy must not trip on a met allowance, but it tripped with " +
          s"${harness.fallbackPolicy.trippedReason}")
      assert(!harness.fallbackPolicy.shouldDelegateToSortShuffle,
        "so the shuffle must still be routed to the streaming path")
      assert(!harness.fallbackPolicy.shuffleHasFallenBack(harness.shuffleId),
        "and no shuffle-wide verdict may have been recorded locally")
      assert(harness.gateway.declaredFallbacks.isEmpty,
        s"nor announced to the coordinator, but it announced " +
          s"${harness.gateway.declaredFallbacks.mkString(", ")}")
      assert(harness.routes.withdrawals.isEmpty,
        "and this generation must still be routed, because it is still streaming")
    }
  }

  test("a hostile consumer channel cannot forge progress, rename a peer or fan a partition out") {
    // The producer's inbound surface is reachable by anything that can open a socket to this
    // executor, so every frame arriving on it is untrusted. Asserting that the handler's counters
    // move when its own methods are called establishes nothing about that surface; what has to be
    // established is that hostile *frames* -- an acknowledgement of bytes never sent, an
    // acknowledgement on a partition the channel never subscribed to, a second channel claiming a
    // live peer's identity, and a crowd of channels subscribing to one partition -- are refused
    // before they can advance a cursor, retire retained bytes or cost this executor per-stream
    // state. Each is delivered as encoded bytes through the transport entry point.
    val partitions = 2
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      val handler = harness.serverHandler
      val honest = new ProducerConsumerChannel(handler, consumerId)
      honest.activate()
      honest.subscribe(harness.shuffleId, defaultMapId, 0)
      harness.writer.write(deterministicRecords(2400, seed = 14L, keySpace = 24).iterator)
      val delivered = drainAllBlocks(handler, honest, Seq(0))(0)
      assert(delivered.nonEmpty, "The honest consumer must have been sent blocks of partition 0")
      val highestSent = delivered.map(_.sequenceNumber()).max

      // 1. An acknowledgement beyond anything the acknowledging channel was sent. The bound is per
      //    session and is what a forged confirmation runs into first: every send records its
      //    sequence number before the bytes reach the socket, so a position past that record cannot
      //    be a receipt. The refusal is fatal to the forging channel -- a peer acknowledging bytes
      //    that were never sent is not a peer to go on serving -- and it must leave the honest
      //    consumer's cursor and the retained window it pins exactly as they were.
      val forger = new ProducerConsumerChannel(handler, s"$consumerId-forger")
      forger.activate()
      forger.subscribe(harness.shuffleId, defaultMapId, 0)
      forger.drainBlocks()
      val refusedAcksBefore = handler.refusedAckCount
      val retainedBefore = harness.spillManager.retainedBlockCount(0)
      val sessionsBeforeForgery = handler.sessionCount
      forger.acknowledge(harness.shuffleId, defaultMapId, 0, highestSent + 1000L)
      assert(handler.refusedAckCount === refusedAcksBefore + 1L,
        "An acknowledgement of bytes that were never sent must be refused")
      assert(handler.sessionCount === sessionsBeforeForgery - 1,
        "The forging channel's session must be closed rather than gone on serving")
      assert(handler.claimedSessionSlots === handler.sessionCount,
        "A session closed for forgery must return its slot")
      assert(handler.acknowledgedPosition(0) === -1L,
        "A refused acknowledgement must not advance the recorded position")
      assert(harness.spillManager.retainedBlockCount(0) === retainedBefore,
        "A refused acknowledgement must not retire a single retained block")
      // Escalated to the producing task as a typed condition rather than merely counted, because a
      // peer sending impossible positions is a protocol violation and not a pacing outcome.
      assert(harness.errorNotifier.error.exists(
          _.getMessage.contains("STREAMING_SHUFFLE_INVALID_SEQUENCE_NUMBER")),
        "A forged acknowledgement must be reported as an invalid sequence number, but the " +
          s"notifier holds ${harness.errorNotifier.error.map(_.getMessage)}")
      forger.close()

      // 2. An acknowledgement on a partition the acknowledging channel never subscribed to:
      //    misaddressed as well as refused, because it names a stream that session has no standing
      //    on. Not fatal to the channel -- a frame for the wrong stream is a mistake rather than an
      //    impossibility -- so it is counted and dropped.
      val stray = new ProducerConsumerChannel(handler, s"$consumerId-stray")
      stray.activate()
      stray.subscribe(harness.shuffleId, defaultMapId, 1)
      stray.drainBlocks()
      val misaddressedBefore = handler.misaddressedMessageCount
      val refusedBeforeStray = handler.refusedAckCount
      stray.acknowledge(harness.shuffleId, defaultMapId, 0, 0L)
      assert(handler.refusedAckCount === refusedBeforeStray + 1L,
        "An acknowledgement on an unsubscribed partition must be refused")
      assert(handler.misaddressedMessageCount === misaddressedBefore + 1L,
        "An acknowledgement on an unsubscribed partition must be recorded as misaddressed")
      assert(handler.acknowledgedPosition(0) === -1L,
        "A misaddressed acknowledgement must not advance any position")
      stray.close()
      assert(handler.subscriberCount(1) === 0,
        "The stray channel's subscriber slot must be returned when it goes")

      // 3. A channel that tries to rename itself mid-flight. A session that could be renamed could
      //    be talked into adopting the cursors -- and therefore the unread output -- of a consumer
      //    that is not the peer holding the socket, so the second, different declaration is refused
      //    and the channel closed.
      val renamer = new ProducerConsumerChannel(handler, s"$consumerId-renamer")
      renamer.activate()
      renamer.subscribe(harness.shuffleId, defaultMapId, 1)
      assert(handler.subscriberCount(1) === 1,
        "The renaming channel must hold a subscription before it tries to rename itself")
      // Whatever its legitimate subscription earned it, taken off the channel first, so that the
      // assertion after the refusal is about frames the refusal produced and not about frames the
      // subscription it was entitled to had already delivered.
      renamer.drainBlocks()
      val conflictsBefore = handler.identityConflictCount
      val sessionsBefore = handler.sessionCount
      renamer.deliver(new HeartbeatMessage(harness.shuffleId, defaultMapId, 1, 0L,
        ManualClockEpochMillis, s"$consumerId-someone-else"))
      assert(handler.identityConflictCount === conflictsBefore + 1L,
        "A channel declaring a second, different identity must be recorded as a conflict")
      assert(handler.sessionCount === sessionsBefore - 1,
        "A channel that tried to rename itself must have had its session closed")
      assert(handler.subscriberCount(1) === 0,
        s"The renamed channel's subscriber slot must be returned, but " +
          s"${handler.subscriberCount(1)} remain claimed")
      assert(renamer.drainBlocks().isEmpty,
        "A channel refused an identity change must not be served another block")
      renamer.close()

      // 4. Fan-out: a partition is legitimately read by one reduce task, so the number of channels
      //    that may subscribe to it is bounded. The bound is enforced before a ledger, a queue
      //    entry or a payload copy exists, so a crowd costs this executor nothing per member.
      val refusalsBefore = handler.subscriptionRefusalCount
      val crowd = (0 until StreamingShuffleServerHandler.MAX_SUBSCRIBERS_PER_PARTITION + 4).map {
        index =>
          val member = new ProducerConsumerChannel(handler, s"$consumerId-crowd-$index")
          member.activate()
          member.subscribe(harness.shuffleId, defaultMapId, 1)
          member
      }
      assert(handler.subscriberCount(1) === StreamingShuffleServerHandler
          .MAX_SUBSCRIBERS_PER_PARTITION,
        "Partition 1 must admit exactly " +
          s"${StreamingShuffleServerHandler.MAX_SUBSCRIBERS_PER_PARTITION} subscriber(s), but " +
          s"admitted ${handler.subscriberCount(1)}")
      assert(handler.subscriptionRefusalCount >= refusalsBefore + 4L,
        "Every subscription past the per-partition ceiling must be refused and counted")
      // The ceiling bounds subscriptions, not connections: each crowd member still holds one
      // session, and the slot ledger must agree with the registry exactly. A divergence between the
      // two is the observable signature of a teardown that forgot to return a slot.
      assert(handler.claimedSessionSlots === handler.sessionCount,
        s"${handler.claimedSessionSlots} session slot(s) are claimed for ${handler.sessionCount} " +
          "live session(s)")
      crowd.foreach(_.close())
      assert(handler.subscriberCount(1) === 0,
        "Every subscriber slot of partition 1 must be returned when its channel goes, but " +
          s"${handler.subscriberCount(1)} remain claimed")
      assert(handler.claimedSessionSlots === handler.sessionCount,
        "Every session slot must be returned when its channel goes")

      // 5. A version the producer cannot speak. Detected by peeking the header rather than by
      //    failing to parse the body, so the mismatch is attributed to the version -- and
      //    published, because a version mismatch is one of the four conditions under which
      //    streaming stands down in favour of the sort-based implementation.
      assert(!handler.versionMismatchDetected,
        "No version mismatch may have been seen before one is delivered")
      val alien = new ProducerConsumerChannel(handler, s"$consumerId-alien")
      // A channel arriving allocates nothing by design -- a peer that has named no partition has
      // asked for nothing -- so this baseline is what the refused frame must leave untouched.
      alien.activate()
      val sessionsBeforeAlien = handler.sessionCount
      val slotsBeforeAlien = handler.claimedSessionSlots
      val framed = new HeartbeatMessage(harness.shuffleId, defaultMapId, 0, 0L,
        ManualClockEpochMillis, s"$consumerId-alien").toByteBuffer()
      val alienBytes = new Array[Byte](framed.remaining())
      framed.duplicate().get(alienBytes)
      // The protocol version is the first byte of the header, immediately after the frame type.
      alienBytes(FrameTypePrefixLength) = (ProtocolVersion + 7).toByte
      alien.deliverRaw(alienBytes)
      assert(handler.versionMismatchDetected,
        "A frame carrying an unsupported protocol version must be detected as a mismatch")
      assert(handler.sessionCount === sessionsBeforeAlien,
        "A frame refused on its version must not allocate a session")
      assert(handler.claimedSessionSlots === slotsBeforeAlien,
        "A frame refused on its version must not claim a session slot")
      assert(alien.drainBlocks().isEmpty,
        "A channel whose frame was refused on its version must not be served a block")
      alien.close()

      // The honest consumer is unharmed by all of it: its position is still its own to advance, and
      // advancing it still works.
      honest.acknowledge(harness.shuffleId, defaultMapId, 0, highestSent)
      assert(handler.acknowledgedPosition(0) === highestSent,
        s"The honest consumer must still be able to acknowledge through $highestSent")

      // 6. Last, the boundary the reconnection path depends on: a *different* channel declaring a
      //    live peer's identity is that peer coming back, not a second tenant. One reduce task
      //    attempt reads through one connection at a time, so the older session is superseded and
      //    released -- which is what stops two channels from both holding one consumer's credit and
      //    both pinning its retained window.
      val supersededBefore = handler.supersededSessionCount
      val returning = new ProducerConsumerChannel(handler, consumerId)
      returning.activate()
      returning.subscribe(harness.shuffleId, defaultMapId, 0, nextPosition = highestSent + 1L)
      assert(handler.supersededSessionCount === supersededBefore + 1L,
        "A known consumer reconnecting on a new channel must supersede its previous session")
      assert(handler.subscriberCount(0) === 1,
        "One consumer identity may hold exactly one subscription to a partition, but " +
          s"${handler.subscriberCount(0)} are claimed")
      assert(handler.claimedSessionSlots === handler.sessionCount,
        "A superseded session must return its slot")
      returning.close()
      honest.close()
    }
  }

  test("the executor-wide framing budget bounds transient egress copies and refuses the excess") {
    // The ceiling on consumers is per map output while the budget is per executor, and per-channel
    // bounds multiply: thousands of sessions each holding one high-water mark's worth of transient
    // framing copies is an aggregate no per-channel limit constrains. The budget is the thing that
    // constrains it, so its arithmetic is asserted directly -- reserving up to the ceiling, being
    // refused past it, and returning what was taken. Driving thirty-two two-megabyte copies through
    // a channel to observe the same three properties would assert them less precisely and cost
    // sixty-four megabytes to do it.
    val budget = StreamingShuffleServerHandler.EgressFramingBudget
    budget.resetForTesting()
    try {
      assert(budget.capacity === 32L * StreamingShuffleServerHandler.MAX_FRAMED_BYTES,
        "The executor framing ceiling must be thirty-two maximal frames, but is " +
          budget.capacity)
      assert(budget.inFlight === 0L, "A reset budget must hold nothing")
      assert(budget.refusalCount === 0L, "A reset budget must have refused nothing")
      // A zero-sized reservation costs nothing and is admitted, so no caller has to special-case
      // it.
      assert(budget.tryReserve(0L), "A zero-byte reservation must be admitted")
      assert(budget.inFlight === 0L, "A zero-byte reservation must not touch the ledger")

      val frame = StreamingShuffleServerHandler.MAX_FRAMED_BYTES.toLong
      (0 until 32).foreach { index =>
        assert(budget.tryReserve(frame),
          s"Reservation ${index + 1} of thirty-two must be admitted within the ceiling")
      }
      assert(budget.inFlight === budget.capacity,
        s"The budget must be full at ${budget.capacity} bytes but holds ${budget.inFlight}")
      assert(!budget.tryReserve(1L),
        "A reservation of one byte past the ceiling must be refused")
      assert(budget.refusalCount === 1L, "A refusal must be counted")
      assert(budget.inFlight === budget.capacity,
        "A refused reservation must leave the ledger exactly as it was")

      // Returning one frame makes room for exactly one more, which is what makes a refusal a delay
      // rather than a loss: the caller holds its block and the next refill admits it.
      budget.release(frame)
      assert(budget.inFlight === budget.capacity - frame,
        "A release must return exactly the bytes it names")
      assert(budget.tryReserve(frame),
        "The room a release made must be available to the next reservation")

      // The release listener is what returns a reservation when the transport is done with the
      // copy, on success, on failure and on cancellation alike -- so a failing consumer cannot
      // retire the executor's allowance one block at a time.
      val channel = new EmbeddedChannel(DefaultChannelId.newInstance())
      try {
        val before = budget.inFlight
        val future = channel.newSucceededFuture()
        budget.releaseOn(frame).operationComplete(future)
        assert(budget.inFlight === before - frame,
          "A completed write must return its framing reservation through the listener")
      } finally {
        channel.finishAndReleaseAll()
      }

      // Floored rather than allowed to go negative: a ledger that could read negative would raise
      // the effective ceiling for every other caller, turning one accounting mistake into an
      // unbounded one.
      budget.release(budget.capacity * 4L)
      assert(budget.inFlight === 0L,
        s"An over-release must floor the ledger at zero but left ${budget.inFlight}")
      assert(budget.tryReserve(budget.capacity),
        "The whole ceiling must be reservable again once everything has been returned")
      assert(!budget.tryReserve(frame), "A full budget must refuse the next frame")
    } finally {
      budget.resetForTesting()
    }
  }

  test("the buffer and spill percentages are range validated and an absent cap means unlimited") {
    Seq(MinBufferSizePercent, DefaultBufferSizePercent, MaxBufferSizePercent).foreach { percent =>
      val accepted = new SparkConf(false).set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, percent)
      assert(accepted.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT) === percent,
        s"A buffer size percent of $percent is inside the specified range and must be accepted")
    }
    Seq(MinBufferSizePercent - 1, MaxBufferSizePercent + 1).foreach { percent =>
      val rejected = new SparkConf(false).set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, percent)
      val failure = intercept[IllegalArgumentException] {
        rejected.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)
      }
      assert(failure.getMessage.contains("The buffer size percent must be in [1, 50]."),
        s"A buffer size percent of $percent must be rejected with the documented requirement, " +
          s"but failed with: ${failure.getMessage}")
    }
    Seq(MinSpillThresholdPercent, DefaultSpillThresholdPercent, MaxSpillThresholdPercent)
      .foreach { percent =>
        val accepted = new SparkConf(false).set(SHUFFLE_STREAMING_SPILL_THRESHOLD, percent)
        assert(accepted.get(SHUFFLE_STREAMING_SPILL_THRESHOLD) === percent,
          s"A spill threshold of $percent is inside the specified range and must be accepted")
      }
    Seq(MinSpillThresholdPercent - 1, MaxSpillThresholdPercent + 1).foreach { percent =>
      val rejected = new SparkConf(false).set(SHUFFLE_STREAMING_SPILL_THRESHOLD, percent)
      val failure = intercept[IllegalArgumentException] {
        rejected.get(SHUFFLE_STREAMING_SPILL_THRESHOLD)
      }
      assert(failure.getMessage.contains("The spill threshold must be in [50, 95]."),
        s"A spill threshold of $percent must be rejected with the documented requirement, but " +
          s"failed with: ${failure.getMessage}")
    }

    // Absence of the bandwidth cap is the unlimited state, emphatically not zero.
    val uncapped = new SparkConf(false)
    assert(uncapped.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS).isEmpty,
      "An unset maximum bandwidth must read back as absent, which is what unlimited means")
    val rejectedCap = new SparkConf(false).set(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, 0)
    val capFailure = intercept[IllegalArgumentException] {
      rejectedCap.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)
    }
    assert(capFailure.getMessage.contains("The maximum bandwidth should be positive."),
      s"A maximum bandwidth of zero must be rejected, but failed with: ${capFailure.getMessage}")

    val clock = newManualClock()
    val unlimited = TokenBucketRateLimiter.unlimited(clock)
    assert(unlimited.isUnlimited, "An absent cap must produce a limiter that enforces no rate")
    assert(unlimited.tryAcquire(MaxEncodedFrameBytes.toLong),
      "An unlimited limiter must admit the largest legal frame without reading the clock")

    assert(BandwidthCeilingPercent === 80L,
      "Streaming holds itself to 80% of the administered link capacity")
    assert(applyBandwidthCeiling(1000L) === 1000L / PercentScale * BandwidthCeilingPercent,
      "The ceiling must divide before multiplying, exactly as the limiter does")
    assert(refillBytesPerSecond(100, 0) === refillBytesPerSecond(100, 1),
      "A concurrent shuffle count of zero or less must be treated as one")
    assert(refillBytesPerSecond(100, 2) < refillBytesPerSecond(100, 1),
      "The administered capacity must be divided across the shuffles the executor is serving")
    assert(TokenBucketRateLimiter.burstCapacityBytes(1L) === MaxEncodedFrameBytes.toLong,
      s"A bucket must be at least one maximum sized frame, i.e. $MaxEncodedFrameBytes bytes, or " +
        "it could neither admit the largest legal frame nor charge for all of it")
    assert(MinTokenBucketCapacityBytes === MaxBlockSizeBytes.toLong,
      "The minimum bucket capacity is stated against the block payload cap")
  }

  test("the direct-to-disk report is bounded per executor, not once per map task") {
    // A block that met the buffer allowance and went straight to disk is a condition an operator
    // has to be told about, and one that recurs for every block once the allowance is met. Bounding
    // the record to the first occurrence OF EACH WRITER is not a bound at all: an executor runs one
    // writer per map task, so a stage of a thousand tasks in this condition produces a thousand
    // default-level records and the log volume becomes a function of the task count -- the volume
    // this feature may not produce. The bound is therefore evaluated on the executor's own
    // aggregation, which is what this case pins.
    MemorySpillManager.resetSharedStateForTesting()
    val aggregation = StreamingShuffleWriter.durableAdmissionLogAggregator
    assert(aggregation.occurrenceCount === 0L,
      "The shared-state reset must leave the executor's aggregation window clean")

    // Two writers of the same stage, standing in as two manual clocks inside one window. The first
    // occurrence reports; the second is aggregated behind it rather than reported again.
    val firstTaskMs = ManualClockEpochMillis
    val secondTaskMs = ManualClockEpochMillis + 1L
    assert(aggregation.record(firstTaskMs, 4096L).isDefined,
      "The first task to meet its allowance must produce the record that tells an operator to look")
    assert(aggregation.record(secondTaskMs, 2048L).isEmpty,
      "A second task inside the same window must be aggregated, not reported; that is precisely " +
        "what a per-task bound could not do")
    assert(aggregation.occurrenceCount === 2L, "Both occurrences must be counted on the executor")
    assert(aggregation.unreportedCount === 1L,
      "The occurrence that stayed silent must be carried for the next admitted record to report")
    assert(aggregation.volumeBytes === 4096L + 2048L,
      s"The executor-wide volume must cover both tasks, but was ${aggregation.volumeBytes}")

    // Past the window an operator hears again, so a sustained condition is bounded rather than
    // silenced, and the admitted record accounts for what it stood in for.
    val laterMs = firstTaskMs + MemorySpillManager.LOG_AGGREGATION_WINDOW_MS
    val summary = aggregation.record(laterMs, 1024L)
    assert(summary.isDefined, "A record past the window must be admitted")
    assert(summary.get.unreported === 1L,
      "The admitted record must state how many occurrences it stands in for")
    assert(summary.get.occurrences === 3L, "It must quote the executor-wide occurrence count")
    assert(aggregation.unreportedCount === 0L,
      "Admitting a record must clear the occurrences it accounted for")

    // Left clean, because the aggregation outlives this test the way it outlives a task.
    MemorySpillManager.resetSharedStateForTesting()
    assert(aggregation.occurrenceCount === 0L && aggregation.unreportedCount === 0L,
      "The one shared-state reset must clear the writer's aggregation as well as the manager's")
  }
}
