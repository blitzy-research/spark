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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.util.control.NonFatal

import _root_.io.netty.channel.{Channel, DefaultChannelId}
import _root_.io.netty.channel.embedded.EmbeddedChannel
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SharedSparkContext, SparkConf, SparkException, SparkFunSuite,
  TaskContextImpl}
import org.apache.spark.internal.config.{SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT,
  SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, SHUFFLE_STREAMING_SPILL_THRESHOLD}
import org.apache.spark.network.client.{TransportClient, TransportResponseHandler}
import org.apache.spark.network.protocol.OneWayMessage
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, DataBlockMessage,
  HeartbeatMessage, RetransmitRequestMessage, StreamingShuffleChecksum, StreamingShuffleMessage,
  StreamingShuffleMessageType, StreamTerminationMessage}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{FetchFailedException, ShuffleWriter}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{Clock, ManualClock}

/** Unit tests for [[StreamingShuffleWriter]], the producer half of the streaming shuffle. */
class StreamingShuffleWriterSuite
  extends SparkFunSuite
    with SharedSparkContext
    with Matchers
    with PrivateMethodTester
    with StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  private val defaultPartitions = 4

  // Fixture for the pipelined-durability cases.
  private val PipelinedDurabilityExecutorMemoryBytes: Long = 1024L * 1024L

  private val PipelinedDurabilityBufferPercent: Int = 50

  private val PipelinedDurabilityRecords: Int = 60000

  /** Returns the process-scoped state to zero on BOTH edges of every case. */
  override def beforeEach(): Unit = {
    super.beforeEach()
    resetStreamingShuffleMetrics()
    MemorySpillManager.resetSharedStateForTesting()
  }

  override def afterEach(): Unit = {
    try {
      super.afterEach()
    } finally {
      resetStreamingShuffleMetrics()
      MemorySpillManager.resetSharedStateForTesting()
    }
  }

  /** Executor memory the default fixture derives its buffer budget from. */
  private val defaultExecutorMemoryBytes = 64L * 1024L * 1024L

  private val defaultMapId = 7L

  private val defaultTaskAttemptId = 21L

  private val capabilityToken = "streaming-shuffle-writer-suite-token"

  private val consumerId = s"attempt-4096-partitions-0-${defaultPartitions - 1}"

  private val authenticatedApplicationId = "application-streaming-shuffle-writer-suite"

  private val declaredEpoch = 1L

  private val protocolShuffleId = 3

  /** A working sort-based delegate that records what it was handed. */
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

    def consumedRecords: Seq[(Int, Int)] = synchronized(consumed.toSeq)

    def stopOutcome: Option[Boolean] = synchronized(stopped)
  }

  /** One consumer channel attached to the producer under test, as the transport attaches one. */
  private class ProducerConsumerChannel(
      val handler: StreamingShuffleServerHandler,
      val consumerId: String,
      val transportPrincipal: String = authenticatedApplicationId) {

    val channel: EmbeddedChannel = new EmbeddedChannel(DefaultChannelId.newInstance())

    val client: TransportClient =
      new TransportClient(channel, new TransportResponseHandler(channel))
    client.setClientId(transportPrincipal)

    private def awaitDataPlane(operation: String): Unit = {
      if (!handler.awaitDataPlaneIdle(10000L)) {
        throw new IllegalStateException(
          s"The producer data plane did not settle after $operation")
      }
    }

    def activate(): Unit = {
      handler.channelActive(client)
      awaitDataPlane("channel activation")
    }

    def deliver(message: StreamingShuffleMessage): Unit = {
      val framed = message.toByteBuffer()
      val bytes = new Array[Byte](framed.remaining())
      framed.duplicate().get(bytes)
      handler.receive(client, ByteBuffer.wrap(bytes))
      awaitDataPlane("frame delivery")
    }

    /**
     * Delivers arbitrary bytes, bypassing every message constructor.
     *
     * @param bytes the raw frame, exactly as it would arrive off the socket
     */
    def deliverRaw(bytes: Array[Byte]): Unit = {
      handler.receive(client, ByteBuffer.wrap(bytes.clone()))
      awaitDataPlane("raw frame delivery")
    }

    /**
     * Subscribes this consumer to one partition from a given position, through a real heartbeat.
     */
    def subscribe(shuffleId: Int, mapId: Long, partitionId: Int, nextPosition: Long = 0L): Unit = {
      deliver(new HeartbeatMessage(shuffleId, mapId, partitionId, nextPosition, consumerToken))
    }

    def consumerToken: Long = BackpressureStreamKey.consumerTokenOf(consumerId)

    def declaredIdentity: String =
      transportPrincipal +
        StreamingShuffleServerHandler.IDENTITY_TOKEN_SEPARATOR + consumerToken

    def acknowledge(shuffleId: Int, mapId: Long, partitionId: Int, position: Long): Unit = {
      deliver(new AckMessage(shuffleId, mapId, partitionId, position))
    }

    def drainOutbound(): Seq[StreamingShuffleMessage] = {
      awaitDataPlane("outbound production")
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

    def drainBlocks(): Seq[DataBlockMessage] = drainOutbound().collect {
      case block: DataBlockMessage => block
    }

    def close(): Unit = {
      awaitDataPlane("channel close")
      handler.channelInactive(client)
      awaitDataPlane("channel-inactive processing")
      // Discards whatever is still queued and releases it, so a test that closes a channel with
      // frames on it does not leave Netty buffers for the leak detector to find.
      channel.finishAndReleaseAll()
    }
  }

  private val DegradationPartitions = 2

  private val DegradationExecutorMemoryBytes = 1024L * 1024L

  private val DegradationRecords = 200000

  private val StreamedRecordsPerConsumerCase = 120000

  private val PreflightRecords = 5000

  private val DirectDiskRecords = StreamedRecordsPerConsumerCase

  private val UnframeablePartitions = 512

  private val MinimumBufferSizePercent = 1

  private val MaximumBufferSizePercent = 50

  /** The executor-memory figure the framing-negotiation cases carve their budget from. */
  private val NegotiationExecutorMemoryBytes = 256L * 1024L * 1024L

  private val IncompatibleProtocolVersion: Byte = (ProtocolVersion + 1).toByte

  /** Records every driver-facing operation a producer performs, and answers all of them. */
  private class RecordingCoordinatorGateway(
      refuseStandDown: Boolean = false,
      completionAnswer: Option[Boolean] = Some(true))
    extends StreamingShuffleCoordinatorGateway {

    private val declared = new mutable.ArrayBuffer[StreamingShuffleStandDownCause]()
    private val invalidated = new mutable.ArrayBuffer[StreamingShuffleInvalidationReason]()
    private val completed = new mutable.ArrayBuffer[StreamingShuffleProducerGeneration]()
    private val heartbeats = new mutable.ArrayBuffer[StreamingShuffleProducerGeneration]()
    private val mapOutputInvalidations = new mutable.ArrayBuffer[String]()
    private val withdrawals = new mutable.ArrayBuffer[String]()
    private var latched: StreamingShuffleFallbackState = StreamingShuffleFallbackState()

    override def declareFallback(
        shuffleId: Int,
        cause: StreamingShuffleStandDownCause,
        detail: String): StreamingShuffleFallbackState = synchronized {
      declared += cause
      if (refuseStandDown) {
        StreamingShuffleFallbackState()
      } else {
        if (!latched.fallenBack) {
          latched = StreamingShuffleFallbackState(cause.toString, declaredEpoch)
        }
        latched
      }
    }

    override def withdrawStreaming(
        shuffleId: Int,
        detail: String): StreamingShuffleFallbackState = synchronized {
      withdrawals += detail
      if (refuseStandDown) {
        StreamingShuffleFallbackState()
      } else {
        if (!latched.fallenBack) {
          latched = StreamingShuffleFallbackState(
            StreamingShuffleCoordinator.WITHDRAWN_WITHOUT_CONDITION, declaredEpoch)
        }
        latched
      }
    }

    override def fallbackState(shuffleId: Int): StreamingShuffleFallbackState =
      synchronized(latched)

    override def invalidateStreamedMapOutput(shuffleId: Int, detail: String): Boolean =
      synchronized {
        mapOutputInvalidations += detail
        !refuseStandDown
      }

    override def completeProducer(
        shuffleId: Int,
        generation: StreamingShuffleProducerGeneration): Option[Boolean] = synchronized {
      completed += generation
      completionAnswer
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

    def declaredFallbacks: Seq[StreamingShuffleStandDownCause] = synchronized(declared.toSeq)

    def unattributedWithdrawals: Seq[String] = synchronized(withdrawals.toSeq)

    def invalidations: Seq[StreamingShuffleInvalidationReason] = synchronized(invalidated.toSeq)

    def completions: Seq[StreamingShuffleProducerGeneration] = synchronized(completed.toSeq)

    def heartbeatedGenerations: Seq[StreamingShuffleProducerGeneration] =
      synchronized(heartbeats.toSeq)

    def streamedMapOutputInvalidations: Seq[String] = synchronized(mapOutputInvalidations.toSeq)
  }

  /** Records the routing a producer installs and withdraws on its own executor. */
  private class RecordingRouteRegistry(
      private val listener: Option[StreamingShuffleListener] = None)
    extends StreamingShuffleRouteRegistry {

    private val withdrawn = new mutable.ArrayBuffer[(Int, Long)]()

    private val routed = new mutable.HashMap[(Int, Long), StreamingShuffleServerHandler]()

    private val live = new mutable.LinkedHashSet[(Int, Long)]()

    private val released = new mutable.ArrayBuffer[(Int, Long)]()

    private val faulted = new mutable.ArrayBuffer[(Int, Long, String)]()

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

    /** Records one producer giving up its participation in a consumer channel. */
    override def releaseChannelParticipation(
        channel: Channel,
        handler: StreamingShuffleServerHandler,
        closeWhenLast: Boolean): Boolean = synchronized {
      released += ((handler.shuffleId, handler.mapId))
      listener.exists(_.releaseChannelParticipation(channel, handler, closeWhenLast))
    }

    /** Records one channel-global fault teardown. */
    override def closeFaultedChannel(
        channel: Channel,
        handler: StreamingShuffleServerHandler,
        reason: String): Unit = synchronized {
      faulted += ((handler.shuffleId, handler.mapId, reason))
      listener.foreach(_.closeFaultedChannel(channel, handler, reason))
    }

    def withdrawals: Seq[(Int, Long)] = synchronized(withdrawn.toSeq)

    def participationReleases: Seq[(Int, Long)] = synchronized(released.toSeq)

    def faultedChannelCloses: Seq[(Int, Long, String)] = synchronized(faulted.toSeq)

    def isRouted(shuffleId: Int, mapId: Long): Boolean =
      synchronized(routed.contains((shuffleId, mapId)))

    /** Routing entries still installed, which a clean teardown must leave empty. */
    def liveRoutes: Seq[(Int, Long)] = synchronized(live.toSeq)

    def liveRouteCount: Int = synchronized(live.size)

    /**
     * How many producers the real routing table is serving, when this fixture has one behind it.
     */
    def routedProducerCount: Option[Int] = listener.map(_.producerCount)

    def routedServes(shuffleId: Int, mapId: Long): Option[Boolean] =
      listener.map(_.serves(shuffleId, mapId))

    /** Releases the real routing table, if there is one. */
    def close(): Unit = listener.foreach(_.releaseAll())
  }

  /** Stands in for the sort-based writer a degraded attempt is handed to. */
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

    def writtenRecords: Int = synchronized(written)

    def stopCalls: Seq[Boolean] = synchronized(stopped.toSeq)
  }

  /** One writer under test together with everything it was built from. */
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

    def reserveRemainingAllowance(headroomBytes: Long = 0L): Long = {
      require(headroomBytes >= 0L, s"Headroom must be non-negative, but was $headroomBytes")
      var request = math.max(0L, quota.totalBytes - quota.reservedBytes - headroomBytes)
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
     * Completes the task the way the executor would, then releases anything a failed test left
     * behind.
     */
    def close(): Unit = {
      // The primary failure is the one the task completion listeners raised, and it is the one that
      // must reach the test.
      var primary: Throwable = null
      try {
        context.markTaskCompleted(None)
      } catch {
        case failure: Throwable => primary = failure
      }
      // The routing table is released first, and that ordering is production's own: stopping the
      // manager releases the executor's listener, which deregisters every producer it routed and so
      // drives each handler's own release.
      Seq[() => Unit](
        () => routes.close(),
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

  /** One consumer attached to the producer under test, over an embedded channel. */
  private class ConsumerAttachment(
      val handler: StreamingShuffleServerHandler,
      val channel: EmbeddedChannel,
      val client: TransportClient,
      val consumerId: String) {

    private def awaitDataPlane(operation: String): Unit = {
      if (!handler.awaitDataPlaneIdle(10000L)) {
        throw new IllegalStateException(
          s"The producer data plane did not settle after $operation")
      }
    }

    def subscribe(
        shuffleId: Int,
        mapId: Long,
        partitionId: Int,
        consumedThrough: Long = AckMessage.NOTHING_CONSUMED): Unit = {
      // A heartbeat states the NEXT position its sender expects, so a consumer that has taken
      // nothing announces zero.
      val frame = new HeartbeatMessage(shuffleId, mapId, partitionId, consumedThrough + 1L,
        BackpressureStreamKey.consumerTokenOf(consumerId))
      handler.receive(client, frame.toByteBuffer())
      awaitDataPlane("consumer subscription")
    }

    def acknowledge(
        shuffleId: Int,
        mapId: Long,
        partitionId: Int,
        position: Long): Unit = {
      handler.receive(client,
        new AckMessage(shuffleId, mapId, partitionId, position).toByteBuffer())
      awaitDataPlane("consumer acknowledgement")
    }

    def retransmit(
        shuffleId: Int,
        mapId: Long,
        partitionId: Int,
        through: Long): Unit = {
      handler.receive(client,
        new RetransmitRequestMessage(shuffleId, mapId, partitionId, through).toByteBuffer())
      awaitDataPlane("consumer retransmission request")
    }

    def drainOutbound(): Seq[StreamingShuffleMessage] = {
      awaitDataPlane("consumer outbound production")
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

    def close(): Unit = {
      awaitDataPlane("consumer close")
      handler.channelInactive(client)
      awaitDataPlane("consumer channel-inactive processing")
      if (channel.isOpen) {
        channel.close().syncUninterruptibly()
      }
    }
  }

  private def acknowledgeThenTrip(
      harness: WriterHarness,
      consumer: ConsumerAttachment,
      recordCount: Int,
      seed: Long)(
      trip: StreamingShuffleFallbackPolicy => Unit): Iterator[Product2[Int, Int]] = {
    val acknowledged = new AtomicLong(MemorySpillManager.UNSET_SEQUENCE)
    val tripped = new AtomicBoolean(false)
    deterministicRecords(recordCount, seed = seed, keySpace = 40).iterator.map { record =>
      if (acknowledged.get() == MemorySpillManager.UNSET_SEQUENCE) {
        if (harness.writer.blocksStreamed > 0L) {
          val delivered = consumer.drainOutbound().collect {
            case block: DataBlockMessage => block.sequenceNumber()
          }
          if (delivered.nonEmpty) {
            consumer.acknowledge(harness.shuffleId, defaultMapId, 0, delivered.max)
            acknowledged.set(delivered.max)
            assert(harness.spillManager.lowestRetainedSequence(0) != 0L,
              "the acknowledgement must have released the retained prefix of partition 0, or " +
                "this case is the in-place rewrite case rather than the one it cannot serve")
          }
        }
      } else if (!tripped.get()) {
        tripped.set(true)
        trip(harness.fallbackPolicy)
      }
      record
    }
  }

  private def attachConsumer(harness: WriterHarness): ConsumerAttachment = {
    val channel = new EmbeddedChannel(DefaultChannelId.newInstance())
    val client = new TransportClient(channel, new TransportResponseHandler(channel))
    client.setClientId(authenticatedApplicationId)
    harness.serverHandler.channelActive(client)
    if (!harness.serverHandler.awaitDataPlaneIdle(10000L)) {
      throw new IllegalStateException(
        "The producer data plane did not settle after attaching a consumer")
    }
    new ConsumerAttachment(harness.serverHandler, channel, client, consumerId)
  }

  /**
   * A consumer channel over an embedded Netty channel, driving a producer handler as the wire
   * would.
   */
  private class ConsumerChannel(
      handler: StreamingShuffleServerHandler,
      val consumerId: String,
      val transportPrincipal: String = authenticatedApplicationId) {

    val channel: EmbeddedChannel = new EmbeddedChannel(DefaultChannelId.newInstance())
    val client: TransportClient =
      new TransportClient(channel, new TransportResponseHandler(channel))
    client.setClientId(transportPrincipal)

    private def awaitDataPlane(operation: String): Unit = {
      if (!handler.awaitDataPlaneIdle(10000L)) {
        throw new IllegalStateException(
          s"The producer data plane did not settle after $operation")
      }
    }

    handler.channelActive(client)
    awaitDataPlane("consumer channel activation")

    def subscribe(partitionId: Int, nextPosition: Long = 0L): Unit = {
      deliver(new HeartbeatMessage(handler.shuffleId, handler.mapId, partitionId,
        nextPosition, consumerToken))
    }

    /** The stable session token every heartbeat from this consumer declares. */
    def consumerToken: Long = BackpressureStreamKey.consumerTokenOf(consumerId)

    def declaredIdentity: String =
      transportPrincipal +
        StreamingShuffleServerHandler.IDENTITY_TOKEN_SEPARATOR + consumerToken

    def acknowledge(partitionId: Int, position: Long): Unit = {
      deliver(new AckMessage(handler.shuffleId, handler.mapId, partitionId, position))
    }

    def deliver(message: StreamingShuffleMessage): Unit = {
      val framed = message.toByteBuffer()
      val bytes = new Array[Byte](framed.remaining())
      framed.duplicate().get(bytes)
      handler.receive(client, ByteBuffer.wrap(bytes))
      awaitDataPlane("consumer frame delivery")
    }

    def drainInbound(): Seq[StreamingShuffleMessage] = {
      awaitDataPlane("consumer inbound production")
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

    def drainInboundBlocks(): Seq[DataBlockMessage] = drainInbound().collect {
      case block: DataBlockMessage => block
    }

    def crash(): Unit = {
      awaitDataPlane("consumer crash")
      channel.close().syncUninterruptibly()
      handler.channelInactive(client)
      awaitDataPlane("consumer crash cleanup")
    }

    def vanish(): Unit = {
      channel.close().syncUninterruptibly()
    }

    /** Closes the channel the way a reduce task that has finished reading does. */
    def disconnect(): Unit = crash()
  }

  /** A sort-based writer that records what it was handed. */
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

    def recordsWritten: Seq[(Int, Int)] = received.toSeq

    def writeCallCount: Int = writeCalls.get()
  }

  /** The parts of a streaming registration a case may need to vary. */
  private case class RegistrationFixture(
      protocolVersion: Byte = ProtocolVersion,
      gateway: RecordingCoordinatorGateway = new RecordingCoordinatorGateway)

  /**
   * Assembles one writer and every collaborator it needs.
   *
   * @param numPartitions reduce partitions the shuffle declares, which is the divisor of every
   *     buffer ceiling and must agree with the dependency's partitioner
   * @param maxBandwidthMBps egress cap for this executor, absent -- and therefore unlimited --
   *     by default.
   * @param sortWriterFactory the same delegate supplied as a factory, for a case that asserts
   *     the writer builds its delegate exactly once and only when it degrades
   * @param recordingSortDelegate a working recording delegate, which is what makes the
   *     *successful* fallback path -- and the cleanup it owes -- observable rather than merely
   *     unreachable
   * @return the assembled fixture, whose `close()` the caller owns
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
    val quota = new MemorySpillManager.ExecutorBufferQuota(
      bufferSizePercent, spillThreshold, () => executorMemoryBytes)
    // autoPoll = false so the 100 ms cadence is driven by this suite's clock through pollOnce()
    // rather than by the executor's shared ticker, which would make a timing assertion a race.
    val spillManager = new MemorySpillManager(
      context.taskMemoryManager, writerConf, clock, Some(quota), autoPoll = false)
    val blockResolver = new StreamingShuffleBlockResolver(writerConf)
    val egressBudget = new TokenBucketRateLimiter.ExecutorEgressBudget(
      maxBandwidthMBps = maxBandwidthMBps, clock = clock)
    val backpressure = new BackpressureProtocol(writerConf, null, egressBudget, clock, quota)
    val rateLimiter = egressBudget.limiterFor(handle.shuffleId)
    val errorNotifier = new StreamingShuffleErrorNotifier(handle.shuffleId, writerConf)
    val routes = new RecordingRouteRegistry(routeTable)
    val fallbackPolicy = new StreamingShuffleFallbackPolicy(writerConf, clock)
    val serverHandler = new StreamingShuffleServerHandler(writerConf, handle.shuffleId, mapId,
      taskAttemptId, handle.numPartitions, blockResolver, routes, backpressure, rateLimiter,
      errorNotifier, fallbackPolicy, clock)
    // Mirrors what the manager does on the way to constructing this writer, and in its order: the
    // routing entry is installed once the handler exists and before the producer is announced, so
    // that a consumer acting on the published address reaches a handler rather than nothing.
    routes.installRoute(handle.shuffleId, mapId, serverHandler)
    val gateway = registration.gateway
    val components = StreamingShuffleWriterComponents(backpressure, rateLimiter, spillManager,
      blockResolver, serverHandler, fallbackPolicy, errorNotifier, gateway)
    // By default a fixture that degraded would silently stop testing the streaming path, so the
    // delegate is a loud failure rather than a working writer: every test that expects streaming
    // therefore proves it.
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
   * @param harness the fixture to use and then close
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
   * @param maxRounds a bound so that a producer which never drains fails the caller rather than
   *     hanging it
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

    val fixedMessages = Seq(
      ack(protocolShuffleId, defaultMapId, 0, 0L),
      streamTermination(protocolShuffleId, defaultMapId, 0, 1L))
    assert(FixedMessageEncodedLength === 25,
      "An identity-free control message is the specified twenty-five bytes")
    assert(fixedMessages.forall(_.encodedLength() == FixedMessageEncodedLength),
      s"Every fixed-size streaming message must encode to $FixedMessageEncodedLength bytes")
    assert(fixedMessages.map(typeOf).distinct.size === fixedMessages.size,
      "Fixed-size messages of equal length must still be distinguished by their type discriminator")
    assert(HeartbeatBaseEncodedLength === 33,
      "A heartbeat is a control message plus the eight bytes of the consumer session token")
    Seq(
      heartbeat(protocolShuffleId, defaultMapId, 0, 0L),
      heartbeat(protocolShuffleId, defaultMapId, 0, 0L, 987654321L)).foreach { beat =>
      assert(beat.encodedLength() === HeartbeatBaseEncodedLength,
        "A heartbeat is of one fixed width whether it declares an identity or not")
      assert(framedLength(beat.encodedLength()) === HeartbeatBaseEncodedLength + 1,
        "A framed heartbeat is its encoded length plus the one-byte type discriminator")
    }
    assert(RetransmitRequestEncodedLength === 33,
      "A retransmission request is a control message plus the eight bytes of its upper bound")
    Seq(
      retransmitRequest(protocolShuffleId, defaultMapId, 0, 0L),
      retransmitRequest(protocolShuffleId, defaultMapId, 0, 0L, 64L)).foreach { request =>
      assert(request.encodedLength() === RetransmitRequestEncodedLength,
        s"A retransmission request must encode to $RetransmitRequestEncodedLength bytes but " +
          s"encoded to ${request.encodedLength()}")
    }

    val partitions = 1
    withHarness(newHarness(
        numPartitions = partitions,
        executorMemoryBytes = 4L * 1024L * 1024L)) { harness =>
      val consumer = attachConsumer(harness)
      try {
        consumer.subscribe(harness.shuffleId, defaultMapId, partitionId = 0)
        harness.writer.write(deterministicRecords(60000, seed = 4L, keySpace = 48).iterator)
        assert(harness.writer.stop(success = true).isDefined,
          "The successful stop must produce a status")
        assert(harness.writer.blockPayloadCapacityBytes > 0,
          "The writer must have derived a positive framing capacity")
        assert(harness.writer.blockPayloadCapacityBytes <= MaxBlockSizeBytes,
          "The writer must never frame above the protocol's block payload cap")

        val emitted = consumer.drainOutbound().collect { case block: DataBlockMessage => block }
        assert(emitted.size > 1,
          s"The producer must have emitted more than one block for the consumer to check, yet it " +
            s"emitted ${emitted.size}")
        assert(emitted.size.toLong <= harness.writer.blocksStreamed,
          "It cannot have emitted more blocks than it framed")

        emitted.zipWithIndex.foreach { case (block, index) =>
          val sequence = index.toLong
          val emittedPayload = block.copyPayload()
          assert(block.protocolVersion() === ProtocolVersion,
            s"Block $sequence must be stamped with the protocol version this build speaks")
          assert(block.shuffleId() === harness.shuffleId && block.mapId() === defaultMapId &&
              block.partitionId() === 0,
            s"Block $sequence must name the stream it belongs to, yet it named shuffle " +
              s"${block.shuffleId()} map ${block.mapId()} partition ${block.partitionId()}")
          assert(block.sequenceNumber() === sequence,
            s"A partition's blocks must be numbered densely from zero, which is what the reader " +
              s"reassembles on, yet block $index arrived as ${block.sequenceNumber()}")
          assert(emittedPayload.length <= harness.writer.blockPayloadCapacityBytes,
            s"Block $sequence carried ${emittedPayload.length} bytes, above the " +
              s"${harness.writer.blockPayloadCapacityBytes} byte framing capacity")
          assert(emittedPayload.length <= MaxBlockSizeBytes,
            s"and above the protocol's own $MaxBlockSizeBytes byte cap")

          assert(block.verifyChecksum(),
            s"Block $sequence must verify against the checksum the producer stamped it with")
          assert(block.checksum() ===
              blockChecksum(harness.shuffleId, defaultMapId, 0, sequence, emittedPayload),
            s"Block $sequence must be stamped through the shared identity-bound helper")
          assert(block.checksum() !==
              StreamingShuffleChecksum.compute(emittedPayload),
            s"Block $sequence must not be stamped with a bare payload checksum, or a block " +
              "delivered against the wrong stream would verify")
          assert(block.checksum() !==
              blockChecksum(harness.shuffleId, defaultMapId, 1, sequence, emittedPayload),
            s"Block $sequence's stamp must cover its partition")
          assert(block.checksum() !==
              blockChecksum(harness.shuffleId, defaultMapId, 0, sequence + 1L, emittedPayload),
            s"and its sequence number")
        }

        val original = emitted.head.copyPayload()
        val tampered = original.clone()
        tampered(tampered.length / 2) = (tampered(tampered.length / 2) ^ 0x01).toByte
        assert(!StreamingShuffleChecksum.verifyBlock(harness.shuffleId, defaultMapId, 0,
            emitted.head.sequenceNumber(), tampered, emitted.head.checksum()),
          "One flipped byte in a block's payload must fail the stamp the producer emitted")
      } finally {
        consumer.close()
      }
    }
  }

  test("every block the producer puts on the wire carries the retained payload and its checksum") {
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
        assert(block.checksum() === blockChecksum(harness.shuffleId, defaultMapId,
            block.partitionId(), block.sequenceNumber(), block.copyPayload()),
          s"Block ${block.sequenceNumber()} of partition ${block.partitionId()} was not stamped " +
            "with the identity-bound CRC32C the protocol specifies")
        val retained = harness.spillManager.retainedPayload(
          block.partitionId(), block.sequenceNumber())
        assert(retained.isDefined,
          s"Block ${block.sequenceNumber()} of partition ${block.partitionId()} was written to " +
            "the wire but is not in the retention window, so it could never be retransmitted")
        assert(java.util.Arrays.equals(retained.get, block.copyPayload()),
          s"Block ${block.sequenceNumber()} of partition ${block.partitionId()} put bytes on the " +
            "wire that differ from the bytes it retained")
      }

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
      // to release.
      val movedBytes = harness.spillManager.spillAllRetained()
      assert(movedBytes > 0L, "Making the retained window durable must move bytes to disk")
      val files = harness.spillFiles()
      assert(files.nonEmpty, "The durability flush must have produced at least one spill file")
      assert(files.forall(_.exists()), "Every spill file must exist before the failure")

      // ==The buffers production owns, and what the failure must do to them== The buffers worth
      // measuring here are the ones the RESOLVER creates: `getBlockData` builds a `ManagedBuffer`
      // over a spill segment, and the party that reads it -- a consumer, or the external shuffle
      // service -- owns it from then on.
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
          buffer.release()
        }
      }
      assert(servedBytes.sum > 0L, "The resolver must have served bytes for the durable window")
      assert(harness.blockResolver.getBlockData(servedBlocks.head).size() === servedBytes.head,
        "and a second reader must still be served the same bytes, because releasing a buffer " +
          "cannot destroy map output")
      val framedBlocks = harness.writer.streamedPartitions
        .map(harness.writer.nextSequenceNumberFor).sum
      assert(framedBlocks === harness.writer.blocksStreamed,
        "Every framed block must be accounted in exactly one partition's numbering")
      assert(harness.spillManager.allSpilledBlocks.size.toLong === framedBlocks,
        s"The durable window must hold exactly the $framedBlocks block(s) the writer framed, yet " +
          s"it holds ${harness.spillManager.allSpilledBlocks.size}")
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

      assert(retainedBlocks.forall { case (partitionId, sequence) =>
          harness.spillManager.retainedPayload(partitionId, sequence).isEmpty
        },
        "No block may remain reachable in the retention window after the attempt failed")
      assert(harness.writer.streamedPartitions.forall(harness.spillManager.retainedBlockCount(_)
          == 0),
        "No partition may retain a block after the attempt failed")

      assert(harness.errorNotifier.fetchFailure.isEmpty,
        "A producer must not construct a fetch failure, which belongs to the reading side")
    }
  }

  test("every resource production held is released exactly once, whichever way the task ends") {
    // The specification's "zero memory leaks under failure" and "no buffer, channel or spill file
    // survives task completion" are claims about resources *production* acquired, so they can only
    // be proved against production's own ledgers.
    val partitions = 4
    Seq("a successful stop", "a failing stop", "a cancellation with no stop at all").foreach {
      ending =>
      val harness = newHarness(numPartitions = partitions)
      try {
        val consumer = new ProducerConsumerChannel(harness.serverHandler, consumerId)
        consumer.activate()
        (0 until partitions).foreach(consumer.subscribe(harness.shuffleId, defaultMapId, _))
        harness.writer.write(deterministicRecords(1500, seed = 10L, keySpace = 40).iterator)
        harness.serverHandler.flushPending()
        val blocks = consumer.drainBlocks()
        assert(blocks.nonEmpty, s"[$ending] the producer must have written blocks to the consumer")

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
        // the spill manager have.
        harness.context.markTaskCompleted(None)
        consumer.close()

        assert(harness.spillManager.isClosed,
          s"[$ending] the buffer store must be closed once the task has ended")
        assert(harness.spillManager.bufferedBytes === 0L,
          s"[$ending] no buffered byte may survive task completion")
        assert(harness.spillManager.scratchBytes === 0L,
          s"[$ending] no framing scratch reservation may survive task completion")
        val retainedMetadataBytes =
          if (ending == "a successful stop") {
            harness.spillManager.allSpilledBlocks.size.toLong *
              MemorySpillManager.SPILLED_RECORD_METADATA_BYTES
          } else {
            0L
          }
        assert(harness.spillManager.executorReservedBytes === retainedMetadataBytes,
          s"[$ending] only resolver-owned durable-record metadata may survive task completion")
        assert(harness.spillManager.getUsed() === 0L,
          s"[$ending] the memory consumer still holds ${harness.spillManager.getUsed()} bytes of " +
            "task execution memory")
        assert(harness.spillManager.spillFileDeletionFailures === 0L,
          s"[$ending] no spill file deletion may have failed")
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
          assert(harness.spillManager.executorReservedBytes === 0L,
            s"[$ending] retiring retained output must return its durable-record metadata too")
        } else {
          assert(!harness.spillManager.spillFilesTransferred,
            s"[$ending] an attempt that did not succeed must not hand its spill files on")
          assert(files.forall(file => !file.exists()),
            s"[$ending] spill files survived: ${files.filter(_.exists()).mkString(", ")}")
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
        assert(harness.backpressure.reservedTransientQuotaBytes === 0L,
          s"[$ending] the executor-wide transient category still holds " +
            s"${harness.backpressure.reservedTransientQuotaBytes} bytes, so a framing copy was " +
            "reserved and never returned")

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
        assert(harness.backpressure.reservedTransientQuotaBytes === 0L,
          s"[$ending] a repeated release must leave the transient category at zero")
      } finally {
        harness.close()
      }
    }
  }

  test("a subscribed consumer is served while the map task is still producing") {
    // This is the overlap claim, and it is a claim about the code path rather than about
    // scheduling.
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
        val onSubscribe = consumer.drainOutbound()
        assert(!onSubscribe.exists(_.isInstanceOf[DataBlockMessage]),
          s"a subscription cannot deliver data that does not exist yet, yet it wrote $onSubscribe")

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
    val partitions = 4
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      val spillManager = harness.spillManager

      harness.writer.write(deterministicRecords(320, seed = 31L, keySpace = 40).iterator)

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

      val retainedChargeBeforeStop = spillManager.bufferedBytes
      val retainedBlocksBeforeStop =
        (0 until partitions).map(spillManager.retainedBlockCount).sum.toLong
      assert(retainedBlocksBeforeStop > 0L, "the retained window must hold blocks to be written")
      assert(harness.writer.stop(success = true).isDefined,
        "the successful stop must produce a status")
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

      val durableSequences = spillManager.spilledBlocks(0).map(_.sequenceNumber)
      assert(durableSequences === (consumedBlocks until admitted).map(_.toLong),
        s"only the unacknowledged sequence numbers may be on disk, yet the durable set was " +
          s"$durableSequences")
      assert(spillManager.spilledBlocks(0).map(_.payloadLength.toLong).sum ===
          stillRetained.toLong * blockBytes.toLong,
        "and they must carry exactly their own payloads, so nothing a consumer took was written")
    }
  }

  test("output nobody has come for is secured while producing, not in a burst at the stop") {
    // The other half of where materialisation happens, and the half that decides how long a map
    // task takes.
    val partitions = 2
    val clock = newManualClock()
    withHarness(newHarness(numPartitions = partitions,
        executorMemoryBytes = PipelinedDurabilityExecutorMemoryBytes,
        bufferSizePercent = PipelinedDurabilityBufferPercent, clock = clock)) { harness =>
      val spillManager = harness.spillManager
      assert(spillManager.registeredConsumerCount === 0,
        "no consumer may be registered, or this case would be measuring the live path")

      var consumed = 0
      val source = deterministicRecords(PipelinedDurabilityRecords, seed = 77L, keySpace = 128)
      val advancing = source.iterator.map { record =>
        consumed += 1
        if (consumed % 64 == 0) {
          clock.advance(StreamingShuffleWriter.RETAINED_DURABILITY_INTERVAL_MS)
        }
        record
      }
      harness.writer.write(advancing)
      assert(harness.writer.blocksStreamed > 1L,
        s"the fixture must frame several blocks for the cadence to have anything to secure, but " +
          s"framed ${harness.writer.blocksStreamed} at a capacity of " +
          s"${harness.writer.blockPayloadCapacityBytes} bytes")

      assert(harness.writer.retainedDurabilityPassCount > 0L,
        "a producer nobody is consuming must secure its unclaimed output while it produces, but " +
          "no pipelined durability pass ran")
      val securedAhead = harness.writer.retainedDurabilityBytesAheadOfStop
      assert(securedAhead > 0L,
        s"the passes must have moved bytes, but moved $securedAhead")

      val residentBeforeStop = spillManager.bufferedBytes
      assert(securedAhead > residentBeforeStop,
        s"the stop must be left with a bounded tail: $securedAhead byte(s) were secured while " +
          s"producing against $residentBeforeStop still resident")

      assert(spillManager.bufferUtilizationPercent < DefaultSpillThresholdPercent.toLong,
        s"the fixture must stay below the ${DefaultSpillThresholdPercent}% threshold, or this " +
          "case would be measuring eviction under pressure")
      assert(spillManager.spillCount === 0L,
        s"securing unclaimed output is not a spill and must not advance the operator-visible " +
          s"spill count, yet ${spillManager.spillCount} event(s) were counted")
      assert(spillManager.durableAdmissionCount === 0L,
        "nor may any block have bypassed the buffer, which is a different event again")
      assert(spillManager.durabilityFlushCount > 0L,
        "it must be counted as the durability flush it is, so the two remain distinguishable")
      assert(spillManager.diskBytesSpilled > 0L,
        "and its volume must still land on Spark's existing spill accumulator")

      assert(harness.writer.stop(success = true).isDefined,
        "the successful stop must still produce a status")
      assert(spillManager.bufferedBytes === 0L,
        "nothing may be resident afterwards, because task memory cannot outlive the task")
      assert(spillManager.spillFilesTransferred,
        "file ownership must have moved to the executor-scoped resolver")
      assert(harness.blockResolver.getBlocksForShuffle(harness.shuffleId, defaultMapId).nonEmpty,
        "and the resolver must be able to serve the whole output to a consumer that arrives later")
      val streamed = harness.writer.getPartitionLengths().sum
      assert(streamed > 0L, "the attempt must report the bytes it streamed")
    }
  }

  test("a producer whose consumer keeps pace is never written ahead of its stop") {
    // The complement, and the property that keeps the pipelined durability pass above from becoming
    // a write-through.
    val partitions = 2
    val clock = newManualClock()
    withHarness(newHarness(numPartitions = partitions,
        executorMemoryBytes = PipelinedDurabilityExecutorMemoryBytes,
        bufferSizePercent = PipelinedDurabilityBufferPercent, clock = clock)) { harness =>
      val spillManager = harness.spillManager
      // The consumer is admitted to the acknowledgement protocol directly, which is exactly the
      // state a subscription establishes and is the state the gate reads.
      assert(spillManager.registerConsumer(consumerId),
        "a store that still serves its retained output must admit a consumer")
      assert(spillManager.registeredConsumerCount > 0,
        "the registration must be visible to the gate the producer consults")

      var consumed = 0
      val source = deterministicRecords(PipelinedDurabilityRecords, seed = 78L, keySpace = 128)
      val advancing = source.iterator.map { record =>
        consumed += 1
        if (consumed % 64 == 0) {
          clock.advance(4L * StreamingShuffleWriter.RETAINED_DURABILITY_INTERVAL_MS)
        }
        record
      }
      harness.writer.write(advancing)
      assert(harness.writer.blocksStreamed > 1L,
        s"the fixture must frame several blocks, or the gate is not what stopped the pass, but " +
          s"framed ${harness.writer.blocksStreamed}")

      assert(harness.writer.retainedDurabilityPassCount === 0L,
        s"a subscribed consumer must stop the pipelined pass however much time passes, yet " +
          s"${harness.writer.retainedDurabilityPassCount} pass(es) ran")
      assert(harness.writer.retainedDurabilityBytesAheadOfStop === 0L,
        "and nothing may have been written out from underneath a consumer that is taking it")
    }
  }

  test("a producer handed to the sort-based writer withdraws every owner it published") {
    // What this proves is an ownership hand-off, not the existence of a delegate.
    val partitions = 4
    val delegate = new WorkingSortShuffleDelegate
    withHarness(newHarness(numPartitions = partitions,
        recordingSortDelegate = Some(delegate))) { harness =>
      harness.routes.installRoute(harness.shuffleId, defaultMapId, harness.serverHandler)
      harness.backpressure.registerShuffle(harness.shuffleId, partitions)
      assert(harness.blockResolver.registerProducer(harness.shuffleId, defaultMapId,
          defaultTaskAttemptId, harness.spillManager),
        "The retained-output registration must take effect before the attempt is stood down")
      val consumer = new ProducerConsumerChannel(harness.serverHandler, consumerId)
      consumer.activate()
      consumer.subscribe(harness.shuffleId, defaultMapId, 0)
      assert(harness.serverHandler.sessionCount === 1,
        "The subscribing consumer must hold a session before the attempt is stood down")

      assert(harness.routes.isRouted(harness.shuffleId, defaultMapId),
        "The routing entry must be installed before the attempt is stood down")
      assert(harness.blockResolver.producerFor(harness.shuffleId, defaultMapId).isDefined,
        "The retained output must be reachable before the attempt is stood down")
      assert(harness.backpressure.registeredShuffleIds.contains(harness.shuffleId),
        "The shuffle must hold its egress share before the attempt is stood down")

      harness.fallbackPolicy.recordAllocationGrant(requestedBytes = 1 << 20, grantedBytes = 0L)
      assert(harness.fallbackPolicy.hasTripped,
        "A refused allocation must trip the fallback policy before the write begins")

      val records = deterministicRecords(192, seed = 6L, keySpace = 24)
      harness.writer.write(records.iterator)

      assert(delegate.consumedRecords === records,
        "The sort-based delegate must receive the whole record iterator, in order")
      assert(harness.writer.recordsStreamed === 0L,
        "A delegated attempt must not have streamed a record of its own")
      assert(harness.writer.blocksStreamed === 0L,
        "A delegated attempt must not have framed a block of its own")

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
    val routeTable = new StreamingShuffleListener(
      streamingConfWithOverrides(), new ManualClock(ManualClockEpochMillis))
    withHarness(newHarness(numPartitions = partitions, routeTable = Some(routeTable))) { harness =>
      harness.writer.write(deterministicRecords(256, seed = 5L, keySpace = 32).iterator)
      val status = harness.writer.stop(success = true)
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

  test("a refused completion publishes no ordinary status and reports the withdrawn one instead") {
    // The publication barrier, from the producer's side.
    val refusing = new RecordingCoordinatorGateway(completionAnswer = Some(false))
    val harness = newHarness(registration = RegistrationFixture(gateway = refusing))
    withHarness(harness) { fixture =>
      fixture.writer.write(deterministicRecords(96, seed = 211L, keySpace = 24).iterator)
      assert(fixture.fallbackPolicy.observeShuffleFallback(fixture.shuffleId,
          StreamingShuffleFallbackState(
            StreamingShuffleFallbackReason.NetworkSaturation.toString, declaredEpoch)),
        "the shuffle-wide verdict must be cached, because that is what the writer consults")

      val status = fixture.writer.stop(success = true)
      assert(status.isDefined,
        "the shared write path dereferences the status unconditionally, so even a withdrawn " +
          "output must report one")
      val reported = status.get
      (0 until defaultPartitions).foreach { partitionId =>
        assert(reported.getSizeForBlock(partitionId) > 0L,
          s"partition $partitionId of a withdrawn output must be unskippable, or a reducer would " +
            "silently produce a result short of data")
      }
      assert(fixture.gateway.completions.nonEmpty,
        "the writer must have asked the driver to accept its completion before publishing anything")
      assert(fixture.routes.withdrawals.nonEmpty,
        s"a refused generation must be withdrawn from every owner on this executor, yet the " +
          s"routing table saw ${fixture.routes.withdrawals}")
      assert(!fixture.serverHandler.servesRetainedOutput,
        "and it must serve nothing further, because the output it held is being recomputed")
      assertNoPublishedFailure(fixture.errorNotifier,
        "a producer that finished into a shuffle-wide fallback")
    }
  }

  test("a refused completion with no shuffle-wide verdict fails rather than publishing") {
    // The other refusal, and the one that must NOT complete.
    val refusing = new RecordingCoordinatorGateway(completionAnswer = Some(false))
    val harness = newHarness(registration = RegistrationFixture(gateway = refusing))
    withHarness(harness) { fixture =>
      fixture.writer.write(deterministicRecords(96, seed = 212L, keySpace = 24).iterator)
      assert(!fixture.fallbackPolicy.hasTripped,
        "no trip may be latched here, or this would be the degradation case rather than the " +
          "zombie one")

      val failure = intercept[SparkException] {
        fixture.writer.stop(success = true)
      }
      assert(failure.getMessage.contains("no longer holds that producer generation"),
        s"the failure must name the reason precisely so it cannot be mistaken for a generic one, " +
          s"but was: ${failure.getMessage}")
      assert(fixture.writer.producedMapStatus.isEmpty,
        "and no map status may exist for a generation the driver disowned")
      assert(fixture.routes.withdrawals.nonEmpty,
        "the failed generation must be withdrawn from every owner on this executor")
    }
  }

  test("a completion the driver could not confirm withdraws instead of publishing") {
    // The third answer, and the one that must fail CLOSED.
    val unreachable = new RecordingCoordinatorGateway(completionAnswer = None)
    val harness = newHarness(registration = RegistrationFixture(gateway = unreachable))
    withHarness(harness) { fixture =>
      fixture.writer.write(deterministicRecords(96, seed = 213L, keySpace = 24).iterator)
      val status = fixture.writer.stop(success = true)
      assert(fixture.gateway.completions.nonEmpty,
        "the writer must have asked the driver to accept its completion before deciding anything")

      assert(status.isDefined,
        "the successful stop must still produce a status, or the shared write path's dereference " +
          "would throw")
      val reported = status.get
      assert(reported.mapId === defaultMapId, "and it must describe this map attempt")
      (0 until defaultPartitions).foreach { partitionId =>
        assert(reported.getSizeForBlock(partitionId) > 0L,
          s"partition $partitionId of an unconfirmed output must be unskippable, or a reducer " +
            "would silently produce a result short of data instead of raising a fetch failure")
      }

      assert(fixture.routes.withdrawals.nonEmpty,
        s"an unconfirmed generation must be withdrawn from every owner on this executor, yet the " +
          s"routing table saw ${fixture.routes.withdrawals}")
      assert(!fixture.serverHandler.servesRetainedOutput,
        "and it must serve nothing further, because the output it held is being recomputed")
      assert(fixture.writer.standDownFallbackReason.contains(
          StreamingShuffleStandDownCause.ProducerUnavailable),
        s"the withdrawal must be recorded as the structural decline it is -- a rendezvous that " +
          s"could not be confirmed is none of the four specified fallback conditions -- but was " +
          s"${fixture.writer.standDownFallbackReason}")

      assertNoPublishedFailure(fixture.errorNotifier, "a producer whose driver was unreachable")
    }
  }

  test("an unconfirmed completion reports no readable output for a partition it streamed") {
    val unreachable = new RecordingCoordinatorGateway(completionAnswer = None)
    val harness = newHarness(registration = RegistrationFixture(gateway = unreachable))
    withHarness(harness) { fixture =>
      // Every key is a multiple of the partition count, and the fixture's partitioner is a hash
      // over the key, so every record lands in partition zero and the rest stream nothing.
      val oneBucket = (0 until 64).map(index => (index * defaultPartitions, index))
      fixture.writer.write(oneBucket.iterator)
      val status = fixture.writer.stop(success = true)
      assert(status.isDefined, "the stop must still answer with a status")

      val streamed = fixture.writer.getPartitionLengths()
      val emptyPartitions = (0 until defaultPartitions).filter(streamed(_) == 0L)
      assert(emptyPartitions.nonEmpty,
        s"the fixture must leave at least one partition unstreamed for this case to distinguish " +
          s"anything, but every one of ${streamed.mkString(", ")} received bytes")
      emptyPartitions.foreach { partitionId =>
        assert(status.get.getSizeForBlock(partitionId) > 0L,
          s"partition $partitionId streamed no byte, so an ordinary status would report it as " +
            "skippable; a withdrawn output must report it as unskippable so the reducer asks and " +
            "the ask fails")
      }
      assert(fixture.routes.withdrawals.nonEmpty,
        "and the generation must have been withdrawn rather than re-registered")
    }
  }

  test("a second stop is a no-op that returns None") {
    withHarness(newHarness()) { harness =>
      harness.writer.write(deterministicRecords(64, seed = 6L, keySpace = 16).iterator)
      assert(harness.writer.stop(success = true).isDefined, "The first stop must succeed")
      val bytesAfterFirst = harness.metrics.bytesWritten
      val recordsAfterFirst = harness.metrics.recordsWritten
      assert(harness.writer.stop(success = true).isEmpty,
        "A repeated successful stop must be a no-op returning None")
      assert(harness.metrics.bytesWritten === bytesAfterFirst,
        "A repeated stop must not republish written bytes")
      assert(harness.metrics.recordsWritten === recordsAfterFirst,
        "A repeated stop must not republish written records")
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
    // clock the test never advances would legitimately report zero.
    val harness = newHarness(clock = wallClock())
    try {
      val records = deterministicRecords(512, seed = 8L, keySpace = 64)
      harness.writer.write(records.iterator)
      assert(harness.writer.stop(success = true).isDefined, "The stop must succeed")
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

      assert(backpressure.unacknowledgedWindow(key).contains((0L, 0L)),
        "A single-element unacknowledged window is valid and inclusive at both ends")
      assert(backpressure.unacknowledgedBlockCount(key) === 1,
        "Exactly the one unacknowledged block must be counted")
      assert(backpressure.isWithinUnacknowledgedWindow(key, 0L),
        "The one block sent must be inside the unacknowledged window")
      assert(!backpressure.isWithinUnacknowledgedWindow(key, 1L),
        "A block that was never sent cannot be inside the unacknowledged window")
      assert(backpressure.canServeRetransmit(key,
          retransmitRequest(harness.shuffleId, defaultMapId, 0, 0L)),
        "A retransmission scoped to the unacknowledged window must be serviceable")
      assert(!backpressure.canServeRetransmit(key,
          retransmitRequest(harness.shuffleId, defaultMapId, 0, 1L)),
        "A retransmission outside the unacknowledged window must be refused, because those bytes " +
          "were released the moment the consumer acknowledged them")

      assert(backpressure.tryAcknowledge(key, 0L).isDefined,
        "An acknowledgement inside the sent range must be applied rather than refused")
      assert(backpressure.outstandingBytes(key) === 0L,
        "An acknowledgement must release the bytes it covers")
      assert(!backpressure.isConsumerTimedOut(key),
        "A stream with nothing outstanding is idle rather than timed out")

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
    // A live consumer subscribes, takes blocks, and then stops acknowledging; the writer's own
    // maintenance pass is what must notice, and what it must do is retain and REPLAY -- never
    // discard, and never fail the task while the budget lasts.
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

        assert(harness.writer.consumerStallCount === 1L,
          s"The stall must be reported exactly once per stalled stream, yet it was reported " +
            s"${harness.writer.consumerStallCount} time(s)")
        assert(harness.writer.replayAttemptCount === MaxRetryAttempts.toLong,
          s"The writer must spend exactly the specified $MaxRetryAttempts replay attempts over a " +
            s"stall this long and no more, yet it made ${harness.writer.replayAttemptCount}")
        assert(harness.errorNotifier.error.isEmpty,
          s"No failure may be latched while the replay budget lasts, yet " +
            s"${harness.errorNotifier.error} was")

        val firstReplayAt = firstSeenAtAttempt(1L)
        assert(firstReplayAt >= ConsumerLivenessTimeoutMillis,
          s"A replay observed at $firstReplayAt ms would be inside the " +
            s"$ConsumerLivenessTimeoutMillis ms liveness window, which must not fire early")
        assert(firstReplayAt < ConsumerLivenessTimeoutMillis + maintenanceGranularityMs,
          s"and one observed at $firstReplayAt ms would be more than one " +
            s"$maintenanceGranularityMs ms maintenance pass late, which would mean something " +
            "other than the window governed it")

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

        assert(harness.writer.stop(success = true).isDefined,
          "The attempt must still complete successfully, because its output is intact")
      } finally {
        consumer.close()
      }
    }
  }

  test("the writer detects a silent consumer at the ten second boundary and not before") {
    val clock = newManualClock()
    withHarness(newHarness(numPartitions = DegradationPartitions,
        executorMemoryBytes = DegradationExecutorMemoryBytes, clock = clock)) { harness =>
      val consumer = new ConsumerChannel(harness.serverHandler, consumerId)
      consumer.subscribe(partitionId = 0)
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

      val acknowledgementsBefore = harness.serverHandler.ackCount
      consumer.acknowledge(partitionId = 0, position = delivered.last.sequenceNumber())
      assert(!harness.serverHandler.isConsumerStalled(0),
        "an acknowledgement must disarm the stall detector, because progress means the consumer " +
          "is alive")
      assert(harness.serverHandler.ackCount === acknowledgementsBefore + 1L,
        "the final acknowledgement must be applied before the completed consumer is retired")
      assert(harness.serverHandler.sessionCount === 0,
        "a consumer that confirmed its terminated stream must be finally unregistered")
      assert(harness.spillManager.registeredConsumers.isEmpty,
        "final completion must remove the retained cursor that would otherwise pin reclamation")
      consumer.crash()
    }
  }

  test("an unacknowledged window survives a consumer crash and is replayed on reconnection") {
    val clock = newManualClock()
    withHarness(newHarness(numPartitions = DegradationPartitions,
        executorMemoryBytes = DegradationExecutorMemoryBytes, clock = clock)) { harness =>
      val first = new ConsumerChannel(harness.serverHandler, consumerId)
      first.subscribe(partitionId = 0)
      harness.writer.write(deterministicRecords(StreamedRecordsPerConsumerCase, seed = 82L,
        keySpace = 8).iterator)
      val original = first.drainInboundBlocks()
      assert(original.nonEmpty, "the first session must have received blocks to replay later")
      val originalSequences = original.map(_.sequenceNumber())

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
      resumed.subscribe(partitionId = 0)
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

  test("a consumer that vanished is superseded on reconnect and resumes from its own cursor") {
    val clock = newManualClock()
    withHarness(newHarness(numPartitions = DegradationPartitions,
        executorMemoryBytes = DegradationExecutorMemoryBytes, clock = clock)) { harness =>
      val handler = harness.serverHandler
      harness.writer.write(deterministicRecords(StreamedRecordsPerConsumerCase, seed = 91L,
        keySpace = 8).iterator)
      val first = new ConsumerChannel(handler, consumerId)
      first.subscribe(partitionId = 0)
      val delivered = first.drainInboundBlocks().map(_.sequenceNumber())
      assert(delivered.length >= 2,
        s"the first session must have been sent more than one block for a partial " +
          s"acknowledgement to mean anything, but it was sent ${delivered.length}")

      // The identity is a wire fact, not a fixture fact: the producer holds exactly the identity
      // the consumer declared, composed with the principal that scopes it.
      assert(handler.consumerIdentities === Set(first.declaredIdentity),
        s"the producer must key this consumer by the identity it declared, but it holds " +
          s"${handler.consumerIdentities.mkString(", ")}")

      val acknowledgedThrough = delivered(delivered.length / 2)
      first.acknowledge(partitionId = 0, position = acknowledgedThrough)
      assert(handler.acknowledgedPosition(0) === acknowledgedThrough,
        s"the producer must have recorded the acknowledgement, but its cursor stands at " +
          s"${handler.acknowledgedPosition(0)}")
      assert(harness.spillManager.registeredConsumers === Set(first.declaredIdentity),
        s"the store must know this consumer by its declared identity, but it knows " +
          s"${harness.spillManager.registeredConsumers.mkString(", ")}")
      assert(harness.spillManager.consumerPosition(first.declaredIdentity, 0) ===
          Some(acknowledgedThrough),
        s"and must hold its acknowledged position against that identity, but it holds " +
          s"${harness.spillManager.consumerPosition(first.declaredIdentity, 0)}")

      first.vanish()
      assert(handler.liveConsumerCount === 1,
        "a consumer that vanished silently leaves its session behind, which is the state that " +
          "makes supersession necessary rather than optional")

      val supersededBefore = handler.supersededSessionCount
      val sessionsBefore = handler.sessionCount
      clock.advance(RetryBaseBackoffMillis)

      val resumed = new ConsumerChannel(handler, consumerId)
      resumed.subscribe(partitionId = 0)

      assert(handler.supersededSessionCount === supersededBefore + 1L,
        s"the lost connection must be released the moment the consumer identifies itself again, " +
          s"but the producer superseded ${handler.supersededSessionCount - supersededBefore}")
      assert(handler.sessionCount === sessionsBefore,
        s"so the producer holds the new connection in place of the old one rather than both, but " +
          s"it holds ${handler.sessionCount} against $sessionsBefore before the reconnection")
      assert(handler.liveConsumerCount === 1,
        "and still exactly one logical consumer, because the two connections are one consumer")
      assert(handler.consumerIdentities === Set(resumed.declaredIdentity),
        s"under the one identity both declared, but the producer holds " +
          s"${handler.consumerIdentities.mkString(", ")}")
      assert(handler.subscriberCount(0) === 1,
        s"and the superseded connection's subscriber slot must have come back with it, but " +
          s"${handler.subscriberCount(0)} are claimed")

      val replayed = resumed.drainInboundBlocks().map(_.sequenceNumber())
      assert(replayed.nonEmpty,
        "a resumed consumer must be served the window it had not acknowledged")
      assert(replayed.forall(_ > acknowledgedThrough),
        s"and must not be served again what it acknowledged before it vanished: the cursor that " +
          s"stops that is the one the identity carries, but the replay covered " +
          s"${replayed.mkString(", ")} against an acknowledgement through $acknowledgedThrough")
      assert(delivered.exists(_ <= acknowledgedThrough),
        "the case is only meaningful if the first session was in fact sent blocks at or below " +
          "the acknowledged position, so that withholding them is an observable decision")
      assert(replayed === replayed.sorted,
        s"and the replay must arrive in sequence order, but arrived as ${replayed.mkString(", ")}")
      resumed.crash()
    }
  }

  test("a multi block gap is repaired by one request inside a single retry episode") {
    val clock = newManualClock()
    withHarness(newHarness(numPartitions = DegradationPartitions,
        executorMemoryBytes = DegradationExecutorMemoryBytes, clock = clock)) { harness =>
      val consumer = new ConsumerChannel(harness.serverHandler, consumerId)
      consumer.subscribe(partitionId = 0)
      harness.writer.write(deterministicRecords(StreamedRecordsPerConsumerCase, seed = 84L,
        keySpace = 8).iterator)
      val delivered = consumer.drainInboundBlocks().map(_.sequenceNumber())
      assert(delivered.size >= 3,
        s"this case needs a gap of at least two positions inside a longer stream, but only " +
          s"${delivered.size} block(s) were delivered")
      val gap = delivered.slice(1, 4)
      assert(gap.size >= 2 && gap === (gap.head to gap.last).toSeq,
        s"the gap must be a contiguous run of at least two positions, but was " +
          s"${gap.mkString(", ")}")

      val before = harness.serverHandler.retransmittedBlockCount
      consumer.deliver(new RetransmitRequestMessage(
        harness.shuffleId, defaultMapId, 0, gap.head, gap.last))
      harness.serverHandler.flushPending()
      val replayed = consumer.drainInboundBlocks().map(_.sequenceNumber())
      assert(gap.forall(replayed.contains),
        s"every position of the run must be replayed by the one request that named it, but " +
          s"${gap.mkString(", ")} was answered with ${replayed.mkString(", ")}")
      assert(harness.serverHandler.retransmittedBlockCount - before >= gap.size.toLong,
        s"the producer must count a replay per block it re-queued, but its count moved from " +
          s"$before to ${harness.serverHandler.retransmittedBlockCount} for a run of " +
          s"${gap.size} block(s)")
      assert(harness.errorNotifier.error.isEmpty,
        s"a repair the producer could serve must not escalate, yet it latched " +
          s"${harness.errorNotifier.error.map(_.getMessage).getOrElse("")}")

      assert(harness.serverHandler.retransmit(0, gap.head, gap.last) === 0,
        "a second request inside the backoff the run armed must be deferred rather than serviced")
      clock.advance(RetryBaseBackoffMillis)
      assert(harness.serverHandler.retransmit(0, gap.head, gap.last) > 0,
        s"the run must still hold its remaining budget once the ${RetryBaseBackoffMillis} ms " +
          "backoff has elapsed, because a repair spends one attempt however many blocks it covers")
      assert(harness.errorNotifier.error.isEmpty,
        "and the budget must not have been exhausted by a single multi block repair")
      consumer.crash()
    }
  }

  test("replay is paced on the one second ladder and escalates after five attempts") {
    // The retry budget and its pacing, driven through the producer that owns them rather than
    // restated from a constant.
    val clock = newManualClock()
    withHarness(newHarness(numPartitions = DegradationPartitions,
        executorMemoryBytes = DegradationExecutorMemoryBytes, clock = clock)) { harness =>
      val consumer = new ConsumerChannel(harness.serverHandler, consumerId)
      consumer.subscribe(partitionId = 0)
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
    val clock = newManualClock()
    withHarness(newHarness(clock = clock)) { harness =>
      harness.spillManager.registerPartitionCount(defaultPartitions)
      assert(harness.blockResolver.registerProducer(harness.shuffleId, defaultMapId,
          defaultTaskAttemptId, harness.spillManager),
        "the fixture must be able to publish its retained output before offering a block")
      val priority = StreamingShuffleServerHandler.EgressPriority(
        stageId = 0, stageAttemptNumber = 0, taskAttemptId = defaultTaskAttemptId,
        attemptNumber = 0)

      def deliverOneBlockTo(identity: String): (ConsumerChannel, Long) = {
        val consumer = new ConsumerChannel(harness.serverHandler, identity)
        consumer.subscribe(partitionId = 0)
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

      val (secondStalled, _) = deliverOneBlockTo(s"$consumerId-stalled-again")
      assert(harness.serverHandler.unacknowledgedBytes > 0L,
        "the second stalled session must also owe bytes when it closes")
      secondStalled.crash()
      assert(harness.serverHandler.lossyChannelClosureCount === 2L,
        "every lost channel must be counted even though only some are logged individually, but " +
          s"the producer counted ${harness.serverHandler.lossyChannelClosureCount}")

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
    // its buffers would pass all of them.
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
        fixture.spillManager.registerPartitionCount(UnframeablePartitions)
        assert(fixture.writer.blockPayloadCapacityBytes === 0,
          s"the fixture's budget of ${fixture.spillManager.totalBudgetBytes} bytes across " +
            s"$UnframeablePartitions partitions must leave no room for a block, but the writer " +
            s"derived a capacity of ${fixture.writer.blockPayloadCapacityBytes} bytes")
      })

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
    // The other arm of the same reservation.
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
      // What is left free is strictly less than the derived envelope and strictly more than the
      // halved one, which is the only band in which this case says what it means to say.
      val blockHeadroom = expectedEnvelope * 3 / 4
      val committed = fixture.spillManager.totalBudgetBytes - expectedEnvelope - blockHeadroom
      assert(committed > 0L, "the fixture must have budget left to commit")
      assert(blockHeadroom < expectedEnvelope,
        s"the free space of ${expectedEnvelope + blockHeadroom} bytes must stay below the " +
          s"derived envelope of ${derivedCapacity.toLong * defaultPartitions} bytes, or the " +
          s"derived reservation would simply be taken and no halving would be forced")
      assert(fixture.spillManager.reserveScratch(committed),
        "the fixture must be able to commit all but the halved envelope and the block headroom")

      val records = deterministicRecords(PreflightRecords, seed = 94L, keySpace = 16)
      var envelopeHeldAtFirstRecord = 0L
      val observing = records.iterator.map { record =>
        if (envelopeHeldAtFirstRecord == 0L) {
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
      assert(fixture.spillManager.durableAdmissionCount === 0L,
        s"and the halved envelope must leave room to buffer blocks, or the refusals below would " +
          s"be genuine memory pressure rather than none; ${
            fixture.spillManager.durableAdmissionCount} block(s) took the disk route")
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

  test("a block the allowance cannot hold reaches disk and then stands the shuffle down") {
    // What a full buffer allowance costs, and what it must NOT cost.
    val directDiskDelegate = new WorkingSortShuffleDelegate
    val controlRecords = deterministicRecords(DirectDiskRecords, seed = 96L, keySpace = 8)
    var expectedBlocks = 0L
    withHarness(newHarness(numPartitions = 1,
        executorMemoryBytes = DegradationExecutorMemoryBytes)) { control =>
      control.writer.write(controlRecords.iterator)
      control.writer.stop(success = true)
      expectedBlocks = control.writer.blocksStreamed
      assert(control.writer.blocksStreamed > 1L,
        s"the control fixture must cut more than one block, or the fixture below is a " +
          s"statement about a single admission; it cut ${control.writer.blocksStreamed}")
      assert(control.spillManager.durableAdmissionCount === 0L,
        s"an unconstrained allowance must admit every block to memory, but " +
          s"${control.spillManager.durableAdmissionCount} went straight to disk")
      assert(!control.fallbackPolicy.hasTripped,
        s"and it must not trip anything, or the trip below would say nothing about the refusal; " +
          s"it tripped with ${control.fallbackPolicy.trippedReason}")
    }

    val delegate = new RecordingSortShuffleWriter(1)
    withHarness(newHarness(numPartitions = 1,
        executorMemoryBytes = DegradationExecutorMemoryBytes,
        sortWriterFactory = Some(() => delegate))) { fixture =>
      var reservedBytes = 0L
      val records = deterministicRecords(DirectDiskRecords, seed = 96L, keySpace = 8)
      val constraining = records.iterator.map { record =>
        if (reservedBytes == 0L) {
          assert(fixture.spillManager.bufferedBytes === 0L,
            "nothing may be buffered yet, or eviction could free room and the refusal under test " +
              "would be a delay instead")
          // Headroom for the metadata a durably retained record is charged, and load bearing: the
          // allowance bounds the bookkeeping of a spilled block as well as the bytes of a buffered
          // one, so a fixture that takes the allowance to the last byte starves the disk route of
          // the little it needs and the block reaches neither memory nor disk.
          reservedBytes = fixture.reserveRemainingAllowance(
            64L * MemorySpillManager.SPILLED_RECORD_METADATA_BYTES)
          assert(reservedBytes > 0L, "the fixture must have allowance left to commit")
        }
        record
      }
      val status = try {
        fixture.writer.write(constraining)
        fixture.writer.stop(success = true)
      } finally {
        if (reservedBytes > 0L) {
          fixture.quota.release(reservedBytes)
        }
      }

      assert(fixture.spillManager.durableAdmissionCount > 0L,
        s"a block the allowance cannot hold must be written straight to local disk, but " +
          s"${fixture.spillManager.durableAdmissionCount} were")
      assert(fixture.writer.admissionRetryCount >=
          StreamingShuffleWriter.MAX_ADMISSION_RECOVERY_ROUNDS.toLong *
            fixture.spillManager.durableAdmissionCount,
        s"each refused admission must exhaust its ${
          StreamingShuffleWriter.MAX_ADMISSION_RECOVERY_ROUNDS} recovery round(s) before going " +
          s"to disk, but only ${fixture.writer.admissionRetryCount} retries were spent over " +
          s"${fixture.spillManager.durableAdmissionCount} admission(s)")
      assert(fixture.writer.memoryPressureBrushCount === 0L,
        s"nothing was rescued by eviction, so nothing may be counted as a brush with pressure, " +
          s"but ${fixture.writer.memoryPressureBrushCount} were")

      // Every spill event this fixture counted is a direct admission and nothing else: no eviction
      // ran, because there was never anything resident to evict.
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
      assert(fixture.backpressure.durableSpillAdmissionCount ===
          fixture.spillManager.durableAdmissionCount,
        s"every block that took the disk route must be visible in the backpressure telemetry an " +
          s"operator consults, but it counted " +
          s"${fixture.backpressure.durableSpillAdmissionCount} against " +
          s"${fixture.spillManager.durableAdmissionCount} admission(s)")

      assert(fixture.fallbackPolicy.hasTripped,
        "a reservation still refused after every recovery round must trip the fallback policy")
      assert(fixture.fallbackPolicy.trippedReason
          .contains(StreamingShuffleFallbackReason.MemoryPressure),
        s"and it must trip as memory pressure, but it tripped as " +
          s"${fixture.fallbackPolicy.trippedReason}")
      assert(fixture.gateway.declaredFallbacks.contains(
          StreamingShuffleFallbackReason.MemoryPressure),
        s"the trip must be declared shuffle-wide so every participant stands down, but the " +
          s"gateway saw ${fixture.gateway.declaredFallbacks.mkString(", ")}")

      assert(delegate.writeCallCount === 1,
        s"the attempt must be finished by exactly one sort-based write, but the delegate was " +
          s"written to ${delegate.writeCallCount} time(s)")
      assert(delegate.recordsWritten.groupBy(identity).map(entry => (entry._1, entry._2.size)) ===
          records.groupBy(identity).map(entry => (entry._1, entry._2.size)),
        "the sort-based writer must receive this map task's input exactly, every duplicate " +
          "preserved: a record lost or repeated here is a data loss no reduce task could detect")
      assert(status.isDefined,
        "a degraded attempt must still report a map status for the write path's dereference")
      assert(fixture.errorNotifier.error.isEmpty,
        "and standing down is not a failure, so nothing may have been raised")
    }
  }

  test("a stand down that cannot be agreed fails the attempt instead of writing it twice") {
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
    withHarness(newHarness(numPartitions = 1,
        executorMemoryBytes = 4L * 1024L * 1024L)) { harness =>
      val consumer = attachConsumer(harness)
      try {
        consumer.subscribe(harness.shuffleId, defaultMapId, partitionId = 0)
        val firstDeliveryOrder = mutable.ArrayBuffer.empty[Long]
        val seen = mutable.HashSet.empty[Long]
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
            consumer.retransmit(harness.shuffleId, defaultMapId, 0, highest)
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
        val ordered = deterministicRecords(30000, seed = 71L, keySpace = 40)
        val records = ordered.iterator.map { record =>
          collectOutbound()
          val highest = harness.writer.blocksStreamed - 1L
          if (highest > 0L) {
            consumer.retransmit(harness.shuffleId, defaultMapId, 0, highest)
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
    val delegate = new RecordingSortShuffleWriter(DegradationPartitions)
    val harness = newHarness(
      numPartitions = DegradationPartitions,
      executorMemoryBytes = DegradationExecutorMemoryBytes,
      spillThreshold = 50,
      sortWriterFactory = Some(() => delegate))
    withHarness(harness) { fixture =>
      val records = deterministicRecords(DegradationRecords, seed = 71L, keySpace = 64)
      var reservedBytes = 0L
      val squeezing = records.iterator.map { record =>
        if (reservedBytes == 0L && fixture.spillManager.memoryBytesSpilled > 0L) {
          fixture.spillManager.spillAllRetained()
          reservedBytes = fixture.reserveRemainingAllowance(
            64L * MemorySpillManager.SPILLED_RECORD_METADATA_BYTES)
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
    val delegate = new RecordingSortShuffleWriter(DegradationPartitions)
    val harness = newHarness(
      numPartitions = DegradationPartitions,
      executorMemoryBytes = DegradationExecutorMemoryBytes,
      sortWriterFactory = Some(() => delegate))
    withHarness(harness) { fixture =>
      val records = deterministicRecords(DegradationRecords, seed = 71L, keySpace = 64)
      // The trip is applied from inside the iterator, after the first record has been consumed, so
      // that it is observed while production is under way rather than before it starts.
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

      assert(fixture.metrics.bytesWritten === 0L,
        s"a degraded attempt must publish none of the bytes it streamed, but published " +
          s"${fixture.metrics.bytesWritten}; the sort delegate accounts for the output that exists")
      assert(fixture.metrics.recordsWritten === 0L,
        s"a degraded attempt must publish none of the records it streamed, but published " +
          s"${fixture.metrics.recordsWritten}")
    }
  }

  test("a runtime stand down after a live consumer acknowledged a block invalidates instead") {
    // The case the in-place rewrite above CANNOT serve, and the reason it is proved impossible
    // before the sort-based writer is ever built.
    val delegate = new RecordingSortShuffleWriter(DegradationPartitions)
    val harness = newHarness(
      numPartitions = DegradationPartitions,
      executorMemoryBytes = DegradationExecutorMemoryBytes,
      sortWriterFactory = Some(() => delegate))
    withHarness(harness) { fixture =>
      val consumer = attachConsumer(fixture)
      try {
        consumer.subscribe(fixture.shuffleId, defaultMapId, partitionId = 0)
        val refusal = intercept[SparkException] {
          fixture.writer.write(
            acknowledgeThenTrip(fixture, consumer, DegradationRecords, seed = 233L) { policy =>
              policy.recordLinkUtilization(
                usedBytesPerSecond = 990.0d, capacityBytesPerSecond = 1000.0d)
              assert(policy.trippedReason.contains(
                  StreamingShuffleFallbackReason.NetworkSaturation),
                s"link saturation must trip NetworkSaturation, but the policy reported " +
                  s"${policy.trippedReason}")
            })
        }

        assert(refusal.getMessage.contains("is no longer retained"),
          s"the refusal must name the block it cannot replay rather than read as a generic " +
            s"failure, but was: ${refusal.getMessage}")
        assert(delegate.writeCallCount === 0,
          s"and nothing may have been written through the sort-based delegate, because a rewrite " +
            s"that cannot be completed must not be started, yet it was written to " +
            s"${delegate.writeCallCount} time(s)")
        assert(fixture.gateway.declaredFallbacks.contains(
            StreamingShuffleFallbackReason.NetworkSaturation),
          s"the verdict must still be latched shuffle-wide so the recomputation is served by the " +
            s"sort-based path, but the gateway saw ${fixture.gateway.declaredFallbacks.mkString}")
        assert(fixture.gateway.invalidations.contains(
            StreamingShuffleInvalidationReason.IncompleteStream),
          s"and the generation must be withdrawn so the consumer's partial consumption is " +
            s"invalidated, but the gateway saw ${fixture.gateway.invalidations.mkString}")
        assert(!fixture.serverHandler.servesRetainedOutput,
          "the withdrawn generation must serve nothing further")
        assert(fixture.metrics.bytesWritten === 0L && fixture.metrics.recordsWritten === 0L,
          "and an attempt whose output is recomputed must publish none of what it streamed")
      } finally {
        consumer.close()
      }
    }
  }

  test("the same invalidation happens when a live consumer's slowness is what stands it down") {
    // The second runtime condition, driven the way the policy documents it: a consumer sustained at
    // least twice as slow as its producer, continuously for longer than the window.
    val delegate = new RecordingSortShuffleWriter(DegradationPartitions)
    val harness = newHarness(
      numPartitions = DegradationPartitions,
      executorMemoryBytes = DegradationExecutorMemoryBytes,
      sortWriterFactory = Some(() => delegate))
    withHarness(harness) { fixture =>
      val consumer = attachConsumer(fixture)
      try {
        consumer.subscribe(fixture.shuffleId, defaultMapId, partitionId = 0)
        val armedAt = fixture.clock.getTimeMillis()
        val refusal = intercept[SparkException] {
          fixture.writer.write(
            acknowledgeThenTrip(fixture, consumer, DegradationRecords, seed = 234L) { policy =>
              policy.recordProducerThroughput(fixture.shuffleId, 1000.0d, armedAt)
              policy.recordConsumerThroughput(fixture.shuffleId, 400.0d, armedAt)
              policy.recordConsumerThroughput(fixture.shuffleId, 400.0d,
                armedAt + StreamingShuffleFallbackPolicy.SUSTAINED_SLOWNESS_WINDOW_MS + 1L)
              assert(policy.trippedReason.contains(
                  StreamingShuffleFallbackReason.ConsumerTooSlow),
                s"a deficit held past the window must trip ConsumerTooSlow, but the policy " +
                  s"reported ${policy.trippedReason}")
            })
        }

        assert(refusal.getMessage.contains("is no longer retained"),
          s"the refusal must name the block it cannot replay, but was: ${refusal.getMessage}")
        assert(delegate.writeCallCount === 0,
          "and no partial map output may have been written through the sort-based delegate")
        assert(fixture.gateway.declaredFallbacks.contains(
            StreamingShuffleFallbackReason.ConsumerTooSlow),
          s"the consumer-slowness verdict must be latched shuffle-wide, but the gateway saw " +
            s"${fixture.gateway.declaredFallbacks.mkString}")
        assert(fixture.gateway.invalidations.contains(
            StreamingShuffleInvalidationReason.IncompleteStream),
          "and the generation must be withdrawn before the attempt fails")
      } finally {
        consumer.close()
      }
    }
  }

  test("a runtime stand down that cannot be agreed fails rather than rewriting through sort") {
    val delegate = new RecordingSortShuffleWriter(DegradationPartitions)
    val refusing = new RecordingCoordinatorGateway(refuseStandDown = true)
    val harness = newHarness(
      numPartitions = DegradationPartitions,
      executorMemoryBytes = DegradationExecutorMemoryBytes,
      sortWriterFactory = Some(() => delegate),
      registration = RegistrationFixture(gateway = refusing))
    withHarness(harness) { fixture =>
      val records = deterministicRecords(DegradationRecords, seed = 73L, keySpace = 64)
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

      val refused = intercept[SparkException](fixture.writer.write(tripping))

      assert(refused.getMessage.contains("could not be stood down for every participant"),
        s"the failure must name what could not be agreed rather than reading as a streaming " +
          s"fault, but was '${refused.getMessage}'")
      assert(refusing.declaredFallbacks ===
          Seq(StreamingShuffleFallbackReason.NetworkSaturation),
        s"the stand-down must have been attempted exactly once, and for the condition actually " +
          s"observed, but the gateway saw ${refusing.declaredFallbacks.mkString(", ")}")
      assert(delegate.writeCallCount === 0,
        s"the sort-based delegate must not have written anything while the rest of the shuffle " +
          s"streams, but it was written to ${delegate.writeCallCount} time(s)")
      assert(!fixture.fallbackPolicy.shuffleHasFallenBack(fixture.shuffleId),
        "no shuffle-wide verdict may be cached from a declaration that did not take effect, or " +
          "the retry would delegate on the strength of a decision nobody agreed to")

      assert(fixture.writer.stop(success = false).isEmpty,
        "a failed attempt reports no map status")
      assert(fixture.spillManager.bufferedBytes === 0L && fixture.spillManager.scratchBytes === 0L,
        s"the failed attempt must hold no buffer and no framing scratch, but held " +
          s"${fixture.spillManager.bufferedBytes} and ${fixture.spillManager.scratchBytes} bytes")
    }
  }

  test("a runtime stand down releases every buffer and spill file it had retained") {
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
    // the real egress path rather than through the ledger APIs the two of them use.
    val clock = newManualClock()
    val startedAtMillis = clock.getTimeMillis()
    withHarness(newHarness(
        numPartitions = 1,
        clock = clock,
        executorMemoryBytes = 256L * 1024L,
        spillThreshold = 50)) { harness =>
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
        if (ticks % 10 == 0) {
          consumer.subscribe(harness.shuffleId, defaultMapId, 0, nextPosition = 0L)
        }
        consumer.drainBlocks()
      }
      val paced = deterministicRecords(20000, seed = 12L, keySpace = 32).iterator.zipWithIndex
        .map { case (record, index) =>
          if (index % 128 == 0) {
            tick()
          }
          record: Product2[Int, Int]
        }

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

      assert(harness.writer.consumerStallCount === 1L,
        s"Exactly one stall must be reported, but ${harness.writer.consumerStallCount} were")
      assert(harness.writer.replayAttemptCount === MaxRetryAttempts.toLong,
        s"Exactly $MaxRetryAttempts replay attempts must be made, but " +
          s"${harness.writer.replayAttemptCount} were")
      assert(harness.serverHandler.retransmittedBlockCount > 0L,
        "A replay must put blocks back on the consumer's channel")
      assert(harness.serverHandler.acknowledgedPosition(0) === -1L,
        "The lost consumer must have no acknowledged position at all")

      assert(harness.writer.spillsObserved > 0L,
        "The unacknowledged window must be spilled while the consumer is being waited for")
      val files = harness.spillFiles()
      assert(files.nonEmpty && files.forall(_.exists()),
        "The spilled window must be on disk when the escalation is raised")

      val committed = harness.writer.nextSequenceNumberFor(0)
      assert(committed > 0L, "The stalled stream must have committed blocks")
      assert(harness.spillManager.retainedBlockCount(0) === committed.toInt,
        s"All $committed committed block(s) must still be retained, but " +
          s"${harness.spillManager.retainedBlockCount(0)} are")

      assert(harness.errorNotifier.fetchFailure.isEmpty,
        "A producer must not construct a fetch failure")
      consumer.close()
    }
  }

  test("a reconnecting consumer is served the retained window from spill, not a recomputation") {
    val partitions = 2
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      val first = new ProducerConsumerChannel(harness.serverHandler, consumerId)
      first.activate()
      (0 until partitions).foreach(first.subscribe(harness.shuffleId, defaultMapId, _))
      harness.writer.write(deterministicRecords(2400, seed = 11L, keySpace = 24).iterator)

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

      first.close()
      assert(harness.serverHandler.sessionCount === 0,
        "The lost consumer's session must be dropped when its channel goes")
      delivered.foreach { case (partitionId, blocks) =>
        val retained = harness.spillManager.retainedBlockCount(partitionId)
        assert(retained === blocks.size,
          s"Partition $partitionId must still retain all ${blocks.size} unconfirmed block(s) but " +
            s"retains $retained")
      }

      assert(harness.spillManager.spillAllRetained() > 0L,
        "Making the unacknowledged window durable must move bytes to disk")
      val files = harness.spillFiles()
      assert(files.nonEmpty && files.forall(_.exists()),
        "The unacknowledged window must be on disk before the reconnection")
      deliveredPayloads.keys.foreach { case (partitionId, sequence) =>
        assert(harness.spillManager.spilledBlock(partitionId, sequence).isDefined,
          s"Block $sequence of partition $partitionId must be locatable on disk")
      }

      val reconnected = new ProducerConsumerChannel(harness.serverHandler, consumerId)
      reconnected.activate()
      (0 until partitions).foreach(
        reconnected.subscribe(harness.shuffleId, defaultMapId, _, nextPosition = 0L))
      assert(harness.serverHandler.sessionCount === 1,
        "The reconnecting consumer must hold exactly one session")
      assert(harness.serverHandler.resumedSessionCount >= 1L,
        "The producer must record that a known consumer resumed on a new channel")
      val replayed = drainAllBlocks(harness.serverHandler, reconnected, 0 until partitions)

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

      val acknowledgementsBefore = harness.serverHandler.ackCount
      val retainedCursor = harness.spillManager.registeredConsumers
      assert(retainedCursor.size === 1,
        s"The reconnecting task must own one stable retained cursor, but found $retainedCursor")
      replayed.foreach { case (partitionId, blocks) =>
        val lastSequence = blocks.map(_.sequenceNumber()).max
        reconnected.acknowledge(harness.shuffleId, defaultMapId, partitionId, lastSequence)
      }
      assert(harness.serverHandler.ackCount === acknowledgementsBefore + replayed.size,
        "Every replayed partition acknowledgement must be applied before final retirement")
      assert(harness.serverHandler.sessionCount === 0,
        "The returning consumer must be retired once every terminated partition is confirmed")
      assert(harness.spillManager.registeredConsumers.isEmpty,
        "Final completion must unregister the stable retained cursor")
      assert(reconnected.channel.isOpen,
        "Completing one producer route must not close the executor-scoped multiplexed channel")
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
        assert(StreamingShuffleServerHandler.TRANSPORT_MODULE_NAME === TransportModuleName,
          s"Streaming egress must be tuned under its own $TransportModuleName transport module")
      }
    }
  }

  test("flush order puts an original attempt on the wire ahead of a speculative one") {
    // The attributes asserted above are the INPUT to the ordering; this is the ordering itself,
    // observed as the sequence of frames that actually reached a consumer's channel.
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
      // every partition's stream and a terminated stream accepts no further block.
      assert(harness.blockResolver.registerProducer(
          harness.shuffleId, defaultMapId, defaultTaskAttemptId, spillManager),
        "The retained store must be registered, or the handler has nothing to frame blocks from")

      val consumer = attachConsumer(harness)
      try {
        consumer.subscribe(harness.shuffleId, defaultMapId, speculativePartition)
        consumer.subscribe(harness.shuffleId, defaultMapId, originalPartition)
        assert(harness.serverHandler.subscriptionCount === 2,
          "Both partitions must be subscribed on the one channel, so both compete for one queue")

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
    val clock = newManualClock()
    withHarness(newHarness(clock = clock, maxBandwidthMBps = Some(1))) { harness =>
      harness.spillManager.registerPartitionCount(defaultPartitions)
      assert(harness.blockResolver.registerProducer(harness.shuffleId, defaultMapId,
          defaultTaskAttemptId, harness.spillManager),
        "the fixture must be able to publish its retained output before offering a block")
      val consumer = new ConsumerChannel(harness.serverHandler, consumerId)
      consumer.subscribe(partitionId = 0)
      consumer.subscribe(partitionId = 1)
      consumer.drainInbound()

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
    val delegate = new RecordingStopSortWriter
    withHarness(newHarness(delegate = Some(delegate))) { harness =>
      val writer = harness.writer
      harness.fallbackPolicy.recordAllocationGrant(1024L, 0L)
      assert(harness.fallbackPolicy.hasTripped,
        "the fixture must have tripped the policy, or the writer would simply stream")
      assert(!harness.spillManager.isClosed, "and the attempt must still hold its buffer state")

      writer.write(Iterator((1, 10), (2, 20), (3, 30)))

      assert(delegate.writtenRecords === 3,
        s"every record must reach the sort-based writer, but it received " +
          s"${delegate.writtenRecords}")
      assert(harness.gateway.declaredFallbacks.nonEmpty,
        "and the shuffle must have stood streaming down for every participant first, because one " +
          "sort-written map output among streaming siblings is unreadable")

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

  test("an unevictable refusal reaches disk, is reported everywhere, and stands the shuffle down") {
    val executorMemory = 1024L * 1024L
    val delegate = new RecordingSortShuffleWriter(2)
    withHarness(newHarness(numPartitions = 2, executorMemoryBytes = executorMemory,
        bufferSizePercent = 4, sortWriterFactory = Some(() => delegate))) { harness =>
      val spillManager = harness.spillManager
      val backpressure = harness.backpressure
      val recordBound = 400000
      var reservedBytes = 0L
      var offered = 0
      val records = new Iterator[Product2[Int, Int]] {
        override def hasNext: Boolean =
          offered < recordBound && spillManager.durableAdmissionCount == 0L

        override def next(): Product2[Int, Int] = {
          if (offered == 0) {
            // Taken after the writer's own framing reservation, which is made before the first
            // record is consumed, so the producer is fully committed to streaming by the time the
            // allowance runs out.
            reservedBytes = harness.reserveRemainingAllowance(
              8L * MemorySpillManager.SPILLED_RECORD_METADATA_BYTES)
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
      assert(spillManager.diskBytesSpilled > 0L,
        "and the retained bytes must be accounted on the existing disk-spill accumulator, since " +
          "a durably retained block really did reach local disk")

      assert(backpressure.durableSpillAdmissionCount >= spillManager.durableAdmissionCount,
        s"every durable admission must be reported to the protocol, but it saw " +
          s"${backpressure.durableSpillAdmissionCount} and the store made " +
          s"${spillManager.durableAdmissionCount}")
      // A block that can reach NEITHER memory nor the disk route is something else, and this
      // fixture ends there by construction: it holds the allowance to the byte, and the allowance
      // bounds a retained record's own bookkeeping as well as a buffered block's bytes, so once the
      // headroom above has been spent on retained records there is nowhere left to put a block.
      assert(backpressure.durableSpillAdmissionCount === spillManager.durableAdmissionCount + 1L,
        s"the refusal that ended the attempt must account for exactly one reported occurrence " +
          s"beyond the ${spillManager.durableAdmissionCount} admitted, but the protocol saw " +
          s"${backpressure.durableSpillAdmissionCount}")
      assert(backpressure.degradationReasons ===
          Seq(BackpressureDegradationReason.BufferAllocationFailure),
        s"the unanswerable refusal must latch exactly the allocation failure, but the protocol " +
          s"holds ${backpressure.degradationReasons.mkString(", ")}")

      assert(harness.fallbackPolicy.hasTripped,
        "a refusal no eviction could have repaired must trip the fallback policy")
      assert(harness.fallbackPolicy.trippedReason
          .contains(StreamingShuffleFallbackReason.MemoryPressure),
        s"as memory pressure, which is trip condition two, but it tripped as " +
          s"${harness.fallbackPolicy.trippedReason}")
      assert(harness.fallbackPolicy.shouldDelegateToSortShuffle,
        "so the shuffle must now be routed to the sort-based writer, which is unmodified")
      assert(harness.gateway.declaredFallbacks.contains(
          StreamingShuffleFallbackReason.MemoryPressure),
        s"and the verdict must be announced to the coordinator, but it announced " +
          s"${harness.gateway.declaredFallbacks.mkString(", ")}")
      assert(harness.fallbackPolicy.shuffleHasFallenBack(harness.shuffleId),
        "and be readable locally as this shuffle's shuffle-wide verdict")
      assert(harness.routes.withdrawals === Seq((harness.shuffleId, defaultMapId)),
        s"this generation must be withdrawn exactly once, because a consumer must not resolve an " +
          s"address for output the executor has stopped streaming, but the registry recorded " +
          s"${harness.routes.withdrawals}")

      // Degradation, not failure -- and on this fixture it is the withdraw-and-recompute form of it
      // rather than the in-place rewrite, because the rewrite has a precondition this fixture
      // deliberately removes.
      assert(delegate.writeCallCount === 0,
        s"an allowance that cannot read the framed blocks back must not be rewritten in place, " +
          s"but the delegate was written to ${delegate.writeCallCount} time(s)")
      val status = harness.writer.stop(success = true)
      assert(status.isDefined,
        "a stood-down attempt must still report a map status for the write path's dereference")
      assert((0 until 2).forall(partitionId => status.get.getSizeForBlock(partitionId) > 0L),
        s"and every declared partition must report a non-zero size, so no reducer skips the " +
          s"withdrawn output instead of asking for it and failing: sizes were " +
          s"${(0 until 2).map(status.get.getSizeForBlock).mkString(", ")}")
      assert(harness.errorNotifier.error.isEmpty,
        "and nothing may have been raised, because standing down is not a failure")
    }
  }

  test("a hostile consumer channel cannot forge progress, rename a peer or fan a partition out") {
    // The producer's inbound surface is reachable by anything that can open a socket to this
    // executor, so every frame arriving on it is untrusted.
    val partitions = 2
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      val handler = harness.serverHandler

      // 0.
      val unauthenticatedChannel = new EmbeddedChannel(DefaultChannelId.newInstance())
      val unauthenticatedClient = new TransportClient(
        unauthenticatedChannel, new TransportResponseHandler(unauthenticatedChannel))
      handler.receive(unauthenticatedClient,
        new HeartbeatMessage(harness.shuffleId, defaultMapId, 0, 0L,
          BackpressureStreamKey.consumerTokenOf(consumerId)).toByteBuffer())
      assert(handler.awaitDataPlaneIdle(10000L),
        "The producer data plane must settle after refusing an unauthenticated frame")
      assert(handler.unauthenticatedRefusalCount === 1L,
        "The producer must count a frame refused for lacking an authenticated transport principal")
      assert(handler.sessionCount === 0 && handler.subscriptionCount === 0,
        "An unauthenticated heartbeat must allocate neither a session nor a subscription")
      assert(!unauthenticatedClient.isActive(),
        "The producer must close a channel that attempts to create an unauthenticated session")
      unauthenticatedChannel.finishAndReleaseAll()

      val honest = new ProducerConsumerChannel(handler, consumerId)
      honest.activate()
      honest.subscribe(harness.shuffleId, defaultMapId, 0)
      harness.writer.write(deterministicRecords(2400, seed = 14L, keySpace = 24).iterator)
      val delivered = drainAllBlocks(handler, honest, Seq(0))(0)
      assert(delivered.nonEmpty, "The honest consumer must have been sent blocks of partition 0")
      val highestSent = delivered.map(_.sequenceNumber()).max

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
      assert(harness.errorNotifier.error.exists(
          _.getMessage.contains("STREAMING_SHUFFLE_INVALID_SEQUENCE_NUMBER")),
        "A forged acknowledgement must be reported as an invalid sequence number, but the " +
          s"notifier holds ${harness.errorNotifier.error.map(_.getMessage)}")
      forger.close()

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

      val renamer = new ProducerConsumerChannel(handler, s"$consumerId-renamer")
      renamer.activate()
      renamer.subscribe(harness.shuffleId, defaultMapId, 1)
      assert(handler.subscriberCount(1) === 1,
        "The channel must hold a subscription before a further frame is delivered on it")
      assert(handler.consumerIdentities.contains(renamer.declaredIdentity),
        s"The declared identity must have been adopted, but the producer holds " +
          s"${handler.consumerIdentities.mkString(", ")}")
      renamer.drainBlocks()
      val sessionsBefore = handler.sessionCount
      renamer.subscribe(harness.shuffleId, defaultMapId, 1)
      renamer.deliver(new HeartbeatMessage(harness.shuffleId, defaultMapId, 1, 0L))
      assert(handler.sessionCount === sessionsBefore,
        "A further heartbeat must refresh the session that sent it rather than creating or " +
          "replacing one")
      assert(handler.subscriberCount(1) === 1,
        s"and must leave that session's subscription exactly where it was, but " +
          s"${handler.subscriberCount(1)} are claimed")
      assert(handler.renameRefusalCount === 0L,
        "and neither repeating an identity nor omitting one is a rename")

      // 3b.
      val renameRefusalsBefore = handler.renameRefusalCount
      renamer.deliver(new HeartbeatMessage(harness.shuffleId, defaultMapId, 1, 0L,
        BackpressureStreamKey.consumerTokenOf(s"$consumerId-someone-else")))
      assert(handler.renameRefusalCount === renameRefusalsBefore + 1L,
        "A second identity on a live session must be refused")
      assert(handler.subscriberCount(1) === 0,
        s"and the refused session's subscriber slot must come back with it, but " +
          s"${handler.subscriberCount(1)} remain claimed")
      assert(!handler.consumerIdentities.contains(renamer.declaredIdentity),
        "and the session must be gone from the identity index")
      renamer.close()

      val incumbent = new ProducerConsumerChannel(handler, s"$consumerId-incumbent")
      incumbent.activate()
      incumbent.subscribe(harness.shuffleId, defaultMapId, 1)
      incumbent.drainBlocks()
      val incumbentSessions = handler.sessionCount
      val supersededBefore = handler.supersededSessionCount
      val impostor = new ProducerConsumerChannel(handler, s"$consumerId-incumbent")
      impostor.activate()
      impostor.subscribe(harness.shuffleId, defaultMapId, 1)
      assert(handler.supersededSessionCount === supersededBefore,
        "A live consumer may not be superseded by a peer that merely names it")
      assert(handler.sessionCount === incumbentSessions,
        s"and the impostor's session must be gone rather than added, but the producer holds " +
          s"${handler.sessionCount} session(s) against $incumbentSessions before it arrived")
      assert(handler.consumerIdentities.contains(incumbent.declaredIdentity),
        "and the incumbent must still hold its identity")
      assert(handler.subscriberCount(1) === 1,
        s"and must still hold exactly its own subscription, but " +
          s"${handler.subscriberCount(1)} are claimed")
      assert(handler.isConsumerStalled(1) === false,
        "and must not have been left in a state that looks like a stall")
      incumbent.close()
      assert(handler.subscriberCount(1) === 0,
        s"The incumbent's subscriber slot must be returned when it goes, but " +
          s"${handler.subscriberCount(1)} remain claimed")
      assert(!renamer.channel.isOpen,
        "An identity-conflicting channel must be closed before it can send another frame")
      renamer.close()

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
      // session, and the slot ledger must agree with the registry exactly.
      assert(handler.claimedSessionSlots === handler.sessionCount,
        s"${handler.claimedSessionSlots} session slot(s) are claimed for ${handler.sessionCount} " +
          "live session(s)")
      crowd.foreach(_.close())
      assert(handler.subscriberCount(1) === 0,
        "Every subscriber slot of partition 1 must be returned when its channel goes, but " +
          s"${handler.subscriberCount(1)} remain claimed")
      assert(handler.claimedSessionSlots === handler.sessionCount,
        "Every session slot must be returned when its channel goes")

      assert(!handler.versionMismatchDetected,
        "No version mismatch may have been seen before one is delivered")
      val alien = new ProducerConsumerChannel(handler, s"$consumerId-alien")
      alien.activate()
      val sessionsBeforeAlien = handler.sessionCount
      val slotsBeforeAlien = handler.claimedSessionSlots
      val framed =
        new HeartbeatMessage(
          harness.shuffleId, defaultMapId, 0, 0L, alien.consumerToken).toByteBuffer()
      val alienBytes = new Array[Byte](framed.remaining())
      framed.duplicate().get(alienBytes)
      alienBytes(FrameTypePrefixLength) = (ProtocolVersion + 7).toByte
      alien.deliverRaw(alienBytes)
      assert(handler.versionMismatchDetected,
        "A frame carrying an unsupported protocol version must be detected as a mismatch")
      assert(harness.fallbackPolicy.hasTripped,
        "A protocol version mismatch must trip the fallback policy the manager and the writer " +
          "consult, not merely set a flag on the handler")
      assert(harness.fallbackPolicy.trippedReason
          .contains(StreamingShuffleFallbackReason.ProtocolVersionMismatch),
        s"and it must trip as the compatibility failure it is, but tripped as " +
          s"${harness.fallbackPolicy.trippedReason}")
      assert(harness.fallbackPolicy.shouldDelegateToSortShuffle,
        "so that the sort-based implementation is where this shuffle goes next")
      assert(handler.sessionCount === sessionsBeforeAlien,
        "A frame refused on its version must not allocate a session")
      assert(handler.claimedSessionSlots === slotsBeforeAlien,
        "A frame refused on its version must not claim a session slot")
      assert(alien.drainBlocks().isEmpty,
        "A channel whose frame was refused on its version must not be served a block")
      alien.close()

      val acknowledgementsBefore = handler.ackCount
      val refusalsBeforeHonestAck = handler.refusedAckCount
      val lossyClosuresBefore = handler.lossyChannelClosureCount
      honest.acknowledge(harness.shuffleId, defaultMapId, 0, highestSent)
      assert(handler.ackCount === acknowledgementsBefore + 1L,
        s"The honest consumer's acknowledgement through $highestSent must be applied, but " +
          s"${handler.ackCount - acknowledgementsBefore} were")
      assert(handler.refusedAckCount === refusalsBeforeHonestAck,
        s"and must not be refused alongside the forged ones, but " +
          s"${handler.refusedAckCount - refusalsBeforeHonestAck} further refusal(s) were counted")
      assert(handler.lossyChannelClosureCount === lossyClosuresBefore,
        "and the honest channel must not be counted lost with bytes owed, because it was owed " +
          "nothing by the time it went")
      assert(!harness.spillManager.registeredConsumers.contains(honest.declaredIdentity),
        s"and the cursor it held must be released with it, because a consumer owed nothing keeps " +
          s"nothing, yet the store still holds " +
          s"${harness.spillManager.registeredConsumers.mkString(", ")}")

      honest.close()
      val acceptedBeforeReturn = handler.acceptedSessionCount
      val subscribersBeforeReturn = handler.subscriberCount(0)
      val returning = new ProducerConsumerChannel(handler, consumerId)
      returning.activate()
      returning.subscribe(harness.shuffleId, defaultMapId, 0, nextPosition = highestSent + 1L)
      assert(handler.acceptedSessionCount === acceptedBeforeReturn + 1L,
        s"A consumer reconnecting on a new channel must be admitted rather than refused, but " +
          s"${handler.acceptedSessionCount - acceptedBeforeReturn} session(s) were accepted")
      // Admitted and then, in the same frame, finally retired: it announced a position past the
      // last block of a stream whose terminator has already been delivered, so it is owed nothing
      // and a consumer owed nothing keeps neither a session nor a subscription.
      assert(handler.sessionCount === 0,
        s"and a return that is already caught up must be retired rather than held, but the " +
          s"producer holds ${handler.sessionCount} session(s)")
      assert(handler.subscriberCount(0) === subscribersBeforeReturn,
        s"leaving no subscriber of partition 0 behind it, but ${handler.subscriberCount(0)} " +
          s"are claimed")
      assert(handler.claimedSessionSlots === handler.sessionCount,
        "Every live session must hold exactly one slot, and no more")
      returning.close()
      assert(handler.subscriberCount(0) === subscribersBeforeReturn,
        "and a late teardown of the retired channel must not disturb that")
    }
  }

  test("a channel arriving before publication is bounded and a retired generation admits none") {
    val partitions = 4
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      val handler = harness.serverHandler
      assert(handler.numPartitions === partitions,
        s"The handler's partition domain must be the handle's ${partitions} but was " +
          s"${handler.numPartitions}")
      assert(!handler.servesRetainedOutput,
        "This case is about the gap before publication, so no retained output may exist yet")

      val early = new ProducerConsumerChannel(handler, s"$consumerId-early")
      early.activate()
      val refusalsBefore = handler.subscriptionRefusalCount
      val misaddressedBefore = handler.misaddressedMessageCount

      val outOfRange = Seq(partitions, partitions + 1, 5000, Int.MaxValue)
      outOfRange.foreach { partitionId =>
        early.subscribe(harness.shuffleId, defaultMapId, partitionId)
        assert(handler.subscriberCount(partitionId) === 0,
          s"Partition $partitionId is outside the domain of $partitions, so it must hold no " +
            s"subscriber, but holds ${handler.subscriberCount(partitionId)}")
      }
      assert(handler.misaddressedMessageCount === misaddressedBefore + outOfRange.size,
        s"Every one of the ${outOfRange.size} out-of-range heartbeats must be counted as " +
          s"misaddressed, but the count moved by " +
          s"${handler.misaddressedMessageCount - misaddressedBefore}")
      assert(handler.subscriptionRefusalCount === refusalsBefore,
        "and none of them may reach the subscription path at all, because a frame for a stream " +
          "this handler does not serve is not a subscription that was refused")

      (0 until partitions).foreach { partitionId =>
        early.subscribe(harness.shuffleId, defaultMapId, partitionId)
        assert(handler.subscriberCount(partitionId) === 1,
          s"Partition $partitionId must admit the early consumer, but holds " +
            s"${handler.subscriberCount(partitionId)} subscriber(s)")
      }
      assert(handler.subscriptionRefusalCount === refusalsBefore,
        s"No legitimate subscription may be refused, but the refusal count moved by " +
          s"${handler.subscriptionRefusalCount - refusalsBefore}")
      assert(handler.subscriptionCount === partitions,
        s"Exactly $partitions subscriptions may exist across every session, but " +
          s"${handler.subscriptionCount} do")

      harness.writer.write(deterministicRecords(600, seed = 31L, keySpace = 12).iterator)
      assert(handler.servesRetainedOutput,
        "The producing task's first records must have published the retained output")
      assert(drainAllBlocks(handler, early, Seq(0))(0).nonEmpty,
        "The consumer that subscribed before publication must have been served the blocks of the " +
          "partition it subscribed to, with no further subscription frame")

      val misaddressedAfter = handler.misaddressedMessageCount
      early.subscribe(harness.shuffleId, defaultMapId, partitions)
      assert(handler.subscriberCount(partitions) === 0,
        "An out-of-range partition must still hold no subscriber after publication")
      assert(handler.misaddressedMessageCount === misaddressedAfter + 1L,
        "and the out-of-range heartbeat must still be counted as misaddressed")
      early.close()
      assert((0 until partitions).forall(handler.subscriberCount(_) == 0),
        "Every subscriber slot the channel held must be returned when it goes")

      assert(handler.withdrawGeneration("the case retires the generation deliberately"),
        "The generation must be withdrawn by this call, or step 4 asserts nothing")
      val late = new ProducerConsumerChannel(handler, s"$consumerId-late")
      late.activate()
      val refusalsBeforeLate = handler.subscriptionRefusalCount
      val misaddressedBeforeLate = handler.misaddressedMessageCount
      late.subscribe(harness.shuffleId, defaultMapId, 0)
      assert(handler.subscriberCount(0) === 0,
        s"A retired generation must admit no subscriber, but partition 0 holds " +
          s"${handler.subscriberCount(0)}")
      assert(handler.subscriptionRefusalCount === refusalsBeforeLate + 1L,
        "and the attempt must be counted as a refused subscription")
      assert(handler.misaddressedMessageCount === misaddressedBeforeLate,
        "and not as a misaddressed frame, because it named a partition this handler does serve")
      assert(late.drainBlocks().isEmpty,
        "and nothing may be served on its channel")
      late.close()
    }
  }

  test("a reconnect atomically migrates one logical consumer session and retained cursor") {
    withHarness(newHarness(numPartitions = 1)) { harness =>
      val handler = harness.serverHandler
      val first = new ProducerConsumerChannel(handler, consumerId)
      first.activate()
      first.subscribe(harness.shuffleId, defaultMapId, 0)
      assert(handler.sessionCount === 1 && handler.subscriberCount(0) === 1,
        "The first channel must own one bounded session and subscription")

      val sessionsBefore = handler.sessionCount
      val subscribersBefore = handler.subscriberCount(0)
      val slotsBefore = handler.claimedSessionSlots
      val supersededBefore = handler.supersededSessionCount
      val releasesBefore = harness.routes.participationReleases.size

      // The first consumer goes silent for a whole liveness window, its socket still open.
      harness.clock.asInstanceOf[ManualClock].advance(
        StreamingShuffleServerHandler.CONSUMER_LIVENESS_TIMEOUT_MS)
      assert(first.channel.isOpen,
        "the stale connection must still be open, or the supersession below would be closing a " +
          "channel that had already gone")

      val returning = new ProducerConsumerChannel(handler, consumerId)
      returning.activate()
      returning.subscribe(harness.shuffleId, defaultMapId, 0)

      assert(handler.supersededSessionCount === supersededBefore + 1L,
        "The replacement channel must supersede the stale session of the same logical task")
      assert(handler.sessionCount === sessionsBefore,
        "A reconnect must replace one session atomically rather than add a second session")
      assert(handler.subscriberCount(0) === subscribersBefore,
        "A reconnect must transfer the partition subscription rather than fan it out")
      assert(handler.claimedSessionSlots === slotsBefore,
        "A reconnect must transfer the existing session slot even when capacity is exact")
      assert(handler.liveConsumerCount === 1,
        "The stable logical identity must still name exactly one live consumer")
      // The stale connection is given up as this producer's participation, with `closeWhenLast`, so
      // the socket goes once no producer is serving it.
      assert(harness.routes.participationReleases.size > releasesBefore &&
          harness.routes.participationReleases.contains((harness.shuffleId, defaultMapId)),
        "Supersession must give up the stale channel as this producer's participation, leaving " +
          "the socket to close when its last producer leaves")

      harness.writer.write(deterministicRecords(600, seed = 19L, keySpace = 12).iterator)
      val delivered = drainAllBlocks(handler, returning, Seq(0))(0)
      assert(delivered.nonEmpty, "The replacement channel must receive the producer's output")
      assert(delivered.size <= MemorySpillManager.MAX_RECLAIMED_BLOCKS_PER_BATCH,
        "The fixture must fit in one bounded reclamation batch so immediate reclaim is observable")
      val highestSent = delivered.map(_.sequenceNumber()).max
      val cursorBefore = harness.spillManager.registeredConsumers
      assert(cursorBefore.size === 1,
        s"One logical task must own one retained cursor, but found ${cursorBefore.mkString(", ")}")
      val retainedCursor = cursorBefore.head
      assert(harness.spillManager.consumerPosition(retainedCursor, 0).isEmpty,
        "The first channel has not acknowledged anything yet")
      val retainedBefore = harness.spillManager.retainedBlockCount(0)
      assert(retainedBefore === delivered.size,
        s"Every delivered block must remain retained, but $retainedBefore of ${delivered.size} do")

      val releasesBeforeFinal = harness.routes.participationReleases.size
      harness.clock.asInstanceOf[ManualClock].advance(
        StreamingShuffleServerHandler.CONSUMER_LIVENESS_TIMEOUT_MS)

      val finalReturn = new ProducerConsumerChannel(handler, consumerId)
      finalReturn.activate()
      finalReturn.subscribe(harness.shuffleId, defaultMapId, 0, nextPosition = highestSent + 1L)

      assert(handler.supersededSessionCount === supersededBefore + 2L,
        "A caught-up reconnect must supersede the channel that delivered the retained window")
      assert(harness.routes.participationReleases.size > releasesBeforeFinal,
        "and must give up that channel as this producer's participation too")
      assert(handler.sessionCount === 0 && handler.liveConsumerCount === 0,
        "A caught-up consumer of a terminated stream must be finally retired")
      assert(handler.subscriberCount(0) === 0 && handler.claimedSessionSlots === 0,
        "Final retirement must return the transferred subscriber and session slots")
      assert(harness.spillManager.registeredConsumers.isEmpty,
        "The migrated cursor must be unregistered once no replay entitlement remains")
      assert(harness.spillManager.retainedBlockCount(0) === 0,
        "Migrating the final cursor must reclaim its confirmed in-memory prefix immediately")
      assert(harness.spillManager.pendingReclamationCount === 0,
        "A prefix within one bounded batch must not leave asynchronous reclamation behind")
      assert(finalReturn.drainBlocks().isEmpty,
        "A replacement already caught up through the retained window must receive no replay")

      first.close()
      returning.close()
      finalReturn.close()
      assert(handler.sessionCount === 0,
        "Late teardowns of superseded channels must not recreate or remove another session")
    }
  }

  test("logical consumer identities are scoped by the authenticated transport principal") {
    withHarness(newHarness(numPartitions = 2)) { harness =>
      val first = new ProducerConsumerChannel(
        harness.serverHandler, consumerId, authenticatedApplicationId)
      val otherApplication = new ProducerConsumerChannel(
        harness.serverHandler, consumerId, s"$authenticatedApplicationId-other")
      first.activate()
      otherApplication.activate()
      first.subscribe(harness.shuffleId, defaultMapId, 0)
      otherApplication.subscribe(harness.shuffleId, defaultMapId, 1)

      assert(harness.serverHandler.sessionCount === 2,
        "Two authenticated applications declaring the same task text must hold separate sessions")
      assert(harness.serverHandler.liveConsumerCount === 2,
        "The transport principal must be part of the logical cursor identity")
      assert(harness.serverHandler.supersededSessionCount === 0L,
        "A task identity from another authenticated application must never supersede this one")
      harness.writer.write(deterministicRecords(600, seed = 23L, keySpace = 2).iterator)
      assert(harness.spillManager.registeredConsumers.size === 2,
        "The retained store must keep principal-scoped cursor identities distinct")

      first.close()
      otherApplication.close()
    }
  }

  test("one executor quota bounds producer, consumer, transient and metadata allocations") {
    val frame = StreamingShuffleServerHandler.MAX_FRAMED_BYTES.toLong
    val aggregateCapacity = 4L * frame
    val quota = new MemorySpillManager.ExecutorBufferQuota(
      bufferSizePercent = 50,
      spillThresholdPercent = DefaultSpillThresholdPercent,
      executorMemoryProvider = () => 2L * aggregateCapacity)
    val clock = new ManualClock(ManualClockEpochMillis)
    val conf = streamingConf()
    val protocol = new BackpressureProtocol(
      conf,
      null,
      new TokenBucketRateLimiter.ExecutorEgressBudget(None, clock),
      clock,
      quota)
    val key = BackpressureStreamKey.forProducer(
      protocolShuffleId, defaultMapId, defaultTaskAttemptId, 0, consumerId)
    val metadataBytes = BackpressureProtocol.STREAM_LEDGER_BASE_BYTES
    val producerBytes = 2L * frame

    assert(quota.totalBytes === aggregateCapacity)
    assert(protocol.registerStream(key, frame),
      "The stream must reserve its base metadata before it can carry a block")
    assert(protocol.reservedMetadataQuotaBytes === metadataBytes)
    assert(quota.tryReserve(producerBytes),
      "Retained producer bytes must be charged to the same aggregate quota")
    assert(protocol.tryReserveReceiveQuota(frame),
      "Consumer payload bytes must fit while aggregate headroom remains")
    assert(quota.reservedBytes === producerBytes + frame + metadataBytes)

    assert(!protocol.tryReserveTransientQuota(frame),
      "A framing copy must be refused when the other categories leave less than one frame free")
    assert(protocol.reservedTransientQuotaBytes === 0L,
      "A refused transient reservation must not change its category")
    assert(quota.refusalCount === 1L, "The aggregate quota must count the refusal")

    protocol.releaseReceiveQuota(frame)
    assert(protocol.tryReserveTransientQuota(frame),
      "Returning consumer bytes must make the same aggregate room available to egress")
    assert(protocol.reservedTransientQuotaBytes === frame)
    assert(quota.reservedBytes === producerBytes + frame + metadataBytes)

    protocol.releaseTransientQuota(aggregateCapacity * 4L)
    assert(protocol.reservedTransientQuotaBytes === 0L,
      "An over-release must floor only the transient category")
    assert(quota.reservedBytes === producerBytes + metadataBytes,
      "Returning transient bytes must not release producer or metadata ownership")

    quota.release(producerBytes)
    assert(quota.reservedBytes === metadataBytes)
    assert(protocol.unregisterStream(key), "The last stream owner must close its ledger")
    assert(quota.reservedBytes === 0L,
      "Closing the final ledger must return the aggregate quota to zero")
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
    MemorySpillManager.resetSharedStateForTesting()
    val aggregation = StreamingShuffleWriter.durableAdmissionLogAggregator
    assert(aggregation.occurrenceCount === 0L,
      "The shared-state reset must leave the executor's aggregation window clean")

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

    val laterMs = firstTaskMs + MemorySpillManager.LOG_AGGREGATION_WINDOW_MS
    val summary = aggregation.record(laterMs, 1024L)
    assert(summary.isDefined, "A record past the window must be admitted")
    assert(summary.get.unreported === 1L,
      "The admitted record must state how many occurrences it stands in for")
    assert(summary.get.occurrences === 3L, "It must quote the executor-wide occurrence count")
    assert(aggregation.unreportedCount === 0L,
      "Admitting a record must clear the occurrences it accounted for")

    MemorySpillManager.resetSharedStateForTesting()
    assert(aggregation.occurrenceCount === 0L && aggregation.unreportedCount === 0L,
      "The one shared-state reset must clear the writer's aggregation as well as the manager's")
  }

  test("a producer's session-expiry report is bounded per executor, not per map output") {
    MemorySpillManager.resetSharedStateForTesting()
    val aggregator = StreamingShuffleServerHandler.sessionExpiryLogAggregator
    assert(aggregator.occurrenceCount === 0L && aggregator.unreportedCount === 0L,
      s"The shared reset must leave the executor's window open and its tally at zero, but the " +
        s"tally read ${aggregator.occurrenceCount} with ${aggregator.unreportedCount} unreported")

    def expireOneSession(harness: WriterHarness): Long = {
      val consumer = new ConsumerChannel(harness.serverHandler, s"$consumerId-expiring")
      consumer.subscribe(0)
      assert(harness.serverHandler.sessionCount === 1,
        s"The producer must hold the consumer's session before it vanishes, but holds " +
          s"${harness.serverHandler.sessionCount}")
      consumer.vanish()
      harness.clock.asInstanceOf[ManualClock].advance(
        StreamingShuffleServerHandler.CONSUMER_LIVENESS_TIMEOUT_MS)
      assert(harness.serverHandler.runMaintenance() >= 1,
        "The upkeep pass must retire the session of a consumer that stopped answering")
      assert(harness.serverHandler.expiredSessionCount === 1L,
        s"and must count exactly the one it retired, but counted " +
          s"${harness.serverHandler.expiredSessionCount}")
      harness.serverHandler.expiredSessionCount
    }

    val ((firstTally, secondTally), captured) = capturingStreamingLogs {
      val first = withHarness(newHarness(numPartitions = 2))(expireOneSession)
      val second = withHarness(newHarness(numPartitions = 2))(expireOneSession)
      (first, second)
    }

    assert(firstTally === 1L && secondTally === 1L,
      s"Each producer must count the one session it retired, but they counted $firstTally and " +
        s"$secondTally")
    assert(aggregator.occurrenceCount === 2L,
      s"The executor's tally must hold both expiries, but read ${aggregator.occurrenceCount}")
    assert(aggregator.unreportedCount === 1L,
      s"The second expiry must have been accounted rather than reported, so exactly one must be " +
        s"unreported, but ${aggregator.unreportedCount} were")
    val released = captured.filter(record =>
      record.isDefaultLevel && record.message.contains("released the egress session"))
    assert(released.size === 1,
      s"Two producers retiring a session must produce one default-level record between them, not " +
        s"one each, but produced ${released.size}: ${released.map(_.message).mkString(" | ")}")
    assert(released.head.message.contains("on this executor"),
      s"The admitted record must state the executor-wide figure beside the per-map-output one, " +
        s"but it read: ${released.head.message}")

    MemorySpillManager.resetSharedStateForTesting()
    assert(aggregator.occurrenceCount === 0L && aggregator.unreportedCount === 0L,
      "The one shared-state reset must clear the producer's aggregation as well as the manager's")
  }
}
