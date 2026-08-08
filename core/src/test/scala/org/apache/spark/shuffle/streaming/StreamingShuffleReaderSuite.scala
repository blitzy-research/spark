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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.lang.reflect.Modifier
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.concurrent.{Callable, CountDownLatch, CyclicBarrier, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import _root_.io.netty.channel.{ChannelFuture, ChannelFutureListener, DefaultChannelId}
import _root_.io.netty.channel.embedded.EmbeddedChannel

import org.apache.spark.{FetchFailed, LocalSparkContext, ShuffleDependency, SparkConf, SparkContext,
  SparkException, SparkFunSuite, SparkThrowable, TaskContext, TaskContextImpl}
import org.apache.spark.internal.config.{SHUFFLE_COMPRESS, SHUFFLE_STREAMING_ENABLED}
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient,
  TransportResponseHandler}
import org.apache.spark.network.protocol.OneWayMessage
import org.apache.spark.network.server.TransportRequestHandler
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, DataBlockMessage,
  HeartbeatMessage, RetransmitRequestMessage, StreamingShuffleChecksum, StreamingShuffleMessage,
  StreamingShuffleMessageType, StreamTerminationMessage}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcTimeout}
import org.apache.spark.shuffle.{FetchFailedException, ShuffleManager, ShuffleReader,
  ShuffleReadMetricsReporter}
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.{Clock, ManualClock, SystemClock, TaskCompletionListenerException,
  ThreadUtils}

/**
 * One consumer channel a test has opened to a streaming shuffle producer.
 *
 * @param handler the consumer handler the reader bound to the channel
 */
private[streaming] class StreamingShuffleTestProducerStream(
    val location: StreamingShuffleProducerLocation,
    val handler: StreamingShuffleClientHandler,
    val channel: EmbeddedChannel,
    val client: TransportClient) {

  private val deliveredBuffers = mutable.ArrayBuffer.empty[RecordingStreamingManagedBuffer]

  private val crashed = new AtomicBoolean(false)

  def awaitDataPlane(operation: String): Unit = {
    if (!handler.awaitDataPlaneIdle(10000L)) {
      throw new IllegalStateException(
        s"The consumer data plane did not settle after $operation")
    }
  }

  /** The transport's own dispatcher for an inbound frame, which is what owns the frame's buffer. */
  private val transportDispatcher: TransportRequestHandler = new TransportRequestHandler(
    channel, client, handler, java.lang.Long.valueOf(Long.MaxValue), null)

  /** Delivers one frame to the consumer exactly as the transport would. */
  def deliver(message: StreamingShuffleMessage): RecordingStreamingManagedBuffer = {
    assert(!isCrashed,
      s"The producer of map ${location.mapId} at ${location.hostPort} has been crashed, so it " +
        s"cannot put another frame on the wire; a case that delivered one would be asserting " +
        s"against a producer that is not actually gone")
    val framed = message.toByteBuffer()
    val bytes = new Array[Byte](framed.remaining())
    framed.duplicate().get(bytes)
    val buffer = new RecordingStreamingManagedBuffer(new NioManagedBuffer(ByteBuffer.wrap(bytes)))
    synchronized {
      deliveredBuffers += buffer
    }
    transportDispatcher.handle(new OneWayMessage(buffer))
    awaitDataPlane("frame delivery")
    assert(buffer.callsToRetain == 0,
      s"The consumer took ${buffer.callsToRetain} reference(s) on a transport buffer it must " +
        s"copy out of instead, so a frame's bytes could outlive the call that delivered it")
    assert(buffer.callsToRelease == 1,
      s"The transport releases a one-way message body exactly once, but this one was released " +
        s"${buffer.callsToRelease} time(s)")
    java.util.Arrays.fill(bytes, StreamingShuffleTestProducerStream.ScribbledByte)
    buffer
  }

  /**
   * Delivers arbitrary bytes to the consumer, bypassing every message constructor.
   *
   * @param bytes the raw frame body, exactly as it would arrive off the socket
   * @return the counting buffer the bytes were delivered in
   */
  def deliverRaw(bytes: Array[Byte]): RecordingStreamingManagedBuffer = {
    val buffer = new RecordingStreamingManagedBuffer(
      new NioManagedBuffer(ByteBuffer.wrap(bytes.clone())))
    synchronized {
      deliveredBuffers += buffer
    }
    handler.receive(client, buffer.nioByteBuffer())
    awaitDataPlane("raw frame delivery")
    buffer
  }

  def deliverFromForeignChannel(message: StreamingShuffleMessage): TransportClient = {
    val foreignChannel = new EmbeddedChannel(DefaultChannelId.newInstance())
    val foreignClient = new TransportClient(foreignChannel, new TransportResponseHandler(
      foreignChannel))
    foreignClient.setClientId("streaming-shuffle-reader-suite")
    val framed = message.toByteBuffer()
    val bytes = new Array[Byte](framed.remaining())
    framed.duplicate().get(bytes)
    handler.receive(foreignClient, ByteBuffer.wrap(bytes))
    awaitDataPlane("foreign-channel frame delivery")
    foreignChannel.finishAndReleaseAll()
    foreignClient
  }

  def deliveredFrameBuffers: Seq[RecordingStreamingManagedBuffer] =
    synchronized(deliveredBuffers.toSeq)

  def drainOutboundMessages(): Seq[StreamingShuffleMessage] = {
    awaitDataPlane("outbound control production")
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

  def encodedOutboundFrames: Seq[Array[Byte]] = {
    awaitDataPlane("encoded outbound control production")
    val frames = mutable.ArrayBuffer.empty[Array[Byte]]
    var written = channel.readOutbound[AnyRef]()
    while (written != null) {
      written match {
        case oneWay: OneWayMessage =>
          val body = oneWay.body().nioByteBuffer()
          val bytes = new Array[Byte](body.remaining())
          body.duplicate().get(bytes)
          frames += bytes
        case _ => ()
      }
      written = channel.readOutbound[AnyRef]()
    }
    frames.toSeq
  }

  def closeChannel(): Unit = {
    channel.close().syncUninterruptibly()
  }

  def crash(): Unit = crashed.set(true)

  def isCrashed: Boolean = crashed.get()
}

private[streaming] object StreamingShuffleTestProducerStream {

  val ScribbledByte: Byte = 0xEF.toByte
}

/**
 * A producer connector that hands out embedded channels instead of sockets.
 *
 * Injecting the connector is the whole reason the reader's abstract seam exists: every failure this
 * suite asserts on -- a refused connection, a lost producer, a corrupt block, a truncated stream --
 * is reachable here without a port, a thread pool or a timing assumption.
 *
 * The connector is owned by whoever built it and closed when that owner stops, never by a reader;
 * [[closeCallCount]] is what lets a suite prove the reader honours that ownership.
 *
 * @param refusalsBeforeSuccess connection attempts to refuse before the first success, which is how
 *                              a suite reaches the reader's bounded retry ladder
 * @param failBoundChannelBeforeRefusal a throwable to report to the handler, from another thread
 *                                      and over a channel the handler is bound to, on every
 *                                      attempt this connector refuses. That is the production
 *                                      connect window: a handler is bound to its channel before
 *                                      the connector decides whether to publish it, so a failure
 *                                      observed there is latched on the shared notifier by a
 *                                      handler the reader never receives, and no producer stream
 *                                      exists to attribute it to
 */
private[streaming] class StreamingShuffleTestProducerConnector(
    refusalsBeforeSuccess: Int = 0,
    failBoundChannelBeforeRefusal: Option[Throwable] = None)
  extends StreamingShuffleProducerConnector {

  private val opened = mutable.ArrayBuffer.empty[StreamingShuffleTestProducerStream]
  private val refusalsRemaining = new AtomicInteger(refusalsBeforeSuccess)
  private val attempts = new AtomicInteger(0)
  private val closeCalls = new AtomicInteger(0)
  private val reportedOnRefusal = new AtomicInteger(0)

  val firstConnection: CountDownLatch = new CountDownLatch(1)

  override def connect(
      location: StreamingShuffleProducerLocation,
      handler: StreamingShuffleClientHandler): Option[TransportClient] = {
    attempts.incrementAndGet()
    if (refusalsRemaining.getAndDecrement() > 0) {
      failBoundChannelBeforeRefusal.foreach(failure => reportOnBoundChannel(handler, failure))
      None
    } else {
      val channel = new EmbeddedChannel()
      val client = new TransportClient(channel, new TransportResponseHandler(channel))
      client.setClientId("streaming-shuffle-reader-suite")
      val stream = new StreamingShuffleTestProducerStream(location, handler, channel, client)
      synchronized {
        opened += stream
      }
      // The transport raises this callback for a live channel, and the consumer's subscription --
      // the in-progress block request -- is written from it.
      handler.channelActive(client)
      if (!handler.awaitDataPlaneIdle(10000L)) {
        throw new IllegalStateException(
          "The consumer data plane did not settle after producer channel activation")
      }
      firstConnection.countDown()
      Some(client)
    }
  }

  /**
   * Binds a channel to the handler, reports a failure on it from another thread, and leaves the
   * connect to be refused by the caller.
   *
   * Every step is required to reach the window this reproduces. The channel is activated first
   * because a handler refuses a callback from a channel it is not bound to, so a failure reported
   * to an unbound handler would be rejected instead of latched. The report is made from another
   * thread because that is where the transport raises it, and because the bridge under test exists
   * precisely to carry a failure from an I/O thread to the task thread. The data plane is settled
   * on both sides of the report so the caller can refuse the connect knowing the escalation has
   * already happened, which is what makes the case deterministic.
   *
   * The handler is deliberately not closed here: the reader closes a handler whose connect was
   * refused, and letting it do so is what keeps this fixture's behaviour the production one.
   *
   * @param handler the handler the reader built for this attempt
   * @param failure the throwable the transport is to report on the bound channel
   */
  private def reportOnBoundChannel(
      handler: StreamingShuffleClientHandler,
      failure: Throwable): Unit = {
    val channel = new EmbeddedChannel(DefaultChannelId.newInstance())
    val client = new TransportClient(channel, new TransportResponseHandler(channel))
    client.setClientId("streaming-shuffle-reader-suite-refused")
    handler.channelActive(client)
    if (!handler.awaitDataPlaneIdle(10000L)) {
      throw new IllegalStateException(
        "The consumer data plane did not settle after the refused channel was activated")
    }
    val pool = ThreadUtils.newDaemonSingleThreadExecutor("streaming-shuffle-refused-channel-io")
    try {
      pool.submit(new Callable[Unit] {
        override def call(): Unit = handler.exceptionCaught(failure, client)
      }).get(10000L, TimeUnit.MILLISECONDS)
    } finally {
      pool.shutdownNow()
    }
    if (!handler.awaitDataPlaneIdle(10000L)) {
      throw new IllegalStateException(
        "The consumer data plane did not settle after the refused channel failed")
    }
    reportedOnRefusal.incrementAndGet()
  }

  override def close(): Unit = {
    closeCalls.incrementAndGet()
  }

  def streams: Seq[StreamingShuffleTestProducerStream] = synchronized(opened.toSeq)

  def onlyStream: StreamingShuffleTestProducerStream = {
    val open = streams
    assert(open.size == 1,
      s"Expected exactly one producer channel to have been opened but found ${open.size}")
    open.head
  }

  def connectAttemptCount: Int = attempts.get()

  def closeCallCount: Int = closeCalls.get()

  /** How many refused attempts reported a failure on a channel the handler was bound to. */
  def reportedOnRefusalCount: Int = reportedOnRefusal.get()
}

/**
 * The production connector with its socket creation replaced by an embedded channel and a barrier.
 */
private[streaming] class BarrieredStreamingShuffleProducerConnector(conf: SparkConf)
  extends NettyStreamingShuffleProducerConnector(conf) {

  private val created = new java.util.concurrent.ConcurrentLinkedQueue[TransportClient]()

  private val insideCreation = new AtomicReference[() => Unit](() => ())

  def duringCreation(action: () => Unit): Unit = insideCreation.set(action)

  private val insideReservation = new AtomicReference[() => Unit](() => ())

  /**
   * Installs an action to run while a place on a shared channel is reserved and before the joining
   * handler is published into it.
   */
  def duringShareReservation(action: () => Unit): Unit = insideReservation.set(action)

  override protected def reserveShare(share: ChannelShare): Boolean = {
    val reserved = super.reserveShare(share)
    insideReservation.get()()
    reserved
  }

  def createdClients: Seq[TransportClient] = created.asScala.toSeq

  override protected def createTransportClient(host: String, port: Int): TransportClient = {
    // A channel id of its own per connection, because the connector keys its registries by channel
    // identity and Netty's `EmbeddedChannel` shares one singleton id across every instance.
    val channel = new EmbeddedChannel(DefaultChannelId.newInstance())
    val client = new TransportClient(channel, new TransportResponseHandler(channel))
    client.setClientId("streaming-shuffle-reader-suite")
    created.add(client)
    insideCreation.get()()
    client
  }
}

private[streaming] class StreamingShuffleTestCoordinatorRef(
    conf: SparkConf,
    clock: ManualClock,
    lookupAdvanceMillis: Long = 0L)
  extends RpcEndpointRef(conf) {

  import StreamingShuffleTestCoordinatorRef._

  private val locations = new AtomicReference[Option[StreamingShuffleProducerLocations]](None)
  private val invalidations = mutable.ArrayBuffer.empty[InvalidateStreamingShuffleProducer]
  private val declarations = mutable.ArrayBuffer.empty[DeclareStreamingShuffleFallback]
  private val lookups = new AtomicInteger(0)
  private val onInvalidate =
    new AtomicReference[InvalidateStreamingShuffleProducer => Unit](_ => ())

  def answerLookupWith(reply: Option[StreamingShuffleProducerLocations]): Unit = {
    locations.set(reply)
  }

  def currentAnswer: Option[StreamingShuffleProducerLocations] = locations.get()

  def observeInvalidation(callback: InvalidateStreamingShuffleProducer => Unit): Unit = {
    onInvalidate.set(callback)
  }

  def invalidationsSent: Seq[InvalidateStreamingShuffleProducer] = synchronized(invalidations.toSeq)

  def fallbacksDeclared: Seq[DeclareStreamingShuffleFallback] = synchronized(declarations.toSeq)

  def lookupCount: Int = lookups.get()

  override def address: RpcAddress = RpcAddress(DriverHost, DriverPort)

  override def name: String = StreamingShuffleCoordinator.ENDPOINT_NAME

  override def send(message: Any): Unit = {
    answer(message)
  }

  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] =
    Future.successful(answer(message).asInstanceOf[T])

  private def answer(message: Any): Any = message match {
    case _: LookupStreamingShuffleProducers =>
      lookups.incrementAndGet()
      if (lookupAdvanceMillis > 0L) {
        clock.advance(lookupAdvanceMillis)
      }
      locations.get()
    case invalidate: InvalidateStreamingShuffleProducer =>
      synchronized {
        invalidations += invalidate
      }
      onInvalidate.get()(invalidate)
      ReportedEpoch
    case declare: DeclareStreamingShuffleFallback =>
      synchronized {
        declarations += declare
      }
      StreamingShuffleFallbackState(declare.reasonName, ReportedEpoch)
    case other =>
      throw new IllegalArgumentException(
        s"The streaming shuffle reader sent an unexpected coordinator message: $other")
  }
}

private[streaming] object StreamingShuffleTestCoordinatorRef {

  val DriverHost: String = "streaming-shuffle-test-driver"

  val DriverPort: Int = 7077

  val ReportedEpoch: Long = 11L
}

/**
 * What else a reader was still holding at the instant one of its resources was released.
 *
 * @param reservedQuotaBytes bytes of the executor's shared receive budget still charged
 */
private[streaming] final case class StreamingShuffleCleanupSnapshot(
    queuedEvents: Int,
    reservedQuotaBytes: Long,
    channelOpen: Boolean,
    ledgerRegistered: Boolean)

/**
 * The ordered log of a reader's cleanup, recorded from inside the production calls that perform it.
 */
private[streaming] class StreamingShuffleCleanupRecorder {

  private val events =
    mutable.ArrayBuffer.empty[(String, Long, StreamingShuffleCleanupSnapshot)]

  @volatile private var armed: Boolean = false

  @volatile private var probe: () => StreamingShuffleCleanupSnapshot =
    () => StreamingShuffleCleanupSnapshot(0, 0L, channelOpen = false, ledgerRegistered = false)

  def observeWith(observation: () => StreamingShuffleCleanupSnapshot): Unit = {
    probe = observation
  }

  /** Starts recording. */
  def arm(): Unit = {
    armed = true
  }

  def record(name: String, bytes: Long): Unit = {
    if (armed) {
      val snapshot = probe()
      synchronized {
        events += ((name, bytes, snapshot))
      }
    }
  }

  def recorded: Seq[(String, Long, StreamingShuffleCleanupSnapshot)] = synchronized(events.toSeq)

  def releaseOrder: Seq[String] = recorded.map(_._1)

  def snapshotOf(name: String): Option[StreamingShuffleCleanupSnapshot] =
    recorded.find(_._1 == name).map(_._3)

  def bytesOf(name: String): Option[Long] = recorded.find(_._1 == name).map(_._2)
}

private[streaming] object StreamingShuffleCleanupRecorder {

  val ReceiveQuotaReturned: String = "receive-quota-returned"

  val ChannelClosed: String = "channel-closed"

  val CreditLedgerReleased: String = "credit-ledger-released"
}

/** A backpressure protocol reporting its own release calls to a cleanup recorder. */
private[streaming] class StreamingShuffleObservingBackpressure(
    conf: SparkConf,
    coordinator: StreamingShuffleCoordinator,
    egressBudget: TokenBucketRateLimiter.ExecutorEgressBudget,
    clock: ManualClock,
    recorder: StreamingShuffleCleanupRecorder)
  extends BackpressureProtocol(conf, coordinator, egressBudget, clock) {

  override def releaseReceiveQuota(bytes: Long): Unit = {
    super.releaseReceiveQuota(bytes)
    if (bytes > 0L) {
      recorder.record(StreamingShuffleCleanupRecorder.ReceiveQuotaReturned, bytes)
    }
  }

  override def unregisterStream(key: BackpressureStreamKey): Boolean = {
    val closed = super.unregisterStream(key)
    if (closed) {
      recorder.record(StreamingShuffleCleanupRecorder.CreditLedgerReleased, 0L)
    }
    closed
  }
}

/** Tests for [[StreamingShuffleReader]], the consumer half of a streaming shuffle. */
class StreamingShuffleReaderSuite
  extends SparkFunSuite
    with LocalSparkContext
    with StreamingShuffleTestHelper
    with StreamingShuffleHadoopCredentialIsolation {

  import StreamingShuffleTestHelper._

  private val NumPartitions = DefaultPartitionCount

  /** The one reduce partition each fixture reads. */
  private val ReducePartition = 3

  private val DeclaredMaps = 1

  private val TwoProducers = 2

  private val FourProducers = 4

  private val ProducerMapId = 0L

  private val ProducerAttemptId = 200L

  private val ConsumerAttemptId = 41L

  private val CoordinatorEpoch = 9L

  private val CapabilityToken = "streaming-shuffle-reader-suite-token"

  private val StreamedRecords: Seq[(Int, Int)] = (1 to 24).map(value => (value, value * 3))

  private val PeerRecords: Seq[(Int, Int)] = (101 to 118).map(value => (value, value * 7))

  private val BlockCount = 4

  private val BlocksPastHighWaterMark = StreamingShuffleClientHandler.INBOUND_QUEUE_HIGH_WATER_MARK

  /** Bound on the two-thread barrier the channel-share race is driven through. */
  private val RaceBarrierTimeoutSeconds = 30L

  private val LedgeredConsumerCreditBytes =
    DataBlockMessage.MAX_ENCODED_FRAME_BYTES.toLong * 8L

  private val LookupWaitMillis = 7L

  private val FullRangeStart = 0

  private val FullRangeEnd = Int.MaxValue

  private val ChecksumVerifyFailedCondition = "STREAMING_SHUFFLE_CHECKSUM_VERIFY_FAILED"

  private val StreamingShuffleSqlState = "XXKST"

  private val UnexpectedMessageTypeCondition = "STREAMING_SHUFFLE_UNEXPECTED_MESSAGE_TYPE"

  override def beforeEach(): Unit = {
    super.beforeEach()
    resetStreamingShuffleMetrics()
    MemorySpillManager.resetSharedStateForTesting()
  }

  /** Returns the process-scoped state to zero AFTER the case has finished, as well as before it. */
  override def afterEach(): Unit = {
    try {
      super.afterEach()
    } finally {
      resetStreamingShuffleMetrics()
      MemorySpillManager.resetSharedStateForTesting()
    }
  }

  /**
   * Everything one streaming read needs, wired the way `StreamingShuffleManager` wires it.
   *
   * @param refusalsBeforeSuccess connection attempts the connector refuses before it succeeds
   * @param lookupAdvanceMillis milliseconds the clock advances inside each rendezvous lookup
   * @param cleanupRecorder the log every release this reader performs is reported to, absent
   *     unless a case is asserting the order those releases happen in
   * @param realTimeReaderClock whether the reader and the handlers it builds read wall-clock
   *     time rather than this fixture's manual clock.
   */
  private class ReaderFixture(
      numPartitions: Int = NumPartitions,
      startPartition: Int = ReducePartition,
      endPartition: Int = ReducePartition + 1,
      numMaps: Int = DeclaredMaps,
      refusalsBeforeSuccess: Int = 0,
      lookupAdvanceMillis: Long = 0L,
      useTaskReporter: Boolean = false,
      cleanupRecorder: Option[StreamingShuffleCleanupRecorder] = None,
      completedMaps: Set[Int] = Set(ProducerMapId.toInt),
      realTimeReaderClock: Boolean = false,
      failBoundChannelBeforeRefusal: Option[Throwable] = None) {

    val conf: SparkConf = streamingConf()
    val clock: ManualClock = newManualClock()

    /** The clock the reader under test -- and every client handler it builds -- reads. */
    val readerClock: Clock = if (realTimeReaderClock) new SystemClock else clock
    val dependency: ShuffleDependency[Int, Int, Int] =
      shuffleDependencyFor(sc, conf, numPartitions, numRecords = StreamedRecords.size)
    val shuffleId: Int = dependency.shuffleId
    val handle: StreamingShuffleHandle[Int, Int, Int] = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId, dependency, numPartitions, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
      CoordinatorEpoch, CapabilityToken)
    val coordinator: StreamingShuffleCoordinator =
      new StreamingShuffleCoordinator(sc.env.rpcEnv, conf, clock)
    val backpressure: BackpressureProtocol = cleanupRecorder match {
      case Some(recorder) =>
        new StreamingShuffleObservingBackpressure(
          conf, coordinator, TokenBucketRateLimiter.executorBudget(conf, clock), clock, recorder)
      case None =>
        new BackpressureProtocol(
          conf, coordinator, TokenBucketRateLimiter.executorBudget(conf, clock), clock)
    }
    val fallbackPolicy: StreamingShuffleFallbackPolicy =
      new StreamingShuffleFallbackPolicy(conf, clock)
    val connector: StreamingShuffleTestProducerConnector =
      new StreamingShuffleTestProducerConnector(
        refusalsBeforeSuccess, failBoundChannelBeforeRefusal)
    val coordinatorRef: StreamingShuffleTestCoordinatorRef =
      new StreamingShuffleTestCoordinatorRef(conf, clock, lookupAdvanceMillis)
    val recordingMetrics: RecordingStreamingShuffleReadMetrics =
      new RecordingStreamingShuffleReadMetrics
    val context: TaskContextImpl =
      newTaskContext(sc.env, partitionId = startPartition, taskAttemptId = ConsumerAttemptId,
        numPartitions = numPartitions)
    val readMetrics: ShuffleReadMetricsReporter =
      if (useTaskReporter) context.taskMetrics.createTempShuffleReadMetrics()
      else recordingMetrics
    /** Every producer generation the coordinator offers this consumer, in map-index order. */
    val producers: Seq[StreamingShuffleProducerLocation] = {
      require(numMaps > 0,
        s"A reader fixture must declare at least one map task but declared $numMaps")
      (0 until numMaps).map(producerLocation)
    }

    val producer: StreamingShuffleProducerLocation = producers.head

    coordinatorRef.answerLookupWith(
      Some(locationsReply(producers, completedMapIndexes = completedMaps)))

    val readerContext: StreamingShuffleReaderContext = StreamingShuffleReaderContext(
      coordinatorRef, backpressure, fallbackPolicy, connector, sc.env.serializerManager)
    val reader: StreamingShuffleReader[Int, Int] = new StreamingShuffleReader[Int, Int](
      handle, FullRangeStart, FullRangeEnd, startPartition, endPartition, context, readMetrics,
      conf, readerContext, readerClock)

    def partitionId: Int = startPartition

    def blockName: String = ShuffleBlockId(shuffleId, ProducerMapId, partitionId).name

    def producerLocation(mapIndex: Int): StreamingShuffleProducerLocation =
      StreamingShuffleProducerLocation(
        executorId = s"producer-executor-$mapIndex",
        host = "producer-host",
        port = StreamingShuffleTestCoordinatorRef.DriverPort + 1 + mapIndex,
        mapId = mapIndex.toLong,
        mapIndex = mapIndex,
        taskAttemptId = ProducerAttemptId + mapIndex,
        blockManagerId = BlockManagerId(s"producer-executor-$mapIndex", "block-manager-host",
          StreamingShuffleTestCoordinatorRef.DriverPort + 1000 + mapIndex))

    def locationsReply(
        producers: Seq[StreamingShuffleProducerLocation],
        fallback: StreamingShuffleFallbackState = StreamingShuffleFallbackState(),
        completedMapIndexes: Set[Int] = Set.empty)
      : StreamingShuffleProducerLocations =
      StreamingShuffleProducerLocations(
        shuffleId = shuffleId,
        locations = producers,
        numPartitions = numPartitions,
        numMaps = numMaps,
        completedMapIndexes = completedMapIndexes,
        protocolVersion = StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
        coordinatorEpoch = CoordinatorEpoch,
        fallback = fallback)

    def encodePartition(
        records: Seq[(Int, Int)],
        mapId: Long = ProducerMapId): Array[Byte] =
      encodePartitionFor(partitionId, records, mapId)

    /**
     * Encodes one partition's records under that partition's own block identity, which is what a
     * case covering a range of partitions needs: the stream wrapper is keyed by block id, so a
     * payload encoded for one partition cannot be read back as another's.
     */
    def encodePartitionFor(
        encodedPartitionId: Int,
        records: Seq[(Int, Int)],
        mapId: Long = ProducerMapId): Array[Byte] = {
      val bytes = new ByteArrayOutputStream()
      val wrapped = sc.env.serializerManager.wrapStream(
        ShuffleBlockId(shuffleId, mapId, encodedPartitionId), bytes)
      val serialized = dependency.serializer.newInstance().serializeStream(wrapped)
      records.foreach { record =>
        serialized.writeKey(record._1)
        serialized.writeValue(record._2)
      }
      serialized.close()
      bytes.toByteArray
    }

    def dataBlocksOf(
        payload: Array[Byte],
        blocks: Int = 1,
        mapId: Long = ProducerMapId): Seq[DataBlockMessage] = {
      assert(payload.nonEmpty, "A partition payload must carry bytes to be cut into blocks")
      assert(blocks > 0, s"A partition must be cut into at least one block but was $blocks")
      val chunkSize = math.max(1, (payload.length + blocks - 1) / blocks)
      payload.grouped(chunkSize).toSeq.zipWithIndex.map { chunk =>
        dataBlock(shuffleId, mapId, partitionId, chunk._2.toLong, chunk._1)
      }
    }

    def terminator(totalBlocks: Long, mapId: Long = ProducerMapId): StreamTerminationMessage =
      streamTermination(shuffleId, mapId, partitionId, totalBlocks)

    def consumerLedgerKey: Option[BackpressureStreamKey] =
      backpressure.registeredStreams.find(key => key.partitionId == partitionId)

    /** The credit ledgers this reader holds for one producer generation. */
    def consumerLedgerKeysOf(mapId: Long): Seq[BackpressureStreamKey] =
      backpressure.registeredStreams.filter(key =>
        key.partitionId == partitionId && key.mapId == mapId)

    def streamOf(mapIndex: Int): StreamingShuffleTestProducerStream = {
      val open = connector.streams.filter(_.location.mapIndex == mapIndex)
      assert(open.size == 1,
        s"Expected exactly one open channel to the producer of map index $mapIndex but found " +
          s"${open.size} among ${connector.streams.map(_.location.mapIndex).mkString(", ")}")
      open.head
    }

    def newReader(
        readerConf: SparkConf,
        startMapIndex: Int,
        endMapIndex: Int): StreamingShuffleReader[Int, Int] =
      new StreamingShuffleReader[Int, Int](handle, startMapIndex, endMapIndex, startPartition,
        endPartition, context, readMetrics, readerConf, readerContext, readerClock)
  }

  /** Starts the live context every fixture needs. */
  private def startContext(compressShuffle: Boolean = false): Unit = {
    // Built on the shared envelope rather than on a bare SparkConf.
    val testConf = testEnvelopeConf().set(SHUFFLE_COMPRESS, compressShuffle)
    sc = new SparkContext("local", "test", testConf)
  }

  private def expectedRecords: Set[(Int, Int)] = StreamedRecords.toSet

  private def readRecords(records: Iterator[Product2[Int, Int]]): Set[(Int, Int)] =
    records.map(record => (record._1, record._2)).toSet

  private def withTaskContext[T](context: TaskContextImpl)(body: => T): T = {
    TaskContext.setTaskContext(context)
    try {
      body
    } finally {
      TaskContext.unset()
    }
  }

  private def containsBytes(haystack: Array[Byte], needle: Array[Byte]): Boolean = {
    if (needle.isEmpty || needle.length > haystack.length) {
      false
    } else {
      (0 to haystack.length - needle.length).exists { offset =>
        needle.indices.forall(index => haystack(offset + index) == needle(index))
      }
    }
  }

  private def fetchFailedReasonOf(failure: FetchFailedException): FetchFailed =
    failure.toTaskFailedReason match {
      case fetchFailed: FetchFailed => fetchFailed
      case other =>
        throw new IllegalStateException(
          s"A streaming shuffle producer loss must convert into a fetch-failed reason so the " +
            s"unmodified scheduler recomputes the upstream stage, but it converted into $other")
    }

  private def injectFromIoThread(
      stream: StreamingShuffleTestProducerStream,
      failure: Throwable): Unit = {
    val reportingThread = new AtomicReference[Thread](null)
    val pool = ThreadUtils.newDaemonSingleThreadExecutor("streaming-shuffle-reader-suite-io")
    try {
      awaitJavaFuture(pool.submit(new Callable[Unit] {
        override def call(): Unit = {
          reportingThread.set(Thread.currentThread())
          stream.handler.exceptionCaught(failure, stream.client)
        }
      }))
      stream.awaitDataPlane("injected I/O failure")
    } finally {
      pool.shutdownNow()
    }
    val reporter = reportingThread.get()
    assert(reporter != null && reporter != Thread.currentThread(),
      "The failure must be reported from a thread other than the one driving the read, or an " +
        "assertion about the bridge between the two proves nothing")
  }

  private def diagnosisOf(failure: FetchFailedException): Seq[Throwable] =
    Option(failure.getCause).toSeq ++ failure.getSuppressed.toSeq

  private def readableRecordsOf(fixture: ReaderFixture, mapIndex: Int): Seq[(Int, Int)] = {
    val stream = fixture.streamOf(mapIndex)
    val mapId = stream.location.mapId
    val payload = new ByteArrayOutputStream()
    var blocksRead = 0
    var event = stream.handler.poll()
    while (event.isDefined) {
      event.get match {
        case StreamingShuffleClientHandler.BlockReceived(block) =>
          assert(block.verifyChecksum(),
            s"A surviving producer's block at sequence ${block.sequenceNumber()} must still " +
              s"match the checksum it arrived with")
          assert(block.mapId == mapId && block.partitionId == fixture.partitionId,
            s"A block held for map $mapId partition ${fixture.partitionId} must belong to it, " +
              s"but named map ${block.mapId} partition ${block.partitionId}")
          payload.write(block.copyPayload())
          blocksRead += 1
        case _ => ()
      }
      event = stream.handler.poll()
    }
    assert(blocksRead > 0,
      s"The producer of map index $mapIndex is holding no block, so there is nothing to prove " +
        s"readable")
    val wrapped = sc.env.serializerManager.wrapStream(
      ShuffleBlockId(fixture.shuffleId, mapId, fixture.partitionId),
      new ByteArrayInputStream(payload.toByteArray))
    fixture.dependency.serializer.newInstance().deserializeStream(wrapped).asKeyValueIterator
      .map(record => (record._1.asInstanceOf[Int], record._2.asInstanceOf[Int])).toSeq
  }

  /**
   * Loses several producers of one shuffle at the same instant, through the shared fault injector.
   *
   * @param injector the fault injector sharing the fixture's clock
   * @return the channels that took the fault, in the order given
   */
  private def crashProducersTogether(
      injector: StreamingShuffleFaultInjector,
      streams: Seq[StreamingShuffleTestProducerStream])
    : Seq[StreamingShuffleTestProducerStream] = {
    assert(streams.size > 1,
      s"Concurrent producer failure needs more than one producer to be concurrent, but " +
        s"${streams.size} was offered")
    injector.crashProducersConcurrently(streams.size)
    val lost = streams.filter(_ =>
      injector.shouldFail(StreamingShuffleFaultScenario.ConcurrentProducerFailures))
    assert(lost.size == streams.size,
      s"The injector must lose every producer it was armed for, but lost ${lost.size} of " +
        s"${streams.size}")
    assert(!injector.isArmed(StreamingShuffleFaultScenario.ConcurrentProducerFailures),
      "The arming budget must be exactly spent, so no later seam in this test can take the fault")
    lost.foreach(_.crash())
    lost
  }

  test("in progress block request and partial consumption") {
    startContext()
    // The coordinator reports *no* map index as complete, which is the state of a shuffle whose map
    // task is still running: the producer has registered its generation and is streaming, and
    // nothing has yet declared that output finished.
    val fixture = new ReaderFixture(completedMaps = Set.empty)
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)
    assert(blocks.size > 1,
      s"The partition payload must span more than one block for partial consumption to mean " +
        s"anything, but it was cut into ${blocks.size}")
    assert(fixture.coordinatorRef.currentAnswer.exists(reply => !reply.mapStageComplete),
      "The rendezvous answer must report the map stage as unfinished, or this case is reading " +
        "retained output rather than output in progress")
    assert(fixture.coordinatorRef.currentAnswer.exists(_.mapOutputsPending),
      "Map output this consumer has not been offered yet must still be expected, which is the " +
        "predicate that distinguishes an in-progress read from a completed one")

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream

    val requestFrames = stream.drainOutboundMessages()
    assert(requestFrames.nonEmpty,
      "The reader must issue an in-progress block request before any block has been produced")
    assert(requestFrames.head.isInstanceOf[HeartbeatMessage],
      s"The first frame a streaming consumer writes must be the in-progress request, but it was " +
        s"${requestFrames.head.getClass.getSimpleName}")
    assert(requestFrames.forall(frame => frame.partitionId() == fixture.partitionId),
      "Every in-progress request must name a partition this reduce task actually reads")
    assert(fixture.recordingMetrics.recordsRead == 0L,
      s"read() must return a lazy iterator, but it had already surfaced " +
        s"${fixture.recordingMetrics.recordsRead} record(s)")

    val arrived = blocks.dropRight(1)
    arrived.foreach(block => stream.deliver(block))
    assert(stream.handler.acceptedBlockCount(fixture.partitionId) == arrived.size.toLong,
      s"Every delivered block must be admitted, but only " +
        s"${stream.handler.acceptedBlockCount(fixture.partitionId)} of ${arrived.size} were")
    assert(!stream.handler.isStreamComplete(fixture.partitionId),
      "The stream must still be open, since an in-progress request is answered before the " +
        "producer knows how much output it will have")
    assert(stream.handler.announcedBlockCount(fixture.partitionId).isEmpty,
      s"No block total may have been announced yet, but the handler holds " +
        s"${stream.handler.announcedBlockCount(fixture.partitionId)}")

    assert(records.hasNext, "The reader must surface records from an unterminated stream")
    val firstRecord = records.next()
    assert(fixture.recordingMetrics.recordsRead == 1L,
      s"Exactly one record must have been surfaced, but the reporter counted " +
        s"${fixture.recordingMetrics.recordsRead}")
    assert(stream.handler.queuedEventCount > 0,
      "The reader must consume one block at a time, so blocks it has not reached yet must still " +
        "be waiting on the hand-off queue rather than having been drained into the reduce input")
    assert(fixture.coordinatorRef.currentAnswer
        .exists(reply => !reply.mapStageComplete && reply.mapOutputsPending),
      "The record above must have been surfaced while the map stage was still producing, which " +
        "is the overlap this feature exists for")

    stream.deliver(blocks.last)
    stream.deliver(fixture.terminator(blocks.size.toLong))
    fixture.coordinatorRef.answerLookupWith(Some(fixture.locationsReply(
      Seq(fixture.producer), completedMapIndexes = Set(ProducerMapId.toInt))))
    val remaining = readRecords(records)
    val surfaced = remaining + ((firstRecord._1, firstRecord._2))
    assert(surfaced == expectedRecords,
      s"A streamed partition must yield exactly the records the producer sent: " +
        s"${expectedRecords.diff(surfaced).size} missing and " +
        s"${surfaced.diff(expectedRecords).size} unexpected")
    assert(fixture.recordingMetrics.recordsRead == StreamedRecords.size.toLong,
      s"Records read must equal the records surfaced, but the reporter counted " +
        s"${fixture.recordingMetrics.recordsRead} of ${StreamedRecords.size}")

    val acknowledgements = stream.drainOutboundMessages().collect {
      case ack: AckMessage => ack
    }
    assert(acknowledgements.nonEmpty,
      "The reader must acknowledge consumption so the producer can reclaim its buffers")
    assert(acknowledgements.map(_.consumerPosition()) ==
        acknowledgements.map(_.consumerPosition()).sorted,
      "Acknowledged positions must advance monotonically as the reduce task makes progress")
    assert(stream.handler.acknowledgedPosition(fixture.partitionId) == blocks.size - 1L,
      s"Every block must be acknowledged by the end of the read, but the position stopped at " +
        s"${stream.handler.acknowledgedPosition(fixture.partitionId)} of ${blocks.size - 1}")
    assert(fixture.coordinatorRef.invalidationsSent.isEmpty,
      "A complete streaming read must invalidate no producer")
    assert(observedPartialReadInvalidations() == 0L,
      s"A complete streaming read must record no partial read invalidation, but " +
        s"${observedPartialReadInvalidations()} was recorded")
    assert(fixture.coordinatorRef.lookupCount == 1,
      s"A streaming read resolves its producers once and then consumes output as it arrives, so " +
        s"the completion the coordinator recorded later must never have been waited on, but the " +
        s"reader looked the rendezvous up ${fixture.coordinatorRef.lookupCount} time(s)")
  }

  test("a streamed partition matches the producer bytes when shuffle compression is on") {
    startContext(compressShuffle = true)
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(block => stream.deliver(block))
    stream.deliver(fixture.terminator(blocks.size.toLong))

    assert(readRecords(records) == expectedRecords,
      "A compressed partition's payload bytes, concatenated in sequence order across the blocks " +
        "of one producer, must be exactly the bytes the writer's wrapping produced")
  }

  test("producer failure detection via connection timeout") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    stream.deliver(blocks.head)
    assert(stream.handler.millisSinceInbound(fixture.partitionId).contains(0L),
      "The handler must record the instant of arrival from the injected clock")

    advancePastProducerTimeout(fixture.clock)
    assert(stream.handler.isProducerSilent(fixture.partitionId),
      s"A producer silent for ${ProducerConnectionTimeoutMillis} ms must be reported as silent")

    val failure = intercept[FetchFailedException] {
      withTaskContext(fixture.context) {
        readRecords(records)
      }
    }
    assert(failure.getClass == classOf[FetchFailedException],
      s"Producer loss must be reported through Spark's own fetch failure and never through a " +
        s"bespoke exception, but a ${failure.getClass.getName} was raised")
    val reason = fetchFailedReasonOf(failure)
    assert(reason.shuffleId == fixture.shuffleId,
      s"The fetch failure must name the shuffle being read, but it named ${reason.shuffleId}")
    assert(reason.mapId == fixture.producer.mapId && reason.mapIndex == fixture.producer.mapIndex,
      s"The fetch failure must name the lost map output so the tracker can remove it, but it " +
        s"named map ${reason.mapId} index ${reason.mapIndex}")
    assert(reason.reduceId == fixture.partitionId,
      s"The fetch failure must name the reduce partition being read, but it named " +
        s"${reason.reduceId}")
    assert(reason.bmAddress == fixture.producer.blockManagerId,
      s"The fetch failure must carry the producer's own MapStatus address, without which " +
        s"MapOutputTracker removes nothing and the recomputation never happens, but it carried " +
        s"${reason.bmAddress}")
    assert(fixture.context.fetchFailed.contains(failure),
      "FetchFailedException must register itself on the task context so the executor reports a " +
        "FetchFailed task-end reason and the unmodified scheduler recomputes the upstream stage")
  }

  test("producer failure detection via connection timeout does not fire before five seconds") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    // The whole read runs on one thread, so every reporter call still arrives on the thread that
    // owns the read.
    val pool = ThreadUtils.newDaemonSingleThreadExecutor("streaming-shuffle-reader-suite")
    try {
      val reading = pool.submit(new Callable[Set[(Int, Int)]] {
        override def call(): Set[(Int, Int)] = readRecords(fixture.reader.read())
      })
      awaitLatch(fixture.connector.firstConnection, "the consumer channel to be opened")
      val stream = fixture.connector.onlyStream
      blocks.foreach(block => stream.deliver(block))

      advanceJustBeforeProducerTimeout(fixture.clock)
      assert(stream.handler.millisSinceInbound(fixture.partitionId)
          .contains(JustBeforeProducerTimeoutMillis),
        s"The elapsed silence must be exactly ${JustBeforeProducerTimeoutMillis} ms")
      assert(!stream.handler.isProducerSilent(fixture.partitionId),
        s"A producer silent for only ${JustBeforeProducerTimeoutMillis} ms must not be reported " +
          s"as lost: the bound is ${ProducerConnectionTimeoutMillis} ms")

      stream.deliver(fixture.terminator(blocks.size.toLong))
      assert(awaitJavaFuture(reading) == expectedRecords,
        "A read that passed through the liveness check one millisecond short of the connection " +
          "timeout must complete with every record intact")
      assert(observedPartialReadInvalidations() == 0L,
        s"No partial read may be invalidated below the connection timeout, but " +
          s"${observedPartialReadInvalidations()} was recorded")
      assert(fixture.coordinatorRef.invalidationsSent.isEmpty,
        "No producer may be invalidated below the connection timeout")
      assertSingleThreadedReporting(fixture.recordingMetrics.foreignThreadCallCount,
        "the streaming shuffle read metrics reporter")
    } finally {
      pool.shutdownNow()
    }
  }

  test("a refused connection is retried on the one second ladder and then streams") {
    startContext()
    // Two refusals and then a channel.
    val refusals = 2
    val fixture = new ReaderFixture(refusalsBeforeSuccess = refusals, realTimeReaderClock = true)
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)
    val laddered = RetryBackoffLadderMillis.take(refusals).sum

    val startNanos = System.nanoTime()
    val records = fixture.reader.read()
    val openElapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos)

    assert(fixture.connector.connectAttemptCount == refusals + 1,
      s"A refused connect must be retried until one succeeds, so the connector must have been " +
        s"asked ${refusals + 1} time(s), but it was asked " +
        s"${fixture.connector.connectAttemptCount} time(s)")
    assert(fixture.connector.streams.size == 1,
      s"Only the attempt that succeeded may leave a channel behind, because a refused handler is " +
        s"closed at once and returns the receive quota it charged, but " +
        s"${fixture.connector.streams.size} channel(s) exist")
    assert(openElapsedMillis >= laddered,
      s"The two refusals must have been paced by the ladder, which is ${laddered} ms of pausing " +
        s"before the third attempt, but opening the channel took ${openElapsedMillis} ms")
    assert(fixture.recordingMetrics.fetchWaitTime >= laddered,
      s"Time spent pausing between connection attempts is time this reduce task waited on its " +
        s"input, so it must be charged to Spark's own fetch-wait metric, but only " +
        s"${fixture.recordingMetrics.fetchWaitTime} ms of ${laddered} ms was")

    val stream = fixture.connector.onlyStream
    blocks.foreach(block => stream.deliver(block))
    stream.deliver(fixture.terminator(blocks.size.toLong))
    assert(readRecords(records) == expectedRecords,
      "A read whose connection had to be retried must still yield every record the producer sent")
    assert(fixture.coordinatorRef.invalidationsSent.isEmpty,
      s"A producer that answered on a later attempt must not be invalidated, but " +
        s"${fixture.coordinatorRef.invalidationsSent.size} invalidation(s) were sent")
    assert(fixture.coordinatorRef.fallbacksDeclared.isEmpty,
      "A retried connection is none of the four trip conditions, so streaming must not be stood " +
        "down for the shuffle")
    assert(observedPartialReadInvalidations() == 0L,
      s"A refused connect delivers nothing, so there is no partial read to invalidate, but " +
        s"${observedPartialReadInvalidations()} was recorded")
    assert(fixture.connector.closeCallCount == 0,
      "The executor-scoped connector is closed by the manager and never by a reader")
  }

  test("the connection timeout diagnostic reports the silence that actually reached the verdict") {
    startContext()
    // A producer that delivers NOTHING -- no block, no heartbeat, no end of stream -- which is what
    // a crash before the first frame, or a partition that never routed, looks like to a consumer.
    // It is also the one case in which the reader's per-partition reading has nothing to measure:
    // the flow-control ledger reaches the verdict, and a diagnostic that quoted the per-partition
    // reading instead rendered "received nothing for 0 ms, past the 5000 ms connection timeout" --
    // asserting a silence of zero to be past five seconds, and withholding the only figure that
    // separates a dead producer from a mis-tuned timeout.
    val fixture = new ReaderFixture()
    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    assert(stream.handler.millisSinceInbound(fixture.partitionId).isEmpty,
      "Nothing may have arrived, or this case is not exercising the reading that has no interval")

    advancePastProducerTimeout(fixture.clock)
    val failure = intercept[FetchFailedException] {
      withTaskContext(fixture.context) {
        readRecords(records)
      }
    }

    // Asserted as an invariant over the rendered text rather than against one expected sentence,
    // because which reading reaches the verdict is production's decision and may legitimately
    // change: whatever silence a connection-timeout diagnostic quotes, it must be at least the
    // bound it claims that silence is past. That is exactly the property the defect broke.
    val message = failure.getMessage
    val timedOut = """(\d+) ms(?: ago)?, past the (\d+) ms connection timeout""".r
    val quoted = timedOut.findAllMatchIn(message)
      .map(matched => (matched.group(1).toLong, matched.group(2).toLong))
      .toSeq
    assert(quoted.nonEmpty,
      s"A connection timeout must quote the silence it measured against the bound it fired on, " +
        s"but it read: $message")
    assert(quoted.forall { case (silence, bound) => silence >= bound },
      s"Every silence a connection timeout quotes must be at least the bound it is said to be " +
        s"past, but $quoted was quoted in: $message")
    assert(quoted.forall { case (_, bound) => bound == ProducerConnectionTimeoutMillis },
      s"The bound quoted must be the contracted ${ProducerConnectionTimeoutMillis} ms connection " +
        s"timeout, but $quoted was quoted in: $message")
    assert(!message.contains("for 0 ms"),
      s"A silence of zero can never be past a ${ProducerConnectionTimeoutMillis} ms bound, but " +
        s"the diagnostic read: $message")

    // The recovery the diagnostic accompanies is unchanged: one atomic per-producer invalidation,
    // and a fetch failure the unmodified scheduler resolves by recomputing the upstream stage.
    assert(observedPartialReadInvalidations() == 1L,
      s"A producer given up on must be counted as one invalidated partial read, but " +
        s"${observedPartialReadInvalidations()} was")
    val reason = fetchFailedReasonOf(failure)
    assert(reason.shuffleId == fixture.shuffleId && reason.reduceId == fixture.partitionId,
      s"The fetch failure must name the shuffle and reduce partition being read, but it named " +
        s"shuffle ${reason.shuffleId} partition ${reason.reduceId}")
    assert(reason.bmAddress == fixture.producer.blockManagerId,
      s"The fetch failure must carry the producer's own MapStatus address, without which " +
        s"MapOutputTracker removes nothing, but it carried ${reason.bmAddress}")
  }

  test("silence evidence names the reading it came from and never quotes another one") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    fixture.reader.read()
    val stream = fixture.connector.onlyStream
    val handler = stream.handler

    // Before the bound there is no verdict and therefore no evidence to quote. The heartbeat is
    // sent through the same call the reader makes on every poll window, which is what brings the
    // per-partition state into existence without anything having arrived on it -- precisely the
    // arrangement in which the two readings disagree.
    advanceJustBeforeProducerTimeout(fixture.clock)
    assert(handler.sendHeartbeatIfDue(fixture.partitionId),
      "A consumer that has waited past the heartbeat interval must have sent one")
    assert(handler.producerSilence(fixture.partitionId).isEmpty,
      s"A producer silent for only ${JustBeforeProducerTimeoutMillis} ms must yield no evidence")
    assert(!handler.isProducerSilent(fixture.partitionId),
      "and must not be judged silent, since the verdict is expressed over that same evidence")

    // At the bound, with nothing ever received, the ledger's interval is the only one there is.
    fixture.clock.advance(1L)
    val beforeAnyBlock = handler.producerSilence(fixture.partitionId)
    assert(handler.isProducerSilent(fixture.partitionId),
      s"At ${ProducerConnectionTimeoutMillis} ms the producer must be judged silent")
    assert(beforeAnyBlock.exists(_.isInstanceOf[
        StreamingShuffleClientHandler.ProducerSilence.SinceStreamOpened]),
      s"With nothing ever received the evidence must be the ledger's own, but it was " +
        s"$beforeAnyBlock")
    val openedSilence = beforeAnyBlock.get
    assert(handler.millisSinceInbound(fixture.partitionId).isEmpty,
      "The per-partition reading must still have no interval, which is why it cannot be quoted")
    assert(openedSilence.elapsedMillis >= openedSilence.thresholdMillis,
      s"Evidence must measure at least the bound it crossed, but it measured " +
        s"${openedSilence.elapsedMillis} ms against ${openedSilence.thresholdMillis} ms")
    assert(openedSilence.describe(fixture.partitionId).contains(
        s"${openedSilence.elapsedMillis} ms"),
      s"The rendered clause must quote the interval it measured, but it read: " +
        s"${openedSilence.describe(fixture.partitionId)}")

    // Once a block has arrived the per-partition reading exists, so it is the one reported, and the
    // figure it quotes is the gap since that block rather than the gap since the stream opened.
    stream.deliver(blocks.head)
    assert(handler.producerSilence(fixture.partitionId).isEmpty,
      "A block resets the silence, so a producer that just delivered is not silent")
    advancePastProducerTimeout(fixture.clock)
    val afterBlock = handler.producerSilence(fixture.partitionId)
    assert(afterBlock.exists(_.isInstanceOf[
        StreamingShuffleClientHandler.ProducerSilence.SinceInbound]),
      s"After a delivery the evidence must be this handler's own reading, but it was $afterBlock")
    assert(afterBlock.map(_.elapsedMillis) ==
        handler.millisSinceInbound(fixture.partitionId),
      s"and must quote exactly that reading, but it quoted ${afterBlock.map(_.elapsedMillis)} " +
        s"against ${handler.millisSinceInbound(fixture.partitionId)}")

    // A terminated stream is complete rather than silent, however long its quiet lasts. The total
    // announced is the one block actually delivered, because a terminator naming more than has
    // arrived is a truncation the handler is right to refuse rather than an orderly end.
    stream.deliver(fixture.terminator(1L))
    advancePastProducerTimeout(fixture.clock)
    assert(handler.producerSilence(fixture.partitionId).isEmpty,
      "An ended stream must yield no silence evidence, or a finished stage would be recomputed")
    assert(!handler.isProducerSilent(fixture.partitionId),
      "and must not be judged silent either")
  }

  test("a producer that refuses every attempt is invalidated once the budget is spent") {
    startContext()
    val fixture = new ReaderFixture(
      refusalsBeforeSuccess = MaxRetryAttempts, realTimeReaderClock = true)
    val laddered = RetryBackoffLadderMillis.take(MaxRetryAttempts - 1).sum

    val startNanos = System.nanoTime()
    val failure = intercept[FetchFailedException] {
      withTaskContext(fixture.context) {
        fixture.reader.read()
      }
    }
    val elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos)

    assert(fixture.connector.connectAttemptCount == MaxRetryAttempts,
      s"The connect budget is exactly ${MaxRetryAttempts} attempts, but the connector was asked " +
        s"${fixture.connector.connectAttemptCount} time(s)")
    assert(fixture.connector.streams.isEmpty,
      s"No channel may exist when every attempt was refused, but " +
        s"${fixture.connector.streams.size} do")
    assert(elapsedMillis >= laddered,
      s"The four gaps between the five attempts must be paced by the ladder, which is " +
        s"${laddered} ms of pausing, but the whole attempt took ${elapsedMillis} ms")
    assert(fixture.recordingMetrics.fetchWaitTime >= laddered,
      s"Every pause must be charged to fetch wait time even when the connect never succeeds, but " +
        s"only ${fixture.recordingMetrics.fetchWaitTime} ms of ${laddered} ms was")
    assert(failure.getMessage.contains(s"${MaxRetryAttempts} attempt(s)"),
      s"The failure must say how many attempts were spent, but it read: ${failure.getMessage}")
    assert(failure.getMessage.contains(ProducerConnectionTimeoutMillis.toString),
      s"The failure must name the connection timeout each attempt was bounded at, but it read: " +
        s"${failure.getMessage}")

    val reason = fetchFailedReasonOf(failure)
    assert(reason.shuffleId == fixture.shuffleId && reason.reduceId == fixture.partitionId,
      s"The fetch failure must name the shuffle and reduce partition being read, but it named " +
        s"shuffle ${reason.shuffleId} partition ${reason.reduceId}")
    assert(reason.mapId == fixture.producer.mapId && reason.mapIndex == fixture.producer.mapIndex,
      s"The fetch failure must name the unreachable map output, but it named map ${reason.mapId} " +
        s"index ${reason.mapIndex}")
    assert(reason.bmAddress == fixture.producer.blockManagerId,
      s"The fetch failure must carry the producer's own MapStatus address, without which " +
        s"MapOutputTracker removes nothing, but it carried ${reason.bmAddress}")
    assert(fixture.context.fetchFailed.contains(failure),
      "FetchFailedException must register itself on the task context so the executor reports a " +
        "FetchFailed task-end reason")

    assert(fixture.coordinatorRef.invalidationsSent.size == 1,
      s"An unreachable producer must be invalidated exactly once, but " +
        s"${fixture.coordinatorRef.invalidationsSent.size} invalidation(s) were sent")
    val invalidation = fixture.coordinatorRef.invalidationsSent.head
    assert(invalidation.reason == StreamingShuffleInvalidationReason.ConnectionTimeout,
      s"An unreachable producer must be invalidated as a connection timeout, but the reason was " +
        s"${invalidation.reason}")
    assert(invalidation.generation == fixture.producer.generation,
      s"The invalidation must name the generation that could not be reached, but it named " +
        s"${invalidation.generation}")
    assert(observedPartialReadInvalidations() == 1L,
      s"A producer given up on after the connect budget was spent is an invalidated partial read " +
        s"and must be counted as one, but ${observedPartialReadInvalidations()} was")

    fixture.context.markTaskFailed(failure)
    fixture.context.markTaskCompleted(Some(failure))
    assert(fixture.consumerLedgerKey.isEmpty,
      s"Every credit ledger the read opened must be released after an exhausted connect, but " +
        s"${fixture.backpressure.registeredStreams.mkString("[", ", ", "]")} remained")
    assert(fixture.backpressure.reservedReceiveQuotaBytes == 0L,
      s"Every byte of the executor's receive quota must be returned after an exhausted connect, " +
        s"but ${fixture.backpressure.reservedReceiveQuotaBytes} byte(s) were still charged")
    assert(fixture.connector.closeCallCount == 0,
      "The executor-scoped connector must outlive a failed read and is closed by the manager")
  }

  test("partial read invalidation and upstream recomputation trigger") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(block => stream.deliver(block))
    assert(stream.handler.acceptedBlockCount(fixture.partitionId) == blocks.size.toLong,
      "Every block must be accepted before the producer is lost, so that there is a partial read " +
        "to invalidate in the first place")

    val channelClosedAtInvalidation = new AtomicReference[Option[Boolean]](None)
    val queuedAtInvalidation = new AtomicReference[Option[Int]](None)
    val countedAtInvalidation = new AtomicReference[Option[Long]](None)
    fixture.coordinatorRef.observeInvalidation { _ =>
      channelClosedAtInvalidation.set(Some(stream.handler.isClosed))
      queuedAtInvalidation.set(Some(stream.handler.queuedEventCount))
      countedAtInvalidation.set(Some(observedPartialReadInvalidations()))
    }

    advancePastProducerTimeout(fixture.clock)
    intercept[FetchFailedException] {
      withTaskContext(fixture.context) {
        readRecords(records)
      }
    }

    assert(channelClosedAtInvalidation.get().contains(true),
      s"The reader must discard everything it took from the lost producer and close its channel " +
        s"before it reports the loss, but the channel state at that moment was " +
        s"${channelClosedAtInvalidation.get()}")
    assert(queuedAtInvalidation.get().contains(0),
      s"No block from the lost producer may remain reachable when the loss is reported, but " +
        s"${queuedAtInvalidation.get()} event(s) were still queued")

    assert(countedAtInvalidation.get().contains(1L),
      s"The partial read invalidation must be counted before the coordinator is told, but the " +
        s"counter read ${countedAtInvalidation.get()} at that moment")
    assert(observedPartialReadInvalidations() == 1L,
      s"Exactly one partial read invalidation must be recorded for one lost producer, but " +
        s"${observedPartialReadInvalidations()} was recorded")

    val sent = fixture.coordinatorRef.invalidationsSent
    assert(sent.size == 1,
      s"Exactly one producer generation must be invalidated, but ${sent.size} were")
    assert(sent.head.shuffleId == fixture.shuffleId && sent.head.capabilityToken == CapabilityToken,
      "An invalidation must name the shuffle and carry the capability token the coordinator issued")
    assert(sent.head.generation == fixture.producer.generation,
      s"The invalidation must name the exact producer generation that was lost, but it named " +
        s"${sent.head.generation}")
    assert(sent.head.reason == StreamingShuffleInvalidationReason.ConnectionTimeout,
      s"A producer lost to silence must be invalidated as a connection timeout, but the reason " +
        s"was ${sent.head.reason}")

    assert(fixture.context.fetchFailed.isDefined,
      "The task context must carry the fetch failure, which is what makes the executor report a " +
        "FetchFailed reason and the unmodified DAG scheduler resubmit the upstream stage")
    assert(fixture.connector.closeCallCount == 0,
      s"A reader must close only the channels it opened and never the executor-scoped connector, " +
        s"but the connector was closed ${fixture.connector.closeCallCount} time(s)")
  }

  test("a lost producer invalidates only its own partial read and leaves a peer's blocks intact") {
    startContext()
    val fixture = new ReaderFixture(numMaps = TwoProducers, completedMaps = Set(0, 1))
    val receiveQuotaBaseline = fixture.backpressure.reservedReceiveQuotaBytes
    val lostProducer = fixture.producers.head
    val survivingProducer = fixture.producers.last
    assert(lostProducer.mapIndex == 0 && survivingProducer.mapIndex == 1,
      s"The lost producer must be the first in read order -- read order is map-index order -- so " +
        s"that the peer's blocks are still queued rather than already consumed when the loss is " +
        s"reported, but the offered indexes were " +
        s"${fixture.producers.map(_.mapIndex).mkString(", ")}")
    assert(lostProducer.blockManagerId != survivingProducer.blockManagerId &&
        lostProducer.mapId != survivingProducer.mapId &&
        lostProducer.taskAttemptId != survivingProducer.taskAttemptId,
      "The two producers must be distinguishable in every field a fetch failure names, or an " +
        "assertion that only one of them was named proves nothing")

    val lostPayload = fixture.encodePartition(StreamedRecords, lostProducer.mapId)
    val lostBlocks = fixture.dataBlocksOf(lostPayload, BlockCount, lostProducer.mapId)
    val survivingPayload = fixture.encodePartition(PeerRecords, survivingProducer.mapId)
    val survivingBlocks =
      fixture.dataBlocksOf(survivingPayload, BlockCount, survivingProducer.mapId)

    val records = fixture.reader.read()
    assert(fixture.connector.streams.size == TwoProducers,
      s"A reader offered ${TwoProducers} producers must open a channel to each of them before it " +
        s"consumes anything, but opened ${fixture.connector.streams.size}")
    val lostStream = fixture.streamOf(lostProducer.mapIndex)
    val survivingStream = fixture.streamOf(survivingProducer.mapIndex)
    assert(fixture.consumerLedgerKeysOf(lostProducer.mapId).size == 1 &&
        fixture.consumerLedgerKeysOf(survivingProducer.mapId).size == 1,
      s"Each producer generation must hold a credit ledger of its own, but the lost producer " +
        s"holds ${fixture.consumerLedgerKeysOf(lostProducer.mapId).size} and the peer " +
        s"${fixture.consumerLedgerKeysOf(survivingProducer.mapId).size}")

    lostBlocks.foreach(block => lostStream.deliver(block))
    assert(lostStream.handler.acceptedBlockCount(fixture.partitionId) == lostBlocks.size.toLong,
      s"Every block of the producer about to be lost must have been accepted, but only " +
        s"${lostStream.handler.acceptedBlockCount(fixture.partitionId)} of ${lostBlocks.size} were")

    // The clock advances past the connection timeout FIRST, and only then does the peer deliver.
    advancePastProducerTimeout(fixture.clock)
    survivingBlocks.foreach(block => survivingStream.deliver(block))
    assert(lostStream.handler.isProducerSilent(fixture.partitionId),
      s"The producer that stopped sending must be reported silent after " +
        s"${ProducerConnectionTimeoutMillis} ms")
    assert(!survivingStream.handler.isProducerSilent(fixture.partitionId),
      s"The peer must not be reported silent: it delivered at this instant, and its channel " +
        s"reads ${survivingStream.handler.millisSinceInbound(fixture.partitionId)} ms of " +
        s"silence")
    val survivingQueuedEvents = survivingStream.handler.queuedEventCount
    val survivingQueuedBytes = survivingStream.handler.queuedByteCount
    val survivingAccepted = survivingStream.handler.acceptedBlockCount(fixture.partitionId)
    assert(survivingAccepted == survivingBlocks.size.toLong && survivingQueuedBytes > 0L,
      s"The peer must be holding all ${survivingBlocks.size} of its accepted blocks and their " +
        s"bytes before the loss, but holds ${survivingAccepted} block(s) and " +
        s"${survivingQueuedBytes} byte(s)")

    val lostQueuedAtInvalidation = new AtomicReference[Option[Int]](None)
    val lostClosedAtInvalidation = new AtomicReference[Option[Boolean]](None)
    val peerQueuedAtInvalidation = new AtomicReference[Option[Int]](None)
    val peerBytesAtInvalidation = new AtomicReference[Option[Long]](None)
    val peerClosedAtInvalidation = new AtomicReference[Option[Boolean]](None)
    val countedAtInvalidation = new AtomicReference[Option[Long]](None)
    fixture.coordinatorRef.observeInvalidation { _ =>
      lostQueuedAtInvalidation.set(Some(lostStream.handler.queuedEventCount))
      lostClosedAtInvalidation.set(Some(lostStream.handler.isClosed))
      peerQueuedAtInvalidation.set(Some(survivingStream.handler.queuedEventCount))
      peerBytesAtInvalidation.set(Some(survivingStream.handler.queuedByteCount))
      peerClosedAtInvalidation.set(Some(survivingStream.handler.isClosed))
      countedAtInvalidation.set(Some(observedPartialReadInvalidations()))
    }

    val failure = intercept[FetchFailedException] {
      withTaskContext(fixture.context) {
        readRecords(records)
      }
    }

    assert(lostQueuedAtInvalidation.get().contains(0),
      s"No block from the lost producer may remain reachable when the loss is reported, but " +
        s"${lostQueuedAtInvalidation.get()} event(s) were still queued")
    assert(lostClosedAtInvalidation.get().contains(true),
      s"The lost producer's channel must be shut before the loss is reported, but its state at " +
        s"that moment was ${lostClosedAtInvalidation.get()}")

    assert(peerQueuedAtInvalidation.get().contains(survivingQueuedEvents),
      s"The peer's queued blocks must be untouched by another producer's invalidation: " +
        s"${survivingQueuedEvents} were queued before it and " +
        s"${peerQueuedAtInvalidation.get()} at the instant it was reported")
    assert(peerBytesAtInvalidation.get().contains(survivingQueuedBytes),
      s"The peer's retained bytes must be untouched by another producer's invalidation: " +
        s"${survivingQueuedBytes} before it and ${peerBytesAtInvalidation.get()} at the instant " +
        s"it was reported")
    assert(peerClosedAtInvalidation.get().contains(false),
      s"The peer's channel must stay open through another producer's invalidation, but its state " +
        s"at that moment was ${peerClosedAtInvalidation.get()}")

    assert(countedAtInvalidation.get().contains(1L),
      s"The partial read invalidation must be counted before the coordinator is told, but the " +
        s"counter read ${countedAtInvalidation.get()} at that moment")
    assert(observedPartialReadInvalidations() == 1L,
      s"Exactly one partial read invalidation must be recorded when one of two producers is " +
        s"lost, but ${observedPartialReadInvalidations()} were recorded")
    val sent = fixture.coordinatorRef.invalidationsSent
    assert(sent.size == 1,
      s"Exactly one producer generation may be invalidated, but ${sent.size} were: " +
        s"${sent.map(_.generation).mkString(", ")}")
    assert(sent.head.generation == lostProducer.generation,
      s"The invalidation must name the lost generation, but named ${sent.head.generation}")
    assert(sent.head.generation != survivingProducer.generation,
      "The peer's generation must never be invalidated, or the recomputation would discard a map " +
        "output that is intact and still streaming")

    val reason = fetchFailedReasonOf(failure)
    assert(reason.shuffleId == fixture.shuffleId && reason.reduceId == fixture.partitionId,
      s"The fetch failure must name the shuffle and reduce partition being read, but named " +
        s"shuffle ${reason.shuffleId} partition ${reason.reduceId}")
    assert(reason.mapId == lostProducer.mapId && reason.mapIndex == lostProducer.mapIndex,
      s"The fetch failure must name the lost map output, but named map ${reason.mapId} index " +
        s"${reason.mapIndex}")
    assert(reason.bmAddress == lostProducer.blockManagerId,
      s"The fetch failure must carry the lost producer's own MapStatus address, without which " +
        s"MapOutputTracker removes nothing, but carried ${reason.bmAddress}")
    assert(reason.mapId != survivingProducer.mapId &&
        reason.mapIndex != survivingProducer.mapIndex &&
        reason.bmAddress != survivingProducer.blockManagerId,
      s"The fetch failure must not name the peer in any field, or the scheduler would " +
        s"recompute a map task whose output never failed, but it named map ${reason.mapId} " +
        s"index ${reason.mapIndex} at ${reason.bmAddress}")
    assert(fixture.context.fetchFailed.contains(failure),
      "The task context must carry the fetch failure, which is what makes the executor report a " +
        "FetchFailed reason and the unmodified DAG scheduler resubmit the upstream stage")

    // The peer's credit ledger is still held, so its receive window was not released along with the
    // lost producer's; the reader's own cleanup owns that, and it has not run.
    assert(fixture.consumerLedgerKeysOf(survivingProducer.mapId).size == 1,
      s"The peer's credit ledger must survive another producer's invalidation, but " +
        s"${fixture.consumerLedgerKeysOf(survivingProducer.mapId).size} remain registered")

    assert(readableRecordsOf(fixture, survivingProducer.mapIndex) == PeerRecords,
      "The peer's accepted blocks must still be readable in full after a sibling producer was " +
        "invalidated, or the invalidation was not per producer at all")
    assert(fixture.connector.closeCallCount == 0,
      s"A reader must close only the channels it opened and never the executor-scoped connector, " +
        s"but the connector was closed ${fixture.connector.closeCallCount} time(s)")

    fixture.context.markTaskFailed(failure)
    fixture.context.markTaskCompleted(Some(failure))
    assert(survivingStream.handler.isClosed,
      "Task completion must release the peer after the per-producer invalidation assertion")
    assert(fixture.backpressure.reservedReceiveQuotaBytes == receiveQuotaBaseline,
      s"Task completion must return the receive quota to $receiveQuotaBaseline byte(s), but " +
        s"${fixture.backpressure.reservedReceiveQuotaBytes} byte(s) remained")
  }

  test("several producers lost together are one fetch failure counted exactly once") {
    startContext()
    val fixture = new ReaderFixture(numMaps = TwoProducers, completedMaps = Set(0, 1))
    val receiveQuotaBaseline = fixture.backpressure.reservedReceiveQuotaBytes
    val injector = newFaultInjector(fixture.clock)
    val firstProducer = fixture.producers.head
    val secondProducer = fixture.producers.last

    val payloads = fixture.producers.map(producer =>
      producer -> fixture.encodePartition(StreamedRecords, producer.mapId))
    val records = fixture.reader.read()
    payloads.foreach { case (producer, payload) =>
      val stream = fixture.streamOf(producer.mapIndex)
      fixture.dataBlocksOf(payload, BlockCount, producer.mapId)
        .foreach(block => stream.deliver(block))
      assert(stream.handler.acceptedBlockCount(fixture.partitionId) == BlockCount.toLong,
        s"Producer index ${producer.mapIndex} must have delivered a partial read to invalidate, " +
          s"but only ${stream.handler.acceptedBlockCount(fixture.partitionId)} block(s) landed")
    }

    val lost = crashProducersTogether(injector, fixture.producers.map(producer =>
      fixture.streamOf(producer.mapIndex)))
    assert(injector.fireCount(StreamingShuffleFaultScenario.ConcurrentProducerFailures) ==
        TwoProducers,
      s"The concurrent-producer-failure scenario must have fired once per producer, but fired " +
        s"${injector.fireCount(StreamingShuffleFaultScenario.ConcurrentProducerFailures)} time(s)")
    assert(injector.networkPartitioned,
      "Losing several producers together must also make them unreachable, or the consumer would " +
        "still be able to talk to executors that are gone")
    assert(lost.forall(_.isCrashed),
      "Every producer the injector lost must be unable to put another frame on the wire")

    // One advance of the shared clock is all it takes now: neither channel has received anything
    // since, so both are past the connection timeout at the same reading.
    advancePastProducerTimeout(fixture.clock)
    assert(lost.forall(_.handler.isProducerSilent(fixture.partitionId)),
      s"Every lost producer must be reported silent after ${ProducerConnectionTimeoutMillis} ms")

    val failure = intercept[FetchFailedException] {
      withTaskContext(fixture.context) {
        readRecords(records)
      }
    }

    assert(observedPartialReadInvalidations() == 1L,
      s"Two producers lost together must be reported as one partial read invalidation, but " +
        s"${observedPartialReadInvalidations()} were recorded")
    val sent = fixture.coordinatorRef.invalidationsSent
    assert(sent.size == 1,
      s"Exactly one producer generation may be invalidated for one fetch failure, but " +
        s"${sent.size} were: ${sent.map(_.generation).mkString(", ")}")
    assert(sent.head.generation == firstProducer.generation,
      s"The reported loss must be the first producer in read order, which is the one the reader " +
        s"reached, but it named ${sent.head.generation}")
    assert(sent.head.reason == StreamingShuffleInvalidationReason.ConnectionTimeout,
      s"A producer lost to silence must be invalidated as a connection timeout, but the reason " +
        s"was ${sent.head.reason}")

    val reason = fetchFailedReasonOf(failure)
    assert(reason.mapId == firstProducer.mapId && reason.mapIndex == firstProducer.mapIndex &&
        reason.bmAddress == firstProducer.blockManagerId,
      s"The single fetch failure must name the producer the reader reached, but named map " +
        s"${reason.mapId} index ${reason.mapIndex} at ${reason.bmAddress}")
    assert(reason.mapId != secondProducer.mapId,
      s"One fetch failure must name one map output, but it named the second producer's map " +
        s"${reason.mapId} as well")
    assert(fixture.context.fetchFailed.contains(failure),
      "The task context must carry the one fetch failure, which is what makes the unmodified DAG " +
        "scheduler recompute the upstream stage and restore both dead map outputs")

    assert(fixture.streamOf(firstProducer.mapIndex).handler.queuedEventCount == 0 &&
        fixture.streamOf(firstProducer.mapIndex).handler.queuedByteCount == 0L,
      s"The reported producer's partial read must be discarded in full, but " +
        s"${fixture.streamOf(firstProducer.mapIndex).handler.queuedEventCount} event(s) and " +
        s"${fixture.streamOf(firstProducer.mapIndex).handler.queuedByteCount} byte(s) remain")
    assert(fixture.streamOf(firstProducer.mapIndex).handler.isClosed,
      "The reported producer's channel must be shut, or a block arriving after the discard could " +
        "be mixed into the recomputed input")

    fixture.context.markTaskFailed(failure)
    fixture.context.markTaskCompleted(Some(failure))
    assert(fixture.connector.streams.forall(_.handler.isClosed),
      "Task completion must release every producer handler after the one reported fetch failure")
    assert(fixture.backpressure.reservedReceiveQuotaBytes == receiveQuotaBaseline,
      s"Task completion must return the receive quota to $receiveQuotaBaseline byte(s), but " +
        s"${fixture.backpressure.reservedReceiveQuotaBytes} byte(s) remained")
  }

  test("checksum validation and retransmission") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val intact = fixture.dataBlocksOf(payload).head
    val corrupt = corruptedDataBlock(fixture.shuffleId, ProducerMapId, fixture.partitionId,
      intact.sequenceNumber(), intact.copyPayload())
    assert(!corrupt.verifyChecksum(),
      "A block whose stamped checksum has been altered must fail CRC32C verification")
    assert(intact.verifyChecksum(),
      "A block whose stamped checksum is correct must pass CRC32C verification")

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    stream.drainOutboundMessages()
    stream.deliver(corrupt)

    assert(stream.handler.corruptBlockCount(fixture.partitionId) == 1L,
      s"The corrupt block must be counted, but the count was " +
        s"${stream.handler.corruptBlockCount(fixture.partitionId)}")
    assert(stream.handler.acceptedBlockCount(fixture.partitionId) == 0L,
      s"A block that failed verification must never be admitted, but " +
        s"${stream.handler.acceptedBlockCount(fixture.partitionId)} was")
    assert(stream.handler.queuedEventCount == 0,
      "A block that failed verification must not be handed to the reduce task")
    assert(fixture.recordingMetrics.recordsRead == 0L,
      "No record of a corrupt block may become visible")

    val requests = stream.drainOutboundMessages().collect {
      case request: RetransmitRequestMessage => request
    }
    assert(requests.size == 1,
      s"Exactly one retransmission must be requested for one corrupt block, but ${requests.size} " +
        s"were")
    assert(requests.head.firstSequenceNumber() == intact.sequenceNumber() &&
        requests.head.lastSequenceNumber() == intact.sequenceNumber(),
      s"The retransmission window must be the inclusive single-element window " +
        s"[${intact.sequenceNumber()}, ${intact.sequenceNumber()}], but it was " +
        s"[${requests.head.firstSequenceNumber()}, ${requests.head.lastSequenceNumber()}]")
    assert(requests.head.blockCount() == 1L,
      s"A single-element inclusive window must span one block, but it spanned " +
        s"${requests.head.blockCount()}")
    assert(requests.head.contains(intact.sequenceNumber()),
      "The retransmission window must contain the position it was opened for, both bounds being " +
        "inclusive")
    assert(requests.head.partitionId() == fixture.partitionId,
      "A retransmission request must name the partition the corrupt block belonged to")

    stream.deliver(intact)
    stream.deliver(fixture.terminator(1L))
    assert(stream.handler.acceptedBlockCount(fixture.partitionId) == 1L,
      s"The replayed block must be admitted once its checksum verifies, but " +
        s"${stream.handler.acceptedBlockCount(fixture.partitionId)} block(s) were admitted")
    assert(readRecords(records) == expectedRecords,
      "A corrupt block repaired by retransmission must leave the reduce input complete and equal " +
        "to what the producer sent")
    assert(observedPartialReadInvalidations() == 0L,
      s"Corruption repaired inside the retained window must not invalidate a partial read, but " +
        s"${observedPartialReadInvalidations()} was recorded")
    assert(fixture.coordinatorRef.invalidationsSent.isEmpty,
      "Corruption repaired inside the retained window must not invalidate a producer")
  }

  test("a held end-of-stream terminates the flow-control ledger as well as the stream") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, blocks = 2)
    val corrupt = corruptedDataBlock(fixture.shuffleId, ProducerMapId, fixture.partitionId,
      blocks(1).sequenceNumber(), blocks(1).copyPayload())

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    val ledgerKey = fixture.consumerLedgerKey
    assert(ledgerKey.isDefined, "The reader must have registered a credit ledger for its partition")
    stream.deliver(blocks.head)
    stream.deliver(corrupt)
    assert(stream.handler.quarantinedPositionCount(fixture.partitionId) == 1,
      s"The corrupt position must be quarantined for a replay, but " +
        s"${stream.handler.quarantinedPositionCount(fixture.partitionId)} position(s) were")
    assert(stream.handler.expectedSequenceNumber(fixture.partitionId) == 1L,
      s"The consumer must still be waiting for position 1, but expects " +
        s"${stream.handler.expectedSequenceNumber(fixture.partitionId)}")

    stream.deliver(fixture.terminator(blocks.size.toLong))
    assert(stream.handler.deferredTerminationCount(fixture.partitionId) == 1L,
      s"The end-of-stream must be held while the repair is outstanding, but " +
        s"${stream.handler.deferredTerminationCount(fixture.partitionId)} was held")
    assert(stream.handler.rejectedTerminationCount(fixture.partitionId) == 0L,
      "A terminator held for a repair must not also be counted as refused")
    assert(!stream.handler.isStreamComplete(fixture.partitionId),
      "A held end-of-stream must leave the stream unfinished")
    assert(!fixture.backpressure.isStreamTerminated(ledgerKey.get),
      "A held end-of-stream must leave the flow-control ledger unfinished too, so the producer " +
        "liveness timer stays armed for the replay that is still owed")

    stream.deliver(blocks(1))
    assert(stream.handler.isStreamComplete(fixture.partitionId),
      "Applying the held end-of-stream must finish the stream")
    assert(stream.handler.announcedBlockCount(fixture.partitionId).contains(blocks.size.toLong),
      s"The applied terminator must publish the total it named, but published " +
        s"${stream.handler.announcedBlockCount(fixture.partitionId)}")
    assert(fixture.backpressure.isStreamTerminated(ledgerKey.get),
      "A deferred end-of-stream must reach the flow-control ledger exactly as an immediate one " +
        "does, or a finished producer goes on being measured for liveness")
    assert(readRecords(records) == expectedRecords,
      "A partition whose end-of-stream was held for a repair must still read complete")
    assert(fixture.coordinatorRef.invalidationsSent.isEmpty,
      "A repair that completed must invalidate no producer")
  }

  test("a consumer's recurring records are bounded by the executor, not by the channel") {
    startContext()
    val fixture = new ReaderFixture(numMaps = TwoProducers, completedMaps = Set(0, 1))
    fixture.reader.read()
    val first = fixture.streamOf(0)
    val second = fixture.streamOf(1)
    MemorySpillManager.resetSharedStateForTesting()
    val aggregator = StreamingShuffleClientHandler.misaddressedFrameLogAggregator
    assert(aggregator.occurrenceCount == 0L && aggregator.unreportedCount == 0L,
      s"The shared reset must leave the executor's window open and its tally at zero, but the " +
        s"tally read ${aggregator.occurrenceCount} with " +
        s"${aggregator.unreportedCount} unreported")

    val foreignPartition = fixture.partitionId + 41
    val stray = fixture.encodePartition(StreamedRecords)
    val (_, captured) = capturingStreamingLogs {
      first.deliver(dataBlock(fixture.shuffleId, first.location.mapId, foreignPartition, 0L, stray))
      second.deliver(
        dataBlock(fixture.shuffleId, second.location.mapId, foreignPartition, 0L, stray))
    }

    assert(first.handler.misaddressedFrameCount == 1L &&
        second.handler.misaddressedFrameCount == 1L,
      s"Each channel must count the frame it dropped, but they counted " +
        s"${first.handler.misaddressedFrameCount} and ${second.handler.misaddressedFrameCount}")
    assert(aggregator.occurrenceCount == 2L,
      s"The executor's tally must have both occurrences, but it read ${aggregator.occurrenceCount}")
    assert(aggregator.unreportedCount == 1L,
      s"The second occurrence must have been accounted rather than reported, so exactly one must " +
        s"be unreported, but ${aggregator.unreportedCount} were")
    val dropped = captured.filter(record =>
      record.isDefaultLevel && record.message.contains("dropped"))
    assert(dropped.size == 1,
      s"Two channels dropping a frame must produce one default-level record, not one each, but " +
        s"produced ${dropped.size}: ${dropped.map(_.message).mkString(" | ")}")
    assert(dropped.head.message.contains("on this executor"),
      s"The admitted record must state the executor-wide figure, because that is what a reader " +
        s"needs to tell one bad producer from a bad executor, but it read: ${dropped.head.message}")

    MemorySpillManager.resetSharedStateForTesting()
  }

  test("one lost producer is reported once at default level, by the reader") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    stream.deliver(blocks.head)

    val (failure, captured) = capturingStreamingLogs {
      stream.closeChannel()
      stream.handler.channelInactive(stream.client)
      stream.awaitDataPlane("channel-inactive processing")
      assert(stream.handler.isProducerLost,
        "The handler must have observed the loss, or there is no duplicate record to rule out")
      intercept[FetchFailedException] {
        withTaskContext(fixture.context) {
          readRecords(records)
        }
      }
    }
    assert(fetchFailedReasonOf(failure).reduceId == fixture.partitionId,
      "The fetch failure must name the partition whose producer was lost")

    assertSingleDefaultLevelOwner(
      captured.filter(record => record.message.contains("invalidated partial") ||
        record.message.contains("consumer failed") ||
        record.message.contains("closed the connection")),
      owner = "StreamingShuffleReader",
      silent = Seq("StreamingShuffleClientHandler"),
      what = "one lost producer")
  }

  test("a gap of several positions is repaired by one request naming the whole window") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)
    assert(blocks.size >= 4,
      s"this case needs a gap of at least two positions inside a longer partition, but the " +
        s"fixture framed ${blocks.size} block(s)")
    val outOfOrder = blocks(3)
    val missing = Seq(blocks(1), blocks(2))

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    stream.drainOutboundMessages()
    stream.deliver(blocks.head)
    stream.deliver(outOfOrder)

    val requests = stream.drainOutboundMessages().collect {
      case request: RetransmitRequestMessage => request
    }
    assert(requests.size == 1,
      s"a gap must be repaired by exactly ONE request naming the whole run, but " +
        s"${requests.size} frame(s) were written: " +
        requests.map(request =>
          s"[${request.firstSequenceNumber()}, ${request.lastSequenceNumber()}]").mkString(", "))
    val request = requests.head
    assert(request.firstSequenceNumber() == missing.head.sequenceNumber() &&
        request.lastSequenceNumber() == outOfOrder.sequenceNumber(),
      s"the window must span the missing positions and the block that revealed them, but it was " +
        s"[${request.firstSequenceNumber()}, ${request.lastSequenceNumber()}]")
    assert(request.blockCount() == 3L,
      s"the request must name three positions but named ${request.blockCount()}")
    (missing.map(_.sequenceNumber()) :+ outOfOrder.sequenceNumber()).foreach { position =>
      assert(request.contains(position),
        s"position $position must be covered by the one repair request")
    }
    assert(request.partitionId() == fixture.partitionId,
      "a retransmission request must name the partition whose sequence gapped")

    missing.foreach(stream.deliver)
    blocks.drop(3).foreach(stream.deliver)
    stream.deliver(fixture.terminator(blocks.size.toLong))
    assert(stream.handler.acceptedBlockCount(fixture.partitionId) == blocks.size.toLong,
      s"every position must be admitted once the run has been replayed, but " +
        s"${stream.handler.acceptedBlockCount(fixture.partitionId)} of ${blocks.size} were")
    assert(readRecords(records) == expectedRecords,
      "a gap repaired by one ranged retransmission must leave the reduce input complete and " +
        "equal to what the producer sent")
    assert(observedPartialReadInvalidations() == 0L,
      s"a repair inside the retained window must not invalidate a partial read, but " +
        s"${observedPartialReadInvalidations()} was recorded")
  }

  test("a corrupt block that can no longer be replayed escalates to a fetch failure") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)
    val corrupt = corruptedDataBlock(fixture.shuffleId, ProducerMapId, fixture.partitionId,
      blocks.head.sequenceNumber(), blocks.head.copyPayload())

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    stream.closeChannel()
    stream.drainOutboundMessages()
    stream.deliver(corrupt)

    val outbound = stream.drainOutboundMessages()
    assert(!outbound.exists(frame => frame.isInstanceOf[RetransmitRequestMessage]),
      "A replay must not be asked for once the bytes behind it can no longer be served")

    val failure = intercept[FetchFailedException] {
      readRecords(records)
    }
    assert(fetchFailedReasonOf(failure).shuffleId == fixture.shuffleId,
      "Corruption outside the repairable window must recover through stage recomputation, which " +
        "means a fetch failure naming the shuffle being read")
    val diagnosis: Seq[Throwable] =
      Option(failure.getCause).toSeq ++ failure.getSuppressed.toSeq
    val checksumConditionReported = diagnosis.exists {
      case throwable: SparkThrowable =>
        throwable.getCondition == ChecksumVerifyFailedCondition
      case _ => false
    }
    assert(checksumConditionReported,
      s"The typed checksum condition must survive into the fetch failure so the recomputation is " +
        s"diagnosable, but the attached failures were " +
        s"${diagnosis.map(_.getClass.getName).mkString("[", ", ", "]")}")
    assert(observedPartialReadInvalidations() == 1L,
      s"Escalating corruption must invalidate the partial read exactly once, but " +
        s"${observedPartialReadInvalidations()} was recorded")
    assert(fixture.coordinatorRef.invalidationsSent.size == 1,
      s"Escalating corruption must invalidate exactly one producer generation, but " +
        s"${fixture.coordinatorRef.invalidationsSent.size} were invalidated")
  }

  test("an orderly end of stream announcing zero blocks yields an empty iterator") {
    startContext()
    val fixture = new ReaderFixture()

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    stream.deliver(fixture.terminator(0L))

    assert(!records.hasNext,
      "A stream that announced zero blocks must yield an empty iterator")
    assert(readRecords(records).isEmpty,
      "An empty partition must surface no records at all")
    assert(fixture.recordingMetrics.recordsRead == 0L,
      s"An empty partition must report no records read, but the reporter counted " +
        s"${fixture.recordingMetrics.recordsRead}")
    assert(observedPartialReadInvalidations() == 0L,
      "An empty partition is completion, not failure, so nothing may be invalidated")
    assert(fixture.coordinatorRef.invalidationsSent.isEmpty,
      "An empty partition must not invalidate its producer")
  }

  test("a stream that ends short of its announced total fails the fetch") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    stream.deliver(blocks.head)
    stream.deliver(fixture.terminator(blocks.size.toLong))

    val failure = intercept[FetchFailedException] {
      readRecords(records)
    }
    val truncationReason = fetchFailedReasonOf(failure)
    assert(truncationReason.reduceId == fixture.partitionId,
      s"A truncated partition must fail the fetch for the partition being read, but the failure " +
        s"named ${truncationReason.reduceId}")
    assert(fixture.coordinatorRef.invalidationsSent.size == 1,
      s"A truncated stream must invalidate its producer exactly once, but " +
        s"${fixture.coordinatorRef.invalidationsSent.size} invalidation(s) were sent")
    assert(fixture.coordinatorRef.invalidationsSent.head.reason ==
        StreamingShuffleInvalidationReason.IncompleteStream,
      s"A truncated stream must be invalidated as an incomplete stream, but the reason was " +
        s"${fixture.coordinatorRef.invalidationsSent.head.reason}")
    assert(observedPartialReadInvalidations() == 1L,
      s"A truncated stream must record one partial read invalidation, but " +
        s"${observedPartialReadInvalidations()} was recorded")
  }

  test("the reader declares no stop method and cleans up through the task completion listener") {
    startContext()
    val fixture = new ReaderFixture()

    // The reader trait exposes exactly one member, so there is no stop() hook to release resources
    // from.
    val traitMembers = classOf[ShuffleReader[_, _]].getMethods.map(_.getName).toSet
    assert(traitMembers == Set("read"),
      s"The shuffle reader service-provider interface must declare read() alone, but it declared " +
        s"${traitMembers.toSeq.sorted.mkString("[", ", ", "]")}")
    val readerMethods = classOf[StreamingShuffleReader[_, _]].getDeclaredMethods.map(_.getName)
    assert(!readerMethods.contains("stop"),
      s"The streaming reader must declare no stop(), because the trait has none and a method " +
        s"nothing calls would leak every resource it was meant to release")

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)
    blocks.foreach(block => stream.deliver(block))
    stream.deliver(fixture.terminator(blocks.size.toLong))
    assert(readRecords(records) == expectedRecords, "The read must succeed before cleanup is run")
    assert(fixture.consumerLedgerKey.isDefined,
      "The reader must hold a credit ledger for the partition it read while the task is running")

    fixture.context.markTaskCompleted(None)
    assertReleased(fixture, stream, "a task that succeeded")

    val lateListenerRan = new AtomicInteger(0)
    fixture.context.addTaskCompletionListener[Unit](_ => lateListenerRan.incrementAndGet())
    assert(lateListenerRan.get() == 1,
      "A listener added to an already completed task must be called immediately")
    fixture.context.markTaskCompleted(None)
    assertReleased(fixture, stream, "a task whose completion path ran twice")
  }

  test("cleanup releases everything on success on failure and on cancellation") {
    startContext()

    val succeeded = new ReaderFixture()
    succeeded.reader.read()
    val successStream = succeeded.connector.onlyStream
    succeeded.context.markTaskCompleted(None)
    assertReleased(succeeded, successStream, "a task that succeeded")

    val failed = new ReaderFixture()
    failed.reader.read()
    val failedStream = failed.connector.onlyStream
    val cause = new SparkException("injected task failure")
    failed.context.markTaskFailed(cause)
    failed.context.markTaskCompleted(Some(cause))
    assertReleased(failed, failedStream, "a task that failed")

    val cancelled = new ReaderFixture()
    cancelled.reader.read()
    val cancelledStream = cancelled.connector.onlyStream
    cancelled.context.markInterrupted("cancelled by the test")
    cancelled.context.markTaskCompleted(None)
    assertReleased(cancelled, cancelledStream, "a task that was cancelled")
  }

  test("cleanup runs in reverse order of acquisition and survives a broken listener") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(block => stream.deliver(block))
    // Exactly one record is consumed, so the read is abandoned in the middle of a partition: blocks
    // are still queued, receive quota is still charged for them, the channel is still open and the
    // ledger still registered.
    assert(records.hasNext, "The read must have input before it is abandoned")
    records.next()
    assert(stream.handler.queuedEventCount > 0,
      "Blocks the reduce task never reached must still be queued, or there is no buffered state " +
        "whose release could be ordered")
    assert(fixture.consumerLedgerKey.isDefined,
      "The credit ledger must be held while the task runs, since it is acquired before any " +
        "channel is opened and is therefore the last thing that may be released")
    assert(fixture.backpressure.reservedReceiveQuotaBytes > 0L,
      "Bytes admitted and not yet consumed must still be charged to the executor's receive " +
        "budget, or the release of that charge could not be ordered against anything")

    val atChannelClose = new AtomicReference[Option[(Boolean, Int, Long, Boolean)]](None)
    stream.channel.closeFuture().addListener(new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture): Unit = {
        atChannelClose.set(Some((
          stream.handler.isClosed,
          stream.handler.queuedEventCount,
          fixture.backpressure.reservedReceiveQuotaBytes,
          fixture.consumerLedgerKey.isDefined)))
      }
    })

    val brokenListenerRuns = new AtomicInteger(0)
    val brokenListenerFailure = new SparkException("a completion listener of another component")
    fixture.context.addTaskCompletionListener[Unit] { _ =>
      brokenListenerRuns.incrementAndGet()
      throw brokenListenerFailure
    }

    val surfaced = intercept[TaskCompletionListenerException] {
      fixture.context.markTaskCompleted(None)
    }
    assert(brokenListenerRuns.get() == 1,
      s"The failing listener must have run exactly once, but it ran ${brokenListenerRuns.get()} " +
        s"time(s)")
    assert(surfaced.getMessage.contains(brokenListenerFailure.getMessage),
      s"A failure raised by a completion listener must be surfaced rather than logged away, but " +
        s"the reported failure read: ${surfaced.getMessage}")

    val (handlerClosed, queuedAtClose, quotaAtClose, ledgerAtClose) = atChannelClose.get()
      .getOrElse(fail("The reader must have closed the producer channel during task completion"))
    assert(handlerClosed,
      "The handler must be closed before its channel is, because the handler is what stops " +
        "accepting frames: closing the channel first would leave a live handler willing to " +
        "allocate for a frame already in flight")
    assert(queuedAtClose == 0,
      s"Every queued block must be discarded before the channel closes, so nothing stays " +
        s"reachable from a handler whose channel has gone, but ${queuedAtClose} event(s) remained")
    assert(quotaAtClose == 0L,
      s"Every byte charged to the executor's receive budget -- including bytes taken from the " +
        s"queue but never consumed -- must be returned before the channel closes, but " +
        s"${quotaAtClose} byte(s) were still charged")
    assert(ledgerAtClose,
      "The credit ledger must outlive the channel and be released last, which is the exact " +
        "reverse of acquisition: the ledger is registered before a channel exists, so releasing " +
        "it first would leave a live channel admitting bytes against an allowance nothing holds")

    assertReleased(fixture, stream, "a task whose completion listener failed")
  }

  test("the reader never retains a transport buffer") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(block => stream.deliver(block))
    stream.deliver(fixture.terminator(blocks.size.toLong))

    assert(readRecords(records) == expectedRecords,
      "The read must succeed over frames whose transport buffers have been overwritten, which is " +
        "only possible if the consumer copied each payload out at decode time")
    fixture.context.markTaskCompleted(None)

    // And the reference contract, observed on the transport's own hand-off rather than on the
    // fixture's.
    val delivered = stream.deliveredFrameBuffers
    assert(delivered.size == blocks.size + 1,
      s"Every frame must have been delivered through a counting buffer, but ${delivered.size} of " +
        s"${blocks.size + 1} were")
    assert(delivered.forall(buffer => buffer.callsToRetain == 0),
      s"The consumer must copy a frame's bytes rather than retain the transport's buffer, but " +
        s"${delivered.count(buffer => buffer.callsToRetain > 0)} buffer(s) were retained")
    assert(delivered.forall(buffer => buffer.callsToRelease == 1),
      s"The transport owns a one-way message body and releases it exactly once, but " +
        s"${delivered.count(buffer => buffer.callsToRelease != 1)} buffer(s) were released a " +
        s"different number of times")
    // Signed, so the diagnosis names the direction: a positive imbalance is the consumer holding a
    // reference the transport has already accounted for, which is the leak this asserts against.
    assert(delivered.forall(buffer => buffer.outstandingReferences <= 0),
      s"No consumer may be left holding a reference on a transport buffer, but " +
        s"${delivered.count(buffer => buffer.outstandingReferences > 0)} were")
  }

  test("cleanup releases a reader's resources in reverse acquisition order") {
    startContext()
    val recorder = new StreamingShuffleCleanupRecorder
    val fixture = new ReaderFixture(cleanupRecorder = Some(recorder))
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(block => stream.deliver(block))

    val queuedBeforeCleanup = stream.handler.queuedEventCount
    assert(queuedBeforeCleanup > 0,
      "The fixture must reach cleanup with frames still queued, or the buffer release it is " +
        "asserting the position of does not happen at all")
    val chargedBeforeCleanup = fixture.backpressure.reservedReceiveQuotaBytes
    assert(chargedBeforeCleanup > 0L,
      "and with receive quota still charged, for the same reason")
    assert(fixture.consumerLedgerKey.isDefined, "and with a credit ledger still open")
    assert(stream.client.isActive(), "and with the channel still open")

    recorder.observeWith(() => StreamingShuffleCleanupSnapshot(
      queuedEvents = stream.handler.queuedEventCount,
      reservedQuotaBytes = fixture.backpressure.reservedReceiveQuotaBytes,
      channelOpen = stream.client.isActive(),
      ledgerRegistered = fixture.consumerLedgerKey.isDefined))
    // The channel's own close notification, which is the transport telling us when that step ran
    // rather than the fixture guessing.
    stream.channel.closeFuture().addListener(new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture): Unit = {
        recorder.record(StreamingShuffleCleanupRecorder.ChannelClosed, 0L)
      }
    })
    recorder.arm()

    fixture.context.markTaskCompleted(None)

    assert(recorder.releaseOrder === Seq(
        StreamingShuffleCleanupRecorder.ReceiveQuotaReturned,
        StreamingShuffleCleanupRecorder.ChannelClosed,
        StreamingShuffleCleanupRecorder.CreditLedgerReleased),
      s"A reader must unwind in reverse acquisition order, but it released " +
        s"${recorder.releaseOrder.mkString("[", ", ", "]")}")

    // Each step's snapshot, which is what makes the sequence above an assertion about the
    // production order rather than about the order this fixture happened to observe it in.
    val quotaStep = recorder.snapshotOf(StreamingShuffleCleanupRecorder.ReceiveQuotaReturned)
      .getOrElse(fail("The receive quota must have been returned"))
    assert(quotaStep.queuedEvents == 0,
      s"Buffers are released before quota is, so no frame may still be queued when the quota " +
        s"comes back, yet ${quotaStep.queuedEvents} were")
    assert(quotaStep.channelOpen,
      "and the channel must still be open, because quota is returned before it closes")
    assert(quotaStep.ledgerRegistered,
      "and the credit ledger must still be open, because it is the last thing released")
    assert(recorder.bytesOf(StreamingShuffleCleanupRecorder.ReceiveQuotaReturned)
        .contains(chargedBeforeCleanup),
      s"Every byte that was charged must come back in that one release, but " +
        s"${recorder.bytesOf(StreamingShuffleCleanupRecorder.ReceiveQuotaReturned)} of " +
        s"$chargedBeforeCleanup did")

    val channelStep = recorder.snapshotOf(StreamingShuffleCleanupRecorder.ChannelClosed)
      .getOrElse(fail("The channel must have been closed"))
    assert(channelStep.reservedQuotaBytes == 0L,
      s"The quota must already be back when the channel closes, yet " +
        s"${channelStep.reservedQuotaBytes} byte(s) were still charged")
    assert(channelStep.ledgerRegistered,
      "and the credit ledger must still be open when the channel closes")

    val ledgerStep = recorder.snapshotOf(StreamingShuffleCleanupRecorder.CreditLedgerReleased)
      .getOrElse(fail("The credit ledger must have been released"))
    assert(!ledgerStep.channelOpen,
      "The channel must already be closed when the ledger is released, or a frame arriving in " +
        "between would be admitted against a ledger about to disappear")
    assert(ledgerStep.reservedQuotaBytes == 0L, "and no quota may still be charged")
    assert(ledgerStep.queuedEvents == 0, "and no frame may still be queued")

    // And the end state, which the existing cleanup assertions describe.
    assertReleased(fixture, stream, "a task completed with frames still queued")
  }

  private def assertReleased(
      fixture: ReaderFixture,
      stream: StreamingShuffleTestProducerStream,
      description: String): Unit = {
    assert(stream.handler.isClosed,
      s"The consumer handler of $description must be closed by the task completion listener")
    assert(!stream.client.isActive(),
      s"The consumer channel of $description must be closed by the task completion listener")
    assert(stream.handler.queuedEventCount == 0,
      s"No block of $description may survive task completion, but " +
        s"${stream.handler.queuedEventCount} event(s) did")
    assert(fixture.consumerLedgerKey.isEmpty,
      s"Every credit ledger $description opened must be released, but " +
        s"${fixture.backpressure.registeredStreams.mkString("[", ", ", "]")} remained")
    assert(fixture.backpressure.reservedReceiveQuotaBytes == 0L,
      s"Every byte of the executor's receive quota that $description held must be returned, but " +
        s"${fixture.backpressure.reservedReceiveQuotaBytes} byte(s) were still charged")
    assert(fixture.connector.closeCallCount == 0,
      s"The executor-scoped connector must outlive $description and is closed by the manager, " +
        s"never by a reader, but it was closed ${fixture.connector.closeCallCount} time(s)")
  }

  test("read metrics land on the task's existing shuffle read accumulators") {
    startContext()
    val fixture = new ReaderFixture(lookupAdvanceMillis = LookupWaitMillis, useTaskReporter = true)
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(block => stream.deliver(block))
    stream.deliver(fixture.terminator(blocks.size.toLong))
    assert(readRecords(records) == expectedRecords, "The read must succeed before it is audited")

    val taskRead = fixture.context.taskMetrics.shuffleReadMetrics
    assert(taskRead.recordsRead == StreamedRecords.size.toLong,
      s"Records read must reach the task's own accumulator, but it holds ${taskRead.recordsRead}")
    assert(taskRead.remoteBlocksFetched == blocks.size.toLong,
      s"Every block delivered by a remote producer must be counted as remotely fetched, but the " +
        s"accumulator holds ${taskRead.remoteBlocksFetched} of ${blocks.size}")
    assert(taskRead.remoteBytesRead == payload.length.toLong,
      s"Remote bytes read must equal the payload bytes that crossed the network, but the " +
        s"accumulator holds ${taskRead.remoteBytesRead} of ${payload.length}")
    assert(taskRead.localBlocksFetched == 0L,
      s"A producer on another executor must never be accounted local, but " +
        s"${taskRead.localBlocksFetched} block(s) were")
    assert(taskRead.fetchWaitTime >= LookupWaitMillis,
      s"Time spent waiting on the rendezvous must be charged to fetch wait time, but the " +
        s"accumulator holds ${taskRead.fetchWaitTime} ms and the lookup alone took " +
        s"${LookupWaitMillis} ms")
    assert(taskRead.totalBytesRead == payload.length.toLong,
      s"Total bytes read must be the remote bytes and nothing else on this path, but it was " +
        s"${taskRead.totalBytesRead}")
    assert(fixture.context.taskMetrics.memoryBytesSpilled == 0L &&
        fixture.context.taskMetrics.diskBytesSpilled == 0L,
      "A streaming read spills nothing, and it must report that on the existing spill " +
        "accumulators rather than inventing counters of its own")
    assert(observedSpillCount() == 0L,
      s"A streaming read must record no spill, but the metrics source reports " +
        s"${observedSpillCount()}")
  }

  test("read metrics are reported only from the task thread") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    val pool = ThreadUtils.newDaemonSingleThreadExecutor("streaming-shuffle-reader-suite-io")
    try {
      awaitJavaFuture(pool.submit(new Callable[Unit] {
        override def call(): Unit = {
          blocks.foreach(block => stream.deliver(block))
          stream.deliver(fixture.terminator(blocks.size.toLong))
        }
      }))
    } finally {
      pool.shutdownNow()
    }
    assert(readRecords(records) == expectedRecords,
      "Frames enqueued by an I/O thread must be turned into records by the task thread")

    assertSingleThreadedReporting(fixture.recordingMetrics.foreignThreadCallCount,
      "the streaming shuffle read metrics reporter")
    assert(fixture.recordingMetrics.reportingThread.contains(Thread.currentThread()),
      s"The reporter must only be called from the thread driving the read, but it was called " +
        s"from ${fixture.recordingMetrics.reportingThread.map(_.getName).getOrElse("no thread")}")
    assert(fixture.recordingMetrics.remoteBlocksFetched == blocks.size.toLong,
      s"Every delivered block must be reported, but only " +
        s"${fixture.recordingMetrics.remoteBlocksFetched} of ${blocks.size} were")
    assert(fixture.recordingMetrics.mergedMetricsTotal == 0L,
      s"Streaming shuffle declines push-based merge, so every merge counter must " +
        s"stay at zero, but they summed to ${fixture.recordingMetrics.mergedMetricsTotal}")
  }

  test("a failure observed on an io thread is re-thrown on the task thread") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(block => stream.deliver(block))

    val injected = new SparkException("a failure observed on a netty event loop thread")
    injectFromIoThread(stream, injected)

    val failure = intercept[FetchFailedException] {
      withTaskContext(fixture.context) {
        records.hasNext
      }
    }
    val diagnosis = diagnosisOf(failure)
    assert(diagnosis.contains(injected),
      s"The failure the I/O thread observed must survive into the fetch failure, but the " +
        s"attached failures were ${diagnosis.map(_.getMessage).mkString("[", ", ", "]")}")
    assert(fixture.context.fetchFailed.isDefined,
      "The fetch failure must be asserted on the task context from the task thread, because that " +
        "is what the executor consults when it chooses between FetchFailed and ExceptionFailure")
  }

  test("every advance after the first raises the latched failure, from next as from hasNext") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(block => stream.deliver(block))

    val firstRecord = withTaskContext(fixture.context)(records.next())
    val secondRecord = withTaskContext(fixture.context)(records.next())
    assert(StreamedRecords.contains((firstRecord._1, firstRecord._2)) &&
        StreamedRecords.contains((secondRecord._1, secondRecord._2)),
      s"The read must yield real records before the failure, but it yielded $firstRecord and " +
        s"$secondRecord")

    // Deliberately NO `hasNext` between the last successful record and the injection.
    val injected = new SparkException("a channel failure observed on a netty event loop thread")
    injectFromIoThread(stream, injected)

    val fromNext = intercept[FetchFailedException] {
      withTaskContext(fixture.context)(records.next())
    }
    assert(diagnosisOf(fromNext).contains(injected),
      s"The converted failure must carry the injected cause, but it carried " +
        s"${diagnosisOf(fromNext).map(_.getMessage).mkString("[", ", ", "]")}")
    assert(fetchFailedReasonOf(fromNext).reduceId == fixture.partitionId,
      s"and it must name the partition being read, but it named " +
        s"${fetchFailedReasonOf(fromNext).reduceId}")

    val secondFromNext = intercept[FetchFailedException] {
      withTaskContext(fixture.context)(records.next())
    }
    assert(secondFromNext eq fromNext,
      "next() must keep re-throwing the latched failure once the queued marker is gone, but it " +
        s"raised a different one: ${secondFromNext.getMessage}")

    val laterAdvances: Seq[FetchFailedException] = Seq(
      intercept[FetchFailedException](withTaskContext(fixture.context)(records.hasNext)),
      intercept[FetchFailedException](withTaskContext(fixture.context)(records.next())),
      intercept[FetchFailedException](withTaskContext(fixture.context)(records.hasNext)),
      intercept[FetchFailedException](withTaskContext(fixture.context)(records.next())))
    assert(laterAdvances.forall(_ eq fromNext),
      "Every advance must keep raising the one latched failure, but at least one raised a " +
        s"different one: ${laterAdvances.map(_.getMessage).distinct.mkString("[", ", ", "]")}")

    assert(fixture.recordingMetrics.recordsRead == 2L,
      s"Exactly the two records taken before the failure may be counted as read, but " +
        s"${fixture.recordingMetrics.recordsRead} were")
    assert(fixture.context.fetchFailed.contains(fromNext),
      "The task context must still name that failure, because that is what the executor consults " +
        "when it chooses between FetchFailed and ExceptionFailure")
    assert(fixture.coordinatorRef.invalidationsSent.size == 1,
      s"A producer lost once must be invalidated once however many advances are refused, but " +
        s"${fixture.coordinatorRef.invalidationsSent.size} invalidation(s) were sent")
  }

  test("an end of stream that arrives while a repair is outstanding is applied when the repair " +
      "lands") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)
    assert(blocks.size >= 3, s"the case needs a gap with blocks on both sides of it, but the " +
      s"fixture cut ${blocks.size}")

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    stream.deliver(blocks.head)
    blocks.drop(2).foreach(block => stream.deliver(block))
    stream.deliver(fixture.terminator(blocks.size.toLong))
    blocks.drop(1).foreach(block => stream.deliver(block))

    val read = withTaskContext(fixture.context)(readRecords(records))
    assert(read == expectedRecords,
      s"every record must be read once the repair completed the stream, but ${read.size} of " +
        s"${expectedRecords.size} were")
    assert(observedPartialReadInvalidations() == 0L,
      s"nothing may be invalidated for a stream that completed, but " +
        s"${observedPartialReadInvalidations()} invalidation(s) were recorded")
    assert(fixture.coordinatorRef.invalidationsSent.isEmpty,
      s"and no producer generation may be invalidated, but " +
        s"${fixture.coordinatorRef.invalidationsSent.size} were")
    assert(fixture.context.fetchFailed.isEmpty,
      "and no fetch failure may be reported, because the producer was never lost")
  }

  test("a duplicate block that arrives after the stream ended is discarded rather than reported " +
      "as a lost producer") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)
    assert(blocks.size >= 2, s"the case needs a block to re-deliver behind the frontier, but the " +
      s"fixture cut ${blocks.size}")

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(block => stream.deliver(block))
    stream.deliver(fixture.terminator(blocks.size.toLong))
    stream.deliver(blocks.head)
    stream.deliver(blocks(blocks.size - 2))

    val read = withTaskContext(fixture.context)(readRecords(records))
    assert(read == expectedRecords,
      s"every record must still be read exactly once, but ${read.size} of " +
        s"${expectedRecords.size} were")
    assert(stream.handler.duplicateBlockCount(fixture.partitionId) == 2L,
      s"both redundant copies must be counted as duplicates, but " +
        s"${stream.handler.duplicateBlockCount(fixture.partitionId)} were")
    assert(stream.handler.blocksAfterTerminationCount(fixture.partitionId) == 0L,
      s"and none of them may be counted as a block past the end of stream, but " +
        s"${stream.handler.blocksAfterTerminationCount(fixture.partitionId)} were")
    assert(observedPartialReadInvalidations() == 0L,
      s"nothing may be invalidated for a stream that completed, but " +
        s"${observedPartialReadInvalidations()} invalidation(s) were recorded")
    assert(fixture.coordinatorRef.invalidationsSent.isEmpty,
      s"and no producer generation may be invalidated, but " +
        s"${fixture.coordinatorRef.invalidationsSent.size} were")
    assert(fixture.context.fetchFailed.isEmpty,
      "and no fetch failure may be reported, because the producer was never lost")

    val ledgerKey = fixture.consumerLedgerKey
    assert(ledgerKey.isDefined,
      "The reader must hold a credit ledger for the partition it read, or nothing below is " +
        "asserted at all")
    ledgerKey.foreach { key =>
      assert(fixture.backpressure.isStreamTerminated(key),
        "A stream completed through a held end-of-stream must be recorded as terminated in the " +
          "credit ledger, exactly as one completed immediately is")
      assert(fixture.backpressure.announcedBlockCount(key).contains(blocks.size.toLong),
        s"and the ledger must carry the total the terminator announced, ${blocks.size}, but " +
          s"carries ${fixture.backpressure.announcedBlockCount(key)}")
    }
  }

  test("a producer failure whose hand-off marker never arrived still escalates to a fetch " +
      "failure") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream

    val injected = StreamingShuffleErrors.invalidSequenceNumber(
      shuffleId = fixture.shuffleId, partitionId = fixture.partitionId, expected = 0L, actual = 1L)
    injectFromIoThread(stream, injected)
    var drained = stream.handler.poll()
    while (drained.isDefined) {
      drained = stream.handler.poll()
    }
    blocks.foreach(block => stream.deliver(block))

    val failure = intercept[FetchFailedException] {
      withTaskContext(fixture.context) {
        records.hasNext
      }
    }
    assert(fetchFailedReasonOf(failure).shuffleId == fixture.shuffleId,
      "A latched producer failure must recover through stage recomputation, which means a fetch " +
        s"failure naming the shuffle being read, but it named " +
        s"${fetchFailedReasonOf(failure).shuffleId}")
    assert(fetchFailedReasonOf(failure).reduceId == fixture.partitionId,
      s"and it must name the partition being read, but it named " +
        s"${fetchFailedReasonOf(failure).reduceId}")
    assert(diagnosisOf(failure).contains(injected),
      s"The escalated failure must carry the cause the I/O thread observed, but it carried " +
        s"${diagnosisOf(failure).map(_.getMessage).mkString("[", ", ", "]")}")
    assert(failure.getMessage.contains("STREAMING_SHUFFLE_INVALID_SEQUENCE_NUMBER"),
      s"The fetch failure must state the typed condition, but it read: ${failure.getMessage}")
    assert(!failure.getMessage.contains("was lost: SparkException"),
      s"and it must not report the cause by class name, but it read: ${failure.getMessage}")
    assert(fixture.context.fetchFailed.isDefined,
      "The fetch failure must be asserted on the task context, because that is what the executor " +
        "consults when it chooses between FetchFailed and ExceptionFailure")
    assert(observedPartialReadInvalidations() == 1L,
      s"Escalating must invalidate the partial read exactly once, but " +
        s"${observedPartialReadInvalidations()} was recorded")
    assert(fixture.coordinatorRef.invalidationsSent.size == 1,
      s"and it must invalidate exactly one producer generation, but " +
        s"${fixture.coordinatorRef.invalidationsSent.size} were invalidated")
  }

  test("a transport failure inside the connect window fails the fetch instead of the task") {
    startContext()
    // The failure classification the scheduler acts upon must not depend on when a channel broke.
    // A handler is bound to its channel before the connector decides whether to publish it, so a
    // connection reset observed inside that window is latched on this reader's notifier by a
    // handler the reader never receives -- and with no producer stream to attribute it to, the read
    // used to
    // raise it as a bare SparkException. That is an ordinary task failure, so the scheduler counted
    // it against `spark.task.maxFailures` instead of recomputing the stage that produced the bytes
    // this consumer cannot read, and under a master whose task budget is one attempt -- `local[N]`,
    // through `SparkContext.MAX_LOCAL_TASK_FAILURES` -- the job aborted outright.
    val injected = new java.io.IOException(
      "send(..) failed with error(-104): Connection reset by peer")
    val fixture = new ReaderFixture(
      refusalsBeforeSuccess = 1,
      realTimeReaderClock = true,
      failBoundChannelBeforeRefusal = Some(injected))

    val failure = intercept[FetchFailedException] {
      withTaskContext(fixture.context) {
        fixture.reader.read()
      }
    }
    assert(fixture.connector.reportedOnRefusalCount == 1,
      s"The case proves nothing unless the refused attempt really did report the failure on a " +
        s"bound channel, but it reported on ${fixture.connector.reportedOnRefusalCount}")
    assert(fixture.connector.streams.isEmpty,
      "and unless no producer stream was ever published, which is the whole of what makes the " +
        "failure unattributable to one")

    val reason = fetchFailedReasonOf(failure)
    assert(reason.shuffleId == fixture.shuffleId,
      s"A transport failure on a producer channel must recover through stage recomputation, so " +
        s"it must raise a fetch failure naming the shuffle being read, but it named " +
        s"${reason.shuffleId}")
    assert(reason.reduceId == fixture.partitionId,
      s"and it must name the partition being read, but it named ${reason.reduceId}")
    // The producer being opened is the honest attribution, and it is also the one that recovers:
    // `MapOutputTracker` removes a map output only when the address in the failure equals the
    // address in the recorded status, so a fetch failure naming no producer recomputes nothing.
    assert(reason.bmAddress == fixture.producer.blockManagerId,
      s"and it must name the producer's own MapStatus address so the tracker removes that one " +
        s"dead map output, but it named ${reason.bmAddress}")
    assert(reason.mapId == fixture.producer.mapId && reason.mapIndex == fixture.producer.mapIndex,
      s"and it must name that producer's map identity, but it named map ${reason.mapId} at index " +
        s"${reason.mapIndex}")
    assert(diagnosisOf(failure).contains(injected),
      s"The failure the transport reported must survive into the fetch failure, but the attached " +
        s"failures were ${diagnosisOf(failure).map(_.getMessage).mkString("[", ", ", "]")}")
    assert(failure.getMessage.contains("Connection reset by peer"),
      s"and the reason an operator reads must state what went wrong, but it read: " +
        s"${failure.getMessage}")
    assert(fixture.context.fetchFailed.isDefined,
      "The fetch failure must be asserted on the task context, because that is what the executor " +
        "consults when it chooses between FetchFailed and ExceptionFailure")
    assert(observedPartialReadInvalidations() == 1L,
      s"The invalidation must be counted exactly once, but " +
        s"${observedPartialReadInvalidations()} was recorded")
    assert(fixture.coordinatorRef.invalidationsSent.size == 1,
      s"and exactly one producer generation must be withdrawn, so the recomputation lands on a " +
        s"fresh map attempt rather than on the same unreachable output, but " +
        s"${fixture.coordinatorRef.invalidationsSent.size} were withdrawn")
  }

  test("a latched failure with no producer left to attribute it to still fails the fetch") {
    startContext()
    // The backstop arm of the same funnel. A failure can outlive every stream this read opened --
    // the task-completion listener releases them all, and it runs on success, on failure and on
    // cancellation alike -- so the arm that names a producer is not always available. What must not
    // vary is the classification: a streaming read failure reaches the scheduler as a fetch failure
    // or the guarantee that every path terminates in a working shuffle is probabilistic.
    val fixture = new ReaderFixture()
    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    val injected = new java.io.IOException(
      "send(..) failed with error(-104): Connection reset by peer")
    injectFromIoThread(stream, injected)
    // Taken off the hand-off queue here rather than by the read, which is the state a queue that
    // refused the marker leaves behind: the failure is latched and nothing queued will mention it.
    var drained = stream.handler.poll()
    while (drained.isDefined) {
      drained = stream.handler.poll()
    }
    // Releasing the streams is what removes the last producer the funnel could have named.
    fixture.context.markTaskCompleted(None)

    val failure = intercept[FetchFailedException] {
      withTaskContext(fixture.context) {
        records.hasNext
      }
    }
    val reason = fetchFailedReasonOf(failure)
    assert(reason.shuffleId == fixture.shuffleId,
      s"An unattributable streaming read failure must still name the shuffle being read, but it " +
        s"named ${reason.shuffleId}")
    assert(reason.reduceId == fixture.partitionId,
      s"and the partition being read, but it named ${reason.reduceId}")
    // No producer is named, which is the shape `FetchFailedException` sanctions for a failure that
    // belongs to no one map output, and which the coordinator-unreachable path already uses.
    assert(reason.bmAddress == null && reason.mapId == -1L && reason.mapIndex == -1,
      s"and it must name no producer at all, but it named ${reason.bmAddress} map " +
        s"${reason.mapId} at index ${reason.mapIndex}")
    assert(diagnosisOf(failure).contains(injected),
      s"The transport failure must survive into it, but the attached failures were " +
        s"${diagnosisOf(failure).map(_.getMessage).mkString("[", ", ", "]")}")
    assert(fixture.context.fetchFailed.isDefined,
      "and the fetch failure must be asserted on the task context, or the executor reports an " +
        "ExceptionFailure however the exception is typed")
  }

  test("a fatal error observed on a producer channel is never downgraded to a fetch failure") {
    startContext()
    // Escalation is total for recoverable failures and must stop at fatal ones: a fetch failure
    // asks the scheduler to recompute and retry, which is the wrong response to a JVM-level
    // condition the executor has to act on. The notifier re-throws an Error ahead of everything,
    // and the funnel in front of it must not have converted it first.
    val fatal = new StackOverflowError("a fatal condition observed on a producer channel")
    val fixture = new ReaderFixture(
      refusalsBeforeSuccess = 1,
      realTimeReaderClock = true,
      failBoundChannelBeforeRefusal = Some(fatal))

    val raised = intercept[StackOverflowError] {
      withTaskContext(fixture.context) {
        fixture.reader.read()
      }
    }
    assert(raised eq fatal,
      "The fatal condition must be re-thrown exactly as it stands, not wrapped and not replaced")
    assert(fixture.context.fetchFailed.isEmpty,
      "No fetch failure may be asserted on the task context, because nothing about a fatal " +
        "condition says an upstream stage should be recomputed")
    assert(observedPartialReadInvalidations() == 0L,
      s"and nothing may be invalidated, but ${observedPartialReadInvalidations()} " +
        s"invalidation(s) were recorded")
    assert(fixture.coordinatorRef.invalidationsSent.isEmpty,
      s"and no producer generation may be withdrawn, but " +
        s"${fixture.coordinatorRef.invalidationsSent.size} were withdrawn")
  }


  test("a wrapped failure reports the cause's message rather than its class name") {
    val notifier = new StreamingShuffleErrorNotifier(shuffleId = 11, debugEnabled = false)
    val typed = StreamingShuffleErrors.invalidSequenceNumber(
      shuffleId = 11, partitionId = 2, expected = 4L, actual = 6L)
    notifier.setError(typed)
    val raised = intercept[SparkException](notifier.throwIfError())
    assert(raised.getCause eq typed,
      "The cause must be attached so the full diagnosis survives")
    assert(raised.getMessage.contains("STREAMING_SHUFFLE_INVALID_SEQUENCE_NUMBER"),
      s"The wrapper must carry the cause's message, but it read: ${raised.getMessage}")
    assert(!raised.getMessage.contains("(org.apache.spark.SparkException)"),
      s"and it must not name the cause's class in its place, but it read: ${raised.getMessage}")

    val silent = new StreamingShuffleErrorNotifier(shuffleId = 12, debugEnabled = false)
    val messageless = new java.io.IOException()
    silent.setError(messageless)
    val fallback = intercept[SparkException](silent.throwIfError())
    assert(fallback.getMessage.contains(classOf[java.io.IOException].getName),
      s"A cause with no message at all must still be named, but the report read: " +
        s"${fallback.getMessage}")
  }

  test("the error notifier keeps the first failure and suppresses the rest") {
    val notifier = new StreamingShuffleErrorNotifier(shuffleId = 0, debugEnabled = false)
    val first = new IllegalStateException("the first failure any thread observed")
    val second = new SparkException("a later failure that only obscures the first")

    notifier.setError(first)
    notifier.setError(second)
    assert(notifier.hasError, "A notifier that was given a failure must report that it has one")
    assert(notifier.error.contains(first),
      s"First-error-wins is what makes the reported failure the one that explains the cascade, " +
        s"but the notifier held ${notifier.error.map(_.getMessage).getOrElse("nothing")}")
    assert(notifier.suppressedFailureCount >= 1,
      s"A later failure must be retained as a suppressed exception rather than discarded, but " +
        s"${notifier.suppressedFailureCount} were retained")
    assert(first.getSuppressed.toSeq.contains(second),
      "The later failure must be attached to the one that propagates, so no diagnosis is lost")

    notifier.setError(first)
    assert(notifier.error.contains(first),
      "Recording the held failure again must be harmless and must not displace it")

    val rethrown = intercept[IllegalStateException] {
      notifier.throwIfError()
    }
    assert(rethrown eq first,
      "throwIfError must re-throw the very instance it latched, not a copy or a wrapper")

    notifier.reset()
    assert(!notifier.hasError && notifier.error.isEmpty,
      "A reset notifier must hold no failure, which is what lets one fixture serve two assertions")

    val fatal = new StackOverflowError("a fatal condition observed on an io thread")
    notifier.setError(fatal)
    notifier.setError(new SparkException("a recoverable failure that must not mask the fatal one"))
    val rethrownFatal = intercept[StackOverflowError] {
      notifier.throwIfError()
    }
    assert(rethrownFatal eq fatal,
      "An Error must be re-thrown exactly as it stands, because a fatal condition must not be " +
        "masked by a recoverable one")
  }

  test("channel autoRead belongs to the client handler and never to the reader") {
    startContext()
    val fixture = new ReaderFixture()

    val readerSurface = classOf[StreamingShuffleReader[_, _]].getDeclaredMethods.map(_.getName) ++
      classOf[StreamingShuffleReader[_, _]].getDeclaredFields.map(_.getName)
    val autoReadMentions =
      readerSurface.filter(name => name.toLowerCase(Locale.ROOT).contains("autoread"))
    assert(autoReadMentions.isEmpty,
      s"The reader must never manipulate a channel's autoRead, but its own surface mentions " +
        s"${autoReadMentions.mkString("[", ", ", "]")}")
    assert(classOf[StreamingShuffleClientHandler].getDeclaredMethods.map(_.getName)
        .contains("isAutoReadEnabled"),
      "The client handler must be the component that owns the receive window, so that the reader " +
        "has no reason to reach for one")

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    assert(stream.handler.isAutoReadEnabled,
      "A freshly opened consumer channel must be reading from its socket")
    assert(stream.handler.throttleTransitionCount == 0L,
      s"No throttle transition may have happened before a single block arrived, but " +
        s"${stream.handler.throttleTransitionCount} did")

    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlocksPastHighWaterMark + 1)
    blocks.foreach(block => stream.deliver(block))
    assert(!stream.handler.isAutoReadEnabled,
      s"The handler must stop reading once its hand-off queue reaches the high-water mark of " +
        s"${BlocksPastHighWaterMark}, but it was still reading with " +
        s"${stream.handler.queuedEventCount} event(s) queued")
    assert(stream.handler.currentThrottleCause.isDefined,
      "A throttled channel must be able to say why it stopped reading")
    assert(stream.handler.throttleTransitionCount >= 1L,
      "Entering the throttled state must be counted so an operator can see the backpressure")

    stream.deliver(fixture.terminator(blocks.size.toLong))
    assert(readRecords(records) == expectedRecords,
      "Throttling must pace the producer rather than lose data")
    assert(stream.handler.isAutoReadEnabled,
      "Reading must resume once the reduce task's acknowledgements have drained the queue below " +
        "the low-water mark")
  }

  test("a wide coalesced reduce range is admitted rather than overrunning the hand-off queue") {
    startContext()
    // Adaptive execution routinely coalesces a wide shuffle into a reduce task that reads a range
    // of partitions, and one client handler carries every stream of that range. Each stream
    // contributes its own small block and its own terminator, so a burst on a wide range delivers
    // far more events than a one-stream allowance would admit -- and the socket cannot help,
    // because every frame in it is decoded before autoRead can take effect.
    val partitions = StreamingShuffleClientHandler.INBOUND_QUEUE_CAPACITY + 8
    val fixture = new ReaderFixture(
      numPartitions = ReducePartition + partitions,
      startPartition = ReducePartition,
      endPartition = ReducePartition + partitions)
    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    val partitionIds = ReducePartition until (ReducePartition + partitions)
    val expected = partitionIds.map(partitionId => (partitionId, partitionId * 7)).toSet

    // The whole burst arrives before the task consumes anything, which is the shape that overran
    // the queue: nothing has been polled, so nothing has drained.
    partitionIds.foreach { partitionId =>
      val payload = fixture.encodePartitionFor(partitionId, Seq((partitionId, partitionId * 7)))
      stream.deliver(dataBlock(fixture.shuffleId, ProducerMapId, partitionId, 0L, payload))
      stream.deliver(streamTermination(fixture.shuffleId, ProducerMapId, partitionId, 1L))
    }

    assert(stream.handler.queuePressureRefusalCount == 0L,
      s"a burst of one block per stream across $partitions stream(s) is inside the receive " +
        s"window this handler is entitled to, but ${stream.handler.queuePressureRefusalCount} " +
        s"block(s) were withheld while it held ${stream.handler.queuedEventCount} event(s)")
    assert(stream.handler.reportedEscalationCount == 0L,
      s"a healthy fan-out must escalate nothing, but " +
        s"${stream.handler.reportedEscalationCount} failure(s) were escalated to the task thread")
    assert(readRecords(records) == expected,
      "every stream of the coalesced range must deliver its records")
  }

  test("a stream past its data-event ceiling is withheld and replayed, never treated as lost") {
    val conf = streamingConf()
    val clock = newManualClock()
    val connector = new BarrieredStreamingShuffleProducerConnector(conf)
    val handler = newConnectorHandler(conf, clock)
    try {
      val client = connector.connect(connectorLocation(handler.mapId), handler).getOrElse(
        fail("the producer must open a channel"))
      val ceiling = StreamingShuffleClientHandler.INBOUND_QUEUE_CAPACITY
      val surplus = 4
      val payload = payloadOfLength(handler.mapId, 64)
      val blocks = (0 until (ceiling + surplus)).map { index =>
        dataBlock(handler.shuffleId, handler.mapId, ReducePartition, index.toLong, payload)
      }
      blocks.foreach(block => handler.receive(client, block.toByteBuffer()))
      assert(handler.awaitDataPlaneIdle(10000L),
        "the consumer data plane must settle before the receive window is read")

      assert(handler.queuedEventCount == ceiling,
        s"a single-stream handler may hold $ceiling data event(s), but it held " +
          s"${handler.queuedEventCount}")
    assert(handler.queuePressureRefusalCount > 0L,
        s"the block(s) past the ceiling must be withheld by the queue [withheld " +
          s"${handler.queuePressureRefusalCount}, quota refusals " +
          s"${handler.quotaRefusalCount(ReducePartition)}, accepted " +
          s"${handler.acceptedBlockCount(ReducePartition)}, duplicates " +
          s"${handler.duplicateBlockCount(ReducePartition)}, queued " +
          s"${handler.queuedEventCount}]")
      assert(handler.reportedEscalationCount == 0L,
        s"saturation is the consumer being behind rather than the producer being gone, so " +
          s"nothing may be escalated, but ${handler.reportedEscalationCount} failure(s) were")
      assert(!handler.isAutoReadEnabled,
        "a handler at its ceiling must have stopped reading from its socket")
      assert(handler.currentThrottleCause.contains(
          StreamingShuffleClientHandler.THROTTLE_CAUSE_QUEUE),
        s"the throttle must name the queue as its cause, but it named " +
          s"${handler.currentThrottleCause}")
      assert(handler.quarantinedPositionCount(ReducePartition) > 0,
        "a withheld position must be quarantined, or nothing would ever ask for it again")
      assert(handler.replayRequestCount(ReducePartition) > 0L,
        "a withheld position must be claimed as a repair, because the block withheld may have " +
          "been the last of its stream and no later block would reveal the gap")

      // The task drains what it holds, which is what makes room and reopens the socket.
      (0 until ceiling).foreach { index =>
        assert(handler.poll().isDefined, s"event $index must be available to the task thread")
      }
      assert(handler.isAutoReadEnabled,
        "reading must resume once the task has drained the queue below the low-water mark")
      val requestedBefore = handler.replayRequestCount(ReducePartition)
      clock.advance(2L * BackpressureProtocol.retryBackoffMillis(1))
      assert(handler.retryDueReplays() > 0,
        "the repair must be asked for again once its backoff has elapsed")
      assert(handler.replayRequestCount(ReducePartition) > requestedBefore,
        "every replay attempt must be counted so an operator can see the repair")

      // The producer replays from its retained window, exactly as it does for a corrupt block.
      blocks.drop(ceiling).foreach(block => handler.receive(client, block.toByteBuffer()))
      assert(handler.awaitDataPlaneIdle(10000L), "the replayed blocks must settle")
      assert(handler.acceptedBlockCount(ReducePartition) == (ceiling + surplus).toLong,
        s"every block must end up accepted, but only " +
          s"${handler.acceptedBlockCount(ReducePartition)} of ${ceiling + surplus} were")
      assert(handler.reportedEscalationCount == 0L,
        s"the repair must complete without a failure, but " +
          s"${handler.reportedEscalationCount} were escalated")
    } finally {
      handler.close()
      connector.close()
    }
  }

  test("a callback from a channel this consumer is not bound to is refused before any decode") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    stream.deliver(blocks.head)
    assert(stream.handler.acceptedBlockCount(fixture.partitionId) == 1L,
      "The bound channel must be serving normally before a foreign one is tried, or the refusal " +
        "below proves nothing about which channel was refused")
    val acceptedBefore = stream.handler.acceptedBlockCount(fixture.partitionId)
    val queuedBefore = stream.handler.queuedEventCount
    val quotaBefore = fixture.backpressure.reservedReceiveQuotaBytes

    val foreignChannel = new EmbeddedChannel(DefaultChannelId.newInstance())
    val foreignClient =
      new TransportClient(foreignChannel, new TransportResponseHandler(foreignChannel))
    foreignClient.setClientId("streaming-shuffle-reader-suite")
    assert(foreignChannel.id().asLongText() != stream.channel.id().asLongText(),
      "The foreign channel must have an identity of its own, or the handler could not tell it " +
        "apart from the one it is bound to")
    try {
      stream.handler.receive(foreignClient, blocks(1).toByteBuffer())
      stream.awaitDataPlane("foreign-channel block callback")
      assert(stream.handler.foreignChannelCallbackCount == 1L,
        s"A frame from an unbound channel must be counted as a refused callback, but " +
          s"${stream.handler.foreignChannelCallbackCount} were")
      assert(stream.handler.misaddressedFrameCount == 0L,
        s"The refusal must happen before the frame is decoded, so the addressing checks that run " +
          s"on a decoded message must never have been reached, but " +
          s"${stream.handler.misaddressedFrameCount} misaddressed frame(s) were counted")
      assert(stream.handler.acceptedBlockCount(fixture.partitionId) == acceptedBefore,
        s"A refused frame must admit nothing, but the accepted count moved from " +
          s"${acceptedBefore} to ${stream.handler.acceptedBlockCount(fixture.partitionId)}")
      assert(stream.handler.queuedEventCount == queuedBefore,
        s"A refused frame must reach no hand-off queue, but the queue moved from ${queuedBefore} " +
          s"to ${stream.handler.queuedEventCount}")
      assert(fixture.backpressure.reservedReceiveQuotaBytes == quotaBefore,
        s"A refused frame must charge no receive quota, but the charge moved from ${quotaBefore} " +
          s"to ${fixture.backpressure.reservedReceiveQuotaBytes} byte(s)")

      stream.handler.receive(foreignClient, ByteBuffer.wrap(Array[Byte](-1, -2, -3, -4)))
      stream.awaitDataPlane("foreign-channel malformed callback")
      assert(stream.handler.foreignChannelCallbackCount == 2L,
        s"Every callback from an unbound channel must be counted, but only " +
          s"${stream.handler.foreignChannelCallbackCount} were")

      stream.handler.channelActive(foreignClient)
      stream.awaitDataPlane("foreign-channel activation callback")
      assert(stream.handler.foreignChannelCallbackCount == 3L,
        "An activation callback from an unbound channel must be refused and counted")
      assert(foreignChannel.readOutbound[AnyRef]() == null,
        "Nothing may be written to a channel this handler is not bound to, because the frame it " +
          "would write is the subscription that names this consumer")

      // A failure raised on a stranger's channel belongs to whoever owns that channel.
      stream.handler.exceptionCaught(
        new SparkException("a transport failure on somebody else's channel"), foreignClient)
      stream.awaitDataPlane("foreign-channel exception callback")
      assert(stream.handler.foreignChannelCallbackCount == 4L,
        "An exception callback from an unbound channel must be refused and counted")
      assert(!foreignClient.isActive(),
        "A channel nobody owns must still be closed rather than left open")

      assert(!stream.handler.isProducerLost,
        "Refusing a stranger must not mark this consumer's own producer lost")
      assert(!stream.handler.isClosed && stream.client.isActive(),
        "The bound channel must survive every refusal, since none of them concerned it")
      blocks.tail.foreach(block => stream.deliver(block))
      stream.deliver(fixture.terminator(blocks.size.toLong))
      assert(readRecords(records) == expectedRecords,
        "A read whose handler refused four foreign callbacks must still yield every record its " +
          "own producer sent")
    } finally {
      foreignChannel.close().syncUninterruptibly()
    }
  }

  test("an unauthenticated producer frame is refused before payload deserialization") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val block = fixture.dataBlocksOf(payload, BlockCount).head
    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    val handler = stream.handler
    val quotaBefore = fixture.backpressure.reservedReceiveQuotaBytes
    val queuedBytesBefore = handler.queuedByteCount

    val unauthenticatedChannel = new EmbeddedChannel(DefaultChannelId.newInstance())
    val unauthenticatedClient = new TransportClient(
      unauthenticatedChannel, new TransportResponseHandler(unauthenticatedChannel))
    try {
      handler.receive(unauthenticatedClient, block.toByteBuffer())
      assert(handler.awaitDataPlaneIdle(10000L),
        "The consumer data plane must settle after an unauthenticated frame")

      assert(handler.unauthenticatedCallbackCount === 1L,
        "The consumer must count the callback it refused for lacking a transport principal")
      assert(!unauthenticatedClient.isActive(),
        "The consumer must close a channel that attempts to deliver unauthenticated payload bytes")
      assert(handler.acceptedBlockCount(fixture.partitionId) === 0L,
        "An unauthenticated block must never be admitted to the reader's payload queue")
      assert(handler.queuedByteCount === queuedBytesBefore,
        "Refusal before decode may enqueue only the failure marker, never payload bytes")
      assert(fixture.backpressure.reservedReceiveQuotaBytes === quotaBefore,
        "Refusal before decode must charge no shared receive quota")
      val failure = intercept[FetchFailedException](records.hasNext)
      val detail = Seq(
        Option(failure.getMessage),
        Option(failure.getCause).map(_.getMessage)).flatten.mkString(" ")
      assert(detail.contains("requires an authenticated transport channel"),
        s"The fetch failure must retain the authentication refusal, but reported $detail")
    } finally {
      unauthenticatedChannel.finishAndReleaseAll()
    }
  }

  test("a misaddressed frame is dropped and a wrongly directed one is refused by type") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)
    val foreignPartition = fixture.partitionId + 1
    val foreignMapId = ProducerMapId + 7
    val foreignShuffleId = fixture.shuffleId + 100

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    val trackedBefore = stream.handler.trackedPartitionCount

    stream.deliver(dataBlock(fixture.shuffleId, ProducerMapId, foreignPartition, 0L, payload))
    stream.deliver(dataBlock(fixture.shuffleId, foreignMapId, fixture.partitionId, 0L, payload))
    stream.deliver(dataBlock(foreignShuffleId, ProducerMapId, fixture.partitionId, 0L, payload))
    assert(stream.handler.misaddressedFrameCount == 3L,
      s"Every misaddressed frame must be counted, but ${stream.handler.misaddressedFrameCount} " +
        s"of 3 were")
    assert(stream.handler.trackedPartitionCount == trackedBefore,
      s"A misaddressed frame must be dropped before any per-partition state is allocated, which " +
        s"is what bounds that state by this task's own request, but the handler tracked " +
        s"${stream.handler.trackedPartitionCount} partition(s) rather than ${trackedBefore}")
    assert(stream.handler.acceptedBlockCount(foreignPartition) == 0L &&
        stream.handler.acceptedBlockCount(fixture.partitionId) == 0L,
      "A misaddressed frame must admit nothing, for the partition it named or for any other")
    assert(stream.handler.queuedEventCount == 0,
      s"No misaddressed frame may reach the hand-off queue, but " +
        s"${stream.handler.queuedEventCount} event(s) did")
    assert(fixture.backpressure.reservedReceiveQuotaBytes == 0L,
      s"A misaddressed frame must charge no receive quota, but " +
        s"${fixture.backpressure.reservedReceiveQuotaBytes} byte(s) were charged")

    blocks.foreach(block => stream.deliver(block))
    stream.deliver(fixture.terminator(blocks.size.toLong))
    assert(readRecords(records) == expectedRecords,
      "A read that dropped three misaddressed frames must still yield every record its own " +
        "producer sent, and must not have been failed by them")

    val directed = new ReaderFixture()
    val directedRecords = directed.reader.read()
    val directedStream = directed.connector.onlyStream
    directedStream.deliver(ack(directed.shuffleId, ProducerMapId, directed.partitionId, 0L))
    val refused = intercept[FetchFailedException] {
      withTaskContext(directed.context) {
        readRecords(directedRecords)
      }
    }
    val attached: Seq[Throwable] =
      Option(refused.getCause).toSeq ++ refused.getSuppressed.toSeq
    val typed = attached.collectFirst {
      case throwable: SparkThrowable
        if throwable.getCondition == UnexpectedMessageTypeCondition => throwable
    }
    assert(typed.isDefined,
      s"An inbound message of an outbound-only type must be refused through the " +
        s"${UnexpectedMessageTypeCondition} condition and that condition must survive into the " +
        s"fetch failure, but the attached failures were " +
        s"${attached.map(_.toString).mkString("[", ", ", "]")}")
    assert(typed.get.getSqlState == StreamingShuffleSqlState,
      s"Every streaming shuffle condition shares SQLSTATE ${StreamingShuffleSqlState}, but this " +
        s"one carried ${typed.get.getSqlState}")
    assert(typed.get.asInstanceOf[Throwable].getMessage
        .contains(StreamingShuffleMessageType.ACK.name()),
      s"The refusal must name the type that arrived, but it read: " +
        s"${typed.get.asInstanceOf[Throwable].getMessage}")
    assert(directedStream.handler.acceptedBlockCount(directed.partitionId) == 0L,
      "An outbound-only message must never be applied to a ledger it does not describe")
  }

  test("the capability token never reaches a producer channel or a failure message") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)
    val tokenBytes = CapabilityToken.getBytes(StandardCharsets.UTF_8)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(block => stream.deliver(block))
    stream.deliver(fixture.terminator(blocks.size.toLong))
    assert(readRecords(records) == expectedRecords, "The read must succeed before it is audited")

    val written = stream.encodedOutboundFrames
    assert(written.nonEmpty,
      "The consumer must have written frames -- its subscription and its acknowledgements -- or " +
        "there is nothing to audit")
    val leaking = written.count(frame => containsBytes(frame, tokenBytes))
    assert(leaking == 0,
      s"No frame written to a producer may carry the shuffle's capability token, but ${leaking} " +
        s"of ${written.size} did")

    assert(!fixture.handle.toString.contains(CapabilityToken),
      s"The shuffle handle must redact its capability token, but it rendered as " +
        s"${fixture.handle.toString}")

    val failing = new ReaderFixture()
    val failingRecords = failing.reader.read()
    val failingStream = failing.connector.onlyStream
    failingStream.deliver(failing.dataBlocksOf(failing.encodePartition(StreamedRecords)).head)
    advancePastProducerTimeout(failing.clock)
    val failure = intercept[FetchFailedException] {
      withTaskContext(failing.context) {
        readRecords(failingRecords)
      }
    }
    assert(!failure.getMessage.contains(CapabilityToken),
      s"A fetch failure must not quote the capability token, but it read: ${failure.getMessage}")
    assert(!fetchFailedReasonOf(failure).toString.contains(CapabilityToken),
      "The task-failure reason the executor sends to the driver must not carry the token either")
  }

  test("only the seven argument getReader may be overridden") {
    val declared = classOf[ShuffleManager].getMethods.filter(_.getName == "getReader")
    val fiveArgument = declared.filter(_.getParameterCount == 5)
    val sevenArgument = declared.filter(_.getParameterCount == 7)
    assert(fiveArgument.size == 1 && sevenArgument.size == 1,
      s"The service-provider interface must offer exactly one five-argument and one " +
        s"seven-argument getReader, but it offered ${fiveArgument.size} and ${sevenArgument.size}")
    assert(!Modifier.isAbstract(fiveArgument.head.getModifiers),
      "The five-argument getReader already has an implementation -- it is final in Scala and " +
        "merely narrows the map range before delegating -- so it must never be the form under test")
    assert(Modifier.isAbstract(sevenArgument.head.getModifiers),
      "The seven-argument getReader is the abstract form, and therefore the only one a manager " +
        "may implement")

    val managerForms = classOf[StreamingShuffleManager].getDeclaredMethods
      .filter(_.getName == "getReader")
    val managerSeven = managerForms.filter(_.getParameterCount == 7)
    val managerFive = managerForms.filter(_.getParameterCount == 5)
    val overriddenFive = managerFive.filterNot(form => Modifier.isFinal(form.getModifiers))
    assert(managerSeven.size == 1,
      s"The streaming manager must implement exactly one seven-argument getReader, but it " +
        s"declared ${managerSeven.size}")
    assert(!Modifier.isAbstract(managerSeven.head.getModifiers),
      "The seven-argument getReader must be implemented by the manager, not left abstract")
    assert(overriddenFive.isEmpty,
      s"Every five-argument shape the manager carries must be the trait's own final forwarder, " +
        s"which is what makes the overload impossible to override, but " +
        s"${overriddenFive.length} was not final")
  }

  test("the streaming shuffle error conditions carry exactly their documented parameters") {
    val intactPayload = Array[Byte](7, 11, 13, 17, 19)
    val damagedPayload = intactPayload.clone()
    damagedPayload(2) = (damagedPayload(2) ^ 0xFF).toByte
    val announcedChecksum = StreamingShuffleChecksum.compute(intactPayload)
    val observedChecksum = StreamingShuffleChecksum.compute(damagedPayload)
    assert(announcedChecksum != observedChecksum,
      s"A flipped byte must change the CRC32C, otherwise the condition could never distinguish " +
        s"the announced value from the observed one, but both were $announcedChecksum")
    assert(StreamingShuffleChecksum.verify(intactPayload, announcedChecksum),
      "The intact payload must verify against the checksum computed over it")
    assert(!StreamingShuffleChecksum.verify(damagedPayload, announcedChecksum),
      "The damaged payload must fail verification against the announced checksum")

    val checksumFailure = StreamingShuffleErrors.checksumVerificationFailed(
      blockId = "shuffle_1_0_3", shuffleId = 1,
      expected = announcedChecksum, computed = observedChecksum)
    val checksum = checksumFailure.asInstanceOf[SparkThrowable]
    assert(checksum.getCondition == ChecksumVerifyFailedCondition,
      s"The checksum failure must be raised as its own condition, but it was raised as " +
        s"${checksum.getCondition}")
    assert(checksum.getMessageParameters.asScala.keySet ==
        Set("blockId", "shuffleId", "expected", "computed"),
      s"The checksum condition takes exactly four parameters, but it was given " +
        s"${checksum.getMessageParameters.asScala.keySet.toSeq.sorted.mkString("[", ", ", "]")}")
    val checksumMessage = checksumFailure.getMessage
    assert(checksumMessage.contains(announcedChecksum.toString) &&
        checksumMessage.contains(observedChecksum.toString),
      s"Both the value the producer sent and the value this consumer computed must appear in the " +
        s"message, but it read: $checksumMessage")

    val sequence = StreamingShuffleErrors.invalidSequenceNumber(
      shuffleId = 1, partitionId = 3, expected = 4L, actual = 6L).asInstanceOf[SparkThrowable]
    assert(sequence.getCondition == "STREAMING_SHUFFLE_INVALID_SEQUENCE_NUMBER",
      s"The sequence violation must be raised as its own condition, but it was raised as " +
        s"${sequence.getCondition}")
    assert(sequence.getMessageParameters.asScala.keySet ==
        Set("shuffleId", "partitionId", "expected", "actual"),
      s"The sequence condition takes exactly four parameters, but it was given " +
        s"${sequence.getMessageParameters.asScala.keySet.toSeq.sorted.mkString("[", ", ", "]")}")

    val unexpected = StreamingShuffleErrors.unexpectedMessageType(
      expected = "DATA_BLOCK", actual = "ACK").asInstanceOf[SparkThrowable]
    assert(unexpected.getCondition == "STREAMING_SHUFFLE_UNEXPECTED_MESSAGE_TYPE",
      s"The message-type violation must be raised as its own condition, but it was raised as " +
        s"${unexpected.getCondition}")
    assert(unexpected.getMessageParameters.asScala.keySet == Set("expected", "actual"),
      s"The message-type condition takes exactly two parameters, but it was given " +
        s"${unexpected.getMessageParameters.asScala.keySet.toSeq.sorted.mkString("[", ", ", "]")}")

    Seq(checksum, sequence, unexpected).foreach { condition =>
      assert(condition.getSqlState == StreamingShuffleSqlState,
        s"Every streaming shuffle condition shares the SQLSTATE the existing checksum " +
          s"precedent uses, but ${condition.getCondition} reported ${condition.getSqlState}")
    }
  }

  test("the producer connector routes only channels it owns and counts the frames it cannot") {
    // The demultiplexer beneath every streaming read.
    val connectorConf = streamingConf()
    val connectorClock = newManualClock()
    val connector = new NettyStreamingShuffleProducerConnector(connectorConf, connectorClock)
    val connectorShuffleId = 17
    try {
      val strangerChannel = new EmbeddedChannel(DefaultChannelId.newInstance())
      val stranger = new TransportClient(strangerChannel, new TransportResponseHandler(
        strangerChannel))
      stranger.setClientId("streaming-shuffle-reader-suite")
      assert(connector.unboundFrameCount === 0L,
        "a connector that has been handed nothing must report no unbound frame")

      val block = dataBlock(connectorShuffleId, ProducerMapId, ReducePartition,
        sequenceNumber = 0L, payload = payloadOfLength(0L, 64))
      connector.receive(stranger, block.toByteBuffer())
      assert(connector.unboundFrameCount === 1L,
        "a data frame on a channel no handler owns must be counted rather than raised")
      connector.receive(stranger, block.toByteBuffer())
      assert(connector.unboundFrameCount === 2L,
        "every unroutable frame must be counted, not just the first one that was logged")

      val answered = new AtomicInteger(0)
      val callback = new RpcResponseCallback {
        override def onSuccess(response: ByteBuffer): Unit = answered.incrementAndGet()
        override def onFailure(cause: Throwable): Unit = answered.incrementAndGet()
      }
      connector.receive(stranger, block.toByteBuffer(), callback)
      assert(connector.unboundFrameCount === 3L,
        "an unroutable frame that came with a callback must be counted like any other")
      assert(answered.get() === 0,
        "a frame on an unowned channel must not be answered: neither success nor failure may be " +
          "reported to a peer this consumer never opened a channel to")

      val countAfterFrames = connector.unboundFrameCount
      connector.channelActive(stranger)
      connector.exceptionCaught(new java.io.IOException("a fault on a channel nobody owns"),
        stranger)
      connector.channelInactive(stranger)
      assert(connector.unboundFrameCount === countAfterFrames,
        s"a lifecycle callback on an unowned channel must not be counted as a lost frame, but " +
          s"count moved from $countAfterFrames to ${connector.unboundFrameCount}")
      assert(strangerChannel.isOpen,
        "an unroutable callback must not close a channel this connector does not own; that " +
          "channel's owner decides its lifetime")

      // A connection that cannot be established is a refusal the caller can act on, not a throw.
      val unreachable = StreamingShuffleProducerLocation(
        executorId = "unreachable-executor",
        host = "streaming-shuffle-connector-suite.invalid",
        port = 1,
        mapId = ProducerMapId,
        mapIndex = 0,
        taskAttemptId = ProducerAttemptId,
        blockManagerId = BlockManagerId("unreachable-executor", "unreachable-host", 1))
      val handler = new StreamingShuffleClientHandler(
        connectorConf,
        connectorShuffleId,
        ProducerMapId,
        ConsumerAttemptId,
        s"consumer-$ConsumerAttemptId",
        ReducePartition,
        ReducePartition + 1,
        new BackpressureProtocol(connectorConf, null,
          TokenBucketRateLimiter.executorBudget(connectorConf, connectorClock), connectorClock),
        new StreamingShuffleErrorNotifier(connectorShuffleId, connectorConf),
        connectorClock)
      assert(connector.connect(unreachable, handler).isEmpty,
        "a connection that cannot be made must be reported as an absent client rather than by " +
          "raising, because the reader's own retry ladder is what decides whether to try again")
      assert(connector.unboundFrameCount === countAfterFrames,
        "a failed connection is not a lost frame")

      connector.close()
      assert(connector.unreleasedChannels === 0,
        s"a connector that opened no channel can leave none behind, but it reported " +
          s"${connector.unreleasedChannels}")
      assert(connector.connect(unreachable, handler).isEmpty,
        "a closed connector must refuse to open a channel rather than opening one nothing will " +
          "ever release")
    } finally {
      connector.close()
    }
  }

  test("the repair budget is five attempts paced from a one second base") {
    val ladder = (1 to MaxRetryAttempts).map(attempt =>
      BackpressureProtocol.retryBackoffMillis(attempt))
    assert(MaxRetryAttempts == 5,
      s"Retransmission is bounded at five attempts, but the bound was ${MaxRetryAttempts}")
    assert(ladder == Seq(1000L, 2000L, 4000L, 8000L, 16000L),
      s"The backoff must double from one second across the five attempts, but it was " +
        s"${ladder.mkString("[", ", ", "]")}")
    assert(RetryBackoffLadderMillis == ladder,
      "The shared fixture's ladder and the protocol's own schedule must be the same numbers")
    assert(ProducerConnectionTimeoutMillis == 5000L,
      s"The producer connection timeout is five seconds, but it was " +
        s"${ProducerConnectionTimeoutMillis} ms")
    assert(ConsumerLivenessTimeoutMillis == 10000L,
      s"The consumer liveness window is ten seconds, but it was " +
        s"${ConsumerLivenessTimeoutMillis} ms")
    assert(JustBeforeProducerTimeoutMillis == 4999L &&
        JustBeforeConsumerLivenessMillis == 9999L,
      "Each bound has a counterpart one millisecond short of it, because a timer firing and " +
        "a timer not firing early are two different assertions")
    assert(StreamingShuffleClientHandler.PRODUCER_CONNECTION_TIMEOUT_MS ==
        ProducerConnectionTimeoutMillis,
      "The reader's liveness detector and the flow-control protocol must measure the connection " +
        "timeout in the same number, or one of them will be stale")
  }

  test("a streaming reader is constructed for any well formed map range with streaming enabled") {
    startContext()
    val fixture = new ReaderFixture()

    val gatedOff = intercept[IllegalArgumentException] {
      fixture.newReader(gatedOffStreamingConf(), FullRangeStart, FullRangeEnd)
    }
    assert(gatedOff.getMessage.contains(SHUFFLE_STREAMING_ENABLED.key),
      s"The refusal must name the gate that is closed, but it read: ${gatedOff.getMessage}")

    val ranged = new ReaderFixture(numMaps = FourProducers, completedMaps = Set(0, 1, 2, 3))
    val everyProducer = ranged.locationsReply((0 until FourProducers).map(ranged.producerLocation))
    Seq((FullRangeStart, FullRangeEnd, Seq(0, 1, 2, 3)), (FullRangeStart + 1, FullRangeStart + 3,
      Seq(1, 2)), (FullRangeStart + 3, FullRangeStart + 4, Seq(3)),
      (FourProducers, FourProducers + 5, Seq.empty[Int])).foreach { case (start, end, expected) =>
      val narrowed = ranged.newReader(ranged.conf, start, end)
      assert(narrowed.servedMapIndexesOf(everyProducer) === expected,
        s"The map range [$start, $end) must select map indexes ${expected.mkString(", ")} but " +
          s"selected ${narrowed.servedMapIndexesOf(everyProducer).mkString(", ")}")
      assert(StreamingShuffleReader.servesFullMapRange(start, end) === (expected.size ==
          FourProducers),
        s"[$start, $end) must be recognised as the whole map range only when it selects every " +
          s"declared map index, or the two diagnoses of an unresolved rendezvous are the wrong " +
          s"way round")
    }

    val inverted = intercept[IllegalArgumentException] {
      fixture.newReader(fixture.conf, FullRangeStart + 3, FullRangeStart + 1)
    }
    assert(inverted.getMessage.contains("Invalid map index range"),
      s"The refusal must name the range as invalid, but it read: ${inverted.getMessage}")
    val negative = intercept[IllegalArgumentException] {
      fixture.newReader(fixture.conf, -1, FullRangeEnd)
    }
    assert(negative.getMessage.contains("Invalid map index range"),
      s"A negative first map index must be refused the same way, but it read: " +
        s"${negative.getMessage}")
  }

  // -----------------------------------------------------------------------------------------------
  // Hostile frame ordering around the end of stream.

  test("a premature end of stream fails the fetch rather than truncating the partition") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    stream.deliver(blocks.head)
    stream.deliver(blocks(1))
    assert(stream.handler.expectedSequenceNumber(fixture.partitionId) == 2L,
      "The consumer must have reached exactly the two positions it was given")
    stream.deliver(fixture.terminator(blocks.size.toLong))

    assert(stream.handler.rejectedTerminationCount(fixture.partitionId) == 1L,
      s"A premature end of stream must be refused, but " +
        s"${stream.handler.rejectedTerminationCount(fixture.partitionId)} refusal(s) were counted")
    assert(!stream.handler.isStreamComplete(fixture.partitionId),
      "A refused terminator must not leave the stream marked complete")
    assert(stream.handler.announcedBlockCount(fixture.partitionId).isEmpty,
      s"A refused terminator must publish no block total, but published " +
        s"${stream.handler.announcedBlockCount(fixture.partitionId)}")
    val failure = intercept[FetchFailedException] {
      readRecords(records)
    }
    assert(fetchFailedReasonOf(failure).reduceId == fixture.partitionId,
      "The fetch failure must name the partition whose stream was truncated")
    assert(fixture.coordinatorRef.invalidationsSent.map(_.reason) ==
        Seq(StreamingShuffleInvalidationReason.IncompleteStream),
      s"A truncated stream must be invalidated exactly once as an incomplete stream, but the " +
        s"invalidations were ${fixture.coordinatorRef.invalidationsSent.map(_.reason)}")
    assert(observedPartialReadInvalidations() == 1L,
      s"Exactly one partial read invalidation must be recorded, but " +
        s"${observedPartialReadInvalidations()} was")
  }

  test("a conflicting second end of stream fails the fetch and cannot rewrite the first total") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(stream.deliver)
    stream.deliver(fixture.terminator(blocks.size.toLong))
    assert(stream.handler.isStreamComplete(fixture.partitionId),
      "The matching terminator must be accepted")
    assert(stream.handler.announcedBlockCount(fixture.partitionId).contains(blocks.size.toLong),
      "The accepted terminator's total is the one the consumer holds")

    stream.deliver(fixture.terminator(blocks.size.toLong + 3L))
    assert(stream.handler.announcedBlockCount(fixture.partitionId).contains(blocks.size.toLong),
      s"A conflicting terminator must not rewrite the accepted total, but the total read " +
        s"${stream.handler.announcedBlockCount(fixture.partitionId)}")
    assert(stream.handler.rejectedTerminationCount(fixture.partitionId) == 1L,
      "The conflicting terminator must be counted as refused")
    val failure = intercept[FetchFailedException] {
      readRecords(records)
    }
    assert(fetchFailedReasonOf(failure).reduceId == fixture.partitionId,
      "The contradiction must fail the fetch for the partition being read")
    assert(fixture.coordinatorRef.invalidationsSent.map(_.reason) ==
        Seq(StreamingShuffleInvalidationReason.IncompleteStream),
      s"A contradicted stream must be invalidated as an incomplete stream, but the invalidations " +
        s"were ${fixture.coordinatorRef.invalidationsSent.map(_.reason)}")
  }

  test("a repeated identical end of stream is ignored and the read still completes") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(stream.deliver)
    stream.deliver(fixture.terminator(blocks.size.toLong))
    stream.deliver(fixture.terminator(blocks.size.toLong))

    assert(stream.handler.duplicateTerminationCount(fixture.partitionId) == 1L,
      s"Exactly one duplicate terminator must be counted, but " +
        s"${stream.handler.duplicateTerminationCount(fixture.partitionId)} was")
    assert(stream.handler.rejectedTerminationCount(fixture.partitionId) == 0L,
      "An identical repeat is not a contradiction and must not be refused")
    assert(readRecords(records) == expectedRecords,
      "A duplicated end of stream must leave the partition's records exactly as they were")
    assert(fixture.coordinatorRef.invalidationsSent.isEmpty,
      "A completed stream must invalidate nothing, however many times its end was announced")
    assert(observedPartialReadInvalidations() == 0L,
      "A duplicated terminator is not a partial read")
  }

  test("a data block after the end of stream fails the fetch rather than extending the partition") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(stream.deliver)
    stream.deliver(fixture.terminator(blocks.size.toLong))
    val extraBlock = dataBlock(fixture.shuffleId, ProducerMapId, fixture.partitionId,
      blocks.size.toLong, payload.take(8))
    stream.deliver(extraBlock)

    assert(stream.handler.blocksAfterTerminationCount(fixture.partitionId) == 1L,
      s"A block after the end of stream must be refused, but " +
        s"${stream.handler.blocksAfterTerminationCount(fixture.partitionId)} refusal(s) were " +
        "counted")
    assert(stream.handler.announcedBlockCount(fixture.partitionId).contains(blocks.size.toLong),
      "A refused late block must not move the accepted total")
    val failure = intercept[FetchFailedException] {
      readRecords(records)
    }
    assert(fetchFailedReasonOf(failure).reduceId == fixture.partitionId,
      "A producer sending past its own end of stream must fail the fetch for that partition")
    assert(fixture.coordinatorRef.invalidationsSent.map(_.reason) ==
        Seq(StreamingShuffleInvalidationReason.IncompleteStream),
      s"The invalidation must name an incomplete stream, but the invalidations were " +
        s"${fixture.coordinatorRef.invalidationsSent.map(_.reason)}")
  }

  // -----------------------------------------------------------------------------------------------
  // Pre-decode binding, and hostile bytes.

  test("a frame on a foreign channel is refused before decode and allocates nothing") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    fixture.reader.read()
    val stream = fixture.connector.onlyStream
    val handler = stream.handler
    val quotaBefore = fixture.backpressure.reservedReceiveQuotaBytes
    val trackedBefore = handler.trackedPartitionCount

    stream.deliverFromForeignChannel(blocks.head)

    assert(handler.foreignChannelCallbackCount >= 1L,
      s"A frame from a foreign channel must be counted as refused, but the count read " +
        s"${handler.foreignChannelCallbackCount}")
    assert(handler.acceptedBlockCount(fixture.partitionId) == 0L,
      "A frame refused at the binding must never be admitted")
    assert(handler.expectedSequenceNumber(fixture.partitionId) == 0L,
      "A frame refused at the binding must not move the sequence frontier")
    assert(handler.trackedPartitionCount == trackedBefore,
      s"A frame refused before decode must allocate no per-partition state, but the tracked " +
        s"count moved from $trackedBefore to ${handler.trackedPartitionCount}")
    assert(fixture.backpressure.reservedReceiveQuotaBytes == quotaBefore,
      s"A frame refused before decode must charge no receive quota, but the reservation moved " +
        s"from $quotaBefore to ${fixture.backpressure.reservedReceiveQuotaBytes}")
    assert(!handler.isProducerLost,
      "A misdirected frame is someone else's routing mistake and must not declare this producer " +
        "lost")
  }

  test("raw malformed frames are refused without allocating state or receive quota") {
    startContext()
    val fixture = new ReaderFixture()

    fixture.reader.read()
    val stream = fixture.connector.onlyStream
    val handler = stream.handler
    val quotaBefore = fixture.backpressure.reservedReceiveQuotaBytes
    val trackedBefore = handler.trackedPartitionCount

    // Every one of these is a frame a hostile peer can send and no message constructor can produce:
    // an empty frame, a type discriminator alone, an unknown discriminator, a truncated header, and
    // a header whose protocol version this build does not speak.
    val emptyFrame = Array.emptyByteArray
    val typeByteOnly = Array[Byte](StreamingShuffleMessageType.DATA_BLOCK.id())
    val unknownType = Array[Byte](0x7f.toByte, 0, 0, 0, 0)
    val truncatedHeader = Array[Byte](StreamingShuffleMessageType.ACK.id(), 1, 0, 0)
    val futureVersion = Array[Byte](StreamingShuffleMessageType.HEARTBEAT.id(),
      (StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION + 1).toByte, 0, 0, 0, 0, 0, 0, 0, 0)
    val hostileFrames =
      Seq(emptyFrame, typeByteOnly, unknownType, truncatedHeader, futureVersion)
    hostileFrames.foreach(stream.deliverRaw)

    assert(handler.acceptedBlockCount(fixture.partitionId) == 0L,
      "No malformed frame may be admitted as a block")
    assert(handler.trackedPartitionCount == trackedBefore,
      s"Malformed frames must allocate no per-partition state, but the tracked count moved from " +
        s"$trackedBefore to ${handler.trackedPartitionCount}")
    assert(fixture.backpressure.reservedReceiveQuotaBytes == quotaBefore,
      s"Malformed frames must charge no receive quota, but the reservation moved from " +
        s"$quotaBefore to ${fixture.backpressure.reservedReceiveQuotaBytes}")
    assert(handler.isProducerLost,
      "A peer sending frames this build cannot parse is a producer this task must stop trusting")
    assert(stream.deliveredFrameBuffers.forall(_.outstandingReferences == 0),
      "The consumer must never retain a buffer the transport owns, whatever the frame contained")
  }

  // -----------------------------------------------------------------------------------------------
  // Admission against closure: the executor's shared receive budget must balance exactly.

  test("a block admitted while the handler closes returns its receive quota exactly once") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    fixture.reader.read()
    val stream = fixture.connector.onlyStream
    val handler = stream.handler
    val baseline = fixture.backpressure.reservedReceiveQuotaBytes

    stream.deliver(blocks.head)
    assert(fixture.backpressure.reservedReceiveQuotaBytes > baseline,
      "An admitted block must charge the executor's shared receive budget")

    handler.close()
    assert(fixture.backpressure.reservedReceiveQuotaBytes == baseline,
      s"Closing must return every charged byte, but the reservation read " +
        s"${fixture.backpressure.reservedReceiveQuotaBytes} against a baseline of $baseline")
    stream.deliver(blocks(1))
    stream.deliver(blocks(2))

    assert(fixture.backpressure.reservedReceiveQuotaBytes == baseline,
      s"Blocks arriving after closure must leave the reservation at its baseline of $baseline, " +
        s"but it read ${fixture.backpressure.reservedReceiveQuotaBytes}")
    assert(handler.admissionsInFlightCount == 0,
      s"No admission may remain in flight once every delivery has returned, but " +
        s"${handler.admissionsInFlightCount} did")
    handler.close()
    assert(fixture.backpressure.reservedReceiveQuotaBytes == baseline,
      "A second close must return nothing, because the first returned everything")
  }

  test("admission racing closure returns the receive quota to its exact baseline") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)

    fixture.reader.read()
    val stream = fixture.connector.onlyStream
    val handler = stream.handler
    val baseline = fixture.backpressure.reservedReceiveQuotaBytes
    val blockBytes = payload.take(64)

    // Deliveries and the closure are started together against a barrier, so the interleaving is
    // decided by the scheduler rather than by the order of two statements -- which is the only way
    // to exercise the window the in-flight accounting closes.
    val admissionCount = 24
    val gate = new CountDownLatch(1)
    val admitting = new Thread(() => {
      gate.await()
      var sequence = 0L
      while (sequence < admissionCount) {
        stream.deliver(dataBlock(fixture.shuffleId, ProducerMapId, fixture.partitionId, sequence,
          blockBytes))
        sequence += 1L
      }
    }, "streaming-shuffle-admitting")
    admitting.setDaemon(true)
    val closing = new Thread(() => {
      gate.await()
      handler.close()
    }, "streaming-shuffle-closing")
    closing.setDaemon(true)
    admitting.start()
    closing.start()
    gate.countDown()
    admitting.join(TimeUnit.SECONDS.toMillis(30))
    closing.join(TimeUnit.SECONDS.toMillis(30))
    assert(!admitting.isAlive && !closing.isAlive,
      "Neither the admitting thread nor the closing thread may block: an admission is lock free " +
        "and a closure waits only a bounded number of spins for one")

    assert(handler.isClosed, "The handler must be closed once the racing thread has finished")
    assert(handler.admissionsInFlightCount == 0,
      s"Every admission must have settled, but ${handler.admissionsInFlightCount} was still in " +
        "flight")
    assert(fixture.backpressure.reservedReceiveQuotaBytes == baseline,
      s"However the admissions and the closure interleaved, the executor's shared receive " +
        s"reservation must return to its baseline of $baseline, but it read " +
        s"${fixture.backpressure.reservedReceiveQuotaBytes}")
  }

  // -----------------------------------------------------------------------------------------------
  // Connector ownership: creating a channel and publishing its owner is one transition.

  /**
   * A consumer handler of the shape the reader builds, for the connector cases.
   *
   * @param consumerId the consuming attempt's identity, which is what confines a shared channel
   *     to the handlers of one reduce task attempt: two handlers share a channel only if they agree
   *     on it
   */
  private def newConnectorHandler(
      conf: SparkConf,
      clock: ManualClock,
      mapId: Long = ProducerMapId,
      consumerId: String = "streaming-shuffle-connector-consumer"
  ): StreamingShuffleClientHandler = {
    val budget = TokenBucketRateLimiter.executorBudget(conf, clock)
    val protocol = new BackpressureProtocol(conf, null, budget, clock)
    new StreamingShuffleClientHandler(conf, shuffleId = 0, mapId = mapId,
      taskAttemptId = ProducerAttemptId, consumerId = consumerId,
      startPartition = ReducePartition, endPartition = ReducePartition + 1, backpressure = protocol,
      errorNotifier = new StreamingShuffleErrorNotifier(0, conf), clock = clock)
  }

  private def connectorLocation(mapId: Long = ProducerMapId): StreamingShuffleProducerLocation =
    StreamingShuffleProducerLocation("producer-executor-0", "producer-host", 7078,
      mapId, mapId.toInt, ProducerAttemptId,
      BlockManagerId("producer-executor-0", "block-manager-host", 7079))

  private def throttleHandler(
      handler: StreamingShuffleClientHandler,
      client: TransportClient): Int = {
    val payload = payloadOfLength(handler.mapId, 64)
    val blocks = BlocksPastHighWaterMark + 1
    (0 until blocks).foreach { index =>
      val block = dataBlock(
        handler.shuffleId, handler.mapId, ReducePartition, index.toLong, payload)
      handler.receive(client, block.toByteBuffer())
    }
    assert(handler.awaitDataPlaneIdle(10000L),
      "the consumer data plane must settle before the receive window is read")
    assert(!handler.isAutoReadEnabled,
      s"a handler holding ${handler.queuedEventCount} event(s) must have stopped reading once it " +
        s"passed the high-water mark of $BlocksPastHighWaterMark [accepted " +
        s"${handler.acceptedBlockCount(ReducePartition)}, duplicates " +
        s"${handler.duplicateBlockCount(ReducePartition)}, quota refusals " +
        s"${handler.quotaRefusalCount(ReducePartition)}, corrupt " +
        s"${handler.corruptBlockCount(ReducePartition)}, misaddressed " +
        s"${handler.misaddressedFrameCount}, replays " +
        s"${handler.replayRequestCount(ReducePartition)}, expecting " +
        s"${handler.expectedSequenceNumber(ReducePartition)}, cause " +
        s"${handler.currentThrottleCause}]")
    blocks
  }

  test("a channel created while the connector closes is closed rather than published") {
    val conf = streamingConf()
    val clock = newManualClock()
    val connector = new BarrieredStreamingShuffleProducerConnector(conf)
    val handler = newConnectorHandler(conf, clock)
    try {
      connector.duringCreation(() => connector.close())
      val client = connector.connect(
        StreamingShuffleProducerLocation("producer-executor-0", "producer-host", 7078,
          ProducerMapId, ProducerMapId.toInt, ProducerAttemptId,
          BlockManagerId("producer-executor-0", "block-manager-host", 7079)),
        handler)

      assert(client.isEmpty,
        "A connector that closed during creation must hand back no client, because nothing owns it")
      assert(connector.declinedConnectionCount == 1,
        s"The channel must be declined exactly once, but ${connector.declinedConnectionCount} " +
          "declination(s) were counted")
      assert(connector.boundChannelCount == 0,
        s"No handler may remain published after the closure, but " +
          s"${connector.boundChannelCount} did")
      assert(connector.createdClients.size == 1,
        "Exactly one channel must have been created, so that its release is what is being asserted")
      assert(connector.createdClients.forall(created => !created.getChannel().isOpen()),
        "Every channel the losing side of the race created must have been closed by it")
      assert(connector.unreleasedChannels == 0,
        s"A connector that declined its only channel must report no straggler, but reported " +
          s"${connector.unreleasedChannels}")
    } finally {
      connector.close()
      handler.close()
    }
  }

  test("a channel that dies before publication tells its handler instead of leaving it waiting") {
    val conf = streamingConf()
    val clock = newManualClock()
    val connector = new BarrieredStreamingShuffleProducerConnector(conf)
    val handler = newConnectorHandler(conf, clock)
    try {
      connector.duringCreation(() => {
        connector.createdClients.foreach { created =>
          connector.channelActive(created)
          created.getChannel().close().syncUninterruptibly()
          connector.channelInactive(created)
        }
      })
      val client = connector.connect(
        StreamingShuffleProducerLocation("producer-executor-0", "producer-host", 7078,
          ProducerMapId, ProducerMapId.toInt, ProducerAttemptId,
          BlockManagerId("producer-executor-0", "block-manager-host", 7079)),
        handler)

      assert(client.isEmpty,
        "A channel that died before it was published must not be handed back as a live one")
      assert(connector.earlyTerminalCallbackCount == 1,
        s"The terminal callback must be replayed exactly once at binding time, but " +
          s"${connector.earlyTerminalCallbackCount} replay(s) were counted")
      assert(connector.boundChannelCount == 0,
        "A channel that is already dead must leave no registration behind")
      assert(handler.awaitDataPlaneIdle(10000L),
        "The consumer data plane must settle after replaying the early terminal callback")
      assert(handler.isProducerLost,
        "The handler must have been told its producer is gone, which is what stops the reduce " +
          "task from waiting out its whole five-second connection timeout")
      assert(handler.producerLostElapsedMillis.isDefined,
        "The loss must be stamped, so a diagnostic can say how long ago the producer went")
    } finally {
      connector.close()
      handler.close()
    }
  }

  test("a connector refuses to connect once it is closed and releases what it held") {
    val conf = streamingConf()
    val clock = newManualClock()
    val connector = new BarrieredStreamingShuffleProducerConnector(conf)
    val handler = newConnectorHandler(conf, clock)
    val location = StreamingShuffleProducerLocation("producer-executor-0", "producer-host", 7078,
      ProducerMapId, ProducerMapId.toInt, ProducerAttemptId,
      BlockManagerId("producer-executor-0", "block-manager-host", 7079))
    try {
      val live = connector.connect(location, handler)
      assert(live.isDefined, "A connector that is open must hand back a live client")
      assert(connector.boundChannelCount == 1, "The live channel must be published")

      connector.close()
      assert(connector.boundChannelCount == 0,
        "Closing must withdraw every registration it held")
      assert(connector.createdClients.forall(created => !created.getChannel().isOpen()),
        "Closing must close every channel this connector opened, unmanaged clients included")
      assert(connector.unreleasedChannels == 0,
        s"A clean release must report no straggling channel, but reported " +
          s"${connector.unreleasedChannels}")
      assert(connector.transportResourcesTerminated,
        "Closing must await the event-loop group retained when the channel was created")

      val createdBefore = connector.createdClients.size
      assert(connector.connect(location, handler).isEmpty,
        "A closed connector must refuse to open a channel")
      assert(connector.createdClients.size == createdBefore,
        "A refused connect must not create a channel at all")
      assert(connector.declinedConnectionCount == 0,
        "A connect refused before creation is not a declined channel: nothing was opened")
    } finally {
      connector.close()
      handler.close()
    }
  }

  // -----------------------------------------------------------------------------------------------
  // The multiplexed consumer channel.

  test("two producers of one consumer share a channel and only its last participant closes it") {
    val conf = streamingConf()
    val clock = newManualClock()
    val connector = new BarrieredStreamingShuffleProducerConnector(conf)
    val first = newConnectorHandler(conf, clock, mapId = ProducerMapId)
    val second = newConnectorHandler(conf, clock, mapId = ProducerMapId + 1L)
    try {
      val firstClient = connector.connect(connectorLocation(first.mapId), first)
      assert(firstClient.isDefined, "the first producer must open a channel")
      assert(connector.createdClients.size == 1, "the first producer opens exactly one socket")
      assert(connector.sharedJoinCount == 0L,
        "the channel that was opened is not a join, so no join may be counted yet")

      val secondClient = connector.connect(connectorLocation(second.mapId), second)
      assert(secondClient.isDefined, "the second producer must be given a channel")
      assert(secondClient.get eq firstClient.get,
        "a second producer of the same consumer on the same executor must JOIN the channel this " +
          "consumer already holds rather than open a socket of its own")
      assert(connector.createdClients.size == 1,
        s"joining must open no further socket, but ${connector.createdClients.size} were created")
      assert(connector.sharedJoinCount == 1L,
        s"the join must be counted so that sharing is observable, but the count read " +
          s"${connector.sharedJoinCount}")
      assert(connector.channelClaimCount(firstClient.get) == 2,
        s"both participants must hold a claim, but the channel reported " +
          s"${connector.channelClaimCount(firstClient.get)}")
      assert(connector.boundChannelCount == 1,
        "two participants of one channel are one registration, not two")

      connector.release(first, firstClient.get)
      assert(firstClient.get.getChannel().isOpen(),
        "the channel must stay open while a producer the reduce task has not finished with is " +
          "still reading from it")
      assert(connector.channelClaimCount(firstClient.get) == 1,
        s"one claim must remain after the first release, but " +
          s"${connector.channelClaimCount(firstClient.get)} did")
      assert(connector.isShareJoinable(firstClient.get),
        "a channel with a live participant must still be joinable by a further producer")
      assert(first.isClosed, "the departing handler itself must be released")
      assert(!second.isClosed, "the remaining handler must be untouched by its peer's departure")

      connector.release(second, firstClient.get)
      assert(!firstClient.get.getChannel().isOpen(),
        "the last participant's departure must release the socket")
      assert(connector.channelClaimCount(firstClient.get) == 0,
        "no claim may remain once the last participant has left")
      assert(!connector.isShareJoinable(firstClient.get),
        "a released channel must be permanently unjoinable, or a joiner could be handed a socket " +
          "that has already been closed")
      assert(connector.boundChannelCount == 0,
        "the registration must be withdrawn with the channel it named")
      assert(second.isClosed, "the last handler must be released too")

      val third = newConnectorHandler(conf, clock, mapId = ProducerMapId + 2L)
      try {
        val thirdClient = connector.connect(connectorLocation(third.mapId), third)
        assert(thirdClient.isDefined, "a producer arriving after the release must still connect")
        assert(!(thirdClient.get eq firstClient.get),
          "a producer arriving after the last release must NOT be handed the closed channel")
        assert(thirdClient.get.getChannel().isOpen(),
          "the channel a late producer is given must be one it can actually use")
        assert(connector.createdClients.size == 2,
          "the late producer must have caused exactly one further socket to be opened")
      } finally {
        third.close()
      }
    } finally {
      connector.close()
      first.close()
      second.close()
    }
  }

  test("a join racing the last release is never handed a channel that is being closed") {
    val conf = streamingConf()
    val clock = newManualClock()
    val rounds = 200
    val handedClosedChannel = new AtomicInteger(0)
    val joinedLive = new AtomicInteger(0)
    val openedFresh = new AtomicInteger(0)
    val executor = ThreadUtils.newDaemonFixedThreadPool(2, "streaming-shuffle-share-race")
    try {
      (0 until rounds).foreach { round =>
        val connector = new BarrieredStreamingShuffleProducerConnector(conf)
        val resident = newConnectorHandler(conf, clock, mapId = ProducerMapId)
        val joiner = newConnectorHandler(conf, clock, mapId = ProducerMapId + 1L)
        try {
          val client = connector.connect(connectorLocation(resident.mapId), resident).getOrElse(
            fail(s"round $round must have opened a channel to race against"))
          val barrier = new CyclicBarrier(2)
          val releaseTask: Runnable = () => {
            barrier.await(RaceBarrierTimeoutSeconds, TimeUnit.SECONDS)
            connector.release(resident, client)
          }
          val joinTask: Runnable = () => {
            barrier.await(RaceBarrierTimeoutSeconds, TimeUnit.SECONDS)
            connector.connect(connectorLocation(joiner.mapId), joiner) match {
              case Some(given) =>
                if (!given.getChannel().isOpen()) {
                  handedClosedChannel.incrementAndGet()
                } else if (given eq client) {
                  joinedLive.incrementAndGet()
                } else {
                  openedFresh.incrementAndGet()
                }
              case None =>
                handedClosedChannel.incrementAndGet()
            }
          }
          val release = executor.submit(releaseTask)
          val join = executor.submit(joinTask)
          release.get(RaceBarrierTimeoutSeconds, TimeUnit.SECONDS)
          join.get(RaceBarrierTimeoutSeconds, TimeUnit.SECONDS)
        } finally {
          connector.close()
          resident.close()
          joiner.close()
        }
      }
    } finally {
      executor.shutdownNow()
    }

    assert(handedClosedChannel.get() == 0,
      s"no joiner may ever be handed a channel that is closed or be refused outright, but " +
        s"${handedClosedChannel.get()} of $rounds round(s) were")
    assert(joinedLive.get() + openedFresh.get() == rounds,
      s"every round must have ended in one of the two legal outcomes, but " +
        s"${joinedLive.get()} join(s) and ${openedFresh.get()} fresh channel(s) account for only " +
        s"${joinedLive.get() + openedFresh.get()} of $rounds")
    // Both orderings must actually have occurred, or the race was never run and the case above
    // proves nothing.
    assert(joinedLive.get() > 0,
      "the joiner must sometimes have won the race and joined the live channel, or the " +
        "barrier is not producing the interleaving this case exists to exercise")
    assert(openedFresh.get() > 0,
      "the releaser must sometimes have won the race, leaving the joiner to open a fresh channel")
  }

  test("the receive window of a shared channel obeys every participant, not the last to speak") {
    val conf = streamingConf()
    val clock = newManualClock()
    val connector = new BarrieredStreamingShuffleProducerConnector(conf)
    val first = newConnectorHandler(conf, clock, mapId = ProducerMapId)
    val second = newConnectorHandler(conf, clock, mapId = ProducerMapId + 1L)
    try {
      val client = connector.connect(connectorLocation(first.mapId), first).getOrElse(
        fail("the first producer must open a channel"))
      assert(connector.connect(connectorLocation(second.mapId), second).contains(client),
        "the second producer must join the same channel for this case to be about sharing")
      val channel = client.getChannel()

      assert(first.currentReadGate eq second.currentReadGate,
        "handlers multiplexed onto one channel must share one receive window, or each will " +
          "believe it owns the socket")
      assert(channel.config().isAutoRead,
        "a freshly opened channel must be reading from its socket")
      assert(first.isChannelReadEnabled && second.isChannelReadEnabled,
        "both participants must see the socket as reading before anything is throttled")

      throttleHandler(first, client)
      assert(!channel.config().isAutoRead,
        "a participant that has stopped reading must close the socket")
      assert(!first.isChannelReadEnabled && !second.isChannelReadEnabled,
        "the socket's state is one state, so every participant must report it")
      assert(second.isAutoReadEnabled,
        "the throttle belongs to the participant that raised it: the other's own intent is " +
          "unchanged, which is exactly why it must not be the one deciding the socket")
      assert(first.throttlingParticipantCount == 1,
        s"exactly one participant may be throttling, but " +
          s"${first.throttlingParticipantCount} were")

      throttleHandler(second, client)
      assert(first.throttlingParticipantCount == 2,
        s"both participants must be throttling, but ${first.throttlingParticipantCount} were")
      connector.release(second, client)
      assert(!channel.config().isAutoRead,
        "a participant leaving must not reopen a socket another participant still needs shut")
      assert(!first.isChannelReadEnabled,
        "the remaining participant must still see its window closed")
      assert(first.throttlingParticipantCount == 1,
        s"the departing participant's throttle must be withdrawn with it, leaving one, but " +
          s"${first.throttlingParticipantCount} remain")
      assert(channel.isOpen(),
        "releasing one of two participants must not close the channel itself")

      connector.release(first, client)
      assert(first.throttlingParticipantCount == 0,
        s"no throttle may outlive its participant, but ${first.throttlingParticipantCount} did")
      assert(!channel.isOpen(),
        "the last participant's departure releases the channel")
    } finally {
      connector.close()
      first.close()
      second.close()
    }
  }

  test("a departing throttled participant never wedges a shared channel shut") {
    val conf = streamingConf()
    val clock = newManualClock()
    val connector = new BarrieredStreamingShuffleProducerConnector(conf)
    val leaving = newConnectorHandler(conf, clock, mapId = ProducerMapId)
    val staying = newConnectorHandler(conf, clock, mapId = ProducerMapId + 1L)
    try {
      val client = connector.connect(connectorLocation(leaving.mapId), leaving).getOrElse(
        fail("the first producer must open a channel"))
      assert(connector.connect(connectorLocation(staying.mapId), staying).contains(client),
        "the second producer must join the same channel")
      val channel = client.getChannel()

      throttleHandler(leaving, client)
      assert(!channel.config().isAutoRead, "the throttling participant must close the socket")

      connector.release(leaving, client)
      assert(channel.isOpen(),
        "one of two participants leaving must not close the channel")
      assert(channel.config().isAutoRead,
        "the last throttle leaving with its participant must reopen the socket for the " +
          "participant that remains")
      assert(staying.isChannelReadEnabled,
        "the remaining participant must be able to receive again")
      assert(staying.throttlingParticipantCount == 0,
        s"no participant may still be throttling, but ${staying.throttlingParticipantCount} was")
    } finally {
      connector.close()
      leaving.close()
      staying.close()
    }
  }

  test("a channel read gate is disabled by any participant and enabled only by all of them") {
    // The rule stated directly, on the class that owns it, with participants that are nothing but
    // identities.
    val channel = new EmbeddedChannel(DefaultChannelId.newInstance())
    val gate = new StreamingShuffleChannelReadGate
    val alpha = new Object
    val beta = new Object
    try {
      gate.attach(channel)
      assert(gate.isReadEnabled && channel.config().isAutoRead,
        "a gate with no throttling participant must leave the socket reading")

      assert(gate.throttle(alpha, "alpha is out of credit"),
        "the first throttle must report that it closed the window")
      assert(!gate.isReadEnabled && !channel.config().isAutoRead,
        "one participant is enough to close the window")
      assert(!gate.throttle(beta, "beta's queue is full"),
        "a second throttle changes nothing about the socket, so it must report no transition")
      assert(gate.throttlingParticipantCount == 2,
        s"both participants must be recorded, but ${gate.throttlingParticipantCount} were")

      assert(!gate.resume(beta),
        "a participant resuming while another is throttled must NOT reopen the window, and must " +
          "report that it changed nothing")
      assert(!gate.isReadEnabled && !channel.config().isAutoRead,
        "the window must stay closed while any participant is throttled")

      assert(gate.resume(alpha), "the last participant to resume reopens the window")
      assert(gate.isReadEnabled && channel.config().isAutoRead,
        "with no participant throttling, the socket must read again")
      assert(gate.throttlingParticipantCount == 0,
        "a resumed participant must leave no record behind")

      assert(!gate.resume(alpha), "resuming a participant that is not throttled changes nothing")
      gate.throttle(alpha, "alpha is out of credit again")
      assert(!gate.throttle(alpha, "alpha is out of credit again"),
        "restating a throttle changes nothing")
      assert(!gate.isReadEnabled, "the restated throttle must still hold the window closed")

      gate.withdraw(alpha)
      assert(gate.isReadEnabled && channel.config().isAutoRead,
        "withdrawing the last throttling participant must reopen the socket")

      val late = new EmbeddedChannel(DefaultChannelId.newInstance())
      try {
        val lateGate = new StreamingShuffleChannelReadGate
        lateGate.throttle(alpha, "alpha throttled before the socket existed")
        assert(late.config().isAutoRead, "a fresh channel reads until something stops it")
        lateGate.attach(late)
        assert(!late.config().isAutoRead,
          "attaching a channel to a gate with a throttling participant must close it at once")
      } finally {
        late.close()
      }
    } finally {
      channel.close()
    }
  }

  test("a contended receive window ends obeying its participants, never the schedule") {
    // The rule above, asserted under contention rather than in sequence.
    val rounds = 400
    val throttledOutcomes = new AtomicInteger(0)
    val openOutcomes = new AtomicInteger(0)
    val channel = new EmbeddedChannel(DefaultChannelId.newInstance())
    val executor = ThreadUtils.newDaemonFixedThreadPool(2, "streaming-shuffle-read-gate-race")
    try {
      (0 until rounds).foreach { round =>
        val gate = new StreamingShuffleChannelReadGate
        val contender = new Object
        val bystander = new Object
        gate.attach(channel)
        assert(channel.config().isAutoRead,
          s"round $round must start from a reading socket, or it measures the wrong transition")

        gate.throttle(contender, "the contender is out of credit")
        assert(!channel.config().isAutoRead, s"round $round must start from a closed window")

        val barrier = new CyclicBarrier(2)
        val resumeTask: Runnable = () => {
          barrier.await(RaceBarrierTimeoutSeconds, TimeUnit.SECONDS)
          gate.resume(contender)
        }
        val throttleTask: Runnable = () => {
          barrier.await(RaceBarrierTimeoutSeconds, TimeUnit.SECONDS)
          gate.throttle(bystander, "the bystander's queue filled up")
        }
        val resume = executor.submit(resumeTask)
        val throttle = executor.submit(throttleTask)
        resume.get(RaceBarrierTimeoutSeconds, TimeUnit.SECONDS)
        throttle.get(RaceBarrierTimeoutSeconds, TimeUnit.SECONDS)

        val participants = gate.throttlingParticipantCount
        assert(gate.isReadEnabled == (participants == 0),
          s"round $round left the gate reporting reading=${gate.isReadEnabled} with " +
            s"$participants throttling participant(s): the socket must read if and only if none is")
        assert(channel.config().isAutoRead == gate.isReadEnabled,
          s"round $round left the channel at autoRead=${channel.config().isAutoRead} while the " +
            s"gate believed it had applied ${gate.isReadEnabled}")
        assert(participants == 1,
          s"round $round must end with the bystander alone throttling, but $participants were")
        if (gate.isReadEnabled) {
          openOutcomes.incrementAndGet()
        } else {
          throttledOutcomes.incrementAndGet()
        }

        gate.withdraw(bystander)
        assert(gate.isReadEnabled && channel.config().isAutoRead,
          s"round $round must leave a reopened socket once its last participant departs")
      }
    } finally {
      executor.shutdownNow()
      channel.close()
    }
    assert(throttledOutcomes.get() == rounds,
      s"every round ends with one participant throttling, so every round must end with a closed " +
        s"window, but ${openOutcomes.get()} of $rounds ended open")
  }

  test("a departed participant can never re-state a throttle into a shared channel") {
    val conf = streamingConf()
    val clock = newManualClock()
    val rounds = 60
    val executor = ThreadUtils.newDaemonFixedThreadPool(2, "streaming-shuffle-departure-race")
    try {
      (0 until rounds).foreach { round =>
        val connector = new BarrieredStreamingShuffleProducerConnector(conf)
        val leaving = newConnectorHandler(conf, clock, mapId = ProducerMapId)
        val staying = newConnectorHandler(conf, clock, mapId = ProducerMapId + 1L)
        try {
          val client = connector.connect(connectorLocation(leaving.mapId), leaving).getOrElse(
            fail(s"round $round must have opened a channel to race against"))
          assert(connector.connect(connectorLocation(staying.mapId), staying).contains(client),
            s"round $round must have joined both producers onto one channel")
          val channel = client.getChannel()
          val payload = payloadOfLength(leaving.mapId, 64)
          val barrier = new CyclicBarrier(2)

          val deliverTask: Runnable = () => {
            barrier.await(RaceBarrierTimeoutSeconds, TimeUnit.SECONDS)
            (0 to BlocksPastHighWaterMark).foreach { index =>
              val block =
                dataBlock(leaving.shuffleId, leaving.mapId, ReducePartition, index.toLong, payload)
              leaving.receive(client, block.toByteBuffer())
            }
          }
          val closeTask: Runnable = () => {
            barrier.await(RaceBarrierTimeoutSeconds, TimeUnit.SECONDS)
            connector.release(leaving, client)
          }
          val deliver = executor.submit(deliverTask)
          val close = executor.submit(closeTask)
          deliver.get(RaceBarrierTimeoutSeconds, TimeUnit.SECONDS)
          close.get(RaceBarrierTimeoutSeconds, TimeUnit.SECONDS)
          assert(leaving.awaitDataPlaneIdle(RaceBarrierTimeoutSeconds * 1000L),
            s"round $round must settle the departing handler's data plane before it is read")

          assert(!staying.currentReadGate.isThrottling(leaving),
            s"round $round left the departed handler recorded as throttling a channel it no " +
              "longer participates in")
          assert(staying.isAutoReadEnabled,
            s"round $round throttled a handler that received nothing, cause " +
              s"${staying.currentThrottleCause}")
          assert(staying.throttlingParticipantCount == 0,
            s"round $round left ${staying.throttlingParticipantCount} participant(s) holding the " +
              "shared window shut with no living participant able to clear it")
          assert(staying.isChannelReadEnabled && channel.config().isAutoRead,
            s"round $round wedged the shared socket shut for the surviving producer")
        } finally {
          connector.close()
          leaving.close()
          staying.close()
        }
      }
    } finally {
      executor.shutdownNow()
    }
  }

  test("a producer-local session teardown leaves the other producers of a shared channel alive") {
    val conf = streamingConf()
    val clock = newManualClock()
    val listener = new StreamingShuffleListener(conf, clock)
    val channel = new EmbeddedChannel(DefaultChannelId.newInstance())
    val consumer = new TransportClient(channel, new TransportResponseHandler(channel))
    // The routing surface refuses a frame that carries no authenticated transport principal, so a
    // consumer that has not completed the platform handshake reaches no producer at all.
    consumer.setClientId("streaming-shuffle-reader-suite")
    val firstProducer = newServerHandler(conf, clock, listener, mapId = ProducerMapId)
    val secondProducer = newServerHandler(conf, clock, listener, mapId = ProducerMapId + 1L)
    try {
      listener.register(0, firstProducer.mapId, firstProducer)
      listener.register(0, secondProducer.mapId, secondProducer)

      listener.receive(consumer, heartbeat(0, firstProducer.mapId, ReducePartition, 0L)
        .toByteBuffer())
      listener.receive(consumer, heartbeat(0, secondProducer.mapId, ReducePartition, 0L)
        .toByteBuffer())
      assert(listener.channelParticipantCount(channel) == 2,
        s"the channel must have reached both producers, but reached " +
          s"${listener.channelParticipantCount(channel)}")
      assert(channel.isOpen(), "the consumer's channel must be open before anything is released")

      val closedOnFirst = listener.releaseChannelParticipation(
        channel, firstProducer, closeWhenLast = true)
      assert(!closedOnFirst,
        "releasing one of two participating producers must not close the consumer's channel")
      assert(channel.isOpen(),
        "the channel must stay open for the producer that is still serving this consumer")
      assert(listener.channelParticipantCount(channel) == 1,
        s"exactly one producer must remain, but ${listener.channelParticipantCount(channel)} did")
      assert(listener.faultedChannelCloseCount == 0L,
        "a producer-local release is not a channel-global fault and must not be counted as one")

      val closedOnLast = listener.releaseChannelParticipation(
        channel, secondProducer, closeWhenLast = true)
      assert(closedOnLast,
        "the last participating producer leaving must reclaim the channel")
      assert(!channel.isOpen(), "an unreferenced consumer channel must be closed")
      assert(listener.channelParticipantCount(channel) == 0,
        "the participation set must be withdrawn with the channel")
    } finally {
      firstProducer.releaseAll()
      secondProducer.releaseAll()
      listener.releaseAll()
      channel.close()
    }
  }

  test("a channel-global fault closes the channel and is counted apart from a local release") {
    val conf = streamingConf()
    val clock = newManualClock()
    val listener = new StreamingShuffleListener(conf, clock)
    val channel = new EmbeddedChannel(DefaultChannelId.newInstance())
    val consumer = new TransportClient(channel, new TransportResponseHandler(channel))
    // Authenticated for the same reason: an unauthenticated frame is refused before it is routed,
    // and this fixture needs the frame to reach the producer it names.
    consumer.setClientId("streaming-shuffle-reader-suite")
    val producer = newServerHandler(conf, clock, listener, mapId = ProducerMapId)
    try {
      listener.register(0, producer.mapId, producer)
      listener.receive(consumer, heartbeat(0, producer.mapId, ReducePartition, 0L).toByteBuffer())
      assert(listener.channelParticipantCount(channel) == 1,
        "the channel must have reached the producer before the fault is raised")

      listener.closeFaultedChannel(
        channel, producer, "it sent a frame a producer may never be sent")
      assert(!channel.isOpen(), "a channel-global fault must close the channel")
      assert(listener.faultedChannelCloseCount == 1L,
        s"the teardown must be counted so it can be told apart from a producer-local " +
          s"release, but the count read ${listener.faultedChannelCloseCount}")
    } finally {
      producer.releaseAll()
      listener.releaseAll()
      channel.close()
    }
  }

  test("a held end-of-stream retires the flow-control ledger exactly as an immediate one does") {
    val conf = streamingConf()
    val clock = newManualClock()
    val (handler, protocol, key) = newLedgeredConsumer(conf, clock, "deferred-completion-consumer")
    val connector = new BarrieredStreamingShuffleProducerConnector(conf)
    try {
      val client = connector.connect(connectorLocation(handler.mapId), handler).getOrElse(
        fail("the producer must open a channel"))
      val payload = payloadOfLength(handler.mapId, 64)
      def block(sequenceNumber: Long): DataBlockMessage =
        dataBlock(handler.shuffleId, handler.mapId, ReducePartition, sequenceNumber, payload)

      feed(handler, client, block(0L))
      feed(handler, client, block(2L))
      assert(handler.quarantinedPositionCount(ReducePartition) > 0,
        "the withheld position must be quarantined for a replay, or nothing is being deferred")

      feed(handler, client,
        streamTermination(handler.shuffleId, handler.mapId, ReducePartition, 3L))
      assert(handler.announcedBlockCount(ReducePartition).isEmpty,
        "a held terminator must not publish its total, because the stream has not ended yet")
      assert(!protocol.isStreamTerminated(key),
        "and it must not retire the ledger either, for the same reason")
      assert(handler.rejectedTerminationCount(ReducePartition) == 0L,
        s"a terminator held for an outstanding repair is not a contradiction and must not be " +
          s"refused, but ${handler.rejectedTerminationCount(ReducePartition)} was")

      feed(handler, client, block(1L))
      feed(handler, client, block(2L))

      assert(handler.announcedBlockCount(ReducePartition).contains(3L),
        s"the held total must be published once the repair completed the stream, but the handler " +
          s"reports ${handler.announcedBlockCount(ReducePartition)}")
      assert(protocol.isStreamTerminated(key),
        "and the ledger must be retired by the same transition: a deferred completion that " +
          "leaves the ledger live is the regression this case exists for")
      assert(protocol.announcedBlockCount(key).contains(3L),
        s"the ledger must hold the same total the handler published, but it holds " +
          s"${protocol.announcedBlockCount(key)}")

      clock.advance(ProducerConnectionTimeoutMillis + ConsumerLivenessTimeoutMillis)
      assert(!protocol.isProducerTimedOut(key),
        "a completed stream's silence is completion, not a producer that died")
      assert(!protocol.isConsumerTimedOut(key),
        "and a completed stream has nothing left for its consumer to acknowledge late")
      assert(protocol.timedOutProducerStreams.isEmpty && protocol.timedOutConsumerStreams.isEmpty,
        s"nor may the whole-set scans report it, but they reported " +
          s"${protocol.timedOutProducerStreams} and ${protocol.timedOutConsumerStreams}")
      assert(!protocol.shouldSendHeartbeat(key),
        "and a terminated stream is never due a heartbeat")

      val completions = drainCompletions(handler).collect {
        case completed: StreamingShuffleClientHandler.StreamCompleted => completed
      }
      assert(completions.map(_.totalBlocks) == Seq(3L),
        s"exactly one completion marker carrying the announced total must be queued, but the " +
          s"markers were ${completions.map(_.totalBlocks)}")
      assert(!handler.isProducerLost,
        "and a stream that completed may not be reported as a lost producer")
    } finally {
      connector.close()
      handler.close()
    }
  }

  test("a replacement asked for after the end of stream repairs the read instead of failing it") {
    val conf = streamingConf()
    val clock = newManualClock()
    val (handler, protocol, key) = newLedgeredConsumer(conf, clock, "post-termination-consumer")
    val connector = new BarrieredStreamingShuffleProducerConnector(conf)
    try {
      val client = connector.connect(connectorLocation(handler.mapId), handler).getOrElse(
        fail("the producer must open a channel"))
      val payload = payloadOfLength(handler.mapId, 64)
      val only = dataBlock(handler.shuffleId, handler.mapId, ReducePartition, 0L, payload)

      feed(handler, client, only)
      feed(handler, client,
        streamTermination(handler.shuffleId, handler.mapId, ReducePartition, 1L))
      assert(handler.announcedBlockCount(ReducePartition).contains(1L),
        "the stream must have ended for this case to be about what follows a terminator")
      assert(protocol.isStreamTerminated(key),
        "and its ledger must be retired, which is the state the repair has to work through")

      assert(handler.requestRetransmission(ReducePartition, 0L, 0L),
        "a position the consumer has not acknowledged is still retained, so the replay request " +
          "must be accepted even though the stream has ended")
      assert(handler.quarantinedPositionCount(ReducePartition) == 1,
        s"the requested position must be quarantined, but " +
          s"${handler.quarantinedPositionCount(ReducePartition)} position(s) were")

      val cursorBeforeRepair = handler.expectedSequenceNumber(ReducePartition)
      feed(handler, client, only)

      assert(handler.repairsAfterTerminationCount(ReducePartition) == 1L,
        s"the replacement must be admitted as the repair it is, but " +
          s"${handler.repairsAfterTerminationCount(ReducePartition)} were")
      assert(handler.blocksAfterTerminationCount(ReducePartition) == 0L,
        s"and it may not be counted as a block past the end of stream, but " +
          s"${handler.blocksAfterTerminationCount(ReducePartition)} was")
      assert(!handler.isProducerLost,
        "a producer answering a request this consumer made is not a lost producer")
      assert(handler.expectedSequenceNumber(ReducePartition) == cursorBeforeRepair,
        s"a repair below the frontier must move no cursor, but it moved from $cursorBeforeRepair " +
          s"to ${handler.expectedSequenceNumber(ReducePartition)}")
      assert(handler.announcedBlockCount(ReducePartition).contains(1L),
        "and it may not disturb the total the terminator fixed")
      assert(handler.quarantinedPositionCount(ReducePartition) == 0,
        "the quarantine must be released by the admission, or the reader would ask again")

      val delivered = drainCompletions(handler).collect {
        case StreamingShuffleClientHandler.BlockReceived(block) => block.sequenceNumber()
      }
      assert(delivered.count(_ == 0L) == 2,
        s"both the original and its replacement must have been handed to the task, but the " +
          s"positions delivered were ${delivered.mkString("[", ", ", "]")}")

      feed(handler, client,
        dataBlock(handler.shuffleId, handler.mapId, ReducePartition, 1L, payload))
      assert(handler.blocksAfterTerminationCount(ReducePartition) == 1L,
        s"a block at the announced frontier must still be refused, but " +
          s"${handler.blocksAfterTerminationCount(ReducePartition)} was")
      assert(handler.repairsAfterTerminationCount(ReducePartition) == 1L,
        "and it may not be mistaken for a repair, which was never asked for at that position")
      assert(handler.isProducerLost,
        "a producer sending past its own end of stream is lost, exactly as it was before")
    } finally {
      connector.close()
      handler.close()
    }
  }

  private def newLedgeredConsumer(
      conf: SparkConf,
      clock: ManualClock,
      consumerId: String
  ): (StreamingShuffleClientHandler, BackpressureProtocol, BackpressureStreamKey) = {
    val budget = TokenBucketRateLimiter.executorBudget(conf, clock)
    val protocol = new BackpressureProtocol(conf, null, budget, clock)
    protocol.registerShuffle(0, NumPartitions)
    val key = BackpressureStreamKey.forConsumer(
      0, ProducerMapId, ProducerAttemptId, ReducePartition, consumerId)
    assert(protocol.registerStream(key, LedgeredConsumerCreditBytes),
      "the stream must be registered for the ledger to record anything about it")
    val handler = new StreamingShuffleClientHandler(conf, shuffleId = 0, mapId = ProducerMapId,
      taskAttemptId = ProducerAttemptId, consumerId = consumerId,
      startPartition = ReducePartition, endPartition = ReducePartition + 1,
      backpressure = protocol,
      errorNotifier = new StreamingShuffleErrorNotifier(0, conf), clock = clock)
    (handler, protocol, key)
  }

  private def feed(
      handler: StreamingShuffleClientHandler,
      client: TransportClient,
      message: StreamingShuffleMessage): Unit = {
    handler.receive(client, message.toByteBuffer())
    // A frame is decoded and applied off the event loop, so `receive` returns before the frame has
    // had any effect.
    assert(handler.awaitDataPlaneIdle(10000L),
      s"the consumer data plane must settle after a ${message.getClass.getSimpleName} frame")
  }

  private def drainCompletions(
      handler: StreamingShuffleClientHandler): Seq[StreamingShuffleClientHandler.Inbound] = {
    val drained = mutable.ArrayBuffer.empty[StreamingShuffleClientHandler.Inbound]
    var event = handler.poll()
    while (event.isDefined) {
      drained += event.get
      event = handler.poll()
    }
    drained.toSeq
  }

  /** A producer-side handler of the shape the manager builds, for the channel-ownership cases. */
  private def newServerHandler(
      conf: SparkConf,
      clock: ManualClock,
      routes: StreamingShuffleRouteRegistry,
      mapId: Long): StreamingShuffleServerHandler = {
    val budget = TokenBucketRateLimiter.executorBudget(conf, clock)
    val protocol = new BackpressureProtocol(conf, null, budget, clock)
    new StreamingShuffleServerHandler(
      conf,
      shuffleId = 0,
      mapId = mapId,
      taskAttemptId = ProducerAttemptId + mapId,
      numPartitions = NumPartitions,
      blockResolver = new StreamingShuffleBlockResolver(conf),
      routes = routes,
      backpressure = protocol,
      rateLimiter = budget.limiterFor(0),
      errorNotifier = new StreamingShuffleErrorNotifier(0, conf),
      fallbackPolicy = new StreamingShuffleFallbackPolicy(conf, clock),
      clock = clock)
  }
}
