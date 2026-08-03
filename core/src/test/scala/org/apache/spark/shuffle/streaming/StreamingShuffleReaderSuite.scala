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

import java.io.ByteArrayOutputStream
import java.lang.reflect.Modifier
import java.nio.ByteBuffer
import java.util.Locale
import java.util.concurrent.{Callable, CountDownLatch}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import _root_.io.netty.channel.embedded.EmbeddedChannel

import org.apache.spark.{FetchFailed, LocalSparkContext, ShuffleDependency, SparkConf, SparkContext, SparkException, SparkFunSuite, SparkThrowable, TaskContext, TaskContextImpl}
import org.apache.spark.internal.config.SHUFFLE_COMPRESS
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{TransportClient, TransportResponseHandler}
import org.apache.spark.network.protocol.OneWayMessage
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, DataBlockMessage, HeartbeatMessage, RetransmitRequestMessage, StreamingShuffleChecksum, StreamingShuffleMessage, StreamTerminationMessage}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcTimeout}
import org.apache.spark.shuffle.{FetchFailedException, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.{ManualClock, ThreadUtils}

/**
 * One consumer channel a test has opened to a streaming shuffle producer.
 *
 * It exists because the reader reaches its producers through
 * [[StreamingShuffleProducerConnector]], which is abstract precisely so that a suite can drive
 * producer loss, corruption repair, sequence violations and orderly completion with no port and no
 * timing. This class is the other side of that seam: it holds the handler the reader built, the
 * Netty channel the frames travel on, and the transport client the reader was handed, so a test can
 * push an inbound frame in and read the consumer's outbound frames back out.
 *
 * An `EmbeddedChannel` rather than a mock: the consumer's acknowledgements, its heartbeats and its
 * retransmission requests are all written through `Channel.writeAndFlush`, and an embedded channel
 * is the one way to observe exactly those writes without a socket. A mock would let an assertion
 * pass that the real encoder would reject.
 *
 * @param location the producer this channel reaches
 * @param handler the consumer handler the reader bound to the channel
 * @param channel the embedded channel the frames travel on
 * @param client the transport client the reader holds
 */
private[streaming] class StreamingShuffleTestProducerStream(
    val location: StreamingShuffleProducerLocation,
    val handler: StreamingShuffleClientHandler,
    val channel: EmbeddedChannel,
    val client: TransportClient) {

  private val deliveredBuffers = mutable.ArrayBuffer.empty[RecordingStreamingManagedBuffer]

  /**
   * Delivers one frame to the consumer exactly as the transport would.
   *
   * The bytes are handed over inside a counting managed buffer so that a suite can prove the
   * consumer never retains the transport's buffer: a streaming block's payload is copied out of the
   * frame at decode time, so nothing the transport owns may outlive the call.
   *
   * @param message the frame to deliver
   * @return the counting buffer the frame was delivered in
   */
  def deliver(message: StreamingShuffleMessage): RecordingStreamingManagedBuffer = {
    val framed = message.toByteBuffer()
    val bytes = new Array[Byte](framed.remaining())
    framed.duplicate().get(bytes)
    val buffer = new RecordingStreamingManagedBuffer(new NioManagedBuffer(ByteBuffer.wrap(bytes)))
    synchronized {
      deliveredBuffers += buffer
    }
    handler.receive(client, buffer.nioByteBuffer())
    buffer
  }

  /** Every counting buffer a frame has been delivered in, in delivery order. */
  def deliveredFrameBuffers: Seq[RecordingStreamingManagedBuffer] =
    synchronized(deliveredBuffers.toSeq)

  /**
   * Every frame the consumer has written since this was last called, decoded in write order.
   *
   * Decoding through the protocol's own single entry point rather than by inspecting bytes is what
   * makes the assertion meaningful: a frame this returns is a frame a real producer could read.
   *
   * @return the frames, oldest first
   */
  def drainOutboundMessages(): Seq[StreamingShuffleMessage] = {
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

  /** Closes the channel under the consumer, which is what a producer going away looks like. */
  def closeChannel(): Unit = {
    channel.close().syncUninterruptibly()
  }
}

/**
 * A producer connector that hands out embedded channels instead of sockets.
 *
 * Injecting the connector is the whole reason the reader's abstract seam exists: every failure this
 * suite asserts on -- a refused connection, a lost producer, a corrupt block, a truncated stream --
 * is reachable here without a port, a thread pool or a timing assumption.
 *
 * The connector is owned by whoever built it and closed when that owner stops, never by a reader.
 * [[closeCallCount]] is what lets a suite prove the reader honours that ownership.
 *
 * @param refusalsBeforeSuccess how many connection attempts to refuse before the first success,
 *                              which is how a suite reaches the reader's bounded retry ladder
 */
private[streaming] class StreamingShuffleTestProducerConnector(refusalsBeforeSuccess: Int = 0)
  extends StreamingShuffleProducerConnector {

  private val opened = mutable.ArrayBuffer.empty[StreamingShuffleTestProducerStream]
  private val refusalsRemaining = new AtomicInteger(refusalsBeforeSuccess)
  private val attempts = new AtomicInteger(0)
  private val closeCalls = new AtomicInteger(0)

  /** Opens after the first channel has been created, so a test can wait for it without sleeping. */
  val firstConnection: CountDownLatch = new CountDownLatch(1)

  override def connect(
      location: StreamingShuffleProducerLocation,
      handler: StreamingShuffleClientHandler): Option[TransportClient] = {
    attempts.incrementAndGet()
    if (refusalsRemaining.getAndDecrement() > 0) {
      None
    } else {
      val channel = new EmbeddedChannel()
      val client = new TransportClient(channel, new TransportResponseHandler(channel))
      val stream = new StreamingShuffleTestProducerStream(location, handler, channel, client)
      synchronized {
        opened += stream
      }
      // The transport raises this callback for a live channel, and the consumer's subscription --
      // the in-progress block request -- is written from it. Raising it here is what makes the
      // fixture behave like the production connector rather than merely resemble it.
      handler.channelActive(client)
      firstConnection.countDown()
      Some(client)
    }
  }

  override def close(): Unit = {
    closeCalls.incrementAndGet()
  }

  /** Every channel opened so far, in the order the reader opened them. */
  def streams: Seq[StreamingShuffleTestProducerStream] = synchronized(opened.toSeq)

  /** The single channel this fixture opened, which is what a one-producer test wants. */
  def onlyStream: StreamingShuffleTestProducerStream = {
    val open = streams
    assert(open.size == 1,
      s"Expected exactly one producer channel to have been opened but found ${open.size}")
    open.head
  }

  /** How many times the reader asked for a channel, refusals included. */
  def connectAttemptCount: Int = attempts.get()

  /** How many times anything closed this connector. A reader must never be the one that does. */
  def closeCallCount: Int = closeCalls.get()
}

/**
 * A driver coordinator reference that answers a consumer's rendezvous synchronously.
 *
 * The reader resolves live producers with `askSync` against the driver's coordinator endpoint. A
 * reference of our own answers in the calling thread, which removes the dispatcher, the endpoint
 * registry and the reaper timer from every test in this suite, and -- more importantly -- lets a
 * test observe the exact state of the reader at the moment the reader asks a question. That is what
 * makes the ordering of the producer-failure sequence assertable rather than merely plausible.
 *
 * @param conf configuration the base class derives its default ask deadline from
 * @param clock the same clock the reader reads, advanced on each lookup by `lookupAdvanceMillis`
 * @param lookupAdvanceMillis how far to advance the clock inside a lookup, which is how a suite
 *                            makes the rendezvous wait a positive, deterministic number of
 *                            milliseconds so that fetch-wait accounting can be asserted on
 */
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

  /** Sets the answer every subsequent lookup receives. */
  def answerLookupWith(reply: Option[StreamingShuffleProducerLocations]): Unit = {
    locations.set(reply)
  }

  /**
   * Installs a callback invoked while an invalidation ask is in flight.
   *
   * This is the observation point for the producer-failure ordering contract: the callback runs
   * after the reader has discarded everything it took from the producer and after it has counted
   * the invalidation, but before the fetch failure has been thrown.
   *
   * @param callback what to run, given the invalidation the reader sent
   */
  def observeInvalidation(callback: InvalidateStreamingShuffleProducer => Unit): Unit = {
    onInvalidate.set(callback)
  }

  /** Every invalidation the reader sent, in order. */
  def invalidationsSent: Seq[InvalidateStreamingShuffleProducer] = synchronized(invalidations.toSeq)

  /** Every shuffle-wide fallback the reader declared, in order. */
  def fallbacksDeclared: Seq[DeclareStreamingShuffleFallback] = synchronized(declarations.toSeq)

  /** How many rendezvous lookups the reader performed. */
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

  /** Host this reference claims to live on, which only ever appears in a diagnostic. */
  val DriverHost: String = "streaming-shuffle-test-driver"

  /** Port this reference claims to listen on, which only ever appears in a diagnostic. */
  val DriverPort: Int = 7077

  /** Epoch every mutating ask is answered with, distinct from the coordinator's no-epoch value. */
  val ReportedEpoch: Long = 11L
}


/**
 * Tests for [[StreamingShuffleReader]], the consumer half of a streaming shuffle.
 *
 * The four cases the feature names are contracts and each has a test of its own: an in-progress
 * block request consumed partially, producer-failure detection on the five-second connection
 * timeout, partial-read invalidation with the upstream recomputation it triggers, and checksum
 * validation with the retransmission it provokes. Around them sit the invariants that make those
 * four safe -- that the reader declares no `stop()` and therefore releases everything through the
 * task-completion listener, that Spark's own shuffle-read accumulators are populated rather than
 * shadowed by counters of the reader's own, that a failure seen on a Netty thread is re-thrown on
 * the task thread, and that the channel's `autoRead` belongs to the client handler alone.
 *
 * <b>Determinism.</b> Nothing here sleeps. Time is a [[ManualClock]] the reader, the handler, the
 * protocol and the fallback policy all read, so a five-second timeout is one method call; the
 * transport is an `EmbeddedChannel`, so a frame arrives when a test hands it over and never before;
 * and the rendezvous is answered in the calling thread. The two assertions that genuinely need a
 * second thread -- proving the timeout does <i>not</i> fire early, and injecting a failure from an
 * I/O thread -- use a bounded wait through the sanctioned `ThreadUtils` wrappers.
 */
class StreamingShuffleReaderSuite
  extends SparkFunSuite
    with LocalSparkContext
    with StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  /** Reduce partitions the shuffle under test declares, following the feature's own example. */
  private val NumPartitions = DefaultPartitionCount

  /**
   * The one reduce partition each fixture reads. Deliberately neither the first nor the last, so
   * that a partition-range confusion shows up as a failure rather than passing by coincidence.
   */
  private val ReducePartition = 3

  /** Map tasks the stage declares. One producer is enough for every contract asserted here. */
  private val DeclaredMaps = 1

  /** The producing map task's identity, shared by its map id and its map index. */
  private val ProducerMapId = 0L

  /** Task attempt id of the producer, which is what makes its output one generation. */
  private val ProducerAttemptId = 200L

  /** Task attempt id of the consumer, which is what makes its receive window its own. */
  private val ConsumerAttemptId = 41L

  /** Coordinator epoch the handle is stamped with. */
  private val CoordinatorEpoch = 9L

  /** Capability token the coordinator issued for the shuffle under test. */
  private val CapabilityToken = "streaming-shuffle-reader-suite-token"

  /** Records each fixture's producer streams, small enough to compare as a set. */
  private val StreamedRecords: Seq[(Int, Int)] = (1 to 24).map(value => (value, value * 3))

  /** Blocks the payload is cut into when a test wants more than one. */
  private val BlockCount = 4

  /** Blocks queued to take the client handler past its inbound high-water mark. */
  private val BlocksPastHighWaterMark = StreamingShuffleClientHandler.INBOUND_QUEUE_HIGH_WATER_MARK

  /** Milliseconds the rendezvous is made to take, so fetch-wait accounting is assertable. */
  private val LookupWaitMillis = 7L

  /** First map index of the whole map range, which is the only range streaming serves. */
  private val FullRangeStart = 0

  /** One past the last map index of the whole map range. */
  private val FullRangeEnd = Int.MaxValue

  /** The condition a block that failed CRC32C verification is raised as. */
  private val ChecksumVerifyFailedCondition = "STREAMING_SHUFFLE_CHECKSUM_VERIFY_FAILED"

  /** SQLSTATE every streaming shuffle condition shares, matching the checksum precedent. */
  private val StreamingShuffleSqlState = "XXKST"

  override def beforeEach(): Unit = {
    super.beforeEach()
    // The metrics source is a JVM singleton, so its counters survive from one test into the next.
    // Resetting is what makes an assertion on partialReadInvalidations a statement about this test.
    resetStreamingShuffleMetrics()
  }

  /**
   * Everything one streaming read needs, wired the way `StreamingShuffleManager` wires it.
   *
   * A live `SparkContext` is created by the caller through [[startContext]], because the reader's
   * locality accounting reads `SparkEnv` and its deserialization uses the environment's serializer
   * manager -- the same reason the sort-path reader suite in the parent package starts one.
   *
   * @param numPartitions reduce partitions the shuffle declares
   * @param startPartition first reduce partition this reader reads, inclusive
   * @param endPartition one past the last reduce partition this reader reads
   * @param numMaps map tasks the stage declared
   * @param refusalsBeforeSuccess connection attempts the connector refuses before it succeeds
   * @param lookupAdvanceMillis milliseconds the clock advances inside each rendezvous lookup
   * @param useTaskReporter whether the reader reports to the task's own shuffle-read metrics rather
   *                        than to a recording reporter, which is how a suite proves the streaming
   *                        figures land on Spark's existing accumulators
   */
  private class ReaderFixture(
      numPartitions: Int = NumPartitions,
      startPartition: Int = ReducePartition,
      endPartition: Int = ReducePartition + 1,
      numMaps: Int = DeclaredMaps,
      refusalsBeforeSuccess: Int = 0,
      lookupAdvanceMillis: Long = 0L,
      useTaskReporter: Boolean = false) {

    val conf: SparkConf = streamingConf()
    val clock: ManualClock = newManualClock()
    val dependency: ShuffleDependency[Int, Int, Int] =
      shuffleDependencyFor(sc, conf, numPartitions, numRecords = StreamedRecords.size)
    val shuffleId: Int = dependency.shuffleId
    val handle: StreamingShuffleHandle[Int, Int, Int] = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId, dependency, numPartitions, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
      CoordinatorEpoch, CapabilityToken)
    val coordinator: StreamingShuffleCoordinator =
      new StreamingShuffleCoordinator(sc.env.rpcEnv, conf, clock)
    val backpressure: BackpressureProtocol = new BackpressureProtocol(
      conf, coordinator, TokenBucketRateLimiter.executorBudget(conf, clock), clock)
    val fallbackPolicy: StreamingShuffleFallbackPolicy =
      new StreamingShuffleFallbackPolicy(conf, clock)
    val connector: StreamingShuffleTestProducerConnector =
      new StreamingShuffleTestProducerConnector(refusalsBeforeSuccess)
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
    val producer: StreamingShuffleProducerLocation = producerLocation(ProducerMapId.toInt)

    coordinatorRef.answerLookupWith(Some(locationsReply(Seq(producer))))

    val readerContext: StreamingShuffleReaderContext = StreamingShuffleReaderContext(
      coordinatorRef, backpressure, fallbackPolicy, connector, sc.env.serializerManager)
    val reader: StreamingShuffleReader[Int, Int] = new StreamingShuffleReader[Int, Int](
      handle, FullRangeStart, FullRangeEnd, startPartition, endPartition, context, readMetrics,
      conf, readerContext, clock)

    /** The one reduce partition this fixture reads. */
    def partitionId: Int = startPartition

    /** The producing map task's block identity for this partition, as a diagnostic names it. */
    def blockName: String = ShuffleBlockId(shuffleId, ProducerMapId, partitionId).name

    /**
     * A producer location whose block-manager identity is the one its `MapStatus` would carry.
     *
     * The distinction matters: `MapOutputTracker` removes a map output only when the address in the
     * fetch failure equals the address in the recorded status, so a suite that let the reader
     * synthesize an address from the streaming endpoint would assert a fetch failure that recovers
     * nothing.
     *
     * @param mapIndex map index this producer produces
     * @return the location
     */
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

    /**
     * The rendezvous answer for a set of producers.
     *
     * @param producers producers the coordinator offers
     * @param fallback whether the shuffle has stood streaming down for every participant
     * @return the reply
     */
    def locationsReply(
        producers: Seq[StreamingShuffleProducerLocation],
        fallback: StreamingShuffleFallbackState = StreamingShuffleFallbackState())
      : StreamingShuffleProducerLocations =
      StreamingShuffleProducerLocations(
        shuffleId = shuffleId,
        locations = producers,
        numPartitions = numPartitions,
        numMaps = numMaps,
        completedMapIndexes = producers.map(_.mapIndex).toSet,
        protocolVersion = StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
        coordinatorEpoch = CoordinatorEpoch,
        fallback = fallback)

    /**
     * The payload bytes a producer streams for this partition.
     *
     * Wrapped through `SerializerManager.wrapStream` for the same `ShuffleBlockId` the reader
     * unwraps with, because that is the contract the streaming writer honours: a
     * partition's payload bytes, concatenated in sequence order across the blocks of one
     * producer, are exactly the bytes that wrapping produces. Wrapping once for the whole
     * partition rather than once per block is what makes the concatenation legal, since a codec's
     * framing spans blocks.
     *
     * @param records the key-value pairs to stream
     * @return the wrapped bytes
     */
    def encodePartition(records: Seq[(Int, Int)]): Array[Byte] = {
      val bytes = new ByteArrayOutputStream()
      val wrapped = sc.env.serializerManager.wrapStream(
        ShuffleBlockId(shuffleId, ProducerMapId, partitionId), bytes)
      val serialized = dependency.serializer.newInstance().serializeStream(wrapped)
      records.foreach { record =>
        serialized.writeKey(record._1)
        serialized.writeValue(record._2)
      }
      serialized.close()
      bytes.toByteArray
    }

    /**
     * The payload cut into correctly checksummed blocks numbered densely from zero.
     *
     * @param payload the whole partition's bytes
     * @param blocks how many blocks to cut it into
     * @return the blocks, in sequence order
     */
    def dataBlocksOf(payload: Array[Byte], blocks: Int = 1): Seq[DataBlockMessage] = {
      assert(payload.nonEmpty, "A partition payload must carry bytes to be cut into blocks")
      assert(blocks > 0, s"A partition must be cut into at least one block but was $blocks")
      val chunkSize = math.max(1, (payload.length + blocks - 1) / blocks)
      payload.grouped(chunkSize).toSeq.zipWithIndex.map { chunk =>
        dataBlock(shuffleId, ProducerMapId, partitionId, chunk._2.toLong, chunk._1)
      }
    }

    /** The orderly end of this partition's stream after the given number of blocks. */
    def terminator(totalBlocks: Long): StreamTerminationMessage =
      streamTermination(shuffleId, ProducerMapId, partitionId, totalBlocks)

    /** This partition's consumer ledger identity, taken from the ledger rather than rebuilt. */
    def consumerLedgerKey: Option[BackpressureStreamKey] =
      backpressure.registeredStreams.find(key => key.partitionId == partitionId)

    /**
     * A second reader over the same collaborators, for the construction guards.
     *
     * Every argument the guards care about is passed explicitly, so a guard that stopped guarding
     * would fail here rather than let a reader exist in a state the subsystem forbids.
     *
     * @param readerConf configuration the reader reads its own five entries from
     * @param startMapIndex first map index requested, inclusive
     * @param endMapIndex map index one past the last requested
     * @return the reader, if its own preconditions admit one
     */
    def newReader(
        readerConf: SparkConf,
        startMapIndex: Int,
        endMapIndex: Int): StreamingShuffleReader[Int, Int] =
      new StreamingShuffleReader[Int, Int](handle, startMapIndex, endMapIndex, startPartition,
        endPartition, context, readMetrics, readerConf, readerContext, clock)
  }

  /**
   * Starts the live context every fixture needs.
   *
   * The configuration is deliberately the stock one rather than a streaming one: nothing here goes
   * through `SparkEnv`'s shuffle manager, and a context on the sort-based default keeps the
   * reader under test the only streaming component in the JVM.
   *
   * Shuffle compression is off by default, and the reason is worth stating because it decides what
   * a partial-consumption assertion can see. A compression codec buffers a whole frame before it
   * yields a byte, so with compression on the deserializer needs every block of a small partition
   * before it can produce the first record -- which would hide the reader's own laziness behind the
   * codec's buffering. Turning it off makes the property under test observable. One test turns it
   * back on, because the wrapping contract has to hold under compression as well.
   *
   * @param compressShuffle whether the environment's serializer manager compresses shuffle payloads
   */
  private def startContext(compressShuffle: Boolean = false): Unit = {
    val testConf = new SparkConf(false).set(SHUFFLE_COMPRESS, compressShuffle)
    // Create a SparkContext as a convenient way of setting SparkEnv (needed because some of the
    // shuffle code calls SparkEnv.get()).
    sc = new SparkContext("local", "test", testConf)
  }

  /** The pairs a fixture's producer streams, as a set, which is how a shuffle's output compares. */
  private def expectedRecords: Set[(Int, Int)] = StreamedRecords.toSet

  /** Turns whatever the reader yielded into the same comparable shape. */
  private def readRecords(records: Iterator[Product2[Int, Int]]): Set[(Int, Int)] =
    records.map(record => (record._1, record._2)).toSet

  /**
   * Runs a read with the task context installed on this thread, exactly as an executor installs it.
   *
   * `FetchFailedException` registers itself on the ambient task context in its own constructor
   * (SPARK-19276), and the first-error-wins notifier re-asserts it there before it throws. Both
   * read the thread-local rather than a reference they were handed, so a suite that never installed
   * one would assert nothing about the mechanism the executor actually consults.
   *
   * @param context the task context to install for the duration of the body
   * @param body the read to run
   * @tparam T the body's result type
   * @return the body's value
   */
  private def withTaskContext[T](context: TaskContextImpl)(body: => T): T = {
    TaskContext.setTaskContext(context)
    try {
      body
    } finally {
      TaskContext.unset()
    }
  }

  /**
   * The task-failure reason a fetch failure converts into.
   *
   * `FetchFailedException` publishes its identity only through this conversion, and that is the
   * right thing to assert on in any case: the reason is what the executor sends to the driver and
   * what `MapOutputTracker` matches a dead map output against, so a suite that read the exception's
   * fields would be asserting something weaker than what recovery actually depends on.
   *
   * @param failure the fetch failure a read raised
   * @return the fetch-failed reason the unmodified scheduler acts upon
   */
  private def fetchFailedReasonOf(failure: FetchFailedException): FetchFailed =
    failure.toTaskFailedReason match {
      case fetchFailed: FetchFailed => fetchFailed
      case other =>
        throw new IllegalStateException(
          s"A streaming shuffle producer loss must convert into a fetch-failed reason so the " +
            s"unmodified scheduler recomputes the upstream stage, but it converted into $other")
    }


  test("in progress block request and partial consumption") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)
    assert(blocks.size > 1,
      s"The partition payload must span more than one block for partial consumption to mean " +
        s"anything, but it was cut into ${blocks.size}")

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream

    // The in-progress request is issued before a single byte of data exists on the wire, and it is
    // carried by the protocol's heartbeat, which is what makes one message both the subscription
    // and the liveness signal. Nothing here waited for the map stage to report complete output.
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

    blocks.foreach(block => stream.deliver(block))
    assert(stream.handler.acceptedBlockCount(fixture.partitionId) == blocks.size.toLong,
      s"Every delivered block must be admitted, but only " +
        s"${stream.handler.acceptedBlockCount(fixture.partitionId)} of ${blocks.size} were")

    assert(records.hasNext, "The reader must surface records from an unterminated stream")
    val firstRecord = records.next()
    assert(fixture.recordingMetrics.recordsRead == 1L,
      s"Exactly one record must have been surfaced, but the reporter counted " +
        s"${fixture.recordingMetrics.recordsRead}")
    assert(stream.handler.queuedEventCount > 0,
      "The reader must consume one block at a time, so blocks it has not reached yet must still " +
        "be waiting on the hand-off queue rather than having been drained into the reduce input")

    // Only now does the producer end the stream, so every record above was consumed while the
    // stream was still open -- which is the whole of what "in progress" claims.
    stream.deliver(fixture.terminator(blocks.size.toLong))
    val remaining = readRecords(records)
    val surfaced = remaining + ((firstRecord._1, firstRecord._2))
    assert(surfaced == expectedRecords,
      s"A streamed partition must yield exactly the records the producer sent: " +
        s"${expectedRecords.diff(surfaced).size} missing and " +
        s"${surfaced.diff(expectedRecords).size} unexpected")
    assert(fixture.recordingMetrics.recordsRead == StreamedRecords.size.toLong,
      s"Records read must equal the records surfaced, but the reporter counted " +
        s"${fixture.recordingMetrics.recordsRead} of ${StreamedRecords.size}")

    // Consumption, not arrival, is what frees the producer's memory, so the acknowledged position
    // must have advanced to the last block only once its last byte had been handed over.
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
    // The producer delivers its first block and then goes silent: no further block, and no orderly
    // end of stream. This is what a crash or a network partition looks like from the consumer.
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
    // Asserted on the task-failure reason rather than on the exception's fields, because that
    // reason is exactly what the scheduler consumes and what MapOutputTracker matches against.
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
    // owns the read. The test thread only advances the clock and delivers frames.
    val pool = ThreadUtils.newDaemonSingleThreadExecutor("streaming-shuffle-reader-suite")
    try {
      val reading = pool.submit(new Callable[Set[(Int, Int)]] {
        override def call(): Set[(Int, Int)] = readRecords(fixture.reader.read())
      })
      awaitLatch(fixture.connector.firstConnection, "the consumer channel to be opened")
      val stream = fixture.connector.onlyStream
      blocks.foreach(block => stream.deliver(block))

      // One millisecond short of the bound. The reader evaluates producer liveness on every empty
      // poll window, so it reaches the check repeatedly at this reading and declines every time.
      advanceJustBeforeProducerTimeout(fixture.clock)
      assert(stream.handler.millisSinceInbound(fixture.partitionId)
          .contains(JustBeforeProducerTimeoutMillis),
        s"The elapsed silence must be exactly ${JustBeforeProducerTimeoutMillis} ms")
      assert(!stream.handler.isProducerSilent(fixture.partitionId),
        s"A producer silent for only ${JustBeforeProducerTimeoutMillis} ms must not be reported " +
          s"as lost: the bound is ${ProducerConnectionTimeoutMillis} ms")

      // The producer then ends its stream normally, and the read completes. A reader that had
      // fired early could not reach this line.
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

    // The three steps of the sequence are observed at the one instant that can see all of them:
    // while the invalidation ask is in flight, which is after the discard and after the count and
    // before the fetch failure has been thrown.
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

    // Step one: the discard is atomic with respect to the failure. Everything taken from the
    // producer is gone and the channel that could deliver more is shut before anything is reported,
    // so a reduce task can never mix data that survived a failure with data the recomputation
    // produces.
    assert(channelClosedAtInvalidation.get().contains(true),
      s"The reader must discard everything it took from the lost producer and close its channel " +
        s"before it reports the loss, but the channel state at that moment was " +
        s"${channelClosedAtInvalidation.get()}")
    assert(queuedAtInvalidation.get().contains(0),
      s"No block from the lost producer may remain reachable when the loss is reported, but " +
        s"${queuedAtInvalidation.get()} event(s) were still queued")

    // Step two: exactly one increment per invalidated producer, and it precedes the report.
    assert(countedAtInvalidation.get().contains(1L),
      s"The partial read invalidation must be counted before the coordinator is told, but the " +
        s"counter read ${countedAtInvalidation.get()} at that moment")
    assert(observedPartialReadInvalidations() == 1L,
      s"Exactly one partial read invalidation must be recorded for one lost producer, but " +
        s"${observedPartialReadInvalidations()} was recorded")

    // Step three: the coordinator is told which generation to forget and why.
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

    // And the terminus: the unmodified scheduler recomputes the upstream stage because the task
    // context now carries a fetch failure, which is the only signal this feature is allowed to use.
    assert(fixture.context.fetchFailed.isDefined,
      "The task context must carry the fetch failure, which is what makes the executor report a " +
        "FetchFailed reason and the unmodified DAG scheduler resubmit the upstream stage")
    assert(fixture.connector.closeCallCount == 0,
      s"A reader must close only the channels it opened and never the executor-scoped connector, " +
        s"but the connector was closed ${fixture.connector.closeCallCount} time(s)")
  }

  test("checksum validation and retransmission") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    // One block, so the repair is observed on its own. A retransmission request quarantines the
    // position it names, and the producer answers it by replaying exactly that position. Keeping
    // the partition to a single block is what makes "the replay repaired the read" the only claim
    // under test here, rather than also a claim about how a producer resumes afterwards.
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

    // Verification happens before a block becomes records, so the corrupt copy is never admitted
    // and no part of it can reach the reduce input.
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

    // Repair inside the retained window is a retransmission request for exactly that one position:
    // an inclusive single-element window, which is the normal shape a corrupt block calls for.
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

    // The producer replays the block from memory or spill, and the read completes intact.
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

  test("a corrupt block that can no longer be replayed escalates to a fetch failure") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)
    val corrupt = corruptedDataBlock(fixture.shuffleId, ProducerMapId, fixture.partitionId,
      blocks.head.sequenceNumber(), blocks.head.copyPayload())

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    // The producer is gone, so its bytes are no longer serviceable: nothing can bring them back and
    // no retransmission request could be answered even if one were sent.
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
    // Zero is a valid total and means the partition was legitimately empty. The absence of data
    // therefore means completion, and must never be mistaken for the five-second producer lapse.
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
    // Only the first block arrives, but the producer announces the whole partition. Every block
    // that did arrive is intact, so a checksum cannot catch this: only reconciling the announced
    // total against what was consumed turns a truncated partition into a failure rather than a
    // silently short result.
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
    // from. That is why cleanup is registered on the task-completion listener instead.
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

    // Listeners run on a stack, so registration order decides teardown order: the reader registers
    // its own release before it opens a channel, which puts it last in the queue and therefore
    // first to run. A listener registered after completion is invoked immediately, which is what
    // this asserts, and the reader's own release is idempotent under a compare-and-set, so running
    // the completion path again releases nothing twice and throws nothing at all.
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

  test("the reader never retains a transport buffer") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(block => stream.deliver(block))
    stream.deliver(fixture.terminator(blocks.size.toLong))
    assert(readRecords(records) == expectedRecords, "The read must succeed before it is audited")
    fixture.context.markTaskCompleted(None)

    // A streaming block's payload is copied out of the frame while it is being decoded, so the
    // consumer never takes a reference on the transport's buffer -- which is what makes it
    // impossible for a transport buffer to outlive the call that delivered it. The counting wrapper
    // states that directly: no retain was taken, and therefore none is outstanding.
    val delivered = stream.deliveredFrameBuffers
    assert(delivered.size == blocks.size + 1,
      s"Every frame must have been delivered through a counting buffer, but ${delivered.size} of " +
        s"${blocks.size + 1} were")
    assert(delivered.forall(buffer => buffer.callsToRetain == 0),
      s"The consumer must copy a frame's bytes rather than retain the transport's buffer, but " +
        s"${delivered.count(buffer => buffer.callsToRetain > 0)} buffer(s) were retained")
    assert(delivered.forall(buffer => buffer.outstandingReferences == 0),
      s"No transport buffer may be left with an outstanding reference, but " +
        s"${delivered.count(buffer => buffer.outstandingReferences != 0)} were")
  }

  /**
   * Asserts that a completed task left nothing of a streaming read behind.
   *
   * @param fixture the fixture whose reader has completed
   * @param stream the channel the reader opened
   * @param description what the task did, quoted back in the failure message
   */
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

    // Without these the Web UI, the history server and the REST API report nothing at all for a
    // streaming shuffle, so they are asserted on the accumulators Spark already has rather than on
    // counters of the reader's own. There are no parallel counters to find.
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
    // Frames are handed over from a thread that is not the task thread, exactly as a Netty event
    // loop would. Those threads may only enqueue: verification, counting and reporting all belong
    // to the task thread, because the reporter contract promises single-threaded use and skips
    // synchronisation on the strength of it.
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
    val reportingThread = new AtomicReference[Thread](null)
    val pool = ThreadUtils.newDaemonSingleThreadExecutor("streaming-shuffle-reader-suite-io")
    try {
      awaitJavaFuture(pool.submit(new Callable[Unit] {
        override def call(): Unit = {
          reportingThread.set(Thread.currentThread())
          stream.handler.exceptionCaught(injected, stream.client)
        }
      }))
    } finally {
      pool.shutdownNow()
    }
    assert(reportingThread.get() != null && reportingThread.get() != Thread.currentThread(),
      "The failure must be injected from a thread other than the one driving the read, or the " +
        "assertion below proves nothing about the bridge")

    // Without the bridge this iterator would wait for input that can never arrive and the task
    // would hang, which is the worst available outcome. With it the failure is raised here, on the
    // task thread, from inside the iterator read() returned.
    val failure = intercept[FetchFailedException] {
      withTaskContext(fixture.context) {
        records.hasNext
      }
    }
    val diagnosis: Seq[Throwable] =
      Option(failure.getCause).toSeq ++ failure.getSuppressed.toSeq
    assert(diagnosis.contains(injected),
      s"The failure the I/O thread observed must survive into the fetch failure, but the " +
        s"attached failures were ${diagnosis.map(_.getMessage).mkString("[", ", ", "]")}")
    assert(fixture.context.fetchFailed.isDefined,
      "The fetch failure must be asserted on the task context from the task thread, because that " +
        "is what the executor consults when it chooses between FetchFailed and ExceptionFailure")
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

    // The JDK refuses to suppress a throwable by itself, so a notifier that recorded the same
    // instance twice would throw from its own reporting path if it did not guard the case.
    notifier.setError(first)
    assert(notifier.error.contains(first),
      "Recording the held failure again must be harmless and must not displace it")

    // The original type is preserved, so a matcher downstream still sees what was thrown.
    val rethrown = intercept[IllegalStateException] {
      notifier.throwIfError()
    }
    assert(rethrown eq first,
      "throwIfError must re-throw the very instance it latched, not a copy or a wrapper")

    notifier.reset()
    assert(!notifier.hasError && notifier.error.isEmpty,
      "A reset notifier must hold no failure, which is what lets one fixture serve two assertions")

    // A fatal condition is never displaced or downgraded by a recoverable one.
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

    // Asserted by absence. TCP-level backpressure lives in the client handler, which is the only
    // place in the subsystem that touches a channel's autoRead, and the reader holds no channel to
    // touch: it is handed a transport client and closes it, nothing more.
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

    // The manager carries both shapes, and the difference between them is the whole point. The
    // seven-argument shape is the manager's own implementation. The five-argument shape is only the
    // mixin forwarder the compiler emits for the trait's final method: it is itself final and its
    // body does nothing but delegate straight back to the trait, so the manager could not have
    // overridden it even had it tried.
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
    // Both halves of the checksum condition come from the real primitive rather than from invented
    // literals: the value the producer would have sent for the intact payload, and the value this
    // consumer computes once a byte has been flipped. That is what makes all four placeholders of
    // the condition genuinely populatable, so both are asserted.
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

  test("a streaming reader may only be constructed for a whole map range with streaming enabled") {
    startContext()
    val fixture = new ReaderFixture()

    // Tier two of the activation model. A reader must never exist while the gate is closed: the
    // manager delegates every call to the sort-based manager it holds internally instead.
    val gatedOff = intercept[IllegalArgumentException] {
      fixture.newReader(gatedOffStreamingConf(), FullRangeStart, FullRangeEnd)
    }
    assert(gatedOff.getMessage.contains("spark.shuffle.streaming.enabled"),
      s"The refusal must name the gate that is closed, but it read: ${gatedOff.getMessage}")

    // A live producer location names a map id and a task attempt id but no map index, so a narrowed
    // range cannot be honoured and the manager must delegate it to the sort-based reader.
    val narrowed = intercept[IllegalArgumentException] {
      fixture.newReader(fixture.conf, FullRangeStart + 1, FullRangeEnd)
    }
    assert(narrowed.getMessage.contains("whole map range"),
      s"The refusal must explain that streaming serves whole map ranges only, but it read: " +
        s"${narrowed.getMessage}")
    assert(!StreamingShuffleReader.servesFullMapRange(FullRangeStart + 1, FullRangeEnd),
      "A range that does not start at the first map index is not the whole map range")
    assert(StreamingShuffleReader.servesFullMapRange(FullRangeStart, FullRangeEnd),
      "The whole map range is the only range a streaming read serves")
  }

}
