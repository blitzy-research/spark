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
import java.util.concurrent.{Callable, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import _root_.io.netty.channel.{ChannelFuture, ChannelFutureListener, DefaultChannelId}
import _root_.io.netty.channel.embedded.EmbeddedChannel

import org.apache.spark.{FetchFailed, LocalSparkContext, ShuffleDependency, SparkConf, SparkContext, SparkException, SparkFunSuite, SparkThrowable, TaskContext, TaskContextImpl}
import org.apache.spark.internal.config.{SHUFFLE_COMPRESS, SHUFFLE_STREAMING_ENABLED}
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient, TransportResponseHandler}
import org.apache.spark.network.protocol.OneWayMessage
import org.apache.spark.network.server.TransportRequestHandler
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, DataBlockMessage, HeartbeatMessage, RetransmitRequestMessage, StreamingShuffleChecksum, StreamingShuffleMessage, StreamingShuffleMessageType, StreamTerminationMessage}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcTimeout}
import org.apache.spark.shuffle.{FetchFailedException, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.{Clock, ManualClock, SystemClock, TaskCompletionListenerException, ThreadUtils}

/**
 * One consumer channel a test has opened to a streaming shuffle producer.
 *
 * The reader reaches its producers through [[StreamingShuffleProducerConnector]], which is abstract
 * precisely so a suite can drive producer loss, corruption repair, sequence violations and orderly
 * completion with no port and no timing. This class is the other side of that seam: it holds the
 * handler the reader built, the channel the frames travel on and the transport client the reader
 * was handed, so a test can push an inbound frame in and read the consumer's outbound frames out.
 *
 * An `EmbeddedChannel` rather than a mock, because the consumer's acknowledgements, heartbeats and
 * retransmission requests are all written through `Channel.writeAndFlush` and an embedded channel
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

  private val crashed = new AtomicBoolean(false)

  /**
   * The transport's own dispatcher for an inbound frame, which is what owns the frame's buffer.
   *
   * A streaming frame reaches a consumer as a one-way message on the channel the consumer opened,
   * and every channel the transport builds carries a request handler as well as a response handler
   * precisely so that a peer-initiated message has somewhere to go. That handler is therefore the
   * real seam: it takes the frame's `ManagedBuffer`, hands the consumer a `ByteBuffer` view of it,
   * and releases the buffer in a `finally` whether the consumer succeeded or not.
   *
   * Frames are pushed through it rather than straight into `handler.receive`, because the ownership
   * contract this suite asserts on -- that no byte the transport owns outlives the call that
   * delivered it -- is a contract between the transport and the consumer. A fixture that performed
   * the hand-off itself would be asserting against its own behaviour rather than production's.
   *
   * The chunk-fetch handler is deliberately absent: it is dereferenced only for a chunk-fetch
   * request, and this protocol never sends one, so supplying one would add a collaborator no frame
   * in this suite can reach. The chunk limit is likewise irrelevant and is set out of the way.
   */
  private val transportDispatcher: TransportRequestHandler = new TransportRequestHandler(
    channel, client, handler, java.lang.Long.valueOf(Long.MaxValue), null)

  /**
   * Delivers one frame to the consumer exactly as the transport would.
   *
   * The bytes are handed over inside a counting managed buffer, through the transport's own request
   * dispatcher, so that a suite can prove the consumer never takes ownership of the transport's
   * buffer: a streaming block's payload is copied out of the frame at decode time, so nothing the
   * transport owns may outlive the call.
   *
   * '''Two guarantees are enforced on every delivery''', not only in the case that audits them, so
   * that any test in this suite fails the moment the consumer starts holding on to a frame.
   *
   *  - The transport released the buffer exactly once and the consumer retained it never, which is
   *    the reference contract for a one-way message body.
   *  - The frame's backing bytes are overwritten once the call returns. A consumer that kept the
   *    `ByteBuffer` instead of copying out of it would then read the scribble rather than the
   *    payload, and its records -- or its checksum -- would not survive. Nothing here asserts that
   *    directly; every assertion in the suite that reads records back is what does.
   *
   * @param message the frame to deliver
   * @return the counting buffer the frame was delivered in
   */
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
   * This is the only way to reach the consumer's pre-decode path. Every other delivery goes through
   * a `StreamingShuffleMessage`, whose constructors validate the header, the payload size and the
   * body domains -- so a suite that used them exclusively would prove only that the consumer
   * handles frames it could not have received. A hostile peer is not obliged to use those
   * constructors, and the frames it sends are exactly the ones this method sends.
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
    buffer
  }

  /**
   * Delivers one frame over a channel this handler was never bound to.
   *
   * A handler is created per producer generation and bound to exactly one authenticated socket, so
   * the channel is the capability: a frame arriving on any other channel must be refused before its
   * body is looked at, however well-formed that body is. Driving it needs a second, foreign
   * channel, which is what this creates -- and the frame is a valid one on purpose, so that a
   * refusal can only be attributed to the binding and never to the content.
   *
   * The foreign channel is given a channel id of its own, and it has to be: Netty's
   * `EmbeddedChannel` shares one singleton id across every instance, so two embedded channels are
   * indistinguishable to the identity check under test -- and a second channel that the production
   * code cannot tell from the first would make this case assert nothing. A real socket always
   * carries a distinct id, which is what `DefaultChannelId.newInstance()` reproduces.
   *
   * @param message the frame to deliver, which is deliberately well-formed
   * @return the foreign client the frame was delivered through
   */
  def deliverFromForeignChannel(message: StreamingShuffleMessage): TransportClient = {
    val foreignChannel = new EmbeddedChannel(DefaultChannelId.newInstance())
    val foreignClient = new TransportClient(foreignChannel, new TransportResponseHandler(
      foreignChannel))
    val framed = message.toByteBuffer()
    val bytes = new Array[Byte](framed.remaining())
    framed.duplicate().get(bytes)
    handler.receive(foreignClient, ByteBuffer.wrap(bytes))
    foreignChannel.finishAndReleaseAll()
    foreignClient
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

  /**
   * Every frame the consumer has written since this was last called, as the bytes that would have
   * gone on the wire.
   *
   * Decoding is deliberately skipped here, because the question this answers is a byte-level one:
   * whether anything at all that the consumer wrote carries material it must not carry. A decoded
   * view could only inspect the fields the protocol declares, and a credential that leaked into any
   * of them -- or into padding -- would still be on the wire.
   *
   * @return the encoded frames, oldest first
   */
  def encodedOutboundFrames: Seq[Array[Byte]] = {
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

  /** Closes the channel under the consumer, which is what a producer going away looks like. */
  def closeChannel(): Unit = {
    channel.close().syncUninterruptibly()
  }

  /**
   * Loses the executor behind this channel, part way through its write.
   *
   * What a consumer can observe of a producer crash is silence: no further block, no orderly end of
   * stream, and a last-inbound timestamp that stops advancing, so the connection timeout is the
   * only thing that can discover it. Marking the crash rather than tearing the channel down is what
   * makes that observable state reachable -- and enforcing it in [[deliver]] is what stops a case
   * from crashing a producer and then, a few lines later, contradicting itself by sending from it.
   */
  def crash(): Unit = crashed.set(true)

  /** Whether the executor behind this channel has been lost. */
  def isCrashed: Boolean = crashed.get()
}

private[streaming] object StreamingShuffleTestProducerStream {

  /**
   * The byte a delivered frame's backing array is overwritten with once the transport has released
   * it.
   *
   * Any value would do so long as it is not one the payloads contain, and this one is chosen to be
   * conspicuous in a hex dump if it ever does reach a decoder. Overwriting is the point: it turns
   * "the consumer must copy out of the frame" from a claim in a comment into a property every
   * record-reading assertion in this suite depends on.
   */
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

  def streams: Seq[StreamingShuffleTestProducerStream] = synchronized(opened.toSeq)

  def onlyStream: StreamingShuffleTestProducerStream = {
    val open = streams
    assert(open.size == 1,
      s"Expected exactly one producer channel to have been opened but found ${open.size}")
    open.head
  }

  def connectAttemptCount: Int = attempts.get()

  def closeCallCount: Int = closeCalls.get()
}

/**
 * The production connector with its socket creation replaced by an embedded channel and a barrier.
 *
 * The ownership races [[NettyStreamingShuffleProducerConnector]] defends against all live in the
 * interval between creating a channel and publishing the handler that owns it. That interval is a
 * socket connect and, when the application is authenticated, a handshake -- so it cannot be *held
 * open* by a test that goes through a real socket, and an interleaving nobody can hold open is an
 * interleaving nobody can prove. Substituting the one seam the production class exposes for exactly
 * this purpose lets a test stand inside that interval and run the other side of the race.
 *
 * Everything else is the production class: the claim registration, the closed checks, the binding,
 * the early-callback replay, the declining path and the whole of `close`. What changes is only
 * where the channel comes from.
 *
 * @param conf the executor configuration the streaming transport namespace is read from
 */
private[streaming] class BarrieredStreamingShuffleProducerConnector(conf: SparkConf)
  extends NettyStreamingShuffleProducerConnector(conf) {

  private val created = new java.util.concurrent.ConcurrentLinkedQueue[TransportClient]()

  private val insideCreation = new AtomicReference[() => Unit](() => ())

  /**
   * Installs an action to run while a channel is being created, before its owner is published.
   *
   * @param action what to run inside the creation interval
   */
  def duringCreation(action: () => Unit): Unit = insideCreation.set(action)

  /** Every channel this connector created, in creation order. */
  def createdClients: Seq[TransportClient] = created.asScala.toSeq

  override protected def createTransportClient(host: String, port: Int): TransportClient = {
    // A channel id of its own per connection, because the connector keys its registries by channel
    // identity and Netty's `EmbeddedChannel` shares one singleton id across every instance. Without
    // this, two connections would collide on one key and the ownership assertions below would be
    // asserting against a registry that had merged them.
    val channel = new EmbeddedChannel(DefaultChannelId.newInstance())
    val client = new TransportClient(channel, new TransportResponseHandler(channel))
    created.add(client)
    insideCreation.get()()
    client
  }
}

/**
 * A driver coordinator reference that answers a consumer's rendezvous synchronously.
 *
 * The reader resolves live producers with `askSync` against the driver's coordinator endpoint. A
 * reference of our own answers in the calling thread, which removes the dispatcher, the endpoint
 * registry and the reaper timer from every test here and lets a test observe the exact state of the
 * reader at the moment it asks, which is what makes the ordering of the producer-failure sequence
 * assertable rather than merely plausible.
 *
 * @param conf configuration the base class derives its default ask deadline from
 * @param clock the same clock the reader reads, advanced on each lookup by `lookupAdvanceMillis`
 * @param lookupAdvanceMillis how far to advance the clock inside a lookup, which is how a suite
 *                            makes the rendezvous wait a positive, deterministic number of
 *                            milliseconds so fetch-wait accounting can be asserted on
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

  def answerLookupWith(reply: Option[StreamingShuffleProducerLocations]): Unit = {
    locations.set(reply)
  }

  /**
   * The answer a lookup would receive right now.
   *
   * Exposed so a test can assert on the state the coordinator is publishing at a chosen instant --
   * in particular whether the map stage is still producing -- rather than on a copy of the reply it
   * built earlier, which would only be asserting against itself.
   */
  def currentAnswer: Option[StreamingShuffleProducerLocations] = locations.get()

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
 * Release order is not observable from the end state -- a teardown that frees everything in the
 * wrong order leaves exactly the same end state as one that frees it in the right order. What
 * distinguishes them is what was still held when each step ran, so every recorded step carries this
 * snapshot of the OTHER three resources and the order is read off the snapshots.
 *
 * @param queuedEvents frames still sitting in the consumer handler's hand-off queue
 * @param reservedQuotaBytes bytes of the executor's shared receive budget still charged
 * @param channelOpen whether the consumer's channel to the producer was still open
 * @param ledgerRegistered whether the consumer's credit ledger was still registered
 */
private[streaming] final case class StreamingShuffleCleanupSnapshot(
    queuedEvents: Int,
    reservedQuotaBytes: Long,
    channelOpen: Boolean,
    ledgerRegistered: Boolean)

/**
 * The ordered log of a reader's cleanup, recorded from inside the production calls that perform it.
 *
 * ==Why a recorder and not an end-state assertion==
 *
 * The reader acquires in one order -- credit ledger, then channel, then receive quota per admitted
 * block, then the buffers those blocks sit in -- and its release path claims to unwind that in
 * reverse. Every existing cleanup assertion in this suite checks the end state, which the wrong
 * order satisfies just as well as the right one. This records each release as it happens, together
 * with a snapshot of everything not yet released, so a reordering of the production steps changes
 * the log and fails the assertion.
 *
 * ==Why it is armed rather than always recording==
 *
 * Two of the three steps observed here also occur during ordinary reading: receive quota comes back
 * every time the reduce task consumes a block. Recording from construction would bury the teardown
 * in the traffic that preceded it, so a test arms the recorder at the moment it triggers task
 * completion and the log then contains the teardown and nothing else.
 */
private[streaming] class StreamingShuffleCleanupRecorder {

  private val events =
    mutable.ArrayBuffer.empty[(String, Long, StreamingShuffleCleanupSnapshot)]

  @volatile private var armed: Boolean = false

  @volatile private var probe: () => StreamingShuffleCleanupSnapshot =
    () => StreamingShuffleCleanupSnapshot(0, 0L, channelOpen = false, ledgerRegistered = false)

  /**
   * Installs the function that reads the state of everything a snapshot describes.
   *
   * Installed after construction rather than passed in, because the reader has to exist before its
   * channel and its handler do, and this recorder has to exist before the reader does -- the
   * backpressure protocol it observes is one of the reader's own constructor arguments.
   *
   * @param observation reads the current state of the four resources
   */
  def observeWith(observation: () => StreamingShuffleCleanupSnapshot): Unit = {
    probe = observation
  }

  /** Starts recording. Called immediately before the teardown a test means to observe. */
  def arm(): Unit = {
    armed = true
  }

  /**
   * Records one release, with a snapshot of what had not been released yet.
   *
   * @param name what was released
   * @param bytes how much of it, where the quantity is meaningful
   */
  def record(name: String, bytes: Long): Unit = {
    if (armed) {
      val snapshot = probe()
      synchronized {
        events += ((name, bytes, snapshot))
      }
    }
  }

  /** Every release recorded since the recorder was armed, in the order it happened. */
  def recorded: Seq[(String, Long, StreamingShuffleCleanupSnapshot)] = synchronized(events.toSeq)

  /** Just the names, which is the sequence an order assertion is stated against. */
  def releaseOrder: Seq[String] = recorded.map(_._1)

  /** The snapshot taken when one named release ran, if it ran. */
  def snapshotOf(name: String): Option[StreamingShuffleCleanupSnapshot] =
    recorded.find(_._1 == name).map(_._3)

  /** The quantity one named release returned, if it ran. */
  def bytesOf(name: String): Option[Long] = recorded.find(_._1 == name).map(_._2)
}

private[streaming] object StreamingShuffleCleanupRecorder {

  /** The consumer handler returned the executor's shared receive budget. */
  val ReceiveQuotaReturned: String = "receive-quota-returned"

  /** The consumer's channel to the producer was closed. */
  val ChannelClosed: String = "channel-closed"

  /** The consumer's credit ledger was released from the executor-wide protocol. */
  val CreditLedgerReleased: String = "credit-ledger-released"
}

/**
 * A backpressure protocol reporting its own release calls to a cleanup recorder.
 *
 * Subclassed rather than mocked, because what has to be observed is the moment the real accounting
 * moves: both methods overridden here are called by production cleanup code and both delegate to
 * the real implementation, so the protocol under observation behaves exactly as the one under test.
 * Recording after the delegation is deliberate -- the snapshot then describes the resources this
 * call did not touch.
 *
 * @param conf configuration the protocol reads its own entries from
 * @param coordinator the executor's coordinator, consulted for the concurrent shuffle count
 * @param egressBudget the executor's egress allowance
 * @param clock the time source every window is measured on
 * @param recorder the log this protocol reports to
 */
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


/**
 * Tests for [[StreamingShuffleReader]], the consumer half of a streaming shuffle.
 *
 * The four cases the feature names each have a test of their own: an in-progress block request
 * consumed partially, producer-failure detection on the five-second connection timeout,
 * partial-read invalidation with the upstream recomputation it triggers, and checksum validation
 * with the retransmission it provokes. Around them sit the invariants that make those four safe --
 * that the reader declares no `stop()` and therefore releases everything through the
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

  private val NumPartitions = DefaultPartitionCount

  /**
   * The one reduce partition each fixture reads. Deliberately neither the first nor the last, so
   * that a partition-range confusion shows up as a failure rather than passing by coincidence.
   */
  private val ReducePartition = 3

  private val DeclaredMaps = 1

  /**
   * Map tasks a case declares when it needs more than one producer feeding one reduce partition.
   *
   * Two, because the properties that only exist above a single producer -- an invalidation reaching
   * exactly one generation, a peer's accepted output surviving it, several losses reported as one
   * fetch failure -- are all fully expressed by two, and a third would add reruns without adding a
   * distinction.
   */
  private val TwoProducers = 2

  private val ProducerMapId = 0L

  private val ProducerAttemptId = 200L

  private val ConsumerAttemptId = 41L

  private val CoordinatorEpoch = 9L

  private val CapabilityToken = "streaming-shuffle-reader-suite-token"

  private val StreamedRecords: Seq[(Int, Int)] = (1 to 24).map(value => (value, value * 3))

  /**
   * The records a second producer streams for the same reduce partition.
   *
   * Disjoint from [[StreamedRecords]] in both key and value, so that "the peer's output survived"
   * cannot be satisfied by bytes belonging to the producer whose output was supposed to be
   * discarded. A different length as well, so a comparison cannot pass on shape alone.
   */
  private val PeerRecords: Seq[(Int, Int)] = (101 to 118).map(value => (value, value * 7))

  private val BlockCount = 4

  private val BlocksPastHighWaterMark = StreamingShuffleClientHandler.INBOUND_QUEUE_HIGH_WATER_MARK

  private val LookupWaitMillis = 7L

  private val FullRangeStart = 0

  private val FullRangeEnd = Int.MaxValue

  private val ChecksumVerifyFailedCondition = "STREAMING_SHUFFLE_CHECKSUM_VERIFY_FAILED"

  private val StreamingShuffleSqlState = "XXKST"

  /** The condition a message that may only ever be sent by a consumer is refused with. */
  private val UnexpectedMessageTypeCondition = "STREAMING_SHUFFLE_UNEXPECTED_MESSAGE_TYPE"

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
   * @param numMaps map tasks the stage declared, which is also how many producer generations the
   *                rendezvous offers this consumer. One is the shape most cases here want; a case
   *                about an invalidation reaching exactly one of several producers declares more,
   *                because the property it asserts does not exist below two
   * @param refusalsBeforeSuccess connection attempts the connector refuses before it succeeds
   * @param lookupAdvanceMillis milliseconds the clock advances inside each rendezvous lookup
   * @param useTaskReporter whether the reader reports to the task's own shuffle-read metrics rather
   *                        than to a recording reporter, which is how a suite proves the streaming
   *                        figures land on Spark's existing accumulators
   * @param cleanupRecorder the log every release this reader performs is reported to, absent unless
   *                        a case is asserting the order those releases happen in
   * @param completedMaps map indexes the coordinator reports as having streamed their output to
   *                      completion. It defaults to the fixture's own producer, which is the state
   *                      of a shuffle whose map task has finished, because that is what most cases
   *                      here are about; a case about reading output that is still being produced
   *                      passes an incomplete set instead, and the reply then reports
   *                      `mapOutputsPending`
   * @param realTimeReaderClock whether the reader and the handlers it builds read wall-clock time
   *                            rather than this fixture's manual clock. Needed by exactly one kind
   *                            of case: the connection-retry ladder pauses on a real latch with a
   *                            real millisecond timeout, so a manual clock reads the same value
   *                            either side of a pause and the reader charges nothing to fetch wait.
   *                            A real clock is what makes "the whole wait is charged to fetch wait
   *                            time" assertable, at the cost of the pause being real time
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
      realTimeReaderClock: Boolean = false) {

    val conf: SparkConf = streamingConf()
    val clock: ManualClock = newManualClock()

    /**
     * The clock the reader under test -- and every client handler it builds -- reads.
     *
     * It is the manual clock in every case but the retry-ladder ones, so the rest of the suite
     * keeps its "a timeout is one method call" determinism, and the collaborators the fixture
     * shares with the reader keep reading the manual clock in every case.
     */
    val readerClock: Clock = if (realTimeReaderClock) new SystemClock else clock
    val dependency: ShuffleDependency[Int, Int, Int] =
      shuffleDependencyFor(sc, conf, numPartitions, numRecords = StreamedRecords.size)
    val shuffleId: Int = dependency.shuffleId
    val handle: StreamingShuffleHandle[Int, Int, Int] = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId, dependency, numPartitions, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
      CoordinatorEpoch, CapabilityToken)
    val coordinator: StreamingShuffleCoordinator =
      new StreamingShuffleCoordinator(sc.env.rpcEnv, conf, clock)
    // Observed only when a case asked to observe it. The observing subclass delegates every call
    // to the real implementation, so the reader under test is driven by the same accounting either
    // way; what changes is whether the release calls are logged as they happen.
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
    /**
     * Every producer generation the coordinator offers this consumer, in map-index order.
     *
     * One per declared map task, because that is what a reduce task genuinely faces: a partition's
     * input is the concatenation of one stream per map output, and the properties that only exist
     * when there is more than one of them -- the credit allowance being split between them, and an
     * invalidation reaching exactly one of them -- are unreachable from a single-producer fixture.
     * A fixture that declares one map therefore offers one producer, which is what the majority of
     * cases here want, and `numMaps` is the one dial that changes it.
     */
    val producers: Seq[StreamingShuffleProducerLocation] = {
      require(numMaps > 0,
        s"A reader fixture must declare at least one map task but declared $numMaps")
      (0 until numMaps).map(producerLocation)
    }

    /**
     * The first producer in read order, which is the only one a single-producer case has.
     *
     * Read order is map-index order, so this is also the producer a multi-producer case reaches
     * first -- the distinction that decides whether a peer's blocks have been consumed or are still
     * queued when a loss is reported.
     */
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
     * @param completedMapIndexes map indexes whose output a producer has streamed to completion.
     *                            This is the field the reply's `mapStageComplete` and
     *                            `mapOutputsPending` are computed from, so it -- and not the
     *                            producer list -- is what says whether the map stage is still
     *                            producing. It is never derived from `producers`, because a
     *                            registered producer is precisely one that may still be producing
     * @return the reply
     */
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
     * @param mapId the producing map task, which is part of the block identity the payload is
     *              wrapped for. Defaulted to the fixture's first producer, and named explicitly by
     *              a multi-producer case so that each producer's bytes are wrapped for its own
     *              block rather than all of them borrowing one producer's identity
     * @return the wrapped bytes
     */
    def encodePartition(
        records: Seq[(Int, Int)],
        mapId: Long = ProducerMapId): Array[Byte] = {
      val bytes = new ByteArrayOutputStream()
      val wrapped = sc.env.serializerManager.wrapStream(
        ShuffleBlockId(shuffleId, mapId, partitionId), bytes)
      val serialized = dependency.serializer.newInstance().serializeStream(wrapped)
      records.foreach { record =>
        serialized.writeKey(record._1)
        serialized.writeValue(record._2)
      }
      serialized.close()
      bytes.toByteArray
    }

    /**
     * Cuts one producer's partition payload into the run of blocks it would put on the wire.
     *
     * @param payload the wrapped partition bytes
     * @param blocks how many blocks to cut the payload into
     * @param mapId the producing map task each block is stamped with, and therefore the identity
     *              its CRC32C binds it to. A block stamped with the wrong map id is refused by the
     *              consumer handler bound to that producer, so this has to travel with the payload
     * @return the blocks, in sequence order
     */
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

    /**
     * The credit ledgers this reader holds for one producer generation.
     *
     * A ledger's identity includes the producing generation, so a multi-producer read holds one per
     * producer per partition. Selecting by map id is what lets a case assert that a peer's
     * allowance was left alone while a lost producer's was released -- an assertion
     * [[consumerLedgerKey]] cannot make, because it matches on the partition alone and every
     * producer of this fixture feeds the same one.
     *
     * @param mapId the producing map task whose ledgers to select
     * @return the registered consumer ledgers of that producer
     */
    def consumerLedgerKeysOf(mapId: Long): Seq[BackpressureStreamKey] =
      backpressure.registeredStreams.filter(key =>
        key.partitionId == partitionId && key.mapId == mapId)

    /**
     * The open channel to one producer, identified by the map index it produces.
     *
     * Channels are opened in map-index order, but selecting by position would make every assertion
     * depend on that order holding; selecting by the producer the channel actually reaches states
     * what the assertion means and fails loudly if the reader ever opened none.
     *
     * @param mapIndex map index of the producer whose channel is wanted
     * @return the channel to that producer
     */
    def streamOf(mapIndex: Int): StreamingShuffleTestProducerStream = {
      val open = connector.streams.filter(_.location.mapIndex == mapIndex)
      assert(open.size == 1,
        s"Expected exactly one open channel to the producer of map index $mapIndex but found " +
          s"${open.size} among ${connector.streams.map(_.location.mapIndex).mkString(", ")}")
      open.head
    }

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
        endPartition, context, readMetrics, readerConf, readerContext, readerClock)
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
    sc = new SparkContext("local", "test", testConf)
  }

  private def expectedRecords: Set[(Int, Int)] = StreamedRecords.toSet

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
  /**
   * Whether a byte sequence occurs anywhere inside another, which is how a credential audit is
   * done: the question is whether the bytes are present at all, not whether some field holds them.
   *
   * @param haystack the bytes to search
   * @param needle the bytes that must not be found
   * @return true if `needle` occurs in `haystack`
   */
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

  /**
   * Reports a transport failure to a consumer handler from a thread that is not driving the read.
   *
   * The bridge under test exists precisely because the two threads differ: a failure observed on
   * a Netty event loop has to be re-thrown on the task thread or the task waits for input that
   * can never arrive. Injecting from this thread would prove nothing about it, so a single-use pool
   * supplies another one and the caller waits for the report to have happened before it advances.
   *
   * @param stream the consumer channel to report the failure on
   * @param failure the throwable an I/O thread observed
   */
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
    } finally {
      pool.shutdownNow()
    }
    val reporter = reportingThread.get()
    assert(reporter != null && reporter != Thread.currentThread(),
      "The failure must be reported from a thread other than the one driving the read, or an " +
        "assertion about the bridge between the two proves nothing")
  }

  /**
   * Everything a fetch failure carries about what caused it.
   *
   * A cause and a suppressed throwable are both used, and which one carries the root cause depends
   * on which failure won the notifier's first-error race, so an assertion that looked at one alone
   * would be asserting the race's outcome rather than the diagnosis.
   *
   * @param failure the fetch failure to open up
   * @return its cause and its suppressed failures
   */
  private def diagnosisOf(failure: FetchFailedException): Seq[Throwable] =
    Option(failure.getCause).toSeq ++ failure.getSuppressed.toSeq

  /**
   * The records still readable from what one producer's handler is holding.
   *
   * This is the strongest available statement of "this producer's accepted data survived". It does
   * not ask the handler whether it still holds bytes; it turns those bytes back into the
   * records the producer sent, through `SerializerManager.wrapStream` for the same
   * `ShuffleBlockId` the reader itself would have used, and re-verifies every block's CRC32C on
   * the way. A block whose payload had been released, truncated or half discarded by another
   * producer's invalidation could not survive that round trip -- so a case that recovers the
   * exact records has proved the invalidation did not reach into this producer, rather than
   * merely proved a counter did not move.
   *
   * Polling drains the hand-off queue, so a case that also asserts on the queue's depth or byte
   * accounting must read those before calling this.
   *
   * @param fixture the read whose producer channel is being examined
   * @param mapIndex map index of the producer to read
   * @return the records, in the order the producer sent them
   */
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
   * The injector owns the decision and the arithmetic: [[
   * StreamingShuffleFaultInjector.crashProducersConcurrently]] arms exactly as many triggers as
   * there are producers to lose, and each seam consumes one, so the count of producers that
   * actually went away is a fact the injector reports rather than one this suite asserts about
   * itself. That is what makes "multiple concurrent producer failures" a scenario driven by the
   * feature's own fault-injection vocabulary instead of by an ad-hoc flag.
   *
   * @param injector the fault injector sharing the fixture's clock
   * @param streams the producer channels to lose, all of them
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
    // nothing has yet declared that output finished. Every record consumed below is therefore
    // consumed from a map stage the coordinator itself says is still producing, which is what "in
    // progress" claims and what a fixture that reported every offered producer complete could not
    // have shown.
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

    // Only part of the partition is put on the wire. The producer has neither sent its last block
    // nor announced a total, so the stream is mid-flight in the strongest sense available here:
    // bytes this reduce task will eventually need do not exist on the consumer side yet.
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

    // Only now does the producer finish: it sends its last block, announces the total, and the
    // coordinator records the map index as complete. Every record consumed above was consumed
    // before any of that -- which is the whole of what "in progress" claims.
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
    // Two refusals and then a channel. The producer is not gone in this case: its executor was
    // momentarily out of accept capacity, or its serving handler was part-way through registration,
    // which is precisely what the ladder exists for -- escalating the first refusal would recompute
    // a map task whose output is intact and milliseconds away from being servable.
    //
    // The clock is a real one here, and only here, because the pause is a real bounded wait on a
    // latch: under a manual clock the reader would read the same millisecond either side of it and
    // charge nothing to fetch wait, so the accounting could not be asserted at all.
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

  test("a producer that refuses every attempt is invalidated once the budget is spent") {
    startContext()
    // Every attempt is refused, so the budget is spent and the refusal means what it says: the peer
    // is unreachable. Setting the refusals to exactly the budget is what makes the count assertable
    // in both directions -- a sixth attempt would have succeeded and opened a channel, so a reader
    // that overran its budget could not reach the assertions below.
    val fixture = new ReaderFixture(
      refusalsBeforeSuccess = MaxRetryAttempts, realTimeReaderClock = true)
    // Four pauses for five attempts: the ladder paces the gaps between attempts, and there is no
    // gap after the last one.
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

    // Recovery is the classic path's: the tracker removes this one dead map output and the
    // unmodified scheduler recomputes this one map task.
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

    // The generation is withdrawn as well as the map output, so a consumer that retries cannot be
    // handed the same unreachable endpoint again.
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
    assert(observedPartialReadInvalidations() == 0L,
      s"Nothing was accepted from a producer that never answered, so no partial read may be " +
        s"counted as invalidated, but ${observedPartialReadInvalidations()} was")

    // Exhaustion is a failure like any other, so the task-completion listener must still release
    // everything the read claimed before it failed.
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

    // And the terminus: the unmodified scheduler recomputes the upstream stage because the task
    // context now carries a fetch failure, which is the only signal this feature is allowed to use.
    assert(fixture.context.fetchFailed.isDefined,
      "The task context must carry the fetch failure, which is what makes the executor report a " +
        "FetchFailed reason and the unmodified DAG scheduler resubmit the upstream stage")
    assert(fixture.connector.closeCallCount == 0,
      s"A reader must close only the channels it opened and never the executor-scoped connector, " +
        s"but the connector was closed ${fixture.connector.closeCallCount} time(s)")
  }

  test("a lost producer invalidates only its own partial read and leaves a peer's blocks intact") {
    startContext()
    // Two producers feeding one reduce partition, which is the shape the isolation property needs:
    // a reduce task's input is the concatenation of one stream per map output, and "invalidation is
    // atomic AND per producer" has no content below two of them. Both map indexes are reported
    // complete, so this is a read of retained output -- the same state as the single-producer
    // invalidation case above -- and the only difference under test is the producer count.
    val fixture = new ReaderFixture(numMaps = TwoProducers, completedMaps = Set(0, 1))
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
    // A payload of its own for the peer, so that recovering the peer's records later cannot be
    // satisfied by bytes that came from the producer whose data was supposed to be discarded.
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

    // The producer that is about to be lost delivers its whole partition and then goes silent: no
    // orderly end of stream, so there is a genuine partial read to invalidate.
    lostBlocks.foreach(block => lostStream.deliver(block))
    assert(lostStream.handler.acceptedBlockCount(fixture.partitionId) == lostBlocks.size.toLong,
      s"Every block of the producer about to be lost must have been accepted, but only " +
        s"${lostStream.handler.acceptedBlockCount(fixture.partitionId)} of ${lostBlocks.size} were")

    // The clock advances past the connection timeout FIRST, and only then does the peer deliver.
    // That ordering is what makes exactly one producer silent: liveness is measured per channel
    // from that channel's own last inbound frame, so the peer's delivery resets its own clock
    // reading to zero while the lost producer's stays at the full timeout.
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

    // Observed at the one instant that can see the discard and the peer together: while the
    // invalidation ask is in flight, which is after the lost producer has been emptied and before
    // the fetch failure has been thrown.
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

    // The discard reached the lost producer completely...
    assert(lostQueuedAtInvalidation.get().contains(0),
      s"No block from the lost producer may remain reachable when the loss is reported, but " +
        s"${lostQueuedAtInvalidation.get()} event(s) were still queued")
    assert(lostClosedAtInvalidation.get().contains(true),
      s"The lost producer's channel must be shut before the loss is reported, but its state at " +
        s"that moment was ${lostClosedAtInvalidation.get()}")

    // ...and stopped exactly there. This is the property the whole case exists for: an invalidation
    // is scoped to the producer that was lost, so a peer that is streaming perfectly well does not
    // have its accepted output thrown away because a sibling map task died.
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

    // Counted once, for the one producer that was lost -- not once per producer of the shuffle.
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

    // The fetch failure names the lost producer and nothing else, which is what makes
    // MapOutputTracker remove that one dead map output rather than the peer's live one.
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

    // And the decisive form of the statement: the peer's accepted blocks are not merely still
    // counted, they still deserialize -- through the same wrapping the reader uses -- into exactly
    // the records the peer sent, every one of them still matching the checksum it arrived with.
    assert(readableRecordsOf(fixture, survivingProducer.mapIndex) == PeerRecords,
      "The peer's accepted blocks must still be readable in full after a sibling producer was " +
        "invalidated, or the invalidation was not per producer at all")
    assert(fixture.connector.closeCallCount == 0,
      s"A reader must close only the channels it opened and never the executor-scoped connector, " +
        s"but the connector was closed ${fixture.connector.closeCallCount} time(s)")
  }

  test("several producers lost together are one fetch failure counted exactly once") {
    startContext()
    // The feature's ninth enumerated failure scenario: multiple concurrent producer failures. Both
    // producers of the shuffle die at the same instant, each having already delivered part of its
    // output, so there are two partial reads outstanding and only one of them can be reported --
    // the reader reaches producers in map-index order and the first loss it meets ends the read.
    val fixture = new ReaderFixture(numMaps = TwoProducers, completedMaps = Set(0, 1))
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

    // Both producers go at once, and the injector is what decides so: it is armed for exactly the
    // number of producers offered and each seam consumes one trigger, so the two losses are one
    // event rather than two independent ones a test happened to stage in sequence.
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

    // One loss is reported, not two. A reduce task has exactly one input and exactly one failure to
    // report about it; the recomputation of the upstream stage that this failure triggers is what
    // restores every dead map output, including the second one, so reporting the second here would
    // inflate the telemetry without recovering anything the first does not already recover.
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

    // No byte of either partial read is still reachable through the producer the reader reached,
    // which is what "zero data loss" requires of the discard side: a recomputed producer's output
    // is never concatenated onto a survivor of the failure.
    assert(fixture.streamOf(firstProducer.mapIndex).handler.queuedEventCount == 0 &&
        fixture.streamOf(firstProducer.mapIndex).handler.queuedByteCount == 0L,
      s"The reported producer's partial read must be discarded in full, but " +
        s"${fixture.streamOf(firstProducer.mapIndex).handler.queuedEventCount} event(s) and " +
        s"${fixture.streamOf(firstProducer.mapIndex).handler.queuedByteCount} byte(s) remain")
    assert(fixture.streamOf(firstProducer.mapIndex).handler.isClosed,
      "The reported producer's channel must be shut, or a block arriving after the discard could " +
        "be mixed into the recomputed input")
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

    // A listener added to an already completed task is invoked immediately, and the reader's own
    // release is idempotent under a compare-and-set, so running the completion path a second time
    // releases nothing twice and throws nothing at all.
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
    // ledger still registered. Teardown asserted against a read that had already drained itself
    // would be asserting about nothing.
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

    // The one instant that can observe the order. An embedded channel completes its close future
    // synchronously, and the handler closes its own channel from inside its own close(), so this
    // listener runs while the reader's release is still part-way through.
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

    // Listeners are a stack, so one registered after the reader's own runs before it. This one
    // fails, which is the case a resource-safety test must not be able to pass through: the failure
    // has to be surfaced to whoever completed the task, and the reader's release has to run anyway.
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

    // And by the end, in spite of the broken listener, nothing survives.
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

    // The audit is this line as much as the counters below it. Every frame's backing array was
    // overwritten the instant the transport released it, so a consumer that had kept the
    // transport's `ByteBuffer` instead of copying out of it would be decoding a scribble by now --
    // and neither these records nor the checksums that guard them would have survived it. The read
    // succeeding IS the proof that the copy happens where the subsystem says it does.
    assert(readRecords(records) == expectedRecords,
      "The read must succeed over frames whose transport buffers have been overwritten, which is " +
        "only possible if the consumer copied each payload out at decode time")
    fixture.context.markTaskCompleted(None)

    // And the reference contract, observed on the transport's own hand-off rather than on the
    // fixture's. Each frame arrives as a one-way message body owned by the transport, which hands
    // the consumer a view of it and then releases it in a `finally`; a consumer that wanted the
    // bytes to outlive the call would have to take a reference, and none was taken.
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
    // The end state a completed task leaves is the same whichever order its resources were released
    // in, so the order cannot be read off it. This case records each release from inside the
    // production call that performs it, together with a snapshot of everything not yet released,
    // and asserts the sequence.
    //
    // Acquisition order, which is fixed by the reader and by the handler it builds: the credit
    // ledger is registered first, then the channel is opened, then receive quota is charged per
    // admitted block, and the buffers those blocks sit in are the last thing to exist. Unwinding in
    // reverse therefore means buffers, then quota, then channel, then ledger -- and it has to be
    // that way round rather than any other: quota returned after the channel closed would leave the
    // executor's shared budget short for as long as the close took, and a ledger released before
    // the channel closed would let a frame arriving in between be admitted against a ledger that
    // no longer exists.
    val recorder = new StreamingShuffleCleanupRecorder
    val fixture = new ReaderFixture(cleanupRecorder = Some(recorder))
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    // Deliberately reads nothing. A reader that consumed its whole input has released everything
    // already and has no order left to get wrong, so the observation is only available while blocks
    // are still queued and their bytes are still charged.
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

    // And the end state, which the existing cleanup assertions describe. Asserting both is the
    // point: the order is right AND nothing was left behind.
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
    injectFromIoThread(stream, injected)

    // Without the bridge this iterator would wait for input that can never arrive and the task
    // would hang, which is the worst available outcome. With it the failure is raised here, on the
    // task thread, from inside the iterator read() returned.
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
    // This case covers the first advance only, and the first advance is the one a channel failure
    // reaches by two independent routes -- the queued marker as well as the notifier. Every advance
    // AFTER the marker has been consumed is covered by the case below, which is what makes the
    // notifier's own contribution to `next()` and to `hasNext` separately assertable.
  }

  test("every advance after the first raises the latched failure, from next as from hasNext") {
    startContext()
    // A channel failure is reported twice by design: as a marker on the consumer's hand-off queue,
    // and as a throwable on the first-error-wins notifier. The marker is consumed by the first
    // advance that sees it, which converts it into the atomic per-producer invalidation and the
    // fetch failure. From that point on the ONLY thing standing between the reduce task and the
    // records still sitting in its buffers is `errorNotifier.throwIfError()`, and it has to be
    // consulted on every advance of either kind -- an advance that skipped it would hand the task a
    // record from a producer already declared lost, which is precisely the mixing of streamed and
    // recomputed input that partial-read invalidation exists to prevent.
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(block => stream.deliver(block))
    // No end-of-stream frame, deliberately. A producer that announced the orderly end of every
    // partition and then dropped its channel has NOT been lost -- the read already has everything
    // it was promised -- and the handler is right to decline to report that as a failure. Leaving
    // the stream open is what makes the channel failure below a genuine producer loss.

    // Positioned on real records first, so nothing below can be confused with exhaustion: an
    // iterator that had run out raises NoSuchElementException from `next()`, and the interceptors
    // below accept only a fetch failure. Four blocks carry all twenty-four records, so two records
    // read here leave the rest decodable from bytes already delivered and no advance has to wait.
    val firstRecord = withTaskContext(fixture.context)(records.next())
    val secondRecord = withTaskContext(fixture.context)(records.next())
    assert(StreamedRecords.contains((firstRecord._1, firstRecord._2)) &&
        StreamedRecords.contains((secondRecord._1, secondRecord._2)),
      s"The read must yield real records before the failure, but it yielded $firstRecord and " +
        s"$secondRecord")

    // Deliberately NO `hasNext` between the last successful record and the injection. Scala's
    // `Iterator.flatMap` -- which is how this read concatenates its producers and their partitions
    // -- memoises a positive `hasNext` and answers the next one from that memo without consulting
    // the underlying iterator at all. A `hasNext` here would therefore be answered by the memo
    // rather than by the reader, and the first advance able to reach the reader again would be
    // `next()`. That is not a defect in either party, and it is exactly why `next()` has to carry
    // its own check: there are positions in a perfectly ordinary read from which `next()` is the
    // ONLY advance that can refuse.
    val injected = new SparkException("a channel failure observed on a netty event loop thread")
    injectFromIoThread(stream, injected)

    // Advance one, through `next()`. The channel failure was reported to this handler as a marker
    // on its hand-off queue as well as on the notifier, and this is where the marker is converted
    // -- atomically discarding everything taken from the producer and raising the one signal the
    // scheduler acts on.
    val fromNext = intercept[FetchFailedException] {
      withTaskContext(fixture.context)(records.next())
    }
    assert(diagnosisOf(fromNext).contains(injected),
      s"The converted failure must carry the injected cause, but it carried " +
        s"${diagnosisOf(fromNext).map(_.getMessage).mkString("[", ", ", "]")}")
    assert(fetchFailedReasonOf(fromNext).reduceId == fixture.partitionId,
      s"and it must name the partition being read, but it named " +
        s"${fetchFailedReasonOf(fromNext).reduceId}")

    // Advance two, through `next()` again. The marker has been consumed, so nothing on the queue
    // can refuse this one: only `errorNotifier.throwIfError()` remains between the reduce task and
    // the bytes it still has buffered. Identity is what states that -- the notifier re-throws the
    // failure it latched rather than manufacturing another, which no other code path would do.
    val secondFromNext = intercept[FetchFailedException] {
      withTaskContext(fixture.context)(records.next())
    }
    assert(secondFromNext eq fromNext,
      "next() must keep re-throwing the latched failure once the queued marker is gone, but it " +
        s"raised a different one: ${secondFromNext.getMessage}")

    // And every advance after that, in either direction, any number of times. A latch that released
    // after one read would hand the task a record from a producer already declared lost, which is
    // precisely the mixing of streamed and recomputed input that invalidation exists to prevent.
    val laterAdvances: Seq[FetchFailedException] = Seq(
      intercept[FetchFailedException](withTaskContext(fixture.context)(records.hasNext)),
      intercept[FetchFailedException](withTaskContext(fixture.context)(records.next())),
      intercept[FetchFailedException](withTaskContext(fixture.context)(records.hasNext)),
      intercept[FetchFailedException](withTaskContext(fixture.context)(records.next())))
    assert(laterAdvances.forall(_ eq fromNext),
      "Every advance must keep raising the one latched failure, but at least one raised a " +
        s"different one: ${laterAdvances.map(_.getMessage).distinct.mkString("[", ", ", "]")}")

    // Six advances were refused above, so the reduce task consumed exactly the two records it took
    // before the failure and not one record of the producer's output after it. Stated on the read
    // accounting rather than inferred from the refusals, because a `next()` that handed out a
    // buffered record and then refused would satisfy every interceptor above and still be the
    // defect: the whole point of atomic per-producer invalidation is that no record from a lost
    // producer reaches the reduce task.
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
    // A producer that has committed every position it names may announce the end of its stream
    // while a replay this consumer asked for is still in flight -- which is the ordinary state of
    // affairs the moment one block arrives out of order, because the consumer does not deliver the
    // ones that followed it either. Refusing that terminator against the position the consumer has
    // reached reports a producer that has done nothing wrong as lost, and the recomputation of the
    // whole upstream stage is paid for a stream that was seconds from completing. The announcement
    // is held instead and applied by the admission that closes the gap.
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)
    assert(blocks.size >= 3, s"the case needs a gap with blocks on both sides of it, but the " +
      s"fixture cut ${blocks.size}")

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    // Position 1 is withheld, so every block after it is out of order and is quarantined for a
    // replay rather than delivered.
    stream.deliver(blocks.head)
    blocks.drop(2).foreach(block => stream.deliver(block))
    // The producer announces the end of a stream it really did produce in full.
    stream.deliver(fixture.terminator(blocks.size.toLong))
    // The replay lands: the withheld position, then the positions that were quarantined behind it.
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
    // Delivery on this protocol is idempotent by design: a resume replays from the position a
    // consumer announced, a queue ceiling defers a block that is re-queued later, and a repair
    // replays a run that can span what has already been delivered. A copy of a position this
    // consumer already holds is therefore routine, and the live path discards it. The same frame
    // arriving just after the terminator is the same redundant copy of the same output, so it must
    // get the same answer -- treating it as proof of an unsound producer failed reduce stages of
    // shuffles whose output was complete and correct, and paid for a whole map stage again.
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)
    assert(blocks.size >= 2, s"the case needs a block to re-deliver behind the frontier, but the " +
      s"fixture cut ${blocks.size}")

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    blocks.foreach(block => stream.deliver(block))
    stream.deliver(fixture.terminator(blocks.size.toLong))
    // The producer had already committed these two positions to the wire when it ended the stream.
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
  }

  test("a producer failure whose hand-off marker never arrived still escalates to a fetch " +
      "failure") {
    startContext()
    // A channel failure is reported twice by design, and only one of the two reports is guaranteed
    // to arrive: the hand-off marker can be refused by a full queue, a second failure on an
    // already-signalled partition raises no further marker, and a failure observed on a channel
    // callback before this reader bound the stream names no partition at all. What remains in every
    // one of those cases is a bare throwable on the notifier -- and a bare throwable propagates as
    // an ordinary task failure, so the scheduler counts it against `spark.task.maxFailures` and
    // aborts the job instead of recomputing the stage that produced the unreadable bytes. The read
    // must therefore escalate a latched producer failure to a fetch failure on its own account.
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream

    val injected = StreamingShuffleErrors.invalidSequenceNumber(
      shuffleId = fixture.shuffleId, partitionId = fixture.partitionId, expected = 0L, actual = 1L)
    injectFromIoThread(stream, injected)
    // The marker is taken off the hand-off queue here rather than by the read, which is exactly the
    // state a queue that refused it leaves behind: the failure is latched and nothing on the queue
    // will ever mention it.
    var drained = stream.handler.poll()
    while (drained.isDefined) {
      drained = stream.handler.poll()
    }
    // Blocks are re-offered afterwards so the read has input to advance over: a read that could not
    // advance at all would refuse for want of data rather than for the latched failure.
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
    // The operator-facing text of the failure the scheduler reports must be the typed condition
    // itself, not the name of the class that carried it: a reason reading "was lost:
    // SparkException" hides the shuffle, the partition and the positions that disagree behind a
    // getCause() walk.
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

  test("a wrapped failure reports the cause's message rather than its class name") {
    // The wrapper is what a task sees when a checked failure has no producer to attribute it to, so
    // its text is the whole of what an operator reads. A typed streaming condition states the
    // condition name, the shuffle and partition, the values that disagree and the SQLSTATE in its
    // message; reporting `(org.apache.spark.SparkException)` in its place discards all of it.
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

    // The JDK refuses to suppress a throwable by itself, so a notifier that recorded the same
    // instance twice would throw from its own reporting path if it did not guard the case.
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

    // A second socket reaching the same handler. The handler latches the first channel it is used
    // on, so this one is a stranger: a peer that reached this handler on a second connection would
    // otherwise simply become its new owner.
    //
    // The identity is supplied explicitly, and it has to be: every plain `EmbeddedChannel` in a JVM
    // reports the same channel id text, so a second one would be indistinguishable from the bound
    // one and this test would assert nothing. A real socket carries a distinct id, which is what
    // the production check relies on, and constructing one here is how the double gets that
    // property.
    val foreignChannel = new EmbeddedChannel(DefaultChannelId.newInstance())
    val foreignClient =
      new TransportClient(foreignChannel, new TransportResponseHandler(foreignChannel))
    assert(foreignChannel.id().asLongText() != stream.channel.id().asLongText(),
      "The foreign channel must have an identity of its own, or the handler could not tell it " +
        "apart from the one it is bound to")
    try {
      // The frame is impeccable in every respect except the channel it arrived on: the right
      // shuffle, the right producer, the right partition, the next sequence number and a correct
      // CRC32C. Nothing about the frame can be the reason it is refused.
      stream.handler.receive(foreignClient, blocks(1).toByteBuffer())
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

      // Bytes that are not a streaming frame at all. Refusal before decode is what makes this
      // harmless: a decoder that had been reached would raise, the failure would be latched on the
      // notifier, and the read below would fail with it.
      stream.handler.receive(foreignClient, ByteBuffer.wrap(Array[Byte](-1, -2, -3, -4)))
      assert(stream.handler.foreignChannelCallbackCount == 2L,
        s"Every callback from an unbound channel must be counted, but only " +
          s"${stream.handler.foreignChannelCallbackCount} were")

      // Nor may a stranger provoke a subscription, which is the message that carries this
      // consumer's identity and its position.
      stream.handler.channelActive(foreignClient)
      assert(stream.handler.foreignChannelCallbackCount == 3L,
        "An activation callback from an unbound channel must be refused and counted")
      assert(foreignChannel.readOutbound[AnyRef]() == null,
        "Nothing may be written to a channel this handler is not bound to, because the frame it " +
          "would write is the subscription that names this consumer")

      // A failure raised on a stranger's channel belongs to whoever owns that channel. Reaching
      // this task's notifier would fail a reduce task over a socket it never read from.
      stream.handler.exceptionCaught(
        new SparkException("a transport failure on somebody else's channel"), foreignClient)
      assert(stream.handler.foreignChannelCallbackCount == 4L,
        "An exception callback from an unbound channel must be refused and counted")
      assert(!foreignClient.isActive(),
        "A channel nobody owns must still be closed rather than left open")

      // And the bound channel is untouched throughout: denial affects the offending channel alone.
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

    // Three ways a peer can name its way towards a stream this task never asked for. Each is
    // checked against a value this executor holds -- the handler's own shuffle, its own producer,
    // and the reduce task's own partition range -- rather than against a field of the frame
    // compared with itself.
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

    // A frame addressed to somebody else is a routing mistake and not a protocol violation, so it
    // must not fail this reduce task. Nothing proves that as directly as the read completing: the
    // iterator consults the first-error-wins notifier on every single advance, so a failure latched
    // by any of the three drops above would be re-thrown here.
    blocks.foreach(block => stream.deliver(block))
    stream.deliver(fixture.terminator(blocks.size.toLong))
    assert(readRecords(records) == expectedRecords,
      "A read that dropped three misaddressed frames must still yield every record its own " +
        "producer sent, and must not have been failed by them")

    // A correctly addressed frame of a type only a consumer sends. Receiving one means the peer has
    // this channel's direction backwards, and it is refused with the typed condition rather than
    // applied to a ledger it does not describe.
    val directed = new ReaderFixture()
    val directedRecords = directed.reader.read()
    val directedStream = directed.connector.onlyStream
    directedStream.deliver(ack(directed.shuffleId, ProducerMapId, directed.partitionId, 0L, 0L))
    val refused = intercept[FetchFailedException] {
      withTaskContext(directed.context) {
        readRecords(directedRecords)
      }
    }
    // The read fails as a fetch failure, because that is the only signal the scheduler understands
    // and a peer that has this channel's direction backwards cannot be recovered from by reading
    // harder. The typed condition travels with it, so the diagnosis is not lost.
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

    // The token authorizes a caller to the driver's coordinator and is the one credential this
    // subsystem holds. A producer is not the coordinator, so nothing this consumer writes on a
    // producer channel may carry it -- and the frames are audited as bytes rather than as decoded
    // fields, because a credential that leaked into any field at all would still be on the wire.
    val written = stream.encodedOutboundFrames
    assert(written.nonEmpty,
      "The consumer must have written frames -- its subscription and its acknowledgements -- or " +
        "there is nothing to audit")
    val leaking = written.count(frame => containsBytes(frame, tokenBytes))
    assert(leaking == 0,
      s"No frame written to a producer may carry the shuffle's capability token, but ${leaking} " +
        s"of ${written.size} did")

    // Nor may it reach a diagnostic. The handle redacts it, which is what keeps it out of every
    // log line and every task-failure report that renders a handle.
    assert(!fixture.handle.toString.contains(CapabilityToken),
      s"The shuffle handle must redact its capability token, but it rendered as " +
        s"${fixture.handle.toString}")

    // Including the diagnostic that a failed read produces, which is the one an operator sees.
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

  test("the producer connector routes only channels it owns and counts the frames it cannot") {
    // The demultiplexer beneath every streaming read. One transport context serves a reduce task's
    // whole set of producers, so each callback has to be routed to the handler that owns the
    // channel it arrived on -- and a channel this connector never opened has no such handler. What
    // it does then is the contract under test, and the two halves of it pull in opposite
    // directions: a *frame* on an unroutable channel is counted, because the sequence-gap repair
    // will ask for it again and raising would fail a reduce task over a frame nobody asked for; a
    // *lifecycle callback* on one is not counted at all, because a channel becoming active or
    // inactive before or after its binding is the transport's ordinary ordering rather than
    // anything lost. A connector that counted the callbacks too would read non-zero on every
    // healthy run.
    val connectorConf = streamingConf()
    val connectorClock = newManualClock()
    val connector = new NettyStreamingShuffleProducerConnector(connectorConf, connectorClock)
    val connectorShuffleId = 17
    try {
      // A channel with an identity of its own: every plain EmbeddedChannel in a JVM reports the
      // same channel id, so two of them would be one key to a connector that keys by channel.
      val strangerChannel = new EmbeddedChannel(DefaultChannelId.newInstance())
      val stranger = new TransportClient(strangerChannel, new TransportResponseHandler(
        strangerChannel))
      assert(connector.unboundFrameCount === 0L,
        "a connector that has been handed nothing must report no unbound frame")

      // A frame that is in every other respect perfectly well formed, so what is being asserted is
      // the routing and nothing else.
      val block = dataBlock(connectorShuffleId, ProducerMapId, ReducePartition,
        sequenceNumber = 0L, payload = payloadOfLength(0L, 64))
      connector.receive(stranger, block.toByteBuffer())
      assert(connector.unboundFrameCount === 1L,
        "a data frame on a channel no handler owns must be counted rather than raised")
      connector.receive(stranger, block.toByteBuffer())
      assert(connector.unboundFrameCount === 2L,
        "every unroutable frame must be counted, not just the first one that was logged")

      // The request/reply form of the same callback. Nothing may answer a frame nobody asked for,
      // so the callback must be left untouched: a reply would tell a stranger it had been served.
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

      // The lifecycle callbacks. Each must be silent, must not raise, and must leave the frame
      // count exactly where the frames left it -- which is what makes the count a statement about
      // frames.
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
      // The reserved .invalid domain never resolves, so this is deterministic rather than dependent
      // on some port happening to be free.
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

  test("a streaming reader may only be constructed for a whole map range with streaming enabled") {
    startContext()
    val fixture = new ReaderFixture()

    val gatedOff = intercept[IllegalArgumentException] {
      fixture.newReader(gatedOffStreamingConf(), FullRangeStart, FullRangeEnd)
    }
    assert(gatedOff.getMessage.contains(SHUFFLE_STREAMING_ENABLED.key),
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

  // -----------------------------------------------------------------------------------------------
  // Hostile frame ordering around the end of stream.
  //
  // A terminator is a peer-authored claim that a stream is finished. Accepting it on trust is the
  // one loss no checksum can catch: every block that did arrive was intact, so a partition short by
  // its last blocks reads as a complete result. Each case below sends an ordering a well-behaved
  // producer never sends, and requires a fetch failure rather than a successful, truncated read.
  // -----------------------------------------------------------------------------------------------

  test("a premature end of stream fails the fetch rather than truncating the partition") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    val records = fixture.reader.read()
    val stream = fixture.connector.onlyStream
    // Two blocks arrive and the producer then claims the stream ended after all four. The claim is
    // ahead of the position the consumer reached, so it names blocks that were never delivered.
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
    // The honest terminator, which the consumer accepts because it names the position reached.
    stream.deliver(fixture.terminator(blocks.size.toLong))
    assert(stream.handler.isStreamComplete(fixture.partitionId),
      "The matching terminator must be accepted")
    assert(stream.handler.announcedBlockCount(fixture.partitionId).contains(blocks.size.toLong),
      "The accepted terminator's total is the one the consumer holds")

    // A second terminator naming a different total. It must not overwrite the accepted one, and it
    // must not be ignored either: a producer contradicting its own end of stream cannot be
    // completed.
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
    // A control frame can legitimately be sent twice -- a producer replaying its end of stream
    // after a reconnection, for instance -- so an identical repeat is a duplicate rather than a
    // fault. It must neither fail the read nor be delivered to the task a second time.
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
    // A producer still sending past its own end of stream contradicts either the terminator or the
    // block. Admitting the block would put the consumed count above the total the terminator fixed,
    // which is the total the completion check compares against.
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
  //
  // A consumer handler is created per producer generation and bound to exactly one channel, so the
  // channel is the capability. A frame on any other channel must be refused before its body is
  // read, and raw bytes that no message constructor would have produced must not be able to
  // allocate state or charge the executor's shared receive budget.
  // -----------------------------------------------------------------------------------------------

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

    // A perfectly valid block, delivered over a channel this handler was never bound to. The
    // refusal can therefore only be attributed to the binding, never to the content.
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
  //
  // A block admission reserves from an executor-wide budget and then records ownership of what it
  // reserved. Cleanup returns what the ledgers hold and drops them. Every interleaving of the two
  // must return the reservation exactly once, because a budget that shrinks by one block per
  // cancelled reduce task starves every consumer that follows it on the executor.
  // -----------------------------------------------------------------------------------------------

  test("a block admitted while the handler closes returns its receive quota exactly once") {
    startContext()
    val fixture = new ReaderFixture()
    val payload = fixture.encodePartition(StreamedRecords)
    val blocks = fixture.dataBlocksOf(payload, BlockCount)

    fixture.reader.read()
    val stream = fixture.connector.onlyStream
    val handler = stream.handler
    val baseline = fixture.backpressure.reservedReceiveQuotaBytes

    // One block is admitted normally, so the budget is genuinely charged and the ledgers genuinely
    // hold something for the closure to return.
    stream.deliver(blocks.head)
    assert(fixture.backpressure.reservedReceiveQuotaBytes > baseline,
      "An admitted block must charge the executor's shared receive budget")

    // The handler closes, and only then does the next block arrive. This is the interleaving the
    // rollback exists for: the block reserves, is handed to a queue nothing will ever drain, and
    // discovers the closure -- so its charge must come back from the admission itself rather than
    // from ledgers the closure has already accounted for.
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
    // Closing twice must not return anything a second time, which is what would inflate the budget.
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
    // to exercise the window the in-flight accounting closes. Whichever side wins, the invariant is
    // the same and it is exact: the executor's shared reservation ends where it began.
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
  //
  // The production connector is exercised directly here, with only its socket creation substituted,
  // because the races below live inside the interval a real connect cannot be held open across. A
  // channel that is published after the connector closed is a socket nothing releases; a channel
  // that dies before it is published is a reduce task waiting out its full five-second timeout on
  // input that will never arrive. Both are settled at the binding, and both are proved here.
  // -----------------------------------------------------------------------------------------------

  /**
   * A consumer handler of the shape the reader builds, for the connector cases.
   *
   * Built by hand rather than through a read, because these cases are about the connector's
   * ownership of a channel and not about what travels over it -- so the handler only has to be the
   * real class, with the real bindings, and does not need a producer behind it.
   *
   * @param conf configuration the handler reads its own entries from
   * @param clock the time source the handler reads
   * @return the handler
   */
  private def newConnectorHandler(
      conf: SparkConf,
      clock: ManualClock): StreamingShuffleClientHandler = {
    val budget = TokenBucketRateLimiter.executorBudget(conf, clock)
    val protocol = new BackpressureProtocol(conf, null, budget, clock)
    new StreamingShuffleClientHandler(conf, shuffleId = 0, mapId = ProducerMapId,
      taskAttemptId = ProducerAttemptId, consumerId = "streaming-shuffle-connector-consumer",
      startPartition = ReducePartition, endPartition = ReducePartition + 1, backpressure = protocol,
      errorNotifier = new StreamingShuffleErrorNotifier(0, conf), clock = clock)
  }

  test("a channel created while the connector closes is closed rather than published") {
    val conf = streamingConf()
    val clock = newManualClock()
    val connector = new BarrieredStreamingShuffleProducerConnector(conf)
    val handler = newConnectorHandler(conf, clock)
    try {
      // Standing inside the creation interval and closing the connector is exactly the race: the
      // closed check at the top of connect has already passed, and every registry close() empties
      // has been emptied by the time the connecting thread reaches the binding.
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
      // The callback arrives while the connecting thread is still inside the creation, which is the
      // ordering that used to be lost: the removals find nothing, so without the record the binding
      // would go on to publish a handler for a channel that was already dead.
      connector.duringCreation(() => {
        connector.createdClients.foreach { created =>
          // Active first and then dead, both inside the creation interval, which is the ordering a
          // real socket produces when a peer accepts a connection and drops it immediately. The
          // activation is what gives the handler the streams the loss is then reported against.
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
      // One healthy connection first, so the closure below has a real channel to release rather
      // than an empty registry to walk.
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

      // And a connect after the closure is refused outright, before any socket is created.
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

}
