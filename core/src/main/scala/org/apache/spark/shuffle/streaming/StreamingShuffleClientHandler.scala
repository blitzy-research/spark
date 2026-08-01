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

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.util.ReferenceCountUtil

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{BLOCK_ID, CHECKSUM, COUNT, ERROR, HOST_PORT, NUM_BYTES, PARTITION_ID, REASON, SHUFFLE_ID, THRESHOLD, TIMEOUT}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, DataBlockMessage, HeartbeatMessage, RetransmitRequestMessage, StreamingShuffleChecksum, StreamingShuffleMessage, StreamingShuffleMessageType, StreamTerminationMessage}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.util.{Clock, SystemClock}

/**
 * The consumer side of a streaming shuffle channel: a Netty inbound handler that accepts blocks a
 * producer pushes, hands them to the reduce task, acknowledges what the task has consumed, repairs
 * bounded corruption by asking for a replay, and exerts TCP-level backpressure by turning the
 * channel's `autoRead` off and on again.
 *
 * <b>Why `autoRead` lives here, and only here.</b> Toggling `autoRead` is a new idiom in this code
 * base: it appears nowhere in the shared transport module and nowhere else in Spark core. Confining
 * it to this file and to its producer-side sibling is precisely what satisfies the directive to
 * prefer the least modification to the network transport layer. No shared transport class is
 * modified by this feature: `TransportContext`, its pipeline initialisation, `TransportConf` and
 * `SparkTransportConf` are all consumed exactly as they stand. The one piece of transport
 * configuration this handler needs -- an independent tuning namespace -- is obtained by passing a
 * module name as an argument to `SparkTransportConf.fromSparkConf`, which yields
 * `spark.shuffle-streaming.io.*` without a single line added to a shared file. That is also why OS
 * level keepalive can be enabled for streaming alone: it is a per-module boolean, and the transport
 * layer exposes no keepalive interval, so the five-second liveness bound this subsystem promises is
 * enforced at application level by the heartbeat timer rather than by TCP.
 *
 * <b>How the three layers of backpressure cooperate.</b> The streaming shuffle holds a producer
 * back in three ways at once, and this handler owns the third of them.
 *
 *  - Application-level credit. [[BackpressureProtocol]] keeps a per-stream ledger and answers
 *    `hasCredit`; a producer whose credit is spent must stop and wait for an acknowledgement.
 *  - Rate limiting. A token bucket paces egress and is consulted by the producer, never here.
 *  - TCP-level throttling. When credit is spent, or when this handler's own hand-off queue is at
 *    its high-water mark, `autoRead` is turned off: Netty stops reading from the socket, the
 *    receive window closes, and the pressure reaches the producer through TCP itself rather than
 *    through any message this subsystem has to invent. An acknowledgement that advances the
 *    consumer position, or a queue that has drained, turns it back on.
 *
 * Every transition into the throttled state belongs to [[BackpressureProtocol]], which owns the
 * `shuffle.streaming.backpressureEvents` counter and advances it from the one place a throttling
 * episode begins. This handler therefore reports the edge to the protocol and deliberately
 * increments no metric of its own: the throttling of a producer and the closing of a consumer's
 * receive window are two faces of one episode, and counting both would report an operator a number
 * twice the size of the thing it names.
 *
 * <b>The thread model, which is binding.</b> Two kinds of thread touch this class.
 *
 *  - A Netty event-loop thread runs every `channel*` callback. It may decode a frame, update the
 *    lock-free bookkeeping, enqueue a block and write an acknowledgement, and it may do nothing
 *    else. In particular it never calls a `ShuffleReadMetricsReporter` method -- that interface
 *    documents itself as single-threaded -- it never parks, blocks or sleeps, and it never lets an
 *    exception escape a callback.
 *  - The reduce task's own thread drains the queue, performs the authoritative checksum
 *    verification, updates the read metrics and throws. A failure this handler observes on an I/O
 *    thread is handed to [[StreamingShuffleErrorNotifier]], whose first-error-wins latch the task
 *    thread reads inside `read()`. Without that bridge a failure seen only on an I/O thread would
 *    be swallowed and the task would wait forever for input that is never going to arrive, because
 *    `ShuffleReader` exposes only `read()` and its `stop()` is commented out.
 *
 * Because that trait has no lifecycle hook, release is not something this handler can schedule for
 * itself. It exposes an idempotent [[close]] instead, which the reader registers on
 * `TaskContext.addTaskCompletionListener` so that it runs on success, on failure and on
 * cancellation alike. No task-completion listener is ever registered from a Netty thread.
 *
 * <b>Framing.</b> Inbound frames are the type byte followed by the encoded body, exactly what
 * `StreamingShuffleMessage.toByteBuffer` produces and `StreamingShuffleMessage.Decoder` consumes;
 * outbound messages are written in the same shape. Messages are discriminated on that type byte and
 * on the concrete class, never on length: the four fixed-shape control messages all encode to the
 * same number of bytes, so length distinguishes nothing at all.
 *
 * One instance serves one channel. The handler is deliberately not annotated as sharable because it
 * holds per-channel state, so Netty refuses to add it to a second pipeline. Any number of reduce
 * partitions of one shuffle may be multiplexed on the channel; the partition each frame belongs to
 * is read from its header.
 *
 * @param conf the executor's configuration, read once at construction and then held immutably,
 *             which is what makes "a configuration change requires an executor restart" true by
 *             construction
 * @param shuffleId the shuffle this channel serves; every frame that names another shuffle is
 *                  refused at the boundary
 * @param backpressure the flow-control protocol that owns the credit ledger, the liveness timers
 *                     and the backpressure-event counter; must not be null
 * @param errorNotifier the bridge that carries a failure observed on this I/O thread to the task
 *                      thread; must not be null
 * @param clock the single time source for every timer in this class, injected so that the suites
 *              covering it are deterministic without sleeping
 */
private[spark] class StreamingShuffleClientHandler(
    conf: SparkConf,
    shuffleId: Int,
    backpressure: BackpressureProtocol,
    errorNotifier: StreamingShuffleErrorNotifier,
    clock: Clock = new SystemClock)
  extends ChannelInboundHandlerAdapter with Logging {

  import StreamingShuffleClientHandler._

  require(conf != null, "The Spark configuration must not be null.")
  require(shuffleId >= 0, s"The shuffle id must be non-negative but was $shuffleId.")
  require(backpressure != null, "The streaming shuffle backpressure protocol must not be null.")
  require(errorNotifier != null, "The streaming shuffle error notifier must not be null.")
  require(clock != null, "The streaming shuffle clock must not be null.")

  // Read once and held immutably. This flag is the single authority over every verbose line this
  // handler writes, so an operator who leaves spark.shuffle.streaming.debug at its default of false
  // sees nothing per block and nothing per autoRead toggle even when logging is globally set to
  // DEBUG. That is what keeps the subsystem inside its budget of under 10 MB per hour per executor,
  // because autoRead can flap once per throttling episode on every channel.
  private val debugEnabled: Boolean = conf.get(config.SHUFFLE_STREAMING_DEBUG)

  // Transport tuning for streaming shuffle alone. The module name is an ARGUMENT, never an entry
  // added to a shared file, and fromSparkConf clones the configuration before touching it, so
  // asking for it here perturbs nothing that block transfer relies on. The result is an independent
  // spark.shuffle-streaming.io.* namespace whose thread counts, retry policy and keepalive setting
  // can be tuned without any effect on the existing shuffle transport.
  private val transportConf: TransportConf = SparkTransportConf.fromSparkConf(
    conf,
    module = TRANSPORT_MODULE,
    numUsableCores = 0,
    role = None,
    sslOptions = None)

  // Replay budget for a corrupt or missing block inside the retained window, taken from the
  // streaming module's own retry knobs rather than from a constant of this file's invention. The
  // protocol caps attempts at five with an exponential pause; these two bound the transport-level
  // retries that sit underneath that.
  private val maxIoRetries: Int = transportConf.maxIORetries()

  private val ioRetryWaitMillis: Int = transportConf.ioRetryWaitTimeMs()

  // OS-level keepalive removes a connection that has been idle too long, but the transport exposes
  // it as a boolean with no interval and the JDK exposes no keepalive-interval socket option, so it
  // cannot be the five-second detector this subsystem promises. It is recorded here so a diagnostic
  // can state which of the two mechanisms is in play, and the five-second bound is enforced by the
  // heartbeat timer below.
  private val tcpKeepAliveEnabled: Boolean = transportConf.enableTcpKeepAlive()

  // The hand-off queue between the event loop and the task thread. Bounded, because an unbounded
  // one would turn a slow reduce task into unbounded retained heap; the high-water mark below is
  // what keeps it from ever reaching the bound, since crossing that mark closes the receive window
  // and the producer's credit limit caps what can already be in flight.
  private val inbound = new LinkedBlockingQueue[Inbound](INBOUND_QUEUE_CAPACITY)

  // Payload bytes currently sitting in that queue. Counted as well as the events themselves because
  // a block may carry anything from nothing to two mebibytes, so an event count alone is a poor
  // description of the heap the queue is holding.
  private val queuedPayloadBytes = new AtomicLong(0L)

  // Per-partition bookkeeping. Written only by this channel's event-loop thread, which serialises
  // every mutation, and read by the task thread, which is why each cell is an atomic: the atomics
  // are here for visibility across the two threads, not to order writes against each other.
  private val partitions = new ConcurrentHashMap[Integer, PartitionState]()

  // The channel this handler is attached to, captured as early as Netty offers it so that the task
  // thread can acknowledge, request a replay and close without having to be handed a context.
  private val channelRef = new AtomicReference[Channel](null)

  // Current autoRead state as this handler believes it to be. Every setAutoRead call in this file
  // is guarded by a compare-and-set on this cell, so a redundant call is skipped and the toggle is
  // idempotent no matter which thread reaches it.
  private val autoReadEnabled = new AtomicBoolean(true)

  // Transitions into the throttled state observed on this channel. The executor-wide
  // shuffle.streaming.backpressureEvents counter is NOT advanced from here -- the protocol owns it
  // -- so this local total exists for assertions that need to speak about one channel.
  private val throttleTransitions = new AtomicLong(0L)

  private val throttleCause = new AtomicReference[String](null)

  // Instant at which this channel went inactive with work outstanding, read from the injected
  // clock. NO_TIMESTAMP while the producer is believed alive.
  private val producerLostAtMillis = new AtomicLong(NO_TIMESTAMP)

  // Warn lines already spent on escalated failures. A peer sending malformed frames produces one
  // failure per frame, so the warn level is spent on the first few and everything after that is
  // debug: the notifier retains the root cause either way, and the count is observable.
  private val escalationsReported = new AtomicLong(0L)

  private val closed = new AtomicBoolean(false)

  // ==========================================================================================
  // Netty lifecycle. Every override carries an explicit result type, none of them lets an
  // exception escape, and none of them performs work that could park the event loop.
  // ==========================================================================================

  /**
   * Captures the channel as soon as Netty attaches this handler, which is earlier than
   * `channelActive` and therefore the first point at which the task thread could need it.
   */
  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    channelRef.set(ctx.channel())
    super.handlerAdded(ctx)
  }

  /** Drops the captured channel when the handler leaves the pipeline. */
  override def handlerRemoved(ctx: ChannelHandlerContext): Unit = {
    channelRef.compareAndSet(ctx.channel(), null)
    super.handlerRemoved(ctx)
  }

  /**
   * Records the channel and states, once, which of the two liveness mechanisms is in play. The
   * transport's keepalive is an idle-connection reaper with no interval of its own, so it is named
   * here only so that a diagnostic cannot mistake it for the five-second detector, which is the
   * application-level heartbeat timer.
   */
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    channelRef.set(ctx.channel())
    producerLostAtMillis.set(NO_TIMESTAMP)
    if (debugEnabled) {
      logDebug(log"Streaming shuffle consumer channel for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} is active against " +
        log"${MDC(HOST_PORT, String.valueOf(ctx.channel().remoteAddress()))} with OS keepalive " +
        log"${MDC(REASON, String.valueOf(tcpKeepAliveEnabled))} and a transport replay budget of " +
        log"${MDC(COUNT, maxIoRetries)} attempt(s) ${MDC(TIMEOUT, ioRetryWaitMillis)} ms apart")
    }
    super.channelActive(ctx)
  }

  /**
   * Signals producer loss.
   *
   * A channel that goes inactive can mean either of two things, and the difference matters: after
   * an orderly end of stream it is simply the socket closing behind a completed transfer, while
   * with blocks still owing it is the producer disappearing. Only the second case is a failure.
   *
   * In that second case the protocol is prompted to re-evaluate, so that its five-second connection
   * timeout is measured from an up-to-date reading rather than from whenever a poll last happened,
   * and every partition still owing data is handed a marker on the queue. That marker is what wakes
   * a reduce task blocked on input which is never going to arrive; leaving it out would leave the
   * task waiting for a full timeout on a socket that has already gone.
   */
  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    val outstanding = partitionsAwaitingData
    if (outstanding.isEmpty) {
      if (debugEnabled) {
        logDebug(log"Streaming shuffle consumer channel for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} closed with every stream already terminated")
      }
    } else {
      producerLostAtMillis.compareAndSet(NO_TIMESTAMP, clock.getTimeMillis())
      logWarning(log"Streaming shuffle producer for shuffle ${MDC(SHUFFLE_ID, shuffleId)} closed " +
        log"the connection from ${MDC(HOST_PORT, String.valueOf(ctx.channel().remoteAddress()))} " +
        log"with ${MDC(COUNT, outstanding.size)} stream(s) still awaiting data")
      // Prompting the protocol is the whole of the hand-off available: it owns the five-second
      // window, judges it from its own clock and re-evaluates every ledger in one non-blocking pass
      // that takes no lock and performs no I/O, so it is safe on an event-loop thread.
      backpressure.pollOnce()
      outstanding.foreach(partitionId => signalProducerLost(partitionId, PRODUCER_CLOSED_REASON))
    }
    super.channelInactive(ctx)
  }

  /**
   * Accepts one inbound frame.
   *
   * Three shapes are accepted, because a streaming channel may be assembled with the decode step
   * either in the pipeline or in this handler: an already-decoded [[StreamingShuffleMessage]], a
   * Netty `ByteBuf` holding a framed message, and an NIO `ByteBuffer` holding one. Anything else is
   * a programming error in the pipeline and is refused with the typed unexpected-type condition
   * rather than passed on.
   *
   * The frame is released in a `finally` block whatever happens. That is the whole of this class's
   * exposure to reference-counted memory: a decoded block owns a heap array of its own, so nothing
   * reference-counted survives this method, and the queue can never be the site of a Netty leak.
   * Since the handler is the terminal consumer of streaming frames, the message is not forwarded
   * down the pipeline; forwarding a buffer this method has released would be a use-after-free.
   */
  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    try {
      msg match {
        case message: StreamingShuffleMessage =>
          dispatch(message)
        case buf: ByteBuf =>
          dispatch(decodeFramed(framedBufferOf(buf)))
        case buffer: ByteBuffer =>
          dispatch(decodeFramed(buffer))
        case other =>
          escalate(
            StreamingShuffleErrors.unexpectedMessageType(
              STREAMING_MESSAGE_DESCRIPTION, describeClassOf(other)),
            UNKNOWN_PARTITION_ID)
      }
    } catch {
      // An exception must never escape a Netty callback: it would tear the pipeline down instead of
      // failing the task cleanly. Recording it is what makes the task thread raise it from read().
      case NonFatal(e) =>
        escalate(e, partitionIdOf(msg))
    } finally {
      releaseQuietly(msg)
    }
    evaluateAutoRead()
  }

  /**
   * Re-evaluates the receive window at the end of a read batch and flushes any acknowledgement that
   * was deferred because the channel was not writable.
   */
  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    flushDeferredAcks()
    evaluateAutoRead()
    super.channelReadComplete(ctx)
  }

  /**
   * Honours the outbound side of flow control.
   *
   * Acknowledgements are written from this handler, so a channel whose outbound buffer is full must
   * not be written to again: doing so would grow Netty's outbound queue without bound while the
   * inbound side is being throttled to protect exactly that kind of growth. A deferred
   * acknowledgement is coalesced to one position per partition -- a later position supersedes an
   * earlier one outright, since the protocol treats acknowledgements as monotonic -- so what is
   * held back is a single number per stream and never a backlog of messages.
   */
  override def channelWritabilityChanged(ctx: ChannelHandlerContext): Unit = {
    if (ctx.channel().isWritable) {
      flushDeferredAcks()
    }
    evaluateAutoRead()
    super.channelWritabilityChanged(ctx)
  }

  /**
   * Records a channel-level failure and closes the channel.
   *
   * The failure is not forwarded down the pipeline: this handler is the terminal consumer of the
   * streaming protocol, and the notifier is the route by which the task thread learns of it.
   * Closing the channel afterwards is what causes `channelInactive` to run, which is where the
   * streams still awaiting data are marked so that a blocked reduce task wakes immediately.
   */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    escalate(cause, UNKNOWN_PARTITION_ID)
    ctx.close()
  }

  // ==========================================================================================
  // The TCP-level backpressure layer: autoRead.
  // ==========================================================================================

  /**
   * Re-evaluates whether this channel should be reading from its socket, and applies the answer.
   *
   * Two conditions close the receive window, and either is sufficient.
   *
   *  - Consumer credit is exhausted. This is the condition the feature specifies, and the answer
   *    belongs to [[BackpressureProtocol]]: `hasCredit` is the predicate, and the bytes this
   *    handler has received but not yet acknowledged, measured against the credit limit the
   *    protocol published for the stream, is the same question asked from the consumer's own books.
   *  - The hand-off queue has reached its high-water mark. A bounded queue that filled up would
   *    leave the event loop with a block it can neither hand on nor drop, and the only alternatives
   *    at that point are to block the event loop or to lose data. Closing the window well before
   *    the bound removes the choice.
   *
   * Reopening is deliberately hysteretic: the window stays closed until the queue has drained to
   * the low-water mark and credit is available again, so that a task consuming one block at a time
   * cannot make the toggle flap once per block. Flapping would be correct but would spend the log
   * budget and the syscalls of an episode on every single block.
   *
   * Safe to call from any thread. `setAutoRead` is a channel configuration write that Netty permits
   * from any thread, the guard is a compare-and-set, and nothing here blocks -- which matters
   * because the task thread calls this after every poll and the event loop calls it after every
   * frame.
   */
  private def evaluateAutoRead(): Unit = {
    if (!closed.get()) {
      if (autoReadEnabled.get()) {
        val cause = throttleCauseNow()
        if (cause != null) {
          disableAutoRead(cause)
        }
      } else if (resumeAllowed()) {
        enableAutoRead()
      }
    }
  }

  /**
   * The reason to stop reading right now, or null when there is none. Credit is tested first
   * because it is the condition the protocol owns and the one an operator will want named.
   */
  private def throttleCauseNow(): String = {
    if (isAnyStreamCreditExhausted) {
      THROTTLE_CAUSE_CREDIT
    } else if (inbound.size() >= INBOUND_QUEUE_HIGH_WATER_MARK ||
        queuedPayloadBytes.get() >= INBOUND_HIGH_WATER_BYTES) {
      THROTTLE_CAUSE_QUEUE
    } else {
      null
    }
  }

  /** Whether both throttling conditions have cleared with margin to spare. */
  private def resumeAllowed(): Boolean = {
    inbound.size() <= INBOUND_QUEUE_LOW_WATER_MARK &&
      queuedPayloadBytes.get() <= INBOUND_LOW_WATER_BYTES &&
      !isAnyStreamCreditExhausted
  }

  /**
   * Stops reading from the socket, once per episode.
   *
   * The compare-and-set is what makes the toggle idempotent: a redundant `setAutoRead(false)` is
   * skipped, so the transition is counted once and logged at most once however many frames or polls
   * observe the same exhausted stream. The protocol is prompted on the edge because it owns both
   * the throttled state and the `shuffle.streaming.backpressureEvents` counter; this handler
   * advances no metric, since a throttled producer and a closed receive window are one episode seen
   * from two ends.
   */
  private def disableAutoRead(cause: String): Unit = {
    if (autoReadEnabled.compareAndSet(true, false)) {
      throttleCause.set(cause)
      throttleTransitions.incrementAndGet()
      applyAutoRead(enabled = false)
      backpressure.pollOnce()
      if (debugEnabled) {
        logDebug(log"Streaming shuffle consumer stopped reading for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} because ${MDC(REASON, cause)}, holding " +
          log"${MDC(COUNT, inbound.size())} block(s) and " +
          log"${MDC(NUM_BYTES, queuedPayloadBytes.get())} queued byte(s)")
      }
    }
  }

  /** Resumes reading from the socket, once per episode, under the same compare-and-set guard. */
  private def enableAutoRead(): Unit = {
    if (autoReadEnabled.compareAndSet(false, true)) {
      throttleCause.set(null)
      applyAutoRead(enabled = true)
      if (debugEnabled) {
        logDebug(log"Streaming shuffle consumer resumed reading for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} with ${MDC(COUNT, inbound.size())} block(s) and " +
          log"${MDC(NUM_BYTES, queuedPayloadBytes.get())} queued byte(s)")
      }
    }
  }

  /**
   * The single point at which this file touches `setAutoRead`, so that the containment claim in the
   * class documentation is checkable by inspection. A channel that has already gone is not an error
   * here: the flag has been moved either way, and there is nothing left to read from.
   */
  private def applyAutoRead(enabled: Boolean): Unit = {
    val channel = channelRef.get()
    if (channel != null) {
      channel.config().setAutoRead(enabled)
    }
  }

  /**
   * Whether any stream still expecting data has spent its credit.
   *
   * Iterates rather than allocating, because this runs after every frame. A stream whose producer
   * has announced the end of its output is skipped: withholding traffic from a stream that has
   * nothing further to send would close the window for no reason and delay the streams sharing the
   * channel with it.
   */
  private def isAnyStreamCreditExhausted: Boolean = {
    var exhausted = false
    val states = partitions.values().iterator()
    while (states.hasNext && !exhausted) {
      val state = states.next()
      if (!state.terminated.get() && isCreditExhausted(state)) {
        exhausted = true
      }
    }
    exhausted
  }

  /**
   * Whether one stream has spent its credit, asked in both of the ways available.
   *
   * `hasCredit` is the protocol's own answer and is authoritative wherever a ledger tracks the
   * outstanding volume. The second test is the same question from the consumer's books: bytes
   * received but not yet acknowledged, against the limit the protocol published. Both are consulted
   * because the ledger of a stream is kept by whichever side is accounting for it, and a consumer
   * that waited for the producer's view of its own backlog would close the window too late.
   */
  private def isCreditExhausted(state: PartitionState): Boolean = {
    val limit = backpressure.creditLimitBytes(shuffleId, state.partitionId)
    val outstanding = state.receivedBytes.get() - state.acknowledgedBytes.get()
    !backpressure.hasCredit(shuffleId, state.partitionId) ||
      (limit > 0L && outstanding >= limit)
  }

  // ==========================================================================================
  // Decoding and dispatch.
  // ==========================================================================================

  /**
   * Presents a Netty buffer's readable bytes as one NIO buffer for the decoder.
   *
   * A contiguous buffer is viewed without copying, which is what keeps a two-mebibyte block cheap.
   * A composite buffer cannot be viewed as a single NIO buffer at all, so its readable bytes are
   * gathered once into a heap array; that costs one copy and only for a frame that arrived
   * fragmented across components.
   */
  private def framedBufferOf(buf: ByteBuf): ByteBuffer = {
    if (buf.nioBufferCount() == 1) {
      buf.nioBuffer(buf.readerIndex(), buf.readableBytes())
    } else {
      val gathered = new Array[Byte](buf.readableBytes())
      buf.getBytes(buf.readerIndex(), gathered)
      ByteBuffer.wrap(gathered)
    }
  }

  /**
   * Settles the compatibility question before committing to a decode, then decodes.
   *
   * The version is read with `peekProtocolVersion`, which is absolute and non-consuming: the
   * buffer's position, limit and mark are all left exactly as they were, so the very same buffer is
   * handed to the decoder below. A mismatch is therefore an explicit check and never a parse
   * failure blamed on the wrong field.
   *
   * A mismatch is reported to the protocol, which latches it as the fourth condition under which
   * streaming steps aside in favour of sort-based shuffle -- the fallback policy reads the latched
   * reasons from there. It is then raised rather than swallowed: silently dropping the frame would
   * leave the reduce task waiting on input this executor cannot decode, so the exception naming
   * both revisions is allowed to propagate to this handler's own recorder, which fails the task and
   * lets the manager delegate.
   */
  private def decodeFramed(framed: ByteBuffer): StreamingShuffleMessage = {
    val version = StreamingShuffleMessage.peekProtocolVersion(framed)
    if (!StreamingShuffleMessage.isCompatible(version)) {
      backpressure.observeProtocolVersion(shuffleId, UNKNOWN_PARTITION_ID, version)
      StreamingShuffleMessage.checkProtocolVersion(version)
    }
    StreamingShuffleMessage.Decoder.fromByteBuffer(framed)
  }

  /**
   * Routes one decoded message.
   *
   * Two boundary checks run before any handler sees the message. The stream context check refuses a
   * frame that names another shuffle, which a block's own checksum cannot contradict once the
   * identifying fields are the ones in question; the partition is accepted as given, because a
   * single channel may carry several reduce partitions of the same shuffle. The version check is
   * explicit again here, so that an already-decoded message reaching this handler from a pipeline
   * stage is held to the same rule as bytes decoded above.
   *
   * Dispatch is on the concrete class, which is the type byte by another name. Length is never
   * consulted: the acknowledgement, heartbeat, retransmission-request and termination messages all
   * encode to the same number of bytes, so a codec that discriminated on length would read one as
   * another.
   *
   * Only two of the four control messages belong on a consumer channel. An acknowledgement and a
   * retransmission request are messages this handler *sends*; receiving one means the peer has this
   * channel's direction backwards, and it is refused with the typed unexpected-type condition
   * rather than applied to a ledger it does not describe.
   */
  private def dispatch(message: StreamingShuffleMessage): Unit = {
    StreamingShuffleMessage.checkStreamContext(message, shuffleId, message.partitionId())
    if (!backpressure.observeProtocolVersion(
        shuffleId, message.partitionId(), message.protocolVersion())) {
      StreamingShuffleMessage.checkProtocolVersion(message.protocolVersion())
    }
    message match {
      case block: DataBlockMessage =>
        handleDataBlock(block)
      case heartbeat: HeartbeatMessage =>
        handleHeartbeat(heartbeat)
      case termination: StreamTerminationMessage =>
        handleTermination(termination)
      case ack: AckMessage =>
        refuseOutboundOnlyMessage(ack, StreamingShuffleMessageType.ACK)
      case request: RetransmitRequestMessage =>
        refuseOutboundOnlyMessage(request, StreamingShuffleMessageType.RETRANSMIT_REQUEST)
      case other =>
        escalate(
          StreamingShuffleErrors.unexpectedMessageType(
            STREAMING_MESSAGE_DESCRIPTION, describeClassOf(other)),
          other.partitionId())
    }
  }

  /**
   * Accepts a data block, or repairs it.
   *
   * The order of the three steps is deliberate.
   *
   *  1. Liveness and volume are recorded first, before anything can reject the block. Arrival is
   *     itself proof that the producer is alive, and a consumer that only credited a block it went
   *     on to accept would declare a healthy producer dead five seconds after the first corrupt
   *     frame.
   *  2. Sequence integrity. A block above the expected position means blocks were lost, which is
   *     repairable while they are still retained; a block below it is a replay of something already
   *     accepted and is discarded, because treating a duplicate as a fault would make the repair
   *     mechanism this very file implements the cause of a failure.
   *  3. Checksum. Verified here as an early reject so that a corrupt block can be replaced by a
   *     replay before the reduce task ever sees it. The reader performs the authoritative
   *     verification on the task thread; this one exists to make the repair possible, not to
   *     replace it.
   */
  private def handleDataBlock(block: DataBlockMessage): Unit = {
    val partitionId = block.partitionId()
    val sequenceNumber = block.sequenceNumber()
    val payloadLength = block.payloadLength()
    val state = partitionStateOf(partitionId)

    state.lastInboundMillis.set(clock.getTimeMillis())
    backpressure.onDataReceived(shuffleId, partitionId, sequenceNumber, payloadLength.toLong)

    val expected = state.expectedSequenceNumber.get()
    if (sequenceNumber > expected) {
      repairSequenceGap(state, expected, sequenceNumber)
    } else if (sequenceNumber < expected) {
      if (debugEnabled) {
        logDebug(log"Streaming shuffle discarded a duplicate block " +
          log"${MDC(BLOCK_ID, blockIdOf(partitionId, sequenceNumber))} of shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} because position " +
          log"${MDC(COUNT, expected)} has already been accepted")
      }
      state.duplicateBlocks.incrementAndGet()
    } else if (!block.verifyChecksum()) {
      repairCorruptBlock(state, block)
    } else {
      state.expectedSequenceNumber.set(sequenceNumber + 1L)
      state.highestReceivedSequenceNumber.set(sequenceNumber)
      state.receivedBytes.addAndGet(payloadLength.toLong)
      state.acceptedBlocks.incrementAndGet()
      enqueue(BlockReceived(block), payloadLength.toLong)
      if (debugEnabled) {
        logDebug(log"Streaming shuffle accepted block " +
          log"${MDC(BLOCK_ID, blockIdOf(partitionId, sequenceNumber))} of " +
          log"${MDC(NUM_BYTES, payloadLength)} byte(s) for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, partitionId)}")
      }
    }
  }

  /**
   * Repairs, or escalates, a gap in a partition's block sequence.
   *
   * The out-of-order block is not delivered: splicing it in ahead of the blocks that should have
   * preceded it would reorder the reduce partition, which no checksum would ever detect. The window
   * asked for instead spans the missing positions together with this one, inclusively, so the
   * producer replays the run and the consumer resumes exactly where it left off.
   */
  private def repairSequenceGap(state: PartitionState, expected: Long, actual: Long): Unit = {
    val failure =
      StreamingShuffleErrors.invalidSequenceNumber(shuffleId, state.partitionId, expected, actual)
    if (requestRetransmission(state.partitionId, expected, actual)) {
      state.repairedGaps.incrementAndGet()
      logWarning(log"Streaming shuffle asked shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, state.partitionId)} to replay position(s) " +
        log"${MDC(COUNT, expected)} through ${MDC(THRESHOLD, actual)} after a gap in the block " +
        log"sequence")
    } else {
      escalate(failure, state.partitionId)
    }
  }

  /**
   * Repairs, or escalates, a block that failed verification.
   *
   * Repair is bounded exactly by what the producer is still holding. Inside the unacknowledged
   * window the bytes exist and a single-position replay restores them. Outside it they were
   * released the moment this consumer acknowledged them, so no replay can succeed and correctness
   * has to be recovered the other way: the failure is recorded, the reduce task raises a fetch
   * failure from it, and the unmodified scheduler recomputes the upstream stage.
   *
   * The recomputation of the checksum on this path is deliberate and costs nothing on the happy
   * path, where `verifyChecksum` has already answered without copying. Here both numbers are
   * needed, because the typed condition reports the value the block arrived with alongside the
   * value this executor computed, and a diagnostic naming only one of them says nothing about the
   * other.
   */
  private def repairCorruptBlock(state: PartitionState, block: DataBlockMessage): Unit = {
    val partitionId = state.partitionId
    val sequenceNumber = block.sequenceNumber()
    val blockId = blockIdOf(partitionId, sequenceNumber)
    val expectedChecksum = block.checksum()
    val computedChecksum = StreamingShuffleChecksum.computeBlock(
      shuffleId, partitionId, sequenceNumber, block.copyPayload())
    val failure = StreamingShuffleErrors.checksumVerificationFailed(
      blockId, shuffleId, expectedChecksum, computedChecksum)
    state.corruptBlocks.incrementAndGet()
    if (requestRetransmission(partitionId, sequenceNumber, sequenceNumber)) {
      logWarning(log"Streaming shuffle block ${MDC(BLOCK_ID, blockId)} of shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} failed verification with checksum " +
        log"${MDC(CHECKSUM, computedChecksum)} against an expected " +
        log"${MDC(THRESHOLD, expectedChecksum)} and a replay has been requested")
    } else {
      logWarning(log"Streaming shuffle block ${MDC(BLOCK_ID, blockId)} of shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} failed verification and can no longer be replayed, so " +
        log"the fetch must fail and the upstream stage be recomputed")
      escalate(failure, partitionId)
    }
  }

  /**
   * Records a producer heartbeat.
   *
   * The protocol keeps the sender's timestamp for diagnostics and judges liveness from the local
   * instant of arrival, because two hosts do not agree on the wall clock and a detector built on a
   * remote reading would mistake skew for a failure.
   */
  private def handleHeartbeat(heartbeat: HeartbeatMessage): Unit = {
    val state = partitionStateOf(heartbeat.partitionId())
    state.lastInboundMillis.set(clock.getTimeMillis())
    state.remoteHeartbeatMillis.set(heartbeat.timestampMs())
    backpressure.onHeartbeat(heartbeat)
    if (debugEnabled) {
      logDebug(log"Streaming shuffle received a heartbeat for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, heartbeat.partitionId())} stamped at " +
        log"${MDC(TIMEOUT, heartbeat.timestampMs())}")
    }
  }

  /**
   * Records an orderly end of stream.
   *
   * This is what distinguishes silence that means completion from silence that means failure. A
   * terminated stream stops arming the liveness timers, so a partition that legitimately produced
   * nothing at all -- announced with a total of zero blocks, which is entirely valid -- is never
   * mistaken for a producer that died five seconds ago. Completion is delivered as a marker on the
   * queue rather than left to be inferred from a timeout, so the reduce task learns of it at once.
   */
  private def handleTermination(termination: StreamTerminationMessage): Unit = {
    val partitionId = termination.partitionId()
    val totalBlocks = termination.totalBlocks()
    val state = partitionStateOf(partitionId)
    state.lastInboundMillis.set(clock.getTimeMillis())
    state.announcedBlocks.set(totalBlocks)
    state.terminated.set(true)
    backpressure.onStreamTermination(termination)
    if (state.completionSignalled.compareAndSet(false, true)) {
      enqueue(StreamCompleted(partitionId, totalBlocks), 0L)
    }
    if (debugEnabled) {
      logDebug(log"Streaming shuffle stream for shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} ended after ${MDC(COUNT, totalBlocks)} block(s)")
    }
  }

  /**
   * Refuses a control message that only ever travels in the other direction. Named with the type
   * that was expected in its place, so the diagnostic states what the channel is for rather than
   * only what arrived on it.
   */
  private def refuseOutboundOnlyMessage(
      message: StreamingShuffleMessage,
      actual: StreamingShuffleMessageType): Unit = {
    escalate(
      StreamingShuffleErrors.unexpectedMessageType(CONSUMER_INBOUND_DESCRIPTION, actual.name()),
      message.partitionId())
  }

  // ==========================================================================================
  // The hand-off queue, read by the reduce task's own thread.
  // ==========================================================================================

  /**
   * Places one event on the hand-off queue.
   *
   * A closed handler retains nothing: the reduce task it was serving has gone, so a block arriving
   * after cleanup is dropped rather than held, which is what keeps a late frame from outliving the
   * task that asked for it.
   *
   * The queue cannot be allowed to fill, and the design ensures it does not: the receive window
   * closes at a high-water mark well below the bound, and the producer's credit limit caps what can
   * already be in flight. If it nevertheless does, the event loop has a block it can neither hand
   * on nor discard, and it must not block. The failure is recorded instead, which fails the reduce
   * task and lets the upstream stage be recomputed -- slower than a transfer that worked, but never
   * a partition that silently lost a block.
   */
  private def enqueue(event: Inbound, payloadBytes: Long): Unit = {
    if (closed.get()) {
      if (debugEnabled) {
        logDebug(log"Streaming shuffle dropped an inbound event for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} partition " +
          log"${MDC(PARTITION_ID, event.partitionId)} because the handler is closed")
      }
    } else if (inbound.offer(event)) {
      if (payloadBytes > 0L) {
        queuedPayloadBytes.addAndGet(payloadBytes)
      }
    } else {
      escalate(
        new IllegalStateException(s"The streaming shuffle hand-off queue of shuffle $shuffleId " +
          s"partition ${event.partitionId} is full at ${inbound.size()} event(s) holding " +
          s"${queuedPayloadBytes.get()} byte(s); the receive window should have closed at " +
          s"$INBOUND_QUEUE_HIGH_WATER_MARK event(s) or $INBOUND_HIGH_WATER_BYTES byte(s)"),
        event.partitionId)
    }
  }

  /**
   * Takes the next inbound event without waiting, or `None` when nothing has arrived yet.
   *
   * Intended for the reduce task's thread. Taking an event frees the heap it was holding, which is
   * the condition that may reopen the receive window, so the window is re-evaluated here rather
   * than left until the next frame arrives -- a channel throttled to a standstill has no next frame
   * to be left until.
   */
  def poll(): Option[Inbound] = accountForPolled(inbound.poll())

  /**
   * Takes the next inbound event, waiting at most `timeoutMillis` for one.
   *
   * Must never be called from a Netty event-loop thread: it is the one method in this class that
   * blocks, and it does so on purpose, for the reduce task that has nothing to do until input
   * arrives. A non-positive timeout degrades to [[poll()]].
   */
  def poll(timeoutMillis: Long): Option[Inbound] = {
    if (timeoutMillis <= 0L) {
      poll()
    } else {
      accountForPolled(inbound.poll(timeoutMillis, TimeUnit.MILLISECONDS))
    }
  }

  /**
   * Discounts a taken event from the queue's byte accounting and re-evaluates the receive window.
   * Null in means an empty queue, which is not an event and must not move the accounting.
   */
  private def accountForPolled(event: Inbound): Option[Inbound] = {
    if (event == null) {
      None
    } else {
      event match {
        case received: BlockReceived =>
          queuedPayloadBytes.addAndGet(-received.block.payloadLength().toLong)
        case _ =>
          ()
      }
      evaluateAutoRead()
      Some(event)
    }
  }

  /** How many inbound events are waiting to be taken. */
  def queuedEventCount: Int = inbound.size()

  /** How many payload bytes those events are holding. */
  def queuedByteCount: Long = queuedPayloadBytes.get()

  // ==========================================================================================
  // Outbound: acknowledgements, replay requests and heartbeats.
  // ==========================================================================================

  /**
   * Acknowledges everything up to and including `consumedSequenceNumber` for one partition.
   *
   * This is the single event that frees producer memory and lifts backpressure, and it is why the
   * feature's hundred-millisecond reclamation bound is achievable at all: the producer releases the
   * buffers behind every block at or below the acknowledged position the moment this message lands.
   * Acknowledgements are monotonic, so a position at or below the one already sent is ignored
   * rather than treated as a regression -- which is what makes this safe to call on every iterator
   * advance without the caller having to remember how far it had got.
   *
   * The local ledger is advanced before the message is written, so the consumer's own view of its
   * outstanding volume is correct even if the write has to be deferred. The protocol is told as
   * well, because its `hasCredit` answer is what reopens the receive window.
   *
   * A channel that is not writable does not get written to: the position is held instead, coalesced
   * to one number per partition, and flushed when writability returns. Growing Netty's outbound
   * queue while throttling the inbound side would defeat the purpose of throttling it.
   *
   * @param partitionId the reduce partition being acknowledged
   * @param consumedSequenceNumber the highest block position the task has consumed, or
   *                               `AckMessage.NOTHING_CONSUMED` to state that it has consumed
   *                               nothing
   * @return true if this call advanced the acknowledged position
   */
  def acknowledge(partitionId: Int, consumedSequenceNumber: Long): Boolean = {
    require(partitionId >= 0 || partitionId == UNKNOWN_PARTITION_ID,
      s"The partition id must be non-negative but was $partitionId.")
    require(consumedSequenceNumber >= AckMessage.NOTHING_CONSUMED,
      s"The consumed position must be at least ${AckMessage.NOTHING_CONSUMED} but was " +
        s"$consumedSequenceNumber.")
    val state = partitionStateOf(partitionId)
    val previous = state.acknowledgedPosition.get()
    if (consumedSequenceNumber <= previous) {
      false
    } else {
      state.acknowledgedPosition.set(consumedSequenceNumber)
      state.acknowledgedBytes.set(state.receivedBytes.get())
      // The protocol releases the credit these blocks held and leaves the throttled state, which is
      // exactly the transition that lets the receive window reopen below.
      backpressure.onAck(shuffleId, partitionId, consumedSequenceNumber)
      val channel = channelRef.get()
      if (channel != null && channel.isActive && channel.isWritable) {
        writeMessage(channel, new AckMessage(shuffleId, partitionId,
          state.outboundSequenceNumber.getAndIncrement(), consumedSequenceNumber))
      } else {
        state.pendingAckPosition.set(consumedSequenceNumber)
        if (debugEnabled) {
          logDebug(log"Streaming shuffle deferred an acknowledgement at position " +
            log"${MDC(COUNT, consumedSequenceNumber)} for shuffle " +
            log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, partitionId)} " +
            log"because the channel is not writable")
        }
      }
      evaluateAutoRead()
      true
    }
  }

  /**
   * Writes every acknowledgement that was held back, in partition order so the sequence of writes
   * is deterministic. Each partition contributes at most one message, because a deferred position
   * supersedes any earlier one outright.
   */
  private def flushDeferredAcks(): Unit = {
    val channel = channelRef.get()
    if (channel != null && channel.isActive && channel.isWritable) {
      partitions.values().asScala.toSeq.sortBy(_.partitionId).foreach { state =>
        val pending = state.pendingAckPosition.getAndSet(AckMessage.NOTHING_CONSUMED)
        if (pending > AckMessage.NOTHING_CONSUMED) {
          writeMessage(channel, new AckMessage(shuffleId, state.partitionId,
            state.outboundSequenceNumber.getAndIncrement(), pending))
        }
      }
    }
  }

  /**
   * Asks the producer to replay an inclusive run of block positions.
   *
   * Both ends are inclusive and a single-position window is the normal case, being what a corrupt
   * block calls for. The request is only sent when it can actually be served, which is what makes
   * repair bounded rather than a retry that cannot succeed.
   *
   *  - Every position asked for must still be retained by the producer. A position this consumer
   *    has not acknowledged is retained by construction, because acknowledgement is the only thing
   *    that releases it; the protocol's own view of the unacknowledged window is consulted as well,
   *    for the case where this executor is keeping that ledger.
   *  - The replay budget must not be spent. The protocol caps attempts at five with a pause growing
   *    from one second, and refuses to hand out a sixth, at which point the failure has to be
   *    escalated instead.
   *  - The window must fit what the message type permits, so an over-wide request is narrowed to
   *    the widest legal one rather than being rejected outright: a narrower replay still makes
   *    progress, and the next gap is repaired the same way.
   *
   * @return true if a request was written, false if the caller must escalate instead
   */
  def requestRetransmission(partitionId: Int, firstSequenceNumber: Long,
      lastSequenceNumber: Long): Boolean = {
    val channel = channelRef.get()
    val bounded = math.min(
      math.max(firstSequenceNumber, lastSequenceNumber),
      firstSequenceNumber + MAX_REPLAY_WINDOW_SPAN)
    if (closed.get() || channel == null || !channel.isActive) {
      false
    } else if (firstSequenceNumber < 0L) {
      false
    } else if (!isRetained(partitionId, firstSequenceNumber) || !isRetained(partitionId, bounded)) {
      false
    } else if (backpressure.isRetryExhausted(shuffleId, partitionId)) {
      logWarning(log"Streaming shuffle will not ask shuffle ${MDC(SHUFFLE_ID, shuffleId)} " +
        log"partition ${MDC(PARTITION_ID, partitionId)} to replay position " +
        log"${MDC(COUNT, firstSequenceNumber)} because the replay budget of " +
        log"${MDC(THRESHOLD, BackpressureProtocol.MAX_RETRY_ATTEMPTS)} attempt(s) is spent")
      false
    } else {
      val state = partitionStateOf(partitionId)
      state.replayRequests.incrementAndGet()
      writeMessage(channel, new RetransmitRequestMessage(
        shuffleId, partitionId, firstSequenceNumber, bounded))
    }
  }

  /**
   * Whether a block position is still retained by the producer and can therefore be replayed.
   *
   * Two readings, either of which is sufficient. The protocol answers directly wherever this
   * executor keeps the stream's ledger. Failing that, the consumer's own books answer it: a
   * position above the highest one this consumer has acknowledged has not been released, because
   * acknowledgement is the only thing that releases it.
   */
  private def isRetained(partitionId: Int, sequenceNumber: Long): Boolean = {
    backpressure.isWithinUnacknowledgedWindow(shuffleId, partitionId, sequenceNumber) ||
      sequenceNumber > partitionStateOf(partitionId).acknowledgedPosition.get()
  }

  /**
   * Sends a heartbeat for one partition if the five-second interval has elapsed.
   *
   * The interval and the "already sent" bookkeeping belong to the protocol wherever it keeps a
   * ledger for the stream, so that one notion of time governs both sides of the liveness question.
   * Where it keeps none, this handler's own interval applies and the message is stamped from the
   * injected clock; the heartbeat type reads no clock of its own, which is what allows a suite to
   * freeze time and still exercise the timer.
   *
   * @return true if a heartbeat was written
   */
  def sendHeartbeatIfDue(partitionId: Int): Boolean = {
    if (closed.get() || backpressure.isStreamTerminated(shuffleId, partitionId)) {
      false
    } else if (backpressure.shouldSendHeartbeat(shuffleId, partitionId)) {
      val stamped = backpressure.heartbeatFor(shuffleId, partitionId)
      stamped.exists(heartbeat => writeHeartbeat(partitionId, heartbeat))
    } else if (backpressure.isStreamRegistered(shuffleId, partitionId)) {
      // The protocol owns the interval for a registered stream and has just said it is not due.
      false
    } else {
      val state = partitionStateOf(partitionId)
      val nowMillis = clock.getTimeMillis()
      val lastSent = state.lastHeartbeatSentMillis.get()
      if (lastSent != NO_TIMESTAMP &&
          nowMillis - lastSent < BackpressureProtocol.HEARTBEAT_INTERVAL_MS) {
        false
      } else {
        writeHeartbeat(partitionId, buildHeartbeat(state, nowMillis))
      }
    }
  }

  /**
   * Builds a heartbeat from the injected clock, positioned at how far this consumer has got.
   *
   * The position is clamped at zero and so is the timestamp, because the message type refuses a
   * negative value for either and a wall clock adjusted backwards past the epoch would otherwise
   * construct a message it rejects.
   */
  private def buildHeartbeat(state: PartitionState, nowMillis: Long): HeartbeatMessage = {
    new HeartbeatMessage(
      shuffleId,
      state.partitionId,
      math.max(0L, state.highestReceivedSequenceNumber.get()),
      math.max(0L, nowMillis))
  }

  /** Writes a heartbeat and restarts this handler's own interval for the partition. */
  private def writeHeartbeat(partitionId: Int, heartbeat: HeartbeatMessage): Boolean = {
    val channel = channelRef.get()
    val written = channel != null && writeMessage(channel, heartbeat)
    if (written) {
      partitionStateOf(partitionId).lastHeartbeatSentMillis.set(clock.getTimeMillis())
    }
    written
  }

  /**
   * Writes one framed message.
   *
   * The framing is the type byte followed by the encoded body, which is exactly what the decoder on
   * the other end consumes, so encode and decode cannot drift apart. Netty releases the buffer once
   * the write completes or fails; the buffer is released here only if the write call itself never
   * got far enough to take ownership of it, which is the one path on which Netty would not.
   *
   * @return true if the message was handed to the channel
   */
  private def writeMessage(channel: Channel, message: StreamingShuffleMessage): Boolean = {
    if (!channel.isActive) {
      false
    } else {
      val framed = Unpooled.wrappedBuffer(message.toByteBuffer())
      var accepted = false
      try {
        channel.writeAndFlush(framed)
        accepted = true
      } catch {
        case NonFatal(e) =>
          escalate(e, message.partitionId())
      } finally {
        if (!accepted) {
          releaseQuietly(framed)
        }
      }
      accepted
    }
  }

  // ==========================================================================================
  // Failure handling and the bridge to the task thread.
  // ==========================================================================================

  /**
   * Records a failure for the task thread to raise, and wakes a task that is waiting for input.
   *
   * This is the only route by which a failure observed on a Netty thread reaches the reduce task,
   * and it deliberately does not construct a fetch failure. That exception registers itself on the
   * task context as it is built, and the context is a thread-local of the task thread, so one
   * constructed here would register nothing at all; it also has to be thrown in the same expression
   * that creates it. Both are reasons the reader builds and throws it, on the task thread, from the
   * cause recorded here.
   *
   * The marker placed on the queue is what makes the failure prompt rather than eventual: without
   * it a task blocked on the queue would wait out its whole timeout before it thought to look at
   * the notifier.
   *
   * Log volume is bounded rather than proportional to the misbehaviour of a peer: a peer sending
   * malformed frames produces one failure per frame, so the first few are reported at warning level
   * and the rest at debug. Nothing is lost by that -- the notifier keeps the root cause and counts
   * what it dropped.
   */
  private def escalate(cause: Throwable, partitionId: Int): Unit = {
    errorNotifier.setError(cause)
    val reported = escalationsReported.incrementAndGet()
    if (reported <= MAX_REPORTED_ESCALATIONS) {
      logWarning(log"Streaming shuffle consumer failed for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, partitionId)}: " +
        log"${MDC(ERROR, cause.getMessage)}", cause)
    } else if (debugEnabled) {
      logDebug(log"Streaming shuffle consumer failed again for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, partitionId)} after " +
        log"${MDC(COUNT, reported)} reported failure(s): ${MDC(ERROR, cause.getMessage)}")
    }
    signalProducerLost(partitionId, cause.getClass.getSimpleName)
  }

  /**
   * Marks one partition's producer as gone, at most once, and wakes whoever is waiting on it.
   *
   * The invalidation of the blocks already accepted from that producer, and the fetch failure that
   * makes the scheduler recompute the upstream stage, belong to the reader: they have to be atomic
   * across everything the task has taken from this queue as well as everything still on it, and
   * only the task thread can see both. This handler's part is to say that no more is coming.
   */
  private def signalProducerLost(partitionId: Int, reason: String): Unit = {
    val state = partitionStateOf(partitionId)
    if (state.producerLostSignalled.compareAndSet(false, true)) {
      producerLostAtMillis.compareAndSet(NO_TIMESTAMP, clock.getTimeMillis())
      enqueue(ProducerLost(partitionId, reason), 0L)
    }
  }

  // ==========================================================================================
  // Observability, all of it derived from state this handler already keeps.
  // ==========================================================================================

  /** Whether this channel is currently reading from its socket. */
  def isAutoReadEnabled: Boolean = autoReadEnabled.get()

  /** Why reading is currently stopped, or `None` when it is not. */
  def currentThrottleCause: Option[String] = Option(throttleCause.get())

  /**
   * Transitions into the throttled state observed on this channel. The executor-wide
   * `shuffle.streaming.backpressureEvents` counter is advanced by [[BackpressureProtocol]] and
   * never from here, so this is a per-channel reading and not a second copy of that metric.
   */
  def throttleTransitionCount: Long = throttleTransitions.get()

  /** Whether the producer of this channel has been declared gone. */
  def isProducerLost: Boolean = producerLostAtMillis.get() != NO_TIMESTAMP

  /**
   * Milliseconds since the producer was declared gone, read from the injected clock, or `None`
   * while it is still believed alive.
   */
  def producerLostElapsedMillis: Option[Long] = {
    val lostAt = producerLostAtMillis.get()
    if (lostAt == NO_TIMESTAMP) None else Some(math.max(0L, clock.getTimeMillis() - lostAt))
  }

  /**
   * Whether nothing at all has been received for one partition for longer than the five-second
   * connection timeout, which is the detection the reader turns into a partial-read invalidation
   * and a fetch failure.
   *
   * A terminated stream is never silent: its quiet means completion, and recomputing a stage that
   * had in fact finished would be the worst possible response to it. Both this handler's own
   * reading and the protocol's are consulted, so a stream the protocol never learned about is still
   * covered.
   */
  def isProducerSilent(partitionId: Int): Boolean = {
    val state = partitions.get(Integer.valueOf(partitionId))
    if (state == null || state.terminated.get()) {
      false
    } else {
      val lastInbound = state.lastInboundMillis.get()
      val silentLocally = lastInbound != NO_TIMESTAMP &&
        clock.getTimeMillis() - lastInbound >= PRODUCER_CONNECTION_TIMEOUT_MS
      silentLocally || backpressure.isProducerTimedOut(shuffleId, partitionId)
    }
  }

  /** Milliseconds since anything was received for one partition, or `None` for an unknown one. */
  def millisSinceInbound(partitionId: Int): Option[Long] = {
    val state = partitions.get(Integer.valueOf(partitionId))
    if (state == null || state.lastInboundMillis.get() == NO_TIMESTAMP) {
      None
    } else {
      Some(math.max(0L, clock.getTimeMillis() - state.lastInboundMillis.get()))
    }
  }

  /** Whether the producer has announced the orderly end of one partition's stream. */
  def isStreamComplete(partitionId: Int): Boolean = {
    val state = partitions.get(Integer.valueOf(partitionId))
    state != null && state.terminated.get()
  }

  /**
   * Blocks the producer claims to have sent for one partition, or `None` before it has said. A
   * returned zero is a genuine reading and means the partition was empty.
   */
  def announcedBlockCount(partitionId: Int): Option[Long] = {
    val state = partitions.get(Integer.valueOf(partitionId))
    if (state == null || state.announcedBlocks.get() == NO_BLOCK_TOTAL) {
      None
    } else {
      Some(state.announcedBlocks.get())
    }
  }

  /** The next block position this handler expects for one partition. */
  def expectedSequenceNumber(partitionId: Int): Long =
    partitionStateOf(partitionId).expectedSequenceNumber.get()

  /** The highest position acknowledged for one partition, or the nothing-consumed sentinel. */
  def acknowledgedPosition(partitionId: Int): Long =
    partitionStateOf(partitionId).acknowledgedPosition.get()

  /** Blocks accepted and handed on for one partition. */
  def acceptedBlockCount(partitionId: Int): Long =
    partitionStateOf(partitionId).acceptedBlocks.get()

  /** Blocks refused for one partition because they failed verification. */
  def corruptBlockCount(partitionId: Int): Long =
    partitionStateOf(partitionId).corruptBlocks.get()

  /** Replays requested for one partition, whether for a gap or for corruption. */
  def replayRequestCount(partitionId: Int): Long =
    partitionStateOf(partitionId).replayRequests.get()

  /** Blocks discarded for one partition because they had already been accepted. */
  def duplicateBlockCount(partitionId: Int): Long =
    partitionStateOf(partitionId).duplicateBlocks.get()

  /** The partitions this channel has seen traffic for, in a deterministic order. */
  def observedPartitionIds: Seq[Int] =
    partitions.values().asScala.map(_.partitionId).toSeq.sorted

  /** Partitions that have neither terminated nor had their producer declared gone. */
  private def partitionsAwaitingData: Seq[Int] = {
    partitions.values().asScala
      .filter(state => !state.terminated.get() && !state.producerLostSignalled.get())
      .map(_.partitionId)
      .toSeq
      .sorted
  }

  /**
   * The transport configuration this channel is tuned by, drawn from the streaming module's own
   * `spark.shuffle-streaming.io.*` namespace rather than from the settings block transfer uses.
   */
  def streamingTransportConf: TransportConf = transportConf

  /** Whether OS-level keepalive is enabled for the streaming module. */
  def isTcpKeepAliveEnabled: Boolean = tcpKeepAliveEnabled

  /** Transport-level replay attempts permitted for this module. */
  def maxTransportRetries: Int = maxIoRetries

  /** Pause between transport-level replay attempts, in milliseconds. */
  def transportRetryWaitMillis: Int = ioRetryWaitMillis

  // ==========================================================================================
  // Cleanup.
  // ==========================================================================================

  /**
   * Releases everything this handler holds, once.
   *
   * Idempotent by compare-and-set, because the reader registers it on
   * `TaskContext.addTaskCompletionListener` and it therefore runs on success, on failure and on
   * cancellation alike, possibly alongside a channel that is closing for its own reasons. It is
   * never registered from a Netty thread; the reader owns that registration, on the task thread.
   *
   * The queue is drained and every event released before the channel is closed, so nothing is left
   * reachable from a handler the pipeline has let go. Release is a formality for the events this
   * handler enqueues -- a decoded block owns a heap array, not a pooled buffer -- and it is
   * performed anyway, so that the zero-leak guarantee holds by construction rather than by anyone
   * remembering that it currently happens to. The test JVM fails a leaked buffer outright, which is
   * what makes that guarantee machine-checked.
   *
   * `setAutoRead` is deliberately not touched here: reopening the receive window of a channel that
   * is about to close could only pull in bytes nobody will read.
   */
  def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      val drained = drainAndRelease()
      partitions.clear()
      val channel = channelRef.getAndSet(null)
      if (channel != null) {
        channel.close()
      }
      if (debugEnabled) {
        logDebug(log"Streaming shuffle consumer for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} released ${MDC(COUNT, drained)} queued event(s) " +
          log"after ${MDC(THRESHOLD, throttleTransitions.get())} throttling transition(s)")
      }
    }
  }

  /** Whether [[close]] has run. */
  def isClosed: Boolean = closed.get()

  /**
   * Empties the hand-off queue, releasing each event and anything reference-counted it holds, and
   * rewinds the byte accounting.
   *
   * @return how many events were discarded
   */
  private def drainAndRelease(): Int = {
    var discarded = 0
    var event = inbound.poll()
    while (event != null) {
      event match {
        case received: BlockReceived =>
          releaseQuietly(received.block)
        case _ =>
          ()
      }
      releaseQuietly(event)
      discarded += 1
      event = inbound.poll()
    }
    inbound.clear()
    queuedPayloadBytes.set(0L)
    discarded
  }

  // ==========================================================================================
  // Small helpers.
  // ==========================================================================================

  /**
   * The bookkeeping for one partition, created on first sight.
   *
   * Written without a lambda so that the common path -- a partition already seen -- is a single map
   * read with no allocation at all, which matters because it runs once per frame.
   */
  private def partitionStateOf(partitionId: Int): PartitionState = {
    val key = Integer.valueOf(partitionId)
    val existing = partitions.get(key)
    if (existing != null) {
      existing
    } else {
      val created = new PartitionState(partitionId)
      val raced = partitions.putIfAbsent(key, created)
      if (raced == null) created else raced
    }
  }

  /**
   * Releases a reference-counted object, absorbing anything that goes wrong.
   *
   * A failure to release is a leak and not a correctness fault, and it must not become the reason a
   * Netty callback unwinds or the reason cleanup stops halfway through a queue. Objects that are
   * not reference-counted pass through untouched.
   */
  private def releaseQuietly(obj: AnyRef): Unit = {
    if (obj != null) {
      try {
        ReferenceCountUtil.release(obj)
      } catch {
        case NonFatal(e) =>
          if (debugEnabled) {
            logDebug(log"Streaming shuffle could not release an inbound object for shuffle " +
              log"${MDC(SHUFFLE_ID, shuffleId)}: ${MDC(ERROR, e.getMessage)}")
          }
      }
    }
  }

  /**
   * A stable identifier for one streaming block, for diagnostics only.
   *
   * Deliberately not a `BlockId`: the streaming path introduces no new block-identity type, because
   * the block-manager storage contracts are a preservation zone. The prefix names the subsystem so
   * that the three numbers cannot be mistaken for the map and reduce ids of a materialised shuffle
   * block.
   */
  private def blockIdOf(partitionId: Int, sequenceNumber: Long): String =
    s"streaming_shuffle_${shuffleId}_${partitionId}_$sequenceNumber"

  /** The partition an inbound object names, or the unknown sentinel when it names none. */
  private def partitionIdOf(msg: Object): Int = msg match {
    case message: StreamingShuffleMessage => message.partitionId()
    case _ => UNKNOWN_PARTITION_ID
  }

  /** The class of an unexpected inbound object, named without dereferencing it. */
  private def describeClassOf(obj: Any): String =
    if (obj == null) "null" else obj.getClass.getName
}

/**
 * Constants, per-partition bookkeeping and the envelope type the consumer channel hands to the
 * reduce task.
 *
 * The envelope is this handler's own delivery type and not a second rendering of any wire message:
 * a received block travels inside it exactly as the codec produced it, and the other two cases
 * carry information no wire message has, namely that a stream has finished and that a producer has
 * gone.
 */
private[spark] object StreamingShuffleClientHandler {

  /**
   * Transport module name for streaming shuffle, passed as an argument to
   * `SparkTransportConf.fromSparkConf` and never added to a shared file. It yields the independent
   * `spark.shuffle-streaming.io.*` tuning namespace, which is what lets thread counts, retry policy
   * and OS keepalive be set for streaming alone.
   */
  val TRANSPORT_MODULE: String = "shuffle-streaming"

  /**
   * Silence after which a producer is declared gone. Enforced at application level, from the
   * injected clock, because the transport exposes keepalive as a boolean with no interval and the
   * JDK offers no keepalive-interval socket option.
   */
  val PRODUCER_CONNECTION_TIMEOUT_MS: Long = BackpressureProtocol.ACK_TIMEOUT_MS

  /**
   * Hard bound on the hand-off queue. Never reached in practice: the receive window closes at the
   * high-water mark below, and the producer's credit limit caps what can already be in flight.
   */
  val INBOUND_QUEUE_CAPACITY: Int = 32

  /** Queued events at which the receive window closes. */
  val INBOUND_QUEUE_HIGH_WATER_MARK: Int = 8

  /** Queued events at which it may reopen, low enough that a slow task cannot make it flap. */
  val INBOUND_QUEUE_LOW_WATER_MARK: Int = 4

  /**
   * Queued payload bytes at which the receive window closes, sixteen mebibytes.
   *
   * Counted as well as the events themselves because a block carries anything from nothing up to
   * the two-mebibyte cap, so a count of events is a poor description of the heap the queue is
   * holding.
   */
  val INBOUND_HIGH_WATER_BYTES: Long = 8L * DataBlockMessage.MAX_BLOCK_SIZE_BYTES

  /** Queued payload bytes at which it may reopen, four mebibytes. */
  val INBOUND_LOW_WATER_BYTES: Long = 2L * DataBlockMessage.MAX_BLOCK_SIZE_BYTES

  /**
   * Widest span a single replay request may name, so that an over-wide window is narrowed rather
   * than refused. One below the message type's own maximum, because the window includes both of its
   * ends.
   */
  val MAX_REPLAY_WINDOW_SPAN: Long = RetransmitRequestMessage.MAX_REQUESTED_BLOCKS - 1L

  /**
   * Escalated failures reported at warning level before the rest drop to debug. A peer sending
   * malformed frames produces one failure per frame, so the ceiling is what keeps the log budget
   * independent of how badly a peer misbehaves.
   */
  val MAX_REPORTED_ESCALATIONS: Long = 4L

  /** Sentinel for a timestamp that has not been taken. */
  val NO_TIMESTAMP: Long = Long.MinValue

  /** Sentinel for a block total the producer has not announced. */
  val NO_BLOCK_TOTAL: Long = -1L

  /**
   * Sentinel partition for a frame refused before its header could be read, and for a failure that
   * belongs to the channel rather than to any one stream.
   */
  val UNKNOWN_PARTITION_ID: Int = -1

  /** Reported cause when the receive window closes because a consumer has no credit left. */
  val THROTTLE_CAUSE_CREDIT: String = "consumer credit is exhausted"

  /** Reported cause when it closes because the reduce task is not keeping up with the queue. */
  val THROTTLE_CAUSE_QUEUE: String = "the hand-off queue reached its high-water mark"

  /** Reason recorded when a producer closes the connection with blocks still owing. */
  val PRODUCER_CLOSED_REASON: String = "the producer closed the connection"

  /** What a consumer channel expects to receive, named in an unexpected-type diagnostic. */
  val CONSUMER_INBOUND_DESCRIPTION: String =
    StreamingShuffleMessageType.DATA_BLOCK.name() + ", " +
      StreamingShuffleMessageType.HEARTBEAT.name() + " or " +
      StreamingShuffleMessageType.STREAM_TERMINATION.name()

  /** What the pipeline is expected to deliver, named when it delivers something else entirely. */
  val STREAMING_MESSAGE_DESCRIPTION: String = classOf[StreamingShuffleMessage].getName

  /**
   * One inbound event on the way from the channel to the reduce task.
   *
   * Three cases, because a reduce task has to be able to tell three outcomes apart: a block
   * arrived, the stream ended, or the producer went away. Only the first carries data; the other
   * two are what let the task stop waiting, and the difference between them is the difference
   * between a completed partition and one that has to be recomputed.
   */
  sealed abstract class Inbound {
    /** The reduce partition this event belongs to, or [[UNKNOWN_PARTITION_ID]]. */
    def partitionId: Int
  }

  /**
   * A verified block, carried exactly as the codec produced it.
   *
   * The block owns a heap array, so nothing reference-counted travels on the queue and the payload
   * can be read as often as the task likes through `payloadBuffer`, which copies nothing. The
   * reader performs the authoritative checksum verification on the task thread; the check the
   * channel already ran is an early reject that exists so a corrupt block can be replaced by a
   * replay before the task ever sees it.
   */
  final case class BlockReceived(block: DataBlockMessage) extends Inbound {
    override def partitionId: Int = block.partitionId()
  }

  /**
   * The orderly end of one partition's stream.
   *
   * A total of zero is valid and means the partition was empty. Delivering completion rather than
   * leaving it to be inferred from silence is what keeps an empty partition from being mistaken for
   * a producer that died five seconds ago.
   */
  final case class StreamCompleted(partitionId: Int, totalBlocks: Long) extends Inbound

  /**
   * The producer of one partition is gone and nothing further is coming.
   *
   * The reader turns this into the atomic invalidation of every block accepted from that producer
   * and into the fetch failure that makes the unmodified scheduler recompute the upstream stage. It
   * is delivered on the queue so that a task blocked on input stops waiting at once rather than
   * after a full timeout on a socket that has already gone.
   */
  final case class ProducerLost(partitionId: Int, reason: String) extends Inbound

  /**
   * Per-partition bookkeeping for one channel.
   *
   * Every cell is an atomic, and for one reason: the writes are all made by this channel's single
   * event-loop thread, which orders them against each other already, while the reads come from the
   * reduce task's thread, which needs to see them. The atomics are there for that visibility, not
   * to order writes that are already ordered, and none of them is ever the subject of a
   * read-modify-write that spans two cells.
   */
  private[streaming] class PartitionState(val partitionId: Int) {

    /** Next block position expected, so a gap or a duplicate is detected rather than spliced. */
    val expectedSequenceNumber = new AtomicLong(0L)

    /** Highest position actually received, which is the position a heartbeat reports. */
    val highestReceivedSequenceNumber = new AtomicLong(NO_BLOCK_TOTAL)

    /** Payload bytes accepted for this partition. */
    val receivedBytes = new AtomicLong(0L)

    /** Payload bytes covered by the acknowledgements already sent. */
    val acknowledgedBytes = new AtomicLong(0L)

    /** Highest position acknowledged, or the nothing-consumed sentinel. */
    val acknowledgedPosition = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    /**
     * An acknowledgement held back because the channel was not writable, coalesced to one position:
     * acknowledgements are monotonic, so a later position supersedes an earlier one outright and
     * what is held is a single number rather than a backlog of messages.
     */
    val pendingAckPosition = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    /**
     * Position stamped into this consumer's own outbound control messages. The consumer's direction
     * has a sequence of its own, independent of the producer's block numbering.
     */
    val outboundSequenceNumber = new AtomicLong(0L)

    /** Instant of the last inbound event of any kind, from the injected clock. */
    val lastInboundMillis = new AtomicLong(NO_TIMESTAMP)

    /** Instant at which this consumer last emitted a heartbeat, from the injected clock. */
    val lastHeartbeatSentMillis = new AtomicLong(NO_TIMESTAMP)

    /** Timestamp the producer stamped into its last heartbeat. Diagnostic only. */
    val remoteHeartbeatMillis = new AtomicLong(NO_TIMESTAMP)

    /** Whether the producer has announced the orderly end of this stream. */
    val terminated = new AtomicBoolean(false)

    /** Blocks the producer claims to have sent, or [[NO_BLOCK_TOTAL]] before it has said. */
    val announcedBlocks = new AtomicLong(NO_BLOCK_TOTAL)

    /** Latch ensuring completion is delivered to the task exactly once. */
    val completionSignalled = new AtomicBoolean(false)

    /** Latch ensuring producer loss is delivered to the task exactly once. */
    val producerLostSignalled = new AtomicBoolean(false)

    /** Blocks accepted and handed on. */
    val acceptedBlocks = new AtomicLong(0L)

    /** Blocks refused because they failed verification. */
    val corruptBlocks = new AtomicLong(0L)

    /** Blocks discarded because they had already been accepted. */
    val duplicateBlocks = new AtomicLong(0L)

    /** Sequence gaps for which a replay was requested. */
    val repairedGaps = new AtomicLong(0L)

    /** Replay requests written, whether for a gap or for corruption. */
    val replayRequests = new AtomicLong(0L)
  }
}
