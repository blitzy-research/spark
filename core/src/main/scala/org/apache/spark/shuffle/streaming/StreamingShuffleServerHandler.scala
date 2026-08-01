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
import java.util.Comparator
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap, PriorityBlockingQueue}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelFuture, ChannelFutureListener, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.util.ReferenceCountUtil

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CONFIG, COUNT, DURATION, ERROR, HOST_PORT, MAX_ATTEMPTS, NUM_BLOCKS, NUM_BYTES, PARTITION_ID, REASON, SHUFFLE_ID, STATUS, TIMEOUT, VALUE}
import org.apache.spark.internal.config.SHUFFLE_STREAMING_DEBUG
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, DataBlockMessage, HeartbeatMessage, RetransmitRequestMessage, StreamingShuffleMessage, StreamingShuffleMessageType, StreamTerminationMessage}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.util.{Clock, SystemClock}

/**
 * The producer side channel handler of the streaming shuffle.
 *
 * This handler owns egress for one shuffle on one channel: it takes the blocks a
 * `StreamingShuffleWriter` has framed, orders them, charges them against the executor's egress
 * budget and puts them on the wire; and it consumes the control traffic the reduce side sends back
 * -- acknowledgements, heartbeats and retransmission requests -- turning acknowledgements into
 * reclaimed producer memory.
 *
 * Together with `StreamingShuffleClientHandler` it is the only place in Spark where channel level
 * flow control idioms live. That containment is deliberate: neither this file nor its consumer side
 * counterpart requires a single edit to the shared transport, so `TransportContext`,
 * `TransportConf` and the shared Netty pipeline are consumed exactly as they stand. The division
 * between the two handlers is equally deliberate -- toggling `Channel.setAutoRead` to exert TCP
 * level backpressure belongs to the consumer side handler alone, and appears nowhere in this file.
 *
 * =Three layers of backpressure=
 *
 * The streaming shuffle applies three, and this handler participates in exactly two of them:
 *
 *  - application level credit derived from consumer acknowledgements, whose ledger is owned by
 *    `BackpressureProtocol`. This handler reports the events that ledger is built from and never
 *    duplicates it;
 *  - rate limiting, owned by [[TokenBucketRateLimiter]]: every data block is charged against the
 *    bucket before it is written, and a refusal means the block is held and retried rather than
 *    dropped. The charge is taken through the protocol's admission call, which holds the same
 *    bucket, so a block's bytes are debited once. That refusal is also the signal the memory spill
 *    manager reacts to, which is why it is surfaced through [[throttleCount]] rather than
 *    swallowed;
 *  - TCP level throttling by way of channel auto-read, which belongs to the consumer side handler.
 *
 * =Thread model=
 *
 * `ShuffleWriteMetricsReporter` documents that all of its methods are called on a single thread and
 * that implementations therefore need not synchronize. A Netty event-loop thread consequently must
 * never touch a reporter, so this handler calls none of them. It instead publishes plain counters
 * -- [[bytesWrittenToChannel]], [[blocksWrittenToChannel]], [[writeTimeNanos]] and the rest -- that
 * the writer reads on the task thread and forwards to the reporter there.
 *
 * The other rules the same model imposes are honoured throughout: no method here parks, blocks or
 * sleeps; every piece of shared state is an atomic or a concurrent collection, and no lock is ever
 * held across a channel write; no exception is allowed to escape a Netty callback; and every
 * failure observed on an event-loop thread is handed to [[StreamingShuffleErrorNotifier]] so that
 * the task thread re-throws it. Without that bridge a failure raised on an I/O thread is swallowed
 * and the producing task waits forever on progress that will never come.
 *
 * =Timing=
 *
 * Every instant this handler needs comes from the injected `Clock`, and no wall clock is read
 * directly. That is what makes the protocol's timing observable and testable without a sleep: a
 * suite advances a manual clock and asserts on [[stalledPartitions]] or on retransmission backoff
 * rather than waiting for real time to pass. It is also why this handler starts no timer thread of
 * its own: liveness is evaluated when it is asked for, on the thread that asks.
 *
 * =Configuration=
 *
 * Configuration is read once, here, and held immutably, which is what makes "streaming shuffle
 * configuration changes require an executor restart" true by construction rather than by
 * convention. The upstream Java message family in `org.apache.spark.network.shuffle.protocol
 * .streaming` is deliberately free of any Spark core coupling -- it reads no configuration, emits
 * no log line and reads no clock -- so this package owns all four of those concerns.
 *
 * This handler is NOT annotated `Sharable`, and must not be: it holds per channel state, so Netty's
 * own check that a non-sharable handler is added to at most one pipeline is exactly the protection
 * required.
 *
 * @param conf the executor's configuration, read exactly once during construction
 * @param shuffleId the shuffle whose partitions this handler streams
 * @param backpressure the protocol that owns the credit ledger, the acknowledgement and heartbeat
 *                     timeouts, cross shuffle utilisation aggregation and the backpressure event
 *                     counter. This handler reports to it and consumes its pacing decisions; it
 *                     never reimplements any part of it
 * @param rateLimiter the egress bucket this shuffle's share of the link capacity is charged against
 * @param errorNotifier the bridge that carries a failure observed on an event-loop thread across to
 *                      the task thread
 * @param clock time source for heartbeat stamps, liveness evaluation, reclamation budgeting and
 *              retransmission backoff, injected so that behaviour is deterministic under test
 */
private[spark] class StreamingShuffleServerHandler(
    conf: SparkConf,
    shuffleId: Int,
    backpressure: BackpressureProtocol,
    rateLimiter: TokenBucketRateLimiter,
    errorNotifier: StreamingShuffleErrorNotifier,
    clock: Clock = new SystemClock)
  extends ChannelInboundHandlerAdapter with Logging {

  import StreamingShuffleServerHandler._

  /**
   * The value of spark.shuffle.streaming.debug, read once and held immutably.
   *
   * It is the sole authority over whether this handler emits per message diagnostics. Logging one
   * line per block would defeat the subsystem's log budget of under ten megabytes an hour for each
   * executor on its own, so per message lines are gated on this flag while lifecycle events --
   * channel activation and loss, stream termination, escalation -- are always reported.
   */
  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  /**
   * The transport configuration for the streaming shuffle's own module.
   *
   * Asking `SparkTransportConf` for a configuration under the module name "shuffle-streaming"
   * yields an independent `spark.shuffle-streaming.io.*` namespace, so thread counts, buffer sizes,
   * retry behaviour and TCP keep-alive can all be tuned for streaming without perturbing the block
   * transfer service that every other shuffle depends on. The component that builds the
   * `TransportContext` for streaming reads this value, so that producer and consumer are configured
   * from one place rather than from two that can drift apart.
   */
  val transportConf: TransportConf = streamingTransportConf(conf)

  /**
   * Per partition egress and acknowledgement state, created on first use.
   *
   * A handler serves every reduce partition this map task produces for one shuffle, so the state is
   * keyed by partition rather than held as fields.
   */
  private val streams = new ConcurrentHashMap[Int, PartitionStream]()

  /**
   * Blocks framed and waiting for the wire, ordered across partitions by [[EgressOrdering]].
   *
   * This queue is the whole of the "quality of service prioritisation" the streaming shuffle
   * offers, and it is worth being exact about what that means: nothing in Spark marks packets at
   * the operating system or network level, and this handler does not attempt to. Prioritisation
   * here is flush ordering, and nothing more.
   *
   * `poll` and `offer` on a `PriorityBlockingQueue` never block, which is what makes it usable from
   * an event-loop thread. Its internal lock is never held across a channel write, because the drain
   * loop removes an entry first and writes afterwards.
   */
  private val egressQueue =
    new PriorityBlockingQueue[PendingBlock](INITIAL_EGRESS_QUEUE_CAPACITY, EgressOrdering)

  /** Monotonic enqueue ticket, which makes the ordering stable for equally urgent blocks. */
  private val egressTicket = new AtomicLong(0L)

  /** Guards the drain loop, so exactly one thread writes to the channel at a time. */
  private val draining = new AtomicBoolean(false)

  /** Set when a drain is requested while another thread holds the guard, to avoid a lost wakeup. */
  private val drainWakeup = new AtomicBoolean(false)

  /** The active channel context, or null before activation and after loss. */
  private val channelContext = new AtomicReference[ChannelHandlerContext](null)

  /**
   * The producing task's attributes, which decide flush order between blocks that are otherwise
   * equally ready. Accepted through [[registerTaskAttempt]] rather than by reading
   * `TaskContext.get()`, because that method is meaningless on an event-loop thread.
   */
  private val attempt = new AtomicReference[EgressPriority](DEFAULT_PRIORITY)

  private val bytesWritten = new AtomicLong(0L)
  private val blocksWritten = new AtomicLong(0L)
  private val writeNanos = new AtomicLong(0L)
  private val throttles = new AtomicLong(0L)
  private val acks = new AtomicLong(0L)
  private val heartbeats = new AtomicLong(0L)
  private val retransmittedBlocks = new AtomicLong(0L)
  private val misaddressedMessages = new AtomicLong(0L)
  private val misaddressReported = new AtomicBoolean(false)
  private val versionMismatch = new AtomicBoolean(false)

  /**
   * Reports an asynchronous write failure to the notifier.
   *
   * A channel write completes on an event-loop thread long after the call that issued it returned,
   * so a failure that is not observed through the future is lost outright: the producing task would
   * believe its bytes reached the consumer. One listener instance is allocated for the handler's
   * lifetime rather than one per write, because a write happens per block.
   */
  private val writeFailureListener: ChannelFutureListener = new ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = {
      if (!future.isSuccess) {
        errorNotifier.setError(future.cause())
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} failed to write to the " +
          log"consumer channel", future.cause())
      }
    }
  }

  if (debugEnabled) {
    logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} egress handler created on " +
      log"transport module ${MDC(VALUE, transportConf.getModuleName())} with " +
      log"${MDC(CONFIG, TCP_KEEP_ALIVE_KEY)} set to " +
      log"${MDC(STATUS, transportConf.enableTcpKeepAlive())} and a per block cap of " +
      log"${MDC(NUM_BYTES, MAX_PAYLOAD_BYTES)} payload byte(s)")
  }

  // ==========================================================================================
  // Producing task identity, which decides flush order
  // ==========================================================================================

  /**
   * Records the attributes of the task attempt whose output this handler streams.
   *
   * The writer calls this once, from the task thread, with values taken from its `TaskContext`.
   * Accepting them rather than reading `TaskContext.get()` here is required, not merely tidy: the
   * drain loop can run on a Netty event-loop thread, where no task context exists at all.
   *
   * @param priority the attributes of the producing attempt
   */
  def registerTaskAttempt(priority: EgressPriority): Unit = {
    require(priority != null, "The streaming shuffle egress priority must not be null.")
    attempt.set(priority)
    if (debugEnabled) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} egress will be ordered for " +
        log"stage ${MDC(VALUE, priority.stageId)} task attempt " +
        log"${MDC(COUNT, priority.taskAttemptId)}, attempt number " +
        log"${MDC(MAX_ATTEMPTS, priority.attemptNumber)}")
    }
  }

  /** The attributes flush ordering is currently derived from. */
  def taskPriority: EgressPriority = attempt.get()

  // ==========================================================================================
  // Egress: framing, checksumming and enqueueing
  // ==========================================================================================

  /**
   * Splits a payload into protocol sized blocks and enqueues each of them for one partition.
   *
   * This is the entry point the writer should reach for, because it is the only one that guarantees
   * the two mebibyte block cap without the caller having to restate it. A payload larger than the
   * cap becomes several consecutively numbered blocks; a payload at or below it becomes one. An
   * empty payload produces no block at all, which is the honest representation of a partition that
   * received no records: end of stream is then signalled by [[terminateStream]] alone.
   *
   * Keeping every block at or under the cap is what lets the reduce side start consuming before
   * the producer has finished, which is the whole point of the streaming path -- a single enormous
   * frame would reintroduce exactly the materialisation barrier this shuffle exists to remove.
   *
   * @param partitionId the reduce partition the bytes belong to
   * @param payload the bytes to stream; the array is not retained, so the caller may reuse it
   * @return the sequence numbers assigned to the blocks, in the order they will be written
   */
  def enqueuePayload(partitionId: Int, payload: Array[Byte]): Seq[Long] = {
    enqueuePayload(partitionId, payload, attempt.get())
  }

  /**
   * Splits a payload into protocol sized blocks and enqueues them under an explicit priority.
   *
   * @param partitionId the reduce partition the bytes belong to
   * @param payload the bytes to stream; the array is not retained, so the caller may reuse it
   * @param priority the attributes of the attempt that produced these bytes
   * @return the sequence numbers assigned to the blocks, in the order they will be written
   */
  def enqueuePayload(
      partitionId: Int,
      payload: Array[Byte],
      priority: EgressPriority): Seq[Long] = {
    require(payload != null, "The streaming shuffle payload must not be null.")
    val assigned = new ArrayBuffer[Long](
      math.max(1, payload.length / MAX_PAYLOAD_BYTES + 1))
    var offset = 0
    while (offset < payload.length) {
      val length = math.min(MAX_PAYLOAD_BYTES, payload.length - offset)
      val block = new Array[Byte](length)
      System.arraycopy(payload, offset, block, 0, length)
      assigned += enqueueBlock(partitionId, block, priority)
      offset += length
    }
    assigned.toSeq
  }

  /**
   * Enqueues one block for one partition, checksummed and ready for the wire.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param payload the block's bytes, at most [[MAX_PAYLOAD_BYTES]] of them
   * @return the sequence number assigned to the block
   */
  def enqueueBlock(partitionId: Int, payload: Array[Byte]): Long = {
    enqueueBlock(partitionId, payload, attempt.get())
  }

  /**
   * Enqueues one block for one partition under an explicit priority.
   *
   * The CRC32C is computed by `DataBlockMessage.withComputedChecksum`, which routes the arithmetic
   * through `StreamingShuffleChecksum` so that producer and consumer are provably running the same
   * computation over the same bytes, and so that the value binds the payload to the shuffle,
   * partition and sequence number it is being sent under rather than covering the bytes in
   * isolation. No checksum arithmetic is reimplemented on this side of the boundary.
   *
   * The block is charged against the executor's egress budget only when it is written, never here:
   * enqueueing is free, so a writer is never refused the chance to hand over bytes it has already
   * produced. Pacing decides when those bytes leave, not whether they may be offered.
   *
   * @param partitionId the reduce partition the block belongs to
   * @param payload the block's bytes, at most [[MAX_PAYLOAD_BYTES]] of them
   * @param priority the attributes of the attempt that produced the block
   * @return the sequence number assigned to the block
   * @throws IllegalArgumentException if the payload is null or larger than the block cap
   * @throws IllegalStateException if the partition's stream has already been terminated
   */
  def enqueueBlock(partitionId: Int, payload: Array[Byte], priority: EgressPriority): Long = {
    require(payload != null, "The streaming shuffle block payload must not be null.")
    require(payload.length <= MAX_PAYLOAD_BYTES,
      s"A streaming shuffle block carries ${payload.length} byte(s), which exceeds the protocol " +
        s"cap of $MAX_PAYLOAD_BYTES byte(s). Frame the payload with enqueuePayload instead.")
    require(priority != null, "The streaming shuffle egress priority must not be null.")
    val stream = streamFor(partitionId)
    if (stream.terminationRequested.get()) {
      throw new IllegalStateException(s"Streaming shuffle $shuffleId partition $partitionId has " +
        "already been terminated, so no further block may be enqueued for it.")
    }
    val sequenceNumber = stream.nextSequenceNumber.getAndIncrement()
    val block = DataBlockMessage.withComputedChecksum(
      shuffleId, partitionId, sequenceNumber, payload)
    val framedBytes = StreamingShuffleMessage.framedLength(block.encodedLength())
    stream.blocksEnqueued.incrementAndGet()
    stream.pendingBlocks.incrementAndGet()
    stream.pendingBytes.addAndGet(framedBytes.toLong)
    egressQueue.offer(
      PendingBlock(block, priority, egressTicket.getAndIncrement(), framedBytes))
    // Queued volume is accounted locally only, in the three counters above. The protocol's ledger
    // records a block when it is admitted for egress, not when it is queued, so a queued block has
    // no ledger slot yet; `pendingBytes` is what `pendingBytes(partitionId)` reports to the writer.
    if (debugEnabled) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} enqueued block ${MDC(COUNT, sequenceNumber)} of " +
        log"${MDC(NUM_BYTES, framedBytes)} framed byte(s)")
    }
    val written = drain()
    if (debugEnabled && written == 0L) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} is holding block ${MDC(COUNT, sequenceNumber)}: " +
        log"egress is paced or the channel is not writable")
    }
    sequenceNumber
  }

  // ==========================================================================================
  // Egress: draining onto the channel
  // ==========================================================================================

  /**
   * Writes as much of the queued output as pacing and the socket presently allow.
   *
   * Safe to call from the task thread and from a Netty event-loop thread alike; `Channel.write`
   * itself is thread safe, and the drain guard means only one thread is writing at any moment. It
   * never parks: when the bucket refuses a block or the socket's outbound buffer is full, the block
   * stays queued and the method returns. Holding rather than dropping is the whole of the response
   * to a refusal, and it is also the signal the spill manager reacts to.
   *
   * @return the number of framed bytes handed to the channel by this call
   */
  def flushPending(): Long = drain()

  private def drain(): Long = {
    var total = 0L
    var again = true
    while (again) {
      again = false
      if (draining.compareAndSet(false, true)) {
        drainWakeup.set(false)
        val written = try {
          drainOnce()
        } finally {
          draining.set(false)
        }
        total += written
        // Re-run when this pass made progress and work remains, or when a contending thread left a
        // note while the guard was held, so a block offered during the pass is not stranded. The
        // progress condition is what stops a throttled or unwritable channel from spinning: a pass
        // that wrote nothing runs at most once more, and the note is cleared at the top of it.
        again = !egressQueue.isEmpty && (written > 0L || drainWakeup.get())
      } else {
        drainWakeup.set(true)
      }
    }
    total
  }

  /**
   * One pass of the drain loop, executed by the single thread holding the drain guard.
   *
   * A block is removed from the queue before it is charged, and returned to the queue unchanged if
   * the charge is refused. Returning it is order preserving, because a pending block's position is
   * decided by its priority and its monotonic ticket, neither of which the round trip alters.
   */
  private def drainOnce(): Long = {
    val ctx = channelContext.get()
    if (ctx == null || !ctx.channel().isActive()) {
      0L
    } else {
      val startedAtNanos = clock.nanoTime()
      var written = 0L
      var flushNeeded = false
      var keepGoing = true
      while (keepGoing) {
        if (!ctx.channel().isWritable()) {
          // The socket's outbound buffer is full. Leaving the block queued is correct: writability
          // is signalled on this very pipeline, and channelWritabilityChanged resumes the drain.
          keepGoing = false
        } else {
          val pending = egressQueue.poll()
          if (pending == null) {
            keepGoing = false
          } else if (!admitForEgress(pending)) {
            egressQueue.offer(pending)
            throttles.incrementAndGet()
            if (debugEnabled) {
              logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
                log"${MDC(PARTITION_ID, pending.block.partitionId())} is throttled holding " +
                log"${MDC(NUM_BYTES, pending.framedBytes)} framed byte(s); " +
                log"${MDC(VALUE, rateLimiter.availableTokens)} token(s) available")
            }
            keepGoing = false
          } else {
            writeBlock(ctx, pending)
            written += pending.framedBytes.toLong
            flushNeeded = true
          }
        }
      }
      // One flush for the whole batch rather than one per block: the two mebibyte cap only
      // pipelines if consecutive blocks share a syscall instead of trickling out one at a time.
      if (flushNeeded) {
        ctx.flush()
      }
      emitDeferredTerminations(ctx)
      writeNanos.addAndGet(math.max(0L, clock.nanoTime() - startedAtNanos))
      written
    }
  }

  /**
   * Writes one block and retains it against the possibility of retransmission.
   *
   * The block enters the unacknowledged window before it is written, never after: a retransmission
   * request that arrives while the write is still in flight must find the block retained rather
   * than find a hole in the window.
   */
  private def writeBlock(ctx: ChannelHandlerContext, pending: PendingBlock): Unit = {
    val block = pending.block
    val partitionId = block.partitionId()
    val stream = streamFor(partitionId)
    val framedBytes = pending.framedBytes.toLong
    val previous = stream.unacknowledged.put(block.sequenceNumber(), block)
    if (previous == null) {
      stream.unacknowledgedBytes.addAndGet(framedBytes)
    }
    stream.pendingBytes.addAndGet(-framedBytes)
    stream.pendingBlocks.decrementAndGet()
    ctx.write(Unpooled.wrappedBuffer(block.toByteBuffer())).addListener(writeFailureListener)
    bytesWritten.addAndGet(framedBytes)
    blocksWritten.incrementAndGet()
    if (debugEnabled) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} wrote block ${MDC(COUNT, block.sequenceNumber())} " +
        log"of ${MDC(NUM_BYTES, framedBytes)} framed byte(s), checksum " +
        log"${MDC(VALUE, block.checksum())}")
    }
  }

  /**
   * Writes a control frame immediately, bypassing the pacing verdict but not the accounting.
   *
   * Control frames are charged against the bucket and then written whatever the verdict.
   * Withholding a heartbeat because egress is paced would be indistinguishable, at the consumer,
   * from the producer having died, and would trip the very failure detector the heartbeat exists
   * to satisfy: pacing must never be able to manufacture a failure. Debiting the bucket regardless
   * keeps the accounting of bytes on the wire honest, and control frames are tens of bytes, so the
   * surplus a refusal lets through cannot meaningfully perturb the rate.
   */
  private def writeControl(ctx: ChannelHandlerContext, message: StreamingShuffleMessage): Unit = {
    val framedBytes = StreamingShuffleMessage.framedLength(message.encodedLength()).toLong
    val charged = rateLimiter.tryAcquire(framedBytes)
    ctx.writeAndFlush(Unpooled.wrappedBuffer(message.toByteBuffer()))
      .addListener(writeFailureListener)
    bytesWritten.addAndGet(framedBytes)
    if (debugEnabled && !charged) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, message.partitionId())} sent an uncharged control frame of " +
        log"${MDC(NUM_BYTES, framedBytes)} byte(s) because the egress bucket was empty")
    }
  }

  // ==========================================================================================
  // Liveness and orderly end of stream
  // ==========================================================================================

  /**
   * Emits a producer heartbeat for one partition, stamped from the injected clock.
   *
   * The wire message carries a timestamp and reads no clock of its own, deliberately: the instant
   * it reports is supplied from here, which is what lets a suite drive the five second liveness
   * bound with a manual clock instead of a sleep. The consumer's own detector is satisfied by this
   * frame, not by TCP keep-alive: keep-alive is a boolean with no interval, and the JDK exposes no
   * socket option for one, so the bound is enforced at the application level or not at all.
   *
   * @param partitionId the reduce partition whose stream is being kept alive
   * @return true if the frame was handed to the channel, false if there is no active channel
   */
  def sendHeartbeat(partitionId: Int): Boolean = {
    val ctx = channelContext.get()
    if (ctx == null || !ctx.channel().isActive()) {
      false
    } else {
      val stream = streamFor(partitionId)
      writeControl(ctx, new HeartbeatMessage(
        shuffleId, partitionId, stream.nextSequenceNumber.get(), clock.getTimeMillis()))
      true
    }
  }

  /**
   * Signals the orderly end of one partition's stream.
   *
   * This is what makes the absence of further data mean completion rather than producer loss: with
   * no terminator the consumer cannot distinguish a finished producer from a dead one, and would
   * wait out its five second detector before concluding anything. A partition that produced no
   * records terminates with a total of zero, which the protocol accepts.
   *
   * The terminator must never overtake the data it terminates, so it is deferred until the
   * partition's queued blocks have all been written. When egress is paced or the socket is full the
   * request is remembered and emitted by a later drain; the return value says which happened.
   *
   * @param partitionId the reduce partition whose stream is complete
   * @return true if the terminator was written by this call, false if it was deferred
   */
  def terminateStream(partitionId: Int): Boolean = {
    val stream = streamFor(partitionId)
    stream.terminationRequested.set(true)
    stream.totalBlocksAtTermination.set(stream.blocksEnqueued.get())
    drain()
    val sent = stream.terminationSent.get()
    if (!sent) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} deferred its end of stream marker with " +
        log"${MDC(COUNT, stream.pendingBlocks.get())} block(s) still queued")
    }
    sent
  }

  /**
   * Emits any terminator whose partition has finished draining.
   *
   * Called at the end of every drain pass, which is the only moment at which a partition can have
   * become empty. The one-shot guard means a terminator is written exactly once even though several
   * drains may observe the same empty partition.
   */
  private def emitDeferredTerminations(ctx: ChannelHandlerContext): Unit = {
    if (ctx.channel().isActive()) {
      streams.values().asScala.foreach { stream =>
        val ready = stream.terminationRequested.get() && stream.pendingBlocks.get() <= 0L
        if (ready && stream.terminationSent.compareAndSet(false, true)) {
          val totalBlocks = stream.totalBlocksAtTermination.get()
          writeControl(ctx, new StreamTerminationMessage(
            shuffleId, stream.partitionId, stream.nextSequenceNumber.get(), totalBlocks))
          logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
            log"${MDC(PARTITION_ID, stream.partitionId)} streamed " +
            log"${MDC(NUM_BLOCKS, totalBlocks)} block(s) and signalled end of stream")
        }
      }
    }
  }

  /**
   * Whether one partition's consumer has stopped acknowledging for longer than the liveness window.
   *
   * Evaluated on the calling thread against the injected clock rather than by a timer, so this
   * handler starts no thread of its own and a suite can assert the ten second window
   * deterministically. A partition with nothing outstanding is never stalled, however long it has
   * been quiet: there is nothing for the consumer to acknowledge.
   *
   * The writer polls this to decide whether to spill the unacknowledged window; the window itself
   * is retained here either way, because a consumer that reconnects must be able to be served from
   * memory or from spill rather than forcing the whole stage to be recomputed.
   *
   * @param partitionId the reduce partition to test
   * @return true if the partition has unacknowledged output and has seen no progress in the window
   */
  def isConsumerStalled(partitionId: Int): Boolean = {
    val stream = streams.get(partitionId)
    stream != null && stalled(stream, clock.getTimeMillis())
  }

  /** Every partition whose consumer has stopped acknowledging for longer than the window. */
  def stalledPartitions: Seq[Int] = {
    val nowMs = clock.getTimeMillis()
    val stalledIds = new ArrayBuffer[Int](streams.size())
    streams.values().asScala.foreach { stream =>
      if (stalled(stream, nowMs)) {
        stalledIds += stream.partitionId
      }
    }
    stalledIds.sorted.toSeq
  }

  private def stalled(stream: PartitionStream, nowMs: Long): Boolean = {
    stream.unacknowledged.size() > 0 &&
      nowMs - stream.lastProgressMs.get() >= CONSUMER_LIVENESS_TIMEOUT_MS
  }

  // ==========================================================================================
  // Retransmission, bounded to the retained window
  // ==========================================================================================

  /**
   * Replays retained blocks for one partition over the inclusive range the consumer asked for.
   *
   * Retransmission is bounded by what is still retained, and the bound is not a limitation of this
   * implementation but the price of reclaiming producer memory on acknowledgement: once a block
   * has been acknowledged its bytes are gone, so no amount of asking can bring them back. A
   * request that reaches below the acknowledged position is therefore not serviceable, and is
   * escalated through the error notifier so that the task thread raises the fetch failure that has
   * the unmodified scheduler recompute the upstream stage. This handler never constructs that
   * fetch failure itself; raising it belongs to the reduce side, which is the party the scheduler
   * attributes it to.
   *
   * Requests are paced by exponential backoff starting at one second and are refused after five
   * attempts, at which point the condition is escalated rather than retried forever. A serviced
   * acknowledgement resets that budget, because progress means the peer is healthy again.
   *
   * @param partitionId the reduce partition whose blocks are being replayed
   * @param firstSequenceNumber inclusive lower bound of the requested range
   * @param lastSequenceNumber inclusive upper bound of the requested range
   * @return the number of blocks re-queued for the wire, zero if the request was deferred, refused
   *         or escalated
   */
  def retransmit(partitionId: Int, firstSequenceNumber: Long, lastSequenceNumber: Long): Int = {
    val stream = streams.get(partitionId)
    if (stream == null || lastSequenceNumber < firstSequenceNumber) {
      misaddressedMessages.incrementAndGet()
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} discarded a retransmission " +
        log"request for partition ${MDC(PARTITION_ID, partitionId)}: " +
        log"${MDC(REASON, "no such stream or an inverted window")}")
      0
    } else {
      val nowMs = clock.getTimeMillis()
      if (nowMs < stream.nextRetransmitAtMs.get()) {
        // Deferred rather than refused: the consumer may ask again once the backoff has elapsed.
        0
      } else {
        val attempts = stream.retransmitAttempts.incrementAndGet()
        if (attempts > MAX_RETRANSMIT_ATTEMPTS) {
          escalateRetransmissionBudget(partitionId, attempts)
          0
        } else {
          stream.nextRetransmitAtMs.set(nowMs + backoffMs(attempts))
          serviceRetransmission(stream, firstSequenceNumber, lastSequenceNumber)
        }
      }
    }
  }

  /**
   * Re-queues every retained block in the requested range, or escalates if the range has been
   * reclaimed. The retained window is exactly the open interval above the acknowledged position, so
   * the acknowledged position alone decides serviceability.
   */
  private def serviceRetransmission(
      stream: PartitionStream,
      firstSequenceNumber: Long,
      lastSequenceNumber: Long): Int = {
    val reclaimedThrough = stream.lastAckPosition.get()
    if (firstSequenceNumber <= reclaimedThrough) {
      errorNotifier.setError(StreamingShuffleErrors.invalidSequenceNumber(
        shuffleId, stream.partitionId, reclaimedThrough + 1L, firstSequenceNumber))
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, stream.partitionId)} cannot retransmit block " +
        log"${MDC(COUNT, firstSequenceNumber)}: everything up to " +
        log"${MDC(VALUE, reclaimedThrough)} was acknowledged and released, so the read must be " +
        log"invalidated and the upstream stage recomputed")
      0
    } else {
      val priority = attempt.get()
      val replayed = stream.unacknowledged
        .subMap(firstSequenceNumber, true, lastSequenceNumber, true)
        .values()
        .asScala
        .toSeq
      replayed.foreach { block =>
        val framedBytes = StreamingShuffleMessage.framedLength(block.encodedLength())
        stream.pendingBlocks.incrementAndGet()
        stream.pendingBytes.addAndGet(framedBytes.toLong)
        egressQueue.offer(
          PendingBlock(block, priority, egressTicket.getAndIncrement(), framedBytes))
      }
      if (replayed.nonEmpty) {
        retransmittedBlocks.addAndGet(replayed.size.toLong)
        logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
          log"${MDC(PARTITION_ID, stream.partitionId)} is replaying " +
          log"${MDC(NUM_BLOCKS, replayed.size)} retained block(s) from the unacknowledged window")
        drain()
      }
      replayed.size
    }
  }

  /**
   * Escalates a consumer that keeps asking for the same bytes past the retry budget.
   *
   * The producing task is failed rather than left replaying forever, which is the escalation the
   * failure protocol prescribes once backoff is exhausted: stage recomputation then recovers the
   * output. The condition has no entry in Spark's error catalogue and none is added for it, because
   * the catalogue is not this file's to extend.
   */
  private def escalateRetransmissionBudget(partitionId: Int, attempts: Int): Unit = {
    errorNotifier.setError(new SparkException(s"Streaming shuffle $shuffleId partition " +
      s"$partitionId exhausted its retransmission budget of $MAX_RETRANSMIT_ATTEMPTS attempt(s) " +
      s"after $attempts request(s); the consumer is not making progress."))
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
      log"${MDC(PARTITION_ID, partitionId)} exhausted its retransmission budget of " +
      log"${MDC(MAX_ATTEMPTS, MAX_RETRANSMIT_ATTEMPTS)} attempt(s)")
  }

  /** Exponential backoff from one second, doubling per attempt and capped by the attempt budget. */
  private def backoffMs(attempts: Int): Long = {
    val shift = math.min(math.max(0, attempts - 1), MAX_RETRANSMIT_BACKOFF_SHIFT)
    RETRANSMIT_INITIAL_BACKOFF_MS << shift
  }

  // ==========================================================================================
  // Netty callbacks
  // ==========================================================================================

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    guard {
      channelContext.set(ctx)
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} egress channel to " +
        log"${MDC(HOST_PORT, ctx.channel().remoteAddress())} is active")
      // Output produced before the channel came up is queued, not lost, so drain it now.
      drain()
    }
    super.channelActive(ctx)
  }

  /**
   * Consumes the control traffic the reduce side sends back.
   *
   * Three shapes are accepted, so the handler composes with either pipeline it can legitimately sit
   * in: an already decoded message, when a decoder precedes it; a `ByteBuf` holding one framed
   * message, when only length delimitation precedes it; and a `ByteBuffer` of the same. Anything
   * else is passed on untouched rather than swallowed, because a handler that consumes messages it
   * does not understand breaks every handler behind it.
   *
   * A `ByteBuf` is released on every path, including the failing ones, so a decode failure cannot
   * leak a pooled buffer.
   */
  override def channelRead(ctx: ChannelHandlerContext, message: AnyRef): Unit = {
    var forwardable = false
    guard {
      message match {
        case decoded: StreamingShuffleMessage =>
          handleInbound(ctx, decoded)
        case buffer: ByteBuf =>
          try {
            decodeAndHandle(ctx, buffer.nioBuffer())
          } finally {
            ReferenceCountUtil.release(buffer)
          }
        case buffer: ByteBuffer =>
          decodeAndHandle(ctx, buffer)
        case _ =>
          forwardable = true
      }
    }
    if (forwardable) {
      super.channelRead(ctx, message)
    }
  }

  override def channelWritabilityChanged(ctx: ChannelHandlerContext): Unit = {
    guard {
      if (ctx.channel().isWritable()) {
        // The socket has drained, so blocks held back for want of room can go now.
        drain()
      }
    }
    super.channelWritabilityChanged(ctx)
  }

  /**
   * Records the loss of the consumer's channel, retaining everything not yet acknowledged.
   *
   * Retention is the point: the failure protocol requires that a consumer which reconnects be
   * served from memory or from spill rather than forcing the whole upstream stage to be
   * recomputed, so this callback deliberately clears nothing. Releasing the window is
   * [[releaseAll]]'s job, and the writer calls it when the task is finished with the shuffle.
   */
  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    guard {
      channelContext.compareAndSet(ctx, null)
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} egress channel to " +
        log"${MDC(HOST_PORT, ctx.channel().remoteAddress())} became inactive with " +
        log"${MDC(NUM_BYTES, unacknowledgedBytes)} unacknowledged byte(s) retained for replay")
      reportPeerLoss()
    }
    super.channelInactive(ctx)
  }

  /**
   * Latches a channel level failure and closes the channel.
   *
   * The failure is not forwarded down the pipeline, because the notifier is the single place the
   * task thread consults and the channel is being closed regardless; forwarding would add a second,
   * weaker report of the same event.
   */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    guard {
      errorNotifier.setError(cause)
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} egress channel raised " +
        log"${MDC(ERROR, cause.getMessage())}; closing it and failing the producing task", cause)
      ctx.close()
    }
  }

  // ==========================================================================================
  // Inbound dispatch
  // ==========================================================================================

  /**
   * Decodes one framed message, checking the peer's protocol revision before its body is trusted.
   *
   * The version is peeked without consuming, so a mismatch is attributed to the version rather
   * than blamed on an unknown type byte further down. A mismatch is one of the conditions under
   * which the streaming shuffle steps aside in favour of the sort based implementation, which is
   * why it is published through [[versionMismatchDetected]] as well as escalated.
   */
  private def decodeAndHandle(ctx: ChannelHandlerContext, frame: ByteBuffer): Unit = {
    val version = StreamingShuffleMessage.peekProtocolVersion(frame)
    if (StreamingShuffleMessage.isCompatible(version)) {
      handleInbound(ctx, StreamingShuffleMessage.Decoder.fromByteBuffer(frame))
    } else {
      reportVersionMismatch(ctx, version)
    }
  }

  /**
   * Routes one inbound message, discriminating on its concrete type.
   *
   * Discrimination is by type and never by encoded length: an acknowledgement, a heartbeat, a
   * retransmission request and an end of stream marker all encode to exactly the same number of
   * bytes, so length carries no information about which of them arrived.
   */
  private def handleInbound(ctx: ChannelHandlerContext, message: StreamingShuffleMessage): Unit = {
    if (message.shuffleId() != shuffleId) {
      misaddressedMessages.incrementAndGet()
      if (misaddressReported.compareAndSet(false, true)) {
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} dropped a message " +
          log"addressed to shuffle ${MDC(VALUE, message.shuffleId())}: " +
          log"${MDC(REASON, "the frame does not belong to this stream")}. Further occurrences " +
          log"are counted but not logged")
      }
    } else {
      message match {
        case ack: AckMessage =>
          handleAck(ack)
        case heartbeat: HeartbeatMessage =>
          handleHeartbeat(heartbeat)
        case request: RetransmitRequestMessage =>
          handleRetransmitRequest(request)
        case unexpected =>
          rejectUnexpected(ctx, unexpected)
      }
    }
  }

  /**
   * Refuses a message a producer can never legitimately receive.
   *
   * Data blocks and end of stream markers travel from producer to consumer, so their arrival here
   * means the channel is carrying the wrong direction of the protocol. That is a typed condition in
   * Spark's error catalogue and is raised as one.
   */
  private def rejectUnexpected(
      ctx: ChannelHandlerContext,
      message: StreamingShuffleMessage): Unit = {
    val actual = message match {
      case _: DataBlockMessage => StreamingShuffleMessageType.DATA_BLOCK.name()
      case _: StreamTerminationMessage => StreamingShuffleMessageType.STREAM_TERMINATION.name()
      case other => other.getClass.getName
    }
    val expected = s"${StreamingShuffleMessageType.ACK.name()}, " +
      s"${StreamingShuffleMessageType.HEARTBEAT.name()} or " +
      s"${StreamingShuffleMessageType.RETRANSMIT_REQUEST.name()}"
    errorNotifier.setError(StreamingShuffleErrors.unexpectedMessageType(expected, actual))
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} received " +
      log"${MDC(VALUE, actual)} on its egress channel, which a producer may never be sent; " +
      log"closing the channel")
    ctx.close()
  }

  /**
   * Applies one acknowledgement: advances the consumer position and releases what it covers.
   *
   * This is the moment producer memory is reclaimed, and the reclamation is bounded: the protocol
   * requires it to complete within one hundred milliseconds of the acknowledgement, so the elapsed
   * time is measured and a breach is reported once per stream rather than on every acknowledgement.
   * Releasing here is also what lets the consumer side handler re-enable reading, because the
   * credit the ledger hands out is exactly what this release makes available.
   */
  private def handleAck(ack: AckMessage): Unit = {
    val partitionId = ack.partitionId()
    val position = ack.consumerPosition()
    val stream = streamFor(partitionId)
    acks.incrementAndGet()
    val startedAtMs = clock.getTimeMillis()
    stream.lastProgressMs.set(startedAtMs)
    advanceAckPosition(stream, position)
    var reclaimedBytes = 0L
    var reclaimedBlocks = 0
    if (position >= 0L) {
      val iterator = stream.unacknowledged.headMap(position, true).entrySet().iterator()
      while (iterator.hasNext()) {
        val released = iterator.next().getValue()
        iterator.remove()
        reclaimedBytes += StreamingShuffleMessage.framedLength(released.encodedLength()).toLong
        reclaimedBlocks += 1
      }
      if (reclaimedBytes > 0L) {
        stream.unacknowledgedBytes.addAndGet(-reclaimedBytes)
      }
    }
    if (reclaimedBlocks > 0) {
      // Progress means the peer is healthy, so the retransmission budget starts afresh.
      stream.retransmitAttempts.set(0)
      stream.nextRetransmitAtMs.set(0L)
    }
    reportAck(partitionId, position)
    val elapsedMs = clock.getTimeMillis() - startedAtMs
    if (elapsedMs > ACK_RECLAMATION_BUDGET_MS &&
      stream.reclamationWarned.compareAndSet(false, true)) {
      logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} took ${MDC(DURATION, elapsedMs)} ms to reclaim " +
        log"${MDC(NUM_BLOCKS, reclaimedBlocks)} block(s), beyond the " +
        log"${MDC(TIMEOUT, ACK_RECLAMATION_BUDGET_MS)} ms budget. Further breaches on this " +
        log"stream are not logged")
    }
    if (debugEnabled) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} acknowledged through ${MDC(COUNT, position)}, " +
        log"reclaiming ${MDC(NUM_BYTES, reclaimedBytes)} byte(s) in " +
        log"${MDC(DURATION, elapsedMs)} ms")
    }
    // Reclaimed memory and advanced credit may both have unblocked egress.
    drain()
  }

  /**
   * Moves the acknowledged position forward only, so a reordered or duplicated acknowledgement can
   * never resurrect blocks that a later one already released.
   */
  private def advanceAckPosition(stream: PartitionStream, position: Long): Unit = {
    var observed = stream.lastAckPosition.get()
    while (position > observed && !stream.lastAckPosition.compareAndSet(observed, position)) {
      observed = stream.lastAckPosition.get()
    }
  }

  private def handleHeartbeat(heartbeat: HeartbeatMessage): Unit = {
    val partitionId = heartbeat.partitionId()
    val stream = streamFor(partitionId)
    heartbeats.incrementAndGet()
    stream.lastProgressMs.set(clock.getTimeMillis())
    reportHeartbeat(heartbeat)
    if (debugEnabled) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} saw a consumer heartbeat stamped " +
        log"${MDC(VALUE, heartbeat.timestampMs())}")
    }
  }

  private def handleRetransmitRequest(request: RetransmitRequestMessage): Unit = {
    val partitionId = request.partitionId()
    val serviced =
      retransmit(partitionId, request.firstSequenceNumber(), request.lastSequenceNumber())
    if (debugEnabled) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition " +
        log"${MDC(PARTITION_ID, partitionId)} was asked to replay " +
        log"${MDC(NUM_BLOCKS, request.blockCount())} block(s) and re-queued " +
        log"${MDC(COUNT, serviced)}")
    }
  }

  private def reportVersionMismatch(ctx: ChannelHandlerContext, version: Byte): Unit = {
    versionMismatch.set(true)
    errorNotifier.setError(new SparkException(s"Streaming shuffle $shuffleId received protocol " +
      s"version $version from its consumer but this executor speaks " +
      s"${StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION}; the shuffle must fall back to the " +
      "sort based implementation."))
    logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} detected a protocol version " +
      log"mismatch: peer sent ${MDC(VALUE, version)} against " +
      log"${MDC(COUNT, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)}. " +
      log"${MDC(REASON, "streaming yields to sort based shuffle")}")
    ctx.close()
  }

  // ==========================================================================================
  // Reporting to the backpressure protocol
  //
  // This is the ONLY region of this file that calls BackpressureProtocol. Keeping every call in one
  // place is deliberate: the protocol owns the credit ledger, the acknowledgement and heartbeat
  // timeouts, the consumer liveness window, cross shuffle utilisation aggregation, arbitration on
  // partition count and pending volume, and the backpressure event counter. This handler supplies
  // the observations that ledger is computed from and reimplements none of it, so the reporting
  // surface it depends on is exactly the four calls below and nothing else.
  // ==========================================================================================

  /**
   * Asks the protocol to admit one pending block for egress, and reports the outcome.
   *
   * This is the single producer side gate, and it is the protocol's rather than this handler's on
   * purpose. `tryAdmit` performs, indivisibly, the three things a block has to clear before it may
   * go on the wire: the stream's consumer credit is checked, the block's framed bytes are charged
   * against the shared egress bucket, and the block is entered into the ledger's unacknowledged
   * window under its sequence number. A refusal is what latches the throttled state and advances
   * the `shuffle.streaming.backpressureEvents` counter, so a throttle is counted once, by the owner
   * of the ledger, instead of once per observer.
   *
   * The bucket is charged here and nowhere else on the data path. The protocol holds the same
   * [[TokenBucketRateLimiter]] this handler is given, so charging it locally as well would debit a
   * block's bytes twice and halve the effective rate. Control frames are the one exception and are
   * charged directly in [[writeControl]], because they bypass the pacing verdict by design.
   *
   * Recording the send is idempotent in the sequence number, which is what makes a retransmitted
   * block safe: replaying it re-enters the same window slot rather than charging the stream twice.
   *
   * @return true if the block may be written now, false if it must stay queued and be retried
   */
  private def admitForEgress(pending: PendingBlock): Boolean = {
    backpressure.tryAdmit(
      shuffleId,
      pending.block.partitionId(),
      pending.framedBytes.toLong,
      pending.block.sequenceNumber())
  }

  /**
   * Reports a consumer acknowledgement, so the ledger can release credit and time reclamation.
   *
   * The position is the consumer's, not this handler's view of it: the ledger retires every block
   * at or below it, which is the event the hundred millisecond reclamation bound is measured from.
   */
  private def reportAck(partitionId: Int, consumerPosition: Long): Unit = {
    backpressure.onAck(shuffleId, partitionId, consumerPosition)
  }

  /**
   * Reports a consumer heartbeat, carrying the frame rather than just its instant.
   *
   * The message is handed over whole because the ledger records two quantities from it: the local
   * arrival time, which arms the liveness window, and the peer's own stamp, which is what lets a
   * consumer's clock be compared with this executor's.
   */
  private def reportHeartbeat(heartbeat: HeartbeatMessage): Unit = {
    backpressure.onHeartbeat(heartbeat)
  }

  /**
   * Reports that the consumer's channel was lost, without latching a degradation.
   *
   * A poll is the whole of it. Losing a channel is explicitly not a fallback condition -- the
   * failure protocol requires that a consumer which reconnects be replayed from the retained window
   * -- so nothing here may trip the sort based fallback. Polling makes the protocol re-evaluate the
   * acknowledgement and heartbeat windows at the moment of the loss instead of at its next
   * scheduled sweep, which is the observation this handler is in a position to contribute; whether
   * the gap has become a timeout stays the protocol's decision.
   */
  private def reportPeerLoss(): Unit = {
    backpressure.pollOnce()
  }

  // ==========================================================================================
  // Observers, read by the task thread
  //
  // The write metrics reporter documents that its methods are called on a single thread, so an
  // event-loop thread must never touch it. These counters exist so that the writer can read the
  // handler's progress on the task thread and forward it to the reporter from there.
  // ==========================================================================================

  /** Framed bytes handed to the channel, data and control frames together. */
  def bytesWrittenToChannel: Long = bytesWritten.get()

  /** Data blocks handed to the channel, retransmissions included. */
  def blocksWrittenToChannel: Long = blocksWritten.get()

  /** Nanoseconds spent inside the drain loop, suitable for the reporter's write time counter. */
  def writeTimeNanos: Long = writeNanos.get()

  /** Framed bytes queued for the wire but not yet written. */
  def pendingBytes: Long = sumOverStreams(stream => stream.pendingBytes.get())

  /** Framed bytes written but not yet acknowledged, and therefore still retained for replay. */
  def unacknowledgedBytes: Long = sumOverStreams(stream => stream.unacknowledgedBytes.get())

  /** Blocks retained for replay across every partition of this shuffle. */
  def unacknowledgedBlockCount: Int = {
    var total = 0
    streams.values().asScala.foreach(stream => total += stream.unacknowledged.size())
    total
  }

  /**
   * The highest block sequence number the consumer has acknowledged for one partition, or
   * `AckMessage.NOTHING_CONSUMED` if it has acknowledged nothing.
   */
  def acknowledgedPosition(partitionId: Int): Long = {
    val stream = streams.get(partitionId)
    if (stream == null) AckMessage.NOTHING_CONSUMED else stream.lastAckPosition.get()
  }

  /** How many times egress was refused by the rate limiter and the block was held instead. */
  def throttleCount: Long = throttles.get()

  /** Acknowledgements consumed from the reduce side. */
  def ackCount: Long = acks.get()

  /** Heartbeats consumed from the reduce side. */
  def heartbeatCount: Long = heartbeats.get()

  /** Blocks re-queued from the retained window in response to retransmission requests. */
  def retransmittedBlockCount: Long = retransmittedBlocks.get()

  /** Frames dropped because they named a different shuffle than the one this handler serves. */
  def misaddressedMessageCount: Long = misaddressedMessages.get()

  /**
   * Whether a peer announced a protocol revision this build cannot speak.
   *
   * Published so that the fallback policy can trip on a version mismatch, which is one of the four
   * conditions under which the streaming shuffle yields to the sort based implementation.
   */
  def versionMismatchDetected: Boolean = versionMismatch.get()

  /** Whether there is a live channel to write to right now. */
  def isChannelReady: Boolean = {
    val ctx = channelContext.get()
    ctx != null && ctx.channel().isActive()
  }

  private def sumOverStreams(measure: PartitionStream => Long): Long = {
    var total = 0L
    streams.values().asScala.foreach(stream => total += measure(stream))
    total
  }

  // ==========================================================================================
  // Teardown
  // ==========================================================================================

  /**
   * Releases every buffered and retained block for this shuffle.
   *
   * This is the counterpart of the writer's unsuccessful stop and of its task completion listener:
   * once the producing task is finished with the shuffle, nothing may be retained, whether the task
   * succeeded, failed or was cancelled. Channel loss deliberately does not come here, because a
   * consumer that reconnects must still be replayable.
   */
  def releaseAll(): Unit = {
    val releasedBytes = pendingBytes + unacknowledgedBytes
    egressQueue.clear()
    streams.values().asScala.foreach { stream =>
      stream.unacknowledged.clear()
      stream.unacknowledgedBytes.set(0L)
      stream.pendingBytes.set(0L)
      stream.pendingBlocks.set(0L)
    }
    streams.clear()
    if (releasedBytes > 0L || debugEnabled) {
      logInfo(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} released " +
        log"${MDC(NUM_BYTES, releasedBytes)} buffered and retained byte(s) after " +
        log"${MDC(NUM_BLOCKS, blocksWritten.get())} block(s) written")
    }
  }

  // ==========================================================================================
  // Internal helpers
  // ==========================================================================================

  /** Per partition state, created on first use without ever taking a lock. */
  private def streamFor(partitionId: Int): PartitionStream = {
    require(partitionId >= 0,
      s"A streaming shuffle partition id must be non-negative but was $partitionId.")
    val existing = streams.get(partitionId)
    if (existing != null) {
      existing
    } else {
      val created = new PartitionStream(partitionId, clock.getTimeMillis())
      val raced = streams.putIfAbsent(partitionId, created)
      if (raced != null) raced else created
    }
  }

  /**
   * Runs a Netty callback body so that no failure escapes into the event loop.
   *
   * A non-fatal failure is latched in the notifier, which is what carries it to the task thread;
   * the event loop itself sees nothing, because an exception thrown from a callback would tear
   * down the pipeline without ever reaching the task that is waiting. A fatal error is
   * deliberately not contained: those belong to the JVM, not to this handler.
   */
  private def guard(operation: => Unit): Unit = {
    try {
      operation
    } catch {
      case NonFatal(e) =>
        errorNotifier.setError(e)
        logWarning(log"Streaming shuffle ${MDC(SHUFFLE_ID, shuffleId)} contained a failure " +
          log"raised on an I/O thread; the task thread will re-throw it", e)
    }
  }
}

/**
 * Wire and pacing constants of the producer side handler, plus the transport configuration factory
 * that gives the streaming shuffle its own tuning namespace.
 *
 * The numeric constants restate the protocol's timings in one place. They are protocol facts rather
 * than tunables, and none of them is configurable: a producer and a consumer that disagreed on the
 * liveness window or on the reclamation budget would each be correct by its own reckoning and wrong
 * about the other.
 */
private[spark] object StreamingShuffleServerHandler {

  /**
   * Transport module name of the streaming shuffle.
   *
   * Asking `SparkTransportConf` for this module yields an independent
   * `spark.shuffle-streaming.io.*` namespace. That is the whole reason the name exists: thread
   * counts, buffer sizes, retry knobs and TCP keep-alive can be set for streaming alone, leaving
   * the block transfer service that every other shuffle depends on exactly as it was. The name is
   * passed as an argument and is never added to any shared file.
   */
  val TRANSPORT_MODULE_NAME: String = "shuffle-streaming"

  /**
   * The key that enables operating system keep-alive for the streaming module only.
   *
   * `TransportConf` exposes keep-alive as a boolean with no interval, and the JDK offers no socket
   * option for one, so this switch cannot express the protocol's five second bound. That bound is
   * enforced by the heartbeat timer at the application level, and never by TCP; keep-alive is
   * enabled here purely so that a connection nobody is using is eventually reaped by the kernel.
   */
  val TCP_KEEP_ALIVE_KEY: String = s"spark.$TRANSPORT_MODULE_NAME.io.enableTcpKeepAlive"

  /** Largest payload a single block may carry, being the protocol's own cap of two mebibytes. */
  val MAX_PAYLOAD_BYTES: Int = DataBlockMessage.MAX_BLOCK_SIZE_BYTES

  /** Largest number of bytes a single block occupies on the wire, framing prefix included. */
  val MAX_FRAMED_BYTES: Int = DataBlockMessage.MAX_ENCODED_FRAME_BYTES

  /**
   * Cadence at which a producer should raise a heartbeat, in milliseconds.
   *
   * It matches the consumer's connection timeout, so a healthy producer refreshes liveness exactly
   * as often as the detector requires and no more; heartbeats are the cheapest frames on the wire
   * but they are not free.
   */
  val PRODUCER_HEARTBEAT_INTERVAL_MS: Long = 5000L

  /** How long a consumer may go without acknowledging before it is treated as stalled. */
  val CONSUMER_LIVENESS_TIMEOUT_MS: Long = 10000L

  /** The budget within which an acknowledgement must have released the memory it covers. */
  val ACK_RECLAMATION_BUDGET_MS: Long = 100L

  /** First retransmission backoff, doubled on each subsequent attempt. */
  val RETRANSMIT_INITIAL_BACKOFF_MS: Long = 1000L

  /** Retransmission attempts allowed for one partition before the condition is escalated. */
  val MAX_RETRANSMIT_ATTEMPTS: Int = 5

  /** Largest doubling the backoff applies, which is one less than the attempt budget. */
  val MAX_RETRANSMIT_BACKOFF_SHIFT: Int = MAX_RETRANSMIT_ATTEMPTS - 1

  /** Starting size of the egress queue; it grows on demand and is never a bound on output. */
  val INITIAL_EGRESS_QUEUE_CAPACITY: Int = 16

  /**
   * Attributes of the task attempt that produced a block, which decide flush order.
   *
   * This is the concrete meaning of prioritising shuffle traffic over speculative execution in a
   * system that marks no packets anywhere: blocks from a first attempt are flushed ahead of blocks
   * from a retry, so a speculative copy of a task cannot delay the attempt that is most likely to
   * be the one whose output is used. The attempt number is the only signal Spark exposes for this
   * on the producer side, and it is exactly the signal `TaskContext` publishes.
   *
   * @param stageId the stage the producing attempt belongs to
   * @param stageAttemptNumber the stage attempt the producing attempt belongs to
   * @param taskAttemptId the globally unique identifier of the producing attempt
   * @param attemptNumber how many times this task has been attempted, counted from zero
   */
  final case class EgressPriority(
      stageId: Int,
      stageAttemptNumber: Int,
      taskAttemptId: Long,
      attemptNumber: Int) {

    /** Whether this is a retry or speculative copy rather than the original attempt. */
    def speculative: Boolean = attemptNumber > 0
  }

  /** The ordering used before any task attempt has been registered. */
  val DEFAULT_PRIORITY: EgressPriority = EgressPriority(0, 0, 0L, 0)

  /**
   * Builds the transport configuration for the streaming shuffle module with keep-alive enabled.
   *
   * The configuration is cloned before the keep-alive key is set, so enabling it for streaming can
   * never leak into the caller's own `SparkConf` and perturb another module. `SparkTransportConf`
   * clones again on its own account, which means the returned configuration is a snapshot: reading
   * it later cannot observe a change made afterwards, and that immutability is what makes
   * "streaming shuffle configuration changes require an executor restart" true rather than merely
   * intended.
   *
   * @param conf the executor's configuration, left untouched
   * @param numUsableCores cores this JVM may use for transport threads, or zero for the default
   * @return a configuration whose keys live under `spark.shuffle-streaming.io.*`
   */
  def streamingTransportConf(conf: SparkConf, numUsableCores: Int = 0): TransportConf = {
    val streamingConf = conf.clone
    streamingConf.set(TCP_KEEP_ALIVE_KEY, "true")
    SparkTransportConf.fromSparkConf(streamingConf, TRANSPORT_MODULE_NAME, numUsableCores)
  }

  /**
   * One block waiting for the wire, together with the ordering it is queued under.
   *
   * @param block the framed, checksummed block
   * @param priority attributes of the attempt that produced it
   * @param ticket monotonic enqueue order, which makes the ordering total and stable
   * @param framedBytes bytes the block occupies on the wire, framing prefix included
   */
  private final case class PendingBlock(
      block: DataBlockMessage,
      priority: EgressPriority,
      ticket: Long,
      framedBytes: Int)

  /**
   * Flush order: original attempts before retries, lower attempt numbers first, and enqueue order
   * between blocks of equal urgency so that a partition's blocks never leave out of sequence.
   */
  private val EgressOrdering: Comparator[PendingBlock] = new Comparator[PendingBlock] {
    override def compare(left: PendingBlock, right: PendingBlock): Int = {
      val bySpeculation =
        java.lang.Boolean.compare(left.priority.speculative, right.priority.speculative)
      if (bySpeculation != 0) {
        bySpeculation
      } else {
        val byAttempt =
          java.lang.Integer.compare(left.priority.attemptNumber, right.priority.attemptNumber)
        if (byAttempt != 0) {
          byAttempt
        } else {
          java.lang.Long.compare(left.ticket, right.ticket)
        }
      }
    }
  }

  /**
   * Egress and acknowledgement state of one reduce partition.
   *
   * Every field is atomic or concurrent, because the writer's task thread and the channel's
   * event-loop thread both reach this state and neither may wait for the other.
   *
   * @param partitionId the reduce partition this state belongs to
   * @param createdAtMs the instant the stream was opened, which seeds the liveness window
   */
  private final class PartitionStream(val partitionId: Int, createdAtMs: Long) {

    /** Next sequence number to assign, counted from zero and increasing by one per block. */
    val nextSequenceNumber: AtomicLong = new AtomicLong(0L)

    /** Blocks ever enqueued for this partition, which is the total the terminator reports. */
    val blocksEnqueued: AtomicLong = new AtomicLong(0L)

    /** Blocks queued but not yet written, including blocks re-queued for retransmission. */
    val pendingBlocks: AtomicLong = new AtomicLong(0L)

    /** Framed bytes queued but not yet written. */
    val pendingBytes: AtomicLong = new AtomicLong(0L)

    /** Framed bytes written but not yet acknowledged. */
    val unacknowledgedBytes: AtomicLong = new AtomicLong(0L)

    /**
     * The unacknowledged window: blocks written and retained against retransmission, keyed by
     * sequence number. Ordered so that the range an acknowledgement releases, and the range a
     * retransmission request asks for, are both sub-map views rather than scans.
     */
    val unacknowledged: ConcurrentSkipListMap[Long, DataBlockMessage] =
      new ConcurrentSkipListMap[Long, DataBlockMessage]()

    /** Highest sequence number the consumer has acknowledged, or nothing consumed. */
    val lastAckPosition: AtomicLong = new AtomicLong(AckMessage.NOTHING_CONSUMED)

    /** When this stream last saw an acknowledgement or a heartbeat from its consumer. */
    val lastProgressMs: AtomicLong = new AtomicLong(createdAtMs)

    /** Whether the writer has asked for end of stream. */
    val terminationRequested: AtomicBoolean = new AtomicBoolean(false)

    /** One-shot guard so the terminator is written exactly once. */
    val terminationSent: AtomicBoolean = new AtomicBoolean(false)

    /** The block total captured when termination was requested. */
    val totalBlocksAtTermination: AtomicLong = new AtomicLong(0L)

    /** Retransmission attempts serviced for this partition since the last acknowledgement. */
    val retransmitAttempts: AtomicInteger = new AtomicInteger(0)

    /** The instant from which the next retransmission may be serviced. */
    val nextRetransmitAtMs: AtomicLong = new AtomicLong(0L)

    /** One-shot guard so a reclamation budget breach is reported once per stream. */
    val reclamationWarned: AtomicBoolean = new AtomicBoolean(false)
  }
}
