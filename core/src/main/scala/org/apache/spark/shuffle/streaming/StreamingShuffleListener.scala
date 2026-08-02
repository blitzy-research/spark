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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{COUNT, DESCRIPTION, ERROR, HOST_PORT, MAP_ID, PORT, SHUFFLE_ID}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager, TransportServer}
import org.apache.spark.network.shuffle.protocol.streaming.StreamingShuffleMessage

/**
 * The one streaming shuffle listener an executor binds, shared by every producer running on it.
 *
 * <b>Why this exists at all.</b> Each map task needs a reachable endpoint from which its output can
 * be streamed, and the obvious implementation -- let every map task bind its own listener -- is
 * wrong twice over, for reasons that are properties of the platform rather than of this subsystem.
 *
 *  1. <b>A listener per task multiplies threads and sockets.</b> `TransportServer.init()`
 *     unconditionally creates its own boss and worker event loop groups, sized from
 *     `spark.shuffle-streaming.io.serverThreads` which defaults to twice the core count. Sharing a
 *     [[org.apache.spark.network.TransportContext]] between servers does not share those groups,
 *     because they are created per server. On a sixteen core executor running a hundred map tasks
 *     that is thousands of threads and a hundred listening sockets for a subsystem that needs one.
 *  2. <b>A listener per task dies before any consumer can use it.</b> The DAG scheduler submits a
 *     stage only once it has no missing parents, so a reduce task never starts until its whole map
 *     stage has finished. A listener whose lifetime is its map task's is therefore guaranteed to be
 *     gone by the time the first consumer tries to connect. The scheduler is an absolute
 *     preservation zone, so the resolution is not to change when reduce tasks start; it is to make
 *     the serving endpoint outlive the task that produced the bytes, which the retained output
 *     already does.
 *
 * This listener is consequently owned by [[StreamingShuffleManager]] and lives as long as the
 * executor's shuffle manager does. It holds no output of its own: it is a router, and everything it
 * routes to belongs to [[StreamingShuffleBlockResolver]], whose lifetime is the shuffle's.
 *
 * <b>What routing requires.</b> A frame has to be attributed to the producer it concerns before
 * anything acts on it, and the protocol makes that a two field lookup: the header carries the
 * shuffle id and the map id at fixed offsets, so
 * [[org.apache.spark.network.shuffle.protocol.streaming.StreamingShuffleMessage#peekShuffleId]] and
 * its map id counterpart answer without allocating, without decoding a body, and without disturbing
 * the buffer that is then handed on to the real handler. A frame naming a producer that is not
 * registered is dropped rather than escalated -- see [[dropUnroutable]] for why.
 *
 * <b>Channel fan-out.</b> One consumer channel may legitimately carry frames for several producers,
 * because a reduce task reads every map output of its shuffle and the executor exposes one port for
 * all of them. Losing that channel therefore has to be reported to each producer handler that has
 * seen traffic on it, and to no others: telling an uninvolved handler that a channel it never
 * served has closed would have it drop a session that does not exist, and worse, would let one
 * consumer's disconnection be attributed to producers it never spoke to. The
 * [[channelParticipants]] map is exactly the set of handlers a channel has actually reached.
 *
 * <b>Thread safety.</b> Every method here runs on a Netty event loop thread and every collection is
 * concurrent. Nothing blocks, and nothing throws into the event loop: failures are contained the
 * same way the per-producer handler contains them, because an exception escaping a callback would
 * tear down a pipeline shared by every producer on the executor.
 *
 * @param conf the executor's configuration, used only to build the transport tuning namespace
 */
private[spark] class StreamingShuffleListener(conf: SparkConf) extends RpcHandler with Logging {

  import StreamingShuffleListener._

  require(conf != null, "The Spark configuration must not be null.")

  /**
   * The producer handlers this executor is currently serving, keyed by shuffle and map id.
   *
   * Registration is driven by `getWriter` and removal by generation withdrawal, by the shuffle
   * being unregistered, or by the manager stopping -- never by task completion, which is the whole
   * point of the class.
   */
  private val producers = new ConcurrentHashMap[ProducerKey, StreamingShuffleServerHandler]()

  /**
   * Which producer handlers each consumer channel has reached, keyed by the channel's own id.
   *
   * Populated as frames arrive rather than on channel activation, because until a frame names a
   * producer there is nothing to record and allocating for a peer that has asked for nothing is the
   * unbounded-state exposure this subsystem avoids everywhere else.
   */
  private val channelParticipants =
    new ConcurrentHashMap[String, ConcurrentHashMap[ProducerKey, StreamingShuffleServerHandler]]()

  /** Frames dropped because they named a producer this executor is not serving. */
  private val unroutableFrames = new AtomicLong(0L)

  /** Whether an unroutable frame has been logged, so the log budget is not spent on them. */
  private val unroutableReported = new AtomicBoolean(false)

  /**
   * This router serves no chunked streams, only one-way messages, so it offers an ordinary empty
   * stream manager. A real instance rather than null, because the transport dereferences it
   * unconditionally when a stream request arrives.
   */
  private val streamManager = new OneForOneStreamManager()

  override def getStreamManager(): StreamManager = streamManager

  // ---------------------------------------------------------------------------------------------
  // Registration, driven by the manager
  // ---------------------------------------------------------------------------------------------

  /**
   * Begins routing frames for one producer.
   *
   * A second registration for the same producer replaces the first, which is the correct answer for
   * a re-attempted map task: the newer attempt's handler is the one bound to the retained output's
   * current generation, and the older one can only serve output that has already been withdrawn.
   *
   * @param shuffleId the shuffle whose output the handler serves
   * @param mapId the map task whose output the handler serves
   * @param handler the producer side handler to route to
   */
  def register(shuffleId: Int, mapId: Long, handler: StreamingShuffleServerHandler): Unit = {
    require(handler != null, "The streaming shuffle producer handler must not be null.")
    val key = ProducerKey(shuffleId, mapId)
    val previous = producers.put(key, handler)
    if (previous != null && previous.ne(handler)) {
      // Superseded rather than duplicated: forget the old handler on every channel that had reached
      // it, so a channel closing later cannot deliver a loss notice to a handler no longer serving.
      channelParticipants.values().asScala.foreach(_.remove(key, previous))
    }
    logInfo(log"Streaming shuffle listener is serving shuffle ${MDC(SHUFFLE_ID, shuffleId)} map " +
      log"${MDC(MAP_ID, mapId)}; ${MDC(COUNT, producers.size())} producer(s) registered")
  }

  /**
   * Stops routing frames for one producer and releases what its handler still holds.
   *
   * Called when a generation is withdrawn or a task fails -- both cases in which the output itself
   * is going away -- so releasing the handler here is correct. It is deliberately not called on the
   * success path: a successful map task's output is exactly the output a consumer has yet to read.
   *
   * @param shuffleId the shuffle whose producer is being withdrawn
   * @param mapId the map task whose producer is being withdrawn
   * @return true if a handler was registered and has now been released
   */
  def deregister(shuffleId: Int, mapId: Long): Boolean = {
    val key = ProducerKey(shuffleId, mapId)
    val removed = producers.remove(key)
    if (removed != null) {
      channelParticipants.values().asScala.foreach(_.remove(key))
      guard(s"release the producer handler of shuffle $shuffleId map $mapId")(removed.releaseAll())
      logInfo(log"Streaming shuffle listener stopped serving shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)}; " +
        log"${MDC(COUNT, producers.size())} producer(s) remain")
    }
    removed != null
  }

  /**
   * Stops routing every producer of one shuffle, which is how `unregisterShuffle` reaches them.
   *
   * @param shuffleId the shuffle being unregistered
   * @return how many producers were released
   */
  def deregisterShuffle(shuffleId: Int): Int = {
    val keys = producers.keySet().asScala.filter(_.shuffleId == shuffleId).toSeq
    keys.count(key => deregister(key.shuffleId, key.mapId))
  }

  /** Releases every producer this listener is serving, for the manager's own shutdown. */
  def releaseAll(): Unit = {
    producers.keySet().asScala.toSeq.foreach(key => deregister(key.shuffleId, key.mapId))
    channelParticipants.clear()
  }

  /** How many producers this executor is currently serving. */
  def producerCount: Int = producers.size()

  /** How many frames have been dropped for naming a producer this executor does not serve. */
  def unroutableFrameCount: Long = unroutableFrames.get()

  /** Whether a producer is currently routed, for the manager's own bookkeeping. */
  def serves(shuffleId: Int, mapId: Long): Boolean =
    producers.containsKey(ProducerKey(shuffleId, mapId))

  // ---------------------------------------------------------------------------------------------
  // Transport callbacks
  // ---------------------------------------------------------------------------------------------

  /**
   * Routes one control frame that arrived as a one-way message.
   *
   * The frame's buffer is passed on untouched: both peeks read at absolute indices, so the handler
   * that receives it sees the buffer exactly as the transport delivered it.
   */
  override def receive(client: TransportClient, message: ByteBuffer): Unit = {
    guard("route a one-way streaming shuffle frame")(route(client, message))
  }

  /**
   * Routes one control frame that arrived as a request expecting a reply.
   *
   * The reply is empty and is sent whether or not the frame could be routed, because a peer left
   * waiting would spend its whole RPC timeout on an answer this protocol never intended to send.
   */
  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    guard("route a request-shaped streaming shuffle frame")(route(client, message))
    callback.onSuccess(ByteBuffer.allocate(0))
  }

  /**
   * Notes a consumer channel, without allocating anything for it.
   *
   * Nothing is recorded here on purpose. Which producers a channel concerns is not known until it
   * says so, and a channel that has named none has asked for nothing.
   */
  override def channelActive(client: TransportClient): Unit = {
    guard("note a new streaming shuffle channel") {
      logInfo(log"Streaming shuffle listener accepted a channel from " +
        log"${MDC(HOST_PORT, client.getSocketAddress())}; " +
        log"${MDC(COUNT, producers.size())} producer(s) registered")
    }
  }

  /**
   * Reports the loss of a consumer channel to exactly the producers it had reached.
   *
   * Each participating handler retains what that consumer had not acknowledged, so a reconnection
   * resumes rather than forcing the upstream stage to be recomputed. Handlers the channel never
   * reached are not told, because the channel's loss says nothing about them.
   */
  override def channelInactive(client: TransportClient): Unit = {
    guard("report the loss of a streaming shuffle channel") {
      val participants = channelParticipants.remove(channelKeyOf(client))
      if (participants != null) {
        participants.values().asScala.foreach { handler =>
          guard("report a channel loss to a producer handler")(handler.channelInactive(client))
        }
      }
    }
  }

  /**
   * Reports a channel level failure to exactly the producers that channel had reached.
   *
   * Only those handlers latch the failure, and only that channel is closed by them. A fault on one
   * consumer's connection says nothing about the other consumers or the other producers, and
   * broadcasting it would turn one channel's problem into every map output's problem.
   */
  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    guard("report a streaming shuffle channel failure") {
      val participants = channelParticipants.remove(channelKeyOf(client))
      if (participants == null || participants.isEmpty) {
        logWarning(log"Streaming shuffle listener saw " +
          log"${MDC(ERROR, cause.getMessage())} on a channel from " +
          log"${MDC(HOST_PORT, client.getSocketAddress())} that had reached no producer; " +
          log"closing it", cause)
        guard("close a failed streaming shuffle channel")(client.getChannel().close())
      } else {
        participants.values().asScala.foreach { handler =>
          guard("report a channel failure to a producer handler") {
            handler.exceptionCaught(cause, client)
          }
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Internals
  // ---------------------------------------------------------------------------------------------

  /**
   * Hands one frame to the producer handler it names, recording the channel's participation.
   *
   * Participation is recorded before the handler runs, so that a frame which provokes a failure
   * still leaves the channel attributable: otherwise a handler could latch an error for a channel
   * whose later loss it would never be told about.
   */
  private def route(client: TransportClient, message: ByteBuffer): Unit = {
    val shuffleId = StreamingShuffleMessage.peekShuffleId(message)
    val mapId = StreamingShuffleMessage.peekMapId(message)
    val key = ProducerKey(shuffleId, mapId)
    val handler = producers.get(key)
    if (handler == null) {
      dropUnroutable(client, shuffleId, mapId)
    } else {
      participantsOf(client).put(key, handler)
      handler.receive(client, message)
    }
  }

  /**
   * Counts and drops a frame naming a producer this executor is not serving.
   *
   * Dropped rather than escalated, and the distinction matters. A consumer legitimately holds a
   * producer address for as long as its rendezvous answer says so, and a producer can stop being
   * served at any moment -- its generation withdrawn, its shuffle unregistered, its executor's
   * manager stopped. A frame that arrives in that window is stale, not hostile, and failing
   * something over it would turn ordinary lifecycle timing into a recomputed stage. The consumer
   * discovers the producer is gone through its own liveness timer, which is the mechanism that
   * exists for exactly this.
   *
   * The first occurrence is logged and the rest are counted, so a peer that sends nothing but
   * unroutable frames cannot spend the executor's log budget.
   */
  private def dropUnroutable(client: TransportClient, shuffleId: Int, mapId: Long): Unit = {
    unroutableFrames.incrementAndGet()
    if (unroutableReported.compareAndSet(false, true)) {
      logWarning(log"Streaming shuffle listener dropped a frame from " +
        log"${MDC(HOST_PORT, client.getSocketAddress())} addressed to shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)}, which this executor is not " +
        log"serving. Further occurrences are counted but not logged")
    }
  }

  /** The participation set of one channel, created on first use without taking a lock. */
  private def participantsOf(
      client: TransportClient): ConcurrentHashMap[ProducerKey, StreamingShuffleServerHandler] = {
    val key = channelKeyOf(client)
    val existing = channelParticipants.get(key)
    if (existing != null) {
      existing
    } else {
      val created = new ConcurrentHashMap[ProducerKey, StreamingShuffleServerHandler]()
      val raced = channelParticipants.putIfAbsent(key, created)
      if (raced != null) raced else created
    }
  }

  /**
   * The routing key of one channel.
   *
   * The channel's own id, matching what the per-producer handler keys its sessions by, so that a
   * reconnection is a different channel here as well and a late teardown of the connection that was
   * lost cannot dispossess the one that replaced it.
   */
  private def channelKeyOf(client: TransportClient): String =
    client.getChannel().id().asLongText()

  /**
   * Runs one callback body so that no failure escapes into the event loop.
   *
   * This pipeline is shared by every producer on the executor, so an exception thrown from a
   * callback would not fail one map task -- it would tear down the listener all of them depend on.
   * A fatal `Error` is deliberately allowed to propagate; those belong to the JVM.
   */
  private def guard(operation: String)(body: => Any): Unit = {
    try {
      body
    } catch {
      case NonFatal(e) =>
        logWarning(log"Streaming shuffle listener failed to " +
          log"${MDC(DESCRIPTION, operation)}; the listener stays up and the affected consumer's " +
          log"own liveness timer recovers", e)
    }
  }
}

/**
 * Construction of the executor's single streaming listener, and the identity its router keys on.
 */
private[spark] object StreamingShuffleListener extends Logging {

  /** Bind to an ephemeral port: the chosen port is published through the coordinator. */
  private val EPHEMERAL_PORT = 0

  /**
   * Binds the executor's one streaming shuffle listener.
   *
   * The transport configuration comes from the same builder the consumer side uses, so keepalive
   * and the rest of the streaming tuning namespace cannot be configured differently on the two ends
   * of a channel. Server bootstraps carry the authentication and encryption the operator has asked
   * for, so a consumer is authenticated before this listener routes a single frame.
   *
   * @param conf the executor's configuration
   * @return the router, the transport context that owns the pipeline, and the bound server
   */
  def bind(conf: SparkConf): (StreamingShuffleListener, TransportContext, TransportServer) = {
    val listener = new StreamingShuffleListener(conf)
    val transportConf = StreamingShuffleServerHandler.streamingTransportConf(conf)
    val transportContext = new TransportContext(transportConf, listener)
    val server = transportContext.createServer(EPHEMERAL_PORT,
      StreamingShuffleServerHandler.streamingServerBootstraps(transportConf))
    logInfo(log"Streaming shuffle listener bound one port for this executor: " +
      log"${MDC(PORT, server.getPort)}")
    (listener, transportContext, server)
  }

  /**
   * The identity of one producer, which is what a frame is routed on.
   *
   * Shuffle and map id together, and nothing else. Not the task attempt id: a frame's header does
   * not carry one, and it must not, because a consumer holding an address for an earlier attempt
   * has to be able to reach whichever attempt is currently registered rather than be refused for
   * naming the attempt it was told about. Which attempt's output is served is decided inside the
   * handler, against the retained store's current generation.
   */
  private[streaming] case class ProducerKey(shuffleId: Int, mapId: Long) {
    override def toString: String = s"shuffle $shuffleId map $mapId"
  }
}
