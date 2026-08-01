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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{BLOCK_ID, COUNT, FILE_NAME, MAP_ID, NUM_BYTES, SHUFFLE_ID}
import org.apache.spark.internal.config.SHUFFLE_STREAMING_DEBUG
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.{ExecutorDiskUtils, MergedBlockMeta}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.shuffle.streaming.MemorySpillManager.SpilledBlock
import org.apache.spark.shuffle.streaming.StreamingShuffleBlockResolver.ProducerKey
import org.apache.spark.storage.{BlockId, BlockManager, DiskBlockManager, ShuffleBlockBatchId,
  ShuffleBlockId, ShuffleMergedBlockId, TempShuffleBlockId}
import org.apache.spark.util.Utils

/**
 * The block resolver of the streaming shuffle subsystem.
 *
 * Why this class has to exist at all. `ShuffleManager` declares `shuffleBlockResolver` as an
 * abstract member, so every manager must supply one, and the resolver trait's own documentation
 * states that a resolver is how the block store abstracts over different shuffle implementations
 * -- which is also how a custom manager co-operates with the External Shuffle Service. A streaming
 * shuffle therefore needs a resolver even though its happy path materialises nothing whatsoever:
 * records are pipelined straight from producer to consumer and no map output file is ever written.
 *
 * What it actually serves. Exactly one thing: the blocks that [[MemorySpillManager]] was forced to
 * evict to local disk when the producer's buffer budget ran short. Those are the only streaming
 * shuffle bytes that exist as durable, addressable storage, and they are addressed here through
 * the location fields of `MemorySpillManager.SpilledBlock`. Blocks still held in memory are
 * deliberately not served here: they are the protocol's retransmission window and are replayed
 * over the streaming channel instead, and their payload is raw framed bytes whereas a spilled
 * segment is a `SerializerManager`-wrapped unit, so the two are not interchangeable encodings and
 * must never be concatenated.
 *
 * Why it is deliberately not index based. This class is intentionally not an
 * `IndexShuffleBlockResolver` and intentionally does not mix in `MigratableResolver`. That single
 * decision is the whole coexistence strategy: `ShuffleWriteProcessor` decides whether to start a
 * push-based merge by pattern matching on the manager's resolver type, and any resolver that is
 * not an `IndexShuffleBlockResolver` falls through its empty default branch. Streaming therefore
 * declines to participate in push-based merge, and in block migration, without a single line of
 * change to `ShuffleWriteProcessor`, to `SortShuffleManager`, or to any other shared shuffle
 * class. Sort-based shuffle keeps its own `IndexShuffleBlockResolver`, keeps being the default and
 * keeps being the fallback: when the streaming kill switch is off, `StreamingShuffleManager`
 * exposes the sort delegate's resolver rather than this one, so the push-merge branch fires exactly
 * as it always did and behaviour is indistinguishable from sort-based shuffle.
 *
 * Lifecycle and thread safety. An instance is created once per driver or executor, from
 * `StreamingShuffleManager`'s constructor, and is then shared by every task on that JVM. All
 * mutable state is a single concurrent map plus one atomic flag, so every method is safe to call
 * from any thread, including a Netty event-loop thread. No environment object is dereferenced
 * during construction; see the deferred-access section of the implementation for why that is
 * mandatory rather than merely tidy. [[stop]] is idempotent and never throws.
 *
 * @param conf the configuration this resolver reads once and then holds immutably, which is what
 *             makes "configuration changes require an executor restart" true by construction
 * @param spillManager the producer consulted when no producer is registered under a requested
 *                     shuffle and map id. May be null, which is the normal case when the resolver
 *                     is built by `StreamingShuffleManager`: a `MemorySpillManager` is bound to a
 *                     task's `TaskMemoryManager` and so cannot exist that early. Per-task
 *                     producers are supplied later through [[registerProducer]].
 */
private[spark] class StreamingShuffleBlockResolver(
    conf: SparkConf,
    spillManager: MemorySpillManager)
  extends ShuffleBlockResolver with Logging {

  /**
   * Convenience constructor for the ordinary case, in which no producer can be known yet because
   * the shuffle manager is built long before any task exists. Mirrors the auxiliary constructors
   * `IndexShuffleBlockResolver` offers for the same reason.
   */
  def this(conf: SparkConf) = this(conf, null)

  // ----------------------------------------------------------------------------------------------
  // Configuration, read exactly once.
  // ----------------------------------------------------------------------------------------------

  /**
   * Whether the verbose per-block debug trail is emitted. Off by default, because a line per
   * served block would breach the log-volume budget this subsystem is held to.
   */
  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  // ----------------------------------------------------------------------------------------------
  // State. A producer here is one map task's MemorySpillManager, which owns that task's spill
  // files and is the only component that knows where their segments live.
  // ----------------------------------------------------------------------------------------------

  /** The constructor-supplied producer, if any. Tolerates a null argument by design. */
  private val rootProducer: Option[MemorySpillManager] = Option(spillManager)

  /** Live producers, keyed by the map output they are producing. */
  private val producers = new ConcurrentHashMap[ProducerKey, MemorySpillManager]()

  /** Guards [[stop]] so that repeated calls are harmless. */
  private val stopped = new AtomicBoolean(false)

  // ----------------------------------------------------------------------------------------------
  // Deferred environment access.
  //
  // None of these may be dereferenced while this object is being constructed. On the driver the
  // shuffle manager is initialised before the memory manager exists, and the block manager is not
  // valid until its own initialize() has run, so an eager dereference here would be a guaranteed
  // NullPointerException during SparkContext startup. A lazy val is Spark's own sanctioned remedy
  // for exactly this hazard, and IndexShuffleBlockResolver defers its block manager the same way.
  // ----------------------------------------------------------------------------------------------

  private lazy val blockManager: BlockManager = SparkEnv.get.blockManager

  private lazy val diskBlockManager: DiskBlockManager = blockManager.diskBlockManager

  /**
   * Transport configuration for the buffers this resolver hands out.
   *
   * Deliberately scoped to its own module name rather than borrowing block transfer's settings, so
   * that streaming gets an independent `spark.shuffle-streaming.io.*` namespace and can be tuned
   * -- including having TCP keepalive enabled -- without perturbing anything else. The factory
   * clones the configuration it is given, so obtaining this value mutates nothing. SSL options are
   * threaded through in the same way the sort-based resolver threads them through, so an
   * SSL-enabled deployment keeps working.
   */
  private lazy val transportConf: TransportConf = {
    val securityManager = new SecurityManager(conf)
    SparkTransportConf.fromSparkConf(
      conf,
      StreamingShuffleBlockResolver.TRANSPORT_MODULE,
      sslOptions = Some(securityManager.getRpcSSLOptions()))
  }

  // ----------------------------------------------------------------------------------------------
  // Producer registry. This is the seam the streaming writer uses: it registers itself when it
  // starts producing a map output and unregisters when the task completes, which is what lets a
  // resolver shared by the whole JVM find the one component that can locate a given spilled block.
  // ----------------------------------------------------------------------------------------------

  /**
   * Registers the producer of one map output so that its spilled blocks become servable.
   *
   * Registering the same key twice replaces the previous producer, which is the correct behaviour
   * for a retried task attempt: only the newest attempt's spill files are still valid. A
   * registration that arrives after [[stop]] is ignored rather than retained, so a late writer
   * cannot resurrect state on a resolver that is shutting down.
   *
   * @param shuffleId the shuffle being produced
   * @param mapId the map output being produced
   * @param producer the spill manager that owns that map output's buffers and spill files
   */
  def registerProducer(shuffleId: Int, mapId: Long, producer: MemorySpillManager): Unit = {
    require(producer != null, "producer must not be null")
    if (stopped.get()) {
      logDebug(log"Ignoring streaming shuffle producer registration for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)} because the block resolver " +
        log"has already been stopped")
    } else {
      producers.put(ProducerKey(shuffleId, mapId), producer)
      if (debugEnabled) {
        logDebug(log"Registered streaming shuffle producer for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)}, " +
          log"${MDC(COUNT, producers.size())} producer(s) now registered")
      }
    }
  }

  /**
   * Drops one producer, which is what a task must do on completion so that neither the producer
   * nor its buffers are reachable from this JVM-scoped object any longer.
   *
   * @return true if a producer was registered under that key and has now been dropped
   */
  def unregisterProducer(shuffleId: Int, mapId: Long): Boolean = {
    val removed = producers.remove(ProducerKey(shuffleId, mapId)) != null
    if (removed && debugEnabled) {
      logDebug(log"Unregistered streaming shuffle producer for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)}, " +
        log"${MDC(COUNT, producers.size())} producer(s) still registered")
    }
    removed
  }

  /**
   * Drops every producer of one shuffle. Called when the shuffle itself is unregistered, so that a
   * completed shuffle leaves nothing behind even if some task failed to unregister its own
   * producer.
   *
   * @return the number of producers dropped
   */
  def removeShuffle(shuffleId: Int): Int = {
    var dropped = 0
    val iterator = producers.keySet().iterator()
    while (iterator.hasNext) {
      if (iterator.next().shuffleId == shuffleId) {
        iterator.remove()
        dropped += 1
      }
    }
    if (dropped > 0 && debugEnabled) {
      logDebug(log"Dropped ${MDC(COUNT, dropped)} streaming shuffle producer(s) of shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)}")
    }
    dropped
  }

  /**
   * The producer able to locate spilled blocks of the given map output, if there is one.
   *
   * Falls back to the constructor-supplied producer when nothing is registered under the key. That
   * fallback is what makes the two-argument constructor meaningful: a single-producer embedding can
   * hand its spill manager straight to the resolver and skip registration entirely.
   */
  def producerFor(shuffleId: Int, mapId: Long): Option[MemorySpillManager] = {
    Option(producers.get(ProducerKey(shuffleId, mapId))).orElse(rootProducer)
  }

  /** How many producers are currently registered. Intended for diagnostics and tests. */
  def registeredProducerCount: Int = producers.size()

  /** Whether [[stop]] has already run. */
  def isStopped: Boolean = stopped.get()

  // ----------------------------------------------------------------------------------------------
  // ShuffleBlockResolver: block retrieval.
  // ----------------------------------------------------------------------------------------------

  /**
   * Retrieves the data for one block of streaming shuffle output.
   *
   * Three block identities are understood, and every one of them is an identity that already
   * exists. `BlockId` is a sealed hierarchy, so a bespoke streaming identity could not be declared
   * outside its own file even if one were wanted; reusing the existing identities is precisely what
   * keeps the block manager's storage contracts untouched.
   *
   *  - `ShuffleBlockId` resolves to the spilled segments of one reduce partition of one map output.
   *  - `ShuffleBlockBatchId` resolves to the spilled segments of a contiguous reduce range,
   *    partition-major and in ascending sequence order within each partition, which is the same
   *    ordering a batched fetch of sort-based output observes.
   *  - `TempShuffleBlockId` resolves to a whole spill file. This is the exact one-to-one identity,
   *    because a spill file is allocated as a temporary shuffle block and so this identity names
   *    precisely one file.
   *
   * A request that resolves to a single segment is answered with a zero-copy file segment. One that
   * resolves to several is answered with their byte-exact, ordered concatenation: a faithful
   * transfer of the requested bytes, but not itself a single decodable stream, because each segment
   * was committed on its own and is unwrapped on its own. A caller that needs those boundaries
   * should use [[getSpilledSegments]], which hands back one buffer per segment.
   *
   * @param blockId the block being requested
   * @param dirs local directories to read from instead of this executor's own. Honoured for the
   *             file-name lookup that backs `TempShuffleBlockId`, which is how a host-local or
   *             External Shuffle Service read reaches a spill file it can name but cannot find in
   *             this JVM's registry. Not applicable to the reduce-partition identities, whose
   *             segment locations are held as absolute paths by the producer that wrote them.
   * @return a buffer over the requested bytes
   */
  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]] = None): ManagedBuffer = {
    blockId match {
      case ShuffleBlockId(shuffleId, mapId, reduceId) =>
        spilledReduceRange(blockId, shuffleId, mapId, reduceId, reduceId + 1)
      case ShuffleBlockBatchId(shuffleId, mapId, startReduceId, endReduceId) =>
        spilledReduceRange(blockId, shuffleId, mapId, startReduceId, endReduceId)
      case tempBlockId: TempShuffleBlockId =>
        spillFileBuffer(tempBlockId, dirs)
      case _ =>
        // Deliberately the same phrasing and the same error condition the sort-based resolver uses
        // for an identity it does not recognise, so an operator reading a log sees one vocabulary.
        throw SparkException.internalError(
          s"unexpected shuffle block id format: $blockId", category = "SHUFFLE")
    }
  }

  /**
   * The spilled segments of one reduce partition, one buffer per segment, in ascending sequence
   * order.
   *
   * This is the segment-granular counterpart of [[getBlockData]] and the form a caller wants when
   * it intends to decode the bytes rather than merely forward them, because each segment is an
   * independently decodable unit. An empty result means that partition has nothing on disk, which
   * on the streaming happy path is the normal case rather than an error.
   *
   * @param shuffleId the shuffle the partition belongs to
   * @param mapId the map output the partition belongs to
   * @param reduceId the reduce partition to report on; must be non-negative
   */
  def getSpilledSegments(shuffleId: Int, mapId: Long, reduceId: Int): Seq[ManagedBuffer] = {
    require(reduceId >= 0, s"reduceId must be non-negative, but was $reduceId")
    producerFor(shuffleId, mapId) match {
      case Some(producer) =>
        producer.spilledBlocks(reduceId).map(segment => segmentBuffer(segment))
      case None =>
        Seq.empty
    }
  }

  /**
   * Answers a reduce-partition or reduce-range request from the spilled segments the owning
   * producer still retains.
   *
   * @param blockId the identity being served, carried through solely so that every diagnostic
   *                names the block the caller actually asked for
   * @param shuffleId the shuffle being read
   * @param mapId the map output being read
   * @param startReduceId first reduce partition of the range, inclusive
   * @param endReduceId last reduce partition of the range, exclusive
   */
  private def spilledReduceRange(
      blockId: BlockId,
      shuffleId: Int,
      mapId: Long,
      startReduceId: Int,
      endReduceId: Int): ManagedBuffer = {
    if (startReduceId < 0 || endReduceId <= startReduceId) {
      throw SparkException.internalError(
        s"invalid reduce range [$startReduceId, $endReduceId) requested for block $blockId",
        category = "SHUFFLE")
    }
    val producer = producerFor(shuffleId, mapId).getOrElse {
      throw SparkException.internalError(
        s"no streaming shuffle producer is registered for block $blockId, so none of its data " +
          "can be served from local storage; the producing task either never spilled or has " +
          "already released its buffers", category = "SHUFFLE")
    }
    val segments = (startReduceId until endReduceId)
      .flatMap(reduceId => producer.spilledBlocks(reduceId))
    if (segments.isEmpty) {
      throw SparkException.internalError(
        s"streaming shuffle holds no spilled data for block $blockId; blocks that are still " +
          "buffered in memory are replayed by retransmission over the streaming channel and are " +
          "deliberately not served from here", category = "SHUFFLE")
    }
    val buffer: ManagedBuffer = if (segments.length == 1) {
      // The overwhelmingly common shape, and the one worth optimising: a single committed segment
      // is handed out as a file segment, so the bytes are never copied through the heap at all.
      segmentBuffer(segments.head)
    } else {
      new NioManagedBuffer(concatenateSegments(blockId, segments))
    }
    if (debugEnabled) {
      logDebug(log"Streaming shuffle served block ${MDC(BLOCK_ID, blockId)} from " +
        log"${MDC(COUNT, segments.length)} spilled segment(s) totalling " +
        log"${MDC(NUM_BYTES, buffer.size())} bytes")
    }
    buffer
  }

  /**
   * Reads several committed segments into one heap buffer, preserving their bytes and their order
   * exactly.
   *
   * The assembled size is bounded by construction: a single buffer cannot address more than
   * `Int.MaxValue` bytes, so a request beyond that is refused with an actionable message rather
   * than allowed to fail as an arithmetic overflow or an allocation error.
   */
  private def concatenateSegments(blockId: BlockId, segments: Seq[SpilledBlock]): ByteBuffer = {
    val totalBytes = segments.foldLeft(0L)((accumulated, segment) => accumulated + segment.length)
    if (totalBytes > StreamingShuffleBlockResolver.MAX_ASSEMBLED_BYTES) {
      throw SparkException.internalError(
        s"streaming shuffle cannot assemble $totalBytes bytes of spilled data into a single " +
          s"buffer for block $blockId, because one buffer cannot exceed " +
          s"${StreamingShuffleBlockResolver.MAX_ASSEMBLED_BYTES} bytes; request the segments " +
          "individually instead", category = "SHUFFLE")
    }
    val assembled = ByteBuffer.allocate(totalBytes.toInt)
    segments.foreach { segment =>
      assembled.put(segmentBuffer(segment).nioByteBuffer())
    }
    assembled.flip()
    assembled
  }

  /** Wraps one committed spill segment as a buffer, without reading it. */
  private def segmentBuffer(segment: SpilledBlock): FileSegmentManagedBuffer = {
    new FileSegmentManagedBuffer(transportConf, segment.file, segment.offset, segment.length)
  }

  /**
   * Answers a request for a whole spill file, which is what a temporary shuffle block identity
   * names.
   *
   * A file may still hold segments the consumer has already acknowledged, because a spill file is
   * only deleted once no retained record refers to it. Serving the whole file is nonetheless
   * byte-valid: every byte in it was committed by this subsystem, and the caller asked for the file
   * rather than for a segment of it.
   */
  private def spillFileBuffer(
      blockId: TempShuffleBlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    val file = locateSpillFile(blockId, dirs)
    val length = file.length()
    if (debugEnabled) {
      logDebug(log"Streaming shuffle served spill file ${MDC(FILE_NAME, file.getName)} for " +
        log"block ${MDC(BLOCK_ID, blockId)}, ${MDC(NUM_BYTES, length)} bytes")
    }
    new FileSegmentManagedBuffer(transportConf, file, 0L, length)
  }

  /**
   * Finds the file backing a temporary shuffle block: first through the producers this resolver
   * knows about, which is authoritative, and only then by name on local disk, which is how a
   * caller that knows the block name but not this JVM's registry is answered.
   */
  private def locateSpillFile(blockId: TempShuffleBlockId, dirs: Option[Array[String]]): File = {
    val registered = allProducers.iterator
      .flatMap(producer => producer.allSpilledBlocks.iterator)
      .find(segment => segment.blockId == blockId)
      .map(segment => segment.file)
    registered.orElse(locateSpillFileByName(blockId, dirs)).filter(file => file.isFile).getOrElse {
      throw SparkException.internalError(
        s"streaming shuffle has no spill file for block $blockId on this executor",
        category = "SHUFFLE")
    }
  }

  /**
   * Resolves a block name to a local file, honouring caller-supplied directories exactly as the
   * sort-based resolver does.
   *
   * Never throws. Reaching the disk block manager needs a live environment, and a caller may be
   * running somewhere that has none; that is not an error condition here, it simply means the file
   * cannot be located this way and the caller is told there is no such block.
   */
  private def locateSpillFileByName(
      blockId: TempShuffleBlockId,
      dirs: Option[Array[String]]): Option[File] = {
    val fileName = blockId.name
    try {
      val file = dirs
        .map(localDirs => new File(
          ExecutorDiskUtils.getFilePath(localDirs, blockManager.subDirsPerLocalDir, fileName)))
        .getOrElse(diskBlockManager.getFile(fileName))
      Some(file)
    } catch {
      case NonFatal(e) =>
        logDebug(log"Could not locate a streaming shuffle spill file named " +
          log"${MDC(FILE_NAME, fileName)} for block ${MDC(BLOCK_ID, blockId)}", e)
        None
    }
  }

  /**
   * Every producer this resolver may consult, registered ones first and the constructor-supplied
   * one last. De-duplicated by identity, because the same producer may be both.
   */
  private def allProducers: Seq[MemorySpillManager] = {
    (producers.values().asScala.toSeq ++ rootProducer.toSeq).distinct
  }

  // ----------------------------------------------------------------------------------------------
  // ShuffleBlockResolver: push-based merge. Streaming declines to participate, by design.
  // ----------------------------------------------------------------------------------------------

  /**
   * Always fails, because streaming shuffle never produces merged output.
   *
   * This is not a gap. A push-based merge is only ever started for a manager whose resolver is an
   * `IndexShuffleBlockResolver`, and this resolver deliberately is not one, so nothing on the
   * streaming path can create a merged block for this method to serve. Reaching it means a merged
   * block was attributed to a streaming shuffle, which is a real programming error and is reported
   * as one rather than disguised as an empty result.
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    throw new UnsupportedOperationException(
      "Streaming shuffle does not participate in push-based shuffle merge, so it never produces " +
        s"merged shuffle data and cannot serve $blockId. Set spark.shuffle.manager to sort, or " +
        "set spark.shuffle.streaming.enabled to false, if push-based merge is required.")
  }

  /**
   * Always fails, for the same reason as [[getMergedBlockData]].
   *
   * A synthetic `MergedBlockMeta` is deliberately not fabricated. Its only constructor demands a
   * chunk count and a non-null chunk bitmap, so any value invented here would be an assertion
   * about merged output that does not exist.
   */
  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    throw new UnsupportedOperationException(
      "Streaming shuffle does not participate in push-based shuffle merge, so it never produces " +
        s"merged shuffle metadata and cannot serve $blockId. Set spark.shuffle.manager to sort, " +
        "or set spark.shuffle.streaming.enabled to false, if push-based merge is required.")
  }

  // ----------------------------------------------------------------------------------------------
  // ShuffleBlockResolver: enumeration and lifecycle.
  // ----------------------------------------------------------------------------------------------

  /**
   * The locally stored blocks of one map output, which for streaming shuffle means the spill files
   * that map output still holds on disk, de-duplicated because one file carries several segments.
   *
   * Overriding the trait's empty default is worthwhile here because these blocks genuinely are
   * enumerable: the owning producer records the identity of every file it wrote. The External
   * Shuffle Service uses this list to delete a removed executor's shuffle files, so returning the
   * real list is what lets streaming spill files be reclaimed rather than orphaned.
   */
  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    producerFor(shuffleId, mapId) match {
      case Some(producer) =>
        producer.allSpilledBlocks.map(segment => segment.blockId).distinct
      case None =>
        Seq.empty
    }
  }

  /**
   * Releases everything this resolver holds.
   *
   * Idempotent and non-throwing, both deliberately. It is invoked from the shuffle manager's own
   * `stop`, exactly as the sort-based manager invokes its resolver's, and a manager that is
   * shutting down must never be derailed by a resolver that objects.
   *
   * Spill files are not deleted here. Ownership of them rests with the producer that wrote them,
   * which deletes each file once no retained record still refers to it and which registers its own
   * task-completion cleanup for the failure and cancellation cases. Deleting from here would race a
   * producer that is still replaying its unacknowledged window.
   */
  override def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      Utils.tryLogNonFatalError {
        val dropped = producers.size()
        producers.clear()
        if (dropped > 0) {
          logDebug(log"Streaming shuffle block resolver stopped, dropping " +
            log"${MDC(COUNT, dropped)} registered producer(s)")
        }
      }
    }
  }

  /** A diagnostic rendering built only from state this object already holds. */
  override def toString: String = {
    s"StreamingShuffleBlockResolver(registeredProducers=$registeredProducerCount, " +
      s"hasRootProducer=${rootProducer.isDefined}, stopped=$isStopped)"
  }
}

/**
 * Constants and the registry key of [[StreamingShuffleBlockResolver]].
 */
private[spark] object StreamingShuffleBlockResolver {

  /**
   * Transport module name of the streaming shuffle, which yields the independent
   * `spark.shuffle-streaming.io.*` configuration namespace. It is passed as an argument to the
   * transport configuration factory and never added to it, so no shared networking code changes
   * and no existing block transfer tuning is perturbed.
   */
  val TRANSPORT_MODULE: String = "shuffle-streaming"

  /**
   * The largest number of bytes that may be assembled into a single buffer. This is not a tuning
   * knob but a hard limit of the buffer abstraction itself, since an array-backed buffer is
   * addressed by an `Int`.
   */
  val MAX_ASSEMBLED_BYTES: Long = Int.MaxValue.toLong

  /**
   * Identifies the producer of one map output.
   *
   * @param shuffleId the shuffle being produced
   * @param mapId the map output being produced
   */
  case class ProducerKey(shuffleId: Int, mapId: Long)
}
