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

import java.io.{ByteArrayOutputStream, File, InputStream}
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{BLOCK_ID, COUNT, FILE_NAME, MAP_ID, NUM_BYTES,
  SHUFFLE_ID, TASK_ATTEMPT_ID}
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, SHUFFLE_STREAMING_DEBUG,
  SHUFFLE_STREAMING_ENABLED}
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer,
  NioManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.{ExecutorDiskUtils, MergedBlockMeta}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.shuffle.streaming.MemorySpillManager.SpilledBlock
import org.apache.spark.shuffle.streaming.StreamingShuffleBlockResolver.{LeasedSegmentBuffer,
  ProducerKey, RegisteredProducer}
import org.apache.spark.storage.{BlockId, BlockManager, DiskBlockManager, ShuffleBlockBatchId,
  ShuffleBlockId, ShuffleMergedBlockId, TempShuffleBlockId}
import org.apache.spark.util.Utils

/**
 * The block resolver of the streaming shuffle subsystem, which serves the blocks a producer spilled
 * or made durable rather than the index-and-data files the sort-based path writes.
 *
 * `ShuffleManager` requires a resolver, and this is deliberately not an `IndexShuffleBlockResolver`
 * subtype: the shared write path pattern-matches on that type to decide whether a map output takes
 * part in push-based merge, so a resolver of another type makes the streaming path decline to
 * participate without any edit to that path.
 *
 * It is executor-scoped and outlives the tasks whose output it serves, which is why a producer
 * registers itself here and why every lookup is checked against the generation that registered it:
 * a superseded attempt must never serve the bytes of the attempt that replaced it.
 *
 * @param conf the configuration this resolver reads once and then holds immutably, which is
 *     what makes "configuration changes require an executor restart" true by construction
 * @param spillManager the producer consulted when no producer is registered under a requested
 *     shuffle and map id.
 */
private[spark] class StreamingShuffleBlockResolver(
    conf: SparkConf,
    spillManager: MemorySpillManager)
  extends ShuffleBlockResolver with Logging {

  /**
   * Convenience constructor for the ordinary case, in which no producer can be known yet because
   * the shuffle manager is built long before any task exists.
   */
  def this(conf: SparkConf) = this(conf, null)

  // Configuration, read exactly once.

  /** Whether the verbose per-block debug trail is emitted. */
  private val debugEnabled: Boolean = conf.get(SHUFFLE_STREAMING_DEBUG)

  // State.

  /** The one monitor that makes this object's lifecycle atomic. */
  private val lifecycle = new Object

  /** The constructor-supplied producer, if any. */
  private val rootProducer: Option[MemorySpillManager] = Option(spillManager)

  /**
   * Live producers, keyed by the map output they are producing and carrying the generation that
   * registered them.
   */
  private val producers = new ConcurrentHashMap[ProducerKey, RegisteredProducer]()

  /** Whether [[stop]] has run. */
  private var stopped: Boolean = false

  // Deferred environment access.

  private lazy val blockManager: BlockManager = SparkEnv.get.blockManager

  private lazy val diskBlockManager: DiskBlockManager = blockManager.diskBlockManager

  /** Transport configuration for the buffers this resolver hands out. */
  private lazy val transportConf: TransportConf = {
    val securityManager = new SecurityManager(conf)
    SparkTransportConf.fromSparkConf(
      conf,
      StreamingShuffleBlockResolver.TRANSPORT_MODULE,
      sslOptions = Some(securityManager.getRpcSSLOptions()))
  }

  // Producer registry.

  /**
   * Registers the producer of one map output so that its spilled blocks become servable.
   *
   * @param shuffleId the shuffle being produced
   * @param mapId the map output being produced
   * @param taskAttemptId the generation registering, which is the producing task's attempt id.
   * @param producer the spill manager that owns that map output's buffers and spill files
   * @return true when the registration took effect, false when it was declined because a newer
   *     generation is already registered or because this resolver has been stopped
   */
  def registerProducer(
      shuffleId: Int,
      mapId: Long,
      taskAttemptId: Long,
      producer: MemorySpillManager): Boolean = {
    require(producer != null, "producer must not be null")
    require(taskAttemptId >= 0L, s"taskAttemptId must be non-negative, but was $taskAttemptId")
    val key = ProducerKey(shuffleId, mapId)
    val incoming = RegisteredProducer(taskAttemptId, producer)
    var declinedBy = -1L
    val accepted = lifecycle.synchronized {
      if (stopped) {
        false
      } else {
        // compute() decides and mutates in one step, so the comparison can never be made against a
        // generation that a concurrent registration has already replaced.
        val resolved = producers.compute(key, (_, existing) => {
          if (existing == null || incoming.supersedes(existing)) incoming else existing
        })
        if (resolved eq incoming) {
          true
        } else {
          declinedBy = resolved.taskAttemptId
          false
        }
      }
    }
    if (accepted) {
      if (debugEnabled) {
        logDebug(log"Registered streaming shuffle producer for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)} generation " +
          log"${MDC(TASK_ATTEMPT_ID, taskAttemptId)}, ${MDC(COUNT, registeredProducerCount)} " +
          log"producer(s) now registered")
      }
    } else if (declinedBy >= 0L) {
      // Not a debug-gated line: a stale attempt still trying to register is worth seeing, because
      // it means a superseded task is still running and still holding buffers.
      logWarning(log"Declined streaming shuffle producer registration for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)} from superseded generation " +
        log"${MDC(TASK_ATTEMPT_ID, taskAttemptId)}; generation " +
        log"${MDC(COUNT, declinedBy)} is registered")
    } else if (debugEnabled) {
      logDebug(log"Ignoring streaming shuffle producer registration for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)} because the block resolver " +
        log"has already been stopped")
    }
    accepted
  }

  /**
   * Drops one producer, which is what a task must do on completion so that neither the producer nor
   * its buffers are reachable from this JVM-scoped object any longer.
   *
   * @param shuffleId the shuffle that was produced
   * @param mapId the map output that was produced
   * @param taskAttemptId the generation retracting its registration
   * @return true if that exact generation was registered and has now been dropped
   */
  def unregisterProducer(shuffleId: Int, mapId: Long, taskAttemptId: Long): Boolean = {
    val key = ProducerKey(shuffleId, mapId)
    var dropped = false
    var orphanedFiles: Seq[File] = Seq.empty
    var orphanedProducer: MemorySpillManager = null
    lifecycle.synchronized {
      // compute() removes the mapping when the remapping function returns null, so the generation
      // comparison and the removal are one indivisible step against the current entry.
      producers.compute(key, (_, existing) => {
        if (existing != null && existing.taskAttemptId == taskAttemptId) {
          dropped = true
          orphanedFiles = existing.retainedFiles
          orphanedProducer = existing.producer
          null
        } else {
          existing
        }
      })
    }
    if (orphanedProducer != null) {
      orphanedProducer.releaseRetainedConsumerState()
    }
    // Outside the monitor: unlinking a file performs I/O, and this registry's monitor is contended
    // by every registration and every lookup in the JVM.
    deleteRetainedFiles(orphanedFiles)
    if (debugEnabled) {
      if (dropped) {
        logDebug(log"Unregistered streaming shuffle producer for shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)} generation " +
          log"${MDC(TASK_ATTEMPT_ID, taskAttemptId)}, ${MDC(COUNT, registeredProducerCount)} " +
          log"producer(s) still registered")
      } else {
        logDebug(log"Left the streaming shuffle producer of shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)} in place: generation " +
          log"${MDC(TASK_ATTEMPT_ID, taskAttemptId)} is not the registered one")
      }
    }
    dropped
  }

  /**
   * Drops every producer of one shuffle.
   *
   * @return the number of producers dropped
   */
  def removeShuffle(shuffleId: Int): Int = {
    var dropped = 0
    val orphanedFiles = new mutable.ArrayBuffer[File]()
    val orphanedProducers = new mutable.ArrayBuffer[MemorySpillManager]()
    lifecycle.synchronized {
      val iterator = producers.entrySet().iterator()
      while (iterator.hasNext) {
        val entry = iterator.next()
        if (entry.getKey.shuffleId == shuffleId) {
          orphanedFiles ++= entry.getValue.retainedFiles
          orphanedProducers += entry.getValue.producer
          iterator.remove()
          dropped += 1
        }
      }
    }
    orphanedProducers.distinct.foreach(_.releaseRetainedConsumerState())
    // Every retained file of every generation of this shuffle goes, which is one of the three
    // boundaries at which retained output is specified to be released.
    deleteRetainedFiles(orphanedFiles.toSeq)
    if (dropped > 0 && debugEnabled) {
      logDebug(log"Dropped ${MDC(COUNT, dropped)} streaming shuffle producer(s) of shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)}")
    }
    dropped
  }

  /**
   * Takes over ownership of one successful map output's spill files.
   *
   * @param shuffleId the shuffle that was produced
   * @param mapId the map output that was produced
   * @param taskAttemptId the generation handing its files across
   * @param files the spill files whose deletion this resolver now owns
   * @return true when the transfer took effect against that exact generation
   */
  def retainProducerOutput(
      shuffleId: Int,
      mapId: Long,
      taskAttemptId: Long,
      files: Seq[File]): Boolean = {
    require(files != null, "files must not be null")
    val key = ProducerKey(shuffleId, mapId)
    var transferred = false
    lifecycle.synchronized {
      if (!stopped) {
        producers.compute(key, (_, existing) => {
          if (existing != null && existing.taskAttemptId == taskAttemptId) {
            transferred = true
            // Accumulated rather than replaced, so a producer that transfers in more than one step
            // -- an early spill followed by the final flush -- cannot orphan the first batch.
            existing.copy(retainedFiles = (existing.retainedFiles ++ files).distinct)
          } else {
            existing
          }
        })
      }
    }
    if (transferred) {
      if (debugEnabled) {
        logDebug(log"Took ownership of ${MDC(COUNT, files.size)} streaming shuffle spill file(s) " +
          log"for shuffle ${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)} generation " +
          log"${MDC(TASK_ATTEMPT_ID, taskAttemptId)}")
      }
    } else {
      logWarning(log"Declined to take ownership of streaming shuffle spill files for shuffle " +
        log"${MDC(SHUFFLE_ID, shuffleId)} map ${MDC(MAP_ID, mapId)} from generation " +
        log"${MDC(TASK_ATTEMPT_ID, taskAttemptId)}: it is not the registered generation")
    }
    transferred
  }

  /** Unlinks the spill files of registrations this resolver has dropped. */
  private def deleteRetainedFiles(files: Seq[File]): Unit = {
    files.foreach { file =>
      try {
        if (file.exists() && !file.delete()) {
          logWarning(log"Could not delete retained streaming shuffle spill file " +
            log"${MDC(FILE_NAME, file.getName)}")
        } else {
          MemorySpillManager.releaseDiskQuota(file)
        }
      } catch {
        case NonFatal(e) =>
          logWarning(log"Could not delete retained streaming shuffle spill file " +
            log"${MDC(FILE_NAME, file.getName)}", e)
      }
    }
  }

  /** The producer able to locate spilled blocks of the given map output, if there is one. */
  def producerFor(shuffleId: Int, mapId: Long): Option[MemorySpillManager] = {
    lifecycle.synchronized {
      if (stopped) {
        None
      } else {
        Option(producers.get(ProducerKey(shuffleId, mapId)))
          .map(registered => registered.producer)
          .orElse(rootProducer)
      }
    }
  }

  /** The generation registered for one map output, if any. */
  def registeredGeneration(shuffleId: Int, mapId: Long): Option[Long] = {
    lifecycle.synchronized {
      if (stopped) {
        None
      } else {
        Option(producers.get(ProducerKey(shuffleId, mapId)))
          .map(registered => registered.taskAttemptId)
      }
    }
  }

  /** How many producers are currently registered. */
  def registeredProducerCount: Int = lifecycle.synchronized(producers.size())

  /** How many spill files this registry currently owns the deletion of, across all producers. */
  def retainedFileCount: Int = lifecycle.synchronized {
    producers.values().asScala.map(_.retainedFiles.size).sum
  }

  /**
   * The spill files this registry currently owns the deletion of for one shuffle.
   *
   * @param shuffleId shuffle to read
   * @return the files, in no particular order
   */
  def retainedFilesOf(shuffleId: Int): Seq[File] = lifecycle.synchronized {
    producers.entrySet().asScala.iterator
      .filter(_.getKey.shuffleId == shuffleId)
      .flatMap(_.getValue.retainedFiles)
      .toSeq
  }

  /** Whether [[stop]] has already run. */
  def isStopped: Boolean = lifecycle.synchronized(stopped)

  // ShuffleBlockResolver: block retrieval.

  /**
   * Retrieves the data for one block of streaming shuffle output.
   *
   * @param blockId the block being requested
   * @param dirs local directories to read from instead of this executor's own.
   * @return a buffer over the requested bytes
   */
  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]] = None): ManagedBuffer = {
    // The lifecycle gate covers every identity, not only the ones answered from the registry.
    requireNotStopped(blockId)
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

  /** Refuses any retrieval once [[stop]] has run. */
  private def requireNotStopped(blockId: BlockId): Unit = {
    if (isStopped) {
      throw SparkException.internalError(
        s"the streaming shuffle block resolver has been stopped and cannot serve $blockId",
        category = "SHUFFLE")
    }
  }

  /**
   * Answers a reduce-partition or reduce-range request from the spilled segments the owning
   * producer still retains.
   *
   * @param blockId the identity being served, carried through solely so that every diagnostic
   *     names the block the caller actually asked for
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
    // The width is bounded before anything is looked up, and independently of any producer.
    val rangeWidth = endReduceId.toLong - startReduceId.toLong
    if (rangeWidth > StreamingShuffleBlockResolver.MAX_REDUCE_RANGE_WIDTH) {
      throw SparkException.internalError(
        s"reduce range [$startReduceId, $endReduceId) requested for block $blockId spans " +
          s"$rangeWidth partitions, which exceeds the maximum of " +
          s"${StreamingShuffleBlockResolver.MAX_REDUCE_RANGE_WIDTH} partitions a single " +
          "request may cover; fetch the range in smaller batches", category = "SHUFFLE")
    }
    val producer = producerFor(shuffleId, mapId).getOrElse {
      throw SparkException.internalError(
        s"no streaming shuffle producer is registered for block $blockId, so none of its data " +
          "can be served from local storage; the producing task either never spilled or has " +
          "already released its buffers", category = "SHUFFLE")
    }
    if (producer.partitionCountRegistered) {
      val owned = producer.numPartitions
      if (endReduceId > owned) {
        throw SparkException.internalError(
          s"reduce range [$startReduceId, $endReduceId) of block $blockId exceeds the " +
            s"$owned reduce partition(s) its producer owns", category = "SHUFFLE")
      }
    } else if (endReduceId - startReduceId > 1) {
      throw SparkException.internalError(
        s"reduce range [$startReduceId, $endReduceId) of block $blockId spans several " +
          "partitions, but its producer has not registered a reduce partition count, so the " +
          "range cannot be validated and is refused rather than traversed", category = "SHUFFLE")
    }
    // Partition-major, and ascending by sequence number within each partition, because that is the
    // order spilledBlocks reports and the order the concatenation has to be in to be decodable.
    val segments = (startReduceId until endReduceId)
      .flatMap(reduceId => producer.spilledBlocks(reduceId))
    if (segments.isEmpty) {
      throw SparkException.internalError(
        s"streaming shuffle holds no spilled data for block $blockId; blocks that are still " +
          "buffered in memory are replayed by retransmission over the streaming channel and are " +
          "deliberately not served from here", category = "SHUFFLE")
    }
    val buffer = assembleLogicalBlock(blockId, producer, segments)
    if (debugEnabled) {
      logDebug(log"Streaming shuffle served block ${MDC(BLOCK_ID, blockId)} as " +
        log"${MDC(NUM_BYTES, buffer.size())} byte(s) assembled from " +
        log"${MDC(COUNT, segments.length)} spilled segment(s)")
    }
    buffer
  }

  /**
   * Assembles one logical block from the payloads of the spill segments that still hold it.
   *
   * @param blockId the identity being served, carried so that every diagnostic names it
   * @param producer the producer that owns these segments and can unwrap them
   * @param segments the retained segments, partition-major and ascending by sequence number
   */
  private def assembleLogicalBlock(
      blockId: BlockId,
      producer: MemorySpillManager,
      segments: Seq[SpilledBlock]): ManagedBuffer = {
    requireDenseSegments(blockId, segments)
    val cap = StreamingShuffleBlockResolver.MAX_SERVED_BYTES
    // Sized from the committed lengths, which understate the payload total whenever shuffle
    // compression is on, with a floor so that a run of small segments does not start from nothing.
    val committedBytes = segments.iterator.map(segment => segment.length).sum
    val assembled = new ByteArrayOutputStream(math.min(
      cap, math.max(committedBytes, StreamingShuffleBlockResolver.INITIAL_ASSEMBLY_BYTES)).toInt)
    segments.foreach { segment =>
      val payload = producer
        .retainedPayload(segment.partitionId, segment.sequenceNumber)
        .getOrElse(throw SparkException.internalError(
          s"streaming shuffle could not read block ${segment.sequenceNumber} of reduce partition " +
            s"${segment.partitionId} while assembling $blockId, so the assembled range would " +
            "have a hole in it; that block was either acknowledged and released or its spill " +
            "file could not be read", category = "SHUFFLE"))
      if (assembled.size().toLong + payload.length > cap) {
        throw SparkException.internalError(
          s"streaming shuffle refuses to assemble more than $cap bytes for block $blockId, which " +
            s"resolves to ${segments.length} retained spill segment(s); fetch a narrower reduce " +
            "range, or read the producer's spill files by their temporary block identity",
          category = "SHUFFLE")
      }
      assembled.write(payload, 0, payload.length)
    }
    new NioManagedBuffer(ByteBuffer.wrap(assembled.toByteArray))
  }

  /** Refuses a segment run that is not a dense ascending sequence within each partition. */
  private def requireDenseSegments(blockId: BlockId, segments: Seq[SpilledBlock]): Unit = {
    var previous: Option[SpilledBlock] = None
    segments.foreach { segment =>
      previous.foreach { earlier =>
        val outOfOrder = segment.partitionId < earlier.partitionId
        val gapped = segment.partitionId == earlier.partitionId &&
          segment.sequenceNumber != earlier.sequenceNumber + 1L
        if (outOfOrder || gapped) {
          throw SparkException.internalError(
            s"streaming shuffle cannot assemble block $blockId, because its retained spill " +
              s"segments are not a dense ascending run: reduce partition ${segment.partitionId} " +
              s"block ${segment.sequenceNumber} follows reduce partition ${earlier.partitionId} " +
              s"block ${earlier.sequenceNumber}", category = "SHUFFLE")
        }
      }
      previous = Some(segment)
    }
  }

  /**
   * Answers a request for a whole spill file, which is what a temporary shuffle block identity
   * names.
   */
  private def spillFileBuffer(
      blockId: TempShuffleBlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    val (owner, file) = locateSpillFile(blockId, dirs)
    val length = file.length()
    if (length > StreamingShuffleBlockResolver.MAX_SERVED_FILE_BYTES) {
      throw SparkException.internalError(
        s"the spill file backing block $blockId is $length bytes, which exceeds the maximum of " +
          s"${StreamingShuffleBlockResolver.MAX_SERVED_FILE_BYTES} bytes this resolver will " +
          "serve as a single block; request the individual reduce partitions instead",
        category = "SHUFFLE")
    }
    val fileSegment = new FileSegmentManagedBuffer(transportConf, file, 0L, length)
    val buffer: ManagedBuffer = owner match {
      case Some(producer) =>
        if (producer.acquireSpillFileLease(file)) {
          new LeasedSegmentBuffer(fileSegment, producer, file)
        } else {
          throw SparkException.internalError(
            s"the spill file of block $blockId was retired before it could be served, which " +
              "means every record it held has already been acknowledged", category = "SHUFFLE")
        }
      case None =>
        fileSegment
    }
    if (debugEnabled) {
      logDebug(log"Streaming shuffle served spill file ${MDC(FILE_NAME, file.getName)} for " +
        log"block ${MDC(BLOCK_ID, blockId)}, ${MDC(NUM_BYTES, length)} bytes")
    }
    buffer
  }

  /**
   * Finds the file backing a temporary shuffle block: first through the producers this resolver
   * knows about, which is authoritative and also identifies the owner able to lease the file, and
   * only then by name on local disk, which is how a caller that knows the block name but not this
   * JVM's registry is answered.
   *
   * @return the owning producer when one is known, together with the file
   */
  private def locateSpillFile(
      blockId: TempShuffleBlockId,
      dirs: Option[Array[String]]): (Option[MemorySpillManager], File) = {
    // One O(1) probe per producer, not a walk of every producer's every retained segment.
    val registered = allProducers.iterator
      .flatMap(producer => producer.spillFile(blockId).map(file => (Some(producer), file)))
      .nextOption()
    val located = registered
      .orElse(locateSpillFileByName(blockId, dirs).map(file => (None, file)))
    located.filter { case (_, file) => file.isFile }.getOrElse {
      throw SparkException.internalError(
        s"streaming shuffle has no spill file for block $blockId on this executor",
        category = "SHUFFLE")
    }
  }

  /**
   * Resolves a block name to a local file, honouring caller-supplied directories exactly as the
   * sort-based resolver does.
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
        if (debugEnabled) {
          logDebug(log"Could not locate a streaming shuffle spill file named " +
            log"${MDC(FILE_NAME, fileName)} for block ${MDC(BLOCK_ID, blockId)}", e)
        }
        None
    }
  }

  /**
   * Every producer this resolver may consult, registered ones first and the constructor-supplied
   * one last.
   */
  private def allProducers: Seq[MemorySpillManager] = {
    lifecycle.synchronized {
      if (stopped) {
        Seq.empty
      } else {
        val registered = producers.values().asScala.toSeq.map(entry => entry.producer)
        (registered ++ rootProducer.toSeq).distinct
      }
    }
  }

  // ShuffleBlockResolver: push-based merge.

  /** Always fails, because streaming shuffle never produces merged output. */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    throw new UnsupportedOperationException(
      "Streaming shuffle does not participate in push-based shuffle merge, so it never produces " +
        s"merged shuffle data and cannot serve $blockId. Set ${SHUFFLE_MANAGER.key} to sort, or " +
        s"set ${SHUFFLE_STREAMING_ENABLED.key} to false, if push-based merge is required.")
  }

  /** Always fails, for the same reason as [[getMergedBlockData]]. */
  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    throw new UnsupportedOperationException(
      "Streaming shuffle does not participate in push-based shuffle merge, so it never produces " +
        s"merged shuffle metadata and cannot serve $blockId. Set ${SHUFFLE_MANAGER.key} to " +
        s"sort, or set ${SHUFFLE_STREAMING_ENABLED.key} to false, if push-based merge is " +
        "required.")
  }

  // ShuffleBlockResolver: enumeration and lifecycle.

  /**
   * The locally stored blocks of one map output, which for streaming shuffle means the spill files
   * that map output still holds on disk, de-duplicated because one file carries several segments.
   */
  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    producerFor(shuffleId, mapId) match {
      case Some(producer) =>
        producer.allSpilledBlocks.map(segment => segment.blockId).distinct
      case None =>
        Seq.empty
    }
  }

  /** Releases everything this resolver holds. */
  override def stop(): Unit = {
    Utils.tryLogNonFatalError {
      val orphanedFiles = new mutable.ArrayBuffer[File]()
      val orphanedProducers = new mutable.ArrayBuffer[MemorySpillManager]()
      val dropped = lifecycle.synchronized {
        if (stopped) {
          -1
        } else {
          stopped = true
          val registered = producers.size()
          producers.values().asScala.foreach { registered =>
            orphanedFiles ++= registered.retainedFiles
            orphanedProducers += registered.producer
          }
          producers.clear()
          registered
        }
      }
      orphanedProducers.distinct.foreach(_.releaseRetainedConsumerState())
      deleteRetainedFiles(orphanedFiles.toSeq)
      if (dropped > 0 && debugEnabled) {
        logDebug(log"Streaming shuffle block resolver stopped, dropping " +
          log"${MDC(COUNT, dropped)} registered producer(s)")
      }
    }
  }

  /** A diagnostic rendering built only from state this object already holds. */
  override def toString: String = {
    s"StreamingShuffleBlockResolver(registeredProducers=$registeredProducerCount, " +
      s"hasRootProducer=${rootProducer.isDefined}, stopped=$isStopped)"
  }
}

/** Constants and the registry key of [[StreamingShuffleBlockResolver]]. */
private[spark] object StreamingShuffleBlockResolver {

  /**
   * Transport module name of the streaming shuffle, which yields the independent
   * `spark.shuffle-streaming.io.*` configuration namespace.
   */
  val TRANSPORT_MODULE: String = "shuffle-streaming"

  /** The largest response a reduce-partition or reduce-range request may be answered with. */
  val MAX_SERVED_BYTES: Long = 32L * 1024 * 1024

  /** The floor on the initial capacity of an assembly buffer. */
  val INITIAL_ASSEMBLY_BYTES: Long = 64L * 1024

  /** The largest reduce range a single request may cover. */
  val MAX_REDUCE_RANGE_WIDTH: Long = 4096L

  /** The largest spill file this resolver will serve as one block. */
  val MAX_SERVED_FILE_BYTES: Long = 1024L * 1024L * 1024L

  /**
   * Identifies the producer of one map output.
   *
   * @param shuffleId the shuffle being produced
   * @param mapId the map output being produced
   */
  case class ProducerKey(shuffleId: Int, mapId: Long)

  /**
   * One registered producer together with the generation that registered it.
   *
   * @param taskAttemptId the generation that registered this producer
   * @param producer the spill manager able to locate that map output's spilled blocks
   * @param retainedFiles spill files whose deletion this registry has taken over, empty until
   *     the producing task hands them across
   */
  case class RegisteredProducer(
      taskAttemptId: Long,
      producer: MemorySpillManager,
      retainedFiles: Seq[File] = Seq.empty) {

    /** Whether this registration is newer than, or a refresh of, an existing one. */
    def supersedes(other: RegisteredProducer): Boolean = taskAttemptId >= other.taskAttemptId
  }

  /**
   * A file segment buffer that holds a reader lease on the spill file behind it.
   *
   * @param delegate the file segment buffer that does the actual work
   * @param producer the producer that owns the file and grants leases over it
   * @param file the leased spill file, held so that leases can be released without reaching
   *     into the delegate
   */
  class LeasedSegmentBuffer(
      delegate: FileSegmentManagedBuffer,
      producer: MemorySpillManager,
      file: File)
    extends ManagedBuffer {

    /** Leases this buffer holds. */
    private val leases = new AtomicInteger(1)

    override def size(): Long = delegate.size()

    override def nioByteBuffer(): ByteBuffer = delegate.nioByteBuffer()

    override def createInputStream(): InputStream = delegate.createInputStream()

    override def convertToNetty(): Object = delegate.convertToNetty()

    override def convertToNettyForSsl(): Object = delegate.convertToNettyForSsl()

    override def retain(): ManagedBuffer = {
      // A refused lease is not an error here: it means the file has been retired, and the caller
      // already holds a lease that keeps it readable.
      if (producer.acquireSpillFileLease(file)) {
        leases.incrementAndGet()
      }
      delegate.retain()
      this
    }

    override def release(): ManagedBuffer = {
      val held = leases.getAndUpdate(outstanding => math.max(0, outstanding - 1))
      if (held > 0) {
        producer.releaseSpillFileLease(file)
      }
      delegate.release()
      this
    }

    /** Leases this buffer is currently accountable for. */
    def outstandingLeases: Int = leases.get()

    override def toString: String = {
      s"LeasedSegmentBuffer(file=${file.getName}, leases=${leases.get()}, delegate=$delegate)"
    }
  }
}
