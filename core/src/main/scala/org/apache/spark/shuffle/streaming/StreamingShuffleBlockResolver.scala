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

import java.io.{File, InputStream}
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
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
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
 * Why a response carries exactly one segment. Every spilled segment was committed on its own
 * through `DiskBlockObjectWriter`, so each one carries its own serializer, compression and
 * encryption framing and is decodable only on its own. Physically concatenating two of them yields
 * bytes that no consumer can decode, so a request resolving to more than one segment is refused
 * outright and the caller is directed at [[getSpilledSegments]], which hands back one independently
 * decodable buffer per segment. Nothing is ever assembled into a heap buffer here: what is served
 * is a file segment whose bytes are never copied, and the size one response may carry is capped.
 *
 * Why every served buffer is leased. A file segment buffer opens its file lazily, when the consumer
 * first reads it, which is necessarily after this resolver has returned. A consumer acknowledgement
 * can retire the last record naming that file in the interval between. Every buffer handed out from
 * here therefore holds a reader lease on its file, taken before the buffer is constructed and
 * dropped when the buffer is released, and the owning producer unlinks a file only once that file
 * is both retired and unleased.
 *
 * Why it is deliberately not index based. This class is not an `IndexShuffleBlockResolver` and does
 * not mix in `MigratableResolver`. `ShuffleWriteProcessor` selects a push-based merge by pattern
 * matching on the manager's resolver type, and a resolver that is not an
 * `IndexShuffleBlockResolver` falls through its empty default branch, so streaming takes no part in
 * push-based merge or in block migration. Sort-based shuffle keeps its own
 * `IndexShuffleBlockResolver` and remains both the default and the fallback: with the streaming
 * kill switch off, `StreamingShuffleManager` exposes the sort delegate's resolver rather than
 * this one, so the push-merge branch fires and behaviour is indistinguishable from sort-based
 * shuffle.
 *
 * Lifecycle and thread safety. An instance is created once per driver or executor, from
 * `StreamingShuffleManager`'s constructor, and is then shared by every task on that JVM. One
 * monitor guards the producer registry and the stopped flag together, so a registration cannot land
 * after [[stop]] has cleared the registry and no lookup survives a stop -- not even the
 * constructor-supplied fallback. Every method is therefore safe to call from any thread, including
 * a Netty event-loop thread, and the monitor is held only long enough to snapshot registry state,
 * never while a producer, the disk or the network is touched. No environment object is dereferenced
 * during construction; see the deferred-access section of the implementation for why that is
 * mandatory rather than merely tidy. [[stop]] is idempotent, and it contains and logs any non-fatal
 * failure it meets rather than propagating it.
 *
 * Producer generations. A map output may be produced more than once, because a task attempt can be
 * retried or speculated, and only the newest attempt's spill files are valid. Producers are
 * therefore registered under a generation -- the task attempt id, which Spark allocates from one
 * monotonically increasing per-application counter -- and every registry mutation is conditioned on
 * it. A stale attempt can neither displace a newer producer nor, when it finally completes, evict
 * the replacement that superseded it.
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

  /**
   * The one monitor that makes this object's lifecycle atomic.
   *
   * It guards [[stopped]] and [[producers]] jointly, which is the whole point: guarding them
   * separately -- a flag read followed by an unrelated map mutation -- lets a registration pass the
   * stop check and then insert itself into a registry that [[stop]] has already cleared, leaving a
   * producer reachable on a resolver that has shut down. Held only to snapshot or mutate registry
   * state, never across a call into a producer, the disk or the network, so it cannot be the inner
   * lock of any lock-ordering cycle.
   */
  private val lifecycle = new Object

  /** The constructor-supplied producer, if any. Tolerates a null argument by design. */
  private val rootProducer: Option[MemorySpillManager] = Option(spillManager)

  /**
   * Live producers, keyed by the map output they are producing and carrying the generation that
   * registered them. Read and written only while holding [[lifecycle]].
   */
  private val producers = new ConcurrentHashMap[ProducerKey, RegisteredProducer]()

  /** Whether [[stop]] has run. Read and written only while holding [[lifecycle]]. */
  private var stopped: Boolean = false

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
   * The registration is conditioned on its generation, so that concurrent attempts at the same map
   * output resolve deterministically rather than by arrival order. A newer generation replaces an
   * older one, because only the newest attempt's spill files are valid; the same generation
   * re-registering is accepted as a refresh; an older generation is declined, because it has
   * already been superseded and its files are the ones being abandoned. The whole decision,
   * including the check that this resolver has not been stopped, is taken under one monitor, so a
   * registration can never land in a registry that [[stop]] has already cleared.
   *
   * @param shuffleId the shuffle being produced
   * @param mapId the map output being produced
   * @param taskAttemptId the generation registering, which is the producing task's attempt id.
   *                      Spark allocates attempt ids from a single monotonically increasing
   *                      per-application counter, so a larger value is unambiguously a newer
   *                      attempt
   * @param producer the spill manager that owns that map output's buffers and spill files
   * @return true when the registration took effect, false when it was declined because a newer
   *         generation is already registered or because this resolver has been stopped
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
   * Drops one producer, which is what a task must do on completion so that neither the producer
   * nor its buffers are reachable from this JVM-scoped object any longer.
   *
   * Removal is conditioned on the generation, and that condition is the whole point of this
   * method's shape. A retried or speculated attempt completes at an arbitrary time, frequently
   * after the attempt that superseded it has already registered; an unconditional removal would
   * then delete the live replacement's registration and make its perfectly valid spill data
   * unservable. Passing the caller's own generation makes a completing task able to retract only
   * its own registration.
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
    lifecycle.synchronized {
      // compute() removes the mapping when the remapping function returns null, so the generation
      // comparison and the removal are one indivisible step against the current entry. A
      // value-based remove() would not do: it would compare the producer reference too, which says
      // nothing about which generation is registered.
      producers.compute(key, (_, existing) => {
        if (existing != null && existing.taskAttemptId == taskAttemptId) {
          dropped = true
          orphanedFiles = existing.retainedFiles
          null
        } else {
          existing
        }
      })
    }
    // Outside the monitor: unlinking a file performs I/O, and this registry's monitor is contended
    // by every registration and every lookup in the JVM. Nothing can reach these files any longer
    // -- the registration naming them has gone -- so there is nothing to serialise against.
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
   * Drops every producer of one shuffle. Called when the shuffle itself is unregistered, so that a
   * completed shuffle leaves nothing behind even if some task failed to unregister its own
   * producer.
   *
   * @return the number of producers dropped
   */
  def removeShuffle(shuffleId: Int): Int = {
    var dropped = 0
    val orphanedFiles = new mutable.ArrayBuffer[File]()
    lifecycle.synchronized {
      val iterator = producers.entrySet().iterator()
      while (iterator.hasNext) {
        val entry = iterator.next()
        if (entry.getKey.shuffleId == shuffleId) {
          orphanedFiles ++= entry.getValue.retainedFiles
          iterator.remove()
          dropped += 1
        }
      }
    }
    // Every retained file of every generation of this shuffle goes, which is one of the three
    // boundaries at which retained output is specified to be released. A consumer that has not read
    // by now cannot: the shuffle itself is being unregistered.
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
   * This is the transfer that makes retained output outlive its producing task. A map task's
   * buffered bytes cannot survive that task: they are task-managed execution memory, which the
   * executor reclaims at completion and, with `spark.unsafe.exceptionOnMemoryLeak` enabled, fails
   * the task over if anything still holds. Its spill files can, being ordinary files in the local
   * directories. So a producer whose task succeeded makes its unacknowledged window durable, hands
   * the files here, and this registry unlinks them at the boundary the feature actually specifies
   * -- generation invalidation, shuffle unregistration, or resolver shutdown -- rather than at the
   * arbitrary moment the producing task happened to finish.
   *
   * Conditioned on the generation for the same reason [[unregisterProducer]] is: a superseded
   * attempt completing late must not be able to attach its files to the live replacement's
   * registration, where they would be served in place of the valid ones.
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

  /**
   * Unlinks the spill files of registrations this resolver has dropped.
   *
   * Only files transferred through [[retainProducerOutput]] are ever passed here, so a producer
   * whose task is still running never has a file removed underneath it. Each deletion absorbs its
   * own failure: a file that cannot be unlinked is a local-disk annoyance the shuffle service will
   * clear with the application directory, and raising here would abort the registry operation that
   * happened to trigger it.
   */
  private def deleteRetainedFiles(files: Seq[File]): Unit = {
    files.foreach { file =>
      try {
        if (file.exists() && !file.delete()) {
          logWarning(log"Could not delete retained streaming shuffle spill file " +
            log"${MDC(FILE_NAME, file.getName)}")
        }
      } catch {
        case NonFatal(e) =>
          logWarning(log"Could not delete retained streaming shuffle spill file " +
            log"${MDC(FILE_NAME, file.getName)}", e)
      }
    }
  }

  /**
   * The producer able to locate spilled blocks of the given map output, if there is one.
   *
   * Falls back to the constructor-supplied producer when nothing is registered under the key. That
   * fallback is what makes the two-argument constructor meaningful: a single-producer embedding can
   * hand its spill manager straight to the resolver and skip registration entirely.
   *
   * Returns nothing at all once [[stop]] has run, and the fallback is suppressed along with the
   * registry. A stopped resolver holds no serving authority whatsoever, so continuing to answer
   * from a producer that happened to be supplied at construction would be the one lookup able to
   * outlive the shutdown that was meant to end all of them. The decision is taken under the
   * lifecycle monitor so that a lookup racing a stop resolves one way or the other and never half
   * way.
   */
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

  /**
   * The generation registered for one map output, if any. Intended for diagnostics and tests, and
   * for a caller that needs to know whether its own attempt is still the serving one.
   */
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

  /** How many producers are currently registered. Intended for diagnostics and tests. */
  def registeredProducerCount: Int = lifecycle.synchronized(producers.size())

  /** Whether [[stop]] has already run. */
  def isStopped: Boolean = lifecycle.synchronized(stopped)

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
   *    ordering a batched fetch of sort-based output observes. The range is validated against the
   *    producer's own reduce partition count before anything is iterated, so a malformed batch
   *    identity is rejected rather than turned into a traversal of billions of empty partitions.
   *  - `TempShuffleBlockId` resolves to a whole spill file. This is the exact one-to-one identity,
   *    because a spill file is allocated as a temporary shuffle block and so this identity names
   *    precisely one file.
   *
   * A request that resolves to a single segment is answered with a leased, zero-copy file segment.
   * One that resolves to several is refused, and refusal is the only correct answer: each segment
   * was committed on its own and carries its own serializer, compression and encryption framing, so
   * their concatenation is not a decodable stream, and handing it out as one buffer would give a
   * consumer bytes it can only fail on -- silently, and as a deserialization error attributed to
   * the wrong layer. A caller that wants those segments must ask for them individually through
   * [[getSpilledSegments]], which hands back one independently decodable buffer per segment.
   *
   * Every buffer returned from here holds a reader lease on the file behind it and must be released
   * by the caller, exactly as the buffers the sort-based resolver returns must be. That release is
   * what finally lets the producer unlink a spill file whose records have all been acknowledged.
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
    // The lifecycle gate covers every identity, not only the ones answered from the registry. A
    // temporary block identity can otherwise be resolved by name straight off local disk, which
    // would let exactly one kind of lookup keep serving bytes -- and serving them unleased -- from
    // a resolver whose stop was meant to end all serving.
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

  /**
   * The spilled segments of one reduce partition, one buffer per segment, in ascending sequence
   * order.
   *
   * This is the segment-granular counterpart of [[getBlockData]] and the form a caller wants when
   * it intends to decode the bytes rather than merely forward them, because each segment is an
   * independently decodable unit. An empty result means that partition has nothing on disk, which
   * on the streaming happy path is the normal case rather than an error.
   *
   * Each returned buffer holds its own reader lease and must be released by the caller. A segment
   * whose lease cannot be taken is omitted rather than returned, because the only way a lease is
   * refused is that the file has already been retired -- which happens precisely when the consumer
   * has acknowledged every record in it, so the bytes are no longer owed to anyone.
   *
   * @param shuffleId the shuffle the partition belongs to
   * @param mapId the map output the partition belongs to
   * @param reduceId the reduce partition to report on; must be non-negative and, once the producer
   *                 has registered its reduce partition count, must be a partition that producer
   *                 actually owns
   */
  def getSpilledSegments(shuffleId: Int, mapId: Long, reduceId: Int): Seq[ManagedBuffer] = {
    require(reduceId >= 0, s"reduceId must be non-negative, but was $reduceId")
    requireNotStopped(ShuffleBlockId(shuffleId, mapId, reduceId))
    producerFor(shuffleId, mapId) match {
      case Some(producer) =>
        requireOwnedPartition(producer, shuffleId, mapId, reduceId)
        producer.spilledBlocks(reduceId).flatMap(segment => leasedSegment(producer, segment))
      case None =>
        Seq.empty
    }
  }

  /**
   * Refuses any retrieval once [[stop]] has run.
   *
   * A stopped resolver has released its serving authority, and the producers it could have
   * consulted are on their way down with it. Refusing here rather than returning an empty result is
   * deliberate: a caller asking a shut-down resolver for bytes has a real ordering defect, and
   * reporting it as one is what makes that defect findable.
   */
  private def requireNotStopped(blockId: BlockId): Unit = {
    if (isStopped) {
      throw SparkException.internalError(
        s"the streaming shuffle block resolver has been stopped and cannot serve $blockId",
        category = "SHUFFLE")
    }
  }

  /**
   * Refuses a reduce partition the given producer cannot own.
   *
   * Validated only once the producer has registered its reduce partition count, because until then
   * there is no ownership to check against: a producer with no registered count has admitted no
   * block and so has nothing spilled, and the empty result it yields is already the correct answer.
   */
  private def requireOwnedPartition(
      producer: MemorySpillManager,
      shuffleId: Int,
      mapId: Long,
      reduceId: Int): Unit = {
    if (producer.partitionCountRegistered && reduceId >= producer.numPartitions) {
      throw SparkException.internalError(
        s"reduce partition $reduceId is not part of shuffle $shuffleId map $mapId, whose " +
          s"producer owns ${producer.numPartitions} reduce partition(s)", category = "SHUFFLE")
    }
  }

  /**
   * Answers a reduce-partition or reduce-range request from the spilled segments the owning
   * producer still retains.
   *
   * The range is validated in two stages, and the order matters. Sign and ordering are checked
   * before a producer is even looked up, because they are checkable without one. The upper bound is
   * then checked against that producer's own reduce partition count, which is the only authority on
   * which partitions exist: without it, a batch identity naming `[0, Int.MaxValue)` would be
   * traversed partition by partition before yielding nothing, turning one malformed request into
   * two billion map lookups. A producer that has not yet registered a count cannot have that bound
   * applied, so such a request is confined to a single partition instead -- which is exactly what
   * an unbatched `ShuffleBlockId` asks for, and the only shape that needs no partition count to be
   * meaningful.
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
    // The width is bounded before anything is looked up, and independently of any producer. A range
    // is a request to perform one lookup per partition in it, each taking the producing manager's
    // monitor, so an unchecked width is an unbounded amount of work purchased with a single
    // fixed-size request. The producer's own partition count bounds it too, but only once one is
    // registered and only as far as that count reaches; this bound holds in every case and is what
    // makes the cost of a request proportional to the request rather than to the largest number the
    // requester can name.
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
    val segments = (startReduceId until endReduceId)
      .flatMap(reduceId => producer.spilledBlocks(reduceId))
    if (segments.isEmpty) {
      throw SparkException.internalError(
        s"streaming shuffle holds no spilled data for block $blockId; blocks that are still " +
          "buffered in memory are replayed by retransmission over the streaming channel and are " +
          "deliberately not served from here", category = "SHUFFLE")
    }
    if (segments.length > 1) {
      // Refusal rather than concatenation, and deliberately so. Each of these segments was
      // committed on its own through DiskBlockObjectWriter, so each carries its own serializer,
      // compression and encryption framing and is decodable only on its own. Their bytes laid end
      // to end are not a stream any consumer can read, and presenting them as one ManagedBuffer
      // would surface as a deserialization failure blamed on the serializer rather than on the
      // composition. There is no heap assembly path here for the same reason there is no correct
      // one.
      throw SparkException.internalError(
        s"block $blockId resolves to ${segments.length} independently committed spill segments, " +
          "which cannot be served as one buffer because each segment carries its own serializer, " +
          "compression and encryption framing and is decodable only on its own; request the " +
          "segments individually through getSpilledSegments instead", category = "SHUFFLE")
    }
    val segment = segments.head
    if (segment.length > StreamingShuffleBlockResolver.MAX_SERVED_BYTES) {
      // A committed segment holds one block, whose payload the protocol caps, so this bound is only
      // ever reached by a corrupt or mis-attributed record. Refusing is far better than handing a
      // consumer a length it will try to buffer.
      throw SparkException.internalError(
        s"streaming shuffle refuses to serve ${segment.length} bytes for block $blockId, because " +
          s"one response may not exceed ${StreamingShuffleBlockResolver.MAX_SERVED_BYTES} bytes",
        category = "SHUFFLE")
    }
    // The bytes are never copied through the heap: a leased file segment is handed out, and the
    // lease is what stops an acknowledgement from unlinking the file before the consumer opens it.
    val buffer = leasedSegment(producer, segment).getOrElse {
      throw SparkException.internalError(
        s"the spilled segment of block $blockId was retired before it could be served, which " +
          "means its consumer has already acknowledged every record in it", category = "SHUFFLE")
    }
    if (debugEnabled) {
      logDebug(log"Streaming shuffle served block ${MDC(BLOCK_ID, blockId)} from one spilled " +
        log"segment of ${MDC(NUM_BYTES, buffer.size())} bytes")
    }
    buffer
  }

  /**
   * Wraps one committed spill segment as a leased buffer, without reading it.
   *
   * The lease is taken before the buffer exists, which is the whole point: a file segment buffer
   * opens its file lazily, so taking the lease inside the buffer would leave a window in which the
   * file has been unlinked and the lazy open is the thing that discovers it.
   *
   * @return the leased buffer, or nothing when the file has already been retired or its producer
   *         has closed, in which case the segment is no longer servable from disk
   */
  private def leasedSegment(
      producer: MemorySpillManager,
      segment: SpilledBlock): Option[ManagedBuffer] = {
    if (producer.acquireSpillFileLease(segment.file)) {
      Some(new LeasedSegmentBuffer(
        new FileSegmentManagedBuffer(transportConf, segment.file, segment.offset, segment.length),
        producer,
        segment.file))
    } else {
      if (debugEnabled) {
        logDebug(log"Streaming shuffle cannot lease spill file " +
          log"${MDC(FILE_NAME, segment.file.getName)}, so its segment is no longer servable")
      }
      None
    }
  }

  /**
   * Answers a request for a whole spill file, which is what a temporary shuffle block identity
   * names.
   *
   * A file may still hold segments the consumer has already acknowledged, because a spill file is
   * only deleted once no retained record refers to it. Serving the whole file is nonetheless
   * byte-valid: every byte in it was committed by this subsystem, and the caller asked for the file
   * rather than for a segment of it. The per-segment ceiling therefore does not apply, because this
   * identity names one file rather than one block, and refusing it on that ceiling would refuse a
   * legitimate one-to-one read.
   *
   * A far looser ceiling does apply, at [[StreamingShuffleBlockResolver.MAX_SERVED_FILE_BYTES]]. A
   * whole-file read is answered with a zero-copy file segment, so the concern is not heap but the
   * size of the transfer one small request can provoke -- and the name-based fallback below answers
   * from local disk for a file this JVM has no producer for, where the producer's buffer budget
   * bounds nothing at all. No spill file this subsystem writes comes near the ceiling, so reaching
   * it means the name resolved to something this resolver has no business serving.
   *
   * The response is leased whenever the owning producer is known. When it is not -- a name-based
   * lookup answered from local disk, which is how a host-local or External Shuffle Service read
   * reaches a file this JVM has no producer for -- there is no lease authority to ask, and none is
   * needed: no producer in this JVM holds a record naming that file, so nothing here will unlink
   * it.
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
    // One O(1) probe per producer, not a walk of every producer's every retained segment. The walk
    // it replaces was linear in the total number of segments the executor had spilled and ran on
    // every single-file lookup, so its cost grew with how much had been spilled while the request
    // that paid for it stayed the same fixed size -- the same amplification the range bounds above
    // exist to prevent, reached by a different route.
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
   *
   * A non-fatal failure answers `None` rather than propagating. Reaching the disk block manager
   * needs a live environment, and a caller may be running somewhere that has none; that is not an
   * error condition here, it simply means the file cannot be located this way and the caller is
   * told there is no such block.
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
   * one last. De-duplicated by identity, because the same producer may be both, and empty once
   * [[stop]] has run, for the same reason [[producerFor]] is.
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

  // ----------------------------------------------------------------------------------------------
  // ShuffleBlockResolver: push-based merge. Streaming declines to participate, by design.
  // ----------------------------------------------------------------------------------------------

  /**
   * Always fails, because streaming shuffle never produces merged output.
   *
   * A push-based merge is only ever started for a manager whose resolver is an
   * `IndexShuffleBlockResolver`, and this resolver is not one, so nothing on the streaming path can
   * create a merged block for this method to serve. Reaching it means a merged block was attributed
   * to a streaming shuffle, which is a programming error and is reported as one rather than
   * disguised as an empty result.
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    throw new UnsupportedOperationException(
      "Streaming shuffle does not participate in push-based shuffle merge, so it never produces " +
        s"merged shuffle data and cannot serve $blockId. Set ${SHUFFLE_MANAGER.key} to sort, or " +
        s"set ${SHUFFLE_STREAMING_ENABLED.key} to false, if push-based merge is required.")
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
        s"merged shuffle metadata and cannot serve $blockId. Set ${SHUFFLE_MANAGER.key} to " +
        s"sort, or set ${SHUFFLE_STREAMING_ENABLED.key} to false, if push-based merge is " +
        "required.")
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
   * Spill files still owned by their producer are not deleted here: that ownership rests with the
   * producer that wrote them, which deletes each file once no retained record still refers to it
   * and no reader lease holds it, and which registers its own task-completion cleanup for the
   * failure and cancellation cases. Deleting those from here would race a producer that is still
   * replaying its unacknowledged window.
   *
   * Files transferred through [[retainProducerOutput]] are a different matter and are deleted,
   * because their producing task has already finished and this resolver is the only thing that
   * still owns them. A stopping resolver is the last boundary at which they can be released, so not
   * deleting them here would leak them onto local disk until the application directory is cleared.
   *
   * The stopped flag is raised and the registry cleared in the same critical section that every
   * registration and every lookup contends for, which is what makes a stop final: no registration
   * can slip in behind the clear, and no lookup afterwards can find anything -- not through the
   * registry and not through the constructor-supplied fallback either.
   */
  override def stop(): Unit = {
    Utils.tryLogNonFatalError {
      val orphanedFiles = new mutable.ArrayBuffer[File]()
      val dropped = lifecycle.synchronized {
        if (stopped) {
          -1
        } else {
          stopped = true
          val registered = producers.size()
          producers.values().asScala.foreach(orphanedFiles ++= _.retainedFiles)
          producers.clear()
          registered
        }
      }
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
   * The largest response a reduce-partition or reduce-range request may be answered with.
   *
   * Deliberately small, and deliberately not derived from what a buffer can address. One response
   * is one committed spill segment, which holds one block whose payload the protocol caps at
   * `DataBlockMessage.MAX_BLOCK_SIZE_BYTES`; the headroom above that cap covers the serializer,
   * compression and encryption framing a committed segment adds, and nothing legitimate reaches it.
   * Its purpose is therefore to bound what a corrupt or mis-attributed record can ask a consumer to
   * buffer, which a limit of `Int.MaxValue` would not do at all.
   */
  val MAX_SERVED_BYTES: Long = 8L * 1024 * 1024

  /**
   * The largest reduce range a single request may cover.
   *
   * A range request costs one lookup per partition it covers, each taking the producing manager's
   * monitor, so the width of a range is the multiplier between the size of a request and the work
   * it commissions. Left unbounded that multiplier reaches `Int.MaxValue`: a fixed-size request
   * naming `[0, 2147483647)` would occupy the producer's monitor for over two billion iterations
   * and starve every other reader of that map output while returning nothing. This bound is
   * generous against real shuffles -- a batched fetch covers the partitions of one map output, and
   * a range wider than this is a range worth splitting on the fetch side regardless -- and it makes
   * the cost of a request proportional to the request.
   */
  val MAX_REDUCE_RANGE_WIDTH: Long = 4096L

  /**
   * The largest spill file this resolver will serve as one block.
   *
   * A whole-file read is answered with a zero-copy file segment, so the concern is not heap but the
   * size of the transfer one small request can provoke. Bounding it keeps a temporary-block
   * identity from being usable as a request for an arbitrarily large transfer, including for a file
   * found by name on local disk that no producer in this JVM accounts for.
   */
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
   * The generation is the producing task's attempt id. Spark allocates attempt ids from a single
   * monotonically increasing per-application counter, so comparing two of them is a total order
   * over "which attempt is newer" without any further bookkeeping -- which is precisely what a
   * registry shared by concurrent attempts at the same map output needs.
   *
   * `retainedFiles` is empty while the producing task is still running and its own cleanup owns
   * its spill files. It is populated by [[StreamingShuffleBlockResolver.retainProducerOutput]] when
   * that task succeeds, at which point the files become this registry's to unlink -- and are
   * unlinked when the generation is superseded, when the shuffle is unregistered, or when the
   * resolver stops. That is what lets a successful map output be served after the task that
   * produced it has gone, which task-managed buffer memory can never be.
   *
   * @param taskAttemptId the generation that registered this producer
   * @param producer the spill manager able to locate that map output's spilled blocks
   * @param retainedFiles spill files whose deletion this registry has taken over, empty until the
   *                      producing task hands them across
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
   * Delegation rather than inheritance, because `FileSegmentManagedBuffer` is final -- and because
   * delegation is the better shape anyway: every byte-level concern stays with the buffer that
   * already implements it correctly, including its lazy open, its size threshold for memory mapping
   * and its SSL variant, and this class adds nothing but ownership.
   *
   * The lease is taken by the resolver before this object is constructed, never by this object
   * itself, so that no window exists in which the file is unlinked before the buffer holding it
   * exists. `retain` takes a further lease and `release` drops exactly one, counted so that the
   * count can never go negative: an over-release would unlink a file a second reader is still
   * entitled to open, and a retain whose lease is refused must not later be released as though it
   * had held one.
   *
   * @param delegate the file segment buffer that does the actual work
   * @param producer the producer that owns the file and grants leases over it
   * @param file the leased spill file, held so that leases can be released without reaching
   *             into the delegate
   */
  class LeasedSegmentBuffer(
      delegate: FileSegmentManagedBuffer,
      producer: MemorySpillManager,
      file: File)
    extends ManagedBuffer {

    /** Leases this buffer holds. Starts at the one the resolver took on its behalf. */
    private val leases = new AtomicInteger(1)

    override def size(): Long = delegate.size()

    override def nioByteBuffer(): ByteBuffer = delegate.nioByteBuffer()

    override def createInputStream(): InputStream = delegate.createInputStream()

    override def convertToNetty(): Object = delegate.convertToNetty()

    override def convertToNettyForSsl(): Object = delegate.convertToNettyForSsl()

    override def retain(): ManagedBuffer = {
      // A refused lease is not an error here: it means the file has been retired, and the caller
      // already holds a lease that keeps it readable. Not counting the refusal is what keeps the
      // matching release from dropping a lease this object never took.
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

    /** Leases this buffer is currently accountable for. Intended for diagnostics and tests. */
    def outstandingLeases: Int = leases.get()

    override def toString: String = {
      s"LeasedSegmentBuffer(file=${file.getName}, leases=${leases.get()}, delegate=$delegate)"
    }
  }
}
