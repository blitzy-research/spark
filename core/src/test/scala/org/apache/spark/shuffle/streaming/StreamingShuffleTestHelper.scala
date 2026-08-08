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

import java.io.{File, InputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.util.{Arrays => JArrays, Properties}
import java.util.concurrent.{Callable, ConcurrentHashMap, CountDownLatch, CyclicBarrier, Semaphore,
  TimeUnit}
import java.util.concurrent.{Future => JFuture}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable
import scala.concurrent.Awaitable
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Random, Try}

import com.codahale.metrics.{Counter, Gauge}
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.{LogEvent, Logger => Log4jLogger}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.scalatest.{BeforeAndAfterAll, Suite, Tag}

import org.apache.spark.{HashPartitioner, Partitioner, SecurityManager, ShuffleDependency,
  SparkConf, SparkContext, SparkEnv, TaskContext, TaskContextImpl}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{AUTH_SECRET, NETWORK_AUTH_ENABLED, SHUFFLE_MANAGER,
  SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_DEBUG, SHUFFLE_STREAMING_ENABLED,
  SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, SHUFFLE_STREAMING_SPILL_THRESHOLD,
  UNSAFE_EXCEPTION_ON_MEMORY_LEAK}
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, DataBlockMessage,
  HeartbeatMessage, RetransmitRequestMessage, StreamingShuffleChecksum, StreamingShuffleMessage,
  StreamingShuffleMessageType, StreamTerminationMessage}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle.{ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter}
import org.apache.spark.util.{Clock, LongAccumulator, ManualClock, SystemClock, ThreadUtils, Utils}

/** ScalaTest tag carried by the five-minute streaming shuffle stress workload. */
object StreamingShuffleStressTest
  extends Tag("org.apache.spark.shuffle.streaming.StreamingShuffleStressTest")

/**
 * A `ShuffleWriteMetricsReporter` that accumulates every reported figure so a test can assert on
 * what the streaming write path actually reported.
 */
class RecordingStreamingShuffleWriteMetrics extends ShuffleWriteMetricsReporter {

  private val bytesWrittenTotal = new AtomicLong(0L)
  private val recordsWrittenTotal = new AtomicLong(0L)
  private val writeTimeTotal = new AtomicLong(0L)
  private val bytesDecrementedTotal = new AtomicLong(0L)
  private val recordsDecrementedTotal = new AtomicLong(0L)
  private val callTotal = new AtomicLong(0L)
  private val owningThread = new AtomicReference[Thread](null)
  private val foreignThreadCallTotal = new AtomicLong(0L)

  private[spark] override def incBytesWritten(v: Long): Unit = add(bytesWrittenTotal, v)

  private[spark] override def incRecordsWritten(v: Long): Unit = add(recordsWrittenTotal, v)

  private[spark] override def incWriteTime(v: Long): Unit = add(writeTimeTotal, v)

  private[spark] override def decBytesWritten(v: Long): Unit = {
    bytesWrittenTotal.addAndGet(-v)
    add(bytesDecrementedTotal, v)
  }

  private[spark] override def decRecordsWritten(v: Long): Unit = {
    recordsWrittenTotal.addAndGet(-v)
    add(recordsDecrementedTotal, v)
  }

  def bytesWritten: Long = bytesWrittenTotal.get()

  def recordsWritten: Long = recordsWrittenTotal.get()

  def writeTime: Long = writeTimeTotal.get()

  def bytesDecremented: Long = bytesDecrementedTotal.get()

  def recordsDecremented: Long = recordsDecrementedTotal.get()

  def callCount: Long = callTotal.get()

  /** Number of calls that arrived on a thread other than the first one seen. */
  def foreignThreadCallCount: Long = foreignThreadCallTotal.get()

  def reportingThread: Option[Thread] = Option(owningThread.get())

  def reset(): Unit = {
    bytesWrittenTotal.set(0L)
    recordsWrittenTotal.set(0L)
    writeTimeTotal.set(0L)
    bytesDecrementedTotal.set(0L)
    recordsDecrementedTotal.set(0L)
    callTotal.set(0L)
    foreignThreadCallTotal.set(0L)
    owningThread.set(null)
  }

  private def add(counter: AtomicLong, delta: Long): Unit = {
    counter.addAndGet(delta)
    observeCallingThread()
  }

  private def observeCallingThread(): Unit = {
    callTotal.incrementAndGet()
    val current = Thread.currentThread()
    if (!owningThread.compareAndSet(null, current) && (owningThread.get() ne current)) {
      foreignThreadCallTotal.incrementAndGet()
    }
  }
}

/**
 * A `ShuffleReadMetricsReporter` that accumulates every reported figure so a test can assert on
 * what the streaming read path actually reported.
 */
class RecordingStreamingShuffleReadMetrics extends ShuffleReadMetricsReporter {

  private val remoteBlocksFetchedTotal = new AtomicLong(0L)
  private val localBlocksFetchedTotal = new AtomicLong(0L)
  private val remoteBytesReadTotal = new AtomicLong(0L)
  private val remoteBytesReadToDiskTotal = new AtomicLong(0L)
  private val localBytesReadTotal = new AtomicLong(0L)
  private val fetchWaitTimeTotal = new AtomicLong(0L)
  private val recordsReadTotal = new AtomicLong(0L)
  private val corruptMergedBlockChunksTotal = new AtomicLong(0L)
  private val mergedFetchFallbackTotal = new AtomicLong(0L)
  private val remoteMergedBlocksFetchedTotal = new AtomicLong(0L)
  private val localMergedBlocksFetchedTotal = new AtomicLong(0L)
  private val remoteMergedChunksFetchedTotal = new AtomicLong(0L)
  private val localMergedChunksFetchedTotal = new AtomicLong(0L)
  private val remoteMergedBytesReadTotal = new AtomicLong(0L)
  private val localMergedBytesReadTotal = new AtomicLong(0L)
  private val remoteReqsDurationTotal = new AtomicLong(0L)
  private val remoteMergedReqsDurationTotal = new AtomicLong(0L)
  private val callTotal = new AtomicLong(0L)
  private val owningThread = new AtomicReference[Thread](null)
  private val foreignThreadCallTotal = new AtomicLong(0L)

  private[spark] override def incRemoteBlocksFetched(v: Long): Unit =
    add(remoteBlocksFetchedTotal, v)

  private[spark] override def incLocalBlocksFetched(v: Long): Unit =
    add(localBlocksFetchedTotal, v)

  private[spark] override def incRemoteBytesRead(v: Long): Unit = add(remoteBytesReadTotal, v)

  private[spark] override def incRemoteBytesReadToDisk(v: Long): Unit =
    add(remoteBytesReadToDiskTotal, v)

  private[spark] override def incLocalBytesRead(v: Long): Unit = add(localBytesReadTotal, v)

  private[spark] override def incFetchWaitTime(v: Long): Unit = add(fetchWaitTimeTotal, v)

  private[spark] override def incRecordsRead(v: Long): Unit = add(recordsReadTotal, v)

  private[spark] override def incCorruptMergedBlockChunks(v: Long): Unit =
    add(corruptMergedBlockChunksTotal, v)

  private[spark] override def incMergedFetchFallbackCount(v: Long): Unit =
    add(mergedFetchFallbackTotal, v)

  private[spark] override def incRemoteMergedBlocksFetched(v: Long): Unit =
    add(remoteMergedBlocksFetchedTotal, v)

  private[spark] override def incLocalMergedBlocksFetched(v: Long): Unit =
    add(localMergedBlocksFetchedTotal, v)

  private[spark] override def incRemoteMergedChunksFetched(v: Long): Unit =
    add(remoteMergedChunksFetchedTotal, v)

  private[spark] override def incLocalMergedChunksFetched(v: Long): Unit =
    add(localMergedChunksFetchedTotal, v)

  private[spark] override def incRemoteMergedBytesRead(v: Long): Unit =
    add(remoteMergedBytesReadTotal, v)

  private[spark] override def incLocalMergedBytesRead(v: Long): Unit =
    add(localMergedBytesReadTotal, v)

  private[spark] override def incRemoteReqsDuration(v: Long): Unit =
    add(remoteReqsDurationTotal, v)

  private[spark] override def incRemoteMergedReqsDuration(v: Long): Unit =
    add(remoteMergedReqsDurationTotal, v)

  def remoteBlocksFetched: Long = remoteBlocksFetchedTotal.get()

  def localBlocksFetched: Long = localBlocksFetchedTotal.get()

  def remoteBytesRead: Long = remoteBytesReadTotal.get()

  def remoteBytesReadToDisk: Long = remoteBytesReadToDiskTotal.get()

  def localBytesRead: Long = localBytesReadTotal.get()

  def fetchWaitTime: Long = fetchWaitTimeTotal.get()

  def recordsRead: Long = recordsReadTotal.get()

  def corruptMergedBlockChunks: Long = corruptMergedBlockChunksTotal.get()

  def mergedFetchFallbackCount: Long = mergedFetchFallbackTotal.get()

  def remoteMergedBlocksFetched: Long = remoteMergedBlocksFetchedTotal.get()

  def localMergedBlocksFetched: Long = localMergedBlocksFetchedTotal.get()

  def remoteMergedChunksFetched: Long = remoteMergedChunksFetchedTotal.get()

  def localMergedChunksFetched: Long = localMergedChunksFetchedTotal.get()

  def remoteMergedBytesRead: Long = remoteMergedBytesReadTotal.get()

  def localMergedBytesRead: Long = localMergedBytesReadTotal.get()

  def remoteReqsDuration: Long = remoteReqsDurationTotal.get()

  def remoteMergedReqsDuration: Long = remoteMergedReqsDurationTotal.get()

  def callCount: Long = callTotal.get()

  def foreignThreadCallCount: Long = foreignThreadCallTotal.get()

  def reportingThread: Option[Thread] = Option(owningThread.get())

  def mergedMetricsTotal: Long = {
    corruptMergedBlockChunksTotal.get() + mergedFetchFallbackTotal.get() +
      remoteMergedBlocksFetchedTotal.get() + localMergedBlocksFetchedTotal.get() +
      remoteMergedChunksFetchedTotal.get() + localMergedChunksFetchedTotal.get() +
      remoteMergedBytesReadTotal.get() + localMergedBytesReadTotal.get() +
      remoteMergedReqsDurationTotal.get()
  }

  def reset(): Unit = {
    remoteBlocksFetchedTotal.set(0L)
    localBlocksFetchedTotal.set(0L)
    remoteBytesReadTotal.set(0L)
    remoteBytesReadToDiskTotal.set(0L)
    localBytesReadTotal.set(0L)
    fetchWaitTimeTotal.set(0L)
    recordsReadTotal.set(0L)
    corruptMergedBlockChunksTotal.set(0L)
    mergedFetchFallbackTotal.set(0L)
    remoteMergedBlocksFetchedTotal.set(0L)
    localMergedBlocksFetchedTotal.set(0L)
    remoteMergedChunksFetchedTotal.set(0L)
    localMergedChunksFetchedTotal.set(0L)
    remoteMergedBytesReadTotal.set(0L)
    localMergedBytesReadTotal.set(0L)
    remoteReqsDurationTotal.set(0L)
    remoteMergedReqsDurationTotal.set(0L)
    callTotal.set(0L)
    foreignThreadCallTotal.set(0L)
    owningThread.set(null)
  }

  private def add(counter: AtomicLong, delta: Long): Unit = {
    counter.addAndGet(delta)
    observeCallingThread()
  }

  private def observeCallingThread(): Unit = {
    callTotal.incrementAndGet()
    val current = Thread.currentThread()
    if (!owningThread.compareAndSet(null, current) && (owningThread.get() ne current)) {
      foreignThreadCallTotal.incrementAndGet()
    }
  }
}

/** Wrapper for a managed buffer that counts retain and release calls. */
class RecordingStreamingManagedBuffer(underlyingBuffer: NioManagedBuffer) extends ManagedBuffer {

  private val retainCalls = new AtomicInteger(0)
  private val releaseCalls = new AtomicInteger(0)

  def callsToRetain: Int = retainCalls.get()

  def callsToRelease: Int = releaseCalls.get()

  override def size(): Long = underlyingBuffer.size()
  override def nioByteBuffer(): ByteBuffer = underlyingBuffer.nioByteBuffer()
  override def createInputStream(): InputStream = underlyingBuffer.createInputStream()
  override def convertToNetty(): AnyRef = underlyingBuffer.convertToNetty()
  override def convertToNettyForSsl(): AnyRef = underlyingBuffer.convertToNettyForSsl()

  override def retain(): ManagedBuffer = {
    retainCalls.incrementAndGet()
    underlyingBuffer.retain()
  }

  override def release(): ManagedBuffer = {
    releaseCalls.incrementAndGet()
    underlyingBuffer.release()
  }

  def releasedExactlyOnce: Boolean = {
    val retained = retainCalls.get()
    val released = releaseCalls.get()
    retained > 0 && retained == released
  }

  /** Retains still outstanding. */
  def outstandingReferences: Int = retainCalls.get() - releaseCalls.get()
}

/**
 * A buffer-utilisation contributor whose reported numerator and denominator are fixed by the test.
 *
 * @param bufferedBytes bytes to report as currently buffered, the gauge's numerator
 * @param budgetBytes bytes to report as the total allowance, the gauge's denominator
 */
class FixedBufferUtilizationContributor(bufferedBytes: Long, budgetBytes: Long)
  extends StreamingShuffleBufferUtilizationContributor {

  private val buffered = new AtomicLong(bufferedBytes)
  private val budget = new AtomicLong(budgetBytes)

  override def contributedBufferedBytes: Long = buffered.get()

  override def contributedBudgetBytes: Long = budget.get()

  def setBufferedBytes(bytes: Long): Unit = buffered.set(bytes)

  def setBudgetBytes(bytes: Long): Unit = budget.set(bytes)
}

sealed abstract class StreamingShuffleFaultScenario(val faultName: String)

object StreamingShuffleFaultScenario {

  case object ProducerCrashDuringWrite
    extends StreamingShuffleFaultScenario("producerCrashDuringWrite")

  case object ConsumerCrashDuringRead
    extends StreamingShuffleFaultScenario("consumerCrashDuringRead")

  case object NetworkPartition extends StreamingShuffleFaultScenario("networkPartition")

  case object MemoryExhaustionOnAllocation
    extends StreamingShuffleFaultScenario("memoryExhaustionOnAllocation")

  case object DiskFailureDuringSpill extends StreamingShuffleFaultScenario("diskFailureDuringSpill")

  case object ChecksumMismatchOnReceive
    extends StreamingShuffleFaultScenario("checksumMismatchOnReceive")

  case object ConnectionTimeoutDuringTransfer
    extends StreamingShuffleFaultScenario("connectionTimeoutDuringTransfer")

  case object ExecutorGarbageCollectionPause
    extends StreamingShuffleFaultScenario("executorGarbageCollectionPause")

  case object ConcurrentProducerFailures
    extends StreamingShuffleFaultScenario("concurrentProducerFailures")

  case object ConsumerReconnectAfterDowntime
    extends StreamingShuffleFaultScenario("consumerReconnectAfterDowntime")

  val all: Seq[StreamingShuffleFaultScenario] = Seq(
    ProducerCrashDuringWrite,
    ConsumerCrashDuringRead,
    NetworkPartition,
    MemoryExhaustionOnAllocation,
    DiskFailureDuringSpill,
    ChecksumMismatchOnReceive,
    ConnectionTimeoutDuringTransfer,
    ExecutorGarbageCollectionPause,
    ConcurrentProducerFailures,
    ConsumerReconnectAfterDowntime)
}

/**
 * Deterministic fault-injection controller for the streaming shuffle suites.
 *
 * @param clock the manual clock the component under test was constructed with, so the component
 *     and the fault share one notion of now
 */
class StreamingShuffleFaultInjector(val clock: ManualClock) {

  private class FaultState {
    val remainingTriggers = new AtomicInteger(0)
    val fireTally = new AtomicInteger(0)
  }

  private val states: Map[StreamingShuffleFaultScenario, FaultState] =
    StreamingShuffleFaultScenario.all.map(scenario => scenario -> new FaultState).toMap

  private val partitioned = new AtomicBoolean(false)
  private val grantedAllocationBytes = new AtomicLong(Long.MaxValue)

  private def stateOf(scenario: StreamingShuffleFaultScenario): FaultState = {
    states.getOrElse(scenario,
      throw new IllegalArgumentException(
        s"Unknown streaming shuffle fault scenario: ${scenario.faultName}"))
  }

  private def tally(scenario: StreamingShuffleFaultScenario): Unit = {
    stateOf(scenario).fireTally.incrementAndGet()
    ()
  }

  def arm(scenario: StreamingShuffleFaultScenario, triggers: Int = 1): Unit = {
    require(triggers > 0, s"triggers must be positive but was $triggers")
    stateOf(scenario).remainingTriggers.set(triggers)
  }

  def armIndefinitely(scenario: StreamingShuffleFaultScenario): Unit = {
    stateOf(scenario).remainingTriggers.set(Int.MaxValue)
  }

  def disarm(scenario: StreamingShuffleFaultScenario): Unit = {
    stateOf(scenario).remainingTriggers.set(0)
  }

  def disarmAll(): Unit = {
    StreamingShuffleFaultScenario.all.foreach(disarm)
    partitioned.set(false)
    grantedAllocationBytes.set(Long.MaxValue)
  }

  def isArmed(scenario: StreamingShuffleFaultScenario): Boolean =
    stateOf(scenario).remainingTriggers.get() > 0

  def fireCount(scenario: StreamingShuffleFaultScenario): Int = stateOf(scenario).fireTally.get()

  def totalFireCount: Int =
    StreamingShuffleFaultScenario.all.map(fireCount).sum

  def shouldFail(scenario: StreamingShuffleFaultScenario): Boolean = {
    val state = stateOf(scenario)
    var decided = false
    var fired = false
    while (!decided) {
      val remaining = state.remainingTriggers.get()
      if (remaining <= 0) {
        decided = true
      } else if (remaining == Int.MaxValue) {
        decided = true
        fired = true
      } else if (state.remainingTriggers.compareAndSet(remaining, remaining - 1)) {
        decided = true
        fired = true
      }
    }
    if (fired) {
      state.fireTally.incrementAndGet()
    }
    fired
  }

  def failIfArmed(scenario: StreamingShuffleFaultScenario, failure: => Throwable): Unit = {
    if (shouldFail(scenario)) {
      throw failure
    }
  }

  def crashProducer(): Unit = {
    armIndefinitely(StreamingShuffleFaultScenario.ProducerCrashDuringWrite)
    partitioned.set(true)
  }

  def crashConsumer(): Unit = {
    armIndefinitely(StreamingShuffleFaultScenario.ConsumerCrashDuringRead)
    partitioned.set(true)
  }

  def crashProducersConcurrently(numProducers: Int): Unit = {
    require(numProducers > 0, s"numProducers must be positive but was $numProducers")
    arm(StreamingShuffleFaultScenario.ConcurrentProducerFailures, numProducers)
    partitioned.set(true)
  }

  def partitionNetwork(): Unit = {
    armIndefinitely(StreamingShuffleFaultScenario.NetworkPartition)
    partitioned.set(true)
  }

  def healNetwork(): Unit = {
    disarm(StreamingShuffleFaultScenario.NetworkPartition)
    partitioned.set(false)
  }

  def networkPartitioned: Boolean = partitioned.get()

  def limitAllocationTo(grantableBytes: Long): Unit = {
    require(grantableBytes >= 0L, s"grantableBytes must be non-negative but was $grantableBytes")
    armIndefinitely(StreamingShuffleFaultScenario.MemoryExhaustionOnAllocation)
    grantedAllocationBytes.set(grantableBytes)
  }

  def restoreAllocation(): Unit = {
    disarm(StreamingShuffleFaultScenario.MemoryExhaustionOnAllocation)
    grantedAllocationBytes.set(Long.MaxValue)
  }

  def grantAllocation(requestedBytes: Long): Long = {
    require(requestedBytes >= 0L, s"requestedBytes must be non-negative but was $requestedBytes")
    if (isArmed(StreamingShuffleFaultScenario.MemoryExhaustionOnAllocation)) {
      math.min(requestedBytes, grantedAllocationBytes.get())
    } else {
      requestedBytes
    }
  }

  def failDiskWrites(): ScopedDiskFault = {
    val directory = Utils.createTempDir(namePrefix = "streaming-shuffle-disk-fault")
    val applied = directory.setWritable(false)
    val effective = applied && !directory.canWrite
    if (effective) {
      armIndefinitely(StreamingShuffleFaultScenario.DiskFailureDuringSpill)
    } else if (applied) {
      directory.setWritable(true)
    }
    new ScopedDiskFault(directory, effective, this)
  }

  /** Returns a checksum that is guaranteed to differ from the correct one for the given payload. */
  def corruptChecksum(correctChecksum: Long): Long = {
    tally(StreamingShuffleFaultScenario.ChecksumMismatchOnReceive)
    correctChecksum ^ 1L
  }

  /**
   * Corrupts a payload in place of transmission damage, deterministically and detectably.
   *
   * @return a copy that differs from `payload` in exactly one byte
   */
  def corruptPayload(payload: Array[Byte]): Array[Byte] = {
    tally(StreamingShuffleFaultScenario.ChecksumMismatchOnReceive)
    if (payload.isEmpty) {
      Array[Byte](1)
    } else {
      val damaged = payload.clone()
      damaged(0) = (damaged(0) ^ 0xFF).toByte
      damaged
    }
  }

  /** Simulates a garbage-collection pause by advancing the manual clock, never by sleeping. */
  def simulateGarbageCollectionPause(pauseMillis: Long): Unit = {
    require(pauseMillis >= 0L, s"pauseMillis must be non-negative but was $pauseMillis")
    tally(StreamingShuffleFaultScenario.ExecutorGarbageCollectionPause)
    clock.advance(pauseMillis)
  }

  def expireProducerConnection(): Unit = {
    tally(StreamingShuffleFaultScenario.ConnectionTimeoutDuringTransfer)
    clock.advance(StreamingShuffleTestHelper.ProducerConnectionTimeoutMillis)
  }

  def expireConsumerLiveness(): Unit = {
    clock.advance(StreamingShuffleTestHelper.ConsumerLivenessTimeoutMillis)
  }

  /**
   * Simulates a consumer that returns after an extended absence.
   *
   * @param downtimeMillis how long the consumer was away; must exceed the liveness window for
   *     the writer to have noticed the absence at all
   */
  def reconnectConsumerAfterDowntime(downtimeMillis: Long): Unit = {
    require(downtimeMillis > StreamingShuffleTestHelper.ConsumerLivenessTimeoutMillis,
      "downtimeMillis must exceed the consumer liveness window of " +
        s"${StreamingShuffleTestHelper.ConsumerLivenessTimeoutMillis} ms but was $downtimeMillis")
    partitioned.set(true)
    clock.advance(downtimeMillis)
    partitioned.set(false)
    tally(StreamingShuffleFaultScenario.ConsumerReconnectAfterDowntime)
  }
}

/**
 * One suite-owned directory made read-only for the duration of a scope, removed when it ends.
 *
 * @param directory the directory whose write permission is suspended, created by and owned by
 *     this fixture, which is why nothing else in the JVM can be affected by it
 */
class ScopedDiskFault(
    val directory: File,
    val isEffective: Boolean,
    injector: StreamingShuffleFaultInjector) extends AutoCloseable {

  private val closed = new AtomicBoolean(false)

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      if (isEffective) {
        injector.disarm(StreamingShuffleFaultScenario.DiskFailureDuringSpill)
      }
      directory.setWritable(true)
      Utils.deleteRecursively(directory)
    }
  }
}

private[spark] class MidWriteTripHolder(val reason: StreamingShuffleFallbackReason)
  extends Serializable {

  @volatile var shuffleId: Int = -1

  def trip(): Unit = {
    SparkEnv.get.shuffleManager match {
      case manager: StreamingShuffleManager if shuffleId >= 0 =>
        StreamingShuffleTestHelper.driveFallbackReason(
          manager.streamingFallbackPolicy, reason, shuffleId)
      case _ =>
    }
  }
}

/**
 * Applies one of the ten enumerated failure scenarios to the '''live''' streaming shuffle a running
 * job is using, on the executor, through production APIs only.
 *
 * @param recordsBeforeFault records to let through before applying it
 */
private[spark] class LiveFaultHolder(
    val scenario: StreamingShuffleFaultScenario,
    val faultPartitions: Set[Int] = Set.empty,
    val recordsBeforeFault: Int = 0)
  extends Serializable {

  @volatile var shuffleId: Int = -1

  def applyOnMapSide(partitionIndex: Int, recordIndex: Int): Int = {
    if (!eligible(partitionIndex, recordIndex)) {
      0
    } else {
      scenario match {
        case StreamingShuffleFaultScenario.ProducerCrashDuringWrite |
             StreamingShuffleFaultScenario.ConcurrentProducerFailures =>
          crashThisAttempt(s"producer of map partition $partitionIndex")
        case StreamingShuffleFaultScenario.MemoryExhaustionOnAllocation =>
          once(commitBufferAllowance())
        case StreamingShuffleFaultScenario.ExecutorGarbageCollectionPause =>
          once(forceCollectionPause())
        case _ => 0
      }
    }
  }

  def applyOnReduceSide(partitionIndex: Int, recordIndex: Int): Int = {
    if (!eligible(partitionIndex, recordIndex)) {
      0
    } else {
      scenario match {
        case StreamingShuffleFaultScenario.ConsumerCrashDuringRead |
             StreamingShuffleFaultScenario.ConsumerReconnectAfterDowntime =>
          crashThisAttempt(s"consumer of reduce partition $partitionIndex")
        case StreamingShuffleFaultScenario.NetworkPartition |
             StreamingShuffleFaultScenario.ConnectionTimeoutDuringTransfer =>
          once(severTransport())
        case StreamingShuffleFaultScenario.DiskFailureDuringSpill =>
          once(destroySpilledSegments())
        case StreamingShuffleFaultScenario.ChecksumMismatchOnReceive =>
          once(corruptSpilledSegments())
        case _ => 0
      }
    }
  }

  private def eligible(partitionIndex: Int, recordIndex: Int): Boolean = {
    shuffleId >= 0 && recordIndex == recordsBeforeFault &&
      (faultPartitions.isEmpty || faultPartitions.contains(partitionIndex))
  }

  private def once(body: => Int): Int = {
    if (LiveFaultHolder.claim(scenario, shuffleId)) body else 0
  }

  private def crashThisAttempt(role: String): Int = {
    val attempt = Option(TaskContext.get()).map(_.attemptNumber()).getOrElse(0)
    if (attempt == 0) {
      throw new IllegalStateException(
        s"Injected streaming shuffle ${scenario.faultName}: the $role of shuffle $shuffleId died " +
          s"after $recordsBeforeFault record(s)")
    }
    0
  }

  private def severTransport(): Int = {
    SparkEnv.get.shuffleManager match {
      case manager: StreamingShuffleManager =>
        manager.boundStreamingListener.map(_.deregisterShuffle(shuffleId)).getOrElse(0)
      case _ => 0
    }
  }

  private def commitBufferAllowance(): Int = {
    val quota = MemorySpillManager.executorQuota(SparkEnv.get.conf)
    var request = quota.totalBytes - quota.reservedBytes
    var taken = 0L
    while (request > 0L && taken == 0L) {
      if (quota.tryReserve(request)) {
        taken = request
      } else {
        request /= 2L
      }
    }
    if (taken > 0L) {
      val held = taken
      Option(TaskContext.get()).foreach { context =>
        context.addTaskCompletionListener[Unit](_ => quota.release(held))
      }
      1
    } else {
      0
    }
  }

  private def forceCollectionPause(): Int = {
    val ballast = Array.fill(LiveFaultHolder.GC_BALLAST_CHUNKS)(
      new Array[Byte](LiveFaultHolder.GC_BALLAST_CHUNK_BYTES))
    var touchedBytes = 0L
    var index = 0
    while (index < ballast.length) {
      ballast(index)(0) = index.toByte
      touchedBytes += ballast(index).length.toLong
      index += 1
    }
    System.gc()
    if (touchedBytes > 0L) 1 else 0
  }

  private def spilledSegments(): Seq[File] = {
    SparkEnv.get.blockManager.diskBlockManager.getAllFiles()
      .filter(file => file.getName.startsWith(LiveFaultHolder.TEMP_SHUFFLE_PREFIX))
      .toSeq
  }

  private def destroySpilledSegments(): Int = spilledSegments().count(file => file.delete())

  private def corruptSpilledSegments(): Int = {
    spilledSegments().count { file =>
      val length = file.length()
      if (length <= 0L) {
        false
      } else {
        val handle = new RandomAccessFile(file, "rw")
        try {
          val offset = length / 2L
          handle.seek(offset)
          val original = handle.readByte()
          handle.seek(offset)
          handle.writeByte(original ^ 0xFF)
          true
        } finally {
          handle.close()
        }
      }
    }
  }
}

private[spark] object LiveFaultHolder {

  val TEMP_SHUFFLE_PREFIX: String = "temp_shuffle"

  val GC_BALLAST_CHUNKS: Int = 16

  val GC_BALLAST_CHUNK_BYTES: Int = 4 * 1024 * 1024

  private val applied = ConcurrentHashMap.newKeySet[(String, Int)]()

  private def claim(scenario: StreamingShuffleFaultScenario, shuffleId: Int): Boolean =
    applied.add((scenario.faultName, shuffleId))

  def reset(): Unit = applied.clear()
}

/** Constants and pure helpers shared by every streaming shuffle suite and by the benchmark. */
object StreamingShuffleTestHelper {

  private val TestApplicationId = "streaming-shuffle-test-application"

  private val TestAuthenticationSecret = "streaming-shuffle-test-authentication-secret"

  val DefaultBufferSizePercent: Int = 20

  val MinBufferSizePercent: Int = 1

  val MaxBufferSizePercent: Int = 50

  val DefaultSpillThresholdPercent: Int = 80

  val MinSpillThresholdPercent: Int = 50

  val MaxSpillThresholdPercent: Int = 95

  val PercentScale: Long = MemorySpillManager.PERCENT_SCALE

  val SpillPollIntervalMillis: Long = MemorySpillManager.POLL_INTERVAL_MS

  val ReclamationDeadlineMillis: Long = MemorySpillManager.RECLAMATION_DEADLINE_MS

  val DiskWriterRecordUpdateInterval: Int = 16384

  /** Property naming the number of times a service may retry a port bind before giving up. */
  val PortMaxRetriesKey: String = "spark.port.maxRetries"

  /**
   * Bind retries every fixture grants, which is the figure `Utils.portMaxRetries` hands to any
   * suite whose configuration carries `spark.testing`.
   */
  val TestPortMaxRetries: Int = 100

  /** Lowest execution-memory accounting identity a fixture task memory manager may use. */
  val FixtureAttemptIdBase: Long = 1000000000000L

  val ProtocolVersion: Byte = 1

  val HeaderEncodedLength: Int = 17

  val ProducerIdEncodedLength: Int = 8

  val FrameTypePrefixLength: Int = 1

  val MaxBlockSizeBytes: Int = 2 * 1024 * 1024

  val DataBlockFramingOverheadBytes: Int = 1 + 17 + 8 + 8 + 4

  val MaxEncodedFrameBytes: Int = 2 * 1024 * 1024 + 38

  val FixedMessageEncodedLength: Int = 25

  val RetransmitRequestEncodedLength: Int = FixedMessageEncodedLength + 8

  val HeartbeatBaseEncodedLength: Int = FixedMessageEncodedLength + 8

  val NothingConsumedPosition: Long = -1L

  val NoConsumerToken: Long = 0L

  val MaxRequestedBlocksPerRequest: Long = RetransmitRequestMessage.MAX_REQUESTED_BLOCKS

  val MaxReplayWindowBlocks: Long = StreamingShuffleClientHandler.MAX_REPLAY_WINDOW_BLOCKS

  val ChecksumAlgorithm: String = "CRC32C"

  def verifyWireContractAgainstEncoder(): Unit = {
    val mismatches = new mutable.ArrayBuffer[String]()
    def check(field: String, expected: Any, actual: Any): Unit = {
      if (expected != actual) {
        mismatches += s"$field: this suite's contract says $expected, the encoder says $actual"
      }
    }
    check("protocol version", ProtocolVersion, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)
    check("header encoded length", HeaderEncodedLength,
      StreamingShuffleMessage.HEADER_ENCODED_LENGTH)
    check("frame type prefix length", FrameTypePrefixLength,
      StreamingShuffleMessage.FRAME_TYPE_PREFIX_LENGTH)
    check("max block payload", MaxBlockSizeBytes, DataBlockMessage.MAX_BLOCK_SIZE_BYTES)
    check("data block framing overhead", DataBlockFramingOverheadBytes,
      DataBlockMessage.FRAMING_OVERHEAD_BYTES)
    check("max encoded frame", MaxEncodedFrameBytes, DataBlockMessage.MAX_ENCODED_FRAME_BYTES)
    check("producer id encoded length", ProducerIdEncodedLength,
      StreamingShuffleMessage.PRODUCER_ID_ENCODED_LENGTH)
    check("control message encoded length", FixedMessageEncodedLength,
      StreamingShuffleMessage.CONTROL_MESSAGE_ENCODED_LENGTH)
    check("nothing consumed sentinel", NothingConsumedPosition, AckMessage.NOTHING_CONSUMED)
    check("max requested blocks per request", MaxRequestedBlocksPerRequest,
      RetransmitRequestMessage.MAX_REQUESTED_BLOCKS)
    check("checksum algorithm", ChecksumAlgorithm, StreamingShuffleChecksum.ALGORITHM)
    val payload = Array[Byte](1, 2, 3, 4)
    val payloadChecksum = StreamingShuffleChecksum.computeBlock(1, 2L, 3, 4L, payload)
    check("acknowledgement encoded length", FixedMessageEncodedLength,
      new AckMessage(1, 2L, 3, 5L).encodedLength())
    check("retransmission request encoded length", RetransmitRequestEncodedLength,
      new RetransmitRequestMessage(1, 2L, 3, 4L).encodedLength())
    check("ranged retransmission request encoded length", RetransmitRequestEncodedLength,
      new RetransmitRequestMessage(1, 2L, 3, 4L, 9L).encodedLength())
    check("stream termination encoded length", FixedMessageEncodedLength,
      new StreamTerminationMessage(1, 2L, 3, 4L).encodedLength())
    check("heartbeat encoded length", HeartbeatBaseEncodedLength,
      new HeartbeatMessage(1, 2L, 3, 4L).encodedLength())
    check("identified heartbeat encoded length", HeartbeatBaseEncodedLength,
      new HeartbeatMessage(1, 2L, 3, 4L, 7L).encodedLength())
    check("no consumer token sentinel", NoConsumerToken, HeartbeatMessage.NO_CONSUMER_TOKEN)
    check("data block encoded length",
      HeaderEncodedLength + ProducerIdEncodedLength + 8 + 4 + payload.length,
      new DataBlockMessage(1, 2L, 3, 4L, payloadChecksum, payload).encodedLength())
    assert(mismatches.isEmpty,
      "The streaming shuffle wire format has drifted from the contract these suites assert " +
        "against. Either the encoder changed and every peer of a different build must be " +
        "considered, or the change was unintended: " + mismatches.mkString("; ") + ".")
  }

  val ProducerConnectionTimeoutMillis: Long = BackpressureProtocol.ACK_TIMEOUT_MS

  val HeartbeatIntervalMillis: Long = BackpressureProtocol.HEARTBEAT_INTERVAL_MS

  val ConsumerLivenessTimeoutMillis: Long = BackpressureProtocol.CONSUMER_LIVENESS_TIMEOUT_MS

  val SustainedSlownessWindowMillis: Long = BackpressureProtocol.SUSTAINED_SLOWNESS_WINDOW_MS

  val SustainedSlownessTripMillis: Long = SustainedSlownessWindowMillis + 1L

  val JustBeforeProducerTimeoutMillis: Long = ProducerConnectionTimeoutMillis - 1L

  val JustBeforeConsumerLivenessMillis: Long = ConsumerLivenessTimeoutMillis - 1L

  val JustBeforeSustainedSlownessMillis: Long = SustainedSlownessWindowMillis - 1L

  val SaturationSampleWindowMillis: Long = BackpressureProtocol.SATURATION_SAMPLE_WINDOW_MS

  val RetryBaseBackoffMillis: Long = BackpressureProtocol.RETRY_BASE_BACKOFF_MS

  val MaxRetryAttempts: Int = BackpressureProtocol.MAX_RETRY_ATTEMPTS

  /** The complete exponential backoff ladder, one entry per permitted attempt. */
  val RetryBackoffLadderMillis: Seq[Long] =
    (1 to MaxRetryAttempts).map(attempt => BackpressureProtocol.retryBackoffMillis(attempt))

  val BandwidthCeilingPercent: Long = TokenBucketRateLimiter.BANDWIDTH_CEILING_PERCENT

  val LinkSaturationTripPercent: Long = BackpressureProtocol.LINK_SATURATION_PERCENT

  /**
   * Consecutive measurement intervals an over-capacity reading must span before saturation counts.
   *
   * Taken from the policy rather than restated, so a suite that drives "a sustained saturation"
   * drives exactly the run the policy requires, and both follow the constant if it ever moves. The
   * protocol's own constant is asserted equal to it by the numeric-contract case, so one figure
   * governs both views of the condition.
   */
  val SaturationSustainedIntervals: Long =
    StreamingShuffleFallbackPolicy.SATURATION_SUSTAINED_INTERVALS

  /**
   * Consecutive over-share intervals a given administered capacity's saturation must span.
   *
   * Asked of the production derivation rather than assumed, because the run absorbs the mandatory
   * burst allowance of that capacity: a small capacity needs a longer run than a large one, and a
   * suite that drove a fixed number would trip nothing at one capacity and prove nothing at
   * another.
   */
  def saturationIntervalsFor(capacityBytesPerSecond: Double): Long =
    StreamingShuffleFallbackPolicy.sustainedIntervalsFor(capacityBytesPerSecond)

  /**
   * The administered capacity, in bytes per second, that the shared saturation driver measures
   * against, and an egress figure strictly above the trip share of it.
   *
   * A hundred, and ninety-nine, because the ratio is the whole of what the condition reads and
   * round numbers make the arithmetic checkable by eye: ninety-nine over a hundred is ninety-nine
   * percent, which is above the ninety percent share and below saturating the link outright.
   */
  val SaturationCapacityBytesPerSecond: Double = 100.0d

  val SaturatedEgressBytesPerSecond: Double = 99.0d

  val BytesPerMebibyte: Long = TokenBucketRateLimiter.BYTES_PER_MIB

  val ConsumerSlownessRatio: Double = StreamingShuffleFallbackPolicy.CONSUMER_SLOWNESS_RATIO

  val MinTokenBucketCapacityBytes: Long = MaxBlockSizeBytes.toLong

  val TransportModuleName: String = "shuffle-streaming"

  val MetricsSourceName: String = StreamingShuffleMetricsSource.sourceName

  val BufferUtilizationMetricName: String = "bufferUtilizationPercent"

  val SpillCountMetricName: String = "spillCount"

  val BackpressureEventsMetricName: String = "backpressureEvents"

  val PartialReadInvalidationsMetricName: String = "partialReadInvalidations"

  val MetricNames: Seq[String] = Seq(
    BufferUtilizationMetricName,
    SpillCountMetricName,
    BackpressureEventsMetricName,
    PartialReadInvalidationsMetricName)

  val StreamingShuffleLoggerName: String = classOf[StreamingShuffleManager].getPackage.getName

  val DefaultPartitionCount: Int = 10

  val TargetDatasetBytes: Long = 100L * 1024L * 1024L

  val RecordValueLength: Int = 256

  val StressDurationMillis: Long = 5L * 60L * 1000L

  val StressConcurrentTasks: Int = 10

  val StressConcurrentShuffles: Int = 5

  val StressFailureInjectionPercent: Int = 10

  val MaxThroughputDegradationPercent: Int = 5

  val MinLatencyReductionPercent: Int = 30

  val MaxLatencyReductionPercent: Int = 50

  val PerTestTimeoutMinutes: Int = 20

  val DefaultAwaitTimeoutMillis: Long = 30000L

  val ThreadSettlementGraceMillis: Long = 10000L

  val StuckThreadStackFrames: Int = 12
  // Progress-watchdog defaults. See StreamingShuffleTestHelper.withProgressWatchdog for why a
  // watchdog is needed at all and for exactly what it can and cannot do.

  /**
   * Wall-clock budget a watched body may take before the case fails.
   *
   * Chosen generously against the per-test ceiling the base suite imposes: a body that overruns
   * this has not merely been unlucky on a busy host, it has changed complexity class.
   */
  val DefaultProgressBudgetMillis: Long = 300000L

  /** How long a watched body's progress reading may stand still before a diagnosis is recorded. */
  val DefaultProgressStallMillis: Long = 20000L

  /** How often the watchdog samples the progress reading. */
  val ProgressPollIntervalMillis: Long = 500L

  /** Frames of the stalled thread's stack recorded in a diagnosis. */
  val ProgressStackFrames: Int = 24

  /** How long a returning body waits for its watchdog to observe completion and exit. */
  val ProgressWatchdogJoinMillis: Long = 5000L

  /** Starting time of a manual clock, chosen non-zero so a bug that reads zero stands out. */
  val ManualClockEpochMillis: Long = 1000000L

  // ---------------------------------------------------------------------------------------------
  // JVM-global Hadoop credential isolation. See StreamingShuffleHadoopCredentialIsolation for why
  // this is needed at all; these two are its mechanism, exposed so that a case can assert on the
  // very state the bracket protects.
  // ---------------------------------------------------------------------------------------------

  /**
   * The secret the Hadoop login user currently holds under Spark's own key, if any.
   *
   * Read from the login user rather than from `getCurrentUser`, deliberately: the login user is the
   * JVM-global one whose credentials survive every context, and it is what `initializeAuth` writes
   * to and what `getSecretKey` reads back outside a `doAs` block. The returned array is a copy --
   * `UserGroupInformation.getCredentials` copies -- so a caller holding it cannot mutate the store.
   */
  def loginUserAuthSecret(): Option[Array[Byte]] = {
    Option(UserGroupInformation.getLoginUser().getCredentials()
      .getSecretKey(SecurityManager.SECRET_LOOKUP_KEY))
  }

  /** Whether two [[loginUserAuthSecret]] readings describe the same state, absence included. */
  def sameLoginUserAuthSecret(one: Option[Array[Byte]], other: Option[Array[Byte]]): Boolean = {
    (one, other) match {
      case (None, None) => true
      case (Some(left), Some(right)) => JArrays.equals(left, right)
      case _ => false
    }
  }

  /**
   * A [[loginUserAuthSecret]] reading rendered for a failure message.
   *
   * The secret itself is never rendered. It is a credential, and a test that printed one would put
   * it in a log file that outlives the run; a length and a short digest identify a reading uniquely
   * enough to tell two of them apart, which is all a message here needs to do.
   */
  def describeLoginUserAuthSecret(secret: Option[Array[Byte]]): String = secret match {
    case None => "no secret"
    case Some(bytes) =>
      f"a ${bytes.length}-byte secret with digest 0x${JArrays.hashCode(bytes) & 0xFFFFFFFFL}%08x"
  }

  /**
   * Puts the login user's Spark secret back to a recorded reading, if something has changed it.
   *
   * A no-op when nothing changed, which is the common case and keeps the cost of the bracket at one
   * credential read per suite. When something did change, the login user is discarded so that the
   * next `getLoginUser()` performs the Hadoop login again and returns a user with empty
   * credentials, and the recorded secret -- if there was one -- is re-added on top. Removing the
   * entry in place is not an option: `getCredentials` hands back a copy and the subject holding the
   * live `Credentials` is not reachable through any public method.
   *
   * @param recorded what [[loginUserAuthSecret]] returned before the work that may have changed it
   */
  def restoreLoginUserAuthSecret(recorded: Option[Array[Byte]]): Unit = {
    if (!sameLoginUserAuthSecret(recorded, loginUserAuthSecret())) {
      UserGroupInformation.setLoginUser(null)
      recorded.foreach { secret =>
        val credentials = new Credentials()
        credentials.addSecretKey(SecurityManager.SECRET_LOOKUP_KEY, secret)
        UserGroupInformation.getCurrentUser().addCredentials(credentials)
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Pure helpers. None of these touch a SparkContext, so they are usable from anywhere.
  // ---------------------------------------------------------------------------------------------

  /**
   * The aggregate buffer allowance, computed exactly as the specification states it and
   * independently of how production computes it.
   *
   * @return bytes the executor-wide streaming buffer allowance holds
   */
  def aggregateBudgetBytes(executorMemoryBytes: Long, bufferPercent: Int): Long = {
    require(executorMemoryBytes >= 0L,
      s"executorMemoryBytes must be non-negative but was $executorMemoryBytes")
    require(bufferPercent >= MinBufferSizePercent && bufferPercent <= MaxBufferSizePercent,
      s"bufferPercent must be in [$MinBufferSizePercent, $MaxBufferSizePercent] " +
        s"but was $bufferPercent")
    (BigInt(executorMemoryBytes) * BigInt(bufferPercent) / BigInt(PercentScale)).toLong
  }

  def exactPercentageOf(value: Long, percent: Int): Long = {
    require(value >= 0L, s"value must be non-negative but was $value")
    require(percent >= 0, s"percent must be non-negative but was $percent")
    (BigInt(value) * BigInt(percent) / BigInt(PercentScale)).toLong
  }

  def perPartitionBudgetBytes(
      executorMemoryBytes: Long,
      bufferPercent: Int,
      numPartitions: Int): Long = {
    require(numPartitions > 0, s"numPartitions must be positive but was $numPartitions")
    aggregateBudgetBytes(executorMemoryBytes, bufferPercent) / numPartitions.toLong
  }

  def applyBandwidthCeiling(bytesPerSecond: Long): Long =
    TokenBucketRateLimiter.applyLinkCapacityCeiling(bytesPerSecond)

  def refillBytesPerSecond(maxBandwidthMBps: Int, numConcurrentShuffles: Int): Long =
    applyBandwidthCeiling(
      TokenBucketRateLimiter.perShuffleBytesPerSecond(maxBandwidthMBps, numConcurrentShuffles))

  def dataBlockEncodedLength(payloadLength: Int): Int = {
    require(payloadLength >= 0, s"payloadLength must be non-negative but was $payloadLength")
    DataBlockFramingOverheadBytes - FrameTypePrefixLength + payloadLength
  }

  def framedLength(encodedLength: Int): Int = StreamingShuffleMessage.framedLength(encodedLength)

  def messageTypeOf(message: StreamingShuffleMessage): StreamingShuffleMessageType = message match {
    case _: DataBlockMessage => StreamingShuffleMessageType.DATA_BLOCK
    case _: AckMessage => StreamingShuffleMessageType.ACK
    case _: HeartbeatMessage => StreamingShuffleMessageType.HEARTBEAT
    case _: RetransmitRequestMessage => StreamingShuffleMessageType.RETRANSMIT_REQUEST
    case _: StreamTerminationMessage => StreamingShuffleMessageType.STREAM_TERMINATION
    case other =>
      throw new IllegalArgumentException(
        s"Unrecognised streaming shuffle message type: ${other.getClass.getName}")
  }

  /**
   * A deterministic payload of the requested length.
   *
   * @param length bytes to produce
   */
  def deterministicPayload(seed: Long, length: Int): Array[Byte] = {
    require(length >= 0, s"length must be non-negative but was $length")
    val payload = new Array[Byte](length)
    new Random(seed).nextBytes(payload)
    payload
  }

  /** A deterministic value string of [[RecordValueLength]] characters. */
  def deterministicValue(partitionIndex: Int, recordIndex: Int): String = {
    val prefix = s"p$partitionIndex-r$recordIndex-"
    val filler = ('a' + Utils.nonNegativeMod(partitionIndex + recordIndex, 26)).toChar
    if (prefix.length >= RecordValueLength) {
      prefix.substring(0, RecordValueLength)
    } else {
      prefix + filler.toString * (RecordValueLength - prefix.length)
    }
  }

  def rawRecordDigest(record: (Int, String)): (Int, Int, Int) = {
    val (key, value) = record
    var index = 0
    var hash = 0
    while (index < value.length) {
      hash = 31 * hash + value.charAt(index).toInt
      index += 1
    }
    (key, value.length, hash)
  }

  val StreamingShuffleConditionPrefix: String = "STREAMING_SHUFFLE_"

  def failureCountingAccumulator(sc: SparkContext, name: String): LongAccumulator = {
    val accumulator = new LongAccumulator
    accumulator.register(sc, Some(name), countFailedValues = true)
    accumulator
  }

  val StreamingShuffleSqlState: String = "XXKST"

  val AuthorizedErrorConditions: Set[String] = Set(
    s"${StreamingShuffleConditionPrefix}CHECKSUM_VERIFY_FAILED",
    s"${StreamingShuffleConditionPrefix}INVALID_SEQUENCE_NUMBER",
    s"${StreamingShuffleConditionPrefix}UNEXPECTED_MESSAGE_TYPE")

  /**
   * Trips one of the four documented degradation conditions on a policy, deterministically.
   *
   * @param policy the policy to trip, which must not have tripped already -- the latch is
   *     monotone, so a second condition driven at a tripped policy would be recorded and ignored
   */
  def driveFallbackReason(
      policy: StreamingShuffleFallbackPolicy,
      reason: StreamingShuffleFallbackReason,
      shuffleId: Int): Unit = {
    reason match {
      case StreamingShuffleFallbackReason.ConsumerTooSlow =>
        val openedAtMillis = 0L
        val beyondWindowMillis = openedAtMillis + SustainedSlownessWindowMillis + 1L
        policy.recordProducerThroughput(shuffleId, 4000.0d, openedAtMillis)
        policy.recordConsumerThroughput(shuffleId, 1000.0d, openedAtMillis)
        policy.recordProducerThroughput(shuffleId, 4000.0d, beyondWindowMillis)
        policy.recordConsumerThroughput(shuffleId, 1000.0d, beyondWindowMillis)
      case StreamingShuffleFallbackReason.MemoryPressure =>
        policy.recordAllocationGrant(MaxBlockSizeBytes.toLong, 0L)
      case StreamingShuffleFallbackReason.NetworkSaturation =>
        // Ninety-nine percent of the administered link, which is strictly above the trip share,
        // across the run of measurement intervals the condition is sustained over. One reading is
        // deliberately not enough: this subsystem's own pacing bucket must admit one maximum-sized
        // frame, which on a small administered link reads above the share for exactly one interval,
        // so a rule that tripped on one reading stood healthy shuffles down.
        driveLinkSaturation(policy, SaturatedEgressBytesPerSecond, SaturationCapacityBytesPerSecond)
      case StreamingShuffleFallbackReason.ProtocolVersionMismatch =>
        policy.checkProtocolVersion((ProtocolVersion + 1).toByte)
    }
  }

  /**
   * Drives a sustained network saturation on a policy, deterministically and without a clock.
   *
   * Saturation is evaluated over a run of consecutive measurement intervals rather than over one
   * reading, so a caller that recorded a single over-capacity sample would leave the policy
   * untripped -- and would be asserting the behaviour of a rule this feature deliberately does not
   * have. One place drives it, so no suite has to know how long the run is.
   *
   * Every instant is SUPPLIED rather than read from a clock, through the policy's three-argument
   * recording overload, because intervals are identified by quantising the observation's instant
   * onto [[SaturationSampleWindowMillis]]. That makes the run exact under test and makes this
   * usable against the policy a real manager built for itself, whose clock is the system's and
   * which a test cannot advance. The policy's own clock still supplies the trip instant, which is
   * why a manual clock is left where it is: a caller asserting `trippedAtTimeMillis` reads the
   * clock it injected.
   *
   * @param policy the policy to saturate
   * @param usedBytesPerSecond observed egress, which must be strictly above the trip share of the
   *                           capacity or nothing is being driven at all
   * @param capacityBytesPerSecond the administered capacity the observation is measured against
   * @param firstSampleMillis instant the first interval's observation belongs to
   */
  def driveLinkSaturation(
      policy: StreamingShuffleFallbackPolicy,
      usedBytesPerSecond: Double,
      capacityBytesPerSecond: Double,
      firstSampleMillis: Long = ManualClockEpochMillis): Unit = {
    require(usedBytesPerSecond >
        capacityBytesPerSecond * LinkSaturationTripPercent.toDouble / PercentScale.toDouble,
      s"$usedBytesPerSecond bytes/s is not above $LinkSaturationTripPercent percent of " +
        s"$capacityBytesPerSecond bytes/s, so this would drive no saturation at all")
    (0L until saturationIntervalsFor(capacityBytesPerSecond)).foreach { interval =>
      policy.recordLinkUtilization(usedBytesPerSecond, capacityBytesPerSecond,
        firstSampleMillis + interval * SaturationSampleWindowMillis)
    }
  }
}

/**
 * Restores the JVM-global Hadoop credentials a streaming suite's own fixtures overwrite.
 *
 * ==The problem this exists to solve==
 *
 * Streaming shuffle requires authenticated transport, so every fixture that starts a real
 * `SparkContext` on the streaming path sets `spark.authenticate`. `SparkEnv` then calls
 * `SecurityManager.initializeAuth()`, and for a `local`, `local[N]`, `local[N,M]` or `yarn` master
 * that method unconditionally generates a fresh secret -- it does not return early when
 * `spark.authenticate.secret` is already set -- and stores it in
 * `UserGroupInformation.getCurrentUser().getCredentials()`. That store is JVM-global and outlives
 * the context. `SecurityManager.getSecretKey()` consults it *first*, ahead of the local field, the
 * environment, the configuration and the secret file, so from that moment every later
 * `SecurityManager` in the same JVM answers with the streaming fixture's secret.
 *
 * The consequence is not a streaming failure but a failure somewhere else entirely: pre-existing
 * suites whose subject is authentication -- one that asserts a missing secret raises, one that
 * asserts a configured secret is returned verbatim, ones that assert a file-mounted secret is
 * refused for a UGI-storing master -- all see a secret that is present and is not theirs. Nothing
 * about the product is at fault: `SecurityManager` is unmodified Spark behaving exactly as
 * documented. What is at fault is a test fixture mutating process-global state and not putting it
 * back, so that is what this trait fixes, and it fixes it for every suite in the package at once
 * rather than one fixture at a time.
 *
 * ==Why restoring rather than preventing==
 *
 * Preventing the write is not available. The streaming manager only activates with authentication
 * enabled, `SparkEnv` always calls `initializeAuth()` on the driver, and every master these
 * fixtures can use for an in-process context is one of the masters that store in the UGI. Spark's
 * own `SecurityManagerSuite` isolates the same mutation by running inside
 * `UserGroupInformation.createUserForTesting(...).doAs(...)`, but `doAs` is stack scoped and a
 * suite spans many independent contexts across many test bodies, so the equivalent here is to
 * bracket the
 * suite instead of each statement.
 *
 * ==How the restoration is exact==
 *
 * The one credential Spark stores under this key is recorded before the suite runs and compared
 * after it. If the suite changed it, the login user is discarded, which makes the next
 * `getLoginUser()` perform the Hadoop login again and yield a user whose credentials are empty --
 * the state the JVM started in -- and the recorded credential is then re-added if there was one.
 * Discarding the login user is narrower than `UserGroupInformation.reset()`, which Spark core also
 * uses for this purpose but which additionally clears the Hadoop configuration, the group mapping
 * and the authentication method that unrelated suites in the same JVM may depend on.
 *
 * The bracket is deliberately the outermost one: because this trait is mixed in last, its snapshot
 * is taken before the suite's own `beforeAll` and its restoration runs after the suite's own
 * `afterAll`, so a context left running until teardown is still inside the bracket.
 */
private[streaming] trait StreamingShuffleHadoopCredentialIsolation extends BeforeAndAfterAll {
  this: Suite =>

  /** What the login user held under Spark's secret key before this suite ran. */
  private var recordedAuthSecret: Option[Array[Byte]] = None

  // Public rather than protected, because `LocalSparkContext` and `SharedSparkContext` -- which
  // several of these suites mix in ahead of this trait -- already widen both hooks to public, and
  // an override may not narrow what it overrides.
  override def beforeAll(): Unit = {
    recordedAuthSecret = StreamingShuffleTestHelper.loginUserAuthSecret()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      StreamingShuffleTestHelper.restoreLoginUserAuthSecret(recordedAuthSecret)
    }
  }
}

/**
 * Shared fixtures, deterministic barriers and fault-injection hooks for the streaming shuffle
 * suites and benchmark.
 */
trait StreamingShuffleTestHelper extends Logging {

  import StreamingShuffleTestHelper._

  // Configuration fixtures.

  /**
   * A `SparkConf` with the three properties of the test envelope that a fixture must state rather
   * than inherit.
   *
   * @return a configuration carrying the envelope properties, ready for a fixture to build on
   */
  def testEnvelopeConf(loadDefaults: Boolean = false): SparkConf = {
    new SparkConf(loadDefaults)
      .set(UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      .set(PortMaxRetriesKey, TestPortMaxRetries.toString)
      .set(UI_ENABLED, false)
  }

  def gatedOffStreamingConf(loadDefaults: Boolean = false): SparkConf = {
    testEnvelopeConf(loadDefaults)
      .set(SHUFFLE_MANAGER, StreamingShuffleManager.SHORT_NAME)
      .set(SHUFFLE_STREAMING_ENABLED, false)
  }

  /** A fully active streaming configuration, including its mandatory transport authentication. */
  def streamingConf(loadDefaults: Boolean = false): SparkConf = {
    gatedOffStreamingConf(loadDefaults)
      .set(SHUFFLE_STREAMING_ENABLED, true)
      .set(NETWORK_AUTH_ENABLED, true)
      .set(AUTH_SECRET, TestAuthenticationSecret)
      .set("spark.app.id", TestApplicationId)
  }

  /**
   * A configuration that selects sort-based shuffle, which is the default and the fallback.
   *
   * This is the baseline every zero-data-loss and every latency comparison is measured against.
   * It carries the same envelope properties as its streaming counterpart -- '''including transport
   * authentication''' -- because a baseline that ran under a different envelope would be comparing
   * two things at once.
   *
   * <b>Why authentication belongs here even though sort-based shuffle does not require it.</b>
   * `StreamingShuffleManager.declineReason` refuses to stream when `spark.authenticate` is false,
   * so the streaming arm has no choice but to enable it. A baseline that left it off would
   * therefore run in a cheaper transport envelope than the arm it is compared against, and the
   * difference would be silently attributed to streaming: measured on this feature's own benchmark
   * master, the cost of authentication was around eight percent on the sort path and around nothing
   * on the streaming path, which biases every reported latency reduction downward. Setting it on
   * both sides removes the confound rather than annotating it, and it makes this method's own
   * contract -- the same envelope as its streaming counterpart -- true rather than aspirational.
   *
   * Setting it changes nothing about shuffle behaviour, which is what "unchanged from stock Spark"
   * refers to: sort-based shuffle serves blocks over an authenticated transport exactly as it
   * serves them over an unauthenticated one, and every output-equality comparison in this package
   * is unaffected. A suite that means to exercise a MISSING secret sets `NETWORK_AUTH_ENABLED` to
   * false explicitly, exactly as the streaming builder documents for its own gate.
   *
   * @param loadDefaults whether to pick up ambient `spark.*` system properties
   * @return a configuration on which shuffle behaviour is unchanged from stock Spark
   */
  def sortBaselineConf(loadDefaults: Boolean = false): SparkConf = {
    testEnvelopeConf(loadDefaults)
      .set(SHUFFLE_MANAGER, "sort")
      .set(NETWORK_AUTH_ENABLED, true)
      .set(AUTH_SECRET, TestAuthenticationSecret)
      .set("spark.app.id", TestApplicationId)
  }

  /** A streaming configuration with every tunable stated explicitly. */
  def streamingConfWithOverrides(
      bufferSizePercent: Int = DefaultBufferSizePercent,
      spillThreshold: Int = DefaultSpillThresholdPercent,
      maxBandwidthMBps: Option[Int] = None,
      debug: Boolean = false,
      enabled: Boolean = true,
      loadDefaults: Boolean = false): SparkConf = {
    val conf = testEnvelopeConf(loadDefaults)
      .set(SHUFFLE_MANAGER, StreamingShuffleManager.SHORT_NAME)
      .set(SHUFFLE_STREAMING_ENABLED, enabled)
      .set(NETWORK_AUTH_ENABLED, true)
      .set(AUTH_SECRET, TestAuthenticationSecret)
      .set("spark.app.id", TestApplicationId)
      .set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, bufferSizePercent)
      .set(SHUFFLE_STREAMING_SPILL_THRESHOLD, spillThreshold)
      .set(SHUFFLE_STREAMING_DEBUG, debug)
    maxBandwidthMBps match {
      case Some(cap) => conf.set(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, cap)
      case None => conf.remove(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)
    }
    conf
  }

  def withLocalMaster(
      conf: SparkConf,
      appName: String = "streaming-shuffle-test",
      master: String = "local[2]"): SparkConf = {
    conf.setAppName(appName).setMaster(master)
  }

  def hashingPartitioner(partitions: Int): Partitioner = {
    require(partitions > 0, s"partitions must be positive but was $partitions")
    new Partitioner() {
      override def numPartitions: Int = partitions
      override def getPartition(key: Any): Int = Utils.nonNegativeMod(key.hashCode, numPartitions)
    }
  }

  def shuffledIntRdd(
      sc: SparkContext,
      conf: SparkConf,
      numPartitions: Int = DefaultPartitionCount,
      numRecords: Int = 10): ShuffledRDD[Int, Int, Int] = {
    require(numRecords > 0, s"numRecords must be positive but was $numRecords")
    val pairs = sc.parallelize(1 to numRecords, 1).map(x => (x, x))
    new ShuffledRDD[Int, Int, Int](pairs, new HashPartitioner(numPartitions))
      .setSerializer(new KryoSerializer(conf))
  }

  /** The shuffle dependency of a freshly built shuffled RDD. */
  def shuffleDependencyFor(
      sc: SparkContext,
      conf: SparkConf,
      numPartitions: Int = DefaultPartitionCount,
      numRecords: Int = 10): ShuffleDependency[Int, Int, Int] = {
    shuffledIntRdd(sc, conf, numPartitions, numRecords)
      .dependencies.head.asInstanceOf[ShuffleDependency[Int, Int, Int]]
  }

  val WorkloadPairs: Seq[(Int, String)] =
    Seq((1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five"))

  def groupByKeyWorkload(
      sc: SparkContext,
      numPartitions: Int = DefaultPartitionCount): RDD[(Int, Iterable[String])] = {
    sc.parallelize(WorkloadPairs, numPartitions).groupByKey(numPartitions)
  }

  /**
   * The grouping workload with a producer crash injected into the job's OWN map side, collected.
   */
  def crashingGroupedOutputAsSet(
      sc: SparkContext,
      numPartitions: Int,
      failureMessage: String): Set[(Int, Seq[String])] = {
    sc.parallelize(WorkloadPairs, numPartitions)
      .mapPartitionsWithIndex { (partitionIndex, records) =>
        records.map { record =>
          if (TaskContext.get().attemptNumber() == 0) {
            throw new IllegalStateException(s"$failureMessage in map partition $partitionIndex")
          }
          record
        }
      }
      .groupByKey(numPartitions)
      .mapValues(values => values.toSeq.sorted)
      .collect()
      .toSet
  }

  def consumerCrashingGroupedOutputAsSet(
      sc: SparkContext,
      numPartitions: Int,
      failureMessage: String): Set[(Int, Seq[String])] = {
    groupByKeyWorkload(sc, numPartitions)
      .mapPartitionsWithIndex { (partitionIndex, groups) =>
        val buffered = groups.buffered
        if (buffered.hasNext && TaskContext.get().attemptNumber() == 0) {
          throw new IllegalStateException(s"$failureMessage in reduce partition $partitionIndex")
        }
        buffered
      }
      .mapValues(values => values.toSeq.sorted)
      .collect()
      .toSet
  }

  /** The same grouping workload, with a mid-write fallback trip wired into its map side. */
  def midWriteTrippingWorkload(
      sc: SparkContext,
      numPartitions: Int,
      holder: MidWriteTripHolder): RDD[(Int, Iterable[String])] = {
    val pairs = Seq((1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five"))
    sc.parallelize(pairs, numPartitions)
      .mapPartitions { records =>
        records.zipWithIndex.map { case (record, index) =>
          if (index == 0) {
            holder.trip()
          }
          record
        }
      }
      .groupByKey(numPartitions)
  }

  def largeDataset(
      sc: SparkContext,
      numPartitions: Int = DefaultPartitionCount,
      totalBytes: Long = TargetDatasetBytes): RDD[(Int, String)] = {
    require(numPartitions > 0, s"numPartitions must be positive but was $numPartitions")
    require(totalBytes > 0L, s"totalBytes must be positive but was $totalBytes")
    val recordsPerPartition =
      math.max(1, (totalBytes / numPartitions.toLong / RecordValueLength.toLong).toInt)
    sc.parallelize(0 until numPartitions, numPartitions).flatMap { partitionIndex =>
      (0 until recordsPerPartition).iterator.map { recordIndex =>
        val key = partitionIndex * recordsPerPartition + recordIndex
        (key, deterministicValue(partitionIndex, recordIndex))
      }
    }
  }

  /** The deterministic large dataset after a raw hash shuffle, with no grouping materialization. */
  def rawShuffleWorkload(
      sc: SparkContext,
      numPartitions: Int = DefaultPartitionCount,
      totalBytes: Long = TargetDatasetBytes): RDD[(Int, String)] = {
    largeDataset(sc, numPartitions, totalBytes)
      .partitionBy(new HashPartitioner(numPartitions))
  }

  def rawOutputDigest(shuffled: RDD[(Int, String)]): Set[(Int, Int, Int)] =
    shuffled.map(rawRecordDigest).collect().toSet

  def sortBaselineRawOutput(
      numPartitions: Int,
      totalBytes: Long,
      master: String = "local[2]"): Set[(Int, Int, Int)] = {
    val conf = withLocalMaster(
      sortBaselineConf(), "streaming-shuffle-raw-sort-baseline", master)
    val baselineContext = new SparkContext(conf)
    try {
      rawOutputDigest(rawShuffleWorkload(baselineContext, numPartitions, totalBytes))
    } finally {
      baselineContext.stop()
    }
  }

  def recordsPerPartitionFor(
      numPartitions: Int = DefaultPartitionCount,
      totalBytes: Long = TargetDatasetBytes): Int = {
    require(numPartitions > 0, s"numPartitions must be positive but was $numPartitions")
    require(totalBytes > 0L, s"totalBytes must be positive but was $totalBytes")
    math.max(1, (totalBytes / numPartitions.toLong / RecordValueLength.toLong).toInt)
  }

  def deterministicRecords(
      numRecords: Int,
      seed: Long = 0L,
      keySpace: Int = Int.MaxValue): Seq[(Int, Int)] = {
    require(numRecords >= 0, s"numRecords must be non-negative but was $numRecords")
    require(keySpace > 0, s"keySpace must be positive but was $keySpace")
    val random = new Random(seed)
    val records = new mutable.ArrayBuffer[(Int, Int)](numRecords)
    var produced = 0
    while (produced < numRecords) {
      records += ((random.nextInt(keySpace), random.nextInt()))
      produced += 1
    }
    records.toSeq
  }

  def payloadOfLength(seed: Long, length: Int): Array[Byte] = deterministicPayload(seed, length)

  def maximumSizedPayload(seed: Long = 0L): Array[Byte] =
    deterministicPayload(seed, MaxBlockSizeBytes)

  def oversizedPayload(seed: Long = 0L): Array[Byte] =
    deterministicPayload(seed, MaxBlockSizeBytes + 1)

  // Deterministic barriers.

  def newLatch(count: Int): CountDownLatch = {
    require(count >= 0, s"count must be non-negative but was $count")
    new CountDownLatch(count)
  }

  /**
   * A barrier that releases when the given number of parties have arrived.
   *
   * @param parties participants that must arrive
   * @return the barrier
   */
  def newBarrier(parties: Int): CyclicBarrier = {
    require(parties > 0, s"parties must be positive but was $parties")
    new CyclicBarrier(parties)
  }

  /** A semaphore with the given number of permits, for pacing a simulated slow consumer. */
  def newPermits(permits: Int): Semaphore = {
    require(permits >= 0, s"permits must be non-negative but was $permits")
    new Semaphore(permits)
  }

  def awaitLatch(
      latch: CountDownLatch,
      description: String = "latch",
      timeoutMillis: Long = DefaultAwaitTimeoutMillis): Unit = {
    val opened = latch.await(timeoutMillis, TimeUnit.MILLISECONDS)
    assert(opened,
      s"Timed out after $timeoutMillis ms waiting for $description; " +
        s"${latch.getCount} countdown(s) never arrived")
  }

  def awaitBarrier(
      barrier: CyclicBarrier,
      description: String = "barrier",
      timeoutMillis: Long = DefaultAwaitTimeoutMillis): Int = {
    try {
      barrier.await(timeoutMillis, TimeUnit.MILLISECONDS)
    } catch {
      case failure: Exception =>
        throw new IllegalStateException(
          s"Timed out after $timeoutMillis ms waiting at $description with " +
            s"${barrier.getNumberWaiting} of ${barrier.getParties} party(s) present", failure)
    }
  }

  def acquirePermit(
      semaphore: Semaphore,
      description: String = "permit",
      timeoutMillis: Long = DefaultAwaitTimeoutMillis): Unit = {
    val acquired = semaphore.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS)
    assert(acquired, s"Timed out after $timeoutMillis ms acquiring $description")
  }

  def awaitJavaFuture[T](future: JFuture[T], timeoutMillis: Long = DefaultAwaitTimeoutMillis): T = {
    ThreadUtils.awaitResult(future, Duration(timeoutMillis, TimeUnit.MILLISECONDS))
  }

  def awaitValue[T](
      awaitable: Awaitable[T],
      timeoutMillis: Long = DefaultAwaitTimeoutMillis): T = {
    ThreadUtils.awaitResult(awaitable, Duration(timeoutMillis, TimeUnit.MILLISECONDS))
  }

  def awaitCompletion[T](
      awaitable: Awaitable[T],
      timeoutMillis: Long = DefaultAwaitTimeoutMillis): awaitable.type = {
    ThreadUtils.awaitReady(awaitable, Duration(timeoutMillis, TimeUnit.MILLISECONDS))
  }

  /**
   * Runs a body under a wall-clock progress watchdog, and fails the case if it stopped progressing.
   *
   * ==Why the project's own safety net is not enough here==
   *
   * `SparkFunSuite` wraps every test body in `failAfter(Span(20, Minutes))`, and ScalaTest enforces
   * that span with a `Signaler` that interrupts the running thread. Interruption is a request, and
   * a thread executing a tight computational loop with no blocking call never observes it -- so a
   * case that stops making progress inside such a loop is not bounded by that net at all. It
   * consumes a core until the process is killed, and because `failAfter` never returns, the suite
   * reports nothing: no failure, no name, no stack. On a Maven run there is no forked-process
   * timeout either, so one occurrence spends the entire job budget and produces no result for the
   * whole module.
   *
   * ==What this adds, stated precisely==
   *
   * It does not stop such a loop. Nothing inside the JVM can: `Thread.stop` is gone, and a body
   * moved onto another thread would leave that thread spinning just the same -- and, for these
   * fixtures, would move `TaskContext`, which is thread confined, out from under the code being
   * exercised. What it adds is a diagnosis. A daemon watchdog samples a caller-supplied progress
   * reading, and on a stall it records the stalled thread's own stack trace and the reading that
   * stopped moving, at warning level, once per stall. So a regression of this shape names itself in
   * the log while it is happening instead of appearing as a run that mysteriously never finished.
   *
   * It also closes the weaker half of the same failure: a body that still terminates but has become
   * pathologically slow is caught outright, because the elapsed wall time is asserted against
   * `budgetMillis` once the body returns.
   *
   * Wall time is read here deliberately, and it is the one thing in this helper that is allowed to.
   * The package's discipline is that no *behaviour* depends on elapsed real time -- every window a
   * component enforces advances through an injected [[ManualClock]] -- and this watchdog asserts on
   * no behaviour whatever. It measures only whether the case is still moving, which is a property
   * of the test process rather than of the subsystem, and cannot be expressed on an injected clock
   * precisely because a stalled body is what stops advancing that clock.
   *
   * @param description what the body is doing, used in the diagnosis and in the failure message
   * @param budgetMillis generous upper bound on the body's own wall-clock duration; exceeding it
   *                     fails the case
   * @param stallMillis how long the progress reading may stand still before a diagnosis is recorded
   * @param progress a cheap, monotonically non-decreasing reading of how far the body has got, for
   *                 example a record or block counter; a reading that stops moving is the stall
   * @param body the work to run, on the caller's own thread
   * @return whatever the body returned
   */
  def withProgressWatchdog[T](
      description: String,
      budgetMillis: Long = DefaultProgressBudgetMillis,
      stallMillis: Long = DefaultProgressStallMillis)(
      progress: => Long)(
      body: => T): T = {
    require(budgetMillis > 0L, s"budgetMillis must be positive but was $budgetMillis")
    require(stallMillis > 0L, s"stallMillis must be positive but was $stallMillis")
    val watched = Thread.currentThread()
    val finished = new CountDownLatch(1)
    val stalls = new AtomicInteger(0)
    val watchdog = new Thread(
      () => {
        var lastReading = Long.MinValue
        var lastMovedAtMillis = System.currentTimeMillis()
        while (!finished.await(ProgressPollIntervalMillis, TimeUnit.MILLISECONDS)) {
          val reading = progress
          val nowMillis = System.currentTimeMillis()
          if (reading != lastReading) {
            lastReading = reading
            lastMovedAtMillis = nowMillis
          } else if (nowMillis - lastMovedAtMillis >= stallMillis) {
            lastMovedAtMillis = nowMillis
            stalls.incrementAndGet()
            // Deliberately assembled here and logged in one record: a stalled body cannot report
            // itself, and the stack of the thread that is not moving is the whole diagnosis.
            val frames = watched.getStackTrace.take(ProgressStackFrames)
              .map(frame => s"    at $frame").mkString("\n")
            logWarning(s"$description has made no progress for at least $stallMillis ms, with " +
              s"its progress reading standing at $reading on thread '${watched.getName}' in " +
              s"state ${watched.getState}. A thread in a computational loop ignores the " +
              s"interruption ScalaTest's failAfter relies on, so this record is the " +
              s"diagnosis:\n$frames")
          }
        }
      },
      s"streaming-shuffle-progress-watchdog-${description.replaceAll("[^A-Za-z0-9]+", "-")}")
    watchdog.setDaemon(true)
    val startedAtMillis = System.currentTimeMillis()
    watchdog.start()
    val result = try {
      body
    } finally {
      finished.countDown()
      watchdog.join(ProgressWatchdogJoinMillis)
    }
    val elapsedMillis = System.currentTimeMillis() - startedAtMillis
    assert(elapsedMillis <= budgetMillis,
      s"$description took $elapsedMillis ms against a budget of $budgetMillis ms, having stalled " +
        s"${stalls.get} time(s); a case of this shape that becomes this slow is the terminating " +
        "half of a defect whose other half never terminates at all, so the budget is asserted " +
        "rather than reported")
    result
  }

  /**
   * Runs a body on the given number of threads, all released from one barrier, and returns the
   * results in thread-index order.
   *
   * @param prefix thread-name prefix, which is what makes a stack dump readable
   * @param timeoutMillis bound on both the barrier and the collection of results
   * @param body work to run, given its own zero-based thread index
   * @return one result per thread, in thread-index order
   */
  def runConcurrently[T](
      numThreads: Int,
      prefix: String = "streaming-shuffle-test",
      timeoutMillis: Long = DefaultAwaitTimeoutMillis)(body: Int => T): Seq[T] = {
    require(numThreads > 0, s"numThreads must be positive but was $numThreads")
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, prefix)
    try {
      val startLine = newBarrier(numThreads)
      val futures = (0 until numThreads).map { index =>
        pool.submit(new Callable[T] {
          override def call(): T = {
            awaitBarrier(startLine, s"$prefix start line", timeoutMillis)
            body(index)
          }
        })
      }
      futures.map(future => awaitJavaFuture(future, timeoutMillis))
    } finally {
      pool.shutdownNow()
    }
  }

  /**
   * Runs one threaded scenario to a bounded end, and completes the task contexts it was built on
   * only once every thread it started has actually stopped.
   *
   * @param threads the threads the body starts, whose settlement this method owns
   * @param taskContexts contexts those threads hold reservations against, completed only once
   *     they have stopped
   * @param joinTimeoutMillis budget for the ordinary join, before anything is forced
   * @param releaseWaits releases whatever the threads may be parked on; invoked only on the
   *     timeout path, so a scenario that finished normally pays nothing for it
   */
  def runBoundedThreadedScenario[T](
      threads: Seq[Thread],
      taskContexts: Seq[TaskContextImpl],
      description: String,
      joinTimeoutMillis: Long = DefaultAwaitTimeoutMillis,
      settlementGraceMillis: Long = ThreadSettlementGraceMillis,
      releaseWaits: () => Unit = () => ())(body: => T): T = {
    require(threads.nonEmpty, s"$description must run at least one thread")
    require(joinTimeoutMillis > 0L,
      s"$description needs a positive join budget but was given $joinTimeoutMillis ms")
    require(settlementGraceMillis > 0L,
      s"$description needs a positive settlement grace but was given $settlementGraceMillis ms")
    val outcome = Try(body)
    val settlement =
      Try(settleThreads(threads, joinTimeoutMillis, settlementGraceMillis, releaseWaits))
    val completion = Try(taskContexts.foreach(_.markTaskCompleted(None)))
    val survival = Try(requireThreadsStopped(settlement.getOrElse(Seq.empty), description))
    rethrowFirstFailure(Seq(outcome, settlement, completion, survival))
    outcome.get
  }

  private def settleThreads(
      threads: Seq[Thread],
      joinTimeoutMillis: Long,
      graceMillis: Long,
      releaseWaits: () => Unit): Seq[Thread] = {
    threads.foreach(thread => joinWithin(thread, joinTimeoutMillis))
    if (threads.exists(_.isAlive)) {
      releaseWaits()
      threads.filter(_.isAlive).foreach(thread => joinWithin(thread, graceMillis))
      val stubborn = threads.filter(_.isAlive)
      stubborn.foreach(_.interrupt())
      stubborn.foreach(thread => joinWithin(thread, graceMillis))
    }
    threads.filter(_.isAlive)
  }

  private def joinWithin(thread: Thread, timeoutMillis: Long): Unit = {
    try {
      thread.join(timeoutMillis)
    } catch {
      case _: InterruptedException => Thread.currentThread().interrupt()
    }
  }

  private def requireThreadsStopped(survivors: Seq[Thread], description: String): Unit = {
    if (survivors.nonEmpty) {
      val diagnosis = survivors.map { thread =>
        val frames = thread.getStackTrace.take(StuckThreadStackFrames)
          .map(frame => s"      at $frame")
          .mkString("\n")
        s"  ${thread.getName} (${thread.getState})\n$frames"
      }.mkString("\n")
      throw new IllegalStateException(
        s"$description left ${survivors.size} thread(s) running after its join budget, a " +
          s"released wait and an interrupt, so its task contexts were completed under live " +
          s"thread(s):\n" +
          diagnosis)
    }
  }

  /**
   * Rethrows the first failure among the given attempts, attaching the rest to it as suppressed.
   *
   * @param attempts the attempts, in the order their failures are preferred
   */
  private def rethrowFirstFailure(attempts: Seq[Try[Any]]): Unit = {
    val failures = attempts.collect { case Failure(failure) => failure }
    failures.headOption.foreach { primary =>
      failures.tail.filterNot(_ eq primary).foreach(primary.addSuppressed)
      throw primary
    }
  }

  // Injected clocks.

  /**
   * A manual clock starting at a deliberately non-zero epoch.
   *
   * @param initialTimeMillis time the clock begins at
   * @return the clock
   */
  def newManualClock(initialTimeMillis: Long = ManualClockEpochMillis): ManualClock =
    new ManualClock(initialTimeMillis)

  /**
   * Runs a body with one directory made read-only, restoring it whatever the body does.
   *
   * @param body the assertions to run while writes into the fixture's directory fail
   */
  def withFailingDiskWrites[T](injector: StreamingShuffleFaultInjector)(
      body: ScopedDiskFault => T): T = {
    val fault = injector.failDiskWrites()
    try {
      body(fault)
    } finally {
      fault.close()
    }
  }

  /**
   * A real system clock, for the rare fixture that genuinely needs wall time.
   *
   * @return a system clock
   */
  def wallClock(): Clock = new SystemClock

  def advanceOneSpillPoll(clock: ManualClock): Unit = clock.advance(SpillPollIntervalMillis)

  def advanceReclamationDeadline(clock: ManualClock): Unit =
    clock.advance(ReclamationDeadlineMillis)

  def advancePastProducerTimeout(clock: ManualClock): Unit =
    clock.advance(ProducerConnectionTimeoutMillis)

  def advanceJustBeforeProducerTimeout(clock: ManualClock): Unit =
    clock.advance(JustBeforeProducerTimeoutMillis)

  def advancePastConsumerLivenessTimeout(clock: ManualClock): Unit =
    clock.advance(ConsumerLivenessTimeoutMillis)

  def advanceJustBeforeConsumerLivenessTimeout(clock: ManualClock): Unit =
    clock.advance(JustBeforeConsumerLivenessMillis)

  def advancePastSustainedSlownessWindow(clock: ManualClock): Unit =
    clock.advance(SustainedSlownessTripMillis)

  def advanceJustBeforeSustainedSlownessWindow(clock: ManualClock): Unit =
    clock.advance(JustBeforeSustainedSlownessMillis)

  /**
   * Walks the clock through the whole retransmission backoff ladder, one delay per attempt.
   *
   * @param clock clock to advance
   * @return the cumulative clock reading after each delay, so a suite can assert on the
   *     sequence of instants at which the attempts become due rather than on the deltas alone
   */
  def advanceThroughRetryBackoff(clock: ManualClock): Seq[Long] = {
    RetryBackoffLadderMillis.map { delay =>
      clock.advance(delay)
      clock.getTimeMillis()
    }
  }

  /**
   * Blocks until the clock reaches the target reading, which another thread must bring about.
   *
   * @param clock clock to observe
   */
  def awaitClockReading(clock: ManualClock, targetTimeMillis: Long): Long =
    clock.waitTillTime(targetTimeMillis)

  // Task context and memory fixtures.

  def fakeTaskContext(sc: SparkContext): TaskContext = newTaskContext(sc.env)

  def newTaskContext(
      env: SparkEnv,
      stageId: Int = 0,
      stageAttemptNumber: Int = 0,
      partitionId: Int = 0,
      taskAttemptId: Long = 0L,
      attemptNumber: Int = 0,
      numPartitions: Int = 1): TaskContextImpl = {
    new TaskContextImpl(
      stageId = stageId,
      stageAttemptNumber = stageAttemptNumber,
      partitionId = partitionId,
      taskAttemptId = taskAttemptId,
      attemptNumber = attemptNumber,
      numPartitions = numPartitions,
      taskMemoryManager = newTaskMemoryManager(env, taskAttemptId),
      localProperties = new Properties,
      metricsSystem = env.metricsSystem)
  }

  /** A real task memory manager backed by the environment's memory manager. */
  def newTaskMemoryManager(env: SparkEnv, taskAttemptId: Long = 0L): TaskMemoryManager =
    new TaskMemoryManager(env.memoryManager, fixtureAccountingAttemptId(taskAttemptId))

  /**
   * The execution-memory accounting identity for a fixture whose logical attempt id is given.
   *
   * @param taskAttemptId logical attempt id a fixture was built with
   */
  def fixtureAccountingAttemptId(taskAttemptId: Long): Long = {
    val offset = math.max(0L, taskAttemptId)
    FixtureAttemptIdBase + (offset % FixtureAttemptIdBase)
  }

  // Wire protocol fixtures.

  def recordingBuffer(payload: Array[Byte]): RecordingStreamingManagedBuffer =
    new RecordingStreamingManagedBuffer(new NioManagedBuffer(ByteBuffer.wrap(payload)))

  def dataBlock(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      sequenceNumber: Long,
      payload: Array[Byte]): DataBlockMessage = {
    val checksum =
      StreamingShuffleChecksum.computeBlock(shuffleId, mapId, partitionId, sequenceNumber, payload)
    new DataBlockMessage(shuffleId, mapId, partitionId, sequenceNumber, checksum, payload)
  }

  def corruptedDataBlock(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      sequenceNumber: Long,
      payload: Array[Byte]): DataBlockMessage = {
    val correct =
      StreamingShuffleChecksum.computeBlock(shuffleId, mapId, partitionId, sequenceNumber, payload)
    new DataBlockMessage(shuffleId, mapId, partitionId, sequenceNumber, correct ^ 1L, payload)
  }

  def ack(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      consumerPosition: Long): AckMessage =
    new AckMessage(shuffleId, mapId, partitionId, consumerPosition)

  /**
   * A liveness signal reporting the next position its sender expects to handle.
   *
   * @param consumerPosition highest sequence the consumer has consumed, or the nothing-consumed
   *     sentinel before it has consumed anything
   */
  def heartbeat(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      consumerPosition: Long,
      consumerToken: Long = NoConsumerToken): HeartbeatMessage =
    new HeartbeatMessage(shuffleId, mapId, partitionId, consumerPosition, consumerToken)

  def retransmitRequest(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      sequenceNumber: Long,
      lastSequenceNumber: Long = -1L): RetransmitRequestMessage =
    new RetransmitRequestMessage(shuffleId, mapId, partitionId, sequenceNumber,
      if (lastSequenceNumber < sequenceNumber) sequenceNumber else lastSequenceNumber)

  def streamTermination(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      totalBlocks: Long): StreamTerminationMessage = {
    require(totalBlocks >= 0L, s"totalBlocks must be non-negative but was $totalBlocks")
    new StreamTerminationMessage(shuffleId, mapId, partitionId, totalBlocks)
  }

  def blockChecksum(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      sequenceNumber: Long,
      payload: Array[Byte]): Long =
    StreamingShuffleChecksum.computeBlock(shuffleId, mapId, partitionId, sequenceNumber, payload)

  def typeOf(message: StreamingShuffleMessage): StreamingShuffleMessageType = messageTypeOf(message)

  def resetStreamingShuffleMetrics(): Unit = StreamingShuffleMetricsSource.reset()

  /** Installs a contributor that pins the buffer utilisation gauge to a chosen percentage. */
  def installBufferUtilization(
      bufferedBytes: Long,
      budgetBytes: Long): FixedBufferUtilizationContributor = {
    val contributor = new FixedBufferUtilizationContributor(bufferedBytes, budgetBytes)
    StreamingShuffleMetricsSource.registerBufferUtilizationContributor(contributor)
    contributor
  }

  def removeBufferUtilization(contributor: FixedBufferUtilizationContributor): Unit =
    StreamingShuffleMetricsSource.unregisterBufferUtilizationContributor(contributor)

  def streamingShuffleSources(metricsSystem: MetricsSystem): Seq[Source] =
    metricsSystem.getSourcesByName(MetricsSourceName)

  def streamingShuffleMetricNames(): Set[String] =
    StreamingShuffleMetricsSource.metricRegistry.getNames.asScala.toSet

  def streamingShuffleCounter(name: String): Counter = {
    val counters = StreamingShuffleMetricsSource.metricRegistry.getCounters
    val counter = counters.get(name)
    assert(counter != null,
      s"No streaming shuffle counter named '$name'; registry holds " +
        s"${counters.keySet.asScala.toSeq.sorted.mkString(", ")}")
    counter
  }

  /** The buffer utilisation gauge from the streaming shuffle registry. */
  def bufferUtilizationGauge(): Gauge[Long] = {
    val gauges = StreamingShuffleMetricsSource.metricRegistry.getGauges
    val gauge = gauges.get(BufferUtilizationMetricName)
    assert(gauge != null,
      s"No streaming shuffle gauge named '$BufferUtilizationMetricName'; registry holds " +
        s"${gauges.keySet.asScala.toSeq.sorted.mkString(", ")}")
    gauge.asInstanceOf[Gauge[Long]]
  }

  def observedSpillCount(): Long = streamingShuffleCounter(SpillCountMetricName).getCount

  def observedBackpressureEvents(): Long =
    streamingShuffleCounter(BackpressureEventsMetricName).getCount

  def observedPartialReadInvalidations(): Long =
    streamingShuffleCounter(PartialReadInvalidationsMetricName).getCount

  def observedBufferUtilizationPercent(): Long = bufferUtilizationGauge().getValue

  def newFaultInjector(clock: ManualClock): StreamingShuffleFaultInjector =
    new StreamingShuffleFaultInjector(clock)

  /**
   * Chooses which of a run's tasks should be made to fail, deterministically.
   *
   * @return the zero-based indices of the tasks to fail, in ascending order
   */
  def selectFailingTasks(
      numTasks: Int,
      failurePercent: Int = StressFailureInjectionPercent,
      seed: Long = 0L): Set[Int] = {
    require(numTasks >= 0, s"numTasks must be non-negative but was $numTasks")
    require(failurePercent >= 0 && failurePercent <= 100,
      s"failurePercent must be in [0, 100] but was $failurePercent")
    val target = numTasks.toLong * failurePercent.toLong / 100L
    val random = new Random(seed)
    val selected = mutable.SortedSet.empty[Int]
    var guard = 0
    val guardLimit = math.max(1, numTasks) * 100
    while (selected.size < target && guard < guardLimit) {
      selected += random.nextInt(math.max(1, numTasks))
      guard += 1
    }
    selected.toSet
  }

  // Sort-based baseline and data-loss assertions.

  def groupedOutputAsSet(
      sc: SparkContext,
      numPartitions: Int = DefaultPartitionCount): Set[(Int, Seq[String])] = {
    groupByKeyWorkload(sc, numPartitions)
      .mapValues(values => values.toSeq.sorted)
      .collect()
      .toSet
  }

  def sortBaselineGroupedOutput(
      numPartitions: Int = DefaultPartitionCount,
      master: String = "local[2]"): Set[(Int, Seq[String])] = {
    val conf = withLocalMaster(sortBaselineConf(), "streaming-shuffle-sort-baseline", master)
    val baselineContext = new SparkContext(conf)
    try {
      groupedOutputAsSet(baselineContext, numPartitions)
    } finally {
      baselineContext.stop()
    }
  }

  def assertNoDataLoss[T](observed: Set[T], baseline: Set[T], description: String): Unit = {
    val missing = baseline.diff(observed)
    val unexpected = observed.diff(baseline)
    assert(missing.isEmpty && unexpected.isEmpty,
      s"Output of $description does not match the sort-based baseline: " +
        s"${missing.size} record(s) missing and ${unexpected.size} unexpected; " +
        s"missing sample ${missing.take(3).mkString("[", ", ", "]")}, " +
        s"unexpected sample ${unexpected.take(3).mkString("[", ", ", "]")}")
  }

  /**
   * Asserts that every buffer was taken exactly the expected number of times and dropped exactly as
   * often as it was taken.
   */
  def assertBuffersReleasedExactlyOnce(
      buffers: Seq[RecordingStreamingManagedBuffer],
      expectedTakesPerBuffer: Int = 1): Unit = {
    require(expectedTakesPerBuffer >= 1,
      s"A documented lifecycle takes at least one reference, but $expectedTakesPerBuffer was " +
        "given.")
    val untouched = buffers.filter(buffer => buffer.callsToRetain == 0)
    assert(untouched.isEmpty,
      s"${untouched.size} of ${buffers.size} managed buffer(s) were never retained, so their " +
        "release accounting proves nothing")
    val overRetained = buffers.filter(buffer => buffer.callsToRetain != expectedTakesPerBuffer)
    assert(overRetained.isEmpty,
      s"${overRetained.size} of ${buffers.size} managed buffer(s) were retained a number of " +
        s"times other than the documented $expectedTakesPerBuffer; retain counts " +
        s"${overRetained.map(_.callsToRetain).mkString(", ")}. An extra reference pins a frame " +
        "for longer than its records are needed, which a balanced count alone would hide")
    val unbalanced = buffers.filter(buffer => buffer.outstandingReferences != 0)
    assert(unbalanced.isEmpty,
      s"${unbalanced.size} of ${buffers.size} managed buffer(s) were not released exactly as " +
        s"often as they were retained; outstanding reference counts " +
        s"${unbalanced.map(_.outstandingReferences).mkString(", ")} (positive is a leak, " +
        "negative is an over release)")
  }

  def assertSingleThreadedReporting(foreignThreadCallCount: Long, description: String): Unit = {
    assert(foreignThreadCallCount == 0L,
      s"$description was called from more than one thread: $foreignThreadCallCount call(s) " +
        "arrived on a thread other than the first one seen, which breaks the reporter's " +
        "single-threaded contract")
  }

  def assertNoPublishedFailure(
      notifier: StreamingShuffleErrorNotifier,
      description: String): Unit = {
    assert(!notifier.hasError,
      s"$description published a failure that was not expected: " +
        s"${notifier.error.map(_.toString).getOrElse("<none>")}")
  }

  def assertPublishedFailure[T <: Throwable](
      notifier: StreamingShuffleErrorNotifier,
      description: String)(implicit tag: scala.reflect.ClassTag[T]): Unit = {
    assert(notifier.hasError, s"$description published no failure at all")
    val published = notifier.error.get
    assert(tag.runtimeClass.isInstance(published),
      s"$description published a ${published.getClass.getName} but " +
        s"${tag.runtimeClass.getName} was expected: ${published.getMessage}")
  }

  /**
   * Runs `body` with every log record this subsystem emits captured, and returns them alongside its
   * result.
   */
  def capturingStreamingLogs[T](body: => T): (T, Seq[CapturedLogRecord]) = {
    val appender =
      new StreamingShuffleLogCaptureAppender(StreamingShuffleLogCapture.LOGGER_NAME_PREFIX)
    val root = LogManager.getRootLogger.asInstanceOf[Log4jLogger]
    appender.start()
    root.addAppender(appender)
    try {
      val result = body
      (result, appender.captured)
    } finally {
      root.removeAppender(appender)
      appender.stop()
    }
  }

  /**
   * Asserts that exactly one component owns the default-level record of one condition.
   *
   * @param silent simple class names of the components that must say nothing at default level
   */
  def assertSingleDefaultLevelOwner(
      records: Seq[CapturedLogRecord],
      owner: String,
      silent: Seq[String],
      what: String): Unit = {
    val atDefaultLevel = records.filter(_.isDefaultLevel)
    val fromOwner = atDefaultLevel.filter(_.loggerName.endsWith(owner))
    assert(fromOwner.size == 1,
      s"$what must be reported once at default level by $owner, but it was reported " +
        s"${fromOwner.size} time(s): ${fromOwner.map(_.message).mkString(" | ")}")
    silent.foreach { component =>
      val fromComponent = atDefaultLevel.filter(_.loggerName.endsWith(component))
      assert(fromComponent.isEmpty,
        s"$component must not report $what at default level -- $owner owns that record -- but it " +
          s"emitted ${fromComponent.size}: ${fromComponent.map(_.message).mkString(" | ")}")
    }
  }
}

private[streaming] object StreamingShuffleLogCapture {
  val LOGGER_NAME_PREFIX: String = "org.apache.spark.shuffle.streaming."
}

/** One log record a case captured, reduced to the three properties an ownership assertion needs. */
private[streaming] case class CapturedLogRecord(
    loggerName: String,
    level: String,
    message: String) {

  def isDefaultLevel: Boolean =
    level == "INFO" || level == "WARN" || level == "ERROR" || level == "FATAL"
}

private[streaming] class StreamingShuffleLogCaptureAppender(loggerNamePrefix: String)
  extends AbstractAppender("streamingShuffleLogCapture", null, null, true, Property.EMPTY_ARRAY) {

  private val records = mutable.ArrayBuffer.empty[CapturedLogRecord]

  override def append(event: LogEvent): Unit = {
    val loggerName = Option(event.getLoggerName).getOrElse("")
    if (loggerName.startsWith(loggerNamePrefix)) {
      val message = Option(event.getMessage).map(_.getFormattedMessage).getOrElse("")
      val level = Option(event.getLevel).map(_.name()).getOrElse("")
      synchronized {
        records += CapturedLogRecord(loggerName, level, message)
      }
    }
  }

  def captured: Seq[CapturedLogRecord] = synchronized(records.toSeq)
}
