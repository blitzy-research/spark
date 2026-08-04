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
import java.util.Properties
import java.util.concurrent.{Callable, CountDownLatch, CyclicBarrier, Semaphore, TimeUnit}
import java.util.concurrent.{Future => JFuture}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable
import scala.concurrent.Awaitable
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.Random

import com.codahale.metrics.{Counter, Gauge}
import org.scalatest.Tag

import org.apache.spark.{HashPartitioner, Partitioner, ShuffleDependency, SparkConf, SparkContext, SparkEnv, TaskContext, TaskContextImpl}
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_DEBUG, SHUFFLE_STREAMING_ENABLED, SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, SHUFFLE_STREAMING_SPILL_THRESHOLD, UNSAFE_EXCEPTION_ON_MEMORY_LEAK}
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, DataBlockMessage, HeartbeatMessage, RetransmitRequestMessage, StreamingShuffleChecksum, StreamingShuffleMessage, StreamingShuffleMessageType, StreamTerminationMessage}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle.{ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter}
import org.apache.spark.util.{Clock, ManualClock, SystemClock, ThreadUtils, Utils}

/**
 * ScalaTest tag carried by the five-minute streaming shuffle stress workload.
 *
 * Declared here rather than in the shared tags module so the feature stays additive: the exclusion
 * wiring already exists, and sbt's `-Dtest.exclude.tags` and Maven's excluded-groups settings both
 * accept this tag's name. It is not a member of the build's default exclusion list, so a full run
 * includes the workload and only a run naming this tag leaves it out. The tag string is this
 * object's fully qualified name, which is the convention ScalaTest expects.
 */
object StreamingShuffleStressTest
  extends Tag("org.apache.spark.shuffle.streaming.StreamingShuffleStressTest")

/**
 * A `ShuffleWriteMetricsReporter` that accumulates every reported figure so a test can assert on
 * what the streaming write path actually reported.
 *
 * All five members of the reporter are implemented, each re-declaring the `private[spark]` modifier
 * the trait carries on the method itself, because a partial implementation would not compile.
 *
 * The contract promises single-threaded use, so an implementation need not synchronize. The
 * streaming write path has Netty event-loop threads alongside the task thread, so this class makes
 * adherence observable rather than assuming it: the first caller becomes the owner, and any later
 * call from another thread increments a foreign-thread counter a test can assert is zero. Counters
 * are `AtomicLong` so such a call is reported rather than lost to the race that hid it.
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

  /**
   * Number of calls that arrived on a thread other than the first one seen.
   *
   * A streaming shuffle that honours the reporter's single-threaded contract leaves this at zero.
   * A non-zero value means an I/O thread reported directly instead of enqueueing for the task
   * thread, which is exactly the defect this fixture exists to surface.
   */
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
 *
 * All seventeen members of the reporter are implemented, each re-declaring the `private[spark]`
 * modifier the trait carries on the method itself, because a partial implementation would not
 * compile. The thread-observation machinery mirrors [[RecordingStreamingShuffleWriteMetrics]] for
 * the same reason: the reader consumes from an asynchronous channel, and a fixture that tolerated a
 * report from a Netty thread would hide the defect worth catching.
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

  /**
   * Sum of every merge-related counter.
   *
   * Streaming shuffle declines to participate in push-based merge, so a suite asserts this is zero
   * with one call rather than seven, which is both shorter to write and impossible to under-cover.
   */
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


/**
 * Wrapper for a managed buffer that counts retain and release calls.
 *
 * Defined here rather than taken from a mocking library because `NioManagedBuffer` is final and a
 * final class cannot be spied on, which is the same reason the sort-path reader suite in the parent
 * package defines its own equivalent.
 *
 * The streaming reader copies a decoded payload and retains no transport buffer, so what these
 * counts audit is that nothing takes a reference without dropping it: [[releasedExactlyOnce]]
 * states the balanced case directly, and [[outstandingReferences]] reports the imbalance when it
 * does not hold, which turns a stray reference into a diagnosable number instead of a hung task.
 *
 * ==Why the counters are atomic==
 *
 * A frame is accepted on a Netty event-loop thread and any reference it takes is dropped by
 * whichever thread surfaces the records -- ordinarily the task thread -- so both counters are
 * written from more than one thread and read from a third. Plain `var`s would make an increment a
 * read-modify-write with no memory barrier: a lost increment reads as a leak that never happened,
 * and a stale read reads as a release that never happened, and in both directions the failure would
 * be an intermittent one in a suite whose whole premise is determinism. `AtomicInteger` removes the
 * possibility rather than making it unlikely.
 */
class RecordingStreamingManagedBuffer(underlyingBuffer: NioManagedBuffer) extends ManagedBuffer {

  private val retainCalls = new AtomicInteger(0)
  private val releaseCalls = new AtomicInteger(0)

  /** How many times anything took a reference on this buffer. */
  def callsToRetain: Int = retainCalls.get()

  /** How many times anything dropped a reference on this buffer. */
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

  /** True when the buffer was taken at least once and each take was matched by one drop. */
  def releasedExactlyOnce: Boolean = {
    // Read once each, in this order, so the verdict describes one observation rather than two: a
    // retain that lands between the two reads can only make the pair look unbalanced, never
    // balanced, so this cannot report success for a buffer that is in fact leaking.
    val retained = retainCalls.get()
    val released = releaseCalls.get()
    retained > 0 && retained == released
  }

  /**
   * Retains still outstanding. Positive means a leak, negative means an over-release; either is a
   * defect, and reporting the signed difference says which one happened.
   */
  def outstandingReferences: Int = retainCalls.get() - releaseCalls.get()
}

/**
 * A buffer-utilisation contributor whose reported numerator and denominator are fixed by the test.
 *
 * `StreamingShuffleMetricsSource` computes `bufferUtilizationPercent` on read from its registry of
 * contributors and deliberately exposes no setter, so registering an instance of this is how a test
 * pins the gauge without asking the production source to grow one.
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

/**
 * One of the ten failure scenarios the streaming shuffle must survive without losing data.
 *
 * The set is closed and is enumerated by [[StreamingShuffleFaultScenario.all]], so a suite cannot
 * silently omit one.
 *
 * @param faultName stable identifier used in assertion messages
 */
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
 * Every fault is driven either by an armed flag a cooperating seam consults or by an advance of the
 * injected [[ManualClock]]. Neither sleeps, which is what makes a timeout assertion instant and
 * exact and lets a suite prove a timer does NOT fire one millisecond early.
 *
 * The armed-flag engine is a fixed map built once from the closed scenario set, so arming and
 * firing need no lock: each scenario owns an arming budget and a fired count, and the budget is
 * consumed atomically. Bounding the budget is what makes "fail the first attempt, then succeed"
 * expressible, which is the shape a retry or a reconnection test needs.
 *
 * @param clock the manual clock the component under test was constructed with, so the component
 *              and the fault share one notion of now
 */
class StreamingShuffleFaultInjector(val clock: ManualClock) {

  private class FaultState {
    val remainingTriggers = new AtomicInteger(0)
    val fireTally = new AtomicInteger(0)
  }

  // Built once from the closed scenario set and never mutated, so no lock is needed: the map is
  // immutable and each value's own counters are atomic.
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

  /**
   * Arms a scenario for a bounded number of triggers.
   *
   * @param scenario the scenario to arm
   * @param triggers how many subsequent [[shouldFail]] calls should report a fault; the default of
   *                 one produces the single-failure-then-recover shape a retry test needs
   */
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

  /**
   * Consumes one unit of arming budget and reports whether the fault should be taken now.
   *
   * This is the single decision point every cooperating seam calls, and it is atomic, so a seam
   * reached concurrently from several threads still takes exactly the armed number of faults --
   * which is what makes the concurrent-producer-failure scenario countable rather than approximate.
   *
   * @param scenario the scenario being checked
   * @return true when the caller should inject the fault
   */
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

  /**
   * Throws the supplied failure when the scenario is armed, and does nothing otherwise.
   *
   * The failure is a by-name parameter so a scenario that is not armed costs nothing at all, not
   * even the construction of an exception that would be discarded.
   *
   * @param scenario the scenario being checked
   * @param failure the throwable to raise when the fault is taken
   */
  def failIfArmed(scenario: StreamingShuffleFaultScenario, failure: => Throwable): Unit = {
    if (shouldFail(scenario)) {
      throw failure
    }
  }

  /**
   * Simulates the loss of a producer executor part way through its write.
   *
   * The two observable consequences a reader must see are the arming of the crash seam and the loss
   * of reachability, so both are applied together rather than left to the caller to remember.
   */
  def crashProducer(): Unit = {
    armIndefinitely(StreamingShuffleFaultScenario.ProducerCrashDuringWrite)
    partitioned.set(true)
  }

  def crashConsumer(): Unit = {
    armIndefinitely(StreamingShuffleFaultScenario.ConsumerCrashDuringRead)
    partitioned.set(true)
  }

  /**
   * Simulates several producers for one shuffle failing at once.
   *
   * @param numProducers how many producers fail together; the arming budget is set to that count
   *                     so exactly that many seams take the fault and no more
   */
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

  /**
   * Caps what a simulated allocator is willing to grant, which is how memory exhaustion is
   * expressed without actually exhausting the heap.
   *
   * @param grantableBytes the largest request the allocator will satisfy; zero refuses everything
   */
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

  /**
   * Makes one directory refuse writes for the duration of a scope, and always restores it.
   *
   * '''Why this is a scoped resource rather than a pair of calls.''' A read-only directory and an
   * armed fault scenario are both process-wide state that survives the test that created them, and
   * the earlier shape of this facility -- arm, then hand the caller a boolean, then rely on the
   * caller to call a separate restore -- got all three of the resulting hazards wrong:
   *
   *  - it armed the scenario '''before''' discovering whether the permission change had taken
   *    effect, so on a filesystem or at a privilege level that ignores the change the injector was
   *    left armed for a fault that could never occur, and every later case in the JVM inherited it;
   *  - it accepted any `File`, so a mistyped path could make a directory the suite does not own --
   *    a shared spill root, or a parent of it -- read-only for the rest of the run;
   *  - restoration was a second call the caller had to remember, which a failing assertion between
   *    the two skips entirely. A directory left read-only by a failing test makes every subsequent
   *    test that spills fail for a reason that has nothing to do with what it was testing.
   *
   * So this takes no directory at all. It '''creates''' the one it makes read-only, inside a
   * temporary root, which is what makes "suite-owned" a property of construction rather than of a
   * path check -- and a path check is exactly what cannot distinguish a directory the suite created
   * from Spark's own local scratch root, since in a test JVM both live under the same temporary
   * parent. A caller that needs a component to spill into the failing directory points that
   * component's configuration at [[ScopedDiskFault.directory]].
   *
   * The fault is then verified before it is armed, and the whole thing is an `AutoCloseable` whose
   * `close` restores the permission, disarms the scenario and removes the directory, whatever
   * happened in between. Callers use it through
   * [[StreamingShuffleTestHelper.withFailingDiskWrites]], which closes it in a `finally`.
   *
   * @return the armed fixture, whose `close()` the caller owns. Whether the permission change took
   *         effect at all is reported by `isEffective`, because a JVM running as root or a
   *         filesystem that ignores the bit cannot produce the fault and a caller must cancel
   *         rather than assert a write failure that will not happen
   */
  def failDiskWrites(): ScopedDiskFault = {
    val directory = Utils.createTempDir(namePrefix = "streaming-shuffle-disk-fault")
    // The permission change first and the arming only if it took effect, so a scenario is never
    // left armed for a fault this environment cannot produce.
    val applied = directory.setWritable(false)
    val effective = applied && !directory.canWrite
    if (effective) {
      armIndefinitely(StreamingShuffleFaultScenario.DiskFailureDuringSpill)
    } else if (applied) {
      // The call was accepted but the directory is still writable -- running as root, or on a
      // filesystem that ignores the bit. Undo it so nothing is left half-changed.
      directory.setWritable(true)
    }
    new ScopedDiskFault(directory, effective, this)
  }

  /**
   * Returns a checksum that is guaranteed to differ from the correct one for the given payload.
   *
   * Flipping the low bit is enough and is deterministic, which matters because a randomly chosen
   * wrong value could in principle collide with the right one and turn a corruption test green.
   *
   * @param correctChecksum the CRC32C the producer would have stamped
   * @return a value that cannot equal `correctChecksum`
   */
  def corruptChecksum(correctChecksum: Long): Long = {
    tally(StreamingShuffleFaultScenario.ChecksumMismatchOnReceive)
    correctChecksum ^ 1L
  }

  /**
   * Corrupts a payload in place of transmission damage, deterministically and detectably.
   *
   * The returned array is a copy, so the caller's original stays available as the expected value
   * for the retransmission that must follow.
   *
   * @param payload the block payload to damage; an empty payload is returned as a single byte,
   *                because a zero-length array cannot carry a detectable change
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

  /**
   * Simulates a garbage-collection pause by advancing the manual clock, never by sleeping.
   *
   * A real pause and an advanced clock are indistinguishable to any component that reads time
   * through the injected clock, which every streaming shuffle component does, so this produces the
   * same observable effect in zero elapsed time.
   *
   * @param pauseMillis how long the JVM should appear to have been stopped
   */
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
   * The absence is expressed as a clock advance well beyond the liveness window, then reachability
   * is restored and the reconnection scenario is tallied, which is the exact sequence the writer's
   * replay path is specified to handle.
   *
   * @param downtimeMillis how long the consumer was away; must exceed the liveness window for the
   *                       writer to have noticed the absence at all
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
 * Created by [[StreamingShuffleFaultInjector.failDiskWrites]] and normally used through
 * [[StreamingShuffleTestHelper.withFailingDiskWrites]], which closes it in a `finally` so that a
 * failing assertion cannot leave a directory unwritable, a scenario armed, or a directory behind
 * for every later case in the JVM.
 *
 * `close` is idempotent, and does the three things a scope end owes in the order that makes each
 * possible: disarm the scenario, restore the write permission, then remove the directory -- the
 * removal needs the permission back to succeed.
 *
 * @param directory the directory whose write permission is suspended, created by and owned by this
 *                  fixture, which is why nothing else in the JVM can be affected by it
 * @param isEffective whether the permission change actually took effect in this environment. False
 *                    on a filesystem or at a privilege level that ignores it -- notably as root --
 *                    in which case nothing was armed and a caller must cancel rather than assert a
 *                    write failure that cannot happen
 * @param injector the injector whose scenario was armed, and which `close` disarms
 */
class ScopedDiskFault(
    val directory: File,
    val isEffective: Boolean,
    injector: StreamingShuffleFaultInjector) extends AutoCloseable {

  private val closed = new AtomicBoolean(false)

  /** Disarms the scenario, restores write permission and removes the directory, exactly once. */
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


/**
 * Carries one fallback condition to an executor and drives it there, mid-write.
 *
 * <b>Why a serializable holder rather than a closure over a value.</b> Two facts have to be true at
 * once. The condition must be driven on the EXECUTOR, because the policy every writer consults is
 * executor scoped and a driver-side instance is a different object. And the shuffle id must be
 * known to the closure, because the throughput recorders are per shuffle -- yet the id does not
 * exist until the shuffled RDD has been defined, which is after the upstream map was written. A
 * mutable holder resolves the ordering: the field is set once the graph exists and before the
 * action submits it, and task closures are serialized at submission rather than at definition.
 *
 * <b>Why each condition is driven through its own recorder.</b> Driving them uniformly through a
 * fabricated verdict would test one code path four times. Each recorder here is the one the
 * production code calls for that condition, so the trip is the trip the feature specifies rather
 * than a stand-in for it. None of them waits: the two that depend on elapsed time take the instant
 * as an argument, so sixty seconds of sustained slowness is two calls rather than a minute of
 * sleeping.
 *
 * @param reason the condition to drive on the executor
 */
private[spark] class MidWriteTripHolder(val reason: StreamingShuffleFallbackReason)
  extends Serializable {

  /**
   * The shuffle whose producers are to stand down. Set by the driver after the shuffled RDD exists
   * and before the job is submitted; negative until then, which the trip treats as nothing to do.
   */
  @volatile var shuffleId: Int = -1

  /**
   * Drives this holder's condition against the running executor's own fallback policy.
   *
   * A no-op when this JVM is not running the streaming manager, or when the shuffle id has not been
   * supplied, so the holder is safe to invoke from a workload that may legitimately be running on
   * the sort-based path.
   */
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
 * Constants and pure helpers shared by every streaming shuffle suite and by the benchmark.
 *
 * Two opposite policies govern this object, and which one applies depends on whether the value is a
 * '''contract with a peer''' or an '''internal threshold'''.
 *
 * ==Wire-protocol values are INDEPENDENT LITERALS==
 *
 * Every byte count, field order and cap of the wire format is written here as a literal, spelled as
 * the sum of its parts, and is deliberately '''not''' read from the encoder that enforces it. The
 * reason is that a constant taken from production can only ever agree with production: it restates
 * the implementation instead of specifying it, so an accidental change to the layout is silently
 * adopted by every assertion that consumes it and nothing fails. A wire format is a contract with a
 * peer that may be running a different build, so it needs an oracle, and the literal is that
 * oracle. [[verifyWireContractAgainstEncoder]] is the single place the two are compared, and the
 * suites that touch the wire call it, so a drift fails a test with a message naming exactly which
 * field moved.
 *
 * ==Internal thresholds are DERIVED==
 *
 * Timeouts, intervals, budget percentages and retry ladders are internal to this subsystem: nothing
 * outside the JVM depends on them, and a suite asserting on the ten second consumer window is
 * asserting that the writer and the protocol agree with each other rather than with a peer. Those
 * values are therefore derived from the component that owns them, so a suite cannot fall behind a
 * deliberate change to a threshold.
 *
 * The remaining literals are values that exist nowhere in the production code: the configuration
 * bounds (which live inside validator closures rather than as named constants), the off-by-one
 * boundary values that exist purely so a suite can prove a timer does not fire early, and the
 * stress-workload shape.
 */
object StreamingShuffleTestHelper {

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

  /**
   * Property naming the number of times a service may retry a port bind before giving up.
   *
   * A raw key rather than a typed entry because Spark declares none for it: `Utils.portMaxRetries`
   * reads the property directly, so there is nothing to import and this string is the only way to
   * state it. Named here rather than written at each fixture so the three fixtures cannot drift.
   */
  val PortMaxRetriesKey: String = "spark.port.maxRetries"

  /**
   * Bind retries every fixture grants, which is the figure `Utils.portMaxRetries` hands to any
   * suite whose configuration carries `spark.testing`.
   *
   * Matched to that figure rather than chosen: these suites bind exactly the same ephemeral
   * endpoints as every other Spark suite -- a driver, a block manager, and on a `local-cluster`
   * master a master, a worker and an executor for each -- so the tolerance they need is the one the
   * rest of the test suite already receives, and any smaller number is a flake waiting for a busy
   * host. See `testEnvelopeConf` for why it has to be stated rather than inherited.
   */
  val TestPortMaxRetries: Int = 100

  /**
   * Lowest execution-memory accounting identity a fixture task memory manager may use.
   *
   * A trillion, which is far above any attempt id a scheduler will hand out in a suite -- attempt
   * ids start at zero and advance by one per task -- and far below `Long.MaxValue`, so the offset
   * cannot overflow for any identity a fixture would sensibly choose. See
   * [[StreamingShuffleTestHelper.newTaskMemoryManager]] for why sharing the range with real tasks
   * makes the managed-memory leak check report a leak against the wrong task.
   */
  val FixtureAttemptIdBase: Long = 1000000000000L

  // ---------------------------------------------------------------------------------------------
  // Wire protocol.
  //
  // INDEPENDENT LITERALS. Nothing in this block reads the encoder it describes: a value copied
  // from the encoder agrees with it by construction and therefore specifies nothing, so a change to
  // the layout would be adopted silently by every assertion built on it. These literals are the
  // oracle for the format; verifyWireContractAgainstEncoder() is the one place they are compared
  // with the production constants, and it names the field that moved when they disagree.
  // ---------------------------------------------------------------------------------------------

  /** The protocol revision a compatible peer must present. */
  val ProtocolVersion: Byte = 1

  /**
   * Bytes occupied by the header every streaming message carries.
   *
   * The header is protocol version, shuffle id, partition id and sequence number, which is one plus
   * four plus four plus eight bytes. Producer identity is not a header field: it is the first field
   * of every message body, so it sits at a fixed frame offset immediately after the header and can
   * be peeked without decoding. That distinction matters because a speculative copy or a retry of
   * the same map task is a separate flow which must still be told apart from the attempt it
   * supersedes.
   */
  val HeaderEncodedLength: Int = 17

  /** Bytes the producer identifier occupies as the first field of every message body. */
  val ProducerIdEncodedLength: Int = 8

  /** Bytes the framing layer prepends to carry the message-type discriminator. */
  val FrameTypePrefixLength: Int = 1

  /** Largest payload a single data block may carry, which is the two megabyte pipelining cap. */
  val MaxBlockSizeBytes: Int = 2 * 1024 * 1024

  /**
   * Bytes a data block spends on framing over and above its payload: the type discriminator (1),
   * the shared header (17), the producer id that opens every body (8), the CRC32C checksum (8) and
   * the payload length prefix (4).
   */
  val DataBlockFramingOverheadBytes: Int = 1 + 17 + 8 + 8 + 4

  /** Largest framed data block, that is a maximum payload plus header, checksum and framing. */
  val MaxEncodedFrameBytes: Int = 2 * 1024 * 1024 + 38

  /**
   * Encoded length of every control message: acknowledgement, heartbeat, retransmission request and
   * stream termination. Each is the shared header followed by the producer identifier and nothing
   * else, because each folds its one semantic value into the header's sequence number.
   *
   * All four are the same size, which is precisely why a decoder must never discriminate on length.
   * The framing type byte is the discriminator; [[messageTypeOf]] reads the concrete class instead,
   * which is the same decision expressed in Scala.
   */
  val FixedMessageEncodedLength: Int = 25

  /** Encoded length of a heartbeat, which is a control message like any other. */
  val HeartbeatBaseEncodedLength: Int = FixedMessageEncodedLength

  /** Sentinel a consumer sends before it has consumed anything. */
  val NothingConsumedPosition: Long = -1L

  /** Blocks a single retransmission request names, which is exactly one position. */
  val RequestedBlocksPerRequest: Long = 1L

  /** Largest span a consumer will ask a producer to replay, in blocks. */
  val MaxReplayWindowBlocks: Long = StreamingShuffleClientHandler.MAX_REPLAY_WINDOW_BLOCKS

  /** Checksum algorithm every streaming block is stamped with. */
  val ChecksumAlgorithm: String = "CRC32C"

  /**
   * Compares every wire-format literal above with the constant the encoder actually enforces.
   *
   * This is the coupling the independent literals deliberately lack, isolated into one method so
   * that it is impossible to consume a wire constant from this object and accidentally consume
   * production's value instead. A suite that touches the wire calls this once; a layout change then
   * fails here, with a message naming the field that moved, rather than being absorbed by whichever
   * assertion happened to be built on the drifted value.
   *
   * Every message's total is checked as well as every field width, because a header change and a
   * body change can cancel out in a total while still being an incompatible format.
   */
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
    check("requested blocks per request", RequestedBlocksPerRequest,
      RetransmitRequestMessage.REQUESTED_BLOCKS)
    check("checksum algorithm", ChecksumAlgorithm, StreamingShuffleChecksum.ALGORITHM)
    // Totals, taken from real messages so that a body change is caught as well as a header change.
    val payload = Array[Byte](1, 2, 3, 4)
    val payloadChecksum = StreamingShuffleChecksum.computeBlock(1, 2L, 3, 4L, payload)
    check("acknowledgement encoded length", FixedMessageEncodedLength,
      new AckMessage(1, 2L, 3, 5L).encodedLength())
    check("retransmission request encoded length", FixedMessageEncodedLength,
      new RetransmitRequestMessage(1, 2L, 3, 4L).encodedLength())
    check("stream termination encoded length", FixedMessageEncodedLength,
      new StreamTerminationMessage(1, 2L, 3, 4L).encodedLength())
    check("heartbeat encoded length", HeartbeatBaseEncodedLength,
      new HeartbeatMessage(1, 2L, 3, 4L).encodedLength())
    check("data block encoded length",
      HeaderEncodedLength + ProducerIdEncodedLength + 8 + 4 + payload.length,
      new DataBlockMessage(1, 2L, 3, 4L, payloadChecksum, payload).encodedLength())
    // Reported through an assertion rather than a thrown Error, which is both what the project's
    // style gate requires of test code and the right shape here: the caller is a test, and the
    // message names every field that moved.
    assert(mismatches.isEmpty,
      "The streaming shuffle wire format has drifted from the contract these suites assert " +
        "against. Either the encoder changed and every peer of a different build must be " +
        "considered, or the change was unintended: " + mismatches.mkString("; ") + ".")
  }

  val ProducerConnectionTimeoutMillis: Long = BackpressureProtocol.ACK_TIMEOUT_MS

  val HeartbeatIntervalMillis: Long = BackpressureProtocol.HEARTBEAT_INTERVAL_MS

  val ConsumerLivenessTimeoutMillis: Long = BackpressureProtocol.CONSUMER_LIVENESS_TIMEOUT_MS

  val SustainedSlownessWindowMillis: Long = BackpressureProtocol.SUSTAINED_SLOWNESS_WINDOW_MS

  /**
   * Smallest advance that trips sustained slowness.
   *
   * The condition is STRICTLY greater than the window, so the trip point is one millisecond past
   * it. A suite that advanced by exactly the window and expected a trip would be asserting the
   * wrong comparison.
   */
  val SustainedSlownessTripMillis: Long = SustainedSlownessWindowMillis + 1L

  val JustBeforeProducerTimeoutMillis: Long = ProducerConnectionTimeoutMillis - 1L

  val JustBeforeConsumerLivenessMillis: Long = ConsumerLivenessTimeoutMillis - 1L

  val JustBeforeSustainedSlownessMillis: Long = SustainedSlownessWindowMillis - 1L

  val RetryBaseBackoffMillis: Long = BackpressureProtocol.RETRY_BASE_BACKOFF_MS

  val MaxRetryAttempts: Int = BackpressureProtocol.MAX_RETRY_ATTEMPTS

  /**
   * The complete exponential backoff ladder, one entry per permitted attempt.
   *
   * Computed from the production backoff function rather than written out, so the ladder a suite
   * advances a clock through is the ladder the protocol actually waits.
   */
  val RetryBackoffLadderMillis: Seq[Long] =
    (1 to MaxRetryAttempts).map(attempt => BackpressureProtocol.retryBackoffMillis(attempt))

  // ---------------------------------------------------------------------------------------------
  // Rate limiting and fallback thresholds. Eighty percent and ninety percent are DIFFERENT
  // constants serving different purposes, and conflating them is the easiest mistake to make here.
  // ---------------------------------------------------------------------------------------------

  val BandwidthCeilingPercent: Long = TokenBucketRateLimiter.BANDWIDTH_CEILING_PERCENT

  val LinkSaturationTripPercent: Long = BackpressureProtocol.LINK_SATURATION_PERCENT

  val BytesPerMebibyte: Long = TokenBucketRateLimiter.BYTES_PER_MIB

  val ConsumerSlownessRatio: Double = StreamingShuffleFallbackPolicy.CONSUMER_SLOWNESS_RATIO

  /**
   * Smallest burst allowance a token bucket may carry.
   *
   * A bucket that cannot hold one maximum-size block could never admit one, so the floor is the
   * block cap itself rather than an arbitrary small number.
   */
  val MinTokenBucketCapacityBytes: Long = MaxBlockSizeBytes.toLong

  val TransportModuleName: String = "shuffle-streaming"

  val MetricsSourceName: String = StreamingShuffleMetricsSource.sourceName

  val BufferUtilizationMetricName: String = "bufferUtilizationPercent"

  val SpillCountMetricName: String = "spillCount"

  val BackpressureEventsMetricName: String = "backpressureEvents"

  val PartialReadInvalidationsMetricName: String = "partialReadInvalidations"

  /**
   * Every metric the streaming shuffle namespace publishes.
   *
   * There are exactly four and there is no fifth, so a suite can assert the registry's name set
   * equals this sequence and catch both an omission and an unannounced addition.
   */
  val MetricNames: Seq[String] = Seq(
    BufferUtilizationMetricName,
    SpillCountMetricName,
    BackpressureEventsMetricName,
    PartialReadInvalidationsMetricName)

  /**
   * The logger every class in the streaming shuffle package logs beneath.
   *
   * Each class logs under its own fully qualified name, and log4j2 propagates a child's events to
   * every ancestor logger's appenders, so attaching to the package is what measures the whole
   * subsystem without naming any of its classes -- and without a new class being added later
   * escaping the measurement. Derived from a type in the package rather than written out, so the
   * name cannot drift from the package it is meant to name.
   */
  val StreamingShuffleLoggerName: String = classOf[StreamingShuffleManager].getPackage.getName

  // Workload shape and stress-run parameters. The latency reduction bounds below are the acceptance
  // targets the benchmark reports against, not thresholds any automated gate asserts.

  val DefaultPartitionCount: Int = 10

  val TargetDatasetBytes: Long = 100L * 1024L * 1024L

  val RecordValueLength: Int = 256

  val StressDurationMillis: Long = 5L * 60L * 1000L

  val StressConcurrentTasks: Int = 10

  val StressConcurrentShuffles: Int = 5

  val StressFailureInjectionPercent: Int = 10

  val MaxThroughputDegradationPercent: Int = 5

  /** Lower bound of the reported latency reduction. */
  val MinLatencyReductionPercent: Int = 30

  /** Upper bound of the reported latency reduction. */
  val MaxLatencyReductionPercent: Int = 50

  /** Per-test ceiling the base suite imposes, in minutes. Everything here must fit inside it. */
  val PerTestTimeoutMinutes: Int = 20

  val DefaultAwaitTimeoutMillis: Long = 30000L

  /** Starting time of a manual clock, chosen non-zero so a bug that reads zero stands out. */
  val ManualClockEpochMillis: Long = 1000000L

  // ---------------------------------------------------------------------------------------------
  // Pure helpers. None of these touch a SparkContext, so they are usable from anywhere.
  // ---------------------------------------------------------------------------------------------

  /**
   * The aggregate buffer allowance, computed exactly as the specification states it and
   * independently of how production computes it.
   *
   * ==Why `BigInt` and not `Long`==
   *
   * The specified quantity is `(executorMemory * bufferPercent) / 100`. Written in `Long`
   * arithmetic that expression can overflow, so any `Long` implementation of it has to be
   * rearranged -- and once it is rearranged, an expectation written the same way is a transcription
   * of the implementation rather than a check on it: both orderings agree with themselves, so a
   * fixture that copies production's ordering passes whichever ordering production uses. `BigInt`
   * needs no rearrangement. It evaluates the specified expression literally, in one obvious step,
   * and truncates once at the end, which makes this an independent statement of the contract that a
   * divide-before-multiply implementation fails for any memory figure that is not a clean multiple
   * of a hundred.
   *
   * @param executorMemoryBytes size of the memory region the budget is carved from
   * @param bufferPercent share of that region reserved for streaming buffers
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

  /**
   * The share of a quantity a whole-number percentage names, computed exactly and independently of
   * production, for the same reason [[aggregateBudgetBytes]] is.
   *
   * @param value the quantity to take a percentage of
   * @param percent the percentage to take
   * @return the exact share, truncated once
   */
  def exactPercentageOf(value: Long, percent: Int): Long = {
    require(value >= 0L, s"value must be non-negative but was $value")
    require(percent >= 0, s"percent must be non-negative but was $percent")
    (BigInt(value) * BigInt(percent) / BigInt(PercentScale)).toLong
  }

  /**
   * The per-partition buffer allowance, computed exactly as the specification states it: the
   * aggregate allowance from [[aggregateBudgetBytes]] divided by the reduce partition count.
   *
   * @param executorMemoryBytes size of the memory region the budget is carved from
   * @param bufferPercent share of that region reserved for streaming buffers
   * @param numPartitions partitions the aggregate allowance is divided across
   * @return bytes one partition may hold
   */
  def perPartitionBudgetBytes(
      executorMemoryBytes: Long,
      bufferPercent: Int,
      numPartitions: Int): Long = {
    require(numPartitions > 0, s"numPartitions must be positive but was $numPartitions")
    aggregateBudgetBytes(executorMemoryBytes, bufferPercent) / numPartitions.toLong
  }

  /**
   * Applies the eighty percent ceiling to a declared link capacity.
   *
   * Dividing before multiplying is what the production limiter does, and reproducing that order
   * here means the expected value matches the implementation bit for bit rather than approximately.
   *
   * @param bytesPerSecond declared capacity
   * @return the capacity streaming permits itself
   */
  def applyBandwidthCeiling(bytesPerSecond: Long): Long =
    TokenBucketRateLimiter.applyLinkCapacityCeiling(bytesPerSecond)

  /**
   * The token bucket refill rate for one shuffle: the declared capacity, held to its ceiling, then
   * divided evenly across the shuffles the executor is serving.
   *
   * @param maxBandwidthMBps declared link capacity in mebibytes per second
   * @param numConcurrentShuffles shuffles the executor is producing for, never treated as zero
   * @return bytes per second one shuffle's bucket refills at
   */
  def refillBytesPerSecond(maxBandwidthMBps: Int, numConcurrentShuffles: Int): Long =
    applyBandwidthCeiling(
      TokenBucketRateLimiter.perShuffleBytesPerSecond(maxBandwidthMBps, numConcurrentShuffles))

  def dataBlockEncodedLength(payloadLength: Int): Int = {
    require(payloadLength >= 0, s"payloadLength must be non-negative but was $payloadLength")
    DataBlockFramingOverheadBytes - FrameTypePrefixLength + payloadLength
  }

  /**
   * Framed length of a message, which is always its encoded length plus the discriminator byte.
   *
   * @param encodedLength the message's own encoded length
   * @return bytes on the wire
   */
  def framedLength(encodedLength: Int): Int = StreamingShuffleMessage.framedLength(encodedLength)

  /**
   * The message type of a decoded message, determined from its concrete class.
   *
   * Length is deliberately not consulted, and could not be: three of the five types encode to the
   * same number of bytes, and two of them -- the data block and the heartbeat -- are variable. A
   * length-based discriminator would therefore be both ambiguous and unstable.
   *
   * @param message any streaming shuffle message
   * @return the discriminator that message would be framed with
   */
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
   * Seeded so the same seed and length always produce the same bytes, which is what lets a
   * retransmission test compare a replayed block against the original by value.
   *
   * @param seed value that fixes the byte sequence
   * @param length bytes to produce
   * @return the payload
   */
  def deterministicPayload(seed: Long, length: Int): Array[Byte] = {
    require(length >= 0, s"length must be non-negative but was $length")
    val payload = new Array[Byte](length)
    new Random(seed).nextBytes(payload)
    payload
  }

  /**
   * A deterministic value string of [[RecordValueLength]] characters.
   *
   * Strings rather than byte arrays are used for dataset values because set equality is what proves
   * zero data loss, and arrays compare by identity rather than by content.
   *
   * @param partitionIndex partition the record belongs to
   * @param recordIndex position of the record within that partition
   * @return a value that is unique to the pair and reproducible from it
   */
  def deterministicValue(partitionIndex: Int, recordIndex: Int): String = {
    val prefix = s"p$partitionIndex-r$recordIndex-"
    val filler = ('a' + Utils.nonNegativeMod(partitionIndex + recordIndex, 26)).toChar
    if (prefix.length >= RecordValueLength) {
      prefix.substring(0, RecordValueLength)
    } else {
      prefix + filler.toString * (RecordValueLength - prefix.length)
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Error conditions this feature is authorized to add to the central catalogue.
  //
  // The set is closed at three, and all three describe a WIRE fault. A degradation is deliberately
  // absent: standing streaming down is reported through a boolean and a value from a sealed set, so
  // it needs no condition. Naming the authorized set here rather than inside one suite is what lets
  // any suite compare the catalogue against it instead of against the absence of a throw.
  // ---------------------------------------------------------------------------------------------

  /** Prefix carried by every error condition this feature is authorized to add. */
  val StreamingShuffleConditionPrefix: String = "STREAMING_SHUFFLE_"

  /**
   * SQLSTATE every streaming shuffle condition carries.
   *
   * `XXKST` is the class Spark already uses for shuffle checksum verification, which is the closest
   * precedent this feature has: an internal, unrecoverable integrity fault.
   */
  val StreamingShuffleSqlState: String = "XXKST"

  /** Every error condition this feature is authorized to add, and no others. */
  val AuthorizedErrorConditions: Set[String] = Set(
    s"${StreamingShuffleConditionPrefix}CHECKSUM_VERIFY_FAILED",
    s"${StreamingShuffleConditionPrefix}INVALID_SEQUENCE_NUMBER",
    s"${StreamingShuffleConditionPrefix}UNEXPECTED_MESSAGE_TYPE")

  /**
   * Trips one of the four documented degradation conditions on a policy, deterministically.
   *
   * One place where each condition is driven, shared by every caller, so a suite that trips a
   * condition on a policy held by a live [[StreamingShuffleManager]] does it in exactly the way a
   * suite that trips one from inside a running map task does. Two sources of truth for "how is
   * memory pressure provoked" would let the two drift until one of them stopped provoking anything
   * and its assertions started passing for the wrong reason.
   *
   * Every condition is driven through the policy's own public recording surface, and every instant
   * is SUPPLIED rather than read from a clock, so the sixty-second sustained-slowness window is
   * crossed without waiting a minute and without the policy needing an injected clock. That is what
   * makes this usable against the policy a real manager built for itself, whose clock is the
   * system's.
   *
   * @param policy the policy to trip, which must not have tripped already -- the latch is monotone,
   *               so a second condition driven at a tripped policy would be recorded and ignored
   * @param reason which of the four conditions to provoke
   * @param shuffleId the shuffle the throughput samples belong to. Only consumer slowness reads it,
   *                  because only it is measured per shuffle
   */
  def driveFallbackReason(
      policy: StreamingShuffleFallbackPolicy,
      reason: StreamingShuffleFallbackReason,
      shuffleId: Int): Unit = {
    reason match {
      case StreamingShuffleFallbackReason.ConsumerTooSlow =>
        // A consumer held at a quarter of the producer's rate, sampled twice so that the second
        // sample lies strictly beyond the sustained window.
        val openedAtMillis = 0L
        val beyondWindowMillis = openedAtMillis + SustainedSlownessWindowMillis + 1L
        policy.recordProducerThroughput(shuffleId, 4000.0d, openedAtMillis)
        policy.recordConsumerThroughput(shuffleId, 1000.0d, openedAtMillis)
        policy.recordProducerThroughput(shuffleId, 4000.0d, beyondWindowMillis)
        policy.recordConsumerThroughput(shuffleId, 1000.0d, beyondWindowMillis)
      case StreamingShuffleFallbackReason.MemoryPressure =>
        // A reservation for one whole block granted nothing at all, which is the OOM risk the
        // condition names rather than a merely tight budget.
        policy.recordAllocationGrant(MaxBlockSizeBytes.toLong, 0L)
      case StreamingShuffleFallbackReason.NetworkSaturation =>
        // Ninety-nine percent of the administered link, which is strictly above the trip share, on
        // as many consecutive samples as a sustained saturation needs. One sample is deliberately
        // not enough: a pacing bucket must be able to admit one maximum-sized block, so its burst
        // allowance legitimately exceeds the administered capacity for a single sampling interval,
        // and tripping on that stood streaming down on links that were never saturated.
        (1L to StreamingShuffleFallbackPolicy.SATURATION_SUSTAINED_SAMPLES).foreach { _ =>
          policy.recordLinkUtilization(99.0d, 100.0d)
        }
      case StreamingShuffleFallbackReason.ProtocolVersionMismatch =>
        // A version one beyond the one this build speaks, detected by the explicit compatibility
        // check rather than inferred from a parse failure.
        policy.checkProtocolVersion((ProtocolVersion + 1).toByte)
    }
  }
}


/**
 * Shared fixtures, deterministic barriers and fault-injection hooks for the streaming shuffle
 * suites and benchmark.
 *
 * A plain trait, like the checksum test helper in the parent package: it declares no self-type and
 * extends no suite base class, so it stays usable from a benchmark, which is not a suite at all.
 * That is why every method needing a live `SparkContext`, `SparkEnv` or `MetricsSystem` takes it as
 * a parameter. Living in the same package as the production types grants access to what they
 * declare `private[spark]` without asking any of them to widen its visibility, which is what keeps
 * the binary compatibility gate passing with no exclusion entries.
 *
 * Three disciplines run through everything below, each making a class of flakiness impossible
 * rather than merely unlikely: nothing sleeps, because a wait is a bounded barrier and elapsed time
 * is an advance of an injected [[ManualClock]]; nothing is random unless it is seeded, so a failure
 * can be replayed rather than chased; and nothing shares mutable global state silently, which is
 * why [[resetStreamingShuffleMetrics]] exists for the JVM-singleton metrics source.
 */
trait StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  // Configuration fixtures. Every value is set through its typed ConfigEntry rather than a raw
  // string key, so a fixture that would violate a documented range fails where the fixture is built
  // instead of deep inside the component under test. The entries are consumed here and never
  // re-declared: each key may be declared exactly once in the JVM. The one exception is
  // `spark.port.maxRetries`, for which Spark declares no entry at all; see [[testEnvelopeConf]].

  /**
   * A `SparkConf` with the three properties of the test envelope that a fixture must state rather
   * than inherit.
   *
   * ==Why stating them is necessary at all==
   *
   * Every fixture below defaults to `loadDefaults = false`, deliberately, so that a case is
   * reproducible whatever the JVM it runs in happens to carry -- see `streamingConfWithOverrides`
   * for the concrete failure that hermeticity prevents. The cost is that the envelope's own
   * `-Dspark.*` properties are dropped with everything else, and three of them are load bearing.
   * A `SparkConf` is the only place the components below read them from, so a property that the
   * surefire and scalatest configurations set for the whole test JVM reaches nothing here unless it
   * is stated. [[StreamingShuffleStressSuite]] already discovered and documented this for the leak
   * check; this method is the same reasoning applied once, for every fixture, rather than once per
   * suite.
   *
   * ==The three properties, and what each one buys==
   *
   *  - '''`spark.unsafe.exceptionOnMemoryLeak`''' arms the managed-memory leak check. The executor
   *    reads it from the `SparkConf` and its own default is false, so without it a task that ended
   *    still holding acquired execution memory is merely logged as a warning and the suite passes
   *    regardless. Stating it is what turns "zero retained heap" from an aspiration into a machine
   *    check for every task every streaming suite runs -- which matters most exactly where these
   *    suites spend their effort, because a streaming producer holds a buffer budget and a spill
   *    manager for the life of a task and releases both through a task-completion listener.
   *  - '''`spark.port.maxRetries`''' restores the bind tolerance every other Spark suite receives.
   *    `Utils.portMaxRetries` grants 100 retries when `spark.testing` is present in the
   *    configuration and 16 otherwise, and these suites start `local-cluster` applications whose
   *    driver, block manager, worker and executor endpoints each bind an ephemeral port; on a busy
   *    host 16 attempts is not enough and the application fails to start with a `BindException`
   *    that has nothing to do with the case under test. The value is stated as the raw key because
   *    it is one of the few `spark.*` properties with no typed entry to state it through.
   *  - '''`spark.ui.enabled`''' keeps the web user interface out of these runs. It is off in the
   *    test envelope for a reason -- a Jetty server per `SparkContext` is a bound port, a thread
   *    pool and a page of log records that no case here reads -- and these suites create hundreds
   *    of contexts, so inheriting the default of true is both the largest single source of their
   *    log volume and one more port each to contend for.
   *
   * `spark.testing` itself is deliberately '''not''' set. It would grant the same bind tolerance,
   * but `UnifiedMemoryManager` also reads it to decide whether to reserve its 300MB system
   * allowance, so putting it in a `SparkConf` silently changes the executor memory a buffer budget
   * is a percentage of -- and the budget arithmetic is precisely what several of these suites
   * assert. The narrow key is used instead, which buys the tolerance and changes nothing else.
   *
   * Exposed rather than private because a suite that has to build its own configuration -- one
   * exercising a component directly, with no shuffle manager selected -- still runs in this
   * envelope, and would otherwise silently opt out of the leak check the package depends on.
   *
   * @param loadDefaults whether to pick up ambient `spark.*` system properties
   * @return a configuration carrying the envelope properties, ready for a fixture to build on
   */
  def testEnvelopeConf(loadDefaults: Boolean = false): SparkConf = {
    new SparkConf(loadDefaults)
      .set(UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      .set(PortMaxRetriesKey, TestPortMaxRetries.toString)
      .set(UI_ENABLED, false)
  }

  /**
   * A configuration that selects the streaming shuffle manager and leaves streaming behaviour off.
   *
   * This is the two-tier model's lower rung and the operator kill switch: the streaming manager
   * class is instantiated, and it forwards every service-provider call to the sort-based manager it
   * holds internally. A suite proving "gated off is indistinguishable from sort" starts here.
   *
   * @param loadDefaults whether to pick up ambient `spark.*` system properties; false by default so
   *                     a fixture is reproducible regardless of the environment it runs in
   * @return a configuration with the manager selected and the behaviour gate closed
   */
  def gatedOffStreamingConf(loadDefaults: Boolean = false): SparkConf = {
    testEnvelopeConf(loadDefaults)
      .set(SHUFFLE_MANAGER, StreamingShuffleManager.SHORT_NAME)
      .set(SHUFFLE_STREAMING_ENABLED, false)
  }

  def streamingConf(loadDefaults: Boolean = false): SparkConf = {
    gatedOffStreamingConf(loadDefaults).set(SHUFFLE_STREAMING_ENABLED, true)
  }

  /**
   * A configuration that selects sort-based shuffle, which is the default and the fallback.
   *
   * This is the baseline every zero-data-loss and every latency comparison is measured against.
   * It carries the same envelope properties as its streaming counterpart, because a baseline that
   * ran under a different envelope would be comparing two things at once.
   *
   * @param loadDefaults whether to pick up ambient `spark.*` system properties
   * @return a configuration on which shuffle behaviour is unchanged from stock Spark
   */
  def sortBaselineConf(loadDefaults: Boolean = false): SparkConf = {
    testEnvelopeConf(loadDefaults).set(SHUFFLE_MANAGER, "sort")
  }

  /**
   * A streaming configuration with every tunable stated explicitly.
   *
   * `maxBandwidthMBps` is an `Option` because the configuration entry is optional and ABSENCE is
   * the unlimited state. It is not encoded as zero or as a negative sentinel, and a fixture
   * that passed `Some(0)` would be rejected by the entry's own positivity validator, which is the
   * behaviour a suite should be asserting rather than working around.
   *
   * `None` is therefore made absent rather than merely left unset. With `loadDefaults` enabled a
   * `SparkConf` imports every ambient `spark.*` system property, so a JVM that happens to carry
   * `spark.shuffle.streaming.maxBandwidthMBps` -- a property another suite set, or one an operator
   * passed to the test JVM -- would hand this builder a capped fixture while its caller had asked
   * for an uncapped one, and a rate-limiting assertion would then be measuring a cap it never
   * chose. Removing the key is what makes `None` mean uncapped unconditionally, and it is a
   * removal rather than an assertion because a fixture's job is to establish the state its caller
   * named, not to fail because the environment disagreed.
   *
   * @param bufferSizePercent share of executor memory reserved for buffers, in one to fifty
   * @param spillThreshold utilisation at which spill triggers, in fifty to ninety-five
   * @param maxBandwidthMBps declared link capacity, or `None` for uncapped egress
   * @param debug whether verbose streaming diagnostics are emitted
   * @param enabled whether the behaviour gate is open; false yields the kill-switch configuration
   * @param loadDefaults whether to pick up ambient `spark.*` system properties
   * @return the configured instance
   */
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

  /**
   * A partitioner that spreads keys by hash across the requested number of partitions.
   *
   * @param partitions number of partitions to spread across
   * @return the partitioner
   */
  def hashingPartitioner(partitions: Int): Partitioner = {
    require(partitions > 0, s"partitions must be positive but was $partitions")
    new Partitioner() {
      override def numPartitions: Int = partitions
      override def getPartition(key: Any): Int = Utils.nonNegativeMod(key.hashCode, numPartitions)
    }
  }

  /**
   * A shuffled RDD over a small integer dataset, serialized with Kryo.
   *
   * Kryo rather than Java serialization is deliberate: it is what makes the sort path eligible for
   * its serialized write, so a comparison against that path compares like with like.
   *
   * @param sc live context to build on
   * @param conf configuration the serializer is constructed from
   * @param numPartitions partitions the shuffle produces
   * @param numRecords records fed into the shuffle
   * @return the shuffled RDD, not yet computed
   */
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

  /**
   * The shuffle dependency of a freshly built shuffled RDD.
   *
   * This is the object a writer or reader fixture needs, and taking it from a real RDD rather than
   * mocking it means the handle under test carries a genuine partitioner, serializer and aggregator
   * configuration.
   *
   * @param sc live context to build on
   * @param conf configuration the serializer is constructed from
   * @param numPartitions partitions the shuffle produces
   * @param numRecords records fed into the shuffle
   * @return the dependency registered for that shuffle
   */
  def shuffleDependencyFor(
      sc: SparkContext,
      conf: SparkConf,
      numPartitions: Int = DefaultPartitionCount,
      numRecords: Int = 10): ShuffleDependency[Int, Int, Int] = {
    shuffledIntRdd(sc, conf, numPartitions, numRecords)
      .dependencies.head.asInstanceOf[ShuffleDependency[Int, Int, Int]]
  }

  def groupByKeyWorkload(
      sc: SparkContext,
      numPartitions: Int = DefaultPartitionCount): RDD[(Int, Iterable[String])] = {
    val pairs = Seq((1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five"))
    sc.parallelize(pairs, numPartitions).groupByKey(numPartitions)
  }

  /**
   * The same grouping workload, with a mid-write fallback trip wired into its map side.
   *
   * <b>Why the trip has to live inside a map function.</b> A streaming writer pulls records from
   * the iterator it was handed, so a transformation upstream of the shuffle executes WHILE the
   * writer is streaming. Tripping the executor's live policy from there is therefore a genuine
   * mid-write trip against the very instance the writer consults at its next block boundary --
   * which no driver-side fixture can reproduce, because the policy is executor scoped.
   *
   * The trip fires once per map partition, on the first record, so it is observed after records
   * have been framed and before the iterator is exhausted. It is deterministic: no clock is waited
   * on and no rate is hoped for, because every condition is driven through a recorder that takes
   * the observation as an argument.
   *
   * @param sc live context to build on
   * @param numPartitions partitions to group into
   * @param holder carries the condition to drive and, once the caller has set it, the shuffle id
   * @return the grouped RDD, not yet computed
   */
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

  /**
   * A dataset of approximately [[TargetDatasetBytes]] spread evenly over `numPartitions`.
   *
   * The records are generated on the executors from their own partition and record indices rather
   * than shipped from the driver, which keeps a hundred mebibytes out of the driver heap and keeps
   * the dataset exactly reproducible at the same time. Values are strings, not byte arrays, so that
   * the output can be compared by value as a `Set` -- which is the operational definition of zero
   * data loss used throughout these suites.
   *
   * @param sc live context to build on
   * @param numPartitions partitions to spread the dataset across
   * @param totalBytes approximate total size of the generated values
   * @return the dataset, not yet computed
   */
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


  // Deterministic barriers. Nothing below sleeps and nothing waits without a bound: an unbounded
  // wait turns a deadlock into a twenty minute timeout with no diagnosis attached, while a bounded
  // wait that asserts on expiry names the barrier that never opened. Waits on futures go through
  // the sanctioned ThreadUtils wrappers, which the style checker requires by name.

  def newLatch(count: Int): CountDownLatch = {
    require(count >= 0, s"count must be non-negative but was $count")
    new CountDownLatch(count)
  }

  /**
   * A barrier that releases when the given number of parties have arrived.
   *
   * This is the tool for the concurrent scenarios: five shuffles or ten tasks that must be in
   * flight simultaneously for arbitration or memory pressure to be exercised at all.
   *
   * @param parties participants that must arrive
   * @return the barrier
   */
  def newBarrier(parties: Int): CyclicBarrier = {
    require(parties > 0, s"parties must be positive but was $parties")
    new CyclicBarrier(parties)
  }

  /**
   * A semaphore with the given number of permits, for pacing a simulated slow consumer.
   *
   * A consumer made to acquire a permit per block, with permits released at half the rate the
   * producer needs, is the deterministic expression of "the consumer runs at fifty percent of the
   * producer's rate" -- no timing assumption is involved.
   *
   * @param permits initial permits
   * @return the semaphore
   */
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
   * Runs a body on the given number of threads, all released from one barrier, and returns the
   * results in thread-index order.
   *
   * The barrier is what makes the concurrency real. Without it, submitting ten tasks to a pool
   * usually results in the first finishing before the last starts, and a test meant to exercise
   * contention exercises nothing. With it, every thread is proven to be inside the body before any
   * of them proceeds.
   *
   * The pool is a daemon pool and is always shut down, including when the body throws, so a failing
   * test cannot leave threads behind for the next one to trip over.
   *
   * @param numThreads threads to run on
   * @param prefix thread-name prefix, which is what makes a stack dump readable
   * @param timeoutMillis bound on both the barrier and the collection of results
   * @param body work to run, given its own zero-based thread index
   * @tparam T the body's result type
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

  // Injected clocks. Every streaming shuffle component accepts a Clock, defaulted to a system clock
  // in service and handed a ManualClock in test, which turns a five second timeout into one method
  // call. ManualClock's setTime and advance both notifyAll and waitTillTime waits on the same
  // monitor, so an advance on one thread releases a thread parked on another -- a legitimate
  // cross-thread rendezvous that is still not sleeping. Helpers come in pairs: one advances far
  // enough for a timer to fire, its counterpart to one millisecond short of the deadline, because
  // "the timer fired" and "the timer did not fire early" are two assertions and a suite owes both.

  /**
   * A manual clock starting at a deliberately non-zero epoch.
   *
   * Starting away from zero means a component that mistakenly reports an uninitialised timestamp
   * produces a value that is obviously wrong rather than one that coincides with the start of time.
   *
   * @param initialTimeMillis time the clock begins at
   * @return the clock
   */
  def newManualClock(initialTimeMillis: Long = ManualClockEpochMillis): ManualClock =
    new ManualClock(initialTimeMillis)

  /**
   * Runs a body with one directory made read-only, restoring it whatever the body does.
   *
   * The only supported way to use [[StreamingShuffleFaultInjector.failDiskWrites]]. Both pieces of
   * state a disk fault involves -- the directory's permission bit and the injector's armed scenario
   * -- outlive the case that created them, so restoration belongs in a `finally` rather than in a
   * call the caller has to remember to make after assertions that may not be reached.
   *
   * The body receives the fixture rather than nothing for two reasons: the directory it must write
   * into is the fixture's own, and whether the fault took effect is a property of the environment.
   * As root, or on a filesystem that ignores the permission bit, the change is not effective and a
   * case that asserted a write failure anyway would fail for the wrong reason; a body that needs
   * the fault to be real guards on `fault.isEffective`.
   *
   * @param injector the fault injector whose scenario is armed for the scope
   * @param body the assertions to run while writes into the fixture's directory fail
   * @tparam T whatever the body returns
   * @return the body's result
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
   * Provided so that reaching for wall time is an explicit, visible decision rather than an
   * accidental default.
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

  /**
   * Advances past the sustained-slowness window that trips the consumer-lag fallback.
   *
   * The trip condition is strictly greater than sixty seconds, so this advances by sixty seconds
   * and one millisecond, not by sixty seconds.
   *
   * @param clock clock to advance
   */
  def advancePastSustainedSlownessWindow(clock: ManualClock): Unit =
    clock.advance(SustainedSlownessTripMillis)

  def advanceJustBeforeSustainedSlownessWindow(clock: ManualClock): Unit =
    clock.advance(JustBeforeSustainedSlownessMillis)

  /**
   * Walks the clock through the whole retransmission backoff ladder, one delay per attempt.
   *
   * @param clock clock to advance
   * @return the cumulative clock reading after each delay, so a suite can assert on the sequence of
   *         instants at which the attempts become due rather than on the deltas alone
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
   * This is the cross-thread rendezvous the manual clock supports directly, and it is how a test
   * body can wait for a background component to have observed a particular instant without polling
   * and without sleeping.
   *
   * @param clock clock to observe
   * @param targetTimeMillis reading to wait for
   * @return the reading once it has been reached
   */
  def awaitClockReading(clock: ManualClock, targetTimeMillis: Long): Long =
    clock.waitTillTime(targetTimeMillis)

  // Task context and memory fixtures. The shared fixture hardcodes attempt number zero, so it
  // cannot express the flush-ordering case in which a speculative attempt is ordered behind the
  // original; the direct builder can, because the remaining TaskContextImpl parameters all default.
  // Both are safe from a test body: a shuffle manager's constructor runs before the driver's memory
  // manager exists, but by the time a test body executes both fixtures find a live one.

  def fakeTaskContext(sc: SparkContext): TaskContext = newTaskContext(sc.env)

  /**
   * A task context with caller-chosen identity fields.
   *
   * All nine required parameters are passed by name so that adding a tenth upstream is a compile
   * error here rather than a silent positional shift. The three remaining parameters are left at
   * their defaults, which the implementation documents as existing only for tests.
   *
   * @param env live environment supplying the memory manager and metrics system
   * @param stageId stage the task belongs to
   * @param stageAttemptNumber attempt of that stage
   * @param partitionId partition the task processes
   * @param taskAttemptId globally unique attempt identifier
   * @param attemptNumber attempt of this task; a non-zero value is what marks a speculative or
   *                      retried attempt, and is the field the shared fixture cannot vary
   * @param numPartitions partitions in the stage
   * @return the task context
   */
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

  /**
   * A real task memory manager backed by the environment's memory manager.
   *
   * Real rather than mocked, because the streaming spill manager is a memory consumer and the whole
   * point of the memory fixtures is that Spark's own accounting and its leak detection apply to the
   * streaming path unchanged.
   *
   * ==Why the accounting identity is not the caller's attempt id==
   *
   * A `MemoryManager` accounts execution memory '''per task attempt id''', and in these suites the
   * fixture and the live application share one: the master is `local`, so the driver is also the
   * executor, and a fixture built in a test body draws on the very memory manager the jobs in that
   * body draw on. Real attempt ids are handed out from zero upwards, and every fixture identity in
   * this package is a small number too -- a per-suite counter, or a literal chosen for readability
   * -- so the two ranges overlap almost completely.
   *
   * An overlap is not merely untidy, it is wrong in a way that fails a test for the opposite of the
   * reason it appears to. A fixture that holds a reservation across a job -- which several cases do
   * deliberately, because holding memory is how they establish pressure -- shares an accounting
   * entry with whichever job task drew the same id. When that task finishes, the executor's leak
   * check calls `releaseAllExecutionMemoryForTask` for the shared id, is handed the '''fixture's'''
   * outstanding bytes, and reports a managed memory leak against a task that released everything it
   * took. The pool then logs "release called on N bytes but task only has 0 bytes" when the fixture
   * finally does release, which is the same collision seen from the other end.
   *
   * So the accounting identity is offset into a range no scheduler will ever allocate, while the
   * identity the task context reports is left exactly as the caller asked. That split is the point:
   * attempt id is a '''logical''' identity that assertions and streaming producer generations are
   * built on and must stay readable, whereas the memory manager's key is an '''accounting''' one
   * that only has to be unique. Keeping the leak check armed is what makes the offset necessary
   * rather than cosmetic: without it the check cannot tell a real leak from this collision.
   *
   * @param env live environment supplying the memory manager
   * @param taskAttemptId logical attempt the allocations belong to; charged to
   *                      [[StreamingShuffleTestHelper.FixtureAttemptIdBase]] plus this value
   * @return the task memory manager
   */
  def newTaskMemoryManager(env: SparkEnv, taskAttemptId: Long = 0L): TaskMemoryManager =
    new TaskMemoryManager(env.memoryManager, fixtureAccountingAttemptId(taskAttemptId))

  /**
   * The execution-memory accounting identity for a fixture whose logical attempt id is given.
   *
   * Offset rather than hashed, so the mapping is order preserving and a diagnostic naming an
   * accounting identity can still be read back to the fixture that owns it. Negative and absurd
   * logical ids are tolerated by clamping the result into the reserved range, because a fixture's
   * identity is chosen for readability and must never be able to fold back onto a real one.
   *
   * @param taskAttemptId logical attempt id a fixture was built with
   * @return the identity its allocations are charged to
   */
  def fixtureAccountingAttemptId(taskAttemptId: Long): Long = {
    val offset = math.max(0L, taskAttemptId)
    FixtureAttemptIdBase + (offset % FixtureAttemptIdBase)
  }


  // Wire protocol fixtures. Every message factory takes its timestamp, sequence number and checksum
  // from its arguments rather than reading a clock or hashing implicitly, which mirrors the
  // encoder's own design and is what makes clock injection possible throughout the subsystem.

  def recordingBuffer(payload: Array[Byte]): RecordingStreamingManagedBuffer =
    new RecordingStreamingManagedBuffer(new NioManagedBuffer(ByteBuffer.wrap(payload)))

  /**
   * A data block whose checksum is the correct CRC32C over its payload and stream identity.
   *
   * The checksum covers the identity as well as the bytes, so a block delivered against the wrong
   * stream fails verification even when its payload is intact. Computing it here through the same
   * helper the producer uses is what makes a positive verification test meaningful.
   *
   * @param shuffleId shuffle the block belongs to
   * @param mapId producing map task
   * @param partitionId partition the block belongs to
   * @param sequenceNumber position of the block within its partition's stream
   * @param payload block payload
   * @return the block, correctly checksummed
   */
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

  /**
   * A data block whose stamped checksum is deliberately wrong.
   *
   * The payload is left intact and only the stamp is corrupted, which is the cleaner of the two
   * corruption shapes to reason about: the expected bytes are still available for comparison after
   * the retransmission that the mismatch must provoke.
   *
   * @param shuffleId shuffle the block belongs to
   * @param mapId producing map task
   * @param partitionId partition the block belongs to
   * @param sequenceNumber position of the block within its partition's stream
   * @param payload block payload, transmitted unchanged
   * @return a block that must fail verification
   */
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

  /**
   * An acknowledgement reporting how far a consumer has got.
   *
   * A control message carries no sequence number of its own: the position it reports IS its header
   * sequence number, encoded as the next position the consumer expects. Suites therefore state only
   * the consumed position, which is the one fact an acknowledgement asserts.
   *
   * @param shuffleId shuffle being acknowledged
   * @param mapId producing map task
   * @param partitionId partition being acknowledged
   * @param consumerPosition highest sequence the consumer has consumed, or the nothing-consumed
   *                         sentinel before it has consumed anything
   * @return the acknowledgement
   */
  def ack(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      consumerPosition: Long): AckMessage =
    new AckMessage(shuffleId, mapId, partitionId, consumerPosition)

  /**
   * A liveness signal reporting the position the consumer has reached.
   *
   * A heartbeat is a control message of exactly the size of every other, so it carries neither an
   * arrival timestamp nor a consumer identity on the wire. The receiver stamps arrival from its own
   * clock -- a peer-supplied instant would be a peer-controlled input to a local timeout -- and
   * identifies the sender from the connection it arrived on.
   *
   * @param shuffleId shuffle the stream belongs to
   * @param mapId producing map task
   * @param partitionId partition the stream belongs to
   * @param consumerPosition highest sequence the consumer has consumed, or the nothing-consumed
   *                         sentinel before it has consumed anything
   * @return the heartbeat
   */
  def heartbeat(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      consumerPosition: Long): HeartbeatMessage =
    new HeartbeatMessage(shuffleId, mapId, partitionId, consumerPosition)

  /**
   * A request to replay one block.
   *
   * A request names exactly one position, so a consumer that has lost a run of blocks emits one
   * request per position. That keeps every control message the same fixed size, and it keeps the
   * producer's obligation per message unambiguous: replay this block or refuse it.
   *
   * @param shuffleId shuffle the stream belongs to
   * @param mapId producing map task
   * @param partitionId partition the stream belongs to
   * @param sequenceNumber position of the block to replay
   * @return the request
   */
  def retransmitRequest(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      sequenceNumber: Long): RetransmitRequestMessage =
    new RetransmitRequestMessage(shuffleId, mapId, partitionId, sequenceNumber)

  /**
   * An orderly end-of-stream signal, placed at the only position the protocol permits.
   *
   * A terminator's position and the count it announces are one and the same field. Only data blocks
   * consume sequence numbers, and they are numbered densely from zero, so the position a terminator
   * sits at is exactly the count that preceded it. Carrying the pair as a single field makes the
   * mismatch this invariant used to guard against unrepresentable rather than merely rejected --
   * which matters, because an undercount was the silent direction: a consumer reconciles the two
   * figures, concludes the stream completed, and hands a truncated result onward with every
   * checksum intact.
   *
   * A total of zero is valid and means the partition was empty, which is a case a reader handles
   * rather than treats as an error.
   *
   * @param shuffleId shuffle the stream belongs to
   * @param mapId producing map task
   * @param partitionId partition the stream belongs to
   * @param totalBlocks blocks the producer sent, which is also the terminator's position
   * @return the termination signal
   */
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

  // Telemetry seams. The metrics source is an object, so its counters are JVM-global and survive
  // from one test into the next; a suite asserting on one resets first. Reset also empties the
  // registry of buffer-utilisation contributors, which detaches whatever was reporting the gauge,
  // so a suite asserting on the gauge installs its own contributor AFTER resetting, not before.

  def resetStreamingShuffleMetrics(): Unit = StreamingShuffleMetricsSource.reset()

  /**
   * Installs a contributor that pins the buffer utilisation gauge to a chosen percentage.
   *
   * The caller owns the returned contributor and should pass it to
   * [[removeBufferUtilization]] when finished, so one test does not leave the gauge reporting
   * another test's numbers.
   *
   * @param bufferedBytes numerator the gauge should report
   * @param budgetBytes denominator the gauge should report
   * @return the registered contributor, which can also be re-pointed in place
   */
  def installBufferUtilization(
      bufferedBytes: Long,
      budgetBytes: Long): FixedBufferUtilizationContributor = {
    val contributor = new FixedBufferUtilizationContributor(bufferedBytes, budgetBytes)
    StreamingShuffleMetricsSource.registerBufferUtilizationContributor(contributor)
    contributor
  }

  def removeBufferUtilization(contributor: FixedBufferUtilizationContributor): Unit =
    StreamingShuffleMetricsSource.unregisterBufferUtilizationContributor(contributor)

  /**
   * The metrics sources registered under the streaming shuffle namespace.
   *
   * This is the read seam an operability assertion uses: it looks the namespace up the same way the
   * metrics system itself does, so a suite proves the source is reachable through the registration
   * path rather than merely that the object exists.
   *
   * @param metricsSystem the driver's or an executor's metrics system
   * @return every source published under the streaming shuffle namespace, normally exactly one
   */
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

  /**
   * The buffer utilisation gauge from the streaming shuffle registry.
   *
   * The gauge is computed on read, so its value reflects whatever contributors are registered at
   * the moment it is queried rather than a figure captured earlier.
   *
   * @return the gauge
   */
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
   * The stress workload injects failures into ten percent of its tasks. Choosing them from a seeded
   * generator rather than at random means a stress failure can be reproduced exactly, which is the
   * difference between a bug that gets fixed and one that gets retried.
   *
   * @param numTasks tasks in the run
   * @param failurePercent share of them to mark for failure
   * @param seed value that fixes the selection
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

  // Sort-based baseline and data-loss assertions. "Zero data loss" is the statement that the set of
  // records a streaming shuffle produces equals the set sort-based shuffle produces from the same
  // input -- a set rather than a sequence, because a shuffle makes no promise about ordering within
  // a partition and comparing sequences would fail for a reason that is not a defect.

  def groupedOutputAsSet(
      sc: SparkContext,
      numPartitions: Int = DefaultPartitionCount): Set[(Int, Seq[String])] = {
    groupByKeyWorkload(sc, numPartitions)
      .mapValues(values => values.toSeq.sorted)
      .collect()
      .toSet
  }

  /**
   * Runs the grouping workload against sort-based shuffle in a context of its own, and returns the
   * baseline output as a set.
   *
   * The context is created and stopped inside this method, so the caller must hold no active
   * context when calling it: Spark permits one per JVM. A suite that already holds a context must
   * stop it first, or capture the baseline before starting it, which is the cheaper of the two.
   *
   * @param numPartitions partitions to group into
   * @param master master URL for the throwaway context
   * @return the baseline output, for comparison against a streaming run
   */
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

  /**
   * Asserts that an observed output set is exactly the baseline set, naming what differs when it is
   * not.
   *
   * Reporting the two directions separately matters: a missing element is data loss, an extra one
   * is duplication, and they have different causes.
   *
   * @param observed output produced by the path under test
   * @param baseline output produced by sort-based shuffle from the same input
   * @param description what was being exercised, quoted back in the failure message
   * @tparam T the record type
   */
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
   *
   * ==Why the exact count and not merely a balance==
   *
   * Balance alone is not the contract. A buffer retained twice and released twice is balanced, and
   * it is also a component that took a second reference it was never entitled to -- an extra
   * reference means the frame is pinned for longer than its records are needed, which is precisely
   * the retention this accounting exists to rule out, and it would sail through a "nonzero and
   * equal" check. So the take count is asserted against the documented lifecycle, which for a
   * streaming frame is one: the consumer takes a reference when it accepts the frame and drops it
   * once the frame's records have been surfaced.
   *
   * The three ways the lifecycle can be wrong are reported separately, because they have different
   * causes and different fixes: never taken (the accounting proves nothing, so the fixture is not
   * measuring the path it claims to), taken more than the documented number of times (over
   * retention), and dropped a different number of times than taken (a leak or an over release).
   *
   * @param buffers the counting buffers a run handed out
   * @param expectedTakesPerBuffer references the component under test is documented to take on each
   *                               buffer, which is one for an ordinary streaming frame
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

  /**
   * Asserts that a reporter only ever heard from one thread.
   *
   * The reporter contract permits an implementation to skip synchronisation because it promises
   * single-threaded use. A streaming shuffle that reported directly from a Netty event-loop thread
   * would break that promise silently; this turns it into a failed assertion.
   *
   * @param foreignThreadCallCount the count the recording reporter accumulated
   * @param description which reporter is being checked
   */
  def assertSingleThreadedReporting(foreignThreadCallCount: Long, description: String): Unit = {
    assert(foreignThreadCallCount == 0L,
      s"$description was called from more than one thread: $foreignThreadCallCount call(s) " +
        "arrived on a thread other than the first one seen, which breaks the reporter's " +
        "single-threaded contract")
  }

  /**
   * Asserts that no failure was published to an error notifier.
   *
   * The notifier is the bridge that carries a failure observed on an I/O thread over to the task
   * thread. Checking it explicitly catches the case in which a fault was swallowed rather than
   * surfaced, which would otherwise show up only as a task that never finishes.
   *
   * @param notifier the notifier a component under test was given
   * @param description what was being exercised, quoted back in the failure message
   */
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
}
