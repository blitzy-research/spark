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
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_DEBUG, SHUFFLE_STREAMING_ENABLED, SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, SHUFFLE_STREAMING_SPILL_THRESHOLD}
import org.apache.spark.memory.{MemoryTestingUtils, TaskMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.protocol.streaming.{AckMessage, DataBlockMessage, HeartbeatMessage, RetransmitRequestMessage, StreamingShuffleChecksum, StreamingShuffleMessage, StreamingShuffleMessageType, StreamTerminationMessage}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle.{ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter}
import org.apache.spark.util.{Clock, ManualClock, SystemClock, ThreadUtils, Utils}

/**
 * ScalaTest tag carried by the long-running streaming shuffle stress workload.
 *
 * The workload the tag marks runs for five minutes by design, which is well inside the twenty
 * minute per-test ceiling `SparkFunSuite` imposes but far too long for a fast verification pass.
 * Declaring the tag here rather than in the shared tags module keeps the whole feature additive:
 * nothing outside this folder changes, and no build file changes either, because the exclusion
 * wiring already exists. Sbt maps `-Dtest.exclude.tags` onto ScalaTest's `-l`, and Maven surfaces
 * the same property through its excluded-groups and tags-to-exclude settings. The tag is not a
 * member of the build's default exclusion list, so a full run INCLUDES the stress workload and
 * only a run that names this tag explicitly leaves it out.
 *
 * The tag string is the fully qualified name of this object, which is the convention ScalaTest
 * expects and which keeps the value unambiguous when it appears on a command line.
 */
object StreamingShuffleStressTest
  extends Tag("org.apache.spark.shuffle.streaming.StreamingShuffleStressTest")

/**
 * A `ShuffleWriteMetricsReporter` that accumulates every reported figure so a test can assert on
 * what the streaming write path actually reported.
 *
 * Two properties of the reporter contract shape this class.
 *
 * First, every method of `ShuffleWriteMetricsReporter` carries the `private[spark]` visibility
 * modifier on the method itself, not merely on the trait, so every override here must re-declare
 * it. That is by design upstream: the modifier exists precisely so a public, concrete
 * implementation can exist while the individual methods stay internal.
 *
 * Second, the reporter contract states that all methods are called from a single thread, so an
 * implementation need not synchronize. The streaming shuffle has genuine concurrency -- Netty
 * event-loop threads produce and consume frames while the task thread runs the writer -- so this
 * class does not merely assume the contract, it makes adherence OBSERVABLE. Every call records the
 * calling thread; the first caller becomes the owner, and any later call from a different thread
 * increments a foreign-thread counter that a test can assert is zero. That turns "Netty threads
 * only enqueue, the task thread reports" from a comment into a checkable property.
 *
 * Counters are `AtomicLong` so that a foreign-thread call is still counted accurately enough to be
 * reported rather than lost to a data race, which would defeat the purpose of detecting it.
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

  /** Net bytes reported written, that is increments less decrements. */
  def bytesWritten: Long = bytesWrittenTotal.get()

  /** Net records reported written, that is increments less decrements. */
  def recordsWritten: Long = recordsWrittenTotal.get()

  /** Total write time reported, in the unit the caller used. */
  def writeTime: Long = writeTimeTotal.get()

  /** Gross bytes withdrawn through `decBytesWritten`, useful when asserting rollback behaviour. */
  def bytesDecremented: Long = bytesDecrementedTotal.get()

  /** Gross records withdrawn through `decRecordsWritten`. */
  def recordsDecremented: Long = recordsDecrementedTotal.get()

  /** Number of reporter calls of any kind, so a test can assert the path reported at all. */
  def callCount: Long = callTotal.get()

  /**
   * Number of calls that arrived on a thread other than the first one seen.
   *
   * A streaming shuffle that honours the reporter's single-threaded contract leaves this at zero.
   * A non-zero value means an I/O thread reported directly instead of enqueueing for the task
   * thread, which is exactly the defect this fixture exists to surface.
   */
  def foreignThreadCallCount: Long = foreignThreadCallTotal.get()

  /** The thread that made the first reporter call, or `None` if nothing has been reported. */
  def reportingThread: Option[Thread] = Option(owningThread.get())

  /** Returns every accumulator to its initial state, including the observed reporting thread. */
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
 * `ShuffleReadMetricsReporter` declares SEVENTEEN members, not the handful the read path of a
 * simple shuffle happens to touch, and every one of them carries `private[spark]` on the method
 * itself. All seventeen are implemented here, each re-declaring the modifier, because a partial
 * implementation would not compile.
 *
 * The thread-observation machinery mirrors [[RecordingStreamingShuffleWriteMetrics]], and for the
 * same reason: the reporter contract promises single-threaded use, the streaming reader consumes
 * from an asynchronous channel, and a fixture that silently tolerated a report from a Netty thread
 * would hide precisely the defect worth catching.
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

  /** Remote blocks reported fetched. Streaming counts each delivered data block here. */
  def remoteBlocksFetched: Long = remoteBlocksFetchedTotal.get()

  /** Local blocks reported fetched. */
  def localBlocksFetched: Long = localBlocksFetchedTotal.get()

  /** Remote bytes reported read, which is where streamed payload bytes are accounted. */
  def remoteBytesRead: Long = remoteBytesReadTotal.get()

  /** Remote bytes reported spilled straight to disk rather than held in memory. */
  def remoteBytesReadToDisk: Long = remoteBytesReadToDiskTotal.get()

  /** Local bytes reported read. */
  def localBytesRead: Long = localBytesReadTotal.get()

  /** Total time the reader reported waiting for data to arrive. */
  def fetchWaitTime: Long = fetchWaitTimeTotal.get()

  /** Records reported read, the figure a data-completeness assertion compares against. */
  def recordsRead: Long = recordsReadTotal.get()

  /** Corrupt merged chunks reported. Push-based merge is not used by streaming, so expect zero. */
  def corruptMergedBlockChunks: Long = corruptMergedBlockChunksTotal.get()

  /** Merged-fetch fallbacks reported. Expect zero on the streaming path. */
  def mergedFetchFallbackCount: Long = mergedFetchFallbackTotal.get()

  /** Remote merged blocks reported fetched. Expect zero on the streaming path. */
  def remoteMergedBlocksFetched: Long = remoteMergedBlocksFetchedTotal.get()

  /** Local merged blocks reported fetched. Expect zero on the streaming path. */
  def localMergedBlocksFetched: Long = localMergedBlocksFetchedTotal.get()

  /** Remote merged chunks reported fetched. Expect zero on the streaming path. */
  def remoteMergedChunksFetched: Long = remoteMergedChunksFetchedTotal.get()

  /** Local merged chunks reported fetched. Expect zero on the streaming path. */
  def localMergedChunksFetched: Long = localMergedChunksFetchedTotal.get()

  /** Remote merged bytes reported read. Expect zero on the streaming path. */
  def remoteMergedBytesRead: Long = remoteMergedBytesReadTotal.get()

  /** Local merged bytes reported read. Expect zero on the streaming path. */
  def localMergedBytesRead: Long = localMergedBytesReadTotal.get()

  /** Cumulative duration of remote requests reported. */
  def remoteReqsDuration: Long = remoteReqsDurationTotal.get()

  /** Cumulative duration of remote merged requests reported. */
  def remoteMergedReqsDuration: Long = remoteMergedReqsDurationTotal.get()

  /** Number of reporter calls of any kind. */
  def callCount: Long = callTotal.get()

  /**
   * Number of calls that arrived on a thread other than the first one seen. Zero is the value a
   * reader that enqueues from its I/O threads and reports from the task thread produces.
   */
  def foreignThreadCallCount: Long = foreignThreadCallTotal.get()

  /** The thread that made the first reporter call, or `None` if nothing has been reported. */
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

  /** Returns every accumulator to its initial state, including the observed reporting thread. */
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
 * Wrapper for a managed buffer that keeps track of how many times retain and release are called.
 *
 * This class is defined here rather than obtained from a mocking library because `NioManagedBuffer`
 * is final and a final class cannot be spied on, which is the same reason the sort-path reader
 * suite in the parent package defines its own equivalent.
 *
 * The streaming reader takes a reference on every frame it accepts and drops it once the frame's
 * records have been surfaced, so the property worth proving is not "release was called" but
 * "release was called EXACTLY once for each retain". [[releasedExactlyOnce]] states that property
 * directly, and [[outstandingReferences]] reports the imbalance when it does not hold, which is
 * what turns a leak into a diagnosable number instead of a hung task.
 */
class RecordingStreamingManagedBuffer(underlyingBuffer: NioManagedBuffer) extends ManagedBuffer {

  var callsToRetain = 0
  var callsToRelease = 0

  override def size(): Long = underlyingBuffer.size()
  override def nioByteBuffer(): ByteBuffer = underlyingBuffer.nioByteBuffer()
  override def createInputStream(): InputStream = underlyingBuffer.createInputStream()
  override def convertToNetty(): AnyRef = underlyingBuffer.convertToNetty()
  override def convertToNettyForSsl(): AnyRef = underlyingBuffer.convertToNettyForSsl()

  override def retain(): ManagedBuffer = {
    callsToRetain += 1
    underlyingBuffer.retain()
  }

  override def release(): ManagedBuffer = {
    callsToRelease += 1
    underlyingBuffer.release()
  }

  /** True when the buffer was taken at least once and each take was matched by one drop. */
  def releasedExactlyOnce: Boolean = callsToRetain > 0 && callsToRetain == callsToRelease

  /**
   * Retains still outstanding. Positive means a leak, negative means an over-release; either is a
   * defect, and reporting the signed difference says which one happened.
   */
  def outstandingReferences: Int = callsToRetain - callsToRelease
}

/**
 * A buffer-utilisation contributor whose reported numerator and denominator are fixed by the test.
 *
 * `StreamingShuffleMetricsSource` exposes `bufferUtilizationPercent` as a gauge computed on read
 * from a registry of contributors rather than as a value a caller can set, because in service the
 * numerator and the denominator both have to come from the one object that owns the buffer budget.
 * That is the right production design and it leaves a test with no way to pin the gauge -- so this
 * class supplies one: register an instance, and the gauge reports exactly the percentage these two
 * numbers imply. It is the deterministic equivalent of a setter without asking the production
 * source to grow one.
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

  /**
   * Moves the reported numerator, so one registered contributor can drive the gauge across a
   * sequence of utilisation levels without being unregistered and replaced.
   *
   * @param bytes new buffered-byte figure to report
   */
  def setBufferedBytes(bytes: Long): Unit = buffered.set(bytes)

  /**
   * Moves the reported denominator.
   *
   * @param bytes new budget figure to report
   */
  def setBudgetBytes(bytes: Long): Unit = budget.set(bytes)
}

/**
 * One of the ten failure scenarios the streaming shuffle must survive without losing data.
 *
 * The set is closed and is enumerated by [[StreamingShuffleFaultScenario.all]], so a suite can
 * iterate the scenarios instead of restating them and cannot silently omit one.
 *
 * @param faultName stable, human-readable identifier used in assertion messages
 */
sealed abstract class StreamingShuffleFaultScenario(val faultName: String)

/**
 * The closed set of failure scenarios, in the order the specification enumerates them.
 */
object StreamingShuffleFaultScenario {

  /** Producer executor dies part way through writing its map output. */
  case object ProducerCrashDuringWrite
    extends StreamingShuffleFaultScenario("producerCrashDuringWrite")

  /** Consumer executor dies part way through reading a partition. */
  case object ConsumerCrashDuringRead
    extends StreamingShuffleFaultScenario("consumerCrashDuringRead")

  /** Producer and consumer are both alive but cannot reach each other. */
  case object NetworkPartition extends StreamingShuffleFaultScenario("networkPartition")

  /** A buffer allocation cannot be granted, which is the OOM-risk fallback condition. */
  case object MemoryExhaustionOnAllocation
    extends StreamingShuffleFaultScenario("memoryExhaustionOnAllocation")

  /** The local disk refuses the write a spill needs to make. */
  case object DiskFailureDuringSpill extends StreamingShuffleFaultScenario("diskFailureDuringSpill")

  /** A block arrives whose CRC32C does not match the value the producer stamped on it. */
  case object ChecksumMismatchOnReceive
    extends StreamingShuffleFaultScenario("checksumMismatchOnReceive")

  /** A transfer stalls long enough for the five second connection timeout to expire. */
  case object ConnectionTimeoutDuringTransfer
    extends StreamingShuffleFaultScenario("connectionTimeoutDuringTransfer")

  /** The JVM stops for a collection pause long enough to look like a liveness failure. */
  case object ExecutorGarbageCollectionPause
    extends StreamingShuffleFaultScenario("executorGarbageCollectionPause")

  /** Several producers for the same shuffle fail at once. */
  case object ConcurrentProducerFailures
    extends StreamingShuffleFaultScenario("concurrentProducerFailures")

  /** A consumer returns after being away long enough for its acknowledgements to lapse. */
  case object ConsumerReconnectAfterDowntime
    extends StreamingShuffleFaultScenario("consumerReconnectAfterDowntime")

  /** All ten scenarios, in specification order. */
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
 * Every fault this class injects is driven by one of exactly two mechanisms: an armed flag that a
 * cooperating seam consults, or an advance of the injected [[ManualClock]]. Neither mechanism
 * sleeps, and that is the whole point. A timeout test that sleeps is a test that is slow when it
 * passes and flaky when the machine is loaded; a timeout test that advances a manual clock is
 * instant and exact, and can assert that a timer does NOT fire one millisecond early, which a
 * sleeping test cannot do at all.
 *
 * The armed-flag engine is a fixed map built once from the closed scenario set, so arming and
 * firing need no lock and no map mutation: each scenario owns two counters, one for the arming
 * budget and one for the number of times the fault was actually taken. A test arms a scenario for
 * a bounded number of triggers, the seam calls [[shouldFail]] where the fault belongs, and the
 * budget is consumed atomically. Bounding the budget is what makes "fail the first attempt, then
 * succeed" expressible, which is exactly the shape a retry or a reconnection test needs.
 *
 * @param clock the manual clock the component under test was constructed with; every time-based
 *              fault is expressed as an advance of this clock, so the component and the fault
 *              share one notion of now
 */
class StreamingShuffleFaultInjector(val clock: ManualClock) {

  /** Per-scenario arming budget and fire tally. */
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

  /**
   * Records that a scenario's fault was taken, for the scenarios whose injection is an action the
   * injector performs itself rather than a decision a cooperating seam asks it to make.
   */
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

  /**
   * Arms a scenario so that it fires on every subsequent check until disarmed.
   *
   * @param scenario the scenario to arm indefinitely
   */
  def armIndefinitely(scenario: StreamingShuffleFaultScenario): Unit = {
    stateOf(scenario).remainingTriggers.set(Int.MaxValue)
  }

  /**
   * Withdraws any remaining arming budget for a scenario, leaving its fire tally intact.
   *
   * @param scenario the scenario to disarm
   */
  def disarm(scenario: StreamingShuffleFaultScenario): Unit = {
    stateOf(scenario).remainingTriggers.set(0)
  }

  /** Withdraws every arming budget and heals the simulated partition. Fire tallies are kept. */
  def disarmAll(): Unit = {
    StreamingShuffleFaultScenario.all.foreach(disarm)
    partitioned.set(false)
    grantedAllocationBytes.set(Long.MaxValue)
  }

  /**
   * @param scenario the scenario to inspect
   * @return true while the scenario still has arming budget left
   */
  def isArmed(scenario: StreamingShuffleFaultScenario): Boolean =
    stateOf(scenario).remainingTriggers.get() > 0

  /**
   * @param scenario the scenario to inspect
   * @return how many times the fault was actually taken, which is what an assertion checks to
   *         prove the seam was reached rather than merely armed
   */
  def fireCount(scenario: StreamingShuffleFaultScenario): Int = stateOf(scenario).fireTally.get()

  /** Total faults taken across all scenarios, useful for a stress-run summary assertion. */
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

  /** Simulates the loss of a consumer executor part way through its read. */
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

  /** Severs reachability between producer and consumer without either of them dying. */
  def partitionNetwork(): Unit = {
    armIndefinitely(StreamingShuffleFaultScenario.NetworkPartition)
    partitioned.set(true)
  }

  /** Restores reachability, which is the precondition for the reconnection scenario. */
  def healNetwork(): Unit = {
    disarm(StreamingShuffleFaultScenario.NetworkPartition)
    partitioned.set(false)
  }

  /** True while the simulated link is severed. A transport seam consults this before sending. */
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

  /** Restores an unbounded allocation grant. */
  def restoreAllocation(): Unit = {
    disarm(StreamingShuffleFaultScenario.MemoryExhaustionOnAllocation)
    grantedAllocationBytes.set(Long.MaxValue)
  }

  /**
   * Reports what a simulated allocator would grant for a request, so a test can feed the pair
   * straight into the fallback policy's allocation-grant observation.
   *
   * @param requestedBytes bytes the caller asked for
   * @return bytes the simulated allocator grants, which is less than the request under exhaustion
   */
  def grantAllocation(requestedBytes: Long): Long = {
    require(requestedBytes >= 0L, s"requestedBytes must be non-negative but was $requestedBytes")
    if (isArmed(StreamingShuffleFaultScenario.MemoryExhaustionOnAllocation)) {
      math.min(requestedBytes, grantedAllocationBytes.get())
    } else {
      requestedBytes
    }
  }

  /**
   * Makes a directory refuse writes, which is how a spill-time disk failure is produced.
   *
   * Some filesystems and some privilege levels ignore the permission change; the boolean result
   * says whether it took effect, so a suite can skip rather than pass vacuously.
   *
   * @param directory the spill directory to make read-only
   * @return true when the directory is genuinely no longer writable
   */
  def failDiskWrites(directory: File): Boolean = {
    armIndefinitely(StreamingShuffleFaultScenario.DiskFailureDuringSpill)
    directory.setWritable(false) && !directory.canWrite
  }

  /**
   * Restores write permission on a directory previously made read-only.
   *
   * @param directory the spill directory to restore
   * @return true when the directory is writable again
   */
  def restoreDiskWrites(directory: File): Boolean = {
    disarm(StreamingShuffleFaultScenario.DiskFailureDuringSpill)
    directory.setWritable(true) && directory.canWrite
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

  /**
   * Advances the clock past the five second producer connection timeout, so a reader watching for
   * a lapsed producer observes exactly that.
   */
  def expireProducerConnection(): Unit = {
    tally(StreamingShuffleFaultScenario.ConnectionTimeoutDuringTransfer)
    clock.advance(StreamingShuffleTestHelper.ProducerConnectionTimeoutMillis)
  }

  /**
   * Advances the clock past the ten second consumer liveness window, so a writer watching for a
   * silent consumer observes exactly that.
   */
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
 * Constants and pure helpers shared by every streaming shuffle suite and by the benchmark.
 *
 * A deliberate choice governs this object: wherever the production code already owns a constant,
 * the value here is DERIVED from it rather than restated as a literal. Restating would create two
 * sources of truth that can drift apart silently, and a test whose expected value drifted away from
 * the implementation's is worse than no test at all -- it passes while the behaviour is wrong, or
 * fails while the behaviour is right. Deriving means a suite asserting on the two megabyte cap is
 * asserting on the same number the encoder enforces, by construction.
 *
 * Only values that exist nowhere in the production code are written as literals here: the
 * configuration bounds (which live inside validator closures rather than as named constants), the
 * off-by-one boundary values that exist purely so a suite can prove a timer does not fire early,
 * and the stress-workload shape.
 */
object StreamingShuffleTestHelper {

  // ---------------------------------------------------------------------------------------------
  // Memory budget and spill.
  // ---------------------------------------------------------------------------------------------

  /** Default share of executor memory reserved across all streaming buffers. */
  val DefaultBufferSizePercent: Int = 20

  /** Smallest value the buffer-percent configuration validator accepts. */
  val MinBufferSizePercent: Int = 1

  /** Largest value the buffer-percent configuration validator accepts. */
  val MaxBufferSizePercent: Int = 50

  /** Default buffer utilisation at which the largest buffered partitions are spilled. */
  val DefaultSpillThresholdPercent: Int = 80

  /** Smallest value the spill-threshold configuration validator accepts. */
  val MinSpillThresholdPercent: Int = 50

  /** Largest value the spill-threshold configuration validator accepts. */
  val MaxSpillThresholdPercent: Int = 95

  /** Denominator the streaming shuffle uses for every percentage it computes. */
  val PercentScale: Long = MemorySpillManager.PERCENT_SCALE

  /** Cadence at which buffer utilisation is compared against the spill threshold. */
  val SpillPollIntervalMillis: Long = MemorySpillManager.POLL_INTERVAL_MS

  /** Bound within which a producer buffer must be reclaimed after an acknowledgement. */
  val ReclamationDeadlineMillis: Long = MemorySpillManager.RECLAMATION_DEADLINE_MS

  /** Records after which the sort-path disk writer refreshes its byte count. */
  val DiskWriterRecordUpdateInterval: Int = 16384

  // ---------------------------------------------------------------------------------------------
  // Wire protocol. Every value below is taken from the encoder that enforces it.
  // ---------------------------------------------------------------------------------------------

  /** The protocol revision a compatible peer must present. */
  val ProtocolVersion: Byte = StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION

  /**
   * Bytes occupied by the header every streaming message carries.
   *
   * The header is protocol version, shuffle id, map id, partition id and sequence number, which is
   * one plus four plus eight plus four plus eight bytes. The map id is part of the header because a
   * speculative copy or a retry of the same map task is a separate flow that must be told apart
   * from the attempt it supersedes.
   */
  val HeaderEncodedLength: Int = StreamingShuffleMessage.HEADER_ENCODED_LENGTH

  /** Bytes the framing layer prepends to carry the message-type discriminator. */
  val FrameTypePrefixLength: Int = StreamingShuffleMessage.FRAME_TYPE_PREFIX_LENGTH

  /** Largest payload a single data block may carry, which is the two megabyte pipelining cap. */
  val MaxBlockSizeBytes: Int = DataBlockMessage.MAX_BLOCK_SIZE_BYTES

  /** Largest framed data block, that is a maximum payload plus header, checksum and framing. */
  val MaxEncodedFrameBytes: Int = DataBlockMessage.MAX_ENCODED_FRAME_BYTES

  /** Bytes a data block spends on framing over and above its payload. */
  val DataBlockFramingOverheadBytes: Int = DataBlockMessage.FRAMING_OVERHEAD_BYTES

  /**
   * Encoded length of the three fixed-size messages: acknowledgement, retransmission request and
   * stream termination. Each adds one eight-byte field to the shared header and nothing else.
   *
   * All three are the same size, which is precisely why a decoder must never discriminate on
   * length. The framing type byte is the discriminator; [[messageTypeOf]] reads the concrete class
   * instead, which is the same decision expressed in Scala.
   *
   * The heartbeat is deliberately NOT in this set. It carries a length-prefixed consumer identity
   * as well as its timestamp, so its size varies with that identity -- see
   * [[heartbeatEncodedLength]]. Assuming otherwise is the mistake a length-based discriminator
   * makes, in miniature.
   */
  val FixedMessageEncodedLength: Int = HeaderEncodedLength + java.lang.Long.BYTES

  /**
   * Bytes a heartbeat's body occupies before its consumer identity: an eight-byte timestamp and a
   * four-byte length prefix. Expressed structurally so it states what it is rather than a number.
   */
  val HeartbeatFixedBodyLength: Int = java.lang.Long.BYTES + java.lang.Integer.BYTES

  /** Encoded length of a heartbeat that names no consumer. */
  val HeartbeatBaseEncodedLength: Int = HeaderEncodedLength + HeartbeatFixedBodyLength

  /** Longest consumer identifier a heartbeat may carry, in encoded bytes. */
  val MaxConsumerIdEncodedBytes: Int = HeartbeatMessage.MAX_CONSUMER_ID_ENCODED_BYTES

  /** Sentinel a consumer sends before it has consumed anything. */
  val NothingConsumedPosition: Long = AckMessage.NOTHING_CONSUMED

  /** Largest window a single retransmission request may span, in blocks. */
  val MaxRequestedBlocks: Long = RetransmitRequestMessage.MAX_REQUESTED_BLOCKS

  /** Checksum algorithm every streaming block is stamped with. */
  val ChecksumAlgorithm: String = StreamingShuffleChecksum.ALGORITHM

  // ---------------------------------------------------------------------------------------------
  // Failure detection and retry timing.
  // ---------------------------------------------------------------------------------------------

  /** Silence after which a reader declares its producer lost. */
  val ProducerConnectionTimeoutMillis: Long = BackpressureProtocol.ACK_TIMEOUT_MS

  /** Interval on which liveness is signalled while a stream is otherwise quiet. */
  val HeartbeatIntervalMillis: Long = BackpressureProtocol.HEARTBEAT_INTERVAL_MS

  /** Acknowledgement silence after which a writer declares its consumer absent. */
  val ConsumerLivenessTimeoutMillis: Long = BackpressureProtocol.CONSUMER_LIVENESS_TIMEOUT_MS

  /** Window over which consumer slowness must persist before it trips the fallback. */
  val SustainedSlownessWindowMillis: Long = BackpressureProtocol.SUSTAINED_SLOWNESS_WINDOW_MS

  /**
   * Smallest advance that trips sustained slowness.
   *
   * The condition is STRICTLY greater than the window, so the trip point is one millisecond past
   * it. A suite that advanced by exactly the window and expected a trip would be asserting the
   * wrong comparison.
   */
  val SustainedSlownessTripMillis: Long = SustainedSlownessWindowMillis + 1L

  /** One millisecond short of the producer timeout, for proving the timer does not fire early. */
  val JustBeforeProducerTimeoutMillis: Long = ProducerConnectionTimeoutMillis - 1L

  /** One millisecond short of the consumer liveness window. */
  val JustBeforeConsumerLivenessMillis: Long = ConsumerLivenessTimeoutMillis - 1L

  /** One millisecond short of the sustained-slowness window. */
  val JustBeforeSustainedSlownessMillis: Long = SustainedSlownessWindowMillis - 1L

  /** First delay in the retransmission backoff ladder. */
  val RetryBaseBackoffMillis: Long = BackpressureProtocol.RETRY_BASE_BACKOFF_MS

  /** Number of retransmission attempts before a failure is escalated. */
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

  /** Share of the declared link capacity the token bucket permits itself to use. */
  val BandwidthCeilingPercent: Long = TokenBucketRateLimiter.BANDWIDTH_CEILING_PERCENT

  /** Link utilisation above which network saturation trips the fallback. */
  val LinkSaturationTripPercent: Long = BackpressureProtocol.LINK_SATURATION_PERCENT

  /** Bytes in one mebibyte, the unit the bandwidth configuration is expressed in. */
  val BytesPerMebibyte: Long = TokenBucketRateLimiter.BYTES_PER_MIB

  /** Multiple by which a consumer must lag a producer for slowness to count. */
  val ConsumerSlownessRatio: Double = StreamingShuffleFallbackPolicy.CONSUMER_SLOWNESS_RATIO

  /**
   * Smallest burst allowance a token bucket may carry.
   *
   * A bucket that cannot hold one maximum-size block could never admit one, so the floor is the
   * block cap itself rather than an arbitrary small number.
   */
  val MinTokenBucketCapacityBytes: Long = MaxBlockSizeBytes.toLong

  /** Transport module name that gives streaming its own independent tuning namespace. */
  val TransportModuleName: String = "shuffle-streaming"

  // ---------------------------------------------------------------------------------------------
  // Telemetry.
  // ---------------------------------------------------------------------------------------------

  /** Metrics namespace the four streaming shuffle metrics live under. */
  val MetricsSourceName: String = StreamingShuffleMetricsSource.sourceName

  /** Registry name of the buffer utilisation gauge. */
  val BufferUtilizationMetricName: String = "bufferUtilizationPercent"

  /** Registry name of the spill counter. */
  val SpillCountMetricName: String = "spillCount"

  /** Registry name of the backpressure event counter. */
  val BackpressureEventsMetricName: String = "backpressureEvents"

  /** Registry name of the partial read invalidation counter. */
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

  // ---------------------------------------------------------------------------------------------
  // Workload shape, benchmark targets and stress-run parameters.
  // ---------------------------------------------------------------------------------------------

  /** Partition count the integration and benchmark scenarios are specified against. */
  val DefaultPartitionCount: Int = 10

  /** Total dataset size the headline latency scenario shuffles, one hundred mebibytes. */
  val TargetDatasetBytes: Long = 100L * 1024L * 1024L

  /** Length in characters of a generated record's value. */
  val RecordValueLength: Int = 256

  /** Duration of the continuous stress workload. */
  val StressDurationMillis: Long = 5L * 60L * 1000L

  /** Concurrent tasks the stress workload runs. */
  val StressConcurrentTasks: Int = 10

  /** Concurrent shuffles the stress workload drives. */
  val StressConcurrentShuffles: Int = 5

  /** Share of stress-workload tasks that are made to fail at random. */
  val StressFailureInjectionPercent: Int = 10

  /** Throughput degradation the stress workload is permitted to show. */
  val MaxThroughputDegradationPercent: Int = 5

  /** Lower bound of the latency reduction the benchmark reports against. */
  val MinLatencyReductionPercent: Int = 30

  /** Upper bound of the latency reduction the benchmark reports against. */
  val MaxLatencyReductionPercent: Int = 50

  /** Per-test ceiling the base suite imposes, in minutes. Everything here must fit inside it. */
  val PerTestTimeoutMinutes: Int = 20

  /** Default bound on any blocking wait a helper performs, generous but never unbounded. */
  val DefaultAwaitTimeoutMillis: Long = 30000L

  /** Starting time of a manual clock, chosen non-zero so a bug that reads zero stands out. */
  val ManualClockEpochMillis: Long = 1000000L

  // ---------------------------------------------------------------------------------------------
  // Pure helpers. None of these touch a SparkContext, so they are usable from anywhere.
  // ---------------------------------------------------------------------------------------------

  /**
   * The per-partition buffer allowance, computed exactly as the specification states it.
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
    require(executorMemoryBytes >= 0L,
      s"executorMemoryBytes must be non-negative but was $executorMemoryBytes")
    require(bufferPercent >= MinBufferSizePercent && bufferPercent <= MaxBufferSizePercent,
      s"bufferPercent must be in [$MinBufferSizePercent, $MaxBufferSizePercent] " +
        s"but was $bufferPercent")
    require(numPartitions > 0, s"numPartitions must be positive but was $numPartitions")
    executorMemoryBytes / PercentScale * bufferPercent.toLong / numPartitions.toLong
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

  /**
   * Encoded length of a data block carrying the given payload.
   *
   * @param payloadLength bytes of payload
   * @return encoded length, exclusive of the one-byte framing discriminator
   */
  def dataBlockEncodedLength(payloadLength: Int): Int = {
    require(payloadLength >= 0, s"payloadLength must be non-negative but was $payloadLength")
    DataBlockFramingOverheadBytes - FrameTypePrefixLength + payloadLength
  }

  /**
   * Encoded length of a heartbeat naming a consumer whose identity is the given number of UTF-8
   * bytes. Pass zero for a heartbeat that names no consumer.
   *
   * @param consumerIdByteLength UTF-8 bytes of the consumer identity, at most
   *                             [[MaxConsumerIdEncodedBytes]]
   * @return encoded length, exclusive of the one-byte framing discriminator
   */
  def heartbeatEncodedLength(consumerIdByteLength: Int): Int = {
    require(consumerIdByteLength >= 0 && consumerIdByteLength <= MaxConsumerIdEncodedBytes,
      s"consumerIdByteLength must be in [0, $MaxConsumerIdEncodedBytes] " +
        s"but was $consumerIdByteLength")
    HeartbeatBaseEncodedLength + consumerIdByteLength
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
}


/**
 * Shared fixtures, deterministic barriers and fault-injection hooks for the streaming shuffle
 * suites and benchmark.
 *
 * This is a plain trait, just like the checksum test helper in the parent package: it declares no
 * self-type and extends no suite base class, so a suite mixes it in with `with` and the trait stays
 * usable from a benchmark, which is not a suite at all. That is why every method that needs a live
 * `SparkContext`, `SparkEnv` or `MetricsSystem` takes it as a parameter rather than reaching for an
 * inherited field.
 *
 * The trait lives in `org.apache.spark.shuffle.streaming`, the same package as the fifteen
 * production types. That single fact is what grants access to types the production code declares
 * `private[spark]` without asking any of them to widen its visibility, which in turn is what keeps
 * the binary compatibility gate passing with no exclusion entries.
 *
 * Three disciplines run through everything below, and each one exists to make a specific class of
 * flakiness impossible rather than merely unlikely.
 *
 *  - Nothing sleeps. Waits are expressed as barriers with a bounded timeout, and elapsed time is
 *    expressed as an advance of an injected [[ManualClock]]. A sleeping test is slow when it passes
 *    and flaky when the machine is busy, and it cannot assert that a timer does NOT fire early;
 *    a clock-driven test is instant, exact, and can.
 *  - Nothing is random unless it is seeded. Every generator here is reproducible from its inputs,
 *    so a failure can be replayed rather than chased.
 *  - Nothing shares mutable global state silently. The metrics source is a JVM singleton whose
 *    counters outlive a test, so [[resetStreamingShuffleMetrics]] exists and its consequences are
 *    documented where they are not obvious.
 */
trait StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  // ---------------------------------------------------------------------------------------------
  // 1. Configuration fixtures.
  //
  // Every value is set through its typed ConfigEntry, never through a raw string key. Typed entries
  // carry their own validators, so a fixture that would violate a documented range fails at the
  // point the fixture is built rather than deep inside the component under test. The entries
  // themselves are consumed here and never re-declared: each key may be declared exactly once in
  // the JVM, and a second declaration would either shadow the first or throw during class
  // initialisation.
  // ---------------------------------------------------------------------------------------------

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
    new SparkConf(loadDefaults)
      .set(SHUFFLE_MANAGER, StreamingShuffleManager.SHORT_NAME)
      .set(SHUFFLE_STREAMING_ENABLED, false)
  }

  /**
   * A configuration that selects the streaming shuffle manager and opens the behaviour gate.
   *
   * @param loadDefaults whether to pick up ambient `spark.*` system properties
   * @return a configuration on which the streaming path is fully active
   */
  def streamingConf(loadDefaults: Boolean = false): SparkConf = {
    gatedOffStreamingConf(loadDefaults).set(SHUFFLE_STREAMING_ENABLED, true)
  }

  /**
   * A configuration that selects sort-based shuffle, which is the default and the fallback.
   *
   * This is the baseline every zero-data-loss and every latency comparison is measured against.
   *
   * @param loadDefaults whether to pick up ambient `spark.*` system properties
   * @return a configuration on which shuffle behaviour is unchanged from stock Spark
   */
  def sortBaselineConf(loadDefaults: Boolean = false): SparkConf = {
    new SparkConf(loadDefaults).set(SHUFFLE_MANAGER, "sort")
  }

  /**
   * A streaming configuration with every tunable stated explicitly.
   *
   * `maxBandwidthMBps` is an `Option` because the configuration entry is optional and ABSENCE is
   * the unlimited state. It is not encoded as zero or as a negative sentinel, and a fixture
   * that passed `Some(0)` would be rejected by the entry's own positivity validator, which is the
   * behaviour a suite should be asserting rather than working around.
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
    val conf = new SparkConf(loadDefaults)
      .set(SHUFFLE_MANAGER, StreamingShuffleManager.SHORT_NAME)
      .set(SHUFFLE_STREAMING_ENABLED, enabled)
      .set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, bufferSizePercent)
      .set(SHUFFLE_STREAMING_SPILL_THRESHOLD, spillThreshold)
      .set(SHUFFLE_STREAMING_DEBUG, debug)
    maxBandwidthMBps.foreach(cap => conf.set(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, cap))
    conf
  }

  /**
   * Adds the app name and master a fixture needs before it can build a `SparkContext`.
   *
   * Kept separate from the configuration builders above so the same builders serve both the unit
   * fixtures, which never start a context, and the integration fixtures, which do.
   *
   * @param conf the configuration to complete, modified in place and returned for chaining
   * @param appName application name to record
   * @param master master URL to run against
   * @return the same configuration, now startable
   */
  def withLocalMaster(
      conf: SparkConf,
      appName: String = "streaming-shuffle-test",
      master: String = "local[2]"): SparkConf = {
    conf.setAppName(appName).setMaster(master)
  }

  // ---------------------------------------------------------------------------------------------
  // 2. Partitioners and workload construction.
  // ---------------------------------------------------------------------------------------------

  /**
   * A partitioner that spreads keys by hash across the requested number of partitions.
   *
   * Both members carry an explicit result type. The nearest precedent in the sort-path writer suite
   * omits them, but the style checker requires a result type on every public method and new code
   * has no reason to inherit an existing blemish. That precedent is left exactly as it is.
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

  /**
   * The simplest end-to-end shuffle a suite can run: group a handful of pairs by key.
   *
   * @param sc live context to build on
   * @param numPartitions partitions to group into
   * @return the grouped RDD, not yet computed
   */
  def groupByKeyWorkload(
      sc: SparkContext,
      numPartitions: Int = DefaultPartitionCount): RDD[(Int, Iterable[String])] = {
    val pairs = Seq((1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five"))
    sc.parallelize(pairs, numPartitions).groupByKey(numPartitions)
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

  /**
   * Records per partition [[largeDataset]] will generate for a given shape.
   *
   * Exposed so a suite can state its expected record count without re-deriving the arithmetic and
   * drifting from it.
   *
   * @param numPartitions partitions the dataset is spread across
   * @param totalBytes approximate total size of the generated values
   * @return records each partition contributes
   */
  def recordsPerPartitionFor(
      numPartitions: Int = DefaultPartitionCount,
      totalBytes: Long = TargetDatasetBytes): Int = {
    require(numPartitions > 0, s"numPartitions must be positive but was $numPartitions")
    require(totalBytes > 0L, s"totalBytes must be positive but was $totalBytes")
    math.max(1, (totalBytes / numPartitions.toLong / RecordValueLength.toLong).toInt)
  }

  /**
   * A reproducible sequence of integer key-value records for driver-side unit fixtures.
   *
   * @param numRecords records to produce
   * @param seed value that fixes the sequence
   * @param keySpace exclusive upper bound on generated keys, so collisions can be forced by
   *                 narrowing it and avoided by widening it
   * @return the records, in generation order
   */
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

  /**
   * A reproducible payload, delegating to the pure helper so a suite has one obvious place to look.
   *
   * @param seed value that fixes the byte sequence
   * @param length bytes to produce
   * @return the payload
   */
  def payloadOfLength(seed: Long, length: Int): Array[Byte] = deterministicPayload(seed, length)

  /**
   * A payload of exactly the maximum permitted block size.
   *
   * The encoder accepts exactly the maximum and rejects one byte more, so this is the value a
   * boundary test needs on the accepting side.
   *
   * @param seed value that fixes the byte sequence
   * @return a payload of [[MaxBlockSizeBytes]] bytes
   */
  def maximumSizedPayload(seed: Long = 0L): Array[Byte] =
    deterministicPayload(seed, MaxBlockSizeBytes)

  /**
   * A payload one byte over the maximum permitted block size.
   *
   * @param seed value that fixes the byte sequence
   * @return a payload of [[MaxBlockSizeBytes]] plus one bytes, which the encoder must refuse
   */
  def oversizedPayload(seed: Long = 0L): Array[Byte] =
    deterministicPayload(seed, MaxBlockSizeBytes + 1)


  // ---------------------------------------------------------------------------------------------
  // 3. Deterministic barriers.
  //
  // No helper below sleeps, and none of them waits without a bound. An unbounded wait turns a
  // deadlock into a twenty minute timeout with no diagnosis attached; a bounded wait that asserts
  // on expiry names the barrier that never opened. Waits on futures go through the sanctioned
  // ThreadUtils wrappers, which exist precisely so that a caller never reaches for the raw
  // scala.concurrent entry points that the style checker forbids by name.
  // ---------------------------------------------------------------------------------------------

  /**
   * A latch that opens after the given number of countdowns.
   *
   * @param count countdowns required to open the latch
   * @return the latch
   */
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

  /**
   * Waits for a latch to open, and fails with a named diagnosis if it does not.
   *
   * @param latch latch to wait on
   * @param description what the latch represents, quoted back in the failure message
   * @param timeoutMillis bound on the wait
   */
  def awaitLatch(
      latch: CountDownLatch,
      description: String = "latch",
      timeoutMillis: Long = DefaultAwaitTimeoutMillis): Unit = {
    val opened = latch.await(timeoutMillis, TimeUnit.MILLISECONDS)
    assert(opened,
      s"Timed out after $timeoutMillis ms waiting for $description; " +
        s"${latch.getCount} countdown(s) never arrived")
  }

  /**
   * Waits at a barrier for every other party to arrive.
   *
   * @param barrier barrier to wait at
   * @param description what the barrier represents, quoted back in the failure message
   * @param timeoutMillis bound on the wait
   * @return the arrival index this party was given, which orders the participants deterministically
   */
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

  /**
   * Acquires one permit, and fails with a named diagnosis if none becomes available.
   *
   * @param semaphore semaphore to acquire from
   * @param description what the permit represents, quoted back in the failure message
   * @param timeoutMillis bound on the wait
   */
  def acquirePermit(
      semaphore: Semaphore,
      description: String = "permit",
      timeoutMillis: Long = DefaultAwaitTimeoutMillis): Unit = {
    val acquired = semaphore.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS)
    assert(acquired, s"Timed out after $timeoutMillis ms acquiring $description")
  }

  /**
   * Waits for a Java future to produce its value, through the sanctioned wrapper.
   *
   * @param future future to wait on
   * @param timeoutMillis bound on the wait
   * @tparam T the future's value type
   * @return the value
   */
  def awaitJavaFuture[T](future: JFuture[T], timeoutMillis: Long = DefaultAwaitTimeoutMillis): T = {
    ThreadUtils.awaitResult(future, Duration(timeoutMillis, TimeUnit.MILLISECONDS))
  }

  /**
   * Waits for a Scala awaitable to produce its value, through the sanctioned wrapper.
   *
   * @param awaitable awaitable to wait on
   * @param timeoutMillis bound on the wait
   * @tparam T the awaitable's value type
   * @return the value
   */
  def awaitValue[T](
      awaitable: Awaitable[T],
      timeoutMillis: Long = DefaultAwaitTimeoutMillis): T = {
    ThreadUtils.awaitResult(awaitable, Duration(timeoutMillis, TimeUnit.MILLISECONDS))
  }

  /**
   * Waits for a Scala awaitable to complete without unwrapping its value, through the sanctioned
   * wrapper. Useful when the assertion is about completion, or about a failure the caller wants to
   * inspect itself rather than have rethrown.
   *
   * @param awaitable awaitable to wait on
   * @param timeoutMillis bound on the wait
   * @tparam T the awaitable's value type
   * @return the same awaitable, now complete
   */
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

  // ---------------------------------------------------------------------------------------------
  // 4. Injected clocks.
  //
  // Every streaming shuffle component accepts a Clock, and every one of them defaults it to a
  // system clock in service and is handed a ManualClock in test. That is the single most important
  // determinism mechanism in this subsystem: it turns a five second timeout into one method call.
  //
  // ManualClock's setTime and advance both call notifyAll, and waitTillTime waits on the same
  // monitor, so a thread parked in waitTillTime is released by an advance on another thread. That
  // makes the clock a legitimate cross-thread rendezvous, not merely a value holder -- and it is
  // still not sleeping.
  //
  // Helpers come in pairs. One advances far enough for a timer to fire; its counterpart advances to
  // one millisecond short of the deadline, because "the timer fired" and "the timer did not fire
  // early" are two different assertions and a suite owes both.
  // ---------------------------------------------------------------------------------------------

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
   * A real system clock, for the rare fixture that genuinely needs wall time.
   *
   * Provided so that reaching for wall time is an explicit, visible decision rather than an
   * accidental default.
   *
   * @return a system clock
   */
  def wallClock(): Clock = new SystemClock

  /**
   * Advances by one spill poll interval, which is also the buffer reclamation deadline.
   *
   * @param clock clock to advance
   */
  def advanceOneSpillPoll(clock: ManualClock): Unit = clock.advance(SpillPollIntervalMillis)

  /**
   * Advances by exactly the reclamation deadline, so a suite can assert a buffer that was
   * acknowledged is released within it and that overrunning it is detected.
   *
   * @param clock clock to advance
   */
  def advanceReclamationDeadline(clock: ManualClock): Unit =
    clock.advance(ReclamationDeadlineMillis)

  /**
   * Advances past the five second producer connection timeout.
   *
   * @param clock clock to advance
   */
  def advancePastProducerTimeout(clock: ManualClock): Unit =
    clock.advance(ProducerConnectionTimeoutMillis)

  /**
   * Advances to one millisecond short of the producer connection timeout, for asserting the timer
   * has NOT fired.
   *
   * @param clock clock to advance
   */
  def advanceJustBeforeProducerTimeout(clock: ManualClock): Unit =
    clock.advance(JustBeforeProducerTimeoutMillis)

  /**
   * Advances past the ten second consumer liveness window.
   *
   * @param clock clock to advance
   */
  def advancePastConsumerLivenessTimeout(clock: ManualClock): Unit =
    clock.advance(ConsumerLivenessTimeoutMillis)

  /**
   * Advances to one millisecond short of the consumer liveness window.
   *
   * @param clock clock to advance
   */
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

  /**
   * Advances to one millisecond short of the sustained-slowness window.
   *
   * @param clock clock to advance
   */
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

  // ---------------------------------------------------------------------------------------------
  // 5. Task context and memory fixtures.
  //
  // Two fixtures, and the second is not a convenience. The shared one hardcodes attempt number
  // zero, so it cannot express the flush-ordering case in which a speculative attempt is ordered
  // behind the original; the direct builder can, because the remaining TaskContextImpl parameters
  // all carry defaults that exist for exactly this purpose.
  //
  // Both are safe from a test body. On the driver, the shuffle manager is initialised BEFORE the
  // memory manager, so SparkEnv.get.memoryManager is null while a shuffle manager's constructor
  // runs -- but by the time a test body executes, the context is fully built and both fixtures find
  // a live memory manager.
  // ---------------------------------------------------------------------------------------------

  /**
   * The shared task context fixture.
   *
   * Its declared type is `TaskContext`, an upcast, so members specific to the implementation are
   * not reachable through it. When a suite needs those, it wants [[newTaskContext]] instead.
   *
   * @param sc live context whose environment supplies the memory manager and metrics system
   * @return a task context suitable for constructing a writer or reader
   */
  def fakeTaskContext(sc: SparkContext): TaskContext = MemoryTestingUtils.fakeTaskContext(sc.env)

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
   * @param env live environment supplying the memory manager
   * @param taskAttemptId attempt the allocations are charged to
   * @return the task memory manager
   */
  def newTaskMemoryManager(env: SparkEnv, taskAttemptId: Long = 0L): TaskMemoryManager =
    new TaskMemoryManager(env.memoryManager, taskAttemptId)


  // ---------------------------------------------------------------------------------------------
  // 6. Wire protocol fixtures.
  //
  // Every message factory here supplies the timestamp, the sequence number and the checksum from
  // its arguments rather than reading a clock or hashing implicitly. That mirrors the encoder's own
  // design -- a heartbeat has no timestamp-less constructor and reads no clock -- and it is what
  // makes clock injection possible throughout the subsystem instead of merely convenient.
  // ---------------------------------------------------------------------------------------------

  /**
   * A managed buffer over the given bytes, wrapped so retains and releases are counted.
   *
   * @param payload bytes the buffer exposes
   * @return the counting buffer
   */
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
   * @param shuffleId shuffle being acknowledged
   * @param mapId producing map task
   * @param partitionId partition being acknowledged
   * @param sequenceNumber sequence the acknowledgement is stamped with
   * @param consumerPosition highest sequence the consumer has consumed, or the nothing-consumed
   *                         sentinel before it has consumed anything
   * @return the acknowledgement
   */
  def ack(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      sequenceNumber: Long,
      consumerPosition: Long): AckMessage =
    new AckMessage(shuffleId, mapId, partitionId, sequenceNumber, consumerPosition)

  /**
   * A liveness signal stamped with a caller-supplied instant.
   *
   * @param shuffleId shuffle the stream belongs to
   * @param mapId producing map task
   * @param partitionId partition the stream belongs to
   * @param sequenceNumber sequence the heartbeat is stamped with
   * @param timestampMillis instant to stamp, normally read from an injected clock
   * @return the heartbeat
   */
  def heartbeat(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      sequenceNumber: Long,
      timestampMillis: Long): HeartbeatMessage =
    new HeartbeatMessage(shuffleId, mapId, partitionId, sequenceNumber, timestampMillis)

  /**
   * A request to replay an inclusive window of blocks.
   *
   * @param shuffleId shuffle the stream belongs to
   * @param mapId producing map task
   * @param partitionId partition the stream belongs to
   * @param firstSequenceNumber lower bound of the window, inclusive
   * @param lastSequenceNumber upper bound of the window, inclusive
   * @return the request
   */
  def retransmitRequest(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      firstSequenceNumber: Long,
      lastSequenceNumber: Long): RetransmitRequestMessage =
    new RetransmitRequestMessage(
      shuffleId, mapId, partitionId, firstSequenceNumber, lastSequenceNumber)

  /**
   * An orderly end-of-stream signal, placed at the only position the protocol permits.
   *
   * The encoder enforces an invariant that is easy to violate by accident and worth stating here
   * because every suite that builds a terminator has to satisfy it: a terminator's sequence number
   * must EQUAL the number of blocks it announces. Only data blocks consume sequence numbers, and
   * they are numbered densely from zero, so the position a terminator sits at is exactly the count
   * that preceded it. The encoder rejects both directions of mismatch, and for asymmetric reasons:
   * an inflated total makes a finished stream look permanently incomplete, while an undercount is
   * worse because it is silent -- a consumer reconciles the two figures, concludes the stream
   * completed, and hands a truncated result onward with every checksum intact.
   *
   * This factory therefore takes only the count and derives the position from it, so a suite cannot
   * get the pair wrong. A suite that wants to prove the encoder REJECTS a mismatched pair should
   * construct the message directly, which is the only way to present one.
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
    new StreamTerminationMessage(shuffleId, mapId, partitionId, totalBlocks, totalBlocks)
  }

  /**
   * The correct CRC32C for a block, identity included.
   *
   * @param shuffleId shuffle the block belongs to
   * @param mapId producing map task
   * @param partitionId partition the block belongs to
   * @param sequenceNumber position of the block within its partition's stream
   * @param payload block payload
   * @return the checksum a producer would stamp
   */
  def blockChecksum(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      sequenceNumber: Long,
      payload: Array[Byte]): Long =
    StreamingShuffleChecksum.computeBlock(shuffleId, mapId, partitionId, sequenceNumber, payload)

  /**
   * The message type a decoded message would be framed with, determined from its class.
   *
   * @param message the message to classify
   * @return its discriminator
   */
  def typeOf(message: StreamingShuffleMessage): StreamingShuffleMessageType = messageTypeOf(message)

  // ---------------------------------------------------------------------------------------------
  // 7. Telemetry seams.
  //
  // The metrics source is an object, so its counters are JVM-global and survive from one test into
  // the next. Every suite that asserts on a counter therefore has to reset first, and that is what
  // the helper below is for.
  //
  // One consequence of reset is worth stating because it is not obvious: reset also empties the
  // registry of buffer-utilisation contributors, which detaches whatever was reporting the gauge --
  // including the executor-wide buffer quota, if one has already been created in this JVM. That is
  // harmless in a test and is why production code never calls reset in service, but it does mean
  // a suite asserting on the gauge should install its own contributor AFTER resetting, not before.
  // ---------------------------------------------------------------------------------------------

  /**
   * Returns the four streaming shuffle metrics to their initial state.
   *
   * @return unit; called for its effect on the JVM-global metric registry
   */
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

  /**
   * Detaches a previously installed contributor.
   *
   * @param contributor the contributor to detach
   */
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

  /**
   * Every metric name the streaming shuffle namespace currently publishes.
   *
   * @return the registry's names, as a set, for comparison against [[MetricNames]]
   */
  def streamingShuffleMetricNames(): Set[String] =
    StreamingShuffleMetricsSource.metricRegistry.getNames.asScala.toSet

  /**
   * A counter from the streaming shuffle registry, by its bare name.
   *
   * @param name one of the three counter names
   * @return the counter
   */
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

  /** Current spill count as an operator would observe it. */
  def observedSpillCount(): Long = streamingShuffleCounter(SpillCountMetricName).getCount

  /** Current backpressure event count as an operator would observe it. */
  def observedBackpressureEvents(): Long =
    streamingShuffleCounter(BackpressureEventsMetricName).getCount

  /** Current partial read invalidation count as an operator would observe it. */
  def observedPartialReadInvalidations(): Long =
    streamingShuffleCounter(PartialReadInvalidationsMetricName).getCount

  /** Current buffer utilisation percentage as an operator would observe it. */
  def observedBufferUtilizationPercent(): Long = bufferUtilizationGauge().getValue

  // ---------------------------------------------------------------------------------------------
  // 8. Fault injection.
  // ---------------------------------------------------------------------------------------------

  /**
   * A fault injector bound to the given clock.
   *
   * Binding the injector to the same clock the component under test was given is what makes a
   * time-based fault visible to that component: there is one notion of now, and the injector moves
   * it.
   *
   * @param clock the clock the component under test reads
   * @return the injector
   */
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

  // ---------------------------------------------------------------------------------------------
  // 9. Sort-based baseline and data-loss assertions.
  //
  // "Zero data loss" is not an opinion about a log line: it is the statement that the set of
  // records a streaming shuffle produces equals the set sort-based shuffle produces from the same
  // input.
  // A set rather than a sequence, because a shuffle makes no promise about ordering within a
  // partition and comparing sequences would fail for a reason that is not a defect.
  // ---------------------------------------------------------------------------------------------

  /**
   * Runs the grouping workload on an existing context and returns its output as a set.
   *
   * Whether the run exercises the streaming path or the sort path is decided entirely by the
   * configuration the given context was built with, so the same method produces both sides of a
   * comparison.
   *
   * @param sc live context to run on
   * @param numPartitions partitions to group into
   * @return each key with its grouped values, the values sorted so the comparison is order-free
   */
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
   * Asserts that every buffer taken was dropped exactly once.
   *
   * @param buffers the counting buffers a run handed out
   */
  def assertBuffersReleasedExactlyOnce(
      buffers: Seq[RecordingStreamingManagedBuffer]): Unit = {
    val leaked = buffers.filter(buffer => buffer.outstandingReferences != 0)
    assert(leaked.isEmpty,
      s"${leaked.size} of ${buffers.size} managed buffer(s) were not released exactly once; " +
        s"outstanding reference counts ${leaked.map(_.outstandingReferences).mkString(", ")}")
    val untouched = buffers.filter(buffer => buffer.callsToRetain == 0)
    assert(untouched.isEmpty,
      s"${untouched.size} of ${buffers.size} managed buffer(s) were never retained, so their " +
        "release accounting proves nothing")
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

  /**
   * Asserts that a failure of the expected kind reached the task thread.
   *
   * @param notifier the notifier a component under test was given
   * @param description what was being exercised, quoted back in the failure message
   * @tparam T the throwable type expected
   */
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

