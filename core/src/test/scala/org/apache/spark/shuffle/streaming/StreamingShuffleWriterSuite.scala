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

import scala.collection.mutable

import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SharedSparkContext, SparkConf, SparkException, SparkFunSuite, TaskContextImpl}
import org.apache.spark.internal.config.{SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, SHUFFLE_STREAMING_SPILL_THRESHOLD}
import org.apache.spark.network.shuffle.protocol.streaming.{DataBlockMessage, StreamingShuffleChecksum}
import org.apache.spark.shuffle.{FetchFailedException, ShuffleWriter}
import org.apache.spark.util.{Clock, ManualClock}

/**
 * Unit tests for [[StreamingShuffleWriter]], the producer half of the streaming shuffle.
 *
 * The four cases the feature specification names for this suite -- buffer allocation with
 * per-partition memory tracking, the spill trigger at the 80% threshold with timing validation,
 * checksum generation, and producer-failure cleanup with resource reclamation -- are contracts
 * rather than suggestions, and each has a test of its own below. The remainder of the suite covers
 * the service-provider obligations a streaming writer must honour so that the unmodified shuffle
 * write path works: a non-empty map status on success, a double-stop guard, a well-formed partition
 * length vector, write metrics reported from the task thread alone, the consumer-failure flow's
 * windows and backoff, intra-subsystem flush ordering, and the memory-pressure escalation a partial
 * allocation grant produces.
 *
 * ==How determinism is achieved==
 *
 * Nothing here sleeps and nothing here waits on wall time. Every window the writer and its
 * collaborators enforce is measured against an injected [[ManualClock]], so a timing assertion is a
 * statement about a clock reading the test itself advanced. The one exception is the write-time
 * assertion, which needs a monotonic source in order to observe a non-zero elapsed interval and
 * therefore builds its harness on a real clock; it asserts only that the interval is positive,
 * never
 * how large it is.
 *
 * ==Why the fixture is assembled by hand==
 *
 * [[StreamingShuffleManager]] normally assembles the streaming subsystem, binds an ephemeral
 * transport server and rendezvouses with a driver endpoint before it hands a writer back. None of
 * that is needed to exercise the writer, and standing it up would make every assertion here depend
 * on a live cluster. The collaborators are therefore constructed directly, with two seams doing the
 * heavy lifting: [[MemorySpillManager.ExecutorBufferQuota]] takes the executor-memory figure as a
 * function, so the buffer arithmetic is a function of this file's own constants rather than of
 * whatever heap the test JVM was given; and the two driver-facing abstractions the writer depends
 * upon -- [[StreamingShuffleCoordinatorGateway]] and [[StreamingShuffleRouteRegistry]] -- are
 * interfaces, so recording doubles over them make the whole of the failure and degradation flow
 * drivable from a test body.
 *
 * A partition with no subscribed consumer queues nothing, which is exactly the state a map task
 * runs in under the unmodified scheduler, so a writer driven here retains every block it frames and
 * makes
 * it durable at the stop. That is the path these tests exercise.
 */
class StreamingShuffleWriterSuite
  extends SparkFunSuite
    with SharedSparkContext
    with Matchers
    with PrivateMethodTester
    with StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  /** Reduce partitions the default fixture declares. */
  private val defaultPartitions = 4

  /**
   * Executor memory the default fixture derives its buffer budget from.
   *
   * Stated here rather than sampled from the JVM so that every expected figure in this suite is
   * arithmetic over a constant. Sixty-four mebibytes at the default twenty percent leaves a budget
   * comfortably larger than the framing reservation four partitions need, which is what keeps the
   * default fixture on the streaming path instead of degrading it for want of memory.
   */
  private val defaultExecutorMemoryBytes = 64L * 1024L * 1024L

  /** Map id every fixture produces under, unless a test needs to distinguish two generations. */
  private val defaultMapId = 7L

  /** Task attempt id every fixture produces under. */
  private val defaultTaskAttemptId = 21L

  /** The capability token a streaming registration carries; never echoed into a message. */
  private val capabilityToken = "streaming-shuffle-writer-suite-token"

  /** Consumer identity used wherever a registered consumer is needed. */
  private val consumerId = "streaming-shuffle-writer-suite-consumer"

  /** Epoch the recording gateway reports; any non-negative value satisfies the handle's check. */
  private val declaredEpoch = 1L

  /**
   * Shuffle id used by the protocol-level assertions, which build messages directly rather than
   * through a fixture and therefore need an identity of their own.
   */
  private val protocolShuffleId = 3

  /**
   * Records every driver-facing operation a producer performs, and answers all of them.
   *
   * A test double over a production interface rather than a stand-in for missing code: the writer
   * takes its two driver-facing operations as [[StreamingShuffleCoordinatorGateway]] precisely so
   * that the consumer-failure and graceful-degradation flows can be driven without an `RpcEnv`.
   * Every method answers rather than throws, which is the contract the interface documents, and the
   * fallback verdict is latched exactly as the coordinator latches it, so a second declaration
   * returns the state the first one established.
   */
  private class RecordingCoordinatorGateway extends StreamingShuffleCoordinatorGateway {

    private val declared = new mutable.ArrayBuffer[StreamingShuffleFallbackReason]()
    private val invalidated = new mutable.ArrayBuffer[StreamingShuffleInvalidationReason]()
    private val completed = new mutable.ArrayBuffer[StreamingShuffleProducerGeneration]()
    private val heartbeats = new mutable.ArrayBuffer[StreamingShuffleProducerGeneration]()
    private var latched: StreamingShuffleFallbackState = StreamingShuffleFallbackState()

    override def declareFallback(
        shuffleId: Int,
        reason: StreamingShuffleFallbackReason,
        detail: String): StreamingShuffleFallbackState = synchronized {
      declared += reason
      if (!latched.fallenBack) {
        latched = StreamingShuffleFallbackState(reason.toString, declaredEpoch)
      }
      latched
    }

    override def fallbackState(shuffleId: Int): StreamingShuffleFallbackState =
      synchronized(latched)

    override def completeProducer(
        shuffleId: Int,
        generation: StreamingShuffleProducerGeneration): Boolean = synchronized {
      completed += generation
      true
    }

    override def heartbeatProducer(
        shuffleId: Int,
        generation: StreamingShuffleProducerGeneration)
      : Option[StreamingShuffleProducerLiveness] = synchronized {
      heartbeats += generation
      Some(StreamingShuffleProducerLiveness(live = true, coordinatorEpoch = declaredEpoch))
    }

    override def invalidateProducer(
        shuffleId: Int,
        generation: StreamingShuffleProducerGeneration,
        reason: StreamingShuffleInvalidationReason,
        detail: String): Long = synchronized {
      invalidated += reason
      declaredEpoch
    }

    /** Trip conditions this producer declared, in declaration order. */
    def declaredFallbacks: Seq[StreamingShuffleFallbackReason] = synchronized(declared.toSeq)

    /** Withdrawal reasons this producer reported, in report order. */
    def invalidations: Seq[StreamingShuffleInvalidationReason] = synchronized(invalidated.toSeq)

    /** Generations this producer reported as streamed to completion. */
    def completions: Seq[StreamingShuffleProducerGeneration] = synchronized(completed.toSeq)

    /** Generations this producer refreshed liveness for. */
    def heartbeatedGenerations: Seq[StreamingShuffleProducerGeneration] =
      synchronized(heartbeats.toSeq)
  }

  /**
   * Records the routing withdrawals a producer performs on its own executor.
   *
   * The registry exists so that the handler can retire one generation's routing entry without
   * holding the table that owns it. Recording the calls is what lets a cleanup assertion prove the
   * retirement happened rather than infer it.
   */
  private class RecordingRouteRegistry extends StreamingShuffleRouteRegistry {

    private val withdrawn = new mutable.ArrayBuffer[(Int, Long)]()

    override def withdrawRoute(
        shuffleId: Int,
        mapId: Long,
        handler: StreamingShuffleServerHandler): Boolean = synchronized {
      withdrawn += ((shuffleId, mapId))
      true
    }

    /** Shuffle and map pairs whose routing entry was withdrawn, in withdrawal order. */
    def withdrawals: Seq[(Int, Long)] = synchronized(withdrawn.toSeq)
  }

  /**
   * One writer under test together with everything it was built from.
   *
   * The collaborators are reachable individually because most of the contracts this suite asserts
   * are joint properties of the writer and one collaborator -- the budget it streams within, the
   * eviction it observes, the priority it registered -- and reading them from the components bundle
   * the writer itself was handed is what makes the assertion about the object under test rather
   * than
   * about a second copy of it.
   */
  private class WriterHarness(
      val writer: StreamingShuffleWriter[Int, Int, Int],
      val writerConf: SparkConf,
      val context: TaskContextImpl,
      val metrics: RecordingStreamingShuffleWriteMetrics,
      val handle: StreamingShuffleHandle[Int, Int, Int],
      val components: StreamingShuffleWriterComponents,
      val gateway: RecordingCoordinatorGateway,
      val routes: RecordingRouteRegistry,
      val clock: Clock) {

    /** This task's buffer budget, retention window and disk spill. */
    def spillManager: MemorySpillManager = components.spillManager

    /** This task's egress path. */
    def serverHandler: StreamingShuffleServerHandler = components.serverHandler

    /** The executor-scoped registry that publishes this task's retained output. */
    def blockResolver: StreamingShuffleBlockResolver = components.blockResolver

    /** Executor-wide flow control. */
    def backpressure: BackpressureProtocol = components.backpressure

    /** Executor-wide egress pacing. */
    def rateLimiter: TokenBucketRateLimiter = components.rateLimiter

    /** The four graceful-degradation trip conditions. */
    def fallbackPolicy: StreamingShuffleFallbackPolicy = components.fallbackPolicy

    /** The first-error-wins bridge from Netty threads to the task thread. */
    def errorNotifier: StreamingShuffleErrorNotifier = components.errorNotifier

    /** The shuffle this fixture streams. */
    def shuffleId: Int = handle.shuffleId

    /** Every spill file this task's retained output currently occupies. */
    def spillFiles(): Seq[File] = spillManager.allSpilledBlocks.map(_.file).distinct

    /**
     * Completes the task the way the executor would, then releases anything a failed test left
     * behind.
     *
     * Marking the context complete is the honest teardown, because that is what fires the
     * task-completion listeners the writer registered, and those listeners are the mechanism by
     * which "no buffer, channel or spill file survives task completion" is met. The explicit
     * releases that follow are belt and braces for a test that failed before the writer had
     * registered anything; every one of them is idempotent.
     */
    def close(): Unit = {
      try {
        context.markTaskCompleted(None)
      } catch {
        case failure: Throwable =>
          logWarning(s"Ignoring a task completion failure while closing the fixture: $failure")
      } finally {
        try {
          serverHandler.releaseAll()
        } finally {
          try {
            spillManager.close()
          } finally {
            blockResolver.stop()
          }
        }
      }
    }
  }

  /**
   * Assembles one writer and every collaborator it needs.
   *
   * @param numPartitions reduce partitions the shuffle declares, which is the divisor of every
   *                      buffer ceiling and must agree with the dependency's partitioner
   * @param executorMemoryBytes the executor-memory figure the buffer budget is carved from,
   *                            supplied
   *                            as a constant so every expected figure is arithmetic rather than a
   *                            reading of the test JVM's heap
   * @param bufferSizePercent share of that figure the aggregate buffer allowance occupies
   * @param spillThreshold utilisation at which eviction triggers
   * @param mapId map task this writer produces
   * @param taskAttemptId attempt this writer produces under
   * @param attemptNumber attempt of this task; a non-zero value marks a speculative or retried
   *                      attempt, which is what the flush-ordering case varies
   * @param clock time source every collaborator reads, manual by default
   * @return the assembled fixture, whose `close()` the caller owns
   */
  private def newHarness(
      numPartitions: Int = defaultPartitions,
      executorMemoryBytes: Long = defaultExecutorMemoryBytes,
      bufferSizePercent: Int = DefaultBufferSizePercent,
      spillThreshold: Int = DefaultSpillThresholdPercent,
      mapId: Long = defaultMapId,
      taskAttemptId: Long = defaultTaskAttemptId,
      attemptNumber: Int = 0,
      clock: Clock = new ManualClock(ManualClockEpochMillis)): WriterHarness = {
    val writerConf = streamingConfWithOverrides(
      bufferSizePercent = bufferSizePercent,
      spillThreshold = spillThreshold)
    val context = newTaskContext(
      sc.env,
      taskAttemptId = taskAttemptId,
      attemptNumber = attemptNumber,
      numPartitions = numPartitions)
    val metrics = new RecordingStreamingShuffleWriteMetrics
    val dependency = shuffleDependencyFor(sc, writerConf, numPartitions)
    val handle = new StreamingShuffleHandle[Int, Int, Int](
      dependency.shuffleId, dependency, numPartitions, ProtocolVersion, declaredEpoch,
      capabilityToken)
    // The executor-memory figure is injected rather than sampled, which is the whole reason this
    // suite can assert the contracted quotient exactly. The JVM-wide shared quota is deliberately
    // not used: it is derived once per JVM from the real heap and would make every budget assertion
    // depend on whichever suite happened to construct it first.
    val quota = new MemorySpillManager.ExecutorBufferQuota(
      bufferSizePercent, spillThreshold, () => executorMemoryBytes)
    // autoPoll = false so the 100 ms cadence is driven by this suite's clock through pollOnce()
    // rather than by the executor's shared ticker, which would make a timing assertion a race.
    val spillManager = new MemorySpillManager(
      context.taskMemoryManager, writerConf, clock, Some(quota), autoPoll = false)
    // The one-argument constructor deliberately: the writer registers its own retained output, so a
    // root producer is not needed, and leaving it unset means a lookup answers from the
    // registration alone rather than falling back to a producer supplied at construction.
    val blockResolver = new StreamingShuffleBlockResolver(writerConf)
    val egressBudget = new TokenBucketRateLimiter.ExecutorEgressBudget(
      maxBandwidthMBps = None, clock = clock)
    // No coordinator in this process: the protocol documents and guards that case, falling back to
    // the shuffles registered locally when it asks how many an executor is serving.
    val backpressure = new BackpressureProtocol(writerConf, null, egressBudget, clock)
    val rateLimiter = egressBudget.limiterFor(handle.shuffleId)
    val errorNotifier = new StreamingShuffleErrorNotifier(handle.shuffleId, writerConf)
    val routes = new RecordingRouteRegistry
    val serverHandler = new StreamingShuffleServerHandler(writerConf, handle.shuffleId, mapId,
      taskAttemptId, blockResolver, routes, backpressure, rateLimiter, errorNotifier, clock)
    val fallbackPolicy = new StreamingShuffleFallbackPolicy(writerConf, clock)
    val gateway = new RecordingCoordinatorGateway
    val components = StreamingShuffleWriterComponents(backpressure, rateLimiter, spillManager,
      blockResolver, serverHandler, fallbackPolicy, errorNotifier, gateway)
    // A fixture that degraded would silently stop testing the streaming path, so the delegate is a
    // loud failure rather than a working writer. Every test that expects streaming therefore proves
    // it, and the one test that expects degradation asserts on the exception this raises.
    val sortDelegate: () => ShuffleWriter[Int, Int] = () =>
      throw new SparkException("The streaming shuffle writer fixture degraded to the sort-based " +
        "writer, so the streaming path under test was never exercised.")
    val writer = new StreamingShuffleWriter[Int, Int, Int](handle, mapId, context, metrics,
      writerConf, components, sortDelegate, clock)
    new WriterHarness(writer, writerConf, context, metrics, handle, components, gateway, routes,
      clock)
  }

  /**
   * Runs a body against a fresh fixture and closes it, whatever the body does.
   *
   * Every test uses this rather than closing by hand, because a fixture that outlives a failing
   * test
   * holds executor memory and spill files, and the memory-leak detection the test JVM runs with
   * turns that into a failure in whichever test happens to run next.
   *
   * @param harness the fixture to use and then close
   * @param body the test body
   * @tparam T the body's result type
   * @return the body's result
   */
  private def withHarness[T](harness: WriterHarness)(body: WriterHarness => T): T = {
    try {
      body(harness)
    } finally {
      harness.close()
    }
  }

  /**
   * Fills a fixture's buffers, round-robin across its partitions, until utilisation reaches the
   * given percentage of the aggregate allowance.
   *
   * Utilisation is checked before every admission rather than after, so the loop stops as soon as
   * the
   * target is met and never relies on eviction to make room. The bound on the number of admissions
   * is a guard against an arithmetic change turning this into an unbounded loop, and it fails
   * loudly
   * rather than silently returning a half-filled buffer.
   *
   * @param spillManager the store to fill
   * @param nextSequence the next sequence number to admit for each partition, indexed by partition
   *                     id and advanced in place, so a caller may fill, evict and fill again
   *                     without
   *                     breaking the gap-free ascending run each partition requires
   * @param targetPercent utilisation, as a percentage of the aggregate allowance, to reach
   * @param blockBytes payload size of each admitted block
   * @return the number of blocks this call admitted
   */
  private def fillToUtilization(
      spillManager: MemorySpillManager,
      nextSequence: Array[Long],
      targetPercent: Long,
      blockBytes: Int): Int = {
    val admissionLimit = 4096
    var admissions = 0
    var partitionId = 0
    while (spillManager.bufferUtilizationPercent < targetPercent && admissions < admissionLimit) {
      val payload = payloadOfLength(admissions.toLong, blockBytes)
      val admitted = spillManager.bufferBlock(partitionId, nextSequence(partitionId), payload)
      assert(admitted,
        s"Admission of block ${nextSequence(partitionId)} to partition $partitionId was refused " +
          s"at ${spillManager.bufferUtilizationPercent}% utilisation, below the $targetPercent% " +
          s"this fixture is filling to; buffered ${spillManager.bufferedBytes} of " +
          s"${spillManager.totalBudgetBytes} budgeted bytes")
      nextSequence(partitionId) += 1L
      admissions += 1
      partitionId = (partitionId + 1) % nextSequence.length
    }
    assert(admissions < admissionLimit,
      s"Filling to $targetPercent% utilisation took $admissions admissions of $blockBytes bytes " +
        "without reaching the target, which means the budget arithmetic has changed; buffered " +
        s"${spillManager.bufferedBytes} of ${spillManager.totalBudgetBytes} budgeted bytes")
    admissions
  }

  test("buffer allocation with partition memory tracking") {
    val partitions = 8
    val executorMemory = 128L * 1024L * 1024L
    withHarness(newHarness(numPartitions = partitions, executorMemoryBytes = executorMemory)) {
      harness =>
        val spillManager = harness.spillManager
        spillManager.registerPartitionCount(partitions)

        // The aggregate allowance is bufferSizePercent of executor memory, and the default is 20.
        assert(DefaultBufferSizePercent === 20,
          "The specified default share of executor memory reserved for streaming buffers is 20%")
        val expectedTotal = executorMemory / PercentScale * DefaultBufferSizePercent.toLong
        assert(spillManager.totalBudgetBytes === expectedTotal,
          s"The aggregate buffer allowance must be $DefaultBufferSizePercent% of the " +
            s"$executorMemory byte executor memory region, i.e. $expectedTotal bytes")
        assert(harness.writer.totalBufferBudgetBytes === expectedTotal,
          "The writer must stream within the same aggregate allowance the spill manager publishes")

        // The per-partition allowance is exactly (executorMemory * bufferPercent) / numPartitions.
        val expectedPerPartition =
          perPartitionBudgetBytes(executorMemory, DefaultBufferSizePercent, partitions)
        assert(spillManager.perPartitionBudgetBytes === expectedPerPartition,
          s"The per-partition allowance must be the contracted quotient, i.e. " +
            s"$expectedPerPartition bytes across $partitions partitions")
        assert(harness.writer.perPartitionBufferBudgetBytes === expectedPerPartition,
          "The writer must divide the aggregate allowance by exactly the declared partition count")
        assert(expectedPerPartition * partitions.toLong <= expectedTotal,
          "The per-partition allowances must not sum to more than the aggregate allowance")

        // Allocation is tracked per partition: two partitions holding one identically sized block
        // each are charged identically, a partition holding nothing is charged nothing, and the
        // aggregate is their sum.
        val blockBytes = 64 * 1024
        val perBlockCharge = blockBytes.toLong + MemorySpillManager.PER_BLOCK_OVERHEAD_BYTES
        assert(spillManager.bufferBlock(0, 0L, payloadOfLength(1L, blockBytes)),
          "The first block of partition 0 must be admissible into an empty allowance")
        assert(spillManager.bufferBlock(1, 0L, payloadOfLength(2L, blockBytes)),
          "The first block of partition 1 must be admissible into an empty allowance")
        assert(spillManager.bufferedBytesFor(0) === perBlockCharge,
          s"Partition 0 must be charged its payload plus the per-block overhead, i.e. " +
            s"$perBlockCharge bytes")
        assert(spillManager.bufferedBytesFor(1) === spillManager.bufferedBytesFor(0),
          "Two partitions holding one identically sized block each must be charged identically")
        assert(spillManager.bufferedBytesFor(2) === 0L,
          "A partition that holds nothing must be charged nothing")
        assert(spillManager.bufferedBytes ===
            spillManager.bufferedBytesFor(0) + spillManager.bufferedBytesFor(1),
          "The aggregate reservation must be the sum of the per-partition reservations")
        assert(spillManager.retainedBlockCount(0) === 1,
          "Partition 0 must retain exactly the one block it was handed")

        // Aggregate usage never exceeds the allowance, and no partition exceeds its own share, even
        // when the store is driven all the way to the spill threshold.
        val nextSequence = Array.fill(partitions)(0L)
        nextSequence(0) = 1L
        nextSequence(1) = 1L
        fillToUtilization(spillManager, nextSequence, DefaultSpillThresholdPercent.toLong,
          blockBytes)
        assert(spillManager.executorReservedBytes <= expectedTotal,
          s"Aggregate buffer usage of ${spillManager.executorReservedBytes} bytes must never " +
            s"exceed the $expectedTotal byte allowance")
        assert(spillManager.bufferedBytes <= expectedTotal,
          "Buffered bytes must never exceed the aggregate allowance")
        (0 until partitions).foreach { partitionId =>
          assert(spillManager.bufferedBytesFor(partitionId) <= expectedPerPartition,
            s"Partition $partitionId held ${spillManager.bufferedBytesFor(partitionId)} bytes, " +
              s"more than its $expectedPerPartition byte allowance")
        }

        // A degenerate partition count is guarded rather than divided by.
        intercept[IllegalArgumentException] {
          spillManager.perPartitionBudgetBytesFor(0)
        }
        intercept[IllegalArgumentException] {
          new StreamingShuffleHandle[Int, Int, Int](harness.shuffleId, harness.handle.dependency, 0,
            ProtocolVersion, declaredEpoch, capabilityToken)
        }
    }
  }

  test("spill trigger at 80% with timing validation") {
    val partitions = 2
    val executorMemory = 2L * 1024L * 1024L
    val bufferPercent = 50
    val clock = newManualClock()
    withHarness(newHarness(numPartitions = partitions, executorMemoryBytes = executorMemory,
        bufferSizePercent = bufferPercent, clock = clock)) { harness =>
      val spillManager = harness.spillManager
      spillManager.registerPartitionCount(partitions)

      assert(DefaultSpillThresholdPercent === 80,
        "The specified default utilisation at which streaming shuffle spills is 80%")
      assert(SpillPollIntervalMillis === 100L,
        "The specified threshold polling cadence is 100 ms")
      assert(ReclamationDeadlineMillis === 100L,
        "The specified buffer reclamation deadline is 100 ms after an acknowledgement")
      val total = spillManager.totalBudgetBytes
      assert(total === executorMemory / PercentScale * bufferPercent.toLong,
        s"The aggregate allowance must be $bufferPercent% of $executorMemory bytes")
      assert(spillManager.spillThresholdBytes ===
          total / PercentScale * DefaultSpillThresholdPercent.toLong,
        s"The spill trigger must be $DefaultSpillThresholdPercent% of the $total byte allowance")
      assert(spillManager.spillCount === 0L, "Nothing may have spilled before anything is buffered")

      val blockBytes = 64 * 1024
      val nextSequence = Array.fill(partitions)(0L)
      fillToUtilization(spillManager, nextSequence, DefaultSpillThresholdPercent.toLong, blockBytes)
      assert(spillManager.bufferUtilizationPercent >= DefaultSpillThresholdPercent.toLong,
        "The fixture must have driven utilisation to the spill threshold before polling")

      // Nothing has polled yet, so the first poll is due and must evict.
      assert(spillManager.pollOnce(),
        "The first threshold poll must be due and must evict at or above the spill threshold")
      assert(spillManager.spillCount === 1L, "Exactly one threshold spill event must be counted")
      assert(spillManager.bufferUtilizationPercent < DefaultSpillThresholdPercent.toLong,
        "Eviction must bring utilisation back below the spill threshold")
      assert(spillManager.allSpilledBlocks.nonEmpty,
        "Eviction must have written at least one block to local disk")
      assert(spillManager.spillSelectionOrder.forall(id => id >= 0 && id < partitions),
        "The eviction order must only name partitions of this shuffle")
      assert(spillManager.lastSpillDurationMs >= 0L,
        "The achieved spill latency must be recorded rather than left unset")

      // The cadence is measured on the injected clock: no work inside 100 ms, work at exactly 100.
      // Refilling can itself evict, because an admission that meets a partition's ceiling reclaims
      // room rather than being refused, so the baseline is read after the refill; what the cadence
      // governs is which POLLS do work, and that is what is asserted.
      fillToUtilization(spillManager, nextSequence, DefaultSpillThresholdPercent.toLong, blockBytes)
      val spillsBeforeCadence = spillManager.spillCount
      assert(!spillManager.pollOnce(),
        "A poll taken without advancing the clock is inside the 100 ms cadence and must do nothing")
      assert(spillManager.spillCount === spillsBeforeCadence,
        "A suppressed poll must not count a spill event")
      clock.advance(SpillPollIntervalMillis - 1L)
      assert(!spillManager.pollOnce(),
        s"A poll at ${SpillPollIntervalMillis - 1L} ms is still inside the cadence and must do " +
          "nothing")
      assert(spillManager.spillCount === spillsBeforeCadence,
        "A poll one millisecond early must not spill")
      clock.advance(1L)
      assert(spillManager.pollOnce(),
        s"A poll at exactly $SpillPollIntervalMillis ms must be due and must evict")
      assert(spillManager.spillCount === spillsBeforeCadence + 1L,
        "The due poll must count exactly one further spill event")

      // Reclamation completes inside the 100 ms bound of a consumer acknowledgement.
      assert(spillManager.registerConsumer(consumerId),
        "A store that still serves its retained output must admit a consumer")
      val freshSequence = nextSequence(0)
      assert(spillManager.bufferBlock(0, freshSequence, payloadOfLength(99L, blockBytes)),
        "A block admitted after eviction must be resident in memory")
      nextSequence(0) += 1L
      val released = spillManager.acknowledge(consumerId, 0, freshSequence)
      assert(released > 0L,
        s"Acknowledging through sequence $freshSequence must release the memory it held")
      assert(spillManager.lastReclamationDurationMs <= ReclamationDeadlineMillis,
        s"Reclamation took ${spillManager.lastReclamationDurationMs} ms, outside the " +
          s"$ReclamationDeadlineMillis ms bound")
      assert(spillManager.reclamationDeadlineBreaches === 0L,
        "No reclamation may be recorded as having overrun its deadline")

      // The MemoryConsumer pressure callback. A request this consumer triggered itself is declined,
      // which is the guard Spark's own spillable collections use against recursing into an eviction
      // while mid-reservation, and a request from any other consumer is honoured in full.
      val pressureSequence = nextSequence(0)
      assert(spillManager.bufferBlock(0, pressureSequence, payloadOfLength(101L, blockBytes)),
        "A block must be resident before the pressure callback is exercised")
      nextSequence(0) += 1L
      val bufferedBeforePressure = spillManager.bufferedBytes
      assert(bufferedBeforePressure > 0L, "The store must hold memory to be asked to release")
      spillManager.spill()
      assert(spillManager.bufferedBytes === bufferedBeforePressure,
        "A spill request a consumer triggered itself must be declined, because a consumer must " +
          "not recurse into its own eviction while it is mid-reservation")
      val neighbourQuota = new MemorySpillManager.ExecutorBufferQuota(
        bufferPercent, DefaultSpillThresholdPercent, () => executorMemory)
      val neighbour = new MemorySpillManager(harness.context.taskMemoryManager, harness.writerConf,
        clock, Some(neighbourQuota), autoPoll = false)
      try {
        assert(spillManager.spill(Long.MaxValue, neighbour) > 0L,
          "A pressure request from another consumer must release memory to disk")
        assert(spillManager.bufferedBytes < bufferedBeforePressure,
          "The honoured pressure request must have moved bytes out of memory")
      } finally {
        neighbour.close()
      }
      assert(spillManager.spillFailureCount === 0L, "No spill may have failed")
    }
  }

  test("checksum generation") {
    // The protocol's own contract: CRC32C, a 2 MB payload cap accepted exactly and rejected one
    // byte later, and a version byte an explicit compatibility check can read.
    assert(ChecksumAlgorithm === "CRC32C", "The specified block checksum algorithm is CRC32C")
    assert(MaxBlockSizeBytes === 2097152,
      "The specified block payload cap is 2 MB, i.e. 2097152 bytes")
    assert(ProtocolVersion === 1.toByte, "The current streaming protocol version is 1")

    val payload = payloadOfLength(7L, 4096)
    val plain = StreamingShuffleChecksum.compute(payload)
    assert(StreamingShuffleChecksum.verify(payload, plain),
      "A payload must verify against the checksum computed over it")
    assert(!StreamingShuffleChecksum.verify(payload, plain + 1L),
      "A payload must not verify against a checksum that is not its own")
    assert(StreamingShuffleChecksum.compute(payload) === plain,
      "The checksum helper must be repeatable, which it is because it uses a fresh CRC32C per call")

    // The block checksum binds the bytes to the stream identity, so a block delivered against the
    // wrong partition fails verification even with its payload intact.
    val block = dataBlock(protocolShuffleId, defaultMapId, 0, 0L, payload)
    assert(block.verifyChecksum(), "A correctly stamped block must verify")
    assert(block.checksum() ===
        blockChecksum(protocolShuffleId, defaultMapId, 0, 0L, payload),
      "A block's stamp must be the identity-bound CRC32C the producer computes")
    assert(block.checksum() !==
        blockChecksum(protocolShuffleId, defaultMapId, 1, 0L, payload),
      "A block's stamp must cover its partition, so the same bytes on another stream differ")
    assert(block.checksum() !==
        blockChecksum(protocolShuffleId, defaultMapId, 0, 1L, payload),
      "A block's stamp must cover its sequence number")
    assert(!corruptedDataBlock(protocolShuffleId, defaultMapId, 0, 0L, payload)
        .verifyChecksum(),
      "A block whose stamp was corrupted must fail verification")

    // Both sides of the size boundary: the comparison is strictly greater than, so exactly the cap
    // is accepted and one byte more is refused.
    val atCap = maximumSizedPayload(11L)
    assert(atCap.length === MaxBlockSizeBytes, "The boundary payload must be exactly at the cap")
    val capped = DataBlockMessage.withComputedChecksum(
      protocolShuffleId, defaultMapId, 0, 0L, atCap)
    assert(capped.payloadLength() === MaxBlockSizeBytes,
      s"A payload of exactly $MaxBlockSizeBytes bytes must be accepted")
    assert(capped.verifyChecksum(), "A maximum sized block must carry a verifiable checksum")
    assert(capped.encodedLength() === dataBlockEncodedLength(MaxBlockSizeBytes),
      "A data block's encoded length must be its payload plus the protocol's framing overhead")
    assert(framedLength(capped.encodedLength()) ===
        capped.encodedLength() + FrameTypePrefixLength,
      "A framed message is always its encoded length plus the one-byte type discriminator")
    val oversized = oversizedPayload(11L)
    assert(oversized.length === MaxBlockSizeBytes + 1,
      "The rejecting payload must be exactly one byte over the cap")
    intercept[IllegalArgumentException] {
      DataBlockMessage.withComputedChecksum(
        protocolShuffleId, defaultMapId, 0, 0L, oversized)
    }

    // Three of the five message types encode to the same number of bytes, so a discriminator must
    // read the framing type byte or the concrete class and never the length.
    val fixedMessages = Seq(
      ack(protocolShuffleId, defaultMapId, 0, 0L, 0L),
      retransmitRequest(protocolShuffleId, defaultMapId, 0, 0L, 0L),
      streamTermination(protocolShuffleId, defaultMapId, 0, 1L))
    assert(fixedMessages.forall(_.encodedLength() == FixedMessageEncodedLength),
      s"Every fixed-size streaming message must encode to $FixedMessageEncodedLength bytes")
    assert(fixedMessages.map(typeOf).distinct.size === fixedMessages.size,
      "Fixed-size messages of equal length must still be distinguished by their type discriminator")

    // Every block the writer frames respects the cap and carries a verifiable identity-bound stamp.
    val partitions = 4
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      harness.writer.write(deterministicRecords(400, seed = 4L, keySpace = 48).iterator)
      assert(harness.writer.blocksStreamed > 0L, "The writer must have framed at least one block")
      assert(harness.writer.blockPayloadCapacityBytes > 0,
        "The writer must have derived a positive framing capacity")
      assert(harness.writer.blockPayloadCapacityBytes <= MaxBlockSizeBytes,
        "The writer must never frame above the protocol's block payload cap")
      var verified = 0
      harness.writer.streamedPartitions.foreach { partitionId =>
        val blocks = harness.writer.nextSequenceNumberFor(partitionId)
        assert(blocks > 0L, s"A streamed partition $partitionId must have framed a block")
        var sequence = 0L
        while (sequence < blocks) {
          val retained = harness.spillManager.retainedPayload(partitionId, sequence)
          assert(retained.isDefined,
            s"Block $sequence of partition $partitionId must still be retained before the stop")
          val bytes = retained.get
          assert(bytes.length <= harness.writer.blockPayloadCapacityBytes,
            s"Block $sequence of partition $partitionId carried ${bytes.length} bytes, above the " +
              s"${harness.writer.blockPayloadCapacityBytes} byte framing capacity")
          val framed = DataBlockMessage.withComputedChecksum(
            harness.shuffleId, defaultMapId, partitionId, sequence, bytes)
          assert(framed.verifyChecksum(),
            s"Block $sequence of partition $partitionId must carry a verifiable CRC32C")
          assert(framed.checksum() ===
              blockChecksum(harness.shuffleId, defaultMapId, partitionId, sequence, bytes),
            s"Block $sequence of partition $partitionId must be stamped through the shared helper")
          verified += 1
          sequence += 1L
        }
      }
      assert(verified > 0, "At least one framed block must have been checksum verified")
    }
  }

  test("producer failure cleanup and resource reclamation") {
    val partitions = 4
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      harness.writer.write(deterministicRecords(512, seed = 3L, keySpace = 64).iterator)
      assert(harness.writer.recordsStreamed === 512L, "Every record handed over must be streamed")

      // Make the retained window durable so that the cleanup has files to unlink as well as memory
      // to release. The no-argument MemoryConsumer seam is deliberately not used here: it triggers
      // itself, and a self-triggered request is declined by design.
      val movedBytes = harness.spillManager.spillAllRetained()
      assert(movedBytes > 0L, "Making the retained window durable must move bytes to disk")
      val files = harness.spillFiles()
      assert(files.nonEmpty, "The durability flush must have produced at least one spill file")
      assert(files.forall(_.exists()), "Every spill file must exist before the failure")

      // The bytes a consumer would have taken are wrapped so that their release accounting is
      // proved rather than assumed. What this asserts is the reference discipline over the retained
      // payloads themselves: each buffer is taken exactly once and dropped exactly once, so a
      // reference that outlived its use would be visible here rather than only as a leak later.
      val buffers = harness.writer.streamedPartitions.flatMap { partitionId =>
        val blocks = harness.writer.nextSequenceNumberFor(partitionId)
        (0L until blocks).flatMap { sequence =>
          harness.spillManager.retainedPayload(partitionId, sequence).map { payload =>
            val buffer = recordingBuffer(payload)
            buffer.retain()
            buffer.release()
            buffer
          }
        }
      }
      assert(buffers.nonEmpty, "The retained window must have handed out at least one buffer")
      assertBuffersReleasedExactlyOnce(buffers)

      val status = harness.writer.stop(success = false)
      assert(status.isEmpty, "An unsuccessful stop must report no map status")
      assert(harness.writer.isStopped, "The writer must record that it has been stopped")
      assert(harness.writer.producedMapStatus.isEmpty,
        "A failed attempt must never publish a map status")

      // Every owner of the failed generation has been retired, driver first.
      assert(harness.gateway.invalidations.contains(
          StreamingShuffleInvalidationReason.IncompleteStream),
        "A failing producer must withdraw its generation from the driver as an incomplete stream")
      assert(harness.routes.withdrawals.contains((harness.shuffleId, defaultMapId)),
        "A failing producer must withdraw its own executor's routing entry")
      assert(harness.blockResolver.registeredGeneration(harness.shuffleId, defaultMapId).isEmpty,
        "A failing producer's generation must no longer be registered with the resolver")
      assert(harness.blockResolver.producerFor(harness.shuffleId, defaultMapId).isEmpty,
        "A failing producer's retained output must no longer be reachable through the resolver")
      assert(harness.blockResolver.registeredProducerCount === 0,
        "No producer may remain registered once the only generation has been withdrawn")

      // Every buffer released and every spill file deleted.
      assert(harness.spillManager.isClosed, "The buffer store must be closed by the failure path")
      assert(harness.spillManager.bufferedBytes === 0L, "No buffered byte may survive the failure")
      assert(harness.spillManager.scratchBytes === 0L,
        "No framing scratch reservation may survive the failure")
      assert(harness.spillManager.executorReservedBytes === 0L,
        "The executor-wide allowance must have every byte of this task's reservation back")
      assert(harness.spillManager.getUsed() === 0L,
        s"The memory consumer still holds ${harness.spillManager.getUsed()} bytes of task memory")
      assert(files.forall(file => !file.exists()),
        s"Spill files survived the failure: ${files.filter(_.exists()).mkString(", ")}")
      assert(harness.spillManager.spillFileDeletionFailures === 0L,
        "No spill file deletion may have failed")

      // The producer never manufactures the reader's failure signal.
      assert(harness.errorNotifier.fetchFailure.isEmpty,
        "A producer must not construct a fetch failure, which belongs to the reading side")
    }
  }

  test("a successful stop returns a non-empty map status honouring the non-zero size invariant") {
    val partitions = 4
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      harness.writer.write(deterministicRecords(256, seed = 5L, keySpace = 32).iterator)
      val status = harness.writer.stop(success = true)
      // ShuffleWriteProcessor dereferences this unconditionally, outside its own isDefined guard,
      // so returning None on success would throw before the task could report anything.
      assert(status.isDefined,
        "A successful stop must return a non-empty map status, because the shared shuffle write " +
          "path dereferences it unconditionally")
      val mapStatus = status.get
      assert(mapStatus.mapId === defaultMapId,
        "The placeholder map status must carry the map id this writer produced")
      assert(mapStatus.location === sc.env.blockManager.shuffleServerId,
        "The placeholder map status must name this executor's shuffle server, which is the " +
          "identity MapOutputTracker matches when a fetch failure asks it to remove the output")
      val lengths = harness.writer.getPartitionLengths()
      assert(lengths.exists(_ > 0L),
        "The fixture must have streamed bytes to at least one partition")
      lengths.indices.foreach { partitionId =>
        if (lengths(partitionId) > 0L) {
          // MapStatus documents this as necessary for correctness, because block fetchers are
          // allowed to skip a partition whose reported size is zero.
          assert(mapStatus.getSizeForBlock(partitionId) > 0L,
            s"Partition $partitionId carried ${lengths(partitionId)} bytes, so its reported size " +
              "must be non-zero or a block fetcher is entitled to skip it")
        } else {
          assert(mapStatus.getSizeForBlock(partitionId) === 0L,
            s"Partition $partitionId carried nothing, so its reported size must be zero")
        }
      }
      assert(harness.writer.producedMapStatus.contains(mapStatus),
        "The writer must retain the status it produced")
      assert(harness.gateway.completions.exists(_.mapId == defaultMapId),
        "A successful producer must report its map output complete to the coordinator")
      assertNoPublishedFailure(harness.errorNotifier, "A successful streaming map task")
    }
  }

  test("a second stop is a no-op that returns None") {
    withHarness(newHarness()) { harness =>
      harness.writer.write(deterministicRecords(64, seed = 6L, keySpace = 16).iterator)
      assert(harness.writer.stop(success = true).isDefined, "The first stop must succeed")
      val bytesAfterFirst = harness.metrics.bytesWritten
      val recordsAfterFirst = harness.metrics.recordsWritten
      // The shared write path calls stop(success = true) and then, from its catch block,
      // stop(success = false); a repeat of either must not report the same bytes twice or delete
      // anything twice.
      assert(harness.writer.stop(success = true).isEmpty,
        "A repeated successful stop must be a no-op returning None")
      assert(harness.metrics.bytesWritten === bytesAfterFirst,
        "A repeated stop must not republish written bytes")
      assert(harness.metrics.recordsWritten === recordsAfterFirst,
        "A repeated stop must not republish written records")
      // Failing after a successful stop withdraws the output, because a consumer must not read the
      // output of an attempt that did not succeed.
      assert(harness.writer.stop(success = false).isEmpty,
        "Failing after a successful stop must return None")
      assert(harness.spillManager.isClosed,
        "Failing after a successful stop must release everything the attempt held")
      assert(harness.writer.stop(success = false).isEmpty,
        "A repeated unsuccessful stop must be a no-op returning None")
      // One writer streams one record iterator exactly once.
      intercept[IllegalStateException] {
        harness.writer.write(Iterator.empty)
      }
    }
  }

  test("getPartitionLengths reports one non-null length per declared partition") {
    val partitions = 5
    withHarness(newHarness(numPartitions = partitions)) { harness =>
      val before = harness.writer.getPartitionLengths()
      assert(before != null, "The partition length vector must never be null, even before a write")
      assert(before.length === partitions,
        s"The partition length vector must have one entry per declared partition, i.e. $partitions")
      assert(before.forall(_ == 0L), "Nothing may be reported as written before a write")

      harness.writer.write(deterministicRecords(200, seed = 7L, keySpace = 40).iterator)
      val after = harness.writer.getPartitionLengths()
      assert(after != null, "The partition length vector must never be null after a write")
      assert(after.length === partitions,
        "A write must not change the length of the partition length vector")
      assert(after.forall(_ >= 0L), "No partition may report a negative number of bytes")
      assert(after.sum === harness.writer.payloadBytesStreamed,
        s"The reported lengths must sum to the ${harness.writer.payloadBytesStreamed} payload " +
          "bytes this writer streamed")
      harness.writer.streamedPartitions.foreach { partitionId =>
        assert(after(partitionId) > 0L,
          s"Partition $partitionId was streamed to, so it must report bytes")
      }
      assert(harness.writer.stop(success = true).isDefined, "The stop must succeed")
      assert(harness.writer.getPartitionLengths().sameElements(after),
        "The stop must not change the lengths the write reported")
    }
  }

  test("write metrics reach the task thread only and are not inflated by a spill") {
    // A real clock, not a manual one: the elapsed write interval is measured with nanoTime, and a
    // clock the test never advances would legitimately report zero. Only the sign is asserted.
    val harness = newHarness(clock = wallClock())
    try {
      val records = deterministicRecords(512, seed = 8L, keySpace = 64)
      harness.writer.write(records.iterator)
      assert(harness.writer.stop(success = true).isDefined, "The stop must succeed")
      // Completing the task is what fires the listeners that publish spill volume, so the task's
      // own accumulators are read after it.
      harness.close()

      val metrics = harness.metrics
      assert(metrics.recordsWritten === records.size.toLong,
        s"The write reporter must have been told about all ${records.size} records, or the Web " +
          "UI, the history server and the metrics REST API stay blank for streaming shuffles")
      assert(metrics.bytesWritten === harness.writer.payloadBytesStreamed,
        "The write reporter must count payload bytes, exactly as the partition lengths do")
      assert(metrics.writeTime > 0L,
        "The write reporter must have been told about a positive elapsed write interval")
      assert(metrics.bytesDecremented === 0L,
        "Nothing on the streaming write path decrements written bytes")
      assert(metrics.recordsDecremented === 0L,
        "Nothing on the streaming write path decrements written records")
      assertSingleThreadedReporting(metrics.foreignThreadCallCount,
        "The streaming shuffle write reporter")
      assert(metrics.reportingThread.contains(Thread.currentThread()),
        "Every reporter call must arrive on the task thread; a Netty event-loop thread may only " +
          "enqueue, because the reporter contract permits implementations not to synchronize")

      // Spill volume lands on the accumulators Spark already has, and never on the task's own
      // shuffle-write reporter, which the spill path must not touch at all.
      val taskMetrics = harness.context.taskMetrics
      assert(taskMetrics.diskBytesSpilled > 0L,
        "Making the retained window durable must be reported on the existing diskBytesSpilled " +
          "accumulator rather than on a parallel counter of the streaming subsystem's own")
      assert(taskMetrics.peakExecutionMemory > 0L,
        "The peak buffer reservation must be reported on the existing peakExecutionMemory " +
          "accumulator")
      assert(taskMetrics.shuffleWriteMetrics.bytesWritten === 0L,
        "The spill path must write to a fresh ShuffleWriteMetrics of its own; a non-zero reading " +
          "here would mean spilled bytes were also counted as shuffle-written bytes")
      assert(metrics.bytesWritten === harness.writer.payloadBytesStreamed,
        s"A spill of ${taskMetrics.diskBytesSpilled} bytes must not inflate the " +
          s"${metrics.bytesWritten} payload bytes reported as written")
    } finally {
      harness.close()
    }
  }

  test("the consumer failure flow arms at ten seconds and backs off exponentially") {
    // The contracted windows and the whole backoff ladder, read from the components that own them.
    assert(ProducerConnectionTimeoutMillis === 5000L,
      "The specified producer connection timeout is 5 s")
    assert(JustBeforeProducerTimeoutMillis === 4999L,
      "The non-firing boundary of the producer timeout is 4999 ms")
    assert(ConsumerLivenessTimeoutMillis === 10000L,
      "The specified consumer liveness window is 10 s")
    assert(JustBeforeConsumerLivenessMillis === 9999L,
      "The non-firing boundary of the consumer liveness window is 9999 ms")
    assert(RetryBaseBackoffMillis === 1000L, "The specified retry backoff starts at 1 s")
    assert(MaxRetryAttempts === 5, "The specified retry budget is 5 attempts")
    assert(RetryBackoffLadderMillis === Seq(1000L, 2000L, 4000L, 8000L, 16000L),
      s"The backoff ladder must double from one second for five attempts, but was " +
        s"${RetryBackoffLadderMillis.mkString(", ")}")
    assert(StreamingShuffleWriter.CONSUMER_LIVENESS_TIMEOUT_MS === ConsumerLivenessTimeoutMillis,
      "The writer must enforce the same consumer liveness window the protocol defines")
    assert(StreamingShuffleWriter.REPLAY_BASE_BACKOFF_MS === RetryBaseBackoffMillis,
      "The writer must replay on the same base backoff the protocol defines")
    assert(StreamingShuffleWriter.MAX_REPLAY_ATTEMPTS === MaxRetryAttempts,
      "The writer must bound replay at the same attempt count the protocol defines")
    assert(StreamingShuffleWriter.MAX_REPLAY_BACKOFF_MS === 16000L,
      "The last rung of the ladder is 16 s, which is where the writer's backoff is capped")

    val clock = newManualClock()
    withHarness(newHarness(clock = clock)) { harness =>
      val backpressure = harness.backpressure
      val key = BackpressureStreamKey.forProducer(harness.shuffleId, defaultMapId,
        defaultTaskAttemptId, partitionId = 0, consumerId = consumerId)
      assert(backpressure.registerStream(key, MaxEncodedFrameBytes.toLong),
        "Registering a producer ledger for the first time must open it")
      val blockBytes = 4096L
      assert(backpressure.tryAdmit(key, blockBytes, 0L),
        "A block within credit must be admitted for sending")
      assert(backpressure.outstandingBytes(key) === blockBytes,
        "An admitted block's bytes must be outstanding until they are acknowledged")

      // The window arms only while something is outstanding, and it does not fire one millisecond
      // short of ten seconds.
      advanceJustBeforeConsumerLivenessTimeout(clock)
      assert(!backpressure.isConsumerTimedOut(key),
        s"The consumer liveness window must not fire at $JustBeforeConsumerLivenessMillis ms")
      clock.advance(1L)
      assert(backpressure.isConsumerTimedOut(key),
        s"The consumer liveness window must fire at exactly $ConsumerLivenessTimeoutMillis ms")
      assert(backpressure.timedOutConsumerStreams.contains(key),
        "A timed out stream must be reported in the set the writer polls")

      // The unacknowledged window is retained, inclusive at both ends, and is the only thing that
      // may be replayed; a request outside it can never be served.
      assert(backpressure.unacknowledgedWindow(key).contains((0L, 0L)),
        "A single-element unacknowledged window is valid and inclusive at both ends")
      assert(backpressure.unacknowledgedBlockCount(key) === 1,
        "Exactly the one unacknowledged block must be counted")
      assert(backpressure.isWithinUnacknowledgedWindow(key, 0L),
        "The one block sent must be inside the unacknowledged window")
      assert(!backpressure.isWithinUnacknowledgedWindow(key, 1L),
        "A block that was never sent cannot be inside the unacknowledged window")
      assert(backpressure.canServeRetransmit(key,
          retransmitRequest(harness.shuffleId, defaultMapId, 0, 0L, 0L)),
        "A retransmission scoped to the unacknowledged window must be serviceable")
      assert(!backpressure.canServeRetransmit(key,
          retransmitRequest(harness.shuffleId, defaultMapId, 0, 1L, 1L)),
        "A retransmission outside the unacknowledged window must be refused, because those bytes " +
          "were released the moment the consumer acknowledged them")

      // An acknowledgement disarms the detector and releases the window, which is what lets the
      // producer reclaim the memory it was holding.
      assert(backpressure.tryAcknowledge(key, 0L).isDefined,
        "An acknowledgement inside the sent range must be applied rather than refused")
      assert(backpressure.outstandingBytes(key) === 0L,
        "An acknowledgement must release the bytes it covers")
      assert(!backpressure.isConsumerTimedOut(key),
        "A stream with nothing outstanding is idle rather than timed out")

      // Retention plus spill: the unacknowledged window survives eviction, moving to disk instead
      // of being discarded, which is what makes replay possible without a stage recomputation.
      val spillManager = harness.spillManager
      spillManager.registerPartitionCount(defaultPartitions)
      val nextSequence = Array.fill(defaultPartitions)(0L)
      fillToUtilization(spillManager, nextSequence, DefaultSpillThresholdPercent.toLong, 64 * 1024)
      assert(spillManager.maybeSpill(),
        "Utilisation at or above the threshold must evict the largest buffered partitions")
      val spilled = spillManager.allSpilledBlocks
      assert(spilled.nonEmpty, "Eviction must have moved blocks to disk")
      spilled.foreach { block =>
        assert(spillManager.retainsBlock(block.partitionId, block.sequenceNumber),
          s"Block ${block.sequenceNumber} of partition ${block.partitionId} must still be " +
            "retained after eviction, because an unacknowledged block is replayable from disk")
        assert(spillManager.spilledBlock(block.partitionId, block.sequenceNumber).isDefined,
          s"Block ${block.sequenceNumber} of partition ${block.partitionId} must be locatable on " +
            "disk after eviction")
      }
    }
  }

  test("the writer never raises a fetch failure and never touches channel auto-read") {
    val writerClass = classOf[StreamingShuffleWriter[Int, Int, Int]]
    val referencedTypes = writerClass.getDeclaredFields.map(_.getType.getName).toSet ++
      writerClass.getDeclaredMethods.flatMap { method =>
        method.getReturnType.getName +: method.getParameterTypes.map(_.getName).toSeq
      }.toSet
    assert(!referencedTypes.contains(classOf[FetchFailedException].getName),
      s"The streaming writer must not mention ${classOf[FetchFailedException].getName}: a fetch " +
        "failure is the reading side's signal, and constructing one on the producer side would " +
        "mark a fetch failure against a task that is not fetching")
    assert(!referencedTypes.exists(_.startsWith("io.netty")),
      s"The streaming writer must not name a Netty type, but named " +
        s"${referencedTypes.filter(_.startsWith("io.netty")).mkString(", ")}; channel level flow " +
        "control belongs to the two channel handlers alone")
    val autoReadMethods = writerClass.getDeclaredMethods
      .map(_.getName)
      .filter(name => name.contains("setAutoRead") || name.contains("autoRead"))
    assert(autoReadMethods.isEmpty,
      s"The streaming writer must not touch channel auto-read, but declares " +
        s"${autoReadMethods.mkString(", ")}; toggling it belongs to the consumer side handler")

    // The behavioural half of the same statement: a complete streaming map task publishes no
    // failure of any kind, and in particular no fetch failure.
    withHarness(newHarness()) { harness =>
      harness.writer.write(deterministicRecords(128, seed = 10L, keySpace = 24).iterator)
      assert(harness.writer.stop(success = true).isDefined, "The stop must succeed")
      assert(harness.errorNotifier.fetchFailure.isEmpty,
        "A streaming producer must never latch a fetch failure")
      assertNoPublishedFailure(harness.errorNotifier, "A complete streaming map task")
    }
  }

  test("flush ordering prefers the original attempt over a speculative one") {
    // The shared task context fixture hardcodes attempt number zero, which is why the flush
    // ordering case builds its context directly instead.
    assert(fakeTaskContext(sc).attemptNumber() === 0,
      "The shared task context fixture reports attempt number zero, so it cannot express a " +
        "speculative attempt")

    withHarness(newHarness(taskAttemptId = 31L, attemptNumber = 0)) { original =>
      withHarness(newHarness(taskAttemptId = 32L, attemptNumber = 2)) { speculative =>
        assert(original.serverHandler.taskPriority ===
            StreamingShuffleServerHandler.DEFAULT_PRIORITY,
          "Before a task attempt registers, egress runs at the default ordering")

        original.writer.write(deterministicRecords(64, seed = 11L, keySpace = 16).iterator)
        speculative.writer.write(deterministicRecords(64, seed = 11L, keySpace = 16).iterator)

        val originalPriority = original.serverHandler.taskPriority
        val speculativePriority = speculative.serverHandler.taskPriority
        assert(originalPriority.attemptNumber === 0,
          "The original attempt must register the attempt number its task context reports")
        assert(speculativePriority.attemptNumber === 2,
          "A retried or speculative attempt must register its own non-zero attempt number")
        assert(!originalPriority.speculative,
          "An attempt whose number is zero is the original and is flushed first")
        assert(speculativePriority.speculative,
          "An attempt whose number is above zero is a retry or a speculative copy")
        assert(originalPriority.taskAttemptId === 31L,
          "Egress ordering must carry the task attempt id the context reports")
        assert(speculativePriority.taskAttemptId === 32L,
          "Two attempts of one map task must be ordered as two distinct generations")
        // This is intra-subsystem flush ordering and nothing else: no operating system or network
        // level quality of service marking is configured anywhere in this repository.
        assert(StreamingShuffleServerHandler.TRANSPORT_MODULE_NAME === TransportModuleName,
          s"Streaming egress must be tuned under its own $TransportModuleName transport module")
      }
    }
  }

  test("a partial memory grant is surfaced as memory pressure") {
    val executorMemory = 2L * 1024L * 1024L
    withHarness(newHarness(numPartitions = 2, executorMemoryBytes = executorMemory,
        bufferSizePercent = 1)) { harness =>
      val spillManager = harness.spillManager
      spillManager.registerPartitionCount(2)
      assert(!spillManager.memoryPressureDetected,
        "No pressure may be reported before a reservation has been refused")
      // acquireMemory may grant less than it was asked for, so a reservation the allowance cannot
      // satisfy is refused outright rather than partially honoured and silently tolerated.
      val beyondAllowance = spillManager.totalBudgetBytes + 1L
      assert(!spillManager.reserveScratch(beyondAllowance),
        s"A reservation of $beyondAllowance bytes exceeds the " +
          s"${spillManager.totalBudgetBytes} byte allowance and must be refused")
      assert(spillManager.memoryPressureDetected,
        "A refused reservation must raise the memory-pressure signal the fallback policy consumes")
      assert(spillManager.memoryPressureEvents > 0L, "The pressure event must be counted")
      assert(spillManager.scratchBytes === 0L,
        "A refused reservation must leave nothing reserved")
      assert(spillManager.getUsed() === 0L,
        "A refused reservation must leave no task memory acquired")
      spillManager.clearMemoryPressure()
      assert(!spillManager.memoryPressureDetected, "The pressure signal must be clearable")

      // Escalation: a short grant is trip condition two, and it stands the shuffle down rather than
      // being tolerated in silence.
      val fallbackPolicy = harness.fallbackPolicy
      assert(!fallbackPolicy.hasTripped, "The policy must not have tripped before it is told")
      fallbackPolicy.recordAllocationGrant(beyondAllowance, 0L)
      assert(fallbackPolicy.hasTripped, "A short allocation grant must trip the fallback policy")
      assert(fallbackPolicy.trippedReason
          .contains(StreamingShuffleFallbackReason.MemoryPressure),
        s"A short grant must be recorded as memory pressure, but was " +
          s"${fallbackPolicy.trippedReason}")
      assert(fallbackPolicy.shouldDelegateToSortShuffle,
        "A tripped policy must route the shuffle to the sort-based writer, which is unmodified")
      // A request for nothing cannot be short granted, so it is never a trip condition.
      val untripped = new StreamingShuffleFallbackPolicy(harness.writerConf, harness.clock)
      untripped.recordAllocationGrant(0L, 0L)
      assert(!untripped.hasTripped,
        "A reservation of zero bytes cannot be short granted and must not trip anything")
      assert(untripped.unevaluableSampleCount === 1L,
        "A request for nothing must be counted as un-evaluable rather than ignored")
    }
  }

  test("the buffer and spill percentages are range validated and an absent cap means unlimited") {
    Seq(MinBufferSizePercent, DefaultBufferSizePercent, MaxBufferSizePercent).foreach { percent =>
      val accepted = new SparkConf(false).set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, percent)
      assert(accepted.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT) === percent,
        s"A buffer size percent of $percent is inside the specified range and must be accepted")
    }
    Seq(MinBufferSizePercent - 1, MaxBufferSizePercent + 1).foreach { percent =>
      val rejected = new SparkConf(false).set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, percent)
      val failure = intercept[IllegalArgumentException] {
        rejected.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT)
      }
      assert(failure.getMessage.contains("The buffer size percent must be in [1, 50]."),
        s"A buffer size percent of $percent must be rejected with the documented requirement, " +
          s"but failed with: ${failure.getMessage}")
    }
    Seq(MinSpillThresholdPercent, DefaultSpillThresholdPercent, MaxSpillThresholdPercent)
      .foreach { percent =>
        val accepted = new SparkConf(false).set(SHUFFLE_STREAMING_SPILL_THRESHOLD, percent)
        assert(accepted.get(SHUFFLE_STREAMING_SPILL_THRESHOLD) === percent,
          s"A spill threshold of $percent is inside the specified range and must be accepted")
      }
    Seq(MinSpillThresholdPercent - 1, MaxSpillThresholdPercent + 1).foreach { percent =>
      val rejected = new SparkConf(false).set(SHUFFLE_STREAMING_SPILL_THRESHOLD, percent)
      val failure = intercept[IllegalArgumentException] {
        rejected.get(SHUFFLE_STREAMING_SPILL_THRESHOLD)
      }
      assert(failure.getMessage.contains("The spill threshold must be in [50, 95]."),
        s"A spill threshold of $percent must be rejected with the documented requirement, but " +
          s"failed with: ${failure.getMessage}")
    }

    // Absence of the bandwidth cap is the unlimited state, emphatically not zero.
    val uncapped = new SparkConf(false)
    assert(uncapped.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS).isEmpty,
      "An unset maximum bandwidth must read back as absent, which is what unlimited means")
    val rejectedCap = new SparkConf(false).set(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, 0)
    val capFailure = intercept[IllegalArgumentException] {
      rejectedCap.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)
    }
    assert(capFailure.getMessage.contains("The maximum bandwidth should be positive."),
      s"A maximum bandwidth of zero must be rejected, but failed with: ${capFailure.getMessage}")

    val clock = newManualClock()
    val unlimited = TokenBucketRateLimiter.unlimited(clock)
    assert(unlimited.isUnlimited, "An absent cap must produce a limiter that enforces no rate")
    assert(unlimited.tryAcquire(MaxEncodedFrameBytes.toLong),
      "An unlimited limiter must admit the largest legal frame without reading the clock")

    // The ceiling and the refill formula the specification fixes.
    assert(BandwidthCeilingPercent === 80L,
      "Streaming holds itself to 80% of the administered link capacity")
    assert(applyBandwidthCeiling(1000L) === 1000L / PercentScale * BandwidthCeilingPercent,
      "The ceiling must divide before multiplying, exactly as the limiter does")
    assert(refillBytesPerSecond(100, 0) === refillBytesPerSecond(100, 1),
      "A concurrent shuffle count of zero or less must be treated as one")
    assert(refillBytesPerSecond(100, 2) < refillBytesPerSecond(100, 1),
      "The administered capacity must be divided across the shuffles the executor is serving")
    assert(TokenBucketRateLimiter.burstCapacityBytes(1L) === MaxEncodedFrameBytes.toLong,
      s"A bucket must be at least one maximum sized frame, i.e. $MaxEncodedFrameBytes bytes, or " +
        "it could neither admit the largest legal frame nor charge for all of it")
    assert(MinTokenBucketCapacityBytes === MaxBlockSizeBytes.toLong,
      "The minimum bucket capacity is stated against the block payload cap")
  }
}
