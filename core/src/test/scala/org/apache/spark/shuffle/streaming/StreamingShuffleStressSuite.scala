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

import java.lang.ref.WeakReference
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.apache.spark.{FetchFailed, HashPartitioner, LocalSparkContext, ShuffleDependency, SparkConf, SparkContext, SparkFunSuite, TaskContext, TaskFailedReason}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.{LISTENER_BUS_EVENT_QUEUE_CAPACITY, SHUFFLE_MANAGER, SHUFFLE_STREAMING_ENABLED, STAGE_MAX_CONSECUTIVE_ATTEMPTS, TASK_MAX_FAILURES, UNSAFE_EXCEPTION_ON_MEMORY_LEAK}
import org.apache.spark.internal.config.Status.{MAX_RETAINED_JOBS, MAX_RETAINED_STAGES, MAX_RETAINED_TASKS_PER_STAGE}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{JobSucceeded, SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.util.Clock

/**
 * The five-minute continuous streaming shuffle stress workload.
 *
 * ==What this suite is for, and why it is tagged==
 *
 * The feature specifies exactly one stress scenario: a five minute continuous workload with ten
 * concurrent tasks and five concurrent shuffles, ten percent random task-failure injection,
 * memory-leak detection by heap analysis, and under five percent throughput degradation. That is
 * one long-running case, so it carries [[StreamingShuffleStressTest]] and a fast verification run
 * leaves it out with '''zero build change''':
 *
 *  - sbt: `-Dtest.exclude.tags=org.apache.spark.shuffle.streaming.StreamingShuffleStressTest`
 *  - Maven: `-Dtest.exclude.tags=org.apache.spark.shuffle.streaming.StreamingShuffleStressTest`
 *
 * The tag is deliberately '''not''' a member of the build's default exclusion list, so a full run
 * includes this workload and only a run that names the tag skips it. The tag itself is declared
 * once in `StreamingShuffleTestHelper.scala` and is used, never redeclared, here; no tag file is
 * added to the shared tags module, whose ten tags are all domain specific and none of which is a
 * generic slow-test marker usable from core.
 *
 * The numeric contract the workload rests on is asserted by a second, cheap case that carries no
 * tag, so a fast run still verifies that the shape of the workload is the shape the feature
 * specifies even when the workload itself is excluded.
 *
 * ==Why the workload runs on a local master with ten slots==
 *
 * `local[10]` is chosen for two reasons, and the first is the requirement itself: ten task slots is
 * what "ten concurrent tasks" means, and it makes ten both the target and the ceiling, so the peak
 * concurrency this suite observes cannot be an accident of scheduling.
 *
 * The second reason is that this suite's subject is '''retention''', and retention is only
 * observable in a JVM you can inspect. In local mode the writers, the readers, the spill managers,
 * the buffer allowance and the metric source all live in the test JVM, so the buffer-utilisation
 * gauge can be read directly and a weak reference can prove that a shuffle's object graph became
 * unreachable. Two child executor JVMs would put every one of those readings out of reach behind a
 * probe job, and a five minute continuous run in child JVMs would additionally compete for memory
 * with a four gigabyte test heap, which is a flakiness source rather than a stronger test. Genuine
 * cross-executor streaming is `StreamingShuffleIntegrationTest`'s subject, and it covers it on
 * `local-cluster[2,1,1024]`; the transport here is still the real Netty transport, over loopback.
 *
 * `SharedSparkContext` could not be used in any case, because it hardcodes its master, so
 * `LocalSparkContext` is the only route to a chosen one.
 *
 * ==Leak detection, which is machine-enforced and then asserted on top==
 *
 * `spark.unsafe.exceptionOnMemoryLeak` is set to true in the conf this suite builds rather than
 * merely inherited, because the executor reads it from the `SparkConf` and the shared configuration
 * fixtures deliberately do not load ambient system properties. So any task memory left acquired at
 * task completion throws instead of passing silently, for every one of the thousands of tasks this
 * workload runs.
 *
 * On top of that the suite performs its own heap analysis and asserts '''zero retained heap''':
 * every shuffle's object graph is held only by a weak reference, and after the workload a bounded
 * collection sweep must find that not one of them survives. Then the executor-wide buffer
 * utilisation gauge must read zero -- no buffer survives task completion -- and once the context
 * has stopped the serving listener must hold no producer, so no channel and no retained spill
 * output survives either. Cleanup is registered through `TaskContext.addTaskCompletionListener`,
 * which runs on success, on failure and on cancellation alike, so those three readings cover all
 * three outcomes that this workload produces by design.
 *
 * ==Determinism, which is a hard requirement and not an aspiration==
 *
 * Nothing here sleeps and nothing waits without a bound. The five minute budget is a deadline
 * computed from a clock, not a sleep; concurrency comes from the shared barrier-released pool; the
 * failure injection is drawn from a seeded generator so a stress failure can be replayed rather
 * than chased; and each injected fault is conditioned on the attempt number, so exactly one attempt
 * of exactly one map partition fails and its retry always succeeds. The retrying variant of `test`
 * is deliberately never used: retrying would hide precisely the order dependence and the timing
 * assumption the zero-flakiness gate exists to find.
 *
 * [[StreamingShuffleMetricsSource]] is a JVM singleton whose counters outlive a case, and the
 * buffer allowance is process scoped, so both are returned to a known state around each case.
 *
 * ==What is asserted about throughput, and how==
 *
 * Every iteration performs exactly the same work, so its throughput is directly comparable to every
 * other iteration's. The run is split in half and the two halves are compared, which is what turns
 * "the system must not decay under sustained load" into an assertion.
 *
 * The statistic compared is each half's '''median''' per-iteration throughput rather than its mean.
 * That is the stronger choice, not the weaker one: a decay caused by retention shifts the whole
 * distribution and the median follows it, whereas a collection pause or a moment of host contention
 * moves the mean without saying anything about the shuffle. Both figures are reported; the median
 * is the one the five percent bound is applied to. The first half carries all of the warm-up, so a
 * healthy run reports a negative degradation, which is exactly what "did not decay" looks like.
 *
 * The whole case fits comfortably inside the twenty minute default per-test timeout that
 * `SparkFunSuite` imposes: the baseline and the workload together are around six minutes, and
 * `spark.test.timeout` is not touched.
 */
class StreamingShuffleStressSuite
  extends SparkFunSuite
  with LocalSparkContext
  with StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  // ---------------------------------------------------------------------------------------------
  // The workload's shape. Every value here is either taken from the shared constants, so that the
  // feature's figures are stated in exactly one place, or derived from them by arithmetic this
  // suite's own contract case checks.
  // ---------------------------------------------------------------------------------------------

  /**
   * Task failures the scheduler may absorb per task.
   *
   * Each injected fault needs exactly one retry, and the allowance is set above that rather than at
   * it so the workload measures recovery instead of measuring how close the injection came to the
   * limit.
   */
  private val MaxTaskFailures: Int = 4

  /**
   * Ten task slots in one JVM, with the task-failure allowance stated in the master string.
   *
   * The slot count is the concurrency the feature names, and the ceiling on it too. The second
   * component is not decoration: `local[N]` pins the task-failure allowance to ONE regardless of
   * `spark.task.maxFailures`, on the reasoning that a local run has nowhere else to place a retry,
   * so a workload that injects failures and expects them to be recovered has to ask for the
   * allowance in the master itself. `local[N,M]` is the documented form that grants it. The typed
   * configuration key is set to the same number as well, so that the two statements of the same
   * intent cannot disagree and the number still travels to a cluster master unchanged.
   */
  private val StressMaster: String = s"local[$StressConcurrentTasks,$MaxTaskFailures]"

  /** A small local master for the throwaway sort-based baseline contexts. */
  private val BaselineMaster: String = "local[2]"

  /**
   * Reduce widths of the five concurrent shuffles, ascending and all distinct.
   *
   * Distinct widths are what make cross-shuffle arbitration meaningful rather than nominal: reduce
   * partition count is one of the two keys the feature names for it, so five shuffles of equal
   * width could not show the key being used, and the narrowest shuffle sharing an executor with the
   * widest is the case in which starvation would show up.
   *
   * They sum to thirty, which is what makes ten percent of them a whole number of tasks.
   */
  private val ShuffleWidths: Seq[Int] = Seq(2, 4, 6, 8, 10)

  /** Map tasks one iteration submits, across all five shuffles. */
  private val MapTasksPerIteration: Int = ShuffleWidths.sum

  /** Ten percent of those, which is a whole number precisely because the widths sum to thirty. */
  private val InjectedFailuresPerIteration: Int =
    MapTasksPerIteration * StressFailureInjectionPercent / 100

  /**
   * Records each map partition generates.
   *
   * Sized so that SHUFFLING dominates an iteration rather than submitting one. That balance is what
   * decides whether the throughput comparison measures the subsystem at all: every iteration pays
   * the same fixed driver cost -- five job submissions, sixty task launches, the bookkeeping each
   * completed stage leaves behind -- and that cost grows slowly with the number of shuffles a JVM
   * has already run, so a workload whose iterations were mostly fixed cost would report the
   * driver's own accumulation as a decay of the shuffle. At this size an iteration moves roughly
   * seven and a half mebibytes across thirty partitions, the fixed share is a small fraction of it,
   * and a five minute budget still buys upwards of a hundred iterations, which leaves the
   * half-against-half median comparison a real sample on each side.
   *
   * It is deliberately not larger than that. The feature's guidance is explicit that per-iteration
   * work is what gives way if the case risks approaching its timeout, and one hundred mebibyte
   * iterations would buy too few samples for a median to mean anything.
   */
  private val RecordsPerPartition: Int = 1024

  /**
   * How far into a doomed partition the injected failure lands.
   *
   * Half way, so it is a genuine mid-write failure: records have already been framed, checksummed
   * and streamed by the time the producer dies, and the iterator is nowhere near exhausted.
   */
  private val RecordsBeforeInjectedFailure: Int = RecordsPerPartition / 2

  /** Base of the seeded failure selection. Fixed, so any stress failure replays exactly. */
  private val FailureSelectionSeed: Long = 4242L

  /**
   * Iterations the half-against-half throughput comparison needs before it means anything.
   *
   * Below this the two halves are too small to have a median worth comparing, so the case says so
   * rather than reporting a number drawn from three samples.
   */
  private val MinIterations: Int = 8

  /** Hard ceiling on the loop, so a clock that never advances fails rather than spins forever. */
  private val IterationGuardLimit: Int = 8192

  /** Bound on one iteration's five concurrent jobs. The per-test timeout is the real guard. */
  private val IterationTimeoutMillis: Long = 180000L

  /** Bound on draining the listener bus before its readings are trusted. */
  private val ListenerDrainTimeoutMillis: Long = 120000L

  /**
   * Listener-bus queue capacity for the run.
   *
   * Comfortably above the tens of thousands of events this workload posts, because the bus DROPS
   * events once a queue is full and a dropped task-start event would understate the peak
   * concurrency this suite asserts on. Bounded rather than effectively unlimited, so that a
   * listener falling behind still cannot turn the queue itself into a growing heap cost that the
   * throughput comparison would then read as a decay of the shuffle.
   */
  private val ListenerQueueCapacity: Int = 200000

  /** Collection passes the retained-heap sweep is allowed before it declares a survivor. */
  private val HeapSweepAttempts: Int = 16

  /** Size of the probe object the heap sweep's own self-check holds on to. */
  private val RetainedProbeBytes: Int = 1024

  /**
   * Jobs, stages and per-stage tasks the driver's status store keeps.
   *
   * Small on purpose. See [[stressConf]] for why bounding the store is the honest choice here
   * rather than leaving it to fill: its growth is driver bookkeeping that has nothing to do with
   * the shuffle, and leaving it unbounded puts that growth inside the figure this suite compares.
   */
  private val RetainedJobCount: Int = 20

  private val RetainedStageCount: Int = 20

  private val RetainedTasksPerStage: Int = 200

  /** Nanoseconds in a second, for turning an elapsed measurement into a rate. */
  private val NanosPerSecond: Long = 1000000000L

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Both of these are JVM-global and outlive a case by design: the metric source is an object,
    // and the buffer allowance is scoped to the executor rather than to a task. A counter assertion
    // that started from whatever ran before it is exactly the order dependence the zero-flakiness
    // gate exists to prevent, so both are returned to a known state here.
    resetStreamingShuffleMetrics()
    MemorySpillManager.resetSharedStateForTesting()
  }

  override def afterEach(): Unit = {
    // The context is stopped first, by the shared trait, because stopping the manager touches the
    // very allowance the reset discards. Resetting afterwards is what stops this suite's global
    // state from reaching the next suite in the same JVM.
    try {
      super.afterEach()
    } finally {
      MemorySpillManager.resetSharedStateForTesting()
      resetStreamingShuffleMetrics()
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Fixtures. Every closure handed to Spark below is built inside one of these methods and captures
  // nothing but its own parameters and locals, so no task closure ever drags this suite instance --
  // which is not serializable -- onto an executor.
  // ---------------------------------------------------------------------------------------------

  /**
   * The configuration the workload runs under.
   *
   * The five streaming keys are set through the shared fixture, which sets each one through its
   * typed entry rather than a raw string key, so a value outside a documented range would be
   * rejected where the fixture is built. Buffers are left at their default twenty percent of
   * executor memory and the spill trigger at its default eighty percent, because the point of a
   * stress run is the configuration an operator would actually deploy. Bandwidth is left absent,
   * which is the unlimited state and is never encoded as zero, and debug logging is left off, which
   * is what the log-volume budget depends on.
   *
   * Three further keys are added, each for a stated reason:
   *
   *  - the task-failure allowance, because ten percent of this workload's tasks are made to fail
   *    and every one must be recovered by the unmodified scheduler rather than abort the job;
   *  - the consecutive stage-attempt allowance, because a fetch failure is reported per map output
   *    that cannot be found, so one recomputation may legitimately resubmit a stage more than once;
   *  - the managed-memory-leak check, set here rather than merely inherited. The executor reads it
   *    from the `SparkConf`, and the shared fixtures deliberately do not load ambient system
   *    properties, so stating it is what actually arms it for this run;
   *  - a generous listener-bus queue. The bus DROPS events when a queue fills, and this workload
   *    posts tens of thousands of them, so a peak-concurrency reading taken from a bus that had
   *    silently discarded a task-start event would understate the truth. Raising the capacity is
   *    what makes the observation exact rather than probable;
   *  - tight bounds on the driver's status store. Its defaults retain a thousand stages and a
   *    hundred thousand tasks, and this workload submits enough of both to fill them, so the store
   *    would grow steadily across the run and its upkeep would show up in the throughput this
   *    suite compares. That growth is Spark's own driver-side bookkeeping, identical under
   *    sort-based shuffle and nothing to do with the subsystem under test, so it is bounded rather
   *    than measured. The user interface is already off in the test envelope, which is why bounding
   *    what it would have displayed costs this run nothing.
   *
   * @param appName application name, so a failure names the case it came from
   * @return the configuration, with streaming selected and its behaviour gate open
   */
  private def stressConf(appName: String): SparkConf = {
    withLocalMaster(
      streamingConfWithOverrides(
        bufferSizePercent = DefaultBufferSizePercent,
        spillThreshold = DefaultSpillThresholdPercent,
        maxBandwidthMBps = None,
        debug = false,
        enabled = true),
      appName,
      StressMaster)
      .set(TASK_MAX_FAILURES, MaxTaskFailures)
      .set(STAGE_MAX_CONSECUTIVE_ATTEMPTS, 2 * ShuffleWidths.max + 4)
      .set(UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      .set(LISTENER_BUS_EVENT_QUEUE_CAPACITY, ListenerQueueCapacity)
      .set(MAX_RETAINED_JOBS, RetainedJobCount)
      .set(MAX_RETAINED_STAGES, RetainedStageCount)
      .set(MAX_RETAINED_TASKS_PER_STAGE, RetainedTasksPerStage)
  }

  /**
   * The workload's dataset, optionally with a producer failure injected into chosen partitions.
   *
   * <b>Why the failure is thrown from inside the record iterator.</b> A streaming writer pulls
   * records through the map function, so throwing part way along the iterator loses a producer in
   * exactly the state the feature's producer-failure flow is specified for: blocks already framed,
   * checksummed and streamed, and the iterator far from exhausted. Throwing before the first record
   * would merely be a task that never produced anything.
   *
   * <b>Why it is deterministic.</b> The fault is conditioned on the attempt number, so attempt zero
   * of a chosen partition always fails and its retry always succeeds. Nothing depends on timing or
   * on how far a concurrent task had progressed.
   *
   * @param context live context to build on
   * @param numPartitions map and reduce width of this shuffle
   * @param failingPartitions map partitions whose first attempt is made to die, possibly empty
   * @return the key-value dataset, not yet computed
   */
  private def stressDataset(
      context: SparkContext,
      numPartitions: Int,
      failingPartitions: Set[Int]): RDD[(Int, String)] = {
    require(numPartitions > 0, s"numPartitions must be positive but was $numPartitions")
    val records = RecordsPerPartition
    val doomed = failingPartitions
    val failAfter = math.min(RecordsBeforeInjectedFailure, records - 1)
    context.parallelize(0 until numPartitions, numPartitions).flatMap { partitionIndex =>
      (0 until records).iterator.map { recordIndex =>
        if (doomed.contains(partitionIndex) && recordIndex == failAfter &&
            TaskContext.get().attemptNumber() == 0) {
          throw new IllegalStateException(
            s"Injected streaming shuffle stress failure in map partition $partitionIndex after " +
              s"$failAfter record(s)")
        }
        (partitionIndex * records + recordIndex, deterministicValue(partitionIndex, recordIndex))
      }
    }
  }

  /**
   * The dataset grouped by key, which is the shuffle this workload actually runs.
   *
   * Grouping rather than reducing, deliberately: a dependency that asks for map-side combining
   * cannot be pipelined -- there is nothing to stream until the last record of a partition has been
   * seen -- and the streaming manager declines such a shuffle outright. A reducing workload would
   * therefore have measured the sort-based delegate while claiming to measure streaming.
   *
   * The partitioner is stated explicitly rather than left to the arity overload, so the reduce
   * width this shuffle has is visible at the call site that also chose its map width.
   *
   * @param context live context to build on
   * @param numPartitions map and reduce width of this shuffle
   * @param failingPartitions map partitions whose first attempt is made to die, possibly empty
   * @return the grouped RDD, not yet computed
   */
  private def groupedStressDataset(
      context: SparkContext,
      numPartitions: Int,
      failingPartitions: Set[Int]): RDD[(Int, Iterable[String])] = {
    stressDataset(context, numPartitions, failingPartitions)
      .groupByKey(new HashPartitioner(numPartitions))
  }

  /**
   * Reduces a grouped shuffle to a comparable set of per-key digests.
   *
   * A set rather than a sequence, because a shuffle promises nothing about ordering within a
   * partition and comparing sequences would fail for a reason that is not a defect. A digest rather
   * than the values themselves, because this runs upwards of a hundred times and each element still
   * carries the group's total value length and an order-independent content hash, so a missing
   * record, a duplicated record and a corrupted record are all differences the comparison reports.
   *
   * @param shuffled the grouped output of one shuffle
   * @return one digest per key
   */
  private def digestOf(shuffled: RDD[(Int, Iterable[String])]): Set[(Int, Int, Int)] = {
    shuffled.map { case (key, values) =>
      val ordered = values.toSeq.sorted
      val totalLength = ordered.foldLeft(0)((running, value) => running + value.length)
      val contentHash = ordered.foldLeft(0)((running, value) => running * 31 + value.hashCode)
      (key, totalLength, contentHash)
    }.collect().toSet
  }

  /**
   * Runs each of the five widths through sort-based shuffle in a context of its own.
   *
   * The context is created and stopped inside this method, so the caller must hold none of its own:
   * Spark permits one per JVM. Capturing the baseline before the context under test is started is
   * the cheaper of the two ways to satisfy that, and it is why this is called first.
   *
   * No failure is injected into the baseline. Its job is to state what the correct output is, and a
   * baseline that had also been perturbed could not do that.
   *
   * @return one baseline digest per width, in the order the widths are declared
   */
  private def sortBaselineDigests(): Seq[Set[(Int, Int, Int)]] = {
    val conf = withLocalMaster(
      sortBaselineConf(), "streaming-shuffle-stress-sort-baseline", BaselineMaster)
    val baselineContext = new SparkContext(conf)
    try {
      ShuffleWidths.map { numPartitions =>
        digestOf(groupedStressDataset(baselineContext, numPartitions, Set.empty))
      }
    } finally {
      baselineContext.stop()
    }
  }

  /**
   * Which map partition of which shuffle is made to fail on one iteration.
   *
   * The selection is drawn over the iteration's whole task population rather than over one shuffle,
   * which is what makes ten percent a whole number: the five widths sum to thirty, so exactly three
   * tasks are chosen, whereas ten percent of a two-partition shuffle would round to none. The
   * global index is then mapped back onto a shuffle and a partition through the cumulative widths.
   *
   * The generator is seeded from a fixed base plus the iteration number, so the same run makes the
   * same choices every time and a stress failure can be replayed rather than chased.
   *
   * @param iteration zero-based iteration number
   * @return one set of doomed map partitions per shuffle, in the order the widths are declared
   */
  private def failingPartitionsFor(iteration: Int): Seq[Set[Int]] = {
    val doomed = selectFailingTasks(
      MapTasksPerIteration, StressFailureInjectionPercent, FailureSelectionSeed + iteration.toLong)
    val offsets = ShuffleWidths.scanLeft(0)((running, width) => running + width)
    ShuffleWidths.indices.map { shuffleIndex =>
      val start = offsets(shuffleIndex)
      val end = start + ShuffleWidths(shuffleIndex)
      doomed.filter(index => index >= start && index < end).map(index => index - start)
    }
  }

  /**
   * Whether a shuffled RDD's shuffle was registered on the streaming path.
   *
   * The handle is the commitment: `registerShuffle` returns a [[StreamingShuffleHandle]] only for a
   * shuffle it has decided to stream, so this is the one reading that distinguishes a streaming run
   * from a run that quietly went through the sort-based delegate. Every measurement in this suite
   * is only evidence about streaming shuffle if this holds, so it is established, never assumed.
   *
   * @param shuffled a shuffled RDD, whose graph need not have been computed yet
   * @return true when this shuffle will be streamed
   */
  private def isStreamingShuffle(shuffled: RDD[_]): Boolean = {
    shuffled.dependencies.headOption.exists {
      case dependency: ShuffleDependency[_, _, _] =>
        dependency.shuffleHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]]
      case _ => false
    }
  }

  /**
   * Asserts that the live environment really is running the streaming manager with its gate open.
   *
   * @param context the context under test
   */
  private def assertStreamingManagerInService(context: SparkContext): Unit = {
    val manager = context.env.shuffleManager
    assert(manager.isInstanceOf[StreamingShuffleManager],
      "spark.shuffle.manager=streaming must have produced a StreamingShuffleManager in the live " +
        s"environment, but the driver holds ${manager.getClass.getName}")
    assert(context.getConf.get(SHUFFLE_MANAGER) === StreamingShuffleManager.SHORT_NAME,
      "the application must have selected the streaming manager by its short name, but selected " +
        s"'${context.getConf.get(SHUFFLE_MANAGER)}'")
    assert(context.getConf.get(SHUFFLE_STREAMING_ENABLED),
      s"${SHUFFLE_STREAMING_ENABLED.key} must be set true explicitly, because it defaults to " +
        "false and a closed gate would have every call delegated to the sort-based manager")
    assert(context.getConf.get(UNSAFE_EXCEPTION_ON_MEMORY_LEAK),
      s"${UNSAFE_EXCEPTION_ON_MEMORY_LEAK.key} must be set on the configuration the executor " +
        "reads, or a retained task reservation would pass silently instead of failing the task")
  }

  /**
   * Records per second, from a record count and the nanoseconds it took.
   *
   * @param recordCount records the interval processed
   * @param elapsedNanos nanoseconds the interval took, which must be positive
   * @return the rate, truncated to a whole number of records per second
   */
  private def throughputRecordsPerSecond(recordCount: Long, elapsedNanos: Long): Long = {
    require(recordCount >= 0L, s"recordCount must be non-negative but was $recordCount")
    require(elapsedNanos > 0L, s"elapsedNanos must be positive but was $elapsedNanos")
    recordCount * NanosPerSecond / elapsedNanos
  }

  /**
   * The median of a non-empty sample, taking the lower of the two middles when the count is even so
   * that the answer is one of the observations rather than an average of two of them.
   *
   * @param values the sample
   * @return its median
   */
  private def medianOf(values: Seq[Long]): Long = {
    require(values.nonEmpty, "a median needs at least one sample")
    val ordered = values.sorted
    ordered((ordered.size - 1) / 2)
  }

  /**
   * How far a figure fell, as a whole percentage of where it started.
   *
   * A figure that rose reports a negative degradation rather than a clamped zero, because "it got
   * faster" and "it held level" are different observations and a stress run should be able to say
   * which one happened.
   *
   * @param before the earlier figure, which must be positive
   * @param after the later figure
   * @return the fall as a percentage of `before`, negative when `after` is the larger
   */
  private def degradationPercent(before: Long, after: Long): Long = {
    require(before > 0L, s"the earlier figure must be positive but was $before")
    (before - after) * 100L / before
  }

  /**
   * How many of the given weak references still resolve after a bounded collection sweep.
   *
   * This is the heap analysis. A weak reference to an object that nothing else reaches is cleared
   * by the collection itself, so a reference that still resolves after this returns is a genuine
   * retention rather than a scheduling artefact. Requesting a collection is not a wait: it makes
   * progress towards the answer, where sleeping would only hope for it.
   *
   * @param references one reference per object whose unreachability is being asserted
   * @return the number that survived, which a healthy run leaves at zero
   */
  private def survivingReferences(references: Seq[WeakReference[AnyRef]]): Int = {
    var survivors = references.count(reference => reference.get() != null)
    var attempt = 0
    while (survivors > 0 && attempt < HeapSweepAttempts) {
      System.gc()
      attempt += 1
      survivors = references.count(reference => reference.get() != null)
    }
    survivors
  }

  /**
   * A weak reference to an object that is unreachable the moment this returns.
   *
   * The allocation happens in a frame of its own precisely so that no local slot of the caller can
   * keep the object alive, which is what makes the negative direction of the heap sweep's own
   * self-check trustworthy rather than dependent on how the caller happened to be compiled.
   *
   * @return a reference whose referent nothing reaches
   */
  private def unreachableReference(): WeakReference[AnyRef] =
    new WeakReference[AnyRef](new Object())

  // ---------------------------------------------------------------------------------------------
  // The numeric contract. Untagged and cheap on purpose: a fast verification run that excludes the
  // workload still checks that the shape of the workload is the shape the feature specifies, and
  // that the arithmetic every assertion in the workload depends on says what it is meant to say.
  // ---------------------------------------------------------------------------------------------

  test("the stress workload's numeric contract is the one the feature specifies") {
    assert(StressDurationMillis === 5L * 60L * 1000L,
      s"the workload runs for five minutes, but the suite reads $StressDurationMillis ms")
    assert(StressConcurrentTasks === 10,
      s"the workload runs ten concurrent tasks, but the suite reads $StressConcurrentTasks")
    assert(StressConcurrentShuffles === 5,
      s"the workload runs five concurrent shuffles, but the suite reads $StressConcurrentShuffles")
    assert(StressFailureInjectionPercent === 10,
      "the workload fails ten percent of its tasks, but the suite reads " +
        s"$StressFailureInjectionPercent percent")
    assert(MaxThroughputDegradationPercent === 5,
      "throughput may degrade by under five percent, but the suite reads " +
        s"$MaxThroughputDegradationPercent percent")
    assert(PerTestTimeoutMinutes === 20,
      s"the base suite allows twenty minutes per test, but the suite reads $PerTestTimeoutMinutes")
    assert(StressDurationMillis * 2L < PerTestTimeoutMinutes.toLong * 60L * 1000L,
      "the five minute workload must leave generous headroom inside the twenty minute per-test " +
        "timeout, because the baseline, the recovery of every injected failure and the teardown " +
        "all have to fit alongside it")

    // The master is what actually carries the concurrency, so it is asserted rather than commented.
    assert(StressMaster === s"local[$StressConcurrentTasks,$MaxTaskFailures]",
      "ten task slots is what ten concurrent tasks means, and the failure allowance has to be in " +
        "the master because local[N] pins it to one whatever the configuration says, but the " +
        s"master is '$StressMaster'")
    assert(StressMaster === "local[10,4]",
      s"spelled out, the master must be local[10,4], but it is '$StressMaster'")

    assert(ShuffleWidths.size === StressConcurrentShuffles,
      s"one width per concurrent shuffle is needed, but ${ShuffleWidths.size} were declared")
    assert(ShuffleWidths.distinct.size === ShuffleWidths.size,
      "the widths must be distinct, or arbitration keyed on reduce partition count could not be " +
        s"observed at all; they are ${ShuffleWidths.mkString(", ")}")
    assert(ShuffleWidths.forall(width => width > 0),
      s"every width must be positive, but they are ${ShuffleWidths.mkString(", ")}")
    assert(MapTasksPerIteration === 30,
      s"the five widths must sum to thirty, but they sum to $MapTasksPerIteration")
    assert(MapTasksPerIteration >= StressConcurrentTasks,
      s"an iteration must offer at least $StressConcurrentTasks runnable map tasks, or ten " +
        s"could never be in flight together, but it offers $MapTasksPerIteration")
    assert(InjectedFailuresPerIteration === 3,
      s"ten percent of thirty tasks is three, but the suite derives $InjectedFailuresPerIteration")
    assert(
      InjectedFailuresPerIteration * 100 === MapTasksPerIteration * StressFailureInjectionPercent,
      s"$InjectedFailuresPerIteration of $MapTasksPerIteration tasks must be exactly " +
        s"$StressFailureInjectionPercent percent, with no rounding either way")
    assert(RecordsBeforeInjectedFailure > 0 && RecordsBeforeInjectedFailure < RecordsPerPartition,
      "the injected failure must land after the first record and before the last, or it would " +
        s"not be a mid-write failure; it lands at $RecordsBeforeInjectedFailure of " +
        s"$RecordsPerPartition")
    assert(MaxTaskFailures > 2,
      "one injected fault needs one retry, and the allowance must sit above that rather than at " +
        s"it, but it is $MaxTaskFailures")

    // The selection is seeded, so it is reproducible; it selects exactly the share it promises; and
    // it selects a different set as the run proceeds rather than punishing the same tasks forever.
    val firstDraw = failingPartitionsFor(0)
    val repeatDraw = failingPartitionsFor(0)
    assert(firstDraw === repeatDraw,
      "the same iteration must select the same tasks every time, or a stress failure could not " +
        s"be replayed; it selected $firstDraw and then $repeatDraw")
    val drawSizes = (0 until 20).map(iteration => failingPartitionsFor(iteration).map(_.size).sum)
    assert(drawSizes.forall(size => size === InjectedFailuresPerIteration),
      s"every iteration must select exactly $InjectedFailuresPerIteration task(s), but the first " +
        s"twenty selected ${drawSizes.distinct.mkString(", ")}")
    val distinctDraws = (0 until 20).map(iteration => failingPartitionsFor(iteration)).distinct
    assert(distinctDraws.size > 1,
      "the selection must move with the iteration rather than doom the same tasks for the whole " +
        s"run, but the first twenty iterations produced ${distinctDraws.size} distinct " +
        "selection(s)")
    failingPartitionsFor(0).zip(ShuffleWidths).foreach { case (doomed, width) =>
      assert(doomed.forall(index => index >= 0 && index < width),
        s"a doomed partition index must fall inside its own $width partition shuffle, but the " +
          s"selection was $doomed")
    }

    // The throughput arithmetic. Each of these is a property the workload's verdict rests on, so
    // each is checked here against a value chosen by hand rather than produced by a run.
    assert(throughputRecordsPerSecond(2000L, NanosPerSecond) === 2000L,
      "two thousand records in one second is two thousand records per second")
    assert(throughputRecordsPerSecond(1000L, NanosPerSecond / 2L) === 2000L,
      "one thousand records in half a second is also two thousand records per second")
    assert(medianOf(Seq(3L, 1L, 2L)) === 2L, "the median of an odd sample is its middle value")
    assert(medianOf(Seq(4L, 1L, 3L, 2L)) === 2L,
      "the median of an even sample takes the lower middle, so the answer is an observation")
    assert(medianOf(Seq(7L)) === 7L, "the median of one observation is that observation")
    assert(degradationPercent(1000L, 950L) === 5L,
      "a fall from a thousand to nine hundred and fifty is a five percent degradation")
    assert(degradationPercent(1000L, 1000L) === 0L, "holding level is no degradation at all")
    assert(degradationPercent(1000L, 1200L) === -20L,
      "getting faster must report a negative degradation rather than a clamped zero")
    assert(degradationPercent(1000L, 960L) < MaxThroughputDegradationPercent.toLong,
      "a four percent fall is inside the bound the workload applies")
    assert(degradationPercent(1000L, 940L) >= MaxThroughputDegradationPercent.toLong,
      "a six percent fall is outside it, so the bound is a bound rather than a formality")

    // The heap sweep, checked in both directions. A sweep that cleared everything would prove
    // nothing about retention, and one that cleared nothing would report a leak on every run. The
    // held probe is read again after the sweep, which is what keeps it genuinely reachable across
    // it rather than merely written down as being so.
    val retained = new Array[Byte](RetainedProbeBytes)
    assert(survivingReferences(Seq(new WeakReference[AnyRef](retained))) === 1,
      "a reference to an object the suite still holds must survive the sweep, or the sweep could " +
        "not tell a retained object from a released one")
    assert(retained.length === RetainedProbeBytes,
      s"the probe is held across the sweep on purpose, and must still be $RetainedProbeBytes " +
        s"byte(s) long afterwards, but is ${retained.length}")
    assert(survivingReferences(Seq(unreachableReference())) === 0,
      "a reference to an unreachable object must not survive the sweep, or a leak would go " +
        "unreported")

    // Exactly four metrics, which is the whole of this subsystem's telemetry contract. This is the
    // "no parallel counters" assertion: there is no fifth series duplicating the spill volume, so
    // the volume can only be read from the existing task metric accumulators.
    assert(MetricNames.size === 4,
      s"the subsystem publishes four metrics, but the suite reads ${MetricNames.size}")
    assert(streamingShuffleMetricNames() === MetricNames.toSet,
      s"the registry must hold exactly ${MetricNames.mkString(", ")}, but holds " +
        s"${streamingShuffleMetricNames().toSeq.sorted.mkString(", ")}")
  }

  // ---------------------------------------------------------------------------------------------
  // The workload itself.
  // ---------------------------------------------------------------------------------------------

  test("a five minute continuous streaming shuffle workload holds throughput and leaks nothing",
      StreamingShuffleStressTest) {
    // The baselines come first, in throwaway contexts of their own, because Spark permits one
    // context per JVM and what the correct output is has to be known before the run under test
    // starts. No failure is injected into them: their job is to state the answer.
    val baselines = sortBaselineDigests()
    baselines.zip(ShuffleWidths).foreach { case (baseline, width) =>
      assert(baseline.size === width * RecordsPerPartition,
        s"the sort-based baseline of the $width partition shuffle must produce one group per " +
          s"distinct key, ${width * RecordsPerPartition} in all, but produced ${baseline.size}")
    }

    sc = new SparkContext(stressConf("streaming-shuffle-stress"))
    assertStreamingManagerInService(sc)
    val manager = sc.env.shuffleManager.asInstanceOf[StreamingShuffleManager]

    val recorder = new StreamingShuffleStressRecorder
    sc.addSparkListener(recorder)

    // Pre-flight. Every measurement below is only evidence about streaming shuffle if the run
    // genuinely goes through the streaming manager, so that is established before anything else and
    // its output is checked against the baseline while there is still only one shuffle to blame.
    val preflight = groupedStressDataset(sc, ShuffleWidths.head, Set.empty)
    assert(isStreamingShuffle(preflight),
      "the pre-flight shuffle must have been registered on the streaming path, but its handle is " +
        "not a StreamingShuffleHandle, which means the manager declined to stream it")
    assertNoDataLoss(digestOf(preflight), baselines.head, "the streaming pre-flight shuffle")

    // The five minute budget, as a deadline read from a clock. Nothing sleeps: the loop does work
    // and then asks the clock whether the budget is spent.
    val clock: Clock = wallClock()
    val startedAtMillis = clock.getTimeMillis()
    val deadlineMillis = startedAtMillis + StressDurationMillis
    val recordsPerIteration = MapTasksPerIteration.toLong * RecordsPerPartition.toLong
    val throughputSamples = new mutable.ArrayBuffer[Long]()
    val shuffleGraphReferences = new mutable.ArrayBuffer[WeakReference[AnyRef]]()
    var iterations = 0
    var injectedFailures = 0L

    while (clock.getTimeMillis() < deadlineMillis && iterations < IterationGuardLimit) {
      val iteration = iterations
      val doomedPerShuffle = failingPartitionsFor(iteration)
      injectedFailures += doomedPerShuffle.map(_.size).sum.toLong
      // Wall time, and only for measurement. Turning an interval into a rate is the one thing in
      // this suite that is genuinely about elapsed time, and reading a clock is not waiting on one.
      val iterationStartedAtNanos = System.nanoTime()
      // Five jobs released from one barrier, so the five shuffles are genuinely concurrent rather
      // than merely consecutive: without the barrier the first would usually finish before the last
      // began and an iteration meant to exercise contention would exercise nothing.
      val outcomes = runConcurrently(
          StressConcurrentShuffles,
          s"streaming-shuffle-stress-iteration-$iteration",
          IterationTimeoutMillis) { shuffleIndex =>
        val shuffled = groupedStressDataset(
          sc, ShuffleWidths(shuffleIndex), doomedPerShuffle(shuffleIndex))
        // The only reference this iteration keeps to the shuffle's object graph is a weak one, so
        // the sweep after the loop is able to prove that nothing retained it.
        val reference = new WeakReference[AnyRef](shuffled)
        (digestOf(shuffled), reference)
      }
      val iterationNanos = System.nanoTime() - iterationStartedAtNanos

      assert(outcomes.size === StressConcurrentShuffles,
        s"iteration $iteration must have completed all $StressConcurrentShuffles of its " +
          s"concurrent shuffles, but ${outcomes.size} returned a result")
      outcomes.zip(ShuffleWidths).zip(baselines).foreach {
        case (((observed, _), width), baseline) =>
          assertNoDataLoss(observed, baseline,
            s"iteration $iteration of the stress workload, its $width partition shuffle")
      }
      shuffleGraphReferences ++= outcomes.map { case (_, reference) => reference }
      assert(iterationNanos > 0L,
        s"iteration $iteration must have taken measurable time, but the clock reported " +
          s"$iterationNanos ns")
      throughputSamples += throughputRecordsPerSecond(recordsPerIteration, iterationNanos)
      iterations += 1
    }

    assert(iterations < IterationGuardLimit,
      s"the loop stopped at its guard of $IterationGuardLimit iteration(s) rather than at the " +
        "five minute deadline, which means the clock never advanced past it")
    assert(iterations >= MinIterations,
      s"the half-against-half throughput comparison needs at least $MinIterations iterations to " +
        s"mean anything, but the five minute budget only bought $iterations; each iteration is " +
        "therefore doing far more work than this workload intends")
    val elapsedMillis = clock.getTimeMillis() - startedAtMillis
    assert(elapsedMillis >= StressDurationMillis,
      s"the workload must have run for the whole $StressDurationMillis ms it was given, but " +
        s"stopped after $elapsedMillis ms")

    // The listener runs on its own thread, so its readings are trusted only once it has drained.
    sc.listenerBus.waitUntilEmpty(ListenerDrainTimeoutMillis)
    sc.removeSparkListener(recorder)

    // ---------------------------------------------------------------------------------------------
    // Concurrency: ten tasks and five shuffles, both observed rather than assumed.
    // ---------------------------------------------------------------------------------------------

    assert(recorder.peakConcurrentTasks >= StressConcurrentTasks,
      s"$StressConcurrentTasks tasks must have been in flight together at some point in the run, " +
        s"but the peak observed across $iterations iteration(s) was " +
        s"${recorder.peakConcurrentTasks}")
    assert(recorder.peakConcurrentJobs >= StressConcurrentShuffles,
      s"$StressConcurrentShuffles shuffles must have been active together, since that is what " +
        "exercises cross-shuffle utilisation aggregation and arbitration at all, but the peak " +
        s"observed was ${recorder.peakConcurrentJobs}")
    assert(recorder.taskEndCount > 0L, "the run must have ended some tasks for a listener to read")

    // ---------------------------------------------------------------------------------------------
    // Failure injection: ten percent of the tasks, seeded, and every one of them recovered.
    // ---------------------------------------------------------------------------------------------

    val mapTasksSubmitted = iterations.toLong * MapTasksPerIteration.toLong
    assert(injectedFailures === iterations.toLong * InjectedFailuresPerIteration.toLong,
      s"the run must have injected $InjectedFailuresPerIteration failure(s) into each of its " +
        s"$iterations iteration(s), but injected $injectedFailures in total")
    assert(injectedFailures * 100L === mapTasksSubmitted * StressFailureInjectionPercent.toLong,
      s"$injectedFailures of $mapTasksSubmitted map tasks must be exactly " +
        s"$StressFailureInjectionPercent percent of them, with no rounding either way")
    assert(recorder.countedFailures.size >= InjectedFailuresPerIteration,
      "the scheduler must have counted the injected failures rather than swallowing them, but it " +
        s"counted ${recorder.countedFailures.size} across $injectedFailures injected")
    // Recovery took one of the paths the feature already provides and no new mechanism: a retry of
    // the map task, or a fetch failure that the UNMODIFIED scheduler resolves by recomputing the
    // upstream stage. That every job nonetheless produced the baseline's output, asserted in the
    // loop above, is the proof that recovery happened; this is the proof nothing was abandoned.
    assert(recorder.failedJobCount === 0,
      "every job must have completed despite the injected failures, but " +
        s"${recorder.failedJobCount} failed outright, which would mean a failure went unrecovered")
    assert(recorder.succeededJobCount >= iterations.toLong * StressConcurrentShuffles.toLong,
      s"all ${iterations.toLong * StressConcurrentShuffles.toLong} shuffle jobs must have " +
        s"succeeded, but only ${recorder.succeededJobCount} did")
    // Where recovery went through the reduce side it did so by the ONE scheduler-facing signal this
    // subsystem uses, and each such report names a real shuffle and a real reduce partition. There
    // is no scheduler edit to test here: stage recomputation is the unmodified scheduler's own.
    assert(recorder.fetchFailures.forall(
      failure => failure.shuffleId >= 0 && failure.reduceId >= 0),
      "every fetch failure must name a real shuffle and reduce partition, but one reported " +
        s"${recorder.fetchFailures.find(f => f.shuffleId < 0 || f.reduceId < 0)}")

    // ---------------------------------------------------------------------------------------------
    // Throughput: under five percent degradation from the first half of the run to the second.
    // ---------------------------------------------------------------------------------------------

    val samples = throughputSamples.toSeq
    assert(samples.size === iterations,
      s"one throughput sample per iteration is needed, but $iterations iteration(s) produced " +
        s"${samples.size} sample(s)")
    val firstHalf = samples.take(samples.size / 2)
    val secondHalf = samples.drop(samples.size / 2)
    assert(firstHalf.nonEmpty && secondHalf.nonEmpty,
      s"both halves of the run must hold samples, but they hold ${firstHalf.size} and " +
        s"${secondHalf.size}")
    val firstMedian = medianOf(firstHalf)
    val secondMedian = medianOf(secondHalf)
    assert(firstMedian > 0L,
      s"the first half of the run must have measurable throughput, but its median was $firstMedian")
    val medianDegradation = degradationPercent(firstMedian, secondMedian)
    // The mean is reported alongside, never asserted on: a single collection pause moves a mean
    // without saying anything about the shuffle, which is exactly why the median carries the bound.
    val firstMean = firstHalf.sum / firstHalf.size.toLong
    val secondMean = secondHalf.sum / secondHalf.size.toLong
    assert(medianDegradation < MaxThroughputDegradationPercent.toLong,
      s"throughput must not decay by $MaxThroughputDegradationPercent percent or more under " +
        s"sustained load, but the median fell $medianDegradation percent, from $firstMedian to " +
        s"$secondMedian record(s) per second across $iterations iteration(s)")
    logInfo(s"Streaming shuffle stress workload ran $iterations iteration(s) of " +
      s"$StressConcurrentShuffles concurrent shuffle(s) over " +
      s"${TimeUnit.MILLISECONDS.toSeconds(elapsedMillis)} s, injecting $injectedFailures " +
      s"failure(s) into $mapTasksSubmitted map task(s) and observing " +
      s"${recorder.countedFailures.size} counted failure(s) of which " +
      s"${recorder.fetchFailures.size} were fetch failures; median throughput moved from " +
      s"$firstMedian to $secondMedian record(s) per second, a degradation of $medianDegradation " +
      s"percent against a bound of $MaxThroughputDegradationPercent percent, with half means of " +
      s"$firstMean and $secondMean record(s) per second and a peak of " +
      s"${recorder.peakConcurrentTasks} concurrent task(s)")

    // ---------------------------------------------------------------------------------------------
    // Spill accounting rides the EXISTING accumulators, and there are no parallel counters.
    // ---------------------------------------------------------------------------------------------

    assert(recorder.peakExecutionMemory > 0L,
      "the streaming path must report peak execution memory on the existing task metric " +
        "accumulator, or the whole run would be invisible to every Spark observability surface, " +
        s"but the high water mark across the run was ${recorder.peakExecutionMemory}")
    assert(recorder.memoryBytesSpilled >= 0L && recorder.diskBytesSpilled >= 0L,
      s"spill volumes must be readable from task metrics, but they read " +
        s"${recorder.memoryBytesSpilled} in memory and ${recorder.diskBytesSpilled} on disk")
    if (observedSpillCount() > 0L) {
      assert(recorder.diskBytesSpilled > 0L,
        s"${observedSpillCount()} spill event(s) were counted, so their volume must appear on " +
          "the existing disk-spill accumulator rather than only on the streaming counter, but " +
          "the accumulator read zero")
    }
    assert(streamingShuffleMetricNames() === MetricNames.toSet,
      "the subsystem must publish exactly its four documented metrics, so that spill volume is " +
        s"reported through task metrics alone, but the registry holds " +
        s"${streamingShuffleMetricNames().toSeq.sorted.mkString(", ")}")
    assert(observedBackpressureEvents() >= 0L && observedPartialReadInvalidations() >= 0L,
      "the two event counters must be readable after the run, whatever they read")

    // ---------------------------------------------------------------------------------------------
    // Zero retained heap, by heap analysis, on top of the machine-enforced leak check.
    // ---------------------------------------------------------------------------------------------

    assert(shuffleGraphReferences.size === iterations * StressConcurrentShuffles,
      s"one reference per shuffle is needed, but $iterations iteration(s) of " +
        s"$StressConcurrentShuffles shuffle(s) produced ${shuffleGraphReferences.size}")
    val leakFailures = recorder.countedFailures.filter(
      reason => reason.toErrorString.contains("Managed memory leak"))
    assert(leakFailures.isEmpty,
      s"${leakFailures.size} task(s) failed the managed-memory-leak check that this run arms " +
        "explicitly, which means task memory was left acquired at task completion")
    assert(observedBufferUtilizationPercent() === 0L,
      "no buffer may survive task completion, so the executor-wide buffer utilisation gauge must " +
        "read zero once every task has ended, but it reads " +
        s"${observedBufferUtilizationPercent()} percent")
    val survivors = survivingReferences(shuffleGraphReferences.toSeq)
    assert(survivors === 0,
      s"$survivors of ${shuffleGraphReferences.size} shuffle object graph(s) were still " +
        s"reachable after $HeapSweepAttempts collection pass(es), which is retained heap: " +
        "something outlived the shuffle that produced it")

    // Stopping the context is what releases the serving side, so the last two readings are taken
    // after it. The manager reference was captured before the stop precisely so that they can be.
    sc.stop()
    assert(manager.boundStreamingListener.forall(listener => listener.producerCount === 0),
      "no producer, and therefore no channel and no retained streamed output, may survive the " +
        "manager being stopped, but the serving listener still holds " +
        s"${manager.boundStreamingListener.map(_.producerCount).getOrElse(0)}")
    assert(manager.boundStreamingListener.forall(listener => listener.malformedFrameCount === 0L),
      "a five minute run of the wire protocol must not have produced a single frame the router " +
        "could not handle, but it produced " +
        s"${manager.boundStreamingListener.map(_.malformedFrameCount).getOrElse(0L)}")
    // Reported rather than asserted, deliberately. A frame naming a producer the router no longer
    // serves is the expected consequence of an injected producer failure -- a consumer's
    // acknowledgement racing a deregistration -- so a non-zero figure here is the recovery path
    // working, not a defect, whereas a frame that could not be decoded at all would be one.
    logInfo("Streaming shuffle stress workload left " +
      s"${manager.boundStreamingListener.map(_.unroutableFrameCount).getOrElse(0L)} unroutable " +
      s"frame(s) and ${manager.boundStreamingListener.map(_.retiredConsumerCount).getOrElse(0L)} " +
      s"retired consumer(s) behind, with ${recorder.memoryBytesSpilled} byte(s) spilled in " +
      s"memory and ${recorder.diskBytesSpilled} on disk across ${recorder.taskEndCount} task(s), " +
      s"and ${observedSpillCount()} spill event(s) counted")
  }
}

/**
 * Records what the scheduler did with the stress workload, so that its properties are observed
 * rather than assumed.
 *
 * Five things are captured because five different claims are made about them. Peak concurrent tasks
 * and peak concurrent jobs are the workload's ten-tasks and five-shuffles requirements, and a peak
 * is a stable statistic over a long run rather than a snapshot that could catch a quiet moment.
 * Task failure reasons say whether an injected fault was counted or silently swallowed, and carry
 * the text that the managed-memory-leak check would fail with. Job results say whether recovery
 * completed or a job was abandoned. Spill volumes and the peak execution memory are accumulated
 * from the task metrics the listener is handed, which is the read path an operator's own tooling
 * uses, so observing them here is what proves the streaming path reports through Spark's existing
 * accumulators rather than through a channel of its own.
 *
 * Every callback and every accessor is synchronized, because the listener bus delivers on its own
 * thread while the test body reads from the task thread. The counters are plain fields under that
 * one monitor rather than atomics, because a peak has to be compared and updated together.
 */
private class StreamingShuffleStressRecorder extends SparkListener {

  private val failures = new mutable.ArrayBuffer[TaskFailedReason]()

  private val fetches = new mutable.ArrayBuffer[FetchFailed]()

  private var activeTasks: Int = 0

  private var peakTasks: Int = 0

  private var activeJobs: Int = 0

  private var peakJobs: Int = 0

  private var tasksEnded: Long = 0L

  private var jobsSucceeded: Long = 0L

  private var jobsFailed: Long = 0L

  private var memorySpilledTotal: Long = 0L

  private var diskSpilledTotal: Long = 0L

  private var peakMemoryHighWater: Long = 0L

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    activeJobs += 1
    peakJobs = math.max(peakJobs, activeJobs)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    activeJobs -= 1
    jobEnd.jobResult match {
      case JobSucceeded => jobsSucceeded += 1L
      case _ => jobsFailed += 1L
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    activeTasks += 1
    peakTasks = math.max(peakTasks, activeTasks)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    activeTasks -= 1
    tasksEnded += 1L
    val metrics: TaskMetrics = taskEnd.taskMetrics
    if (metrics != null) {
      memorySpilledTotal += metrics.memoryBytesSpilled
      diskSpilledTotal += metrics.diskBytesSpilled
      peakMemoryHighWater = math.max(peakMemoryHighWater, metrics.peakExecutionMemory)
    }
    taskEnd.reason match {
      case fetchFailed: FetchFailed =>
        fetches += fetchFailed
        failures += fetchFailed
      case failed: TaskFailedReason =>
        failures += failed
      case _ =>
    }
  }

  def peakConcurrentTasks: Int = synchronized(peakTasks)

  def peakConcurrentJobs: Int = synchronized(peakJobs)

  def countedFailures: Seq[TaskFailedReason] = synchronized(failures.toSeq)

  def fetchFailures: Seq[FetchFailed] = synchronized(fetches.toSeq)

  def taskEndCount: Long = synchronized(tasksEnded)

  def succeededJobCount: Long = synchronized(jobsSucceeded)

  def failedJobCount: Long = synchronized(jobsFailed)

  def memoryBytesSpilled: Long = synchronized(memorySpilledTotal)

  def diskBytesSpilled: Long = synchronized(diskSpilledTotal)

  def peakExecutionMemory: Long = synchronized(peakMemoryHighWater)
}
