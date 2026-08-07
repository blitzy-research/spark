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

import java.lang.reflect.Modifier
import java.nio.ByteBuffer
import java.util.Locale

import scala.collection.immutable.SortedMap
import scala.jdk.CollectionConverters._

import _root_.io.netty.channel.DefaultChannelId
import _root_.io.netty.channel.embedded.EmbeddedChannel
import org.apache.logging.log4j.Level
import org.mockito.Mockito.mock
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.{Aggregator, HashPartitioner, LocalSparkContext, MapOutputTrackerMaster,
  Partitioner, SecurityManager, ShuffleDependency, SparkConf, SparkContext, SparkEnv,
  SparkException, SparkFunSuite, SparkIllegalArgumentException, TaskContext}
import org.apache.spark.internal.config.{AUTH_SECRET, NETWORK_AUTH_ENABLED, SHUFFLE_MANAGER,
  SHUFFLE_SERVICE_ENABLED, SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_DEBUG,
  SHUFFLE_STREAMING_ENABLED, SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS,
  SHUFFLE_STREAMING_SPILL_THRESHOLD}
import org.apache.spark.metrics.source.StaticSources
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.{TransportClient, TransportClientBootstrap,
  TransportClientFactory, TransportResponseHandler}
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.network.shuffle.protocol.streaming.{HeartbeatMessage,
  StreamingShuffleMessage}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, MigratableResolver,
  ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.{BlockManagerId, ShuffleMergedBlockId}
import org.apache.spark.util.{RpcUtils, Utils}

/** Tests of [[StreamingShuffleManager]] as a `ShuffleManager` service provider. */
class StreamingShuffleManagerSuite extends SparkFunSuite
  with LocalSparkContext
  with StreamingShuffleTestHelper
  with StreamingShuffleHadoopCredentialIsolation {

  import StreamingShuffleTestHelper._

  private val streamingManagerClassName: String = classOf[StreamingShuffleManager].getName

  private val sortManagerClassName: String = classOf[SortShuffleManager].getName

  private val initialBufferSizePercent: Int = 10

  private val mutatedBufferSizePercent: Int = 40

  private val ThroughputProbeShuffleId = 9001

  private val PreTripMapId = 90L

  private val DirectConsumerToken: Long = 4711001L

  private val unknownShuffleId: Int = 6599

  /**
   * The streaming metric handles are a JVM-wide singleton, so a clean baseline is taken before
   * every case rather than assumed.
   */
  override def beforeEach(): Unit = {
    super.beforeEach()
    resetStreamingShuffleMetrics()
  }

  override def afterEach(): Unit = {
    try {
      resetStreamingShuffleMetrics()
    } finally {
      super.afterEach()
    }
  }

  private def withManager[T](conf: SparkConf, isDriver: Boolean)(
      body: StreamingShuffleManager => T): T = {
    val manager = new StreamingShuffleManager(conf, isDriver)
    try {
      body(manager)
    } finally {
      manager.stop()
    }
  }

  private def withSortManager[T](conf: SparkConf)(body: SortShuffleManager => T): T = {
    val manager = new SortShuffleManager(conf)
    try {
      body(manager)
    } finally {
      manager.stop()
    }
  }

  /** An answer that fails loudly when an un-stubbed member of a mock is touched. */
  private class RuntimeExceptionAnswer extends Answer[Object] {
    override def answer(invocation: InvocationOnMock): Object = {
      throw new RuntimeException("Called non-stubbed method, " + invocation.getMethod.getName)
    }
  }

  private def doReturn(value: Any): org.mockito.stubbing.Stubber =
    org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  private def shuffleDep(
      numPartitions: Int = 4,
      mapSideCombine: Boolean = false): ShuffleDependency[Any, Any, Any] = {
    val serializer: Serializer = new JavaSerializer(new SparkConf(false))
    val partitioner: Partitioner = new HashPartitioner(numPartitions)
    val dep = mock(classOf[ShuffleDependency[Any, Any, Any]], new RuntimeExceptionAnswer())
    doReturn(0).when(dep).shuffleId
    doReturn(partitioner).when(dep).partitioner
    doReturn(serializer).when(dep).serializer
    doReturn(None).when(dep).keyOrdering
    doReturn(None: Option[Aggregator[Any, Any, Any]]).when(dep).aggregator
    doReturn(mapSideCombine).when(dep).mapSideCombine
    dep
  }

  // Tier one: selection through the ShuffleManager factory.

  test("the streaming short name resolves to StreamingShuffleManager through the factory") {
    val conf = new SparkConf(false).set(SHUFFLE_MANAGER, StreamingShuffleManager.SHORT_NAME)
    assert(ShuffleManager.getShuffleManagerClassName(conf) === streamingManagerClassName,
      "spark.shuffle.manager=streaming must resolve to the streaming manager through the " +
        "short-name table, which is the only selection mechanism the trait offers")
    assert(StreamingShuffleManager.SHORT_NAME === "streaming",
      "the selector an operator writes is published as a constant so it cannot drift from the " +
        "class it names")
  }

  test("the resolved class name loads to exactly the streaming manager class") {
    val conf = streamingConf()
    val resolved = ShuffleManager.getShuffleManagerClassName(conf)
    val loaded = Utils.classForName[ShuffleManager](resolved)
    assert(loaded === classOf[StreamingShuffleManager],
      s"the name '$resolved' resolved from the short-name table must load to the streaming " +
        "manager class itself, not merely to something assignable to ShuffleManager")
    assert(classOf[ShuffleManager].isAssignableFrom(loaded),
      "the streaming manager must be a ShuffleManager for the reflective factory to accept it")
  }

  test("short name resolution is case insensitive because it lower-cases with Locale.ROOT") {
    val spellings = Seq("streaming", "STREAMING", "Streaming", "sTrEaMiNg")
    spellings.foreach { spelling =>
      val conf = new SparkConf(false).set(SHUFFLE_MANAGER, spelling)
      assert(ShuffleManager.getShuffleManagerClassName(conf) === streamingManagerClassName,
        s"the short name '$spelling' must resolve to the streaming manager, because the table is " +
          "consulted with the value lower-cased under Locale.ROOT")
      assert(spelling.toLowerCase(Locale.ROOT) === StreamingShuffleManager.SHORT_NAME,
        s"'$spelling' must fold to the published short name under Locale.ROOT, which is the key " +
          "the table is actually looked up with")
    }
  }

  test("an unrecognised shuffle manager value passes through as a class name unchanged") {
    val customName = "com.example.shuffle.CustomShuffleManager"
    val conf = new SparkConf(false).set(SHUFFLE_MANAGER, customName)
    assert(ShuffleManager.getShuffleManagerClassName(conf) === customName,
      "a value that is not a registered short name must pass through unchanged, so operators can " +
        "still name their own ShuffleManager implementation")
    val streamingByClassName =
      new SparkConf(false).set(SHUFFLE_MANAGER, streamingManagerClassName)
    assert(ShuffleManager.getShuffleManagerClassName(streamingByClassName) ===
      streamingManagerClassName,
      "naming the streaming manager by its fully qualified class name must work as well as " +
        "naming it by its short name, since the fall-through returns the value verbatim")
  }

  test("the sort short names and the sort default are unaffected by the streaming entry") {
    Seq("sort", "tungsten-sort").foreach { shortName =>
      val conf = new SparkConf(false).set(SHUFFLE_MANAGER, shortName)
      assert(ShuffleManager.getShuffleManagerClassName(conf) === sortManagerClassName,
        s"the pre-existing short name '$shortName' must still resolve to SortShuffleManager; " +
          "streaming coexists with sort-based shuffle rather than replacing it")
    }
    assert(SHUFFLE_MANAGER.defaultValue === Some("sort"),
      "sort must remain the documented default value of spark.shuffle.manager")
    val untouched = new SparkConf(false)
    assert(ShuffleManager.getShuffleManagerClassName(untouched) === sortManagerClassName,
      "a configuration that never mentions spark.shuffle.manager must still get sort-based " +
        "shuffle, which is what makes the whole feature opt-in")
    assert(sortManagerClassName !== streamingManagerClassName,
      "the two managers must be distinct classes for the coexistence claim to mean anything")
  }

  test("the factory instantiates the streaming manager on the driver and on an executor") {
    // create() goes through the reflective instantiation helper, so this exercises the mandated
    // (SparkConf, isDriver: Boolean) constructor shape and not merely a `new` the test itself
    // wrote.
    Seq(true, false).foreach { isDriver =>
      val manager = ShuffleManager.create(gatedOffStreamingConf(), isDriver)
      try {
        assert(manager.isInstanceOf[StreamingShuffleManager],
          s"ShuffleManager.create(conf, isDriver = $isDriver) must instantiate the streaming " +
            s"manager, but produced ${manager.getClass.getName}")
      } finally {
        manager.stop()
      }
    }
  }

  test("the constructor takes exactly a SparkConf and an isDriver flag") {
    // The trait's own scaladoc fixes this shape: SparkEnv instantiates the manager reflectively
    // with a SparkConf and a boolean.
    val constructors = classOf[StreamingShuffleManager].getConstructors
    val mandated = constructors.filter { candidate =>
      candidate.getParameterTypes.toSeq === Seq(classOf[SparkConf], classOf[Boolean])
    }
    assert(mandated.length === 1,
      "exactly one (SparkConf, Boolean) constructor must exist, because that is the signature " +
        "the reflective factory looks for; found " +
        constructors.map(_.toString).mkString("; "))
  }

  test("only the seven-argument getReader is overridden; the final five-argument form is not") {
    val declared = classOf[StreamingShuffleManager].getDeclaredMethods
      .filter(_.getName === "getReader")
    val sevenArg = declared.filter(_.getParameterCount === 7)
    val fiveArg = declared.filter(_.getParameterCount === 5)
    assert(sevenArg.length === 1,
      "the streaming manager must declare exactly one seven-argument getReader, which is the " +
        "only abstract reader factory in the trait; found " +
        sevenArg.map(_.toString).mkString("; "))
    assert(!Modifier.isFinal(sevenArg.head.getModifiers),
      "the seven-argument getReader must be a plain override rather than final, or no further " +
        "specialisation of the manager would be possible")
    assert(fiveArg.forall(candidate => Modifier.isFinal(candidate.getModifiers)),
      "every five-argument getReader on this class must be final, which is the signature of the " +
        "compiler-generated forwarder for the trait's final method. A non-final one would mean " +
        "the overload had been overridden. Found " + fiveArg.map(_.toString).mkString("; "))
    val sortReaders = classOf[SortShuffleManager].getDeclaredMethods
      .filter(_.getName === "getReader")
    assert(readerShapeOf(declared) === readerShapeOf(sortReaders),
      "the streaming manager's reader factories must have the same arity and finality shape as " +
        s"SortShuffleManager's, but got ${readerShapeOf(declared)} against " +
        s"${readerShapeOf(sortReaders)}")
    val traitReaders = classOf[ShuffleManager].getDeclaredMethods.filter(_.getName === "getReader")
    assert(traitReaders.exists(candidate =>
      candidate.getParameterCount === 5 && !Modifier.isAbstract(candidate.getModifiers)),
      "the trait itself must still offer the five-argument convenience overload as a concrete " +
        "method, since that is what makes overriding it unnecessary")
    assert(traitReaders.exists(candidate =>
      candidate.getParameterCount === 7 && Modifier.isAbstract(candidate.getModifiers)),
      "the trait must still declare the seven-argument form abstractly; it is the member every " +
        "manager is obliged to implement")
  }

  test("the inherited five-argument getReader forwards the whole map range to the override") {
    val probe = new RecordingReaderStreamingShuffleManager(gatedOffStreamingConf())
    try {
      val handle = new BaseShuffleHandle(shuffleId = 7, dependency = shuffleDep())
      val context = mock(classOf[TaskContext], new RuntimeExceptionAnswer())
      val metrics = mock(classOf[ShuffleReadMetricsReporter], new RuntimeExceptionAnswer())
      probe.getReader[Any, Any](handle, 3, 9, context, metrics)
      assert(probe.recorded === Seq((0, Int.MaxValue, 3, 9)),
        "the final five-argument overload must forward (0, Int.MaxValue) as the map range and " +
          "pass the reduce range through untouched, but recorded " + probe.recorded)
    } finally {
      probe.stop()
    }
  }

  test("unregisterShuffle reports success and is idempotent on both tiers") {
    Seq(
      ("the kill switch engaged", gatedOffStreamingConf(), true),
      ("streaming enabled on an executor", streamingConf(), false)).foreach {
      case (posture, conf, isDriver) =>
        withManager(conf, isDriver) { manager =>
          assert(manager.unregisterShuffle(11),
            s"unregisterShuffle must report success with $posture, because the sort delegate's " +
              "bookkeeping is the only verdict the caller can act on")
          assert(manager.unregisterShuffle(11),
            s"unregisterShuffle must be idempotent with $posture; the block manager's cleaner " +
              "reaches every JVM and may ask more than once")
        }
    }
  }

  test("a requested streaming subsystem that cannot activate says so once, at warning level") {
    // The decline this covers is silent and entirely self-consistent without it: the streaming
    // manager is instantiated, its metrics source registers, four MBeans appear, and every one of
    // them reads a plausible 0 forever. An operator following the monitoring documentation cannot
    // then distinguish an idle executor from a configuration that excluded streaming, which
    // is exactly the diagnosis this line exists to make possible.
    val requestedWithoutAuth = testEnvelopeConf()
      .set(SHUFFLE_MANAGER, StreamingShuffleManager.SHORT_NAME)
      .set(SHUFFLE_STREAMING_ENABLED, true)
      .set(NETWORK_AUTH_ENABLED, false)

    val appender = new LogAppender("streaming requested but excluded")
    withLogAppender(
      appender,
      loggerNames = Seq(classOf[StreamingShuffleManager].getName),
      level = Some(Level.WARN)) {
      withManager(requestedWithoutAuth, isDriver = true) { manager =>
        assert(manager.shuffleBlockResolver != null,
          "the excluded manager must still be fully constructed and delegating")
      }
    }
    val excluded = appender.loggingEvents
      .map(_.getMessage.getFormattedMessage)
      .filter(_.contains("Streaming shuffle was requested"))
    assert(excluded.size == 1,
      s"exactly one line must report the exclusion but ${excluded.size} did: ${excluded.mkString}")
    assert(excluded.head.contains(NETWORK_AUTH_ENABLED.key),
      s"the line must name the setting that excluded streaming, but read: ${excluded.head}")
    assert(excluded.head.contains("sort-based shuffle manager"),
      s"the line must say which manager serves the shuffles instead, but read: ${excluded.head}")

    // The kill switch is not an exclusion to report: an operator who left streaming off asked for
    // precisely this, and a warning on every executor of every sort-based application would be
    // noise. Likewise a fully active configuration has nothing to say.
    val quietPostures = Seq(
      ("the kill switch engaged", gatedOffStreamingConf(), true),
      ("a fully active streaming configuration", streamingConf(), false))
    quietPostures.foreach { case (posture, conf, isDriver) =>
      val quiet = new LogAppender(s"no exclusion notice with $posture")
      withLogAppender(
        quiet,
        loggerNames = Seq(classOf[StreamingShuffleManager].getName),
        level = Some(Level.WARN)) {
        withManager(conf, isDriver) { manager =>
          assert(manager.shuffleBlockResolver != null, s"the manager must construct with $posture")
        }
      }
      val notices = quiet.loggingEvents
        .map(_.getMessage.getFormattedMessage)
        .filter(_.contains("Streaming shuffle was requested"))
      assert(notices.isEmpty,
        s"nothing may be reported with $posture, but ${notices.mkString("; ")} was")
    }
  }

  test("stop is idempotent and never throws on both tiers") {
    Seq(
      ("the kill switch engaged", gatedOffStreamingConf(), true),
      ("streaming enabled on an executor", streamingConf(), false)).foreach {
      case (posture, conf, isDriver) =>
        val manager = new StreamingShuffleManager(conf, isDriver)
        manager.unregisterShuffle(3)
        manager.stop()
        manager.stop()
        manager.stop()
        assert(manager.shuffleBlockResolver != null,
          s"the resolver reference must survive shutdown with $posture, so that a late block " +
            "request is answered by a stopped resolver rather than by a null dereference")
    }
  }

  test("the shuffle block resolver is present and never null on either tier") {
    Seq(
      ("the kill switch engaged", gatedOffStreamingConf(), true),
      ("streaming enabled on an executor", streamingConf(), false)).foreach {
      case (posture, conf, isDriver) =>
        withManager(conf, isDriver) { manager =>
          assert(manager.shuffleBlockResolver != null,
            s"shuffleBlockResolver is abstract and mandatory, so it must be present with $posture")
          assert(manager.shuffleBlockResolver eq manager.shuffleBlockResolver,
            s"the resolver must be a stable val rather than a fresh instance per call with " +
              s"$posture; the block manager and ShuffleWriteProcessor both hold on to it")
        }
    }
  }

  private def readerShapeOf(methods: Array[java.lang.reflect.Method]): Seq[(Int, Boolean)] = {
    methods
      .map(method => (method.getParameterCount, Modifier.isFinal(method.getModifiers)))
      .toSeq
      .sorted
  }

  private def pushBasedMergeWouldEngage(manager: ShuffleManager): Boolean = {
    manager.shuffleBlockResolver match {
      case _: IndexShuffleBlockResolver => true
      case _ => false
    }
  }

  test("with streaming enabled the resolver is not an index resolver so push merge declines") {
    withManager(streamingConf(), isDriver = false) { manager =>
      val resolver = manager.shuffleBlockResolver
      assert(!resolver.isInstanceOf[IndexShuffleBlockResolver],
        "the streaming resolver must not present itself as an IndexShuffleBlockResolver: doing " +
          "so would switch push-based merge back on for streamed writes and then push a data " +
          s"file streaming never materialises. Got ${resolver.getClass.getName}")
      assert(!pushBasedMergeWouldEngage(manager),
        "ShuffleWriteProcessor's resolver match must take its empty default branch while " +
          "streaming is enabled, which is what lets that file stay unmodified")
    }
  }

  test("with streaming enabled the resolver is not a MigratableResolver") {
    withManager(streamingConf(), isDriver = false) { manager =>
      val resolver = manager.shuffleBlockResolver
      assert(!resolver.isInstanceOf[MigratableResolver],
        "the resolver published while streaming is enabled must not present itself as a " +
          "MigratableResolver: streamed output cannot be migrated, so claiming otherwise would " +
          s"offer the decommissioner a migration that cannot succeed. Got ${resolver.getClass}")
      assert(!resolver.isInstanceOf[IndexShuffleBlockResolver],
        "the same resolver must not be an IndexShuffleBlockResolver either, which is the " +
          s"stronger statement the negative above rests on. Got ${resolver.getClass.getName}")

      val declared = resolver.getClass.getMethods.map(_.getName).toSet
      Seq("getStoredShuffles", "addShuffleToSkip", "putShuffleBlockAsStream",
          "getMigrationBlocks").foreach { member =>
        assert(!declared.contains(member),
          s"a resolver that declines the migration contract must not declare $member, because a " +
            "declared member is what would make the cast succeed and the migration then fail")
      }
    }
  }

  test("with the kill switch engaged the resolver is migratable again") {
    withManager(gatedOffStreamingConf(), isDriver = false) { manager =>
      assert(manager.shuffleBlockResolver.isInstanceOf[MigratableResolver],
        "with the gate closed the published resolver must be migratable, because it is the sort " +
          s"delegate's own. Got ${manager.shuffleBlockResolver.getClass.getName}")
    }
  }

  test("with the kill switch engaged the resolver is the sort delegate's index resolver") {
    val conf = gatedOffStreamingConf()
    withManager(conf, isDriver = true) { manager =>
      assert(manager.shuffleBlockResolver.isInstanceOf[IndexShuffleBlockResolver],
        "with the gate closed the delegate's own index resolver must be published unchanged, so " +
          "that push-based merge behaves exactly as it does under sort-based shuffle. Got " +
          manager.shuffleBlockResolver.getClass.getName)
      assert(pushBasedMergeWouldEngage(manager),
        "ShuffleWriteProcessor's push-merge branch must fire with the gate closed; that is part " +
          "of what makes the disabled posture indistinguishable from sort")
      withSortManager(conf) { sort =>
        assert(manager.shuffleBlockResolver.getClass === sort.shuffleBlockResolver.getClass,
          "the resolver published with the gate closed must be the same class a standalone " +
            "SortShuffleManager publishes, not merely something index-shaped")
      }
    }
  }

  test("the streaming resolver refuses merged block requests because streaming never merges") {
    val resolver = new StreamingShuffleBlockResolver(streamingConf())
    try {
      val blockId = ShuffleMergedBlockId(1, 0, 2)
      val dataFailure = intercept[UnsupportedOperationException] {
        resolver.getMergedBlockData(blockId, None)
      }
      assert(dataFailure.getMessage.contains("push-based"),
        "the refusal must explain that streaming does not participate in push-based merge, so an " +
          s"operator can act on it; got '${dataFailure.getMessage}'")
      val metaFailure = intercept[UnsupportedOperationException] {
        resolver.getMergedBlockMeta(blockId, None)
      }
      assert(metaFailure.getMessage.contains("push-based"),
        "merged metadata must be refused for the same stated reason as merged data; got " +
          s"'${metaFailure.getMessage}'")
    } finally {
      resolver.stop()
    }
  }

  test("the behaviour gate defaults to off so selecting the manager alone changes nothing") {
    val selectedOnly =
      new SparkConf(false).set(SHUFFLE_MANAGER, StreamingShuffleManager.SHORT_NAME)
    assert(!selectedOnly.get(SHUFFLE_STREAMING_ENABLED),
      "selecting the streaming manager must not by itself enable streaming behaviour; the second " +
        "key defaults to false so the feature is opt-in twice over")
    assert(ShuffleManager.getShuffleManagerClassName(selectedOnly) === streamingManagerClassName,
      "the class is still selected with the gate closed, which is exactly what gives an operator " +
        "a kill switch that needs no change of manager class")
  }

  test("with the kill switch engaged registerShuffle produces the sort based handle") {
    val conf = gatedOffStreamingConf()
    val dependency = shuffleDep(numPartitions = 4)
    withManager(conf, isDriver = true) { manager =>
      val handle = manager.registerShuffle(0, dependency)
      assert(!handle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
        "no streaming handle may be minted while the gate is closed; a streaming handle is the " +
          s"commitment to stream, and there must be none. Got ${handle.getClass.getName}")
      withSortManager(conf) { sort =>
        val sortHandle = sort.registerShuffle(0, dependency)
        assert(handle.getClass === sortHandle.getClass,
          "the handle produced with the gate closed must be the very class a standalone " +
            s"SortShuffleManager produces for the same dependency, but got " +
            s"${handle.getClass.getName} against ${sortHandle.getClass.getName}")
      }
    }
  }

  test("a shuffle may only be registered from the driver even with streaming enabled") {
    withManager(streamingConf(), isDriver = false) { manager =>
      val handle = manager.registerShuffle(0, shuffleDep(numPartitions = 4))
      assert(!handle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
        "an executor must not mint a streaming handle; only the driver's instance registers a " +
          s"shuffle. Got ${handle.getClass.getName}")
    }
  }

  test("the set of fallback conditions is closed at the four documented members") {
    val reasons = StreamingShuffleFallbackReason.all
    assert(reasons.size === 4,
      "there must be exactly four fallback conditions, matching the four documented ones; a " +
        "fifth would be a degradation path with no specified terminus. Got " +
        reasons.mkString(", "))
    assert(reasons.distinct.size === reasons.size,
      s"the fallback conditions must be distinct, but got ${reasons.mkString(", ")}")
    assert(reasons.toSet === Set(
      StreamingShuffleFallbackReason.ConsumerTooSlow,
      StreamingShuffleFallbackReason.MemoryPressure,
      StreamingShuffleFallbackReason.NetworkSaturation,
      StreamingShuffleFallbackReason.ProtocolVersionMismatch),
      "the four conditions must be exactly consumer slowness, memory pressure, network " +
        "saturation and protocol version mismatch, but got " + reasons.mkString(", "))
    reasons.foreach { reason =>
      assert(reason.description.nonEmpty,
        s"$reason must describe itself, because its description is what an operator reads in the " +
          "log line explaining why streaming stood down")
    }
  }

  test("the kill switch alone routes to the sort delegate") {
    val policy = new StreamingShuffleFallbackPolicy(gatedOffStreamingConf())
    assert(policy.killSwitchEngaged,
      "a closed gate must read as an engaged kill switch")
    assert(!policy.streamingActive,
      "streaming must not be active while the kill switch is engaged")
    assert(policy.shouldDelegateToSortShuffle,
      "an engaged kill switch must route to the sort delegate, which is the same terminus every " +
        "fallback condition uses")
    assert(!policy.hasTripped,
      "the kill switch is an operator decision rather than an observation, so it must not be " +
        "reported as a tripped condition")
  }

  test("every fallback condition routes the policy to the same sort delegate terminus") {
    // Each condition is tripped through the policy's own public surface, with time supplied
    // explicitly rather than slept for, so the case is deterministic.
    val trips: Seq[(StreamingShuffleFallbackReason, StreamingShuffleFallbackPolicy => Unit)] = Seq(
      (StreamingShuffleFallbackReason.MemoryPressure,
        policy => policy.recordAllocationGrant(requestedBytes = 1024L, grantedBytes = 512L)),
      (StreamingShuffleFallbackReason.NetworkSaturation,
        // Above the ninety percent share across the run of measurement intervals the condition is
        // sustained over. One reading is not the condition: this subsystem's own pacing bucket must
        // admit one maximum-sized frame, which reads above the share for one interval on a small
        // administered link, so a rule tripping on one reading stood healthy shuffles down.
        policy => driveLinkSaturation(policy, 990.0d, 1000.0d)),
      (StreamingShuffleFallbackReason.ProtocolVersionMismatch,
        policy => policy.checkProtocolVersion((ProtocolVersion + 1).toByte)),
      (StreamingShuffleFallbackReason.ConsumerTooSlow, policy => {
        val shuffleId = 1
        val armedAt = ManualClockEpochMillis
        policy.recordProducerThroughput(shuffleId, 1000.0d, armedAt)
        policy.recordConsumerThroughput(shuffleId, 100.0d, armedAt)
        policy.recordConsumerThroughput(shuffleId, 100.0d, armedAt + SustainedSlownessTripMillis)
      }))
    trips.foreach { case (expected, trip) =>
      val policy = new StreamingShuffleFallbackPolicy(streamingConf())
      assert(policy.streamingActive,
        s"streaming must start active before the $expected condition is applied")
      trip(policy)
      assert(policy.hasTripped,
        s"the $expected condition must trip the policy")
      assert(policy.trippedReason === Some(expected),
        s"the latched reason must be $expected, but was ${policy.trippedReason}")
      assert(!policy.streamingActive,
        s"streaming must not remain active after the $expected condition trips")
      assert(policy.shouldDelegateToSortShuffle,
        s"the $expected condition must route to the sort delegate, the same terminus the kill " +
          "switch uses; that shared terminus is how zero regression is guaranteed structurally")
    }
  }

  test("every fallback condition makes a live manager delegate every service provider call") {
    // The verdict a standalone policy returns is not the claim that matters.
    val baseline = sortBaselineGroupedOutput(numPartitions = 4)
    assert(baseline.nonEmpty, "the sort-based baseline must produce output to compare against")
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-live-fallback", "local[2]"))
    val manager = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]
    val context = fakeTaskContext(sc)
    val resolverBeforeAnyTrip = manager.shuffleBlockResolver
    assert(resolverBeforeAnyTrip.isInstanceOf[StreamingShuffleBlockRouter],
      s"a manager with streaming enabled must publish the routing resolver, but published " +
        s"${resolverBeforeAnyTrip.getClass.getName}")

    withSortManager(sc.conf) { sort =>
      StreamingShuffleFallbackReason.all.zipWithIndex.foreach { case (reason, index) =>
        manager.streamingFallbackPolicy.reset()
        assert(manager.streamingFallbackPolicy.streamingActive,
          s"streaming must be back in service before $reason is driven")

        val committed = shuffleDependencyFor(sc, sc.conf, numPartitions = 4, numRecords = 20)
        val streamingHandle = committed.shuffleHandle
        assert(streamingHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
          s"the manager must have committed to streaming before $reason is driven, but it minted " +
            s"${streamingHandle.getClass.getName}")

        driveFallbackReason(manager.streamingFallbackPolicy, reason, committed.shuffleId)
        assert(manager.streamingFallbackPolicy.trippedReason === Some(reason),
          s"$reason must be the condition the live manager's own policy latched, but it latched " +
            s"${manager.streamingFallbackPolicy.trippedReason}")

        val declined = shuffleDependencyFor(sc, sc.conf, numPartitions = 4, numRecords = 20)
        val declinedHandle = declined.shuffleHandle
        val sortHandle = sort.registerShuffle(declined.shuffleId, declined)
        assert(declinedHandle.getClass === sortHandle.getClass,
          s"after $reason, registerShuffle must return ${sortHandle.getClass.getName} but " +
            s"returned ${declinedHandle.getClass.getName}")
        assert(!declinedHandle.getClass.getName.contains(".streaming."),
          s"after $reason no streaming handle may be minted, yet a " +
            s"${declinedHandle.getClass.getName} was")

        val committedWriter = manager.getWriter[Any, Any](streamingHandle, 100L + index, context,
          context.taskMetrics().shuffleWriteMetrics)
        val sortWriter = sort.getWriter[Any, Any](streamingHandle, 200L + index, context,
          context.taskMetrics().shuffleWriteMetrics)
        try {
          assert(committedWriter.getClass === sortWriter.getClass,
            s"after $reason, getWriter must produce ${sortWriter.getClass.getName} even for the " +
              s"streaming handle, but produced ${committedWriter.getClass.getName}")
        } finally {
          committedWriter.stop(success = false)
          sortWriter.stop(success = false)
        }

        val committedReader = manager.getReader[Any, Any](streamingHandle, 0, Int.MaxValue, 0, 1,
          context, context.taskMetrics().createTempShuffleReadMetrics())
        val sortReader = sort.getReader[Any, Any](streamingHandle, 0, Int.MaxValue, 0, 1, context,
          context.taskMetrics().createTempShuffleReadMetrics())
        assert(committedReader.getClass === sortReader.getClass,
          s"after $reason, getReader must produce ${sortReader.getClass.getName} even for the " +
            s"streaming handle, but produced ${committedReader.getClass.getName}")

        assert(manager.unregisterShuffle(declined.shuffleId) ===
            sort.unregisterShuffle(declined.shuffleId),
          s"after $reason, unregisterShuffle must answer as the sort delegate answers")

        assert(manager.shuffleBlockResolver eq resolverBeforeAnyTrip,
          s"$reason must not swap the executor's block resolver, which serves blocks published " +
            "before the trip as well as after it")

        val observed = groupedOutputAsSet(sc, numPartitions = 4)
        assertNoDataLoss(observed, baseline,
          s"a shuffle registered after the live manager stood streaming down for $reason")
      }
    }
  }

  test("a trip observed by a live manager is declared shuffle wide and stands the shuffle down") {
    val baseline = sortBaselineGroupedOutput(numPartitions = 4)
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-live-trip", "local[2]"))
    val manager = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]
    val coordinatorRef = RpcUtils.makeDriverRef(
      StreamingShuffleCoordinator.ENDPOINT_NAME, sc.conf, SparkEnv.get.rpcEnv)
    val context = fakeTaskContext(sc)

    val dependency = shuffleDependencyFor(sc, sc.conf, numPartitions = 4, numRecords = 20)
    val handle = dependency.shuffleHandle.asInstanceOf[StreamingShuffleHandle[Int, Int, Int]]
    assert(manager.degradationPolicy.streamingActive,
      "the live manager's own policy must start active, or the trip below proves nothing")

    val streamingWriter = manager.getWriter[Int, Int](handle, PreTripMapId, context,
      context.taskMetrics().shuffleWriteMetrics)
    try {
      assert(streamingWriter.isInstanceOf[StreamingShuffleWriter[_, _, _]],
        s"with the policy untripped the manager must produce its own writer, but produced " +
          s"${streamingWriter.getClass.getName}")
    } finally {
      streamingWriter.stop(success = false)
    }
    val streamingReader = manager.getReader[Int, Int](handle, 0, Int.MaxValue, 0, 1, context,
      context.taskMetrics().createTempShuffleReadMetrics())
    assert(streamingReader.isInstanceOf[StreamingShuffleReader[_, _]],
      s"with the policy untripped the manager must produce its own reader, but produced " +
        s"${streamingReader.getClass.getName}")

    // The condition is applied to the very policy the service-provider methods consult, which is
    // what a writer or a reader on this executor would have done on observing it.
    // Above the ninety percent share across the sustained run of measurement intervals. Driven
    // through the shared helper, which supplies every instant, so this works against the policy a
    // real manager built for itself and whose clock is the system's.
    driveLinkSaturation(manager.degradationPolicy, 990.0d, 1000.0d)
    assert(manager.degradationPolicy.hasTripped &&
        manager.degradationPolicy.trippedReason
          .contains(StreamingShuffleFallbackReason.NetworkSaturation),
      s"the live manager's policy must have latched network saturation, but held " +
        s"${manager.degradationPolicy.trippedReason}")

    withSortManager(sc.conf) { sort =>
      val trippedWriter = manager.getWriter[Int, Int](handle, 0L, context,
        context.taskMetrics().shuffleWriteMetrics)
      val sortWriter = sort.getWriter[Int, Int](handle, 1L, context,
        context.taskMetrics().shuffleWriteMetrics)
      try {
        assert(trippedWriter.getClass === sortWriter.getClass,
          s"a tripped manager must hand back the writer a standalone SortShuffleManager hands " +
            s"back for the same handle, but handed back ${trippedWriter.getClass.getName} " +
            s"against ${sortWriter.getClass.getName}")
      } finally {
        trippedWriter.stop(success = false)
        sortWriter.stop(success = false)
      }
      val trippedReader = manager.getReader[Int, Int](handle, 0, Int.MaxValue, 0, 1, context,
        context.taskMetrics().createTempShuffleReadMetrics())
      val sortReader = sort.getReader[Int, Int](handle, 0, Int.MaxValue, 0, 1, context,
        context.taskMetrics().createTempShuffleReadMetrics())
      assert(trippedReader.getClass === sortReader.getClass,
        s"a tripped manager must hand back the sort delegate's own reader, but handed back " +
          s"${trippedReader.getClass.getName} against ${sortReader.getClass.getName}")
    }

    val declared = coordinatorRef.askSync[StreamingShuffleFallbackState](
      GetStreamingShuffleFallbackState(handle.shuffleId, handle.capabilityToken))
    assert(declared.fallenBack,
      "the trip must have been declared to the coordinator, so that every other participant of " +
        "the shuffle stands down too")
    assert(declared.reasonName === StreamingShuffleFallbackReason.NetworkSaturation.toString,
      s"the declaration must carry the condition that was observed, but carried " +
        s"${declared.reasonName}")

    val laterDependency = shuffleDependencyFor(sc, sc.conf, numPartitions = 4, numRecords = 20)
    assert(!laterDependency.shuffleHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
      s"a dependency registered after the trip must be declined to the sort delegate, but got " +
        s"${laterDependency.shuffleHandle.getClass.getName}")
    assert(manager.unregisterShuffle(handle.shuffleId),
      "unregisterShuffle must report the delegate's own verdict, which is success")

    val observed = groupedOutputAsSet(sc, numPartitions = 4)
    assertNoDataLoss(observed, baseline, "a shuffle run after the manager stood streaming down")
  }

  test("a fallback verdict reports the declarer's own account, and names a condition only if one " +
      "was observed") {
    val withDetail = StreamingShuffleFallbackState(
      StreamingShuffleFallbackReason.NetworkSaturation.toString,
      declaredAtEpoch = 7L,
      detail = "egress held 94% of the administered link for the whole sampling run")
    assert(withDetail.condition ===
        "egress held 94% of the administered link for the whole sampling run",
      s"a verdict carrying a detail must report it, but reported ${withDetail.condition}")
    assert(withDetail.reason.contains(StreamingShuffleFallbackReason.NetworkSaturation),
      "and the name must still resolve onto this build's closed set")

    val withoutDetail = StreamingShuffleFallbackState(
      StreamingShuffleFallbackReason.ConsumerTooSlow.toString, declaredAtEpoch = 7L)
    assert(withoutDetail.condition === StreamingShuffleFallbackReason.ConsumerTooSlow.description,
      s"a verdict with no detail must fall back to this build's prose for the member, but " +
        s"reported ${withoutDetail.condition}")
    assert(withoutDetail.condition.contains("2x") && withoutDetail.condition.contains("60 seconds"),
      s"and that prose must state the measurement the member IS, narrowly, so a record naming it " +
        s"cannot be read as anything else, but read '${withoutDetail.condition}'")

    val unavailable = StreamingShuffleFallbackState(
      StreamingShuffleFallbackReason.UNAVAILABLE_NAME, declaredAtEpoch = 9L)
    assert(unavailable.fallenBack,
      "the unavailable marker must stand the shuffle down, or a retry would stream again")
    assert(unavailable.reason.isEmpty,
      s"it must resolve to no condition, because it names none, but resolved ${unavailable.reason}")
    assert(StreamingShuffleFallbackReason.fromName(
        StreamingShuffleFallbackReason.UNAVAILABLE_NAME).isEmpty,
      "and it must stay outside the closed set of four")
    assert(StreamingShuffleFallbackReason.all.size === 4,
      s"which must still hold exactly four members, but holds " +
        s"${StreamingShuffleFallbackReason.all.size}")
    assert(StreamingShuffleFallbackReason.isDeclarable(
        StreamingShuffleFallbackReason.UNAVAILABLE_NAME),
      "while still being a record this build is willing to latch")
    assert(!StreamingShuffleFallbackReason.isDeclarable("SomethingThisBuildCannotResolve"),
      "unlike a name it has never heard of, which is refused at the boundary")
    assert(unavailable.condition === StreamingShuffleFallbackReason.UNAVAILABLE_DESCRIPTION,
      s"and its rendering must say that streaming was unavailable rather than naming a resource " +
        s"to tune, but read '${unavailable.condition}'")
    assert(!unavailable.condition.contains("2x") && !unavailable.condition.contains("memory") &&
        !unavailable.condition.contains("link"),
      s"naming no resource at all, but read '${unavailable.condition}'")

    val capability = StreamingShuffleFallbackState(
      StreamingShuffleStandDownCause.UnsupportedReadShape.toString, declaredAtEpoch = 8L)
    assert(capability.fallenBack, "a capability stand-down must read as fallen back")
    assert(capability.reason.isEmpty,
      s"but it must NOT resolve onto the four conditions, because none was measured: " +
        s"${capability.reason}")
    assert(capability.cause.map(_.toString) ===
        Some(StreamingShuffleStandDownCause.UnsupportedReadShape.toString),
      s"and it must resolve as the cause it is, but resolved to ${capability.cause}")
    assert(capability.condition ===
        StreamingShuffleStandDownCause.UnsupportedReadShape.description,
      s"with prose of its own rather than a member's, but read '${capability.condition}'")

    val unresolvable = StreamingShuffleFallbackState("SomethingThisBuildCannotResolve", 7L)
    assert(unresolvable.condition === "SomethingThisBuildCannotResolve",
      s"and a name this build cannot resolve must be reported verbatim rather than as prose it " +
        s"cannot produce, but reported ${unresolvable.condition}")
    assert(unresolvable.cause.isEmpty && unresolvable.reason.isEmpty,
      "a name in neither set must resolve to nothing at all")

    val stillStreaming = StreamingShuffleFallbackState()
    assert(!stillStreaming.fallenBack,
      "a verdict for a shuffle that is still streaming must not read as fallen back")
    assert(stillStreaming.condition === StreamingShuffleCoordinator.NO_FALLBACK_REASON,
      s"and it must describe no condition at all, because there is none to describe, but " +
        s"described '${stillStreaming.condition}'")
  }

  test("a narrowed map range is served by the streaming reader and filtered exactly") {
    val baseline = sortBaselineGroupedOutput(numPartitions = 4)
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-narrowed-range", "local[2]"))
    val manager = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]
    val coordinatorRef = RpcUtils.makeDriverRef(
      StreamingShuffleCoordinator.ENDPOINT_NAME, sc.conf, SparkEnv.get.rpcEnv)
    val context = fakeTaskContext(sc)

    val dependency = shuffleDependencyFor(sc, sc.conf, numPartitions = 4, numRecords = 20)
    val handle = dependency.shuffleHandle.asInstanceOf[StreamingShuffleHandle[Int, Int, Int]]
    assert(manager.getReader[Int, Int](handle, 0, Int.MaxValue, 0, 1, context,
        context.taskMetrics().createTempShuffleReadMetrics())
        .isInstanceOf[StreamingShuffleReader[_, _]],
      "the whole map range must be served by the streaming reader")

    Seq((1, 3), (0, 1), (2, 7)).foreach { case (startMapIndex, endMapIndex) =>
      val reader = manager.getReader[Int, Int](handle, startMapIndex, endMapIndex, 0, 1, context,
        context.taskMetrics().createTempShuffleReadMetrics())
      assert(reader.isInstanceOf[StreamingShuffleReader[_, _]],
        s"the narrowed map range [$startMapIndex, $endMapIndex) must be served by the streaming " +
          s"reader too, but was served by ${reader.getClass.getName}")
      assert(!StreamingShuffleReader.servesFullMapRange(startMapIndex, endMapIndex),
        s"[$startMapIndex, $endMapIndex) must be a narrowed range, or this case proves nothing")
    }

    val declared = coordinatorRef.askSync[StreamingShuffleFallbackState](
      GetStreamingShuffleFallbackState(handle.shuffleId, handle.capabilityToken))
    assert(!declared.fallenBack,
      s"a valid narrowed read must leave the shuffle streaming, but it stood down for " +
        s"${declared.reasonName}")
    assert(declared.reasonName === StreamingShuffleCoordinator.NO_FALLBACK_REASON,
      s"and no condition may be recorded at all, but '${declared.reasonName}' was")

    val locations = (0 until 4).map { mapIndex =>
      StreamingShuffleProducerLocation(executorId = s"exec-$mapIndex", host = "producer-host",
        port = 7337 + mapIndex, mapId = 100L + mapIndex, mapIndex = mapIndex,
        taskAttemptId = 100L + mapIndex,
        blockManagerId = BlockManagerId(s"exec-$mapIndex", "producer-host", 7337 + mapIndex))
    }
    val reply = StreamingShuffleProducerLocations(shuffleId = handle.shuffleId,
      locations = locations, numPartitions = 4, numMaps = 4, completedMapIndexes = Set.empty,
      protocolVersion = StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, coordinatorEpoch = 3L,
      fallback = StreamingShuffleFallbackState())
    Seq((0, Int.MaxValue, Seq(0, 1, 2, 3)), (1, 3, Seq(1, 2)), (3, 4, Seq(3)),
      (4, 9, Seq.empty[Int])).foreach { case (startMapIndex, endMapIndex, expected) =>
      val reader = manager.getReader[Int, Int](handle, startMapIndex, endMapIndex, 0, 1, context,
        context.taskMetrics().createTempShuffleReadMetrics())
        .asInstanceOf[StreamingShuffleReader[Int, Int]]
      assert(reader.servedMapIndexesOf(reply) === expected,
        s"[$startMapIndex, $endMapIndex) must select map indexes ${expected.mkString(", ")} but " +
          s"selected ${reader.servedMapIndexesOf(reply).mkString(", ")}")
    }

    val observed = groupedOutputAsSet(sc, numPartitions = 4)
    assertNoDataLoss(observed, baseline, "a shuffle read through the streaming reader")
  }

  test("an enabled external shuffle service declines streaming for the whole application") {
    // The service serves a block by reading the index and data files an executor left on disk, from
    // a process that outlives that executor.
    val conf = streamingConf().set(SHUFFLE_SERVICE_ENABLED, true)
    val dependency = shuffleDep(numPartitions = 4)
    withManager(conf, isDriver = true) { manager =>
      val handle = manager.registerShuffle(0, dependency)
      assert(!handle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
        s"an application with the external shuffle service enabled must not be given a streaming " +
          s"handle, but was given ${handle.getClass.getName}")
      withSortManager(conf) { sort =>
        val sortHandle = sort.registerShuffle(0, dependency)
        assert(handle.getClass === sortHandle.getClass,
          s"the handle must be exactly what a standalone SortShuffleManager produces, so the " +
            s"service sees the files it already knows how to serve, but got " +
            s"${handle.getClass.getName} against ${sortHandle.getClass.getName}")
      }
      assert(manager.shuffleBlockResolver.isInstanceOf[IndexShuffleBlockResolver],
        "the resolver published under an enabled service must be the delegate's index resolver, " +
          s"which is the only resolver the service can read, but was " +
          s"${manager.shuffleBlockResolver.getClass.getName}")
      withSortManager(conf) { sort =>
        assert(manager.shuffleBlockResolver.getClass === sort.shuffleBlockResolver.getClass,
          "and it must be the same class a standalone SortShuffleManager publishes, not merely " +
            "something index-shaped")
      }
      assert(pushBasedMergeWouldEngage(manager),
        "ShuffleWriteProcessor's push-merge branch must fire under an enabled service, because " +
          "that is part of what makes this posture indistinguishable from sort-based shuffle")
    }
  }

  test("each fallback condition routes a live manager's writer reader and unregister to sort") {
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-conditions", "local[2]"))
    val context = fakeTaskContext(sc)
    val coordinatorRef = RpcUtils.makeDriverRef(
      StreamingShuffleCoordinator.ENDPOINT_NAME, sc.conf, SparkEnv.get.rpcEnv)
    var mapId = 0L

    val trips: Seq[(StreamingShuffleFallbackReason, StreamingShuffleFallbackPolicy => Unit)] = Seq(
      (StreamingShuffleFallbackReason.MemoryPressure,
        policy => policy.recordAllocationGrant(requestedBytes = 1024L, grantedBytes = 512L)),
      (StreamingShuffleFallbackReason.NetworkSaturation,
        // Above the ninety percent share across the sustained run of measurement intervals, for the
        // reason given at the first of these tables: one legal burst is not a saturated link.
        policy => driveLinkSaturation(policy, 990.0d, 1000.0d)),
      (StreamingShuffleFallbackReason.ProtocolVersionMismatch,
        policy => policy.checkProtocolVersion((ProtocolVersion + 1).toByte)),
      (StreamingShuffleFallbackReason.ConsumerTooSlow, policy => {
        val armedAt = ManualClockEpochMillis
        policy.recordProducerThroughput(ThroughputProbeShuffleId, 1000.0d, armedAt)
        policy.recordConsumerThroughput(ThroughputProbeShuffleId, 100.0d, armedAt)
        policy.recordConsumerThroughput(
          ThroughputProbeShuffleId, 100.0d, armedAt + SustainedSlownessTripMillis)
      }))

    trips.foreach { case (reason, trip) =>
      val dependency = shuffleDependencyFor(sc, sc.conf, numPartitions = 2, numRecords = 12)
      val handle = dependency.shuffleHandle.asInstanceOf[StreamingShuffleHandle[Int, Int, Int]]
      val manager = new StreamingShuffleManager(sc.conf, isDriver = false)
      try {
        assert(manager.degradationPolicy.streamingActive,
          s"the manager must start active before the $reason condition is applied")
        val streamingWriter = manager.getWriter[Int, Int](handle, mapId, context,
          context.taskMetrics().shuffleWriteMetrics)
        mapId += 1L
        try {
          assert(streamingWriter.isInstanceOf[StreamingShuffleWriter[_, _, _]],
            s"before the $reason condition the manager must produce its own writer, but produced " +
              s"${streamingWriter.getClass.getName}")
        } finally {
          streamingWriter.stop(success = false)
        }
        trip(manager.degradationPolicy)
        assert(manager.degradationPolicy.trippedReason.contains(reason),
          s"the manager's own policy must latch $reason, but held " +
            s"${manager.degradationPolicy.trippedReason}")

        withSortManager(sc.conf) { sort =>
          val trippedWriter = manager.getWriter[Int, Int](handle, mapId, context,
            context.taskMetrics().shuffleWriteMetrics)
          mapId += 1L
          val sortWriter = sort.getWriter[Int, Int](handle, mapId, context,
            context.taskMetrics().shuffleWriteMetrics)
          mapId += 1L
          try {
            assert(trippedWriter.getClass === sortWriter.getClass,
              s"the $reason condition must route getWriter to the sort delegate, but produced " +
                s"${trippedWriter.getClass.getName} against ${sortWriter.getClass.getName}")
          } finally {
            trippedWriter.stop(success = false)
            sortWriter.stop(success = false)
          }
          val trippedReader = manager.getReader[Int, Int](handle, 0, Int.MaxValue, 0, 1, context,
            context.taskMetrics().createTempShuffleReadMetrics())
          val sortReader = sort.getReader[Int, Int](handle, 0, Int.MaxValue, 0, 1, context,
            context.taskMetrics().createTempShuffleReadMetrics())
          assert(trippedReader.getClass === sortReader.getClass,
            s"the $reason condition must route getReader to the sort delegate, but produced " +
              s"${trippedReader.getClass.getName} against ${sortReader.getClass.getName}")
        }

        val declared = coordinatorRef.askSync[StreamingShuffleFallbackState](
          GetStreamingShuffleFallbackState(handle.shuffleId, handle.capabilityToken))
        assert(declared.fallenBack && declared.reasonName === reason.toString,
          s"the $reason condition must be declared shuffle wide before it is acted upon, but the " +
            s"coordinator holds ${declared.reasonName}")
        assert(manager.unregisterShuffle(handle.shuffleId),
          s"unregisterShuffle must reach the delegate and report its verdict after the $reason " +
            "condition")
      } finally {
        manager.stop()
        manager.stop()
      }
    }
  }

  test("streaming channels require the platform auth handshake and fail closed without it") {
    val authenticated = new SparkConf(false)
      .set(NETWORK_AUTH_ENABLED, true)
      .set(AUTH_SECRET, "streaming-shuffle-manager-suite-secret")
      .set("spark.app.id", "streaming-shuffle-auth")
    val plaintext = new SparkConf(false)
      .set(NETWORK_AUTH_ENABLED, false)
      .set("spark.app.id", "streaming-shuffle-plaintext")

    val authenticatedTransport = StreamingShuffleServerHandler.streamingTransportConf(
      authenticated, numUsableCores = 1, security = None)
    val plaintextTransport = StreamingShuffleServerHandler.streamingTransportConf(
      plaintext, numUsableCores = 1, security = None)
    assert(authenticatedTransport.enableTcpKeepAlive() && plaintextTransport.enableTcpKeepAlive(),
      "OS keepalive must be enabled for the streaming module on both postures, since the " +
        "five-second liveness bound is enforced by the protocol's own heartbeat above it")

    val authenticatedSecurity = new SecurityManager(authenticated)
    assert(authenticatedSecurity.isAuthenticationEnabled(),
      "the authenticated configuration must really enable authentication, or the assertions " +
        "below would pass for the wrong reason")
    val clientBootstraps = StreamingShuffleServerHandler.streamingClientBootstraps(
      authenticated, authenticatedTransport, Some(authenticatedSecurity))
    val serverBootstraps = StreamingShuffleServerHandler.streamingServerBootstraps(
      authenticatedTransport, Some(authenticatedSecurity))
    assert(clientBootstraps.size() === 1 &&
        clientBootstraps.get(0).isInstanceOf[AuthClientBootstrap],
      s"a consumer channel must complete the platform's auth handshake before a frame is " +
        s"exchanged, but its bootstraps were " +
        s"${clientBootstraps.asScala.map(_.getClass.getName).mkString("[", ", ", "]")}")
    assert(serverBootstraps.size() === 1 &&
        serverBootstraps.get(0).isInstanceOf[AuthServerBootstrap],
      s"a producer server must require the handshake, or it would serve one map task's output to " +
        s"any peer that could reach the port, but its bootstraps were " +
        s"${serverBootstraps.asScala.map(_.getClass.getName).mkString("[", ", ", "]")}")

    val plaintextSecurity = new SecurityManager(plaintext)
    assert(!plaintextSecurity.isAuthenticationEnabled(),
      "the plaintext configuration must really disable authentication")
    val plaintextClientRefusal = intercept[SparkException] {
      StreamingShuffleServerHandler.streamingClientBootstraps(
        plaintext, plaintextTransport, Some(plaintextSecurity))
    }
    assert(plaintextClientRefusal.getMessage.contains(NETWORK_AUTH_ENABLED.key),
      s"the client refusal must name the missing security gate, but said " +
        s"${plaintextClientRefusal.getMessage}")
    val plaintextServerRefusal = intercept[SparkException] {
      StreamingShuffleServerHandler.streamingServerBootstraps(
        plaintextTransport, Some(plaintextSecurity))
    }
    assert(plaintextServerRefusal.getMessage.contains(NETWORK_AUTH_ENABLED.key),
      s"the server refusal must name the missing security gate, but said " +
        s"${plaintextServerRefusal.getMessage}")

    intercept[SparkException] {
      StreamingShuffleServerHandler.streamingClientBootstraps(
        authenticated, authenticatedTransport, security = None)
    }
    intercept[SparkException] {
      StreamingShuffleServerHandler.streamingServerBootstraps(
        authenticatedTransport, security = None)
    }
  }

  test("the coordinator refuses a producer address that could forge a log record") {
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-log-forgery", "local[2]"))
    val coordinator = new StreamingShuffleCoordinator(sc.env.rpcEnv, sc.conf, newManualClock())
    val shuffleId = 11
    val grant = coordinator.registerShuffle(shuffleId, numPartitions = 2, numMaps = 1,
      ProtocolVersion).getOrElse(
      fail("the coordinator must accept a well-formed registration of the current protocol"))

    def register(executorId: String, host: String): Boolean = {
      coordinator.registerProducer(shuffleId, grant.capabilityToken,
        StreamingShuffleProducerLocation(executorId = executorId, host = host, port = 7337,
          mapId = 0L, mapIndex = 0, taskAttemptId = 0L,
          blockManagerId = BlockManagerId(executorId, host, 7337)),
        numPartitions = 2, ProtocolVersion).accepted
    }

    Seq(
      (0x2028.toChar, "U+2028 LINE SEPARATOR"),
      (0x2029.toChar, "U+2029 PARAGRAPH SEPARATOR"),
      ('\n', "a line feed"),
      ('\r', "a carriage return"),
      (0.toChar, "a NUL")).foreach { case (character, description) =>
      assert(!register(s"exec$character-1", "producer-host"),
        s"an executor id carrying $description must be refused, because it would let a peer " +
          "forge a line in the driver's log")
      assert(!register("exec-1", s"producer$character-host"),
        s"and a host carrying $description must be refused for the same reason")
    }

    assert(register("exec-1", "producer-host"),
      "a producer whose address carries no line terminator must be registered")
  }

  test("the coordinator refuses an unauthorised caller at every gate without acting") {
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-capability", "local[2]"))
    val coordinator = new StreamingShuffleCoordinator(sc.env.rpcEnv, sc.conf, newManualClock())
    val shuffleId = 7
    val grant = coordinator.registerShuffle(shuffleId, numPartitions = 2, numMaps = 1,
      ProtocolVersion).getOrElse(
      fail("the coordinator must accept a well-formed registration of the current protocol"))
    val token = grant.capabilityToken
    assert(token.nonEmpty, "a registration must mint a capability token")

    assert(StreamingShuffleCoordinator.tokenMatches(token, token),
      "the token a registration minted must authorise its holder")
    val sameLengthWrongToken = "x" * token.length
    assert(!StreamingShuffleCoordinator.tokenMatches(token, sameLengthWrongToken),
      "a token of the right length and the wrong bytes must not authorise anything")
    assert(!StreamingShuffleCoordinator.tokenMatches(token, token.dropRight(1)),
      "a prefix of the token must not authorise anything")
    assert(!StreamingShuffleCoordinator.tokenMatches(token, ""),
      "an empty token must never match, since absence of a credential is not a credential")
    assert(!StreamingShuffleCoordinator.tokenMatches(token, null) &&
        !StreamingShuffleCoordinator.tokenMatches(null, token),
      "a null on either side must never match")

    val deniedBefore = coordinator.deniedOperationCount
    Seq(sameLengthWrongToken, "", "too-short").foreach { presented =>
      assert(!coordinator.declareFallback(shuffleId, presented,
          StreamingShuffleFallbackReason.NetworkSaturation, "an unauthorised declaration")
        .fallenBack,
        s"forcing a shuffle onto the sort-based path is a decision no stranger may drive, but a " +
          s"declaration presenting '$presented' was accepted")
      assert(!coordinator.fallbackStateFor(shuffleId, presented).fallenBack,
        s"a query presenting '$presented' must be answered as the streaming-in-force default")
      assert(coordinator.lookupProducers(shuffleId, presented, 0, 2).isEmpty,
        s"a producer lookup presenting '$presented' must be answered as though the shuffle were " +
          s"unknown, so the shape of the reply leaks nothing")
      assert(!coordinator.unregisterShuffle(shuffleId, presented),
        s"an unregistration presenting '$presented' must be refused")
    }
    assert(coordinator.deniedOperationCount > deniedBefore,
      s"every refusal must be counted, but the denial count stayed at ${deniedBefore}")

    assert(coordinator.activeShuffleIds.contains(shuffleId),
      "a refused unregistration must leave the shuffle registered")
    assert(coordinator.epochFor(shuffleId).contains(grant.coordinatorEpoch),
      s"no refused operation may advance the epoch, but it moved from ${grant.coordinatorEpoch} " +
        s"to ${coordinator.epochFor(shuffleId)}")
    assert(!coordinator.fallbackStateFor(shuffleId, token).fallenBack,
      "read back with the real token, the shuffle must still be streaming")

    val declared = coordinator.declareFallback(shuffleId, token,
      StreamingShuffleFallbackReason.NetworkSaturation, "an authorised declaration")
    assert(declared.fallenBack &&
        declared.reasonName === StreamingShuffleFallbackReason.NetworkSaturation.toString,
      s"the token holder must still be able to stand the shuffle down, but got ${declared}")
    assert(coordinator.unregisterShuffle(shuffleId, token),
      "the token holder must still be able to unregister the shuffle")
    assert(!declared.toString.contains(token) && !grant.fallback.toString.contains(token),
      "no reply may echo the capability token, because a reply reaches log records and " +
        "task-failure reports")

    val dependency = shuffleDependencyFor(sc, sc.conf, numPartitions = 2, numRecords = 12)
    val handle = dependency.shuffleHandle.asInstanceOf[StreamingShuffleHandle[Int, Int, Int]]
    val coordinatorRef = RpcUtils.makeDriverRef(
      StreamingShuffleCoordinator.ENDPOINT_NAME, sc.conf, sc.env.rpcEnv)
    val refused = coordinatorRef.askSync[StreamingShuffleFallbackState](
      DeclareStreamingShuffleFallback(handle.shuffleId, sameLengthWrongToken,
        StreamingShuffleFallbackReason.MemoryPressure.toString,
        "an unauthorised remote declaration"))
    assert(!refused.fallenBack,
      s"a remote declaration presenting the wrong token must be refused, but was answered " +
        s"${refused}")
    val authorised = coordinatorRef.askSync[StreamingShuffleFallbackState](
      GetStreamingShuffleFallbackState(handle.shuffleId, handle.capabilityToken))
    assert(!authorised.fallenBack,
      s"the live shuffle must still be streaming after the refused declaration, but the " +
        s"coordinator reports ${authorised}")
  }

  test("an unauthorised fallback declaration withdraws no map output from the tracker") {
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-withdrawal", "local[2]"))
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val coordinator = new StreamingShuffleCoordinator(sc.env.rpcEnv, sc.conf, newManualClock())
    val streamingShuffleId = 41
    val sortOwnedShuffleId = 42
    val grant = coordinator.registerShuffle(streamingShuffleId, numPartitions = 2, numMaps = 1,
      ProtocolVersion).getOrElse(
      fail("the coordinator must accept a well-formed registration of the current protocol"))
    val token = grant.capabilityToken
    val location = BlockManagerId("exec-withdrawal", "withdrawal-host", 7077)

    def publishMapOutput(shuffleId: Int, attemptId: Long): Unit = {
      val mismatched = tracker.registerMapOutput(shuffleId, mapIndex = 0,
        MapStatus(location, Array(11L, 13L), attemptId))
      assert(!mismatched,
        s"publishing map output for shuffle $shuffleId cannot be read as a checksum change")
    }

    try {
      tracker.registerShuffle(streamingShuffleId, numMaps = 1, numReduces = 2)
      tracker.registerShuffle(sortOwnedShuffleId, numMaps = 1, numReduces = 2)
      publishMapOutput(streamingShuffleId, attemptId = 100L)
      publishMapOutput(sortOwnedShuffleId, attemptId = 200L)
      assert(tracker.getNumAvailableOutputs(streamingShuffleId) === 1 &&
          tracker.getNumAvailableOutputs(sortOwnedShuffleId) === 1,
        "both shuffles must start with their map output registered, or nothing below is a test " +
          "of a withdrawal")

      val sameLengthWrongToken = "x" * token.length
      Seq(sameLengthWrongToken, "", "too-short").foreach { presented =>
        assert(!coordinator.declareFallback(streamingShuffleId, presented,
            StreamingShuffleFallbackReason.NetworkSaturation, "an unauthorised declaration")
          .fallenBack,
          s"a declaration presenting '$presented' must be refused")
        assert(tracker.getNumAvailableOutputs(streamingShuffleId) === 1,
          s"a declaration presenting '$presented' must withdraw NOTHING: the map output of " +
            s"shuffle $streamingShuffleId was withdrawn before the token was compared, which " +
            s"forces a stage recomputation at an unauthorised caller's request")
        assert(!coordinator.declareFallback(sortOwnedShuffleId, token,
            StreamingShuffleFallbackReason.NetworkSaturation, "an unknown-shuffle declaration")
          .fallenBack,
          "a declaration naming a shuffle this coordinator never registered must be refused")
        assert(tracker.getNumAvailableOutputs(sortOwnedShuffleId) === 1,
          s"and it must withdraw nothing: the map output of unregistered shuffle " +
            s"$sortOwnedShuffleId is not this subsystem's to invalidate")
      }
      assert(coordinator.epochFor(streamingShuffleId).contains(grant.coordinatorEpoch),
        "no refused declaration may advance the epoch")

      assert(coordinator.declareFallback(streamingShuffleId, token,
          StreamingShuffleFallbackReason.NetworkSaturation, "an authorised declaration").fallenBack,
        "the token holder must be able to stand its own shuffle down")
      assert(tracker.getNumAvailableOutputs(streamingShuffleId) === 0,
        "an authorised stand-down must withdraw the streamed map output, or the delegated reduce " +
          "stage would read output no owner will serve")
      assert(tracker.getNumAvailableOutputs(sortOwnedShuffleId) === 1,
        "and it must withdraw nothing else")

      // The publication barrier.
      val barriersBefore = coordinator.withdrawalBarrierCount
      publishMapOutput(streamingShuffleId, attemptId = 101L)
      assert(tracker.getNumAvailableOutputs(streamingShuffleId) === 1,
        "the late publication must be registered, or the barrier has nothing to remove")
      coordinator.sealWithdrawalAfterLatch(streamingShuffleId,
        coordinator.epochFor(streamingShuffleId).getOrElse(
          fail("a stood-down shuffle must still report an epoch")))
      assert(tracker.getNumAvailableOutputs(streamingShuffleId) === 0,
        "the barrier must withdraw a map status published inside the withdrawal interval, or the " +
          "map stage reports itself available again and the stand-down forces no recomputation")
      assert(coordinator.withdrawalBarrierCount === barriersBefore + 1,
        s"and the barrier must record that it fired, but the count stayed at $barriersBefore")

      // Idempotent, and silent when there is nothing to do: a barrier that counted or withdrew on
      // an empty tracker would fire during every ordinary stand-down.
      coordinator.sealWithdrawalAfterLatch(streamingShuffleId, grant.coordinatorEpoch)
      assert(coordinator.withdrawalBarrierCount === barriersBefore + 1,
        "a barrier that finds no registered output must neither withdraw nor count")
    } finally {
      Seq(streamingShuffleId, sortOwnedShuffleId).foreach(tracker.unregisterShuffle)
    }
  }

  test("a channel that keeps sending frames the listener cannot handle is closed exactly once") {
    // The listener owns one port for every producer on the executor, so a peer decides how many
    // frames reach it.
    val listener = new StreamingShuffleListener(streamingConf(), newManualClock())
    try {
      val abusive = new EmbeddedChannel(DefaultChannelId.newInstance())
      val innocent = new EmbeddedChannel(DefaultChannelId.newInstance())
      val abusiveClient = new TransportClient(abusive, new TransportResponseHandler(abusive))
      val innocentClient = new TransportClient(innocent, new TransportResponseHandler(innocent))
      abusiveClient.setClientId("streaming-shuffle-manager-suite")
      innocentClient.setClientId("streaming-shuffle-manager-suite")
      val unhandleable = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))

      (1 until StreamingShuffleListener.MAX_MALFORMED_FRAMES_PER_CHANNEL).foreach { _ =>
        listener.receive(abusiveClient, unhandleable.duplicate())
      }
      assert(listener.malformedFrameCount ===
          StreamingShuffleListener.MAX_MALFORMED_FRAMES_PER_CHANNEL - 1,
        s"every unhandleable frame must be counted, but " +
          s"${listener.malformedFrameCount} were")
      assert(listener.abusiveChannelClosedCount === 0L,
        s"a channel one frame short of its allowance must still be served, but " +
          s"${listener.abusiveChannelClosedCount} had been closed")
      assert(abusive.isOpen,
        "the channel must still be open one frame short of the allowance")

      listener.receive(abusiveClient, unhandleable.duplicate())
      assert(listener.abusiveChannelClosedCount === 1L,
        s"the frame that exhausts the allowance must close the channel, but " +
          s"${listener.abusiveChannelClosedCount} channel(s) had been closed")
      assert(!abusive.isOpen,
        "the channel that exhausted its allowance must have been closed")

      listener.receive(abusiveClient, unhandleable.duplicate())
      listener.receive(abusiveClient, unhandleable.duplicate())
      assert(listener.abusiveChannelClosedCount === 1L,
        s"a channel past its allowance must be closed once however many further failures it " +
          s"produces, but ${listener.abusiveChannelClosedCount} closures were counted")

      listener.receive(innocentClient, unhandleable.duplicate())
      assert(innocent.isOpen,
        "a channel with one failure must not be closed because another channel exhausted its own " +
          "allowance")
      assert(listener.abusiveChannelClosedCount === 1L,
        s"only the offending channel may be closed, but " +
          s"${listener.abusiveChannelClosedCount} closures were counted")

      // A well-formed frame naming a producer this executor does not serve is a different thing
      // entirely: stale rather than hostile, so it is dropped and counted without charging the
      // channel, because a consumer legitimately holds an address until its own liveness timer says
      // otherwise.
      val unroutable = heartbeat(shuffleId = 3, mapId = 5L, partitionId = 1,
        consumerPosition = 0L).toByteBuffer()
      listener.receive(innocentClient, unroutable)
      assert(listener.unroutableFrameCount === 1L,
        s"a frame naming an unserved producer must be counted as unroutable, but " +
          s"${listener.unroutableFrameCount} were")
      assert(innocent.isOpen,
        "an unroutable frame must not close the channel that sent it")
      assert(listener.producerCount === 0,
        s"no producer may have been created by anything a peer sent, but the listener serves " +
          s"${listener.producerCount}")
    } finally {
      listener.releaseAll()
    }
  }

  test("the malformed-channel ledger fails closed when every tracking slot is occupied") {
    val limits = StreamingShuffleListener.DefaultLimits.copy(
      maxMalformedFramesPerChannel = 3,
      maxTrackedMalformedChannels = 2)
    val listener = new StreamingShuffleListener(streamingConf(), newManualClock(), limits)

    def authenticatedClient(name: String): (EmbeddedChannel, TransportClient) = {
      val channel = new EmbeddedChannel(DefaultChannelId.newInstance())
      val client = new TransportClient(channel, new TransportResponseHandler(channel))
      client.setClientId(name)
      (channel, client)
    }

    val first = authenticatedClient("malformed-peer-1")
    val second = authenticatedClient("malformed-peer-2")
    val overflow = authenticatedClient("malformed-peer-3")
    val malformed = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))
    try {
      listener.receive(first._2, malformed.duplicate())
      listener.receive(second._2, malformed.duplicate())
      assert(listener.trackedMalformedChannelCount === 2,
        "the two configured malformed-channel slots must both be occupied")

      listener.receive(overflow._2, malformed.duplicate())
      assert(!overflow._2.isActive,
        "a new malformed channel must fail closed when the tracking ledger is full")
      assert(listener.abusiveChannelClosedCount === 1L,
        "the untracked overflow channel must be closed on its first malformed frame")
      assert(listener.trackedMalformedChannelCount === 2,
        "failing closed must not grow the bounded malformed-channel ledger")

      listener.receive(first._2, malformed.duplicate())
      listener.receive(first._2, malformed.duplicate())
      assert(!first._2.isActive,
        "a channel already in the ledger must still close at its configured allowance")
      assert(listener.abusiveChannelClosedCount === 2L,
        "the overflow channel and the tracked channel must each be counted once")
      assert(second._2.isActive,
        "one channel exhausting its allowance must not close another tracked channel")
    } finally {
      Seq(first, second, overflow).foreach { case (channel, _) =>
        channel.finishAndReleaseAll()
      }
      listener.releaseAll()
    }
  }

  test("listener quotas bound participant channels and routes before state is created") {
    def authenticatedClient(name: String): (EmbeddedChannel, TransportClient) = {
      val channel = new EmbeddedChannel(DefaultChannelId.newInstance())
      val client = new TransportClient(channel, new TransportResponseHandler(channel))
      client.setClientId(name)
      (channel, client)
    }

    def sendHeartbeat(
        listener: StreamingShuffleListener,
        client: TransportClient,
        shuffleId: Int,
        mapId: Long): Unit = {
      listener.receive(client,
        new HeartbeatMessage(shuffleId, mapId, 0, 0L, shuffleId.toLong * 1000L + mapId)
          .toByteBuffer())
    }

    val channelLimits = StreamingShuffleListener.DefaultLimits.copy(
      maxParticipantChannels = 3,
      maxParticipantChannelsPerPeer = 2,
      maxParticipantRoutes = 6,
      maxParticipantRoutesPerPeer = 4,
      maxParticipantRoutesPerChannel = 2)
    val channelListener =
      new StreamingShuffleListener(streamingConf(), newManualClock(), channelLimits)
    val channelHandler = mock(classOf[StreamingShuffleServerHandler])
    channelListener.register(70, 1L, channelHandler)
    val peerA1 = authenticatedClient("participant-peer-a")
    val peerA2 = authenticatedClient("participant-peer-a")
    val peerA3 = authenticatedClient("participant-peer-a")
    val peerB1 = authenticatedClient("participant-peer-b")
    val peerB2 = authenticatedClient("participant-peer-b")
    try {
      Seq(peerA1, peerA2).foreach(entry => sendHeartbeat(channelListener, entry._2, 70, 1L))
      assert(channelListener.participantChannelCount === 2,
        "two channels from one peer must be admitted below its quota")

      sendHeartbeat(channelListener, peerA3._2, 70, 1L)
      assert(!peerA3._2.isActive,
        "a third channel from the same peer must be refused before channel state is created")
      assert(channelListener.participantChannelCount === 2,
        "a per-peer channel refusal must leave the channel registry unchanged")

      sendHeartbeat(channelListener, peerB1._2, 70, 1L)
      assert(channelListener.participantChannelCount === 3,
        "a different peer may use the executor's remaining channel slot")
      sendHeartbeat(channelListener, peerB2._2, 70, 1L)
      assert(!peerB2._2.isActive,
        "a channel beyond the executor-wide ceiling must be refused")
      assert(channelListener.participantChannelCount === 3 &&
          channelListener.participantRouteCount === 3,
        "refused channels must add neither channel nor participant-route state")
      assert(channelListener.remoteStateRefusalCount === 2L,
        "the per-peer and executor-wide channel refusals must both be counted")
    } finally {
      Seq(peerA1, peerA2, peerA3, peerB1, peerB2).foreach { case (channel, client) =>
        channelListener.channelInactive(client)
        channel.finishAndReleaseAll()
      }
      channelListener.releaseAll()
    }

    val routeLimits = StreamingShuffleListener.DefaultLimits.copy(
      maxParticipantChannels = 10,
      maxParticipantChannelsPerPeer = 10,
      maxParticipantRoutes = 4,
      maxParticipantRoutesPerPeer = 3,
      maxParticipantRoutesPerChannel = 2)
    val routeListener =
      new StreamingShuffleListener(streamingConf(), newManualClock(), routeLimits)
    val routeHandler = mock(classOf[StreamingShuffleServerHandler])
    (1L to 3L).foreach(mapId => routeListener.register(71, mapId, routeHandler))
    val channelBound = authenticatedClient("route-peer-a")
    val peerBound1 = authenticatedClient("route-peer-a")
    val peerBound2 = authenticatedClient("route-peer-a")
    val global1 = authenticatedClient("route-peer-b")
    val global2 = authenticatedClient("route-peer-b")
    val globalOverflow = authenticatedClient("route-peer-c")
    try {
      sendHeartbeat(routeListener, channelBound._2, 71, 1L)
      sendHeartbeat(routeListener, channelBound._2, 71, 2L)
      sendHeartbeat(routeListener, channelBound._2, 71, 3L)
      assert(!channelBound._2.isActive && routeListener.participantRouteCount === 2,
        "the per-channel route ceiling must refuse a third producer before allocating its entry")
      routeListener.channelInactive(channelBound._2)

      sendHeartbeat(routeListener, peerBound1._2, 71, 1L)
      sendHeartbeat(routeListener, peerBound1._2, 71, 2L)
      sendHeartbeat(routeListener, peerBound2._2, 71, 1L)
      sendHeartbeat(routeListener, peerBound2._2, 71, 2L)
      assert(!peerBound2._2.isActive && routeListener.participantRouteCount === 3,
        "the per-peer route ceiling must refuse state while executor capacity remains")
      routeListener.channelInactive(peerBound2._2)

      sendHeartbeat(routeListener, global1._2, 71, 1L)
      sendHeartbeat(routeListener, global2._2, 71, 1L)
      sendHeartbeat(routeListener, globalOverflow._2, 71, 1L)
      assert(!globalOverflow._2.isActive && routeListener.participantRouteCount === 4,
        "the executor route ceiling must refuse a new peer before allocating its first route")
      assert(routeListener.remoteStateRefusalCount === 3L,
        "channel, peer and executor route refusals must each be counted")
    } finally {
      Seq(channelBound, peerBound1, peerBound2, global1, global2, globalOverflow).foreach {
        case (channel, client) =>
          routeListener.channelInactive(client)
          channel.finishAndReleaseAll()
      }
      routeListener.releaseAll()
    }
  }

  test("the five streaming keys carry the documented defaults") {
    val untouched = new SparkConf(false)
    assert(!untouched.get(SHUFFLE_STREAMING_ENABLED),
      "the behaviour gate must default to false, which is what makes the feature opt-in")
    assert(untouched.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT) === DefaultBufferSizePercent,
      s"the buffer share must default to $DefaultBufferSizePercent percent of executor memory")
    assert(untouched.get(SHUFFLE_STREAMING_SPILL_THRESHOLD) === DefaultSpillThresholdPercent,
      s"the spill trigger must default to $DefaultSpillThresholdPercent percent utilisation")
    assert(!untouched.get(SHUFFLE_STREAMING_DEBUG),
      "verbose diagnostics must default to off, which is what keeps the log-volume budget met")
    assert(untouched.get(SHUFFLE_MANAGER) === "sort",
      "sort-based shuffle must remain the default manager")
  }

  test("an absent bandwidth cap means unlimited rather than zero") {
    val untouched = new SparkConf(false)
    assert(untouched.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS).isEmpty,
      "an unset bandwidth cap must read as None, meaning uncapped egress, not as Some(0)")
    val capped = new SparkConf(false).set(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, 64)
    assert(capped.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS) === Some(64),
      "a declared bandwidth cap must read back exactly as declared")
    val zeroed = new SparkConf(false).set(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, 0)
    val failure = intercept[SparkIllegalArgumentException] {
      zeroed.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)
    }
    checkError(
      exception = failure,
      condition = "INVALID_CONF_VALUE.REQUIREMENT",
      sqlState = "22022",
      parameters = Map(
        "confName" -> SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS.key,
        "confValue" -> "0",
        "confRequirement" -> "The maximum bandwidth should be positive."))
  }

  test("an uncapped fixture is uncapped even when the JVM carries an ambient cap") {
    // The fixture builders take `loadDefaults`, and a SparkConf built that way imports every
    // ambient `spark.*` system property.
    val key = SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS.key
    val ambient = Option(System.getProperty(key))
    try {
      System.setProperty(key, "7")
      assert(new SparkConf(true).get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS) === Some(7),
        "an ambient system property must reach a SparkConf that loads defaults, or this case " +
          "would be asserting nothing")
      val uncapped = streamingConfWithOverrides(maxBandwidthMBps = None, loadDefaults = true)
      assert(uncapped.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS).isEmpty,
        s"a fixture asked for an uncapped egress must be uncapped whatever the JVM carries, but " +
          s"it read back ${uncapped.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)}")
      assert(!uncapped.contains(key),
        "and the key must be absent rather than present with some other value, because it is " +
          "absence that the optional entry uses to express unlimited")
      val capped = streamingConfWithOverrides(maxBandwidthMBps = Some(64), loadDefaults = true)
      assert(capped.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS) === Some(64),
        s"a declared cap must override the ambient one, but read back " +
          s"${capped.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)}")
    } finally {
      ambient match {
        case Some(value) => System.setProperty(key, value)
        case None => System.clearProperty(key)
      }
    }
    assert(System.getProperty(key) === ambient.orNull,
      "the JVM property must be left exactly as it was found, because a test that changes global " +
        "state is a test that breaks whichever case runs next")
  }

  test("out of range buffer and spill values are rejected at configuration read time") {
    // Validation lives in the entry's converter, so a bad value is accepted by set and refused by
    // get.
    val rejections = Seq(
      (SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, 0, "The buffer size percent must be in [1, 50]."),
      (SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, 51, "The buffer size percent must be in [1, 50]."),
      (SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, -1, "The buffer size percent must be in [1, 50]."),
      (SHUFFLE_STREAMING_SPILL_THRESHOLD, 49, "The spill threshold must be in [50, 95]."),
      (SHUFFLE_STREAMING_SPILL_THRESHOLD, 96, "The spill threshold must be in [50, 95]."),
      (SHUFFLE_STREAMING_SPILL_THRESHOLD, 0, "The spill threshold must be in [50, 95]."))
    rejections.foreach { case (entry, value, requirement) =>
      val conf = new SparkConf(false).set(entry, value)
      val failure = intercept[SparkIllegalArgumentException] {
        conf.get(entry)
      }
      checkError(
        exception = failure,
        condition = "INVALID_CONF_VALUE.REQUIREMENT",
        sqlState = "22022",
        parameters = Map(
          "confName" -> entry.key,
          "confValue" -> value.toString,
          "confRequirement" -> requirement))
    }
  }

  test("the documented range boundaries are accepted") {
    Seq(MinBufferSizePercent, DefaultBufferSizePercent, MaxBufferSizePercent).foreach { percent =>
      val conf = new SparkConf(false).set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, percent)
      assert(conf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT) === percent,
        s"$percent must be an accepted buffer share, because the documented range is inclusive " +
          s"at both ends, from $MinBufferSizePercent to $MaxBufferSizePercent")
    }
    Seq(MinSpillThresholdPercent, DefaultSpillThresholdPercent, MaxSpillThresholdPercent)
      .foreach { percent =>
        val conf = new SparkConf(false).set(SHUFFLE_STREAMING_SPILL_THRESHOLD, percent)
        assert(conf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD) === percent,
          s"$percent must be an accepted spill threshold, because the documented range is " +
            s"inclusive from $MinSpillThresholdPercent to $MaxSpillThresholdPercent")
      }
  }

  test("configuration is read once at construction so a later change has no effect") {
    val conf = gatedOffStreamingConf()
    withManager(conf, isDriver = true) { manager =>
      assert(manager.shuffleBlockResolver.isInstanceOf[IndexShuffleBlockResolver],
        "the manager must start in the delegating posture the configuration described")
      conf.set(SHUFFLE_STREAMING_ENABLED, true)
      conf.set(SHUFFLE_STREAMING_DEBUG, true)
      assert(conf.get(SHUFFLE_STREAMING_ENABLED),
        "the fixture must actually have mutated the configuration for this case to mean anything")
      assert(manager.shuffleBlockResolver.isInstanceOf[IndexShuffleBlockResolver],
        "mutating the configuration after construction must not change the manager's behaviour; " +
          "every value it depends on was read once and is held immutably")
      assert(pushBasedMergeWouldEngage(manager),
        "the push-merge decision must likewise be unchanged by a post-construction edit")
    }
  }

  test("the buffer and spill percentages are read once so a live budget cannot move") {
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-immutable", "local[2]"))
    val partitions = 8
    val conf = streamingConfWithOverrides(
      bufferSizePercent = initialBufferSizePercent,
      spillThreshold = MinSpillThresholdPercent)
    val clock = newManualClock()
    val context = newTaskContext(SparkEnv.get, taskAttemptId = 91L, numPartitions = partitions)
    MemorySpillManager.resetSharedStateForTesting()
    val egressBudget = new TokenBucketRateLimiter.ExecutorEgressBudget(
      maxBandwidthMBps = None, clock = clock)
    val spillManager = new MemorySpillManager(
      context.taskMemoryManager, conf, clock, None, autoPoll = false)
    val protocol = new BackpressureProtocol(conf, null, egressBudget, clock)
    try {
      spillManager.registerPartitionCount(partitions)
      val budgetBytes = spillManager.totalBudgetBytes
      val perPartitionBytes = spillManager.perPartitionBudgetBytes
      val spillTriggerBytes = spillManager.spillThresholdBytes
      assert(budgetBytes > 0L, "the derivation must have produced a real allowance to hold on to")
      assert(perPartitionBytes === math.max(1L, budgetBytes / partitions),
        s"the per-partition allowance must be the contracted quotient of $budgetBytes bytes over " +
          s"$partitions partitions, but was $perPartitionBytes")
      assert(protocol.spillThresholdPercent === MinSpillThresholdPercent,
        s"flow control must have read the configured threshold, but read " +
          s"${protocol.spillThresholdPercent}")

      conf.set(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, mutatedBufferSizePercent)
      conf.set(SHUFFLE_STREAMING_SPILL_THRESHOLD, MaxSpillThresholdPercent)
      assert(conf.get(SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT) === mutatedBufferSizePercent &&
          conf.get(SHUFFLE_STREAMING_SPILL_THRESHOLD) === MaxSpillThresholdPercent,
        "the fixture must actually have mutated the configuration for this case to mean anything")

      assert(spillManager.totalBudgetBytes === budgetBytes,
        s"the aggregate allowance was derived once and must not move: it was $budgetBytes bytes " +
          s"and now reports ${spillManager.totalBudgetBytes}")
      assert(spillManager.perPartitionBudgetBytes === perPartitionBytes,
        s"nor may the per-partition allowance every buffer ceiling divides from it: it was " +
          s"$perPartitionBytes bytes and now reports ${spillManager.perPartitionBudgetBytes}")
      assert(spillManager.spillThresholdBytes === spillTriggerBytes,
        s"nor may the utilisation at which eviction triggers: it was $spillTriggerBytes and " +
          s"now reports ${spillManager.spillThresholdBytes}")
      assert(protocol.spillThresholdPercent === MinSpillThresholdPercent,
        s"nor may the threshold flow control compares utilisation against: it was " +
          s"$MinSpillThresholdPercent and now reports ${protocol.spillThresholdPercent}")

      MemorySpillManager.resetSharedStateForTesting()
      val restarted = new MemorySpillManager(
        context.taskMemoryManager, conf, clock, None, autoPoll = false)
      try {
        restarted.registerPartitionCount(partitions)
        val restartedBudget = restarted.totalBudgetBytes
        val executorBytes = MemorySpillManager.executorQuota(conf).executorMemoryBytes
        assert(restartedBudget === exactPercentageOf(executorBytes, mutatedBufferSizePercent),
          s"a component built after the change must observe it: $mutatedBufferSizePercent% of " +
            s"the $executorBytes byte executor memory is " +
            s"${exactPercentageOf(executorBytes, mutatedBufferSizePercent)}, but it reported " +
            s"$restartedBudget")
        assert(budgetBytes === exactPercentageOf(executorBytes, initialBufferSizePercent),
          s"and the allowance it replaced must have been $initialBufferSizePercent% of the same " +
            s"memory, which is " +
            s"${exactPercentageOf(executorBytes, initialBufferSizePercent)}, but was $budgetBytes")
        assert(restartedBudget > budgetBytes,
          "which is a larger allowance, because the mutation raised the percentage")
        assert(restarted.spillThresholdBytes ===
            math.max(1L, exactPercentageOf(restartedBudget, MaxSpillThresholdPercent)),
          s"and its eviction point must follow the new threshold, which is " +
            s"$MaxSpillThresholdPercent% of $restartedBudget bytes or " +
            s"${exactPercentageOf(restartedBudget, MaxSpillThresholdPercent)}, but was " +
            s"${restarted.spillThresholdBytes}")
        val restartedProtocol = new BackpressureProtocol(conf, null, egressBudget, clock)
        assert(restartedProtocol.spillThresholdPercent === MaxSpillThresholdPercent,
          s"flow control built after the change must read the new threshold, but read " +
            s"${restartedProtocol.spillThresholdPercent}")
      } finally {
        restarted.close()
      }
    } finally {
      try {
        spillManager.close()
      } finally {
        MemorySpillManager.resetSharedStateForTesting()
      }
    }
  }

  test("the streaming namespace publishes exactly the four documented metrics") {
    assert(StreamingShuffleMetricsSource.sourceName === "shuffle.streaming",
      "the source name supplies the shared namespace prefix exactly once, so the full metric " +
        "names come out as shuffle.streaming.<metric>")
    assert(MetricNames.size === 4,
      s"exactly four metrics are specified, but the fixture lists ${MetricNames.mkString(", ")}")
    assert(streamingShuffleMetricNames() === MetricNames.toSet,
      "the registry must publish exactly the four documented metrics and no fifth; found " +
        s"${streamingShuffleMetricNames().toSeq.sorted.mkString(", ")} against " +
        s"${MetricNames.sorted.mkString(", ")}")
    val qualified = MetricNames.map(name => s"${StreamingShuffleMetricsSource.sourceName}.$name")
    assert(qualified.toSet === Set(
      "shuffle.streaming.bufferUtilizationPercent",
      "shuffle.streaming.spillCount",
      "shuffle.streaming.backpressureEvents",
      "shuffle.streaming.partialReadInvalidations"),
      "the operator-facing names must be the four mandated ones, but got " +
        qualified.mkString(", "))
  }

  test("three metrics are counters and buffer utilisation is a gauge computed on read") {
    val registry = StreamingShuffleMetricsSource.metricRegistry
    val counterNames = registry.getCounters.keySet.asScala.toSet
    assert(counterNames === Set(SpillCountMetricName, BackpressureEventsMetricName,
      PartialReadInvalidationsMetricName),
      s"the three event series must be counters, but the registry holds ${
        counterNames.toSeq.sorted.mkString(", ")}")
    val gaugeNames = registry.getGauges.keySet.asScala.toSet
    assert(gaugeNames === Set(BufferUtilizationMetricName),
      "buffer utilisation must be the single gauge, computed when read so that the write path " +
        "does no metric work at all, but the registry holds " +
        gaugeNames.toSeq.sorted.mkString(", "))
    val contributor = installBufferUtilization(bufferedBytes = 40L, budgetBytes = 100L)
    try {
      assert(observedBufferUtilizationPercent() === 40L,
        "the gauge must aggregate the registered contributions when it is read")
      contributor.setBufferedBytes(90L)
      assert(observedBufferUtilizationPercent() === 90L,
        "re-pointing a contributor must change the value the gauge reports on its next read, " +
          "with no notification to the gauge; that is what 'computed on read' means")
    } finally {
      removeBufferUtilization(contributor)
    }
    assert(observedBufferUtilizationPercent() === 0L,
      "a departing contributor must subtract exactly its own contribution, leaving nothing behind")
  }

  test("the counters advance through their public increment seams and reset to a clean baseline") {
    // reset() is the only way a suite can get a clean baseline, because the source is a JVM
    // singleton.
    assert(observedSpillCount() === 0L, "beforeEach must have reset the spill counter")
    assert(observedBackpressureEvents() === 0L,
      "beforeEach must have reset the backpressure counter")
    assert(observedPartialReadInvalidations() === 0L,
      "beforeEach must have reset the invalidation counter")
    StreamingShuffleMetricsSource.incrementSpillCount(3L)
    StreamingShuffleMetricsSource.incrementBackpressureEvents(2L)
    StreamingShuffleMetricsSource.incrementPartialReadInvalidations(1L)
    assert(observedSpillCount() === 3L, "the spill counter must record what it was handed")
    assert(observedBackpressureEvents() === 2L,
      "the backpressure counter must record what it was handed")
    assert(observedPartialReadInvalidations() === 1L,
      "the invalidation counter must record what it was handed")
    StreamingShuffleMetricsSource.incrementSpillCount(-5L)
    StreamingShuffleMetricsSource.incrementSpillCount(0L)
    assert(observedSpillCount() === 3L,
      "a negative or zero increment must be ignored, keeping the exported series monotonic")
    resetStreamingShuffleMetrics()
    assert(observedSpillCount() === 0L, "reset must return the spill counter to zero")
    assert(observedBackpressureEvents() === 0L,
      "reset must return the backpressure counter to zero")
    assert(observedPartialReadInvalidations() === 0L,
      "reset must return the invalidation counter to zero")
  }

  test("the streaming source is registered through the static source list") {
    assert(StaticSources.allSources.contains(StreamingShuffleMetricsSource),
      "the streaming source must be an element of StaticSources.allSources, which is what makes " +
        "registration automatic everywhere the metrics system starts")
    assert(StaticSources.allSources.count(_ eq StreamingShuffleMetricsSource) === 1,
      "the source must appear exactly once, so it is not registered twice")
    assert(StaticSources.allSources.map(_.sourceName).distinct.size ===
      StaticSources.allSources.size,
      "every static source must occupy its own namespace, or one would mask another's metrics")
  }

  // Both masters, with a live context.

  test("in local mode one manager instance serves both roles and publishes the endpoint") {
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-local", "local[2]"))
    val env = SparkEnv.get
    assert(env.shuffleManager.isInstanceOf[StreamingShuffleManager],
      "spark.shuffle.manager=streaming must have produced a streaming manager in the live " +
        s"environment, but got ${env.shuffleManager.getClass.getName}")
    val reference = RpcUtils.makeDriverRef(
      StreamingShuffleCoordinator.ENDPOINT_NAME, sc.conf, env.rpcEnv)
    assert(reference != null,
      s"the coordinator must be resolvable under '${StreamingShuffleCoordinator.ENDPOINT_NAME}' " +
        "from the driver's own RpcEnv, because that is the name every executor looks up")
    assert(reference.name === StreamingShuffleCoordinator.ENDPOINT_NAME,
      s"the resolved reference must carry the published endpoint name, but was ${reference.name}")
  }

  test("in local mode the driver registers the streaming metrics source with no extra setup") {
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-metrics", "local[2]"))
    val sources = streamingShuffleSources(SparkEnv.get.metricsSystem)
    assert(sources.size === 1,
      "exactly one source must be published under the streaming namespace on the driver, reached " +
        s"the same way the metrics system itself reaches it, but found ${sources.size}")
    assert(sources.head eq StreamingShuffleMetricsSource,
      "the registered source must be the streaming shuffle source itself, so that the handles " +
        "the subsystem increments are the handles an operator observes")
    assert(sources.head.metricRegistry.getNames.asScala.toSet === MetricNames.toSet,
      "the registered source must expose exactly the four documented metrics")
  }

  test("with streaming enabled on the driver a streamable dependency gets a streaming handle") {
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-register", "local[2]"))
    assert(SparkEnv.get.shuffleManager.isInstanceOf[StreamingShuffleManager],
      "the live environment must hold a streaming manager for this case to mean anything")
    val partitionCount = 4
    val dependency = shuffleDependencyFor(sc, sc.conf, numPartitions = partitionCount,
      numRecords = 20)
    val handle = dependency.shuffleHandle
    assert(handle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
      "a streamable dependency registered on the driver with the gate open must produce a " +
        s"streaming handle, which is the commitment to stream. Got ${handle.getClass.getName}")
    val streamingHandle = handle.asInstanceOf[StreamingShuffleHandle[Int, Int, Int]]
    assert(streamingHandle.shuffleId === dependency.shuffleId,
      "the handle must carry the shuffle id it was registered for")
    assert(streamingHandle.numPartitions === partitionCount,
      "the handle must carry the dependency's partition count, but carried " +
        streamingHandle.numPartitions.toString)
    assert(streamingHandle.protocolVersion === ProtocolVersion,
      "the handle must stamp the current protocol version, which is what lets a peer detect a " +
        "version mismatch by a compatibility check rather than by a parse failure. Got " +
        streamingHandle.protocolVersion.toString)
    assert(streamingHandle.coordinatorEpoch >= 0L,
      s"the coordinator epoch must be non-negative, but was ${streamingHandle.coordinatorEpoch}")
    assert(streamingHandle.capabilityToken.nonEmpty,
      "the handle must carry a capability token, which authorises later coordinator calls")
    assert(!streamingHandle.toString.contains(streamingHandle.capabilityToken),
      "the handle's rendering must not echo the capability token: it reaches log records and " +
        "exception messages, and a credential printed there is a credential leaked")
  }

  test("the coordinator maintains executor concurrency and producer order incrementally") {
    val coordinator =
      new StreamingShuffleCoordinator(mock(classOf[RpcEnv]), streamingConf(), newManualClock())
    try {
      val firstShuffle = 6491
      val secondShuffle = 6492
      val partitions = 4
      val firstToken = coordinator.registerShuffle(
        firstShuffle, partitions, numMaps = 3, ProtocolVersion).get.capabilityToken
      val secondToken = coordinator.registerShuffle(
        secondShuffle, partitions, numMaps = 1, ProtocolVersion).get.capabilityToken

      def location(
          executorId: String,
          mapIndex: Int,
          taskAttemptId: Long): StreamingShuffleProducerLocation = {
        val mapId = 100L + mapIndex
        val host = s"host-$executorId"
        val port = 7300 + mapIndex
        StreamingShuffleProducerLocation(executorId, host, port, mapId, mapIndex, taskAttemptId,
          BlockManagerId(executorId, host, port))
      }

      val mapTwo = location("exec-a", mapIndex = 2, taskAttemptId = 12L)
      val mapZero = location("exec-a", mapIndex = 0, taskAttemptId = 10L)
      val mapOne = location("exec-b", mapIndex = 1, taskAttemptId = 11L)
      Seq(mapTwo, mapZero, mapOne).foreach { producer =>
        assert(coordinator.registerProducer(
          firstShuffle, firstToken, producer, partitions, ProtocolVersion).accepted)
      }
      val secondProducer = location("exec-a", mapIndex = 0, taskAttemptId = 20L)
      assert(coordinator.registerProducer(
        secondShuffle, secondToken, secondProducer, partitions, ProtocolVersion).accepted)

      assert(coordinator.producersFor(firstShuffle).map(_.mapIndex) === Seq(0, 1, 2),
        "the state must preserve map-index order as producers are inserted out of order")
      assert(coordinator.indexedShuffleCountFor("exec-a") === 2)
      assert(coordinator.numConcurrentShufflesFor("exec-a") === 2)
      assert(coordinator.indexedShuffleCountFor("exec-b") === 1,
        "several producers of one shuffle must count as one executor membership")
      assert(coordinator.expiryEntryCount === 4,
        "one liveness deadline must exist per incomplete producer, not per registration event")

      val replacedZero = location("exec-b", mapIndex = 0, taskAttemptId = 30L)
      assert(coordinator.registerProducer(
        firstShuffle, firstToken, replacedZero, partitions, ProtocolVersion).accepted)
      assert(coordinator.indexedShuffleCountFor("exec-a") === 2,
        "one remaining producer must retain the executor's membership in the first shuffle")
      assert(coordinator.expiryEntryCount === 4,
        "replacing a generation must replace its deadline rather than append another")

      val replacedTwo = location("exec-b", mapIndex = 2, taskAttemptId = 32L)
      assert(coordinator.registerProducer(
        firstShuffle, firstToken, replacedTwo, partitions, ProtocolVersion).accepted)
      assert(coordinator.indexedShuffleCountFor("exec-a") === 1,
        "moving the last producer must remove exactly one shuffle membership")
      assert(coordinator.indexedShuffleCountFor("exec-b") === 1,
        "three producers on one executor still represent one active shuffle")

      assert(coordinator.unregisterShuffle(secondShuffle, secondToken))
      assert(coordinator.indexedShuffleCountFor("exec-a") === 0)
      assert(coordinator.numConcurrentShufflesFor("exec-a") === 1,
        "the public divisor must retain its minimum of one after the index becomes empty")
      assert(coordinator.expiryEntryCount === 3,
        "unregistration must remove every deadline belonging to the dropped shuffle")
    } finally {
      coordinator.onStop()
    }
  }

  test("the coordinator expiry index replaces refreshes and reaps only elapsed targets") {
    val clock = newManualClock()
    val coordinator =
      new StreamingShuffleCoordinator(mock(classOf[RpcEnv]), streamingConf(), clock)
    try {
      val shuffleId = 6493
      val partitions = 2
      val token = coordinator.registerShuffle(
        shuffleId, partitions, numMaps = 2, ProtocolVersion).get.capabilityToken

      def location(
          executorId: String,
          mapIndex: Int,
          taskAttemptId: Long): StreamingShuffleProducerLocation = {
        val mapId = 200L + mapIndex
        val host = s"host-$executorId"
        val port = 7400 + mapIndex
        StreamingShuffleProducerLocation(executorId, host, port, mapId, mapIndex, taskAttemptId,
          BlockManagerId(executorId, host, port))
      }

      val incomplete = location("exec-live", mapIndex = 0, taskAttemptId = 40L)
      val completed = location("exec-complete", mapIndex = 1, taskAttemptId = 41L)
      Seq(incomplete, completed).foreach { producer =>
        assert(coordinator.registerProducer(
          shuffleId, token, producer, partitions, ProtocolVersion).accepted)
      }
      assert(coordinator.expiryEntryCount === 2)

      (1 to 100).foreach { _ =>
        assert(coordinator.heartbeatProducer(shuffleId, token, incomplete.generation).live)
      }
      assert(coordinator.expiryEntryCount === 2,
        "repeated refreshes must replace one indexed deadline rather than append stale entries")
      assert(coordinator.completeProducer(shuffleId, token, completed.generation))
      assert(coordinator.expiryEntryCount === 1,
        "a completed producer is exempt from silence-based reaping and needs no deadline")

      clock.advance(StreamingShuffleCoordinator.PRODUCER_LIVENESS_TIMEOUT_MS)
      assert(coordinator.reapStaleProducers() === 0,
        "the strict liveness bound must not expire a producer at exact equality")
      assert(coordinator.producersFor(shuffleId).map(_.mapIndex) === Seq(0, 1))
      clock.advance(1L)
      assert(coordinator.reapStaleProducers() === 1)
      assert(coordinator.producersFor(shuffleId).map(_.mapIndex) === Seq(1),
        "only the elapsed incomplete generation may be reaped")
      assert(coordinator.indexedShuffleCountFor("exec-live") === 0)
      assert(coordinator.indexedShuffleCountFor("exec-complete") === 1)
      assert(coordinator.expiryEntryCount === 0)

      val emptyShuffle = 6494
      val emptyToken = coordinator.registerShuffle(
        emptyShuffle, partitions, numMaps = 0, ProtocolVersion).get.capabilityToken
      assert(emptyToken.nonEmpty)
      assert(coordinator.expiryEntryCount === 1,
        "an empty registration must hold exactly one idle-eviction deadline")
      clock.advance(StreamingShuffleCoordinator.EMPTY_STATE_TTL_MS)
      assert(coordinator.reapStaleProducers() === 0)
      assert(coordinator.activeShuffleIds.contains(emptyShuffle),
        "an empty shuffle must remain registered at the exact idle deadline")
      clock.advance(1L)
      assert(coordinator.reapStaleProducers() === 0)
      assert(!coordinator.activeShuffleIds.contains(emptyShuffle),
        "the expiry index must evict an empty shuffle after its strict deadline")
      assert(coordinator.expiryEntryCount === 0)
    } finally {
      coordinator.onStop()
    }
  }

  test("no coordinator value renders the capability token, whatever asks it to render itself") {
    val clock = newManualClock()
    val conf = streamingConf()
    val coordinator = new StreamingShuffleCoordinator(mock(classOf[RpcEnv]), conf, clock)
    try {
      val shuffleId = 6501
      val grant = coordinator.registerShuffle(shuffleId, numPartitions = 4, numMaps = 2,
        ProtocolVersion)
      assert(grant.isDefined, "the shuffle must be registered for this case to have a token")
      val token = grant.get.capabilityToken
      assert(token.nonEmpty, "the grant must carry a non-empty token")
      val generation = StreamingShuffleProducerGeneration(mapIndex = 0, mapId = 11L,
        taskAttemptId = 21L)
      val location = StreamingShuffleProducerLocation("exec-1", "host-1", 7337, mapId = 11L,
        mapIndex = 0, taskAttemptId = 21L, BlockManagerId("exec-1", "host-1", 7337))
      assert(coordinator.registerProducer(shuffleId, token, location, numPartitions = 4,
          ProtocolVersion).accepted,
        "the producer must be registered so that the registry entry below holds one")
      val state = StreamingShuffleState(
        numPartitions = 4,
        mapStage = StreamingShuffleMapStage(numMaps = 2),
        protocolVersion = ProtocolVersion,
        coordinatorEpoch = grant.get.coordinatorEpoch,
        capabilityToken = token,
        producers = SortedMap(0 -> StreamingShuffleProducerEntry(location, 0L)))

      // Every value the coordinator defines that carries the token, named individually rather than
      // gathered by reflection, so that a value added later without a redacting `toString` fails
      // this case by being absent from the list a reviewer reads rather than by being invisible.
      val tokenBearing: Seq[(String, Any)] = Seq(
        "StreamingShuffleRegistrationGrant" -> grant.get,
        "RegisterStreamingShuffleProducer" ->
          RegisterStreamingShuffleProducer(shuffleId, token, location, 4, ProtocolVersion, 0L),
        "LookupStreamingShuffleProducers" ->
          LookupStreamingShuffleProducers(shuffleId, token, 0, 4),
        "HeartbeatStreamingShuffleProducer" ->
          HeartbeatStreamingShuffleProducer(shuffleId, token, generation, 0L),
        "InvalidateStreamingShuffleProducer" ->
          InvalidateStreamingShuffleProducer(shuffleId, token, generation,
            StreamingShuffleInvalidationReason.ConnectionTimeout, "detail"),
        "UnregisterStreamingShuffle" -> UnregisterStreamingShuffle(shuffleId, token),
        "CompleteStreamingShuffleProducer" ->
          CompleteStreamingShuffleProducer(shuffleId, token, generation),
        "DeclareStreamingShuffleFallback" ->
          DeclareStreamingShuffleFallback(shuffleId, token,
            StreamingShuffleFallbackReason.MemoryPressure.toString, "detail"),
        "GetStreamingShuffleFallbackState" ->
          GetStreamingShuffleFallbackState(shuffleId, token),
        "StreamingShuffleState" -> state)
      assert(tokenBearing.size === 10,
        s"every token-bearing coordinator value must be enumerated, but ${tokenBearing.size} are")

      tokenBearing.foreach { case (name, value) =>
        val rendered = value.toString
        assert(!rendered.contains(token),
          s"$name rendered the capability token: $rendered")
        assert(rendered.contains(s"capabilityToken=${StreamingShuffleCoordinator.REDACTED_TOKEN}"),
          s"$name must report the token's presence without its value, but rendered: $rendered")
        assert(rendered.startsWith(s"$name("),
          s"$name must still name itself, but rendered: $rendered")
        assert(rendered.contains(shuffleId.toString) ||
            rendered.contains(state.coordinatorEpoch.toString),
          s"$name must still render the identity it describes, but rendered: $rendered")
      }

      assert(!Some(grant.get).toString.contains(token),
        "a grant wrapped in an Option must not disclose the token")
      assert(!Seq(state).toString.contains(token),
        "a registry entry inside a collection must not disclose the token")
      assert(!Map(shuffleId -> state).toString.contains(token),
        "a registry entry as a map value must not disclose the token")

      assert(StreamingShuffleCoordinator.REDACTED_TOKEN === "<redacted>",
        "the substitute must be the fixed rendering the shuffle handle already uses, but is " +
          StreamingShuffleCoordinator.REDACTED_TOKEN)
      assert(!StreamingShuffleCoordinator.REDACTED_TOKEN.contains(token.take(4)),
        "the substitute must not carry any part of the token it replaces")
    } finally {
      coordinator.onStop()
    }
  }

  test("every coordinator operation refuses a caller that cannot present the shuffle's token") {
    val clock = newManualClock()
    val coordinator = new StreamingShuffleCoordinator(mock(classOf[RpcEnv]), streamingConf(), clock)
    try {
      val shuffleId = 6502
      val otherShuffleId = 6503
      val numPartitions = 4
      val grant = coordinator.registerShuffle(shuffleId, numPartitions, numMaps = 2,
        ProtocolVersion)
      val otherGrant = coordinator.registerShuffle(otherShuffleId, numPartitions, numMaps = 2,
        ProtocolVersion)
      assert(grant.isDefined && otherGrant.isDefined, "both shuffles must be registered")
      val token = grant.get.capabilityToken
      val foreignToken = otherGrant.get.capabilityToken
      assert(foreignToken !== token, "the two shuffles must not share a token")

      val location = StreamingShuffleProducerLocation("exec-1", "host-1", 7337, mapId = 11L,
        mapIndex = 0, taskAttemptId = 21L, BlockManagerId("exec-1", "host-1", 7337))
      val generation = location.generation
      assert(coordinator.registerProducer(shuffleId, token, location, numPartitions,
          ProtocolVersion).accepted,
        "the authorized registration must be accepted, or every refusal below is vacuous")
      assert(coordinator.lookupProducers(shuffleId, token, 0, numPartitions).isDefined,
        "the authorized lookup must answer, or every refusal below is vacuous")
      assert(coordinator.heartbeatProducer(shuffleId, token, generation).live,
        "the authorized heartbeat must refresh, or every refusal below is vacuous")

      val rejected: Seq[(String, String)] = Seq(
        "a wrong token" -> (token + "x"),
        "a token of the right length but different bytes" ->
          (token.dropRight(1) + (if (token.last == 'a') 'b' else 'a')),
        "an empty token" -> "",
        "a null token" -> null,
        "a token for another shuffle" -> foreignToken)

      rejected.foreach { case (description, presented) =>
        val deniedBefore = coordinator.deniedOperationCount

        val hijack = location.copy(executorId = "exec-attacker", host = "host-attacker",
          taskAttemptId = 99L)
        assert(!coordinator.registerProducer(shuffleId, presented, hijack, numPartitions,
            ProtocolVersion).accepted,
          s"$description must not be able to register a producer")
        assert(coordinator.lookupProducers(shuffleId, token, 0, numPartitions).get.locations
            .forall(_.executorId == "exec-1"),
          s"$description must not have replaced the registered producer address")

        assert(coordinator.lookupProducers(shuffleId, presented, 0, numPartitions).isEmpty,
          s"$description must not be able to look up live producer addresses")
        assert(coordinator.lookupProducers(unknownShuffleId, token, 0, numPartitions).isEmpty,
          "an unknown shuffle must answer the same way an unauthorized lookup does, so the two " +
            "are indistinguishable")

        assert(!coordinator.heartbeatProducer(shuffleId, presented, generation).live,
          s"$description must not be able to refresh a producer's liveness")

        assert(!coordinator.completeProducer(shuffleId, presented, generation),
          s"$description must not be able to report a producer complete")

        assert(coordinator.invalidateProducer(shuffleId, presented, generation,
            StreamingShuffleInvalidationReason.ConnectionTimeout, "unauthorized") ===
            StreamingShuffleCoordinator.NO_EPOCH,
          s"$description must not be able to invalidate a producer, and must not be told the epoch")
        assert(coordinator.lookupProducers(shuffleId, token, 0, numPartitions).get.locations
            .exists(_.generation == generation),
          s"$description must have left the registered generation in place")

        assert(!coordinator.declareFallback(shuffleId, presented,
            StreamingShuffleFallbackReason.MemoryPressure, "unauthorized").fallenBack,
          s"$description must not be able to stand a shuffle down")
        assert(!coordinator.fallbackStateFor(shuffleId, token).fallenBack,
          s"$description must not have latched a fallback readable by the authorized caller")

        assert(!coordinator.fallbackStateFor(shuffleId, presented).fallenBack,
          s"$description must be answered as though streaming were in force")

        assert(!coordinator.unregisterShuffle(shuffleId, presented),
          s"$description must not be able to unregister the shuffle")
        assert(coordinator.activeShuffleIds.contains(shuffleId),
          s"$description must have left the shuffle registered")

        assert(coordinator.deniedOperationCount > deniedBefore,
          s"$description must be counted as a denied operation so an operator can see it")
      }

      assert(coordinator.heartbeatProducer(shuffleId, token, generation).live,
        "the token holder must still be able to refresh its producer")
      assert(coordinator.completeProducer(shuffleId, token, generation),
        "the token holder must still be able to report completion")
      assert(coordinator.unregisterShuffle(shuffleId, token),
        "the token holder must still be able to unregister its shuffle")
      assert(!coordinator.activeShuffleIds.contains(shuffleId),
        "the authorized unregistration must have taken effect")
    } finally {
      coordinator.onStop()
    }
  }

  test("a map-side combining dependency is declined even with streaming fully enabled") {
    // The one structural exclusion.
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-combine", "local[2]"))
    val manager = SparkEnv.get.shuffleManager
    val shuffleId = 4243
    val handle = manager.registerShuffle(shuffleId, shuffleDep(numPartitions = 4,
      mapSideCombine = true))
    try {
      assert(!handle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
        "a dependency asking for map-side combining must be declined and handed to the sort " +
          s"delegate, but got ${handle.getClass.getName}")
    } finally {
      manager.unregisterShuffle(shuffleId)
    }
  }

  test("streaming requires authentication by default and refuses every unauthenticated peer") {
    val appId = "streaming-shuffle-auth"
    val secret = "streaming-shuffle-application-secret"

    val plainConf = streamingConf()
      .set("spark.app.id", appId)
      .set(NETWORK_AUTH_ENABLED, false)
    val plainSecurity = new SecurityManager(plainConf)
    val plainTransportConf = StreamingShuffleServerHandler.streamingTransportConf(
      plainConf, security = Some(plainSecurity))
    assert(!plainSecurity.isAuthenticationEnabled(),
      "authentication must be off for the plaintext half of this case")
    withManager(plainConf, isDriver = false) { manager =>
      assert(manager.shuffleBlockResolver.isInstanceOf[IndexShuffleBlockResolver],
        "authentication off must leave the selected streaming manager on the sort resolver")
      assert(manager.boundStreamingListener.isEmpty,
        "authentication off must never bind a streaming listener")
    }
    intercept[SparkException] {
      StreamingShuffleServerHandler.streamingClientBootstraps(
        plainConf, plainTransportConf, Some(plainSecurity))
    }
    intercept[SparkException] {
      StreamingShuffleServerHandler.streamingServerBootstraps(
        plainTransportConf, Some(plainSecurity))
    }

    val authConf = streamingConf()
      .set("spark.app.id", appId)
      .set(NETWORK_AUTH_ENABLED, true)
      .set(AUTH_SECRET, secret)
    val security = new SecurityManager(authConf)
    assert(security.isAuthenticationEnabled(),
      "authentication must be on for the authenticated half of this case")
    val transportConf = StreamingShuffleServerHandler.streamingTransportConf(
      authConf, security = Some(security))
    val clientBootstraps = StreamingShuffleServerHandler.streamingClientBootstraps(
      authConf, transportConf, Some(security))
    val serverBootstraps = StreamingShuffleServerHandler.streamingServerBootstraps(
      transportConf, Some(security))
    assert(clientBootstraps.size === 1,
      "a streaming consumer must install exactly one client bootstrap, not " +
        clientBootstraps.size)
    assert(clientBootstraps.get(0).isInstanceOf[AuthClientBootstrap],
      "the client bootstrap must be the platform's own auth bootstrap, not " +
        clientBootstraps.get(0).getClass.getName)
    assert(serverBootstraps.size === 1,
      "a streaming producer must install exactly one server bootstrap, not " +
        serverBootstraps.size)
    assert(serverBootstraps.get(0).isInstanceOf[AuthServerBootstrap],
      "the server bootstrap must be the platform's own auth bootstrap, not " +
        serverBootstraps.get(0).getClass.getName)
    assert(transportConf.getModuleName === StreamingShuffleServerHandler.TRANSPORT_MODULE_NAME,
      s"the authenticated channel must be tuned as the streaming module, not " +
        transportConf.getModuleName)
    assert(transportConf.enableTcpKeepAlive(),
      "the streaming module must enable operating system keep-alive")

    val listener = new StreamingShuffleListener(authConf)
    val transportContext = new TransportContext(transportConf, listener)
    var server: StreamingShuffleListener.BoundServer = null
    var authorizedFactory: TransportClientFactory = null
    var unauthenticatedFactory: TransportClientFactory = null
    try {
      server = new StreamingShuffleListener.BoundServer(
        transportContext, transportConf, listener, serverBootstraps)
      assert(server.getPort > 0, "the producer server must have bound a port")

      // Defense in depth at the listener itself.
      val directChannel = new EmbeddedChannel(DefaultChannelId.newInstance())
      val directClient = new TransportClient(
        directChannel, new TransportResponseHandler(directChannel))
      val directHeartbeat =
        new HeartbeatMessage(4711, 1L, 0, 0L, DirectConsumerToken)
      val unroutableBeforeDirect = listener.unroutableFrameCount
      listener.receive(directClient, directHeartbeat.toByteBuffer())
      assert(listener.unauthenticatedFrameCount === 1L,
        "the listener must count an unauthenticated frame refused before routing")
      assert(listener.unroutableFrameCount === unroutableBeforeDirect,
        "an unauthenticated frame must be refused before its producer identity is inspected")
      assert(listener.participantChannelCount === 0 && listener.participantRouteCount === 0,
        "an unauthenticated frame must allocate no channel or producer participation state")
      assert(!directClient.isActive,
        "the listener must close a direct unauthenticated channel on its first frame")
      directChannel.finishAndReleaseAll()

      authorizedFactory = transportContext.createClientFactory(clientBootstraps)
      val client = authorizedFactory.createClient(Utils.localHostName(), server.getPort)
      try {
        assert(client.isActive, "an authenticated consumer must hold a live channel")
        assert(client.getClientId === appId,
          s"the handshake must bind the application identity, but bound ${client.getClientId}")
        val unroutableBefore = listener.unroutableFrameCount
        val heartbeat =
          new HeartbeatMessage(4711, 1L, 0, 0L, DirectConsumerToken)
        client.send(heartbeat.toByteBuffer())
        eventually(timeout(10.seconds), interval(50.milliseconds)) {
          assert(listener.unroutableFrameCount > unroutableBefore,
            "a frame sent over the authenticated channel must reach the producer router")
        }
        assert(listener.malformedFrameCount === 0L,
          "the frame must have decoded cleanly on the far side of the handshake")
      } finally {
        client.close()
      }

      unauthenticatedFactory = transportContext.createClientFactory(
        java.util.Collections.emptyList[TransportClientBootstrap]())
      val unauthenticated = unauthenticatedFactory.createClient(
        Utils.localHostName(), server.getPort)
      try {
        assert(unauthenticated.getClientId == null,
          "the negative client must not have an identity before it sends its frame")
        val unroutableBefore = listener.unroutableFrameCount
        val refusal = intercept[Exception] {
          unauthenticated.sendRpcSync(directHeartbeat.toByteBuffer(), 5000L)
        }
        assert(refusal != null,
          "an unauthenticated connection must not receive a successful streaming RPC response")
        assert(listener.unroutableFrameCount === unroutableBefore,
          "the auth wrapper must refuse the frame before the streaming listener can route it")
      } finally {
        unauthenticated.close()
      }

    } finally {
      if (authorizedFactory != null) {
        authorizedFactory.close()
      }
      if (unauthenticatedFactory != null) {
        unauthenticatedFactory.close()
      }
      if (server != null) {
        server.close()
        assert(server.isTerminated,
          "closing the streaming listener must await both owned event-loop termination futures")
      }
      transportContext.close()
      listener.releaseAll()
    }
  }

  test("with the kill switch engaged the writer and reader are the sort delegate's own") {
    sc = new SparkContext(
      withLocalMaster(gatedOffStreamingConf(), "streaming-shuffle-manager-gated", "local[2]"))
    val manager = SparkEnv.get.shuffleManager
    assert(manager.isInstanceOf[StreamingShuffleManager],
      "the streaming manager class must still be the selected one with the gate closed")
    val dependency = shuffleDependencyFor(sc, sc.conf, numPartitions = 4, numRecords = 20)
    val handle = dependency.shuffleHandle
    assert(!handle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
      s"the gate is closed, so no streaming handle may exist. Got ${handle.getClass.getName}")
    val context = fakeTaskContext(sc)
    withSortManager(sc.conf) { sort =>
      val gatedWriter = manager.getWriter[Any, Any](handle, 0L, context,
        context.taskMetrics().shuffleWriteMetrics)
      val sortWriter = sort.getWriter[Any, Any](handle, 1L, context,
        context.taskMetrics().shuffleWriteMetrics)
      try {
        assert(gatedWriter.getClass === sortWriter.getClass,
          "getWriter must produce the same class a standalone SortShuffleManager produces, but " +
            s"produced ${gatedWriter.getClass.getName} against ${sortWriter.getClass.getName}")
      } finally {
        gatedWriter.stop(success = false)
        sortWriter.stop(success = false)
      }
      val gatedReader = manager.getReader[Any, Any](handle, 0, Int.MaxValue, 0, 1, context,
        context.taskMetrics().createTempShuffleReadMetrics())
      val sortReader = sort.getReader[Any, Any](handle, 0, Int.MaxValue, 0, 1, context,
        context.taskMetrics().createTempShuffleReadMetrics())
      assert(gatedReader.getClass === sortReader.getClass,
        "getReader must produce the same class a standalone SortShuffleManager produces, but " +
          s"produced ${gatedReader.getClass.getName} against ${sortReader.getClass.getName}")
    }
    assert(observedPartialReadInvalidations() === 0L,
      "no streaming telemetry may be produced while the gate is closed")
  }

  test("with the kill switch engaged a shuffle produces exactly the sort based output") {
    val baseline = sortBaselineGroupedOutput(numPartitions = 4)
    sc = new SparkContext(
      withLocalMaster(gatedOffStreamingConf(), "streaming-shuffle-manager-parity", "local[2]"))
    assert(SparkEnv.get.shuffleManager.isInstanceOf[StreamingShuffleManager],
      "the run must genuinely have gone through the streaming manager class for the comparison " +
        "to prove anything")
    val observed = groupedOutputAsSet(sc, numPartitions = 4)
    assertNoDataLoss(observed, baseline,
      "a shuffle run through the streaming manager with the kill switch engaged")
  }

  test("with streaming enabled a shuffle still produces exactly the sort based output") {
    val baseline = sortBaselineGroupedOutput(numPartitions = 4)
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-streamed", "local[2]"))
    assert(SparkEnv.get.shuffleManager.isInstanceOf[StreamingShuffleManager],
      "the run must have gone through the streaming manager for the comparison to prove anything")
    val observed = groupedOutputAsSet(sc, numPartitions = 4)
    assertNoDataLoss(observed, baseline, "a shuffle run with streaming fully enabled")
  }

  test("a streaming context leaves the JVM-global Hadoop credentials as it found them") {
    // The regression guard for a defect whose symptom appeared nowhere near its cause. Streaming
    // needs authenticated transport, so a fixture that starts a real context sets
    // `spark.authenticate`; `SparkEnv` then calls `SecurityManager.initializeAuth()`, which for a
    // `local*` or `yarn` master generates a fresh secret whether or not the configuration already
    // carries one and stores it in the Hadoop login user's credentials. That store is JVM-global,
    // and `SecurityManager.getSecretKey()` reads it ahead of every other source, so an unrestored
    // write
    // makes every later SecurityManager in the JVM answer with this suite's secret -- and the tests
    // that then fail belong to pre-existing suites about authentication, not to this package.
    //
    // What is asserted is the mechanism the suite-wide bracket is built on, in both directions and
    // in one place. Naming a victim suite instead would be a weaker test in every respect: it could
    // only observe the damage indirectly, only when the runner happened to order it after a
    // streaming suite, and it would report a failure in a suite that had done nothing wrong.
    //
    // Both halves matter. Without the first, an implementation that stopped writing the secret at
    // all would satisfy the second vacuously; without the second there is no restoration to speak
    // of. The bracket this package mixes into every suite is exactly these two steps applied around
    // a whole suite instead of around one context.
    val before = loginUserAuthSecret()
    try {
      sc = new SparkContext(
        withLocalMaster(streamingConf(), "streaming-shuffle-manager-ugi-guard", "local[2]"))
      assert(SparkEnv.get.shuffleManager.isInstanceOf[StreamingShuffleManager],
        "the guard is only meaningful if the context really took the streaming path")
      // Run a shuffle, so the context is exercised rather than merely constructed.
      groupedOutputAsSet(sc, numPartitions = 2)
      resetSparkContext()
      val during = loginUserAuthSecret()
      assert(during.isDefined,
        "a streaming context must have installed a secret in the Hadoop login user's " +
          "credentials, because that is what SecurityManager.initializeAuth does for a local " +
          "master and it is " +
          "the write the isolation exists to undo; with nothing written the restoration below " +
          "would be asserting nothing at all")
      assert(!sameLoginUserAuthSecret(before, during),
        s"and it must differ from what the JVM held beforehand, yet both read " +
          s"${describeLoginUserAuthSecret(before)}")
    } finally {
      restoreLoginUserAuthSecret(before)
    }
    val after = loginUserAuthSecret()
    assert(sameLoginUserAuthSecret(before, after),
      s"restoring must leave the Hadoop login user's Spark secret exactly as it was found, but " +
        s"the reading went from ${describeLoginUserAuthSecret(before)} to " +
        s"${describeLoginUserAuthSecret(after)}")
  }

  test("on a real cluster executors resolve the driver endpoint and stream correct output") {
    // local-cluster gives separate executor JVMs, so each builds its own StreamingShuffleManager
    // and resolves the coordinator by name rather than hosting it.
    val baseline = sortBaselineGroupedOutput(numPartitions = 4)
    sc = new SparkContext(withLocalMaster(streamingConf(),
      "streaming-shuffle-manager-cluster", "local-cluster[2,1,1024]"))
    assert(SparkEnv.get.shuffleManager.isInstanceOf[StreamingShuffleManager],
      "the driver must hold a streaming manager on a real cluster too")
    val observed = groupedOutputAsSet(sc, numPartitions = 4)
    assertNoDataLoss(observed, baseline, "a streaming shuffle across two real executor JVMs")
  }

  test("on a real cluster every executor registers the streaming metrics source") {
    sc = new SparkContext(withLocalMaster(streamingConf(),
      "streaming-shuffle-manager-cluster-metrics", "local-cluster[2,1,1024]"))
    val expectedNames = MetricNames.toSet
    val namespace = StreamingShuffleMetricsSource.sourceName
    val observed = sc.parallelize(1 to 8, 4).mapPartitions { _ =>
      val sources = TaskContext.get().getMetricsSources(namespace)
      Iterator.single((sources.size, sources.flatMap(_.metricRegistry.getNames.asScala).toSet))
    }.collect()
    assert(observed.nonEmpty, "the probe must have run on at least one executor")
    observed.foreach { case (sourceCount, metricNames) =>
      assert(sourceCount === 1,
        s"each executor must publish exactly one source under '$namespace', but one reported " +
          s"$sourceCount; registration is automatic on executors as well as on the driver")
      assert(metricNames === expectedNames,
        s"each executor must expose exactly the four documented metrics, but one reported " +
          s"${metricNames.toSeq.sorted.mkString(", ")}")
    }
  }
}

/** A probe that records what the seven-argument `getReader` was handed. */
private class RecordingReaderStreamingShuffleManager(conf: SparkConf)
  extends StreamingShuffleManager(conf, isDriver = true) {

  @volatile private var observed: Seq[(Int, Int, Int, Int)] = Seq.empty

  def recorded: Seq[(Int, Int, Int, Int)] = observed

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    synchronized {
      observed = observed :+ ((startMapIndex, endMapIndex, startPartition, endPartition))
    }
    new ShuffleReader[K, C] {
      override def read(): Iterator[Product2[K, C]] = Iterator.empty
    }
  }
}
