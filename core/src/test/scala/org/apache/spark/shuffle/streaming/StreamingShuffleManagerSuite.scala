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

import scala.jdk.CollectionConverters._

import _root_.io.netty.channel.DefaultChannelId
import _root_.io.netty.channel.embedded.EmbeddedChannel
import org.mockito.Mockito.mock
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.{Aggregator, HashPartitioner, LocalSparkContext, Partitioner, SecurityManager, ShuffleDependency, SparkConf, SparkContext, SparkEnv, SparkFunSuite, SparkIllegalArgumentException, TaskContext}
import org.apache.spark.internal.config.{AUTH_SECRET, NETWORK_AUTH_ENABLED, SHUFFLE_MANAGER, SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_DEBUG, SHUFFLE_STREAMING_ENABLED, SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, SHUFFLE_STREAMING_SPILL_THRESHOLD}
import org.apache.spark.metrics.source.StaticSources
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.{TransportClient, TransportClientFactory, TransportResponseHandler}
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.network.server.TransportServer
import org.apache.spark.network.shuffle.protocol.streaming.HeartbeatMessage
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, MigratableResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.{BlockManagerId, ShuffleMergedBlockId}
import org.apache.spark.util.{RpcUtils, Utils}

/**
 * Tests of [[StreamingShuffleManager]] as a `ShuffleManager` service provider.
 *
 * The suite is organised around the six properties that decide whether the subsystem is correct,
 * each one an independent way for the feature to be wrong:
 *
 *  1. <b>Selection.</b> `spark.shuffle.manager=streaming` resolves to this class, and the two
 *     pre-existing short names and the `sort` default still resolve to `SortShuffleManager`.
 *  2. <b>Service-provider conformance.</b> Every trait member is implemented with the mandated
 *     shape, the `final` five-argument `getReader` is inherited rather than overridden, and the
 *     lifecycle members tolerate being called twice.
 *  3. <b>Resolver identity.</b> Which resolver the manager publishes decides whether push-based
 *     merge engages and whether executor decommissioning works, so both branches are pinned.
 *  4. <b>Two-tier gating.</b> With the kill switch engaged the manager is a pass-through to an
 *     internally held, unmodified `SortShuffleManager` -- asserted per service-provider method and
 *     then end to end against a sort-based baseline.
 *  5. <b>Configuration.</b> The five keys carry the documented defaults, reject out-of-range values
 *     at read time, and are read once so a change needs a restart.
 *  6. <b>Observability.</b> Exactly four metrics are registered, automatically and on the driver
 *     and every executor alike, through the ordinary static-source mechanism and with no
 *     executor-lifecycle wiring of their own. Registration is not export: reaching an operator
 *     additionally requires a sink configured in `metrics.properties`, JMX included, and reading
 *     them over JMX means enabling the existing `JmxSink`, which ships commented out. What the
 *     subsystem guarantees is that it adds no sink, agent or UI surface of its own, so this suite
 *     asserts registration and leaves export to the sink.
 *
 * <b>Why some cases build a manager directly and others start a context.</b> The constructor
 * dereferences `SparkEnv` only when it is going to host the coordinator endpoint, which happens
 * when `isDriver` and the behaviour gate are both true; every other combination constructs against
 * a bare `SparkConf`. Cases needing no environment therefore build the manager directly, which
 * keeps them fast and free of cross-test coupling, while cases asserting on the rendezvous, on real
 * writers and readers, or on end-to-end output start a real context. Both masters are exercised:
 * `local[N]`, where one manager instance serves producer and consumer roles, and `local-cluster`,
 * where executors are separate JVMs that must resolve the driver's endpoint by name.
 *
 * Every manager built here is stopped in a `finally`, because one that is not stopped leaves an
 * index resolver and possibly a coordinator endpoint behind, and the test JVM runs with memory leak
 * detection enabled.
 */
class StreamingShuffleManagerSuite extends SparkFunSuite
  with LocalSparkContext
  with StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  private val streamingManagerClassName: String = classOf[StreamingShuffleManager].getName

  private val sortManagerClassName: String = classOf[SortShuffleManager].getName

  /**
   * Buffer share a live budget is derived from before the immutability case mutates it, and the
   * share it is mutated to. Both are inside the documented one-to-fifty range, and the second is a
   * multiple of the first so that the expected allowance after a restart is exact arithmetic rather
   * than an approximation: an allowance is the integer quotient of the unified memory region by a
   * hundred, multiplied by the share, so the shares' ratio carries through without rounding.
   */
  private val initialBufferSizePercent: Int = 10

  private val mutatedBufferSizePercent: Int = 40

  /**
   * Shuffle id the consumer-slowness condition is armed against.
   *
   * Any id serves: the sustained-slowness reading is kept per shuffle, while the trip it produces
   * is the executor's own verdict, so it is not confined to the shuffle that revealed it.
   */
  private val ThroughputProbeShuffleId = 9001

  /**
   * Map id used for the writer built before a condition is tripped, kept clear of the ids the
   * comparisons afterwards use so that no two writers of one case share an output file.
   */
  private val PreTripMapId = 90L

  /**
   * A shuffle id the coordinator has never been told about.
   *
   * Used by the authorization cases to establish that a refused operation is answered exactly as an
   * unknown shuffle is: were the two distinguishable, the refusal would itself disclose which
   * shuffle ids the driver holds.
   */
  private val unknownShuffleId: Int = 6599

  /**
   * The streaming metric handles are a JVM-wide singleton, so a clean baseline is taken before
   * every case rather than assumed. This is the mechanism the zero-flakiness gate rests on: without
   * it a counter assertion would depend on which cases had run earlier in the same JVM.
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

  /**
   * An answer that fails loudly when an un-stubbed member of a mock is touched.
   *
   * Borrowed from the sort path's own manager suite so that a case which accidentally depends on a
   * member it did not describe fails with the member's name rather than silently observing a zero
   * or a null.
   */
  private class RuntimeExceptionAnswer extends Answer[Object] {
    override def answer(invocation: InvocationOnMock): Object = {
      throw new RuntimeException("Called non-stubbed method, " + invocation.getMethod.getName)
    }
  }

  private def doReturn(value: Any): org.mockito.stubbing.Stubber =
    org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  /**
   * A shuffle dependency described exactly as far as the managers under test interrogate it.
   *
   * Mocked rather than derived from a live RDD so that the gating cases need no `SparkContext` at
   * all: what they assert is a branch taken on the dependency's shape, and a mock states that shape
   * without a scheduler behind it.
   *
   * @param numPartitions partitions the shuffle produces
   * @param mapSideCombine whether the dependency asks for map-side combining, which streaming
   *                       declines because a combining writer cannot pipeline
   * @return the dependency
   */
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

  // Tier one: selection through the ShuffleManager factory. The short-name table plus the
  // reflective factory beside it are the whole of the selection mechanism, so these cases go
  // through the factory rather than naming the class directly; doing it any other way would pass
  // even if the table entry had never been added.

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
    // The table lookup lower-cases the configured value with Locale.ROOT, so casing is irrelevant
    // and, more importantly, the result does not depend on the JVM's default locale. Locale.ROOT is
    // what makes that true; the same code with a locale-sensitive lower-casing would fail to
    // resolve "STREAMING" under a Turkish default locale, where uppercase I lower-cases outside
    // ASCII.
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
    // The lookup is a getOrElse, so a value that is not a known short name is returned verbatim and
    // treated as a fully qualified class name. Preserving case matters here: a custom class name is
    // case sensitive, and lower-casing the fall-through value would make every custom manager
    // unloadable.
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
    // wrote. The kill-switch configuration is used deliberately: it is the default posture, and it
    // needs no live SparkEnv, because no coordinator endpoint is hosted while the gate is closed.
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
    // with a SparkConf and a boolean. A third parameter, even a defaulted one, would remove the
    // two-argument constructor the factory looks for and make the manager impossible to select.
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
    // The five-argument overload is final in the trait and already forwards to the seven-argument
    // form with the whole map range, so overriding it is neither possible nor necessary. What the
    // bytecode holds is worth stating, because it is not what a first guess suggests: a concrete
    // `final def` in a Scala trait cannot become a final default method, since a JVM interface
    // method may not carry that flag, so scalac emits a `final` forwarder into every implementing
    // class instead. The five-argument method therefore IS present on this class, and what proves
    // it was inherited rather than hand-written is precisely that it is final, which a user
    // override could not be. The seven-argument method is non-final, being the abstract member this
    // manager implements.
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
    // Proven by observation rather than by reading the trait: a probe subclass records what the
    // seven-argument override was handed, and the five-argument overload is the only thing called.
    // This needs no SparkContext, which is the point -- the delegation contract is pure argument
    // forwarding and should be provable without a cluster.
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

  test("stop is idempotent and never throws on both tiers") {
    // Guarded by a compare-and-set, so the first caller releases and later callers return. A stop
    // that threw on the second call would break SparkEnv shutdown, which stops the manager without
    // knowing whether anything else already has.
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
    // Mandatory and abstract in the trait, and the trait's own note warns that a custom manager has
    // to co-exist with the External Shuffle Service through it, so a null here is not a lesser
    // failure than a missing method.
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

  // Resolver identity. Which resolver the manager publishes is not cosmetic: ShuffleWriteProcessor
  // decides whether push-based merge engages by pattern matching on its type, and BlockManager
  // reaches migration support by casting it. Both consequences are pinned here, on both tiers.

  private def readerShapeOf(methods: Array[java.lang.reflect.Method]): Seq[(Int, Boolean)] = {
    methods
      .map(method => (method.getParameterCount, Modifier.isFinal(method.getModifiers)))
      .toSeq
      .sorted
  }

  /**
   * Mirrors the decision `ShuffleWriteProcessor` makes on a manager's resolver.
   *
   * The shared write path matches the resolver against `IndexShuffleBlockResolver` and takes an
   * empty default branch for anything else, which is precisely why a streaming manager needs no
   * edit to that file. Reproducing the match here means a case asserts on the consequence an
   * operator actually gets rather than on a type name.
   *
   * @param manager the manager whose resolver decides the question
   * @return whether the push-merge branch would be taken for this manager
   */
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
    // Streamed output is not migratable: it is a live retransmission window bound to the executor
    // that produced it, addressed by no index and no data file, so no other executor could serve it
    // and no decommissioning executor could hand it over. Advertising the mixin would therefore
    // claim a capability the streaming path does not have. Declining it is safe because
    // BlockManager.migratableResolver is a lazy val that nothing forces until migration is actually
    // attempted, and every caller that does force it degrades rather than failing a job -- which
    // this case asserts below rather than merely asserting the negative type.
    withManager(streamingConf(), isDriver = false) { manager =>
      val resolver = manager.shuffleBlockResolver
      assert(!resolver.isInstanceOf[MigratableResolver],
        "the resolver published while streaming is enabled must not present itself as a " +
          "MigratableResolver: streamed output cannot be migrated, so claiming otherwise would " +
          s"offer the decommissioner a migration that cannot succeed. Got ${resolver.getClass}")
      assert(!resolver.isInstanceOf[IndexShuffleBlockResolver],
        "the same resolver must not be an IndexShuffleBlockResolver either, which is the " +
          s"stronger statement the negative above rests on. Got ${resolver.getClass.getName}")

      // The consequence an operator actually gets: the two members that would have been forwarded
      // are simply not declared, so the router's surface is exactly the four-member resolver
      // contract and nothing more.
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
    // The mirror image of the case above, and the reason declining the mixin costs nothing by
    // default: with the gate closed the sort delegate's own IndexShuffleBlockResolver is published
    // unchanged, and that class does mix MigratableResolver in, so decommission-based shuffle block
    // migration behaves exactly as it does under sort-based shuffle.
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
    // Push-based merge never runs while the streaming router is the manager's resolver, so a merged
    // block attributed to a streaming shuffle is a programming error and is reported as one rather
    // than disguised as an empty result. The router above it forwards merged requests to the sort
    // delegate instead, because such a request can only concern output written before streaming was
    // enabled.
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

  // Two-tier gating. Tier one selects the class; tier two gates its behaviour, and while it is
  // closed every service-provider call is forwarded to an internally held, unmodified
  // SortShuffleManager.

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
    // registerShuffle decides once, for the whole shuffle, whether it will be streamed, and that
    // decision has to be identical on every executor. An executor-side registration therefore
    // delegates rather than minting a handle of its own, which would be a second, competing
    // verdict.
    withManager(streamingConf(), isDriver = false) { manager =>
      val handle = manager.registerShuffle(0, shuffleDep(numPartitions = 4))
      assert(!handle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
        "an executor must not mint a streaming handle; only the driver's instance registers a " +
          s"shuffle. Got ${handle.getClass.getName}")
    }
  }

  // One terminus for every degradation path, which is the central safety property of the feature:
  // no configuration, no failure and no resource condition leaves a job without a working shuffle,
  // because every path ends at the same internally held, unmodified SortShuffleManager.

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
    // explicitly rather than slept for, so the case is deterministic. What is asserted here is the
    // POLICY's routing verdict -- that a tripped policy stands streaming down -- not the detection
    // thresholds, which belong to the fallback suite, and not the manager's service-provider
    // routing, which a standalone policy cannot speak for at all. That half is asserted against a
    // live manager by "every fallback condition makes a live manager delegate every service
    // provider call" below, and the two together are what the coexistence guarantee rests on.
    val trips: Seq[(StreamingShuffleFallbackReason, StreamingShuffleFallbackPolicy => Unit)] = Seq(
      (StreamingShuffleFallbackReason.MemoryPressure,
        policy => policy.recordAllocationGrant(requestedBytes = 1024L, grantedBytes = 512L)),
      (StreamingShuffleFallbackReason.NetworkSaturation,
        policy => policy.recordLinkUtilization(
          usedBytesPerSecond = 990.0d, capacityBytesPerSecond = 1000.0d)),
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
    // The verdict a standalone policy returns is not the claim that matters. What matters is that a
    // manager which has ALREADY committed a shuffle to streaming, and whose executor has already
    // built streaming machinery, answers every subsequent service-provider call exactly as the
    // sort-based manager would -- for shuffles registered after the trip AND for the streaming
    // handle it minted before it. Anything less would leave a job holding a handle nothing can
    // serve.
    //
    // Each condition is driven against the policy the LIVE manager holds, through the one shared
    // driver every suite uses, and with instants supplied rather than slept for -- which is what
    // makes a sixty-second sustained-slowness window reachable against a manager whose clock is the
    // system's.
    //
    // The baseline is captured first because Spark permits one context per JVM, and it is the
    // operational definition of a working shuffle used throughout these suites.
    val baseline = sortBaselineGroupedOutput(numPartitions = 4)
    assert(baseline.nonEmpty, "the sort-based baseline must produce output to compare against")
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-live-fallback", "local[2]"))
    val manager = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]
    val context = fakeTaskContext(sc)
    // Fixed at construction on the kill switch alone, and deliberately NOT on a trip: the resolver
    // is what the block manager serves local blocks from and what ShuffleWriteProcessor inspects,
    // and swapping it under a running executor would strand blocks a previous shuffle had already
    // published. The router it publishes serves the sort delegate's blocks as well as its own, so
    // one object covers both paths for the executor's whole life.
    val resolverBeforeAnyTrip = manager.shuffleBlockResolver
    assert(resolverBeforeAnyTrip.isInstanceOf[StreamingShuffleBlockRouter],
      s"a manager with streaming enabled must publish the routing resolver, but published " +
        s"${resolverBeforeAnyTrip.getClass.getName}")

    withSortManager(sc.conf) { sort =>
      StreamingShuffleFallbackReason.all.zipWithIndex.foreach { case (reason, index) =>
        // The latch is monotone, so each condition needs the policy back in service to be the one
        // that trips it. Returning it is what makes four conditions assertable in one context.
        manager.streamingFallbackPolicy.reset()
        assert(manager.streamingFallbackPolicy.streamingActive,
          s"streaming must be back in service before $reason is driven")

        // Streaming genuinely in service: this handle is a commitment the manager made, and it is
        // the handle whose service has to survive the trip.
        val committed = shuffleDependencyFor(sc, sc.conf, numPartitions = 4, numRecords = 20)
        val streamingHandle = committed.shuffleHandle
        assert(streamingHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
          s"the manager must have committed to streaming before $reason is driven, but it minted " +
            s"${streamingHandle.getClass.getName}")

        driveFallbackReason(manager.streamingFallbackPolicy, reason, committed.shuffleId)
        assert(manager.streamingFallbackPolicy.trippedReason === Some(reason),
          s"$reason must be the condition the live manager's own policy latched, but it latched " +
            s"${manager.streamingFallbackPolicy.trippedReason}")

        // 1. registerShuffle. A shuffle registered now must be literally the sort manager's own
        //    handle class, not merely "not streaming".
        val declined = shuffleDependencyFor(sc, sc.conf, numPartitions = 4, numRecords = 20)
        val declinedHandle = declined.shuffleHandle
        val sortHandle = sort.registerShuffle(declined.shuffleId, declined)
        assert(declinedHandle.getClass === sortHandle.getClass,
          s"after $reason, registerShuffle must return ${sortHandle.getClass.getName} but " +
            s"returned ${declinedHandle.getClass.getName}")
        assert(!declinedHandle.getClass.getName.contains(".streaming."),
          s"after $reason no streaming handle may be minted, yet a " +
            s"${declinedHandle.getClass.getName} was")

        // 2. getWriter, asked for the handle minted while streaming was still in service. Distinct
        //    map ids so the two writers never contend for one output file.
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

        // 3. getReader, the seven-argument form, for that same committed handle.
        val committedReader = manager.getReader[Any, Any](streamingHandle, 0, Int.MaxValue, 0, 1,
          context, context.taskMetrics().createTempShuffleReadMetrics())
        val sortReader = sort.getReader[Any, Any](streamingHandle, 0, Int.MaxValue, 0, 1, context,
          context.taskMetrics().createTempShuffleReadMetrics())
        assert(committedReader.getClass === sortReader.getClass,
          s"after $reason, getReader must produce ${sortReader.getClass.getName} even for the " +
            s"streaming handle, but produced ${committedReader.getClass.getName}")

        // 4. unregisterShuffle, which must answer exactly as the delegate answers.
        assert(manager.unregisterShuffle(declined.shuffleId) ===
            sort.unregisterShuffle(declined.shuffleId),
          s"after $reason, unregisterShuffle must answer as the sort delegate answers")

        // 5. The resolver is the same object it was before any condition tripped, for the reason
        //    stated above. Identity rather than class: the point is that it was not rebuilt.
        assert(manager.shuffleBlockResolver eq resolverBeforeAnyTrip,
          s"$reason must not swap the executor's block resolver, which serves blocks published " +
            "before the trip as well as after it")

        // 6. And the output. A real grouping shuffle registered after the trip runs end to end on
        //    the sort-based path, and produces the baseline's output -- which is the whole purpose
        //    of routing to that terminus rather than failing the job.
        val observed = groupedOutputAsSet(sc, numPartitions = 4)
        assertNoDataLoss(observed, baseline,
          s"a shuffle registered after the live manager stood streaming down for $reason")
      }
    }
  }

  test("a trip observed by a live manager is declared shuffle wide and stands the shuffle down") {
    // The baseline is captured before the context under test starts, because Spark permits one
    // context per JVM and the comparison at the end of this case is the definitive statement that
    // the work reached the sort-based delegate.
    val baseline = sortBaselineGroupedOutput(numPartitions = 4)
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-live-trip", "local[2]"))
    val manager = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]
    // The application's own coordinator, reached by the name it published. A coordinator built here
    // would hold a registry of its own and would answer about shuffles it had never seen.
    val coordinatorRef = RpcUtils.makeDriverRef(
      StreamingShuffleCoordinator.ENDPOINT_NAME, sc.conf, SparkEnv.get.rpcEnv)
    val context = fakeTaskContext(sc)

    // A shuffle the manager has committed to stream. Everything below is asserted against this
    // handle, so the delegation cannot be an artefact of a handle that was never streaming.
    val dependency = shuffleDependencyFor(sc, sc.conf, numPartitions = 4, numRecords = 20)
    val handle = dependency.shuffleHandle.asInstanceOf[StreamingShuffleHandle[Int, Int, Int]]
    assert(manager.degradationPolicy.streamingActive,
      "the live manager's own policy must start active, or the trip below proves nothing")

    // Before the trip the same handle really does produce the streaming halves. Without this the
    // comparisons below could hold because nothing was ever going to stream, which would make the
    // whole case vacuous.
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
    manager.degradationPolicy.recordLinkUtilization(
      usedBytesPerSecond = 990.0d, capacityBytesPerSecond = 1000.0d)
    assert(manager.degradationPolicy.hasTripped &&
        manager.degradationPolicy.trippedReason
          .contains(StreamingShuffleFallbackReason.NetworkSaturation),
      s"the live manager's policy must have latched network saturation, but held " +
        s"${manager.degradationPolicy.trippedReason}")

    withSortManager(sc.conf) { sort =>
      // Distinct map ids, so the two writers never contend for one output file.
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

    // A local observation is turned into a shuffle-wide decision before it is acted upon, and that
    // ordering is the whole difference between degrading correctly and degrading locally: a manager
    // that delegated on its own observation alone would have this executor's tasks served by the
    // sort delegate while every other executor kept streaming the same shuffle.
    val declared = coordinatorRef.askSync[StreamingShuffleFallbackState](
      GetStreamingShuffleFallbackState(handle.shuffleId, handle.capabilityToken))
    assert(declared.fallenBack,
      "the trip must have been declared to the coordinator, so that every other participant of " +
        "the shuffle stands down too")
    assert(declared.reasonName === StreamingShuffleFallbackReason.NetworkSaturation.toString,
      s"the declaration must carry the condition that was observed, but carried " +
        s"${declared.reasonName}")

    // registerShuffle takes the same terminus: a manager that has stood streaming down declines the
    // next dependency rather than minting a handle it cannot honour.
    val laterDependency = shuffleDependencyFor(sc, sc.conf, numPartitions = 4, numRecords = 20)
    assert(!laterDependency.shuffleHandle.isInstanceOf[StreamingShuffleHandle[_, _, _]],
      s"a dependency registered after the trip must be declined to the sort delegate, but got " +
        s"${laterDependency.shuffleHandle.getClass.getName}")
    assert(manager.unregisterShuffle(handle.shuffleId),
      "unregisterShuffle must report the delegate's own verdict, which is success")

    // And the definitive statement: the same workload, the same output set. This exercises the
    // delegate's writer, its reader and the resolver the manager publishes, end to end.
    val observed = groupedOutputAsSet(sc, numPartitions = 4)
    assertNoDataLoss(observed, baseline, "a shuffle run after the manager stood streaming down")
  }

  test("a fallback verdict reports the declarer's own account before the member's prose") {
    // The set of conditions is closed at four, so a declaration whose true condition is narrower
    // than any member has to pick the member it belongs under -- and the member's prose would then
    // be the only thing an operator ever read, stating a measurement nobody took. The declarer's
    // sentence therefore travels with the verdict and is what a rendering prefers, while the name
    // stays the stable machine identifier.
    val withDetail = StreamingShuffleFallbackState(
      StreamingShuffleFallbackReason.ProtocolVersionMismatch.toString,
      declaredAtEpoch = 7L,
      detail = "a consumer requested the narrowed map range [1, 3)")
    assert(withDetail.condition === "a consumer requested the narrowed map range [1, 3)",
      s"a verdict carrying a detail must report it, but reported ${withDetail.condition}")
    assert(withDetail.reason.contains(StreamingShuffleFallbackReason.ProtocolVersionMismatch),
      "and the name must still resolve onto this build's closed set")

    val withoutDetail = StreamingShuffleFallbackState(
      StreamingShuffleFallbackReason.ConsumerTooSlow.toString, declaredAtEpoch = 7L)
    assert(withoutDetail.condition === StreamingShuffleFallbackReason.ConsumerTooSlow.description,
      s"a verdict with no detail must fall back to this build's prose for the member, but " +
        s"reported ${withoutDetail.condition}")

    val unresolvable = StreamingShuffleFallbackState("SomethingThisBuildCannotResolve", 7L)
    assert(unresolvable.condition === "SomethingThisBuildCannotResolve",
      s"and a name this build cannot resolve must be reported verbatim rather than as prose it " +
        s"cannot produce, but reported ${unresolvable.condition}")

    val stillStreaming = StreamingShuffleFallbackState()
    assert(!stillStreaming.fallenBack,
      "a verdict for a shuffle that is still streaming must not read as fallen back")
    assert(stillStreaming.condition === StreamingShuffleCoordinator.NO_FALLBACK_REASON,
      s"and it must describe no condition at all, because there is none to describe, but " +
        s"described '${stillStreaming.condition}'")
  }

  test("a narrowed map range is declined as a compatibility failure and never as consumer " +
      "slowness") {
    // A live producer location names a map id and a task attempt id but no map index, so "map
    // indexes 1 to 3" cannot be served from producers that are still running and the read must be
    // declined. What is asserted here is the ATTRIBUTION of that decline. It used to be declared as
    // a consumer held at 2x behind the producer for more than a minute -- a measurement nothing on
    // this path takes -- which told an operator reading the driver log to go and tune consumer
    // throughput for a condition that was purely structural. The decline is a compatibility failure
    // between what a consumer asked for and what the protocol can serve, which is the same member a
    // partition-count disagreement is declared under, and the detail carries the specifics.
    val baseline = sortBaselineGroupedOutput(numPartitions = 4)
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-narrowed-range", "local[2]"))
    val manager = SparkEnv.get.shuffleManager.asInstanceOf[StreamingShuffleManager]
    val coordinatorRef = RpcUtils.makeDriverRef(
      StreamingShuffleCoordinator.ENDPOINT_NAME, sc.conf, SparkEnv.get.rpcEnv)
    val context = fakeTaskContext(sc)

    val dependency = shuffleDependencyFor(sc, sc.conf, numPartitions = 4, numRecords = 20)
    val handle = dependency.shuffleHandle.asInstanceOf[StreamingShuffleHandle[Int, Int, Int]]
    // The full range still streams, so the decline below is attributable to the narrowing alone.
    assert(manager.getReader[Int, Int](handle, 0, Int.MaxValue, 0, 1, context,
        context.taskMetrics().createTempShuffleReadMetrics())
        .isInstanceOf[StreamingShuffleReader[_, _]],
      "the whole map range must still be served by the streaming reader, or the narrowed case " +
        "below proves nothing")

    val narrowedReader = manager.getReader[Int, Int](handle, 1, 3, 0, 1, context,
      context.taskMetrics().createTempShuffleReadMetrics())
    withSortManager(sc.conf) { sort =>
      val sortReader = sort.getReader[Int, Int](handle, 1, 3, 0, 1, context,
        context.taskMetrics().createTempShuffleReadMetrics())
      assert(narrowedReader.getClass === sortReader.getClass,
        s"a narrowed map range must be served by the sort delegate's own reader, but was served " +
          s"by ${narrowedReader.getClass.getName} against ${sortReader.getClass.getName}")
    }

    val declared = coordinatorRef.askSync[StreamingShuffleFallbackState](
      GetStreamingShuffleFallbackState(handle.shuffleId, handle.capabilityToken))
    assert(declared.fallenBack,
      "the decline must stand the shuffle down for every participant, or the recomputation would " +
        "land on producers that cannot serve the narrowed range either")
    assert(declared.reasonName !== StreamingShuffleFallbackReason.ConsumerTooSlow.toString,
      "a structural decline must not be attributed to sustained consumer slowness, which is a " +
        "measurement this path never takes")
    assert(declared.reason.contains(StreamingShuffleFallbackReason.ProtocolVersionMismatch),
      s"it must be declared as the compatibility failure it is, but was declared as " +
        s"${declared.reasonName}")
    assert(declared.detail.contains("narrowed map range [1, 3)"),
      s"the declaration must carry the requested range, but carried '${declared.detail}'")
    assert(declared.condition === declared.detail,
      s"and an operator must read that account rather than the member's prose, but would read " +
        s"'${declared.condition}'")
    assert(!declared.condition.contains("slower") && !declared.condition.contains("60 seconds"),
      s"nothing an operator reads may claim a throughput condition, but the rendering was " +
        s"'${declared.condition}'")

    // The definitive statement: the workload still completes, on the sort-based path, with the
    // baseline's output.
    val observed = groupedOutputAsSet(sc, numPartitions = 4)
    assertNoDataLoss(observed, baseline, "a shuffle whose narrowed read stood streaming down")
  }

  test("each fallback condition routes a live manager's writer reader and unregister to sort") {
    // One live application, one executor-side manager per condition. A manager is needed per
    // condition because a trip latches: the first condition observed is the one the policy holds
    // for the life of that manager, which is itself the documented behaviour.
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
        policy => policy.recordLinkUtilization(
          usedBytesPerSecond = 990.0d, capacityBytesPerSecond = 1000.0d)),
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
      // A live streaming shuffle, registered by the application's own driver manager, so the handle
      // and its capability token are the production ones.
      val dependency = shuffleDependencyFor(sc, sc.conf, numPartitions = 2, numRecords = 12)
      val handle = dependency.shuffleHandle.asInstanceOf[StreamingShuffleHandle[Int, Int, Int]]
      // An executor-side manager: it hosts no endpoint and resolves the driver's by name, which is
      // exactly the posture every manager that serves a task is in.
      val manager = new StreamingShuffleManager(sc.conf, isDriver = false)
      try {
        assert(manager.degradationPolicy.streamingActive,
          s"the manager must start active before the $reason condition is applied")
        // Proof that this manager and this handle really would have streamed, so the delegation
        // asserted below is caused by the condition and not by something that never streamed.
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
        // Stopping a tripped manager must release the delegate as well, and must tolerate being
        // called twice, exactly as it does on an untripped one.
        manager.stop()
        manager.stop()
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // 5b. The trust boundary.
  //
  // A streaming frame's payload is handed to Spark's deserialization, and the block checksum is a
  // CRC32C, which detects corruption and forges trivially. Two things therefore stand between a
  // peer and that deserialization -- the platform's own authenticated transport, and the capability
  // token the driver mints per shuffle -- and a third bounds what a merely broken peer can cost.
  // Each is asserted here, on the production seams, because none of them is observable from a
  // shuffle that succeeds.
  // -----------------------------------------------------------------------------------------------

  test("streaming channels carry the platform's auth handshake exactly when it is enabled") {
    val authenticated = new SparkConf(false)
      .set(NETWORK_AUTH_ENABLED, true)
      .set(AUTH_SECRET, "streaming-shuffle-manager-suite-secret")
      .set("spark.app.id", "streaming-shuffle-auth")
    val plaintext = new SparkConf(false)
      .set(NETWORK_AUTH_ENABLED, false)
      .set("spark.app.id", "streaming-shuffle-plaintext")

    // The transport configuration is the streaming module's own, so what is asserted is the list a
    // streaming channel is really created with rather than the block transfer service's.
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
    assert(StreamingShuffleServerHandler.streamingClientBootstraps(
        plaintext, plaintextTransport, Some(plaintextSecurity)).isEmpty,
      "with authentication disabled the list must be empty, which reproduces the platform's " +
        "behaviour for every other connection in the same application rather than inventing one")
    assert(StreamingShuffleServerHandler.streamingServerBootstraps(
        plaintextTransport, Some(plaintextSecurity)).isEmpty,
      "with authentication disabled the producer server must accept channels exactly as every " +
        "other server in the application does")

    // No environment to read a secret from: the posture must be the plaintext one rather than a
    // handshake with no secret behind it.
    assert(StreamingShuffleServerHandler.streamingClientBootstraps(
        authenticated, authenticatedTransport, security = None).isEmpty &&
        StreamingShuffleServerHandler.streamingServerBootstraps(
          authenticatedTransport, security = None).isEmpty,
      "without a security manager there is no secret to complete a handshake with, so no " +
        "bootstrap may be installed")
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

    // The comparison itself. Length is compared before the constant-time comparison, and a null or
    // empty token never matches, so no absent credential can be mistaken for a present one.
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

    // Every gate, with three shapes of wrong token. Each answer must be the answer a caller would
    // get for a shuffle that simply has nothing to report, so the reply cannot be used to learn
    // whether the shuffle exists.
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

    // Nothing happened. The registration is intact, the epoch has not moved, and streaming is still
    // in force -- which is what makes the refusals refusals rather than partially applied changes.
    assert(coordinator.activeShuffleIds.contains(shuffleId),
      "a refused unregistration must leave the shuffle registered")
    assert(coordinator.epochFor(shuffleId).contains(grant.coordinatorEpoch),
      s"no refused operation may advance the epoch, but it moved from ${grant.coordinatorEpoch} " +
        s"to ${coordinator.epochFor(shuffleId)}")
    assert(!coordinator.fallbackStateFor(shuffleId, token).fallenBack,
      "read back with the real token, the shuffle must still be streaming")

    // And the legitimate holder's authority is undiminished by the refusals: denial is scoped to
    // the caller that presented the wrong token and to nothing else.
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

    // The same gate, over the wire, on the application's own coordinator: a shuffle the driver
    // really registered may not be stood down by a caller presenting the wrong token.
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

  test("a channel that keeps sending frames the listener cannot handle is closed exactly once") {
    // The listener owns one port for every producer on the executor, so a peer decides how many
    // frames reach it. Bounding the log alone would leave a decode attempt and an exception per
    // frame for as long as the peer cared to keep sending; closing the channel is what makes that
    // cost terminate. Driven directly, with no context: the listener needs a configuration and
    // nothing else.
    val listener = new StreamingShuffleListener(streamingConf(), newManualClock())
    try {
      // Each channel is given an identity of its own, because every plain EmbeddedChannel in a JVM
      // reports the same channel id text and the failure ledger is keyed by exactly that.
      val abusive = new EmbeddedChannel(DefaultChannelId.newInstance())
      val innocent = new EmbeddedChannel(DefaultChannelId.newInstance())
      val abusiveClient = new TransportClient(abusive, new TransportResponseHandler(abusive))
      val innocentClient = new TransportClient(innocent, new TransportResponseHandler(innocent))
      // Too short to carry the routing identity a router dispatches on, so handling fails outright
      // rather than being routed anywhere.
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

      // The frame that exhausts the allowance.
      listener.receive(abusiveClient, unhandleable.duplicate())
      assert(listener.abusiveChannelClosedCount === 1L,
        s"the frame that exhausts the allowance must close the channel, but " +
          s"${listener.abusiveChannelClosedCount} channel(s) had been closed")
      assert(!abusive.isOpen,
        "the channel that exhausted its allowance must have been closed")

      // A close is asynchronous and frames already in the pipeline still arrive, so the threshold
      // must answer once per channel rather than once per failure past it.
      listener.receive(abusiveClient, unhandleable.duplicate())
      listener.receive(abusiveClient, unhandleable.duplicate())
      assert(listener.abusiveChannelClosedCount === 1L,
        s"a channel past its allowance must be closed once however many further failures it " +
          s"produces, but ${listener.abusiveChannelClosedCount} closures were counted")

      // And the allowance is per channel: one abusive peer must cost a well-behaved one nothing.
      listener.receive(innocentClient, unhandleable.duplicate())
      assert(innocent.isOpen,
        "a channel with one failure must not be closed because another channel exhausted its own " +
          "allowance")
      assert(listener.abusiveChannelClosedCount === 1L,
        s"only the offending channel may be closed, but " +
          s"${listener.abusiveChannelClosedCount} closures were counted")

      // A well-formed frame naming a producer this executor does not serve is a different thing
      // entirely: stale rather than hostile, so it is dropped and counted without charging the
      // channel, because a consumer legitimately holds an address until its own liveness timer
      // says otherwise.
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

  // -----------------------------------------------------------------------------------------------
  // 6. Configuration.
  //
  // The five keys are consumed through their typed entries and never re-declared: each may be
  // declared exactly once in the JVM, so a suite that re-declared one would either shadow the real
  // entry or fail during class initialisation. Values are always set through the entry rather than
  // through a raw string key.
  // -----------------------------------------------------------------------------------------------
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
    // Declared with createOptional precisely so that absence expresses the unlimited state.
    // Encoding it as 0 or -1 would make "unlimited" indistinguishable from a misconfiguration, and
    // would put a magic sentinel into the token bucket's divisor arithmetic.
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
    // ambient `spark.*` system property. So "no cap was asked for" and "no cap is in force" are two
    // different statements unless the builder makes them one: a property another suite set, or one
    // an operator passed to the test JVM, would otherwise hand back a capped configuration while
    // the caller had asked for an uncapped one -- and a rate-limiting assertion would then be
    // measuring a cap it never chose, which is a false pass rather than a failure.
    val key = SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS.key
    val ambient = Option(System.getProperty(key))
    try {
      System.setProperty(key, "7")
      // The premise: with defaults loaded the property really does reach a configuration.
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
      // An explicit cap still wins over the ambient one, so the removal is scoped to the None case.
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
    // get. That is the documented behaviour and it is what makes the failure surface in the
    // component that reads the value rather than in whichever fixture happened to write it. The
    // error class is a pre-existing one, so no catalogue entry is added for configuration
    // validation.
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
    // This is what makes "a configuration change requires an executor restart" true by construction
    // rather than by convention, and it is why there is no dynamic reconfiguration path to test.
    // The manager is built with the gate closed and publishes the delegate's index resolver;
    // opening the gate on the same SparkConf afterwards must not change what it already decided.
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
    // The case above settles the manager's own posture. This one settles the two keys that actually
    // size memory, against the components that consume them, because "a configuration change
    // requires an executor restart" is a claim about THOSE: a budget that re-read its percentage
    // would let an operator shrink an allowance under a producer that had already reserved from it,
    // and a spill threshold that moved would change the point at which eviction triggers half way
    // through a shuffle. Neither is asserted by observing the manager, so neither was covered.
    //
    // A live context is needed rather than a bare SparkConf: the buffer allowance is derived from
    // the on-heap unified memory region through the environment's memory manager, and the spill
    // manager is a real memory consumer of the task memory manager. Both are the production
    // derivation path, not a stand-in for it -- the point is to prove that the production path
    // reads the configuration exactly once.
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-immutable", "local[2]"))
    val partitions = 8
    val conf = streamingConfWithOverrides(
      bufferSizePercent = initialBufferSizePercent,
      spillThreshold = MinSpillThresholdPercent)
    val clock = newManualClock()
    val context = newTaskContext(SparkEnv.get, taskAttemptId = 91L, numPartitions = partitions)
    // The executor-scoped allowance is process state, so it is discarded first and last: the
    // component under test must derive it from THIS configuration, and whichever case runs next
    // must not inherit it.
    MemorySpillManager.resetSharedStateForTesting()
    val egressBudget = new TokenBucketRateLimiter.ExecutorEgressBudget(
      maxBandwidthMBps = None, clock = clock)
    // quotaOverride = None deliberately: the allowance must come from the production derivation,
    // which is the only thing that can read the configuration. autoPoll = false because a
    // background ticker would add nothing to a budget assertion.
    val spillManager = new MemorySpillManager(
      context.taskMemoryManager, conf, clock, None, autoPoll = false)
    val protocol = new BackpressureProtocol(conf, null, egressBudget, clock)
    try {
      spillManager.registerPartitionCount(partitions)
      // Forcing the lazy derivation is the whole premise: a budget that has never been read has
      // nothing to keep unchanged, so a case that skipped this would pass against a component that
      // re-read the configuration on every access.
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

      // The mutation. Both keys are moved to the far end of their documented ranges, so an
      // implementation that re-read either of them could not possibly report the same figures.
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

      // And the other half of "requires a restart": a component built afterwards -- which is what a
      // restarted executor amounts to -- does observe the new values. Without this the assertions
      // above would also pass against a component that ignored the configuration altogether.
      MemorySpillManager.resetSharedStateForTesting()
      val restarted = new MemorySpillManager(
        context.taskMemoryManager, conf, clock, None, autoPoll = false)
      try {
        restarted.registerPartitionCount(partitions)
        val restartedBudget = restarted.totalBudgetBytes
        // Exact, and derived from the unified-memory reading rather than by rescaling the first
        // allowance. Rescaling would be wrong arithmetic dressed as an equality: the allowance
        // takes the product before the division, so for a heap that is not a clean multiple of a
        // hundred `budget(10) / 10 * 40` falls short of `budget(40)` by the remainder the first
        // division threw away -- which is exactly the truncation the derivation exists to avoid.
        // `exactPercentageOf` computes the same quantity independently, in `BigInt`, from the very
        // quota object the restarted component memoised.
        val unifiedBytes = MemorySpillManager.executorQuota(conf).unifiedMemoryBytes
        assert(restartedBudget === exactPercentageOf(unifiedBytes, mutatedBufferSizePercent),
          s"a component built after the change must observe it: $mutatedBufferSizePercent% of " +
            s"the $unifiedBytes byte unified region is " +
            s"${exactPercentageOf(unifiedBytes, mutatedBufferSizePercent)}, but it reported " +
            s"$restartedBudget")
        assert(budgetBytes === exactPercentageOf(unifiedBytes, initialBufferSizePercent),
          s"and the allowance it replaced must have been $initialBufferSizePercent% of the same " +
            s"region, which is ${exactPercentageOf(unifiedBytes, initialBufferSizePercent)}, but " +
            s"was $budgetBytes")
        assert(restartedBudget > budgetBytes,
          "which is a larger allowance, because the mutation raised the percentage")
        // The same product-before-division arithmetic, for the same reason: an eviction point
        // computed as `budget / 100 * threshold` would trigger a few bytes early on every allowance
        // that is not a clean multiple of a hundred.
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

  // -----------------------------------------------------------------------------------------------
  // 7. Observability.
  //
  // Four metrics, registered by the ordinary static-source mechanism so that no new agent, sink or
  // executor-lifecycle change is needed. Registration is asserted separately from export, because
  // appending a source configures no sink: every sink, JMX included, stays opt-in through
  // metrics.properties.
  // -----------------------------------------------------------------------------------------------

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
    // Computed on read: re-pointing a contributor changes the reported value without the gauge
    // being told, which is the property that keeps telemetry off every hot path.
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
    // singleton. The zero-flakiness gate depends on it, which is why it is asserted rather than
    // merely used.
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
    // Monotonic by construction: an operator-facing event series must never run backwards, so a
    // non-positive increment is ignored rather than applied.
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
    // Appending to allSources is the entire registration mechanism: the metrics system walks that
    // list when it starts, on the driver and on every executor alike. Nothing in executor lifecycle
    // code is touched, and no import was added to the list's own file.
    assert(StaticSources.allSources.contains(StreamingShuffleMetricsSource),
      "the streaming source must be an element of StaticSources.allSources, which is what makes " +
        "registration automatic everywhere the metrics system starts")
    assert(StaticSources.allSources.count(_ eq StreamingShuffleMetricsSource) === 1,
      "the source must appear exactly once, so it is not registered twice")
    assert(StaticSources.allSources.map(_.sourceName).distinct.size ===
      StaticSources.allSources.size,
      "every static source must occupy its own namespace, or one would mask another's metrics")
  }

  // Both masters, with a live context. local[N] and local-cluster exercise genuinely different
  // code: in local mode the executor deliberately skips shuffle manager initialisation because the
  // driver's instance already exists, so one manager serves producer and consumer roles and the
  // coordinator can only ever take the setupEndpoint path, whereas local-cluster gives real
  // executor JVMs, each of which builds its own manager and must resolve the driver's endpoint by
  // name. SharedSparkContext cannot serve the second case because it hardcodes its master.

  test("in local mode one manager instance serves both roles and publishes the endpoint") {
    sc = new SparkContext(
      withLocalMaster(streamingConf(), "streaming-shuffle-manager-local", "local[2]"))
    val env = SparkEnv.get
    assert(env.shuffleManager.isInstanceOf[StreamingShuffleManager],
      "spark.shuffle.manager=streaming must have produced a streaming manager in the live " +
        s"environment, but got ${env.shuffleManager.getClass.getName}")
    // Register-then-lookup inside one instance: the driver hosts the endpoint, and the name it
    // published must be resolvable by the very same JVM. A name that is registered only on first
    // use is a race an executor loses, and in local mode that race is invisible, which is exactly
    // why it is asserted here rather than assumed.
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
    // Taken from a real dependency rather than by invoking registerShuffle by hand, so what is
    // asserted is the handle the production path actually minted: building a ShuffledRDD registers
    // its shuffle with the live manager as a side effect of creating the dependency.
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

  test("no coordinator value renders the capability token, whatever asks it to render itself") {
    // The token is the whole of the coordinator's authorization: a peer holding it may replace a
    // producer address, refresh a liveness window, invalidate a generation, force a shuffle onto
    // the sort-based path, or drop a registration outright. `case class` gives every one of these
    // values a generated `toString` that renders every field, and they reach renderings nobody
    // writes by hand -- Spark's own RPC layer logs a message it cannot deliver or does not
    // recognise in full, an `Option` or a collection of them lands in a `require` diagnostic, and a
    // registry entry is interpolated into a log record about the shuffle it describes. Asserting
    // the property on the shuffle handle alone leaves nine other renderings of the same secret, so
    // every token-bearing value the coordinator defines is enumerated here.
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
      // The registry entry is built directly rather than read back, because the coordinator
      // deliberately publishes no accessor for it -- handing out the value would hand out the token
      // with it. What is asserted is the rendering of the production type holding a real token, and
      // constructing it here is the only way to hold one without widening the endpoint's surface.
      val state = StreamingShuffleState(
        numPartitions = 4,
        mapStage = StreamingShuffleMapStage(numMaps = 2),
        protocolVersion = ProtocolVersion,
        coordinatorEpoch = grant.get.coordinatorEpoch,
        capabilityToken = token,
        producers = Map(0 -> StreamingShuffleProducerEntry(location, 0L)))

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
        // The identifying fields must survive, or the redaction would have bought its safety by
        // making the rendering useless for the diagnostics it exists to serve.
        assert(rendered.startsWith(s"$name("),
          s"$name must still name itself, but rendered: $rendered")
        assert(rendered.contains(shuffleId.toString) ||
            rendered.contains(state.coordinatorEpoch.toString),
          s"$name must still render the identity it describes, but rendered: $rendered")
      }

      // Nesting is where a redaction is most easily lost: these values travel as `Option`s and
      // inside collections, and the enclosing rendering delegates to the element's own `toString`.
      assert(!Some(grant.get).toString.contains(token),
        "a grant wrapped in an Option must not disclose the token")
      assert(!Seq(state).toString.contains(token),
        "a registry entry inside a collection must not disclose the token")
      assert(!Map(shuffleId -> state).toString.contains(token),
        "a registry entry as a map value must not disclose the token")

      // And the redaction is a fixed string rather than a derivation of the secret: a stable hash
      // or a prefix would still distinguish one token from another to whoever reads the log.
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
    // The endpoint is reachable by every peer of the application and a shuffle id is a small
    // guessable integer, so the token is the only thing standing between a stranger and the
    // registry. A suite that exercises only the authorized path leaves that guard untested in the
    // one direction that matters, so every operation is driven here with a valid token first --
    // proving the operation works at all, which is what makes the refusals meaningful -- and then
    // with each way a token can be wrong.
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
      // A token minted for a different shuffle: the closest thing to a valid credential a peer that
      // legitimately participates in the application can hold, and the case a per-shuffle secret
      // exists to refuse.
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

      // Every way a presented token can fail, including the two that a constant-time comparison has
      // to answer without dereferencing: null and empty.
      val rejected: Seq[(String, String)] = Seq(
        "a wrong token" -> (token + "x"),
        "a token of the right length but different bytes" ->
          (token.dropRight(1) + (if (token.last == 'a') 'b' else 'a')),
        "an empty token" -> "",
        "a null token" -> null,
        "a token for another shuffle" -> foreignToken)

      rejected.foreach { case (description, presented) =>
        val deniedBefore = coordinator.deniedOperationCount

        // A mutation that would replace a live producer address.
        val hijack = location.copy(executorId = "exec-attacker", host = "host-attacker",
          taskAttemptId = 99L)
        assert(!coordinator.registerProducer(shuffleId, presented, hijack, numPartitions,
            ProtocolVersion).accepted,
          s"$description must not be able to register a producer")
        assert(coordinator.lookupProducers(shuffleId, token, 0, numPartitions).get.locations
            .forall(_.executorId == "exec-1"),
          s"$description must not have replaced the registered producer address")

        // A disclosure. Answered exactly as an unknown shuffle is, so a refusal cannot be used to
        // discover which shuffle ids exist.
        assert(coordinator.lookupProducers(shuffleId, presented, 0, numPartitions).isEmpty,
          s"$description must not be able to look up live producer addresses")
        assert(coordinator.lookupProducers(unknownShuffleId, token, 0, numPartitions).isEmpty,
          "an unknown shuffle must answer the same way an unauthorized lookup does, so the two " +
            "are indistinguishable")

        // A liveness refresh, which would keep a reaped generation looking alive.
        assert(!coordinator.heartbeatProducer(shuffleId, presented, generation).live,
          s"$description must not be able to refresh a producer's liveness")

        // A completion report, which tells consumers a map output is finished.
        assert(!coordinator.completeProducer(shuffleId, presented, generation),
          s"$description must not be able to report a producer complete")

        // The invalidation, which is the operation that forces an upstream stage to be recomputed.
        assert(coordinator.invalidateProducer(shuffleId, presented, generation,
            StreamingShuffleInvalidationReason.ConnectionTimeout, "unauthorized") ===
            StreamingShuffleCoordinator.NO_EPOCH,
          s"$description must not be able to invalidate a producer, and must not be told the epoch")
        assert(coordinator.lookupProducers(shuffleId, token, 0, numPartitions).get.locations
            .exists(_.generation == generation),
          s"$description must have left the registered generation in place")

        // The shuffle-wide stand-down, which would deny streaming to every participant.
        assert(!coordinator.declareFallback(shuffleId, presented,
            StreamingShuffleFallbackReason.MemoryPressure, "unauthorized").fallenBack,
          s"$description must not be able to stand a shuffle down")
        assert(!coordinator.fallbackStateFor(shuffleId, token).fallenBack,
          s"$description must not have latched a fallback readable by the authorized caller")

        // The state query, answered with the streaming-in-force value so that it discloses nothing.
        assert(!coordinator.fallbackStateFor(shuffleId, presented).fallenBack,
          s"$description must be answered as though streaming were in force")

        // And the removal, which would strand every consumer of the shuffle.
        assert(!coordinator.unregisterShuffle(shuffleId, presented),
          s"$description must not be able to unregister the shuffle")
        assert(coordinator.activeShuffleIds.contains(shuffleId),
          s"$description must have left the shuffle registered")

        assert(coordinator.deniedOperationCount > deniedBefore,
          s"$description must be counted as a denied operation so an operator can see it")
      }

      // The authorized caller is unaffected by every refusal above: the guard rejects strangers
      // rather than degrading the holder.
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
    // The one structural exclusion. A combining writer must accumulate every record of a partition
    // before it can emit a combined value, which is precisely the materialisation barrier streaming
    // exists to remove, so there is nothing to pipeline. Asserted on the driver with the gate open,
    // where map-side combining is the only reason the dependency can be declined.
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

  test("streaming channels authenticate when the application does and refuse a peer without it") {
    // A streaming shuffle channel carries serialized records straight into Spark's deserialization,
    // so an unauthenticated one is a remote code execution surface: the block checksum is a CRC32C,
    // which detects corruption and forges trivially, and is therefore no part of the answer. The
    // platform's own answer is the auth handshake, and the whole of this subsystem's contribution
    // to it is that the bootstraps are installed on both ends. Asserting that the bootstrap lists
    // are non-empty would establish only that two collections have members; what has to be
    // established is that a channel really completes the handshake, and that a peer which cannot
    // complete it gets no channel at all. So a real server is bound and connected to twice.
    val appId = "streaming-shuffle-auth"
    val secret = "streaming-shuffle-application-secret"

    // With authentication off the lists are empty, which reproduces the platform's behaviour for
    // every other connection in the same application rather than inventing a different one.
    val plainConf = streamingConf().set("spark.app.id", appId)
    val plainSecurity = new SecurityManager(plainConf)
    val plainTransportConf = StreamingShuffleServerHandler.streamingTransportConf(
      plainConf, security = Some(plainSecurity))
    assert(!plainSecurity.isAuthenticationEnabled(),
      "authentication must be off for the plaintext half of this case")
    assert(StreamingShuffleServerHandler.streamingClientBootstraps(plainConf, plainTransportConf,
        Some(plainSecurity)).isEmpty,
      "with authentication off a streaming consumer must install no client bootstrap")
    assert(StreamingShuffleServerHandler.streamingServerBootstraps(plainTransportConf,
        Some(plainSecurity)).isEmpty,
      "with authentication off a streaming producer must install no server bootstrap")

    // With authentication on, both ends install the platform's own handshake.
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
    // The transport configuration itself must carry the streaming module's own tuning, so the
    // authenticated channel is the streaming one rather than a borrowed block-transfer channel.
    assert(transportConf.getModuleName === StreamingShuffleServerHandler.TRANSPORT_MODULE_NAME,
      s"the authenticated channel must be tuned as the streaming module, not " +
        transportConf.getModuleName)
    assert(transportConf.enableTcpKeepAlive(),
      "the streaming module must enable operating system keep-alive")

    // A real bound producer server behind the real handshake, with a real routing listener behind
    // it, so that a frame arriving is proof the handshake completed.
    val listener = new StreamingShuffleListener(authConf)
    val transportContext = new TransportContext(transportConf, listener)
    var server: TransportServer = null
    var authorizedFactory: TransportClientFactory = null
    var strangerFactory: TransportClientFactory = null
    try {
      server = transportContext.createServer(0, serverBootstraps)
      assert(server.getPort > 0, "the producer server must have bound a port")

      // The holder of the application secret gets a channel and can put a frame on it.
      authorizedFactory = transportContext.createClientFactory(clientBootstraps)
      val client = authorizedFactory.createClient(Utils.localHostName(), server.getPort)
      try {
        assert(client.isActive, "an authenticated consumer must hold a live channel")
        assert(client.getClientId === appId,
          s"the handshake must bind the application identity, but bound ${client.getClientId}")
        val unroutableBefore = listener.unroutableFrameCount
        // A well-formed frame for a producer this executor does not host. What it proves is that
        // the frame crossed an authenticated channel and reached the routing layer: an
        // unauthenticated channel would never have been established for it to cross.
        val heartbeat = new HeartbeatMessage(4711, 1L, 0, 0L)
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

      // A peer that cannot present the application secret gets no channel at all. Asserted against
      // a *different* secret rather than against no authentication, because that is the stronger
      // statement: it is the handshake failing rather than the two ends disagreeing about whether
      // to perform one.
      val strangerConf = streamingConf()
        .set("spark.app.id", appId)
        .set(NETWORK_AUTH_ENABLED, true)
        .set(AUTH_SECRET, "a-secret-this-application-never-issued")
      val strangerSecurity = new SecurityManager(strangerConf)
      strangerFactory = transportContext.createClientFactory(
        StreamingShuffleServerHandler.streamingClientBootstraps(strangerConf, transportConf,
          Some(strangerSecurity)))
      val refusal = intercept[Exception] {
        strangerFactory.createClient(Utils.localHostName(), server.getPort)
      }
      assert(refusal != null,
        "a consumer without the application secret must be refused a streaming channel")
    } finally {
      if (strangerFactory != null) {
        strangerFactory.close()
      }
      if (authorizedFactory != null) {
        authorizedFactory.close()
      }
      if (server != null) {
        server.close()
      }
      transportContext.close()
      listener.releaseAll()
    }
  }

  test("with the kill switch engaged the writer and reader are the sort delegate's own") {
    // Asserted as a positive identity rather than as the absence of a streaming type: the writer
    // and reader the gated manager hands back must be the very classes a standalone
    // SortShuffleManager hands back for the same handle. That is stronger than "not streaming", and
    // the precise meaning of indistinguishable for these two service-provider methods.
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
    // The definitive statement of "indistinguishable from sort-based shuffle": the same workload,
    // the same output set. The baseline is captured before the context under test is started,
    // because Spark permits one context per JVM.
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

  test("on a real cluster executors resolve the driver endpoint and stream correct output") {
    // local-cluster gives separate executor JVMs, so each builds its own StreamingShuffleManager
    // and resolves the coordinator by name rather than hosting it. A successful streaming shuffle
    // here is therefore evidence that the driver-registers / executor-resolves rendezvous works,
    // which is the one thing local mode cannot show.
    val baseline = sortBaselineGroupedOutput(numPartitions = 4)
    sc = new SparkContext(withLocalMaster(streamingConf(),
      "streaming-shuffle-manager-cluster", "local-cluster[2,1,1024]"))
    assert(SparkEnv.get.shuffleManager.isInstanceOf[StreamingShuffleManager],
      "the driver must hold a streaming manager on a real cluster too")
    val observed = groupedOutputAsSet(sc, numPartitions = 4)
    assertNoDataLoss(observed, baseline, "a streaming shuffle across two real executor JVMs")
  }

  test("on a real cluster every executor registers the streaming metrics source") {
    // Registration reaches executors through the same static-source list, with no
    // executor-lifecycle wiring of its own; export to an operator still needs a configured sink.
    // Asserted from inside a task, which is the only place an executor's own metrics system is
    // observable.
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

/**
 * A probe that records what the seven-argument `getReader` was handed.
 *
 * It exists to make the `final` five-argument overload's forwarding observable. The override
 * returns a reader that yields nothing, which is a complete implementation of the reader contract
 * rather than a placeholder: the case under test asserts on the recorded arguments, and a reader
 * that produced records would be asserting something the trait does not promise here.
 *
 * @param conf configuration the manager reads; the kill-switch posture is used so no environment is
 *             touched
 */
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
