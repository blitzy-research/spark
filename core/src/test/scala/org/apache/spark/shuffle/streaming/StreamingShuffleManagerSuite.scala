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
import java.util.Locale

import scala.jdk.CollectionConverters._

import org.mockito.Mockito.mock
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.spark.{Aggregator, HashPartitioner, LocalSparkContext, Partitioner, ShuffleDependency, SparkConf, SparkContext, SparkEnv, SparkFunSuite, SparkIllegalArgumentException, TaskContext}
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, SHUFFLE_STREAMING_BUFFER_SIZE_PERCENT, SHUFFLE_STREAMING_DEBUG, SHUFFLE_STREAMING_ENABLED, SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS, SHUFFLE_STREAMING_SPILL_THRESHOLD}
import org.apache.spark.metrics.source.StaticSources
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, MigratableResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.ShuffleMergedBlockId
import org.apache.spark.util.{RpcUtils, Utils}

/**
 * Tests of [[StreamingShuffleManager]] as a `ShuffleManager` service provider.
 *
 * The suite is organised around the six properties that decide whether the streaming shuffle
 * subsystem is safe to ship, because each one is an independent way for the feature to be wrong:
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
 *  6. <b>Observability.</b> Exactly four metrics reach an operator through the ordinary static
 *     source registration path, with no extra configuration and no new sink.
 *
 * ==Why some cases build a manager directly and others start a context==
 *
 * The manager's constructor dereferences `SparkEnv` only when it is going to host the coordinator
 * endpoint, which happens when `isDriver` and the behaviour gate are both true. Every other
 * combination constructs against a bare `SparkConf`. Cases that need no environment therefore build
 * the manager directly, which keeps them fast and free of cross-test coupling; cases that assert on
 * the rendezvous, on real writers and readers, or on end-to-end output start a real context. Both
 * masters named in the plan are exercised: `local[N]`, where one manager instance serves producer
 * and consumer roles, and `local-cluster`, where executors are separate JVMs that must resolve the
 * driver's endpoint by name.
 *
 * Every manager built here is stopped in a `finally`, because a manager that is not stopped leaves
 * an index resolver and possibly a coordinator endpoint behind, and the test JVM runs with memory
 * leak detection enabled.
 */
class StreamingShuffleManagerSuite extends SparkFunSuite
  with LocalSparkContext
  with StreamingShuffleTestHelper {

  import StreamingShuffleTestHelper._

  /** Fully qualified name the `streaming` short name must resolve to. */
  private val streamingManagerClassName: String = classOf[StreamingShuffleManager].getName

  /** Fully qualified name both sort short names and the default must resolve to. */
  private val sortManagerClassName: String = classOf[SortShuffleManager].getName

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

  /**
   * Builds a manager, hands it to the body and stops it afterwards whatever happens.
   *
   * @param conf configuration selecting and gating the manager
   * @param isDriver whether the manager should behave as the driver's instance
   * @param body assertions to run against the manager
   * @tparam T whatever the body returns
   * @return the body's result
   */
  private def withManager[T](conf: SparkConf, isDriver: Boolean)(
      body: StreamingShuffleManager => T): T = {
    val manager = new StreamingShuffleManager(conf, isDriver)
    try {
      body(manager)
    } finally {
      manager.stop()
    }
  }

  /**
   * Builds the sort-based delegate the streaming manager holds internally, so a case can compare
   * the two side by side, and stops it afterwards.
   *
   * `SortShuffleManager` takes exactly one argument. Passing an `isDriver` flag to it would not
   * compile, which is worth stating because the streaming manager beside it takes two.
   *
   * @param conf configuration the delegate reads
   * @param body assertions to run against the delegate
   * @tparam T whatever the body returns
   * @return the body's result
   */
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

  /** Varargs shim for Mockito's `doReturn`, matching the sort path suite's own idiom. */
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

  // -----------------------------------------------------------------------------------------------
  // 1. Tier one: selection through the ShuffleManager factory.
  //
  // The short-name table plus the reflective factory beside it are the whole of the selection
  // mechanism, so these cases go through the factory rather than naming the class directly.
  // Doing it any other way would pass even if the table entry had never been added.
  // -----------------------------------------------------------------------------------------------

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
    // Resolved through Utils.classForName, which is both the loader the style gate mandates and the
    // one the reflective factory itself uses, so this exercises the same lookup path.
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
    // wrote. The kill-switch configuration is used deliberately: it is the default posture,
    // and it needs no live SparkEnv, because no coordinator endpoint is hosted while the gate is
    // closed.
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

  // -----------------------------------------------------------------------------------------------
  // 2. Service-provider conformance.
  //
  // Every member of the trait is checked, and the two members with a shape constraint rather than a
  // behaviour constraint -- the constructor and the final five-argument getReader -- are checked
  // reflectively, because their contract is about what the class declares.
  // -----------------------------------------------------------------------------------------------

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
    // form with the whole map range, so overriding it is neither possible nor necessary.
    //
    // What the bytecode holds is worth stating, because it is not what a first guess suggests. A
    // concrete `final def` in a Scala trait cannot become a final default method -- a JVM interface
    // method may not carry that flag -- so scalac emits a `final` forwarder into every implementing
    // class instead. The five-argument method therefore IS present on this class, and what proves
    // it was inherited rather than hand-written is precisely that it is final: a user override
    // could not be. The seven-argument method is non-final, because that is the abstract member
    // this manager implements.
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
    // Differential check against the manager that demonstrably does not override the overload
    // either. An identical modifier shape means the streaming manager took no liberty with the
    // reader factories that the sort-based manager did not also take.
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
    // The delegate's verdict is the returned one, and the delegate reports success even for a
    // shuffle it never knew about, which is exactly what makes a repeated call safe. Both tiers are
    // covered because the streaming tier has extra owners to notify and each of them has to
    // tolerate being asked twice.
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

  // -----------------------------------------------------------------------------------------------
  // 3. Resolver identity.
  //
  // Which resolver the manager publishes is not cosmetic: ShuffleWriteProcessor decides whether
  // push-based merge engages by pattern matching on its type, and BlockManager reaches migration
  // support by casting it. Both consequences are pinned here, on both tiers.
  // -----------------------------------------------------------------------------------------------

  /**
   * The arity and finality of a set of reader factories, as a comparable summary.
   *
   * Reduced to a sorted sequence so that two classes can be compared without depending on the order
   * reflection happens to return methods in, which the JVM does not specify.
   *
   * @param methods the `getReader` methods declared by one class
   * @return each method as its parameter count paired with whether it is final, sorted
   */
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

  test("with streaming enabled the resolver is still a MigratableResolver") {
    // BlockManager casts the manager's resolver to MigratableResolver without testing the type
    // first, so a resolver that did not mix it in would turn every executor decommission and every
    // shuffle-block stream upload into a ClassCastException. The mixin is therefore mandatory even
    // though streamed output itself is never migratable; every migration member forwards to the
    // sort delegate, where all migratable output actually lives.
    withManager(streamingConf(), isDriver = false) { manager =>
      assert(manager.shuffleBlockResolver.isInstanceOf[MigratableResolver],
        "the published resolver must be a MigratableResolver, because BlockManager casts to it " +
          s"unconditionally. Got ${manager.shuffleBlockResolver.getClass.getName}")
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

  // -----------------------------------------------------------------------------------------------
  // 4. Two-tier gating.
  //
  // Tier one selects the class. Tier two gates its behaviour, and while it is closed every
  // service-provider call is forwarded to an internally held, unmodified SortShuffleManager.
  // -----------------------------------------------------------------------------------------------

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

  // -----------------------------------------------------------------------------------------------
  // 5. One terminus for every degradation path.
  //
  // The central safety property of the whole feature: no configuration, no failure and no resource
  // condition leaves a job without a working shuffle, because every path ends at the same
  // internally held, unmodified SortShuffleManager.
  // -----------------------------------------------------------------------------------------------

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

  test("every fallback condition routes to the same sort delegate terminus") {
    // Each condition is tripped through the policy's own public surface, with time supplied
    // explicitly rather than slept for, so the case is deterministic. What is asserted here is the
    // routing -- that a tripped policy stands streaming down -- not the detection thresholds, which
    // belong to the fallback suite.
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
    // Zero is rejected rather than silently treated as unlimited, which is the whole point of not
    // using it as the sentinel.
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

  // -----------------------------------------------------------------------------------------------
  // 7. Observability.
  //
  // Four metrics, registered by the ordinary static-source mechanism so that no new agent, sink or
  // executor-lifecycle change is needed. Registration is asserted separately from export, because
  // appending a source configures no sink: every sink stays opt-in through metrics.properties.
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

  // -----------------------------------------------------------------------------------------------
  // 8. Both masters, with a live context.
  //
  // local[N] and local-cluster exercise genuinely different code. In local mode the executor
  // deliberately skips shuffle manager initialisation because the driver's instance already exists,
  // so one manager serves producer and consumer roles and the coordinator can only ever take the
  // setupEndpoint path. local-cluster gives real executor JVMs, each of which builds its own
  // manager and must resolve the driver's endpoint by name. SharedSparkContext cannot serve the
  // second case because it hardcodes its master.
  // -----------------------------------------------------------------------------------------------

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
      // Distinct map ids, so the two writers never contend for one output file.
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
    // Nothing streaming may have been built, so the invalidation counter must still read zero.
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
    // executor-lifecycle change and no extra configuration. Asserted from inside a task, which is
    // the only place an executor's own metrics system is observable.
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

  /** The `(startMapIndex, endMapIndex, startPartition, endPartition)` tuples seen so far. */
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
