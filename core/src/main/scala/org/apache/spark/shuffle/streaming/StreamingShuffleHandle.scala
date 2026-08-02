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

import org.apache.spark.ShuffleDependency
import org.apache.spark.shuffle.BaseShuffleHandle

/**
 * Subclass of [[BaseShuffleHandle]], used to identify a shuffle that was registered on the
 * streaming shuffle path and to carry the streaming-specific registration state that the
 * producer side and the consumer side both require.
 *
 * This type exists so that streaming registration state reaches the streaming shuffle writer
 * and the streaming shuffle reader without widening any shared type. `registerShuffle` returns
 * a `ShuffleHandle`, and both `getWriter` and the seven-argument `getReader` receive one back,
 * which makes the handle the only non-invasive carrier for this state. Adding these fields to
 * `ShuffleDependency` or to the `@DeveloperApi` `ShuffleHandle` base class instead would widen
 * a public surface, break binary compatibility and force a MiMa exclusion, so it is
 * deliberately not done. Following the precedent set by `SerializedShuffleHandle` and
 * `BypassMergeSortShuffleHandle`, this class is `private[spark]` and extends the
 * `private[spark]` [[BaseShuffleHandle]] rather than the public `ShuffleHandle`.
 *
 * The four pieces of streaming registration state carried here are:
 *
 *  - `numPartitions`, the reduce partition count, captured at registration time from
 *    `dependency.partitioner.numPartitions`. The writer divides its buffer budget by this value
 *    to obtain the per-partition allowance,
 *    `(executorMemory * bufferPercent / 100) / numPartitions`.
 *  - `protocolVersion`, the streaming wire-protocol version that the registering side speaks.
 *    Holding it here gives the consumer a local expectation to compare a peeked message version
 *    against, so a producer/consumer version mismatch is detected by an explicit compatibility
 *    check rather than inferred from a parse failure.
 *  - `coordinatorEpoch`, the epoch stamped by the streaming shuffle coordinator at
 *    registration, which lets a reader recognize that it is talking to a stale producer
 *    generation after an upstream stage has been recomputed.
 *  - `capabilityToken`, the bearer credential minted by the coordinator when the shuffle was
 *    registered on the driver. Every coordinator operation on this shuffle -- registering a
 *    producer, heartbeating one, looking producers up, invalidating a generation, unregistering
 *    the shuffle -- must present it, because the coordinator is an `RpcEnv` endpoint that every
 *    peer of the application can reach and a shuffle id is a small guessable integer.
 *
 * Carrying the token here is what makes it reach executors safely. The handle travels inside the
 * serialized task binary, which is the same channel that already carries the shuffle dependency
 * and the closure, and is delivered only to executors the driver has assigned work to. A peer
 * that was never given a task for this shuffle therefore never learns the token, and so cannot
 * hijack, invalidate or drop the shuffle's registration. The token is deliberately not logged and
 * is deliberately absent from `toString`, so that a handle can be rendered into a diagnostic or an
 * assertion message without disclosing the credential.
 *
 * `ShuffleHandle` extends `java.io.Serializable` and the handle is shipped to executors inside
 * the serialized task binary, so this class is deliberately an immutable value object: three
 * primitives and one immutable credential string. Nothing live -- no channel, no `SparkConf`, no
 * `RpcEndpointRef`, no metric handle, no clock -- may ever be added to it, and it carries no
 * mutable state.
 *
 * @tparam K the key type of the shuffle
 * @tparam V the value type of the shuffle
 * @tparam C the combiner type of the shuffle
 * @param shuffleId ID of the shuffle. Forwarded to [[BaseShuffleHandle]] and never redeclared
 *                  here, because `ShuffleHandle` already exposes it as a val.
 * @param dependency the dependency describing this shuffle. Forwarded to [[BaseShuffleHandle]]
 *                   and never redeclared here, because it is already a val on that class.
 * @param numPartitions number of reduce partitions this shuffle streams to; must be positive
 * @param protocolVersion streaming wire-protocol version in use; must be positive
 * @param coordinatorEpoch coordinator epoch stamped at registration; must be non-negative
 * @param capabilityToken coordinator capability token for this shuffle; must be non-empty
 */
private[spark] class StreamingShuffleHandle[K, V, C](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, C],
    val numPartitions: Int,
    val protocolVersion: Byte,
    val coordinatorEpoch: Long,
    val capabilityToken: String)
  extends BaseShuffleHandle[K, V, C](shuffleId, dependency) {

  // The streaming registration state is validated once, at construction time on the driver, so
  // these checks sit off every hot path and surface a misconfiguration before a single record
  // has been streamed. Java deserialization does not re-run constructors, so the executors that
  // receive this handle inside the task binary pay nothing for them. `this.shuffleId` reads the
  // val inherited from `ShuffleHandle`, which the super constructor has already initialized;
  // referring to it that way keeps the `shuffleId` constructor parameter field-free so that
  // nothing shadows the inherited member.
  require(numPartitions > 0,
    s"numPartitions must be positive for shuffle ${this.shuffleId}, but was $numPartitions")
  require(protocolVersion > 0,
    s"protocolVersion must be positive for shuffle ${this.shuffleId}, but was $protocolVersion")
  require(coordinatorEpoch >= 0L,
    s"coordinatorEpoch must be non-negative for shuffle ${this.shuffleId}, " +
      s"but was $coordinatorEpoch")
  // Checked for presence but never echoed: a require message becomes an exception message, which is
  // logged, so the token's value must not appear in it.
  require(capabilityToken != null && capabilityToken.nonEmpty,
    s"capabilityToken must be non-empty for shuffle ${this.shuffleId}")

  /**
   * A stable, allocation-light rendering of the streaming registration state, used in
   * diagnostics such as `require` failures raised further downstream and assertion messages in
   * tests. Derived purely from primitives, so it is safe to call on any thread and never
   * touches the shuffle dependency.
   *
   * `capabilityToken` is deliberately omitted. This rendering ends up in exception messages and
   * log records, and a credential that is printed there is a credential that has leaked, so the
   * token's presence is reported without its value.
   */
  override def toString: String =
    s"StreamingShuffleHandle(shuffleId=${this.shuffleId}, numPartitions=$numPartitions, " +
      s"protocolVersion=$protocolVersion, coordinatorEpoch=$coordinatorEpoch, " +
      s"capabilityToken=<redacted>)"
}
