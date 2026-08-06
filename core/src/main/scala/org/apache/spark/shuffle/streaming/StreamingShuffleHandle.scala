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
 * streaming shuffle path and to carry the streaming-specific registration state that the producer
 * side and the consumer side both require. Widening `ShuffleDependency` or the public
 * `ShuffleHandle` instead would extend a public surface and force a binary-compatibility exclusion.
 *
 * The handle travels to executors inside the serialized task binary, so it stays an immutable value
 * object: nothing live -- no channel, no `SparkConf`, no endpoint reference, no clock -- may be
 * added to it. `capabilityToken` is a bearer credential, which is why it is absent from `toString`
 * and never logged.
 *
 * @tparam K the key type of the shuffle
 * @tparam V the value type of the shuffle
 * @tparam C the combiner type of the shuffle
 * @param shuffleId ID of the shuffle.
 * @param dependency the dependency describing this shuffle.
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
  // these checks sit off every hot path and surface a misconfiguration before a single record has
  // been streamed.
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
   * A stable, allocation-light rendering of the streaming registration state, used in diagnostics
   * such as `require` failures raised further downstream and assertion messages in tests.
   */
  override def toString: String =
    s"StreamingShuffleHandle(shuffleId=${this.shuffleId}, numPartitions=$numPartitions, " +
      s"protocolVersion=$protocolVersion, coordinatorEpoch=$coordinatorEpoch, " +
      s"capabilityToken=<redacted>)"
}
