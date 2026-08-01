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

import org.apache.spark.SparkException

/**
 * Constructors for the typed errors raised by the streaming shuffle subsystem.
 *
 * Each is a named condition in Spark's central error catalogue at SQLSTATE `XXKST`:
 *
 *  - `STREAMING_SHUFFLE_CHECKSUM_VERIFY_FAILED` from [[checksumVerificationFailed]]
 *  - `STREAMING_SHUFFLE_INVALID_SEQUENCE_NUMBER` from [[invalidSequenceNumber]]
 *  - `STREAMING_SHUFFLE_UNEXPECTED_MESSAGE_TYPE` from [[unexpectedMessageType]]
 *
 * This object is the only place the subsystem constructs one of those conditions, and the message
 * parameter keys below must match their catalogue templates key for key: a missing or surplus key
 * fails at runtime rather than degrading the message, so the keys are spelled out literally.
 *
 * Every factory returns the throwable rather than throwing it, so a caller on a Netty event-loop
 * thread can hand it to [[StreamingShuffleErrorNotifier]] for re-throw on the task thread while a
 * caller already on the task thread throws it directly.
 */
private[spark] object StreamingShuffleErrors {

  /**
   * A received streaming shuffle block failed CRC32C verification: `expected` is the value carried
   * on the block and `computed` the value recomputed locally over the payload that arrived.
   *
   * Both are unsigned 32-bit CRC32C values widened into a `Long`, so both render as non-negative
   * decimals in the message.
   */
  def checksumVerificationFailed(
      blockId: String,
      shuffleId: Int,
      expected: Long,
      computed: Long): Throwable = {
    new SparkException(
      errorClass = "STREAMING_SHUFFLE_CHECKSUM_VERIFY_FAILED",
      messageParameters = Map(
        "blockId" -> blockId,
        "shuffleId" -> shuffleId.toString,
        "expected" -> expected.toString,
        "computed" -> computed.toString),
      cause = null)
  }

  /**
   * A streaming shuffle block arrived carrying a sequence number other than the expected one.
   *
   * Blocks of one reduce partition are numbered consecutively so that a gap, a duplicate or a
   * reordering is detected rather than spliced together in the wrong order. Both numbers are
   * reported, so the direction and size of the discontinuity are visible from the one message.
   */
  def invalidSequenceNumber(
      shuffleId: Int,
      partitionId: Int,
      expected: Long,
      actual: Long): Throwable = {
    new SparkException(
      errorClass = "STREAMING_SHUFFLE_INVALID_SEQUENCE_NUMBER",
      messageParameters = Map(
        "shuffleId" -> shuffleId.toString,
        "partitionId" -> partitionId.toString,
        "expected" -> expected.toString,
        "actual" -> actual.toString),
      cause = null)
  }

  /**
   * A streaming shuffle message carried a type discriminator the current state does not admit, or
   * one that maps to no known type at all. The header is read before any body, so decoding is
   * abandoned rather than attempted against the wrong layout.
   *
   * Both arguments are plain strings so a caller can name a type in whichever form is legible where
   * the check happens: `StreamingShuffleMessageType.name()` where the discriminator decoded, or a
   * class name where it did not.
   */
  def unexpectedMessageType(expected: String, actual: String): Throwable = {
    new SparkException(
      errorClass = "STREAMING_SHUFFLE_UNEXPECTED_MESSAGE_TYPE",
      messageParameters = Map(
        "expected" -> expected,
        "actual" -> actual),
      cause = null)
  }
}
