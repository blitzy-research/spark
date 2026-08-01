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
 * The subsystem admits exactly three protocol-integrity failures, and each one is declared as a
 * named condition in Spark's central error catalogue,
 * `common/utils/src/main/resources/error/error-conditions.json`, at SQLSTATE `XXKST` -- the state
 * the pre-existing shuffle checksum-verification failure already uses:
 *
 *  - `STREAMING_SHUFFLE_CHECKSUM_VERIFY_FAILED`, a CRC32C mismatch on a received block, built by
 *    [[checksumVerificationFailed]].
 *  - `STREAMING_SHUFFLE_INVALID_SEQUENCE_NUMBER`, a block that arrived out of order, built by
 *    [[invalidSequenceNumber]].
 *  - `STREAMING_SHUFFLE_UNEXPECTED_MESSAGE_TYPE`, a message whose type discriminator was not the
 *    one the receiving side was ready to handle, built by [[unexpectedMessageType]].
 *
 * This object is the single place in the subsystem where a `STREAMING_SHUFFLE_*` condition is
 * constructed. The streaming shuffle reader and the streaming channel handlers call into it and
 * must never build a `SparkException` with an inline error class of their own, so that condition
 * names, message parameter names and the catalogue stay in one-to-one correspondence and can be
 * audited from one place.
 *
 * Loss of a peer is deliberately absent from the list, because neither kind of peer loss is a
 * catalogue error. A producer that stops responding is reported to the scheduler as a
 * `FetchFailedException`, whose conversion into a fetch-failure task reason is what makes the
 * unmodified DAG scheduler recompute the upstream stage. A consumer that stops acknowledging is
 * handled by retaining, and later replaying, the unacknowledged window, which is a retransmission
 * rather than a failure. Even the three integrity violations above are repaired by retransmission
 * whenever the offending block is still inside that window; the errors built here are for the
 * cases where repair is no longer possible and correctness has to be recovered by failing the
 * read, so that a task never observes a spliced or corrupted stream as if it were intact.
 *
 * Every factory returns the throwable instead of throwing it, which leaves the caller free to
 * choose where the failure surfaces: code running on a Netty event-loop thread hands it to
 * [[StreamingShuffleErrorNotifier]] for re-throw on the task thread, whereas code already on the
 * task thread can throw it directly.
 *
 * The message parameter maps below must match their catalogue templates exactly, key for key.
 * `ErrorClassesJsonReader` counts the angle-bracketed placeholders in a template and fails hard in
 * both directions: an omitted parameter makes substitution raise, and a surplus parameter is
 * rejected outright while testing, since none of these three conditions is on that reader's
 * allowlist for extra parameters. A single misspelled key is therefore a hard runtime failure and
 * not a cosmetic defect, which is why the keys are spelled out literally here rather than being
 * derived programmatically from the parameter names.
 */
private[spark] object StreamingShuffleErrors {

  /**
   * A received streaming shuffle block failed CRC32C verification.
   *
   * Raised on the consumer side once it has recomputed the checksum over the payload it actually
   * received and found it different from the value the producer stamped on the block. Both values
   * are available at the call site through the wire-protocol helper
   * `org.apache.spark.network.shuffle.protocol.streaming.StreamingShuffleChecksum`, whose
   * `compute(byte[])` and `compute(byte[], int, int)` produce the local value and whose
   * `verify(byte[], long)` performs the comparison against the value carried on the block.
   *
   * Checksums are CRC32C values, so they are unsigned 32-bit quantities widened into a `Long` and
   * are always rendered as non-negative decimals in the message.
   *
   * @param blockId identifier of the block that failed verification, as it appears on the wire
   * @param shuffleId the shuffle the failed block belongs to
   * @param expected checksum value carried on the received block
   * @param computed checksum value recomputed locally over the received payload
   * @return the error to throw, or to hand to the error notifier from an I/O thread
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
   * The producer numbers the blocks of one reduce partition consecutively precisely so that the
   * consumer can detect a gap, a duplicate or a reordering instead of splicing the stream back
   * together in the wrong order. Both numbers are reported, so the direction and the size of the
   * discontinuity are visible without correlating two log lines.
   *
   * @param shuffleId the shuffle whose stream is out of order
   * @param partitionId the reduce partition whose stream is out of order
   * @param expected sequence number the consumer was waiting for
   * @param actual sequence number carried on the block that arrived instead
   * @return the error to throw, or to hand to the error notifier from an I/O thread
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
   * A streaming shuffle message carried a type discriminator other than the expected one.
   *
   * Both sides of the protocol read a fixed header before any body, so a message whose type the
   * current state does not admit -- a data block where an acknowledgement was due, say -- is
   * detected before decoding begins, and decoding is abandoned rather than attempted against the
   * wrong layout. The same error covers a discriminator byte that maps to no known type at all.
   *
   * Both arguments are plain strings so that a caller can describe a type in whichever form is
   * most legible where the check happens: `StreamingShuffleMessageType.name()` for a discriminator
   * that decoded successfully, or a concrete class name where it did not.
   *
   * @param expected description of the message type the receiver was ready to handle
   * @param actual description of the message type that arrived instead
   * @return the error to throw, or to hand to the error notifier from an I/O thread
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
