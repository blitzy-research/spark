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

package org.apache.spark.network.shuffle.protocol.streaming;

import java.util.Objects;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import org.apache.spark.annotation.Private;

/**
 * Computes and verifies the CRC32C checksums that protect streaming shuffle blocks in flight,
 * using the JDK's {@link java.util.zip.CRC32C} so that no dependency is added for it.
 *
 * A block checksum covers the block's routing identity as well as its payload, so a frame whose
 * bytes survived but whose identity was rewritten fails verification. CRC32C detects corruption
 * only: it is not a message authentication code, and a peer able to rewrite a frame can rewrite the
 * checksum with it. Authentication remains the transport's responsibility.
 *
 * @since 4.2.0
 */
@Private
public class StreamingShuffleChecksum {

  /**
   * Name of the fixed checksum algorithm, spelled as Spark's existing shuffle checksum
   * configuration already spells it.
   */
  public static final String ALGORITHM = "CRC32C";

  private StreamingShuffleChecksum() {
  }

  public static long compute(byte[] data) {
    Objects.requireNonNull(data, "data");
    return compute(data, 0, data.length);
  }

  /**
   * Computes the CRC32C checksum over length bytes of the given array starting at offset.
   *
   * @throws NullPointerException if data is null
   * @throws IllegalArgumentException if offset or length is negative, or if the range reaches
   *     past the end of data
   */
  public static long compute(byte[] data, int offset, int length) {
    Objects.requireNonNull(data, "data");
    // Comparing offset against `data.length - length` rather than testing `offset + length` against
    // `data.length` keeps the bound exact: length is already known to be non-negative, so the
    // subtraction cannot overflow, whereas the addition can and would then wrap into a range that
    // looks valid.
    if (offset < 0 || length < 0 || offset > data.length - length) {
      throw new IllegalArgumentException("Invalid checksum range: offset=" + offset +
        ", length=" + length + ", array length=" + data.length);
    }
    // A fresh instance per call: Checksum accumulates mutable state and is not thread-safe, and
    // this method runs concurrently on Netty event-loop threads and on task threads.
    Checksum checksum = new CRC32C();
    checksum.update(data, offset, length);
    return checksum.getValue();
  }

  public static boolean verify(byte[] data, long expectedChecksum) {
    return compute(data) == expectedChecksum;
  }

  /**
   * Verifies length bytes of the given array starting at offset against the checksum that
   * accompanied them on the wire.
   *
   * @throws NullPointerException if data is null
   * @throws IllegalArgumentException if offset or length is negative, or if the range reaches
   *     past the end of data
   */
  public static boolean verify(byte[] data, int offset, int length, long expectedChecksum) {
    return compute(data, offset, length) == expectedChecksum;
  }

  /**
   * Number of bytes in the canonical metadata preamble that {@link #computeBlock} folds into a
   * block checksum ahead of the payload: shuffleId (4) + mapId (8) + partitionId (4) +
   * sequenceNumber (8) + payloadLength (4).
   */
  static final int BLOCK_METADATA_PREAMBLE_LENGTH = 4 + 8 + 4 + 8 + 4;

  /**
   * Computes the canonical checksum of a streaming shuffle data block, binding the payload to the
   * identity it was produced under.
   *
   * @throws NullPointerException if payload is null
   */
  public static long computeBlock(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      byte[] payload) {
    Objects.requireNonNull(payload, "payload");
    return computeBlock(shuffleId, mapId, partitionId, sequenceNumber, payload, 0, payload.length);
  }

  public static long computeBlock(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      byte[] payload,
      int offset,
      int length) {
    Objects.requireNonNull(payload, "payload");
    // Same exact-bound reasoning as compute(byte[], int, int): subtract rather than add, so the
    // comparison cannot be defeated by integer overflow.
    if (offset < 0 || length < 0 || offset > payload.length - length) {
      throw new IllegalArgumentException("Invalid checksum range: offset=" + offset +
        ", length=" + length + ", array length=" + payload.length);
    }
    byte[] preamble = new byte[BLOCK_METADATA_PREAMBLE_LENGTH];
    writeInt(preamble, 0, shuffleId);
    writeLong(preamble, 4, mapId);
    writeInt(preamble, 12, partitionId);
    writeLong(preamble, 16, sequenceNumber);
    writeInt(preamble, 24, length);
    // A fresh instance per call, as in compute(...). The preamble and the payload are folded into
    // one accumulation, so the result is a single CRC32C over their concatenation and not a
    // combination of two independent values.
    Checksum checksum = new CRC32C();
    checksum.update(preamble, 0, preamble.length);
    checksum.update(payload, offset, length);
    return checksum.getValue();
  }

  /**
   * Verifies a received block against the canonical checksum that accompanied it, binding the
   * payload to the header fields under which it arrived.
   *
   * @throws NullPointerException if payload is null
   */
  public static boolean verifyBlock(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      byte[] payload,
      long expectedChecksum) {
    return computeBlock(shuffleId, mapId, partitionId, sequenceNumber, payload) == expectedChecksum;
  }

  private static void writeInt(byte[] buf, int index, int value) {
    buf[index] = (byte) (value >>> 24);
    buf[index + 1] = (byte) (value >>> 16);
    buf[index + 2] = (byte) (value >>> 8);
    buf[index + 3] = (byte) value;
  }

  private static void writeLong(byte[] buf, int index, long value) {
    buf[index] = (byte) (value >>> 56);
    buf[index + 1] = (byte) (value >>> 48);
    buf[index + 2] = (byte) (value >>> 40);
    buf[index + 3] = (byte) (value >>> 32);
    buf[index + 4] = (byte) (value >>> 24);
    buf[index + 5] = (byte) (value >>> 16);
    buf[index + 6] = (byte) (value >>> 8);
    buf[index + 7] = (byte) value;
  }
}
