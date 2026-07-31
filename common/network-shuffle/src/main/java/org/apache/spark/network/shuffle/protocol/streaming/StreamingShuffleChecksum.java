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
 * Computes and verifies the CRC32C checksums that protect streaming shuffle blocks in flight.
 *
 * Every block the streaming shuffle producer puts on the wire carries a CRC32C value covering its
 * payload, and the consumer recomputes that value on receipt before the block's records are made
 * visible. The algorithm is fixed to CRC32C and is deliberately not configurable: it is supplied
 * by the JDK as {@link java.util.zip.CRC32C}, so this helper adds no dependency of any kind.
 *
 * Both the expected and the computed value are reachable through this API, which is why
 * {@link #compute(byte[])} returns the raw value rather than the class exposing verification
 * alone. A consumer that detects a mismatch must report the two numbers side by side in its
 * diagnostics, so a helper that answered only yes or no would leave the computed value
 * unobtainable. {@link #verify(byte[], long)} is provided for the common case where the caller
 * needs nothing more than the verdict.
 *
 * All methods are static and the class holds no state. A fresh {@link java.util.zip.Checksum}
 * instance is allocated on every call, because a Checksum accumulates mutable state across
 * updates and is not thread-safe, and these methods are invoked concurrently from Netty
 * event-loop threads and from task threads. No instance is ever cached or shared, so every method
 * here is safe to call from any number of threads at once and is free of side effects.
 *
 * This class is intentionally free of logging and of any coupling to Spark core: a corrupt block
 * is reported by the caller, which owns the surrounding context, and the checksum layer confines
 * itself to arithmetic over bytes.
 *
 * @since 4.2.0
 */
@Private
public class StreamingShuffleChecksum {

  /**
   * Name of the fixed checksum algorithm, spelled as Spark's existing shuffle checksum
   * configuration already spells it. Exposed for diagnostics and error messages only; the
   * streaming protocol does not negotiate the algorithm.
   */
  public static final String ALGORITHM = "CRC32C";

  /**
   * This class only exposes static helpers and is never instantiated.
   */
  private StreamingShuffleChecksum() {
  }

  /**
   * Computes the CRC32C checksum over the whole of the given array.
   *
   * @param data the bytes to checksum; must not be null
   * @return the unsigned CRC32C value, carried in the low 32 bits of the result
   * @throws NullPointerException if data is null
   */
  public static long compute(byte[] data) {
    Objects.requireNonNull(data, "data");
    return compute(data, 0, data.length);
  }

  /**
   * Computes the CRC32C checksum over length bytes of the given array starting at offset. A
   * zero-length range is legal and yields the CRC32C of empty input, which lets an end-of-stream
   * block with no payload be checksummed on the same code path as any other block.
   *
   * @param data the array backing the block payload; must not be null
   * @param offset index of the first byte to include
   * @param length number of bytes to include
   * @return the unsigned CRC32C value, carried in the low 32 bits of the result
   * @throws NullPointerException if data is null
   * @throws IllegalArgumentException if offset or length is negative, or if the range reaches
   *                                 past the end of data
   */
  public static long compute(byte[] data, int offset, int length) {
    Objects.requireNonNull(data, "data");
    // Comparing offset against `data.length - length` rather than testing `offset + length`
    // against `data.length` keeps the bound exact: length is already known to be non-negative,
    // so the subtraction cannot overflow, whereas the addition can and would then wrap into a
    // range that looks valid.
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

  /**
   * Verifies the whole of the given array against the checksum that accompanied it on the wire.
   *
   * @param data the received bytes; must not be null
   * @param expectedChecksum the CRC32C value the producer computed over those bytes
   * @return true if the recomputed value matches, false if the block is corrupt
   * @throws NullPointerException if data is null
   */
  public static boolean verify(byte[] data, long expectedChecksum) {
    return compute(data) == expectedChecksum;
  }

  /**
   * Verifies length bytes of the given array starting at offset against the checksum that
   * accompanied them on the wire. Use this overload when the payload occupies a slice of a larger
   * receive buffer, so that the slice need not be copied out merely to be checked.
   *
   * @param data the array backing the block payload; must not be null
   * @param offset index of the first byte to include
   * @param length number of bytes to include
   * @param expectedChecksum the CRC32C value the producer computed over those bytes
   * @return true if the recomputed value matches, false if the block is corrupt
   * @throws NullPointerException if data is null
   * @throws IllegalArgumentException if offset or length is negative, or if the range reaches
   *                                 past the end of data
   */
  public static boolean verify(byte[] data, int offset, int length, long expectedChecksum) {
    return compute(data, offset, length) == expectedChecksum;
  }
}
