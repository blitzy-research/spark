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
 * Every block the streaming shuffle producer puts on the wire carries a CRC32C value, and the
 * consumer recomputes that value on receipt before the block's records are made visible. The
 * algorithm is fixed to CRC32C and is deliberately not configurable: it is supplied by the JDK as
 * {@link java.util.zip.CRC32C}, so this helper adds no dependency of any kind.
 *
 * Two families of entry point are offered, and the distinction matters. {@link #computeBlock(int,
 * long, int, long, byte[])} is the one the data-block protocol uses: it binds the payload to the
 * shuffle, map task, partition and sequence number it was produced under, so a block whose header
 * is rewritten in flight fails verification even though its payload bytes are intact. The map task
 * is part of that binding because one listener per executor serves every producer on it, which
 * makes the map id the field that decides whose output a block is: a rewritten map id would
 * otherwise reattribute an intact block to a different producer's stream undetected.
 * {@link #compute(byte[])} covers a byte range alone and is retained for callers that genuinely
 * have no block identity to bind -- checksumming a spill file segment, for instance.
 *
 * Both the expected and the computed value are reachable through this API, which is why {@link
 * #compute(byte[])} returns the raw value rather than the class exposing verification alone. A
 * consumer that detects a mismatch must report the two numbers side by side in its diagnostics, so
 * a helper that answered only yes or no would leave the computed value unobtainable. {@link
 * #verify(byte[], long)} is provided for the common case where the caller needs nothing more than
 * the verdict.
 *
 * All methods are static and the class holds no state. A fresh {@link java.util.zip.Checksum}
 * instance is allocated on every call, because a Checksum accumulates mutable state across updates
 * and is not thread-safe, and these methods are invoked concurrently from Netty event-loop threads
 * and from task threads. No instance is ever cached or shared, so every method here is safe to call
 * from any number of threads at once and is free of side effects.
 *
 * This class is intentionally free of logging and of any coupling to Spark core: a corrupt block is
 * reported by the caller, which owns the surrounding context, and the checksum layer confines
 * itself to arithmetic over bytes.
 *
 * @since 4.2.0
 */
@Private
public class StreamingShuffleChecksum {

  /**
   * Name of the fixed checksum algorithm, spelled as Spark's existing shuffle checksum
   * configuration already spells it. Exposed for diagnostics and error messages only; the streaming
   * protocol does not negotiate the algorithm.
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
   * zero-length range is legal and yields the CRC32C of empty input, so a payload-free block needs
   * no special case on this path.
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
   * A checksum taken over the payload alone attests only that some sequence of bytes arrived
   * intact. It says nothing about <em>where those bytes belong</em>, so a block whose header is
   * rewritten in flight -- moved to a different partition, replayed under a different sequence
   * number, or attributed to a different shuffle -- still verifies cleanly and is then consumed as
   * though it were legitimately addressed. Folding the identifying metadata into the same CRC32C
   * closes that gap: the value now attests to the payload <em>and</em> to the four routing and
   * position fields
   * that place it in the stream, so any header rewrite is detected by exactly the check that
   * already runs on every block. The cost is a fixed one -- a
   * {@link #BLOCK_METADATA_PREAMBLE_LENGTH}-byte preamble, that is twenty-eight bytes, to allocate,
   * fill and fold in -- rather than a second pass over the payload, which is what an independent
   * header checksum would have required.
   *
   * The map id is one of those four fields, and it has to be. A single listener per executor serves
   * every producer running on it, so the map id is what decides which producer's stream a block
   * belongs to; leaving it out would let an intact block be reattributed to a different map task's
   * stream without the check noticing, which is precisely the class of error this binding exists to
   * catch.
   *
   * The preamble is serialized in big-endian order, matching the wire encoding of the header
   * itself, so producer and consumer derive byte-identical input without a shared helper buffer.
   * The payload length is included as well as the payload, which makes the encoding unambiguous:
   * without it, a differently split payload of the same total content could collide.
   *
   * @param shuffleId identifier of the shuffle the block belongs to
   * @param mapId identifier of the map task whose output the block carries
   * @param partitionId identifier of the reduce partition the block belongs to
   * @param sequenceNumber position of the block within its partition stream
   * @param payload the block payload; must not be null
   * @return the unsigned CRC32C value over the metadata preamble followed by the payload
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

  /**
   * Computes the canonical checksum of a streaming shuffle data block whose payload occupies a
   * slice of a larger buffer, so that the slice need not be copied out merely to be checksummed.
   * See {@link #computeBlock(int, long, int, long, byte[])} for why the metadata is folded in.
   *
   * @param shuffleId identifier of the shuffle the block belongs to
   * @param mapId identifier of the map task whose output the block carries
   * @param partitionId identifier of the reduce partition the block belongs to
   * @param sequenceNumber position of the block within its partition stream
   * @param payload the array backing the block payload; must not be null
   * @param offset index of the first payload byte to include
   * @param length number of payload bytes to include
   * @return the unsigned CRC32C value over the metadata preamble followed by the payload slice
   * @throws NullPointerException if payload is null
   * @throws IllegalArgumentException if offset or length is negative, or if the range reaches past
   *                                 the end of payload
   */
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
    // A fresh instance per call, for the same thread-safety reason as compute(...): the preamble
    // and the payload are folded into one accumulation so the result is a single CRC32C over the
    // concatenation, not a combination of two independent values.
    Checksum checksum = new CRC32C();
    checksum.update(preamble, 0, preamble.length);
    checksum.update(payload, offset, length);
    return checksum.getValue();
  }

  /**
   * Verifies a received block against the canonical checksum that accompanied it, binding the
   * payload to the header fields under which it arrived. A block whose header was rewritten in
   * flight fails this check even when its payload bytes are intact.
   *
   * @param shuffleId shuffleId read from the received header
   * @param mapId mapId read from the received header
   * @param partitionId partitionId read from the received header
   * @param sequenceNumber sequenceNumber read from the received header
   * @param payload the received payload; must not be null
   * @param expectedChecksum the value the producer put on the wire
   * @return true if the recomputed value matches, false if the block is corrupt or misaddressed
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

  /**
   * Writes a big-endian int into buf at the given index. Hand-rolled rather than routed through a
   * Netty buffer or a ByteBuffer so that this class stays free of any coupling beyond the JDK, and
   * so that computing a checksum allocates nothing beyond the fixed-size preamble.
   */
  private static void writeInt(byte[] buf, int index, int value) {
    buf[index] = (byte) (value >>> 24);
    buf[index + 1] = (byte) (value >>> 16);
    buf[index + 2] = (byte) (value >>> 8);
    buf[index + 3] = (byte) value;
  }

  /**
   * Writes a big-endian long into buf at the given index. See {@link #writeInt} for why this is
   * hand-rolled.
   */
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
