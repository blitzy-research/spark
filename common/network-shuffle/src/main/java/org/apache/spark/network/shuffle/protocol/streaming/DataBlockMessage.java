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

import java.util.Arrays;
import java.util.Objects;

import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encoders;

/**
 * One block of streaming shuffle data on its way from a producer executor to a consumer executor.
 *
 * This is the only message of the streaming shuffle family that carries a payload, and so the
 * only one whose encoded length varies. A producer frames the records it has buffered for a reduce
 * partition into a run of these messages, stamps each with the next sequence number and with a
 * CRC32C over its bytes, and puts them on the wire while the map stage is still running. A
 * consumer recomputes the checksum before the block's records become visible and then acknowledges
 * the position it has reached, which is what lets the producer release the memory the block held.
 *
 * <b>Wire layout.</b> The shared header comes first, exactly as {@link StreamingShuffleMessage}
 * requires of every concrete message, followed by the checksum and then the length-prefixed
 * payload.
 *
 * <pre>
 *   +--------+-----------+--------------+---------------+------------------+
 *   | type   | header    | checksum     | payload len   | payload          |
 *   | 1 byte | 17 bytes  | long, 8      | int, 4        | payload.length   |
 *   +--------+-----------+--------------+---------------+------------------+
 *      framing            the body encoded by encode(ByteBuf)
 * </pre>
 *
 * {@link #encodedLength()} is therefore exactly {@code 29 + payload.length}, and a framed message
 * produced by {@link #toByteBuffer()} occupies {@code 30 + payload.length} bytes. The payload is
 * written with {@code Encoders.ByteArrays}, the same length-prefixed form the sibling
 * block-transfer messages already use for their own byte arrays, rather than a hand-rolled
 * equivalent that could encode the same bytes a slightly different way.
 *
 * <b>Size cap.</b> A payload may be at most {@value #MAX_BLOCK_SIZE_BYTES} bytes. The cap is what
 * makes pipelining work at all: output has to leave the producer in small pieces rather than as
 * one buffer per partition for reduce-side work to overlap map-side work. It is a wire constant
 * rather than a tunable, because both peers must agree on it for a receiver to be entitled to
 * reject an over-size length prefix, and it is enforced at three points so that no route into an
 * instance can bypass it:
 *
 * <ul>
 *   <li>every constructor rejects an over-size array, so a producer cannot build one;</li>
 *   <li>{@link #decode(ByteBuf)} builds through a constructor, so the wire cannot smuggle one
 *       past that check;</li>
 *   <li>{@link #decode(ByteBuf)} additionally inspects the payload's length prefix
 *       <em>without consuming it</em> and rejects a negative, over-size or unbacked value
 *       <em>before</em> any array is allocated. This one matters most: the length prefix arrives
 *       from a remote peer, and {@code Encoders.ByteArrays.decode} allocates an array of exactly
 *       the size the prefix claims with no bound of its own, so a corrupt or hostile frame would
 *       otherwise turn four bytes into an arbitrarily large allocation.</li>
 * </ul>
 *
 * A payload of exactly {@value #MAX_BLOCK_SIZE_BYTES} bytes is legal; one byte more is not. An
 * empty payload is also legal, which lets a zero-record partition be streamed and checksummed on
 * the same code path as any other.
 *
 * <b>Failure reporting.</b> Everything this class rejects is signalled with
 * {@link IllegalArgumentException}, matching the sibling shuffle protocol messages. That is
 * deliberate on both sides of the boundary: these are recoverable conditions, so a caller can
 * catch one and fall back to sort-based shuffle or ask for the block again, and this module owns
 * none of the surrounding context needed to raise a Spark error condition. Translating a rejection
 * into a protocol-level failure belongs to the caller.
 *
 * <b>Immutability and threading.</b> An instance is immutable once constructed and carries no
 * logging and no dependency on Spark core, so it is safe to hand between Netty event-loop threads
 * and task threads without further synchronisation. {@link #payload()} returns the block's array
 * directly rather than a defensive copy, because copying up to two mebibytes on every access would
 * defeat the point of streaming; by the same token callers must treat that array as read-only, as
 * they already must with the byte-array fields of the sibling block-transfer messages.
 *
 * @since 4.2.0
 */
public class DataBlockMessage extends StreamingShuffleMessage {

  /**
   * Largest payload, in bytes, that a single streaming shuffle data block may carry.
   *
   * Two mebibytes. This is a wire constant shared by producer and consumer and is not
   * configurable: a receiver can only reject an over-size length prefix before allocating for it
   * if it already knows the bound the sender was held to.
   */
  public static final int MAX_BLOCK_SIZE_BYTES = 2 * 1024 * 1024;

  /**
   * Bytes the CRC32C value occupies on the wire, being one {@code long}.
   *
   * Named rather than spelled as a bare literal so that {@link #encodedLength()} and the bounds
   * check in {@link #decode(ByteBuf)} cannot come to disagree about the same field, which is the
   * reasoning {@link StreamingShuffleMessage#HEADER_ENCODED_LENGTH} applies to the header.
   */
  private static final int CHECKSUM_ENCODED_LENGTH = 8;

  /**
   * Bytes the payload's length prefix occupies, being the {@code int} that
   * {@code Encoders.ByteArrays} writes ahead of the array itself.
   */
  private static final int PAYLOAD_LENGTH_PREFIX_LENGTH = 4;

  private final long checksum;
  private final byte[] payload;

  /**
   * Creates a data block carrying an explicit protocol version, which is how a decoded block keeps
   * the version it actually arrived with instead of silently adopting this build's own.
   *
   * @param protocolVersion the wire revision this message was encoded with
   * @param shuffleId identifier of the shuffle this block belongs to
   * @param partitionId identifier of the reduce partition this block belongs to
   * @param sequenceNumber position of this block within its partition's stream, counted from zero
   * @param checksum the CRC32C value covering the whole of the payload
   * @param payload the block's bytes; must not be null and must not exceed
   *                {@link #MAX_BLOCK_SIZE_BYTES}. The array is retained, not copied
   * @throws NullPointerException if payload is null
   * @throws IllegalArgumentException if payload is longer than {@link #MAX_BLOCK_SIZE_BYTES}
   */
  public DataBlockMessage(
      byte protocolVersion,
      int shuffleId,
      int partitionId,
      long sequenceNumber,
      long checksum,
      byte[] payload) {
    super(protocolVersion, shuffleId, partitionId, sequenceNumber);
    this.checksum = checksum;
    // Validated here, in the one constructor every other route funnels through, so that neither a
    // producer nor the decoder can construct an over-size block.
    this.payload = checkPayload(payload);
  }

  /**
   * Creates a data block stamped with this build's protocol version. This is the constructor a
   * producer uses when it already holds a checksum for the payload.
   *
   * @param shuffleId identifier of the shuffle this block belongs to
   * @param partitionId identifier of the reduce partition this block belongs to
   * @param sequenceNumber position of this block within its partition's stream, counted from zero
   * @param checksum the CRC32C value covering the whole of the payload
   * @param payload the block's bytes; must not be null and must not exceed
   *                {@link #MAX_BLOCK_SIZE_BYTES}. The array is retained, not copied
   * @throws NullPointerException if payload is null
   * @throws IllegalArgumentException if payload is longer than {@link #MAX_BLOCK_SIZE_BYTES}
   */
  public DataBlockMessage(
      int shuffleId,
      int partitionId,
      long sequenceNumber,
      long checksum,
      byte[] payload) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, partitionId, sequenceNumber, checksum, payload);
  }

  /**
   * Creates a data block from a header just read off the wire, which is how
   * {@link #decode(ByteBuf)} rebuilds one. Passing the header as a single value rather than as four
   * positional arguments removes any chance of transposing {@code shuffleId} and
   * {@code partitionId} on the way in.
   *
   * @param header the decoded header; must not be null
   * @param checksum the CRC32C value the producer computed over the payload
   * @param payload the block's bytes; must not be null and must not exceed
   *                {@link #MAX_BLOCK_SIZE_BYTES}. The array is retained, not copied
   * @throws NullPointerException if header or payload is null
   * @throws IllegalArgumentException if payload is longer than {@link #MAX_BLOCK_SIZE_BYTES}
   */
  public DataBlockMessage(Header header, long checksum, byte[] payload) {
    // The base constructor rejects a null header, so the cap check below is the only guard this
    // constructor has to add of its own.
    super(header);
    this.checksum = checksum;
    this.payload = checkPayload(payload);
  }

  /**
   * Creates a data block and computes its CRC32C from the payload, for the common producer-side
   * case where the checksum is not already in hand.
   *
   * The computation is routed through {@link StreamingShuffleChecksum} rather than performed here,
   * so that producer and consumer are provably running the same arithmetic over the same bytes.
   *
   * @param shuffleId identifier of the shuffle this block belongs to
   * @param partitionId identifier of the reduce partition this block belongs to
   * @param sequenceNumber position of this block within its partition's stream, counted from zero
   * @param payload the block's bytes; must not be null and must not exceed
   *                {@link #MAX_BLOCK_SIZE_BYTES}. The array is retained, not copied
   * @return a data block whose checksum covers the whole of the payload
   * @throws NullPointerException if payload is null
   * @throws IllegalArgumentException if payload is longer than {@link #MAX_BLOCK_SIZE_BYTES}
   */
  public static DataBlockMessage withComputedChecksum(
      int shuffleId,
      int partitionId,
      long sequenceNumber,
      byte[] payload) {
    // Checked before the payload is read, so an over-size block is rejected without first paying
    // for a checksum over bytes that are about to be discarded.
    checkPayload(payload);
    return new DataBlockMessage(shuffleId, partitionId, sequenceNumber,
      StreamingShuffleChecksum.compute(payload), payload);
  }

  /**
   * The CRC32C value the producer computed over this block's payload.
   *
   * A consumer that finds a mismatch reports this number as the expected value alongside the one
   * it recomputed, so it stays reachable rather than being folded into a bare verdict.
   *
   * @return the checksum carried with this block
   */
  public long checksum() {
    return checksum;
  }

  /**
   * This block's bytes.
   *
   * The array is returned directly rather than copied, because a block runs to as much as
   * {@link #MAX_BLOCK_SIZE_BYTES} bytes and copying it on every access would undo the saving
   * streaming exists to make. Callers must therefore treat it as read-only.
   *
   * @return the payload backing this block, never null
   */
  public byte[] payload() {
    return payload;
  }

  /**
   * Number of payload bytes this block carries, which is what a diagnostic wants in place of the
   * bytes themselves.
   *
   * @return the payload length, between zero and {@link #MAX_BLOCK_SIZE_BYTES} inclusive
   */
  public int payloadLength() {
    return payload.length;
  }

  /**
   * Recomputes the CRC32C over this block's payload and compares it with the value the producer
   * sent, which is the check a consumer runs before making the block's records visible.
   *
   * When this returns false the caller needs both numbers for its diagnostic: the expected value is
   * {@link #checksum()} and the recomputed one is
   * {@code StreamingShuffleChecksum.compute(payload())}.
   *
   * @return true if the payload matches the checksum it arrived with, false if the block is corrupt
   */
  public boolean verifyChecksum() {
    return StreamingShuffleChecksum.verify(payload, checksum);
  }

  @Override
  protected StreamingShuffleMessageType type() {
    return StreamingShuffleMessageType.DATA_BLOCK;
  }

  @Override
  public int hashCode() {
    // Combined the way the sibling messages with byte-array fields combine theirs. Handing the
    // array to Objects.hash would hash it by identity, so two blocks holding equal bytes in
    // distinct arrays would disagree, and a decoded block would never match the one it came from.
    int headerAndChecksum = headerHashCode() * 41 + Long.hashCode(checksum);
    return headerAndChecksum * 41 + Arrays.hashCode(payload);
  }

  @Override
  public String toString() {
    // The payload bytes are deliberately not rendered: a block runs to two mebibytes, and this
    // string reaches error messages and diagnostics, where its length has to stay bounded.
    return "DataBlockMessage[" + headerToString() + ",checksum=" + checksum +
        ",payloadLength=" + payload.length + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof DataBlockMessage o) {
      return headerEquals(o)
        && checksum == o.checksum
        && Arrays.equals(payload, o.payload);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    // 17 (header) + 8 (checksum) + 4 (length prefix) + payload.length == 29 + payload.length
    return HEADER_ENCODED_LENGTH + CHECKSUM_ENCODED_LENGTH +
        Encoders.ByteArrays.encodedLength(payload);
  }

  @Override
  public void encode(ByteBuf buf) {
    // The header always goes first, and in the order the base class owns, so that it cannot drift
    // apart from the read in decode below.
    encodeHeader(buf);
    buf.writeLong(checksum);
    Encoders.ByteArrays.encode(buf, payload);
  }

  /**
   * Reads a data block back off the wire, in the same field order {@link #encode(ByteBuf)} wrote.
   *
   * The buffer's bytes arrive from a remote peer, so every length this method depends on is checked
   * rather than assumed, and the payload's length prefix is validated before anything is allocated
   * for it. The block is then built through a constructor, so the size cap applies to a decoded
   * block exactly as it does to a constructed one.
   *
   * @param buf a buffer positioned at the first header byte of an encoded data block
   * @return the decoded block
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if the frame is truncated, or if the payload's length prefix
   *                                 is negative, exceeds {@link #MAX_BLOCK_SIZE_BYTES}, or claims
   *                                 more bytes than the buffer actually holds
   */
  public static DataBlockMessage decode(ByteBuf buf) {
    // readHeader rejects a null buffer and a header too short to be read.
    Header header = readHeader(buf);
    if (buf.readableBytes() < CHECKSUM_ENCODED_LENGTH) {
      throw new IllegalArgumentException("Truncated streaming shuffle data block: expected " +
        CHECKSUM_ENCODED_LENGTH + " checksum byte(s) but only " + buf.readableBytes() +
        " byte(s) remain");
    }
    long checksum = buf.readLong();
    checkPayloadLengthPrefix(buf);
    byte[] payload = Encoders.ByteArrays.decode(buf);
    return new DataBlockMessage(header, checksum, payload);
  }

  /**
   * Validates the payload's length prefix without consuming it, so that
   * {@code Encoders.ByteArrays.decode} is only ever reached with a length that is known to be
   * non-negative, within the block size cap, and actually backed by bytes in the buffer.
   *
   * This is the guard that keeps a corrupt or hostile four-byte prefix from becoming an arbitrarily
   * large allocation: the array is sized from the prefix with no bound of its own, so the bound has
   * to be applied here, ahead of it. The read is absolute, at an explicit index, which leaves the
   * reader index exactly where the delegate expects to find it.
   *
   * @param buf a buffer positioned at the payload's length prefix
   * @throws IllegalArgumentException if the prefix is absent, negative, over-size or unbacked
   */
  private static void checkPayloadLengthPrefix(ByteBuf buf) {
    if (buf.readableBytes() < PAYLOAD_LENGTH_PREFIX_LENGTH) {
      throw new IllegalArgumentException("Truncated streaming shuffle data block: expected " +
        PAYLOAD_LENGTH_PREFIX_LENGTH + " payload length byte(s) but only " + buf.readableBytes() +
        " byte(s) remain");
    }
    int length = buf.getInt(buf.readerIndex());
    if (length < 0) {
      throw new IllegalArgumentException(
        "Negative streaming shuffle data block payload length: " + length);
    }
    if (length > MAX_BLOCK_SIZE_BYTES) {
      throw new IllegalArgumentException("Streaming shuffle data block payload length of " +
        length + " byte(s) exceeds the maximum block size of " + MAX_BLOCK_SIZE_BYTES +
        " byte(s)");
    }
    // Comparing against the bytes left after the prefix rather than against the whole readable
    // count keeps the prefix itself out of the payload's budget.
    int available = buf.readableBytes() - PAYLOAD_LENGTH_PREFIX_LENGTH;
    if (available < length) {
      throw new IllegalArgumentException("Truncated streaming shuffle data block payload: the " +
        "length prefix declares " + length + " byte(s) but only " + available +
        " byte(s) remain");
    }
  }

  /**
   * Rejects a null payload and one that exceeds the block size cap, returning the array unchanged
   * so that it can be assigned straight to the field.
   *
   * The comparison is strictly greater than the cap: a payload of exactly
   * {@link #MAX_BLOCK_SIZE_BYTES} bytes is a full block, not an over-size one.
   *
   * @param payload the candidate payload
   * @return payload, unchanged
   * @throws NullPointerException if payload is null
   * @throws IllegalArgumentException if payload is longer than {@link #MAX_BLOCK_SIZE_BYTES}
   */
  private static byte[] checkPayload(byte[] payload) {
    Objects.requireNonNull(payload, "payload");
    if (payload.length > MAX_BLOCK_SIZE_BYTES) {
      throw new IllegalArgumentException("Streaming shuffle data block payload is " +
        payload.length + " byte(s), which exceeds the maximum block size of " +
        MAX_BLOCK_SIZE_BYTES + " byte(s)");
    }
    return payload;
  }
}
