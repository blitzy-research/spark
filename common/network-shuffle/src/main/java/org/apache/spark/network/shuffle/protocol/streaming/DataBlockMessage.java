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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.spark.annotation.Private;
import org.apache.spark.network.protocol.Encoders;

/**
 * One block of streaming shuffle data on its way from a producer executor to a consumer executor.
 *
 * The payload is capped at {@value #MAX_BLOCK_SIZE_BYTES} bytes so that a map output is pipelined
 * as many small blocks rather than shipped as one, and so that a receiver can size its own bound
 * before it reads. The inherited sequence number counts blocks per producer and per partition, and
 * the CRC32C carried alongside the payload covers the block's routing identity as well as its
 * bytes, so a frame that is intact but misrouted fails verification too.
 *
 * @since 4.2.0
 */
@Private
public final class DataBlockMessage extends StreamingShuffleMessage {

  /** Largest payload, in bytes, that a single streaming shuffle data block may carry. */
  public static final int MAX_BLOCK_SIZE_BYTES = 2 * 1024 * 1024;

  /** Bytes the CRC32C value occupies on the wire, being one {@code long}. */
  private static final int CHECKSUM_ENCODED_LENGTH = 8;

  /** Bytes of the length prefix {@code Encoders.ByteArrays} writes ahead of the payload. */
  private static final int PAYLOAD_LENGTH_PREFIX_LENGTH = 4;

  private static final long CHECKSUM_NOT_COMPUTED = -1L;

  /** Bytes a data block occupies on the wire over and above its payload. */
  public static final int FRAMING_OVERHEAD_BYTES =
    StreamingShuffleMessage.FRAME_TYPE_PREFIX_LENGTH +
      StreamingShuffleMessage.HEADER_ENCODED_LENGTH +
      StreamingShuffleMessage.PRODUCER_ID_ENCODED_LENGTH + CHECKSUM_ENCODED_LENGTH +
      PAYLOAD_LENGTH_PREFIX_LENGTH;

  /** Largest number of bytes one data block may occupy on the wire, framing included. */
  public static final int MAX_ENCODED_FRAME_BYTES = MAX_BLOCK_SIZE_BYTES + FRAMING_OVERHEAD_BYTES;

  private final long checksum;
  private final byte[] payload;
  private final PayloadReservation payloadReservation;
  private final AtomicBoolean payloadReservationHeld;

  /** Whether this block has already been shown to match the checksum it arrived with. */
  private transient volatile boolean checksumVerified;

  /** The locally computed CRC32C value, cached for both success and failure. */
  private transient volatile long computedChecksum = CHECKSUM_NOT_COMPUTED;

  /**
   * The one constructor that assigns the payload field, and therefore the single place where
   * ownership of the array is decided.
   */
  private DataBlockMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long checksum,
      byte[] payload,
      boolean ownsPayload) {
    this(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber, checksum, payload,
      ownsPayload, null, false);
  }

  private DataBlockMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long checksum,
      byte[] payload,
      boolean ownsPayload,
      PayloadReservation payloadReservation,
      boolean reservationHeld) {
    super(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
    this.checksum = checksum;
    byte[] checked = checkPayload(payload);
    this.payload = ownsPayload ? checked : checked.clone();
    this.payloadReservation = payloadReservation;
    this.payloadReservationHeld = new AtomicBoolean(reservationHeld);
  }

  public DataBlockMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long checksum,
      byte[] payload) {
    this(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber, checksum, payload, false);
  }

  public DataBlockMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long checksum,
      byte[] payload) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, sequenceNumber, checksum, payload,
      false);
  }

  public DataBlockMessage(Header header, long mapId, long checksum, byte[] payload) {
    // The base constructor rejects a null header, so the cap check below is the only guard this
    // constructor has to add of its own.
    this(Objects.requireNonNull(header, "header").protocolVersion(), header.shuffleId(),
      mapId, header.partitionId(), header.sequenceNumber(), checksum, payload, false);
  }

  /**
   * Creates a data block that adopts the given array outright, without copying it.
   *
   * @throws NullPointerException if payload is null
   * @throws IllegalArgumentException if payload is longer than {@link #MAX_BLOCK_SIZE_BYTES}
   */
  @Private
  public static DataBlockMessage withOwnedPayload(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long checksum,
      byte[] payload) {
    return new DataBlockMessage(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber,
      checksum, payload, true);
  }

  /**
   * Creates a data block and computes its checksum, for the common producer-side case where the
   * value is not already in hand.
   *
   * @throws NullPointerException if payload is null
   * @throws IllegalArgumentException if payload is longer than {@link #MAX_BLOCK_SIZE_BYTES}
   */
  public static DataBlockMessage withComputedChecksum(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      byte[] payload) {
    // Checked before the payload is read, so an over-size block is rejected without first paying
    // for a checksum over bytes that are about to be discarded.
    checkPayload(payload);
    long computed =
      StreamingShuffleChecksum.computeBlock(shuffleId, mapId, partitionId, sequenceNumber, payload);
    DataBlockMessage block = new DataBlockMessage(CURRENT_PROTOCOL_VERSION, shuffleId, mapId,
      partitionId, sequenceNumber, computed, payload, false);
    block.computedChecksum = computed;
    block.checksumVerified = true;
    return block;
  }

  /** Creates a data block that computes its checksum and adopts the caller's immutable payload. */
  @Private
  public static DataBlockMessage withComputedChecksumAndOwnedPayload(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      byte[] payload) {
    checkPayload(payload);
    long computed =
      StreamingShuffleChecksum.computeBlock(shuffleId, mapId, partitionId, sequenceNumber, payload);
    DataBlockMessage block = new DataBlockMessage(CURRENT_PROTOCOL_VERSION, shuffleId, mapId,
      partitionId, sequenceNumber, computed, payload, true);
    block.computedChecksum = computed;
    block.checksumVerified = true;
    return block;
  }

  public long checksum() {
    return checksum;
  }

  /** This block's bytes, as a read-only buffer over the block's own array. */
  public ByteBuffer payloadBuffer() {
    return ByteBuffer.wrap(payload).asReadOnlyBuffer();
  }

  /** A fresh copy of this block's bytes, which the caller owns and may mutate freely. */
  public byte[] copyPayload() {
    return payload.clone();
  }

  /**
   * Number of payload bytes this block carries, which is what a diagnostic wants in place of the
   * bytes themselves.
   */
  public int payloadLength() {
    return payload.length;
  }

  /** Transfers a decoder reservation to the consumer's own per-sequence accounting. */
  @Private
  public boolean transferPayloadReservation() {
    return payloadReservationHeld.compareAndSet(true, false);
  }

  /**
   * Returns a decoder reservation for a block that was rejected before consumer accounting adopted
   * it.
   */
  @Private
  public boolean releasePayloadReservation() {
    if (payloadReservationHeld.compareAndSet(true, false)) {
      payloadReservation.release(payload.length);
      return true;
    }
    return false;
  }

  @Private
  public boolean hasPayloadReservation() {
    return payloadReservationHeld.get();
  }

  /**
   * Recomputes this block's checksum and compares it with the value the producer sent, which is the
   * check a consumer runs before making the block's records visible.
   */
  public boolean verifyChecksum() {
    if (checksumVerified) {
      return true;
    }
    boolean intact = computedChecksum() == checksum;
    if (intact) {
      checksumVerified = true;
    }
    return intact;
  }

  /**
   * Returns the checksum computed locally over this block, scanning the immutable payload at most
   * once.
   */
  @Private
  public long computedChecksum() {
    long computed = computedChecksum;
    if (computed == CHECKSUM_NOT_COMPUTED) {
      computed = StreamingShuffleChecksum.computeBlock(
        shuffleId(), mapId(), partitionId(), sequenceNumber(), payload);
      computedChecksum = computed;
    }
    return computed;
  }

  @Override
  protected StreamingShuffleMessageType type() {
    return StreamingShuffleMessageType.DATA_BLOCK;
  }

  @Override
  public int hashCode() {
    // Combined the way the sibling messages with byte-array fields combine theirs.
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
    return HEADER_ENCODED_LENGTH + PRODUCER_ID_ENCODED_LENGTH + CHECKSUM_ENCODED_LENGTH +
        Encoders.ByteArrays.encodedLength(payload);
  }

  @Override
  public void encode(ByteBuf buf) {
    // The header always goes first, and in the order the base class owns, so that it cannot drift
    // apart from the read in decode below; the producer id follows it, as it does in every message
    // of this family, so that a router finds it at one offset whatever the type.
    encodeHeader(buf);
    encodeProducerId(buf);
    buf.writeLong(checksum);
    Encoders.ByteArrays.encode(buf, payload);
  }

  @Override
  protected ByteBuf encodeManagedFrame() {
    ByteBuf header = Unpooled.buffer(FRAMING_OVERHEAD_BYTES);
    header.writeByte(type().id());
    encodeHeader(header);
    encodeProducerId(header);
    header.writeLong(checksum);
    header.writeInt(payload.length);
    assert header.writableBytes() == 0 : "Writable header bytes remain: " + header.writableBytes();
    return Unpooled.wrappedUnmodifiableBuffer(
      header, Unpooled.wrappedBuffer(payload).asReadOnly());
  }

  /**
   * Reads a data block back off the wire, in the same field order {@link #encode(ByteBuf)} wrote.
   *
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if the frame is truncated, if its header carries a
   *     negative shuffle id, partition id or sequence number, or if the payload's length prefix is
   *     negative, exceeds {@link #MAX_BLOCK_SIZE_BYTES}, or does not account for exactly the bytes
   *     the buffer holds
   */
  static DataBlockMessage decode(ByteBuf buf) {
    return decode(buf, null);
  }

  static DataBlockMessage decode(ByteBuf buf, PayloadReservation payloadReservation) {
    // readHeader rejects a null buffer, a header too short to be read, an unsupported protocol
    // version and a negative identifier.
    Header header = readHeader(buf);
    // At least the producer id, the checksum and the payload's length prefix must be present.
    int minimumBody =
      PRODUCER_ID_ENCODED_LENGTH + CHECKSUM_ENCODED_LENGTH + PAYLOAD_LENGTH_PREFIX_LENGTH;
    if (buf.readableBytes() < minimumBody) {
      throw new IllegalArgumentException("Truncated streaming shuffle data block: expected at " +
        "least " + minimumBody + " body byte(s) but only " + buf.readableBytes() + " remain");
    }
    long mapId = readProducerId(buf);
    long checksum = buf.readLong();
    int payloadLength = checkPayloadLengthPrefix(buf);
    boolean reserved = payloadReservation != null;
    if (reserved && !payloadReservation.tryReserve(
        header.shuffleId(), mapId, header.partitionId(), header.sequenceNumber(), payloadLength)) {
      throw new PayloadReservationRejectedException(
        header.shuffleId(), mapId, header.partitionId(), header.sequenceNumber(), payloadLength);
    }
    boolean transferred = false;
    try {
      // Consume the prefix only after the reservation has succeeded, then allocate exactly the
      // validated length.
      buf.readInt();
      byte[] payload = new byte[payloadLength];
      buf.readBytes(payload);
      DataBlockMessage decoded = new DataBlockMessage(
        header.protocolVersion(), header.shuffleId(), mapId, header.partitionId(),
        header.sequenceNumber(), checksum, payload, true, payloadReservation, reserved);
      transferred = true;
      return decoded;
    } finally {
      if (reserved && !transferred) {
        payloadReservation.release(payloadLength);
      }
    }
  }

  /**
   * Validates the payload's length prefix without consuming it, so that {@code
   * Encoders.ByteArrays.decode} is only ever reached with a length that is known to be
   * non-negative, within the block size cap, and actually backed by bytes in the buffer.
   *
   * @throws IllegalArgumentException if the prefix is absent, negative, over-size, or does not
   *     account for exactly the bytes that follow it
   */
  private static int checkPayloadLengthPrefix(ByteBuf buf) {
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
    // Exactly, not at least.
    if (available != length) {
      String problem = available < length ? "Truncated" : "Over-long";
      throw new IllegalArgumentException(problem + " streaming shuffle data block payload: the " +
        "length prefix declares " + length + " byte(s) but " + available + " byte(s) remain");
    }
    return length;
  }

  /**
   * Rejects a null payload and one that exceeds the block size cap, returning the array unchanged
   * so that it can be assigned straight to the field.
   *
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
