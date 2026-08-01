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

import io.netty.buffer.ByteBuf;

/**
 * The orderly end-of-stream signal of the streaming shuffle wire protocol.
 *
 * A streaming shuffle producer sends exactly one of these once it has emitted every data block of a
 * single {@code (shuffleId, partitionId)} stream. Without such a marker a consumer could only infer
 * that the producer had finished from the fact that nothing further arrived, and silence is also
 * the symptom of the failure case: a producer that has crashed likewise stops sending. This message
 * is what separates the two. Its arrival means the stream ended because it was complete, whereas
 * continued silence without it is what the consumer's liveness timer is armed to catch, and that
 * lapse is the trigger for discarding every block already accepted from the producer and having the
 * upstream stage recomputed instead.
 *
 * Completion versus truncation. Knowing that a producer believes it has finished is not by itself
 * enough, because the last few blocks of a stream may still have been lost in flight. The message
 * therefore carries {@link #totalBlocks}, the number of data blocks the producer actually emitted
 * for this stream, so the consumer can reconcile that figure against the number of blocks it
 * accepted. Agreement is an orderly end of stream; a shortfall is a truncated one, and the consumer
 * treats it exactly as it treats any other missing block, by asking for a replay while the block is
 * still inside the producer's unacknowledged window or by failing the fetch so that the stage is
 * recomputed once it is not. The count is a total rather than a delta so that the check holds
 * however many times the stream was throttled, spilled or partially retransmitted along the way.
 *
 * The header's {@code sequenceNumber} is the position this terminator itself occupies in the
 * stream, that is, one past the last data block, so the sequence space stays gap-free right up to
 * the end and a terminator can be detected as missing by the same reasoning that detects a missing
 * block.
 *
 * Wire layout, {@value StreamingShuffleMessage#HEADER_ENCODED_LENGTH} header bytes then one long,
 * for 25 bytes in total; framed by {@link StreamingShuffleMessage#toByteBuffer()} it occupies 26.
 *
 * <pre>
 *   +-----------------+-----------+-------------+----------------+-------------+
 *   | protocolVersion | shuffleId | partitionId | sequenceNumber | totalBlocks |
 *   | byte, 1 byte    | int, 4    | int, 4      | long, 8        | long, 8     |
 *   +-----------------+-----------+-------------+----------------+-------------+
 *   |&lt;----------------- inherited header ----------------------&gt;|&lt;-- body --&gt;|
 * </pre>
 *
 * Three of the sibling messages encode to the same 25 bytes, so length can never be used to tell
 * them apart. The one-byte discriminator that precedes the body does that on the wire, and
 * {@link #equals(Object)} does it in memory by requiring the concrete type to match before it looks
 * at a single field.
 *
 * This type is internal to Spark. It is pure data plus codec: it holds no reference to Spark core,
 * reads no configuration and no clock, and emits no log line, so that the decision of what an
 * orderly or a truncated end of stream means belongs entirely to the caller that owns the
 * surrounding context. Every field is supplied by that caller and every field is final, so an
 * instance is immutable once constructed and may be handed between Netty event-loop threads and
 * task threads with no further synchronisation.
 *
 * @since 4.2.0
 */
public class StreamTerminationMessage extends StreamingShuffleMessage {

  /**
   * Total number of data blocks the producer emitted for this stream, counted over the whole life
   * of the stream and never negative.
   *
   * Zero is a legitimate value: a reduce partition to which no record was assigned produces no data
   * block at all and still terminates, so rejecting zero would make an empty shuffle partition
   * unrepresentable. The field is exposed directly, as every message in the surrounding shuffle
   * protocol exposes its own fields, and is additionally readable through {@link #totalBlocks()} so
   * that a call site reading it beside the inherited {@link #shuffleId()},
   * {@link #partitionId()} and {@link #sequenceNumber()} accessors can do so in one style.
   */
  public final long totalBlocks;

  /**
   * Creates a terminator carrying an explicit protocol version, which is how a decoded message
   * keeps the version it actually arrived with rather than silently adopting this build's own.
   *
   * This is the constructor every other one funnels through, so the block-count check below is
   * applied on exactly one path and therefore covers construction by a producer and reconstruction
   * by {@link #decode(ByteBuf)} alike.
   *
   * @param protocolVersion the wire revision this message was encoded with
   * @param shuffleId identifier of the shuffle this stream belongs to
   * @param partitionId identifier of the shuffle partition whose stream is ending
   * @param sequenceNumber position of this terminator within the stream, one past the last data
   *                       block
   * @param totalBlocks number of data blocks emitted for this stream; zero is legal, negative is
   *                    not
   * @throws IllegalArgumentException if totalBlocks is negative
   */
  public StreamTerminationMessage(
      byte protocolVersion,
      int shuffleId,
      int partitionId,
      long sequenceNumber,
      long totalBlocks) {
    super(protocolVersion, shuffleId, partitionId, sequenceNumber);
    // Signalled as an argument failure rather than an error: a peer that sends a nonsensical count
    // is a recoverable protocol fault the caller converts into a fetch failure, not a condition
    // that should tear the executor down.
    if (totalBlocks < 0) {
      throw new IllegalArgumentException("Total number of blocks in a streaming shuffle stream " +
        "cannot be negative: " + totalBlocks);
    }
    this.totalBlocks = totalBlocks;
  }

  /**
   * Creates a terminator stamped with this build's protocol version. This is the constructor a
   * producer uses, so that {@link StreamingShuffleMessage#CURRENT_PROTOCOL_VERSION} need not be
   * repeated at every construction site.
   *
   * @param shuffleId identifier of the shuffle this stream belongs to
   * @param partitionId identifier of the shuffle partition whose stream is ending
   * @param sequenceNumber position of this terminator within the stream, one past the last data
   *                       block
   * @param totalBlocks number of data blocks emitted for this stream; zero is legal, negative is
   *                    not
   * @throws IllegalArgumentException if totalBlocks is negative
   */
  public StreamTerminationMessage(
      int shuffleId,
      int partitionId,
      long sequenceNumber,
      long totalBlocks) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, partitionId, sequenceNumber, totalBlocks);
  }

  /**
   * Creates a terminator from a header just read off the wire, which is the form
   * {@link #decode(ByteBuf)} uses. Taking the header as one value rather than as four positional
   * arguments removes any chance of transposing {@code shuffleId} and {@code partitionId} on the
   * way in.
   *
   * @param header the decoded header, which must not be null
   * @param totalBlocks number of data blocks emitted for this stream; zero is legal, negative is
   *                    not
   * @throws NullPointerException if header is null
   * @throws IllegalArgumentException if totalBlocks is negative
   */
  public StreamTerminationMessage(Header header, long totalBlocks) {
    this(Objects.requireNonNull(header, "header").protocolVersion(), header.shuffleId(),
      header.partitionId(), header.sequenceNumber(), totalBlocks);
  }

  /**
   * Total number of data blocks the producer emitted for this stream. Reads the same value as the
   * {@link #totalBlocks} field, in the accessor style the inherited header fields use.
   *
   * @return the block count carried by this terminator, never negative
   */
  public long totalBlocks() {
    return totalBlocks;
  }

  @Override
  protected StreamingShuffleMessageType type() {
    return StreamingShuffleMessageType.STREAM_TERMINATION;
  }

  @Override
  public int hashCode() {
    return Objects.hash(headerHashCode(), totalBlocks);
  }

  @Override
  public String toString() {
    return "StreamTerminationMessage[" + headerToString() +
        ",totalBlocks=" + totalBlocks + "]";
  }

  @Override
  public boolean equals(Object other) {
    // The concrete type is tested first and deliberately: the acknowledgement, heartbeat and
    // retransmission messages all carry a header and a single long as well, so two of them holding
    // identical values must still never compare equal to one another.
    if (other instanceof StreamTerminationMessage o) {
      return headerEquals(o)
        && totalBlocks == o.totalBlocks;
    }
    return false;
  }

  @Override
  public int encodedLength() {
    // The shared header, then the eight bytes of totalBlocks: 25 bytes in all.
    return HEADER_ENCODED_LENGTH + 8;
  }

  @Override
  public void encode(ByteBuf buf) {
    // The header always goes first, and is written by the base class so that this order and the
    // order decode reads in cannot drift apart.
    encodeHeader(buf);
    buf.writeLong(totalBlocks);
  }

  /**
   * Reads a terminator from a buffer positioned at the first header byte, that is, immediately
   * after the framing type byte that identified it.
   *
   * The remaining length is checked rather than asserted, because these bytes arrive from a remote
   * peer and a truncated frame must be reported the same way in production as it is under test. The
   * result is built through the public constructor, so the block-count check applies to a decoded
   * message exactly as it applies to one a producer created. The protocol version is deliberately
   * not re-validated here: {@link StreamingShuffleMessage.Decoder#fromByteBuffer} settles
   * compatibility before it dispatches, and a message decoded directly must round-trip whatever
   * version it arrived with.
   *
   * @param buf the buffer to read from, positioned at the first header byte
   * @return the terminator just consumed from the buffer
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if the buffer is truncated, or if it carries a negative block
   *         count
   */
  public static StreamTerminationMessage decode(ByteBuf buf) {
    Header header = readHeader(buf);
    if (buf.readableBytes() < Long.BYTES) {
      throw new IllegalArgumentException("Truncated stream termination message: expected " +
        Long.BYTES + " byte(s) for totalBlocks but only " + buf.readableBytes() + " remain");
    }
    long totalBlocks = buf.readLong();
    return new StreamTerminationMessage(header, totalBlocks);
  }
}
