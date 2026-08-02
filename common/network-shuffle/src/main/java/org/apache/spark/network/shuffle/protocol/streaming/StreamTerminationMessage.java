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

import org.apache.spark.annotation.Private;

/**
 * The orderly end-of-stream signal of the streaming shuffle wire protocol.
 *
 * A streaming shuffle producer sends exactly one of these once it has emitted every data block of a
 * single {@code (shuffleId, mapId, partitionId)} stream. Without such a marker a consumer could
 * only infer
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
 * stream, and under this protocol's sequencing model it is required to equal {@link #totalBlocks}
 * exactly. Only data blocks consume sequence numbers: they are numbered densely from zero, while a
 * heartbeat and this terminator both report the next unissued position without claiming it. The
 * position a terminator sits at is therefore precisely the number of data blocks that preceded it,
 * and the constructor rejects any other relation.
 *
 * Requiring equality rather than merely {@code totalBlocks <= sequenceNumber} is a correctness
 * requirement and not a tightening for its own sake. A terminator claiming <em>fewer</em> blocks
 * than the positions preceding it is internally consistent under the weaker rule, yet it is exactly
 * the shape that silently truncates a stream: a consumer that has accepted {@code n} blocks and is
 * told the total was {@code m < n} reconciles the two, concludes the stream is complete and hands a
 * short result to the reduce task, with every checksum intact and no error raised anywhere. Under
 * the stronger rule that message cannot be constructed at all, on either side of the wire, so the
 * only remaining outcome of a shortfall is the one that is safe: the consumer detects that it is
 * missing blocks and either has them replayed or fails the fetch so the stage is recomputed.
 *
 * Because the relation is an equality, the sequence space stays gap-free right up to the end, and a
 * terminator can be detected as missing by the same reasoning that detects a missing block.
 *
 * Wire layout, {@value StreamingShuffleMessage#HEADER_ENCODED_LENGTH} header bytes then one long,
 * for 33 bytes in total; framed by {@link StreamingShuffleMessage#toByteBuffer()} it occupies 34.
 *
 * <pre>
 *   +-----------------+-----------+---------+-------------+----------------+-------------+
 *   | protocolVersion | shuffleId | mapId   | partitionId | sequenceNumber | totalBlocks |
 *   | byte, 1 byte    | int, 4    | long, 8 | int, 4      | long, 8        | long, 8     |
 *   +-----------------+-----------+---------+-------------+----------------+-------------+
 *   |&lt;----------------- inherited header ----------------------&gt;|&lt;-- body --&gt;|
 * </pre>
 *
 * Three of the sibling messages encode to the same 33 bytes, so length can never be used to tell
 * them apart. The one-byte discriminator that precedes the body does that on the wire, and {@link
 * #equals(Object)} does it in memory by requiring the concrete type to match before it looks at a
 * single field.
 *
 * The inherited {@code shuffleId}, {@code mapId}, {@code partitionId} and {@code sequenceNumber}
 * are all required to be non-negative, and {@link StreamingShuffleMessage} enforces that centrally
 * on construction and on decode, so a terminator can no more name an impossible partition or
 * stream position
 * than it can claim a negative block count.
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
@Private
public final class StreamTerminationMessage extends StreamingShuffleMessage {

  /**
   * Total number of data blocks the producer emitted for this stream, counted over the whole life
   * of the stream and never negative.
   *
   * Zero is a legitimate value: a reduce partition to which no record was assigned produces no data
   * block at all and still terminates, so rejecting zero would make an empty shuffle partition
   * unrepresentable. The field is exposed directly, as every message in the surrounding shuffle
   * protocol exposes its own fields, and is additionally readable through {@link #totalBlocks()} so
   * that a call site reading it beside the inherited {@link #shuffleId()}, {@link #partitionId()}
   * and {@link #sequenceNumber()} accessors can do so in one style.
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
   * @param mapId identifier of the map task whose output stream is ending
   * @param partitionId identifier of the shuffle partition whose stream is ending
   * @param sequenceNumber position of this terminator within the stream; must equal
   *                       {@code totalBlocks}
   * @param totalBlocks number of data blocks emitted for this stream; zero is legal, negative is
   *                    not
   * @throws IllegalArgumentException if totalBlocks is negative, if it differs from sequenceNumber,
   *                                  or if any header field is negative
   */
  public StreamTerminationMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long totalBlocks) {
    super(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
    // Signalled as an argument failure rather than an error: a peer that sends a nonsensical count
    // is a recoverable protocol fault the caller converts into a fetch failure, not a condition
    // that should tear the executor down.
    if (totalBlocks < 0) {
      throw new IllegalArgumentException("Total number of blocks in a streaming shuffle stream " +
        "cannot be negative: " + totalBlocks);
    }
    // Data blocks are the only messages that consume a sequence number, and they are numbered
    // densely from zero; a heartbeat and this terminator both report the next unissued position
    // without claiming it. The position a terminator sits at is therefore exactly the number of
    // data blocks that preceded it, and any other relation is a protocol violation.
    //
    // The equality is enforced in both directions, because each direction is unsafe on its own.
    // An inflated total -- more blocks than positions -- makes a stream that has in fact ended look
    // permanently incomplete, so the consumer waits for blocks that were never sent. An undercount
    // -- fewer blocks than positions -- is worse, because it is silent: a consumer that accepted n
    // blocks and is told the total was m < n reconciles the two figures, concludes the stream
    // completed and hands a truncated result to the reduce task with every checksum intact and no
    // error raised anywhere. Refusing both here, on the one construction path that decode also
    // funnels through, is what makes a shortfall detectable rather than self-consistent.
    if (totalBlocks != sequenceNumber) {
      throw new IllegalArgumentException("Stream termination claims " + totalBlocks +
        " block(s) but sits at sequenceNumber " + sequenceNumber + " for shuffle " + shuffleId +
        " partition " + partitionId + "; a terminator must sit at exactly the position that " +
        "follows the blocks it announces, because only data blocks consume sequence numbers");
    }
    this.totalBlocks = totalBlocks;
  }

  /**
   * Creates a terminator stamped with this build's protocol version. This is the constructor a
   * producer uses, so that {@link StreamingShuffleMessage#CURRENT_PROTOCOL_VERSION} need not be
   * repeated at every construction site.
   *
   * @param shuffleId identifier of the shuffle this stream belongs to
   * @param mapId identifier of the map task whose output stream is ending
   * @param partitionId identifier of the shuffle partition whose stream is ending
   * @param sequenceNumber position of this terminator within the stream; must equal
   *                       {@code totalBlocks}
   * @param totalBlocks number of data blocks emitted for this stream; zero is legal, negative is
   *                    not
   * @throws IllegalArgumentException if totalBlocks is negative, if it differs from sequenceNumber,
   *                                  or if any header field is negative
   */
  public StreamTerminationMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long totalBlocks) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, sequenceNumber, totalBlocks);
  }

  /**
   * Creates a terminator from a header just read off the wire, which is the form {@link
   * #decode(ByteBuf)} uses. Taking the header as one value rather than as five positional arguments
   * removes any chance of transposing {@code shuffleId} and {@code partitionId} on the way in.
   *
   * @param header the decoded header, which must not be null
   * @param totalBlocks number of data blocks emitted for this stream; zero is legal, negative is
   *                    not
   * @throws NullPointerException if header is null
   * @throws IllegalArgumentException if totalBlocks is negative, if it differs from sequenceNumber,
   *                                  or if any header field is negative
   */
  public StreamTerminationMessage(Header header, long totalBlocks) {
    this(Objects.requireNonNull(header, "header").protocolVersion(), header.shuffleId(),
      header.mapId(), header.partitionId(), header.sequenceNumber(), totalBlocks);
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
    // The shared header, then the eight bytes of totalBlocks: 33 bytes in all.
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
   * @throws IllegalArgumentException if the buffer is truncated, if it carries a negative block
   *         count, if that count differs from the header's sequence number, or if its header
   *         carries a negative shuffle id, partition id or sequence number
   */
  static StreamTerminationMessage decode(ByteBuf buf) {
    Header header = readHeader(buf);
    // Exactly, not at least: a surplus is content the codec would never examine, and tolerating it
    // would let a peer append bytes that survive the message boundary unparsed.
    if (buf.readableBytes() != Long.BYTES) {
      throw new IllegalArgumentException("Malformed stream termination message: expected exactly " +
        Long.BYTES + " byte(s) for totalBlocks but " + buf.readableBytes() + " remain");
    }
    long totalBlocks = buf.readLong();
    return new StreamTerminationMessage(header, totalBlocks);
  }
}
