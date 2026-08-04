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
 * That equality is also why the count needs no field of its own. The announced total <em>is</em>
 * the position the terminator sits at, so it is carried by the inherited {@code sequenceNumber} and
 * {@link #totalBlocks()} reads it back from there. One field cannot disagree with itself, which
 * retires the whole class of defect the equality check above existed to catch, and it keeps this
 * message the fixed {@value StreamingShuffleMessage#CONTROL_MESSAGE_ENCODED_LENGTH} bytes the
 * specification gives a control message.
 *
 * Wire layout, {@value StreamingShuffleMessage#HEADER_ENCODED_LENGTH} header bytes then the
 * producer id, for {@value StreamingShuffleMessage#CONTROL_MESSAGE_ENCODED_LENGTH} bytes in total;
 * framed by {@link StreamingShuffleMessage#toByteBuffer()} it occupies 26.
 *
 * <pre>
 *   +-----------------+-----------+-------------+----------------+---------+
 *   | protocolVersion | shuffleId | partitionId | sequenceNumber | mapId   |
 *   | byte, 1 byte    | int, 4    | int, 4      | long, 8        | long, 8 |
 *   +-----------------+-----------+-------------+----------------+---------+
 *   |&lt;--------------- inherited header ------------------&gt;|&lt;- body -&gt;|
 * </pre>
 *
 * All four control messages encode to the same {@value
 * StreamingShuffleMessage#CONTROL_MESSAGE_ENCODED_LENGTH} bytes, so length can never be used to
 * tell them apart. The one-byte discriminator that precedes the body does that on the wire, and
 * {@link #equals(Object)} does it in memory by requiring the concrete type to match before it looks
 * at a single field.
 *
 * The inherited {@code shuffleId}, {@code partitionId} and {@code sequenceNumber}, and the producer
 * id that opens the body, are all required to be non-negative, and {@link StreamingShuffleMessage}
 * enforces that centrally on construction and on decode, so a terminator can no more name an
 * impossible partition or stream position than it can claim a negative block count.
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
   * Bytes this message adds after the shared header: eight, for the producer id, which is the only
   * field of its own that it carries.
   *
   * Named rather than repeated so that {@link #encodedLength()} and the body-length check in {@link
   * #decode(ByteBuf)} cannot drift apart.
   */
  private static final int BODY_ENCODED_LENGTH = PRODUCER_ID_ENCODED_LENGTH;

  /**
   * Creates a terminator carrying an explicit protocol version, which is how a decoded message
   * keeps the version it actually arrived with instead of silently adopting this build's own.
   *
   * @param protocolVersion the wire revision this message was encoded with
   * @param shuffleId identifier of the shuffle whose stream has ended
   * @param mapId identifier of the map task whose output stream has ended
   * @param partitionId identifier of the shuffle partition whose stream has ended
   * @param totalBlocks number of data blocks the producer sent on this stream, which is also the
   *                    position this terminator sits at; zero is the valid announcement of an empty
   *                    partition
   */
  public StreamTerminationMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long totalBlocks) {
    // Signalled as an argument failure rather than an error: a peer that sends a nonsensical count
    // is a recoverable protocol fault the caller converts into a fetch failure, not a condition
    // that should tear the executor down. The base constructor refuses a negative sequence number,
    // and the count IS the sequence number, so the check lives there and needs no repetition here.
    super(protocolVersion, shuffleId, mapId, partitionId, totalBlocks);
  }

  /**
   * Creates a terminator stamped with the protocol version this build speaks, which is how a
   * producer raises one.
   *
   * @param shuffleId identifier of the shuffle whose stream has ended
   * @param mapId identifier of the map task whose output stream has ended
   * @param partitionId identifier of the shuffle partition whose stream has ended
   * @param totalBlocks number of data blocks the producer sent on this stream
   */
  public StreamTerminationMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long totalBlocks) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, totalBlocks);
  }

  /**
   * Creates a terminator from a header just read off the wire, together with the producer id that
   * opened the body, which is the form {@link #decode(ByteBuf)} uses.
   *
   * @param header the decoded header, which must not be null
   * @param mapId identifier of the map task whose output stream has ended
   * @throws NullPointerException if header is null
   */
  public StreamTerminationMessage(Header header, long mapId) {
    super(header, mapId);
  }

  /**
   * Number of data blocks the producer sent on this stream before ending it, read back from the
   * position this terminator sits at. Zero is the valid announcement of an empty partition.
   *
   * @return the announced block total
   */
  public long totalBlocks() {
    return sequenceNumber();
  }

  @Override
  protected StreamingShuffleMessageType type() {
    return StreamingShuffleMessageType.STREAM_TERMINATION;
  }

  @Override
  public int hashCode() {
    // The header hash is the whole identity of this message, the announced total included, because
    // the total is the header's own sequence number.
    return headerHashCode();
  }

  @Override
  public String toString() {
    return "StreamTerminationMessage[" + headerToString() +
        ",totalBlocks=" + totalBlocks() + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof StreamTerminationMessage o) {
      return headerEquals(o);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return HEADER_ENCODED_LENGTH + BODY_ENCODED_LENGTH;
  }

  @Override
  public void encode(ByteBuf buf) {
    // Header first, as everywhere in this family, then the producer id that opens every body.
    encodeHeader(buf);
    encodeProducerId(buf);
  }

  /**
   * Reads a terminator from a buffer positioned at the first byte of the encoded body, that is,
   * immediately after the framing type discriminator has been consumed.
   *
   * It is package-private on purpose: {@code StreamingShuffleMessage.Decoder.fromByteBuffer} is the
   * only entry point a peer's bytes may take into the protocol, and that holds only if a caller
   * outside this package cannot reach a concrete decoder and skip the checks the central decoder
   * applies to the frame as a whole.
   *
   * @param buf the buffer to read from, which must not be null
   * @return the terminator just consumed from the buffer
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if the frame is truncated, if its body is not exactly the
   *         specified size, or if any identifier falls outside its domain
   */
  static StreamTerminationMessage decode(ByteBuf buf) {
    Header header = readHeader(buf);
    // Exactly, not at least: a surplus is content the codec would never examine, and tolerating it
    // would let a peer append bytes that survive the message boundary unparsed.
    if (buf.readableBytes() != BODY_ENCODED_LENGTH) {
      throw new IllegalArgumentException("Malformed streaming shuffle stream termination: " +
        "expected exactly " + BODY_ENCODED_LENGTH + " byte(s) of body but " +
        buf.readableBytes() + " remain");
    }
    long mapId = readProducerId(buf);
    return new StreamTerminationMessage(header, mapId);
  }
}
