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
 * A consumer's acknowledgement of how far it has consumed a streaming shuffle partition, which is
 * what lets the producer release the memory holding everything up to that point.
 *
 * Streaming shuffle pipelines map output straight from a producer executor to a consumer executor
 * while the map stage is still running, so the producer necessarily retains every block it has
 * sent but that the consumer has not yet confirmed: those are the blocks it may still be asked to
 * replay. This message is the only thing that shrinks that unacknowledged window. On receiving it
 * the producer frees the buffers holding the acknowledged blocks and, if it had stopped reading
 * from the channel because the consumer had fallen behind, lets data flow again.
 *
 * Cumulative, not per block. The acknowledgement is cumulative in the manner of a TCP
 * acknowledgement: {@link #consumerPosition} names the highest data-block sequence number the
 * consumer has consumed and thereby acknowledges every lower one along with it. A consumer may
 * consequently acknowledge in batches rather than once per block, which is what keeps this control
 * traffic off the per-record path, and a lost acknowledgement costs nothing because the next one
 * supersedes it. The producer's reclamation rule is correspondingly simple: release every retained
 * block whose sequence number is not greater than the position just acknowledged.
 *
 * Two sequence-valued fields, both needed. The inherited {@link #sequenceNumber()} positions this
 * acknowledgement within the stream of messages the consumer sends, so that a producer can tell a
 * fresh acknowledgement from a stale one that overtook it. {@code consumerPosition} is instead the
 * value being acknowledged, a position in the stream of data blocks travelling the other way. The
 * two advance independently and neither can be derived from the other.
 *
 * Wire layout. The body is a single {@code long} placed immediately after the inherited header, so
 * the message is fixed size: {@value StreamingShuffleMessage#HEADER_ENCODED_LENGTH} bytes of
 * header plus eight of body, and one further byte for the type discriminator once framed by
 * {@link StreamingShuffleMessage#toByteBuffer()}.
 *
 * <pre>
 *   +--------+-----------------+-----------+-------------+----------------+------------------+
 *   | type   | protocolVersion | shuffleId | partitionId | sequenceNumber | consumerPosition |
 *   | 1 byte | byte, 1         | int, 4    | int, 4      | long, 8        | long, 8          |
 *   +--------+-----------------+-----------+-------------+----------------+------------------+
 *   framing prefix, then the inherited 17-byte header, then this message's 8-byte body
 * </pre>
 *
 * A position is deliberately not range-checked. Sequence numbers are counted from zero, so a
 * consumer that has consumed nothing yet must still be able to say so, and it does that with a
 * negative position; rejecting negatives here would make that inexpressible. What is checked is
 * the length of the encoded body, because those bytes arrive from a remote peer and a truncated
 * frame has to be reported the same way in production as it is under test.
 *
 * This type is internal to Spark. It neither extends nor is registered with
 * {@link org.apache.spark.network.shuffle.protocol.BlockTransferMessage}, the family it is
 * modelled on rather than joined to, so the sort-based shuffle that family serves is entirely
 * unaffected by the presence of this one. Like the rest of the streaming family it is pure data
 * plus codec: it carries no logging and no dependency on Spark core, and an unacceptable frame is
 * rejected here with a plain {@link IllegalArgumentException} that the caller, which owns the
 * surrounding context, turns into a protocol-level failure. Instances are immutable once
 * constructed and are therefore safe to hand between Netty event-loop threads and task threads
 * without further synchronisation.
 *
 * @since 4.2.0
 */
public class AckMessage extends StreamingShuffleMessage {

  /**
   * Number of bytes this message adds to the inherited header, namely the eight of the single
   * {@code long} that is its whole body.
   *
   * The count is named rather than written out twice so that {@link #encodedLength()} and the
   * length check in {@link #decode(ByteBuf)} cannot drift apart: the encoder promises exactly this
   * many bytes and the decoder requires exactly this many to be present.
   */
  private static final int BODY_ENCODED_LENGTH = 8;

  /**
   * The highest data-block sequence number the sending consumer has consumed for this shuffle
   * partition, acknowledging every lower one along with it.
   *
   * The value is reachable as a field, for parity with the message family in the parent package on
   * which the streaming family is modelled, and equally through {@link #consumerPosition()}, for
   * parity with the inherited header accessors, so that a call site may read every field of a
   * streaming message uniformly through a method if it prefers to.
   */
  public final long consumerPosition;

  /**
   * Creates an acknowledgement stamped with this build's protocol version. This is the constructor
   * a consumer uses, so that the current wire revision never has to be repeated at a construction
   * site.
   *
   * @param shuffleId identifier of the shuffle being acknowledged
   * @param partitionId identifier of the shuffle partition being acknowledged
   * @param sequenceNumber position of this acknowledgement within the stream of messages the
   *                       consumer sends, which is what distinguishes a fresh acknowledgement from
   *                       a stale one that overtook it
   * @param consumerPosition highest data-block sequence number consumed so far; a negative value
   *                         states that nothing has been consumed yet
   */
  public AckMessage(
      int shuffleId,
      int partitionId,
      long sequenceNumber,
      long consumerPosition) {
    super(shuffleId, partitionId, sequenceNumber);
    this.consumerPosition = consumerPosition;
  }

  /**
   * Creates an acknowledgement carrying an explicit protocol version, which is how a decoded
   * message keeps the version it actually arrived with instead of silently adopting this build's
   * own.
   *
   * @param protocolVersion the wire revision this message was encoded with
   * @param shuffleId identifier of the shuffle being acknowledged
   * @param partitionId identifier of the shuffle partition being acknowledged
   * @param sequenceNumber position of this acknowledgement within the consumer's message stream
   * @param consumerPosition highest data-block sequence number consumed so far
   */
  public AckMessage(
      byte protocolVersion,
      int shuffleId,
      int partitionId,
      long sequenceNumber,
      long consumerPosition) {
    super(protocolVersion, shuffleId, partitionId, sequenceNumber);
    this.consumerPosition = consumerPosition;
  }

  /**
   * Creates an acknowledgement from a header just read off the wire, which is the form
   * {@link #decode(ByteBuf)} uses: taking the header as one value rather than as four positional
   * arguments removes any chance of transposing two same-typed fields on the way in.
   *
   * @param header the decoded header, which must not be null
   * @param consumerPosition highest data-block sequence number consumed so far
   * @throws NullPointerException if header is null
   */
  public AckMessage(Header header, long consumerPosition) {
    super(header);
    this.consumerPosition = consumerPosition;
  }

  /**
   * The highest data-block sequence number the sending consumer has consumed for this shuffle
   * partition. A producer may release every retained block whose sequence number is not greater
   * than this value.
   *
   * @return the acknowledged position, or a negative value if nothing has been consumed yet
   */
  public long consumerPosition() {
    return consumerPosition;
  }

  @Override
  protected StreamingShuffleMessageType type() {
    return StreamingShuffleMessageType.ACK;
  }

  @Override
  public int hashCode() {
    // The header hash is folded in rather than recomputed here, so that two acknowledgements
    // differing only in a header field never collapse to one value.
    return Objects.hash(headerHashCode(), consumerPosition);
  }

  @Override
  public String toString() {
    return "AckMessage[" + headerToString() +
        ",consumerPosition=" + consumerPosition + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof AckMessage o) {
      return headerEquals(o)
        && consumerPosition == o.consumerPosition;
    }
    return false;
  }

  @Override
  public int encodedLength() {
    // Seventeen bytes of inherited header plus eight for consumerPosition, twenty-five in all,
    // written as a sum of named parts in the surrounding convention of four bytes per int and
    // eight per long so that it cannot drift from what encode(ByteBuf) actually writes.
    return HEADER_ENCODED_LENGTH + BODY_ENCODED_LENGTH;
  }

  @Override
  public void encode(ByteBuf buf) {
    // The header goes first, and is written by the parent class, so that its field order can never
    // diverge from the order readHeader expects to find it in.
    encodeHeader(buf);
    buf.writeLong(consumerPosition);
  }

  /**
   * Reads an acknowledgement from a buffer positioned at the first byte of the encoded body, that
   * is, immediately after the framing type discriminator has been consumed.
   *
   * @param buf the buffer to read from, which must not be null
   * @return the acknowledgement just consumed from the buffer
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if the encoded message is truncated, whether in its header or
   *         in its body
   */
  public static AckMessage decode(ByteBuf buf) {
    // Read in exactly the order encode wrote: the shared header first, then this message's own
    // field. The body length is checked rather than asserted because these bytes come from a
    // remote peer, so the failure has to be reported identically in production and under test.
    Header header = readHeader(buf);
    if (buf.readableBytes() < BODY_ENCODED_LENGTH) {
      throw new IllegalArgumentException("Truncated streaming shuffle acknowledgement: expected " +
        BODY_ENCODED_LENGTH + " byte(s) of body but only " + buf.readableBytes() + " remain");
    }
    long consumerPosition = buf.readLong();
    return new AckMessage(header, consumerPosition);
  }
}
