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
 * A consumer's acknowledgement of how far it has consumed a streaming shuffle partition, which is
 * what lets the producer release the memory holding everything up to that point.
 *
 * Streaming shuffle pipelines map output straight from a producer executor to a consumer executor
 * while the map stage is still running, so the producer necessarily retains every block it has sent
 * but that the consumer has not yet confirmed: those are the blocks it may still be asked to
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
 * the message is fixed size: {@value StreamingShuffleMessage#HEADER_ENCODED_LENGTH} bytes of header
 * plus eight of body, and one further byte for the type discriminator once framed by {@link
 * StreamingShuffleMessage#toByteBuffer()}.
 *
 * <pre>
 *   +--------+-----------------+-----------+-------------+----------------+------------------+
 *   | type   | protocolVersion | shuffleId | partitionId | sequenceNumber | consumerPosition |
 *   | 1 byte | byte, 1         | int, 4    | int, 4      | long, 8        | long, 8          |
 *   +--------+-----------------+-----------+-------------+----------------+------------------+
 *   framing prefix, then the inherited 17-byte header, then this message's 8-byte body
 * </pre>
 *
 * <b>The position's domain.</b> A consumer that has consumed nothing yet must still be able to say
 * so, and since sequence numbers are counted from zero it cannot say it with a non-negative number.
 * It says it with exactly one value, {@link #NOTHING_CONSUMED}, and every other negative number is
 * refused. Admitting the whole negative range instead -- as an earlier revision of this class did
 * -- would have handed a peer an unbounded supply of positions that pass validation and then flow
 * into the producer's buffer-reclamation arithmetic, where a value like {@link Long#MIN_VALUE} is
 * not a harmless small number but one that inverts comparisons and overflows window calculations.
 * One sentinel expresses the only thing the negative range was ever needed for, and closes the
 * rest.
 *
 * The sentinel is confined to this body field: the inherited header's {@code shuffleId},
 * {@code partitionId} and {@code sequenceNumber} are all required to be non-negative and are
 * rejected centrally by {@link StreamingShuffleMessage}, on construction and on decode alike, so a
 * negative position sentinel never becomes a licence for a negative routing identity. What is also
 * checked is the length of the encoded body, because those bytes arrive from a remote peer and a
 * truncated frame has to be reported the same way in production as it is under test.
 *
 * The length of the encoded body is checked too, and exactly: those bytes arrive from a remote
 * peer, so a truncated frame has to be reported the same way in production as it is under test, and
 * a frame with bytes to spare is a framing error rather than something to ignore.
 *
 * This type is internal to Spark. It neither extends nor is registered with {@link
 * org.apache.spark.network.shuffle.protocol.BlockTransferMessage}, the family it is modelled on
 * rather than joined to, so the sort-based shuffle that family serves is entirely unaffected by the
 * presence of this one. Like the rest of the streaming family it is pure data plus codec: it
 * carries no logging and no dependency on Spark core, and an unacceptable frame is rejected here
 * with a plain {@link IllegalArgumentException} that the caller, which owns the surrounding
 * context, turns into a protocol-level failure. Instances are immutable once constructed and are
 * therefore safe to hand between Netty event-loop threads and task threads without further
 * synchronisation.
 *
 * @since 4.2.0
 */
@Private
public final class AckMessage extends StreamingShuffleMessage {

  /**
   * The one position value that states no block has been consumed yet.
   *
   * Sequence numbers start at zero, so "nothing consumed" cannot be spelled with a non-negative
   * number, and this sentinel is how it is spelled instead. It is the <em>only</em> negative value
   * a well-formed acknowledgement may carry: {@link #consumerPosition} is validated against it, so
   * a peer cannot supply an arbitrary negative number that would then be used in the producer's
   * reclamation arithmetic.
   */
  public static final long NOTHING_CONSUMED = -1L;

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
   * @param consumerPosition highest data-block sequence number consumed so far, or
   *                         {@link #NOTHING_CONSUMED} if nothing has been consumed yet
   */
  public AckMessage(
      int shuffleId,
      int partitionId,
      long sequenceNumber,
      long consumerPosition) {
    super(shuffleId, partitionId, sequenceNumber);
    this.consumerPosition = checkConsumerPosition(consumerPosition);
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
    this.consumerPosition = checkConsumerPosition(consumerPosition);
  }

  /**
   * Creates an acknowledgement from a header just read off the wire, which is the form {@link
   * #decode(ByteBuf)} uses: taking the header as one value rather than as four positional arguments
   * removes any chance of transposing two same-typed fields on the way in.
   *
   * @param header the decoded header, which must not be null
   * @param consumerPosition highest data-block sequence number consumed so far
   * @throws NullPointerException if header is null
   */
  public AckMessage(Header header, long consumerPosition) {
    super(header);
    this.consumerPosition = checkConsumerPosition(consumerPosition);
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
    // written as a sum of named parts in the surrounding convention of four bytes per int and eight
    // per long so that it cannot drift from what encode(ByteBuf) actually writes.
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
   * It is package-private on purpose: {@code StreamingShuffleMessage.Decoder.fromByteBuffer} is the
   * only entry point a peer's bytes may take into the protocol, and that holds only if a caller
   * outside this package cannot reach a concrete decoder and skip the checks the central decoder
   * applies to the frame as a whole.
   *
   * @param buf the buffer to read from, which must not be null
   * @return the acknowledgement just consumed from the buffer
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if the encoded message is truncated, whether in its header or
   *         in its body, if its header carries a negative shuffle id, partition id or sequence
   *         number, or if the position lies outside its domain
   */
  static AckMessage decode(ByteBuf buf) {
    // Read in exactly the order encode wrote: the shared header first, then this message's own
    // field. readHeader validates the version and the header's domains; the body length is checked
    // here rather than asserted because these bytes come from a remote peer, so the failure has to
    // be reported identically in production and under test.
    Header header = readHeader(buf);
    // Exactly, not at least: a shortfall is a truncated frame and a surplus is content the codec
    // would never examine, and tolerating the latter would let a peer append bytes that survive the
    // message boundary unparsed.
    if (buf.readableBytes() != BODY_ENCODED_LENGTH) {
      throw new IllegalArgumentException("Malformed streaming shuffle acknowledgement: expected " +
        "exactly " + BODY_ENCODED_LENGTH + " byte(s) of body but " + buf.readableBytes() +
        " remain");
    }
    long consumerPosition = buf.readLong();
    return new AckMessage(header, consumerPosition);
  }

  /**
   * Rejects a position outside its legitimate domain, returning it unchanged so that it can be
   * assigned straight to the field.
   *
   * The domain is {@link #NOTHING_CONSUMED} together with every non-negative sequence number. This
   * runs on every construction path, including {@link #decode(ByteBuf)}, so an acknowledgement that
   * exists at all carries a position the producer can safely do arithmetic with -- which is the
   * whole point of validating here rather than at each site that consumes the value.
   *
   * @param consumerPosition the candidate position
   * @return consumerPosition, unchanged
   * @throws IllegalArgumentException if the position is negative and is not
   *                                 {@link #NOTHING_CONSUMED}
   */
  private static long checkConsumerPosition(long consumerPosition) {
    if (consumerPosition < 0L && consumerPosition != NOTHING_CONSUMED) {
      throw new IllegalArgumentException("Streaming shuffle acknowledgement carries an invalid " +
        "consumerPosition: " + consumerPosition + " is negative but is not the " +
        NOTHING_CONSUMED + " sentinel that states nothing has been consumed");
    }
    return consumerPosition;
  }
}
