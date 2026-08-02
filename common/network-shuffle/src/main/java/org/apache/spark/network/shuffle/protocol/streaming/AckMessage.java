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
 * the producer frees the buffers holding the acknowledged blocks and resumes the egress it had
 * deferred while the consumer was behind. It resumes sending, not reading: the receive window is
 * governed solely by the consumer, which is the only side that toggles {@code autoRead}.
 *
 * Cumulative, not per block. The acknowledgement is cumulative in the manner of a TCP
 * acknowledgement: {@link #consumerPosition} names the highest data-block sequence number the
 * consumer has consumed and thereby acknowledges every lower one along with it. A consumer may
 * consequently acknowledge in batches rather than once per block, which is what keeps this control
 * traffic off the per-record path, and a lost acknowledgement costs nothing because the next one
 * supersedes it. The producer's reclamation rule is correspondingly simple: release every retained
 * block whose sequence number is not greater than the position just acknowledged.
 *
 * Two sequence-valued fields. The inherited {@link #sequenceNumber()} counts the acknowledgements
 * the consumer has sent, positioning this one within the consumer's own control-message stream;
 * {@code consumerPosition} is the value being acknowledged, a position in the stream of data blocks
 * travelling the other way. The two advance independently and neither can be derived from the
 * other.
 * Only {@code consumerPosition} is acted upon: the producer applies it monotonically and reclaims
 * every retained block at or below it, so a reordered or duplicated acknowledgement is absorbed
 * without consulting the header, and the counter is carried for diagnostics and for a future
 * revision that wants to validate acknowledgement ordering explicitly.
 *
 * Wire layout. The body is a single {@code long} placed immediately after the inherited header, so
 * the message is fixed size: {@value StreamingShuffleMessage#HEADER_ENCODED_LENGTH} bytes of header
 * plus eight of body, and one further byte for the type discriminator once framed by {@link
 * StreamingShuffleMessage#toByteBuffer()}.
 *
 * <pre>
 *   +--------+-----------------+-----------+-----------+-------------+----------------+---------+
 *   | type   | protocolVersion | shuffleId | mapId     | partitionId | sequenceNumber | consumer |
 *   |        |                 |           |           |             |                | Position |
 *   | 1 byte | byte, 1         | int, 4    | long, 8   | int, 4      | long, 8        | long, 8  |
 *   +--------+-----------------+-----------+-----------+-------------+----------------+---------+
 *   framing prefix, then the inherited 25-byte header, then this message's 8-byte body
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
 * {@code mapId}, {@code partitionId} and {@code sequenceNumber} are all required to be
 * non-negative and are
 * rejected centrally by {@link StreamingShuffleMessage}, on construction and on decode alike, so a
 * negative position sentinel never becomes a licence for a negative routing identity. What is also
 * checked is the length of the encoded body, because those bytes arrive from a remote peer and a
 * truncated frame has to be reported the same way in production as it is under test.
 *
 * <b>What this class cannot decide, and the contract that covers it.</b> The syntactic domain above
 * still admits every non-negative {@code long} up to {@code Long.MAX_VALUE}, and it must: this
 * class holds no stream state, so it cannot know how many blocks the producer has sent. That
 * makes an acknowledgement <em>syntactically</em> valid and <em>semantically</em> unverified, and
 * the gap is not academic. A position above the highest sequence number the producer ever issued is
 * a forged acknowledgement of blocks that do not exist, and a producer that applies it releases its
 * entire retained window at once -- destroying exactly the bytes a genuine consumer would later ask
 * to have replayed. The same is true of the inherited control {@code sequenceNumber}: a stale or
 * replayed acknowledgement that overtakes a fresher one must not be allowed to rewind the window.
 *
 * A receiving producer is therefore <b>required</b> to establish both facts against its own
 * authoritative state <em>before</em> it mutates anything, and this class publishes the two
 * predicates so that every layer that touches an acknowledgement -- the channel handler, the
 * backpressure ledger and the spill manager -- tests the identical condition rather than each
 * writing its own comparison:
 *
 * <ul>
 *   <li>{@code acknowledgesWithin(long)} against the highest sequence number the producer has
 *       actually sent on this stream. A message that fails it is a protocol violation, not a
 *       tolerable oddity, and the channel carrying it is closed.</li>
 *   <li>{@code supersedes(long)} against the highest control sequence number already applied for
 *       this stream. A message that fails it is stale and is discarded without effect.</li>
 * </ul>
 *
 * Both are pure functions of this message and one caller-supplied bound, so they add no state here
 * and stay usable from a Netty event-loop thread.
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
   * @param mapId identifier of the map task whose output is being acknowledged
   * @param partitionId identifier of the shuffle partition being acknowledged
   * @param sequenceNumber count of the acknowledgements the consumer has sent, positioning this one
   *                       within the consumer's own control-message stream
   * @param consumerPosition highest data-block sequence number consumed so far, or
   *                         {@link #NOTHING_CONSUMED} if nothing has been consumed yet
   */
  public AckMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long consumerPosition) {
    super(shuffleId, mapId, partitionId, sequenceNumber);
    this.consumerPosition = checkConsumerPosition(consumerPosition);
  }

  /**
   * Creates an acknowledgement carrying an explicit protocol version, which is how a decoded
   * message keeps the version it actually arrived with instead of silently adopting this build's
   * own.
   *
   * @param protocolVersion the wire revision this message was encoded with
   * @param shuffleId identifier of the shuffle being acknowledged
   * @param mapId identifier of the map task whose output is being acknowledged
   * @param partitionId identifier of the shuffle partition being acknowledged
   * @param sequenceNumber count of the acknowledgements the consumer has sent
   * @param consumerPosition highest data-block sequence number consumed so far
   */
  public AckMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long consumerPosition) {
    super(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
    this.consumerPosition = checkConsumerPosition(consumerPosition);
  }

  /**
   * Creates an acknowledgement from a header just read off the wire, which is the form {@link
   * #decode(ByteBuf)} uses: taking the header as one value rather than as five positional arguments
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

  /**
   * Whether this acknowledgement stays inside the range of blocks the producer has actually sent,
   * and may therefore be applied to the retained window.
   *
   * This is the check that separates a legitimate acknowledgement from a forged one. Nothing in the
   * encoded form of the message can establish it, because the bound lives in the producer's own
   * stream state; so the producer supplies the bound and this method applies the comparison. A
   * consumer cannot acknowledge a block that was never issued, so a position above the highest
   * sequence number sent is a violation of the protocol rather than an optimistic guess, and a
   * producer that honoured it would drain every block it was holding for replay.
   *
   * {@link #NOTHING_CONSUMED} always passes: it acknowledges no block at all, so there is nothing
   * for it to overreach. A producer that has sent nothing yet passes
   * {@link #NOTHING_CONSUMED} as the bound, which then admits only that same sentinel -- exactly
   * right, because no position can be acknowledged before a position exists.
   *
   * @param highestSentSequenceNumber the highest data-block sequence number the producer has issued
   *                                  on this stream, or {@link #NOTHING_CONSUMED} if it has issued
   *                                  none
   * @return true if the acknowledged position lies at or below that bound
   */
  public boolean acknowledgesWithin(long highestSentSequenceNumber) {
    return consumerPosition == NOTHING_CONSUMED || consumerPosition <= highestSentSequenceNumber;
  }

  /**
   * Whether this acknowledgement is newer than the last one already applied for its stream.
   *
   * The comparison is on the inherited control {@link #sequenceNumber()}, which numbers the
   * consumer's own outbound messages, and not on {@link #consumerPosition()}, which numbers the
   * data travelling the other way. Two acknowledgements may report the same consumed position while
   * being distinct messages, and a reordered pair may report positions in the wrong order, so
   * freshness has to be decided on the control sequence and applied before the position is used.
   * Strict inequality makes the test idempotent: a duplicate delivery is refused, not reapplied.
   *
   * @param lastAppliedControlSequenceNumber highest control sequence number already applied for
   *                                         this stream, or a negative value if none has been
   * @return true if this message is newer and may be applied
   */
  public boolean supersedes(long lastAppliedControlSequenceNumber) {
    return sequenceNumber() > lastAppliedControlSequenceNumber;
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
    // Twenty-five bytes of inherited header plus eight for consumerPosition, thirty-three in all,
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
   *         in its body, if its header carries a negative shuffle id, map id, partition id or
   *         sequence number, or if the position lies outside its domain
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
