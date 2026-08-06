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
 * Asks a streaming shuffle producer to replay a contiguous run of data blocks the consumer could
 * not use.
 *
 * A consumer sends this message when a block fails its CRC32C verification on receipt, or when the
 * sequence numbers it has seen reveal a gap. The request names one closed interval of positions
 * within one partition's stream: {@code sequenceNumber}, inherited from the header, is its
 * inclusive lower bound, {@link #lastSequenceNumber()} is its inclusive upper bound, {@link
 * #firstSequenceNumber()} names the lower bound in the vocabulary of a window, and {@link
 * #contains(long)} answers whether one position falls inside it. Equal bounds request a single
 * block, so {@link #blockCount()} is never zero.
 *
 * <b>Why one frame names the whole run rather than one frame per position.</b> A repair window is
 * one repair, and the producer's replay budget and backoff are charged per request: five attempts
 * spaced by a pause that grows exponentially from one second. Split across a frame per position,
 * the first frame of a two-position gap would arm the stream's backoff and every sibling would be
 * deferred behind it, so the run would spend the whole budget re-requesting its first position and
 * escalate to a stage recomputation while the producer still held every byte it was asked for. The
 * interval is therefore a field of the request, which makes the repair of a run atomic: one frame,
 * one serviceability decision over the whole range, one attempt charged, one backoff armed.
 *
 * The width a peer may name is bounded by {@link #MAX_REQUESTED_BLOCKS}, which is what keeps the
 * range from becoming a window a peer could inflate.
 *
 * Scope: the unacknowledged window. Retransmission is bounded to the blocks the producer has not
 * yet seen acknowledged, and that bound is a contract rather than a convenience. A producer frees a
 * buffered block as soon as the consumer acknowledges having consumed past it, which is exactly
 * what keeps the producer's memory inside its budget; once freed, those bytes are gone, and no
 * message can conjure them back. A request whose whole interval lies inside the unacknowledged
 * window is therefore serviceable, and the producer replays those blocks from memory or from spill.
 * A request that reaches back past the acknowledged position is not serviceable at all, and the
 * receiving side refuses the whole of it rather than answering it in part: a consumer splicing a
 * partial replay into its input would reorder the partition. Recovery in that case is not
 * retransmission:
 * the consumer fails the fetch so that the upstream stage is recomputed, which restores correctness
 * by a route that needs none of the discarded bytes. That escalation belongs to the shuffle reader
 * in Spark core. This class carries the request and nothing more, and deliberately knows nothing
 * about how a non-serviceable one is answered.
 *
 * Wire format. The body is the producer id followed by the interval's inclusive upper bound, so a
 * request encodes to {@value StreamingShuffleMessage#HEADER_ENCODED_LENGTH} plus sixteen bytes,
 * that is 33, and occupies 34 once framed with its type discriminator. It is consequently the one
 * control message that is wider than the fixed {@value
 * StreamingShuffleMessage#CONTROL_MESSAGE_ENCODED_LENGTH} bytes the other three share -- still a
 * fixed width, and still a width no peer chooses. The lower bound costs no byte of its own, because
 * the header already carries a sequence number and this message simply gives that field the meaning
 * of a lower bound; reusing it is also what makes the two bounds impossible to transpose on the
 * wire.
 *
 * <pre>
 *   +---------------------------------------+-------------------+-------------------------+
 *   | header, 17 bytes                      | mapId             | lastSequenceNumber      |
 *   | its sequenceNumber field is the       | long, 8 bytes     | long, 8 bytes           |
 *   | inclusive LOWER bound of the window   | the producing map | inclusive UPPER bound   |
 *   +---------------------------------------+-------------------+-------------------------+
 * </pre>
 *
 * Instances are immutable and validated on construction: an interval whose upper bound falls below
 * its lower bound is rejected, an upper bound that is negative is rejected, an interval wider than
 * {@link #MAX_REQUESTED_BLOCKS} is rejected, and the inherited {@code shuffleId},
 * {@code partitionId} and {@code sequenceNumber} -- the last of which is this window's lower bound
 * -- and the producer id that opens the body are all required to be non-negative by {@link
 * StreamingShuffleMessage}, so a request can name neither an impossible partition nor a negative
 * position. Because the static {@link #decode(ByteBuf)} builds its result through a public
 * constructor, a request arriving from a peer is refused on exactly the same terms as one built
 * locally, and there is no route by which an invalid object can come into existence. Like the rest
 * of this family the class carries no logging and no dependency on Spark core, being pure data plus
 * codec, so an instance is safe to hand between Netty event-loop threads and task threads without
 * further synchronisation.
 *
 * @since 4.2.0
 */
@Private
public final class RetransmitRequestMessage extends StreamingShuffleMessage {

  /**
   * Bytes this message adds after the shared header: sixteen, being the producer id every message
   * of this family carries followed by this request's own inclusive upper bound.
   *
   * The count is a named constant rather than a literal repeated at each use, so that {@link
   * #encodedLength()} and the body-length check in {@link #decode(ByteBuf)} cannot drift apart.
   */
  private static final int BODY_ENCODED_LENGTH = PRODUCER_ID_ENCODED_LENGTH + 8;

  /**
   * Most blocks a single retransmission request may name.
   *
   * A request names a closed window of sequence numbers, and without a ceiling on its width a peer
   * could name every number a {@code long} can express. The producer answering such a request would
   * be asked to walk a window of 2^63 positions, which is a denial of service written in
   * well-formed fields.
   *
   * The value is deliberately generous rather than tight: at the two-mebibyte block cap, 4096
   * blocks is eight gibibytes of retransmission, far more than a producer can be holding when
   * buffers are capped at half of executor memory. It therefore never refuses a request a real
   * consumer would make, while refusing every request no real consumer could.
   */
  public static final long MAX_REQUESTED_BLOCKS = 4096L;

  private final long lastSequenceNumber;

  /**
   * Creates a request carrying an explicit protocol version, which is how a decoded message keeps
   * the version it actually arrived with instead of silently adopting this build's own.
   *
   * This is the constructor every other route funnels into, and therefore the single place the
   * window is validated.
   *
   * @param protocolVersion the wire revision this message was encoded with
   * @param shuffleId identifier of the shuffle whose blocks are being requested
   * @param mapId identifier of the map task whose output blocks are being requested
   * @param partitionId identifier of the shuffle partition whose blocks are being requested
   * @param sequenceNumber inclusive lower bound of the requested window
   * @param lastSequenceNumber inclusive upper bound of the requested window, which must not fall
   *                           below {@code sequenceNumber}; equal bounds request a single block
   * @throws IllegalArgumentException if the window is inverted, negative or wider than {@link
   *                                 #MAX_REQUESTED_BLOCKS}
   */
  public RetransmitRequestMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long lastSequenceNumber) {
    super(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
    // The checks follow the super call because Java forbids any statement before it at this
    // language level. Nothing observable has happened by then: the base constructor only assigns
    // header fields, so a rejected request is simply discarded before anyone can hold a reference.
    if (lastSequenceNumber < sequenceNumber) {
      throw new IllegalArgumentException("Retransmission window is inverted: upper bound " +
        lastSequenceNumber + " is below lower bound " + sequenceNumber + " for shuffle " +
        shuffleId + " partition " + partitionId);
    }
    // Both bounds are positions counted from zero. readHeader already refuses a negative lower
    // bound on every decode path, and the inversion check above then makes the upper bound
    // non-negative too, but a locally constructed request reaches this constructor without passing
    // through readHeader, so the bound is checked here as well rather than assumed.
    if (lastSequenceNumber < 0L) {
      throw new IllegalArgumentException("Retransmission window upper bound cannot be negative: " +
        lastSequenceNumber + " for shuffle " + shuffleId + " partition " + partitionId);
    }
    // Compared as a span rather than as a count. Both bounds are non-negative and ordered by this
    // point, so their difference always lands in [0, Long.MAX_VALUE] and cannot overflow -- but
    // adding one to it can, and does for exactly one input pair: an upper bound of Long.MAX_VALUE
    // against a lower bound of zero wraps the count to Long.MIN_VALUE, which would slip past a
    // greater-than test and admit the widest possible window. Since the window includes both of its
    // ends, a span of MAX_REQUESTED_BLOCKS - 1 is the widest legal one.
    long span = lastSequenceNumber - sequenceNumber;
    if (span > MAX_REQUESTED_BLOCKS - 1L) {
      throw new IllegalArgumentException("Retransmission window spans more block(s) than the " +
        "maximum of " + MAX_REQUESTED_BLOCKS + ": bounds [" + sequenceNumber + ", " +
        lastSequenceNumber + "] for shuffle " + shuffleId + " partition " + partitionId);
    }
    this.lastSequenceNumber = lastSequenceNumber;
  }

  /**
   * Creates a request stamped with the protocol version this build speaks, which is how a consumer
   * raises one.
   *
   * @param shuffleId identifier of the shuffle whose blocks are being requested
   * @param mapId identifier of the map task whose output blocks are being requested
   * @param partitionId identifier of the shuffle partition whose blocks are being requested
   * @param sequenceNumber inclusive lower bound of the requested window
   * @param lastSequenceNumber inclusive upper bound of the requested window
   * @throws IllegalArgumentException if the window is inverted, negative or too wide
   */
  public RetransmitRequestMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long lastSequenceNumber) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, sequenceNumber,
      lastSequenceNumber);
  }

  /**
   * Creates a request for exactly one position, which is the ordinary shape a corrupt block calls
   * for and is expressed by making the two bounds equal.
   *
   * @param shuffleId identifier of the shuffle whose block is being requested
   * @param mapId identifier of the map task whose output block is being requested
   * @param partitionId identifier of the shuffle partition whose block is being requested
   * @param sequenceNumber position of the block being requested
   */
  public RetransmitRequestMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, sequenceNumber, sequenceNumber);
  }

  /**
   * Creates a request from a header just read off the wire, together with the producer id and the
   * upper bound that followed it in the body, which is the form {@link #decode(ByteBuf)} uses.
   *
   * @param header the decoded header, which must not be null
   * @param mapId identifier of the map task whose output blocks are being requested
   * @param lastSequenceNumber inclusive upper bound of the requested window
   * @throws NullPointerException if header is null
   * @throws IllegalArgumentException if the window is inverted, negative or too wide
   */
  public RetransmitRequestMessage(Header header, long mapId, long lastSequenceNumber) {
    super(header, mapId);
    if (lastSequenceNumber < header.sequenceNumber()) {
      throw new IllegalArgumentException("Retransmission window is inverted: upper bound " +
        lastSequenceNumber + " is below lower bound " + header.sequenceNumber() +
        " for shuffle " + header.shuffleId() + " partition " + header.partitionId());
    }
    if (lastSequenceNumber - header.sequenceNumber() > MAX_REQUESTED_BLOCKS - 1L) {
      throw new IllegalArgumentException("Retransmission window spans more block(s) than the " +
        "maximum of " + MAX_REQUESTED_BLOCKS + ": bounds [" + header.sequenceNumber() + ", " +
        lastSequenceNumber + "] for shuffle " + header.shuffleId() + " partition " +
        header.partitionId());
    }
    this.lastSequenceNumber = lastSequenceNumber;
  }

  @Override
  protected StreamingShuffleMessageType type() {
    return StreamingShuffleMessageType.RETRANSMIT_REQUEST;
  }

  /**
   * The inclusive upper bound of the window this request names, which for a single-block request is
   * the same position as its lower bound.
   *
   * @return the last position being requested
   */
  public long lastSequenceNumber() {
    return lastSequenceNumber;
  }

  /**
   * The inclusive lower bound of the window, stated as an alias of the header's sequence number so
   * that a caller reading a request never has to know which field carries it.
   *
   * @return the first position being requested
   */
  public long firstSequenceNumber() {
    return sequenceNumber();
  }

  /**
   * Whether the candidate position falls inside the window this request names.
   *
   * Offered so that the producer's replay path and this type agree on the membership rule rather
   * than each writing its own comparison, and so that the rule is stated once as the closed
   * interval it is.
   *
   * @param candidateSequenceNumber the position to test
   * @return true when the request covers that position
   */
  public boolean contains(long candidateSequenceNumber) {
    return candidateSequenceNumber >= sequenceNumber() &&
      candidateSequenceNumber <= lastSequenceNumber;
  }

  /**
   * Number of blocks this request asks to have replayed, which is the width of its closed interval
   * and therefore never zero.
   *
   * The construction-time width check bounds this at {@link #MAX_REQUESTED_BLOCKS}, so the addition
   * below cannot overflow.
   *
   * @return blocks in the requested window
   */
  public long blockCount() {
    return lastSequenceNumber - sequenceNumber() + 1L;
  }

  @Override
  public int hashCode() {
    // The header hash carries the lower bound, because that is a header field; the upper bound is
    // this class's own field and is mixed in here so two windows sharing a lower bound are
    // distinguishable.
    return 31 * headerHashCode() + Long.hashCode(lastSequenceNumber);
  }

  @Override
  public String toString() {
    return "RetransmitRequestMessage[" + headerToString() +
      ", lastSequenceNumber=" + lastSequenceNumber + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RetransmitRequestMessage o) {
      return headerEquals(o) && lastSequenceNumber == o.lastSequenceNumber;
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return HEADER_ENCODED_LENGTH + BODY_ENCODED_LENGTH;
  }

  @Override
  public void encode(ByteBuf buf) {
    // The header always goes first, and it is the header that carries the window's lower bound; the
    // producer id follows it, as it does in every message of this family, and the upper bound comes
    // last.
    encodeHeader(buf);
    encodeProducerId(buf);
    buf.writeLong(lastSequenceNumber);
  }

  /**
   * Reads a request back off the wire, header first and in exactly the order {@link
   * #encode(ByteBuf)} wrote it.
   *
   * The remaining length is checked rather than asserted, because these bytes arrive from a remote
   * peer and a truncated frame must be reported the same way in production as it is under test. The
   * result is built through a public constructor, so a peer's request is refused on exactly the
   * same terms as one a local caller tries to build.
   *
   * @param buf the buffer to read from, positioned at the first header byte
   * @return the decoded request
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if the frame is truncated, if its body is not exactly the
   * specified size, if its header carries a negative shuffle id, partition id or sequence number,
   * or if the window it names is inverted or wider than {@link #MAX_REQUESTED_BLOCKS}
   */
  static RetransmitRequestMessage decode(ByteBuf buf) {
    Header header = readHeader(buf);
    // Exactly, not at least: a surplus is content the codec would never examine, and tolerating it
    // would let a peer append bytes that survive the message boundary unparsed.
    if (buf.readableBytes() != BODY_ENCODED_LENGTH) {
      throw new IllegalArgumentException("Malformed retransmission request: expected exactly " +
        BODY_ENCODED_LENGTH + " byte(s) of body but " + buf.readableBytes() + " remain");
    }
    long mapId = readProducerId(buf);
    long lastSequenceNumber = buf.readLong();
    return new RetransmitRequestMessage(header, mapId, lastSequenceNumber);
  }
}
