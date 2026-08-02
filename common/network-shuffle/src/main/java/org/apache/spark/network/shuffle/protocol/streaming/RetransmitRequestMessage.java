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
 * Asks a streaming shuffle producer to replay a contiguous run of data blocks that the consumer
 * could not use.
 *
 * A consumer sends this message when a block fails its CRC32C verification on receipt, or when the
 * sequence numbers it has seen reveal a gap. The request names a closed interval of sequence
 * numbers within one partition's stream: {@code sequenceNumber}, inherited from the header, is the
 * inclusive lower bound of that interval, and {@link #lastSequenceNumber()} is its inclusive upper
 * bound. Both ends belong to the request, so a single block is asked for by making the two equal,
 * and {@link #blockCount()} is consequently never zero. The interval is written {@code
 * [sequenceNumber(), lastSequenceNumber()]} throughout, {@link #firstSequenceNumber()} names its
 * lower bound in the vocabulary of the window, and {@link #contains(long)} answers whether a
 * particular block falls inside it.
 *
 * Scope: the unacknowledged window. Retransmission is bounded to the blocks the producer has not
 * yet seen acknowledged, and that bound is a contract rather than a convenience. A producer frees a
 * buffered block as soon as the consumer acknowledges having consumed past it, which is exactly
 * what keeps the producer's memory inside its budget; once freed, those bytes are gone, and no
 * message can conjure them back. A request whose interval lies within the unacknowledged window is
 * therefore serviceable, and the producer replays those blocks from memory or from spill. A request
 * that reaches back past the acknowledged position is not serviceable at all, and the receiving
 * side refuses it rather than pretending otherwise. Recovery in that case is not retransmission:
 * the consumer fails the fetch so that the upstream stage is recomputed, which restores correctness
 * by a route that needs none of the discarded bytes. That escalation belongs to the shuffle reader
 * in Spark core. This class carries the request and nothing more, and deliberately knows nothing
 * about how a non-serviceable one is answered.
 *
 * Wire format. The body is the shared header followed by a single {@code long}, so a request
 * encodes to {@link #HEADER_ENCODED_LENGTH} plus eight bytes, that is 33 bytes, and occupies 34
 * once framed with its type discriminator. The lower bound costs no byte of its own, because the
 * header already carries a sequence number and this message simply gives that field the meaning of
 * a lower bound; reusing it is also what makes the two bounds impossible to transpose on the wire.
 *
 * <pre>
 *   +---------------------------------------+----------------------------------+
 *   | header, 25 bytes                      | lastSequenceNumber               |
 *   | its sequenceNumber field is the       | long, 8 bytes                    |
 *   | inclusive LOWER bound of the window   | inclusive UPPER bound            |
 *   +---------------------------------------+----------------------------------+
 * </pre>
 *
 * Instances are immutable and validated on construction: an interval whose upper bound falls below
 * its lower bound is rejected with {@link IllegalArgumentException}, and the inherited
 * {@code shuffleId}, {@code mapId}, {@code partitionId} and {@code sequenceNumber} -- the last of
 * which is this
 * window's lower bound -- are all required to be non-negative by
 * {@link StreamingShuffleMessage}, so a request can name neither an impossible partition nor a
 * negative window. Because the static
 * {@link #decode(ByteBuf)} builds its result through a public constructor, an inverted interval
 * arriving from a peer is refused on exactly the same terms as one built locally, and there is no
 * route by which such an object can come into existence. Like the rest of this family the class
 * carries no logging and no dependency on Spark core, being pure data plus codec, so an instance is
 * safe to hand between Netty event-loop threads and task threads without further synchronisation.
 *
 * @since 4.2.0
 */
@Private
public final class RetransmitRequestMessage extends StreamingShuffleMessage {

  /**
   * Bytes this message adds after the shared header: eight, for the inclusive upper bound, which is
   * the only field of its own that it carries.
   *
   * The count is a named constant rather than a literal repeated at each use, so that {@link
   * #encodedLength()} and the body-length check in {@link #decode(ByteBuf)} cannot drift apart.
   * Added to {@link #HEADER_ENCODED_LENGTH} it yields an encoded length of 33 bytes.
   */
  private static final int BODY_ENCODED_LENGTH = 8;

  /**
   * Most blocks a single retransmission request may name.
   *
   * A request names a closed window of sequence numbers, and without a ceiling on its width a peer
   * could name every number a {@code long} can express. The producer answering such a request would
   * be asked to walk a window of 2^63 positions, which is a denial of service written in five
   * well-formed fields -- and it is worth noting that {@link #blockCount()} had to saturate at
   * {@link Long#MAX_VALUE} precisely because the width was unbounded. Bounding the width removes
   * the need for that saturation to ever come into play.
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
   * @throws IllegalArgumentException if lastSequenceNumber is below sequenceNumber
   */
  public RetransmitRequestMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long lastSequenceNumber) {
    super(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
    // The check follows the super call because Java forbids any statement before it at this
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
   * Creates a request stamped with this build's protocol version. This is the constructor a
   * consumer uses on detecting corruption or a gap, so that {@link #CURRENT_PROTOCOL_VERSION} need
   * not be repeated at every construction site.
   *
   * @param shuffleId identifier of the shuffle whose blocks are being requested
   * @param mapId identifier of the map task whose output blocks are being requested
   * @param partitionId identifier of the shuffle partition whose blocks are being requested
   * @param sequenceNumber inclusive lower bound of the requested window
   * @param lastSequenceNumber inclusive upper bound of the requested window, which must not fall
   *                           below {@code sequenceNumber}; equal bounds request a single block
   * @throws IllegalArgumentException if lastSequenceNumber is below sequenceNumber
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
   * Creates a request from a header the base class has just read off the wire. This is the
   * constructor {@link #decode(ByteBuf)} uses: passing the header as one value rather than as five
   * positional arguments removes any chance of transposing {@code shuffleId} and {@code
   * partitionId} on the way in.
   *
   * @param header the decoded header, whose {@code sequenceNumber} is the inclusive lower bound of
   *               the requested window; must not be null
   * @param lastSequenceNumber inclusive upper bound of the requested window, which must not fall
   *                           below the header's {@code sequenceNumber}
   * @throws NullPointerException if header is null
   * @throws IllegalArgumentException if lastSequenceNumber is below the header's sequenceNumber
   */
  public RetransmitRequestMessage(Header header, long lastSequenceNumber) {
    // requireNonNull sits inside the argument list because no statement may precede a constructor
    // delegation; this is the same idiom the base class uses for its own header constructor.
    this(Objects.requireNonNull(header, "header").protocolVersion(), header.shuffleId(),
      header.mapId(), header.partitionId(), header.sequenceNumber(), lastSequenceNumber);
  }

  @Override
  protected StreamingShuffleMessageType type() {
    return StreamingShuffleMessageType.RETRANSMIT_REQUEST;
  }

  /**
   * Inclusive upper bound of the requested window: the sequence number of the last block the
   * producer is being asked to replay.
   *
   * @return the last sequence number in the requested window, never below
   *         {@link #firstSequenceNumber()}
   */
  public long lastSequenceNumber() {
    return lastSequenceNumber;
  }

  /**
   * Inclusive lower bound of the requested window: the sequence number of the first block the
   * producer is being asked to replay.
   *
   * This reads the header's own {@code sequenceNumber} under the name the window semantics give it.
   * The alias exists so that a call site reads as a pair of bounds instead of leaving the reader to
   * remember which header field plays the role of the lower one. It reads the same field, adds no
   * state, and costs no byte on the wire.
   *
   * @return the first sequence number in the requested window
   */
  public long firstSequenceNumber() {
    return sequenceNumber();
  }

  /**
   * Whether a given block falls inside the requested window.
   *
   * The test is written as two bounds comparisons rather than as arithmetic on their difference, so
   * that it is exact for every pair of {@code long} values and cannot overflow.
   *
   * @param candidateSequenceNumber the sequence number of a block to test
   * @return true if the block lies within {@code [firstSequenceNumber(), lastSequenceNumber()]}
   */
  public boolean contains(long candidateSequenceNumber) {
    return candidateSequenceNumber >= sequenceNumber()
      && candidateSequenceNumber <= lastSequenceNumber;
  }

  /**
   * Number of blocks the requested window spans, counting both of its ends.
   *
   * The result lies in {@code [1, }{@link #MAX_REQUESTED_BLOCKS}{@code ]}: the window is inclusive
   * at both ends, the constructor refuses an upper bound below the lower one, and it also refuses a
   * span wider than the ceiling. The saturation below is therefore unreachable from any window that
   * exists; it is kept as the one place where an arithmetic overflow could otherwise turn a
   * nonsense window into a small, plausible-looking count, so that a future relaxation of the
   * ceiling cannot reintroduce that failure silently.
   *
   * @return the count of sequence numbers in the requested window, never below one and never above
   *         {@link #MAX_REQUESTED_BLOCKS}
   */
  public long blockCount() {
    long span = lastSequenceNumber - sequenceNumber();
    // A negative span can only mean the subtraction overflowed, since the constructor guarantees
    // the bounds are ordered; a span already at Long.MAX_VALUE would overflow on the increment.
    return (span < 0 || span == Long.MAX_VALUE) ? Long.MAX_VALUE : span + 1;
  }

  @Override
  public int hashCode() {
    return Objects.hash(headerHashCode(), lastSequenceNumber);
  }

  @Override
  public String toString() {
    return "RetransmitRequestMessage[" + headerToString() +
        ",lastSequenceNumber=" + lastSequenceNumber + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RetransmitRequestMessage o) {
      return headerEquals(o)
        && lastSequenceNumber == o.lastSequenceNumber;
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return HEADER_ENCODED_LENGTH + BODY_ENCODED_LENGTH;
  }

  @Override
  public void encode(ByteBuf buf) {
    // The header always goes first, and it is the header that carries the window's lower bound.
    encodeHeader(buf);
    buf.writeLong(lastSequenceNumber);
  }

  /**
   * Reads a request back off the wire, header first and in exactly the order {@link
   * #encode(ByteBuf)} wrote it.
   *
   * The remaining length is checked rather than asserted, because these bytes arrive from a remote
   * peer and a truncated frame must be reported the same way in production as it is under test. The
   * result is built through a public constructor, so a peer that sends an inverted window is
   * refused on exactly the same terms as a local caller that tries to build one.
   *
   * @param buf the buffer to read from, positioned at the first header byte
   * @return the decoded request
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if the frame is truncated, if the window it carries is
   *         inverted, or if its header carries a negative shuffle id, partition id or sequence
   *         number
   */
  static RetransmitRequestMessage decode(ByteBuf buf) {
    Header header = readHeader(buf);
    // Exactly, not at least: a surplus is content the codec would never examine, and tolerating it
    // would let a peer append bytes that survive the message boundary unparsed.
    if (buf.readableBytes() != BODY_ENCODED_LENGTH) {
      throw new IllegalArgumentException("Malformed retransmission request: expected exactly " +
        BODY_ENCODED_LENGTH + " byte(s) of body but " + buf.readableBytes() + " remain");
    }
    long lastSequenceNumber = buf.readLong();
    return new RetransmitRequestMessage(header, lastSequenceNumber);
  }
}
