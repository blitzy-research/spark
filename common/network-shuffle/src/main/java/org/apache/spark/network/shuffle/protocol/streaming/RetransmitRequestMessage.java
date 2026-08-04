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
 * Asks a streaming shuffle producer to replay one data block that the consumer could not use.
 *
 * A consumer sends this message when a block fails its CRC32C verification on receipt, or when the
 * sequence numbers it has seen reveal a gap. The request names a single position within one
 * partition's stream: {@code sequenceNumber}, inherited from the header, is that position, and
 * {@link #firstSequenceNumber()}, {@link #lastSequenceNumber()} and {@link #contains(long)} read it
 * back in the vocabulary a producer servicing a replay uses. A consumer repairing several
 * positions sends one request apiece, each carrying its own budget and its own backoff, which is
 * also what bounds the work a peer can ask a producer to do: there is no range for it to name and
 * so no window for it to inflate.
 *
 * Scope: the unacknowledged window. Retransmission is bounded to the blocks the producer has not
 * yet seen acknowledged, and that bound is a contract rather than a convenience. A producer frees a
 * buffered block as soon as the consumer acknowledges having consumed past it, which is exactly
 * what keeps the producer's memory inside its budget; once freed, those bytes are gone, and no
 * message can conjure them back. A request for a position inside the unacknowledged window is
 * therefore serviceable, and the producer replays that block from memory or from spill. A request
 * that reaches back past the acknowledged position is not serviceable at all, and the receiving
 * side refuses it rather than pretending otherwise. Recovery in that case is not retransmission:
 * the consumer fails the fetch so that the upstream stage is recomputed, which restores correctness
 * by a route that needs none of the discarded bytes. That escalation belongs to the shuffle reader
 * in Spark core. This class carries the request and nothing more, and deliberately knows nothing
 * about how a non-serviceable one is answered.
 *
 * Wire format. The body is the producer id and nothing else, so a request encodes to the fixed
 * {@value StreamingShuffleMessage#CONTROL_MESSAGE_ENCODED_LENGTH} bytes of a control message and
 * occupies 26 once framed with its type discriminator. The position costs no byte of its own,
 * because the header already carries a sequence number and this message simply gives that field the
 * meaning of the position being asked for.
 *
 * <pre>
 *   +---------------------------------------+----------------------------------+
 *   | header, 17 bytes                      | mapId                            |
 *   | its sequenceNumber field is the       | long, 8 bytes                    |
 *   | position being requested              | the producing map task           |
 *   +---------------------------------------+----------------------------------+
 * </pre>
 *
 * Instances are immutable and validated on construction: the inherited {@code shuffleId},
 * {@code partitionId} and {@code sequenceNumber}, and the producer id that opens the body, are all
 * required to be non-negative by {@link StreamingShuffleMessage}, so a request can name neither an
 * impossible partition nor a negative position. Because the static {@link #decode(ByteBuf)} builds
 * its result through a public constructor, a request arriving from a peer is refused on exactly the
 * same terms as one built locally, and there is no route by which an invalid object can come into
 * existence. Like the rest of this family the class carries no logging and no dependency on Spark
 * core, being pure data plus codec, so an instance is safe to hand between Netty event-loop threads
 * and task threads without further synchronisation.
 *
 * @since 4.2.0
 */
@Private
public final class RetransmitRequestMessage extends StreamingShuffleMessage {

  /**
   * Bytes this message adds after the shared header: eight, for the producer id, which is the only
   * field of its own that it carries.
   *
   * The count is a named constant rather than a literal repeated at each use, so that {@link
   * #encodedLength()} and the body-length check in {@link #decode(ByteBuf)} cannot drift apart.
   * Added to {@link #HEADER_ENCODED_LENGTH} it yields the fixed {@value
   * StreamingShuffleMessage#CONTROL_MESSAGE_ENCODED_LENGTH} bytes of a control message.
   */
  private static final int BODY_ENCODED_LENGTH = PRODUCER_ID_ENCODED_LENGTH;

  /**
   * Blocks a single retransmission request names, which is exactly one.
   *
   * A request names one position rather than a window, because the position it asks for is the
   * header's own sequence number and a control message carries no field beyond the producer id.
   * That is a bound as well as a shape: a peer cannot name a range and so cannot ask a producer to
   * walk one, which is the denial of service an unbounded window would have written in well-formed
   * fields. A consumer repairing several positions sends one request apiece, each with its own
   * budget and its own backoff.
   */
  public static final long REQUESTED_BLOCKS = 1L;

  /**
   * Creates a request carrying an explicit protocol version, which is how a decoded message keeps
   * the version it actually arrived with instead of silently adopting this build's own.
   *
   * @param protocolVersion the wire revision this message was encoded with
   * @param shuffleId identifier of the shuffle whose block is being requested
   * @param mapId identifier of the map task whose output block is being requested
   * @param partitionId identifier of the shuffle partition whose block is being requested
   * @param sequenceNumber position of the block being requested
   */
  public RetransmitRequestMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber) {
    super(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
  }

  /**
   * Creates a request stamped with the protocol version this build speaks, which is how a consumer
   * raises one.
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
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, sequenceNumber);
  }

  /**
   * Creates a request from a header just read off the wire, together with the producer id that
   * opened the body, which is the form {@link #decode(ByteBuf)} uses.
   *
   * @param header the decoded header, which must not be null
   * @param mapId identifier of the map task whose output block is being requested
   * @throws NullPointerException if header is null
   */
  public RetransmitRequestMessage(Header header, long mapId) {
    super(header, mapId);
  }

  @Override
  protected StreamingShuffleMessageType type() {
    return StreamingShuffleMessageType.RETRANSMIT_REQUEST;
  }

  /**
   * The last position this request names, which is the only position it names.
   *
   * Retained alongside {@link #firstSequenceNumber()} so that a producer servicing a request reads
   * the same closed interval whatever a future revision of this message may express, and so that
   * the two ends can never be transposed: both are the header's sequence number.
   *
   * @return the position being requested
   */
  public long lastSequenceNumber() {
    return sequenceNumber();
  }

  /**
   * The first position this request names, stated as an alias of the header's sequence number so
   * that a caller reading a request never has to know which field carries the window's lower bound.
   *
   * @return the position being requested
   */
  public long firstSequenceNumber() {
    return sequenceNumber();
  }

  /**
   * Whether the candidate position is the one this request names.
   *
   * Offered so that the producer's replay path and this type agree on the membership rule rather
   * than each writing its own comparison.
   *
   * @param candidateSequenceNumber the position to test
   * @return true when the request names exactly that position
   */
  public boolean contains(long candidateSequenceNumber) {
    return candidateSequenceNumber == sequenceNumber();
  }

  /**
   * Number of blocks this request asks to have replayed, which is always {@link #REQUESTED_BLOCKS}.
   *
   * @return one
   */
  public long blockCount() {
    return REQUESTED_BLOCKS;
  }

  @Override
  public int hashCode() {
    // The header hash is the whole identity of this message, the requested position included,
    // because the position is a header field rather than a field of this class.
    return headerHashCode();
  }

  @Override
  public String toString() {
    return "RetransmitRequestMessage[" + headerToString() + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RetransmitRequestMessage o) {
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
    // The header always goes first, and it is the header that carries the position being requested;
    // the producer id follows it, as it does in every message of this family.
    encodeHeader(buf);
    encodeProducerId(buf);
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
   * specified size, or if its header carries a negative shuffle id, partition id or sequence number
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
    return new RetransmitRequestMessage(header, mapId);
  }
}
