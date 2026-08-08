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
 * not use, the inherited sequence number being the run's first position and the body its inclusive
 * upper bound.
 *
 * A request is only ever answerable from the producer's retained unacknowledged window; a position
 * the producer has already reclaimed is recovered by failing the fetch and letting the unmodified
 * scheduler recompute the upstream stage instead. The run is capped at
 * {@value #MAX_REQUESTED_BLOCKS} blocks so that one frame cannot ask a producer for unbounded work.
 *
 * @since 4.2.0
 */
@Private
public final class RetransmitRequestMessage extends StreamingShuffleMessage {

  /** Bytes this message adds to the header: the producer id, then the inclusive upper bound. */
  private static final int BODY_ENCODED_LENGTH = PRODUCER_ID_ENCODED_LENGTH + 8;

  /** Most blocks a single retransmission request may name. */
  public static final long MAX_REQUESTED_BLOCKS = 4096L;

  private final long lastSequenceNumber;

  public RetransmitRequestMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long lastSequenceNumber) {
    super(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
    // After the super call, which Java requires first; the rejected message escapes to no caller.
    if (lastSequenceNumber < sequenceNumber) {
      throw new IllegalArgumentException("Retransmission window is inverted: upper bound " +
        lastSequenceNumber + " is below lower bound " + sequenceNumber + " for shuffle " +
        shuffleId + " partition " + partitionId);
    }
    // Both bounds are positions counted from zero.
    if (lastSequenceNumber < 0L) {
      throw new IllegalArgumentException("Retransmission window upper bound cannot be negative: " +
        lastSequenceNumber + " for shuffle " + shuffleId + " partition " + partitionId);
    }
    // Compared as a span rather than as a count.
    long span = lastSequenceNumber - sequenceNumber;
    if (span > MAX_REQUESTED_BLOCKS - 1L) {
      throw new IllegalArgumentException("Retransmission window spans more block(s) than the " +
        "maximum of " + MAX_REQUESTED_BLOCKS + ": bounds [" + sequenceNumber + ", " +
        lastSequenceNumber + "] for shuffle " + shuffleId + " partition " + partitionId);
    }
    this.lastSequenceNumber = lastSequenceNumber;
  }

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
   */
  public long lastSequenceNumber() {
    return lastSequenceNumber;
  }

  /**
   * The inclusive lower bound of the window, stated as an alias of the header's sequence number so
   * that a caller reading a request never has to know which field carries it.
   */
  public long firstSequenceNumber() {
    return sequenceNumber();
  }

  public boolean contains(long candidateSequenceNumber) {
    return candidateSequenceNumber >= sequenceNumber() &&
      candidateSequenceNumber <= lastSequenceNumber;
  }

  /**
   * Number of blocks this request asks to have replayed, which is the width of its closed interval
   * and therefore never zero.
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
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if the frame is truncated, if its body is not exactly the
   *     specified size, if its header carries a negative shuffle id, partition id or sequence
   *     number, or if the window it names is inverted or wider than {@link #MAX_REQUESTED_BLOCKS}
   */
  static RetransmitRequestMessage decode(ByteBuf buf) {
    Header header = readHeader(buf);
    // Exactly the body: a shortfall is a truncated frame and a surplus is unparsed content.
    if (buf.readableBytes() != BODY_ENCODED_LENGTH) {
      throw new IllegalArgumentException("Malformed retransmission request: expected exactly " +
        BODY_ENCODED_LENGTH + " byte(s) of body but " + buf.readableBytes() + " remain");
    }
    long mapId = readProducerId(buf);
    long lastSequenceNumber = buf.readLong();
    return new RetransmitRequestMessage(header, mapId, lastSequenceNumber);
  }
}
