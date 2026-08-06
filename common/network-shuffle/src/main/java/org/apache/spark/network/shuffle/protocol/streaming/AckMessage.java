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
 * A consumer's acknowledgement of how far it has consumed a streaming shuffle partition, which is
 * what lets the producer release the memory holding everything up to that point.
 *
 * What travels on the wire is the <b>next</b> expected position, carried by the inherited sequence
 * number, so acknowledging nothing never requires a negative header field. {@link
 * #NOTHING_CONSUMED} is the one value that expresses "nothing consumed yet", and {@link
 * #MAX_CONSUMER_POSITION} bounds the other end, because the successor of {@link Long#MAX_VALUE} is
 * not a position at all. A producer may release every retained block up to and including the
 * acknowledged position, and only blocks it has actually sent may be acknowledged.
 *
 * @since 4.2.0
 */
@Private
public final class AckMessage extends StreamingShuffleMessage {

  /** The one position value that states no block has been consumed yet. */
  public static final long NOTHING_CONSUMED = -1L;

  /** The highest position an acknowledgement can express. */
  public static final long MAX_CONSUMER_POSITION = Long.MAX_VALUE - 1L;

  /** Bytes this message adds to the header: the producer id is its whole body. */
  private static final int BODY_ENCODED_LENGTH = PRODUCER_ID_ENCODED_LENGTH;

  /** Creates an acknowledgement stamped with this build's protocol version. */
  public AckMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long consumerPosition) {
    super(shuffleId, mapId, partitionId, nextExpectedPosition(consumerPosition));
  }

  public AckMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long consumerPosition) {
    super(protocolVersion, shuffleId, mapId, partitionId, nextExpectedPosition(consumerPosition));
  }

  public AckMessage(Header header, long mapId) {
    super(header, mapId);
  }

  /**
   * The highest data-block sequence number the sending consumer has consumed for this shuffle
   * partition.
   */
  public long consumerPosition() {
    return sequenceNumber() - 1L;
  }

  /**
   * Whether this acknowledgement stays inside the range of blocks the producer has actually sent,
   * and may therefore be applied to the retained window.
   */
  public boolean acknowledgesWithin(long highestSentSequenceNumber) {
    long position = consumerPosition();
    return position == NOTHING_CONSUMED || position <= highestSentSequenceNumber;
  }

  /** Whether this acknowledgement is newer than the last one already applied for its stream. */
  public boolean supersedes(long lastAppliedPosition) {
    return sequenceNumber() > lastAppliedPosition;
  }

  @Override
  protected StreamingShuffleMessageType type() {
    return StreamingShuffleMessageType.ACK;
  }

  @Override
  public int hashCode() {
    return headerHashCode();
  }

  @Override
  public String toString() {
    return "AckMessage[" + headerToString() +
        ",consumerPosition=" + consumerPosition() + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof AckMessage o) {
      return headerEquals(o);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    // Seventeen bytes of inherited header plus eight for the producer id, twenty-five in all, which
    // is the fixed size the specification gives a control message.
    return HEADER_ENCODED_LENGTH + BODY_ENCODED_LENGTH;
  }

  @Override
  public void encode(ByteBuf buf) {
    // The header goes first, and is written by the parent class, so that its field order can never
    // diverge from the order readHeader expects to find it in; the producer id follows it, as it
    // does in every message of this family.
    encodeHeader(buf);
    encodeProducerId(buf);
  }

  /**
   * Reads an acknowledgement from a buffer positioned at the first byte of the encoded body, that
   * is, immediately after the framing type discriminator has been consumed.
   *
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if the encoded message is truncated, whether in its header
   *     or in its body, if its header carries a negative shuffle id, map id, partition id or
   *     sequence number, or if the position lies outside its domain
   */
  static AckMessage decode(ByteBuf buf) {
    // Read in exactly the order encode wrote: the shared header first, then this message's own
    // field.
    Header header = readHeader(buf);
    // Exactly, not at least: a shortfall is a truncated frame and a surplus is content the codec
    // would never examine, and tolerating the latter would let a peer append bytes that survive the
    // message boundary unparsed.
    if (buf.readableBytes() != BODY_ENCODED_LENGTH) {
      throw new IllegalArgumentException("Malformed streaming shuffle acknowledgement: expected " +
        "exactly " + BODY_ENCODED_LENGTH + " byte(s) of body but " + buf.readableBytes() +
        " remain");
    }
    long mapId = readProducerId(buf);
    return new AckMessage(header, mapId);
  }

  /**
   * Converts an acknowledged position into the next-expected position the header carries, rejecting
   * a position outside its legitimate domain on the way.
   *
   * @throws IllegalArgumentException if the position is negative and is not {@link
   *     #NOTHING_CONSUMED}, or exceeds {@link #MAX_CONSUMER_POSITION}
   */
  private static long nextExpectedPosition(long consumerPosition) {
    if (consumerPosition < 0L && consumerPosition != NOTHING_CONSUMED) {
      throw new IllegalArgumentException("Streaming shuffle acknowledgement carries an invalid " +
        "consumerPosition: " + consumerPosition + " is negative but is not the " +
        NOTHING_CONSUMED + " sentinel that states nothing has been consumed");
    }
    if (consumerPosition > MAX_CONSUMER_POSITION) {
      throw new IllegalArgumentException("Streaming shuffle acknowledgement carries an invalid " +
        "consumerPosition: " + consumerPosition + " exceeds the highest expressible position " +
        MAX_CONSUMER_POSITION);
    }
    return consumerPosition + 1L;
  }
}
