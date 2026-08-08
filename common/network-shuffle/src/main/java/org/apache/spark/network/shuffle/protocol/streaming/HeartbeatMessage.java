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
 * Liveness and subscription signal exchanged over a streaming shuffle stream. The inherited
 * sequence number is the next block position the sender expects to handle, which makes the first
 * heartbeat on a channel both the subscription request and the resume handshake.
 *
 * No time value travels on the wire and this class reads no clock: a receiver measures liveness by
 * when a heartbeat arrives, which is the only sound comparison between executors whose clocks need
 * not agree.
 *
 * A consumer heartbeat carries the stable identity of the reduce task attempt behind the channel as
 * one fixed-width token, so that an attempt returning on a new socket is recognised as the same
 * consumer and resumes its cursor rather than opening a second session. The token is fixed-width so
 * that no field of this protocol has a length a peer chooses, and it is authenticated by
 * combination: a producer pairs it with the transport-authenticated client identity before using it
 * as a cursor key. A producer heartbeat declares {@link #NO_CONSUMER_TOKEN}.
 *
 * @since 4.2.0
 */
@Private
public final class HeartbeatMessage extends StreamingShuffleMessage {

  /** Bytes this message adds to the header: the producer id, then the consumer session token. */
  private static final int BODY_ENCODED_LENGTH = PRODUCER_ID_ENCODED_LENGTH + 8;

  /** The token a consumer that presents no stable identity announces. */
  public static final long NO_CONSUMER_TOKEN = 0L;

  private final long consumerToken;

  public HeartbeatMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, sequenceNumber,
      NO_CONSUMER_TOKEN);
  }

  public HeartbeatMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long consumerToken) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, sequenceNumber, consumerToken);
  }

  public HeartbeatMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long consumerToken) {
    super(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
    // After the super call, which Java requires first; the rejected message escapes to no caller.
    checkConsumerToken(consumerToken, shuffleId, partitionId);
    this.consumerToken = consumerToken;
  }

  public HeartbeatMessage(Header header, long mapId) {
    this(header, mapId, NO_CONSUMER_TOKEN);
  }

  public HeartbeatMessage(Header header, long mapId, long consumerToken) {
    super(header, mapId);
    checkConsumerToken(consumerToken, header.shuffleId(), header.partitionId());
    this.consumerToken = consumerToken;
  }

  /** Refuses a negative consumer token, on every route into this class. */
  private static void checkConsumerToken(long consumerToken, int shuffleId, int partitionId) {
    if (consumerToken < 0L) {
      throw new IllegalArgumentException("Consumer session token cannot be negative: " +
        consumerToken + " for shuffle " + shuffleId + " partition " + partitionId);
    }
  }

  /**
   * The stable identity of the logical consumer behind this heartbeat's channel, or {@link
   * #NO_CONSUMER_TOKEN} when the sender presents none.
   */
  public long consumerToken() {
    return consumerToken;
  }

  @Override
  protected StreamingShuffleMessageType type() {
    return StreamingShuffleMessageType.HEARTBEAT;
  }

  @Override
  public int hashCode() {
    return 31 * headerHashCode() + Long.hashCode(consumerToken);
  }

  @Override
  public String toString() {
    return "HeartbeatMessage[" + headerToString() + ", consumerToken=" + consumerToken + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof HeartbeatMessage o) {
      return headerEquals(o) && consumerToken == o.consumerToken;
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return HEADER_ENCODED_LENGTH + BODY_ENCODED_LENGTH;
  }

  /**
   * Writes the header, then the producer id, in the one order every message of this family uses so
   * that the encoders and {@link StreamingShuffleMessage#peekMapId(java.nio.ByteBuffer)} cannot
   * come to disagree about where the routing identity sits, and then the session token.
   */
  @Override
  public void encode(ByteBuf buf) {
    encodeHeader(buf);
    encodeProducerId(buf);
    buf.writeLong(consumerToken);
  }

  /**
   * Reads a heartbeat from a buffer positioned at the first byte of the encoded body, that is,
   * immediately after the framing type discriminator has been consumed.
   *
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if the frame is truncated, if its body is not exactly the
   *     specified size, or if any identifier falls outside its domain
   */
  static HeartbeatMessage decode(ByteBuf buf) {
    Header header = readHeader(buf);
    // Exactly the body: a shortfall is a truncated frame and a surplus is unparsed content.
    if (buf.readableBytes() != BODY_ENCODED_LENGTH) {
      throw new IllegalArgumentException("Malformed streaming shuffle heartbeat: expected " +
        "exactly " + BODY_ENCODED_LENGTH + " byte(s) of body but " + buf.readableBytes() +
        " remain");
    }
    long mapId = readProducerId(buf);
    long consumerToken = buf.readLong();
    return new HeartbeatMessage(header, mapId, consumerToken);
  }
}
