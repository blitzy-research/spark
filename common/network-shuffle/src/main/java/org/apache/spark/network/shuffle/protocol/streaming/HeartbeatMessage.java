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
 * Liveness and subscription signal exchanged over a streaming shuffle stream.
 *
 * The inherited sequence number is the next block position the sender expects to handle. A
 * producer announces the next position it will produce; a consumer announces the next position it
 * expects to receive. That makes the first heartbeat on a channel both the subscription request and
 * the resume handshake.
 *
 * A consumer heartbeat also carries a stable logical identity, as one fixed-width token. The token
 * is derived by Spark core from the reduce task attempt and remains stable when that attempt
 * reconnects on a new socket. The producer binds it to the transport-authenticated peer before
 * using it as a retained output cursor key. A producer heartbeat declares
 * {@link #NO_CONSUMER_TOKEN}, because a producer has no consumer cursor to resume. The identity is
 * a number rather than text precisely so that no field of the streaming control protocol has a
 * length a peer chooses, and so that nothing travelling on it can forge or split a log record.
 *
 * No time value travels on the wire and this class reads no clock. A receiver measures liveness by
 * when a heartbeat arrives, which is the only sound comparison between executors whose clocks need
 * not agree.
 *
 * <b>What a heartbeat says.</b> A heartbeat is also how a consumer subscribes and how it resumes,
 * and it says two things. The first is the <b>next</b> position its sender expects to handle, in
 * the inherited {@link #sequenceNumber()}: a consumer that has received nothing announces zero and
 * served from the beginning of the retained window, and a consumer that reconnects announces the
 * position it had reached. The second is {@link #consumerToken()}, the stable identity of the
 * logical consumer behind the channel.
 *
 * <b>Why a stable token, and why a fixed-width one.</b> A reduce task attempt that loses its
 * connection returns on a new socket, and a producer that knew its consumers only by socket would
 * treat the returning one as a stranger: it would open a second session with a cursor of its own
 * while the abandoned session went on pinning retained output and egress bandwidth until an expiry
 * sweep noticed it, and the two would serve the same partition in parallel. The token is what lets
 * the producer recognise the returning consumer as the same one, replace its session atomically and
 * carry its cursor across. It is authenticated by combination rather than on its own: the producer
 * pairs it with the transport's authenticated client identity, so within an authenticated
 * application no peer can present another application's consumer identity, and the cooperating task
 * attempts of one application are the only parties that can mint tokens for it.
 *
 * It is a {@code long} rather than a declared string, which is the whole of its bound: an earlier
 * revision carried a variable-length identity, and that made the heartbeat the one frame whose size
 * a peer could choose. A fixed eight bytes cannot be inflated, cannot carry a record separator into
 * a log line and cannot make a producer index content of a length it did not choose.
 * {@link #NO_CONSUMER_TOKEN} is the absence of a token, which a peer that does not present one
 * announces, and it is served exactly as before: by position alone, on the channel it arrived on.
 *
 * <pre>
 *   framed by StreamingShuffleMessage#toByteBuffer, 34 bytes in total
 *   +--------+-----------------------------------+-----------+---------------+
 *   | type   | header                            | mapId     | consumerToken |
 *   | 1 byte | 17 bytes, written by encodeHeader | long, 8   | long, 8       |
 *   +--------+-----------------------------------+-----------+---------------+
 * </pre>
 *
 * The body is the producer id followed by that token, so {@link #encodedLength()} is a fixed
 * thirty-three bytes, to which framing adds one further byte. A frame of any other size is refused
 * where it enters, so no remote peer can make a producer hold, index or log content of a length it
 * chose.
 *
 * The discriminator is {@link StreamingShuffleMessageType#HEARTBEAT}, whose wire id is 2. That is
 * unrelated to the {@code HEARTBEAT} constant of {@link
 * org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type}, which carries id 5 in the
 * separate id space of the block-transfer and push-based-shuffle families. The two families never
 * share a channel and neither is registered in the other, so the coincidence of names is harmless.
 *
 * The inherited {@code shuffleId}, {@code partitionId} and {@code sequenceNumber}, and the producer
 * id that opens the body, are all required to be non-negative, and {@link StreamingShuffleMessage}
 * enforces that centrally on construction and on decode, so a heartbeat cannot assert liveness for
 * an impossible stream position. The consumer token is required to be non-negative for the same
 * reason and on the same two routes, so there is exactly one value that means "no token".
 *
 * Instances are immutable once constructed, carry no logging and depend on nothing from Spark
 * core, so they are safe to hand between Netty event-loop threads and task threads without
 * further synchronisation.
 *
 * @since 4.2.0
 */
@Private
public final class HeartbeatMessage extends StreamingShuffleMessage {

  /**
   * Bytes this message adds to the inherited header: the eight of the producer id every message of
   * this family carries, followed by the eight of the consumer session token.
   *
   * The count is named rather than written out twice so that {@link #encodedLength()} and the
   * length check in {@link #decode(ByteBuf)} cannot drift apart: the encoder promises exactly this
   * many bytes and the decoder requires exactly this many to be present.
   */
  private static final int BODY_ENCODED_LENGTH = PRODUCER_ID_ENCODED_LENGTH + 8;

  /**
   * The token a consumer that presents no stable identity announces.
   *
   * Zero rather than a negative sentinel, because the field's domain is the non-negative longs and
   * a negative value is refused where the frame enters. A producer receiving this serves the
   * heartbeat exactly as it would have before the field existed: by the position declared, on the
   * channel the frame arrived on, with no session to supersede.
   */
  public static final long NO_CONSUMER_TOKEN = 0L;

  private final long consumerToken;

  /**
   * Creates a heartbeat stamped with the protocol version this build speaks, which is how both a
   * producer and a consumer raise one.
   *
   * @param shuffleId identifier of the shuffle this heartbeat belongs to
   * @param mapId identifier of the map task whose output stream this heartbeat concerns
   * @param partitionId identifier of the shuffle partition this heartbeat belongs to
   * @param sequenceNumber the next position within the partition's stream that the sender expects
   * to handle, so that a heartbeat also states where the stream stands -- and, for a consumer,
   * where a resumption is to begin -- while carrying no data of its own
   */
  public HeartbeatMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, sequenceNumber,
      NO_CONSUMER_TOKEN);
  }

  /**
   * Creates a heartbeat that presents a stable consumer session token, which is how a consumer that
   * may reconnect raises one.
   *
   * @param shuffleId identifier of the shuffle this heartbeat belongs to
   * @param mapId identifier of the map task whose output stream this heartbeat concerns
   * @param partitionId identifier of the shuffle partition this heartbeat belongs to
   * @param sequenceNumber the next position within the partition's stream that the sender expects
   * @param consumerToken stable identity of the logical consumer, or {@link #NO_CONSUMER_TOKEN}
   * @throws IllegalArgumentException if the token is negative
   */
  public HeartbeatMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long consumerToken) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, sequenceNumber, consumerToken);
  }

  /**
   * Creates a heartbeat carrying an explicit protocol version, which is how a decoded message keeps
   * the version it actually arrived with instead of silently adopting this build's own.
   *
   * @param protocolVersion the wire revision this message was encoded with
   * @param shuffleId identifier of the shuffle this heartbeat belongs to
   * @param mapId identifier of the map task whose output stream this heartbeat concerns
   * @param partitionId identifier of the shuffle partition this heartbeat belongs to
   * @param sequenceNumber the next position within the partition's stream that the sender expects
   * @param consumerToken stable identity of the logical consumer, or {@link #NO_CONSUMER_TOKEN}
   * @throws IllegalArgumentException if the token is negative
   */
  public HeartbeatMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long consumerToken) {
    super(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
    // After the super call because Java permits no statement before it; nothing observable has
    // happened by then, so a rejected heartbeat is discarded before any caller can hold it.
    checkConsumerToken(consumerToken, shuffleId, partitionId);
    this.consumerToken = consumerToken;
  }

  /**
   * Creates a producer heartbeat from a decoded header and producer id.
   *
   * @param header the decoded header, which must not be null
   * @param mapId identifier of the map task whose output stream this heartbeat concerns
   * @throws NullPointerException if header is null
   */
  public HeartbeatMessage(Header header, long mapId) {
    this(header, mapId, NO_CONSUMER_TOKEN);
  }

  /**
   * Creates a heartbeat from a header just read off the wire, together with the producer id and the
   * consumer session token that followed it in the body.
   *
   * @param header the decoded header, which must not be null
   * @param mapId identifier of the map task whose output stream this heartbeat concerns
   * @param consumerToken stable identity of the logical consumer, or {@link #NO_CONSUMER_TOKEN}
   * @throws NullPointerException if header is null
   * @throws IllegalArgumentException if the token is negative
   */
  public HeartbeatMessage(Header header, long mapId, long consumerToken) {
    super(header, mapId);
    checkConsumerToken(consumerToken, header.shuffleId(), header.partitionId());
    this.consumerToken = consumerToken;
  }

  /**
   * Refuses a negative consumer token, on every route into this class.
   *
   * A locally built heartbeat is checked on the same terms as a decoded one, so the codec cannot
   * encode a value it would then refuse to read back.
   */
  private static void checkConsumerToken(long consumerToken, int shuffleId, int partitionId) {
    if (consumerToken < 0L) {
      throw new IllegalArgumentException("Consumer session token cannot be negative: " +
        consumerToken + " for shuffle " + shuffleId + " partition " + partitionId);
    }
  }

  /**
   * The stable identity of the logical consumer behind this heartbeat's channel, or {@link
   * #NO_CONSUMER_TOKEN} when the sender presents none.
   *
   * A producer pairs this with the transport's authenticated client identity before it uses it, so
   * the value is a name within an authenticated application rather than an authority on its own.
   *
   * @return the token, never negative
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
    // The header hash covers the declared position and the producer id; the session token is this
    // class's own field and is mixed in so two heartbeats of different consumers are distinct.
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

  /**
   * The fixed encoded size of a heartbeat: the shared header, the producer id and the consumer
   * session token, and nothing else.
   */
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
   * It is package-private on purpose: {@code StreamingShuffleMessage.Decoder.fromByteBuffer} is the
   * only entry point a peer's bytes may take into the protocol, and that holds only if a caller
   * outside this package cannot reach a concrete decoder and skip the checks the central decoder
   * applies to the frame as a whole.
   *
   * @param buf the buffer to read from, which must not be null
   * @return the heartbeat just consumed from the buffer
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if the frame is truncated, if its body is not exactly the
   *                                 specified size, or if any identifier falls outside its domain
   */
  static HeartbeatMessage decode(ByteBuf buf) {
    Header header = readHeader(buf);
    // Exactly the body, not at least: a shortfall is a truncated frame and a surplus is content the
    // codec would never examine, and tolerating the latter would let a peer append bytes that
    // survive the message boundary unparsed. A heartbeat has one fixed size, so the check is a
    // comparison rather than a bound.
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
