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
 * Liveness signal exchanged over a streaming shuffle stream, which keeps failure detection armed
 * through a lull in which neither a data block nor an acknowledgement is due.
 *
 * Streaming shuffle pipelines map output straight from a producer executor to a consumer executor
 * rather than materialising it to local disk first, so each side depends on the other making
 * progress and each has to notice promptly when the other stops. Two windows are armed against this
 * message. A consumer treats five seconds without a data block or a heartbeat from its producer as
 * a producer failure: it atomically discards every block it accepted from that producer and has the
 * upstream stage recomputed. A producer treats ten seconds without acknowledgement progress from
 * its consumer as a consumer failure: it retains the unacknowledged window, spills it if buffer
 * utilisation warrants, and replays it when the consumer reconnects. Both timers live on the Spark
 * core side of the subsystem; this class is purely the datum they exchange, together with its
 * codec.
 *
 * Why the bound is an application-level one. The transport configuration exposes TCP keepalive as a
 * boolean only, and the keepalive interval is not available as a JDK socket option, so enabling OS
 * keepalive cannot by itself deliver a five-second bound. This message supplies the liveness input
 * and the receiver enforces the timeout against it, which is also why a heartbeat is sent on a
 * schedule rather than only when something has happened.
 *
 * No time value travels on the wire. This class reads no clock of any kind and carries none: a
 * receiver measures liveness by when a heartbeat <i>arrives</i>, which is the only reading that is
 * sound between two executors whose clocks need not agree, and it is also what keeps this message
 * the fixed {@value StreamingShuffleMessage#CONTROL_MESSAGE_ENCODED_LENGTH} bytes the specification
 * gives a control message. Encoding, decoding and comparing a heartbeat are therefore exactly
 * reproducible for a given set of inputs, which is what lets the streaming shuffle suites assert on
 * them without tolerating jitter.
 *
 * <b>What a heartbeat says, and how a resumption works without a wire identity.</b> A heartbeat is
 * also how a consumer subscribes and how it resumes, and it says exactly one substantive thing: the
 * <b>next</b> position its sender expects to handle, carried by the inherited {@link
 * #sequenceNumber()}. A consumer that has received nothing announces zero and is served from the
 * beginning of the retained window; a consumer that reconnects announces the position it had
 * reached and is served from there. The producer therefore needs no logical identity on the wire in
 * order to resume a consumer correctly: the position the consumer declares <i>is</i> the resume
 * point, and the session it is served on is the channel the heartbeat arrived on. That is what
 * allows the identity field an earlier revision carried to be dropped -- with it went the only
 * variable-length field in the streaming control protocol, and with that the only frame whose size
 * a peer could choose.
 *
 * <pre>
 *   framed by StreamingShuffleMessage#toByteBuffer, 26 bytes in total
 *   +--------+-----------------------------------+-----------+
 *   | type   | header                            | mapId     |
 *   | 1 byte | 17 bytes, written by encodeHeader | long, 8   |
 *   +--------+-----------------------------------+-----------+
 * </pre>
 *
 * The body is the producer id and nothing else, so {@link #encodedLength()} is exactly {@value
 * StreamingShuffleMessage#CONTROL_MESSAGE_ENCODED_LENGTH} bytes, to which framing adds one further
 * byte. A frame of any other size is refused where it enters, so no remote peer can make a producer
 * hold, index or log content of a length it chose.
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
 * an impossible stream position.
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
   * Bytes this message adds to the inherited header, namely the eight of the producer id that is
   * its whole body.
   *
   * The count is named rather than written out twice so that {@link #encodedLength()} and the
   * length check in {@link #decode(ByteBuf)} cannot drift apart: the encoder promises exactly this
   * many bytes and the decoder requires exactly this many to be present.
   */
  private static final int BODY_ENCODED_LENGTH = PRODUCER_ID_ENCODED_LENGTH;

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
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, sequenceNumber);
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
   */
  public HeartbeatMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber) {
    super(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
  }

  /**
   * Creates a heartbeat from a header just read off the wire, together with the producer id that
   * opened the body. This is the form {@link #decode(ByteBuf)} uses: taking the header as one value
   * rather than as four positional arguments removes any chance of transposing two same-typed
   * fields on the way in.
   *
   * @param header the decoded header, which must not be null
   * @param mapId identifier of the map task whose output stream this heartbeat concerns
   * @throws NullPointerException if header is null
   */
  public HeartbeatMessage(Header header, long mapId) {
    super(header, mapId);
  }

  @Override
  protected StreamingShuffleMessageType type() {
    return StreamingShuffleMessageType.HEARTBEAT;
  }

  @Override
  public int hashCode() {
    // The header hash is the whole of this message's identity, the declared position included,
    // because a heartbeat carries no field of its own beyond the producer id the header hash
    // covers.
    return headerHashCode();
  }

  @Override
  public String toString() {
    return "HeartbeatMessage[" + headerToString() + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof HeartbeatMessage o) {
      return headerEquals(o);
    }
    return false;
  }

  /**
   * The fixed encoded size of a heartbeat: the shared header plus the producer id, and nothing
   * else.
   */
  @Override
  public int encodedLength() {
    return HEADER_ENCODED_LENGTH + BODY_ENCODED_LENGTH;
  }

  /**
   * Writes the header, then the producer id, in the one order every message of this family uses so
   * that the encoders and {@link StreamingShuffleMessage#peekMapId(java.nio.ByteBuffer)} cannot
   * come to disagree about where the routing identity sits.
   */
  @Override
  public void encode(ByteBuf buf) {
    encodeHeader(buf);
    encodeProducerId(buf);
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
    return new HeartbeatMessage(header, mapId);
  }
}
