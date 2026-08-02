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
 * The timestamp is supplied by the caller. This class reads no clock of any kind: not in a
 * constructor, not in a factory and not as a default. Encoding, decoding and comparing a heartbeat
 * are therefore exactly reproducible for a given set of inputs, which is what lets the streaming
 * shuffle suites assert on them without tolerating jitter. The sender owns the notion of time that
 * matters to it and passes the instant in; there is deliberately no constructor that would
 * synthesise one. Nothing here interprets the value either: a receiver compares successive
 * heartbeats from the same peer rather than comparing one against its own clock, so no assumption
 * is made that two executors' clocks agree.
 *
 * <pre>
 *   framed by StreamingShuffleMessage#toByteBuffer, 34 bytes in total
 *   +--------+-----------------------------------------+--------------+
 *   | type   | header                                  | timestampMs  |
 *   | 1 byte | 25 bytes, written by encodeHeader       | long, 8      |
 *   +--------+-----------------------------------------+--------------+
 * </pre>
 *
 * The body is a single {@code long}, so {@link #encodedLength()} is {@link
 * StreamingShuffleMessage#HEADER_ENCODED_LENGTH} plus eight, that is 33 bytes, and a framed
 * heartbeat therefore occupies 34.
 *
 * The discriminator is {@link StreamingShuffleMessageType#HEARTBEAT}, whose wire id is 2. That is
 * unrelated to the {@code HEARTBEAT} constant of {@link
 * org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type}, which carries id 5 in the
 * separate id space of the block-transfer and push-based-shuffle families. The two families never
 * share a channel and neither is registered in the other, so the coincidence of names is harmless.
 *
 * The inherited {@code shuffleId}, {@code mapId}, {@code partitionId} and {@code sequenceNumber}
 * are all required to be non-negative, and {@link StreamingShuffleMessage} enforces that centrally
 * on construction and on decode, so a heartbeat cannot assert liveness for an impossible stream
 * position. The
 * timestamp is a body field and carries no such restriction: it is caller-supplied and used only
 * for clock-skew diagnostics.
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
   * The instant at which the sender raised this heartbeat, in milliseconds.
   *
   * The value is caller-supplied and is never read from a clock here, which is what keeps a
   * heartbeat's encoding and its equality deterministic.
   *
   * The field is public and final, matching the concrete messages of the sibling block-transfer
   * family, and {@code timestampMs()} returns the same value in the accessor form this family's
   * header fields use, so a call site may be written either way.
   */
  public final long timestampMs;

  /**
   * Creates a heartbeat stamped with the protocol version this build speaks, which is how a
   * producer or a consumer raises one.
   *
   * @param shuffleId identifier of the shuffle this heartbeat belongs to
   * @param mapId identifier of the map task whose output stream this heartbeat concerns
   * @param partitionId identifier of the shuffle partition this heartbeat belongs to
   * @param sequenceNumber position within the partition's stream that the sender has reached, so
   *                       that a heartbeat also confirms where the stream stands while carrying no
   *                       data of its own
   * @param timestampMs the sending instant in milliseconds, supplied by the caller
   */
  public HeartbeatMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long timestampMs) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, sequenceNumber, timestampMs);
  }

  /**
   * Creates a heartbeat carrying an explicit protocol version, so that a heartbeat reconstructed
   * from the wire keeps the revision it actually arrived with instead of silently adopting this
   * build's own. That is what lets a version mismatch be reported precisely, and it is why the
   * version takes part in equality.
   *
   * @param protocolVersion the wire revision this heartbeat was, or will be, encoded with
   * @param shuffleId identifier of the shuffle this heartbeat belongs to
   * @param mapId identifier of the map task whose output stream this heartbeat concerns
   * @param partitionId identifier of the shuffle partition this heartbeat belongs to
   * @param sequenceNumber position within the partition's stream that the sender has reached
   * @param timestampMs the sending instant in milliseconds, supplied by the caller
   */
  public HeartbeatMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long timestampMs) {
    super(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
    this.timestampMs = checkTimestampMs(timestampMs);
  }

  /**
   * Creates a heartbeat from a header that has just been read off the wire, which is the form
   * {@link #decode(ByteBuf)} uses. Accepting the header as one value rather than as five positional
   * arguments removes any chance of transposing {@code shuffleId} and {@code partitionId} on the
   * way in, which is the reason {@code StreamingShuffleMessage(Header)} exists.
   *
   * @param header the decoded header, which must not be null
   * @param timestampMs the sending instant in milliseconds, as decoded from the message body
   * @throws NullPointerException if header is null
   */
  public HeartbeatMessage(Header header, long timestampMs) {
    super(header);
    this.timestampMs = checkTimestampMs(timestampMs);
  }

  /**
   * The instant at which the sender raised this heartbeat, in milliseconds.
   *
   * @return the caller-supplied timestamp carried in this heartbeat's body
   */
  public long timestampMs() {
    return timestampMs;
  }

  @Override
  protected StreamingShuffleMessageType type() {
    return StreamingShuffleMessageType.HEARTBEAT;
  }

  @Override
  public int hashCode() {
    return Objects.hash(headerHashCode(), timestampMs);
  }

  @Override
  public String toString() {
    return "HeartbeatMessage[" + headerToString() + ",timestampMs=" + timestampMs + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof HeartbeatMessage o) {
      return headerEquals(o)
        && timestampMs == o.timestampMs;
    }
    return false;
  }

  /**
   * The shared header followed by the eight bytes of {@code timestampMs}, that is 33 bytes, to
   * which framing adds one further byte for the type discriminator.
   *
   * @return the exact number of bytes {@link #encode(ByteBuf)} writes
   */
  @Override
  public int encodedLength() {
    return HEADER_ENCODED_LENGTH + 8;
  }

  /**
   * Writes the shared header, then the timestamp as the whole of the body.
   *
   * @param buf the buffer to write into, which must not be null and must have at least
   *            {@link #encodedLength()} writable bytes
   * @throws NullPointerException if buf is null
   */
  @Override
  public void encode(ByteBuf buf) {
    encodeHeader(buf);
    buf.writeLong(timestampMs);
  }

  /**
   * Reconstructs a heartbeat from a buffer positioned at the first byte of its encoded body, that
   * is, immediately after the framing type byte.
   *
   * The header is read first, and by the base class, so its field order can never drift from the
   * order {@link #encode(ByteBuf)} wrote it in; the timestamp follows as the whole of the body. The
   * remaining length is checked rather than asserted, because these bytes arrive from a remote peer
   * and a truncated frame has to be reported the same way in production as it is under test.
   *
   * @param buf the buffer to read from, positioned at the first header byte
   * @return the heartbeat just consumed from the buffer, carrying the protocol version its sender
   *         stamped into it
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if too few bytes remain for a header and a timestamp, or if
   *         the header carries a negative shuffle id, partition id or sequence number
   */
  static HeartbeatMessage decode(ByteBuf buf) {
    Header header = readHeader(buf);
    // Eight bytes, the same term encodedLength() adds on top of the shared header. Exactly eight,
    // not at least: a surplus is content the codec would never examine, and tolerating it would let
    // a peer append bytes that survive the message boundary unparsed.
    if (buf.readableBytes() != 8) {
      throw new IllegalArgumentException("Malformed streaming shuffle heartbeat: expected " +
        "exactly 8 byte(s) of timestamp but " + buf.readableBytes() + " remain");
    }
    long timestampMs = buf.readLong();
    return new HeartbeatMessage(header, timestampMs);
  }

  /**
   * Rejects a timestamp outside its legitimate domain, returning it unchanged so that it can be
   * assigned straight to the field.
   *
   * The value is milliseconds since the epoch, which no real clock reports as negative. The reason
   * to refuse one is not tidiness: a receiver judges liveness by subtracting this value from its
   * own clock, and a negative timestamp makes that difference enormous -- or, at {@link
   * Long#MIN_VALUE}, makes it overflow and change sign -- so a peer supplying one could drive a
   * liveness decision either way at will. Clock skew between hosts is real and is why the timestamp
   * is not compared against the receiver's own time here; a negative value is not skew, it is not a
   * time at all.
   *
   * @param timestampMs the candidate timestamp, in milliseconds since the epoch
   * @return timestampMs, unchanged
   * @throws IllegalArgumentException if the timestamp is negative
   */
  private static long checkTimestampMs(long timestampMs) {
    if (timestampMs < 0L) {
      throw new IllegalArgumentException(
        "Streaming shuffle heartbeat carries a negative timestampMs: " + timestampMs);
    }
    return timestampMs;
  }
}
