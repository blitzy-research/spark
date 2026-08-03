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

import java.nio.charset.StandardCharsets;
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
 * <b>Why a heartbeat carries the sender's identity.</b> A heartbeat is also how a consumer
 * subscribes and how it resumes, and a resumption is only meaningful against the position the
 * producer recorded for <i>that</i> consumer. A producer that had to infer the identity from the
 * connection would key its per-consumer cursor by something a reconnection changes, so the very
 * event the resume handshake exists for -- the consumer coming back on a new channel -- would
 * present as a consumer that had never been seen and would be served from the beginning of the
 * retained window or from nothing at all. The identity is therefore stated by the sender, in the
 * body rather than in the shared header, because it is meaningful to exactly one message type. A
 * producer's own heartbeats carry {@link #NO_CONSUMER_ID}, since a producer is not a consumer of
 * anything and has no cursor to resume.
 *
 * <pre>
 *   framed by StreamingShuffleMessage#toByteBuffer, 38 + n bytes in total
 *   +--------+-----------------------------------+-------------+--------+------------+
 *   | type   | header                            | timestampMs | length | consumerId |
 *   | 1 byte | 25 bytes, written by encodeHeader | long, 8     | int, 4 | n bytes    |
 *   +--------+-----------------------------------+-------------+--------+------------+
 * </pre>
 *
 * The body is a {@code long} followed by a length-prefixed UTF-8 identity, so {@link
 * #encodedLength()} is {@link StreamingShuffleMessage#HEADER_ENCODED_LENGTH} plus twelve plus the
 * identity's encoded length -- 37 bytes for an empty identity, to which framing adds one further
 * byte. The prefix is read as a bound rather than trusted: an identity longer than {@link
 * #MAX_CONSUMER_ID_ENCODED_BYTES} is refused where it enters, so no remote peer can make a producer
 * hold, index or log an arbitrarily long string by claiming one.
 *
 * <p><b>Why the identity travels on the wire, which is what makes this the one variable-length
 * message.</b> The consumer-failure flow requires a producer to retain a consumer's unacknowledged
 * window while that consumer is absent and to <i>resume</i> it -- replaying from memory or from
 * spill -- when the consumer reconnects. A reconnection arrives on a new channel, so an identity
 * derived from the channel would make the returning consumer a different consumer, and the
 * producer would have to discard the window it was holding and force the reduce task's stage to be
 * recomputed instead. The identity is therefore a property of the consumer session rather than of
 * the connection, and it has to be stated in the first frame the session sends, which is this one.
 * The heartbeat is consequently 37 bytes plus the identity rather than a fixed 33, and that is a
 * deliberate cost of a recoverable reconnection: the departure is recorded here for the same reason
 * the header's own field set is recorded on {@link StreamingShuffleMessage}.
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
 * position. The timestamp is a body field and is required to be non-negative too, but that is the
 * whole of its domain: it is caller-supplied and read only for clock-skew diagnostics, never for a
 * liveness decision.
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
   * The identity a sender declares when it has none to declare, which is every producer.
   *
   * A producer heartbeats to prove it is alive, not to be resumed: it holds no cursor of its own on
   * the peer, so there is nothing for an identity to name. The empty string says exactly that, and
   * says it in one encoded field of four bytes.
   */
  public static final String NO_CONSUMER_ID = "";

  /**
   * The longest identity this message will encode or accept, in UTF-8 bytes.
   *
   * A consumer's own identity is a short derivation of its task attempt and partition range, so
   * this is an order of magnitude more room than a legitimate sender needs. It exists for the
   * illegitimate one: the length prefix arrives from a remote peer, and without a ceiling a frame
   * ask a producer to allocate, retain and index a string bounded only by the transport's frame
   * size.
   */
  public static final int MAX_CONSUMER_ID_ENCODED_BYTES = 256;

  /** Bytes this message's body occupies before the identity's own bytes: a long and a length. */
  private static final int FIXED_BODY_LENGTH = 8 + 4;

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
   * The stable logical identity of the sending consumer, or {@link #NO_CONSUMER_ID} when the sender
   * declares none.
   *
   * Stable is the operative word: the value must survive the loss of the channel it was first sent
   * on, because it is what a producer keys its per-consumer acknowledgement cursor by and therefore
   * what makes a reconnection resume rather than restart.
   */
  public final String consumerId;

  /**
   * Creates a heartbeat stamped with the protocol version this build speaks and declaring no
   * consumer identity, which is how a producer raises one.
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
    this(shuffleId, mapId, partitionId, sequenceNumber, timestampMs, NO_CONSUMER_ID);
  }

  /**
   * Creates a heartbeat stamped with the protocol version this build speaks and declaring the
   * sender's stable identity, which is how a consumer subscribes, heartbeats and resumes.
   *
   * @param shuffleId identifier of the shuffle this heartbeat belongs to
   * @param mapId identifier of the map task whose output stream this heartbeat concerns
   * @param partitionId identifier of the shuffle partition this heartbeat belongs to
   * @param sequenceNumber position within the partition's stream that the sender has reached
   * @param timestampMs the sending instant in milliseconds, supplied by the caller
   * @param consumerId the sender's stable logical identity, or {@link #NO_CONSUMER_ID} for none
   */
  public HeartbeatMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long timestampMs,
      String consumerId) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, sequenceNumber, timestampMs,
      consumerId);
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
   * @param consumerId the sender's stable logical identity, or {@link #NO_CONSUMER_ID} for none
   */
  public HeartbeatMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long timestampMs,
      String consumerId) {
    super(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
    this.timestampMs = checkTimestampMs(timestampMs);
    this.consumerId = checkConsumerId(consumerId);
  }

  /**
   * Creates a heartbeat from a header that has just been read off the wire, which is the form
   * {@link #decode(ByteBuf)} uses. Accepting the header as one value rather than as five positional
   * arguments removes any chance of transposing {@code shuffleId} and {@code partitionId} on the
   * way in, which is the reason {@code StreamingShuffleMessage(Header)} exists.
   *
   * @param header the decoded header, which must not be null
   * @param timestampMs the sending instant in milliseconds, as decoded from the message body
   * @param consumerId the sender's declared identity, as decoded from the message body
   * @throws NullPointerException if header is null
   */
  public HeartbeatMessage(Header header, long timestampMs, String consumerId) {
    super(header);
    this.timestampMs = checkTimestampMs(timestampMs);
    this.consumerId = checkConsumerId(consumerId);
  }

  /**
   * The instant at which the sender raised this heartbeat, in milliseconds.
   *
   * @return the caller-supplied timestamp carried in this heartbeat's body
   */
  public long timestampMs() {
    return timestampMs;
  }

  /**
   * The stable logical identity the sender declared, which is empty when it declared none.
   *
   * @return the sender's identity, never null
   */
  public String consumerId() {
    return consumerId;
  }

  /** Whether this heartbeat declares a stable logical identity for its sender. */
  public boolean declaresConsumerId() {
    return !consumerId.isEmpty();
  }

  @Override
  protected StreamingShuffleMessageType type() {
    return StreamingShuffleMessageType.HEARTBEAT;
  }

  @Override
  public int hashCode() {
    return Objects.hash(headerHashCode(), timestampMs, consumerId);
  }

  @Override
  public String toString() {
    return "HeartbeatMessage[" + headerToString() + ",timestampMs=" + timestampMs +
      ",consumerId=" + consumerId + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof HeartbeatMessage o) {
      return headerEquals(o)
        && timestampMs == o.timestampMs
        && consumerId.equals(o.consumerId);
    }
    return false;
  }

  /**
   * The shared header, the eight bytes of {@code timestampMs} and the length-prefixed identity: 37
   * bytes for an empty identity, to which framing adds one further byte for the type discriminator.
   *
   * @return the exact number of bytes {@link #encode(ByteBuf)} writes
   */
  @Override
  public int encodedLength() {
    return HEADER_ENCODED_LENGTH + FIXED_BODY_LENGTH + encodedConsumerId().length;
  }

  /**
   * Writes the shared header, then the timestamp and the length-prefixed identity as the body.
   *
   * @param buf the buffer to write into, which must not be null and must have at least
   *            {@link #encodedLength()} writable bytes
   * @throws NullPointerException if buf is null
   */
  @Override
  public void encode(ByteBuf buf) {
    encodeHeader(buf);
    buf.writeLong(timestampMs);
    byte[] identity = encodedConsumerId();
    buf.writeInt(identity.length);
    buf.writeBytes(identity);
  }

  /**
   * The identity's UTF-8 bytes, computed rather than cached.
   *
   * Recomputed on each of the two calls a send makes -- one to size the buffer, one to fill it --
   * because a cached array would be mutable state on a type this family guarantees is immutable and
   * safe to hand between an event-loop thread and a task thread. The identity is a short string and
   * a heartbeat is raised once per interval per stream, so the arithmetic is not on any hot path.
   */
  private byte[] encodedConsumerId() {
    return consumerId.getBytes(StandardCharsets.UTF_8);
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
   * @throws IllegalArgumentException if too few bytes remain for a header, a timestamp and an
   *         identity length; if the identity length is negative, exceeds
   *         {@link #MAX_CONSUMER_ID_ENCODED_BYTES} or exceeds the bytes that remain; if bytes
   *         remain once the body has been read; or if the header carries a negative shuffle id,
   *         partition id or sequence number
   */
  static HeartbeatMessage decode(ByteBuf buf) {
    Header header = readHeader(buf);
    if (buf.readableBytes() < FIXED_BODY_LENGTH) {
      throw new IllegalArgumentException("Malformed streaming shuffle heartbeat: expected at " +
        "least " + FIXED_BODY_LENGTH + " byte(s) of timestamp and identity length but " +
        buf.readableBytes() + " remain");
    }
    long timestampMs = buf.readLong();
    int identityLength = buf.readInt();
    // The prefix arrives from a remote peer, so it is bounded before it is used to allocate. Both
    // bounds are needed: the ceiling stops a peer naming a plausible-looking but enormous identity,
    // and the readable-bytes check stops one naming a length its own frame does not carry.
    if (identityLength < 0 || identityLength > MAX_CONSUMER_ID_ENCODED_BYTES) {
      throw new IllegalArgumentException("Malformed streaming shuffle heartbeat: the consumer " +
        "identity length " + identityLength + " is outside [0, " +
        MAX_CONSUMER_ID_ENCODED_BYTES + "]");
    }
    if (identityLength > buf.readableBytes()) {
      throw new IllegalArgumentException("Truncated streaming shuffle heartbeat: a consumer " +
        "identity of " + identityLength + " byte(s) was declared but only " +
        buf.readableBytes() + " remain");
    }
    byte[] identity = new byte[identityLength];
    buf.readBytes(identity);
    // Exactly the body, not at least: a surplus is content the codec would never examine, and
    // tolerating it would let a peer append bytes that survive the message boundary unparsed.
    if (buf.readableBytes() != 0) {
      throw new IllegalArgumentException("Malformed streaming shuffle heartbeat: " +
        buf.readableBytes() + " byte(s) remain after its body");
    }
    return new HeartbeatMessage(header, timestampMs, new String(identity, StandardCharsets.UTF_8));
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

  /**
   * Rejects an identity outside its legitimate domain, returning it unchanged so that it can be
   * assigned straight to the field.
   *
   * The same rules apply to an identity built in memory as to one read off the wire, and they are
   * stated here so that a sender cannot construct a message a receiver would be obliged to refuse,
   * and so that {@link #decode(ByteBuf)} -- which reaches the field only through this constructor
   * -- enforces exactly what construction does. An empty identity is legitimate and means the
   * sender declares none; null is not, because the field is compared and encoded unconditionally.
   *
   * <b>Length is not the only bound, and the second one is a security bound.</b> This identity is
   * chosen by a remote peer and it is written into a producer executor's log records, where it
   * names the consumer a message is about. A log record is a line-oriented artefact read by
   * operators and parsed by log pipelines, so an identity containing a carriage return, a line feed
   * or either of Unicode's own line separators does not merely look untidy -- it ends the record
   * early and starts a new one whose whole content the peer chose, which is how a peer forges log
   * entries it was never the subject of. The other non-printable characters are refused with them:
   * a NUL truncates a record in a C-based consumer of the log, a backspace or an escape rewrites
   * what a terminal displays, and none of them can appear in a legitimate executor identity, which
   * Spark composes from an executor id and a socket address.
   *
   * Refusing at the boundary rather than sanitising at each use is deliberate. There are many log
   * sites and a sanitiser omitted at one of them reinstates the whole problem, whereas a value that
   * cannot enter the system cannot reach any of them. It also keeps the identity usable as a map
   * key without a second, differently-normalised form of it existing anywhere.
   *
   * @param consumerId the candidate identity
   * @return consumerId, unchanged
   * @throws NullPointerException if consumerId is null
   * @throws IllegalArgumentException if the identity's UTF-8 encoding exceeds
   *         {@link #MAX_CONSUMER_ID_ENCODED_BYTES}, or if it contains a control character, a
   *         delete, or a Unicode line or paragraph separator
   */
  private static String checkConsumerId(String consumerId) {
    Objects.requireNonNull(consumerId, "consumerId");
    int encodedLength = consumerId.getBytes(StandardCharsets.UTF_8).length;
    if (encodedLength > MAX_CONSUMER_ID_ENCODED_BYTES) {
      throw new IllegalArgumentException("Streaming shuffle heartbeat carries a consumer " +
        "identity of " + encodedLength + " byte(s), which exceeds the limit of " +
        MAX_CONSUMER_ID_ENCODED_BYTES);
    }
    for (int index = 0; index < consumerId.length(); index++) {
      char candidate = consumerId.charAt(index);
      if (isForbiddenInConsumerId(candidate)) {
        // The offending character is reported by code point rather than by value: this message
        // becomes an exception message and therefore a log record of its own, so echoing the
        // character would reproduce in the diagnostic exactly the injection it is refusing.
        throw new IllegalArgumentException("Streaming shuffle heartbeat carries a consumer " +
          "identity with a forbidden character at index " + index + ": U+" +
          String.format("%04X", (int) candidate) + ". A consumer identity may not contain a " +
          "control character, a delete, or a Unicode line or paragraph separator, because it is " +
          "written into line-oriented log records on the producer executor");
      }
    }
    return consumerId;
  }

  /**
   * Whether one character may never appear in a consumer identity.
   *
   * The set is the union of the two C0 and C1 control ranges, the delete character, and the two
   * Unicode separators that a conforming reader treats as a line break -- {@code U+2028 LINE
   * SEPARATOR} and {@code U+2029 PARAGRAPH SEPARATOR}. Expressed as ranges rather than as a list of
   * individual characters so that nothing in either control block can be overlooked, and checked
   * per {@code char} rather than per code point because every character in the set is in the Basic
   * Multilingual Plane and outside the surrogate range, so a surrogate pair can never match one
   * half of it by accident.
   *
   * @param candidate the character to test
   * @return true when the character must be refused
   */
  private static boolean isForbiddenInConsumerId(char candidate) {
    return candidate <= 0x1F                      // C0 controls, including tab, CR and LF
      || candidate == 0x7F                        // delete
      || (candidate >= 0x80 && candidate <= 0x9F)  // C1 controls, including NEL at U+0085
      || candidate == '\u2028'                    // Unicode line separator
      || candidate == '\u2029';                   // Unicode paragraph separator
  }
}
