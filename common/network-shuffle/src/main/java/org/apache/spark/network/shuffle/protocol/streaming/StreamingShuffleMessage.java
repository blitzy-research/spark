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

import java.nio.ByteBuffer;
import java.util.Objects;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.spark.network.protocol.Encodable;

/**
 * Abstract base of the streaming shuffle wire protocol, owning the fixed header that every
 * streaming shuffle message carries.
 *
 * Streaming shuffle pipelines map output straight from a producer executor to a consumer executor
 * instead of materialising it to local disk first, so the two exchange a small family of messages
 * while the map stage is still running: a data block, an acknowledgement of the consumer's
 * position, a liveness heartbeat, a retransmission request, and an orderly end-of-stream signal.
 * This class is what they all have in common. It is modelled deliberately on
 * {@link org.apache.spark.network.shuffle.protocol.BlockTransferMessage}, the base of the
 * block-transfer and push-based-shuffle families in the parent package, so that the streaming
 * family reads the same way instead of inventing a parallel encoding scheme. The two families stay
 * independent: they never share a channel, and neither is registered in the other's id space.
 *
 * Framing. A message goes on the wire through {@link #toByteBuffer()} as a single type byte
 * followed by the encoded body, and comes back through
 * {@link Decoder#fromByteBuffer(ByteBuffer)}. The body always opens with this class's header, so a
 * framed message is laid out as follows.
 *
 * <pre>
 *   +--------+---------------------------------------------------------------+
 *   | type   | encoded body                                                  |
 *   | 1 byte | header (17 bytes) then the concrete message's own fields       |
 *   +--------+---------------------------------------------------------------+
 *
 *   the header, written by encodeHeader and read back by readHeader
 *   +-----------------+-----------+-------------+----------------+
 *   | protocolVersion | shuffleId | partitionId | sequenceNumber |
 *   | byte, 1 byte    | int, 4    | int, 4      | long, 8        |
 *   +-----------------+-----------+-------------+----------------+
 * </pre>
 *
 * The header is {@value #HEADER_ENCODED_LENGTH} bytes, so a framed message always occupies
 * {@code encodedLength() + 1} bytes in total. The field order above is normative. It is written by
 * {@link #encodeHeader(ByteBuf)} and read by {@link #readHeader(ByteBuf)}, both defined here rather
 * than in the subclasses, precisely so that no subclass can let the two orders drift apart.
 *
 * Why the protocol version comes first. Streaming shuffle gives way to sort-based shuffle when
 * producer and consumer disagree on the protocol revision. Making that an explicit compatibility
 * check rather than an outcome inferred from a parse failure requires the version to be legible
 * before anything is dispatched, which is why it is the first field of the header and therefore
 * sits at a fixed offset of one byte into every framed message.
 * {@link #peekProtocolVersion(ByteBuffer)} reads it without disturbing the buffer it is given, and
 * {@link Decoder#fromByteBuffer(ByteBuffer)} validates it before it so much as looks at the type
 * byte. The version is deliberately not duplicated as a second framing prefix: it is a genuine
 * header field, so it round-trips through every concrete decoder, takes part in value equality, and
 * costs no byte twice.
 *
 * Contract for concrete messages. A subclass names its discriminator through {@link #type()} and
 * then encodes and decodes the header first, before any field of its own.
 *
 * <pre>
 *   public int encodedLength() {
 *     return HEADER_ENCODED_LENGTH + 8;          // header, then this message's own fields
 *   }
 *
 *   public void encode(ByteBuf buf) {
 *     encodeHeader(buf);                         // always first
 *     buf.writeLong(ackedSequenceNumber);
 *   }
 *
 *   public static AckMessage decode(ByteBuf buf) {
 *     Header header = readHeader(buf);           // always first
 *     long ackedSequenceNumber = buf.readLong();
 *     return new AckMessage(header, ackedSequenceNumber);
 *   }
 * </pre>
 *
 * {@link Encodable} requires {@link #encode(ByteBuf)} to write exactly {@link #encodedLength()}
 * bytes and {@link #toByteBuffer()} asserts it, so a subclass that adds a field must extend both.
 * Subclasses likewise fold {@link #headerEquals(StreamingShuffleMessage)},
 * {@link #headerHashCode()} and {@link #headerToString()} into their own {@code equals},
 * {@code hashCode} and {@code toString}, so that two messages differing only in their header are
 * never mistaken for one another.
 *
 * This type is internal to Spark. It carries no logging and no dependency on Spark core, being pure
 * data plus codec: a peer that sends something unacceptable is rejected here with a plain
 * {@link IllegalArgumentException}, and the caller, which owns the surrounding context, is what
 * turns that into a protocol-level failure. Instances are immutable once constructed and are
 * therefore safe to hand between Netty event-loop threads and task threads without further
 * synchronisation.
 *
 * @since 4.2.0
 */
public abstract class StreamingShuffleMessage implements Encodable {

  /**
   * Revision of the streaming shuffle wire protocol that this build speaks, written into the
   * header of every message it sends.
   *
   * The value is a wire constant: it is bumped only when the meaning of an existing field changes
   * or a field is added, removed or reordered, because a peer built against a different revision
   * must be able to detect the difference from the version byte alone rather than by
   * misinterpreting bytes. {@link #isCompatible(byte)} is the single place that decides which
   * revisions this build accepts.
   */
  public static final byte CURRENT_PROTOCOL_VERSION = 1;

  /**
   * Number of bytes the shared header occupies: one for {@code protocolVersion}, four for
   * {@code shuffleId}, four for {@code partitionId} and eight for {@code sequenceNumber}.
   *
   * The total is 17 bytes. Every subclass builds its own {@code encodedLength()} on top of this
   * constant, so the four concrete messages that add a single {@code long} encode to
   * {@code HEADER_ENCODED_LENGTH + 8}, that is 25 bytes, while a data block adds its checksum and
   * its length-prefixed payload on top of that. The value is written as the sum of its parts rather
   * than as a literal so that it cannot drift from the layout it describes.
   */
  public static final int HEADER_ENCODED_LENGTH = 1 + 4 + 4 + 8;

  /**
   * Offset of the protocol-version byte within a framed message, that is, immediately after the
   * one-byte type discriminator that {@link #toByteBuffer()} writes as the framing prefix.
   */
  private static final int PROTOCOL_VERSION_FRAME_OFFSET = 1;

  /**
   * Fewest bytes a framed message must contain before its version can be peeked and its type read,
   * namely the type byte plus the version byte that opens the header. A frame shorter than this
   * cannot be routed to a decoder at all, so it is rejected outright rather than being handed on.
   */
  private static final int MIN_FRAME_LENGTH = PROTOCOL_VERSION_FRAME_OFFSET + 1;

  private final byte protocolVersion;
  private final int shuffleId;
  private final int partitionId;
  private final long sequenceNumber;

  /**
   * Creates a message carrying an explicit protocol version, which is how a decoded message keeps
   * the version it actually arrived with instead of silently adopting this build's own.
   *
   * @param protocolVersion the wire revision this message was encoded with
   * @param shuffleId identifier of the shuffle this message belongs to
   * @param partitionId identifier of the shuffle partition this message belongs to
   * @param sequenceNumber position of this message within its partition's stream, counted from
   *                       zero and increasing by one per data block, which is what lets a consumer
   *                       detect a gap or a reordering and lets a producer bound the window of
   *                       blocks it must retain for retransmission
   */
  protected StreamingShuffleMessage(
      byte protocolVersion,
      int shuffleId,
      int partitionId,
      long sequenceNumber) {
    this.protocolVersion = protocolVersion;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.sequenceNumber = sequenceNumber;
  }

  /**
   * Creates a message stamped with this build's protocol version. This is the constructor a
   * producer uses, so that {@link #CURRENT_PROTOCOL_VERSION} never has to be repeated at every
   * construction site.
   *
   * @param shuffleId identifier of the shuffle this message belongs to
   * @param partitionId identifier of the shuffle partition this message belongs to
   * @param sequenceNumber position of this message within its partition's stream
   */
  protected StreamingShuffleMessage(int shuffleId, int partitionId, long sequenceNumber) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, partitionId, sequenceNumber);
  }

  /**
   * Creates a message from a header that has just been read off the wire by
   * {@link #readHeader(ByteBuf)}. This is the constructor a concrete {@code decode(ByteBuf)} should
   * use: passing the header as one value rather than as four positional arguments removes any
   * chance of transposing {@code shuffleId} and {@code partitionId} on the way in.
   *
   * @param header the decoded header, which must not be null
   * @throws NullPointerException if header is null
   */
  protected StreamingShuffleMessage(Header header) {
    this(Objects.requireNonNull(header, "header").protocolVersion(), header.shuffleId(),
      header.partitionId(), header.sequenceNumber());
  }

  /**
   * The wire revision this message was encoded with. For a message this build created it is
   * {@link #CURRENT_PROTOCOL_VERSION}; for a decoded message it is whatever the peer sent, which is
   * what allows a mismatch to be reported precisely.
   *
   * @return the protocol version carried in this message's header
   */
  public final byte protocolVersion() {
    return protocolVersion;
  }

  /**
   * Identifier of the shuffle this message belongs to. Needed on the receiving side to attribute a
   * message to the right stream, and to name the shuffle in a diagnostic when a block fails
   * verification or arrives out of order.
   *
   * @return the shuffle id carried in this message's header
   */
  public final int shuffleId() {
    return shuffleId;
  }

  /**
   * Identifier of the shuffle partition this message belongs to. A producer streams every reduce
   * partition it writes over its own logical stream, so this is what distinguishes them.
   *
   * @return the partition id carried in this message's header
   */
  public final int partitionId() {
    return partitionId;
  }

  /**
   * Position of this message within its partition's stream, counted from zero. Data blocks carry
   * consecutive sequence numbers, so a consumer detects a gap by comparing the number it receives
   * against the one it expected, and a producer uses the numbers a consumer acknowledges to bound
   * the window of blocks it must keep for retransmission.
   *
   * @return the sequence number carried in this message's header
   */
  public final long sequenceNumber() {
    return sequenceNumber;
  }

  /**
   * The discriminator for this message, written as the single framing byte that precedes the
   * encoded body and used by {@link Decoder#fromByteBuffer(ByteBuffer)} to choose a decoder.
   *
   * @return the type of this message, never null
   */
  protected abstract StreamingShuffleMessageType type();

  /** Serializes the 'type' byte followed by the message itself. */
  public ByteBuffer toByteBuffer() {
    // Allow room for encoded message, plus the type byte
    ByteBuf buf = Unpooled.buffer(encodedLength() + 1);
    // The wire id is reached through the accessor rather than through the enum's private field: the
    // discriminator is a top-level type in its own file, unlike the nested enum of
    // BlockTransferMessage, so its field is not visible from here.
    buf.writeByte(type().id());
    encode(buf);
    assert buf.writableBytes() == 0 : "Writable bytes remain: " + buf.writableBytes();
    return buf.nioBuffer();
  }

  /**
   * Writes the shared header, which a subclass's {@link #encode(ByteBuf)} must do before it writes
   * any field of its own.
   *
   * The write order here is the mirror image of {@link #readHeader(ByteBuf)} and the two must never
   * diverge; keeping both in this class rather than repeating them per message is what guarantees
   * it.
   *
   * @param buf the buffer to write into, which must not be null and must have at least
   *            {@link #HEADER_ENCODED_LENGTH} writable bytes
   * @throws NullPointerException if buf is null
   */
  protected final void encodeHeader(ByteBuf buf) {
    Objects.requireNonNull(buf, "buf");
    buf.writeByte(protocolVersion);
    buf.writeInt(shuffleId);
    buf.writeInt(partitionId);
    buf.writeLong(sequenceNumber);
  }

  /**
   * Reads the shared header, which a subclass's static {@code decode(ByteBuf)} must do before it
   * reads any field of its own. The read order is the mirror image of
   * {@link #encodeHeader(ByteBuf)}.
   *
   * The length is checked rather than asserted, because these bytes arrive from a remote peer and a
   * truncated frame must be reported the same way in production as it is under test.
   *
   * @param buf the buffer to read from, positioned at the first header byte
   * @return the header just consumed from the buffer
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if fewer than {@link #HEADER_ENCODED_LENGTH} bytes remain
   */
  protected static Header readHeader(ByteBuf buf) {
    Objects.requireNonNull(buf, "buf");
    if (buf.readableBytes() < HEADER_ENCODED_LENGTH) {
      throw new IllegalArgumentException("Truncated streaming shuffle header: expected " +
        HEADER_ENCODED_LENGTH + " byte(s) but only " + buf.readableBytes() + " remain");
    }
    byte protocolVersion = buf.readByte();
    int shuffleId = buf.readInt();
    int partitionId = buf.readInt();
    long sequenceNumber = buf.readLong();
    return new Header(protocolVersion, shuffleId, partitionId, sequenceNumber);
  }

  /**
   * Compares the four header fields of this message with those of another, for a subclass to fold
   * into its own {@code equals} alongside its own fields.
   *
   * @param other the message to compare against; null is never equal
   * @return true if both messages carry an identical header
   */
  protected final boolean headerEquals(StreamingShuffleMessage other) {
    return other != null
      && protocolVersion == other.protocolVersion
      && shuffleId == other.shuffleId
      && partitionId == other.partitionId
      && sequenceNumber == other.sequenceNumber;
  }

  /**
   * Hash of the four header fields, for a subclass to combine with its own fields so that its
   * {@code hashCode} stays consistent with an {@code equals} built on
   * {@link #headerEquals(StreamingShuffleMessage)}.
   *
   * @return a hash over protocolVersion, shuffleId, partitionId and sequenceNumber
   */
  protected final int headerHashCode() {
    return Objects.hash(protocolVersion, shuffleId, partitionId, sequenceNumber);
  }

  /**
   * The header rendered for diagnostics, as comma-separated {@code name=value} pairs with no
   * surrounding brackets, so that a subclass can embed it in the bracketed form the shuffle
   * protocol messages already use, for example
   * {@code "AckMessage[" + headerToString() + ",ackedSequenceNumber=7]"}.
   *
   * @return the header fields in wire order, rendered for a log or an error message
   */
  protected final String headerToString() {
    return "protocolVersion=" + protocolVersion + ",shuffleId=" + shuffleId +
        ",partitionId=" + partitionId + ",sequenceNumber=" + sequenceNumber;
  }

  /**
   * Reads the protocol version of a framed message without advancing the buffer's position.
   *
   * This is how a consumer or producer settles the compatibility question before committing to a
   * decode, and it is non-destructive so that the very same buffer can still be handed to
   * {@link Decoder#fromByteBuffer(ByteBuffer)} afterwards. The read is absolute, at an explicit
   * index, so the caller's position, limit and mark are all left exactly as they were.
   *
   * @param msg a framed message, that is, the type byte followed by the encoded body
   * @return the protocol version the sender stamped into the header
   * @throws NullPointerException if msg is null
   * @throws IllegalArgumentException if msg is too short to carry a type byte and a version byte
   */
  public static byte peekProtocolVersion(ByteBuffer msg) {
    Objects.requireNonNull(msg, "msg");
    if (msg.remaining() < MIN_FRAME_LENGTH) {
      throw new IllegalArgumentException("Streaming shuffle message is too short to carry a " +
        "protocol version: " + msg.remaining() + " byte(s) remain");
    }
    return msg.get(msg.position() + PROTOCOL_VERSION_FRAME_OFFSET);
  }

  /**
   * Whether this build can speak the given protocol revision.
   *
   * The streaming protocol is exact-match today, so only {@link #CURRENT_PROTOCOL_VERSION} is
   * accepted and an executor rolled to a different revision degrades to sort-based shuffle instead
   * of risking a misread frame. This one method is the whole of that policy: when a later revision
   * becomes able to read an earlier one, widening it here is all that is required.
   *
   * @param version a protocol version read from a message header
   * @return true if a message stamped with that version can be decoded by this build
   */
  public static boolean isCompatible(byte version) {
    return version == CURRENT_PROTOCOL_VERSION;
  }

  /**
   * Asserts that the given protocol revision is one this build can speak, naming both revisions if
   * it is not.
   *
   * The failure is signalled with {@link IllegalArgumentException} rather than an error, because a
   * version mismatch is a recoverable condition: it is one of the situations in which streaming
   * shuffle steps aside in favour of the sort-based implementation, and the caller needs to be able
   * to catch it and do exactly that.
   *
   * @param version a protocol version read from a message header
   * @throws IllegalArgumentException if the version is not one this build can decode
   */
  public static void checkProtocolVersion(byte version) {
    if (!isCompatible(version)) {
      throw new IllegalArgumentException("Incompatible streaming shuffle protocol version: peer " +
        "sent " + version + " but this executor speaks " + CURRENT_PROTOCOL_VERSION);
    }
  }

  /**
   * The four header fields, decoded from the wire as one value.
   *
   * Returning a single value from {@link StreamingShuffleMessage#readHeader(ByteBuf)} and accepting
   * one in {@link StreamingShuffleMessage#StreamingShuffleMessage(Header)} keeps the header out of
   * every subclass's argument lists, where a transposition of two same-typed fields would be easy
   * to make and hard to see.
   *
   * @param protocolVersion the wire revision the sender stamped into the message
   * @param shuffleId identifier of the shuffle the message belongs to
   * @param partitionId identifier of the shuffle partition the message belongs to
   * @param sequenceNumber position of the message within its partition's stream
   */
  public record Header(
      byte protocolVersion,
      int shuffleId,
      int partitionId,
      long sequenceNumber) {
  }

  // NB: Java does not support static methods in interfaces, so we must put this in a static class.
  public static class Decoder {
    /** Deserializes the 'type' byte followed by the message itself. */
    public static StreamingShuffleMessage fromByteBuffer(ByteBuffer msg) {
      Objects.requireNonNull(msg, "msg");
      // Wrapping shares the bytes and leaves the caller's ByteBuffer position untouched, so a
      // caller that peeked the version beforehand can pass the very same buffer here.
      ByteBuf buf = Unpooled.wrappedBuffer(msg);
      if (buf.readableBytes() < MIN_FRAME_LENGTH) {
        throw new IllegalArgumentException("Streaming shuffle message is too short to decode: " +
          buf.readableBytes() + " byte(s) remain");
      }
      // The protocol version is the first field of the encoded header, immediately after the type
      // byte. Read it WITHOUT consuming, so a version mismatch is an explicit check rather than a
      // parse failure attributed to the wrong cause further down.
      byte version = buf.getByte(buf.readerIndex() + PROTOCOL_VERSION_FRAME_OFFSET);
      checkProtocolVersion(version);
      byte type = buf.readByte();
      // Resolving the byte through the discriminator keeps one id space rather than two: an
      // unknown id is rejected by fromId, and the switch below is exhaustive over the enum, so a
      // message type added later cannot be forgotten here without failing to compile.
      //
      // Each arm becomes a delegation to that type's own static decode(ByteBuf) as the concrete
      // per-type message classes land. Until an arm's class is available it reports the type it
      // cannot yet materialise rather than returning null, so a premature caller fails loudly
      // and precisely here instead of propagating a null message down the pipeline. The switch
      // stays exhaustive over the enum throughout, preserving the compile-time guarantee above.
      StreamingShuffleMessageType resolved = StreamingShuffleMessageType.fromId(type);
      throw switch (resolved) {
        case DATA_BLOCK, ACK, HEARTBEAT, RETRANSMIT_REQUEST, STREAM_TERMINATION ->
            undecodable(resolved);
      };
    }

    /**
     * Builds the failure raised when a message type is recognised by the discriminator but its
     * concrete message class is not available to decode the body.
     */
    private static UnsupportedOperationException undecodable(StreamingShuffleMessageType type) {
      return new UnsupportedOperationException(
        "No decoder is registered for streaming shuffle message type " + type +
          "; the concrete message class for this type is not available yet");
    }
  }
}
