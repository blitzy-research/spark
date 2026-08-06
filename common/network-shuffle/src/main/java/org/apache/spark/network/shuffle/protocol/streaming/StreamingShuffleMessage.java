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

import org.apache.spark.annotation.Private;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;
import org.apache.spark.network.protocol.Encodable;

/**
 * Abstract base of the streaming shuffle wire protocol, owning the fixed header that every
 * streaming shuffle message carries. It follows {@link
 * org.apache.spark.network.shuffle.protocol.BlockTransferMessage}, the base of the block-transfer
 * family in the parent package. The two families are independent: they never share a channel and
 * neither is registered in the other's type-id space.
 *
 * <pre>
 *   +--------+-------------------------------------------------------------+
 *   | type   | encoded body                                                |
 *   | 1 byte | header (17 bytes), producer id (8 bytes), own fields         |
 *   +--------+-------------------------------------------------------------+
 *
 *   header, written by encodeHeader and read back by readHeader
 *   +-----------------+-----------+-------------+----------------+
 *   | protocolVersion | shuffleId | partitionId | sequenceNumber |
 *   | byte, 1 byte    | int, 4    | int, 4      | long, 8        |
 *   +-----------------+-----------+-------------+----------------+
 * </pre>
 *
 * The field order above is normative, and both directions of it live here rather than in the
 * subclasses so that no subclass can let them drift apart. The protocol version is the first header
 * field, and therefore at a fixed offset in every framed message, so that a version disagreement is
 * an explicit compatibility check rather than an outcome inferred from a parse failure. The
 * producing map id is the first body field of every message because one executor hosts a single
 * streaming listener for all of its map tasks, so a frame must name the producer it concerns before
 * anything can route it, and because sequence numbers are counted per producer and per partition.
 *
 * {@code shuffleId}, {@code partitionId}, {@code sequenceNumber} and the producer id are counted
 * from zero, so all four are rejected when negative -- centrally, on construction and on decode
 * alike, since every concrete decoder rebuilds its message from a {@link Header}. A body field
 * keeps whatever domain its own message documents.
 *
 * @since 4.2.0
 */
@Private
public abstract class StreamingShuffleMessage implements Encodable {

  /**
   * Revision of the streaming shuffle wire protocol that this build speaks, written into the header
   * of every message it sends.
   */
  public static final byte CURRENT_PROTOCOL_VERSION = 1;

  /** Bytes the shared header occupies, laid out as the class comment's table shows. */
  public static final int HEADER_ENCODED_LENGTH = 1 + 4 + 4 + 8;

  /** Bytes the producer id occupies as the first field of every message's body. */
  public static final int PRODUCER_ID_ENCODED_LENGTH = 8;

  /** Encoded length of an acknowledgement or a termination, neither carrying a field of its own. */
  public static final int CONTROL_MESSAGE_ENCODED_LENGTH =
      HEADER_ENCODED_LENGTH + PRODUCER_ID_ENCODED_LENGTH;

  /** Bytes the type discriminator occupies ahead of the encoded body. */
  public static final int FRAME_TYPE_PREFIX_LENGTH = 1;

  private static final int PROTOCOL_VERSION_FRAME_OFFSET = FRAME_TYPE_PREFIX_LENGTH;

  /** Fewest bytes a frame must hold before its type and version can be peeked. */
  private static final int MIN_FRAME_LENGTH = PROTOCOL_VERSION_FRAME_OFFSET + 1;

  private static final int SHUFFLE_ID_FRAME_OFFSET = PROTOCOL_VERSION_FRAME_OFFSET + 1;

  private static final int PRODUCER_ID_FRAME_OFFSET =
      FRAME_TYPE_PREFIX_LENGTH + HEADER_ENCODED_LENGTH;

  /** Fewest bytes a frame must hold before its routing identity can be peeked. */
  private static final int MIN_ROUTABLE_FRAME_LENGTH =
      PRODUCER_ID_FRAME_OFFSET + PRODUCER_ID_ENCODED_LENGTH;

  /** Most bytes a well-formed frame may occupy, past which it is rejected before dispatch. */
  static final int MAX_FRAME_LENGTH = DataBlockMessage.MAX_ENCODED_FRAME_BYTES;

  /**
   * The number of bytes a message of the given encoded body length occupies on the wire, framing
   * prefix included.
   */
  public static int framedLength(int encodedBodyLength) {
    return FRAME_TYPE_PREFIX_LENGTH + encodedBodyLength;
  }

  private final byte protocolVersion;
  private final int shuffleId;
  private final long mapId;
  private final int partitionId;
  private final long sequenceNumber;

  protected StreamingShuffleMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber) {
    checkHeaderDomains(shuffleId, partitionId, sequenceNumber);
    checkProducerId(mapId);
    this.protocolVersion = protocolVersion;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.partitionId = partitionId;
    this.sequenceNumber = sequenceNumber;
  }

  /**
   * Creates a message stamped with this build's protocol version.
   *
   * @throws IllegalArgumentException if the shuffle id, the map id, the partition id or the
   *     sequence number is negative
   */
  protected StreamingShuffleMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, sequenceNumber);
  }

  protected StreamingShuffleMessage(Header header, long mapId) {
    this(Objects.requireNonNull(header, "header").protocolVersion(), header.shuffleId(),
      mapId, header.partitionId(), header.sequenceNumber());
  }

  public final byte protocolVersion() {
    return protocolVersion;
  }

  /** Identifier of the shuffle this message belongs to. */
  public final int shuffleId() {
    return shuffleId;
  }

  public final long mapId() {
    return mapId;
  }

  public final int partitionId() {
    return partitionId;
  }

  /** Position of this message within its partition's stream, counted from zero. */
  public final long sequenceNumber() {
    return sequenceNumber;
  }

  protected abstract StreamingShuffleMessageType type();

  public ByteBuffer toByteBuffer() {
    return encodeContiguousFrame().nioBuffer();
  }

  @Private
  public ManagedBuffer toManagedBuffer() {
    return new NettyManagedBuffer(encodeManagedFrame());
  }

  protected ByteBuf encodeManagedFrame() {
    return encodeContiguousFrame();
  }

  private ByteBuf encodeContiguousFrame() {
    ByteBuf buf = Unpooled.buffer(framedLength(encodedLength()));
    buf.writeByte(type().id());
    encode(buf);
    assert buf.writableBytes() == 0 : "Writable bytes remain: " + buf.writableBytes();
    return buf;
  }

  /**
   * Writes the shared header, which a subclass's {@link #encode(ByteBuf)} must do before it writes
   * any field of its own.
   *
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
   * Writes the producer id, which every concrete message emits as the first field of its body,
   * immediately after {@link #encodeHeader(ByteBuf)} and before any field of its own.
   */
  protected final void encodeProducerId(ByteBuf buf) {
    Objects.requireNonNull(buf, "buf");
    buf.writeLong(mapId);
  }

  /**
   * Reads the producer id a concrete decoder must consume immediately after the header.
   *
   * @throws IllegalArgumentException if the body is truncated or the producer id is negative
   */
  protected static long readProducerId(ByteBuf buf) {
    Objects.requireNonNull(buf, "buf");
    if (buf.readableBytes() < PRODUCER_ID_ENCODED_LENGTH) {
      throw new IllegalArgumentException("Truncated streaming shuffle producer id: expected " +
        PRODUCER_ID_ENCODED_LENGTH + " byte(s) but only " + buf.readableBytes() + " remain");
    }
    long mapId = buf.readLong();
    checkProducerId(mapId);
    return mapId;
  }

  /**
   * Reads the shared header, which a subclass's static {@code decode(ByteBuf)} must do before it
   * reads any field of its own.
   *
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if fewer than {@link #HEADER_ENCODED_LENGTH} bytes remain,
   *     if the protocol version is not one this build speaks, or if the header carries a negative
   *     shuffle id, partition id or sequence number
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
    // Version first: the fields below only mean what this build thinks they mean if the peer is
    // speaking this build's revision of the protocol.
    checkProtocolVersion(protocolVersion);
    checkHeaderDomains(shuffleId, partitionId, sequenceNumber);
    return new Header(protocolVersion, shuffleId, partitionId, sequenceNumber);
  }

  /**
   * Rejects header identifiers that fall outside their legitimate domains.
   *
   * @throws IllegalArgumentException if any argument is negative
   */
  private static void checkHeaderDomains(
      int shuffleId,
      int partitionId,
      long sequenceNumber) {
    if (shuffleId < 0) {
      throw new IllegalArgumentException(
        "Streaming shuffle message carries a negative shuffleId: " + shuffleId);
    }
    if (partitionId < 0) {
      throw new IllegalArgumentException(
        "Streaming shuffle message carries a negative partitionId: " + partitionId);
    }
    if (sequenceNumber < 0) {
      throw new IllegalArgumentException(
        "Streaming shuffle message carries a negative sequenceNumber: " + sequenceNumber);
    }
  }

  /**
   * Rejects a producer id that falls outside its legitimate domain.
   *
   * @throws IllegalArgumentException if the producer id is negative
   */
  private static void checkProducerId(long mapId) {
    if (mapId < 0) {
      throw new IllegalArgumentException(
        "Streaming shuffle message carries a negative mapId: " + mapId);
    }
  }

  /**
   * Binds a decoded message to the stream the caller believes it is reading, rejecting one that is
   * structurally valid but addressed elsewhere.
   *
   * @throws NullPointerException if message is null
   * @throws IllegalArgumentException if the message names a different shuffle, map or partition
   */
  public static void checkStreamContext(
      StreamingShuffleMessage message,
      int expectedShuffleId,
      long expectedMapId,
      int expectedPartitionId) {
    Objects.requireNonNull(message, "message");
    if (message.shuffleId != expectedShuffleId || message.mapId != expectedMapId
        || message.partitionId != expectedPartitionId) {
      throw new IllegalArgumentException("Streaming shuffle message does not belong to this " +
        "stream: message names shuffleId=" + message.shuffleId + ",mapId=" + message.mapId +
        ",partitionId=" + message.partitionId + " but the stream serves shuffleId=" +
        expectedShuffleId + ",mapId=" + expectedMapId + ",partitionId=" + expectedPartitionId);
    }
  }

  protected final boolean headerEquals(StreamingShuffleMessage other) {
    return other != null
      && protocolVersion == other.protocolVersion
      && shuffleId == other.shuffleId
      && mapId == other.mapId
      && partitionId == other.partitionId
      && sequenceNumber == other.sequenceNumber;
  }

  protected final int headerHashCode() {
    return Objects.hash(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
  }

  protected final String headerToString() {
    return "protocolVersion=" + protocolVersion + ",shuffleId=" + shuffleId +
        ",mapId=" + mapId + ",partitionId=" + partitionId + ",sequenceNumber=" + sequenceNumber;
  }

  /**
   * Reads the protocol version of a framed message without advancing the buffer's position.
   *
   * @throws NullPointerException if msg is null
   * @throws IllegalArgumentException if msg is too short to carry a type byte and a version
   *     byte
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
   * Reads the shuffle id out of a framed message without disturbing the buffer.
   *
   * @throws NullPointerException if msg is null
   * @throws IllegalArgumentException if the frame is too short to carry a routing identity
   */
  public static int peekShuffleId(ByteBuffer msg) {
    checkRoutable(msg);
    return msg.getInt(msg.position() + SHUFFLE_ID_FRAME_OFFSET);
  }

  public static long peekMapId(ByteBuffer msg) {
    checkRoutable(msg);
    return msg.getLong(msg.position() + PRODUCER_ID_FRAME_OFFSET);
  }

  /** Rejects a frame too short to carry the identity a router dispatches on. */
  private static void checkRoutable(ByteBuffer msg) {
    Objects.requireNonNull(msg, "msg");
    if (msg.remaining() < MIN_ROUTABLE_FRAME_LENGTH) {
      throw new IllegalArgumentException("Streaming shuffle message is too short to carry a " +
        "routing identity: " + msg.remaining() + " byte(s) remain but at least " +
        MIN_ROUTABLE_FRAME_LENGTH + " are needed");
    }
  }

  /** Whether this build can speak the given protocol revision. */
  public static boolean isCompatible(byte version) {
    return version == CURRENT_PROTOCOL_VERSION;
  }

  /**
   * Asserts that the given protocol revision is one this build can speak, naming both revisions if
   * it is not.
   *
   * @throws IllegalArgumentException if the version is not one this build can decode
   */
  public static void checkProtocolVersion(byte version) {
    if (!isCompatible(version)) {
      throw new IllegalArgumentException("Incompatible streaming shuffle protocol version: peer " +
        "sent " + version + " but this executor speaks " + CURRENT_PROTOCOL_VERSION);
    }
  }

  /** The four header fields, decoded from the wire as one value. */
  @Private
  public record Header(
      byte protocolVersion,
      int shuffleId,
      int partitionId,
      long sequenceNumber) {

    public Header {
      checkHeaderDomains(shuffleId, partitionId, sequenceNumber);
    }
  }

  /** Executor-owned reservation gate used before a decoder allocates a data-block payload. */
  @Private
  public interface PayloadReservation {

    boolean tryReserve(
        int shuffleId,
        long mapId,
        int partitionId,
        long sequenceNumber,
        int payloadBytes);

    void release(int payloadBytes);
  }

  /** Raised when the caller's executor-wide quota refuses a payload before allocation. */
  @Private
  public static class PayloadReservationRejectedException extends RuntimeException {

    private final int shuffleId;
    private final long mapId;
    private final int partitionId;
    private final long sequenceNumber;
    private final int payloadBytes;

    public PayloadReservationRejectedException(
        int shuffleId,
        long mapId,
        int partitionId,
        long sequenceNumber,
        int payloadBytes) {
      super("Streaming shuffle payload reservation was refused for shuffle " + shuffleId +
        " map " + mapId + " partition " + partitionId + " sequence " + sequenceNumber +
        " before allocating " + payloadBytes + " byte(s)");
      this.shuffleId = shuffleId;
      this.mapId = mapId;
      this.partitionId = partitionId;
      this.sequenceNumber = sequenceNumber;
      this.payloadBytes = payloadBytes;
    }

    public int shuffleId() {
      return shuffleId;
    }

    public long mapId() {
      return mapId;
    }

    public int partitionId() {
      return partitionId;
    }

    public long sequenceNumber() {
      return sequenceNumber;
    }

    public int payloadBytes() {
      return payloadBytes;
    }
  }

  @Private
  public static class Decoder {

    /**
     * Deserializes the 'type' byte followed by the message itself.
     *
     * @throws NullPointerException if msg is null
     * @throws IllegalArgumentException if the frame is too short, longer than {@link
     *     #MAX_FRAME_LENGTH}, stamped with an unsupported protocol version, tagged with an unknown
     *     type, malformed for its type, or followed by bytes the decoder did not consume
     */
    public static StreamingShuffleMessage fromByteBuffer(ByteBuffer msg) {
      return fromByteBuffer(msg, null);
    }

    /** Deserializes one frame while reserving a data payload before its array is allocated. */
    public static StreamingShuffleMessage fromByteBuffer(
        ByteBuffer msg,
        PayloadReservation payloadReservation) {
      Objects.requireNonNull(msg, "msg");
      // Wrapping shares the bytes and leaves the caller's ByteBuffer position untouched, so a
      // caller that peeked the version beforehand can pass the very same buffer here.
      ByteBuf buf = Unpooled.wrappedBuffer(msg);
      if (buf.readableBytes() < MIN_FRAME_LENGTH) {
        throw new IllegalArgumentException("Streaming shuffle message is too short to decode: " +
          buf.readableBytes() + " byte(s) remain");
      }
      // Bound the frame before trusting anything inside it.
      if (buf.readableBytes() > MAX_FRAME_LENGTH) {
        throw new IllegalArgumentException("Streaming shuffle message exceeds the maximum frame " +
          "length: " + buf.readableBytes() + " byte(s) received but at most " + MAX_FRAME_LENGTH +
          " are permitted");
      }
      // The protocol version is the first field of the encoded header, immediately after the type
      // byte.
      byte version = buf.getByte(buf.readerIndex() + PROTOCOL_VERSION_FRAME_OFFSET);
      checkProtocolVersion(version);
      byte type = buf.readByte();
      // Resolving the byte through the discriminator keeps one id space rather than two: an unknown
      // id is rejected by fromId, and the switch below is exhaustive over the enum, so a message
      // type added later cannot be forgotten here without failing to compile.
      StreamingShuffleMessageType resolved = StreamingShuffleMessageType.fromId(type);
      StreamingShuffleMessage decoded = switch (resolved) {
        case DATA_BLOCK -> DataBlockMessage.decode(buf, payloadReservation);
        case ACK -> AckMessage.decode(buf);
        case HEARTBEAT -> HeartbeatMessage.decode(buf);
        case RETRANSMIT_REQUEST -> RetransmitRequestMessage.decode(buf);
        case STREAM_TERMINATION -> StreamTerminationMessage.decode(buf);
      };
      // Every decoder in this family consumes its body exactly, never merely at least: a surplus is
      // content the codec would not examine, and tolerating it would let a peer append bytes that
      // survive the message boundary unparsed.
      if (buf.readableBytes() != 0) {
        throw new IllegalArgumentException("Streaming shuffle " + resolved + " message has " +
          buf.readableBytes() + " unconsumed trailing byte(s)");
      }
      return decoded;
    }

    public static StreamingShuffleMessage fromByteBuffer(
        ByteBuffer msg,
        int expectedShuffleId,
        long expectedMapId,
        int expectedPartitionId) {
      StreamingShuffleMessage decoded = fromByteBuffer(msg);
      checkStreamContext(decoded, expectedShuffleId, expectedMapId, expectedPartitionId);
      return decoded;
    }
  }
}
