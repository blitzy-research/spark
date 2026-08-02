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
import org.apache.spark.network.protocol.Encodable;

/**
 * Abstract base of the streaming shuffle wire protocol, owning the fixed header that every
 * streaming shuffle message carries.
 *
 * Streaming shuffle pipelines map output straight from a producer executor to a consumer executor
 * instead of materialising it to local disk first, so the two exchange a small family of messages
 * while the map stage is still running: a data block, an acknowledgement of the consumer's
 * position, a liveness heartbeat, a retransmission request, and an orderly end-of-stream signal.
 * This class is what they all have in common. It follows {@link
 * org.apache.spark.network.shuffle.protocol.BlockTransferMessage}, the base of the block-transfer
 * and push-based-shuffle families in the parent package, so the streaming family reads the same
 * way. The two families are independent: they never share a channel, they have separate framing,
 * and neither is registered in the other's type-id space.
 *
 * Framing. A message goes on the wire through {@link #toByteBuffer()} as a single type byte
 * followed by the encoded body, and comes back through {@link Decoder#fromByteBuffer(ByteBuffer)}.
 * The body always opens with this class's header, so a framed message is laid out as follows.
 *
 * <pre>
 *   +--------+---------------------------------------------------------------+
 *   | type   | encoded body                                                  |
 *   | 1 byte | header (25 bytes) then the concrete message's own fields       |
 *   +--------+---------------------------------------------------------------+
 *
 *   the header, written by encodeHeader and read back by readHeader
 *   +-----------------+-----------+-----------+-------------+----------------+
 *   | protocolVersion | shuffleId | mapId     | partitionId | sequenceNumber |
 *   | byte, 1 byte    | int, 4    | long, 8   | int, 4      | long, 8        |
 *   +-----------------+-----------+-----------+-------------+----------------+
 * </pre>
 *
 * Why the map id is in the header. One executor hosts a single streaming listener for every map
 * task it runs, because a listener owns Netty event loops and a per-task listener would multiply
 * them by the number of tasks. A frame arriving there must therefore say which producer it concerns
 * before anything can route it, and a data block must say which producer it came from before a
 * consumer can attribute it: sequence numbers are counted per producer and per partition, so two
 * maps writing the same partition use the very same numbers. The map id is the field that keeps
 * those two streams apart, which is why it is part of the shared header rather than of one message.
 *
 * The header is {@value #HEADER_ENCODED_LENGTH} bytes, so a framed message always occupies {@code
 * encodedLength() + 1} bytes in total. The field order above is normative. It is written by {@link
 * #encodeHeader(ByteBuf)} and read by {@link #readHeader(ByteBuf)}, both defined here rather than
 * in the subclasses, precisely so that no subclass can let the two orders drift apart.
 *
 * Why the protocol version comes first. Streaming shuffle gives way to sort-based shuffle when
 * producer and consumer disagree on the protocol revision. Making that an explicit compatibility
 * check rather than an outcome inferred from a parse failure requires the version to be legible
 * before anything is dispatched, which is why it is the first field of the header and therefore
 * sits at a fixed offset of one byte into every framed message. {@link
 * #peekProtocolVersion(ByteBuffer)} reads it without disturbing the buffer it is given, and {@link
 * Decoder#fromByteBuffer(ByteBuffer)} validates it before it so much as looks at the type byte. The
 * version is deliberately not duplicated as a second framing prefix: it is a genuine header field,
 * so it round-trips through every concrete decoder, takes part in value equality, and costs no byte
 * twice.
 *
 * Validated header domains. {@code shuffleId}, {@code partitionId} and {@code sequenceNumber} are
 * all counted from zero, so a negative value in any of them is not a legal position in this
 * protocol: it can only be a mis-derived local value or a malformed or hostile frame. All three are
 * therefore checked centrally, in the one constructor every construction route funnels through and
 * in the {@link Header} record that {@link #readHeader(ByteBuf)} produces, so that no subclass and
 * no decoder can admit a routing identity or a stream position that the rest of the subsystem would
 * then have to distrust. Checking here rather than per message is what makes the guarantee total:
 * every concrete decoder rebuilds its message from a {@code Header}, so every one of them inherits
 * the check without repeating it. A body field is a separate matter and keeps whatever domain its
 * own message documents -- notably {@code AckMessage.consumerPosition}, which is deliberately
 * allowed to be negative because a consumer that has consumed nothing yet has no non-negative way
 * to say so.
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
 * Subclasses likewise fold {@link #headerEquals(StreamingShuffleMessage)}, {@link
 * #headerHashCode()} and {@link #headerToString()} into their own {@code equals}, {@code hashCode}
 * and {@code toString}, so that two messages differing only in their header are never mistaken for
 * one another.
 *
 * Validating untrusted input. Every byte handled here arrives from a remote peer over a channel
 * this process does not control, so the codec treats the frame as hostile until proved otherwise.
 * Three defences apply, and all three are enforced in this class rather than left to the
 * subclasses, because a check that each of five messages has to remember is a check that one of
 * them will eventually omit.
 *
 * <ul>
 *   <li><b>A frame maximum.</b> {@link #MAX_FRAME_LENGTH} bounds a framed message at the largest
 *       size the protocol can legitimately produce, so a peer cannot induce work or allocation
 *       proportional to a length it invented. It is checked before dispatch.</li>
 *   <li><b>Semantic domains, at one choke point.</b> {@link #readHeader(ByteBuf)} validates the
 *       protocol version and rejects a negative shuffle id, map id, partition id or sequence
 *       number. Every concrete decoder reads the header through that method before it reads a
 *       field of its own, so the domain check cannot be bypassed by reaching a concrete decoder
 *       directly.</li>
 *   <li><b>Exact frame consumption.</b> A decoder consumes its body exactly, and
 *       {@link Decoder#fromByteBuffer(ByteBuffer)} then requires the frame to be fully spent.
 *       Trailing bytes are a framing error and are rejected rather than ignored, which denies a
 *       peer the ability to smuggle unparsed content past the codec.</li>
 * </ul>
 *
 * Field values that are structurally valid may still be wrong <em>for the stream they arrived
 * on</em> -- a well-formed block addressed to another partition, or to another map task's output,
 * for instance. That is a binding question rather than a parsing one, so it is answered by {@link
 * #checkStreamContext(StreamingShuffleMessage, int, long, int)} and by the {@link
 * Decoder#fromByteBuffer(ByteBuffer, int, long, int)} overload, which a caller that knows which
 * stream it is reading should prefer.
 *
 * This type is internal to Spark. It carries no logging and no dependency on Spark core, being pure
 * data plus codec: a peer that sends something unacceptable is rejected here with a plain {@link
 * IllegalArgumentException}, and the caller, which owns the surrounding context, is what turns that
 * into a protocol-level failure. Instances are immutable once constructed and are therefore safe to
 * hand between Netty event-loop threads and task threads without further synchronisation.
 *
 * @since 4.2.0
 */
@Private
public abstract class StreamingShuffleMessage implements Encodable {

  /**
   * Revision of the streaming shuffle wire protocol that this build speaks, written into the header
   * of every message it sends.
   *
   * The value is a wire constant: it is bumped only when the meaning of an existing field changes
   * or a field is added, removed or reordered, because a peer built against a different revision
   * must be able to detect the difference from the version byte alone rather than by
   * misinterpreting bytes. {@link #isCompatible(byte)} is the single place that decides which
   * revisions this build accepts.
   */
  public static final byte CURRENT_PROTOCOL_VERSION = 1;

  /**
   * Number of bytes the shared header occupies: one for {@code protocolVersion}, four for {@code
   * shuffleId}, eight for {@code mapId}, four for {@code partitionId} and eight for {@code
   * sequenceNumber}.
   *
   * The total is 25 bytes. Every subclass builds its own {@code encodedLength()} on top of this
   * constant, so the four concrete messages that add a single {@code long} encode to {@code
   * HEADER_ENCODED_LENGTH + 8}, that is 33 bytes, while a data block adds its checksum and its
   * length-prefixed payload on top of that. The value is written as the sum of its parts rather
   * than as a literal so that it cannot drift from the layout it describes.
   */
  public static final int HEADER_ENCODED_LENGTH = 1 + 4 + 8 + 4 + 8;

  /**
   * Bytes the framing prefix occupies, being the single type-discriminator byte that
   * {@link #toByteBuffer()} writes ahead of the encoded body.
   *
   * This is the authoritative name for that one byte, and it is public because a framed message is
   * a resource in its own right: memory accounting and rate limiting must budget for the bytes that
   * actually reach the network, which are this prefix plus {@code encodedLength()} and not
   * {@code encodedLength()} alone. Every place that needs the framed size derives it from this
   * constant rather than restating the literal one, so the layers cannot come to disagree about the
   * same byte -- which is exactly the reasoning {@link #HEADER_ENCODED_LENGTH} applies to the
   * header.
   */
  public static final int FRAME_TYPE_PREFIX_LENGTH = 1;

  /**
   * Offset of the protocol-version byte within a framed message, that is, immediately after the
   * one-byte type discriminator that {@link #toByteBuffer()} writes as the framing prefix.
   */
  private static final int PROTOCOL_VERSION_FRAME_OFFSET = FRAME_TYPE_PREFIX_LENGTH;

  /**
   * Fewest bytes a framed message must contain before its version can be peeked and its type read,
   * namely the type byte plus the version byte that opens the header. A frame shorter than this
   * cannot be routed to a decoder at all, so it is rejected outright rather than being handed on.
   */
  private static final int MIN_FRAME_LENGTH = PROTOCOL_VERSION_FRAME_OFFSET + 1;

  /** Offset of the shuffle id within a framed message, counted from the type byte. */
  private static final int SHUFFLE_ID_FRAME_OFFSET = PROTOCOL_VERSION_FRAME_OFFSET + 1;

  /** Offset of the map id within a framed message, counted from the type byte. */
  private static final int MAP_ID_FRAME_OFFSET = SHUFFLE_ID_FRAME_OFFSET + 4;

  /**
   * Bytes a framed message must carry before {@link #peekShuffleId(ByteBuffer)} and
   * {@link #peekMapId(ByteBuffer)} can answer: the type byte, the version, the shuffle id and the
   * map id.
   */
  private static final int MIN_ROUTABLE_FRAME_LENGTH = MAP_ID_FRAME_OFFSET + 8;

  /**
   * Most bytes a well-formed framed message can occupy, and therefore the point past which a frame
   * is rejected before it is dispatched.
   *
   * The bound is the largest frame the protocol is able to produce, which is a data block carrying
   * a maximum-size payload. That figure is defined once, by the class that owns the block layout,
   * as {@link DataBlockMessage#MAX_ENCODED_FRAME_BYTES}, and is consumed here rather than restated:
   * a second arithmetic expression for the same quantity is a second thing to keep in step, and the
   * two would eventually disagree. Every other message type is far smaller, so one ceiling covers
   * the family.
   *
   * Why this exists at all: without it, the only thing standing between a peer and an arbitrary
   * allocation is the per-message body check, and a peer that never intends to send a valid message
   * is not obliged to reach one. Bounding the frame first means a hostile length is refused on the
   * strength of the frame alone, before any field of it is trusted or any buffer sized from it.
   *
   * The referenced value is a compile-time constant, so naming it here does not trigger class
   * initialisation and no initialisation cycle between the two classes is possible.
   */
  static final int MAX_FRAME_LENGTH = DataBlockMessage.MAX_ENCODED_FRAME_BYTES;

  /**
   * The number of bytes a message of the given encoded body length occupies on the wire, framing
   * prefix included.
   *
   * Callers outside this module -- the producer's buffer accounting and its egress rate limiter --
   * budget for framed bytes, so they compute the figure through this method instead of adding the
   * prefix themselves.
   *
   * @param encodedBodyLength the value the message's own {@code encodedLength()} reports
   * @return the total framed length in bytes
   */
  public static int framedLength(int encodedBodyLength) {
    return FRAME_TYPE_PREFIX_LENGTH + encodedBodyLength;
  }

  private final byte protocolVersion;
  private final int shuffleId;
  private final long mapId;
  private final int partitionId;
  private final long sequenceNumber;

  /**
   * Creates a message carrying an explicit protocol version, which is how a decoded message keeps
   * the version it actually arrived with instead of silently adopting this build's own.
   *
   * This is the constructor every other route reaches, whether a producer builds a message or a
   * decoder rebuilds one, so it is where the header's value domains are enforced.
   *
   * @param protocolVersion the wire revision this message was encoded with
   * @param shuffleId identifier of the shuffle this message belongs to; must be non-negative
   * @param mapId identifier of the map task whose output this message concerns; must be
   *              non-negative
   * @param partitionId identifier of the shuffle partition this message belongs to; must be
   *                    non-negative
   * @param sequenceNumber a position within this partition's stream, counted from zero. Which
   *                       position it is belongs to the concrete subtype and is documented there:
   *                       for a data block it is the block's own index, advancing by one per block,
   *                       which is what lets a consumer detect a gap or a reordering and lets a
   *                       producer bound its retransmission window; for an acknowledgement it is
   *                       the consumer's outbound control counter; for a retransmission request it
   *                       is the inclusive lower bound of the requested run; and for a heartbeat or
   *                       a termination it is the next data position expected or produced. Must be
   *                       non-negative in every case
   * @throws IllegalArgumentException if the shuffle id, the map id, the partition id or the
   *                                 sequence number is negative
   */
  protected StreamingShuffleMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber) {
    // Applied to a locally built message as well as to a decoded one, so that the domain rule is
    // the same in both directions. Without this a caller could construct a message that this codec
    // is then unable to decode -- it would encode without complaint and be rejected on arrival --
    // and an encode/decode asymmetry in a wire protocol is a bug waiting for a caller to find it.
    // The cost is four comparisons per message, which is not measurable against the work of
    // encoding one.
    checkHeaderDomains(shuffleId, mapId, partitionId, sequenceNumber);
    this.protocolVersion = protocolVersion;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.partitionId = partitionId;
    this.sequenceNumber = sequenceNumber;
  }

  /**
   * Creates a message stamped with this build's protocol version. This is the constructor a
   * producer uses, so that {@link #CURRENT_PROTOCOL_VERSION} never has to be repeated at every
   * construction site.
   *
   * @param shuffleId identifier of the shuffle this message belongs to; must be non-negative
   * @param mapId identifier of the map task whose output this message concerns; must be
   *              non-negative
   * @param partitionId identifier of the shuffle partition this message belongs to; must be
   *                    non-negative
   * @param sequenceNumber position of this message within its partition's stream; must be
   *                       non-negative
   * @throws IllegalArgumentException if the shuffle id, the map id, the partition id or the
   *                                 sequence number is negative
   */
  protected StreamingShuffleMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, sequenceNumber);
  }

  /**
   * Creates a message from a header that has just been read off the wire by {@link
   * #readHeader(ByteBuf)}. This is the constructor a concrete {@code decode(ByteBuf)} should use:
   * passing the header as one value rather than as five positional arguments removes any chance of
   * transposing {@code shuffleId} and {@code partitionId}, or {@code mapId} and {@code
   * sequenceNumber}, on the way in.
   *
   * @param header the decoded header, which must not be null
   * @throws NullPointerException if header is null
   */
  protected StreamingShuffleMessage(Header header) {
    this(Objects.requireNonNull(header, "header").protocolVersion(), header.shuffleId(),
      header.mapId(), header.partitionId(), header.sequenceNumber());
  }

  /**
   * The wire revision this message was encoded with. For a message this build created it is {@link
   * #CURRENT_PROTOCOL_VERSION}; for a decoded message it is whatever the peer sent, which is what
   * allows a mismatch to be reported precisely.
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
   * Identifier of the map task whose output this message concerns.
   *
   * This is the field that makes one listener per executor possible. Every frame names the producer
   * it belongs to, so an executor-scoped listener can route an inbound control frame to the right
   * producer's handler, and a consumer receiving blocks from several maps of the same partition can
   * attribute each one to the stream whose sequence space it was numbered in.
   *
   * @return the map id carried in this message's header
   */
  public final long mapId() {
    return mapId;
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
    // Allow room for encoded message, plus the type byte. The framed size is derived from
    // FRAME_TYPE_PREFIX_LENGTH rather than by adding one here, so that this allocation and the
    // budgets the producer's memory accounting and rate limiter apply cannot disagree.
    ByteBuf buf = Unpooled.buffer(framedLength(encodedLength()));
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
    buf.writeLong(mapId);
    buf.writeInt(partitionId);
    buf.writeLong(sequenceNumber);
  }

  /**
   * Reads the shared header, which a subclass's static {@code decode(ByteBuf)} must do before it
   * reads any field of its own. The read order is the mirror image of {@link
   * #encodeHeader(ByteBuf)}.
   *
   * The length is checked rather than asserted, because these bytes arrive from a remote peer and a
   * truncated frame must be reported the same way in production as it is under test. The five
   * fields are then handed to {@link Header}, whose constructor rejects a negative shuffle id, map
   * id, partition id or sequence number, so a malformed or hostile frame is refused here rather
   * than being carried into the subsystem as an impossible routing identity or stream position.
   *
   * This method is the protocol's validation choke point, and that is a deliberate placement. Every
   * concrete decoder must read the header before it reads a field of its own, so validating here
   * validates every inbound message exactly once, on the only path any of them can take. The
   * alternative -- checking in {@link Decoder#fromByteBuffer(ByteBuffer)} alone -- would leave the
   * per-type {@code decode} methods as an unvalidated side entrance, and checking in each of the
   * five decoders would make the guarantee depend on five separate authors remembering it.
   *
   * Three things are established before a header is handed back:
   *
   * <ul>
   *   <li>the frame carries a whole header, so no field is read from bytes that are not there;</li>
   *   <li>the protocol version is one this build speaks, checked before any semantic meaning is
   *       attached to the remaining fields, since their meaning is exactly what a version change is
   *       permitted to alter;</li>
   *   <li>the identifiers lie in their legitimate domains. {@code shuffleId} and
   *       {@code partitionId} are indices, {@code mapId} is a monotonically assigned task
   *       identifier and {@code sequenceNumber} is a position counted from zero, so a negative
   *       value in any of them is not a small message that will be handled downstream: it is a
   *       value that would be used to index a partition array, to key a producer registry, or to
   *       compute a retransmission window, and it is refused here where it enters rather than
   *       wherever it first happens to do damage.</li>
   * </ul>
   *
   * @param buf the buffer to read from, positioned at the first header byte
   * @return the header just consumed from the buffer
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if fewer than {@link #HEADER_ENCODED_LENGTH} bytes remain, if
   *                                 the protocol version is not one this build speaks, or if the
   *                                 header carries a negative shuffle id, map id, partition id or
   *                                 sequence number
   */
  protected static Header readHeader(ByteBuf buf) {
    Objects.requireNonNull(buf, "buf");
    if (buf.readableBytes() < HEADER_ENCODED_LENGTH) {
      throw new IllegalArgumentException("Truncated streaming shuffle header: expected " +
        HEADER_ENCODED_LENGTH + " byte(s) but only " + buf.readableBytes() + " remain");
    }
    byte protocolVersion = buf.readByte();
    int shuffleId = buf.readInt();
    long mapId = buf.readLong();
    int partitionId = buf.readInt();
    long sequenceNumber = buf.readLong();
    // Version first: the fields below only mean what this build thinks they mean if the peer is
    // speaking this build's revision of the protocol.
    checkProtocolVersion(protocolVersion);
    checkHeaderDomains(shuffleId, mapId, partitionId, sequenceNumber);
    return new Header(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
  }

  /**
   * Rejects header identifiers that fall outside their legitimate domains.
   *
   * Kept separate from {@link #readHeader(ByteBuf)} so that the same domain rule applies to a
   * header built in memory as to one read off the wire, and so that the rule is stated once. All
   * four fields are non-negative by construction on the producing side -- two are array indices,
   * one is a monotonically assigned task identifier and the last is a position counted from zero --
   * so a negative value can only be a corrupt or forged frame.
   *
   * @param shuffleId identifier of the shuffle the message belongs to
   * @param mapId identifier of the map task whose output the message concerns
   * @param partitionId identifier of the shuffle partition the message belongs to
   * @param sequenceNumber position of the message within its partition's stream
   * @throws IllegalArgumentException if any argument is negative
   */
  private static void checkHeaderDomains(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber) {
    if (shuffleId < 0) {
      throw new IllegalArgumentException(
        "Streaming shuffle message carries a negative shuffleId: " + shuffleId);
    }
    if (mapId < 0) {
      throw new IllegalArgumentException(
        "Streaming shuffle message carries a negative mapId: " + mapId);
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
   * Binds a decoded message to the stream the caller believes it is reading, rejecting one that is
   * structurally valid but addressed elsewhere.
   *
   * Decoding establishes that a frame is well formed; it cannot establish that the frame belongs
   * here, because the codec has no notion of which stream is being served. A consumer reading one
   * partition of one shuffle does know, and this is where that knowledge is applied. Without it a
   * peer able to place bytes on an established channel could have a block accepted into a partition
   * it was never produced for, which the block's own checksum would not contradict once the
   * identifying fields are the ones being questioned.
   *
   * @param message a decoded message; must not be null
   * @param expectedShuffleId the shuffle the receiving stream belongs to
   * @param expectedMapId the map task whose output the receiving stream carries
   * @param expectedPartitionId the partition the receiving stream belongs to
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

  /**
   * Compares the five header fields of this message with those of another, for a subclass to fold
   * into its own {@code equals} alongside its own fields.
   *
   * @param other the message to compare against; null is never equal
   * @return true if both messages carry an identical header
   */
  protected final boolean headerEquals(StreamingShuffleMessage other) {
    return other != null
      && protocolVersion == other.protocolVersion
      && shuffleId == other.shuffleId
      && mapId == other.mapId
      && partitionId == other.partitionId
      && sequenceNumber == other.sequenceNumber;
  }

  /**
   * Hash of the five header fields, for a subclass to combine with its own fields so that its
   * {@code hashCode} stays consistent with an {@code equals} built on {@link
   * #headerEquals(StreamingShuffleMessage)}.
   *
   * @return a hash over protocolVersion, shuffleId, mapId, partitionId and sequenceNumber
   */
  protected final int headerHashCode() {
    return Objects.hash(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
  }

  /**
   * The header rendered for diagnostics, as comma-separated {@code name=value} pairs with no
   * surrounding brackets, so that a subclass can embed it in the bracketed form the shuffle
   * protocol messages already use, for example {@code "AckMessage[" + headerToString() +
   * ",ackedSequenceNumber=7]"}.
   *
   * @return the header fields in wire order, rendered for a log or an error message
   */
  protected final String headerToString() {
    return "protocolVersion=" + protocolVersion + ",shuffleId=" + shuffleId +
        ",mapId=" + mapId + ",partitionId=" + partitionId + ",sequenceNumber=" + sequenceNumber;
  }

  /**
   * Reads the protocol version of a framed message without advancing the buffer's position.
   *
   * This is how a consumer or producer settles the compatibility question before committing to a
   * decode, and it is non-destructive so that the very same buffer can still be handed to {@link
   * Decoder#fromByteBuffer(ByteBuffer)} afterwards. The read is absolute, at an explicit index, so
   * the caller's position, limit and mark are all left exactly as they were.
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
   * Reads the shuffle id out of a framed message without disturbing the buffer.
   *
   * This exists for one caller: the executor-scoped listener, which owns a single port shared by
   * every producer on the executor and must decide which producer's handler a frame belongs to
   * before anything decodes it. Reading two fields at fixed offsets is far cheaper than decoding a
   * message only to discover it belongs to a different handler, and it keeps the offset arithmetic
   * inside the type that defines the layout rather than spread across a router that would then have
   * to be kept in step with it by hand.
   *
   * The read is absolute, at an explicit index, so the buffer's position is untouched and the
   * caller can hand the very same buffer on to a decoder afterwards.
   *
   * @param msg a framed message, that is, the type byte followed by the encoded body
   * @return the shuffle id the frame names
   * @throws NullPointerException if msg is null
   * @throws IllegalArgumentException if the frame is too short to carry a routing identity
   */
  public static int peekShuffleId(ByteBuffer msg) {
    checkRoutable(msg);
    return msg.getInt(msg.position() + SHUFFLE_ID_FRAME_OFFSET);
  }

  /**
   * Reads the map id out of a framed message without disturbing the buffer.
   *
   * See {@link #peekShuffleId(ByteBuffer)} for why this is offered. Together the two identify the
   * producer whose output a frame concerns, which is the whole of what a router needs to dispatch.
   *
   * @param msg a framed message, that is, the type byte followed by the encoded body
   * @return the map id the frame names
   * @throws NullPointerException if msg is null
   * @throws IllegalArgumentException if the frame is too short to carry a routing identity
   */
  public static long peekMapId(ByteBuffer msg) {
    checkRoutable(msg);
    return msg.getLong(msg.position() + MAP_ID_FRAME_OFFSET);
  }

  /**
   * Rejects a frame too short to carry the identity a router dispatches on.
   *
   * Checked before either peek reads, rather than relying on the buffer's own bounds check, so that
   * a truncated frame is reported as the framing error it is instead of surfacing as an index
   * failure from inside a buffer implementation.
   */
  private static void checkRoutable(ByteBuffer msg) {
    Objects.requireNonNull(msg, "msg");
    if (msg.remaining() < MIN_ROUTABLE_FRAME_LENGTH) {
      throw new IllegalArgumentException("Streaming shuffle message is too short to carry a " +
        "routing identity: " + msg.remaining() + " byte(s) remain but at least " +
        MIN_ROUTABLE_FRAME_LENGTH + " are needed");
    }
  }

  /**
   * Whether this build can speak the given protocol revision.
   *
   * Compatibility requires exact version equality: only {@link #CURRENT_PROTOCOL_VERSION} is
   * accepted, so an executor rolled to a different revision degrades to sort-based shuffle instead
   * of risking a misread frame. This one method is the whole of that policy.
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
   * The five header fields, decoded from the wire as one value.
   *
   * Returning a single value from {@link StreamingShuffleMessage#readHeader(ByteBuf)} and accepting
   * one in {@link StreamingShuffleMessage#StreamingShuffleMessage(Header)} keeps the header out of
   * every subclass's argument lists, where a transposition of two same-typed fields would be easy
   * to make and hard to see.
   *
   * The constructor enforces the header's value domains, which is why validation is total across
   * the decode path: {@code readHeader} is the only way a header is reconstructed from bytes, and
   * every concrete {@code decode(ByteBuf)} builds its message from the {@code Header} it returns.
   *
   * @param protocolVersion the wire revision the sender stamped into the message
   * @param shuffleId identifier of the shuffle the message belongs to; must be non-negative
   * @param mapId identifier of the map task whose output the message concerns; must be non-negative
   * @param partitionId identifier of the shuffle partition the message belongs to; must be
   *                    non-negative
   * @param sequenceNumber position of the message within its partition's stream; must be
   *                       non-negative
   */
  @Private
  public record Header(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber) {

    /**
     * Rejects a header whose identities or stream position are outside the protocol's domains.
     *
     * @throws IllegalArgumentException if the shuffle id, the map id, the partition id or the
     *         sequence number is negative
     */
    public Header {
      checkHeaderDomains(shuffleId, mapId, partitionId, sequenceNumber);
    }
  }

  // Decoding lives in its own nested type so that the single validated entry point is reachable
  // without an instance and cannot be confused with the per-subclass decoders it dispatches to.
  @Private
  public static class Decoder {

    /**
     * Deserializes the 'type' byte followed by the message itself.
     *
     * This is the only entry point a peer's bytes may take into the streaming protocol. The
     * per-type {@code decode(ByteBuf)} methods are package-private precisely so that this remains
     * true: a caller outside the package cannot reach a concrete decoder and thereby skip the frame
     * bounds, the version check or the exact-consumption requirement enforced here.
     *
     * @param msg a framed message, that is, the type byte followed by the encoded body
     * @return the decoded message
     * @throws NullPointerException if msg is null
     * @throws IllegalArgumentException if the frame is too short, longer than
     *                                 {@link #MAX_FRAME_LENGTH}, stamped with an unsupported
     *                                 protocol version, tagged with an unknown type, malformed for
     *                                 its type, or followed by bytes the decoder did not consume
     */
    public static StreamingShuffleMessage fromByteBuffer(ByteBuffer msg) {
      Objects.requireNonNull(msg, "msg");
      // Wrapping shares the bytes and leaves the caller's ByteBuffer position untouched, so a
      // caller that peeked the version beforehand can pass the very same buffer here.
      ByteBuf buf = Unpooled.wrappedBuffer(msg);
      if (buf.readableBytes() < MIN_FRAME_LENGTH) {
        throw new IllegalArgumentException("Streaming shuffle message is too short to decode: " +
          buf.readableBytes() + " byte(s) remain");
      }
      // Bound the frame before trusting anything inside it. The largest legitimate frame is a
      // maximum-size data block, so anything beyond that is malformed no matter what its fields
      // claim, and refusing it here keeps the work done on a hostile frame independent of the size
      // that frame asserts.
      if (buf.readableBytes() > MAX_FRAME_LENGTH) {
        throw new IllegalArgumentException("Streaming shuffle message exceeds the maximum frame " +
          "length: " + buf.readableBytes() + " byte(s) received but at most " + MAX_FRAME_LENGTH +
          " are permitted");
      }
      // The protocol version is the first field of the encoded header, immediately after the type
      // byte. Read it WITHOUT consuming, so a version mismatch is an explicit check rather than a
      // parse failure attributed to the wrong cause further down. readHeader checks it again on the
      // path every concrete decoder takes; checking here as well is what lets a mismatch be
      // reported before an unknown type byte is blamed for it.
      byte version = buf.getByte(buf.readerIndex() + PROTOCOL_VERSION_FRAME_OFFSET);
      checkProtocolVersion(version);
      byte type = buf.readByte();
      // Resolving the byte through the discriminator keeps one id space rather than two: an unknown
      // id is rejected by fromId, and the switch below is exhaustive over the enum, so a message
      // type added later cannot be forgotten here without failing to compile.
      //
      // Every arm delegates to that type's own static decode(ByteBuf), which reads the shared
      // header through readHeader before its own body, mirroring exactly what toByteBuffer wrote:
      // the type byte is already consumed above, so each decoder receives the buffer positioned at
      // the first header field. Dispatching here rather than in each caller keeps the wire format
      // knowledge in one place, and the exhaustive switch means a new message type cannot be added
      // to the discriminator without this dispatch failing to compile until it is handled.
      StreamingShuffleMessageType resolved = StreamingShuffleMessageType.fromId(type);
      StreamingShuffleMessage decoded = switch (resolved) {
        case DATA_BLOCK -> DataBlockMessage.decode(buf);
        case ACK -> AckMessage.decode(buf);
        case HEARTBEAT -> HeartbeatMessage.decode(buf);
        case RETRANSMIT_REQUEST -> RetransmitRequestMessage.decode(buf);
        case STREAM_TERMINATION -> StreamTerminationMessage.decode(buf);
      };
      // A decoder consumes its body exactly, so anything left over is content the codec never
      // examined. Treat that as a framing error rather than discarding it: silently tolerating a
      // suffix would let a peer append bytes to a valid message and have them survive the boundary
      // unparsed, and it would mask an encode/decode length disagreement that this check turns into
      // an immediate, attributable failure.
      if (buf.readableBytes() != 0) {
        throw new IllegalArgumentException("Streaming shuffle " + resolved + " message has " +
          buf.readableBytes() + " unconsumed trailing byte(s)");
      }
      return decoded;
    }

    /**
     * Deserializes a framed message and binds it to the stream the caller is reading, which is the
     * overload a consumer serving a known shuffle and partition should use.
     *
     * Decoding alone cannot tell a block addressed to this partition from one addressed to another,
     * nor one carrying this map task's output from one carrying another's, because the codec has no
     * idea which stream it is serving. Supplying that context here means a misaddressed frame is
     * refused at the boundary instead of being handed to a caller that has to remember to question
     * it.
     *
     * @param msg a framed message, that is, the type byte followed by the encoded body
     * @param expectedShuffleId the shuffle the receiving stream belongs to
     * @param expectedMapId the map task whose output the receiving stream carries
     * @param expectedPartitionId the partition the receiving stream belongs to
     * @return the decoded message, guaranteed to name the expected shuffle, map and partition
     * @throws NullPointerException if msg is null
     * @throws IllegalArgumentException for any reason {@link #fromByteBuffer(ByteBuffer)} rejects a
     *                                 frame, or if the message names a different shuffle, map or
     *                                 partition
     */
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
