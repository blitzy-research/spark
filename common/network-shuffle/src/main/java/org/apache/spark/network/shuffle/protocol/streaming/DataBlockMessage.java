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
import java.util.Arrays;
import java.util.Objects;

import io.netty.buffer.ByteBuf;

import org.apache.spark.annotation.Private;
import org.apache.spark.network.protocol.Encoders;

/**
 * One block of streaming shuffle data on its way from a producer executor to a consumer executor.
 *
 * This is the only message of the streaming shuffle family that carries a payload, and so the only
 * one whose encoded length varies. A producer frames the records it has buffered for a reduce
 * partition into a run of these messages, stamps each with the next sequence number and with a
 * CRC32C binding its bytes to that identity, and puts them on the wire while the map stage is still
 * running. A consumer recomputes the checksum before the block's records become visible and then
 * acknowledges the position it has reached, which is what lets the producer release the memory the
 * block held.
 *
 * <b>Wire layout.</b> The shared header comes first, exactly as {@link StreamingShuffleMessage}
 * requires of every concrete message, followed by the checksum and then the length-prefixed
 * payload.
 *
 * <pre>
 *   +--------+-----------+--------------+---------------+------------------+
 *   | type   | header    | checksum     | payload len   | payload          |
 *   | 1 byte | 25 bytes  | long, 8      | int, 4        | payload.length   |
 *   +--------+-----------+--------------+---------------+------------------+
 *      framing            the body encoded by encode(ByteBuf)
 * </pre>
 *
 * {@link #encodedLength()} is therefore exactly {@code 29 + payload.length}, and a framed message
 * produced by {@link #toByteBuffer()} occupies {@code 30 + payload.length} bytes. The payload is
 * written with {@code Encoders.ByteArrays}, the same length-prefixed form the sibling
 * block-transfer messages already use for their own byte arrays, rather than a hand-rolled
 * equivalent that could encode the same bytes a slightly different way.
 *
 * <b>Size cap.</b> A payload may be at most {@value #MAX_BLOCK_SIZE_BYTES} bytes. The cap is what
 * makes pipelining work at all: output has to leave the producer in small pieces rather than as one
 * buffer per partition for reduce-side work to overlap map-side work. It is a wire constant rather
 * than a tunable, because both peers must agree on it for a receiver to be entitled to reject an
 * over-size length prefix, and it is enforced at three points so that no route into an instance can
 * bypass it:
 *
 * <ul>
 *   <li>every constructor rejects an over-size array, so a producer cannot build one;</li>
 *   <li>{@link #decode(ByteBuf)} builds through a constructor, so the wire cannot smuggle one
 *       past that check;</li>
 *   <li>{@link #decode(ByteBuf)} additionally inspects the payload's length prefix
 *       <em>without consuming it</em> and rejects a negative, over-size or unbacked value
 *       <em>before</em> any array is allocated. This one matters most: the length prefix arrives
 *       from a remote peer, and {@code Encoders.ByteArrays.decode} allocates an array of exactly
 *       the size the prefix claims with no bound of its own, so a corrupt or hostile frame would
 *       otherwise turn four bytes into an arbitrarily large allocation.</li>
 * </ul>
 *
 * A payload of exactly {@value #MAX_BLOCK_SIZE_BYTES} bytes is legal; one byte more is not. An
 * empty payload is also legal, which lets a zero-record partition be streamed and checksummed on
 * the same code path as any other.
 *
 * <b>Header domains.</b> The inherited {@code shuffleId}, {@code mapId}, {@code partitionId} and
 * {@code sequenceNumber} are all required to be non-negative, and {@link StreamingShuffleMessage}
 * enforces that centrally on construction and on decode, so a block can no more carry an impossible
 * routing identity or stream position than it can carry an over-size payload.
 *
 * <b>Payload bytes versus framed bytes.</b> The cap above bounds the <em>payload</em>. A block put
 * on the wire additionally carries {@value #FRAMING_OVERHEAD_BYTES} bytes of framing, so the
 * largest legal block occupies {@value #MAX_ENCODED_FRAME_BYTES} bytes in total. The two figures
 * are published as {@link #MAX_BLOCK_SIZE_BYTES} and {@link #MAX_ENCODED_FRAME_BYTES} precisely
 * so that a component budgeting a network-facing or memory-facing resource can name the one it
 * means: this class is the single authority for both, and no other layer restates either value.
 *
 * <b>Failure reporting.</b> Everything this class rejects is signalled with
 * {@link IllegalArgumentException}, matching the sibling shuffle protocol messages. That is
 * deliberate on both sides of the boundary: these are recoverable conditions, so a caller can
 * catch one and fall back to sort-based shuffle or ask for the block again, and this module owns
 * none of the surrounding context needed to raise a Spark error condition. Translating a rejection
 * into a protocol-level failure belongs to the caller.
 *
 * <b>Checksum scope.</b> The CRC32C covers the payload <em>and</em> the header fields that place it
 * in the stream -- shuffle id, partition id, sequence number and payload length -- computed by
 * {@link StreamingShuffleChecksum#computeBlock(int, long, int, long, byte[])}. Covering the payload
 * alone
 * would attest only that some bytes arrived intact and would say nothing about where they belong,
 * so a block whose header was rewritten in flight would verify cleanly and then be consumed as
 * though it were legitimately addressed. Binding the metadata in closes that gap at no extra cost,
 * since the same single pass produces the value.
 *
 * <b>Immutability and threading.</b> An instance is genuinely immutable once constructed, and that
 * is enforced rather than merely documented. The payload is a private array that no caller ever
 * holds a reference to: the public constructors copy the array handed to them, and the accessors
 * hand back either a read-only view ({@link #payloadBuffer()}) or a fresh copy ({@link
 * #copyPayload()}). Returning the live array, as an earlier revision of this class did, made the
 * block mutable by anyone who had ever touched it -- a caller reusing its own buffer would silently
 * rewrite the bytes of a block already queued for transmission, already checksummed, or already
 * retained for retransmission, and the checksum would then be a promise about content the block no
 * longer held. A block is checksummed once and may be replayed long afterwards, so nothing short of
 * exclusive ownership makes that promise keepable.
 *
 * Ownership can be transferred explicitly where a copy would be pure waste, through {@link
 * #withOwnedPayload}: the caller surrenders the array and must not touch it again. That is how
 * {@link #decode(ByteBuf)} avoids copying, since the array it passes was freshly allocated by the
 * decoder and is reachable from nowhere else. Because an instance is immutable, it is safe to hand
 * between Netty event-loop threads and task threads without further synchronisation. The class
 * carries no logging and no dependency on Spark core.
 *
 * @since 4.2.0
 */
@Private
public final class DataBlockMessage extends StreamingShuffleMessage {

  /**
   * Largest payload, in bytes, that a single streaming shuffle data block may carry.
   *
   * Two mebibytes. This is a wire constant shared by producer and consumer and is not configurable:
   * a receiver can only reject an over-size length prefix before allocating for it if it already
   * knows the bound the sender was held to.
   */
  public static final int MAX_BLOCK_SIZE_BYTES = 2 * 1024 * 1024;

  /**
   * Bytes the CRC32C value occupies on the wire, being one {@code long}.
   *
   * Named rather than spelled as a bare literal so that {@link #encodedLength()} and the bounds
   * check in {@link #decode(ByteBuf)} cannot come to disagree about the same field, which is the
   * reasoning {@link StreamingShuffleMessage#HEADER_ENCODED_LENGTH} applies to the header.
   */
  private static final int CHECKSUM_ENCODED_LENGTH = 8;

  /**
   * Bytes the payload's length prefix occupies, being the {@code int} that {@code
   * Encoders.ByteArrays} writes ahead of the array itself.
   */
  private static final int PAYLOAD_LENGTH_PREFIX_LENGTH = 4;

  /**
   * Bytes a data block occupies on the wire over and above its payload.
   *
   * Thirty-eight bytes: the one-byte framing prefix, the twenty-five-byte header, the eight-byte
   * CRC32C and the four-byte payload length prefix. This constant exists because payload bytes and
   * framed bytes are two different resources and confusing them is a real defect: a producer that
   * budgets
   * {@link #MAX_BLOCK_SIZE_BYTES} for a block it then puts on the network as
   * {@link #MAX_ENCODED_FRAME_BYTES} bytes has under-accounted for every block it sends. Every
   * layer that has to reason about framed bytes -- the buffer accounting of the memory spill
   * manager and the burst allowance of the egress rate limiter -- reads the overhead from here
   * rather than restating it.
   */
  public static final int FRAMING_OVERHEAD_BYTES =
    StreamingShuffleMessage.FRAME_TYPE_PREFIX_LENGTH +
      StreamingShuffleMessage.HEADER_ENCODED_LENGTH + CHECKSUM_ENCODED_LENGTH +
      PAYLOAD_LENGTH_PREFIX_LENGTH;

  /**
   * Largest number of bytes a single streaming shuffle data block can occupy on the wire, framing
   * included.
   *
   * This is the one authoritative maximum <em>encoded frame</em> size of the streaming protocol, as
   * distinct from the maximum <em>payload</em> size {@link #MAX_BLOCK_SIZE_BYTES}. It is
   * {@link #MAX_BLOCK_SIZE_BYTES} plus {@link #FRAMING_OVERHEAD_BYTES}, that is 2,097,182 bytes,
   * and it is the figure any component sizing a network-facing resource must use: a rate limiter
   * whose burst allowance were only {@link #MAX_BLOCK_SIZE_BYTES} could not represent the largest
   * legal frame at all, and would either refuse it forever or let the surplus bytes go uncharged.
   */
  public static final int MAX_ENCODED_FRAME_BYTES = MAX_BLOCK_SIZE_BYTES + FRAMING_OVERHEAD_BYTES;

  private final long checksum;
  private final byte[] payload;

  /**
   * Whether this block has already been shown to match the checksum it arrived with.
   *
   * A received block is verified on the channel thread, so that a corrupt one can be replaced by a
   * replay before a task ever sees it, and the same block is verified again where its records are
   * turned into values. Recomputing the checksum there covers the same immutable bytes and the same
   * immutable header, so it can only ever reach the same answer -- at the cost of a second pass
   * over every payload byte of the whole shuffle. A successful verification is therefore remembered
   * here and the second pass is skipped.
   *
   * Only success is remembered: a block that fails verification is never marked, so the repair path
   * that asks for a replay behaves exactly as it did. The field is deliberately not volatile,
   * because publication between the two threads happens through the receive queue, which already
   * establishes the ordering; a reader that somehow saw the stale value would recompute the
   * checksum and reach the same answer, so the worst case is the cost this exists to avoid rather
   * than a wrong result.
   *
   * The one precondition is that the payload array is not mutated in place behind the block's back
   * after a successful verification. Every route into this class either copies the array it is
   * given or adopts an array allocated for the block and never touched again.
   */
  private transient boolean checksumVerified;

  /**
   * The one constructor that assigns the payload field, and therefore the single place where
   * ownership of the array is decided.
   *
   * Every other route into an instance funnels through here, which is what makes the size cap and
   * the copy policy impossible to bypass. {@code ownsPayload} says whether this block may keep the
   * array it was handed: true only when the caller has surrendered an array that is reachable from
   * nowhere else, false whenever the array came from outside, in which case it is cloned so that
   * later mutation by its original holder cannot reach the block's bytes.
   *
   * @param protocolVersion the wire revision this message was encoded with
   * @param shuffleId identifier of the shuffle this block belongs to
   * @param mapId identifier of the map task whose output this block carries
   * @param partitionId identifier of the reduce partition this block belongs to
   * @param sequenceNumber position of this block within its partition's stream
   * @param checksum the CRC32C value covering this block
   * @param payload the block's bytes
   * @param ownsPayload true to adopt the array as-is, false to store a defensive copy
   */
  private DataBlockMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long checksum,
      byte[] payload,
      boolean ownsPayload) {
    super(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber);
    this.checksum = checksum;
    // Validated here, in the one constructor every other route funnels through, so that neither a
    // producer nor the decoder can construct an over-size block.
    byte[] checked = checkPayload(payload);
    this.payload = ownsPayload ? checked : checked.clone();
  }

  /**
   * Creates a data block carrying an explicit protocol version, which is how a decoded block keeps
   * the version it actually arrived with instead of silently adopting this build's own.
   *
   * @param protocolVersion the wire revision this message was encoded with
   * @param shuffleId identifier of the shuffle this block belongs to
   * @param mapId identifier of the map task whose output this block carries
   * @param partitionId identifier of the reduce partition this block belongs to
   * @param sequenceNumber position of this block within its partition's stream, counted from zero
   * @param checksum the CRC32C value covering this block
   * @param payload the block's bytes; must not be null and must not exceed
   *                {@link #MAX_BLOCK_SIZE_BYTES}. The array is copied, so the caller may continue
   *                to use and mutate it; see {@link #withOwnedPayload} to avoid the copy
   * @throws NullPointerException if payload is null
   * @throws IllegalArgumentException if payload is longer than {@link #MAX_BLOCK_SIZE_BYTES}
   */
  public DataBlockMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long checksum,
      byte[] payload) {
    this(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber, checksum, payload, false);
  }

  /**
   * Creates a data block stamped with this build's protocol version. This is the constructor a
   * producer uses when it already holds a checksum for the payload.
   *
   * @param shuffleId identifier of the shuffle this block belongs to
   * @param mapId identifier of the map task whose output this block carries
   * @param partitionId identifier of the reduce partition this block belongs to
   * @param sequenceNumber position of this block within its partition's stream, counted from zero
   * @param checksum the CRC32C value covering this block
   * @param payload the block's bytes; must not be null and must not exceed
   *                {@link #MAX_BLOCK_SIZE_BYTES}. The array is copied, so the caller may continue
   *                to use and mutate it; see {@link #withOwnedPayload} to avoid the copy
   * @throws NullPointerException if payload is null
   * @throws IllegalArgumentException if payload is longer than {@link #MAX_BLOCK_SIZE_BYTES}
   */
  public DataBlockMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long checksum,
      byte[] payload) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, sequenceNumber, checksum, payload,
      false);
  }

  /**
   * Creates a data block from a header just read off the wire. Passing the header as a single value
   * rather than as five positional arguments removes any chance of transposing {@code shuffleId}
   * and {@code partitionId} on the way in.
   *
   * @param header the decoded header; must not be null
   * @param checksum the CRC32C value the producer computed for the block
   * @param payload the block's bytes; must not be null and must not exceed
   *                {@link #MAX_BLOCK_SIZE_BYTES}. The array is copied, so the caller may continue
   *                to use and mutate it; see {@link #withOwnedPayload} to avoid the copy
   * @throws NullPointerException if header or payload is null
   * @throws IllegalArgumentException if payload is longer than {@link #MAX_BLOCK_SIZE_BYTES}
   */
  public DataBlockMessage(Header header, long checksum, byte[] payload) {
    // The base constructor rejects a null header, so the cap check below is the only guard this
    // constructor has to add of its own.
    this(Objects.requireNonNull(header, "header").protocolVersion(), header.shuffleId(),
      header.mapId(), header.partitionId(), header.sequenceNumber(), checksum, payload, false);
  }

  /**
   * Creates a data block that adopts the given array outright, without copying it.
   *
   * This is the ownership-transfer entry point, and calling it is a promise: the caller must never
   * read from or write to {@code payload} again, because the block now treats those bytes as its
   * own immutable content and has checksummed them on that basis. Break the promise and the block's
   * checksum becomes a statement about content it no longer holds, which is precisely the
   * corruption the checksum exists to detect -- except that it would now be introduced locally,
   * after verification, where nothing would catch it.
   *
   * It exists because the copy the public constructors make is pure waste in the two situations
   * where the array is provably private already: {@link #decode(ByteBuf)}, whose array was freshly
   * allocated by the decoder and is reachable from nowhere else, and a producer that has just
   * serialized records into a buffer it will not reuse. At up to two mebibytes a block, on the
   * shuffle hot path, that copy is worth avoiding where it is genuinely unnecessary -- but only
   * where, which is why this is a named, {@code @Private} factory rather than the default behaviour
   * of a constructor.
   *
   * @param protocolVersion the wire revision this message was encoded with
   * @param shuffleId identifier of the shuffle this block belongs to
   * @param mapId identifier of the map task whose output this block carries
   * @param partitionId identifier of the reduce partition this block belongs to
   * @param sequenceNumber position of this block within its partition's stream, counted from zero
   * @param checksum the CRC32C value covering this block
   * @param payload an array the caller surrenders; must not be null and must not exceed
   *                {@link #MAX_BLOCK_SIZE_BYTES}
   * @return a data block backed directly by the given array
   * @throws NullPointerException if payload is null
   * @throws IllegalArgumentException if payload is longer than {@link #MAX_BLOCK_SIZE_BYTES}
   */
  @Private
  public static DataBlockMessage withOwnedPayload(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      long checksum,
      byte[] payload) {
    return new DataBlockMessage(protocolVersion, shuffleId, mapId, partitionId, sequenceNumber,
      checksum, payload, true);
  }

  /**
   * Creates a data block and computes its checksum, for the common producer-side case where the
   * value is not already in hand.
   *
   * The computation is routed through {@link StreamingShuffleChecksum#computeBlock(int, long, int,
   * long, byte[])} rather than performed here, so that producer and consumer are provably running
   * the
   * same arithmetic over the same bytes, and so that the value binds the payload to the identity
   * this block is being sent under rather than covering the payload in isolation.
   *
   * The checksum is computed over the caller's array and the block then stores a copy of it. Those
   * are the same bytes at the moment of construction, so the value is correct for what the block
   * holds; and because the block's copy is thereafter unreachable from outside, the value stays
   * correct however the caller goes on to use its own array.
   *
   * @param shuffleId identifier of the shuffle this block belongs to
   * @param mapId identifier of the map task whose output this block carries
   * @param partitionId identifier of the reduce partition this block belongs to
   * @param sequenceNumber position of this block within its partition's stream, counted from zero
   * @param payload the block's bytes; must not be null and must not exceed
   *                {@link #MAX_BLOCK_SIZE_BYTES}. The array is copied
   * @return a data block whose checksum covers its metadata and payload
   * @throws NullPointerException if payload is null
   * @throws IllegalArgumentException if payload is longer than {@link #MAX_BLOCK_SIZE_BYTES}
   */
  public static DataBlockMessage withComputedChecksum(
      int shuffleId,
      long mapId,
      int partitionId,
      long sequenceNumber,
      byte[] payload) {
    // Checked before the payload is read, so an over-size block is rejected without first paying
    // for a checksum over bytes that are about to be discarded.
    checkPayload(payload);
    return new DataBlockMessage(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId,
      sequenceNumber,
      StreamingShuffleChecksum.computeBlock(shuffleId, mapId, partitionId, sequenceNumber, payload),
      payload, false);
  }

  /**
   * The CRC32C value the producer computed over this block's payload.
   *
   * A consumer that finds a mismatch reports this number as the expected value alongside the one it
   * recomputed, so it stays reachable rather than being folded into a bare verdict.
   *
   * @return the checksum carried with this block
   */
  public long checksum() {
    return checksum;
  }

  /**
   * This block's bytes, as a read-only buffer over the block's own array.
   *
   * This is the accessor to reach for. It costs no copy, so reading a two-mebibyte block is as
   * cheap as it was when the array itself was handed out, while the returned buffer's {@code put}
   * methods throw {@link java.nio.ReadOnlyBufferException} and its {@code array()} is inaccessible,
   * so the block's content cannot be reached through it. Each call returns an independent buffer
   * with its own position and limit, so concurrent readers do not interfere with one another and no
   * reader can disturb another's progress.
   *
   * Use {@link #copyPayload()} instead when a caller needs a {@code byte[]} it may mutate.
   *
   * @return a read-only view of this block's payload, never null
   */
  public ByteBuffer payloadBuffer() {
    return ByteBuffer.wrap(payload).asReadOnlyBuffer();
  }

  /**
   * A fresh copy of this block's bytes, which the caller owns and may mutate freely.
   *
   * This allocates and copies up to {@link #MAX_BLOCK_SIZE_BYTES} bytes every call, so prefer
   * {@link #payloadBuffer()} on any path that merely reads. It exists for the callers that
   * genuinely need a mutable array -- handing bytes to an API that only accepts {@code byte[]}, or
   * building on top of the payload -- and giving them a copy is what lets those callers exist at
   * all without the block's own content being exposed to mutation.
   *
   * @return a new array holding this block's payload, never null
   */
  public byte[] copyPayload() {
    return payload.clone();
  }

  /**
   * Number of payload bytes this block carries, which is what a diagnostic wants in place of the
   * bytes themselves.
   *
   * @return the payload length, between zero and {@link #MAX_BLOCK_SIZE_BYTES} inclusive
   */
  public int payloadLength() {
    return payload.length;
  }

  /**
   * Recomputes this block's checksum and compares it with the value the producer sent, which is the
   * check a consumer runs before making the block's records visible.
   *
   * The recomputation covers the payload together with the shuffle id, map id, partition id and
   * sequence number carried in <em>this</em> block's header, so the check answers two questions at
   * once:
   * were the payload bytes delivered intact, and is this block addressed where its producer
   * addressed it. A block whose header was rewritten in flight fails here even though every payload
   * byte survived, which a payload-only checksum could not detect.
   *
   * When this returns false the caller needs both numbers for its diagnostic: the expected value is
   * {@link #checksum()} and the recomputed one is {@code
   * StreamingShuffleChecksum.computeBlock(shuffleId(), mapId(), partitionId(), sequenceNumber(),
   * copyPayload())}.
   *
   * @return true if the block matches the checksum it arrived with, false if it is corrupt or
   *         misaddressed
   */
  public boolean verifyChecksum() {
    if (checksumVerified) {
      return true;
    }
    boolean intact = StreamingShuffleChecksum.verifyBlock(
      shuffleId(), mapId(), partitionId(), sequenceNumber(), payload, checksum);
    if (intact) {
      checksumVerified = true;
    }
    return intact;
  }

  @Override
  protected StreamingShuffleMessageType type() {
    return StreamingShuffleMessageType.DATA_BLOCK;
  }

  @Override
  public int hashCode() {
    // Combined the way the sibling messages with byte-array fields combine theirs. Handing the
    // array to Objects.hash would hash it by identity, so two blocks holding equal bytes in
    // distinct arrays would disagree, and a decoded block would never match the one it came from.
    int headerAndChecksum = headerHashCode() * 41 + Long.hashCode(checksum);
    return headerAndChecksum * 41 + Arrays.hashCode(payload);
  }

  @Override
  public String toString() {
    // The payload bytes are deliberately not rendered: a block runs to two mebibytes, and this
    // string reaches error messages and diagnostics, where its length has to stay bounded.
    return "DataBlockMessage[" + headerToString() + ",checksum=" + checksum +
        ",payloadLength=" + payload.length + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof DataBlockMessage o) {
      return headerEquals(o)
        && checksum == o.checksum
        && Arrays.equals(payload, o.payload);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    // 17 (header) + 8 (checksum) + 4 (length prefix) + payload.length == 29 + payload.length
    return HEADER_ENCODED_LENGTH + CHECKSUM_ENCODED_LENGTH +
        Encoders.ByteArrays.encodedLength(payload);
  }

  @Override
  public void encode(ByteBuf buf) {
    // The header always goes first, and in the order the base class owns, so that it cannot drift
    // apart from the read in decode below.
    encodeHeader(buf);
    buf.writeLong(checksum);
    Encoders.ByteArrays.encode(buf, payload);
  }

  /**
   * Reads a data block back off the wire, in the same field order {@link #encode(ByteBuf)} wrote.
   *
   * The buffer's bytes arrive from a remote peer, so every length this method depends on is checked
   * rather than assumed, and the payload's length prefix is validated before anything is allocated
   * for it. The block is then built through a constructor, so the size cap applies to a decoded
   * block exactly as it does to a constructed one.
   *
   * It is package-private on purpose. {@code StreamingShuffleMessage.Decoder.fromByteBuffer} is the
   * only entry point a peer's bytes may take into the protocol, and that guarantee holds only if a
   * caller outside this package cannot reach a concrete decoder and thereby skip the frame bounds
   * and the exact-consumption requirement the central decoder enforces.
   *
   * @param buf a buffer positioned at the first header byte of an encoded data block
   * @return the decoded block
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if the frame is truncated, if its header carries a negative
   *                                 shuffle id, partition id or sequence number, or if the
   *                                 payload's length prefix is negative, exceeds
   *                                 {@link #MAX_BLOCK_SIZE_BYTES}, or does not account for exactly
   *                                 the bytes the buffer holds
   */
  static DataBlockMessage decode(ByteBuf buf) {
    // readHeader rejects a null buffer, a header too short to be read, an unsupported protocol
    // version and a negative identifier.
    Header header = readHeader(buf);
    // At least the checksum and the payload's length prefix must be present. Requiring both up
    // front means the checksum is only read once there is a well-formed body behind it.
    int minimumBody = CHECKSUM_ENCODED_LENGTH + PAYLOAD_LENGTH_PREFIX_LENGTH;
    if (buf.readableBytes() < minimumBody) {
      throw new IllegalArgumentException("Truncated streaming shuffle data block: expected at " +
        "least " + minimumBody + " body byte(s) but only " + buf.readableBytes() + " remain");
    }
    long checksum = buf.readLong();
    checkPayloadLengthPrefix(buf);
    byte[] payload = Encoders.ByteArrays.decode(buf);
    // The array was allocated by the decoder a line ago and is reachable from nowhere else, so
    // handing ownership to the block is safe and saves copying up to two mebibytes per block on the
    // receive path.
    return withOwnedPayload(header.protocolVersion(), header.shuffleId(), header.mapId(),
      header.partitionId(), header.sequenceNumber(), checksum, payload);
  }

  /**
   * Validates the payload's length prefix without consuming it, so that {@code
   * Encoders.ByteArrays.decode} is only ever reached with a length that is known to be
   * non-negative, within the block size cap, and actually backed by bytes in the buffer.
   *
   * This is the guard that keeps a corrupt or hostile four-byte prefix from becoming an arbitrarily
   * large allocation: the array is sized from the prefix with no bound of its own, so the bound has
   * to be applied here, ahead of it. The read is absolute, at an explicit index, which leaves the
   * reader index exactly where the delegate expects to find it.
   *
   * @param buf a buffer positioned at the payload's length prefix
   * @throws IllegalArgumentException if the prefix is absent, negative, over-size, or does not
   *                                 account for exactly the bytes that follow it
   */
  private static void checkPayloadLengthPrefix(ByteBuf buf) {
    if (buf.readableBytes() < PAYLOAD_LENGTH_PREFIX_LENGTH) {
      throw new IllegalArgumentException("Truncated streaming shuffle data block: expected " +
        PAYLOAD_LENGTH_PREFIX_LENGTH + " payload length byte(s) but only " + buf.readableBytes() +
        " byte(s) remain");
    }
    int length = buf.getInt(buf.readerIndex());
    if (length < 0) {
      throw new IllegalArgumentException(
        "Negative streaming shuffle data block payload length: " + length);
    }
    if (length > MAX_BLOCK_SIZE_BYTES) {
      throw new IllegalArgumentException("Streaming shuffle data block payload length of " +
        length + " byte(s) exceeds the maximum block size of " + MAX_BLOCK_SIZE_BYTES +
        " byte(s)");
    }
    // Comparing against the bytes left after the prefix rather than against the whole readable
    // count keeps the prefix itself out of the payload's budget.
    int available = buf.readableBytes() - PAYLOAD_LENGTH_PREFIX_LENGTH;
    // Exactly, not at least. A shortfall is a truncated frame, and a surplus is bytes the codec
    // would never look at: accepting either would let a peer append content that survives the
    // message boundary unparsed, and would hide a disagreement between what encode() wrote and what
    // decode() consumed. Both directions are framing errors, so both are refused here, and the
    // message distinguishes them so the cause is not left to guesswork.
    if (available != length) {
      String problem = available < length ? "Truncated" : "Over-long";
      throw new IllegalArgumentException(problem + " streaming shuffle data block payload: the " +
        "length prefix declares " + length + " byte(s) but " + available + " byte(s) remain");
    }
  }

  /**
   * Rejects a null payload and one that exceeds the block size cap, returning the array unchanged
   * so that it can be assigned straight to the field.
   *
   * The comparison is strictly greater than the cap: a payload of exactly {@link
   * #MAX_BLOCK_SIZE_BYTES} bytes is a full block, not an over-size one.
   *
   * @param payload the candidate payload
   * @return payload, unchanged
   * @throws NullPointerException if payload is null
   * @throws IllegalArgumentException if payload is longer than {@link #MAX_BLOCK_SIZE_BYTES}
   */
  private static byte[] checkPayload(byte[] payload) {
    Objects.requireNonNull(payload, "payload");
    if (payload.length > MAX_BLOCK_SIZE_BYTES) {
      throw new IllegalArgumentException("Streaming shuffle data block payload is " +
        payload.length + " byte(s), which exceeds the maximum block size of " +
        MAX_BLOCK_SIZE_BYTES + " byte(s)");
    }
    return payload;
  }
}
