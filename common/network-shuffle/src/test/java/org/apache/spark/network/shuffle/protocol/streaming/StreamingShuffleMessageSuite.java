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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.annotation.Private;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Wire-contract tests for the streaming shuffle protocol message family.
 *
 * A wire format is a contract between two executors that may be built from different revisions of
 * this code, so every property a peer is entitled to rely on is asserted here rather than left to
 * the implementation: the exact byte layout and field order of the shared header and of each body,
 * the encoded and framed lengths, the type discriminator, the CRC32C algorithm and its binding to a
 * block's metadata, the two-mebibyte payload cap alongside the distinct encoded-frame cap, the
 * domain of every header and body field in both directions, and the equality and rendering
 * contracts that diagnostics depend on.
 *
 * Hostile and malformed input is exercised as deliberately as the happy path, because the decoder
 * is reachable from a remote peer: over-long frames, trailing bytes, truncation at every field
 * boundary, unknown type bytes, incompatible protocol versions, and payload length prefixes that
 * do not account for the bytes behind them are each refused, and each refusal is asserted to name
 * its cause so that a failure in production is attributable.
 *
 * The suite reads no clock and draws no random numbers -- payloads are filled from their index and
 * timestamps are literals -- so a failure here is always reproducible.
 */
public class StreamingShuffleMessageSuite {

  /**
   * Header values used throughout, chosen distinct and non-zero so that a field transposed between
   * shuffleId, partitionId and sequenceNumber cannot pass unnoticed.
   */
  private static final int SHUFFLE_ID = 7;
  private static final int PARTITION_ID = 3;
  /**
   * A map task identifier. Non-zero and distinct from every other constant here, so that a header
   * field silently dropped from an encoder or a decoder shows up as a mismatch rather than
   * coinciding with a neighbour's value.
   */
  private static final long MAP_ID = 11L;
  private static final long SEQUENCE_NUMBER = 42L;

  /**
   * A consumer session token. Distinct from every other constant here and far outside the range of
   * any of them, so a heartbeat that encoded its sequence number where its token belongs -- or the
   * reverse -- shows up as a mismatch rather than coinciding with a neighbour's value.
   */
  private static final long CONSUMER_TOKEN = 0x51A5_1D3E_7C0F_0042L;

  /**
   * A fixed timestamp literal. The heartbeat's timestamp is caller-supplied precisely so that this
   * suite never has to read a clock, which is what keeps it deterministic.
   */
  private static final long TIMESTAMP_MS = 1700000000000L;

  /** A protocol version this build does not speak, used to exercise the compatibility check. */
  private static final byte UNSUPPORTED_VERSION = (byte) 99;

  /** A message type id no constant uses, used to exercise unknown-discriminator handling. */
  private static final byte UNKNOWN_TYPE_ID = (byte) 9;

  /**
   * Every buffer this suite allocated, in allocation order, so that each is released exactly once.
   *
   * A {@link ByteBuf} is reference counted, and an unreleased one is a leak whether it is heap
   * backed or not: the JVM reclaims the array, but Netty's leak detector and any pooled allocator
   * this suite is later run against do not agree that nothing was lost. Tracking them here rather
   * than releasing at each site is deliberate -- a release written at the end of a test is a
   * release that a test throwing half way through skips, and these tests provoke exceptions on
   * purpose.
   */
  private final List<ByteBuf> ownedBuffers = new ArrayList<>();

  /**
   * Releases every buffer the test allocated, and asserts that this suite owned each of them alone.
   *
   * The reference count is checked on both sides of the release. A count other than one before it
   * means something retained the buffer or released it already -- either way the ownership contract
   * this message family is built on has been broken, and a decoder that retained a frame's buffer
   * is exactly the defect that copying the payload out at decode time exists to prevent. A count of
   * zero after it is the release itself, observed rather than assumed.
   */
  @AfterEach
  public void releaseOwnedBuffers() {
    try {
      for (ByteBuf buf : ownedBuffers) {
        assertEquals(1, buf.refCnt(),
            "this suite owns every buffer it allocates, so nothing may have retained or released "
                + "it before teardown");
        assertTrue(buf.release(), "the last reference must be the one released here");
        assertEquals(0, buf.refCnt(), "a released buffer holds no reference");
      }
    } finally {
      ownedBuffers.clear();
    }
  }

  /** An empty buffer whose release this suite owns. */
  private ByteBuf buffer() {
    return track(Unpooled.buffer());
  }

  /** A buffer of an exact capacity whose release this suite owns. */
  private ByteBuf buffer(int capacity) {
    return track(Unpooled.buffer(capacity));
  }

  /** A buffer wrapping bytes this suite holds, whose release this suite owns. */
  private ByteBuf wrapped(byte[] bytes) {
    return track(Unpooled.wrappedBuffer(bytes));
  }

  private ByteBuf track(ByteBuf buf) {
    ownedBuffers.add(buf);
    return buf;
  }

  // Group 1: round-trip encode/decode for every message type, with exact-literal lengths.

  @Test
  public void testDataBlockMessageEncodeDecode() {
    // EMPTY_DATA_BLOCK_BYTES + payload.length for a data block: 17 header + 8 producer id
    // + 8 checksum + 4 length prefix + payload.
    for (int payloadLength : new int[] {0, 1, 1024, 8192}) {
      byte[] payload = payload(payloadLength);
      DataBlockMessage message = DataBlockMessage.withComputedChecksum(
          SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, payload);
      int len = message.encodedLength();
      assertEquals(EMPTY_DATA_BLOCK_BYTES + payloadLength, len);
      ByteBuf buf = buffer(len);
      message.encode(buf);
      // Encodable requires encode to write exactly encodedLength() bytes; with an exact-size buffer
      // that is precisely what no writable bytes remaining means.
      assertEquals(0, buf.writableBytes());

      DataBlockMessage decoded = DataBlockMessage.decode(buf);
      assertEquals(message, decoded);
      assertEquals(payloadLength, decoded.payloadLength());
      assertArrayEquals(payload, decoded.copyPayload());
    }
  }

  @Test
  public void testAckMessageEncodeDecode() {
    AckMessage message = new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 40L);
    int len = message.encodedLength();
    // The specified fixed size of a control message: seventeen header bytes and eight of producer
    // id. The literal is the contract; it is deliberately not derived from the implementation.
    assertEquals(CONTROL_MESSAGE_BYTES, len);
    assertEquals(25, len);
    // The position acknowledged travels as the next position expected, one above it.
    assertEquals(41L, message.sequenceNumber());
    ByteBuf buf = buffer(len);
    message.encode(buf);
    assertEquals(0, buf.writableBytes());

    AckMessage decoded = AckMessage.decode(buf);
    assertEquals(message, decoded);
    assertEquals(40L, decoded.consumerPosition());
    assertEquals(MAP_ID, decoded.mapId());
  }

  @Test
  public void testHeartbeatMessageEncodeDecode() {
    HeartbeatMessage message = new HeartbeatMessage(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, CONSUMER_TOKEN);
    int len = message.encodedLength();
    // Seventeen header bytes, eight of producer id and eight of consumer session token: one fixed
    // size, with no variable-length field anywhere in the streaming control protocol for a peer to
    // choose. The token is a number precisely so that this stays true -- the identity it stands for
    // is a string whose length the consumer would otherwise decide.
    assertEquals(HEARTBEAT_BYTES, len);
    assertEquals(33, len);
    ByteBuf buf = buffer(len);
    message.encode(buf);
    assertEquals(0, buf.writableBytes());

    HeartbeatMessage decoded = HeartbeatMessage.decode(buf);
    assertEquals(message, decoded);
    assertEquals(SEQUENCE_NUMBER, decoded.sequenceNumber());
    assertEquals(MAP_ID, decoded.mapId());
    assertEquals(CONSUMER_TOKEN, decoded.consumerToken());
  }

  @Test
  public void testHeartbeatMessageWithoutATokenRoundTripsAtTheSameWidth() {
    // A heartbeat that declares nothing -- a producer's own, or a peer of an earlier revision --
    // occupies exactly the same bytes and reports the one reserved value. The width may not depend
    // on whether an identity was declared, or a producer's framing budget would depend on what its
    // peers chose to say.
    HeartbeatMessage message = heartbeatWithoutToken();
    assertEquals(HEARTBEAT_BYTES, message.encodedLength());
    assertEquals(HeartbeatMessage.NO_CONSUMER_TOKEN, message.consumerToken());
    assertEquals(0L, HeartbeatMessage.NO_CONSUMER_TOKEN);

    HeartbeatMessage decoded = (HeartbeatMessage) roundTrip(message);
    assertEquals(message, decoded);
    assertEquals(HeartbeatMessage.NO_CONSUMER_TOKEN, decoded.consumerToken());
    // Declaring an identity and declaring none are different heartbeats, so the token takes part in
    // equality: a producer that treated them as equal could adopt an identity never sent.
    assertNotEquals(heartbeat(), message);
    assertNotEquals(heartbeat().hashCode(), message.hashCode());
  }

  @Test
  public void testHeartbeatConsumerTokenDomainIsEnforcedOnBothRoutes() {
    // The domain is the non-negative longs, and it is the same domain in both directions: a locally
    // built heartbeat and a decoded one are refused on identical terms, so the codec cannot encode
    // a value it would then refuse to read back.
    assertThrows(IllegalArgumentException.class,
        () -> new HeartbeatMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, -1L));
    assertThrows(IllegalArgumentException.class,
        () -> new HeartbeatMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER,
            Long.MIN_VALUE));
    assertThrows(IllegalArgumentException.class,
        () -> new HeartbeatMessage(header(), MAP_ID, -1L));
    assertThrows(IllegalArgumentException.class, () -> decodeHeartbeatWithToken(-1L));
    assertThrows(IllegalArgumentException.class,
        () -> decodeHeartbeatWithToken(Long.MIN_VALUE));
    // The reserved value and the largest legal one are both accepted, on both routes.
    assertDoesNotThrow(() -> decodeHeartbeatWithToken(HeartbeatMessage.NO_CONSUMER_TOKEN));
    assertDoesNotThrow(() -> decodeHeartbeatWithToken(Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, new HeartbeatMessage(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, Long.MAX_VALUE).consumerToken());
  }

  @Test
  public void testEveryControlMessageIsFixedAtTheSpecifiedSize() {
    // Every control message has a size this build fixes, and none has a size a peer chooses.
    // Asserted against literals rather than against the production constants, so that a change to
    // the wire layout has to be a deliberate change to this contract as well.
    for (StreamingShuffleMessage message : fixedSizeMessages()) {
      assertEquals(CONTROL_MESSAGE_BYTES, message.encodedLength(),
          message.getClass().getSimpleName());
      assertEquals(CONTROL_MESSAGE_BYTES + FRAME_TYPE_PREFIX_BYTES,
          message.toByteBuffer().remaining(), message.getClass().getSimpleName());
    }
    // A heartbeat is wider by exactly the eight bytes of the consumer session token, and is that
    // width whether it declares one or not.
    assertEquals(33, heartbeat().encodedLength());
    assertEquals(33, heartbeatWithoutToken().encodedLength());
    assertEquals(34, heartbeat().toByteBuffer().remaining());
    assertEquals(34, heartbeatWithoutToken().toByteBuffer().remaining());
    // A retransmission request is wider by exactly the eight bytes of the inclusive upper bound
    // that lets one frame name a whole repair window. It is still a fixed width, and it is fixed
    // whatever window the request names.
    RetransmitRequestMessage single =
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER);
    RetransmitRequestMessage run =
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER,
            SEQUENCE_NUMBER + 100L);
    assertEquals(33, single.encodedLength());
    assertEquals(33, run.encodedLength());
    assertEquals(34, single.toByteBuffer().remaining());
    assertEquals(34, run.toByteBuffer().remaining());
    // And a data block is the header, the producer id, the checksum, the length prefix and the
    // payload -- thirty-seven bytes before a single payload byte.
    assertEquals(EMPTY_DATA_BLOCK_BYTES, dataBlock(0).encodedLength());
    assertEquals(EMPTY_DATA_BLOCK_BYTES + 1024, dataBlock(1024).encodedLength());
  }

  @Test
  public void testRetransmitRequestMessageEncodeDecode() {
    RetransmitRequestMessage message =
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER);
    int len = message.encodedLength();
    assertEquals(RETRANSMIT_REQUEST_BYTES, len);
    assertEquals(33, len);
    ByteBuf buf = buffer(len);
    message.encode(buf);
    assertEquals(0, buf.writableBytes());

    RetransmitRequestMessage decoded = RetransmitRequestMessage.decode(buf);
    assertEquals(message, decoded);
    // A request built from one position states it as the inclusive single-element window it is, so
    // both ends coincide and the count is one.
    assertEquals(SEQUENCE_NUMBER, decoded.firstSequenceNumber());
    assertEquals(SEQUENCE_NUMBER, decoded.lastSequenceNumber());
    assertEquals(1L, decoded.blockCount());
    assertTrue(decoded.contains(SEQUENCE_NUMBER));
    assertFalse(decoded.contains(SEQUENCE_NUMBER - 1L));
    assertFalse(decoded.contains(SEQUENCE_NUMBER + 1L));
    assertEquals(MAP_ID, decoded.mapId());
  }

  @Test
  public void testRetransmitRequestMessageCarriesAMultiBlockWindowThroughARoundTrip() {
    // The whole point of the field: one frame names a run of positions, so a multi-block gap is one
    // repair the producer charges one attempt and one backoff for. A frame per position would have
    // the producer defer every sibling behind the first one's pause, spend the five-attempt budget
    // re-asking for the first position and escalate to a stage recomputation while still holding
    // every byte that was asked for.
    RetransmitRequestMessage window =
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 7L, 11L);
    assertEquals(RETRANSMIT_REQUEST_BYTES, window.encodedLength());
    RetransmitRequestMessage decoded = (RetransmitRequestMessage) roundTrip(window);
    assertEquals(window, decoded);
    assertEquals(7L, decoded.firstSequenceNumber());
    assertEquals(11L, decoded.lastSequenceNumber());
    assertEquals(5L, decoded.blockCount());
    for (long position = 7L; position <= 11L; position++) {
      assertTrue(decoded.contains(position), "position " + position + " must be covered");
    }
    assertFalse(decoded.contains(6L));
    assertFalse(decoded.contains(12L));
    // Two windows that share a lower bound are different requests, so the upper bound has to
    // participate in identity as well as in the encoding.
    RetransmitRequestMessage narrower =
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 7L, 10L);
    assertNotEquals(window, narrower);
    assertNotEquals(window.hashCode(), narrower.hashCode());
    assertTrue(window.toString().contains("lastSequenceNumber=11"));
  }

  @Test
  public void testStreamTerminationMessageEncodeDecode() {
    StreamTerminationMessage message =
        new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 40L);
    int len = message.encodedLength();
    assertEquals(CONTROL_MESSAGE_BYTES, len);
    assertEquals(25, len);
    ByteBuf buf = buffer(len);
    message.encode(buf);
    assertEquals(0, buf.writableBytes());

    StreamTerminationMessage decoded = StreamTerminationMessage.decode(buf);
    assertEquals(message, decoded);
    // The announced total is the position the terminator sits at, so the two cannot disagree.
    assertEquals(40L, decoded.totalBlocks());
    assertEquals(40L, decoded.sequenceNumber());
    assertEquals(MAP_ID, decoded.mapId());
  }

  // Group 2: header integrity.

  @Test
  public void testHeaderLayoutConstants() {
    // The specified header: one version byte, a four-byte shuffle id, a four-byte partition id and
    // an eight-byte sequence number. Seventeen bytes, and nothing else in it.
    assertEquals(HEADER_BYTES, StreamingShuffleMessage.HEADER_ENCODED_LENGTH);
    assertEquals(17, StreamingShuffleMessage.HEADER_ENCODED_LENGTH);
    assertEquals(1 + 4 + 4 + 8, StreamingShuffleMessage.HEADER_ENCODED_LENGTH);
    // The producer id is the first body field of every message, and a control message that carries
    // no field of its own is the header plus that field: twenty-five bytes, twenty-six once framed.
    // A retransmission request adds the eight bytes of its window's upper bound, which is a width
    // this build fixes rather than one a peer chooses.
    assertEquals(8, StreamingShuffleMessage.PRODUCER_ID_ENCODED_LENGTH);
    assertEquals(CONTROL_MESSAGE_BYTES, StreamingShuffleMessage.CONTROL_MESSAGE_ENCODED_LENGTH);
    assertEquals(25, StreamingShuffleMessage.CONTROL_MESSAGE_ENCODED_LENGTH);
    assertEquals(RETRANSMIT_REQUEST_BYTES,
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 4L, 6L).encodedLength());
    // A heartbeat adds the eight bytes of the consumer session token, likewise a fixed width.
    assertEquals(HEARTBEAT_BYTES, heartbeat().encodedLength());
    assertEquals(33, heartbeat().encodedLength());
    assertEquals(1, StreamingShuffleMessage.FRAME_TYPE_PREFIX_LENGTH);
    assertEquals(1, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION);
  }

  @Test
  public void testEveryHeaderFieldSurvivesRoundTrip() {
    for (StreamingShuffleMessage message : oneOfEachType()) {
      StreamingShuffleMessage decoded =
          StreamingShuffleMessage.Decoder.fromByteBuffer(message.toByteBuffer());
      assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, decoded.protocolVersion());
      assertEquals(SHUFFLE_ID, decoded.shuffleId());
      assertEquals(MAP_ID, decoded.mapId());
      assertEquals(PARTITION_ID, decoded.partitionId());
      // Every type states a position, and each states its own: a block and a heartbeat the position
      // they concern, an acknowledgement the next position expected, a request the position asked
      // for, and a terminator the block total. The value is whatever the message was built with.
      assertEquals(message.sequenceNumber(), decoded.sequenceNumber(),
          message.getClass().getSimpleName());
      assertTrue(decoded.sequenceNumber() >= 0L);
    }
  }

  @Test
  public void testDecodedMessageKeepsTheVersionItArrivedWith() {
    // A message built with an explicit version must round-trip that version rather than silently
    // adopting this build's own, which is what allows a mismatch to be reported precisely.
    AckMessage message = new AckMessage(
        StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, SHUFFLE_ID, MAP_ID,
        PARTITION_ID, 40L);
    assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, message.protocolVersion());
    StreamingShuffleMessage decoded =
        StreamingShuffleMessage.Decoder.fromByteBuffer(message.toByteBuffer());
    assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, decoded.protocolVersion());
  }

  @Test
  public void testPeekProtocolVersionIsNonDestructive() {
    for (StreamingShuffleMessage message : oneOfEachType()) {
      ByteBuffer framed = message.toByteBuffer();
      int position = framed.position();
      int remaining = framed.remaining();

      assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
          StreamingShuffleMessage.peekProtocolVersion(framed));
      assertEquals(position, framed.position());
      assertEquals(remaining, framed.remaining());
      // The assertion that really proves the peek was non-destructive: the very same buffer is
      // still decodable afterwards.
      assertEquals(message, StreamingShuffleMessage.Decoder.fromByteBuffer(framed));
    }
  }

  @Test
  public void testPeekProtocolVersionRejectsShortBuffer() {
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.peekProtocolVersion(ByteBuffer.allocate(1)));
  }

  @Test
  public void testProtocolVersionCompatibility() {
    assertTrue(StreamingShuffleMessage.isCompatible(
        StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION));
    assertDoesNotThrow(() -> StreamingShuffleMessage.checkProtocolVersion(
        StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION));

    assertFalse(StreamingShuffleMessage.isCompatible(UNSUPPORTED_VERSION));
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.checkProtocolVersion(UNSUPPORTED_VERSION));
  }

  @Test
  public void testDecoderRejectsIncompatibleProtocolVersion() {
    // The version sits at a fixed offset of one byte into every framed message, so it can be
    // rewritten in place to simulate a peer built against a different revision.
    byte[] framed = toByteArray(new AckMessage(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, 40L).toByteBuffer());
    framed[1] = UNSUPPORTED_VERSION;
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.Decoder.fromByteBuffer(ByteBuffer.wrap(framed)));
  }

  // Group 3: framed round trip through toByteBuffer and Decoder.fromByteBuffer.

  @Test
  public void testFramedRoundTripForEveryType() {
    for (StreamingShuffleMessage message : oneOfEachType()) {
      checkSerializeDeserialize(message);
    }
  }

  @Test
  public void testFramedLengthIsEncodedLengthPlusTypeByte() {
    for (StreamingShuffleMessage message : oneOfEachType()) {
      assertEquals(message.encodedLength() + 1, message.toByteBuffer().remaining());
    }
    // And for the variable-length message at several payload sizes.
    for (int payloadLength : new int[] {0, 1, 1024}) {
      DataBlockMessage block = DataBlockMessage.withComputedChecksum(
          SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, payload(payloadLength));
      assertEquals(38 + payloadLength, block.toByteBuffer().remaining());
    }
  }

  @Test
  public void testOwnedChecksumFactoryAdoptsRetainedPayloadAndSeedsChecksum() throws Exception {
    byte[] retainedPayload = payload(1024);
    DataBlockMessage block = DataBlockMessage.withComputedChecksumAndOwnedPayload(
      SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, retainedPayload);

    Field payloadField = DataBlockMessage.class.getDeclaredField("payload");
    payloadField.setAccessible(true);
    assertSame(retainedPayload, payloadField.get(block),
      "The producer-owned factory must adopt the retained array instead of cloning it.");
    assertEquals(block.checksum(), block.computedChecksum(),
      "The checksum computed before enqueue must be cached for consumer diagnostics.");
    assertTrue(block.verifyChecksum());
  }

  @Test
  public void testManagedDataBlockFrameGathersHeaderAndPayloadWithoutCopy() throws Exception {
    DataBlockMessage block = DataBlockMessage.withComputedChecksumAndOwnedPayload(
      SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, payload(1024));
    ManagedBuffer managed = block.toManagedBuffer();
    try {
      assertInstanceOf(NettyManagedBuffer.class, managed);
      Field bufferField = NettyManagedBuffer.class.getDeclaredField("buf");
      bufferField.setAccessible(true);
      ByteBuf encoded = (ByteBuf) bufferField.get(managed);
      assertEquals(2, encoded.nioBufferCount(),
        "A data block must gather its fixed header and retained payload as two buffers.");
      assertEquals(block.toByteBuffer().remaining(), managed.size());
      assertArrayEquals(toByteArray(block.toByteBuffer()), toByteArray(managed.nioByteBuffer()));
      assertEquals(block,
        StreamingShuffleMessage.Decoder.fromByteBuffer(managed.nioByteBuffer()));
    } finally {
      managed.release();
    }
  }

  @Test
  public void testFramingTypeByteMatchesDiscriminator() {
    assertFramingTypeByte(StreamingShuffleMessageType.DATA_BLOCK, dataBlock(16));
    assertFramingTypeByte(StreamingShuffleMessageType.ACK, ack(40L));
    assertFramingTypeByte(StreamingShuffleMessageType.HEARTBEAT, heartbeat());
    assertFramingTypeByte(StreamingShuffleMessageType.RETRANSMIT_REQUEST, retransmit(44L));
    assertFramingTypeByte(StreamingShuffleMessageType.STREAM_TERMINATION, termination(40L));
  }

  @Test
  public void testDecoderReturnsTheCorrectConcreteType() {
    assertInstanceOf(DataBlockMessage.class, roundTrip(dataBlock(16)));
    assertInstanceOf(AckMessage.class, roundTrip(ack(40L)));
    assertInstanceOf(HeartbeatMessage.class, roundTrip(heartbeat()));
    assertInstanceOf(RetransmitRequestMessage.class, roundTrip(retransmit(44L)));
    assertInstanceOf(StreamTerminationMessage.class, roundTrip(termination(40L)));
  }

  // Group 4: the two-mebibyte size cap.

  @Test
  public void testMaxBlockSizeConstant() {
    assertEquals(2 * 1024 * 1024, DataBlockMessage.MAX_BLOCK_SIZE_BYTES);
    assertEquals(2097152, DataBlockMessage.MAX_BLOCK_SIZE_BYTES);
  }

  @Test
  public void testPayloadExactlyAtCapIsAccepted() {
    // The production comparison is strictly greater than the cap, so a full block is legal.
    byte[] payload = new byte[DataBlockMessage.MAX_BLOCK_SIZE_BYTES];
    DataBlockMessage block = DataBlockMessage.withComputedChecksum(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, payload);
    assertEquals(DataBlockMessage.MAX_BLOCK_SIZE_BYTES, block.payloadLength());
    assertEquals(EMPTY_DATA_BLOCK_BYTES + DataBlockMessage.MAX_BLOCK_SIZE_BYTES,
        block.encodedLength());
    assertEquals(block, roundTrip(block));
    assertTrue(block.verifyChecksum());
  }

  @Test
  public void testPayloadOneByteOverCapIsRejectedOnConstruction() {
    byte[] tooBig = new byte[DataBlockMessage.MAX_BLOCK_SIZE_BYTES + 1];
    assertThrows(IllegalArgumentException.class, () ->
        new DataBlockMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, 0L, tooBig));
    assertThrows(IllegalArgumentException.class, () ->
        DataBlockMessage.withComputedChecksum(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER,
            tooBig));
  }

  @Test
  public void testOverSizePayloadLengthPrefixIsRejectedBeforeAllocating() {
    // Encoders.ByteArrays.decode allocates an array of exactly the size the prefix claims and has
    // no bound of its own, so the prefix must be refused before it is reached. The point of this
    // test is that the failure is an IllegalArgumentException rather than an OutOfMemoryError, and
    // that it happens without the buffer ever holding the bytes the prefix claims: the crafted
    // frame below is a few dozen bytes long, not two gibibytes.
    for (int hostilePrefix :
        new int[] {Integer.MAX_VALUE, DataBlockMessage.MAX_BLOCK_SIZE_BYTES + 1}) {
      ByteBuf buf = dataBlockBodyWithPayloadPrefix(hostilePrefix, 0);
      assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.decode(buf));
    }
  }

  @Test
  public void testNegativePayloadLengthPrefixIsRejected() {
    // Must be an IllegalArgumentException, not the NegativeArraySizeException that a bare
    // allocation would raise.
    for (int negativePrefix : new int[] {-1, Integer.MIN_VALUE}) {
      ByteBuf buf = dataBlockBodyWithPayloadPrefix(negativePrefix, 0);
      assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.decode(buf));
    }
  }

  @Test
  public void testTruncatedPayloadIsRejected() {
    ByteBuf buf = dataBlockBodyWithPayloadPrefix(10, 4);
    assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.decode(buf));
  }

  @Test
  public void testPayloadLongerThanItsPrefixDeclaresIsRejected() {
    // A surplus is as much a framing error as a shortfall: bytes the codec never examines must not
    // be allowed to ride along past the message boundary.
    ByteBuf buf = dataBlockBodyWithPayloadPrefix(4, 10);
    assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.decode(buf));
  }

  @Test
  public void testPayloadReservationIsConsultedBeforeDecodeAllocates() {
    int payloadBytes = 1024;
    RecordingPayloadReservation reservation = new RecordingPayloadReservation(false);
    StreamingShuffleMessage.PayloadReservationRejectedException failure =
        assertThrows(StreamingShuffleMessage.PayloadReservationRejectedException.class,
            () -> StreamingShuffleMessage.Decoder.fromByteBuffer(
                dataBlock(payloadBytes).toByteBuffer(), reservation));

    assertEquals(1, reservation.attempts);
    assertEquals(0, reservation.releases);
    assertEquals(SHUFFLE_ID, reservation.shuffleId);
    assertEquals(MAP_ID, reservation.mapId);
    assertEquals(PARTITION_ID, reservation.partitionId);
    assertEquals(SEQUENCE_NUMBER, reservation.sequenceNumber);
    assertEquals(payloadBytes, reservation.payloadBytes);
    assertEquals(SHUFFLE_ID, failure.shuffleId());
    assertEquals(MAP_ID, failure.mapId());
    assertEquals(PARTITION_ID, failure.partitionId());
    assertEquals(SEQUENCE_NUMBER, failure.sequenceNumber());
    assertEquals(payloadBytes, failure.payloadBytes());
  }

  @Test
  public void testDecodedBlockReturnsItsReservationExactlyOnce() {
    int payloadBytes = 512;
    RecordingPayloadReservation reservation = new RecordingPayloadReservation(true);
    DataBlockMessage decoded = (DataBlockMessage) StreamingShuffleMessage.Decoder.fromByteBuffer(
        dataBlock(payloadBytes).toByteBuffer(), reservation);

    assertTrue(decoded.hasPayloadReservation());
    assertEquals(payloadBytes, reservation.reservedBytes);
    assertTrue(decoded.releasePayloadReservation());
    assertFalse(decoded.hasPayloadReservation());
    assertFalse(decoded.releasePayloadReservation());
    assertEquals(1, reservation.releases);
    assertEquals(0, reservation.reservedBytes);
  }

  @Test
  public void testTransferredReservationIsNotReleasedByTheMessage() {
    int payloadBytes = 256;
    RecordingPayloadReservation reservation = new RecordingPayloadReservation(true);
    DataBlockMessage decoded = (DataBlockMessage) StreamingShuffleMessage.Decoder.fromByteBuffer(
        dataBlock(payloadBytes).toByteBuffer(), reservation);

    assertTrue(decoded.transferPayloadReservation());
    assertFalse(decoded.hasPayloadReservation());
    assertFalse(decoded.transferPayloadReservation());
    assertFalse(decoded.releasePayloadReservation());
    assertEquals(0, reservation.releases);
    assertEquals(payloadBytes, reservation.reservedBytes);
  }

  @Test
  public void testControlMessagesDoNotConsultPayloadReservation() {
    RecordingPayloadReservation reservation = new RecordingPayloadReservation(false);
    assertEquals(ack(40L), StreamingShuffleMessage.Decoder.fromByteBuffer(
        ack(40L).toByteBuffer(), reservation));
    assertEquals(0, reservation.attempts);
    assertEquals(0, reservation.releases);
  }

  // Group 5: checksum behaviour.

  @Test
  public void testComputeMatchesTheJdkCrc32c() {
    byte[][] inputs = {
        new byte[0],
        "streaming shuffle".getBytes(StandardCharsets.UTF_8),
        payload(4096),
    };
    for (byte[] data : inputs) {
      CRC32C expected = new CRC32C();
      expected.update(data, 0, data.length);
      assertEquals(expected.getValue(), StreamingShuffleChecksum.compute(data));
    }
    assertEquals("CRC32C", StreamingShuffleChecksum.ALGORITHM);
  }

  @Test
  public void testComputeSliceAgreesWithWholeArrayForm() {
    byte[] data = payload(512);
    int[][] ranges = {{0, 0}, {0, 1}, {0, 512}, {7, 0}, {7, 100}, {511, 1}, {512, 0}};
    for (int[] range : ranges) {
      int offset = range[0];
      int length = range[1];
      assertEquals(
          StreamingShuffleChecksum.compute(Arrays.copyOfRange(data, offset, offset + length)),
          StreamingShuffleChecksum.compute(data, offset, length));
    }
  }

  @Test
  public void testComputeRejectsRangesOutsideTheArray() {
    byte[] data = payload(16);
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleChecksum.compute(data, -1, 4));
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleChecksum.compute(data, 0, -1));
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleChecksum.compute(data, 8, 16));
    assertThrows(NullPointerException.class, () -> StreamingShuffleChecksum.compute(null));
  }

  @Test
  public void testComputeIsDeterministic() {
    // Proves no stateful Checksum instance is shared between calls, which would make the second
    // call accumulate on top of the first.
    byte[] data = "repeatable".getBytes(StandardCharsets.UTF_8);
    long first = StreamingShuffleChecksum.compute(data);
    assertEquals(first, StreamingShuffleChecksum.compute(data));
    assertEquals(first, StreamingShuffleChecksum.compute(data));
  }

  @Test
  public void testVerifyAcceptsIntactDataAndRejectsAWrongChecksum() {
    byte[] data = payload(256);
    long checksum = StreamingShuffleChecksum.compute(data);
    assertTrue(StreamingShuffleChecksum.verify(data, checksum));
    assertFalse(StreamingShuffleChecksum.verify(data, checksum + 1));
    assertTrue(StreamingShuffleChecksum.verify(data, 0, data.length, checksum));
  }

  @Test
  public void testVerifyDetectsASingleFlippedByte() {
    byte[] data = payload(256);
    long checksum = StreamingShuffleChecksum.compute(data);
    for (int index : new int[] {0, 127, 255}) {
      byte[] corrupted = data.clone();
      corrupted[index] ^= 0x01;
      assertFalse(StreamingShuffleChecksum.verify(corrupted, checksum));
    }
  }

  @Test
  public void testBlockChecksumBindsThePayloadToItsMetadata() {
    // A block checksum covers the payload together with the identity the block was produced under,
    // so a block whose header is rewritten in flight fails verification even though every payload
    // byte survived. A payload-only checksum could not tell the difference.
    byte[] payload = payload(1024);
    DataBlockMessage block = DataBlockMessage.withComputedChecksum(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, payload);
    assertTrue(block.verifyChecksum());
    assertEquals(StreamingShuffleChecksum.computeBlock(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, payload), block.checksum());

    long checksum = block.checksum();
    // Same payload and same checksum, but re-addressed to a different partition, a different
    // sequence number, and a different shuffle in turn.
    assertFalse(reAddressed(PARTITION_ID + 1, SEQUENCE_NUMBER, SHUFFLE_ID, payload, checksum));
    assertFalse(reAddressed(PARTITION_ID, SEQUENCE_NUMBER + 1, SHUFFLE_ID, payload, checksum));
    assertFalse(reAddressed(PARTITION_ID, SEQUENCE_NUMBER, SHUFFLE_ID + 1, payload, checksum));
  }

  @Test
  public void testBlockChecksumDetectsPayloadCorruption() {
    byte[] payload = payload(1024);
    DataBlockMessage block = DataBlockMessage.withComputedChecksum(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, payload);
    byte[] corrupted = payload.clone();
    corrupted[500] ^= 0x01;
    assertFalse(reAddressed(PARTITION_ID, SEQUENCE_NUMBER, SHUFFLE_ID, corrupted,
        block.checksum()));
    assertFalse(StreamingShuffleChecksum.verifyBlock(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, corrupted, block.checksum()));
  }

  @Test
  public void testBlockChecksumSurvivesRoundTrip() {
    for (int payloadLength : new int[] {0, 1, 1024}) {
      DataBlockMessage block = DataBlockMessage.withComputedChecksum(
          SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, payload(payloadLength));
      DataBlockMessage decoded = (DataBlockMessage) roundTrip(block);
      assertEquals(block.checksum(), decoded.checksum());
      assertTrue(decoded.verifyChecksum());
    }
  }

  @Test
  public void testBlockMetadataPreambleLengthMatchesTheFieldsItCovers() {
    // The metadata preamble is what makes a block checksum attest to where the bytes belong as well
    // as to the bytes themselves, and its length is stated in prose next to the constant. Pin the
    // number so that adding a covered field cannot leave that prose describing the old layout.
    assertEquals(28, StreamingShuffleChecksum.BLOCK_METADATA_PREAMBLE_LENGTH);
    // shuffleId (int) + mapId (long) + partitionId (int) + sequenceNumber (long) + the payload
    // length (int), which is covered so that a truncated payload cannot verify.
    assertEquals(4 + 8 + 4 + 8 + 4, StreamingShuffleChecksum.BLOCK_METADATA_PREAMBLE_LENGTH);
  }

  @Test
  public void testComputeBlockSliceAgreesWithWholeArrayForm() {
    byte[] data = payload(512);
    assertEquals(
        StreamingShuffleChecksum.computeBlock(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER,
            Arrays.copyOfRange(data, 8, 8 + 100)),
        StreamingShuffleChecksum.computeBlock(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER,
            data, 8,
            100));
    assertThrows(IllegalArgumentException.class, () -> StreamingShuffleChecksum.computeBlock(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, data, 8, 512));
  }

  // Group 6: unknown type and id handling.

  @Test
  public void testMessageTypeIdsAreFrozen() {
    assertEquals(0, StreamingShuffleMessageType.DATA_BLOCK.id());
    assertEquals(1, StreamingShuffleMessageType.ACK.id());
    assertEquals(2, StreamingShuffleMessageType.HEARTBEAT.id());
    assertEquals(3, StreamingShuffleMessageType.RETRANSMIT_REQUEST.id());
    assertEquals(4, StreamingShuffleMessageType.STREAM_TERMINATION.id());
    assertEquals(5, StreamingShuffleMessageType.values().length);
  }

  @Test
  public void testEveryMessageTypeRoundTripsThroughItsId() {
    for (StreamingShuffleMessageType type : StreamingShuffleMessageType.values()) {
      assertEquals(type, StreamingShuffleMessageType.fromId(type.id()));
    }
  }

  @Test
  public void testUnknownMessageTypeIdIsRejected() {
    // 5 is HEARTBEAT in the unrelated BlockTransferMessage.Type id space and must not resolve here,
    // which is what keeps the two families' parallel id spaces genuinely independent.
    for (byte id : new byte[] {UNKNOWN_TYPE_ID, (byte) 5, (byte) -1, Byte.MAX_VALUE}) {
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
          () -> StreamingShuffleMessageType.fromId(id));
      assertTrue(e.getMessage().contains("Unknown message type"), e.getMessage());
    }
  }

  @Test
  public void testDecoderRejectsUnknownFramingTypeByte() {
    // The version byte is left compatible on purpose, so the failure observed is genuinely the
    // unknown-type path rather than the version check firing first.
    byte[] framed = toByteArray(ack(40L).toByteBuffer());
    framed[0] = UNKNOWN_TYPE_ID;
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.Decoder.fromByteBuffer(ByteBuffer.wrap(framed)));
  }

  @Test
  public void testDecoderRejectsFramesTooShortToRoute() {
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.Decoder.fromByteBuffer(ByteBuffer.allocate(0)));
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.Decoder.fromByteBuffer(ByteBuffer.allocate(1)));
  }

  @Test
  public void testDecoderRejectsNullBuffer() {
    assertThrows(NullPointerException.class,
        () -> StreamingShuffleMessage.Decoder.fromByteBuffer(null));
  }

  // Group 7: equals and hashCode contract.

  @Test
  public void testDataBlockEqualityIsValueBasedOverThePayload() {
    // Two distinct arrays holding equal content must produce equal blocks, which is what proves
    // Arrays.equals and Arrays.hashCode are used rather than array identity.
    byte[] first = payload(64);
    byte[] second = payload(64);
    assertNotSame(first, second);
    DataBlockMessage a = DataBlockMessage.withComputedChecksum(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, first);
    DataBlockMessage b = DataBlockMessage.withComputedChecksum(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, second);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertEquals(a.toString(), b.toString());
  }

  @Test
  public void testReflexivityAndSymmetryForEveryType() {
    for (StreamingShuffleMessage message : oneOfEachType()) {
      assertEquals(message, message);
      StreamingShuffleMessage twin = roundTrip(message);
      assertEquals(message, twin);
      assertEquals(twin, message);
      assertEquals(message.hashCode(), twin.hashCode());
    }
  }

  @Test
  public void testInequalityWhenAnySingleFieldDiffers() {
    AckMessage base = ack(40L);
    assertNotEquals(base, new AckMessage(SHUFFLE_ID + 1, MAP_ID, PARTITION_ID, 40L));
    assertNotEquals(base, new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 41L));
    assertNotEquals(base, new AckMessage(SHUFFLE_ID, MAP_ID + 1, PARTITION_ID, 40L));
    assertNotEquals(base, ack(41L));

    HeartbeatMessage beat = heartbeat();
    assertNotEquals(beat, new HeartbeatMessage(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, CONSUMER_TOKEN + 1L));
    assertNotEquals(beat, heartbeatWithoutToken());

    DataBlockMessage block = dataBlock(64);
    assertNotEquals(block, DataBlockMessage.withComputedChecksum(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, payload(65)));
    byte[] different = payload(64);
    different[0] ^= 0x01;
    assertNotEquals(block, DataBlockMessage.withComputedChecksum(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, different));
    // Same payload, different checksum field.
    assertNotEquals(block, new DataBlockMessage(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, block.checksum() + 1, payload(64)));
  }

  @Test
  public void testTheFourControlMessagesAreNeverEqualToEachOther() {
    // Built over identical header and producer-id values, the control messages agree on every field
    // they share, so content alone cannot tell them apart. Given that, they must still be distinct
    // in memory, which is what confirms each equals implementation checks the concrete type before
    // it compares a field.
    StreamingShuffleMessage[] identical = {
        new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER - 1L),
        new HeartbeatMessage(
            SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, CONSUMER_TOKEN),
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER),
        new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER),
    };
    for (int i = 0; i < identical.length; i++) {
      assertTrue(identical[i].encodedLength() >= 25);
      for (int j = i + 1; j < identical.length; j++) {
        assertNotEquals(identical[i], identical[j]);
        assertNotEquals(identical[j], identical[i]);
      }
      // Each still round-trips to its own concrete class.
      assertSame(identical[i].getClass(), roundTrip(identical[i]).getClass());
    }
  }

  @Test
  public void testNoMessageEqualsNullOrAnUnrelatedObject() {
    Object nothing = null;
    Object unrelated = "not a streaming shuffle message";
    for (StreamingShuffleMessage message : oneOfEachType()) {
      assertFalse(message.equals(nothing));
      assertFalse(message.equals(unrelated));
      assertNotEquals(message, unrelated);
    }
  }

  @Test
  public void testDataBlockToStringReportsTheLengthAndNotTheBytes() {
    DataBlockMessage block = dataBlock(1024);
    String rendered = block.toString();
    assertTrue(rendered.contains("1024"), rendered);
    assertTrue(rendered.contains("DataBlockMessage"), rendered);
    // A block runs to two mebibytes and this string reaches diagnostics, so it has to stay bounded.
    assertTrue(rendered.length() < 200, "toString is too long: " + rendered.length());
  }

  // Frame bounds and exact consumption.

  @Test
  public void testMaximumFrameLengthMatchesTheLargestLegitimateFrame() {
    // The bound is the framing byte plus a maximum-size data block, so a full block must frame to
    // exactly the maximum and nothing legitimate may exceed it.
    DataBlockMessage fullBlock = DataBlockMessage.withComputedChecksum(SHUFFLE_ID, MAP_ID,
        PARTITION_ID,
        SEQUENCE_NUMBER, new byte[DataBlockMessage.MAX_BLOCK_SIZE_BYTES]);
    assertEquals(StreamingShuffleMessage.MAX_FRAME_LENGTH,
        fullBlock.toByteBuffer().remaining());
    assertEquals(1 + 25 + 8 + 4 + DataBlockMessage.MAX_BLOCK_SIZE_BYTES,
        StreamingShuffleMessage.MAX_FRAME_LENGTH);
  }

  @Test
  public void testDecoderRejectsAnOverLongFrame() {
    // One byte past the ceiling, with a plausible type and a compatible version, so the rejection
    // can only be the frame bound itself.
    ByteBuffer overLong = ByteBuffer.allocate(StreamingShuffleMessage.MAX_FRAME_LENGTH + 1);
    overLong.put(StreamingShuffleMessageType.ACK.id());
    overLong.put(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION);
    overLong.clear();
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.Decoder.fromByteBuffer(overLong));
  }

  @Test
  public void testDecoderRejectsTrailingBytesAfterAValidMessage() {
    // Appending to an otherwise valid frame must not succeed: the suffix would survive the message
    // boundary without the codec ever examining it.
    for (StreamingShuffleMessage message : oneOfEachType()) {
      byte[] valid = toByteArray(message.toByteBuffer());
      byte[] withSuffix = Arrays.copyOf(valid, valid.length + 3);
      assertThrows(IllegalArgumentException.class,
          () -> StreamingShuffleMessage.Decoder.fromByteBuffer(ByteBuffer.wrap(withSuffix)));
    }
  }

  @Test
  public void testEachControlDecoderRequiresAnExactBody() {
    // Directly against each concrete decoder, one byte short and one byte long. The heartbeat's
    // body is variable across identities but exact after its length prefix has been read.
    for (StreamingShuffleMessage message : exactBodyMessages()) {
      byte[] body = encodedBody(message);
      ByteBuf shortBody = wrapped(Arrays.copyOf(body, body.length - 1));
      ByteBuf longBody = wrapped(Arrays.copyOf(body, body.length + 1));
      assertThrows(IllegalArgumentException.class, () -> decodeAs(message, shortBody));
      assertThrows(IllegalArgumentException.class, () -> decodeAs(message, longBody));
    }
  }

  // Header identifier domains.

  @Test
  public void testNegativeHeaderIdentifiersAreRejectedOnDecode() {
    // Hand-crafted because the constructors refuse these values too, which is the point: the
    // domain rule is the same in both directions, so no message can be built that cannot be
    // decoded, and none can be decoded that could not have been built.
    assertThrows(IllegalArgumentException.class, () -> decodeAckWithHeader(-1, PARTITION_ID, 0L));
    assertThrows(IllegalArgumentException.class, () -> decodeAckWithHeader(SHUFFLE_ID, -1, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> decodeAckWithHeader(SHUFFLE_ID, PARTITION_ID, -1L));
    assertThrows(IllegalArgumentException.class,
        () -> decodeAckWithHeader(Integer.MIN_VALUE, PARTITION_ID, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> decodeAckWithHeader(SHUFFLE_ID, PARTITION_ID, Long.MIN_VALUE));
    // The map id joined the header when one listener per executor became the serving model, so it
    // is a routing identity like the others and is refused on the same terms.
    assertThrows(IllegalArgumentException.class,
        () -> decodeAckWithHeader(SHUFFLE_ID, -1L, PARTITION_ID, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> decodeAckWithHeader(SHUFFLE_ID, Long.MIN_VALUE, PARTITION_ID, 0L));
  }

  @Test
  public void testNegativeHeaderIdentifiersAreRejectedOnConstruction() {
    assertThrows(IllegalArgumentException.class,
        () -> new AckMessage(-1, MAP_ID, PARTITION_ID, 0L));
    assertThrows(IllegalArgumentException.class, () -> new AckMessage(SHUFFLE_ID, MAP_ID, -1, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> new HeartbeatMessage(-1, MAP_ID, 0, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> new HeartbeatMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, -1L));
    assertThrows(IllegalArgumentException.class,
        () -> new RetransmitRequestMessage(-1, MAP_ID, 0, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, -1L));
    assertThrows(IllegalArgumentException.class,
        () -> new StreamTerminationMessage(-1, MAP_ID, 0, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, -1L));
    assertThrows(IllegalArgumentException.class,
        () -> DataBlockMessage.withComputedChecksum(-1, MAP_ID, 0, 0L, payload(8)));
    // A negative producer id is rejected wherever a message can be built, exactly as a negative
    // shuffle or partition id is, even though it now travels in the body rather than the header.
    assertThrows(IllegalArgumentException.class,
        () -> new AckMessage(SHUFFLE_ID, -1L, PARTITION_ID, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> new HeartbeatMessage(SHUFFLE_ID, -1L, PARTITION_ID, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> new RetransmitRequestMessage(SHUFFLE_ID, -1L, PARTITION_ID, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> new StreamTerminationMessage(SHUFFLE_ID, -1L, PARTITION_ID, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> DataBlockMessage.withComputedChecksum(SHUFFLE_ID, -1L, PARTITION_ID, 0L, payload(8)));
    assertThrows(IllegalArgumentException.class, () -> new StreamingShuffleMessage.Header(
        StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, -1, PARTITION_ID, 0L));
    // Zero is the smallest legal value for all of them and must be accepted.
    assertDoesNotThrow(() -> new AckMessage(0, 0L, 0, AckMessage.NOTHING_CONSUMED));
  }

  // Body domains: acknowledgement position, retransmission position, termination count.

  @Test
  public void testAcknowledgementPositionDomain() {
    assertEquals(-1L, AckMessage.NOTHING_CONSUMED);
    // The one legal negative, and it must survive a round trip. On the wire it is the next position
    // expected -- zero -- so nothing negative is ever encoded.
    AckMessage nothingConsumed =
        new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, AckMessage.NOTHING_CONSUMED);
    assertEquals(AckMessage.NOTHING_CONSUMED, nothingConsumed.consumerPosition());
    assertEquals(0L, nothingConsumed.sequenceNumber());
    assertEquals(nothingConsumed, roundTrip(nothingConsumed));
    // Non-negative positions up to the expressible maximum are legal.
    assertDoesNotThrow(() -> ack(0L));
    assertDoesNotThrow(() -> ack(AckMessage.MAX_CONSUMER_POSITION));
    assertEquals(Long.MAX_VALUE - 1L, AckMessage.MAX_CONSUMER_POSITION);
    assertEquals(Long.MAX_VALUE, ack(AckMessage.MAX_CONSUMER_POSITION).sequenceNumber());
    // Every other negative is refused, because it would flow into the producer's buffer
    // reclamation arithmetic where it is not a small number but a broken comparison.
    for (long invalid : new long[] {-2L, -100L, Long.MIN_VALUE}) {
      assertThrows(IllegalArgumentException.class, () -> ack(invalid));
    }
    // And so is a position whose successor is not a position: the next expected value is what
    // travels, so Long.MAX_VALUE cannot be acknowledged.
    assertThrows(IllegalArgumentException.class, () -> ack(Long.MAX_VALUE));
  }

  @Test
  public void testAcknowledgementPositionDomainIsEnforcedOnDecode() {
    // The wire carries the next expected position in the header, so a negative one is refused by
    // the header's own domain check -- on the single path every decoder takes.
    for (long invalid : new long[] {-1L, -2L, Long.MIN_VALUE}) {
      ByteBuf buf = buffer();
      buf.writeByte(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION);
      buf.writeInt(SHUFFLE_ID);
      buf.writeInt(PARTITION_ID);
      buf.writeLong(invalid);
      buf.writeLong(MAP_ID);
      assertThrows(IllegalArgumentException.class, () -> AckMessage.decode(buf));
    }
  }

  @Test
  public void testRetransmissionRequestWindowIsClosedAndBounded() {
    // A request names a closed interval. A single block is the interval whose ends coincide, which
    // is the ordinary shape a corrupt block calls for.
    RetransmitRequestMessage single =
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 5L);
    assertEquals(1L, single.blockCount());
    assertEquals(5L, single.firstSequenceNumber());
    assertEquals(5L, single.lastSequenceNumber());
    assertTrue(single.contains(5L));
    assertFalse(single.contains(4L));
    assertFalse(single.contains(6L));
    assertEquals(single, roundTrip(single));
    assertEquals(RETRANSMIT_REQUEST_BYTES, single.encodedLength());
    // Position zero is legal, since block numbering starts there.
    assertDoesNotThrow(() -> new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L));
    // The extreme is legal for a single position: the width is zero, so nothing overflows.
    assertDoesNotThrow(
        () -> new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, Long.MAX_VALUE));
    // An inverted window is refused, so the two ends can never be transposed.
    assertThrows(IllegalArgumentException.class,
        () -> new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 5L, 4L));
    // The widest legal window includes both of its ends, so its span is one below the ceiling.
    long widest = RetransmitRequestMessage.MAX_REQUESTED_BLOCKS - 1L;
    RetransmitRequestMessage atTheCeiling =
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L, widest);
    assertEquals(RetransmitRequestMessage.MAX_REQUESTED_BLOCKS, atTheCeiling.blockCount());
    assertEquals(atTheCeiling, roundTrip(atTheCeiling));
    // One block wider is refused, which stops a peer naming a window for a producer to walk.
    assertThrows(IllegalArgumentException.class, () -> new RetransmitRequestMessage(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L, RetransmitRequestMessage.MAX_REQUESTED_BLOCKS));
    // Including the window a Long.MAX_VALUE upper bound would express, whose count would otherwise
    // wrap negative and slip past a naive comparison.
    assertThrows(IllegalArgumentException.class,
        () -> new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L, Long.MAX_VALUE));
  }

  @Test
  public void testRetransmissionRequestWindowDomainIsEnforcedOnDecode() {
    // A peer cannot smuggle an inverted or over-wide window past the codec: the same constructor
    // validates a decoded request and a locally built one.
    ByteBuf inverted = Unpooled.buffer();
    writeHeader(inverted, SHUFFLE_ID, PARTITION_ID, 9L);
    inverted.writeLong(8L);
    assertThrows(IllegalArgumentException.class,
        () -> RetransmitRequestMessage.decode(inverted));

    ByteBuf overWide = Unpooled.buffer();
    writeHeader(overWide, SHUFFLE_ID, PARTITION_ID, 0L);
    overWide.writeLong(RetransmitRequestMessage.MAX_REQUESTED_BLOCKS);
    assertThrows(IllegalArgumentException.class,
        () -> RetransmitRequestMessage.decode(overWide));
  }

  @Test
  public void testRetransmissionRequestDomainIsEnforcedOnDecode() {
    ByteBuf negative = buffer();
    negative.writeByte(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION);
    negative.writeInt(SHUFFLE_ID);
    negative.writeInt(PARTITION_ID);
    negative.writeLong(-1L);
    negative.writeLong(MAP_ID);
    assertThrows(IllegalArgumentException.class,
        () -> RetransmitRequestMessage.decode(negative));

    // A body of any size other than the producer id followed by the upper bound is a framing error
    // rather than something to tolerate, in either direction.
    ByteBuf truncated = buffer();
    writeHeader(truncated, SHUFFLE_ID, PARTITION_ID, 5L);
    assertThrows(IllegalArgumentException.class,
        () -> RetransmitRequestMessage.decode(truncated));

    ByteBuf surplus = buffer();
    writeHeader(surplus, SHUFFLE_ID, PARTITION_ID, 5L);
    surplus.writeLong(5L);
    surplus.writeByte(0);
    assertThrows(IllegalArgumentException.class, () -> RetransmitRequestMessage.decode(surplus));
  }

  @Test
  public void testStreamTerminationBlockCountRelation() {
    // Zero blocks is legal: an empty partition legitimately terminates having sent nothing.
    assertDoesNotThrow(() -> new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L));
    assertEquals(0L, new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L)
        .totalBlocks());
    // Only data blocks consume sequence numbers -- a heartbeat and the terminator both report the
    // next unissued position without claiming it -- so the terminator's position equals the number
    // of blocks that preceded it. The two are now one field, so the relation cannot be violated at
    // all: an undercount, which is the silent truncation case, is unrepresentable rather than
    // merely refused.
    StreamTerminationMessage ten = new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID,
        10L);
    assertEquals(10L, ten.totalBlocks());
    assertEquals(10L, ten.sequenceNumber());
    assertEquals(ten, roundTrip(ten));
    // A negative count is refused by the header's own domain check.
    assertThrows(IllegalArgumentException.class,
        () -> new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, -1L));
    assertThrows(IllegalArgumentException.class,
        () -> new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, Long.MIN_VALUE));
  }

  @Test
  public void testStreamTerminationBlockCountRelationIsEnforcedOnDecode() {
    // A peer cannot state a total that disagrees with the position, because there is one field for
    // both; what it can state is a negative one, and that is refused where it enters.
    ByteBuf negative = buffer();
    negative.writeByte(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION);
    negative.writeInt(SHUFFLE_ID);
    negative.writeInt(PARTITION_ID);
    negative.writeLong(-1L);
    negative.writeLong(MAP_ID);
    assertThrows(IllegalArgumentException.class,
        () -> StreamTerminationMessage.decode(negative));

    ByteBuf surplus = buffer();
    writeHeader(surplus, SHUFFLE_ID, PARTITION_ID, 10L);
    surplus.writeLong(10L);
    assertThrows(IllegalArgumentException.class,
        () -> StreamTerminationMessage.decode(surplus));
  }

  // Shared acknowledgement predicates, and the boundaries between them.
  //
  // acknowledgesWithin and supersedes are the protocol's own normative answers to "may this
  // acknowledgement be applied", published here so that every producer decides it the same way
  // rather than each open-coding a comparison. Both read the one position the message carries: one
  // against the highest position the producer has issued, the other against the highest position it
  // has already applied. These tests pin each predicate to its own boundary.

  @Test
  public void testAcknowledgesWithinAtBelowAndAboveTheBound() {
    // At the bound is legal: the consumer has consumed exactly what the producer has issued.
    assertTrue(ack(10L).acknowledgesWithin(10L));
    assertTrue(ack(9L).acknowledgesWithin(10L));
    // One block past the bound is not an optimistic guess but a claim on a block that was never
    // issued, and honouring it would drain the whole retained window.
    assertFalse(ack(11L).acknowledgesWithin(10L));
    // The extremes behave the same way, since the comparison never adds anything.
    assertTrue(ack(AckMessage.MAX_CONSUMER_POSITION)
        .acknowledgesWithin(AckMessage.MAX_CONSUMER_POSITION));
    assertFalse(ack(AckMessage.MAX_CONSUMER_POSITION)
        .acknowledgesWithin(AckMessage.MAX_CONSUMER_POSITION - 1L));
  }

  @Test
  public void testAcknowledgesWithinTreatsNothingConsumedAsTheOnlySentinel() {
    // NOTHING_CONSUMED acknowledges no block, so it can overreach no bound.
    for (long bound : new long[] {AckMessage.NOTHING_CONSUMED, 0L, 7L, Long.MAX_VALUE}) {
      assertTrue(ack(AckMessage.NOTHING_CONSUMED).acknowledgesWithin(bound),
          "the sentinel must pass against every bound, including " + bound);
    }
    // A producer that has issued nothing passes the sentinel as its bound, which must then admit
    // only that same sentinel: no position can be acknowledged before a position exists.
    assertFalse(ack(0L).acknowledgesWithin(AckMessage.NOTHING_CONSUMED));
  }

  @Test
  public void testAcknowledgesWithinReadsTheAcknowledgedPositionAndNotTheWireValue() {
    // The wire carries the next expected position, one above the acknowledged one, so a predicate
    // that read the header field directly would be off by one at every boundary. These pairs pin
    // the difference: position five is within a bound of five, and six is not.
    assertTrue(ack(5L).acknowledgesWithin(5L));
    assertFalse(ack(6L).acknowledgesWithin(5L));
    assertEquals(6L, ack(5L).sequenceNumber());
    // Two acknowledgements of the same position are the same message, because the position is the
    // whole of what an acknowledgement says.
    assertEquals(ack(5L), ack(5L));
    assertNotEquals(ack(5L), ack(6L));
  }

  @Test
  public void testSupersedesIsStrictSoADuplicateIsRefused() {
    AckMessage atOneHundred = ack(100L);
    assertTrue(atOneHundred.supersedes(100L));
    // Equal is refused, which is what makes applying an acknowledgement idempotent: a duplicate
    // delivery must not be counted twice. The applied value is the wire position, one above the
    // acknowledged one.
    assertFalse(atOneHundred.supersedes(101L));
    // Older is refused, which is what makes it safe against reordering.
    assertFalse(atOneHundred.supersedes(102L));
    assertFalse(atOneHundred.supersedes(Long.MAX_VALUE));
    // The extreme boundary, for the same reason as the position predicate.
    assertTrue(ack(AckMessage.MAX_CONSUMER_POSITION).supersedes(Long.MAX_VALUE - 1L));
    assertFalse(ack(AckMessage.MAX_CONSUMER_POSITION).supersedes(Long.MAX_VALUE));
  }

  @Test
  public void testSupersedesAdmitsTheFirstMessageAgainstTheNothingAppliedSeed() {
    // A producer seeds its cursor with NOTHING_CONSUMED, and the first acknowledgement a consumer
    // emits announces the next position it expects, which is at least zero. If that pair did not
    // admit, the very first acknowledgement of every stream would be discarded as a repeat.
    assertTrue(ack(AckMessage.NOTHING_CONSUMED).supersedes(AckMessage.NOTHING_CONSUMED));
    assertTrue(ack(0L).supersedes(AckMessage.NOTHING_CONSUMED));
  }

  @Test
  public void testSupersedesIsMonotonicOverARunAndRefusesAReplay() {
    // Fold a strictly increasing run of positions exactly as a producer does. Every message must be
    // admitted once, and replaying the same run must admit none of them.
    long applied = AckMessage.NOTHING_CONSUMED;
    long[] positions = {40L, 90L, 140L, 190L};
    for (long position : positions) {
      AckMessage message = ack(position);
      assertTrue(message.supersedes(applied), "position " + position + " must be admitted once");
      applied = message.sequenceNumber();
      assertFalse(message.supersedes(applied), "and must not be admitted a second time");
    }
    for (long position : positions) {
      assertFalse(ack(position).supersedes(applied), "a replayed run is refused");
    }
  }

  // Payload ownership.

  @Test
  public void testConstructorCopiesThePayloadSoLaterMutationCannotReachTheBlock() {
    // A producer commonly serializes into a reusable buffer. If the block kept that array, the
    // producer's next use of it would silently rewrite bytes already checksummed and possibly
    // already queued for retransmission, and the checksum would become a promise about content the
    // block no longer held.
    byte[] caller = {1, 2, 3, 4};
    DataBlockMessage block =
        DataBlockMessage.withComputedChecksum(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER,
            caller);
    long checksumWhenBuilt = block.checksum();

    Arrays.fill(caller, (byte) 99);

    assertArrayEquals(new byte[] {1, 2, 3, 4}, block.copyPayload());
    assertEquals(checksumWhenBuilt, block.checksum());
    assertTrue(block.verifyChecksum());
  }

  @Test
  public void testPayloadBufferIsReadOnlyAndIndependentPerCall() {
    DataBlockMessage block = dataBlock(32);
    ByteBuffer view = block.payloadBuffer();
    assertTrue(view.isReadOnly());
    assertEquals(32, view.remaining());
    assertThrows(java.nio.ReadOnlyBufferException.class, () -> view.put(0, (byte) 0));

    // Consuming one view must not disturb another, so concurrent readers cannot interfere.
    view.get();
    assertEquals(31, view.remaining());
    assertEquals(32, block.payloadBuffer().remaining());
  }

  @Test
  public void testCopyPayloadHandsOutAFreshMutableArrayEachTime() {
    DataBlockMessage block = dataBlock(16);
    byte[] first = block.copyPayload();
    byte[] second = block.copyPayload();
    assertNotSame(first, second);
    assertArrayEquals(first, second);

    Arrays.fill(first, (byte) 77);
    assertArrayEquals(second, block.copyPayload());
    assertTrue(block.verifyChecksum());
  }

  // The map id, which is what lets one listener per executor serve every producer on it.

  @Test
  public void testMapIdSurvivesTheRoundTripOnEveryMessageType() {
    // The map id is the field a router reads to decide which producer a frame concerns, so it has
    // to come back off the wire exactly as it went on for every message in the family -- not only
    // for data blocks.
    for (StreamingShuffleMessage message : oneOfEachType()) {
      assertEquals(MAP_ID, message.mapId(), message.getClass().getSimpleName());
      StreamingShuffleMessage decoded =
          StreamingShuffleMessage.Decoder.fromByteBuffer(message.toByteBuffer());
      assertEquals(MAP_ID, decoded.mapId(), message.getClass().getSimpleName());
      assertEquals(message, decoded);
    }
  }

  @Test
  public void testMapIdParticipatesInEqualityAndInStreamBinding() {
    // Two blocks alike in every field but the map id are different blocks. Without this, output
    // from two map tasks sharing a partition and a sequence number would be indistinguishable,
    // which is precisely the collision the field was added to prevent.
    AckMessage base = ack(40L);
    assertNotEquals(base, new AckMessage(SHUFFLE_ID, MAP_ID + 1L, PARTITION_ID, 40L));
    assertNotEquals(base.hashCode(),
        new AckMessage(SHUFFLE_ID, MAP_ID + 1L, PARTITION_ID, 40L).hashCode());
    assertTrue(base.toString().contains("mapId=" + MAP_ID));

    // A frame that names another producer is refused at the binding boundary rather than being
    // handed to a caller who has to remember to question it.
    for (StreamingShuffleMessage message : oneOfEachType()) {
      assertThrows(IllegalArgumentException.class, () -> StreamingShuffleMessage.Decoder
          .fromByteBuffer(message.toByteBuffer(), SHUFFLE_ID, MAP_ID + 1L, PARTITION_ID));
      assertThrows(IllegalArgumentException.class, () ->
          StreamingShuffleMessage.checkStreamContext(message, SHUFFLE_ID, MAP_ID + 1L,
              PARTITION_ID));
    }
  }

  @Test
  public void testChecksumBindsTheMapIdSoAReattributedBlockFailsVerification() {
    // The checksum's whole purpose is to bind the payload to the identity it was sent under. Now
    // that the map id decides whose output a block is, a block re-stamped with another map id must
    // fail verification even though every payload byte survived -- otherwise a rewritten map id
    // would silently reattribute intact bytes to the wrong producer's stream.
    byte[] bytes = payload(256);
    DataBlockMessage block =
        DataBlockMessage.withComputedChecksum(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER,
            bytes);
    assertTrue(block.verifyChecksum());

    assertNotEquals(
        StreamingShuffleChecksum.computeBlock(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER,
            bytes),
        StreamingShuffleChecksum.computeBlock(SHUFFLE_ID, MAP_ID + 1L, PARTITION_ID,
            SEQUENCE_NUMBER, bytes));
    assertFalse(StreamingShuffleChecksum.verifyBlock(SHUFFLE_ID, MAP_ID + 1L, PARTITION_ID,
        SEQUENCE_NUMBER, bytes, block.checksum()));
    assertFalse(new DataBlockMessage(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, SHUFFLE_ID,
        MAP_ID + 1L, PARTITION_ID, SEQUENCE_NUMBER, block.checksum(), bytes).verifyChecksum());
  }

  @Test
  public void testProducerIdIsTheFirstBodyFieldOfEveryMessage() {
    // The producer id is not a header field: the header is the seventeen bytes the specification
    // fixes, and the id opens every body immediately after it. That placement is what lets a router
    // read it at one offset for all five types without decoding a body.
    for (StreamingShuffleMessage message : oneOfEachType()) {
      byte[] body = encodedBody(message);
      assertEquals(MAP_ID,
          ByteBuffer.wrap(body).getLong(StreamingShuffleMessage.HEADER_ENCODED_LENGTH),
          message.getClass().getSimpleName());
      assertEquals(MAP_ID, ByteBuffer.wrap(body).getLong(17));
    }
    // And a control message that carries no field of its own is exactly the header plus that id:
    // twenty-five bytes, twenty-six framed.
    for (StreamingShuffleMessage message : fixedSizeMessages()) {
      assertEquals(25, message.encodedLength(), message.getClass().getSimpleName());
      assertEquals(26, message.toByteBuffer().remaining());
    }
  }
  @Test
  public void testStreamContextBindingAcceptsTheMatchingStream() {
    for (StreamingShuffleMessage message : oneOfEachType()) {
      assertEquals(message, StreamingShuffleMessage.Decoder.fromByteBuffer(
          message.toByteBuffer(), SHUFFLE_ID, MAP_ID, PARTITION_ID));
      assertDoesNotThrow(() ->
          StreamingShuffleMessage.checkStreamContext(message, SHUFFLE_ID, MAP_ID, PARTITION_ID));
    }
  }

  @Test
  public void testStreamContextBindingRejectsAMisaddressedMessage() {
    // Structurally valid, correctly checksummed, and addressed elsewhere: decoding alone cannot
    // catch this, because the codec has no notion of which stream it is serving.
    for (StreamingShuffleMessage message : oneOfEachType()) {
      assertThrows(IllegalArgumentException.class,
          () -> StreamingShuffleMessage.Decoder.fromByteBuffer(
              message.toByteBuffer(), SHUFFLE_ID, MAP_ID, PARTITION_ID + 1));
      assertThrows(IllegalArgumentException.class,
          () -> StreamingShuffleMessage.Decoder.fromByteBuffer(
              message.toByteBuffer(), SHUFFLE_ID + 1, MAP_ID, PARTITION_ID));
      assertThrows(IllegalArgumentException.class, () ->
          StreamingShuffleMessage.checkStreamContext(message, SHUFFLE_ID, MAP_ID,
              PARTITION_ID + 1));
    }
    assertThrows(NullPointerException.class,
        () -> StreamingShuffleMessage.checkStreamContext(null, SHUFFLE_ID, MAP_ID, PARTITION_ID));
  }

  // Framing constants: the payload cap and the encoded-frame cap are two different resources.

  @Test
  public void testFramingConstantsFormOneConsistentContract() {
    assertEquals(1, StreamingShuffleMessage.FRAME_TYPE_PREFIX_LENGTH);
    // One framing prefix + seventeen header + eight producer id + eight checksum + four payload
    // length prefix.
    assertEquals(38, DataBlockMessage.FRAMING_OVERHEAD_BYTES);
    assertEquals(1 + StreamingShuffleMessage.HEADER_ENCODED_LENGTH + 8 + 8 + 4,
        DataBlockMessage.FRAMING_OVERHEAD_BYTES);
    assertEquals(2097190, DataBlockMessage.MAX_ENCODED_FRAME_BYTES);
    // The relation is asserted rather than assumed, because a component that budgets the payload
    // cap while transmitting the frame cap under-accounts for every block it sends.
    assertEquals(DataBlockMessage.MAX_BLOCK_SIZE_BYTES + DataBlockMessage.FRAMING_OVERHEAD_BYTES,
        DataBlockMessage.MAX_ENCODED_FRAME_BYTES);
    assertTrue(DataBlockMessage.MAX_ENCODED_FRAME_BYTES > DataBlockMessage.MAX_BLOCK_SIZE_BYTES);
    // The decoder's ceiling and the published frame cap have to be the same number, or a frame the
    // producer considers legal is one the consumer refuses.
    assertEquals(DataBlockMessage.MAX_ENCODED_FRAME_BYTES,
        StreamingShuffleMessage.MAX_FRAME_LENGTH);
  }

  @Test
  public void testMaximumPayloadOccupiesExactlyTheMaximumEncodedFrame() {
    // The assertion the shared-constant contract exists for: the largest legal payload frames to
    // precisely MAX_ENCODED_FRAME_BYTES, no more and no less.
    byte[] atCap = new byte[DataBlockMessage.MAX_BLOCK_SIZE_BYTES];
    DataBlockMessage block =
        new DataBlockMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, 0L, atCap);
    assertEquals(DataBlockMessage.MAX_ENCODED_FRAME_BYTES,
        StreamingShuffleMessage.framedLength(block.encodedLength()));

    ByteBuffer frame = block.toByteBuffer();
    assertEquals(DataBlockMessage.MAX_ENCODED_FRAME_BYTES, frame.remaining());
    DataBlockMessage decoded =
        (DataBlockMessage) StreamingShuffleMessage.Decoder.fromByteBuffer(frame);
    assertEquals(DataBlockMessage.MAX_BLOCK_SIZE_BYTES, decoded.payloadLength());
  }

  @Test
  public void testFramedLengthAgreesWithTheFramedFormForEveryType() {
    for (StreamingShuffleMessage message : oneOfEachType()) {
      assertEquals(StreamingShuffleMessage.framedLength(message.encodedLength()),
          message.toByteBuffer().remaining());
    }
    // A block carrying nothing occupies exactly the framing overhead, which is what makes the
    // overhead constant meaningful to a component sizing a per-block charge.
    DataBlockMessage empty = dataBlock(0);
    assertEquals(DataBlockMessage.FRAMING_OVERHEAD_BYTES,
        StreamingShuffleMessage.framedLength(empty.encodedLength()));
  }

  // Exact wire layout: field order and offsets, asserted byte by byte.

  @Test
  public void testHeaderOccupiesFixedWireOffsetsInDeclaredOrder() {
    AckMessage message = new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 7L);
    ByteBuffer framed = ByteBuffer.wrap(toByteArray(message.toByteBuffer()));

    // The specified layout, read field by field in the order the specification fixes: one framing
    // type byte, then the seventeen-byte header of version, shuffle id, partition id and sequence
    // number, then the body -- which opens with the producer id in every message of the family.
    assertEquals(StreamingShuffleMessageType.ACK.id(), framed.get());
    assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, framed.get());
    assertEquals(SHUFFLE_ID, framed.getInt());
    assertEquals(PARTITION_ID, framed.getInt());
    // An acknowledgement's sequence number is the next position it expects, one above the position
    // it acknowledges.
    assertEquals(8L, framed.getLong());
    assertEquals(MAP_ID, framed.getLong());
    assertFalse(framed.hasRemaining(), "the frame carries no bytes beyond header and body");
    // Twenty-six bytes framed: one type byte, seventeen of header and eight of producer id.
    assertEquals(26, toByteArray(message.toByteBuffer()).length);
    // The producer id sits at one offset in every type, which is what makes routing a peek: one
    // type byte plus the whole header, that is offset eighteen.
    for (StreamingShuffleMessage other : oneOfEachType()) {
      ByteBuffer otherFramed = other.toByteBuffer();
      assertEquals(MAP_ID, otherFramed.getLong(otherFramed.position() + 18),
          other.getClass().getSimpleName());
      assertEquals(MAP_ID, StreamingShuffleMessage.peekMapId(otherFramed));
      assertEquals(SHUFFLE_ID, StreamingShuffleMessage.peekShuffleId(otherFramed));
    }
  }

  @Test
  public void testDataBlockBodyIsChecksumThenLengthPrefixedPayload() {
    byte[] contents = payload(3);
    DataBlockMessage block = DataBlockMessage.withComputedChecksum(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, contents);
    ByteBuffer framed = ByteBuffer.wrap(toByteArray(block.toByteBuffer()));

    assertEquals(StreamingShuffleMessageType.DATA_BLOCK.id(), framed.get());
    assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, framed.get());
    assertEquals(SHUFFLE_ID, framed.getInt());
    assertEquals(PARTITION_ID, framed.getInt());
    assertEquals(SEQUENCE_NUMBER, framed.getLong());
    assertEquals(MAP_ID, framed.getLong());
    assertEquals(StreamingShuffleChecksum.computeBlock(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, contents), framed.getLong());
    assertEquals(contents.length, framed.getInt());
    byte[] onTheWire = new byte[contents.length];
    framed.get(onTheWire);
    assertArrayEquals(contents, onTheWire);
    assertFalse(framed.hasRemaining());
  }

  @Test
  public void testHeartbeatBodyIsProducerIdThenConsumerToken() {
    HeartbeatMessage message = heartbeat();
    ByteBuffer framed = ByteBuffer.wrap(toByteArray(message.toByteBuffer()));

    assertEquals(StreamingShuffleMessageType.HEARTBEAT.id(), framed.get());
    assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, framed.get());
    assertEquals(SHUFFLE_ID, framed.getInt());
    assertEquals(PARTITION_ID, framed.getInt());
    assertEquals(SEQUENCE_NUMBER, framed.getLong());
    assertEquals(MAP_ID, framed.getLong());
    assertEquals(CONSUMER_TOKEN, framed.getLong());
    // Nothing follows the token, so the frame has exactly one size and a peer cannot choose it.
    assertFalse(framed.hasRemaining());
    assertEquals(34, toByteArray(message.toByteBuffer()).length);
  }

  @Test
  public void testShuffleIdAndPartitionIdAreNeverTransposed() {
    // Two adjacent int fields in one header are a transposition hazard, so assert they survive a
    // real round trip rather than trusting encoder and decoder to agree with each other.
    AckMessage decoded = (AckMessage) roundTrip(new AckMessage(1, MAP_ID, 2, 4L));
    assertEquals(1, decoded.shuffleId());
    assertEquals(2, decoded.partitionId());
    assertEquals(5L, decoded.sequenceNumber());
    assertEquals(4L, decoded.consumerPosition());
    // And the swapped identity is a different message, so a transposition could not go unnoticed.
    assertNotEquals(decoded, new AckMessage(2, MAP_ID, 1, 4L));
  }

  @Test
  public void testHeaderRecordCarriesTheFourFieldsInOrder() {
    // The record exists so a decoder cannot transpose two same-typed fields; that guarantee is
    // only worth anything if the constructor taking it preserves the mapping. All four components
    // are asserted, because a mapping that drops one is exactly as wrong as one that swaps two.
    StreamingShuffleMessage.Header header = header();
    AckMessage fromHeader = new AckMessage(header, MAP_ID);

    assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, fromHeader.protocolVersion());
    assertEquals(SHUFFLE_ID, fromHeader.shuffleId());
    assertEquals(MAP_ID, fromHeader.mapId());
    assertEquals(PARTITION_ID, fromHeader.partitionId());
    assertEquals(SEQUENCE_NUMBER, fromHeader.sequenceNumber());
    assertEquals(SEQUENCE_NUMBER - 1L, fromHeader.consumerPosition());
    assertEquals(new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER - 1L),
        fromHeader);
    // The same header drives every control message, so each must read it identically.
    assertEquals(heartbeat(), new HeartbeatMessage(header, MAP_ID, CONSUMER_TOKEN));
    assertEquals(heartbeatWithoutToken(), new HeartbeatMessage(header, MAP_ID));
    assertEquals(retransmit(SEQUENCE_NUMBER),
        new RetransmitRequestMessage(header, MAP_ID, SEQUENCE_NUMBER));
    assertEquals(termination(SEQUENCE_NUMBER), new StreamTerminationMessage(header, MAP_ID));
  }

  // Buffer position discipline: a receive buffer rarely starts at zero.

  @Test
  public void testPeekProtocolVersionHonoursANonZeroBufferPosition() {
    ByteBuffer framed = ack(0L).toByteBuffer();
    ByteBuffer padded = ByteBuffer.allocate(framed.remaining() + 4);
    padded.putInt(0xDEADBEEF);
    padded.put(framed.duplicate());
    padded.position(4);

    assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
        StreamingShuffleMessage.peekProtocolVersion(padded));
    assertEquals(4, padded.position(), "peeking must not advance the caller's buffer");
  }

  @Test
  public void testDecodeHonoursAnOffsetPositionWithoutDisturbingTheCaller() {
    // A receive buffer rarely starts at position zero, so the decoder has to honour position and
    // limit rather than assuming it owns the whole backing array.
    byte[] framed =
        toByteArray(new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 9L).toByteBuffer());
    ByteBuffer buffer = ByteBuffer.allocate(framed.length + 6);
    buffer.position(3);
    buffer.put(framed);
    buffer.position(3).limit(3 + framed.length);

    assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
        StreamingShuffleMessage.peekProtocolVersion(buffer));
    assertEquals(3, buffer.position());

    AckMessage decoded = (AckMessage) StreamingShuffleMessage.Decoder.fromByteBuffer(buffer);
    assertEquals(9L, decoded.consumerPosition());
    assertEquals(SHUFFLE_ID, decoded.shuffleId());
    assertEquals(PARTITION_ID, decoded.partitionId());
    assertEquals(3, buffer.position(), "decoding must not advance the caller's buffer");
  }

  // Version diagnostics and the order in which a frame's fields are checked.

  @Test
  public void testCheckProtocolVersionNamesBothRevisions() {
    // The mismatch message is what an operator reads when streaming falls back to sort-based
    // shuffle, so it has to name the peer's revision and this build's, not merely fail.
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.checkProtocolVersion(UNSUPPORTED_VERSION));
    assertTrue(error.getMessage().contains(String.valueOf(UNSUPPORTED_VERSION)),
        error.getMessage());
    assertTrue(error.getMessage().contains(
        String.valueOf(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)), error.getMessage());
  }

  @Test
  public void testVersionIsCheckedBeforeTheTypeByteIsResolved() {
    // An unknown type from an incompatible peer is still a version problem, because that is the
    // condition the fallback policy acts on. Blaming the type byte would send it down the wrong
    // path entirely.
    ByteBuf frame = buffer();
    frame.writeByte(UNKNOWN_TYPE_ID);
    frame.writeByte(UNSUPPORTED_VERSION);
    frame.writeInt(SHUFFLE_ID);
    frame.writeInt(PARTITION_ID);
    frame.writeLong(SEQUENCE_NUMBER);
    frame.writeLong(0L);
    ByteBuffer framed = frame.nioBuffer();

    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.Decoder.fromByteBuffer(framed));
    assertTrue(error.getMessage().contains("Incompatible"), error.getMessage());
  }

  // Null rejection at every construction and decode entry point.

  @Test
  public void testNullPayloadIsRejectedByEveryConstructionRoute() {
    assertThrows(NullPointerException.class, () -> DataBlockMessage.withComputedChecksum(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, null));
    assertThrows(NullPointerException.class,
        () -> new DataBlockMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, 0L, null));
    assertThrows(NullPointerException.class,
        () -> new DataBlockMessage(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
            SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, 0L, null));
    assertThrows(NullPointerException.class,
        () -> new DataBlockMessage(header(), MAP_ID, 0L, null));
    assertThrows(NullPointerException.class,
        () -> DataBlockMessage.withOwnedPayload(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
            SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, 0L, null));
    assertThrows(NullPointerException.class, () -> StreamingShuffleChecksum.verify(null, 0L));
  }

  @Test
  public void testNullHeaderIsRejectedByEveryConstructionRoute() {
    assertThrows(NullPointerException.class, () -> new AckMessage(null, MAP_ID));
    assertThrows(NullPointerException.class, () -> new HeartbeatMessage(null, MAP_ID));
    assertThrows(NullPointerException.class,
        () -> new HeartbeatMessage(null, MAP_ID, CONSUMER_TOKEN));
    assertThrows(NullPointerException.class,
        () -> new RetransmitRequestMessage(null, MAP_ID, 0L));
    assertThrows(NullPointerException.class, () -> new StreamTerminationMessage(null, MAP_ID));
    assertThrows(NullPointerException.class,
        () -> new DataBlockMessage(null, MAP_ID, 0L, payload(4)));
  }

  @Test
  public void testNullBufferIsRejectedByEveryDecodeEntryPoint() {
    assertThrows(NullPointerException.class,
        () -> StreamingShuffleMessage.Decoder.fromByteBuffer(null));
    assertThrows(NullPointerException.class,
        () -> StreamingShuffleMessage.Decoder.fromByteBuffer(null, SHUFFLE_ID, MAP_ID,
            PARTITION_ID));
    assertThrows(NullPointerException.class,
        () -> StreamingShuffleMessage.peekProtocolVersion(null));
    assertThrows(NullPointerException.class, () -> DataBlockMessage.decode(null));
    assertThrows(NullPointerException.class, () -> AckMessage.decode(null));
    assertThrows(NullPointerException.class, () -> HeartbeatMessage.decode(null));
    assertThrows(NullPointerException.class, () -> RetransmitRequestMessage.decode(null));
    assertThrows(NullPointerException.class, () -> StreamTerminationMessage.decode(null));
  }

  // Data block body truncation, distinguished by where the frame runs out.

  @Test
  public void testDataBlockDecodeRejectsATruncatedChecksum() {
    // A well-formed header followed by four bytes: not enough for the eight-byte checksum, so the
    // checksum must never be read at all.
    ByteBuf buf = buffer();
    writeHeader(buf, SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER);
    buf.writeBytes(new byte[] {1, 2, 3, 4});
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.decode(buf));
    assertTrue(error.getMessage().contains("Truncated streaming shuffle data block"),
        error.getMessage());
  }

  @Test
  public void testDataBlockDecodeRejectsAMissingPayloadLengthPrefix() {
    // Header and checksum present, length prefix absent or partial. The body minimum is checked
    // as one quantity ahead of any read, so the checksum is never consumed off a body that cannot
    // also carry the prefix behind it, and the diagnostic names the shortfall in body bytes.
    for (int bodyBytesAfterHeader : new int[] {0, 8, 16, 17, 19}) {
      ByteBuf buf = buffer();
      buf.writeByte(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION);
      buf.writeInt(SHUFFLE_ID);
      buf.writeInt(PARTITION_ID);
      buf.writeLong(SEQUENCE_NUMBER);
      buf.writeBytes(new byte[bodyBytesAfterHeader]);
      IllegalArgumentException error =
          assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.decode(buf));
      assertTrue(error.getMessage().contains("Truncated streaming shuffle"), error.getMessage());
    }
    // Exactly the minimum body is a zero-length payload, which is legal, so the bound is not
    // off by one in the refusing direction.
    ByteBuf minimal = buffer();
    writeHeader(minimal, SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER);
    minimal.writeLong(StreamingShuffleChecksum.computeBlock(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, new byte[0]));
    minimal.writeInt(0);
    DataBlockMessage decoded = DataBlockMessage.decode(minimal);
    assertEquals(0, decoded.payloadLength());
    assertTrue(decoded.verifyChecksum());
  }

  @Test
  public void testPayloadLengthPrefixLongerThanTheFrameIsRejected() {
    // A prefix claiming more than the frame holds is the allocation-amplification case, and the
    // diagnostic has to name the shortfall so it is not confused with an over-size prefix.
    ByteBuf buf = dataBlockBodyWithPayloadPrefix(64, 16);
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.decode(buf));
    assertTrue(error.getMessage().contains("Truncated streaming shuffle data block payload"),
        error.getMessage());
    assertTrue(error.getMessage().contains("64"), error.getMessage());
    assertTrue(error.getMessage().contains("16"), error.getMessage());
  }

  // Legality boundaries at the low end of every domain.

  @Test
  public void testZeroIsAValidHeaderIdentity() {
    // The bound is non-negative, not positive: shuffle 0, partition 0, sequence 0 is the first
    // block of the first partition of the first shuffle and must round trip.
    AckMessage first = new AckMessage(0, 0L, 0, AckMessage.NOTHING_CONSUMED);
    assertEquals(first, roundTrip(first));
    assertEquals(0, first.shuffleId());
    assertEquals(0, first.partitionId());
    assertEquals(0L, first.mapId());
    assertEquals(0L, first.sequenceNumber());
    DataBlockMessage firstBlock = DataBlockMessage.withComputedChecksum(0, MAP_ID, 0, 0L,
        payload(8));
    assertEquals(firstBlock, roundTrip(firstBlock));
  }

  @Test
  public void testEmptyPayloadIsLegalOnTheWireAndVerifies() {
    // Empty is a wire-level legality: the size cap is an upper bound only, and a block carrying
    // nothing has to survive a framed round trip and still verify.
    DataBlockMessage empty = dataBlock(0);
    DataBlockMessage decoded = (DataBlockMessage) roundTrip(empty);
    assertEquals(empty, decoded);
    assertEquals(0, decoded.payloadLength());
    assertEquals(0, decoded.copyPayload().length);
    assertTrue(decoded.verifyChecksum());
    // The checksum of nothing travels the same path as the checksum of anything else.
    assertEquals(StreamingShuffleChecksum.computeBlock(SHUFFLE_ID, MAP_ID, PARTITION_ID,
        SEQUENCE_NUMBER,
        new byte[0]), decoded.checksum());
  }

  @Test
  public void testExtremeLegalHeaderValuesSurviveRoundTrip() {
    // The largest values each field admits are not rejected, so they must survive faithfully
    // rather than being silently normalised or wrapped.
    AckMessage extreme = new AckMessage(Integer.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE,
        AckMessage.MAX_CONSUMER_POSITION);
    AckMessage decoded = (AckMessage) roundTrip(extreme);
    assertEquals(Integer.MAX_VALUE, decoded.shuffleId());
    assertEquals(Integer.MAX_VALUE, decoded.partitionId());
    assertEquals(Long.MAX_VALUE, decoded.mapId());
    assertEquals(Long.MAX_VALUE, decoded.sequenceNumber());
    assertEquals(AckMessage.MAX_CONSUMER_POSITION, decoded.consumerPosition());

    HeartbeatMessage beat =
        new HeartbeatMessage(0, Long.MAX_VALUE, 0, Long.MAX_VALUE, Long.MAX_VALUE);
    assertEquals(beat, roundTrip(beat));
    assertEquals(Long.MAX_VALUE, roundTrip(beat).sequenceNumber());
    assertEquals(Long.MAX_VALUE, ((HeartbeatMessage) roundTrip(beat)).consumerToken());
  }

  // Rendering: every message names its own body field and stays bounded.

  @Test
  public void testControlMessagesRenderTheirOwnBodyFieldAndStayBounded() {
    assertTrue(ack(40L).toString().contains("consumerPosition=40"), ack(40L).toString());
    assertTrue(heartbeat().toString().contains("sequenceNumber=" + SEQUENCE_NUMBER),
        heartbeat().toString());
    assertTrue(heartbeat().toString().contains("consumerToken=" + CONSUMER_TOKEN),
        heartbeat().toString());
    assertTrue(retransmit(44L).toString().contains("sequenceNumber=44"),
        retransmit(44L).toString());
    assertTrue(termination(40L).toString().contains("totalBlocks=40"), termination(40L).toString());
    for (StreamingShuffleMessage message : oneOfEachType()) {
      String rendered = message.toString();
      assertTrue(rendered.startsWith(message.getClass().getSimpleName()), rendered);
      assertTrue(rendered.contains(String.valueOf(SHUFFLE_ID)), rendered);
      assertTrue(rendered.contains(String.valueOf(PARTITION_ID)), rendered);
      assertTrue(rendered.contains(String.valueOf(MAP_ID)), rendered);
      assertTrue(rendered.contains(String.valueOf(message.sequenceNumber())), rendered);
      // These strings reach diagnostics on every backpressure event, so none may be unbounded.
      assertTrue(rendered.length() < 200, "toString is too long: " + rendered);
    }
  }

  // The ownership-transferring factory, which is the one route that does not copy.

  @Test
  public void testWithOwnedPayloadAdoptsTheArrayItIsGiven() {
    // The receive path allocates the array a line before handing it over, so copying it again
    // would cost up to two mebibytes per block for no benefit. This factory therefore adopts the
    // array, and that difference from the copying constructors is the whole reason it exists.
    byte[] surrendered = payload(64);
    DataBlockMessage adopted = DataBlockMessage.withOwnedPayload(
        StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, SHUFFLE_ID, MAP_ID, PARTITION_ID,
        SEQUENCE_NUMBER, StreamingShuffleChecksum.computeBlock(
            SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, surrendered), surrendered);
    assertTrue(adopted.verifyChecksum());
    assertEquals(64, adopted.payloadLength());
    surrendered[0] = (byte) ~surrendered[0];
    assertEquals(surrendered[0], adopted.copyPayload()[0], "the factory adopts rather than copies");

    // The size cap and the round trip apply to the adopting route exactly as to any other.
    assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.withOwnedPayload(
        StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, SHUFFLE_ID, MAP_ID, PARTITION_ID,
        SEQUENCE_NUMBER, 0L, new byte[DataBlockMessage.MAX_BLOCK_SIZE_BYTES + 1]));
    assertEquals(adopted, roundTrip(adopted));
  }

  // The shape of the protocol surface itself: internal, final, and not instantiable by mistake.

  @Test
  public void testEveryProtocolTypeIsMarkedPrivateApi() {
    // These types are public only because the producer-side and consumer-side handlers live in
    // another package. Carrying @Private is what keeps them out of the compatibility-checked
    // surface, so an absent annotation would silently widen Spark's public API.
    Class<?>[] types = {
        StreamingShuffleMessage.class, StreamingShuffleMessageType.class,
        StreamingShuffleChecksum.class, DataBlockMessage.class, AckMessage.class,
        HeartbeatMessage.class, RetransmitRequestMessage.class, StreamTerminationMessage.class,
        StreamingShuffleMessage.Header.class, StreamingShuffleMessage.Decoder.class,
    };
    for (Class<?> type : types) {
      assertNotNull(type.getAnnotation(Private.class), type.getName() + " is missing @Private");
    }
  }

  @Test
  public void testConcreteMessageTypesAreFinalAndTheBaseIsAbstract() {
    // Final concrete messages mean the validation each constructor performs cannot be bypassed by
    // a subclass, which is what lets a decoded message be trusted without re-checking it.
    Class<?>[] concrete = {
        DataBlockMessage.class, AckMessage.class, HeartbeatMessage.class,
        RetransmitRequestMessage.class, StreamTerminationMessage.class,
    };
    for (Class<?> type : concrete) {
      assertTrue(Modifier.isFinal(type.getModifiers()), type.getName() + " is not final");
      assertTrue(StreamingShuffleMessage.class.isAssignableFrom(type), type.getName());
    }
    assertTrue(Modifier.isAbstract(StreamingShuffleMessage.class.getModifiers()));
    // A record is implicitly final, and the header being immutable is what makes passing it
    // around instead of five positional arguments safe.
    assertTrue(StreamingShuffleMessage.Header.class.isRecord());
    assertTrue(Modifier.isFinal(StreamingShuffleMessage.Header.class.getModifiers()));
  }

  @Test
  public void testChecksumHelperCannotBeInstantiated() {
    // A holder of static methods with per-call state; an instance would only invite a caller to
    // believe the CRC32C is being reused across calls when it is not.
    Constructor<?>[] constructors = StreamingShuffleChecksum.class.getDeclaredConstructors();
    assertEquals(1, constructors.length);
    assertTrue(Modifier.isPrivate(constructors[0].getModifiers()));
  }

  @Test
  public void testOnlyTheFramingByteDistinguishesTheFixedSizeMessagesOnTheWire() {
    // The control messages that carry no field of their own are the header plus the producer id, so
    // messages built over one identity encode to byte-for-byte identical bodies. Only the framing
    // discriminator tells them apart on the wire, and the decoder must therefore rely on it alone,
    // never on a length and never on a body byte. A heartbeat is excluded because it carries a
    // field of its own, not because it is any less strict about the discriminator.
    StreamingShuffleMessage[] messages = {
        ack(SEQUENCE_NUMBER - 1L),
        termination(SEQUENCE_NUMBER),
    };
    byte[] reference = encodedBody(messages[0]);
    assertEquals(25, reference.length);
    for (StreamingShuffleMessage message : messages) {
      assertEquals(25, message.encodedLength());
      assertArrayEquals(reference, encodedBody(message), message.toString());
      byte[] framed = toByteArray(message.toByteBuffer());
      assertEquals(26, framed.length);
      // Identical bodies, distinct type bytes, and each still decodes back to its own class.
      assertArrayEquals(reference, Arrays.copyOfRange(framed, 1, framed.length));
      StreamingShuffleMessage decoded =
          StreamingShuffleMessage.Decoder.fromByteBuffer(ByteBuffer.wrap(framed));
      assertSame(message.getClass(), decoded.getClass());
      assertEquals(message, decoded);
    }
  }

  @Test
  public void testChecksumMatchesThePublishedCheckValue() {
    // Anchored to the Castagnoli check value rather than only to the JDK, so that the algorithm
    // this protocol claims to speak is pinned to the published one. A consumer built against a
    // different CRC32C would otherwise agree with its own producer and with nobody else.
    assertEquals(0xE3069283L,
        StreamingShuffleChecksum.compute("123456789".getBytes(StandardCharsets.UTF_8)));
    assertTrue(StreamingShuffleChecksum.verify(
        "123456789".getBytes(StandardCharsets.UTF_8), 0xE3069283L));
  }

  @Test
  public void testNonCurrentProtocolVersionIsPreservedButItsFramedFormIsRejected() {
    // Construction with a foreign version is deliberately permitted, because that is how a
    // decoded message reports the revision its peer actually spoke. What must not happen is such
    // a message being accepted back off the wire as if it were current.
    AckMessage foreign =
        new AckMessage(UNSUPPORTED_VERSION, SHUFFLE_ID, MAP_ID, PARTITION_ID, 40L);
    assertEquals(UNSUPPORTED_VERSION, foreign.protocolVersion());
    assertEquals(40L, foreign.consumerPosition());
    // The version participates in identity, so a foreign-version message is not the current one.
    assertNotEquals(ack(40L), foreign);

    ByteBuffer framed = foreign.toByteBuffer();
    assertEquals(UNSUPPORTED_VERSION, StreamingShuffleMessage.peekProtocolVersion(framed));
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.Decoder.fromByteBuffer(framed));
    // And the concrete decoder refuses it too, so the check does not live only in the dispatcher.
    ByteBuf body = buffer();
    body.writeByte(UNSUPPORTED_VERSION);
    body.writeInt(SHUFFLE_ID);
    body.writeInt(PARTITION_ID);
    body.writeLong(SEQUENCE_NUMBER);
    body.writeLong(MAP_ID);
    assertThrows(IllegalArgumentException.class, () -> AckMessage.decode(body));
  }

  @Test
  public void testEveryMessageReportsItsOwnDiscriminatorBeforeAndAfterTheWire() {
    // The discriminator a class reports and the byte the framing writes are two expressions of the
    // same fact, and the decoder trusts the byte. Asserting them equal for a locally built message
    // and again for the decoded one is what keeps the two from drifting apart, in either direction.
    StreamingShuffleMessageType[] expected = {
        StreamingShuffleMessageType.DATA_BLOCK,
        StreamingShuffleMessageType.ACK,
        StreamingShuffleMessageType.HEARTBEAT,
        StreamingShuffleMessageType.RETRANSMIT_REQUEST,
        StreamingShuffleMessageType.STREAM_TERMINATION,
    };
    StreamingShuffleMessage[] messages = oneOfEachType();
    assertEquals(expected.length, messages.length);
    assertEquals(expected.length, StreamingShuffleMessageType.values().length);
    for (int i = 0; i < messages.length; i++) {
      StreamingShuffleMessage message = messages[i];
      assertEquals(expected[i], message.type());
      assertEquals(expected[i].id(), toByteArray(message.toByteBuffer())[0]);
      StreamingShuffleMessage decoded =
          StreamingShuffleMessage.Decoder.fromByteBuffer(message.toByteBuffer());
      assertEquals(expected[i], decoded.type());
      assertSame(message.getClass(), decoded.getClass());
    }
  }

  // Helpers.

  /**
   * Framed round trip asserting equals, hashCode and toString, mirroring the helper that
   * {@code BlockTransferMessagesSuite} uses for the sibling message family.
   */
  private StreamingShuffleMessage checkSerializeDeserialize(StreamingShuffleMessage msg) {
    StreamingShuffleMessage msg2 =
        StreamingShuffleMessage.Decoder.fromByteBuffer(msg.toByteBuffer());
    assertEquals(msg, msg2);
    assertEquals(msg.hashCode(), msg2.hashCode());
    assertEquals(msg.toString(), msg2.toString());
    return msg2;
  }

  private StreamingShuffleMessage roundTrip(StreamingShuffleMessage msg) {
    return StreamingShuffleMessage.Decoder.fromByteBuffer(msg.toByteBuffer());
  }

  /** One message of each of the five types, all sharing the same header values. */
  private StreamingShuffleMessage[] oneOfEachType() {
    return new StreamingShuffleMessage[] {
        dataBlock(1024), ack(40L), heartbeat(), retransmit(44L), termination(SEQUENCE_NUMBER),
    };
  }

  /**
   * The control messages that carry no field of their own, every one of which encodes to exactly
   * twenty-five bytes: the header and the producer id and nothing else. A heartbeat is not among
   * them -- it carries a consumer session token -- and neither is a retransmission request, which
   * carries the upper bound of its repair window. Both are still of a width this build fixes.
   */
  private StreamingShuffleMessage[] fixedSizeMessages() {
    return new StreamingShuffleMessage[] {
        ack(40L), termination(SEQUENCE_NUMBER)};
  }

  /**
   * Every message whose decoder demands an exact body, which is all four control messages: each has
   * a body of one legal size, whether that is the shared twenty-five bytes or the wider fixed size
   * a retransmission request occupies.
   */
  private StreamingShuffleMessage[] exactBodyMessages() {
    return new StreamingShuffleMessage[] {
        ack(40L), heartbeat(), retransmit(44L), termination(SEQUENCE_NUMBER)};
  }

  private DataBlockMessage dataBlock(int payloadLength) {
    return DataBlockMessage.withComputedChecksum(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, payload(payloadLength));
  }

  /** A header carrying this suite's standard identity, for the header-taking constructors. */
  private StreamingShuffleMessage.Header header() {
    return new StreamingShuffleMessage.Header(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
        SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER);
  }

  private AckMessage ack(long consumerPosition) {
    return new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, consumerPosition);
  }

  private HeartbeatMessage heartbeat() {
    return new HeartbeatMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, CONSUMER_TOKEN);
  }

  /**
   * A heartbeat that declares no consumer session token, which is what a producer's own heartbeat
   * is and what a peer speaking an earlier revision of this protocol sends.
   */
  private HeartbeatMessage heartbeatWithoutToken() {
    return new HeartbeatMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER);
  }

  private RetransmitRequestMessage retransmit(long sequenceNumber) {
    return new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, sequenceNumber);
  }

  private StreamTerminationMessage termination(long totalBlocks) {
    // Only data blocks consume sequence numbers, so a terminator's own position is exactly the
    // number of blocks that preceded it: the two values are one value, carried by the header.
    return new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, totalBlocks);
  }

  /**
   * A deterministic payload of the requested length. Filled from the index rather than from a
   * random source so that every run checksums identical bytes.
   */
  private static byte[] payload(int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (i * 31 + 7);
    }
    return bytes;
  }

  /** Records the allocation gate's call order and ownership transitions without allocating. */
  private static final class RecordingPayloadReservation
      implements StreamingShuffleMessage.PayloadReservation {

    private final boolean grant;
    private int attempts;
    private int releases;
    private int shuffleId = -1;
    private long mapId = -1L;
    private int partitionId = -1;
    private long sequenceNumber = -1L;
    private int payloadBytes = -1;
    private int reservedBytes;

    private RecordingPayloadReservation(boolean grant) {
      this.grant = grant;
    }

    @Override
    public boolean tryReserve(
        int observedShuffleId,
        long observedMapId,
        int observedPartitionId,
        long observedSequenceNumber,
        int observedPayloadBytes) {
      attempts++;
      shuffleId = observedShuffleId;
      mapId = observedMapId;
      partitionId = observedPartitionId;
      sequenceNumber = observedSequenceNumber;
      payloadBytes = observedPayloadBytes;
      if (grant) {
        reservedBytes += observedPayloadBytes;
      }
      return grant;
    }

    @Override
    public void release(int releasedPayloadBytes) {
      releases++;
      reservedBytes -= releasedPayloadBytes;
    }
  }

  private static byte[] toByteArray(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  /** The encoded body of a message, that is, its framed form without the leading type byte. */
  private static byte[] encodedBody(StreamingShuffleMessage message) {
    // Released here rather than tracked, because the buffer does not escape this method: the bytes
    // the caller receives are a copy of it.
    ByteBuf buf = Unpooled.buffer(message.encodedLength());
    try {
      message.encode(buf);
      byte[] body = new byte[buf.readableBytes()];
      buf.readBytes(body);
      return body;
    } finally {
      buf.release();
    }
  }

  private static void writeHeader(ByteBuf buf, int shuffleId, int partitionId, long sequence) {
    writeHeader(buf, shuffleId, MAP_ID, partitionId, sequence);
  }

  /**
   * Writes a header and the producer id that opens every body, by hand and in exactly the field
   * order the encoders use, so that a frame carrying a value the constructors would refuse can be
   * presented to a decoder anyway. The seventeen header bytes come first -- version, shuffle id,
   * partition id, sequence number -- and the eight-byte producer id follows them as the first body
   * field, which is the layout the specification fixes.
   */
  private static void writeHeader(
      ByteBuf buf, int shuffleId, long mapId, int partitionId, long sequence) {
    buf.writeByte(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION);
    buf.writeInt(shuffleId);
    buf.writeInt(partitionId);
    buf.writeLong(sequence);
    buf.writeLong(mapId);
  }

  /**
   * A hand-crafted data block body carrying a chosen payload length prefix followed by a chosen
   * number of actual payload bytes, so that a hostile prefix can be presented without the frame
   * ever growing to the size that prefix claims.
   */
  private ByteBuf dataBlockBodyWithPayloadPrefix(int declaredLength, int actualBytes) {
    // Tracked rather than released here, because this buffer is handed back to the caller: its
    // release belongs to the teardown that owns every buffer this suite allocates.
    ByteBuf buf = buffer();
    writeHeader(buf, SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER);
    buf.writeLong(0L);
    buf.writeInt(declaredLength);
    buf.writeBytes(new byte[actualBytes]);
    return buf;
  }

  /** The specified encoded size of each fixed control message: header plus producer id. */
  private static final int CONTROL_MESSAGE_BYTES = 25;

  /**
   * Encoded length of a retransmission request: a control message plus the eight bytes of the
   * inclusive upper bound that lets one frame name a whole repair window.
   */
  private static final int RETRANSMIT_REQUEST_BYTES = CONTROL_MESSAGE_BYTES + 8;

  /**
   * Encoded length of a heartbeat: a control message plus the eight bytes of the consumer session
   * token that lets a producer recognise a reconnection as the consumer it already knows.
   */
  private static final int HEARTBEAT_BYTES = CONTROL_MESSAGE_BYTES + 8;

  /** The specified encoded size of the shared header. */
  private static final int HEADER_BYTES = 17;

  /** The specified encoded size of a data block with an empty payload. */
  private static final int EMPTY_DATA_BLOCK_BYTES = 37;

  /** The one byte of type prefix a framed message carries ahead of its encoded body. */
  private static final int FRAME_TYPE_PREFIX_BYTES = 1;

  private static void decodeAckWithHeader(int shuffleId, int partitionId, long sequence) {
    decodeAckWithHeader(shuffleId, MAP_ID, partitionId, sequence);
  }

  /** As {@link #decodeAckWithHeader(int, int, long)}, but with the map id chosen as well. */
  private static void decodeAckWithHeader(
      int shuffleId, long mapId, int partitionId, long sequence) {
    // The decode below is expected to raise, so the release belongs in a finally: a buffer freed
    // only on the success path is a buffer leaked by every case this helper exists to exercise.
    ByteBuf buf = Unpooled.buffer();
    try {
      writeHeader(buf, shuffleId, mapId, partitionId, sequence);
      AckMessage.decode(buf);
    } finally {
      buf.release();
    }
  }

  /**
   * Decodes a heartbeat body carrying the given consumer session token, hand-written so that a
   * value the constructors refuse can still be presented to the decoder -- which is the point: the
   * domain rule has to hold on the route a remote peer actually uses.
   */
  private static HeartbeatMessage decodeHeartbeatWithToken(long consumerToken) {
    ByteBuf buf = Unpooled.buffer();
    writeHeader(buf, SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER);
    buf.writeLong(consumerToken);
    return HeartbeatMessage.decode(buf);
  }

  /** Routes a buffer to the concrete decoder matching the given message's type. */
  private static void decodeAs(StreamingShuffleMessage template, ByteBuf buf) {
    if (template instanceof AckMessage) {
      AckMessage.decode(buf);
    } else if (template instanceof HeartbeatMessage) {
      HeartbeatMessage.decode(buf);
    } else if (template instanceof RetransmitRequestMessage) {
      RetransmitRequestMessage.decode(buf);
    } else if (template instanceof StreamTerminationMessage) {
      StreamTerminationMessage.decode(buf);
    } else {
      DataBlockMessage.decode(buf);
    }
  }

  /**
   * Asserts that the framing type byte of a message equals the given discriminator's id, read
   * absolutely so that the buffer's position is not advanced.
   */
  private static void assertFramingTypeByte(
      StreamingShuffleMessageType expected,
      StreamingShuffleMessage message) {
    ByteBuffer framed = message.toByteBuffer();
    assertEquals(expected.id(), framed.get(framed.position()));
    assertEquals(message.encodedLength() + 1, framed.remaining());
  }

  /**
   * Whether a block re-addressed to the given identity still verifies against the supplied
   * checksum. Used to show that a rewritten header is detected.
   */
  private static boolean reAddressed(
      int partitionId,
      long sequenceNumber,
      int shuffleId,
      byte[] payload,
      long checksum) {
    return new DataBlockMessage(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, shuffleId,
        MAP_ID, partitionId, sequenceNumber, checksum, payload).verifyChecksum();
  }
}
