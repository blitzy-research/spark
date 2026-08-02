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
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32C;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import org.apache.spark.annotation.Private;

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
   * A fixed timestamp literal. The heartbeat's timestamp is caller-supplied precisely so that this
   * suite never has to read a clock, which is what keeps it deterministic.
   */
  private static final long TIMESTAMP_MS = 1700000000000L;

  /** A protocol version this build does not speak, used to exercise the compatibility check. */
  private static final byte UNSUPPORTED_VERSION = (byte) 99;

  /** A message type id no constant uses, used to exercise unknown-discriminator handling. */
  private static final byte UNKNOWN_TYPE_ID = (byte) 9;

  // ===========================================================================================
  // Group 1: round-trip encode/decode for every message type, with exact-literal lengths.
  // ===========================================================================================

  @Test
  public void testDataBlockMessageEncodeDecode() {
    // 29 + payload.length for a data block: 17 header + 8 checksum + 4 length prefix + payload.
    for (int payloadLength : new int[] {0, 1, 1024, 8192}) {
      byte[] payload = payload(payloadLength);
      DataBlockMessage message = DataBlockMessage.withComputedChecksum(
          SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, payload);
      int len = message.encodedLength();
      assertEquals(37 + payloadLength, len);
      ByteBuf buf = Unpooled.buffer(len);
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
    AckMessage message = new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, 40L);
    int len = message.encodedLength();
    assertEquals(33, len);
    ByteBuf buf = Unpooled.buffer(len);
    message.encode(buf);
    assertEquals(0, buf.writableBytes());

    AckMessage decoded = AckMessage.decode(buf);
    assertEquals(message, decoded);
    assertEquals(40L, decoded.consumerPosition());
    assertEquals(40L, decoded.consumerPosition);
  }

  @Test
  public void testHeartbeatMessageEncodeDecode() {
    HeartbeatMessage message =
        new HeartbeatMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, TIMESTAMP_MS);
    int len = message.encodedLength();
    assertEquals(33, len);
    ByteBuf buf = Unpooled.buffer(len);
    message.encode(buf);
    assertEquals(0, buf.writableBytes());

    HeartbeatMessage decoded = HeartbeatMessage.decode(buf);
    assertEquals(message, decoded);
    assertEquals(TIMESTAMP_MS, decoded.timestampMs());
    assertEquals(TIMESTAMP_MS, decoded.timestampMs);
  }

  @Test
  public void testRetransmitRequestMessageEncodeDecode() {
    RetransmitRequestMessage message =
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, 44L);
    int len = message.encodedLength();
    assertEquals(33, len);
    ByteBuf buf = Unpooled.buffer(len);
    message.encode(buf);
    assertEquals(0, buf.writableBytes());

    RetransmitRequestMessage decoded = RetransmitRequestMessage.decode(buf);
    assertEquals(message, decoded);
    assertEquals(SEQUENCE_NUMBER, decoded.firstSequenceNumber());
    assertEquals(44L, decoded.lastSequenceNumber());
    assertEquals(3L, decoded.blockCount());
    assertTrue(decoded.contains(43L));
    assertFalse(decoded.contains(45L));
  }

  @Test
  public void testStreamTerminationMessageEncodeDecode() {
    StreamTerminationMessage message =
        new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 40L, 40L);
    int len = message.encodedLength();
    assertEquals(33, len);
    ByteBuf buf = Unpooled.buffer(len);
    message.encode(buf);
    assertEquals(0, buf.writableBytes());

    StreamTerminationMessage decoded = StreamTerminationMessage.decode(buf);
    assertEquals(message, decoded);
    assertEquals(40L, decoded.totalBlocks());
    assertEquals(40L, decoded.totalBlocks);
  }

  // ===========================================================================================
  // Group 2: header integrity.
  // ===========================================================================================

  @Test
  public void testHeaderLayoutConstants() {
    assertEquals(25, StreamingShuffleMessage.HEADER_ENCODED_LENGTH);
    assertEquals(1 + 4 + 8 + 4 + 8, StreamingShuffleMessage.HEADER_ENCODED_LENGTH);
    assertEquals(1, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION);
  }

  @Test
  public void testEveryHeaderFieldSurvivesRoundTrip() {
    for (StreamingShuffleMessage message : oneOfEachType()) {
      StreamingShuffleMessage decoded =
          StreamingShuffleMessage.Decoder.fromByteBuffer(message.toByteBuffer());
      assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, decoded.protocolVersion());
      assertEquals(SHUFFLE_ID, decoded.shuffleId());
      assertEquals(PARTITION_ID, decoded.partitionId());
      assertEquals(SEQUENCE_NUMBER, decoded.sequenceNumber());
    }
  }

  @Test
  public void testDecodedMessageKeepsTheVersionItArrivedWith() {
    // A message built with an explicit version must round-trip that version rather than silently
    // adopting this build's own, which is what allows a mismatch to be reported precisely.
    AckMessage message = new AckMessage(
        StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, SHUFFLE_ID, MAP_ID, PARTITION_ID,
        SEQUENCE_NUMBER, 40L);
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
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, 40L).toByteBuffer());
    framed[1] = UNSUPPORTED_VERSION;
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.Decoder.fromByteBuffer(ByteBuffer.wrap(framed)));
  }

  // ===========================================================================================
  // Group 3: framed round trip through toByteBuffer and Decoder.fromByteBuffer.
  // ===========================================================================================

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

  // ===========================================================================================
  // Group 4: the two-mebibyte size cap.
  // ===========================================================================================

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
    assertEquals(37 + DataBlockMessage.MAX_BLOCK_SIZE_BYTES, block.encodedLength());
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

  // ===========================================================================================
  // Group 5: checksum behaviour.
  // ===========================================================================================

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

  // ===========================================================================================
  // Group 6: unknown type and id handling.
  // ===========================================================================================

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

  // ===========================================================================================
  // Group 7: equals and hashCode contract.
  // ===========================================================================================

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
    assertNotEquals(base, new AckMessage(SHUFFLE_ID + 1, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER,
        40L));
    assertNotEquals(base, new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID + 1, SEQUENCE_NUMBER,
        40L));
    assertNotEquals(base, new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER + 1,
        40L));
    assertNotEquals(base, ack(41L));

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
  public void testTheFourFixedSizeMessagesAreNeverEqualToEachOther() {
    // All four encode to exactly 25 bytes and carry a single long body, so length can never tell
    // them apart. Given identical header and body values they must still be distinct, which is
    // what confirms each equals implementation checks the concrete type.
    StreamingShuffleMessage[] identical = {
        new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, SEQUENCE_NUMBER),
        new HeartbeatMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, SEQUENCE_NUMBER),
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER,
            SEQUENCE_NUMBER),
        new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER,
            SEQUENCE_NUMBER),
    };
    for (int i = 0; i < identical.length; i++) {
      assertEquals(33, identical[i].encodedLength());
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

  // ===========================================================================================
  // Frame bounds and exact consumption.
  // ===========================================================================================

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
  public void testEachFixedSizeDecoderRequiresAnExactBody() {
    // Directly against each concrete decoder, one byte short and one byte long.
    for (StreamingShuffleMessage message : fixedSizeMessages()) {
      byte[] body = encodedBody(message);
      ByteBuf shortBody = Unpooled.wrappedBuffer(Arrays.copyOf(body, body.length - 1));
      ByteBuf longBody = Unpooled.wrappedBuffer(Arrays.copyOf(body, body.length + 1));
      assertThrows(IllegalArgumentException.class, () -> decodeAs(message, shortBody));
      assertThrows(IllegalArgumentException.class, () -> decodeAs(message, longBody));
    }
  }

  // ===========================================================================================
  // Header identifier domains.
  // ===========================================================================================

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
    assertThrows(IllegalArgumentException.class, () -> new AckMessage(-1, MAP_ID, PARTITION_ID, 0L,
        0L));
    assertThrows(IllegalArgumentException.class, () -> new AckMessage(SHUFFLE_ID, MAP_ID, -1, 0L,
        0L));
    assertThrows(IllegalArgumentException.class,
        () -> new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, -1L, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> new HeartbeatMessage(-1, MAP_ID, 0, 0L, TIMESTAMP_MS));
    assertThrows(IllegalArgumentException.class, () -> new RetransmitRequestMessage(-1, MAP_ID, 0,
        0L, 0L));
    assertThrows(IllegalArgumentException.class, () -> new StreamTerminationMessage(-1, MAP_ID, 0,
        0L, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> DataBlockMessage.withComputedChecksum(-1, MAP_ID, 0, 0L, payload(8)));
    // A negative map id is rejected wherever a message can be built, exactly as a negative shuffle
    // or partition id is.
    assertThrows(IllegalArgumentException.class,
        () -> new AckMessage(SHUFFLE_ID, -1L, PARTITION_ID, 0L, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> new HeartbeatMessage(SHUFFLE_ID, -1L, PARTITION_ID, 0L, TIMESTAMP_MS));
    assertThrows(IllegalArgumentException.class,
        () -> new RetransmitRequestMessage(SHUFFLE_ID, -1L, PARTITION_ID, 0L, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> new StreamTerminationMessage(SHUFFLE_ID, -1L, PARTITION_ID, 0L, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> DataBlockMessage.withComputedChecksum(SHUFFLE_ID, -1L, PARTITION_ID, 0L, payload(8)));
    assertThrows(IllegalArgumentException.class, () -> new StreamingShuffleMessage.Header(
        StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, SHUFFLE_ID, -1L, PARTITION_ID, 0L));
    // Zero is the smallest legal value for all three and must be accepted.
    assertDoesNotThrow(() -> new AckMessage(0, MAP_ID, 0, 0L, AckMessage.NOTHING_CONSUMED));
  }

  // ===========================================================================================
  // Body domains: acknowledgement position, retransmission window, termination count.
  // ===========================================================================================

  @Test
  public void testAcknowledgementPositionDomain() {
    assertEquals(-1L, AckMessage.NOTHING_CONSUMED);
    // The one legal negative, and it must survive a round trip.
    AckMessage nothingConsumed =
        new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER,
            AckMessage.NOTHING_CONSUMED);
    assertEquals(AckMessage.NOTHING_CONSUMED, nothingConsumed.consumerPosition());
    assertEquals(nothingConsumed, roundTrip(nothingConsumed));
    // Non-negative positions are always legal.
    assertDoesNotThrow(() -> ack(0L));
    assertDoesNotThrow(() -> ack(Long.MAX_VALUE));
    // Every other negative is refused, because it would flow into the producer's buffer
    // reclamation arithmetic where it is not a small number but a broken comparison.
    for (long invalid : new long[] {-2L, -100L, Long.MIN_VALUE}) {
      assertThrows(IllegalArgumentException.class, () -> ack(invalid));
    }
  }

  @Test
  public void testAcknowledgementPositionDomainIsEnforcedOnDecode() {
    for (long invalid : new long[] {-2L, Long.MIN_VALUE}) {
      ByteBuf buf = Unpooled.buffer();
      writeHeader(buf, SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER);
      buf.writeLong(invalid);
      assertThrows(IllegalArgumentException.class, () -> AckMessage.decode(buf));
    }
  }

  @Test
  public void testHeartbeatTimestampDomain() {
    assertDoesNotThrow(() -> heartbeat());
    assertDoesNotThrow(() -> new HeartbeatMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID,
        SEQUENCE_NUMBER, 0L));
    // A negative timestamp is not clock skew; it makes the receiver's elapsed-time subtraction
    // enormous, or at Long.MIN_VALUE makes it overflow and change sign.
    for (long invalid : new long[] {-1L, Long.MIN_VALUE}) {
      assertThrows(IllegalArgumentException.class,
          () -> new HeartbeatMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, invalid));
    }
  }

  @Test
  public void testRetransmissionWindowBounds() {
    // A single-block window, where the bounds are equal, is legal.
    RetransmitRequestMessage single =
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 5L, 5L);
    assertEquals(1L, single.blockCount());
    assertTrue(single.contains(5L));
    assertFalse(single.contains(4L));
    assertFalse(single.contains(6L));
    assertEquals(single, roundTrip(single));

    // An inverted window is refused.
    assertThrows(IllegalArgumentException.class,
        () -> new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 5L, 4L));

    // The width is bounded, so a peer cannot ask a producer to walk 2^63 positions.
    assertEquals(4096L, RetransmitRequestMessage.MAX_REQUESTED_BLOCKS);
    RetransmitRequestMessage widest = new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID,
        PARTITION_ID, 0L,
        RetransmitRequestMessage.MAX_REQUESTED_BLOCKS - 1);
    assertEquals(RetransmitRequestMessage.MAX_REQUESTED_BLOCKS, widest.blockCount());
    assertThrows(IllegalArgumentException.class, () -> new RetransmitRequestMessage(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L, RetransmitRequestMessage.MAX_REQUESTED_BLOCKS));
    assertThrows(IllegalArgumentException.class,
        () -> new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L,
            Long.MAX_VALUE - 1L));
    // The one pair for which counting the window rather than measuring its span overflows: an
    // upper bound at Long.MAX_VALUE against a lower bound of zero. A count would wrap to
    // Long.MIN_VALUE and read as small; the span is exact.
    assertThrows(IllegalArgumentException.class,
        () -> new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L, Long.MAX_VALUE));
  }

  @Test
  public void testRetransmissionWindowBoundsAreEnforcedOnDecode() {
    ByteBuf inverted = Unpooled.buffer();
    writeHeader(inverted, SHUFFLE_ID, PARTITION_ID, 5L);
    inverted.writeLong(4L);
    assertThrows(IllegalArgumentException.class,
        () -> RetransmitRequestMessage.decode(inverted));

    ByteBuf tooWide = Unpooled.buffer();
    writeHeader(tooWide, SHUFFLE_ID, PARTITION_ID, 0L);
    tooWide.writeLong(Long.MAX_VALUE);
    assertThrows(IllegalArgumentException.class, () -> RetransmitRequestMessage.decode(tooWide));
  }

  @Test
  public void testStreamTerminationBlockCountRelation() {
    // Zero blocks is legal: an empty partition legitimately terminates having sent nothing.
    assertDoesNotThrow(() -> new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L,
        0L));
    // Only data blocks consume sequence numbers -- a heartbeat and the terminator both report the
    // next unissued position without claiming it -- so the terminator's position equals the number
    // of blocks that preceded it, and equality is the one legal relation.
    assertDoesNotThrow(() -> new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 10L,
        10L));
    // An undercount is the silent truncation case: a consumer that accepted ten blocks and is told
    // the total was four reconciles the two, concludes the stream completed and hands a short
    // result to the reduce task with every checksum intact. It must not be constructible.
    assertThrows(IllegalArgumentException.class,
        () -> new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 10L, 4L));
    // Claiming more blocks than there are preceding positions is arithmetically impossible, and
    // would leave a consumer waiting for blocks that were never sent.
    assertThrows(IllegalArgumentException.class,
        () -> new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 10L, 11L));
    assertThrows(IllegalArgumentException.class,
        () -> new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 10L, Long.MAX_VALUE));
    assertThrows(IllegalArgumentException.class,
        () -> new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 10L, -1L));
  }

  @Test
  public void testStreamTerminationBlockCountRelationIsEnforcedOnDecode() {
    ByteBuf overcount = Unpooled.buffer();
    writeHeader(overcount, SHUFFLE_ID, PARTITION_ID, 10L);
    overcount.writeLong(11L);
    assertThrows(IllegalArgumentException.class,
        () -> StreamTerminationMessage.decode(overcount));
    // A frame arriving from a peer is rejected for an undercount too, because decode funnels
    // through the same construction path the local producer uses.
    ByteBuf undercount = Unpooled.buffer();
    writeHeader(undercount, SHUFFLE_ID, PARTITION_ID, 10L);
    undercount.writeLong(4L);
    assertThrows(IllegalArgumentException.class,
        () -> StreamTerminationMessage.decode(undercount));
  }

  // ===========================================================================================
  // Payload ownership.
  // ===========================================================================================

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

  // ===========================================================================================
  // The map id, which is what lets one listener per executor serve every producer on it.
  // ===========================================================================================

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
    assertNotEquals(base, new AckMessage(SHUFFLE_ID, MAP_ID + 1L, PARTITION_ID, SEQUENCE_NUMBER,
        40L));
    assertNotEquals(base.hashCode(), new AckMessage(SHUFFLE_ID, MAP_ID + 1L, PARTITION_ID,
        SEQUENCE_NUMBER, 40L).hashCode());
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
  public void testMapIdWidensTheHeaderByEightBytes() {
    // The map id is a long, so every framed message grew by eight bytes. Asserted against the
    // constant rather than a literal total so that the encoder and this expectation cannot drift.
    for (StreamingShuffleMessage message : fixedSizeMessages()) {
      assertEquals(StreamingShuffleMessage.HEADER_ENCODED_LENGTH + 8, message.encodedLength());
      assertEquals(33, message.encodedLength(), message.getClass().getSimpleName());
      assertEquals(34, message.toByteBuffer().remaining());
    }
  }

  // ===========================================================================================
  // Stream-context binding.
  // ===========================================================================================

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

  // ===========================================================================================
  // Framing constants: the payload cap and the encoded-frame cap are two different resources.
  // ===========================================================================================

  @Test
  public void testFramingConstantsFormOneConsistentContract() {
    assertEquals(1, StreamingShuffleMessage.FRAME_TYPE_PREFIX_LENGTH);
    // One framing prefix + twenty-five header + eight checksum + four payload length prefix.
    assertEquals(38, DataBlockMessage.FRAMING_OVERHEAD_BYTES);
    assertEquals(1 + StreamingShuffleMessage.HEADER_ENCODED_LENGTH + 8 + 4,
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

  // ===========================================================================================
  // Exact wire layout: field order and offsets, asserted byte by byte.
  // ===========================================================================================

  @Test
  public void testHeaderOccupiesFixedWireOffsetsInDeclaredOrder() {
    AckMessage message = new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, 7L);
    ByteBuffer framed = ByteBuffer.wrap(toByteArray(message.toByteBuffer()));

    assertEquals(StreamingShuffleMessageType.ACK.id(), framed.get());
    assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, framed.get());
    assertEquals(SHUFFLE_ID, framed.getInt());
    // The map id is written between the shuffle id and the partition id, so that a router can read
    // whose output a frame concerns from a fixed offset without decoding the body.
    assertEquals(MAP_ID, framed.getLong());
    assertEquals(PARTITION_ID, framed.getInt());
    assertEquals(SEQUENCE_NUMBER, framed.getLong());
    assertEquals(7L, framed.getLong());
    assertFalse(framed.hasRemaining(), "the frame carries no bytes beyond header and body");
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
    assertEquals(MAP_ID, framed.getLong());
    assertEquals(PARTITION_ID, framed.getInt());
    assertEquals(SEQUENCE_NUMBER, framed.getLong());
    assertEquals(StreamingShuffleChecksum.computeBlock(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, contents), framed.getLong());
    assertEquals(contents.length, framed.getInt());
    byte[] onTheWire = new byte[contents.length];
    framed.get(onTheWire);
    assertArrayEquals(contents, onTheWire);
    assertFalse(framed.hasRemaining());
  }

  @Test
  public void testShuffleIdAndPartitionIdAreNeverTransposed() {
    // Two same-typed adjacent fields are the classic transposition hazard, so assert they stay
    // apart across a real round trip rather than trusting encoder and decoder to agree.
    AckMessage decoded = (AckMessage) roundTrip(new AckMessage(1, MAP_ID, 2, 3L, 4L));
    assertEquals(1, decoded.shuffleId());
    assertEquals(2, decoded.partitionId());
    assertEquals(3L, decoded.sequenceNumber());
    assertEquals(4L, decoded.consumerPosition());
    // And the swapped identity is a different message, so a transposition could not go unnoticed.
    assertNotEquals(decoded, new AckMessage(2, MAP_ID, 1, 3L, 4L));
  }

  @Test
  public void testHeaderRecordCarriesTheFourFieldsInOrder() {
    // The record exists so a decoder cannot transpose two same-typed fields; that guarantee is
    // only worth anything if the constructor taking it preserves the mapping.
    StreamingShuffleMessage.Header header = header();
    AckMessage fromHeader = new AckMessage(header, 55L);

    assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, fromHeader.protocolVersion());
    assertEquals(SHUFFLE_ID, fromHeader.shuffleId());
    assertEquals(PARTITION_ID, fromHeader.partitionId());
    assertEquals(SEQUENCE_NUMBER, fromHeader.sequenceNumber());
    assertEquals(55L, fromHeader.consumerPosition());
    assertEquals(new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, 55L),
        fromHeader);
    // The same header drives every control message, so each must read it identically.
    assertEquals(heartbeat(), new HeartbeatMessage(header, TIMESTAMP_MS));
    assertEquals(retransmit(44L), new RetransmitRequestMessage(header, 44L));
    assertEquals(termination(SEQUENCE_NUMBER),
        new StreamTerminationMessage(header, SEQUENCE_NUMBER));
  }

  // ===========================================================================================
  // Buffer position discipline: a receive buffer rarely starts at zero.
  // ===========================================================================================

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
    byte[] framed = toByteArray(new AckMessage(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, 9L).toByteBuffer());
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

  // ===========================================================================================
  // Version diagnostics and the order in which a frame's fields are checked.
  // ===========================================================================================

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
    ByteBuf frame = Unpooled.buffer();
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

  // ===========================================================================================
  // Null rejection at every construction and decode entry point.
  // ===========================================================================================

  @Test
  public void testNullPayloadIsRejectedByEveryConstructionRoute() {
    assertThrows(NullPointerException.class, () -> DataBlockMessage.withComputedChecksum(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, null));
    assertThrows(NullPointerException.class,
        () -> new DataBlockMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, 0L, null));
    assertThrows(NullPointerException.class,
        () -> new DataBlockMessage(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
            SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, 0L, null));
    assertThrows(NullPointerException.class, () -> new DataBlockMessage(header(), 0L, null));
    assertThrows(NullPointerException.class,
        () -> DataBlockMessage.withOwnedPayload(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
            SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, 0L, null));
    assertThrows(NullPointerException.class, () -> StreamingShuffleChecksum.verify(null, 0L));
  }

  @Test
  public void testNullHeaderIsRejectedByEveryConstructionRoute() {
    assertThrows(NullPointerException.class, () -> new AckMessage(null, 0L));
    assertThrows(NullPointerException.class, () -> new HeartbeatMessage(null, TIMESTAMP_MS));
    assertThrows(NullPointerException.class, () -> new RetransmitRequestMessage(null, 0L));
    assertThrows(NullPointerException.class, () -> new StreamTerminationMessage(null, 0L));
    assertThrows(NullPointerException.class, () -> new DataBlockMessage(null, 0L, payload(4)));
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

  // ===========================================================================================
  // Data block body truncation, distinguished by where the frame runs out.
  // ===========================================================================================

  @Test
  public void testDataBlockDecodeRejectsATruncatedChecksum() {
    // A well-formed header followed by four bytes: not enough for the eight-byte checksum, so the
    // checksum must never be read at all.
    ByteBuf buf = Unpooled.buffer();
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
    for (int bodyBytesAfterHeader : new int[] {0, 8, 9, 11}) {
      ByteBuf buf = Unpooled.buffer();
      writeHeader(buf, SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER);
      buf.writeBytes(new byte[bodyBytesAfterHeader]);
      IllegalArgumentException error =
          assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.decode(buf));
      assertTrue(error.getMessage().contains("Truncated streaming shuffle data block"),
          error.getMessage());
      assertTrue(error.getMessage().contains("12"), error.getMessage());
    }
    // Exactly the minimum body is a zero-length payload, which is legal, so the bound is not
    // off by one in the refusing direction.
    ByteBuf minimal = Unpooled.buffer();
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

  // ===========================================================================================
  // Legality boundaries at the low end of every domain.
  // ===========================================================================================

  @Test
  public void testZeroIsAValidHeaderIdentity() {
    // The bound is non-negative, not positive: shuffle 0, partition 0, sequence 0 is the first
    // block of the first partition of the first shuffle and must round trip.
    AckMessage first = new AckMessage(0, MAP_ID, 0, 0L, 0L);
    assertEquals(first, roundTrip(first));
    assertEquals(0, first.shuffleId());
    assertEquals(0, first.partitionId());
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
    AckMessage extreme = new AckMessage(
        Integer.MAX_VALUE, MAP_ID, Integer.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
    AckMessage decoded = (AckMessage) roundTrip(extreme);
    assertEquals(Integer.MAX_VALUE, decoded.shuffleId());
    assertEquals(Integer.MAX_VALUE, decoded.partitionId());
    assertEquals(Long.MAX_VALUE, decoded.sequenceNumber());
    assertEquals(Long.MAX_VALUE, decoded.consumerPosition());

    HeartbeatMessage beat = new HeartbeatMessage(0, MAP_ID, 0, Long.MAX_VALUE, Long.MAX_VALUE);
    assertEquals(beat, roundTrip(beat));
    assertEquals(Long.MAX_VALUE, ((HeartbeatMessage) roundTrip(beat)).timestampMs());
  }

  // ===========================================================================================
  // Rendering: every message names its own body field and stays bounded.
  // ===========================================================================================

  @Test
  public void testControlMessagesRenderTheirOwnBodyFieldAndStayBounded() {
    assertTrue(ack(40L).toString().contains("consumerPosition=40"), ack(40L).toString());
    assertTrue(heartbeat().toString().contains("timestampMs=" + TIMESTAMP_MS),
        heartbeat().toString());
    assertTrue(retransmit(44L).toString().contains("lastSequenceNumber=44"),
        retransmit(44L).toString());
    assertTrue(termination(40L).toString().contains("totalBlocks=40"), termination(40L).toString());
    for (StreamingShuffleMessage message : oneOfEachType()) {
      String rendered = message.toString();
      assertTrue(rendered.startsWith(message.getClass().getSimpleName()), rendered);
      assertTrue(rendered.contains(String.valueOf(SHUFFLE_ID)), rendered);
      assertTrue(rendered.contains(String.valueOf(PARTITION_ID)), rendered);
      assertTrue(rendered.contains(String.valueOf(SEQUENCE_NUMBER)), rendered);
      // These strings reach diagnostics on every backpressure event, so none may be unbounded.
      assertTrue(rendered.length() < 200, "toString is too long: " + rendered);
    }
  }

  // ===========================================================================================
  // The ownership-transferring factory, which is the one route that does not copy.
  // ===========================================================================================

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

  // ===========================================================================================
  // The shape of the protocol surface itself: internal, final, and not instantiable by mistake.
  // ===========================================================================================

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
  public void testOnlyTheFramingByteDistinguishesTheFourFixedSizeMessagesOnTheWire() {
    // The four control messages encode to byte-identical bodies when their single body field
    // happens to match, so the framing byte is the only thing that tells the decoder which one it
    // is holding. If routing ever fell back on anything else it would silently mis-decode here.
    // The one value legal in all four body domains at once: at or above the sequence number for a
    // retransmission window's upper bound, equal to it for a termination's block count, and
    // non-negative for an acknowledgement position and a timestamp.
    long sharedBodyValue = SEQUENCE_NUMBER;
    StreamingShuffleMessage[] messages = {
        ack(sharedBodyValue),
        new HeartbeatMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, sharedBodyValue),
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER,
            sharedBodyValue),
        termination(sharedBodyValue),
    };
    byte[] reference = encodedBody(messages[0]);
    for (StreamingShuffleMessage message : messages) {
      assertEquals(33, message.encodedLength());
      assertArrayEquals(reference, encodedBody(message), message.toString());
      byte[] framed = toByteArray(message.toByteBuffer());
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
        new AckMessage(UNSUPPORTED_VERSION, SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, 40L);
    assertEquals(UNSUPPORTED_VERSION, foreign.protocolVersion());
    assertEquals(40L, foreign.consumerPosition());
    // The version participates in identity, so a foreign-version message is not the current one.
    assertNotEquals(ack(40L), foreign);

    ByteBuffer framed = foreign.toByteBuffer();
    assertEquals(UNSUPPORTED_VERSION, StreamingShuffleMessage.peekProtocolVersion(framed));
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.Decoder.fromByteBuffer(framed));
    // And the concrete decoder refuses it too, so the check does not live only in the dispatcher.
    ByteBuf body = Unpooled.buffer();
    body.writeByte(UNSUPPORTED_VERSION);
    body.writeInt(SHUFFLE_ID);
    body.writeInt(PARTITION_ID);
    body.writeLong(SEQUENCE_NUMBER);
    body.writeLong(40L);
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

  // ===========================================================================================
  // Helpers.
  // ===========================================================================================

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

  /** The four messages whose encoded form is exactly 33 bytes. */
  private StreamingShuffleMessage[] fixedSizeMessages() {
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
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER);
  }

  private AckMessage ack(long consumerPosition) {
    return new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, consumerPosition);
  }

  private HeartbeatMessage heartbeat() {
    return new HeartbeatMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, TIMESTAMP_MS);
  }

  private RetransmitRequestMessage retransmit(long lastSequenceNumber) {
    return new RetransmitRequestMessage(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, lastSequenceNumber);
  }

  private StreamTerminationMessage termination(long totalBlocks) {
    // Only data blocks consume sequence numbers, so a terminator's own position is exactly the
    // number of blocks that preceded it: the two values are one value here.
    return new StreamTerminationMessage(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, totalBlocks, totalBlocks);
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

  private static byte[] toByteArray(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  /** The encoded body of a message, that is, its framed form without the leading type byte. */
  private static byte[] encodedBody(StreamingShuffleMessage message) {
    ByteBuf buf = Unpooled.buffer(message.encodedLength());
    message.encode(buf);
    byte[] body = new byte[buf.readableBytes()];
    buf.readBytes(body);
    return body;
  }

  private static void writeHeader(ByteBuf buf, int shuffleId, int partitionId, long sequence) {
    writeHeader(buf, shuffleId, MAP_ID, partitionId, sequence);
  }

  /**
   * Writes a header by hand, in exactly the field order {@code encodeHeader} uses, so that a frame
   * carrying a value the constructors would refuse can still be presented to a decoder. The map id
   * is written between the shuffle id and the partition id, matching the wire layout.
   */
  private static void writeHeader(
      ByteBuf buf, int shuffleId, long mapId, int partitionId, long sequence) {
    buf.writeByte(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION);
    buf.writeInt(shuffleId);
    buf.writeLong(mapId);
    buf.writeInt(partitionId);
    buf.writeLong(sequence);
  }

  /**
   * A hand-crafted data block body carrying a chosen payload length prefix followed by a chosen
   * number of actual payload bytes, so that a hostile prefix can be presented without the frame
   * ever growing to the size that prefix claims.
   */
  private static ByteBuf dataBlockBodyWithPayloadPrefix(int declaredLength, int actualBytes) {
    ByteBuf buf = Unpooled.buffer();
    writeHeader(buf, SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER);
    buf.writeLong(0L);
    buf.writeInt(declaredLength);
    buf.writeBytes(new byte[actualBytes]);
    return buf;
  }

  private static void decodeAckWithHeader(int shuffleId, int partitionId, long sequence) {
    decodeAckWithHeader(shuffleId, MAP_ID, partitionId, sequence);
  }

  /** As {@link #decodeAckWithHeader(int, int, long)}, but with the map id chosen as well. */
  private static void decodeAckWithHeader(
      int shuffleId, long mapId, int partitionId, long sequence) {
    ByteBuf buf = Unpooled.buffer();
    writeHeader(buf, shuffleId, mapId, partitionId, sequence);
    buf.writeLong(0L);
    AckMessage.decode(buf);
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
