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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32C;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies that all streaming shuffle protocol messages can be serialized correctly. */
public class StreamingShuffleMessageSuite {

  /**
   * Header values shared by every message this suite builds. They are deliberately distinct from
   * one another and non-zero, so that an encoder which transposed two header fields, or a decoder
   * which read them back in the wrong order, cannot slip through by coincidence.
   */
  private static final int SHUFFLE_ID = 7;
  private static final int PARTITION_ID = 3;
  private static final long SEQUENCE_NUMBER = 42L;

  /**
   * A fixed instant in milliseconds. A heartbeat takes its timestamp from the caller precisely so
   * that this suite never reads a clock, which is what makes every assertion here reproducible.
   */
  private static final long TIMESTAMP_MS = 1_700_000_000_000L;

  /**
   * Encoded length of every streaming message whose body is a single long: the 17-byte shared
   * header plus 8 bytes of body. Framing adds one further byte for the type discriminator.
   */
  private static final int FIXED_BODY_ENCODED_LENGTH = 25;

  /** A protocol revision this build does not speak, used to exercise the compatibility check. */
  private static final byte INCOMPATIBLE_PROTOCOL_VERSION = 99;

  // -------------------------------------------------------------------------------------------
  // Group 1: encode and decode round trips, with each encoded length asserted as an exact
  // literal. A data block encodes to 29 + payload.length bytes; the other four to 25 bytes.
  // -------------------------------------------------------------------------------------------

  @Test
  public void testDataBlockMessageEncodeDecode() {
    // 29 is the fixed cost: 17 bytes of header, 8 of checksum and a 4-byte payload length prefix.
    assertDataBlockRoundTrip(0, 29);
    assertDataBlockRoundTrip(1, 30);
    assertDataBlockRoundTrip(1024, 1053);
    assertDataBlockRoundTrip(8192, 8221);
  }

  @Test
  public void testAckMessageEncodeDecode() {
    AckMessage ack = new AckMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, 41L);
    int len = ack.encodedLength();
    assertEquals(25, len);
    assertEquals(FIXED_BODY_ENCODED_LENGTH, len);
    ByteBuf buf = Unpooled.buffer(len);
    ack.encode(buf);
    assertEquals(0, buf.writableBytes());
    assertEquals(len, buf.readableBytes());

    AckMessage decoded = AckMessage.decode(buf);
    assertEquals(ack, decoded);
    assertEquals(41L, decoded.consumerPosition());
    assertEquals(0, buf.readableBytes());
  }

  @Test
  public void testHeartbeatMessageEncodeDecode() {
    HeartbeatMessage heartbeat =
        new HeartbeatMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, TIMESTAMP_MS);
    int len = heartbeat.encodedLength();
    assertEquals(25, len);
    ByteBuf buf = Unpooled.buffer(len);
    heartbeat.encode(buf);
    assertEquals(0, buf.writableBytes());
    assertEquals(len, buf.readableBytes());

    HeartbeatMessage decoded = HeartbeatMessage.decode(buf);
    assertEquals(heartbeat, decoded);
    assertEquals(TIMESTAMP_MS, decoded.timestampMs());
    assertEquals(0, buf.readableBytes());
  }

  @Test
  public void testRetransmitRequestMessageEncodeDecode() {
    // Equal bounds: the narrowest legal window, asking for a single block. This is the edge the
    // production ordering check has to accept, as against the inverted window it may reject.
    RetransmitRequestMessage single = retransmit(SEQUENCE_NUMBER);
    int len = single.encodedLength();
    assertEquals(25, len);
    ByteBuf buf = Unpooled.buffer(len);
    single.encode(buf);
    assertEquals(0, buf.writableBytes());
    assertEquals(len, buf.readableBytes());

    RetransmitRequestMessage decoded = RetransmitRequestMessage.decode(buf);
    assertEquals(single, decoded);
    assertEquals(SEQUENCE_NUMBER, decoded.firstSequenceNumber());
    assertEquals(SEQUENCE_NUMBER, decoded.lastSequenceNumber());
    assertEquals(1L, decoded.blockCount());
    assertTrue(decoded.contains(SEQUENCE_NUMBER));
    assertFalse(decoded.contains(SEQUENCE_NUMBER - 1L));
    assertFalse(decoded.contains(SEQUENCE_NUMBER + 1L));
    assertEquals(0, buf.readableBytes());
  }

  @Test
  public void testRetransmitRequestWindowSpanningSeveralBlocks() {
    RetransmitRequestMessage window = retransmit(45L);
    assertEquals(25, window.encodedLength());
    // The header's own sequence number doubles as the window's inclusive lower bound.
    assertEquals(SEQUENCE_NUMBER, window.firstSequenceNumber());
    assertEquals(SEQUENCE_NUMBER, window.sequenceNumber());
    assertEquals(45L, window.lastSequenceNumber());
    assertEquals(4L, window.blockCount());
    assertFalse(window.contains(41L));
    assertTrue(window.contains(42L));
    assertTrue(window.contains(44L));
    assertTrue(window.contains(45L));
    assertFalse(window.contains(46L));

    ByteBuf buf = Unpooled.buffer(window.encodedLength());
    window.encode(buf);
    assertEquals(0, buf.writableBytes());
    assertEquals(window, RetransmitRequestMessage.decode(buf));
  }

  @Test
  public void testStreamTerminationMessageEncodeDecode() {
    StreamTerminationMessage termination = termination(17L);
    int len = termination.encodedLength();
    assertEquals(25, len);
    ByteBuf buf = Unpooled.buffer(len);
    termination.encode(buf);
    assertEquals(0, buf.writableBytes());
    assertEquals(len, buf.readableBytes());

    StreamTerminationMessage decoded = StreamTerminationMessage.decode(buf);
    assertEquals(termination, decoded);
    assertEquals(17L, decoded.totalBlocks());
    assertEquals(0, buf.readableBytes());
  }

  @Test
  public void testStreamTerminationWithZeroBlocksIsAccepted() {
    // A partition that produced no records at all still terminates its stream, so a count of zero
    // is a legitimate value and must not be confused with the negative count that may be rejected.
    StreamTerminationMessage empty = termination(0L);
    assertEquals(0L, empty.totalBlocks());
    assertEquals(25, empty.encodedLength());
    assertEquals(empty, checkSerializeDeserialize(empty));
  }

  // -------------------------------------------------------------------------------------------
  // Group 2: header integrity. The header is 17 bytes laid out as protocolVersion, shuffleId,
  // partitionId and sequenceNumber, and the version sits at a fixed offset so that compatibility
  // can be settled before anything is decoded.
  // -------------------------------------------------------------------------------------------

  @Test
  public void testHeaderEncodedLengthConstant() {
    assertEquals(17, StreamingShuffleMessage.HEADER_ENCODED_LENGTH);
    // One byte for the version, four for each of the two ids, eight for the sequence number.
    assertEquals(1 + 4 + 4 + 8, StreamingShuffleMessage.HEADER_ENCODED_LENGTH);
    // Every fixed-size message is the header plus one long.
    assertEquals(StreamingShuffleMessage.HEADER_ENCODED_LENGTH + 8, FIXED_BODY_ENCODED_LENGTH);
  }

  @Test
  public void testCurrentProtocolVersionConstant() {
    assertEquals((byte) 1, StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION);
  }

  @Test
  public void testEveryHeaderFieldSurvivesRoundTrip() {
    for (StreamingShuffleMessage msg : oneOfEachType()) {
      assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, msg.protocolVersion());
      StreamingShuffleMessage decoded = decodeFramed(msg);
      assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, decoded.protocolVersion());
      assertEquals(SHUFFLE_ID, decoded.shuffleId());
      assertEquals(PARTITION_ID, decoded.partitionId());
      assertEquals(SEQUENCE_NUMBER, decoded.sequenceNumber());
    }
  }

  @Test
  public void testEveryBodyFieldSurvivesRoundTrip() {
    DataBlockMessage block = dataBlock(1024);
    DataBlockMessage decodedBlock = (DataBlockMessage) decodeFramed(block);
    assertEquals(block.checksum(), decodedBlock.checksum());
    assertArrayEquals(block.payload(), decodedBlock.payload());
    assertEquals(1024, decodedBlock.payloadLength());

    AckMessage decodedAck = (AckMessage) decodeFramed(ack(41L));
    assertEquals(41L, decodedAck.consumerPosition());

    HeartbeatMessage decodedHeartbeat = (HeartbeatMessage) decodeFramed(heartbeat());
    assertEquals(TIMESTAMP_MS, decodedHeartbeat.timestampMs());

    RetransmitRequestMessage decodedRetransmit =
        (RetransmitRequestMessage) decodeFramed(retransmit(45L));
    assertEquals(45L, decodedRetransmit.lastSequenceNumber());

    StreamTerminationMessage decodedTermination =
        (StreamTerminationMessage) decodeFramed(termination(17L));
    assertEquals(17L, decodedTermination.totalBlocks());
  }

  @Test
  public void testPeekProtocolVersionDoesNotAdvanceBuffer() {
    for (StreamingShuffleMessage msg : oneOfEachType()) {
      ByteBuffer framed = msg.toByteBuffer();
      int position = framed.position();
      int remaining = framed.remaining();

      byte peeked = StreamingShuffleMessage.peekProtocolVersion(framed);
      assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, peeked);
      assertEquals(position, framed.position());
      assertEquals(remaining, framed.remaining());
      // A second peek can only agree with the first if the first consumed nothing.
      assertEquals(peeked, StreamingShuffleMessage.peekProtocolVersion(framed));
      assertEquals(position, framed.position());

      // The decisive assertion: the very same buffer is still fully decodable afterwards.
      assertEquals(msg, StreamingShuffleMessage.Decoder.fromByteBuffer(framed));
      assertEquals(position, framed.position());
      assertEquals(remaining, framed.remaining());
    }
  }

  @Test
  public void testPeekProtocolVersionRejectsTooShortBuffer() {
    // A frame must carry at least a type byte and a version byte before it can be inspected.
    assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleMessage.peekProtocolVersion(ByteBuffer.allocate(0)));
    assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleMessage.peekProtocolVersion(ByteBuffer.allocate(1)));
  }

  @Test
  public void testProtocolVersionCompatibility() {
    byte current = StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION;
    assertTrue(StreamingShuffleMessage.isCompatible(current));
    assertDoesNotThrow(() -> StreamingShuffleMessage.checkProtocolVersion(current));

    assertFalse(StreamingShuffleMessage.isCompatible(INCOMPATIBLE_PROTOCOL_VERSION));
    assertFalse(StreamingShuffleMessage.isCompatible((byte) 0));
    assertFalse(StreamingShuffleMessage.isCompatible((byte) -1));
    assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleMessage.checkProtocolVersion(INCOMPATIBLE_PROTOCOL_VERSION));
    assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleMessage.checkProtocolVersion((byte) 0));
  }

  @Test
  public void testDecodedMessageKeepsTheProtocolVersionItArrivedWith() {
    // The version-carrying constructor is what lets a decoded message report the peer's revision
    // instead of silently adopting this build's own, which is what makes a mismatch nameable.
    AckMessage foreign = new AckMessage(
      INCOMPATIBLE_PROTOCOL_VERSION, SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, 41L);
    assertEquals(INCOMPATIBLE_PROTOCOL_VERSION, foreign.protocolVersion());
    ByteBuf buf = Unpooled.buffer(foreign.encodedLength());
    foreign.encode(buf);
    assertEquals(0, buf.writableBytes());

    AckMessage decoded = AckMessage.decode(buf);
    assertEquals(INCOMPATIBLE_PROTOCOL_VERSION, decoded.protocolVersion());
    assertEquals(foreign, decoded);
    // The version takes part in equality, so the same fields at this build's revision differ.
    // Only inequality is asserted, never hash-code inequality: distinct values are permitted to
    // collide, so asserting they do not would demand more than the hashCode contract promises.
    assertNotEquals(ack(41L), decoded);
    assertNotEquals(decoded, ack(41L));
  }

  @Test
  public void testDecoderRejectsIncompatibleProtocolVersion() {
    AckMessage foreign = new AckMessage(
      INCOMPATIBLE_PROTOCOL_VERSION, SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, 41L);
    ByteBuffer framed = foreign.toByteBuffer();
    assertEquals(INCOMPATIBLE_PROTOCOL_VERSION,
      StreamingShuffleMessage.peekProtocolVersion(framed));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleMessage.Decoder.fromByteBuffer(framed));
    assertTrue(e.getMessage().contains("protocol version"), e.getMessage());
  }

  // -------------------------------------------------------------------------------------------
  // Group 3: the framed form produced by toByteBuffer and consumed by Decoder.fromByteBuffer.
  // A frame is the one-byte type discriminator followed by the encoded body, so it always
  // occupies encodedLength() + 1 bytes.
  // -------------------------------------------------------------------------------------------

  @Test
  public void testFramedRoundTripForEveryMessageType() {
    for (StreamingShuffleMessage msg : oneOfEachType()) {
      assertEquals(msg, checkSerializeDeserialize(msg));
    }
    // Also at the payload boundaries a data block has to cope with.
    checkSerializeDeserialize(dataBlock(0));
    checkSerializeDeserialize(dataBlock(1));
    checkSerializeDeserialize(dataBlock(8192));
  }

  @Test
  public void testFramedLengthIsEncodedLengthPlusTypeByte() {
    for (StreamingShuffleMessage msg : oneOfEachType()) {
      assertEquals(msg.encodedLength() + 1, msg.toByteBuffer().remaining());
    }
    // And at the documented literals: 30 + payload.length for a data block, 26 for the rest.
    assertEquals(30, dataBlock(0).toByteBuffer().remaining());
    assertEquals(1054, dataBlock(1024).toByteBuffer().remaining());
    assertEquals(26, ack(41L).toByteBuffer().remaining());
    assertEquals(26, heartbeat().toByteBuffer().remaining());
    assertEquals(26, retransmit(45L).toByteBuffer().remaining());
    assertEquals(26, termination(17L).toByteBuffer().remaining());
  }

  @Test
  public void testFramingTypeByteMatchesMessageTypeId() {
    assertFramingTypeByte(dataBlock(64), StreamingShuffleMessageType.DATA_BLOCK, (byte) 0);
    assertFramingTypeByte(ack(41L), StreamingShuffleMessageType.ACK, (byte) 1);
    assertFramingTypeByte(heartbeat(), StreamingShuffleMessageType.HEARTBEAT, (byte) 2);
    assertFramingTypeByte(retransmit(45L),
      StreamingShuffleMessageType.RETRANSMIT_REQUEST, (byte) 3);
    assertFramingTypeByte(termination(17L),
      StreamingShuffleMessageType.STREAM_TERMINATION, (byte) 4);
  }

  @Test
  public void testDecoderResolvesTheCorrectConcreteClass() {
    assertInstanceOf(DataBlockMessage.class, decodeFramed(dataBlock(64)));
    assertInstanceOf(AckMessage.class, decodeFramed(ack(41L)));
    assertInstanceOf(HeartbeatMessage.class, decodeFramed(heartbeat()));
    assertInstanceOf(RetransmitRequestMessage.class, decodeFramed(retransmit(45L)));
    assertInstanceOf(StreamTerminationMessage.class, decodeFramed(termination(17L)));
  }

  // -------------------------------------------------------------------------------------------
  // Group 4: the two mebibyte block size cap. It is enforced on construction and again on decode,
  // and the decode-side check runs before any array is allocated for the payload, because the
  // length prefix arrives from a remote peer and the array is sized from whatever it claims.
  // -------------------------------------------------------------------------------------------

  @Test
  public void testMaxBlockSizeConstant() {
    assertEquals(2097152, DataBlockMessage.MAX_BLOCK_SIZE_BYTES);
    assertEquals(2 * 1024 * 1024, DataBlockMessage.MAX_BLOCK_SIZE_BYTES);
  }

  @Test
  public void testPayloadExactlyAtTheCapIsAccepted() {
    // The production comparison is strictly greater than the cap, so a full block is legal.
    byte[] payload = new byte[DataBlockMessage.MAX_BLOCK_SIZE_BYTES];
    DataBlockMessage block =
        new DataBlockMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, 0L, payload);
    assertEquals(DataBlockMessage.MAX_BLOCK_SIZE_BYTES, block.payloadLength());
    int len = block.encodedLength();
    assertEquals(2097181, len);
    assertEquals(29 + DataBlockMessage.MAX_BLOCK_SIZE_BYTES, len);

    ByteBuf buf = Unpooled.buffer(len);
    block.encode(buf);
    assertEquals(0, buf.writableBytes());
    assertEquals(len, buf.readableBytes());

    DataBlockMessage decoded = DataBlockMessage.decode(buf);
    assertEquals(block, decoded);
    assertEquals(DataBlockMessage.MAX_BLOCK_SIZE_BYTES, decoded.payloadLength());
    assertEquals(0, buf.readableBytes());
  }

  @Test
  public void testPayloadOneByteAboveTheCapIsRejectedOnConstruction() {
    byte[] tooLarge = new byte[DataBlockMessage.MAX_BLOCK_SIZE_BYTES + 1];
    // Every route into an instance has to reject it, or a producer could build one after all.
    assertThrows(IllegalArgumentException.class, () ->
      new DataBlockMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, 0L, tooLarge));
    assertThrows(IllegalArgumentException.class, () -> new DataBlockMessage(
      StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, SHUFFLE_ID, PARTITION_ID,
      SEQUENCE_NUMBER, 0L, tooLarge));
    assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.withComputedChecksum(
      SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, tooLarge));
  }

  @Test
  public void testDecodeRejectsOversizePayloadLengthPrefix() {
    // The point of these two is not merely that they fail, but that they fail as an
    // IllegalArgumentException rather than as an OutOfMemoryError from the enormous allocation a
    // trusted length prefix would have asked for. The crafted buffers stay 29 bytes long.
    assertRejectedPayloadLengthPrefix(DataBlockMessage.MAX_BLOCK_SIZE_BYTES + 1);
    assertRejectedPayloadLengthPrefix(Integer.MAX_VALUE);
  }

  @Test
  public void testDecodeRejectsNegativePayloadLengthPrefix() {
    // Reported as an IllegalArgumentException, not surfaced as a NegativeArraySizeException.
    assertRejectedPayloadLengthPrefix(-1);
    assertRejectedPayloadLengthPrefix(Integer.MIN_VALUE);
  }

  @Test
  public void testDecodeRejectsTruncatedDataBlockFrames() {
    // A header shorter than the 17 bytes it must occupy.
    ByteBuf shortHeader = Unpooled.buffer(4);
    shortHeader.writeInt(SHUFFLE_ID);
    assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.decode(shortHeader));

    // A complete header, but no checksum behind it.
    ByteBuf noChecksum = headerOnlyBuffer();
    assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.decode(noChecksum));

    // A header and a checksum, but no payload length prefix.
    ByteBuf noPrefix = Unpooled.buffer(StreamingShuffleMessage.HEADER_ENCODED_LENGTH + 8);
    writeHeader(noPrefix);
    noPrefix.writeLong(0L);
    assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.decode(noPrefix));

    // A prefix that is within the cap but claims more bytes than the buffer actually holds.
    ByteBuf unbacked = craftedDataBlockPrefix(64);
    assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.decode(unbacked));
  }

  @Test
  public void testDecodeRejectsTruncatedFixedSizeBodies() {
    // Each of the four reads a header and then eight bytes of body; a header alone is not enough.
    assertThrows(IllegalArgumentException.class, () -> AckMessage.decode(headerOnlyBuffer()));
    assertThrows(IllegalArgumentException.class,
      () -> HeartbeatMessage.decode(headerOnlyBuffer()));
    assertThrows(IllegalArgumentException.class,
      () -> RetransmitRequestMessage.decode(headerOnlyBuffer()));
    assertThrows(IllegalArgumentException.class,
      () -> StreamTerminationMessage.decode(headerOnlyBuffer()));
  }

  // -------------------------------------------------------------------------------------------
  // Group 5: checksum behaviour. The algorithm is fixed to CRC32C and is cross-checked against a
  // directly constructed java.util.zip.CRC32C, so the helper cannot drift from the JDK primitive.
  // -------------------------------------------------------------------------------------------

  @Test
  public void testChecksumAlgorithmIsCrc32c() {
    assertEquals("CRC32C", StreamingShuffleChecksum.ALGORITHM);
  }

  @Test
  public void testChecksumMatchesJdkCrc32c() {
    assertCrc32cAgreement(new byte[0]);
    assertCrc32cAgreement("streaming shuffle".getBytes(StandardCharsets.UTF_8));
    assertCrc32cAgreement(payloadOf(1));
    assertCrc32cAgreement(payloadOf(8192));
  }

  @Test
  public void testChecksumRangeOverloadAgreesWithWholeArrayForm() {
    byte[] data = payloadOf(8192);
    assertRangeAgreement(data, 0, 0);
    assertRangeAgreement(data, 0, 1);
    assertRangeAgreement(data, 1, 100);
    assertRangeAgreement(data, 4096, 4096);
    assertRangeAgreement(data, 0, data.length);
    assertRangeAgreement(data, data.length, 0);
  }

  @Test
  public void testChecksumRejectsInvalidRanges() {
    byte[] data = payloadOf(64);
    assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleChecksum.compute(data, -1, 4));
    assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleChecksum.compute(data, 0, -1));
    assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleChecksum.compute(data, 1, data.length));
    assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleChecksum.compute(data, data.length + 1, 0));
  }

  @Test
  public void testChecksumIsDeterministicAcrossCalls() {
    // A shared, stateful Checksum instance would make the second call disagree with the first.
    byte[] data = payloadOf(4096);
    long first = StreamingShuffleChecksum.compute(data);
    assertEquals(first, StreamingShuffleChecksum.compute(data));
    assertEquals(first, StreamingShuffleChecksum.compute(data));
    assertEquals(first, StreamingShuffleChecksum.compute(data, 0, data.length));
  }

  @Test
  public void testChecksumVerifyDetectsCorruption() {
    byte[] data = "streaming shuffle".getBytes(StandardCharsets.UTF_8);
    long checksum = StreamingShuffleChecksum.compute(data);
    assertTrue(StreamingShuffleChecksum.verify(data, checksum));
    assertFalse(StreamingShuffleChecksum.verify(data, checksum + 1));
    assertFalse(StreamingShuffleChecksum.verify(data, checksum - 1));

    // A single flipped bit anywhere in the block has to be caught, at every position.
    for (int i = 0; i < data.length; i++) {
      byte[] flipped = data.clone();
      flipped[i] ^= 0x01;
      assertFalse(StreamingShuffleChecksum.verify(flipped, checksum));
      assertTrue(StreamingShuffleChecksum.verify(
        flipped, StreamingShuffleChecksum.compute(flipped)));
    }
  }

  @Test
  public void testChecksumSurvivesDataBlockRoundTrip() {
    byte[] payload = payloadOf(4096);
    DataBlockMessage block = DataBlockMessage.withComputedChecksum(
      SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, payload);
    assertEquals(StreamingShuffleChecksum.compute(payload), block.checksum());
    assertTrue(block.verifyChecksum());

    DataBlockMessage decoded = (DataBlockMessage) decodeFramed(block);
    assertEquals(block.checksum(), decoded.checksum());
    assertArrayEquals(payload, decoded.payload());
    assertTrue(decoded.verifyChecksum());
    assertTrue(StreamingShuffleChecksum.verify(decoded.payload(), decoded.checksum()));

    // A block altered in flight no longer matches the checksum it arrived with, which is exactly
    // the condition that has a consumer ask for the block to be replayed.
    byte[] corrupted = decoded.payload().clone();
    corrupted[2048] ^= 0x01;
    DataBlockMessage tampered = new DataBlockMessage(
      SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, decoded.checksum(), corrupted);
    assertFalse(tampered.verifyChecksum());
    assertNotEquals(decoded, tampered);
  }

  @Test
  public void testEmptyPayloadIsChecksummedOnTheSamePath() {
    byte[] empty = new byte[0];
    DataBlockMessage block = DataBlockMessage.withComputedChecksum(
      SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, empty);
    assertEquals(0, block.payloadLength());
    assertEquals(StreamingShuffleChecksum.compute(empty), block.checksum());
    assertTrue(block.verifyChecksum());
    assertEquals(29, block.encodedLength());
    assertEquals(block, checkSerializeDeserialize(block));
  }

  /**
   * Cross-checks both {@code compute} overloads against a {@link CRC32C} constructed directly from
   * the JDK, which is the only way to show that the helper computes CRC32C and not something else
   * that merely happens to round-trip through itself consistently.
   */
  private void assertCrc32cAgreement(byte[] data) {
    CRC32C expected = new CRC32C();
    expected.update(data, 0, data.length);
    assertEquals(expected.getValue(), StreamingShuffleChecksum.compute(data));
    assertEquals(expected.getValue(), StreamingShuffleChecksum.compute(data, 0, data.length));
    assertTrue(StreamingShuffleChecksum.verify(data, expected.getValue()));
  }

  /**
   * Asserts that checksumming a slice in place agrees with checksumming a copy of that slice, so
   * that a caller may verify a payload sitting inside a larger receive buffer without copying it.
   */
  private void assertRangeAgreement(byte[] data, int offset, int length) {
    long viaCopy = StreamingShuffleChecksum.compute(
      Arrays.copyOfRange(data, offset, offset + length));
    assertEquals(viaCopy, StreamingShuffleChecksum.compute(data, offset, length));
    assertTrue(StreamingShuffleChecksum.verify(data, offset, length, viaCopy));
  }

  /**
   * Builds a data-block frame body carrying the given declared payload length and no payload bytes
   * at all, then asserts that decoding it is refused. The buffer stays 29 bytes long whatever the
   * prefix claims, which is the whole point of validating the prefix before allocating for it.
   */
  private void assertRejectedPayloadLengthPrefix(int declaredLength) {
    ByteBuf buf = craftedDataBlockPrefix(declaredLength);
    assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.decode(buf));
  }

  // -------------------------------------------------------------------------------------------
  // Group 6: unknown type ids. The streaming discriminator is a separate id space from
  // BlockTransferMessage.Type, so an id that is meaningful there must not resolve here.
  // -------------------------------------------------------------------------------------------

  @Test
  public void testMessageTypeIdsAreFrozen() {
    assertEquals((byte) 0, StreamingShuffleMessageType.DATA_BLOCK.id());
    assertEquals((byte) 1, StreamingShuffleMessageType.ACK.id());
    assertEquals((byte) 2, StreamingShuffleMessageType.HEARTBEAT.id());
    assertEquals((byte) 3, StreamingShuffleMessageType.RETRANSMIT_REQUEST.id());
    assertEquals((byte) 4, StreamingShuffleMessageType.STREAM_TERMINATION.id());
    assertEquals(5, StreamingShuffleMessageType.values().length);
  }

  @Test
  public void testMessageTypeFromIdRoundTripsEveryConstant() {
    for (StreamingShuffleMessageType type : StreamingShuffleMessageType.values()) {
      assertEquals(type, StreamingShuffleMessageType.fromId(type.id()));
    }
  }

  @Test
  public void testMessageTypeFromIdRejectsUnknownIds() {
    assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleMessageType.fromId((byte) 99));
    // 5 is HEARTBEAT in the unrelated BlockTransferMessage id space, and must not resolve here.
    assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleMessageType.fromId((byte) 5));
    assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleMessageType.fromId((byte) -1));
    assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleMessageType.fromId(Byte.MIN_VALUE));
    assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleMessageType.fromId(Byte.MAX_VALUE));
  }

  @Test
  public void testDecoderRejectsUnknownFramingTypeByte() {
    // The version byte is deliberately the compatible one, so that the failure observed here is
    // genuinely the unknown-type path and not the compatibility check that runs ahead of it.
    ByteBuffer framed =
        ByteBuffer.allocate(1 + StreamingShuffleMessage.HEADER_ENCODED_LENGTH + 8);
    framed.put((byte) 9);
    framed.put(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION);
    framed.putInt(SHUFFLE_ID);
    framed.putInt(PARTITION_ID);
    framed.putLong(SEQUENCE_NUMBER);
    framed.putLong(0L);
    framed.flip();
    assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION,
      StreamingShuffleMessage.peekProtocolVersion(framed));

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleMessage.Decoder.fromByteBuffer(framed));
    assertTrue(e.getMessage().contains("Unknown message type: "), e.getMessage());
  }

  @Test
  public void testDecoderRejectsFrameTooShortToRoute() {
    // Fewer than two bytes cannot carry both a type byte and a version byte, so such a frame is
    // refused outright rather than being handed to a decoder that would read past its end.
    ByteBuffer noBytes = ByteBuffer.allocate(0);
    assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleMessage.Decoder.fromByteBuffer(noBytes));
    ByteBuffer typeByteOnly = ByteBuffer.allocate(1);
    assertThrows(IllegalArgumentException.class,
      () -> StreamingShuffleMessage.Decoder.fromByteBuffer(typeByteOnly));
  }

  // -------------------------------------------------------------------------------------------
  // Group 7: the equals and hashCode contract. A data block's payload has to compare by value,
  // and the four fixed-size messages have to stay distinguishable from one another even though
  // they encode to byte-identical bodies for identical field values.
  // -------------------------------------------------------------------------------------------

  @Test
  public void testDataBlockEqualityIsValueBasedOverThePayload() {
    // Distinct arrays holding identical bytes. Equality has to be by value, or a decoded block
    // would never compare equal to the block it was decoded from.
    byte[] first = payloadOf(256);
    byte[] second = payloadOf(256);
    assertNotSame(first, second);
    assertArrayEquals(first, second);

    DataBlockMessage a =
        new DataBlockMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, 123L, first);
    DataBlockMessage b =
        new DataBlockMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, 123L, second);
    assertEquals(a, b);
    assertEquals(b, a);
    assertEquals(a.hashCode(), b.hashCode());
    assertEquals(a.toString(), b.toString());

    byte[] differing = payloadOf(256);
    differing[128] ^= 0x01;
    DataBlockMessage c =
        new DataBlockMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, 123L, differing);
    assertNotEquals(a, c);
    assertNotEquals(c, a);
  }

  @Test
  public void testEqualsIsReflexiveAndSymmetricForEveryType() {
    for (StreamingShuffleMessage msg : oneOfEachType()) {
      assertEquals(msg, msg);
      assertEquals(msg.hashCode(), msg.hashCode());

      StreamingShuffleMessage copy = decodeFramed(msg);
      assertNotSame(msg, copy);
      assertEquals(msg, copy);
      assertEquals(copy, msg);
      assertEquals(msg.hashCode(), copy.hashCode());
      assertEquals(msg.toString(), copy.toString());
    }
  }

  @Test
  public void testInequalityWhenAnySingleFieldDiffers() {
    AckMessage base = ack(41L);
    assertNotEquals(base, new AckMessage(SHUFFLE_ID + 1, PARTITION_ID, SEQUENCE_NUMBER, 41L));
    assertNotEquals(base, new AckMessage(SHUFFLE_ID, PARTITION_ID + 1, SEQUENCE_NUMBER, 41L));
    assertNotEquals(base, new AckMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER + 1L, 41L));
    assertNotEquals(base, new AckMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, 42L));

    assertNotEquals(heartbeat(),
      new HeartbeatMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, TIMESTAMP_MS + 1L));
    assertNotEquals(retransmit(45L), retransmit(46L));
    assertNotEquals(termination(17L), termination(18L));

    DataBlockMessage block = dataBlock(64);
    assertNotEquals(block, new DataBlockMessage(
      SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, block.checksum() + 1L, block.payload()));
    assertNotEquals(block, new DataBlockMessage(
      SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, block.checksum(), payloadOf(65)));
    assertNotEquals(block, dataBlock(65));
  }

  @Test
  public void testCrossTypeInequalityAmongFixedSizeMessages() {
    // All four encode to exactly the same 25 bytes when their field values match, so an encoded
    // length can never tell them apart. Only the concrete type and the framing byte can.
    // The body value is chosen to be legal for every one of them: as a retransmission upper bound
    // it equals the lower bound, giving the narrowest legal window, and as a block count it is
    // non-negative.
    long body = SEQUENCE_NUMBER;
    StreamingShuffleMessage[] messages = {
      new AckMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, body),
      new HeartbeatMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, body),
      new RetransmitRequestMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, body),
      new StreamTerminationMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, body),
    };

    byte[] reference = encodedBody(messages[0]);
    for (StreamingShuffleMessage msg : messages) {
      assertEquals(FIXED_BODY_ENCODED_LENGTH, msg.encodedLength());
      // The bodies really are byte-identical, which is what makes the checks below necessary.
      assertArrayEquals(reference, encodedBody(msg));
    }

    for (int i = 0; i < messages.length; i++) {
      for (int j = 0; j < messages.length; j++) {
        if (i == j) {
          assertEquals(messages[i], messages[j]);
        } else {
          assertNotEquals(messages[i], messages[j]);
          assertNotEquals(messages[j], messages[i]);
        }
      }
      // Each nevertheless round-trips to its own concrete class, because the framing byte differs.
      StreamingShuffleMessage decoded = decodeFramed(messages[i]);
      assertEquals(messages[i].getClass(), decoded.getClass());
      assertEquals(messages[i], decoded);
    }
  }

  @Test
  public void testEqualsRejectsNullAndUnrelatedTypes() {
    for (StreamingShuffleMessage msg : oneOfEachType()) {
      assertNotEquals(msg, null);
      assertNotEquals(msg, "not a streaming shuffle message");
      assertNotEquals(msg, Long.valueOf(SEQUENCE_NUMBER));
    }
  }

  @Test
  public void testDataBlockToStringOmitsPayloadBytes() {
    DataBlockMessage block = dataBlock(1024);
    String rendered = block.toString();
    assertTrue(rendered.startsWith("DataBlockMessage["), rendered);
    assertTrue(rendered.contains("payloadLength=1024"), rendered);
    assertTrue(rendered.contains("protocolVersion=1"), rendered);
    assertTrue(rendered.contains("shuffleId=" + SHUFFLE_ID), rendered);
    assertTrue(rendered.contains("partitionId=" + PARTITION_ID), rendered);
    assertTrue(rendered.contains("sequenceNumber=" + SEQUENCE_NUMBER), rendered);
    assertTrue(rendered.contains("checksum=" + block.checksum()), rendered);
    // A block runs to two mebibytes, so its rendering has to stay bounded whatever it carries.
    assertTrue(rendered.length() < 200, rendered);
    assertEquals(rendered, decodeFramed(block).toString());
  }

  @Test
  public void testFixedSizeMessagesRenderTheirBodyField() {
    assertTrue(ack(41L).toString().contains("consumerPosition=41"));
    assertTrue(heartbeat().toString().contains("timestampMs=" + TIMESTAMP_MS));
    assertTrue(retransmit(45L).toString().contains("lastSequenceNumber=45"));
    assertTrue(termination(17L).toString().contains("totalBlocks=17"));
  }

  /**
   * Reads the framing type byte of a message without consuming it, using an absolute get so that
   * the buffer's position is left exactly where the decoder expects to find it, and checks that the
   * byte agrees with the discriminator's own declared id.
   */
  private void assertFramingTypeByte(
      StreamingShuffleMessage msg,
      StreamingShuffleMessageType expectedType,
      byte expectedId) {
    ByteBuffer framed = msg.toByteBuffer();
    int position = framed.position();
    assertEquals(expectedId, framed.get(position));
    assertEquals(expectedType.id(), framed.get(position));
    assertEquals(position, framed.position());
    assertEquals(expectedType, StreamingShuffleMessageType.fromId(framed.get(position)));
  }

  /**
   * Round-trips a data block of the given payload length through {@code encode} and {@code decode},
   * asserting that its encoded length is exactly the expected literal, that the encode consumed the
   * whole of an exactly sized buffer, and that the decode consumed the whole of it back again.
   */
  private void assertDataBlockRoundTrip(int payloadLength, int expectedEncodedLength) {
    byte[] payload = payloadOf(payloadLength);
    DataBlockMessage block = DataBlockMessage.withComputedChecksum(
      SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, payload);
    int len = block.encodedLength();
    assertEquals(expectedEncodedLength, len);
    assertEquals(29 + payloadLength, len);
    assertEquals(payloadLength, block.payloadLength());
    ByteBuf buf = Unpooled.buffer(len);
    block.encode(buf);
    // Encodable requires encode to write exactly encodedLength() bytes; this is what proves it.
    assertEquals(0, buf.writableBytes());
    assertEquals(len, buf.readableBytes());

    DataBlockMessage decoded = DataBlockMessage.decode(buf);
    assertEquals(block, decoded);
    assertEquals(0, buf.readableBytes());
    assertArrayEquals(payload, decoded.payload());
    assertEquals(payloadLength, decoded.payloadLength());
    assertTrue(decoded.verifyChecksum());
  }

  /**
   * Serializes a message to its framed form, deserializes it, and asserts the whole value contract:
   * equality, a matching hash code, and a matching rendering. This mirrors the helper the sibling
   * block-transfer suite uses for exactly the same purpose.
   */
  private StreamingShuffleMessage checkSerializeDeserialize(StreamingShuffleMessage msg) {
    StreamingShuffleMessage msg2 =
        StreamingShuffleMessage.Decoder.fromByteBuffer(msg.toByteBuffer());
    assertEquals(msg, msg2);
    assertEquals(msg.hashCode(), msg2.hashCode());
    assertEquals(msg.toString(), msg2.toString());
    return msg2;
  }

  /**
   * Serializes a message to its framed form and deserializes it, asserting nothing, for the cases
   * that go on to inspect the concrete type or an individual field of the result.
   */
  private StreamingShuffleMessage decodeFramed(StreamingShuffleMessage msg) {
    return StreamingShuffleMessage.Decoder.fromByteBuffer(msg.toByteBuffer());
  }

  /**
   * Encodes a message into an exactly sized buffer and returns the bytes written, having first
   * confirmed that {@code encode} wrote neither fewer nor more than {@code encodedLength()}.
   */
  private byte[] encodedBody(StreamingShuffleMessage msg) {
    ByteBuf buf = Unpooled.buffer(msg.encodedLength());
    msg.encode(buf);
    assertEquals(0, buf.writableBytes());
    assertEquals(msg.encodedLength(), buf.readableBytes());
    byte[] bytes = new byte[buf.readableBytes()];
    buf.readBytes(bytes);
    return bytes;
  }

  /** One message of each of the five types, all carrying the same header values. */
  private StreamingShuffleMessage[] oneOfEachType() {
    return new StreamingShuffleMessage[] {
      dataBlock(1024), ack(41L), heartbeat(), retransmit(45L), termination(17L)
    };
  }

  private DataBlockMessage dataBlock(int payloadLength) {
    return DataBlockMessage.withComputedChecksum(
      SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, payloadOf(payloadLength));
  }

  private AckMessage ack(long consumerPosition) {
    return new AckMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, consumerPosition);
  }

  private HeartbeatMessage heartbeat() {
    return new HeartbeatMessage(SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, TIMESTAMP_MS);
  }

  /**
   * A retransmission request whose window runs from the shared header sequence number up to the
   * given upper bound. Callers choose only the upper bound, so every request this suite builds is a
   * legal, non-inverted window.
   */
  private RetransmitRequestMessage retransmit(long lastSequenceNumber) {
    return new RetransmitRequestMessage(
      SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, lastSequenceNumber);
  }

  /** A terminator reporting the given non-negative block count. */
  private StreamTerminationMessage termination(long totalBlocks) {
    return new StreamTerminationMessage(
      SHUFFLE_ID, PARTITION_ID, SEQUENCE_NUMBER, totalBlocks);
  }

  /**
   * A payload of the requested length, filled arithmetically rather than randomly so that every
   * checksum, hash code and encoded byte this suite asserts on is reproducible run after run.
   */
  private byte[] payloadOf(int length) {
    byte[] payload = new byte[length];
    for (int i = 0; i < length; i++) {
      payload[i] = (byte) (i * 31 + 7);
    }
    return payload;
  }

  /** Writes a valid 17-byte header, in the normative field order, into the given buffer. */
  private void writeHeader(ByteBuf buf) {
    buf.writeByte(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION);
    buf.writeInt(SHUFFLE_ID);
    buf.writeInt(PARTITION_ID);
    buf.writeLong(SEQUENCE_NUMBER);
  }

  /** A buffer holding a complete, valid header and nothing whatsoever behind it. */
  private ByteBuf headerOnlyBuffer() {
    ByteBuf buf = Unpooled.buffer(StreamingShuffleMessage.HEADER_ENCODED_LENGTH);
    writeHeader(buf);
    assertEquals(0, buf.writableBytes());
    return buf;
  }

  /**
   * A 29-byte data-block body: a valid header, an eight-byte checksum, and a four-byte payload
   * length prefix declaring the given length, with no payload bytes behind it. The buffer's size is
   * independent of what the prefix claims, which is what lets an absurd claim be tested cheaply.
   */
  private ByteBuf craftedDataBlockPrefix(int declaredLength) {
    ByteBuf buf = Unpooled.buffer(StreamingShuffleMessage.HEADER_ENCODED_LENGTH + 8 + 4);
    writeHeader(buf);
    buf.writeLong(0L);
    buf.writeInt(declaredLength);
    assertEquals(0, buf.writableBytes());
    assertEquals(29, buf.readableBytes());
    return buf;
  }
}
