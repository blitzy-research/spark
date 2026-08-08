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

/** Wire-contract tests for the streaming shuffle protocol message family. */
public class StreamingShuffleMessageSuite {

  private static final int SHUFFLE_ID = 7;
  private static final int PARTITION_ID = 3;
  private static final long MAP_ID = 11L;
  private static final long SEQUENCE_NUMBER = 42L;

  private static final long CONSUMER_TOKEN = 0x51A5_1D3E_7C0F_0042L;


  private static final byte UNSUPPORTED_VERSION = (byte) 99;

  private static final byte UNKNOWN_TYPE_ID = (byte) 9;

  /**
   * Every buffer this suite allocated, in allocation order, so that each is released exactly once.
   */
  private final List<ByteBuf> ownedBuffers = new ArrayList<>();

  /**
   * Releases every buffer the test allocated, and asserts that this suite owned each of them alone.
   */
  @AfterEach
  public void releaseOwnedBuffers() {
    List<String> violations = new ArrayList<>();
    try {
      for (int index = 0; index < ownedBuffers.size(); index++) {
        ByteBuf buf = ownedBuffers.get(index);
        int held = buf.refCnt();
        if (held != 1) {
          violations.add("buffer " + index + " held " + held + " reference(s) before release");
        }
        if (held > 0) {
          buf.release(held);
        }
        int remaining = buf.refCnt();
        if (remaining != 0) {
          violations.add("buffer " + index + " held " + remaining + " reference(s) after release");
        }
      }
    } finally {
      // Cleared whatever happened above, so a buffer already freed here can never be presented to
      // the next test's teardown as one of its own.
      ownedBuffers.clear();
    }
    assertTrue(violations.isEmpty(),
        "this suite owns every buffer it allocates alone, so each must hold exactly one reference "
            + "before teardown releases it and none afterwards: " + violations);
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

  @Test
  public void testDataBlockMessageEncodeDecode() {
    for (int payloadLength : new int[] {0, 1, 1024, 8192}) {
      byte[] payload = payload(payloadLength);
      DataBlockMessage message = DataBlockMessage.withComputedChecksum(
          SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, payload);
      int len = message.encodedLength();
      assertEquals(EMPTY_DATA_BLOCK_BYTES + payloadLength, len);
      ByteBuf buf = buffer(len);
      message.encode(buf);
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
    assertEquals(CONTROL_MESSAGE_BYTES, len);
    assertEquals(25, len);
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
    HeartbeatMessage message = heartbeatWithoutToken();
    assertEquals(HEARTBEAT_BYTES, message.encodedLength());
    assertEquals(HeartbeatMessage.NO_CONSUMER_TOKEN, message.consumerToken());
    assertEquals(0L, HeartbeatMessage.NO_CONSUMER_TOKEN);

    HeartbeatMessage decoded = (HeartbeatMessage) roundTrip(message);
    assertEquals(message, decoded);
    assertEquals(HeartbeatMessage.NO_CONSUMER_TOKEN, decoded.consumerToken());
    assertNotEquals(heartbeat(), message);
    assertNotEquals(heartbeat().hashCode(), message.hashCode());
  }

  @Test
  public void testHeartbeatConsumerTokenDomainIsEnforcedOnBothRoutes() {
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
    assertDoesNotThrow(() -> decodeHeartbeatWithToken(HeartbeatMessage.NO_CONSUMER_TOKEN));
    assertDoesNotThrow(() -> decodeHeartbeatWithToken(Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, new HeartbeatMessage(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, Long.MAX_VALUE).consumerToken());
  }

  @Test
  public void testEveryControlMessageIsFixedAtTheSpecifiedSize() {
    for (StreamingShuffleMessage message : fixedSizeMessages()) {
      assertEquals(CONTROL_MESSAGE_BYTES, message.encodedLength(),
          message.getClass().getSimpleName());
      assertEquals(CONTROL_MESSAGE_BYTES + FRAME_TYPE_PREFIX_BYTES,
          message.toByteBuffer().remaining(), message.getClass().getSimpleName());
    }
    assertEquals(33, heartbeat().encodedLength());
    assertEquals(33, heartbeatWithoutToken().encodedLength());
    assertEquals(34, heartbeat().toByteBuffer().remaining());
    assertEquals(34, heartbeatWithoutToken().toByteBuffer().remaining());
    RetransmitRequestMessage single =
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER);
    RetransmitRequestMessage run =
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER,
            SEQUENCE_NUMBER + 100L);
    assertEquals(33, single.encodedLength());
    assertEquals(33, run.encodedLength());
    assertEquals(34, single.toByteBuffer().remaining());
    assertEquals(34, run.toByteBuffer().remaining());
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
    assertEquals(40L, decoded.totalBlocks());
    assertEquals(40L, decoded.sequenceNumber());
    assertEquals(MAP_ID, decoded.mapId());
  }

  @Test
  public void testHeaderLayoutConstants() {
    assertEquals(HEADER_BYTES, StreamingShuffleMessage.HEADER_ENCODED_LENGTH);
    assertEquals(17, StreamingShuffleMessage.HEADER_ENCODED_LENGTH);
    assertEquals(1 + 4 + 4 + 8, StreamingShuffleMessage.HEADER_ENCODED_LENGTH);
    assertEquals(8, StreamingShuffleMessage.PRODUCER_ID_ENCODED_LENGTH);
    assertEquals(CONTROL_MESSAGE_BYTES, StreamingShuffleMessage.CONTROL_MESSAGE_ENCODED_LENGTH);
    assertEquals(25, StreamingShuffleMessage.CONTROL_MESSAGE_ENCODED_LENGTH);
    assertEquals(RETRANSMIT_REQUEST_BYTES,
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 4L, 6L).encodedLength());
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
      assertEquals(message.sequenceNumber(), decoded.sequenceNumber(),
          message.getClass().getSimpleName());
      assertTrue(decoded.sequenceNumber() >= 0L);
    }
  }

  @Test
  public void testDecodedMessageKeepsTheVersionItArrivedWith() {
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
    byte[] framed = toByteArray(new AckMessage(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, 40L).toByteBuffer());
    framed[1] = UNSUPPORTED_VERSION;
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.Decoder.fromByteBuffer(ByteBuffer.wrap(framed)));
  }

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

  @Test
  public void testMaxBlockSizeConstant() {
    assertEquals(2 * 1024 * 1024, DataBlockMessage.MAX_BLOCK_SIZE_BYTES);
    assertEquals(2097152, DataBlockMessage.MAX_BLOCK_SIZE_BYTES);
  }

  @Test
  public void testPayloadExactlyAtCapIsAccepted() {
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
    for (int hostilePrefix :
        new int[] {Integer.MAX_VALUE, DataBlockMessage.MAX_BLOCK_SIZE_BYTES + 1}) {
      ByteBuf buf = dataBlockBodyWithPayloadPrefix(hostilePrefix, 0);
      assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.decode(buf));
    }
  }

  @Test
  public void testNegativePayloadLengthPrefixIsRejected() {
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
    byte[] payload = payload(1024);
    DataBlockMessage block = DataBlockMessage.withComputedChecksum(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, payload);
    assertTrue(block.verifyChecksum());
    assertEquals(StreamingShuffleChecksum.computeBlock(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, payload), block.checksum());

    long checksum = block.checksum();
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
    assertEquals(28, StreamingShuffleChecksum.BLOCK_METADATA_PREAMBLE_LENGTH);
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
    for (byte id : new byte[] {UNKNOWN_TYPE_ID, (byte) 5, (byte) -1, Byte.MAX_VALUE}) {
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
          () -> StreamingShuffleMessageType.fromId(id));
      assertTrue(e.getMessage().contains("Unknown message type"), e.getMessage());
    }
  }

  @Test
  public void testDecoderRejectsUnknownFramingTypeByte() {
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

  @Test
  public void testDataBlockEqualityIsValueBasedOverThePayload() {
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
    assertNotEquals(block, new DataBlockMessage(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, block.checksum() + 1, payload(64)));
  }

  @Test
  public void testTheFourControlMessagesAreNeverEqualToEachOther() {
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
    assertTrue(rendered.length() < 200, "toString is too long: " + rendered.length());
  }

  @Test
  public void testMaximumFrameLengthMatchesTheLargestLegitimateFrame() {
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
    ByteBuffer overLong = ByteBuffer.allocate(StreamingShuffleMessage.MAX_FRAME_LENGTH + 1);
    overLong.put(StreamingShuffleMessageType.ACK.id());
    overLong.put(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION);
    overLong.clear();
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.Decoder.fromByteBuffer(overLong));
  }

  @Test
  public void testDecoderRejectsTrailingBytesAfterAValidMessage() {
    for (StreamingShuffleMessage message : oneOfEachType()) {
      byte[] valid = toByteArray(message.toByteBuffer());
      byte[] withSuffix = Arrays.copyOf(valid, valid.length + 3);
      assertThrows(IllegalArgumentException.class,
          () -> StreamingShuffleMessage.Decoder.fromByteBuffer(ByteBuffer.wrap(withSuffix)));
    }
  }

  @Test
  public void testEachControlDecoderRequiresAnExactBody() {
    for (StreamingShuffleMessage message : exactBodyMessages()) {
      byte[] body = encodedBody(message);
      ByteBuf shortBody = wrapped(Arrays.copyOf(body, body.length - 1));
      ByteBuf longBody = wrapped(Arrays.copyOf(body, body.length + 1));
      assertThrows(IllegalArgumentException.class, () -> decodeAs(message, shortBody));
      assertThrows(IllegalArgumentException.class, () -> decodeAs(message, longBody));
    }
  }

  @Test
  public void testNegativeHeaderIdentifiersAreRejectedOnDecode() {
    assertThrows(IllegalArgumentException.class, () -> decodeAckWithHeader(-1, PARTITION_ID, 0L));
    assertThrows(IllegalArgumentException.class, () -> decodeAckWithHeader(SHUFFLE_ID, -1, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> decodeAckWithHeader(SHUFFLE_ID, PARTITION_ID, -1L));
    assertThrows(IllegalArgumentException.class,
        () -> decodeAckWithHeader(Integer.MIN_VALUE, PARTITION_ID, 0L));
    assertThrows(IllegalArgumentException.class,
        () -> decodeAckWithHeader(SHUFFLE_ID, PARTITION_ID, Long.MIN_VALUE));
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
    assertDoesNotThrow(() -> new AckMessage(0, 0L, 0, AckMessage.NOTHING_CONSUMED));
  }

  @Test
  public void testAcknowledgementPositionDomain() {
    assertEquals(-1L, AckMessage.NOTHING_CONSUMED);
    AckMessage nothingConsumed =
        new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, AckMessage.NOTHING_CONSUMED);
    assertEquals(AckMessage.NOTHING_CONSUMED, nothingConsumed.consumerPosition());
    assertEquals(0L, nothingConsumed.sequenceNumber());
    assertEquals(nothingConsumed, roundTrip(nothingConsumed));
    assertDoesNotThrow(() -> ack(0L));
    assertDoesNotThrow(() -> ack(AckMessage.MAX_CONSUMER_POSITION));
    assertEquals(Long.MAX_VALUE - 1L, AckMessage.MAX_CONSUMER_POSITION);
    assertEquals(Long.MAX_VALUE, ack(AckMessage.MAX_CONSUMER_POSITION).sequenceNumber());
    for (long invalid : new long[] {-2L, -100L, Long.MIN_VALUE}) {
      assertThrows(IllegalArgumentException.class, () -> ack(invalid));
    }
    assertThrows(IllegalArgumentException.class, () -> ack(Long.MAX_VALUE));
  }

  @Test
  public void testAcknowledgementPositionDomainIsEnforcedOnDecode() {
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
    assertDoesNotThrow(() -> new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L));
    assertDoesNotThrow(
        () -> new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, Long.MAX_VALUE));
    assertThrows(IllegalArgumentException.class,
        () -> new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 5L, 4L));
    long widest = RetransmitRequestMessage.MAX_REQUESTED_BLOCKS - 1L;
    RetransmitRequestMessage atTheCeiling =
        new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L, widest);
    assertEquals(RetransmitRequestMessage.MAX_REQUESTED_BLOCKS, atTheCeiling.blockCount());
    assertEquals(atTheCeiling, roundTrip(atTheCeiling));
    assertThrows(IllegalArgumentException.class, () -> new RetransmitRequestMessage(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L, RetransmitRequestMessage.MAX_REQUESTED_BLOCKS));
    assertThrows(IllegalArgumentException.class,
        () -> new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L, Long.MAX_VALUE));
  }

  @Test
  public void testRetransmissionRequestWindowDomainIsEnforcedOnDecode() {
    ByteBuf inverted = buffer();
    writeHeader(inverted, SHUFFLE_ID, PARTITION_ID, 9L);
    inverted.writeLong(8L);
    assertThrows(IllegalArgumentException.class,
        () -> RetransmitRequestMessage.decode(inverted));

    ByteBuf overWide = buffer();
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
    assertDoesNotThrow(() -> new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L));
    assertEquals(0L, new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 0L)
        .totalBlocks());
    StreamTerminationMessage ten = new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID,
        10L);
    assertEquals(10L, ten.totalBlocks());
    assertEquals(10L, ten.sequenceNumber());
    assertEquals(ten, roundTrip(ten));
    assertThrows(IllegalArgumentException.class,
        () -> new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, -1L));
    assertThrows(IllegalArgumentException.class,
        () -> new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, Long.MIN_VALUE));
  }

  @Test
  public void testStreamTerminationBlockCountRelationIsEnforcedOnDecode() {
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

  @Test
  public void testAcknowledgesWithinAtBelowAndAboveTheBound() {
    assertTrue(ack(10L).acknowledgesWithin(10L));
    assertTrue(ack(9L).acknowledgesWithin(10L));
    assertFalse(ack(11L).acknowledgesWithin(10L));
    assertTrue(ack(AckMessage.MAX_CONSUMER_POSITION)
        .acknowledgesWithin(AckMessage.MAX_CONSUMER_POSITION));
    assertFalse(ack(AckMessage.MAX_CONSUMER_POSITION)
        .acknowledgesWithin(AckMessage.MAX_CONSUMER_POSITION - 1L));
  }

  @Test
  public void testAcknowledgesWithinTreatsNothingConsumedAsTheOnlySentinel() {
    for (long bound : new long[] {AckMessage.NOTHING_CONSUMED, 0L, 7L, Long.MAX_VALUE}) {
      assertTrue(ack(AckMessage.NOTHING_CONSUMED).acknowledgesWithin(bound),
          "the sentinel must pass against every bound, including " + bound);
    }
    assertFalse(ack(0L).acknowledgesWithin(AckMessage.NOTHING_CONSUMED));
  }

  @Test
  public void testAcknowledgesWithinReadsTheAcknowledgedPositionAndNotTheWireValue() {
    assertTrue(ack(5L).acknowledgesWithin(5L));
    assertFalse(ack(6L).acknowledgesWithin(5L));
    assertEquals(6L, ack(5L).sequenceNumber());
    assertEquals(ack(5L), ack(5L));
    assertNotEquals(ack(5L), ack(6L));
  }

  @Test
  public void testSupersedesIsStrictSoADuplicateIsRefused() {
    AckMessage atOneHundred = ack(100L);
    assertTrue(atOneHundred.supersedes(100L));
    assertFalse(atOneHundred.supersedes(101L));
    assertFalse(atOneHundred.supersedes(102L));
    assertFalse(atOneHundred.supersedes(Long.MAX_VALUE));
    assertTrue(ack(AckMessage.MAX_CONSUMER_POSITION).supersedes(Long.MAX_VALUE - 1L));
    assertFalse(ack(AckMessage.MAX_CONSUMER_POSITION).supersedes(Long.MAX_VALUE));
  }

  @Test
  public void testSupersedesAdmitsTheFirstMessageAgainstTheNothingAppliedSeed() {
    assertTrue(ack(AckMessage.NOTHING_CONSUMED).supersedes(AckMessage.NOTHING_CONSUMED));
    assertTrue(ack(0L).supersedes(AckMessage.NOTHING_CONSUMED));
  }

  @Test
  public void testSupersedesIsMonotonicOverARunAndRefusesAReplay() {
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

  @Test
  public void testMapIdSurvivesTheRoundTripOnEveryMessageType() {
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
    AckMessage base = ack(40L);
    assertNotEquals(base, new AckMessage(SHUFFLE_ID, MAP_ID + 1L, PARTITION_ID, 40L));
    assertNotEquals(base.hashCode(),
        new AckMessage(SHUFFLE_ID, MAP_ID + 1L, PARTITION_ID, 40L).hashCode());
    assertTrue(base.toString().contains("mapId=" + MAP_ID));

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
    for (StreamingShuffleMessage message : oneOfEachType()) {
      byte[] body = encodedBody(message);
      assertEquals(MAP_ID,
          ByteBuffer.wrap(body).getLong(StreamingShuffleMessage.HEADER_ENCODED_LENGTH),
          message.getClass().getSimpleName());
      assertEquals(MAP_ID, ByteBuffer.wrap(body).getLong(17));
    }
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

  @Test
  public void testFramingConstantsFormOneConsistentContract() {
    assertEquals(1, StreamingShuffleMessage.FRAME_TYPE_PREFIX_LENGTH);
    assertEquals(38, DataBlockMessage.FRAMING_OVERHEAD_BYTES);
    assertEquals(1 + StreamingShuffleMessage.HEADER_ENCODED_LENGTH + 8 + 8 + 4,
        DataBlockMessage.FRAMING_OVERHEAD_BYTES);
    assertEquals(2097190, DataBlockMessage.MAX_ENCODED_FRAME_BYTES);
    assertEquals(DataBlockMessage.MAX_BLOCK_SIZE_BYTES + DataBlockMessage.FRAMING_OVERHEAD_BYTES,
        DataBlockMessage.MAX_ENCODED_FRAME_BYTES);
    assertTrue(DataBlockMessage.MAX_ENCODED_FRAME_BYTES > DataBlockMessage.MAX_BLOCK_SIZE_BYTES);
    assertEquals(DataBlockMessage.MAX_ENCODED_FRAME_BYTES,
        StreamingShuffleMessage.MAX_FRAME_LENGTH);
  }

  @Test
  public void testMaximumPayloadOccupiesExactlyTheMaximumEncodedFrame() {
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
    DataBlockMessage empty = dataBlock(0);
    assertEquals(DataBlockMessage.FRAMING_OVERHEAD_BYTES,
        StreamingShuffleMessage.framedLength(empty.encodedLength()));
  }

  @Test
  public void testHeaderOccupiesFixedWireOffsetsInDeclaredOrder() {
    AckMessage message = new AckMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, 7L);
    ByteBuffer framed = ByteBuffer.wrap(toByteArray(message.toByteBuffer()));

    assertEquals(StreamingShuffleMessageType.ACK.id(), framed.get());
    assertEquals(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, framed.get());
    assertEquals(SHUFFLE_ID, framed.getInt());
    assertEquals(PARTITION_ID, framed.getInt());
    assertEquals(8L, framed.getLong());
    assertEquals(MAP_ID, framed.getLong());
    assertFalse(framed.hasRemaining(), "the frame carries no bytes beyond header and body");
    assertEquals(26, toByteArray(message.toByteBuffer()).length);
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
    assertFalse(framed.hasRemaining());
    assertEquals(34, toByteArray(message.toByteBuffer()).length);
  }

  @Test
  public void testShuffleIdAndPartitionIdAreNeverTransposed() {
    AckMessage decoded = (AckMessage) roundTrip(new AckMessage(1, MAP_ID, 2, 4L));
    assertEquals(1, decoded.shuffleId());
    assertEquals(2, decoded.partitionId());
    assertEquals(5L, decoded.sequenceNumber());
    assertEquals(4L, decoded.consumerPosition());
    assertNotEquals(decoded, new AckMessage(2, MAP_ID, 1, 4L));
  }

  @Test
  public void testHeaderRecordCarriesTheFourFieldsInOrder() {
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
    assertEquals(heartbeat(), new HeartbeatMessage(header, MAP_ID, CONSUMER_TOKEN));
    assertEquals(heartbeatWithoutToken(), new HeartbeatMessage(header, MAP_ID));
    assertEquals(retransmit(SEQUENCE_NUMBER),
        new RetransmitRequestMessage(header, MAP_ID, SEQUENCE_NUMBER));
    assertEquals(termination(SEQUENCE_NUMBER), new StreamTerminationMessage(header, MAP_ID));
  }

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

  @Test
  public void testCheckProtocolVersionNamesBothRevisions() {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.checkProtocolVersion(UNSUPPORTED_VERSION));
    assertTrue(error.getMessage().contains(String.valueOf(UNSUPPORTED_VERSION)),
        error.getMessage());
    assertTrue(error.getMessage().contains(
        String.valueOf(StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION)), error.getMessage());
  }

  @Test
  public void testVersionIsCheckedBeforeTheTypeByteIsResolved() {
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

  @Test
  public void testDataBlockDecodeRejectsATruncatedChecksum() {
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
    ByteBuf buf = dataBlockBodyWithPayloadPrefix(64, 16);
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.decode(buf));
    assertTrue(error.getMessage().contains("Truncated streaming shuffle data block payload"),
        error.getMessage());
    assertTrue(error.getMessage().contains("64"), error.getMessage());
    assertTrue(error.getMessage().contains("16"), error.getMessage());
  }

  @Test
  public void testZeroIsAValidHeaderIdentity() {
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
    DataBlockMessage empty = dataBlock(0);
    DataBlockMessage decoded = (DataBlockMessage) roundTrip(empty);
    assertEquals(empty, decoded);
    assertEquals(0, decoded.payloadLength());
    assertEquals(0, decoded.copyPayload().length);
    assertTrue(decoded.verifyChecksum());
    assertEquals(StreamingShuffleChecksum.computeBlock(SHUFFLE_ID, MAP_ID, PARTITION_ID,
        SEQUENCE_NUMBER,
        new byte[0]), decoded.checksum());
  }

  @Test
  public void testExtremeLegalHeaderValuesSurviveRoundTrip() {
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
      assertTrue(rendered.length() < 200, "toString is too long: " + rendered);
    }
  }

  // The ownership-transferring factory, which is the one route that does not copy.

  @Test
  public void testWithOwnedPayloadAdoptsTheArrayItIsGiven() {
    byte[] surrendered = payload(64);
    DataBlockMessage adopted = DataBlockMessage.withOwnedPayload(
        StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, SHUFFLE_ID, MAP_ID, PARTITION_ID,
        SEQUENCE_NUMBER, StreamingShuffleChecksum.computeBlock(
            SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, surrendered), surrendered);
    assertTrue(adopted.verifyChecksum());
    assertEquals(64, adopted.payloadLength());
    surrendered[0] = (byte) ~surrendered[0];
    assertEquals(surrendered[0], adopted.copyPayload()[0], "the factory adopts rather than copies");

    assertThrows(IllegalArgumentException.class, () -> DataBlockMessage.withOwnedPayload(
        StreamingShuffleMessage.CURRENT_PROTOCOL_VERSION, SHUFFLE_ID, MAP_ID, PARTITION_ID,
        SEQUENCE_NUMBER, 0L, new byte[DataBlockMessage.MAX_BLOCK_SIZE_BYTES + 1]));
    assertEquals(adopted, roundTrip(adopted));
  }

  @Test
  public void testEveryProtocolTypeIsMarkedPrivateApi() {
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
    // Final concrete messages mean the validation each constructor performs cannot be bypassed by a
    // subclass, which is what lets a decoded message be trusted without re-checking it.
    Class<?>[] concrete = {
        DataBlockMessage.class, AckMessage.class, HeartbeatMessage.class,
        RetransmitRequestMessage.class, StreamTerminationMessage.class,
    };
    for (Class<?> type : concrete) {
      assertTrue(Modifier.isFinal(type.getModifiers()), type.getName() + " is not final");
      assertTrue(StreamingShuffleMessage.class.isAssignableFrom(type), type.getName());
    }
    assertTrue(Modifier.isAbstract(StreamingShuffleMessage.class.getModifiers()));
    assertTrue(StreamingShuffleMessage.Header.class.isRecord());
    assertTrue(Modifier.isFinal(StreamingShuffleMessage.Header.class.getModifiers()));
  }

  @Test
  public void testChecksumHelperCannotBeInstantiated() {
    Constructor<?>[] constructors = StreamingShuffleChecksum.class.getDeclaredConstructors();
    assertEquals(1, constructors.length);
    assertTrue(Modifier.isPrivate(constructors[0].getModifiers()));
  }

  @Test
  public void testOnlyTheFramingByteDistinguishesTheFixedSizeMessagesOnTheWire() {
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
      assertArrayEquals(reference, Arrays.copyOfRange(framed, 1, framed.length));
      StreamingShuffleMessage decoded =
          StreamingShuffleMessage.Decoder.fromByteBuffer(ByteBuffer.wrap(framed));
      assertSame(message.getClass(), decoded.getClass());
      assertEquals(message, decoded);
    }
  }

  @Test
  public void testChecksumMatchesThePublishedCheckValue() {
    assertEquals(0xE3069283L,
        StreamingShuffleChecksum.compute("123456789".getBytes(StandardCharsets.UTF_8)));
    assertTrue(StreamingShuffleChecksum.verify(
        "123456789".getBytes(StandardCharsets.UTF_8), 0xE3069283L));
  }

  @Test
  public void testNonCurrentProtocolVersionIsPreservedButItsFramedFormIsRejected() {
    AckMessage foreign =
        new AckMessage(UNSUPPORTED_VERSION, SHUFFLE_ID, MAP_ID, PARTITION_ID, 40L);
    assertEquals(UNSUPPORTED_VERSION, foreign.protocolVersion());
    assertEquals(40L, foreign.consumerPosition());
    assertNotEquals(ack(40L), foreign);

    ByteBuffer framed = foreign.toByteBuffer();
    assertEquals(UNSUPPORTED_VERSION, StreamingShuffleMessage.peekProtocolVersion(framed));
    assertThrows(IllegalArgumentException.class,
        () -> StreamingShuffleMessage.Decoder.fromByteBuffer(framed));
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

  private StreamingShuffleMessage[] oneOfEachType() {
    return new StreamingShuffleMessage[] {
        dataBlock(1024), ack(40L), heartbeat(), retransmit(44L), termination(SEQUENCE_NUMBER),
    };
  }

  private StreamingShuffleMessage[] fixedSizeMessages() {
    return new StreamingShuffleMessage[] {
        ack(40L), termination(SEQUENCE_NUMBER)};
  }

  private StreamingShuffleMessage[] exactBodyMessages() {
    return new StreamingShuffleMessage[] {
        ack(40L), heartbeat(), retransmit(44L), termination(SEQUENCE_NUMBER)};
  }

  private DataBlockMessage dataBlock(int payloadLength) {
    return DataBlockMessage.withComputedChecksum(
        SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER, payload(payloadLength));
  }

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

  private HeartbeatMessage heartbeatWithoutToken() {
    return new HeartbeatMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER);
  }

  private RetransmitRequestMessage retransmit(long sequenceNumber) {
    return new RetransmitRequestMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, sequenceNumber);
  }

  private StreamTerminationMessage termination(long totalBlocks) {
    return new StreamTerminationMessage(SHUFFLE_ID, MAP_ID, PARTITION_ID, totalBlocks);
  }

  /** A deterministic payload of the requested length. */
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

  private static byte[] encodedBody(StreamingShuffleMessage message) {
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

  private static final int CONTROL_MESSAGE_BYTES = 25;

  private static final int RETRANSMIT_REQUEST_BYTES = CONTROL_MESSAGE_BYTES + 8;

  private static final int HEARTBEAT_BYTES = CONTROL_MESSAGE_BYTES + 8;

  private static final int HEADER_BYTES = 17;

  private static final int EMPTY_DATA_BLOCK_BYTES = 37;

  private static final int FRAME_TYPE_PREFIX_BYTES = 1;

  private static void decodeAckWithHeader(int shuffleId, int partitionId, long sequence) {
    decodeAckWithHeader(shuffleId, MAP_ID, partitionId, sequence);
  }

  private static void decodeAckWithHeader(
      int shuffleId, long mapId, int partitionId, long sequence) {
    ByteBuf buf = Unpooled.buffer();
    try {
      writeHeader(buf, shuffleId, mapId, partitionId, sequence);
      AckMessage.decode(buf);
    } finally {
      buf.release();
    }
  }

  private static HeartbeatMessage decodeHeartbeatWithToken(long consumerToken) {
    // Released in a finally rather than tracked, because this helper is static and so cannot reach
    // the instance ownership list -- and a release written after the decode is a release that every
    // refusing case skips, which is precisely the route this helper exists to exercise.
    ByteBuf buf = Unpooled.buffer();
    try {
      writeHeader(buf, SHUFFLE_ID, MAP_ID, PARTITION_ID, SEQUENCE_NUMBER);
      buf.writeLong(consumerToken);
      return HeartbeatMessage.decode(buf);
    } finally {
      buf.release();
    }
  }

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

  private static void assertFramingTypeByte(
      StreamingShuffleMessageType expected,
      StreamingShuffleMessage message) {
    ByteBuffer framed = message.toByteBuffer();
    assertEquals(expected.id(), framed.get(framed.position()));
    assertEquals(message.encodedLength() + 1, framed.remaining());
  }

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
