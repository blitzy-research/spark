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

import io.netty.buffer.ByteBuf;

import org.apache.spark.annotation.Private;

/**
 * The orderly end-of-stream signal of the streaming shuffle wire protocol, whose inherited sequence
 * number is the total number of data blocks the producer sent for the partition.
 *
 * That count is what lets a consumer distinguish a stream that ended from one that stopped: a
 * terminator names how much output it should have seen, so a short stream is a detected failure
 * rather than a silently truncated read.
 *
 * @since 4.2.0
 */
@Private
public final class StreamTerminationMessage extends StreamingShuffleMessage {

  /** Bytes this message adds to the header: the producer id is its whole body. */
  private static final int BODY_ENCODED_LENGTH = PRODUCER_ID_ENCODED_LENGTH;

  public StreamTerminationMessage(
      byte protocolVersion,
      int shuffleId,
      long mapId,
      int partitionId,
      long totalBlocks) {
    // Signalled as an argument failure rather than an error: a peer that sends a nonsensical count
    // is a recoverable protocol fault the caller converts into a fetch failure, not a condition
    // that should tear the executor down.
    super(protocolVersion, shuffleId, mapId, partitionId, totalBlocks);
  }

  public StreamTerminationMessage(
      int shuffleId,
      long mapId,
      int partitionId,
      long totalBlocks) {
    this(CURRENT_PROTOCOL_VERSION, shuffleId, mapId, partitionId, totalBlocks);
  }

  public StreamTerminationMessage(Header header, long mapId) {
    super(header, mapId);
  }

  /**
   * Number of data blocks the producer sent on this stream before ending it, read back from the
   * position this terminator sits at.
   */
  public long totalBlocks() {
    return sequenceNumber();
  }

  @Override
  protected StreamingShuffleMessageType type() {
    return StreamingShuffleMessageType.STREAM_TERMINATION;
  }

  @Override
  public int hashCode() {
    return headerHashCode();
  }

  @Override
  public String toString() {
    return "StreamTerminationMessage[" + headerToString() +
        ",totalBlocks=" + totalBlocks() + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof StreamTerminationMessage o) {
      return headerEquals(o);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return HEADER_ENCODED_LENGTH + BODY_ENCODED_LENGTH;
  }

  @Override
  public void encode(ByteBuf buf) {
    encodeHeader(buf);
    encodeProducerId(buf);
  }

  /**
   * Reads a terminator from a buffer positioned at the first byte of the encoded body, that is,
   * immediately after the framing type discriminator has been consumed.
   *
   * @throws NullPointerException if buf is null
   * @throws IllegalArgumentException if the frame is truncated, if its body is not exactly the
   *     specified size, or if any identifier falls outside its domain
   */
  static StreamTerminationMessage decode(ByteBuf buf) {
    Header header = readHeader(buf);
    // Exactly the body: a shortfall is a truncated frame and a surplus is unparsed content.
    if (buf.readableBytes() != BODY_ENCODED_LENGTH) {
      throw new IllegalArgumentException("Malformed streaming shuffle stream termination: " +
        "expected exactly " + BODY_ENCODED_LENGTH + " byte(s) of body but " +
        buf.readableBytes() + " remain");
    }
    long mapId = readProducerId(buf);
    return new StreamTerminationMessage(header, mapId);
  }
}
