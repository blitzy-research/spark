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

import org.apache.spark.annotation.Private;

/**
 * The one-byte discriminator for the streaming shuffle wire protocol.
 *
 * Every streaming shuffle message is framed on the wire as a single type byte followed by the
 * message body, so this enum is what lets a received frame be routed to the correct decoder. The
 * byte is written as the framing prefix by {@code StreamingShuffleMessage.toByteBuffer()} and
 * consumed by {@code StreamingShuffleMessage.Decoder.fromByteBuffer(ByteBuffer)}.
 *
 * The five types cover the whole streaming exchange: {@code DATA_BLOCK} carries a payload block
 * from producer to consumer, {@code ACK} reports the consumer's position so the producer can
 * reclaim buffers, {@code HEARTBEAT} keeps liveness detection armed while no data is flowing,
 * {@code RETRANSMIT_REQUEST} asks for a block to be replayed from the unacknowledged window, and
 * {@code STREAM_TERMINATION} signals an orderly end of stream.
 *
 * This is a deliberately separate, parallel id space from the {@link
 * org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type} enum used by the
 * block-transfer and push-based-shuffle families: streaming shuffle neither extends nor
 * interoperates with them, so its ids start again at 0. The two enums are never mixed on a single
 * channel, which is why the overlap in numeric values is harmless. Because these are wire values
 * they are frozen: an existing constant must never be renumbered, and a new message type must take
 * the next unused id, so that a peer built against a different revision fails fast with an explicit
 * error instead of silently misreading a frame.
 *
 * This type is internal to Spark: it is part of a wire protocol between Spark's own executors, not
 * a surface any third party is invited to build against, and it carries no compatibility guarantee
 * across releases.
 *
 * @since 4.2.0
 */
@Private
public enum StreamingShuffleMessageType {
  DATA_BLOCK(0), ACK(1), HEARTBEAT(2), RETRANSMIT_REQUEST(3), STREAM_TERMINATION(4);

  private final byte id;

  StreamingShuffleMessageType(int id) {
    assert id < 128 : "Cannot have more than 128 message types";
    this.id = (byte) id;
  }

  /**
   * The byte written to the wire for this message type.
   *
   * This accessor is deliberately public and must remain so. Unlike {@link
   * org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type}, which is nested inside
   * the class that encodes it and can therefore read the private field directly, this enum is a
   * top-level type in its own file, so the encoder reaches the wire value through {@code
   * type().id()}.
   *
   * @return the wire id of this message type, always in the range 0 to 127 inclusive
   */
  public byte id() { return id; }

  /**
   * Resolves a type byte read off the wire back into the constant it denotes.
   *
   * @param id the type byte, as produced by {@link #id()}
   * @return the message type carrying that id
   * @throws IllegalArgumentException if the byte denotes no known message type; this is the
   *         mechanism by which a frame from an incompatible or corrupt peer is rejected rather
   *         than misinterpreted, and callers translate it into a protocol-level failure
   */
  public static StreamingShuffleMessageType fromId(byte id) {
    // A switch rather than a scan over values(): values() defensively clones its backing array on
    // every call, and this method runs once per received frame on the shuffle hot path.
    return switch (id) {
      case 0 -> DATA_BLOCK;
      case 1 -> ACK;
      case 2 -> HEARTBEAT;
      case 3 -> RETRANSMIT_REQUEST;
      case 4 -> STREAM_TERMINATION;
      default -> throw new IllegalArgumentException("Unknown message type: " + id);
    };
  }
}
