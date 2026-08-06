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
 * The one-byte discriminator for the streaming shuffle wire protocol. Its id space is separate from
 * that of {@link org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type}: the two
 * families never share a channel, so ids repeat between them harmlessly.
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

  /** The byte written to the wire for this message type. */
  public byte id() { return id; }

  public static StreamingShuffleMessageType fromId(byte id) {
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
