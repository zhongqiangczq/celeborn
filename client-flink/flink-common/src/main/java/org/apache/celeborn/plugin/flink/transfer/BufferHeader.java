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

package org.apache.celeborn.plugin.flink.transfer;

import org.apache.flink.runtime.io.network.buffer.Buffer;

/** Header information for a {@link org.apache.flink.runtime.io.network.buffer.Buffer}. */
public class BufferHeader {

  private final int subPartitionIndex;

  private final int attemptId;

  private final int nextBatchId;

  // totalsize of all buffers, a buffer can contains other buffer,
  private final int totalSize;

  private final Buffer.DataType dataType;

  private final boolean isCompressed;

  // current buffer's size
  private final int size;

  public BufferHeader(Buffer.DataType dataType, boolean isCompressed, int size) {
    this(0, 0, 0, 0, dataType, isCompressed, size);
  }

  public BufferHeader(
      int subPartitionIndex,
      int attemptId,
      int nextBatchId,
      int totalSize,
      Buffer.DataType dataType,
      boolean isCompressed,
      int size) {
    this.subPartitionIndex = subPartitionIndex;
    this.attemptId = attemptId;
    this.nextBatchId = nextBatchId;
    this.totalSize = totalSize;
    this.dataType = dataType;
    this.isCompressed = isCompressed;
    this.size = size;
  }

  public Buffer.DataType getDataType() {
    return dataType;
  }

  public boolean isCompressed() {
    return isCompressed;
  }

  public int getSize() {
    return size;
  }

  public int getSubPartitionIndex() {
    return subPartitionIndex;
  }

  public int getAttemptId() {
    return attemptId;
  }

  public int getNextBatchId() {
    return nextBatchId;
  }

  public int getTotalSize() {
    return totalSize;
  }
}
