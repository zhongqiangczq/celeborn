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

package org.apache.celeborn.plugin.flink;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;

import org.apache.celeborn.plugin.flink.buffer.PartitionSortedBuffer;
import org.apache.celeborn.plugin.flink.buffer.SortBuffer;

/**
 * A {@link SortBuffer} implementation which sorts all appended records only by subpartition index.
 * Records of the same subpartition keep the appended order.
 *
 * <p>It maintains a list of {@link MemorySegment}s as a joint buffer. Data will be appended to the
 * joint buffer sequentially. When writing a record, an index entry will be appended first. An index
 * entry consists of 4 fields: 4 bytes for record length, 4 bytes for {@link DataType} and 8 bytes
 * for address pointing to the next index entry of the same channel which will be used to index the
 * next record to read when coping data from this {@link SortBuffer}. For simplicity, no index entry
 * can span multiple segments. The corresponding record data is seated right after its index entry
 * and different from the index entry, records have variable length thus may span multiple segments.
 */
@NotThreadSafe
public class PartitionSortedBufferWithLock extends PartitionSortedBuffer implements SortBuffer {

  private final Object lock;

  public PartitionSortedBufferWithLock() {
    this.lock = new Object();
  }

  public PartitionSortedBufferWithLock(
      BufferPool bufferPool,
      int numSubpartitions,
      int bufferSize,
      @Nullable int[] customReadOrder) {
    super(bufferPool, numSubpartitions, bufferSize, customReadOrder);
    this.lock = new Object();
  }

  protected void addBuffer(MemorySegment segment) {
    synchronized (lock) {
      super.addBuffer(segment);
    }
  }

  @Override
  public BufferWithChannel copyIntoSegment(
      MemorySegment target, BufferRecycler recycler, int offset) {
    synchronized (lock) {
      return super.copyIntoSegment(target, recycler, offset);
    }
  }

  @Override
  public void release() {
    synchronized (lock) {
      super.release();
    }
  }

  @Override
  public boolean isReleased() {
    // maybe releases by other threads
    synchronized (lock) {
      return super.isReleased();
    }
  }
}
