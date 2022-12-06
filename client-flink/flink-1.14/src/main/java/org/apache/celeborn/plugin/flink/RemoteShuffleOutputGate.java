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

import java.io.IOException;
import java.util.Optional;

import org.apache.celeborn.plugin.flink.utils.BufferUtils;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.celeborn.plugin.flink.utils.CommonUtils;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.protocol.PartitionLocation;

/**
 * A transportation gate used to spill buffers from {@link ResultPartitionWriter} to remote shuffle
 * worker.
 */
public class RemoteShuffleOutputGate {

  /** A {@link ShuffleDescriptor} which describes shuffle meta and shuffle worker address. */
  private final RemoteShuffleDescriptor shuffleDesc;

  /** Number of subpartitions of the corresponding {@link ResultPartitionWriter}. */
  protected final int numSubs;

  /** Used to transport data to a shuffle worker. */
  private final ShuffleClient shuffleWriteClient;

  /** Used to consolidate buffers. */
  private final BufferPacker bufferPacker;

  /** {@link BufferPool} provider. */
  protected final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

  /** Provides buffers to hold data to send online by Netty layer. */
  protected BufferPool bufferPool;

  private CelebornConf celebornConf;

  // the number of map tasks
  private final int numMappers;

  private PartitionLocation partitionLocation;

  private int currentRegionIndex = 0;

  private int bufferSize;
  private String applicationId;
  private int shuffleId;
  private int mapId;
  private int attemptId;
  private String rssMetaServiceHost;
  private int rssMetaServicePort;
  private UserIdentifier userIdentifier;

  /**
   * @param shuffleDesc Describes shuffle meta and shuffle worker address.
   * @param numSubs Number of subpartitions of the corresponding {@link ResultPartitionWriter}.
   * @param bufferPoolFactory {@link BufferPool} provider.
   */
  public RemoteShuffleOutputGate(
      RemoteShuffleDescriptor shuffleDesc,
      int numSubs,
      int bufferSize,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory,
      CelebornConf celebornConf,
      int numMappers) {

    this.shuffleDesc = shuffleDesc;
    this.numSubs = numSubs;
    this.bufferPoolFactory = bufferPoolFactory;
    this.shuffleWriteClient = createWriteClient();
    this.bufferPacker = new BufferPacker(this::write);
    this.celebornConf = celebornConf;
    this.numMappers = numMappers;
    this.bufferSize = bufferSize;
    this.applicationId = shuffleDesc.getJobID().toString();
    this.shuffleId =
        shuffleDesc.getShuffleResource().getMapPartitionShuffleDescriptor().getShuffleId();
    this.mapId = shuffleDesc.getShuffleResource().getMapPartitionShuffleDescriptor().getMapId();
    this.attemptId =
        shuffleDesc.getShuffleResource().getMapPartitionShuffleDescriptor().getAttemptId();
    this.rssMetaServiceHost =
        ((RemoteShuffleResource) shuffleDesc.getShuffleResource()).getRssMetaServiceHost();
    this.rssMetaServicePort =
        ((RemoteShuffleResource) shuffleDesc.getShuffleResource()).getRssMetaServicePort();
  }

  /** Initialize transportation gate. */
  public void setup() throws IOException, InterruptedException {
    bufferPool = CommonUtils.checkNotNull(bufferPoolFactory.get());
    CommonUtils.checkArgument(
        bufferPool.getNumberOfRequiredMemorySegments() >= 2,
        "Too few buffers for transfer, the minimum valid required size is 2.");

    // guarantee that we have at least one buffer
    BufferUtils.reserveNumRequiredBuffers(bufferPool, 1);

    // handshake
    handshake();
  }

  /** Get transportation buffer pool. */
  public BufferPool getBufferPool() {
    return bufferPool;
  }

  /** Writes a {@link Buffer} to a subpartition. */
  public void write(Buffer buffer, int subIdx) throws InterruptedException {
    bufferPacker.process(buffer, subIdx);
  }

  /**
   * Indicates the start of a region. A region of buffers guarantees the records inside are
   * completed.
   *
   * @param isBroadcast Whether it's a broadcast region.
   */
  public void regionStart(boolean isBroadcast) {
    Optional<PartitionLocation> newPartitionLoc = null;
    try {
      newPartitionLoc =
          shuffleWriteClient.regionStart(
              applicationId,
              shuffleId,
              mapId,
              attemptId,
              partitionLocation,
              currentRegionIndex,
              isBroadcast);
      // revived
      if (newPartitionLoc.isPresent()) {
        partitionLocation = newPartitionLoc.get();
        // send handshake again
        handshake();
        // send regionstart again
        shuffleWriteClient.regionStart(
            applicationId,
            shuffleId,
            mapId,
            attemptId,
            newPartitionLoc.get(),
            currentRegionIndex,
            isBroadcast);
      }
    } catch (IOException e) {
      CommonUtils.rethrowAsRuntimeException(e);
    }
  }

  /**
   * Indicates the finish of a region. A region is always bounded by a pair of region-start and
   * region-finish.
   */
  public void regionFinish() throws InterruptedException {
    bufferPacker.drain();
    try {
      shuffleWriteClient.regionFinish(
          applicationId, shuffleId, mapId, attemptId, partitionLocation);
      currentRegionIndex++;
    } catch (IOException e) {
      CommonUtils.rethrowAsRuntimeException(e);
    }
  }

  /** Indicates the writing/spilling is finished. */
  public void finish() throws InterruptedException, IOException {
    shuffleWriteClient.mapperEnd(applicationId, shuffleId, mapId, attemptId, numMappers);
  }

  /** Close the transportation gate. */
  public void close() throws IOException {
    if (bufferPool != null) {
      bufferPool.lazyDestroy();
    }
    bufferPacker.close();
    shuffleWriteClient.shutDown();
  }

  /** Returns shuffle descriptor. */
  public RemoteShuffleDescriptor getShuffleDesc() {
    return shuffleDesc;
  }

  private ShuffleClient createWriteClient() {
    return ShuffleClient.get(rssMetaServiceHost, rssMetaServicePort, celebornConf, userIdentifier);
  }

  /** Writes a piece of data to a subpartition. */
  public void write(ByteBuf byteBuf, int subIdx) throws InterruptedException {
    try {
      // byteBuf.retain();
      shuffleWriteClient.pushData(
          applicationId,
          shuffleId,
          mapId,
          attemptId,
          subIdx,
          io.netty.buffer.Unpooled.wrappedBuffer(byteBuf.nioBuffer()),
          partitionLocation,
          () -> byteBuf.release());
    } catch (IOException e) {
      CommonUtils.rethrowAsRuntimeException(e);
    }
  }

  public void handshake() {
    if (partitionLocation == null) {
      partitionLocation =
          shuffleWriteClient.registerMapPartitionTask(
              applicationId, shuffleId, numMappers, mapId, attemptId);
    }
    currentRegionIndex = 0;
    try {
      shuffleWriteClient.pushDataHandShake(
          applicationId, shuffleId, mapId, attemptId, numSubs, bufferSize, partitionLocation);
    } catch (IOException e) {
      CommonUtils.rethrowAsRuntimeException(e);
    }
  }
}
