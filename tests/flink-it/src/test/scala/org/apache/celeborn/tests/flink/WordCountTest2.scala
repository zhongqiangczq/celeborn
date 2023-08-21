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

package org.apache.celeborn.tests.flink

import java.io.File
import java.nio.{Buffer, ByteBuffer}
import java.nio.channels.FileChannel

import scala.collection.JavaConverters._
import org.apache.flink.api.common.{ExecutionMode, RuntimeExecutionMode}
import org.apache.flink.configuration.{Configuration, ExecutionOptions, RestOptions}
import org.apache.flink.runtime.jobgraph.JobType
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.GlobalStreamExchangeMode
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.FileCorruptedException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.plugin.flink.buffer.BufferPacker
import org.apache.celeborn.plugin.flink.utils.Utils
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.worker.Worker
import org.apache.celeborn.service.deploy.worker.storage.FileChannelUtils
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer
import org.apache.flink.runtime.io.network.buffer.{BufferCompressor, BufferDecompressor, NetworkBufferPool}
import org.apache.flink.shaded.netty4.io.netty.buffer.{ByteBuf, Unpooled}

class WordCountTest2 extends AnyFunSuite with Logging with MiniClusterFeature
  with BeforeAndAfterAll {
  var workers: collection.Set[Worker] = null

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup celeborn mini cluster")
    val masterConf = Map(
      "celeborn.master.host" -> "localhost",
      "celeborn.master.port" -> "9097")
    val workerConf = Map("celeborn.master.endpoints" -> "localhost:9097")
    workers = setUpMiniCluster(masterConf, workerConf, 1)._2
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    workers.foreach(w => {
      val workingdirs = w.conf.get(CelebornConf.WORKER_STORAGE_DIRS).get.head
      val baseDir = workingdirs + "/" + w.conf.get(CelebornConf.WORKER_WORKING_DIR)
      val shuffleBaseDir = new File(baseDir).listFiles.head.listFiles().head
      val file1 = shuffleBaseDir.listFiles()(0)
      val file2 = shuffleBaseDir.listFiles()(1)
      var dataFile = file1
      var indexFile = file2
      if (file1.getName.contains(".index")) {
        indexFile = file1
        dataFile = file2
      }
      println(s"datafileName:${dataFile.getName} index FileName: ${indexFile.getName}")
      parseShuffleWriteFile(dataFile, indexFile, 8)
    })

    shutdownMiniCluster()
  }

  def parseShuffleWriteFile(datafile: File, indexFile: File, subPatitionNums: Integer): Unit = {
    val dataFileChanel = FileChannelUtils.openReadableFileChannel(datafile.getAbsolutePath)
    val indexChannel = FileChannelUtils.openReadableFileChannel(indexFile.getAbsolutePath)
    val indexSize = indexChannel.size

    val indexRegionSize = subPatitionNums * 16
    val numRegions = Utils.checkedDownCast(indexSize / indexRegionSize)
    val totalPartitionNums = indexSize/16
    val indexBuffer = ByteBuffer.allocateDirect(16)
    println(s"filename: ${indexFile.getAbsolutePath}, numRegions:$numRegions")
    var startIndex = 0
    val headerBuffer = ByteBuffer.allocateDirect(16)
    //var dataByteBuffer = Unpooled.buffer(32 * 1024)
    val networkBufferPool = new NetworkBufferPool(2, 32 * 1024)
    val bufferPool = networkBufferPool.createBufferPool(1, 2)
    while (startIndex < totalPartitionNums) {
      readRegionOrDataHeader(indexChannel, indexBuffer, 16)
      val dataConsumingOffset = indexBuffer.getLong
      val currentPartitionRemainingBytes = indexBuffer.getLong
      println(s"offset:$dataConsumingOffset, length:$currentPartitionRemainingBytes" )
      //read data
      readRegionOrDataHeader(dataFileChanel, headerBuffer, headerBuffer.capacity())
      val dataByteBuffer = bufferPool.requestBuffer().asByteBuf()

      readBuffer(dataFileChanel, headerBuffer, dataByteBuffer, 16)

      val unpackedBuffers = BufferPacker.unpack(dataByteBuffer)
      while (!unpackedBuffers.isEmpty) {
        val sliceBuffer = unpackedBuffers.poll
        if (sliceBuffer.isBuffer) {
          val bufferCompressor = new BufferDecompressor(32 * 1024 , "LZ4")
          bufferCompressor.decompressToIntermediateBuffer(sliceBuffer)
          println("isDataBuffer")
        } else {
          val event = EventSerializer.fromBuffer(sliceBuffer, getClass.getClassLoader)

          println(s"isEventBuffer, ${event.getClass}")
        }
      }
      dataByteBuffer.release()
      startIndex = startIndex + 1
    }


  }

  private def readBuffer(channel: FileChannel, header: ByteBuffer, buffer: ByteBuf, headerSize: Int) = {
    // header is combined of mapId(4),attemptId(4),nextBatchId(4) and total Compresszed Length(4)
    // we need size here,so we read length directly
    val bufferLength = header.getInt(12)
    if (bufferLength <= 0 || bufferLength > buffer.capacity) {
//      System.err("Incorrect buffer header, buffer length: " +  bufferLength)
      throw new FileCorruptedException("File is corrupted")
    }
    buffer.writeBytes(header)
    val tmpBuffer = ByteBuffer.allocate(bufferLength)
    while (tmpBuffer.hasRemaining) channel.read(tmpBuffer)
    tmpBuffer.flip
    buffer.writeBytes(tmpBuffer)
    bufferLength + headerSize
  }

  def readRegionOrDataHeader(channel: FileChannel,  buffer: ByteBuffer, bufferLength: Integer): Unit = {
    buffer.clear
    buffer.limit(bufferLength)
    while (buffer.hasRemaining) {
      channel.read(buffer)
    }
    buffer.flip
  }


  test("celeborn flink integration test - word count") {
    // set up execution environment
    val configuration = new Configuration
    val parallelism = 8
    configuration.setString(
      "shuffle-service-factory.class",
      "org.apache.celeborn.plugin.flink.RemoteShuffleServiceFactory")
    configuration.setString("celeborn.master.endpoints", "localhost:9097")
    configuration.setString("execution.batch-shuffle-mode", "ALL_EXCHANGES_BLOCKING")
    configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH)
    configuration.setString("taskmanager.memory.network.min", "1024m")
    configuration.setString(RestOptions.BIND_PORT, "8081-8089")
    configuration.setString(
      "execution.batch.adaptive.auto-parallelism.min-parallelism",
      "" + parallelism)
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    env.getConfig.setExecutionMode(ExecutionMode.BATCH)
    env.getConfig.setParallelism(parallelism)
//    env.disableOperatorChaining()
    // make parameters available in the web interface
    WordCountHelper2.execute(env, parallelism)

    val graph = env.getStreamGraph
    graph.setGlobalStreamExchangeMode(GlobalStreamExchangeMode.ALL_EDGES_BLOCKING)
    graph.setJobType(JobType.BATCH)
    env.execute(graph)
    checkFlushingFileLength()
  }

  private def checkFlushingFileLength(): Unit = {
    workers.map(worker => {
      worker.storageManager.workingDirWriters.values().asScala.map(writers => {
        writers.forEach((fileName, fileWriter) => {
          assert(new File(fileName).length() == fileWriter.getFileInfo.getFileLength)
        })
      })
    })
  }
}
