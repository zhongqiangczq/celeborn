package org.apache.celeborn.tests.flink

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Paths, StandardOpenOption}

import org.apache.flink.runtime.io.network.api.serialization.EventSerializer
import org.apache.flink.runtime.io.network.buffer.{BufferDecompressor, NetworkBuffer, NetworkBufferPool}
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf

import org.apache.celeborn.common.exception.FileCorruptedException
import org.apache.celeborn.plugin.flink.buffer.BufferPacker
import org.apache.celeborn.plugin.flink.utils.Utils

object ParseShuffleWrite {
  def main(args: Array[String]): Unit = {
    println(s"datafile:${args(0)}, indexFile:${args(1)}, subPartitionNums:${args(2)}")
    parseShuffleWriteFile(new File(args(0)), new File(args(1)), Integer.valueOf(args(2)))

  }
  def parseShuffleWriteFile(datafile: File, indexFile: File, subPatitionNums: Integer): Unit = {
    val dataFileChanel = openReadableFileChannel(datafile.getAbsolutePath)
    val indexChannel = openReadableFileChannel(indexFile.getAbsolutePath)
    val indexSize = indexChannel.size

    val indexRegionSize = subPatitionNums * 16
    val numRegions = Utils.checkedDownCast(indexSize / indexRegionSize)
    val totalPartitionNums = indexSize / 16
    val indexBuffer = ByteBuffer.allocateDirect(16)
    println(
      s"filename: ${indexFile.getAbsolutePath}, indexSize: ${indexSize}, numRegions:${indexSize.doubleValue() / indexRegionSize}")
    var startIndex = 0
    val headerBuffer = ByteBuffer.allocateDirect(16)
    // var dataByteBuffer = Unpooled.buffer(32 * 1024)
    val networkBufferPool = new NetworkBufferPool(2, 32 * 1024)
    val bufferPool = networkBufferPool.createBufferPool(2, 2)

    while (startIndex < totalPartitionNums) {
      readRegionOrDataHeader(indexChannel, indexBuffer, 16)
      val dataConsumingOffset = indexBuffer.getLong
      var currentPartitionRemainingBytes = indexBuffer.getLong
      println(s"partitionStartIndex: $startIndex offset:$dataConsumingOffset, length:$currentPartitionRemainingBytes")
      // read data
      dataFileChanel.position(dataConsumingOffset)
      while (currentPartitionRemainingBytes > 0) {
        readRegionOrDataHeader(dataFileChanel, headerBuffer, headerBuffer.capacity())
        val bufferLength = headerBuffer.getInt(12)
        val segment = bufferPool.requestMemorySegmentBlocking()
        val dataByteBuffer = new NetworkBuffer(segment, bufferPool)

        val readSize = readBuffer(dataFileChanel, headerBuffer, dataByteBuffer, bufferLength)
        currentPartitionRemainingBytes -= readSize
        println(s"  partitionStartIndex: $startIndex, subPartitionid: ${headerBuffer.getInt(
          0)} attemptId: ${headerBuffer.getInt(4)}" +
          s" readSize: $readSize, currentPartitionRemainingBytes: $currentPartitionRemainingBytes")
        val unpackedBuffers = BufferPacker.unpack(dataByteBuffer)
        var unpackedBufferCnt = 0;
        while (!unpackedBuffers.isEmpty) {
          val sliceBuffer = unpackedBuffers.poll
          if (sliceBuffer.isBuffer) {
            val bufferCompressor = new BufferDecompressor(32 * 1024, "LZ4")
            if (sliceBuffer.isCompressed) {
              try {
                val decompressoredBuffer =
                  bufferCompressor.decompressToIntermediateBuffer(sliceBuffer)
                println(s"   currentSliceIndex: $unpackedBufferCnt isDataBuffer compress: size: ${decompressoredBuffer.getSize}")
              } catch {
                case e: Exception => {
                  println(
                    s"   currentSliceIndex: $unpackedBufferCnt isDataBuffer compress error ${e.getMessage}")
                }
              }
            } else {
              println(s"   currentSliceIndex: $unpackedBufferCnt isDataBuffer notcompress size: ${sliceBuffer.getSize}")
            }
          } else {
            val event = EventSerializer.fromBuffer(sliceBuffer, getClass.getClassLoader)

            println(s"    currentSliceIndex: $unpackedBufferCnt isEventBuffer, ${event.getClass}")
          }
          unpackedBufferCnt += 1
          if (dataByteBuffer.refCnt() > 1) {
            dataByteBuffer.release(1)
          }
        }
        dataByteBuffer.recycleBuffer()
      }
      startIndex = startIndex + 1
    }
    bufferPool.lazyDestroy()

  }

  def openReadableFileChannel(filePath: String): FileChannel =
    FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)
  private def readBuffer(
      channel: FileChannel,
      header: ByteBuffer,
      buffer: ByteBuf,
      bufferLength: Int) = {
    // header is combined of mapId(4),attemptId(4),nextBatchId(4) and total Compresszed Length(4)
    // we need size here,so we read length directly

    if (bufferLength <= 0 || bufferLength > buffer.capacity) {
      //      System.err("Incorrect buffer header, buffer length: " +  bufferLength)
      throw new FileCorruptedException("File is corrupted")
    }
    buffer.writeBytes(header)
    val tmpBuffer = ByteBuffer.allocate(bufferLength)
    while (tmpBuffer.hasRemaining) {
//      println(s"readerBuffer position ${tmpBuffer.position()}, totalLength: $bufferLength")
      channel.read(tmpBuffer)
    }
    tmpBuffer.flip
    buffer.writeBytes(tmpBuffer)
    bufferLength + 16
  }

  def readRegionOrDataHeader(
      channel: FileChannel,
      buffer: ByteBuffer,
      bufferLength: Integer): Unit = {
    buffer.clear
    buffer.limit(bufferLength)
    while (buffer.hasRemaining) {
      channel.read(buffer)
    }
    buffer.flip
  }

}
