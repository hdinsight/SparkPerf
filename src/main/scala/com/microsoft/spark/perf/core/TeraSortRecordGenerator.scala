package com.microsoft.spark.perf.core

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

private[core] object TeraSortRecordGenerator {
  private def generateRecord(
    recBuf: Array[Byte],
    rand: Unsigned16,
    recordNumber: Unsigned16): Unit = {
    // Generate the 10-byte key using the high 10 bytes of the 128-bit random number
    var i = 0
    while (i < 10) {
      recBuf(i) = rand.getByte(i)
      i += 1
    }

    // Add 2 bytes of "break"
    recBuf(10) = 0x00.toByte
    recBuf(11) = 0x11.toByte

    // Convert the 128-bit record number to 32 bits of ascii hexadecimal
    // as the next 32 bytes of the record. (each bit maps to a byte)
    i = 0
    while (i < 32) {
      recBuf(12 + i) = recordNumber.getHexDigit(i).toByte
      i += 1
    }

    // Add 4 bytes of "break" data
    recBuf(44) = 0x88.toByte
    recBuf(45) = 0x99.toByte
    recBuf(46) = 0xAA.toByte
    recBuf(47) = 0xBB.toByte

    // Add 48 bytes of filler based on low 48 bits of random number
    i = 0
    while (i < 12) {
      val v = rand.getHexDigit(20 + i).toByte
      recBuf(48 + i * 4) = v
      recBuf(49 + i * 4) = v
      recBuf(50 + i * 4) = v
      recBuf(51 + i * 4) = v
      i += 1
    }

    // Add 4 bytes of "break" data
    recBuf(96) = 0xCC.toByte
    recBuf(97) = 0xDD.toByte
    recBuf(98) = 0xEE.toByte
    recBuf(99) = 0xFF.toByte
  }

  private[core] def sizeStrToBytes(str: String): Long = {
    val lower = str.toLowerCase
    if (lower.endsWith("k")) {
      lower.substring(0, lower.length - 1).toLong * 1000
    } else if (lower.endsWith("m")) {
      lower.substring(0, lower.length - 1).toLong * 1000 * 1000
    } else if (lower.endsWith("g")) {
      lower.substring(0, lower.length - 1).toLong * 1000 * 1000 * 1000
    } else if (lower.endsWith("t")) {
      lower.substring(0, lower.length - 1).toLong * 1000 * 1000 * 1000 * 1000
    } else {
      // no suffix, so it's just a number in bytes
      lower.toLong
    }
  }

  private[core] def generateInputRecords(
    sparkContext: SparkContext,
    sizeStr: String,
    inputPartitions: Int): RDD[RecordWrapper] = {
    val inputSizeInBytes = sizeStrToBytes(sizeStr)
    val recordsPerPartition = inputSizeInBytes / 100 / inputPartitions
    sparkContext.parallelize(1 to inputPartitions, inputPartitions).
      mapPartitionsWithIndex { case (index, _) =>
        val one = new Unsigned16(1)
        val firstRecordNumber = new Unsigned16(index.toLong * recordsPerPartition.toLong)
        val recordsToGenerate = new Unsigned16(recordsPerPartition)

        val recordNumber = new Unsigned16(firstRecordNumber)
        val lastRecordNumber = new Unsigned16(firstRecordNumber)
        lastRecordNumber.add(recordsToGenerate)

        val rand = Random16.skipAhead(firstRecordNumber)

        Iterator.tabulate(recordsPerPartition.toInt) { offset =>
          val rowBytes: Array[Byte] = new Array[Byte](100)
          val row = new RecordWrapper(rowBytes)
          Random16.nextRand(rand)
          generateRecord(rowBytes, rand, recordNumber)
          recordNumber.add(one)
          row
        }
      }
  }
}
