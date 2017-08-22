package com.microsoft.spark.perf.core

import org.apache.spark.sql.SparkSession

object TeraSortDataGenerator {

  private def measureTimeMs[A](f: => A) : (Double, A) = {
    val startTime = System.nanoTime()
    val res = f
    val endTime = System.nanoTime()
    (endTime - startTime).toDouble / 1000000 -> res
  }

  def main(args: Array[String]): Unit = {
    val sizeStr = args(0)
    val inputPartitions = args(1).toInt
    val outputPath = args(2)

    val sparkSession = SparkSession
      .builder()
      .appName("TeraSortDataGenerator")
      .getOrCreate()

    println("GENERATING records")
    println(s"Total size: $sizeStr")
    println(s"Partition count: $inputPartitions")
    println(s"Output path: $outputPath")

    val (timeCost, _) = measureTimeMs {
      val dataset = TeraSortRecordGenerator.generateInputRecords(
        sparkSession.sparkContext,
        sizeStr,
        inputPartitions)
      dataset.map(_._1).saveAsObjectFile(outputPath)
    }

    println(s"Data Generation Time: $timeCost ms")
  }
}
