package com.microsoft.spark.perf.core

import scala.collection.mutable.ArrayBuffer

import com.microsoft.spark.perf.Benchmarkable
import com.microsoft.spark.perf.core.report.CoreBenchmarkResult
import com.microsoft.spark.perf.report.{BenchmarkResult, ExecutionMode, Failure}
import com.microsoft.spark.perf.report.ExecutionMode.{ForeachResults, WriteTextFile}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, ShuffledRDD}

private[core] class TeraSort (
    @transient sparkContext: SparkContext,
    inputPath: String,
    outputPath: String = "",
    inputPartitions: Int,
    outputPartitions: Int,
    override protected val executionMode: ExecutionMode = ExecutionMode.ForeachResults)
  extends Benchmarkable(sparkContext) {

  override val name = "TeraSort"

  private[core] def generateInputRecords: RDD[RecordWrapper] = {
    println(s"Reading from $inputPath")
    val inputData = sparkContext.objectFile[Array[Byte]](inputPath, inputPartitions).cache()
    val dataset = inputData.map(record => new RecordWrapper(record)).cache()
    dataset
  }

  override protected def doBenchmark(
    includeBreakdown: Boolean,
    description: String,
    messages: ArrayBuffer[String]): BenchmarkResult = {
    try {
      val dataset = generateInputRecords
      val partitioner = new TeraSortPartitioner(outputPartitions * 2)
      val output = new ShuffledRDD[Array[Byte], Array[Byte], Array[Byte]](dataset, partitioner).
        cache()
      output.setSerializer(new TeraSortSerializer)
      output.setKeyOrdering(new TeraSortRecordOrdering)

      val executionTime = measureTimeMs {
        executionMode match {
          case ForeachResults =>
            output.foreach(_ => Unit)
          case WriteTextFile(outputPath) =>
            output.map(result => UnsafeUtils.getUnsafeInstance.getLong(result._1, 16)).
              saveAsTextFile(outputPath)
        }
      }
      new CoreBenchmarkResult(name, executionMode.toString, executionTime = Some(executionTime))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        BenchmarkResult(name, executionMode.toString,
          failure = Some(Failure(e.getClass.getName, e.getMessage)))
    }
  }
}
