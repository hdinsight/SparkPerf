/*
 * Copyright (C) 2017 Microsoft
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.spark.perf.core

import scala.collection.mutable.ArrayBuffer

import com.microsoft.spark.perf.Benchmarkable
import com.microsoft.spark.perf.core.report.CoreBenchmarkResult
import com.microsoft.spark.perf.report.{BenchmarkResult, ExecutionMode, Failure}
import com.microsoft.spark.perf.report.ExecutionMode.{ForeachResults, WriteTextFile}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.ShuffledRDD

private[core] class OnFlyTeraSort(
    @transient sparkContext: SparkContext,
    sizeStr: String,
    inputPartitions: Int,
    outputPartitions: Int,
    override protected val executionMode: ExecutionMode = ExecutionMode.ForeachResults)
  extends Benchmarkable(sparkContext) {

  override val name: String = "OnFlyTeraSort"

  override protected def doBenchmark(
      includeBreakdown: Boolean,
      description: String,
      messages: ArrayBuffer[String]): BenchmarkResult = {
    try {
      val inputSizeInBytes = TeraSortRecordGenerator.sizeStrToBytes(sizeStr)
      val recordsPerPartition = inputSizeInBytes / 100 / inputPartitions

      assert(recordsPerPartition < Int.MaxValue, s"records per partition > ${Int.MaxValue}")

      // Generate the data on the fly.
      val dataset = TeraSortRecordGenerator.generateInputRecords(sparkContext, sizeStr,
        inputPartitions)
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
