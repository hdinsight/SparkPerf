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

import java.io.File

import scala.collection.mutable.ArrayBuffer

import com.microsoft.spark.perf.report.ExecutionMode
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

class TeraSortSuite extends FunSuite with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder().config(
    new SparkConf().setAppName("SparkBenchmarkTest").setMaster("local[*]")).getOrCreate()

  private val sparkContext: SparkContext = sparkSession.sparkContext

  private val perRecordLengthInBytes = 100
  private val TeraDir = System.getProperty("java.io.tmpdir") + s"/TeraSortSuite"

  private def deleteAll(teraDir : File): Unit = {
    if (teraDir.isDirectory) {
      Option(teraDir.listFiles()).map(_.toList).getOrElse(Nil).foreach { file =>
          deleteAll(file)
      }
    }
    teraDir.delete()
  }

  private def validateOutputFile(outputDir : String) : String = {
    val newDir = new File(outputDir)
    assert(newDir.exists())
    newDir.listFiles().filter(file => file.length() > 0 &&
      !file.getName.contains(".crc"))(0).getAbsolutePath
  }

  override def beforeAll(): Unit = {
    new File(TeraDir).mkdir()
    sparkContext.setLogLevel("ERROR")
  }

  override def afterAll() {
    sparkSession.stop()
    deleteAll(new File(TeraDir))
  }

  test("the specified number of input records are generated correctly") {
    val partitionNum = Runtime.getRuntime.availableProcessors()
    var onFlyTeraSort = new OnFlyTeraSort(sparkContext, "20m",
      inputPartitions = partitionNum,
      outputPartitions = partitionNum)
    assert(TeraSortRecordGenerator.generateInputRecords(
      sparkContext,
      "20m",
      partitionNum).count() == 20 * 1000 * 1000 / perRecordLengthInBytes /
      partitionNum * partitionNum)

    onFlyTeraSort = new OnFlyTeraSort(sparkContext, "200m",
      inputPartitions = partitionNum,
      outputPartitions = partitionNum)
    assert(TeraSortRecordGenerator.generateInputRecords(
      sparkContext,
      "200m",
      partitionNum).count() == 200 * 1000 * 1000 / perRecordLengthInBytes
      / partitionNum * partitionNum)
  }

  test("the data is correctly sorted") {
    val partitionNum = Runtime.getRuntime.availableProcessors()
    val randomPath = TeraDir + s"/terasort-${System.currentTimeMillis()}"
    val onFlyTeraSort = new OnFlyTeraSort(sparkContext, "20m",
      inputPartitions = partitionNum,
      outputPartitions = 1,
      executionMode = ExecutionMode.WriteTextFile(randomPath))
    assert(TeraSortRecordGenerator.generateInputRecords(
      sparkContext,
      "20m",
      partitionNum).count() == 20 * 1000 * 1000 / perRecordLengthInBytes /
      partitionNum * partitionNum)
    onFlyTeraSort.benchmark(includeBreakdown = false,
      messages = new ArrayBuffer[String], timeout = 120 * 1000)
    assert(SortValidation.validate(validateOutputFile(randomPath),
      20 * 1000 * 1000 / perRecordLengthInBytes / partitionNum * partitionNum))
  }

  test("TeraGen and TeraSort") {
    val partitionNum = Runtime.getRuntime.availableProcessors()
    val currentTime = System.currentTimeMillis()
    val randomInputPath = TeraDir + s"/teragen-$currentTime"
    val randomOutputPath = TeraDir + s"/terasort-$currentTime"

    val dataset = TeraSortRecordGenerator.generateInputRecords(
      sparkContext,
      "20m",
      partitionNum)
    assert(dataset.count() == 20 * 1000 * 1000 / perRecordLengthInBytes /
       partitionNum * partitionNum)
    dataset.map(result => result._1).saveAsObjectFile(randomInputPath)
    assert(validateOutputFile(randomInputPath).length > 0)

    val terasort = new TeraSort(sparkContext,
      inputPath = randomInputPath,
      outputPath = randomOutputPath,
      inputPartitions = partitionNum,
      outputPartitions = 1,
      executionMode = ExecutionMode.WriteTextFile(randomOutputPath)
    )
    assert(terasort.generateInputRecords.count() == 20 * 1000 * 1000 / perRecordLengthInBytes /
        partitionNum * partitionNum)

    terasort.benchmark(includeBreakdown = false,
        messages = new ArrayBuffer[String], timeout = 120 * 1000)
    assert(SortValidation.validate(validateOutputFile(randomOutputPath),
      20 * 1000 * 1000 / perRecordLengthInBytes / partitionNum * partitionNum))
  }
}
