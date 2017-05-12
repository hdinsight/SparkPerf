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

package com.microsoft.spark.perf.sql.tpcds

import java.io.File

import scala.collection.mutable.ArrayBuffer

import com.microsoft.spark.perf.report.ExecutionMode
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class TPCDSQueriesSuite extends FunSuite with BeforeAndAfterAll {

  private val timestamp = System.currentTimeMillis()

  private val tpcdsKitPath = System.getProperty("java.io.tmpdir") +
    s"/tpcds-$timestamp"
  private val tpcdsTablePath = System.getProperty("java.io.tmpdir") +
    s"/tpcds-tables-$timestamp"
  private val tpcdsImpalaOutputPath = System.getProperty("java.io.tmpdir") +
    s"/tpcds-ImpalaOutput-$timestamp"

  private val resourcePath = getClass.getResource("/compile_dsdgen.sh").getPath

  private var impalaKitQueries: ImpalaKitQueries = _

  override def beforeAll(): Unit = {
    val sparkSession = SparkSession.builder().config(
      new SparkConf().setAppName("SparkBenchmarkTest").setMaster("local[*]")).getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val pb = new ProcessBuilder("bash", resourcePath, tpcdsKitPath)
    val p = pb.start()
    p.waitFor()
    TPCDSDataGenerator.dataGen(sparkSession.sqlContext, tpcdsKitPath + "/tools",
      tpcdsTablePath, 1)
    System.setProperty("spark.master", "local[*]")
    System.setProperty("spark.app.name", "test")
    sparkSession.stop()
  }

  override def afterAll(): Unit = {
    deleteFolder(new File(tpcdsKitPath))
    deleteFolder(new File(tpcdsTablePath))
    deleteFolder(new File(tpcdsImpalaOutputPath))
    impalaKitQueries.stop()
  }

  private def deleteFolder(folder: File): Unit = {
    val files = folder.listFiles()
    if (files != null) {
      for (f <- files) {
        if (f.isDirectory) {
          deleteFolder(f)
        } else {
          f.delete()
        }
      }
    }
    folder.delete()
  }

  test("data are generated correctly") {
    // TPCDS 1.4
    assert(new File(tpcdsTablePath).listFiles().length == 24)
  }

  test("ImpalaSuite is running correctly") {
    impalaKitQueries = new ImpalaKitQueries(tpcdsTablePath,
      ExecutionMode.WriteParquet(tpcdsImpalaOutputPath))
    impalaKitQueries.start()
    impalaKitQueries.sparkContext.setLogLevel("ERROR")
    impalaKitQueries.buildTables()
    impalaKitQueries.allQueries.foreach(query => {
      val qResults = query.benchmark(includeBreakdown = false, messages = new ArrayBuffer[String](),
        timeout = 2 * 60 * 1000)
      assert(qResults.failure.isEmpty)
    })
    impalaKitQueries.allQueries.foreach{
      query => impalaKitQueries.sparkSession.read.
        parquet(tpcdsImpalaOutputPath + s"/${query.name}").count()
    }
  }
}
