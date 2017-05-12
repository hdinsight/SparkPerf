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

package com.microsoft.spark.perf.sql.hibench

import java.io.File

import scala.collection.mutable.ArrayBuffer

import com.microsoft.spark.perf.report.ExecutionMode
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class HiBenchSQLSuite extends FunSuite with BeforeAndAfterAll {

  private val numURLPerPartition = 10000
  private val conf: SparkConf = new SparkConf()
  private val tempPath: String = System.getProperty("java.io.tmpdir") +
    s"/hibench-${System.currentTimeMillis()}"
  private var urlRDD: RDD[(Long, String)] = _
  private var urlRankingsRDD: RDD[(String, Long)] = _
  private var userVisitsRDD: RDD[UserVisitRecord] = _

  private var hiBenchQueries: HiBenchQueries = _

  private var userRankingCount = 0L
  private var userVisitCount = 0L


  override def beforeAll(): Unit = {
    val sparkSession = SparkSession.builder().config(
      new SparkConf().setAppName("SparkBenchmarkTest").setMaster("local[*]")).getOrCreate()
    System.setProperty("spark.master", "local[*]")
    System.setProperty("spark.app.name", "test")
    sparkSession.sparkContext.setLogLevel("INFO")

    val numPartitionsRanking = Runtime.getRuntime.availableProcessors()
    val numPartitionsVisits = Runtime.getRuntime.availableProcessors()
    conf.set("numPartitionsRanking", numPartitionsRanking.toString)
    conf.set("numPartitionsVisits", numPartitionsVisits.toString)
    conf.set("pages", s"${numPartitionsRanking * numURLPerPartition}")
    conf.set("slotpages", numURLPerPartition.toString)
    conf.set("outputPath", tempPath)

    urlRDD = HiBenchDataGenerator.generateUrlRDD(conf, sparkSession, numPartitionsRanking)
    urlRankingsRDD = HiBenchDataGenerator.generateRankingRDD(conf, sparkSession, urlRDD).cache()
    userVisitsRDD = HiBenchDataGenerator.generateUserVisits(conf, sparkSession, urlRankingsRDD)
    userRankingCount = urlRankingsRDD.count()
    userVisitCount = userVisitsRDD.count()

    HiBenchDataGenerator.outputRankingFiles(conf, sparkSession, urlRankingsRDD)
    HiBenchDataGenerator.outputUsersVisitsFiles(conf, sparkSession, userVisitsRDD)
    sparkSession.stop()
    hiBenchQueries = new HiBenchQueries(tempPath,
      ExecutionMode.WriteParquet(tempPath + "/output"))
    hiBenchQueries.start()
    hiBenchQueries.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    deleteFolder(new File(tempPath))
    hiBenchQueries.stop()
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

  test("data is generated correctly") {
    val totalInLinks = hiBenchQueries.sparkSession.read.parquet(s"$tempPath/rankings").
      toDF("url", "inLinks").agg(sum(col("inLinks"))).first().getAs[Long](0)
    assert(hiBenchQueries.sparkSession.read.parquet(s"$tempPath/uservisits").count() ===
      totalInLinks)
    // we have to do this
    hiBenchQueries.buildTables()
  }

  test("scan executed correctly") {
    val sparkSession = hiBenchQueries.sparkSession
    val scanQuery = hiBenchQueries.allQueries.filter(_.name == "scan")
    scanQuery.head.benchmark(includeBreakdown = false, messages = new ArrayBuffer[String](),
      timeout = 2 * 60 * 1000)
    assert(sparkSession.read.parquet(s"$tempPath/output/scan").count() === userVisitCount)
  }

  test("join executed correctly") {
    val sparkSession = hiBenchQueries.sparkSession
    val joinQuery = hiBenchQueries.allQueries.filter(_.name == "join")
    joinQuery.head.benchmark(
      includeBreakdown = false, messages = new ArrayBuffer[String](),
      timeout = 2 * 60 * 1000)
    val count = sparkSession.read.parquet(s"$tempPath/output/join").count()
    assert(count > numURLPerPartition)
  }

  test("aggregation executed correctly") {
    val sparkSession = hiBenchQueries.sparkSession
    val joinQuery = hiBenchQueries.allQueries.filter(_.name == "aggregation")
    joinQuery.head.benchmark(
      includeBreakdown = false, messages = new ArrayBuffer[String](),
      timeout = 2 * 60 * 1000)
    val count = sparkSession.read.parquet(s"$tempPath/output/aggregation").count()
    assert(count > numURLPerPartition * Runtime.getRuntime.availableProcessors() * 10)
  }
}
