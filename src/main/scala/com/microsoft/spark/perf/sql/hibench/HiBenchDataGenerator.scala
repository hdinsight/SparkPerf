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

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object HiBenchDataGenerator {

  private def generateKeyToURL(lowBound: Long, upperBound: Long, generator: HtmlCore):
      Iterator[(Long, String)] = {
    val hashMap = new mutable.HashMap[Long, String]
    for (i <- lowBound until upperBound) {
      val urlLength = generator.nextUrlLength()
      val charArray = new Array[Char](urlLength)
      for (j <- 0 until urlLength) {
        charArray(j) = (97 + Random.nextInt(26)).asInstanceOf[Char]
      }
      val url = new String(charArray)
      hashMap += i -> url
    }
    hashMap.iterator
  }

  private def generateKeyToOutputLinks(lowBound: Long, upperBound: Long, generator: HtmlCore):
      Iterator[(Long, Long)] = {
    val hashMap = new mutable.HashMap[Long, Long]
    // generate page range
    for (i <- lowBound until upperBound) {
      val linkIds = generator.genPureLinkIds()
      for (j <- 0 until linkIds.length) {
        val urlId = linkIds(j)
        val currentInLinks = hashMap.getOrElseUpdate(urlId, 0) + 1
        hashMap += urlId -> currentInLinks
      }
    }
    hashMap.iterator
  }

  private[hibench] def generateRankingRDD(
      conf: SparkConf,
      sparkSession: SparkSession,
      urlRDD: RDD[(Long, String)]): RDD[(String, Long)] = {
    val inLinksRDD = sparkSession.sparkContext.parallelize(0 until urlRDD.getNumPartitions,
      urlRDD.getNumPartitions).mapPartitions {
      _ =>
        val htmlGenerator = new HtmlCore(conf)
        htmlGenerator.fireRandom(System.currentTimeMillis().toInt)
        val Array(lowBound, upBound) = HtmlCore.getPageRange(TaskContext.getPartitionId(),
          conf.getInt("pages", 0), conf.getInt("slotpages", 0))
        val keyToOutputLinks = generateKeyToOutputLinks(lowBound, upBound, htmlGenerator)
        keyToOutputLinks
    }.reduceByKey(_ + _)
    urlRDD.join(inLinksRDD).map(_._2)
  }

  private[hibench] def outputRankingFiles(
      conf: SparkConf,
      sparkSession: SparkSession,
      rankingRDD: RDD[(String, Long)]): Unit = {
    import sparkSession.implicits._
    val rankingDF = rankingRDD.toDF("url", "rankings")
    rankingDF.write.mode(SaveMode.Overwrite).parquet(conf.get("outputPath") + "/rankings")
  }


  private[hibench] def generateUserVisits(
      conf: SparkConf,
      sparkSession: SparkSession,
      rankingRDD: RDD[(String, Long)]): RDD[UserVisitRecord] = {
    rankingRDD.repartition(conf.getInt("numPartitionsVisits", 100)).mapPartitions {
      urlAndInLinks =>
        if (urlAndInLinks.isEmpty) {
          new ListBuffer[UserVisitRecord].iterator
        } else {
          val userVisitsGenerator = new Visit(",")
          userVisitsGenerator.fireRandom(System.currentTimeMillis().toInt)
          urlAndInLinks.flatMap {
            case (url, inLinks) =>
              val urlVisitPairs = new ListBuffer[UserVisitRecord]
              // TODO: avoid this long -> int conversion
              for (i <- 0 until inLinks.toInt) {
                urlVisitPairs += userVisitsGenerator.nextAccess(url)
              }
              urlVisitPairs
          }
        }
    }
  }

  private[hibench] def outputUsersVisitsFiles(
      conf: SparkConf,
      sparkSession: SparkSession,
      userVisitsRDD: RDD[UserVisitRecord]): Unit = {
    import sparkSession.implicits._
    userVisitsRDD.toDF.write.mode(SaveMode.Overwrite).parquet(conf.get("outputPath") +
      "/uservisits")
  }

  private[hibench] def generateUrlRDD(
      sparkConf: SparkConf,
      sparkSession: SparkSession,
      numPartitionsRanking: Int) = {
    // generate random URLs
    sparkSession.sparkContext.parallelize(0 until numPartitionsRanking,
      numPartitionsRanking).mapPartitions { _ =>
      val htmlGenerator = new HtmlCore(sparkConf)
      htmlGenerator.fireRandom(System.currentTimeMillis().toInt)
      val Array(lowBound, upBound) = HtmlCore.getPageRange(TaskContext.getPartitionId(),
        sparkConf.getInt("pages", 0), sparkConf.getInt("slotpages", 0))
      val keyToURLItr = generateKeyToURL(lowBound, upBound, htmlGenerator)
      keyToURLItr
    }
  }

  def main(args: Array[String]): Unit = {

    val numPartitionsRanking = args(0).toInt
    val numPartitionsVisits = args(1).toInt

    val conf = new SparkConf
    conf.set("numPartitionsRanking", numPartitionsRanking.toString)
    conf.set("numPartitionsVisits", numPartitionsVisits.toString)
    conf.set("pages", args(2))
    conf.set("slotpages", args(3))
    conf.set("outputPath", args(4))

    val sparkSession = SparkSession.builder().getOrCreate()


    val urlRDD = generateUrlRDD(conf, sparkSession, numPartitionsRanking)

    // ranking data
    val urlRankingsRDD = generateRankingRDD(conf, sparkSession, urlRDD).cache()
    outputRankingFiles(conf, sparkSession, urlRankingsRDD)

    // user visits data
    val urlVisitsRDD = generateUserVisits(conf, sparkSession, urlRankingsRDD)
    outputUsersVisitsFiles(conf, sparkSession, urlVisitsRDD)
  }
}
