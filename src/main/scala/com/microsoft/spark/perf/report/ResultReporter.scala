/*
 * Copyright 2015 Databricks Inc.
 *
 * Modifications copyright (C) 2017 Microsoft
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

package com.microsoft.spark.perf.report

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

class ResultPipeline(resultRootPath: String, reporterNames: String*) {

  private val sparkSession = SparkSession.builder().getOrCreate()

  private def preloadResultTables(benchmarkResultPaths: Seq[String], format: String): DataFrame = {
    sparkSession.read.format(format).load(benchmarkResultPaths: _*)
  }

  def report(benchmarkResultPaths: Seq[String], inputFormat: String, outputFormat: String): Unit = {
    // preload temp table
    val mergedResult = preloadResultTables(benchmarkResultPaths, inputFormat).cache()
    for (name <- reporterNames) {
      name match {
        case "scalability" =>
          new ScalabilityTestReporter(sparkSession).report(mergedResult, outputFormat,
            resultRootPath)
        case "aggregation" =>
          new AggregationTestReporter(sparkSession).report(mergedResult, outputFormat,
            resultRootPath)
      }
    }
  }
}


abstract class ResultReporter {
  def report(resultDF: DataFrame, outputFormat: String, resultRootPath: String)
}

class ScalabilityTestReporter(sparkSession: SparkSession) extends ResultReporter {

  import sparkSession.implicits._

  override def report(resultDF: DataFrame, outputFormat: String, resultRootPath: String): Unit = {
    val aggregatedResults = resultDF
      .withColumn("result", explode($"results"))
      .select("result.*", "configuration.*")
      .groupBy("name", "resourceSpecification")
      .agg(
        min($"executionTime") as 'minTimeMs,
        max($"executionTime") as 'maxTimeMs,
        avg($"executionTime") as 'avgTimeMs,
        stddev($"executionTime") as 'stdDev
      )
      .orderBy("name").cache()
    aggregatedResults.write.format(outputFormat).mode(SaveMode.Overwrite).save(resultRootPath +
      "/scalability")
  }
}

class AggregationTestReporter(sparkSession: SparkSession) extends ResultReporter {

  import sparkSession.implicits._

  override def report(resultDF: DataFrame, outputFormat: String, resultRootPath: String): Unit = {
    val aggregatedResults = resultDF
      .withColumn("result", explode($"results"))
      .select("result.*")
      .agg(
        min($"executionTime") as 'minTimeMs,
        max($"executionTime") as 'maxTimeMs,
        avg($"executionTime") as 'avgTimeMs,
        stddev($"executionTime") as 'stdDev
      )
      .orderBy("name").cache()
    aggregatedResults.write.format(outputFormat).mode(SaveMode.Overwrite).save(resultRootPath +
      "/aggregation")
  }
}
