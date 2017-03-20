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

package com.microsoft.spark.perf

import java.net.InetAddress

import com.microsoft.spark.perf.report.ExecutionMode
import com.microsoft.spark.perf.sql.Benchmark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class RunConfig(
    benchmarkName: String = null,
    databaseName: String = null,
    databasePath: String = null,
    filter: Option[String] = None,
    iterations: Int = 3,
    baseline: Option[Long] = None,
    executionMode: String = "foreach",
    outputDir: String = "",
    outputReportFormat: String = "json",
    s3AccessKey: String = null,
    s3SecretKey: String = null)

/**
 * Runs a benchmark locally and prints the results to the screen.
 */
object RunBenchmark {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RunConfig]("spark-sql-perf") {
      head("spark-sql-perf", "0.2.0")
      opt[String]('b', "benchmark")
        .action { (x, c) => c.copy(benchmarkName = x) }
        .text("the name of the benchmark to run")
        .required()
      opt[String]('d', "database")
        .action { (x, c) => c.copy(databaseName = x) }
        .text("the name of database")
        .required()
      opt[String]('p', "path")
        .action { (x, c) => c.copy(databasePath = x) }
        .text("the path of database path")
        .required()
      opt[String]('f', "filter")
        .action((x, c) => c.copy(filter = Some(x)))
        .text("a filter on the name of the queries to run")
      opt[Int]('i', "iterations")
        .action((x, c) => c.copy(iterations = x))
        .text("the number of iterations to run")
      opt[Long]('c', "compare")
          .action((x, c) => c.copy(baseline = Some(x)))
          .text("the timestamp of the baseline experiment to compare with")
      opt[String]("s3AccessKey")
        .action((x, c) => c.copy(s3AccessKey = x))
        .text("s3 access key")
      opt[String]("executionMode")
        .action((x, c) => c.copy(executionMode = x))
        .text("execution mode of queries")
      opt[String]("outputDir")
        .action((x, c) => c.copy(outputDir = x))
        .text("output directory")
      opt[String]("reportFormat")
        .action((x, c) => c.copy(outputReportFormat = x))
        .text("output report format")
      opt[String]("s3SecreteKey")
        .action((x, c) => c.copy(s3SecretKey = x))
        .text("s3 secrete key")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, RunConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: RunConfig): Unit = {
    val sparkSession = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", config.databasePath)
      .getOrCreate()
    import sparkSession.implicits._

    if (config.s3AccessKey != null) {
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem")
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3.awsAccessKeyId",
        config.s3AccessKey)
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3.awsSecretAccessKey",
        config.s3SecretKey)
    }

    val benchmark = Class.forName(config.benchmarkName).getConstructor(classOf[String],
      classOf[ExecutionMode]).newInstance(config.databasePath, {config.executionMode match {
        case "foreach" => ExecutionMode.ForeachResults
        case "collect" => ExecutionMode.CollectResults
        case "parquet" =>
          ExecutionMode.WriteParquet(config.outputDir)
      }}).asInstanceOf[Benchmark]

    val allQueries = config.filter.map { f =>
      benchmark.allQueries.filter(_.name contains f)
    } getOrElse {
      benchmark.allQueries
    }

    println("== QUERY LIST ==")
    allQueries.foreach(println)

    val experiment = benchmark.runExperiment(
      executionsToRun = allQueries,
      iterations = config.iterations,
      tags = Map(
        "runtype" -> "local",
        "host" -> InetAddress.getLocalHost().getHostName()),
      reportFormat = config.outputReportFormat)

    println("== STARTING EXPERIMENT ==")
    experiment.waitForFinish(1000 * 60 * 30)

    sparkSession.sqlContext.setConf("spark.sql.shuffle.partitions", "1")
    val groupedResults = experiment.getCurrentRuns()
      .withColumn("result", explode($"results"))
      .select("result.*")
      .groupBy("name")
    groupedResults.agg(
      min($"executionTime") as 'minTimeMs,
      max($"executionTime") as 'maxTimeMs,
      avg($"executionTime") as 'avgTimeMs,
      stddev($"executionTime") as 'stdDev)
      .orderBy("name")
      .show(100, truncate = false)
    println(s"""Results: sqlContext.read.json/parquet("${experiment.resultPath}")""")

    config.baseline.foreach { baseTimestamp =>
      val baselineTime = when($"timestamp" === baseTimestamp, $"executionTime").otherwise(null)
      val thisRunTime = when($"timestamp" === experiment.timestamp, $"executionTime").
        otherwise(null)

      val data = sparkSession.read.json(benchmark.resultsLocation)
          .coalesce(1)
          .where(s"timestamp IN ($baseTimestamp, ${experiment.timestamp})")
          .withColumn("result", explode($"results"))
          .select("timestamp", "result.*")
          .groupBy("name")
          .agg(
            avg(baselineTime) as 'baselineTimeMs,
            avg(thisRunTime) as 'thisRunTimeMs,
            stddev(baselineTime) as 'stddev)
          .withColumn(
            "percentChange", ($"baselineTimeMs" - $"thisRunTimeMs") / $"baselineTimeMs" * 100)
          .filter('thisRunTimeMs.isNotNull)

      data.show(100, truncate = false)
    }
  }
}
