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

package com.microsoft.spark.perf

import com.microsoft.spark.perf.configurations.ResourceSpecification
import com.microsoft.spark.perf.core.CoreBenchmark
import com.microsoft.spark.perf.report.ExecutionMode
import com.microsoft.spark.perf.sql.SQLBenchmark
import scopt.OptionParser

private[perf] abstract class RunConfig(
    benchmarkName: String,
    iterations: Int = 3,
    executionMode: String = "foreach",
    filter: Option[String] = None,
    outputReportFormat: String = "json") {

  protected def runBenchmark(benchmark: Benchmark): String = {
    val allWorkloads = filter.map {
      f => benchmark.allWorkloads.filter(_.name.equals(f))
    }.getOrElse(benchmark.allWorkloads)
    val resultPath = benchmark.run(allWorkloads, iterations, outputReportFormat)
    benchmark.stop()
    resultPath
  }

  private[perf] def createBenchmarks: Iterator[Benchmark]

  /**
   * in some workload, e.g. Spark SQL, the data sources which is created per benchmark bind itself
   * to the available SparkContext when it is created, so that we cannot create multiple
   * SparkContext/SparkSession *within* the same benchmark
   *
   * our definition here is:
   * Benchmark is a set of workloads given a certain amount of resources
   * Experiment is a set of running instances of the workloads belonging to the same benchmark, each
   * running instance corresponds to a set of runtime Spark parameters (variance)
   *
   * We also have to return the sequence of result path of all benchmarks, because DataFrame binds
   * itself with a particular SparkSession making us using a different SparkContext when collecting
   * results
   */
  def run: List[String] = {
    createBenchmarks.map(_.start()).map(runBenchmark).toList
  }
}

case class ResourceParamGrid(
    driverMemory: Seq[String], // spark.driver.memory
    driverCores: Seq[Int], // spark.driver.cores
    executorMemory: Seq[String], // spark.executor.memory
    executorCores: Seq[Int], // spark.executor.cores
    executorNum: Seq[Int] // spark.executor.instances
  ) {

  def toResourceSpecs: Seq[ResourceSpecification] = {
    for (dm <- driverMemory; dc <- driverCores; em <- executorMemory; ec <- executorCores;
         en <- executorNum) yield ResourceSpecification(dm, dc, em, ec, en)
  }
}

private[perf] case class CoreConfig(
    benchmarkName: String = null,
    iterations: Int = 3,
    filter: Option[String] = None,
    executionMode: String = "foreach",
    sizeStr: String = "0k",
    inputPartitions: Int = 0,
    outputPartitions: Int = 0,
    inputDir: Option[String] = None,
    outputDir: String = "",
    outputReportFormat: String = "json",
    resourceParams: Option[ResourceParamGrid] = None)
  extends RunConfig(benchmarkName, iterations, executionMode, filter, outputReportFormat) {

  override private[perf] def createBenchmarks: Iterator[Benchmark] = {
    val benchmarkConstructor = Class.forName(benchmarkName).getConstructor(classOf[String],
      classOf[Int], classOf[Int], classOf[ExecutionMode], classOf[String],
      classOf[String], classOf[Option[ResourceSpecification]])
    if (resourceParams.isDefined) {
      resourceParams.get.toResourceSpecs.iterator.map {
        spec =>
          benchmarkConstructor.newInstance(
            sizeStr, Int.box(inputPartitions), Int.box(outputPartitions), {
              executionMode match {
                case "foreach" => ExecutionMode.ForeachResults
                case "outputTextFile" => ExecutionMode.WriteTextFile(outputDir)
              }
            }, inputDir.getOrElse(""), outputDir, Some(spec)).asInstanceOf[CoreBenchmark]
      }
    } else {
      Iterator(benchmarkConstructor.newInstance(
        sizeStr, Int.box(inputPartitions), Int.box(outputPartitions), {
          executionMode match {
            case "foreach" => ExecutionMode.ForeachResults
            case "outputTextFile" => ExecutionMode.WriteTextFile(outputDir)
          }
        }, inputDir.getOrElse(""), outputDir, None).asInstanceOf[CoreBenchmark])
    }
  }
}

private[perf] object CoreConfig {
  def getParser: OptionParser[CoreConfig] = {
    new scopt.OptionParser[CoreConfig]("spark-benchmark") {
      head("spark-benchmark", "0.1.0")
      opt[String]('b', "benchmark")
        .action { (x, c) => c.copy(benchmarkName = x) }
        .text("the name of the benchmark to run")
        .required()
      opt[String]('f', "filter")
        .action((x, c) => c.copy(filter = Some(x)))
        .text("a filter on the name of the queries to run")
      opt[Int]('i', "iterations")
        .action((x, c) => c.copy(iterations = x))
        .text("the number of iterations to run")
      opt[String]("executionMode")
        .action((x, c) => c.copy(executionMode = x))
        .text("execution mode of queries")
      opt[String]("inputDir")
        .action((x, c) => c.copy(inputDir = Some(x)))
        .text("input directory for TeraSort")
      opt[String]("outputDir")
        .action((x, c) => c.copy(outputDir = x))
        .text("output directory")
      opt[String]("reportFormat")
        .action((x, c) => c.copy(outputReportFormat = x))
        .text("output report format")
      opt[String]("size")
        .action((x, c) => c.copy(sizeStr = x))
        .text("size of the test data")
      opt[Int]("inputPartitions")
        .action((x, c) => c.copy(inputPartitions = x))
        .text("number of input partitions")
        .required()
      opt[Int]("outputPartitions")
        .action((x, c) => c.copy(outputPartitions = x))
        .text("number of output partitions")
      opt[String]("resourceParams")
        .action((x, c) => {
          // parsing resourceAllocationString
          val Array(driverMemoryStr, driverCoresStr, executorMemoryStr, executorCoresStr,
            executorNumStr) = x.split(";")
          val driverMemorySeq = driverMemoryStr.split(",").toSeq
          val driverCoresSeq = driverCoresStr.split(",").toSeq.map(_.toInt)
          val executorMemorySeq = executorMemoryStr.split(",").toSeq
          val executorCoresSeq = executorCoresStr.split(",").toSeq.map(_.toInt)
          val executorNumSeq = executorNumStr.split(",").toSeq.map(_.toInt)
          c.copy(resourceParams = Some(ResourceParamGrid(driverMemorySeq,
            driverCoresSeq, executorMemorySeq, executorCoresSeq, executorNumSeq)))
        })
        .text("resourceSpecification")
      help("help")
        .text("prints this usage text")
    }
  }
}

private[perf] case class SQLBenchmarkConfig (
    benchmarkName: String = null,
    databaseName: String = null,
    databasePath: String = null,
    filter: Option[String] = None,
    iterations: Int = 3,
    baseline: Option[Long] = None,
    executionMode: String = "foreach",
    outputDir: String = "",
    outputReportFormat: String = "json",
    resourceParams: Option[ResourceParamGrid] = None)
  extends RunConfig(benchmarkName, iterations, executionMode, filter, outputReportFormat) {

  override private[perf] def createBenchmarks: Iterator[Benchmark] = {
    val benchmarkConstructor = Class.forName(benchmarkName).getConstructor(classOf[String],
      classOf[ExecutionMode], classOf[Option[ResourceSpecification]])
    if (resourceParams.isDefined) {
      resourceParams.get.toResourceSpecs.iterator.map {
        spec =>
          benchmarkConstructor.newInstance(
            databasePath, {
              executionMode match {
                case "foreach" => ExecutionMode.ForeachResults
                case "collect" => ExecutionMode.CollectResults
                case "parquet" =>
                  ExecutionMode.WriteParquet(outputDir)
              }
            }, Some(spec)).asInstanceOf[SQLBenchmark]
      }
    } else {
      Iterator(benchmarkConstructor.newInstance(
        databasePath, {
          executionMode match {
            case "foreach" => ExecutionMode.ForeachResults
            case "collect" => ExecutionMode.CollectResults
            case "parquet" =>
              ExecutionMode.WriteParquet(outputDir)
          }
        }, None).asInstanceOf[SQLBenchmark])
    }
  }
}

private[perf] object SQLBenchmarkConfig {
  def getParser: OptionParser[SQLBenchmarkConfig] = {
    new scopt.OptionParser[SQLBenchmarkConfig]("spark-benchmark") {
      head("spark-benchmark", "0.1.0")
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
      opt[String]("executionMode")
        .action((x, c) => c.copy(executionMode = x))
        .text("execution mode of queries")
      opt[String]("outputDir")
        .action((x, c) => c.copy(outputDir = x))
        .text("output directory")
      opt[String]("reportFormat")
        .action((x, c) => c.copy(outputReportFormat = x))
        .text("output report format")
      opt[String]("resourceParams")
        .action((x, c) => {
          // parsing resourceAllocationString
          val Array(driverMemoryStr, driverCoresStr, executorMemoryStr, executorCoresStr,
          executorNumStr) = x.split(";")
          val driverMemorySeq = driverMemoryStr.split(",").toSeq
          val driverCoresSeq = driverCoresStr.split(",").toSeq.map(_.toInt)
          val executorMemorySeq = executorMemoryStr.split(",").toSeq
          val executorCoresSeq = executorCoresStr.split(",").toSeq.map(_.toInt)
          val executorNumSeq = executorNumStr.split(",").toSeq.map(_.toInt)
          c.copy(resourceParams = Some(ResourceParamGrid(driverMemorySeq,
            driverCoresSeq, executorMemorySeq, executorCoresSeq, executorNumSeq)))
        })
        .text("resourceSpecification")
      help("help")
        .text("prints this usage text")
    }
  }
}

private[perf] object RunConfig {
  def apply(name: String, args: Array[String]): Option[RunConfig] = name.toLowerCase match {
    case "sql" =>
      val parser = SQLBenchmarkConfig.getParser
      parser.parse(args, SQLBenchmarkConfig())
    case "core" =>
      val parser = CoreConfig.getParser
      parser.parse(args, CoreConfig())
    case x =>
      System.err.println(s"unrecognized $x")
      None
  }
}
