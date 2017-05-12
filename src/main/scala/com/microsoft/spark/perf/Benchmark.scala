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

import com.microsoft.spark.perf.configurations.{ResourceSpecification, Variation}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

private[perf] abstract class Benchmark(resourceSpecification: Option[ResourceSpecification]) {

  protected[perf] var sparkContext: SparkContext = _
  protected[perf] var sparkSession: SparkSession = _

  final def start(): Benchmark = {
    sparkContext = Benchmark.createSparkContextFromResourceSpec(resourceSpecification)
    sparkSession = SparkSession.builder().config(sparkContext.getConf).getOrCreate()
    this
  }

  final def stop(): Unit = {
    sparkSession.stop()
    sparkContext.stop()
  }

  private def composeResultsLocation: String = {
    if (resourceSpecification.isDefined) {
      val resourceSpec = resourceSpecification.get
      s"/driver-${resourceSpec.driverMemory}-${resourceSpec.driverCores}-exec-" +
        s"${resourceSpec.executorMemory}-${resourceSpec.executorCores}-${resourceSpec.executorNum}"
    } else {
      ""
    }
  }

  protected lazy val resultsLocation: String =
    sparkContext.getConf.get("spark.perf.results", "/spark/performance/") +
      s"${this.getClass.getName}" + s"$composeResultsLocation"

  protected[perf] lazy val allWorkloads: Seq[Benchmarkable] = Seq()

  def createExperiment(
      executionsToRun: Seq[Benchmarkable],
      includeBreakdown: Boolean = false,
      iterations: Int = 3,
      variations: Seq[Variation[_]] = Seq(Variation("StandardRun", Seq("true")) { _ => {} }),
      timeout: Long = 0L,
      resultLocation: String = resultsLocation,
      forkThread: Boolean = true,
      reportFormat: String = "json"): ExperimentStatus

  def run(workloads: Seq[Benchmarkable], iterations: Int, reportFormat: String): String = {
    val experiment = createExperiment(workloads, iterations = iterations,
      reportFormat = reportFormat)
    experiment.waitForFinish(60 * 30 * 1000)
    experiment.resultPath
  }
}


object Benchmark {

  def createSparkContextFromResourceSpec(
      resourceSpecificationOpt: Option[ResourceSpecification]): SparkContext = {

    val conf = new SparkConf(true)
    new SparkContext(
      if (resourceSpecificationOpt.isDefined) {
        val resourceSpecification = resourceSpecificationOpt.get
        conf.
          set("spark.driver.memory", resourceSpecification.driverMemory).
          set("spark.driver.cores", resourceSpecification.driverCores.toString).
          set("spark.executor.memory", resourceSpecification.executorMemory).
          set("spark.executor.cores", resourceSpecification.executorCores.toString).
          set("spark.executor.instances", resourceSpecification.executorNum.toString)
      } else {
        conf
      }
    )
  }
}
