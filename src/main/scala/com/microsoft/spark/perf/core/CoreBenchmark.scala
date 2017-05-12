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

import scala.util.Try

import com.microsoft.spark.perf.{Benchmark, Benchmarkable, ExperimentStatus, ResourceParamGrid}
import com.microsoft.spark.perf.configurations.{ResourceSpecification, Variation}
import com.microsoft.spark.perf.report.{BenchmarkConfiguration, ExecutionMode}

class CoreBenchmark (
    sizeStr: String,
    inputPartitions: Int,
    outputPartitions: Int,
    executionMode: ExecutionMode,
    inputPath: String,
    outputPath: String,
    resourceSpecification: Option[ResourceSpecification])
  extends Benchmark(resourceSpecification) {

  def this(sizeStr: String, inputPartitions: Int, outputPartitions: Int,
           executionMode: ExecutionMode) =
    this(sizeStr, inputPartitions, outputPartitions, executionMode, "", "", None)

  override protected[perf] lazy val allWorkloads: Seq[Benchmarkable] = Seq(
    new OnFlyTeraSort(sparkContext, sizeStr, inputPartitions, outputPartitions, executionMode),
    new TeraSort(sparkContext, inputPath, outputPath, inputPartitions, outputPartitions,
      executionMode)
  )

  private val buildInfo = Try(getClass.getClassLoader.loadClass("org.apache.spark.BuildInfo")).
    map { cls =>
      cls.getMethods
        .filter(_.getReturnType == classOf[String])
        .filterNot(_.getName == "toString")
        .map(m => m.getName -> m.invoke(cls).asInstanceOf[String])
        .toMap
    }.getOrElse(Map.empty)

  /**
   * Starts an experiment run with a given set of executions to run.
   *
   * @param executionsToRun a list of executions to run.
   * @param includeBreakdown If it is true, breakdown results of an execution will be recorded.
   *                         Setting it to true may significantly increase the time used to
   *                         run an execution.
   * @param iterations The number of iterations to run of each execution.
   * @param variations [[Variation]]s used in this run.  The cross product of all variations will be
   *                   run for each execution * iteration.
   * @param timeout wait at most timeout milliseconds for each query, 0 means wait forever
   * @return It returns a ExperimentStatus object that can be used to
   *         track the progress of this experiment run.
   */
  override def createExperiment(
      executionsToRun: Seq[Benchmarkable],
      includeBreakdown: Boolean = false,
      iterations: Int = 3,
      variations: Seq[Variation[_]] = Seq(Variation("StandardRun", Seq("true")) { _ => {} }),
      timeout: Long = 0L,
      resultLocation: String = resultsLocation,
      forkThread: Boolean = true,
      reportFormat: String = "json"): ExperimentStatus = {

    new CoreExperimentStatus(
      executionsToRun, includeBreakdown, iterations,
      timeout, variations, resultLocation,
      BenchmarkConfiguration(sparkConf = sparkContext.getConf.getAll.toMap,
        defaultParallelism = sparkContext.defaultParallelism, buildInfo = buildInfo,
        resourceSpecification = resourceSpecification),
      forkThread, reportFormat)
  }
}
