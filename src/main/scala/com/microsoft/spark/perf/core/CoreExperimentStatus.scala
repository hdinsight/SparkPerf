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

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{Failure => ScalaFailure, Success, Try}

import com.microsoft.spark.perf.{Benchmark, Benchmarkable, ExperimentStatus}
import com.microsoft.spark.perf.configurations.Variation
import com.microsoft.spark.perf.report.{BenchmarkConfiguration, ExperimentRun}

import org.apache.spark.sql.SparkSession

private[perf] class CoreExperimentStatus(
    executionsToRun: Seq[Benchmarkable],
    includeBreakdown: Boolean,
    iterations: Int,
    timeout: Long,
    variations: Seq[Variation[_]],
    resultsLocation: String,
    currentConfiguration: BenchmarkConfiguration,
    forkThread: Boolean = true,
    outputFormat: String = "json") extends ExperimentStatus(variations, resultsLocation) {

  // TODO: make it more readable
  override protected val resultsFuture = Future {
    // Run the benchmarks!
    val results: Seq[ExperimentRun] = (1 to iterations).flatMap { i =>
      combinations.map { setup =>
        val currentOptions = variations.asInstanceOf[Seq[Variation[Any]]].zip(setup).map {
          case (v, idx) =>
            v.setup(v.options(idx))
            v.name -> v.options(idx).toString
        }
        currentConfig = currentOptions.map{case (k, v) => s"$k: $v" }.mkString(", ")

        val res = executionsToRun.flatMap { q =>
          val setup = s"iteration: $i, ${currentOptions.map { case (k, v) => s"$k=$v"}.
            mkString(", ")}"
          logMessage(s"Running execution ${q.name} $setup")

          startTime = System.currentTimeMillis()

          val singleResultT = Try {
            q.benchmark(includeBreakdown, setup, currentMessages, timeout,
              forkThread = forkThread)
          }

          singleResultT match {
            case Success(singleResult) =>
              singleResult.failure.foreach { f =>
                failures += 1
                logMessage(s"Execution '${q.name}' failed: ${f.message}")
              }
              singleResult.executionTime.foreach { time =>
                logMessage(s"Execution time: ${time / 1000}s")
              }
              currentResults += singleResult
              singleResult :: Nil
            case ScalaFailure(e) =>
              failures += 1
              logMessage(s"Execution '${q.name}' failed: $e")
              Nil
          }
        }

        val result = ExperimentRun(
          timestamp = timestamp,
          iteration = i,
          configuration = currentConfiguration,
          res)
        currentRuns += result
        result
      }
    }

    try {
      val resultsTable = SparkSession.builder().getOrCreate().createDataFrame(results)
      logMessage(s"Results written to table: 'sqlPerformance' at $resultPath")
      resultsTable
        .coalesce(1)
        .write
        .format(outputFormat)
        .save(resultPath)
    } catch {
      case e: Throwable =>
        logMessage(s"Failed to write data: $e")
    }
    logCollection()
  }
}
