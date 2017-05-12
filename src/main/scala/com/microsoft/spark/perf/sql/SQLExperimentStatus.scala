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

package com.microsoft.spark.perf.sql

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.implicitConversions
import scala.util.{Failure => ScalaFailure, Success, Try}

import com.microsoft.spark.perf.{Benchmark, Benchmarkable, ExperimentStatus}
import com.microsoft.spark.perf.configurations.Variation
import com.microsoft.spark.perf.report._
import com.microsoft.spark.perf.report.cpu._
import com.microsoft.spark.perf.sql.report.SQLBenchmarkResult

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation

private[sql] class SQLExperimentStatus(
    executionsToRun: Seq[Benchmarkable],
    includeBreakdown: Boolean,
    iterations: Int,
    timeout: Long,
    variations: Seq[Variation[_]],
    resultsLocation: String,
    sqlContext: SQLContext,
    allTables: Seq[Table],
    currentConfiguration: BenchmarkConfiguration,
    forkThread: Boolean = true,
    outputFormat: String = "json") extends ExperimentStatus(variations, resultsLocation) {

  // Stats for HTML status message.
  @volatile var currentExecution = ""
  @volatile var currentPlan = "" // for queries only

  // TODO: make it more readable
  override protected val resultsFuture = Future {

    // If we're running queries, create tables for them
    executionsToRun
      .collect { case query: Query => query }
      .flatMap { query =>
        try {
          query.newDataFrame().queryExecution.logical.collect {
            case UnresolvedRelation(t, _) => t.table
          }
        } catch {
          // ignore the queries that can't be parsed
          case e: Exception => Seq()
        }
      }.distinct.foreach { name =>
      try {
        sqlContext.table(name)
        logMessage(s"Table $name exists.")
      } catch {
        case ae: Exception =>
          val table = allTables.find(_.name == name)
          if (table.isDefined) {
            logMessage(s"Creating table: $name")
            table.get.data
              .write
              .mode("overwrite")
              .saveAsTable(name)
          } else {
            // the table could be subquery
            logMessage(s"Couldn't read table $name and its not defined as a Benchmark.Table.")
          }
      }
    }

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

          currentExecution = q.name
          currentPlan = q match {
            case query: Query =>
              try {
                query.newDataFrame().queryExecution.executedPlan.toString()
              } catch {
                case e: Exception =>
                  s"failed to parse: $e"
              }
            case _ => ""
          }
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
              currentResults += singleResult.asInstanceOf[SQLBenchmarkResult]
              singleResult :: Nil
            case ScalaFailure(e) =>
              failures += 1
              logMessage(s"Execution '${q.name}' failed: ${e}")
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
      val resultsTable = sqlContext.createDataFrame(results)
      logMessage(s"Results written to table: 'sqlPerformance' at $resultPath")
      resultsTable
        .coalesce(1)
        .write
        .format(outputFormat)
        .save(resultPath)
    } catch {
      case e: Throwable => logMessage(s"Failed to write data: $e")
    }

    logCollection()
  }

  def scheduleCpuCollection(fs: FS): Unit = {
    logCollection = () => {
      logMessage(s"Begining CPU log collection")
      try {
        val location = cpu.collectLogs(sqlContext, fs, timestamp)
        logMessage(s"cpu results recorded to $location")
      } catch {
        case e: Throwable =>
          logMessage(s"Error collecting logs: $e")
          throw e
      }
    }
  }

  def cpuProfile: Profile = {
    new Profile(sqlContext, sqlContext.read.json(getCpuLocation(timestamp)))
  }

  def cpuProfileHtml(fs: FS): String = {
    s"""
       |<h1>CPU Profile</h1>
       |<b>Permalink:</b> <tt>sqlContext.read.json("${getCpuLocation(timestamp)}")</tt></br>
       |${cpuProfile.buildGraph(fs)}
         """.stripMargin
  }

  private def tail(n: Int = 20): String = {
    currentMessages.takeRight(n).mkString("\n")
  }

  private def status: String = {
    if (resultsFuture.isCompleted) {
      if (resultsFuture.value.get.isFailure) "Failed" else "Successful"
    } else {
      "Running"
    }
  }

  override def toString: String =
    s"""Permalink: table("sqlPerformance").where('timestamp === ${timestamp}L)"""


  def html: String = {
    // scalastyle:off
    val maybeQueryPlan: String =
      if (currentPlan.nonEmpty) {
        s"""
           |<h3>QueryPlan</h3>
           |<pre>
           |${currentPlan.replaceAll("\n", "<br/>")}
           |</pre>
            """.stripMargin
      } else {
        ""
      }
    s"""
       |<h2>$status Experiment</h2>
       |<b>Permalink:</b> <tt>sqlContext.read.json("$resultPath")</tt><br/>
       |<b>Iterations complete:</b> ${currentRuns.size / combinations.size} / $iterations<br/>
       |<b>Failures:</b> $failures<br/>
       |<b>Executions run:</b> ${currentResults.size} / ${iterations * combinations.size * executionsToRun.size}
       |<br/>
       |<b>Run time:</b> ${(System.currentTimeMillis() - timestamp) / 1000}s<br/>
       |
           |<h2>Current Execution: $currentExecution</h2>
       |Runtime: ${(System.currentTimeMillis() - startTime) / 1000}s<br/>
       |$currentConfig<br/>
       |$maybeQueryPlan
       |<h2>Logs</h2>
       |<pre>
       |${tail()}
       |</pre>
         """.stripMargin
    // scalastyle:on
  }
}
