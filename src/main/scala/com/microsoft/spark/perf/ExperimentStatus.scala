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

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import com.microsoft.spark.perf.configurations.Variation
import com.microsoft.spark.perf.report.{BenchmarkResult, ExperimentRun}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

private[perf] abstract class ExperimentStatus(
    variations: Seq[Variation[_]],
    resultsLocation: String) extends Serializable {

  protected val currentResults = new collection.mutable.ArrayBuffer[BenchmarkResult]()
  protected val currentRuns = new collection.mutable.ArrayBuffer[ExperimentRun]()
  @volatile protected var currentConfig = ""

  protected val currentMessages = new collection.mutable.ArrayBuffer[String]()

  /** An optional log collection task that will run after the experiment. */
  @volatile protected var logCollection: () => Unit = () => {}

  @volatile protected var failures = 0
  @volatile protected var startTime = 0L

  protected def logMessage(msg: String) = {
    currentMessages += msg
  }

  private def cartesianProduct[T](xss: List[List[T]]): List[List[T]] = xss match {
    case Nil => List(Nil)
    case h :: t => for (xh <- h; xt <- cartesianProduct(t)) yield xh :: xt
  }

  protected val resultsFuture: Future[Unit] = null

  protected val combinations = cartesianProduct(
    variations.map(l => l.options.indices.toList).toList)

  protected val timestamp = System.currentTimeMillis()

  val resultPath = s"$resultsLocation/timestamp=$timestamp"

  def getCurrentRuns: DataFrame = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val tbl = sparkSession.createDataFrame(currentRuns)
    tbl.createOrReplaceTempView("currentRuns")
    tbl
  }

  /**
   * Waits for the finish of the experiment.
   */
  def waitForFinish(timeoutInSeconds: Int): Unit = {
    Await.result(resultsFuture, timeoutInSeconds.seconds)
  }
}
