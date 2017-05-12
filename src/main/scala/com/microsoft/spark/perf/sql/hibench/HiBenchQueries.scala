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

import com.microsoft.spark.perf.configurations.ResourceSpecification
import com.microsoft.spark.perf.report.ExecutionMode
import com.microsoft.spark.perf.report.ExecutionMode.WriteParquet
import com.microsoft.spark.perf.sql.{Query, SQLBenchmark}

import org.apache.spark.sql.SparkSession

class HiBenchQueries(
    tableRootPath: String,
    executionMode: ExecutionMode = ExecutionMode.ForeachResults,
    resourceSpecification: Option[ResourceSpecification])
  extends SQLBenchmark(resourceSpecification) {

  def this(tableRootPath: String, executionMode: ExecutionMode) =
    this(tableRootPath, executionMode, None)

  override private[perf] def buildTables(): Unit = {
    sparkSession.read.parquet(tableRootPath + "/uservisits").
      createOrReplaceTempView("hibench_uservisits")
    sparkSession.read.parquet(tableRootPath + "/rankings").
      createOrReplaceTempView("hibench_rankings")
  }

  private lazy val queries = Seq(
    ("join",
      """
        |SELECT
        |    ip,
        |    avg(rankings) avg_rankings,
        |    sum(profit) sum_of_profit
        |FROM hibench_rankings R JOIN
        |    (SELECT
        |         ip,
        |         url,
        |         profit FROM hibench_uservisits UV
        |     WHERE (
        |         datediff(UV.date, '1999-01-01') >=0
        |             AND
        |         datediff(UV.date, '2000-01-01')<=0)
        |           ) NUV ON (R.url = NUV.url)
        |     GROUP BY ip
        |     ORDER BY sum_of_profit DESC
      """.stripMargin),
    ("aggregation",
      """
        |SELECT ip, SUM(profit) sum_of_profit FROM hibench_uservisits GROUP BY ip
      """.stripMargin),
    ("scan",
      """
        |SELECT * FROM hibench_uservisits
      """.stripMargin)
  ).map {
    case (name, sqlText) =>
      Query(sparkSession, name, sqlText, description = "original query",
        executionMode = {
          executionMode match {
            case WriteParquet(location) =>
              WriteParquet(location + s"/$name")
            case exeMode => exeMode
          }
        })
  }

  private lazy val queriesMap = queries.map(q => q.name -> q).toMap

  override lazy val allQueries = Seq("join", "aggregation", "scan").map(queriesMap)
}
