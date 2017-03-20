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

import com.microsoft.spark.perf.report.ExecutionMode
import com.microsoft.spark.perf.report.ExecutionMode.WriteParquet
import com.microsoft.spark.perf.sql.{Benchmark, Query}

import org.apache.spark.sql.SparkSession

class HiBenchQueries(
    tableRootPath: String,
    executionMode: ExecutionMode = ExecutionMode.ForeachResults) extends Benchmark(tableRootPath) {

  override protected def buildTables(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    spark.read.parquet(tableRootPath + "/uservisits").createOrReplaceTempView("uservisits")
    spark.read.parquet(tableRootPath + "/rankings").createOrReplaceTempView("rankings")
  }

  val queries = Seq(
    ("join",
      """
        |SELECT
        |    ip,
        |    avg(rankings) avg_rankings,
        |    sum(profit) sum_of_profit
        |FROM rankings R JOIN
        |    (SELECT
        |         ip,
        |         url,
        |         profit FROM uservisits UV
        |     WHERE (
        |         datediff(UV.date, '1999-01-01') >=0
        |             AND
        |         datediff(UV.date, '2000-01-01')<=0)
        |           ) NUV ON (R.url = NUV.url)
        |     GROUP BY ip
        |     ORDER BY sum_of_profit DESC
      """.
        stripMargin),
    ("aggregation",
      """
        |SELECT ip, SUM(profit) sum_of_profit FROM uservisits GROUP BY ip
      """.stripMargin),
    ("scan",
      """
        |SELECT * FROM uservisits
      """.stripMargin)
  ).map {
    case (name, sqlText) =>
      Query(name, sqlText, description = "original query", executionMode = {
        executionMode match {
          case WriteParquet(location) =>
            WriteParquet(location + s"/$name")
          case exeMode => exeMode
        }
      })
  }

  val queriesMap = queries.map(q => q.name -> q).toMap

  override lazy val allQueries = Seq("join", "aggregation", "scan").map(queriesMap)
}
