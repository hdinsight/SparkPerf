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

package com.microsoft.spark.perf.sql.tpcds

import scala.collection.mutable

import com.microsoft.spark.perf.ResourceParamGrid
import com.microsoft.spark.perf.configurations.ResourceSpecification
import com.microsoft.spark.perf.report.ExecutionMode
import com.microsoft.spark.perf.report.ExecutionMode.WriteParquet
import com.microsoft.spark.perf.sql.Query

import org.apache.spark.sql.SparkSession

/**
 * TPC-DS benchmark's dataset.
 */
class TPCDS(
    tableRootPath: String,
    executionMode: ExecutionMode = ExecutionMode.ForeachResults,
    resourceSpecification: Option[ResourceSpecification])
  extends TPCDSSQLBenchmark(tableRootPath, resourceSpecification)
  with Serializable {

  def this(tableRootPath: String, executionMode: ExecutionMode) =
    this(tableRootPath, executionMode, None)

  val tpcdsQueries = new TPCDS14Queries(executionMode)

  private lazy val queries = tpcdsQueries.tpcds1_4Queries.map { case (name, sqlText) =>
    Query(sparkSession, name, sqlText, description = "TPCDS 1.4 Query",
      executionMode = {
        executionMode match {
          case WriteParquet(location) =>
            WriteParquet(location)
          case exeMode => exeMode
        }
      })
  }

  private lazy val tpcds1_4QueriesMap = queries.map(q => q.name -> q).toMap

  private lazy val runnable: Seq[Query] = Seq(
    "q1", "q2", "q3", "q4", "q5", "q7", "q8", "q9",
    "q11", "q12", "q13", "q15", "q17", "q18", "q19",
    "q20", "q21", "q22", "q25", "q26", "q27", "q28", "q29",
    "q31", "q34", "q36", "q37", "q38", "q39a", "q39b",
    "q40", "q42", "q43", "q44", "q46", "q47", "q48", "q49",
    "q50", "q51", "q52", "q53", "q54", "q55", "q57", "q59",
    "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68",
    "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79",
    "q80", "q82", "q84", "q85", "q86", "q87", "q88", "q89",
    "q90", "q91", "q93", "q96", "q97", "q98", "q99", "qSsMax").map(tpcds1_4QueriesMap)

  override lazy val allQueries: Seq[Query] = runnable
}



