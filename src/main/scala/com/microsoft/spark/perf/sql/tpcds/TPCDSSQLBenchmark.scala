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

package com.microsoft.spark.perf.sql.tpcds

import com.microsoft.spark.perf.configurations.ResourceSpecification
import com.microsoft.spark.perf.sql.SQLBenchmark

class TPCDSSQLBenchmark(
    tableRootPath: String,
    resourceSpecification: Option[ResourceSpecification])
  extends SQLBenchmark(resourceSpecification) {

  override private[perf] def buildTables(): Unit = {
    val dummyTablesObj = new Tables(sparkSession.sqlContext, "", 1)
    dummyTablesObj.tables.foreach(table => {
      val df = sparkSession.read.parquet(s"$tableRootPath/${table.name}")
      df.createOrReplaceTempView(s"${table.name}")
    })
  }
}

