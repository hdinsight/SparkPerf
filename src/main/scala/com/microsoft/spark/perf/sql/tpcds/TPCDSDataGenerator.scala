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

import org.apache.spark.sql.{SparkSession, SQLContext}

object TPCDSDataGenerator {

  private[tpcds] def dataGen(
      sqlContext: SQLContext, dsdgenPath: String, tableLocation: String,
      scaleFactor: Int, numPartitions: Int): Unit = {
    val tables = new Tables(sqlContext, dsdgenPath, scaleFactor)
    tables.genData(tableLocation, "parquet",
      overwrite = true, partitionTables = true, useDoubleForDecimal = false,
      clusterByPartitionColumns = true, filterOutNullPartitionValues = true,
      numPartitions = numPartitions)
  }

  def main(args: Array[String]): Unit = {
    val dsdgenPath = args(0)
    val scaleFactor = args(1).toInt
    val tableLocation = args(2)
    val genData = args(3).toBoolean
    val numPartitions = args(4).toInt

    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      // .config("spark.sql.warehouse.dir", tableLocation)
      // .enableHiveSupport()
      .getOrCreate()

    if (genData) {
      dataGen(sparkSession.sqlContext, dsdgenPath, tableLocation, scaleFactor, numPartitions)
    }
  }
}
