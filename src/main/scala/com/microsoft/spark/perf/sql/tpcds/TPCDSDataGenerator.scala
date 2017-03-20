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

import org.apache.spark.sql.SparkSession

object TPCDSDataGenerator {

  def main(args: Array[String]): Unit = {
    val dsdgenPath = args(0)
    val scaleFactor = args(1).toInt
    val tableLocation = args(2)
    val genData = args(3).toBoolean

    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      // .config("spark.sql.warehouse.dir", tableLocation)
      // .enableHiveSupport()
      .getOrCreate()

    if (args.length > 4) {
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem")
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key",
        args(4))
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key",
        args(5))
    }
    val tables = new Tables(sparkSession.sqlContext, dsdgenPath, scaleFactor)

    if (genData) {
      tables.genData(tableLocation, "parquet",
        overwrite = true, partitionTables = true, useDoubleForDecimal = false,
        clusterByPartitionColumns = true, filterOutNullPartitionValues = true)
    }

    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    tables.createExternalTables(tableLocation, "parquet", "db1", overwrite = true)
  }
}
