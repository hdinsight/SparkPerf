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

package com.microsoft.spark.perf.utils

import com.microsoft.spark.perf.sql.tpcds.Tables

import org.apache.spark.sql.SparkSession

object DataMover {

  def main(args: Array[String]): Unit = {
    val srcPath = args(0)
    val dstPath = args(1)
    val sparkSession = SparkSession.builder().getOrCreate()
    val dummyTableObj = new Tables(sparkSession.sqlContext, "", 1)

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.impl",
      "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key",
      args(2))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key",
      args(3))

    val tables = dummyTableObj.tables
    for (table <- tables) {
      val df = sparkSession.read.parquet(srcPath + "/" + table.name)
      df.write.partitionBy(table.partitionColumns : _*).parquet(dstPath + "/" + table.name)
    }
  }
}
