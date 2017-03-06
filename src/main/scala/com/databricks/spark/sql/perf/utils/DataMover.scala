package com.databricks.spark.sql.perf.utils

import com.databricks.spark.sql.perf.sql.tpcds.Tables

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
