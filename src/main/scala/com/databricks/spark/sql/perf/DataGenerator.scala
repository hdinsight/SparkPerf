package com.databricks.spark.sql.perf

import org.apache.spark.sql.SparkSession


object DataGenerator {

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
    import com.databricks.spark.sql.perf.queries.tpcds.Tables
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
