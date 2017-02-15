package com.databricks.spark.sql.perf.runner

import org.apache.spark.sql.SparkSession


object DataGenerator {

  def main(args: Array[String]): Unit = {
    val dsdgenPath = args(0)
    val scaleFactor = args(1).toInt
    val tableLocation = args(2)

    val sparkSession = SparkSession.builder().appName("Spark SQL basic example")
      .getOrCreate()
    import com.databricks.spark.sql.perf.tpcds.Tables
    // Tables in TPC-DS benchmark used by experiments.
    // dsdgenDir is the location of dsdgen tool installed in your machines.
    val tables = new Tables(sparkSession.sqlContext, dsdgenPath, scaleFactor)
    // Generate data.
    tables.genData(tableLocation, "parquet",
      overwrite = true, partitionTables = true, useDoubleForDecimal = false,
      clusterByPartitionColumns = true, filterOutNullPartitionValues = true)

    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    tables.createExternalTables(tableLocation, "parquet", "db1", overwrite = true)

    // Setup TPC-DS experiment
    import com.databricks.spark.sql.perf.tpcds.TPCDS
    val tpcds = new TPCDS (sqlContext = sparkSession.sqlContext)

  }
}
