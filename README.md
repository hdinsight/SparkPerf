# Modified Version Tutorial - CodingCat

0.	Install necessary tools
sudo apt-get install bison flex build-essential

echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list

sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823

sudo apt-get update

sudo apt-get install sbt

1.	git clone https://github.com/CodingCat/spark-benchmark.git
2.	Run the following command:
cd spark-benchmark; sbt assembly; cd ..
3.	Git clone https://github.com/davies/tpcds-kit
4.	Run the following command: (Note: if you have to copy this directory to all worker nodes (script action failed for unknown reason))
cd tpcds-kit/tools; mv Makefile.suite Makefile; make; cd -
5.	Run the following command:
cd spark-sql-perf; 
spark-submit --class com.databricks.spark.sql.perf.sql.tpcds.TPCDSDataGenerator --executor-memory 16g target/scala-2.11/spark-benchmark-assembly-0.4.11-SNAPSHOT.jar ~/tpcds-kit/tools size_of_data_in_GB location_of_generated_data blank_or_access_key blank_or_secret_key
6.  Run benchmark

spark-submit --master yarn-client --class com.databricks.spark.sql.perf.RunBenchmark --driver-memory 16g --executor-memory 16g --executor-cores 8 --num-executors 4  target/scala-2.11/spark-sql-perf-assembly-0.4.11-SNAPSHOT.jar --benchmark com.databricks.spark.sql.perf.queries.tpcds.ImpalaKitQueries --database db1 --path /tablebucket/ --executionMode parquet -i 10 --outputDir /outputresults/



## NOTE
1. if you are not using S3 to store generated data, you do not need the last two parameters in step 6
2. to use multiple machines to generate data, you have to do 3, 4 in every machine


# Spark SQL Performance Tests

[![Build Status](https://travis-ci.org/databricks/spark-sql-perf.svg)](https://travis-ci.org/databricks/spark-sql-perf)

This is a performance testing framework for [Spark SQL](https://spark.apache.org/sql/) in [Apache Spark](https://spark.apache.org/) 1.6+.

**Note: This README is still under development. Please also check our source code for more information.**

# Quick Start

```
$ bin/run --help

spark-sql-perf 0.2.0
Usage: spark-sql-perf [options]

  -b <value> | --benchmark <value>
        the name of the benchmark to run
  -f <value> | --filter <value>
        a filter on the name of the queries to run
  -i <value> | --iterations <value>
        the number of iterations to run
  --help
        prints this usage text
        
$ bin/run --benchmark DatasetPerformance
```

# TPC-DS

## How to use it
The rest of document will use TPC-DS benchmark as an example. We will add contents to explain how to use other benchmarks add the support of a new benchmark dataset in future.

### Setup a benchmark
Before running any query, a dataset needs to be setup by creating a `Benchmark` object. Generating
the TPCDS data requires dsdgen built and available on the machines. We have a fork of dsdgen that
you will need. It can be found [here](https://github.com/davies/tpcds-kit).  

```
import com.databricks.spark.sql.perf.benchmarks.tpcds.Tables
// Tables in TPC-DS benchmark used by experiments.
// dsdgenDir is the location of dsdgen tool installed in your machines.
val tables = new Tables(sqlContext, dsdgenDir, scaleFactor)
// Generate data.
tables.genData(location, format, overwrite, partitionTables, useDoubleForDecimal, clusterByPartitionColumns, filterOutNullPartitionValues)
// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.
tables.createExternalTables(location, format, databaseName, overwrite)
// Or, if you want to create temporary tables
tables.createTemporaryTables(location, format)
// Setup TPC-DS experiment
import com.databricks.spark.sql.perf.benchmarks.tpcds.TPCDS
val tpcds = new TPCDS (sqlContext = sqlContext)
```

### Run benchmarking queries
After setup, users can use `runExperiment` function to run benchmarking queries and record query execution time. Taking TPC-DS as an example, you can start an experiment by using

```
val experiment = tpcds.runExperiment(tpcds.interactiveQueries)
```

For every experiment run (i.e. every call of `runExperiment`), Spark SQL Perf will use the timestamp of the start time to identify this experiment. Performance results will be stored in the sub-dir named by the timestamp in the given `resultsLocation` (for example `results/1429213883272`). The performance results are stored in the JSON format.

### Retrieve results
While the experiment is running you can use `experiment.html` to list the status.  Once the experiment is complete, the results will be saved to the table sqlPerformance in json.

```
// Get all experiments results.
tpcds.createResultsTable()
sqlContext.table("sqlPerformance")
// Get the result of a particular run by specifying the timestamp of that run.
sqlContext.table("sqlPerformance").filter("timestamp = 1429132621024")
```
