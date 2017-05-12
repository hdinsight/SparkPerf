package com.microsoft.spark.perf.sql

import scala.collection.mutable.ArrayBuffer

import com.microsoft.spark.perf.{Benchmarkable, DummyWorkload}
import com.microsoft.spark.perf.configurations.ResourceSpecification
import com.microsoft.spark.perf.report.{BenchmarkResult, ExecutionMode}
import com.microsoft.spark.perf.sql.report.SQLBenchmarkResult

class DummySQLBenchmark(
    tableRootPath: String,
    executionMode: ExecutionMode = ExecutionMode.ForeachResults,
    resourceSpecification: Option[ResourceSpecification])
  extends SQLBenchmark(resourceSpecification) {

  override private[perf] def buildTables(): Unit = {}

  override protected[perf] lazy val allWorkloads: Seq[Benchmarkable] = (0 until 10).map(i =>
    new DummyWorkload(sparkContext))
}
