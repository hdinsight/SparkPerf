package com.microsoft.spark.perf

import scala.collection.mutable.ArrayBuffer

import com.microsoft.spark.perf.report.{BenchmarkResult, ExecutionMode}
import com.microsoft.spark.perf.sql.report.SQLBenchmarkResult

import org.apache.spark.SparkContext

class DummyWorkload(sparkContext: SparkContext) extends Benchmarkable(sparkContext) {
  override val name: String = "dummyworkload1"
  override protected val executionMode = ExecutionMode.ForeachResults

  override protected def doBenchmark(
                                      includeBreakdown: Boolean,
                                      description: String,
                                      messages: ArrayBuffer[String]): BenchmarkResult = {
    sparkContext.parallelize(List(0 until 10000)).count()
    new SQLBenchmarkResult("name", "mode")
  }
}
