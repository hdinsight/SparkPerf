package com.microsoft.spark.perf.core

import com.microsoft.spark.perf.{Benchmarkable, DummyWorkload, ExperimentStatus}
import com.microsoft.spark.perf.configurations.{ResourceSpecification, Variation}
import com.microsoft.spark.perf.report.ExecutionMode

class DummyCoreBenchmark(
    sizeStr: String,
    inputPartitions: Int,
    outputPartitions: Int,
    executionMode: ExecutionMode,
    inputPath: String,
    outputPath: String,
    resourceSpecification: Option[ResourceSpecification])
  extends CoreBenchmark(sizeStr, inputPartitions, outputPartitions, executionMode, "", "",
    resourceSpecification) {

  override protected[perf] lazy val allWorkloads: Seq[Benchmarkable] = (0 until 10).map(i =>
    new DummyWorkload(sparkContext))
}
