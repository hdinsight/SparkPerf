package com.microsoft.spark.sql.perf

import com.microsoft.spark.perf.sql.micro.DatasetPerformance
import org.scalatest.FunSuite

import org.apache.spark.sql.hive.test.TestHive

class DatasetPerformanceSuite extends FunSuite {
  ignore("run benchmark") {
    TestHive // Init HiveContext
    val benchmark = new DatasetPerformance() {
      override val numLongs = 100
    }
    import benchmark._

    val exp = runExperiment(allBenchmarks)
    exp.waitForFinish(10000)
  }
}
