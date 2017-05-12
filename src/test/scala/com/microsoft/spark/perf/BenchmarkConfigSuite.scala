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

package com.microsoft.spark.perf

import com.microsoft.spark.perf.core.CoreBenchmark
import com.microsoft.spark.perf.sql.hibench.HiBenchQueries
import com.microsoft.spark.perf.sql.tpcds.{ImpalaKitQueries, TPCDS}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import org.apache.spark.{SparkContext, SparkException}

class BenchmarkConfigSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {

  override def beforeAll(): Unit = {
    System.setProperty("spark.master", "local[*]")
    System.setProperty("spark.app.name", "test")
  }

  test("a single core benchmark is generated when we do not specify resource specification") {
    val coreConfig = CoreConfig(
      "com.microsoft.spark.perf.core.CoreBenchmark")
    val benchmarks = coreConfig.createBenchmarks.toList
    assert(benchmarks.length === 1)
    assert(benchmarks.head.isInstanceOf[CoreBenchmark])
  }

  test("multiple core benchmarks are generated when we specify resource specifications") {
    val coreConfig = CoreConfig(
      benchmarkName = "com.microsoft.spark.perf.core.CoreBenchmark",
      resourceParams = Some(ResourceParamGrid(Seq("1g", "2g"), Seq(1, 2), Seq("1g", "2g"),
        Seq(2, 4, 8), Seq(1, 2, 4))))
    val benchmarks = coreConfig.createBenchmarks.toList
    assert(benchmarks.length === 72)
    assert(benchmarks.forall(_.isInstanceOf[CoreBenchmark]))
  }

  test("a single ImpalaKitQueries benchmark is generated when we do not specify resource" +
    " specification") {
    val sqlConfig = SQLBenchmarkConfig(
      "com.microsoft.spark.perf.sql.tpcds.ImpalaKitQueries",
      "databaseName", "/tmp")
    val benchmarks = sqlConfig.createBenchmarks.toList
    assert(benchmarks.length === 1)
    assert(benchmarks.head.isInstanceOf[ImpalaKitQueries])
  }

  test("multiple ImpalaKitQueries benchmarks are generated when we specify resource" +
    " specification") {
    val sqlConfig = SQLBenchmarkConfig(
      "com.microsoft.spark.perf.sql.tpcds.ImpalaKitQueries",
      "databaseName", "/tmp",
      resourceParams = Some(ResourceParamGrid(Seq("1g", "2g"), Seq(1, 2), Seq("1g", "2g"),
        Seq(2, 4, 8), Seq(1, 2, 4))))
    val benchmarks = sqlConfig.createBenchmarks.toList
    assert(benchmarks.length === 72)
    assert(benchmarks.head.isInstanceOf[ImpalaKitQueries])
  }

  test("a single TPCDS benchmark is generated when we do not specify resource" +
    " specification") {
    val sqlConfig = SQLBenchmarkConfig(
      "com.microsoft.spark.perf.sql.tpcds.TPCDS",
      "databaseName", "/tmp")
    val benchmarks = sqlConfig.createBenchmarks.toList
    assert(benchmarks.length === 1)
    assert(benchmarks.head.isInstanceOf[TPCDS])
  }

  test("multiple TPCDS benchmarks are generated when we specify resource" +
    " specification") {
    val sqlConfig = SQLBenchmarkConfig(
      "com.microsoft.spark.perf.sql.tpcds.TPCDS",
      "databaseName", "/tmp",
      resourceParams = Some(ResourceParamGrid(Seq("1g", "2g"), Seq(1, 2), Seq("1g", "2g"),
        Seq(2, 4, 8), Seq(1, 2, 4))))
    val benchmarks = sqlConfig.createBenchmarks.toList
    assert(benchmarks.length === 72)
    assert(benchmarks.head.isInstanceOf[TPCDS])
  }

  test("a single HiBench benchmark is generated when we do not specify resource" +
    " specification") {
    val sqlConfig = SQLBenchmarkConfig(
      "com.microsoft.spark.perf.sql.hibench.HiBenchQueries",
      "databaseName", "/tmp")
    val benchmarks = sqlConfig.createBenchmarks.toList
    assert(benchmarks.length === 1)
    assert(benchmarks.head.isInstanceOf[HiBenchQueries])
  }

  test("multiple HiBench benchmarks are generated when we specify resource" +
    " specification") {
    val sqlConfig = SQLBenchmarkConfig(
      "com.microsoft.spark.perf.sql.hibench.HiBenchQueries",
      "databaseName", "/tmp",
      resourceParams = Some(ResourceParamGrid(Seq("1g", "2g"), Seq(1, 2), Seq("1g", "2g"),
        Seq(2, 4, 8), Seq(1, 2, 4))))
    val benchmarks = sqlConfig.createBenchmarks.toList
    assert(benchmarks.length === 72)
    assert(benchmarks.head.isInstanceOf[HiBenchQueries])
  }

  test("multiple SparkContext are generated correctly in SQL Benchmarks") {
    System.setProperty("spark.perf.results", "/tmp/spark")
    val sqlConfig = SQLBenchmarkConfig(
      "com.microsoft.spark.perf.sql.DummySQLBenchmark",
      "databaseName", "/tmp",
      resourceParams = Some(ResourceParamGrid(Seq("1g", "2g"), Seq(1, 2), Seq("1g"),
        Seq(2), Seq(1))))
    sqlConfig.run
  }

  test("multiple SparkContext are generated correctly in Core Benchmarks") {
    System.setProperty("spark.perf.results", "/tmp/spark")
    val coreConfig = CoreConfig(
      "com.microsoft.spark.perf.core.DummyCoreBenchmark",
      resourceParams = Some(ResourceParamGrid(Seq("1g", "2g"), Seq(1, 2), Seq("1g"),
        Seq(2), Seq(1))))
    coreConfig.run
  }

  test("cannot start a new benchmark when there is an active one") {
    SparkContext.getOrCreate()
    System.setProperty("spark.perf.results", "/tmp/spark")
    val sqlConfig = SQLBenchmarkConfig(
      "com.microsoft.spark.perf.sql.DummySQLBenchmark",
      "databaseName", "/tmp",
      resourceParams = Some(ResourceParamGrid(Seq("1g", "2g"), Seq(1, 2), Seq("1g"),
        Seq(2), Seq(1))))
    val e = intercept[SparkException] {
      sqlConfig.run
    }
    assert(e.getMessage.startsWith("Only one SparkContext may be running in this JVM"))
  }
}
