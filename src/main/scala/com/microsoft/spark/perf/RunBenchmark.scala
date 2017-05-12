/*
 * Copyright 2015 Databricks Inc.
 *
 * Modifications copyright (C) 2017 Microsoft
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

import java.net.URLClassLoader

import com.microsoft.spark.perf.report.ResultPipeline

/**
 * Runs a benchmark locally and prints the results to the screen.
 */
object RunBenchmark {

  private def runExperiments(name: String, args: Array[String]): List[String] = {
    val runConfig = RunConfig(name, args)
    if (runConfig.isDefined) {
      println("====STARTING EXPERIMENTS=======")
      runConfig.get.run
    } else {
      throw new Exception("failed to parse input parameter")
    }
  }

  def main(args: Array[String]): Unit = {
    // TODO: auto detect name according to package name
    // TODO: wrap this as an API
    val classLoader = ClassLoader.getSystemClassLoader.asInstanceOf[URLClassLoader]
    println(classLoader.getURLs.map(_.toString))
    val name = args(0)
    val resultPaths = runExperiments(name, args.takeRight(args.length - 1))
    val resultPipeline = new ResultPipeline("/spark/performance", "scalability")
    resultPipeline.report(resultPaths, "parquet", "parquet")
  }
}
