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

package com.microsoft.spark.perf.report

import com.microsoft.spark.perf.configurations.ResourceSpecification

/**
 * The performance results of all given queries for a single iteration.
 *
 * @param timestamp The timestamp indicates when the entire experiment is started.
 * @param iteration The index number of the current iteration.
 * @param configuration Configuration properties of this iteration.
 * @param results The performance results of queries for this iteration.
 */
case class ExperimentRun(
    timestamp: Long,
    iteration: Int,
    configuration: BenchmarkConfiguration,
    results: Seq[BenchmarkResult])

/**
 * The configuration used for an iteration of an experiment.
 *
 * @param sparkVersion The version of Spark.
 * @param sparkConf All configuration properties of Spark.
 * @param defaultParallelism The default parallelism of the cluster.
 *                           Usually, it is the number of cores of the cluster.
 */
case class BenchmarkConfiguration(
    sparkVersion: String = org.apache.spark.SPARK_VERSION,
    sparkConf: Map[String, String],
    defaultParallelism: Int,
    buildInfo: Map[String, String],
    resourceSpecification: Option[ResourceSpecification])

/**
 * The result of a query.
 *
 * @param name The name of the query.
 * @param mode The ExecutionMode of this run.
 * @param parameters Additional parameters that describe this query.
 */
case class BenchmarkResult(
    name: String,
    mode: String,
    parameters: Map[String, String] = Map.empty[String, String],
    executionTime: Option[Double] = None,
    failure: Option[Failure] = None)

case class Failure(className: String, message: String)

/*
// KEEP ARGUMENTS SORTED BY NAME.
// It simplifies lookup when checking if a parameter is here already.
case class MLParams(
    // *** Common to all algorithms ***
    randomSeed: Option[Int] = Some(42),
    numExamples: Option[Long] = None,
    numTestExamples: Option[Long] = None,
    numPartitions: Option[Int] = None,
    // *** Specialized and sorted by name ***
    depth: Option[Int] = None,
    elasticNetParam: Option[Double] = None,
    family: Option[String] = None,
    k: Option[Int] = None,
    ldaDocLength: Option[Int] = None,
    ldaNumVocabulary: Option[Int] = None,
    link: Option[String] = None,
    maxIter: Option[Int] = None,
    numClasses: Option[Int] = None,
    numFeatures: Option[Int] = None,
    numItems: Option[Int] = None,
    numUsers: Option[Int] = None,
    optimizer: Option[String] = None,
    regParam: Option[Double] = None,
    rank: Option[Int] = None,
    tol: Option[Double] = None
)

object MLParams {
  val empty = MLParams()
}

/**
 * Result information specific to MLlib.
 *
 * @param trainingTime  (MLlib) Training time.
 *                      executionTime is set to the same value to match Spark Core tests.
 * @param trainingMetric  (MLlib) Training metric, such as accuracy
 * @param testTime  (MLlib) Test time (for prediction on test set, or on training set if there
 *                  is no test set).
 * @param testMetric  (MLlib) Test metric, such as accuracy
 */
case class MLResult(
    trainingTime: Option[Double] = None,
    trainingMetric: Option[Double] = None,
    testTime: Option[Double] = None,
    testMetric: Option[Double] = None)
*/
