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

package com.microsoft.spark.perf.sql

import scala.language.implicitConversions
import scala.util.Try

import com.microsoft.spark.perf.{Benchmark, Benchmarkable, ExperimentStatus, ResourceParamGrid}
import com.microsoft.spark.perf.configurations.{ResourceSpecification, Variation}

import org.apache.spark.sql.Dataset

/**
 * A collection of queries that test a particular aspect of Spark SQL.
 */
abstract class SQLBenchmark (
    resourceSpecification: Option[ResourceSpecification]) extends Benchmark(resourceSpecification) {

  protected implicit def toOption[A](a: A): Option[A] = Option(a)

  private[perf] def buildTables(): Unit

  val buildInfo = Try(getClass.getClassLoader.loadClass("org.apache.spark.BuildInfo")).map { cls =>
    cls.getMethods
      .filter(_.getReturnType == classOf[String])
        .filterNot(_.getName == "toString")
        .map(m => m.getName -> m.invoke(cls).asInstanceOf[String])
        .toMap
  }.getOrElse(Map.empty)

  private def currentConfiguration = new SQLBenchmarkConfiguration(
    sqlConf = sparkSession.sqlContext.getAllConfs,
    sparkConf = sparkContext.getConf.getAll.toMap,
    defaultParallelism = sparkContext.defaultParallelism,
    buildInfo = buildInfo,
    resourceSpecification = resourceSpecification)

  /**
   * Starts an experiment run with a given set of executions to run.
   *
   * @param executionsToRun a list of executions to run.
   * @param includeBreakdown If it is true, breakdown results of an execution will be recorded.
   *                         Setting it to true may significantly increase the time used to
   *                         run an execution.
   * @param iterations The number of iterations to run of each execution.
   * @param variations [[Variation]]s used in this run.  The cross product of all variations will be
   *                   run for each execution * iteration.
   * @param timeout wait at most timeout milliseconds for each query, 0 means wait forever
   * @return It returns a ExperimentStatus object that can be used to
   *         track the progress of this experiment run.
   */
  override def createExperiment(
      executionsToRun: Seq[Benchmarkable],
      includeBreakdown: Boolean = false,
      iterations: Int = 3,
      variations: Seq[Variation[_]] = Seq(Variation("StandardRun", Seq("true")) { _ => {} }),
      timeout: Long = 0L,
      resultLocation: String = resultsLocation,
      forkThread: Boolean = true,
      reportFormat: String = "json"): ExperimentStatus = {

    buildTables()

    new SQLExperimentStatus(
      executionsToRun, includeBreakdown, iterations,
      timeout, variations, resultLocation, sparkSession.sqlContext, allTables, currentConfiguration,
      forkThread, reportFormat)
  }

  import reflect.runtime._
  import universe._

  @transient
  private val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

  @transient
  val myType = runtimeMirror.classSymbol(getClass).toType

  private def singleTables =
    myType.declarations
      .filter(m => m.isMethod)
      .map(_.asMethod)
      .filter(_.asMethod.returnType =:= typeOf[Table])
      .map(method => runtimeMirror.reflect(this).reflectMethod(method).apply().asInstanceOf[Table])

  private def groupedTables =
    myType.declarations
      .filter(m => m.isMethod)
      .map(_.asMethod)
      .filter(_.asMethod.returnType =:= typeOf[Seq[Table]])
      .flatMap(method => runtimeMirror.reflect(this).reflectMethod(method).apply()
      .asInstanceOf[Seq[Table]])

  @transient
  lazy val allTables: Seq[Table] = (singleTables ++ groupedTables).toSeq

  lazy val allQueries: Seq[Query] = Seq()

  override protected[perf] lazy val allWorkloads: Seq[Benchmarkable] = allQueries

  def html: String = {
    val singleQueries =
      myType.declarations
        .filter(m => m.isMethod)
        .map(_.asMethod)
        .filter(_.asMethod.returnType =:= typeOf[Query])
        .map(method => runtimeMirror.reflect(this).reflectMethod(method).apply().
          asInstanceOf[Query])
        .mkString(",")
    val queries =
      myType.declarations
      .filter(m => m.isMethod)
      .map(_.asMethod)
      .filter(_.asMethod.returnType =:= typeOf[Seq[Query]])
      .map { method =>
        val queries = runtimeMirror.reflect(this).reflectMethod(method).apply().
          asInstanceOf[Seq[Query]]
        val queryList = queries.map(_.name).mkString(", ")
        s"""
          |<h3>${method.name}</h3>
          |<ul>$queryList</ul>
        """.stripMargin
    }.mkString("\n")

    s"""
       |<h1>Spark SQL Performance Benchmarking</h1>
       |<h2>Available Queries</h2>
       |$singleQueries
       |$queries
     """.stripMargin
  }
}

case class Table(
    name: String,
    data: Dataset[_])

