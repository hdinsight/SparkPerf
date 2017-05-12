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

import com.microsoft.spark.perf.configurations.ResourceSpecification
import com.microsoft.spark.perf.report.BenchmarkConfiguration

class SQLBenchmarkConfiguration(
    sparkVersion: String = org.apache.spark.SPARK_VERSION,
    sqlConf: Map[String, String],
    sparkConf: Map[String, String],
    defaultParallelism: Int,
    buildInfo: Map[String, String],
    resourceSpecification: Option[ResourceSpecification])
  extends BenchmarkConfiguration(sparkVersion, sparkConf, defaultParallelism, buildInfo,
    resourceSpecification)
