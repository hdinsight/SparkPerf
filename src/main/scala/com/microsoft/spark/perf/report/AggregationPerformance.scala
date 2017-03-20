/*
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

import com.microsoft.spark.perf.sql.{Benchmark, Query, Table, Variation}

trait AggregationPerformance extends Benchmark {

  import ExecutionMode._
  import sqlContext.implicits._

  val sizes = (1 to 6).map(math.pow(10, _).toInt).toSeq

  val variousCardinality = sizes.map { size =>
    Table(s"ints$size",
      sparkContext.parallelize(1 to size).flatMap { group =>
        (1 to 10000).map(i => (group, i))
      }.toDF("a", "b"))
  }

  val lowCardinality = sizes.map { size =>
    val fullSize = size * 10000L
    Table(
      s"twoGroups$fullSize",
      sqlContext.range(0, fullSize).select($"id" % 2 as 'a, $"id" as 'b))
  }

  val newAggreation = Variation("aggregationType", Seq("new", "old")) {
    case "old" => sqlContext.setConf("spark.sql.useAggregate2", "false")
    case "new" => sqlContext.setConf("spark.sql.useAggregate2", "true")
  }

  val varyNumGroupsAvg: Seq[Query] = variousCardinality.map(_.name).map { table =>
    Query(
      s"avg-$table",
      s"SELECT AVG(b) FROM $table GROUP BY a",
      "an average with a varying number of groups",
      executionMode = ForeachResults)
  }

  val twoGroupsAvg: Seq[Query] = lowCardinality.map(_.name).map { table =>
    Query(
      s"avg-$table",
      s"SELECT AVG(b) FROM $table GROUP BY a",
      "an average on an int column with only two groups",
      executionMode = ForeachResults)
  }

  val complexInput =
    Seq("1milints", "100milints", "1bilints").map { table =>
      Query(
        s"aggregation-complex-input-$table",
        s"SELECT SUM(id + id + id + id + id + id + id + id + id + id) FROM $table",
        "Sum of 9 columns added together",
        executionMode = CollectResults)
    }

  val aggregates =
    Seq("1milints", "100milints", "1bilints").flatMap { table =>
      Seq("SUM", "AVG", "COUNT", "STDDEV").map { agg =>
        Query(
          s"single-aggregate-$agg-$table",
          s"SELECT $agg(id) FROM $table",
          "aggregation of a single column",
          executionMode = CollectResults)
      }
    }
}
