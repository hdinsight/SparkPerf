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

package com.microsoft.spark.perf.configurations

/**
 * A Variation represents a setting (e.g. the number of shuffle partitions or if tables
 * are cached in memory) that we want to change in a experiment run.
 * A Variation has three parts, `name`, `options`, and `setup`.
 * The `name` is the identifier of a Variation. `options` is a Seq of options that
 * will be used for a query. Basically, a query will be executed with every option
 * defined in the list of `options`. `setup` defines the needed action for every
 * option. For example, the following Variation is used to change the number of shuffle
 * partitions of a query. The name of the Variation is "shufflePartitions". There are
 * two options, 200 and 2000. The setup is used to set the value of property
 * "spark.sql.shuffle.partitions".
 *
 * {{{
 *   Variation("shufflePartitions", Seq("200", "2000")) {
 *     case num => sqlContext.setConf("spark.sql.shuffle.partitions", num)
 *   }
 * }}}
 *
 * TODO: unused for now, need to add it later
 */
private[perf] case class Variation[T](name: String, options: Seq[T])(val setup: T => Unit)

