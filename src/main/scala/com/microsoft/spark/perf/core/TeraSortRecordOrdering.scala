/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.spark.perf.core

import scala.collection.concurrent.{Map, TrieMap}

import sun.misc.Unsafe

import org.apache.spark.TaskContext

/**
 * Comparator used for terasort. It compares the first ten bytes of the record by
 * first comparing the first 8 bytes, and then the last 2 bytes (using unsafe).
 */
private[core] class TeraSortRecordOrdering extends Ordering[Array[Byte]] {

  private val byteArrayBaseOffset = 16

  override def compare(x: Array[Byte], y: Array[Byte]): Int = {

    val unsafe = UnsafeUtils.getUnsafeInstance
    val leftWord = unsafe.getLong(x, byteArrayBaseOffset)
    val rightWord = unsafe.getLong(y, byteArrayBaseOffset)

    if (leftWord > rightWord) {
      1
    } else if (leftWord < rightWord) {
      -1
    } else {
      unsafe.getShort(x, byteArrayBaseOffset + 8) - unsafe.getShort(y, byteArrayBaseOffset + 8)
    }
  }
}

