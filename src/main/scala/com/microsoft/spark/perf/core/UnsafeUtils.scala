package com.microsoft.spark.perf.core

import scala.collection.concurrent.TrieMap

import sun.misc.Unsafe

import org.apache.spark.TaskContext

private[core] object UnsafeUtils {
  private val unsafeMap = new TrieMap[Int, Unsafe]

  private[core] def getUnsafeInstance: Unsafe = {
    val partitionId = TaskContext.getPartitionId()
    unsafeMap.getOrElseUpdate(partitionId, {
      val unsafeField = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
      unsafeField.setAccessible(true)
      unsafeField.get(null).asInstanceOf[sun.misc.Unsafe]}
    )
  }
}
