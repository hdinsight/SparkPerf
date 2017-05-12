package com.microsoft.spark.perf.core

/**
 *  A simple wrapper around a byte array to store terasort data.
 */
private[core] class RecordWrapper(val bytes: Array[Byte])
    extends Product2[Array[Byte], Array[Byte]] {
  override def _1: Array[Byte] = bytes
  override def _2: Array[Byte] = bytes
  override def canEqual(that: Any): Boolean = {
    true
  }
}
