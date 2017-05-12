package com.microsoft.spark.perf.core

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}

class TeraSortSerializer extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new TeraSortSerializerInstance
}


class TeraSortSerializerInstance extends SerializerInstance {
  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    throw new UnsupportedOperationException
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException

  override def serializeStream(s: OutputStream): SerializationStream = {
    new TeraSortSerializationStream(s)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new TeraSortDeserializationStream(s)
  }
}


class TeraSortSerializationStream(stream: OutputStream) extends SerializationStream {
  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    stream.write(t.asInstanceOf[Array[Byte]])
    this
  }

  override def flush(): Unit = stream.flush()

  override def close(): Unit = stream.close()
}


class TeraSortDeserializationStream(stream: InputStream) extends DeserializationStream {
  override def readObject[T: ClassTag](): T = {
    val record = new RecordWrapper(new Array[Byte](100))
    readFully(stream, record.bytes, 100)
    record.bytes.asInstanceOf[T]
  }

  override def close(): Unit = stream.close()

  def readFully(stream: InputStream, bytes: Array[Byte], length: Int) {
    var read = 0
    while (read < length) {
      val inc = stream.read(bytes, read, length - read)
      if (inc < 0) throw new java.io.EOFException
      read += inc
    }
  }
}
