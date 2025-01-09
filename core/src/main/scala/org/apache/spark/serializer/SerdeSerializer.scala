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

package org.apache.spark.serializer

import scala.reflect.ClassTag
import scala.reflect.runtime._
import org.apache.spark.executor.InTaskMetrics
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, Utils}
import pdsl.dpx.{SerdeInputStream, SerdeOutputStream}

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

class SerdeWrapInputStream(s: InputStream) extends DeserializationStream {
  val ss = new SerdeInputStream(s)

  override def readObject[T: ClassTag](): T = {
    ss.readObject(implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])
  }

  override def close(): Unit = {
    ss.close()
  }
}

class SerdeWrapOutputStream(s: OutputStream) extends SerializationStream {
  val ss = new SerdeOutputStream(s)

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    ss.writeObject(t)
    this
  }

  override def flush(): Unit = {
    ss.flush()
  }

  override def close(): Unit = {
    ss.close()
  }
}

class SerdeSerializerInstance extends SerializerInstance {
  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)
    out.close()
    bos.toByteBuffer
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new SerdeWrapOutputStream(s)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new SerdeWrapInputStream(s)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject()
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    deserialize(bytes)
  }
}

class SerdeSerializer extends Serializer {
  override def newInstance(inTaskMetrics: InTaskMetrics): SerializerInstance = {
    new SerdeSerializerInstance()
  }
}
