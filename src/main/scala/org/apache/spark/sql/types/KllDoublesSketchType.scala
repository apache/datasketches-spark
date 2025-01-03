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

package org.apache.spark.sql.types

import org.apache.spark.sql.functions.udf

class KllDoublesSketchType extends UserDefinedType[KllDoublesSketchWrapper] with Serializable {
  override def sqlType: DataType = DataTypes.BinaryType

  override def serialize(wrapper: KllDoublesSketchWrapper): Array[Byte] = {
    wrapper.toByteArray
  }

  override def deserialize(data: Any): KllDoublesSketchWrapper = {
    val bytes = data.asInstanceOf[Array[Byte]]
    KllDoublesSketchWrapper.deserialize(bytes)
  }

  override def userClass: Class[KllDoublesSketchWrapper] = classOf[KllDoublesSketchWrapper]

  override def catalogString: String = "KllDoublesSketch"
}

case object KllDoublesSketchType extends KllDoublesSketchType {
  // udf to allow importing serialized sketches into dataframes
  val wrapBytes = udf((bytes: Array[Byte]) => {
    if (bytes == null) {
      null
    } else {
      deserialize(bytes)
    }
  })
}
