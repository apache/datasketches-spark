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

package org.apache.spark.sql.datasketches.theta.types

import org.apache.datasketches.theta.{UpdateSketch, CompactSketch, Union}
import org.apache.datasketches.memory.Memory
import org.apache.spark.sql.types.SQLUserDefinedType

@SQLUserDefinedType(udt = classOf[ThetaSketchType])
class ThetaSketchWrapper(var updateSketch: Option[UpdateSketch] = None, var compactSketch: Option[CompactSketch] = None, var union: Option[Union] = None) {

  def serialize: Array[Byte] = {
    if (updateSketch.isDefined) return updateSketch.get.compact().toByteArrayCompressed
    else if (compactSketch.isDefined) return compactSketch.get.toByteArrayCompressed
    else if (union.isDefined) return union.get.getResult.toByteArrayCompressed
    null
  }

  override def toString(): String = {
    if (updateSketch.isDefined) return updateSketch.get.toString
    else if (compactSketch.isDefined) return compactSketch.get.toString
    else if (union.isDefined) return union.get.toString
    ""
  }
}

object ThetaSketchWrapper {
  def deserialize(bytes: Array[Byte]): ThetaSketchWrapper = {
    new ThetaSketchWrapper(compactSketch = Some(CompactSketch.heapify(Memory.wrap(bytes))))
  }

  // this can go away in favor of directly calling the Sketch.wrap
  // from codegen once janino can generate java 8+ code
  def wrapAsReadOnlySketch(bytes: Array[Byte]): CompactSketch = {
    CompactSketch.wrap(Memory.wrap(bytes))
  }
}
