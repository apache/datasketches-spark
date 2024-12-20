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

import org.apache.datasketches.kll.KllDoublesSketch
import org.apache.datasketches.memory.Memory

@SQLUserDefinedType(udt = classOf[KllDoublesSketchType])
case class KllDoublesSketchWrapper(sketch: KllDoublesSketch) {

  def toByteArray: Array[Byte] = sketch.toByteArray

  def serialize: Array[Byte] = sketch.toByteArray

  def toString(withLevels: Boolean = false, withData: Boolean = false ): String = {
    sketch.toString(withLevels, withData)
  }
}

object KllDoublesSketchWrapper {
  // TODO: determine if wrap or writableWrap is a better option
  def deserialize(bytes: Array[Byte]): KllDoublesSketchWrapper = {
    val sketch = KllDoublesSketch.heapify(Memory.wrap(bytes))
    KllDoublesSketchWrapper(sketch)
  }

  // this can go away in favor of directly calling the KllDoublesSketch.wrap
  // from codegen once janino can generate java 8+ code
  def wrapAsReadOnlySketch(bytes: Array[Byte]): KllDoublesSketch = {
    KllDoublesSketch.wrap(Memory.wrap(bytes))
  }
}
