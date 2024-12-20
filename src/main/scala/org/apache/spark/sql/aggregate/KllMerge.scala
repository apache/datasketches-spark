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

package org.apache.spark.sql.aggregate

import org.apache.datasketches.kll.{KllSketch, KllDoublesSketch}

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, KllDoublesSketchWrapper, KllDoublesSketchType}
import org.apache.datasketches.memory.Memory

/**
 * The KllDoublesMergeAgg function utilizes a Datasketches KllDoublesSketch instance to
 * combine multiple sketches into a single sketch.
 *
 * See [[https://datasketches.apache.org/docs/HLL/HLL.html]] for more information.
 *
 * @param child child expression against which unique counting will occur
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, k) - Merges multiple KllDoublesSketch images and returns the binary representation
    """,
  examples = """
    Examples:
      > SELECT kll_get_quantile(_FUNC_(sketch), 0.5) FROM (SELECT kll_sketch_agg(col) as sketch FROM VALUES (1.0), (2.0) tab(col) UNION ALL SELECT kll_sketch_agg(col) as sketch FROM VALUES (2.0), (3.0) tab(col));
       2.0
  """,
  //group = "agg_funcs",
)
// scalastyle:on line.size.limit
case class KllDoublesMergeAgg(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[Option[KllDoublesSketchWrapper]]
    with UnaryLike[Expression]
    with ExpectsInputTypes {

  // Constructors

  // this seems redundant given the constructor?
  def this(child: Expression) = this(child, 0, 0)

  // Copy constructors

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): KllDoublesMergeAgg =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): KllDoublesMergeAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): KllDoublesMergeAgg =
    copy(child = newChild)


  // overrides for TypedImperativeAggregate
  override def prettyName: String = "kll_merge_agg"

  override def dataType: DataType = KllDoublesSketchType

  override def nullable: Boolean = false

  // TODO: refine this?
  override def inputTypes: Seq[AbstractDataType] = Seq(KllDoublesSketchType)

  // create buffer
  override def createAggregationBuffer(): Option[KllDoublesSketchWrapper] = {
    None
  }

  // update
  override def update(unionOption: Option[KllDoublesSketchWrapper], input: InternalRow): Option[KllDoublesSketchWrapper] = {
    val value = child.eval(input)
    if (value != null && value != None) {
      child.dataType match {
        case KllDoublesSketchType =>
          if (unionOption == None || unionOption.get.sketch.isEmpty) {
            // if union is empty, just return a copy of the input sketch
            // TODO: is this serialized or already as a sketch object?
            //Some(KllDoublesSketch.heapify(Memory.wrap(value.asInstanceOf[Array[Byte]])))
            Some(KllDoublesSketchWrapper.deserialize(value.asInstanceOf[Array[Byte]]))
          } else {
            unionOption.get.sketch.merge(KllDoublesSketch.wrap(Memory.wrap(value.asInstanceOf[Array[Byte]])))
            unionOption
          }
        case _ => throw new SparkUnsupportedOperationException(
          s"Unsupported input type ${child.dataType.catalogString}",
          Map("dataType" -> dataType.toString))
      }
    } else {
      unionOption
    }
  }

  // union (merge)
  override def merge(unionOption: Option[KllDoublesSketchWrapper], otherOption: Option[KllDoublesSketchWrapper]): Option[KllDoublesSketchWrapper] = {
    (unionOption, otherOption) match {
      case (Some(union), Some(other)) =>
        union.sketch.merge(other.sketch)
        Some(union)

      // for these others, we'll return the input even if degenerate
      case (Some(union), None) =>
        unionOption
      case (None, Some(other)) =>
        otherOption
      case (None, None) =>
        unionOption
    }
  }

  // eval
  override def eval(unionOption: Option[KllDoublesSketchWrapper]): Any = {
    unionOption match {
      case Some(wrapper) => wrapper.sketch.toByteArray
      case None => None // can this happen in practice? If so, what should we return?
    }
  }

  override def serialize(sketchOption: Option[KllDoublesSketchWrapper]): Array[Byte] = {
    sketchOption match {
      case Some(wrapper) => wrapper.sketch.toByteArray
      case None => KllDoublesSketch.newHeapInstance(KllSketch.DEFAULT_K).toByteArray
    }
  }

  override def deserialize(bytes: Array[Byte]): Option[KllDoublesSketchWrapper] = {
    if (bytes.length > 0) {
      Some(KllDoublesSketchType.deserialize(bytes))
    } else {
      None
    }
  }
}
