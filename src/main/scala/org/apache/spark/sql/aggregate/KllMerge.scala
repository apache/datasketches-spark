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

import org.apache.datasketches.memory.Memory
import org.apache.datasketches.kll.{KllSketch, KllDoublesSketch}

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, KllDoublesSketchType}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult

/**
 * The KllDoublesMergeAgg function utilizes a Datasketches KllDoublesSketch instance to
 * combine multiple sketches into a single sketch.
 *
 * @param left Expression from which the sketch will be merged
 * @param right k, the size-accuracy trade-off parameter for the sketch, int in range [1, 65535]
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, k]) - Merges multiple KllDoublesSketch images and returns the binary representation
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
    sketchExpr: Expression,
    kExpr: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[KllDoublesSketch]
    with BinaryLike[Expression]
    with ExpectsInputTypes {

  lazy val k: Int = {
    kExpr.eval() match {
      case null => KllSketch.DEFAULT_K
      case k: Int => k
      // this shouldn't happen after checkInputDataTypes()
      case _ => throw new SparkUnsupportedOperationException(
        s"Unsupported input type ${right.dataType.catalogString}",
        Map("dataType" -> dataType.toString))
    }
  }

  // define names for BinaryLike
  override def left: Expression = sketchExpr
  override def right: Expression = kExpr

  // Constructors
  def this(sketchExpr: Expression) = {
    this(sketchExpr, Literal(KllSketch.DEFAULT_K), 0, 0)
  }

  def this(sketchExpr: Expression, kExpr: Expression) = {
    this(sketchExpr, kExpr, 0, 0)
  }

  def this(sketchExpr: Expression, k: Int) = {
    this(sketchExpr, Literal(k), 0, 0)
  }

  // Copy constructors
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): KllDoublesMergeAgg =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): KllDoublesMergeAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): KllDoublesMergeAgg =
    copy(sketchExpr = newLeft, kExpr = newRight)

  // overrides for TypedImperativeAggregate
  override def prettyName: String = "kll_merge_agg"

  override def dataType: DataType = KllDoublesSketchType

  override def nullable: Boolean = false

  override def stateful: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(KllDoublesSketchType, IntegerType)

  override def checkInputDataTypes(): TypeCheckResult = {
    // k must be a constant
    if (!kExpr.foldable) {
      return TypeCheckResult.TypeCheckFailure(s"k must be foldable, but got: ${kExpr}")
    }
    // Check if k >= 8 and k <= MAX_K
    kExpr.eval() match {
      case k: Int if k >= 8 && k <= KllSketch.MAX_K => // valid state, do nothing
      case k: Int if k > KllSketch.MAX_K => return TypeCheckResult.TypeCheckFailure(
        s"k must be less than or equal to ${KllSketch.MAX_K}, but got: $k")
      case k: Int => return TypeCheckResult.TypeCheckFailure(s"k must be at least 8 and no greater than ${KllSketch.MAX_K}, but got: $k")
      case _ => return TypeCheckResult.TypeCheckFailure(s"Unsupported input type ${kExpr.dataType.catalogString}")
    }

    // additional validations of k handled in the DataSketches library
    TypeCheckResult.TypeCheckSuccess
  }

  override def createAggregationBuffer(): KllDoublesSketch = {
    KllDoublesSketch.newHeapInstance(k)
  }

  override def update(union: KllDoublesSketch, input: InternalRow): KllDoublesSketch = {
    val value = sketchExpr.eval(input)
    if (value != null && value != None) {
      sketchExpr.dataType match {
        case KllDoublesSketchType =>
            union.merge(KllDoublesSketch.wrap(Memory.wrap(value.asInstanceOf[Array[Byte]])))
            union
        case _ => throw new SparkUnsupportedOperationException(
          s"Unsupported input type ${sketchExpr.dataType.catalogString}",
          Map("dataType" -> dataType.toString))
      }
    } else {
      union
    }
  }

  override def merge(union: KllDoublesSketch, other: KllDoublesSketch): KllDoublesSketch = {
    if (union != null && other != null) {
      union.merge(other)
      union
    } else if (union != null && other == null) {
      union
    } else if (union == null && other != null) {
      other
    } else {
      union
    }
  }

  override def eval(sketch: KllDoublesSketch): Any = {
    sketch.toByteArray
  }

  override def serialize(sketch: KllDoublesSketch): Array[Byte] = {
    sketch.toByteArray()
  }

  override def deserialize(bytes: Array[Byte]): KllDoublesSketch = {
    if (bytes.length > 0) {
      KllDoublesSketchType.deserialize(bytes)
    } else {
      null
    }
  }
}
