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
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, LongType, NumericType, FloatType, DoubleType, KllDoublesSketchType}

/**
 * The KllDoublesSketchAgg function utilizes a Datasketches KllDoublesSketch instance
 * to create a sketch from a column of values which can be used to estimate quantiles
 * and histograms.
 *
 * @param child child expression against which the sketch will be created
 * @param k the size-accuracy trade-off parameter for the sketch
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, k) - Creates a KllDoublesSketch and returns the binary representation.
      `k` (optional, default: 200) the size-accuracy trade-off parameter.""",
  examples = """
    Examples:
      > SELECT kll_get_quantile(_FUNC_(col, 200), 0.5) FROM VALUES (1.0), (1.0), (2.0), (2.0), (3.0) tab(col);
       2.0
  """,
)
// scalastyle:on line.size.limit
case class KllDoublesSketchAgg(
    left: Expression,
    right: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[KllDoublesSketch]
    with BinaryLike[Expression]
    with ExpectsInputTypes {

  lazy val k: Int = {
    right.eval() match {
      case null => KllSketch.DEFAULT_K
      case k: Int => k
      case _ => throw new SparkUnsupportedOperationException(
        s"Unsupported input type ${right.dataType.catalogString}",
        Map("dataType" -> dataType.toString))
    }
  }

  // Constructors

  def this(child: Expression) = {
    this(child, Literal(KllSketch.DEFAULT_K), 0, 0)
  }

  def this(child: Expression, k: Expression) = {
    this(child, k, 0, 0)
  }

  def this(child: Expression, k: Int) = {
    this(child, Literal(k), 0, 0)
  }

  // Copy constructors
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): KllDoublesSketchAgg =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): KllDoublesSketchAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newLeft: Expression,
                                                 newRight: Expression): KllDoublesSketchAgg = {
    copy(left = newLeft, right = newRight)
  }

  // overrides for TypedImperativeAggregate
  override def prettyName: String = "kll_sketch_agg"

  override def dataType: DataType = KllDoublesSketchType

  override def nullable: Boolean = false

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, IntegerType, LongType, FloatType, DoubleType)

  // create buffer
  override def createAggregationBuffer(): KllDoublesSketch = KllDoublesSketch.newHeapInstance(k)

  // update
  override def update(sketch: KllDoublesSketch, input: InternalRow): KllDoublesSketch = {
    val value = left.eval(input)
    if (value != null) {
      left.dataType match {
        case DoubleType => sketch.update(value.asInstanceOf[Double])
        case FloatType => sketch.update(value.asInstanceOf[Float].toDouble)
        case IntegerType => sketch.update(value.asInstanceOf[Int].toDouble)
        case LongType => sketch.update(value.asInstanceOf[Long].toDouble)
        case _ => throw new SparkUnsupportedOperationException(
          s"Unsupported input type ${left.dataType.catalogString}",
          Map("dataType" -> dataType.toString))
      }
    }
    sketch
  }

  // union (merge)
  override def merge(sketch: KllDoublesSketch, other: KllDoublesSketch): KllDoublesSketch = {
    if (other != null && !other.isEmpty) {
      sketch.merge(other)
    }
    sketch
  }

  // eval
  override def eval(sketch: KllDoublesSketch): Any = {
    if (sketch == null || sketch.isEmpty) {
      null
    } else {
      sketch.toByteArray
    }
  }

  override def serialize(sketch: KllDoublesSketch): Array[Byte] = {
    KllDoublesSketchType.serialize(sketch)
  }

  override def deserialize(bytes: Array[Byte]): KllDoublesSketch = {
    KllDoublesSketchType.deserialize(bytes)
  }
}
