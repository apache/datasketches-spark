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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, LongType, NumericType, FloatType, DoubleType, KllDoublesSketchType}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult

/**
 * The KllDoublesSketchAgg function utilizes a Datasketches KllDoublesSketch instance
 * to create a sketch from a column of values which can be used to estimate quantiles
 * and histograms.
 *
 * @param data Expression with data values against which the sketch will be created
 * @param kExpr Expression for k, the size-accuracy trade-off parameter for the sketch, int in range [1, 65535]
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
case class KllDoublesSketchAggBuild(
    dataExpr: Expression,
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
      case _ => throw new IllegalArgumentException(
        s"Unsupported input type ${kExpr.dataType.catalogString}")
    }
  }

  // define names for BinaryLike
  override def left: Expression = dataExpr
  override def right: Expression = kExpr

  // Constructors
  def this(dataExpr: Expression) = {
    this(dataExpr, Literal(KllSketch.DEFAULT_K), 0, 0)
  }

  def this(dataExpr: Expression, kExpr: Expression) = {
    this(dataExpr, kExpr, 0, 0)
  }

  def this(dataExpr: Expression, k: Int) = {
    this(dataExpr, Literal(k), 0, 0)
  }

  // Copy constructors
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): KllDoublesSketchAggBuild =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): KllDoublesSketchAggBuild =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newLeft: Expression,
                                                 newRight: Expression): KllDoublesSketchAggBuild = {
    copy(dataExpr = newLeft, kExpr = newRight)
  }

  // overrides for TypedImperativeAggregate
  override lazy val deterministic: Boolean = false

  override def prettyName: String = "kll_sketch_double_agg_build"

  override def dataType: DataType = KllDoublesSketchType

  override def nullable: Boolean = false

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, IntegerType)

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

  override def createAggregationBuffer(): KllDoublesSketch = KllDoublesSketch.newHeapInstance(k)

  override def update(sketch: KllDoublesSketch, input: InternalRow): KllDoublesSketch = {
    val value = dataExpr.eval(input)
    if (value != null) {
      dataExpr.dataType match {
        case DoubleType => sketch.update(value.asInstanceOf[Double])
        case FloatType => sketch.update(value.asInstanceOf[Float].toDouble)
        case IntegerType => sketch.update(value.asInstanceOf[Int].toDouble)
        case LongType => sketch.update(value.asInstanceOf[Long].toDouble)
        case _ => throw new IllegalArgumentException(
          s"Unsupported input type ${dataExpr.dataType.catalogString}")
      }
    }
    sketch
  }

  override def merge(sketch: KllDoublesSketch, other: KllDoublesSketch): KllDoublesSketch = {
    if (other != null && !other.isEmpty) {
      sketch.merge(other)
    }
    sketch
  }

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
