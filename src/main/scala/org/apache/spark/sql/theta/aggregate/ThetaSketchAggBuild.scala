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

import org.apache.datasketches.theta.{UpdateSketch, SetOperation}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, LongType, NumericType, FloatType, DoubleType, ThetaSketchWrapper, ThetaSketchType}

/**
 * The ThetaSketchBuild function creates a Theta sketch from a column of values
 * which can be used to estimate distinct count.
 *
 * See [[https://datasketches.apache.org/docs/Theta/ThetaSketches.html]] for more information.
 *
 * @param child child expression, from which to build a sketch
 * @param lgk the size-accraucy trade-off parameter for the sketch
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, lgk) - Creates a Theta Sketch and returns the binary representation.
      `lgk` (optional, default: 12) the size-accuracy trade-off parameter.""",
  examples = """
    Example:
      > SELECT theta_sketch_get_estimate(_FUNC_(col, 12)) FROM VALUES (1), (2), (3), (4), (5) tab(col);
       5.0
  """,
)
// scalastyle:on line.size.limit
case class ThetaSketchAggBuild(
    left: Expression,
    right: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ThetaSketchWrapper]
    with BinaryLike[Expression]
    with ExpectsInputTypes {

  lazy val lgk: Int = {
    right.eval() match {
      case null => 12
      case lgk: Int => lgk
      case _ => throw new IllegalArgumentException(
        s"Unsupported input type ${right.dataType.catalogString}")
    }
  }

  def this(child: Expression) = this(child, Literal(12), 0, 0)
  def this(child: Expression, lgk: Expression) = this(child, lgk, 0, 0)
  def this(child: Expression, lgk: Int) = this(child, Literal(lgk), 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ThetaSketchAggBuild =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ThetaSketchAggBuild =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): ThetaSketchAggBuild = {
    copy(left = newLeft, right = newRight)
  }

  override def prettyName: String = "theta_sketch_build"

  override def dataType: DataType = ThetaSketchType

  override def nullable: Boolean = false

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, IntegerType, LongType, FloatType, DoubleType)

  override def createAggregationBuffer(): ThetaSketchWrapper = new ThetaSketchWrapper(updateSketch = Some(UpdateSketch.builder().setLogNominalEntries(lgk).build()))

  override def update(wrapper: ThetaSketchWrapper, input: InternalRow): ThetaSketchWrapper = {
    val value = left.eval(input)
    if (value != null) {
      left.dataType match {
        case DoubleType => wrapper.updateSketch.get.update(value.asInstanceOf[Double])
        case FloatType => wrapper.updateSketch.get.update(value.asInstanceOf[Float])
        case IntegerType => wrapper.updateSketch.get.update(value.asInstanceOf[Int])
        case LongType => wrapper.updateSketch.get.update(value.asInstanceOf[Long])
        case _ => throw new IllegalArgumentException(
          s"Unsupported input type ${left.dataType.catalogString}")
      }
    }
    wrapper
  }

  override def merge(wrapper: ThetaSketchWrapper, other: ThetaSketchWrapper): ThetaSketchWrapper = {
    if (other != null && !other.compactSketch.get.isEmpty) {
      if (wrapper.union.isEmpty) {
        wrapper.union = Some(SetOperation.builder().setLogNominalEntries(lgk).buildUnion)
        if (wrapper.compactSketch.isDefined) {
          wrapper.union.get.union(wrapper.compactSketch.get)
          wrapper.compactSketch = None
        }
      }
      wrapper.union.get.union(other.compactSketch.get)
    }
    wrapper
  }

  override def eval(wrapper: ThetaSketchWrapper): Any = {
    if (wrapper == null || wrapper.union.isEmpty) {
      null
    } else {
      wrapper.union.get.getResult.toByteArrayCompressed
    }
  }

  override def serialize(wrapper: ThetaSketchWrapper): Array[Byte] = {
    ThetaSketchType.serialize(wrapper)
  }

  override def deserialize(bytes: Array[Byte]): ThetaSketchWrapper = {
    ThetaSketchType.deserialize(bytes)
  }
}
