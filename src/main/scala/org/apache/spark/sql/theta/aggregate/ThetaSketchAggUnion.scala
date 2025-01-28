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
import org.apache.datasketches.theta.{Sketch, SetOperation}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, ThetaSketchWrapper, ThetaSketchType}

/**
 * Theta Union operation.
 *
 * See [[https://datasketches.apache.org/docs/Theta/ThetaSketches.html]] for more information.
 *
 * @param child child expression, on which to perform the union operation
 * @param lgk the size-accraucy trade-off parameter for the sketch
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, lgk) - Performs Theta Union operation and returns the result as Theta Sketch in binary form
  """,
  examples = """
    Example:
      > SELECT theta_sketch_get_estimate(_FUNC_(sketch)) FROM (SELECT theta_sketch_build(col) as sketch FROM VALUES (1), (2), (3) tab(col) UNION ALL SELECT theta_sketch_build(col) as sketch FROM VALUES (3), (4), (5) tab(col));
       5.0
  """
)
// scalastyle:on line.size.limit
case class ThetaSketchAggUnion(
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

  def this(left: Expression, right: Expression) = this(left, right, 0, 0)
  def this(left: Expression) = this(left, Literal(12), 0, 0)
//  def this(child: Expression, lgk: Expression) = this(child, lgk, 0, 0)
//  def this(child: Expression, lgk: Int) = this(child, Literal(lgk), 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ThetaSketchAggUnion =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ThetaSketchAggUnion =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): ThetaSketchAggUnion = {
    copy(left = newLeft, right = newRight)
  }

  // overrides for TypedImperativeAggregate
  override def prettyName: String = "theta_union"
  override def dataType: DataType = ThetaSketchType
  override def nullable: Boolean = false

  // TODO: refine this?
  override def inputTypes: Seq[AbstractDataType] = Seq(ThetaSketchType, IntegerType)

  override def createAggregationBuffer(): ThetaSketchWrapper = new ThetaSketchWrapper(union = Some(SetOperation.builder().setLogNominalEntries(lgk).buildUnion))

  override def update(wrapper: ThetaSketchWrapper, input: InternalRow): ThetaSketchWrapper = {
    val bytes = left.eval(input)
    if (bytes != null) {
      left.dataType match {
        case ThetaSketchType =>
          wrapper.union.get.union(Sketch.wrap(Memory.wrap(bytes.asInstanceOf[Array[Byte]])))
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
    wrapper.union.get.getResult.toByteArrayCompressed
  }

  override def serialize(wrapper: ThetaSketchWrapper): Array[Byte] = {
    ThetaSketchType.serialize(wrapper)
  }

  override def deserialize(bytes: Array[Byte]): ThetaSketchWrapper = {
    ThetaSketchType.deserialize(bytes)
  }
}
