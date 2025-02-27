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

package org.apache.spark.sql.datasketches.theta.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, LongType}

import org.apache.spark.sql.datasketches.theta.ThetaSketchConstants.DEFAULT_LG_K
import org.apache.spark.sql.datasketches.theta.types.{ThetaSketchType, ThetaSketchWrapper}

import org.apache.datasketches.memory.Memory
import org.apache.datasketches.theta.{Sketch, SetOperation}
import org.apache.datasketches.thetacommon.ThetaUtil.DEFAULT_UPDATE_SEED

/**
 * Theta Union operation.
 *
 * See [[https://datasketches.apache.org/docs/Theta/ThetaSketches.html]] for more information.
 *
 * @param input expression, on which to perform the union operation
 * @param lgK size-accraucy trade-off parameter for the sketch
 * @param seed update seed for the sketch
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, lgK, seed) - Performs Theta Union operation and returns the result as Theta Sketch in binary form
      `lgK` (optional, default: 12) size-accuracy trade-off parameter.
      `seed` (optional, default: 9001) update seed for the sketch.
  """,
  examples = """
    Example:
      > SELECT theta_sketch_get_estimate(_FUNC_(sketch)) FROM (SELECT theta_sketch_build(col) as sketch FROM VALUES (1), (2), (3) tab(col) UNION ALL SELECT theta_sketch_build(col) as sketch FROM VALUES (3), (4), (5) tab(col));
       5.0
  """
)
// scalastyle:on line.size.limit
case class ThetaSketchAggUnion(
    inputExpr: Expression,
    lgKExpr: Expression,
    seedExpr: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ThetaSketchWrapper]
    with TernaryLike[Expression]
    with ExpectsInputTypes {

  lazy val lgK: Int = {
    lgKExpr.eval() match {
      case null => DEFAULT_LG_K
      case lgK: Int => lgK
      case _ => throw new IllegalArgumentException(
        s"Unsupported input type ${lgKExpr.dataType.catalogString}")
    }
  }

  lazy val seed: Long = {
    seedExpr.eval() match {
      case null => DEFAULT_UPDATE_SEED
      case seed: Long => seed
      case _ => throw new IllegalArgumentException(
        s"Unsupported input type ${seedExpr.dataType.catalogString}")
    }
  }

  override def first: Expression = inputExpr
  override def second: Expression = lgKExpr
  override def third: Expression = seedExpr

  def this(inputExpr: Expression, lgKExpr: Expression, seedExpr: Expression) = this(inputExpr, lgKExpr, seedExpr, 0, 0)
  def this(inputExpr: Expression, lgKExpr: Expression) = this(inputExpr, lgKExpr, Literal(DEFAULT_UPDATE_SEED))
  def this(inputExpr: Expression) = this(inputExpr, Literal(DEFAULT_LG_K))

  def this(inputExpr: Expression, lgK: Int) = this(inputExpr, Literal(lgK))
  def this(inputExpr: Expression, lgK: Int, seed: Long) = this(inputExpr, Literal(lgK), Literal(seed))

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ThetaSketchAggUnion =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ThetaSketchAggUnion =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newInputExpr: Expression, newLgKExpr: Expression, newSeedExpr: Expression): ThetaSketchAggUnion = {
    copy(inputExpr = newInputExpr, lgKExpr = newLgKExpr, seedExpr = newSeedExpr)
  }

  // overrides for TypedImperativeAggregate
  override def prettyName: String = "theta_sketch_agg_union"
  override def dataType: DataType = ThetaSketchType
  override def nullable: Boolean = false

  override def inputTypes: Seq[AbstractDataType] = Seq(ThetaSketchType, IntegerType, LongType)

  override def createAggregationBuffer(): ThetaSketchWrapper = new ThetaSketchWrapper(union
    = Some(SetOperation.builder().setLogNominalEntries(lgK).setSeed(seed).buildUnion()))

  override def update(wrapper: ThetaSketchWrapper, input: InternalRow): ThetaSketchWrapper = {
    val bytes = inputExpr.eval(input)
    if (bytes != null) {
      inputExpr.dataType match {
        case ThetaSketchType =>
          wrapper.union.get.union(Sketch.wrap(Memory.wrap(bytes.asInstanceOf[Array[Byte]])))
        case _ => throw new IllegalArgumentException(
          s"Unsupported input type ${inputExpr.dataType.catalogString}")
      }
    }
    wrapper
  }

  override def merge(wrapper: ThetaSketchWrapper, other: ThetaSketchWrapper): ThetaSketchWrapper = {
    if (other != null && !other.compactSketch.get.isEmpty()) {
      if (wrapper.union.isEmpty) {
        wrapper.union = Some(SetOperation.builder().setLogNominalEntries(lgK).setSeed(seed).buildUnion())
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
    wrapper.union.get.getResult.toByteArrayCompressed()
  }

  override def serialize(wrapper: ThetaSketchWrapper): Array[Byte] = {
    ThetaSketchType.serialize(wrapper)
  }

  override def deserialize(bytes: Array[Byte]): ThetaSketchWrapper = {
    ThetaSketchType.deserialize(bytes)
  }
}
