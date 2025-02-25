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
import org.apache.spark.sql.catalyst.trees.QuaternaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, LongType, NumericType, FloatType, DoubleType, StringType, TypeCollection}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.spark.sql.datasketches.theta.ThetaSketchConstants.DEFAULT_LG_K
import org.apache.spark.sql.datasketches.theta.types.{ThetaSketchType, ThetaSketchWrapper}

import org.apache.datasketches.common.ResizeFactor
import org.apache.datasketches.theta.{UpdateSketch, SetOperation}
import org.apache.datasketches.thetacommon.ThetaUtil.DEFAULT_UPDATE_SEED

/**
 * The ThetaSketchBuild function creates a Theta sketch from a column of values
 * which can be used to estimate distinct count.
 *
 * See [[https://datasketches.apache.org/docs/Theta/ThetaSketches.html]] for more information.
 *
 * @param input expression, from which to build a sketch
 * @param lgK size-accraucy trade-off parameter for the sketch
 * @param seed update seed for the sketch
 * @param p initial sampling probability for the sketch
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, lgK, seed, p) - Creates a Theta Sketch and returns the binary representation.
      `lgK` (optional, default: 12) size-accuracy trade-off parameter.
      `seed` (optional, default: 9001) update seed for the sketch.
      `p` (optional, default: 1) initial sampling probability for the sketch.""",
  examples = """
    Example:
      > SELECT theta_sketch_get_estimate(_FUNC_(col)) FROM VALUES (1), (2), (3), (4), (5) tab(col);
       5.0
  """,
)
// scalastyle:on line.size.limit
case class ThetaSketchAggBuild(
    inputExpr: Expression,
    lgKExpr: Expression,
    seedExpr: Expression,
    pExpr: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ThetaSketchWrapper]
    with QuaternaryLike[Expression]
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

  lazy val p: Float = {
    pExpr.eval() match {
      case null => 1f
      case p: Float => p
      case _ => throw new IllegalArgumentException(
        s"Unsupported input type ${pExpr.dataType.catalogString}")
    }
  }
  override def first: Expression = inputExpr
  override def second: Expression = lgKExpr
  override def third: Expression = seedExpr
  override def fourth: Expression = pExpr

  def this(inputExpr: Expression, lgKExpr: Expression, seedExpr: Expression, pExpr: Expression) = this(inputExpr, lgKExpr, seedExpr, pExpr, 0, 0)
  def this(inputExpr: Expression, lgKExpr: Expression, seedExpr: Expression) = this(inputExpr, lgKExpr, seedExpr, Literal(1f))
  def this(inputExpr: Expression, lgKExpr: Expression) = this(inputExpr, lgKExpr, Literal(DEFAULT_UPDATE_SEED))
  def this(inputExpr: Expression) = this(inputExpr, Literal(DEFAULT_LG_K))

  def this(inputExpr: Expression, lgK: Int) = this(inputExpr, Literal(lgK))
  def this(inputExpr: Expression, lgK: Int, seed: Long) = this(inputExpr, Literal(lgK), Literal(seed))
  def this(inputExpr: Expression, lgK: Int, seed: Long, p: Float) = this(inputExpr, Literal(lgK), Literal(seed), Literal(p))

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ThetaSketchAggBuild =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ThetaSketchAggBuild =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression, newFourth: Expression): ThetaSketchAggBuild = {
    copy(inputExpr = newFirst, lgKExpr = newSecond, seedExpr = newThird, pExpr = newFourth)
  }

  override def prettyName: String = "theta_sketch_agg_build"

  override def dataType: DataType = ThetaSketchType

  override def nullable: Boolean = false

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(NumericType, StringType), IntegerType, LongType, FloatType)

  override def createAggregationBuffer(): ThetaSketchWrapper = new ThetaSketchWrapper(updateSketch
    = Some(UpdateSketch.builder().setLogNominalEntries(lgK).setSeed(seed).setP(p).build()))

  override def update(wrapper: ThetaSketchWrapper, input: InternalRow): ThetaSketchWrapper = {
    val value = inputExpr.eval(input)
    if (value != null) {
      inputExpr.dataType match {
        case DoubleType => wrapper.updateSketch.get.update(value.asInstanceOf[Double])
        case FloatType => wrapper.updateSketch.get.update(value.asInstanceOf[Float])
        case IntegerType => wrapper.updateSketch.get.update(value.asInstanceOf[Int])
        case LongType => wrapper.updateSketch.get.update(value.asInstanceOf[Long])
        case StringType => wrapper.updateSketch.get.update(value.asInstanceOf[UTF8String].toString)
        case _ => throw new IllegalArgumentException(
          s"Unsupported input type ${inputExpr.dataType.catalogString}")
      }
    }
    wrapper
  }

  override def merge(wrapper: ThetaSketchWrapper, other: ThetaSketchWrapper): ThetaSketchWrapper = {
    if (other != null && !other.compactSketch.get.isEmpty()) {
      if (wrapper.union.isEmpty) {
        wrapper.union = Some(SetOperation.builder().setLogNominalEntries(lgK).setSeed(seed).setP(p).buildUnion())
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
      wrapper.union.get.getResult.toByteArrayCompressed()
    }
  }

  override def serialize(wrapper: ThetaSketchWrapper): Array[Byte] = {
    ThetaSketchType.serialize(wrapper)
  }

  override def deserialize(bytes: Array[Byte]): ThetaSketchWrapper = {
    ThetaSketchType.deserialize(bytes)
  }
}
