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

package org.apache.spark.sql.datasketches.kll.expressions

import org.apache.datasketches.memory.Memory
import org.apache.datasketches.kll.KllDoublesSketch
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria
import org.apache.spark.sql.datasketches.kll.types.KllDoublesSketchType

import org.apache.spark.sql.types.{AbstractDataType, ArrayType, BooleanType, DataType, DoubleType}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpectsInputTypes, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.TernaryExpression
import org.apache.spark.sql.catalyst.expressions.{Literal, NullIntolerant, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeBlock, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult

@ExpressionDescription(
  usage = """
    _FUNC_(expr, expr, isInclusive) - Returns an approximation to the PMF
      of the given KllDoublesSketch using the specified search criteria (default: inclusive, isInclusive = true)
      or exclusive using the given split points.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg(col), array(1.5, 3.5)) FROM VALUES (1.0), (2.0), (3.0) tab(col);
       [0.3333333333333333, 0.6666666666666666, 0.0]
  """
)
case class KllDoublesSketchGetPmf(sketchExpr: Expression,
                                  splitPointsExpr: Expression,
                                  isInclusiveExpr: Expression)
    extends RuntimeReplaceable
    with ImplicitCastInputTypes
    with TernaryLike[Expression] {

    def this(sketchExpr: Expression, splitPointsExpr: Expression) = {
        this(sketchExpr, splitPointsExpr, Literal(true))
    }

    override def first: Expression = sketchExpr
    override def second: Expression = splitPointsExpr
    override def third: Expression = isInclusiveExpr

    override lazy val replacement: Expression = KllDoublesSketchGetPmfCdf(sketchExpr, splitPointsExpr, isInclusiveExpr, true)
    override def inputTypes: Seq[AbstractDataType] = Seq(KllDoublesSketchType, ArrayType(DoubleType), BooleanType)
    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = {
        copy(sketchExpr = newFirst, splitPointsExpr = newSecond, isInclusiveExpr = newThird)
    }
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr, expr, isInclusive) - Returns an approximation to the PMF
      of the given KllDoublesSketch using the specified search criteria (default: inclusive, isInclusive = true)
      or exclusive using the given split points.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg(col), array(1.5, 3.5)) FROM VALUES (1.0), (2.0), (3.0) tab(col);
       [0.3333333333333333, 0.6666666666666666, 0.0]
  """
)
case class KllDoublesSketchGetCdf(sketchExpr: Expression,
                                  splitPointsExpr: Expression,
                                  isInclusiveExpr: Expression)
    extends RuntimeReplaceable
    with ImplicitCastInputTypes
    with TernaryLike[Expression] {

    def this(sketchExpr: Expression, splitPointsExpr: Expression) = {
        this(sketchExpr, splitPointsExpr, Literal(true))
    }

    override def first: Expression = sketchExpr
    override def second: Expression = splitPointsExpr
    override def third: Expression = isInclusiveExpr

    override lazy val replacement: Expression = KllDoublesSketchGetPmfCdf(sketchExpr, splitPointsExpr, isInclusiveExpr, false)
    override def inputTypes: Seq[AbstractDataType] = Seq(KllDoublesSketchType, ArrayType(DoubleType), BooleanType)
    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = {
        copy(sketchExpr = newFirst, splitPointsExpr = newSecond, isInclusiveExpr = newThird)
    }
}

/**
  * Returns the PMF and CDF of the given quantile search criteria.
  *
  * @param sketchExpr A KllDoublesSketch sketch, in serialized form
  * @param splitPointsExpr An array of split points, as doubles
  * @param isInclusiveExpr A boolean flag for inclusive mode. If true, use INCLUSIVE else EXCLUSIVE
  * @param isPmf Whether to return the PMF (true) or CDF (false)
  */
@ExpressionDescription(
  usage = """
    _FUNC_(expr, expr, isInclusive, isPmf) - Returns an approximation to the PMF or CDF (default: isPmf = false)
      of the given KllDoublesSketch using the specified search criteria (default: inclusive, isInclusive = true)
      or exclusive using the given split points.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg(col), array(1.5, 3.5), true, true) FROM VALUES (1.0), (2.0), (3.0) tab(col);
       [0.3333333333333333, 0.6666666666666666, 0.0]
  """
)
case class KllDoublesSketchGetPmfCdf(sketchExpr: Expression,
                                     splitPointsExpr: Expression,
                                     isInclusiveExpr: Expression,
                                     isPmf: Boolean = false)
 extends TernaryExpression
 with ExpectsInputTypes
 with NullIntolerant
 with ImplicitCastInputTypes {

  lazy val isInclusive = third.eval().asInstanceOf[Boolean]

  override def first: Expression = sketchExpr
  override def second: Expression = splitPointsExpr
  override def third: Expression = isInclusiveExpr

  override protected def withNewChildrenInternal(newFirst: Expression,
                                                 newSecond: Expression,
                                                 newThird: Expression) = {
    copy(sketchExpr = newFirst, splitPointsExpr = newSecond, isInclusiveExpr = newThird, isPmf = isPmf)
  }

  override def prettyName: String = "kll_sketch_double_get_pmf_cdf"

  override def inputTypes: Seq[AbstractDataType] = Seq(KllDoublesSketchType, ArrayType(DoubleType), BooleanType)

  override def dataType: DataType = ArrayType(DoubleType, containsNull = false)

  // letting underlying library validate input types, not defining checkInputDataTypes() here
  override def checkInputDataTypes(): TypeCheckResult = {
    // splitPints and isInclusive must be a constant
    if (!splitPointsExpr.foldable) {
      return TypeCheckResult.TypeCheckFailure(s"splitPointsExpr must be foldable, but got: ${splitPointsExpr}")
    }
    if (!isInclusiveExpr.foldable) {
      return TypeCheckResult.TypeCheckFailure(s"isInclusiveExpr must be foldable, but got: ${isInclusiveExpr}")
    }
    if (splitPointsExpr.eval().asInstanceOf[GenericArrayData].numElements() == 0) {
      return TypeCheckResult.TypeCheckFailure(s"splitPointsExpr must not be empty")
    }

    TypeCheckResult.TypeCheckSuccess
  }

  override def nullSafeEval(sketchInput: Any, splitPointsInput: Any, isInclusiveInput: Any): Any = {
    val sketchBytes = sketchInput.asInstanceOf[Array[Byte]]
    val splitPoints = splitPointsInput.asInstanceOf[GenericArrayData].toDoubleArray()
    val sketch = KllDoublesSketch.wrap(Memory.wrap(sketchBytes))

    val result: Array[Double] =
      if (isPmf) {
        sketch.getPMF(splitPoints, if (isInclusive) QuantileSearchCriteria.INCLUSIVE else QuantileSearchCriteria.EXCLUSIVE)
      } else {
        sketch.getCDF(splitPoints, if (isInclusive) QuantileSearchCriteria.INCLUSIVE else QuantileSearchCriteria.EXCLUSIVE)
      }
    new GenericArrayData(result)
  }

  override protected def nullSafeCodeGen(ctx: CodegenContext, ev: ExprCode, f: (String, String, String) => String): ExprCode = {
    val sketchEval = sketchExpr.genCode(ctx)
    val splitPointsEval = splitPointsExpr.genCode(ctx)
    val sketch = ctx.freshName("sketch")
    val searchCriterion = ctx.freshName("searchCriterion")
    val splitPoints = ctx.freshName("splitPoints")
    val result = ctx.freshName("result")

    val code =
      s"""
         |${sketchEval.code}
         |${splitPointsEval.code}
         |org.apache.datasketches.quantilescommon.QuantileSearchCriteria $searchCriterion = ${if (isInclusive) "org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE" else "org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE"};
         |final org.apache.datasketches.kll.KllDoublesSketch $sketch = org.apache.spark.sql.datasketches.kll.types.KllDoublesSketchType.wrap(${sketchEval.value});
         |final double[] $splitPoints = ((org.apache.spark.sql.catalyst.util.GenericArrayData)${splitPointsEval.value}).toDoubleArray();
         |final double[] $result = ${if (isPmf) s"$sketch.getPMF($splitPoints, $searchCriterion)" else s"$sketch.getCDF($splitPoints, $searchCriterion)"};
         |final boolean ${ev.isNull} = false;
         |org.apache.spark.sql.catalyst.util.GenericArrayData ${ev.value} = new org.apache.spark.sql.catalyst.util.GenericArrayData($result);
       """.stripMargin
    ev.copy(code = CodeBlock(Seq(code), Seq.empty))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (arg1, arg2, arg3) => s"($arg1, $arg2, $arg3)")
  }
}

// default search criteria = inclusive
// getQuantile(rank, QuantileSearchCriteria)
// getQuantileLowerBound(rank)
// getQuantileUpperBound(rank)
// getQuantiles(double ranks[], QuantileSearchCriteria)
// getRank(quantile, QuantileSearchCriteria)
// getRanks(quantile[]), QuantileSearchCriteria)
// getNormalizedRankError(bool isPmf)
// toString(bool, bool) -- already part of the wrapper
// getK() ?
// getNumRetained() ?
