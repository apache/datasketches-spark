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

package org.apache.spark.sql


import org.apache.spark.sql.aggregate.{KllDoublesSketchAgg, KllDoublesMergeAgg}
import org.apache.spark.sql.aggregate.{ThetaSketchBuild, ThetaUnion}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{ArrayType, DoubleType}

// this class defines and maps all the variants of each function invocation, analagous
// to the functions object in org.apache.spark.sql.functions
object functions_ds {

  private def withExpr(expr: => Expression): Column = Column(expr)

  private def withAggregateFunction(func: AggregateFunction): Column = {
    Column(func.toAggregateExpression())
  }

  // get min
  def kll_get_min(expr: Column): Column = withExpr {
    new KllGetMin(expr.expr)
  }

  def kll_get_min(columnName: String): Column = {
    kll_get_min(Column(columnName))
  }

  // get max
  def kll_get_max(expr: Column): Column = withExpr {
    new KllGetMax(expr.expr)
  }

  def kll_get_max(columnName: String): Column = {
    kll_get_max(Column(columnName))
  }

  // build sketch
  def kll_sketch_agg(expr: Column, k: Column): Column = withAggregateFunction {
    new KllDoublesSketchAgg(expr.expr, k.expr)
  }

  def kll_sketch_agg(expr: Column, k: Int): Column = {
    kll_sketch_agg(expr, lit(k))
  }

  def kll_sketch_agg(columnName: String, k: Int): Column = {
    kll_sketch_agg(Column(columnName), k)
  }

  def kll_sketch_agg(expr: Column): Column = withAggregateFunction {
    new KllDoublesSketchAgg(expr.expr)
  }

  def kll_sketch_agg(columnName: String): Column = {
    kll_sketch_agg(Column(columnName))
  }

  // merge sketches
  def kll_merge_agg(expr: Column): Column = withAggregateFunction {
    new KllDoublesMergeAgg(expr.expr)
  }

  def kll_merge_agg(columnName: String): Column = {
    kll_merge_agg(Column(columnName))
  }

  // get PMF
  def kll_get_pmf(sketch: Column, splitPoints: Column, isInclusive: Boolean): Column = withExpr {
    new KllGetPmfCdf(sketch.expr, splitPoints.expr, isInclusive, true)
  }

  def kll_get_pmf(sketch: Column, splitPoints: Column): Column = withExpr {
    new KllGetPmfCdf(sketch.expr, splitPoints.expr, true, true)
  }

  def kll_get_pmf(columnName: String, splitPoints: Column, isInclusive: Boolean): Column = {
    kll_get_pmf(Column(columnName), splitPoints, isInclusive)
  }

  def kll_get_pmf(columnName: String, splitPoints: Column): Column = {
    kll_get_pmf(Column(columnName), splitPoints)
  }

  def kll_get_pmf(sketch: Column, splitPoints: Array[Double], isInclusive: Boolean): Column = {
    kll_get_pmf(sketch, Column(Literal.create(splitPoints, ArrayType(DoubleType))), isInclusive)
  }

  def kll_get_pmf(sketch: Column, splitPoints: Array[Double]): Column = {
    kll_get_pmf(sketch, Column(Literal.create(splitPoints, ArrayType(DoubleType))))
  }

  def kll_get_pmf(columnName: String, splitPoints: Array[Double], isInclusive: Boolean): Column = {
    kll_get_pmf(Column(columnName), splitPoints, isInclusive)
  }

  def kll_get_pmf(columnName: String, splitPoints: Array[Double]): Column = {
    kll_get_pmf(Column(columnName), splitPoints)
  }


  // get CDF
  def kll_get_cdf(sketch: Column, splitPoints: Column, isInclusive: Boolean): Column = withExpr {
    new KllGetPmfCdf(sketch.expr, splitPoints.expr, isInclusive, false)
  }

  def kll_get_cdf(sketch: Column, splitPoints: Column): Column = withExpr {
    new KllGetPmfCdf(sketch.expr, splitPoints.expr, true, false)
  }

  def kll_get_cdf(columnName: String, splitPoints: Column, isInclusive: Boolean): Column = {
    kll_get_cdf(Column(columnName), splitPoints, isInclusive)
  }

  def kll_get_cdf(columnName: String, splitPoints: Column): Column = {
    kll_get_cdf(Column(columnName), splitPoints)
  }

  def kll_get_cdf(sketch: Column, splitPoints: Array[Double], isInclusive: Boolean): Column = {
    kll_get_cdf(sketch, Column(Literal.create(splitPoints, ArrayType(DoubleType))), isInclusive)
  }

  def kll_get_cdf(sketch: Column, splitPoints: Array[Double]): Column = {
    kll_get_cdf(sketch, Column(Literal.create(splitPoints, ArrayType(DoubleType))))
  }

  def kll_get_cdf(columnName: String, splitPoints: Array[Double], isInclusive: Boolean): Column = {
    kll_get_cdf(Column(columnName), splitPoints, isInclusive)
  }

  def kll_get_cdf(columnName: String, splitPoints: Array[Double]): Column = {
    kll_get_cdf(Column(columnName), splitPoints)
  }

  // Theta

  def theta_sketch_build(column: Column, lgk: Int): Column = withAggregateFunction {
    new ThetaSketchBuild(column.expr, lgk)
  }

  def theta_sketch_build(columnName: String, lgk: Int): Column = {
    theta_sketch_build(Column(columnName), lgk)
  }

  def theta_sketch_build(column: Column): Column = withAggregateFunction {
    new ThetaSketchBuild(column.expr)
  }

  def theta_sketch_build(columnName: String): Column = {
    theta_sketch_build(Column(columnName))
  }

  def theta_union(column: Column, lgk: Int): Column = withAggregateFunction {
    new ThetaUnion(column.expr, lit(lgk).expr)
  }

  def theta_union(columnName: String, lgk: Int): Column = withAggregateFunction {
    new ThetaUnion(Column(columnName).expr, lit(lgk).expr)
  }

  def theta_union(column: Column): Column = withAggregateFunction {
    new ThetaUnion(column.expr)
  }

  def theta_union(columnName: String): Column = withAggregateFunction {
    new ThetaUnion(Column(columnName).expr)
  }

  def theta_sketch_get_estimate(column: Column): Column = withExpr {
    new ThetaSketchGetEstimate(column.expr)
  }

  def theta_sketch_get_estimate(columnName: String): Column = {
    theta_sketch_get_estimate(Column(columnName))
  }
}
