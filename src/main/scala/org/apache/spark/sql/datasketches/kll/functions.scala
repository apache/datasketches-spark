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

package org.apache.spark.sql.datasketches.kll

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType}

import org.apache.spark.sql.datasketches.common.DatasketchesScalaFunctionBase
import org.apache.spark.sql.datasketches.kll.aggregate.{KllDoublesSketchAggMerge, KllDoublesSketchAggBuild}
import org.apache.spark.sql.datasketches.kll.expressions.{KllDoublesSketchGetMin,
                                                          KllDoublesSketchGetMax,
                                                          KllDoublesSketchGetPmfCdf,
                                                          KllDoublesSketchGetNumRetained,
                                                          KllDoublesSketchGetK,
                                                          KllDoublesSketchIsEstimationMode}

object functions extends DatasketchesScalaFunctionBase {

  // build sketch
  def kll_sketch_double_agg_build(expr: Column, k: Column): Column = withAggregateFunction {
    new KllDoublesSketchAggBuild(expr.expr, k.expr)
  }

  def kll_sketch_double_agg_build(expr: Column, k: Int): Column = {
    kll_sketch_double_agg_build(expr, lit(k))
  }

  def kll_sketch_double_agg_build(columnName: String, k: Int): Column = {
    kll_sketch_double_agg_build(Column(columnName), k)
  }

  def kll_sketch_double_agg_build(expr: Column): Column = withAggregateFunction {
    new KllDoublesSketchAggBuild(expr.expr)
  }

  def kll_sketch_double_agg_build(columnName: String): Column = {
    kll_sketch_double_agg_build(Column(columnName))
  }

  // merge sketches
  def kll_sketch_double_agg_merge(expr: Column): Column = withAggregateFunction {
    new KllDoublesSketchAggMerge(expr.expr)
  }

  def kll_sketch_double_agg_merge(columnName: String): Column = {
    kll_sketch_double_agg_merge(Column(columnName))
  }

  def kll_sketch_double_agg_merge(expr: Column, k: Column): Column = withAggregateFunction {
    new KllDoublesSketchAggMerge(expr.expr, k.expr)
  }

  def kll_sketch_double_agg_merge(expr: Column, k: Int): Column = withAggregateFunction {
    new KllDoublesSketchAggMerge(expr.expr, lit(k).expr)
  }

  def kll_sketch_double_agg_merge(columnName: String, k: Int): Column = {
    kll_sketch_double_agg_merge(Column(columnName), lit(k))
  }

  // get k
  def kll_sketch_double_get_k(expr: Column): Column = withExpr {
    new KllDoublesSketchGetK(expr.expr)
  }

  def kll_sketch_double_get_k(columnName: String): Column = {
    kll_sketch_double_get_k(Column(columnName))
  }

  // get num retained
  def kll_sketch_double_get_num_retained(expr: Column): Column = withExpr {
    new KllDoublesSketchGetNumRetained(expr.expr)
  }

  def kll_sketch_double_get_num_retained(columnName: String): Column = {
    kll_sketch_double_get_num_retained(Column(columnName))
  }

  // is estimation mode
  def kll_sketch_double_is_estimation_mode(expr: Column): Column = withExpr {
    new KllDoublesSketchIsEstimationMode(expr.expr)
  }

  def kll_sketch_double_is_estimation_mode(columnName: String): Column = {
    kll_sketch_double_is_estimation_mode(Column(columnName))
  }

  // get min
  def kll_sketch_double_get_min(expr: Column): Column = withExpr {
    new KllDoublesSketchGetMin(expr.expr)
  }

  def kll_sketch_double_get_min(columnName: String): Column = {
    kll_sketch_double_get_min(Column(columnName))
  }

  // get max
  def kll_sketch_double_get_max(expr: Column): Column = withExpr {
    new KllDoublesSketchGetMax(expr.expr)
  }

  def kll_sketch_double_get_max(columnName: String): Column = {
    kll_sketch_double_get_max(Column(columnName))
  }

  // get PMF
  def kll_sketch_double_get_pmf(sketch: Column, splitPoints: Column, isInclusive: Boolean): Column = withExpr {
    new KllDoublesSketchGetPmfCdf(sketch.expr, splitPoints.expr, Literal.create(isInclusive, BooleanType), true)
  }

  def kll_sketch_double_get_pmf(sketch: Column, splitPoints: Column): Column = withExpr {
    new KllDoublesSketchGetPmfCdf(sketch.expr, splitPoints.expr, Literal(true), true)
  }

  def kll_sketch_double_get_pmf(columnName: String, splitPoints: Column, isInclusive: Boolean): Column = {
    kll_sketch_double_get_pmf(Column(columnName), splitPoints, isInclusive)
  }

  def kll_sketch_double_get_pmf(columnName: String, splitPoints: Column): Column = {
    kll_sketch_double_get_pmf(Column(columnName), splitPoints)
  }

  def kll_sketch_double_get_pmf(sketch: Column, splitPoints: Array[Double], isInclusive: Boolean): Column = {
    kll_sketch_double_get_pmf(sketch, Column(Literal.create(splitPoints, ArrayType(DoubleType))), isInclusive)
  }

  def kll_sketch_double_get_pmf(sketch: Column, splitPoints: Array[Double]): Column = {
    kll_sketch_double_get_pmf(sketch, Column(Literal.create(splitPoints, ArrayType(DoubleType))))
  }

  def kll_sketch_double_get_pmf(columnName: String, splitPoints: Array[Double], isInclusive: Boolean): Column = {
    kll_sketch_double_get_pmf(Column(columnName), splitPoints, isInclusive)
  }

  def kll_sketch_double_get_pmf(columnName: String, splitPoints: Array[Double]): Column = {
    kll_sketch_double_get_pmf(Column(columnName), splitPoints)
  }


  // get CDF
  def kll_sketch_double_get_cdf(sketch: Column, splitPoints: Column, isInclusive: Boolean): Column = withExpr {
    new KllDoublesSketchGetPmfCdf(sketch.expr, splitPoints.expr, Literal.create(isInclusive, BooleanType), false)
  }

  def kll_sketch_double_get_cdf(sketch: Column, splitPoints: Column): Column = withExpr {
    new KllDoublesSketchGetPmfCdf(sketch.expr, splitPoints.expr, Literal(true), false)
  }

  def kll_sketch_double_get_cdf(columnName: String, splitPoints: Column, isInclusive: Boolean): Column = {
    kll_sketch_double_get_cdf(Column(columnName), splitPoints, isInclusive)
  }

  def kll_sketch_double_get_cdf(columnName: String, splitPoints: Column): Column = {
    kll_sketch_double_get_cdf(Column(columnName), splitPoints)
  }

  def kll_sketch_double_get_cdf(sketch: Column, splitPoints: Array[Double], isInclusive: Boolean): Column = {
    kll_sketch_double_get_cdf(sketch, Column(Literal.create(splitPoints, ArrayType(DoubleType))), isInclusive)
  }

  def kll_sketch_double_get_cdf(sketch: Column, splitPoints: Array[Double]): Column = {
    kll_sketch_double_get_cdf(sketch, Column(Literal.create(splitPoints, ArrayType(DoubleType))))
  }

  def kll_sketch_double_get_cdf(columnName: String, splitPoints: Array[Double], isInclusive: Boolean): Column = {
    kll_sketch_double_get_cdf(Column(columnName), splitPoints, isInclusive)
  }

  def kll_sketch_double_get_cdf(columnName: String, splitPoints: Array[Double]): Column = {
    kll_sketch_double_get_cdf(Column(columnName), splitPoints)
  }
}
