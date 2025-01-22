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

import org.apache.spark.sql.functions.lit

import org.apache.spark.sql.aggregate.{ThetaSketchAggBuild, ThetaSketchAggUnion}
import org.apache.spark.sql.expressions.ThetaSketchGetEstimate

object functions_datasketches_theta extends DatasketchesScalaFunctionBase {
  def theta_sketch_agg_build(column: Column, lgk: Int): Column = withAggregateFunction {
    new ThetaSketchAggBuild(column.expr, lgk)
  }

  def theta_sketch_agg_build(columnName: String, lgk: Int): Column = {
    theta_sketch_agg_build(Column(columnName), lgk)
  }

  def theta_sketch_agg_build(column: Column): Column = withAggregateFunction {
    new ThetaSketchAggBuild(column.expr)
  }

  def theta_sketch_agg_build(columnName: String): Column = {
    theta_sketch_agg_build(Column(columnName))
  }

  def theta_sketch_agg_union(column: Column, lgk: Int): Column = withAggregateFunction {
    new ThetaSketchAggUnion(column.expr, lit(lgk).expr)
  }

  def theta_sketch_agg_union(columnName: String, lgk: Int): Column = withAggregateFunction {
    new ThetaSketchAggUnion(Column(columnName).expr, lit(lgk).expr)
  }

  def theta_sketch_agg_union(column: Column): Column = withAggregateFunction {
    new ThetaSketchAggUnion(column.expr)
  }

  def theta_sketch_agg_union(columnName: String): Column = withAggregateFunction {
    new ThetaSketchAggUnion(Column(columnName).expr)
  }

  def theta_sketch_get_estimate(column: Column): Column = withExpr {
    new ThetaSketchGetEstimate(column.expr)
  }

  def theta_sketch_get_estimate(columnName: String): Column = {
    theta_sketch_get_estimate(Column(columnName))
  }
}
