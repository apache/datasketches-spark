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

package org.apache.spark.sql.datasketches.theta

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{ExpressionInfo}

import org.apache.spark.sql.datasketches.common.DatasketchesFunctionRegistry
import org.apache.spark.sql.datasketches.theta.aggregate.{ThetaSketchAggBuild, ThetaSketchAggUnion}
import org.apache.spark.sql.datasketches.theta.expressions.{ThetaSketchGetEstimate, ThetaSketchToString}
import org.apache.spark.sql.datasketches.common.DatasketchesFunctionRegistry

object ThetaFunctionRegistry extends DatasketchesFunctionRegistry {
  override val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    expression[ThetaSketchAggBuild]("theta_sketch_agg_build"),
    expression[ThetaSketchAggUnion]("theta_sketch_agg_union"),
    expression[ThetaSketchGetEstimate]("theta_sketch_get_estimate"),
    expression[ThetaSketchToString]("theta_sketch_to_string")
  )
}
