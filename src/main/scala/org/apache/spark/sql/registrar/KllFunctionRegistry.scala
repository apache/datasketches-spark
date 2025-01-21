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

package org.apache.spark.sql.registrar

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{ExpressionInfo}

import org.apache.spark.sql.aggregate.{KllDoublesSketchAgg, KllDoublesSketchMergeAgg}
import org.apache.spark.sql.expressions.{KllDoublesSketchGetMin, KllDoublesSketchGetMax, KllDoublesSketchGetPmf, KllDoublesSketchGetCdf}

object KllFunctionRegistry extends DatasketchesFunctionRegistry {
  override val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    expression[KllDoublesSketchAgg]("kll_sketch_double_agg_build"),
    expression[KllDoublesSketchMergeAgg]("kll_sketch_double_agg_merge"),
    expression[KllDoublesSketchGetMin]("kll_sketch_double_get_min"),
    expression[KllDoublesSketchGetMax]("kll_sketch_double_get_max"),
    expression[KllDoublesSketchGetPmf]("kll_sketch_double_get_pmf"),
    expression[KllDoublesSketchGetCdf]("kll_sketch_double_get_cdf"),
  )
}
