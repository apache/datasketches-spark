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

package org.apache.spark.sql.datasketches.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}

import scala.reflect.ClassTag

// based on org.apache.spark.sql.catalyst.FunctionRegistry
trait DatasketchesFunctionRegistry {
  // override this to define the actual functions
  val expressions: Map[String, (ExpressionInfo, FunctionBuilder)]

  // registers all the functions in the expressions Map
  def registerFunctions(spark: SparkSession): Unit = {
    expressions.foreach { case (name, (info, builder)) =>
      spark.sessionState.functionRegistry.registerFunction(FunctionIdentifier(name), info, builder)
    }
  }

  // simplifies defining the expression (ignoring "since" as a stand-alone library)
  protected def expression[T <: Expression : ClassTag](name: String): (String, (ExpressionInfo, FunctionBuilder)) = {
    val (expressionInfo, builder) = FunctionRegistryBase.build[T](name, None)
    (name, (expressionInfo, builder))
  }

  // some functions throw a query compile-time exception around the wrong
  // number of parameters when using expression(). This function allows
  // explicit argument handling by providing a lambda to use for the bulder.
  // This seems to be related to non-Expression inputs to the classes, but keeping
  // this an an example of usage for now in case it really is needed:
  //    complexExpression[KllGetPmfCdf]("kll_get_cdf") { args: Seq[Expression] =>
  //      val isInclusive = if (args.length > 2) args(2).eval().asInstanceOf[Boolean] else true
  //      new KllGetPmfCdf(args(0), args(1), isInclusive = isInclusive, isPmf = false)
  //    }
  protected def complexExpression[T <: Expression : ClassTag](name: String)(f: (Seq[Expression]) => T): (String, (ExpressionInfo, FunctionBuilder)) = {
    val expressionInfo = FunctionRegistryBase.expressionInfo[T](name, None)
    val builder: FunctionBuilder = (args: Seq[Expression]) => f(args)
    (name, (expressionInfo, builder))
  }
}
