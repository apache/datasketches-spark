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

package org.apache.spark.sql.expressions

import org.apache.datasketches.memory.Memory
import org.apache.datasketches.theta.Sketch

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpectsInputTypes, UnaryExpression, BinaryExpression}
import org.apache.spark.sql.catalyst.expressions.NullIntolerant
import org.apache.spark.sql.catalyst.expressions.ExpressionDescription
import org.apache.spark.sql.catalyst.expressions.ImplicitCastInputTypes
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeBlock, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{AbstractDataType, DataType, ArrayType, DoubleType, ThetaSketchType}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns distinct count estimate from a given sketch
  """,
  examples = """
    Example:
      > SELECT _FUNC_(theta_sketch_build(col)) FROM VALUES (1), (2), (3) tab(col);
       3.0
  """
)
case class ThetaSketchGetEstimate(child: Expression)
 extends UnaryExpression
 with ExpectsInputTypes
 with NullIntolerant {

  override protected def withNewChildInternal(newChild: Expression): ThetaSketchGetEstimate = {
    copy(child = newChild)
  }

  override def prettyName: String = "theta_sketch_get_estimate"

  override def inputTypes: Seq[AbstractDataType] = Seq(ThetaSketchType)

  override def dataType: DataType = DoubleType

  override def nullSafeEval(input: Any): Any = {
    Sketch.wrap(Memory.wrap(input.asInstanceOf[Array[Byte]])).getEstimate
  }

  override protected def nullSafeCodeGen(ctx: CodegenContext, ev: ExprCode, f: String => String): ExprCode = {
    val childEval = child.genCode(ctx)
    val sketch = ctx.freshName("sketch")
    val code = s"""
      ${childEval.code}
      final org.apache.datasketches.theta.Sketch $sketch = org.apache.spark.sql.types.ThetaSketchWrapper.wrapAsReadOnlySketch(${childEval.value});
      final double ${ev.value} = $sketch.getEstimate();
    """
    ev.copy(code = CodeBlock(Seq(code), Seq.empty), isNull = childEval.isNull)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"($c)")
  }
}
