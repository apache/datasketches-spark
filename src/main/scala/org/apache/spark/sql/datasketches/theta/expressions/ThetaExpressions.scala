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

package org.apache.spark.sql.datasketches.theta.expressions

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpectsInputTypes, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.NullIntolerant
import org.apache.spark.sql.catalyst.expressions.ExpressionDescription
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeBlock, CodegenContext, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, DataType, DoubleType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.spark.sql.datasketches.theta.types.ThetaSketchType

import org.apache.datasketches.memory.Memory
import org.apache.datasketches.theta.Sketch

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns distinct count estimate from a given sketch
  """,
  examples = """
    Example:
      > SELECT _FUNC_(theta_sketch_agg_build(col)) FROM VALUES (1), (2), (3) tab(col);
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
    Sketch.wrap(Memory.wrap(input.asInstanceOf[Array[Byte]])).getEstimate()
  }

  override protected def nullSafeCodeGen(ctx: CodegenContext, ev: ExprCode, f: String => String): ExprCode = {
    val childEval = child.genCode(ctx)
    val sketch = ctx.freshName("sketch")
    val code = s"""
      ${childEval.code}
      if (${childEval.isNull}) {
        ${ev.value} = null;
      } else {
        final org.apache.datasketches.theta.Sketch $sketch = org.apache.spark.sql.datasketches.theta.types.ThetaSketchWrapper.wrapAsReadOnlySketch(${childEval.value});
        ${ev.value} = $sketch.getEstimate();
      }
    """
    ev.copy(code = CodeBlock(Seq(code), Seq.empty), isNull = childEval.isNull)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"($c)")
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns a summary string that represents the state of the given sketch
  """,
  examples = """
    Example:
      > SELECT _FUNC_(theta_sketch_agg_build(col)) FROM VALUES (1), (2), (3) tab(col);
      ### HeapCompactSketch SUMMARY:
         Estimate                : 3.0
         Upper Bound, 95% conf   : 3.0
         Lower Bound, 95% conf   : 3.0
         Theta (double)          : 1.0
         Theta (long)            : 9223372036854775807
         Theta (long) hex        : 7fffffffffffffff
         EstMode?                : false
         Empty?                  : false
         Ordered?                : true
         Retained Entries        : 3
         Seed Hash               : 93cc | 37836
      ### END SKETCH SUMMARY
  """
)
case class ThetaSketchToString(child: Expression)
 extends UnaryExpression
 with ExpectsInputTypes
 with NullIntolerant {

  override protected def withNewChildInternal(newChild: Expression): ThetaSketchToString = {
    copy(child = newChild)
  }

  override def prettyName: String = "theta_sketch_to_string"

  override def inputTypes: Seq[AbstractDataType] = Seq(ThetaSketchType)

  override def dataType: DataType = StringType

  override def nullSafeEval(input: Any): Any = {
    UTF8String.fromString(Sketch.wrap(Memory.wrap(input.asInstanceOf[Array[Byte]])).toString());
  }

  override protected def nullSafeCodeGen(ctx: CodegenContext, ev: ExprCode, f: String => String): ExprCode = {
    val childEval = child.genCode(ctx)
    val sketch = ctx.freshName("sketch")
    val code = s"""
      ${childEval.code}
      final org.apache.datasketches.theta.Sketch $sketch = org.apache.spark.sql.datasketches.theta.types.ThetaSketchWrapper.wrapAsReadOnlySketch(${childEval.value});
      final org.apache.spark.unsafe.types.UTF8String ${ev.value} = org.apache.spark.unsafe.types.UTF8String.fromString($sketch.toString());
    """
    ev.copy(code = CodeBlock(Seq(code), Seq.empty), isNull = childEval.isNull)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"($c)")
  }
}
