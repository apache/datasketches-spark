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

import org.apache.spark.sql.catalyst.expressions.{Expression,
                                                  ExpressionDescription,
                                                  UnaryExpression,
                                                  ExpectsInputTypes,
                                                  NullIntolerant}
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeBlock, CodegenContext, ExprCode}
import org.apache.spark.sql.datasketches.kll.types.KllDoublesSketchType
import org.apache.datasketches.kll.KllDoublesSketch
import org.apache.datasketches.memory.Memory
import org.apache.spark.unsafe.types.UTF8String

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns a string with information about the sketch given the binary representation
    of a Datasketches KllDoublesSketch. """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg(col)) FROM VALUES (1.0), (2.0), (3.0) tab(col);
      ### KLL sketch summary:
        K              : 200
        min K          : 200
        M              : 8
        N              : 3
        Epsilon        : 1.33%
        Epsilon PMF    : 1.65%
        Empty          : false
        Estimation mode: false
        Levels         : 1
        Sorted         : false
        Capacity items : 200
        Retained items : 3
        Min item      : 1
        Max item      : 3
      ### End sketch summary
  """
  //group = "misc_funcs",
)
case class KllDoublesSketchToString(sketchExpr: Expression)
 extends UnaryExpression
 with ExpectsInputTypes
 with NullIntolerant {

  override def child: Expression = sketchExpr

  override protected def withNewChildInternal(newChild: Expression): KllDoublesSketchToString = {
    copy(sketchExpr = newChild)
  }

  override def prettyName: String = "kll_sketch_double_to_string"

  override def inputTypes: Seq[AbstractDataType] = Seq(KllDoublesSketchType)

  override def dataType: DataType = StringType

  override def nullSafeEval(input: Any): Any = {
    val bytes = input.asInstanceOf[Array[Byte]]
    val sketch = KllDoublesSketch.wrap(Memory.wrap(bytes))
    UTF8String.fromString(sketch.toString())
  }

  override protected def nullSafeCodeGen(ctx: CodegenContext, ev: ExprCode, f: String => String): ExprCode = {
    val sketchEval = child.genCode(ctx)
    val sketch = ctx.freshName("sketch")

    val code =
      s"""
         |${sketchEval.code}
         |final org.apache.datasketches.kll.KllDoublesSketch $sketch = org.apache.spark.sql.datasketches.kll.types.KllDoublesSketchType.wrap(${sketchEval.value});
         |final org.apache.spark.unsafe.types.UTF8String ${ev.value} = org.apache.spark.unsafe.types.UTF8String.fromString($sketch.toString());
         |final boolean ${ev.isNull} = ${sketchEval.isNull};
       """.stripMargin
    ev.copy(code = CodeBlock(Seq(code), Seq.empty))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"($c)")
  }
}
