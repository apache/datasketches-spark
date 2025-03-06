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
import org.apache.spark.sql.types.{AbstractDataType, DataType, DoubleType}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeBlock, CodegenContext, ExprCode}
import org.apache.spark.sql.datasketches.kll.types.KllDoublesSketchType
import org.apache.datasketches.kll.KllDoublesSketch
import org.apache.datasketches.memory.Memory

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the configured size-accuracy parameter k given the binary representation
    of a Datasketches KllDoublesSketch. """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg(col, 200)) FROM VALUES (1.0), (2.0), (3.0) tab(col);
       200
  """
  //group = "misc_funcs",
)
case class KllDoublesSketchGetK(sketchExpr: Expression)
 extends UnaryExpression
 with ExpectsInputTypes
 with NullIntolerant {

  override def child: Expression = sketchExpr

  override protected def withNewChildInternal(newChild: Expression): KllDoublesSketchGetK = {
    copy(sketchExpr = newChild)
  }

  override def prettyName: String = "kll_sketch_double_get_k"

  override def inputTypes: Seq[AbstractDataType] = Seq(KllDoublesSketchType)

  override def dataType: DataType = DoubleType

  override def nullSafeEval(input: Any): Any = {
    val bytes = input.asInstanceOf[Array[Byte]]
    val sketch = KllDoublesSketch.wrap(Memory.wrap(bytes))
    sketch.getK()
  }

  override protected def nullSafeCodeGen(ctx: CodegenContext, ev: ExprCode, f: String => String): ExprCode = {
    val sketchEval = child.genCode(ctx)
    val sketch = ctx.freshName("sketch")

    val code =
      s"""
         |${sketchEval.code}
         |final org.apache.datasketches.kll.KllDoublesSketch $sketch = org.apache.spark.sql.datasketches.kll.types.KllDoublesSketchType.wrap(${sketchEval.value});
         |final int ${ev.value} = $sketch.getK();
         |final boolean ${ev.isNull} = ${sketchEval.isNull};
       """.stripMargin
    ev.copy(code = CodeBlock(Seq(code), Seq.empty))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"($c)")
  }
}
