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

import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, BinaryType, DataType}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpectsInputTypes}
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeBlock, CodegenContext, ExprCode}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the input as a BinaryType (Array[Byte]). """
  //group = "misc_funcs",
)
case class CastToBinary(sketchExpr: Expression)
 extends UnaryExpression
 with ExpectsInputTypes {

  override def child: Expression = sketchExpr

  override protected def withNewChildInternal(newChild: Expression): CastToBinary = {
    copy(sketchExpr = newChild)
  }

  override def prettyName: String = "cast_to_binary"

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def dataType: DataType = BinaryType

  override def nullSafeEval(input: Any): Any = {
    input.asInstanceOf[Array[Byte]]
  }

  override protected def nullSafeCodeGen(ctx: CodegenContext, ev: ExprCode, f: String => String): ExprCode = {
    val sketchEval = child.genCode(ctx)

    val code =
      s"""
         |${sketchEval.code}
         |final byte[] ${ev.value} = ${sketchEval.value};
         |final boolean ${ev.isNull} = ${sketchEval.isNull};
       """.stripMargin
    ev.copy(code = CodeBlock(Seq(code), Seq.empty))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"($c)")
  }
}
