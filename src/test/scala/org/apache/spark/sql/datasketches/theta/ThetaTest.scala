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

package org.apache.spark.sql.theta

import org.apache.spark.sql.Row
import org.apache.spark.sql.datasketches.common.SparkSessionManager
import org.apache.spark.sql.datasketches.theta.functions._
import org.apache.spark.sql.datasketches.theta.ThetaFunctionRegistry

import org.scalatest.matchers.should.Matchers._

class ThetaTest extends SparkSessionManager {
  import spark.implicits._

  test("Theta Sketch build via Scala with defaults") {
    val n = 100
    val data = (for (i <- 1 to n) yield i).toDF("value")

    val sketchDf = data.agg(theta_sketch_agg_build("value").as("sketch"))
    val result: Row = sketchDf.select(theta_sketch_get_estimate("sketch").as("estimate")).head()

    assert(result.getAs[Double]("estimate") == 100.0)
  }

  test("Theta Sketch build via Scala with lgk") {
    val n = 100
    val data = (for (i <- 1 to n) yield i).toDF("value")

    val sketchDf = data.agg(theta_sketch_agg_build("value", 14).as("sketch"))
    val result: Row = sketchDf.select(theta_sketch_get_estimate("sketch").as("estimate")).head()

    assert(result.getAs[Double]("estimate") == 100.0)
  }

  test("Theta Sketch build via Scala with lgk and seed") {
    val n = 100
    val data = (for (i <- 1 to n) yield i).toDF("value")

    val sketchDf = data.agg(theta_sketch_agg_build("value", 14, 111).as("sketch"))
    val result: Row = sketchDf.select(theta_sketch_get_estimate("sketch").as("estimate")).head()

    assert(result.getAs[Double]("estimate") == 100.0)
  }

  test("Theta Sketch build via Scala with lgk, seed and p") {
    val n = 100
    val data = (for (i <- 1 to n) yield i).toDF("value")

    val sketchDf = data.agg(theta_sketch_agg_build("value", 14, 111, 0.99f).as("sketch"))
    val result: Row = sketchDf.select(theta_sketch_get_estimate("sketch").as("estimate")).head()

    result.getAs[Double]("estimate") shouldBe (100.0 +- 2.0)
  }

  test("Theta Sketch build via SQL with defaults") {
    ThetaFunctionRegistry.registerFunctions(spark)

    val n = 100
    val data = (for (i <- 1 to n) yield i).toDF("value")
    data.createOrReplaceTempView("theta_input_table")

    val df = spark.sql(s"""
      SELECT
        theta_sketch_get_estimate(theta_sketch_agg_build(value)) AS estimate
      FROM
        theta_input_table
    """)
    assert(df.head().getAs[Double]("estimate") == 100.0)
  }

  test("Theta Sketch build via SQL with lgk") {
    ThetaFunctionRegistry.registerFunctions(spark)

    val n = 100
    val data = (for (i <- 1 to n) yield i).toDF("value")
    data.createOrReplaceTempView("theta_input_table")

    val df = spark.sql(s"""
      SELECT
        theta_sketch_get_estimate(theta_sketch_agg_build(value, 14)) AS estimate
      FROM
        theta_input_table
    """)
    assert(df.head().getAs[Double]("estimate") == 100.0)
  }

  test("Theta Sketch build from strings via SQL with lgk and seed") {
    ThetaFunctionRegistry.registerFunctions(spark)

    val n = 100
    val data = (for (i <- 1 to n) yield i.toString()).toDF("str")
    data.createOrReplaceTempView("theta_input_table")

    val df = spark.sql(s"""
      SELECT
        theta_sketch_get_estimate(theta_sketch_agg_build(str, 14, 111L)) AS estimate
      FROM
        theta_input_table
    """)
    assert(df.head().getAs[Double]("estimate") == 100.0)
  }

  test("Theta Sketch build via SQL with lgk, seed and p") {
    ThetaFunctionRegistry.registerFunctions(spark)

    val n = 100
    val data = (for (i <- 1 to n) yield i).toDF("value")
    data.createOrReplaceTempView("theta_input_table")

    val df = spark.sql(s"""
      SELECT
        theta_sketch_get_estimate(theta_sketch_agg_build(value, 14, 111L, 0.99f)) AS estimate
      FROM
        theta_input_table
    """)
    df.head().getAs[Double]("estimate") shouldBe (100.0 +- 2.0)
  }

  test("Theta Union via Scala with defauls") {
    val numGroups = 10
    val numDistinct = 2000
    val data = (for (i <- 1 to numDistinct) yield (i % numGroups, i)).toDF("group", "value")

    val groupedDf = data.groupBy("group").agg(theta_sketch_agg_build("value").as("sketch"))
    val mergedDf = groupedDf.agg(theta_sketch_agg_union("sketch").as("merged"))
    val result: Row = mergedDf.select(theta_sketch_get_estimate("merged").as("estimate")).head()
    assert(result.getAs[Double]("estimate") == numDistinct)
  }

  test("Theta Union via Scala with lgk") {
    val numGroups = 10
    val numDistinct = 2000
    val data = (for (i <- 1 to numDistinct) yield (i % numGroups, i)).toDF("group", "value")

    val groupedDf = data.groupBy("group").agg(theta_sketch_agg_build("value", 14).as("sketch"))
    val mergedDf = groupedDf.agg(theta_sketch_agg_union("sketch", 14).as("merged"))
    val result: Row = mergedDf.select(theta_sketch_get_estimate("merged").as("estimate")).head()
    assert(result.getAs[Double]("estimate") == numDistinct)
  }

  test("Theta Union via Scala with lgk and seed") {
    val numGroups = 10
    val numDistinct = 2000
    val data = (for (i <- 1 to numDistinct) yield (i % numGroups, i)).toDF("group", "value")

    val groupedDf = data.groupBy("group").agg(theta_sketch_agg_build("value", 14, 111).as("sketch"))
    val mergedDf = groupedDf.agg(theta_sketch_agg_union("sketch", 14, 111).as("merged"))
    val result: Row = mergedDf.select(theta_sketch_get_estimate("merged").as("estimate")).head()
    assert(result.getAs[Double]("estimate") == numDistinct)

    val toStr: Row = mergedDf.select(theta_sketch_to_string("merged").as("summary")).head()
    toStr.getAs[String]("summary") should startWith ("\n### HeapCompactSketch")
  }

  test("Theta Union via SQL with defaults") {
    ThetaFunctionRegistry.registerFunctions(spark)

    val numGroups = 10
    val numDistinct = 2000
    val data = (for (i <- 1 to numDistinct) yield (i % numGroups, i)).toDF("group", "value")
    data.createOrReplaceTempView("theta_input_table")

    val groupedDf = spark.sql(s"""
      SELECT
        group,
        theta_sketch_agg_build(value) AS sketch
      FROM
        theta_input_table
      GROUP BY
        group
    """)
    groupedDf.createOrReplaceTempView("theta_sketch_table")

    val mergedDf = spark.sql(s"""
      SELECT
        theta_sketch_get_estimate(theta_sketch_agg_union(sketch)) AS estimate
      FROM
        theta_sketch_table
    """)
    assert(mergedDf.head().getAs[Double]("estimate") == numDistinct)
  }

  test("Theta Union via SQL with lgk") {
    ThetaFunctionRegistry.registerFunctions(spark)

    val numGroups = 10
    val numDistinct = 2000
    val data = (for (i <- 1 to numDistinct) yield (i % numGroups, i)).toDF("group", "value")
    data.createOrReplaceTempView("theta_input_table")

    val groupedDf = spark.sql(s"""
      SELECT
        group,
        theta_sketch_agg_build(value, 14) AS sketch
      FROM
        theta_input_table
      GROUP BY
        group
    """)
    groupedDf.createOrReplaceTempView("theta_sketch_table")

    val mergedDf = spark.sql(s"""
      SELECT
        theta_sketch_get_estimate(theta_sketch_agg_union(sketch, 14)) AS estimate
      FROM
        theta_sketch_table
    """)
    assert(mergedDf.head().getAs[Double]("estimate") == numDistinct)
  }

  test("Theta Union via SQL with lgk and seed") {
    ThetaFunctionRegistry.registerFunctions(spark)

    val numGroups = 10
    val numDistinct = 2000
    val data = (for (i <- 1 to numDistinct) yield (i % numGroups, i)).toDF("group", "value")
    data.createOrReplaceTempView("theta_input_table")

    val groupedDf = spark.sql(s"""
      SELECT
        group,
        theta_sketch_agg_build(value, 14, 111L) AS sketch
      FROM
        theta_input_table
      GROUP BY
        group
    """)
    groupedDf.createOrReplaceTempView("theta_sketch_table")

    val mergedDf = spark.sql(s"""
      SELECT
        theta_sketch_get_estimate(theta_sketch_agg_union(sketch, 14, 111L)) AS estimate
      FROM
        theta_sketch_table
    """)
    assert(mergedDf.head().getAs[Double]("estimate") == numDistinct)
  }
}
