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

import scala.util.Random
import org.apache.spark.sql.functions._
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, BinaryType}

import org.apache.spark.sql.functions_ds._
import org.apache.datasketches.kll.KllDoublesSketch
import org.apache.spark.sql.types.KllDoublesSketchType
import org.apache.spark.registrar.DatasketchesFunctionRegistry

class KllTest extends SparkSessionManager {
  import spark.implicits._

  // helper method to check if two arrays are equal
  private def compareArrays(ref: Array[Double], tst: WrappedArray[Double]) {
    val tstArr = tst.toArray
    if (ref.length != tstArr.length)
      throw new AssertionError("Array lengths do not match: " + ref.length + " != " + tstArr.length)
    (ref zip tstArr).foreach { case (v1, v2) => if (v1 != v2) throw new AssertionError("Values do not match: " + v1 + " != " + v2) }
  }

  test("Create DataFrame from List") {
    import java.util.ArrayList

    val numClass = 10
    val numSamples = 10000
    val dataList: ArrayList[Row] = new ArrayList[Row]

    // produce a List[Row] of (id, sk)
    for (i <- 1 to numClass) yield {
      val sk = KllDoublesSketch.newHeapInstance(200)
      for (j <- 0 until numSamples) sk.update(Random.nextDouble)
      dataList.add(Row(i, sk))
    }

    val schema = StructType(Array(
      StructField("id", IntegerType, false),
      StructField("kll", KllDoublesSketchType, true)
    ))

    val df = spark.createDataFrame(dataList, schema)
    assert(df.count() == numClass)
  }

  test("Create DataFrame from parallelize()") {
    val numClass = 10
    val numSamples = 10000

    // produce a Seq(Array(id, sk))
    val data = for (i <- 1 to numClass) yield {
      val sk = KllDoublesSketch.newHeapInstance(200)
      for (j <- 0 until numSamples) sk.update(Random.nextDouble)
      Row(i, sk.toByteArray)
    }

    val schema = StructType(Array(
      StructField("id", IntegerType, false),
      StructField("kll", BinaryType, true)
    ))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .select($"id", KllDoublesSketchType.wrapBytes($"kll").as("sketch"))

    assert(df.count() == numClass)
  }

  test("KLL Doubles Sketch via scala") {
    val n = 100
    val data = (for (i <- 1 to n) yield i.toDouble).toDF("value")

    val sketchDf = data.agg(kll_sketch_agg("value").as("sketch"))
    val result: Row = sketchDf.select(kll_get_min($"sketch").as("min"),
                                      kll_get_max($"sketch").as("max")
                                      ).head

    val minValue = result.getAs[Double]("min")
    val maxValue = result.getAs[Double]("max")
    assert(minValue == 1.0)
    assert(maxValue == n.toDouble)

    val splitPoints = Array[Double](20.5, 50, 102)
    val pmfCdfResult = sketchDf.select(
      kll_get_pmf($"sketch", splitPoints).as("pmf_inclusive"),
      kll_get_pmf($"sketch", splitPoints, false).as("pmf_exclusive"),
      kll_get_cdf($"sketch", splitPoints).as("cdf_inclusive"),
      kll_get_cdf($"sketch", splitPoints, false).as("cdf_exclusive")
    ).head

    val pmf_incl = Array[Double](0.2, 0.3, 0.5, 0.0)
    compareArrays(pmf_incl, pmfCdfResult.getAs[WrappedArray[Double]]("pmf_inclusive"))

    val pmf_excl = Array[Double](0.2, 0.29, 0.51, 0.0)
    compareArrays(pmf_excl, pmfCdfResult.getAs[WrappedArray[Double]]("pmf_exclusive"))

    val cdf_incl = Array[Double](0.2, 0.5, 1.0, 1.0)
    compareArrays(cdf_incl, pmfCdfResult.getAs[WrappedArray[Double]]("cdf_inclusive"))

    val cdf_excl = Array[Double](0.2, 0.49, 1.0, 1.0)
    compareArrays(cdf_excl, pmfCdfResult.getAs[WrappedArray[Double]]("cdf_exclusive"))
  }

  test("Kll Doubles Sketch via SQL") {
    // register Datasketches functions
    DatasketchesFunctionRegistry.registerFunctions(spark)

    val n = 100
    val data = (for (i <- 1 to n) yield i.toDouble).toDF("value")
    data.createOrReplaceTempView("data_table")

    val kllDf = spark.sql(
      s"""
      |SELECT
      |  kll_get_min(kll_sketch_agg(value, 200)) AS min,
      |  kll_get_max(kll_sketch_agg(value, 200)) AS max
      |FROM
      |  data_table
    """.stripMargin
    )
    val minValue = kllDf.head.getAs[Double]("min")
    val maxValue = kllDf.head.getAs[Double]("max")
    assert(minValue == 1.0)
    assert(maxValue == n.toDouble)

    val splitPoints = "array(20.5, 50, 102)"
    val pmfCdfResult = spark.sql(
      s"""
      |SELECT
      |  kll_get_pmf(t.sketch, ${splitPoints}) AS pmf_inclusive,
      |  kll_get_pmf(t.sketch, ${splitPoints}, false) AS pmf_exclusive,
      |  kll_get_cdf(t.sketch, ${splitPoints}) AS cdf_inclusive,
      |  kll_get_cdf(t.sketch, ${splitPoints}, false) AS cdf_exclusive
      |FROM
      |  (SELECT
      |     kll_sketch_agg(value, 200) sketch
      |   FROM
      |     data_table) t
      """.stripMargin
    ).head

    val pmf_incl = Array[Double](0.2, 0.3, 0.5, 0.0)
    compareArrays(pmf_incl, pmfCdfResult.getAs[WrappedArray[Double]]("pmf_inclusive"))

    val pmf_excl = Array[Double](0.2, 0.29, 0.51, 0.0)
    compareArrays(pmf_excl, pmfCdfResult.getAs[WrappedArray[Double]]("pmf_exclusive"))

    val cdf_incl = Array[Double](0.2, 0.5, 1.0, 1.0)
    compareArrays(cdf_incl, pmfCdfResult.getAs[WrappedArray[Double]]("cdf_inclusive"))

    val cdf_excl = Array[Double](0.2, 0.49, 1.0, 1.0)
    compareArrays(cdf_excl, pmfCdfResult.getAs[WrappedArray[Double]]("cdf_exclusive"))
  }

  test("KLL Doubles Merge via Scala") {
    val data = generateData().toDF("id", "value")

    // compute global min and max
    val minMax: Row = data.agg(min("value").as("min"), max("value").as("max")).collect.head
    val globalMin = minMax.getAs[Double]("min")
    val globalMax = minMax.getAs[Double]("max")

    // create a sketch for each id value
    val idSketchDf = data.groupBy($"id").agg(kll_sketch_agg($"value").as("sketch"))

    // default k
    // merge into an aggregate sketch
    var mergedSketchDf = idSketchDf.agg(kll_merge_agg($"sketch").as("sketch"))

    // check min and max
    var result: Row = mergedSketchDf.select(kll_get_min($"sketch").as("min"),
                                            kll_get_max($"sketch").as("max"))
                                    .head

    var sketchMin = result.getAs[Double]("min")
    var sketchMax = result.getAs[Double]("max")

    assert(globalMin == sketchMin)
    assert(globalMax == sketchMax)

    // specified k
    // merge into an aggregate sketch
    mergedSketchDf = idSketchDf.agg(kll_merge_agg($"sketch", 160).as("sketch"))

    // check min and max
    result = mergedSketchDf.select(kll_get_min($"sketch").as("min"),
                                   kll_get_max($"sketch").as("max"))
                           .head

    sketchMin = result.getAs[Double]("min")
    sketchMax = result.getAs[Double]("max")

    assert(globalMin == sketchMin)
    assert(globalMax == sketchMax)
  }

  test("KLL Doubles Merge via SQL") {
    // register Datasketches functions
    DatasketchesFunctionRegistry.registerFunctions(spark)

    val data = generateData().toDF()
    data.createOrReplaceTempView("data_table")

    // compute global min and max from dataframe
    val minMax: Row = data.agg(min("value").as("min"), max("value").as("max")).head
    val globalMin = minMax.getAs[Double]("min")
    val globalMax = minMax.getAs[Double]("max")

    // cannot do nested aggregations so need
    // need a first pass to generate sketches
    val idSketchDf = spark.sql(
      s"""
      |SELECT
      |  id,
      |  kll_sketch_agg(value, 200) AS sketch
      |FROM
      |  data_table
      |GROUP BY
      |  id
      """.stripMargin
    )
    idSketchDf.createOrReplaceTempView("sketch_table")

    // default k
    // now merge the sketches
    var mergedSketchDf = spark.sql(
      s"""
      |SELECT
      |  kll_get_min(sub.sketch) AS min,
      |  kll_get_max(sub.sketch) AS max
      |FROM
      |  (SELECT
      |     kll_merge_agg(sketch) AS sketch
      |  FROM
      |    sketch_table
      |  ) sub
      """.stripMargin
    )

    // check min and max
    var result: Row = mergedSketchDf.head
    var sketchMin = result.getAs[Double]("min")
    var sketchMax = result.getAs[Double]("max")

    assert(globalMin == sketchMin)
    assert(globalMax == sketchMax)

    // specified k
    // now merge the sketches
    mergedSketchDf = spark.sql(
      s"""
      |SELECT
      |  kll_get_min(sub.sketch) AS min,
      |  kll_get_max(sub.sketch) AS max
      |FROM
      |  (SELECT
      |     kll_merge_agg(sketch, 160) AS sketch
      |  FROM
      |    sketch_table
      |  ) sub
      """.stripMargin
    )

    // check min and max
    result = mergedSketchDf.head
    sketchMin = result.getAs[Double]("min")
    sketchMax = result.getAs[Double]("max")

    assert(globalMin == sketchMin)
    assert(globalMax == sketchMax)
  }

  // a simple data generator
  private def generateData(numRecords: Int = 25000, numClasses: Int = 20): Seq[DataRow] = {
    val maxId = numClasses
    for (i <- 0 until numRecords) yield {
      val id = i % maxId
      DataRow(id, id + Random.nextDouble())
    }
  }
}

case class DataRow(id: Integer, value: Double)
