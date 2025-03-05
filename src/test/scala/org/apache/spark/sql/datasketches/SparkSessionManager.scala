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

import org.apache.log4j.{Level, Logger}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

/**
  * This class provides a common base for tests. It can perhaps
  * be simplified or eliminated but was useful for very early-stage
  * testing.
  */
trait SparkSessionManager extends AnyFunSuite with BeforeAndAfterAll {
  Logger.getRootLogger().setLevel(Level.OFF)

  lazy val spark: SparkSession = {
    // environment variable to force codegen for testing
    // FORCE_CODEGEN means allow only codegen
    // no argument means to use Spark's default (try codegen, fall back on error)
    val forceCodegen = sys.env.getOrElse("FORCE_CODEGEN", "false").toBoolean

    val builder = SparkSession.builder()
    builder
      .appName(s"datasketches-spark-tests")
      .master("local[1]")
      .config("spark.driver.bindAddress", "localhost")
      .config("spark.driver.host", "localhost")

    if (forceCodegen) {
      builder
        .config("spark.sql.codegen.wholeStage", "true")
        .config("spark.sql.codegen.fallback", "false")
    }

    Logger.getRootLogger().info(s"Spark session started with codegen: $forceCodegen")
    builder.getOrCreate()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("OFF")
  }
}
