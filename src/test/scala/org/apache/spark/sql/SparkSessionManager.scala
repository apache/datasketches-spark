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

import org.apache.log4j.{Level, Logger}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
  * This class provides a common base for tests. It can perhaps
  * be simplified or eliminated but was useful for very early-stage
  * testing.
  */
trait SparkSessionManager extends AnyFunSuite with BeforeAndAfterAll {
  Logger.getRootLogger().setLevel(Level.OFF)

  lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("datasketches-spark-tests")
      .master("local[3]")
      //.config("spark.sql.debug.codegen", "true")
      .getOrCreate()

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("OFF")
  }
}
