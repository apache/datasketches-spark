# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyspark.sql.types import StructType, StructField, DoubleType

from datasketches import kll_doubles_sketch
from datasketches_spark.kll import *

def test_kll_build(spark):
  n = 100000
  k = 160
  data = [(float(i),) for i in range(1, n + 1)]
  schema = StructType([StructField("value", DoubleType(), True)])
  df = spark.createDataFrame(data, schema)

  df_agg = df.agg(kll_sketch_double_agg_build("value", k).alias("sketch"))

  result = df_agg.select(
    "sketch",
    kll_sketch_double_get_min("sketch").alias("min"),
    kll_sketch_double_get_max("sketch").alias("max"),
    kll_sketch_double_get_pmf("sketch", [25000, 30000, 75000]).alias("pmf"),
    kll_sketch_double_get_cdf("sketch", [20000, 50000, 95000], False).alias("cdf")
  ).first()
  sk = result["sketch"]

  assert(sk.n == n)
  assert(sk.k == k)
  assert(result["min"] == sk.get_min_value())
  assert(result["max"] == sk.get_max_value())
  assert(sk.get_pmf([25000, 30000, 75000]) == result["pmf"])
  assert(sk.get_cdf([20000, 50000, 95000], False) == result["cdf"])

def test_kll_merge(spark):
  n = 75 # stay in exact mode
  k = 200
  data1 = [(float(i),) for i in range(1, n + 1)]
  data2 = [(float(i),) for i in range(n + 1, 2 * n + 1)]
  schema = StructType([StructField("value", DoubleType(), True)])
  df1 = spark.createDataFrame(data1, schema)
  df2 = spark.createDataFrame(data2, schema)

  df_agg1 = df1.agg(kll_sketch_double_agg_build("value", k).alias("sketch"))
  df_agg2 = df2.agg(kll_sketch_double_agg_build("value", k).alias("sketch"))

  result = df_agg1.union(df_agg2).select(
    kll_sketch_double_agg_merge("sketch").alias("sketch")
  ).first()
  sk = result["sketch"]

  assert(sk.n == 2 * n)
  assert(sk.k == k)
  assert(sk.get_min_value() == 1.0)
  assert(sk.get_max_value() == 2 * n)
