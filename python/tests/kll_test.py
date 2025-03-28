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

from pyspark.sql.types import StructType, StructField, BinaryType, DoubleType, IntegerType

#from datasketches import kll_doubles_sketch
from datasketches_spark.common import cast_as_binary
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
  assert(sk.get_min_value() == result["min"])
  assert(sk.get_max_value() == result["max"])
  assert(sk.get_pmf([25000, 30000, 75000]) == result["pmf"])
  assert(sk.get_cdf([20000, 50000, 95000], False) == result["cdf"])

  df_types = df_agg.select(
    "sketch",
    cast_as_binary("sketch").alias("asBinary")
  )
  assert(df_types.schema["sketch"].dataType == KllDoublesSketchUDT())
  assert(df_types.schema["asBinary"].dataType == BinaryType())


def test_kll_merge(spark):
  n = 75 # stay in exact mode
  k = 200
  data1 = [(1, float(i)) for i in range(1, n + 1)]
  data2 = [(2, float(i)) for i in range(n + 1, 2 * n + 1)]
  schema = StructType([StructField("id", IntegerType(), True),
                       StructField("value", DoubleType(), True)])
  df = spark.createDataFrame(data1 + data2, schema)

  df_agg = df.groupBy("id").agg(kll_sketch_double_agg_build("value", k).alias("sketch"))
  assert(df_agg.count() == 2)

  # merge and get a few attributes to check
  result = df_agg.select(
    kll_sketch_double_agg_merge("sketch").alias("sketch")
  ).select(
    "sketch",
    kll_sketch_double_get_min("sketch").alias("min"),
    kll_sketch_double_get_max("sketch").alias("max")
  ).first()
  sk = result["sketch"]

  assert(sk.n == 2 * n)
  assert(sk.k == k)
  assert(sk.get_min_value() == result["min"])
  assert(sk.get_max_value() == result["max"])
