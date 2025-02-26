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

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType

from datasketches_spark.kll import *
# unclear if this is needed, but we print the sketch which relies on the class
from datasketches import kll_doubles_sketch


if __name__ == "__main__":
    # we need datasketches-spark jars st up prior to this script running,
    # meaning we need to have done it in an initialization script first
    spark = (
        SparkSession.builder
        .appName('simple_kll_example')
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

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
    sk.update(-1.0)

    print(result) # Row object, from pyspark
    print(sk)     # sketch object, from datasketches-python

    spark.stop()
