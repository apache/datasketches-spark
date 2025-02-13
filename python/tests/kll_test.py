
import unittest
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.session import SparkSession

from datasketches_spark import get_dependency_classpath
from datasketches_spark.kll import *

class PySparkBase(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.spark = (
          SparkSession.builder
            .appName("test")
            .master("local[1]")
            .config("spark.driver.userClassPathFirst", "true")
            .config("spark.executor.userClassPathFirst", "true")
            .config("spark.driver.extraClassPath", get_dependency_classpath())
            .config("spark.executor.extraClassPath", get_dependency_classpath())
            .getOrCreate()
        )

  @classmethod
  def tearDownClass(cls):
    cls.spark.stop()

class TestKll(PySparkBase):
  def test_kll(self):
    spark = self.spark

    # Create a DataFrame
    n = 100000
    data = [(float(i),) for i in range(1, n + 1)]
    schema = StructType([StructField("value", DoubleType(), True)])
    df = spark.createDataFrame(data, schema)
    df_agg = df.agg(kll_sketch_double_agg_build("value", 160).alias("sketch"))
    df_agg.show()

    df_agg.select(
      kll_sketch_double_get_min("sketch").alias("min"),
      kll_sketch_double_get_max("sketch").alias("max"),
      kll_sketch_double_get_pmf("sketch", [25000, 30000, 75000]).alias("pmf")
    ).show()
