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

from typing import List, Optional, Tuple, Union
from py4j.java_gateway import JavaClass
from pyspark.sql.column import Column, _to_java_column # possibly fragile
from pyspark.sql.functions import lit
from pyspark.sql.utils import try_remote_functions

from pyspark.sql.types import UserDefinedType, BinaryType
from datasketches import kll_doubles_sketch

from .common import (
                     ColumnOrName,
                     _invoke_function,
                     _invoke_function_over_columns,
                     _get_jvm_class,
                     _array_as_java_column
                    )

_kll_functions_class: JavaClass = None

def _get_kll_functions_class() -> JavaClass:
    global _kll_functions_class
    if _kll_functions_class is None:
        _kll_functions_class = _get_jvm_class("org.apache.spark.sql.datasketches.kll.functions")
    return _kll_functions_class

class KllDoublesSketchUDT(UserDefinedType):
    """UDT to translate kll_doubles_sketch to/from spark"""

    @classmethod
    def sqlType(cls):
        return BinaryType()

    def serialize(self, sketch: kll_doubles_sketch) -> bytes:
        if sketch is None:
            return None
        return sketch.serialize()

    def deserialize(self, data: bytes) -> kll_doubles_sketch:
        if data is None:
            return None
        return kll_doubles_sketch.deserialize(bytes(data))

    @classmethod
    def module(cls):
        return "datasketches"

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.datasketches.kll.KllDoublesSketchType"

@try_remote_functions
def kll_sketch_double_agg_build(col: "ColumnOrName", k: Optional[Union[int, Column]] = None) -> Column:
    if k is None:
        return _invoke_function_over_columns(_get_kll_functions_class(), "kll_sketch_double_agg_build", col)
    else:
        _k = lit(k) if isinstance(k, int) else k
        return _invoke_function_over_columns(_get_kll_functions_class(), "kll_sketch_double_agg_build", col, _k)

@try_remote_functions
def kll_sketch_double_agg_merge(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns(_get_kll_functions_class(), "kll_sketch_double_agg_merge", col)

@try_remote_functions
def kll_sketch_double_get_min(col: "ColumnOrName") -> Column:
    return _invoke_function(_get_kll_functions_class(), "kll_sketch_double_get_min", _to_java_column(col))

@try_remote_functions
def kll_sketch_double_get_max(col: "ColumnOrName") -> Column:
    return _invoke_function(_get_kll_functions_class(), "kll_sketch_double_get_max", _to_java_column(col))

@try_remote_functions
def kll_sketch_double_get_pmf(col: "ColumnOrName", splitPoints: Union[List[float], Tuple[float], Column], isInclusive: bool = True) -> Column:
    if isinstance(splitPoints, (list, tuple)):
        splitPoints = _array_as_java_column(splitPoints)
    elif isinstance(splitPoints, Column):
        splitPoints = _to_java_column(splitPoints)

    return _invoke_function(_get_kll_functions_class(), "kll_sketch_double_get_pmf", col, splitPoints, isInclusive)

@try_remote_functions
def kll_sketch_double_get_cdf(col: "ColumnOrName", splitPoints: Union[List[float], Column], isInclusive: bool = True) -> Column:
    if isinstance(splitPoints, (list, tuple)):
        splitPoints = _array_as_java_column(splitPoints)
    elif isinstance(splitPoints, Column):
        splitPoints = _to_java_column(splitPoints)

    return _invoke_function(_get_kll_functions_class(), "kll_sketch_double_get_cdf", col, splitPoints, isInclusive)
