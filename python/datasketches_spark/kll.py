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
from pyspark.sql.column import Column, _to_java_column, _to_seq, _create_column_from_literal
from pyspark.sql.functions import lit
from pyspark.sql.utils import try_remote_functions

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
