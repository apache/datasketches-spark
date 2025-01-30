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

from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column, _to_seq, _create_column_from_literal
from py4j.java_gateway import JavaClass
from typing import Any, TypeVar, Union, Callable
from functools import lru_cache

ColumnOrName = Union[Column, str]
ColumnOrName_ = TypeVar("ColumnOrName_", bound=ColumnOrName)

# Since we have functions from different packages, rather than the
# single 16k+ line functions class in core Spark, we'll have each
# sketch family grab its own functions class from the JVM and cache it

def _get_jvm_class(name: str) -> JavaClass:
    """
    Retrieves JVM class identified by name from
    Java gateway associated with the current active Spark context.
    """
    assert SparkContext._active_spark_context is not None
    return getattr(SparkContext._active_spark_context._jvm, name)

@lru_cache
def _get_jvm_function(cls: JavaClass, name: str) -> Callable:
    """
    Retrieves JVM function identified by name from
    Java gateway associated with sc.
    """
    assert cls is not None
    return getattr(cls, name)

def _invoke_function(cls: JavaClass, name: str, *args: Any) -> Column:
    """
    Invokes JVM function identified by name with args
    and wraps the result with :class:`~pyspark.sql.Column`.
    """
    #assert SparkContext._active_spark_context is not None
    assert cls is not None
    jf = _get_jvm_function(cls, name)
    return Column(jf(*args))


def _invoke_function_over_columns(cls: JavaClass, name: str, *cols: "ColumnOrName") -> Column:
    """
    Invokes n-ary JVM function identified by name
    and wraps the result with :class:`~pyspark.sql.Column`.
    """
    return _invoke_function(cls, name, *(_to_java_column(col) for col in cols))


# lazy init so we know the SparkContext exists first
_spark_functions_class: JavaClass = None

def _get_spark_functions_class() -> JavaClass:
    global _spark_functions_class
    if _spark_functions_class is None:
        _spark_functions_class = _get_jvm_class("org.apache.spark.sql.functions")
    return _spark_functions_class

# borrowed from PySpark
def _array_as_java_column(data: Union[list, tuple]) -> Column:
    """
    Converts a Python list or tuple to a Spark DataFrame column.
    """
    sc = SparkContext._active_spark_context
    return _invoke_function(_get_spark_functions_class(), "array", _to_seq(sc, [_create_column_from_literal(x) for x in data])._jc)
