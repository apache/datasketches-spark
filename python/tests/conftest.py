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

import os
import subprocess
import pytest
from typing import Optional
from pyspark.sql import SparkSession
from datasketches_spark import get_dependency_classpath

# Attempt to determin the Java version -- this may still fail
# based on command line arguments or other pyspark config, but
# it should often work for local testing.
# Favor looking for a java executable in $SPARK_HOME, else
# $JAVA_HOME, else $PATH from just running `java`
# May return an incorrect number for Java 8 (aka 1.8) but
# we only care about detecting 17 and higher
def get_java_version() -> Optional[int]:
    java_cmd = 'java' # fallback option
    # check $JAVA_HOME first
    if 'JAVA_HOME' in os.environ:
        java_cmd = os.path.join(os.environ['JAVA_HOME'], 'bin', 'java')

    # check $SPARK_HOME next -- top choice, if available
    if 'SPARK_HOME' in os.environ:
        spark_java = os.path.join(os.environ['SPARK_HOME'], 'bin', 'java')
        if os.path.exists(spark_java):
            java_cmd = spark_java

    # now attempt to run java --version
    try:
        # java -version prints to stderr
        # Version is the 3nd argument of the 1st line and may be in double quotes
        # We care only about the major version
        output = subprocess.run([java_cmd, '-version'],
                                capture_output=True,
                                text=True)
        version = output.stderr.splitlines()[0]
        return int(version.split()[2].strip('"').split('.')[0])

    except Exception as e:
        print(f"Could not determine java version: {e}")
        return None

@pytest.fixture(scope='session')
def spark():
    java_opts = ''
    java_version = get_java_version()

    if java_version is not None and java_version >= 17 and java_version < 21:
        java_opts = '--add-modules=jdk.incubator.foreign --add-exports=java.base/sun.nio.ch=ALL-UNNAMED'
        os.environ['PYSPARK_SUBMIT_ARGS'] = f"--driver-java-options '{java_opts}' pyspark-shell"

    spark = (
        SparkSession.builder
        .appName('test')
        .master('local[1]')
        .config('spark.driver.userClassPathFirst', 'true')
        .config('spark.executor.userClassPathFirst', 'true')
        .config('spark.driver.extraClassPath', get_dependency_classpath())
        .config('spark.executor.extraClassPath', get_dependency_classpath())
        .config('spark.executor.extraJavaOptions', java_opts)
        .config('spark.driver.bindAddress', 'localhost')
        .config('spark.driver.host', 'localhost')
        .getOrCreate()
    )
    yield spark
    spark.stop()
