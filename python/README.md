<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Apache<sup>&reg;</sup> DataSketches&trade; PySpark Library

This repo is still an early-stage work in progress.

This is the PySpark plugin component.

## Usage

There are several Spark config options needed to use the library.
[tests/conftest.py](tests/conftest.py) provides a basic example. The key settings to
note are:

* `.config("spark.driver.userClassPathFirst", "true")`
* `.config("spark.executor.userClassPathFirst", "true")`
* `.config("spark.driver.extraClassPath", get_dependency_classpath())`
* `.config("spark.executor.extraClassPath", get_dependency_classpath())`

Starting with Spark 3.5, Spark includes an older version of the DataSketches java library, so Spark needs to know to use the provided verison.

When using a datasketches-spark library compiled for Java 17 or newer, there are additional configuration options needed. Specifically, you must enable the `jdk.incubator.foreign` module in both the Spark driver and executors. Especially for the driver, this must be specified prior to creating the JVM; the module cannot be added once the JVM is initialized.

Looking again at [tests/conftest.py](tests/conftest.py), we need to add

* `.config('spark.executor.extraJavaOptions', java_opts)`

with `java_opts` set to `--add-modules=jdk.incubator.foreign --add-exports=java.base/sun.nio.ch=ALL-UNNAMED'`. We must also use that value to specify `--driver-java-options ${java_opts}`. That option may be specified either via the `PYSPARK_SUBMIT_ARGS` environment variable if running a python script directly, or as a command-line argument to `spark-submit` if submitting to a cluster.

## Build and Test Instructions

This component requires that the Scala library is already built.
The build process will check for the availability of the relevant
jars and fail if they do not exist. It will also update the jars
in the event the current python module's copies are older.

The easiest way to build the library is with the `build` package:
`python -m build --wheel`. The resulting wheel can then be installed with `python -m pip install dist/datasketches_spark_<version-info>.whl`

Tests are run with `pytest` or `tox`.
