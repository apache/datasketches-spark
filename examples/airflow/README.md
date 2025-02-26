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

# Dataproc on GCP via Airflow

This directory contains a small example of how to use
DataSktches via pyspark in an Airflow DAG on Dataproc.

The workflow is very incomplete, and intended to show only
a few key fields. The main points are specifying an initialization
script to ensure the wheels and jars are correctly placed,
and adding the jars so Spark includes them on the classpath. As
of Spark 3.5, we need to specify the user classpath first, as Spark
has an older version of the HLL sketch using an incompatible
library version.

The initialization script `install_wheels.sh`
is important here, as that installs the wheels -- both a
wheel built for dataskethces-spark as well as the regular
datasketches-python package. To simplify workflow management,
the example script symlinks the named versions to generic names,
so that the workflow itself can remain unchanged during verion
updates, although the initializaiton script and possibly the real
compute task would need updates.

`simple_kll_example.py` is a very trivial example, feeding some
points into a KLL sketch and querying it. The example shows how
sketches can be used from within pyspark, but also move seamlessly
into python for additional analysis.
