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

# Apache<sup>&reg;</sup> DataSketches&trade; Spark Library

This repo is still an early-stage work in progress.

There have been multiple attempts to help integrate Apache DataSketches
into Apache Spark, including one built into Spark itself as of v3.5.
All are useful work, but in comparing them, there are various limitations
to each library. Whether limiting the type of sketches available
(e.g. native Spark provides only HLL) or limiting flexibility and
functionality (e.g. forcing HLL and Theta to use a common interface which
precludes set operations HLL cannot support, or using global parameters
to control the sizes of all sketch instances in the query), the other
libraries place undesirable constraints on developers looking to use
sketches in their queries or data systems. This library aims to restore
that choice to develoeprs.

## Build and Test Instructions

Building the library requires `sbt`, a commonly used build
system for Scala projects. There are several environment variables
that can be used to configure the project:

* Java version, typically via `$JAVA_HOME`: Default is 11
* `$SCALA_VERSION`: Default is 2.12.20
* `$SPARK_VERSION`: Default is 3.5.4

The package is built using `sbt package` and tests are
run with `sbt test`.

If building for the pyspark package, please also read
[python/README.md](python/README.md).
