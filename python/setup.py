#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import glob
import os
import sys
from setuptools import setup
from shutil import copyfile

DS_SPARK_HOME = os.environ.get("DS_SPARK_HOME", os.path.abspath("../"))
DEPS_PATH = "src/datasketches_spark/deps" # we can store the relevant jars in here

# An error message if trying to run this without first building the jars
missing_jars_message = """
If you are trying to install the datasketches_spark Python package
from source, you need to first build the jars.

To build the jars, run the following command from the root directory of
the repository:
    sbt clean package

If building for pyspark, you should build the jar with any versiion of
Scala you may expect to use. The Scala verison can be set via the
SCALA_VERSION environment variable.

Then return to this diretory and resume building your sdist or wheel.
"""

def check_or_copy_files(filename_pattern: str, src: str, dst: str) -> None:
    """
    Checks if file(s) exist(s) in dst, updating if src has a newer version
    """

    # create list in src, check if files exist in dst
    # copy if src has a newer version
    src_list = glob.glob(os.path.join(src, filename_pattern))
    if (len(src_list) > 0):
        for src_file in src_list:
            dst_file = os.path.join(dst, os.path.basename(src_file))
            if os.path.exists(dst_file):
                if os.path.getmtime(src_file) > os.path.getmtime(dst_file):
                    copyfile(src_file, dst_file)
            else:
                copyfile(src_file, dst_file)

    # copying done, if necessary, so check if file exists in dst
    dst_file = glob.glob(os.path.join(dst, filename_pattern))
    if (len(dst_file) == 0):
        print(missing_jars_message, file=sys.stderr)
        sys.exit(-1)


# Find the datasketches-spark jar path -- other dependencies handled separately
sbt_scala_dir = os.path.join(DS_SPARK_HOME, "target", "scala-*")
check_or_copy_files("datasketches-spark_*.jar", sbt_scala_dir, DEPS_PATH)

# Find the datasketches-java and datasketches-memory dependency jar path
sbt_lib_dir = os.path.join(DS_SPARK_HOME, "target", "lib")
check_or_copy_files("datasketches-java-*.jar", sbt_lib_dir, DEPS_PATH)
check_or_copy_files("datasketches-memory-*.jar", sbt_lib_dir, DEPS_PATH)
check_or_copy_files("dependencies.txt", sbt_lib_dir, DEPS_PATH)

setup()
