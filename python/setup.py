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

#import importlib.util
import glob
import os
import sys
#import ctypes
from setuptools import setup, find_packages
#from setuptools.command.install import install
from shutil import copyfile #, copytree, rmtree

DS_SPARK_HOME = os.environ.get("DS_SPARK_HOME", os.path.abspath("../"))
with open(f'{DS_SPARK_HOME}/version.cfg.in', 'r') as file:
    VERSION = file.read().rstrip()
TEMP_PATH = "src/datasketches_spark/deps" # we can store the relevant jars in here

# An error message if trying to run this without first building the jars
missing_jars_message = """
If you are trying to install the datasketches_spark Python package
from source, you need to first build the jars.

To build the jars, run the following command from the root directory of
the repository:
    sbt clean package
Next, you can return to this diretory and resume.
"""

# Find the datasketches-spark jar path -- other dependencies handled separately
DS_SPARK_JAR_PATH = glob.glob(os.path.join(DS_SPARK_HOME, "target/scala-*/"))
if len(DS_SPARK_JAR_PATH) == 1:
    DS_SPARK_JAR_PATH = DS_SPARK_JAR_PATH[0]
elif len(DS_SPARK_JAR_PATH) > 1:
    print(
        "Found jars for multiple scala versions ({0}). Please clean up the target directory".format(
            DS_SPARK_JAR_PATH
        ),
        file=sys.stderr
    )
    sys.exit(-1)
elif len(DS_SPARK_JAR_PATH) == 0: # core spark also checks for TEMP_PATH -- unclear why?
    print(missing_jars_message, file=sys.stderr)
    sys.exit(-1)

# Find the datasketches-java and datasketches-memory dependency jar path
DS_JAVA_JAR_PATH = glob.glob(os.path.join(DS_SPARK_HOME, "target/lib/"))
if len(DS_JAVA_JAR_PATH) == 1:
    DS_JAVA_JAR_PATH = DS_JAVA_JAR_PATH[0]
else: # error if something other than 1 directory found
    print(missing_jars_message, file=sys.stderr)
    sys.exit(-1)

# Copy the jars to the temporary directory
# Future possible enhancement: symlink instead of copy
try:
    os.makedirs(TEMP_PATH)
except OSError:
    # we don't care if it already exists
    pass

# Copy the relevant jar files to temp path
for jar_file in glob.glob(os.path.join(DS_SPARK_JAR_PATH, f"datasketches-spark_*-{VERSION}.jar")):
    copyfile(jar_file, os.path.join(TEMP_PATH, os.path.basename(jar_file)))
for jar_file in glob.glob(os.path.join(DS_JAVA_JAR_PATH, f"datasketches-java-*.jar")):
    copyfile(jar_file, os.path.join(TEMP_PATH, os.path.basename(jar_file)))
for jar_file in glob.glob(os.path.join(DS_JAVA_JAR_PATH, f"datasketches-memory-*.jar")):
    copyfile(jar_file, os.path.join(TEMP_PATH, os.path.basename(jar_file)))

setup(
    name='datasketches_spark',
    version=VERSION,
    author='Apache Software Foundation',
    author_email='dev@datasketches.apache.org',
    description='The Apache DataSketches Library for Python',
    license='Apache License 2.0',
    url='http://datasketches.apache.org',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    include_package_data=True,
    package_dir={'':'src'},
    packages=find_packages(where='src'),
    install_requires=['pyspark'],
    python_requires='>=3.8',
    zip_safe=False
)
