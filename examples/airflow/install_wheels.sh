#!/bin/bash

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

set -ex

export DSPY_PACKAGE=datasketches-5.2.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
export DSSPARK_PACKAGE=datasketches_spark-0.1.0.dev0-py3-none-any.whl

# probably no need to export but convenient to define this
export JAR_PATH=/opt/datasketches_jars

# if using java 17, we may need to add extra options
#java_opts = '--add-modules=jdk.incubator.foreign --add-exports=java.base/sun.nio.ch=ALL-UNNAMED'
#os.environ['PYSPARK_SUBMIT_ARGS'] = f"--driver-java-options '{java_opts}' pyspark-shell"

# download wheels from GCS buckets
gsutil cp gs://GCS_BUCKET_NAME/${DSPY_PACKAGE} /tmp/
gsutil cp gs://GCS_BUCKET_NAME/${DSSPARK_PACKAGE} /tmp/

# install wheels
python -m pip install /tmp/${DSPY_PACKAGE}
python -m pip install /tmp/${DSSPARK_PACKAGE}

# create symlink to the installed wheels, using the classpath from
# datasketches_spark.get_dependency_classpath()
mkdir -p $JAR_PATH
chmod 755 $JAR_PATH

python -c "
import datasketches_spark
import re
import os
jars = datasketches_spark.get_dependency_classpath().split(':')
for jar in jars:
    filename = os.path.basename(jar)
    os.system(f'ln -s {jar} $JAR_PATH/datasketches-{re.search(r\"datasketches-([^-_]+).*?.jar$\", filename).group(1)}.jar')
"

# update permissions away from root
chmod 644 $JAR_PATH/datasketches-*.jar
chown -R dataproc:dataproc $JAR_PATH

ls -al $JAR_PATH
