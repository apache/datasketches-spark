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

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.trigger_rule import TriggerRule

# THIS IS A VERY INCOMPLETE EXAMPLE
#
# This file shows only a few key configuration options needed for
# running datasketches-spark on Dataproc using pyspark.
#
# The key points are that we need to install and prepare the jars
# in the INIT_ACTIONS script and specify the jars when creating
# the pyspark job.

INIT_ACTIONS = "gs://GCS_BUCKET/install_wheels.sh"
PYSPARK_SCRIPT_PATH = "gs://GCS_BUCKET/simple_kll_example.py"

CLUSTER_CONFIG = {
    "initialization_actions": [
        {
            "executable_file": INIT_ACTIONS
        }
    ],
    "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
    "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
}

PYSPARK_JOB = {
    "main_python_file_uri": PYSPARK_SCRIPT_PATH,
    "properties": {
        "spark.driver.userClassPathFirst": "true",
        "spark.executor.userClassPathFirst": "true",
        "spark.jars": "/opt/datasketches_jars/datasketches-memory.jar,/opt/datasketches_jars/datasketches-java.jar,/opt/datasketches_jars/datasketches-spark.jar",
    }
}


# DAG definition
with DAG(
    "partial_sketches_example",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    cluster_name = "my_cluster"

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_name=cluster_name, # allows for dynamic name (e.g. w/ timestamp)
        region=REGION,
        cluster_config=CLUSTER_CONFIG,
        impersonation_chain=SERVICE_ACCOUNT,
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="run_pyspark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": "{{ task_instance.xcom_pull('create_cluster')['cluster_name'] }}"},
            "pyspark_job": PYSPARK_JOB,
        },
        impersonation_chain=SERVICE_ACCOUNT,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name="{{ task_instance.xcom_pull('create_cluster')['cluster_name'] }}",
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Define DAG dependencies
    create_cluster >> submit_job >> delete_cluster
