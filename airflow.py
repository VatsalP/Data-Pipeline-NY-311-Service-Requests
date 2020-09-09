import datetime
import logging
import os

from base64 import b64encode

from airflow import configuration
from airflow import models
from airflow.contrib.hooks import gcs_hook
from airflow.contrib.operators import dataflow_operator
from airflow.operators import python_operator
from airflow.providers.google.cloud.operators import pubsub
from airflow.utils.trigger_rule import TriggerRule

YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)

SUCCESS_TAG = "success"
FAILURE_TAG = "failure"

COMPLETION_BUCKET = models.Variable.get("gcs_completion_bucket")
GCP_PROJECT_ID = models.Variable.get("gcp_project")
PUBSUB_TOPIC = models.Variable.get("pubsub_topic_email")
DATAFLOW_FILE = os.path.join(
    configuration.get("core", "dags_folder"), "dataflow", "process_csv_files.py"
)

DEFAULT_DAG_ARGS = {
    "start_date": YESTERDAY,
    "retries": 0,
    "project_id": models.Variable.get("gcp_project"),
    "dataflow_default_options": {
        "project": models.Variable.get("gcp_project"),
        "temp_location": models.Variable.get("gcp_temp_location"),
        "runner": "DataflowRunner",
    },
}


def move_to_completion_bucket(target_bucket, target_infix, **kwargs):
    """A utility method to move an object to a target location in GCS."""
    # hook to cloud storaged
    conn = gcs_hook.GoogleCloudStorageHook()

    # we get conf dictionary from cloud function pass data
    # which has has varible names
    source_bucket = kwargs["dag_run"].conf["bucket"]
    source_object = kwargs["dag_run"].conf["name"]

    # set path
    target_object = os.path.join(target_infix, source_object)

    logging.info(
        "Copying %s to %s",
        os.path.join(source_bucket, source_object),
        os.path.join(target_bucket, target_object),
    )
    # copy file to processed bucket
    conn.copy(source_bucket, source_object, target_bucket, target_object)

    logging.info("Deleting %s", os.path.join(source_bucket, source_object))
    # delete from processing bucket
    conn.delete(source_bucket, source_object)


with models.DAG(
    dag_id="dataflow_pipeline",
    description="DAG for dataflow pipeline",
    schedule_interval=None,
    default_args=DEFAULT_DAG_ARGS,
) as dag:
    # dataflow args
    job_args = {
        "input": 'gs://{{ dag_run.conf["bucket"] }}/{{ dag_run.conf["name"] }}',
        "output": models.Variable.get("bq_output_table"),
        "fields": models.Variable.get("input_field_names"),
    }

    # Main Dataflow task that will process and load the input delimited file.
    dataflow_task = dataflow_operator.DataFlowPythonOperator(
        task_id="process-data", py_file=DATAFLOW_FILE, options=job_args
    )

    # trigger on sucess
    success_move_task = python_operator.PythonOperator(
        task_id="success-move-to-completion",
        python_callable=move_to_completion_bucket,
        op_args=[COMPLETION_BUCKET, SUCCESS_TAG],
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # trigger on failure
    failure_move_task = python_operator.PythonOperator(
        task_id="failure-move-to-completion",
        python_callable=move_to_completion_bucket,
        op_args=[COMPLETION_BUCKET, FAILURE_TAG],
        provide_context=True,
        trigger_rule=TriggerRule.ALL_FAILED,
    )

    # After moving the bucket send email or other type of notification about success or failure
    success_message = "Successfully processed the latest file, moved to gs://{}.".format(COMPLETION_BUCKET)
    publish_task_success = pubsub.PubSubPublishMessageOperator(
        task_id="publish_task_success",
        project_id=GCP_PROJECT_ID,
        topic=PUBSUB_TOPIC,
        messages=[{"data": success_message.encode()}],
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    failure_message = "Failure in processing the latest file, moved to gs://{}.".format(COMPLETION_BUCKET)
    publish_task_failue = pubsub.PubSubPublishMessageOperator(
        task_id="publish_task_failure",
        project_id=GCP_PROJECT_ID,
        topic=PUBSUB_TOPIC,
        messages=[{"data": failure_message.encode()}],
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    dataflow_task >> success_move_task >> publish_task_success
    dataflow_task >> failure_move_task >> publish_task_failue