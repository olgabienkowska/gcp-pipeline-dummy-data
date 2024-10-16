from datetime import datetime

from airflow import models
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

from scripts.read_file_mask_upload import process_and_load_data
from scripts.generate_call_center_data import generate_and_upload_data

from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

@task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
def watcher():
    raise AirflowException("Failing task because one or more upstream tasks failed.")


with models.DAG(
    dag_id="call_center_pipeline",
    start_date=datetime(2024, 10, 15),
    schedule_interval=None,
    catchup=False,
) as dag:

    generate_data_task = PythonOperator(
        task_id="generate_and_upload_data",
        python_callable=generate_and_upload_data,
        op_kwargs={
            "bucket_name": 'call-center-project-bucket',
            "num_records": 10
        },
    )

    process_and_load_data_task = PythonOperator(
        task_id="process_and_load_data",
        python_callable=process_and_load_data,
        op_kwargs={
            "bucket_name": 'call-center-project-bucket',
            "csv_filename": f'call_center_data_{datetime.today().strftime("%Y%m%d")}.csv',
            "column_names": 'rep_name',
            "table_id": 'call-center-project-438709.stg.call_center_input_data'
        },
    )

    compile_result_task = DataformCreateCompilationResultOperator(
        task_id='compile_result',
        project_id='call-center-project-438709',
        region='europe-west1',
        repository_id='call-center-project',
        compilation_result={
            "git_commitish": 'main',
        },
    )

    invoke_workflow_task = DataformCreateWorkflowInvocationOperator(
        task_id='invoke_workflow',
        project_id='call-center-project-438709',
        region='europe-west1',
        repository_id='call-center-project',
        workflow_invocation={
            "compilation_result":
                "{{ task_instance.xcom_pull('compile_result')['name'] }}"
        },
        retries=0
    )

    generate_data_task >> process_and_load_data_task >> compile_result_task >> invoke_workflow_task

    list(dag.tasks) >> watcher()
