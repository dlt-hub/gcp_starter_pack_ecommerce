from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import dlt
import os


def shopify_dbt():
    pipeline = dlt.pipeline(pipeline_name='shopify', destination='bigquery', dataset_name='shopify_data')
    # now that data is loaded, let's transform it
    # make or restore venv for dbt, uses latest dbt version
    venv = dlt.dbt.get_venv(pipeline)
    # get runner, optionally pass the venv
    here = os.path.dirname(os.path.realpath(__file__))
    dbt = dlt.dbt.package(pipeline,
        os.path.join(here,"shopify_dlt/dbt_shopify"),
        venv=venv)
    models = dbt.run_all()
    for m in models:
        print(f"Model {m.model_name} materialized in {m.time} with status {m.status} and message {m.message}")



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'test@test.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'max_active_runs': 1
}

dag = DAG(dag_id='shopify_transform_dbt',
          default_args=default_args,
          start_date=pendulum.datetime(2023, 1, 1),
          schedule_interval=None,
          max_active_runs=1,
          catchup=False)



dbt_shopify_task = PythonOperator(
        task_id=f"dbt_shopify",
        python_callable=shopify_dbt,
        trigger_rule="all_done",
        retries=1,
        dag=dag)