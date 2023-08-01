import dlt
import os
from airflow.decorators import dag
from dlt.common import pendulum
from dlt.common.runners import Venv

# modify the dag arguments

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'test@test.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'max_active_runs': 1
}

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args
)
def shopify_dbt():
    pipeline = dlt.pipeline(pipeline_name='shopify', destination='bigquery', dataset_name='shopify_data')
    # now that data is loaded, let's transform it
    # make or restore venv for dbt, uses latest dbt version
    venv = Venv.restore_current()
    # get runner, optionally pass the venv
    here = os.path.dirname(os.path.realpath(__file__))
    dbt = dlt.dbt.package(pipeline, os.path.join(here, "../transform/shopify_dbt"),
        venv=venv)
    models = dbt.run_all()
    for m in models:
        print(f"Model {m.model_name} materialized in {m.time} with status {m.status} and message {m.message}")

shopify_dbt()