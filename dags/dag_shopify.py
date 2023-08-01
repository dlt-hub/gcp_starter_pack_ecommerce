import dlt
import os
from airflow.decorators import dag
from dlt.common import pendulum
from dlt.helpers.airflow_helper import PipelineTasksGroup
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
def shopify_load():
    # set `use_data_folder` to True to store temporary data on the `data` bucket. Use only when it does not fit on the local storage
    tasks = PipelineTasksGroup("shopify", use_data_folder=False, wipe_local_data=True)

    # import your source from pipeline script
    from shopify_dlt import shopify_source

    source = shopify_source().with_resources("products")

    # modify the pipeline parameters 
    pipeline = dlt.pipeline(pipeline_name='shopify',
                     dataset_name='shopify_data',
                     destination='bigquery',
                     full_refresh=False # must be false if we decompose
                     )
    # create the source, the "serialize" decompose option will converts dlt resources into Airflow tasks. use "none" to disable it
    tasks.add_run(pipeline, source, decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)

def shopify_dbt():
    pipeline = dlt.pipeline(pipeline_name='shopify', destination='bigquery', dataset_name='shopify_data')
    # now that data is loaded, let's transform it
    # make or restore venv for dbt, uses latest dbt version
    venv = Venv.restore_current()
    # get runner, optionally pass the venv
    # here = os.path.dirname(os.path.realpath(__file__))
    dbt = dlt.dbt.package(pipeline, "../transform/shopify_dbt",
        venv=venv)
    models = dbt.run_all()
    for m in models:
        print(f"Model {m.model_name} materialized in {m.time} with status {m.status} and message {m.message}")

shopify_load() >> shopify_dbt()