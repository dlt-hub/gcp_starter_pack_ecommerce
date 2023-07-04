import dlt
from airflow.decorators import dag
from dlt.common import pendulum
from dlt.helpers.airflow_helper import PipelineTasksGroup

# Modify the dag arguments
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
def load_shopify_data():
    from tenacity import Retrying, stop_after_attempt

    exec_date = "{{ data_interval_start }}"
    # Set `use_data_folder` to True to store temporary data on the `data` bucket.
    # Use only when it does not fit on the local storage.
    tasks = PipelineTasksGroup(
        pipeline_name="shopify",
        use_data_folder=False,
        wipe_local_data=True,
        use_task_logger=True,
        retry_policy=Retrying(stop=stop_after_attempt(3), reraise=True),
    )

    # Import your source from pipeline script
    from shopify_dlt import shopify_source

    """Example to incrementally load activities limited to items updated after a given date"""

    pipeline = dlt.pipeline(
        pipeline_name="shopify", destination='bigquery', dataset_name="shopify_data"
    )

    # First source configure to load everything
    # except activities from the beginning
    source = shopify_source(start_date=exec_date)

    # Another source configured to activities
    # starting at the given date (custom_fields_mapping is included to
    # translate custom field hashes to names)
    # customer_source = shopify_source(
    #     start_date="2023-01-01 00:00:00Z"
    # ).with_resources("customers")

    # Create the source, the "serialize" decompose option
    # will convert dlt resources into Airflow tasks.
    # Use "none" to disable it.
    tasks.add_run(
        pipeline=pipeline,
        data=source,
        decompose="serialize",
        trigger_rule="all_done",
        retries=0,
        provide_context=True
    )

    # # PipelineTasksGroup can’t handle the list of sources
    # # (e.g. data=[source, activities_source]),
    # # so we have to add them sequentially.
    # tasks.add_run(
    #     pipeline=pipeline,
    #     data=activities_source,
    #     decompose="serialize",
    #     trigger_rule="all_done",
    #     retries=0,
    #     provide_context=True
    # )

load_shopify_data()