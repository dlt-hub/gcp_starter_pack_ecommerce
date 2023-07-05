import dlt
from airflow.decorators import dag, task
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
def load_shopify_data_airflow():
    from tenacity import Retrying, stop_after_attempt
    from shopify_dlt import shopify_source
    
    @task
    def create_pipeline(**kwargs):
        ds = kwargs["ds"]

        pipeline = dlt.pipeline(
            pipeline_name="shopify", destination='bigquery', dataset_name="shopify_data_airflow_two"
        )

        source = shopify_source(start_date=ds)

        return source, pipeline


    def load_tables(source, pipeline):

        # Set `use_data_folder` to True to store temporary data on the `data` bucket.
        # Use only when it does not fit on the local storage.
        tasks = PipelineTasksGroup(
            pipeline_name="shopify",
            use_data_folder=True,
            wipe_local_data=True,
            use_task_logger=True,
            retry_policy=Retrying(stop=stop_after_attempt(3), reraise=True),
        )


        tasks.add_run(
            pipeline=pipeline,
            data=source,
            decompose="serialize",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
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
    source_data, source_pipeline = create_pipeline()
    load_tables(source_data, source_pipeline)

load_shopify_data_airflow()