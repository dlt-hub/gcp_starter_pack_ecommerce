import dlt
import pendulum
from airflow.decorators import dag, task

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
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["steinkraus, shopify"]
)
def load_shopify_data_two():
    """Execute a pipeline that will load all the resources for the given endpoints."""
    from shopify_dlt import shopify_source
    
    @task
    def load_table(table, **kwargs):

        ds = kwargs["ds"]

        pipeline = dlt.pipeline(
        pipeline_name="shopify", destination='bigquery', dataset_name="shopify_data_2.0")

        load_info = pipeline.run(
            shopify_source(start_date=ds).with_resources(table))
        
        print(load_info)


    load_table.expand(table=["products", "orders", "customers"])

load_shopify_data_two()