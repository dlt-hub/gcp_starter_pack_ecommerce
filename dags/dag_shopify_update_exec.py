import dlt
import pendulum
from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["steinkraus, shopify"]
)
def load_shopify_data_two():
    from shopify_dlt import shopify_source
    
    @task
    def load_table(table, **kwargs):

        ds = kwargs["ds"]

        pipeline = dlt.pipeline(
        pipeline_name="shopify", destination='bigquery', dataset_name="shopify_data_two")

        load_info = pipeline.run(
            shopify_source(start_date=ds).with_resources(table))
        
        print(load_info)


    load_table.expand(table=["products", "orders", "customers"])

load_shopify_data_two()