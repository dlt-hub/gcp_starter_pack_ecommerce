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
    def create_pipeline():
        pipeline = dlt.pipeline(
        pipeline_name="shopify", destination='bigquery', dataset_name="shopify_data_two")

        return pipeline
    
    @task
    def load_products(pipeline, **kwargs):
        """
        Task to load tables into Bigquery.

        :params:
        :tables: -> The dlt resource  name to load.
        :kwards: -> Parameters passed by airflow at runtime.
        """

        ds = kwargs["ds"]

        load_info = pipeline.run(
            shopify_source(start_date=ds).with_resources("products"))
        
        print(load_info)

    @task
    def load_orders(pipeline, **kwargs):
        """
        Task to load tables into Bigquery.

        :params:
        :tables: -> The dlt resource  name to load.
        :kwards: -> Parameters passed by airflow at runtime.
        """

        ds = kwargs["ds"]

        load_info = pipeline.run(
            shopify_source(start_date=ds).with_resources("orders"))
        
        print(load_info)

    @task
    def load_customers(pipeline, **kwargs):
        """
        Task to load tables into Bigquery.

        :params:
        :tables: -> The dlt resource  name to load.
        :kwards: -> Parameters passed by airflow at runtime.
        """

        ds = kwargs["ds"]

        load_info = pipeline.run(
            shopify_source(start_date=ds).with_resources("customers"))
        
        print(load_info)

    data_pipeline = create_pipeline()
    load_products(data_pipeline)
    load_orders(data_pipeline)
    load_customers(data_pipeline)

load_shopify_data_two()