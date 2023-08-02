
/*
    You can confiqure the models directly within SQL files.
    This will override configurations stated in dbt_project.yml

    Use the config block below to overidde dbt_project.yml file.
    https://docs.getdbt.com/docs/build/sql-models#configuring-models
    
    {{ config(
        materialized="table",
        schema="marketing"
    ) }}

    The name of the SQL file, in this case products_view will be
    the name of the view created in the bigquery. 

*/


SELECT 
    id,
    title, 
    vendor,
    created_at,
    updated_at,
    _dlt_load_id 
FROM {{ source('shopify', 'products') }} 
ORDER BY created_at DESC
LIMIT 100


