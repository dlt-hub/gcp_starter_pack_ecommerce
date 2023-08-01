
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/



SELECT 
    id,
    title, 
    vendor,
    created_at,
    handle,
    updated_at,
    status,
    tags,
    _dlt_load_id 
FROM {{ source('dlt_analytics', 'products') }} 
LIMIT 100



/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
