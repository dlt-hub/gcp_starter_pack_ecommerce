
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
    updated_at,
    _dlt_load_id 
FROM {{ source('dlthub-analytics', 'products') }} 
ORDER BY created_at DESC
LIMIT 100



/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
