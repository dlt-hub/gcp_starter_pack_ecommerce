
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

SELECT 
    id,
    name, 
    fulfillment_status,
    created_at,
    updated_at,
    _dlt_load_id 
FROM {{ source('proud-plane-390513', 'orders') }} 
ORDER BY created_at DESC
LIMIT 100;
