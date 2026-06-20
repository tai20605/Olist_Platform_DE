with geo_source as (
    select distinct
        customer_zip_code as zip_code,
        customer_city as city,
        customer_state as state,
        customer_latitude as latitude,
        customer_longitude as longitude
    from {{ ref('stg_customers') }}
)

select 
    zip_code,
    city,
    state,
    latitude,
    longitude
from geo_source