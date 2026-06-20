with source as (
    select * from {{ ref('raw_events') }}
    where order_id is not null and order_id != '' 
      and order_status is not null and order_status != ''
)

select
    order_id,
    lower(trim(order_status)) as order_status,
    event_type,
    coalesce(metadata_device, 'unknown') as metadata_device,
    coalesce(metadata_os, 'unknown') as metadata_os,
    from_iso8601_timestamp(timestamp) as status_updated_at,
    cast(try_cast(nullif(estimated_delivery_date, '') as timestamp) as date) as estimated_delivery_date,
    try_cast(nullif(actual_delivery_date, '') as timestamp) as actual_delivery_date
from source