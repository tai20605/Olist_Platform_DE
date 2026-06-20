with source as (
    select * from {{ ref('raw_events') }}
    where event_type = 'order.created' and payload_items is not null
),

parsed as (
    select
        order_id,
        user_id,
        json_extract_scalar(payload_items, '$[0].product_id') as product_id,
        coalesce(json_extract_scalar(payload_items, '$[0].product_category_name'), 'unassigned') as product_category_name,
        cast(json_extract_scalar(payload_items, '$[0].price') as double) as product_price,
        cast(json_extract_scalar(payload_items, '$[0].freight_value') as double) as freight_value,
        json_extract_scalar(payload_items, '$[0].seller_id') as seller_id,
        json_extract_scalar(payload_items, '$[0].seller_city') as seller_city,
        upper(json_extract_scalar(payload_items, '$[0].seller_state')) as seller_state,
        cast(json_extract_scalar(payload_items, '$[0].seller_latitude') as double) as seller_latitude,
        cast(json_extract_scalar(payload_items, '$[0].seller_longitude') as double) as seller_longitude,
        json_extract_scalar(payload_items, '$[0].seller_zip_code') as seller_zip_code,
        coalesce(cast(json_extract_scalar(payload_items, '$[0].product_weight_g') as double), 0.0) as product_weight_g,
        from_iso8601_timestamp(timestamp) as created_at
    from source
)

select *,
    round(product_price + freight_value, 2) as total_item_cost
from parsed
where product_price > 0 and freight_value >= 0