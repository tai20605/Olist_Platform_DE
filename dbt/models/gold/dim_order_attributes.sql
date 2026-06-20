with unique_orders as (
    select distinct
        order_id,
        order_status,
        metadata_device,
        metadata_os
    from {{ ref('stg_order_status_history') }}
)

select
    order_id,
    order_status,
    metadata_device as purchase_device,
    metadata_os as purchase_os
from unique_orders