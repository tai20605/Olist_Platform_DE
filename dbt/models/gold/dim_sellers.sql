with seller_source as (
    select distinct
        seller_id,
        seller_zip_code -- Sử dụng cột zip gốc từ Olist nếu có, hoặc map qua cấu trúc địa lý
    from {{ ref('stg_order_items') }}
)

select
    seller_id,
    seller_zip_code as zip_code -- Liên kết bắc cầu sang dim_geography
from seller_source