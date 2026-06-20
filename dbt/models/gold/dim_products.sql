with prod_source as (
    select distinct
        product_id,
        product_category_name as category_name,
        product_weight_g
    from {{ ref('stg_order_items') }}
)

select
    p.product_id,
    -- Liên kết bắc cầu sang dim_categories để tìm tên danh mục
    to_hex(md5(cast(p.category_name as varbinary))) as category_key,
    p.product_weight_g
from prod_source p