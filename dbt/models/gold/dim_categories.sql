with cat_source as (
    select distinct
        product_category_name as category_name
    from {{ ref('stg_order_items') }}
    where product_category_name is not null
)

select 
    -- Tạo Surrogate Key đơn giản cho danh mục bằng hàm hash
    to_hex(md5(cast(category_name as varbinary))) as category_key,
    category_name
from cat_source