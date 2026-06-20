with raw_source as (
    select * from {{ ref('raw_events') }}
    where user_id is not null and user_id != ''
      and customer_id is not null and customer_id != ''
),

ranked_records as (
    select
        user_id,
        customer_id,
        -- Chuẩn hóa dữ liệu chữ hoa/chữ thường và xử lý Null
        coalesce(trim(customer_city), 'unknown') as customer_city,
        coalesce(upper(trim(customer_state)), 'un') as customer_state,
        coalesce(customer_zip_code, '00000') as customer_zip_code,
        coalesce(cast(customer_latitude as double), 0.0) as customer_latitude,
        coalesce(cast(customer_longitude as double), 0.0) as customer_longitude,
        timestamp,
        row_number() over (partition by user_id order by timestamp desc) as rn
    from raw_source
)

-- Đảm bảo tính nhất quán (Deduplication): Chỉ giữ lại 1 bản ghi duy nhất của mỗi khách hàng
select
    user_id,
    customer_id,
    customer_city,
    customer_state,
    customer_zip_code,
    customer_latitude,
    customer_longitude
from ranked_records
where rn = 1