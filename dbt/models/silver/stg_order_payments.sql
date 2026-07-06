with source as (
    select *,
        replace(payload_payments, '''', '"') as payload_payments_clean
    from {{ ref('raw_events') }}
    where event_type = 'payment.processed' and payload_payments is not null
),

parsed as (
    select
        order_id,
        -- Chuẩn hóa chuỗi text phân loại hình thức thanh toán
        coalesce(lower(trim(json_extract_scalar(payload_payments_clean, '$[0].payment_type'))), 'not_defined') as payment_type,
        coalesce(cast(json_extract_scalar(payload_payments_clean, '$[0].payment_installments') as integer), 1) as payment_installments,
        cast(json_extract_scalar(payload_payments_clean, '$[0].payment_value') as double) as payment_value,
        from_iso8601_timestamp(timestamp) as approved_at
    from source
)

select * from parsed
-- Lọc bỏ dữ liệu lỗi (Các event giao dịch lỗi không phát sinh dòng tiền)
where payment_value > 0