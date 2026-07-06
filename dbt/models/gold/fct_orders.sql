with order_items as (
    select * from {{ ref('stg_order_items') }}
),

payments_agg as (
    select
        order_id,
        max(payment_type) as payment_type,
        sum(payment_value) as total_payment_value,
        max(payment_installments) as payment_installments
    from {{ ref('stg_order_payments') }}
    group by order_id
),

delivery_dates as (
    select
        order_id,
        max(status_updated_at) as latest_update,
        max(estimated_delivery_date) as estimated_delivery_date,
        max(actual_delivery_date) as actual_delivery_date
    from {{ ref('stg_order_status_history') }}
    group by order_id
)

select
    -- 1. FOREIGN KEYS (Dùng để kết nối hệ thống Snowflake Dimension)
    i.order_id,
    i.user_id as customer_unique_key,
    i.product_id,
    i.seller_id,
    
    -- 2. MEASURABLE FACTS / METRICS (Các chỉ số đo lường cốt lõi)
    1 as order_item_quantity,
    i.product_price as item_merchandise_revenue,
    i.freight_value as item_logistic_cost,
    round(i.product_price + i.freight_value, 2) as gross_amount,
    
    coalesce(p.total_payment_value, 0.0) as cash_flow_received,
    coalesce(p.payment_installments, 1) as financing_installments,
    
    -- 3. OPERATIONAL LOGISTIC METRICS (KPI Đo lường thời gian)
    d.estimated_delivery_date,
    d.actual_delivery_date,
    case 
        when d.actual_delivery_date is null then null
        when d.actual_delivery_date > cast(d.estimated_delivery_date as timestamp) then 1
        else 0
    end as count_delayed_order,                             
    
    -- 4. TIMESTAMPS
    i.created_at as order_timestamp
    
from order_items i
left join payments_agg p on i.order_id = p.order_id
left join delivery_dates d on i.order_id = d.order_id