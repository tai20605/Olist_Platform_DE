with source as (
    select * from {{ ref('raw_events') }}
    where event_type = 'order.reviewed' and payload_review is not null
),

parsed as (
    select
        order_id,
        json_extract_scalar(payload_review, '$.review_id') as review_id,
        cast(json_extract_scalar(payload_review, '$.review_score') as integer) as review_score,
        -- Làm sạch chuỗi văn bản comment đầu vào
        trim(json_extract_scalar(payload_review, '$.review_comment_title')) as review_comment_title,
        trim(json_extract_scalar(payload_review, '$.review_comment_message')) as review_comment_message,
        from_iso8601_timestamp(timestamp) as reviewed_at
    from source
)

select * from parsed
where review_score between 1 and 5