select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    reviewed_at
from {{ ref('stg_order_reviews') }}