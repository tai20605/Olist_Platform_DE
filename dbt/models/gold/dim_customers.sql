select
    customer_id,
    user_id as customer_unique_id,
    customer_zip_code as zip_code
from {{ ref('stg_customers') }}