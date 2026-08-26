select
    payment_id,
    customer_id,
    merchant_id,
    amount,
    currency,
    status,
    created_at,
    is_deleted,
    change_lsn_numeric,
    change_commit_time
from {{ source('silver', 'payments') }}
where is_current
