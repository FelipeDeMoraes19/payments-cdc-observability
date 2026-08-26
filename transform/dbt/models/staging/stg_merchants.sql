select
    merchant_id,
    legal_name,
    category,
    country,
    is_deleted,
    change_lsn_numeric,
    change_commit_time
from {{ source('silver', 'merchants') }}
where is_current
