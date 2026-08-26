select
    md5(merchant_id::varchar) as merchant_sk,
    merchant_id,
    legal_name,
    category,
    country,
    is_deleted
from {{ ref('stg_merchants') }}
