select
    currency,
    quote_date,
    rate_brl
from {{ source('bronze', 'fx') }}
