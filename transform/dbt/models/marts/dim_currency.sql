with used as (

    select distinct currency from {{ ref('stg_payments') }}
    union
    select distinct currency from {{ ref('stg_fx_rates') }}

)

select
    md5(currency) as currency_sk,
    currency,
    currency = 'BRL' as is_base_currency
from used
