with payments as (

    select * from {{ ref('stg_payments') }} where not is_deleted

),

rates as (

    select * from {{ ref('stg_fx_rates') }}

)

select
    md5(p.payment_id::varchar) as payment_sk,
    c.customer_sk,
    m.merchant_sk,
    d.date_sk,
    cur.currency_sk,
    p.payment_id,
    p.amount as amount_original,
    round(p.amount * coalesce(r.rate_brl, 1), 2) as amount_brl,
    p.currency,
    p.status,
    p.created_at
from payments p
left join {{ ref('dim_customer') }} c
    on c.customer_id = p.customer_id
   and p.created_at >= c.valid_from
   and (c.valid_to is null or p.created_at < c.valid_to)
left join {{ ref('dim_merchant') }} m
    on m.merchant_id = p.merchant_id
left join {{ ref('dim_date') }} d
    on d.date_day = cast(p.created_at as date)
left join {{ ref('dim_currency') }} cur
    on cur.currency = p.currency
left join rates r
    on r.currency = p.currency
   and r.quote_date = cast(p.created_at as date)
