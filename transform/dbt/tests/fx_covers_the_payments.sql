{% set max_gap = var('fx_max_gap_days', 5) %}

with quotes as (

    select
        currency,
        quote_date,
        lag(quote_date) over (partition by currency order by quote_date) as previous_date
    from {{ ref('stg_fx_rates') }}

),

interior_gaps as (

    select
        currency,
        'gap between quotes' as problem,
        previous_date as from_date,
        quote_date as to_date
    from quotes
    where previous_date is not null
      and quote_date - previous_date > {{ max_gap }}

),

needed as (

    select currency, max(cast(created_at as date)) as newest_payment
    from {{ ref('stg_payments') }}
    where currency <> 'BRL' and not is_deleted
    group by currency

),

available as (

    select currency, max(quote_date) as newest_quote
    from {{ ref('stg_fx_rates') }}
    group by currency

),

missing_tail as (

    select
        n.currency,
        'no quote covering recent payments' as problem,
        a.newest_quote as from_date,
        n.newest_payment as to_date
    from needed n
    left join available a on a.currency = n.currency
    where a.newest_quote is null
       or n.newest_payment - a.newest_quote > {{ max_gap }}

)

select * from interior_gaps
union all
select * from missing_tail
