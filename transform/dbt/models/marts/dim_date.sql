with observed as (

    select cast(created_at as date) as day from {{ ref('stg_payments') }}
    union all
    select quote_date as day from {{ ref('stg_fx_rates') }}

),

span as (

    select min(day) as first_day, max(day) as last_day from observed

),

days as (

    select cast(unnest(generate_series(first_day, last_day, interval 1 day)) as date) as date_day
    from span

)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_sk,
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day_of_month,
    extract(isodow from date_day) as iso_day_of_week,
    extract(isodow from date_day) >= 6 as is_weekend
from days
