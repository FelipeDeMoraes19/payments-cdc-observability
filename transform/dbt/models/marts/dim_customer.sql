with changes as (

    select * from {{ ref('stg_customers') }}

),

intervals as (

    select
        customer_id,
        full_name,
        email,
        cpf,
        is_deleted,
        change_lsn_numeric,
        case
            when row_number() over (
                partition by customer_id order by change_lsn_numeric
            ) = 1
            then cast('1900-01-01' as timestamp)
            else change_commit_time
        end as valid_from,
        change_commit_time as first_seen_at,
        lead(change_commit_time) over (
            partition by customer_id
            order by change_lsn_numeric
        ) as valid_to,
        row_number() over (
            partition by customer_id
            order by change_lsn_numeric desc
        ) = 1 as is_current
    from changes

)

select
    md5(customer_id::varchar || '|' || change_lsn_numeric::varchar) as customer_sk,
    customer_id,
    full_name,
    email,
    cpf,
    valid_from,
    valid_to,
    first_seen_at,
    is_current,
    is_deleted
from intervals
