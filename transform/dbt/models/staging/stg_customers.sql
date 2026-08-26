with versions as (

    select
        customer_id,
        full_name,
        email,
        cpf,
        is_deleted,
        change_lsn_numeric,
        change_xid,
        change_commit_time
    from {{ source('silver', 'customers') }}

),

ranked_within_transaction as (

    select
        *,
        row_number() over (
            partition by customer_id, change_xid
            order by change_lsn_numeric desc
        ) as rank_in_transaction
    from versions

)

select
    customer_id,
    full_name,
    email,
    cpf,
    is_deleted,
    change_lsn_numeric,
    change_xid,
    change_commit_time
from ranked_within_transaction
where rank_in_transaction = 1
