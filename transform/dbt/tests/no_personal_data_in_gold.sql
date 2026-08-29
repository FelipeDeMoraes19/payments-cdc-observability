{#
  Masked values are a 64 character hex digest and look nothing like the thing
  they replace. That is deliberate, from ADR 0005: if a masked CPF still looked
  like a CPF, this test could not tell a masked value from a leaked one and would
  prove nothing.
#}

with customers as (

    select customer_id, cpf, email from {{ ref('dim_customer') }}

)

select
    customer_id,
    'cpf does not look masked' as problem
from customers
where cpf is not null
  and not regexp_matches(cpf, '^[0-9a-f]{64}$')

union all

select
    customer_id,
    'email does not look masked' as problem
from customers
where email is not null
  and not regexp_matches(email, '^[0-9a-f]{64}$')
