

-- One row per subscription. Staging only normalizes — no derivations.

with src as (
    select * from {{ source('raw', 'subscriptions') }}
)
select
    lower(replace(trim(subscription_id), '_', '-')) as subscription_id,
    upper(replace(trim(customer_id), '_', '-'))     as customer_id,
    lower(plan)                                     as plan,
    lower(status)                                   as status,
    try_cast(mrr as integer)                        as mrr,
    try_cast(start_date as date)                    as start_date,
    try_cast(end_date as date)                      as end_date,
    lower(billing_cycle)                            as billing_cycle
from src

-- If the same subscription_id appears more than once, keep the latest start_date.
qualify row_number() over (
    partition by lower(replace(trim(subscription_id), '_', '-'))
    order by try_cast(start_date as date) desc
) = 1
