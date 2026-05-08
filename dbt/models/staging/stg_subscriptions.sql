{{ config(materialized='view') }}

with src as (
    select * from {{ source('raw', 'subscriptions') }}
)
select
    lower(replace(trim(subscription_id), '_', '-'))   as subscription_id,
    upper(replace(trim(customer_id), '_', '-'))       as customer_id,
    lower(plan)                                       as plan,
    lower(status)                                     as status,
    cast(mrr as integer)                              as mrr,
    cast(arr as integer)                              as arr,
    try_cast(start_date as date)                      as start_date,
    try_cast(end_date as date)                        as end_date,
    lower(billing_cycle)                              as billing_cycle,
    cast(is_active as boolean)                        as is_active,
    cast(lifetime_days as integer)                    as lifetime_days,
    cast(ltv_so_far as integer)                       as ltv_so_far,
    case lower(churned_within_90d) when 'true' then true when 'false' then false else null end as churned_within_90d
from src

-- Deduplicate: keep most recent version of each subscription_id
qualify row_number() over (partition by lower(replace(trim(subscription_id), '_', '-')) order by start_date desc) = 1
