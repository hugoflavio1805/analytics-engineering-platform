{{ config(materialized='table') }}

-- One row per subscription, with derivations computed here (not in source CSV).

with s as (
    select * from {{ ref('stg_subscriptions') }}
),
c as (
    select customer_id, segment from {{ ref('stg_customers') }}
)
select
    s.subscription_id,
    s.customer_id,
    c.segment,
    s.plan,
    s.status,
    s.mrr,
    s.mrr * 12                                                            as arr,
    s.start_date,
    s.end_date,
    s.billing_cycle,

    s.status = 'active'                                                   as is_active,
    s.status = 'cancelled'                                                as is_churned,

    date_diff('day', s.start_date, coalesce(s.end_date, current_date))    as lifetime_days,
    s.mrr * greatest(1, date_diff('month', s.start_date, coalesce(s.end_date, current_date)))
                                                                          as ltv_so_far,

    case
        when s.end_date is null then null
        when date_diff('day', s.start_date, s.end_date) <= 90 then true
        else false
    end                                                                   as churned_within_90d
from s
left join c using (customer_id)
