{{ config(materialized='table') }}

-- Conformed customer dimension. Joins the customer master with the
-- subscription rollup. All derivations come from intermediate models —
-- this layer composes, it does not compute.

with c as (
    select * from {{ ref('stg_customers') }}
),
s as (
    select * from {{ ref('int_customer_subscription_rollup') }}
)
select
    c.customer_id,
    c.name,
    c.email,
    c.plan,
    c.segment,
    c.country,
    c.status                                              as customer_status,
    c.created_at                                          as customer_created_at,
    date_diff('day', c.created_at, current_date)          as tenure_days,

    coalesce(s.subscriptions_count, 0)                    as subscriptions_count,
    coalesce(s.has_active_subscription, false)            as has_active_subscription,
    coalesce(s.has_annual_plan, false)                    as has_annual_plan,
    coalesce(s.current_mrr, 0)                            as current_mrr,
    coalesce(s.current_arr, 0)                            as current_arr,
    coalesce(s.lifetime_mrr, 0)                           as lifetime_mrr,
    coalesce(s.mrr_band, 'none')                          as mrr_band,
    s.latest_churn_date,
    s.first_subscription_date,

    case
        when c.status = 'churned' then true
        when not coalesce(s.has_active_subscription, false) then true
        else false
    end                                                   as is_churned
from c
left join s using (customer_id)
