{{ config(materialized='table') }}

select
    s.subscription_id,
    s.customer_id,
    c.segment,
    s.plan,
    s.status,
    s.mrr,
    s.arr,
    s.start_date,
    s.end_date,
    s.lifetime_days,
    s.ltv_so_far,
    s.billing_cycle,
    s.is_active,
    s.churned_within_90d,
    case when s.status = 'cancelled' then true else false end as is_churned
from {{ ref('stg_subscriptions') }} s
left join {{ ref('stg_customers') }} c using (customer_id)
