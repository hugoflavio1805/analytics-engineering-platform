{{ config(materialized='table') }}

with c as (
    select * from {{ ref('stg_customers') }}
),
s as (
    select
        customer_id,
        sum(case when status = 'active' then mrr else 0 end) as current_mrr,
        sum(mrr) as total_mrr_lifetime,
        max(case when status = 'active' then 1 else 0 end) = 1 as has_active_subscription,
        max(case when status = 'cancelled' then end_date end) as latest_churn_date
    from {{ ref('stg_subscriptions') }}
    group by customer_id
)
select
    c.customer_id,
    c.name,
    c.email,
    c.plan,
    c.segment,
    c.country,
    c.status                          as customer_status,
    c.created_at                      as customer_created_at,
    c.tenure_days,
    coalesce(s.current_mrr, 0)        as current_mrr,
    coalesce(s.total_mrr_lifetime, 0) as total_mrr_lifetime,
    coalesce(s.has_active_subscription, false) as has_active_subscription,
    s.latest_churn_date,
    c.mrr_band,
    c.feedback_count,
    c.negative_feedback_count,
    c.praise_count,
    c.feature_request_count,
    c.churn_risk_score
from c
left join s using (customer_id)
