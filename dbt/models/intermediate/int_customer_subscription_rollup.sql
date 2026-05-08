-- Per-customer rollup of subscription history.
-- Derives revenue and lifecycle facts that downstream marts consume.

with subs as (
    select * from {{ ref('stg_subscriptions') }}
),
agg as (
    select
        customer_id,
        count(*)                                                                              as subscriptions_count,
        sum(case when status = 'active' then 1 else 0 end)                                    as active_subscriptions_count,
        sum(case when status = 'cancelled' then 1 else 0 end)                                 as cancelled_subscriptions_count,
        sum(case when status = 'active' then mrr else 0 end)                                  as current_mrr,
        sum(mrr)                                                                              as lifetime_mrr,
        max(case when status = 'active' then 1 else 0 end) = 1                                as has_active_subscription,
        max(case when status = 'cancelled' then end_date end)                                 as latest_churn_date,
        min(start_date)                                                                       as first_subscription_date,
        max(case when status = 'active' and billing_cycle = 'annual' then 1 else 0 end) = 1   as has_annual_plan
    from subs
    group by customer_id
)
select
    customer_id,
    subscriptions_count,
    active_subscriptions_count,
    cancelled_subscriptions_count,
    current_mrr,
    current_mrr * 12                                          as current_arr,
    lifetime_mrr,
    has_active_subscription,
    has_annual_plan,
    latest_churn_date,
    first_subscription_date,
    case
        when current_mrr >= 500 then 'high'
        when current_mrr >= 200 then 'medium'
        when current_mrr >    0 then 'low'
        else 'none'
    end                                                       as mrr_band
from agg
