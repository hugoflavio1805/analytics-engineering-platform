{{ config(materialized='table') }}

-- Customer Lifetime Value broken down by region, prime status, and tenure cohort.
-- LTV here = lifetime_gmv minus refunds.

with c as (
    select * from {{ ref('dim_mkt_customer') }}
),
refunds as (
    select
        o.customer_id,
        coalesce(sum(r.refund_amount), 0) as total_refunds
    from {{ ref('fct_orders') }} o
    left join {{ ref('fct_returns') }} r using (order_id)
    group by o.customer_id
),
enriched as (
    select
        c.customer_id,
        c.region,
        c.country_code,
        c.is_prime,
        c.tenure_days,
        c.orders_count,
        c.lifetime_gmv,
        coalesce(r.total_refunds, 0)             as total_refunds,
        c.lifetime_gmv - coalesce(r.total_refunds, 0) as net_ltv,
        case
            when c.tenure_days <  90  then '< 3mo'
            when c.tenure_days < 365  then '3-12mo'
            when c.tenure_days < 730  then '1-2y'
            else                            '2y+'
        end                                       as tenure_bucket
    from c
    left join refunds r using (customer_id)
)
select
    region,
    is_prime,
    tenure_bucket,
    count(*)                            as customers,
    avg(orders_count)                   as avg_orders_per_customer,
    avg(lifetime_gmv)                   as avg_lifetime_gmv,
    avg(net_ltv)                        as avg_net_ltv,
    sum(net_ltv)                        as total_net_ltv,
    median(net_ltv)                     as median_net_ltv
from enriched
where region is not null
group by region, is_prime, tenure_bucket
order by region, tenure_bucket
