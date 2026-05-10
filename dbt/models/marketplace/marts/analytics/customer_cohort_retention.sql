{{ config(materialized='table') }}

-- Customer cohort retention.
-- One row per (cohort_month, activity_month). cohort_month = month of first
-- order. activity_month = month of any subsequent order.
-- Retention pct = customers active in (cohort + N months) / cohort size.

with first_order as (
    select
        customer_id,
        date_trunc('month', min(order_date)) as cohort_month
    from {{ ref('fct_orders') }}
    group by customer_id
),
activity as (
    select
        o.customer_id,
        date_trunc('month', o.order_date) as activity_month,
        f.cohort_month
    from {{ ref('fct_orders') }} o
    join first_order f using (customer_id)
),
cohort_size as (
    select cohort_month, count(distinct customer_id) as cohort_size
    from first_order
    group by cohort_month
),
agg as (
    select
        cohort_month,
        activity_month,
        count(distinct customer_id) as active_customers,
        date_diff('month', cohort_month, activity_month) as months_since_acquisition
    from activity
    group by cohort_month, activity_month
)
select
    a.cohort_month,
    a.activity_month,
    a.months_since_acquisition,
    a.active_customers,
    s.cohort_size,
    1.0 * a.active_customers / s.cohort_size as retention_pct
from agg a
join cohort_size s using (cohort_month)
where a.months_since_acquisition >= 0
order by a.cohort_month, a.months_since_acquisition
