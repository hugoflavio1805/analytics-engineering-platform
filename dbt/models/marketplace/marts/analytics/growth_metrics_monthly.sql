{{ config(materialized='table') }}

-- Month-by-month growth metrics: GMV, MoM growth, YoY growth, run-rate.

with monthly as (
    select
        date_trunc('month', order_date) as month,
        sum(total)                       as gmv,
        count(*)                         as orders,
        count(distinct customer_id)      as active_customers,
        sum(items_count)                 as units_sold,
        avg(total)                       as avg_order_value
    from {{ ref('fct_orders') }}
    group by 1
),
with_lags as (
    select
        month,
        gmv,
        orders,
        active_customers,
        units_sold,
        avg_order_value,
        lag(gmv, 1)  over (order by month) as gmv_prev_month,
        lag(gmv, 12) over (order by month) as gmv_prev_year
    from monthly
)
select
    month,
    gmv,
    orders,
    active_customers,
    units_sold,
    avg_order_value,
    gmv_prev_month,
    gmv_prev_year,
    case when gmv_prev_month > 0 then (gmv - gmv_prev_month) / gmv_prev_month end   as mom_growth_pct,
    case when gmv_prev_year  > 0 then (gmv - gmv_prev_year)  / gmv_prev_year  end   as yoy_growth_pct,
    gmv * 12 as annualized_run_rate
from with_lags
order by month
