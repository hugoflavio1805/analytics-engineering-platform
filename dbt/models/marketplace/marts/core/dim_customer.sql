{{ config(materialized='table') }}

-- Customer dim, conformed with geography (region/continent/currency).

with c as (
    select * from {{ ref('stg_customers') }}
),
g as (
    select * from {{ ref('stg_geography') }}
),
orders as (
    select
        customer_id,
        count(*)                                as orders_count,
        sum(total)                              as lifetime_gmv,
        sum(case when status = 'returned' then 1 else 0 end) as returned_orders_count
    from {{ ref('stg_orders') }}
    group by customer_id
)
select
    c.customer_id,
    c.full_name,
    c.email,
    c.has_valid_email,
    c.country                              as country_code,
    g.country_name,
    g.region,
    g.continent,
    g.currency,
    c.signup_date,
    date_diff('day', c.signup_date, current_date)   as tenure_days,
    c.is_prime,
    coalesce(orders.orders_count, 0)       as orders_count,
    coalesce(orders.lifetime_gmv, 0)       as lifetime_gmv,
    coalesce(orders.returned_orders_count, 0) as returned_orders_count
from c
left join g on c.country = g.country_code
left join orders using (customer_id)
