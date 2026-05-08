{{ config(materialized='table') }}

with s as (
    select * from {{ ref('stg_sellers') }}
),
g as (
    select * from {{ ref('stg_geography') }}
),
prods as (
    select seller_id, count(*) as products_count
    from {{ ref('stg_products') }}
    group by seller_id
),
ords as (
    select
        oi.seller_id,
        count(distinct oi.order_id)        as orders_count,
        sum(oi.line_total)                 as gmv
    from {{ ref('stg_order_items') }} oi
    group by oi.seller_id
)
select
    s.seller_id,
    s.seller_name,
    s.country                                  as country_code,
    g.country_name,
    g.region,
    g.continent,
    s.is_verified,
    s.join_date,
    date_diff('day', s.join_date, current_date) as tenure_days,
    coalesce(prods.products_count, 0)          as products_count,
    coalesce(ords.orders_count, 0)             as orders_count,
    coalesce(ords.gmv, 0)                      as gmv
from s
left join g on s.country = g.country_code
left join prods using (seller_id)
left join ords using (seller_id)
