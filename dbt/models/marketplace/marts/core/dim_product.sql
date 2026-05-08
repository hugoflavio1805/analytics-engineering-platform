{{ config(materialized='table') }}

with p as (
    select * from {{ ref('stg_products') }}
),
oi as (
    select product_id, sum(quantity) as units_sold, sum(line_total) as revenue
    from {{ ref('stg_order_items') }}
    group by product_id
),
returns_agg as (
    select
        oi.product_id,
        count(*) as returns_count
    from {{ ref('stg_order_items') }} oi
    join {{ ref('stg_returns') }} r using (order_id)
    group by oi.product_id
)
select
    p.product_id,
    p.sku,
    p.product_name,
    p.category,
    p.category_id,
    p.seller_id,
    p.unit_price,
    p.stock_qty,
    p.created_at,
    p.has_negative_price,
    p.has_negative_stock,
    coalesce(oi.units_sold, 0)        as units_sold,
    coalesce(oi.revenue, 0)           as revenue,
    coalesce(r.returns_count, 0)      as returns_count
from p
left join oi using (product_id)
left join returns_agg r using (product_id)
