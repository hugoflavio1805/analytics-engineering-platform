{{ config(materialized='table') }}

-- Line-item grain fact. One row per (order_id, line_no).
-- FKs: order, product, seller, category.

with oi as (
    select * from {{ ref('stg_order_items') }}
),
p as (
    select product_id, category, category_id from {{ ref('stg_products') }}
),
o as (
    select order_id, order_date, ship_country from {{ ref('int_orders_clean') }}
)
select
    oi.order_id,
    oi.line_no,
    oi.product_id,
    oi.seller_id,
    p.category_id,
    p.category                          as category_name,
    o.order_date,
    o.ship_country,
    oi.quantity,
    oi.unit_price,
    oi.line_total
from oi
join o  using (order_id)
left join p using (product_id)
