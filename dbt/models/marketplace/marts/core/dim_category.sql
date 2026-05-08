{{ config(materialized='table') }}

-- Category dimension with parent hierarchy + per-category sales rollup.

with c as (
    select * from {{ ref('stg_categories') }}
),
sales as (
    select
        p.category_id,
        count(*)                       as products_count,
        sum(oi.quantity)               as units_sold,
        sum(oi.line_total)             as gross_sales
    from {{ ref('stg_order_items') }} oi
    join {{ ref('stg_products') }} p using (product_id)
    where p.category_id is not null
    group by p.category_id
)
select
    c.category_id,
    c.category_name,
    c.parent_category,
    c.return_window_days,
    c.is_high_return,
    coalesce(s.products_count, 0)        as products_count,
    coalesce(s.units_sold, 0)            as units_sold,
    coalesce(s.gross_sales, 0)           as gross_sales
from c
left join sales s using (category_id)
