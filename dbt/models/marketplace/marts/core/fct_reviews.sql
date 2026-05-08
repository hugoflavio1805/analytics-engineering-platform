{{ config(materialized='table') }}

with r as (
    select * from {{ ref('stg_reviews') }}
),
o as (
    select order_id, status as order_status, ship_country, total from {{ ref('int_orders_clean') }}
)
select
    r.review_id,
    r.order_id,
    r.customer_id,
    r.rating,
    r.title,
    r.body,
    r.review_date,
    r.verified_purchase,
    o.order_status,
    o.ship_country,
    o.total                              as order_total,
    r.rating <= 2                        as is_negative_review,
    r.rating >= 4                        as is_positive_review
from r
left join o using (order_id)
