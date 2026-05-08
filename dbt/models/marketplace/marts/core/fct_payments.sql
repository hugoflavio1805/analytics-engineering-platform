{{ config(materialized='table') }}

-- Payment-event fact. One row per payment.
-- FKs: order, payment_method.

with p as (
    select * from {{ ref('stg_payments') }}
),
o as (
    select order_id, total, customer_id, ship_country from {{ ref('int_orders_clean') }}
)
select
    p.payment_id,
    p.order_id,
    p.method                                       as method_code,
    p.status                                       as payment_status,
    p.amount,
    p.currency,
    p.paid_at,
    o.total                                        as order_total,
    o.customer_id,
    o.ship_country,
    abs(p.amount - o.total) > 0.01                 as has_amount_mismatch,
    p.status = 'chargeback'                        as is_chargeback,
    p.status = 'refunded'                          as is_refunded
from p
left join o using (order_id)
