{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns'
) }}

-- Order header fact. One row per cleaned order.
-- FKs: customer, geography, carrier, promotion, payment_method, primary category.

with o as (
    select * from {{ ref('int_orders_clean') }}
),
c as (
    select customer_id, country, is_prime, has_valid_email
    from {{ ref('stg_customers') }}
),
primary_cat as (
    select
        oi.order_id,
        any_value(p.category_id)               as category_id,
        any_value(p.category)                  as category_name
    from {{ ref('stg_order_items') }} oi
    join {{ ref('stg_products') }} p using (product_id)
    group by oi.order_id
),
pay as (
    select order_id, status as payment_status, amount as payment_amount, method
    from {{ ref('stg_payments') }}
),
calendar as (
    select date_key, is_black_friday, is_cyber_monday from {{ ref('stg_dates') }}
)
select
    o.order_id,
    o.customer_id,
    c.country                                  as customer_country_code,
    o.ship_country,
    o.order_date,
    o.status                                   as order_status,
    o.ship_date,
    o.delivered_date,
    o.items_count,
    o.subtotal,
    o.shipping_cost,
    o.total,
    o.carrier_id,
    o.promotion_id,
    primary_cat.category_id                    as category_id,
    primary_cat.category_name                  as category_primary,
    pay.payment_status,
    pay.payment_amount,
    pay.method                                 as payment_method,
    abs(coalesce(pay.payment_amount, 0) - o.total) > 0.01   as has_payment_amount_mismatch,
    case
        when o.delivered_date is not null and o.ship_date is not null
            then date_diff('day', o.ship_date, o.delivered_date)
        else null
    end                                        as transit_days,
    coalesce(cal.is_black_friday, false)       as is_black_friday_order,
    coalesce(cal.is_cyber_monday, false)       as is_cyber_monday_order,
    c.is_prime,
    current_timestamp                          as _loaded_at
from o
left join c using (customer_id)
left join primary_cat using (order_id)
left join pay using (order_id)
left join calendar cal on o.order_date = cal.date_key

{% if is_incremental() %}
where o.order_date > (select coalesce(max(order_date), date '1900-01-01') from {{ this }})
{% endif %}
