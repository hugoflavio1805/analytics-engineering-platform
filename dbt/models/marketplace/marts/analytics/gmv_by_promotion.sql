{{ config(materialized='table') }}

-- Promotion ROI mart.
-- Question: which campaigns drove GMV vs which drove chargebacks?
-- One row per promotion_id (or 'no_promotion' bucket).

with orders as (
    select
        coalesce(promotion_id, 'NO_PROMOTION')                  as promotion_id,
        order_id,
        total,
        is_black_friday_order,
        is_cyber_monday_order
    from {{ ref('fct_orders') }}
),
order_agg as (
    select
        promotion_id,
        count(*)                       as orders_count,
        sum(total)                     as gmv,
        avg(total)                     as avg_order_value,
        sum(case when is_black_friday_order then 1 else 0 end) as black_friday_orders,
        sum(case when is_cyber_monday_order then 1 else 0 end) as cyber_monday_orders
    from orders
    group by promotion_id
),
chargeback_agg as (
    select
        coalesce(o.promotion_id, 'NO_PROMOTION') as promotion_id,
        sum(case when p.is_chargeback then 1 else 0 end) * 1.0 / nullif(count(*), 0) as chargeback_rate,
        sum(case when p.is_refunded then 1 else 0 end)  * 1.0 / nullif(count(*), 0) as refund_rate
    from {{ ref('fct_orders') }} o
    join {{ ref('fct_payments') }} p using (order_id)
    group by 1
),
returns_agg as (
    select
        coalesce(o.promotion_id, 'NO_PROMOTION') as promotion_id,
        count(r.return_id) * 1.0 / nullif(count(distinct o.order_id), 0)   as return_rate
    from {{ ref('fct_orders') }} o
    left join {{ ref('fct_returns') }} r using (order_id)
    group by 1
)
select
    a.promotion_id,
    p.promotion_name,
    p.promotion_type,
    p.discount_value,
    p.is_aggressive,
    a.orders_count,
    a.gmv,
    a.avg_order_value,
    a.black_friday_orders,
    a.cyber_monday_orders,
    coalesce(c.chargeback_rate, 0)                  as chargeback_rate,
    coalesce(c.refund_rate, 0)                      as refund_rate,
    coalesce(r.return_rate, 0)                      as return_rate
from order_agg a
left join {{ ref('stg_promotions') }} p on a.promotion_id = p.promotion_id
left join chargeback_agg c on a.promotion_id = c.promotion_id
left join returns_agg r    on a.promotion_id = r.promotion_id
order by gmv desc
