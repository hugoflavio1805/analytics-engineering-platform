{{ config(materialized='table') }}

-- Promotion dim with rollup of campaign performance.

with p as (
    select * from {{ ref('stg_promotions') }}
),
usage as (
    select
        promotion_id,
        count(*)                       as orders_count,
        sum(total)                     as gmv
    from {{ ref('stg_orders') }}
    where promotion_id is not null
    group by promotion_id
),
chargebacks as (
    select
        o.promotion_id,
        sum(case when p.status = 'chargeback' then 1 else 0 end) * 1.0
            / nullif(count(*), 0)      as chargeback_rate
    from {{ ref('stg_orders') }} o
    join {{ ref('stg_payments') }} p using (order_id)
    where o.promotion_id is not null
    group by o.promotion_id
)
select
    p.promotion_id,
    p.promotion_name,
    p.promotion_type,
    p.discount_value,
    p.starts_at,
    p.ends_at,
    p.is_aggressive,
    coalesce(u.orders_count, 0)        as orders_count,
    coalesce(u.gmv, 0)                 as gmv,
    coalesce(c.chargeback_rate, 0)     as chargeback_rate
from p
left join usage u using (promotion_id)
left join chargebacks c using (promotion_id)
