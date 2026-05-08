{{ config(materialized='table') }}

select
    r.return_id,
    r.order_id,
    o.customer_country_code,
    r.ship_region,
    r.category_primary,
    o.category_id,
    r.return_date,
    r.delivered_date,
    r.days_to_return,
    r.is_within_policy,
    r.reason                                        as reason_code,
    rr.reason_label,
    rr.reason_category,
    rr.is_actionable_by_seller,
    r.refund_amount,
    o.total                                         as order_total,
    case when o.total > 0 then r.refund_amount / o.total else null end   as refund_pct,
    o.payment_method,
    o.carrier_id,
    r.processed
from {{ ref('int_returns_classified') }} r
left join {{ ref('fct_orders') }} o using (order_id)
left join {{ ref('stg_return_reasons') }} rr on r.reason = rr.reason_code
