{{ config(materialized='table') }}

-- One row per shipping event. Used for SLA / lead time analysis.

with e as (
    select * from {{ ref('stg_shipping_events') }}
),
order_first as (
    -- Anchor at the 'order_created' event so we can compute hours_since_order
    select
        order_id,
        min(case when event_type = 'order_created' then event_at end) as anchor_at
    from e
    group by order_id
)
select
    e.event_id,
    e.order_id,
    e.carrier_id,
    e.event_type,
    e.event_at,
    case
        when ofa.anchor_at is null then null
        else date_diff('hour', ofa.anchor_at, e.event_at)
    end                                  as hours_since_order
from e
left join order_first ofa using (order_id)
