{{ config(materialized='table') }}

-- Carrier SLA performance mart.
-- Question: which carriers miss their SLA most, and how does that
-- correlate with 'arrived_late' returns?
-- One row per carrier × ship_country.

with orders_with_transit as (
    select
        carrier_id,
        ship_country,
        order_id,
        transit_days,
        order_status,
        delivered_date is not null            as was_delivered
    from {{ ref('fct_orders') }}
    where carrier_id is not null
),
late_returns as (
    select
        o.carrier_id,
        o.ship_country,
        count(*)                              as late_returns
    from {{ ref('fct_orders') }} o
    join {{ ref('fct_returns') }} r using (order_id)
    where r.reason_code = 'arrived_late'
    group by o.carrier_id, o.ship_country
),
agg as (
    select
        o.carrier_id,
        o.ship_country,
        c.carrier_name,
        c.sla_promised_days,
        c.reliability_score,
        count(*)                                                   as orders_total,
        count(case when o.was_delivered then 1 end)                as orders_delivered,
        avg(o.transit_days)                                        as avg_transit_days,
        sum(case when o.transit_days > c.sla_promised_days then 1 else 0 end) * 1.0
            / nullif(count(case when o.was_delivered then 1 end), 0) as sla_miss_rate
    from orders_with_transit o
    left join {{ ref('stg_carriers') }} c using (carrier_id)
    group by o.carrier_id, o.ship_country, c.carrier_name, c.sla_promised_days, c.reliability_score
)
select
    a.carrier_id,
    a.carrier_name,
    a.ship_country,
    a.sla_promised_days,
    a.reliability_score,
    a.orders_total,
    a.orders_delivered,
    round(cast(a.avg_transit_days as double), 1)        as avg_transit_days,
    round(cast(a.sla_miss_rate as double), 3)           as sla_miss_rate,
    coalesce(lr.late_returns, 0)                        as late_returns,
    case when a.orders_total > 0
        then round(cast(coalesce(lr.late_returns, 0) as double) / a.orders_total, 4)
        else 0
    end                                                  as late_return_rate
from agg a
left join late_returns lr using (carrier_id, ship_country)
order by sla_miss_rate desc nulls last
