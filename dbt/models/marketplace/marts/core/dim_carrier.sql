{{ config(materialized='table') }}

-- Carrier dim with derived performance rollup (actual vs promised SLA).

with c as (
    select * from {{ ref('stg_carriers') }}
),
perf as (
    select
        carrier_id,
        count(*)                                                       as orders_shipped,
        avg(date_diff('day', ship_date, delivered_date))               as avg_transit_days,
        sum(case when delivered_date is not null
                  and date_diff('day', ship_date, delivered_date) > c.sla_promised_days
                 then 1 else 0 end) * 1.0 / nullif(count(*), 0)        as sla_miss_rate
    from {{ ref('stg_orders') }} o
    left join {{ ref('stg_carriers') }} c using (carrier_id)
    where ship_date is not null
    group by carrier_id, sla_promised_days
)
select
    c.carrier_id,
    c.carrier_name,
    c.origin_country,
    c.sla_promised_days,
    c.reliability_score,
    coalesce(perf.orders_shipped, 0)                              as orders_shipped,
    perf.avg_transit_days,
    coalesce(perf.sla_miss_rate, 0)                               as sla_miss_rate
from c
left join perf using (carrier_id)
