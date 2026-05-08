-- Carrier SLA performance per ship_country.
select
    carrier_id,
    carrier_name,
    ship_country,
    sla_promised_days,
    reliability_score,
    orders_total,
    orders_delivered,
    avg_transit_days,
    round(sla_miss_rate * 100, 1)        as sla_miss_pct,
    late_returns,
    round(late_return_rate * 100, 2)     as late_return_pct
from marketplace_analytics.sla_by_carrier
order by sla_miss_pct desc nulls last;
