-- Promotion ROI — net contribution after chargeback and return cost.
select
    promotion_id,
    promotion_name,
    promotion_type,
    discount_value,
    is_aggressive,
    orders_count,
    round(cast(gmv as double), 0)                  as gmv,
    round(cast(avg_order_value as double), 2)      as aov,
    round(chargeback_rate * 100, 2)                as chargeback_rate_pct,
    round(refund_rate * 100, 2)                    as refund_rate_pct,
    round(return_rate * 100, 2)                    as return_rate_pct,
    black_friday_orders,
    cyber_monday_orders
from marketplace_analytics.gmv_by_promotion
order by gmv desc;
