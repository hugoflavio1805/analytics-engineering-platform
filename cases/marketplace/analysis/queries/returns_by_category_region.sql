-- Answers: which (category, region) pairs leak the most GMV via returns?
-- Runs unchanged on DuckDB and Snowflake.

select
    category,
    region,
    orders_total,
    returns_total,
    round(return_rate * 100, 1)            as return_rate_pct,
    round(cast(gmv as double), 0)          as gmv,
    round(cast(refund_total as double), 0) as refund_total,
    round(gmv_loss_rate * 100, 1)          as gmv_loss_pct,
    returns_product_issue,
    returns_fit_issue,
    returns_customer_driven,
    returns_outside_policy
from marketplace_analytics.category_region_return_rate
order by refund_total desc;
