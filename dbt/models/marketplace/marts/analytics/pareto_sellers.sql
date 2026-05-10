{{ config(materialized='table') }}

-- Pareto / 80-20 analysis on sellers.
-- Adds rank, cumulative GMV share and bucket (A/B/C) to dim_seller.

with ranked as (
    select
        seller_id,
        seller_name,
        country_code,
        region,
        gmv,
        orders_count,
        row_number() over (order by gmv desc)                          as rank,
        sum(gmv) over (order by gmv desc rows between unbounded preceding and current row)
            / nullif(sum(gmv) over (), 0)                              as cumulative_gmv_pct,
        sum(gmv) over () as total_gmv
    from {{ ref('dim_seller') }}
    where gmv > 0
)
select
    rank,
    seller_id,
    seller_name,
    country_code,
    region,
    gmv,
    orders_count,
    cumulative_gmv_pct,
    case
        when cumulative_gmv_pct <= 0.50 then 'A'   -- top sellers driving 50% of GMV
        when cumulative_gmv_pct <= 0.80 then 'B'   -- next 30% of GMV
        else 'C'                                    -- long tail
    end as pareto_bucket
from ranked
order by rank
