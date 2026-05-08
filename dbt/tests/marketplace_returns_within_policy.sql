-- Singular test: returns should happen within the configured policy window
-- (default 30 days). The synthetic dataset has ~5% of rows outside the window
-- on purpose — this test surfaces them as warnings.
{{ config(severity='warn') }}

select
    return_id,
    order_id,
    delivered_date,
    return_date,
    days_to_return
from {{ ref('fct_returns') }}
where days_to_return is not null
  and days_to_return > {{ var('marketplace_return_window_days', 30) }}
