-- Returns enriched with:
--   - within-policy flag (default 30-day window, configurable via var)
--   - days_to_return (relative to delivered_date)
--   - reason_category (group reasons into "product issue" vs "customer-driven")

{% set window = var('marketplace_return_window_days', 30) %}

with r as (
    select * from {{ ref('stg_returns') }}
),
o as (
    select order_id, delivered_date from {{ ref('int_orders_clean') }}
),
joined as (
    select
        r.*,
        o.delivered_date,
        case
            when o.delivered_date is null then null
            else date_diff('day', o.delivered_date, r.return_date)
        end                                                    as days_to_return,
        case r.reason
            when 'defective'        then 'product_issue'
            when 'damaged'          then 'product_issue'
            when 'wrong_item'       then 'product_issue'
            when 'not_as_described' then 'product_issue'
            when 'quality_issue'    then 'product_issue'
            when 'arrived_late'     then 'fulfillment_issue'
            when 'size_mismatch'    then 'fit_issue'
            when 'color_different' then 'fit_issue'
            when 'changed_mind'     then 'customer_driven'
            else                         'other'
        end                                                    as reason_category
    from r
    left join o on r.order_id = o.order_id
)
select
    *,
    case
        when days_to_return is null then null
        when days_to_return between 0 and {{ window }} then true
        else false
    end                                                        as is_within_policy
from joined
