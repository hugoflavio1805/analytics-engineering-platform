{{ config(materialized='table') }}

-- Returns mart for the obligatory question.
-- One row per (category × region).

with orders_in_scope as (
    select *
    from {{ ref('fct_orders') }}
    where order_status in ('delivered', 'returned')
      and category_primary is not null
),
geo_region as (
    select country_code, region from {{ ref('stg_geography') }}
),
order_agg as (
    select
        o.category_id,
        o.category_primary                       as category,
        coalesce(g.region, 'Other')              as region,
        count(*)                                 as orders_total,
        sum(o.total)                             as gmv,
        avg(o.total)                             as avg_order_value
    from orders_in_scope o
    left join geo_region g on o.ship_country = g.country_code
    group by 1, 2, 3
),
return_agg as (
    select
        r.category_id,
        r.category_primary                       as category,
        coalesce(g.region, 'Other')              as region,
        count(*)                                 as returns_total,
        sum(r.refund_amount)                     as refund_total,
        sum(case when r.is_within_policy = false then 1 else 0 end)         as returns_outside_policy,
        sum(case when r.reason_category = 'product_issue' then 1 else 0 end) as returns_product_issue,
        sum(case when r.reason_category = 'fit_issue' then 1 else 0 end)     as returns_fit_issue,
        sum(case when r.reason_category = 'customer_driven' then 1 else 0 end) as returns_customer_driven,
        sum(case when r.reason_category = 'fulfillment_issue' then 1 else 0 end) as returns_fulfillment_issue
    from {{ ref('fct_returns') }} r
    left join geo_region g on r.customer_country_code = g.country_code
    where r.category_primary is not null
    group by 1, 2, 3
)
select
    o.category_id,
    o.category,
    cat.parent_category,
    cat.is_high_return                              as expected_high_return,
    o.region,
    o.orders_total,
    o.gmv,
    o.avg_order_value,
    coalesce(r.returns_total, 0)                     as returns_total,
    coalesce(r.refund_total, 0)                      as refund_total,
    case when o.orders_total > 0
        then 1.0 * coalesce(r.returns_total, 0) / o.orders_total
        else 0
    end                                              as return_rate,
    case when o.gmv > 0
        then coalesce(r.refund_total, 0) / o.gmv
        else 0
    end                                              as gmv_loss_rate,
    coalesce(r.returns_product_issue, 0)             as returns_product_issue,
    coalesce(r.returns_fit_issue, 0)                 as returns_fit_issue,
    coalesce(r.returns_customer_driven, 0)           as returns_customer_driven,
    coalesce(r.returns_fulfillment_issue, 0)         as returns_fulfillment_issue,
    coalesce(r.returns_outside_policy, 0)            as returns_outside_policy
from order_agg o
left join return_agg r using (category_id, category, region)
left join {{ ref('stg_categories') }} cat using (category_id)
order by return_rate desc
