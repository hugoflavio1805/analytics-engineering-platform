{{ config(materialized='view') }}

-- Orders staging — handles the mixed-format order_date issue:
--   most rows: yyyy-MM-dd ; ~4% rows: dd/MM/yyyy (legacy import).
-- Try ISO first; if that fails, parse the slash form. Anything else -> NULL,
-- caught by the int_orders_clean filter.

with src as (
    select * from {{ source('marketplace_raw', 'orders') }}
),
parsed as (
    select
        *,
        coalesce(
            try_cast(order_date as date),
            try_cast(strptime(order_date, '%d/%m/%Y') as date)
        ) as order_date_parsed
    from src
)
select
    order_id,
    customer_id,
    order_date_parsed                                as order_date,
    lower(status)                                    as status,
    try_cast(ship_date as date)                      as ship_date,
    try_cast(delivered_date as date)                 as delivered_date,
    upper(trim(ship_country))                        as ship_country,
    nullif(trim(carrier_id), '')                     as carrier_id,
    nullif(trim(carrier), '')                        as carrier_name,
    nullif(trim(promotion_id), '')                   as promotion_id,
    try_cast(items_count as integer)                 as items_count,
    try_cast(subtotal as decimal(12, 2))             as subtotal,
    try_cast(shipping_cost as decimal(12, 2))        as shipping_cost,
    try_cast(total as decimal(12, 2))                as total
from parsed
