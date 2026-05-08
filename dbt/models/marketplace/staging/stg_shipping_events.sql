{{ config(materialized='view') }}

select
    event_id,
    order_id,
    carrier_id,
    lower(event_type)                  as event_type,
    try_cast(event_at as timestamp)    as event_at
from {{ source('marketplace_raw', 'shipping_events') }}
