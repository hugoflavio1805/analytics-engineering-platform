{{ config(materialized='view') }}

with src as (
    select * from {{ source('marketplace_raw', 'returns') }}
)
select
    return_id,
    order_id,
    try_cast(return_date as date)                 as return_date,
    lower(reason)                                 as reason,
    try_cast(refund_amount as decimal(12, 2))     as refund_amount,
    category_primary,
    ship_region,
    case lower(trim(processed))
        when 'true'  then true
        when 'false' then false
        else null
    end                                           as processed
from src
