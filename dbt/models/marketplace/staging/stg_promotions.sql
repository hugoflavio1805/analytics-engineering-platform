{{ config(materialized='view') }}

select
    promotion_id,
    promotion_name,
    lower(promotion_type)                       as promotion_type,
    try_cast(discount_value as integer)         as discount_value,
    try_cast(starts_at as date)                 as starts_at,
    try_cast(ends_at as date)                   as ends_at,
    case lower(is_aggressive) when 'true' then true else false end as is_aggressive
from {{ source('marketplace_raw', 'promotions') }}
