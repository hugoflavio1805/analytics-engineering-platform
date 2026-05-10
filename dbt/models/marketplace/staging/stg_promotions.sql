

select
    promotion_id,
    promotion_name,
    lower(promotion_type)                       as promotion_type,
    try_cast(discount_value as integer)         as discount_value,
    try_cast(starts_at as date)                 as starts_at,
    try_cast(ends_at as date)                   as ends_at,
    try_cast(is_aggressive as boolean)          as is_aggressive
from {{ source('marketplace_raw', 'promotions') }}
