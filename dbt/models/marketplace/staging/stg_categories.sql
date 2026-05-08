{{ config(materialized='view') }}

select
    category_id,
    category_name,
    parent_category,
    try_cast(return_window_days as integer)  as return_window_days,
    case lower(trim(is_high_return)) when 'true' then true when 'false' then false else null end as is_high_return
from {{ source('marketplace_raw', 'categories') }}
