

select
    category_id,
    category_name,
    parent_category,
    try_cast(return_window_days as integer)  as return_window_days,
    try_cast(is_high_return as boolean)      as is_high_return
from {{ source('marketplace_raw', 'categories') }}
