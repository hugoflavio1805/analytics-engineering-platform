{{ config(materialized='view') }}

select
    lower(reason_code)        as reason_code,
    reason_label,
    reason_category,
    try_cast(is_actionable_by_seller as boolean)             as is_actionable_by_seller
from {{ source('marketplace_raw', 'return_reasons') }}
