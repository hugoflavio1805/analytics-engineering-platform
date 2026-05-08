{{ config(materialized='view') }}

select
    lower(reason_code)        as reason_code,
    reason_label,
    reason_category,
    case lower(trim(is_actionable_by_seller)) when 'true' then true when 'false' then false else null end as is_actionable_by_seller
from {{ source('marketplace_raw', 'return_reasons') }}
