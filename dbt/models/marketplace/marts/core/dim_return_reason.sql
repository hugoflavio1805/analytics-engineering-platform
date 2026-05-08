{{ config(materialized='table') }}

select
    reason_code,
    reason_label,
    reason_category,
    is_actionable_by_seller
from {{ ref('stg_return_reasons') }}
