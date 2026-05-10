

-- One row per customer.
-- Responsibilities of this staging layer (and only these):
--   1. Type-cast columns
--   2. Normalize identifiers (uppercase, hyphen separator)
--   3. Trim whitespace, lowercase emails
-- All business derivations (tenure, mrr_band, churn_risk_score, etc.)
-- live downstream in intermediate / marts — never here.

with src as (
    select * from {{ source('raw', 'customers') }}
)
select
    upper(replace(trim(customer_id), '_', '-'))  as customer_id,
    name,
    lower(trim(email))                           as email,
    lower(plan)                                  as plan,
    segment,
    lower(status)                                as status,
    try_cast(created_at as date)                 as created_at,
    upper(country)                               as country
from src
