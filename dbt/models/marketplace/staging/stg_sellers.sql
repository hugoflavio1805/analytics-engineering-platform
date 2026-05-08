{{ config(materialized='view') }}

-- Sellers staging:
--   - normalize seller_id casing/separator
--   - uppercase country codes (some came in lowercase)
--   - cast booleans

with src as (
    select * from {{ source('marketplace_raw', 'sellers') }}
)
select
    upper(replace(trim(seller_id), '_', '-'))    as seller_id,
    seller_name,
    upper(trim(country))                         as country,
    try_cast(is_verified as boolean)             as is_verified,
    try_cast(join_date as date)                  as join_date,
    nullif(trim(contact_email), '')              as contact_email
from src
