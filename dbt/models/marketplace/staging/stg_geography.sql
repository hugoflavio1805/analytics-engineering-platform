{{ config(materialized='view') }}

select
    upper(trim(country_code))   as country_code,
    country_name,
    region,
    continent,
    upper(currency)             as currency,
    lower(language)             as language,
    try_cast(is_eu as boolean)  as is_eu
from {{ source('marketplace_raw', 'geography') }}
