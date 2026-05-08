{{ config(materialized='view') }}

select
    upper(trim(country_code))   as country_code,
    country_name,
    region,
    continent,
    upper(currency)             as currency,
    lower(language)             as language,
    case lower(trim(is_eu)) when 'true' then true when 'false' then false else null end as is_eu
from {{ source('marketplace_raw', 'geography') }}
