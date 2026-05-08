{{ config(materialized='table') }}

select
    country_code,
    country_name,
    region,
    continent,
    currency,
    language,
    is_eu
from {{ ref('stg_geography') }}
