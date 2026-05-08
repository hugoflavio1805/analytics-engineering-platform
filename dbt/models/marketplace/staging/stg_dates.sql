{{ config(materialized='view') }}

select
    try_cast(date_key as date)              as date_key,
    try_cast(year as integer)               as year,
    try_cast(month as integer)              as month,
    try_cast(day as integer)                as day,
    day_of_week,
    quarter,
    cast(is_weekend       as boolean) as is_weekend,
    cast(is_black_friday  as boolean) as is_black_friday,
    cast(is_cyber_monday  as boolean) as is_cyber_monday,
    cast(is_christmas_eve as boolean) as is_christmas_eve,
    cast(is_new_year      as boolean) as is_new_year
from {{ source('marketplace_raw', 'dates') }}
