{{ config(materialized='view') }}

select
    try_cast(date_key as date)              as date_key,
    try_cast(year as integer)               as year,
    try_cast(month as integer)              as month,
    try_cast(day as integer)                as day,
    day_of_week,
    quarter,
    case lower(is_weekend)        when 'true' then true else false end as is_weekend,
    case lower(is_black_friday)   when 'true' then true else false end as is_black_friday,
    case lower(is_cyber_monday)   when 'true' then true else false end as is_cyber_monday,
    case lower(is_christmas_eve)  when 'true' then true else false end as is_christmas_eve,
    case lower(is_new_year)       when 'true' then true else false end as is_new_year
from {{ source('marketplace_raw', 'dates') }}
