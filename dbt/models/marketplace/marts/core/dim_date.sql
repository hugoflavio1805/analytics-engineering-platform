{{ config(materialized='table') }}

select
    date_key,
    year,
    month,
    day,
    day_of_week,
    quarter,
    is_weekend,
    is_black_friday,
    is_cyber_monday,
    is_christmas_eve,
    is_new_year,
    is_black_friday or is_cyber_monday   as is_promo_peak_day
from {{ ref('stg_dates') }}
