{{ config(materialized='view') }}

-- One row per feedback event. Staging only normalizes — no derivations.

with src as (
    select * from {{ source('raw', 'feedbacks') }}
)
select
    feedback_id,
    upper(replace(trim(customer_id), '_', '-'))   as customer_id,
    lower(type)                                   as type,
    lower(channel)                                as channel,
    lower(sentiment)                              as sentiment,
    message,
    try_cast(created_at as date)                  as created_at,
    try_cast(nps_score as integer)                as nps_score,
    try_cast(resolved as boolean)                 as resolved
from src

-- Drop hard duplicates: same customer + same message + same date.
-- Keeps the lowest feedback_id deterministically.
qualify row_number() over (
    partition by
        upper(replace(trim(customer_id), '_', '-')),
        message,
        try_cast(created_at as date)
    order by feedback_id
) = 1
