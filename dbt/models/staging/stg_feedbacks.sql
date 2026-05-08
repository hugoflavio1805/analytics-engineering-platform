{{ config(materialized='view') }}

with src as (
    select * from {{ source('raw', 'feedbacks') }}
),
normalized as (
    select
        feedback_id,
        upper(replace(trim(customer_id), '_', '-'))       as customer_id,
        lower(type)                                       as type,
        lower(channel)                                    as channel,
        lower(sentiment)                                  as sentiment,
        cast(sentiment_score as integer)                  as sentiment_score,
        message,
        cast(message_length as integer)                   as message_length,
        try_cast(created_at as date)                      as created_at,
        try_cast(nps_score as integer)                    as nps_score,
        nps_band,
        case lower(resolved) when 'true' then true when 'false' then false else null end as resolved,
        cast(is_negative as boolean)                      as is_negative,
        cast(is_positive as boolean)                      as is_positive,
        try_cast(days_before_churn as integer)            as days_before_churn,
        cast(is_pre_churn_90d as boolean)                 as is_pre_churn_90d,
        cast(is_duplicate_candidate as boolean)           as is_duplicate_candidate
    from src
)
select *
from normalized
-- Drop hard duplicates (same customer + same message + same day)
qualify row_number() over (partition by customer_id, message, created_at order by feedback_id) = 1
