{{ config(materialized='view') }}

with src as (
    select * from {{ source('raw', 'customers') }}
)
select
    upper(replace(trim(customer_id), '_', '-'))         as customer_id,
    name,
    lower(trim(email))                                  as email,
    lower(plan)                                         as plan,
    segment,
    lower(status)                                       as status,
    try_cast(created_at as date)                        as created_at,
    upper(country)                                      as country,
    cast(tenure_days as integer)                        as tenure_days,
    cast(is_active_paying as boolean)                   as is_active_paying,
    cast(total_mrr as integer)                          as total_mrr,
    mrr_band,
    cast(subscriptions_count as integer)                as subscriptions_count,
    try_cast(churn_date as date)                        as churn_date,
    try_cast(days_to_churn as integer)                  as days_to_churn,
    cast(feedback_count as integer)                     as feedback_count,
    cast(negative_feedback_count as integer)            as negative_feedback_count,
    cast(praise_count as integer)                       as praise_count,
    cast(feature_request_count as integer)              as feature_request_count,
    cast(churn_risk_score as double)                    as churn_risk_score
from src
