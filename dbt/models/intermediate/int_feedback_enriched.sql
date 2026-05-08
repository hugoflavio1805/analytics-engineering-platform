-- Enriches each feedback event with derivations:
--   - sentiment_score (+1/-1/0 from sentiment)
--   - is_negative / is_positive flags (from type)
--   - nps_band (promoter/passive/detractor)
--   - days_before_churn (relative to the customer's latest_churn_date)
--   - is_pre_churn_90d (within the configured pre-churn window)

{% set window_days = var('feedback_pre_churn_window_days', 90) %}

with f as (
    select * from {{ ref('stg_feedbacks') }}
),
churn as (
    select
        customer_id,
        latest_churn_date
    from {{ ref('int_customer_subscription_rollup') }}
    where latest_churn_date is not null
)
select
    f.feedback_id,
    f.customer_id,
    f.type,
    f.channel,
    f.sentiment,
    f.message,
    f.created_at as feedback_date,
    f.nps_score,
    f.resolved,
    length(f.message)                                                       as message_length,

    case f.sentiment
        when 'positive' then 1
        when 'negative' then -1
        when 'neutral'  then 0
    end                                                                     as sentiment_score,

    f.type in ('bug', 'complaint')                                          as is_negative,
    f.type = 'praise'                                                        as is_positive,

    case
        when f.nps_score is null then null
        when f.nps_score >= 9    then 'promoter'
        when f.nps_score >= 7    then 'passive'
        else                          'detractor'
    end                                                                     as nps_band,

    case
        when churn.latest_churn_date is null then null
        when f.created_at > churn.latest_churn_date then null
        else date_diff('day', f.created_at, churn.latest_churn_date)
    end                                                                     as days_before_churn,

    case
        when churn.latest_churn_date is null then false
        when f.created_at > churn.latest_churn_date then false
        when date_diff('day', f.created_at, churn.latest_churn_date) <= {{ window_days }} then true
        else false
    end                                                                     as is_pre_churn_90d
from f
left join churn using (customer_id)
