{{ config(
    materialized='incremental',
    unique_key='feedback_id',
    on_schema_change='append_new_columns'
) }}

-- One row per feedback event, with plan/segment from dim_customer
-- and all derivations from the intermediate model.

select
    f.feedback_id,
    f.customer_id,
    c.plan,
    c.segment,
    f.type,
    f.channel,
    f.sentiment,
    f.sentiment_score,
    f.nps_score,
    f.nps_band,
    f.resolved,
    f.is_negative,
    f.is_positive,
    f.is_pre_churn_90d,
    f.days_before_churn,
    f.message_length,
    f.feedback_date,
    current_timestamp as _loaded_at
from {{ ref('int_feedback_enriched') }} f
left join {{ ref('dim_customer') }} c using (customer_id)

{% if is_incremental() %}
where f.feedback_date > (select coalesce(max(feedback_date), date '1900-01-01') from {{ this }})
{% endif %}
