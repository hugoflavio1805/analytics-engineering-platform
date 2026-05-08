{{ config(materialized='table') }}

-- Semantic mart for the obligatory question:
-- "Is there a relationship between feedback type and churn,
--  and how does it vary by plan segment?"
--
-- One row per customer, with everything the dashboard needs.
-- The dashboard SHOULD NOT compute anything on top of this.

with feedbacks_by_type as (
    select
        customer_id,
        sum(case when type = 'bug'             then 1 else 0 end) as bugs,
        sum(case when type = 'complaint'       then 1 else 0 end) as complaints,
        sum(case when type = 'praise'          then 1 else 0 end) as praises,
        sum(case when type = 'feature_request' then 1 else 0 end) as feature_requests,
        sum(case when type = 'support'         then 1 else 0 end) as support_requests,
        count(*)                                                  as total_feedbacks,
        sum(case when is_pre_churn_90d then 1 else 0 end)         as pre_churn_feedbacks
    from {{ ref('fct_feedbacks') }}
    group by customer_id
)
select
    d.customer_id,
    d.segment,
    d.plan,
    d.is_churned,
    d.tenure_days,
    d.current_mrr,
    d.mrr_band,

    coalesce(f.bugs, 0)              as bugs,
    coalesce(f.complaints, 0)        as complaints,
    coalesce(f.praises, 0)           as praises,
    coalesce(f.feature_requests, 0)  as feature_requests,
    coalesce(f.support_requests, 0)  as support_requests,
    coalesce(f.total_feedbacks, 0)   as total_feedbacks,
    coalesce(f.pre_churn_feedbacks, 0) as pre_churn_feedbacks,

    coalesce(f.bugs, 0) + coalesce(f.complaints, 0) as negative_feedback_count,

    case
        when coalesce(f.total_feedbacks, 0) = 0 then 0.0
        else cast(coalesce(f.bugs, 0) + coalesce(f.complaints, 0) as double)
             / f.total_feedbacks
    end as negative_feedback_ratio,

    -- 0..1 score: share of feedbacks that are negative.
    -- Same definition as negative_feedback_ratio, kept as a separate column
    -- because it's the metric the dashboard surfaces under the "risk" label.
    case
        when coalesce(f.total_feedbacks, 0) = 0 then 0.0
        else cast(coalesce(f.bugs, 0) + coalesce(f.complaints, 0) as double)
             / f.total_feedbacks
    end as churn_risk_score
from {{ ref('dim_customer') }} d
left join feedbacks_by_type f using (customer_id)
