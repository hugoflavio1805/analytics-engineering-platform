{{ config(materialized='table') }}

-- Semantic mart for the obligatory question:
-- "Is there a relationship between feedback type and churn, and how does it vary by plan segment?"

with feedbacks_by_type as (
    select
        customer_id,
        segment,
        plan,
        sum(case when type = 'bug' then 1 else 0 end)             as bugs,
        sum(case when type = 'complaint' then 1 else 0 end)       as complaints,
        sum(case when type = 'praise' then 1 else 0 end)          as praises,
        sum(case when type = 'feature_request' then 1 else 0 end) as feature_requests,
        count(*)                                                   as total_feedbacks,
        sum(case when is_pre_churn_90d then 1 else 0 end)         as pre_churn_feedbacks
    from {{ ref('fct_feedbacks') }}
    group by 1, 2, 3
),
customer_outcome as (
    select
        d.customer_id,
        d.segment,
        d.plan,
        case when d.customer_status = 'churned' or not d.has_active_subscription then true else false end as is_churned,
        d.tenure_days,
        d.current_mrr
    from {{ ref('dim_customer') }} d
)
select
    o.customer_id,
    o.segment,
    o.plan,
    o.is_churned,
    o.tenure_days,
    o.current_mrr,
    coalesce(f.bugs, 0)              as bugs,
    coalesce(f.complaints, 0)        as complaints,
    coalesce(f.praises, 0)           as praises,
    coalesce(f.feature_requests, 0)  as feature_requests,
    coalesce(f.total_feedbacks, 0)   as total_feedbacks,
    coalesce(f.pre_churn_feedbacks, 0) as pre_churn_feedbacks,
    case
        when coalesce(f.total_feedbacks, 0) = 0 then 0
        else (coalesce(f.bugs, 0) + coalesce(f.complaints, 0)) * 1.0 / f.total_feedbacks
    end as negative_feedback_ratio
from customer_outcome o
left join feedbacks_by_type f on o.customer_id = f.customer_id
