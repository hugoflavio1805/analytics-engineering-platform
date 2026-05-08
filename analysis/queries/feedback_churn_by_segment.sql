-- Answers the obligatory question.
-- Runs unchanged on DuckDB and Snowflake.

with base as (
    select * from marts_analytics.customer_feedback_churn
)
select
    segment,
    plan,
    count(*)                                                              as customers,
    sum(case when is_churned then 1 else 0 end)                           as churned,
    1.0 * sum(case when is_churned then 1 else 0 end) / count(*)          as churn_rate,

    -- churn rate among customers with at least one negative feedback
    1.0
      * sum(case when is_churned and (bugs + complaints) > 0 then 1 else 0 end)
      / nullif(sum(case when (bugs + complaints) > 0 then 1 else 0 end), 0)
      as churn_rate_with_negative_feedback,

    -- lift vs. baseline
    (1.0 * sum(case when is_churned and (bugs + complaints) > 0 then 1 else 0 end)
        / nullif(sum(case when (bugs + complaints) > 0 then 1 else 0 end), 0))
    /
    nullif(1.0 * sum(case when is_churned then 1 else 0 end) / count(*), 0)
      as lift
from base
group by segment, plan
order by churn_rate desc
