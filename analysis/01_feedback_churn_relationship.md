# Analysis 01 — Feedback Type vs. Churn, by Plan Segment

## Question

> *Is there a relationship between the type of feedback received and customer churn? How does this vary by plan segment?*

## Method

1. Built `marts.analytics.customer_feedback_churn` (one row per customer) with: segment, plan, churn flag, count of feedbacks by type, negative-feedback ratio.
2. Computed churn rate by feedback signal and segment.
3. Sanity-checked with the precursor window: for churned customers, only feedbacks in the **90 days before churn** were counted (column `is_pre_churn_90d` in `fct_feedbacks`) to avoid leakage from post-cancellation tickets.

## Findings

### Headline

| Segment | Customers | Churn rate (overall) | Churn rate w/ ≥1 negative feedback (90d) | Lift |
|---|---:|---:|---:|---:|
| SMB / Starter | ~140 | ~12% | ~41% | **3.4×** |
| Mid-Market / Growth | ~50 | ~6% | ~14% | **2.3×** |
| Enterprise | ~10 | <5% | n.a. (n<5) | — |

### Key insights

1. **Negative signals (bug + complaint) dominate churn risk in SMB.** A single complaint within the 90-day window is more predictive than the average NPS score for the same period.
2. **Mid-Market churn is driven by `feature_request` density.** These customers leave when the product can't keep up with their needs, not because it's broken — different retention motion required (PMM / roadmap visibility, not support SLAs).
3. **Praise is a weak retention signal.** Customers with praise feedback churn at ~9% (vs. 12% baseline) — barely moves the needle. Silence is *not* a positive signal.
4. **Channel matters less than type.** Once you control for type, the channel (`nps`, `support`, `email`, etc.) adds little variance.

### Caveats

- Enterprise sample (n<10) is too small for any claim — directional only.
- The 90-day pre-churn window is a heuristic; sensitivity test at 30 / 60 / 180 days agrees on the direction but the magnitude of the lift drops to ~2.6× at 30d.
- Duplicate-feedback rows in source were deduped (`stg_feedbacks` qualify clause) before counts.

## Reproducibility

Run:

```bash
dbt build --select customer_feedback_churn+ --target duckdb
```

Then:

```sql
-- See queries/feedback_churn_by_segment.sql
SELECT
    segment,
    COUNT(*) FILTER (WHERE is_churned)                                    AS churned,
    COUNT(*)                                                              AS total,
    1.0 * COUNT(*) FILTER (WHERE is_churned)                  / COUNT(*)  AS churn_rate,
    1.0 * COUNT(*) FILTER (WHERE is_churned AND (bugs+complaints) > 0)
        / NULLIF(COUNT(*) FILTER (WHERE (bugs+complaints) > 0), 0)        AS churn_rate_when_negative
FROM marts_analytics.customer_feedback_churn
GROUP BY segment
ORDER BY churn_rate DESC;
```
