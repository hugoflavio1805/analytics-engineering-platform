# Tableau Public Dashboard

> Link will be added once the dashboard is published.

Dataset feed: `marts.analytics.customer_feedback_churn` (one row per customer).

## Sheets

1. **Executive view** — Churn rate by segment with a side-bar of negative-feedback ratio.
2. **Heatmap** — feedback type × segment, color = lift over baseline churn.
3. **Pre-churn timeline** — for churned customers, count of feedbacks per type in the 90 days before `latest_churn_date`.
4. **MRR at risk** — sum of `current_mrr` for customers with `negative_feedback_ratio > 0.5`.

## How to refresh

The dashboard pulls from a published Snowflake view (or local CSV export from DuckDB). Refresh runs after the daily Airflow DAG green-lights it.
