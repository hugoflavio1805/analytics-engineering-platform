# Customer Feedback × Revenue Analytics — Data Engineering Challenge

> End-to-end analytics engineering pipeline that integrates customer, subscription and multi-channel feedback data to answer one strategic question:
> **"Is there a relationship between feedback type and customer churn, and how does that vary by plan segment?"**

[![CI](https://github.com/hugoflavio1805/customer-feedback-revenue-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/hugoflavio1805/customer-feedback-revenue-analytics/actions)
[![dbt](https://img.shields.io/badge/dbt-1.8-orange)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-ready-29B5E8)](https://www.snowflake.com/)
[![Airflow](https://img.shields.io/badge/Airflow-2.9-017CEE)](https://airflow.apache.org/)

---

## 1. Executive Summary (TL;DR)

A SaaS company collects customer feedback from five channels (form, support, platform, email, NPS) and wants to cross-reference it with revenue (MRR, plan, subscription status) to drive retention decisions.

This repository delivers a **production-shaped data pipeline**:

| Layer | Tool | Purpose |
|---|---|---|
| **Ingestion** | Python + DuckDB / Snowflake `COPY INTO` | Load raw CSVs idempotently |
| **Transformation** | **dbt** (staging → intermediate → marts) | Clean, conform, model |
| **Warehouse** | **Snowflake** (prod) / **DuckDB** (local) / **PostgreSQL** (dev) | Same dbt code runs in all three |
| **Orchestration** | **Apache Airflow** (DAG) | Schedules: extract → load → dbt run → dbt test → freshness |
| **Quality** | dbt tests + freshness + custom singular tests | Block bad data before BI |
| **CI/CD** | **GitHub Actions** | Lint (sqlfluff), dbt build on PR, dbt docs deploy on merge |
| **BI** | **Tableau Public** + **Metabase** (Docker) | Executive dashboard |
| **Packaging** | **Docker Compose** | One-command local stack |

**Headline finding (preview):** customers who issue ≥1 `bug` or `complaint` feedback in the 90 days before churn are **~3.4×** more likely to cancel than the baseline; the effect is concentrated in the **Starter / SMB** segment, while **Enterprise** churn correlates more with `feature_request` density (signal of unmet needs, not dissatisfaction).

---

## 2. Architecture

```
                        ┌──────────────────────────────────────────────┐
                        │                  Airflow DAG                 │
                        │  feedback_revenue_pipeline (daily 06:00 UTC) │
                        └──────────────────────────────────────────────┘
                                              │
            ┌─────────────────┬───────────────┼────────────────┬─────────────────┐
            ▼                 ▼               ▼                ▼                 ▼
     [extract_csvs]    [load_to_warehouse]  [dbt_run]      [dbt_test]      [publish_dashboard]
            │                 │               │                │                 │
            ▼                 ▼               ▼                ▼                 ▼
   raw/*.csv (S3 sim)   RAW.* tables      STG → INT → MARTS  44 tests OK    Tableau / Metabase

  ┌─────────────────────────────── dbt project layout ───────────────────────────────┐
  │  models/staging/   one-to-one with RAW, type-cast, deduplicated, light renames    │
  │  models/intermediate/  conformed dims & business logic (e.g. customer_lifecycle)  │
  │  models/marts/core/    fct_feedbacks, fct_subscriptions, dim_customer (gold)      │
  │  models/marts/analytics/  semantic layer used by BI (one-row-per-question)        │
  └───────────────────────────────────────────────────────────────────────────────────┘
```

### Why Snowflake (and how DuckDB stands in for it locally)

Snowflake is the production target because:

- **Separation of compute/storage** → cheap to keep all history, scale up only for `dbt run`.
- **Zero-copy clones** → CI spins a clone of `PROD` in 1 second to run tests against real data shape.
- **Snowpipe / `COPY INTO`** → fits the daily CSV ingestion pattern with minimal code.
- **`QUALIFY` + window functions** → makes deduplication of feedback events trivial.

Locally we run **DuckDB** because the dbt SQL is ~99% portable (window functions, `qualify`, CTEs, `try_cast`). The same `models/` directory builds against either adapter — selected via dbt `target`. **Redshift equivalence:** the same code runs on Redshift with `dbt-redshift` (the only differences are `OBJECT` vs `SUPER` for nested types and `MERGE` syntax — both isolated to two macros in `macros/cross_warehouse/`).

---

## 3. The Data

Three source files in `data/raw/` (200 customers, 215 subscriptions, 229 feedbacks):

| File | Grain | Notes |
|---|---|---|
| `customers.csv` | one row per customer | plan, segment, status, country, created_at |
| `subscriptions.csv` | one row per subscription | MRR, billing cycle, start/end dates |
| `feedbacks.csv` | one row per feedback event | type, channel, sentiment, NPS, resolved flag |

### Known data quality issues (intentional — part of the challenge)

| # | Issue | Where | Treatment |
|---|---|---|---|
| 1 | `customer_id` uses both `CUST-0178` and `CUST_0178` | feedbacks | Normalize to hyphen in `stg_feedbacks` |
| 2 | `subscription_id` mixes `sub_0001` and `SUB-0001` | subscriptions | Lower-case + hyphen |
| 3 | Duplicate feedback rows (same customer, same message, same day) | feedbacks | `qualify row_number() over (partition by …) = 1` |
| 4 | NPS score blank for non-NPS channels | feedbacks | Kept as `NULL`, documented (`accepted_values` test scoped to channel='nps') |
| 5 | `resolved` is `True` / `False` / blank | feedbacks | Cast to `BOOLEAN`, blank → `NULL` (means "not applicable") |
| 6 | Customers without active subscription | join | LEFT JOIN; flagged `is_active_paying = false` rather than dropped |
| 7 | `end_date` in the future for cancelled rows | subscriptions | Kept; `churn_date = least(end_date, current_date)` |
| 8 | Free-text `message` with PII risk | feedbacks | Hash + length only in `dim_feedback`; raw text only in `stg_*` (restricted role) |

All issues are codified as **dbt tests** so they fail loudly if the next batch reintroduces them.

---

## 4. Data Model (gold layer)

```
  raw.customers   raw.subscriptions   raw.feedbacks            ← landing
       │                 │                  │
       ▼                 ▼                  ▼
  stg_customers   stg_subscriptions    stg_feedbacks            ← staging (cast + normalize)
       │                 │                  │
       │     int_customer_subscription_rollup                   ← intermediate
       │                 │              int_feedback_enriched
       │                 │                  │
       └────────┬────────┘                  │
                ▼                           │
           dim_customer ─── fct_subscriptions
                │                           │
                └───────────┬───────────────┘
                            ▼
                  fct_feedbacks
                            │
                            ▼
                customer_feedback_churn                         ← analytics mart
                (1 row per customer; consumed by Tableau)
```

Two principles enforced by the layering:

1. **Raw stays raw.** The CSVs in `data/raw/` are the schema the source system would actually emit — no derived columns, no analytical hints. Anything calculable belongs in dbt.
2. **Dashboards are dumb.** `customer_feedback_churn` already contains every metric the BI surfaces; the dashboard paints, it does not compute. If the metric needs to change, it changes in one place — in dbt — with a test and a PR review.

---

## 5. The Obligatory Analysis

> *"Is there a relationship between feedback type and customer churn? How does it vary by plan segment?"*

See [`analysis/01_feedback_churn_relationship.md`](analysis/01_feedback_churn_relationship.md) for the full write-up. Highlights:

- **Bug + complaint density** is the single strongest leading indicator of churn for SMB / Starter (lift 3.4×).
- **Praise** correlates with retention, but the effect is dwarfed by absence of negative signals — silence is *not* a positive signal.
- **Mid-Market / Growth** plan churns less overall, but when it does, the precursor is `feature_request` volume, not complaints — these customers leave because the product *isn't enough*, not because it's broken.
- **Enterprise** sample is small (n<20) — directional only.

The exact SQL (runs in DuckDB and Snowflake) lives in [`analysis/queries/feedback_churn_by_segment.sql`](analysis/queries/feedback_churn_by_segment.sql).

---

## 6. Dashboard

- **Tableau Public:** [link to be filled after publish] — executive view (churn-rate by segment × feedback signal).
- **Metabase (local):** `docker compose up metabase` → `http://localhost:3000` — operational drill-down.

Both dashboards read **only** from `marts.analytics.*` — no ad-hoc joins.

---

## 7. Quality & Observability

| Test type | Count | Where |
|---|---|---|
| `unique` / `not_null` | 18 | every PK + business key |
| `relationships` | 6 | every FK across marts |
| `accepted_values` | 9 | enum-like columns (plan, segment, status, sentiment, channel) |
| Custom singular | 5 | e.g. *"no feedback may reference a customer that doesn't exist after id-normalization"* |
| **Source freshness** | 3 | warns at 26h, errors at 48h since last load |
| **Incremental models** | 2 | `fct_feedbacks` and `customer_health_daily` use `incremental` with `on_schema_change='append_new_columns'` — re-runs cost cents on Snowflake |

Everything runs on every PR via GitHub Actions. Failures block merge.

---

## 8. Running it Locally

### Option A — Docker (recommended)

```bash
git clone https://github.com/hugoflavio1805/customer-feedback-revenue-analytics.git
cd customer-feedback-revenue-analytics
docker compose up --build
```

That spins up: Postgres + Airflow webserver/scheduler + Metabase, and runs `dbt build` against DuckDB on first start. Open:

- Airflow UI → `http://localhost:8080` (admin / admin)
- Metabase → `http://localhost:3000`
- dbt docs → `http://localhost:8081`

### Option B — Bare metal

```bash
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
dbt deps && dbt seed && dbt build --target duckdb
dbt docs generate && dbt docs serve
```

### Switching to Snowflake

```bash
export DBT_PROFILE_TARGET=snowflake
# fill ~/.dbt/profiles.yml with your account / user / role / warehouse
dbt build --target snowflake
```

The same models run unchanged.

---

## 9. CI/CD

Every PR runs (`.github/workflows/ci.yml`):

1. `sqlfluff lint` — SQL style
2. `dbt deps` + `dbt parse` — catches Jinja errors
3. `dbt build --target ci` — full build + tests against an ephemeral DuckDB
4. Comments the test summary on the PR

On merge to `main`:

5. `dbt docs generate` → published to GitHub Pages
6. (production deploy step is stubbed — would `dbt build --target prod` against Snowflake)

---

## 10. Key Decisions & Trade-offs

| Decision | Why | Trade-off |
|---|---|---|
| **dbt + DuckDB locally**, Snowflake in prod | One codebase, $0 to develop | Two adapters to keep tested in CI |
| **Hash PII in marts**, keep raw in staging | GDPR-friendly defaults | Joining back to raw text needs elevated role |
| **One semantic mart per question** (`customer_feedback_churn`) | Dashboards stay dumb, logic is testable | More tables to maintain |
| **Airflow over a cron + script** | Retries, SLAs, lineage view | Heavier infra footprint |
| **Incremental** for fact tables only | Fast nightly runs | Backfills need `--full-refresh` |
| **Tableau Public + Metabase** (both) | Tableau for execs, Metabase for engineers | Two BI surfaces to keep aligned |

---

## 11. What I'd Do With More Time

1. **Reverse ETL** of `customer_health_daily` back to the CRM so CSMs see churn risk in their tool.
2. **Great Expectations** alongside dbt tests for column-distribution monitoring (not just constraints).
3. **dbt semantic layer / MetricFlow** so MRR and churn-rate definitions live in one place across BI tools.
4. **Anomaly detection** on feedback volume per channel (sudden silence on `support` is itself a signal).
5. **Cohort retention curves** by acquisition month, materialized.

---

## 12. Repository Layout

```
.
├── data/raw/                      # raw CSVs (300 customers / 328 subs / 435 feedbacks); originals + synthetic rows
├── data/raw_original/             # untouched copy of the original 200/215/229 rows
├── scripts/generate_synthetic_data.ps1  # appends synthetic rows preserving raw schema and quality issues
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/               # 1:1 with raw, cleaned
│   │   ├── intermediate/          # conformed business logic
│   │   └── marts/
│   │       ├── core/              # dim_customer, fct_*
│   │       └── analytics/         # one mart per question
│   ├── tests/                     # custom singular tests
│   ├── macros/cross_warehouse/    # snowflake/duckdb/redshift portability
│   └── seeds/
├── dags/
│   └── feedback_revenue_pipeline.py
├── docker/
│   ├── Dockerfile.dbt
│   └── Dockerfile.airflow
├── docker-compose.yml
├── .github/workflows/ci.yml
├── analysis/
│   ├── 01_feedback_churn_relationship.md
│   └── queries/
├── dashboards/
│   └── tableau_public_link.md
├── sql/                           # ad-hoc exploration queries
└── README.md
```

---

## 13. Cost & Time Estimate (for the reviewer)

| Item | Estimate |
|---|---|
| Local-only run | **$0** — DuckDB + Docker on a laptop |
| Snowflake (XS warehouse, daily run) | **~$15–25 / month** for this volume; ~$80 / month at 10× scale |
| First end-to-end build (clone → green CI → dashboard) | **~6 minutes** |
| Time invested in this challenge | ~6 hours scoped (MVP) + extra polish for the 10/10 stretch goals |

---

## 14. Author

Developed by **Hugo Flávio Santos**
Data Engineer | Analytics Engineering | Data Platform

[hugoflavio1805@hotmail.com](mailto:hugoflavio1805@hotmail.com) · [github.com/hugoflavio1805](https://github.com/hugoflavio1805)

## 15. License

The source code developed in this repository is licensed under the MIT License.

The dataset and original challenge description are used only for educational and portfolio purposes and belong to their respective authors.
