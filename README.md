# Analytics Engineering Platform

> Production-grade dimensional analytics on a generic marketplace dataset.
> dbt · DuckDB · Snowflake · Streamlit · Plotly · Anthropic Claude · Docker.

[![CI](https://github.com/hugoflavio1805/analytics-engineering-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/hugoflavio1805/analytics-engineering-platform/actions)
[![dbt](https://img.shields.io/badge/dbt-1.9-orange)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-ready-29B5E8)](https://www.snowflake.com/)

The platform answers three operational questions about an e-commerce marketplace, calibrated against publicly reported industry benchmarks (Fashion ~25–30% return rate, Electronics ~15–20%, Books ~3%, etc.):

1. **Returns** — which categories and regions leak the most GMV via refunds?
2. **Promotions** — which campaigns produce positive net contribution after chargebacks?
3. **Logistics** — which carriers miss SLA and how does that correlate with late-delivery returns?

Discipline applied at every layer: **raw stays raw, dashboards stay dumb, every metric has one definition in dbt**.

---

## What's inside

| Layer | Tool | Purpose |
|---|---|---|
| Sources | 16 raw CSVs | 6 fact-grade + 10 dimension-grade tables in `cases/marketplace/data/raw/` |
| Transformation | **dbt-core 1.9** | Staging → Intermediate → Marts (Kimball star schema) |
| Warehouse (dev) | **DuckDB 1.5** | Same SQL runs locally without infra |
| Warehouse (prod) | **Snowflake** | Production target via env vars |
| Quality | dbt tests | 174 tests (schema + relationships + 5 singular + freshness) |
| Observability | `main_audit.dbt_runs` | Every dbt run captured via on-run-end macro |
| BI | **Streamlit + Plotly** | Dark-themed 7-tab dashboard at port 8501 |
| AI | **Anthropic Claude** (Sonnet 4.6) | Narrative insights with anti-hallucination guardrails |
| CI/CD | **GitHub Actions** | dbt build on every PR, docs deploy on merge |
| Packaging | **Docker Compose** | Two-service stack: dbt-build (one-shot) + dashboard |

---

## The dimensional model

| | Tables | Notes |
|---|---|---|
| **Dimensions (10)** | `dim_mkt_customer` · `dim_seller` · `dim_product` · `dim_category` · `dim_geography` · `dim_date` · `dim_carrier` · `dim_payment_method` · `dim_return_reason` · `dim_promotion` | Conformed; each carries derived rollups (e.g. `dim_carrier.sla_miss_rate`) |
| **Facts (6)** | `fct_orders` (incremental) · `fct_order_items` · `fct_payments` · `fct_returns` · `fct_reviews` · `fct_shipping_events` | Standard star schema; `fct_orders` is the central fact |
| **Analytical marts (7)** | `category_region_return_rate` · `gmv_by_promotion` · `sla_by_carrier` · `customer_cohort_retention` · `pareto_sellers` · `growth_metrics_monthly` · `customer_ltv_by_segment` | One mart per question — dashboards consume directly |
| **Audit (1 view)** | `pipeline_health` | Reads from `main_audit.dbt_runs` populated by on-run-end hook |

---

## Embedded business signals (calibrated to industry benchmarks)

The synthetic data is not random — it embeds patterns the pipeline detects:

- **Q4 spike + cyber-week pulse** — 30% of orders fall Nov 20-Dec 31, sub-pulse Nov 24-Dec 2
- **July dip** — post-Prime cooldown reduces July volume to ~50% of average
- **YoY ~15% growth** — newer years overrepresented
- **Aggressive promotions inflate chargebacks** — BOGO/≥30%-off campaigns observe 18.4% chargeback rate vs 3.9% baseline (+367% relative lift)
- **Crypto + LATAM = elevated fraud** — 12% chargeback rate vs 4% baseline
- **Carrier reliability drives late-delivery returns** — Correios (BR, 0.72 reliability) vs Sedex (0.83) substitution is the cheapest retention lever in fulfillment
- **Fashion + LATAM dominates absolute refund** — 30% return rate (27% Fashion baseline × 1.10× LATAM modifier)
- **APAC returns 15% below baseline** — industry pattern
- **Returned orders correlate with low ratings** — 1-3 star reviews concentrate in returned orders

---

## Quickstart (Docker)

```bash
# 1. (Optional) Set your Anthropic API key for the AI Insights tab
echo 'ANTHROPIC_API_KEY=sk-ant-api03-...' > .env

# 2. Build and run the full stack
docker compose -f docker-compose.dashboard.yml up -d --build

# 3. Open the dashboard
open http://localhost:8501
```

The compose file orchestrates two services:

1. **`dbt-build`** — one-shot container that runs `dbt build` against DuckDB, populates the named volume `aep_warehouse`, and exits with code 0
2. **`dashboard`** — Streamlit app that depends on `dbt-build` succeeding, reads the warehouse read-only, and serves the UI on port 8501

Without `ANTHROPIC_API_KEY`, the dashboard still works — only the AI Insights tab degrades to showing the JSON payload that *would have been* sent to Claude.

---

## Quickstart (bare metal)

```bash
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -r app/requirements_dashboard.txt

# Generate the synthetic dataset (idempotent — overwrites raw CSVs)
powershell -ExecutionPolicy Bypass -File cases/marketplace/scripts/generate_marketplace_data.ps1

# Build the marts
cd dbt && dbt deps && dbt build --profiles-dir . --target duckdb && cd ..

# Run the dashboard
cd app && streamlit run dashboard.py
```

---

## Dashboard — 7 tabs

| Tab | What it shows |
|---|---|
| **Overview** | KPI strip, sales-by-day, world choropleth, 30-day summary, monthly + ad spend, shipping-level donut, top countries, AOV by day, review distribution |
| **Growth** | GMV with MoM/YoY overlay, cohort retention heatmap, Pareto curve with A/B/C buckets, LTV by region × tenure |
| **Returns** | Heatmap category × region, top-8 by refund total, full mart explorer |
| **Promotions** | KPI strip (active campaigns, promo GMV, aggressive premium), ROI scatter, type breakdown (dual-axis), aggressive vs baseline comparison |
| **Logistics** | SLA bar by carrier, chargeback rate by payment method |
| **Pipeline Health** | Last run hero card, runs timeline, test outcome history, slowest models, last-run breakdown |
| **AI Insights** | 4 narrative sections generated by Claude, cached 60 min per data signature |

---

## Pipeline observability

Every `dbt build` invocation is captured automatically via the `persist_run_results` macro (configured as `on-run-end`). Each row in `main_audit.dbt_runs` records:

- `run_id`, `git_sha`, `invocation_at`, `started_at`, `completed_at`
- `node_id`, `node_name`, `resource_type` (model/test/seed/source)
- `status`, `message`, `duration_ms`, `rows_affected`

The view `marketplace_analytics.pipeline_health` rolls these up per invocation — the dashboard's Pipeline Health tab paints from it.

---

## Data quality discipline

- **174 tests** in the build, 0 warnings, 0 errors as of latest commit
- **5 singular tests** for business invariants (negative prices, payment-amount mismatch, return policy window, etc.)
- **Source freshness** thresholds — warn at 26h, error at 48h
- **`relationships`** tests ensure FK integrity across all marts
- **Self-contained warehouse** — staging materialized as `table` (not `view`), so the `.duckdb` file works without source CSVs at runtime

---

## Cost & scale

| Item | Estimate |
|---|---|
| Local DuckDB + Docker | **$0** |
| Snowflake XS warehouse, daily build (current volume) | **$15–25/month** |
| Snowflake XS, 10× scale | **~$80/month** |
| GitHub Actions (public repo) | **$0** (within 2,000 min free tier) |
| Anthropic Claude (Sonnet 4.6) | **~$0.01 per dashboard refresh** |
| End-to-end build (clone → dashboard up) | **~17 seconds** |

---

## Repository layout

```
.
├── app/                          # Streamlit dashboard
│   ├── dashboard.py              # 7-tab UI
│   ├── data_access.py            # cached DuckDB queries
│   ├── charts.py                 # Plotly factories (dark theme)
│   ├── ai_insights.py            # Anthropic Claude wrapper with cache
│   ├── Dockerfile
│   ├── requirements_dashboard.txt
│   └── README.md
├── cases/marketplace/
│   ├── data/raw/                 # 16 source CSVs
│   ├── data/raw_original/        # frozen copies
│   ├── scripts/
│   │   └── generate_marketplace_data.ps1
│   ├── analysis/                 # OP1-style executive reviews
│   └── README.md
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml              # duckdb / snowflake / ci targets
│   ├── models/marketplace/
│   │   ├── staging/              # 16 stg_* models (table-materialized)
│   │   ├── intermediate/         # int_orders_clean, int_returns_classified
│   │   └── marts/
│   │       ├── core/             # 10 dims + 6 facts
│   │       └── analytics/        # 7 analytical marts + pipeline_health view
│   ├── macros/
│   │   ├── audit/                # persist_run_results, ensure_audit_table
│   │   └── cross_warehouse/      # snowflake/duckdb/redshift portability
│   └── tests/                    # 5 singular tests
├── docker/
│   └── Dockerfile.dbt
├── docker-compose.dashboard.yml
├── .github/workflows/ci.yml
├── requirements.txt
└── README.md
```

---

## Author

Developed by **Hugo Flávio Santos**
Data Engineer | Analytics Engineering | Data Platform

[hugoflavio1805@hotmail.com](mailto:hugoflavio1805@hotmail.com) · [github.com/hugoflavio1805](https://github.com/hugoflavio1805)

## License

The source code is licensed under the MIT License.

The synthetic dataset and analytical narratives are used only for educational and portfolio purposes.
