# Marketplace Analytics Dashboard

Streamlit-based operating-review dashboard sitting on top of the dbt
marketplace marts. Connects to the local DuckDB warehouse, paints
interactive Plotly charts, and uses the **Anthropic Claude API** to
generate narrative insights on demand.

## Why this architecture

The dashboard is built around two non-negotiable principles:

1. **Single source of metric truth.** Every number on screen comes
   directly from a `marketplace_*` mart. There is **no** SQL join,
   filter or aggregation in this app — only `SELECT … FROM mart`. If a
   metric definition needs to change, it changes in dbt and propagates
   here automatically.
2. **AI is a writer, not an analyst.** Claude never sees raw rows. It
   receives **pre-aggregated facts** (top-5 offenders, anomalies,
   month-over-month deltas) and writes the narrative around those
   facts. The system prompt forbids inventing numbers. This is the only
   safe way to put generative AI into a financial dashboard.

## Layout

```
app/
├── dashboard.py              ← Streamlit entry point (5 tabs)
├── data_access.py            ← cached DuckDB queries (one fn per mart)
├── charts.py                 ← Plotly factories (KPI strip, heatmap,
│                                scatter, stacked bar, time series)
├── ai_insights.py            ← Claude wrapper with on-disk cache
├── requirements_dashboard.txt
├── Dockerfile                ← container build for deploy
├── .env.example              ← copy to .env, fill ANTHROPIC_API_KEY
└── .ai_cache/                ← auto-created, git-ignored
```

## Tab structure (OP1-review style)

| Tab | Owner question | Primary metric | Secondary chart |
|---|---|---|---|
| **Overview** | "Is GMV growth real or promo-driven?" | Monthly GMV | Promo-peak orders overlay |
| **Returns** | "Where do returns leak GMV?" | `gmv_loss_rate` | Heatmap category × region |
| **Promotions** | "Which campaigns net out positive?" | GMV vs chargeback rate | Aggressive flag color |
| **Logistics** | "Which carriers cost us the most via late delivery?" | `sla_miss_rate` + `late_return_rate` | Side-by-side bar |
| **AI Insights** | "What patterns is the data showing this week?" | 4 narrative sections | Cached 60 min per signature |

Each tab follows the same reading order: **narrative → chart → table →
expander with full data**. This mirrors how Amazon teams structure WBR
and OP1 documents — the reader can stop after the narrative if they
trust the analyst, or drill all the way down if they don't.

## Run locally

```bash
# 1. Build the marts (from repo root)
cd dbt
dbt deps && dbt build --target duckdb
cd ..

# 2. Set up the dashboard
cd app
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements_dashboard.txt
cp .env.example .env
# (optional) edit .env and paste your ANTHROPIC_API_KEY

# 3. Launch
streamlit run dashboard.py
```

The dashboard opens at <http://localhost:8501>.

## Run via Docker

```bash
# Build
docker build -t marketplace-dashboard ./app

# Run, mounting the warehouse and forwarding the API key
docker run --rm -p 8501:8501 \
  -v "$(pwd)/warehouse.duckdb:/warehouse.duckdb:ro" \
  -e ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" \
  marketplace-dashboard
```

## Cost & performance

- DuckDB queries: <100 ms cold, <10 ms warm (Streamlit cache, 5-min TTL).
- Claude calls: ~600 input + ~350 output tokens per section × 4
  sections = **~$0.01 per refresh** on Sonnet 4.6.
- AI cache (60 min, signature-keyed): a typical browsing session costs
  cents, not dollars.

## Failure modes & graceful degradation

| Missing thing | What happens |
|---|---|
| `warehouse.duckdb` not built | Dashboard shows a clear error pointing the user to `dbt build`. |
| `ANTHROPIC_API_KEY` not set | All four AI sections render the pre-aggregated payload as JSON, with a "_AI commentary disabled_" notice. The dashboard still works. |
| API call fails (rate limit, timeout) | Section shows the error; other tabs untouched. |
| `anthropic` lib not installed | Same — the section explains how to install. |

This is the discipline of a production dashboard: nothing is allowed to
take the whole app down because of an optional dependency.

## Anti-hallucination guarantees

The prompt sent to Claude has a hard rule: *"only cite numbers that
appear verbatim in the input"*. The input is a JSON payload built in
[`ai_insights.py`](ai_insights.py) using `payload_for_*` functions —
these select **only** the rows the model is allowed to see. If the
model writes a number that's not in the payload, the rule was broken.
A future hardening step would be a regex-based fact-check pass that
fails the response if any number isn't in the payload.

## Why Streamlit and not Tableau / Looker / Dash?

- **Streamlit** = Python end-to-end → same language as dbt extensions
  and the AI module, no JS/JSON config dialect.
- **Tableau** = great for BI consumers, but harder to wire to an LLM,
  and licensing matters at company scale.
- **Looker** = excellent semantic layer, redundant here because dbt
  already plays that role.
- **Dash** = more flexible, but verbose; not needed for this scope.

If this dashboard ever ships to non-engineers, the right next step is
to publish the same marts to Tableau **as a parallel surface** (not a
replacement). The discipline of "one mart, many surfaces" is what dbt
exists to enforce.
