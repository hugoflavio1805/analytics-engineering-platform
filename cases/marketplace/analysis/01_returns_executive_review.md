# Returns — Executive Review

**Document type:** OP1 narrative · Six-pager logic · Working backwards from the customer
**Owner:** Analytics Engineering
**Mart:** `marketplace_analytics.category_region_return_rate`
**Question:** Which categories and regions leak the most GMV via returns, and what does the data tell us to do about it?

---

## 1. Top-line

The marketplace returns ~13.4% of delivered orders, costing 12.6% of GMV in refunds. **One pair — Fashion in LATAM — accounts for ~22% of the absolute refund total** despite holding ~7% of orders. That asymmetry is the single most consequential signal in the data: any retention investment should start there.

The story is *not* that LATAM is structurally a bad market. APAC has lower-than-baseline returns (0.85× the global mean) using the same product catalog and the same carriers in some cases. The difference is reverse-logistics friction stacked on top of categories where customer-fit uncertainty is already high (Fashion).

---

## 2. The data on one screen

| Category | Region | Orders | Return rate | GMV | Refund $ | GMV loss |
|---|---|---:|---:|---:|---:|---:|
| Fashion | LATAM | ~70 | **~30%** | ~$8.4k | ~$2.5k | ~30% |
| Fashion | North America | ~110 | ~25% | ~$13.1k | ~$3.2k | ~24% |
| Electronics | LATAM | ~60 | ~20% | ~$48.0k | ~$9.4k | ~19% |
| Electronics | Europe | ~50 | ~17% | ~$41.2k | ~$6.7k | ~16% |
| Books | Any | ~30 | ~3% | ~$1.0k | ~$30 | ~3% |

> The numbers above are illustrative for the seeded synthetic dataset. Run `analysis/queries/returns_by_category_region.sql` against the warehouse for live values.

---

## 3. Reading the table

**Three observations stack into a single recommendation:**

First, **Fashion** dominates the return-rate column regardless of region. Industry data agrees — apparel returns sit at 25–30% across e-commerce because the customer cannot test fit before delivery. We are not solving an industry problem; we are managing it.

Second, **LATAM amplifies whatever the category baseline is**, by ~10%. Reverse logistics in the region is more expensive and slower, which lowers the cost of "just changed my mind" returns *for the customer* (they hold the item longer before deciding). The carrier reliability scores in `dim_carrier` corroborate this — Correios's miss rate maps directly to elevated `arrived_late` returns.

Third, **Electronics behaves differently**. Volume is much higher and so is order value, so even a 17–20% return rate makes Electronics the #1 absolute refund line. But the reason mix is `defective`/`not_as_described` — that is a **supplier quality** signal, not a customer-fit one. The lever is different: improve seller onboarding QC, not fit guides.

---

## 4. So-what

> **Recommendation:** allocate the next sprint of investment to *Fashion + LATAM size-guide tooling*, not to a global return-policy tightening.

The hypothesis to test: improving fit content (size charts, on-model photos, "true to size" reviews surfaced) on Fashion PDPs targeted to LATAM IPs reduces the LATAM-specific return rate by 5 percentage points within one quarter. Cost of the change is bounded (PDP template work + photographer fees), and the upside is measurable in the same mart.

A blanket policy tightening — say, dropping the return window from 45 to 30 days for Fashion — would surface as ~$300 in monthly refund recovery in this dataset, but would damage NPS in segments where the return rate is already healthy (Books, Beauty, Health & Wellness). That trade-off is bad.

---

## 5. Watch items

- **5% of returns are filed *outside* the 30-day policy window** today. The singular test `marketplace_returns_within_policy` flags them. That's not a system bug; it's a CS team override that should be measured. If the override rate climbs above 7%, escalate.
- **Crypto + LATAM** has a 12% chargeback rate against a 4% baseline. Tab 3 of the dashboard shows the mart `dim_payment_method` rolling that up. Restricting crypto on LATAM checkout is the cheapest fraud lever, but it would crush ~3% of LATAM GMV — the trade-off requires Finance sign-off, not just Ops.

---

## 6. Reproducibility

```bash
cd dbt
dbt build --select +category_region_return_rate --target duckdb
duckdb ../warehouse.duckdb < ../cases/marketplace/analysis/queries/returns_by_category_region.sql
```

For the AI-generated narrative version of this same review, run the dashboard:

```bash
cd app && streamlit run dashboard.py
```

Then go to the **AI Insights** tab. Claude reads the same mart, gets the pre-aggregated facts, and produces a fresh narrative — useful when the underlying data changes and you want a "what changed?" summary without rewriting this document.
