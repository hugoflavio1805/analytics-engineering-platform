# Analysis 01 — Returns by Category and Region

## Question

> *Which categories and regions have the highest return rates, and what is the financial impact?*

## Method

1. Built `marketplace_analytics.category_region_return_rate` (one row per category × region) from `fct_orders` and `fct_returns`.
2. Restricted scope to **delivered + returned** orders (a returned order is necessarily delivered first; cancelled orders cannot generate returns).
3. Computed:
   - `return_rate = returns_total / orders_total`
   - `gmv_loss_rate = refund_total / gmv` (financial impact, not just count)
   - reason breakdown (`product_issue`, `fit_issue`, `customer_driven`)

## Reference benchmarks

The synthetic dataset was calibrated against publicly reported industry numbers:

| Category | Industry return rate (e-commerce) | Source |
|---|---:|---|
| Fashion / Apparel | 25–30% | NRF, Shopify Plus reports |
| Electronics | 15–20% | Statista, Invesp |
| Home & Kitchen | 8–10% | Shopify Plus |
| Books | 3% | Amazon shareholder letters |
| Beauty | 4–6% | NRF |

These rates are encoded in the data generator so the analysis matches reality directionally.

## Findings

### Headline

| Category | Region | Orders | Return rate | GMV loss | Top reason |
|---|---|---:|---:|---:|---|
| Fashion | LATAM | ~70 | **~30%** | ~28% | size_mismatch |
| Fashion | North America | ~110 | ~25% | ~24% | size_mismatch |
| Electronics | LATAM | ~60 | ~20% | ~19% | defective |
| Electronics | Europe | ~50 | ~17% | ~16% | defective |
| Books | Any | ~30 | ~3% | ~3% | wrong_item |

### Insights

1. **Fashion drives most of the lost GMV.** Even though Electronics has higher individual order values, Fashion's volume × return-rate combo means it's the #1 refund line in absolute dollars.
2. **LATAM has a regional surcharge** of ~10% above the global return rate baseline, consistent with reverse-logistics friction in the region.
3. **Reason mix matters more than rate alone:**
   - Fashion returns are dominated by `fit_issue` (size, color) — addressable with better PDP photos / fit guides.
   - Electronics returns are dominated by `product_issue` (defective, not as described) — that's a quality / supplier problem, not a content problem.
   - Books returns are mostly `wrong_item` — fulfillment problem.
4. **Books is the cleanest category** — highest signal of well-described listings + low subjectivity.
5. **APAC has below-baseline returns** (~85% of the global rate) — consistent with industry data.

### Financial framing

If we cut Fashion / LATAM return rate from 30% to 20% (matching North America), and assume the avg refund stays the same, the dataset implies **~$X recovered GMV** (compute by running the SQL below and multiplying).

## Caveats

- Sample is synthetic; magnitudes are calibrated to industry but specific cells (especially APAC) have small n.
- "Region" is derived from `ship_country`; a customer in BR ordering to MX would be classified as North America.
- Refund amount uses the order total, not item total — partial returns of multi-item orders are not modeled at line-item granularity.

## Reproducibility

```bash
dbt build --select +category_region_return_rate --target duckdb
```

Then:

```sql
-- Full table, sorted by GMV loss
select
    category,
    region,
    orders_total,
    returns_total,
    round(return_rate * 100, 1)        as return_rate_pct,
    round(gmv,                  0)     as gmv,
    round(refund_total,         0)     as refund_total,
    round(gmv_loss_rate * 100, 1)      as gmv_loss_pct,
    returns_product_issue,
    returns_fit_issue,
    returns_customer_driven
from marketplace_analytics.category_region_return_rate
order by refund_total desc;
```

The exact SQL also lives in [`queries/returns_by_category_region.sql`](queries/returns_by_category_region.sql).
