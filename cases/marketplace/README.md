# Case 2 — Generic Marketplace Analytics (Kimball Model)

Multi-domain case in this Analytics Engineering platform — a **generic marketplace** inspired by patterns from Amazon, MercadoLivre, Shopee, Etsy and eBay.

This case is the architectural showcase: a **full Kimball star schema** with 10 dimensions and 6 fact tables, calibrated against publicly reported industry benchmarks (Fashion ~25–30% returns, Books ~3%, etc.).

## Business questions answered

1. **Returns:** which categories and regions have the highest return rates, and what is the financial impact?
2. **Promotions:** which campaigns drove GMV vs which drove chargebacks?
3. **Logistics:** which carriers miss SLA most, and how does that correlate with late-delivery returns?

Each question maps to a dedicated semantic mart — dashboards never compute.

## Source CSVs (16 files)

### Fact-grade sources (6)

| File | Grain | Rows |
|---|---|---:|
| `orders.csv` | one row per order | 1,500 |
| `order_items.csv` | one row per (order, line_no) | ~3,758 |
| `payments.csv` | one row per payment | ~1,492 |
| `returns.csv` | one row per return | ~289 |
| `reviews.csv` | one row per review | ~1,016 |
| `shipping_events.csv` | one row per logistics event | ~5,688 |

### Dimension-grade sources (10)

| File | Grain | Rows |
|---|---|---:|
| `customers.csv` | one row per customer | 800 |
| `sellers.csv` | one row per seller | 200 |
| `products.csv` | one row per product | 1,000 |
| `categories.csv` | category catalog with parent hierarchy | 9 |
| `geography.csv` | country master | 15 |
| `dates.csv` | calendar with holiday flags | ~1,581 |
| `carriers.csv` | shipping carriers with SLA + reliability | 10 |
| `payment_methods.csv` | methods with fee + chargeback risk | 8 |
| `return_reasons.csv` | reason taxonomy | 9 |
| `promotions.csv` | campaigns with date range + aggressive flag | 50 |

## The Kimball model

```
                       dim_date                dim_geography
                          │                        │
                          │                        │
                          ▼                        ▼
                                 ╔══════════════╗
   dim_customer ─────────────▶  ║  fct_orders   ║  ◀───────── dim_carrier
                                 ║              ║
   dim_promotion ───────────▶   ║              ║  ◀───────── dim_payment_method
                                 ║              ║
   dim_category ────────────▶   ╚══════════════╝
                                       │
                  ┌────────────────────┼────────────────────┐
                  ▼                    ▼                    ▼
       fct_order_items         fct_payments           fct_returns ◀─── dim_return_reason
       (line grain)            (event grain)          (event grain)
                                                              │
                                                              ▼
                                                       dim_seller (also FK on items)

                                       fct_shipping_events  ◀──── dim_carrier
                                       (logistics events)

                                       fct_reviews          (1 per review)
```

## Built-in business signals (calibrated to real benchmarks)

The generator embeds these patterns so the dbt analysis surfaces them:

| Signal | Encoded as | Surfaces in |
|---|---|---|
| Black Friday / Cyber Monday spike | 25% of orders fall on Nov 20–Dec 5; baskets +1 item | `is_black_friday_order` flag, `gmv_by_promotion` |
| Aggressive promotions (≥30%, BOGO) elevate chargebacks | +6% chargeback boost | `dim_promotion.chargeback_rate`, `gmv_by_promotion` |
| Crypto + LATAM combo | 12% chargeback rate vs 4% baseline | `dim_payment_method.chargeback_rate` |
| Carrier reliability drives late delivery returns | Low-reliability carriers (Correios) trigger `arrived_late` returns | `sla_by_carrier`, `dim_carrier.sla_miss_rate` |
| Fashion + LATAM = highest absolute refund | Fashion 27% × LATAM 1.10× = 30% return rate | `category_region_return_rate` (top row) |
| Books are the cleanest category | 3% return rate, mostly `wrong_item` | Same mart |
| APAC has below-baseline returns | 0.85× regional modifier | Same mart |
| Returned orders correlate with low ratings | 1–3 stars dominate returned-order reviews | `fct_reviews.is_negative_review` |

## Built-in data quality issues

| # | Issue | Detection |
|---|---|---|
| 1 | Mixed `seller_id` separators (- vs _) | Normalized in `stg_sellers` / `stg_products` / `stg_order_items` |
| 2 | Duplicate `sku` (~4%) | Surfaced via `dim_product` (TODO: add `dq_duplicate_skus` test) |
| 3 | Negative `unit_price` (~1%) | Singular test `marketplace_no_negative_unit_price` (warn) |
| 4 | Negative `stock_qty` | Flag in `dim_product.has_negative_stock` |
| 5 | Orphan FK `SELLER-9999` | Caught by `relationships` test on `dim_product.seller_id` |
| 6 | Missing / malformed `email` | `stg_customers.has_valid_email` flag |
| 7 | Mixed `order_date` formats (yyyy-MM-dd ↔ dd/MM/yyyy) | Coerced in `stg_orders` via dual COALESCE |
| 8 | Lowercase country codes | UPPER in all `stg_*` |
| 9 | Payment amount ≠ order total (~1%) | `marketplace_payment_amount_matches_order_total` (warn) |
| 10 | Returns outside 30-day window (~5%) | `marketplace_returns_within_policy` (warn) |

## How to regenerate

```powershell
powershell -ExecutionPolicy Bypass -File scripts\generate_marketplace_data.ps1
```

(Idempotent — overwrites both `data/raw/` and `data/raw_original/`.)

## How to build the pipeline

```bash
cd dbt
dbt build --select marketplace.* --target duckdb
```

## Layer-by-layer summary

| Layer | Models | Purpose |
|---|---|---|
| **staging** | 16 `stg_*` (one per source) | Type-cast, normalize ids, flag DQ issues. Materialized as views. |
| **intermediate** | `int_orders_clean`, `int_returns_classified` | Multi-format date parsing, return policy classification. Ephemeral. |
| **marts.core** | 10 dims + 6 facts | Conformed Kimball star schema. |
| **marts.analytics** | `category_region_return_rate`, `gmv_by_promotion`, `sla_by_carrier` | One mart per business question. Consumed directly by BI. |

## Singular tests (warn-level)

| Test | What it catches |
|---|---|
| `marketplace_no_negative_unit_price` | Source-emitted negative prices (~1% of products) |
| `marketplace_payment_amount_matches_order_total` | Payment amount ≠ order total (~1%) |
| `marketplace_returns_within_policy` | Returns filed > 30 days after delivery (~5%) |

These are `severity: warn` because the goal is **documenting** the issue, not blocking the build for known source problems. Schema tests (`unique`, `not_null`, `relationships`, `accepted_values`) remain `error`.

## Findings (preview)

See [`analysis/01_returns_by_category_and_region.md`](analysis/01_returns_by_category_and_region.md) for the full write-up of the obligatory question.

Headline: **Fashion in LATAM** is the worst offender — both highest return rate (~30%) and largest absolute refund total. Recommended action is improvement of fit guides + size charts on Fashion PDPs in LATAM, not blanket return-policy tightening.

Two follow-up marts add color:
- `gmv_by_promotion` shows that aggressive Black Friday campaigns drive GMV but at a measurable chargeback premium — net contribution should be the metric, not gross GMV.
- `sla_by_carrier` shows Correios (BR) as the SLA-miss leader, with elevated `arrived_late` return rate — substituting Sedex on BR ⇆ BR routes would reduce returns at no margin cost.
