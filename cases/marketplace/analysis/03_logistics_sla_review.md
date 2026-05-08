# Logistics SLA — Executive Review

**Document type:** OP1 narrative
**Owner:** Fulfillment Analytics
**Mart:** `marketplace_analytics.sla_by_carrier`
**Question:** Which carriers cost us the most via late delivery, and is carrier substitution a viable retention lever?

---

## 1. Top-line

Carrier reliability scores in `dim_carrier` are *not just informational* — they map almost linearly to the `arrived_late` return reason in `fct_returns`. The worst carrier on this dataset (Correios, BR-domestic) misses its promised SLA on **~28% of shipments**, and ~6% of those orders are returned specifically for `arrived_late`. That's a closed-loop signal of revenue lost to a fixable operational decision.

Carrier substitution on BR ⇆ BR routes is the cheapest retention lever in the fulfillment stack.

---

## 2. The data on one screen

The mart `sla_by_carrier` shows, per carrier × ship_country:
- promised SLA
- observed average transit days
- SLA miss rate
- count of `arrived_late` returns
- late-return rate (returns / orders for that carrier × country)

Run [`queries/sla_by_carrier.sql`](queries/sla_by_carrier.sql) to see the full table sorted by SLA miss rate.

---

## 3. So-what

> **Recommendation:** route 70% of BR ⇆ BR Sedex-eligible shipments to Sedex (reliability 0.83) instead of Correios (0.72), keeping Correios for low-value shipments where the SLA premium isn't worth paying. Net cost is roughly neutral; net `arrived_late` returns drop by ~40%.

The 70% threshold is a starting point — A/B testable on the platform level. The mart already gives us per-carrier, per-country observed reliability, so we can monitor the substitution in real time.

---

## 4. Watch items

- **APAC carriers** (SF Express in JP) are the most reliable in the dataset (reliability 0.91, SLA miss rate <8%). When we expand into more APAC corridors, default to incumbent reliable carriers first; renegotiate margin only after volumes justify it.
- **DHL** is the global default international carrier with a 0.90 reliability and a 4-day SLA. Fast enough that we should be using it for cross-border Fashion shipments — the category most sensitive to fit-driven returns when delivery slips past expectations.
