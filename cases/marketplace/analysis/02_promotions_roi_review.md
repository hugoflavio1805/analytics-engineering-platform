# Promotions ROI — Executive Review

**Document type:** OP1 narrative
**Owner:** Marketing Analytics
**Mart:** `marketplace_analytics.gmv_by_promotion`
**Question:** Which campaigns net out positive after chargebacks and returns?

---

## 1. Top-line

Promotional campaigns lift GMV by single-digit percentage points but carry a measurable chargeback premium — the riskiest configuration is *aggressive (≥30% off or BOGO) campaigns running on Black Friday / Cyber Monday paid via crypto*. Three campaigns drive 60% of total promo GMV; one of them is the single biggest chargeback contributor in the dataset.

We optimize for **net contribution**, not gross GMV. Stop reporting promo GMV as an isolated KPI in business reviews — it's misleading without the chargeback denominator.

---

## 2. What the mart shows

The mart `gmv_by_promotion` carries one row per campaign plus a `'NO_PROMOTION'` baseline bucket. Compare the columns side by side and the picture clears up immediately:

- **Aggressive campaigns** average ~6% chargeback rate vs ~3% baseline.
- **Free shipping** campaigns drive baskets larger than the AOV but with the **lowest** chargeback rate of any type.
- **BOGO** campaigns have the highest return rate of any promo type (customers buy 2, intend to keep 1) — that's not a fraud problem, but it inflates the operational cost of the campaign in ways the topline GMV column hides.

---

## 3. So-what

> **Recommendation:** require Finance approval for any campaign with `is_aggressive = true` running during Black Friday / Cyber Monday windows. Default to free-shipping promotions for Q4 unless a specific category warrants an aggressive discount.

Implementation lever: a campaign-approval workflow keyed on the `dim_promotion.is_aggressive` flag. The flag is already populated and tested.

---

## 4. Watch items

- The **`NO_PROMOTION` baseline** has been growing month-over-month — promo dependence is *decreasing* over the dataset window. That's the healthiest signal in this review and should be celebrated. Don't lose it by overusing aggressive campaigns to hit a quarterly GMV target.
- **Crypto + aggressive promotion** is a multiplicative chargeback risk. The synthetic data encodes this and the `dim_payment_method` × `dim_promotion` cross-tab confirms it. A simple checkout rule (crypto disabled on aggressive promos) would surface measurable savings.
