"""
Marketplace Analytics Dashboard.

Runs against the DuckDB warehouse produced by `dbt build`. Layout follows the
OP1/OP2 review style at Amazon: KPI strip on top, narrative + chart + table
in each section, AI Insights as a final tab.

Run:
    cd app
    pip install -r requirements_dashboard.txt
    cp .env.example .env   # then add your ANTHROPIC_API_KEY (optional)
    streamlit run dashboard.py
"""
from __future__ import annotations

import streamlit as st

import charts
import data_access as da
import ai_insights as ai


# ---------------------------------------------------------------------------
# Page config (must be the first Streamlit call)
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Marketplace Analytics — OP1 Review",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("Marketplace OP1")
    st.caption(
        "Operating Plan review — multi-tab narrative built directly on top of "
        "the dbt marts. Dashboards never compute; they paint what the marts proved."
    )
    st.divider()
    st.markdown("**Data freshness**")
    st.code(f"warehouse: {da.DUCKDB_PATH}", language="text")
    if st.button("Clear AI cache", help="Force fresh AI insights on next render"):
        import shutil
        shutil.rmtree(ai.CACHE_DIR, ignore_errors=True)
        ai.CACHE_DIR.mkdir(exist_ok=True)
        st.toast("AI cache cleared.")

    st.divider()
    st.caption(
        "Built with dbt · DuckDB · Streamlit · Anthropic Claude.\n\n"
        "Source: github.com/hugoflavio1805/analytics-engineering-platform"
    )

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("Marketplace Analytics")
st.caption(
    "OP1-style operating review — Returns, Promotions, Logistics, AI Insights. "
    "Reading order: KPI strip → tab narrative → chart → table → recommendation."
)


# ---------------------------------------------------------------------------
# Try-load KPIs (graceful failure if warehouse missing)
# ---------------------------------------------------------------------------

try:
    kpis = da.kpi_overview()
except FileNotFoundError as e:
    st.error(str(e))
    st.stop()


# ---------------------------------------------------------------------------
# KPI strip (top of page, always visible)
# ---------------------------------------------------------------------------

c1, c2, c3, c4, c5 = st.columns(5)

c1.metric("Orders", f"{int(kpis['orders_total']):,}")
c2.metric("GMV", f"${kpis['gmv']:,.0f}")
c3.metric("AOV", f"${kpis['avg_order_value']:.2f}")
c4.metric("Return rate", f"{kpis['return_rate']:.1%}")
c5.metric("Chargeback rate", f"{kpis['chargeback_rate']:.2%}")

st.divider()

# ---------------------------------------------------------------------------
# Tabs (the "5 deep dives")
# ---------------------------------------------------------------------------

tab_overview, tab_returns, tab_promo, tab_logistics, tab_ai = st.tabs([
    "Overview", "Returns", "Promotions", "Logistics", "AI Insights",
])


# ===== Overview ============================================================

with tab_overview:
    st.subheader("How the business performed")
    st.markdown(
        "**Working backwards:** the VP wants to know whether GMV growth is real or "
        "concentrated in promotional spikes. The chart below splits the trend from the noise."
    )

    gmv_df = da.gmv_by_month()
    st.plotly_chart(charts.gmv_time_series(gmv_df), use_container_width=True)

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown("**Top 10 sellers by GMV**")
        st.dataframe(da.top_sellers(10), use_container_width=True, hide_index=True)
    with col_r:
        st.markdown("**Reviews — rating × outcome**")
        st.plotly_chart(charts.review_distribution_bar(da.reviews_distribution()), use_container_width=True)


# ===== Returns =============================================================

with tab_returns:
    st.subheader("Where returns leak GMV")
    st.markdown(
        "**Why this matters:** returns are not just a logistics line — every "
        "refund is GMV given back, often *with* the original shipping cost. "
        "We optimize for `gmv_loss_rate`, not raw return count."
    )

    returns_df = da.category_region_returns()

    col_l, col_r = st.columns([3, 2])
    with col_l:
        st.plotly_chart(charts.return_rate_heatmap(returns_df), use_container_width=True)
    with col_r:
        st.markdown("**Top 8 (category × region) by refund $**")
        top_table = returns_df.nlargest(8, "refund_total")[
            ["category", "region", "orders_total", "returns_total",
             "return_rate", "refund_total", "gmv_loss_rate"]
        ].copy()
        top_table["return_rate"]   = (top_table["return_rate"]   * 100).round(1).astype(str) + "%"
        top_table["gmv_loss_rate"] = (top_table["gmv_loss_rate"] * 100).round(1).astype(str) + "%"
        st.dataframe(top_table, use_container_width=True, hide_index=True)

    with st.expander("Full mart — category × region", expanded=False):
        st.dataframe(returns_df, use_container_width=True, hide_index=True)


# ===== Promotions ==========================================================

with tab_promo:
    st.subheader("Promotion ROI — GMV vs chargeback")
    st.markdown(
        "**Why this matters:** aggressive discounts (≥30%, BOGO) lift GMV but "
        "attract higher chargeback rates. Net contribution is what should drive "
        "campaign approval — not gross GMV alone."
    )

    promo_df = da.gmv_by_promotion()
    promotions = promo_df[promo_df["promotion_id"] != "NO_PROMOTION"]

    if not promotions.empty:
        st.plotly_chart(charts.promotion_roi_scatter(promo_df), use_container_width=True)

        st.markdown("**Top 5 campaigns by GMV**")
        top5 = promotions.nlargest(5, "gmv")[[
            "promotion_name", "promotion_type", "discount_value",
            "is_aggressive", "orders_count", "gmv", "chargeback_rate", "return_rate",
        ]].copy()
        top5["chargeback_rate"] = (top5["chargeback_rate"] * 100).round(2).astype(str) + "%"
        top5["return_rate"]     = (top5["return_rate"]     * 100).round(1).astype(str) + "%"
        st.dataframe(top5, use_container_width=True, hide_index=True)
    else:
        st.info("No promotion records in the warehouse.")


# ===== Logistics ===========================================================

with tab_logistics:
    st.subheader("Carrier SLA — promised vs delivered")
    st.markdown(
        "**Why this matters:** every percentage point of SLA miss correlates "
        "with measurable `arrived_late` returns. Carrier substitution is the "
        "cheapest retention lever in fulfillment."
    )

    sla_df = da.sla_by_carrier()
    st.plotly_chart(charts.sla_by_carrier_bar(sla_df), use_container_width=True)

    st.markdown("**Chargeback rate by payment method**")
    st.plotly_chart(charts.chargeback_by_method_bar(da.chargeback_by_method()), use_container_width=True)


# ===== AI Insights =========================================================

with tab_ai:
    st.subheader("AI Insights — generated by Claude")
    st.markdown(
        "Pre-aggregated facts from each mart are sent to Claude with strict "
        "anti-hallucination instructions. The model writes the **narrative**; "
        "every number is grounded in the data the dbt pipeline already proved."
        "  \n*Insights are cached for 60 minutes per data signature to keep "
        "token usage predictable.*"
    )

    returns_df = da.category_region_returns()
    promo_df   = da.gmv_by_promotion()
    sla_df     = da.sla_by_carrier()
    anomalies  = da.anomalies_summary(top_n=5)
    gmv_df     = da.gmv_by_month()

    sections = [
        ("Overview",   ai.payload_for_overview(kpis, gmv_df)),
        ("Returns",    ai.payload_for_returns(returns_df, anomalies)),
        ("Promotions", ai.payload_for_promotions(promo_df)),
        ("Logistics",  ai.payload_for_sla(sla_df)),
    ]

    for name, payload in sections:
        with st.container(border=True):
            st.markdown(f"### {name}")
            with st.spinner(f"Analyzing {name.lower()}…"):
                insight = ai.generate_insight(name, payload)
            st.markdown(insight.body_markdown)

            meta_bits = []
            if insight.used_cache: meta_bits.append("cached")
            if insight.elapsed_ms: meta_bits.append(f"{insight.elapsed_ms} ms")
            if insight.error:      meta_bits.append(f"⚠ {insight.error}")
            if meta_bits:
                st.caption(" · ".join(meta_bits))

    with st.expander("Anomalies fed to the model", expanded=False):
        st.dataframe(anomalies, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.caption(
    "Each chart reads from a single mart; no joins or aggregations happen in this app. "
    "If a metric needs to change, it changes in dbt — once — and propagates here."
)
