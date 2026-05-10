"""
Marketplace Operations Dashboard — Amazon Seller Central / Grow.com style.

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
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Marketplace Operations",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Custom CSS — dark theme, dense grid, Amazon-Seller-Central look
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    /* Page background */
    .stApp { background-color: #1F2937; }

    /* Reduce default padding */
    .main .block-container { padding-top: 1rem; padding-bottom: 1rem; max-width: 1600px; }

    /* Panels */
    [data-testid="stVerticalBlock"] > div[style*="border"] {
        background-color: #2A2F3A !important;
        border-color: #3A4150 !important;
    }

    /* KPI cards */
    [data-testid="stMetric"] {
        background-color: #2A2F3A;
        padding: 14px 18px;
        border-radius: 8px;
        border: 1px solid #3A4150;
    }
    [data-testid="stMetricValue"] {
        color: #F9FAFB !important;
        font-size: 28px !important;
        font-weight: 600 !important;
    }
    [data-testid="stMetricLabel"] {
        color: #9CA3AF !important;
        font-size: 11px !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    [data-testid="stMetricDelta"] svg { vertical-align: middle; }

    /* Section headers */
    h1, h2, h3, h4 { color: #F9FAFB !important; }
    h1 { font-size: 22px !important; font-weight: 700 !important; }
    h2 { font-size: 16px !important; font-weight: 600 !important; margin-top: 1.5rem !important; }
    h3 { font-size: 14px !important; font-weight: 600 !important; }

    /* Body text */
    p, label, .stMarkdown { color: #D1D5DB !important; }
    code { background: #1F2937 !important; color: #F59E0B !important; }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        background-color: #2A2F3A;
        border-radius: 6px;
        padding: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        color: #9CA3AF;
        font-weight: 500;
    }
    .stTabs [aria-selected="true"] {
        background-color: #3B82F6 !important;
        color: white !important;
        border-radius: 4px;
    }

    /* Tables */
    .stDataFrame { background-color: #2A2F3A; }

    /* Sidebar */
    [data-testid="stSidebar"] { background-color: #1F2937; border-right: 1px solid #3A4150; }
    [data-testid="stSidebar"] * { color: #D1D5DB !important; }
    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2 { color: #F9FAFB !important; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Sidebar — minimal but professional
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("### 📦 Marketplace Ops")
    st.caption("Multi-case Analytics Engineering Platform")
    st.divider()

    st.markdown("**Active dashboard**")
    st.markdown("Operations · *Marketplace*")

    st.divider()
    st.markdown("**Data freshness**")
    try:
        kpis_preview = da.kpi_overview()
        st.code(f"orders: {int(kpis_preview['orders_total']):,}", language="text")
    except FileNotFoundError as e:
        st.error(str(e))
        st.stop()

    if st.button("Clear AI cache", use_container_width=True):
        import shutil
        shutil.rmtree(ai.CACHE_DIR, ignore_errors=True)
        ai.CACHE_DIR.mkdir(exist_ok=True)
        st.toast("AI cache cleared.")

    st.divider()
    st.caption(
        "dbt · DuckDB · Streamlit · Anthropic Claude\n\n"
        "github.com/hugoflavio1805/analytics-engineering-platform"
    )

# ---------------------------------------------------------------------------
# Top header with title + share-style chip
# ---------------------------------------------------------------------------

header_l, header_r = st.columns([4, 1])
with header_l:
    st.markdown("# Marketplace Operations")
    st.caption("Multi-region commerce review · GMV / Returns / Logistics / Promotions")
with header_r:
    st.markdown(
        "<div style='text-align: right; padding-top: 14px;'>"
        "<span style='background:#3B82F6;color:white;padding:4px 12px;border-radius:12px;"
        "font-size:11px;font-weight:600;'>LIVE</span></div>",
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Load data once
# ---------------------------------------------------------------------------

kpis     = da.kpi_overview()
kpi30    = da.kpi_30day_summary()
wkdelta  = da.kpi_with_delta_week()

# ---------------------------------------------------------------------------
# KPI strip — 6 scorecards Amazon-Seller-Central style
# ---------------------------------------------------------------------------

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Orders (all)", f"{int(kpis['orders_total']):,}")
c2.metric("GMV (all)", f"${kpis['gmv']:,.0f}")
c3.metric("AOV", f"${kpis['avg_order_value']:.2f}")
c4.metric(
    "GMV this week",
    f"${(wkdelta.get('gmv_this_week') or 0):,.0f}",
    delta=f"{(wkdelta.get('gmv_delta_pct') or 0)*100:+.1f}%",
)
c5.metric("Return rate", f"{kpis['return_rate']:.1%}")
c6.metric("Chargeback rate", f"{kpis['chargeback_rate']:.2%}")

st.markdown("")  # breathing room

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_overview, tab_returns, tab_promo, tab_logistics, tab_ai = st.tabs([
    "Overview", "Returns", "Promotions", "Logistics", "AI Insights",
])


# ===== Overview ============================================================

with tab_overview:
    # Row 1: Sales by Day | World choropleth
    r1c1, r1c2 = st.columns([3, 4])
    with r1c1:
        with st.container(border=True):
            st.markdown("**Sales by Day** · last 14 days")
            day_kpi_l, day_kpi_r = st.columns([2, 1])
            day_kpi_l.markdown(
                f"<div style='font-size:30px;color:#F9FAFB;font-weight:600;'>"
                f"${(wkdelta.get('gmv_this_week') or 0):,.0f}</div>"
                f"<div style='color:#9CA3AF;font-size:11px;'>This Week vs Last</div>",
                unsafe_allow_html=True,
            )
            delta = (wkdelta.get('gmv_delta_pct') or 0) * 100
            color = "#10B981" if delta >= 0 else "#EF4444"
            arrow = "▲" if delta >= 0 else "▼"
            day_kpi_r.markdown(
                f"<div style='text-align:right;padding-top:8px;'>"
                f"<span style='color:{color};font-size:14px;font-weight:600;'>"
                f"{arrow} {abs(delta):.1f}%</span></div>",
                unsafe_allow_html=True,
            )
            st.plotly_chart(charts.sales_by_day_bar(da.gmv_by_day(14)),
                            use_container_width=True, config={"displayModeBar": False})
    with r1c2:
        with st.container(border=True):
            st.markdown("**Sales by Country** · all time")
            st.plotly_chart(charts.choropleth_world(da.gmv_by_country()),
                            use_container_width=True, config={"displayModeBar": False})

    # Row 2: 30 Day Summary strip
    with st.container(border=True):
        st.markdown("**30 Day Summary**")
        s1, s2, s3, s4, s5 = st.columns(5)
        s1.markdown(
            f"<div style='color:#F9FAFB;font-size:24px;font-weight:600;'>"
            f"${(kpi30.get('sales') or 0)/1000:,.1f}K</div>"
            f"<div style='color:#9CA3AF;font-size:10px;text-transform:uppercase;'>Sales</div>",
            unsafe_allow_html=True,
        )
        s2.markdown(
            f"<div style='color:#F9FAFB;font-size:24px;font-weight:600;'>"
            f"{int(kpi30.get('units_sold') or 0):,}</div>"
            f"<div style='color:#9CA3AF;font-size:10px;text-transform:uppercase;'>Units Sold</div>",
            unsafe_allow_html=True,
        )
        s3.markdown(
            f"<div style='color:#F9FAFB;font-size:24px;font-weight:600;'>"
            f"{int(kpi30.get('returns_count') or 0):,}</div>"
            f"<div style='color:#9CA3AF;font-size:10px;text-transform:uppercase;'>Returns</div>",
            unsafe_allow_html=True,
        )
        s4.markdown(
            f"<div style='color:#F9FAFB;font-size:24px;font-weight:600;'>"
            f"${(kpi30.get('aov') or 0):,.0f}</div>"
            f"<div style='color:#9CA3AF;font-size:10px;text-transform:uppercase;'>AOV</div>",
            unsafe_allow_html=True,
        )
        s5.markdown(
            f"<div style='color:#F9FAFB;font-size:24px;font-weight:600;'>"
            f"{(kpi30.get('auo') or 0):.2f}</div>"
            f"<div style='color:#9CA3AF;font-size:10px;text-transform:uppercase;'>AUO</div>",
            unsafe_allow_html=True,
        )

    # Row 3: Monthly Sales + Ad Spend | Shipping Level | Top Countries
    r3c1, r3c2, r3c3 = st.columns([3, 2, 3])
    with r3c1:
        with st.container(border=True):
            st.plotly_chart(
                charts.stacked_bars_with_line(
                    da.monthly_sales_with_ad_spend(),
                    x_col="month",
                    stack_cols=["google_ads", "facebook_ads", "tiktok_ads"],
                    line_col="amazon_sales",
                    title="Monthly Sales and Ad Spend",
                ),
                use_container_width=True, config={"displayModeBar": False},
            )
    with r3c2:
        with st.container(border=True):
            st.plotly_chart(
                charts.donut_chart(da.shipping_level_breakdown(),
                                   label="level", value="orders",
                                   title="Shipping Level"),
                use_container_width=True, config={"displayModeBar": False},
            )
    with r3c3:
        with st.container(border=True):
            st.plotly_chart(
                charts.top_n_horizontal_bar(da.top_countries(10),
                                            label="country", value="gmv",
                                            title="GMV by Country (Top 10)"),
                use_container_width=True, config={"displayModeBar": False},
            )

    # Row 4: Avg Order Amount by Day | Reviews
    r4c1, r4c2 = st.columns([3, 2])
    with r4c1:
        with st.container(border=True):
            df_aov = da.avg_order_amount_by_weekday()
            st.plotly_chart(
                charts.grouped_bars_with_goal(
                    df_aov, x_col="day_label",
                    last_col="last_week", this_col="this_week",
                    goal=float(kpis['avg_order_value']),
                    title="Avg Order Amount by Day",
                ),
                use_container_width=True, config={"displayModeBar": False},
            )
    with r4c2:
        with st.container(border=True):
            st.plotly_chart(
                charts.review_distribution_bar(da.reviews_distribution()),
                use_container_width=True, config={"displayModeBar": False},
            )


# ===== Returns =============================================================

with tab_returns:
    st.markdown("### Where returns leak GMV")
    st.caption("Returns are GMV given back. Optimize for `gmv_loss_rate`, not raw count.")

    returns_df = da.category_region_returns()

    rc1, rc2 = st.columns([3, 2])
    with rc1:
        with st.container(border=True):
            st.plotly_chart(charts.return_rate_heatmap(returns_df),
                            use_container_width=True, config={"displayModeBar": False})
    with rc2:
        with st.container(border=True):
            st.markdown("**Top 8 (category × region) by refund $**")
            top_table = returns_df.nlargest(8, "refund_total")[
                ["category", "region", "orders_total", "returns_total",
                 "return_rate", "refund_total", "gmv_loss_rate"]
            ].copy()
            top_table["return_rate"]   = (top_table["return_rate"]   * 100).round(1).astype(str) + "%"
            top_table["gmv_loss_rate"] = (top_table["gmv_loss_rate"] * 100).round(1).astype(str) + "%"
            st.dataframe(top_table, use_container_width=True, hide_index=True, height=320)

    with st.expander("Full mart — category × region", expanded=False):
        st.dataframe(returns_df, use_container_width=True, hide_index=True)


# ===== Promotions ==========================================================

with tab_promo:
    st.markdown("### Promotion ROI — GMV vs chargeback")
    st.caption("Aggressive discounts (≥30%, BOGO) lift GMV but attract chargebacks. Optimize for net contribution.")

    promo_df = da.gmv_by_promotion()
    promotions = promo_df[promo_df["promotion_id"] != "NO_PROMOTION"]

    if not promotions.empty:
        with st.container(border=True):
            st.plotly_chart(charts.promotion_roi_scatter(promo_df),
                            use_container_width=True, config={"displayModeBar": False})

        with st.container(border=True):
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
    st.markdown("### Carrier SLA — promised vs delivered")
    st.caption("Each percentage point of SLA miss correlates with measurable `arrived_late` returns.")

    sla_df = da.sla_by_carrier()
    lc1, lc2 = st.columns([3, 2])
    with lc1:
        with st.container(border=True):
            st.plotly_chart(charts.sla_by_carrier_bar(sla_df),
                            use_container_width=True, config={"displayModeBar": False})
    with lc2:
        with st.container(border=True):
            st.plotly_chart(charts.chargeback_by_method_bar(da.chargeback_by_method()),
                            use_container_width=True, config={"displayModeBar": False})


# ===== AI Insights =========================================================

with tab_ai:
    st.markdown("### AI Insights — generated by Claude")
    st.caption(
        "Pre-aggregated facts from each mart are sent to Claude with strict "
        "anti-hallucination instructions. The model writes the **narrative**; "
        "every number is grounded in the dbt marts. Cached 60 min per data signature."
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
            st.markdown(f"#### {name}")
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

st.markdown("---")
st.caption(
    "Each chart reads from a single mart; no joins or aggregations happen in this app. "
    "If a metric needs to change, it changes in dbt — once — and propagates here."
)
