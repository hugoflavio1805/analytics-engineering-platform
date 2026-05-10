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

tab_overview, tab_growth, tab_returns, tab_promo, tab_logistics, tab_pipeline, tab_ai = st.tabs([
    "Overview", "Growth", "Returns", "Promotions", "Logistics", "Pipeline Health", "AI Insights",
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
    st.markdown("### Promotion ROI — net contribution after chargebacks")
    st.caption("Aggressive discounts lift GMV but attract chargebacks. We optimize for net contribution, not gross GMV.")

    promo_df       = da.gmv_by_promotion()
    promotions     = promo_df[promo_df["promotion_id"] != "NO_PROMOTION"]
    promo_type_df  = da.gmv_by_promotion_type()
    aggressive_df  = da.aggressive_vs_baseline()

    if promotions.empty:
        st.info("No promotion records in the warehouse.")
    else:
        # KPI strip — promotional health at a glance
        promo_orders   = int(promotions["orders_count"].sum())
        promo_gmv      = float(promotions["gmv"].sum())
        baseline       = aggressive_df[aggressive_df["bucket"].str.startswith("baseline")]
        aggressive     = aggressive_df[aggressive_df["bucket"].str.startswith("aggressive")]
        baseline_cb    = float(baseline["weighted_chargeback_rate"].iloc[0]) if not baseline.empty else 0.0
        aggressive_cb  = float(aggressive["weighted_chargeback_rate"].iloc[0]) if not aggressive.empty else 0.0
        cb_premium     = aggressive_cb - baseline_cb
        share_orders   = promo_orders / max(int(da.kpi_overview()['orders_total']), 1)

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Active campaigns", f"{len(promotions):,}")
        k2.metric("Promo GMV", f"${promo_gmv:,.0f}")
        k3.metric("Promo share of orders", f"{share_orders:.1%}")
        k4.metric("Baseline chargeback", f"{baseline_cb*100:.2f}%")
        k5.metric(
            "Aggressive premium", f"+{cb_premium*100:.2f} pp",
            delta=f"{(cb_premium/max(baseline_cb,1e-9))*100:+.0f}% rel",
            delta_color="inverse",
        )

        # Row 1: ROI scatter
        with st.container(border=True):
            st.plotly_chart(charts.promotion_roi_scatter(promo_df),
                            use_container_width=True, config={"displayModeBar": False})

        # Row 2: Type breakdown + aggressive comparison (two new charts)
        pc1, pc2 = st.columns(2)
        with pc1:
            with st.container(border=True):
                st.plotly_chart(charts.promotion_type_breakdown(promo_type_df),
                                use_container_width=True, config={"displayModeBar": False})
                st.caption(
                    "Dual axis: GMV per promotion type (blue bars) versus the average "
                    "chargeback rate of campaigns of that type (red diamonds). "
                    "Free-shipping consistently has the lowest chargeback rate."
                )
        with pc2:
            with st.container(border=True):
                st.plotly_chart(charts.aggressive_comparison(aggressive_df),
                                use_container_width=True, config={"displayModeBar": False})
                st.caption(
                    "Side-by-side risk profile: orders without any promotion vs regular "
                    "campaigns vs aggressive (≥30% off / BOGO). Aggressive campaigns "
                    "consistently elevate both chargeback and return rates."
                )

        # Row 3: Top 5 + full type breakdown table
        with st.container(border=True):
            st.markdown("**Top 5 campaigns by GMV**")
            top5 = promotions.nlargest(5, "gmv")[[
                "promotion_name", "promotion_type", "discount_value",
                "is_aggressive", "orders_count", "gmv", "chargeback_rate", "return_rate",
            ]].copy()
            top5["chargeback_rate"] = (top5["chargeback_rate"] * 100).round(2).astype(str) + "%"
            top5["return_rate"]     = (top5["return_rate"]     * 100).round(1).astype(str) + "%"
            top5["gmv"]             = top5["gmv"].apply(lambda v: f"${v:,.0f}")
            st.dataframe(top5, use_container_width=True, hide_index=True)

        with st.expander("All promotion types — aggregated", expanded=False):
            df_show = promo_type_df.copy()
            df_show["gmv"]                 = df_show["gmv"].apply(lambda v: f"${v:,.0f}")
            df_show["avg_chargeback_rate"] = (df_show["avg_chargeback_rate"]*100).round(3).astype(str) + "%"
            df_show["avg_return_rate"]     = (df_show["avg_return_rate"]*100).round(2).astype(str) + "%"
            st.dataframe(df_show, use_container_width=True, hide_index=True)


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


# ===== Growth Analytics ====================================================

with tab_growth:
    st.markdown("### Growth Analytics")
    st.caption("Cohorts · Pareto concentration · MoM/YoY growth · Customer LTV.")

    # Row 1: growth lines (full width)
    growth_df = da.growth_metrics()
    with st.container(border=True):
        st.plotly_chart(charts.growth_lines(growth_df),
                        use_container_width=True, config={"displayModeBar": False})

    # Row 2: cohort + pareto
    g2c1, g2c2 = st.columns([3, 4])
    with g2c1:
        with st.container(border=True):
            st.plotly_chart(charts.cohort_heatmap(da.cohort_retention()),
                            use_container_width=True, config={"displayModeBar": False})
    with g2c2:
        with st.container(border=True):
            pareto_df = da.pareto_sellers()
            st.plotly_chart(charts.pareto_curve(pareto_df),
                            use_container_width=True, config={"displayModeBar": False})
            # Pareto callout
            top_a = pareto_df[pareto_df["pareto_bucket"] == "A"]
            if len(top_a):
                share_a = top_a["gmv"].sum() / pareto_df["gmv"].sum()
                st.caption(
                    f"Top **{len(top_a)} sellers** (bucket A) drive "
                    f"**{share_a:.0%}** of total GMV. Bucket B: "
                    f"**{len(pareto_df[pareto_df['pareto_bucket']=='B'])}** sellers."
                )

    # Row 3: LTV breakdown + LTV table
    g3c1, g3c2 = st.columns([3, 2])
    ltv_df = da.ltv_by_segment()
    with g3c1:
        with st.container(border=True):
            st.plotly_chart(charts.ltv_breakdown_bars(ltv_df),
                            use_container_width=True, config={"displayModeBar": False})
    with g3c2:
        with st.container(border=True):
            st.markdown("**LTV by segment**")
            ltv_table = ltv_df[["region", "tenure_bucket", "customers",
                                "avg_net_ltv", "median_net_ltv"]].copy()
            ltv_table["avg_net_ltv"]    = ltv_table["avg_net_ltv"].apply(lambda v: f"${v:,.0f}")
            ltv_table["median_net_ltv"] = ltv_table["median_net_ltv"].apply(lambda v: f"${v:,.0f}")
            st.dataframe(ltv_table, use_container_width=True, hide_index=True, height=320)


# ===== Pipeline Health =====================================================

with tab_pipeline:
    st.markdown("### Pipeline Health")
    st.caption("Every dbt run is captured in `main_audit.dbt_runs` via on-run-end hook. "
               "This tab is the operational view of the platform itself.")

    # Latest run hero card
    last = da.latest_run()
    if not last:
        st.info("No runs recorded yet. Run `dbt build` to populate the audit log.")
    else:
        status_color = {"green": "#10B981", "warning": "#F59E0B", "failed": "#EF4444"}.get(
            last.get("overall_status", "green"), "#9CA3AF"
        )
        with st.container(border=True):
            ph1, ph2, ph3, ph4, ph5 = st.columns([2, 1, 1, 1, 2])
            ph1.markdown(
                f"<div style='font-size:11px;color:#9CA3AF;text-transform:uppercase;'>Last run</div>"
                f"<div style='font-size:22px;color:#F9FAFB;font-weight:600;font-family:Consolas,monospace;'>"
                f"{last['run_short']}</div>"
                f"<div style='color:{status_color};font-size:13px;font-weight:600;text-transform:uppercase;'>"
                f"● {last.get('overall_status', '?')}</div>",
                unsafe_allow_html=True,
            )
            ph2.metric("PASS",  int(last.get("pass_count", 0)))
            ph3.metric("WARN",  int(last.get("warn_count", 0)))
            ph4.metric("ERROR", int(last.get("error_count", 0)))
            ph5.metric("Duration", f"{last.get('total_duration_seconds', 0):.1f}s")
            st.caption(
                f"git_sha: `{last.get('git_sha', 'unknown')}` · "
                f"started_at: `{last.get('started_at')}` · "
                f"completed_at: `{last.get('completed_at')}`"
            )

    # Row: timeline + test history
    p2c1, p2c2 = st.columns([2, 3])
    with p2c1:
        with st.container(border=True):
            runs = da.pipeline_runs(25)
            if not runs.empty:
                st.plotly_chart(charts.pipeline_runs_timeline(runs),
                                use_container_width=True, config={"displayModeBar": False})
    with p2c2:
        with st.container(border=True):
            tests = da.test_outcome_history()
            if not tests.empty:
                st.plotly_chart(charts.test_outcome_area(tests),
                                use_container_width=True, config={"displayModeBar": False})
            else:
                st.info("No tests recorded yet.")

    # Row: slowest models + last run breakdown
    p3c1, p3c2 = st.columns([2, 3])
    with p3c1:
        with st.container(border=True):
            st.markdown("**Slowest models (avg)**")
            slow = da.slowest_models(10)
            slow["avg_duration_ms"] = slow["avg_duration_ms"].apply(lambda v: f"{v:,.0f}")
            st.dataframe(slow, use_container_width=True, hide_index=True, height=320)
    with p3c2:
        with st.container(border=True):
            st.markdown("**Last run — node-by-node**")
            br = da.latest_run_node_breakdown()
            br_show = br[["node_name", "resource_type", "status", "duration_ms", "rows_affected"]].copy()
            br_show["duration_ms"] = br_show["duration_ms"].apply(lambda v: f"{v:,.0f}")
            st.dataframe(br_show, use_container_width=True, hide_index=True, height=320)


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
