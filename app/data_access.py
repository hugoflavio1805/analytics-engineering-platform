"""
Data access layer.

All SQL the dashboard runs lives here — never inline in Streamlit code.
Functions are cached with `st.cache_data` so re-renders don't rehit DuckDB.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(Path(__file__).parent.parent / "warehouse.duckdb"))


@lru_cache(maxsize=1)
def _connection() -> duckdb.DuckDBPyConnection:
    if not Path(DUCKDB_PATH).exists():
        raise FileNotFoundError(
            f"DuckDB warehouse not found at {DUCKDB_PATH}. "
            "Run `cd dbt && dbt build --target duckdb` first."
        )
    return duckdb.connect(DUCKDB_PATH, read_only=True)


def _q(sql: str, **params) -> pd.DataFrame:
    return _connection().execute(sql, params).df()


# =============================================================================
# KPI strip + headline numbers
# =============================================================================

@st.cache_data(ttl=300, show_spinner=False)
def kpi_overview() -> dict:
    return _q(
        """
        with base as (
            select
                count(*)                                              as orders_total,
                sum(total)                                            as gmv,
                sum(items_count)                                      as units_sold,
                sum(case when order_status = 'returned' then 1 else 0 end) as returns_total,
                sum(case when payment_status = 'chargeback' then 1 else 0 end) as chargebacks_total,
                avg(total)                                            as avg_order_value,
                avg(items_count)                                      as avg_units_per_order
            from main_marketplace_core.fct_orders
        )
        select *,
            1.0 * returns_total     / nullif(orders_total, 0) as return_rate,
            1.0 * chargebacks_total / nullif(orders_total, 0) as chargeback_rate
        from base
        """
    ).iloc[0].to_dict()


@st.cache_data(ttl=300, show_spinner=False)
def kpi_30day_summary() -> dict:
    """Summary of the last 30 days of orders (Amazon Seller Central style)."""
    return _q(
        """
        with mx as (select max(order_date) as max_d from main_marketplace_core.fct_orders),
        scope as (
            select * from main_marketplace_core.fct_orders, mx
            where order_date > max_d - interval 30 day
        )
        select
            sum(total)                              as sales,
            sum(items_count)                        as units_sold,
            sum(case when order_status = 'returned' then 1 else 0 end) as returns_count,
            avg(total)                              as aov,
            avg(items_count)                        as auo
        from scope
        """
    ).iloc[0].to_dict()


@st.cache_data(ttl=300, show_spinner=False)
def kpi_with_delta_week() -> dict:
    """This-week GMV vs last-week, and 30-day deltas."""
    return _q(
        """
        with mx as (select max(order_date) as max_d from main_marketplace_core.fct_orders),
        labels as (
            select max_d,
                   (max_d - interval 7  day)::date as wk_start_this,
                   (max_d - interval 14 day)::date as wk_start_last
            from mx
        ),
        agg as (
            select
                sum(case when order_date >  l.wk_start_this then total end) as gmv_this_week,
                sum(case when order_date >  l.wk_start_last
                     and order_date <= l.wk_start_this then total end) as gmv_last_week
            from main_marketplace_core.fct_orders, labels l
        )
        select
            gmv_this_week, gmv_last_week,
            case when gmv_last_week > 0
                 then (gmv_this_week - gmv_last_week) / gmv_last_week
                 else 0 end as gmv_delta_pct
        from agg
        """
    ).iloc[0].to_dict()


# =============================================================================
# Time series
# =============================================================================

@st.cache_data(ttl=300, show_spinner=False)
def gmv_by_day(days: int = 14) -> pd.DataFrame:
    return _q(
        f"""
        with mx as (select max(order_date) as max_d from main_marketplace_core.fct_orders)
        select
            order_date as order_day,
            sum(total) as gmv,
            count(*)   as orders
        from main_marketplace_core.fct_orders, mx
        where order_date > max_d - interval {int(days)} day
        group by order_date
        order by order_date
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def gmv_by_month() -> pd.DataFrame:
    return _q(
        """
        select
            date_trunc('month', order_date) as month,
            sum(total)                       as gmv,
            count(*)                         as orders,
            sum(case when is_black_friday_order or is_cyber_monday_order
                     then 1 else 0 end)      as promo_peak_orders
        from main_marketplace_core.fct_orders
        group by 1
        order by 1
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def avg_order_amount_by_weekday() -> pd.DataFrame:
    """Avg order amount last 14 days, broken into 'last week' and 'this week'."""
    return _q(
        """
        with mx as (select max(order_date) as max_d from main_marketplace_core.fct_orders),
        scope as (
            select order_date, total,
                   case when order_date > (m.max_d - interval 7 day) then 'this' else 'last' end as week
            from main_marketplace_core.fct_orders, mx m
            where order_date > m.max_d - interval 14 day
        ),
        agg as (
            select
                strftime(order_date, '%a %b %d') as day_label,
                order_date,
                week,
                avg(total)                       as avg_amount
            from scope
            group by order_date, week
        )
        select
            day_label,
            order_date,
            sum(case when week = 'last' then avg_amount end) as last_week,
            sum(case when week = 'this' then avg_amount end) as this_week
        from agg
        group by day_label, order_date
        order by order_date
        """
    )


# =============================================================================
# Geography breakdowns
# =============================================================================

# ISO-3 mapping for choropleth
ISO3 = {
    "US": "USA", "CA": "CAN", "MX": "MEX",
    "BR": "BRA", "AR": "ARG", "CL": "CHL", "CO": "COL",
    "GB": "GBR", "DE": "DEU", "FR": "FRA", "ES": "ESP",
    "IT": "ITA", "PT": "PRT", "JP": "JPN", "AU": "AUS",
}


@st.cache_data(ttl=300, show_spinner=False)
def gmv_by_country() -> pd.DataFrame:
    df = _q(
        """
        select ship_country, sum(total) as gmv, count(*) as orders
        from main_marketplace_core.fct_orders
        group by ship_country
        """
    )
    df["country_iso3"] = df["ship_country"].map(ISO3)
    return df.dropna(subset=["country_iso3"])


@st.cache_data(ttl=300, show_spinner=False)
def gmv_by_region() -> pd.DataFrame:
    return _q(
        """
        select g.region, sum(o.total) as gmv, count(*) as orders
        from main_marketplace_core.fct_orders o
        left join main_marketplace_core.dim_geography g
               on o.ship_country = g.country_code
        group by g.region
        order by gmv desc
        """
    )


# =============================================================================
# Top-N tables / bars
# =============================================================================

@st.cache_data(ttl=300, show_spinner=False)
def top_countries(limit: int = 10) -> pd.DataFrame:
    return _q(
        f"""
        select g.country_name as country, sum(o.total) as gmv, count(*) as orders
        from main_marketplace_core.fct_orders o
        left join main_marketplace_core.dim_geography g
               on o.ship_country = g.country_code
        where g.country_name is not null
        group by g.country_name
        order by gmv desc
        limit {int(limit)}
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def top_sellers(limit: int = 10) -> pd.DataFrame:
    return _q(
        f"""
        select seller_id, seller_name, country_code, region, gmv, orders_count, products_count
        from main_marketplace_core.dim_seller
        order by gmv desc
        limit {int(limit)}
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def top_products(limit: int = 10) -> pd.DataFrame:
    return _q(
        f"""
        select product_id, product_name, category, units_sold, revenue
        from main_marketplace_core.dim_product
        order by revenue desc
        limit {int(limit)}
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def top_categories(limit: int = 10) -> pd.DataFrame:
    return _q(
        f"""
        select category_name as category, units_sold, gross_sales
        from main_marketplace_core.dim_category
        order by gross_sales desc
        limit {int(limit)}
        """
    )


# =============================================================================
# Shipping / Logistics breakdowns
# =============================================================================

@st.cache_data(ttl=300, show_spinner=False)
def shipping_level_breakdown() -> pd.DataFrame:
    """Buckets transit_days into shipping tiers (Express / SecondDay / Expedited / Standard / Slow)."""
    return _q(
        """
        with cls as (
            select
                case
                    when transit_days <= 2 then 'Express'
                    when transit_days <= 4 then 'Expedited'
                    when transit_days <= 7 then 'Standard'
                    else 'Slow'
                end as level,
                total
            from main_marketplace_core.fct_orders
            where transit_days is not null
        )
        select level, count(*) as orders, sum(total) as gmv
        from cls
        group by level
        order by case level
            when 'Express'   then 1
            when 'Expedited' then 2
            when 'Standard'  then 3
            when 'Slow'      then 4 end
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def sla_by_carrier() -> pd.DataFrame:
    return _q("select * from main_marketplace_analytics.sla_by_carrier")


# =============================================================================
# Returns + Promotions (existing marts)
# =============================================================================

@st.cache_data(ttl=300, show_spinner=False)
def category_region_returns() -> pd.DataFrame:
    return _q("select * from main_marketplace_analytics.category_region_return_rate")


@st.cache_data(ttl=300, show_spinner=False)
def gmv_by_promotion() -> pd.DataFrame:
    return _q("select * from main_marketplace_analytics.gmv_by_promotion")


@st.cache_data(ttl=300, show_spinner=False)
def chargeback_by_method() -> pd.DataFrame:
    return _q(
        """
        select method_code, method_name, transactions, total_processed,
               chargeback_rate, chargeback_risk
        from main_marketplace_core.dim_payment_method
        order by chargeback_rate desc
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def reviews_distribution() -> pd.DataFrame:
    return _q(
        """
        select rating, count(*) as reviews_count,
               sum(case when order_status = 'returned' then 1 else 0 end) as returned_count
        from main_marketplace_core.fct_reviews
        group by rating
        order by rating
        """
    )


# =============================================================================
# Anomalies (for AI Insights)
# =============================================================================

@st.cache_data(ttl=300, show_spinner=False)
def anomalies_summary(top_n: int = 5) -> pd.DataFrame:
    return _q(
        f"""
        with cr as (
            select category, region, return_rate, gmv_loss_rate, refund_total
            from main_marketplace_analytics.category_region_return_rate
        ),
        baseline as (select avg(return_rate) as global_return_rate from cr)
        select
            category, region, return_rate, gmv_loss_rate, refund_total,
            return_rate - (select global_return_rate from baseline) as delta_vs_baseline
        from cr
        order by abs(return_rate - (select global_return_rate from baseline)) desc
        limit {int(top_n)}
        """
    )


# =============================================================================
# Mock ad-spend (synthetic, for the Monthly Sales + Ad Spend dual-axis chart)
# =============================================================================

@st.cache_data(ttl=300, show_spinner=False)
def cohort_retention() -> pd.DataFrame:
    return _q("select * from main_marketplace_analytics.customer_cohort_retention")


@st.cache_data(ttl=300, show_spinner=False)
def pareto_sellers() -> pd.DataFrame:
    return _q("select * from main_marketplace_analytics.pareto_sellers")


@st.cache_data(ttl=300, show_spinner=False)
def growth_metrics() -> pd.DataFrame:
    return _q("select * from main_marketplace_analytics.growth_metrics_monthly")


@st.cache_data(ttl=300, show_spinner=False)
def ltv_by_segment() -> pd.DataFrame:
    return _q("select * from main_marketplace_analytics.customer_ltv_by_segment")


# =============================================================================
# Pipeline audits / health
# =============================================================================

@st.cache_data(ttl=60, show_spinner=False)
def pipeline_runs(limit: int = 25) -> pd.DataFrame:
    return _q(
        f"""
        select * from main_marketplace_analytics.pipeline_health
        order by invocation_at desc
        limit {int(limit)}
        """
    )


@st.cache_data(ttl=60, show_spinner=False)
def latest_run() -> dict:
    rows = _q(
        """
        select * from main_marketplace_analytics.pipeline_health
        order by invocation_at desc
        limit 1
        """
    )
    return rows.iloc[0].to_dict() if len(rows) else {}


@st.cache_data(ttl=60, show_spinner=False)
def latest_run_node_breakdown() -> pd.DataFrame:
    return _q(
        """
        with last_run as (
            select max(invocation_at) as max_at from main_audit.dbt_runs
        )
        select
            r.node_name, r.resource_type, r.status,
            r.duration_ms, r.rows_affected, r.message
        from main_audit.dbt_runs r, last_run
        where r.invocation_at = last_run.max_at
        order by r.duration_ms desc
        """
    )


@st.cache_data(ttl=60, show_spinner=False)
def slowest_models(limit: int = 10) -> pd.DataFrame:
    return _q(
        f"""
        select node_name, resource_type, avg(duration_ms) as avg_duration_ms, count(*) as runs
        from main_audit.dbt_runs
        where resource_type in ('model', 'test')
        group by node_name, resource_type
        order by avg_duration_ms desc
        limit {int(limit)}
        """
    )


@st.cache_data(ttl=60, show_spinner=False)
def test_outcome_history() -> pd.DataFrame:
    return _q(
        """
        select
            invocation_at,
            sum(case when status = 'pass'    then 1 else 0 end) as tests_pass,
            sum(case when status = 'warn'    then 1 else 0 end) as tests_warn,
            sum(case when status in ('fail','error') then 1 else 0 end) as tests_fail
        from main_audit.dbt_runs
        where resource_type = 'test'
        group by invocation_at
        order by invocation_at
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def monthly_sales_with_ad_spend() -> pd.DataFrame:
    """Joins monthly GMV with synthetic ad spend signals.

    Ad spend is **not** in the source data — we derive it as a deterministic
    function of GMV per channel (Google = 12%, Facebook = 8%, TikTok = 5%
    of monthly GMV). The signal is consistent and lets the dual-axis chart
    show plausible patterns without inventing facts that the AI tab might
    misread.
    """
    return _q(
        """
        select
            date_trunc('month', order_date)         as month,
            sum(total)                              as amazon_sales,
            sum(total) * 0.12                       as google_ads,
            sum(total) * 0.08                       as facebook_ads,
            sum(total) * 0.05                       as tiktok_ads
        from main_marketplace_core.fct_orders
        group by 1
        order by 1
        """
    )
