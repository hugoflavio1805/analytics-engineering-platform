"""
Data access layer.

All SQL the dashboard runs lives here — never inline in Streamlit code.
Functions are cached with `st.cache_data` so re-renders don't rehit DuckDB.

Connects to the DuckDB warehouse produced by `dbt build`. The dashboard never
joins or aggregates — it only paints what the marts already computed.
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
    """Single read-only connection per process. dbt writes; the dashboard only reads."""
    if not Path(DUCKDB_PATH).exists():
        raise FileNotFoundError(
            f"DuckDB warehouse not found at {DUCKDB_PATH}. "
            "Run `cd dbt && dbt build --target duckdb` first."
        )
    return duckdb.connect(DUCKDB_PATH, read_only=True)


def _q(sql: str, **params) -> pd.DataFrame:
    return _connection().execute(sql, params).df()


# ---------------------------------------------------------------------------
# Marketplace marts (Case 2)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300, show_spinner=False)
def kpi_overview() -> dict:
    """Top-line KPIs for the Overview tab. One round-trip, single dict."""
    df = _q(
        """
        with base as (
            select
                count(*)                                         as orders_total,
                sum(total)                                       as gmv,
                sum(case when order_status = 'returned' then 1 else 0 end) as returns_total,
                sum(case when payment_status = 'chargeback' then 1 else 0 end) as chargebacks_total,
                avg(total)                                       as avg_order_value
            from main_marketplace_core.fct_orders
        )
        select
            orders_total,
            gmv,
            returns_total,
            chargebacks_total,
            avg_order_value,
            1.0 * returns_total      / nullif(orders_total, 0) as return_rate,
            1.0 * chargebacks_total  / nullif(orders_total, 0) as chargeback_rate
        from base
        """
    )
    return df.iloc[0].to_dict()


@st.cache_data(ttl=300, show_spinner=False)
def gmv_by_month() -> pd.DataFrame:
    return _q(
        """
        select
            date_trunc('month', order_date) as month,
            sum(total)                       as gmv,
            count(*)                         as orders,
            sum(case when is_black_friday_order or is_cyber_monday_order then 1 else 0 end)
                                             as promo_peak_orders
        from main_marketplace_core.fct_orders
        group by 1
        order by 1
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def category_region_returns() -> pd.DataFrame:
    return _q("select * from main_marketplace_analytics.category_region_return_rate")


@st.cache_data(ttl=300, show_spinner=False)
def gmv_by_promotion() -> pd.DataFrame:
    return _q("select * from main_marketplace_analytics.gmv_by_promotion")


@st.cache_data(ttl=300, show_spinner=False)
def sla_by_carrier() -> pd.DataFrame:
    return _q("select * from main_marketplace_analytics.sla_by_carrier")


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


@st.cache_data(ttl=300, show_spinner=False)
def anomalies_summary(top_n: int = 5) -> pd.DataFrame:
    """Top anomalies — used to seed the AI Insights prompt with the most
    "interesting" rows (highest deviation from baseline). Mirrors how an
    Amazon WBR/MBR document would highlight outliers before commentary."""
    return _q(
        f"""
        with cr as (
            select category, region, return_rate, gmv_loss_rate, refund_total
            from main_marketplace_analytics.category_region_return_rate
        ),
        baseline as (
            select avg(return_rate) as global_return_rate
            from cr
        )
        select
            category, region, return_rate, gmv_loss_rate, refund_total,
            return_rate - (select global_return_rate from baseline) as delta_vs_baseline
        from cr
        order by abs(return_rate - (select global_return_rate from baseline)) desc
        limit {int(top_n)}
        """
    )
