"""
Plotly chart factory.

Style is deliberately conservative: low chartjunk, monospace numbers, no animations.
Mirrors the visual language of OP1/OP2 Amazon decks: density over decoration.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


# A neutral, professional palette (Tableau 10 — Amazon decks use a similar one).
PALETTE = ["#4E79A7", "#F28E2B", "#E15759", "#76B7B2", "#59A14F",
           "#EDC948", "#B07AA1", "#FF9DA7", "#9C755F", "#BAB0AC"]


def _base_layout(title: str | None = None, height: int = 360) -> dict:
    return dict(
        title=dict(text=title, x=0, xanchor="left", font=dict(size=15)) if title else None,
        margin=dict(l=10, r=10, t=40 if title else 10, b=10),
        height=height,
        font=dict(family="Inter, system-ui, -apple-system, sans-serif", size=12),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=False, zeroline=False),
        yaxis=dict(gridcolor="#EEE", zeroline=False, tickformat=","),
        legend=dict(orientation="h", y=-0.2),
        colorway=PALETTE,
    )


# ---------------------------------------------------------------------------
# Time series — GMV by month
# ---------------------------------------------------------------------------

def gmv_time_series(df: pd.DataFrame) -> go.Figure:
    """Bar chart of GMV by month, with promo-peak orders overlaid as line."""
    fig = go.Figure()
    fig.add_bar(
        x=df["month"], y=df["gmv"], name="GMV",
        marker=dict(color=PALETTE[0]),
        hovertemplate="%{x|%b %Y}<br>GMV $%{y:,.0f}<extra></extra>",
    )
    fig.add_scatter(
        x=df["month"], y=df["promo_peak_orders"], name="Promo-peak orders",
        mode="lines+markers", yaxis="y2",
        line=dict(color=PALETTE[1], width=2),
    )
    layout = _base_layout("GMV by month (promo-peak orders overlaid)")
    layout["yaxis"]["title"] = "GMV ($)"
    layout["yaxis2"] = dict(overlaying="y", side="right", showgrid=False, title="Promo-peak orders")
    fig.update_layout(**layout)
    return fig


# ---------------------------------------------------------------------------
# Heatmap — return rate by category × region
# ---------------------------------------------------------------------------

def return_rate_heatmap(df: pd.DataFrame) -> go.Figure:
    pivot = df.pivot_table(index="category", columns="region", values="return_rate", aggfunc="mean")
    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=pivot.columns,
            y=pivot.index,
            colorscale="RdYlGn_r",
            zmin=0, zmax=0.4,
            hovertemplate="%{y} × %{x}<br>return rate %{z:.1%}<extra></extra>",
            colorbar=dict(title="Return rate", tickformat=".0%"),
        )
    )
    layout = _base_layout("Return rate by category × region", height=420)
    fig.update_layout(**layout)
    return fig


# ---------------------------------------------------------------------------
# Promotion ROI scatter — GMV vs chargeback rate
# ---------------------------------------------------------------------------

def promotion_roi_scatter(df: pd.DataFrame) -> go.Figure:
    df = df[df["promotion_id"] != "NO_PROMOTION"].copy()
    fig = px.scatter(
        df,
        x="gmv", y="chargeback_rate",
        size="orders_count", color="is_aggressive",
        color_discrete_map={True: PALETTE[2], False: PALETTE[0]},
        hover_data=["promotion_name", "promotion_type", "discount_value"],
        labels={"chargeback_rate": "Chargeback rate", "gmv": "GMV ($)"},
    )
    layout = _base_layout("Promotion ROI: GMV vs chargeback rate")
    layout["yaxis"]["tickformat"] = ".1%"
    fig.update_layout(**layout)
    return fig


# ---------------------------------------------------------------------------
# SLA by carrier — bar with reference line
# ---------------------------------------------------------------------------

def sla_by_carrier_bar(df: pd.DataFrame) -> go.Figure:
    g = (
        df.groupby("carrier_name", as_index=False)
          .agg(sla_miss_rate=("sla_miss_rate", "mean"),
               late_return_rate=("late_return_rate", "mean"),
               orders_total=("orders_total", "sum"))
          .sort_values("sla_miss_rate", ascending=True)
    )
    fig = go.Figure()
    fig.add_bar(
        x=g["sla_miss_rate"], y=g["carrier_name"], orientation="h",
        name="SLA miss rate", marker=dict(color=PALETTE[2]),
        hovertemplate="%{y}<br>SLA miss %{x:.1%}<extra></extra>",
    )
    fig.add_scatter(
        x=g["late_return_rate"], y=g["carrier_name"], mode="markers",
        name="Late-delivery returns", marker=dict(color=PALETTE[1], size=10, symbol="diamond"),
        hovertemplate="%{y}<br>Late-return rate %{x:.1%}<extra></extra>",
    )
    layout = _base_layout("Carrier SLA performance", height=420)
    layout["xaxis"]["tickformat"] = ".0%"
    fig.update_layout(**layout)
    return fig


# ---------------------------------------------------------------------------
# Reviews — stacked bar
# ---------------------------------------------------------------------------

def review_distribution_bar(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_bar(
        x=df["rating"], y=df["reviews_count"] - df["returned_count"],
        name="Kept (delivered)", marker=dict(color=PALETTE[4]),
    )
    fig.add_bar(
        x=df["rating"], y=df["returned_count"],
        name="Returned", marker=dict(color=PALETTE[2]),
    )
    layout = _base_layout("Review rating × outcome")
    fig.update_layout(barmode="stack", **layout)
    return fig


# ---------------------------------------------------------------------------
# Chargeback by method — bar
# ---------------------------------------------------------------------------

def chargeback_by_method_bar(df: pd.DataFrame) -> go.Figure:
    fig = px.bar(
        df.sort_values("chargeback_rate", ascending=True),
        x="chargeback_rate", y="method_name",
        color="chargeback_risk",
        color_discrete_map={"very_low": PALETTE[4], "low": PALETTE[3], "medium": PALETTE[1], "high": PALETTE[2]},
        labels={"chargeback_rate": "Chargeback rate", "method_name": ""},
    )
    layout = _base_layout("Chargeback rate by payment method")
    layout["xaxis"]["tickformat"] = ".1%"
    fig.update_layout(**layout)
    return fig
