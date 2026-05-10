"""
Plotly chart factory — Amazon Seller Central / Grow.com inspired dark theme.

Design language:
  - Dark canvas (#1F2937), slightly lighter panel cards (#2A2F3A)
  - Primary accent: Amazon-blue #3B82F6
  - Secondary accent: Amazon-orange #F59E0B
  - Positive delta green #10B981, negative delta red #EF4444
  - Compact, high-density layout. Very little chartjunk.
  - Numbers in monospace where possible; KPI scorecards lead each panel.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


# Brand palette (calibrated against Amazon Seller Central + Grow dashboards)
BG_PAGE       = "#1F2937"
BG_PANEL      = "#2A2F3A"
BORDER        = "#3A4150"
TEXT_PRIMARY  = "#F9FAFB"
TEXT_MUTED    = "#9CA3AF"
ACCENT_BLUE   = "#3B82F6"
ACCENT_ORANGE = "#F59E0B"
ACCENT_GREEN  = "#10B981"
ACCENT_RED    = "#EF4444"
ACCENT_PURPLE = "#8B5CF6"
GRID_LINE     = "#3A4150"

# Sequential palettes for stacks / categorical
PALETTE = [ACCENT_BLUE, ACCENT_ORANGE, "#60A5FA", "#FBBF24", ACCENT_PURPLE,
           "#34D399", "#F472B6", "#A78BFA", "#FB923C", "#22D3EE"]


def _layout(title: str | None = None, height: int = 280, show_legend: bool = True) -> dict:
    return dict(
        title=dict(
            text=title, x=0, xanchor="left",
            font=dict(size=13, color=TEXT_PRIMARY, family="Inter, system-ui, sans-serif"),
        ) if title else None,
        margin=dict(l=10, r=10, t=36 if title else 8, b=8),
        height=height,
        font=dict(family="Inter, system-ui, -apple-system, sans-serif", size=11, color=TEXT_PRIMARY),
        plot_bgcolor=BG_PANEL,
        paper_bgcolor=BG_PANEL,
        xaxis=dict(showgrid=False, zeroline=False, color=TEXT_MUTED, tickfont=dict(size=10)),
        yaxis=dict(gridcolor=GRID_LINE, zeroline=False, color=TEXT_MUTED, tickfont=dict(size=10), tickformat=","),
        legend=dict(orientation="h", y=1.10, x=1, xanchor="right",
                    bgcolor="rgba(0,0,0,0)", font=dict(size=10, color=TEXT_MUTED)),
        showlegend=show_legend,
        colorway=PALETTE,
        hoverlabel=dict(bgcolor=BG_PANEL, bordercolor=BORDER, font=dict(color=TEXT_PRIMARY)),
    )


# =============================================================================
# Sales-by-day (KPI + bar chart) — first panel of Amazon Seller Central layout
# =============================================================================

def sales_by_day_bar(df: pd.DataFrame, value_col: str = "gmv", date_col: str = "order_day") -> go.Figure:
    """Vertical bars per day. Column labels above bars, monospace."""
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])

    fig = go.Figure()
    fig.add_bar(
        x=df[date_col], y=df[value_col],
        marker=dict(color=ACCENT_BLUE),
        text=df[value_col].apply(lambda v: f"${v:,.0f}"),
        textposition="outside",
        textfont=dict(size=9, color=TEXT_MUTED),
        hovertemplate="%{x|%b %d}<br>$%{y:,.0f}<extra></extra>",
        cliponaxis=False,
    )
    layout = _layout(height=240, show_legend=False)
    layout["xaxis"]["tickformat"] = "%b %d"
    layout["yaxis"]["showticklabels"] = False
    fig.update_layout(**layout)
    return fig


# =============================================================================
# Choropleth map — Sales by region (any country with ISO-3 or US state)
# =============================================================================

def choropleth_world(df: pd.DataFrame, locations: str = "country_iso3", value: str = "gmv") -> go.Figure:
    """World choropleth using country ISO-3 codes."""
    fig = go.Figure(data=go.Choropleth(
        locations=df[locations],
        z=df[value],
        locationmode="ISO-3",
        colorscale=[[0, "#3A4150"], [0.5, ACCENT_ORANGE], [1, "#FB923C"]],
        marker_line_color=BG_PANEL,
        marker_line_width=0.5,
        colorbar=dict(
            title=dict(text="GMV ($)", font=dict(color=TEXT_MUTED, size=10)),
            tickfont=dict(color=TEXT_MUTED, size=9),
            thickness=10, len=0.7, x=1.0,
        ),
        hovertemplate="%{location}<br>$%{z:,.0f}<extra></extra>",
    ))
    layout = _layout(height=280, show_legend=False)
    layout["geo"] = dict(
        bgcolor=BG_PANEL,
        showframe=False,
        showcoastlines=False,
        showland=True,
        landcolor="#3A4150",
        projection=dict(type="natural earth"),
    )
    layout.pop("xaxis")
    layout.pop("yaxis")
    fig.update_layout(**layout)
    return fig


# =============================================================================
# Donut — Shipping level breakdown
# =============================================================================

def donut_chart(df: pd.DataFrame, label: str, value: str, title: str | None = None) -> go.Figure:
    fig = go.Figure(data=[go.Pie(
        labels=df[label],
        values=df[value],
        hole=0.55,
        marker=dict(colors=PALETTE[:len(df)], line=dict(color=BG_PANEL, width=2)),
        textinfo="label+percent",
        textfont=dict(size=11, color=TEXT_PRIMARY),
        insidetextorientation="radial",
        hovertemplate="%{label}<br>%{value:,.0f}<br>%{percent}<extra></extra>",
    )])
    layout = _layout(title=title, height=300, show_legend=False)
    layout.pop("xaxis")
    layout.pop("yaxis")
    fig.update_layout(**layout)
    return fig


# =============================================================================
# Top-N horizontal bars — Sales by State / Top sellers / etc.
# =============================================================================

def top_n_horizontal_bar(df: pd.DataFrame, label: str, value: str,
                          title: str | None = None, value_format: str = "$,.0f") -> go.Figure:
    df = df.sort_values(value, ascending=True)
    fig = go.Figure()
    fig.add_bar(
        x=df[value], y=df[label], orientation="h",
        marker=dict(color=ACCENT_BLUE),
        text=df[value].apply(lambda v: f"${v:,.0f}"),
        textposition="outside",
        textfont=dict(size=10, color=TEXT_MUTED),
        hovertemplate="%{y}<br>$%{x:,.0f}<extra></extra>",
        cliponaxis=False,
    )
    layout = _layout(title=title, height=320, show_legend=False)
    layout["yaxis"]["tickfont"] = dict(size=10, color=TEXT_PRIMARY)
    layout["xaxis"]["tickformat"] = "$,.0f"
    fig.update_layout(**layout)
    return fig


# =============================================================================
# Dual-axis: stacked bars (ad spend by channel) + line (sales)
# =============================================================================

def stacked_bars_with_line(df: pd.DataFrame,
                            x_col: str,
                            stack_cols: list[str],
                            line_col: str,
                            title: str | None = None) -> go.Figure:
    fig = go.Figure()
    for i, col in enumerate(stack_cols):
        fig.add_bar(
            x=df[x_col], y=df[col], name=col,
            marker=dict(color=PALETTE[i]),
            hovertemplate=f"{col}: $%{{y:,.0f}}<extra></extra>",
        )
    fig.add_scatter(
        x=df[x_col], y=df[line_col], name=line_col,
        mode="lines+markers", yaxis="y2",
        line=dict(color=ACCENT_ORANGE, width=2),
        marker=dict(size=6),
        hovertemplate=f"{line_col}: $%{{y:,.0f}}<extra></extra>",
    )
    layout = _layout(title=title, height=320)
    layout["barmode"] = "stack"
    layout["yaxis"]["title"] = dict(text="Ad Spend ($)", font=dict(size=10, color=TEXT_MUTED))
    layout["yaxis2"] = dict(
        overlaying="y", side="right", showgrid=False,
        title=dict(text=line_col, font=dict(size=10, color=TEXT_MUTED)),
        tickfont=dict(size=10, color=TEXT_MUTED), tickformat="$,.0f",
    )
    fig.update_layout(**layout)
    return fig


# =============================================================================
# Heatmap — return rate category × region (kept from previous version, restyled)
# =============================================================================

def return_rate_heatmap(df: pd.DataFrame) -> go.Figure:
    pivot = df.pivot_table(index="category", columns="region", values="return_rate", aggfunc="mean")
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values, x=pivot.columns, y=pivot.index,
        colorscale=[[0, "#10B981"], [0.5, ACCENT_ORANGE], [1, ACCENT_RED]],
        zmin=0, zmax=0.4,
        hovertemplate="%{y} × %{x}<br>%{z:.1%}<extra></extra>",
        colorbar=dict(
            title=dict(text="Return rate", font=dict(color=TEXT_MUTED, size=10)),
            tickfont=dict(color=TEXT_MUTED, size=9),
            tickformat=".0%", thickness=10,
        ),
    ))
    layout = _layout(title="Return rate by category × region", height=380, show_legend=False)
    fig.update_layout(**layout)
    return fig


# =============================================================================
# Promotion ROI scatter — kept from previous version, restyled
# =============================================================================

def promotion_roi_scatter(df: pd.DataFrame) -> go.Figure:
    df = df[df["promotion_id"] != "NO_PROMOTION"].copy()
    fig = px.scatter(
        df, x="gmv", y="chargeback_rate",
        size="orders_count", color="is_aggressive",
        color_discrete_map={True: ACCENT_RED, False: ACCENT_BLUE},
        hover_data=["promotion_name", "promotion_type", "discount_value"],
    )
    layout = _layout(title="Promotion ROI: GMV vs chargeback rate", height=320)
    layout["yaxis"]["tickformat"] = ".1%"
    layout["xaxis"]["tickformat"] = "$,.0f"
    fig.update_layout(**layout)
    return fig


# =============================================================================
# Grouped bars — Avg Order Amount by Day (this week vs last week)
# =============================================================================

def grouped_bars_with_goal(df: pd.DataFrame, x_col: str, last_col: str, this_col: str,
                            goal: float | None = None,
                            title: str | None = None) -> go.Figure:
    fig = go.Figure()
    fig.add_bar(x=df[x_col], y=df[last_col], name="Last Week", marker=dict(color=ACCENT_BLUE))
    fig.add_bar(x=df[x_col], y=df[this_col], name="This Week", marker=dict(color=ACCENT_ORANGE))
    if goal is not None:
        fig.add_hline(y=goal, line=dict(color=TEXT_MUTED, dash="dot", width=1),
                      annotation=dict(text="Goal", font=dict(color=TEXT_MUTED, size=10)),
                      annotation_position="top right")
    layout = _layout(title=title, height=300)
    layout["barmode"] = "group"
    layout["yaxis"]["tickformat"] = "$,.0f"
    fig.update_layout(**layout)
    return fig


# =============================================================================
# SLA bar (kept, restyled)
# =============================================================================

def sla_by_carrier_bar(df: pd.DataFrame) -> go.Figure:
    g = (df.groupby("carrier_name", as_index=False)
            .agg(sla_miss_rate=("sla_miss_rate", "mean"),
                 late_return_rate=("late_return_rate", "mean"),
                 orders_total=("orders_total", "sum"))
            .sort_values("sla_miss_rate", ascending=True))
    fig = go.Figure()
    fig.add_bar(
        x=g["sla_miss_rate"], y=g["carrier_name"], orientation="h",
        name="SLA miss rate", marker=dict(color=ACCENT_RED),
        hovertemplate="%{y}<br>SLA miss %{x:.1%}<extra></extra>",
    )
    fig.add_scatter(
        x=g["late_return_rate"], y=g["carrier_name"], mode="markers",
        name="Late-delivery returns", marker=dict(color=ACCENT_ORANGE, size=10, symbol="diamond"),
        hovertemplate="%{y}<br>Late-return %{x:.1%}<extra></extra>",
    )
    layout = _layout(title="Carrier SLA performance", height=380)
    layout["xaxis"]["tickformat"] = ".0%"
    fig.update_layout(**layout)
    return fig


# =============================================================================
# Stacked bar (kept) — review rating outcome
# =============================================================================

def review_distribution_bar(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_bar(x=df["rating"], y=df["reviews_count"] - df["returned_count"],
                name="Kept", marker=dict(color=ACCENT_BLUE))
    fig.add_bar(x=df["rating"], y=df["returned_count"],
                name="Returned", marker=dict(color=ACCENT_RED))
    layout = _layout(title="Review rating × outcome", height=280)
    layout["barmode"] = "stack"
    fig.update_layout(**layout)
    return fig


# =============================================================================
# Chargeback bar (kept, restyled)
# =============================================================================

def chargeback_by_method_bar(df: pd.DataFrame) -> go.Figure:
    fig = px.bar(
        df.sort_values("chargeback_rate", ascending=True),
        x="chargeback_rate", y="method_name",
        color="chargeback_risk",
        color_discrete_map={"very_low": ACCENT_GREEN, "low": ACCENT_BLUE,
                             "medium": ACCENT_ORANGE, "high": ACCENT_RED},
        labels={"chargeback_rate": "", "method_name": ""},
    )
    layout = _layout(title="Chargeback rate by payment method", height=300)
    layout["xaxis"]["tickformat"] = ".1%"
    fig.update_layout(**layout)
    return fig
