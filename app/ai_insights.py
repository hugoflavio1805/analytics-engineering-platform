"""
AI Insights module — calls the Anthropic Claude API to generate narrative
analysis on top of the marts.

Design choices (Amazon-style discipline):
  - Insights are seeded with PRE-COMPUTED data (not raw rows), so the model
    summarizes facts the dbt pipeline already proved. The model is a writer,
    not an analyst — this avoids hallucinated numbers.
  - Prompts are structured with explicit anti-hallucination instructions and
    "Working Backwards from the customer" framing.
  - Responses are cached on disk for 60 minutes per (mart_signature) so the
    dashboard re-runs don't burn tokens.
  - If the API key is missing, the module degrades gracefully — the dashboard
    still works without AI commentary.

Token budget: a typical run uses ~600 input tokens + 350 output tokens per
section (4 sections), so under $0.01 per refresh on Sonnet 4.6.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

CACHE_DIR = Path(__file__).parent / ".ai_cache"
CACHE_DIR.mkdir(exist_ok=True)
CACHE_TTL_SECONDS = 60 * 60   # 1 hour

DEFAULT_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")


# ---------------------------------------------------------------------------
# System prompt — written to constrain the model to the data we hand over.
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a senior business analyst writing a section for an OP1
operating-plan review at a major e-commerce company. Your reader is the VP of
Operations.

You receive PRE-AGGREGATED data from a dbt pipeline. You MUST:

1. Only cite numbers that appear verbatim in the input data. If a number
   isn't in the input, do not invent it.
2. Lead with the "so-what" — the action the VP should take, not a
   description of the data.
3. Use Amazon's narrative style: short paragraphs, one idea per paragraph,
   no bullet points unless asked, no marketing language.
4. Highlight anomalies (>2x deviation from baseline) as "watch items".
5. End with one concrete recommendation, framed as a hypothesis to test.

Format your response as Markdown. No preamble — start with the insight
directly. Maximum 4 short paragraphs."""


@dataclass
class Insight:
    section: str
    body_markdown: str
    used_cache: bool
    elapsed_ms: int
    error: str | None = None


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _signature(section: str, payload: dict) -> str:
    blob = json.dumps({"section": section, "payload": payload}, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def _read_cache(sig: str) -> str | None:
    path = CACHE_DIR / f"{sig}.json"
    if not path.exists():
        return None
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if time.time() - record.get("written_at", 0) > CACHE_TTL_SECONDS:
        return None
    return record.get("body")


def _write_cache(sig: str, body: str) -> None:
    path = CACHE_DIR / f"{sig}.json"
    path.write_text(
        json.dumps({"written_at": time.time(), "body": body}),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_insight(section: str, prompt_payload: dict) -> Insight:
    """Generate a narrative insight for the given section.

    `prompt_payload` is a dict of pre-aggregated facts. It is dumped as JSON
    into the user prompt so the model knows exactly what data is on the
    table.
    """
    sig = _signature(section, prompt_payload)
    cached = _read_cache(sig)
    if cached is not None:
        return Insight(section=section, body_markdown=cached, used_cache=True, elapsed_ms=0)

    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        placeholder = (
            "_AI commentary disabled — set `ANTHROPIC_API_KEY` in `.env` to enable._\n\n"
            f"Pre-computed facts for **{section}**:\n\n```json\n"
            f"{json.dumps(prompt_payload, indent=2, default=str)}\n```"
        )
        return Insight(
            section=section, body_markdown=placeholder, used_cache=False,
            elapsed_ms=0, error="missing_api_key",
        )

    try:
        from anthropic import Anthropic
    except ImportError:
        return Insight(
            section=section, body_markdown="", used_cache=False, elapsed_ms=0,
            error="anthropic-python not installed (run `pip install -r requirements_dashboard.txt`)",
        )

    client = Anthropic(api_key=api_key)
    user_prompt = (
        f"Section: **{section}**\n\n"
        f"Pre-aggregated facts (JSON):\n```json\n{json.dumps(prompt_payload, indent=2, default=str)}\n```\n\n"
        "Write the OP1 narrative for this section."
    )

    started = time.time()
    try:
        response = client.messages.create(
            model=DEFAULT_MODEL,
            max_tokens=600,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        body = response.content[0].text
    except Exception as exc:  # network / auth / rate-limit
        return Insight(
            section=section, body_markdown="",
            used_cache=False, elapsed_ms=int((time.time() - started) * 1000),
            error=str(exc),
        )

    elapsed_ms = int((time.time() - started) * 1000)
    _write_cache(sig, body)
    return Insight(section=section, body_markdown=body, used_cache=False, elapsed_ms=elapsed_ms)


# ---------------------------------------------------------------------------
# Section-specific payload builders
# ---------------------------------------------------------------------------

def payload_for_returns(returns_df: pd.DataFrame, anomalies: pd.DataFrame) -> dict:
    """Compress the returns mart into a small payload for the AI."""
    top = returns_df.nlargest(5, "return_rate")[
        ["category", "region", "orders_total", "returns_total", "return_rate", "gmv_loss_rate", "refund_total"]
    ]
    bottom = returns_df.nsmallest(3, "return_rate")[
        ["category", "region", "return_rate"]
    ]
    return {
        "global_avg_return_rate": float(returns_df["return_rate"].mean()),
        "global_total_refunds": float(returns_df["refund_total"].sum()),
        "top_5_offenders": top.to_dict("records"),
        "bottom_3_safe_zones": bottom.to_dict("records"),
        "anomalies_vs_baseline": anomalies.to_dict("records"),
    }


def payload_for_promotions(promo_df: pd.DataFrame) -> dict:
    promos = promo_df[promo_df["promotion_id"] != "NO_PROMOTION"].copy()
    aggressive = promos[promos["is_aggressive"] == True]
    return {
        "campaigns_count": int(len(promos)),
        "total_promo_gmv": float(promos["gmv"].sum()),
        "aggressive_campaigns_chargeback_rate": float(aggressive["chargeback_rate"].mean()) if len(aggressive) else 0.0,
        "non_aggressive_chargeback_rate": float(promos[promos["is_aggressive"] == False]["chargeback_rate"].mean()) if (promos["is_aggressive"] == False).any() else 0.0,
        "top_3_by_gmv": promos.nlargest(3, "gmv")[["promotion_name", "promotion_type", "discount_value", "gmv", "chargeback_rate"]].to_dict("records"),
    }


def payload_for_sla(sla_df: pd.DataFrame) -> dict:
    g = (
        sla_df.groupby("carrier_name", as_index=False)
              .agg(sla_miss_rate=("sla_miss_rate", "mean"),
                   late_return_rate=("late_return_rate", "mean"),
                   orders_total=("orders_total", "sum"))
              .sort_values("sla_miss_rate", ascending=False)
    )
    return {
        "worst_carriers": g.head(3).to_dict("records"),
        "best_carriers":  g.tail(3).to_dict("records"),
        "global_sla_miss_rate": float(g["sla_miss_rate"].mean()),
    }


def payload_for_overview(kpis: dict, gmv_monthly: pd.DataFrame) -> dict:
    last3 = gmv_monthly.tail(3)
    return {
        "kpis": {k: float(v) if isinstance(v, (int, float)) else v for k, v in kpis.items()},
        "last_3_months": last3.to_dict("records"),
        "peak_month": gmv_monthly.loc[gmv_monthly["gmv"].idxmax()].to_dict() if len(gmv_monthly) else {},
    }
