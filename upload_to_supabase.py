"""
NGX Pipeline — Supabase Uploader
==================================
Reads the pipeline outputs and pushes them into your Supabase tables.
Run this after pipeline.py, or call upload_all() from within the pipeline.

Usage:
  1. Add your Supabase credentials to a .env file (see below)
  2. Run:  python upload_to_supabase.py
"""

import os
import json
import logging
from datetime import date
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from supabase import create_client, Client

load_dotenv()   # reads your .env file automatically

log = logging.getLogger("ngx.supabase")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")

DATA_DIR = Path("data")

# ── Connect to Supabase ────────────────────────────────────────────────────────

def get_client() -> Client:
    """
    Create a Supabase client using credentials from your .env file.
    Uses the SERVICE ROLE key so the pipeline can write to the database.
    """
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")   # service role key (never expose publicly)

    if not url or not key:
        raise EnvironmentError(
            "\n\nMissing Supabase credentials!\n"
            "Create a .env file in this folder with:\n\n"
            "  SUPABASE_URL=https://your-project.supabase.co\n"
            "  SUPABASE_SERVICE_KEY=your-service-role-key\n\n"
            "Find these in: Supabase Dashboard → Settings → API\n"
        )
    return create_client(url, key)


# ── Upload helpers ─────────────────────────────────────────────────────────────

def _upsert(client: Client, table: str, records: list[dict], conflict_cols: str):
    """
    Insert rows, updating existing ones if the unique key already exists.
    'conflict_cols' is the column(s) that define uniqueness (e.g. 'ticker,score_date').
    """
    if not records:
        log.warning(f"  No records to upload for {table}")
        return

    BATCH = 100   # Supabase recommends batches of ~100–500 rows
    for i in range(0, len(records), BATCH):
        batch = records[i : i + BATCH]
        client.table(table).upsert(batch, on_conflict=conflict_cols).execute()

    log.info(f"  ✓ {table}: {len(records)} rows uploaded")


def _safe(val):
    """Convert NaN / None / numpy types to plain Python for JSON serialisation."""
    import math
    import numpy as np
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return None if math.isnan(float(val)) else float(val)
    if isinstance(val, (np.bool_,)):
        return bool(val)
    if isinstance(val, float) and val == int(val):
        return int(val)
    return val


# ── Table-specific uploaders ───────────────────────────────────────────────────

def upload_prices(client: Client, prices_df: pd.DataFrame):
    """Upload historical prices. prices_df: rows=dates, cols=tickers."""
    log.info("Uploading prices…")
    records = []
    today = date.today().isoformat()

    for ticker in prices_df.columns:
        series = prices_df[ticker].dropna()
        for dt, close in series.items():
            records.append({
                "ticker":      ticker,
                "price_date":  str(dt.date()),
                "close_price": _safe(close),
            })

    _upsert(client, "prices", records, "ticker,price_date")


def upload_fundamentals(client: Client, fundamentals_df: pd.DataFrame):
    """Upload fundamental ratios."""
    log.info("Uploading fundamentals…")
    today = date.today().isoformat()
    records = []

    col_map = {
        "pe_ratio":           "pe_ratio",
        "pb_ratio":           "pb_ratio",
        "ps_ratio":           "ps_ratio",
        "ev_ebitda":          "ev_ebitda",
        "roe":                "roe",
        "roa":                "roa",
        "profit_margin":      "profit_margin",
        "gross_margin":       "gross_margin",
        "debt_to_equity":     "debt_to_equity",
        "current_ratio":      "current_ratio",
        "quick_ratio":        "quick_ratio",
        "revenue_growth":     "revenue_growth",
        "earnings_growth":    "earnings_growth",
        "dividend_yield":     "dividend_yield",
        "market_cap":         "market_cap",
        "shares_outstanding": "shares_outstanding",
    }

    for ticker, row in fundamentals_df.iterrows():
        record = {"ticker": ticker, "fetch_date": today}
        for src_col, dest_col in col_map.items():
            if src_col in row:
                record[dest_col] = _safe(row[src_col])
        records.append(record)

    _upsert(client, "fundamentals", records, "ticker,fetch_date")


def upload_technicals_to_prices(client: Client, technicals_df: pd.DataFrame):
    """
    Technicals are stored in the prices table alongside closing prices.
    This updates today's price rows with the computed indicator values.
    """
    log.info("Uploading technical indicators to prices table…")
    today = date.today().isoformat()
    records = []

    tech_cols = [
        "current_price", "ret_1w", "ret_1m", "ret_3m", "ret_6m", "ret_1y",
        "ma_50", "ma_200", "above_ma50", "above_ma200",
        "rsi_14", "volatility_ann", "high_52w", "low_52w",
        "pct_from_high", "pct_from_low",
    ]

    price_col_map = {
        "current_price": "close_price",
        "ret_1w":        "ret_1w",
        "ret_1m":        "ret_1m",
        "ret_3m":        "ret_3m",
        "ret_6m":        "ret_6m",
        "ret_1y":        "ret_1y",
        "ma_50":         "ma_50",
        "ma_200":        "ma_200",
        "above_ma50":    "above_ma50",
        "above_ma200":   "above_ma200",
        "rsi_14":        "rsi_14",
        "volatility_ann":"volatility_ann",
        "high_52w":      "high_52w",
        "low_52w":       "low_52w",
        "pct_from_high": "pct_from_high",
        "pct_from_low":  "pct_from_low",
    }

    for ticker, row in technicals_df.iterrows():
        record = {"ticker": ticker, "price_date": today}
        for src_col, dest_col in price_col_map.items():
            if src_col in row:
                record[dest_col] = _safe(row[src_col])
        records.append(record)

    _upsert(client, "prices", records, "ticker,price_date")


def upload_scores(client: Client, scores_df: pd.DataFrame):
    """Upload Smart Scores — the main output the dashboard reads."""
    log.info("Uploading Smart Scores…")
    today = date.today().isoformat()
    records = []

    for ticker, row in scores_df.iterrows():
        records.append({
            "ticker":              ticker,
            "score_date":          today,
            "fundamental_score":   _safe(row.get("fundamental_score")),
            "momentum_score":      _safe(row.get("momentum_score")),
            "value_score":         _safe(row.get("value_score")),
            "growth_score":        _safe(row.get("growth_score")),
            "smart_score":         _safe(row.get("smart_score")),
            "signal":              str(row.get("signal", "Hold")),
            "risk_flag":           str(row.get("risk_flag", "Normal")),
            "current_price":       _safe(row.get("current_price")),
            "ret_1m_pct":          _safe(row.get("ret_1m")),
            "ret_3m_pct":          _safe(row.get("ret_3m")),
            "dividend_yield_pct":  _safe(row.get("dividend_yield")),
            "pe_ratio":            _safe(row.get("pe_ratio")),
            "roe_pct":             _safe(row.get("roe")),
        })

    _upsert(client, "scores", records, "ticker,score_date")


# ── Main upload function ───────────────────────────────────────────────────────

def upload_all(
    prices_df=None,
    fundamentals_df=None,
    technicals_df=None,
    scores_df=None,
):
    """
    Upload all pipeline outputs to Supabase.
    Pass DataFrames directly, or leave as None to load from /data folder.
    """
    client = get_client()
    log.info("Connected to Supabase ✓")

    # Load from disk if not passed in directly
    if prices_df is None and (DATA_DIR / "prices_latest.csv").exists():
        # prices file has dates as index
        prices_df = pd.read_csv(DATA_DIR / "prices_latest.csv", index_col=0, parse_dates=True)
        # Find the most recent dated prices file
        dated = sorted(DATA_DIR.glob("prices_*.csv"))
        if dated:
            prices_df = pd.read_csv(dated[-1], index_col=0, parse_dates=True)

    if fundamentals_df is None and (DATA_DIR / "fundamentals_latest.csv").exists():
        fundamentals_df = pd.read_csv(DATA_DIR / "fundamentals_latest.csv", index_col=0)

    if technicals_df is None:
        dated = sorted(DATA_DIR.glob("technicals_*.csv"))
        if dated:
            technicals_df = pd.read_csv(dated[-1], index_col=0)

    if scores_df is None and (DATA_DIR / "scores_latest.csv").exists():
        scores_df = pd.read_csv(DATA_DIR / "scores_latest.csv", index_col=0)

    # Upload each table
    if prices_df is not None:
        upload_prices(client, prices_df)
    if fundamentals_df is not None:
        upload_fundamentals(client, fundamentals_df)
    if technicals_df is not None:
        upload_technicals_to_prices(client, technicals_df)
    if scores_df is not None:
        upload_scores(client, scores_df)

    log.info("\n✓ All data uploaded to Supabase successfully.")
    log.info("  Check your dashboard: Supabase → Table Editor → scores")


if __name__ == "__main__":
    upload_all()
