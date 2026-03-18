"""
NGX Investment Platform - Data Pipeline v3
===========================================
- Price data: EODHD API (free tier, correct XNSA exchange code)
- Fundamentals: Hardcoded from latest NGX filings (update quarterly)
- Scoring: Full Smart Score 0-100
"""

import json
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
import warnings
warnings.filterwarnings("ignore")

import os
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

EODHD_API_KEY = os.environ.get("EODHD_API_KEY")
BASE_URL      = "https://eodhd.com/api"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ngx")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


# ── NGX Stock Universe ────────────────────────────────────────────────────────
# EODHD uses TICKER.XNSA format for Nigerian Stock Exchange

NGX_STOCKS = {
    "GTCO.XNSA":       {"name": "Guaranty Trust Holding Co",  "sector": "Banking"},
    "ZENITHBANK.XNSA": {"name": "Zenith Bank",                "sector": "Banking"},
    "ACCESS.XNSA":     {"name": "Access Holdings",            "sector": "Banking"},
    "UBA.XNSA":        {"name": "United Bank for Africa",     "sector": "Banking"},
    "FBNH.XNSA":       {"name": "FBN Holdings",               "sector": "Banking"},
    "STANBIC.XNSA":    {"name": "Stanbic IBTC Holdings",      "sector": "Banking"},
    "NESTLE.XNSA":     {"name": "Nestle Nigeria",             "sector": "Consumer Goods"},
    "NB.XNSA":         {"name": "Nigerian Breweries",         "sector": "Consumer Goods"},
    "UNILEVER.XNSA":   {"name": "Unilever Nigeria",           "sector": "Consumer Goods"},
    "DANGSUGAR.XNSA":  {"name": "Dangote Sugar Refinery",     "sector": "Consumer Goods"},
    "DANGCEM.XNSA":    {"name": "Dangote Cement",             "sector": "Industrial"},
    "BUACEMENT.XNSA":  {"name": "BUA Cement",                 "sector": "Industrial"},
    "WAPCO.XNSA":      {"name": "Lafarge Africa",             "sector": "Industrial"},
    "SEPLAT.XNSA":     {"name": "Seplat Energy",              "sector": "Oil & Gas"},
    "CONOIL.XNSA":     {"name": "Conoil",                     "sector": "Oil & Gas"},
    "MTNN.XNSA":       {"name": "MTN Nigeria",                "sector": "Telecom"},
    "AIRTELAFRI.XNSA": {"name": "Airtel Africa",              "sector": "Telecom"},
    "FIDSON.XNSA":     {"name": "Fidson Healthcare",          "sector": "Healthcare"},
    "MAYBAKER.XNSA":   {"name": "May & Baker Nigeria",        "sector": "Healthcare"},
    "AIICO.XNSA":      {"name": "AIICO Insurance",            "sector": "Insurance"},
}


# ── Hardcoded Fundamentals ────────────────────────────────────────────────────
# Source: NGX filings, company annual reports (FY2023/2024)
# Update this section quarterly when new results are released.
# All ratios are as reported — adjust when companies publish new results.
#
# KEY:
#   pe_ratio       = Price / Earnings (lower = cheaper)
#   pb_ratio       = Price / Book value (lower = cheaper)
#   roe            = Return on Equity (higher = better, as decimal e.g. 0.28 = 28%)
#   profit_margin  = Net profit margin (higher = better, as decimal)
#   debt_to_equity = Total debt / equity (lower = safer)
#   dividend_yield = Annual dividend / price (as decimal e.g. 0.05 = 5%)
#   revenue_growth = YoY revenue growth (as decimal)
#   earnings_growth= YoY earnings growth (as decimal)
#   last_updated   = Quarter these figures are from

NGX_FUNDAMENTALS = {
    "GTCO.XNSA": {
        "pe_ratio": 3.2, "pb_ratio": 1.1, "roe": 0.42,
        "profit_margin": 0.38, "debt_to_equity": 45,
        "dividend_yield": 0.072, "revenue_growth": 0.68,
        "earnings_growth": 0.71, "last_updated": "FY2023"
    },
    "ZENITHBANK.XNSA": {
        "pe_ratio": 3.5, "pb_ratio": 1.2, "roe": 0.38,
        "profit_margin": 0.35, "debt_to_equity": 52,
        "dividend_yield": 0.085, "revenue_growth": 0.72,
        "earnings_growth": 0.65, "last_updated": "FY2023"
    },
    "ACCESS.XNSA": {
        "pe_ratio": 2.8, "pb_ratio": 0.8, "roe": 0.31,
        "profit_margin": 0.22, "debt_to_equity": 78,
        "dividend_yield": 0.045, "revenue_growth": 0.81,
        "earnings_growth": 0.58, "last_updated": "FY2023"
    },
    "UBA.XNSA": {
        "pe_ratio": 2.5, "pb_ratio": 0.7, "roe": 0.34,
        "profit_margin": 0.28, "debt_to_equity": 61,
        "dividend_yield": 0.062, "revenue_growth": 0.75,
        "earnings_growth": 0.62, "last_updated": "FY2023"
    },
    "FBNH.XNSA": {
        "pe_ratio": 2.1, "pb_ratio": 0.6, "roe": 0.28,
        "profit_margin": 0.21, "debt_to_equity": 85,
        "dividend_yield": 0.038, "revenue_growth": 0.69,
        "earnings_growth": 0.44, "last_updated": "FY2023"
    },
    "STANBIC.XNSA": {
        "pe_ratio": 4.1, "pb_ratio": 1.3, "roe": 0.33,
        "profit_margin": 0.31, "debt_to_equity": 48,
        "dividend_yield": 0.058, "revenue_growth": 0.55,
        "earnings_growth": 0.52, "last_updated": "FY2023"
    },
    "NESTLE.XNSA": {
        "pe_ratio": 18.5, "pb_ratio": 8.2, "roe": 0.45,
        "profit_margin": 0.12, "debt_to_equity": 120,
        "dividend_yield": 0.028, "revenue_growth": 0.42,
        "earnings_growth": -0.15, "last_updated": "FY2023"
    },
    "NB.XNSA": {
        "pe_ratio": 22.0, "pb_ratio": 3.1, "roe": 0.14,
        "profit_margin": 0.06, "debt_to_equity": 95,
        "dividend_yield": 0.018, "revenue_growth": 0.38,
        "earnings_growth": -0.32, "last_updated": "FY2023"
    },
    "UNILEVER.XNSA": {
        "pe_ratio": 15.2, "pb_ratio": 4.5, "roe": 0.29,
        "profit_margin": 0.09, "debt_to_equity": 88,
        "dividend_yield": 0.022, "revenue_growth": 0.31,
        "earnings_growth": 0.18, "last_updated": "FY2023"
    },
    "DANGSUGAR.XNSA": {
        "pe_ratio": 6.8, "pb_ratio": 2.1, "roe": 0.31,
        "profit_margin": 0.14, "debt_to_equity": 72,
        "dividend_yield": 0.035, "revenue_growth": 0.55,
        "earnings_growth": 0.28, "last_updated": "FY2023"
    },
    "DANGCEM.XNSA": {
        "pe_ratio": 8.5, "pb_ratio": 2.8, "roe": 0.33,
        "profit_margin": 0.28, "debt_to_equity": 42,
        "dividend_yield": 0.048, "revenue_growth": 0.48,
        "earnings_growth": 0.22, "last_updated": "FY2023"
    },
    "BUACEMENT.XNSA": {
        "pe_ratio": 7.2, "pb_ratio": 2.2, "roe": 0.30,
        "profit_margin": 0.25, "debt_to_equity": 38,
        "dividend_yield": 0.042, "revenue_growth": 0.52,
        "earnings_growth": 0.31, "last_updated": "FY2023"
    },
    "WAPCO.XNSA": {
        "pe_ratio": 9.1, "pb_ratio": 1.8, "roe": 0.20,
        "profit_margin": 0.15, "debt_to_equity": 55,
        "dividend_yield": 0.031, "revenue_growth": 0.44,
        "earnings_growth": 0.15, "last_updated": "FY2023"
    },
    "SEPLAT.XNSA": {
        "pe_ratio": 5.5, "pb_ratio": 1.4, "roe": 0.26,
        "profit_margin": 0.22, "debt_to_equity": 62,
        "dividend_yield": 0.041, "revenue_growth": 0.35,
        "earnings_growth": 0.28, "last_updated": "FY2023"
    },
    "CONOIL.XNSA": {
        "pe_ratio": 7.8, "pb_ratio": 1.6, "roe": 0.21,
        "profit_margin": 0.08, "debt_to_equity": 44,
        "dividend_yield": 0.035, "revenue_growth": 0.28,
        "earnings_growth": 0.19, "last_updated": "FY2023"
    },
    "MTNN.XNSA": {
        "pe_ratio": 12.5, "pb_ratio": 0.0, "roe": 0.00,
        "profit_margin": 0.08, "debt_to_equity": 180,
        "dividend_yield": 0.055, "revenue_growth": 0.32,
        "earnings_growth": -0.45, "last_updated": "FY2023"
    },
    "AIRTELAFRI.XNSA": {
        "pe_ratio": 10.2, "pb_ratio": 2.5, "roe": 0.24,
        "profit_margin": 0.11, "debt_to_equity": 145,
        "dividend_yield": 0.038, "revenue_growth": 0.18,
        "earnings_growth": 0.12, "last_updated": "FY2023"
    },
    "FIDSON.XNSA": {
        "pe_ratio": 8.5, "pb_ratio": 2.1, "roe": 0.25,
        "profit_margin": 0.13, "debt_to_equity": 58,
        "dividend_yield": 0.028, "revenue_growth": 0.41,
        "earnings_growth": 0.35, "last_updated": "FY2023"
    },
    "MAYBAKER.XNSA": {
        "pe_ratio": 9.8, "pb_ratio": 1.9, "roe": 0.19,
        "profit_margin": 0.10, "debt_to_equity": 48,
        "dividend_yield": 0.022, "revenue_growth": 0.38,
        "earnings_growth": 0.25, "last_updated": "FY2023"
    },
    "AIICO.XNSA": {
        "pe_ratio": 5.2, "pb_ratio": 0.9, "roe": 0.18,
        "profit_margin": 0.09, "debt_to_equity": 35,
        "dividend_yield": 0.031, "revenue_growth": 0.29,
        "earnings_growth": 0.22, "last_updated": "FY2023"
    },
}


# ── Step 1: Fetch Prices from EODHD ──────────────────────────────────────────

def fetch_prices(tickers, days=365):
    log.info(f"Fetching price history for {len(tickers)} stocks ({days} days)...")
    end   = datetime.today().strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    all_prices = {}
    failed = []

    for ticker in tickers:
        url    = f"{BASE_URL}/eod/{ticker}"
        params = {"api_token": EODHD_API_KEY, "from": start, "to": end, "fmt": "json"}
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code != 200:
                log.warning(f"  x {ticker}: HTTP {r.status_code}")
                failed.append(ticker)
                continue
            data = r.json()
            if not data:
                log.warning(f"  x {ticker}: No data")
                failed.append(ticker)
                continue
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date").sort_index()
            all_prices[ticker] = df["close"]
            log.info(f"  v {ticker}  ({len(df)} days, last: {df['close'].iloc[-1]:.2f})")
            time.sleep(0.3)
        except Exception as e:
            log.warning(f"  x {ticker}: {e}")
            failed.append(ticker)

    if failed:
        log.warning(f"Could not fetch: {failed}")
    if not all_prices:
        log.error("No price data fetched. Check EODHD_API_KEY and exchange codes.")
        return pd.DataFrame()

    prices = pd.DataFrame(all_prices)
    prices.index = pd.to_datetime(prices.index)
    return prices.sort_index()


# ── Step 2: Load Hardcoded Fundamentals ───────────────────────────────────────

def load_fundamentals(tickers):
    log.info(f"Loading fundamental data for {len(tickers)} stocks...")
    rows = []
    for ticker in tickers:
        fund = NGX_FUNDAMENTALS.get(ticker, {})
        row  = {"ticker": ticker}
        row.update(fund)
        rows.append(row)
        src = fund.get("last_updated", "default")
        log.info(f"  v {ticker}  (source: {src})")
    df = pd.DataFrame(rows).set_index("ticker")
    return df


# ── Step 3: Technical Indicators ─────────────────────────────────────────────

def _compute_rsi(prices, period=14):
    delta = prices.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss
    rsi   = 100 - (100 / (1 + rs))
    val   = rsi.iloc[-1]
    return float(val) if pd.notna(val) else None


def compute_technicals(prices):
    log.info("Computing technical indicators...")
    rows = []
    if prices.empty:
        return pd.DataFrame()

    for ticker in prices.columns:
        p = prices[ticker].dropna()
        if len(p) < 10:
            continue
        close   = p.values
        current = close[-1]

        ret_1w  = (current / close[-5]   - 1) if len(close) >= 5   else None
        ret_1m  = (current / close[-21]  - 1) if len(close) >= 21  else None
        ret_3m  = (current / close[-63]  - 1) if len(close) >= 63  else None
        ret_6m  = (current / close[-126] - 1) if len(close) >= 126 else None
        ret_1y  = (current / close[-252] - 1) if len(close) >= 252 else None
        ma_50   = float(p.rolling(50).mean().iloc[-1])  if len(p) >= 50  else None
        ma_200  = float(p.rolling(200).mean().iloc[-1]) if len(p) >= 200 else None
        vola    = float(p.pct_change().dropna().std() * (252**0.5)) if len(p) > 10 else None
        rsi     = _compute_rsi(p)
        h52     = float(p.tail(252).max()) if len(p) >= 252 else float(p.max())
        l52     = float(p.tail(252).min()) if len(p) >= 252 else float(p.min())

        rows.append({
            "ticker":         ticker,
            "current_price":  round(current, 2),
            "ret_1w":         ret_1w,
            "ret_1m":         ret_1m,
            "ret_3m":         ret_3m,
            "ret_6m":         ret_6m,
            "ret_1y":         ret_1y,
            "ma_50":          round(ma_50,  2) if ma_50  else None,
            "ma_200":         round(ma_200, 2) if ma_200 else None,
            "above_ma50":     bool(current > ma_50)  if ma_50  else None,
            "above_ma200":    bool(current > ma_200) if ma_200 else None,
            "rsi_14":         round(rsi, 1) if rsi else None,
            "volatility_ann": round(vola, 4) if vola else None,
            "high_52w":       round(h52, 2),
            "low_52w":        round(l52, 2),
            "pct_from_high":  round(current / h52 - 1, 4) if h52 else None,
            "pct_from_low":   round(current / l52 - 1, 4) if l52 else None,
        })

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return df.set_index("ticker") if "ticker" in df.columns else pd.DataFrame()


# ── Step 4: Smart Score ────────────────────────────────────────────────────────

def compute_smart_score(fundamentals, technicals):
    log.info("Computing Smart Scores...")
    if fundamentals.empty or technicals.empty:
        log.warning("Cannot compute scores — data is empty.")
        return pd.DataFrame()

    merged = fundamentals.join(technicals, how="inner")
    if merged.empty:
        log.warning("No matching tickers.")
        return pd.DataFrame()

    scores = pd.DataFrame(index=merged.index)

    def norm(series, low, high, invert=False):
        clipped  = series.clip(lower=low, upper=high)
        result   = (clipped - low) / (high - low)
        return (1 - result) if invert else result

    def col(name, default):
        return pd.to_numeric(
            merged.get(name, pd.Series([default]*len(merged), index=merged.index)),
            errors="coerce"
        ).fillna(default)

    # Fundamental (35 pts)
    fund_score = (
        norm(col("pe_ratio",      15),  3,  40, invert=True) * 0.25 +
        norm(col("roe",          0.1),  0, 0.4)              * 0.30 +
        norm(col("profit_margin",0.1),-0.1,0.4)              * 0.20 +
        norm(col("debt_to_equity",50),  0, 200, invert=True) * 0.15 +
        norm(col("dividend_yield",0.03),0, 0.12)             * 0.10
    ) * 35

    # Momentum (25 pts)
    mom_score = (
        norm(col("ret_1m",  0), -0.3, 0.3) * 0.30 +
        norm(col("ret_3m",  0), -0.5, 0.5) * 0.30 +
        col("above_ma50", 0.5)              * 0.25 +
        norm(col("rsi_14", 50),  30,   70)  * 0.15
    ) * 25

    # Value (25 pts)
    val_score = (
        norm(col("pb_ratio",     1.5), 0.2, 5, invert=True) * 0.40 +
        norm(col("pct_from_low", 0.3),   0, 1)              * 0.35 +
        norm(col("dividend_yield",0.03),  0, 0.12)          * 0.25
    ) * 25

    # Growth (15 pts)
    growth_score = (
        norm(col("revenue_growth",  0.1), -0.2, 0.8) * 0.50 +
        norm(col("earnings_growth", 0.1), -0.5, 1.0) * 0.50
    ) * 15

    scores["fundamental_score"] = fund_score.round(1)
    scores["momentum_score"]    = mom_score.round(1)
    scores["value_score"]       = val_score.round(1)
    scores["growth_score"]      = growth_score.round(1)
    scores["smart_score"]       = (fund_score + mom_score + val_score + growth_score).round(1)

    def label(s):
        if s >= 72: return "Strong Buy"
        if s >= 58: return "Buy"
        if s >= 44: return "Hold"
        if s >= 30: return "Caution"
        return "Avoid"

    scores["signal"]    = scores["smart_score"].apply(label)
    scores["risk_flag"] = (
        (col("volatility_ann", 0) > 0.60) | (col("debt_to_equity", 0) > 150)
    ).map({True: "High Risk", False: "Normal"})

    scores["current_price"]  = col("current_price",   0)
    scores["ret_1m"]         = (col("ret_1m",          0) * 100).round(1)
    scores["ret_3m"]         = (col("ret_3m",          0) * 100).round(1)
    scores["dividend_yield"] = (col("dividend_yield",  0) * 100).round(2)
    scores["pe_ratio"]       = col("pe_ratio",         0).round(1)
    scores["roe"]            = (col("roe",             0) * 100).round(1)

    return scores.sort_values("smart_score", ascending=False)


# ── Step 5: Save Outputs ──────────────────────────────────────────────────────

def save_outputs(prices, fundamentals, technicals, scores, stock_meta):
    today = datetime.today().strftime("%Y-%m-%d")
    if not prices.empty:       prices.to_csv(DATA_DIR / f"prices_{today}.csv")
    if not fundamentals.empty: fundamentals.to_csv(DATA_DIR / f"fundamentals_{today}.csv")
    if not technicals.empty:   technicals.to_csv(DATA_DIR / f"technicals_{today}.csv")
    if not scores.empty:
        scores.to_csv(DATA_DIR / f"scores_{today}.csv")
        scores.to_csv(DATA_DIR / "scores_latest.csv")
        fundamentals.to_csv(DATA_DIR / "fundamentals_latest.csv")

    if scores.empty:
        log.warning("Scores empty — skipping JSON.")
        return

    summary = []
    for ticker, row in scores.iterrows():
        meta = stock_meta.get(ticker, {})
        fund = NGX_FUNDAMENTALS.get(ticker, {})
        summary.append({
            "ticker":         ticker,
            "name":           meta.get("name", ticker),
            "sector":         meta.get("sector", "Unknown"),
            "smart_score":    row["smart_score"],
            "signal":         row["signal"],
            "risk_flag":      row["risk_flag"],
            "current_price":  row["current_price"],
            "ret_1m_pct":     row["ret_1m"],
            "ret_3m_pct":     row["ret_3m"],
            "dividend_yield": row["dividend_yield"],
            "pe_ratio":       row["pe_ratio"],
            "roe_pct":        row["roe"],
            "fundamentals_as_of": fund.get("last_updated", "unknown"),
            "sub_scores": {
                "fundamental": row["fundamental_score"],
                "momentum":    row["momentum_score"],
                "value":       row["value_score"],
                "growth":      row["growth_score"],
            },
            "last_updated": today,
        })

    with open(DATA_DIR / "summary_latest.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)
    log.info("v Saved all outputs to /data/")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_pipeline(tickers=None):
    if not EODHD_API_KEY:
        raise EnvironmentError("EODHD_API_KEY not found in .env file!")

    stock_meta = NGX_STOCKS
    if tickers:
        stock_meta = {k: v for k, v in NGX_STOCKS.items() if k in tickers}
    ticker_list = list(stock_meta.keys())

    log.info("=" * 60)
    log.info("  NGX Investment Platform - Data Pipeline v3")
    log.info(f"  Stocks: {len(ticker_list)}  |  {datetime.today().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    prices       = fetch_prices(ticker_list, days=365)
    fundamentals = load_fundamentals(ticker_list)
    technicals   = compute_technicals(prices)
    scores       = compute_smart_score(fundamentals, technicals)
    save_outputs(prices, fundamentals, technicals, scores, stock_meta)

    if not scores.empty:
        log.info("\n-- Top 10 NGX Smart Scores --")
        cols = [c for c in ["smart_score","signal","current_price","ret_1m","pe_ratio","roe"] if c in scores.columns]
        print(scores[cols].head(10).to_string())
    else:
        log.warning("No scores generated — prices may not have loaded.")

    log.info("\nPipeline complete.")
    return scores


if __name__ == "__main__":
    run_pipeline()
