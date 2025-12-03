#!/usr/bin/env python3
"""
nifty500_glb_scanner.py
Scan NIFTY-500 tickers for Dr. Wish Green Line Breakout (GLB) + RS + Volume confirmation.

Requirements:
  pip install yfinance pandas numpy requests openpyxl

Usage:
  python nifty500_glb_scanner.py

Outputs:
  - glb_signals.csv       : full scan results (sorted with signals first)
  - glb_signals.jsonl     : JSON-lines records (one per ticker)
"""

import yfinance as yf
import pandas as pd
import numpy as np
import requests
import time
import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# -------------- USER CONFIG --------------
TICKERS_FILE = "nifty500_tickers.txt"   # one ticker per line (e.g. RELIANCE.NS)
OUTPUT_CSV = "glb_signals.csv"
OUTPUT_JSONL = "glb_signals.jsonl"
BENCHMARK = "NIFTY_50.NS"               # benchmark Yahoo symbol (change if needed)
LOOKBACK_YEARS_GL = 5                   # Green Line lookback (years)
LOOKBACK_YEARS_RS = 3                   # RS lookback (years)
VOL_SMA_LEN = 20
VOL_MULT = 1.0
DATA_INTERVAL = "1d"                    # daily data
DOWNLOAD_BATCH = 60                     # how many tickers to fetch at a time (yfinance handles many)
RETRY_SLEEP = 2.0                       # seconds between failed retries
MAX_RETRIES = 3
WEBHOOK_URL = None                       # set to your webhook URL to push signals (optional)
MIN_TRADING_DAYS_REQUIRED = 60          # minimal data length for reliable computation
# -----------------------------------------

# Convert years -> trading days approx
TRADING_DAYS_PER_YEAR = 252
BARS_GLB = int(LOOKBACK_YEARS_GL * TRADING_DAYS_PER_YEAR)
BARS_RS = int(LOOKBACK_YEARS_RS * TRADING_DAYS_PER_YEAR)

def load_tickers(path: str) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Tickers file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
    return lines

def download_hist(tickers: List[str], days_back: int) -> pd.DataFrame:
    start = (datetime.today() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    for attempt in range(MAX_RETRIES):
        try:
            df = yf.download(tickers,
                             start=start,
                             interval=DATA_INTERVAL,
                             group_by="ticker",
                             auto_adjust=False,
                             threads=True,
                             progress=False)
            return df
        except Exception as e:
            print(f"[download_hist] Error (attempt {attempt+1}/{MAX_RETRIES}): {e}")
            time.sleep(RETRY_SLEEP)
    raise RuntimeError("Failed to download data from yfinance after retries")

def compute_for_ticker(sym: str, df_sym: pd.DataFrame, bench_close: pd.Series) -> Dict:
    out = {
        "ticker": sym,
        "date": None,
        "close": None,
        "glb": False,
        "rs_break": False,
        "vol_ok": False,
        "signal": False,
        "notes": ""
    }

    if df_sym is None or len(df_sym) < max(BARS_GLB, BARS_RS, VOL_SMA_LEN, MIN_TRADING_DAYS_REQUIRED):
        out["notes"] = "insufficient_data"
        return out

    df_sym = df_sym.sort_index().dropna(subset=["Close"])
    if df_sym.empty:
        out["notes"] = "no_close_data"
        return out

    bench_aligned = bench_close.reindex(df_sym.index).ffill().bfill()
    if bench_aligned.isnull().all():
        out["notes"] = "bench_not_aligned"
        return out

    hh = df_sym["High"].rolling(window=BARS_GLB, min_periods=1).max()
    hh_prev = hh.shift(1)

    latest = df_sym.index[-1]
    out["date"] = latest.strftime("%Y-%m-%d")
    out["close"] = float(df_sym["Close"].iloc[-1])

    try:
        is_new_hh = (hh.iloc[-1] > hh_prev.iloc[-1])
        close_above_prev = (df_sym["Close"].iloc[-1] > hh_prev.iloc[-1])
        out["glb"] = bool(is_new_hh and close_above_prev)
    except Exception:
        out["glb"] = False

    rs = df_sym["Close"] / bench_aligned
    rs_high = rs.rolling(window=BARS_RS, min_periods=1).max()
    rs_prev_high = rs_high.shift(1)
    try:
        out["rs_break"] = bool(rs.iloc[-1] > rs_prev_high.iloc[-1])
    except Exception:
        out["rs_break"] = False

    try:
        vol_sma = df_sym["Volume"].rolling(window=VOL_SMA_LEN, min_periods=1).mean()
        out["vol_ok"] = bool(df_sym["Volume"].iloc[-1] > VOL_MULT * vol_sma.iloc[-1])
    except Exception:
        out["vol_ok"] = False

    out["signal"] = bool(out["glb"] and out["rs_break"] and out["vol_ok"])
    return out

def chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    return [items[i:i+chunk_size] for i in range(0, len(items), chunk_size)]

def main():
    print("NIFTY-500 GLB Scanner starting...")
    tickers = load_tickers(TICKERS_FILE)
    print(f"Loaded {len(tickers)} tickers from {TICKERS_FILE}")

    max_days = int(max(LOOKBACK_YEARS_GL, LOOKBACK_YEARS_RS) * 365.25) + 60

    print(f"Downloading benchmark {BENCHMARK}...")
    bench_df = download_hist(BENCHMARK, max_days)
    if isinstance(bench_df.columns, pd.MultiIndex):
        try:
            bench_close = bench_df["Close"]
            if isinstance(bench_close, pd.DataFrame):
                bench_close = bench_close.iloc[:, 0]
        except Exception:
            close_cols = [c for c in bench_df.columns if c[1] == "Close"] if isinstance(bench_df.columns, pd.MultiIndex) else []
            if close_cols:
                bench_close = bench_df[close_cols[0]]
            else:
                bench_close = bench_df.iloc[:, 0]
    else:
        bench_close = bench_df["Close"] if "Close" in bench_df.columns else bench_df.iloc[:, 0]

    results = []
    errors = []

    batches = chunk_list(tickers, DOWNLOAD_BATCH)
    print(f"Processing {len(batches)} batches (batch size up to {DOWNLOAD_BATCH})...")

    for bi, batch in enumerate(batches, start=1):
        print(f"[{bi}/{len(batches)}] Downloading batch of {len(batch)} tickers...")
        try:
            df_batch = download_hist(batch, max_days)
        except Exception as e:
            print(f"Batch download error: {e}")
            for t in batch:
                errors.append({"ticker": t, "error": str(e)})
            time.sleep(RETRY_SLEEP)
            continue

        for sym in batch:
            try:
                if isinstance(df_batch.columns, pd.MultiIndex):
                    if sym in df_batch.columns.get_level_values(0):
                        df_sym = df_batch[sym].dropna(how="all")
                    else:
                        try:
                            df_sym = df_batch.xs(sym, level=0, axis=1).dropna(how="all")
                        except Exception:
                            df_sym = None
                else:
                    df_sym = df_batch.copy()

                if df_sym is None or df_sym.empty:
                    res = {"ticker": sym, "date": None, "close": None, "glb": False, "rs_break": False, "vol_ok": False, "signal": False, "notes": "no_data"}
                    results.append(res)
                    continue

                if "Close" not in df_sym.columns and "Adj Close" in df_sym.columns:
                    df_sym["Close"] = df_sym["Adj Close"]

                bench_aligned = bench_close.reindex(df_sym.index).ffill().bfill()

                res = compute_for_ticker(sym, df_sym, bench_aligned)
                results.append(res)

                if WEBHOOK_URL and res["signal"]:
                    payload = {"ticker": res["ticker"], "date": res["date"], "close": res["close"], "message": "GLB + RS + VOL SIGNAL"}
                    try:
                        requests.post(WEBHOOK_URL, json=payload, timeout=5)
                    except Exception as we:
                        print(f"Webhook post error for {sym}: {we}")

            except Exception as e:
                print(f"Error processing {sym}: {e}")
                errors.append({"ticker": sym, "error": str(e)})
        time.sleep(1.0)

    df_out = pd.DataFrame(results)
    df_out = df_out.sort_values(by=["signal", "ticker"], ascending=[False, True])
    df_out.to_csv(OUTPUT_CSV, index=False)
    print(f"Wrote CSV -> {OUTPUT_CSV}")

    with open(OUTPUT_JSONL, "w", encoding="utf-8") as jf:
        for r in results:
            jf.write(json.dumps(r) + "\n")
    print(f"Wrote JSONL -> {OUTPUT_JSONL}")

    if errors:
        print(f"Completed with {len(errors)} errors. First 5 errors: {errors[:5]}")

    signals = df_out[df_out["signal"]]
    print(f"Total signals found: {len(signals)}")
    if not signals.empty:
        print(signals[["ticker", "date", "close"]].to_string(index=False))

if __name__ == "__main__":
    main()
