"""
Ops module — compute RSI-14 over stored candles and persist to SQL.

Usage:
    python ops.py --db market_data.db --table banknifty_5m --only-missing
"""
from __future__ import annotations

import argparse
from typing import Optional

import numpy as np
import pandas as pd

from storage import SQLiteStore


def rsi_wilder(close: pd.Series, period: int = 14) -> pd.Series:
    """RSI with Wilder's smoothing (common convention).
    Returns [0, 100] with NaN for first `period` samples.
    """
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def compute_and_persist_rsi(db_path: str, table: str, only_missing: bool = False) -> int:
    store = SQLiteStore(db_path=db_path, table=table)
    con = store.connect()

    # Ensure column exists (created by schema), else ALTER TABLE
    con.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in con.fetchall()] if (cur := con.execute(f"PRAGMA table_info({table})")) else []
    if "rsi14" not in [r[1] for r in cur.fetchall()] if cur else []:
        con.execute(f"ALTER TABLE {table} ADD COLUMN rsi14 REAL")
        con.commit()

    if only_missing:
        df = store.fetch_missing_rsi(con)
        if df.empty:
            print("[rsi] no missing rows; up-to-date")
            con.close()
            return 0
        df_sorted = df.sort_values("timestamp").reset_index(drop=True)
        rsi = rsi_wilder(df_sorted["close"].astype(float), period=14)
        df_sorted["rsi14"] = rsi
        rows = [ (float(v), ts) for v, ts in zip(df_sorted["rsi14"], df_sorted["timestamp"]) if pd.notna(v) ]
        updated = store.update_rsi(con, rows)
        con.close()
        print(f"[rsi] updated rows: {updated}")
        return updated

    # full recompute
    df = store.fetch_all(con)
    if df.empty:
        print("[rsi] table empty; run ingest first")
        con.close()
        return 0

    df = df.sort_values("timestamp").reset_index(drop=True)
    df["rsi14"] = rsi_wilder(df["close"].astype(float), period=14)
    rows = [ (float(v), ts) for v, ts in zip(df["rsi14"], df["timestamp"]) if pd.notna(v) ]
    updated = store.update_rsi(con, rows)
    con.close()
    print(f"[rsi] updated rows: {updated}")
    return updated


def parse_args():
    ap = argparse.ArgumentParser(description="Compute RSI-14 and persist into SQLite")
    ap.add_argument("--db", default="market_data.db", help="SQLite db path")
    ap.add_argument("--table", default="banknifty_5m", help="Table name")
    ap.add_argument("--only-missing", action="store_true", help="Only fill rows where RSI is NULL")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    compute_and_persist_rsi(args.db, args.table, args.only_missing)
