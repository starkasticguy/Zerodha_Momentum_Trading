"""
Ingest module — download ~2 years of 5-minute NIFTY BANK candles into SQLite.

Keeps it simple:
- Resumable (continues from last timestamp if table not empty)
- Idempotent (PRIMARY KEY on timestamp)
- Obeys intraday range constraints with <=60-day chunks
- Polite sleep between calls

Usage:
    python ingest.py --days 730 --db market_data.db --table banknifty_5m
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from typing import Iterable, Tuple, Optional

import pandas as pd
from kiteconnect import KiteConnect
from kiteconnect.exceptions import KiteException

from auth import get_kite
from instruments import get_nifty_bank_token
from storage import SQLiteStore

MAX_WINDOW_DAYS = 60
INTERVAL = "5minute"
POLITE_SLEEP_SEC = 0.40


def daterange_chunks(start: pd.Timestamp, end: pd.Timestamp, days: int) -> Iterable[Tuple[pd.Timestamp, pd.Timestamp]]:
    s = start
    while s < end:
        e = min(s + pd.Timedelta(days=days), end)
        yield s, e
        s = e


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["timestamp","open","high","low","close","volume"])
    df = df.rename(columns={"date": "timestamp"})
    ts = pd.to_datetime(df["timestamp"], utc=False)
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize("Asia/Kolkata")
    df["timestamp"] = ts.dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    return df[["timestamp","open","high","low","close","volume"]]


def fetch_chunk(kite: KiteConnect, token: int, frm: pd.Timestamp, to: pd.Timestamp) -> pd.DataFrame:
    to_plus = to + pd.Timedelta(minutes=1)  # guard exclusivity
    raw = kite.historical_data(token, frm.to_pydatetime(), to_plus.to_pydatetime(), INTERVAL, continuous=False, oi=False)
    df = pd.DataFrame(raw)
    return _normalize_df(df)


def ingest(days: int, db_path: str, table: str) -> int:
    store = SQLiteStore(db_path=db_path, table=table)
    con = store.connect()

    # Determine start/end
    last = store.last_timestamp(con)
    if last is None:
        start_from = (pd.Timestamp.now(tz="Asia/Kolkata") - pd.Timedelta(days=days)).floor("D")
    else:
        start_from = pd.to_datetime(last).tz_convert("Asia/Kolkata") + pd.Timedelta(minutes=5)
    end_to = pd.Timestamp.now(tz="Asia/Kolkata")

    if start_from >= end_to:
        print("[ingest] up-to-date; nothing to fetch")
        con.close()
        return 0

    kite = get_kite()
    token = get_nifty_bank_token(kite)
    print(f"[ingest] NIFTY BANK token: {token}")

    inserted_total = 0

    for frm, to in daterange_chunks(start_from, end_to, MAX_WINDOW_DAYS):
        attempt = 0
        while True:
            try:
                df = fetch_chunk(kite, token, frm, to)
                break
            except KiteException as ke:
                attempt += 1
                wait = min(60, (2 ** attempt) * 0.5)
                print(f"[retry] {ke} | attempt {attempt} -> sleep {wait:.1f}s")
                time.sleep(wait)
        time.sleep(POLITE_SLEEP_SEC)

        inserted = store.insert_candles(con, df)
        inserted_total += max(inserted, 0)
        print(f"[chunk] {len(df):5d} rows [{frm} → {to}] | inserted {inserted:5d}")

    con.close()
    print(f"[ingest] inserted total: {inserted_total}")
    return inserted_total


def parse_args():
    ap = argparse.ArgumentParser(description="Ingest ~2 years of 5m NIFTY BANK candles into SQLite")
    ap.add_argument("--days", type=int, default=730, help="Lookback days if table empty (default: 730)")
    ap.add_argument("--db", type=str, default="market_data.db", help="SQLite db path (default: market_data.db)")
    ap.add_argument("--table", type=str, default="banknifty_5m", help="Table name (default: banknifty_5m)")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ingest(days=args.days, db_path=args.db, table=args.table)
