"""
Project CLI — SQL-only pipeline for NIFTY BANK 5-minute data.

Subcommands:
    python main.py ingest        # pull ~2 years (or resume) into SQLite
    python main.py rsi           # compute RSI-14 and persist to SQL
    python main.py status        # show simple table stats

Environment:
    KITE_API_KEY, KITE_API_SECRET must be set (used by auth.get_kite())
"""
from __future__ import annotations

import argparse

from ingest import ingest
from ops import compute_and_persist_rsi
from storage import SQLiteStore
KITE_API_KEY = "pi2xk2iokf0u2nt3"
KITE_API_SECRET = "ssifv93g7g6xo9lofpduwwzy4n45q3d2"

def cmd_ingest(args):
    ingest(days=args.days, db_path=args.db, table=args.table)


def cmd_rsi(args):
    compute_and_persist_rsi(db_path=args.db, table=args.table, only_missing=args.only_missing)


def cmd_status(args):
    store = SQLiteStore(db_path=args.db, table=args.table)
    con = store.connect()
    try:
        n = store.count(con)
        last = store.last_timestamp(con)
        print("DB:", args.db)
        print("Table:", args.table)
        print("Rows:", n)
        print("Last timestamp:", last)
    finally:
        con.close()


def parse_args():
    p = argparse.ArgumentParser(description="NIFTY BANK SQL-only data pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_ing = sub.add_parser("ingest", help="Ingest ~2 years of 5m data")
    p_ing.add_argument("--days", type=int, default=730, help="Lookback days if table empty")
    p_ing.add_argument("--db", default="market_data.db", help="SQLite db path")
    p_ing.add_argument("--table", default="banknifty_5m", help="Table name")
    p_ing.set_defaults(func=cmd_ingest)

    p_rsi = sub.add_parser("rsi", help="Compute RSI-14 and persist")
    p_rsi.add_argument("--db", default="market_data.db", help="SQLite db path")
    p_rsi.add_argument("--table", default="banknifty_5m", help="Table name")
    p_rsi.add_argument("--only-missing", action="store_true", help="Only fill NULL RSI values")
    p_rsi.set_defaults(func=cmd_rsi)

    p_stat = sub.add_parser("status", help="Show row count and last timestamp")
    p_stat.add_argument("--db", default="market_data.db", help="SQLite db path")
    p_stat.add_argument("--table", default="banknifty_5m", help="Table name")
    p_stat.set_defaults(func=cmd_status)

    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    args.func(args)
