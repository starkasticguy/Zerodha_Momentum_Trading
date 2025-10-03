"""
SQLite storage — SQL‑only persistence for NIFTY BANK 5m candles.

Schema (single table):
    CREATE TABLE IF NOT EXISTS banknifty_5m (
        timestamp TEXT PRIMARY KEY,   -- ISO8601 with tz offset (Asia/Kolkata)
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        rsi14 REAL                    -- nullable; computed later
    );

This module keeps things simple and explicit — no ORMs.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence, Tuple

import pandas as pd

DEFAULT_DB_PATH = "market_data.db"
DEFAULT_TABLE = "banknifty_5m"


@dataclass
class SQLiteStore:
    db_path: str = DEFAULT_DB_PATH
    table: str = DEFAULT_TABLE

    # ---------- connections ----------
    def connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path)
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                timestamp TEXT PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                rsi14 REAL
            )
            """
        )
        con.commit()
        return con

    # ---------- writes ----------
    def insert_candles(self, con: sqlite3.Connection, df: pd.DataFrame) -> int:
        """Insert OHLCV rows. Idempotent via PRIMARY KEY(timestamp).
        Expects columns: timestamp, open, high, low, close, volume
        """
        if df is None or df.empty:
            return 0
        cols = ["timestamp", "open", "high", "low", "close", "volume"]
        missing = [c for c in cols if c not in df.columns]
        if missing:
            raise ValueError(f"insert_candles() missing columns: {missing}")
        rows = [tuple(x) for x in df[cols].itertuples(index=False, name=None)]
        before = con.total_changes
        con.executemany(
            f"INSERT OR IGNORE INTO {self.table} (timestamp, open, high, low, close, volume) VALUES (?,?,?,?,?,?)",
            rows,
        )
        con.commit()
        return con.total_changes - before

    def update_rsi(self, con: sqlite3.Connection, rows: Sequence[Tuple[float, str]]) -> int:
        """Bulk update (rsi14, timestamp). Skips NaNs automatically via SQL semantics if you prefilter."""
        if not rows:
            return 0
        before = con.total_changes
        con.executemany(
            f"UPDATE {self.table} SET rsi14 = ? WHERE timestamp = ?",
            rows,
        )
        con.commit()
        return con.total_changes - before

    # ---------- reads ----------
    def last_timestamp(self, con: sqlite3.Connection) -> Optional[pd.Timestamp]:
        cur = con.execute(f"SELECT MAX(timestamp) FROM {self.table}")
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return pd.Timestamp(row[0])

    def fetch_all(self, con: sqlite3.Connection) -> pd.DataFrame:
        return pd.read_sql_query(
            f"SELECT timestamp, open, high, low, close, volume, rsi14 FROM {self.table} ORDER BY timestamp",
            con,
        )

    def fetch_missing_rsi(self, con: sqlite3.Connection) -> pd.DataFrame:
        return pd.read_sql_query(
            f"SELECT timestamp, close FROM {self.table} WHERE rsi14 IS NULL ORDER BY timestamp",
            con,
        )

    def count(self, con: sqlite3.Connection) -> int:
        cur = con.execute(f"SELECT COUNT(1) FROM {self.table}")
        return int(cur.fetchone()[0])
