# db.py
import sqlite3
from contextlib import contextmanager
from typing import Iterable
import pandas as pd
from config import DB_PATH

@contextmanager
def conn_ctx(db_path: str = DB_PATH):
    con = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    try:
        yield con
    finally:
        con.commit()
        con.close()

def init_schema():
    with conn_ctx() as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS ohlc (
            instrument_token INTEGER NOT NULL,
            date TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            PRIMARY KEY (instrument_token, date)
        );
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_ohlc_token_date ON ohlc(instrument_token, date);")

def upsert_ohlc(token: int, df: pd.DataFrame):
    # Expected df columns: date, open, high, low, close, volume
    records = list(df[["date","open","high","low","close","volume"]].itertuples(index=False, name=None))
    with conn_ctx() as con:
        con.executemany("""
            INSERT INTO ohlc (instrument_token, date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_token, date) DO UPDATE SET
                open=excluded.open, high=excluded.high, low=excluded.low,
                close=excluded.close, volume=excluded.volume;
        """, [(token, *r) for r in records])

def load_ohlc(token: int) -> pd.DataFrame:
    with conn_ctx() as con:
        df = pd.read_sql_query(
            "SELECT date, open, high, low, close, volume FROM ohlc WHERE instrument_token=? ORDER BY date ASC",
            con, params=(token,), parse_dates=["date"]
        )
    return df
