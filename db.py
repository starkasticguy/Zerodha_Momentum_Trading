# db.py — tiny SQLite wrapper
import sqlite3
from pathlib import Path
import pandas as pd
from typing import Iterable, Dict, Any

from config import DB_PATH

def get_conn():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)

def init():
    with get_conn() as con:
        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS ohlc (
              ts TIMESTAMP NOT NULL,
              open REAL NOT NULL,
              high REAL NOT NULL,
              low REAL NOT NULL,
              close REAL NOT NULL,
              volume REAL,
              oi REAL,
              instrument_token INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              interval TEXT NOT NULL,
              PRIMARY KEY (ts, instrument_token, interval)
            )
            '''
        )
        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS instruments (
              instrument_token INTEGER PRIMARY KEY,
              exchange_token INTEGER,
              tradingsymbol TEXT,
              name TEXT,
              last_price REAL,
              expiry TEXT,
              strike REAL,
              tick_size REAL,
              lot_size INTEGER,
              instrument_type TEXT,
              segment TEXT,
              exchange TEXT
            )
            '''
        )
        con.commit()

def upsert_ohlc(rows: Iterable[Dict[str, Any]]):
    with get_conn() as con:
        con.executemany(
            '''
            INSERT OR REPLACE INTO ohlc
            (ts, open, high, low, close, volume, oi, instrument_token, symbol, interval)
            VALUES (:date, :open, :high, :low, :close, :volume, :oi, :instrument_token, :symbol, :interval)
            ''',
            rows
        )
        con.commit()

def load_ohlc_df(symbol: str, interval: str = "day") -> pd.DataFrame:
    with get_conn() as con:
        return pd.read_sql_query(
            "SELECT * FROM ohlc WHERE symbol=? AND interval=? ORDER BY ts",
            con,
            params=(symbol, interval,),
            parse_dates=["ts"]
        )

def replace_instruments_df(df: pd.DataFrame):
    with get_conn() as con:
        con.execute("DELETE FROM instruments")
        df.to_sql("instruments", con, if_exists="append", index=False)
        con.commit()

def load_instruments_df() -> pd.DataFrame:
    with get_conn() as con:
        return pd.read_sql_query("SELECT * FROM instruments", con)
