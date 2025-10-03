# ingest.py
from datetime import datetime
import pandas as pd
from broker import get_kite
from db import init_schema, upsert_ohlc
from config import DEFAULT_INTERVAL

def fetch_historical(instrument_token: int, from_date: str, to_date: str, interval: str = DEFAULT_INTERVAL) -> pd.DataFrame:
    kite = get_kite()
    data = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_date,
        to_date=to_date,
        interval=interval,
        continuous=False,
        oi=False
    )
    df = pd.DataFrame(data)
    # Standardize columns
    df.rename(columns={"date":"date","open":"open","high":"high","low":"low","close":"close","volume":"volume"}, inplace=True)
    # Ensure datetime & just date for daily
    df["date"] = pd.to_datetime(df["date"])
    if interval == "day":
        df["date"] = df["date"].dt.tz_localize(None)
    return df[["date","open","high","low","close","volume"]]

def ingest_to_db(instrument_token: int, from_date: str, to_date: str, interval: str = DEFAULT_INTERVAL):
    init_schema()
    df = fetch_historical(instrument_token, from_date, to_date, interval)
    upsert_ohlc(instrument_token, df)
    return df
