# instruments.py — cache instruments and resolve BANKNIFTY token
import pandas as pd
import re
from typing import Optional
from kiteconnect import KiteConnect

import db
from config import API_KEY

def download_and_store_instruments(access_token: str) -> pd.DataFrame:
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(access_token)
    data = kite.instruments()
    df = pd.DataFrame(data)
    db.replace_instruments_df(df)
    return df

def _norm(s: str) -> str:
    return re.sub(r"\W+", "", (s or "").upper())

def find_banknifty_token(df: Optional[pd.DataFrame] = None) -> Optional[int]:
    if df is None:
        df = db.load_instruments_df()
    if df is None or df.empty:
        return None
    df = df.copy()
    df["n_tradingsymbol"] = df["tradingsymbol"].astype(str).map(_norm)
    df["n_name"] = df["name"].astype(str).map(_norm)
    is_index = df["instrument_type"].astype(str).str.upper().eq("INDICES")
    candidates = ["NIFTYBANK", "BANKNIFTY", "NSE:NIFTYBANK", "NSE:BANKNIFTY"]
    mask = is_index & (
        df["n_tradingsymbol"].isin([_norm(c) for c in candidates]) |
        (df["n_name"].str.contains("NIFT") & df["n_name"].str.contains("BANK"))
    )
    m = df.loc[mask]
    if not m.empty:
        return int(m.iloc[0]["instrument_token"])
    mask2 = df["n_tradingsymbol"].str.contains("BANK") & df["n_tradingsymbol"].str.contains("NIFT")
    if mask2.any():
        return int(df.loc[mask2].iloc[0]["instrument_token"])
    return None
