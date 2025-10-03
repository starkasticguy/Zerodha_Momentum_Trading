# ingest.py — fetch BANKNIFTY daily OHLC and store in SQLite
from datetime import datetime, timedelta
from typing import List, Dict, Any

from kiteconnect import KiteConnect

import db
from config import API_KEY
from auth import load_access_token
from instruments import download_and_store_instruments, find_banknifty_token

def fetch_and_store_banknifty_daily(years: int = 2) -> int:
    db.init()
    access_token = load_access_token()
    if not access_token:
        raise SystemExit("No access token found. Please run main.py and complete login.")

    # Refresh instruments and resolve token
    df_instr = download_and_store_instruments(access_token)
    token = find_banknifty_token(df_instr)
    if not token:
        raise SystemExit("BANKNIFTY instrument token not found in instruments dump.")

    to_dt = datetime.now()
    from_dt = to_dt - timedelta(days=365*years + 7)
    interval = "day"
    symbol = "BANKNIFTY"

    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(access_token)

    cursor = from_dt
    rows: List[Dict[str, Any]] = []
    while cursor < to_dt:
        window_end = min(cursor + timedelta(days=90), to_dt)  # chunking
        data = kite.historical_data(
            instrument_token=token,
            from_date=cursor,
            to_date=window_end,
            interval=interval,
            continuous=False,
            oi=True,
        )
        for bar in data:
            bar["instrument_token"] = token
            bar["symbol"] = symbol
            bar["interval"] = interval
            rows.append(bar)
        cursor = window_end

    if not rows:
        raise SystemExit("No historical data returned.")

    db.upsert_ohlc(rows)
    return len(rows)
