"""
Run-this-in-your-IDE version
- No CLI args needed.
- Uses values from config.py
- Pipeline: (optional) ingest -> indicators/strategy -> plots
"""

from __future__ import annotations

import sys
import traceback

from config import (
    DEFAULT_INSTRUMENT_TOKEN, DEFAULT_FROM, DEFAULT_TO, DEFAULT_INTERVAL,
    RSI_WINDOW, RSI_OVERSOLD, RSI_OVERBOUGHT, BB_WINDOW, BB_STD,
)
from db import init_schema, load_ohlc
from ingest import ingest_to_db
from strategy import make_indicators, rsi_bb_signals
from plotter import plot_price_bbands_signals, plot_equity_curves

# --- Simple toggles for IDE runs ---
RUN_INGEST = True           # set False if you already ingested data
RUN_PLOT_PRICE = True
RUN_PLOT_EQUITY = True

# Optional: override defaults from config.py (leave as None to use config values)
OVERRIDE_TOKEN: int | None = None
OVERRIDE_FROM: str | None = None
OVERRIDE_TO: str | None = None
OVERRIDE_INTERVAL: str | None = None

# Optional: indicator/strategy knobs (None = use config defaults)
OVERRIDE_RSI_WINDOW: int | None = None
OVERRIDE_RSI_OVERSOLD: float | None = None
OVERRIDE_RSI_OVERBOUGHT: float | None = None
OVERRIDE_BB_WINDOW: int | None = None
OVERRIDE_BB_STD: float | None = None


def _val(v, fallback):
    return v if v is not None else fallback


def run_pipeline():
    # 1) Config resolution
    token = _val(OVERRIDE_TOKEN, DEFAULT_INSTRUMENT_TOKEN)
    from_date = _val(OVERRIDE_FROM, DEFAULT_FROM)
    to_date = _val(OVERRIDE_TO, DEFAULT_TO)
    interval = _val(OVERRIDE_INTERVAL, DEFAULT_INTERVAL)

    rsi_window = _val(OVERRIDE_RSI_WINDOW, RSI_WINDOW)
    rsi_oversold = _val(OVERRIDE_RSI_OVERSOLD, RSI_OVERSOLD)
    rsi_overbought = _val(OVERRIDE_RSI_OVERBOUGHT, RSI_OVERBOUGHT)
    bb_window = _val(OVERRIDE_BB_WINDOW, BB_WINDOW)
    bb_std = _val(OVERRIDE_BB_STD, BB_STD)

    # 2) Ensure DB schema
    init_schema()

    # 3) Ingest (optional)
    if RUN_INGEST:
        print(f"[INGEST] token={token} {from_date}→{to_date} interval={interval}")
        df_ing = ingest_to_db(token, from_date, to_date, interval)
        print(f"[INGEST] rows fetched: {len(df_ing)}")

    # 4) Load from DB
    raw = load_ohlc(token)
    if raw.empty:
        raise RuntimeError("No OHLC data in DB. Set RUN_INGEST=True or adjust dates/token.")
    print(f"[LOAD] rows: {len(raw)}  span: {raw['date'].min()} → {raw['date'].max()}")

    # 5) Indicators
    ind = make_indicators(
        raw,
        rsi_window=rsi_window,
        bb_window=bb_window,
        bb_std=bb_std,
    )

    # 6) Strategy / backtest
    out = rsi_bb_signals(
        ind,
        rsi_oversold=rsi_oversold,
        rsi_overbought=rsi_overbought,
    )

    # 7) Summary
    final_strat = float(out["strat_eq"].iloc[-1])
    final_bh = float(out["bh_eq"].iloc[-1])
    print(
        f"[RESULT] Bars={len(out)} | Strategy final eq={final_strat:.4f} | Buy&Hold final eq={final_bh:.4f}"
    )

    # 8) Plots
    if RUN_PLOT_PRICE:
        plot_price_bbands_signals(out, title="Price + Bollinger + RSI signals")
    if RUN_PLOT_EQUITY:
        plot_equity_curves(out, title="Equity Curves: Strategy vs Buy & Hold")


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        print("\n[ERROR]".ljust(10, "-"))
        print(str(e))
        traceback.print_exc()
        sys.exit(1)
