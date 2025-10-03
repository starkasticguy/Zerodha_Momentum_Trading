# strategy.py
import pandas as pd
from indicators import rsi, bollinger_bands

def make_indicators(df: pd.DataFrame, rsi_window=14, bb_window=20, bb_std=2.0) -> pd.DataFrame:
    out = df.copy()
    out["rsi"] = rsi(out["close"], window=rsi_window)
    ma, u, l = bollinger_bands(out["close"], window=bb_window, num_std=bb_std)
    out["bb_mid"] = ma
    out["bb_up"] = u
    out["bb_lo"] = l
    return out

def rsi_bb_signals(df: pd.DataFrame, rsi_oversold=30, rsi_overbought=70) -> pd.DataFrame:
    out = df.copy()

    # Signals
    out["buy_sig"]  = (out["close"] <= out["bb_lo"]) & (out["rsi"] < rsi_oversold)
    out["sell_sig"] = (out["close"] >= out["bb_up"]) & (out["rsi"] > rsi_overbought)

    # Position logic (long/flat)
    pos = 0
    positions = []
    for buy, sell in zip(out["buy_sig"].fillna(False), out["sell_sig"].fillna(False)):
        if sell:
            pos = 0
        elif buy:
            pos = 1
        positions.append(pos)
    out["position"] = positions

    # Returns
    out["ret"] = out["close"].pct_change().fillna(0.0)
    out["strat_ret"] = out["position"].shift(1).fillna(0.0) * out["ret"]
    out["bh_ret"] = out["ret"]

    out["strat_eq"] = (1 + out["strat_ret"]).cumprod()
    out["bh_eq"]    = (1 + out["bh_ret"]).cumprod()
    return out
