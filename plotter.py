# plotter.py
import matplotlib.pyplot as plt
import pandas as pd

def plot_price_bbands_signals(df: pd.DataFrame, title="Price with Bollinger + Signals"):
    fig, ax = plt.subplots(figsize=(12,6))
    ax.plot(df["date"], df["close"], label="Close")
    ax.plot(df["date"], df["bb_up"], label="BB Upper")
    ax.plot(df["date"], df["bb_mid"], label="BB Mid")
    ax.plot(df["date"], df["bb_lo"], label="BB Lower")
    # Markers
    buys = df[df["buy_sig"]]
    sells = df[df["sell_sig"]]
    ax.scatter(buys["date"], buys["close"], marker="^", s=60, label="Buy", zorder=3)
    ax.scatter(sells["date"], sells["close"], marker="v", s=60, label="Sell", zorder=3)
    ax.set_title(title)
    ax.set_xlabel("Date"); ax.set_ylabel("Price")
    ax.legend()
    plt.tight_layout()
    plt.show()

def plot_equity_curves(df: pd.DataFrame, title="Equity Curves: Strategy vs Buy & Hold"):
    fig, ax = plt.subplots(figsize=(12,5))
    ax.plot(df["date"], df["strat_eq"], label="Strategy")
    ax.plot(df["date"], df["bh_eq"], label="Buy & Hold")
    ax.set_title(title)
    ax.set_xlabel("Date"); ax.set_ylabel("Equity (normalized)")
    ax.legend()
    plt.tight_layout()
    plt.show()
