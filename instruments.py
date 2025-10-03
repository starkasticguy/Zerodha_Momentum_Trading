"""
Instrument utilities — resolve the instrument token for NIFTY BANK (index).

Usage:
    from auth import get_kite
    from instruments import get_nifty_bank_token

    kite = get_kite()
    token = get_nifty_bank_token(kite)
"""
from __future__ import annotations

from kiteconnect import KiteConnect


NSE_SYMBOLS = ("NSE:NIFTY BANK", "NSE:NIFTYBANK")


def get_nifty_bank_token(kite: KiteConnect) -> int:
    """Resolve the NIFTY BANK index instrument token.
    Tries fast LTP path first, then falls back to scanning instruments.
    """
    # 1) Fast path: LTP endpoint returns instrument_token field
    for s in NSE_SYMBOLS:
        try:
            data = kite.ltp([s])
            if s in data and "instrument_token" in data[s]:
                return int(data[s]["instrument_token"])
        except Exception:
            pass

    # 2) Fallback: scan the instruments catalogue
    try:
        for inst in kite.instruments():
            ts = inst.get("tradingsymbol")
            itype = (inst.get("instrument_type") or "").upper()
            exch = inst.get("exchange")
            if itype in ("INDEX", "INDICES") and exch == "NSE" and ts in ("NIFTY BANK", "NIFTYBANK"):
                return int(inst["instrument_token"])
    except Exception:
        pass

    raise RuntimeError("Could not resolve NIFTY BANK instrument token on NSE.")
