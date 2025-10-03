
# main.py — single run script (no CLI). Just: python3 main.py
import threading
import time
import sys
import webbrowser

from auth import run_server, ensure_token_interactive, load_access_token
from ingest import fetch_and_store_banknifty_daily
import db

BANNER = """
=== Zerodha Auth + BANKNIFTY Ingest ===
This script will:
  1) start the local auth server (http://127.0.0.1:8750)
  2) if needed, open the Zerodha login URL
  3) wait for the token to be saved at /kite/callback
  4) download ~2 years of BANKNIFTY daily OHLC into SQLite
"""

def start_server_in_background():
    t = threading.Thread(target=run_server, daemon=True)
    t.start()
    # small wait to let Flask boot up
    for _ in range(30):
        time.sleep(0.1)
    return t

def wait_for_token(timeout_sec=180):
    """Poll for access token to appear (written by /kite/callback)."""
    start = time.time()
    while time.time() - start < timeout_sec:
        tok = load_access_token()
        if tok:
            return tok
        time.sleep(1.0)
    return ""

def main():
    print(BANNER)
    # Init DB first so we fail early if something's off with permissions
    db.init()

    print("[1/4] Starting local auth server on http://127.0.0.1:8750 ...")
    start_server_in_background()

    token = load_access_token()
    if not token:
        print("[2/4] No access token found. Launching Zerodha login flow...")
        url = ensure_token_interactive(open_browser=True)
        print("If the browser didn't open automatically, open this URL:\n", url)
        print("After login, you'll be redirected to /kite/callback and the token will be saved.")

        print("[3/4] Waiting for token to be saved (up to 30 seconds)...")
        token = wait_for_token(timeout_sec=30)
        if not token:
            print("❌ Token not received. Please re-run and complete the login promptly.")
            sys.exit(1)
        print("✅ Access token saved.")

    else:
        print("✅ Access token already present.")

    print("[4/4] Ingesting ~2 years of BANKNIFTY daily OHLC into SQLite...")
    n = fetch_and_store_banknifty_daily(years=2)
    print(f"✅ Ingest complete. Rows inserted/updated: {n}")
    print("Done. You can now explore the data with db.load_ohlc_df('BANKNIFTY','day').")

if __name__ == "__main__":
    main()
