# main.py — single-run orchestration: start server, prompt login, wait for token, ingest BANKNIFTY
import threading, time, sys
from auth import run_server, ensure_token_interactive, load_access_token
from ingest import fetch_and_store_banknifty_daily
import db

def start_server_background():
    t = threading.Thread(target=run_server, daemon=True)
    t.start()
    # small delay to let Flask bind the port
    time.sleep(0.5)
    return t

def wait_for_token(timeout_sec=300):
    start = time.time()
    while time.time() - start < timeout_sec:
        tok = load_access_token()
        if tok:
            return tok
        time.sleep(1.0)
    return ""

def main():
    print("=== Zerodha Auth + BANKNIFTY Ingest ===")
    print("1) starting local auth server (http://127.0.0.1:8750)")
    db.init()
    start_server_background()

    token = load_access_token()
    if not token:
        print("2) no access token found — opening Zerodha login...")
        url = ensure_token_interactive(open_browser=True)
        print("   if the browser didn't open, visit:\n   ", url)
        print("3) waiting for token to be saved by /kite/callback (up to 5 minutes)...")
        token = wait_for_token(timeout_sec=300)
        if not token:
            print("❌ token not received. please re-run and complete the login promptly.")
            sys.exit(1)
        print("✅ access token saved.")
    else:
        print("✅ access token already present.")

    print("4) ingesting ~2 years of BANKNIFTY daily OHLC into SQLite...")
    n = fetch_and_store_banknifty_daily(years=2)
    print(f"✅ ingest complete — rows inserted/updated: {n}")

if __name__ == '__main__':
    main()
