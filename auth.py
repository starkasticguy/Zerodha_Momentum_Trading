# auth.py — Zerodha login + token persistence (Flask callback) using official pykiteconnect v4 flow
from flask import Flask, redirect, request, jsonify
from kiteconnect import KiteConnect
from pathlib import Path
from typing import Optional

from config import API_KEY, API_SECRET, REDIRECT_URL, ACCESS_TOKEN_FILE

app = Flask(__name__)

def save_access_token(token: str):
    Path(ACCESS_TOKEN_FILE).write_text(token)

def load_access_token() -> str:
    p = Path(ACCESS_TOKEN_FILE)
    return p.read_text().strip() if p.exists() else ""

def new_kite():
    kite = KiteConnect(api_key=API_KEY)
    at = load_access_token()
    if at:
        kite.set_access_token(at)
    return kite

@app.get("/kite/login")
def kite_login():
    kite = new_kite()
    login_url = kite.login_url()
    return redirect(login_url, code=302)

@app.get("/kite/callback")
def kite_callback():
    req_token = request.args.get("request_token")
    if not req_token:
        return jsonify({"ok": False, "error": "missing request_token"}), 400
    kite = new_kite()
    # Official flow: exchange request_token -> access_token
    data = kite.generate_session(req_token, api_secret=API_SECRET)
    access_token = data["access_token"]
    save_access_token(access_token)
    return jsonify({"ok": True, "access_token_saved": True})

@app.get("/kite/status")
def kite_status():
    return jsonify({"access_token_present": bool(load_access_token())})

def run_server():
    app.run(host="127.0.0.1", port=8750, debug=True)

def ensure_token_interactive(open_browser: bool = True) -> str:
    """Return login URL and optionally open it. Token will be saved by /kite/callback."""
    kite = new_kite()
    url = kite.login_url()
    if open_browser:
        try:
            import webbrowser
            webbrowser.open(url)
        except Exception:
            pass
    return url

if __name__ == "__main__":
    run_server()
