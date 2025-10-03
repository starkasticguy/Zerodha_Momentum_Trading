# auth.py — token store + Zerodha login helpers + Flask endpoints
from kiteconnect import KiteConnect, exceptions
import json, os
from typing import Optional
from config import API_KEY, API_SECRET, TOKENS_FILE

class TokenStore:
    """Simple JSON-backed token store (dev only)."""
    def __init__(self, path: str = TOKENS_FILE):
        self.path = path
        self._data = {}
        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    self._data = json.load(f)
            except Exception:
                self._data = {}

    def get(self, key: str, default=None):
        return self._data.get(key, default)

    def set(self, key: str, value):
        self._data[key] = value
        self.save()

    def save(self):
        # ensure parent dir exists
        os.makedirs(os.path.dirname(self.path), exist_ok=True) if os.path.dirname(self.path) else None
        with open(self.path, "w") as f:
            json.dump(self._data, f, indent=2)

class KiteAuth:
    """
    OOP wrapper around KiteConnect for dev workflows.

      auth = KiteAuth()
      if not auth.has_token():
          print(auth.login_url())  # open in browser -> request_token via redirect
          auth.exchange_request_token(request_token)
      kite = auth.kite()
    """
    def __init__(self, api_key: str = API_KEY, api_secret: str = API_SECRET, token_store: Optional[TokenStore] = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self._store = token_store or TokenStore()
        self._kite = KiteConnect(api_key=self.api_key)
        self._access_token = self._store.get("access_token")
        if self._access_token:
            self._kite.set_access_token(self._access_token)

    def login_url(self) -> str:
        return self._kite.login_url()

    def exchange_request_token(self, request_token: str) -> dict:
        """Exchange request_token → access_token. Saves token and updates client."""
        try:
            data = self._kite.generate_session(request_token, api_secret=self.api_secret)
        except exceptions.TokenException as e:
            raise RuntimeError(f"Token exchange failed: {e}") from e

        access_token = data.get("access_token")
        if not access_token:
            raise RuntimeError("No access_token returned by generate_session()")

        self._access_token = access_token
        self._kite.set_access_token(access_token)
        self._store.set("access_token", access_token)
        # persist some useful bits (optional)
        if "user_id" in data:
            self._store.set("user_id", data["user_id"])
        return data

    def has_token(self) -> bool:
        return bool(self._access_token)

    def kite(self) -> KiteConnect:
        if not self.has_token():
            raise RuntimeError("No access token available. Call exchange_request_token() first.")
        return self._kite

# --- Flask endpoints for web auth flow ---
from flask import Flask, redirect, request, jsonify
import webbrowser

app = Flask(__name__)

_store = TokenStore()
_auth = KiteAuth(token_store=_store)

@app.get("/kite/login")
def kite_login():
    url = _auth.login_url()
    return redirect(url, code=302)

@app.get("/kite/callback")
def kite_callback():
    req_token = request.args.get("request_token")
    if not req_token:
        return jsonify({"ok": False, "error": "missing request_token"}), 400
    try:
        data = _auth.exchange_request_token(req_token)
    except Exception as e:
        import traceback
        print("[/kite/callback] ERROR:", e)
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 400
    print("[/kite/callback] Token saved OK")
    return jsonify({"ok": True, "access_token_saved": True, "user": data.get("user", {})})

@app.get("/kite/status")
def kite_status():
    return jsonify({"access_token_present": bool(_auth.has_token())})

def run_server(host: str = "127.0.0.1", port: int = 8750):
    """
    Run Flask without the reloader/signals so it can run in a background thread.
    (This avoids: ValueError: signal only works in main thread)
    """
    app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)

def ensure_token_interactive(open_browser: bool = True) -> str:
    """Return login URL and optionally open it. Token is saved by /kite/callback."""
    url = _auth.login_url()
    if open_browser:
        try:
            webbrowser.open(url)
        except Exception:
            pass
    return url

def load_access_token() -> str:
    return _store.get("access_token") or ""
