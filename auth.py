"""
Auth module — clean, minimal, and self‑contained.

• Reads API creds from env: KITE_API_KEY, KITE_API_SECRET
• If env vars not set, falls back to hardcoded values (less secure).
• Opens browser for one‑time login per day and caches access_token
• Exposes get_kite() for the rest of the project

Usage:
    from auth import get_kite
    kite = get_kite()   # returns an authenticated KiteConnect client
"""
from __future__ import annotations

import json
import os
import threading
import webbrowser
from datetime import datetime, date
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs

from flask import Flask, request
from kiteconnect import KiteConnect


# ---------- Config ----------
REDIRECT_URL = os.getenv("KITE_REDIRECT_URL", "http://127.0.0.1:8750/kite/callback")
TOKEN_PATH = Path(os.getenv("KITE_TOKEN_PATH", str(Path.home() / ".kite/tokens.json")))

# Fallback hardcoded keys (less secure)
API_KEY_FALLBACK = "pi2xk2iokf0u2nt3"
API_SECRET_FALLBACK = "ssifv93g7g6xo9lofpduwwzy4n45q3d2"


class _TokenStore:
    """Very small JSON store for daily tokens."""
    def __init__(self, path: Path = TOKEN_PATH):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def save(self, data: dict) -> None:
        self.path.write_text(json.dumps(data, indent=2))

    def load(self) -> Optional[dict]:
        if not self.path.exists():
            return None
        try:
            return json.loads(self.path.read_text())
        except Exception:
            return None

    @staticmethod
    def is_same_day(login_time_iso: str) -> bool:
        try:
            t = datetime.fromisoformat(login_time_iso)
            return t.date() == date.today()
        except Exception:
            return False


class _KiteAuth:
    """Tiny helper that performs the documented Kite login flow."""
    def __init__(self, api_key: str, api_secret: str, redirect_url: str = REDIRECT_URL, store: Optional[_TokenStore] = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.redirect_url = redirect_url
        self.store = store or _TokenStore()
        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_session_expiry_hook(lambda: print("[auth] Session expired — please login again."))
        self._request_token: Optional[str] = None
        self._server_thread: Optional[threading.Thread] = None

    def login(self, force: bool = False) -> KiteConnect:
        cached = self.store.load()
        if not force and cached and cached.get("access_token") and cached.get("api_key") == self.api_key and self.store.is_same_day(cached.get("login_time", "")):
            self.kite.set_access_token(cached["access_token"])
            return self.kite

        # Fresh login
        login_url = self.kite.login_url()
        self._start_callback_server()
        print(f"[auth] Opening login URL. If it doesn't pop, visit:\n{login_url}")
        webbrowser.open(login_url, new=1, autoraise=True)

        # Wait for redirect to hit our local server
        self._server_thread.join(timeout=180)
        if not self._request_token:
            raise RuntimeError("Did not receive request_token — retry login.")

        data = self.kite.generate_session(self._request_token, api_secret=self.api_secret)
        access_token = data["access_token"]
        login_time = datetime.now().isoformat(timespec="seconds")
        self.store.save({
            "api_key": self.api_key,
            "access_token": access_token,
            "login_time": login_time,
        })
        self.kite.set_access_token(access_token)
        print("[auth] Login successful; token cached for today.")
        return self.kite

    # ---- internals ----
    def _start_callback_server(self):
        app = Flask(__name__)
        parent = self

        @app.get("/kite/callback")
        def cb():
            q = parse_qs(urlparse(request.url).query)
            parent._request_token = (q.get("request_token") or [None])[0]
            status = (q.get("status") or [None])[0]
            ok = bool(parent._request_token and status == "success")
            return (f"<h3>Kite login {'successful ✅' if ok else 'incomplete ⚠️'}</h3>\n<p>You can close this tab.</p>", 200 if ok else 400)

        def run():
            app.run(host="127.0.0.1", port=8750, debug=False, use_reloader=False)

        self._server_thread = threading.Thread(target=run, daemon=True)
        self._server_thread.start()


# ---------- Public entrypoint ----------

def get_kite(force_login: bool = False) -> KiteConnect:
    api_key = os.getenv("KITE_API_KEY", API_KEY_FALLBACK)
    api_secret = os.getenv("KITE_API_SECRET", API_SECRET_FALLBACK)

    auth = _KiteAuth(api_key=api_key, api_secret=api_secret, redirect_url=REDIRECT_URL)
    return auth.login(force=force_login)
