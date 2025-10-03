# config.py — development config (replace with your values locally)
API_KEY = "pi2xk2iokf0u2nt3"
API_SECRET = "ssifv93g7g6xo9lofpduwwzy4n45q3d2"


# Must exactly match your Kite app's Redirect URL setting
REDIRECT_URL = "http://127.0.0.1:8750/kite/callback"

# SQLite DB path (created automatically)
DB_PATH = "market.db"

# Absolute path to the token store (so server & script agree)
from pathlib import Path
TOKENS_FILE = str((Path(__file__).resolve().parent / "tokens.json").resolve())
