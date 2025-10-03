# config.py — development config (replace with your values locally)
API_KEY = "pi2xk2iokf0u2nt3"
API_SECRET = "ssifv93g7g6xo9lofpduwwzy4n45q3d2"

# Must match your Kite app's redirect URL
REDIRECT_URL = "http://127.0.0.1:8750/kite/callback"

# Where to persist the access token (plaintext, dev-only)
from pathlib import Path
ACCESS_TOKEN_FILE = str((Path(__file__).resolve().parent / ".access_token").resolve())

# SQLite DB
DB_PATH = "market.db"