# config.py
# ---------
# DEV-ONLY: hardcode your creds here. Do NOT commit real keys.
API_KEY      = "pi2xk2iokf0u2nt3"
API_SECRET   = "ssifv93g7g6xo9lofpduwwzy4n45q3d2"
# Paste a fresh access token after login (you can automate later)
ACCESS_TOKEN = "your_access_token"

# Default instrument config (adjust as needed)
# You can use instrument_token for historical_data (preferred)
DEFAULT_INSTRUMENT_TOKEN = 260105  # e.g., NIFTY BANK spot token (adjust!)
DEFAULT_SYMBOL = "NSE:NIFTYBANK"

# DB path
DB_PATH = "market_data.db"

# Ingest defaults
DEFAULT_INTERVAL = "day"   # 'minute','5minute','15minute','60minute','day'
DEFAULT_FROM = "2024-01-01"
DEFAULT_TO   = "2025-10-01"

# Indicators / strategy defaults
BB_WINDOW = 20
BB_STD = 2.0
RSI_WINDOW = 14
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70
