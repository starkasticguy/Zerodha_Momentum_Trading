# broker.py
from kiteconnect import KiteConnect
from config import API_KEY, ACCESS_TOKEN

def get_kite() -> KiteConnect:
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)
    return kite
