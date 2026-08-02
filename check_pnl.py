import os
import time
import hmac
import hashlib
import json
import urllib.request
import urllib.parse
import ssl
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")

if not api_key or not api_secret:
    print("API keys not found in .env")
    exit(1)

_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

BASE_URL = "https://demo-fapi.binance.com"

def _sign_request(secret: str, params: dict) -> str:
    query = urllib.parse.urlencode(params)
    sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    return f"{query}&signature={sig}"

def _get(api_key: str, secret: str, path: str, params: dict = None) -> dict:
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    query = _sign_request(secret, params)
    url = f"{BASE_URL}{path}?{query}"
    req = urllib.request.Request(url, headers={"X-MBX-APIKEY": api_key})
    with urllib.request.urlopen(req, timeout=10, context=_ssl_ctx) as r:
        return json.loads(r.read().decode())

def check():
    account = _get(api_key, api_secret, "/fapi/v2/positionRisk")
    
    total_unrealized_pnl = 0.0
    open_positions = 0
    print("-" * 50)
    for p in account:
        qty = float(p["positionAmt"])
        if qty != 0:
            open_positions += 1
            unrealized_pnl = float(p["unRealizedProfit"])
            total_unrealized_pnl += unrealized_pnl
            print(f"💰 {p['symbol']} | Yön: {'LONG' if qty > 0 else 'SHORT'} | Miktar: {qty} | Giriş: ${p['entryPrice']} | Kâr/Zarar: {unrealized_pnl:.2f} USDT")
            
    if open_positions == 0:
        print("Şu anda açık pozisyonunuz bulunmuyor.")
    else:
        print("-" * 50)
        print(f"Toplam Açık Pozisyon Sayısı: {open_positions}")
        print(f"Toplam Anlık (Unrealized) PnL: {total_unrealized_pnl:.2f} USDT")

if __name__ == "__main__":
    check()
