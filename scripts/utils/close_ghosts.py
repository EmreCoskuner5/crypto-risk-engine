import os
import json
import urllib.request
import urllib.parse
import hmac
import hashlib
import ssl
from dotenv import load_dotenv
from colorama import Fore, Style, init

init(autoreset=True)
load_dotenv()

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
BASE_URL = "https://demo-fapi.binance.com"

_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

def _sign_request(secret: str, params: dict) -> str:
    query = urllib.parse.urlencode(params)
    sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    return f"{query}&signature={sig}"

def _post(endpoint: str, params: dict) -> dict:
    import time
    params["timestamp"] = int(time.time() * 1000)
    query_string = _sign_request(API_SECRET, params)
    url = f"{BASE_URL}{endpoint}?{query_string}"
    
    req = urllib.request.Request(url, method="POST")
    req.add_header("X-MBX-APIKEY", API_KEY)
    
    try:
        with urllib.request.urlopen(req, context=_ssl_ctx) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"Error {e.code}: {body}")
        raise e

def _get(endpoint: str) -> dict:
    import time
    params = {"timestamp": int(time.time() * 1000)}
    query_string = _sign_request(API_SECRET, params)
    url = f"{BASE_URL}{endpoint}?{query_string}"
    
    req = urllib.request.Request(url, method="GET")
    req.add_header("X-MBX-APIKEY", API_KEY)
    
    with urllib.request.urlopen(req, context=_ssl_ctx) as response:
        return json.loads(response.read().decode())

def close_all():
    print(f"{Fore.CYAN}Borsadaki tüm açık pozisyonlar aranıyor...{Style.RESET_ALL}")
    account = _get("/fapi/v2/positionRisk")
    closed_count = 0
    for p in account:
        amt = float(p["positionAmt"])
        if amt != 0:
            sym = p["symbol"]
            side = "BUY" if amt < 0 else "SELL"
            qty = abs(amt)
            print(f"Kapatılıyor: {sym} (Yön: {side}, Miktar: {qty})")
            try:
                res = _post("/fapi/v1/order", {
                    "symbol": sym,
                    "side": side,
                    "type": "MARKET",
                    "quantity": qty,
                    "reduceOnly": "true"
                })
                print(f"{Fore.GREEN}[✓] Başarıyla kapatıldı! Emir ID: {res.get('orderId')}{Style.RESET_ALL}")
                closed_count += 1
            except Exception as e:
                print(f"{Fore.RED}[X] Kapatma başarısız: {e}{Style.RESET_ALL}")
                
    if closed_count == 0:
        print("Açık pozisyon bulunamadı.")
    else:
        print(f"{Fore.GREEN}Toplam {closed_count} pozisyon başarıyla kapatıldı!{Style.RESET_ALL}")

if __name__ == "__main__":
    close_all()
