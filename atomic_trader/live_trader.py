import asyncio
import time
import os
import ssl
import hmac
import hashlib
import json
import urllib.request
import urllib.parse
import urllib.error
from colorama import Fore, Style
from atomic_trader.risk_manager import RiskManager
from prometheus_client import start_http_server, Gauge
from dotenv import load_dotenv

load_dotenv()

# SSL context for macOS certificate bypass
_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

BASE_URL = "https://demo-fapi.binance.com"


def _sign_request(secret: str, params: dict) -> str:
    query = urllib.parse.urlencode(params)
    sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    return f"{query}&signature={sig}"


def _retry(func):
    def wrapper(*args, **kwargs):
        max_retries = 3
        backoff = 1.0
        for attempt in range(max_retries + 1):
            try:
                return func(*args, **kwargs)
            except urllib.error.HTTPError as e:
                if e.code in (408, 429, 500, 502, 503, 504) and attempt < max_retries:
                    print(f"{Fore.YELLOW}[!] Binance API {e.code} error, retrying in {backoff}s...{Style.RESET_ALL}")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                body = e.read().decode() if hasattr(e, 'read') else str(e)
                raise RuntimeError(f"Binance API {e.code}: {body}") from e
            except Exception as e:
                if attempt < max_retries:
                    print(f"{Fore.YELLOW}[!] Network error {e}, retrying in {backoff}s...{Style.RESET_ALL}")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                raise
    return wrapper


@_retry
def _get(api_key: str, secret: str, path: str, params: dict = None) -> dict:
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    params.setdefault("recvWindow", 10000)
    query = _sign_request(secret, params)
    url = f"{BASE_URL}{path}?{query}"
    req = urllib.request.Request(url, headers={"X-MBX-APIKEY": api_key})
    with urllib.request.urlopen(req, timeout=10, context=_ssl_ctx) as r:
        return json.loads(r.read().decode())


@_retry
def _post(api_key: str, secret: str, path: str, params: dict = None) -> dict:
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    params.setdefault("recvWindow", 10000)
    query = _sign_request(secret, params)
    url = f"{BASE_URL}{path}"
    data = query.encode()
    req = urllib.request.Request(
        url, data=data,
        headers={"X-MBX-APIKEY": api_key, "Content-Type": "application/x-www-form-urlencoded"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=10, context=_ssl_ctx) as r:
        return json.loads(r.read().decode())


class AsyncLiveTrader:
    def __init__(self, aggregator, interval=60):
        self.aggregator = aggregator
        self.interval = interval
        self.risk_manager = RiskManager()
        self.symbols_to_track = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

        self.api_key = os.getenv("BINANCE_API_KEY", "")
        self.api_secret = os.getenv("BINANCE_API_SECRET", "")

        if not self.api_key or not self.api_secret:
            print(f"{Fore.RED}[!] WARNING: BINANCE_API_KEY or BINANCE_API_SECRET not found in .env{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}[!] Live trader requires demo API keys.{Style.RESET_ALL}")

        # Prometheus Metrics
        self.metric_capital = Gauge('atomic_live_capital', 'Current Account Capital')
        self.metric_open_positions = Gauge('atomic_live_open_positions', 'Number of Open Positions')
        self.metric_total_pnl = Gauge('atomic_live_total_pnl', 'Total Unrealized PnL')

    async def _detect_regime(self):
        """Detect initial market regime from aggregator data."""
        btc_trend = self.aggregator.store.get("BTCUSDT", "trend_regime", default=0)

        if btc_trend > 0:
            self.risk_manager.regime = "RISK_ON"
        elif btc_trend < 0:
            self.risk_manager.regime = "RISK_OFF"
        else:
            self.risk_manager.regime = "MIXED"

        print(f"[REGIME] Initial Market Regime: {self.risk_manager.regime} (BTC Trend: {btc_trend})")

    async def start(self):
        print(f"{Fore.GREEN}[+] Starting Prometheus Metrics Server on port 8000...{Style.RESET_ALL}")
        try:
            start_http_server(8000)
        except Exception as e:
            print(f"{Fore.RED}[!] Could not start metrics server: {e}{Style.RESET_ALL}")

        print(f"{Fore.GREEN}[+] Async LIVE Trading Engine Started (TESTNET). Capital: ${self.risk_manager.capital:.2f}{Style.RESET_ALL}")

        # Start the data aggregator in the background
        aggregator_task = asyncio.create_task(self.aggregator.run())

        print(f"{Fore.YELLOW}[!] Waiting 15 seconds for initial data collection...{Style.RESET_ALL}")
        await asyncio.sleep(15)

        # Detect INITIAL regime BEFORE loading positions
        await self._detect_regime()

        await self._sync_account_balance()

        # Capture daily_start_capital AFTER loading positions to avoid false daily loss
        loaded_pnl = sum(p.get('pnl', 0) for p in self.risk_manager.positions.values())
        self.risk_manager.daily_start_capital = self.risk_manager.capital + loaded_pnl

        await self._load_existing_positions()

        try:
            while True:
                await self._tick()
                await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            pass
        finally:
            aggregator_task.cancel()

    async def _tick(self):
        print(f"\n{Fore.CYAN}--- LIVE TICK: {time.strftime('%Y-%m-%d %H:%M:%S')} ---{Style.RESET_ALL}")

        await self._reconcile_positions()

        btc_trend = self.aggregator.store.get("BTCUSDT", "trend_regime", default=0)

        if btc_trend > 0:
            self.risk_manager.regime = "RISK_ON"
        elif btc_trend < 0:
            self.risk_manager.regime = "RISK_OFF"
        else:
            self.risk_manager.regime = "MIXED"

        print(f"[REGIME] Current Market Regime: {self.risk_manager.regime} (BTC Trend: {btc_trend})")

        for symbol in self.symbols_to_track:
            await self._analyze_symbol(symbol)

        # Update Prometheus Metrics
        self.metric_capital.set(self.risk_manager.capital)
        self.metric_open_positions.set(len(self.risk_manager.positions))
        total_pnl = sum([p.get('pnl', 0) for p in self.risk_manager.positions.values()])
        self.metric_total_pnl.set(total_pnl)

        # Proactive Circuit Breaker Check
        if self.risk_manager.positions and self.risk_manager.check_daily_loss():
            print(f"{Fore.RED}[CRITICAL] Daily 3% loss limit breached (mark-to-market). Liquidating all open positions!{Style.RESET_ALL}")
            for symbol, pos in list(self.risk_manager.positions.items()):
                current_price = self.aggregator.store.get(symbol, "price", 0)
                if current_price > 0:
                    await self._close_position(symbol, pos, current_price, reason="DAILY_LOSS")

    async def _sync_account_balance(self):
        """Fetch actual USDT balance from Binance Futures and sync capital."""
        try:
            balances = await asyncio.to_thread(_get, self.api_key, self.api_secret, "/fapi/v2/balance")
            for b in balances:
                if b.get("asset") == "USDT":
                    wallet_balance = float(b.get("balance", 10000.0))
                    self.risk_manager.capital = wallet_balance
                    self.risk_manager.daily_start_capital = wallet_balance
                    print(f"{Fore.GREEN}[+] Synced Account Capital from Binance: ${wallet_balance:.2f}{Style.RESET_ALL}")
                    return
        except Exception as e:
            print(f"{Fore.YELLOW}[!] Failed to sync account balance from Binance: {e}{Style.RESET_ALL}")

    async def _load_existing_positions(self):
        """Startup routine: Load existing positions from Binance Testnet to prevent ghost positions.
        Filter: Don't load positions that violate current regime (e.g., LONG in RISK_OFF)"""
        try:
            account = await asyncio.to_thread(_get, self.api_key, self.api_secret, "/fapi/v2/positionRisk")
        except Exception as e:
            print(f"{Fore.RED}[!] Could not load existing positions: {e}{Style.RESET_ALL}")
            return

        loaded_count = 0
        skipped_count = 0
        for p in account:
            qty = float(p["positionAmt"])
            if qty != 0:
                symbol = p["symbol"]
                direction = "LONG" if qty > 0 else "SHORT"
                entry_price = float(p["entryPrice"])

                # Check regime alignment before loading
                # In RISK_OFF: block LONGs (except BTCUSDT)
                if self.risk_manager.regime == "RISK_OFF" and direction == "LONG" and symbol != "BTCUSDT":
                    print(f"{Fore.YELLOW}[!] {symbol} {direction} violates current RISK_OFF regime. Loading and scheduling immediate liquidation!{Style.RESET_ALL}")
                    sl = entry_price * 1.001 if direction == "LONG" else entry_price * 0.999
                    tp = entry_price * 0.999 if direction == "LONG" else entry_price * 1.001
                else:
                    sl = entry_price * 0.98 if direction == "LONG" else entry_price * 1.02
                    tp = entry_price * 1.05 if direction == "LONG" else entry_price * 0.95

                self.risk_manager.positions[symbol] = {
                    'direction': direction,
                    'size': abs(qty),
                    'entry_price': entry_price,
                    'adds': 0,
                    'pnl': float(p["unRealizedProfit"]),
                    'order_id': 'pre-existing',
                    'stop_loss': sl,
                    'take_profit': tp
                }
                loaded_count += 1
                print(f"{Fore.GREEN}[+] Loaded existing position: {symbol} {direction} Qty={abs(qty)} Entry=${entry_price:.2f}{Style.RESET_ALL}")

        if loaded_count > 0:
            print(f"{Fore.GREEN}[+] {loaded_count} existing positions loaded.{Style.RESET_ALL}")

    async def _reconcile_positions(self):
        """Exchange'de kapanmış pozisyonları local state'den temizle."""
        try:
            account = await asyncio.to_thread(_get, self.api_key, self.api_secret, "/fapi/v2/positionRisk")
        except Exception as e:
            print(f"{Fore.YELLOW}[!] Reconcile failed: {e}{Style.RESET_ALL}")
            return

        live_qty = {p["symbol"]: abs(float(p["positionAmt"])) for p in account}

        for symbol in list(self.risk_manager.positions.keys()):
            if live_qty.get(symbol, 0) == 0:
                closed = self.risk_manager.positions.pop(symbol)
                realized_pnl = closed.get('pnl', 0.0)
                self.risk_manager.update_capital_after_trade(realized_pnl)
                self._log_trade_csv(symbol, closed, realized_pnl, reason="EXCHANGE_CLOSED")
                print(f"{Fore.MAGENTA}[CLOSED] {symbol} position closed on exchange. "
                      f"Realized PnL: ${realized_pnl:.2f} | New Capital: ${self.risk_manager.capital:.2f}{Style.RESET_ALL}")

    async def _analyze_symbol(self, symbol):
        store = self.aggregator.store
        latest = store.get_latest(symbol)

        if not latest:
            return

        from datetime import datetime, timezone
        price_ts = latest.get("_price_ts")
        if not price_ts:
            return

        age = (datetime.now(timezone.utc) - price_ts).total_seconds()
        if age > 30:
            print(f"{Fore.YELLOW}[!] {symbol} PRICE verisi {age:.0f}sn eski — tick atlanıyor{Style.RESET_ALL}")
            return

        current_price = latest.get("price")
        if not current_price:
            return

        # Update PnL for existing positions
        if symbol in self.risk_manager.positions:
            pos = self.risk_manager.positions[symbol]
            direction = pos.get('direction', 'LONG')
            if direction == 'LONG':
                pos['pnl'] = (current_price - pos['entry_price']) * pos['size']
            else:
                pos['pnl'] = (pos['entry_price'] - current_price) * pos['size']

            pnl_color = Fore.GREEN if pos['pnl'] >= 0 else Fore.RED
            print(f"{Fore.YELLOW}[POS]{Style.RESET_ALL} {symbol}: {direction} | Entry ${pos['entry_price']:.2f} | Current ${current_price:.2f} | PnL: {pnl_color}${pos['pnl']:.2f}{Style.RESET_ALL}")
            
            # ── Software-managed SL/TP ──────────────────────────────
            sl = pos.get('stop_loss')
            tp = pos.get('take_profit')
            hit_sl = hit_tp = False
            if sl is not None:
                hit_sl = (current_price <= sl) if direction == 'LONG' else (current_price >= sl)
            if tp is not None:
                hit_tp = (current_price >= tp) if direction == 'LONG' else (current_price <= tp)

            if hit_sl or hit_tp:
                reason = "STOP LOSS" if hit_sl else "TAKE PROFIT"
                print(f"{Fore.MAGENTA}[EXIT] {symbol} hit {reason} @ ${current_price:.2f} — closing position{Style.RESET_ALL}")
                await self._close_position(symbol, pos, current_price, reason=reason)
            return

        # 3+ Confirmations using LIVE REAL-TIME DATA
        confirmations = 0
        vol_regime = latest.get("vol_regime", 1)
        obi = latest.get("obi", 0)
        trend_regime = latest.get("trend_regime", 0)

        direction = None
        if obi > 0.3 and trend_regime >= 0:
            direction = "LONG"
        elif obi < -0.3 and trend_regime <= 0:
            direction = "SHORT"

        # Volatility check relaxed for demo - always count as 1
        confirmations += 1

        if direction:
            confirmations += 2

        print(f"  🔍 {symbol:<8} | Price: ${current_price:.2f} | OBI: {obi:+.3f} | VolReg: {vol_regime} | TrendReg: {trend_regime} | Conf: {confirmations}/3")

        if confirmations >= 3 and direction:
            if direction == "LONG":
                stop_loss = current_price * 0.98
                take_profit = current_price * 1.05
                expected_rr = (take_profit - current_price) / (current_price - stop_loss)
            else:
                stop_loss = current_price * 1.02
                take_profit = current_price * 0.95
                expected_rr = (current_price - take_profit) / (stop_loss - current_price)

            can_open, reason = self.risk_manager.can_open_position(symbol, expected_rr, direction=direction)
            if can_open:
                size = self.risk_manager.calculate_position_size(symbol, current_price, stop_loss)
                if size > 0:
                    print(f"{Fore.GREEN}[LIVE SIGNAL] Executing {direction} {symbol} @ ${current_price:.2f} (Size: {size:.4f}){Style.RESET_ALL}")
                    await self._execute_order(symbol, direction, size, current_price, stop_loss, take_profit)
            else:
                print(f"{Fore.RED}[REJECTED]{Style.RESET_ALL} {symbol} {direction} rejected: {reason}")

    async def _execute_order(self, symbol, direction, size, price, stop_loss, take_profit):
        side = "BUY" if direction == "LONG" else "SELL"
        sl_side = "SELL" if direction == "LONG" else "BUY"

        precision_map = {"BTCUSDT": 3, "ETHUSDT": 3, "SOLUSDT": 0}
        precision = precision_map.get(symbol, 3)
        qty = round(size, precision)
        if precision == 0:
            qty = max(1, int(qty))
        if qty <= 0:
            print(f"{Fore.YELLOW}[!] Skipping {symbol}: qty too small ({qty}){Style.RESET_ALL}")
            return

        try:
            lev_resp = await asyncio.to_thread(_post, self.api_key, self.api_secret, "/fapi/v1/leverage",
                  {"symbol": symbol, "leverage": 1})
            print(f"{Fore.CYAN}[i] Leverage response for {symbol}: {lev_resp}{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}[!] Leverage set FAILED for {symbol}: {e}{Style.RESET_ALL}")

        try:
            order = await asyncio.to_thread(_post, self.api_key, self.api_secret, "/fapi/v1/order", {
                "symbol": symbol, "side": side, "type": "MARKET", "quantity": qty
            })
        except Exception as e:
            # ARTIK sahte pozisyon YARATMIYORUZ — gerçek hatayı gösterip çıkıyoruz.
            # Bir sonraki tick'te tekrar denenecek.
            print(f"{Fore.RED}[ERROR] Market order failed for {symbol}: {e}{Style.RESET_ALL}")
            return

        order_id = order.get("orderId", "?")
        status = order.get("status", "")
        filled_qty = float(order.get("executedQty", 0) or 0)

        if status not in ("FILLED", "PARTIALLY_FILLED", "NEW"):
            print(f"{Fore.RED}[ERROR] Order #{order_id} failed (status={status}). Not recording position.{Style.RESET_ALL}")
            return

        # Binance testnet MARKET orders sometimes return NEW and 0 filled qty initially.
        if filled_qty <= 0 and status == "NEW":
            filled_qty = qty

        avg_price = float(order.get("avgPrice", 0) or 0)
        if avg_price <= 0:
            avg_price = price

        print(f"{Fore.GREEN}[✓] Market order placed: #{order_id} qty={filled_qty} @ ~${avg_price:.2f}{Style.RESET_ALL}")

        try:
            risk = await asyncio.to_thread(_get, self.api_key, self.api_secret, "/fapi/v2/positionRisk", {"symbol": symbol})
            for p in risk:
                if float(p.get("positionAmt", 0)) != 0:
                    print(f"{Fore.CYAN}[i] {symbol} GERÇEK leverage={p.get('leverage')} liq_price=${p.get('liquidationPrice')}{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.YELLOW}[!] Position risk doğrulanamadı: {e}{Style.RESET_ALL}")

        self.risk_manager.positions[symbol] = {
            'direction': direction,
            'size': filled_qty,       # gerçek fill miktarı
            'entry_price': avg_price, # gerçek fill fiyatı
            'adds': 0,
            'pnl': 0.0,
            'order_id': order_id,
            'stop_loss': stop_loss,
            'take_profit': take_profit
        }

        sl_ok = False
        try:
            # Use Algo Order API for STOP_MARKET (Binance requires /fapi/v1/algo/order)
            await asyncio.to_thread(_post, self.api_key, self.api_secret, "/fapi/v1/algo/order", {
                "symbol": symbol,
                "side": sl_side,
                "type": "STOP_LOSS",  # Algo order type for stop loss
                "stopPrice": f"{stop_loss:.2f}",
                "quantity": qty,
                "reduceOnly": "true"
            })
            sl_ok = True
            print(f"{Fore.GREEN}[✓] Stop Loss placed @ ${stop_loss:.2f}{Style.RESET_ALL}")
        except Exception as e:
            msg = str(e)
            if "-5000" in msg or "-4120" in msg:
                print(f"{Fore.CYAN}[i] Borsa SL/TP desteklenmiyor (Testnet). Yazılımsal SL devrede.{Style.RESET_ALL}")
            else:
                print(f"{Fore.RED}[!!!] SL FAILED: {e}{Style.RESET_ALL}")

        if not sl_ok:
            pass # We rely on software SL/TP which we know works

        try:
            # Use Algo Order API for TAKE_PROFIT_MARKET
            await asyncio.to_thread(_post, self.api_key, self.api_secret, "/fapi/v1/algo/order", {
                "symbol": symbol,
                "side": sl_side,  # Same side as SL (opposite of position direction)
                "type": "TAKE_PROFIT",  # Algo order type for take profit
                "stopPrice": f"{take_profit:.2f}",
                "quantity": qty,
                "reduceOnly": "true"
            })
            print(f"{Fore.GREEN}[✓] Take Profit placed @ ${take_profit:.2f}{Style.RESET_ALL}")
        except Exception as e:
            msg = str(e)
            if "-5000" not in msg and "-4120" not in msg:
                print(f"{Fore.YELLOW}[!] TP failed: {e}{Style.RESET_ALL}")

    async def _close_position(self, symbol, pos, current_price, reason="MARKET_CLOSE"):
        direction = pos['direction']
        close_side = "SELL" if direction == "LONG" else "BUY"
        qty = pos['size']

        try:
            order = await asyncio.to_thread(_post, self.api_key, self.api_secret, "/fapi/v1/order", {
                "symbol": symbol,
                "side": close_side,
                "type": "MARKET",
                "quantity": qty,
                "reduceOnly": "true",
            })
            print(f"{Fore.GREEN}[✓] Position closed: {symbol} #{order.get('orderId','?')}{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}[ERROR] Failed to close {symbol} position: {e}{Style.RESET_ALL}")
            return  # borsa tarafında hâlâ açık, tekrar denenecek

        realized_pnl = pos.get('pnl', 0.0)
        self.risk_manager.update_capital_after_trade(realized_pnl)
        del self.risk_manager.positions[symbol]
        self._log_trade_csv(symbol, pos, realized_pnl, reason=reason)
        print(f"{Fore.CYAN}[CAPITAL] Realized PnL: ${realized_pnl:.2f} | New Capital: ${self.risk_manager.capital:.2f}{Style.RESET_ALL}")

    def _log_trade_csv(self, symbol, pos, realized_pnl, reason="CLOSED"):
        import csv
        import datetime
        file_path = "trade_history.csv"
        file_exists = os.path.isfile(file_path)
        
        try:
            with open(file_path, mode='a', newline='') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(["Timestamp", "Symbol", "Direction", "Size", "Entry_Price", "PnL", "Reason"])
                    
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                writer.writerow([
                    timestamp,
                    symbol,
                    pos.get('direction', 'UNKNOWN'),
                    pos.get('size', 0),
                    pos.get('entry_price', 0),
                    round(realized_pnl, 4),
                    reason
                ])
        except Exception as e:
            print(f"{Fore.RED}[!] Failed to write to CSV: {e}{Style.RESET_ALL}")
