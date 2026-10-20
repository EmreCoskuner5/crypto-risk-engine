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
import logging
from colorama import Fore, Style
from atomic_trader.risk_manager import RiskManager
from prometheus_client import start_http_server, Gauge
from dotenv import load_dotenv
from atomic_trader.core.db import get_clickhouse_client, redis_client
import datetime

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

        # Logger
        self.logger = logging.getLogger(__name__)

        # Prometheus Metrics
        self.metric_capital = Gauge('atomic_live_capital', 'Current Account Capital')
        self.metric_open_positions = Gauge('atomic_live_open_positions', 'Number of Open Positions')
        self.metric_total_pnl = Gauge('atomic_live_total_pnl', 'Total Unrealized PnL')
        self.metric_regime_score = Gauge('atomic_live_regime_score', 'Multi-factor Regime Score (-10 to +10)')
        self.metric_drawdown_pct = Gauge('atomic_live_drawdown_pct', 'Portfolio Drawdown from Peak (%)')
        self.metric_circuit_breaker_active = Gauge('atomic_live_circuit_breaker_active', 'Circuit Breaker Active (1=yes, 0=no)')
        self.metric_losing_positions = Gauge('atomic_live_losing_positions', 'Number of Losing Positions')
        self.metric_source_health = Gauge('atomic_live_source_health', 'Data Source Health (1=healthy, 0=unhealthy)', ['source'])
        self.metric_source_message_rate = Gauge('atomic_live_source_message_rate', 'Data Source Messages per Minute', ['source'])
        self.metric_source_staleness = Gauge('atomic_live_source_staleness', 'Data Source Staleness (seconds)', ['source'])

    async def _detect_regime(self):
        """Detect initial market regime from aggregator data."""
        await self._compute_multi_factor_regime()
        print(f"[REGIME] Initial Market Regime: {self.risk_manager.regime} (Score: {getattr(self, '_regime_score', 0):.2f})")

    async def _compute_multi_factor_regime(self):
        """
        Multi-factor regime detection combining:
        1. BTC Trend (from TradingView)
        2. BTC Dominance (from CoinGlass OI or market cap)
        3. Alt Season Indicator (ETH/BTC ratio, alt breadth)
        4. Market Breadth (advancing vs declining symbols)
        5. Volatility Regime (ATR percentile, BB width)
        6. Funding Rate Extremes (cross-venue spread)
        """
        store = self.aggregator.store

        # ── 1. BTC Trend (primary driver) ─────────────────────────────────────
        btc_trend = store.get("BTCUSDT", "trend_regime", default=0)  # -2 to +2

        # ── 2. BTC Dominance proxy ───────────────────────────────────────────
        # Use BTC market cap vs total crypto market cap approximation
        btc_mcap = store.get("BTC-USD", "market_cap", default=0)  # from yfinance
        eth_mcap = store.get("ETH-USD", "market_cap", default=0)
        # Approximate dominance: if BTC mcap > ETH mcap * 10, dominance is high
        btc_dominance_score = 0
        if btc_mcap > 0 and eth_mcap > 0:
            ratio = btc_mcap / max(eth_mcap, 1)
            if ratio > 15:
                btc_dominance_score = 2  # Strong BTC dominance (risk-off for alts)
            elif ratio > 10:
                btc_dominance_score = 1
            elif ratio < 8:
                btc_dominance_score = -1  # Alt season
            else:
                btc_dominance_score = 0

        # ── 3. Alt Season / ETH-BTC Ratio ─────────────────────────────────────
        eth_btc_raw = 0.0
        eth_btc_ratio = 0  # Scored: -1 (BTC dom), 0 (neutral), +1 (alt season)
        btc_price = store.get("BTCUSDT", "price", default=0)
        eth_price = store.get("ETHUSDT", "price", default=0)
        if btc_price > 0 and eth_price > 0:
            eth_btc_raw = eth_price / btc_price  # Actual ratio (~0.03-0.05)
            # Check trend of ratio over recent history
            eth_btc_history = store.get_history("ETHUSDT", "close", n=20)
            if len(eth_btc_history) >= 10:
                import pandas as pd
                # Calculate rolling ratio - align by index first
                btc_hist = store.get_history("BTCUSDT", "close", n=20)
                if len(btc_hist) >= 10:
                    # Align series by common index (timestamps)
                    common_idx = eth_btc_history.index.intersection(btc_hist.index)
                    if len(common_idx) >= 10:
                        eth_aligned = eth_btc_history.loc[common_idx]
                        btc_aligned = btc_hist.loc[common_idx]
                        ratio_series = eth_aligned / btc_aligned
                        # If ratio is trending up, alt season is building
                        slope = ratio_series.diff().rolling(5).mean().iloc[-1]
                        if pd.notna(slope):
                            if slope > 0.000001:  # Adjusted threshold for non-normalized ratio
                                eth_btc_ratio = 1  # Alt season bullish
                            elif slope < -0.000001:
                                eth_btc_ratio = -1  # BTC dominance strengthening
                            else:
                                eth_btc_ratio = 0

        # ── 4. Market Breadth (advancing vs declining) ─────────────────────────
        symbols = store.get_symbols()
        advancing = 0
        declining = 0
        for sym in symbols:
            change_pct = store.get(sym, "price_change_pct", default=0)
            if isinstance(change_pct, (int, float)):
                if change_pct > 0.5:
                    advancing += 1
                elif change_pct < -0.5:
                    declining += 1
        breadth_score = 0
        total = advancing + declining
        if total > 0:
            breadth_ratio = (advancing - declining) / total
            if breadth_ratio > 0.4:
                breadth_score = 2
            elif breadth_ratio > 0.1:
                breadth_score = 1
            elif breadth_ratio < -0.4:
                breadth_score = -2
            elif breadth_ratio < -0.1:
                breadth_score = -1

        # ── 5. Volatility Regime ───────────────────────────────────────────────
        btc_vol_regime = store.get("BTCUSDT", "vol_regime", default=1)  # 0=low, 1=med, 2=high
        vol_score = btc_vol_regime - 1  # -1 (low), 0 (med), +1 (high)
        # High vol = risk-off, Low vol = complacency (can be risky)

        # ── 6. Funding Rate Extremes ───────────────────────────────────────────
        funding_score = 0
        btc_funding = store.get("BTCUSDT", "funding_rate", default=0)
        cross_venue_spread = store.get("BTCUSDT", "cross_venue_arb_bps", default=0)
        if abs(btc_funding) > 0.0005:  # > 5 bps is high
            funding_score = -1 if btc_funding > 0 else 1  # High positive = overleveraged longs
        if cross_venue_spread > 10:  # > 10 bps spread = dislocation
            funding_score -= 1

        # ── 7. Cross-Asset Correlation (SPY vs BTC) ───────────────────────────
        spy_price = store.get("SPY", "price", default=0)
        spy_change = store.get("SPY", "price_change_pct", default=0)
        btc_change = store.get("BTCUSDT", "price_change_pct", default=0)
        correlation_score = 0
        if spy_price > 0 and btc_price > 0:
            # If both moving same direction strongly
            if spy_change > 1 and btc_change > 1:
                correlation_score = 1  # Risk-on correlated rally
            elif spy_change < -1 and btc_change < -1:
                correlation_score = -1  # Risk-off correlated sell
            elif spy_change * btc_change < -1:  # Divergence
                correlation_score = 0.5  # Mixed signals

        # ── COMPOSITE SCORE ───────────────────────────────────────────────────
        # Weights sum to ~10 for easy interpretation
        weights = {
            "btc_trend": 3.0,
            "btc_dominance": 1.5,
            "eth_btc_ratio": 1.0,
            "market_breadth": 2.0,
            "volatility": 1.0,
            "funding": 1.0,
            "correlation": 0.5,
        }

        score = (
            weights["btc_trend"] * btc_trend +
            weights["btc_dominance"] * btc_dominance_score +
            weights["eth_btc_ratio"] * eth_btc_ratio +
            weights["market_breadth"] * breadth_score +
            weights["volatility"] * vol_score +
            weights["funding"] * funding_score +
            weights["correlation"] * correlation_score
        )

        # Max possible: 3*2 + 1.5*2 + 1*1 + 2*2 + 1*1 + 1*1 + 0.5*1 = 6 + 3 + 1 + 4 + 1 + 1 + 0.5 = 16.5
        # Normalize to -10 to +10 range for thresholds
        normalized_score = score / 16.5 * 10

        # Store for logging/debugging
        self._regime_score = normalized_score
        self._regime_components = {
            "btc_trend": btc_trend,
            "btc_dominance": btc_dominance_score,
            "eth_btc_ratio": eth_btc_ratio,
            "eth_btc_raw": eth_btc_raw,
            "market_breadth": breadth_score,
            "volatility": vol_score,
            "funding": funding_score,
            "correlation": correlation_score,
            "advancing": advancing,
            "declining": declining,
            "btc_price": btc_price,
            "eth_price": eth_price,
            "btc_funding": btc_funding,
        }

        # ── REGIME CLASSIFICATION ──────────────────────────────────────────────
        # RISK_ON:  Score > 2   (clear bullish confluence)
        # MIXED:    -2 <= Score <= 2  (conflicting signals)
        # RISK_OFF: Score < -2  (clear bearish confluence)
        if normalized_score > 2.0:
            self.risk_manager.regime = "RISK_ON"
        elif normalized_score < -2.0:
            self.risk_manager.regime = "RISK_OFF"
        else:
            self.risk_manager.regime = "MIXED"

    async def start(self):
        print(f"{Fore.GREEN}[+] Starting Prometheus Metrics Server on 127.0.0.1:8000...{Style.RESET_ALL}")
        try:
            start_http_server(8000, addr="127.0.0.1")
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

        # Multi-factor regime detection
        await self._compute_multi_factor_regime()

        print(f"[REGIME] Current Market Regime: {self.risk_manager.regime} (Score: {self._regime_score:.2f})")
        # Log detailed components for debugging
        if hasattr(self, '_regime_components'):
            comp = self._regime_components
            eth_btc_raw = comp.get('eth_btc_raw', 0)
            print(f"  Components: BTC_Trend={comp['btc_trend']} | Dom={comp['btc_dominance']} | "
                  f"ETH/BTC={eth_btc_raw:.5f} (score={comp['eth_btc_ratio']}) | Breadth={comp['market_breadth']} "
                  f"({comp['advancing']}↑/{comp['declining']}↓) | Vol={comp['volatility']} | "
                  f"Funding={comp['funding']} | Corr={comp['correlation']}")

        for symbol in self.symbols_to_track:
            await self._analyze_symbol(symbol)

        # Check data source health
        await self._check_data_source_health()

        # Update Prometheus Metrics
        self.metric_capital.set(self.risk_manager.capital)
        self.metric_open_positions.set(len(self.risk_manager.positions))
        total_pnl = sum([p.get('pnl', 0) for p in self.risk_manager.positions.values()])
        self.metric_total_pnl.set(total_pnl)
        self.metric_regime_score.set(self._regime_score)

        # Drawdown metric
        equity = self.risk_manager.get_mark_to_market_capital()
        if hasattr(self, '_peak_equity') and self._peak_equity > 0:
            drawdown_pct = (self._peak_equity - equity) / self._peak_equity * 100
            self.metric_drawdown_pct.set(drawdown_pct)
        else:
            self.metric_drawdown_pct.set(0)

        # Circuit breaker active
        self.metric_circuit_breaker_active.set(1 if time.time() < self.risk_manager.halt_until else 0)

        # Losing positions count
        losing_positions = sum(1 for p in self.risk_manager.positions.values() if p.get('pnl', 0) < 0)
        self.metric_losing_positions.set(losing_positions)

        # 🚨 Stablecoin Depeg Circuit Breaker 🚨
        usdt_price = self.aggregator.store.get("USDT-USD", "price", default=1.0)
        if usdt_price < 0.95:
            print(f"{Fore.RED}[CRITICAL DEPEG] USDT Price dropped to ${usdt_price:.3f}! Haulting and Liquidating!{Style.RESET_ALL}")
            self.risk_manager.halt_until = time.time() + 86400  # Halt for 24h
            if self.risk_manager.positions:
                for symbol, pos in list(self.risk_manager.positions.items()):
                    current_price = self.aggregator.store.get(symbol, "price", 0)
                    if current_price > 0:
                        await self._close_position(symbol, pos, current_price, reason="DEPEG_LIQUIDATION")
            return

        # Proactive Circuit Breaker Check
        if self.risk_manager.positions and self.risk_manager.check_daily_loss():
            print(f"{Fore.RED}[CRITICAL] Daily 3% loss limit breached (mark-to-market). Liquidating all open positions!{Style.RESET_ALL}")
            for symbol, pos in list(self.risk_manager.positions.items()):
                current_price = self.aggregator.store.get(symbol, "price", 0)
                if current_price > 0:
                    await self._close_position(symbol, pos, current_price, reason="DAILY_LOSS")

        # ── Additional Circuit Breakers ────────────────────────────────────────
        await self._check_circuit_breakers()

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
        """Exchange'de kapanmış pozisyonları local state'den temizle.
        Also syncs position sizes and detects partial closes/increases."""
        try:
            account = await asyncio.to_thread(_get, self.api_key, self.api_secret, "/fapi/v2/positionRisk")
        except Exception as e:
            print(f"{Fore.YELLOW}[!] Reconcile failed: {e}{Style.RESET_ALL}")
            return

        live_positions = {}
        for p in account:
            qty = float(p["positionAmt"])
            if qty != 0:
                live_positions[p["symbol"]] = {
                    "qty": abs(qty),
                    "direction": "LONG" if qty > 0 else "SHORT",
                    "entry_price": float(p["entryPrice"]),
                    "unrealized_pnl": float(p["unRealizedProfit"]),
                    "leverage": int(p.get("leverage", 1)),
                    "liq_price": float(p.get("liquidationPrice", 0) or 0),
                }

        local_positions = self.risk_manager.positions

        # Check for closed positions
        for symbol in list(local_positions.keys()):
            if symbol not in live_positions:
                closed = local_positions.pop(symbol)
                realized_pnl = closed.get('pnl', 0.0)
                self.risk_manager.update_capital_after_trade(realized_pnl)
                self._log_trade(symbol, closed, realized_pnl, reason="EXCHANGE_CLOSED")
                print(f"{Fore.MAGENTA}[CLOSED] {symbol} position closed on exchange. "
                      f"Realized PnL: ${realized_pnl:.2f} | New Capital: ${self.risk_manager.capital:.2f}{Style.RESET_ALL}")
            else:
                # Position still open - check for size changes
                live = live_positions[symbol]
                local = local_positions[symbol]

                # Detect size reduction (partial close)
                if live["qty"] < local.get("size", 0) * 0.95:  # 5% tolerance
                    reduction = local["size"] - live["qty"]
                    avg_entry = local.get("entry_price", live["entry_price"])
                    if local["direction"] == "LONG":
                        partial_pnl = (live["entry_price"] - avg_entry) * reduction
                    else:
                        partial_pnl = (avg_entry - live["entry_price"]) * reduction

                    local["size"] = live["qty"]
                    local["pnl"] = live["unrealized_pnl"]  # Use exchange PnL as source of truth

                    self.risk_manager.update_capital_after_trade(partial_pnl)
                    self._log_trade(symbol, local, partial_pnl, reason="PARTIAL_CLOSE_SYNC")
                    print(f"{Fore.CYAN}[SYNC] {symbol} partial close detected: -{reduction:.4f} "
                          f"(Exchange PnL: ${live['unrealized_pnl']:.2f}){Style.RESET_ALL}")

                # Detect size increase (pyramided)
                elif live["qty"] > local.get("size", 0) * 1.05:
                    increase = live["qty"] - local["size"]
                    local["size"] = live["qty"]
                    local["adds"] = local.get("adds", 0) + 1
                    local["entry_price"] = live["entry_price"]  # Use exchange average entry
                    local["pnl"] = live["unrealized_pnl"]
                    print(f"{Fore.CYAN}[SYNC] {symbol} position increased: +{increase:.4f} "
                          f"(New avg entry: ${live['entry_price']:.2f}, Adds: {local['adds']}){Style.RESET_ALL}")

                # Sync PnL to exchange value (more accurate)
                else:
                    local["pnl"] = live["unrealized_pnl"]

        # Check for new positions on exchange not in local state
        for symbol, live in live_positions.items():
            if symbol not in local_positions:
                # New position appeared on exchange
                direction = live["direction"]
                entry_price = live["entry_price"]
                sl = entry_price * 0.98 if direction == "LONG" else entry_price * 1.02
                tp = entry_price * 1.05 if direction == "LONG" else entry_price * 0.95

                self.risk_manager.positions[symbol] = {
                    'direction': direction,
                    'size': live["qty"],
                    'entry_price': entry_price,
                    'adds': 0,
                    'pnl': live["unrealized_pnl"],
                    'order_id': 'synced-from-exchange',
                    'stop_loss': sl,
                    'take_profit': tp,
                    'initial_stop_loss': sl,
                    'initial_take_profit': tp,
                    'trailing_stop': sl,
                    'tp1_hit': False,
                    'tp2_hit': False,
                    'partial_closed': 0.0,
                    'breakeven_moved': False,
                }
                print(f"{Fore.GREEN}[SYNC] New position detected on exchange: {symbol} {direction} "
                      f"Qty={live['qty']} Entry=${entry_price:.2f}{Style.RESET_ALL}")

    async def _check_circuit_breakers(self):
        """
        Additional circuit breakers beyond daily loss limit.

        1. Portfolio drawdown from peak (e.g., 5% from peak = halt)
        2. Volatility spike (ATR > 5x normal)
        3. Funding rate extreme (> 0.1% = overleveraged)
        4. Correlation breakdown (BTC vs SPY divergence)
        5. Too many concurrent losses (3+ losing positions)
        6. Single position risk > 1.5% of equity
        """
        store = self.aggregator.store
        equity = self.risk_manager.get_mark_to_market_capital()

        # 1. Portfolio drawdown from peak
        if not hasattr(self, '_peak_equity') or equity > self._peak_equity:
            self._peak_equity = equity

        drawdown_pct = (self._peak_equity - equity) / self._peak_equity if self._peak_equity > 0 else 0
        if drawdown_pct >= 0.05:  # 5% drawdown from peak
            print(f"{Fore.RED}[CIRCUIT BREAKER] Portfolio drawdown {drawdown_pct:.1%} from peak (${self._peak_equity:.2f}) >= 5%. Halting trading!{Style.RESET_ALL}")
            self.risk_manager.halt_until = time.time() + 3600  # 1 hour
            for symbol, pos in list(self.risk_manager.positions.items()):
                current_price = store.get(symbol, "price", 0)
                if current_price > 0:
                    await self._close_position(symbol, pos, current_price, reason="DRAWDOWN_5PCT")
            return

        # 2. Volatility spike detection
        for symbol in self.symbols_to_track:
            atr = store.get_atr(symbol, period=14)
            atr_pct = store.get_atr_percentile(symbol, lookback=100, period=14)
            if atr_pct is not None and atr_pct > 0.95:  # Top 5% of volatility
                print(f"{Fore.RED}[CIRCUIT BREAKER] {symbol} ATR in 95th percentile ({atr_pct:.1%}) — Extreme volatility!{Style.RESET_ALL}")
                # Reduce position sizes or halt new entries
                if symbol in self.risk_manager.positions:
                    pos = self.risk_manager.positions[symbol]
                    current_price = store.get(symbol, "price", 0)
                    if current_price > 0:
                        await self._partial_close_position(symbol, pos, pos['size'] * 0.5, current_price, reason="VOL_SPIKE_REDUCE")

        # 3. Funding rate extremes
        for symbol in self.symbols_to_track:
            funding = store.get(symbol, "funding_rate", default=0)
            if abs(funding) > 0.001:  # > 0.1% (10 bps) = very high
                print(f"{Fore.YELLOW}[WARNING] {symbol} funding rate extreme: {funding:.4%} ({funding*10000:.1f} bps){Style.RESET_ALL}")
                # If we have a position in same direction as funding, reduce it
                if symbol in self.risk_manager.positions:
                    pos = self.risk_manager.positions[symbol]
                    direction = pos.get('direction', 'LONG')
                    # Long position with high positive funding = overleveraged longs
                    if direction == 'LONG' and funding > 0.0008:
                        current_price = store.get(symbol, "price", 0)
                        if current_price > 0:
                            print(f"{Fore.CYAN}[REDUCE] {symbol} Long position reduced due to high positive funding{Style.RESET_ALL}")
                            await self._partial_close_position(symbol, pos, pos['size'] * 0.3, current_price, reason="HIGH_FUNDING_REDUCE")

        # 4. Correlation breakdown (BTC vs SPY)
        spy_change = store.get("SPY", "price_change_pct", default=0)
        btc_change = store.get("BTCUSDT", "price_change_pct", default=0)
        if spy_change != 0 and btc_change != 0:
            # Strong divergence
            if (spy_change > 2 and btc_change < -2) or (spy_change < -2 and btc_change > 2):
                print(f"{Fore.RED}[CIRCUIT BREAKER] Correlation breakdown: SPY {spy_change:+.2f}% vs BTC {btc_change:+.2f}%{Style.RESET_ALL}")
                # Reduce all positions
                for symbol, pos in list(self.risk_manager.positions.items()):
                    current_price = store.get(symbol, "price", 0)
                    if current_price > 0:
                        await self._partial_close_position(symbol, pos, pos['size'] * 0.5, current_price, reason="CORR_BREAKDOWN")

        # 5. Too many concurrent losing positions
        losing_positions = sum(1 for p in self.risk_manager.positions.values() if p.get('pnl', 0) < 0)
        if losing_positions >= 3:
            print(f"{Fore.RED}[CIRCUIT BREAKER] {losing_positions} concurrent losing positions — reducing risk!{Style.RESET_ALL}")
            # Close worst performer
            worst_symbol = None
            worst_pnl = 0
            for symbol, pos in self.risk_manager.positions.items():
                pnl_pct = pos.get('pnl', 0) / (pos.get('entry_price', 1) * pos.get('size', 1)) if pos.get('size', 0) > 0 else 0
                if pnl_pct < worst_pnl:
                    worst_pnl = pnl_pct
                    worst_symbol = symbol
            if worst_symbol:
                pos = self.risk_manager.positions[worst_symbol]
                current_price = store.get(worst_symbol, "price", 0)
                if current_price > 0:
                    await self._close_position(worst_symbol, pos, current_price, reason="WORST_PERFORMER")

        # 6. Single position risk > 1.5% of equity
        for symbol, pos in self.risk_manager.positions.items():
            entry = pos.get('entry_price', 0)
            sl = pos.get('trailing_stop') or pos.get('stop_loss', 0)
            size = pos.get('size', 0)
            if entry > 0 and sl > 0 and size > 0:
                risk_per_unit = abs(entry - sl)
                position_risk_usd = risk_per_unit * size
                risk_pct = position_risk_usd / equity if equity > 0 else 0
                if risk_pct > 0.015:  # > 1.5%
                    print(f"{Fore.YELLOW}[WARNING] {symbol} position risk {risk_pct:.2%} > 1.5% equity limit{Style.RESET_ALL}")
                    # Reduce position
                    current_price = store.get(symbol, "price", 0)
                    if current_price > 0:
                        await self._partial_close_position(symbol, pos, pos['size'] * 0.3, current_price, reason="POSITION_RISK_LIMIT")

    async def _check_data_source_health(self):
        """Monitor data source health and update Prometheus metrics."""
        try:
            for source in self.aggregator._sources:
                if hasattr(source, 'get_health'):
                    health = source.get_health()
                    source_name = source.name

                    # Update metrics
                    self.metric_source_health.labels(source=source_name).set(
                        1 if health.get('connected', False) else 0
                    )
                    self.metric_source_message_rate.labels(source=source_name).set(
                        health.get('message_rate_per_min', 0)
                    )
                    stale = health.get('last_message_age_sec', 0)
                    self.metric_source_staleness.labels(source=source_name).set(stale)

                    # Log warnings for unhealthy sources
                    if not health.get('connected', False):
                        self.logger.warning(f"Data source {source_name} disconnected!")
                    elif stale and stale > 60:
                        self.logger.warning(f"Data source {source_name} stale: {stale:.0f}s since last message")
        except Exception as e:
            self.logger.debug(f"Health check error: {e}")

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

    async def _manage_trailing_and_partial_tp(self, symbol: str, pos: dict, current_price: float, direction: str):
        """
        Manage trailing stops and partial take profits.

        Strategy:
        - TP1 at 1.5R: Close 50% of position, move SL to breakeven
        - TP2 at 3R: Close remaining position
        - Trailing stop: After TP1, trail by 1.5x ATR (or 2% if ATR unavailable)
        """
        entry_price = pos.get('entry_price', 0)
        size = pos.get('size', 0)
        initial_sl = pos.get('initial_stop_loss', 0)
        initial_tp = pos.get('initial_take_profit', 0)

        if entry_price == 0 or size == 0 or initial_sl == 0 or initial_tp == 0:
            return

        # Calculate R-multiple
        risk_per_unit = abs(entry_price - initial_sl)
        if risk_per_unit == 0:
            return

        if direction == 'LONG':
            profit_per_unit = current_price - entry_price
        else:
            profit_per_unit = entry_price - current_price

        r_multiple = profit_per_unit / risk_per_unit if risk_per_unit > 0 else 0

        # Get ATR for trailing distance (fallback to 2%)
        atr = None
        if hasattr(self.aggregator, 'store'):
            atr = self.aggregator.store.get_atr(symbol, period=14)

        trailing_distance_pct = (atr / current_price) * 1.5 if atr and current_price > 0 else 0.02
        trailing_distance_pct = max(trailing_distance_pct, 0.01)  # Min 1%

        # ── Partial TP1 at 1.5R ──────────────────────────────────────────────
        if r_multiple >= 1.5 and not pos.get('tp1_hit', False):
            # Close 50% of position
            close_qty = size * 0.5
            print(f"{Fore.CYAN}[PARTIAL TP1] {symbol} hit {r_multiple:.1f}R — closing 50% (@ ${current_price:.2f}){Style.RESET_ALL}")
            await self._partial_close_position(symbol, pos, close_qty, current_price, reason="TP1_50%")

            # Move stop loss to breakeven
            pos['stop_loss'] = entry_price
            pos['trailing_stop'] = entry_price
            pos['breakeven_moved'] = True
            pos['tp1_hit'] = True
            print(f"{Fore.GREEN}[✓] {symbol} SL moved to breakeven: ${entry_price:.2f}{Style.RESET_ALL}")

        # ── Trailing Stop after TP1 ──────────────────────────────────────────
        if pos.get('tp1_hit', False) and not pos.get('tp2_hit', False):
            current_trailing = pos.get('trailing_stop', entry_price)

            if direction == 'LONG':
                # Trail up: new trailing = max(current_trailing, current_price - trailing_distance)
                new_trailing = max(current_trailing, current_price * (1 - trailing_distance_pct))
            else:
                # Trail down: new trailing = min(current_trailing, current_price + trailing_distance)
                new_trailing = min(current_trailing, current_price * (1 + trailing_distance_pct))

            if (direction == 'LONG' and new_trailing > current_trailing) or \
               (direction == 'SHORT' and new_trailing < current_trailing):
                pos['trailing_stop'] = new_trailing
                pos['stop_loss'] = new_trailing  # Update effective SL
                print(f"{Fore.CYAN}[TRAIL] {symbol} trailing stop updated to ${new_trailing:.2f} (R={r_multiple:.1f}){Style.RESET_ALL}")

        # ── Partial TP2 at 3R ────────────────────────────────────────────────
        if r_multiple >= 3.0 and not pos.get('tp2_hit', False) and pos.get('tp1_hit', False):
            remaining_qty = pos.get('size', 0)
            if remaining_qty > 0:
                print(f"{Fore.CYAN}[PARTIAL TP2] {symbol} hit {r_multiple:.1f}R — closing remaining (@ ${current_price:.2f}){Style.RESET_ALL}")
                await self._partial_close_position(symbol, pos, remaining_qty, current_price, reason="TP2_FULL")
                pos['tp2_hit'] = True

        # Update PnL for existing positions
        if symbol in self.risk_manager.positions:
            pos = self.risk_manager.positions[symbol]
            direction = pos.get('direction', 'LONG')
            if direction == 'LONG':
                pos['pnl'] = (current_price - pos['entry_price']) * pos['size']
            else:
                pos['pnl'] = (pos['entry_price'] - current_price) * pos['size']

            pnl_color = Fore.GREEN if pos['pnl'] >= 0 else Fore.RED
            sl_display = pos.get('stop_loss') or pos.get('trailing_stop')
            print(f"{Fore.YELLOW}[POS]{Style.RESET_ALL} {symbol}: {direction} | Entry ${pos['entry_price']:.2f} | Current ${current_price:.2f} | PnL: {pnl_color}${pos['pnl']:.2f} | SL: ${sl_display:.2f} | TP: ${pos.get('take_profit', 0):.2f}")

            # ── Trailing Stop & Partial Take Profit Logic ────────────────────
            await self._manage_trailing_and_partial_tp(symbol, pos, current_price, direction)

            # ── Software-managed SL/TP ──────────────────────────────
            sl = pos.get('trailing_stop') or pos.get('stop_loss')
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

        # Determine direction based on Trend and OBI alignment
        direction = None
        if trend_regime > 0 and obi > 0.05:
            direction = "LONG"
        elif trend_regime < 0 and obi < -0.05:
            direction = "SHORT"

        # 1. Volatility Regime check (< 2 means low or medium volatility)
        if vol_regime < 2:
            confirmations += 1

        # 2 & 3. Trend and OBI alignment check
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
                # Pass feature_store for volatility-targeted sizing
                size = self.risk_manager.calculate_position_size(
                    symbol, current_price, stop_loss, feature_store=self.aggregator.store
                )
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
            'take_profit': take_profit,
            # Trailing stop / partial TP fields
            'initial_stop_loss': stop_loss,
            'initial_take_profit': take_profit,
            'trailing_stop': stop_loss,
            'tp1_hit': False,
            'tp2_hit': False,
            'partial_closed': 0.0,    # Amount of position closed via partial TP
            'breakeven_moved': False,
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
        self._log_trade(symbol, pos, realized_pnl, reason=reason)
        print(f"{Fore.CYAN}[CAPITAL] Realized PnL: ${realized_pnl:.2f} | New Capital: ${self.risk_manager.capital:.2f}{Style.RESET_ALL}")

    async def _partial_close_position(self, symbol: str, pos: dict, close_qty: float, current_price: float, reason: str):
        """Close a portion of the position."""
        direction = pos['direction']
        close_side = "SELL" if direction == "LONG" else "BUY"
        entry_price = pos.get('entry_price', current_price)

        # Calculate PnL for the portion being closed
        if direction == 'LONG':
            partial_pnl = (current_price - entry_price) * close_qty
        else:
            partial_pnl = (entry_price - current_price) * close_qty

        try:
            order = await asyncio.to_thread(_post, self.api_key, self.api_secret, "/fapi/v1/order", {
                "symbol": symbol,
                "side": close_side,
                "type": "MARKET",
                "quantity": close_qty,
                "reduceOnly": "true",
            })
            print(f"{Fore.GREEN}[✓] Partial close: {symbol} qty={close_qty:.4f} @ ${current_price:.2f} (#{order.get('orderId','?')}){Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}[ERROR] Failed to partially close {symbol}: {e}{Style.RESET_ALL}")
            return

        # Update position size and realized PnL
        pos['size'] -= close_qty
        pos['partial_closed'] += close_qty
        pos['pnl'] = pos.get('pnl', 0) - partial_pnl  # Adjust unrealized PnL

        # Update capital with realized PnL from partial close
        self.risk_manager.update_capital_after_trade(partial_pnl)

        # Log partial close
        self._log_trade(symbol, pos, partial_pnl, reason=reason)
        print(f"{Fore.CYAN}[PARTIAL] {symbol} Realized PnL: ${partial_pnl:.2f} | Remaining Size: {pos['size']:.4f} | New Capital: ${self.risk_manager.capital:.2f}{Style.RESET_ALL}")

    def _log_trade(self, symbol, pos, realized_pnl, reason="CLOSED"):
        # Log to local CSV as backup
        import csv
        import datetime
        file_path = "trade_history.csv"
        file_exists = os.path.isfile(file_path)
        
        try:
            with open(file_path, mode='a', newline='') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(["Timestamp", "Symbol", "Direction", "Size", "Entry_Price", "PnL", "Reason"])
                    
                timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                writer.writerow([
                    timestamp_str,
                    symbol,
                    pos.get('direction', 'UNKNOWN'),
                    pos.get('size', 0),
                    pos.get('entry_price', 0),
                    round(realized_pnl, 4),
                    reason
                ])
        except Exception as e:
            print(f"{Fore.RED}[!] Failed to write to CSV: {e}{Style.RESET_ALL}")

        # Primary log to ClickHouse
        try:
            ch = get_clickhouse_client()
            row = [
                datetime.datetime.now(),
                symbol,
                pos.get('direction', 'UNKNOWN'),
                float(pos.get('size', 0)),
                float(pos.get('entry_price', 0)),
                float(realized_pnl),
                reason
            ]
            ch.insert('trading.trade_logs', [row], column_names=['timestamp', 'symbol', 'direction', 'size', 'entry_price', 'pnl', 'reason'])
            print(f"{Fore.GREEN}[+] Trade log successfully written to ClickHouse.{Style.RESET_ALL}")
            
            # Update latest PnL to Redis for fast dashboard reads
            redis_client.set(f"latest_pnl:{symbol}", realized_pnl)
        except Exception as e:
            print(f"{Fore.YELLOW}[!] Failed to write to ClickHouse/Redis: {e} (Will use CSV only){Style.RESET_ALL}")
