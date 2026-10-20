"""Binance Spot WebSocket + Futures REST data source."""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp
import websockets

from atomic_trader.data.base import DataEvent, DataSourceBase
from atomic_trader.data.bus import DataBus

# Spot WebSocket — works globally without VPN
BINANCE_SPOT_WS = "wss://stream.binance.com:9443/stream"
# Futures REST — for funding rates
BINANCE_FUTURES_REST = "https://fapi.binance.com"
# Spot REST — fallback for prices
BINANCE_SPOT_REST = "https://api.binance.com"

FUNDING_POLL_INTERVAL = 3600  # seconds (every hour is enough; rates update every 8h)

# Reconnection settings
WS_RECONNECT_DELAY = 5  # seconds
WS_MAX_RECONNECT_DELAY = 60
WS_RECONNECT_MULTIPLIER = 1.5


class BinanceSource(DataSourceBase):
    """
    Streams real-time price and order book data from Binance via WebSocket.
    Polls funding rates from Binance Futures REST API hourly.

    Order Book Imbalance (OBI) = (bid_volume - ask_volume) / total_volume
      > 0  → buy pressure
      < 0  → sell pressure

    Features:
    - Automatic reconnection with exponential backoff
    - Health monitoring (message rate, latency)
    - Graceful handling of connection drops
    """

    def __init__(self, symbols: list[str] | None = None) -> None:
        super().__init__("binance")
        self.symbols = symbols or ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        self._ws: Any = None
        self._session: aiohttp.ClientSession | None = None
        self._reconnect_delay = WS_RECONNECT_DELAY
        self._running = False
        self._message_count = 0
        self._last_message_time: float = 0
        self._last_ping_time: float = 0
        self._ping_latency_ms: float = 0

    async def connect(self) -> None:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False)
        )
        # Build combined stream URL
        streams: list[str] = []
        for sym in self.symbols:
            s = sym.lower()
            streams.append(f"{s}@ticker")          # 24h price ticker
            streams.append(f"{s}@depth20@1000ms")  # 20-level order book, 1s updates
            streams.append(f"{s}@kline_1m")        # 1-minute klines for OHLC/ATR
        url = f"{BINANCE_SPOT_WS}?streams={'/'.join(streams)}"
        self._ws = await websockets.connect(
            url,
            ssl=ssl_context,
            ping_interval=20,
            ping_timeout=10,
            open_timeout=30,
        )
        self._running = True
        self.logger.info(f"Connected to Binance WS: {len(self.symbols)} symbols")

    async def _run_internal(self, bus: DataBus) -> None:
        # Start funding rate polling as a background task
        funding_task = asyncio.create_task(
            self._poll_funding_rates(bus), name="binance_funding_poll"
        )
        # Start health monitoring task
        health_task = asyncio.create_task(
            self._health_monitor(), name="binance_health_monitor"
        )

        try:
            while self._running:
                try:
                    raw_msg = await self._ws.recv()
                except websockets.exceptions.ConnectionClosed as e:
                    self.logger.warning(f"Binance WS closed: {e.code} - {e.reason}. Reconnecting...")
                    await self._reconnect()
                    continue
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger.error(f"Binance WS recv error: {e}")
                    await self._reconnect()
                    continue

                self._message_count += 1
                self._last_message_time = time.time()

                try:
                    msg = json.loads(raw_msg)
                except json.JSONDecodeError:
                    self.logger.debug("Invalid JSON from Binance WS")
                    continue

                if "data" not in msg:
                    continue
                stream: str = msg.get("stream", "")
                payload: dict = msg["data"]

                if "@ticker" in stream:
                    await bus.publish(self._parse_ticker(payload))
                elif "@depth" in stream:
                    event = self._parse_orderbook(payload, stream)
                    if event:
                        await bus.publish(event)
                elif "@kline" in stream:
                    event = self._parse_kline(payload)
                    if event:
                        await bus.publish(event)

        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            funding_task.cancel()
            health_task.cancel()
            await asyncio.gather(funding_task, health_task, return_exceptions=True)

    async def _reconnect(self) -> None:
        """Reconnect to WebSocket with exponential backoff."""
        if not self._running:
            return

        self.logger.info(f"Reconnecting to Binance WS in {self._reconnect_delay:.1f}s...")
        await asyncio.sleep(self._reconnect_delay)

        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            streams: list[str] = []
            for sym in self.symbols:
                s = sym.lower()
                streams.append(f"{s}@ticker")
                streams.append(f"{s}@depth20@1000ms")
                streams.append(f"{s}@kline_1m")
            url = f"{BINANCE_SPOT_WS}?streams={'/'.join(streams)}"

            self._ws = await websockets.connect(
                url,
                ssl=ssl_context,
                ping_interval=20,
                ping_timeout=10,
                open_timeout=30,
            )
            self._reconnect_delay = WS_RECONNECT_DELAY  # Reset on successful reconnect
            self.logger.info("Reconnected to Binance WS successfully")
        except Exception as e:
            self.logger.error(f"Reconnection failed: {e}")
            self._reconnect_delay = min(self._reconnect_delay * WS_RECONNECT_MULTIPLIER, WS_MAX_RECONNECT_DELAY)
            # Schedule another reconnect attempt
            asyncio.create_task(self._reconnect())

    async def _health_monitor(self) -> None:
        """Monitor WebSocket health: message rate, latency, staleness."""
        import time
        while self._running:
            await asyncio.sleep(30)
            now = time.time()

            # Check message staleness
            if self._last_message_time > 0:
                stale_seconds = now - self._last_message_time
                if stale_seconds > 60:
                    self.logger.warning(f"Binance WS stale: no messages for {stale_seconds:.0f}s")

            # Log message rate (per minute)
            self.logger.debug(f"Binance WS health: {self._message_count} msgs/min, latency: {self._ping_latency_ms:.1f}ms")
            self._message_count = 0

    # ── Parsers ──────────────────────────────────────────────────────────────

    def _parse_ticker(self, d: dict[str, Any]) -> DataEvent:
        return DataEvent(
            source="binance",
            event_type="price",
            symbol=d["s"],
            timestamp=datetime.now(timezone.utc),
            data={
                "price": float(d["c"]),
                "volume_24h": float(d["v"]),
                "quote_volume_24h": float(d["q"]),
                "price_change_pct": float(d["P"]),
                "high_24h": float(d["h"]),
                "low_24h": float(d["l"]),
                "num_trades": int(d["n"]),
            },
        )

    def _parse_orderbook(self, d: dict[str, Any], stream: str) -> DataEvent | None:
        bids = [(float(p), float(q)) for p, q in d.get("bids", [])[:10]]
        asks = [(float(p), float(q)) for p, q in d.get("asks", [])[:10]]
        if not bids or not asks:
            return None

        bid_vol = sum(q for _, q in bids)
        ask_vol = sum(q for _, q in asks)
        total = bid_vol + ask_vol
        obi = (bid_vol - ask_vol) / total if total > 0 else 0.0

        # Extract symbol: "btcusdt@depth20@1000ms" → "BTCUSDT"
        symbol = stream.split("@")[0].upper()

        return DataEvent(
            source="binance",
            event_type="orderbook",
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            data={
                "obi": round(obi, 6),
                "best_bid": bids[0][0],
                "best_ask": asks[0][0],
                "spread": asks[0][0] - bids[0][0],
                "bid_volume": round(bid_vol, 4),
                "ask_volume": round(ask_vol, 4),
            },
        )

    def _parse_kline(self, d: dict[str, Any]) -> DataEvent | None:
        """
        Parse kline/candlestick data for OHLC features.
        Binance kline format: {"e":"kline","E":123456789,"s":"BTCUSDT","k":{"t":...,"o":"...","h":"...","l":"...","c":"...","v":"...","x":true,...}}
        """
        k = d.get("k", {})
        if not k.get("x", False):  # Only process closed klines
            return None

        symbol = d.get("s", "").upper()

        return DataEvent(
            source="binance",
            event_type="kline",
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            data={
                "open": float(k["o"]),
                "high": float(k["h"]),
                "low": float(k["l"]),
                "close": float(k["c"]),
                "volume": float(k["v"]),
                "trade_count": int(k["n"]),
                "kline_closed": True,
            },
        )

    # ── Funding Rate Poller ───────────────────────────────────────────────────

    async def _poll_funding_rates(self, bus: DataBus) -> None:
        """Poll Binance Futures funding rates every hour."""
        while self._running:
            for sym in self.symbols:
                try:
                    async with self._session.get(
                        f"{BINANCE_FUTURES_REST}/fapi/v1/premiumIndex",
                        params={"symbol": sym},
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        if resp.status == 200:
                            d = await resp.json()
                            mark = float(d["markPrice"])
                            index = float(d["indexPrice"])
                            await bus.publish(DataEvent(
                                source="binance",
                                event_type="funding",
                                symbol=sym,
                                timestamp=datetime.now(timezone.utc),
                                data={
                                    "funding_rate": float(d["lastFundingRate"]),
                                    "mark_price": mark,
                                    "index_price": index,
                                    "basis": mark - index,
                                    "basis_bps": (mark - index) / index * 10000 if index else 0,
                                },
                            ))
                        else:
                            self.logger.debug(f"Futures endpoint returned {resp.status} for {sym}")
                except Exception as exc:
                    # Futures may be blocked in some regions — not fatal
                    self.logger.debug(f"Funding poll failed for {sym}: {exc}")
            await asyncio.sleep(FUNDING_POLL_INTERVAL)

    async def disconnect(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()

    def get_health(self) -> dict[str, Any]:
        """Return health metrics for monitoring."""
        import time
        return {
            "connected": self._ws is not None and self._running,
            "message_rate_per_min": self._message_count,
            "last_message_age_sec": time.time() - self._last_message_time if self._last_message_time else None,
            "ping_latency_ms": self._ping_latency_ms,
            "reconnect_delay": self._reconnect_delay,
        }
