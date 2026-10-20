"""Bybit WebSocket + REST data source."""
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

BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
BYBIT_REST = "https://api.bybit.com"
FUNDING_POLL_INTERVAL = 3600

# Reconnection settings
WS_RECONNECT_DELAY = 5
WS_MAX_RECONNECT_DELAY = 60
WS_RECONNECT_MULTIPLIER = 1.5


class BybitSource(DataSourceBase):
    """
    Streams price and order book from Bybit via WebSocket.
    Polls funding rate history via Bybit REST.

    Features:
    - Automatic reconnection with exponential backoff
    - Health monitoring
    """

    def __init__(self, symbols: list[str] | None = None) -> None:
        super().__init__("bybit")
        self.symbols = symbols or ["BTCUSDT", "ETHUSDT"]
        self._ws: Any = None
        self._session: aiohttp.ClientSession | None = None
        self._reconnect_delay = WS_RECONNECT_DELAY
        self._running = False
        self._message_count = 0
        self._last_message_time: float = 0

    async def connect(self) -> None:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False)
        )
        self._ws = await websockets.connect(
            BYBIT_WS,
            ssl=ssl_context,
            ping_interval=20,
            ping_timeout=10,
            open_timeout=30,
        )
        # Subscribe to tickers, order books, and klines
        topics = []
        for sym in self.symbols:
            topics.append(f"tickers.{sym}")
            topics.append(f"orderbook.1.{sym}")
            topics.append(f"kline.1.{sym}")  # 1-minute klines
        await self._ws.send(json.dumps({"op": "subscribe", "args": topics}))
        self._running = True
        self.logger.info(f"Connected to Bybit WS: {len(self.symbols)} symbols")

    async def _run_internal(self, bus: DataBus) -> None:
        funding_task = asyncio.create_task(
            self._poll_funding(bus), name="bybit_funding_poll"
        )
        health_task = asyncio.create_task(
            self._health_monitor(), name="bybit_health_monitor"
        )

        try:
            while self._running:
                try:
                    raw_msg = await self._ws.recv()
                except websockets.exceptions.ConnectionClosed as e:
                    self.logger.warning(f"Bybit WS closed: {e.code} - {e.reason}. Reconnecting...")
                    await self._reconnect()
                    continue
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger.error(f"Bybit WS recv error: {e}")
                    await self._reconnect()
                    continue

                self._message_count += 1
                self._last_message_time = time.time()

                try:
                    msg = json.loads(raw_msg)
                except json.JSONDecodeError:
                    self.logger.debug("Invalid JSON from Bybit WS")
                    continue

                # Skip subscription confirmations and heartbeats
                if msg.get("op") in ("subscribe", "ping", "pong"):
                    continue

                topic: str = msg.get("topic", "")
                payload: dict = msg.get("data", {})

                if topic.startswith("tickers."):
                    symbol = topic.split(".", 1)[1]
                    event = self._parse_ticker(symbol, payload)
                    if event:
                        await bus.publish(event)
                elif topic.startswith("orderbook."):
                    parts = topic.split(".")
                    symbol = parts[-1]
                    event = self._parse_orderbook(symbol, payload)
                    if event:
                        await bus.publish(event)
                elif topic.startswith("kline."):
                    event = self._parse_kline(symbol, payload)
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

        self.logger.info(f"Reconnecting to Bybit WS in {self._reconnect_delay:.1f}s...")
        await asyncio.sleep(self._reconnect_delay)

        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            self._ws = await websockets.connect(
                BYBIT_WS,
                ssl=ssl_context,
                ping_interval=20,
                ping_timeout=10,
                open_timeout=30,
            )
            # Resubscribe
            topics = []
            for sym in self.symbols:
                topics.append(f"tickers.{sym}")
                topics.append(f"orderbook.1.{sym}")
                topics.append(f"kline.1.{sym}")
            await self._ws.send(json.dumps({"op": "subscribe", "args": topics}))

            self._reconnect_delay = WS_RECONNECT_DELAY
            self.logger.info("Reconnected to Bybit WS successfully")
        except Exception as e:
            self.logger.error(f"Bybit reconnection failed: {e}")
            self._reconnect_delay = min(self._reconnect_delay * WS_RECONNECT_MULTIPLIER, WS_MAX_RECONNECT_DELAY)
            asyncio.create_task(self._reconnect())

    async def _health_monitor(self) -> None:
        """Monitor WebSocket health."""
        while self._running:
            await asyncio.sleep(30)
            now = time.time()

            if self._last_message_time > 0:
                stale_seconds = now - self._last_message_time
                if stale_seconds > 60:
                    self.logger.warning(f"Bybit WS stale: no messages for {stale_seconds:.0f}s")

            self.logger.debug(f"Bybit WS health: {self._message_count} msgs/min")
            self._message_count = 0

    # ── Parsers ──────────────────────────────────────────────────────────────

    def _parse_ticker(self, symbol: str, d: dict[str, Any]) -> DataEvent | None:
        def _float(val: Any, default: float = 0.0) -> float:
            try:
                return float(val) if val else default
            except (TypeError, ValueError):
                return default

        data = {}
        if "lastPrice" in d:
            data["price"] = _float(d["lastPrice"])
        if "volume24h" in d:
            data["volume_24h"] = _float(d["volume24h"])
        if "highPrice24h" in d:
            data["high_24h"] = _float(d["highPrice24h"])
        if "lowPrice24h" in d:
            data["low_24h"] = _float(d["lowPrice24h"])
        if "fundingRate" in d:
            data["funding_rate"] = _float(d["fundingRate"])
        if "openInterest" in d:
            data["open_interest"] = _float(d["openInterest"])
        if "openInterestValue" in d:
            data["open_interest_value"] = _float(d["openInterestValue"])
        if "price24hPcnt" in d:
            data["price_change_pct_24h"] = _float(d["price24hPcnt"]) * 100

        if not data:
            return None

        return DataEvent(
            source="bybit",
            event_type="price",
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            data=data,
        )

    def _parse_orderbook(self, symbol: str, d: dict[str, Any]) -> DataEvent | None:
        bids = [(float(p), float(q)) for p, q in d.get("b", [])[:10] if float(q) > 0]
        asks = [(float(p), float(q)) for p, q in d.get("a", [])[:10] if float(q) > 0]
        if not bids or not asks:
            return None

        bid_vol = sum(q for _, q in bids)
        ask_vol = sum(q for _, q in asks)
        total = bid_vol + ask_vol
        obi = (bid_vol - ask_vol) / total if total > 0 else 0.0

        return DataEvent(
            source="bybit",
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

    def _parse_kline(self, symbol: str, d: Any) -> DataEvent | None:
        """
        Parse kline/candlestick data from Bybit for OHLC features.
        Bybit kline format (v5): {"topic":"kline.1.BTCUSDT","data":[{"start":...,"end":...,"open":"...","high":"...","low":"...","close":"...","volume":"...","turnover":"...","confirm":true}]}
        """
        # Bybit v5 sends kline data as a list
        if isinstance(d, list):
            if not d:
                return None
            d = d[0]  # Take the first (most recent) kline

        if not isinstance(d, dict):
            return None

        if not d.get("confirm", False):  # Only process closed klines
            return None

        return DataEvent(
            source="bybit",
            event_type="kline",
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            data={
                "open": float(d["open"]),
                "high": float(d["high"]),
                "low": float(d["low"]),
                "close": float(d["close"]),
                "volume": float(d["volume"]),
                "turnover": float(d.get("turnover", 0)),
                "kline_closed": True,
            },
        )

    # ── Funding Rate Poller ───────────────────────────────────────────────────

    async def _poll_funding(self, bus: DataBus) -> None:
        """Poll the latest funding rate from Bybit REST every hour."""
        while self._running:
            for sym in self.symbols:
                try:
                    async with self._session.get(
                        f"{BYBIT_REST}/v5/market/funding/history",
                        params={
                            "category": "linear",
                            "symbol": sym,
                            "limit": 1,
                        },
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        if resp.status == 200:
                            res = await resp.json()
                            items = res.get("result", {}).get("list", [])
                            if items:
                                item = items[0]
                                await bus.publish(DataEvent(
                                    source="bybit",
                                    event_type="funding",
                                    symbol=sym,
                                    timestamp=datetime.now(timezone.utc),
                                    data={
                                        "funding_rate": float(item["fundingRate"]),
                                        "funding_rate_timestamp": item["fundingRateTimestamp"],
                                    },
                                ))
                except Exception as exc:
                    self.logger.debug(f"Bybit funding poll error for {sym}: {exc}")
            await asyncio.sleep(FUNDING_POLL_INTERVAL)

    async def disconnect(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()

    def get_health(self) -> dict[str, Any]:
        """Return health metrics for monitoring."""
        return {
            "connected": self._ws is not None and self._running,
            "message_rate_per_min": self._message_count,
            "last_message_age_sec": time.time() - self._last_message_time if self._last_message_time else None,
            "reconnect_delay": self._reconnect_delay,
        }