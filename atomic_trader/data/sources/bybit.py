"""Bybit WebSocket + REST data source."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import aiohttp
import websockets

from atomic_trader.data.base import DataEvent, DataSourceBase
from atomic_trader.data.bus import DataBus

BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
BYBIT_REST = "https://api.bybit.com"
FUNDING_POLL_INTERVAL = 3600


class BybitSource(DataSourceBase):
    """
    Streams price and order book from Bybit via WebSocket.
    Polls funding rate history via Bybit REST.
    """

    def __init__(self, symbols: list[str] | None = None) -> None:
        super().__init__("bybit")
        self.symbols = symbols or ["BTCUSDT", "ETHUSDT"]
        self._ws: Any = None
        self._session: aiohttp.ClientSession | None = None

    async def connect(self) -> None:
        import ssl
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
        # Subscribe to tickers and order books
        topics = []
        for sym in self.symbols:
            topics.append(f"tickers.{sym}")
            topics.append(f"orderbook.1.{sym}")
        await self._ws.send(json.dumps({"op": "subscribe", "args": topics}))

    async def _run_internal(self, bus: DataBus) -> None:
        funding_task = asyncio.create_task(
            self._poll_funding(bus), name="bybit_funding_poll"
        )
        try:
            async for raw_msg in self._ws:
                msg = json.loads(raw_msg)
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
        finally:
            funding_task.cancel()

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

    # ── Funding Rate Poller ───────────────────────────────────────────────────

    async def _poll_funding(self, bus: DataBus) -> None:
        """Poll the latest funding rate from Bybit REST every hour."""
        while True:
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
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()
