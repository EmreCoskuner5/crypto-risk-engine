"""Binance Spot WebSocket + Futures REST data source."""
from __future__ import annotations

import asyncio
import json
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


class BinanceSource(DataSourceBase):
    """
    Streams real-time price and order book data from Binance via WebSocket.
    Polls funding rates from Binance Futures REST API hourly.

    Order Book Imbalance (OBI) = (bid_volume - ask_volume) / total_volume
      > 0  → buy pressure
      < 0  → sell pressure
    """

    def __init__(self, symbols: list[str] | None = None) -> None:
        super().__init__("binance")
        self.symbols = symbols or ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
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
        # Build combined stream URL
        streams: list[str] = []
        for sym in self.symbols:
            s = sym.lower()
            streams.append(f"{s}@ticker")          # 24h price ticker
            streams.append(f"{s}@depth20@1000ms")  # 20-level order book, 1s updates
        url = f"{BINANCE_SPOT_WS}?streams={'/'.join(streams)}"
        self._ws = await websockets.connect(
            url,
            ssl=ssl_context,
            ping_interval=20,
            ping_timeout=10,
            open_timeout=30,
        )

    async def _run_internal(self, bus: DataBus) -> None:
        # Start funding rate polling as a background task
        funding_task = asyncio.create_task(
            self._poll_funding_rates(bus), name="binance_funding_poll"
        )
        try:
            async for raw_msg in self._ws:
                msg = json.loads(raw_msg)
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
        finally:
            funding_task.cancel()

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

    # ── Funding Rate Poller ───────────────────────────────────────────────────

    async def _poll_funding_rates(self, bus: DataBus) -> None:
        """Poll Binance Futures funding rates every hour."""
        while True:
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
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()
