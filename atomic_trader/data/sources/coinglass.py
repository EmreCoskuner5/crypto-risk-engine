"""CoinGlass REST data source — cross-venue funding rate aggregator."""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Any

import aiohttp

from atomic_trader.data.base import DataEvent, DataSourceBase
from atomic_trader.data.bus import DataBus

COINGLASS_API = "https://open-api.coinglass.com/public/v2"
POLL_INTERVAL = 15 * 60  # 15 minutes


class CoinglassSource(DataSourceBase):
    """
    Polls CoinGlass for cross-venue funding rates and open interest.
    Requires COINGLASS_API_KEY in environment.
    If no key is set, this source is silently skipped.

    Free tier: https://www.coinglass.com/pricing
    Sign up → API Keys → Copy key → add to .env as COINGLASS_API_KEY=xxx
    """

    def __init__(self, symbols: list[str] | None = None) -> None:
        super().__init__("coinglass")
        self.symbols = symbols or ["BTC", "ETH", "SOL"]
        self.api_key: str = os.getenv("COINGLASS_API_KEY", "")
        self._session: aiohttp.ClientSession | None = None
        self._enabled: bool = bool(self.api_key)

    async def connect(self) -> None:
        if not self._enabled:
            self.logger.warning(
                "⚠️  No COINGLASS_API_KEY in .env — CoinGlass disabled. "
                "Get a free key at https://www.coinglass.com/pricing"
            )
            return
        self._session = aiohttp.ClientSession(
            headers={"coinglassSecret": self.api_key},
            connector=aiohttp.TCPConnector(ssl=False),
        )

    async def _run_internal(self, bus: DataBus) -> None:
        if not self._enabled:
            # Silently do nothing; don't block the aggregator
            await asyncio.sleep(float("inf"))
            return

        while True:
            await self._fetch_funding(bus)
            await self._fetch_open_interest(bus)
            await asyncio.sleep(POLL_INTERVAL)

    # ── Fetchers ─────────────────────────────────────────────────────────────

    async def _fetch_funding(self, bus: DataBus) -> None:
        """Fetch cross-venue funding rates for all symbols."""
        try:
            async with self._session.get(
                f"{COINGLASS_API}/funding",
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    self.logger.warning(f"CoinGlass /funding returned HTTP {resp.status}")
                    return
                result = await resp.json()
                for item in result.get("data", []):
                    symbol: str = item.get("symbol", "")
                    if symbol not in self.symbols:
                        continue
                    await self._emit_funding_event(bus, symbol, item)
        except Exception as exc:
            self.logger.warning(f"CoinGlass funding fetch error: {exc}")

    async def _emit_funding_event(
        self, bus: DataBus, symbol: str, item: dict[str, Any]
    ) -> None:
        """Parse cross-venue funding data and publish to bus."""
        exchange_rates: dict[str, float] = {}
        for ex in item.get("uMarginList", []):
            rate = ex.get("rate")
            if rate is not None:
                exchange_rates[ex["exchangeName"]] = float(rate)

        if len(exchange_rates) < 2:
            return  # Not enough venues to compute spread

        rates = list(exchange_rates.values())
        max_rate = max(rates)
        min_rate = min(rates)
        spread = max_rate - min_rate

        # Find which exchanges have max/min rates
        max_venue = max(exchange_rates, key=exchange_rates.get)
        min_venue = min(exchange_rates, key=exchange_rates.get)

        await bus.publish(DataEvent(
            source="coinglass",
            event_type="funding",
            symbol=f"{symbol}USDT",
            timestamp=datetime.now(timezone.utc),
            data={
                "exchange_rates": exchange_rates,
                "max_rate": max_rate,
                "min_rate": min_rate,
                "spread": spread,
                "cross_venue_arb_bps": spread * 10_000,
                "max_rate_venue": max_venue,
                "min_rate_venue": min_venue,
                "venue_count": len(exchange_rates),
            },
        ))

    async def _fetch_open_interest(self, bus: DataBus) -> None:
        """Fetch open interest data for all symbols."""
        try:
            async with self._session.get(
                f"{COINGLASS_API}/open_interest",
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    return
                result = await resp.json()
                for item in result.get("data", []):
                    symbol = item.get("symbol", "")
                    if symbol not in self.symbols:
                        continue
                    await bus.publish(DataEvent(
                        source="coinglass",
                        event_type="open_interest",
                        symbol=f"{symbol}USDT",
                        timestamp=datetime.now(timezone.utc),
                        data={
                            "total_oi": float(item.get("openInterest", 0) or 0),
                            "oi_change_24h": float(item.get("openInterestChange24h", 0) or 0),
                            "oi_change_pct_24h": float(item.get("openInterestChange24hPercent", 0) or 0),
                        },
                    ))
        except Exception as exc:
            self.logger.warning(f"CoinGlass open interest error: {exc}")

    async def disconnect(self) -> None:
        if self._session:
            await self._session.close()
