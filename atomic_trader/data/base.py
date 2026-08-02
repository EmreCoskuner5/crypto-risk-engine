"""Base classes and shared data types for the data layer."""
from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from atomic_trader.data.bus import DataBus

logger = logging.getLogger(__name__)


@dataclass
class DataEvent:
    """
    Normalized data event emitted by any data source.
    All sources convert their raw data into this common format
    before publishing to the DataBus.
    """

    source: str        # "binance" | "bybit" | "coinglass" | "tradingview" | "yfinance"
    event_type: str    # "price" | "funding" | "orderbook" | "regime" | "equity" | "open_interest"
    symbol: str        # "BTCUSDT", "ETHUSDT", "AAPL"
    timestamp: datetime
    data: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        ts = self.timestamp.strftime("%H:%M:%S")
        return f"[{self.source:12s}] {self.event_type:12s} {self.symbol:10s} @ {ts}"


class DataSourceBase(ABC):
    """
    Abstract base for all data sources.
    Implements automatic reconnection with exponential backoff.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.is_connected: bool = False
        self._retry_count: int = 0
        self._max_retries: int = 20
        self.logger = logging.getLogger(f"atomic_trader.data.{name}")

    async def run(self, bus: "DataBus") -> None:
        """
        Main entry point. Calls connect() then _run_internal(bus).
        On any failure, waits with exponential backoff and retries.
        """
        while True:
            try:
                self.logger.info(f"🔌 Connecting [{self.name}]...")
                await self.connect()
                self.is_connected = True
                self._retry_count = 0
                self.logger.info(f"✅ [{self.name}] connected")
                await self._run_internal(bus)
            except asyncio.CancelledError:
                self.logger.info(f"⏹ [{self.name}] cancelled")
                raise
            except Exception as exc:
                self.is_connected = False
                self._retry_count += 1
                wait = min(2 ** self._retry_count, 60)
                self.logger.warning(
                    f"⚠️ [{self.name}] error: {exc!r} — "
                    f"retry {self._retry_count}/{self._max_retries} in {wait}s"
                )
                if self._retry_count >= self._max_retries:
                    self.logger.error(f"❌ [{self.name}] max retries reached. Disabling source.")
                    return
                await asyncio.sleep(wait)
            finally:
                await self.disconnect()

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the data source."""
        ...

    @abstractmethod
    async def _run_internal(self, bus: "DataBus") -> None:
        """
        Publish DataEvents to bus. Called after connect() succeeds.
        Must run indefinitely until an exception is raised.
        """
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Clean up and release resources."""
        ...
