"""DataAggregator — orchestrates all data sources concurrently."""
from __future__ import annotations

import asyncio
import logging
import signal
from datetime import datetime, timezone

from atomic_trader.data.base import DataSourceBase
from atomic_trader.data.bus import DataBus
from atomic_trader.features.store import FeatureStore

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 30  # seconds


class DataAggregator:
    """
    Runs all registered data sources concurrently via asyncio.gather.
    All events flow through a shared DataBus into a shared FeatureStore.

    Usage:
        agg = DataAggregator()
        agg.add_source(BinanceSource())
        agg.add_source(BybitSource())
        # ...
        asyncio.run(agg.run())

    Access live data:
        store = agg.store
        latest = store.get_latest("BTCUSDT")
    """

    def __init__(self) -> None:
        self.bus: DataBus = DataBus()
        self.store: FeatureStore = FeatureStore()
        self._sources: list[DataSourceBase] = []

        # Wire FeatureStore as a bus subscriber
        self.bus.subscribe(self.store.handle)

    def add_source(self, source: DataSourceBase) -> "DataAggregator":
        """Register a data source. Returns self for chaining."""
        self._sources.append(source)
        return self

    async def run(self) -> None:
        """
        Start all sources, the bus dispatcher, and a heartbeat logger.
        Runs until cancelled (Ctrl+C) or all sources permanently fail.
        """
        if not self._sources:
            raise ValueError("No data sources registered. Call add_source() first.")

        logger.info(
            f"🚀 DataAggregator starting with {len(self._sources)} source(s): "
            f"{[s.name for s in self._sources]}"
        )

        tasks = [
            asyncio.create_task(source.run(self.bus), name=f"src:{source.name}")
            for source in self._sources
        ]
        tasks.append(asyncio.create_task(self.bus.dispatch(), name="bus:dispatcher"))
        tasks.append(asyncio.create_task(self._heartbeat(), name="sys:heartbeat"))

        try:
            await asyncio.gather(*tasks)
        except (asyncio.CancelledError, KeyboardInterrupt):
            logger.info("⏹ DataAggregator shutting down...")
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for source in self._sources:
                if hasattr(source, "disconnect"):
                    try:
                        await source.disconnect()
                    except Exception as e:
                        logger.debug(f"Error disconnecting {source.name}: {e}")
            logger.info("✅ DataAggregator stopped cleanly.")

    async def _heartbeat(self) -> None:
        """Log system status every HEARTBEAT_INTERVAL seconds."""
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            bus_stats = self.bus.stats()
            health = self.store.source_health()
            symbols = self.store.get_symbols()

            source_status = " | ".join(
                f"{'🟢' if s.is_connected else '🔴'} {s.name}"
                for s in self._sources
            )
            event_summary = " | ".join(
                f"{src}:{count}" for src, count in bus_stats.items()
            )

            logger.info(
                f"💓 Heartbeat — {source_status}\n"
                f"   Events: {event_summary}\n"
                f"   Symbols tracked: {len(symbols)} — {symbols[:5]}{'...' if len(symbols) > 5 else ''}\n"
                f"   Queue size: {self.bus.queue_size}"
            )


def build_default_aggregator(
    crypto_symbols: list[str] | None = None,
    equity_symbols: list[str] | None = None,
) -> DataAggregator:
    """
    Factory function: creates a fully wired DataAggregator with all sources.
    CoinGlass is included but disabled automatically if COINGLASS_API_KEY is missing.
    """
    from atomic_trader.data.sources import (
        BinanceSource,
        BybitSource,
        CoinglassSource,
        TradingViewSource,
        YFinanceSource,
    )

    crypto = crypto_symbols or ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    equity = equity_symbols or ["SPY", "QQQ", "BTC-USD", "ETH-USD"]

    agg = DataAggregator()
    agg.add_source(BinanceSource(symbols=crypto))
    agg.add_source(BybitSource(symbols=[s for s in crypto if s in ["BTCUSDT", "ETHUSDT"]]))
    agg.add_source(CoinglassSource(symbols=[s.replace("USDT", "") for s in crypto]))
    agg.add_source(TradingViewSource(include_equity=True))
    agg.add_source(YFinanceSource(symbols=equity))
    return agg
