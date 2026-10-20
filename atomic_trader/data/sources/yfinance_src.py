"""Yahoo Finance data source for US equity and crypto spot prices."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from atomic_trader.data.base import DataEvent, DataSourceBase
from atomic_trader.data.bus import DataBus

POLL_INTERVAL = 60  # 1 minute (yfinance free tier is 15m delayed, but still useful)


class YFinanceSource(DataSourceBase):
    """
    Polls Yahoo Finance for US equity OHLCV and crypto spot prices.
    Uses yfinance which is already in requirements.txt.

    Runs in thread pool (asyncio.to_thread) since yfinance is synchronous.
    """

    DEFAULT_SYMBOLS = [
        "SPY",     # S&P 500 ETF — macro regime
        "QQQ",     # Nasdaq ETF
        "BTC-USD", # BTC spot cross-check
        "ETH-USD", # ETH spot
        "USDT-USD", # Stablecoin Depeg monitor
        "AAPL",    # Example equity
    ]

    def __init__(self, symbols: list[str] | None = None) -> None:
        super().__init__("yfinance")
        self.symbols = symbols or self.DEFAULT_SYMBOLS

    async def connect(self) -> None:
        try:
            import yfinance as yf  # noqa: F401
            import logging
            logging.getLogger("yfinance").setLevel(logging.CRITICAL)
        except ImportError as exc:
            raise RuntimeError(
                "yfinance not installed — run: pip install yfinance"
            ) from exc

    async def _run_internal(self, bus: DataBus) -> None:
        while True:
            events = await asyncio.to_thread(self._fetch_all)
            for event in events:
                await bus.publish(event)
            await asyncio.sleep(POLL_INTERVAL)

    def _fetch_all(self) -> list[DataEvent]:
        """Synchronous bulk fetch — called in thread pool."""
        import yfinance as yf

        events: list[DataEvent] = []
        try:
            # Batch download is more efficient than individual calls
            tickers = yf.Tickers(" ".join(self.symbols))
            for sym in self.symbols:
                try:
                    events.append(self._fetch_single(sym, tickers))
                except Exception as exc:
                    self.logger.debug(f"yfinance {sym} error: {exc}")
        except Exception as exc:
            self.logger.warning(f"yfinance batch error: {exc}")
        return [e for e in events if e is not None]

    def _fetch_single(self, sym: str, tickers: Any) -> DataEvent | None:
        """Fetch and parse data for a single symbol."""
        t = tickers.tickers.get(sym)
        if t is None:
            return None

        info = t.fast_info
        history = t.history(period="2d", interval="1h")
        if history.empty:
            return None

        last_row = history.iloc[-1]
        prev_close = history.iloc[-2]["Close"] if len(history) > 1 else last_row["Close"]
        change_pct = ((last_row["Close"] - prev_close) / prev_close * 100) if prev_close else 0

        # Detect if crypto (yfinance crypto symbols end with -USD or -USDT)
        is_crypto = "-USD" in sym or "-USDT" in sym
        event_type = "price" if is_crypto else "equity"

        return DataEvent(
            source="yfinance",
            event_type=event_type,
            symbol=sym,
            timestamp=datetime.now(timezone.utc),
            data={
                "price": float(last_row["Close"]),
                "open": float(last_row["Open"]),
                "high": float(last_row["High"]),
                "low": float(last_row["Low"]),
                "volume": float(last_row["Volume"]),
                "price_change_pct": round(change_pct, 4),
                "52w_high": float(getattr(info, "year_high", 0) or 0),
                "52w_low": float(getattr(info, "year_low", 0) or 0),
                "market_cap": float(getattr(info, "market_cap", 0) or 0),
            },
        )

    async def disconnect(self) -> None:
        pass  # No persistent connection
