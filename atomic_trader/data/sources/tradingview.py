"""TradingView Technical Analysis source using tradingview_ta library."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from atomic_trader.data.base import DataEvent, DataSourceBase
from atomic_trader.data.bus import DataBus

POLL_INTERVAL = 60  # seconds — TV library polls their servers, keep reasonable


class TradingViewSource(DataSourceBase):
    """
    Uses the tradingview_ta library to fetch technical indicators.
    Derives regime signals (trend_regime, vol_regime) from:
      - Moving average recommendations (STRONG_BUY / SELL / NEUTRAL)
      - Bollinger Band width for volatility regime
      - RSI for overbought/oversold

    Runs in a thread pool (asyncio.to_thread) since tradingview_ta is sync.
    """

    CRYPTO_WATCHLIST: dict[str, tuple[str, str, str]] = {
        # symbol: (exchange, screener_symbol, screener)
        "BTCUSDT": ("BINANCE", "BTCUSDT", "crypto"),
        "ETHUSDT": ("BINANCE", "ETHUSDT", "crypto"),
        "SOLUSDT": ("BINANCE", "SOLUSDT", "crypto"),
    }

    EQUITY_WATCHLIST: dict[str, tuple[str, str, str]] = {
        "SPY":  ("AMEX",   "SPY",  "america"),
        "QQQ":  ("NASDAQ", "QQQ",  "america"),
    }

    def __init__(self, include_equity: bool = True) -> None:
        super().__init__("tradingview")
        self._watchlist = dict(self.CRYPTO_WATCHLIST)
        if include_equity:
            self._watchlist.update(self.EQUITY_WATCHLIST)

    async def connect(self) -> None:
        try:
            from tradingview_ta import TA_Handler  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                "tradingview_ta not installed — run: pip install tradingview_ta"
            ) from exc

    async def _run_internal(self, bus: DataBus) -> None:
        while True:
            tasks = [
                self._analyze_and_publish(bus, symbol, exchange, screener_sym, screener)
                for symbol, (exchange, screener_sym, screener) in self._watchlist.items()
            ]
            # Run all symbols concurrently (each in its own thread)
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(POLL_INTERVAL)

    async def _analyze_and_publish(
        self,
        bus: DataBus,
        symbol: str,
        exchange: str,
        screener_sym: str,
        screener: str,
    ) -> None:
        try:
            event = await asyncio.to_thread(
                self._analyze_sync, symbol, exchange, screener_sym, screener
            )
            if event:
                await bus.publish(event)
        except Exception as exc:
            self.logger.debug(f"TradingView {symbol}: {exc}")

    def _analyze_sync(
        self,
        symbol: str,
        exchange: str,
        screener_sym: str,
        screener: str,
    ) -> DataEvent | None:
        """Synchronous TA analysis — runs in thread pool."""
        from tradingview_ta import TA_Handler, Interval

        handler = TA_Handler(
            symbol=screener_sym,
            screener=screener,
            exchange=exchange,
            interval=Interval.INTERVAL_1_HOUR,
        )
        analysis = handler.get_analysis()
        summary = analysis.summary
        indicators: dict[str, Any] = analysis.indicators

        # ── Trend Regime ─────────────────────────────────────────────────────
        rec: str = summary.get("RECOMMENDATION", "NEUTRAL")
        if "STRONG_BUY" in rec:
            trend_regime = 2
        elif "BUY" in rec:
            trend_regime = 1
        elif "STRONG_SELL" in rec:
            trend_regime = -2
        elif "SELL" in rec:
            trend_regime = -1
        else:
            trend_regime = 0

        # ── Volatility Regime ─────────────────────────────────────────────────
        bb_upper = float(indicators.get("BB.upper") or 0)
        bb_lower = float(indicators.get("BB.lower") or 0)
        bb_mid   = float(indicators.get("BB.basis") or 1)
        bb_width = (bb_upper - bb_lower) / bb_mid if bb_mid else 0
        # 0 = low vol, 1 = medium, 2 = high vol
        if bb_width > 0.05:
            vol_regime = 2
        elif bb_width > 0.025:
            vol_regime = 1
        else:
            vol_regime = 0

        # ── Key Indicators ────────────────────────────────────────────────────
        rsi  = float(indicators.get("RSI") or 50)
        macd = float(indicators.get("MACD.macd") or 0)
        macd_signal = float(indicators.get("MACD.signal") or 0)
        close = float(indicators.get("close") or 0)
        ema_20 = float(indicators.get("EMA20") or 0)
        ema_50 = float(indicators.get("EMA50") or 0)

        return DataEvent(
            source="tradingview",
            event_type="regime",
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            data={
                # Regime signals (used directly by strategies)
                "trend_regime": trend_regime,
                "vol_regime": vol_regime,
                # Raw indicators
                "rsi": round(rsi, 2),
                "macd": round(macd, 4),
                "macd_signal": round(macd_signal, 4),
                "bb_width": round(bb_width, 4),
                "close": close,
                "ema_20": ema_20,
                "ema_50": ema_50,
                # Signal counts
                "tv_buy_signals": summary.get("BUY", 0),
                "tv_sell_signals": summary.get("SELL", 0),
                "tv_neutral_signals": summary.get("NEUTRAL", 0),
                "tv_recommendation": rec,
            },
        )

    async def disconnect(self) -> None:
        pass  # No persistent connection to close
