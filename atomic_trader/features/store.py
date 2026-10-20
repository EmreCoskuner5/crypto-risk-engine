"""In-memory feature store backed by pandas."""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

from atomic_trader.data.base import DataEvent

logger = logging.getLogger(__name__)

_NUMERIC = (int, float)
MAX_HISTORY = 500  # max events stored per (symbol, feature)


class FeatureStore:
    """
    Receives DataEvents from the DataBus and maintains rolling windows
    of feature values per symbol.

    Usage:
        store = FeatureStore()
        bus.subscribe(store.handle)

        # Later:
        latest = store.get_latest("BTCUSDT")
        obi_series = store.get_history("BTCUSDT", "obi", n=50)
    """

    def __init__(self, max_history: int = MAX_HISTORY) -> None:
        self._max_history = max_history
        self._lock = asyncio.Lock()

        # _history[symbol][feature] → deque of (timestamp, value)
        self._history: dict[str, dict[str, deque[tuple[datetime, float]]]] = (
            defaultdict(lambda: defaultdict(lambda: deque(maxlen=max_history)))
        )

        # _latest[symbol] → {feature: latest_value}
        self._latest: dict[str, dict[str, Any]] = defaultdict(dict)

        # Event counter per source (for health monitoring)
        self._event_counts: dict[str, int] = defaultdict(int)

    async def handle(self, event: DataEvent) -> None:
        """DataBus subscriber — called for every incoming event."""
        async with self._lock:
            sym = event.symbol
            ts = event.timestamp

            for key, value in event.data.items():
                if isinstance(value, _NUMERIC):
                    self._history[sym][key].append((ts, float(value)))
                    self._latest[sym][key] = value
                    self._latest[sym][f"_{key}_ts"] = ts

            # Track metadata
            self._latest[sym]["_source"] = event.source
            self._latest[sym]["_event_type"] = event.event_type
            self._latest[sym]["_updated_at"] = ts
            self._event_counts[event.source] += 1

    # ── Read API ──────────────────────────────────────────────────────────────

    def get_latest(self, symbol: str) -> dict[str, Any]:
        """Return the most recent feature snapshot for a symbol."""
        return dict(self._latest.get(symbol, {}))

    def get(self, symbol: str, feature: str, default: Any = None) -> Any:
        """Get the latest value of a single feature for a symbol."""
        return self._latest.get(symbol, {}).get(feature, default)

    def get_history(self, symbol: str, feature: str, n: int = 100) -> pd.Series:
        """Return the last N values of a feature as a time-indexed pandas Series."""
        data = list(self._history.get(symbol, {}).get(feature, []))[-n:]
        if not data:
            return pd.Series(dtype=float, name=feature)
        timestamps, values = zip(*data)
        return pd.Series(
            list(values),
            index=pd.DatetimeIndex(list(timestamps)),
            name=feature,
        )

    def get_symbols(self) -> list[str]:
        """Return all tracked symbols."""
        return list(self._latest.keys())

    def summary(self) -> pd.DataFrame:
        """
        Return a DataFrame with the latest numeric feature values per symbol.
        Useful for debugging and dashboards.
        """
        rows = []
        for sym, features in self._latest.items():
            row = {"symbol": sym}
            for k, v in features.items():
                if not k.startswith("_") and isinstance(v, _NUMERIC):
                    row[k] = v
            rows.append(row)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows).set_index("symbol")
        return df.sort_index()

    def source_health(self) -> dict[str, int]:
        """Return total events received per source."""
        return dict(self._event_counts)

    def __repr__(self) -> str:
        syms = len(self._latest)
        total = sum(self._event_counts.values())
        return f"FeatureStore(symbols={syms}, total_events={total})"

    # ── Derived Metrics ──────────────────────────────────────────────────────────

    def get_atr(self, symbol: str, period: int = 14) -> float | None:
        """
        Compute Average True Range (ATR) from high/low/close history.
        Requires high, low, close features to be available from data sources.

        ATR = SMA(TR, period) where TR = max(H-L, |H-C_prev|, |L-C_prev|)
        """
        # Try to get OHLC data from different sources
        high_series = self.get_history(symbol, "high", n=period + 5)
        low_series = self.get_history(symbol, "low", n=period + 5)
        close_series = self.get_history(symbol, "close", n=period + 5)

        # Fallback: try 24h high/low from Binance ticker
        if high_series.empty or low_series.empty or close_series.empty:
            high_24h = self.get(symbol, "high_24h")
            low_24h = self.get(symbol, "low_24h")
            price = self.get(symbol, "price")
            if high_24h and low_24h and price:
                # Approximate ATR from 24h range (not ideal but better than nothing)
                return float(high_24h - low_24h) * 0.5  # Rough approximation
            return None

        # Ensure we have enough aligned data
        min_len = min(len(high_series), len(low_series), len(close_series))
        if min_len < period + 1:
            return None

        # Align series by index
        high_vals = high_series.values[-min_len:]
        low_vals = low_series.values[-min_len:]
        close_vals = close_series.values[-min_len:]

        # Compute True Range
        prev_close = np.roll(close_vals, 1)
        prev_close[0] = close_vals[0]  # First element has no previous

        tr1 = high_vals - low_vals
        tr2 = np.abs(high_vals - prev_close)
        tr3 = np.abs(low_vals - prev_close)

        true_range = np.maximum(np.maximum(tr1, tr2), tr3)

        # ATR = SMA of True Range
        atr = pd.Series(true_range).rolling(window=period).mean().iloc[-1]

        return float(atr) if not np.isnan(atr) else None

    def get_atr_percentile(self, symbol: str, lookback: int = 100, period: int = 14) -> float | None:
        """
        Compute ATR percentile over lookback period.
        Returns 0-1 indicating where current ATR sits in historical distribution.
        """
        # Build rolling ATR series
        high_series = self.get_history(symbol, "high", n=lookback + period)
        low_series = self.get_history(symbol, "low", n=lookback + period)
        close_series = self.get_history(symbol, "close", n=lookback + period)

        if high_series.empty or low_series.empty or close_series.empty:
            return None

        min_len = min(len(high_series), len(low_series), len(close_series))
        if min_len < period + 10:
            return None

        high_vals = high_series.values[-min_len:]
        low_vals = low_series.values[-min_len:]
        close_vals = close_series.values[-min_len:]

        prev_close = np.roll(close_vals, 1)
        prev_close[0] = close_vals[0]

        tr1 = high_vals - low_vals
        tr2 = np.abs(high_vals - prev_close)
        tr3 = np.abs(low_vals - prev_close)

        true_range = np.maximum(np.maximum(tr1, tr2), tr3)
        atr_series = pd.Series(true_range).rolling(window=period).mean().dropna()

        if len(atr_series) < 20:
            return None

        current_atr = atr_series.iloc[-1]
        # Percentile rank
        percentile = (atr_series < current_atr).mean()

        return float(percentile)

    def get_volatility_target_size(
        self,
        symbol: str,
        equity: float,
        target_vol_pct: float = 0.02,  # 2% daily vol target
        period: int = 14,
    ) -> float | None:
        """
        Calculate position size targeting a specific portfolio volatility.

        Formula: position_size = (equity * target_vol) / (asset_vol * sqrt(252))

        For daily data: asset_vol = ATR / price (daily)
        """
        atr = self.get_atr(symbol, period)
        price = self.get(symbol, "price") or self.get(symbol, "close")

        if atr is None or price is None or price <= 0:
            return None

        # Daily volatility estimate from ATR
        daily_vol = atr / price

        if daily_vol <= 0:
            return None

        # Target volatility per trade (annualized target -> daily)
        daily_target_vol = target_vol_pct / np.sqrt(252)

        # Position size as fraction of equity
        position_fraction = daily_target_vol / daily_vol

        # Cap at reasonable limits (e.g., max 20% of equity per trade)
        position_fraction = min(position_fraction, 0.20)
        position_fraction = max(position_fraction, 0.001)

        position_size = equity * position_fraction / price

        return position_size
