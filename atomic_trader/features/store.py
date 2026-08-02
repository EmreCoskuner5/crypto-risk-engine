"""In-memory feature store backed by pandas."""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime
from typing import Any

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
