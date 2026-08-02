"""Central async event bus for inter-component communication."""
from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from atomic_trader.data.base import DataEvent

logger = logging.getLogger(__name__)

Handler = Callable[[DataEvent], Awaitable[None]]


class DataBus:
    """
    Async pub/sub event bus.
    Sources publish DataEvents; subscribers (FeatureStore, strategies) receive them.
    """

    def __init__(self, maxsize: int = 50_000) -> None:
        self._queue: asyncio.Queue[DataEvent] = asyncio.Queue(maxsize=maxsize)
        self._handlers: list[Handler] = []
        self._stats: dict[str, int] = {}  # events per source

    async def publish(self, event: DataEvent) -> None:
        """Publish an event. Drops oldest if queue is full."""
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(event)
            except asyncio.QueueEmpty:
                pass
        self._stats[event.source] = self._stats.get(event.source, 0) + 1

    def subscribe(self, handler: Handler) -> None:
        """Register a coroutine function to receive all events."""
        self._handlers.append(handler)

    async def _safe_call(self, handler, event) -> None:
        try:
            await handler(event)
        except Exception as exc:
            logger.warning(f"Handler error: {exc}")

    async def dispatch(self) -> None:
        """
        Continuously pop events and fan out to all handlers.
        Must run as a background asyncio Task.
        """
        while True:
            event = await self._queue.get()
            await asyncio.gather(
                *[self._safe_call(h, event) for h in self._handlers],
                return_exceptions=True
            )
            self._queue.task_done()

    def stats(self) -> dict[str, int]:
        """Return total event counts per source."""
        return dict(self._stats)

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()
