import asyncio
import logging
import math
from typing import Dict, Any, Callable

logger = logging.getLogger(__name__)

class ExecutionAlgorithm:
    def __init__(self, symbol: str, total_qty: float, side: str, params: Dict[str, Any]):
        self.symbol = symbol
        self.total_qty = total_qty
        self.side = side
        self.params = params
        self.executed_qty = 0.0
        
    async def run(self, place_order_cb: Callable):
        raise NotImplementedError

class TWAPAlgorithm(ExecutionAlgorithm):
    """
    Time-Weighted Average Price Algorithm.
    Splits the total order into smaller chunks over a specific time window.
    """
    async def run(self, place_order_cb: Callable):
        duration_mins = self.params.get('duration_mins', 5)
        chunks = self.params.get('chunks', 5)
        
        chunk_qty = self.total_qty / chunks
        sleep_time = (duration_mins * 60) / chunks
        
        logger.info(f"[TWAP] Starting execution for {self.symbol} {self.side}. Total: {self.total_qty}, Chunks: {chunks}, Interval: {sleep_time}s")
        
        for i in range(chunks):
            if self.executed_qty >= self.total_qty:
                break
                
            qty_to_trade = chunk_qty
            # Handle rounding/leftovers on last chunk
            if i == chunks - 1:
                qty_to_trade = self.total_qty - self.executed_qty
                
            logger.info(f"[TWAP] Chunk {i+1}/{chunks} - Executing {self.side} {qty_to_trade} {self.symbol}")
            try:
                # Call the exchange API
                await place_order_cb(self.symbol, qty_to_trade, self.side)
                self.executed_qty += qty_to_trade
            except Exception as e:
                logger.error(f"[TWAP] Error placing chunk order: {e}")
                
            if i < chunks - 1:
                await asyncio.sleep(sleep_time)
        
        logger.info(f"[TWAP] Completed for {self.symbol}. Total Executed: {self.executed_qty}")

class VWAPAlgorithm(ExecutionAlgorithm):
    """
    Volume-Weighted Average Price Algorithm.
    (Placeholder for future volume profile integration)
    """
    async def run(self, place_order_cb: Callable):
        logger.warning("[VWAP] VWAP is not fully implemented yet. Falling back to TWAP.")
        twap = TWAPAlgorithm(self.symbol, self.total_qty, self.side, self.params)
        await twap.run(place_order_cb)
