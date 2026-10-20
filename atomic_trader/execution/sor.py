import logging
from typing import Dict, Any, Callable
from .algorithms import TWAPAlgorithm, VWAPAlgorithm

logger = logging.getLogger(__name__)

class SmartOrderRouter:
    """
    Smart Order Router (SOR) for execution algorithms.
    Routes execution requests to the appropriate algorithmic strategy.
    """
    def __init__(self, exchange_client=None):
        self.exchange = exchange_client
        self.active_executions = {}

    async def execute_order(self, symbol: str, qty: float, side: str, algo: str = "TWAP", params: Dict[str, Any] = None):
        """
        Executes an order using the specified algorithm.
        """
        if params is None:
            params = {}
            
        logger.info(f"[SOR] Routing {side} order for {qty} {symbol} using {algo} algorithm")
        
        # Define the callback that actually hits the exchange API
        async def place_order_cb(sym: str, amount: float, order_side: str):
            if self.exchange:
                # In a real environment, this would call ccxt's create_market_order
                logger.debug(f"[EXCHANGE] Placing Market {order_side} for {amount} {sym}")
                # return await self.exchange.create_market_order(sym, order_side, amount)
            else:
                logger.debug(f"[DRY RUN] Placing Market {order_side} for {amount} {sym}")

        if algo.upper() == "TWAP":
            executor = TWAPAlgorithm(symbol, qty, side, params)
        elif algo.upper() == "VWAP":
            executor = VWAPAlgorithm(symbol, qty, side, params)
        else:
            logger.warning(f"[SOR] Unknown algorithm '{algo}', defaulting to Market Execution.")
            # Immediate Market Execution
            return await place_order_cb(symbol, qty, side)
            
        # Run the algo executor (in a production environment, you might run this as a background task)
        await executor.run(place_order_cb)
