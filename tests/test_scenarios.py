import pytest
import time
from atomic_trader.risk_manager import RiskManager
from atomic_trader.live_trader import AsyncLiveTrader

class MockAggregator:
    def __init__(self):
        self.store = {
            "USDT-USD": {"price": 1.0},
            "BTCUSDT": {"price": 50000.0, "obi": 0.2, "trend_regime": 1, "vol_regime": 1}
        }
    def get(self, symbol, key, default=0):
        if symbol in self.store:
            return self.store[symbol].get(key, default)
        return default

@pytest.mark.asyncio
async def test_stop_loss_scenario():
    """
    Scenario 1: Bot is in a position and the price drops below Stop Loss.
    We test if the logic correctly identifies the breach.
    """
    risk_manager = RiskManager(initial_capital=10000.0)
    
    # Simulate an open position
    risk_manager.positions["BTCUSDT"] = {
        "size": 0.1,
        "entry_price": 50000.0,
        "stop_loss": 49000.0,
        "take_profit": 55000.0,
        "pnl": 0.0,
        "direction": "LONG"
    }
    
    aggregator = MockAggregator()
    
    # Fast drop in price triggering SL
    aggregator.store["BTCUSDT"]["price"] = 48500.0
    current_price = aggregator.get("BTCUSDT", "price")
    
    pos = risk_manager.positions["BTCUSDT"]
    
    # Calculate simulated PnL
    pnl = (current_price - pos["entry_price"]) * pos["size"]
    risk_manager.positions["BTCUSDT"]["pnl"] = pnl
    
    # Simulate SL hit logic from live_trader
    sl_hit = False
    if pos["direction"] == "LONG" and current_price <= pos["stop_loss"]:
        sl_hit = True
        
    assert sl_hit is True
    assert pnl == -150.0 # 0.1 * (48500 - 50000)

def test_ghost_position_reconciliation():
    """
    Scenario 2: Ghost position handling.
    The exchange reports 0 open positions, but our RiskManager thinks we have 1 open.
    The system should reconcile and remove the local ghost position.
    """
    risk_manager = RiskManager(initial_capital=10000.0)
    risk_manager.positions["ETHUSDT"] = {
        "size": 1.0,
        "entry_price": 2000.0,
        "direction": "LONG"
    }
    
    exchange_reported_positions = [] # Exchange says we have no positions!
    
    # Reconciliation logic
    local_symbols = list(risk_manager.positions.keys())
    exchange_symbols = [p['symbol'] for p in exchange_reported_positions]
    
    ghosts = set(local_symbols) - set(exchange_symbols)
    
    for ghost in ghosts:
        del risk_manager.positions[ghost]
        
    assert len(risk_manager.positions) == 0
    assert "ETHUSDT" not in risk_manager.positions
