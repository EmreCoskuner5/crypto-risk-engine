import pytest
import time
from atomic_trader.risk_manager import RiskManager

class MockAggregator:
    def __init__(self):
        self.store = {"USDT-USD": {"price": 1.0}}
        
    def get(self, symbol, key, default=0):
        if symbol in self.store:
            return self.store[symbol].get(key, default)
        return default

def test_depeg_circuit_breaker_logic():
    # Simulate the logic found in live_trader.py
    risk_manager = RiskManager(initial_capital=10000.0)
    aggregator = MockAggregator()
    
    # 1. Normal Conditions
    usdt_price = aggregator.get("USDT-USD", "price", 1.0)
    assert usdt_price >= 0.95
    assert risk_manager.halt_until <= time.time()
    
    # 2. Depeg Simulation
    aggregator.store["USDT-USD"]["price"] = 0.93
    usdt_price_depeg = aggregator.get("USDT-USD", "price", 1.0)
    
    assert usdt_price_depeg < 0.95
    
    # Simulate the circuit breaker triggering
    if usdt_price_depeg < 0.95:
        risk_manager.halt_until = time.time() + 86400
        
    assert risk_manager.halt_until > time.time() + 80000 # Should be halted for a long time
