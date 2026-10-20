import pytest
from atomic_trader.risk_manager import RiskManager

def test_initialization():
    rm = RiskManager(initial_capital=10000.0)
    assert rm.capital == 10000.0
    assert rm.daily_start_capital == 10000.0
    assert rm.get_mark_to_market_capital() == 10000.0

def test_daily_drawdown_limit():
    rm = RiskManager(initial_capital=10000.0)
    
    # Simulate a floating loss of $299 (2.99% - should not halt)
    rm.positions["BTCUSDT"] = {"pnl": -299.0}
    assert rm.check_daily_loss() is False
    
    # Simulate a floating loss of $301 (3.01% - MUST HALT)
    rm.positions["BTCUSDT"] = {"pnl": -301.0}
    assert rm.check_daily_loss() is True

def test_update_capital_after_trade():
    rm = RiskManager(initial_capital=10000.0)
    
    # Profit trade
    rm.update_capital_after_trade(200.0)
    assert rm.capital == 10200.0
    assert rm.daily_realized_pnl == 200.0
    assert rm.consecutive_losses == 0
    
    # 3 consecutive losses
    rm.update_capital_after_trade(-50.0)
    rm.update_capital_after_trade(-50.0)
    rm.update_capital_after_trade(-50.0)
    
    assert rm.consecutive_losses == 3
    assert rm.halt_until > 0 # Should be paused for 1 hour

def test_can_open_position_rules():
    rm = RiskManager(initial_capital=10000.0)
    
    # Test valid opening
    can_open, msg = rm.can_open_position("ETHUSDT", 2.5)
    assert can_open is True
    
    # Test RR < 1.95 block
    can_open, msg = rm.can_open_position("ETHUSDT", 1.5)
    assert can_open is False
    assert "R:R" in msg
    
    # Test Max Positions Block
    for i in range(5):
        rm.positions[f"COIN{i}"] = {"pnl": 0}
    
    can_open, msg = rm.can_open_position("NEWCOIN", 2.0)
    assert can_open is False
    assert "Max 5 positions" in msg

def test_position_sizing_regime():
    rm = RiskManager(initial_capital=10000.0)
    # Risk 1% of 10000 = $100 per trade. 
    # Current price 50000, Stop Loss 49000 -> Risk per share = $1000
    # Size = 100 / 1000 = 0.1
    
    size_risk_on = rm.calculate_position_size("BTCUSDT", 50000, 49000)
    assert size_risk_on == 0.1
    
    # Mixed regime reduces size by 25% (0.075)
    rm.regime = "MIXED"
    size_mixed = rm.calculate_position_size("BTCUSDT", 50000, 49000)
    assert size_mixed == 0.075

def test_daily_reset_on_new_day():
    from datetime import datetime, timezone, timedelta
    rm = RiskManager(initial_capital=10000.0)
    rm.capital = 10500.0
    rm.daily_realized_pnl = 500.0
    
    # Simulate that last reset was yesterday (UTC)
    utc_today = datetime.now(timezone.utc).date()
    rm.last_reset_date = utc_today - timedelta(days=1)
    
    # Trigger check_daily_loss which invokes _maybe_reset_daily()
    halt = rm.check_daily_loss()
    assert halt is False
    assert rm.daily_start_capital == 10500.0
    assert rm.daily_realized_pnl == 0.0
    assert rm.last_reset_date == utc_today

def test_daily_reset_prevents_false_circuit_breaker():
    from datetime import datetime, timezone, timedelta
    rm = RiskManager(initial_capital=10000.0)
    
    # Yesterday suffered 3.5% loss and halted
    rm.capital = 9650.0
    rm.daily_start_capital = 10000.0
    utc_today = datetime.now(timezone.utc).date()
    rm.last_reset_date = utc_today - timedelta(days=1)
    
    # Today starts: check_daily_loss should reset baseline to 9650 instead of checking against 10000
    halt = rm.check_daily_loss()
    assert halt is False
    assert rm.daily_start_capital == 9650.0
