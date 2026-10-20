"""Tests for live and paper signal confirmation logic consistency."""
import pytest


def calculate_confirmations(latest: dict, price: float = 50000.0):
    """Mirror of the unified confirmation logic in live_trader and paper_trader."""
    confirmations = 0
    vol_regime = latest.get("vol_regime", 1)
    obi = latest.get("obi", 0)
    trend_regime = latest.get("trend_regime", 0)

    direction = None
    if trend_regime > 0 and obi > 0.05:
        direction = "LONG"
    elif trend_regime < 0 and obi < -0.05:
        direction = "SHORT"

    # 1. Volatility Regime check (< 2 means low or medium volatility)
    if vol_regime < 2:
        confirmations += 1

    # 2 & 3. Trend and OBI alignment check
    if direction:
        confirmations += 2

    return direction, confirmations


def test_volatility_regime_filtering():
    # Low volatility (0) -> allowed
    _, conf_low = calculate_confirmations({"vol_regime": 0, "trend_regime": 0, "obi": 0})
    assert conf_low == 1

    # Med volatility (1) -> allowed
    _, conf_med = calculate_confirmations({"vol_regime": 1, "trend_regime": 0, "obi": 0})
    assert conf_med == 1

    # High volatility (2) -> blocked
    _, conf_high = calculate_confirmations({"vol_regime": 2, "trend_regime": 0, "obi": 0})
    assert conf_high == 0


def test_long_signal_alignment():
    direction, conf = calculate_confirmations({
        "vol_regime": 1,
        "trend_regime": 1,
        "obi": 0.20,
    })
    assert direction == "LONG"
    assert conf == 3  # 1 from vol + 2 from direction


def test_short_signal_alignment():
    direction, conf = calculate_confirmations({
        "vol_regime": 0,
        "trend_regime": -1,
        "obi": -0.15,
    })
    assert direction == "SHORT"
    assert conf == 3  # 1 from vol + 2 from direction


def test_high_volatility_blocks_even_with_direction():
    # Strong direction but high volatility (vol_regime=2) -> total conf is only 2 (needs 3)
    direction, conf = calculate_confirmations({
        "vol_regime": 2,
        "trend_regime": 1,
        "obi": 0.30,
    })
    assert direction == "LONG"
    assert conf == 2  # Fails 3-confirmation barrier


def test_neutral_trend_blocks_direction():
    direction, conf = calculate_confirmations({
        "vol_regime": 1,
        "trend_regime": 0,
        "obi": 0.25,
    })
    assert direction is None
    assert conf == 1


def test_contradictory_trend_and_obi_blocked():
    direction, conf = calculate_confirmations({
        "vol_regime": 1,
        "trend_regime": 1,
        "obi": -0.20,
    })
    assert direction is None
    assert conf == 1
