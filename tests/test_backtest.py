"""Tests for the historical backtesting engine and data loader."""
import pandas as pd
import pytest
from atomic_trader.backtest.data_loader import HistoricalDataLoader
from atomic_trader.backtest.engine import BacktestEngine, BacktestMetrics


def test_historical_data_loader_structure():
    loader = HistoricalDataLoader()
    # Generates benchmark candles if offline
    df = loader.fetch_ohlcv("BTCUSDT", timeframe="1h", months=1, use_cache=False)
    assert not df.empty
    assert "close" in df.columns
    assert "open" in df.columns
    assert "high" in df.columns
    assert "low" in df.columns
    assert "volume" in df.columns
    assert len(df) >= 500


def test_backtest_indicator_preparation():
    loader = HistoricalDataLoader()
    df = loader.fetch_ohlcv("BTCUSDT", timeframe="1h", months=1, use_cache=False)
    engine = BacktestEngine(df, symbol="BTCUSDT")
    ind_df = engine.prepare_indicators()

    assert "ema_20" in ind_df.columns
    assert "ema_50" in ind_df.columns
    assert "trend_regime" in ind_df.columns
    assert "bb_width" in ind_df.columns
    assert "vol_regime" in ind_df.columns
    assert "obi" in ind_df.columns

    # Verify indicator value domains
    assert set(ind_df["vol_regime"].unique()).issubset({0, 1, 2})
    assert set(ind_df["trend_regime"].unique()).issubset({-1, 0, 1})


def test_backtest_engine_execution_and_metrics():
    loader = HistoricalDataLoader()
    df = loader.fetch_ohlcv("BTCUSDT", timeframe="1h", months=2, use_cache=False)
    engine = BacktestEngine(df, symbol="BTCUSDT", initial_capital=10000.0)
    metrics = engine.run()

    assert isinstance(metrics, BacktestMetrics)
    assert metrics.initial_capital == 10000.0
    assert metrics.final_capital > 0
    assert 0.0 <= metrics.win_rate <= 100.0
    assert metrics.max_drawdown_pct >= 0.0
    assert isinstance(metrics.summary(), str)
    assert "ATOMIC TRADER BACKTEST PERFORMANCE" in metrics.summary()


def test_backtest_trailing_stop_protection():
    # Synthetic trending dataframe where price climbs then plummets
    dates = pd.date_range("2026-01-01", periods=100, freq="1h")
    # Upward ramp followed by sharp dump
    prices = [50000 + i * 100 for i in range(50)] + [55000 - i * 500 for i in range(50)]
    df = pd.DataFrame({
        "open": prices,
        "high": [p + 20 for p in prices],
        "low": [p - 20 for p in prices],
        "close": prices,
        "volume": [1000.0] * 100,
    }, index=dates)

    engine = BacktestEngine(df, symbol="BTCUSDT", use_trailing_stop=True)
    metrics = engine.run()
    assert isinstance(metrics, BacktestMetrics)
