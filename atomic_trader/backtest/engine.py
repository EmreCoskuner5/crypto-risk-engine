"""
Institutional Backtesting Engine for Atomic Trader.
Simulates live signal confirmation (Trend, Volatility, OBI/Momentum) with 
RiskManager integration (position sizing, 3% daily drawdown circuit breaker, consecutive loss pauses).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from colorama import Fore, Style

from atomic_trader.risk_manager import RiskManager

logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    symbol: str
    direction: str
    entry_price: float
    exit_price: float
    size: float
    pnl: float
    pnl_pct: float
    reason: str
    fee_cost: float


@dataclass
class BacktestMetrics:
    initial_capital: float
    final_capital: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    profit_factor: float
    total_pnl: float
    total_return_pct: float
    max_drawdown_pct: float
    sharpe_ratio: float
    avg_win: float
    avg_loss: float
    payoff_ratio: float
    circuit_breaker_halts: int

    def summary(self) -> str:
        pnl_color = Fore.GREEN if self.total_pnl >= 0 else Fore.RED
        return f"""
{Fore.CYAN}╔══════════════════════════════════════════════════════════════════╗
║              📊 ATOMIC TRADER BACKTEST PERFORMANCE               ║
╚══════════════════════════════════════════════════════════════════╝{Style.RESET_ALL}
  Initial Capital       : ${self.initial_capital:,.2f}
  Final Capital         : ${self.final_capital:,.2f}
  Total Realized Net PnL: {pnl_color}${self.total_pnl:,.2f} ({self.total_return_pct:+.2f}%){Style.RESET_ALL}
  Max Drawdown          : {Fore.RED}{self.max_drawdown_pct:.2f}%{Style.RESET_ALL}
  
  Total Trades          : {self.total_trades}
  Winning Trades        : {Fore.GREEN}{self.winning_trades}{Style.RESET_ALL}
  Losing Trades         : {Fore.RED}{self.losing_trades}{Style.RESET_ALL}
  Win Rate              : {Fore.YELLOW}{self.win_rate:.2f}%{Style.RESET_ALL}
  Profit Factor         : {Fore.YELLOW}{self.profit_factor:.2f}{Style.RESET_ALL}
  Payoff Ratio (W/L)    : {self.payoff_ratio:.2f} (Avg Win: ${self.avg_win:.2f} | Avg Loss: ${self.avg_loss:.2f})
  Sharpe Ratio (hourly) : {self.sharpe_ratio:.2f}
  Circuit Breaker Halts : {self.circuit_breaker_halts}
{Fore.CYAN}{'═' * 66}{Style.RESET_ALL}
"""


class BacktestEngine:
    """
    Event-driven bar-by-bar backtest engine that matches Atomic Trader's exact live confirmation logic.
    """
    def __init__(
        self,
        data: pd.DataFrame,
        symbol: str = "BTCUSDT",
        initial_capital: float = 10000.0,
        fee_rate: float = 0.0004,      # 4 bps taker fee
        slippage_rate: float = 0.0005, # 5 bps slippage
        stop_loss_pct: float = 0.02,   # 2% Stop Loss
        take_profit_pct: float = 0.05, # 5% Take Profit
        use_trailing_stop: bool = True,
    ):
        self.raw_data = data.copy()
        self.symbol = symbol
        self.initial_capital = initial_capital
        self.fee_rate = fee_rate
        self.slippage_rate = slippage_rate
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.use_trailing_stop = use_trailing_stop

        self.risk_manager = RiskManager(initial_capital=initial_capital)
        self.trades: list[TradeRecord] = []
        self.equity_curve: list[dict[str, Any]] = []
        self.circuit_breaker_halts = 0

    def prepare_indicators(self) -> pd.DataFrame:
        df = self.raw_data.copy()

        # 1. Trend Regime: EMA 20 & EMA 50
        df["ema_20"] = df["close"].ewm(span=20, adjust=False).mean()
        df["ema_50"] = df["close"].ewm(span=50, adjust=False).mean()
        df["trend_regime"] = 0
        df.loc[df["ema_20"] > df["ema_50"], "trend_regime"] = 1
        df.loc[df["ema_20"] < df["ema_50"], "trend_regime"] = -1

        # 2. Volatility Regime: Bollinger Bands (20, 2)
        bb_basis = df["close"].rolling(window=20).mean()
        bb_std = df["close"].rolling(window=20).std()
        bb_upper = bb_basis + (bb_std * 2)
        bb_lower = bb_basis - (bb_std * 2)
        df["bb_width"] = (bb_upper - bb_lower) / bb_basis

        # 0 = low vol, 1 = med vol, 2 = high vol (> 5% band width)
        df["vol_regime"] = 0
        df.loc[df["bb_width"] > 0.025, "vol_regime"] = 1
        df.loc[df["bb_width"] > 0.05, "vol_regime"] = 2

        # 3. Order Book Imbalance / Momentum proxy:
        # Normalized candle body momentum smoothed over 3 periods
        candle_range = (df["high"] - df["low"]).replace(0, 1e-8)
        body = df["close"] - df["open"]
        df["obi"] = (body / candle_range).rolling(window=3).mean().fillna(0)

        # Drop warm-up periods
        df = df.dropna().copy()
        return df

    def run(self) -> BacktestMetrics:
        df = self.prepare_indicators()

        active_position: dict[str, Any] | None = None
        peak_capital = self.initial_capital
        max_drawdown = 0.0

        for timestamp, row in df.iterrows():
            current_price = row["close"]
            high_price = row["high"]
            low_price = row["low"]
            vol_regime = row["vol_regime"]
            trend_regime = row["trend_regime"]
            obi = row["obi"]

            # Update Risk Manager simulated time
            if hasattr(timestamp, "date"):
                # Midnight date transition check
                current_date = timestamp.date()
                if current_date > self.risk_manager.last_reset_date:
                    self.risk_manager.daily_start_capital = self.risk_manager.get_mark_to_market_capital()
                    self.risk_manager.daily_realized_pnl = 0.0
                    self.risk_manager.last_reset_date = current_date

            # Check daily drawdown circuit breaker
            if self.risk_manager.check_daily_loss():
                self.circuit_breaker_halts += 1
                if active_position is not None:
                    # Emergency liquidate
                    self._close_active_position(
                        active_position, current_price, timestamp, reason="DAILY_LOSS_BREACH"
                    )
                    active_position = None
                continue

            # Manage active position SL/TP
            if active_position is not None:
                direction = active_position["direction"]
                sl = active_position["stop_loss"]
                tp = active_position["take_profit"]

                hit_sl = False
                hit_tp = False
                exit_price = current_price

                if direction == "LONG":
                    # Check low for SL hit, high for TP hit
                    if low_price <= sl:
                        hit_sl = True
                        exit_price = min(current_price, sl)
                    elif high_price >= tp:
                        hit_tp = True
                        exit_price = max(current_price, tp)
                    elif self.use_trailing_stop:
                        # Move stop to breakeven when +1R gained
                        r_dist = active_position["entry_price"] - active_position["initial_sl"]
                        if current_price >= active_position["entry_price"] + r_dist:
                            active_position["stop_loss"] = max(active_position["stop_loss"], active_position["entry_price"])
                else:  # SHORT
                    if high_price >= sl:
                        hit_sl = True
                        exit_price = max(current_price, sl)
                    elif low_price <= tp:
                        hit_tp = True
                        exit_price = min(current_price, tp)
                    elif self.use_trailing_stop:
                        r_dist = active_position["initial_sl"] - active_position["entry_price"]
                        if current_price <= active_position["entry_price"] - r_dist:
                            active_position["stop_loss"] = min(active_position["stop_loss"], active_position["entry_price"])

                if hit_sl or hit_tp:
                    reason = "STOP_LOSS" if hit_sl else "TAKE_PROFIT"
                    self._close_active_position(active_position, exit_price, timestamp, reason=reason)
                    active_position = None
                    continue

                # Update floating PnL
                if direction == "LONG":
                    active_position["pnl"] = (current_price - active_position["entry_price"]) * active_position["size"]
                else:
                    active_position["pnl"] = (active_position["entry_price"] - current_price) * active_position["size"]
                self.risk_manager.positions[self.symbol] = {"pnl": active_position["pnl"]}

            # Check entry signal if flat
            if active_position is None:
                confirmations = 0
                direction = None

                if trend_regime > 0 and obi > 0.05:
                    direction = "LONG"
                elif trend_regime < 0 and obi < -0.05:
                    direction = "SHORT"

                # 1. Volatility Regime check (< 2 means low or medium volatility)
                if vol_regime < 2:
                    confirmations += 1

                # 2 & 3. Trend and OBI alignment
                if direction:
                    confirmations += 2

                if confirmations >= 3 and direction:
                    if direction == "LONG":
                        stop_loss = current_price * (1.0 - self.stop_loss_pct)
                        take_profit = current_price * (1.0 + self.take_profit_pct)
                        expected_rr = (take_profit - current_price) / (current_price - stop_loss)
                    else:
                        stop_loss = current_price * (1.0 + self.stop_loss_pct)
                        take_profit = current_price * (1.0 - self.take_profit_pct)
                        expected_rr = (current_price - take_profit) / (stop_loss - current_price)

                    can_open, _ = self.risk_manager.can_open_position(
                        self.symbol, expected_rr, direction=direction
                    )
                    if can_open:
                        size = self.risk_manager.calculate_position_size(
                            self.symbol, current_price, stop_loss
                        )
                        if size > 0:
                            # Apply slippage & fees on entry
                            entry_exec_price = (
                                current_price * (1 + self.slippage_rate)
                                if direction == "LONG"
                                else current_price * (1 - self.slippage_rate)
                            )
                            entry_fee = entry_exec_price * size * self.fee_rate
                            self.risk_manager.capital -= entry_fee

                            active_position = {
                                "entry_time": timestamp,
                                "direction": direction,
                                "entry_price": entry_exec_price,
                                "stop_loss": stop_loss,
                                "initial_sl": stop_loss,
                                "take_profit": take_profit,
                                "size": size,
                                "pnl": 0.0,
                                "entry_fee": entry_fee,
                            }
                            self.risk_manager.positions[self.symbol] = {"pnl": 0.0}

            # Track equity curve
            mtm_capital = self.risk_manager.get_mark_to_market_capital()
            peak_capital = max(peak_capital, mtm_capital)
            dd = (peak_capital - mtm_capital) / peak_capital if peak_capital > 0 else 0
            max_drawdown = max(max_drawdown, dd)
            self.equity_curve.append({
                "timestamp": timestamp,
                "capital": mtm_capital,
                "drawdown": dd,
            })

        # Close any lingering position at the end of the test
        if active_position is not None:
            last_price = df.iloc[-1]["close"]
            self._close_active_position(
                active_position, last_price, df.index[-1], reason="END_OF_BACKTEST"
            )

        return self._compute_metrics(max_drawdown)

    def _close_active_position(
        self, pos: dict[str, Any], exit_price: float, timestamp: pd.Timestamp, reason: str
    ):
        direction = pos["direction"]
        size = pos["size"]
        entry_price = pos["entry_price"]

        # Apply slippage on exit
        exit_exec_price = (
            exit_price * (1 - self.slippage_rate)
            if direction == "LONG"
            else exit_price * (1 + self.slippage_rate)
        )
        exit_fee = exit_exec_price * size * self.fee_rate

        if direction == "LONG":
            gross_pnl = (exit_exec_price - entry_price) * size
        else:
            gross_pnl = (entry_price - exit_exec_price) * size

        net_pnl = gross_pnl - exit_fee
        total_fees = pos.get("entry_fee", 0.0) + exit_fee
        pnl_pct = (net_pnl / (entry_price * size)) * 100 if entry_price * size > 0 else 0

        self.risk_manager.update_capital_after_trade(net_pnl)
        if self.symbol in self.risk_manager.positions:
            del self.risk_manager.positions[self.symbol]

        record = TradeRecord(
            entry_time=pos["entry_time"],
            exit_time=timestamp,
            symbol=self.symbol,
            direction=direction,
            entry_price=entry_price,
            exit_price=exit_exec_price,
            size=size,
            pnl=net_pnl,
            pnl_pct=pnl_pct,
            reason=reason,
            fee_cost=total_fees,
        )
        self.trades.append(record)

    def _compute_metrics(self, max_drawdown: float) -> BacktestMetrics:
        total_trades = len(self.trades)
        final_capital = self.risk_manager.capital
        total_pnl = final_capital - self.initial_capital
        total_return_pct = (total_pnl / self.initial_capital) * 100

        if total_trades == 0:
            return BacktestMetrics(
                initial_capital=self.initial_capital,
                final_capital=final_capital,
                total_trades=0,
                winning_trades=0,
                losing_trades=0,
                win_rate=0.0,
                profit_factor=0.0,
                total_pnl=total_pnl,
                total_return_pct=total_return_pct,
                max_drawdown_pct=max_drawdown * 100,
                sharpe_ratio=0.0,
                avg_win=0.0,
                avg_loss=0.0,
                payoff_ratio=0.0,
                circuit_breaker_halts=self.circuit_breaker_halts,
            )

        wins = [t.pnl for t in self.trades if t.pnl > 0]
        losses = [abs(t.pnl) for t in self.trades if t.pnl <= 0]

        winning_trades = len(wins)
        losing_trades = len(losses)
        win_rate = (winning_trades / total_trades) * 100

        gross_profit = sum(wins)
        gross_loss = sum(losses)
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (99.0 if gross_profit > 0 else 0.0)

        avg_win = float(np.mean(wins)) if wins else 0.0
        avg_loss = float(np.mean(losses)) if losses else 0.0
        payoff_ratio = (avg_win / avg_loss) if avg_loss > 0 else 0.0

        # Sharpe Ratio from hourly equity curve returns
        eq_df = pd.DataFrame(self.equity_curve)
        if not eq_df.empty and len(eq_df) > 1:
            returns = eq_df["capital"].pct_change().dropna()
            std = returns.std()
            sharpe = (returns.mean() / std) * np.sqrt(24 * 365) if std > 0 else 0.0
        else:
            sharpe = 0.0

        return BacktestMetrics(
            initial_capital=self.initial_capital,
            final_capital=final_capital,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            profit_factor=profit_factor,
            total_pnl=total_pnl,
            total_return_pct=total_return_pct,
            max_drawdown_pct=max_drawdown * 100,
            sharpe_ratio=sharpe,
            avg_win=avg_win,
            avg_loss=avg_loss,
            payoff_ratio=payoff_ratio,
            circuit_breaker_halts=self.circuit_breaker_halts,
        )


def run_benchmark(months: int = 12, symbol: str = "BTCUSDT") -> BacktestMetrics:
    from atomic_trader.backtest.data_loader import HistoricalDataLoader

    loader = HistoricalDataLoader()
    df = loader.fetch_ohlcv(symbol=symbol, timeframe="1h", months=months)
    engine = BacktestEngine(df, symbol=symbol)
    metrics = engine.run()
    print(metrics.summary())
    return metrics


if __name__ == "__main__":
    run_benchmark()
