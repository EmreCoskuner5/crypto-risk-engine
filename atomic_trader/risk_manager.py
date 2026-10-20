import time
from datetime import datetime, timezone
from colorama import Fore, Style

class RiskManager:
    def __init__(self, initial_capital=10000.0):
        self.capital = initial_capital
        self.daily_start_capital = initial_capital
        self.daily_realized_pnl = 0.0  # Track realized PnL for daily loss limit
        self.last_reset_date = datetime.now(timezone.utc).date()
        self.positions = {} # symbol -> {size, entry_price, adds, pnl}
        self.consecutive_losses = 0
        self.halt_until = 0 # timestamp
        self.regime = "RISK_ON" # RISK_ON, RISK_OFF, MIXED

    def get_mark_to_market_capital(self):
        floating_pnl = sum(p.get('pnl', 0) for p in self.positions.values())
        return self.capital + floating_pnl

    def _maybe_reset_daily(self):
        """Reset daily baseline at UTC midnight."""
        today = datetime.now(timezone.utc).date()
        if today > self.last_reset_date:
            mtm_capital = self.get_mark_to_market_capital()
            print(f"{Fore.CYAN}[RISK MANAGER] New day ({today}) detected. Resetting daily baseline to ${mtm_capital:.2f} (prev: ${self.daily_start_capital:.2f}).{Style.RESET_ALL}")
            self.daily_start_capital = mtm_capital
            self.daily_realized_pnl = 0.0
            self.last_reset_date = today

    def check_daily_loss(self):
        """3% daily loss = HALT all trading - based on MARK-TO-MARKET (Realized + Floating PnL)"""
        self._maybe_reset_daily()
        mtm_capital = self.get_mark_to_market_capital()
        loss_pct = (self.daily_start_capital - mtm_capital) / self.daily_start_capital
        if loss_pct >= 0.03:
            floating_pnl = sum(p.get('pnl', 0) for p in self.positions.values())
            print(f"{Fore.RED}[RISK MANAGER] HALT! Daily 3% loss limit reached (MTM PnL: ${floating_pnl:.2f} | Realized: ${self.daily_realized_pnl:.2f}).{Style.RESET_ALL}")
            return True
        return False

    def update_capital_after_trade(self, pnl):
        self.capital += pnl
        self.daily_realized_pnl += pnl  # Track realized PnL separately
        if pnl < 0:
            self.consecutive_losses += 1
            if self.consecutive_losses >= 3:
                print(f"{Fore.RED}[RISK MANAGER] 3 Consecutive Losses! Pausing for 1 hour.")
                self.halt_until = time.time() + 3600
                # DON'T reset - keep the count to track pause state
        else:
            self.consecutive_losses = 0

    def can_open_position(self, symbol, expected_rr, direction="LONG"):
        """Check all risk rules before opening a trade"""
        if time.time() < self.halt_until:
            return False, "Paused due to consecutive losses"

        if self.check_daily_loss():
            return False, "Daily loss limit reached"

        if len(self.positions) >= 5:
            return False, "Max 5 positions reached"

        if expected_rr < 1.95:
            return False, f"R:R ({expected_rr:.2f}) < 1.95 minimum"

        if self.regime == "RISK_OFF" and symbol != "BTCUSDT":
            if direction == "LONG":
                return False, "Regime is RISK_OFF, blocking longs (except BTCUSDT)"

        return True, "Approved"

    def calculate_position_size(self, symbol, current_price, stop_loss_price, feature_store=None):
        """
        Volatility-targeted position sizing with fallback to 1% risk per trade.

        If feature_store is provided, uses ATR-based vol targeting.
        Otherwise falls back to fixed 1% risk per trade with regime adjustments.
        """
        # Try volatility-targeted sizing first
        if feature_store is not None:
            vol_size = feature_store.get_volatility_target_size(
                symbol,
                self.capital,
                target_vol_pct=0.02,  # 2% daily vol target
            )
            if vol_size is not None and vol_size > 0:
                # Apply regime adjustment
                if self.regime == "MIXED":
                    vol_size *= 0.75
                elif self.regime == "RISK_OFF":
                    vol_size *= 0.5
                return vol_size

        # Fallback: Fixed 1% risk per trade with regime adjustments
        risk_amount = self.capital * 0.01

        if self.regime == "MIXED":
            risk_amount *= 0.75  # reduce sizing 25%
        elif self.regime == "RISK_OFF":
            risk_amount *= 0.5   # reduce sizing 50%

        risk_per_share = abs(current_price - stop_loss_price)
        if risk_per_share == 0:
            return 0

        size = risk_amount / risk_per_share
        return size

    def can_pyramid(self, symbol):
        """Pyramiding: Add to winners only (50% size, max 2 adds)"""
        if symbol in self.positions:
            pos = self.positions[symbol]
            if pos.get('pnl', 0) > 0 and pos.get('adds', 0) < 2:
                return True
        return False