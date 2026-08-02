import time
from colorama import Fore

class RiskManager:
    def __init__(self, initial_capital=10000.0):
        self.capital = initial_capital
        self.daily_start_capital = initial_capital
        self.daily_realized_pnl = 0.0  # Track realized PnL for daily loss limit
        self.positions = {} # symbol -> {size, entry_price, adds, pnl}
        self.consecutive_losses = 0
        self.halt_until = 0 # timestamp
        self.regime = "RISK_ON" # RISK_ON, RISK_OFF, MIXED

    def get_mark_to_market_capital(self):
        floating_pnl = sum(p.get('pnl', 0) for p in self.positions.values())
        return self.capital + floating_pnl

    def check_daily_loss(self):
        """3% daily loss = HALT all trading - based on MARK-TO-MARKET (Realized + Floating PnL)"""
        mtm_capital = self.get_mark_to_market_capital()
        loss_pct = (self.daily_start_capital - mtm_capital) / self.daily_start_capital
        if loss_pct >= 0.03:
            floating_pnl = sum(p.get('pnl', 0) for p in self.positions.values())
            print(f"{Fore.RED}[RISK MANAGER] HALT! Daily 3% loss limit reached (MTM PnL: ${floating_pnl:.2f} | Realized: ${self.daily_realized_pnl:.2f}).")
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

    def calculate_position_size(self, symbol, current_price, stop_loss_price):
        """1% risk per trade with regime adjustments"""
        risk_amount = self.capital * 0.01

        if self.regime == "MIXED":
            risk_amount *= 0.75 # reduce sizing 25%

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