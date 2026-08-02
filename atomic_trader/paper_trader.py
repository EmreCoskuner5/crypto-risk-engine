import asyncio
import time
from colorama import Fore, Style
from atomic_trader.risk_manager import RiskManager
from prometheus_client import start_http_server, Gauge

class AsyncPaperTrader:
    def __init__(self, aggregator, interval=60):
        self.aggregator = aggregator
        self.interval = interval
        self.risk_manager = RiskManager()
        self.symbols_to_track = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        
        # Prometheus Metrics
        self.metric_capital = Gauge('atomic_capital', 'Current Account Capital')
        self.metric_open_positions = Gauge('atomic_open_positions', 'Number of Open Positions')
        self.metric_total_pnl = Gauge('atomic_total_pnl', 'Total Unrealized PnL')
        
    async def start(self):
        print(f"{Fore.GREEN}[+] Starting Prometheus Metrics Server on port 8000...{Style.RESET_ALL}")
        try:
            start_http_server(8000)
        except Exception as e:
            print(f"{Fore.RED}[!] Could not start metrics server: {e}{Style.RESET_ALL}")
            
        print(f"{Fore.GREEN}[+] Async Paper Trading Engine Started. Capital: ${self.risk_manager.capital:.2f}{Style.RESET_ALL}")
        
        # Start the data aggregator in the background
        aggregator_task = asyncio.create_task(self.aggregator.run())
        
        print(f"{Fore.YELLOW}[!] Waiting 15 seconds for initial data collection...{Style.RESET_ALL}")
        await asyncio.sleep(15)
        
        try:
            while True:
                await self._tick()
                await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            pass
        finally:
            aggregator_task.cancel()
            
    async def _tick(self):
        print(f"\n{Fore.CYAN}--- TICK: {time.strftime('%Y-%m-%d %H:%M:%S')} ---{Style.RESET_ALL}")
        
        # S&P500 trend_regime (SPY) or BTCUSDT trend_regime determines the global risk regime
        # +2 = STRONG_BUY, +1 = BUY, 0 = NEUTRAL, -1 = SELL, -2 = STRONG_SELL
        btc_trend = self.aggregator.store.get("BTCUSDT", "trend_regime", default=0)
        
        if btc_trend > 0:
            self.risk_manager.regime = "RISK_ON"
        elif btc_trend < 0:
            self.risk_manager.regime = "RISK_OFF"
        else:
            self.risk_manager.regime = "MIXED"
            
        print(f"[REGIME] Current Market Regime: {self.risk_manager.regime} (BTC Trend: {btc_trend})")
        
        for symbol in self.symbols_to_track:
            self._analyze_symbol(symbol)
            
        # Update Prometheus Metrics
        self.metric_capital.set(self.risk_manager.capital)
        self.metric_open_positions.set(len(self.risk_manager.positions))
        total_pnl = sum([p.get('pnl', 0) for p in self.risk_manager.positions.values()])
        self.metric_total_pnl.set(total_pnl)
            
    def _analyze_symbol(self, symbol):
        store = self.aggregator.store
        latest = store.get_latest(symbol)
        
        if not latest:
            return
            
        current_price = latest.get("price")
        if not current_price:
            return
            
        # Update PnL for existing positions
        if symbol in self.risk_manager.positions:
            pos = self.risk_manager.positions[symbol]
            direction = pos.get('direction', 'LONG')
            if direction == 'LONG':
                pos['pnl'] = (current_price - pos['entry_price']) * pos['size']
            else:
                pos['pnl'] = (pos['entry_price'] - current_price) * pos['size']
            
            pnl_color = Fore.GREEN if pos['pnl'] >= 0 else Fore.RED
            print(f"{Fore.YELLOW}[POS]{Style.RESET_ALL} {symbol}: {direction} | Entry ${pos['entry_price']:.2f} | Current ${current_price:.2f} | PnL: {pnl_color}${pos['pnl']:.2f}{Style.RESET_ALL}")
            
            # Kural: Pyramiding
            if self.risk_manager.can_pyramid(symbol):
                print(f"{Fore.MAGENTA}[PYRAMID] Adding to winning position {symbol} (+50% size){Style.RESET_ALL}")
                add_size = pos['size'] * 0.5
                pos['size'] += add_size
                pos['adds'] += 1
            return 
            
        # 3+ Confirmations using the LIVE REAL-TIME DATA
        confirmations = 0
        vol_regime = latest.get("vol_regime", 1)
        obi = latest.get("obi", 0)
        trend_regime = latest.get("trend_regime", 0)
        
        # Determine direction based on Trend and OBI alignment
        direction = None
        if trend_regime > 0 and obi > 0.05:
            direction = "LONG"
        elif trend_regime < 0 and obi < -0.05:
            direction = "SHORT"

        # 1. Volatility Regime check
        if vol_regime < 2:
            confirmations += 1
            
        # 2 & 3. Trend and OBI alignment check
        if direction:
            confirmations += 2
            
        print(f"  🔍 {symbol:<8} | Price: ${current_price:.2f} | OBI: {obi:+.3f} | VolReg: {vol_regime} | TrendReg: {trend_regime} | Conf: {confirmations}/3")
            
        if confirmations >= 3 and direction:
            if direction == "LONG":
                stop_loss = current_price * 0.98  # %2 stop loss
                take_profit = current_price * 1.05 # %5 take profit
                expected_rr = (take_profit - current_price) / (current_price - stop_loss)
            else:
                stop_loss = current_price * 1.02  # %2 stop loss for short
                take_profit = current_price * 0.95 # %5 take profit for short
                expected_rr = (current_price - take_profit) / (stop_loss - current_price)
            
            can_open, reason = self.risk_manager.can_open_position(symbol, expected_rr, direction=direction)
            if can_open:
                size = self.risk_manager.calculate_position_size(symbol, current_price, stop_loss)
                if size > 0:
                    self.risk_manager.positions[symbol] = {
                        'direction': direction,
                        'size': size,
                        'entry_price': current_price,
                        'adds': 0,
                        'pnl': 0.0
                    }
                    print(f"{Fore.GREEN}[TRADE] OPEN {direction} {symbol} @ ${current_price:.2f} (Size: {size:.4f}, RR: {expected_rr:.2f}){Style.RESET_ALL}")
            else:
                print(f"{Fore.RED}[REJECTED]{Style.RESET_ALL} {symbol} {direction} trade rejected by Risk Manager: {reason}")
