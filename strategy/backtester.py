import pandas as pd

class Backtester:
    def __init__(self, data: pd.DataFrame):
        self.data = data
        
    def run_simple_moving_average_crossover(self, short_window=10, long_window=30):
        """
        A very simple backtest simulation: buy when short SMA crosses above long SMA, 
        sell when it crosses below. 
        Returns estimated returns.
        """
        df = self.data.copy()
        df['short_sma'] = df['close'].rolling(window=short_window).mean()
        df['long_sma'] = df['close'].rolling(window=long_window).mean()
        
        # Generate signals: 1 (buy), -1 (sell), 0 (hold)
        df['signal'] = 0
        df.loc[df['short_sma'] > df['long_sma'], 'signal'] = 1
        df.loc[df['short_sma'] < df['long_sma'], 'signal'] = -1
        
        # Calculate daily returns
        df['returns'] = df['close'].pct_change()
        
        # Shift signal by 1 to represent trading at the next close after the signal
        df['strategy_returns'] = df['signal'].shift(1) * df['returns']
        
        cumulative_returns = (1 + df['strategy_returns'].fillna(0)).cumprod() - 1
        total_return = cumulative_returns.iloc[-1] * 100
        
        return {
            "strategy": "SMA Crossover",
            "total_return_percent": round(total_return, 2)
        }
