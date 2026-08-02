import ccxt
import pandas as pd
from tradingview_ta import TA_Handler, Interval, Exchange

class MarketFetcher:
    def __init__(self):
        self.exchange = ccxt.binance()
        
    def get_ohlcv(self, symbol: str, timeframe: str = '1h', limit: int = 100) -> pd.DataFrame:
        """Fetch historical OHLCV data from Binance"""
        # symbol format: 'BTC/USDT'
        bars = self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df

    def get_tradingview_analysis(self, symbol: str, exchange: str = 'BINANCE', screener: str = 'crypto', interval=Interval.INTERVAL_1_HOUR):
        """Fetch TradingView technical analysis summary"""
        # symbol format for TV: 'BTCUSDT'
        tv_symbol = symbol.replace('/', '')
        handler = TA_Handler(
            symbol=tv_symbol,
            exchange=exchange,
            screener=screener,
            interval=interval,
            timeout=10
        )
        try:
            analysis = handler.get_analysis()
            return analysis.summary, analysis.indicators
        except Exception as e:
            return None, None
