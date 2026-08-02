import pandas as pd
import numpy as np

class TechnicalScanners:
    @staticmethod
    def calculate_bollinger_bands(df: pd.DataFrame, window: int = 20, num_of_std: int = 2):
        """Calculates Bollinger Bands and returns if price is near/outside bands"""
        df['sma'] = df['close'].rolling(window=window).mean()
        df['std'] = df['close'].rolling(window=window).std()
        df['upper_band'] = df['sma'] + (df['std'] * num_of_std)
        df['lower_band'] = df['sma'] - (df['std'] * num_of_std)
        
        last_close = df['close'].iloc[-1]
        lower_band = df['lower_band'].iloc[-1]
        upper_band = df['upper_band'].iloc[-1]
        
        risk = "NEUTRAL"
        if last_close < lower_band:
            risk = "OVERSOLD_LOW_RISK"
        elif last_close > upper_band:
            risk = "OVERBOUGHT_HIGH_RISK"
            
        return risk, lower_band, upper_band

    @staticmethod
    def detect_volume_breakout(df: pd.DataFrame, window: int = 20):
        """Detects if recent volume is significantly higher than average"""
        df['vol_sma'] = df['volume'].rolling(window=window).mean()
        last_vol = df['volume'].iloc[-1]
        avg_vol = df['vol_sma'].iloc[-1]
        
        is_breakout = last_vol > (avg_vol * 2.0)
        return is_breakout, last_vol, avg_vol
