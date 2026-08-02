class RiskCalculator:
    def __init__(self):
        pass
        
    def calculate_risk(self, tv_summary, bollinger_risk, volume_breakout, social_sentiments=None) -> dict:
        """
        Calculates a risk score from 0 (Safest) to 100 (Highest Risk)
        """
        score = 50 # Default Neutral
        reasons = []
        
        # 1. TradingView Recommendation (e.g. STRONG_BUY, BUY, NEUTRAL, SELL, STRONG_SELL)
        if tv_summary:
            rec = tv_summary.get('RECOMMENDATION')
            if rec == 'STRONG_BUY':
                score -= 20
                reasons.append("TradingView signals Strong Buy (Low Risk)")
            elif rec == 'BUY':
                score -= 10
                reasons.append("TradingView signals Buy")
            elif rec == 'SELL':
                score += 15
                reasons.append("TradingView signals Sell (Elevated Risk)")
            elif rec == 'STRONG_SELL':
                score += 30
                reasons.append("TradingView signals Strong Sell (High Risk)")
                
        # 2. Bollinger Bands
        if bollinger_risk == 'OVERBOUGHT_HIGH_RISK':
            score += 20
            reasons.append("Price is above Upper Bollinger Band (Overbought, Correction Risk)")
        elif bollinger_risk == 'OVERSOLD_LOW_RISK':
            score -= 15
            reasons.append("Price is below Lower Bollinger Band (Oversold, Rebound Potential)")
            
        # 3. Volume Breakout
        if volume_breakout:
            reasons.append("High Volume Breakout detected. Volatility is high.")
            score += 10
            
        # 4. Social & News Sentiment
        if social_sentiments:
            valid_scores = [s for s in social_sentiments if s is not None]
            if valid_scores:
                avg_social = sum(valid_scores) / len(valid_scores)
                if avg_social <= -0.15:
                    score += 20
                    reasons.append(f"Genel haber duyarliligi negatif (Score: {avg_social:.2f}). Piyasa Kotumser.")
                elif avg_social >= 0.15:
                    score -= 15
                    reasons.append(f"Genel haber duyarliligi pozitif (Score: {avg_social:.2f}). Piyasa Iyimser.")
            
        # Clamp score between 0 and 100
        score = max(0, min(100, score))
        
        # Categorize
        if score < 30:
            category = "LOW RISK"
        elif score < 60:
            category = "MODERATE RISK"
        elif score < 80:
            category = "HIGH RISK"
        else:
            category = "EXTREME RISK"
            
        return {
            "score": score,
            "category": category,
            "reasons": reasons
        }
