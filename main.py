import argparse
from colorama import Fore, Style, init
from data.market_fetcher import MarketFetcher
from data.social_fetcher import SocialFetcher
from analysis.technical_scanners import TechnicalScanners
from core.risk_calculator import RiskCalculator
from strategy.backtester import Backtester

init(autoreset=True)

def main():
    parser = argparse.ArgumentParser(description="Crypto Risk Engine")
    parser.add_argument("--symbol", type=str, default="BTC/USDT", help="Symbol to analyze (e.g., BTC/USDT)")
    parser.add_argument("--timeframe", type=str, default="1h", help="Timeframe (e.g., 1h, 4h, 1d)")
    parser.add_argument("--channel", type=str, default="tradingview", help="Telegram channel username to fetch data from")
    parser.add_argument("--demo", action="store_true", help="Run in demo mode")
    args = parser.parse_args()

    symbol = args.symbol
    
    print(f"{Fore.CYAN}=========================================")
    print(f"{Fore.CYAN}   CRYPTO RISK ENGINE INITIALIZING...    ")
    print(f"{Fore.CYAN}=========================================")
    print(f"Target: {Fore.YELLOW}{symbol}{Style.RESET_ALL} | Timeframe: {Fore.YELLOW}{args.timeframe}{Style.RESET_ALL}\n")

    fetcher = MarketFetcher()
    social_fetcher = SocialFetcher()
    scanners = TechnicalScanners()
    calculator = RiskCalculator()

    # 1. Fetch Data
    print(f"{Fore.BLUE}[*] Fetching market data from Binance & TradingView...")
    df = fetcher.get_ohlcv(symbol, timeframe=args.timeframe, limit=100)
    tv_summary, tv_indicators = fetcher.get_tradingview_analysis(symbol)
    
    if df.empty:
        print(f"{Fore.RED}[!] Failed to fetch market data.")
        return

    current_price = df['close'].iloc[-1]
    print(f"{Fore.GREEN}[+] Current Price: ${current_price:,.2f}")

    # 2. Run Technical Scanners
    print(f"{Fore.BLUE}[*] Running technical scanners...")
    bollinger_risk, lb, ub = scanners.calculate_bollinger_bands(df)
    vol_breakout, last_vol, avg_vol = scanners.detect_volume_breakout(df)
    
    # 3. Fetch Social & News Data
    print(f"{Fore.BLUE}[*] Fetching Global News & Sentiment Data...")
    reddit_avg, reddit_news = social_fetcher.fetch_reddit_sentiment(limit=2)
    yf_avg, yf_news = social_fetcher.fetch_yfinance_news(symbol)
    cp_avg, cp_news = social_fetcher.fetch_cryptopanic_news(limit=2)
    newsapi_avg, newsapi_news = social_fetcher.fetch_newsapi_news(limit=2)
    
    social_sentiments = [reddit_avg, yf_avg, cp_avg, newsapi_avg]
    
    # 4. Calculate Risk
    print(f"{Fore.BLUE}[*] Calculating Risk Score...")
    risk_result = calculator.calculate_risk(tv_summary, bollinger_risk, vol_breakout, social_sentiments)

    # 5. Display Results
    print(f"\n{Fore.MAGENTA}--- RISK REPORT ---")
    score = risk_result['score']
    
    # Color code category
    cat_color = Fore.GREEN if score < 40 else (Fore.YELLOW if score < 70 else Fore.RED)
    
    print(f"Risk Score: {cat_color}{score}/100{Style.RESET_ALL}")
    print(f"Category:   {cat_color}{risk_result['category']}{Style.RESET_ALL}")
    
    print("\nReasons:")
    if not risk_result['reasons']:
        print(" - No specific high-risk indicators found.")
    else:
        for r in risk_result['reasons']:
            print(f" - {r}")
            
    print(f"\n{Fore.MAGENTA}--- SOCIAL & NEWS RADAR ---")
    if yf_news:
        print(f"{Fore.CYAN}[Yahoo Finance - {symbol}] Avg Score: {yf_avg:.2f}{Style.RESET_ALL}" if yf_avg is not None else f"{Fore.CYAN}[Yahoo Finance - {symbol}]{Style.RESET_ALL}")
        for n in yf_news:
            print(f"  • {n['text'][:70]}... (Score: {n['score']})")
            
    if cp_news:
        print(f"\n{Fore.CYAN}[CryptoPanic] Avg Score: {cp_avg:.2f}{Style.RESET_ALL}" if cp_avg is not None else f"\n{Fore.CYAN}[CryptoPanic]{Style.RESET_ALL}")
        for n in cp_news:
            print(f"  • {n['text'][:70]}... (Score: {n['score']})")
            
    if newsapi_news:
        print(f"\n{Fore.CYAN}[NewsAPI - World Press] Avg Score: {newsapi_avg:.2f}{Style.RESET_ALL}" if newsapi_avg is not None else f"\n{Fore.CYAN}[NewsAPI - World Press]{Style.RESET_ALL}")
        for n in newsapi_news:
            print(f"  • {n['text'][:70]}... (Score: {n['score']})")
            
    if reddit_news:
        print(f"\n{Fore.CYAN}[Reddit r/CryptoCurrency] Avg Score: {reddit_avg:.2f}{Style.RESET_ALL}" if reddit_avg is not None else f"\n{Fore.CYAN}[Reddit r/CryptoCurrency]{Style.RESET_ALL}")
        for n in reddit_news:
            print(f"  • {n['title'][:70]}... (Score: {n['score']})")
        
    # 6. Backtest simple strategy
    print(f"\n{Fore.BLUE}[*] Running SMA Crossover Backtest on recent data...")
    backtester = Backtester(df)
    bt_result = backtester.run_simple_moving_average_crossover()
    print(f"Strategy: {bt_result['strategy']}")
    print(f"Estimated Return (last 100 candles): {Fore.YELLOW}{bt_result['total_return_percent']}%{Style.RESET_ALL}")
    print(f"{Fore.CYAN}=========================================\n")

if __name__ == "__main__":
    main()
