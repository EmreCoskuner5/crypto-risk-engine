"""CLI for Atomic Trader — paper trading and live data aggregation."""
import asyncio
import logging
import sys

from colorama import Fore, Style, init as colorama_init

colorama_init(autoreset=True)


def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
    logging.basicConfig(level=level, format=fmt, datefmt="%H:%M:%S")
    # Quiet down noisy libraries
    for lib in ("websockets", "aiohttp", "urllib3", "asyncio"):
        logging.getLogger(lib).setLevel(logging.WARNING)


def _print_banner() -> None:
    print(f"""
{Fore.CYAN}╔══════════════════════════════════════════════╗
║  {Fore.WHITE}⚡  ATOMIC TRADER  ⚡{Fore.CYAN}                      ║
║  {Fore.YELLOW}Real-Time Multi-Source Data Aggregator{Fore.CYAN}      ║
╚══════════════════════════════════════════════╝{Style.RESET_ALL}
""")


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_live_data(verbose: bool = False) -> None:
    """
    Start the live data aggregator.
    Pulls from: Binance WS, Bybit WS, CoinGlass REST, TradingView TA, Yahoo Finance.
    """
    _setup_logging(verbose)
    _print_banner()

    from atomic_trader.data.aggregator import build_default_aggregator

    print(f"{Fore.GREEN}[+] Starting all data sources...{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}    Press Ctrl+C to stop.\n{Style.RESET_ALL}")

    agg = build_default_aggregator()

    try:
        asyncio.run(agg.run())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}[!] Stopped by user.{Style.RESET_ALL}")


def cmd_live_data_watch(verbose: bool = False) -> None:
    """
    Live data + pretty-print latest feature snapshot every 10 seconds.
    """
    _setup_logging(verbose)
    _print_banner()

    from atomic_trader.data.aggregator import build_default_aggregator

    agg = build_default_aggregator()

    async def _watch_loop() -> None:
        watch_task = asyncio.create_task(_print_snapshot(agg))
        agg_task = asyncio.create_task(agg.run())
        await asyncio.gather(watch_task, agg_task)

    async def _print_snapshot(agg_ref) -> None:
        import pandas as pd
        pd.set_option("display.max_columns", 20)
        pd.set_option("display.width", 120)
        pd.set_option("display.float_format", "{:.6f}".format)

        await asyncio.sleep(10)  # warm-up
        while True:
            df = agg_ref.store.summary()
            print(f"\n{Fore.CYAN}{'─' * 80}")
            print(f"  📊 Feature Snapshot — {__import__('datetime').datetime.now().strftime('%H:%M:%S')}")
            print(f"{'─' * 80}{Style.RESET_ALL}")
            if df.empty:
                print(f"{Fore.YELLOW}  No data yet...{Style.RESET_ALL}")
            else:
                print(df.to_string())
            print()
            await asyncio.sleep(10)

    try:
        asyncio.run(_watch_loop())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}[!] Stopped by user.{Style.RESET_ALL}")


def cmd_paper_trade(interval: int = 10, verbose: bool = False) -> None:
    """Run paper trading asynchronously with real-time data."""
    _setup_logging(verbose)
    _print_banner()
    
    from atomic_trader.data.aggregator import build_default_aggregator
    from atomic_trader.paper_trader import AsyncPaperTrader
    
    agg = build_default_aggregator()
    pt = AsyncPaperTrader(aggregator=agg, interval=interval)
    
    try:
        asyncio.run(pt.start())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}[!] Stopped by user.{Style.RESET_ALL}")


from atomic_trader.paper_trader import AsyncPaperTrader
from atomic_trader.live_trader import AsyncLiveTrader
from atomic_trader.data.aggregator import build_default_aggregator


def cmd_live_trade(interval: int = 10, verbose: bool = False) -> None:
    """Start live trading engine on Binance Testnet."""
    _setup_logging(verbose)
    print(f"{Fore.CYAN}[+] Initializing Live Trading Engine (interval={interval}s){Style.RESET_ALL}")
    
    agg = build_default_aggregator()
    lt = AsyncLiveTrader(aggregator=agg, interval=interval)
    
    try:
        asyncio.run(lt.start())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}[!] Stopped by user.{Style.RESET_ALL}")


def cmd_test_alpaca(symbol: str = "AAPL", verbose: bool = False) -> None:
    """Test Yahoo Finance data connection for a given symbol."""
    _setup_logging(verbose)
    import yfinance as yf
    print(f"{Fore.GREEN}[+] Fetching {symbol} from Yahoo Finance...{Style.RESET_ALL}")
    t = yf.Ticker(symbol)
    hist = t.history(period="5d", interval="1h")
    if hist.empty:
        print(f"{Fore.RED}[!] No data returned for {symbol}{Style.RESET_ALL}")
    else:
        print(hist.tail(5).to_string())
        print(f"{Fore.GREEN}[✓] Connection OK — {len(hist)} rows{Style.RESET_ALL}")


def cmd_test_binance(verbose: bool = False) -> None:
    """Test Binance Demo API credentials."""
    _setup_logging(verbose)
    import os, ssl, hmac, hashlib, time, urllib.request
    from dotenv import load_dotenv
    load_dotenv()

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        print(f"{Fore.RED}[!] Error: BINANCE_API_KEY or BINANCE_API_SECRET not found in .env{Style.RESET_ALL}")
        return

    print(f"{Fore.CYAN}[+] Connecting to Binance Demo Trading (demo-fapi.binance.com)...{Style.RESET_ALL}")
    try:
        ts = int(time.time() * 1000)
        params = f"timestamp={ts}"
        sig = hmac.new(api_secret.encode(), params.encode(), hashlib.sha256).hexdigest()
        url = f"https://demo-fapi.binance.com/fapi/v2/balance?{params}&signature={sig}"

        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        req = urllib.request.Request(url, headers={"X-MBX-APIKEY": api_key})
        with urllib.request.urlopen(req, timeout=10, context=ctx) as r:
            import json
            data = json.loads(r.read().decode())
            usdt = next((x for x in data if x.get("asset") == "USDT"), None)
            balance = float(usdt["balance"]) if usdt else 0
            print(f"{Fore.GREEN}[✓] Connection Successful!{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}[+] Demo USDT Balance: ${balance:,.2f}{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}[!] Connection Failed: {e}{Style.RESET_ALL}")


def cmd_backtest(months: int = 12, symbol: str = "BTCUSDT", verbose: bool = False) -> None:
    """Run institutional backtest on Bybit 1h klines matching live signal confirmations."""
    _setup_logging(verbose)
    from atomic_trader.backtest.engine import run_benchmark
    print(f"{Fore.CYAN}[+] Running {months}-Month Backtest on {symbol} (1h timeframe)...{Style.RESET_ALL}")
    run_benchmark(months=months, symbol=symbol)

# ── Entry Point ───────────────────────────────────────────────────────────────

COMMANDS = {
    "live-data":       (cmd_live_data,       "Start real-time data aggregator (all sources)"),
    "live-data-watch": (cmd_live_data_watch, "Live data + pretty-print feature snapshot every 10s"),
    "paper-trade":     (cmd_paper_trade,     "Legacy paper trading loop"),
    "live-trade":      (cmd_live_trade,      "Start live trading on Binance Testnet"),
    "test-alpaca":     (cmd_test_alpaca,     "Test Yahoo Finance connection"),
    "test-binance":    (cmd_test_binance,    "Test Binance Testnet API credentials"),
    "backtest":        (cmd_backtest,        "Run 6-12 month backtest on Bybit 1h klines"),
}


def main() -> None:
    args = sys.argv[1:]

    if not args or args[0] in ("-h", "--help"):
        print(f"\n{Fore.CYAN}Atomic Trader CLI{Style.RESET_ALL}\n")
        print("Usage: python -m atomic_trader.cli <command> [options]\n")
        print("Commands:")
        for cmd, (_, desc) in COMMANDS.items():
            print(f"  {Fore.GREEN}{cmd:<20}{Style.RESET_ALL} {desc}")
        print(f"\nAdd {Fore.YELLOW}--verbose{Style.RESET_ALL} to any command for debug logs.\n")
        return

    command = args[0]
    verbose = "--verbose" in args or "-v" in args

    if command not in COMMANDS:
        print(f"{Fore.RED}Unknown command: {command}{Style.RESET_ALL}")
        sys.exit(1)

    fn, _ = COMMANDS[command]

    # Simple arg parsing
    if command == "live-data":
        fn(verbose=verbose)
    elif command == "live-data-watch":
        fn(verbose=verbose)
    elif command == "paper-trade":
        interval = 60
        for arg in args[1:]:
            if arg.startswith("--interval="):
                interval = int(arg.split("=")[1].rstrip(","))
        fn(interval=interval, verbose=verbose)
    elif command == "live-trade":
        interval = 60
        for arg in args[1:]:
            if arg.startswith("--interval="):
                interval = int(arg.split("=")[1])
        fn(interval=interval, verbose=verbose)
    elif command == "test-alpaca":
        symbol = args[1] if len(args) > 1 and not args[1].startswith("-") else "AAPL"
        fn(symbol=symbol, verbose=verbose)
    elif command == "test-binance":
        fn(verbose=verbose)
    elif command == "backtest":
        months = 12
        symbol = "BTCUSDT"
        for arg in args[1:]:
            if arg.startswith("--months="):
                months = int(arg.split("=")[1])
            elif arg.startswith("--symbol="):
                symbol = arg.split("=")[1]
        fn(months=months, symbol=symbol, verbose=verbose)


if __name__ == "__main__":
    main()
