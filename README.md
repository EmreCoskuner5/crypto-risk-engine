# 🚀 Atomic Trader (Crypto Risk Engine)

Atomic Trader is an advanced, event-driven algorithmic trading bot for cryptocurrency markets. Designed to operate safely in high-volatility environments, it incorporates real-time multi-source data aggregation, software-managed risk controls, and automated portfolio management.

## ✨ Features

- **Multi-Source Data Aggregation**: Ingests live data from Binance, Bybit, CoinGlass, TradingView, and Yahoo Finance concurrently.
- **Event-Driven Architecture**: Uses a robust `DataBus` to asynchronously distribute market events across the system without blocking the main event loop.
- **Software-Managed SL/TP**: In environments where native exchange algorithmic endpoints are restricted (e.g., Binance Testnet), the bot handles Stop Loss (-2%) and Take Profit (+5%) entirely via software logic, dynamically closing positions when targets are hit.
- **Data Freshness Shield**: Continuously monitors the age of market data. If the data goes stale (e.g., websocket disconnects or internet drops), the bot automatically halts trading decisions to prevent executing on ghost prices.
- **System Sleep Prevention**: Uses macOS `caffeinate` to strictly prevent system and display sleep modes during live trading, ensuring uninterrupted websocket connections.
- **Live Metrics Dashboard**: Fully integrated with **Prometheus & Grafana**, providing real-time visibility into capital, active positions, floating PnL, and system health.

## 🏗 Architecture

The system is built on a modern Python async stack:
- **`DataAggregator`**: Connects to multiple API/Websocket sources and normalizes data.
- **`FeatureStore`**: A thread-safe, in-memory cache that holds the most recent ticks, volume regimes, and order book imbalances (OBI).
- **`LiveTrader`**: The core execution engine. It polls the `FeatureStore` every 10 seconds, calculates confirmations, manages risk (1x leverage by default), and executes orders.
- **`RiskManager`**: Ensures daily loss limits and position sizing rules are strictly followed before allowing any trade execution.

## 📊 Grafana Integration

The bot exposes a Prometheus metrics server on port `8000`. By running the included `docker-compose.yml`, you get a pre-configured Grafana dashboard that monitors:
- `atomic_live_capital`: Total account capital.
- `atomic_live_open_positions`: Number of currently active trades.
- `atomic_live_total_pnl`: Floating/Unrealized PnL of open positions.

## 🚀 Getting Started

1. **Clone the repository**:
   ```bash
   git clone <your-repo-url>
   cd crypto-risk-engine
   ```

2. **Install dependencies**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure Environment**:
   Create a `.env` file in the root directory and add your API keys:
   ```env
   BINANCE_API_KEY=your_testnet_api_key
   BINANCE_SECRET=your_testnet_secret
   # Other keys...
   ```

4. **Run the Bot**:
   Use the startup script to prevent system sleep and launch the engine:
   ```bash
   ./run.sh
   ```

## ⚠️ Disclaimer
This software is for educational purposes only. Do not risk money which you are afraid to lose. USE THE SOFTWARE AT YOUR OWN RISK. THE AUTHORS AND ALL AFFILIATES ASSUME NO RESPONSIBILITY FOR YOUR TRADING RESULTS.
