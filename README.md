# QuantAuto - Algorithmic Trading System

Cryptocurrency automated trading system based on trendline breakout and StochRSI indicators, primarily supporting OKX exchange.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Launch Web interface
python TrendlineWebApp.py

# Or start complete monitoring system
python run_trendline_monitor.py
```

Web interface: http://localhost:5000

## Core Features

- 🎯 **Trendline Breakout Detection** - Automatic identification of price breakout signals
- 📊 **StochRSI Monitoring** - Multi-timeframe StochRSI indicator analysis
- 📈 **Volatility Analysis** - 2σ/3σ abnormal volatility detection
- 💹 **Divergence Signals** - Bullish divergence (TBL) and double bullish divergence (DBL)
- 🌐 **Web Management Interface** - Visual configuration and monitoring panel

## Supported Trading Pairs

- SOL-USDT-SWAP
- ETH-USDT-SWAP
- BTC-USDT-SWAP
- Other OKX perpetual contracts

## Timeframes

- 15 minutes (15m)
- 4 hours (4H)
- 1 day (1D)
- 1 week (1W)