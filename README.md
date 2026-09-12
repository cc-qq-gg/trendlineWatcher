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

## Quant Lab

The independent strategy lab lives in `quant_lab/`. It reads and merges every matching OKX CSV under `data/okx/**/*.csv` including `data/okx/csv` and dated subdirectories such as `data/okx/spot/YYYY-MM-DD`, computes indicators, combines weighted buy/sell rules, and runs local backtests.

Start it with the Fuel uv environment:

```bash
cd /Users/cq/projects/quant/quantauto
source ~/venvs/Fuel/bin/activate
streamlit run quant_lab/app.py --server.port 8501 --server.headless true
```

Open: http://localhost:8501

Current built-in modes:

- Quick event validation: test signals such as `MA(21,45)` rising plus StochRSI K first crossing above `10`, then summarize forward N-bar win rate, payoff ratio, profit factor, MFE, and MAE without running a full position backtest.
- Multi-timeframe MA pyramid long strategy: select `1W`, `1D`, `4H` and MA windows such as `21/147`; enter when the execution timeframe crosses up and all selected timeframes are bullish; add 1x leverage every configured price step; move older tranche stops to breakeven; close tranches on stop loss or exit all when price falls below the slow MA by the configured percent.
- Weighted composite strategy: combine MA trend/cross, RSI, MACD, Bollinger Bands, ADX, StochRSI, and Donchian breakout components into one weighted score. Edit each component weight and params, then backtest the score threshold.
- Regime Filter composite strategy: combine MA cross or Donchian breakout entries with ADX, ATR volatility, volume, and higher-timeframe trend gates before allowing trades.
- MA + StochRSI long/short strategy: scan whether at least 1, 2, or 3 of MA(21/60/128) point up/down together with StochRSI turning up/down.
- Batch validation/results comparison: run saved pyramid, weighted, and regime strategies together, compare return/drawdown/trades, and optionally save the batch artifacts under `quant_lab/runs/`.
- Parameter grid search: scan saved strategy parameters such as score thresholds, stop loss, MA windows, add-step percentage, and component weights; rank results by return and drawdown.
- Walk-forward validation: select parameters on rolling training windows and measure the next out-of-sample test windows.
- Freeform backtest: select indicators and combine weighted entry/exit rules.

Strategies are stored as JSON files under `quant_lab/configs/`. In the Quant Lab sidebar, use:

- `加载策略到表单` to load a saved strategy for editing.
- `另存为策略` to save the current parameters as a new strategy.
- `覆盖当前策略` to update the loaded strategy after editing.

Run the Quant Lab regression checks:

```bash
source ~/venvs/Fuel/bin/activate
python quant_lab/verify_quant_lab.py
```

Run saved strategies in batch from the terminal:

```bash
source ~/venvs/Fuel/bin/activate
python quant_lab/batch_runner.py --save --name batch_validation
```

Run a parameter grid search:

```bash
source ~/venvs/Fuel/bin/activate
python quant_lab/grid_search.py quant_lab/configs/weighted_composite_default.json --save --name weighted_grid
```

Run a Regime Filter grid search:

```bash
source ~/venvs/Fuel/bin/activate
python quant_lab/grid_search.py quant_lab/configs/regime_filter_default.json --grid '{"settings.adx_min":[15,18,22],"settings.volume_min_ratio":[0.7,0.9]}' --save --name regime_grid
```

Run walk-forward validation:

```bash
source ~/venvs/Fuel/bin/activate
python quant_lab/walk_forward.py quant_lab/configs/weighted_composite_default.json --grid '{"settings.entry_score":[0.15,0.2],"settings.stop_loss_pct":[0.03,0.05]}' --train-bars 800 --test-bars 300 --save --name weighted_walk
```

Run the quick event validator from a script:

```bash
source ~/venvs/Fuel/bin/activate
python - <<'PY'
from quant_lab.quick_event_validator import QuickEventSettings, run_quick_event_validation

settings = QuickEventSettings(
    symbol="BTC-USDT-SWAP",
    timeframe="4H",
    ma_lengths=[21, 45],
    ma_direction="up",
    required_ma_count=2,
    stochrsi_threshold=10,
    cross_direction="above",
    side="long",
    forward_bars=[3, 5, 10, 20],
)
print(run_quick_event_validation(settings)["summary"])
PY
```

If Streamlit is missing:

```bash
source ~/venvs/Fuel/bin/activate
uv pip install streamlit
```

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
