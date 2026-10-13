from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from btc_strategy.config import StrategyConfig
from btc_strategy.data import load_csv, normalize_ohlcv


COST_RATE = 5 / 10_000 + 1 / 1_000
FUNDING_RATE_PER_8H = 1 / 10_000
MIN_MARGIN_RATIO = 1 / 100
HALVINGS = (
    pd.Timestamp("2016-07-09", tz="UTC"),
    pd.Timestamp("2020-05-11", tz="UTC"),
    pd.Timestamp("2024-04-20", tz="UTC"),
    pd.Timestamp("2028-04-20", tz="UTC"),
)


@dataclass(frozen=True)
class BacktestResult:
    bars: pd.DataFrame
    trades: pd.DataFrame
    open_position: dict[str, Any] | None
    equity: np.ndarray
    effective_position: np.ndarray
    effective_leverage: np.ndarray
    state: np.ndarray
    pending_action: str | None
    liquidated: bool
    metrics: dict[str, Any]


def complete_4h(raw: pd.DataFrame, observed_at: pd.Timestamp | None = None) -> pd.DataFrame:
    """Aggregate 5m candles and discard the still-open 4h bucket."""
    frame = normalize_ohlcv(raw)
    if observed_at is not None:
        observed_at = pd.Timestamp(observed_at)
        if observed_at.tzinfo is None:
            observed_at = observed_at.tz_localize("UTC")
        else:
            observed_at = observed_at.tz_convert("UTC")
        frame = frame[frame["candle_begin_time"] < observed_at]

    indexed = frame.set_index("candle_begin_time")
    counts = indexed.resample("4h", label="left", closed="left").size()
    output = indexed.resample("4h", label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    )
    valid = counts >= 48
    if observed_at is not None:
        valid &= output.index + pd.Timedelta(hours=4) <= observed_at
    else:
        # A bucket is complete only when all 48 source candles are present.
        valid &= counts >= 48
    output = output[valid].dropna(subset=["open", "high", "low", "close"]).reset_index()
    if (output[["open", "high", "low", "close"]] <= 0).any().any():
        raise ValueError("OHLC prices must be positive")
    output.attrs["period"] = "4h"
    return output


def make_signal(ma: pd.Series, ma_ref: pd.Series, slope_threshold: float) -> np.ndarray:
    slope = ma / ma_ref - 1
    signal = np.full(len(ma), np.nan)
    signal[(ma > ma_ref) & (slope > slope_threshold)] = 1
    signal[(ma < ma_ref) & (slope < -slope_threshold)] = 0
    return signal


def cycle_days(times: pd.Series) -> np.ndarray:
    values = pd.to_datetime(times, utc=True)
    out = []
    for value in values:
        previous = HALVINGS[0]
        for halving in HALVINGS:
            if value >= halving:
                previous = halving
            else:
                break
        out.append((value - previous).days)
    return np.asarray(out, dtype=float)


def add_features(frame: pd.DataFrame, config: StrategyConfig) -> pd.DataFrame:
    frame = frame.copy()
    frame["ma"] = frame["close"].rolling(config.ma_len).mean()
    frame["ma_ref"] = frame["ma"].shift(config.shift)
    frame["signal"] = make_signal(frame["ma"], frame["ma_ref"], config.slope_threshold)
    frame["ma200d"] = frame["close"].rolling(200 * 6).mean()
    frame["ma200d_ref_30d"] = frame["ma200d"].shift(30 * 6)
    frame["ma200d_slope_30d"] = frame["ma200d"] / frame["ma200d_ref_30d"] - 1
    frame["dist_ma200d"] = frame["close"] / frame["ma200d"] - 1
    frame["cycle_day"] = cycle_days(frame["candle_begin_time"])
    return frame


def regime_state(frame: pd.DataFrame, mode: str = "ma200") -> np.ndarray:
    close = frame["close"].to_numpy(dtype=float)
    ma200 = frame["ma200d"].to_numpy(dtype=float)
    slope = frame["ma200d_slope_30d"].to_numpy(dtype=float)
    dist = frame["dist_ma200d"].to_numpy(dtype=float)
    cycle_day = frame["cycle_day"].to_numpy(dtype=float)
    state = np.zeros(len(frame), dtype=int)

    above = close > ma200
    below = close < ma200
    slope_up = slope > 0
    slope_down = slope < 0
    early_mid_cycle = (cycle_day >= 0) & (cycle_day <= 540)
    late_cycle = (cycle_day > 540) & (cycle_day <= 1050)

    if mode == "ma200":
        state[above] = 1
        state[below] = -1
    elif mode == "cycle_ma200_overheat":
        state[above & slope_up] = 1
        state[below & slope_down] = -1
        state[early_mid_cycle & above & slope_up & (dist < 1.0)] = 1
        state[(late_cycle | (below & slope_down) | (dist > 1.5))] = -1
    else:
        raise ValueError(f"unknown regime mode: {mode}")
    state[~np.isfinite(ma200)] = 0
    return state


def lagged_targets(
    frame: pd.DataFrame, config: StrategyConfig, use_regime: bool, regime_mode: str = "ma200"
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    signal = pd.Series(frame["signal"]).ffill().fillna(0.0)
    target_position = signal.shift(1).fillna(0.0).to_numpy(dtype=float)
    state = regime_state(frame, regime_mode) if use_regime else np.zeros(len(frame), dtype=int)
    state_leverage = np.full(len(frame), config.base_leverage, dtype=float)
    if use_regime:
        state_leverage[state < 0] = config.risk_off_leverage
        state_leverage[state > 0] = config.risk_on_leverage
    # State is known at the prior close and is applied at this bar's open.
    lagged_state_leverage = np.r_[0.0, state_leverage[:-1]]
    lagged_state_leverage = lagged_state_leverage * (target_position != 0)
    return target_position, state, lagged_state_leverage


def _empty_trade_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "entry_time", "exit_time", "entry_idx", "exit_idx", "bars",
            "return", "entry_equity", "exit_equity", "entry_price", "exit_price",
            "fee_slippage_rate", "funding_rate", "total_cost_rate", "exit_reason",
        ]
    )


def run_backtest(
    frame: pd.DataFrame,
    config: StrategyConfig,
    *,
    use_regime: bool = False,
    regime_mode: str = "ma200",
) -> BacktestResult:
    if frame.empty:
        raise ValueError("backtest frame is empty")

    frame = frame.copy()
    if "ma" not in frame:
        frame["ma"] = frame["close"].rolling(config.ma_len).mean()
    if "ma_ref" not in frame:
        frame["ma_ref"] = frame["ma"].shift(config.shift)
    if "signal" not in frame:
        frame["signal"] = make_signal(frame["ma"], frame["ma_ref"], config.slope_threshold)
    if "ma200d" not in frame:
        frame["ma200d"] = frame["close"].rolling(200 * 6).mean()
        frame["ma200d_ref_30d"] = frame["ma200d"].shift(30 * 6)
        frame["ma200d_slope_30d"] = frame["ma200d"] / frame["ma200d_ref_30d"] - 1
        frame["dist_ma200d"] = frame["close"] / frame["ma200d"] - 1
        frame["cycle_day"] = cycle_days(frame["candle_begin_time"])

    open_price = frame["open"].to_numpy(dtype=float)
    close = frame["close"].to_numpy(dtype=float)
    low = frame["low"].to_numpy(dtype=float)
    times = frame["candle_begin_time"]
    target_position, state, lagged_state_leverage = lagged_targets(frame, config, use_regime, regime_mode)

    size = len(frame)
    equity = np.ones(size, dtype=float)
    effective_position = np.zeros(size, dtype=float)
    effective_leverage = np.zeros(size, dtype=float)
    transaction_cost_cash = np.zeros(size, dtype=float)
    funding_cost_cash = np.zeros(size, dtype=float)
    trades: list[dict[str, Any]] = []
    open_trade: dict[str, Any] | None = None
    disabled_until_flat = False
    pending_stop = False
    liquidated = False

    for index in range(size):
        previous_position = effective_position[index - 1] if index else 0.0
        previous_leverage = effective_leverage[index - 1] if index else 0.0
        previous_equity = equity[index - 1] if index else 1.0

        if pending_stop:
            desired_position = 0.0
        else:
            if disabled_until_flat and target_position[index] == 0:
                disabled_until_flat = False
            desired_position = 0.0 if disabled_until_flat else target_position[index]
        target_leverage = lagged_state_leverage[index] if desired_position != 0 else 0.0

        if index == 0:
            open_equity = previous_equity
        else:
            gap_return = open_price[index] / close[index - 1] - 1
            open_equity = previous_equity * (1 + previous_position * previous_leverage * gap_return)

        transaction_rate = abs(target_leverage - previous_leverage) * COST_RATE
        equity_at_open = open_equity * (1 - transaction_rate)
        transaction_cost_cash[index] += open_equity * transaction_rate

        entered = previous_position == 0 and desired_position != 0
        exited = previous_position != 0 and desired_position == 0

        if entered:
            open_trade = {
                "entry_time": times.iloc[index],
                "entry_idx": index,
                "entry_equity": float(equity_at_open),
                "entry_price": float(open_price[index]),
            }

        if exited and open_trade is not None:
            reason = "stop_loss" if pending_stop else "signal_exit"
            trades.append(
                {
                    **open_trade,
                    "exit_time": times.iloc[index],
                    "exit_idx": index,
                    "exit_equity": float(equity_at_open),
                    "exit_price": float(open_price[index]),
                    "exit_reason": reason,
                }
            )
            open_trade = None
            pending_stop = False

        funding_rate = target_leverage * FUNDING_RATE_PER_8H * 4 / 8 if target_leverage > 0 else 0.0
        if target_leverage > 0:
            bar_return = close[index] / open_price[index] - 1
            equity[index] = equity_at_open * (1 + target_leverage * bar_return - funding_rate)
            funding_cost_cash[index] = equity_at_open * funding_rate
        else:
            equity[index] = equity_at_open

        effective_position[index] = desired_position
        effective_leverage[index] = target_leverage

        if desired_position != 0:
            adverse_return = low[index] / open_price[index] - 1
            liquidation_hit = adverse_return * target_leverage <= -(1 - MIN_MARGIN_RATIO) or equity[index] <= 0
            if liquidation_hit:
                equity[index:] = 0
                effective_position[index:] = 0
                effective_leverage[index:] = 0
                if open_trade is not None:
                    trades.append(
                        {
                            **open_trade,
                            "exit_time": times.iloc[index],
                            "exit_idx": index,
                            "exit_equity": 0.0,
                            "exit_price": float(close[index]),
                            "exit_reason": "liquidation",
                        }
                    )
                    open_trade = None
                liquidated = True
                break

            trade_return = equity[index] / open_trade["entry_equity"] - 1
            if config.stop_loss is not None and trade_return <= -config.stop_loss:
                pending_stop = True

        if pending_stop:
            disabled_until_flat = True

    if open_trade is not None:
        start = int(open_trade["entry_idx"])
        end = size - 1
        fee_cost = float(transaction_cost_cash[start : end + 1].sum())
        funding_cost = float(funding_cost_cash[start : end + 1].sum())
        next_target_position = float(pd.Series(frame["signal"]).ffill().fillna(0).iloc[-1])
        if pending_stop:
            pending_action = "stop_exit"
        elif next_target_position == 0:
            pending_action = "signal_exit"
        else:
            pending_action = "hold"
        open_position = {
            **open_trade,
            "exit_idx": end,
            "bars": end - start + 1,
            "mark_time": times.iloc[end],
            "mark_price": float(close[end]),
            "exit_equity": float(equity[end]),
            "unrealized_return": float(equity[end] / open_trade["entry_equity"] - 1),
            "fee_slippage_rate": fee_cost / open_trade["entry_equity"],
            "funding_rate": funding_cost / open_trade["entry_equity"],
            "total_cost_rate": (fee_cost + funding_cost) / open_trade["entry_equity"],
            "exit_reason": "open",
            "pending_action": pending_action,
        }
    else:
        open_position = None
        pending_action = None

    trades_frame = pd.DataFrame(trades, columns=_empty_trade_frame().columns)
    if not trades_frame.empty:
        trades_frame["bars"] = trades_frame["exit_idx"] - trades_frame["entry_idx"] + 1
        trades_frame["return"] = trades_frame["exit_equity"] / trades_frame["entry_equity"] - 1
        fee_rates = []
        funding_rates = []
        for row in trades_frame.itertuples(index=False):
            fee_cost = float(transaction_cost_cash[row.entry_idx : row.exit_idx + 1].sum())
            funding_cost = float(funding_cost_cash[row.entry_idx : row.exit_idx + 1].sum())
            fee_rates.append(fee_cost / row.entry_equity)
            funding_rates.append(funding_cost / row.entry_equity)
        trades_frame["fee_slippage_rate"] = fee_rates
        trades_frame["funding_rate"] = funding_rates
        trades_frame["total_cost_rate"] = trades_frame["fee_slippage_rate"] + trades_frame["funding_rate"]

    detail = frame[
        ["candle_begin_time", "open", "high", "low", "close", "ma", "ma_ref", "signal", "ma200d"]
    ].copy()
    detail["pos"] = effective_position
    detail["leverage"] = effective_leverage
    detail["regime_state"] = state
    detail["equity"] = equity
    detail["drawdown"] = equity / np.maximum.accumulate(equity) - 1

    metrics = calculate_metrics(frame, equity, effective_position, trades_frame, open_position, liquidated)
    return BacktestResult(
        bars=detail,
        trades=trades_frame,
        open_position=open_position,
        equity=equity,
        effective_position=effective_position,
        effective_leverage=effective_leverage,
        state=state,
        pending_action=pending_action,
        liquidated=liquidated,
        metrics=metrics,
    )


def calculate_metrics(
    frame: pd.DataFrame,
    equity: np.ndarray,
    position: np.ndarray,
    trades: pd.DataFrame,
    open_position: dict[str, Any] | None,
    liquidated: bool,
) -> dict[str, Any]:
    returns = trades["return"] if not trades.empty else pd.Series(dtype=float)
    wins = returns[returns > 0]
    losses = returns[returns < 0]
    gross_loss = abs(losses.sum())
    return {
        "final_equity": float(equity[-1]),
        "max_drawdown": float((equity / np.maximum.accumulate(equity) - 1).min()),
        "trade_count": int(len(returns)),
        "open_position_count": int(open_position is not None),
        "win_rate": float((returns > 0).mean()) if len(returns) else 0.0,
        "profit_loss_ratio": float(wins.sum() / gross_loss) if gross_loss else 0.0,
        "exposure": float((position != 0).mean()),
        "liquidated": bool(liquidated),
        "data_start": frame["candle_begin_time"].iloc[0],
        "data_end": frame["candle_begin_time"].iloc[-1],
    }


def annual_rows(equity: np.ndarray, times: pd.Series) -> list[dict[str, Any]]:
    frame = pd.DataFrame({"time": times, "equity": equity})
    rows = []
    for year, group in frame.groupby(frame["time"].dt.year, sort=True):
        start_equity = float(frame.loc[group.index[0] - 1, "equity"]) if group.index[0] else 1.0
        end_equity = float(group["equity"].iloc[-1])
        rows.append(
            {
                "year": int(year),
                "yearEquity": end_equity / start_equity if start_equity else 0.0,
                "endEquity": end_equity,
                "yearMdd": float((group["equity"] / np.maximum.accumulate(group["equity"]) - 1).min()),
            }
        )
    return rows


def _time_value(value: pd.Timestamp) -> int:
    return int(pd.Timestamp(value).timestamp())


def _safe_float(value: Any, digits: int = 8) -> float | None:
    if value is None or pd.isna(value) or not np.isfinite(value):
        return None
    return round(float(value), digits)


def payload_bars(result: BacktestResult) -> list[dict[str, Any]]:
    rows = []
    for row in result.bars.itertuples(index=False):
        rows.append(
            {
                "time": _time_value(row.candle_begin_time),
                "open": round(float(row.open), 4),
                "high": round(float(row.high), 4),
                "low": round(float(row.low), 4),
                "close": round(float(row.close), 4),
                "ma": _safe_float(row.ma, 4),
                "maRef": _safe_float(row.ma_ref, 4),
                "signal": _safe_float(row.signal),
                "pos": float(row.pos),
                "leverage": _safe_float(row.leverage, 4),
                "regimeState": int(row.regime_state),
                "ma200d": _safe_float(row.ma200d, 4),
                "equity": round(float(row.equity), 8),
                "drawdown": round(float(row.drawdown), 8),
            }
        )
    return rows


def payload_trades(trades: pd.DataFrame) -> list[dict[str, Any]]:
    output = []
    for index, row in enumerate(trades.to_dict("records"), start=1):
        output.append(
            {
                "id": index,
                "entryTime": _time_value(row["entry_time"]),
                "exitTime": _time_value(row["exit_time"]),
                "entryText": str(row["entry_time"]),
                "exitText": str(row["exit_time"]),
                "bars": int(row["bars"]),
                "return": round(float(row["return"]), 8),
                "entryEquity": round(float(row["entry_equity"]), 8),
                "exitEquity": round(float(row["exit_equity"]), 8),
                "entryPrice": round(float(row["entry_price"]), 4),
                "exitPrice": round(float(row["exit_price"]), 4),
                "priceReturn": round(float(row["exit_price"] / row["entry_price"] - 1), 8),
                "feeSlippage": _safe_float(row["fee_slippage_rate"]),
                "funding": _safe_float(row["funding_rate"]),
                "wear": _safe_float(row["total_cost_rate"]),
                "exitReason": str(row["exit_reason"]),
                "year": int(pd.Timestamp(row["entry_time"]).year),
                "isWin": bool(row["return"] > 0),
                "isOpen": False,
            }
        )
    return output


def payload_open_position(open_position: dict[str, Any] | None) -> dict[str, Any] | None:
    if open_position is None:
        return None
    return {
        "id": 0,
        "entryTime": _time_value(open_position["entry_time"]),
        "entryText": str(open_position["entry_time"]),
        "markTime": _time_value(open_position["mark_time"]),
        "markText": str(open_position["mark_time"]),
        "bars": int(open_position["bars"]),
        "return": round(float(open_position["unrealized_return"]), 8),
        "entryEquity": round(float(open_position["entry_equity"]), 8),
        "exitEquity": round(float(open_position["exit_equity"]), 8),
        "entryPrice": round(float(open_position["entry_price"]), 4),
        "exitPrice": round(float(open_position["mark_price"]), 4),
        "priceReturn": round(float(open_position["mark_price"] / open_position["entry_price"] - 1), 8),
        "feeSlippage": _safe_float(open_position["fee_slippage_rate"]),
        "funding": _safe_float(open_position["funding_rate"]),
        "wear": _safe_float(open_position["total_cost_rate"]),
        "exitReason": "open",
        "pendingAction": open_position["pending_action"],
        "year": int(pd.Timestamp(open_position["entry_time"]).year),
        "isWin": bool(open_position["unrealized_return"] > 0),
        "isOpen": True,
    }


def strategy_payload(
    key: str,
    label: str,
    description: str,
    result: BacktestResult,
    config: StrategyConfig,
) -> dict[str, Any]:
    metrics = result.metrics
    return {
        "key": key,
        "label": label,
        "meta": {
            "symbol": "BTC-USDT",
            "period": "4h",
            "ma": config.ma_len,
            "window": config.shift,
            "slope": config.slope_threshold,
            "description": description,
            "execution": "next_open_after_completed_4h",
            "version": config.version,
            "finalEquity": round(float(metrics["final_equity"]), 8),
            "maxDrawdown": round(float(metrics["max_drawdown"]), 8),
            "tradeCount": int(metrics["trade_count"]),
            "openPositionCount": int(metrics["open_position_count"]),
            "winRate": round(float(metrics["win_rate"]), 8),
            "profitLossRatio": round(float(metrics["profit_loss_ratio"]), 8),
            "exposure": round(float(metrics["exposure"]), 8),
            "avgLeverage": _safe_float(
                result.effective_leverage[result.effective_leverage > 0].mean(), 4
            ),
            "stopLoss": config.stop_loss,
            "riskOnPct": round(float((result.state > 0).mean()), 8),
            "riskOffPct": round(float((result.state < 0).mean()), 8),
            "pendingAction": result.pending_action,
            "liquidated": result.liquidated,
            "dataStart": metrics["data_start"].isoformat(),
            "dataEnd": metrics["data_end"].isoformat(),
        },
        "bars": payload_bars(result),
        "trades": payload_trades(result.trades),
        "openPosition": payload_open_position(result.open_position),
        "annual": annual_rows(result.equity, result.bars["candle_begin_time"]),
    }


def build_payload(
    data_path: str | Path = Path("data/btc_strategy/raw/BTC-USDT_5m.csv"),
    observed_at: pd.Timestamp | str | None = None,
) -> dict[str, Any]:
    config = StrategyConfig(name="BTC 4H")
    features = add_features(complete_4h(load_csv(data_path), observed_at), config)
    models = (
        ("baseline", "Baseline fixed 2.2", "固定 2.2x / stop 8%", False, "ma200", config.risk_off_leverage, config.risk_on_leverage),
        ("defensive", "MA200 defensive 0.8/2.4", "risk-off 0.8x / risk-on 2.4x", True, "ma200", 0.8, 2.4),
        ("growth", "MA200 growth 0.8/2.8", "risk-off 0.8x / risk-on 2.8x", True, "ma200", 0.8, 2.8),
        ("aggressive", "MA200 aggressive 0.8/3.0", "risk-off 0.8x / risk-on 3.0x", True, "ma200", 0.8, 3.0),
    )
    datasets = {}
    for key, label, description, use_regime, regime_mode, risk_off, risk_on in models:
        model_config = replace(config, risk_off_leverage=risk_off, risk_on_leverage=risk_on)
        result = run_backtest(features, model_config, use_regime=use_regime, regime_mode=regime_mode)
        datasets[key] = strategy_payload(key, label, description, result, model_config)
    return {
        "default": "baseline",
        "schema": "btc-strategy-next-open-v1",
        "datasets": datasets,
    }


def atomic_json(payload: dict[str, Any], path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    temporary.replace(target)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the BTC 4h strategy payload")
    parser.add_argument("--data-path", default="data/btc_strategy/raw/BTC-USDT_5m.csv")
    parser.add_argument("--output", default="data/btc_strategy/backtests/latest_payload.json")
    parser.add_argument("--observed-at", default=None)
    args = parser.parse_args()
    payload = build_payload(args.data_path, args.observed_at)
    atomic_json(payload, args.output)
    for key, item in payload["datasets"].items():
        meta = item["meta"]
        print(f"{key}: trades={meta['tradeCount']} open={meta['openPositionCount']} equity={meta['finalEquity']}")


if __name__ == "__main__":
    main()
