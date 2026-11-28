import numpy as np
import pandas as pd

from btc_strategy.backtest import build_payload, complete_4h, run_backtest
from btc_strategy.config import AGGRESSIVE


def four_h_frame(n=120):
    times = pd.date_range("2026-01-01", periods=n, freq="4h", tz="UTC")
    frame = pd.DataFrame(
        {
            "candle_begin_time": times,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 101.0,
            "volume": 1.0,
        }
    )
    frame["signal"] = float("nan")
    return frame


def test_signal_exit_happens_at_next_open():
    frame = four_h_frame()
    frame.loc[10, "signal"] = 1.0
    frame.loc[20, "signal"] = 0.0
    result = run_backtest(frame, AGGRESSIVE)

    assert len(result.trades) == 1
    trade = result.trades.iloc[0]
    assert trade["entry_idx"] == 11
    assert trade["exit_idx"] == 21
    assert trade["entry_price"] == frame.loc[11, "open"]
    assert trade["exit_price"] == frame.loc[21, "open"]
    assert trade["exit_reason"] == "signal_exit"
    assert result.open_position is None


def test_nan_signal_keeps_the_final_position_open():
    frame = four_h_frame()
    frame.loc[10, "signal"] = 1.0
    result = run_backtest(frame, AGGRESSIVE)

    assert result.trades.empty
    assert result.effective_position[-1] == 1.0
    assert result.open_position is not None
    assert result.open_position["entry_idx"] == 11
    assert result.open_position["pending_action"] == "hold"
    assert result.metrics["trade_count"] == 0
    assert result.metrics["open_position_count"] == 1


def test_stop_exit_waits_for_the_next_open():
    frame = four_h_frame()
    frame.loc[10, "signal"] = 1.0
    frame.loc[11, "close"] = 94.0
    frame.loc[12, "open"] = 94.0
    frame.loc[12, "close"] = 94.0
    result = run_backtest(frame, AGGRESSIVE)

    assert len(result.trades) == 1
    assert result.trades.iloc[0]["exit_reason"] == "stop_loss"
    assert result.trades.iloc[0]["exit_idx"] == 12


def test_complete_4h_drops_partial_bucket():
    times = pd.date_range("2026-01-01", periods=50, freq="5min", tz="UTC")
    raw = pd.DataFrame(
        {
            "candle_begin_time": times,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 1.0,
        }
    )
    output = complete_4h(raw)
    assert len(output) == 1
    assert output.iloc[0]["candle_begin_time"] == pd.Timestamp("2026-01-01", tz="UTC")


def write_5m_csv(path, days=320):
    """合成一段正弦行情：4H 上 MA11 会反复上穿/下穿，保证每档真的产生持仓。"""
    step = np.arange(days * 288)
    close = 100.0 * (1.0 + 0.25 * np.sin(step / 288.0))
    frame = pd.DataFrame(
        {
            "candle_begin_time": pd.date_range("2020-01-01", periods=len(step), freq="5min", tz="UTC"),
            "open": close,
            "high": close * 1.002,
            "low": close * 0.998,
            "close": close,
            "volume": 1.0,
        }
    )
    frame.to_csv(path, index=False)
    return path


def test_payload_ships_fixed_one_before_baseline(tmp_path):
    payload = build_payload(write_5m_csv(tmp_path / "5m.csv"))

    assert list(payload["datasets"]) == ["fixed1", "baseline", "defensive", "growth", "aggressive"]
    assert payload["default"] == "baseline"
    assert payload["datasets"]["fixed1"]["meta"]["description"] == "固定 1.0x / stop 8%"


def strategy_column(payload, key, field):
    index = payload["strategyFields"].index(field)
    return [row[index] for row in payload["datasets"][key]["bars"]]


def test_fixed_one_is_same_signal_at_unit_leverage(tmp_path):
    payload = build_payload(write_5m_csv(tmp_path / "5m.csv"))

    fixed_lev = strategy_column(payload, "fixed1", "leverage")
    base_lev = strategy_column(payload, "baseline", "leverage")
    assert set(fixed_lev) == {0.0, 1.0}          # 有仓恒 1x，空仓 0
    assert set(base_lev) == {0.0, 2.2}           # 对照组仍是固定 2.2x
    # 择时来源相同（都不做 MA200 择时），但止损按含杠杆的权益回报判定，
    # 2.2x 会更早打到 -8%，所以持仓序列允许分叉，这里只校验来源与杠杆口径。
    assert set(strategy_column(payload, "fixed1", "regimeState")) == {0}
    assert set(strategy_column(payload, "baseline", "regimeState")) == {0}
    assert strategy_column(payload, "fixed1", "equity") != strategy_column(payload, "baseline", "equity")
