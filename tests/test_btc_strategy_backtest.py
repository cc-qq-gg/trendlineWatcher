import pandas as pd

from btc_strategy.backtest import complete_4h, run_backtest
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
