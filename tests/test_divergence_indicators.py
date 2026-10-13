import numpy as np
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import alert_indicators
from divergence_visualizer import divergence_detector as detector


def run_with_macd_hist(monkeypatch, hist, highs=None, lows=None):
    hist = np.asarray(hist, dtype=float)
    size = len(hist)
    highs = np.full(size, 12.0) if highs is None else np.asarray(highs, dtype=float)
    lows = np.full(size, 10.0) if lows is None else np.asarray(lows, dtype=float)
    frame = pd.DataFrame(
        {
            "candle_begin_time": pd.date_range("2026-01-01", periods=size, freq="4h"),
            "Open": (highs + lows) / 2,
            "High": highs,
            "Low": lows,
            "Close": (highs + lows) / 2,
            "Volume": np.ones(size),
        }
    )
    monkeypatch.setattr(
        alert_indicators.talib,
        "MACD",
        lambda *args, **kwargs: (np.zeros(size), np.zeros(size), hist),
    )
    return detector.add_macd_indicators(frame)


def test_macd_divergence_marks_top_and_bottom_with_project_ohlc_columns(monkeypatch):
    top_highs = [10, 15, 14, 10, 10, 10, 16, 14, 10, 10]
    top = run_with_macd_hist(
        monkeypatch,
        [1, 4, 1, -1, -1, 1, 2, 1, -1, -1],
        highs=top_highs,
    )

    assert int(top["macd_top_divergence"].sum()) == 1
    assert int(top.loc[6, "macd_top_divergence"]) == 1
    assert int(top["macd_top_confirmed"].sum()) == 1
    assert int(top.loc[8, "macd_top_confirmed"]) == 1
    assert top.loc[8, "macd_hist"] < 0

    bottom_lows = [10, 5, 6, 10, 10, 10, 4, 5, 10, 10]
    bottom = run_with_macd_hist(
        monkeypatch,
        [-1, -4, -1, 1, 1, -1, -2, -1, 1, 1],
        lows=bottom_lows,
    )

    assert int(bottom["macd_bottom_divergence"].sum()) == 1
    assert int(bottom.loc[6, "macd_bottom_divergence"]) == 1
    assert int(bottom["macd_bottom_confirmed"].sum()) == 1
    assert int(bottom.loc[8, "macd_bottom_confirmed"]) == 1
    assert bottom.loc[8, "macd_hist"] > 0


def test_macd_divergence_is_not_confirmed_without_reverse_flip(monkeypatch):
    top = run_with_macd_hist(
        monkeypatch,
        [1, 4, 1, -1, -1, 1, 2, 1, 0, 0],
        highs=[10, 15, 14, 10, 10, 10, 16, 14, 12, 11],
    )

    assert int(top["macd_top_confirmed"].sum()) == 0


def test_boll_indicators_mark_band_width_and_breakouts():
    frame = pd.DataFrame(
        {
            "candle_begin_time": pd.date_range("2026-01-01", periods=8, freq="4h"),
            "Open": [10, 10, 10, 10, 10, 13, 10, 7],
            "High": [10, 10, 10, 10, 10, 13, 10, 7],
            "Low": [10, 10, 10, 10, 10, 13, 10, 7],
            "Close": [10, 10, 10, 10, 10, 13, 10, 7],
            "Volume": np.ones(8),
        }
    )

    result = detector.add_boll_indicators(frame, length=3, mult=1.0, width=1.0)

    assert {
        "boll_upper",
        "boll_middle",
        "boll_lower",
        "boll_width_pct",
        "boll_width_flag",
        "boll_width_signal",
        "boll_width_below_signal",
        "boll_breakout_signal",
    }.issubset(result.columns)
    assert int(result.loc[5, "boll_breakout_signal"]) == 1
    assert int(result.loc[7, "boll_breakout_signal"]) == -1
    assert int(result["boll_width_signal"].sum()) >= 1


def test_boll_width_below_marker_uses_first_cross_below_threshold():
    frame = pd.DataFrame(
        {
            "candle_begin_time": pd.date_range("2026-01-01", periods=8, freq="4h"),
            "Open": [10, 10, 10, 10, 10, 10.1, 10.1, 10.1],
            "High": [10, 10, 10, 10, 10, 10.1, 10.1, 10.1],
            "Low": [10, 10, 10, 10, 10, 10.1, 10.1, 10.1],
            "Close": [8, 12, 8, 12, 10, 10.1, 10.1, 10.1],
            "Volume": np.ones(8),
        }
    )

    result = detector.add_boll_indicators(frame, length=3, mult=1.0, width=5.0)

    assert int(result["boll_width_below_signal"].sum()) == 1
    assert int(result.loc[6, "boll_width_below_signal"]) == 1


def test_process_symbol_outputs_boll_and_macd_columns(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    output_dir = tmp_path / "output"
    data_dir.mkdir()
    output_dir.mkdir()

    rows = 90
    close = 100 + np.sin(np.arange(rows) / 3) * 5 + np.arange(rows) * 0.05
    frame = pd.DataFrame(
        {
            "candle_begin_time": pd.date_range("2026-01-01", periods=rows, freq="15min"),
            "open": close,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "volume": np.ones(rows),
        }
    )
    frame.to_csv(data_dir / "BTC-USDT-SWAP_15m.csv", index=False)
    monkeypatch.setattr(detector, "OUTPUT_DIR", str(output_dir))

    detector.process_symbol(
        "BTC",
        "15m",
        data_dir=str(data_dir),
        show_interactive=False,
        limit_bars=None,
    )

    csv_path = output_dir / "detector_BTC_15M_signals.csv"
    image_path = output_dir / "detector_BTC_15M_overview.png"
    saved = pd.read_csv(csv_path)

    assert image_path.exists()
    assert {
        "boll_upper",
        "boll_middle",
        "boll_lower",
        "boll_width_pct",
        "boll_width_flag",
        "boll_width_signal",
        "boll_width_below_signal",
        "boll_breakout_signal",
        "macd",
        "macd_signal",
        "macd_hist",
        "macd_top_divergence",
        "macd_bottom_divergence",
    }.issubset(saved.columns)


def test_plot_divergence_no_longer_requires_stochrsi_panel(tmp_path):
    rows = 60
    close = 100 + np.sin(np.arange(rows) / 3)
    frame = pd.DataFrame(
        {
            "candle_begin_time": pd.date_range("2026-01-01", periods=rows, freq="4h"),
            "Open": close,
            "High": close + 1,
            "Low": close - 1,
            "Close": close,
            "Volume": np.ones(rows),
        }
    )
    frame = detector.add_boll_indicators(frame)
    frame = detector.add_macd_indicators(frame)

    fig, _ = detector.plot_divergence_fast(
        frame,
        pd.DataFrame(columns=frame.columns),
        pd.DataFrame(columns=frame.columns),
        "BTC",
        "4h",
        tmp_path / "chart.png",
    )

    detector.plt.close(fig)


def test_process_symbol_uses_daily_boll_width_threshold(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    output_dir = tmp_path / "output"
    data_dir.mkdir()
    output_dir.mkdir()

    rows = 120
    close = 100 + np.sin(np.arange(rows) / 3)
    frame = pd.DataFrame(
        {
            "candle_begin_time": pd.date_range("2026-01-01", periods=rows, freq="15min"),
            "open": close,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "volume": np.ones(rows),
        }
    )
    frame.to_csv(data_dir / "BTC-USDT-SWAP_15m.csv", index=False)
    monkeypatch.setattr(detector, "OUTPUT_DIR", str(output_dir))
    monkeypatch.setattr(detector, "plot_divergence_fast", lambda *args, **kwargs: (None, None))
    monkeypatch.setattr(detector.plt, "savefig", lambda path, *args, **kwargs: Path(path).touch())
    monkeypatch.setattr(detector.plt, "close", lambda *args, **kwargs: None)

    calls = []
    original = detector.add_boll_indicators

    def spy_add_boll_indicators(df, length=10, mult=3.0, width=1.0):
        calls.append(width)
        return original(df, length=length, mult=mult, width=width)

    monkeypatch.setattr(detector, "add_boll_indicators", spy_add_boll_indicators)

    detector.process_symbol(
        "BTC",
        "1d",
        data_dir=str(data_dir),
        show_interactive=False,
        limit_bars=None,
    )

    assert calls == [6.0]
