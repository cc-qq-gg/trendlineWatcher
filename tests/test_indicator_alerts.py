import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import watcher_stochrsi as watcher


def alert_frame():
    return pd.DataFrame(
        {
            "datetime": pd.date_range("2026-01-01", periods=4, freq="4h"),
            "open": [100, 101, 102, 103],
            "high": [101, 102, 105, 104],
            "low": [99, 100, 101, 102],
            "close": [100, 101, 104, 103],
            "volume": np.ones(4),
            "boll_width_pct": [8.0, 6.0, 0.9, 0.8],
            "boll_width_below_signal": [0, 0, 1, 0],
            "boll_breakout_signal": [np.nan, np.nan, np.nan, np.nan],
            "macd_hist": [1.0, 2.0, 0.5, 0.2],
            "macd_top_divergence": [0, 0, 1, 0],
            "macd_bottom_divergence": [0, 0, 0, 0],
            "macd_top_confirmed": [0, 0, 1, 0],
            "macd_bottom_confirmed": [0, 0, 0, 0],
        }
    )


def test_build_indicator_alerts_describes_confirmed_macd_and_width_signals():
    alerts = watcher.build_indicator_alerts(
        alert_frame(),
        symbol="BTC-USDT-SWAP",
        time_interval="4H",
    )

    subjects = [alert["subject"] for alert in alerts]
    contents = [alert["content"] for alert in alerts]

    assert "BTC-4H 顶背离，注意上方压力" in subjects
    assert "BTC-4H W收缩，等待方向突破" in subjects
    assert "顶背离：上涨动能减弱，注意上方压力。" in contents
    assert "W收缩：波动率压缩，等待方向突破。" in contents
    assert all("时间:" not in content for content in contents)
    assert all("价格:" not in content for content in contents)


def test_build_indicator_alerts_does_not_send_before_macd_confirmation():
    frame = alert_frame()
    frame["macd_top_confirmed"] = 0
    alerts = watcher.build_indicator_alerts(
        frame,
        symbol="BTC-USDT-SWAP",
        time_interval="4H",
    )

    assert "BTC-4H 顶背离，注意上方压力" not in [alert["subject"] for alert in alerts]


def test_watch_sends_new_indicator_alerts_from_confirmed_candle(tmp_path, monkeypatch):
    frame = alert_frame().rename(columns={"datetime": "candle_begin_time"})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(watcher, "smart_get_data", lambda **kwargs: {"df": frame.copy()})
    monkeypatch.setattr(watcher, "add_boll_indicators", lambda df, width: df)
    monkeypatch.setattr(watcher, "add_macd_indicators", lambda df: df)
    monkeypatch.setattr(watcher, "show_notification", lambda *args, **kwargs: None)

    sent = []
    monkeypatch.setattr(
        watcher,
        "send_email",
        lambda subject="", content="": sent.append((subject, content)),
    )

    watcher.watch(time_interval="4H", symbol="BTC-USDT-SWAP")

    subjects = [subject for subject, _ in sent]
    assert subjects == [
        "BTC-4H 顶背离，注意上方压力",
        "BTC-4H W收缩，等待方向突破",
    ]
    assert (tmp_path / "data" / "indicator_alerts" / "boll_macd_BTC-USDT-SWAP_4H.csv").exists()
