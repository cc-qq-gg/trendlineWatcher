import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import TrendlineWebApp as webapp
from divergence_visualizer import dxy as dxy_module
from divergence_visualizer import payload as payload_module
from divergence_visualizer import weekly as weekly_module

CSV_COLUMNS = [
    "candle_begin_time", "Open", "High", "Low", "Close", "Volume", "stochrsi",
    "boll_upper", "boll_middle", "boll_lower", "boll_width_pct", "boll_width_flag",
    "boll_width_signal", "boll_width_below_signal", "boll_breakout_signal",
    "macd", "macd_signal", "macd_hist", "macd_top_divergence", "macd_bottom_divergence",
    "macd_divergence_version", "tbl", "dbl",
]


def login(client):
    with client.session_transaction() as sess:
        sess["authenticated"] = True
        sess["username"] = "trendline"


def make_client():
    webapp.app.config["TESTING"] = True
    client = webapp.app.test_client()
    login(client)
    return client


def build_fixture_frame():
    """6 根 1D K线，五类标记各一个，用于对账 counts 与 markers。"""
    rows = []
    for i in range(6):
        rows.append({
            "candle_begin_time": f"2026-01-0{i + 1} 00:00:00",
            "Open": 100.0 + i,
            "High": 110.0 + i,
            "Low": 90.0 + i,
            "Close": 105.0 + i,
            "Volume": 1000 + i,
            "stochrsi": 50.0,
            "boll_upper": 120.0,
            "boll_middle": 105.0,
            "boll_lower": 90.0,
            "boll_width_pct": 28.5,
            "boll_width_flag": 1,
            "boll_width_signal": 0,
            "boll_width_below_signal": 1 if i == 2 else 0,
            "boll_breakout_signal": None,
            "macd": 1.5,
            "macd_signal": 1.2,
            "macd_hist": 0.3,
            "macd_top_divergence": 1 if i == 0 else 0,
            "macd_bottom_divergence": 1 if i == 3 else 0,
            "macd_divergence_version": 4,
            "tbl": -1.0 if i == 1 else None,
            "dbl": 1.0 if i == 4 else None,
        })
    return pd.DataFrame(rows, columns=CSV_COLUMNS)


@pytest.fixture
def fake_output(tmp_path, monkeypatch):
    build_fixture_frame().to_csv(tmp_path / "detector_TST_1D_signals.csv", index=False)
    monkeypatch.setattr(payload_module, "OUTPUT_DIR", str(tmp_path))
    return tmp_path


def test_divergence_chart_requires_login():
    webapp.app.config["TESTING"] = True
    client = webapp.app.test_client()

    response = client.get("/api/divergence/chart/BTC/1d")

    assert response.status_code == 401


def test_divergence_chart_payload_matches_csv_signals(fake_output):
    client = make_client()

    response = client.get("/api/divergence/chart/TST/1d")
    body = response.get_json()

    assert response.status_code == 200
    assert body["success"] is True
    assert body["counts"] == {
        "macd_top": 1, "macd_bottom": 1, "tbl": 1, "dbl": 1,
        "boll_w": 1, "boll_break_up": 0, "boll_break_down": 0,
    }
    # 五个标记，且时间升序
    assert len(body["markers"]) == 5
    marker_times = [m["t"] for m in body["markers"]]
    assert marker_times == sorted(marker_times)
    # 指标数组与K线等长，且首根 NaN 转成 null
    for key in ("boll_up", "boll_mid", "boll_low", "macd", "macd_signal", "macd_hist"):
        assert len(body["ind"][key]) == 6
    # 顶背离标在高点、底背离标在低点
    top = next(m for m in body["markers"] if m["kind"] == "macd_top")
    bottom = next(m for m in body["markers"] if m["kind"] == "dbl")
    assert top["price"] == 110.0
    assert bottom["price"] == 94.0
    assert body["meta"]["time_basis"] == "Asia/Shanghai"


def test_divergence_chart_rejects_unknown_timeframe(fake_output):
    client = make_client()

    response = client.get("/api/divergence/chart/TST/1h")

    assert response.status_code == 400


def test_payload_keeps_null_for_missing_indicator(tmp_path, monkeypatch):
    frame = build_fixture_frame()
    frame.loc[0, "boll_upper"] = None
    frame.loc[0, "macd"] = None
    frame.to_csv(tmp_path / "detector_NUL_1D_signals.csv", index=False)
    monkeypatch.setattr(payload_module, "OUTPUT_DIR", str(tmp_path))

    payload = payload_module.build_chart_payload("NUL", "1d")

    assert payload["ind"]["boll_up"][0] is None
    assert payload["ind"]["macd"][0] is None
    assert payload["ind"]["boll_up"][1] == 120.0


@pytest.fixture
def dxy_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(dxy_module, "CACHE_DIR", str(tmp_path))
    monkeypatch.setattr(dxy_module, "CACHE_PATH", str(tmp_path / "dxy_weekly.csv"))
    return tmp_path


def weekly_frame(days):
    rows = []
    for day in days:
        rows.append({
            "date": pd.Timestamp(day),
            "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5,
        })
    return pd.DataFrame(rows)


def test_dxy_window_follows_btc_daily_range(dxy_cache, monkeypatch):
    weeks = weekly_frame([
        "2025-07-27", "2025-08-03", "2025-08-10",  # 起始周在窗口内
        "2026-09-13", "2026-09-20", "2026-09-27",  # 末尾周被 end 截断
    ])
    monkeypatch.setattr(dxy_module, "fetch_cnbc_weekly", lambda: weeks)

    result = dxy_module.get_weekly_bars("2025-08-07", "2026-09-20", force=True)

    assert result["success"] is True
    assert result["source"] == "cnbc"
    # start 所在的那一周（2025-08-03）要保留，end 之后的周（2026-09-27）要丢掉
    assert [pd.Timestamp(b["t"], unit="s").strftime("%Y-%m-%d") for b in result["bars"]] == [
        "2025-08-03", "2025-08-10", "2026-09-13", "2026-09-20",
    ]


def test_dxy_uses_cache_when_network_fails(dxy_cache, monkeypatch):
    weeks = weekly_frame(["2025-08-03", "2025-08-10"])
    dxy_module.save_cache(weeks, "cnbc", datetime.now() - timedelta(hours=30))

    def boom():
        raise RuntimeError("network down")

    monkeypatch.setattr(dxy_module, "fetch_cnbc_weekly", boom)

    result = dxy_module.get_weekly_bars("2025-08-07", "2025-08-20", force=True)

    assert result["success"] is True
    assert result["source"] == "cnbc"
    assert result["stale"] is True
    assert len(result["bars"]) == 2


def test_dxy_reports_failure_when_no_source(dxy_cache, monkeypatch):
    def boom():
        raise RuntimeError("network down")

    def boom_ecb(start, end):
        raise RuntimeError("ecb down")

    monkeypatch.setattr(dxy_module, "fetch_cnbc_weekly", boom)
    monkeypatch.setattr(dxy_module, "synthesize_from_ecb", boom_ecb)

    result = dxy_module.get_weekly_bars("2025-08-07", "2025-08-20", force=True)

    assert result["success"] is False
    assert "美元指数" in result["message"]


def unix(day: str) -> int:
    return int(pd.Timestamp(day, tz="UTC").timestamp())


def weekly_bars(days, close=100.0):
    return [
        {"t": unix(day), "o": close, "h": close, "l": close, "c": close}
        for day in days
    ]


def test_weekly_payload_aligns_both_sides_to_monday(monkeypatch):
    """DXY 按周日标注、BTC 按周一开始，叠加前必须统一到同一周的周一。"""
    monkeypatch.setattr(
        dxy_module, "get_weekly_bars",
        lambda **kwargs: {
            "success": True, "source": "cnbc",
            "bars": weekly_bars(["2026-11-01", "2026-11-08"]), "partial_last": False,
        },
    )
    monkeypatch.setattr(
        weekly_module, "get_btc_weekly",
        lambda **kwargs: {
            "success": True, "source": "okx",
            "bars": weekly_bars(["2026-11-02", "2026-11-09"]), "partial_last": True,
        },
    )

    payload = webapp._weekly_payload()

    for key in ("dxy", "btc_weekly"):
        days = [datetime.fromtimestamp(b["t"], timezone.utc) for b in payload[key]["bars"]]
        assert all(day.weekday() == 0 for day in days)
    # 周日的 DXY 11-08 前移后落在 11-02，与 BTC 的 11-02 完全重合
    assert payload["dxy"]["bars"][-1]["t"] == payload["btc_weekly"]["bars"][0]["t"]
    assert payload["dxy"]["window"] == {"from": "2026-10-26", "to": "2026-11-02"}


def test_weekly_bars_returns_tail_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(weekly_module, "CACHE_DIR", str(tmp_path))
    mondays = ["2026-10-05", "2026-10-12", "2026-10-19", "2026-10-26", "2026-11-02"]
    frame = pd.DataFrame({
        "date": pd.to_datetime(mondays),
        "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
    })
    weekly_module.save_cache(frame, "okx", datetime.now())

    result = weekly_module.get_btc_weekly(limit=3, allow_network=False)

    assert result["success"] is True
    assert len(result["bars"]) == 3
    assert result["bars"][-1]["t"] == unix("2026-11-02")
    assert result["bars"][0]["t"] == unix("2026-10-19")


def test_weekly_uses_cache_when_network_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(weekly_module, "CACHE_DIR", str(tmp_path))
    frame = pd.DataFrame({
        "date": pd.to_datetime(["2026-10-26", "2026-11-02"]),
        "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
    })
    weekly_module.save_cache(frame, "okx", datetime.now() - timedelta(hours=30))

    def boom(*args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(weekly_module, "fetch_okx_weekly", boom)

    result = weekly_module.get_btc_weekly(limit=200, force=True)

    assert result["success"] is True
    assert result["source"] == "okx"
    assert result["stale"] is True
    assert len(result["bars"]) == 2
