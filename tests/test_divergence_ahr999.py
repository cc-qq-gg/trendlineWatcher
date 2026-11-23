"""AHR999 口径与缓存行为的测试。

覆盖：
1. 200 日定投成本是调和平均（原文明确更正过，不能用算术/几何均值代替）；
2. 指数增长估值 = 10^(5.84·log10(币龄) − 17.01)；
3. ahr999 = (P/DCA200)×(P/V)，分区边界；
4. 周序列锚点必须是周一，且取该周最后一个交易日的值；
5. 缓存：新鲜命中、force 重算、请求路径只读缓存、缺源文件不抛异常；
6. 5m 缺尾部时由永续 1D 补齐，且不覆盖已有的现货日期。
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from divergence_visualizer import ahr999 as ahr


@pytest.fixture
def cache_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(ahr, "CACHE_DIR", str(tmp_path))
    monkeypatch.setattr(ahr, "CACHE_PATH", str(tmp_path / "ahr999_weekly.csv"))
    monkeypatch.setattr(ahr, "SPOT_5M_PATH", str(tmp_path / "spot_5m.csv"))
    monkeypatch.setattr(ahr, "PERP_1D_PATH", str(tmp_path / "perp_1d.csv"))
    return tmp_path


def daily_series(closes, start="2024-01-01"):
    index = pd.date_range(start, periods=len(closes), freq="D", tz="UTC")
    return pd.Series([float(c) for c in closes], index=index)


def write_5m(path, days):
    """days: [(日期, 收盘)]，每天写两行，日线取最后一根。"""
    rows = []
    for day, close in days:
        stamp = pd.Timestamp(day, tz="UTC")
        rows.append({"candle_begin_time": (stamp + pd.Timedelta(hours=1)).isoformat(), "close": close * 0.99})
        rows.append({"candle_begin_time": (stamp + pd.Timedelta(hours=13)).isoformat(), "close": close})
    pd.DataFrame(rows).to_csv(path, index=False)


def write_1d(path, days):
    rows = [
        {"candle_begin_time": pd.Timestamp(day, tz="UTC").isoformat(), "close": close}
        for day, close in days
    ]
    pd.DataFrame(rows).to_csv(path, index=False)


def test_dca200_is_harmonic_mean_not_arithmetic():
    closes = [30000 + (i % 37) * 250 for i in range(260)]
    frame = ahr.compute_ahr999(daily_series(closes))
    last = frame.iloc[-1]

    window = np.array(closes[-200:], dtype=float)
    harmonic = 200 / np.sum(1 / window)
    arithmetic = window.mean()

    assert last["dca200"] == pytest.approx(harmonic, rel=1e-9)
    assert abs(last["dca200"] - arithmetic) > 1.0, "调和平均必须与算术均值有可见差异"


def test_valuation_follows_power_law_of_coin_age():
    closes = [20000 + i for i in range(230)]
    frame = ahr.compute_ahr999(daily_series(closes))
    last = frame.iloc[-1]

    age = (pd.Timestamp(frame["date"].iloc[-1]) - ahr.GENESIS).days
    expected = 10 ** (5.84 * np.log10(age) - 17.01)
    assert last["valuation"] == pytest.approx(expected, rel=1e-9)


def test_ahr999_multiplies_both_ratios_and_classifies_zones():
    frame = ahr.compute_ahr999(daily_series([40000 - i * 50 for i in range(240)]))
    last = frame.iloc[-1]
    expected = (last["price"] / last["dca200"]) * (last["price"] / last["valuation"])
    assert last["ahr999"] == pytest.approx(expected, rel=1e-9)

    assert ahr.zone_of(0.30) == "抄底区"
    assert ahr.zone_of(0.45) == "定投区"
    assert ahr.zone_of(1.20) == "起飞区"
    assert ahr.zone_of(5.00) == "高估区"
    assert ahr.zone_of(9.9) == "高估区"


def test_weekly_anchor_is_monday_and_keeps_last_day_of_week():
    # 2026-11-02 是周一；周一~周日各给一个不同价格
    days = pd.date_range("2026-11-02", periods=14, freq="D", tz="UTC")
    frame = pd.DataFrame(
        {
            "date": days,
            "price": np.arange(1.0, 15.0),
            "dca200": np.arange(1.0, 15.0),
            "valuation": np.arange(1.0, 15.0),
            "ahr999": np.arange(1.0, 15.0),
        }
    )

    weekly = ahr.to_weekly(frame)

    assert len(weekly) == 2
    assert all(pd.Timestamp(d).weekday() == 0 for d in weekly["date"])
    # 第一周取周日（11-08，第 7 行）的值
    assert weekly["ahr999"].iloc[0] == pytest.approx(7.0)
    assert weekly["ahr999"].iloc[1] == pytest.approx(14.0)


def test_daily_close_fills_tail_from_perp_without_overwriting_spot(cache_paths, monkeypatch):
    write_5m(cache_paths / "spot_5m.csv", [("2026-10-01", 100.0), ("2026-10-02", 101.0)])
    write_1d(
        cache_paths / "perp_1d.csv",
        [("2026-10-02", 999.0), ("2026-10-03", 103.0), ("2026-10-04", 104.0)],
    )

    daily = ahr.daily_close()

    assert list(daily.values) == pytest.approx([100.0, 101.0, 103.0, 104.0])
    assert daily.loc[pd.Timestamp("2026-10-02", tz="UTC")] == pytest.approx(101.0)


def test_missing_sources_returns_failure_without_raising(cache_paths):
    result = ahr.get_weekly_series(limit=200, force=True)

    assert result["success"] is False
    assert result["needs_build"] is True
    assert "失败" in result["message"] or "缓存" in result["message"]


def test_request_path_reads_cache_only(cache_paths, monkeypatch):
    weekly = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-11-02", "2026-11-09"]),
            "price": [60000.0, 61000.0],
            "dca200": [70000.0, 70500.0],
            "valuation": [180000.0, 181000.0],
            "ahr999": [0.28, 0.29],
        }
    )
    ahr.save_cache(weekly, datetime.now())

    def boom():
        raise AssertionError("请求路径不应读取 5m 全表")

    monkeypatch.setattr(ahr, "daily_close", boom)

    result = ahr.get_weekly_series(limit=200, allow_rebuild=False)

    assert result["success"] is True
    assert result["source"] == "cache"
    assert result["count"] == 2
    assert result["latest"]["zone"] == "抄底区"


def test_force_rebuild_overrides_fresh_cache(cache_paths, monkeypatch):
    stale = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-11-02"]),
            "price": [1.0],
            "dca200": [1.0],
            "valuation": [1.0],
            "ahr999": [9.99],
        }
    )
    ahr.save_cache(stale, datetime.now())

    closes = [30000.0 + i for i in range(210)]
    monkeypatch.setattr(ahr, "daily_close", lambda: daily_series(closes, start="2026-04-01"))

    result = ahr.get_weekly_series(limit=200, force=True)

    assert result["success"] is True
    assert result["source"] == "rebuild"
    assert result["bars"][-1]["v"] != pytest.approx(9.99)


def test_stale_cache_is_flagged_but_still_served(cache_paths):
    weekly = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-11-02"]),
            "price": [60000.0],
            "dca200": [70000.0],
            "valuation": [180000.0],
            "ahr999": [0.28],
        }
    )
    ahr.save_cache(weekly, datetime.now() - timedelta(hours=ahr.CACHE_TTL_HOURS + 1))

    result = ahr.get_weekly_series(limit=200, allow_rebuild=False)

    assert result["success"] is True
    assert result["stale"] is True


def test_partial_last_flags_current_week(cache_paths):
    weekly = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-11-02", "2026-11-09"]),
            "price": [1.0, 1.0],
            "dca200": [1.0, 1.0],
            "valuation": [1.0, 1.0],
            "ahr999": [0.2, 0.2],
        }
    )
    ahr.save_cache(weekly, datetime.now())

    result = ahr.get_weekly_series(limit=200, allow_rebuild=False)

    # 末根 2026-11-09 早于"本周周一"时不算进行中
    expected = pd.Timestamp(result["latest"]["t"], unit="s") >= (
        pd.Timestamp(datetime.now().date()) - pd.Timedelta(days=datetime.now().weekday())
    )
    assert result["partial_last"] is expected


def test_zones_use_null_for_unbounded_upper(cache_paths):
    """inf 会被序列化成非法 JSON 字面量 Infinity，浏览器 JSON.parse 直接失败。"""
    weekly = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-11-02"]),
            "price": [60000.0],
            "dca200": [70000.0],
            "valuation": [180000.0],
            "ahr999": [6.5],
        }
    )
    ahr.save_cache(weekly, datetime.now())

    result = ahr.get_weekly_series(limit=200, allow_rebuild=False)

    json.dumps(result, allow_nan=False)  # 含 inf/nan 会抛 ValueError
    assert result["zones"][-1] == {"label": "高估区", "low": 5.0, "high": None}
    assert result["zones"][0]["high"] == pytest.approx(0.45)
    assert result["latest"]["zone"] == "高估区"
