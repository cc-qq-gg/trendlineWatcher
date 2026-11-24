"""JSON 契约：时间键还原、非有限数过滤、序列白名单。"""

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from astock import data, payload


def test_time_ms_round_trips_to_the_same_calendar_day():
    """t 是交易日零点的 UTC 毫秒，前端 slice(0, 10) 必须还原成原日期。

    回归：pandas 3 的 DatetimeIndex 是 datetime64[us]，拿 asi8 除 1e6 会得到秒。
    """
    dates = pd.bdate_range("2020-01-01", periods=3)
    stamps = payload.time_ms(dates)

    assert all(stamp > 10 ** 12 for stamp in stamps), "毫秒戳必须是 13 位量级"
    for stamp, day in zip(stamps, dates):
        restored = datetime.fromtimestamp(stamp / 1000, tz=UTC).strftime("%Y-%m-%d")
        assert restored == day.strftime("%Y-%m-%d")
    # datetime64 数组走同一条路，结果必须一致
    assert payload.time_ms(dates.to_numpy()) == stamps
    assert payload.time_ms(dates.astype("datetime64[ns]")) == stamps


def test_points_turn_non_finite_into_none_and_round_to_four():
    dates = pd.bdate_range("2020-01-01", periods=3).to_numpy()
    points = payload.points(dates, np.array([1.234567, np.nan, np.inf]))

    assert [point["v"] for point in points] == [1.2346, None, None]
    assert "raw" not in points[0]
    json.dumps(points, allow_nan=False)


def test_points_attach_raw_readings_for_percentile_series():
    dates = pd.bdate_range("2020-01-01", periods=2).to_numpy()
    points = payload.points(dates, np.array([50.0, 66.66666]), raw=np.array([12.0, np.nan]))

    assert [point["v"] for point in points] == [50.0, 66.6667]
    assert [point["raw"] for point in points] == [12.0, None]


def test_round_numbers_makes_the_payload_rfc8259_safe():
    """JSON 不允许 NaN/Infinity，一旦漏出去前端 JSON.parse 直接白屏。"""
    dirty = {
        "cagr": np.float64(0.3013456789),
        "sharpe": float("nan"),
        "maxdd": float("-inf"),
        "switches": np.int64(229),
        "flag": True,
        "note": None,
        "nested": [{"v": np.float32(1.5)}, (np.float64(2.0),)],
    }

    clean = payload.round_numbers(dirty)

    assert clean["cagr"] == 0.301346
    assert clean["sharpe"] is None and clean["maxdd"] is None
    assert clean["switches"] == 229 and clean["flag"] is True
    assert clean["nested"] == [{"v": 1.5}, [2.0]]
    json.dumps(clean, allow_nan=False)


def test_catalog_exposes_flags_but_not_collection_details():
    catalog = {item["code"]: item for item in payload.catalog()}

    assert catalog["HS300_ERP"] == {
        "code": "HS300_ERP", "name": "股债性价比（ERP）", "display": "percentile", "axis": "right",
        "unit": "%", "group": "估值", "internal": False, "derived": True,
        "note": "100/沪深300PE − 10Y国债，越高代表股票相对债券越便宜；现算不落库",
    }
    assert "secid" not in catalog["sh.000001"]
    assert set(catalog) == set(data.SERIES)
