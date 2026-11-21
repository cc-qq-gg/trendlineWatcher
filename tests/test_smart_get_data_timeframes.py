"""smart_get_data 的 base_timeframe 行为测试。

页面最小展示 4H，取数也应以 4H 为基础周期：只生成 4H 及更粗的周期，
不再为了 4H 去抓 16 倍的 15m，也不依赖 15m 文件的完整性。
这里把本地数据与网络抓取都替换成替身，只验证"周期列表与产物"这一层。
"""

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import get_data


def _frame(end, rows, freq):
    times = pd.date_range(end=end, periods=rows, freq=freq)
    close = 100.0 + np.arange(rows) * 0.01
    return pd.DataFrame(
        {
            "candle_begin_time": times,
            "open": close,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "volume": np.ones(rows),
        }
    )


def _install_fakes(monkeypatch, tmp_path, *, base_suffix="4H", freq="4h", rows=600):
    """伪造本地文件与网络抓取，返回 (data_dir, csv_dir, local_csv)。"""
    data_dir = tmp_path / "data"
    csv_dir = data_dir / "okx" / "csv"
    csv_dir.mkdir(parents=True)

    end = pd.Timestamp(datetime.now().replace(minute=0, second=0, microsecond=0))
    base = _frame(end, rows, freq)
    local_csv = csv_dir / f"BTC-USDT-SWAP_{base_suffix}.csv"
    base.to_csv(local_csv, index=False)

    local_info = {
        "file_path": str(local_csv),
        "exists": True,
        "row_count": len(base),
        "time_range": (
            base["candle_begin_time"].iloc[0],
            base["candle_begin_time"].iloc[-1],
        ),
        "has_missing": False,
        "missing_count": 0,
        "file_size_kb": 1.0,
        "completeness_rate": 1.0,
    }
    monkeypatch.setattr(get_data, "get_local_data_info", lambda *args, **kwargs: dict(local_info))

    # 只用于"更新最新 10 根"的分支；返回与本地同口径的连续数据
    latest = _frame(end, 10, freq)
    monkeypatch.setattr(get_data, "get_kline", lambda *args, **kwargs: latest.copy())

    return data_dir, csv_dir, local_csv


def _run(data_dir, timeframe, base_timeframe):
    return get_data.smart_get_data(
        "BTC-USDT-SWAP",
        timeframe,
        required_days=90,
        data_dir=str(data_dir),
        verbose=False,
        base_timeframe=base_timeframe,
    )


def test_base_4h_generates_only_4h_and_coarser(monkeypatch, tmp_path):
    data_dir, csv_dir, _ = _install_fakes(monkeypatch, tmp_path)

    result = _run(data_dir, "4H", "4H")

    assert list(result["all_timeframes"].keys()) == ["4H", "1D", "1W"]
    assert result["status"] == "success"


def test_base_4h_never_writes_a_15m_file(monkeypatch, tmp_path):
    data_dir, csv_dir, _ = _install_fakes(monkeypatch, tmp_path)

    _run(data_dir, "4H", "4H")

    written = sorted(path.name for path in csv_dir.glob("BTC-USDT-SWAP_*.csv"))
    assert written == [
        "BTC-USDT-SWAP_1D.csv",
        "BTC-USDT-SWAP_1W.csv",
        "BTC-USDT-SWAP_4H.csv",
    ]


def test_base_1d_generates_only_daily_and_weekly(monkeypatch, tmp_path):
    data_dir, _, _ = _install_fakes(monkeypatch, tmp_path, base_suffix="1D", freq="D", rows=200)

    result = _run(data_dir, "1D", "1D")

    assert list(result["all_timeframes"].keys()) == ["1D", "1W"]


def test_default_base_timeframe_keeps_15m_chain(monkeypatch, tmp_path):
    """不传 base_timeframe 时保持旧行为，stochrsi 等调用方仍按 15m 工作。"""
    data_dir, _, _ = _install_fakes(
        monkeypatch, tmp_path, base_suffix="15m", freq="15min", rows=200
    )

    result = get_data.smart_get_data(
        "BTC-USDT-SWAP",
        "15m",
        required_days=1,
        data_dir=str(data_dir),
        verbose=False,
    )

    assert list(result["all_timeframes"].keys()) == ["15m", "4H", "1D", "1W"]


def test_unknown_base_timeframe_is_prepended(monkeypatch, tmp_path):
    data_dir, _, _ = _install_fakes(monkeypatch, tmp_path, base_suffix="2H", freq="2h", rows=600)

    result = _run(data_dir, "2H", "2H")

    assert list(result["all_timeframes"].keys()) == ["2H", "15m", "4H", "1D", "1W"]


def test_requested_timeframe_is_case_insensitive(monkeypatch, tmp_path):
    data_dir, _, _ = _install_fakes(monkeypatch, tmp_path)

    result = _run(data_dir, "1d", "4H")

    deltas = result["df"]["candle_begin_time"].diff().dropna().unique()
    assert list(deltas) == [pd.Timedelta(days=1)]


def test_daily_is_aggregated_from_4h_base(monkeypatch, tmp_path):
    data_dir, _, _ = _install_fakes(monkeypatch, tmp_path)

    result = _run(data_dir, "4H", "4H")

    rows_4h = result["all_timeframes"]["4H"]["rows"]
    rows_1d = result["all_timeframes"]["1D"]["rows"]
    # 每 6 根 4H 合成 1 根日线
    assert abs(rows_4h / 6 - rows_1d) <= 2


def _install_covered_local(monkeypatch, tmp_path, covered):
    """造一份以 covered 结尾的 4H 本地数据，并统计网络调用。"""
    data_dir = tmp_path / "data"
    csv_dir = data_dir / "okx" / "csv"
    csv_dir.mkdir(parents=True)

    frame = _frame(covered, 600, "4h")
    local_csv = csv_dir / "BTC-USDT-SWAP_4H.csv"
    frame.to_csv(local_csv, index=False)

    monkeypatch.setattr(
        get_data,
        "get_local_data_info",
        lambda *args, **kwargs: {
            "file_path": str(local_csv),
            "exists": True,
            "row_count": len(frame),
            "time_range": (
                frame["candle_begin_time"].iloc[0],
                frame["candle_begin_time"].iloc[-1],
            ),
            "has_missing": False,
            "missing_count": 0,
            "file_size_kb": 1.0,
            "completeness_rate": 1.0,
        },
    )

    calls = []
    monkeypatch.setattr(
        get_data,
        "get_kline",
        lambda *args, **kwargs: calls.append(kwargs) or pd.DataFrame(),
    )
    return data_dir, calls


def test_skips_network_when_local_covers_current_bucket(monkeypatch, tmp_path):
    covered = get_data.bucket_start(datetime.now(), 4 * 3600)
    data_dir, calls = _install_covered_local(monkeypatch, tmp_path, covered)

    result = _run(data_dir, "4H", "4H")

    # 本 4H 桶内不会再有新 K，必须一次网络请求都不发
    assert calls == []
    assert result["status"] == "success"


def test_fetches_when_local_is_one_bucket_behind(monkeypatch, tmp_path):
    stale = get_data.bucket_start(datetime.now(), 4 * 3600) - pd.Timedelta(hours=4)
    data_dir, calls = _install_covered_local(monkeypatch, tmp_path, stale)

    _run(data_dir, "4H", "4H")

    assert len(calls) >= 1, "跨桶后必须联网补最新一根"
