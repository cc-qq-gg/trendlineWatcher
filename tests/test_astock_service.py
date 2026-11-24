"""service 层：预设与显式参数的优先级、结果缓存与刷新失效。"""

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from astock import data, momentum, service


@pytest.fixture()
def library(tmp_path):
    """三列日线就够跑轮动；关系图的读法由 test_astock_relation 负责。"""
    days = pd.bdate_range("2019-01-01", periods=300)
    rng = np.random.default_rng(3)
    rows = []
    for code, step in ((momentum.POOL[0], 0.001), (momentum.POOL[1], 0.0006), (momentum.BOND, 0.00005)):
        price = 100.0
        for day in days:
            price *= 1.0 + step + rng.normal(0, 0.007)
            rows.append((day.strftime("%Y-%m-%d"), code, round(price, 4)))
    path = tmp_path / "index_daily.csv"
    pd.DataFrame(rows, columns=data.CSV_COLUMNS).to_csv(path, index=False)
    service._CLOSES.clear()
    service._RELATION.clear()
    service._MOMENTUM.clear()
    service._REFRESH.update(running=False, started_at=None, last=None, error=None)
    return path


def test_explicit_params_override_the_preset(library):
    base = service._resolve("reference", None, None, None, None)
    assert base == {"window": 20, "entry": 0.005, "exit_threshold": -0.005,
                    "cost_rate": momentum.COST_PER_SIDE}

    overridden = service._resolve("reference", 30, 0.02, 0.0, None)
    assert overridden == {"window": 30, "entry": 0.02, "exit_threshold": 0.0,
                          "cost_rate": momentum.COST_PER_SIDE}


def test_out_of_range_params_fail_before_computing(library):
    with pytest.raises(ValueError):
        service._resolve("optimized", 1, None, None, None)
    with pytest.raises(ValueError):
        service._resolve("nope", None, None, None, None)


def test_momentum_results_are_cached_per_parameter_set(library):
    first = service.momentum_view(preset="optimized", path=library)
    again = service.momentum_view(preset="optimized", path=library)
    other = service.momentum_view(preset="reference", path=library)

    assert again is first, "同参数必须命中缓存，连对象都不该重建"
    assert other is not first
    assert other["meta"]["label"] == momentum.PRESETS["reference"]["label"]


def test_refresh_invalidates_both_caches(library, monkeypatch):
    service.momentum_view(preset="optimized", path=library)
    service.closes(library)
    seen = {}

    def fake_sync(path, cutoff=None):
        seen["cutoff"] = cutoff
        return {"added": 0, "rows": 0}

    monkeypatch.setattr(data, "sync", fake_sync)

    assert service.refresh(library)["started"] is True
    for _ in range(200):
        if not service._REFRESH["running"]:
            break
        time.sleep(0.01)

    assert not service._MOMENTUM and not service._CLOSES, "数据换了，缓存必须全部作废"
    assert service._REFRESH["last"] == {"added": 0, "rows": 0}
    assert seen["cutoff"] == data.settled_cutoff(), "刷新必须带时间闸，盘中不许写当日"


def test_refresh_records_worker_failure(library, monkeypatch):
    def boom(path, cutoff=None):
        raise RuntimeError("网络断了")

    monkeypatch.setattr(data, "sync", boom)

    assert service.refresh(library)["started"] is True
    for _ in range(200):
        if not service._REFRESH["running"]:
            break
        time.sleep(0.01)

    assert "网络断了" in service._REFRESH["error"], "失败要落到状态里给前端看，不能抛进线程里丢掉"
    assert service._REFRESH["running"] is False
