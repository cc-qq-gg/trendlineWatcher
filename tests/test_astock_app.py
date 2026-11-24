"""Flask 接口：正常路径、参数校验分支与后台刷新（临时 CSV，采集被替换，不联网）。"""

import gzip
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from astock import app as app_module
from astock import data, momentum, relation, service


@pytest.fixture()
def client(tmp_path, monkeypatch):
    """搭一份够跑两个视图的 CSV，并把默认库指过去。"""
    days = pd.bdate_range("2019-01-01", periods=400)
    drift = {
        momentum.POOL[0]: 0.0012, momentum.POOL[1]: 0.0009, momentum.BOND: 0.00005,
        "sh.000001": 0.0004, "sh.000300": 0.0006,
    }
    rng = np.random.default_rng(11)
    rows = []
    for code, step in drift.items():
        price = 100.0
        for day in days:
            price *= 1.0 + step + rng.normal(0, 0.008)
            rows.append((day.strftime("%Y-%m-%d"), code, round(price, 4)))
    rows += [(day.strftime("%Y-%m-%d"), "HS300_PE", round(12.0 + 0.4 * np.sin(i / 9.0), 4))
             for i, day in enumerate(days[::5])]
    rows += [(day.strftime("%Y-%m-%d"), "CN10Y", round(2.4 + 0.01 * (i % 30), 4))
             for i, day in enumerate(days)]
    rows += [(day.strftime("%Y-%m-%d"), "PMI_CN", round(49.5 + (i % 5) * 0.4, 4))
             for i, day in enumerate(pd.date_range("2019-01-01", periods=36, freq="MS"))]

    path = tmp_path / "index_daily.csv"
    pd.DataFrame(rows, columns=data.CSV_COLUMNS).to_csv(path, index=False)

    monkeypatch.setattr(data, "DEFAULT_PATH", path)
    service._CLOSES.clear()
    service._RELATION.clear()
    service._MOMENTUM.clear()
    service._REFRESH.update(running=False, started_at=None, last=None, error=None)
    app_module.app.config.update(TESTING=True)
    return app_module.app.test_client()


def test_index_serves_the_static_page(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "A 股指数".encode() in resp.data


def test_meta_reports_catalog_presets_and_coverage(client):
    resp = client.get("/api/meta")
    body = resp.get_json()

    assert resp.status_code == 200
    assert body["mom_pool"] == list(momentum.POOL)
    assert [preset["key"] for preset in body["presets"]] == list(momentum.PRESETS)
    catalog = {item["code"]: item for item in body["catalog"]}
    assert catalog["HS300_ERP"]["derived"] is True and catalog["HS300_ERP"]["internal"] is False
    assert catalog["CN10Y"]["internal"] is True
    assert body["latest"] == days_end(body) and body["refresh"]["running"] is False
    assert body["relation_default_codes"] == list(relation.DEFAULT_CODES)


def days_end(body):
    ends = [row["end"] for row in body["coverage"] if row["end"]]
    return max(ends)


def test_relations_defaults_to_the_three_headline_series(client):
    resp = client.get("/api/relations")
    body = resp.get_json()

    assert resp.status_code == 200
    assert [item["code"] for item in body["series"]] == list(relation.DEFAULT_CODES)
    assert all(item["points"] for item in body["series"]), "临时库覆盖了默认三条（含现算的 ERP）"
    assert body["meta"]["percentile_years"] == service.DEFAULT_YEARS
    json.dumps(body, allow_nan=False)


def test_relations_still_serves_every_visible_series_when_asked(client):
    codes = ",".join(relation.visible_codes())
    body = client.get(f"/api/relations?codes={codes}").get_json()

    assert [item["code"] for item in body["series"]] == relation.visible_codes()


def test_momentum_matches_the_engine_and_is_json_safe(client):
    resp = client.get("/api/momentum")
    body = resp.get_json()

    assert resp.status_code == 200
    top = body["metrics"][0]
    assert top["code"] == "STRATEGY" and -1.0 < top["cagr"] < 5.0
    assert body["meta"]["window"] == momentum.WINDOW
    assert body["meta"]["preset"] == "optimized"
    assert len(body["metrics"]) == len(body["series"]) == len(momentum.ALL_CODES) + 1
    assert body["drawdown"] and all(point["v"] <= 0 for point in body["drawdown"])
    json.dumps(body, allow_nan=False)


def test_momentum_detail_flag_skips_the_heavy_blocks(client):
    body = client.get("/api/momentum?detail=0").get_json()
    assert body["series"] == [] and body["episodes"] == [] and body["sensitivity"] == []
    assert body["metrics"][0]["code"] == "STRATEGY"


def test_momentum_carries_span_returns_for_the_range_buttons(client):
    """周期按钮要在一次请求里就拿到全部区间的收益，不能靠前端再发 5 次回测。"""
    body = client.get("/api/momentum?years=1").get_json()

    spans = body["spans"]
    assert [row["years"] for row in spans] == list(momentum.SPAN_YEARS)
    one = next(row for row in spans if row["years"] == 1)
    assert one["total"] == pytest.approx(body["metrics"][0]["total"], abs=1e-9), "与当前周期读数同源"
    assert all(-1.0 < row["total"] < 100.0 for row in spans)


def test_relations_start_trims_the_payload(client):
    full = client.get("/api/relations").get_json()
    cut = client.get("/api/relations?start=2019-09-02").get_json()

    assert len(cut["series"][0]["points"]) < len(full["series"][0]["points"])
    assert cut["series"][0]["points"][0]["t"] > full["series"][0]["points"][0]["t"]


def test_momentum_years_selects_a_recent_window(client):
    """years 必须真的把区间截短：以前的默认 start 会把它整个吃掉。"""
    full = client.get("/api/momentum").get_json()
    one = client.get("/api/momentum?years=1").get_json()

    assert one["meta"]["start"] > full["meta"]["start"]
    assert one["meta"]["bars"] < full["meta"]["bars"]
    assert one["meta"]["years"] <= 1.1
    assert one["series"][0]["points"][0]["t"] > full["series"][0]["points"][0]["t"]


def test_momentum_explicit_start_wins_over_years(client):
    body = client.get("/api/momentum?start=2019-09-02&years=5").get_json()
    assert body["meta"]["start"] >= "2019-09-02"
    assert body["meta"]["years"] < 5


@pytest.mark.parametrize("target, query", [
    ("/api/momentum", "preset=nope"),
    ("/api/momentum", "window=1"),
    ("/api/momentum", "entry=abc"),
    ("/api/momentum", "exit_threshold=-9"),
    ("/api/relations", "years=1"),
    ("/api/relations", "codes=NOPE"),
])
def test_bad_arguments_return_400(client, target, query):
    resp = client.get(f"{target}?{query}")
    assert resp.status_code == 400
    assert resp.get_json()["error"]


def test_refresh_runs_in_the_background_without_network(client, monkeypatch):
    calls = []

    def fake_sync(path, cutoff=None):
        calls.append((path, cutoff))
        return {"added": 3, "rows": 9, "codes": 5, "end": "2020-07-01", "errors": {}, "elapsed_ms": 1.0}

    monkeypatch.setattr(service.data, "sync", fake_sync)

    assert client.post("/api/refresh").get_json()["started"] is True
    for _ in range(200):
        if not service._REFRESH["running"]:
            break
        time.sleep(0.01)

    assert calls, "采集必须真的被调用"
    assert calls[0][1] == data.settled_cutoff(), "后台刷新也要带时间闸"
    assert service._REFRESH["last"]["added"] == 3
    assert service._REFRESH["error"] is None
    assert client.get("/api/meta").get_json()["refresh_last"]["added"] == 3


def test_refresh_is_not_queued_twice(client, monkeypatch):
    monkeypatch.setattr(service, "_REFRESH", {"running": True, "started_at": None, "last": None, "error": None})
    body = client.post("/api/refresh").get_json()
    assert body == {"started": False, "reason": "已有刷新任务在跑"}


def test_json_is_gzipped_only_when_the_client_asks(client):
    plain = client.get("/api/momentum?years=1")
    assert "Content-Encoding" not in plain.headers

    packed = client.get("/api/momentum?years=1", headers={"Accept-Encoding": "gzip"})
    assert packed.headers["Content-Encoding"] == "gzip"
    assert packed.headers["Vary"] == "Accept-Encoding"
    assert int(packed.headers["Content-Length"]) < len(plain.data)
    assert gzip.decompress(packed.data) == plain.data
