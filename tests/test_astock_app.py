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
    resp = client.get("/biga/")
    assert resp.status_code == 200
    assert "A 股指数".encode() in resp.data


def test_meta_reports_catalog_presets_and_coverage(client):
    resp = client.get("/biga/api/meta")
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
    resp = client.get("/biga/api/relations")
    body = resp.get_json()

    assert resp.status_code == 200
    assert [item["code"] for item in body["series"]] == list(relation.DEFAULT_CODES)
    assert all(item["points"] for item in body["series"]), "临时库覆盖了默认三条（含现算的 ERP）"
    assert body["meta"]["percentile_years"] == service.DEFAULT_YEARS
    json.dumps(body, allow_nan=False)


def test_relations_still_serves_every_visible_series_when_asked(client):
    codes = ",".join(relation.visible_codes())
    body = client.get(f"/biga/api/relations?codes={codes}").get_json()

    assert [item["code"] for item in body["series"]] == relation.visible_codes()


def test_momentum_matches_the_engine_and_is_json_safe(client):
    resp = client.get("/biga/api/momentum")
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
    body = client.get("/biga/api/momentum?detail=0").get_json()
    assert body["series"] == [] and body["episodes"] == [] and body["sensitivity"] == []
    assert body["metrics"][0]["code"] == "STRATEGY"


def test_momentum_carries_span_returns_for_the_range_buttons(client):
    """周期按钮要在一次请求里就拿到全部区间的收益，不能靠前端再发 5 次回测。"""
    body = client.get("/biga/api/momentum?years=1").get_json()

    spans = body["spans"]
    assert [row["years"] for row in spans] == list(momentum.SPAN_YEARS)
    one = next(row for row in spans if row["years"] == 1)
    assert one["total"] == pytest.approx(body["metrics"][0]["total"], abs=1e-9), "与当前周期读数同源"
    assert all(-1.0 < row["total"] < 100.0 for row in spans)


def test_relations_start_trims_the_payload(client):
    full = client.get("/biga/api/relations").get_json()
    cut = client.get("/biga/api/relations?start=2019-09-02").get_json()

    assert len(cut["series"][0]["points"]) < len(full["series"][0]["points"])
    assert cut["series"][0]["points"][0]["t"] > full["series"][0]["points"][0]["t"]


def test_momentum_years_selects_a_recent_window(client):
    """years 必须真的把区间截短：以前的默认 start 会把它整个吃掉。"""
    full = client.get("/biga/api/momentum").get_json()
    one = client.get("/biga/api/momentum?years=1").get_json()

    assert one["meta"]["start"] > full["meta"]["start"]
    assert one["meta"]["bars"] < full["meta"]["bars"]
    assert one["meta"]["years"] <= 1.1
    assert one["series"][0]["points"][0]["t"] > full["series"][0]["points"][0]["t"]


def test_momentum_explicit_start_wins_over_years(client):
    body = client.get("/biga/api/momentum?start=2019-09-02&years=5").get_json()
    assert body["meta"]["start"] >= "2019-09-02"
    assert body["meta"]["years"] < 5


@pytest.mark.parametrize("target, query", [
    ("/biga/api/momentum", "preset=nope"),
    ("/biga/api/momentum", "window=1"),
    ("/biga/api/momentum", "entry=abc"),
    ("/biga/api/momentum", "exit_threshold=-9"),
    ("/biga/api/relations", "years=1"),
    ("/biga/api/relations", "codes=NOPE"),
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

    assert client.post("/biga/api/refresh").get_json()["started"] is True
    for _ in range(200):
        if not service._REFRESH["running"]:
            break
        time.sleep(0.01)

    assert calls, "采集必须真的被调用"
    assert calls[0][1] == data.settled_cutoff(), "后台刷新也要带时间闸"
    assert service._REFRESH["last"]["added"] == 3
    assert service._REFRESH["error"] is None
    assert client.get("/biga/api/meta").get_json()["refresh_last"]["added"] == 3


def test_refresh_is_not_queued_twice(client, monkeypatch):
    monkeypatch.setattr(service, "_REFRESH", {"running": True, "started_at": None, "last": None, "error": None})
    body = client.post("/biga/api/refresh").get_json()
    assert body == {"started": False, "reason": "已有刷新任务在跑"}


def test_json_is_gzipped_only_when_the_client_asks(client):
    plain = client.get("/biga/api/momentum?years=1")
    assert "Content-Encoding" not in plain.headers

    packed = client.get("/biga/api/momentum?years=1", headers={"Accept-Encoding": "gzip"})
    assert packed.headers["Content-Encoding"] == "gzip"
    assert packed.headers["Vary"] == "Accept-Encoding"
    assert int(packed.headers["Content-Length"]) < len(plain.data)
    assert gzip.decompress(packed.data) == plain.data


def test_page_and_chart_library_are_gzipped_too(client):
    """静态文件也得压：页面 41KB、图表库 196KB，手机网络差别肉眼可见。"""
    for path in ("/biga/", "/biga/vendor/lightweight-charts/lightweight-charts.standalone.production.js"):
        plain = client.get(path)
        packed = client.get(path, headers={"Accept-Encoding": "gzip"})

        assert plain.status_code == packed.status_code == 200, path
        assert packed.headers["Content-Encoding"] == "gzip", path
        assert packed.headers["Vary"] == "Accept-Encoding", path
        assert len(packed.data) < len(plain.data) / 2, path
        assert gzip.decompress(packed.data) == plain.data, path


def test_static_assets_answer_304_on_a_repeat_visit(client):
    """conditional=True：浏览器带着 ETag 再来一次就别重传 196KB 图表库。"""
    first = client.get("/biga/vendor/lightweight-charts/lightweight-charts.standalone.production.js")
    etag = first.headers["ETag"]

    again = client.get("/biga/vendor/lightweight-charts/lightweight-charts.standalone.production.js",
                       headers={"If-None-Match": etag})
    assert again.status_code == 304 and not again.data


# ---------------- 挂到现有页面服务（/biga） ----------------

def test_root_redirects_to_biga(client):
    """老地址不 404：`/` 直接送到 /biga/，本地收藏夹不用改。"""
    resp = client.get("/")
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/biga/")


def test_page_uses_relative_urls_so_one_prefix_fits_both_envs(client):
    """本地 8012 和线上 5000 共用同一份 HTML：接口与图表库都必须走相对路径。"""
    page = client.get("/biga/").get_data(as_text=True)
    assert "getJSON('api/" in page and "getJSON('/api/" not in page
    assert 'src="vendor/' in page and 'src="/vendor/' not in page


def test_mounted_on_a_host_app_with_login_gate(tmp_path, monkeypatch):
    """宿主（趋势线）有自己的登录：页面未登录跳 /login，接口 401，登录后通。"""
    from flask import Flask

    host = Flask(__name__)
    host.secret_key = "test"
    app_module.init_app(host, "/biga", require_login=True)

    @host.get("/login")
    def _login():
        return "login page"

    with host.test_client() as client:
        resp = client.get("/biga/")
        assert resp.status_code == 302 and resp.headers["Location"].endswith("/login")
        resp = client.get("/biga/api/meta")
        assert resp.status_code == 401 and resp.get_json()["success"] is False

        with client.session_transaction() as sess:
            sess["authenticated"] = True
        assert client.get("/biga/api/meta").status_code == 200
        assert client.get("/biga/").status_code == 200


def test_gzip_is_scoped_to_the_biga_api_on_a_host_app():
    """挂到宿主上只压本前缀的 JSON，宿主自己的响应不动（那是别人的性能开关）。"""
    from flask import Flask, jsonify

    host = Flask(__name__)
    app_module.init_app(host, "/biga")

    @host.get("/other")
    def _other():
        return jsonify({"blob": "x" * 4096})

    with host.test_client() as client:
        assert "Content-Encoding" not in client.get("/other", headers={"Accept-Encoding": "gzip"}).headers
        packed = client.get("/biga/api/meta", headers={"Accept-Encoding": "gzip"})
        assert packed.headers.get("Content-Encoding") == "gzip"


def test_missing_database_is_backfilled_on_the_first_get(tmp_path, monkeypatch):
    """服务器首次部署 data/ 还不存在：第一个 GET 就起一次增量采集，不用人工跑 CLI。"""
    calls = []

    def fake_sync(path, cutoff=None):
        calls.append(path)
        return {"added": 5, "rows": 15, "codes": 3, "end": "2020-01-01", "errors": {}, "elapsed_ms": 1.0}

    monkeypatch.setattr(service.data, "sync", fake_sync)
    service._CLOSES.clear()
    service._REFRESH.update(running=False, started_at=None, last=None, error=None)

    empty = tmp_path / "missing.csv"          # 不建文件：库缺失的场景
    ready = service.ensure_data(empty)
    assert ready is False and calls, "空库必须先起采集"
    for _ in range(200):
        if not service._REFRESH["running"]:
            break
        time.sleep(0.01)


def test_ensure_data_is_a_noop_when_the_database_is_there(client, monkeypatch):
    """库已就绪时 GET 不能反复触发采集：每次请求只 stat 一下。"""
    calls = []
    monkeypatch.setattr(service.data, "sync", lambda *a, **k: calls.append(a) or {})
    service.closes()                          # 第一次 GET 会做的事：把宽表读进缓存
    assert service.ensure_data() is True
    assert calls == []
