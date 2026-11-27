"""BTC 双动量：日线聚合口径、缓存失效与接口契约（全部用合成分钟线，不联网、不读真实 CSV）。"""

import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from astock import btcdata, data, momentum, service


def write_minutes(path, start="2020-01-01 00:00:00+00:00", days=2):
    """UTC 5 分钟线，收盘价逐根递增，方便定位「cutoff 前最后一根」。"""
    stamps = pd.date_range(start, periods=days * 288, freq="5min")
    close = 1000.0 + np.arange(len(stamps), dtype=float)
    pd.DataFrame({
        "candle_begin_time": stamps,
        "open": close, "high": close, "low": close, "close": close, "volume": 1.0,
    }).to_csv(path, index=False)
    return stamps, close


def test_cutoff_takes_the_last_bar_before_seven_utc(tmp_path):
    """07:00 UTC 对齐 A 股 15:00 收盘：取 06:55 那根，07:00 之后的涨跌计入下一天。"""
    source, cache = tmp_path / "btc_5m.csv", tmp_path / "btc_daily.csv"
    stamps, close = write_minutes(source, days=2)

    daily = btcdata.load_btc_daily(source, cache)

    picked = close[(stamps.hour == 6) & (stamps.minute == 55)]
    assert len(daily) == 2
    assert daily.tolist() == pytest.approx(picked.tolist())
    assert [str(day.date()) for day in daily.index] == ["2020-01-01", "2020-01-02"]


def test_daily_cache_is_reused_until_the_source_adds_bars(tmp_path, monkeypatch):
    """831k 行源文件不能反复解析：命中缓存就只读 50KB 小文件，源文件变了才重建。"""
    source, cache = tmp_path / "btc_5m.csv", tmp_path / "btc_daily.csv"
    write_minutes(source, days=2)
    parses = {"n": 0}
    real_read_csv = pd.read_csv

    def counting(*args, **kwargs):
        parses["n"] += 1
        return real_read_csv(*args, **kwargs)

    monkeypatch.setattr(btcdata.pd, "read_csv", counting)

    first = btcdata.load_btc_daily(source, cache)
    assert cache.exists() and parses["n"] == 1
    again = btcdata.load_btc_daily(source, cache)
    assert parses["n"] == 1, "第二次必须直接吃缓存"
    assert again.tolist() == first.tolist()

    write_minutes(source, days=3)
    grown = btcdata.load_btc_daily(source, cache)
    assert parses["n"] == 2, "源文件追加了 K 线必须重建"
    assert len(grown) == 3 and grown.iloc[:2].tolist() == pytest.approx(first.tolist())


def test_btc_closes_uses_the_ashare_calendar(tmp_path):
    """BTC 7×24，但轮动只能按 A 股交易日对齐：周末与停机日直接丢掉。"""
    source, cache = tmp_path / "btc_5m.csv", tmp_path / "btc_daily.csv"
    stamps, close = write_minutes(source, days=6)              # 01-01 ~ 01-06，含周末
    days = ["2020-01-02", "2020-01-03", "2020-01-06"]         # 周五直接跳到周一
    index_path = tmp_path / "index_daily.csv"
    rows = [(day, code, 100.0 + i)
            for code in ("sz.399006", "sh.000012")
            for i, day in enumerate(days)]
    pd.DataFrame(rows, columns=data.CSV_COLUMNS).to_csv(index_path, index=False)

    closes = btcdata.btc_closes(
        data.load_closes(["sz.399006", "sh.000012"], index_path), source, cache)

    assert list(closes.columns) == [btcdata.BTC_CODE, "sz.399006", "sh.000012"]
    assert [day.strftime("%Y-%m-%d") for day in closes.index] == days
    picked = close[(stamps.hour == 6) & (stamps.minute == 55)][[1, 2, 5]]   # 01-02/01-03/01-06
    assert closes[btcdata.BTC_CODE].tolist() == pytest.approx(picked.tolist())


def btc_frame(days=300, btc_step=1.02):
    """BTC 单边上涨、创业板指横盘、国债走平：排名里 BTC 永远第一。"""
    dates = pd.bdate_range("2019-01-02", periods=days, name="trade_date")
    return pd.DataFrame({
        momentum.BTC_POOL[0]: 100.0 * btc_step ** np.arange(days),
        momentum.BTC_POOL[1]: np.full(days, 100.0),
        momentum.BTC_BOND: np.full(days, 100.0),
    }, index=dates)[momentum.BTC_ALL_CODES]


def test_btc_pool_rotation_holds_bitcoin_and_reports_its_own_pool():
    """池配置落地：BTC 排在池内，国债仍只做空仓替代。"""
    result = momentum.run(btc_frame(), window=2, entry=0.02, exit_threshold=-0.005,
                          pool=momentum.BTC, start_date=momentum.BTC.start)

    assert result["meta"]["pool"] == momentum.BTC_POOL
    assert result["meta"]["bond"] == momentum.BTC_BOND
    assert [row["code"] for row in result["metrics"]] == ["STRATEGY", *momentum.BTC_ALL_CODES]
    top = result["metrics"][0]
    assert top["bond_pct"] < 0.1, "BTC 一路上涨，不应该长期抱着国债指数"
    assert {row["code"] for row in result["episodes"]} <= {"BTC", "sz.399006", "sh.000012"}
    assert any(row["code"] == "BTC" for row in result["episodes"])
    json.dumps(result, allow_nan=False)


def test_btc_presets_stay_inside_the_btc_pool():
    """两套预设互不覆盖：top1/2/3 只在 BTC 池里存在，A 股池仍是 reference/optimized。"""
    assert list(momentum.BTC_PRESETS) == ["top1", "top2", "top3"]
    top1 = momentum.preset("top1", pool=momentum.BTC)
    assert (top1["window"], top1["entry"], top1["exit_threshold"]) == (24, 0.095, 0.026)
    assert set(momentum.PRESETS) == {"reference", "optimized"}
    with pytest.raises(ValueError, match="unknown preset"):
        momentum.preset("top1")                     # 不传池 = A 股，没有 top1


def test_btc_span_buttons_have_four_notches_and_match_the_full_run():
    """BTC 样本只有 7 年：档位是 1/3/5/全部，且「全部」必须与主回测逐位一致。"""
    frame = btc_frame(days=1400, btc_step=1.002)
    prices = frame[momentum.BTC_ALL_CODES].to_numpy(dtype=float)

    rows = momentum.spans(prices, frame.index, pool=momentum.BTC)
    assert [row["years"] for row in rows] == list(momentum.BTC_SPAN_YEARS)

    full = momentum.run(frame, pool=momentum.BTC)
    assert rows[-1]["total"] == pytest.approx(full["metrics"][0]["total"], rel=1e-9)
    assert rows[-1]["start"] == full["meta"]["start"]
    assert rows[-1]["start"] >= momentum.BTC.start, "全部档不能早于 BTC 样本起点"


def write_ashare(path, days):
    rows = [(day.strftime("%Y-%m-%d"), code, 100.0 + i)
            for code in ("sz.399006", "sh.000012")
            for i, day in enumerate(days)]
    pd.DataFrame(rows, columns=data.CSV_COLUMNS).to_csv(path, index=False)


def write_daily_minutes(path, days, step=0.004):
    """每个交易日只写一根 06:55 的 K 线：service 层只关心能 join 上，不关心分钟粒度。"""
    stamps = [pd.Timestamp(day.strftime("%Y-%m-%d")) + pd.Timedelta(minutes=6 * 60 + 55)
              for day in days]
    close = 100.0 * (1.0 + step) ** np.arange(len(days))
    pd.DataFrame({"candle_begin_time": [stamp.strftime("%Y-%m-%d %H:%M:%S+00:00") for stamp in stamps],
                  "close": close}).to_csv(path, index=False)


@pytest.fixture()
def btc_env(tmp_path, monkeypatch):
    """约 1.6 年的 A 股 + BTC 合成数据，并把三个默认路径都指到临时目录。"""
    days = pd.bdate_range("2019-01-02", periods=400)
    index_path, source, cache = (tmp_path / "index_daily.csv", tmp_path / "btc_5m.csv",
                                 tmp_path / "btc_daily.csv")
    write_ashare(index_path, days)
    write_daily_minutes(source, days)

    monkeypatch.setattr(data, "DEFAULT_PATH", index_path)
    monkeypatch.setattr(btcdata, "DEFAULT_SOURCE", source)
    monkeypatch.setattr(btcdata, "DEFAULT_CACHE", cache)
    for store in (service._CLOSES, service._RELATION, service._MOMENTUM, service._BTC_FRAME):
        store.clear()
    return days


def test_btc_view_serves_the_btc_pool_with_four_span_buttons(btc_env):
    """service 层入口：默认 top1 预设、池配置进 meta、区间按钮是 1/3/5/全部。"""
    body = service.btc_view()

    assert body["meta"]["pool_key"] == "btc"
    assert body["meta"]["pool"] == momentum.BTC_POOL
    assert body["meta"]["bond"] == momentum.BTC_BOND
    assert body["meta"]["preset"] == "top1"
    assert body["meta"]["label"] == momentum.BTC_PRESETS["top1"]["label"]
    assert [row["years"] for row in body["spans"]] == list(momentum.BTC_SPAN_YEARS)
    assert body["metrics"][0]["code"] == "STRATEGY"
    assert body["meta"]["elapsed_ms"] >= 0.0
    json.dumps(body, allow_nan=False)


def test_btc_view_caches_identical_params_and_rejects_unknown_presets(btc_env, monkeypatch):
    """相同参数命中 LRU 不再碰宽表；未知预设直接报错给路由转 400。"""
    first = service.btc_view()
    with monkeypatch.context() as patch:
        patch.setattr(service, "btc_closes_frame",
                      lambda *args, **kwargs: pytest.fail("命中缓存不该再取宽表"))
        assert service.btc_view() is first

    second = service.btc_view(preset="top2")
    assert second["meta"]["label"] == momentum.BTC_PRESETS["top2"]["label"]
    assert second["meta"]["exit_threshold"] == pytest.approx(0.022)

    with pytest.raises(ValueError, match="unknown preset"):
        service.btc_view(preset="nope")

    assert [row["key"] for row in service.btc_presets()] == ["top1", "top2", "top3"]


@pytest.fixture()
def btc_client(btc_env):
    """真实蓝图：验证路由拼接、参数校验与错误码（不含登录宿主）。"""
    from astock import app as app_module

    app_module.app.config.update(TESTING=True)
    return app_module.app.test_client()


def test_btc_api_serves_the_payload_with_query_params(btc_client):
    resp = btc_client.get("/biga/api/btc?preset=top2&years=1")
    body = resp.get_json()

    assert resp.status_code == 200
    assert body["meta"]["pool_key"] == "btc"
    assert body["meta"]["preset"] == "top2"
    assert [row["years"] for row in body["spans"]] == list(momentum.BTC_SPAN_YEARS)
    json.dumps(body, allow_nan=False)


def test_btc_api_maps_bad_params_to_400_and_missing_minutes_to_503(btc_client, btc_env,
                                                                  monkeypatch, tmp_path):
    assert btc_client.get("/biga/api/btc?preset=nope").status_code == 400
    assert btc_client.get("/biga/api/btc?window=1").status_code == 400      # 窗口下限 2

    monkeypatch.setattr(btcdata, "DEFAULT_SOURCE", tmp_path / "missing_5m.csv")
    service._MOMENTUM.clear()
    service._BTC_FRAME.clear()
    resp = btc_client.get("/biga/api/btc?preset=top3")

    assert resp.status_code == 503
    assert "分钟线" in resp.get_json()["message"]


def test_meta_exposes_the_btc_pool_and_its_coverage(btc_client, btc_env):
    body = btc_client.get("/biga/api/meta").get_json()

    assert body["btc_pool"] == momentum.BTC_POOL
    assert body["btc_bond"] == momentum.BTC_BOND
    assert [row["key"] for row in body["btc_presets"]] == ["top1", "top2", "top3"]
    assert body["btc_spans"] == list(momentum.BTC_SPAN_YEARS)
    assert body["btc_coverage"]["missing"] is False
    assert body["btc_coverage"]["rows"] == len(btc_env)
    json.dumps(body, allow_nan=False)


PAGE = Path(__file__).resolve().parents[1] / "astock" / "static" / "index.html"


def test_page_splits_momentum_into_ashare_and_btc_tabs():
    """页面契约：原来那个「双动量轮动」拆成 A 股 / BTC 两个 tab，各接自己的接口。"""
    html = PAGE.read_text(encoding="utf-8")

    assert '<button id="tabMom">A 股双动量</button>' in html
    assert '<button id="tabMomBtc">BTC 双动量</button>' in html
    assert "双动量轮动" not in html, "旧按钮名要彻底拆干净，别留第二个入口"
    ashare = re.search(r"ashare: \{(.*?)\n  \},", html, re.DOTALL).group(1)
    btc = re.search(r"btc: \{(.*?)\n  \},", html, re.DOTALL).group(1)
    assert "api/momentum" in ashare and "10 年" in ashare
    assert "api/btc" in btc and "10 年" not in btc, "BTC 只有 7 年样本，不该有 10 年档"
    assert "'BTC'" in btc or '"BTC"' in btc, "BTC 净值颜色配置要在系列表里"