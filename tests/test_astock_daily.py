"""14:50 双动量检测：换仓判定、正文口径、非交易日跳过（全部离线）。"""

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from astock import daily, data, momentum

# 每个数据源都要能单独打桩；加源时只改这里，别在 5 个测试里各抄一遍
SOURCE_NAMES = ("_quote_eastmoney", "_quote_sina", "_quote_eastmoney_live", "_quote_tencent")

# 交易清单的一行：区间 / 标的 / 天数 / 收益，各列之间至少两个空格（render 用 pad 排的）
TRADE_ROW = re.compile(r"^  (?P<span>.+?)\s{2,}(?P<name>\S+)\s+(?P<days>\d+（持有中）|\d+ 个交易日)")


@pytest.fixture(autouse=True)
def _compare_in_tmp(tmp_path, monkeypatch):
    """对照表默认落在仓库 data/ 下：测试一律挪到临时目录，别把真实表写脏。"""
    monkeypatch.setattr(daily, "COMPARE_PATH", tmp_path / "intraday_compare.csv")


def closes_from(**columns):
    """按 ALL_CODES 列序拼一张日线宽表。"""
    n = len(next(iter(columns.values())))
    dates = pd.bdate_range("2020-01-01", periods=n, name="trade_date")
    frame = pd.DataFrame({code: np.asarray(columns[code], float) for code in momentum.ALL_CODES},
                         index=dates)
    return frame


def test_detect_reports_holding_and_gap():
    n = 30
    frame = closes_from(**{
        "sz.399006": 100 * 1.05 ** np.arange(n),        # 两日 10.25%，稳过 2% 开仓线
        "513100_PX": np.full(n, 100.0),
        "sh.000012": np.full(n, 100.0),
    })
    signal = daily.detect(frame, window=2, entry=0.02, exit_threshold=-0.005)

    assert signal["target_code"] == momentum.POOL[0]
    assert signal["target"] == data.SERIES[momentum.POOL[0]]["name"]
    assert signal["switched"] is False
    assert signal["reads"][momentum.POOL[0]]["return"] == pytest.approx(0.1025, abs=1e-9)
    assert signal["reads"][momentum.POOL[0]]["gap"] < 0, "已超阈值，gap 应为负"
    assert signal["last_switch"]["date"] == "2020-01-03"


def test_detect_flags_switch_back_to_bond():
    n = 30
    # 前 n-1 天每天 +10%，最后一根直接 -20%：两日涨幅由 +21% 翻到 -3.2%，
    # 平仓线 -0.5% 与新第一名都不过关 → 当天从创业板切回国债
    series = 100 * 1.10 ** np.arange(n - 1)
    series = np.append(series, series[-1] * 0.80)
    frame = closes_from(**{
        "sz.399006": series,
        "513100_PX": np.full(n, 100.0),
        "sh.000012": np.full(n, 100.0),
    })
    signal = daily.detect(frame, window=2, entry=0.02, exit_threshold=-0.005)

    assert signal["prev_code"] == momentum.POOL[0]
    assert signal["target_code"] == momentum.BOND
    assert signal["switched"] is True


def test_render_states_the_intraday_caveat():
    n = 30
    frame = closes_from(**{
        "sz.399006": np.full(n, 100.0),
        "513100_PX": np.full(n, 100.0),
        "sh.000012": np.full(n, 100.0),
    })
    signal = daily.detect(frame, window=2, entry=0.02, exit_threshold=-0.005)
    subject, body = daily.render(signal, cost_bp=4, fetched_at="14:50")

    assert signal["date"] in subject and "国债指数" in subject
    assert subject.startswith("双动量") and "维持·" in subject
    assert "盘中估算" in body and "最后 10 分钟" in body
    # 涨幅那一行的周期数必须跟参数一致，不能写死 20 日
    assert "2 日涨幅" in body and "开仓线 2.00%" in body and "平仓线 -0.50%" in body
    assert "单边 4bp" in body


def test_recent_lists_newest_first_and_marks_the_open_segment():
    n = 30
    series = np.concatenate([
        np.full(10, 100.0),                          # 前 10 天不动 → 空仓持债
        100 * 1.02 ** np.arange(1, 11),              # 10 天每天 +2% → 两日 4% 过 2% 开仓线
        100 * 1.02 ** 10 * (1 - 0.02) ** np.arange(1, 11),   # 10 天每天 -2% → 跌破 -0.5% 平仓
    ])
    assert len(series) == n
    frame = closes_from(**{
        "sz.399006": series,
        "513100_PX": np.full(n, 100.0),
        "sh.000012": np.full(n, 100.0),
    })
    recent = daily.detect(frame, window=2, entry=0.02, exit_threshold=-0.005)["recent"]

    assert len(recent) == 3, "债 → 创业板 → 债"
    assert [row["open"] for row in recent] == sorted((row["open"] for row in recent), reverse=True)
    assert recent[0]["code"] == momentum.BOND and recent[0]["closed"] is False
    assert recent[0]["name"] == "国债指数"
    assert recent[1]["code"] == momentum.POOL[0] and recent[1]["closed"] is True
    assert recent[1]["days"] > 0 and recent[1]["ret"] is not None
    assert daily._recent(*_recent_args(frame), limit=1) == [recent[0]]


def _recent_args(frame):
    """把 frame 摊成 _recent 需要的四个入参，避免测试里重复算状态机。"""
    prices = frame[momentum.ALL_CODES].to_numpy(dtype=float)
    holds = momentum.target_index(prices, 2, 0.02, -0.005)
    return frame.index, prices, holds, momentum.COST_PER_SIDE


def test_render_prints_the_trade_log_without_duplicate_switch_line():
    n = 30
    # 后 10 天每天 +2%：两日涨幅稳过 2%，一直持有到末日 → 维持 + 持有中 + 浮盈
    series = np.concatenate([np.full(20, 100.0), 100 * 1.02 ** np.arange(1, 11)])
    frame = closes_from(**{
        "sz.399006": series,
        "513100_PX": np.full(n, 100.0),
        "sh.000012": np.full(n, 100.0),
    })
    subject, body = daily.render(daily.detect(frame, window=2, entry=0.02, exit_threshold=-0.005),
                                 cost_bp=4, fetched_at="14:50")

    assert "最近十次交易" in body
    assert "持有中" in body and "浮盈" in body
    assert "最近换手" not in body, "换手信息已并入交易清单，不要重复"
    assert "维持·" in subject


def test_render_flags_a_switch_that_lands_on_the_last_day():
    n = 30
    series = np.full(n, 100.0)
    series[-1] = 103.0                       # 末日跳 3%，两日涨幅过线 → 当天开仓
    frame = closes_from(**{
        "sz.399006": series,
        "513100_PX": np.full(n, 100.0),
        "sh.000012": np.full(n, 100.0),
    })
    signal = daily.detect(frame, window=2, entry=0.02, exit_threshold=-0.005)
    subject, body = daily.render(signal, cost_bp=4, fetched_at="14:50")

    assert signal["switched"] is True
    assert "换仓·" in subject and "按今日收盘成交" in body
    assert "0（持有中）" in body and "浮亏" in body   # 刚成交，浮动收益只剩开仓成本


@pytest.fixture()
def book(tmp_path):
    """60 个交易日的三条序列，末日 2026-11-23；当天（24 日）由 fetch 打桩补上。"""
    days = pd.bdate_range(end="2026-11-23", periods=60)
    rows = []
    for code, drift in ((momentum.POOL[0], 0.004), (momentum.POOL[1], 0.001),
                        (momentum.BOND, 0.0001)):
        price = 100.0
        for day in days:
            price *= 1.0 + drift
            rows.append((day.strftime("%Y-%m-%d"), code, round(price, 4)))
    path = tmp_path / "index_daily.csv"
    pd.DataFrame(rows, columns=data.CSV_COLUMNS).to_csv(path, index=False)
    return path


def live_prices(path, *, spike=None):
    """以 CSV 末日价为底，给「今天」造一组盘中价；spike 让某条序列当天跳一格。"""
    long = data.load_long(path)
    last = long.groupby("code")["close"].last().to_dict()
    out = dict(last)
    if spike:
        out[spike[0]] = last[spike[0]] * spike[1]
    return out


def test_run_sends_when_all_three_codes_are_live(book, tmp_path, monkeypatch):
    prices = live_prices(book, spike=(momentum.POOL[0], 1.12))
    monkeypatch.setattr(daily, "fetch_today", lambda codes, today, trace=None: prices)
    sent = {}

    def mailer(subject, body, html=None):
        sent["subject"], sent["body"] = subject, body

    result = daily.run(today="2026-11-24", path=book, log_path=tmp_path / "log.txt",
                       mailer=mailer, send=True)

    assert result["sent"] is True and result["reason"] == "sent"
    assert result["signal"]["target_code"] == momentum.POOL[0]
    assert sent["subject"] == result["subject"]
    assert "2026-11-24" in sent["subject"]
    assert (tmp_path / "log.txt").read_text(encoding="utf-8").strip().endswith(sent["subject"])


def test_run_writes_only_settled_bars(book, tmp_path, monkeypatch):
    """--write 也走时间闸：盘中跑只补到已定稿的那天，当日那根留给收盘后重跑。"""
    monkeypatch.setattr(daily, "fetch_today",
                        lambda codes, today, trace=None: live_prices(book))
    monkeypatch.setattr(data, "settled_cutoff", lambda now=None: "2026-11-23")
    seen = {}

    def fake_sync(path, codes=None, cutoff=None):
        seen["cutoff"] = cutoff
        return {"added": 0, "updated": 0, "rows": 0}

    monkeypatch.setattr(data, "sync", fake_sync)

    daily.run(today="2026-11-24", path=book, log_path=tmp_path / "log.txt", write=True)

    assert seen["cutoff"] == "2026-11-23", "14:50 落库必须止于前一交易日"
    assert "cutoff=2026-11-23" in (tmp_path / "log.txt").read_text(encoding="utf-8")


def test_run_skips_non_trading_day_without_calling_smtp(book, tmp_path, monkeypatch):
    monkeypatch.setattr(daily, "fetch_today", lambda codes, today, trace=None: {})
    called = []
    result = daily.run(today="2026-11-24", path=book, log_path=tmp_path / "log.txt",
                       mailer=lambda *a: called.append(a), send=True)

    assert result == {"sent": False, "reason": "non-trading-day", "date": "2026-11-24"}
    assert called == []
    assert "non-trading-day" in (tmp_path / "log.txt").read_text(encoding="utf-8")


def test_run_refuses_to_send_on_partial_data(book, tmp_path, monkeypatch):
    prices = live_prices(book)
    prices.pop(momentum.BOND)
    monkeypatch.setattr(daily, "fetch_today", lambda codes, today, trace=None: prices)
    sent = {}

    def mailer(subject, body, html=None):
        sent["subject"], sent["body"] = subject, body

    result = daily.run(today="2026-11-24", path=book, log_path=tmp_path / "log.txt",
                       send=True, mailer=mailer)

    assert result["reason"] == "incomplete" and result["sent"] is True
    assert "采集失败" in sent["subject"] and "国债指数" in sent["body"]
    assert "今日未判定" in sent["body"]
    assert "incomplete" in (tmp_path / "log.txt").read_text(encoding="utf-8")


# ---------------- 多源兜底 ----------------

def test_fetch_today_falls_back_to_sina_when_eastmoney_raises(monkeypatch):
    def boom(code, today):
        raise RuntimeError("ProxyError")

    monkeypatch.setattr(daily, "_quote_eastmoney", boom)
    monkeypatch.setattr(daily, "_quote_sina", lambda code, today: 232.4937)
    monkeypatch.setattr(daily, "_quote_eastmoney_live", lambda code, today: 232.50)
    monkeypatch.setattr(daily, "_quote_tencent", lambda code, today: 232.49)
    trace = {}

    prices = daily.fetch_today(["sh.000012"], "2026-11-24", trace)

    assert prices == {"sh.000012": 232.4937}, "日K挂掉时优先取精度最高的新浪"
    assert trace == {"sh.000012": "新浪"}


def test_fetch_today_uses_eastmoney_realtime_when_the_kline_endpoint_is_dead(monkeypatch):
    """2026-11-24 实测：push2his 日K 整个域名不可达，同家 push2 实时却正常。"""
    def kline_down(code, today):
        raise RuntimeError("ConnectionError: RemoteDisconnected")

    monkeypatch.setattr(daily, "_quote_eastmoney", kline_down)
    monkeypatch.setattr(daily, "_quote_sina", lambda code, today: None)
    monkeypatch.setattr(daily, "_quote_eastmoney_live", lambda code, today: 2278.99)
    monkeypatch.setattr(daily, "_quote_tencent", lambda code, today: 2278.99)
    trace = {}

    assert daily.fetch_today(["sz.399006"], "2026-11-24", trace) == {"sz.399006": 2278.99}
    assert trace == {"sz.399006": "东财实时"}


def test_fetch_today_reaches_tencent_when_everyone_else_is_down(monkeypatch):
    def boom(code, today):
        raise RuntimeError("connreset")

    for name in ("_quote_eastmoney", "_quote_sina", "_quote_eastmoney_live"):
        monkeypatch.setattr(daily, name, boom)
    monkeypatch.setattr(daily, "_quote_tencent", lambda code, today: 2.131)
    trace = {}

    assert daily.fetch_today(["513100_PX"], "2026-11-24", trace) == {"513100_PX": 2.131}
    assert trace == {"513100_PX": "腾讯"}


def test_fetch_today_skips_a_source_whose_bar_is_not_from_today(monkeypatch):
    """节假日各家会把上一交易日的报价原样返回：日期不匹配就必须继续往下问。"""
    monkeypatch.setattr(daily, "_quote_eastmoney", lambda code, today: None)
    monkeypatch.setattr(daily, "_quote_sina", lambda code, today: None)
    monkeypatch.setattr(daily, "_quote_eastmoney_live", lambda code, today: None)
    monkeypatch.setattr(daily, "_quote_tencent", lambda code, today: 232.49)
    trace = {}

    assert daily.fetch_today(["sh.000012"], "2026-11-24", trace) == {"sh.000012": 232.49}
    assert trace == {"sh.000012": "腾讯"}


def test_fetch_today_answers_holiday_without_raising(monkeypatch):
    """每个源都「答了但没有今天这根」= 非交易日，不能当故障。"""
    for name in SOURCE_NAMES:
        monkeypatch.setattr(daily, name, lambda code, today: None)
    assert daily.fetch_today(["sh.000012"], "2026-11-24") == {}


def test_fetch_today_raises_when_every_source_is_down(monkeypatch):
    def boom(code, today):
        raise RuntimeError("connreset")

    for name in SOURCE_NAMES:
        monkeypatch.setattr(daily, name, boom)
    with pytest.raises(RuntimeError, match="所有源都连不上"):
        daily.fetch_today(["sh.000012"], "2026-11-24")


def test_run_sends_a_notice_when_every_source_is_down(book, tmp_path, monkeypatch):
    def boom(codes, today, trace=None):
        raise RuntimeError("ProxyError: Unable to connect to proxy")

    monkeypatch.setattr(daily, "fetch_today", boom)
    sent = {}

    def mailer(subject, body, html=None):
        sent["subject"], sent["body"] = subject, body

    result = daily.run(today="2026-11-24", path=book, log_path=tmp_path / "log.txt",
                       send=True, mailer=mailer)

    assert result["reason"] == "fetch-error" and result["sent"] is True
    assert sent["subject"] == "双动量 2026-11-24 采集失败"
    assert "所有源都连不上" in sent["body"] and "--date 2026-11-24" in sent["body"]
    log_text = (tmp_path / "log.txt").read_text(encoding="utf-8")
    assert "fetch-error" in log_text and "notice" in log_text


def test_render_always_states_which_source_the_prices_came_from(book):
    """数据源每封都要写：同源写一次带序列名，混源逐条列并说明为什么切了源。"""
    closes = data.load_closes(momentum.ALL_CODES, book)
    signal = daily.detect(closes)
    _, body = daily.render(signal, cost_bp=4, fetched_at="14:50",
                           sources={"sz.399006": "腾讯", "513100_PX": "腾讯", "sh.000012": "东财日K"})

    assert ("数据  创业板指 腾讯 · 纳指ETF 腾讯 · 国债指数 东财日K；"
            "东财日K 不可用，已切备用源") in body

    _, plain = daily.render(signal, cost_bp=4, fetched_at="14:50",
                            sources={code: "东财日K" for code in momentum.ALL_CODES})
    assert "数据  东财日K（创业板指、纳指ETF、国债指数）" in plain


def test_render_prints_the_execution_price_of_each_trade(book):
    """每笔交易都要能对上行情：开仓 → 平仓价；持有中那段的后一个价是现价。"""
    closes = data.load_closes(momentum.ALL_CODES, book)
    signal = daily.detect(closes)
    _, body = daily.render(signal, cost_bp=4, fetched_at="14:50")

    for item in signal["recent"]:
        assert (f"{daily._price(item['open_price'])} → "
                f"{daily._price(item['close_price'])}") in body


def test_price_precision_follows_the_magnitude():
    """指数三千点用两位、ETF 两块钱用四位：位数不对看着像没成交。"""
    assert daily._price(3740.1623) == "3740.16"
    assert daily._price(231.5745) == "231.57"
    assert daily._price(12.3456) == "12.346"
    assert daily._price(2.283) == "2.2830"


def test_render_prints_asset_codes_for_the_pool_and_the_verdict(book):
    """池内资产和结论都要带代码：邮件里只有中文名对不上行情软件。"""
    closes = data.load_closes(momentum.ALL_CODES, book)
    signal = daily.detect(closes)
    _, body = daily.render(signal, cost_bp=4, fetched_at="14:50")

    assert "池  创业板指 sz.399006 · 纳指ETF 513100_PX · 国债指数 sh.000012" in body
    verdict = "换仓" if signal["switched"] else "维持"
    assert f"结论  {verdict} {signal['target']}（{signal['target_code']}）" in body


def test_render_html_keeps_every_number_from_the_plain_body(book):
    """HTML 版和纯文本版必须说同一套数：收益、余量、代码一个都不能漂。"""
    closes = data.load_closes(momentum.ALL_CODES, book)
    signal = daily.detect(closes)
    _, body = daily.render(signal, cost_bp=4, fetched_at="14:50")
    page = daily.render_html(signal, cost_bp=4, fetched_at="14:50")

    numbers = set(re.findall(r"[+-]\d+\.\d{2}%", body)) | set(re.findall(r"\d+\.\d{2} 个百分点", body))
    assert numbers, "纯文本里本来就该有数字"
    for piece in numbers:
        assert piece in page, f"{piece} 在 HTML 里丢了"
    assert signal["target_code"] in page


def test_render_html_lays_out_cards_instead_of_padding(book):
    """手机是比例字体：对齐交给 table + nowrap，交易清单拆成「标的/收益 + 区间/天数」两行。"""
    closes = data.load_closes(momentum.ALL_CODES, book)
    signal = daily.detect(closes)
    page = daily.render_html(signal, cost_bp=4, fetched_at="14:50")

    assert page.count("<table") >= 2, "涨幅和交易清单各一张表"
    assert page.count("white-space:nowrap") >= 3, "手机窄屏不能靠折行"
    assert "持有中" in page or "个交易日" in page
    # 每笔交易的区间与天数在同一格里（「区间 · 18 日」），不会像纯文本那样跨列折行
    head = signal["recent"][0]
    assert f'{head["open"]} 起 · {head["days"]} 日' in page


def test_render_aligns_the_trade_log_into_four_columns():
    """交易清单按显示宽度分列：标的列、天数列的起点在所有行必须一致。"""
    n = 30
    series = np.concatenate([
        np.full(10, 100.0),                                   # 前 10 天不动 → 空仓持债
        100 * 1.02 ** np.arange(1, 11),                       # 10 天每天 +2% → 开仓
        100 * 1.02 ** 10 * (1 - 0.02) ** np.arange(1, 11),    # 10 天每天 -2% → 平仓
    ])
    frame = closes_from(**{
        "sz.399006": series,
        "513100_PX": np.full(n, 100.0),
        "sh.000012": np.full(n, 100.0),
    })
    signal = daily.detect(frame, window=2, entry=0.02, exit_threshold=-0.005)
    _, body = daily.render(signal, cost_bp=4, fetched_at="14:50")

    rows = [line for line in body.splitlines() if TRADE_ROW.match(line)]
    assert len(rows) >= 2, "这几行才是交易清单"

    name_starts = {daily._width(line[:TRADE_ROW.match(line).start("name")]) for line in rows}
    day_starts = {daily._width(line[:TRADE_ROW.match(line).start("days")]) for line in rows}
    assert len(name_starts) == 1, "标的列没对齐"
    assert len(day_starts) == 1, "天数列没对齐"


def test_run_skips_weekend_before_touching_the_network(book, tmp_path, monkeypatch):
    monkeypatch.setattr(daily, "fetch_today",
                        lambda codes, today, trace=None: pytest.fail("周末不该联网"))
    result = daily.run(today="2026-11-21", path=book, log_path=tmp_path / "log.txt")

    assert result["reason"] == "weekend"


# ---------------- 盘中对照表（14:50 vs 收盘） ----------------

def _snapshot_all(price, day, at="14:50", source="东财日K"):
    codes = momentum.ALL_CODES
    return daily.log_snapshot({code: price for code in codes}, {code: source for code in codes},
                              day, pd.Timestamp(f"{day} {at}"))


def test_log_snapshot_writes_one_row_per_code_and_overwrites_the_same_day():
    assert _snapshot_all(10.0, "2026-11-24") == 3
    frame = daily.load_compare()
    assert list(frame.columns) == daily.COMPARE_COLUMNS
    assert set(frame["code"]) == set(momentum.ALL_CODES)
    assert set(frame["snapshot_at"]) == {"14:50:00"}

    _snapshot_all(10.5, "2026-11-24", at="14:55")
    frame = daily.load_compare()
    assert len(frame) == 3, "同一天重跑要覆盖，不能追加"
    assert set(frame["snapshot_price"]) == {10.5}
    assert set(frame["snapshot_at"]) == {"14:55:00"}


def test_backfill_close_trusts_only_the_daily_kline_sources():
    """实时源会把今天最新价冒充历史收盘，只认东财日K/新浪报出来的数。"""
    _snapshot_all(10.0, "2026-11-23")

    def realtime(codes, day, trace=None):
        trace.update({code: "腾讯" for code in codes})
        return {code: 9.0 for code in codes}

    assert daily.backfill_close("2026-11-23", fetch=realtime) == 0
    assert daily.load_compare()["close_price"].isna().all(), "实时源的数不许当收盘"

    def kline(codes, day, trace=None):
        trace.update({code: "东财日K" for code in codes})
        return {code: 9.5 for code in codes}

    assert daily.backfill_close("2026-11-23", fetch=kline) == 3
    frame = daily.load_compare()
    assert (frame["close_source"] == "东财日K").all()
    assert frame["diff_pct"].iloc[0] == pytest.approx((10.0 / 9.5 - 1) * 100, abs=1e-3)


def test_backfill_close_waits_until_the_day_is_settled():
    _snapshot_all(10.0, "2026-11-24")
    called = []

    def fetch(codes, day, trace=None):
        called.append(day)
        return {}

    assert daily.backfill_close("2026-11-23", fetch=fetch) == 0
    assert called == [], "没定稿的那天收的是盘中价，回填了就是拿 14:50 冒充收盘"


def test_compare_summary_is_none_until_the_first_close_lands():
    assert daily.compare_summary() is None
    _snapshot_all(10.0, "2026-11-23")
    assert daily.compare_summary() is None, "只有快照没有收盘，不能编出对照"


def test_compare_summary_reports_the_latest_day_and_the_recent_mean():
    for day, close in (("2026-11-19", 10.2), ("2026-11-20", 9.8)):
        _snapshot_all(10.0, day)
        daily.backfill_close(day, fetch=lambda codes, d, trace=None, c=close: (
            trace.update({code: "东财日K" for code in codes}),
            {code: c for code in codes})[1])
    summary = daily.compare_summary()

    assert summary["date"] == "2026-11-20"
    assert summary["days"] == 2
    assert {row["diff"] for row in summary["rows"]} == {"+2.04%"}, "10 / 9.8 - 1"
    assert summary["mean_abs"] == "2.00%"


def test_run_snapshots_at_1450_then_backfills_the_next_session(book, tmp_path, monkeypatch):
    """第一天只记快照；第二天跑时才补前一天的收盘，并把偏差贴进正文。"""
    live = live_prices(book)
    sent = {}

    def mailer(subject, body, html=None):
        sent["subject"], sent["body"], sent["html"] = subject, body, html

    monkeypatch.setattr(daily, "fetch_today", lambda codes, today, trace=None: dict(live))
    first = daily.run(today="2026-11-24", path=book, log_path=tmp_path / "log.txt",
                      now=pd.Timestamp("2026-11-24 14:50"), send=True, mailer=mailer)

    assert first["snapshot"] == 3 and first["backfilled"] == 0
    assert "盘中 14:50 → 收盘" not in sent["body"], "还没有收盘价，不能编出对照"

    def fetch(codes, day, trace=None):
        trace.update({code: "东财日K" for code in codes})
        if day == "2026-11-24":                       # 已定稿 → 历史日K就是那根收盘
            return {code: float(live[code]) * 1.01 for code in codes}
        return {code: float(live[code]) for code in codes}

    monkeypatch.setattr(daily, "fetch_today", fetch)
    second = daily.run(today="2026-11-25", path=book, log_path=tmp_path / "log.txt",
                       now=pd.Timestamp("2026-11-25 14:50"), send=True, mailer=mailer)

    assert second["snapshot"] == 3 and second["backfilled"] == 3
    assert "盘中 14:50 → 收盘（2026-11-24）" in sent["body"]
    assert "平均绝对偏差 0.99%" in sent["body"], "(1 - 1/1.01) * 100"
    assert "盘中 14:50 → 收盘" in sent["html"]


def test_run_does_not_snapshot_a_bar_that_already_closed(book, tmp_path, monkeypatch):
    monkeypatch.setattr(daily, "fetch_today",
                        lambda codes, today, trace=None: live_prices(book))
    result = daily.run(today="2026-11-24", path=book, log_path=tmp_path / "log.txt",
                       now=pd.Timestamp("2026-11-24 15:30"))

    assert result["snapshot"] == 0, "盘中价只在 15:00 前记；收盘后记下来就是脏快照"
    assert daily.load_compare().empty


COMPARE_BLOCK = {"date": "2026-11-23", "days": 20, "mean_abs": "0.18%",
                 "rows": [{"name": "创业板指", "diff": "+0.31%"},
                          {"name": "纳指ETF", "diff": "-0.12%"}]}


def test_render_prints_the_intraday_drift_only_when_it_exists(book):
    closes = data.load_closes(momentum.ALL_CODES, book)
    signal = daily.detect(closes)

    _, body = daily.render(signal, cost_bp=4, fetched_at="14:50", compare=COMPARE_BLOCK)
    assert "盘中 14:50 → 收盘（2026-11-23）" in body
    assert "  创业板指  +0.31%" in body
    assert "近 20 个交易日平均绝对偏差 0.18%" in body

    _, plain = daily.render(signal, cost_bp=4, fetched_at="14:50")
    assert "盘中 14:50 → 收盘" not in plain, "表格还没有收盘价就不该出现这一块"


def test_render_html_prints_the_intraday_drift_table(book):
    closes = data.load_closes(momentum.ALL_CODES, book)
    signal = daily.detect(closes)
    page = daily.render_html(signal, cost_bp=4, fetched_at="14:50", compare=COMPARE_BLOCK)

    assert "盘中 14:50 → 收盘" in page
    assert "2026-11-23 · 近 20 个交易日平均绝对偏差 0.18%" in page
    assert "+0.31%" in page and "-0.12%" in page

