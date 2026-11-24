"""双动量轮动的信号、成本与明细口径（全部用合成价格，不联网）。"""

import itertools
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from astock import momentum

WIN, ENTRY, EXIT, COST = 2, 0.02, -0.005, 0.0004


def frame(**columns):
    """按 ALL_CODES 列序拼价格矩阵，返回 (dates, prices)。"""
    n = len(next(iter(columns.values())))
    dates = pd.bdate_range("2020-01-01", periods=n, name="trade_date")
    prices = np.column_stack([np.asarray(columns[code], float) for code in momentum.ALL_CODES])
    return dates, prices


def test_bond_index_never_joins_the_ranking():
    """国债指数只做空仓替代，涨得再猛也不能被当成池内标的持有。"""
    n = 30
    _, prices = frame(**{
        "sz.399006": 100 * 1.05 ** np.arange(n),
        "513100_PX": np.full(n, 100.0),
        "sh.000012": 100 * 1.50 ** np.arange(n),
    })
    holds = momentum.target_index(prices, WIN, ENTRY, EXIT)
    assert holds.max() <= 1, "持仓列号必须落在池内两列里"
    assert (holds[WIN:] == 0).all()


def test_close_when_losing_the_top_slot():
    """失去第一名且新第一名没过开仓阈值 → 平仓。"""
    n = 30
    head = 100 * 1.05 ** np.arange(12)
    _, prices = frame(**{
        "sz.399006": np.concatenate([head, np.full(n - 12, head[-1])]),
        "513100_PX": 100 * 1.003 ** np.arange(n),        # 两日 0.6%，过不了 2% 开仓线
        "sh.000012": np.full(n, 100.0),
    })
    holds = momentum.target_index(prices, WIN, ENTRY, EXIT)
    # head[-1] 已是第 11 天，停涨从第 12 天开始：第 12 天两日涨幅仍按 5% 算，第 13 天归零
    assert (holds[WIN:13] == 0).all()
    assert (holds[13:] == -1).all()


def test_close_when_own_momentum_breaks_exit():
    """还是第一名，但自身两日涨幅跌破平仓线 → 平仓。"""
    n = 30
    up = 100 * 1.05 ** np.arange(13)
    down = up[-1] * 0.93 ** np.arange(1, n - 12)
    _, prices = frame(**{
        "sz.399006": np.concatenate([up, down]),
        "513100_PX": 100 * 0.98 ** np.arange(n),        # 跌得更狠，第一名仍是被套的创业板指
        "sh.000012": np.full(n, 100.0),
    })
    holds = momentum.target_index(prices, WIN, ENTRY, EXIT)
    assert holds[12] == 0, "第 12 天两日 +10.25%，继续持有"
    assert holds[13] == -1, "第 13 天两日 -2.35%，已跌破 -0.5% 平仓线"


def test_switch_to_new_leader_when_it_clears_entry():
    """失去第一名，但新第一名两日涨幅 4.06% 过了 2% 开仓线 → 换仓。"""
    n = 30
    head = 100 * 1.06 ** np.arange(12)
    _, prices = frame(**{
        "sz.399006": np.concatenate([head, np.full(n - 12, head[-1])]),
        "513100_PX": 100 * 1.02 ** np.arange(n),        # 两日 4.04%
        "sh.000012": np.full(n, 100.0),
    })
    holds = momentum.target_index(prices, WIN, ENTRY, EXIT)
    assert (holds[WIN:13] == 0).all()
    assert (holds[13:] == 1).all()


def test_entry_threshold_is_strict():
    """空仓时只有严格大于开仓阈值才开仓；恰好等于不开。"""
    n = 20
    exact = 100 * 1.02 ** np.arange(n)          # 两日涨幅恰为 2.00% 的量级
    returns = momentum.momentum_returns(np.column_stack([exact]), 2)
    entry = float(returns[2, 0])                # 把阈值设成这个值本身
    _, prices = frame(**{
        "sz.399006": exact,
        "513100_PX": np.full(n, 100.0),
        "sh.000012": np.full(n, 100.0),
    })
    holds = momentum.target_index(prices, WIN, entry, EXIT)
    assert holds[2] == -1, "等于阈值不算过线"


def test_net_returns_turnover_and_cost():
    """换手额：空仓→持仓=1，满仓换一次=2，持仓→空仓=1，首日=0。"""
    prices = np.array([
        [100.0, 100.0, 100.0],
        [110.0, 100.0, 100.0],
        [110.0, 121.0, 100.0],
        [110.0, 121.0, 100.0],
    ])
    held = np.array([-1, 0, 1, -1], dtype=np.int16)
    net, turnover = momentum.net_returns(prices, held, 0.001)
    assert turnover.tolist() == [0.0, 1.0, 2.0, 1.0]
    assert net == pytest.approx([0.0, 0.1 - 0.001, 0.21 - 0.002, -0.001])


def test_segments_share_the_switch_day():
    """相邻持仓段共享换仓日，段收益才连乘得回净值；末段标记为未平仓。"""
    n = 30
    # 整数价格：第 13 天创业板指停涨让位、纳指ETF 两日 3.28% 过线，段首尾价都是整数便于核对
    dates, prices = frame(**{
        "sz.399006": np.concatenate([100.0 + 5 * np.arange(12), np.full(n - 12, 155.0)]),
        "513100_PX": 100.0 + 2 * np.arange(n),
        "sh.000012": np.full(n, 100.0),
    })
    perf = momentum.performance(prices, dates, WIN, ENTRY, EXIT, COST, None, None)
    trades, segments, yearly = momentum.episodes(dates, prices, perf["holds"], perf["nav"], COST, perf["lo"])
    assert [(row["code"], row["open_date"], row["close_date"]) for row in segments] == [
        ("sz.399006", dates[2].strftime("%Y-%m-%d"), dates[13].strftime("%Y-%m-%d")),
        ("513100_PX", dates[13].strftime("%Y-%m-%d"), dates[-1].strftime("%Y-%m-%d")),
    ]
    for prev, cur in itertools.pairwise(segments):
        assert prev["close_date"] == cur["open_date"], "换仓日既是旧段收尾也是新段开头"
        assert prev["closed"] is True
    assert segments[-1]["closed"] is False
    for row in segments:
        assert row["gross_ret"] == pytest.approx(round(row["close_price"] / row["open_price"] - 1.0, 6))
    # 首段继承窗口起点持仓不收开仓费，收一次平仓费；末段开仓收费、未平仓
    assert segments[0]["net_ret"] == pytest.approx(155 / 110 - 1.0 - COST, abs=1e-6)
    assert segments[1]["net_ret"] == pytest.approx(158 / 126 - 1.0 - COST, abs=1e-6)
    assert len(trades) == 2 * (len(segments) - 1)
    assert [row["side"] for row in trades] == ["buy", "sell"], "换仓按先买后卖记录"
    assert yearly and yearly[0]["count"] == 2


def test_entries_counted_with_full_history_shift():
    """窗口起点已经持仓不算一次开仓（bigA 在全历史上 shift 再切窗口）。"""
    n = 40
    dates, prices = frame(**{
        "sz.399006": 100 * 1.03 ** np.arange(n),
        "513100_PX": 100 * 1.01 ** np.arange(n),
        "sh.000012": np.full(n, 100.0),
    })
    full = momentum.performance(prices, dates, WIN, ENTRY, EXIT, COST, None, None)
    cut = momentum.performance(prices, dates, WIN, ENTRY, EXIT, COST, None,
                               dates[20].strftime("%Y-%m-%d"))
    assert full["entries"] == 1
    assert cut["entries"] == 0, "窗口首日已持仓，之后没再开仓"
    assert cut["hold_changes"] == 0


def test_yearly_lists_idle_years_too():
    """全年空仓的年份也要出现在年度收益里，否则「每年收益」会跳年。"""
    n = 520
    dates = pd.bdate_range("2020-01-01", periods=n, name="trade_date")
    first_2021 = int(np.flatnonzero(dates.year == 2021)[0])
    # 2020 年末见顶回落并在年内平仓，此后池内两列都不动 → 2021 全年空仓持债
    up = 100 * 1.03 ** np.arange(first_2021 - 8)
    down = up[-1] * 0.97 ** np.arange(1, 9)
    tail = np.full(n - up.size - down.size, down[-1])
    _, prices = frame(**{
        "sz.399006": np.concatenate([up, down, tail]),
        "513100_PX": np.full(n, 100.0),
        "sh.000012": np.full(n, 100.0),
    })
    perf = momentum.performance(prices, dates, WIN, ENTRY, EXIT, COST, None, None)
    _, segments, yearly = momentum.episodes(dates, prices, perf["holds"], perf["nav"], COST, perf["lo"])
    by_year = {row["year"]: row for row in yearly}

    assert sorted(by_year) == [2020, 2021], "窗口内的年份一个都不能少"
    assert by_year[2020]["count"] >= 1
    # 池内平仓都在 2020；持债段会跨年，不计入年度「段数」
    assert all(row["close_date"] < "2021-01-01" for row in segments if row["in_pool"])
    idle = by_year[2021]
    assert idle["count"] == 0
    assert idle["win_rate"] is None and idle["avg_days"] is None
    assert idle["return"] == pytest.approx(0.0, abs=1e-9), "持债价格不动，净值走平"


def test_monthly_win_rate_uses_month_ends_not_next_month_starts():
    """月末对月末：次月首日在数据里但绝不能拿来当上月收尾。"""
    dates = pd.DatetimeIndex(["2020-01-31", "2020-02-03", "2020-02-28", "2020-03-02", "2020-03-31"])
    nav = np.array([100.0, 999.0, 110.0, 999.0, 121.0])
    bench = np.full(5, 100.0)
    assert momentum._monthly_win_rate(nav, bench, dates) == pytest.approx(1.0)


@pytest.mark.parametrize("bad", [
    {"window": 1}, {"window": 2.5}, {"entry": 0.9}, {"exit_threshold": -0.9},
    {"cost_rate": 0.05}, {"entry": float("nan")},
])
def test_check_rejects_out_of_range(bad):
    params = {"window": 20, "entry": 0.0375, "exit_threshold": 0.0, "cost_rate": COST}
    params.update(bad)
    with pytest.raises(ValueError):
        momentum.check(**params)


def test_run_payload_is_json_safe_and_carries_meta():
    import json

    n = 60
    dates, prices = frame(**{
        "sz.399006": 100 * 1.02 ** np.arange(n),
        "513100_PX": 100 * 1.015 ** np.arange(n),
        "sh.000012": np.full(n, 100.0),
    })
    closes = pd.DataFrame(prices, index=dates, columns=momentum.ALL_CODES)
    result = momentum.run(closes, window=WIN, entry=ENTRY, exit_threshold=EXIT, start_date=None)
    json.dumps(result, allow_nan=False)
    assert result["meta"]["window"] == WIN
    assert result["meta"]["signal_lag_days"] == 1
    assert next(row["code"] for row in result["metrics"]) == "STRATEGY"
    assert len(result["metrics"]) == len(momentum.ALL_CODES) + 1
    assert len(result["series"]) == len(momentum.ALL_CODES) + 1
    assert result["drawdown"] and all(point["v"] <= 0 for point in result["drawdown"])


def test_run_rejects_missing_columns():
    dates = pd.bdate_range("2020-01-01", periods=60)
    closes = pd.DataFrame({"sz.399006": np.linspace(100, 200, 60)}, index=dates)
    with pytest.raises(ValueError, match="缺少指数数据"):
        momentum.run(closes)


def test_spans_reports_the_same_total_as_a_full_run():
    """周期按钮上的数字必须和该周期回测的「区间总收益」逐位一致，不能另算一套。"""
    n = 780                                     # 约 3 年，够 1 年档真正裁窗口
    dates, prices = frame(**{
        "sz.399006": 100 * 1.0006 ** np.arange(n) * (1 + 0.02 * np.sin(np.arange(n) / 11)),
        "513100_PX": 100 * 1.0003 ** np.arange(n),
        "sh.000012": 100 * 1.00008 ** np.arange(n),
    })
    for years in (1, 3, 0):                     # 0 = 全部
        perf = momentum.performance(prices, dates, WIN, ENTRY, EXIT, COST,
                                    years=years or None,
                                    start_date=None if years else momentum.ALL_START)
        span = momentum.spans(prices, dates, WIN, ENTRY, EXIT, COST, years_list=(years,))[0]
        assert span["years"] == years
        assert span["total"] == pytest.approx(float(perf["nav"][-1] / perf["nav"][0] - 1.0), rel=1e-12)
        assert span["cagr"] == pytest.approx(perf["stats"]["cagr"], rel=1e-12)
        assert span["start"] == dates[perf["lo"]].strftime("%Y-%m-%d")


def test_spans_uses_the_documented_button_order():
    """按钮顺序在前端写死为 1/3/5/10/全部，后端顺序反过来也得对得上。"""
    n = 60
    dates, prices = frame(**{
        "sz.399006": 100 * 1.02 ** np.arange(n),
        "513100_PX": 100 * 1.015 ** np.arange(n),
        "sh.000012": np.full(n, 100.0),
    })
    spans = momentum.spans(prices, dates, WIN, ENTRY, EXIT, COST)
    assert [row["years"] for row in spans] == list(momentum.SPAN_YEARS)
