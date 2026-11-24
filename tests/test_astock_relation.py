"""指数关系三件事：重定基、滚动分位、ERP，以及「先全历史算完再裁窗口」不变量。"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from astock import data, relation


def stamps(days):
    return np.array(days, dtype="datetime64[ns]")


# ---------------- 重定基 ----------------

def test_rebase_anchors_the_window_head_at_100():
    dates = pd.bdate_range("2020-01-01", periods=5).to_numpy()
    values = np.array([10.0, 20.0, 40.0, 80.0, 160.0])

    kept, rebased = relation.rebase(dates, values, "2020-01-03")

    assert kept.tolist() == dates[2:].tolist()
    assert rebased.tolist() == [100.0, 200.0, 400.0], "基准是裁完窗口后的首点，不是全历史首点"


def test_rebase_uses_full_history_head_when_no_start():
    dates = pd.bdate_range("2020-01-01", periods=3).to_numpy()
    _, rebased = relation.rebase(dates, np.array([50.0, 60.0, 75.0]), None)
    assert rebased.tolist() == [100.0, 120.0, 150.0]


def test_rebase_rejects_a_non_finite_base():
    dates = pd.bdate_range("2020-01-01", periods=3).to_numpy()
    with pytest.raises(ValueError, match="重定基失败"):
        relation.rebase(dates, np.array([np.nan, 1.0, 2.0]), None)


def test_rebase_returns_empty_when_window_starts_after_history():
    dates = pd.bdate_range("2020-01-01", periods=3).to_numpy()
    kept, values = relation.rebase(dates, np.array([1.0, 2.0, 3.0]), "2030-01-01")
    assert kept.size == 0 and values.size == 0


def test_rebase_hands_back_the_window_head_on_request():
    dates = pd.bdate_range("2020-01-01", periods=3).to_numpy()

    kept, rebased, base = relation.rebase(dates, np.array([50.0, 60.0, 75.0]), None, with_base=True)

    assert base == pytest.approx(50.0)
    assert rebased.tolist() == [100.0, 120.0, 150.0]
    assert kept.tolist() == dates.tolist()


# ---------------- 滚动分位 ----------------

def test_percentile_counts_the_current_day_inside_its_window():
    dates = pd.bdate_range("2020-01-01", periods=5).to_numpy()
    values = np.array([3.0, 1.0, 2.0, 5.0, 4.0])

    kept, quantiles, raw = relation.rolling_percentile(dates, values, years=10, min_samples=3)

    assert kept.tolist() == dates[2:].tolist(), "样本不足的热身期直接跳过"
    assert raw.tolist() == [2.0, 5.0, 4.0]
    assert quantiles.tolist() == pytest.approx([200 / 3, 100.0, 80.0]), "含自身，所以最小值是 1/窗口长度"


def test_percentile_window_is_bounded_by_years():
    """11 年前的高点不该把今天算成低分位。"""
    dates = stamps(["2010-01-04", "2021-01-04"])
    values = np.array([2.0, 1.0])

    _, wide, _ = relation.rolling_percentile(dates, values, years=20, min_samples=1)
    _, narrow, _ = relation.rolling_percentile(dates, values, years=10, min_samples=1)

    assert wide.tolist() == pytest.approx([100.0, 50.0]), "20 年窗口包含 2010 年的高点"
    assert narrow.tolist() == pytest.approx([100.0, 100.0]), "10 年窗口只剩自己"


def test_percentile_has_no_lookahead():
    """截掉尾部数据不能改变已有日期的分位 —— 一旦回看未来，这里立刻炸。"""
    dates = pd.bdate_range("2005-01-03", periods=2600)
    values = np.round(100 + np.cumsum(np.random.default_rng(7).normal(0, 1, 2600)), 4)

    kept, quantiles, _ = relation.rolling_percentile(dates.to_numpy(), values, years=2, min_samples=5)
    cut = 1500
    kept_cut, quantiles_cut, _ = relation.rolling_percentile(
        dates.to_numpy()[:cut], values[:cut], years=2, min_samples=5)

    assert kept_cut.size > 0 and quantiles_cut.size < quantiles.size
    assert kept_cut.tolist() == kept[:kept_cut.size].tolist()
    assert quantiles_cut.tolist() == quantiles[:quantiles_cut.size].tolist()


# ---------------- ERP ----------------

def test_erp_uses_the_latest_yield_on_or_before_the_day():
    pe = (stamps(["2024-01-02", "2024-01-03", "2024-01-05"]), np.array([12.0, 15.0, 20.0]))
    yields = (stamps(["2024-01-02", "2024-01-04"]), np.array([2.56, 2.60]))

    dates, values = relation.derive_erp(pe, yields)

    assert dates.tolist() == pe[0].tolist()
    assert values.tolist() == pytest.approx([100 / 12 - 2.56, 100 / 15 - 2.56, 100 / 20 - 2.60])


def test_erp_drops_points_whose_yield_is_too_old():
    pe_dates = pd.bdate_range("2024-01-01", periods=20)
    pe = (pe_dates.to_numpy(), np.full(20, 12.0))
    yields = (pe_dates.to_numpy()[:1], np.array([2.5]))

    dates, _ = relation.derive_erp(pe, yields)

    # 只有一个 10Y 读数时，只有落在它 7 个自然日内的 PE 点能算 ERP
    expected = [day for day in pe_dates if (day - pe_dates[0]).days <= relation.ERP_MAX_GAP_DAYS]
    assert len(expected) == 6
    assert list(pd.DatetimeIndex(dates)) == expected


def test_erp_ignores_non_positive_pe():
    pe = (stamps(["2024-01-02", "2024-01-03"]), np.array([0.0, 12.0]))
    yields = (stamps(["2024-01-02"]), np.array([2.5]))
    dates, values = relation.derive_erp(pe, yields)
    assert list(pd.DatetimeIndex(dates)) == [pd.Timestamp("2024-01-03")], "PE 为 0 的点不能除出 inf"
    assert values.tolist() == pytest.approx([100 / 12 - 2.5])


def test_erp_returns_empty_without_inputs():
    empty = (np.empty(0, "datetime64[ns]"), np.empty(0))
    assert relation.derive_erp(empty, empty)[0].size == 0


# ---------------- 组装 ----------------

@pytest.fixture()
def panel(tmp_path):
    """8 年周频 PE + 日频 10Y + 月频 PMI：够跑 10 年分位与 ERP。"""
    days = pd.bdate_range("2015-01-01", periods=2000)
    rows = [(day.strftime("%Y-%m-%d"), "CN10Y", round(2.5 + 0.001 * (i % 50), 4))
            for i, day in enumerate(days)]
    rows += [(day.strftime("%Y-%m-%d"), "HS300_PE", round(12.0 + 0.4 * np.sin(i / 7.0), 4))
             for i, day in enumerate(days[::5])]
    rows += [(day.strftime("%Y-%m-%d"), "PMI_CN", round(49.0 + (i % 7) * 0.3, 4))
             for i, day in enumerate(pd.date_range("2015-01-01", periods=96, freq="MS"))]
    path = tmp_path / "long.csv"
    pd.DataFrame(rows, columns=data.CSV_COLUMNS).to_csv(path, index=False)
    return path


def test_visible_codes_hide_internal_but_keep_derived():
    codes = relation.visible_codes()
    assert "CN10Y" not in codes and "sh.000012" not in codes and "513100_PX" not in codes
    assert "HS300_ERP" in codes, "派生序列照样要展示，只是不落库"


def test_build_rejects_unknown_code(panel):
    with pytest.raises(ValueError, match="未知指数代码"):
        relation.build(["NOPE"], path=panel)


def test_percentile_is_identical_whether_or_not_the_window_is_cut(panel):
    """裁窗口只减少点数，不能改已有日期的分位 —— 否则起点会被热身期污染。"""
    full = relation.build(["HS300_PE"], path=panel)["series"][0]["points"]
    cut = relation.build(["HS300_PE"], path=panel, start_date="2020-01-02")["series"][0]["points"]

    assert 0 < len(cut) < len(full)
    lookup = {point["t"]: point["v"] for point in full}
    assert all(point["v"] == pytest.approx(lookup[point["t"]], abs=1e-9) for point in cut)


def test_default_codes_are_the_three_headline_series(panel):
    """默认只画 上证 / 沪深300 / 股债性价比；PE、PMI、创业板留作可选项。"""
    assert relation.DEFAULT_CODES == ("sh.000001", "sh.000300", "HS300_ERP")
    assert set(relation.DEFAULT_CODES) < set(relation.visible_codes())
    assert [item["code"] for item in relation.build(path=panel)["series"]] == list(relation.DEFAULT_CODES)


def test_build_includes_derived_erp_with_raw_readings(panel):
    series = relation.build(path=panel)["series"]
    assert [item["code"] for item in series] == list(relation.DEFAULT_CODES)

    erp = next(item for item in series if item["code"] == "HS300_ERP")
    assert erp["display"] == "percentile" and erp["axis"] == "right"
    assert erp["points"] and all(point["raw"] is not None for point in erp["points"])
    assert erp["latest"]["v"] is not None
    assert "base" not in erp, "分位序列没有「窗口首日点位」这个口径"


def test_build_ships_the_window_head_so_the_frontend_can_plot_raw_points(tmp_path):
    """左轴画原始点位（对照 bigA）：v 仍是重定基，base 让前端还原 v × base / 100。"""
    days = pd.bdate_range("2024-01-01", periods=5)
    closes = [3000.0, 3030.0, 3060.0, 3090.0, 3120.0]
    rows = [(day.strftime("%Y-%m-%d"), "sh.000300", close) for day, close in zip(days, closes)]
    path = tmp_path / "px.csv"
    pd.DataFrame(rows, columns=data.CSV_COLUMNS).to_csv(path, index=False)

    item = relation.build(["sh.000300"], start_date="2024-01-02", path=path)["series"][0]

    assert item["base"] == pytest.approx(3030.0), "基准是裁完窗口后的首点"
    assert item["points"][0]["v"] == pytest.approx(100.0)
    raw = [point["v"] * item["base"] / 100 for point in item["points"]]
    # v 下发时保留 4 位小数，乘回去的误差在千分之一量级，远小于显示的两位小数
    assert raw == pytest.approx(closes[1:], abs=1e-2)
    assert "raw" not in item["points"][0], "左轴不重复下发原始值，省一半包体"


def test_latest_reads_agree_with_build(panel):
    rows = {row["code"]: row for row in relation.latest_reads(path=panel)}
    series = {item["code"]: item for item in relation.build(path=panel)["series"]}

    for code, item in series.items():
        if not item["points"]:
            assert code not in rows, "无数据的序列不该出现在读数里"
            continue
        assert rows[code]["value"] == pytest.approx(item["points"][-1].get("raw", item["points"][-1]["v"]), rel=1e-6)
        if item["display"] == "percentile":
            assert rows[code]["percentile"] == pytest.approx(item["points"][-1]["v"], abs=0.01)


def test_latest_reads_rejects_unknown_code(panel):
    with pytest.raises(ValueError, match="未知指数代码"):
        relation.latest_reads(["NOPE"], path=panel)
