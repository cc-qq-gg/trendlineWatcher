"""CSV 长表存取、宽表对齐、原子写与增量落库（全部临时文件，不联网）。"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from astock import data


def write_long(path: Path, rows) -> Path:
    pd.DataFrame(list(rows), columns=data.CSV_COLUMNS).to_csv(path, index=False)
    return path


def test_load_long_missing_file_is_empty_with_columns(tmp_path):
    """文件不存在也要返回带列名的空表，调用方不必判形状。"""
    frame = data.load_long(tmp_path / "nope.csv")
    assert frame.empty and list(frame.columns) == data.CSV_COLUMNS


def test_load_long_drops_rows_without_a_usable_close(tmp_path):
    path = write_long(tmp_path / "long.csv", [
        ("2024-01-02", "sh.000001", "3000.5"),
        ("2024-01-03", "sh.000001", "bad"),
        ("2024-01-04", "sh.000001", ""),
        ("2024-01-05", "sh.000001", "3010"),
    ])
    frame = data.load_long(path)
    assert frame["trade_date"].tolist() == ["2024-01-02", "2024-01-05"]
    assert frame["close"].dtype.kind == "f"


def test_load_closes_keeps_only_common_days_and_price_codes(tmp_path):
    """宏观序列（周频 PE、月频 PMI）不能混进宽表，否则 dropna 会把日线清空。"""
    rows = []
    for day in ("2024-01-02", "2024-01-03", "2024-01-04"):
        for code in data.PRICE_CODES:
            rows.append((day, code, 100.0))
        rows.append((day, "HS300_PE", 12.0))
    rows = [row for row in rows if not (row[0] == "2024-01-03" and row[1] == "sh.000300")]
    wide = data.load_closes(path=write_long(tmp_path / "long.csv", rows))

    assert list(wide.columns) == data.PRICE_CODES
    assert "HS300_PE" not in wide.columns
    assert wide.index.tolist() == [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-04")], "缺一天就该剔除"
    assert wide.index.is_monotonic_increasing


def test_load_closes_keeps_last_quote_for_duplicate_day(tmp_path):
    path = write_long(tmp_path / "long.csv", [
        ("2024-01-02", code, 100.0) for code in data.PRICE_CODES
    ] + [
        ("2024-01-02", data.PRICE_CODES[0], 111.0),
    ])
    wide = data.load_closes(path=path)
    assert wide[data.PRICE_CODES[0]].iloc[-1] == pytest.approx(111.0)


def test_load_closes_empty_file_returns_empty_frame_with_columns(tmp_path):
    wide = data.load_closes(path=tmp_path / "nope.csv")
    assert wide.empty and list(wide.columns) == data.PRICE_CODES


def test_load_series_rejects_derived_code():
    with pytest.raises(ValueError, match="派生序列"):
        data.load_series(data.DERIVED[0])


def test_save_atomic_replaces_file_without_leaving_temp(tmp_path):
    path = tmp_path / "long.csv"
    write_long(path, [("2024-01-02", "sh.000001", "3000")])
    frame = pd.DataFrame([("2024-01-03", "sh.000001", 3010.0)], columns=data.CSV_COLUMNS)

    data.save_atomic(frame, path)

    assert data.load_long(path)["trade_date"].tolist() == ["2024-01-03"], "旧内容被整体替换"
    assert not list(tmp_path.glob(".tmp_*")), "临时文件必须清理干净"


def test_coverage_flags_derived_and_missing(tmp_path):
    path = write_long(tmp_path / "long.csv", [("2024-01-02", "sh.000001", "3000")])
    rows = {row["code"]: row for row in data.coverage(path, required=("sh.000300",))}

    assert rows["HS300_ERP"]["derived"] is True
    assert rows["HS300_ERP"]["rows"] == 0, "派生序列不落库"
    assert rows["sh.000001"]["rows"] == 1 and rows["sh.000001"]["missing"] is False
    assert rows["sh.000300"]["missing"] is True
    assert rows["CN10Y"]["internal"] is True


def test_stored_codes_exclude_derived_but_keep_sources():
    assert data.DERIVED[0] not in data.STORED_CODES
    assert set(data.STORED_CODES) == set(data.SERIES) - set(data.DERIVED)


def test_sync_is_incremental_and_idempotent(tmp_path, monkeypatch):
    path = write_long(tmp_path / "long.csv", [
        ("2024-01-02", "sh.000001", "3000"),
        ("2024-01-03", "sh.000001", "3010"),
    ])
    fake = {"sh.000001": {"2024-01-02": 3000.0, "2024-01-03": 3010.0, "2024-01-04": 3020.0}}
    monkeypatch.setattr(data, "collect", lambda codes, start_date: (fake, {}))

    first = data.sync(path)
    assert (first["added"], first["rows"], first["end"]) == (1, 3, "2024-01-04")
    assert first["errors"] == {}

    second = data.sync(path)
    assert (second["added"], second["rows"]) == (0, 3), "重复采集同一批数据必须幂等"
    assert data.load_long(path)["trade_date"].tolist() == ["2024-01-02", "2024-01-03", "2024-01-04"]


def test_sync_survives_a_failing_source(tmp_path, monkeypatch):
    """单条采集失败只记在 errors 里，其余序列照常落库。"""
    monkeypatch.setattr(data, "collect", lambda codes, start_date: (
        {"sh.000001": {"2024-01-02": 3000.0}},
        {"CN10Y": "RuntimeError: boom"},
    ))
    result = data.sync(tmp_path / "long.csv")
    assert result["errors"] == {"CN10Y": "RuntimeError: boom"}
    assert result["rows"] == 1


def test_sync_overwrites_the_same_day_instead_of_keeping_the_stale_bar(tmp_path, monkeypatch):
    """盘中快照写进来的半截收盘，重采同一天必须覆盖——旧版按 day > last_day 追加会永久留着脏值。"""
    path = write_long(tmp_path / "long.csv", [
        ("2024-01-02", "sh.000001", "3000"),
        ("2024-01-03", "sh.000001", "2346.22"),        # 09:47 的盘中快照
    ])
    monkeypatch.setattr(data, "collect", lambda codes, start_date: (
        {"sh.000001": {"2024-01-02": 3000.0, "2024-01-03": 2278.99}}, {}))

    result = data.sync(path)

    assert (result["added"], result["updated"], result["rows"]) == (0, 1, 2)
    assert data.load_long(path)["close"].tolist() == [3000.0, 2278.99], "同日期要覆盖，不能追加也留旧行"


def test_sync_refuses_bars_after_the_settlement_cutoff(tmp_path, monkeypatch):
    """时间闸：cutoff 之后的点一根都不写，盘中刷新不会把今天的半截 K 线落进来。"""
    path = write_long(tmp_path / "long.csv", [("2024-01-02", "sh.000001", "3000")])
    monkeypatch.setattr(data, "collect", lambda codes, start_date: (
        {"sh.000001": {"2024-01-02": 3000.0, "2024-01-03": 3010.0, "2024-01-04": 3020.0}}, {}))

    result = data.sync(path, cutoff="2024-01-03")

    assert result["cutoff"] == "2024-01-03"
    assert (result["added"], result["end"]) == (1, "2024-01-03")
    assert data.load_long(path)["trade_date"].tolist() == ["2024-01-02", "2024-01-03"]


def test_settled_cutoff_holds_the_current_day_until_1505():
    """15:05 前当日 K 线还没定稿，只放到前一自然日；之后当日才算可落库。"""
    assert data.settled_cutoff(pd.Timestamp("2026-11-24 09:47")) == "2026-11-23"
    assert data.settled_cutoff(pd.Timestamp("2026-11-24 15:04:59")) == "2026-11-23"
    assert data.settled_cutoff(pd.Timestamp("2026-11-24 15:05")) == "2026-11-24"
    assert data.settled_cutoff(pd.Timestamp("2026-11-24 23:10")) == "2026-11-24"


# ---------------- 盘中现价（push2） ----------------

STAMP = 1704351600              # 2024-01-04 15:00:00 +08


def test_fetch_live_quote_scales_by_the_declared_digits(monkeypatch):
    """f43 是放大过的整数：国债指数 4 位、创业板指 2 位，都得按 f59 还原。"""
    for digits, raw, expect in ((4, 2325023, 232.5023), (2, 227899, 2278.99), (3, 2131, 2.131)):
        payload = {"data": {"f43": raw, "f58": "x", "f59": digits, "f86": STAMP}}
        monkeypatch.setattr(data, "_http_json", lambda url, _p=payload, **_kw: _p)
        quote = data.fetch_live_quote("1.000012")
        assert quote == {"price": expect, "date": "2024-01-04", "name": "x"}


def test_fetch_live_quote_treats_a_suspended_code_as_missing(monkeypatch):
    """停牌时 f43 是字符串 "-"，不能被当成价格。"""
    monkeypatch.setattr(data, "_http_json", lambda url, **_kw: {
        "data": {"f43": "-", "f58": "x", "f59": 2, "f86": STAMP}})
    assert data.fetch_live_quote("1.000012") is None

    monkeypatch.setattr(data, "_http_json", lambda url, **_kw: {"data": None})
    assert data.fetch_live_quote("1.000012") is None
