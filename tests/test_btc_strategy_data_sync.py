import pandas as pd
from btc_strategy_fakes import FakeOkx, candle_rows

from btc_strategy.data_sync import (
    completed_cutoff,
    find_missing_ranges,
    merge_candles,
    sync_btc_data,
)


def candles(times):
    return pd.DataFrame(
        {
            "candle_begin_time": pd.to_datetime(times, utc=True),
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 1.0,
        }
    )


def test_find_missing_ranges_detects_internal_and_tail_gaps():
    frame = candles(["2026-01-01 00:00", "2026-01-01 00:10", "2026-01-01 00:20"])
    ranges = find_missing_ranges(frame, "5m", "2026-01-01 00:33")

    assert [(item.start, item.end) for item in ranges] == [
        (pd.Timestamp("2026-01-01 00:05", tz="UTC"), pd.Timestamp("2026-01-01 00:05", tz="UTC")),
        (pd.Timestamp("2026-01-01 00:15", tz="UTC"), pd.Timestamp("2026-01-01 00:15", tz="UTC")),
        (pd.Timestamp("2026-01-01 00:25", tz="UTC"), pd.Timestamp("2026-01-01 00:25", tz="UTC")),
    ]


def test_completed_cutoff_only_includes_finished_candles():
    cutoff = completed_cutoff("2026-01-01 00:07", "5m")
    assert cutoff == pd.Timestamp("2026-01-01 00:05", tz="UTC")


def test_merge_candles_sorts_and_keeps_remote_overlaps():
    local = candles(["2026-01-01 00:00", "2026-01-01 00:05"])
    local.loc[0, "close"] = 9.0
    remote = candles(["2026-01-01 00:05", "2026-01-01 00:10"])

    merged = merge_candles(local, remote)

    assert merged["candle_begin_time"].tolist() == [
        pd.Timestamp("2026-01-01 00:00", tz="UTC"),
        pd.Timestamp("2026-01-01 00:05", tz="UTC"),
        pd.Timestamp("2026-01-01 00:10", tz="UTC"),
    ]
    assert merged.loc[1, "close"] == 1.5


def local_csv(tmp_path, times):
    path = tmp_path / "BTC-USDT_5m.csv"
    candles(times).to_csv(path, index=False)
    return path


def test_sync_fills_the_tail_gap_from_okx(tmp_path):
    path = local_csv(tmp_path, ["2026-01-01 00:00", "2026-01-01 00:05", "2026-01-01 00:10", "2026-01-01 00:15", "2026-01-01 00:20"])
    exchange = FakeOkx(candle_rows("2026-01-01 00:00", 7))

    report = sync_btc_data(path, observed_at="2026-01-01 00:35", exchange=exchange)

    assert [(item.start, item.end) for item in report.missing_ranges_before] == [
        (pd.Timestamp("2026-01-01 00:25", tz="UTC"), pd.Timestamp("2026-01-01 00:30", tz="UTC"))
    ]
    assert report.fetched_rows == 2
    assert report.rows_after == 7
    assert report.missing_ranges_after == []
    assert report.written is True
    assert len(pd.read_csv(path)) == 7


def test_sync_check_only_reports_gaps_without_fetching(tmp_path):
    path = local_csv(tmp_path, ["2026-01-01 00:00", "2026-01-01 00:05", "2026-01-01 00:10"])
    exchange = FakeOkx(candle_rows("2026-01-01 00:00", 7))

    report = sync_btc_data(
        path,
        observed_at="2026-01-01 00:35",
        repair=False,
        write=False,
        exchange=exchange,
    )

    assert exchange.calls == []
    assert report.fetched_rows == 0
    assert report.written is False
    assert len(report.missing_ranges_before) == 1
