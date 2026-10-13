import pandas as pd

from btc_strategy.data_sync import completed_cutoff, find_missing_ranges, merge_candles


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
