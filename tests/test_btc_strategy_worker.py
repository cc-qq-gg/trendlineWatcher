import pandas as pd

from btc_strategy.worker import next_run_at, parse_run_times


def test_next_run_at_uses_utc_schedule():
    run_times = parse_run_times("00:05,04:05,08:05,12:05,16:05,20:05")
    assert next_run_at(pd.Timestamp("2026-10-13 03:59", tz="UTC"), run_times) == pd.Timestamp(
        "2026-10-13 04:05", tz="UTC"
    )
    assert next_run_at(pd.Timestamp("2026-10-13 04:06", tz="UTC"), run_times) == pd.Timestamp(
        "2026-10-13 08:05", tz="UTC"
    )
    assert next_run_at(pd.Timestamp("2026-10-13 23:30", tz="UTC"), run_times) == pd.Timestamp(
        "2026-10-14 00:05", tz="UTC"
    )
