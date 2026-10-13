import pandas as pd

from btc_strategy.data import normalize_ohlcv, resample_4h, validate_5m_continuity


def sample():
    times = pd.date_range("2026-01-01", periods=48, freq="5min", tz="UTC")
    return pd.DataFrame({"candle_begin_time": times, "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 1})


def test_normalize_deduplicates_and_resamples_4h():
    frame = pd.concat([sample(), sample().iloc[:1]], ignore_index=True)
    normalized = normalize_ohlcv(frame)
    assert len(normalized) == 48
    assert len(resample_4h(normalized)) == 1


def test_continuity_detects_missing_5m_candle():
    frame = sample().drop(index=5)
    assert validate_5m_continuity(frame)["gap_count"] == 1
