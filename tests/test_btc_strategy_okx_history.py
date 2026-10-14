import pandas as pd
import pytest
from btc_strategy_fakes import FakeOkx, candle_rows

from btc_strategy.okx_history import RAW_COLUMNS, fetch_candles, parse_page, to_inst_id


def test_to_inst_id_maps_spot_pair():
    assert to_inst_id("BTC/USDT") == "BTC-USDT"


def test_parse_page_drops_unconfirmed_candle():
    frame = parse_page(
        [
            ["1704067500000", "1", "2", "0.5", "1.5", "3", "0", "0", "1"],
            ["1704067800000", "1", "2", "0.5", "1.5", "3", "0", "0", "0"],
        ]
    )

    assert frame["candle_begin_time"].tolist() == [pd.Timestamp("2024-01-01 00:05", tz="UTC")]


def test_fetch_candles_pages_backwards_over_full_range():
    exchange = FakeOkx(candle_rows("2026-01-01 00:00", 10))

    frame = fetch_candles(
        "2026-01-01 00:10",
        "2026-01-01 00:40",
        exchange=exchange,
        limit=3,
        request_interval=0.0,
    )

    assert len(exchange.calls) >= 3
    assert frame["candle_begin_time"].tolist() == list(
        pd.date_range("2026-01-01 00:10", "2026-01-01 00:40", freq="5min", tz="UTC")
    )
    assert list(frame.columns) == RAW_COLUMNS
    assert int(exchange.calls[0]["after"]) == int(pd.Timestamp("2026-01-01 00:40:00.001", tz="UTC").value // 1_000_000)


def test_fetch_candles_skips_the_still_forming_candle():
    exchange = FakeOkx(candle_rows("2026-01-01 00:00", 3, confirm_last=True))

    frame = fetch_candles(
        "2026-01-01 00:00",
        "2026-01-01 00:10",
        exchange=exchange,
        request_interval=0.0,
    )

    assert frame["candle_begin_time"].tolist() == [
        pd.Timestamp("2026-01-01 00:00", tz="UTC"),
        pd.Timestamp("2026-01-01 00:05", tz="UTC"),
    ]


def test_fetch_candles_returns_empty_frame_without_rows():
    frame = fetch_candles("2026-01-01 00:00", "2026-01-01 00:10", exchange=FakeOkx(), request_interval=0.0)

    assert frame.empty
    assert list(frame.columns) == RAW_COLUMNS


def test_fetch_candles_rejects_reversed_range():
    with pytest.raises(ValueError):
        fetch_candles("2026-01-01 00:10", "2026-01-01 00:00", exchange=FakeOkx(), request_interval=0.0)


def test_fetch_candles_rejects_candles_off_the_bar_grid():
    exchange = FakeOkx(candle_rows("2026-01-01 00:00", 2, offset_ms=120_000))

    with pytest.raises(ValueError, match="aligned"):
        fetch_candles("2026-01-01 00:00", "2026-01-01 00:10", exchange=exchange, request_interval=0.0)


def test_fetch_candles_retries_then_fails_loudly():
    exchange = FakeOkx(candle_rows("2026-01-01 00:00", 2), failures=99)

    with pytest.raises(RuntimeError, match="history-candles request failed"):
        fetch_candles(
            "2026-01-01 00:00",
            "2026-01-01 00:05",
            exchange=exchange,
            request_interval=0.0,
            max_retries=2,
        )

    assert len(exchange.calls) == 2
