from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from btc_strategy.data import atomic_csv, load_csv, normalize_ohlcv
from btc_strategy.okx_history import fetch_candles, to_inst_id

DEFAULT_DATA_PATH = Path("data/btc_strategy/raw/BTC-USDT_5m.csv")
DEFAULT_SYMBOL = "BTC/USDT"
DEFAULT_TIMEFRAME = "5m"
CANDLE_COLUMNS = ["candle_begin_time", "open", "high", "low", "close", "volume"]


@dataclass(frozen=True)
class MissingRange:
    start: pd.Timestamp
    end: pd.Timestamp


@dataclass
class SyncReport:
    symbol: str
    timeframe: str
    rows_before: int
    rows_after: int
    fetched_rows: int
    new_rows: int
    missing_ranges_before: list[MissingRange]
    missing_ranges_after: list[MissingRange]
    previous_end: pd.Timestamp | None
    new_end: pd.Timestamp | None
    cutoff: pd.Timestamp
    written: bool

    def as_dict(self) -> dict[str, Any]:
        def ranges(items: list[MissingRange]) -> list[dict[str, str]]:
            return [
                {"start": item.start.isoformat(), "end": item.end.isoformat()}
                for item in items
            ]

        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "rowsBefore": self.rows_before,
            "rowsAfter": self.rows_after,
            "fetchedRows": self.fetched_rows,
            "newRows": self.new_rows,
            "missingRangesBefore": ranges(self.missing_ranges_before),
            "missingRangesAfter": ranges(self.missing_ranges_after),
            "previousEnd": self.previous_end.isoformat() if self.previous_end else None,
            "newEnd": self.new_end.isoformat() if self.new_end else None,
            "cutoff": self.cutoff.isoformat(),
            "written": self.written,
        }


def interval_for(timeframe: str) -> pd.Timedelta:
    interval = pd.Timedelta(timeframe)
    if interval <= pd.Timedelta(0):
        raise ValueError(f"invalid timeframe: {timeframe}")
    return interval


def completed_cutoff(now: pd.Timestamp | None, interval: pd.Timedelta | str) -> pd.Timestamp:
    interval = pd.Timedelta(interval)
    timestamp = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    nanoseconds = (timestamp.value // interval.value) * interval.value
    return pd.Timestamp(nanoseconds, unit="ns", tz="UTC")


def find_missing_ranges(
    frame: pd.DataFrame,
    timeframe: str = DEFAULT_TIMEFRAME,
    observed_at: pd.Timestamp | None = None,
) -> list[MissingRange]:
    """Return inclusive missing candle ranges through the last completed candle."""
    if frame.empty:
        raise ValueError("local data is empty")
    interval = interval_for(timeframe)
    cutoff = completed_cutoff(observed_at, interval)
    times = frame["candle_begin_time"].sort_values().reset_index(drop=True)
    if times.iloc[-1] >= cutoff:
        times = times[times < cutoff].reset_index(drop=True)
    if times.empty:
        raise ValueError("local data has no completed candles")

    ranges: list[MissingRange] = []
    previous = times.iloc[0]
    for current in times.iloc[1:]:
        expected = previous + interval
        if current > expected:
            ranges.append(MissingRange(start=expected, end=current - interval))
        previous = current

    tail_start = times.iloc[-1] + interval
    tail_end = cutoff - interval
    if tail_start <= tail_end:
        ranges.append(MissingRange(start=tail_start, end=tail_end))
    return ranges


def fetch_missing_candles(
    missing_ranges: list[MissingRange],
    *,
    inst_id: str,
    bar: str,
    exchange: Any | None = None,
) -> pd.DataFrame:
    """Fetch every missing window from OKX and return one normalized frame."""
    if not missing_ranges:
        return pd.DataFrame(columns=CANDLE_COLUMNS)
    frames = [
        frame
        for frame in (
            fetch_candles(item.start, item.end, inst_id=inst_id, bar=bar, exchange=exchange)
            for item in missing_ranges
        )
        if not frame.empty
    ]
    if not frames:
        return pd.DataFrame(columns=CANDLE_COLUMNS)
    return normalize_ohlcv(pd.concat(frames, ignore_index=True))


def validate_prices(frame: pd.DataFrame) -> None:
    if frame.empty:
        raise ValueError("candles are empty")
    if (frame[["open", "high", "low", "close"]] <= 0).any().any():
        raise ValueError("candles contain non-positive OHLC values")
    invalid = (
        (frame["high"] < frame[["open", "close", "low"]].max(axis=1))
        | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))
    )
    if invalid.any():
        raise ValueError(f"candles contain invalid high/low values at rows: {invalid[invalid].index.tolist()[:5]}")


def merge_candles(local: pd.DataFrame, remote: pd.DataFrame) -> pd.DataFrame:
    local = normalize_ohlcv(local)[CANDLE_COLUMNS]
    remote = normalize_ohlcv(remote)[CANDLE_COLUMNS]
    merged = pd.concat([local, remote], ignore_index=True)
    merged = merged.sort_values("candle_begin_time").drop_duplicates("candle_begin_time", keep="last")
    return merged.reset_index(drop=True)[CANDLE_COLUMNS]


def sync_btc_data(
    data_path: str | Path = DEFAULT_DATA_PATH,
    *,
    symbol: str = DEFAULT_SYMBOL,
    timeframe: str = DEFAULT_TIMEFRAME,
    observed_at: pd.Timestamp | str | None = None,
    repair: bool = True,
    write: bool = True,
    exchange: Any | None = None,
) -> SyncReport:
    path = Path(data_path)
    local = load_csv(path)
    rows_before = len(local)
    missing_before = find_missing_ranges(local, timeframe, observed_at)
    cutoff = completed_cutoff(observed_at, interval_for(timeframe))

    fetched = pd.DataFrame(columns=CANDLE_COLUMNS)
    fetched_rows = 0
    should_fetch = repair and bool(missing_before)
    if should_fetch:
        fetched = fetch_missing_candles(
            missing_before,
            inst_id=to_inst_id(symbol),
            bar=timeframe,
            exchange=exchange,
        )
        fetched_rows = len(fetched)
        if not fetched.empty:
            validate_prices(fetched)

    updated = merge_candles(local, fetched) if not fetched.empty else local.copy()
    rows_after = len(updated)
    missing_after = find_missing_ranges(updated, timeframe, cutoff)
    new_end = updated["candle_begin_time"].iloc[-1]
    written = False

    if missing_after and repair:
        raise RuntimeError(
            "data still has missing candles after repair: "
            + "; ".join(f"{item.start.isoformat()}..{item.end.isoformat()}" for item in missing_after)
        )
    validate_prices(updated)

    if write and rows_after != rows_before:
        atomic_csv(updated, path)
        written = True

    return SyncReport(
        symbol=symbol,
        timeframe=timeframe,
        rows_before=rows_before,
        rows_after=rows_after,
        fetched_rows=fetched_rows,
        new_rows=rows_after - rows_before,
        missing_ranges_before=missing_before,
        missing_ranges_after=missing_after,
        previous_end=local["candle_begin_time"].iloc[-1],
        new_end=new_end,
        cutoff=cutoff,
        written=written,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Incrementally sync BTC candles")
    parser.add_argument("--data-path", default=str(DEFAULT_DATA_PATH))
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    parser.add_argument("--observed-at", default=None)
    parser.add_argument("--check-only", action="store_true", help="report gaps without fetching or writing")
    parser.add_argument("--no-write", action="store_true", help="fetch and validate but do not write")
    args = parser.parse_args()

    report = sync_btc_data(
        args.data_path,
        symbol=args.symbol,
        timeframe=args.timeframe,
        observed_at=args.observed_at,
        repair=not args.check_only,
        write=not args.no_write,
    )
    print(report.as_dict())


if __name__ == "__main__":
    main()
