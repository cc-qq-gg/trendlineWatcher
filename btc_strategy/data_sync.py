from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from btc_strategy.data import atomic_csv, load_csv, normalize_ohlcv


DEFAULT_DATA_PATH = Path("data/btc_strategy/raw/BTC-USDT_5m.csv")
DEFAULT_SYMBOL = "BTC/USDT"
DEFAULT_TIMEFRAME = "5m"
DEFAULT_RUNNER_PYTHON = os.environ.get(
    "BTC_SYNC_PYTHON",
    "/Users/cq/venvs/Fuel/bin/python",
)
DEFAULT_STATER_ROOT = os.environ.get(
    "BTC_STATER_ROOT",
    str(Path(__file__).resolve().parents[2] / "stater"),
)


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


STATER_FETCH_SCRIPT = r'''
import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.environ["BTC_STATER_ROOT"])
import get_data as stater_get_data

exchange = stater_get_data.exchange
original_fetch_markets_by_type = exchange.fetch_markets_by_type
original_fetch_ohlcv = exchange.fetch_ohlcv

def safe_fetch_markets_by_type(market_type, params=None):
    params = params or {}
    if market_type != "spot":
        return []
    request = {"instType": exchange.convert_to_instrument_type(market_type)}
    response = exchange.publicGetPublicInstruments(exchange.extend(request, params))
    markets = [
        item
        for item in response.get("data", [])
        if item.get("baseCcy") and item.get("quoteCcy")
    ]
    return exchange.parse_markets(markets)

exchange.fetch_markets_by_type = safe_fetch_markets_by_type

def fetch_ohlcv_history(symbol, timeframe="1m", since=None, limit=None, params=None):
    params = dict(params or {})
    params["type"] = "HistoryCandles"
    return original_fetch_ohlcv(
        symbol,
        timeframe=timeframe,
        since=since,
        limit=min(limit or 300, 300),
        params=params,
    )

exchange.fetch_ohlcv = fetch_ohlcv_history
from get_data import get_kline

ranges = json.loads(os.environ["BTC_SYNC_MISSING_RANGES"])
frames = []
for item in ranges:
    start = pd.Timestamp(item["start"])
    end = pd.Timestamp(item["end"])
    start_utc = start.tz_convert("UTC") if start.tzinfo else start.tz_localize("UTC")
    end_utc = end.tz_convert("UTC") if end.tzinfo else end.tz_localize("UTC")
    days = max(1, (end_utc - start_utc).days + 1)
    frame = get_kline(
        start_time=start_utc.tz_localize(None).isoformat(),
        symbol=os.environ["BTC_SYNC_SYMBOL"],
        time_interval=os.environ["BTC_SYNC_TIMEFRAME"],
        days=days,
    )
    if frame is None or frame.empty:
        continue
    frame["candle_begin_time"] = pd.to_datetime(frame["candle_begin_time"], utc=True)
    frame = frame[
        (frame["candle_begin_time"] >= start_utc)
        & (frame["candle_begin_time"] <= end_utc)
    ]
    if not frame.empty:
        frames.append(frame)

columns = ["candle_begin_time", "open", "high", "low", "close", "volume"]
if frames:
    pd.concat(frames, ignore_index=True)[columns].to_csv(
        os.environ["BTC_SYNC_REMOTE_PATH"], index=False
    )
else:
    pd.DataFrame(columns=columns).to_csv(
        os.environ["BTC_SYNC_REMOTE_PATH"], index=False
    )
'''


def fetch_missing_with_stater(
    missing_ranges: list[MissingRange],
    *,
    symbol: str,
    timeframe: str,
    runner_python: str,
    stater_root: str | Path,
) -> pd.DataFrame:
    if not missing_ranges:
        return pd.DataFrame(columns=["candle_begin_time", "open", "high", "low", "close", "volume"])

    columns = ["candle_begin_time", "open", "high", "low", "close", "volume"]
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as handle:
        remote_path = Path(handle.name)
    environment = os.environ.copy()
    chunked_ranges: list[MissingRange] = []
    for item in missing_ranges:
        chunk_start = item.start
        while chunk_start <= item.end:
            chunk_end = min(item.end, chunk_start + pd.Timedelta(days=1) - pd.Timedelta(minutes=5))
            chunked_ranges.append(MissingRange(start=chunk_start, end=chunk_end))
            chunk_start = chunk_end + pd.Timedelta(minutes=5)

    environment.update(
        {
            "BTC_STATER_ROOT": str(Path(stater_root).resolve()),
            "BTC_SYNC_SYMBOL": symbol,
            "BTC_SYNC_TIMEFRAME": timeframe,
            "BTC_SYNC_REMOTE_PATH": str(remote_path),
            "BTC_SYNC_MISSING_RANGES": json.dumps(
                [
                    {"start": item.start.isoformat(), "end": item.end.isoformat()}
                    for item in chunked_ranges
                ]
            ),
        }
    )
    try:
        result = subprocess.run(
            [runner_python, "-c", STATER_FETCH_SCRIPT],
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.stdout:
            print(result.stdout, end="")
        if result.stderr:
            print(result.stderr, end="", file=__import__("sys").stderr)
        if result.returncode != 0:
            raise RuntimeError(f"stater data fetch failed with exit code {result.returncode}")
        remote = pd.read_csv(remote_path)
    finally:
        remote_path.unlink(missing_ok=True)

    if remote.empty:
        return pd.DataFrame(columns=columns)
    return normalize_ohlcv(remote)


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
    columns = ["candle_begin_time", "open", "high", "low", "close", "volume"]
    local = normalize_ohlcv(local)[columns]
    remote = normalize_ohlcv(remote)[columns]
    merged = pd.concat([local, remote], ignore_index=True)
    merged = merged.sort_values("candle_begin_time").drop_duplicates("candle_begin_time", keep="last")
    return merged.reset_index(drop=True)[columns]


def sync_btc_data(
    data_path: str | Path = DEFAULT_DATA_PATH,
    *,
    symbol: str = DEFAULT_SYMBOL,
    timeframe: str = DEFAULT_TIMEFRAME,
    observed_at: pd.Timestamp | str | None = None,
    repair: bool = True,
    write: bool = True,
    runner_python: str = DEFAULT_RUNNER_PYTHON,
    stater_root: str | Path = DEFAULT_STATER_ROOT,
) -> SyncReport:
    path = Path(data_path)
    local = load_csv(path)
    rows_before = len(local)
    missing_before = find_missing_ranges(local, timeframe, observed_at)
    cutoff = completed_cutoff(observed_at, interval_for(timeframe))

    fetched = pd.DataFrame(columns=["candle_begin_time", "open", "high", "low", "close", "volume"])
    fetched_rows = 0
    should_fetch = repair and bool(missing_before)
    if should_fetch:
        fetched = fetch_missing_with_stater(
            missing_before,
            symbol=symbol,
            timeframe=timeframe,
            runner_python=runner_python,
            stater_root=stater_root,
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
    parser.add_argument("--runner-python", default=DEFAULT_RUNNER_PYTHON)
    parser.add_argument("--stater-root", default=DEFAULT_STATER_ROOT)
    args = parser.parse_args()

    report = sync_btc_data(
        args.data_path,
        symbol=args.symbol,
        timeframe=args.timeframe,
        observed_at=args.observed_at,
        repair=not args.check_only,
        write=not args.no_write,
        runner_python=args.runner_python,
        stater_root=args.stater_root,
    )
    print(report.as_dict())


if __name__ == "__main__":
    main()
