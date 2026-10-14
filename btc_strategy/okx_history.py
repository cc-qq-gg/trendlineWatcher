"""OKX historical candle source owned by the btc_strategy package.

Reads candles straight from OKX with the project's own ccxt client, so the
strategy brings its own data pipeline instead of importing another project's
``get_data`` module or shelling out to an external interpreter.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any

import ccxt
import pandas as pd

RAW_COLUMNS = ["candle_begin_time", "open", "high", "low", "close", "volume"]

DEFAULT_INST_ID = "BTC-USDT"
DEFAULT_BAR = "5m"
PAGE_LIMIT = 300

BAR_MILLISECONDS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}


def to_inst_id(symbol: str) -> str:
    """Map a pair such as ``BTC/USDT`` to the OKX instrument id ``BTC-USDT``."""
    return symbol.replace("/", "-").upper()


def to_milliseconds(value: pd.Timestamp | str | int) -> int:
    if isinstance(value, int):
        return value
    timestamp = pd.Timestamp(value)
    timestamp = timestamp.tz_convert("UTC") if timestamp.tzinfo else timestamp.tz_localize("UTC")
    return int(timestamp.value // 1_000_000)


def create_exchange(config: dict[str, Any] | None = None) -> ccxt.okx:
    """Build the OKX client from the project's read-only credentials."""
    if config is None:
        from config_constants import OKEX_READONLY_CONFIG

        config = OKEX_READONLY_CONFIG
    return ccxt.okx(config)


def parse_page(rows: Sequence[Sequence[Any]]) -> pd.DataFrame:
    """Convert raw OKX candle rows into a UTC DataFrame, newest first."""
    records = []
    for row in rows:
        if len(row) < 6:
            continue
        if len(row) > 8 and str(row[8]) == "0":
            # Unconfirmed candle: still forming, skip it.
            continue
        records.append(
            {
                "timestamp": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
            }
        )
    if not records:
        return pd.DataFrame(columns=RAW_COLUMNS)
    frame = pd.DataFrame(records)
    frame["candle_begin_time"] = pd.to_datetime(frame["timestamp"], unit="ms", utc=True)
    frame = frame.sort_values("candle_begin_time", ascending=False)
    return frame.drop_duplicates("candle_begin_time", keep="first").reset_index(drop=True)


def _request_page(
    client: Any,
    params: dict[str, str],
    *,
    max_retries: int,
    request_interval: float,
) -> list[Sequence[Any]]:
    last_error: Exception | None = None
    for attempt in range(max(1, max_retries)):
        try:
            response = client.publicGetMarketHistoryCandles(params)
            return list(response.get("data") or [])
        except Exception as error:  # noqa: BLE001 - retried below, re-raised when exhausted
            last_error = error
            if attempt + 1 < max(1, max_retries) and request_interval:
                time.sleep(request_interval)
    raise RuntimeError(f"OKX history-candles request failed: {last_error}")


def fetch_candles(
    start: pd.Timestamp | str | int,
    end: pd.Timestamp | str | int,
    *,
    inst_id: str = DEFAULT_INST_ID,
    bar: str = DEFAULT_BAR,
    exchange: Any | None = None,
    limit: int = PAGE_LIMIT,
    request_interval: float = 0.05,
    max_retries: int = 3,
    max_pages: int = 5000,
) -> pd.DataFrame:
    """Fetch closed candles in ``[start, end]``, page by page, oldest last."""
    step_ms = BAR_MILLISECONDS.get(bar)
    if step_ms is None:
        raise ValueError(f"unsupported bar: {bar}")
    if not 1 <= limit <= PAGE_LIMIT:
        raise ValueError(f"limit must be between 1 and {PAGE_LIMIT}")

    start_ms = to_milliseconds(start)
    end_ms = to_milliseconds(end)
    if start_ms > end_ms:
        raise ValueError(f"start {start} is after end {end}")
    start_at = pd.Timestamp(start_ms, unit="ms", tz="UTC")
    end_at = pd.Timestamp(end_ms, unit="ms", tz="UTC")

    client = exchange if exchange is not None else create_exchange()
    pages: list[pd.DataFrame] = []
    cursor = end_ms + 1
    for _ in range(max_pages):
        rows = _request_page(
            client,
            {"instId": inst_id, "bar": bar, "after": str(cursor), "limit": str(limit)},
            max_retries=max_retries,
            request_interval=request_interval,
        )
        if not rows:
            break
        page = parse_page(rows)
        if page.empty:
            break
        if (page["timestamp"] % step_ms != 0).any():
            raise ValueError(f"OKX returned candles that are not aligned to {bar} bars")

        oldest_ms = int(page["timestamp"].iloc[-1])
        window = page[page["candle_begin_time"] <= end_at]
        window = window[window["candle_begin_time"] >= start_at]
        if not window.empty:
            pages.append(window)
        if oldest_ms <= start_ms or oldest_ms >= cursor:
            break
        cursor = oldest_ms
        if request_interval:
            time.sleep(request_interval)
    else:
        raise RuntimeError(f"stopped after {max_pages} pages before reaching {start_at.isoformat()}")

    if not pages:
        return pd.DataFrame(columns=RAW_COLUMNS)
    frame = pd.concat(pages, ignore_index=True)
    frame = frame.sort_values("candle_begin_time").drop_duplicates("candle_begin_time", keep="last")
    return frame.reset_index(drop=True)[RAW_COLUMNS]
