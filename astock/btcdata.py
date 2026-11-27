"""BTC 日线（07:00 UTC 收盘口径）：从 btc_strategy 的 5 分钟线预聚合成小文件。

BTC 7×24 交易，而 A 股每天只在 15:00（北京）定一次价，所以 BTC 的「日收盘」取
07:00 UTC 之前最后一根 5 分钟线；07:00 之后到次日 06:55 的涨跌计入下一交易日收益。
源 CSV 由 btc-strategy-worker 每 4 小时维护（UTC naive 带 +00:00 时区）。

831k 行的源文件全量解析约 1.9s，绝不能放进请求路径：首次构建落盘
data/astock/btc_daily.csv（约 50KB），之后按源文件 (mtime, size) 判断是否重建。
"""

from __future__ import annotations

import csv
import os
import tempfile
from pathlib import Path

import pandas as pd

DEFAULT_SOURCE = Path("data/btc_strategy/raw/BTC-USDT_5m.csv")
DEFAULT_CACHE = Path("data/astock/btc_daily.csv")
CUTOFF_MINUTES = 7 * 60              # 北京 15:00 = 07:00 UTC
BTC_CODE = "BTC"


def _daily_from_minutes(source: Path, cutoff_minutes: int) -> pd.Series:
    """5 分钟线 → 指定 UTC 时点的日收盘（取 cutoff 前最后一根，源文件 tz-aware）。"""
    frame = pd.read_csv(source, usecols=["candle_begin_time", "close"])
    if frame.empty:
        raise ValueError(f"BTC 分钟线为空: {source}")
    stamp = pd.to_datetime(frame["candle_begin_time"], utc=True).dt.tz_localize(None)
    close = pd.to_numeric(frame["close"], errors="coerce")
    keep = ((stamp.dt.hour * 60 + stamp.dt.minute) < cutoff_minutes) & close.notna()
    if not keep.any():
        raise ValueError(f"BTC 分钟线里没有 {cutoff_minutes} 分钟之前的 K 线: {source}")
    daily = close[keep].groupby(stamp[keep].dt.normalize()).last().sort_index()
    daily.name = BTC_CODE
    return daily


def _stamp(source: Path, cutoff_minutes: int) -> tuple[int, int, int]:
    """缓存指纹：源文件 mtime 纳秒 + 字节数 + 收盘时点。"""
    info = source.stat()
    return info.st_mtime_ns, info.st_size, cutoff_minutes


def _header(stamp: tuple[int, int, int]) -> str:
    return "# stamp={}-{}-{}".format(*stamp)


def _read_cache(cache: Path, stamp: tuple[int, int, int]) -> pd.Series | None:
    """指纹一致才读；手写 csv 解析，避免再把 50KB 小文件走 pandas 的完整读入。"""
    try:
        with open(cache, "r", encoding="utf-8", newline="") as handle:
            if handle.readline().strip() != _header(stamp):
                return None
            days, closes = [], []
            for row in csv.reader(handle):
                if not row or row[0] == "trade_date":
                    continue
                days.append(row[0])
                closes.append(float(row[1]))
    except (OSError, ValueError):
        return None
    if not days:
        return None
    return pd.Series(closes, index=pd.DatetimeIndex(days), name=BTC_CODE)


def _write_cache(cache: Path, daily: pd.Series, stamp: tuple[int, int, int]) -> None:
    """临时文件 + 原子替换，进程被杀也不会留下半份缓存；写不了就只少一层缓存。"""
    handle_name = None
    try:
        cache.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", newline="", delete=False,
                dir=cache.parent, prefix=cache.name + ".", suffix=".tmp") as handle:
            handle_name = handle.name
            handle.write(_header(stamp) + "\n")
            writer = csv.writer(handle)
            writer.writerow(["trade_date", "close"])
            for day, value in daily.items():
                writer.writerow([day.strftime("%Y-%m-%d"), repr(float(value))])
        os.replace(handle_name, cache)
    except OSError:
        if handle_name:
            try:
                os.unlink(handle_name)
            except OSError:
                pass


def load_btc_daily(source: str | Path = DEFAULT_SOURCE,
                   cache: str | Path = DEFAULT_CACHE,
                   cutoff_minutes: int = CUTOFF_MINUTES) -> pd.Series:
    """BTC 日收盘序列，索引是 UTC 自然日（naive DatetimeIndex）。"""
    source, cache = Path(source), Path(cache)
    stamp = _stamp(source, cutoff_minutes)
    cached = _read_cache(cache, stamp)
    if cached is not None:
        return cached
    daily = _daily_from_minutes(source, cutoff_minutes)
    _write_cache(cache, daily, stamp)
    return daily


def btc_closes(ashare: pd.DataFrame, source: str | Path = DEFAULT_SOURCE,
               cache: str | Path = DEFAULT_CACHE) -> pd.DataFrame:
    """把 BTC 日收盘并到 A 股交易日表上：BTC 当第一列，周末涨跌归入下一交易日。

    ashare 是 data.load_closes(BTC_POOL + [BTC_BOND]) 的宽表，两边同一天都有价才保留。
    """
    daily = load_btc_daily(source, cache)
    merged = ashare.join(daily, how="inner")
    return merged[[BTC_CODE, *ashare.columns]].dropna()