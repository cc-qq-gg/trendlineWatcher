"""ICE 美元指数（DXY）周K 数据源。

主源: CNBC 图表接口，返回真实 ICE U.S. Dollar Index 的周K（W-SUN，真 OHLC）。
兜底: ECB 日收盘按 ICE 官方权重公式合成（无 OHLC，标记为合成）。
缓存: data/dxy/dxy_weekly.csv，默认 TTL 6 小时，失败时降级到缓存。

对外只暴露 get_weekly_bars(start, end)，任何异常都不会向外抛。
"""

import calendar
import json
import os
import threading
import urllib.request
from datetime import datetime, timedelta

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(PROJECT_ROOT, "data", "dxy")
CACHE_PATH = os.path.join(CACHE_DIR, "dxy_weekly.csv")

CNBC_CHART_URL = (
    "https://ts-api.cnbc.com/harmony/app/charts/5Y.json?symbol=.DXY&interval=1W"
)
ECB_URL = "https://api.frankfurter.app/{start}..{end}?from=USD"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
CACHE_TTL_HOURS = 6
# ICE 官方口径：指数 = 常数 × 各货币对按权重的几何加权
DXY_CONSTANT = 50.14348112
DXY_WEIGHTS = {
    "EUR": -0.576,  # EUR/USD，取倒数后指数为负
    "JPY": 0.136,   # USD/JPY
    "GBP": -0.119,  # GBP/USD，取倒数后指数为负
    "CAD": 0.091,   # USD/CAD
    "SEK": 0.042,   # USD/SEK
    "CHF": 0.036,   # USD/CHF
}

_fetch_lock = threading.Lock()


def _http_json(url: str, timeout: int = 15) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def _to_unix(date_value) -> int:
    """日期 → UTC 零点秒级时间戳，避免受本地时区影响。"""
    dt = date_value if isinstance(date_value, datetime) else pd.Timestamp(date_value)
    return calendar.timegm((dt.year, dt.month, dt.day, 0, 0, 0))


def _normalize(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    for col in ("open", "high", "low", "close"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.dropna(subset=["open", "high", "low", "close"])
    return frame.sort_values("date").drop_duplicates("date").reset_index(drop=True)


def fetch_cnbc_weekly() -> pd.DataFrame:
    """CNBC 图表接口：真实 ICE 美元指数周K。"""
    payload = _http_json(CNBC_CHART_URL)
    bars = (payload.get("barData") or {}).get("priceBars") or []
    if not bars:
        raise ValueError("CNBC 未返回周K数据")

    rows = []
    for bar in bars:
        stamp = str(bar.get("tradeTime", ""))
        if len(stamp) < 8:
            continue
        rows.append(
            {
                "date": datetime.strptime(stamp[:8], "%Y%m%d"),
                "open": bar.get("open"),
                "high": bar.get("high"),
                "low": bar.get("low"),
                "close": bar.get("close"),
            }
        )
    if not rows:
        raise ValueError("CNBC 周K字段解析为空")
    return _normalize(pd.DataFrame(rows))


def synthesize_from_ecb(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """ECB 日收盘 → 按 ICE 公式合成 DXY，再重采样成周K（无真实 OHLC）。"""
    url = ECB_URL.format(
        start=start.strftime("%Y-%m-%d"), end=(end + timedelta(days=7)).strftime("%Y-%m-%d")
    )
    payload = _http_json(url)
    rates = payload.get("rates") or {}
    if not rates:
        raise ValueError("ECB 未返回汇率数据")

    rows = []
    for day, quotes in rates.items():
        try:
            eurusd = 1.0 / float(quotes["EUR"])
            gbpusd = 1.0 / float(quotes["GBP"])
        except (KeyError, TypeError, ZeroDivisionError):
            continue
        value = DXY_CONSTANT
        # 权重为负的货币对（EURUSD / GBPUSD）本币越强指数越低
        value *= eurusd ** DXY_WEIGHTS["EUR"]
        value *= gbpusd ** DXY_WEIGHTS["GBP"]
        for code in ("JPY", "CAD", "SEK", "CHF"):
            rate = quotes.get(code)
            if not rate:
                value = None
                break
            value *= float(rate) ** DXY_WEIGHTS[code]
        if value:
            rows.append({"date": day, "close": value})

    if not rows:
        raise ValueError("ECB 汇率不足以合成美元指数")

    daily = pd.DataFrame(rows)
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.set_index("date")["close"].sort_index()
    weekly = daily.resample("W-SUN").agg(["first", "max", "min", "last"]).dropna()
    weekly.columns = ["open", "high", "low", "close"]
    weekly = weekly.reset_index()
    return _normalize(weekly)


def load_cache() -> tuple[pd.DataFrame, datetime | None, str | None]:
    if not os.path.exists(CACHE_PATH):
        return pd.DataFrame(), None, None
    try:
        frame = pd.read_csv(CACHE_PATH)
        if frame.empty:
            return pd.DataFrame(), None, None
        fetched_at = pd.to_datetime(frame["fetched_at"].iloc[0]).to_pydatetime()
        source = str(frame["source"].iloc[0])
        return _normalize(frame), fetched_at, source
    except Exception:
        return pd.DataFrame(), None, None


def save_cache(frame: pd.DataFrame, source: str, fetched_at: datetime) -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    out = frame.copy()
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    out["source"] = source
    out["fetched_at"] = fetched_at.strftime("%Y-%m-%d %H:%M:%S")
    out.to_csv(CACHE_PATH, index=False)


def _slice_window(frame: pd.DataFrame, start, end) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame
    if start is not None:
        # 周K按周日标注，起始日所在的那一周也要包含进来
        out = out[out["date"] >= pd.Timestamp(start).normalize() - timedelta(days=6)]
    if end is not None:
        out = out[out["date"] <= pd.Timestamp(end).normalize()]
    return out.reset_index(drop=True)


def get_weekly_bars(start=None, end=None, force: bool = False, allow_network: bool = True,
                    limit: int | None = None) -> dict:
    """返回裁剪到 [start, end] 的美元指数周K；limit 给定时再取尾部 N 根。

    降级链：CNBC 新数据 → 未过期缓存 → 过期缓存（stale）→ ECB 合成 → 失败。
    """
    result = {
        "success": False,
        "source": "",
        "symbol": "DXY",
        "interval": "1W",
        "bars": [],
        "partial_last": False,
        "stale": False,
        "fetched_at": "",
        "message": "",
    }

    frame, fetched_at, cached_source = load_cache()
    now = datetime.now()
    cache_age_hours = (now - fetched_at).total_seconds() / 3600 if fetched_at else None
    cache_fresh = (
        not frame.empty
        and cache_age_hours is not None
        and cache_age_hours < CACHE_TTL_HOURS
        and not force
    )

    if not cache_fresh and allow_network:
        with _fetch_lock:
            try:
                fresh = fetch_cnbc_weekly()
                if not fresh.empty:
                    frame, fetched_at, cached_source = fresh, now, "cnbc"
                    save_cache(fresh, "cnbc", now)
                    cache_age_hours = 0.0
            except Exception:
                pass  # 保持缓存；缓存为空时走下面的兜底

    if frame.empty and allow_network:
        ref_end = pd.Timestamp(end) if end is not None else pd.Timestamp(now.date())
        ref_start = (
            pd.Timestamp(start) if start is not None else ref_end - timedelta(days=365 * 2)
        )
        try:
            frame = synthesize_from_ecb(ref_start, ref_end)
            fetched_at, cached_source = now, "ecb_synthetic"
            cache_age_hours = 0.0
            save_cache(frame, "ecb_synthetic", now)
        except Exception as exc:
            result["message"] = f"美元指数取数失败: {exc}"
            return result

    if frame.empty:
        result["message"] = "美元指数无可用数据（无缓存且网络失败）"
        return result

    windowed = _slice_window(frame, start, end)
    if limit:
        windowed = windowed.tail(int(limit)).reset_index(drop=True)
    if windowed.empty:
        result["message"] = "美元指数在请求窗口内无数据"
        return result

    last_date = windowed["date"].iloc[-1]
    result.update(
        {
            "success": True,
            "source": cached_source or "cache",
            "bars": [
                {
                    "t": _to_unix(row.date),
                    "o": round(float(row.open), 3),
                    "h": round(float(row.high), 3),
                    "l": round(float(row.low), 3),
                    "c": round(float(row.close), 3),
                }
                for row in windowed.itertuples(index=False)
            ],
            # 周K按周日标注：末根日期 >= 本周周日 才说明这根还在走
            "partial_last": last_date >= (
                pd.Timestamp(now.date()) + timedelta(days=6 - now.weekday())
            ),
            "stale": cached_source == "cnbc" and not cache_fresh and bool(cache_age_hours and cache_age_hours >= CACHE_TTL_HOURS),
            "fetched_at": fetched_at.strftime("%Y-%m-%d %H:%M:%S") if fetched_at else "",
            "cache_age_hours": round(cache_age_hours, 2) if cache_age_hours is not None else None,
            "count": int(len(windowed)),
        }
    )
    if result["source"] == "ecb_synthetic":
        result["message"] = "美元指数为 ECB 汇率合成值，非交易所报价"
    return result
