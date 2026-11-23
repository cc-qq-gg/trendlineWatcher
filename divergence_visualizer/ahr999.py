"""AHR999 囤币指数的周序列。

口径来自 ahr999 原文及其更新篇：
    ahr999 = (P / DCA200) × (P / V)
    DCA200 = 200 / Σ(1/Pᵢ)                      200 日定投成本，调和平均
    V      = 10 ** (5.84 × log10(币龄) − 17.01)   指数增长估值（幂律拟合）
    币龄    = 自 2009-01-03 起的天数

原文特别更正过：200 日定投成本"本质上是调和平均值，不是几何平均值"，
因此不能用 MA200 代替，否则该比值会系统性偏高。

分区（沿用原文）：<0.45 抄底 / 0.45~1.2 定投 / 1.2~5 起飞 / >5 高估。

数据源：data/btc_strategy/raw/BTC-USDT_5m.csv（2019-01 起，现货）聚合为
UTC 日线；尾部用 data/okx/csv/BTC-USDT-SWAP_1D.csv 补齐 5m 未覆盖的日期。
读 5m 全表约 2.7 秒，因此只在缓存失效或 force 时计算，请求路径永远只读缓存。

缓存：data/ahr999/ahr999_weekly.csv，TTL 6 小时，原子写。任何异常不向外抛。
"""

from __future__ import annotations

import calendar
import math
import os
import threading
from datetime import datetime

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(PROJECT_ROOT, "data", "ahr999")
CACHE_PATH = os.path.join(CACHE_DIR, "ahr999_weekly.csv")
SPOT_5M_PATH = os.path.join(PROJECT_ROOT, "data", "btc_strategy", "raw", "BTC-USDT_5m.csv")
PERP_1D_PATH = os.path.join(PROJECT_ROOT, "data", "okx", "csv", "BTC-USDT-SWAP_1D.csv")

CACHE_TTL_HOURS = 6
DCA_DAYS = 200
GENESIS = pd.Timestamp("2009-01-03")
POWER_LAW_SLOPE = 5.84
POWER_LAW_OFFSET = 17.01
ZONES = {"bottom": 0.45, "dca": 1.2, "takeoff": 5.0}
COLUMNS = ["date", "price", "dca200", "valuation", "ahr999"]

_rebuild_lock = threading.Lock()
_warm_thread: threading.Thread | None = None

ZONE_LABELS = (
    ("抄底区", 0.0, ZONES["bottom"]),
    ("定投区", ZONES["bottom"], ZONES["dca"]),
    ("起飞区", ZONES["dca"], ZONES["takeoff"]),
    ("高估区", ZONES["takeoff"], float("inf")),
)


def _json_safe(value):
    """inf / nan 不是合法 JSON（浏览器 JSON.parse 会抛错），序列化成 null。"""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _sanitize_json(value):
    """递归清理 payload，保证整份 dict 都能被严格 JSON 解析。"""
    if isinstance(value, dict):
        return {key: _sanitize_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_json(item) for item in value]
    return _json_safe(value)


def _to_unix(date_value) -> int:
    """日期 → UTC 零点秒级时间戳。"""
    dt = date_value if isinstance(date_value, datetime) else pd.Timestamp(date_value)
    return calendar.timegm((dt.year, dt.month, dt.day, 0, 0, 0))


def _atomic_write(frame: pd.DataFrame, path: str) -> None:
    """先写临时文件再替换。

    不直接复用 get_data.save_csv_atomic：该模块在导入时会构造 ccxt 交易所
    对象，本模块要能在纯本地单元测试里独立导入。
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = f"{path}.tmp"
    frame.to_csv(tmp_path, index=False)
    os.replace(tmp_path, path)


def _read_close(path: str) -> pd.Series:
    frame = pd.read_csv(path, usecols=["candle_begin_time", "close"])
    stamp = pd.to_datetime(frame["candle_begin_time"], utc=True).dt.floor("D")
    series = pd.to_numeric(frame.groupby(stamp)["close"].last(), errors="coerce").dropna()
    series.index.name = "date"
    return series.astype(float)


def daily_close() -> pd.Series:
    """BTC 日线收盘（UTC 日）。

    5m 全表优先；永续 1D 只补 5m 缺失的日期，避免现货/永续混价覆盖已有现货价。
    """
    frames = [p for p in (SPOT_5M_PATH, PERP_1D_PATH) if os.path.exists(p)]
    if not frames:
        raise FileNotFoundError("AHR999 缺少 BTC 日线数据源（5m 与 1D 都不存在）")

    base = _read_close(frames[0])
    for extra_path in frames[1:]:
        extra = _read_close(extra_path)
        missing = extra.index.difference(base.index)
        if len(missing):
            base = pd.concat([base, extra.loc[missing]]).sort_index()
    return base


def compute_ahr999(daily: pd.Series) -> pd.DataFrame:
    """由日线收盘算 ahr999 序列（date/price/dca200/valuation/ahr999）。"""
    price = pd.to_numeric(daily, errors="coerce").astype(float)
    index = pd.DatetimeIndex(price.index)
    if index.tz is not None:
        index = index.tz_convert("UTC").tz_localize(None)

    # 200 日定投成本 = 调和平均 = 200 / Σ(1/P)；用累计倒数差做，避免逐窗口调用
    inverse_cum = (1.0 / price).cumsum()
    dca200 = DCA_DAYS / (inverse_cum - inverse_cum.shift(DCA_DAYS))

    age = (index - GENESIS).days.to_numpy(dtype="float64")
    valuation = np.power(10.0, POWER_LAW_SLOPE * np.log10(age) - POWER_LAW_OFFSET)

    values = price.to_numpy()
    ahr999 = (values / dca200.to_numpy()) * (values / valuation)

    frame = pd.DataFrame(
        {
            "date": index,
            "price": values,
            "dca200": dca200.to_numpy(),
            "valuation": valuation,
            "ahr999": ahr999,
        }
    )
    return frame.replace([np.inf, -np.inf], np.nan).dropna(subset=["ahr999"]).reset_index(drop=True)


def to_weekly(frame: pd.DataFrame) -> pd.DataFrame:
    """按所在周的周一聚合，取该周最后一个交易日的值。

    与 TrendlineWebApp._align_weekly_anchor 的"周锚点统一到周一"保持一致，
    这样副图折线的时间点能和数据里的周K一一对应。
    """
    if frame.empty:
        return frame
    day = pd.DatetimeIndex(frame["date"])
    week = (day - pd.to_timedelta(day.weekday, unit="D")).normalize()
    weekly = frame.assign(date=week).groupby("date", as_index=False).last()
    return weekly[COLUMNS].reset_index(drop=True)


def zone_of(value: float) -> str:
    for label, low, high in ZONE_LABELS:
        if low <= value < high:
            return label
    return "高估区"


def load_cache() -> tuple[pd.DataFrame, datetime | None]:
    if not os.path.exists(CACHE_PATH):
        return pd.DataFrame(), None
    try:
        frame = pd.read_csv(CACHE_PATH)
        if frame.empty:
            return pd.DataFrame(), None
        frame["date"] = pd.to_datetime(frame["date"])
        fetched_at = pd.to_datetime(frame["fetched_at"].iloc[0]).to_pydatetime()
        return frame[COLUMNS], fetched_at
    except Exception:
        return pd.DataFrame(), None


def save_cache(frame: pd.DataFrame, fetched_at: datetime) -> None:
    out = frame.copy()
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    out["fetched_at"] = fetched_at.strftime("%Y-%m-%d %H:%M:%S")
    _atomic_write(out, CACHE_PATH)


def _rebuild() -> tuple[pd.DataFrame, datetime]:
    weekly = to_weekly(compute_ahr999(daily_close()))
    if weekly.empty:
        raise ValueError("AHR999 计算结果为空")
    fetched_at = datetime.now()
    save_cache(weekly, fetched_at)
    return weekly, fetched_at


def _cache_age_hours(fetched_at: datetime | None) -> float | None:
    if not fetched_at:
        return None
    return (datetime.now() - fetched_at).total_seconds() / 3600


def get_weekly_series(limit: int = 200, force: bool = False, allow_rebuild: bool = True) -> dict:
    """返回最新 limit 根周序列。

    降级链：未过期缓存 → 重新计算（allow_rebuild 时）→ 过期缓存 → 失败。
    请求路径应传 allow_rebuild=False，只读缓存。
    """
    now = datetime.now()
    result = {
        "success": False,
        "symbol": "BTC",
        "interval": "1W",
        "source": "local",
        "bars": [],
        "thresholds": dict(ZONES),
        "zones": [
            {"label": label, "low": _json_safe(low), "high": _json_safe(high)}
            for label, low, high in ZONE_LABELS
        ],
        "partial_last": False,
        "stale": False,
        "needs_build": False,
        "fetched_at": "",
        "cache_age_hours": None,
        "count": 0,
        "latest": None,
        "message": "",
    }

    frame, fetched_at = load_cache()
    age_hours = _cache_age_hours(fetched_at)
    cache_fresh = bool(
        not frame.empty and age_hours is not None and age_hours < CACHE_TTL_HOURS and not force
    )

    rebuilt = False
    if not cache_fresh and allow_rebuild:
        with _rebuild_lock:
            # 双检：等锁期间可能已有别的线程写好了缓存
            frame, fetched_at = load_cache()
            age_hours = _cache_age_hours(fetched_at)
            if frame.empty or force or (age_hours or 0) >= CACHE_TTL_HOURS:
                try:
                    frame, fetched_at = _rebuild()
                    age_hours = 0.0
                    cache_fresh = True
                    rebuilt = True
                except Exception as exc:
                    result["message"] = f"AHR999 计算失败: {exc}"

    if frame.empty:
        result["needs_build"] = True
        result["message"] = result["message"] or "AHR999 尚无缓存，等待后台生成"
        return _sanitize_json(result)

    windowed = frame.tail(int(limit)).reset_index(drop=True)
    last_date = pd.Timestamp(windowed["date"].iloc[-1]).normalize()
    week_start = (pd.Timestamp(now.date()) - pd.Timedelta(days=now.weekday())).normalize()
    latest_value = float(windowed["ahr999"].iloc[-1])

    result.update(
        {
            "success": True,
            "source": "rebuild" if rebuilt else "cache",
            "bars": [
                {"t": _to_unix(row.date), "v": round(float(row.ahr999), 4)}
                for row in windowed.itertuples(index=False)
            ],
            "partial_last": last_date >= week_start,
            "stale": not cache_fresh and bool(age_hours and age_hours >= CACHE_TTL_HOURS),
            "fetched_at": fetched_at.strftime("%Y-%m-%d %H:%M:%S") if fetched_at else "",
            "cache_age_hours": round(age_hours, 2) if age_hours is not None else None,
            "count": int(len(windowed)),
            "latest": {
                "t": _to_unix(windowed["date"].iloc[-1]),
                "v": round(latest_value, 4),
                "zone": zone_of(latest_value),
                "price": round(float(windowed["price"].iloc[-1]), 2),
                "dca200": round(float(windowed["dca200"].iloc[-1]), 2),
                "valuation": round(float(windowed["valuation"].iloc[-1]), 2),
            },
        }
    )
    return _sanitize_json(result)


def warm_cache_in_background() -> None:
    """缓存缺失或过期时起一个守护线程重建，立即返回。

    5m 全表约 2.7 秒，绝不能落在请求路径上；已有线程在跑就直接跳过。
    """
    global _warm_thread
    if _warm_thread is not None and _warm_thread.is_alive():
        return
    frame, fetched_at = load_cache()
    age_hours = _cache_age_hours(fetched_at)
    if not frame.empty and age_hours is not None and age_hours < CACHE_TTL_HOURS:
        return

    def run():
        try:
            get_weekly_series(force=True)
        except Exception as exc:  # 后台线程不允许抛出去
            print(f"后台生成 AHR999 失败: {exc}")

    _warm_thread = threading.Thread(target=run, name="ahr999-warm", daemon=True)
    _warm_thread.start()
