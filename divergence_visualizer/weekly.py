"""BTC（及同类合约）周K 数据源。

数据源: OKX 公共 REST `market/candles?bar=1W&limit=300`，一次可取 300 根周K。
缓存: data/weekly/{INST}_1W.csv，默认 TTL 6 小时，失败时降级到过期缓存。

为什么不复用 data/okx/csv/*_1W.csv：那份是由 15m 重采样出来的，只有本地
15m 覆盖的几十周；也不走 get_data.get_kline，因为它内部写死 limit=2000，
会被 OKX 的 300 根上限退化成“最新 300 根”。这里固定 limit<=300 直取。

对外只暴露 get_btc_weekly(limit=200)，任何异常都不会向外抛。
"""

import calendar
import json
import os
import threading
import urllib.request
from datetime import datetime

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(PROJECT_ROOT, "data", "weekly")

DEFAULT_INST_ID = "BTC-USDT-SWAP"
# OKX 单次最多返回 300 根；一次取满，接口再按 limit 截尾
FETCH_LIMIT = 300
OKX_CANDLES_URL = (
    "https://www.okx.com/api/v5/market/candles"
    "?instId={inst_id}&bar=1W&limit={limit}"
)
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
CACHE_TTL_HOURS = 6

_fetch_lock = threading.Lock()


def cache_path(inst_id: str = DEFAULT_INST_ID) -> str:
    return os.path.join(CACHE_DIR, f"{inst_id}_1W.csv")


def _http_json(url: str, timeout: int = 15) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def _to_unix(date_value) -> int:
    """日期 → UTC 零点秒级时间戳，与 dxy 保持同一横轴基准。"""
    dt = date_value if isinstance(date_value, datetime) else pd.Timestamp(date_value)
    return calendar.timegm((dt.year, dt.month, dt.day, 0, 0, 0))


def _normalize(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    for col in ("open", "high", "low", "close"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.dropna(subset=["open", "high", "low", "close"])
    return frame.sort_values("date").drop_duplicates("date").reset_index(drop=True)


def fetch_okx_weekly(inst_id: str = DEFAULT_INST_ID, limit: int = FETCH_LIMIT) -> pd.DataFrame:
    """OKX 周K：bar=1W（UTC 周一起），时间转成北京时间日期，与项目其它 CSV 口径一致。"""
    url = OKX_CANDLES_URL.format(inst_id=inst_id, limit=min(limit, FETCH_LIMIT))
    payload = _http_json(url)
    rows = payload.get("data") or []
    if payload.get("code") not in ("0", 0, None) or not rows:
        raise ValueError(f"OKX 未返回周K数据: {payload.get('msg') or payload.get('code')}")

    records = []
    for row in rows:
        try:
            stamp = pd.to_datetime(int(row[0]), unit="ms", utc=True)
        except (TypeError, ValueError):
            continue
        records.append(
            {
                "date": stamp.tz_convert("Asia/Shanghai").tz_localize(None).normalize(),
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
                # confirm='0' 表示这根周K还没走完
                "confirm": row[8] if len(row) > 8 else "1",
            }
        )
    if not records:
        raise ValueError("OKX 周K字段解析为空")
    return _normalize(pd.DataFrame(records))


def load_cache(inst_id: str = DEFAULT_INST_ID):
    path = cache_path(inst_id)
    if not os.path.exists(path):
        return pd.DataFrame(), None, None
    try:
        frame = pd.read_csv(path)
        if frame.empty:
            return pd.DataFrame(), None, None
        fetched_at = pd.to_datetime(frame["fetched_at"].iloc[0]).to_pydatetime()
        source = str(frame["source"].iloc[0])
        return _normalize(frame), fetched_at, source
    except Exception:
        return pd.DataFrame(), None, None


def save_cache(frame: pd.DataFrame, source: str, fetched_at: datetime,
               inst_id: str = DEFAULT_INST_ID) -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    out = frame.copy()
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    if "confirm" not in out.columns:
        out["confirm"] = "1"
    out["source"] = source
    out["fetched_at"] = fetched_at.strftime("%Y-%m-%d %H:%M:%S")
    out.to_csv(cache_path(inst_id), index=False)


def get_btc_weekly(limit: int = 200, force: bool = False, allow_network: bool = True,
                   inst_id: str = DEFAULT_INST_ID) -> dict:
    """返回最新 limit 根周K。

    降级链：OKX 新数据 → 未过期缓存 → 过期缓存（stale）→ 失败。
    """
    result = {
        "success": False,
        "symbol": inst_id.split("-")[0],
        "interval": "1W",
        "source": "",
        "bars": [],
        "partial_last": False,
        "stale": False,
        "fetched_at": "",
        "message": "",
    }

    frame, fetched_at, cached_source = load_cache(inst_id)
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
                fresh = fetch_okx_weekly(inst_id)
                if not fresh.empty:
                    frame, fetched_at, cached_source = fresh, now, "okx"
                    save_cache(fresh, "okx", now, inst_id)
                    cache_age_hours = 0.0
            except Exception:
                pass  # 保持缓存；缓存为空时走下面的失败分支

    if frame.empty:
        result["message"] = "BTC 周K无可用数据（无缓存且网络失败）"
        return result

    windowed = frame.tail(limit).reset_index(drop=True)
    last_date = windowed["date"].iloc[-1]
    last_confirm = str(windowed["confirm"].iloc[-1]) if "confirm" in windowed.columns else "1"

    result.update(
        {
            "success": True,
            "source": cached_source or "cache",
            "bars": [
                {
                    "t": _to_unix(row.date),
                    "o": round(float(row.open), 2),
                    "h": round(float(row.high), 2),
                    "l": round(float(row.low), 2),
                    "c": round(float(row.close), 2),
                }
                for row in windowed.itertuples(index=False)
            ],
            # confirm='0' 或 末根日期落在本周内，都算进行中的周
            "partial_last": last_confirm == "0"
            or (pd.Timestamp(now.date()) - last_date).days < 7,
            "stale": cached_source == "okx"
            and not cache_fresh
            and bool(cache_age_hours and cache_age_hours >= CACHE_TTL_HOURS),
            "fetched_at": fetched_at.strftime("%Y-%m-%d %H:%M:%S") if fetched_at else "",
            "cache_age_hours": round(cache_age_hours, 2) if cache_age_hours is not None else None,
            "count": int(len(windowed)),
        }
    )
    return result
