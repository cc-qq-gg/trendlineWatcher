"""JSON 契约：时间键、点位数组、指标取整与序列白名单。

计算层（momentum / relation）只产出 numpy 数组和本地 dict，凡是「前端要怎么读」
的规定都集中在这里，两个模块共用一份，不再各写一个 _points。

时间键约定：t 是交易日零点的 UTC 毫秒。因为不带时区，前端
`new Date(t).toISOString().slice(0, 10)` 能原样还原成 YYYY-MM-DD，不会被本地时区挪一天。
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from . import data


def time_ms(dates: pd.DatetimeIndex | np.ndarray) -> list[int]:
    """DatetimeIndex 或 datetime64 数组 -> UTC 毫秒整数列表。

    不能拿 asi8 直接除 1e6：pandas 3 的 DatetimeIndex 是 datetime64[us]，
    那样算出来是「秒」，前端 new Date(t) 会整体落到 1970 年。先统一转成 ms。
    """
    values = dates.to_numpy() if isinstance(dates, pd.DatetimeIndex) else np.asarray(dates)
    return values.astype("datetime64[ms]").astype("int64").tolist()


def _clean(values) -> list:
    """取整到 4 位，非有限数直接给 None（JSON 不接受 NaN/Infinity）。

    点位是整包里最庞大的部分，逐点走 round_numbers 会白烧几十毫秒，
    所以有限性在这一层就地保证，让上游只需要管小结构。
    """
    rounded = np.round(np.asarray(values, dtype=float), 4).tolist()
    return [value if math.isfinite(value) else None for value in rounded]


def points(dates, values, raw=None) -> list[dict]:
    """点位数组；raw 只给分位类序列附带原始读数（悬浮框里要看到 PE 本身而不是分位）。

    先脱掉 numpy 标量再组装字典，八千点的序列比逐元素索引快好几倍。
    """
    stamp = time_ms(dates)
    body = _clean(values)
    if raw is None:
        return [{"t": t, "v": v} for t, v in zip(stamp, body)]
    return [{"t": t, "v": v, "raw": r} for t, v, r in zip(stamp, body, _clean(raw))]


def round_numbers(obj, digits: int = 6):
    """递归收敛 float 精度，并把非有限数转成 None。

    JSON 不允许 NaN/Infinity（RFC 8259），一旦漏进去浏览器 JSON.parse 直接白屏，
    所以出口统一在这里过滤，而不是指望每个计算式都恰好不除零。
    """
    if isinstance(obj, dict):
        return {key: round_numbers(value, digits) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [round_numbers(value, digits) for value in obj]
    if isinstance(obj, bool) or obj is None or isinstance(obj, (int, str)):
        return obj
    if isinstance(obj, float):
        return round(obj, digits) if math.isfinite(obj) else None
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return round(float(obj), digits) if math.isfinite(float(obj)) else None
    return obj


def catalog() -> list[dict]:
    """序列白名单：只下发前端要用的字段，secid 之类的采集细节留在 data 层。"""
    out = []
    for code, meta in data.SERIES.items():
        entry = {
            "code": code,
            "name": meta["name"],
            "display": meta.get("display", "rebase"),
            "axis": meta.get("axis", "left"),
            "unit": meta.get("unit", ""),
            "group": meta.get("group", ""),
            "internal": bool(meta.get("internal")),
            "derived": meta.get("source") == "derived",
        }
        if meta.get("note"):
            entry["note"] = meta["note"]
        out.append(entry)
    return out

