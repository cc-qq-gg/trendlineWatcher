"""指数关系：多条序列同轴对比（原始点位 / 滚动历史分位 / ERP 派生）。

计算口径抄 bigA/index_series.py，实现换成 numpy：
    原始点位  左轴直接画收盘点位；区间涨跌幅 = 当日 / 窗口首日 − 1（与 bigA 图例一致）
    滚动分位  只回看当日及以前近 N 年，(窗口内 <= 当日) / 窗口长度 × 100，样本不足跳点
    ERP      100 / PE − 10Y 收益率；10Y 只取当日或更早的最近值，缺口超过 7 天丢弃

分位和重定基都是「先在全历史算完，再按 start_date 裁窗口」：裁完再算会让窗口起点
的分位只回看到几天数据，画出来说谎。

bigA 用 bisect + Python 内层循环数窗口（O(n²) 且解释器开销大），这里把定窗一次性
交给 searchsorted，内层每点只剩一次 C 级比较，十条序列十毫秒量级。

启动（工作目录必须是仓库根）：

    uv run python -m astock.relation                    # 打印默认面板的最新读数
    uv run python -m astock.relation --code HS300_PE    # 只看某条序列
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from . import data, payload

PERCENTILE_YEARS = 10
MIN_SAMPLES = 12                       # 滚动窗口里最少样本数，不足就跳过该点
ERP_MAX_GAP_DAYS = 7                   # 10Y 缺价时允许回溯的最大天数
DEFAULT_PERIODS = ("5", "10", "20", "max")
# 默认进关系图的三条：上证（大盘情绪）+ 沪深300（宽基收益）+ 股债性价比（估值水位），
# 其余（创业板 / PE / PMI）留作可选项，避免一上来六条线挤在 0–100 的右轴上。
DEFAULT_CODES = ("sh.000001", "sh.000300", "HS300_ERP")


def visible_codes() -> list[str]:
    """可展示的全部序列：internal（国债指数、纳指ETF、10Y）只服务回测，不展示。"""
    return [code for code, meta in data.SERIES.items() if not meta.get("internal")]


def load_all(codes: list[str] | None = None, path: str | Path = data.DEFAULT_PATH) -> dict[str, tuple]:
    """一次读 CSV 拆出多条序列，返回 {code: (日期 datetime64 数组, 数值数组)}。

    每条序列保留自己的日期：PE 周频、PMI 月频，跟日线对齐会把它们抹空。
    """
    codes = list(codes or visible_codes())
    frame = data.load_long(path)
    out: dict[str, tuple] = {code: (np.empty(0, "datetime64[ns]"), np.empty(0)) for code in codes}
    if frame.empty:
        return out
    frame = frame[frame["code"].isin(codes)]
    frame = frame.drop_duplicates(["code", "trade_date"], keep="last")
    frame["trade_date"] = pd.to_datetime(frame["trade_date"].astype(str))
    frame = frame.sort_values("trade_date")
    for code, part in frame.groupby("code", sort=False):
        out[code] = (part["trade_date"].to_numpy(), part["close"].to_numpy(dtype=float))
    return out


# ---------------- 单序列变换 ----------------

def rebase(dates: np.ndarray, values: np.ndarray, start_date: str | None = None,
           with_base: bool = False):
    """整条序列重定基到 100，再按 start_date 裁窗口（基准始终是裁完后窗口首点）。

    with_base 时额外返回窗口首值：前端左轴画原始点位（对照 bigA 的图），
    区间涨跌幅仍要这个基准，一个标量就够了，不必再下发一份原始值数组。
    """
    if dates.size == 0:
        return (dates, values, None) if with_base else (dates, values)
    lo = 0
    if start_date:
        lo = int(dates.searchsorted(np.datetime64(pd.Timestamp(start_date).to_datetime64()), side="left"))
        if lo >= dates.size:
            return (dates[:0], values[:0], None) if with_base else (dates[:0], values[:0])
    base = float(values[lo])
    if not np.isfinite(base) or base == 0.0:
        raise ValueError("重定基失败：窗口首个值非有限数")
    kept, rebased = dates[lo:], values[lo:] * (100.0 / base)
    return (kept, rebased, base) if with_base else (kept, rebased)


def rolling_percentile(dates: np.ndarray, values: np.ndarray, years: int = PERCENTILE_YEARS,
                       min_samples: int = MIN_SAMPLES) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """滚动历史分位（0–100），返回 (日期, 分位, 原始值)；样本不足的点直接跳过。

    分位包含当日自身，所以最小值是 1/窗口长度而不是 0，和 bigA 一样。
    """
    n = dates.size
    if n < min_samples:
        return dates[:0], values[:0], values[:0]
    offset = np.timedelta64(365 * int(years), "D")
    lows = dates.searchsorted(dates - offset, side="left")      # 一次性定窗
    keeps = np.flatnonzero(np.arange(n) + 1 - lows >= min_samples)
    if keeps.size == 0:
        return dates[:0], values[:0], values[:0]
    quantiles = np.array(
        [(values[low:i + 1] <= values[i]).sum() / (i + 1 - low) * 100.0 for i, low in zip(keeps, lows[keeps])],
        dtype=float,
    )
    return dates[keeps], quantiles, values[keeps]


def derive_erp(pe: tuple, yields: tuple, max_gap_days: int = ERP_MAX_GAP_DAYS) -> tuple:
    """ERP = 100 / PE − 10Y 收益率；10Y 取当日或更早的最近值，缺口过大就丢弃该点。"""
    pe_dates, pe_values = pe
    y_dates, y_values = yields
    if pe_dates.size == 0 or y_dates.size == 0:
        return np.empty(0, "datetime64[ns]"), np.empty(0)
    idx = y_dates.searchsorted(pe_dates, side="right") - 1      # 最近一个不晚于当日的 10Y
    usable = idx >= 0
    gap = (pe_dates[usable] - y_dates[idx[usable]]) / np.timedelta64(1, "D")
    usable[usable] = gap <= max_gap_days
    valid = usable & np.isfinite(pe_values) & (pe_values > 0)
    out_dates = pe_dates[valid]
    out_values = 100.0 / pe_values[valid] - y_values[idx[valid]]
    return out_dates, out_values


# ---------------- 组装 ----------------

def build(codes: list[str] | None = None, start_date: str | None = None,
          years: int = PERCENTILE_YEARS, path: str | Path = data.DEFAULT_PATH) -> dict:
    """指数关系图主入口，返回前端可直接渲染的结构；不传 codes 时用 DEFAULT_CODES。"""
    wanted = list(codes or DEFAULT_CODES)
    unknown = [code for code in wanted if code not in data.SERIES]
    if unknown:
        raise ValueError(f"未知指数代码: {', '.join(unknown)}")
    loaded = load_all(sorted(set(wanted) | set(data.STORED_CODES)), path)

    series_out = []
    for code in wanted:
        meta = data.SERIES[code]
        if meta.get("source") == "derived":
            dates, values = derive_erp(loaded.get("HS300_PE", ()), loaded.get("CN10Y", ()))
            if dates.size == 0:
                continue
        else:
            dates, values = loaded.get(code, (np.empty(0, "datetime64[ns]"), np.empty(0)))
        display = meta.get("display", "rebase")
        window_base = None
        if dates.size == 0:
            points: list[dict] = []
        elif display == "percentile":
            # 分位一律在全历史算完再裁窗口，否则起点分位没有回看样本
            kept_dates, quantiles, raw = rolling_percentile(dates, values, years)
            lo = int(kept_dates.searchsorted(np.datetime64(pd.Timestamp(start_date).to_datetime64()), side="left")) if start_date else 0
            points = payload.points(kept_dates[lo:], quantiles[lo:], raw[lo:])
        else:
            kept, rebased, base = rebase(dates, values, start_date, with_base=True)
            points = payload.points(kept, rebased)
            window_base = base
        entry = {
            "code": code,
            "name": meta["name"],
            "display": display,
            "axis": meta.get("axis", "left"),
            "unit": meta.get("unit", ""),
            "group": meta.get("group", ""),
            "is_base": bool(meta.get("base")),
            "latest": points[-1] if points else None,
            "points": points,
        }
        # 左轴画原始点位：v 是重定基，原值 = v × base / 100；分位序列没有这个口径
        if display != "percentile":
            entry["base"] = round(float(window_base), 6) if window_base else None
        if meta.get("note"):
            entry["note"] = meta["note"]
        series_out.append(entry)

    stamps = [point["t"] for item in series_out for point in item["points"]]
    latest = pd.Timestamp(max(stamps), unit="ms") if stamps else None
    return {
        "meta": {
            "start": start_date,
            "end": latest.strftime("%Y-%m-%d") if latest is not None else None,
            "percentile_years": int(years),
            "periods": list(DEFAULT_PERIODS),
        },
        "series": series_out,
    }


def latest_reads(codes: list[str] | None = None, path: str | Path = data.DEFAULT_PATH) -> list[dict]:
    """每条序列的当前读数 + 近 N 年分位，供 CLI 与概览接口复用（只读一次 CSV）。"""
    wanted = list(codes or visible_codes())
    unknown = [code for code in wanted if code not in data.SERIES]
    if unknown:
        raise ValueError(f"未知指数代码: {', '.join(unknown)}")
    need = set(wanted) | {"HS300_PE", "CN10Y"}
    loaded = load_all(sorted(need), path)
    out = []
    for code in wanted:
        meta = data.SERIES[code]
        if meta.get("source") == "derived":
            dates, values = derive_erp(loaded["HS300_PE"], loaded["CN10Y"])
        else:
            dates, values = loaded[code]
        if dates.size == 0:
            continue
        row = {
            "code": code,
            "name": meta["name"],
            "date": pd.Timestamp(dates[-1]).strftime("%Y-%m-%d"),
            "value": round(float(values[-1]), 4),
            "unit": meta.get("unit", ""),
        }
        _, quantiles, _ = rolling_percentile(dates, values)
        row["percentile"] = round(float(quantiles[-1]), 2) if quantiles.size else None
        out.append(row)
    return out


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--code", action="append", help="只看指定序列，可重复")
    parser.add_argument("--start", default=None, help="窗口起点 YYYY-MM-DD")
    args = parser.parse_args()

    for row in latest_reads(args.code):
        pct = f"{row['percentile']:>6.2f}%" if row["percentile"] is not None else "     -"
        print(f"{row['date']}  {row['name']:<14} {row['value']:>12.4f}{row['unit']:<3} 近10年分位 {pct}")

    if args.code:
        payload = build(args.code, args.start)
        print(f"\n窗口 {payload['meta']['start']} → {payload['meta']['end']}")
        for item in payload["series"]:
            points = item["points"]
            span = f"{points[0]['v']:.2f} → {points[-1]['v']:.2f}" if points else "无数据"
            print(f"{item['name']:<14} {item['display']:<11} {item['axis']:<5} {len(points):>5} 点  {span}")
