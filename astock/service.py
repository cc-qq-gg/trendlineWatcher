"""按参数缓存指数关系与轮动回测结果；请求路径只读 CSV，绝不联网。

两个视图的耗时差得很远：关系图全量 56ms、轮动一次 20ms（含六档敏感性），
但都远高于缓存命中，所以按参数做 LRU。数据刷新是独立动作——`refresh()` 起
后台线程跑增量采集，接口立刻返回，前端轮询 meta 里的 refresh 状态，首屏不会
被 3 秒的采集卡住。
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from pathlib import Path

import pandas as pd

from . import data, momentum, payload, relation

DEFAULT_YEARS = relation.PERCENTILE_YEARS
DEFAULT_START = momentum.ALL_START

STATE_CACHE = 4                     # 参数组合只有几十种，留 4 份够来回切（8 份在这个机器上白占几十 MB）
_CLOSES: dict[str, pd.DataFrame] = {}
_RELATION: OrderedDict[tuple, dict] = OrderedDict()
_MOMENTUM: OrderedDict[tuple, dict] = OrderedDict()

_REFRESH = {"running": False, "started_at": None, "last": None, "error": None}
_LOCK = threading.Lock()


def _key(path) -> str:
    return str(Path(path) if path else data.DEFAULT_PATH)


def closes(path: str | Path | None = None) -> pd.DataFrame:
    """三列宽表（顺序池内两列 + 国债指数），解析一次反复用。"""
    root = _key(path)
    if root not in _CLOSES:
        _CLOSES[root] = data.load_closes(momentum.ALL_CODES, root)
    return _CLOSES[root]


def _remember(cache: OrderedDict, key: tuple, value: dict) -> dict:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > STATE_CACHE:
        cache.popitem(last=False)
    return value


# ---------------- 指数关系 ----------------

def relations(codes: list[str] | None = None, start: str | None = None,
              years: int = relation.PERCENTILE_YEARS, path: str | Path | None = None) -> dict:
    """多条序列同轴对比（原始点位 / 滚动分位 / ERP）。"""
    if not (2 <= int(years) <= 20):
        raise ValueError(f"years={years!r} 需在 [2, 20] 之间")
    root = _key(path)
    key = (root, tuple(codes or ()), start, int(years))
    if key in _RELATION:
        _RELATION.move_to_end(key)
        return _RELATION[key]
    started = time.perf_counter()
    result = relation.build(codes, start, int(years), root)
    result["meta"]["elapsed_ms"] = round((time.perf_counter() - started) * 1000.0, 1)
    return _remember(_RELATION, key, result)


# ---------------- 双动量轮动 ----------------

def _resolve(preset: str, window, entry, exit_threshold, cost_rate) -> dict:
    """预设打底，显式参数覆盖；两边都不给就用优化参数。"""
    base = momentum.preset(preset)
    params = {
        "window": base["window"] if window is None else int(window),
        "entry": base["entry"] if entry is None else float(entry),
        "exit_threshold": base["exit_threshold"] if exit_threshold is None else float(exit_threshold),
        "cost_rate": momentum.COST_PER_SIDE if cost_rate is None else float(cost_rate),
    }
    momentum.check(**params)
    return params


def momentum_view(preset: str = "optimized", window: int | None = None, entry: float | None = None,
                  exit_threshold: float | None = None, cost_rate: float | None = None,
                  years: float | None = None, start: str | None = momentum.ALL_START,
                  detail: bool = True, path: str | Path | None = None) -> dict:
    """L146064 双动量轮动回测（净值 / 指标 / 逐笔 / 敏感性）。"""
    params = _resolve(preset, window, entry, exit_threshold, cost_rate)
    root = _key(path)
    key = (root, preset, params["window"], params["entry"], params["exit_threshold"],
           params["cost_rate"], years, start, bool(detail))
    if key in _MOMENTUM:
        _MOMENTUM.move_to_end(key)
        return _MOMENTUM[key]
    started = time.perf_counter()
    result = momentum.run(closes(root), years=years, start_date=start, detail=bool(detail), **params)
    # 周期按钮上要显示各区间收益：顺手算掉（5 个区间约 10ms），省掉前端再发 5 次请求
    frame = closes(root)
    result["spans"] = payload.round_numbers(momentum.spans(
        frame[momentum.ALL_CODES].to_numpy(dtype=float), frame.index, **params))
    result["meta"]["preset"] = preset
    result["meta"]["label"] = momentum.preset(preset)["label"]
    result["meta"]["elapsed_ms"] = round((time.perf_counter() - started) * 1000.0, 1)
    return _remember(_MOMENTUM, key, result)


def presets() -> list[dict]:
    """两个预设的口径对比，前端下拉框直接用。"""
    return [{"key": name, **momentum.preset(name)} for name in momentum.PRESETS]


# ---------------- 概览与刷新 ----------------

def meta(path: str | Path | None = None) -> dict:
    """页面头部：数据覆盖、序列清单、默认参数、刷新状态。"""
    root = _key(path)
    covered = data.coverage(root, required=tuple(momentum.ALL_CODES))
    ends = [row["end"] for row in covered if row["end"]]
    latest = max(ends) if ends else None
    age_days = None
    if latest:
        age_days = round((pd.Timestamp.today().normalize() - pd.Timestamp(latest)).days, 1)
    return payload.round_numbers({
        "coverage": covered,
        "latest": latest,
        "age_days": age_days,
        "stale": bool(age_days is not None and age_days > 4),   # 跨周末 + 一天余量
        "catalog": payload.catalog(),
        "mom_pool": list(momentum.POOL),
        "mom_bond": momentum.BOND,
        "mom_start": momentum.ALL_START,
        "mom_cost_bp": round(momentum.COST_PER_SIDE * 10000, 2),
        "mom_default": {"window": momentum.WINDOW, "entry": momentum.ENTRY,
                        "exit_threshold": momentum.EXIT},
        "presets": presets(),
        "relation_years": relation.PERCENTILE_YEARS,
        # 关系图默认勾选的三条，前端照这个清单点亮 chip，不要在 JS 里再抄一份
        "relation_default_codes": list(relation.DEFAULT_CODES),
        "refresh": {key: _REFRESH[key] for key in ("running", "started_at", "error")},
        "refresh_last": _REFRESH["last"],
    })


def refresh(path: str | Path | None = None) -> dict:
    """起后台线程做增量采集；已经在跑就直接返回，不排队。"""
    if not _LOCK.acquire(blocking=False):
        return {"started": False, "reason": "已有刷新任务在跑"}
    try:
        if _REFRESH["running"]:
            return {"started": False, "reason": "已有刷新任务在跑"}
        _REFRESH.update(running=True, started_at=pd.Timestamp.now().isoformat(), error=None)
        root = _key(path)
    finally:
        _LOCK.release()

    def worker() -> None:
        try:
            # 时间闸：15:05 之前当日 K 线没定稿，后台刷新只补到前一交易日，写不进盘中快照
            result = data.sync(root, cutoff=data.settled_cutoff())
            _CLOSES.pop(root, None)                 # 数据换了，缓存全部作废
            _RELATION.clear()
            _MOMENTUM.clear()
            _REFRESH["last"] = result
        except Exception as exc:                     # noqa: BLE001 —— 刷新失败不该打挂服务
            _REFRESH["error"] = f"{type(exc).__name__}: {exc}"
        finally:
            _REFRESH["running"] = False

    threading.Thread(target=worker, name="astock-sync", daemon=True).start()
    return {"started": True}


def summary(path: str | Path | None = None) -> str:
    """CLI 用的一屏摘要：数据覆盖 + 两个预设锚点 + 关系图末值。"""
    info = meta(path)
    lines = [f"数据 {info['latest']}（{info['age_days']} 天前，{'偏旧' if info['stale'] else '新鲜'}）"]
    lines += [f"  {row['code']:>10} {row['name']:<12} {row['rows']:>6} 行 {row['start']} → {row['end']}"
              for row in info["coverage"] if not row["derived"]]
    for item in presets():
        result = momentum_view(item["key"], path=path)
        top = result["metrics"][0]
        lines.append(f"{item['label']:<6} 窗口{item['window']} 开仓{item['entry'] * 100:.2f}% "
                     f"CAGR {top['cagr'] * 100:6.2f}% Sharpe {top['sharpe']:5.2f} "
                     f"回撤 {top['maxdd'] * 100:7.2f}% 换仓 {top['switches']:>4} "
                     f"（{result['meta']['elapsed_ms']} ms）")
    for row in relation.latest_reads(path=path):
        pct = f"{row['percentile']:6.2f}%" if row["percentile"] is not None else "     -"
        lines.append(f"  {row['name']:<14} {row['value']:>10.4f}{row['unit']} 近10年分位 {pct}")
    return "\n".join(lines)


if __name__ == "__main__":
    print(summary())
