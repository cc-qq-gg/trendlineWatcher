"""L146064 双动量轮动（规则来自 bigA/momentum_backtest.py，实现重写为纯 numpy）。

规则：window 日涨幅排名第一且严格大于 entry 时持有该标的；持仓失去第一名，或
自身 window 日涨幅跌破 exit_threshold 则平仓；空仓持有国债指数。T 日收盘出信号并按 T 日
收盘价成交，收益自下一交易日起算（position = target.shift(1)），换仓日成本 =
Σ|Δpos| × 单边费率。

与 bigA 的差别只在实现：那边把持仓展开成 one-hot DataFrame 再逐列相乘，这里把
持仓压成一个「持有列号」向量，一次 fancy indexing 出净收益，参数网格能跑到千级
组合（见 search）；口径逐位对齐，等价性由 tests/test_astock_momentum.py 验证。

预设：reference 是 ycyflh L146064 原规则（±0.5%），optimized 是 279 万组全网格
（窗口 10-40 × 开仓/平仓 0.05%-15% 步长 0.05%）按 Calmar 取最优：开仓 3.45%、
平仓 1.05%。窗口 20 日是唯一回撤可控的窗口，19/21 日的最优点落在完全不同的阈值上、
27 日以后 Calmar 掉到 0.9 以下，所以「窗口」这一维本身就是尖峰而非平台；优化参数是
样本内寻优，不宜外推。另有一个结构性事实：平仓阈值 ≥ 开仓阈值时是无效参数
（`best > entry` 已先命中，粘滞分支永远不会单独生效）。
"""

from __future__ import annotations

import itertools
import math
from dataclasses import dataclass

import numpy as np
import pandas as pd

from . import payload

POOL = ["sz.399006", "513100_PX"]          # 列 0、1
BOND = "sh.000012"                          # 列 2：空仓替代
ALL_CODES = POOL + [BOND]
BOND_COL = len(POOL)                        # 目标里的 -1 映射到这一列

WINDOW = 20
ENTRY = 0.0345                              # 优化参数（默认）：开仓阈值
EXIT = 0.0105                               # 优化参数（默认）：平仓阈值
ALL_START = "2014-01-02"                    # 三条池内序列的共同历史起点

PRESETS = {
    "reference": {"label": "参考参数", "window": 20, "entry": 0.005, "exit_threshold": -0.005,
                  "note": "ycyflh L146064 原始规则"},
    "optimized": {"label": "优化参数", "window": 20, "entry": 0.0345, "exit_threshold": 0.0105,
                  "note": "279 万组全网格寻优（Calmar 最高），非参考站规则"},
}

# BTC 双动量（bigA 宽网格总收益前三）：池内是 BTC 现货与创业板指，空仓仍持国债指数。
# BTC 日线取 07:00 UTC（北京 15:00）收盘口径，与 A 股同一天对齐，见 btcdata.py。
BTC_POOL = ["BTC", "sz.399006"]
BTC_BOND = "sh.000012"
BTC_ALL_CODES = BTC_POOL + [BTC_BOND]
BTC_WINDOW = 24
BTC_ENTRY = 0.095
BTC_EXIT = 0.026
BTC_START = "2019-01-02"                    # BTC 现货 5 分钟线起点
BTC_SPAN_YEARS = (1, 3, 5, 0)               # 样本只有 7 年，10 年档与「全部」重合，去掉

BTC_PRESETS = {
    "top1": {"label": "总收益第 1", "window": 24, "entry": 0.095, "exit_threshold": 0.026,
             "note": "50388 组宽网格按区间总收益排序第 1（收盘口径）"},
    "top2": {"label": "总收益第 2", "window": 24, "entry": 0.095, "exit_threshold": 0.022,
             "note": "同上网格第 2，破位更敏感"},
    "top3": {"label": "总收益第 3", "window": 24, "entry": 0.095, "exit_threshold": 0.034,
             "note": "同上网格第 3，破位更宽容"},
}

COST_PER_SIDE = 0.0004
RF_ANNUAL = 0.02
TRADING_DAYS = 252
SPAN_YEARS = (1, 3, 5, 10, 0)      # 页面周期按钮：最近 N 年，0 = 全部
LIMITS = {"window": (2, 250), "entry": (-0.5, 0.5), "exit_threshold": (-0.5, 0.5), "cost_rate": (0.0, 0.02)}


@dataclass(frozen=True)
class Pool:
    """一套轮动配置：池内标的、空仓替代、默认参数与预设。"""
    key: str
    label: str
    pool: list[str]
    bond: str
    presets: dict
    window: int
    entry: float
    exit_threshold: float
    start: str
    spans: tuple[int, ...] = SPAN_YEARS
    bond_label: str = "国债指数"

    @property
    def all_codes(self) -> list[str]:
        return [*self.pool, self.bond]

    @property
    def bond_col(self) -> int:
        return len(self.pool)


ASHARE = Pool("ashare", "A 股双动量", POOL, BOND, PRESETS, WINDOW, ENTRY, EXIT, ALL_START)
BTC = Pool("btc", "BTC 双动量", BTC_POOL, BTC_BOND, BTC_PRESETS, BTC_WINDOW, BTC_ENTRY, BTC_EXIT,
           BTC_START, BTC_SPAN_YEARS)
POOLS = {item.key: item for item in (ASHARE, BTC)}


def preset(name: str, pool: Pool | None = None) -> dict:
    """按名称取预设副本，未知预设抛 ValueError。"""
    pc = pool or ASHARE
    item = pc.presets.get(name)
    if item is None:
        raise ValueError(f"unknown preset {name!r}, expect {sorted(pc.presets)}")
    return dict(item)


def check(window: int, entry: float, exit_threshold: float, cost_rate: float = COST_PER_SIDE) -> None:
    """参数越界在计算之前就报错，接口层统一转 400。"""
    for name, value in (("window", window), ("entry", entry), ("exit_threshold", exit_threshold), ("cost_rate", cost_rate)):
        low, high = LIMITS[name]
        if value is None or (isinstance(value, float) and not math.isfinite(value)):
            raise ValueError(f"{name} must be a finite number")
        if not (low <= value <= high):
            raise ValueError(f"{name}={value!r} out of range [{low}, {high}]")
    if isinstance(window, bool) or int(window) != window:
        raise ValueError(f"window={window!r} must be int")


def rule_text(window: int, entry: float, exit_threshold: float, bond_label: str = "国债指数") -> str:
    return (f"{window}日涨幅排名第一且>{entry * 100:+.2f}%时持有，"
            f"失去第一或跌破{exit_threshold * 100:+.2f}%平仓，空仓持{bond_label}")


# ---------------- 信号 ----------------

def momentum_returns(prices: np.ndarray, window: int) -> np.ndarray:
    """window 日涨幅，前 window 行为 NaN；形状与 prices 一致。"""
    n = prices.shape[0]
    out = np.full((n, prices.shape[1]), np.nan)
    if n > window:
        out[window:] = prices[window:] / prices[:-window] - 1.0
    return out


def target_index(prices: np.ndarray, window: int = WINDOW, entry: float = ENTRY,
                 exit_threshold: float = EXIT, pool: Pool | None = None) -> np.ndarray:
    """逐日目标持仓列号：-1 = 空仓（持债），0..k-1 = 池内标的。

    状态机必须串行推进（今天的持仓取决于昨天），但涨幅、第一名都已向量化，
    循环体只剩几个标量比较，3000 个交易日约 1ms。

    排名只在池内各列之间做，空仓替代品种不参与竞争；热身期任一列还没涨幅数据就
    整行跳过，与 bigA 的 `np.isnan(row).any()` 同口径。
    """
    ret = momentum_returns(prices[:, :len((pool or ASHARE).pool)], window)
    valid = ~np.isnan(ret).any(axis=1)
    leaders = np.argmax(np.where(np.isnan(ret), -np.inf, ret), axis=1)
    best = ret[np.arange(ret.shape[0]), leaders]
    leaders = leaders.tolist()
    best = best.tolist()
    columns = [column.tolist() for column in ret.T]     # columns[资产][日]，比 ret[i, h] 快一截

    holds = np.full(ret.shape[0], -1, dtype=np.int8)
    holding = -1
    for i in range(ret.shape[0]):
        if not valid[i]:
            continue
        leader = leaders[i]
        if holding >= 0:
            if holding == leader and columns[holding][i] >= exit_threshold:
                current = holding
            elif best[i] > entry:
                current = leader
            else:
                current = -1
        else:
            current = leader if best[i] > entry else -1
        holding = current
        holds[i] = current
    return holds


def held_columns(holds: np.ndarray, pool: Pool | None = None) -> np.ndarray:
    """信号滞后一天变成实际持有列；-1 表示空仓无持仓（首日）。"""
    bond_col = (pool or ASHARE).bond_col
    n = holds.shape[0]
    held = np.empty(n, dtype=np.int16)
    held[0] = -1
    held[1:] = np.where(holds[:-1] < 0, bond_col, holds[:-1])
    return held


# ---------------- 净值 ----------------

def net_returns(prices: np.ndarray, held: np.ndarray, cost_rate: float) -> tuple[np.ndarray, np.ndarray]:
    """日净收益与换手额（Σ|Δpos|，满仓换一次 = 2）。

    首日既没有持仓也没有换手，和 bigA 里 positions 全零 + turnover.iloc[0]=0 一致。
    """
    n = prices.shape[0]
    gross = np.zeros(n)
    if n > 1:
        change = prices[1:] / prices[:-1] - 1.0
        open_pos = held[1:] >= 0
        gross[1:][open_pos] = change[np.arange(n - 1)[open_pos], held[1:][open_pos]]

    prev = np.empty(n, dtype=np.int16)
    prev[0] = -1
    prev[1:] = held[:-1]
    swapped = held != prev
    both = swapped & (held >= 0) & (prev >= 0)
    turnover = np.zeros(n)
    turnover[both] = 2.0
    turnover[swapped & ~both] = 1.0
    turnover[0] = 0.0
    return gross - turnover * cost_rate, turnover


def metrics(nav: np.ndarray, years: float) -> dict:
    """CAGR / 年化波动 / Sharpe / 最大回撤 / Calmar，口径与 bigA._series_metrics 一致。"""
    total = float(nav[-1] / nav[0]) if nav.size > 1 else 1.0
    cagr = total ** (1.0 / years) - 1.0 if years > 0 and total > 0 else 0.0
    daily = np.diff(nav) / nav[:-1] if nav.size > 1 else np.empty(0)
    std = float(daily.std(ddof=1)) if daily.size > 1 else 0.0
    vol = std * math.sqrt(TRADING_DAYS)
    sharpe = ((float(daily.mean()) - RF_ANNUAL / TRADING_DAYS) / std * math.sqrt(TRADING_DAYS)
              if std > 0 else 0.0)
    drawdown = nav / np.maximum.accumulate(nav) - 1.0
    maxdd = float(drawdown.min()) if drawdown.size else 0.0
    return {
        "cagr": cagr,
        "vol": vol,
        "sharpe": sharpe,
        "maxdd": maxdd,
        "calmar": cagr / abs(maxdd) if maxdd < 0 else 0.0,
        "drawdown": drawdown,
    }


def elapsed_years(dates: pd.DatetimeIndex) -> float:
    days = (dates[-1] - dates[0]).days
    return days / 365.25 if days > 0 else len(dates) / TRADING_DAYS


def window_start(dates: pd.DatetimeIndex, years: float | None, start_date: str | None, warmup: int) -> int:
    """窗口起点下标：优先 start_date，其次最近 years 年，都不早于热身期。"""
    lo = min(warmup, len(dates) - 1)
    if start_date:
        cut = max(pd.Timestamp(start_date), dates[lo])
    elif years:
        cut = max(dates[-1] - pd.Timedelta(days=int(365 * years)), dates[lo])
    else:
        return lo
    return int(dates.searchsorted(cut, side="left"))


# ---------------- 明细 ----------------

def _num(value: float, digits: int) -> float | None:
    """取整并挡掉非有限数（JSON 不接受 NaN/Infinity）。"""
    return round(value, digits) if math.isfinite(value) else None


def episodes(dates: pd.DatetimeIndex, prices: np.ndarray, holds: np.ndarray,
             nav: np.ndarray, cost_rate: float, lo: int,
             pool: Pool | None = None) -> tuple[list[dict], list[dict], list[dict]]:
    """按信号日切持仓段，并汇出逐笔成交与年度统计（口径同 bigA._l146064_trade_log）。

    成交价取信号日收盘价；段收益按段首/段尾收盘价计算，费后收益开、平各收一次单边
    成本（换仓是真实的双边成交，切去持币国债也要付费）；窗口首日继承的仓位没有买入
    记录，不收开仓成本；未平仓段收益是浮动收益。
    """
    pc = pool or ASHARE
    n = len(dates)
    labels = dates.strftime("%Y-%m-%d").tolist()        # 逐段取 DatetimeIndex 标量太贵，先摊平
    bounds = (np.flatnonzero(np.diff(holds[lo:]) != 0) + lo + 1).tolist()
    starts = [lo, *bounds]
    segments: list[dict] = []
    for position, start in enumerate(starts):
        closed = position + 1 < len(starts)
        # 换仓日既是旧段的收尾也是新段的开头：买卖同日按同一根收盘价成交，
        # 这样相邻段的段收益连乘就还原回策略净值
        close_idx = starts[position + 1] if closed else n - 1
        col = int(holds[start])
        open_col = col if col >= 0 else pc.bond_col
        open_price = float(prices[start, open_col])
        close_price = float(prices[close_idx, open_col])
        gross = close_price / open_price - 1.0
        inherited = position == 0 and col >= 0
        open_cost = 0.0 if inherited else cost_rate
        close_cost = cost_rate if closed else 0.0
        segments.append({
            "code": pc.bond if col < 0 else pc.all_codes[col],
            "open_date": labels[start],
            "close_date": labels[close_idx],
            "open_price": _num(open_price, 4),
            "close_price": _num(close_price, 4),
            "gross_ret": _num(gross, 6),
            "net_ret": _num(gross - open_cost - close_cost, 6),
            "days": int(close_idx - start),
            "closed": bool(closed),
            "in_pool": col >= 0,
        })

    # 每次换仓拆成「买入新的 + 卖掉旧的」两条成交记录，换仓之间按时间倒序（最新在前）
    trades: list[dict] = []
    for index in range(len(segments) - 1, 0, -1):
        cur, prev = segments[index], segments[index - 1]
        trades.append({"date": cur["open_date"], "code": cur["code"], "side": "buy",
                       "position": "0%→100%", "price": cur["open_price"], "ret": None})
        trades.append({"date": prev["close_date"], "code": prev["code"], "side": "sell",
                       "position": "100%→0%", "price": prev["close_price"], "ret": prev["net_ret"]})

    years = dates[lo:].year.to_numpy()      # 与 nav 等长：nav 已经是裁好窗口的净值
    pool_rows = [row for row in segments if row["in_pool"]]
    by_year: dict[int, list[dict]] = {}
    for row in pool_rows:
        by_year.setdefault(int(row["close_date"][:4]), []).append(row)
    yearly = []
    # 以窗口内的每个年份为基准，而不是只看有持仓的年份：全年空仓（一直持债）也是
    # 一年的业绩，缺了这行「每年收益」就会跳年
    for year in sorted({int(item) for item in years}, reverse=True):
        items = by_year.get(year, [])
        inside = np.flatnonzero(years == year)
        before = np.flatnonzero(years < year)
        base = float(nav[before[-1]]) if before.size else float(nav[0])
        yearly.append({
            "year": year,
            "count": len(items),
            # 没有池内持仓段时胜率与均持无定义，给 None 让前端显示「—」
            "win_rate": _num(sum(1 for item in items if item["net_ret"] > 0) / len(items), 6) if items else None,
            "avg_days": _num(sum(item["days"] for item in items) / len(items), 2) if items else None,
            "return": _num(float(nav[inside[-1]]) / base - 1.0, 6),
        })
    return trades, segments, yearly


# ---------------- 组装 ----------------

def _monthly_win_rate(nav: np.ndarray, bench: np.ndarray, dates: pd.DatetimeIndex) -> float:
    """月度收益跑赢创业板指的比例（月末对月末，口径同 bigA 的 resample('ME').last()）。"""
    period = dates.to_period("M").astype("int64").to_numpy()
    # 变化点 +1 是新月份的首日，所以月末下标要再退一格；最后补上当月最后一个交易日
    edges = np.flatnonzero(np.diff(period) != 0)
    month_ends = np.append(edges, len(period) - 1)
    if month_ends.size < 2:
        return 0.0
    strat = nav[month_ends[1:]] / nav[month_ends[:-1]] - 1.0
    base = bench[month_ends[1:]] / bench[month_ends[:-1]] - 1.0
    return float((strat > base).mean())


def engine(prices: np.ndarray, window: int, entry: float, exit_threshold: float,
           cost_rate: float = COST_PER_SIDE, pool: Pool | None = None) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """全历史信号：(目标列号, 滞后持有列号, 净收益, 换手额)。

    参数网格每换一次阈值就要重跑一遍状态机，但只有这一步和参数有关，
    后面的分段统计都是在净收益数组上切片，所以寻优不额外吃性能。
    """
    holds = target_index(prices, int(window), float(entry), float(exit_threshold), pool)
    held = held_columns(holds, pool)
    net, turnover = net_returns(prices, held, cost_rate)
    return holds, held, net, turnover


def performance(prices: np.ndarray, dates: pd.DatetimeIndex, window: int | None = None,
                entry: float | None = None, exit_threshold: float | None = None,
                cost_rate: float = COST_PER_SIDE,
                years: float | None = None, start_date: str | None = None,
                pool: Pool | None = None) -> dict:
    """只算指标不算明细，给参数对比与网格寻优复用（一次调用约 2ms）。

    参数为空时取池配置的默认值；start_date 为空表示不裁起点（从热身期末开始），
    由调用方决定是否传池起点——直接在这里兑底会用 start_date 把 years 挡掉。
    """
    pc = pool or ASHARE
    window = pc.window if window is None else window
    entry = pc.entry if entry is None else entry
    exit_threshold = pc.exit_threshold if exit_threshold is None else exit_threshold
    check(window, entry, exit_threshold, cost_rate)
    holds, held, net_all, turnover_all = engine(prices, window, entry, exit_threshold, cost_rate, pc)
    lo = window_start(dates, years, start_date, int(window))
    span = slice(lo, len(dates))
    seg = net_all[span]
    nav = 100.0 * np.cumprod(1.0 + seg) / (1.0 + seg[0])
    turn = turnover_all[span].copy()
    turn[0] = 0.0                       # 窗口首日沿用上一段持仓，不算一次换仓
    span_years = elapsed_years(dates[span])
    stats = metrics(nav, span_years)
    # 开仓/换仓计数先在全历史上 shift 再切窗口，否则窗口首日已持仓会被误记成一次开仓
    in_pool = holds >= 0
    prev_in_pool = np.append(False, in_pool[:-1])
    prev_hold = np.append(holds[0], holds[:-1])
    return {
        "holds": holds,
        "held": held,
        "net": seg,
        "nav": nav,
        "turnover": turn,
        "lo": lo,
        "years": span_years,
        "stats": stats,
        "switches": int((turn > 1e-12).sum()),
        "turnover_annual": float(turn.sum()) / span_years if span_years > 0 else 0.0,
        "entries": int((in_pool & ~prev_in_pool)[span].sum()),
        "hold_changes": int((in_pool & (holds != prev_hold))[span].sum()),
        "bond_pct": float((held[span] == pc.bond_col).mean()),
    }


def spans(prices: np.ndarray, dates: pd.DatetimeIndex, window: int | None = None,
          entry: float | None = None, exit_threshold: float | None = None,
          cost_rate: float = COST_PER_SIDE,
          years_list: tuple[int, ...] | None = None, pool: Pool | None = None) -> list[dict]:
    """各周期（最近 N 年 / 0 表示全部）的区间总收益，给页面周期按钮直接用。

    只复用 performance（一次约 2ms），不构造净值序列与逐笔——按钮上只要两个数。
    years=0 与前端「全部」按钮同值，落到池配置的起点；有 years 时不能同时给 start_date，
    否则 window_start 里 start_date 会压过 years，1/3/5/10 年档全被拉成全历史。
    """
    pc = pool or ASHARE
    out = []
    for years in (pc.spans if years_list is None else years_list):
        perf = performance(prices, dates, window, entry, exit_threshold, cost_rate,
                           years=int(years) or None,
                           start_date=None if years else pc.start, pool=pc)
        nav = perf["nav"]
        out.append({
            "years": int(years),
            "start": dates[perf["lo"]].strftime("%Y-%m-%d"),
            "end": dates[-1].strftime("%Y-%m-%d"),
            "total": float(nav[-1] / nav[0] - 1.0),
            "cagr": perf["stats"]["cagr"],
        })
    return out


def run(closes: pd.DataFrame, window: int | None = None, entry: float | None = None,
        exit_threshold: float | None = None, cost_rate: float = COST_PER_SIDE,
        years: float | None = None, start_date: str | None = None, detail: bool = True,
        pool: Pool | None = None) -> dict:
    """双动量轮动回测主入口，返回前端可直接渲染的结构。

    信号一律在完整历史上算完再裁窗口，否则窗口首日会被热身期污染。
    window/entry/exit_threshold 为空时取池配置的默认值（A 股或 BTC）。
    """
    pc = pool or ASHARE
    window = pc.window if window is None else window
    entry = pc.entry if entry is None else entry
    exit_threshold = pc.exit_threshold if exit_threshold is None else exit_threshold
    missing = [code for code in pc.all_codes if code not in closes.columns]
    if missing:
        raise ValueError(f"缺少指数数据: {', '.join(missing)}")
    if len(closes) <= window + 1:
        raise ValueError(f"指数数据不足，至少需要 {window + 2} 个共同交易日")
    dates = closes.index
    prices = closes[pc.all_codes].to_numpy(dtype=float)

    perf = performance(prices, dates, window, entry, exit_threshold, cost_rate, years, start_date, pc)
    lo = perf["lo"]
    span = slice(lo, len(dates))
    window_dates = dates[span]
    stats = perf["stats"]

    metrics_rows = [{
        "code": "STRATEGY",
        "cagr": stats["cagr"], "vol": stats["vol"], "sharpe": stats["sharpe"],
        "maxdd": stats["maxdd"], "calmar": stats["calmar"],
        "total": float(perf["nav"][-1] / perf["nav"][0] - 1.0),
        "turnover": perf["turnover_annual"], "switches": perf["switches"],
        "entries": perf["entries"], "hold_changes": perf["hold_changes"],
        "bond_pct": perf["bond_pct"],
        "win_rate": 0.0,
    }]
    bench_navs = {}
    for col, code in enumerate(pc.all_codes):
        series = prices[span, col]
        bench_navs[code] = 100.0 * series / series[0]
        bench = metrics(bench_navs[code], perf["years"])
        metrics_rows.append({
            "code": code, "cagr": bench["cagr"], "vol": bench["vol"], "sharpe": bench["sharpe"],
            "maxdd": bench["maxdd"], "calmar": bench["calmar"],
            "total": float(bench_navs[code][-1] / 100.0 - 1.0),
            "turnover": 0.0, "switches": 0, "entries": 0, "hold_changes": 0,
            "bond_pct": 1.0 if code == pc.bond else 0.0, "win_rate": 0.0,
        })
    metrics_rows[0]["win_rate"] = _monthly_win_rate(perf["nav"], bench_navs[pc.pool[0]], window_dates)

    result = {
        "meta": {
            "strategy": "momentum",
            "start": window_dates[0].strftime("%Y-%m-%d"),
            "end": window_dates[-1].strftime("%Y-%m-%d"),
            "bars": len(window_dates),
            "years": round(perf["years"], 3),
            "pool": list(pc.pool),
            "bond": pc.bond,
            "pool_key": pc.key,
            "window": int(window),
            "entry": float(entry),
            "exit_threshold": float(exit_threshold),
            "cost_rate": float(cost_rate),
            "rule": rule_text(int(window), float(entry), float(exit_threshold), pc.bond_label),
            "signal_lag_days": 1,
        },
        "metrics": payload.round_numbers(metrics_rows),
        "series": [],
        "drawdown": [],
        "trades": [],
        "episodes": [],
        "yearly": [],
        "sensitivity": [],
    }
    if not detail:
        return result

    result["series"] = (
        [{"code": "STRATEGY", "points": payload.points(window_dates, perf["nav"])}]
        + [{"code": code, "points": payload.points(window_dates, bench_navs[code])} for code in pc.all_codes]
    )
    result["drawdown"] = payload.points(window_dates, np.minimum(stats["drawdown"], 0.0))
    trades, segments, yearly = episodes(dates, prices, perf["holds"], perf["nav"], cost_rate, lo, pc)
    result["trades"] = trades
    result["episodes"] = segments
    result["yearly"] = yearly

    for label, override in (
        ("成本×2", {"cost_rate": cost_rate * 2}),
        ("成本×3", {"cost_rate": cost_rate * 3}),
        (f"窗口{int(window) - 1}", {"window": int(window) - 1}),
        (f"窗口{int(window) + 1}", {"window": int(window) + 1}),
        (f"开仓{entry * 50:.2f}%", {"entry": entry * 0.5}),
        (f"开仓{entry * 200:.2f}%", {"entry": entry * 2}),
    ):
        params = {"window": window, "entry": entry, "exit_threshold": exit_threshold, "cost_rate": cost_rate}
        params.update(override)
        try:
            other = performance(prices, dates, years=years, start_date=start_date, pool=pc, **params)
        except ValueError:
            continue
        row = other["stats"]
        # stats 里还挂着逐根 drawdown 数组，只取标量，别把它带进 JSON
        result["sensitivity"].append({
            "label": label, "switches": other["switches"],
            "cagr": row["cagr"], "sharpe": row["sharpe"], "maxdd": row["maxdd"], "calmar": row["calmar"],
        })
    result["sensitivity"] = payload.round_numbers(result["sensitivity"])
    return result


def period_ranges(dates: pd.DatetimeIndex, window: int,
                  train: tuple[str, str] = ("2014-01-02", "2020-12-31"),
                  test_start: str = "2021-01-01") -> dict[str, tuple[int, int]]:
    """分段回测的下标区间；all 从共同历史起点算，train/test 用来判断是否样本内自嗨。"""
    n = len(dates)
    ranges = {
        "all": (window_start(dates, None, ALL_START, window), n),
        "train": (int(dates.searchsorted(pd.Timestamp(train[0]))),
                  int(dates.searchsorted(pd.Timestamp(train[1]), side="right"))),
        "test": (int(dates.searchsorted(pd.Timestamp(test_start))), n),
    }
    for years in (1, 3, 5, 10):
        ranges[str(years)] = (window_start(dates, float(years), None, window), n)
    return ranges


def search(prices: np.ndarray, dates: pd.DatetimeIndex, grid,
           cost_rate: float = COST_PER_SIDE, periods: tuple[str, ...] | None = None,
           train: tuple[str, str] = ("2014-01-02", "2020-12-31"),
           test_start: str = "2021-01-01") -> list[dict]:
    """参数网格寻优：每个组合给出各分段的 CAGR/Sharpe/回撤与换仓次数。

    这就是得出 optimized 预设的那把尺子。每个组合只跑一遍状态机，分段只在切好的
    净收益数组上重定基，所以千级网格也只是秒级；只看全样本 CAGR 会挑到尖峰，必须
    同时看窗口邻域和验证段，样本内外都站得住才算平台。
    """
    rows = []
    for window, entry, exit_threshold in grid:
        _, _, net, turnover = engine(prices, window, entry, exit_threshold, cost_rate)
        ranges = period_ranges(dates, int(window), train, test_start)
        item = {"window": int(window), "entry": float(entry), "exit_threshold": float(exit_threshold)}
        for label in (periods or tuple(ranges)):
            lo, hi = ranges[label]
            if hi - lo < 2:
                continue
            seg = net[lo:hi]
            nav = 100.0 * np.cumprod(1.0 + seg) / (1.0 + seg[0])
            stats = metrics(nav, elapsed_years(dates[lo:hi]))
            item[f"{label}_cagr"] = round(stats["cagr"], 6)
            item[f"{label}_sharpe"] = round(stats["sharpe"], 4)
            item[f"{label}_maxdd"] = round(stats["maxdd"], 6)
        lo_all, hi_all = ranges["all"]
        span = slice(lo_all, hi_all)
        item["switches"] = int((turnover[span] > 1e-12).sum())
        rows.append(item)
    return rows


def grid_coarse():
    """粗网格：窗口 5-40 日 × 开仓 0-5% × 平仓 0 至 -4%，用于定位平台在哪个区域。"""
    return list(itertools.product(
        range(5, 41, 5),
        (0.0, 0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05),
        (0.0, -0.005, -0.01, -0.02, -0.03, -0.04),
    ))


def grid_fine():
    """细网格：窗口 16-24 日 × 开仓 1.5-5%（步长 0.25%）× 平仓 0 至 -1%。"""
    return [
        (window, 0.015 + 0.0025 * step, exit_threshold)
        for window in range(16, 25)
        for step in range(15)
        for exit_threshold in (0.0, -0.0025, -0.005, -0.01)
    ]


def _row_text(tag: str, row: dict) -> str:
    stats = row.get("stats", row)
    return (f"{tag:<26} CAGR {stats['cagr'] * 100:>6.2f}%  Sharpe {stats['sharpe']:>5.2f}  "
            f"最大回撤 {stats['maxdd'] * 100:>7.2f}%  Calmar {stats['calmar']:>5.2f}  "
            f"换仓 {row.get('switches', 0):>4}")


if __name__ == "__main__":
    import argparse
    import time

    from . import data

    parser = argparse.ArgumentParser()
    parser.add_argument("--grid", choices=("coarse", "fine"), help="跑参数网格而不是打印锚点")
    parser.add_argument("--top", type=int, default=12)
    parser.add_argument("--sort", default="all_cagr", help="网格排序字段")
    args = parser.parse_args()

    closes = data.load_closes(ALL_CODES)
    dates, prices = closes.index, closes[ALL_CODES].to_numpy(dtype=float)
    print(f"样本 {dates[0].date()} → {dates[-1].date()}，{len(dates)} 个共同交易日")

    if args.grid:
        started = time.perf_counter()
        rows = search(prices, dates, grid_coarse() if args.grid == "coarse" else grid_fine())
        frame = pd.DataFrame(rows)
        print(f"{len(rows)} 个组合，耗时 {time.perf_counter() - started:.1f}s"
              f"（{(time.perf_counter() - started) / max(len(rows), 1) * 1000:.1f} ms/个）")
        columns = [c for c in ("window", "entry", "exit_threshold", "all_cagr", "all_sharpe",
                               "all_maxdd", "10_cagr", "train_cagr", "test_cagr", "test_sharpe",
                               "switches") if c in frame.columns]
        for _, row in frame.sort_values(args.sort, ascending=False).head(args.top)[columns].iterrows():
            print(f"w={int(row.window):>2} e={row.entry * 100:>5.2f}% x={row.exit_threshold * 100:>6.2f}% " +
                  " ".join(f"{col.removesuffix('_cagr').removesuffix('_sharpe').removesuffix('_maxdd')}"
                           f"={row[col]:.4f}" for col in columns[3:-1]) + f" n={int(row.switches)}")
    else:
        for name, item in PRESETS.items():
            perf = performance(prices, dates, item["window"], item["entry"], item["exit_threshold"])
            print(_row_text(f"{name} {item['label']}", {**perf, "stats": perf["stats"]}))
        for window in (19, 20, 21):
            perf = performance(prices, dates, window, ENTRY, 0.0)
            print(_row_text(f"邻域 窗口{window}", {**perf, "stats": perf["stats"]}))
