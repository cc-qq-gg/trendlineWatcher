"""每个交易日 14:50 的双动量检测：取当日盘中价、算目标持仓、发一封邮件。

三个取舍（这台机器资源有限）：

1. 独立短命进程，launchd 到点拉起，跑完即退。不常驻、不给 8012 加调度线程，
   空闲时的成本是 0。
2. 默认不写 CSV，`--write` 也受同一道时间闸约束（`data.settled_cutoff`）：15:05 之前
   当日 K 线没定稿，只能补到前一交易日，盘中价进不了库；收盘后重跑 `--write` 才补当日那根。
   盘中价只在内存里 overlay，页面/邮件都不受污染。
3. 只采集 POOL + BOND 三条，不碰 PE / PMI / 10Y —— 那几条又慢又跟信号无关。

东财日线在盘中就会带当日那一根（实测 11:33 已有），所以 14:50 的价可以当 T 日
收盘价的近似，但**不是收盘价**，邮件正文里必须写明，最后 10 分钟仍可能翻转信号。

「取不到数」和「今天没信号」必须能在收件箱里区分开，所以两件事都要发信：

- 正常：结论 + 阈值余量 + 最近十次交易；
- 异常：`采集失败` 主题的短通知（所有源全挂 / 某条序列缺当日价），并写日志。
  节假日不算异常——各家都会把上一交易日的报价原样返回，日期校验不通过就静默跳过。

单源单点故障靠四家兜底（东财日K → 新浪实时 → 东财实时 → 腾讯实时），逐个序列各自降级，
真实来源会印在正文「数据」一行，避免把备用源的数当成东财的数。第三家是东财自己的实时
接口：2026-11-24 实测 push2his 日K 整个域名不可达、同家 push2 实时却正常，多留这条
比再加一家第三方更值。

正文顺序按「手机上先看到要不要动手」排：结论（换仓/维持 + 持仓与已持天数）→ 两条
序列的涨幅与开仓余量 → 最近十次交易（新→旧，末段标浮盈/浮亏）。交易段直接复用
`momentum.episodes`，和页面逐笔同源，不另算一套。

    python -m astock.daily              # 只打印正文，不发信
    python -m astock.daily --send       # 发信（launchd 用这个）
    python -m astock.daily --send --write   # 顺带把已定稿的收盘行落库（15:05 后才会写当日）
"""

from __future__ import annotations

import argparse
import html
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from . import data, momentum

ROOT = Path(__file__).resolve().parent.parent
LOG_PATH = ROOT / "data" / "astock" / "daily_log.txt"

# 邮件里用短名：「纳指ETF场内价」这种口径名留给页面，正文越短越好扫
SHORT = {"sz.399006": "创业板指", "513100_PX": "纳指ETF", "sh.000012": "国债指数"}
PRIMARY_SOURCE = "东财日K"      # _sources() 的第一家；正文据此判断要不要标备用源

# 14:50 抓到的盘中价 vs 收盘价，逐日累积成对照表
COMPARE_PATH = ROOT / "data" / "astock" / "intraday_compare.csv"
COMPARE_COLUMNS = ["trade_date", "code", "snapshot_price", "snapshot_source", "snapshot_at",
                   "close_price", "close_source", "diff_pct"]
COMPARE_TEXT_COLUMNS = {name: str for name in
                        ("trade_date", "code", "snapshot_source", "snapshot_at", "close_source")}
# 只有日K型源对历史日期返回的是那一天的收盘价；实时源会拿今天的最新价冒充，必须排除
CLOSE_SOURCES = (PRIMARY_SOURCE, "新浪")
SNAPSHOT_DEADLINE = pd.Timestamp("15:00").time()   # 之后跑的这次已经是收盘价，不算盘中快照


def _name(code: str) -> str:
    return SHORT.get(code) or data.SERIES.get(code, {}).get("name", code)


def log(line: str, path: str | Path = LOG_PATH, now: pd.Timestamp | None = None) -> None:
    """一行一次检测，谁跑了什么、为什么跳过，都留在盘上。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = (now or pd.Timestamp.now()).strftime("%Y-%m-%d %H:%M:%S")
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{stamp}\t{line}\n")


# ---------------- 取数 ----------------

# 各家独立，任一可用即可；但必须验「这根 K 线是不是今天的」——
# 节假日腾讯/新浪会把上一个交易日的报价原样返回，不校验日期就会把旧价当成今天。
#   东财日K   kline klt=101：盘中就带当日那根（实测 11:33 已有），口径 fqt=1
#   新浪      hq.sinajs.cn：实时现价，[30]/[31] 是 2026-11-24 / 15:13:08，4 位小数
#   东财实时  push2：f43 整数价 + f59 小数位，f86 是最后成交时间戳
#   腾讯      qt.gtimg.cn：实时现价，[30] 是 20261124151245 这样的时间戳，只有 2 位小数
# 注意新浪的日线接口（scale=240）盘中**不含**当日那根，所以只能做补数、不能做盘中判定。
TENCENT_URL = "https://qt.gtimg.cn/q={symbol}"
SINA_URL = "https://hq.sinajs.cn/list={symbol}"
QUOTE_TIMEOUT = 8


def _symbol(code: str) -> str:
    """东财 secid「0.399006」→ 腾讯/新浪要的 sz399006。"""
    secid = data.SERIES.get(code, {}).get("secid") or ""
    market, _, number = secid.partition(".")
    return ("sz" if market == "0" else "sh") + number


def _get_text(url: str, referer: str) -> str:
    """腾讯/新浪都是 GBK，价格与时间戳是 ASCII，按 gbk 解出来再切分不会错位。"""
    resp = requests.get(url, timeout=QUOTE_TIMEOUT,
                        headers={"User-Agent": "Mozilla/5.0", "Referer": referer})
    resp.raise_for_status()
    return resp.content.decode("gbk", errors="ignore")


def _quote_eastmoney(code: str, today: str) -> float | None:
    secid = data.SERIES.get(code, {}).get("secid")
    if not secid:
        return None
    return data.fetch_price(secid).get(today)


def _quote_eastmoney_live(code: str, today: str) -> float | None:
    """东财实时接口（push2）：日K接口单点挂掉时，同家的第二条路。"""
    secid = data.SERIES.get(code, {}).get("secid")
    if not secid:
        return None
    quote = data.fetch_live_quote(secid)
    return quote["price"] if quote and quote["date"] == today else None


def _quote_sina(code: str, today: str) -> float | None:
    fields = _get_text(SINA_URL.format(symbol=_symbol(code)), "https://finance.sina.com.cn/")
    fields = fields.split('="', 1)[-1].split('";', 1)[0].split(",")
    if len(fields) < 31 or fields[30] != today:
        return None
    return float(fields[3])


def _quote_tencent(code: str, today: str) -> float | None:
    fields = _get_text(TENCENT_URL.format(symbol=_symbol(code)), "https://gu.qq.com/")
    fields = fields.split('="', 1)[-1].split('";', 1)[0].split("~")
    if len(fields) < 31 or fields[30][:8] != today.replace("-", ""):
        return None
    return float(fields[3])


def _sources() -> tuple[tuple[str, object], ...]:
    """按优先级返回数据源；每次现取，不要在模块加载时固化成常量（那样打桩就失效了）。

    排序看「精度 + 故障相关性」：
    1. 东财日K——唯一给的是 T 日那根 K 线本身，历史与回填同源，排第一；
    2. 新浪——实时，4 位小数（国债指数 232.5023），与东财异家，覆盖「东财整体不可达」；
    3. 东财实时——只 2 位小数，但和 1 同家不同域名/接口，覆盖「日K挂而实时活」这种
       2026-11-24 实测过的单接口故障；
    4. 腾讯——实时 2 位，与新浪同属「异家兜底」，所以排在同样 2 位的东财实时之后。
    """
    return ((PRIMARY_SOURCE, _quote_eastmoney),
            ("新浪", _quote_sina),
            ("东财实时", _quote_eastmoney_live),
            ("腾讯", _quote_tencent))


def fetch_today(codes: list[str] | None = None, today: str | None = None,
                trace: dict[str, str] | None = None) -> dict[str, float]:
    """当日盘中价，按 `_sources()` 的顺序逐个源兜底；trace 记录每个序列实际用的源。

    - 单个源报错只影响它自己那一格，继续问下一个源；
    - 所有源都「答了但没有今天的 K 线」= 非交易日，返回空字典（不算故障）；
    - 所有源都连不上 = 真故障，抛异常让 run() 发失败通知，不能静默当成非交易日。
    """
    codes = list(codes or momentum.ALL_CODES)
    today = today or pd.Timestamp.now().strftime("%Y-%m-%d")
    out: dict[str, float] = {}
    answered = False
    errors: list[str] = []
    for code in codes:
        for name, source in _sources():
            try:
                price = source(code, today)
            except Exception as exc:                     # noqa: BLE001 —— 采集端任何异常都只降级、不上抛
                errors.append(f"{name}/{code}: {type(exc).__name__}")
                continue
            answered = True
            if price:                                    # 0 视为缺失（停牌/无报价）
                out[code] = float(price)
                if trace is not None:
                    trace[code] = name
                break
    if not out and not answered:
        raise RuntimeError("所有源都连不上：" + "; ".join(errors[:3]))
    return out


def overlay(closes: pd.DataFrame, today: str, prices: dict[str, float]) -> pd.DataFrame:
    """把当日盘中价拼到历史尾部（已有当天行就整行替换），只在内存里，不落盘。"""
    stamp = pd.Timestamp(today)
    row = pd.DataFrame([{code: prices[code] for code in closes.columns}], index=[stamp])
    return pd.concat([closes[closes.index < stamp], row])


# ---------------- 信号 ----------------

def _recent(dates: pd.DatetimeIndex, prices: np.ndarray, holds: np.ndarray, cost_rate: float,
            limit: int = 10) -> list[dict]:
    """最近 limit 段持仓（新→旧）。段口径直接复用 momentum.episodes，保证和页面逐笔一致。

    末段是当前持仓（closed=False）：开仓价到今日盘中价，只收了开仓成本，所以那段收益
    是浮动收益，正文里要标「浮盈/浮亏」而不是已实现。
    """
    held = momentum.held_columns(holds)
    net, _ = momentum.net_returns(prices, held, cost_rate)
    nav = 100.0 * np.cumprod(1.0 + net)
    _, segments, _ = momentum.episodes(dates, prices, holds, nav, cost_rate, 0)
    return [{
        "open": item["open_date"], "close": item["close_date"],
        "code": item["code"], "name": _name(item["code"]),
        "days": item["days"], "ret": item["net_ret"], "closed": item["closed"],
        "open_price": item["open_price"], "close_price": item["close_price"],
        "in_pool": item["in_pool"],
    } for item in reversed(segments[-limit:])]


def detect(closes: pd.DataFrame, window: int = momentum.WINDOW, entry: float = momentum.ENTRY,
           exit_threshold: float = momentum.EXIT,
           cost_rate: float = momentum.COST_PER_SIDE) -> dict:
    """复用回测的同一套规则算末两日的目标持仓：今天该持什么、跟昨天比换没换。"""
    prices = closes[momentum.ALL_CODES].to_numpy(dtype=float)
    dates = closes.index
    if len(dates) <= window + 1:
        raise ValueError(f"共同交易日不足 {window + 2} 天，无法判定")
    rets = momentum.momentum_returns(prices, window)
    holds = momentum.target_index(prices, window, entry, exit_threshold)

    def label(idx: int) -> tuple[str, str]:
        code = momentum.ALL_CODES[idx] if idx >= 0 else momentum.BOND
        return code, _name(code)

    target_code, target = label(int(holds[-1]))
    prev_code, prev = label(int(holds[-2]))
    switches = np.flatnonzero(holds[1:] != holds[:-1]) + 1

    last_switch = None
    if switches.size:
        at = int(switches[-1])
        last_switch = {"date": str(dates[at].date()), "from": label(int(holds[at - 1]))[1],
                       "to": label(int(holds[at]))[1]}

    reads = {}
    for col, code in enumerate(momentum.POOL):
        value = float(rets[-1, col])
        reads[code] = {
            "return": value,
            "gap": float(entry - value),       # 还差多少才够开仓，负数代表已超过
        }
    return {
        "date": str(dates[-1].date()),
        "target": target, "target_code": target_code,
        "prev": prev, "prev_code": prev_code,
        "switched": bool(holds[-1] != holds[-2]),
        "reads": reads,
        "last_switch": last_switch,
        "recent": _recent(dates, prices, holds, cost_rate),
        "window": int(window), "entry": float(entry), "exit_threshold": float(exit_threshold),
    }


def _span(item: dict) -> str:
    """段的时间跨度：同年只留一次年份，「2026-11-19 → 11-21」比两串完整日期好扫。"""
    if not item["closed"]:
        return f"{item['open']} 起"
    close = item["close"][5:] if item["open"][:4] == item["close"][:4] else item["close"]
    return f"{item['open']} → {close}"


def _width(text: str) -> int:
    """显示宽度：中日韩全角字符占 2 格。纯文本邮件靠空格排版，用 len() 会算错。"""
    return sum(2 if unicodedata.east_asian_width(ch) in "WF" else 1 for ch in text)


def _pad(text: str, width: int, right: bool = False) -> str:
    """补空格到 width 格；right=True 时右对齐（收益列要让百分号对齐）。"""
    gap = " " * max(0, width - _width(text))
    return gap + text if right else text + gap


def _holding(signal: dict) -> str:
    """结论下面那行：换仓写路径，维持写起点与已持有交易日，没持仓则留空。"""
    recent = signal.get("recent") or []
    if signal["switched"]:
        return f"{signal['prev']} → {signal['target']}（按今日收盘成交）"
    if recent and not recent[0]["closed"]:
        # 结论行已经写了持仓名，这里只补「什么时候买的、拿了多久」这类新信息
        return f"自 {recent[0]['open']} 起，已 {recent[0]['days']} 个交易日"
    return ""


def _read_rows(reads: dict) -> list[dict]:
    """池内每个资产的涨幅与距开仓线余量：纯文本与 HTML 共用，避免两版说两套数。"""
    rows = []
    for code, read in reads.items():
        gap = read["gap"] * 100
        tail = f"还差 {gap:.2f} 个百分点开仓" if gap > 0 else f"已超开仓线 {-gap:.2f} 个百分点"
        rows.append({"name": _name(code), "ret": f"{read['return'] * 100:+.2f}%", "tail": tail})
    return rows


def _price(value: float) -> str:
    """成交价按量级取小数：指数三千点两位够用，ETF 两块钱必须四位，不然看着像没成交。"""
    if value >= 100:
        return f"{value:.2f}"
    if value >= 10:
        return f"{value:.3f}"
    return f"{value:.4f}"


def _trade_rows(recent: list[dict]) -> list[dict]:
    """最近十次交易：区间 / 标的 / 天数 / 价格 / 收益。

    `short` 是手机端的紧凑天数（18 日），`prices` 是开仓→平仓的成交价
    （口径同 momentum.episodes：信号日收盘价）；持有中那段的后一个价是现价。
    """
    rows = []
    for item in recent:
        ret = item["ret"]
        prices = f"{_price(item['open_price'])} → {_price(item['close_price'])}"
        if item["closed"]:
            days = f"{item['days']} 个交易日"
            value = "—" if ret is None else f"{ret * 100:+.2f}%"
        else:
            days = f"{item['days']}（持有中）"
            flavor = "浮盈" if (ret or 0) >= 0 else "浮亏"
            value = "—" if ret is None else f"{flavor} {ret * 100:+.2f}%"
        rows.append({"span": _span(item), "name": item["name"], "days": days,
                     "value": value, "prices": prices,
                     "closed": item["closed"], "short": f"{item['days']} 日"})
    return rows


def _source_note(sources: dict[str, str] | None) -> str:
    """这一版的数从哪来：同源写一次并列出序列，混源逐条列；切过备用源再补一句原因。"""
    pairs = list((sources or {}).items())
    if not pairs:
        return ""
    names = {name for _, name in pairs}
    if len(names) == 1:
        detail = "、".join(_name(code) for code, _ in pairs)
        text = f"{names.pop()}（{detail}）"
    else:
        text = " · ".join(f"{_name(code)} {name}" for code, name in pairs)
    if any(name != PRIMARY_SOURCE for _, name in pairs):
        text += f"；{PRIMARY_SOURCE} 不可用，已切备用源"
    return text


def _source_short(sources: dict[str, str] | None) -> str:
    """标题下那行用的简称：同源一个词，混源按出现顺序去重（明细留在脚注）。"""
    return " / ".join(dict.fromkeys((sources or {}).values()))


# ---------------- 盘中对照（14:50 vs 收盘） ----------------

def load_compare(path: str | Path | None = None) -> pd.DataFrame:
    """读对照表；文件不存在就当空表，第一次跑不该报错。

    path 默认在调用期取 COMPARE_PATH：写成默认参数会在导入时固化成常量，
    测试把它挪到临时目录就不生效了。
    """
    path = Path(path or COMPARE_PATH)
    if not path.exists():
        return pd.DataFrame(columns=COMPARE_COLUMNS)
    # 文本列必须显式声明：close_source 首次落盘时整列为空，按内容推断会变成 float64，
    # 下次回填写「东财日K」就直接 TypeError
    return pd.read_csv(path, dtype=COMPARE_TEXT_COLUMNS,
                       keep_default_na=False, na_values=[""]).reindex(columns=COMPARE_COLUMNS)


def _save_compare(frame: pd.DataFrame, path: str | Path | None = None) -> None:
    """先写 .tmp 再 replace：中途被打断也不会留下半张表。"""
    path = Path(path or COMPARE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    frame.to_csv(tmp, index=False)
    tmp.replace(path)


def log_snapshot(live: dict[str, float], trace: dict[str, str], today: str,
                 at: pd.Timestamp, path: str | Path | None = None) -> int:
    """把这次抓到的盘中价落盘：同一天重复跑覆盖旧行（自愈口径同 CSV）。"""
    rows = [{"trade_date": today, "code": code, "snapshot_price": float(price),
             "snapshot_source": trace.get(code, ""), "snapshot_at": at.strftime("%H:%M:%S")}
            for code, price in live.items()]
    frame = pd.concat([load_compare(path), pd.DataFrame(rows)], ignore_index=True)
    frame = (frame.drop_duplicates(["trade_date", "code"], keep="last")
             .sort_values(["trade_date", "code"], kind="stable")
             .reindex(columns=COMPARE_COLUMNS).reset_index(drop=True))
    _save_compare(frame, path)
    return len(rows)


def backfill_close(cutoff: str, path: str | Path | None = None, fetch=None) -> int:
    """回填已定稿交易日的收盘价，并算出 14:50 → 收盘的偏差。

    收盘价现采而不是读 CSV：launchd 的 14:50 任务不带 --write，CSV 里常常没有
    前一天的收盘。日K接口对历史日期返回的就是那一天收盘；实时源会拿今天最新价
    冒充，所以只认 CLOSE_SOURCES 里报出来的源，拿不到就留到下次再补。
    """
    fetch = fetch or fetch_today
    frame = load_compare(path)
    if frame.empty:
        return 0
    pending = frame[frame["close_price"].isna() & (frame["trade_date"] <= cutoff)]
    filled = 0
    for day, group in pending.groupby("trade_date", sort=True):
        trace: dict[str, str] = {}
        try:
            prices = fetch(group["code"].tolist(), str(day), trace)
        except Exception:                       # noqa: BLE001, S112 —— 单日回填失败只跳过，不能挡住发信
            continue
        for _, row in group.iterrows():
            price = prices.get(row["code"])
            source = trace.get(row["code"], "")
            if price is None or source not in CLOSE_SOURCES:
                continue
            mask = (frame["trade_date"] == day) & (frame["code"] == row["code"])
            frame.loc[mask, "close_price"] = float(price)
            frame.loc[mask, "close_source"] = source
            frame.loc[mask, "diff_pct"] = round(
                (float(row["snapshot_price"]) / float(price) - 1.0) * 100.0, 3)
            filled += 1
    if filled:
        _save_compare(frame, path)
    return filled


def compare_summary(path: str | Path | None = None, window: int = 20) -> dict | None:
    """邮件里那块「14:50 → 收盘」：最近一个已回填日的逐条偏差 + 近 N 日平均绝对偏差。"""
    frame = load_compare(path)
    done = frame[frame["close_price"].notna()] if not frame.empty else frame
    if done is None or done.empty:
        return None
    last = done["trade_date"].max()
    rows = [{"name": _name(row["code"]), "diff": f"{float(row['diff_pct']):+.2f}%"}
            for _, row in done[done["trade_date"] == last].sort_values("code").iterrows()]
    days = sorted(done["trade_date"].unique())[-window:]
    mean_abs = done[done["trade_date"].isin(days)]["diff_pct"].abs().mean()
    return {"date": last, "rows": rows, "mean_abs": f"{float(mean_abs):.2f}%", "days": len(days)}


def render(signal: dict, cost_bp: float, fetched_at: str,
           sources: dict[str, str] | None = None,
           compare: dict | None = None) -> tuple[str, str]:
    """纯文本正文（HTML 不可用时的兜底）：先给结论，再给阈值余量，最后是最近十次交易。

    - 标题行直接带数据源：这一版的数是新浪还是东财日K，一眼要能看到；
    - 第二行列出池内资产的代码：邮件里只有中文名时，对不上行情软件里的代码；
    - 涨幅按显示宽度补齐，纯文本客户端里两行的余量才对得齐；
    - 交易清单按五列固定宽度排（区间 / 标的 / 交易日 / 价格 / 收益），宽度按显示宽度算，
      只在等宽字体下严格成立——比例字体的客户端仍会歪，手机端请走 render_html；
    - 不换仓时不写「国债指数 → 国债指数」，改成当前位置与已持有几个交易日；
    - 天数一律注明「交易日」，段长来自下标差，不是自然日。
    """
    verdict = "换仓" if signal["switched"] else "维持"
    subject = f"双动量 {signal['date']} {verdict}·{signal['target']}"
    recent = signal.get("recent") or []
    holding = _holding(signal)
    title = f"双动量轮动 · {signal['date']} {fetched_at}（盘中估算，非收盘价）"
    src = _source_short(sources)
    if src:
        title += f" · 数据源 {src}"
    lines = [
        title,
        "池  " + " · ".join(f"{_name(code)} {code}" for code in momentum.ALL_CODES),
        "",
        f"结论  {verdict} {signal['target']}（{signal['target_code']}）",
    ]
    if holding:
        lines.append(f"      {holding}")
    lines += [
        "",
        (f"{signal['window']} 日涨幅（开仓线 {signal['entry'] * 100:.2f}%，"
         f"平仓线 {signal['exit_threshold'] * 100:.2f}%）"),
    ]
    read_rows = _read_rows(signal["reads"])
    name_w = max(_width(row["name"]) for row in read_rows)
    ret_w = max(_width(row["ret"]) for row in read_rows)
    for row in read_rows:
        lines.append(f"  {_pad(row['name'], name_w)}  "
                     f"{_pad(row['ret'], ret_w, right=True)}  {row['tail']}")

    lines += ["", "最近十次交易（新→旧，费后）"]
    if not recent:
        lines.append("  区间内没有成交")
    rows = _trade_rows(recent)
    columns = ("span", "name", "days", "prices", "value")
    widths = [max(_width(row[col]) for row in rows) for col in columns] if rows else []
    for row in rows:
        cells = [_pad(row[col], widths[i]) for i, col in enumerate(columns)]
        lines.append("  " + "  ".join(cells).rstrip())

    if compare:
        lines += ["", f"盘中 14:50 → 收盘（{compare['date']}）"]
        diff_w = max(_width(row["name"]) for row in compare["rows"])
        for row in compare["rows"]:
            lines.append(f"  {_pad(row['name'], diff_w)}  {row['diff']}")
        lines.append(f"  近 {compare['days']} 个交易日平均绝对偏差 {compare['mean_abs']}")

    lines += [
        "",
        (f"参数  窗口 {signal['window']} 日 / 开仓 {signal['entry'] * 100:.2f}% / "
         f"平仓 {signal['exit_threshold'] * 100:.2f}% / 单边 {cost_bp:g}bp"),
    ]
    note = _source_note(sources)
    if note:
        lines.append(f"数据  {note}")
    lines.append("说明  14:50 的价不是收盘价，最后 10 分钟仍可能翻转信号；以收盘复核为准。")
    return subject, "\n".join(lines)


HTML_FONT = ("-apple-system,BlinkMacSystemFont,'PingFang SC','Hiragino Sans GB',"
             "'Microsoft YaHei',sans-serif")
GREY = "#8b949e"
INK2 = "#57606a"
RULE = "#eaeef2"


def render_html(signal: dict, cost_bp: float, fetched_at: str,
                sources: dict[str, str] | None = None,
                compare: dict | None = None) -> str:
    """HTML 正文（手机优先）：纯文本那套空格对齐在比例字体里会塌、长行折行还会拆词，
    所以列对齐交给表格，交易清单拆成「标的一行 + 区间一行」的两行卡片，
    行内一律 nowrap，不依赖字体也不依赖视口宽度。
    """
    verdict = "换仓" if signal["switched"] else "维持"
    holding = _holding(signal)
    rows = _trade_rows(signal.get("recent") or [])
    esc = html.escape

    pool = "".join(
        f'<span style="display:inline-block;white-space:nowrap;margin:0 12px 2px 0">'
        f'{esc(_name(code))} <span style="color:{GREY}">{esc(code)}</span></span>'
        for code in momentum.ALL_CODES)

    reads = "".join(
        f'<tr><td style="padding:7px 0 0">{esc(row["name"])}</td>'
        f'<td align="right" style="padding:7px 0 0;font-weight:600;white-space:nowrap">'
        f'{esc(row["ret"])}</td></tr>'
        f'<tr><td colspan="2" style="padding:1px 0 7px;color:{GREY};font-size:12px;'
        f'border-bottom:1px solid {RULE}">{esc(row["tail"])}</td></tr>'
        for row in _read_rows(signal["reads"]))

    trades = "".join(
        f'<table width="100%" style="border-collapse:collapse;border-bottom:1px solid {RULE}">'
        f'<tr><td style="padding:8px 0 0;font-weight:600;white-space:nowrap">'
        f'{esc(row["name"])}'
        + ('' if row["closed"] else f'<span style="font-weight:400;font-size:12px;'
                                    f'color:{GREY}"> 持有中</span>')
        + f'</td>'
        f'<td align="right" style="padding:8px 0 0;font-weight:600;white-space:nowrap">'
        f'{esc(row["value"])}</td></tr>'
        f'<tr><td colspan="2" style="padding:1px 0 8px;color:{GREY};font-size:12px">'
        f'{esc(row["prices"])} · {esc(row["span"])} · {esc(row["short"])}</td></tr></table>'
        for row in rows)

    notes = [(f"窗口 {signal['window']} 日 · 开仓 {signal['entry'] * 100:.2f}% · "
              f"平仓 {signal['exit_threshold'] * 100:.2f}% · 单边 {cost_bp:g}bp")]
    note = _source_note(sources)
    if note:
        notes.append(f"数据源 {note}")
    notes.append("14:50 的价不是收盘价，最后 10 分钟仍可能翻转信号；以收盘复核为准。")
    note_html = "<br>".join(esc(text) for text in notes)
    empty = f'<div style="color:{GREY};font-size:12px">区间内没有成交</div>'

    src = _source_short(sources)
    subtitle = "盘中估算，非收盘价" + (f" · 数据源 {src}" if src else "")
    drift = ""
    if compare:
        drift_rows = "".join(
            f'<tr><td style="padding:5px 0">{esc(row["name"])}</td>'
            f'<td align="right" style="padding:5px 0;white-space:nowrap">{esc(row["diff"])}</td>'
            f'</tr>' for row in compare["rows"])
        drift = (
            f'<div style="margin-top:22px;font-weight:600">盘中 14:50 → 收盘</div>'
            f'<div style="color:{GREY};font-size:12px">{esc(compare["date"])} · '
            f'近 {compare["days"]} 个交易日平均绝对偏差 {esc(compare["mean_abs"])}</div>'
            f'<table width="100%" style="border-collapse:collapse;font-size:14px;margin-top:2px">'
            f'{drift_rows}</table>')

    return (
        f'<div style="font:14px/1.65 {HTML_FONT};color:#1f2328;max-width:480px">'
        f'<div style="font-size:17px;font-weight:600">双动量轮动 · {esc(signal["date"])} '
        f'{esc(fetched_at)}</div>'
        f'<div style="color:{GREY};font-size:12px;margin-top:2px">{esc(subtitle)}</div>'
        f'<div style="color:{INK2};font-size:12px;line-height:1.9;margin-top:6px">{pool}</div>'

        f'<div style="margin-top:18px;font-size:20px;font-weight:600;line-height:1.35">'
        f'{esc(verdict)} {esc(signal["target"])}'
        f'<span style="font-size:13px;font-weight:400;color:{GREY}"> '
        f'{esc(signal["target_code"])}</span></div>'
        + (f'<div style="color:{INK2};font-size:13px;margin-top:2px">{esc(holding)}</div>'
           if holding else '')
        + f'<div style="margin-top:20px;font-weight:600">{signal["window"]} 日涨幅</div>'
        f'<div style="color:{GREY};font-size:12px">开仓线 {signal["entry"] * 100:.2f}% · '
        f'平仓线 {signal["exit_threshold"] * 100:.2f}%</div>'
        f'<table width="100%" style="border-collapse:collapse;font-size:14px;margin-top:2px">'
        f'{reads}</table>'

        f'<div style="margin-top:22px;font-weight:600">最近十次交易</div>'
        f'<div style="color:{GREY};font-size:12px">新→旧，费后；价格为开仓→平仓收盘价，'
        f'天数为交易日</div>'
        f'<div style="margin-top:4px">{trades or empty}</div>'
        f'{drift}'

        f'<div style="margin-top:16px;color:{GREY};font-size:12px;line-height:1.7">'
        f'{note_html}</div>'
        f'</div>'
    )


def render_failure(today: str, reason: str, detail: str, fetched_at: str) -> tuple[str, str]:
    """取不到数时的通知：宁可多发一封，也不能让「没收到信」被当成「今天没信号」。"""
    subject = f"双动量 {today} 采集失败"
    lines = [
        f"双动量轮动 · {today} {fetched_at}",
        "",
        "结论  今日未判定",
        f"原因  {reason}",
        f"详情  {detail}",
        "",
        "处理  稍后手动补跑（收盘价更准）：",
        f"      python -m astock.daily --send --date {today}",
        "",
        "说明  各家（东财日K/新浪/东财实时/腾讯）都没拿到当日价，不做任何推断，也不沿用昨收。",
    ]
    return subject, "\n".join(lines)


# ---------------- 发送 ----------------

def default_mailer(subject: str, body: str, html_body: str | None = None) -> None:
    """仓库根的 send_email 脚本按需导入：离线/测试时不碰 SMTP 配置。"""
    root = str(ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)
    from send_email import send_email as send  # 刻意延迟，避免离线时拉 SMTP 配置

    send(subject, body, html_body)


REASON_TEXT = {"fetch-error": "所有源都连不上", "incomplete": "部分序列取不到当日价"}


def _notify(reason: str, detail: str, today: str, now: pd.Timestamp, *, send: bool,
            mailer, log_path: str | Path) -> dict:
    """把「今天没判定出来」这件事发出去：没有这封信，沉默就会被误读成没有信号。"""
    subject, body = render_failure(today, REASON_TEXT.get(reason, reason), detail,
                                   now.strftime("%H:%M"))
    if send:
        (mailer or default_mailer)(subject, body)
    log(f"{'notice' if send else 'dry-run-notice'} {subject}", log_path, now)
    return {"sent": bool(send), "reason": reason, "date": today, "subject": subject, "body": body}


def run(*, send: bool = False, write: bool = False, today: str | None = None,
        mailer=None, path: str | Path | None = None,
        log_path: str | Path = LOG_PATH, compare_path: str | Path | None = None,
        now: pd.Timestamp | None = None, fetch=None) -> dict:
    """跑一次检测；返回 {sent, reason, subject, body, signal}，不抛业务异常。"""
    now = now or pd.Timestamp.now()
    compare_path = compare_path or COMPARE_PATH   # 运行期取，测试才好把它挪到临时目录
    today = today or now.strftime("%Y-%m-%d")
    if pd.Timestamp(today).weekday() >= 5:
        log(f"skip weekend {today}", log_path, now)
        return {"sent": False, "reason": "weekend", "date": today}

    trace: dict[str, str] = {}
    fetch = fetch or fetch_today
    try:
        live = fetch(momentum.ALL_CODES, today, trace)
    except Exception as exc:                       # noqa: BLE001 —— 采集端全挂：发通知，绝不静默跳过
        log(f"fetch-error {today} {type(exc).__name__}", log_path, now)
        return _notify("fetch-error", f"{type(exc).__name__}: {exc}", today, now,
                       send=send, mailer=mailer, log_path=log_path)
    if not live:
        log(f"skip non-trading-day {today}", log_path, now)
        return {"sent": False, "reason": "non-trading-day", "date": today}
    missing = [code for code in momentum.ALL_CODES if code not in live]
    if missing:
        detail = "、".join(_name(code) for code in missing)
        log(f"incomplete {today} missing={','.join(missing)}", log_path, now)
        return _notify("incomplete", f"缺 {detail}", today, now,
                       send=send, mailer=mailer, log_path=log_path)

    cutoff = data.settled_cutoff(now)
    if write:
        result = data.sync(path or data.DEFAULT_PATH, codes=momentum.ALL_CODES, cutoff=cutoff)
        log(f"sync cutoff={cutoff} added={result['added']} updated={result['updated']} "
            f"rows={result['rows']}", log_path, now)

    # 对照表：先补上已定稿日的收盘价，再决定要不要把这一次的盘中价记下来。
    # 两步都 fail-soft —— 对照表坏了也不能挡发信。
    filled = snapshot = 0
    try:
        filled = backfill_close(cutoff, compare_path, fetch)
        if filled:
            log(f"backfill {filled} close rows cutoff={cutoff}", log_path, now)
    except Exception as exc:                       # noqa: BLE001
        log(f"backfill-error {type(exc).__name__}: {exc}", log_path, now)
    if today == now.strftime("%Y-%m-%d") and now.time() < SNAPSHOT_DEADLINE:
        # 只有盘中的那次跑才叫「14:50 快照」；收盘后补跑记下来会把收盘价伪装成盘中价
        try:
            snapshot = log_snapshot(live, trace, today, now, compare_path)
        except Exception as exc:                   # noqa: BLE001
            log(f"snapshot-error {type(exc).__name__}: {exc}", log_path, now)

    closes = data.load_closes(momentum.ALL_CODES, path or data.DEFAULT_PATH)
    signal = detect(overlay(closes, today, live))
    compare = compare_summary(compare_path)
    cost_bp = momentum.COST_PER_SIDE * 10000
    subject, body = render(signal, cost_bp, now.strftime("%H:%M"), trace, compare)
    # 手机邮箱都是比例字体，纯文本的空格对齐会塌，正文同时挂一份 HTML
    html_body = render_html(signal, cost_bp, now.strftime("%H:%M"), trace, compare)

    if send:
        (mailer or default_mailer)(subject, body, html_body)
        used = f" src={','.join(sorted(set(trace.values())))}" if trace else ""
        log(f"sent {subject}{used}", log_path, now)
    else:
        log(f"dry-run {subject}", log_path, now)
    return {"sent": bool(send), "reason": "sent" if send else "dry-run",
            "date": today, "subject": subject, "body": body, "html": html_body,
            "signal": signal, "snapshot": snapshot, "backfilled": filled}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="双动量轮动每日 14:50 检测 + 邮件")
    parser.add_argument("--send", action="store_true", help="真的发邮件（默认只打印）")
    parser.add_argument("--write", action="store_true", help="把当日盘中行写进 CSV（默认不写）")
    parser.add_argument("--date", help="覆盖日期（YYYY-MM-DD），补跑用")
    args = parser.parse_args(argv)

    result = run(send=args.send, write=args.write, today=args.date)
    if result["reason"] in {"weekend", "non-trading-day"}:
        print(f"{result['date']} 非交易日，跳过")
        return 0
    print(result["subject"])
    print()
    print(result["body"])
    return 1 if result["reason"] in REASON_TEXT else 0


if __name__ == "__main__":
    raise SystemExit(main())
