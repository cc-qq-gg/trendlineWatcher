"""A 股指数与宏观序列：单 CSV 长表存取 + 增量采集（不依赖任何数据库）。

序列清单沿用 bigA/index_series.py 的 SERIES_REGISTRY 口径，但采集端收敛成
两个源：东财行情（日线，一次请求回溯到 1990 年）与东财/蛋卷的三张表
（PE、10Y 国债、PMI）。bigA 那套 baostock + csindex + 腾讯分年分片的采集
是为了绕开各家接口的历史长度限制，本地只要一条 CSV，全部砍掉。

列名沿用仓库里 index_daily 的习惯（trade_date / code / close），长表存
data/astock/index_daily.csv；宽表在内存里 pivot，不落盘第二份。

采集只由 CLI 或手动刷新触发，请求路径永远只读 CSV（见 service.py）。

启动（工作目录必须是仓库根；项目是 package=false，包靠 cwd 进 sys.path）：

    uv run python -m astock.data            # 只看现状
    uv run python -m astock.data --sync     # 增量补到最新交易日
"""

from __future__ import annotations

import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

CSV_COLUMNS = ["trade_date", "code", "close"]
DEFAULT_PATH = Path("data/astock/index_daily.csv")

EM_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
EM_LIVE_URL = "https://push2.eastmoney.com/api/qt/stock/get"     # 实时现价，与日K不同域名
EM_TABLE_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
DANJUAN_PE_URL = "https://danjuanfunds.com/djapi/index_eva/pe_history/SH000300?day=all"
YIELD_TEN_YEAR_COLUMN = "EMM00166466"          # 东财国债收益率报表里「中国10年」所在列
HTTP_TIMEOUT = 20
HTTP_RETRIES = 3
LIVE_TIMEOUT = 6                # 盘中兜底：一次不成就把机会让给下一家，不能拖住 14:50
SETTLE_TIME = pd.Timestamp("15:05").time()      # 收盘 15:00 + 5 分钟等 K 线定稿

# secid 形式的序列走东财行情；其余按 source 分发。internal 的不进指数关系图默认列表。
SERIES: dict[str, dict] = {
    "sh.000001": {"name": "上证指数", "secid": "1.000001", "base": True, "group": "宽基"},
    "sz.399006": {"name": "创业板指", "secid": "0.399006", "group": "宽基"},
    "sh.000300": {"name": "沪深300", "secid": "1.000300", "group": "宽基"},
    "sh.000012": {
        "name": "国债指数", "secid": "1.000012", "internal": True, "group": "债券",
        "note": "上证国债价格指数（不含票息），仅用于轮动空仓替代",
    },
    "513100_PX": {
        "name": "纳指ETF场内价", "secid": "1.513100", "internal": True, "group": "海外",
        "note": "513100 前复权场内收盘价，与累计净值口径不同",
    },
    "HS300_PE": {
        "name": "沪深300市盈率", "source": "danjuan", "group": "估值",
        "display": "percentile", "axis": "right", "unit": "倍",
        "note": "蛋卷周频滚动市盈率，非中证官方口径",
    },
    "CN10Y": {
        "name": "10年期国债收益率", "source": "eastmoney_yield", "internal": True,
        "group": "利率", "unit": "%", "note": "仅作为 ERP 输入",
    },
    "PMI_CN": {
        "name": "制造业PMI", "source": "eastmoney_pmi", "group": "宏观",
        "display": "percentile", "axis": "right", "unit": "点",
        "note": "月度，50 为荣枯线",
    },
    "HS300_ERP": {
        "name": "股债性价比（ERP）", "source": "derived", "group": "估值",
        "display": "percentile", "axis": "right", "unit": "%",
        "note": "100/沪深300PE − 10Y国债，越高代表股票相对债券越便宜；现算不落库",
    },
}
DERIVED = ("HS300_ERP",)                      # 由其他序列现算，不采集也不写 CSV
PRICE_CODES = [code for code, meta in SERIES.items() if "secid" in meta]
# 落库序列：有采集来源且不是现算的派生序列（DERIVED 只在这里排除一次，避免两处各记一份）
STORED_CODES = [code for code, meta in SERIES.items()
                if ("secid" in meta or "source" in meta) and code not in DERIVED]

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (quantauto astock collector)",
    "Referer": "https://data.eastmoney.com/",
}

_SHARED: requests.Session | None = None


def _session() -> requests.Session:
    """复用连接；采集是低频动作，一个进程内一个 session 就够。"""
    global _SHARED
    if _SHARED is None:
        _SHARED = requests.Session()
        _SHARED.headers.update(_HEADERS)
    return _SHARED


def _http_json(url: str, headers: dict | None = None,
               retries: int | None = None, timeout: int | None = None) -> dict:
    """带重试的 GET + JSON 解析；东财偶发直接断连（RemoteDisconnected），重试一次基本就过。

    历史采集可以慢慢重试；盘中兜底要抢在收盘前出结论，所以那边传更小的 retries/timeout。
    """
    last: Exception | None = None
    for attempt in range(retries or HTTP_RETRIES):
        try:
            resp = _session().get(url, headers=headers, timeout=timeout or HTTP_TIMEOUT)
            resp.raise_for_status()
            loaded = resp.json()
            if not isinstance(loaded, dict):
                raise TypeError("返回不是 JSON 对象")
            return loaded
        except (requests.RequestException, ValueError, TypeError) as exc:
            last = exc
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"采集失败 {url[:80]}: {last}")


# ---------------- 读取 ----------------

def load_long(path: str | Path | None = None) -> pd.DataFrame:
    """读长表；文件不存在返回空表（列齐全，调用方不用判形状）。path=None 即默认库。"""
    path = Path(path or DEFAULT_PATH)
    if not path.exists():
        return pd.DataFrame(columns=CSV_COLUMNS)
    frame = pd.read_csv(path, usecols=CSV_COLUMNS)
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    return frame.dropna(subset=CSV_COLUMNS).reset_index(drop=True)


def load_closes(codes: list[str] | None = None, path: str | Path = DEFAULT_PATH) -> pd.DataFrame:
    """透视成收盘价宽表，只留共同交易日（轮动要求同一天两边都有价，缺一天就会错位）。

    默认只取行情序列：PE 是周频、PMI 是月频，混进来会让 dropna 把宽表清空。
    宏观序列走 load_series 单独取。

    返回按日期升序、列为 codes 的 DataFrame，索引是 DatetimeIndex。
    """
    codes = list(codes or PRICE_CODES)
    frame = load_long(path)
    if frame.empty:
        return pd.DataFrame(columns=codes)
    frame = frame.drop_duplicates(["code", "trade_date"], keep="last")
    wide = frame.pivot(index="trade_date", columns="code", values="close")
    for code in codes:
        if code not in wide.columns:
            wide[code] = float("nan")
    wide = wide[codes].dropna(how="any")
    wide.index = pd.to_datetime(wide.index)
    return wide.sort_index()


def load_series(code: str, path: str | Path = DEFAULT_PATH) -> tuple[pd.Series, pd.Series]:
    """取单条序列，返回 (日期 Series, 数值 Series)，按日期升序。"""
    if code in DERIVED:
        raise ValueError(f"{code} 是派生序列，请走 relation.derive_erp")
    frame = load_long(path)
    if frame.empty:
        empty = pd.Series(dtype="float64")
        return empty, empty
    part = frame[frame["code"] == code].drop_duplicates("trade_date", keep="last")
    part = part.sort_values("trade_date")
    dates = pd.to_datetime(part["trade_date"].astype(str)).reset_index(drop=True)
    values = part["close"].astype(float).reset_index(drop=True)
    return dates, values


# ---------------- 采集 ----------------

def fetch_price(secid: str) -> dict[str, float]:
    """东财日线收盘价（前复权），一次拉全量。返回 {YYYY-MM-DD: close}。"""
    url = (
        f"{EM_KLINE_URL}?secid={secid}&klt=101&fqt=1&end=20500101&lmt=9000"
        "&fields1=f1,f2,f3&fields2=f51,f52,f53"
    )
    payload = _http_json(url)
    rows = (payload.get("data") or {}).get("klines") or []
    out: dict[str, float] = {}
    for line in rows:
        parts = line.split(",")
        if len(parts) < 3:
            continue
        try:
            close = float(parts[2])
        except ValueError:
            continue
        if close > 0:
            out[parts[0]] = close
    return out


def fetch_live_quote(secid: str) -> dict | None:
    """东财实时现价（盘中估算用），返回 {price, date, name}；停牌/无报价时 None。

    f43 是放大过的整数，按 f59 给的小数位还原（创业板指 2 位、纳指ETF 3 位、国债指数 2 位）；
    f86 是最后成交时间戳，调用方拿 date 比对「这根是不是今天的」。

    与 fetch_price（push2his 日K）是两个不同接口：实测日K域名被拒时实时仍可用，
    所以盘中兜底走这条，历史与回填仍然只认日K。
    """
    payload = _http_json(f"{EM_LIVE_URL}?secid={secid}&fields=f43,f58,f59,f86",
                         retries=1, timeout=LIVE_TIMEOUT)
    item = payload.get("data") or {}
    raw, digits, stamp = item.get("f43"), item.get("f59"), item.get("f86")
    if not isinstance(raw, int) or raw <= 0 or not isinstance(digits, int) or not isinstance(stamp, int):
        return None                                    # 停牌时 f43 是 "-"，不是数字
    day = datetime.fromtimestamp(stamp, tz=timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
    return {"price": raw / 10 ** digits, "date": day, "name": item.get("f58") or ""}


def fetch_pe() -> dict[str, float]:
    """蛋卷沪深300 周频滚动 PE；ts 是北京时间零点的毫秒，按 +08 取日避免挪到前一天。"""
    payload = _http_json(DANJUAN_PE_URL, {"Referer": "https://danjuanfunds.com/"})
    items = (payload.get("data") or {}).get("index_eva_pe_growths") or []
    out: dict[str, float] = {}
    for item in items:
        ts, pe = item.get("ts"), item.get("pe")
        if not ts or not pe:
            continue
        day = datetime.fromtimestamp(int(ts) / 1000, tz=timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
        out[day] = float(pe)
    return out


def fetch_cn10y(start_date: str) -> dict[str, float]:
    """东财 10Y 国债收益率日线；降序分页 500 条，翻到早于 start_date 就停。"""
    out: dict[str, float] = {}
    for page in range(1, 25):
        payload = _http_json(
            f"{EM_TABLE_URL}?reportName=RPTA_WEB_TREASURYYIELD"
            f"&columns=SOLAR_DATE,{YIELD_TEN_YEAR_COLUMN}&pageSize=500&pageNumber={page}"
            "&sortColumns=SOLAR_DATE&sortTypes=-1"
        )
        rows = ((payload.get("result") or {}).get("data")) or []
        for row in rows:
            value = row.get(YIELD_TEN_YEAR_COLUMN)
            day = str(row.get("SOLAR_DATE") or "")[:10]
            if day and value and value > 0:
                out[day] = float(value)
        if len(rows) < 500 or (rows and str(rows[-1].get("SOLAR_DATE"))[:10] < start_date):
            break
    return out


def fetch_pmi() -> dict[str, float]:
    """东财制造业 PMI 月度，接口一次给全历史（200 多条）。"""
    payload = _http_json(
        f"{EM_TABLE_URL}?reportName=RPT_ECONOMY_PMI&columns=REPORT_DATE,MAKE_INDEX"
        "&pageNumber=1&pageSize=1000&sortColumns=REPORT_DATE&sortTypes=-1"
    )
    out: dict[str, float] = {}
    for row in ((payload.get("result") or {}).get("data")) or []:
        value = row.get("MAKE_INDEX")
        day = str(row.get("REPORT_DATE") or "")[:10]
        if day and value and value > 0:
            out[day] = float(value)
    return out


def collect(codes: list[str] | None = None, start_date: str = "2000-01-01") -> tuple[dict, dict]:
    """按序列采集 {code: {date: close}} 与 {code: 错误}；单条失败不拖垮整批。"""
    codes = [code for code in (codes or STORED_CODES) if code not in DERIVED]
    fetched: dict[str, dict[str, float]] = {}
    errors: dict[str, str] = {}
    for code in codes:
        meta = SERIES[code]
        try:
            if "secid" in meta:
                fetched[code] = fetch_price(meta["secid"])
            elif meta["source"] == "danjuan":
                fetched[code] = fetch_pe()
            elif meta["source"] == "eastmoney_yield":
                fetched[code] = fetch_cn10y(start_date)
            elif meta["source"] == "eastmoney_pmi":
                fetched[code] = fetch_pmi()
            else:
                raise ValueError(f"未知采集源 {meta.get('source')}")
        except (RuntimeError, ValueError) as exc:
            errors[code] = str(exc)
    return fetched, errors


# ---------------- 写入 ----------------

def save_atomic(frame: pd.DataFrame, path: str | Path = DEFAULT_PATH) -> None:
    """原子替换 CSV：同目录临时文件 + os.replace，避免读到写了一半的文件。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp_", suffix=".csv")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            frame.to_csv(handle, index=False)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def settled_cutoff(now: pd.Timestamp | None = None) -> str:
    """当前允许落库的最新交易日，给 sync 的 cutoff 用（唯一的时间闸入口）。

    15:05 之前当日那根 K 线还没定稿（东财盘中就给，值会随尾盘变），只放到前一自然日；
    15:05 之后当日算定稿。只跟本地时钟走、不看传进来的业务日期——补跑历史日永远早于
    「昨天」，不会被误挡；被挡住的就是「今天这一根」，正是盘中快照写脏数据的源头。
    """
    now = now or pd.Timestamp.now()
    day = now.normalize()
    if now.time() < SETTLE_TIME:
        day -= pd.Timedelta(days=1)
    return day.strftime("%Y-%m-%d")


def sync(path: str | Path = DEFAULT_PATH, codes: list[str] | None = None,
         cutoff: str | None = None) -> dict:
    """增量补数并回写。东财给的本来就是全量，同日期覆盖即可幂等，只统计新增与改写的行数。

    cutoff 是允许落库的最新交易日（见 settled_cutoff）；None 表示不设闸（手工回填用）。
    同日期重采一定覆盖旧值——盘中快照写进来的半截收盘靠这一条自愈，而不是等人工清理。
    """
    started = time.perf_counter()
    path = Path(path)
    old = load_long(path)
    peak = old.groupby("code")["trade_date"].max().to_dict() if not old.empty else {}

    fetched, errors = collect(codes, start_date=min(peak.values()) if peak else "2000-01-01")
    known = (old.set_index(["code", "trade_date"])["close"].round(6).to_dict()
             if not old.empty else {})
    frames = [old] if not old.empty else []
    added = updated = 0
    for code, points in fetched.items():
        rows = []
        for day, value in sorted(points.items()):
            if cutoff and day > cutoff:
                continue                        # 未收盘的当日 K 线一律不落库
            fresh = round(float(value), 6)
            prev = known.get((code, day))
            if prev is not None and abs(prev - fresh) <= 1e-9:
                continue                        # 值没变：旧行留着，不必重写
            if prev is None:
                added += 1
            else:
                updated += 1                    # 同一交易日换了值：覆盖旧行（自愈）
            rows.append((day, code, value))
        frames.append(pd.DataFrame(rows, columns=CSV_COLUMNS))
    merged = (pd.concat(frames, ignore_index=True)
              .drop_duplicates(["code", "trade_date"], keep="last")   # 新采的在后，压过旧行
              .sort_values(["trade_date", "code"], kind="stable")
              .reset_index(drop=True))
    merged["close"] = merged["close"].astype(float).round(6)
    save_atomic(merged, path)
    return {
        "rows": len(merged),
        "added": int(added),
        "updated": int(updated),
        "codes": int(merged["code"].nunique()),
        "end": str(merged["trade_date"].max()) if len(merged) else None,
        "cutoff": cutoff,
        "errors": errors,
        "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 1),
    }


def coverage(path: str | Path = DEFAULT_PATH, required: tuple[str, ...] = ()) -> list[dict]:
    """每条序列的行数与区间，用来看数据齐不齐、要不要补采。"""
    frame = load_long(path)
    out = []
    for code, meta in SERIES.items():
        part = frame[frame["code"] == code] if not frame.empty else frame
        out.append({
            "code": code,
            "name": meta["name"],
            "rows": len(part),
            "start": None if part.empty else str(part["trade_date"].min()),
            "end": None if part.empty else str(part["trade_date"].max()),
            "internal": bool(meta.get("internal")),
            "derived": meta.get("source") == "derived",
            "missing": bool(part.empty) and code in required,
        })
    return out


if __name__ == "__main__":
    import argparse

    from . import (
        momentum,  # 必需清单归 momentum 所有，这里只在 CLI 里取，避免模块级反向依赖
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--sync", action="store_true", help="联网增量采集后回写 CSV")
    args = parser.parse_args()
    if args.sync:
        print(sync(cutoff=settled_cutoff()))
    for row in coverage(required=tuple(momentum.ALL_CODES)):
        if row["derived"]:                       # 派生序列不落库，行数恒为 0，不必打印
            continue
        print(f"{row['code']:>10} {row['name']:<12} {row['rows']:>6} 行 "
              f"{row['start']} → {row['end']}{'  <缺>' if row['missing'] else ''}")
