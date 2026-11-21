"""把 divergence CSV 转成前端 lightweight-charts 需要的紧凑 JSON。

数据来源固定为 divergence_visualizer/output/detector_{COIN}_{TF}_signals.csv，
不重算指标、不改判定口径，只做格式转换。

时间约定：源 CSV 是北京时间（UTC+8）naive 时间戳（get_data.py 在落盘前已 tz_convert），
这里按“墙上时间”编码成 UTC 秒级时间戳，前端显示出来的日期/时刻与 CSV 完全一致。
"""

import calendar
import os
from datetime import datetime, timedelta, timezone

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "divergence_visualizer", "output")

# 重采样出来的每根K线跨度，用于判断末根是否还没走完
TIMEFRAME_HOURS = {"4h": 4, "1d": 24}

PRICE_COLS = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
}
IND_COLS = {
    "boll_upper": "boll_up",
    "boll_middle": "boll_mid",
    "boll_lower": "boll_low",
    "macd": "macd",
    "macd_signal": "macd_signal",
    "macd_hist": "macd_hist",
}


def signals_path(symbol: str, timeframe: str, mobile: bool = False) -> str:
    suffix = "_mobile" if mobile else ""
    return os.path.join(
        OUTPUT_DIR, f"detector_{symbol.upper()}_{timeframe.upper()}_signals{suffix}.csv"
    )


def load_signals(symbol: str, timeframe: str, mobile: bool = False) -> pd.DataFrame:
    path = signals_path(symbol, timeframe, mobile)
    if not os.path.exists(path):
        return pd.DataFrame()
    frame = pd.read_csv(path)
    if "candle_begin_time" not in frame.columns or frame.empty:
        return pd.DataFrame()
    frame["candle_begin_time"] = pd.to_datetime(frame["candle_begin_time"])
    return frame.sort_values("candle_begin_time").reset_index(drop=True)


def _unix(value) -> int:
    stamp = pd.Timestamp(value)
    return calendar.timegm((stamp.year, stamp.month, stamp.day, stamp.hour, stamp.minute, 0))


def _num(value, digits: int = 4):
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


def _column(frame: pd.DataFrame, name: str, digits: int = 4) -> list:
    if name not in frame.columns:
        return [None] * len(frame)
    return [_num(v, digits) for v in frame[name]]


def _flag(frame: pd.DataFrame, name: str, target) -> pd.Series:
    if name not in frame.columns:
        return pd.Series(False, index=frame.index)
    values = pd.to_numeric(frame[name], errors="coerce")
    return values == target


def build_markers(frame: pd.DataFrame) -> list:
    """按现有口径生成标记：MACD 顶底背离、StochRSI tbl/dbl、BOLL-W、BOLL 突破。"""
    markers = []

    def add(index, kind, price, text):
        markers.append(
            {
                "t": _unix(frame.at[index, "candle_begin_time"]),
                "kind": kind,
                "price": _num(price),
                "text": text,
            }
        )

    for idx in frame.index[_flag(frame, "macd_top_divergence", 1)]:
        add(idx, "macd_top", frame.at[idx, "High"], "")
    for idx in frame.index[_flag(frame, "macd_bottom_divergence", 1)]:
        add(idx, "macd_bottom", frame.at[idx, "Low"], "")
    # tbl = 顶背离，dbl = 底背离（detect_* 返回的 signal 值）
    for idx in frame.index[_flag(frame, "tbl", -1)]:
        add(idx, "tbl", frame.at[idx, "High"], "T")
    for idx in frame.index[_flag(frame, "dbl", 1)]:
        add(idx, "dbl", frame.at[idx, "Low"], "D")
    for idx in frame.index[_flag(frame, "boll_width_below_signal", 1)]:
        add(idx, "boll_w", frame.at[idx, "Close"], "W")
    for idx in frame.index[_flag(frame, "boll_breakout_signal", 1)]:
        add(idx, "boll_break_up", frame.at[idx, "High"], "")
    for idx in frame.index[_flag(frame, "boll_breakout_signal", -1)]:
        add(idx, "boll_break_down", frame.at[idx, "Low"], "")

    markers.sort(key=lambda m: m["t"])
    return markers


def build_counts(frame: pd.DataFrame) -> dict:
    return {
        "macd_top": int(_flag(frame, "macd_top_divergence", 1).sum()),
        "macd_bottom": int(_flag(frame, "macd_bottom_divergence", 1).sum()),
        "tbl": int(_flag(frame, "tbl", -1).sum()),
        "dbl": int(_flag(frame, "dbl", 1).sum()),
        "boll_w": int(_flag(frame, "boll_width_below_signal", 1).sum()),
        "boll_break_up": int(_flag(frame, "boll_breakout_signal", 1).sum()),
        "boll_break_down": int(_flag(frame, "boll_breakout_signal", -1).sum()),
    }


def _meta(frame: pd.DataFrame, symbol: str, timeframe: str, path: str) -> dict:
    first = frame["candle_begin_time"].iloc[0]
    last = frame["candle_begin_time"].iloc[-1]
    span_hours = TIMEFRAME_HOURS.get(str(timeframe).lower(), 24)
    end_of_last = last + pd.Timedelta(hours=span_hours)
    # 源时间戳是北京时间墙上时间，这里也取北京时间再相减，避免受服务器本地时区影响
    now_bj = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=8)
    data_age_hours = (now_bj - end_of_last.to_pydatetime()).total_seconds() / 3600
    return {
        "symbol": symbol.upper(),
        "timeframe": str(timeframe).upper(),
        "from": first.strftime("%Y-%m-%d %H:%M"),
        "to": last.strftime("%Y-%m-%d %H:%M"),
        "count": int(len(frame)),
        "updated_at": datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M:%S"),
        "data_age_hours": round(data_age_hours, 1),
        "partial_last": data_age_hours < 0,
        "time_basis": "Asia/Shanghai",
    }


def build_card_meta(symbol: str, timeframe: str) -> dict:
    """概览用：只回元数据和各类标记数量，不含逐根数据。"""
    path = signals_path(symbol, timeframe)
    frame = load_signals(symbol, timeframe)
    if frame.empty or not os.path.exists(path):
        return {
            "symbol": symbol.upper(),
            "timeframe": str(timeframe).upper(),
            "success": False,
            "message": "信号文件缺失，等待刷新",
        }
    meta = _meta(frame, symbol, timeframe, path)
    meta.update({"success": True, "counts": build_counts(frame)})
    return meta


def build_chart_payload(symbol: str, timeframe: str, mobile: bool = False) -> dict:
    """单卡片用：K线 + 指标 + 标记 + 元数据。"""
    path = signals_path(symbol, timeframe, mobile)
    frame = load_signals(symbol, timeframe, mobile)
    if frame.empty or not os.path.exists(path):
        return {
            "success": False,
            "symbol": symbol.upper(),
            "timeframe": str(timeframe).upper(),
            "message": "信号文件缺失，等待刷新",
        }

    payload = {
        "success": True,
        "symbol": symbol.upper(),
        "timeframe": str(timeframe).upper(),
        "bars": {"t": [_unix(v) for v in frame["candle_begin_time"]]},
        "ind": {},
        "markers": build_markers(frame),
        "counts": build_counts(frame),
        "meta": _meta(frame, symbol, timeframe, path),
    }
    for src, dst in PRICE_COLS.items():
        payload["bars"][dst] = _column(frame, src, 4)
    for src, dst in IND_COLS.items():
        payload["ind"][dst] = _column(frame, src, 6)
    return payload
