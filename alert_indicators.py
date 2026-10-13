import numpy as np
import talib


# 预警和图表共用的轻量指标计算。
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# MACD背离规则版本，用于缓存失效。
MACD_DIVERGENCE_VERSION = 5

# BOLL默认参数，对齐stater。
DEFAULT_BOLL_LENGTH = 10
DEFAULT_BOLL_MULT = 3.0

# BOLL-W收缩阈值：4h=1%，1d=6.0%。
DEFAULT_BOLL_WIDTH = 1.0
DAILY_BOLL_WIDTH = 6.0


def _column_name(df, name):
    """兼容大小写OHLC列名。"""
    if name in df.columns:
        return name
    lower = name.lower()
    if lower in df.columns:
        return lower
    raise KeyError(name)


def histogram_segments(values):
    """按MACD柱体正负切段，零值归入当前柱段。"""
    values = np.asarray(values, dtype="float64")
    segments: list[tuple[int, np.ndarray]] = []
    current_sign = 0
    start = None
    for index, value in enumerate(values):
        if not np.isfinite(value) or value == 0:
            continue
        sign = 1 if value > 0 else -1
        if current_sign == 0:
            current_sign = sign
            start = index
            continue
        if sign != current_sign:
            segments.append((current_sign, np.arange(start, index)))
            current_sign = sign
            start = index
    if current_sign != 0 and start is not None:
        segments.append((current_sign, np.arange(start, len(values))))
    return segments


def _segment_stats(
    sign,
    segment,
    hist,
    highs,
    lows,
):
    if sign > 0:
        hist_idx = int(segment[np.argmax(hist[segment])])
        price_idx = int(segment[np.argmax(highs[segment])])
    else:
        hist_idx = int(segment[np.argmin(hist[segment])])
        price_idx = int(segment[np.argmin(lows[segment])])
    return {
        "sign": sign,
        "start": int(segment[0]),
        "end": int(segment[-1]),
        "hist_idx": hist_idx,
        "price_idx": price_idx,
    }


def add_macd_indicators(df):
    """计算MACD并标记顶/底背离。"""
    df = df.copy()
    close_col = _column_name(df, "Close")
    high_col = _column_name(df, "High")
    low_col = _column_name(df, "Low")
    macd, macd_signal, macd_hist = talib.MACD(
        np.asarray(df[close_col], dtype="float64"),
        fastperiod=MACD_FAST,
        slowperiod=MACD_SLOW,
        signalperiod=MACD_SIGNAL,
    )
    df["macd"] = macd
    df["macd_signal"] = macd_signal
    df["macd_hist"] = macd_hist
    df["macd_top_divergence"] = 0
    df["macd_bottom_divergence"] = 0
    df["macd_divergence_version"] = MACD_DIVERGENCE_VERSION

    hist = np.asarray(macd_hist, dtype="float64")
    highs = np.asarray(df[high_col], dtype="float64")
    lows = np.asarray(df[low_col], dtype="float64")
    segments = histogram_segments(hist)
    df["macd_top_confirmed"] = 0
    df["macd_bottom_confirmed"] = 0

    previous_segment = None
    previous_positive = None
    previous_negative = None
    for segment in segments:
        current = _segment_stats(segment[0], segment[1], hist, highs, lows)
        if previous_segment is not None:
            if previous_segment["sign"] > 0 and current["sign"] < 0:
                if previous_positive is not None:
                    price_now = previous_segment["price_idx"]
                    price_before = previous_positive["price_idx"]
                    hist_now = previous_segment["hist_idx"]
                    hist_before = previous_positive["hist_idx"]
                    if highs[price_now] > highs[price_before] and hist[hist_now] < hist[hist_before]:
                        df.loc[price_now, "macd_top_divergence"] = 1
                        df.loc[current["start"], "macd_top_confirmed"] = 1
                previous_positive = previous_segment
            elif previous_segment["sign"] < 0 and current["sign"] > 0:
                if previous_negative is not None:
                    price_now = previous_segment["price_idx"]
                    price_before = previous_negative["price_idx"]
                    hist_now = previous_segment["hist_idx"]
                    hist_before = previous_negative["hist_idx"]
                    if lows[price_now] < lows[price_before] and hist[hist_now] > hist[hist_before]:
                        df.loc[price_now, "macd_bottom_divergence"] = 1
                        df.loc[current["start"], "macd_bottom_confirmed"] = 1
                previous_negative = previous_segment
        previous_segment = current

    return df


def add_boll_indicators(
    df,
    length=DEFAULT_BOLL_LENGTH,
    mult=DEFAULT_BOLL_MULT,
    width=DEFAULT_BOLL_WIDTH,
):
    """计算BOLL、带宽收缩和突破信号。"""
    df = df.copy()
    close_col = _column_name(df, "Close")
    close = np.asarray(df[close_col], dtype="float64")
    upper, middle, lower = talib.BBANDS(
        close,
        timeperiod=length,
        nbdevup=mult,
        nbdevdn=mult,
        matype=0,
    )
    df["boll_upper"] = upper
    df["boll_middle"] = middle
    df["boll_lower"] = lower
    df["boll_width_pct"] = (df["boll_upper"] - df["boll_lower"]) / df["boll_middle"] * 100

    valid_width = df["boll_width_pct"] >= width
    below_width = df["boll_width_pct"] < width
    prev_valid_width = valid_width.shift(1, fill_value=False).astype(bool)
    prev_below_width = below_width.shift(1, fill_value=False).astype(bool)
    break_up = (df[close_col] > df["boll_upper"]) & (
        df[close_col].shift(1) <= df["boll_upper"].shift(1)
    )
    break_down = (df[close_col] < df["boll_lower"]) & (
        df[close_col].shift(1) >= df["boll_lower"].shift(1)
    )

    df["boll_width_flag"] = valid_width.astype(int)
    df["boll_width_signal"] = (valid_width & ~prev_valid_width).astype(int)
    df["boll_width_below_signal"] = (below_width & ~prev_below_width).astype(int)
    df["boll_breakout_signal"] = np.nan
    df.loc[valid_width & break_up, "boll_breakout_signal"] = 1
    df.loc[valid_width & break_down, "boll_breakout_signal"] = -1
    return df


def boll_width_threshold_for_timeframe(timeframe):
    """返回BOLL-W收缩阈值。"""
    return DAILY_BOLL_WIDTH if str(timeframe).lower() == "1d" else DEFAULT_BOLL_WIDTH
