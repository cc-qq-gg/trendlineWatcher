import numpy as np
import talib


# 预警和图表共用的轻量指标计算。
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# MACD背离规则版本，用于缓存失效。
MACD_DIVERGENCE_VERSION = 4

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
    """按MACD柱体正负切段。"""
    values = np.asarray(values, dtype="float64")
    signs = np.zeros(len(values), dtype=int)
    finite = np.isfinite(values)
    signs[finite] = np.sign(values[finite]).astype(int)
    segments = []
    if len(signs) == 0:
        return segments
    start = 0
    for index in range(1, len(signs)):
        if signs[index] != signs[start]:
            segments.append((int(signs[start]), np.arange(start, index)))
            start = index
    segments.append((int(signs[start]), np.arange(start, len(signs))))
    return segments


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

    for previous, middle, current in zip(segments, segments[1:], segments[2:]):
        previous_sign, previous_segment = previous
        middle_sign, _ = middle
        current_sign, current_segment = current
        if (previous_sign, middle_sign, current_sign) != (1, -1, 1):
            continue
        previous_hist = previous_segment[np.argmax(hist[previous_segment])]
        current_hist = current_segment[np.argmax(hist[current_segment])]
        previous_price = previous_segment[np.argmax(highs[previous_segment])]
        current_price = current_segment[np.argmax(highs[current_segment])]
        if highs[current_price] > highs[previous_price] and hist[current_hist] < hist[previous_hist]:
            df.loc[current_price, "macd_top_divergence"] = 1

    for previous, middle, current in zip(segments, segments[1:], segments[2:]):
        previous_sign, previous_segment = previous
        middle_sign, _ = middle
        current_sign, current_segment = current
        if (previous_sign, middle_sign, current_sign) != (-1, 1, -1):
            continue
        previous_hist = previous_segment[np.argmin(hist[previous_segment])]
        current_hist = current_segment[np.argmin(hist[current_segment])]
        previous_price = previous_segment[np.argmin(lows[previous_segment])]
        current_price = current_segment[np.argmin(lows[current_segment])]
        if lows[current_price] < lows[previous_price] and hist[current_hist] > hist[previous_hist]:
            df.loc[current_price, "macd_bottom_divergence"] = 1

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
