import random
import numpy as np
import pandas as pd


# 将None作为信号返回
def real_signal_none(df, para):
    """
    发出空交易信号
    :param df:
    :param para:
    :return:
    """

    return None


# 随机生成交易信号
def real_signal_random(df, para=[200, 2]):
    """
    随机发出交易信号
    :param df:
    :param para:
    :return:
    """

    r = random.random()
    if r <= 0.25:
        return 1
    elif r <= 0.5:
        return -1
    elif r <= 0.75:
        return 0
    else:
        return None


# 布林策略实盘交易信号
def real_signal_simple_bolling(df, para=[200, 2]):
    """
    实盘产生布林线策略信号的函数，和历史回测函数相比，计算速度更快。
    布林线中轨：n天收盘价的移动平均线
    布林线上轨：n天收盘价的移动平均线 + m * n天收盘价的标准差
    布林线上轨：n天收盘价的移动平均线 - m * n天收盘价的标准差
    当收盘价由下向上穿过上轨的时候，做多；然后由上向下穿过中轨的时候，平仓。
    当收盘价由上向下穿过下轨的时候，做空；然后由下向上穿过中轨的时候，平仓。
    :param df:  原始数据
    :param para:  参数，[n, m]
    :return:
    """

    # ===策略参数
    # n代表取平均线和标准差的参数
    # m代表标准差的倍数
    n = int(para[0])
    m = para[1]

    # ===计算指标
    # 计算均线
    df["median"] = (
        df["close"].rolling(n).mean()
    )  # 此处只计算最后几行的均线值，因为没有加min_period参数
    median = df.iloc[-1]["median"]
    median2 = df.iloc[-2]["median"]
    # 计算标准差
    df["std"] = (
        df["close"].rolling(n).std(ddof=0)
    )  # ddof代表标准差自由度，只计算最后几行的均线值，因为没有加min_period参数
    std = df.iloc[-1]["std"]
    std2 = df.iloc[-2]["std"]
    # 计算上轨、下轨道
    upper = median + m * std
    lower = median - m * std
    upper2 = median2 + m * std2
    lower2 = median2 - m * std2

    # ===寻找交易信号
    signal = None
    close = df.iloc[-1]["close"]
    close2 = df.iloc[-2]["close"]
    # 找出做多信号
    if (close > upper) and (close2 <= upper2):
        signal = 1
    # 找出做空信号
    elif (close < lower) and (close2 >= lower2):
        signal = -1
    # 找出做多平仓信号
    elif (close < median) and (close2 >= median2):
        signal = 0
    # 找出做空平仓信号
    elif (close > median) and (close2 <= median2):
        signal = 0

    return signal


def define_trendline(df, start_point, end_point):
    """
    根据用户定义的两个点计算趋势线(只在右侧延伸)
    参数:
        df: 包含K线数据的DataFrame
        start_point: 趋势线起点 [时间,价格]
        end_point: 趋势线终点 [时间,价格]
    返回:
        趋势线值的Series
    """
    # 解析起点和终点参数
    start_time, start_price = start_point
    end_time, end_price = end_point

    # 转换为索引位置
    start_idx = df[df["candle_begin_time_GMT8"] == start_time].index[0]
    end_idx = df[df["candle_begin_time_GMT8"] == end_time].index[0]

    # 计算斜率
    slope = (end_price - start_price) / (end_idx - start_idx)

    # 计算趋势线(只在起点右侧延伸)
    trendline = pd.Series(np.nan, index=df.index)
    trendline[start_idx:] = start_price + slope * (
        np.arange(len(df))[start_idx:] - start_idx
    )

    return trendline


def monitor_breakout(df, trendline):
    """
    监控突破情况
    参数:
        df: 包含K线数据的DataFrame
        trendline: 趋势线值的Series
    返回:
        突破信号(1=向上突破, -1=向下跌破, 0=无突破)
    """
    # 获取前一根和收盘价
    prev_close = df["close"].iloc[-2]
    prev_trend = trendline.iloc[-2]

    # 获取前一根和趋势线值
    current_close = df["close"].iloc[-1]
    current_trend = trendline.iloc[-1]

    # 向上突破: 前一根收盘价小于趋势线，当前收盘价大于等于趋势线
    if prev_close < prev_trend and current_close >= current_trend:
        return 1  # 向上突破

    # 向下跌破: 前一根收盘价大于趋势线，当前收盘价小于等于趋势线
    elif prev_close > prev_trend and current_close <= current_trend:
        return -1  # 向下跌破

    else:
        return None


def real_signal_trendline(df, para):
    """
    实盘产生趋势线策略信号的函数
    参数:
        df: 包含K线数据的DataFrame
        para: 策略配置字典，包含以下键:
            - start_point: 趋势线起点 [时间,价格]
            - end_point: 趋势线终点 [时间,价格]
            - direction: 交易方向(1=多, -1=空)
    返回:
        signal: 交易信号(1=做多, -1=做空, 0=平仓, None=无信号)
    """
    # 提取配置参数
    start_point = para.get("start_point")
    end_point = para.get("end_point")
    direction = para.get("direction", 1)

    # 计算趋势线
    trendline = define_trendline(df, start_point, end_point)

    # 获取突破信号
    breakout = monitor_breakout(df, trendline)

    # 初始化信号
    signal = None

    # ===寻找交易信号
    # 向上突破信号（多头）
    if direction == 1 and breakout == 1:
        signal = 1

    # 向下跌破信号（空头）
    elif direction == -1 and breakout == -1:
        signal = -1

    return signal
