import numpy as np
import pandas as pd
import talib
from get_data import smart_get_data
import schedule
import time
from send_email import send_email
import os
from divergence_algorithm import detect_top_divergence, detect_bottom_divergence
from alert_indicators import (
    add_boll_indicators,
    add_macd_indicators,
    boll_width_threshold_for_timeframe,
)

np.set_printoptions(suppress=True)  # 取消科学计数法


# 计算公式
def StochRSI(close=[], m=14, p=3):
    RSI = talib.RSI(np.array(close), timeperiod=m)
    RSI = pd.DataFrame(RSI)
    LLV = RSI.rolling(window=m).min()
    HHV = RSI.rolling(window=m).max()
    stochRSI = (RSI - LLV) / (HHV - LLV) * 100
    stochRSI = talib.MA(np.array(stochRSI[0]), p)
    stochRSI = np.around(stochRSI, decimals=2, out=None)
    fastk = talib.MA(np.array(stochRSI), p)
    fastk = np.around(fastk, decimals=2, out=None)
    dif = stochRSI - fastk
    return stochRSI, fastk


# 创建一个通知函数替代 ToastNotifier
def show_notification(title, message):
    # 使用 macOS 的 osascript 来显示通知
    os.system(
        f"""
    osascript -e 'display notification "{message}" with title "{title}"'
    """
    )


def _format_number(value, digits=2):
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.{digits}f}"


def _time_text(row):
    value = row.get("datetime", row.get("candle_begin_time", ""))
    return str(value)


def _close_value(row):
    return row.get("close", row.get("Close"))


def build_indicator_alerts(df, symbol, time_interval):
    """基于上一根已确认K线生成简短预警。"""
    if len(df) < 2:
        return []

    row = df.iloc[-2]
    coin = symbol.split("-")[0]
    tf = str(time_interval).upper()
    alerts = []

    if int(row.get("macd_top_divergence", 0) or 0) == 1:
        subject = f"{coin}-{tf} 顶背离，注意上方压力"
        content = "顶背离：上涨动能减弱，注意上方压力。"
        alerts.append({"subject": subject, "content": content})

    if int(row.get("macd_bottom_divergence", 0) or 0) == 1:
        subject = f"{coin}-{tf} 底背离，注意下方支撑"
        content = "底背离：下跌动能减弱，注意下方支撑。"
        alerts.append({"subject": subject, "content": content})

    if int(row.get("boll_width_below_signal", 0) or 0) == 1:
        subject = f"{coin}-{tf} W收缩，等待方向突破"
        content = "W收缩：波动率压缩，等待方向突破。"
        alerts.append({"subject": subject, "content": content})

    breakout_signal = row.get("boll_breakout_signal")
    if pd.notna(breakout_signal) and int(breakout_signal) in (1, -1):
        side = "上破" if int(breakout_signal) == 1 else "下破"
        subject = f"{coin}-{tf} BOLL{side}，关注{'延续' if int(breakout_signal) == 1 else '下行'}"
        content = (
            "BOLL上破：价格突破上轨，关注趋势延续。"
            if int(breakout_signal) == 1
            else "BOLL下破：价格跌破下轨，关注下行延续。"
        )
        alerts.append({"subject": subject, "content": content})

    return alerts


def get_confirmation_time(df, point_index):
    point_pos = df.index.get_loc(point_index)
    if point_pos < len(df) - 1:
        return df.iloc[point_pos + 1]["datetime"]
    return None


def is_tbl(df, time_interval, symbol):
    try:
        df = df.copy()
        confirmation_time = df.iloc[-2]["datetime"]

        # 计算 turn + 顶背离
        df, df_turn, bl = detect_top_divergence(
            df, stoch_col="stochrsi", high_col="high", turn_col="turn", signal_col="bl"
        )
        # 确保 data/stochrsi 目录存在
        output_dir = "data/stochrsi"
        os.makedirs(output_dir, exist_ok=True)

        output_file = f"{output_dir}/stochrsi_{symbol}_{time_interval}_tbl_turn.csv"
        df.to_csv(output_file, index=False)
        print(f"Turn数据已保存到: {output_file}")

        # 确保 data/stochrsi 目录存在
        output_dir = "data/stochrsi"
        os.makedirs(output_dir, exist_ok=True)

        # 保存结果到指定目录
        output_file = f"{output_dir}/stochrsi_{symbol}_{time_interval}_tbl.csv"
        bl.to_csv(output_file, index=False)
        print(f"Turn分析结果已保存到: {output_file}")

        # 检查是否有tbl信号并返回详细信息
        if not bl.empty:
            current_point = bl.iloc[-1]
            current_confirmation_time = get_confirmation_time(df, current_point.name)
            if current_confirmation_time != confirmation_time:
                return {"is_tbl": False}
            # 找到形成本次背离的前一个顶部转折点，而不是前一个背离信号
            current_pos = df_turn.index.get_loc(current_point.name)
            if current_pos >= 1:
                prev_point = df_turn.iloc[current_pos - 1]
                # 计算距离周期数：在原始df中两个点之间的K线数量
                first_time = prev_point["datetime"]
                second_time = current_point["datetime"]
                distance = len(df[(df["datetime"] > first_time) & (df["datetime"] <= second_time)])

                return {
                    "is_tbl": True,
                    "first_stochrsi": int(round(prev_point["stochrsi"])),
                    "second_stochrsi": int(round(current_point["stochrsi"])),
                    "distance": distance
                }
            else:
                # 只有一个点的情况
                return {
                    "is_tbl": True,
                    "first_stochrsi": None,
                    "second_stochrsi": int(round(current_point["stochrsi"])),
                    "distance": None
                }

        return {"is_tbl": False}
    except Exception as e:
        print(e)
        return {"is_tbl": False}


def is_dbl(df, time_interval, symbol):
    try:
        df = df.copy()
        confirmation_time = df.iloc[-2]["datetime"]

        # 计算 turn + 底背离
        df, df_turn, bl = detect_bottom_divergence(
            df, stoch_col="stochrsi", low_col="low", turn_col="turn", signal_col="dbl"
        )

        # 确保 data/stochrsi 目录存在
        output_dir = "data/stochrsi"
        os.makedirs(output_dir, exist_ok=True)

        # 保存结果到指定目录
        output_file = f"{output_dir}/stochrsi_{symbol}_{time_interval}_dbl.csv"
        df_turn.to_csv(output_file, index=False)
        print(f"DBL分析结果已保存到: {output_file}")

        # 检查是否有dbl信号并返回详细信息
        if not bl.empty:
            current_point = bl.iloc[-1]
            current_confirmation_time = get_confirmation_time(df, current_point.name)
            if current_confirmation_time != confirmation_time:
                return {"is_dbl": False}
            current_pos = df_turn.index.get_loc(current_point.name)
            if current_pos >= 1:
                prev_point = df_turn.iloc[current_pos - 1]
                # 计算距离周期数：在原始df中两个点之间的K线数量
                first_time = prev_point["datetime"]
                second_time = current_point["datetime"]
                distance = len(df[(df["datetime"] > first_time) & (df["datetime"] <= second_time)])

                return {
                    "is_dbl": True,
                    "first_stochrsi": int(round(prev_point["stochrsi"])),
                    "second_stochrsi": int(round(current_point["stochrsi"])),
                    "distance": distance
                }
            else:
                # 只有一个点的情况
                return {
                    "is_dbl": True,
                    "first_stochrsi": None,
                    "second_stochrsi": int(round(current_point["stochrsi"])),
                    "distance": None
                }

        return {"is_dbl": False}
    except Exception as e:
        print(e)
        return {"is_dbl": False}


# def watch(time_interval="5m", rule="15min", symbol="BTC/USDT", days=15):
def watch(time_interval="15m", symbol="SOL-USDT-SWAP", required_days=90):
    try:
        # 使用 smart_get_data 获取数据
        result = smart_get_data(
            symbol=symbol,
            timeframe=time_interval,
            required_days=required_days,
            verbose=True,
        )
        df = result["df"].copy()

        # 将 candle_begin_time 重命名为 datetime，保持兼容性
        if "candle_begin_time" in df.columns:
            df.rename(columns={"candle_begin_time": "datetime"}, inplace=True)

        df.drop_duplicates(subset=["datetime"], inplace=True)
        df.reset_index(drop=True, inplace=True)

        # 停用StochRSI预警，改用BOLL/MACD。
        df = add_boll_indicators(df, width=boll_width_threshold_for_timeframe(time_interval))
        df = add_macd_indicators(df)

        # 确保 data/indicator_alerts 目录存在
        output_dir = "data/indicator_alerts"
        os.makedirs(output_dir, exist_ok=True)

        # 保存结果到指定目录
        output_file = f"{output_dir}/boll_macd_{symbol}_{time_interval}.csv"
        df.to_csv(output_file, index=False)
        print(f"原始数据和指标已保存到: {output_file}")
        alerts = build_indicator_alerts(df, symbol=symbol, time_interval=time_interval)
        for alert in alerts:
            send_email(subject=alert["subject"], content=alert["content"])
            show_notification(f"{symbol.split('-')[0]}-{str(time_interval).upper()}", alert["subject"])

    except Exception as e:
        print('Exception',e)


def watch15m():
    watch(time_interval="15m")
    # 打印当前时间
    print("watch15m", time.strftime("%Y-%m-%d %H:%M:%S"))


def watch1w():
    watch(time_interval="1h")
    print("watch1w", time.strftime("%Y-%m-%d %H:%M:%S"))


def watch4h():
    watch(time_interval="4h")
    print("watch4h", time.strftime("%Y-%m-%d %H:%M:%S"))


def watch1d():
    # watch(time_interval="15m",limit=100)
    # watch(time_interval="4H",limit=100)
    watch(time_interval="1D", required_days=200)
    # watch(time_interval="1W",limit=200)
    print("watch1d", time.strftime("%Y-%m-%d %H:%M:%S"))


def watchPlan():
    symbols = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"]
    # time_intervals = ["15m", "1W", "1D", "4H"]
    time_intervals = ["1W", "1D", "4H"]
    for symbol in symbols:
        for time_interval in time_intervals:
            watch(time_interval=time_interval, symbol=symbol)
    print("watchPlan", time.strftime("%Y-%m-%d %H:%M:%S"))


# 15m
# schedule.every().hour.at(":00").do(watchPlan)
# schedule.every().hour.at(":15").do(watchPlan)
# schedule.every().hour.at(":30").do(watchPlan)
# schedule.every().hour.at(":45").do(watchPlan)
# 4h
schedule.every().day.at("00:00").do(watchPlan)
schedule.every().day.at("04:00").do(watchPlan)
schedule.every().day.at("08:00").do(watchPlan)
schedule.every().day.at("12:00").do(watchPlan)
schedule.every().day.at("16:00").do(watchPlan)
schedule.every().day.at("20:00").do(watchPlan)
# # 1d
# schedule.every().day.at("23:00").do(watch1d)
# # 1w
# schedule.every().hour.at(":00").do(watch1w)

if __name__ == "__main__":
    # watch15m()
    # watch1w()
    # watch1d()
    watchPlan()
    send_email(subject=f"indicatorWatcher start", content='')
    # exit()
    while True:
        schedule.run_pending()
        time.sleep(1)
