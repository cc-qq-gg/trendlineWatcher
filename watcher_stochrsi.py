import numpy as np
from datetime import datetime, timedelta
import pandas as pd
import talib
from get_data import smart_get_data
import schedule
import time
from send_email import send_email
import os
from volatility_calculator import calculate_volatility, calculate_sigma_level
from divergence_algorithm import detect_top_divergence, detect_bottom_divergence

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

        stochrsi, _ = StochRSI(df["close"].tolist(), m=14, p=3)

        df.drop_duplicates(subset=["datetime"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        df["stochrsi"] = stochrsi

        # 计算波动率
        window = 20
        print(f"正在计算 {symbol} {time_interval} 的波动率...")
        volatility_data = calculate_volatility(df, method='returns', window=window)

        # 计算每行的滚动波动率
        returns = df['close'].pct_change().dropna()


        # 计算滚动波动率序列
        rolling_volatility = returns.rolling(window=window).std() * 100

        # 为每行计算对应的波动率值
        df['volatility'] = np.nan
        df['volatility_rolling'] = np.nan
        df['volatility_sigma_level'] = ''
        df['volatility_trend'] = 'stable'
        df['volatility_historical_mean'] = volatility_data['historical_mean']
        df['volatility_historical_std'] = volatility_data['historical_std']

        # 为有效数据行赋值
        for i in range(window, len(df)):
            if i-1 < len(rolling_volatility) and not pd.isna(rolling_volatility.iloc[i-1]):
                df.iloc[i, df.columns.get_loc('volatility')] = rolling_volatility.iloc[i-1]
                df.iloc[i, df.columns.get_loc('volatility_rolling')] = rolling_volatility.iloc[i-1]

                # 计算该行的sigma级别 - 使用整个滚动序列的历史统计
                vol = rolling_volatility.iloc[i-1]

                # 使用整个有效波动率序列计算历史统计
                valid_vols = rolling_volatility.dropna()
                if len(valid_vols) > 1:
                    historical_mean = valid_vols.mean()
                    historical_std = valid_vols.std()
                    sigma_level, _ = calculate_sigma_level(vol, historical_mean, historical_std)
                else:
                    sigma_level = ''

                df.iloc[i, df.columns.get_loc('volatility_sigma_level')] = sigma_level

        print(f"波动率计算完成: {volatility_data['current_volatility']:.4f}% ({volatility_data['sigma_level']})")

        # 确保 data/stochrsi 目录存在
        output_dir = "data/stochrsi"
        os.makedirs(output_dir, exist_ok=True)

        # 保存结果到指定目录
        output_file = f"{output_dir}/stochrsi_{symbol}_{time_interval}.csv"
        df.to_csv(output_file, index=False)
        print(f"原始数据和指标已保存到: {output_file}")
        # stochrsi 15m
        # 1. 小于5，注意最近支撑位强度!!!；三次以上触及支撑，看空!!!, 4h 大概率低点
        # 2. 大于92，可能短期高点，是否顶背离，4h 首次达到 大概率继续
        last_stochrsi = stochrsi[-1]
        HIGH = 94
        # if last_stochrsi < 5 or last_stochrsi > HIGH:
        #     msg = (
        #         f"{time_interval}，{last_stochrsi},可能短期高点位，观察顶背离"
        #         if last_stochrsi > HIGH
        #         else f"{time_interval}，{last_stochrsi}，注意最近支撑位强度!!!；三次以上触及支撑，看空!!!"
        #     )
        #     send_email(subject=f"{time_interval}-{last_stochrsi}", content=msg)
        #     print(msg)

        coin = symbol.split("-")[0]  # BTC-USDT-SWAP -> BTC

        dbl_result = is_dbl(df, time_interval, symbol)
        if dbl_result["is_dbl"]:
            first = dbl_result.get("first_stochrsi", "?")
            dist = dbl_result.get("distance", "?")
            second = dbl_result.get("second_stochrsi", "?")

            subject = f"{coin}-{time_interval}-dbl {first},{dist},{second}"
            send_email(subject=subject, content="")
            show_notification(f"{coin}-{time_interval}", subject)

        tbl_result = is_tbl(df, time_interval, symbol)
        if tbl_result["is_tbl"]:
            first = tbl_result.get("first_stochrsi", "?")
            dist = tbl_result.get("distance", "?")
            second = tbl_result.get("second_stochrsi", "?")

            subject = f"{coin}-{time_interval}-tbl {first},{dist},{second}"
            send_email(subject=subject, content="")
            show_notification(f"{coin}-{time_interval}", subject)

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
    watch(time_interval="1D",limit=200)
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
    send_email(subject=f"stochrsiWatcher start", content='')
    # exit()
    while True:
        schedule.run_pending()
        time.sleep(1)
