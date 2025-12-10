"""
更新数据
"""

import pandas as pd
import ccxt
import time
import os
import re
import glob
import traceback
import pytz
from datetime import datetime, timedelta

from config_constants import OKEX_READONLY_CONFIG

pd.set_option("expand_frame_repr", False)  # 当列太多时不换行

# =====设定参数
exchange = ccxt.okx(OKEX_READONLY_CONFIG)
# exchange = ccxt.binance(p)


def get_last_datetime(exchange, symbol, time_interval):
    file_path = f"./data/{exchange.id}/csv/{symbol.replace('/','-')}_{time_interval}.csv"
    print('file_path', file_path)
    df = pd.read_csv(file_path)
    return df["candle_begin_time"].max()


def get_kline(
    start_time="",
    exchange=exchange,
    symbol="BTC/USDT",
    time_interval="15m",
    days=1900,
):
    try:
        end_time = pd.to_datetime(start_time) + timedelta(days=days)
        print("start_time", start_time)
        print("end_time", end_time)
        # =====开始循环抓取数据
        df_list = []
        start_time_since = exchange.parse8601(start_time)

        while True:
            try:
                # 获取数据
                df = exchange.fetch_ohlcv(
                    symbol=symbol, timeframe=time_interval, since=start_time_since, limit=2000
                )

                # 检查是否获取到数据
                if not df or len(df) == 0:
                    print(f"警告: 未获取到数据，symbol={symbol}, timeframe={time_interval}, since={start_time_since}")
                    break

                # 整理数据
                df = pd.DataFrame(df, dtype=float)  # 将数据转换为dataframe
                df["candle_begin_time"] = pd.to_datetime(df[0], unit="ms")  # 整理时间
                # 转换为UTC时区，然后转换为北京时间
                df["candle_begin_time"] = df["candle_begin_time"].dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
                # 保存时去掉时区信息，但保持北京时间
                df["candle_begin_time"] = df["candle_begin_time"].dt.tz_localize(None)
                print(df)

                # 合并数据
                df_list.append(df)

                # 新的since
                t = pd.to_datetime(df.iloc[-1][0], unit="ms")
                print(t)
                start_time_since = exchange.parse8601(str(t))

                # 判断是否跳出循环
                if t >= end_time or df.shape[0] <= 1:
                    print("抓取完所需数据，或抓取至最新数据，完成抓取任务，退出循环")
                    break

                # 抓取间隔需要暂停2s，防止抓取过于频繁
                time.sleep(2)

            except ccxt.NetworkError as e:
                print(f"网络错误: {str(e)}，等待10秒后重试...")
                time.sleep(10)  # 网络错误时等待更长时间
                continue

            except ccxt.ExchangeNotAvailable as e:
                print(f"交易所不可用: {str(e)}")
                if "restricted location" in str(e):
                    print("检测到地区限制，尝试从本地文件加载数据...")
                    try:
                        local_file = f"./data/{exchange.id}/csv/{symbol.replace('/','-')}_{time_interval}.csv"
                        if os.path.exists(local_file):
                            print(f"从本地文件加载数据: {local_file}")
                            return pd.read_csv(local_file)
                    except Exception as local_e:
                        print(f"从本地文件加载失败: {str(local_e)}")
                break

            except ccxt.RateLimitExceeded as e:
                print(f"超过频率限制: {str(e)}，等待60秒...")
                time.sleep(60)
                continue

            except Exception as e:
                print(f"获取数据时发生错误: {str(e)}")
                traceback.print_exc()
                break

        # 检查是否获取到任何数据
        if not df_list:
            print(f"未能获取到任何数据，返回空DataFrame")
            return pd.DataFrame(columns=["candle_begin_time", "open", "high", "low", "close", "volume"])

        # =====合并整理数据
        df = pd.concat(df_list, ignore_index=True)
        df.rename(
            columns={0: "MTS", 1: "open", 2: "high", 3: "low", 4: "close", 5: "volume"},
            inplace=True,
        )  # 重命名
        df["candle_begin_time"] = pd.to_datetime(df["MTS"], unit="ms")  # 整理时间
        # 转换为UTC时区，然后转换为北京时间
        df["candle_begin_time"] = df["candle_begin_time"].dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
        # 保存时去掉时区信息，但保持北京时间
        df["candle_begin_time"] = df["candle_begin_time"].dt.tz_localize(None)
        df = df[
            ["candle_begin_time", "open", "high", "low", "close", "volume"]
        ]  # 整理列的顺序

        # 去重、排序
        df.drop_duplicates(subset=["candle_begin_time"], keep="last", inplace=True)
        df.sort_values("candle_begin_time", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    except Exception as e:
        print(f"处理数据时发生错误: {str(e)}")
        traceback.print_exc()
        # 返回空DataFrame，保持列结构一致
        return pd.DataFrame(columns=["candle_begin_time", "open", "high", "low", "close", "volume"])


def get_data(
    start_time,
    exchange,
    symbol,
    time_interval,
    days=1900,
):
    df = get_kline(start_time, exchange, symbol, time_interval, days)
    # =====保存数据到文件
    if df.shape[0] > 0:
        # 根目录，确保该路径存在
        path = "data"
        # # 创建交易所文件夹
        path = os.path.join(path, exchange.id)
        if os.path.exists(path) is False:
            os.mkdir(path)
        # 创建spot文件夹
        path = os.path.join(path, "spot")
        if os.path.exists(path) is False:
            os.mkdir(path)
        # 创建日期文件夹
        path = os.path.join(path, str(pd.to_datetime(start_time).date()))
        if os.path.exists(path) is False:
            os.mkdir(path)

        # 拼接文件目录
        file_name = "_".join([symbol.replace("/", "-"), time_interval]) + ".csv"
        path = os.path.join(path, file_name)
        print(path)

        df.to_csv(path, index=False)


def parse_timeframe(timeframe):
    """将时间间隔字符串（如5m, 1h）解析为timedelta参数"""
    match = re.search(r"(\d+)([mhHdwM])", timeframe)
    if not match:
        return None, None

    value, unit = match.groups()
    value = int(value)

    if unit.lower() == "m":
        return value, "minutes"
    elif unit.lower() == "h":
        return value, "hours"
    elif unit.lower() == "d":
        return value, "days"
    elif unit.lower() == "w":
        return value * 7, "days"
    elif unit.lower() == "M":
        return value * 30, "days"
    return None, None


def fill_missing_data_api(file_path, exchange=exchange, max_days_per_request=5):
    """
    通过API调用补全CSV文件中缺失的K线数据

    参数:
    file_path: CSV文件路径
    exchange: 交易所对象
    max_days_per_request: 每次API请求最大天数，避免请求过大

    返回:
    补全后的DataFrame
    """
    try:
        print(f"正在处理文件: {file_path}")

        # 读取现有数据
        df = pd.read_csv(file_path)
        if "candle_begin_time" not in df.columns:
            print(f"错误: {file_path} 中没有找到 candle_begin_time 列")
            return None

        # 确保时间列是日期时间格式
        df["candle_begin_time"] = pd.to_datetime(df["candle_begin_time"])

        # 提取交易对和时间间隔信息
        filename = os.path.basename(file_path)
        parts = filename.split("_")
        if len(parts) < 2:
            print(f"错误: 无法从文件名 {filename} 解析出交易对和时间间隔")
            return None

        symbol_parts = parts[0].split("-")
        if len(symbol_parts) < 2:
            print(f"错误: 无法从 {parts[0]} 解析出交易对")
            return None

        symbol = f"{symbol_parts[0]}/{symbol_parts[1]}"
        time_interval = parts[1].replace(".csv", "")

        # 从文件名解析时间间隔
        interval_value, interval_unit = parse_timeframe(time_interval)
        if not interval_value or not interval_unit:
            print(f"错误: 无法解析时间间隔 {time_interval}")
            return None

        # 计算时间间隔
        kwargs = {interval_unit: interval_value}
        expected_interval = timedelta(**kwargs)

        # 排序数据
        df = df.sort_values("candle_begin_time").reset_index(drop=True)

        # 检测缺失的时间段
        missing_periods = []
        for i in range(1, len(df)):
            actual_interval = (
                df["candle_begin_time"].iloc[i] - df["candle_begin_time"].iloc[i - 1]
            )
            if actual_interval > expected_interval:
                # 计算缺失的起止时间
                start_time = df["candle_begin_time"].iloc[i - 1] + expected_interval
                end_time = df["candle_begin_time"].iloc[i] - expected_interval

                # 如果只缺一个点，调整结束时间
                if start_time > end_time:
                    end_time = start_time

                missing_periods.append({"start_time": start_time, "end_time": end_time})

        if not missing_periods:
            print(f"文件 {filename} 没有检测到缺失的数据")
            return df

        print(f"检测到 {len(missing_periods)} 个缺失时间段，正在通过API获取数据...")

        # 获取所有缺失数据
        all_missing_data = []

        for period in missing_periods:
            start_time_str = str(period["start_time"])
            end_time = period["end_time"]

            # 根据时间跨度可能需要分多次请求
            current_start = period["start_time"]

            while current_start <= end_time:
                # 计算当前批次的结束时间，不超过原定结束时间和最大请求天数
                current_end = min(
                    end_time, current_start + timedelta(days=max_days_per_request)
                )

                print(
                    f"正在请求: {symbol} {time_interval} 从 {current_start} 到 {current_end}"
                )

                try:
                    # 获取这段时间的数据
                    missing_df = get_kline(
                        start_time=str(current_start),
                        exchange=exchange,
                        symbol=symbol,
                        time_interval=time_interval,
                        days=(current_end - current_start).days + 1,
                    )

                    # 检查是否获取到了数据
                    if missing_df is not None and not missing_df.empty:
                        all_missing_data.append(missing_df)
                        print(f"成功获取到 {len(missing_df)} 条数据")

                        # 保存到相应的日期目录中
                        for date, group in missing_df.groupby(missing_df["candle_begin_time"].dt.date):
                            # 构建目标目录路径
                            target_dir = os.path.join("data", exchange.id, "spot", str(date))
                            os.makedirs(target_dir, exist_ok=True)

                            # 构建目标文件路径
                            target_file = os.path.join(target_dir, filename)

                            # 如果文件已存在，则合并数据
                            if os.path.exists(target_file):
                                try:
                                    existing_df = pd.read_csv(target_file)
                                    existing_df["candle_begin_time"] = pd.to_datetime(existing_df["candle_begin_time"])

                                    # 合并数据
                                    merged_df = pd.concat([existing_df, group], ignore_index=True)
                                    merged_df.drop_duplicates(subset=["candle_begin_time"], keep="first", inplace=True)
                                    merged_df.sort_values("candle_begin_time", inplace=True)
                                    merged_df.reset_index(drop=True, inplace=True)

                                    # 保存合并后的数据
                                    merged_df.to_csv(target_file, index=False)
                                    print(f"已将数据合并到现有文件: {target_file}")
                                except Exception as e:
                                    print(f"合并数据到 {target_file} 时出错: {str(e)}")
                                    # 如果合并失败，直接保存新数据
                                    group.to_csv(target_file, index=False)
                            else:
                                # 直接保存新数据
                                group.to_csv(target_file, index=False)
                                print(f"已将数据保存到新文件: {target_file}")
                    else:
                        print(f"未能获取到从 {current_start} 到 {current_end} 的数据")

                except Exception as e:
                    print(f"获取数据时出错: {str(e)}")

                # 更新下一批次的起始时间
                current_start = current_end + expected_interval

                # 请求间隔，避免API限制
                time.sleep(3)

        # 合并所有数据到原始文件
        if all_missing_data:
            missing_df_combined = pd.concat(all_missing_data, ignore_index=True)

            # 确保没有重复
            missing_df_combined.drop_duplicates(
                subset=["candle_begin_time"], keep="first", inplace=True
            )

            # 与原数据合并
            combined_df = pd.concat([df, missing_df_combined], ignore_index=True)
            combined_df.drop_duplicates(
                subset=["candle_begin_time"], keep="first", inplace=True
            )
            combined_df.sort_values("candle_begin_time", inplace=True)
            combined_df.reset_index(drop=True, inplace=True)

            # 保存合并后的数据到原始文件
            combined_df.to_csv(file_path, index=False)
            print(f"已将补全后的数据保存到原始文件: {file_path}")

            return combined_df
        else:
            print(f"没有获取到任何缺失数据，保持原文件不变")
            return df

    except Exception as e:
        print(f"处理文件 {file_path} 时出错: {str(e)}")
        traceback.print_exc()
        return None


def fill_missing_batch_api(directory, exchange=exchange, pattern="*.csv"):
    """
    批量处理目录中的文件，通过API补全缺失数据

    参数:
    directory: 目录路径
    exchange: 交易所对象
    pattern: 文件匹配模式
    """

    # 获取所有匹配的文件
    files = glob.glob(os.path.join(directory, "**", pattern), recursive=True)

    if not files:
        print(f"在目录 {directory} 中没有找到匹配的文件")
        return

    print(f"找到 {len(files)} 个文件，开始处理...")

    # 处理每个文件
    for file_path in files:
        try:
            fill_missing_data_api(file_path, exchange)
        except Exception as e:
            print(f"处理文件 {file_path} 时出错: {str(e)}")

            traceback.print_exc()

    print("批量处理完成")


if __name__ == "__main__":
    symbols = [
        "BTC/USDT", "ETH/USDT",
        "SOL/USDT"]
    time_interval = "5m"  # 其他可以尝试的值：'1m', '5m', '15m', '30m', '1h', '2h', '1d', '1w', '1M', '1y'
    for symbol in symbols:
        last_datetime = get_last_datetime(exchange, symbol, time_interval)
        last_datetime = str(datetime.strptime(
            last_datetime, "%Y-%m-%d %H:%M:%S"
        ) - timedelta(days=6))
        print(f"{symbol} 开始时间：{last_datetime}")
        get_data(last_datetime, exchange, symbol, time_interval)

    # 示例：通过API补全指定文件的缺失数据
    # fill_missing_data_api('./data/okx/csv/BTC-USDT_5m.csv')
    # fill_missing_data_api("./data/okx/csv/SOL-USDT_5m.csv")
    # fill_missing_data_api("./data/okx/csv/ETH-USDT_5m.csv")

    # 示例：批量通过API补全目录中所有5分钟数据文件的缺失数据
    # fill_missing_batch_api('./data/okx/csv', pattern='*_5m.csv')
