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
    file_path = (
        f"./data/{exchange.id}/csv/{symbol.replace('/','-')}_{time_interval}.csv"
    )
    print("file_path", file_path)
    df = pd.read_csv(file_path)
    return df["candle_begin_time"].max()


def resample_kline(
    df: pd.DataFrame, target_timeframe: str, time_col: str = None
) -> pd.DataFrame:
    """
    将K线数据从15m周期聚合为目标周期

    参数:
        df: 15m周期的K线数据
        target_timeframe: 目标时间周期，如 '30m', '1H', '4H', '1D', '1W'
        time_col: 时间列名（自动检测）

    返回:
        聚合后的K线数据
    """
    if df is None or df.empty:
        return df

    # 如果目标周期是 15m，直接返回
    if target_timeframe.lower() in ["15m", "15min"]:
        return df

    # 自动检测时间列
    if time_col is None:
        for col in ["candle_begin_time", "datetime", "candle_begin_time_GMT8", "time"]:
            if col in df.columns:
                time_col = col
                break

    if time_col is None:
        print(f"警告: 未找到时间列，无法聚合")
        return df

    # 周期映射到 pandas resample 频率
    timeframe_map = {
        "30m": "30min",
        "1h": "1h",
        "1H": "1h",
        "2h": "2h",
        "2H": "2h",
        "4h": "4h",
        "4H": "4h",
        "6h": "6h",
        "6H": "6h",
        "12h": "12h",
        "12H": "12h",
        "1d": "1D",
        "1D": "1D",
        "1w": "1W",
        "1W": "1W",
    }

    freq = timeframe_map.get(target_timeframe)
    if freq is None:
        print(f"警告: 不支持的时间周期 {target_timeframe}，返回原始数据")
        return df

    # 确保时间列是 datetime 类型
    df = df.copy()
    df[time_col] = pd.to_datetime(df[time_col])
    df = df.set_index(time_col)

    # 聚合规则
    ohlc_dict = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }

    # 只保留存在的列
    available_cols = {k: v for k, v in ohlc_dict.items() if k in df.columns}

    # 执行聚合
    resampled = df.resample(freq).agg(available_cols)

    # 删除空行（非交易时段）
    resampled = resampled.dropna()

    # 重置索引
    resampled = resampled.reset_index()

    print(
        f"聚合完成: {len(df)} 条 15m 数据 -> {len(resampled)} 条 {target_timeframe} 数据"
    )

    return resampled


def get_kline(
    start_time="",
    exchange=exchange,
    symbol="BTC/USDT",
    time_interval="15m",
    days=1900,
    limit=None,
):
    """
    获取K线数据

    参数:
        start_time: 开始时间
        exchange: 交易所
        symbol: 交易对
        time_interval: 时间周期
        days: 获取天数
        limit: 直接获取最新 N 根数据（不指定时间范围）

    注意: 始终从交易所获取 15m 数据，其他周期通过聚合计算得到
    """
    # 记录原始请求的时间周期
    original_timeframe = time_interval

    try:
        # 如果指定了 limit，直接获取最新 N 根数据
        if limit is not None:
            print(f"请求最新 {limit} 根 K线 (周期: 15m)")
            df = exchange.fetch_ohlcv(
                symbol=symbol, timeframe="15m", limit=limit
            )
            if not df or len(df) == 0:
                print(f"警告: 未获取到数据，symbol={symbol}")
                return pd.DataFrame()

            df = pd.DataFrame(df, dtype=float)
            df["candle_begin_time"] = pd.to_datetime(df[0], unit="ms")
            df["candle_begin_time"] = (
                df["candle_begin_time"]
                .dt.tz_localize("UTC")
                .dt.tz_convert("Asia/Shanghai")
            )
            df["candle_begin_time"] = df["candle_begin_time"].dt.tz_localize(None)
            df.columns = ["timestamp", "open", "high", "low", "close", "volume", "candle_begin_time"]
            print(f"成功获取 {len(df)} 根 K线")
            return df

        end_time = pd.to_datetime(start_time) + timedelta(days=days)
        print("start_time", start_time)
        print("end_time", end_time)
        print(f"请求周期: {original_timeframe} (实际获取: 15m)")

        # =====开始循环抓取数据
        # 始终获取 15m 数据
        df_list = []
        start_time_since = exchange.parse8601(start_time)

        while True:
            try:
                # 获取数据 - 始终使用 15m 周期
                df = exchange.fetch_ohlcv(
                    symbol=symbol, timeframe="15m", since=start_time_since, limit=2000
                )

                # 检查是否获取到数据
                if not df or len(df) == 0:
                    print(
                        f"警告: 未获取到数据，symbol={symbol}, timeframe=15m, since={start_time_since}"
                    )
                    break

                # 整理数据
                df = pd.DataFrame(df, dtype=float)  # 将数据转换为dataframe
                df["candle_begin_time"] = pd.to_datetime(df[0], unit="ms")  # 整理时间
                # 转换为UTC时区，然后转换为北京时间
                df["candle_begin_time"] = (
                    df["candle_begin_time"]
                    .dt.tz_localize("UTC")
                    .dt.tz_convert("Asia/Shanghai")
                )
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
            return pd.DataFrame(
                columns=["candle_begin_time", "open", "high", "low", "close", "volume"]
            )

        # =====合并整理数据
        df = pd.concat(df_list, ignore_index=True)
        df.rename(
            columns={0: "MTS", 1: "open", 2: "high", 3: "low", 4: "close", 5: "volume"},
            inplace=True,
        )  # 重命名
        df["candle_begin_time"] = pd.to_datetime(df["MTS"], unit="ms")  # 整理时间
        # 转换为UTC时区，然后转换为北京时间
        df["candle_begin_time"] = (
            df["candle_begin_time"].dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai")
        )
        # 保存时去掉时区信息，但保持北京时间
        df["candle_begin_time"] = df["candle_begin_time"].dt.tz_localize(None)
        df = df[
            ["candle_begin_time", "open", "high", "low", "close", "volume"]
        ]  # 整理列的顺序

        # 去重、排序
        df.drop_duplicates(subset=["candle_begin_time"], keep="last", inplace=True)
        df.sort_values("candle_begin_time", inplace=True)
        df.reset_index(drop=True, inplace=True)

        # 如果请求的不是 15m 周期，进行聚合
        if original_timeframe.lower() not in ["15m", "15min"]:
            print(f"\n聚合数据: 15m -> {original_timeframe}")
            df = resample_kline(df, original_timeframe)

        return df

    except Exception as e:
        print(f"处理数据时发生错误: {str(e)}")
        traceback.print_exc()
        # 返回空DataFrame，保持列结构一致
        return pd.DataFrame(
            columns=["candle_begin_time", "open", "high", "low", "close", "volume"]
        )


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
                        for date, group in missing_df.groupby(
                            missing_df["candle_begin_time"].dt.date
                        ):
                            # 构建目标目录路径
                            target_dir = os.path.join(
                                "data", exchange.id, "spot", str(date)
                            )
                            os.makedirs(target_dir, exist_ok=True)

                            # 构建目标文件路径
                            target_file = os.path.join(target_dir, filename)

                            # 如果文件已存在，则合并数据
                            if os.path.exists(target_file):
                                try:
                                    existing_df = pd.read_csv(target_file)
                                    existing_df["candle_begin_time"] = pd.to_datetime(
                                        existing_df["candle_begin_time"]
                                    )

                                    # 合并数据
                                    merged_df = pd.concat(
                                        [existing_df, group], ignore_index=True
                                    )
                                    merged_df.drop_duplicates(
                                        subset=["candle_begin_time"],
                                        keep="first",
                                        inplace=True,
                                    )
                                    merged_df.sort_values(
                                        "candle_begin_time", inplace=True
                                    )
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


# ==================== 智能数据管理功能 ====================


def get_local_data_info(
    symbol: str, timeframe: str, data_dir: str = "data", exchange_id: str = "okx"
) -> dict:
    """
    获取本地数据信息

    参数:
        symbol: 交易对，如 'BTC-USDT-SWAP' 或 'BTC/USDT'
        timeframe: 时间周期，如 '1D', '4H', '15m'
        data_dir: 数据目录
        exchange_id: 交易所ID

    返回:
        {
            'file_path': str,        # 数据文件路径
            'exists': bool,          # 文件是否存在
            'row_count': int,        # 数据行数
            'time_range': tuple,     # (start_time, end_time)
            'has_missing': bool,     # 是否有缺失
            'missing_count': int,    # 缺失数量
            'file_size_kb': float    # 文件大小(KB)
        }
    """
    # 标准化symbol格式
    symbol_normalized = symbol.replace("/", "-")

    # 可能的文件路径（按优先级排序）
    possible_paths = [
        os.path.join(
            data_dir, exchange_id, "csv", f"{symbol_normalized}_{timeframe}.csv"
        ),
        os.path.join(data_dir, f"{symbol_normalized}_{timeframe}.csv"),
        os.path.join(
            data_dir, "klines", f"{symbol_normalized}_{timeframe}_candles.csv"
        ),
        os.path.join(
            data_dir, "stochrsi", f"stochrsi_{symbol_normalized}_{timeframe}.csv"
        ),
    ]

    result = {
        "file_path": None,
        "exists": False,
        "row_count": 0,
        "time_range": (None, None),
        "has_missing": False,
        "missing_count": 0,
        "file_size_kb": 0,
    }

    for path in possible_paths:
        if os.path.exists(path):
            result["file_path"] = path
            result["exists"] = True
            result["file_size_kb"] = os.path.getsize(path) / 1024

            try:
                df = pd.read_csv(path)
                result["row_count"] = len(df)

                # 检测时间列
                time_col = None
                for col in [
                    "candle_begin_time",
                    "datetime",
                    "candle_begin_time_GMT8",
                    "time",
                ]:
                    if col in df.columns:
                        time_col = col
                        break

                if time_col:
                    df[time_col] = pd.to_datetime(df[time_col])
                    df = df.sort_values(time_col)
                    result["time_range"] = (df[time_col].min(), df[time_col].max())

                    # 检查缺失
                    missing_info = check_time_continuity(df, timeframe, time_col)
                    result["has_missing"] = missing_info["has_missing"]
                    result["missing_count"] = missing_info["missing_count"]
                    result["missing_periods"] = missing_info.get("missing_periods", [])
                    result["completeness_rate"] = missing_info.get(
                        "completeness_rate", 1.0
                    )

            except Exception as e:
                print(f"读取文件 {path} 时出错: {str(e)}")

            break

    return result


def find_data_files(
    symbol: str = None, timeframe: str = None, data_dir: str = "data"
) -> list:
    """
    查找所有匹配的数据文件

    参数:
        symbol: 交易对（可选），如 'BTC-USDT-SWAP'
        timeframe: 时间周期（可选），如 '1D'
        data_dir: 数据目录

    返回:
        文件路径列表
    """
    files = []

    # 搜索模式
    if symbol and timeframe:
        pattern = f"**/{symbol.replace('/', '-')}_{timeframe}*.csv"
    elif symbol:
        pattern = f"**/{symbol.replace('/', '-')}*.csv"
    elif timeframe:
        pattern = f"**/*_{timeframe}*.csv"
    else:
        pattern = "**/*.csv"

    # 排除 stochrsi 分析结果文件（只获取原始K线数据）
    for f in glob.glob(os.path.join(data_dir, pattern), recursive=True):
        # 排除分析结果文件
        if (
            "stochrsi" not in f or f.endswith(f"_{timeframe}.csv")
            if timeframe
            else True
        ):
            if not any(x in f for x in ["_dbl", "_tbl", "_turn"]):
                files.append(f)

    return sorted(files)


def check_time_continuity(
    df: pd.DataFrame, timeframe: str, time_col: str = "candle_begin_time"
) -> dict:
    """
    检查时间连续性

    参数:
        df: 数据DataFrame
        timeframe: 时间周期
        time_col: 时间列名

    返回:
        {
            'has_missing': bool,       # 是否有缺失
            'missing_count': int,      # 缺失数量
            'missing_periods': list,   # 缺失时间段列表
            'completeness_rate': float # 完整度百分比
        }
    """
    result = {
        "has_missing": False,
        "missing_count": 0,
        "missing_periods": [],
        "completeness_rate": 1.0,
    }

    if time_col not in df.columns or len(df) < 2:
        return result

    # 解析时间间隔
    interval_minutes = {
        "1m": 1,
        "5m": 5,
        "15m": 15,
        "30m": 30,
        "1H": 60,
        "2H": 120,
        "4H": 240,
        "1D": 1440,
        "1D": 1440,
        "1W": 10080,
        "1w": 10080,
    }

    # 尝试匹配时间间隔
    minutes = None
    for key, val in interval_minutes.items():
        if timeframe.upper() == key.upper() or timeframe == key:
            minutes = val
            break

    if minutes is None:
        # 尝试解析
        interval_value, interval_unit = parse_timeframe(timeframe)
        if interval_value and interval_unit:
            if interval_unit == "minutes":
                minutes = interval_value
            elif interval_unit == "hours":
                minutes = interval_value * 60
            elif interval_unit == "days":
                minutes = interval_value * 1440

    if minutes is None:
        return result

    expected_interval = timedelta(minutes=minutes)

    # 排序数据
    df = df.sort_values(time_col).reset_index(drop=True)

    # 检测缺失
    missing_periods = []
    total_expected = 0

    for i in range(1, len(df)):
        actual_interval = df[time_col].iloc[i] - df[time_col].iloc[i - 1]
        gap_count = int(actual_interval / expected_interval) - 1

        if gap_count > 0:
            result["has_missing"] = True
            result["missing_count"] += gap_count

            missing_periods.append(
                {
                    "start_time": df[time_col].iloc[i - 1] + expected_interval,
                    "end_time": df[time_col].iloc[i] - expected_interval,
                    "missing_count": gap_count,
                }
            )

        total_expected += int(actual_interval / expected_interval)

    result["missing_periods"] = missing_periods

    # 计算完整度
    if total_expected > 0:
        actual_count = len(df) - 1
        result["completeness_rate"] = actual_count / (
            actual_count + result["missing_count"]
        )

    return result


def check_price_validity(df: pd.DataFrame) -> dict:
    """
    检查价格有效性

    返回:
        {
            'is_valid': bool,
            'anomalies': list,    # 异常点列表
            'errors': list        # 错误列表
        }
    """
    result = {"is_valid": True, "anomalies": [], "errors": []}

    required_cols = ["open", "high", "low", "close"]
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        result["is_valid"] = False
        result["errors"].append(f"缺少必要列: {missing_cols}")
        return result

    for idx, row in df.iterrows():
        # 检查 high >= low
        if row["high"] < row["low"]:
            result["anomalies"].append(
                {
                    "index": idx,
                    "type": "high_low_error",
                    "message": f"high({row['high']}) < low({row['low']})",
                }
            )
            result["is_valid"] = False

        # 检查 open/close 在 high/low 范围内
        if not (row["low"] <= row["open"] <= row["high"]):
            result["anomalies"].append(
                {
                    "index": idx,
                    "type": "open_out_of_range",
                    "message": f"open({row['open']}) 不在 [{row['low']}, {row['high']}] 范围内",
                }
            )

        if not (row["low"] <= row["close"] <= row["high"]):
            result["anomalies"].append(
                {
                    "index": idx,
                    "type": "close_out_of_range",
                    "message": f"close({row['close']}) 不在 [{row['low']}, {row['high']}] 范围内",
                }
            )

        # 检查价格异常跳变（超过20%）
        if idx > 0:
            prev_close = df.iloc[idx - 1]["close"]
            if prev_close > 0:
                change_pct = abs(row["close"] - prev_close) / prev_close * 100
                if change_pct > 20:
                    result["anomalies"].append(
                        {
                            "index": idx,
                            "type": "price_jump",
                            "message": f"价格跳变 {change_pct:.2f}%",
                        }
                    )

    return result


def validate_data(df: pd.DataFrame, timeframe: str, symbol: str = None) -> dict:
    """
    验证K线数据完整性和有效性

    参数:
        df: 数据DataFrame
        timeframe: 时间周期
        symbol: 交易对（可选）

    返回:
        {
            'is_valid': bool,            # 整体是否有效
            'completeness': {...},       # 完整性检查结果
            'quality': {...},            # 质量检查结果
            'time_range': {...},         # 时间范围
            'warnings': list,            # 警告信息
            'errors': list               # 错误信息
        }
    """
    result = {
        "is_valid": True,
        "completeness": {},
        "quality": {},
        "time_range": {},
        "warnings": [],
        "errors": [],
    }

    if df is None or df.empty:
        result["is_valid"] = False
        result["errors"].append("数据为空")
        return result

    # 检测时间列
    time_col = None
    for col in ["candle_begin_time", "datetime", "candle_begin_time_GMT8", "time"]:
        if col in df.columns:
            time_col = col
            break

    if time_col is None:
        result["is_valid"] = False
        result["errors"].append("未找到时间列")
        return result

    # 时间范围
    df[time_col] = pd.to_datetime(df[time_col])
    df = df.sort_values(time_col).reset_index(drop=True)

    result["time_range"] = {
        "start": df[time_col].min(),
        "end": df[time_col].max(),
        "span_days": (df[time_col].max() - df[time_col].min()).days,
    }

    # 完整性检查
    completeness = check_time_continuity(df, timeframe, time_col)
    result["completeness"] = {
        "total_rows": len(df),
        "has_missing": completeness["has_missing"],
        "missing_count": completeness["missing_count"],
        "missing_periods": completeness["missing_periods"],
        "completeness_rate": completeness["completeness_rate"],
    }

    if completeness["has_missing"]:
        result["warnings"].append(
            f"检测到 {completeness['missing_count']} 个缺失数据点"
        )

    # 质量检查
    price_check = check_price_validity(df)
    result["quality"] = {
        "price_anomalies": [
            a for a in price_check["anomalies"] if a["type"] != "price_jump"
        ][:10],
        "price_jumps": [
            a for a in price_check["anomalies"] if a["type"] == "price_jump"
        ][:10],
        "duplicate_rows": df.duplicated(subset=[time_col]).sum(),
        "null_values": df.isnull().sum().to_dict(),
    }

    if not price_check["is_valid"]:
        result["is_valid"] = False
        result["errors"].extend(price_check["errors"])

    if result["quality"]["duplicate_rows"] > 0:
        result["warnings"].append(
            f"发现 {result['quality']['duplicate_rows']} 个重复时间点"
        )

    return result


def smart_get_data(
    symbol: str,
    timeframe: str,
    required_days: int = 90,
    exchange=exchange,
    data_dir: str = "data",
    auto_fill_missing: bool = True,
    verbose: bool = True,
) -> dict:
    """
    智能获取K线数据

    自动检查本地已有数据，计算需要获取的新数据范围，验证数据完整性。

    参数:
        symbol: 交易对，如 'BTC-USDT-SWAP' 或 'BTC/USDT'
        timeframe: 时间周期，如 '1D', '4H', '15m'
        required_days: 需要的数据天数
        exchange: 交易所对象
        data_dir: 数据目录
        auto_fill_missing: 是否自动补全缺失数据
        verbose: 是否打印详细信息

    返回:
        {
            'df': DataFrame,              # 完整数据
            'local_info': dict,           # 本地数据信息
            'fetch_info': dict,           # 获取信息
            'validation': dict,           # 验证报告
            'status': str,                # 状态描述
            'all_timeframes': dict        # 所有周期的数据
        }
    """
    result = {
        "df": None,
        "local_info": None,
        "fetch_info": None,
        "validation": None,
        "status": "pending",
        "all_timeframes": {},
    }

    # 要生成的所有周期
    all_timeframes = ["15m", "4H", "1D", "1W"]

    # 标准化symbol格式（用于API调用）
    # BTC-USDT-SWAP -> BTC/USDT:USDT (OKX swap 格式)
    if "-SWAP" in symbol:
        base = symbol.split("-")[0]  # BTC
        symbol_api = f"{base}/USDT:USDT"
    else:
        symbol_api = symbol.replace("-", "/")

    symbol_file = symbol.replace("/", "-")

    if verbose:
        print(f"\n{'='*60}")
        print(f"智能数据获取: {symbol}")
        print(f"需要 {required_days} 天数据，生成周期: {', '.join(all_timeframes)}")
        print(f"{'='*60}")

    # 1. 检查本地 15m 数据（基础数据）
    local_info = get_local_data_info(symbol, "15m", data_dir, exchange.id)
    result["local_info"] = local_info

    if verbose:
        if local_info["exists"]:
            print(f"\n[本地 15m 数据]")
            print(f"  文件: {local_info['file_path']}")
            print(f"  行数: {local_info['row_count']}")
            print(
                f"  时间范围: {local_info['time_range'][0]} ~ {local_info['time_range'][1]}"
            )
            print(f"  完整度: {local_info.get('completeness_rate', 1.0):.2%}")
            if local_info["has_missing"]:
                print(f"  缺失: {local_info['missing_count']} 个点")
        else:
            print(f"\n[本地 15m 数据] 不存在")

    # 2. 计算需要获取的数据
    now = datetime.now()
    required_start = now - timedelta(days=required_days)

    fetch_info = {
        "need_fetch": False,
        "fetch_start": None,
        "fetch_end": None,
        "fetch_days": 0,
        "reason": "",
    }

    if not local_info["exists"]:
        fetch_info["need_fetch"] = True
        fetch_info["fetch_start"] = required_start
        fetch_info["fetch_end"] = now
        fetch_info["fetch_days"] = required_days
        fetch_info["reason"] = "本地无数据"
    elif local_info["time_range"][1] is None:
        fetch_info["need_fetch"] = True
        fetch_info["fetch_start"] = required_start
        fetch_info["fetch_end"] = now
        fetch_info["fetch_days"] = required_days
        fetch_info["reason"] = "本地数据时间范围异常"
    else:
        local_end = local_info["time_range"][1]
        if isinstance(local_end, str):
            local_end = pd.to_datetime(local_end)
        # 确保 local_end 是 tz-naive
        if hasattr(local_end, "tz_localize") and local_end.tzinfo is not None:
            local_end = local_end.tz_localize(None)

        gap_days = (now - local_end).days

        if gap_days > 0:
            fetch_info["need_fetch"] = True
            fetch_info["fetch_start"] = local_end + timedelta(minutes=1)
            fetch_info["fetch_end"] = now
            fetch_info["fetch_days"] = gap_days + 1
            fetch_info["reason"] = f"本地数据落后 {gap_days} 天"

        local_start = local_info["time_range"][0]
        if isinstance(local_start, str):
            local_start = pd.to_datetime(local_start)
        # 确保 local_start 是 tz-naive
        if hasattr(local_start, "tz_localize") and local_start.tzinfo is not None:
            local_start = local_start.tz_localize(None)

        # 计算本地数据的实际跨度
        local_span_days = (local_end - local_start).days

        # 只有当本地数据跨度不足时才补充历史数据
        # 允许 1 天的容差，避免因时区或时间点差异导致的不必要获取
        if local_start > required_start and local_span_days < required_days - 1:
            extra_days = (local_start - required_start).days
            fetch_info["need_fetch"] = True
            fetch_info["fetch_start"] = required_start
            fetch_info["fetch_days"] = max(
                fetch_info["fetch_days"], extra_days + (now - local_start).days
            )
            fetch_info["reason"] += f", 需补充历史 {extra_days} 天"

        # 检查中间是否有缺失数据
        if local_info["has_missing"] and auto_fill_missing:
            fetch_info["need_fetch"] = True
            fetch_info["has_gaps"] = True
            fetch_info["missing_periods"] = local_info.get("missing_periods", [])
            fetch_info["reason"] += f', 中间缺失 {local_info["missing_count"]} 个点'

    result["fetch_info"] = fetch_info

    if verbose:
        print(f"\n[获取计划]")
        if fetch_info["need_fetch"]:
            print(f"  需要获取: {fetch_info['fetch_days']} 天 15m 数据")
            print(
                f"  时间范围: {fetch_info['fetch_start']} ~ {fetch_info['fetch_end']}"
            )
            print(f"  原因: {fetch_info['reason']}")
            if fetch_info.get("has_gaps"):
                print(f"  中间缺失时段:")
                for p in fetch_info.get("missing_periods", [])[:3]:
                    print(
                        f"    {p['start_time']} ~ {p['end_time']} ({p['missing_count']} 条)"
                    )
        else:
            print(f"  本地数据已满足需求，将获取最新 10 根 K 线更新")

    # 3. 获取 15m 数据
    df_15m = None

    # 始终获取数据（更新最新K线或补充缺失）
    if True:
        if verbose:
            print(f"\n[开始获取 15m 数据]...")

        all_new_data = []

        try:
            # 如果有中间缺失，需要分别获取缺失时段
            if fetch_info.get("has_gaps") and fetch_info.get("missing_periods"):
                if verbose:
                    print(f"  检测到中间缺失，将分别获取缺失时段数据...")

                for period in fetch_info["missing_periods"]:
                    gap_start = period["start_time"]
                    gap_end = period["end_time"]

                    # 计算需要获取的天数
                    if isinstance(gap_start, str):
                        gap_start = pd.to_datetime(gap_start)
                    if isinstance(gap_end, str):
                        gap_end = pd.to_datetime(gap_end)

                    gap_days = (gap_end - gap_start).days + 1

                    # 时区修正：将开始时间提前 9 小时，以处理 UTC+8 时区问题
                    gap_start_adjusted = gap_start - pd.Timedelta(hours=9)
                    gap_days_adjusted = gap_days + 1

                    if verbose:
                        print(f"  获取缺失时段: {gap_start} ~ {gap_end}")
                        print(
                            f"    时区修正后: {gap_start_adjusted} ({gap_days_adjusted} 天)"
                        )

                    gap_df = get_kline(
                        start_time=str(gap_start_adjusted),
                        exchange=exchange,
                        symbol=symbol_api,
                        time_interval="15m",
                        days=gap_days_adjusted,
                    )

                    if gap_df is not None and not gap_df.empty:
                        all_new_data.append(gap_df)
                        if verbose:
                            print(f"    获取 {len(gap_df)} 条数据")

            # 如果需要获取历史或最新数据
            if fetch_info["fetch_start"] is not None and fetch_info["fetch_days"] > 0:
                new_df = get_kline(
                    start_time=str(fetch_info["fetch_start"]),
                    exchange=exchange,
                    symbol=symbol_api,
                    time_interval="15m",  # 始终获取 15m
                    days=fetch_info["fetch_days"] + 1,
                )

                if new_df is not None and not new_df.empty:
                    all_new_data.append(new_df)
                    if verbose:
                        print(f"  成功获取 {len(new_df)} 条 15m 数据")
            elif not fetch_info["need_fetch"]:
                # 即使数据已最新，也获取最新 10 根 K 线更新
                if verbose:
                    print(f"  获取最新 10 根 K 线更新...")
                latest_df = get_kline(
                    exchange=exchange,
                    symbol=symbol_api,
                    time_interval="15m",
                    limit=10,  # 直接获取最新 10 根
                )
                print(latest_df)
                if latest_df is not None and not latest_df.empty:
                    all_new_data.append(latest_df)
                    if verbose:
                        print(f"  成功获取最新 {len(latest_df)} 条 15m 数据")

            # 合并所有新获取的数据
            if all_new_data:
                combined_new = (
                    pd.concat(all_new_data, ignore_index=True)
                    if len(all_new_data) > 1
                    else all_new_data[0]
                )

                # 合并数据
                if local_info["exists"] and local_info["file_path"]:
                    try:
                        old_df = pd.read_csv(local_info["file_path"])
                        time_col = "candle_begin_time"
                        if time_col in old_df.columns:
                            old_df[time_col] = pd.to_datetime(old_df[time_col])
                            df_15m = pd.concat(
                                [old_df, combined_new], ignore_index=True
                            )
                            df_15m.drop_duplicates(
                                subset=[time_col], keep="last", inplace=True
                            )
                            df_15m.sort_values(time_col, inplace=True)
                            df_15m.reset_index(drop=True, inplace=True)
                            if verbose:
                                print(f"  合并后总数据: {len(df_15m)} 条")
                        else:
                            df_15m = combined_new
                    except Exception as e:
                        if verbose:
                            print(f"  合并数据时出错: {str(e)}")
                        df_15m = combined_new
                else:
                    df_15m = combined_new
            else:
                if verbose:
                    print(f"  未获取到数据")
                if local_info["exists"]:
                    df_15m = pd.read_csv(local_info["file_path"])

        except Exception as e:
            if verbose:
                print(f"  获取数据出错: {str(e)}")
            if local_info["exists"]:
                df_15m = pd.read_csv(local_info["file_path"])
    else:
        if local_info["exists"]:
            df_15m = pd.read_csv(local_info["file_path"])

    # 4. 生成并保存所有周期的数据
    if df_15m is not None and not df_15m.empty:
        if verbose:
            print(f"\n[生成多周期数据]...")

        for tf in all_timeframes:
            if tf == "15m":
                df_tf = df_15m
            else:
                df_tf = resample_kline(df_15m, tf)

            if df_tf is not None and not df_tf.empty:
                # 保存数据到 data/{exchange.id}/csv/ 目录
                csv_dir = os.path.join(data_dir, exchange.id, "csv")
                os.makedirs(csv_dir, exist_ok=True)
                save_path = os.path.join(csv_dir, f"{symbol_file}_{tf}.csv")
                df_tf.to_csv(save_path, index=False)

                result["all_timeframes"][tf] = {
                    "df": df_tf,
                    "rows": len(df_tf),
                    "path": save_path,
                }

                if verbose:
                    print(f"  {tf}: {len(df_tf)} 条 -> {save_path}")

        # 设置主 df 为请求的周期
        if timeframe in result["all_timeframes"]:
            result["df"] = result["all_timeframes"][timeframe]["df"]
        else:
            result["df"] = df_15m

    # 5. 验证数据
    if result["df"] is not None and not result["df"].empty:
        if verbose:
            print(f"\n[数据验证 - {timeframe}]...")

        validation = validate_data(result["df"], timeframe, symbol)
        result["validation"] = validation

        if verbose:
            print(f"  总行数: {validation['completeness']['total_rows']}")
            print(
                f"  时间范围: {validation['time_range']['start']} ~ {validation['time_range']['end']}"
            )
            print(f"  跨度: {validation['time_range']['span_days']} 天")
            print(f"  完整度: {validation['completeness']['completeness_rate']:.2%}")

            if validation["warnings"]:
                print(f"  警告: {validation['warnings']}")
            if validation["errors"]:
                print(f"  错误: {validation['errors']}")

        # 自动补全 15m 缺失数据
        if (
            auto_fill_missing
            and timeframe == "15m"
            and validation["completeness"]["has_missing"]
        ):
            if verbose:
                print(f"\n[自动补全缺失数据]...")

            csv_dir = os.path.join(data_dir, exchange.id, "csv")
            save_path = os.path.join(csv_dir, f"{symbol_file}_15m.csv")
            df_15m = fill_missing_data_api(save_path, exchange)
            if df_15m is not None:
                # 重新生成所有周期
                for tf in all_timeframes:
                    if tf == "15m":
                        df_tf = df_15m
                    else:
                        df_tf = resample_kline(df_15m, tf)

                    if df_tf is not None:
                        save_path = os.path.join(csv_dir, f"{symbol_file}_{tf}.csv")
                        df_tf.to_csv(save_path, index=False)
                        result["all_timeframes"][tf] = {
                            "df": df_tf,
                            "rows": len(df_tf),
                            "path": save_path,
                        }

                result["df"] = (
                    result["all_timeframes"].get(timeframe, {}).get("df", df_15m)
                )
                result["validation"] = validate_data(result["df"], timeframe, symbol)

    # 设置状态
    if result["df"] is not None and not result["df"].empty:
        if result["validation"] and result["validation"]["is_valid"]:
            result["status"] = "success"
        else:
            result["status"] = "success_with_warnings"
    else:
        result["status"] = "failed"

    if verbose:
        print(f"\n[完成] 状态: {result['status']}")
        print(f"{'='*60}\n")

    return result


def batch_smart_get_data(
    symbols: list,
    required_days: int = 90,
    exchange=exchange,
    data_dir: str = "data",
    verbose: bool = True,
) -> dict:
    """
    批量智能获取多个币种的数据

    每个币种获取一次 15m 数据，自动生成 15m, 4H, 1D, 1W 四个周期

    返回:
        {
            'results': dict,      # 各币种的获取结果
            'summary': dict       # 汇总信息
        }
    """
    results = {}
    summary = {
        "total": len(symbols),
        "success": 0,
        "failed": 0,
        "warnings": 0,
        "files_generated": 0,
    }

    for symbol in symbols:
        key = symbol
        result = smart_get_data(
            symbol=symbol,
            timeframe="1D",  # 默认返回 1D 数据
            required_days=required_days,
            exchange=exchange,
            data_dir=data_dir,
            verbose=verbose,
        )
        results[key] = result

        # 统计生成的文件数
        if "all_timeframes" in result:
            summary["files_generated"] += len(result["all_timeframes"])

        if result["status"] == "success":
            summary["success"] += 1
        elif result["status"] == "success_with_warnings":
            summary["success"] += 1
            summary["warnings"] += 1
        else:
            summary["failed"] += 1

    return {"results": results, "summary": summary}


if __name__ == "__main__":
    # ==================== 智能数据获取 ====================

    # 批量获取多个币种的 90 天数据，自动生成 15m, 4H, 1D, 1W 四个周期
    result = batch_smart_get_data(
        symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"],
        required_days=220,
        verbose=True,
    )
    print(f"\n{'='*60}")
    print(
        f"汇总: 成功 {result['summary']['success']}, 失败 {result['summary']['failed']}, 警告 {result['summary']['warnings']}"
    )
    print(f"生成文件数: {result['summary']['files_generated']}")
    print(f"{'='*60}")

    # 示例3: 验证现有数据
    # local_info = get_local_data_info("BTC-USDT-SWAP", "1D")
    # if local_info['exists']:
    #     df = pd.read_csv(local_info['file_path'])
    #     validation = validate_data(df, "1D", "BTC-USDT-SWAP")
    #     print(f"数据完整度: {validation['completeness']['completeness_rate']:.2%}")
    #     print(f"时间范围: {validation['time_range']['start']} ~ {validation['time_range']['end']}")

    # ==================== 原有功能 ====================
    # 1.获取制定币种数据（原有方式）
    # symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    # time_interval = "5m"
    # for symbol in symbols:
    #     last_datetime = get_last_datetime(exchange, symbol, time_interval)
    #     last_datetime = str(datetime.strptime(
    #         last_datetime, "%Y-%m-%d %H:%M:%S"
    #     ) - timedelta(days=6))
    #     print(f"{symbol} 开始时间：{last_datetime}")
    #     get_data(last_datetime, exchange, symbol, time_interval)

    # 2.检查处理缺失数据
    # fill_missing_data_api('./data/okx/csv/BTC-USDT_5m.csv')
    # fill_missing_batch_api('./data/okx/csv', pattern='*_5m.csv')

    # 3.验证各币种数据是否正确
    # files = find_data_files()
    # for f in files:
    #     df = pd.read_csv(f)
    #     validation = validate_data(df, "1D")
    #     print(f"{f}: {validation['status']}")
