#!/usr/bin/env python3
"""
最新K线数据获取模块

基于CCXT 4.0.85库封装，提供简洁统一的K线数据获取接口
"""

import ccxt
import pandas as pd
import time
from typing import Optional
from config_constants import OKEX_READONLY_CONFIG


class KlineFetcher:
    """K线数据获取器"""

    def __init__(self, exchange_config: Optional[dict] = None):
        """
        初始化K线获取器

        Args:
            exchange_config: 交易所配置字典，如为None则使用默认OKX配置
        """
        if exchange_config is None:
            exchange_config = OKEX_READONLY_CONFIG

        self.exchange = ccxt.okx(exchange_config)
        self.exchange_config = exchange_config

    def get_klines(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 200,
        retries: int = 3,
        retry_delay: float = 2.0
    ) -> Optional[pd.DataFrame]:
        """
        获取最新K线数据

        Args:
            symbol: 交易对符号 (如 'SOL-USDT-SWAP', 'BTC/USDT')
            timeframe: 时间周期 (如 '1W', '1D', '4H', '15m', '1h', '5m')
            limit: K线数量，okx接口一次性最多返回300 (默认200)
            retries: 重试次数 (默认3)
            retry_delay: 重试间隔秒数 (默认2.0)

        Returns:
            pandas.DataFrame: 包含OHLCV数据的DataFrame，失败时返回None
        """
        for attempt in range(retries):
            try:
                print(f"正在获取 {symbol} {timeframe} K线数据 (尝试 {attempt + 1}/{retries})...")

                # 使用CCXT标准API获取数据
                if '/' in symbol:
                    # 标准格式，如 'BTC/USDT'
                    ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
                else:
                    # OKX格式，如 'BTC-USDT-SWAP'
                    ohlcv = self.exchange.publicGetMarketCandles({
                        'instId': symbol,
                        'bar': timeframe,
                        'limit': limit
                    })['data']

                if not ohlcv:
                    print(f"⚠️  未获取到 {symbol} {timeframe} 的数据")
                    return None

                # 转换为DataFrame - OKX返回9列数据
                if isinstance(ohlcv[0], list) and len(ohlcv[0]) == 9:
                    # OKX原生API格式：[timestamp, open, high, low, close, volume, volume_ccy, volume_ccy_quote, confirm]
                    df = pd.DataFrame(ohlcv, columns=[
                        'timestamp', 'open', 'high', 'low', 'close',
                        'volume', 'volume_ccy', 'volume_ccy_quote', 'confirm'
                    ])
                    # 只使用标准的6列
                    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
                else:
                    # 标准CCXT API格式
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

                # 转换时间戳
                df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
                df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

                # 转换数据类型
                numeric_columns = ['open', 'high', 'low', 'close', 'volume']
                for col in numeric_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

                # 时区转换：UTC -> 北京时间
                df['datetime_utc'] = df['datetime'].dt.tz_localize('UTC')
                df['datetime_beijing'] = df['datetime_utc'].dt.tz_convert('Asia/Shanghai')
                # 去掉时区信息，只保留时间字符串
                df['datetime_beijing'] = df['datetime_beijing'].dt.tz_localize(None)
                # df['datetime_beijing_str'] = df['datetime_beijing'].dt.strftime('%Y-%m-%d %H:%M:%S')

                # 重新排列列顺序
                df = df[[
                    # 'datetime_beijing_str',
                    'datetime_beijing', 'open', 'high', 'low', 'close', 'volume'
                ]]

                # 重命名列
                df.rename(columns={
                    # 'datetime_beijing_str': 'time',
                    'datetime_beijing': 'datetime'
                }, inplace=True)
                # 时间排序
                df.sort_values(by='datetime', inplace=True)

                print(f"✅ 成功获取 {len(df)} 根 {symbol} {timeframe} K线数据")
                return df

            except Exception as e:
                error_msg = str(e)
                print(f"❌ 获取 {symbol} {timeframe} 失败 (尝试 {attempt + 1}/{retries}): {error_msg}")

                if attempt < retries - 1:
                    print(f"等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
                else:
                    print(f"获取 {symbol} {timeframe} 失败次数过多，已放弃")
                    return None

        return None

    def get_latest_price(self, symbol: str) -> Optional[float]:
        """
        获取最新价格

        Args:
            symbol: 交易对符号

        Returns:
            float: 最新价格，失败时返回None
        """
        try:
            # 使用1分钟K线获取最新价格
            df = self.get_klines(symbol, '1m', limit=1)
            if df is not None and len(df) > 0:
                return float(df['close'].iloc[-1])
        except Exception as e:
            print(f"获取 {symbol} 最新价格失败: {str(e)}")

        return None

    def get_multiple_symbols(
        self,
        symbols: list,
        timeframe: str,
        limit: int = 200
    ) -> dict:
        """
        批量获取多个交易对的K线数据

        Args:
            symbols: 交易对符号列表
            timeframe: 时间周期
            limit: K线数量

        Returns:
            dict: {symbol: DataFrame} 格式的字典
        """
        results = {}

        for symbol in symbols:
            print(f"\n📊 处理交易对: {symbol}")
            df = self.get_klines(symbol, timeframe, limit)
            if df is not None:
                results[symbol] = df
            else:
                print(f"⚠️  跳过 {symbol}，获取失败")

        return results

    def get_multiple_timeframes(
        self,
        symbol: str,
        timeframes: list,
        limit: int = 200
    ) -> dict:
        """
        获取单个交易对的多个时间周期数据

        Args:
            symbol: 交易对符号
            timeframes: 时间周期列表
            limit: 每个周期的K线数量

        Returns:
            dict: {timeframe: DataFrame} 格式的字典
        """
        results = {}

        for timeframe in timeframes:
            print(f"\n📊 处理时间周期: {timeframe}")
            df = self.get_klines(symbol, timeframe, limit)
            if df is not None:
                results[timeframe] = df
            else:
                print(f"⚠️  跳过 {timeframe}，获取失败")

        return results

    def save_to_csv(self, df: pd.DataFrame, filepath: str) -> bool:
        """
        保存DataFrame到CSV文件

        Args:
            df: 要保存的DataFrame
            filepath: 文件路径

        Returns:
            bool: 保存是否成功
        """
        try:
            df.to_csv(filepath, index=False)
            print(f"💾 数据已保存到: {filepath}")
            return True
        except Exception as e:
            print(f"❌ 保存文件失败: {str(e)}")
            return False

    def get_supported_timeframes(self) -> list:
        """
        获取支持的时间周期列表

        Returns:
            list: 支持的时间周期列表
        """
        return [
            '1W',  # 周线
            '1D',  # 日线
            '4H',  # 4小时
            '2H',  # 2小时
            '1H',  # 1小时
            '30m', # 30分钟
            '15m', # 15分钟
            '5m',  # 5分钟
            '3m',  # 3分钟
            '1m'   # 1分钟
        ]


# 便捷函数
def get_klines(symbol: str, timeframe: str, limit: int = 200) -> Optional[pd.DataFrame]:
    """
    快捷获取K线数据

    Args:
        symbol: 交易对符号
        timeframe: 时间周期
        limit: K线数量

    Returns:
        pandas.DataFrame: K线数据
    """
    fetcher = KlineFetcher()
    return fetcher.get_klines(symbol, timeframe, limit)


def get_latest_price(symbol: str) -> Optional[float]:
    """
    快捷获取最新价格

    Args:
        symbol: 交易对符号

    Returns:
        float: 最新价格
    """
    fetcher = KlineFetcher()
    return fetcher.get_latest_price(symbol)


# 示例用法
if __name__ == "__main__":
    # 创建K线获取器
    fetcher = KlineFetcher()

    print("=== K线数据获取测试 ===\n")

    # 示例1: 获取单个交易对的K线数据
    print("示例1: 获取SOL-USDT-SWAP的日线数据")
    df = fetcher.get_klines('SOL-USDT-SWAP', '1D', limit=10)
    if df is not None:
        print(f"数据预览:")
        print(df.head())
        print(f"\n数据统计:")
        print(df[['open', 'high', 'low', 'close', 'volume']].describe())

    print("\n" + "="*50 + "\n")

    # 示例2: 获取多个时间周期
    print("示例2: 获取BTC-USDT-SWAP的多个时间周期数据")
    timeframes = ['1D', '4H', '1H']
    multi_tf_data = fetcher.get_multiple_timeframes('BTC-USDT-SWAP', timeframes, limit=5)

    for tf, data in multi_tf_data.items():
        print(f"\n{tf} 数据:")
        print(f"时间范围: {data['time'].iloc[0]} 至 {data['time'].iloc[-1]}")
        print(f"最新价格: {data['close'].iloc[-1]}")

    print("\n" + "="*50 + "\n")
    exit()

    # 示例3: 获取多个交易对
    print("示例3: 获取多个交易对的15分钟数据")
    symbols = ['BTC-USDT-SWAP', 'ETH-USDT-SWAP', 'SOL-USDT-SWAP']
    multi_symbol_data = fetcher.get_multiple_symbols(symbols, '15m', limit=3)

    for symbol, data in multi_symbol_data.items():
        latest_price = data['close'].iloc[-1]
        latest_volume = data['volume'].iloc[-1]
        print(f"{symbol}: 最新价格 {latest_price}, 成交量 {latest_volume}")

    print("\n" + "="*50 + "\n")

    # 示例4: 获取最新价格
    print("示例4: 获取最新价格")
    symbols_to_check = ['SOL-USDT-SWAP', 'BTC-USDT-SWAP']

    for symbol in symbols_to_check:
        latest_price = fetcher.get_latest_price(symbol)
        if latest_price:
            print(f"{symbol}: ${latest_price}")
        else:
            print(f"{symbol}: 获取失败")

    print("\n=== 测试完成 ===")