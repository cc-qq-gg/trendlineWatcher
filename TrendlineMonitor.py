"""
趋势线监测引擎 - 实时监测突破/跌破信号
复用现有的K线数据获取和趋势线计算逻辑
"""

import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import ccxt
from TrendlineManager import TrendlineManager
from Function import fetch_okex_symbol_history_candle_data
from Config import *
from config_constants import OKEX_READONLY_CONFIG
from Signals import define_trendline, monitor_breakout
import os

# =交易所配置
OKEX_CONFIG = OKEX_READONLY_CONFIG
exchange = ccxt.okx(OKEX_CONFIG)


class TrendlineMonitor:
    """趋势线监测引擎"""

    def __init__(self, exchange_config: Dict = None, data_dir: str = "data"):
        """初始化监测引擎"""
        self.manager = TrendlineManager(data_dir)
        self.exchange_config = exchange_config or OKEX_CONFIG
        self.exchange = exchange
        self.monitoring = False
        self.monitor_thread = None
        self.candle_cache = {}  # 缓存K线数据

    def start_monitoring(
        self,
        symbols: List[str],
        time_interval: str = "15m",
        max_candles: int = 1000,
        check_interval: int = 30,
    ):
        """启动监测"""
        if self.monitoring:
            print("监测已在运行中")
            return

        self.monitoring = True
        self.symbols = symbols
        self.time_interval = time_interval
        self.max_candles = max_candles
        self.check_interval = check_interval

        # 初始化K线数据
        self._init_candle_data()
        self._monitor_loop()

        # 启动监测线程
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

        print(f"趋势线监测已启动 - 交易对: {symbols}, 时间间隔: {time_interval}")

    def stop_monitoring(self):
        """停止监测"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
        print("趋势线监测已停止")

    def init_cache(self, symbol):
        # 获取历史K线数据
        df = fetch_okex_symbol_history_candle_data(
            self.exchange, symbol, self.time_interval, self.max_candles
        )
        if not df.empty:
            # 时间倒序排序
            df.sort_values(by="candle_begin_time_GMT8", ascending=True, inplace=True)
            df.reset_index(drop=True, inplace=True)
            self.candle_cache[symbol] = df
            if not os.path.exists("./data/klines"):
                os.makedirs("./data/klines")
            df.to_csv(
                f"./data/klines/{symbol}_{self.time_interval}_candles.csv",
                index=False,
            )
            print(f"{symbol}: 已加载 {len(df)} 根K线")
        else:
            print(f"{symbol}: 警告 - 未获取到K线数据")

    def _init_candle_data(self):
        """初始化K线数据"""
        print("正在初始化K线数据...")
        for symbol in self.symbols:
            try:
                self.init_cache(symbol)
            except Exception as e:
                print(f"{symbol}: 获取K线数据失败 - {e}")

    def _init_new_symbols(self, new_symbols: List[str]):
        """为新增的symbol初始化历史数据"""
        print(f"正在初始化新增交易对的历史数据: {new_symbols}")
        for symbol in new_symbols:
            try:
                self.init_cache(symbol=symbol)  # 保存到文件
            except Exception as e:
                print(f"{symbol}: 获取K线数据失败（新增） - {e}")

    def _monitor_loop(self):
        """监测循环"""
        print("开始监测循环...")
        while self.monitoring:
            try:
                # 每次循环都重新检查活跃趋势线，实现动态更新
                active_trendlines = self.manager.get_active_trendlines()
                active_symbols = list(set([tl["symbol"] for tl in active_trendlines]))

                # 如果没有活跃趋势线，等待而不是停止监测
                if not active_symbols:
                    print("没有活跃趋势线，等待...")
                    time.sleep(self.check_interval)
                    continue

                # 动态更新监控的symbols列表
                old_symbols = set(self.symbols) if hasattr(self, "symbols") else set()
                new_symbols = set(active_symbols)

                # 检查symbols变化
                if old_symbols != new_symbols:
                    added_symbols = new_symbols - old_symbols
                    removed_symbols = old_symbols - new_symbols

                    if added_symbols:
                        print(f"新增监控交易对: {list(added_symbols)}")
                        # 为新增的symbol初始化历史数据
                        self._init_new_symbols(list(added_symbols))
                    if removed_symbols:
                        print(f"移除监控交易对: {list(removed_symbols)}")
                        # 清理不再需要的缓存数据
                        for symbol in removed_symbols:
                            if symbol in self.candle_cache:
                                del self.candle_cache[symbol]

                self.symbols = active_symbols

                # 更新K线数据
                self._update_candle_data()

                # 检查所有活跃趋势线
                self._check_all_trendlines()

                # 等待下一次检查
                time.sleep(self.check_interval)

            except Exception as e:
                print(f"监测循环出错: {e}")
                time.sleep(self.check_interval)

    def _update_candle_data(self):
        """更新K线数据"""
        for symbol in self.symbols:
            try:
                # 获取最新的几根K线
                new_df = fetch_okex_symbol_history_candle_data(
                    self.exchange, symbol, self.time_interval, 100
                )

                if not new_df.empty:
                    # 合并到缓存
                    if symbol in self.candle_cache:
                        old_df = self.candle_cache[symbol]
                        combined_df = pd.concat([old_df, new_df], ignore_index=True)
                        combined_df.drop_duplicates(
                            subset=["candle_begin_time_GMT8"], keep="last", inplace=True
                        )
                        combined_df.sort_values(
                            by="candle_begin_time_GMT8", ascending=True, inplace=True
                        )
                        combined_df.reset_index(drop=True, inplace=True)
                        # 检查/data/klines 目录是否存在，不存在则创建
                        if not os.path.exists("./data/klines"):
                            os.makedirs("./data/klines")
                        combined_df.to_csv(
                            f"./data/klines/{symbol}_{self.time_interval}_candles.csv",
                            index=False,
                        )
                        combined_df = combined_df.iloc[-self.max_candles :]

                        print(f"{symbol}: 已更新 {len(new_df)} 根K线")
                        self.candle_cache[symbol] = combined_df
                    else:
                        self.candle_cache[symbol] = new_df

            except Exception as e:
                print(f"更新 {symbol} K线数据失败: {e}")

    def _check_all_trendlines(self):
        """检查所有趋势线的突破信号"""
        # 获取所有活跃趋势线
        active_trendlines = self.manager.get_active_trendlines()

        for trendline in active_trendlines:
            symbol = trendline["symbol"]
            trendline_id = trendline["id"]

            # 检查是否有对应的K线数据
            if symbol not in self.candle_cache or self.candle_cache[symbol].empty:
                continue

            try:
                # 检查突破信号
                signal = self.manager.check_breakout_signal(
                    trendline_id, self.candle_cache[symbol]
                )

                if signal is not None:
                    self._handle_breakout_signal(trendline, signal)

            except Exception as e:
                print(f"检查趋势线 {trendline_id} 失败: {e}")

    def _handle_breakout_signal(self, trendline: Dict, signal: int):
        """处理突破信号"""
        symbol = trendline["symbol"]
        direction = trendline["direction"]
        trendline_id = trendline["id"]

        signal_text = "多头突破" if signal == 1 else "空头跌破"
        direction_text = "多头" if direction == 1 else "空头"

        message = f"""
🚨 {symbol},{signal_text},{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

        print(message)

        # 这里可以集成钉钉通知
        try:
            from Function import send_dingding_msg

            send_dingding_msg(message)
        except:
            pass

        # 暂停该趋势线监测（防止重复提醒）
        self.manager.update_trendline(trendline_id, status="paused")

    def get_monitoring_status(self) -> Dict:
        """获取监测状态"""
        return {
            "monitoring": self.monitoring,
            "symbols": self.symbols,
            "time_interval": self.time_interval,
            "active_trendlines_count": len(self.manager.get_active_trendlines()),
            "candle_cache_status": {
                symbol: len(df) if not df.empty else 0
                for symbol, df in self.candle_cache.items()
            },
        }

    def check_trendline_now(self, trendline_id: str) -> Optional[int]:
        """立即检查指定趋势线"""
        trendline = self.manager.get_trendline(trendline_id)
        if not trendline:
            return None

        symbol = trendline["symbol"]
        if symbol not in self.candle_cache or self.candle_cache[symbol].empty:
            return None

        try:
            return self.manager.check_breakout_signal(
                trendline_id, self.candle_cache[symbol]
            )
        except Exception as e:
            print(f"检查趋势线,check_trendline_now {trendline_id} 失败: {e}")
            return None

    def check_trendline_breakout_detailed(self, trendline_id: str) -> Optional[Dict]:
        """详细检查趋势线突破，返回前端需要的数据"""
        trendline = self.manager.get_trendline(trendline_id)
        if not trendline:
            return None

        symbol = trendline["symbol"]
        if symbol not in self.candle_cache or self.candle_cache[symbol].empty:
            # 如果没有缓存数据，尝试获取最新数据
            try:
                df = fetch_okex_symbol_history_candle_data(
                    self.exchange, symbol, "15m", 100
                )
                if df.empty:
                    return None
            except Exception as e:
                print(f"获取 {symbol} 数据失败: {e}")
                return None
        else:
            df = self.candle_cache[symbol].copy()

        try:
            # 解析趋势线数据
            start_time = trendline["start_time"]
            end_time = trendline["end_time"]
            start_price = float(trendline["start_price"])
            end_price = float(trendline["end_price"])

            # 使用Signals.py中的方法计算趋势线
            trendline_values = define_trendline(
                df, [start_time, start_price], [end_time, end_price]
            )

            # 使用Signals.py中的方法检测突破
            breakout_signal = monitor_breakout(df, trendline_values)

            # 获取当前价格和趋势线值
            current_price = df["close"].iloc[-1]
            current_trendline_value = (
                trendline_values.iloc[-1]
                if not pd.isna(trendline_values.iloc[-1])
                else None
            )

            # 获取前一个数据点用于判断突破
            prev_price = df["close"].iloc[-2] if len(df) >= 2 else None
            prev_trendline_value = (
                trendline_values.iloc[-2]
                if len(trendline_values) >= 2 and not pd.isna(trendline_values.iloc[-2])
                else None
            )

            # 计算价格偏离度
            price_deviation = None
            if current_trendline_value is not None:
                price_deviation = (
                    (current_price - current_trendline_value) / current_trendline_value
                ) * 100

            result = {
                "trendline_id": trendline_id,
                "trendline_name": trendline["name"],
                "symbol": symbol,
                "direction": trendline["direction"],
                "breakout_signal": breakout_signal,
                "current_price": current_price,
                "trendline_value": current_trendline_value,
                "price_deviation_percent": price_deviation,
                "prev_price": prev_price,
                "prev_trendline_value": prev_trendline_value,
                "check_time": datetime.now().isoformat(),
                "data_points": len(df),
                "trendline_start": {"time": start_time, "price": start_price},
                "trendline_end": {"time": end_time, "price": end_price},
            }

            return result

        except Exception as e:
            print(f"详细检查趋势线 {trendline_id} 失败: {e}")
            import traceback

            traceback.print_exc()
            return None

    def get_trendline_data(self, trendline_id: str) -> Optional[Dict]:
        """获取趋势线数据（包含K线和趋势线值）"""
        trendline = self.manager.get_trendline(trendline_id)
        if not trendline:
            return None

        symbol = trendline["symbol"]

        # 如果没有缓存数据，尝试获取最新数据
        if symbol not in self.candle_cache or self.candle_cache[symbol].empty:
            try:
                df = fetch_okex_symbol_history_candle_data(
                    self.exchange, symbol, "15m", 2000
                )
                if df.empty:
                    return None
            except Exception as e:
                print(f"获取 {symbol} 数据失败: {e}")
                return None
        else:
            df = self.candle_cache[symbol].copy()

        try:
            # 解析趋势线数据
            start_time = trendline["start_time"]
            end_time = trendline["end_time"]
            start_price = float(trendline["start_price"])
            end_price = float(trendline["end_price"])

            # 使用Signals.py中的方法计算趋势线
            trendline_values = define_trendline(
                df, [start_time, start_price], [end_time, end_price]
            )

            # 转换K线数据为前端格式
            candle_data = []
            for _, row in df.iterrows():
                try:
                    # 转换时间为UTC时间戳（秒）
                    dt = pd.to_datetime(row["candle_begin_time_GMT8"])
                    if hasattr(dt, "tz") and dt.tz is not None:
                        dt_utc = dt.tz_convert("UTC")
                        timestamp = int(dt_utc.timestamp())
                    else:
                        dt_utc = dt - pd.Timedelta(hours=8)
                        timestamp = int(dt_utc.timestamp())

                    candle_data.append(
                        {
                            "time": timestamp,
                            "open": float(row["open"]),
                            "high": float(row["high"]),
                            "low": float(row["low"]),
                            "close": float(row["close"]),
                        }
                    )
                except Exception as e:
                    continue

            # 生成趋势线数据点（用于图表绘制）
            trendline_chart_data = []
            for i, value in enumerate(trendline_values):
                if not pd.isna(value):
                    try:
                        # 获取对应的时间戳
                        dt = pd.to_datetime(df.iloc[i]["candle_begin_time_GMT8"])
                        if hasattr(dt, "tz") and dt.tz is not None:
                            dt_utc = dt.tz_convert("UTC")
                            timestamp = int(dt_utc.timestamp())
                        else:
                            dt_utc = dt - pd.Timedelta(hours=8)
                            timestamp = int(dt_utc.timestamp())

                        trendline_chart_data.append(
                            {"time": timestamp, "value": float(value)}
                        )
                    except Exception as e:
                        continue

            # 获取最新状态
            latest_signal = self.check_trendline_now(trendline_id)

            return {
                "trendline_info": trendline,
                "candle_data": candle_data,
                "trendline_values": trendline_chart_data,
                "latest_signal": latest_signal,
                "data_summary": {
                    "candle_count": len(candle_data),
                    "trendline_points": len(trendline_chart_data),
                    "time_range": {
                        "start": candle_data[0]["time"] if candle_data else None,
                        "end": candle_data[-1]["time"] if candle_data else None,
                    },
                },
            }
        except Exception as e:
            print(f"获取趋势线 {trendline_id} 数据失败: {e}")
            import traceback

            traceback.print_exc()
            return None

    def refresh_candle_data(
        self, symbol: str = None, time_interval: str = None, limit: int = None
    ):
        """手动刷新K线数据"""
        symbols = [symbol] if symbol else self.symbols
        interval = time_interval or self.time_interval
        max_candles = limit or self.max_candles

        for s in symbols:
            try:
                # 获取最新的K线数据
                df = fetch_okex_symbol_history_candle_data(
                    self.exchange, s, interval, max_candles
                )
                if not df.empty:
                    self.candle_cache[s] = df
                    print(f"{s}: K线数据已刷新，共 {len(df)} 根")
                else:
                    print(f"{s}: 未获取到K线数据")
            except Exception as e:
                print(f"刷新 {s} K线数据失败: {e}")

    def get_latest_candle_data(
        self, symbol: str, limit: int = 100
    ) -> Optional[pd.DataFrame]:
        """获取最新的K线数据（用于前端API）"""
        try:
            # 优先从缓存获取
            if symbol in self.candle_cache and not self.candle_cache[symbol].empty:
                df = self.candle_cache[symbol].tail(limit).copy()
            else:
                # 如果没有缓存，直接获取
                df = fetch_okex_symbol_history_candle_data(
                    self.exchange, symbol, "15m", limit
                )
                if df.empty:
                    return None

            return df
        except Exception as e:
            print(f"获取 {symbol} 最新K线数据失败: {e}")
            return None

    def batch_check_trendlines(self, symbol: str = None) -> Dict[str, Dict]:
        """批量检查趋势线突破状态"""
        try:
            # 获取要检查的趋势线
            if symbol:
                active_trendlines = [
                    tl
                    for tl in self.manager.get_active_trendlines()
                    if tl["symbol"] == symbol
                ]
            else:
                active_trendlines = self.manager.get_active_trendlines()

            results = {}
            for trendline in active_trendlines:
                trendline_id = trendline["id"]
                result = self.check_trendline_breakout_detailed(trendline_id)
                if result:
                    results[trendline_id] = result

            return results
        except Exception as e:
            print(f"批量检查趋势线失败: {e}")
            return {}


# 全局监测实例
_global_monitor = None


def get_global_monitor(data_dir: str = "data") -> TrendlineMonitor:
    """获取全局监测实例"""
    global _global_monitor
    if _global_monitor is None:
        _global_monitor = TrendlineMonitor(data_dir=data_dir)
    return _global_monitor


def start_global_monitoring(
    symbols: List[str], time_interval: str = "5m", data_dir: str = "data"
):
    """启动全局监测"""
    monitor = get_global_monitor(data_dir)
    monitor.start_monitoring(symbols, time_interval)


def stop_global_monitoring():
    """停止全局监测"""
    global _global_monitor
    if _global_monitor:
        _global_monitor.stop_monitoring()


# 示例使用
if __name__ == "__main__":
    # 创建监测引擎
    monitor = TrendlineMonitor()
    # 从趋势线中获取所有活跃趋势线的symbols
    active_trendlines = monitor.manager.get_active_trendlines()
    active_symbols = list(set([tl["symbol"] for tl in active_trendlines]))
    print(active_symbols)

    # 启动监测
    monitor.start_monitoring(
        symbols=active_symbols, time_interval="15m", check_interval=60
    )

    try:
        # 保持运行
        while True:
            time.sleep(10)
            # 显示监测状态
            status = monitor.get_monitoring_status()
            print(f"监测状态: {status}")
    except KeyboardInterrupt:
        monitor.stop_monitoring()
