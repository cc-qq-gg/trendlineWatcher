from time import sleep
from Function import *
from Config import *
from config_constants import (
    OKEX_READONLY_CONFIG,
    DINGTALK_ROBOT_CONFIG,
    DEFAULT_TIME_INTERVAL,
    EXCHANGE_TIMEOUT
)

pd.set_option("future.no_silent_downcasting", True)
pd.set_option("display.max_rows", 1000)
pd.set_option("expand_frame_repr", False)  # 当列太多时不换行
# 设置命令行输出时的列对齐功能
pd.set_option("display.unicode.ambiguous_as_wide", True)
pd.set_option("display.unicode.east_asian_width", True)
# 测试时ccxt版本为 1.57.11。若不是此版本，可能会报错，可能性很低。print(ccxt.__version__)可以查看ccxt版本。

# =====配置运行相关参数=====
# =执行的时间间隔 [1m/3m/5m/15m/30m/1H/2H/4H]
time_interval = DEFAULT_TIME_INTERVAL

# =钉钉
# 在一个钉钉群中，可以创建多个钉钉机器人。
# 建议单独建立一个报错机器人，该机器人专门发报错信息。请务必将报错机器人在id和secret放到function.send_dingding_msg的默认参数中。
robot_id_secret = DINGTALK_ROBOT_CONFIG

# =交易所配置
OKEX_CONFIG = OKEX_READONLY_CONFIG
exchange = ccxt.okx(OKEX_CONFIG)

# =====配置交易相关参数=====
# 更新需要交易的合约、策略参数、下单量等配置信息
symbol_config = {
    # 'BTC-USDT-SWAP'.lower(): {'instrument_id': 'BTC-USDT-SWAP',  # 合约代码，当更换合约的时候需要手工修改
    #              'leverage': '1.2',  # 控制实际交易的杠杆倍数，在实际交易中可以自己修改。此处杠杆数，必须小于页面上的最大杠杆数限制
    #              'strategy_name': 'real_signal_simple_bolling',  # 使用的策略的名称
    #              'para': [10, 1.6]},  # 策略参数
    "SOL-USDT-SWAP".lower(): {
        "instrument_id": "SOL-USDT-SWAP",
        "leverage": "1.2",
        "strategy_name": "real_signal_trendline",  # 不同币种可以使用不同的策略
        "para": {
            "start_point": ["2025-05-09 18:00:00", 174.25],
            "end_point": ["2025-05-14 06:00:00", 183.44],
            "direction": 1,
        },
    },
}


def main():
    # =====获取需要交易币种的历史数据=====
    max_len = 1000  # 设定最多收集多少根K线，okex不能超过1440根
    symbol_candle_data = dict()  # 用于存储K线数据
    # 遍历获取币种历史数据
    for symbol in symbol_config.keys():
        # 获取币种的历史数据，会删除最新一行的数据
        symbol_candle_data[symbol] = fetch_okex_symbol_history_candle_data(
            exchange,
            symbol_config[symbol]["instrument_id"],
            time_interval,
            max_len=max_len,
        )

        time.sleep(medium_sleep_time)
    # ===进入每次的循环
    while True:
        # =获取持仓数据
        # 初始化symbol_info，在每次循环开始时都初始化
        symbol_info_columns = [
            "账户余额",
            "持仓方向",
            "持仓量",
            "持仓收益率",
            "持仓收益",
            "持仓均价",
            "当前价格",
            "最大杠杆",
        ]
        symbol_info = pd.DataFrame(
            index=symbol_config.keys(), columns=symbol_info_columns
        )  # 转化为dataframe
        # 更新账户信息symbol_info
        symbol_info = update_symbol_info(exchange, symbol_info, symbol_config)

        print("\nsymbol_info:\n", symbol_info, "\n")

        # =获取策略执行时间，并sleep至该时间
        run_time = sleep_until_run_time(time_interval)

        # =并行获取所有币种最近数据
        exchange.timeout = (
            1000  # 即将获取最新数据，临时将timeout设置为1s，加快获取数据速度
        )
        candle_num = 10  # 只获取最近candle_num根K线数据，可以获得更快的速度
        # 获取数据
        recent_candle_data = single_threading_get_data(
            exchange, symbol_info, symbol_config, time_interval, run_time, candle_num
        )
        for symbol in symbol_config.keys():
            print(recent_candle_data[symbol].tail(2))

        # 将symbol_candle_data和最新获取的recent_candle_data数据合并
        for symbol in symbol_config.keys():
            df = pd.concat(
                [symbol_candle_data[symbol], recent_candle_data[symbol]],
                ignore_index=True,
            )
            df.drop_duplicates(
                subset=["candle_begin_time_GMT8"], keep="last", inplace=True
            )
            df.sort_values(
                by="candle_begin_time_GMT8", inplace=True
            )  # 排序，理论上这步应该可以省略，加快速度
            df = df.iloc[-max_len:]  # 保持最大K线数量不会超过max_len个
            df.reset_index(drop=True, inplace=True)
            symbol_candle_data[symbol] = df

        # =计算每个币种的交易信号
        symbol_signal = calculate_signal(symbol_info, symbol_config, symbol_candle_data)
        print("\nsymbol_info:\n", symbol_info)
        print("本周期交易计划:", symbol_signal)

        # =下单
        exchange.timeout = (
            exchange_timeout  # 下单时需要增加timeout的时间，将timout恢复正常
        )
        symbol_order = pd.DataFrame()
        if symbol_signal:
            symbol_order = single_threading_place_order(
                exchange, symbol_info, symbol_config, symbol_signal
            )  # 单线程下单
            print("下单记录：\n", symbol_order)
        # 重新更新账户信息symbol_info
        time.sleep(long_sleep_time)  # 休息一段时间再更新
        # symbol_info = pd.DataFrame(index=symbol_config.keys(), columns=symbol_info_columns)
        symbol_info = pd.DataFrame(index=symbol_config.keys())
        symbol_info = update_symbol_info(exchange, symbol_info, symbol_config)
        print("\nsymbol_info:\n", symbol_info, "\n")

        # 发送钉钉
        dingding_report_every_loop(
            symbol_info, symbol_signal, symbol_order, run_time, robot_id_secret
        )

        # 本次循环结束
        print(
            "\n",
            "-" * 20,
            "本次循环结束，%f秒后进入下一次循环" % long_sleep_time,
            "-" * 20,
            "\n\n",
        )
        time.sleep(long_sleep_time)


if __name__ == "__main__":
    # 查看账户信息
    # d = exchange.private_get_account_balance({'ccy': 'BTC'})
    # order_info = ''
    # try:
    #    order_info = exchange.private_post_trade_order({'instId': 'SOL-USDT-SWAP', 'ordType': 'limit', 'px': '147.87', 'side': 'buy', 'sz': '0.64', 'tdMode': 'cross'})
    # except Exception as e:
    #    print(e)
    # print(order_info)
    # send_dingding_msg('系统出错，10s之后重新运行，出错原因：')
    # exit()
    while True:
        try:
            main()
        except Exception as e:
            send_dingding_msg("系统出错，10s之后重新运行，出错原因：" + str(e))
            print(e)
            sleep(long_sleep_time)
