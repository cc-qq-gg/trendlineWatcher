"""
K线图绘制演示
使用matplotlib绘制SOL的K线图
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def plot_kline_from_csv():
    """从CSV文件读取数据并绘制K线图"""

    # 读取数据
    df = pd.read_csv('./data/klines/SOL-USDT-SWAP_15m_candles.csv')

    # 转换时间列
    df['candle_begin_time_GMT8'] = pd.to_datetime(df['candle_begin_time_GMT8'])
    df = df.set_index('candle_begin_time_GMT8')

    # 重置索引并创建临时列用于define_trendline函数
    df_temp = df.reset_index()
    df_temp['candle_begin_time_GMT8'] = df_temp['candle_begin_time_GMT8'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # 只显示最近100条数据
    df_temp = df_temp.tail(600)
    df = df.tail(600)

    # 创建图表
    fig, ax1 = plt.subplots(1, 1, figsize=(15, 8))

    # 设置中文字体
    plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False

    # 标题
    ax1.set_title('SOL-USDT-SWAP 15分钟K线图与趋势线', fontsize=16, fontweight='bold')

    # 绘制K线
    for time, row in df.iterrows():
        color = 'red' if row['close'] >= row['open'] else 'green'

        # 绘制实体
        ax1.plot([time, time], [row['open'], row['close']],
                color=color, linewidth=3)

        # 绘制影线
        ax1.plot([time, time], [row['low'], row['high']],
                color=color, linewidth=1, alpha=0.7)

    # 读取趋势线数据
    try:
        trendlines_df = pd.read_csv('data/trendlines.csv')

        # 绘制趋势线
        for _, trendline in trendlines_df.iterrows():
            if trendline['status'] == 'active':
                start_time = trendline['start_time']
                end_time = trendline['end_time']
                start_price = trendline['start_price']
                end_price = trendline['end_price']
                direction = trendline['direction']

                # 简单画一条直线
                try:
                    # 找到起点和终点位置
                    start_time_dt = pd.to_datetime(start_time)
                    end_time_dt = pd.to_datetime(end_time)

                    # 计算趋势线斜率
                    time_delta = (end_time_dt - start_time_dt).total_seconds()
                    price_delta = end_price - start_price
                    slope = price_delta / time_delta if time_delta != 0 else 0

                    # 延伸到最新K线
                    latest_time = df.index[-1]

                    # 计算延伸后的价格
                    extend_time_delta = (latest_time - end_time_dt).total_seconds()
                    extend_price = end_price + slope * extend_time_delta

                    # 确保起点在图表时间范围内
                    if start_time_dt >= df.index.min():
                        # 绘制连接起点到最新K线的趋势线
                        line_x = [start_time_dt, end_time_dt, latest_time]
                        line_y = [start_price, end_price, extend_price]

                        line_color = 'green' if direction == 1 else 'red'
                        line_label = '上升趋势线' if direction == 1 else '下降趋势线'

                        ax1.plot(line_x, line_y,
                               color=line_color, linewidth=1, linestyle='-', alpha=0.8,
                               label=line_label)
                    else:
                        print(f"趋势线点超出范围: {start_time} - {end_time}")

                except Exception as e:
                    print(f"绘制趋势线时出错: {e}")

                # 标记趋势线起点和终点
                # start_time_dt = pd.to_datetime(start_time)
                # end_time_dt = pd.to_datetime(end_time)
                # ax1.scatter([start_time_dt], [start_price], color='green', s=80, zorder=5)
                # ax1.scatter([end_time_dt], [end_price], color='red', s=80, zorder=5)

        ax1.legend()

    except FileNotFoundError:
        print("未找到趋势线数据文件")
    except Exception as e:
        print(f"读取趋势线数据时出错: {e}")

    # 设置价格轴
    ax1.set_ylabel('价格 (USDT)', fontsize=12)
    ax1.grid(True, alpha=0.3)

    # 格式化时间轴
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax1.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

    # 显示最新价格
    latest_price = df.iloc[-1]['close']
    latest_time = df.index[-1].strftime('%Y-%m-%d %H:%M:%S')
    ax1.text(0.02, 0.98, f'最新价格: {latest_price:.2f}\n时间: {latest_time}',
             transform=ax1.transAxes, verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
             fontsize=10)

    plt.tight_layout()
    plt.show()

    print(f"K线图已生成，数据范围: {df.index[0]} 到 {df.index[-1]}")
    print(f"最新价格: {latest_price:.2f}")

if __name__ == "__main__":
    plot_kline_from_csv()