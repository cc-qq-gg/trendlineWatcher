"""
顶背离/底背离检测与可视化
- 支持 BTC、ETH、SOL 的 4h、1d 周期
- 基于StochRSI指标检测背离
- 使用mplfinance加速绘图
- 保存数据到CSV方便后续分析
"""

import numpy as np
import pandas as pd
import talib
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import mplfinance as mpf
import os
import sys
import warnings
from get_data import smart_get_data

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from divergence_algorithm import (
    detect_top_divergence as core_detect_top_divergence,
    detect_bottom_divergence as core_detect_bottom_divergence,
)
warnings.filterwarnings('ignore')

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
DEFAULT_DATA_DIR = os.path.join(PROJECT_ROOT, 'data', 'okx', 'csv')

# 设置中文字体 - macOS
plt.rcParams['font.sans-serif'] = ['PingFang HK', 'STHeiti', 'Heiti TC', 'SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['font.family'] = 'sans-serif'


def StochRSI(close, m=14, p=3):
    """计算 StochRSI 指标"""
    RSI = talib.RSI(np.array(close), timeperiod=m)
    RSI = pd.DataFrame(RSI)
    LLV = RSI.rolling(window=m).min()
    HHV = RSI.rolling(window=m).max()
    stochrsi = (RSI - LLV) / (HHV - LLV) * 100
    stochrsi = talib.MA(np.array(stochrsi[0]), p)
    fastk = talib.MA(np.array(stochrsi), p)
    return stochrsi, fastk


def detect_top_divergence(df):
    """
    检测顶背离 (tbl)
    条件:
    1. 找转折点: stochrsi 是局部高点
    2. 背离: stochrsi.shift(1) > 93 且 high > high.shift(1) 且 stochrsi < stochrsi.shift(1)
       - 价格创新高，但指标没创新高
    """
    _, _, tbl = core_detect_top_divergence(
        df, stoch_col="stochrsi", high_col="High", turn_col="turn", signal_col="tbl"
    )
    return tbl


def detect_bottom_divergence(df):
    """
    检测底背离 (dbl)
    条件:
    1. 找转折点: stochrsi 是局部低点
    2. 背离: stochrsi.shift(1) < 5 且 low < low.shift(1) 且 stochrsi > stochrsi.shift(1)
       - 价格创新低，但指标没创新低
    """
    _, _, dbl = core_detect_bottom_divergence(
        df, stoch_col="stochrsi", low_col="Low", turn_col="turn", signal_col="dbl"
    )
    return dbl


def save_data(df, tbl_points, dbl_points, save_path):
    """保存数据到CSV，包含背离标记"""
    df_save = df.copy()
    
    # 添加背离标记列
    df_save['tbl'] = np.nan  # 顶背离标记
    df_save['dbl'] = np.nan  # 底背离标记
    
    # 标记背离点
    for idx in tbl_points.index:
        df_save.loc[idx, 'tbl'] = -1
    for idx in dbl_points.index:
        df_save.loc[idx, 'dbl'] = 1
    
    # 删除临时列
    if 'turn' in df_save.columns:
        df_save = df_save.drop(columns=['turn'])
    
    df_save.to_csv(save_path, index=False)
    print(f"已保存数据: {save_path}")


def plot_divergence_fast(df, tbl_points, dbl_points, symbol, timeframe, save_path):
    """使用mplfinance快速绘制K线图并标注背离点"""
    
    # 准备mplfinance格式的数据
    df_plot = df.copy()
    df_plot = df_plot.set_index('candle_begin_time')
    
    # 创建StochRSI的副图数据
    stochrsi_plot = df_plot['stochrsi'].values
    
    # 获取背离点的时间索引
    tbl_times = tbl_points['candle_begin_time'].tolist() if len(tbl_points) > 0 else []
    dbl_times = dbl_points['candle_begin_time'].tolist() if len(dbl_points) > 0 else []
    
    # 创建附加图层
    addplots = [
        mpf.make_addplot(stochrsi_plot, panel=1, color='blue', width=1, 
                         ylabel='StochRSI', ylim=(0, 100)),
        mpf.make_addplot([80] * len(df_plot), panel=1, color='red', 
                         linestyle='--', alpha=0.5, width=0.5),
        mpf.make_addplot([20] * len(df_plot), panel=1, color='green', 
                         linestyle='--', alpha=0.5, width=0.5),
    ]
    
    # 创建自定义样式 - 包含中文字体支持
    mc = mpf.make_marketcolors(
        up='red', down='green',
        edge='inherit',
        wick='inherit',
        volume='in',
    )
    s = mpf.make_mpf_style(
        marketcolors=mc,
        gridstyle='-',
        gridcolor='lightgray',
        y_on_right=False,
        rc={
            'font.sans-serif': ['PingFang HK', 'STHeiti', 'Heiti TC', 'SimHei'],
            'axes.unicode_minus': False,
            'font.family': 'sans-serif'
        }
    )
    
    # 绘制图表
    fig, axes = mpf.plot(
        df_plot,
        type='candle',
        style=s,
        title=f'{symbol} {timeframe} - Divergence',
        ylabel='价格',
        volume=False,
        addplot=addplots,
        figsize=(16, 10),
        panel_ratios=(3, 1),
        returnfig=True
    )
    
    # 在K线图上添加背离标记
    ax1 = axes[0]  # K线图
    ax2 = axes[2]  # StochRSI图
    
    # 标记顶背离 - 使用时间匹配（只显示箭头，不显示文字）
    for t in tbl_times:
        if t in df_plot.index:
            pos = df_plot.index.get_loc(t)
            ax1.scatter(pos, df_plot.loc[t, 'High'] * 1.01, 
                       marker='v', s=50, c='red', zorder=10)
    
    # 标记底背离 - 使用时间匹配（只显示箭头，不显示文字）
    for t in dbl_times:
        if t in df_plot.index:
            pos = df_plot.index.get_loc(t)
            ax1.scatter(pos, df_plot.loc[t, 'Low'] * 0.99, 
                       marker='^', s=50, c='green', zorder=10)
    
    # 在StochRSI图上也标记背离点
    for t in tbl_times:
        if t in df_plot.index:
            pos = df_plot.index.get_loc(t)
            ax2.scatter(pos, df_plot.loc[t, 'stochrsi'], 
                       marker='v', s=40, c='red', zorder=5)
    
    for t in dbl_times:
        if t in df_plot.index:
            pos = df_plot.index.get_loc(t)
            ax2.scatter(pos, df_plot.loc[t, 'stochrsi'], 
                       marker='^', s=40, c='green', zorder=5)
    
    # 添加图例
    ax1.legend(['up ▼', 'down ▲'], loc='upper left')
    
    plt.tight_layout()
    
    return fig, axes


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def format_timeframe(timeframe):
    return timeframe.upper()


def get_output_paths(symbol, timeframe, suffix=''):
    coin = symbol.split('-')[0] if '-' in symbol else symbol
    tf = format_timeframe(timeframe)
    ensure_output_dir()
    return (
        os.path.join(OUTPUT_DIR, f"detector_{coin}_{tf}_signals{suffix}.csv"),
        os.path.join(OUTPUT_DIR, f"detector_{coin}_{tf}_overview{suffix}.png"),
    )


def resolve_data_dir(data_dir=None):
    """将数据目录解析为绝对路径，避免受启动目录影响。"""
    if not data_dir:
        return DEFAULT_DATA_DIR
    if os.path.isabs(data_dir):
        return data_dir
    return os.path.abspath(os.path.join(PROJECT_ROOT, data_dir))


def process_symbol(symbol, timeframe, data_dir=None, show_interactive=False, limit_bars=None, output_suffix=''):
    """处理单个品种和周期
    
    Args:
        symbol: 币种符号 (BTC, ETH, SOL)
        timeframe: 时间周期 (4h, 1d, 1w)
        data_dir: 数据目录
        show_interactive: 是否显示交互式图表（可放大缩小）
        limit_bars: 限制K线数量，None表示不限制
    """
    
    # 使用15m数据，然后resample到目标周期
    data_dir_resolved = resolve_data_dir(data_dir)
    csv_path = os.path.join(data_dir_resolved, f'{symbol}-USDT-SWAP_15m.csv')
    
    if not os.path.exists(csv_path):
        print(f"文件不存在: {csv_path}")
        return None
    
    # 读取数据
    df = pd.read_csv(csv_path, index_col='candle_begin_time', parse_dates=True)
    df.columns = [c.capitalize() for c in df.columns]
    
    # Resample到目标周期
    if timeframe != '15m':
        df = df.resample(timeframe, label='left', closed='left').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }).dropna()
    
    df = df.reset_index()
    
    # 统一列名（时间列保持小写，其他列首字母大写）
    df.columns = ['candle_begin_time' if 'candle_begin_time' in c.lower() else c.capitalize() 
                  for c in df.columns]
    
    # 限制K线数量
    if limit_bars and len(df) > limit_bars:
        df = df.tail(limit_bars).reset_index(drop=True)
    
    # 计算StochRSI
    stochrsi, _ = StochRSI(df['Close'].tolist(), m=14, p=3)
    df['stochrsi'] = stochrsi
    
    # 检测背离
    tbl_points = detect_top_divergence(df)
    dbl_points = detect_bottom_divergence(df)
    
    # 输出统计
    print(f"\n{'='*50}")
    print(f"{symbol} {timeframe}")
    print(f"数据范围: {df['candle_begin_time'].iloc[0]} ~ {df['candle_begin_time'].iloc[-1]}")
    print(f"K线数量: {len(df)}")
    print(f"顶背离数量: {len(tbl_points)}")
    print(f"底背离数量: {len(dbl_points)}")
    
    # 显示最近的背离
    if len(tbl_points) > 0:
        print(f"\n最近顶背离:")
        for idx, row in tbl_points.tail(5).iterrows():
            print(f"  {row['candle_begin_time']}: 价格={row['High']:.2f}, StochRSI={row['stochrsi']:.2f}")
    
    if len(dbl_points) > 0:
        print(f"\n最近底背离:")
        for idx, row in dbl_points.tail(5).iterrows():
            print(f"  {row['candle_begin_time']}: 价格={row['Low']:.2f}, StochRSI={row['stochrsi']:.2f}")
    
    data_path, img_path = get_output_paths(symbol, timeframe, suffix=output_suffix)

    # 保存数据到CSV
    save_data(df, tbl_points, dbl_points, data_path)
    
    # 绘图
    fig, axes = plot_divergence_fast(df, tbl_points, dbl_points, symbol, timeframe, img_path)
    
    if show_interactive:
        print(f"\n显示交互式图表（可放大缩小）...")
        plt.show()
    else:
        plt.savefig(img_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"已保存图片: {img_path}")
    
    return tbl_points, dbl_points


def generate_detector_assets(symbol: str, timeframe: str, mobile_limit: int = 200, required_days: int = 90):
    """生成桌面版和移动版背离图。

    - 桌面图: detector_{COIN}_{TF}_overview.png
    - 移动图: detector_{COIN}_{TF}_overview_mobile.png（最多 mobile_limit 根K线）
    """
    symbol = symbol.upper()
    timeframe = timeframe.lower()
    symbol_swap = f"{symbol}-USDT-SWAP"

    # 先确保本地多周期数据是最新，后续绘图直接复用本地csv
    smart_get_data(
        symbol=symbol_swap,
        timeframe=timeframe.upper(),
        required_days=required_days,
        verbose=False,
    )

    data_dir = resolve_data_dir()

    # 桌面图
    process_symbol(symbol, timeframe, data_dir=data_dir, show_interactive=False, limit_bars=None, output_suffix='')
    # 移动图（限制K线数量）
    process_symbol(symbol, timeframe, data_dir=data_dir, show_interactive=False, limit_bars=mobile_limit, output_suffix='_mobile')


def view_interactive(symbol='BTC', timeframe='4h', limit_bars=500, data_dir=None):
    """
    交互式查看单个品种的背离图表（可放大缩小）
    
    Args:
        symbol: 币种符号，默认 BTC
        timeframe: 时间周期，默认 4h
        limit_bars: 显示的K线数量，默认500（太多会很卡）
        data_dir: 数据目录
    
    使用方法:
        python -c "from divergence_detector import view_interactive; view_interactive('BTC', '4h')"
    或在脚本中:
        view_interactive('ETH', '1d', limit_bars=300)
    """
    print(f"\n交互式查看: {symbol} {timeframe}")
    print("提示: 图表窗口中可以使用工具栏放大、缩小、平移")
    
    process_symbol(symbol, timeframe, data_dir, show_interactive=True, limit_bars=limit_bars)


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='顶背离/底背离检测工具')
    parser.add_argument('--symbol', '-s', default=None, help='单独查看某个币种 (BTC/ETH/SOL)')
    parser.add_argument('--timeframe', '-t', default='4h', help='时间周期 (4h/1d)')
    parser.add_argument('--interactive', '-i', action='store_true', help='交互式显示（可放大缩小）')
    parser.add_argument('--limit', '-l', type=int, default=500, help='K线数量限制（交互模式建议500以内）')
    
    args = parser.parse_args()
    
    if args.symbol:
        # 单独查看某个币种
        process_symbol(
            args.symbol, 
            args.timeframe, 
            show_interactive=args.interactive,
            limit_bars=args.limit
        )
    else:
        # 批量处理所有币种
        symbols = ['BTC', 'ETH', 'SOL']
        timeframes = ['4h', '1d']
        
        print("="*60)
        print("顶背离/底背离检测工具")
        print("="*60)
        
        all_results = {}
        
        for symbol in symbols:
            for tf in timeframes:
                result = process_symbol(symbol, tf, show_interactive=False)
                if result:
                    all_results[f"{symbol}_{tf}"] = result
        
        # 汇总统计
        print("\n" + "="*60)
        print("汇总统计")
        print("="*60)
        
        for key, (tbl, dbl) in all_results.items():
            print(f"{key}: 顶背离={len(tbl)}, 底背离={len(dbl)}")
        
        print(f"\n所有文件已保存到: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
