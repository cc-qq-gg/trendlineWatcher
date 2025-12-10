#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
波动率计算器模块
为K线数据提供多种波动率计算方法和2σ/3σ异常检测
"""

import pandas as pd
import numpy as np
import math
from typing import Dict, Any, Optional, Tuple

def calculate_returns_volatility(df: pd.DataFrame, window: int = 20) -> float:
    """
    基于收益率计算波动率

    Args:
        df: K线数据DataFrame，包含close列
        window: 滚动窗口大小

    Returns:
        float: 波动率百分比
    """
    if len(df) < 2:
        return 0.0

    # 计算收益率
    returns = df['close'].pct_change().dropna()

    if len(returns) == 0:
        return 0.0

    # 计算滚动标准差
    if len(returns) >= window:
        rolling_std = returns.rolling(window=window).std()
        return rolling_std.iloc[-1] * 100 if not pd.isna(rolling_std.iloc[-1]) else 0.0
    else:
        # 如果数据不足，使用所有数据
        return returns.std() * 100

def calculate_range_volatility(df: pd.DataFrame, window: int = 20) -> float:
    """
    基于价格区间计算波动率

    Args:
        df: K线数据DataFrame，包含high和low列
        window: 滚动窗口大小

    Returns:
        float: 波动率百分比
    """
    if len(df) < 2:
        return 0.0

    # 计算价格范围百分比
    range_pct = (df['high'] - df['low']) / df['close'] * 100

    if len(range_pct) >= window:
        rolling_avg = range_pct.rolling(window=window).mean()
        return rolling_avg.iloc[-1] if not pd.isna(rolling_avg.iloc[-1]) else 0.0
    else:
        return range_pct.mean()

def calculate_parkinson_volatility(df: pd.DataFrame, window: int = 20) -> float:
    """
    基于Parkinson方法计算波动率（更精确）

    Args:
        df: K线数据DataFrame，包含high和low列
        window: 滚动窗口大小

    Returns:
        float: 波动率百分比
    """
    if len(df) < 2:
        return 0.0

    # 计算Parkinson波动率
    hl_ratio = df['high'] / df['low']
    parkinson_vol = np.sqrt(0.361 * (np.log(hl_ratio) ** 2))

    if len(parkinson_vol) >= window:
        rolling_avg = pd.Series(parkinson_vol).rolling(window=window).mean()
        return rolling_avg.iloc[-1] * 100 if not pd.isna(rolling_avg.iloc[-1]) else 0.0
    else:
        return parkinson_vol.mean() * 100

def calculate_sigma_level(current_vol: float, historical_mean: float, historical_std: float) -> Tuple[str, str]:
    """
    计算当前波动率的σ水平

    Args:
        current_vol: 当前波动率
        historical_mean: 历史平均波动率
        historical_std: 历史波动率标准差

    Returns:
        Tuple[str, str]: (sigma_level, description)
    """
    if historical_std == 0:
        return '', '正常'

    z_score = (current_vol - historical_mean) / historical_std
    abs_z_score = abs(z_score)

    if abs_z_score >= 3:
        return '3σ', '极端波动'
    elif abs_z_score >= 2:
        return '2σ', '高波动'
    elif abs_z_score >= 1:
        return '1σ', '中等波动'
    else:
        return '', '正常波动'

def calculate_volatility_trend(volatility_series: pd.Series) -> str:
    """
    计算波动率趋势

    Args:
        volatility_series: 波动率序列

    Returns:
        str: 趋势描述 ('increasing', 'decreasing', 'stable')
    """
    if len(volatility_series) < 5:
        return 'stable'

    # 使用线性回归判断趋势
    x = np.arange(len(volatility_series))
    y = volatility_series.values

    # 去除NaN值
    mask = ~np.isnan(y)
    if mask.sum() < 3:
        return 'stable'

    x_clean = x[mask]
    y_clean = y[mask]

    # 计算斜率
    slope = np.polyfit(x_clean, y_clean, 1)[0]

    # 根据斜率判断趋势
    if slope > 0.1:
        return 'increasing'
    elif slope < -0.1:
        return 'decreasing'
    else:
        return 'stable'

def detect_anomalies(returns: pd.Series, mean_return: float, std_return: float) -> Dict[str, Any]:
    """
    检测收益率异常

    Args:
        returns: 收益率序列
        mean_return: 平均收益率
        std_return: 收益率标准差

    Returns:
        Dict: 异常检测结果
    """
    anomalies = {
        'extreme_positive': 0,
        'extreme_negative': 0,
        'moderate_positive': 0,
        'moderate_negative': 0,
        'total_anomalies': 0
    }

    # 3σ阈值
    upper_3sigma = mean_return + 3 * std_return
    lower_3sigma = mean_return - 3 * std_return

    # 2σ阈值
    upper_2sigma = mean_return + 2 * std_return
    lower_2sigma = mean_return - 2 * std_return

    for ret in returns:
        if not pd.isna(ret):
            if ret > upper_3sigma:
                anomalies['extreme_positive'] += 1
            elif ret < lower_3sigma:
                anomalies['extreme_negative'] += 1
            elif ret > upper_2sigma:
                anomalies['moderate_positive'] += 1
            elif ret < lower_2sigma:
                anomalies['moderate_negative'] += 1

    anomalies['total_anomalies'] = sum(anomalies[k] for k in anomalies.keys() if k != 'total_anomalies')

    return anomalies

def calculate_volatility(df: pd.DataFrame, method: str = 'returns', window: int = 20) -> Dict[str, Any]:
    """
    计算K线数据的波动率指标

    Args:
        df: K线数据DataFrame，必须包含OHLCV列
        method: 计算方法 ('returns', 'range', 'parkinson')
        window: 滚动窗口大小

    Returns:
        Dict: 包含各种波动率指标的字典

    Returns格式:
        {
            'current_volatility': float,      # 当前波动率 %
            'rolling_volatility': float,       # 滚动窗口波动率 %
            'sigma_level': str,               # '', '1σ', '2σ', '3σ'
            'sigma_description': str,         # σ水平描述
            'historical_mean': float,         # 历史平均波动率
            'historical_std': float,          # 历史波动率标准差
            'volatility_trend': str,          # 'increasing', 'decreasing', 'stable'
            'anomaly_flags': list,            # 异常标记列表
            'method_used': str,               # 使用的计算方法
            'data_points': int,               # 数据点数量
            'anomaly_stats': dict            # 异常统计
        }
    """

    # 验证输入数据
    if df is None or df.empty:
        return {
            'current_volatility': 0.0,
            'rolling_volatility': 0.0,
            'sigma_level': '',
            'sigma_description': '无数据',
            'historical_mean': 0.0,
            'historical_std': 0.0,
            'volatility_trend': 'stable',
            'anomaly_flags': ['no_data'],
            'method_used': method,
            'data_points': 0,
            'anomaly_stats': {}
        }

    # 检查必需的列
    required_cols = {
        'returns': ['close'],
        'range': ['high', 'low', 'close'],
        'parkinson': ['high', 'low']
    }

    if not all(col in df.columns for col in required_cols.get(method, [])):
        return {
            'current_volatility': 0.0,
            'rolling_volatility': 0.0,
            'sigma_level': '',
            'sigma_description': '数据列不完整',
            'historical_mean': 0.0,
            'historical_std': 0.0,
            'volatility_trend': 'stable',
            'anomaly_flags': ['incomplete_data'],
            'method_used': method,
            'data_points': len(df),
            'anomaly_stats': {}
        }

    try:
        # 计算当前波动率
        if method == 'returns':
            current_vol = calculate_returns_volatility(df, window)
        elif method == 'range':
            current_vol = calculate_range_volatility(df, window)
        elif method == 'parkinson':
            current_vol = calculate_parkinson_volatility(df, window)
        else:
            # 默认使用returns方法
            current_vol = calculate_returns_volatility(df, window)
            method = 'returns'

        # 计算历史波动率统计
        if method == 'returns':
            returns = df['close'].pct_change().dropna()
            volatility_series = returns.rolling(window=window).std() * 100
        else:
            # 对于其他方法，需要计算整个序列的波动率
            vols = []
            for i in range(window, len(df)):
                window_df = df.iloc[i-window+1:i+1]
                if method == 'range':
                    vol = calculate_range_volatility(window_df, window)
                elif method == 'parkinson':
                    vol = calculate_parkinson_volatility(window_df, window)
                else:
                    vol = calculate_returns_volatility(window_df, window)
                vols.append(vol)
            volatility_series = pd.Series(vols)

        if len(volatility_series) > 0:
            historical_mean = volatility_series.mean()
            historical_std = volatility_series.std()
        else:
            historical_mean = current_vol
            historical_std = 0.0

        # 计算σ水平
        sigma_level, sigma_description = calculate_sigma_level(current_vol, historical_mean, historical_std)

        # 计算趋势
        trend = calculate_volatility_trend(volatility_series)

        # 异常检测
        anomaly_flags = []
        anomaly_stats = {}

        if method == 'returns' and len(returns) > 0:
            mean_return = returns.mean()
            std_return = returns.std()
            anomaly_stats = detect_anomalies(returns, mean_return, std_return)

            if anomaly_stats['total_anomalies'] > 0:
                anomaly_flags.append('return_anomalies')
            if anomaly_stats['extreme_positive'] > 0 or anomaly_stats['extreme_negative'] > 0:
                anomaly_flags.append('extreme_movements')

        # 波动率异常检测
        if sigma_level in ['2σ', '3σ']:
            anomaly_flags.append('high_volatility')

        # 数据质量检查
        if len(df) < window:
            anomaly_flags.append('insufficient_data')

        return {
            'current_volatility': round(float(current_vol), 4),
            'rolling_volatility': round(float(current_vol), 4),  # 当前实现中与current相同
            'sigma_level': sigma_level,
            'sigma_description': sigma_description,
            'historical_mean': round(float(historical_mean), 4),
            'historical_std': round(float(historical_std), 4),
            'volatility_trend': trend,
            'anomaly_flags': anomaly_flags,
            'method_used': method,
            'data_points': len(df),
            'anomaly_stats': anomaly_stats
        }

    except Exception as e:
        # 错误处理
        return {
            'current_volatility': 0.0,
            'rolling_volatility': 0.0,
            'sigma_level': '',
            'sigma_description': f'计算错误: {str(e)}',
            'historical_mean': 0.0,
            'historical_std': 0.0,
            'volatility_trend': 'stable',
            'anomaly_flags': ['calculation_error'],
            'method_used': method,
            'data_points': len(df) if df is not None else 0,
            'anomaly_stats': {}
        }

def add_volatility_to_dataframe(df: pd.DataFrame, method: str = 'returns', window: int = 20) -> pd.DataFrame:
    """
    为DataFrame添加波动率列

    Args:
        df: K线数据DataFrame
        method: 计算方法
        window: 窗口大小

    Returns:
        pd.DataFrame: 添加了波动率列的DataFrame
    """
    df_result = df.copy()

    # 计算波动率指标
    vol_data = calculate_volatility(df, method, window)

    # 添加波动率列
    df_result['volatility'] = vol_data['current_volatility']
    df_result['volatility_rolling'] = vol_data['rolling_volatility']
    df_result['volatility_sigma_level'] = vol_data['sigma_level']
    df_result['volatility_trend'] = vol_data['volatility_trend']
    df_result['volatility_historical_mean'] = vol_data['historical_mean']
    df_result['volatility_historical_std'] = vol_data['historical_std']

    return df_result

# 示例使用
if __name__ == "__main__":
    # 测试代码
    print("波动率计算器模块测试")
    print("=" * 50)

    # 创建测试数据
    dates = pd.date_range('2025-01-01', periods=100, freq='D')
    np.random.seed(42)

    # 模拟价格数据
    base_price = 100
    returns = np.random.normal(0.001, 0.02, 100)  # 1%收益，2%波动率
    prices = [base_price]

    for ret in returns[1:]:
        prices.append(prices[-1] * (1 + ret))

    # 创建OHLC数据
    data = []
    for i, price in enumerate(prices):
        high = price * (1 + abs(np.random.normal(0, 0.01)))
        low = price * (1 - abs(np.random.normal(0, 0.01)))
        open_price = low + (high - low) * np.random.random()
        volume = np.random.randint(1000000, 10000000)

        data.append({
            'datetime': dates[i],
            'open': open_price,
            'high': high,
            'low': low,
            'close': price,
            'volume': volume
        })

    df_test = pd.DataFrame(data)

    # 测试不同计算方法
    methods = ['returns', 'range', 'parkinson']

    for method in methods:
        print(f"\n{method.upper()} 方法:")
        vol_result = calculate_volatility(df_test, method=method, window=20)

        print(f"  当前波动率: {vol_result['current_volatility']:.4f}%")
        print(f"  σ水平: {vol_result['sigma_level']}")
        print(f"  趋势: {vol_result['volatility_trend']}")
        print(f"  异常标记: {vol_result['anomaly_flags']}")

    print(f"\n模块加载成功！")