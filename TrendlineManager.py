"""
趋势线管理系统 - 趋势线配置管理模块
复用现有的K线数据和趋势线计算逻辑
使用CSV文件存储数据
"""

import json
import uuid
from datetime import datetime
from typing import List, Dict, Optional, Any
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from Signals import define_trendline, monitor_breakout


class TrendlineManager:
    """趋势线管理器"""

    def __init__(self, data_dir: str = "data"):
        """初始化数据目录"""
        self.data_dir = data_dir
        self.trendlines_file = f"{data_dir}/trendlines.csv"
        self.logs_file = f"{data_dir}/monitor_logs.csv"
        self.init_data_files()

    def init_data_files(self):
        """初始化数据文件"""
        import os
        os.makedirs(self.data_dir, exist_ok=True)

        # 初始化趋势线文件
        if not os.path.exists(self.trendlines_file):
            trendlines_df = pd.DataFrame(columns=[
                'id', 'name', 'symbol', 'start_time', 'start_price',
                'end_time', 'end_price', 'direction', 'status',
                'created_at', 'updated_at'
            ])
            trendlines_df.to_csv(self.trendlines_file, index=False)

        # 初始化日志文件
        if not os.path.exists(self.logs_file):
            logs_df = pd.DataFrame(columns=[
                'id', 'trendline_id', 'signal_type', 'price',
                'trendline_value', 'detected_at'
            ])
            logs_df.to_csv(self.logs_file, index=False)

    def _load_trendlines(self) -> pd.DataFrame:
        """加载趋势线数据"""
        try:
            df = pd.read_csv(self.trendlines_file)
            return df.copy()  # 加载所有记录，不过滤
        except:
            return pd.DataFrame(columns=[
                'id', 'name', 'symbol', 'start_time', 'start_price',
                'end_time', 'end_price', 'direction', 'status',
                'created_at', 'updated_at'
            ])

    def _save_trendlines(self, df: pd.DataFrame):
        """保存趋势线数据"""
        df.to_csv(self.trendlines_file, index=False)

    def _load_logs(self) -> pd.DataFrame:
        """加载日志数据"""
        try:
            return pd.read_csv(self.logs_file)
        except:
            return pd.DataFrame(columns=[
                'id', 'trendline_id', 'signal_type', 'price',
                'trendline_value', 'detected_at'
            ])

    def _save_logs(self, df: pd.DataFrame):
        """保存日志数据"""
        df.to_csv(self.logs_file, index=False)

    def create_trendline(self, name: str, symbol: str, start_point: List,
                        end_point: List, direction: int, price_info: str = None,
                        candle_data: Dict = None, end_price_info: str = None,
                        end_candle_data: Dict = None) -> str:
        """创建新的趋势线配置"""
        trendline_id = str(uuid.uuid4())
        now = datetime.now().isoformat()

        start_time, start_price = start_point
        end_time, end_price = end_point

        new_trendline = {
            'id': trendline_id,
            'name': name,
            'symbol': symbol,
            'start_time': start_time,
            'start_price': start_price,
            'end_time': end_time,
            'end_price': end_price,
            'direction': direction,
            'status': 'active',
            'created_at': now,
            'updated_at': now
        }

        # 添加价格信息（如果提供）
        if price_info:
            new_trendline['price_info'] = price_info
        if candle_data:
            new_trendline['candle_data'] = json.dumps(candle_data) if not isinstance(candle_data, str) else candle_data
        if end_price_info:
            new_trendline['end_price_info'] = end_price_info
        if end_candle_data:
            new_trendline['end_candle_data'] = json.dumps(end_candle_data) if not isinstance(end_candle_data, str) else end_candle_data

        # 加载现有数据
        df = self._load_trendlines()

        # 添加新趋势线
        new_row = pd.DataFrame([new_trendline])
        df = pd.concat([df, new_row], ignore_index=True)

        # 保存数据
        self._save_trendlines(df)

        return trendline_id

    def get_trendline(self, trendline_id: str) -> Optional[Dict]:
        """获取单个趋势线配置"""
        df = self._load_trendlines()
        row = df[df['id'] == trendline_id]

        if not row.empty and row.iloc[0]['status'] != 'deleted':
            return row.iloc[0].to_dict()
        return None

    def get_all_trendlines(self, symbol: Optional[str] = None) -> List[Dict]:
        """获取所有趋势线配置"""
        df = self._load_trendlines()
        df = df[df['status'] != 'deleted']  # 只过滤已删除的记录

        if symbol:
            df = df[df['symbol'] == symbol]

        return df.to_dict('records')

    def update_trendline(self, trendline_id: str, **kwargs) -> bool:
        """更新趋势线配置"""
        if not kwargs:
            return False

        df = self._load_trendlines()

        # 查找要更新的行
        mask = df['id'] == trendline_id
        if not mask.any():
            return False

        # 更新字段
        for key, value in kwargs.items():
            if key in df.columns:
                # 确保数据类型兼容
                if key in ['start_price', 'end_price'] and isinstance(value, str):
                    # 价格字段确保为float类型
                    df.loc[mask, key] = float(value)
                else:
                    df.loc[mask, key] = value

        # 更新修改时间
        df.loc[mask, 'updated_at'] = datetime.now().isoformat()

        # 保存数据
        self._save_trendlines(df)

        return True

    def delete_trendline(self, trendline_id: str) -> bool:
        """删除趋势线（软删除）"""
        return self.update_trendline(trendline_id, status='deleted')

    def get_active_trendlines(self, symbol: Optional[str] = None) -> List[Dict]:
        """获取活跃的趋势线配置"""
        df = self._load_trendlines()
        df = df[df['status'] == 'active']

        if symbol:
            df = df[df['symbol'] == symbol]

        return df.to_dict('records')

    def calculate_trendline_values(self, trendline_id: str, df: pd.DataFrame) -> pd.Series:
        """计算趋势线值（复用Signals.define_trendline）"""
        trendline = self.get_trendline(trendline_id)
        if not trendline:
            raise ValueError(f"趋势线不存在: {trendline_id}")

        start_point = [trendline['start_time'], trendline['start_price']]
        end_point = [trendline['end_time'], trendline['end_price']]

        return define_trendline(df, start_point, end_point)

    def check_breakout_signal(self, trendline_id: str, df: pd.DataFrame) -> Optional[int]:
        """检查突破信号（复用Signals.monitor_breakout）"""
        trendline = self.get_trendline(trendline_id)
        if not trendline or trendline['status'] != 'active':
            return None

        # 计算趋势线值
        trendline_values = self.calculate_trendline_values(trendline_id, df)

        # 检查突破信号
        breakout = monitor_breakout(df, trendline_values)

        # 如果检测到突破，记录日志
        if breakout is not None and breakout != 0:
            self._log_breakout(trendline_id, breakout, df)

        # 只返回符合方向的信号
        if trendline['direction'] == 1 and breakout == 1:
            return 1  # 多头突破
        elif trendline['direction'] == -1 and breakout == -1:
            return -1  # 空头跌破

        return None

    def manual_check_breakout(self, trendline_id: str, df: pd.DataFrame) -> Dict:
        """手动检查突破信号并返回详细信息"""
        trendline = self.get_trendline(trendline_id)
        if not trendline or trendline['status'] != 'active':
            return {'has_signal': False, 'message': '趋势线不存在或未激活'}

        try:
            # 计算趋势线值
            trendline_values = self.calculate_trendline_values(trendline_id, df)

            # 检查突破信号
            breakout = monitor_breakout(df, trendline_values)

            # 获取最新价格和趋势线值
            current_price = df['close'].iloc[-1]
            current_trend_value = trendline_values.iloc[-1] if not pd.isna(trendline_values.iloc[-1]) else None

            result = {
                'has_signal': False,
                'current_price': current_price,
                'trendline_value': current_trend_value,
                'trendline_info': trendline,
                'breakout_type': None
            }

            if breakout is not None and breakout != 0:
                # 如果检测到突破，记录日志
                self._log_breakout(trendline_id, breakout, df)

                result['has_signal'] = True
                result['breakout_type'] = 'breakout' if breakout == 1 else 'breakdown'

                # 只返回符合方向的信号
                if trendline['direction'] == 1 and breakout == 1:
                    result['signal'] = 1  # 多头突破
                    result['message'] = '检测到多头突破信号！'
                elif trendline['direction'] == -1 and breakout == -1:
                    result['signal'] = -1  # 空头跌破
                    result['message'] = '检测到空头跌破信号！'
                else:
                    result['message'] = f'检测到{("突破" if breakout == 1 else "跌破")}，但不符合策略方向'
            else:
                result['message'] = '当前无突破信号'

            return result

        except Exception as e:
            return {'has_signal': False, 'message': f'检查失败: {str(e)}'}

    def _log_breakout(self, trendline_id: str, signal_type: int, df: pd.DataFrame):
        """记录突破日志"""
        log_id = str(uuid.uuid4())
        detected_at = datetime.now().isoformat()

        # 获取最新价格和趋势线值
        current_price = df['close'].iloc[-1]
        trendline_values = self.calculate_trendline_values(trendline_id, df)
        trendline_value = trendline_values.iloc[-1]

        signal_name = 'breakout' if signal_type == 1 else 'breakdown'

        new_log = {
            'id': log_id,
            'trendline_id': trendline_id,
            'signal_type': signal_name,
            'price': current_price,
            'trendline_value': trendline_value,
            'detected_at': detected_at
        }

        # 加载现有日志
        logs_df = self._load_logs()

        # 添加新日志
        if logs_df.empty:
            logs_df = pd.DataFrame([new_log])
        else:
            new_row = pd.DataFrame([new_log])
            logs_df = pd.concat([logs_df, new_row], ignore_index=True)

        # 保存日志
        self._save_logs(logs_df)

    def get_monitor_logs(self, trendline_id: Optional[str] = None,
                        limit: int = 100) -> List[Dict]:
        """获取监测日志"""
        logs_df = self._load_logs()

        if trendline_id:
            logs_df = logs_df[logs_df['trendline_id'] == trendline_id]

        # 按时间倒序排列并限制数量
        logs_df = logs_df.sort_values('detected_at', ascending=False).head(limit)

        return logs_df.to_dict('records')

    def export_trendlines(self, file_path: str):
        """导出趋势线配置到JSON文件"""
        trendlines = self.get_all_trendlines()
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(trendlines, f, ensure_ascii=False, indent=2)

    def import_trendlines(self, file_path: str) -> int:
        """从JSON文件导入趋势线配置"""
        with open(file_path, 'r', encoding='utf-8') as f:
            trendlines = json.load(f)

        imported_count = 0
        for trendline in trendlines:
            # 检查是否已存在
            existing = self.get_trendline(trendline['id'])
            if not existing:
                self.create_trendline(
                    name=trendline['name'],
                    symbol=trendline['symbol'],
                    start_point=[trendline['start_time'], trendline['start_price']],
                    end_point=[trendline['end_time'], trendline['end_price']],
                    direction=trendline['direction']
                )
                imported_count += 1

        return imported_count


# 趋势线配置验证
def validate_trendline_config(start_point: List, end_point: List,
                            df: pd.DataFrame) -> Dict[str, Any]:
    """验证趋势线配置的有效性"""
    errors = []
    warnings = []

    # 检查格式
    if (len(start_point) != 2 or len(end_point) != 2 or
        not isinstance(start_point[0], str) or not isinstance(start_point[1], (int, float))):
        errors.append("起点和终点格式应为 [时间字符串, 价格数值]")

    # 检查时间格式
    try:
        start_time = pd.to_datetime(start_point[0])
        end_time = pd.to_datetime(end_point[0])
    except:
        errors.append("时间格式错误，请使用 YYYY-MM-DD HH:MM:SS 格式")

    # 检查时间顺序
    if start_time >= end_time:
        errors.append("起点时间必须早于终点时间")

    # 检查价格
    if start_point[1] <= 0 or end_point[1] <= 0:
        errors.append("价格必须大于0")

    # 检查时间点是否在数据范围内
    if 'candle_begin_time_GMT8' in df.columns:
        df_time = pd.to_datetime(df['candle_begin_time_GMT8'])
        if start_time < df_time.min() or end_time > df_time.max():
            warnings.append("趋势线时间点超出K线数据范围")

    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings
    }


# 示例使用
if __name__ == "__main__":
    # 创建趋势线管理器
    manager = TrendlineManager()

    # 示例：创建趋势线
    trendline_id = manager.create_trendline(
        name="SOL上升趋势线",
        symbol="SOL-USDT-SWAP",
        start_point=["2025-05-09 18:00:00", 174.25],
        end_point=["2025-05-14 06:00:00", 183.44],
        direction=1  # 多头突破
    )

    print(f"创建趋势线成功: {trendline_id}")

    # 获取所有趋势线
    trendlines = manager.get_all_trendlines()
    print(f"当前趋势线数量: {len(trendlines)}")

    # 获取活跃趋势线
    active_trendlines = manager.get_active_trendlines()
    print(f"活跃趋势线数量: {len(active_trendlines)}")

    # 导出配置
    manager.export_trendlines("trendlines_backup.json")
    print("配置已导出到 trendlines_backup.json")