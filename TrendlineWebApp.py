"""
趋势线监测系统 - Web应用
基于Flask的Web界面，提供趋势线管理和可视化功能
"""

from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import json
import uuid
import hashlib
import time
from datetime import datetime, timedelta
from TrendlineManager import TrendlineManager, validate_trendline_config
from TrendlineMonitor import TrendlineMonitor, get_global_monitor
from Function import ccxt_fetch_candle_data, fetch_okex_symbol_history_candle_data
from cryptography.fernet import Fernet
import base64
import pandas as pd
import ccxt
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import hashlib
import os
import glob
from config_constants import OKEX_READONLY_CONFIG, WEB_SECRET_KEY


app = Flask(__name__)
app.config['SECRET_KEY'] = WEB_SECRET_KEY

# 简单的CORS支持
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

# OPTIONS请求处理
@app.route('/', methods=['OPTIONS'])
@app.route('/api/<path:path>', methods=['OPTIONS'])
def options_handler(path=None):
    return '', 200

# 全局实例
manager = TrendlineManager()
monitor = get_global_monitor()

# 公共的exchange配置
EXCHANGE_CONFIG = OKEX_READONLY_CONFIG

# 简单的认证系统
USERNAME = "trendline"
PASSWORD_HASH = hashlib.sha256("trendline".encode()).hexdigest()

# 登录失败记录
login_attempts = {}

# 防重复提交记录 (基于IP和时间的防抖)
submit_timestamps = {}

# StochRSI数据配置
STOCHRSI_CONFIG = {
    'symbols': ['SOL-USDT-SWAP','BTC-USDT-SWAP', 'ETH-USDT-SWAP'],
    'timeframes': ['1W', '1D', '4H', '15m'],
    'data_dir': 'data/stochrsi'
}

def get_stochrsi_data(symbol, timeframe):
    """获取指定币种和时间周期的StochRSI数据"""
    try:
        # 构造文件路径
        base_filename = f"{STOCHRSI_CONFIG['data_dir']}/stochrsi_{symbol}_{timeframe}"

        # 读取原始数据
        main_file = f"{base_filename}.csv"
        if not os.path.exists(main_file):
            return None

        df_main = pd.read_csv(main_file)
        if df_main.empty:
            return None

        # 获取最新的StochRSI值
        latest_row = df_main.iloc[-1]
        latest_stochrsi = latest_row.get('stochrsi', None)

        # 读取背离信号数据
        tbl_file = f"{base_filename}_tbl.csv"
        dbl_file = f"{base_filename}_dbl.csv"

        latest_tbl = None
        latest_dbl = None

        if os.path.exists(tbl_file):
            df_tbl = pd.read_csv(tbl_file)
            if not df_tbl.empty:
                latest_tbl = df_tbl.iloc[-1]

        if os.path.exists(dbl_file):
            df_dbl = pd.read_csv(dbl_file)
            if not df_dbl.empty:
                latest_dbl = df_dbl.iloc[-1]

        # 检查TBL信号是否在当前周期或上一个周期
        tbl_has_signal = False
        tbl_timestamp = ''
        tbl_stochrsi = None
        tbl_price = None

        if latest_tbl is not None and pd.notna(latest_tbl.get('bl', -1)) and latest_tbl.get('bl', -1) == -1:
            tbl_signal_time = str(latest_tbl.get('datetime', ''))
            latest_kline_time = str(latest_row.get('datetime', ''))

            # 计算时间差异
            try:
                from datetime import datetime, timedelta
                tbl_dt = datetime.strptime(tbl_signal_time, '%Y-%m-%d %H:%M:%S')
                latest_dt = datetime.strptime(latest_kline_time, '%Y-%m-%d %H:%M:%S')

                # 根据时间周期判断时间间隔
                time_diff = latest_dt - tbl_dt

                # 定义各时间周期的间隔（小时）
                timeframe_intervals = {
                    '5m': 0.083,    # 5分钟
                    '15m': 0.25,    # 15分钟
                    '30m': 0.5,     # 30分钟
                    '1H': 1,        # 1小时
                    '2H': 2,        # 2小时
                    '4H': 4,        # 4小时
                    '6H': 6,        # 6小时
                    '12H': 12,      # 12小时
                    '1D': 24,       # 1天
                    '3D': 72,       # 3天
                    '1W': 168,      # 1周
                }

                interval = timeframe_intervals.get(timeframe, 4)  # 默认4小时

                # 如果背离信号时间在当前周期的上一个周期内，就标注信号
                if timedelta(0) <= time_diff <= timedelta(hours=interval):
                    tbl_has_signal = True
                    tbl_timestamp = tbl_signal_time
                    tbl_stochrsi = float(latest_tbl.get('stochrsi', 0)) if pd.notna(latest_tbl.get('stochrsi')) else None
                    tbl_price = float(latest_tbl.get('close', 0)) if pd.notna(latest_tbl.get('close')) else None
            except:
                # 如果时间解析失败，使用原来的逻辑
                if tbl_signal_time == latest_kline_time:
                    tbl_has_signal = True
                    tbl_timestamp = tbl_signal_time
                    tbl_stochrsi = float(latest_tbl.get('stochrsi', 0)) if pd.notna(latest_tbl.get('stochrsi')) else None
                    tbl_price = float(latest_tbl.get('close', 0)) if pd.notna(latest_tbl.get('close')) else None

        # 检查DBL信号是否在当前周期或上一个周期
        dbl_has_signal = False
        dbl_timestamp = ''
        dbl_stochrsi = None
        dbl_price = None

        if latest_dbl is not None and pd.notna(latest_dbl.get('dbl', -1)) and latest_dbl.get('dbl', -1) == 1:
            dbl_signal_time = str(latest_dbl.get('datetime', ''))
            latest_kline_time = str(latest_row.get('datetime', ''))

            # 计算时间差异
            try:
                from datetime import datetime, timedelta
                dbl_dt = datetime.strptime(dbl_signal_time, '%Y-%m-%d %H:%M:%S')
                latest_dt = datetime.strptime(latest_kline_time, '%Y-%m-%d %H:%M:%S')

                # 根据时间周期判断时间间隔
                time_diff = latest_dt - dbl_dt

                # 定义各时间周期的间隔（小时）
                timeframe_intervals = {
                    '5m': 0.083,    # 5分钟
                    '15m': 0.25,    # 15分钟
                    '30m': 0.5,     # 30分钟
                    '1H': 1,        # 1小时
                    '2H': 2,        # 2小时
                    '4H': 4,        # 4小时
                    '6H': 6,        # 6小时
                    '12H': 12,      # 12小时
                    '1D': 24,       # 1天
                    '3D': 72,       # 3天
                    '1W': 168,      # 1周
                }

                interval = timeframe_intervals.get(timeframe, 4)  # 默认4小时

                # 如果背离信号时间在当前周期的上一个周期内，就标注信号
                if timedelta(0) <= time_diff <= timedelta(hours=interval):
                    dbl_has_signal = True
                    dbl_timestamp = dbl_signal_time
                    dbl_stochrsi = float(latest_dbl.get('stochrsi', 0)) if pd.notna(latest_dbl.get('stochrsi')) else None
                    dbl_price = float(latest_dbl.get('close', 0)) if pd.notna(latest_dbl.get('close')) else None
            except:
                # 如果时间解析失败，使用原来的逻辑
                if dbl_signal_time == latest_kline_time:
                    dbl_has_signal = True
                    dbl_timestamp = dbl_signal_time
                    dbl_stochrsi = float(latest_dbl.get('stochrsi', 0)) if pd.notna(latest_dbl.get('stochrsi')) else None
                    dbl_price = float(latest_dbl.get('close', 0)) if pd.notna(latest_dbl.get('close')) else None

        # 检查是否有波动率数据
        volatility_data = {
            'current_volatility': None,
            'rolling_volatility': None,
            'sigma_level': '',
            'trend': 'stable',
            'historical_mean': None,
            'historical_std': None
        }

        # 如果CSV文件包含波动率列，则提取数据
        if 'volatility' in df_main.columns:
            # 处理 NaN 值，确保 JSON 序列化兼容
            def safe_get_float(key, default=None):
                value = latest_row.get(key, default)
                try:
                    if pd.isna(value):
                        return default
                    return float(value) if value is not None else default
                except (TypeError, ValueError):
                    return default

          # 处理 sigma_level 字符串中的无效值
            def safe_get_string(key, default=''):
                value = latest_row.get(key, default)
                if pd.isna(value):
                    return default
                if value in ['normal', 'nan', 'NaN', 'None']:
                    return default
                return str(value) if value is not None else default

            volatility_data = {
                'current_volatility': safe_get_float('volatility'),
                'rolling_volatility': safe_get_float('volatility_rolling'),
                'sigma_level': safe_get_string('volatility_sigma_level'),
                'trend': safe_get_string('volatility_trend', 'stable'),
                'historical_mean': safe_get_float('volatility_historical_mean'),
                'historical_std': safe_get_float('volatility_historical_std')
            }

        return {
            'symbol': symbol,
            'timeframe': timeframe,
            'latest_stochrsi': float(latest_stochrsi) if pd.notna(latest_stochrsi) else None,
            'latest_timestamp': latest_row.get('datetime', ''),
            'tbl_signal': {
                'has_signal': bool(tbl_has_signal),
                'timestamp': tbl_timestamp,
                'stochrsi': tbl_stochrsi,
                'price': tbl_price
            },
            'dbl_signal': {
                'has_signal': bool(dbl_has_signal),
                'timestamp': dbl_timestamp,
                'stochrsi': dbl_stochrsi,
                'price': dbl_price
            },
            'volatility': volatility_data  # 新增波动率数据
        }
    except Exception as e:
        print(f"获取StochRSI数据失败 {symbol} {timeframe}: {str(e)}")
        return None

def get_divergence_history(symbol, timeframe, limit=10):
    """获取背离信号历史"""
    try:
        base_filename = f"{STOCHRSI_CONFIG['data_dir']}/stochrsi_{symbol}_{timeframe}"

        # 读取背离信号数据
        tbl_file = f"{base_filename}_tbl.csv"
        dbl_file = f"{base_filename}_dbl.csv"

        divergence_data = []

        # 处理TBL信号
        if os.path.exists(tbl_file):
            df_tbl = pd.read_csv(tbl_file)
            if not df_tbl.empty:
                # 由于CSV格式问题，bl列为空，使用turn列来判断
                # turn列值为-1.0的行表示TBL信号
                valid_tbl = df_tbl[df_tbl['turn'] == -1.0].tail(limit)
                for _, row in valid_tbl.iterrows():
                    # 提取波动率数据 - 正确处理空字符串和NaN值
                    volatility_data = {}
                    volatility_val = row.get('volatility', '')

                    # 检查是否有有效的波动率数据（不是空字符串也不是NaN）
                    if 'volatility' in row and pd.notna(volatility_val) and str(volatility_val).strip() != '':
                        try:
                            volatility_data = {
                                'current_volatility': float(volatility_val),
                                'rolling_volatility': float(row.get('volatility_rolling', 0)) if pd.notna(row.get('volatility_rolling')) and str(row.get('volatility_rolling', '')).strip() != '' else None,
                                'sigma_level': str(row.get('volatility_sigma_level', '')).strip() if pd.notna(row.get('volatility_sigma_level')) and str(row.get('volatility_sigma_level', '')).strip() != '' else '',
                                'trend': str(row.get('volatility_trend', 'stable')).strip() if pd.notna(row.get('volatility_trend')) and str(row.get('volatility_trend', '')).strip() != '' else 'stable',
                                'historical_mean': float(row.get('volatility_historical_mean', 0)) if pd.notna(row.get('volatility_historical_mean')) and str(row.get('volatility_historical_mean', '')).strip() != '' else None,
                                'historical_std': float(row.get('volatility_historical_std', 0)) if pd.notna(row.get('volatility_historical_std')) and str(row.get('volatility_historical_std', '')).strip() != '' else None
                            }
                        except (ValueError, TypeError) as e:
                            # 如果转换失败，创建空的波动率数据
                            volatility_data = {}
                            print(f"波动率数据转换失败 TBL: {str(e)}, 原值: {volatility_val}")

                    divergence_data.append({
                        'type': 'TBL',
                        'timestamp': row.get('datetime', ''),
                        'stochrsi': float(row.get('stochrsi', 0)) if pd.notna(row.get('stochrsi')) else None,
                        'price': float(row.get('close', 0)) if pd.notna(row.get('close')) else None,
                        'signal_strength': '底部背离',
                        'volatility': volatility_data
                    })

        # 处理DBL信号
        if os.path.exists(dbl_file):
            df_dbl = pd.read_csv(dbl_file)
            if not df_dbl.empty:
                # 由于CSV格式问题，dbl列为空，使用turn列来判断
                # turn列值为1.0的行表示DBL信号
                valid_dbl = df_dbl[df_dbl['turn'] == 1.0].tail(limit)
                for _, row in valid_dbl.iterrows():
                    # 提取波动率数据 - 正确处理空字符串和NaN值
                    volatility_data = {}
                    volatility_val = row.get('volatility', '')

                    # 检查是否有有效的波动率数据（不是空字符串也不是NaN）
                    if 'volatility' in row and pd.notna(volatility_val) and str(volatility_val).strip() != '':
                        try:
                            volatility_data = {
                                'current_volatility': float(volatility_val),
                                'rolling_volatility': float(row.get('volatility_rolling', 0)) if pd.notna(row.get('volatility_rolling')) and str(row.get('volatility_rolling', '')).strip() != '' else None,
                                'sigma_level': str(row.get('volatility_sigma_level', '')).strip() if pd.notna(row.get('volatility_sigma_level')) and str(row.get('volatility_sigma_level', '')).strip() != '' else '',
                                'trend': str(row.get('volatility_trend', 'stable')).strip() if pd.notna(row.get('volatility_trend')) and str(row.get('volatility_trend', '')).strip() != '' else 'stable',
                                'historical_mean': float(row.get('volatility_historical_mean', 0)) if pd.notna(row.get('volatility_historical_mean')) and str(row.get('volatility_historical_mean', '')).strip() != '' else None,
                                'historical_std': float(row.get('volatility_historical_std', 0)) if pd.notna(row.get('volatility_historical_std')) and str(row.get('volatility_historical_std', '')).strip() != '' else None
                            }
                        except (ValueError, TypeError) as e:
                            # 如果转换失败，创建空的波动率数据
                            volatility_data = {}
                            print(f"波动率数据转换失败 DBL: {str(e)}, 原值: {volatility_val}")

                    divergence_data.append({
                        'type': 'DBL',
                        'timestamp': row.get('datetime', ''),
                        'stochrsi': float(row.get('stochrsi', 0)) if pd.notna(row.get('stochrsi')) else None,
                        'price': float(row.get('close', 0)) if pd.notna(row.get('close')) else None,
                        'signal_strength': '顶部背离',
                        'volatility': volatility_data
                    })

        # 按时间排序，最新的在前
        divergence_data.sort(key=lambda x: x['timestamp'], reverse=True)
        return divergence_data[:limit]

    except Exception as e:
        print(f"获取背离历史失败 {symbol} {timeframe}: {str(e)}")
        return []

def is_rate_limited(client_ip, limit_seconds=2):
    """检查是否被频率限制"""
    current_time = time.time()
    if client_ip in submit_timestamps:
        last_submit = submit_timestamps[client_ip]
        if current_time - last_submit < limit_seconds:
            return True
    return False

def update_submit_timestamp(client_ip):
    """更新提交时间戳"""
    submit_timestamps[client_ip] = time.time()

def generate_key():
    """生成加密密钥"""
    return Fernet.generate_key()

def encrypt_data(data, key):
    """加密数据"""
    f = Fernet(key)
    return f.encrypt(data.encode()).decode()

def decrypt_data(encrypted_data, key):
    """解密数据 - 兼容前端XOR加密"""
    try:
        # 解码base64数据
        import base64
        encrypted_bytes = base64.b64decode(encrypted_data)

        # XOR解密
        result = ''
        for i, byte in enumerate(encrypted_bytes):
            key_char = ord(key[i % len(key)])
            decrypted_char = byte ^ key_char
            result += chr(decrypted_char)

        return result
    except Exception as e:
        # 如果XOR解密失败，尝试Fernet解密（向后兼容）
        try:
            f = Fernet(key)
            return f.decrypt(encrypted_data.encode()).decode()
        except:
            raise e

def is_ip_blocked(ip):
    """检查IP是否被阻止"""
    if ip in login_attempts:
        attempts = login_attempts[ip]
        if attempts['count'] >= 3:
            # 检查是否还在阻止期内
            if time.time() - attempts['last_attempt'] < 300:  # 5分钟
                return True
            else:
                # 重置计数器
                del login_attempts[ip]
    return False

def record_failed_attempt(ip):
    """记录失败尝试"""
    if ip not in login_attempts:
        login_attempts[ip] = {'count': 0, 'last_attempt': 0}

    login_attempts[ip]['count'] += 1
    login_attempts[ip]['last_attempt'] = time.time()

def require_auth(f):
    """认证装饰器"""
    def decorated_function(*args, **kwargs):
        if not session.get('authenticated'):
            return jsonify({'success': False, 'message': '需要登录', 'redirect': '/login'}), 401
        return f(*args, **kwargs)
    decorated_function.__name__ = f.__name__
    return decorated_function

@app.route('/')
def index():
    """主页 - 趋势线管理界面"""
    if not session.get('authenticated'):
        return redirect(url_for('login'))
    return render_template('index.html', symbols=STOCHRSI_CONFIG['symbols'])

@app.route('/stochrsi')
def stochrsi_dashboard():
    """StochRSI监控面板"""
    if not session.get('authenticated'):
        return redirect(url_for('login'))
    return render_template('stochrsi_dashboard.html', symbols=STOCHRSI_CONFIG['symbols'])

@app.route('/stochrsi/test')
def stochrsi_dashboard_test():
    """StochRSI监控面板测试页面（无需认证）"""
    return render_template('stochrsi_dashboard.html', symbols=STOCHRSI_CONFIG['symbols'])

@app.route('/login', methods=['GET', 'POST'])
def login():
    """登录页面"""
    if request.method == 'GET':
        return render_template('login.html')

    if request.method == 'POST':
        try:
            data = request.get_json()

            # 获取客户端IP
            client_ip = request.remote_addr

            # 检查防抖限制
            if is_rate_limited(client_ip, limit_seconds=2):
                return jsonify({
                    'success': False,
                    'message': '提交过于频繁，请稍后再试'
                }), 429

            # 更新提交时间戳
            update_submit_timestamp(client_ip)

            # 检查是否被阻止
            if is_ip_blocked(client_ip):
                remaining_time = 300 - int(time.time() - login_attempts[client_ip]['last_attempt'])
                return jsonify({
                    'success': False,
                    'message': f'登录失败次数过多，请等待{remaining_time // 60}分钟后重试'
                }), 429

            # 解密数据
            encrypted_key = data.get('key')
            encrypted_username = data.get('username')
            encrypted_password = data.get('password')

            if not all([encrypted_key, encrypted_username, encrypted_password]):
                return jsonify({'success': False, 'message': '数据不完整'}), 400

            # 解密
            try:
                # 前端使用btoa()进行标准base64编码，不是URL安全编码
                key = base64.b64decode(encrypted_key.encode()).decode('utf-8')
                username = decrypt_data(encrypted_username, key)
                password = decrypt_data(encrypted_password, key)
            except Exception as e:
                return jsonify({'success': False, 'message': '解密失败'}), 400

            # 验证用户名密码
            if username == USERNAME and hashlib.sha256(password.encode()).hexdigest() == PASSWORD_HASH:
                # 登录成功，清除失败记录
                if client_ip in login_attempts:
                    del login_attempts[client_ip]

                session['authenticated'] = True
                session['username'] = username
                session.permanent = True
                return jsonify({'success': True, 'message': '登录成功'})
            else:
                # 登录失败，记录尝试
                record_failed_attempt(client_ip)
                remaining_attempts = 3 - login_attempts[client_ip]['count']

                if remaining_attempts <= 0:
                    return jsonify({
                        'success': False,
                        'message': '登录失败次数过多，请等待5分钟后重试'
                    }), 429
                else:
                    return jsonify({
                        'success': False,
                        'message': f'用户名或密码错误，剩余尝试次数：{remaining_attempts}'
                    }), 401

        except Exception as e:
            return jsonify({'success': False, 'message': f'登录失败: {str(e)}'}), 500

@app.route('/logout', methods=['POST'])
def logout():
    """登出"""
    session.clear()
    return jsonify({'success': True, 'message': '已退出登录'})

@app.route('/api/trendlines', methods=['GET'])
@require_auth
def get_trendlines():
    """获取所有趋势线"""
    symbol = request.args.get('symbol')
    trendlines = manager.get_all_trendlines(symbol)

    # 转换格式以匹配前端期望
    formatted_trendlines = []
    for tl in trendlines:
        formatted_tl = {
            'id': tl['id'],
            'name': tl['name'],
            'symbol': tl['symbol'],
            'startPoint': {
                'time': tl['start_time'],
                'price': float(tl['start_price'])
            },
            'endPoint': {
                'time': tl['end_time'],
                'price': float(tl['end_price'])
            },
            'direction': tl['direction'],
            'enabled': tl['status'] == 'active',
            'createdAt': tl['created_at'],
            'updatedAt': tl['updated_at']
        }

        # 添加价格信息字段（兼容前端的数据结构）
        if 'price_info' in tl and tl['price_info'] and pd.notna(tl['price_info']):
            formatted_tl['startPoint']['priceInfo'] = tl['price_info']
        if 'candle_data' in tl and tl['candle_data'] and pd.notna(tl['candle_data']):
            try:
                candle_data = json.loads(tl['candle_data']) if isinstance(tl['candle_data'], str) else tl['candle_data']
                if candle_data and isinstance(candle_data, dict):
                    candle_data = {k: v for k, v in candle_data.items() if pd.notna(v)}
                formatted_tl['startPoint']['candleData'] = candle_data if candle_data else None
            except:
                formatted_tl['startPoint']['candleData'] = None

        if 'end_price_info' in tl and tl['end_price_info'] and pd.notna(tl['end_price_info']):
            formatted_tl['endPoint']['priceInfo'] = tl['end_price_info']
        if 'end_candle_data' in tl and tl['end_candle_data'] and pd.notna(tl['end_candle_data']):
            try:
                candle_data = json.loads(tl['end_candle_data']) if isinstance(tl['end_candle_data'], str) else tl['end_candle_data']
                if candle_data and isinstance(candle_data, dict):
                    candle_data = {k: v for k, v in candle_data.items() if pd.notna(v)}
                formatted_tl['endPoint']['candleData'] = candle_data if candle_data else None
            except:
                formatted_tl['endPoint']['candleData'] = None

        formatted_trendlines.append(formatted_tl)

    return jsonify({'success': True, 'data': formatted_trendlines})

@app.route('/api/trendlines', methods=['POST'])
@require_auth
def create_trendline():
    """创建趋势线"""
    try:
        data = request.json

        # 验证数据
        required_fields = ['name', 'symbol', 'startPoint', 'endPoint', 'direction']
        for field in required_fields:
            if field not in data:
                return jsonify({'success': False, 'message': f'缺少字段: {field}'})

        # 转换前端格式到后端格式
        start_point = [data['startPoint']['time'], data['startPoint']['price']]
        end_point = [data['endPoint']['time'], data['endPoint']['price']]

        # 提取价格信息
        start_price_info = data['startPoint'].get('priceInfo')
        start_candle_data = data['startPoint'].get('candleData')
        end_price_info = data['endPoint'].get('priceInfo')
        end_candle_data = data['endPoint'].get('candleData')

        # 创建趋势线
        trendline_id = manager.create_trendline(
            name=data['name'],
            symbol=data['symbol'],
            start_point=start_point,
            end_point=end_point,
            direction=data['direction'],
            price_info=start_price_info,
            candle_data=start_candle_data,
            end_price_info=end_price_info,
            end_candle_data=end_candle_data
        )

        # 返回创建的趋势线数据
        trendline = manager.get_trendline(trendline_id)
        response_data = {
            'id': trendline['id'],
            'name': trendline['name'],
            'symbol': trendline['symbol'],
            'startPoint': {
                'time': trendline['start_time'],
                'price': float(trendline['start_price'])
            },
            'endPoint': {
                'time': trendline['end_time'],
                'price': float(trendline['end_price'])
            },
            'direction': trendline['direction'],
            'enabled': trendline['status'] == 'active',
            'createdAt': trendline['created_at'],
            'updatedAt': trendline['updated_at']
        }

        return jsonify({
            'success': True,
            'data': response_data,
            'message': '趋势线创建成功'
        })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/trendlines/<trendline_id>', methods=['GET'])
@require_auth
def get_trendline(trendline_id):
    """获取单个趋势线"""
    try:
        trendline = manager.get_trendline(trendline_id)
        if trendline:
            response_data = {
                'id': trendline['id'],
                'name': trendline['name'],
                'symbol': trendline['symbol'],
                'startPoint': {
                    'time': trendline['start_time'],
                    'price': float(trendline['start_price'])
                },
                'endPoint': {
                    'time': trendline['end_time'],
                    'price': float(trendline['end_price'])
                },
                'direction': trendline['direction'],
                'enabled': trendline['status'] == 'active',
                'createdAt': trendline['created_at'],
                'updatedAt': trendline['updated_at']
            }
            return jsonify({'success': True, 'data': response_data})
        else:
            return jsonify({'success': False, 'message': '趋势线不存在'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/trendlines/<trendline_id>', methods=['PUT'])
@require_auth
def update_trendline(trendline_id):
    """更新趋势线"""
    try:
        data = request.json

        # 转换前端格式到后端格式
        update_data = {}

        if 'name' in data:
            update_data['name'] = data['name']
        if 'symbol' in data:
            update_data['symbol'] = data['symbol']
        if 'direction' in data:
            update_data['direction'] = data['direction']
        if 'enabled' in data:
            update_data['status'] = 'active' if data['enabled'] else 'inactive'

        if 'startPoint' in data:
            update_data['start_time'] = data['startPoint']['time']
            update_data['start_price'] = data['startPoint']['price']
            if 'priceInfo' in data['startPoint']:
                update_data['price_info'] = data['startPoint']['priceInfo']
            if 'candleData' in data['startPoint']:
                update_data['candle_data'] = data['startPoint']['candleData']

        if 'endPoint' in data:
            update_data['end_time'] = data['endPoint']['time']
            update_data['end_price'] = data['endPoint']['price']
            if 'priceInfo' in data['endPoint']:
                update_data['end_price_info'] = data['endPoint']['priceInfo']
            if 'candleData' in data['endPoint']:
                update_data['end_candle_data'] = data['endPoint']['candleData']

        # 更新趋势线
        success = manager.update_trendline(trendline_id, **update_data)

        if success:
            return jsonify({'success': True, 'message': '趋势线更新成功'})
        else:
            return jsonify({'success': False, 'message': '趋势线不存在或更新失败'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/trendlines/<trendline_id>', methods=['DELETE'])
@require_auth
def delete_trendline(trendline_id):
    """删除趋势线"""
    try:
        success = manager.delete_trendline(trendline_id)
        if success:
            return jsonify({'success': True, 'message': '趋势线删除成功'})
        else:
            return jsonify({'success': False, 'message': '趋势线不存在'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/trendlines/<trendline_id>/data', methods=['GET'])
@require_auth
def get_trendline_data(trendline_id):
    """获取趋势线数据（包含K线和趋势线值）"""
    try:
        monitor = get_global_monitor()
        data = monitor.get_trendline_data(trendline_id)
        if data:
            return jsonify({'success': True, 'data': data})
        else:
            return jsonify({'success': False, 'message': '无法获取趋势线数据'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/trendlines/<trendline_id>/check', methods=['POST'])
@require_auth
def check_trendline(trendline_id):
    """立即检查趋势线突破信号（手动检查）"""
    try:
        monitor = get_global_monitor()
        result = monitor.check_trendline_breakout_detailed(trendline_id)

        if result is None:
            return jsonify({'success': False, 'message': '无法检查趋势线突破'})

        return jsonify({
            'success': True,
            'data': result,
            'message': '检查完成'
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/monitor/status', methods=['GET'])
@require_auth
def get_monitor_status():
    """获取系统状态"""
    try:
        status = {
            'monitoring': False,
            'symbols': ['SOL-USDT-SWAP'],
            'time_interval': '5m',
            'active_trendlines_count': len(manager.get_active_trendlines()),
            'candle_cache_status': {},
            'manual_refresh_only': True
        }
        return jsonify({'success': True, 'data': status})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/monitor/refresh', methods=['GET'])
@require_auth
def refresh_candle_data():
    """刷新K线数据（手动刷新）"""
    try:
        symbol = request.args.get('symbol', 'SOL-USDT-SWAP')
        time_interval = request.args.get('time_interval', '5m')
        limit = int(request.args.get('limit', 100))

        df = ccxt_fetch_candle_data(
            ccxt.okx(EXCHANGE_CONFIG),
            symbol,
            time_interval,
            limit
        )

        if not df.empty:
            return jsonify({
                'success': True,
                'message': 'K线数据已刷新',
                'data': df.to_dict('records')
            })
        else:
            return jsonify({
                'success': False,
                'message': '未获取到K线数据'
            })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/logs', methods=['GET'])
@require_auth
def get_logs():
    """获取监测日志"""
    try:
        trendline_id = request.args.get('trendline_id')
        limit = int(request.args.get('limit', 100))
        logs = manager.get_monitor_logs(trendline_id, limit)
        return jsonify({'success': True, 'data': logs})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/candles', methods=['GET'])
@require_auth
def get_candles():
    """获取K线数据"""
    try:
        symbol = request.args.get('symbol', 'SOL-USDT-SWAP')
        time_interval = request.args.get('time_interval', '5m')
        limit = int(request.args.get('limit', 100))

        exchange = ccxt.okx(EXCHANGE_CONFIG)
        df = ccxt_fetch_candle_data(exchange, symbol, time_interval, limit)

        if not df.empty:
            return jsonify({
                'success': True,
                'data': df.to_dict('records')
            })
        else:
            return jsonify({'success': False, 'message': '未获取到K线数据'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/kline/<symbol>')
@require_auth
def get_kline_data(symbol):
    """获取K线数据 - 兼容Lightweight Charts格式"""
    try:
        limit = int(request.args.get('limit', 2000))
        time_interval = '15m'

        exchange = ccxt.okx(EXCHANGE_CONFIG)
        df = fetch_okex_symbol_history_candle_data(exchange, symbol, time_interval, limit)

        if df.empty:
            return jsonify({'error': '未获取到K线数据'})

        df.to_csv(f'./data/{symbol}_{time_interval}.csv', index=False)

        kline_data = []
        for _, row in df.iterrows():
            try:
                if (pd.isna(row['open']) or pd.isna(row['high']) or
                    pd.isna(row['low']) or pd.isna(row['close'])):
                    continue

                dt = pd.to_datetime(row['candle_begin_time_GMT8'])
                if hasattr(dt, 'tz') and dt.tz is not None:
                    dt_utc = dt.tz_convert('UTC')
                    timestamp = int(dt_utc.timestamp())
                else:
                    dt_utc = dt - pd.Timedelta(hours=8)
                    timestamp = int(dt_utc.timestamp())

                open_price = float(row['open'])
                high_price = float(row['high'])
                low_price = float(row['low'])
                close_price = float(row['close'])

                if (high_price >= max(open_price, low_price, close_price) and
                    low_price <= min(open_price, high_price, close_price) and
                    all(price > 0 for price in [open_price, high_price, low_price, close_price])):

                    # kline_data.append({
                    #     'time': timestamp,
                    #     'open': open_price,
                    #     'high': high_price,
                    #     'low': low_price,
                    #     'close': close_price
                    # })
                    kline_data.append(f'{open_price},{high_price},{low_price},{close_price},{timestamp}')
            except Exception as e:
                print(f"跳过无效数据行: {e}, 行数据: {row.to_dict()}")
                continue

        if not kline_data:
            return jsonify({'error': '没有有效的K线数据'})

        print(f"返回 {len(kline_data)} 条有效K线数据")
        return jsonify(kline_data)

    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/validate', methods=['POST'])
@require_auth
def validate_trendline():
    """验证趋势线配置"""
    try:
        data = request.json
        symbol = data.get('symbol', 'SOL-USDT-SWAP')
        start_point = data.get('start_point')
        end_point = data.get('end_point')

        df = ccxt_fetch_candle_data(monitor.exchange, symbol, '5m', 1000)

        if df.empty:
            return jsonify({'success': False, 'message': '无法获取K线数据进行验证'})

        result = validate_trendline_config(start_point, end_point, symbol, df)

        return jsonify({
            'success': True,
            'data': result
        })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/export', methods=['GET'])
@require_auth
def export_trendlines():
    """导出趋势线配置"""
    try:
        file_path = f"data/trendlines_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        manager.export_trendlines(file_path)
        return jsonify({
            'success': True,
            'data': {'file_path': file_path},
            'message': '配置已导出'
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/import', methods=['POST'])
@require_auth
def import_trendlines():
    """导入趋势线配置"""
    try:
        data = request.json
        file_path = data.get('file_path')

        if not file_path:
            return jsonify({'success': False, 'message': '请提供文件路径'})

        imported_count = manager.import_trendlines(file_path)
        return jsonify({
            'success': True,
            'data': {'imported_count': imported_count},
            'message': f'成功导入 {imported_count} 个趋势线配置'
        })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/trendlines/batch-check', methods=['POST'])
@require_auth
def batch_check_trendlines():
    """批量检查趋势线突破"""
    try:
        data = request.json or {}
        symbol = data.get('symbol')

        monitor = get_global_monitor()
        results = monitor.batch_check_trendlines(symbol)

        return jsonify({
            'success': True,
            'data': results,
            'message': f'检查完成，共检查 {len(results)} 条趋势线'
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/symbols/<symbol>/candles', methods=['GET'])
@require_auth
def get_symbol_candles(symbol):
    """获取指定交易对的K线数据"""
    try:
        limit = int(request.args.get('limit', 2000))
        time_interval = request.args.get('time_interval', '15m')

        monitor = get_global_monitor()
        df = monitor.get_latest_candle_data(symbol, limit)

        if df is None:
            return jsonify({'success': False, 'message': '无法获取K线数据'})

        candle_data = []
        for _, row in df.iterrows():
            try:
                dt = pd.to_datetime(row['candle_begin_time_GMT8'])
                if hasattr(dt, 'tz') and dt.tz is not None:
                    dt_utc = dt.tz_convert('UTC')
                    timestamp = int(dt_utc.timestamp())
                else:
                    dt_utc = dt - pd.Timedelta(hours=8)
                    timestamp = int(dt_utc.timestamp())

                candle_data.append({
                    'time': timestamp,
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close'])
                })
            except Exception as e:
                continue

        return jsonify({
            'success': True,
            'data': candle_data,
            'message': f'获取到 {len(candle_data)} 条K线数据'
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/monitor/refresh-data', methods=['POST'])
@require_auth
def refresh_symbol_data():
    """刷新指定交易对的数据"""
    try:
        data = request.json or {}
        symbol = data.get('symbol')
        time_interval = data.get('time_interval', '15m')
        limit = int(data.get('limit', 2000))

        monitor = get_global_monitor()
        monitor.refresh_candle_data(symbol, time_interval, limit)

        return jsonify({
            'success': True,
            'message': f'{symbol or "所有"} 数据刷新完成'
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# StochRSI相关接口 - 测试版本(无认证)
@app.route('/api/stochrsi/overview/test', methods=['GET'])
@require_auth
def get_stochrsi_overview_test():
    """获取StochRSI概览数据 - 测试版本"""
    try:
        overview_data = {}

        for symbol in STOCHRSI_CONFIG['symbols']:
            symbol_data = {}
            for timeframe in STOCHRSI_CONFIG['timeframes']:
                stochrsi_data = get_stochrsi_data(symbol, timeframe)
                if stochrsi_data:
                    symbol_data[timeframe] = stochrsi_data
                else:
                    symbol_data[timeframe] = {
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'latest_stochrsi': None,
                        'latest_timestamp': '',
                        'tbl_signal': {'has_signal': False},
                        'dbl_signal': {'has_signal': False}
                    }
            overview_data[symbol] = symbol_data

        return jsonify({
            'success': True,
            'data': overview_data,
            'message': 'StochRSI概览数据获取成功'
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取StochRSI概览数据失败: {str(e)}'
        })

@app.route('/api/stochrsi/overview', methods=['GET'])
@require_auth
def get_stochrsi_overview():
    """获取StochRSI概览数据"""
    try:
        overview_data = {}

        for symbol in STOCHRSI_CONFIG['symbols']:
            symbol_data = {}
            for timeframe in STOCHRSI_CONFIG['timeframes']:
                stochrsi_data = get_stochrsi_data(symbol, timeframe)
                if stochrsi_data:
                    symbol_data[timeframe] = stochrsi_data
                else:
                    symbol_data[timeframe] = {
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'latest_stochrsi': None,
                        'latest_timestamp': '',
                        'tbl_signal': {'has_signal': False},
                        'dbl_signal': {'has_signal': False}
                    }
            overview_data[symbol] = symbol_data

        return jsonify({
            'success': True,
            'data': overview_data,
            'message': 'StochRSI概览数据获取成功'
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取StochRSI概览数据失败: {str(e)}'
        })

@app.route('/api/stochrsi/divergence/<symbol>/<timeframe>/test', methods=['GET'])
@require_auth
def get_stochrsi_divergence_test(symbol, timeframe):
    """获取特定币种时间周期的背离信号历史 - 测试版本"""
    try:
        # 验证参数
        if symbol not in STOCHRSI_CONFIG['symbols']:
            return jsonify({
                'success': False,
                'message': f'不支持的币种: {symbol}'
            }), 400

        if timeframe not in STOCHRSI_CONFIG['timeframes']:
            return jsonify({
                'success': False,
                'message': f'不支持的时间周期: {timeframe}'
            }), 400

        limit = int(request.args.get('limit', 20))
        divergence_history = get_divergence_history(symbol, timeframe, limit)

        return jsonify({
            'success': True,
            'divergences': divergence_history,
            'data': {
                'symbol': symbol,
                'timeframe': timeframe
            },
            'message': f'获取到 {len(divergence_history)} 个背离信号'
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取背离信号历史失败: {str(e)}'
        })

@app.route('/api/stochrsi/divergence/<symbol>/<timeframe>', methods=['GET'])
@require_auth
def get_stochrsi_divergence(symbol, timeframe):
    """获取特定币种时间周期的背离信号历史"""
    try:
        # 验证参数
        if symbol not in STOCHRSI_CONFIG['symbols']:
            return jsonify({
                'success': False,
                'message': f'不支持的币种: {symbol}'
            }), 400

        if timeframe not in STOCHRSI_CONFIG['timeframes']:
            return jsonify({
                'success': False,
                'message': f'不支持的时间周期: {timeframe}'
            }), 400

        limit = int(request.args.get('limit', 10))
        divergence_history = get_divergence_history(symbol, timeframe, limit)

        return jsonify({
            'success': True,
            'data': {
                'symbol': symbol,
                'timeframe': timeframe,
                'divergence_signals': divergence_history
            },
            'message': f'获取到 {len(divergence_history)} 个背离信号'
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取背离信号历史失败: {str(e)}'
        })

@app.route('/api/stochrsi/refresh/test', methods=['POST'])
@require_auth
def refresh_stochrsi_data_test():
    """刷新StochRSI数据 - 测试版本"""
    try:
        # 测试版本直接返回成功，不实际执行数据刷新
        return jsonify({
            'success': True,
            'message': 'StochRSI数据刷新完成 (测试模式)'
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'刷新StochRSI数据失败: {str(e)}'
        })

@app.route('/api/stochrsi/refresh', methods=['POST'])
@require_auth
def refresh_stochrsi_data():
    """刷新StochRSI数据"""
    try:
        from watcher_stochrsi import watch
        import asyncio

        data = request.json or {}
        symbol = data.get('symbol')
        timeframe = data.get('timeframe')

        # 如果没有指定币种和时间周期，刷新所有数据
        if symbol and timeframe:
            if symbol not in STOCHRSI_CONFIG['symbols'] or timeframe not in STOCHRSI_CONFIG['timeframes']:
                return jsonify({
                    'success': False,
                    'message': '不支持的币种或时间周期'
                }), 400

            # 刷新特定数据
            try:
                if timeframe == '1W':
                    watch(time_interval='1W', symbol=symbol, limit=200)
                else:
                    watch(time_interval=timeframe, symbol=symbol, limit=100)
            except Exception as e:
                return jsonify({
                    'success': False,
                    'message': f'刷新 {symbol} {timeframe} 数据失败: {str(e)}'
                })

            message = f'{symbol} {timeframe} 数据刷新完成'
        else:
            # 刷新所有数据
            for sym in STOCHRSI_CONFIG['symbols']:
                for tf in STOCHRSI_CONFIG['timeframes']:
                    try:
                        if tf == '1W':
                            watch(time_interval='1W', symbol=sym, limit=200)
                        else:
                            watch(time_interval=tf, symbol=sym, limit=100)
                        print(f"已刷新 {sym} {tf}")
                    except Exception as e:
                        print(f"刷新 {sym} {tf} 失败: {e}")

            message = '所有StochRSI数据刷新完成'

        return jsonify({
            'success': True,
            'message': message
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'刷新StochRSI数据失败: {str(e)}'
        })


if __name__ == '__main__':
    # 创建数据目录
    import os
    os.makedirs('data', exist_ok=True)
    os.makedirs('templates', exist_ok=True)

    # 启动Flask应用
    app.run(debug=True, host='0.0.0.0', port=5000)