# 趋势线监测系统

基于现有量化交易框架的趋势线监测系统，提供Web界面进行趋势线管理和实时突破监测。

## 系统特点

- ✅ **完全复用现有代码**：基于现有的K线数据获取和趋势线计算逻辑
- 📊 **可视化界面**：直观的Web界面管理趋势线
- 🚨 **实时监测**：自动监测突破/跌破信号
- 💾 **CSV存储**：使用CSV文件存储配置和日志
- 🔄 **钉钉通知**：集成现有的钉钉通知功能

## 文件结构

```
quantauto/
├── TrendlineManager.py      # 趋势线配置管理（CSV存储）
├── TrendlineMonitor.py      # 监测引擎（复用现有逻辑）
├── TrendlineWebApp.py       # Web应用（Flask）
├── templates/
│   └── index.html           # Web界面
├── data/                    # 数据目录（自动创建）
│   ├── trendlines.csv       # 趋势线配置
│   └── monitor_logs.csv     # 监测日志
├── run_trendline_monitor.py # 启动脚本
└── requirements.txt         # Python依赖
```

## 核心功能

### 1. 趋势线管理
- ✅ 创建趋势线（起点、终点、方向）
- ✅ 编辑趋势线配置
- ✅ 删除趋势线
- ✅ 启用/禁用监测
- ✅ 导入/导出配置

### 2. 实时监测
- ✅ 复用 `ccxt_fetch_candle_data()` 获取K线数据
- ✅ 复用 `define_trendline()` 计算趋势线
- ✅ 复用 `monitor_breakout()` 检测突破
- ✅ 支持多头突破和空头跌破策略
- ✅ 钉钉通知集成

### 3. Web界面
- 📊 K线图表可视化
- 📈 趋势线显示
- 📋 趋势线列表管理
- 📝 监测日志查看
- ⚙️ 监测状态控制

## 快速开始

环境以 `pyproject.toml` + `uv.lock` 为准，统一用 uv（不要用 pip / requirements.txt，
那份缺 `requests`、`schedule`、`ta-lib`）。

### 1. 同步依赖
```bash
uv sync
```

### 2. 启动系统
```bash
uv run run_trendline_monitor.py      # 趋势线 + A 股 /biga + 股票监控调度（5000）
```

### 3. 访问Web界面
打开浏览器访问: http://localhost:5000 · A 股页面: http://localhost:5000/biga/

### 4. 添加趋势线
1. 点击"新增趋势线"
2. 填写趋势线信息：
   - 名称：趋势线的描述名称
   - 交易对：如 SOL-USDT-SWAP
   - 起点：时间和价格
   - 终点：时间和价格
   - 方向：多头突破或空头跌破

### 5. 启动监测
1. 在Web界面点击"启动监测"
2. 系统会自动监测突破信号
3. 检测到突破时会发送钉钉通知

## API接口

### 趋势线管理
- `GET /api/trendlines` - 获取所有趋势线
- `POST /api/trendlines` - 创建趋势线
- `GET /api/trendlines/{id}` - 获取单个趋势线
- `PUT /api/trendlines/{id}` - 更新趋势线
- `DELETE /api/trendlines/{id}` - 删除趋势线

### 监测功能
- `GET /api/monitor/status` - 获取监测状态
- `POST /api/monitor/start` - 启动监测
- `POST /api/monitor/stop` - 停止监测
- `POST /api/trendlines/{id}/check` - 检查趋势线
- `GET /api/trendlines/{id}/data` - 获取趋势线数据

### 数据查询
- `GET /api/logs` - 获取监测日志
- `GET /api/candles` - 获取K线数据
- `GET /api/export` - 导出配置
- `POST /api/import` - 导入配置

## 数据格式

### 趋势线配置 (trendlines.csv)
```csv
id,name,symbol,start_time,start_price,end_time,end_price,direction,status,created_at,updated_at
uuid,SOL上升趋势线,SOL-USDT-SWAP,2025-05-09 18:00:00,174.25,2025-05-14 06:00:00,183.44,1,active,2025-01-01T10:00:00,2025-01-01T10:00:00
```

### 监测日志 (monitor_logs.csv)
```csv
id,trendline_id,signal_type,price,trendline_value,detected_at
uuid,uuid,breakout,175.50,175.20,2025-01-01T15:30:00
```

## 复用的现有组件

### K线数据获取
- **函数**: `Function.ccxt_fetch_candle_data()`
- **功能**: 从OKX获取K线数据
- **复用方式**: 直接调用

### 趋势线计算
- **函数**: `Signals.define_trendline()`
- **功能**: 根据两点计算趋势线值
- **复用方式**: 直接调用

### 突破检测
- **函数**: `Signals.monitor_breakout()`
- **功能**: 检测突破/跌破信号
- **复用方式**: 直接调用

### 通知系统
- **函数**: `Function.send_dingding_msg()`
- **功能**: 发送钉钉通知
- **复用方式**: 直接调用

## 使用示例

### 创建趋势线
```python
from TrendlineManager import TrendlineManager

manager = TrendlineManager()
trendline_id = manager.create_trendline(
    name="SOL上升趋势线",
    symbol="SOL-USDT-SWAP",
    start_point=["2025-05-09 18:00:00", 174.25],
    end_point=["2025-05-14 06:00:00", 183.44],
    direction=1  # 多头突破
)
```

### 启动监测
```python
from TrendlineMonitor import start_global_monitoring

start_global_monitoring(
    symbols=["SOL-USDT-SWAP"],
    time_interval="5m"
)
```

### 检查突破信号
```python
monitor = TrendlineMonitor()
signal = monitor.check_trendline_now(trendline_id)
if signal == 1:
    print("检测到多头突破信号")
elif signal == -1:
    print("检测到空头跌破信号")
```

## 注意事项

1. **时间格式**: 使用 `YYYY-MM-DD HH:MM:SS` 格式
2. **交易对**: 确保交易对名称正确（如 SOL-USDT-SWAP）
3. **数据范围**: 确保趋势线时间点在K线数据范围内
4. **监测频率**: 默认30秒检查一次，可根据需要调整
5. **API限制**: 注意OKX API的调用频率限制

## 故障排除

### 常见问题

1. **无法获取K线数据**
   - 检查网络连接和代理设置
   - 确认API密钥配置正确

2. **趋势线计算错误**
   - 检查时间格式是否正确
   - 确认时间点在数据范围内

3. **Web界面无法访问**
   - 确认端口5000未被占用
   - 检查防火墙设置

4. **监测未启动**
   - 检查是否有活跃的趋势线配置
   - 确认交易对配置正确

## 扩展功能

系统设计为可扩展的架构，可以轻松添加：
- 更多交易所支持
- 自定义通知渠道
- 更复杂的监测策略
- 数据分析和报告功能