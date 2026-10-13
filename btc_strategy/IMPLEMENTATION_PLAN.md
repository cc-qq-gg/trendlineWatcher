# BTC 策略迁移实施计划

## 目标
将当前项目的 BTC 4H MA Regime 策略迁移到 quantauto，实现数据增量更新、完整回测、登录保护的动态页面和 Aggressive 邮件预警。

## 已完成
- 已创建独立 btc_strategy 模块。
- 已迁移 BTC 5m HDF5 与 CSV 快照，并生成 4H 派生 CSV。
- 已复制当前 BTC 回测产物。
- 已添加数据规范化、去重、4H 聚合、连续性检测和原子写入。
- 已添加 /btc-strategy 页面和 /api/btc-strategy/status 认证接口。
- 已将当前完整 viewer 迁移到 btc_strategy/static/viewer.html，保留原有图表、筛选、交易详情和四档策略切换。
- viewer 已改为优先请求 /api/btc-strategy/payload，API 返回最新 payload 后刷新即可显示最新结果；请求失败时回退到内嵌快照。
- 已添加 /api/btc-strategy/payload 登录保护接口和 latest_payload.json 初始快照。
- 数据契约测试已通过。

验证记录：

- 数据和调度回归测试：3 passed。
- viewer 动态加载检查：passed。
- payload 数据集：baseline、defensive、growth、aggressive 四档均存在 bars、trades、annual 数据。
- Aggressive 初始快照：17,025 根 K 线、85 笔交易。
- pytest 运行需要项目根目录加入 PYTHONPATH，例如 PYTHONPATH=. .venv/bin/pytest。

当前快照截止 2024-09-08，不能直接作为实时状态，必须先完成增量更新和 dry-run。

## 固定策略契约
MA=11，shift=77，slope threshold=1%，stop loss=8%，risk-off=0.8x，risk-on=3.0x，周期=4h。

执行语义：只使用已经收盘的 4H K 线；收盘产生信号；下一根 4H K 线开盘执行；MA200 状态只影响下一根 K 线；禁止未来函数；止损交易的退出时间、价格、净值和原因必须一致。

## 实施阶段

### 1. 数据管线
新增 data_sync.py：复用 quantauto 现有 OKX 只读配置，从 CSV 尾部增量获取 5m；统一 UTC；合并、排序、去重；检查连续性；逐段补全缺口；补全失败时不覆盖成功快照；所有写入使用临时文件替换。

生成 derived/BTC-USDT_4h.csv 时使用 label=left、closed=left，只输出完整 4H K 线，并检查时间间隔和 OHLCV 完整性。

### 2. 回测引擎
新增 backtest.py，不复用执行语义不同的通用回测器。实现 next-open、MA200 风险状态、8% 止损、手续费、滑点、资金费，以及 baseline、defensive、growth、aggressive 四档输出。

每次运行保存到 data/btc_strategy/runs/时间戳/，包括 manifest、summary、trades、equity、annual 文件；成功后原子更新 backtests/latest；失败时保留旧 latest 并标记 stale。

### 3. 预警引擎
新增 alerts.py，支持 AGGRESSIVE_ON 和 AGGRESSIVE_OFF。去重键为 strategy_version + candle_time + signal_type。同一轮多个事件合并为一封邮件。

邮件必须包含策略名称、信号类型、信号价格、K 线时间、当前杠杆、信号原因、数据尾部时间和 stale 状态。复用 quantauto/send_email.py，不负责下单。

### 4. 独立 Worker
新增 worker.py，启动命令：

    cd /Users/cq/projects/quant/quantauto
    source .venv/bin/activate
    python -m btc_strategy.worker

按 UTC 的 00:05、04:05、08:05、12:05、16:05、20:05 执行。顺序为：增量更新、补缺、确认 4H 收盘、聚合、完整回测、写快照、更新 latest、计算信号、写 alerts.csv、发送汇总邮件。

任何步骤失败都不覆盖旧结果、不发邮件，并记录失败原因和 stale 状态。

### 5. 动态页面和 API
扩展登录保护 API：

    GET /btc-strategy
    GET /api/btc-strategy/status
    GET /api/btc-strategy/summary
    GET /api/btc-strategy/equity
    GET /api/btc-strategy/trades
    GET /api/btc-strategy/alerts

页面必须保持当前 stater viewer 的展示方式和交互体验，不改成只有摘要的简化状态页。当前页面的 HTML/JS/CSS 作为展示基线，迁移时将其静态内嵌数据改为 API 动态加载。

必须保留：

- 策略选择器：baseline、defensive、growth、aggressive。
- K 线图和 MA11、MA200D 等指标线。
- 入场、出场、止损等交易标记。
- 风险状态和当前杠杆区间展示。
- 策略净值曲线和回撤曲线。
- 年度收益、年末净值和年度回撤表。
- 选中交易详情，包括入场价、出场价、价格变动、净收益、手续费/滑点、资金费和总交易磨损。
- 逐笔交易列表，包括时间、价格、收益、磨损和 K 线数量。
- 当前 viewer 的筛选、全图和交易选中交互。

页面只读取 latest 快照，不在请求期间回测。后台更新成功后，页面刷新即可显示最新结果；不要求重新生成包含死数据的 viewer.html。

当前 btc_strategy.html 仅保留为旧的路由占位文件，最终页面入口已改为完整的 btc_strategy/static/viewer.html。下一步需要让 worker 在每次成功回测后原子更新 latest_payload.json，并同步更新 manifest、状态和 stale 标记。

### 6. dry-run
正式邮件前执行 python -m btc_strategy.worker --dry-run。dry-run 不发邮件，但必须完成数据更新、缺口检查、4H 聚合、完整回测、latest 生成、预警计算和页面 API 读取。

核对最新数据、完整 4H、next-open 交易、止损字段、页面与 CSV 一致性、重复运行去重、补全失败保留旧结果。

## 测试要求
至少覆盖：5m 去重和连续性；4H 边界和未完成 K 线；next-open 无未来函数；MA200 延后一根；止损字段一致；失败保留旧快照；预警去重和同轮合并；未登录页面重定向；未登录 API 返回 401；登录后 API 读取最新快照。

## 当前执行顺序
1. 完成 data_sync.py。
2. 完成 backtest.py 和四档输出。
3. 完成 manifest、latest 原子快照和 stale 状态。
4. 完成 alerts.py。
5. 完成 worker.py 和 dry-run。
6. 完成 Flask API 和页面展示。
7. 补齐测试并运行回归。
8. dry-run 验收通过后开启正式邮件预警。
