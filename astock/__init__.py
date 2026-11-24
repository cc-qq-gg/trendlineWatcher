"""A 股指数：双动量轮动回测 + 指数关系图（从 bigA 精炼迁移）。

模块划分：
    data      单 CSV 长表读写 + 东财/蛋卷增量采集（不依赖 MySQL）
    momentum  L146064 双动量轮动：20 日涨幅排名第一且过开仓阈值，空仓持国债指数
    relation  指数关系：原始点位 + 滚动历史分位 + ERP 派生（全部 numpy 向量化）
    payload   JSON 契约（关系图多条序列 / 轮动净值与逐笔）
    service   按参数缓存计算结果，请求路径不联网
    app       独立的本地 Flask 预览（127.0.0.1:8012）

启动（工作目录必须是仓库根；项目是 package=false，包靠 cwd 进 sys.path）：

    uv run python -m astock.data --sync     # 首次建库 / 补到最新交易日
    uv run python -m astock.service         # 打印两个模块的锚点摘要
    uv run python -m astock.app             # http://127.0.0.1:8012/
"""

__all__ = ["data", "momentum", "payload", "relation", "service"]
