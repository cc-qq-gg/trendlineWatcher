"""
趋势线监测系统启动脚本
"""

import os
import sys
import threading
import time
from TrendlineMonitor import start_global_monitoring, stop_global_monitoring
from TrendlineWebApp import app
from TrendlineManager import TrendlineManager
from stock_monitor_scheduler import start_stock_monitor_scheduler


def main():
    """主函数"""
    print("🚀 趋势线监测系统启动中...")

    # 创建数据目录
    os.makedirs("data", exist_ok=True)

    # 初始化趋势线管理器
    manager = TrendlineManager()
    print("✅ 趋势线管理器已初始化")

    # 检查是否有趋势线配置
    trendlines = manager.get_all_trendlines()
    if not trendlines:
        print("📝 当前没有趋势线配置，请通过Web界面添加")

    print("\n🌐 启动Web界面...")
    print("📱 请在浏览器中访问: http://localhost:5000")
    print("💡 系统为手动刷新模式，请通过Web界面手动刷新数据和检查信号")
    start_stock_monitor_scheduler()
    print("📈 股票监控将在每日16:00自动检查")
    print("⚠️  按 Ctrl+C 停止系统")

    try:
        # 启动Flask应用
        app.run(debug=False, host='0.0.0.0', port=5000)
    except KeyboardInterrupt:
        print("\n🛑 系统已停止")


if __name__ == "__main__":
    main()
