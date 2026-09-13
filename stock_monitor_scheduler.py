import threading
import time

import schedule

from stock_monitor_service import StockMonitorService


def run_stock_monitor_check():
    """执行一次股票收盘后检查。"""
    return StockMonitorService().check_all()


def configure_stock_monitor_schedule(scheduler=schedule, job_func=run_stock_monitor_check, close_time="16:00"):
    """注册每日收盘后检查任务。"""
    return scheduler.every().day.at(close_time).do(job_func)


def start_stock_monitor_scheduler(close_time="16:00"):
    """后台启动股票监控调度。"""
    configure_stock_monitor_schedule(close_time=close_time)

    def loop():
        while True:
            schedule.run_pending()
            time.sleep(30)

    thread = threading.Thread(target=loop, name="stock-monitor-scheduler", daemon=True)
    thread.start()
    return thread
