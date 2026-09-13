import os
import re
import uuid
from datetime import datetime

import pandas as pd


class StockMonitorManager:
    """股票价格监控配置和触发日志。"""

    MONITOR_COLUMNS = [
        "id",
        "code",
        "name",
        "target_price",
        "note",
        "status",
        "last_checked_date",
        "last_price",
        "triggered_at",
        "triggered_price",
        "created_at",
        "updated_at",
    ]
    LOG_COLUMNS = [
        "id",
        "monitor_id",
        "code",
        "target_price",
        "hit_price",
        "trade_date",
        "message",
        "created_at",
    ]

    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        self.monitors_file = os.path.join(data_dir, "stock_monitors.csv")
        self.logs_file = os.path.join(data_dir, "stock_monitor_logs.csv")
        self._init_files()

    def _init_files(self):
        os.makedirs(self.data_dir, exist_ok=True)
        if not os.path.exists(self.monitors_file):
            pd.DataFrame(columns=self.MONITOR_COLUMNS).to_csv(self.monitors_file, index=False)
        if not os.path.exists(self.logs_file):
            pd.DataFrame(columns=self.LOG_COLUMNS).to_csv(self.logs_file, index=False)

    def _load_monitors(self):
        try:
            df = pd.read_csv(self.monitors_file, dtype={"code": str}).fillna("")
        except Exception:
            df = pd.DataFrame(columns=self.MONITOR_COLUMNS)
        for column in self.MONITOR_COLUMNS:
            if column not in df.columns:
                df[column] = ""
        return df[self.MONITOR_COLUMNS].astype(object).copy()

    def _save_monitors(self, df):
        df.to_csv(self.monitors_file, index=False)

    def _load_logs(self):
        try:
            df = pd.read_csv(self.logs_file, dtype={"code": str}).fillna("")
        except Exception:
            df = pd.DataFrame(columns=self.LOG_COLUMNS)
        for column in self.LOG_COLUMNS:
            if column not in df.columns:
                df[column] = ""
        return df[self.LOG_COLUMNS].astype(object).copy()

    def _save_logs(self, df):
        df.to_csv(self.logs_file, index=False)

    def create_monitor(self, code, target_price, note="", name=""):
        monitor_id = str(uuid.uuid4())
        now = datetime.now().isoformat(timespec="seconds")
        row = {
            "id": monitor_id,
            "code": normalize_stock_code(code),
            "name": name,
            "target_price": float(target_price),
            "note": note,
            "status": "active",
            "last_checked_date": "",
            "last_price": "",
            "triggered_at": "",
            "triggered_price": "",
            "created_at": now,
            "updated_at": now,
        }
        df = self._load_monitors()
        df.loc[len(df)] = row
        self._save_monitors(df)
        return monitor_id

    def get_monitor(self, monitor_id):
        df = self._load_monitors()
        row = df[(df["id"] == monitor_id) & (df["status"] != "deleted")]
        if row.empty:
            return None
        return row.iloc[0].to_dict()

    def get_all_monitors(self):
        df = self._load_monitors()
        df = df[df["status"] != "deleted"].copy()
        status_order = {"active": 0, "inactive": 1, "triggered": 2}
        df["_status_order"] = df["status"].map(status_order).fillna(9)
        df = df.sort_values(["_status_order", "created_at"], ascending=[True, True])
        return df.drop(columns=["_status_order"]).to_dict("records")

    def update_monitor(self, monitor_id, **kwargs):
        df = self._load_monitors()
        mask = df["id"] == monitor_id
        if not mask.any():
            return False
        allowed = set(self.MONITOR_COLUMNS) - {"id", "created_at"}
        for key, value in kwargs.items():
            if key in allowed:
                df.loc[mask, key] = value
        df.loc[mask, "updated_at"] = datetime.now().isoformat(timespec="seconds")
        self._save_monitors(df)
        return True

    def mark_triggered(self, monitor_id, trade_date, hit_price, message):
        monitor = self.get_monitor(monitor_id)
        if not monitor:
            return False
        now = datetime.now().isoformat(timespec="seconds")
        self.update_monitor(
            monitor_id,
            status="triggered",
            last_checked_date=trade_date,
            last_price=hit_price,
            triggered_at=now,
            triggered_price=hit_price,
        )
        logs = self._load_logs()
        log = {
            "id": str(uuid.uuid4()),
            "monitor_id": monitor_id,
            "code": monitor["code"],
            "target_price": monitor["target_price"],
            "hit_price": hit_price,
            "trade_date": trade_date,
            "message": message,
            "created_at": now,
        }
        logs.loc[len(logs)] = log
        self._save_logs(logs)
        return True

    def reactivate_monitor(self, monitor_id):
        return self.update_monitor(
            monitor_id,
            status="active",
            triggered_at="",
            triggered_price="",
        )

    def delete_monitor(self, monitor_id):
        return self.update_monitor(monitor_id, status="deleted")


def normalize_stock_code(code):
    """补齐A股市场前缀。"""
    value = str(code).strip().lower()
    if value.startswith(("sh.", "sz.")):
        market, raw_code = value.split(".", 1)
        if re.fullmatch(r"\d{6}", raw_code):
            return f"{market}.{raw_code}"
        raise ValueError("股票代码必须是6位数字")
    if not re.fullmatch(r"\d{6}", value):
        raise ValueError("股票代码必须是6位数字")
    if value.startswith(("6", "9")):
        return f"sh.{value}"
    return f"sz.{value}"
