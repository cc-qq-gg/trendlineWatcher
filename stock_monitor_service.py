from StockMonitorManager import StockMonitorManager
from send_email import send_email
from stock_price_provider import FallbackPriceProvider


class StockMonitorService:
    """执行股票触价检查并发送提醒。"""

    def __init__(self, manager=None, provider=None, send_email_func=None):
        self.manager = manager or StockMonitorManager()
        self.provider = provider or FallbackPriceProvider()
        self.send_email = send_email_func or send_email

    def check_all(self):
        checked = 0
        triggered = 0
        errors = []

        for monitor in self.manager.get_all_monitors():
            if monitor.get("status") != "active":
                continue

            try:
                quote = self.provider.latest_daily_quote(monitor["code"])
                if not quote:
                    errors.append({
                        "id": monitor.get("id"),
                        "code": monitor.get("code"),
                        "error": "未获取到行情数据",
                    })
                    continue
                checked += 1
                self._update_monitor_quote(monitor, quote)
                if self._is_touched(monitor, quote):
                    self._trigger_monitor(monitor, quote)
                    triggered += 1
            except Exception as exc:
                errors.append({"id": monitor.get("id"), "code": monitor.get("code"), "error": str(exc)})

        return {"success": len(errors) == 0, "checked": checked, "triggered": triggered, "errors": errors}

    def _is_touched(self, monitor, quote):
        target = float(monitor["target_price"])
        return float(quote["low"]) <= target <= float(quote["high"])

    def _update_monitor_quote(self, monitor, quote):
        update_data = {
            "last_checked_date": quote.get("date", ""),
            "last_price": quote.get("close", ""),
        }
        if quote.get("name"):
            update_data["name"] = quote["name"]
        self.manager.update_monitor(monitor["id"], **update_data)

    def _trigger_monitor(self, monitor, quote):
        target = float(monitor["target_price"])
        subject = f"股票触价提醒：{monitor['code']} 已触及 {target:.2f}"
        content = f"{monitor['code']} 已触及监控价 {target:.2f}。备注：{monitor.get('note') or '无'}"
        self.send_email(subject=subject, content=content)
        self.manager.mark_triggered(
            monitor["id"],
            trade_date=quote.get("date", ""),
            hit_price=target,
            message=content,
        )
