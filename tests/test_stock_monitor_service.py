import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from StockMonitorManager import StockMonitorManager
from stock_monitor_service import StockMonitorService


class FakeProvider:
    def latest_daily_quote(self, code):
        return {
            "date": "2026-09-11",
            "code": code,
            "name": "浦发银行",
            "high": 12.5,
            "low": 12.0,
            "close": 12.4,
            "tradestatus": "1",
        }


class EmptyProvider:
    def latest_daily_quote(self, code):
        return None


def test_daily_check_sends_email_once_when_price_is_touched(tmp_path):
    manager = StockMonitorManager(data_dir=str(tmp_path))
    monitor_id = manager.create_monitor(
        code="600000",
        target_price=12.3,
        note="回踩确认",
    )
    sent = []
    service = StockMonitorService(
        manager=manager,
        provider=FakeProvider(),
        send_email_func=lambda subject="", content="": sent.append((subject, content)),
    )

    first = service.check_all()
    second = service.check_all()

    monitor = manager.get_monitor(monitor_id)
    assert first["triggered"] == 1
    assert second["triggered"] == 0
    assert monitor["status"] == "triggered"
    assert float(monitor["triggered_price"]) == 12.3
    assert sent == [
        (
            "股票触价提醒：sh.600000 已触及 12.30",
            "sh.600000 已触及监控价 12.30。备注：回踩确认",
        )
    ]


def test_daily_check_updates_stock_name_from_quote(tmp_path):
    manager = StockMonitorManager(data_dir=str(tmp_path))
    monitor_id = manager.create_monitor(code="600000", target_price=20)
    service = StockMonitorService(
        manager=manager,
        provider=FakeProvider(),
        send_email_func=lambda **kwargs: None,
    )

    result = service.check_all()

    assert result["checked"] == 1
    assert manager.get_monitor(monitor_id)["name"] == "浦发银行"


def test_daily_check_reports_active_monitor_without_quote(tmp_path):
    manager = StockMonitorManager(data_dir=str(tmp_path))
    manager.create_monitor(code="600000", target_price=12.3)
    service = StockMonitorService(manager=manager, provider=EmptyProvider(), send_email_func=lambda **kwargs: None)

    result = service.check_all()

    assert result["success"] is False
    assert result["checked"] == 0
    assert result["triggered"] == 0
    assert result["errors"] == [
        {
            "id": manager.get_all_monitors()[0]["id"],
            "code": "sh.600000",
            "error": "未获取到行情数据",
        }
    ]
