import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import TrendlineWebApp as webapp
from StockMonitorManager import StockMonitorManager


class FakeService:
    def check_all(self):
        return {"success": True, "checked": 1, "triggered": 0, "errors": []}


def login(client):
    with client.session_transaction() as sess:
        sess["authenticated"] = True
        sess["username"] = "trendline"


def test_stock_monitor_api_requires_login():
    webapp.app.config["TESTING"] = True
    client = webapp.app.test_client()

    response = client.get("/api/stock-monitors")

    assert response.status_code == 401


def test_stock_monitor_api_create_list_reactivate_and_check(tmp_path, monkeypatch):
    manager = StockMonitorManager(data_dir=str(tmp_path))
    monkeypatch.setattr(webapp, "stock_monitor_manager", manager)
    monkeypatch.setattr(webapp, "build_stock_monitor_service", lambda: FakeService())

    webapp.app.config["TESTING"] = True
    client = webapp.app.test_client()
    login(client)

    create_response = client.post(
        "/api/stock-monitors",
        json={"code": "600000", "target_price": 12.3, "note": "回踩确认"},
    )
    created = create_response.get_json()

    assert create_response.status_code == 200
    assert created["success"] is True
    monitor_id = created["data"]["id"]
    assert created["data"]["code"] == "sh.600000"

    manager.mark_triggered(monitor_id, trade_date="2026-09-11", hit_price=12.3, message="hit")
    list_response = client.get("/api/stock-monitors")
    listed = list_response.get_json()["data"]
    assert listed[-1]["status"] == "triggered"

    reactivate_response = client.post(f"/api/stock-monitors/{monitor_id}/reactivate")
    assert reactivate_response.get_json()["success"] is True
    assert manager.get_monitor(monitor_id)["status"] == "active"

    check_response = client.post("/api/stock-monitors/check")
    assert check_response.get_json()["data"]["checked"] == 1
