import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from StockMonitorManager import StockMonitorManager, normalize_stock_code


def test_stock_monitors_sort_triggered_last_and_can_reactivate(tmp_path):
    manager = StockMonitorManager(data_dir=str(tmp_path))

    active_id = manager.create_monitor(
        code="600000",
        target_price=12.3,
        note="active one",
    )
    triggered_id = manager.create_monitor(
        code="000001",
        target_price=10.0,
        note="triggered one",
    )
    manager.mark_triggered(
        triggered_id,
        trade_date="2026-09-11",
        hit_price=10.0,
        message="hit",
    )

    monitors = manager.get_all_monitors()

    assert [item["id"] for item in monitors] == [active_id, triggered_id]
    assert monitors[-1]["status"] == "triggered"

    assert manager.reactivate_monitor(triggered_id)
    reactivated = manager.get_monitor(triggered_id)

    assert reactivated["status"] == "active"
    assert reactivated["triggered_at"] == ""


def test_normalize_stock_code_rejects_invalid_a_share_code():
    assert normalize_stock_code("600000") == "sh.600000"

    with pytest.raises(ValueError, match="股票代码"):
        normalize_stock_code("6000000")
