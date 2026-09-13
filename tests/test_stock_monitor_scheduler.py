import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stock_monitor_scheduler import configure_stock_monitor_schedule


class FakeEvery:
    def __init__(self):
        self.time = None
        self.job = None

    @property
    def day(self):
        return self

    def at(self, value):
        self.time = value
        return self

    def do(self, job):
        self.job = job
        return self


class FakeScheduler:
    def __init__(self):
        self.every_obj = FakeEvery()

    def every(self):
        return self.every_obj


def test_configure_stock_monitor_schedule_registers_close_time_job():
    scheduler = FakeScheduler()
    calls = []

    configure_stock_monitor_schedule(scheduler=scheduler, job_func=lambda: calls.append("checked"), close_time="16:00")
    scheduler.every_obj.job()

    assert scheduler.every_obj.time == "16:00"
    assert calls == ["checked"]
