"""pm2 部署相关：cron 窗口守卫（不许把部署动作变成一封假 14:50 邮件）。"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from astock import daily


def test_in_cron_window_accepts_the_cron_fire_time():
    assert daily.in_cron_window(pd.Timestamp("2026-11-24 14:50:03"))
    assert daily.in_cron_window(pd.Timestamp("2026-11-24 14:45:00"))
    assert daily.in_cron_window(pd.Timestamp("2026-11-24 15:04:59"))


def test_in_cron_window_honours_a_custom_range():
    """收盘同步用 15:05-15:30：15:10 能跑，14:50 和 21:42 都不行。"""
    assert daily.in_cron_window(pd.Timestamp("2026-11-24 15:10:00"), "15:05-15:30")
    assert not daily.in_cron_window(pd.Timestamp("2026-11-24 14:50:00"), "15:05-15:30")
    assert not daily.in_cron_window(pd.Timestamp("2026-11-24 21:42:00"), "15:05-15:30")


def test_in_cron_window_rejects_deploy_and_close_times():
    """21:42 部署、15:10 收盘同步、早上 pm2 resurrect 都不算 14:50 那一刻。"""
    for stamp in ("2026-11-24 21:42:00", "2026-11-24 15:05:00", "2026-11-24 09:31:00"):
        assert not daily.in_cron_window(pd.Timestamp(stamp)), stamp


def test_cron_main_exits_without_mailing_outside_the_window(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(daily, "run", lambda **kw: calls.append(kw) or {"reason": "sent"})
    monkeypatch.setattr(daily, "_now", lambda: pd.Timestamp("2026-11-24 21:42:00"))

    assert daily.main(["--send", "--cron"]) == 0
    assert calls == [], "窗口外一次 run 都不能调，否则就是真发信"
    assert "跳过" in capsys.readouterr().out


def test_cron_main_runs_inside_the_window(monkeypatch):
    calls = []
    monkeypatch.setattr(daily, "run", lambda **kw: calls.append(kw) or {
        "date": "2026-11-24", "reason": "sent", "subject": "s", "body": "b"})
    monkeypatch.setattr(daily, "_now", lambda: pd.Timestamp("2026-11-24 14:50:03"))

    assert daily.main(["--send", "--cron"]) == 0
    assert calls and calls[0]["send"] is True
    assert calls[0]["now"] == pd.Timestamp("2026-11-24 14:50:03"), "窗口判定与执行必须用同一个 now"
