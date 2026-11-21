"""divergence 刷新链的按需刷新行为测试。

覆盖三件事：
1. 请求路径不阻塞：overview 只触发后台刷新并立即返回；
2. 成本去重：每个品种只取一次数，指标按周期各算一次；
3. 并发与脏标记：拿不到锁立即返回 busy，崩溃遗留的 running 会过期。
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import TrendlineWebApp as webapp
from divergence_visualizer import divergence_detector as detector


@pytest.fixture
def state_file(tmp_path, monkeypatch):
    path = tmp_path / "detector_refresh_state.json"
    monkeypatch.setattr(webapp, "_divergence_state_path", lambda: str(path))
    monkeypatch.setattr(webapp, "_has_missing_divergence_assets", lambda: False)
    monkeypatch.setattr(webapp, "_current_4h_bucket", lambda now=None: "20261121-12")
    return path


def test_request_path_only_starts_background_work(state_file, monkeypatch):
    started = []

    def fake_start(force=False):
        started.append(force)
        return True

    monkeypatch.setattr(webapp, "_start_background_refresh", fake_start)
    monkeypatch.setattr(webapp, "_refresh_running", lambda state: False)

    result = webapp.ensure_divergence_fresh()

    # 立即返回 running，真正的刷新交给后台线程
    assert result["status"] == "running"
    assert result["triggered"] is True
    assert started == [False]


def test_same_bucket_is_skipped(state_file):
    needed, reason = webapp._divergence_refresh_needed(
        False, {"last_success_bucket": "20261121-12"}
    )
    assert needed is False
    assert reason == "same_bucket_skip"


def test_missing_assets_force_refresh(state_file, monkeypatch):
    monkeypatch.setattr(webapp, "_has_missing_divergence_assets", lambda: True)
    needed, reason = webapp._divergence_refresh_needed(False, {})
    assert needed is True
    assert reason == "missing_files"


def test_stale_running_marker_is_ignored():
    stale = (
        datetime.now() - timedelta(seconds=webapp.REFRESH_RUNNING_TTL_SECONDS + 60)
    ).strftime("%Y-%m-%d %H:%M:%S")
    fresh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    assert webapp._refresh_running({"running": True, "running_since": stale}) is False
    assert webapp._refresh_running({"running": True, "running_since": fresh}) is True
    assert webapp._refresh_running({}) is False


def test_refresh_fetches_data_once_per_symbol(state_file, monkeypatch):
    ensure_calls = []
    render_calls = []

    monkeypatch.setattr(detector, "ensure_symbol_data", lambda symbol, **kwargs: ensure_calls.append(symbol))
    monkeypatch.setattr(
        detector, "render_symbol", lambda symbol, timeframe, **kwargs: render_calls.append((symbol, timeframe))
    )

    result = webapp.refresh_divergence_assets(force=True)

    symbols = webapp.DIVERGENCE_CONFIG["symbols"]
    timeframes = webapp.DIVERGENCE_CONFIG["timeframes"]
    assert ensure_calls == symbols, "每个品种应只取一次数"
    assert render_calls == [(symbol, tf) for symbol in symbols for tf in timeframes]
    assert result["status"] == "done"
    assert result["updated"] is True


def test_busy_lock_returns_running_without_waiting(state_file):
    assert webapp._divergence_refresh_lock.acquire(blocking=False)
    try:
        result = webapp.refresh_divergence_assets(force=True, wait_lock=False)
    finally:
        webapp._divergence_refresh_lock.release()

    assert result["status"] == "running"
    assert result["reason"] == "busy"
    assert result["triggered"] is False


def test_refresh_writes_and_clears_running_flag(state_file, monkeypatch):
    observed = []

    def fake_ensure(symbol, **kwargs):
        observed.append(webapp._read_divergence_state().get("running"))

    monkeypatch.setattr(detector, "ensure_symbol_data", fake_ensure)
    monkeypatch.setattr(detector, "render_symbol", lambda *args, **kwargs: None)

    webapp.refresh_divergence_assets(force=True)
    state = webapp._read_divergence_state()

    # 刷新执行期间 running=True，结束后落盘为 False
    assert observed and all(value is True for value in observed)
    assert state["running"] is False
    assert state["last_success_bucket"] == "20261121-12"
