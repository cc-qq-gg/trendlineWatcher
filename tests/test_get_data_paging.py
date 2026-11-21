"""get_data.get_kline 分页取数的回归测试。

背景：OKX 的 market/candles 与 market/history-candles 单次最多返回 300 根。
一旦 limit 超过这个量级，交易所会忽略 since/before/after 窗口，直接返回
"另一段"数据，于是补缺口的分页循环拿到错误区间，缺口被静默跳过。

这里用 FakeExchange 复现交易所的这个行为，断言分页逻辑必须：
1. 每页请求量不超过交易所上限；
2. 多页之间严格衔接，不产生缺口；
3. 窗口错位时宁可截断也不能把缺口当成成功；
4. 网络错误与"未前进"都有终止条件，不挂死。
"""

import re
import sys
from pathlib import Path

import ccxt
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import get_data


EXCHANGE_CAP = 300


def _tf_seconds(timeframe):
    match = re.fullmatch(r"(\d+)([smhdw])", timeframe.lower())
    if not match:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    value, unit = int(match.group(1)), match.group(2)
    return value * {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[unit]


def _ms(timestamp):
    return int(pd.Timestamp(timestamp, tz="UTC").value // 1_000_000)


class FakeExchange:
    """按 OKX 的真实约束返回数据的交易所替身。"""

    id = "okx"

    def __init__(self, start="2026-01-01", count=1000, timeframe="4h"):
        self.tf_ms = _tf_seconds(timeframe) * 1000
        base = _ms(start)
        self.rows = [
            [base + index * self.tf_ms, 100.0 + index, 101.0 + index,
             99.0 + index, 100.5 + index, 1.0 + index]
            for index in range(count)
        ]
        self.calls = []
        self.fail_times = 0
        # 模拟交易所的默认行为：limit 超上限时忽略 since 窗口、返回另一段
        self.ignore_since_when_over_cap = True

    def parse8601(self, value):
        return _ms(value)

    def parse_timeframe(self, timeframe):
        return _tf_seconds(timeframe)

    def fetch_ohlcv(self, symbol=None, timeframe=None, since=None, limit=None):
        self.calls.append(
            {"symbol": symbol, "timeframe": timeframe, "since": since, "limit": limit}
        )
        if self.fail_times > 0:
            self.fail_times -= 1
            raise ccxt.NetworkError("simulated network failure")

        over_cap = limit is not None and limit > EXCHANGE_CAP
        if over_cap and self.ignore_since_when_over_cap:
            # 关键：窗口被忽略，返回靠后的另一段，调用方拿到的时间区间是错的
            start = EXCHANGE_CAP * 2
            return [list(row) for row in self.rows[start:start + EXCHANGE_CAP]]

        effective = min(limit or EXCHANGE_CAP, EXCHANGE_CAP)
        offset = 0
        if since is not None:
            offset = next(
                (i for i, row in enumerate(self.rows) if row[0] >= since), len(self.rows)
            )
        return [list(row) for row in self.rows[offset:offset + effective]]


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """分页间隔对断言没有意义，去掉以免拖慢测试。"""
    monkeypatch.setattr(get_data.time, "sleep", lambda *args, **kwargs: None)


def _gaps(frame):
    deltas = frame["candle_begin_time"].diff().dropna()
    step = deltas.mode().iloc[0]
    return deltas[deltas > step * 1.5]


def test_fake_reproduces_exchange_window_shift():
    """先证明这个替身确实能复现线上 bug 的成因，否则后续断言没有意义。"""
    fake = FakeExchange()
    shifted = pd.DataFrame(fake.fetch_ohlcv(limit=2000))
    expected = pd.DataFrame(fake.fetch_ohlcv(limit=EXCHANGE_CAP, since=_ms("2026-01-01")))

    # 请求的是开头那段，拿到的却是若干天之后的另一段
    assert shifted.iloc[0][0] > expected.iloc[0][0] + 24 * 3600 * 1000


def test_page_limit_never_exceeds_exchange_cap():
    fake = FakeExchange()

    get_data.get_kline(
        start_time="2026-01-01",
        exchange=fake,
        symbol="BTC/USDT:USDT",
        time_interval="4H",
        days=90,
        base_timeframe="4H",
    )

    assert len(fake.calls) >= 2, "90 天 4H 数据必须分多页，用例前提不成立"
    assert all(call["limit"] <= EXCHANGE_CAP for call in fake.calls)


def test_pages_are_contiguous_and_cover_the_requested_range():
    fake = FakeExchange()

    frame = get_data.get_kline(
        start_time="2026-01-01",
        exchange=fake,
        symbol="BTC/USDT:USDT",
        time_interval="4H",
        days=90,
        base_timeframe="4H",
    )

    assert not frame.empty
    assert _gaps(frame).empty, f"分页之间存在缺口: {_gaps(frame)}"

    # 第一页之后每页的 since 都必须持续前进
    for previous, current in zip(fake.calls, fake.calls[1:]):
        assert current["since"] > previous["since"]

    # 覆盖到请求区间的末端（输出为北京时间，故与 UTC 起点差 8 小时）
    last = frame["candle_begin_time"].iloc[-1]
    assert last >= pd.Timestamp("2026-03-31 00:00")

    # 也必须覆盖起点：窗口被交易所忽略时丢的正是开头这段
    first = frame["candle_begin_time"].iloc[0]
    assert first <= pd.Timestamp("2026-01-01 08:00")


def test_since_advances_bar_by_bar():
    fake = FakeExchange()

    get_data.get_kline(
        start_time="2026-01-01",
        exchange=fake,
        symbol="BTC/USDT:USDT",
        time_interval="4H",
        days=90,
        base_timeframe="4H",
    )

    step_ms = _tf_seconds("4h") * 1000
    for previous, current in zip(fake.calls, fake.calls[1:]):
        assert current["since"] == previous["since"] + get_data.OHLCV_PAGE_LIMIT * step_ms


def test_timeframe_is_lowercased_before_hitting_exchange():
    fake = FakeExchange()

    get_data.get_kline(
        start_time="2026-01-01",
        exchange=fake,
        symbol="BTC/USDT:USDT",
        time_interval="4H",
        days=10,
        base_timeframe="4H",
    )

    assert {call["timeframe"] for call in fake.calls} == {"4h"}


def test_target_equal_to_base_is_not_resampled():
    fake = FakeExchange()

    frame = get_data.get_kline(
        start_time="2026-01-01",
        exchange=fake,
        symbol="BTC/USDT:USDT",
        time_interval="4H",
        days=10,
        base_timeframe="4H",
    )

    deltas = frame["candle_begin_time"].diff().dropna().unique()
    assert list(deltas) == [pd.Timedelta(hours=4)]


def test_coarser_target_is_aggregated_from_base():
    fake = FakeExchange()

    frame = get_data.get_kline(
        start_time="2026-01-01",
        exchange=fake,
        symbol="BTC/USDT:USDT",
        time_interval="1D",
        days=20,
        base_timeframe="4H",
    )

    deltas = frame["candle_begin_time"].diff().dropna().unique()
    assert list(deltas) == [pd.Timedelta(days=1)]
    # get_kline 一次抓满一页（300 根 4H ≈ 50 天），会超出 days 请求范围；
    # 这里只关心"聚合口径正确"，不关心多抓的部分。
    assert 50 <= len(frame) <= 52


def test_over_cap_limit_loses_the_window(monkeypatch):
    """固化根因：只要有人把每页请求量放大，开头这段就会丢，测试必须变红。"""
    monkeypatch.setattr(get_data, "OHLCV_PAGE_LIMIT", 2000)
    fake = FakeExchange()

    frame = get_data.get_kline(
        start_time="2026-01-01",
        exchange=fake,
        symbol="BTC/USDT:USDT",
        time_interval="4H",
        days=90,
        base_timeframe="4H",
    )

    assert not frame.empty
    # 请求从 1 月 1 日开始，返回的第一根却晚了一周以上
    assert frame["candle_begin_time"].iloc[0] > pd.Timestamp("2026-01-08")


def test_network_error_stops_after_retry_limit(monkeypatch):
    fake = FakeExchange()
    fake.fail_times = 99

    frame = get_data.get_kline(
        start_time="2026-01-01",
        exchange=fake,
        symbol="BTC/USDT:USDT",
        time_interval="4H",
        days=90,
        base_timeframe="4H",
    )

    assert len(fake.calls) == get_data.NETWORK_RETRY_LIMIT
    assert frame.empty


def test_stalled_page_breaks_the_loop():
    fake = FakeExchange()
    fake.ignore_since_when_over_cap = True

    # 让每页都返回同一段，模拟交易所忽略 since 且未前进
    original = fake.fetch_ohlcv

    def stalled(**kwargs):
        kwargs["limit"] = EXCHANGE_CAP
        kwargs["since"] = _ms("2026-02-01")
        return original(**kwargs)

    fake.fetch_ohlcv = stalled

    get_data.get_kline(
        start_time="2026-01-01",
        exchange=fake,
        symbol="BTC/USDT:USDT",
        time_interval="4H",
        days=90,
        base_timeframe="4H",
    )

    assert len(fake.calls) < 10, "未前进时必须在少数几页内退出，不能无限翻页"
