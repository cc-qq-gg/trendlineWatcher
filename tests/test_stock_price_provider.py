import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from http.client import RemoteDisconnected

import requests

from stock_price_provider import (
    EastmoneyPriceProvider,
    FallbackPriceProvider,
    SinaPriceProvider,
    eastmoney_secid,
)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.urls = []

    def get(self, url, params=None, timeout=None, headers=None):
        self.urls.append((url, params, timeout, headers))
        return FakeResponse(self.payload)


class FakeTextResponse:
    def __init__(self, text):
        self.text = text

    def raise_for_status(self):
        return None


class FakeTextSession:
    def __init__(self, text, responses=None):
        self.text = text
        self.responses = responses or {}
        self.urls = []

    def get(self, url, params=None, timeout=None, headers=None):
        self.urls.append((url, params, timeout, headers))
        for key, text in self.responses.items():
            if key in url:
                return FakeTextResponse(text)
        return FakeTextResponse(self.text)


def test_eastmoney_provider_parses_latest_daily_kline():
    payload = {
        "data": {
            "code": "600000",
            "name": "浦发银行",
            "klines": [
                "2026-09-11,10.00,10.30,10.50,9.90,100,1000,0,0,0,0",
            ],
        }
    }
    session = FakeSession(payload)

    quote = EastmoneyPriceProvider(session=session).latest_daily_quote("sh.600000")

    assert quote == {
        "date": "2026-09-11",
        "code": "sh.600000",
        "name": "浦发银行",
        "open": 10.0,
        "close": 10.3,
        "high": 10.5,
        "low": 9.9,
        "tradestatus": "1",
    }
    assert session.urls[0][1]["secid"] == "1.600000"


def test_eastmoney_provider_default_session_ignores_environment_proxy():
    provider = EastmoneyPriceProvider()

    assert provider.session.trust_env is False


def test_sina_provider_parses_latest_daily_kline():
    session = FakeTextSession(
        '[{"day":"2026-09-11","open":"41.750","high":"41.750","low":"41.050","close":"41.350","volume":"73798295"}]'
    )

    quote = SinaPriceProvider(session=session).latest_daily_quote("sh.600036")

    assert quote == {
        "date": "2026-09-11",
        "code": "sh.600036",
        "name": "",
        "open": 41.75,
        "close": 41.35,
        "high": 41.75,
        "low": 41.05,
        "tradestatus": "1",
    }


def test_sina_provider_adds_stock_name_from_realtime_quote():
    session = FakeTextSession(
        "",
        responses={
            "CN_MarketData.getKLineData": (
                '[{"day":"2026-09-11","open":"41.750","high":"41.750","low":"41.050","close":"41.350","volume":"73798295"}]'
            ),
            "hq.sinajs.cn": (
                'var hq_str_sh600036="招商银行,41.750,41.570,41.350,41.750,41.050,41.330,41.350,73798295";'
            ),
        },
    )

    quote = SinaPriceProvider(session=session).latest_daily_quote("sh.600036")

    assert quote["name"] == "招商银行"


def test_fallback_provider_uses_sina_when_primary_disconnects():
    class BrokenProvider:
        def latest_daily_quote(self, code):
            raise requests.ConnectionError(RemoteDisconnected("Remote end closed connection without response"))

    class BackupProvider:
        def latest_daily_quote(self, code):
            return {"code": code, "close": 1}

    quote = FallbackPriceProvider(primary=BrokenProvider(), backup=BackupProvider()).latest_daily_quote("sh.600036")

    assert quote == {"code": "sh.600036", "close": 1}


def test_eastmoney_secid_normalizes_a_share_codes():
    assert eastmoney_secid("600000") == ("sh.600000", "1.600000")
    assert eastmoney_secid("000001") == ("sz.000001", "0.000001")
    assert eastmoney_secid("sh.600000") == ("sh.600000", "1.600000")
