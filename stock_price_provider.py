import json

import requests

from StockMonitorManager import normalize_stock_code


EASTMONEY_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
SINA_KLINE_URL = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
SINA_REALTIME_URL = "https://hq.sinajs.cn/list={symbol}"


def eastmoney_secid(code):
    """转换为东方财富secid。"""
    normalized = normalize_stock_code(code)
    market = "1" if normalized.startswith("sh.") else "0"
    return normalized, f"{market}.{normalized.split('.', 1)[1]}"


class EastmoneyPriceProvider:
    """东方财富日K数据源。"""

    def __init__(self, session=None, timeout=10):
        self.session = session or requests.Session()
        if session is None:
            self.session.trust_env = False
        self.timeout = timeout

    def latest_daily_quote(self, code):
        normalized_code, secid = eastmoney_secid(code)
        response = self.session.get(
            EASTMONEY_KLINE_URL,
            params={
                "secid": secid,
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": "101",
                "fqt": "0",
                "end": "20500101",
                "lmt": "1",
            },
            timeout=self.timeout,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://quote.eastmoney.com/",
            },
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") or {}
        klines = data.get("klines") or []
        if not klines:
            return None

        fields = klines[-1].split(",")
        if len(fields) < 5:
            raise RuntimeError("东方财富日K数据格式异常")

        return {
            "date": fields[0],
            "code": normalized_code,
            "name": data.get("name", ""),
            "open": float(fields[1]),
            "close": float(fields[2]),
            "high": float(fields[3]),
            "low": float(fields[4]),
            "tradestatus": "1",
        }


def sina_symbol(code):
    """转换为新浪股票代码。"""
    normalized = normalize_stock_code(code)
    return normalized, normalized.replace(".", "")


class SinaPriceProvider:
    """新浪日K备用数据源。"""

    def __init__(self, session=None, timeout=10):
        self.session = session or requests.Session()
        if session is None:
            self.session.trust_env = False
        self.timeout = timeout

    def latest_daily_quote(self, code):
        normalized_code, symbol = sina_symbol(code)
        response = self.session.get(
            SINA_KLINE_URL,
            params={
                "symbol": symbol,
                "scale": "240",
                "ma": "no",
                "datalen": "1",
            },
            timeout=self.timeout,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        response.raise_for_status()
        rows = json.loads(response.text)
        if not rows:
            return None
        row = rows[-1]
        name = self._stock_name(symbol)
        return {
            "date": row["day"],
            "code": normalized_code,
            "name": name,
            "open": float(row["open"]),
            "close": float(row["close"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "tradestatus": "1",
        }

    def _stock_name(self, symbol):
        try:
            response = self.session.get(
                SINA_REALTIME_URL.format(symbol=symbol),
                timeout=self.timeout,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Referer": "https://finance.sina.com.cn/",
                },
            )
            response.raise_for_status()
            text = response.text
            if '="' not in text:
                return ""
            payload = text.split('="', 1)[1].split('";', 1)[0]
            return payload.split(",", 1)[0]
        except requests.RequestException:
            return ""


class FallbackPriceProvider:
    """主源失败时切到备用源。"""

    def __init__(self, primary=None, backup=None):
        self.primary = primary or EastmoneyPriceProvider()
        self.backup = backup or SinaPriceProvider()

    def latest_daily_quote(self, code):
        try:
            return self.primary.latest_daily_quote(code)
        except requests.RequestException:
            return self.backup.latest_daily_quote(code)
