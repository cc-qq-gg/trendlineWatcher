#!/usr/bin/env python3

import argparse
import hashlib
import hmac
import json
import os
import ssl
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


DEFAULT_PATH = "/api/risk/order"
DEFAULT_CA_CERT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "server-cert.pem"
)
DEFAULT_BASE_URL = "https://36.14.43.234:8888"
DEFAULT_TRADER_ID = "binance_deepseek"
DEFAULT_API_KEY = "5527e00c26a0daf2221047e4f9917413"
DEFAULT_API_SECRET = "5c265f34cc67141462d1f7c74907a6d2b645293f305f60fe16223e9c9e8ed050"


def build_signature(
    secret: str, method: str, path: str, timestamp: str, nonce: str, body: bytes
) -> str:
    body_hash = hashlib.sha256(body).hexdigest()
    payload = "\n".join(
        [
            method.upper(),
            path,
            timestamp,
            nonce,
            body_hash,
        ]
    )
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


@dataclass
class RiskOrderConfig:
    base_url: str
    trader_id: str
    api_key: str
    api_secret: str
    ca_cert: str
    timeout: int = 30
    path: str = DEFAULT_PATH


class RiskOrderClient:
    def __init__(self, config: RiskOrderConfig):
        self.config = config

    def place_order(
        self, symbol: str, side: str, client_order_id: str | None = None
    ) -> dict[str, Any]:
        request_body = {
            "symbol": symbol,
            "side": side,
            "client_order_id": client_order_id or str(uuid.uuid4()),
        }
        return self._post(self.config.path, request_body)

    def place_long(
        self, symbol: str, client_order_id: str | None = None
    ) -> dict[str, Any]:
        return self.place_order(symbol, "LONG", client_order_id)

    def place_short(
        self, symbol: str, client_order_id: str | None = None
    ) -> dict[str, Any]:
        return self.place_order(symbol, "SHORT", client_order_id)

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        timestamp = str(int(time.time() * 1000))
        nonce = str(uuid.uuid4())
        body = json.dumps(payload, separators=(",", ":")).encode()
        signature = build_signature(
            self.config.api_secret, "POST", path, timestamp, nonce, body
        )

        url = self._build_url(path)
        request = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-API-KEY": self.config.api_key,
                "X-TIMESTAMP": timestamp,
                "X-NONCE": nonce,
                "X-SIGNATURE": signature,
            },
        )

        try:
            with urllib.request.urlopen(
                request,
                timeout=self.config.timeout,
                context=self._build_ssl_context(),
            ) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode()
            raise RuntimeError(f"HTTP {exc.code}: {error_body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"request failed: {exc.reason}") from exc

    def _build_url(self, path: str) -> str:
        base = self.config.base_url.rstrip("/")
        query = urllib.parse.urlencode({"trader_id": self.config.trader_id})
        return f"{base}{path}?{query}"

    def _build_ssl_context(self) -> ssl.SSLContext:
        if not os.path.exists(self.config.ca_cert):
            raise RuntimeError(
                f"CA certificate not found: {self.config.ca_cert}. "
                "Copy server-cert.pem to the client machine first."
            )
        return ssl.create_default_context(cafile=self.config.ca_cert)


def create_default_client(
    base_url: str = DEFAULT_BASE_URL,
    trader_id: str = DEFAULT_TRADER_ID,
    api_key: str = DEFAULT_API_KEY,
    api_secret: str = DEFAULT_API_SECRET,
    ca_cert: str = DEFAULT_CA_CERT,
    timeout: int = 30,
) -> RiskOrderClient:
    return RiskOrderClient(
        RiskOrderConfig(
            base_url=base_url,
            trader_id=trader_id,
            api_key=api_key,
            api_secret=api_secret,
            ca_cert=ca_cert,
            timeout=timeout,
        )
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Call nofx risk order API")
    parser.add_argument(
        "--base-url", default="https://127.0.0.1:8888", help="API base URL"
    )
    parser.add_argument("--trader-id", required=True, help="Trader ID")
    parser.add_argument("--symbol", required=True, help="Trading symbol, e.g. BTCUSDT")
    parser.add_argument(
        "--side", required=True, choices=["LONG", "SHORT"], help="Order side"
    )
    parser.add_argument("--api-key", required=True, help="Risk API key")
    parser.add_argument("--api-secret", required=True, help="Risk API secret")
    parser.add_argument(
        "--client-order-id", default="", help="Optional client order id"
    )
    parser.add_argument(
        "--ca-cert", default=DEFAULT_CA_CERT, help="Trusted server certificate path"
    )
    parser.add_argument(
        "--timeout", type=int, default=30, help="Request timeout in seconds"
    )
    return parser.parse_args()


def main() -> None:
    # args = parse_args()
    # client = create_default_client(
    #     base_url=args.base_url,
    #     trader_id=args.trader_id,
    #     api_key=args.api_key,
    #     api_secret=args.api_secret,
    #     ca_cert=args.ca_cert,
    #     timeout=args.timeout,
    # )
    # result = client.place_order(args.symbol, args.side, args.client_order_id or None)
    # print(json.dumps(result, ensure_ascii=False, indent=2))
    client = create_default_client()
    result = client.place_long("BTCUSDT")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
