"""Test doubles for the btc_strategy data pipeline."""

from __future__ import annotations

import pandas as pd

BAR_MS = 300_000


def candle_rows(start, count, *, step_ms=BAR_MS, confirm_last=False, offset_ms=0):
    """Build OKX-shaped candle rows (oldest first) with distinct prices."""
    base = int(pd.Timestamp(start, tz="UTC").value // 1_000_000) + offset_ms
    rows = []
    for index in range(count):
        confirm = 0 if (confirm_last and index == count - 1) else 1
        rows.append(
            (
                base + index * step_ms,
                100.0 + index,
                101.0 + index,
                99.0 + index,
                100.5 + index,
                1.0 + index,
                confirm,
            )
        )
    return rows


class FakeOkx:
    """Stands in for the ccxt implicit API used by btc_strategy.okx_history."""

    def __init__(self, rows=(), *, failures: int = 0):
        self.rows = list(rows)
        self.calls: list[dict[str, str]] = []
        self.failures = failures

    def publicGetMarketHistoryCandles(self, params):
        self.calls.append(dict(params))
        if self.failures > 0:
            self.failures -= 1
            raise RuntimeError("simulated OKX failure")
        after = int(params["after"])
        limit = int(params["limit"])
        selected = [row for row in self.rows if row[0] < after][-limit:]
        selected.reverse()  # OKX returns the newest candle first
        return {
            "data": [
                [
                    str(row[0]),
                    str(row[1]),
                    str(row[2]),
                    str(row[3]),
                    str(row[4]),
                    str(row[5]),
                    "0",
                    "0",
                    str(row[6]),
                ]
                for row in selected
            ]
        }
