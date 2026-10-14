from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd

from btc_strategy.backtest import atomic_json, build_payload, complete_4h
from btc_strategy.data import load_csv, validate_5m_continuity
from btc_strategy.data_sync import (
    DEFAULT_DATA_PATH,
    DEFAULT_SYMBOL,
    DEFAULT_TIMEFRAME,
    sync_btc_data,
)

DEFAULT_PAYLOAD_PATH = Path("data/btc_strategy/backtests/latest_payload.json")
DEFAULT_STATE_PATH = Path("data/btc_strategy/worker_state.json")
DEFAULT_PID_PATH = Path("data/btc_strategy/worker.pid")
DEFAULT_LOG_PATH = Path("data/btc_strategy/worker.log")
DEFAULT_RUN_TIMES = ((0, 5), (4, 5), (8, 5), (12, 5), (16, 5), (20, 5))


def utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def parse_run_times(value: str) -> tuple[tuple[int, int], ...]:
    parsed = []
    for item in value.split(","):
        hour_text, minute_text = item.strip().split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
        if not 0 <= hour <= 23 or not 0 <= minute <= 59:
            raise ValueError(f"invalid run time: {item}")
        parsed.append((hour, minute))
    if not parsed:
        raise ValueError("at least one run time is required")
    return tuple(sorted(set(parsed)))


def next_run_at(now: pd.Timestamp, run_times: tuple[tuple[int, int], ...] = DEFAULT_RUN_TIMES) -> pd.Timestamp:
    timestamp = pd.Timestamp(now)
    timestamp = timestamp.tz_convert("UTC") if timestamp.tzinfo else timestamp.tz_localize("UTC")
    for hour, minute in run_times:
        candidate = timestamp.normalize() + pd.Timedelta(hours=hour, minutes=minute)
        if candidate > timestamp:
            return candidate
    return timestamp.normalize() + pd.Timedelta(days=1, hours=run_times[0][0], minutes=run_times[0][1])


def process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def read_pid(path: Path = DEFAULT_PID_PATH) -> int | None:
    if not path.exists():
        return None
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (TypeError, ValueError):
        return None


def acquire_pid(path: Path = DEFAULT_PID_PATH) -> None:
    existing = read_pid(path)
    if existing and process_alive(existing):
        raise RuntimeError(f"btc strategy worker is already running with pid {existing}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(os.getpid()), encoding="utf-8")


def release_pid(path: Path = DEFAULT_PID_PATH) -> None:
    if read_pid(path) == os.getpid():
        path.unlink(missing_ok=True)


def configure_logging(log_path: Path = DEFAULT_LOG_PATH) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
    )


def payload_summary(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    datasets = {}
    for key, item in payload.get("datasets", {}).items():
        meta = item.get("meta", {})
        datasets[key] = {
            "tradeCount": meta.get("tradeCount"),
            "openPositionCount": meta.get("openPositionCount"),
            "finalEquity": meta.get("finalEquity"),
            "dataEnd": meta.get("dataEnd"),
            "pendingAction": meta.get("pendingAction"),
        }
    return {"default": payload.get("default"), "datasets": datasets}


def write_state(state_path: Path, state: dict[str, Any]) -> None:
    atomic_json(state, state_path)


def run_once(
    *,
    data_path: str | Path = DEFAULT_DATA_PATH,
    payload_path: str | Path = DEFAULT_PAYLOAD_PATH,
    state_path: str | Path = DEFAULT_STATE_PATH,
    symbol: str = DEFAULT_SYMBOL,
    timeframe: str = DEFAULT_TIMEFRAME,
    dry_run: bool = False,
    exchange: Any | None = None,
) -> dict[str, Any]:
    started_at = utc_now()
    sync_report = sync_btc_data(
        data_path,
        symbol=symbol,
        timeframe=timeframe,
        repair=True,
        write=True,
        exchange=exchange,
    )
    frame = load_csv(data_path)
    continuity = validate_5m_continuity(frame)
    if continuity["gap_count"]:
        raise RuntimeError(f"5m continuity check failed: {continuity}")
    payload = build_payload(data_path)
    atomic_json(payload, payload_path)
    completed = complete_4h(frame)
    state = {
        "status": "success",
        "dryRun": bool(dry_run),
        "startedAt": started_at.isoformat(),
        "finishedAt": utc_now().isoformat(),
        "sync": sync_report.as_dict(),
        "continuity": {
            "rows": continuity["rows"],
            "start": continuity["start"].isoformat(),
            "end": continuity["end"].isoformat(),
            "gapCount": continuity["gap_count"],
        },
        "completed4h": {
            "rows": len(completed),
            "end": completed["candle_begin_time"].iloc[-1].isoformat() if not completed.empty else None,
        },
        "payload": payload_summary(Path(payload_path)),
    }
    write_state(Path(state_path), state)
    return state


def check_status(
    *,
    data_path: str | Path = DEFAULT_DATA_PATH,
    payload_path: str | Path = DEFAULT_PAYLOAD_PATH,
    state_path: str | Path = DEFAULT_STATE_PATH,
    pid_path: str | Path = DEFAULT_PID_PATH,
    symbol: str = DEFAULT_SYMBOL,
    timeframe: str = DEFAULT_TIMEFRAME,
    exchange: Any | None = None,
) -> dict[str, Any]:
    report = sync_btc_data(
        data_path,
        symbol=symbol,
        timeframe=timeframe,
        repair=False,
        write=False,
        exchange=exchange,
    )
    frame = load_csv(data_path)
    continuity = validate_5m_continuity(frame)
    completed = complete_4h(frame)
    result: dict[str, Any] = {
        "mode": "check",
        "dataPath": str(Path(data_path).resolve()),
        "missingRanges": [
            {"start": item.start.isoformat(), "end": item.end.isoformat()}
            for item in report.missing_ranges_before
        ],
        "rows": continuity["rows"],
        "dataStart": continuity["start"].isoformat(),
        "dataEnd": continuity["end"].isoformat(),
        "gapCount": continuity["gap_count"],
        "completed4hEnd": completed["candle_begin_time"].iloc[-1].isoformat() if not completed.empty else None,
    }
    return result


def status_report(
    *,
    data_path: str | Path = DEFAULT_DATA_PATH,
    payload_path: str | Path = DEFAULT_PAYLOAD_PATH,
    state_path: str | Path = DEFAULT_STATE_PATH,
    pid_path: str | Path = DEFAULT_PID_PATH,
) -> dict[str, Any]:
    pid = read_pid(Path(pid_path))
    state = None
    if Path(state_path).exists():
        state = json.loads(Path(state_path).read_text(encoding="utf-8"))
    payload = payload_summary(Path(payload_path)) if Path(payload_path).exists() else None
    return {
        "mode": "status",
        "running": bool(pid and process_alive(pid)),
        "pid": pid,
        "dataPath": str(Path(data_path).resolve()),
        "dataModifiedAt": pd.Timestamp(Path(data_path).stat().st_mtime, unit="s", tz="UTC").isoformat()
        if Path(data_path).exists()
        else None,
        "payloadPath": str(Path(payload_path).resolve()),
        "payloadModifiedAt": pd.Timestamp(Path(payload_path).stat().st_mtime, unit="s", tz="UTC").isoformat()
        if Path(payload_path).exists()
        else None,
        "lastRun": state,
        "payload": payload,
    }


def scheduler_loop(
    *,
    data_path: str | Path = DEFAULT_DATA_PATH,
    payload_path: str | Path = DEFAULT_PAYLOAD_PATH,
    state_path: str | Path = DEFAULT_STATE_PATH,
    pid_path: str | Path = DEFAULT_PID_PATH,
    symbol: str = DEFAULT_SYMBOL,
    timeframe: str = DEFAULT_TIMEFRAME,
    run_times: tuple[tuple[int, int], ...] = DEFAULT_RUN_TIMES,
) -> None:
    acquire_pid(Path(pid_path))
    logging.info("BTC worker config: data=%s payload=%s", data_path, payload_path)
    stop = False

    def handle_stop(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)
    try:
        logging.info("running initial BTC strategy update")
        try:
            state = run_once(
                data_path=data_path,
                payload_path=payload_path,
                state_path=state_path,
                symbol=symbol,
                timeframe=timeframe,
            )
            logging.info("initial update complete: %s", json.dumps(state["payload"], ensure_ascii=False))
        except Exception:
            logging.exception("initial BTC strategy update failed")

        while not stop:
            next_run = next_run_at(utc_now(), run_times)
            logging.info("next BTC strategy update at %s", next_run.isoformat())
            while not stop and utc_now() < next_run:
                time.sleep(min(30.0, max(0.1, (next_run - utc_now()).total_seconds())))
            if stop:
                break
            try:
                state = run_once(
                    data_path=data_path,
                    payload_path=payload_path,
                    state_path=state_path,
                    symbol=symbol,
                    timeframe=timeframe,
                )
                logging.info("scheduled update complete: %s", json.dumps(state["payload"], ensure_ascii=False))
            except Exception:
                logging.exception("scheduled BTC strategy update failed")
    finally:
        release_pid(Path(pid_path))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="BTC strategy data sync, backtest, and scheduler")
    parser.add_argument("--check", action="store_true", help="check data completeness without writing")
    parser.add_argument("--status", action="store_true", help="show worker and payload status")
    parser.add_argument("--once", action="store_true", help="run one full update and exit")
    parser.add_argument("--dry-run", action="store_true", help="run once without sending alerts")
    parser.add_argument("--at", default="00:05,04:05,08:05,12:05,16:05,20:05", help="UTC schedule HH:MM,HH:MM")
    parser.add_argument("--data-path", default=str(DEFAULT_DATA_PATH))
    parser.add_argument("--payload-path", default=str(DEFAULT_PAYLOAD_PATH))
    parser.add_argument("--state-path", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--pid-path", default=str(DEFAULT_PID_PATH))
    parser.add_argument("--log-path", default=str(DEFAULT_LOG_PATH))
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    args = parser.parse_args(argv)

    configure_logging(Path(args.log_path))
    common = {
        "data_path": args.data_path,
        "payload_path": args.payload_path,
        "state_path": args.state_path,
        "symbol": args.symbol,
        "timeframe": args.timeframe,
    }

    if args.check:
        print(json.dumps(check_status(**common), ensure_ascii=False, indent=2))
        return 0
    if args.status:
        print(
            json.dumps(
                status_report(
                    data_path=args.data_path,
                    payload_path=args.payload_path,
                    state_path=args.state_path,
                    pid_path=args.pid_path,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if args.once or args.dry_run:
        state = run_once(**common, dry_run=args.dry_run)
        print(json.dumps(state, ensure_ascii=False, indent=2))
        return 0

    scheduler_loop(**common, pid_path=args.pid_path, run_times=parse_run_times(args.at))
    return 0


if __name__ == "__main__":
    sys.exit(main())
