from __future__ import annotations

from pathlib import Path
import pandas as pd


RAW_COLUMNS = ["candle_begin_time", "open", "high", "low", "close", "volume"]


def load_csv(path: str | Path) -> pd.DataFrame:
    return normalize_ohlcv(pd.read_csv(path))


def normalize_ohlcv(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize, sort and deduplicate BTC candles."""
    frame = frame.copy()
    frame.columns = [str(c).strip() for c in frame.columns]
    if "candle_begin_time_GMT8" in frame.columns and "candle_begin_time" not in frame.columns:
        frame = frame.rename(columns={"candle_begin_time_GMT8": "candle_begin_time"})
    missing = set(RAW_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"missing columns: {sorted(missing)}")
    frame["candle_begin_time"] = pd.to_datetime(frame["candle_begin_time"], utc=True)
    for col in RAW_COLUMNS[1:]:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.dropna(subset=RAW_COLUMNS).sort_values("candle_begin_time")
    return frame.drop_duplicates("candle_begin_time", keep="last").reset_index(drop=True)[RAW_COLUMNS]


def resample_4h(frame: pd.DataFrame) -> pd.DataFrame:
    frame = normalize_ohlcv(frame).set_index("candle_begin_time")
    out = frame.resample("4h", label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return out


def validate_5m_continuity(frame: pd.DataFrame) -> dict:
    frame = normalize_ohlcv(frame)
    delta = frame["candle_begin_time"].diff().dropna()
    gaps = frame.loc[delta[delta != pd.Timedelta(minutes=5)].index]
    return {"rows": len(frame), "start": frame.candle_begin_time.iloc[0], "end": frame.candle_begin_time.iloc[-1], "gap_count": len(gaps)}


def atomic_csv(frame: pd.DataFrame, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(tmp, index=False)
    tmp.replace(path)
