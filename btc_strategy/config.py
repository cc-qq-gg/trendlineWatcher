from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class StrategyConfig:
    name: str
    ma_len: int = 11
    shift: int = 77
    slope_threshold: float = 0.01
    stop_loss: float = 0.08
    base_leverage: float = 2.2
    risk_off_leverage: float = 0.8
    risk_on_leverage: float = 3.0
    timeframe: str = "4h"
    execution_model: str = "next_open_after_completed_4h"
    version: str = "btc-aggressive-2026-10-v1"

    def manifest(self):
        return asdict(self)


AGGRESSIVE = StrategyConfig(name="Aggressive")
