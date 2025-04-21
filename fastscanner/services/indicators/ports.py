from datetime import date
from typing import Protocol

import pandas as pd


class CandleStore(Protocol):
    def get(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame: ...


class FundamentalDataStore(Protocol):
    def get(self, symbol: str, start: date, end: date) -> list[dict]: ...


class CandleCol:
    DATETIME = "datetime"
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    VOLUME = "volume"

    RESAMPLE_MAP = {
        OPEN: "first",
        HIGH: "max",
        LOW: "min",
        CLOSE: "last",
        VOLUME: "sum",
    }

    COLUMNS = list(RESAMPLE_MAP.keys())
