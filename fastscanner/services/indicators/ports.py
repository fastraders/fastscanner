from dataclasses import dataclass
from datetime import date
from typing import Protocol

import pandas as pd


class CandleStore(Protocol):
    def get(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame: ...


class FundamentalDataStore(Protocol):
    def get(self, symbol: str) -> "FundamentalData": ...


@dataclass
class FundamentalData:
    exchange: str
    country: str
    city: str
    gic_industry: str
    gic_sector: str
    historical_market_cap: "pd.Series[float]"
    earnings_dates: pd.DatetimeIndex
    insiders_ownership_perc: float
    institutional_ownership_perc: float
    shares_float: float


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
