from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

import pandas as pd


class CandleStore(Protocol):
    async def get(
        self, symbol: str, start: date, end: date, freq: str, adjusted: bool = True
    ) -> pd.DataFrame: ...


class FundamentalDataStore(Protocol):
    async def get(self, symbol: str) -> "FundamentalData": ...


class Cache(Protocol):
    def save(self, key: str, value: str) -> None: ...

    def get(self, key: str) -> str: ...


@dataclass
class FundamentalData:
    type: str
    exchange: str
    country: str
    city: str
    gic_industry: str
    gic_sector: str
    historical_market_cap: "pd.Series[float]"
    earnings_dates: pd.DatetimeIndex
    insiders_ownership_perc: float | None
    institutional_ownership_perc: float | None
    shares_float: float | None
    beta: float | None


class PublicHolidaysStore(Protocol):
    def get(self) -> set[date]: ...


class Channel(Protocol):
    async def subscribe(self, channel_id: str, handler: "ChannelHandler"): ...

    async def unsubscribe(self, channel_id: str, handler_id: str): ...

    async def push(self, channel_id: str, data: dict, flush: bool = True): ...

    async def flush(self): ...

    async def reset(self): ...


class ChannelHandler(Protocol):
    async def handle(self, channel_id: str, data: dict): ...

    def id(self) -> str: ...


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
