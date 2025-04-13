from datetime import datetime
from typing import Any

from fastscanner.pkg.types import TimeSeries

from .ports import CandleStore, FundamentalDataStore
from .registry import ApplicationRegistry


class IndicatorsCalculator:
    def __init__(
        self, candles: CandleStore, fundamentals: FundamentalDataStore
    ) -> None:
        self.candles = candles
        self.fundamentals = fundamentals
        ApplicationRegistry.init(candles, fundamentals)

    def calculate(
        self, type_: str, start: datetime, end: datetime, params: dict[str, Any]
    ) -> TimeSeries: ...

    def calculate_after(
        self, type_: str, start: datetime, params: dict[str, Any]
    ) -> TimeSeries: ...
