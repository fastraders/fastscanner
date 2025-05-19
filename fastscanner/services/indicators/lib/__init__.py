from contextvars import ContextVar
from datetime import date, datetime
from typing import Any, Hashable, Protocol

import pandas as pd

from .candle import (
    ATRIndicator,
    CumulativeDailyVolumeIndicator,
    PositionInRangeIndicator,
    PremarketCumulativeIndicator,
)
from .daily import (
    DailyATRGapIndicator,
    DailyATRIndicator,
    DailyGapIndicator,
    PrevDayIndicator,
)
from .fundamental import DaysFromEarningsIndicator, DaysToEarningsIndicator

_indicators: list[type["Indicator"]] = [
    CumulativeDailyVolumeIndicator,
    PremarketCumulativeIndicator,
    PositionInRangeIndicator,
    ATRIndicator,
    DailyGapIndicator,
    PrevDayIndicator,
    DaysFromEarningsIndicator,
    DaysToEarningsIndicator,
    DailyATRIndicator,
    DailyATRGapIndicator,
]


class Indicator(Protocol):
    async def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extend the DataFrame with the indicator's values.
        """
        ...

    @classmethod
    def type(cls) -> str: ...

    def column_name(self) -> str: ...

    def lookback_days(self) -> int:
        """
        Returns the number of days that the indicator needs to look back in order to calculate its value.
        """
        ...

    async def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series: ...


class IndicatorsLibrary:
    _instance: ContextVar["IndicatorsLibrary"] = ContextVar("IndicatorsLibrary")

    def __init__(self):
        self._indicators: dict[str, type[Indicator]] = {}

    def get(self, type_: str, params: dict[str, Hashable]) -> Indicator:
        if type_ not in self._indicators:
            raise ValueError(f"Indicator {type_} not found.")
        indicator_class = self._indicators[type_]
        return indicator_class(**params)

    def register(self, indicator: type[Indicator]) -> None:
        self._indicators[indicator.type()] = indicator

    @classmethod
    def instance(cls) -> "IndicatorsLibrary":
        try:
            return cls._instance.get()
        except LookupError:
            instance = cls()
            for indicator in _indicators:
                instance.register(indicator)
            cls._instance.set(instance)
            return instance
