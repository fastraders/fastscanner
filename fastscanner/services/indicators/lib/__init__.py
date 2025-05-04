from contextvars import ContextVar
from datetime import date, datetime
from typing import Any, Protocol

import pandas as pd

from .candle import CumulativeDailyVolumeIndicator, PremarketCumulativeIndicator

_indicators: list[type["Indicator"]] = [
    CumulativeDailyVolumeIndicator,
    PremarketCumulativeIndicator,
]


class Indicator(Protocol):
    def extend(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extend the DataFrame with the indicator's values.
        """
        ...

    @classmethod
    def type(cls) -> str: ...

    def column_name(self) -> str: ...


class RealtimeIndicator(Indicator, Protocol):
    def extend_realtime(self, symbol: str, new_row: pd.Series) -> pd.Series: ...


class IndicatorsLibrary:
    _instance: ContextVar["IndicatorsLibrary"] = ContextVar("IndicatorsLibrary")

    def __init__(self):
        self.indicators: dict[str, type[Indicator]] = {}

    def get(self, type_: str, params: dict[str, Any]) -> Indicator:
        if type_ not in self.indicators:
            raise ValueError(f"Indicator {type_} not found.")
        indicator_class = self.indicators[type_]
        return indicator_class(**params)

    def register(self, indicator: type[Indicator]) -> None:
        self.indicators[indicator.type()] = indicator

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
