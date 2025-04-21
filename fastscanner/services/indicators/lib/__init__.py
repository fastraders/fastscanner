from datetime import date, datetime
from typing import Any, Protocol

import pandas as pd


class Indicator(Protocol):
    def calculate(
        self, symbol: str, start: datetime, end: datetime | None, freq: str
    ) -> pd.Series: ...

    @classmethod
    def type(cls) -> str: ...

    def column_name(self) -> str: ...


class IndicatorsLibrary:
    def __init__(self):
        self.indicators: dict[str, type[Indicator]] = {}

    def get(self, type_: str, params: dict[str, Any]) -> Indicator:
        if type_ not in self.indicators:
            raise ValueError(f"Indicator {type_} not found.")
        indicator_class = self.indicators[type_]
        return indicator_class(**params)

    def register(self, indicator: type[Indicator]) -> None:
        self.indicators[indicator.type()] = indicator
