from datetime import date, datetime
from typing import Any, Protocol

from fastscanner.pkg.types import TimeSeries


class Indicator:
    def calculate(self, start: datetime, end: datetime | None) -> TimeSeries: ...


class IndicatorsLibrary:
    def get(self, type_: str, params: dict[str, Any]) -> Indicator: ...
