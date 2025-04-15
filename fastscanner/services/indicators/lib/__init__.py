from datetime import date, datetime
from typing import Any, Protocol

import pandas as pd


class Indicator:
    def calculate(self, start: datetime, end: datetime | None) -> pd.Series: ...


class IndicatorsLibrary:
    def get(self, type_: str, params: dict[str, Any]) -> Indicator: ...
