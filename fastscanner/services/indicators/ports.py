from datetime import date
from typing import Protocol

import pandas as pd


class CandleStore(Protocol):
    def get(self, symbol: str, start: date, end: date, freq: str) -> pd.DataFrame: ...


class FundamentalDataStore(Protocol):
    def get(self, symbol: str, start: date, end: date) -> list[dict]: ...
