from datetime import date
from typing import Protocol

from fastscanner.pkg.types import CandlesDataFrame


class CandleStore(Protocol):
    def get(self, symbol: str, start: date, end: date) -> CandlesDataFrame: ...


class FundamentalDataStore(Protocol):
    def get(self, symbol: str, start: date, end: date) -> list[dict]: ...
