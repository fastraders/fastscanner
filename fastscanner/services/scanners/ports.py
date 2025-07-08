from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

import pandas as pd


@dataclass
class ScannerParams:
    type_: str
    params: dict[str, Any]


@dataclass
class ScanAllResult:
    results: list[dict[str, Any]]
    total_symbols: int
    scanner_type: str


class Scanner(Protocol):
    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        """
        Scan the symbol with the given parameters.
        """
        ...

    async def scan_realtime(
        self, symbol: str, new_row: pd.Series, freq: str
    ) -> tuple[pd.Series, bool]: ...

    def id(self) -> str: ...

    @classmethod
    def type(cls) -> str: ...


class SymbolsProvider(Protocol):
    async def active_symbols(self, exchanges: list[str] | None = None) -> list[str]: ...
