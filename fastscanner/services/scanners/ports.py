from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

import pandas as pd


@dataclass
class ScannerParams:
    type_: str
    params: dict[str, Any]


class Scanner(Protocol):
    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        """
        Scan the symbol with the given parameters.
        """
        ...

    async def scan_realtime(
        self, symbol: str, new_row: dict[str, Any], freq: str
    ) -> tuple[dict[str, Any], bool]: ...

    def id(self) -> str: ...

    @classmethod
    def type(cls) -> str: ...


class SymbolsProvider(Protocol):
    async def active_symbols(self, exchanges: list[str] | None = None) -> list[str]: ...
