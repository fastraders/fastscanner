from datetime import date
from typing import Protocol

import pandas as pd


class Scanner(Protocol):
    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        """
        Scan the symbol with the given parameters.
        """
        ...
