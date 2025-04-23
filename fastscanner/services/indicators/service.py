from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from .lib import IndicatorsLibrary
from .ports import CandleStore, FundamentalDataStore
from .registry import ApplicationRegistry


@dataclass
class IndicatorParams:
    type_: str
    params: dict[str, Any]


class IndicatorsService:
    def __init__(
        self, candles: CandleStore, fundamentals: FundamentalDataStore
    ) -> None:
        self.candles = candles
        self.fundamentals = fundamentals
        ApplicationRegistry.init(candles, fundamentals)

    def calculate(
        self,
        symbol: str,
        start: date,
        end: date,
        freq: str,
        indicators: list[IndicatorParams],
    ) -> pd.DataFrame:
        df = self.candles.get(symbol, start, end, freq)
        if df.empty:
            return df

        for params in indicators:
            indicator = IndicatorsLibrary().get(params.type_, params.params)
            df = indicator.extend(df)
        return df
