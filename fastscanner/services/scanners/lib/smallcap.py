import math
from datetime import date, time

import pandas as pd

from fastscanner.services.indicators.lib.candle import (
    ATRIndicator,
    CumulativeDailyVolumeIndicator,
    CumulativeIndicator,
)
from fastscanner.services.indicators.lib.candle import CumulativeOperation as CumOp
from fastscanner.services.indicators.lib.daily import (
    ADRIndicator,
    ADVIndicator,
    DailyATRIndicator,
    PrevDayIndicator,
)
from fastscanner.services.indicators.lib.fundamental import MarketCapIndicator
from fastscanner.services.indicators.ports import CandleCol as C
from fastscanner.services.registry import ApplicationRegistry

from .utils import filter_by_market_cap


class SmallCapScanner:
    def __init__(
        self,
        start_time: time,
        end_time: time,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
        include_null_market_cap: bool = False,
    ) -> None:
        self._start_time = start_time
        self._end_time = end_time
        self._min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap
        self._include_null_market_cap = include_null_market_cap

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame: ...
