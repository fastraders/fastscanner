import math
import uuid
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
    PrevAllDayIndicator,
    PrevDayIndicator,
)
from fastscanner.services.indicators.lib.fundamental import MarketCapIndicator
from fastscanner.services.indicators.ports import CandleCol as C
from fastscanner.services.indicators.utils import lookback_days
from fastscanner.services.registry import ApplicationRegistry

from .utils import filter_by_market_cap


class Day2GapScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        min_gap: float,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
        include_null_market_cap: bool = False,
    ) -> None:
        self._id = str(uuid.uuid4())
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._min_gap = min_gap
        self._min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap
        self._include_null_market_cap = include_null_market_cap

    def id(self) -> str:
        return self._id

    @classmethod
    def type(cls) -> str:
        return "day2_gap"

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        if freq != "1d":
            raise ValueError("Day2GapScanner only supports '1d' frequency")

        adv = ADVIndicator(period=14)
        adr = ADRIndicator(period=14)
        market_cap = MarketCapIndicator()
        prev_all_day_close = PrevAllDayIndicator(candle_col=C.CLOSE)
        prev_all_day_high = PrevAllDayIndicator(candle_col=C.HIGH)
        prev_2day_close = PrevDayIndicator(candle_col=C.CLOSE, n_days_offset=2)

        daily_df = await ApplicationRegistry.candles.get(
            symbol,
            lookback_days(start, 1),
            end,
            "1d",
        )
        daily_df = daily_df.shift(1)
        daily_df = daily_df.loc[daily_df.index.date >= start]  # type: ignore
        if daily_df.empty:
            return daily_df

        daily_df = await adv.extend(symbol, daily_df)
        daily_df = await adr.extend(symbol, daily_df)
        daily_df = await prev_all_day_close.extend(symbol, daily_df)
        daily_df = await prev_all_day_high.extend(symbol, daily_df)
        daily_df = await prev_2day_close.extend(symbol, daily_df)
        daily_df = await market_cap.extend(symbol, daily_df)

        daily_df = daily_df[daily_df[adv.column_name()] >= self._min_adv]
        daily_df = daily_df[daily_df[adr.column_name()] >= self._min_adr]
        daily_df = filter_by_market_cap(
            daily_df,
            self._min_market_cap,
            self._max_market_cap,
            self._include_null_market_cap,
        )
        if daily_df.empty:
            return daily_df

        daily_df["retrace"] = (
            daily_df[prev_all_day_high.column_name()]
            - daily_df[prev_all_day_close.column_name()]
        ) / daily_df[prev_all_day_close.column_name()]

        signal_col = "gain_all_sessions"
        daily_df[signal_col] = (
            daily_df[prev_all_day_high.column_name()]
            - daily_df[prev_2day_close.column_name()]
        ) / daily_df[prev_2day_close.column_name()]

        return daily_df[daily_df[signal_col] > self._min_gap]

    async def scan_realtime(
        self, symbol: str, new_row: pd.Series, freq: str
    ) -> tuple[pd.Series, bool]: ...
