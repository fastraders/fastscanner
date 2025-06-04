import math
from datetime import date, time

import pandas as pd

from fastscanner.services.indicators.lib.candle import ATRIndicator
from fastscanner.services.indicators.lib.candle import CumulativeOperation as CumOp
from fastscanner.services.indicators.lib.candle import (
    DailyRollingIndicator,
    PremarketCumulativeIndicator,
)
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


class ATRGapDownScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        atr_multiplier: float,
        start_time: time,
        end_time: time,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
        include_null_market_cap: bool = False,
    ) -> None:
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self._start_time = start_time
        self._end_time = end_time
        self._min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap
        self._include_null_market_cap = include_null_market_cap
        self._adv_indicator = ADVIndicator(period=14)
        self._adr_indicator = ADRIndicator(period=14)
        self._atr_indicator = DailyATRIndicator(period=14)
        self._prev_close_indicator = PrevDayIndicator(candle_col=C.CLOSE)
        self._market_cap_indicator = MarketCapIndicator()
        self._atr_iday_indicator = ATRIndicator(period=140, freq="1d")
        self._cum_low_indicator = PremarketCumulativeIndicator(C.LOW, CumOp.MIN)

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:

        daily_df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            "1d",
            [
                self._adv_indicator,
                self._adr_indicator,
                self._atr_indicator,
                self._prev_close_indicator,
                self._market_cap_indicator,
            ],
        )
        if daily_df.empty:
            return daily_df

        daily_df = daily_df[
            daily_df[self._adv_indicator.column_name()] >= self._min_adv
        ]
        daily_df = daily_df[
            daily_df[self._adr_indicator.column_name()] >= self._min_adr
        ]
        daily_df = filter_by_market_cap(
            daily_df,
            self._min_market_cap,
            self._max_market_cap,
            self._include_null_market_cap,
        )
        if daily_df.empty:
            return daily_df
        self._atr_iday_indicator._freq = freq
        cum_low = PremarketCumulativeIndicator(C.LOW, CumOp.MIN)
        df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            freq,
            [self._atr_iday_indicator, cum_low],
        )
        df = df.loc[df.index.time <= self._end_time]  # type: ignore
        if df.empty:
            return df

        df.loc[:, "date"] = df.index.date  # type: ignore
        daily_df = daily_df.set_index(daily_df.index.date)[  # type: ignore
            [
                self._adv_indicator.column_name(),
                self._adr_indicator.column_name(),
                self._atr_indicator.column_name(),
                self._prev_close_indicator.column_name(),
            ]
        ]
        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])
        if df.empty:
            return df

        gap_col = cum_low.column_name()
        prev_close_col = self._prev_close_indicator.column_name()
        atr_col = self._atr_indicator.column_name()

        df["signal"] = (df[gap_col] - df[prev_close_col]) / df[atr_col]
        df = df[df["signal"] < self._atr_multiplier]
        return df

    async def scan_realtime(
        self, symbol: str, new_row: pd.Series, freq: str
    ) -> tuple[pd.Series, bool]:
        """
        Realtime scan implementation that enriches the new_row with indicators
        and returns whether it passes the filter criteria.
        """


        new_row = await self._adv_indicator.extend_realtime(symbol, new_row)
        new_row = await self._adr_indicator.extend_realtime(symbol, new_row)
        new_row = await self._atr_indicator.extend_realtime(symbol, new_row)
        new_row = await self._prev_close_indicator.extend_realtime(symbol, new_row)
        new_row = await self._market_cap_indicator.extend_realtime(symbol, new_row)

        adv_value = new_row[self._adv_indicator.column_name()]
        adr_value = new_row[self._adr_indicator.column_name()]
        market_cap_value = new_row[self._market_cap_indicator.column_name()]

        if pd.isna(adv_value) or adv_value < self._min_adv:
            return new_row, False
        if pd.isna(adr_value) or adr_value < self._min_adr:
            return new_row, False

        if (
            pd.isna(market_cap_value)
            or market_cap_value < self._min_market_cap
            or market_cap_value > self._max_market_cap
        ):
            return new_row, False

        self._atr_iday_indicator._freq = freq

        new_row = await self._atr_iday_indicator.extend_realtime(symbol, new_row)
        new_row = await self._cum_low_indicator.extend_realtime(symbol, new_row)

        gap_col = self._cum_low_indicator.column_name()
        prev_close_col = self._prev_close_indicator.column_name()
        atr_col = self._atr_indicator.column_name()

        gap_value = new_row[gap_col]
        prev_close_value = new_row[prev_close_col]
        atr_value = new_row[atr_col]

        if (
            pd.isna(gap_value)
            or pd.isna(prev_close_value)
            or pd.isna(atr_value)
            or atr_value == 0
        ):
            new_row["signal"] = pd.NA
            passes_filter = False
        else:
            signal = (gap_value - prev_close_value) / atr_value
            new_row["signal"] = signal
            passes_filter = signal < self._atr_multiplier

        return new_row, passes_filter


class ATRGapUpScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        atr_multiplier: float,
        start_time: time,
        end_time: time,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
        include_null_market_cap: bool = False,
    ) -> None:
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self._start_time = start_time
        self._end_time = end_time
        self._min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap
        self._include_null_market_cap = include_null_market_cap

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        adv = ADVIndicator(period=14)
        adr = ADRIndicator(period=14)
        atr = DailyATRIndicator(period=14)
        prev_close = PrevDayIndicator(candle_col=C.CLOSE)
        market_cap = MarketCapIndicator()

        daily_df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            "1d",
            [adv, adr, atr, prev_close, market_cap],
        )
        if daily_df.empty:
            return daily_df

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

        atr_iday = ATRIndicator(period=140, freq=freq)
        cum_high = PremarketCumulativeIndicator(C.HIGH, CumOp.MAX)
        df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            freq,
            [atr_iday, cum_high],
        )
        df = df.loc[df.index.time <= self._end_time]  # type: ignore
        if df.empty:
            return df

        df.loc[:, "date"] = df.index.date  # type: ignore
        daily_df = daily_df.set_index(daily_df.index.date)[  # type: ignore
            [
                adv.column_name(),
                adr.column_name(),
                atr.column_name(),
                prev_close.column_name(),
            ]
        ]

        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])
        if df.empty:
            return df

        gap_col = cum_high.column_name()
        prev_close_col = prev_close.column_name()
        atr_col = atr.column_name()
        df["signal"] = (df[gap_col] - df[prev_close_col]) / df[atr_col]
        df = df[df["signal"] > self._atr_multiplier]

        return df
