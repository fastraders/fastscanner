import math
from datetime import date, time

import pandas as pd

from fastscanner.services.indicators.lib.candle import ATRIndicator
from fastscanner.services.indicators.lib.candle import CumulativeOperation as CumOp
from fastscanner.services.indicators.lib.candle import PremarketCumulativeIndicator
from fastscanner.services.indicators.lib.daily import (
    ADRIndicator,
    ADVIndicator,
    DailyATRIndicator,
    PrevDayIndicator,
)
from fastscanner.services.indicators.lib.fundamental import MarketCapIndicator
from fastscanner.services.indicators.ports import CandleCol as C
from fastscanner.services.registry import ApplicationRegistry


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
    ) -> None:
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self._start_time = start_time
        self._end_time = end_time
        self.min_market_cap = (min_market_cap,)
        self._max_market_cap = max_market_cap

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
        daily_df = daily_df[daily_df[market_cap.column_name()] >= self.min_market_cap]
        daily_df = daily_df[daily_df[market_cap.column_name()] <= self._max_market_cap]
        if daily_df.empty:
            return daily_df

        atr_iday = ATRIndicator(period=140, freq="1d")
        cum_low = PremarketCumulativeIndicator(C.LOW, CumOp.MIN)
        df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            freq,
            [atr_iday, cum_low],
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

        gap_col = cum_low.column_name()
        prev_close_col = prev_close.column_name()
        atr_col = atr.column_name()

        df["signal"] = (df[gap_col] - df[prev_close_col]) / df[atr_col]
        df = df[df["signal"] < self._atr_multiplier]
        return df


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
    ) -> None:
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self._start_time = start_time
        self._end_time = end_time
        self.min_market_cap = (min_market_cap,)
        self._max_market_cap = max_market_cap

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
        daily_df = daily_df[daily_df[market_cap.column_name()] >= self.min_market_cap]
        daily_df = daily_df[daily_df[market_cap.column_name()] <= self._max_market_cap]
        if daily_df.empty:
            return daily_df

        atr_iday = ATRIndicator(period=140, freq="1d")
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
