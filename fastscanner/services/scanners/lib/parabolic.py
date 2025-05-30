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


class ATRParabolicDownScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        atr_multiplier: float,
        min_volume: float,
        end_time: time,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
    ) -> None:
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self._min_volume = min_volume
        self._end_time = end_time
        self.min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        adv = ADVIndicator(period=14)
        adr = ADRIndicator(period=14)
        atr = DailyATRIndicator(period=14)
        market_cap = MarketCapIndicator()

        daily_df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            "1d",
            [adv, adr, atr, market_cap],
        )
        if daily_df.empty:
            return daily_df

        # High level filtering
        daily_df = daily_df[daily_df[adv.column_name()] >= self._min_adv]
        daily_df = daily_df[daily_df[adr.column_name()] >= self._min_adr]
        daily_df = daily_df[daily_df[market_cap.column_name()] >= self.min_market_cap]
        daily_df = daily_df[daily_df[market_cap.column_name()] <= self._max_market_cap]
        if daily_df.empty:
            return daily_df

        atr_iday = ATRIndicator(period=140, freq="1d")
        cum_low = CumulativeIndicator(C.LOW, CumOp.MIN)
        cum_volume = CumulativeDailyVolumeIndicator()
        df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            freq,
            [atr_iday, cum_low, cum_volume],
        )
        df = df.loc[df.index.time <= self._end_time]  # type: ignore
        if df.empty:
            return df

        df.loc[:, "date"] = df.index.date  # type: ignore
        daily_df = daily_df.set_index(daily_df.index.date)[[adv.column_name(), adr.column_name(), atr.column_name()]]  # type: ignore
        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])

        # Logic: parabolic down condition
        df["signal"] = (df[C.OPEN] - df[C.CLOSE]) / df[atr.column_name()]
        df = df[df["signal"] > self._atr_multiplier]
        df = df[df[cum_volume.column_name()] >= self._min_volume]
        df = df[(df[cum_low.column_name()] - df[C.LOW]).abs() < 0.0001]

        return df


class ATRParabolicUpScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        atr_multiplier: float,
        min_volume: float,
        end_time: time,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
    ) -> None:
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self._min_volume = min_volume
        self._end_time = (end_time,)
        self.min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        adv = ADVIndicator(period=14)
        adr = ADRIndicator(period=14)
        atr = DailyATRIndicator(period=14)
        market_cap = MarketCapIndicator()

        daily_df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            "1d",
            [adv, adr, atr, market_cap],
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
        cum_high = CumulativeIndicator(C.HIGH, CumOp.MAX)
        cum_volume = CumulativeDailyVolumeIndicator()
        df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            freq,
            [atr_iday, cum_high, cum_volume],
        )
        df = df.loc[df.index.time <= self._end_time]  # type: ignore
        if df.empty:
            return df

        df.loc[:, "date"] = df.index.date  # type: ignore
        daily_df = daily_df.set_index(daily_df.index.date)[  # type: ignore
            [adv.column_name(), adr.column_name(), atr.column_name()]
        ]
        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])

        # Logic: parabolic up condition
        df["signal"] = (df[C.CLOSE] - df[C.OPEN]) / df[atr.column_name()]
        df = df[df["signal"] > self._atr_multiplier]
        df = df[df[cum_volume.column_name()] >= self._min_volume]
        df = df[(df[cum_high.column_name()] - df[C.HIGH]).abs() < 0.0001]

        return df


class DailyATRParabolicUpScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        atr_multiplier: float,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
    ) -> None:
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self.min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        adv = ADVIndicator(period=14)
        adr = ADRIndicator(period=14)
        daily_atr = DailyATRIndicator(period=14)
        prev_close = PrevDayIndicator(C.CLOSE)
        prev_open = PrevDayIndicator(C.OPEN)
        market_cap = MarketCapIndicator()

        daily_df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            "1d",
            [adv, adr, daily_atr, prev_close, prev_open, market_cap],
        )
        if daily_df.empty:
            return daily_df

        daily_df = daily_df[daily_df[adv.column_name()] >= self._min_adv]
        daily_df = daily_df[daily_df[adr.column_name()] >= self._min_adr]
        daily_df = daily_df[daily_df[market_cap.column_name()] >= self.min_market_cap]
        daily_df = daily_df[daily_df[market_cap.column_name()] <= self._max_market_cap]
        if daily_df.empty:
            return daily_df

        daily_df["signal"] = (
            daily_df[prev_close.column_name()] - daily_df[prev_open.column_name()]
        ) / daily_df[daily_atr.column_name()]

        return daily_df[daily_df["signal"] > self._atr_multiplier]


class DailyATRParabolicDownScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        atr_multiplier: float,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
    ) -> None:
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self.min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        adv = ADVIndicator(period=14)
        adr = ADRIndicator(period=14)
        daily_atr = DailyATRIndicator(period=14)
        prev_close = PrevDayIndicator(C.CLOSE)
        prev_open = PrevDayIndicator(C.OPEN)
        market_cap = MarketCapIndicator()

        daily_df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            "1d",
            [adv, adr, daily_atr, prev_close, prev_open, market_cap],
        )
        if daily_df.empty:
            return daily_df

        daily_df = daily_df[daily_df[adv.column_name()] >= self._min_adv]
        daily_df = daily_df[daily_df[adr.column_name()] >= self._min_adr]
        daily_df = daily_df[daily_df[market_cap.column_name()] >= self.min_market_cap]
        daily_df = daily_df[daily_df[market_cap.column_name()] <= self._max_market_cap]
        if daily_df.empty:
            return daily_df

        daily_df["signal"] = (
            daily_df[prev_open.column_name()] - daily_df[prev_close.column_name()]
        ) / daily_df[daily_atr.column_name()]

        return daily_df[daily_df["signal"] > self._atr_multiplier]
