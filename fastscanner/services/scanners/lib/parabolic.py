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
)
from fastscanner.services.indicators.ports import CandleCol as C
from fastscanner.services.indicators.utils import get_df_with_atr, lookback_days
from fastscanner.services.registry import ApplicationRegistry


class ATRParabolicDownScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        atr_multiplier: float,
        min_volume: float,
        end_time: time,
    ) -> None:
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self._min_volume = min_volume
        self._end_time = end_time

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        adv = ADVIndicator(period=14)
        adr = ADRIndicator(period=14)
        atr = DailyATRIndicator(period=14)

        daily_df = await ApplicationRegistry.candles.get(symbol, start, end, "1d")
        if daily_df.empty:
            return daily_df

        daily_df = await atr.extend(symbol, daily_df)
        daily_df = await adv.extend(symbol, daily_df)
        daily_df = await adr.extend(symbol, daily_df)
        daily_df = daily_df[daily_df[adv.column_name()] >= self._min_adv]
        daily_df = daily_df[daily_df[adr.column_name()] >= self._min_adr]
        if daily_df.empty:
            return daily_df

        df = await get_df_with_atr(symbol, start, end, freq)
        df.loc[:, "date"] = df.index.date  # type: ignore
        df = df[df.loc[:, "date"] >= start]

        df = df.loc[df.index.time <= self._end_time]  # type: ignore
        if df.empty:
            return df

        daily_df = daily_df.set_index(daily_df.index.date)[[adv.column_name(), adr.column_name(), atr.column_name()]]  # type: ignore
        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])

        cum_low = CumulativeIndicator(C.LOW, CumOp.MIN)
        cum_volume = CumulativeDailyVolumeIndicator()
        df = await cum_low.extend(symbol, df)
        df = await cum_volume.extend(symbol, df)

        df = df[df[C.OPEN] - df[C.CLOSE] > self._atr_multiplier * df[atr.column_name()]]
        df = df[df[cum_volume.column_name()] >= self._min_volume]
        # lowest low and low are virtually equal
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
    ) -> None:
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self._min_volume = min_volume
        self._end_time = end_time

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        adv = ADVIndicator(period=14)
        adr = ADRIndicator(period=14)
        atr = DailyATRIndicator(period=14)

        daily_df = await ApplicationRegistry.candles.get(symbol, start, end, "1d")
        if daily_df.empty:
            return daily_df

        daily_df = await atr.extend(symbol, daily_df)
        daily_df = await adv.extend(symbol, daily_df)
        daily_df = await adr.extend(symbol, daily_df)

        daily_df = daily_df[daily_df[adv.column_name()] >= self._min_adv]
        daily_df = daily_df[daily_df[adr.column_name()] >= self._min_adr]
        if daily_df.empty:
            return daily_df

        df = await get_df_with_atr(symbol, start, end, freq)
        df.loc[:, "date"] = df.index.date  # type: ignore
        df = df[df.loc[:, "date"] >= start]

        df = df.loc[df.index.time <= self._end_time]  # type: ignore
        if df.empty:
            return df

        daily_df = daily_df.set_index(daily_df.index.date)[  # type: ignore
            [adv.column_name(), adr.column_name(), atr.column_name()]  
        ]
        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])

        if df.empty:
            return df

        cum_high = CumulativeIndicator(C.HIGH, CumOp.MAX)
        df = await cum_high.extend(symbol, df)

        cum_volume = CumulativeDailyVolumeIndicator()
        df = await cum_volume.extend(symbol, df)

        # Logic: parabolic up condition
        atr_col = ATRIndicator(period=140, freq=freq).column_name()
        df = df[(df[C.CLOSE] - df[C.OPEN]) > df[atr_col] * self._atr_multiplier]
        df = df[df[cum_volume.column_name()] >= self._min_volume]
        df = df[(df[cum_high.column_name()] - df[C.HIGH]).abs() < 0.0001]

        return df
