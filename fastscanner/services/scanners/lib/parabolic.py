from datetime import date, time

import pandas as pd

from fastscanner.services.indicators.lib.candle import (
    CumulativeDailyVolumeIndicator,
    CumulativeIndicator,
)
from fastscanner.services.indicators.lib.candle import CumulativeOperation as CumOp
from fastscanner.services.indicators.lib.daily import ADRIndicator, ADVIndicator
from fastscanner.services.indicators.ports import CandleCol as C
from fastscanner.services.registry import ApplicationRegistry


class ATRParabolicDownScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        min_high_low_ratio: float,
        min_volume: float,
    ) -> None:
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._min_high_low_ratio = min_high_low_ratio
        self._min_volume = min_volume

    # async def scan(
    #     self, symbol: str, start: date, end: date, freq: str
    # ) -> pd.DataFrame:
    #     adv = ADVIndicator(period=14)
    #     adr = ADRIndicator(period=14)

    #     import time as t

    #     start_time = t.time()
    #     df = await ApplicationRegistry.candles.get(symbol, start, end, freq)

    #     df = df.loc[df.index.time <= time(9, 30)]  # type: ignore
    #     if df.empty:
    #         return df

    #     df = await adv.extend(symbol, df)
    #     df = await adr.extend(symbol, df)
    #     df = df[df[adv.column_name()] >= self._min_adv]
    #     df = df[df[adr.column_name()] >= self._min_adr]

    #     if df.empty:
    #         return df

    #     df = df[(df[C.HIGH] - df[C.LOW]) / df[C.LOW] > self._min_high_low_ratio]
    #     df = df[df[C.CLOSE] < df[C.OPEN]]
    #     if df.empty:
    #         return df

    #     cum_low = PremarketCumulativeIndicator(C.LOW, CumOp.MIN)
    #     df = await cum_low.extend(symbol, df)
    #     # Test that they're virtually equal
    #     df = df[(df[cum_low.column_name()] - df[C.LOW]).abs() < 0.0001]
    #     return df

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        adv = ADVIndicator(period=14)
        adr = ADRIndicator(period=14)

        daily_df = await ApplicationRegistry.candles.get(symbol, start, end, "1d")
        if daily_df.empty:
            return daily_df

        daily_df = await adv.extend(symbol, daily_df)
        daily_df = await adr.extend(symbol, daily_df)

        daily_df = daily_df[daily_df[adv.column_name()] >= self._min_adv]
        daily_df = daily_df[daily_df[adr.column_name()] >= self._min_adr]
        if daily_df.empty:
            return daily_df

        df = await ApplicationRegistry.candles.get(symbol, start, end, freq)
        df = df.loc[df.index.time <= time(9, 30)]  # type: ignore
        if df.empty:
            return df

        df["date"] = df.index.date
        daily_df = daily_df.set_index(daily_df.index.date)[[adv.column_name(), adr.column_name()]]  # type: ignore
        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])

        if df.empty:
            return df

        cum_low = CumulativeIndicator(C.LOW, CumOp.MIN)
        df = await cum_low.extend(symbol, df)
        cum_volume = CumulativeDailyVolumeIndicator()
        df = await cum_volume.extend(symbol, df)

        df = df[(df[C.HIGH] - df[C.LOW]) / df[C.LOW] > self._min_high_low_ratio]
        df = df[df[C.CLOSE] < df[C.OPEN]]
        df = df[df[cum_volume.column_name()] >= self._min_volume]
        # lowest low and low are virtually equal
        df = df[(df[cum_low.column_name()] - df[C.LOW]).abs() < 0.0001]

        return df
