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
from fastscanner.services.indicators.ports import CandleCol as C
from fastscanner.services.indicators.utils import lookback_days
from fastscanner.services.registry import ApplicationRegistry


class ATRGapDownScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        atr_multiplier: float,
        start_time: time,
        end_time: time,
    ) -> None:
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self._start_time = start_time
        self._end_time = end_time

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        adv = ADVIndicator(period=14)
        adr = ADRIndicator(period=14)
        atr = DailyATRIndicator(period=14)
        prev_close = PrevDayIndicator(candle_col=C.CLOSE)

        daily_df = await ApplicationRegistry.candles.get(symbol, start, end, "1d")
        if daily_df.empty:
            return daily_df

        daily_df = await adv.extend(symbol, daily_df)
        daily_df = await adr.extend(symbol, daily_df)
        daily_df = await atr.extend(symbol, daily_df)
        daily_df = await prev_close.extend(symbol, daily_df)

        daily_df = daily_df[daily_df[adv.column_name()] >= self._min_adv]
        daily_df = daily_df[daily_df[adr.column_name()] >= self._min_adr]
        if daily_df.empty:
            return daily_df

        atr_iday = ATRIndicator(period=140, freq=freq)
        start_atr = lookback_days(start, atr_iday.lookback_days())
        df = await ApplicationRegistry.candles.get(symbol, start_atr, end, freq)
        df = await atr_iday.extend(symbol, df)
        df.loc[:, "date"] = df.index.date  # type: ignore
        df = df[df.loc[:, "date"] >= start]

        df = df.loc[df.index.time <= self._end_time]  # type: ignore
        df = df.loc[df.index.time >= self._start_time]  # type: ignore
        if df.empty:
            return df

        daily_df = daily_df.set_index(daily_df.index.date)[  # type: ignore
            [
                adv.column_name(),
                adr.column_name(),
                atr.column_name(),
                prev_close.column_name(),
            ]
        ].set_index(
            daily_df.index.date  # type: ignore
        )

        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])
        if df.empty:
            return df

        cum_low = PremarketCumulativeIndicator(C.LOW, CumOp.MIN)
        df = await cum_low.extend(symbol, df)

        gap_col = cum_low.column_name()
        prev_close_col = prev_close.column_name()
        atr_col = atr.column_name()

        df = df[
            (df[gap_col] - df[prev_close_col]) < (df[atr_col] * self._atr_multiplier)
        ]
        return df


class ATRGapUpScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        atr_multiplier: float,
        start_time: time,
        end_time: time,
    ) -> None:
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self._start_time = start_time
        self._end_time = end_time

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        adv = ADVIndicator(period=14)
        adr = ADRIndicator(period=14)
        atr = DailyATRIndicator(period=14)
        prev_close = PrevDayIndicator(candle_col=C.CLOSE)

        daily_df = await ApplicationRegistry.candles.get(symbol, start, end, "1d")
        if daily_df.empty:
            return daily_df

        daily_df = await adv.extend(symbol, daily_df)
        daily_df = await adr.extend(symbol, daily_df)
        daily_df = await atr.extend(symbol, daily_df)
        daily_df = await prev_close.extend(symbol, daily_df)

        daily_df = daily_df[daily_df[adv.column_name()] >= self._min_adv]
        daily_df = daily_df[daily_df[adr.column_name()] >= self._min_adr]
        if daily_df.empty:
            return daily_df

        atr_iday = ATRIndicator(period=140, freq=freq)
        start_atr = lookback_days(start, atr_iday.lookback_days())
        df = await ApplicationRegistry.candles.get(symbol, start_atr, end, freq)
        df = await atr_iday.extend(symbol, df)
        df.loc[:, "date"] = df.index.date  # type: ignore
        df = df[df.loc[:, "date"] >= start]

        df = df.loc[df.index.time <= self._end_time]  # type: ignore
        df = df.loc[df.index.time >= self._start_time]  # type: ignore
        if df.empty:
            return df

        daily_df = daily_df.set_index(daily_df.index.date)[  # type: ignore
            [
                adv.column_name(),
                adr.column_name(),
                atr.column_name(),
                prev_close.column_name(),
            ]
        ].set_index(
            daily_df.index.date  # type: ignore
        )

        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])
        if df.empty:
            return df

        cum_high = PremarketCumulativeIndicator(C.HIGH, CumOp.MAX)
        df = await cum_high.extend(symbol, df)

        gap_col = cum_high.column_name()
        prev_close_col = prev_close.column_name()
        atr_col = atr.column_name()

        df = df[
            (df[gap_col] - df[prev_close_col]) > (df[atr_col] * self._atr_multiplier)
        ]
        return df
