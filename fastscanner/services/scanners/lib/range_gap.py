import math
from datetime import date, time

import pandas as pd

from fastscanner.services.indicators.lib.candle import ATRGapIndicator
from fastscanner.services.indicators.lib.candle import CumulativeOperation as CumOp
from fastscanner.services.indicators.lib.candle import (
    DailyRollingIndicator,
    GapIndicator,
    PremarketCumulativeIndicator,
)
from fastscanner.services.indicators.lib.daily import ADRIndicator, ADVIndicator
from fastscanner.services.indicators.lib.fundamental import MarketCapIndicator
from fastscanner.services.indicators.ports import CandleCol as C
from fastscanner.services.registry import ApplicationRegistry

from .utils import filter_by_market_cap


class HighRangeGapUpScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        start_time: time,
        end_time: time,
        min_volume: float,
        n_days: int,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
        include_null_market_cap: bool = False,
    ) -> None:
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._start_time = start_time
        self._end_time = end_time
        self._min_volume = min_volume
        self._n_days = n_days
        self._min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap
        self._include_null_market_cap = include_null_market_cap

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        adv = ADVIndicator(period=14)
        adr = ADRIndicator(period=14)
        market_cap = MarketCapIndicator()
        highest_high = DailyRollingIndicator(
            n_days=self._n_days, operation="max", candle_col=C.HIGH
        )

        daily_df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            "1d",
            [adv, adr, market_cap, highest_high],
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

        cum_volume = PremarketCumulativeIndicator(C.VOLUME, CumOp.SUM)
        gap = GapIndicator(C.HIGH)
        atr_gap = ATRGapIndicator(
            period=14,
            candle_col=C.HIGH,
        )

        df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            freq,
            [cum_volume, gap, atr_gap],
        )

        if df.empty:
            return df

        df = df.loc[df.index.time >= self._start_time]  # type: ignore
        df = df.loc[df.index.time <= self._end_time]  # type: ignore

        if df.empty:
            return df

        df.loc[:, "date"] = df.index.date  # type: ignore
        daily_df = daily_df.set_index(daily_df.index.date)[  # type: ignore
            [
                adv.column_name(),
                adr.column_name(),
                highest_high.column_name(),
            ]
        ]
        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])

        if df.empty:
            return df

        cum_volume_col = cum_volume.column_name()
        highest_high_col = highest_high.column_name()

        df = df[df[cum_volume_col] >= self._min_volume]
        df = df[df[C.HIGH] > df[highest_high_col]]

        return df

    async def scan_realtime(
        self, symbol: str, new_row: pd.Series, freq: str
    ) -> tuple[pd.Series, bool]: ...


class LowRangeGapDownScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        start_time: time,
        end_time: time,
        min_volume: float,
        n_days: int,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
        include_null_market_cap: bool = False,
    ) -> None:
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._start_time = start_time
        self._end_time = end_time
        self._min_volume = min_volume
        self._n_days = n_days
        self._min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap
        self._include_null_market_cap = include_null_market_cap

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        adv = ADVIndicator(period=14)
        adr = ADRIndicator(period=14)
        market_cap = MarketCapIndicator()
        lowest_low = DailyRollingIndicator(
            n_days=self._n_days, operation="min", candle_col=C.LOW
        )

        daily_df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            "1d",
            [adv, adr, market_cap, lowest_low],
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

        cum_volume = PremarketCumulativeIndicator(C.VOLUME, CumOp.SUM)
        gap = GapIndicator(C.LOW)
        atr_gap = ATRGapIndicator(
            period=14,
            candle_col=C.LOW,
        )

        df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            freq,
            [cum_volume, gap, atr_gap],
        )

        if df.empty:
            return df

        df = df.loc[df.index.time >= self._start_time]  # type: ignore
        df = df.loc[df.index.time <= self._end_time]  # type: ignore

        if df.empty:
            return df

        df.loc[:, "date"] = df.index.date  # type: ignore
        daily_df = daily_df.set_index(daily_df.index.date)[  # type: ignore
            [
                adv.column_name(),
                adr.column_name(),
                lowest_low.column_name(),
            ]
        ]
        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])

        if df.empty:
            return df

        cum_volume_col = cum_volume.column_name()
        lowest_low_col = lowest_low.column_name()

        df = df[df[cum_volume_col] >= self._min_volume]
        df = df[df[C.LOW] < df[lowest_low_col]]

        return df

    async def scan_realtime(
        self, symbol: str, new_row: pd.Series, freq: str
    ) -> tuple[pd.Series, bool]: ...
