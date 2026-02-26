import math
import uuid
from datetime import date, time

import pandas as pd

from fastscanner.pkg.candle import Candle
from fastscanner.services.indicators.lib.candle import (
    ATRGapIndicator,
    ATRIndicator,
    CumulativeDailyVolumeIndicator,
    GapIndicator,
)
from fastscanner.services.indicators.lib.daily import (
    ADRIndicator,
    ADVIndicator,
    DailyATRIndicator,
    PrevDayIndicator,
)
from fastscanner.services.indicators.lib.fundamental import (
    DaysFromEarningsIndicator,
    MarketCapIndicator,
)
from fastscanner.services.indicators.ports import CandleCol as C
from fastscanner.services.registry import ApplicationRegistry

from .utils import filter_by_market_cap


class ATRGapDownScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        atr_multiplier: float,
        min_volume: float,
        start_time: time,
        end_time: time,
        max_adr: float | None = None,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
        min_days_from_earnings: int | None = None,
        max_days_from_earnings: int | None = None,
        days_of_week: list[int] | None = None,
        include_null_market_cap: bool = False,
    ) -> None:
        self._id = str(uuid.uuid4())
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._max_adr = max_adr
        self._atr_multiplier = atr_multiplier
        self._min_volume = min_volume
        self._start_time = start_time
        self._end_time = end_time
        self._min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap
        self._min_days_from_earnings = min_days_from_earnings
        self._max_days_from_earnings = max_days_from_earnings
        self._days_of_week = days_of_week
        self._include_null_market_cap = include_null_market_cap
        self._adv = ADVIndicator(period=14)
        self._adr = ADRIndicator(period=14)
        self._market_cap = MarketCapIndicator()
        self._days_from_earnings = DaysFromEarningsIndicator()
        self._cum_volume = CumulativeDailyVolumeIndicator()
        self._gap = GapIndicator(C.LOW)
        self._atr_gap = ATRGapIndicator(period=14, candle_col=C.LOW)

    def id(self) -> str:
        return self._id

    @classmethod
    def type(cls) -> str:
        return "atr_gap_down"

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        daily_df = await ApplicationRegistry.candles.get(
            symbol,
            start,
            end,
            "1d",
        )
        if daily_df.empty:
            return daily_df

        daily_df = await self._adv.extend(symbol, daily_df)
        daily_df = await self._adr.extend(symbol, daily_df)
        daily_df = await self._market_cap.extend(symbol, daily_df)
        daily_df = await self._days_from_earnings.extend(symbol, daily_df)

        if self._min_days_from_earnings is not None:
            daily_df = daily_df.loc[
                daily_df[self._days_from_earnings.column_name()]
                >= self._min_days_from_earnings
            ]
        if self._max_days_from_earnings is not None:
            daily_df = daily_df.loc[
                daily_df[self._days_from_earnings.column_name()]
                <= self._max_days_from_earnings
            ]
        if self._days_of_week is not None:
            daily_df = daily_df.loc[daily_df.index.dayofweek.isin(self._days_of_week)]  # type: ignore

        daily_df = daily_df[daily_df[self._adv.column_name()] >= self._min_adv]
        daily_df = daily_df[daily_df[self._adr.column_name()] >= self._min_adr]
        if self._max_adr is not None:
            daily_df = daily_df[daily_df[self._adr.column_name()] <= self._max_adr]
        daily_df = filter_by_market_cap(
            daily_df,
            self._min_market_cap,
            self._max_market_cap,
            self._include_null_market_cap,
        )
        if daily_df.empty:
            return daily_df

        df = await ApplicationRegistry.candles.get(
            symbol,
            start,
            end,
            freq,
        )
        df = await self._cum_volume.extend(symbol, df)
        df = df.loc[(df.index.time >= self._start_time) & (df.index.time <= self._end_time)]  # type: ignore
        if df.empty:
            return df

        df.loc[:, "date"] = df.index.date  # type: ignore
        daily_df = daily_df.set_index(daily_df.index.date)[  # type: ignore
            [
                self._adv.column_name(),
                self._adr.column_name(),
                self._market_cap.column_name(),
            ]
        ]
        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])
        if df.empty:
            return df

        df = await self._gap.extend(symbol, df)
        df = await self._atr_gap.extend(symbol, df)

        atr_gap_col = self._atr_gap.column_name()
        df = df[df[self._cum_volume.column_name()] >= self._min_volume]
        df = df[df[atr_gap_col].abs() > self._atr_multiplier]
        df = df[df[atr_gap_col] < 0]
        df = df[df[C.CLOSE] < df[C.OPEN]]
        return df

    async def scan_realtime(
        self, symbol: str, new_row: Candle
    ) -> tuple[Candle, bool]:
        if (
            new_row.timestamp.time() > self._end_time
            or new_row.timestamp.time() < self._start_time
        ):
            new_row["signal"] = pd.NA
            return new_row, False
        if (
            self._days_of_week is not None
            and new_row.timestamp.dayofweek not in self._days_of_week
        ):
            new_row["signal"] = pd.NA
            return new_row, False

        new_row = await self._adv.extend_realtime(symbol, new_row)
        new_row = await self._adr.extend_realtime(symbol, new_row)
        new_row = await self._gap.extend_realtime(symbol, new_row)
        new_row = await self._atr_gap.extend_realtime(symbol, new_row)
        new_row = await self._market_cap.extend_realtime(symbol, new_row)
        new_row = await self._cum_volume.extend_realtime(symbol, new_row)
        new_row = await self._days_from_earnings.extend_realtime(symbol, new_row)

        adv_value = new_row[self._adv.column_name()]
        adr_value = new_row[self._adr.column_name()]
        market_cap_value = new_row[self._market_cap.column_name()]
        cum_volume_value = new_row[self._cum_volume.column_name()]
        days_from_earnings_value = new_row[self._days_from_earnings.column_name()]

        gap_value = new_row[self._gap.column_name()]
        atr_gap_value = new_row[self._atr_gap.column_name()]

        mandatory_values = [
            adv_value,
            adr_value,
            gap_value,
            atr_gap_value,
            cum_volume_value,
        ]
        if (
            self._min_days_from_earnings is not None
            or self._max_days_from_earnings is not None
        ):
            mandatory_values.append(days_from_earnings_value)
        if any(pd.isna(v) for v in mandatory_values):
            new_row["signal"] = pd.NA
            return new_row, False

        market_cap_passes = (
            not pd.isna(market_cap_value)
            and self._min_market_cap <= market_cap_value <= self._max_market_cap
        ) or (pd.isna(market_cap_value) and self._include_null_market_cap)
        days_from_earnings_passes = (
            self._min_days_from_earnings is None
            or days_from_earnings_value >= self._min_days_from_earnings
        ) and (
            self._max_days_from_earnings is None
            or days_from_earnings_value <= self._max_days_from_earnings
        )

        passes_filter = (
            adv_value >= self._min_adv
            and adr_value >= self._min_adr
            and (self._max_adr is None or adr_value <= self._max_adr)
            and market_cap_passes
            and cum_volume_value >= self._min_volume
            and abs(atr_gap_value) > self._atr_multiplier
            and atr_gap_value < 0
            and new_row[C.CLOSE] < new_row[C.OPEN]
            and days_from_earnings_passes
        )

        return new_row, passes_filter


class ATRGapUpScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        atr_multiplier: float,
        min_volume: float,
        start_time: time,
        end_time: time,
        max_adr: float | None = None,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
        min_days_from_earnings: int | None = None,
        max_days_from_earnings: int | None = None,
        days_of_week: list[int] | None = None,
        include_null_market_cap: bool = False,
    ) -> None:
        self._id = str(uuid.uuid4())
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._max_adr = max_adr
        self._atr_multiplier = atr_multiplier
        self._min_volume = min_volume
        self._start_time = start_time
        self._end_time = end_time
        self._min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap
        self._min_days_from_earnings = min_days_from_earnings
        self._max_days_from_earnings = max_days_from_earnings
        self._days_of_week = days_of_week
        self._include_null_market_cap = include_null_market_cap
        self._adv = ADVIndicator(period=14)
        self._adr = ADRIndicator(period=14)
        self._market_cap = MarketCapIndicator()
        self._days_from_earnings = DaysFromEarningsIndicator()
        self._cum_volume = CumulativeDailyVolumeIndicator()
        self._gap = GapIndicator(C.HIGH)
        self._atr_gap = ATRGapIndicator(period=14, candle_col=C.HIGH)

    def id(self) -> str:
        return self._id

    @classmethod
    def type(cls) -> str:
        return "atr_gap_up"

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        daily_df = await ApplicationRegistry.candles.get(
            symbol,
            start,
            end,
            "1d",
        )
        if daily_df.empty:
            return daily_df

        daily_df = await self._adv.extend(symbol, daily_df)
        daily_df = await self._adr.extend(symbol, daily_df)
        daily_df = await self._market_cap.extend(symbol, daily_df)
        daily_df = await self._days_from_earnings.extend(symbol, daily_df)

        if self._min_days_from_earnings is not None:
            daily_df = daily_df.loc[
                daily_df[self._days_from_earnings.column_name()]
                >= self._min_days_from_earnings
            ]
        if self._max_days_from_earnings is not None:
            daily_df = daily_df.loc[
                daily_df[self._days_from_earnings.column_name()]
                <= self._max_days_from_earnings
            ]
        if self._days_of_week is not None:
            daily_df = daily_df.loc[daily_df.index.dayofweek.isin(self._days_of_week)]  # type: ignore

        daily_df = daily_df[daily_df[self._adv.column_name()] >= self._min_adv]
        daily_df = daily_df[daily_df[self._adr.column_name()] >= self._min_adr]
        if self._max_adr is not None:
            daily_df = daily_df[daily_df[self._adr.column_name()] <= self._max_adr]
        daily_df = filter_by_market_cap(
            daily_df,
            self._min_market_cap,
            self._max_market_cap,
            self._include_null_market_cap,
        )
        if daily_df.empty:
            return daily_df

        df = await ApplicationRegistry.candles.get(
            symbol,
            start,
            end,
            freq,
        )
        df = await self._cum_volume.extend(symbol, df)
        df = df.loc[(df.index.time >= self._start_time) & (df.index.time <= self._end_time)]  # type: ignore
        if df.empty:
            return df

        df.loc[:, "date"] = df.index.date  # type: ignore
        daily_df = daily_df.set_index(daily_df.index.date)[  # type: ignore
            [
                self._adv.column_name(),
                self._adr.column_name(),
                self._market_cap.column_name(),
            ]
        ]
        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])
        if df.empty:
            return df

        df = await self._gap.extend(symbol, df)
        df = await self._atr_gap.extend(symbol, df)

        atr_gap_col = self._atr_gap.column_name()
        df = df[df[self._cum_volume.column_name()] >= self._min_volume]
        df = df[df[atr_gap_col].abs() > self._atr_multiplier]
        df = df[df[atr_gap_col] > 0]
        df = df[df[C.CLOSE] > df[C.OPEN]]

        return df

    async def scan_realtime(
        self, symbol: str, new_row: Candle
    ) -> tuple[Candle, bool]:
        if (
            new_row.timestamp.time() > self._end_time
            or new_row.timestamp.time() < self._start_time
        ):
            new_row["signal"] = pd.NA
            return new_row, False
        if (
            self._days_of_week is not None
            and new_row.timestamp.dayofweek not in self._days_of_week
        ):
            new_row["signal"] = pd.NA
            return new_row, False

        new_row = await self._adv.extend_realtime(symbol, new_row)
        new_row = await self._adr.extend_realtime(symbol, new_row)
        new_row = await self._gap.extend_realtime(symbol, new_row)
        new_row = await self._atr_gap.extend_realtime(symbol, new_row)
        new_row = await self._market_cap.extend_realtime(symbol, new_row)
        new_row = await self._cum_volume.extend_realtime(symbol, new_row)
        new_row = await self._days_from_earnings.extend_realtime(symbol, new_row)

        adv_value = new_row[self._adv.column_name()]
        adr_value = new_row[self._adr.column_name()]
        market_cap_value = new_row[self._market_cap.column_name()]
        cum_volume_value = new_row[self._cum_volume.column_name()]
        days_from_earnings_value = new_row[self._days_from_earnings.column_name()]

        gap_value = new_row[self._gap.column_name()]
        atr_gap_value = new_row[self._atr_gap.column_name()]

        mandatory_values = [
            adv_value,
            adr_value,
            gap_value,
            atr_gap_value,
            cum_volume_value,
        ]
        if (
            self._min_days_from_earnings is not None
            or self._max_days_from_earnings is not None
        ):
            mandatory_values.append(days_from_earnings_value)
        if any(pd.isna(v) for v in mandatory_values):
            new_row["signal"] = pd.NA
            return new_row, False

        market_cap_passes = (
            not pd.isna(market_cap_value)
            and self._min_market_cap <= market_cap_value <= self._max_market_cap
        ) or (pd.isna(market_cap_value) and self._include_null_market_cap)
        days_from_earnings_passes = (
            self._min_days_from_earnings is None
            or days_from_earnings_value >= self._min_days_from_earnings
        ) and (
            self._max_days_from_earnings is None
            or days_from_earnings_value <= self._max_days_from_earnings
        )

        passes_filter = (
            adv_value >= self._min_adv
            and adr_value >= self._min_adr
            and (self._max_adr is None or adr_value <= self._max_adr)
            and market_cap_passes
            and cum_volume_value >= self._min_volume
            and abs(atr_gap_value) > self._atr_multiplier
            and atr_gap_value > 0
            and new_row[C.CLOSE] > new_row[C.OPEN]
            and days_from_earnings_passes
        )

        return new_row, passes_filter
