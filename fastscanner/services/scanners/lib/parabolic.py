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
    PrevDayIndicator,
)
from fastscanner.services.indicators.lib.fundamental import MarketCapIndicator
from fastscanner.services.indicators.ports import CandleCol as C
from fastscanner.services.registry import ApplicationRegistry

from .utils import filter_by_market_cap


class ATRParabolicDownScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        atr_multiplier: float,
        min_volume: float,
        start_time: time,
        end_time: time,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
        include_null_market_cap: bool = False,
    ) -> None:
        self._id = str(uuid.uuid4())
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self._min_volume = min_volume
        self._start_time = start_time
        self._end_time = end_time
        self._min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap
        self._include_null_market_cap = include_null_market_cap

        self._adv = ADVIndicator(period=14)
        self._adr = ADRIndicator(period=14)
        self._atr = DailyATRIndicator(period=14)
        self._market_cap = MarketCapIndicator()
        self._cum_low = CumulativeIndicator(C.LOW, CumOp.MIN)
        self._cum_volume = CumulativeDailyVolumeIndicator()

    def id(self) -> str:
        return self._id

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
        daily_df = await self._atr.extend(symbol, daily_df)
        daily_df = await self._market_cap.extend(symbol, daily_df)

        daily_df = daily_df[daily_df[self._adv.column_name()] >= self._min_adv]
        daily_df = daily_df[daily_df[self._adr.column_name()] >= self._min_adr]
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

        # Comment out for lowest low from 4am logic
        # df = await self._cum_low.extend(symbol, df)
        # df = df[(df[self._cum_low.column_name()] - df[C.LOW]).abs() < 0.0001]

        df = df.loc[(df.index.time >= self._start_time) & (df.index.time <= self._end_time)]  # type: ignore

        # Comment out for lowest lowe from start_time logic
        # df = await self._cum_low.extend(symbol, df)
        # df = df[(df[self._cum_low.column_name()] - df[C.LOW]).abs() < 0.0001]

        if df.empty:
            return df

        df = await self._cum_volume.extend(symbol, df)
        df.loc[:, "date"] = df.index.date  # type: ignore
        daily_df = daily_df.set_index(daily_df.index.date)[  # type: ignore
            [
                self._adv.column_name(),
                self._adr.column_name(),
                self._atr.column_name(),
                self._market_cap.column_name(),
            ]
        ]
        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])

        # Logic: parabolic down condition
        df["signal"] = (df[C.OPEN] - df[C.CLOSE]) / df[self._atr.column_name()]
        df = df[df["signal"] > self._atr_multiplier]
        df = df[df[self._cum_volume.column_name()] >= self._min_volume]

        return df

    async def scan_realtime(
        self, symbol: str, new_row: pd.Series, freq: str
    ) -> tuple[pd.Series, bool]:

        assert isinstance(new_row.name, pd.Timestamp)
        if (
            new_row.name.time() > self._end_time
            or new_row.name.time() < self._start_time
        ):
            new_row["signal"] = pd.NA
            return new_row, False

        new_row = await self._adv.extend_realtime(symbol, new_row)
        new_row = await self._adr.extend_realtime(symbol, new_row)
        new_row = await self._atr.extend_realtime(symbol, new_row)
        new_row = await self._market_cap.extend_realtime(symbol, new_row)
        new_row = await self._cum_low.extend_realtime(symbol, new_row)
        new_row = await self._cum_volume.extend_realtime(symbol, new_row)

        adv_value = new_row[self._adv.column_name()]
        adr_value = new_row[self._adr.column_name()]
        atr_value = new_row[self._atr.column_name()]
        market_cap_value = new_row[self._market_cap.column_name()]
        cum_volume_value = new_row[self._cum_volume.column_name()]

        mandatory_values = [
            adv_value,
            adr_value,
            atr_value,
            cum_volume_value,
        ]
        if any(pd.isna(v) for v in mandatory_values):
            new_row["signal"] = pd.NA
            return new_row, False

        signal_value = (new_row[C.OPEN] - new_row[C.CLOSE]) / atr_value
        new_row["signal"] = signal_value

        market_cap_passes = (
            not pd.isna(market_cap_value)
            and self._min_market_cap <= market_cap_value <= self._max_market_cap
        ) or (pd.isna(market_cap_value) and self._include_null_market_cap)

        passes_filter = (
            adv_value >= self._min_adv
            and adr_value >= self._min_adr
            and market_cap_passes
            and signal_value > self._atr_multiplier
            and cum_volume_value >= self._min_volume
        )

        return new_row, passes_filter


class ATRParabolicUpScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        atr_multiplier: float,
        min_volume: float,
        start_time: time,
        end_time: time,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
        include_null_market_cap: bool = False,
    ) -> None:
        self._id = str(uuid.uuid4())
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self._min_volume = min_volume
        self._start_time = start_time
        self._end_time = end_time
        self._min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap
        self._include_null_market_cap = include_null_market_cap

        self._adv = ADVIndicator(period=14)
        self._adr = ADRIndicator(period=14)
        self._atr = DailyATRIndicator(period=14)
        self._market_cap = MarketCapIndicator()
        self._cum_high = CumulativeIndicator(C.HIGH, CumOp.MAX)
        self._cum_volume = CumulativeDailyVolumeIndicator()

    def id(self) -> str:
        return self._id

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
        daily_df = await self._atr.extend(symbol, daily_df)
        daily_df = await self._market_cap.extend(symbol, daily_df)

        daily_df = daily_df[daily_df[self._adv.column_name()] >= self._min_adv]
        daily_df = daily_df[daily_df[self._adr.column_name()] >= self._min_adr]
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
        # Comment out for highest high from 4am logic
        # df = await self._cum_high.extend(symbol, df)
        # df = df[(df[self._cum_high.column_name()] - df[C.HIGH]).abs() < 0.0001]

        df = df.loc[(df.index.time >= self._start_time) & (df.index.time <= self._end_time)]  # type: ignore

        # Comment out for highest high from start_time logic
        # df = await self._cum_high.extend(symbol, df)
        # df = df[(df[self._cum_high.column_name()] - df[C.HIGH]).abs() < 0.0001]
        if df.empty:
            return df

        df = await self._cum_volume.extend(symbol, df)
        df.loc[:, "date"] = df.index.date  # type: ignore
        daily_df = daily_df.set_index(daily_df.index.date)[  # type: ignore
            [
                self._adv.column_name(),
                self._adr.column_name(),
                self._atr.column_name(),
                self._market_cap.column_name(),
            ]
        ]
        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])

        # Logic: parabolic up condition
        df["signal"] = (df[C.CLOSE] - df[C.OPEN]) / df[self._atr.column_name()]
        df = df[df["signal"] > self._atr_multiplier]
        df = df[df[self._cum_volume.column_name()] >= self._min_volume]

        return df

    async def scan_realtime(
        self, symbol: str, new_row: pd.Series, freq: str
    ) -> tuple[pd.Series, bool]:

        assert isinstance(new_row.name, pd.Timestamp)
        if (
            new_row.name.time() > self._end_time
            or new_row.name.time() < self._start_time
        ):
            new_row["signal"] = pd.NA
            return new_row, False

        new_row = await self._adv.extend_realtime(symbol, new_row)
        new_row = await self._adr.extend_realtime(symbol, new_row)
        new_row = await self._atr.extend_realtime(symbol, new_row)
        new_row = await self._market_cap.extend_realtime(symbol, new_row)
        new_row = await self._cum_high.extend_realtime(symbol, new_row)
        new_row = await self._cum_volume.extend_realtime(symbol, new_row)

        adv_value = new_row[self._adv.column_name()]
        adr_value = new_row[self._adr.column_name()]
        atr_value = new_row[self._atr.column_name()]
        market_cap_value = new_row[self._market_cap.column_name()]
        cum_volume_value = new_row[self._cum_volume.column_name()]

        mandatory_values = [
            adv_value,
            adr_value,
            atr_value,
            cum_volume_value,
        ]
        if any(pd.isna(v) for v in mandatory_values):
            new_row["signal"] = pd.NA
            return new_row, False

        signal_value = (new_row[C.CLOSE] - new_row[C.OPEN]) / atr_value
        new_row["signal"] = signal_value

        market_cap_passes = (
            not pd.isna(market_cap_value)
            and self._min_market_cap <= market_cap_value <= self._max_market_cap
        ) or (pd.isna(market_cap_value) and self._include_null_market_cap)

        passes_filter = (
            adv_value >= self._min_adv
            and adr_value >= self._min_adr
            and market_cap_passes
            and signal_value > self._atr_multiplier
            and cum_volume_value >= self._min_volume
        )

        return new_row, passes_filter


class DailyATRParabolicUpScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        atr_multiplier: float,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
        include_null_market_cap: bool = False,
    ) -> None:
        self._id = str(uuid.uuid4())
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self._min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap
        self._include_null_market_cap = include_null_market_cap

    def id(self) -> str:
        return self._id

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        if freq != "1d":
            raise ValueError("DailyATRParabolicUpScanner only supports '1d' frequency")

        adv = ADVIndicator(period=14)
        adr = ADRIndicator(period=14)
        daily_atr = DailyATRIndicator(period=14)
        prev_close = PrevDayIndicator(C.CLOSE)
        prev_open = PrevDayIndicator(C.OPEN)
        market_cap = MarketCapIndicator()

        daily_df = await ApplicationRegistry.candles.get(
            symbol,
            start,
            end,
            "1d",
        )
        if daily_df.empty:
            return daily_df

        daily_df = daily_df.shift(1)
        daily_df = await adv.extend(symbol, daily_df)
        daily_df = await adr.extend(symbol, daily_df)
        daily_df = await daily_atr.extend(symbol, daily_df)
        daily_df = await prev_close.extend(symbol, daily_df)
        daily_df = await prev_open.extend(symbol, daily_df)
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

        daily_df["signal"] = (
            daily_df[prev_close.column_name()] - daily_df[prev_open.column_name()]
        ) / daily_df[daily_atr.column_name()]

        return daily_df[daily_df["signal"] > self._atr_multiplier]

    async def scan_realtime(
        self, symbol: str, new_row: pd.Series, freq: str
    ) -> tuple[pd.Series, bool]: ...


class DailyATRParabolicDownScanner:
    def __init__(
        self,
        min_adv: float,
        min_adr: float,
        atr_multiplier: float,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
        include_null_market_cap: bool = False,
    ) -> None:
        self._id = str(uuid.uuid4())
        self._min_adv = min_adv
        self._min_adr = min_adr
        self._atr_multiplier = atr_multiplier
        self._min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap
        self._include_null_market_cap = include_null_market_cap

    def id(self) -> str:
        return self._id

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:
        if freq != "1d":
            raise ValueError("DailyATRParabolicUpScanner only supports '1d' frequency")

        adv = ADVIndicator(period=14)
        adr = ADRIndicator(period=14)
        daily_atr = DailyATRIndicator(period=14)
        prev_close = PrevDayIndicator(C.CLOSE)
        prev_open = PrevDayIndicator(C.OPEN)
        market_cap = MarketCapIndicator()

        daily_df = await ApplicationRegistry.candles.get(
            symbol,
            start,
            end,
            "1d",
        )
        if daily_df.empty:
            return daily_df

        daily_df = daily_df.shift(1)
        daily_df = await adv.extend(symbol, daily_df)
        daily_df = await adr.extend(symbol, daily_df)
        daily_df = await daily_atr.extend(symbol, daily_df)
        daily_df = await prev_close.extend(symbol, daily_df)
        daily_df = await prev_open.extend(symbol, daily_df)
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

        daily_df["signal"] = (
            daily_df[prev_open.column_name()] - daily_df[prev_close.column_name()]
        ) / daily_df[daily_atr.column_name()]

        return daily_df[daily_df["signal"] > self._atr_multiplier]

    async def scan_realtime(
        self, symbol: str, new_row: pd.Series, freq: str
    ) -> tuple[pd.Series, bool]: ...
