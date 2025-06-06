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
        self._adv = ADVIndicator(period=14)
        self._adr = ADRIndicator(period=14)
        self._atr = DailyATRIndicator(period=14)
        self._prev_close = PrevDayIndicator(candle_col=C.CLOSE)
        self._market_cap = MarketCapIndicator()
        self._cum_low = PremarketCumulativeIndicator(C.LOW, CumOp.MIN)
        self._atr_iday: dict[str, ATRIndicator] = {}

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:

        daily_df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            "1d",
            [
                self._adv,
                self._adr,
                self._atr,
                self._prev_close,
                self._market_cap,
            ],
        )
        if daily_df.empty:
            return daily_df

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
        atr_iday = self._atr_iday.setdefault(freq, ATRIndicator(period=140, freq=freq))
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
                self._adv.column_name(),
                self._adr.column_name(),
                self._atr.column_name(),
                self._prev_close.column_name(),
            ]
        ]
        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])
        if df.empty:
            return df

        gap_col = cum_low.column_name()
        prev_close_col = self._prev_close.column_name()
        atr_col = self._atr.column_name()

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
        
        # Check time filter first
        assert isinstance(new_row.name, pd.Timestamp)
        if new_row.name.time() > self._end_time:
            new_row["signal"] = pd.NA
            return new_row, False

        atr_iday = self._atr_iday.setdefault(freq, ATRIndicator(period=140, freq=freq))
        new_row = await self._adv.extend_realtime(symbol, new_row)
        new_row = await self._adr.extend_realtime(symbol, new_row)
        new_row = await self._atr.extend_realtime(symbol, new_row)
        new_row = await self._prev_close.extend_realtime(symbol, new_row)
        new_row = await self._market_cap.extend_realtime(symbol, new_row)

        new_row = await atr_iday.extend_realtime(symbol, new_row)
        new_row = await self._cum_low.extend_realtime(symbol, new_row)

        adv_value = new_row[self._adv.column_name()]
        adr_value = new_row[self._adr.column_name()]
        market_cap_value = new_row[self._market_cap.column_name()]

        gap_col = self._cum_low.column_name()
        prev_close_col = self._prev_close.column_name()
        atr_col = self._atr.column_name()

        gap_value = new_row[gap_col]
        prev_close_value = new_row[prev_close_col]
        atr_value = new_row[atr_col]

        mandatory_values = [
            adv_value,
            adr_value,
            gap_value,
            prev_close_value,
            atr_value,
        ]
        if any(pd.isna(v) for v in mandatory_values):
            new_row["signal"] = pd.NA
            return new_row, False

        market_cap_passes = (
            not pd.isna(market_cap_value)
            and self._min_market_cap <= market_cap_value <= self._max_market_cap
        ) or (pd.isna(market_cap_value) and self._include_null_market_cap)

        signal = (gap_value - prev_close_value) / atr_value
        new_row["signal"] = signal

        passes_filter = (
            adv_value >= self._min_adv
            and adr_value >= self._min_adr
            and market_cap_passes
            and atr_value > 0
            and signal < self._atr_multiplier
        )

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

        self._adv = ADVIndicator(period=14)
        self._adr = ADRIndicator(period=14)
        self._atr = DailyATRIndicator(period=14)
        self._prev_close = PrevDayIndicator(candle_col=C.CLOSE)
        self._market_cap = MarketCapIndicator()
        self._cum_high = PremarketCumulativeIndicator(C.HIGH, CumOp.MAX)
        self._atr_iday: dict[str, ATRIndicator] = {}

    async def scan(
        self, symbol: str, start: date, end: date, freq: str
    ) -> pd.DataFrame:

        daily_df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            "1d",
            [self._adv, self._adr, self._atr, self._prev_close, self._market_cap],
        )
        if daily_df.empty:
            return daily_df

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

        atr_iday = self._atr_iday.setdefault(freq, ATRIndicator(period=140, freq=freq))
        df = await ApplicationRegistry.indicators.calculate(
            symbol,
            start,
            end,
            freq,
            [atr_iday, self._cum_high],
        )
        df = df.loc[df.index.time <= self._end_time]  # type: ignore
        if df.empty:
            return df

        df.loc[:, "date"] = df.index.date  # type: ignore
        daily_df = daily_df.set_index(daily_df.index.date)[  # type: ignore
            [
                self._adv.column_name(),
                self._adr.column_name(),
                self._atr.column_name(),
                self._prev_close.column_name(),
            ]
        ]

        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])
        if df.empty:
            return df

        gap_col = self._cum_high.column_name()
        prev_close_col = self._prev_close.column_name()
        atr_col = self._atr.column_name()
        df["signal"] = (df[gap_col] - df[prev_close_col]) / df[atr_col]
        df = df[df["signal"] > self._atr_multiplier]

        return df

    async def scan_realtime(
        self, symbol: str, new_row: pd.Series, freq: str
    ) -> tuple[pd.Series, bool]:
        """
        Realtime scan implementation that enriches the new_row with indicators
        and returns whether it passes the filter criteria.
        """

        atr_iday = self._atr_iday.setdefault(freq, ATRIndicator(period=140, freq=freq))
        new_row = await self._adv.extend_realtime(symbol, new_row)
        new_row = await self._adr.extend_realtime(symbol, new_row)
        new_row = await self._atr.extend_realtime(symbol, new_row)
        new_row = await self._prev_close.extend_realtime(symbol, new_row)
        new_row = await self._market_cap.extend_realtime(symbol, new_row)

        new_row = await atr_iday.extend_realtime(symbol, new_row)
        new_row = await self._cum_high.extend_realtime(symbol, new_row)

        adv_value = new_row[self._adv.column_name()]
        adr_value = new_row[self._adr.column_name()]
        market_cap_value = new_row[self._market_cap.column_name()]

        gap_col = self._cum_high.column_name()
        prev_close_col = self._prev_close.column_name()
        atr_col = self._atr.column_name()

        gap_value = new_row[gap_col]
        prev_close_value = new_row[prev_close_col]
        atr_value = new_row[atr_col]

        mandatory_values = [
            adv_value,
            adr_value,
            gap_value,
            prev_close_value,
            atr_value,
        ]
        if any(pd.isna(v) for v in mandatory_values):
            new_row["signal"] = pd.NA
            return new_row, False

        market_cap_passes = (
            not pd.isna(market_cap_value)
            and self._min_market_cap <= market_cap_value <= self._max_market_cap
        ) or (pd.isna(market_cap_value) and self._include_null_market_cap)

        signal = (gap_value - prev_close_value) / atr_value
        new_row["signal"] = signal

        passes_filter = (
            adv_value >= self._min_adv
            and adr_value >= self._min_adr
            and market_cap_passes
            and atr_value > 0
            and signal > self._atr_multiplier
        )

        return new_row, passes_filter
