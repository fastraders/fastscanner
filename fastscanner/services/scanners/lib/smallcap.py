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
from fastscanner.services.indicators.lib.candle import GapIndicator, ShiftIndicator
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


class SmallCapUpScanner:
    def __init__(
        self,
        min_volume: float,
        start_time: time,
        end_time: time,
        min_gap: float,
        min_price: float = 0,
        max_price: float = math.inf,
        min_market_cap: float = 0,
        max_market_cap: float = math.inf,
        include_null_market_cap: bool = False,
    ) -> None:
        self._id = str(uuid.uuid4())
        self._min_volume = min_volume
        self._min_gap = min_gap
        self._id = str(uuid.uuid4())
        self._start_time = start_time
        self._end_time = end_time
        self._min_market_cap = min_market_cap
        self._max_market_cap = max_market_cap
        self._min_price = min_price
        self._max_price = max_price
        self._include_null_market_cap = include_null_market_cap
        self._market_cap = MarketCapIndicator()
        self._cum_volume = CumulativeDailyVolumeIndicator()
        self._gap = GapIndicator(C.HIGH)
        self._cum_high = CumulativeIndicator(C.HIGH, CumOp.MAX)
        # The order of the periods defines the priority of the alerts.
        self._shift_periods = [15, 10, 2]
        self._shift_min_change = [0.16, 0.14, 0.12]
        self._shift_indicators = [
            ShiftIndicator(C.LOW, period) for period in self._shift_periods
        ]

    def id(self) -> str:
        return self._id

    @classmethod
    def type(cls) -> str:
        return "small_cap_up"

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

        daily_df = await self._market_cap.extend(symbol, daily_df)
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
        for shift, min_change, shift_indicator in zip(
            self._shift_periods, self._shift_min_change, self._shift_indicators
        ):
            df = await shift_indicator.extend(symbol, df)
            change_col = f"change_{shift}"
            try:
                df[change_col] = (df[C.HIGH] - df[shift_indicator.column_name()]) / df[
                    shift_indicator.column_name()
                ]
                df.loc[df[change_col] > min_change, "triggered_alert"] = change_col
            except Exception as e:
                print(f"Error calculating change for {symbol} with shift {shift}: {e}")
                raise e

        # Comment out for highest high from 4am logic
        # df = await self._cum_high.extend(symbol, df)
        # df = df[(df[self._cum_high.column_name()] - df[C.HIGH]).abs() < 0.0001]

        df = df.loc[(df.index.time >= self._start_time) & (df.index.time <= self._end_time)]  # type: ignore

        # Comment out for highest high from start time logic
        # df = await self._cum_high.extend(symbol, df)
        # df = df[(df[self._cum_high.column_name()] - df[C.HIGH]).abs() < 0.0001]
        # filtered_df['High_Change'] = filtered_df['high'].pct_change()
        # filtered_df['Low_Change'] = filtered_df['low'].pct_change()

        # Comment out for anomaly filtering logic
        # df.loc[:, "_body"] = (df[C.CLOSE] - df[C.OPEN]).abs().replace(0, 0.0001)
        # df.loc[:, "_range"] = (df[C.HIGH] - df[C.LOW]).replace(0, 0.0001)
        # df.loc[:, "_body_change"] = df["_body"].pct_change()
        # df.loc[:, "_range_change"] = df["_range"].pct_change()
        # df.loc[:, "_high_change"] = df[C.HIGH].pct_change()
        # df.loc[:, "_low_change"] = df[C.LOW].pct_change()
        # df.loc[:, "_close_change"] = df[C.CLOSE].pct_change()
        # df.loc[:, "_is_anomaly"] = (
        #     (df["_body"] * 3 < df["_range"])
        #     & (df["_range_change"] * 3 > df["_body_change"])
        #     & (
        #         (df["_high_change"] > 3 * df["_close_change"])
        #         | (df["_low_change"] > 3 * df["_close_change"])
        #     )
        # )
        # n_anomalies = df["_is_anomaly"].sum()
        # df = df[~df["_is_anomaly"]].drop(
        #     columns=[
        #         "_body",
        #         "_range",
        #         "_body_change",
        #         "_range_change",
        #         "_high_change",
        #         "_low_change",
        #         "_close_change",
        #         "_is_anomaly",
        #     ]
        # )

        df = df[df[C.CLOSE] >= self._min_price]
        df = df[df[C.CLOSE] <= self._max_price]
        if df.empty:
            return df

        df.loc[:, "date"] = df.index.date  # type: ignore
        daily_df = daily_df.set_index(daily_df.index.date)[  # type: ignore
            [self._market_cap.column_name()]
        ]
        df = df.join(daily_df, on="date", how="inner")
        df = df.drop(columns=["date"])
        if df.empty:
            return df

        df = await self._gap.extend(symbol, df)

        df = df[df[self._cum_volume.column_name()] >= self._min_volume]
        df = df[df[self._gap.column_name()] >= self._min_gap]
        df = df[df["triggered_alert"].notnull()]

        return df

    async def scan_realtime(
        self, symbol: str, new_row: pd.Series, freq: str
    ) -> tuple[pd.Series, bool]:
        return pd.Series(), False
