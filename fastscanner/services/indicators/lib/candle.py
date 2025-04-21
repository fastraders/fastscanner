from datetime import datetime, time

import numpy as np
import pandas as pd

from ..ports import CandleCol
from ..registry import ApplicationRegistry


class CumulativeDailyVolumeIndicator:
    @classmethod
    def type(cls):
        return "cumulative_daily_volume"

    def column_name(self):
        return self.type()

    def enrich(self, prev_df: pd.DataFrame, new_rows: pd.DataFrame) -> pd.DataFrame:
        volume = new_rows[CandleCol.VOLUME]
        if not prev_df.empty:
            volume = pd.concat([prev_df.iloc[-1:][CandleCol.VOLUME], volume])

        assert isinstance(volume.index, pd.DatetimeIndex)
        cum_volume = volume.groupby(volume.index.date).cumsum()
        if not prev_df.empty:
            cum_volume = cum_volume.iloc[1:]
        new_rows[self.column_name()] = cum_volume

        return new_rows


class PremarketHighIndicator:
    @classmethod
    def type(cls):
        return "premarket_high"

    def column_name(self):
        return self.type()

    def enrich(self, prev_df: pd.DataFrame, new_rows: pd.DataFrame) -> pd.DataFrame:
        # Adds the last row to take it into account for the cumulative operation
        high = new_rows[CandleCol.HIGH]
        if not prev_df.empty:
            high = pd.concat([prev_df.iloc[-1:][CandleCol.HIGH], high])
        assert isinstance(high.index, pd.DatetimeIndex)
        cum_high = high.groupby(high.index.date).cummax()
        cum_high[cum_high.index.time < time(9, 30)] = pd.NA  # type: ignore
        cum_high = cum_high.ffill()

        # Removes the first row that was previously added.
        if not prev_df.empty:
            cum_high = cum_high.iloc[1:]
        new_rows[self.column_name()] = cum_high

        return new_rows


class PremarketLowIndicator:
    @classmethod
    def type(cls):
        return "premarket_low"

    def column_name(self):
        return self.type()

    def enrich(self, prev_df: pd.DataFrame, new_rows: pd.DataFrame) -> pd.DataFrame:
        # Adds the last row to take it into account for the cumulative operation
        low = new_rows[CandleCol.LOW]
        if not prev_df.empty:
            low = pd.concat([prev_df.iloc[-1:][CandleCol.LOW], low])
        assert isinstance(low.index, pd.DatetimeIndex)
        cum_low = low.groupby(low.index.date).cummin()
        cum_low[cum_low.index.time < time(9, 30)] = pd.NA  # type: ignore
        cum_low = cum_low.ffill()

        # Removes the first row that was previously added.
        if not prev_df.empty:
            cum_low = cum_low.iloc[1:]
        new_rows[self.column_name()] = cum_low
        return new_rows
