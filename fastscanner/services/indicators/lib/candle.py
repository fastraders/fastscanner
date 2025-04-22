from datetime import datetime, time

import numpy as np
import pandas as pd

from ..ports import CandleCol


class CumulativeDailyVolumeIndicator:
    @classmethod
    def type(cls):
        return "cumulative_daily_volume"

    def column_name(self):
        return self.type()

    def extend(self, df: pd.DataFrame) -> pd.DataFrame:
        volume = df[CandleCol.VOLUME]
        assert isinstance(volume.index, pd.DatetimeIndex)
        cum_volume = volume.groupby(volume.index.date).cumsum()
        df[self.column_name()] = cum_volume
        return df

    def extend_realtime(
        self, new_rows: pd.DataFrame, prev_df: pd.DataFrame | None
    ) -> pd.DataFrame:
        if prev_df is not None and not prev_df.empty:
            # Adds the last row to take it into account for the cumulative operation
            new_rows = pd.concat([prev_df.iloc[-1:][new_rows.columns], new_rows])

        new_rows = self.extend(new_rows)
        if prev_df is not None and not prev_df.empty:
            new_rows = new_rows.iloc[1:]

        return new_rows


class PremarketCumulativeIndicator:
    def __init__(self, candle_col: str):
        self.candle_col = candle_col

    @classmethod
    def type(cls):
        return "premarket_cumulative"

    def column_name(self):
        return f"premarket_{self.candle_col}"

    def extend(self, df: pd.DataFrame) -> pd.DataFrame:
        values = df[self.candle_col]
        assert isinstance(values.index, pd.DatetimeIndex)
        cum_values = values.groupby(values.index.date).cummax()
        cum_values[cum_values.index.time >= time(9, 30)] = pd.NA  # type: ignore
        cum_values = cum_values.ffill()
        df[self.column_name()] = cum_values
        return df

    def extend_realtime(
        self, new_rows: pd.DataFrame, prev_df: pd.DataFrame | None
    ) -> pd.DataFrame:
        if prev_df is not None and not prev_df.empty:
            # Adds the last row to take it into account for the cumulative operation
            new_rows = pd.concat([prev_df.iloc[-1:][new_rows.columns], new_rows])

        new_rows = self.extend(new_rows)
        if prev_df is not None and not prev_df.empty:
            new_rows = new_rows.iloc[1:]

        return new_rows
