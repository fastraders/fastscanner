from datetime import date, timedelta

import pandas as pd

from fastscanner.services.indicators.lib.candle import ATRIndicator

from ..registry import ApplicationRegistry


def lookback_days(
    start: date,
    n_days: int,
):
    public_holidays = ApplicationRegistry.holidays.get()
    while n_days > 0:
        start -= timedelta(days=1)
        if start.weekday() < 5 and start not in public_holidays:
            n_days -= 1
    return start


async def get_df_with_atr(
    symbol: str, start: date, end: date, freq: str
) -> pd.DataFrame:
    atr_iday = ATRIndicator(period=140, freq=freq)
    start_atr = lookback_days(start, atr_iday.lookback_days())
    df = await ApplicationRegistry.candles.get(symbol, start_atr, end, freq)
    df = await atr_iday.extend(symbol, df)
    return df
