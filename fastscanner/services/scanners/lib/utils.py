import math

import pandas as pd

from fastscanner.services.indicators.lib.fundamental import (
    DaysSinceIPOIndicator,
    MarketCapIndicator,
)

market_cap_col = MarketCapIndicator().column_name()
days_since_ipo_col = DaysSinceIPOIndicator().column_name()


def filter_by_market_cap(
    df: pd.DataFrame,
    min_market_cap,
    max_market_cap,
    include_null_market_cap,
) -> pd.DataFrame:
    market_cap_filter = (df[market_cap_col] >= min_market_cap) & (
        df[market_cap_col] <= max_market_cap
    )

    if include_null_market_cap:
        market_cap_filter |= df[market_cap_col].isnull()

    return df[market_cap_filter]


def filter_by_days_since_ipo(
    df: pd.DataFrame,
    min_days_since_ipo: int,
) -> pd.DataFrame:
    return df[
        (df[days_since_ipo_col] >= min_days_since_ipo) | df[days_since_ipo_col].isnull()
    ]
