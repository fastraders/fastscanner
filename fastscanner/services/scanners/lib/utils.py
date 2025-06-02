import math

import pandas as pd

from fastscanner.services.indicators.lib.fundamental import MarketCapIndicator

market_cap_col = MarketCapIndicator().column_name()


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
