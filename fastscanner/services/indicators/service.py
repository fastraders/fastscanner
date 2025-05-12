from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from .lib import IndicatorsLibrary
from .ports import CandleStore, Channel, FundamentalDataStore
from .registry import ApplicationRegistry
from .utils import lookback_days


@dataclass
class IndicatorParams:
    type_: str
    params: dict[str, Any]


class IndicatorsService:
    def __init__(
        self,
        candles: CandleStore,
        fundamentals: FundamentalDataStore,  # , channel: Channel
    ) -> None:
        self.candles = candles
        self.fundamentals = fundamentals
        # self.channel = channel

    def calculate(
        self,
        symbol: str,
        start: date,
        end: date,
        freq: str,
        indicators: list[IndicatorParams],
    ) -> pd.DataFrame:
        ind_instances = [
            IndicatorsLibrary.instance().get(i.type_, i.params) for i in indicators
        ]
        days = max(i.lookback_days() for i in ind_instances)
        lagged_start = lookback_days(start, days)

        df = self.candles.get(symbol, lagged_start, end, freq)
        if df.empty:
            return df

        for indicator in ind_instances:
            df = indicator.extend(symbol, df)
        return df.loc[df.index.date >= start]  # type: ignore

    def subscribe_realtime(
        self,
        symbol: str,
        indicators: list[IndicatorParams],
        handler: "SubscriptionHandler",
    ):
        """
        Every time you get a new subscription to a symbol in Redis, cancel the current xread and start a new one subscribed to multiple streams.

        Redis -> RedisChannel -> For all subscribers, compute the indicators -> SubscriptionHandler
            candle                                                          candle with indicators
        Store the subscription handler in a dictionary.
        Every time we get a new candle, for the symbol, we will first fill the new row with the indicators (extend_realtime).
        Then we will call the handler with the new row.
        The first time you get a subscription to a symbol, you need to subscribe to the channel.
        """
        ...


class SubscriptionHandler:
    def handle(self, symbol: str, new_row: pd.Series) -> pd.Series: ...
