from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

from .lib import Indicator, IndicatorsLibrary
from .ports import (
    CandleCol,
    CandleStore,
    Channel,
    ChannelHandler,
    FundamentalDataStore,
    PublicHolidaysStore,
)
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
        fundamentals: FundamentalDataStore,
        channel: Channel,
    ) -> None:
        self.candles = candles
        self.fundamentals = fundamentals
        self.channel = channel

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

    async def subscribe_realtime(
        self,
        symbol: str,
        freq: str,
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
        indicator_instances = [
            IndicatorsLibrary.instance().get(i.type_, i.params) for i in indicators
        ]

        max_days = max(ind.lookback_days() for ind in indicator_instances)
        today = datetime.now().date()
        lookback_start = today - timedelta(days=max_days)
        end_date = today - timedelta(days=1)

        df = self.candles.get(symbol, lookback_start, end_date, freq)
        if df.empty:
            df = pd.DataFrame()

        for idx in df.index:
            row = df.loc[idx]
            for ind in indicator_instances:
                row = ind.extend_realtime(symbol, row)
            df.loc[idx] = row
        stream_key = f"candles_min_{symbol}"
        await self.channel.subscribe(
            stream_key, CandleChannelHandler(symbol, indicator_instances, handler)
        )


class SubscriptionHandler:
    def handle(self, symbol: str, new_row: pd.Series) -> pd.Series:
        ...


class CandleChannelHandler(ChannelHandler):
    def __init__(
        self,
        symbol: str,
        indicators: list[Indicator],
        handler: SubscriptionHandler,
    ) -> None:
        self.symbol = symbol
        self.indicators = indicators
        self.handler = handler

    async def handle(self, channel_id: str, data: dict[Any, Any]) -> None:
        print(f"[Redis] Received data on {channel_id}: {data}")

        try:

            for field in (
                CandleCol.OPEN,
                CandleCol.HIGH,
                CandleCol.LOW,
                CandleCol.CLOSE,
                CandleCol.VOLUME,
            ):
                if field in data:
                    data[field] = float(data[field])

            ts_raw = data.get("timestamp")
            if ts_raw:
                timestamp = pd.to_datetime(int(ts_raw), unit="ms")
            new_row = pd.Series(data)
            new_row.name = timestamp

            for indicator in self.indicators:
                try:
                    new_row = indicator.extend_realtime(self.symbol, new_row)
                except Exception as ind_err:
                    print(
                        f"[Indicator Error] {indicator.__class__.__name__} failed: {ind_err}"
                    )
                    raise

            self.handler.handle(self.symbol, new_row)

        except Exception as e:
            print(f"[Handler Error] Failed processing message from {channel_id}: {e}")
