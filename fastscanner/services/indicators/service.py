import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any
from uuid import uuid4

import pandas as pd

from fastscanner.pkg.candle import CandleBuffer
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR

from .lib import Indicator, IndicatorsLibrary
from .ports import CandleCol, CandleStore, Channel, FundamentalDataStore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


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

    async def calculate_from_params(
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
        return await self.calculate(symbol, start, end, freq, ind_instances)

    async def calculate(
        self,
        symbol: str,
        start: date,
        end: date,
        freq: str,
        indicators: list[Indicator],
    ) -> pd.DataFrame:
        for indicator in indicators:
            df = await indicator.extend(symbol, df)
        return df.loc[df.index.date >= start]  # type: ignore

    async def subscribe_realtime(
        self,
        symbol: str,
        freq: str,
        indicators: list[IndicatorParams],
        handler: "SubscriptionHandler",
    ) -> str:
        """
        Redis -> RedisChannel -> For all subscribers, compute the indicators -> SubscriptionHandler
            candle                                                          candle with indicators
        Store the subscription handler in a dictionary.
        Every time we get a new candle, for the symbol, we will first fill the new row with the indicators (extend_realtime).
        Then we will call the handler with the new row.
        The first time you get a subscription to a symbol, you need to subscribe to the channel.

        Return the subscription ID.
        """
        indicator_instances = [
            IndicatorsLibrary.instance().get(i.type_, i.params) for i in indicators
        ]

        stream_key = f"candles_min_{symbol}"
        sub_handler = CandleChannelHandler(symbol, indicator_instances, handler, freq)
        await self.channel.subscribe(stream_key, sub_handler)
        return sub_handler.id()

    async def unsubscribe_realtime(self, symbol: str, subscription_id: str):
        """
        Unsubscribe from real-time updates for a specific symbol and frequency.
        """
        stream_key = f"candles_min_{symbol}"
        await self.channel.unsubscribe(stream_key, subscription_id)


class SubscriptionHandler:
    async def handle(self, symbol: str, new_row: pd.Series) -> pd.Series: ...


class CandleChannelHandler:
    def __init__(
        self,
        symbol: str,
        indicators: list[Indicator],
        handler: SubscriptionHandler,
        freq: str,
        candle_timeout: float = 20,
    ) -> None:
        self._id = str(uuid4())
        self._symbol = symbol
        self._indicators = indicators
        self._handler = handler
        self._freq = freq
        self._buffer = CandleBuffer(symbol, freq, self._handle, candle_timeout)
        self._candle_timeout = candle_timeout
        self._buffer_lock = asyncio.Lock()

    async def _handle(self, row: pd.Series) -> None:
        for ind in self._indicators:
            row = await ind.extend_realtime(self._symbol, row)

        await self._handler.handle(self._symbol, row)

    async def handle(self, channel_id: str, data: dict[Any, Any]) -> None:
        data = data.copy()
        timestamp = data.pop("timestamp")
        ts = pd.to_datetime(timestamp, unit="ms", utc=True).tz_convert(
            LOCAL_TIMEZONE_STR
        )
        new_row = pd.Series(data, name=ts)
        if self._freq == "1min":
            for ind in self._indicators:
                new_row = await ind.extend_realtime(self._symbol, new_row)
            await self._handler.handle(self._symbol, new_row)
            return
        agg = await self._buffer.add(new_row)
        if agg is None:
            return
        await self._handle(agg)

    def id(self) -> str:
        return self._id
