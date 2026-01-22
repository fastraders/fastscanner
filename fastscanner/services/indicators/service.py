import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Awaitable, Callable
from uuid import uuid4

import pandas as pd

from fastscanner.pkg.candle import CandleBuffer
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR, split_freq
from fastscanner.services.exceptions import UnsubscribeSignal

from .lib import Indicator, IndicatorsLibrary
from .ports import CandleStore, Channel, FundamentalDataStore

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
        symbols_subscribe_channel: str,
        symbols_unsubscribe_channel: str,
    ) -> None:
        self.candles = candles
        self.fundamentals = fundamentals
        self.channel = channel
        self._symbols_subscribe_channel = symbols_subscribe_channel
        self._symbols_unsubscribe_channel = symbols_unsubscribe_channel
        self._subscription_to_channel: dict[str, str] = {}

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
        df = await self.candles.get(symbol, start, end, freq)
        if df.empty:
            return df

        for indicator in indicators:
            df = await indicator.extend(symbol, df)
        return df.loc[df.index.date >= start]  # type: ignore

    async def subscribe_realtime(
        self,
        symbol: str,
        freq: str,
        indicators: list[IndicatorParams],
        handler: "SubscriptionHandler",
        _send_events: bool = True,
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

        _, unit = split_freq(freq)
        unit_to_channel = {
            "s": "candles.s.",
            "min": "candles.min.",
        }
        stream_key = f"{unit_to_channel[unit]}{symbol}"
        sub_handler = CandleChannelHandler(
            symbol, indicator_instances, handler, freq, self.unsubscribe_realtime
        )
        # Skip sending subscribe signal for persister subscriptions to avoid cycles.
        if _send_events:
            await self.channel.push(
                self._symbols_subscribe_channel,
                {
                    "symbol": symbol,
                    "subscriber_id": sub_handler.id(),
                    "unit": unit,
                },
            )
        self._subscription_to_channel[sub_handler.id()] = stream_key
        # Configures the handler to receive messages from the channel
        await self.channel.subscribe(stream_key, sub_handler)
        return sub_handler.id()

    async def unsubscribe_realtime(
        self,
        symbol: str,
        subscription_id: str,
        _send_events: bool = True,
    ) -> None:
        """
        Unsubscribe from real-time updates for a specific symbol and frequency.
        """
        stream_key = self._subscription_to_channel.get(subscription_id)
        if stream_key is None:
            return
        # Skip sending unsubscribe signal for persister subscriptions to avoid cycles.
        if _send_events:
            await self.channel.push(
                self._symbols_unsubscribe_channel,
                {
                    "symbol": symbol,
                    "subscriber_id": subscription_id,
                    "unit": stream_key.split(".")[1],
                },
            )
        await self.channel.unsubscribe(stream_key, subscription_id)

    async def stop(self):
        for sub_id, channel in self._subscription_to_channel.items():
            _, unit, symbol = channel.split(".")
            await self.channel.unsubscribe(channel, sub_id)
            await self.channel.push(
                self._symbols_unsubscribe_channel,
                {
                    "symbol": symbol,
                    "subscriber_id": sub_id,
                    "unit": unit,
                },
            )


class SubscriptionHandler:
    async def handle(self, symbol: str, new_row: pd.Series) -> pd.Series: ...


class CandleChannelHandler:
    def __init__(
        self,
        symbol: str,
        indicators: list[Indicator],
        handler: SubscriptionHandler,
        freq: str,
        unsubscribe: Callable[[str, str], Awaitable[None]],
    ) -> None:
        self._id = str(uuid4())
        self._symbol = symbol
        self._indicators = indicators
        self._handler = handler
        self._freq = freq
        self._timeout_seconds = 2.8
        self._timeout_minutes = 10.0
        self._buffer = CandleBuffer(symbol, freq, self._handle, self._candle_timeout)
        self._buffer_lock = asyncio.Lock()
        self._unsubscribe = unsubscribe

    @property
    def _candle_timeout(self) -> float:
        if self._freq.endswith("s"):
            return self._timeout_seconds
        return self._timeout_minutes

    async def _handle(self, row: pd.Series) -> None:
        for ind in self._indicators:
            row = await ind.extend_realtime(self._symbol, row)

        try:
            await self._handler.handle(self._symbol, row)
        except UnsubscribeSignal:
            await self._unsubscribe(self._id, self._symbol)
            return

    async def handle(self, channel_id: str, data: dict[Any, Any]) -> None:
        data = data.copy()
        timestamp = data.pop("timestamp")
        ts = pd.to_datetime(timestamp, unit="ms", utc=True).tz_convert(
            LOCAL_TIMEZONE_STR
        )
        new_row = pd.Series(data, name=ts)
        if self._freq == "1min":
            return await self._handle(new_row)
        agg = await self._buffer.add(new_row)
        if agg is None:
            return
        await self._handle(agg)

    def id(self) -> str:
        return self._id
