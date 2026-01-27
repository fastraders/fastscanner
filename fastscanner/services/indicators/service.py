import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Awaitable, Callable, Iterable, Protocol
from uuid import uuid4

import pandas as pd

from fastscanner.pkg.candle import CandleBuffer
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR, ClockRegistry, split_freq
from fastscanner.services.exceptions import UnsubscribeSignal

from .lib import Cacheable, CacheableIndicator, Indicator, IndicatorsLibrary
from .ports import Cache, CandleStore, Channel, FundamentalDataStore

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
        cache: Cache,
        symbols_subscribe_channel: str,
        symbols_unsubscribe_channel: str,
        cache_at_seconds: int,
    ) -> None:
        self.candles = candles
        self.fundamentals = fundamentals
        self.channel = channel
        self._cache = cache
        self._symbols_subscribe_channel = symbols_subscribe_channel
        self._symbols_unsubscribe_channel = symbols_unsubscribe_channel
        self._subscription_to_channel: dict[str, str] = {}
        # Cache parameters
        self._cached_indicators: list[CacheableIndicator] = []
        self._cache_at_seconds = cache_at_seconds
        self._caching_task: asyncio.Task[None] | None = None

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
            indicator_instances, handler, freq, self.unsubscribe_realtime
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

    async def cache_indicators(
        self,
        indicators: Iterable[CacheableIndicator],
    ) -> str:
        self._cached_indicators.extend(indicators)
        if self._caching_task is None:
            self._caching_task = asyncio.create_task(self._start_caching())
        stream_pattern = "candles.min.*"

        latency_handler = LatencyMeasurementHandler("1min")
        cache_handler = CandleChannelHandler(
            indicators,
            latency_handler,
            "1min",
            self.unsubscribe_realtime,
        )

        self._subscription_to_channel[cache_handler.id()] = stream_pattern
        await self.channel.subscribe(stream_pattern, cache_handler)
        return cache_handler.id()

    async def stop_caching(self, subscription_id: str) -> None:
        await self.unsubscribe_realtime(subscription_id)
        if self._caching_task:
            try:
                self._caching_task.cancel()
                await self._caching_task
            except asyncio.CancelledError:
                pass
            self._caching_task = None
        self._cached_indicators = []

    async def _start_caching(self) -> None:
        while True:
            now = ClockRegistry.clock.now()
            # We receive the latest candle at 20:00 UTC but it can have a bit of delay.
            if (now.time() < time(4, 0, self._cache_at_seconds)) or (
                now.time() > time(20, 1, self._cache_at_seconds)
            ):
                next_premarket = ClockRegistry.clock.next_datetime_at(
                    time(4, 0, self._cache_at_seconds)
                )
                logger.info(
                    f"Waiting for pre-market to cache indicators. Now: {now}, next pre-market at: {next_premarket}"
                )
                await asyncio.sleep((next_premarket - now).total_seconds())
                continue

            now = ClockRegistry.clock.now()
            for indicator in self._cached_indicators:
                try:
                    await indicator.save_to_cache()
                except Exception as e:
                    logger.exception(e)
                    logger.error(f"Error caching indicator {indicator.column_name()}")

            next_minute = (now + timedelta(minutes=1)).replace(
                second=self._cache_at_seconds, microsecond=0
            )
            await asyncio.sleep((next_minute - now).total_seconds())

    async def unsubscribe_realtime(
        self,
        subscription_id: str,
        _send_events: bool = True,
    ) -> None:
        """
        Unsubscribe from real-time updates for a specific symbol and frequency.
        """
        stream_key = self._subscription_to_channel.get(subscription_id)
        if stream_key is None:
            return
        _, unit, symbol = stream_key.split(".", 2)
        # Skip sending unsubscribe signal for persister subscriptions to avoid cycles.
        if _send_events and symbol != "*":
            await self.channel.push(
                self._symbols_unsubscribe_channel,
                {
                    "symbol": symbol,
                    "subscriber_id": subscription_id,
                    "unit": unit,
                },
            )
        await self.channel.unsubscribe(stream_key, subscription_id)

    async def stop(self):
        for sub_id, channel in self._subscription_to_channel.items():
            _, unit, symbol = channel.split(".", 2)
            await self.channel.unsubscribe(channel, sub_id)
            await self.channel.push(
                self._symbols_unsubscribe_channel,
                {
                    "symbol": symbol,
                    "subscriber_id": sub_id,
                    "unit": unit,
                },
            )


class SubscriptionHandler(Protocol):
    async def handle(self, symbol: str, new_row: pd.Series) -> pd.Series: ...


class CandleChannelHandler:
    def __init__(
        self,
        indicators: Iterable[Indicator],
        handler: SubscriptionHandler,
        freq: str,
        unsubscribe: Callable[[str], Awaitable[None]],
    ) -> None:
        self._id = str(uuid4())
        self._indicators = indicators
        self._handler = handler
        self._freq = freq
        self._timeout_seconds = 2.8
        self._timeout_minutes = 10.0
        self._buffers: dict[str, CandleBuffer] = {}
        self._unsubscribe = unsubscribe

    async def _handle(self, symbol: str, new_row: pd.Series) -> None:
        for ind in self._indicators:
            new_row = await ind.extend_realtime(symbol, new_row)
        try:
            await self._handler.handle(symbol, new_row)
        except UnsubscribeSignal:
            await self._unsubscribe(self._id)
            return

    def _new_buffer(self, symbol: str) -> CandleBuffer:
        async def _handle(new_row: pd.Series) -> None:
            await self._handle(symbol, new_row)

        buffer = CandleBuffer(symbol, self._freq, _handle)
        self._buffers[symbol] = buffer
        return buffer

    async def handle(self, channel_id: str, data: dict[Any, Any]) -> None:
        symbol = channel_id.split(".", 2)[-1]
        buffer = self._buffers.get(symbol)
        if buffer is None:
            buffer = self._new_buffer(symbol)
        ts = pd.to_datetime(int(data["timestamp"]), unit="ms", utc=True).tz_convert(
            LOCAL_TIMEZONE_STR
        )
        new_row = pd.Series(data, name=ts)
        if self._freq == "1min":
            return await self._handle(symbol, new_row)
        agg = await buffer.add(new_row)
        if agg is None:
            return
        await self._handle(symbol, agg)

    def id(self) -> str:
        return self._id


class LatencyMeasurementHandler(SubscriptionHandler):
    def __init__(self, freq: str) -> None:
        self._freq = freq
        self._latest_timestamp: datetime | None = None
        self._current_minute: datetime | None = None

    async def handle(self, symbol: str, new_row: pd.Series) -> pd.Series:
        if self._current_minute is None or self._latest_timestamp is None:
            self._current_minute = new_row.name + pd.Timedelta(self._freq)  # type: ignore
            self._latest_timestamp = ClockRegistry.clock.now()
            return new_row
        new_minute = new_row.name + pd.Timedelta(self._freq)  # type: ignore
        if new_minute > self._current_minute:
            latency = self._latest_timestamp - self._current_minute
            logger.info(
                f"Latency at {self._current_minute.strftime('%H:%M')}: {latency.total_seconds():.2f} seconds"
            )
            self._current_minute = new_minute

        self._latest_timestamp = ClockRegistry.clock.now()
        return new_row
