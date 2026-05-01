import asyncio
import contextlib
import logging
import random
from datetime import time, timedelta

from fastscanner.pkg.clock import ClockRegistry
from fastscanner.services.indicators.lib.news import InNewsIndicator
from fastscanner.services.indicators.ports import Cache, Channel, ChannelHandler

logger = logging.getLogger(__name__)


class NewsCacheService:
    def __init__(
        self,
        channel: Channel,
        cache: Cache,
        news_subscribe_channel: str,
        news_unsubscribe_channel: str,
        fetch_interval_seconds: int = 60,
        jitter_max_seconds: float = 45.0,
    ) -> None:
        self._channel = channel
        self._cache = cache
        self._news_subscribe_channel = news_subscribe_channel
        self._news_unsubscribe_channel = news_unsubscribe_channel
        self._fetch_interval = fetch_interval_seconds
        self._jitter_max = jitter_max_seconds
        self._symbols: set[str] = set()
        self._subscribers: dict[str, str] = {}  # subscriber_id → symbol
        self._indicator = InNewsIndicator()      # used only for _has_news_today
        self._loop_task: asyncio.Task | None = None

    async def start(self) -> None:
        await self._channel.subscribe(
            self._news_subscribe_channel, _SubscribeHandler(self)
        )
        await self._channel.subscribe(
            self._news_unsubscribe_channel, _UnsubscribeHandler(self)
        )
        self._loop_task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._loop_task:
            self._loop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._loop_task

    def _add_symbol(self, subscriber_id: str, sym: str) -> None:
        self._subscribers[subscriber_id] = sym
        self._symbols.add(sym)

    def _remove_symbol(self, subscriber_id: str) -> None:
        sym = self._subscribers.pop(subscriber_id, None)
        if sym is None:
            return
        if sym not in self._subscribers.values():
            self._symbols.discard(sym)

    async def _run(self) -> None:
        while True:
            now = ClockRegistry.clock.now()
            if now.time() < time(4, 0) or now.time() > time(20, 1):
                next_pm = ClockRegistry.clock.next_datetime_at(time(4, 0))
                logger.info(
                    "News cache: outside market hours, sleeping until %s", next_pm
                )
                await asyncio.sleep((next_pm - now).total_seconds())
                continue

            await asyncio.gather(
                *(self._fetch_one(sym) for sym in sorted(self._symbols)),
                return_exceptions=True,
            )

            now = ClockRegistry.clock.now()
            next_tick = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
            await asyncio.sleep((next_tick - now).total_seconds())

    async def _fetch_one(self, symbol: str) -> None:
        await asyncio.sleep(random.uniform(0, self._jitter_max))
        try:
            in_news = await self._indicator._has_news_today(symbol)
            await self._cache.save(
                InNewsIndicator._cache_key(symbol), "true" if in_news else "false"
            )
            logger.info("[news_cache] %s → in_news=%s", symbol, in_news)
        except Exception:
            logger.exception("[news_cache] fetch failed for %s", symbol)


class _SubscribeHandler:
    def __init__(self, service: NewsCacheService) -> None:
        self._service = service

    async def handle(self, channel_id: str, data: dict) -> None:
        indicator_types: list[str] = data.get("indicator_types", [])
        if "in_news" not in indicator_types:
            return
        subscriber_id: str = data.get("subscriber_id", "")
        symbol: str = data.get("symbol", "")
        if subscriber_id and symbol:
            self._service._add_symbol(subscriber_id, symbol)
            logger.info("[news_cache] subscribed %s for symbol %s", subscriber_id, symbol)

    def id(self) -> str:
        return "news_cache_subscribe"


class _UnsubscribeHandler:
    def __init__(self, service: NewsCacheService) -> None:
        self._service = service

    async def handle(self, channel_id: str, data: dict) -> None:
        subscriber_id: str = data.get("subscriber_id", "")
        if subscriber_id:
            self._service._remove_symbol(subscriber_id)
            logger.info("[news_cache] unsubscribed %s", subscriber_id)

    def id(self) -> str:
        return "news_cache_unsubscribe"
