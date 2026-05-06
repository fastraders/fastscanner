import logging
from time import perf_counter
from typing import Iterable
from uuid import uuid4

import pandas as pd

from fastscanner.pkg.candle import Candle
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR
from fastscanner.pkg.observability import metrics
from fastscanner.services.indicators.lib import Indicator
from fastscanner.services.indicators.ports import Channel

logger = logging.getLogger(__name__)


class SlowIndicatorsService:
    """Per-symbol candle dispatcher for slow, externally-sourced indicators.

    Each registered indicator is expected to be a `caching=True` instance
    whose `extend_realtime` self-schedules its own external fetch (jitter,
    in-flight dedup, day-boundary, sticky values, cache writes). The service
    only takes responsibility for: tracking who is subscribed to what, and
    for each (symbol, indicator_type) pair with at least one subscriber,
    feeding the per-symbol minute candle stream into `extend_realtime`.
    """

    def __init__(
        self,
        channel: Channel,
        indicators: Iterable[Indicator],
        subscribe_channel: str,
        unsubscribe_channel: str,
    ) -> None:
        self._channel = channel
        self._indicators: dict[str, Indicator] = {
            ind.type(): ind for ind in indicators
        }
        self._subscribe_channel = subscribe_channel
        self._unsubscribe_channel = unsubscribe_channel
        # symbol → indicator_type → set[subscriber_id]
        self._subscribers: dict[str, dict[str, set[str]]] = {}
        # symbol → handler currently bound to candles.min.{symbol}
        self._handlers: dict[str, "_SlowIndicatorCandleHandler"] = {}

    async def start(self) -> None:
        await self._channel.subscribe(
            self._subscribe_channel, _SubscribeHandler(self)
        )
        await self._channel.subscribe(
            self._unsubscribe_channel, _UnsubscribeHandler(self)
        )

    async def stop(self) -> None:
        for symbol, handler in list(self._handlers.items()):
            await self._channel.unsubscribe(
                _candle_stream(symbol), handler.id()
            )
        self._handlers.clear()
        self._subscribers.clear()
        metrics.set_active_subscriptions("slow_indicator_fanout", 0)

    def _bindings_count(self) -> int:
        return sum(len(types) for types in self._subscribers.values())

    async def _add_subscription(
        self,
        subscriber_id: str,
        symbol: str,
        indicator_types: Iterable[str],
    ) -> None:
        types_for_symbol = self._subscribers.setdefault(symbol, {})
        prev_active = set(types_for_symbol.keys())
        for ind_type in indicator_types:
            if ind_type not in self._indicators:
                continue
            types_for_symbol.setdefault(ind_type, set()).add(subscriber_id)
        new_active = set(types_for_symbol.keys())
        if new_active != prev_active:
            await self._sync_handler(symbol)
        metrics.set_active_subscriptions("slow_indicator_fanout", self._bindings_count())

    async def _remove_subscription(self, subscriber_id: str) -> None:
        affected: set[str] = set()
        for symbol, types_dict in list(self._subscribers.items()):
            for ind_type, subs in list(types_dict.items()):
                if subscriber_id not in subs:
                    continue
                subs.discard(subscriber_id)
                if not subs:
                    del types_dict[ind_type]
                affected.add(symbol)
            if not types_dict:
                self._subscribers.pop(symbol, None)
        for symbol in affected:
            await self._sync_handler(symbol)
        metrics.set_active_subscriptions("slow_indicator_fanout", self._bindings_count())

    async def _sync_handler(self, symbol: str) -> None:
        active_types = sorted(self._subscribers.get(symbol, {}).keys())
        existing = self._handlers.get(symbol)
        if existing is not None:
            await self._channel.unsubscribe(
                _candle_stream(symbol), existing.id()
            )
            del self._handlers[symbol]
        if not active_types:
            return
        indicators = [self._indicators[t] for t in active_types]
        handler = _SlowIndicatorCandleHandler(symbol, indicators)
        self._handlers[symbol] = handler
        await self._channel.subscribe(_candle_stream(symbol), handler)
        logger.info(
            "[slow_indicators] %s subscribed to candles for types=%s",
            symbol,
            active_types,
        )


def _candle_stream(symbol: str) -> str:
    return f"candles.min.{symbol}"


class _SlowIndicatorCandleHandler:
    def __init__(self, symbol: str, indicators: list[Indicator]) -> None:
        self._id = str(uuid4())
        self._symbol = symbol
        self._indicators = indicators

    def id(self) -> str:
        return self._id

    async def handle(self, channel_id: str, data: dict) -> None:
        ts = pd.to_datetime(int(data["timestamp"]), unit="ms", utc=True).tz_convert(
            LOCAL_TIMEZONE_STR
        )
        new_row = Candle(data, timestamp=ts)
        for ind in self._indicators:
            start = perf_counter()
            try:
                await ind.extend_realtime(self._symbol, new_row)
            except Exception:
                logger.exception(
                    "[slow_indicators] %s.extend_realtime failed for %s",
                    ind.type(),
                    self._symbol,
                )
            finally:
                metrics.indicator_extend_latency(
                    type(ind).__name__, perf_counter() - start
                )


class _SubscribeHandler:
    def __init__(self, service: SlowIndicatorsService) -> None:
        self._service = service

    def id(self) -> str:
        return "slow_indicators_subscribe"

    async def handle(self, channel_id: str, data: dict) -> None:
        indicator_types: list[str] = data.get("indicator_types", [])
        subscriber_id: str = data.get("subscriber_id", "")
        symbol: str = data.get("symbol", "")
        if not (subscriber_id and symbol and indicator_types):
            return
        await self._service._add_subscription(
            subscriber_id, symbol, indicator_types
        )


class _UnsubscribeHandler:
    def __init__(self, service: SlowIndicatorsService) -> None:
        self._service = service

    def id(self) -> str:
        return "slow_indicators_unsubscribe"

    async def handle(self, channel_id: str, data: dict) -> None:
        subscriber_id: str = data.get("subscriber_id", "")
        if not subscriber_id:
            return
        await self._service._remove_subscription(subscriber_id)
