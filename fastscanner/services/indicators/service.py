import asyncio
import logging
import time as _time_module
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from time import perf_counter
from typing import Any, Awaitable, Callable, Iterable, Protocol
from uuid import uuid4

import numpy as np
import pandas as pd

from fastscanner.pkg.candle import Candle, CandleBuffer
from fastscanner.pkg.clock import LOCAL_TIMEZONE_STR, ClockRegistry, split_freq
from fastscanner.pkg.observability import metrics
from fastscanner.services.exceptions import UnsubscribeSignal

from .lib import Cacheable, CacheableIndicator, Indicator, IndicatorsLibrary
from .ports import Cache, CandleCol, CandleStore, Channel, FundamentalDataStore


PREWARM_TIME = time(3, 0)
PREWARM_DEFAULT_CONCURRENCY = 50


class SymbolSource(Protocol):
    async def active_symbols(self) -> list[str]: ...


@dataclass
class _PreWarmStats:
    succeeded: int
    failed: int
    elapsed_s: float

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class IndicatorParams:
    type_: str
    params: dict[str, Any]


class IndicatorsService:
    _SLOW_INDICATOR_TYPES: frozenset[str] = frozenset(
        {"news_confidence", "shares_float"}
    )

    def __init__(
        self,
        candles: CandleStore,
        fundamentals: FundamentalDataStore,
        channel: Channel,
        cache: Cache,
        symbols_subscribe_channel: str,
        symbols_unsubscribe_channel: str,
        cache_at_seconds: int,
        symbols_slow_indicators_subscribe_channel: str,
        symbols_slow_indicators_unsubscribe_channel: str,
    ) -> None:
        self.candles = candles
        self.fundamentals = fundamentals
        self.channel = channel
        self._cache = cache
        self._symbols_subscribe_channel = symbols_subscribe_channel
        self._symbols_unsubscribe_channel = symbols_unsubscribe_channel
        self._symbols_slow_indicators_subscribe_channel = (
            symbols_slow_indicators_subscribe_channel
        )
        self._symbols_slow_indicators_unsubscribe_channel = (
            symbols_slow_indicators_unsubscribe_channel
        )
        self._subscription_to_channel: dict[str, str] = {}
        self._slow_indicator_subscriptions: set[str] = set()
        # Cache parameters
        self._cached_indicators: list[CacheableIndicator] = []
        self._cache_at_seconds = cache_at_seconds
        self._caching_task: asyncio.Task[None] | None = None
        # Daily pre-warm state
        self._prewarm_task: asyncio.Task[None] | None = None
        self._prewarm_indicators: list[CacheableIndicator] = []
        self._prewarm_symbols_source: SymbolSource | None = None
        self._prewarm_concurrency: int = PREWARM_DEFAULT_CONCURRENCY
        self._prewarm_at: time = PREWARM_TIME
        self._prewarm_inflight: int = 0

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
        Store the subscription handler in a dictionary.
        Every time we get a new candle, for the symbol, we will first fill the new row with the indicators (extend_realtime).
        Then we will call the handler with the new row.
        The first time you get a subscription to a symbol, you need to subscribe to the channel.

        Return the subscription ID.
        """
        indicator_instances = [
            IndicatorsLibrary.instance().get(i.type_, i.params) for i in indicators
        ]
        await asyncio.gather(
            *(self._load_cached_indicator(i, symbol) for i in indicator_instances)
        )

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
            slow_types = [
                i.type()
                for i in indicator_instances
                if i.type() in self._SLOW_INDICATOR_TYPES
            ]
            if slow_types:
                await self.channel.push(
                    self._symbols_slow_indicators_subscribe_channel,
                    {
                        "symbol": symbol,
                        "subscriber_id": sub_handler.id(),
                        "unit": unit,
                        "indicator_types": slow_types,
                    },
                )
                self._slow_indicator_subscriptions.add(sub_handler.id())
        self._subscription_to_channel[sub_handler.id()] = stream_key
        metrics.set_active_subscriptions(
            "indicator_fanout", len(self._subscription_to_channel)
        )
        # Configures the handler to receive messages from the channel
        await self.channel.subscribe(stream_key, sub_handler)
        return sub_handler.id()

    async def _load_cached_indicator(self, i: Indicator, symbol: str) -> None:
        if not isinstance(i, Cacheable):
            return
        col = i.column_name()
        try:
            await i.load_from_cache(symbol)
            metrics.indicator_cache_load(col, "ok")
        except KeyError:
            metrics.indicator_cache_load(col, "miss")
            logger.debug("Cache miss loading indicator %s for %s", col, symbol)
        except Exception:
            metrics.indicator_cache_load(col, "error")
            logger.exception(
                "Error loading cached indicator %s for %s", col, symbol
            )

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
        metrics.set_active_subscriptions(
            "indicator_fanout", len(self._subscription_to_channel)
        )
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
        scheduled_wake: datetime | None = None
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
                scheduled_wake = None
                continue

            now = ClockRegistry.clock.now()
            if scheduled_wake is not None:
                lag = (now - scheduled_wake).total_seconds()
                metrics.indicator_caching_loop_lag(max(lag, 0.0))

            for indicator in self._cached_indicators:
                col = indicator.column_name()
                save_start = perf_counter()
                try:
                    await indicator.save_to_cache()
                    duration = perf_counter() - save_start
                    metrics.indicator_cache_save(col, "ok")
                    metrics.indicator_cache_save_duration(col, duration)
                    metrics.indicator_cache_last_success(col, _time_module.time())
                except Exception as e:
                    duration = perf_counter() - save_start
                    metrics.indicator_cache_save(col, "error")
                    metrics.indicator_cache_save_duration(col, duration)
                    logger.exception(e)
                    logger.error(f"Error caching indicator {col}")

            next_minute = (now + timedelta(minutes=1)).replace(
                second=self._cache_at_seconds, microsecond=0
            )
            scheduled_wake = next_minute
            await asyncio.sleep((next_minute - now).total_seconds())

    async def start_daily_prewarm(
        self,
        symbols_source: SymbolSource,
        indicators: Iterable[CacheableIndicator],
        concurrency: int = PREWARM_DEFAULT_CONCURRENCY,
        run_at: time = PREWARM_TIME,
    ) -> None:
        """Start the daily pre-warm scheduler.

        Indicators passed here MUST be safe with a NaN OHLCV candle: their
        extend_realtime cold-path may read only the candle's timestamp.

        On startup, if the per-day completion marker is absent, runs once
        immediately. Otherwise sleeps until the next scheduled time.
        """
        if self._prewarm_task is not None:
            return
        self._prewarm_symbols_source = symbols_source
        self._prewarm_indicators = list(indicators)
        self._prewarm_concurrency = concurrency
        self._prewarm_at = run_at
        self._prewarm_task = asyncio.create_task(self._prewarm_loop())

    async def stop_daily_prewarm(self) -> None:
        if self._prewarm_task is None:
            return
        self._prewarm_task.cancel()
        try:
            await self._prewarm_task
        except asyncio.CancelledError:
            pass
        self._prewarm_task = None
        self._prewarm_symbols_source = None
        self._prewarm_indicators = []

    async def _prewarm_loop(self) -> None:
        try:
            if not await self._is_prewarmed_today():
                await self._run_prewarm_once()
            while True:
                now = ClockRegistry.clock.now()
                next_run = ClockRegistry.clock.next_datetime_at(self._prewarm_at)
                await asyncio.sleep((next_run - now).total_seconds())
                await self._run_prewarm_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[daily_prewarm] loop crashed")
            raise

    async def _is_prewarmed_today(self) -> bool:
        today = ClockRegistry.clock.today()
        try:
            raw = await self._cache.get(_prewarm_marker_key(today))
        except KeyError:
            return False
        return bool(raw)

    async def _run_prewarm_once(self) -> "_PreWarmStats":
        if self._prewarm_symbols_source is None:
            raise RuntimeError("prewarm symbols_source not set")

        start_ts = ClockRegistry.clock.now()
        today = start_ts.date()
        symbols = await self._prewarm_symbols_source.active_symbols()
        logger.info(
            "[daily_prewarm] start: %d symbols x %d indicators @ %s",
            len(symbols),
            len(self._prewarm_indicators),
            today.isoformat(),
        )

        fake_candle = _build_fake_prewarm_candle(today, self._prewarm_at)
        sem = asyncio.Semaphore(self._prewarm_concurrency)
        self._prewarm_inflight = 0
        metrics.indicator_prewarm_inflight_set(0)

        async def _one(symbol: str, indicator: CacheableIndicator) -> bool:
            async with sem:
                self._prewarm_inflight += 1
                metrics.indicator_prewarm_inflight_set(self._prewarm_inflight)
                try:
                    await indicator.extend_realtime(symbol, fake_candle.copy())
                    metrics.indicator_prewarm_symbol("ok")
                    return True
                except Exception:
                    logger.exception(
                        "[daily_prewarm] %s.%s failed for %s",
                        type(indicator).__name__,
                        indicator.column_name(),
                        symbol,
                    )
                    metrics.indicator_prewarm_symbol("error")
                    return False
                finally:
                    self._prewarm_inflight -= 1
                    metrics.indicator_prewarm_inflight_set(self._prewarm_inflight)

        tasks = [
            _one(symbol, indicator)
            for symbol in symbols
            for indicator in self._prewarm_indicators
        ]
        results = await asyncio.gather(*tasks)
        succeeded = sum(results)
        failed = len(results) - succeeded
        metrics.indicator_prewarm_failed_symbols(failed)

        for indicator in self._prewarm_indicators:
            try:
                await indicator.save_to_cache()
            except Exception:
                logger.exception(
                    "[daily_prewarm] save_to_cache failed for %s",
                    indicator.column_name(),
                )
                # Skip marker so the next start re-runs.
                elapsed = (ClockRegistry.clock.now() - start_ts).total_seconds()
                metrics.indicator_prewarm_run("error", elapsed)
                logger.error(
                    "[daily_prewarm] aborted before marker: succeeded=%d failed=%d elapsed=%.2fs",
                    succeeded,
                    failed,
                    elapsed,
                )
                return _PreWarmStats(succeeded, failed, elapsed)

        await self._cache.save(_prewarm_marker_key(today), "1")
        elapsed = (ClockRegistry.clock.now() - start_ts).total_seconds()
        outcome = "partial" if failed else "ok"
        metrics.indicator_prewarm_run(outcome, elapsed)
        metrics.indicator_prewarm_last_success(_time_module.time())
        logger.info(
            "[daily_prewarm] done: succeeded=%d failed=%d elapsed=%.2fs outcome=%s",
            succeeded,
            failed,
            elapsed,
            outcome,
        )
        return _PreWarmStats(succeeded, failed, elapsed)

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
            if subscription_id in self._slow_indicator_subscriptions:
                await self.channel.push(
                    self._symbols_slow_indicators_unsubscribe_channel,
                    {
                        "symbol": symbol,
                        "subscriber_id": subscription_id,
                        "unit": unit,
                    },
                )
                self._slow_indicator_subscriptions.discard(subscription_id)
        await self.channel.unsubscribe(stream_key, subscription_id)
        self._subscription_to_channel.pop(subscription_id, None)
        metrics.set_active_subscriptions(
            "indicator_fanout", len(self._subscription_to_channel)
        )

    async def stop(self):
        await self.stop_daily_prewarm()
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
            if sub_id in self._slow_indicator_subscriptions:
                await self.channel.push(
                    self._symbols_slow_indicators_unsubscribe_channel,
                    {
                        "symbol": symbol,
                        "subscriber_id": sub_id,
                        "unit": unit,
                    },
                )
        self._slow_indicator_subscriptions.clear()
        self._subscription_to_channel.clear()
        metrics.set_active_subscriptions("indicator_fanout", 0)


def _prewarm_marker_key(d: date) -> str:
    return f"prewarm:completed:{d.isoformat()}"


def _build_fake_prewarm_candle(today: date, at: time) -> Candle:
    ts = pd.Timestamp(
        year=today.year,
        month=today.month,
        day=today.day,
        hour=at.hour,
        minute=at.minute,
        second=at.second,
        tz=LOCAL_TIMEZONE_STR,
    )
    return Candle(
        {
            CandleCol.OPEN: np.nan,
            CandleCol.HIGH: np.nan,
            CandleCol.LOW: np.nan,
            CandleCol.CLOSE: np.nan,
            CandleCol.VOLUME: np.nan,
        },
        timestamp=ts,
    )


class SubscriptionHandler(Protocol):
    async def handle(self, symbol: str, new_row: Candle) -> Candle: ...


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

    async def _handle(self, symbol: str, new_row: Candle) -> None:
        for ind in self._indicators:
            start = perf_counter()
            try:
                new_row = await ind.extend_realtime(symbol, new_row)
            except Exception:
                metrics.indicator_extend_error(ind.column_name())
                raise
            metrics.indicator_extend_latency(ind.column_name(), perf_counter() - start)
        try:
            await self._handler.handle(symbol, new_row)
        except UnsubscribeSignal:
            await self._unsubscribe(self._id)
            return

    def _new_buffer(self, symbol: str) -> CandleBuffer:
        async def _handle(new_row: Candle) -> None:
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
        new_row = Candle(data, timestamp=ts)
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

    async def handle(self, symbol: str, new_row: Candle) -> Candle:
        if self._current_minute is None or self._latest_timestamp is None:
            self._current_minute = new_row.timestamp + pd.Timedelta(self._freq)
            self._latest_timestamp = ClockRegistry.clock.now()
            return new_row
        new_minute = new_row.timestamp + pd.Timedelta(self._freq)
        if new_minute > self._current_minute:
            latency = self._latest_timestamp - self._current_minute
            logger.info(
                f"Latency at {self._current_minute.strftime('%H:%M')}: {latency.total_seconds():.2f} seconds"
            )
            self._current_minute = new_minute

        self._latest_timestamp = ClockRegistry.clock.now()
        return new_row
