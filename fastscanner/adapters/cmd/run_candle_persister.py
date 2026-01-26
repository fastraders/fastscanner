import asyncio
import logging
import os
import random
from collections import defaultdict
from datetime import date
from uuid import uuid4

import pandas as pd
import uvloop

from fastscanner.adapters.realtime.nats_channel import NATSChannel
from fastscanner.client.candle import CandleClient, CandleMessage
from fastscanner.pkg import config
from fastscanner.pkg.clock import LOCAL_TIMEZONE
from fastscanner.pkg.logging import load_logging_config
from fastscanner.services.indicators.ports import CandleCol

load_logging_config()
logger = logging.getLogger(__name__)


class CandlePersister:
    REALTIME_DIR = os.path.join(config.DATA_BASE_DIR, "data", "realtime")

    def __init__(self, client: CandleClient) -> None:
        self._client = client
        self._candle_buffers: dict[tuple[str, str], list[dict]] = defaultdict(list)
        self._buffer_dates: dict[tuple[str, str], date | None] = {}
        self._id_suffix = str(uuid4())[:8]

    def _subscription_id(self, symbol: str, freq: str) -> str:
        return (
            f"{config.PERSISTER_SUBSCRIPTION_PREFIX}{symbol}_{freq}_{self._id_suffix}"
        )

    @staticmethod
    def _parse_subscription_id(subscription_id: str) -> tuple[str, str]:
        parts = subscription_id.split("_")
        if len(parts) < 4 or not subscription_id.startswith(
            config.PERSISTER_SUBSCRIPTION_PREFIX
        ):
            raise ValueError(f"Invalid subscription_id format: {subscription_id}")
        symbol = parts[1]
        freq = parts[2]
        return symbol, freq

    async def handle_candle(self, message: CandleMessage):
        symbol, freq = self._parse_subscription_id(message.subscription_id)
        dt = message.timestamp
        candle_date = dt.date()
        candle_data = message.candle

        candle = {
            CandleCol.DATETIME: pd.Timestamp(
                dt.timestamp(), unit="s", tz=LOCAL_TIMEZONE
            ),
            CandleCol.OPEN: candle_data["open"],
            CandleCol.HIGH: candle_data["high"],
            CandleCol.LOW: candle_data["low"],
            CandleCol.CLOSE: candle_data["close"],
            CandleCol.VOLUME: candle_data["volume"],
        }

        key = (symbol, freq)
        current_buffer_date = self._buffer_dates.get(key)

        if current_buffer_date is not None and current_buffer_date != candle_date:
            await self._flush_buffer(symbol, freq)

        self._buffer_dates[key] = candle_date
        self._candle_buffers[key].append(candle)

        if len(self._candle_buffers[key]) >= 100:
            await self._flush_buffer(symbol, freq)

    async def subscribe(self, symbol: str, freq: str) -> None:
        subscription_id = self._subscription_id(symbol, freq)
        await self._client.subscribe(
            subscription_id=subscription_id,
            symbol=symbol,
            freq=freq,
            indicators=[],
            handler=self.handle_candle,
        )
        logger.info(f"Subscribed to candles for {symbol} ({freq})")

    async def unsubscribe(self, symbol: str, freq: str) -> None:
        subscription_id = self._subscription_id(symbol, freq)
        await self._client.unsubscribe(subscription_id)
        await self._flush_buffer(symbol, freq)
        logger.info(f"Unsubscribed from candles for {symbol} ({freq})")

    async def _flush_buffer(self, symbol: str, freq: str):
        key = (symbol, freq)
        if not self._candle_buffers[key]:
            return

        candles = self._candle_buffers[key]
        self._candle_buffers[key] = []
        self._buffer_dates[key] = None

        df = pd.DataFrame(candles)
        if df.empty:
            return

        df = df.set_index(CandleCol.DATETIME)

        candle_date = df.index[0].date()
        date_str = candle_date.strftime("%Y-%m-%d")
        csv_path = os.path.join(self.REALTIME_DIR, symbol, freq, f"{date_str}.csv")
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)

        if os.path.exists(csv_path):
            existing_df = pd.read_csv(csv_path)
            existing_df[CandleCol.DATETIME] = pd.to_datetime(
                existing_df[CandleCol.DATETIME], utc=True
            ).dt.tz_convert(LOCAL_TIMEZONE)
            existing_df = existing_df.set_index(CandleCol.DATETIME)

            df = pd.concat([existing_df, df])

        save_df = df.reset_index()
        save_df[CandleCol.DATETIME] = (
            save_df[CandleCol.DATETIME]
            .dt.tz_convert("utc")  # type: ignore
            .dt.strftime("%Y-%m-%d %H:%M:%S")
        )
        save_df.to_csv(csv_path, index=False)

        # Log occasionally to avoid log flooding
        if random.random() < 0.05:
            logger.info(
                f"Persisted {len(df)} candles for {symbol} ({freq}) on {date_str}"
            )

    async def periodic_flush(self):
        while True:
            await asyncio.sleep(60)
            for (symbol, freq), candles in list(self._candle_buffers.items()):
                if len(candles) > 0:
                    await self._flush_buffer(symbol, freq)


class CandlePersistenceManager:
    UNIT_TO_FREQS: dict[str, list[str]] = {
        "s": [],
        "min": ["1min"],
    }

    def __init__(self, persister: CandlePersister) -> None:
        self._persister = persister
        self._unit_to_symbol_to_subscribers: dict[str, dict[str, set[str]]] = {
            "s": {},
            "min": {},
        }
        self._lock = asyncio.Lock()

    async def subscribe(self, symbol: str, subscriber_id: str, unit: str) -> None:
        async with self._lock:
            if symbol not in self._unit_to_symbol_to_subscribers[unit]:
                freqs = self.UNIT_TO_FREQS[unit]
                for freq in freqs:
                    await self._persister.subscribe(symbol, freq)
                self._unit_to_symbol_to_subscribers[unit][symbol] = {subscriber_id}
                logger.info(
                    f"First subscriber for {symbol} ({unit}), subscribed to freqs: {freqs}"
                )
                return
            self._unit_to_symbol_to_subscribers[unit][symbol].add(subscriber_id)

    async def unsubscribe(self, symbol: str, subscriber_id: str, unit: str) -> None:
        async with self._lock:
            if symbol not in self._unit_to_symbol_to_subscribers[unit]:
                return
            self._unit_to_symbol_to_subscribers[unit][symbol].discard(subscriber_id)
            freqs = self.UNIT_TO_FREQS[unit]
            subscribers = self._unit_to_symbol_to_subscribers[unit][symbol]
            logger.info(
                f"Unsubscribed subscriber {subscriber_id} from {symbol} ({unit}). Remaining subscribers: {subscribers}."
            )
            if len(subscribers) == 0:
                del self._unit_to_symbol_to_subscribers[unit][symbol]
                logger.info(
                    f"No more subscribers for {symbol} ({unit}), unsubscribing from freqs: {freqs}"
                )
                for freq in freqs:
                    await self._persister.unsubscribe(symbol, freq)


class SubscribeHandler:
    def __init__(self, manager: CandlePersistenceManager) -> None:
        self._manager = manager

    async def handle(self, channel_id: str, data: dict):
        if "symbol" not in data or "subscriber_id" not in data or "unit" not in data:
            logger.error(f"Invalid subscribe message data {data}")
            return

        symbol = data["symbol"]
        subscriber_id = data["subscriber_id"]
        unit = data["unit"]

        await self._manager.subscribe(symbol, subscriber_id, unit)

    def id(self) -> str:
        return "candle_persister_subscribe"


class UnsubscribeHandler:
    def __init__(self, manager: CandlePersistenceManager) -> None:
        self._manager = manager

    async def handle(self, channel_id: str, data: dict):
        if data.get("symbol") == "__ALL__":
            return

        if "symbol" not in data or "subscriber_id" not in data or "unit" not in data:
            logger.error(f"Invalid unsubscribe message data {data}")
            return

        symbol = data["symbol"]
        subscriber_id = data["subscriber_id"]
        unit = data["unit"]

        await self._manager.unsubscribe(symbol, subscriber_id, unit)

    def id(self) -> str:
        return "candle_persister_unsubscribe"


async def main():
    nats_channel = NATSChannel(servers=config.NATS_SERVER)
    candle_client = CandleClient(host=config.SERVER_HOST, port=config.SERVER_PORT)
    persister = CandlePersister(candle_client)
    manager = CandlePersistenceManager(persister)

    subscribe_handler = SubscribeHandler(manager)
    unsubscribe_handler = UnsubscribeHandler(manager)

    await nats_channel.subscribe(
        config.NATS_SYMBOL_SUBSCRIBE_CHANNEL, subscribe_handler
    )
    await nats_channel.subscribe(
        config.NATS_SYMBOL_UNSUBSCRIBE_CHANNEL, unsubscribe_handler
    )

    flush_task = asyncio.create_task(persister.periodic_flush())
    logger.info("Candle persister started")

    try:
        while True:
            await asyncio.sleep(5)
    finally:
        logger.info("Shutting down candle persister")
        try:
            await nats_channel.unsubscribe(
                config.NATS_SYMBOL_SUBSCRIBE_CHANNEL, subscribe_handler.id()
            )
            await nats_channel.unsubscribe(
                config.NATS_SYMBOL_UNSUBSCRIBE_CHANNEL, unsubscribe_handler.id()
            )
        except Exception as ex:
            logger.exception(ex)
        try:
            flush_task.cancel()
            await flush_task
        except asyncio.CancelledError:
            pass
        await candle_client.stop()


if __name__ == "__main__":
    uvloop.run(main())
