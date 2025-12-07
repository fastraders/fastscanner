import asyncio
import logging
import traceback

import uvloop

from fastscanner.adapters.realtime.nats_channel import NATSChannel
from fastscanner.adapters.realtime.polygon_realtime import PolygonRealtime
from fastscanner.pkg import config
from fastscanner.pkg.logging import load_logging_config

load_logging_config()
logger = logging.getLogger(__name__)


class SymbolSubscriberManager:
    def __init__(self, polygon: PolygonRealtime) -> None:
        self._polygon = polygon
        self._symbol_to_subscribers: dict[str, set[str]] = {}
        self._lock = asyncio.Lock()

    async def subscribe(self, symbol: str, subscriber_id: str, unit: str) -> None:
        async with self._lock:
            if symbol not in self._symbol_to_subscribers:
                logger.info(
                    f"Sending subscribe request for symbol {symbol} to Polygon."
                )
                self._symbol_to_subscribers[symbol] = {subscriber_id}
                unit_to_func = {
                    "s": self._polygon.subscribe_s,
                    "min": self._polygon.subscribe_min,
                }
                await unit_to_func[unit](set([symbol]))
                logger.info(f"Subscribed to symbol {symbol} successfully.")
                return
            self._symbol_to_subscribers[symbol].add(subscriber_id)

    async def unsubscribe(self, symbol: str, subscriber_id: str, unit: str) -> None:
        async with self._lock:
            if symbol not in self._symbol_to_subscribers:
                return
            self._symbol_to_subscribers[symbol].discard(subscriber_id)
            if not self._symbol_to_subscribers[symbol]:
                logger.info(f"No more subscribers for symbol {symbol}, unsubscribing.")
                unit_to_func = {
                    "s": self._polygon.unsubscribe_s,
                    "min": self._polygon.unsubscribe_min,
                }
                del self._symbol_to_subscribers[symbol]
                await unit_to_func[unit](set([symbol]))


class SymbolSubscriber:
    def __init__(self, manager: SymbolSubscriberManager) -> None:
        self._manager = manager

    async def handle(self, channel_id: str, data: dict):
        if "symbol" not in data or "subscriber_id" not in data:
            logger.error(f"Invalid subscribe message data {data}")
            return
        symbol = data["symbol"]
        subscriber_id = data["subscriber_id"]
        unit = data["unit"]
        logger.info(
            f"Subscribing to symbol: {symbol} with subscriber ID: {subscriber_id} and unit: {unit}"
        )
        await self._manager.subscribe(symbol, subscriber_id, unit)

    def id(self) -> str:
        return "symbol_subscriber"


class SymbolUnsubscriber:
    def __init__(self, manager: SymbolSubscriberManager) -> None:
        self._manager = manager

    async def handle(self, channel_id: str, data: dict):
        if "symbol" not in data or "subscriber_id" not in data:
            logger.error(f"Invalid unsubscribe message data {data}")
            return
        symbol = data["symbol"]
        subscriber_id = data["subscriber_id"]
        unit = data["unit"]
        logger.info(f"Unsubscribing from symbol: {symbol}")
        await self._manager.unsubscribe(symbol, subscriber_id, unit)

    def id(self) -> str:
        return "symbol_unsubscriber"


async def main():
    try:
        # channel = RedisChannel(
        #     unix_socket_path=config.UNIX_SOCKET_PATH,
        #     host=config.REDIS_DB_HOST,
        #     port=config.REDIS_DB_PORT,
        #     password=None,
        #     db=0,
        # )
        channel = NATSChannel(servers=config.NATS_SERVER)
        realtime = PolygonRealtime(
            api_key=config.POLYGON_API_KEY,
            channel=channel,
        )
        manager = SymbolSubscriberManager(realtime)
        subscriber = SymbolSubscriber(manager)
        unsubscriber = SymbolUnsubscriber(manager)

        await realtime.start()
        await channel.subscribe(config.NATS_SYMBOL_SUBSCRIBE_CHANNEL, subscriber)
        await channel.subscribe(config.NATS_SYMBOL_UNSUBSCRIBE_CHANNEL, unsubscriber)

        while True:
            await asyncio.sleep(5)

    except Exception as e:
        logger.error(f"Error in main(): {e}")
        logger.error(traceback.format_exc())
    finally:
        await realtime.stop()


if __name__ == "__main__":
    uvloop.run(main())
