import asyncio
import logging
import traceback

import uvloop
from massive import WebSocketClient
from massive.websocket.models import (
    EquityAgg,
    EventType,
    Feed,
    Market,
    WebSocketMessage,
)
from websockets import ConnectionClosedError

from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config
from fastscanner.services.indicators.ports import Channel

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PolygonRealtime:
    def __init__(self, api_key: str, channel: Channel):
        self._api_key = api_key
        self._client: WebSocketClient | None = None
        self._running = False
        self._channel = channel
        self._ws_task: asyncio.Task | None = None

    async def start(self):
        if self._running:
            logger.warning("WebSocket already running.")
            return

        self._client = WebSocketClient(
            api_key=self._api_key,
            feed=Feed.RealTime,
            market=Market.Stocks,
            max_reconnects=None,  # Unlimited reconnects.
        )
        self._running = True
        logger.info("Connecting WebSocket")

        self._ws_task = asyncio.create_task(self._client.connect(self.handle_messages))

    async def stop(self):
        if not self._running:
            logger.warning("WebSocket is not running.")
            return
        try:
            if self._client:
                await self._client.close()
            if self._ws_task:
                await self._ws_task
        except ConnectionClosedError as e:
            logger.warning(f"Client was disconnected: {e}")
        finally:
            self._running = False
            logger.info("WebSocket stopped.")

    def subscribe_all(self):
        if not self._running:
            raise RuntimeError("WebSocket is not running")
        if self._client is None:
            raise RuntimeError("WebSocketClient is None during subscribe.")
        self._client.subscribe("AM.*")

    def subscribe_min(self, symbols: set[str]):
        if not self._running:
            raise RuntimeError("WebSocket is not running")
        if self._client is None:
            raise RuntimeError("WebSocketClient is None during subscribe.")

        tickers = [f"AM.{symbol}" for symbol in symbols]
        self._client.subscribe(*tickers)

    def subscribe_s(self, symbols: set[str]):
        if not self._running:
            raise RuntimeError("WebSocket is not running")
        if self._client is None:
            raise RuntimeError("WebSocketClient is None during subscribe.")

        tickers = [f"A.{symbol}" for symbol in symbols]
        self._client.subscribe(*tickers)

    def unsubscribe_min(self, symbols: set[str]):
        if not self._running:
            logger.warning("WebSocket is not running.")
            return

        if self._client is None:
            raise RuntimeError("WebSocketClient is None during unsubscribe.")

        if not symbols:
            logger.warning("No symbols to unsubscribe.")
            return

        tickers = [f"AM.{symbol}" for symbol in symbols]
        self._client.unsubscribe(*tickers)

    def unsubscribe_s(self, symbols: set[str]):
        if not self._running:
            logger.warning("WebSocket is not running.")
            return

        if self._client is None:
            raise RuntimeError("WebSocketClient is None during unsubscribe.")

        if not symbols:
            logger.warning("No symbols to unsubscribe.")
            return

        tickers = [f"A.{symbol}" for symbol in symbols]
        self._client.unsubscribe(*tickers)

    def unsubscribe_all(self):
        if not self._running:
            logger.warning("WebSocket is not running.")
            return

        if self._client is None:
            raise RuntimeError("WebSocketClient is None during unsubscribe.")

        self._client.unsubscribe_all()

    _event_type_to_channel = {
        EventType.EquityAgg: "candles.s.",
        EventType.EquityAggMin: "candles.min.",
        EventType.EquityAgg.value: "candles.s.",
        EventType.EquityAggMin.value: "candles.min.",
    }

    async def handle_messages(self, msgs: list[WebSocketMessage]):
        parsed_msgs: list[dict] = []
        try:
            for msg in msgs:
                if not isinstance(msg, EquityAgg):
                    logger.warning("Received unexpected message %s", str(msg))
                    continue
                if msg.start_timestamp is None:
                    continue

                if msg.event_type not in self._event_type_to_channel:
                    logger.warning(f"Unknown event type: {msg.event_type}")
                    continue

                record = {
                    "timestamp": msg.start_timestamp,
                    "open": msg.open,
                    "high": msg.high,
                    "low": msg.low,
                    "close": msg.close,
                    "volume": msg.volume,
                }
                parsed_msgs.append(record)
                channel_id = (
                    f"{self._event_type_to_channel[msg.event_type]}{msg.symbol}"
                )
                await self._channel.push(channel_id, record, flush=False)

            await self._channel.flush()
        except Exception as e:
            logger.error(traceback.format_exc())


async def main():
    try:
        redis_channel = RedisChannel(
            unix_socket_path=config.UNIX_SOCKET_PATH,
            host=config.REDIS_DB_HOST,
            port=config.REDIS_DB_PORT,
            password=None,
            db=0,
        )

        realtime = PolygonRealtime(
            api_key=config.POLYGON_API_KEY, channel=redis_channel
        )

        await realtime.start()
        realtime.subscribe_min({"AAPL", "MSFT", "GOOGL"})
        await asyncio.sleep(300)
        realtime.unsubscribe_min({"MSFT", "GOOGL"})
        await realtime.stop()

    except Exception as e:
        logger.error(f"Error in main(): {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    uvloop.run(main())
