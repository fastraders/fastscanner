import asyncio
import logging
import traceback

import uvloop
from polygon import WebSocketClient
from polygon.websocket.models import EquityAgg, Feed, Market, WebSocketMessage
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
        self._symbols: set[str] = set()
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
        )
        self._running = True
        logger.info("Connecting WebSocket")

        self._ws_task = asyncio.create_task(self._client.connect(self.handle_messages))

    async def stop(self):
        if not self._running:
            logger.warning("WebSocket is not running.")
            return
        await self.unsubscribe(self._symbols)
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

    async def subscribe(self, symbols: list[str]):
        if not self._running:
            logger.warning("WebSocket is not running")
            return
        if self._client is None:
            raise RuntimeError("WebSocketClient is None during subscribe.")

        if not symbols:
            logger.warning("No symbols to subscribe.")
            return

        tickers = [f"AM.{symbol}" for symbol in symbols]
        self._client.subscribe(*tickers)
        self._symbols.update(symbols)

    async def unsubscribe(self, symbols: set[str]):
        if not self._running:
            logger.warning("WebSocket is not running.")
            return

        if self._client is None:
            raise RuntimeError("WebSocketClient is None during unsubscribe.")

        if not symbols:
            logger.warning("No symbols to unsubscribe.")
            return

        tickers = [f"AM.{symbol}" for symbol in symbols]
        try:
            self._client.unsubscribe(*tickers)
            self._symbols.difference_update(symbols)
            logger.info(f"Unsubscribed from: {tickers}")
        except ConnectionClosedError as e:
            logger.warning(f"WebSocket connection was already closed: {e}")

    async def handle_messages(self, msgs: list[WebSocketMessage]):
        parsed_msgs: list[dict] = []
        try:
            for msg in msgs:
                if not isinstance(msg, EquityAgg):
                    logger.warning("Received unexpected message %s", str(msg))
                    continue
                if msg.start_timestamp is None:
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
                channel_id = f"candles_min_{msg.symbol}"
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
        await realtime.subscribe(["AAPL", "MSFT", "GOOGL"])
        await asyncio.sleep(300)
        await realtime.unsubscribe({"MSFT", "GOOGL"})
        await realtime.stop()

    except Exception as e:
        logger.error(f"Error in main(): {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    uvloop.run(main())
