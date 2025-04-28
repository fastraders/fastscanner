import asyncio
import logging
import traceback
from typing import List, Optional, Set

import pandas as pd
from polygon import WebSocketClient
from polygon.websocket.models import Feed, Market, WebSocketMessage

from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PolygonRealtime:
    def __init__(self, api_key: str, channel=None):
        self.api_key = api_key
        self.client: Optional[WebSocketClient] = None
        self._running = False
        self.symbols: Set[str] = set()
        self.channel = channel
        self._ws_task: Optional[asyncio.Task] = None

    async def start(self):
        if self._running:
            logger.warning("WebSocket already running.")
            return
        try:
            self.client = WebSocketClient(
                api_key=self.api_key,
                feed=Feed.RealTime,
                market=Market.Stocks,
            )
            self._running = True
            logger.info("Connecting WebSocket")

            self._ws_task = asyncio.create_task(
                self.client.connect(self.handle_message)
            )
        except Exception as e:
            logger.error(f"Error in start(): {e}")
            logger.error(traceback.format_exc())

    async def stop(self):
        if not self._running:
            logger.warning("WebSocket is not running.")
            return
        if self.client is None:
            logger.warning("Client not initialized. Cannot unsubscribe.")
            return
        try:
            if self.symbols:
                await self.unsubscribe(self.symbols)

            await self.client.close()
            if self._ws_task:
                await self._ws_task

            self._running = False
            logger.info("WebSocket stopped.")
        except Exception as e:
            logger.error(f"Error in stop(): {e}")
            logger.error(traceback.format_exc())

    async def subscribe(self, symbols: Set[str]):
        if not self._running:
            logger.warning("WebSocket is not running")
            return
        if self.client is None:
            logger.warning("Client not initialized. Cannot unsubscribe.")
            return
        try:
            if not symbols:
                logger.warning("No symbols to subscribe.")
                return

            tickers = [f"AM.{symbol}" for symbol in symbols]
            self.client.subscribe(*tickers)
            self.symbols.update(symbols)
            logger.info(f"Subscribed to: {tickers}")
        except Exception as e:
            logger.error(f"Error in subscribe(): {e}")
            logger.error(traceback.format_exc())

    async def unsubscribe(self, symbols: Set[str]):
        if not self._running:
            logger.warning("WebSocket is not running.")
            return
        if self.client is None:
            logger.warning("Client not initialized. Cannot unsubscribe.")
            return
        try:
            if not symbols:
                logger.warning("No symbols to unsubscribe.")
                return

            tickers = [f"AM.{symbol}" for symbol in symbols]
            self.client.unsubscribe(*tickers)
            self.symbols.difference_update(symbols)
            logger.info(f"Unsubscribed from: {tickers}")
        except Exception as e:
            logger.error(f"Error in unsubscribe(): {e}")
            logger.error(traceback.format_exc())

    async def handle_message(self, msgs: List[WebSocketMessage]):
        try:
            logger.info(f"Received messages: {msgs}")

            data = []
            for msg in msgs:
                record = {
                    "symbol": getattr(msg, "symbol", None),
                    "timestamp": getattr(msg, "start_timestamp", None),
                    "open": getattr(msg, "open", None),
                    "high": getattr(msg, "high", None),
                    "low": getattr(msg, "low", None),
                    "close": getattr(msg, "close", None),
                    "volume": getattr(msg, "volume", None),
                }
                data.append(record)

            if data:
                df = pd.DataFrame(data)
                await self._push(df)
            else:
                logger.info("No data records to push.")
        except Exception as e:
            logger.error(f"Error in handle_message(): {e}")
            logger.error(traceback.format_exc())

    async def _push(self, df: pd.DataFrame):
        try:
            logger.info(f"Pushing {len(df)} records to Redis.")
            if self.channel:
                await self.channel.push(df.to_dict(orient="records"))
            else:
                print(df)
        except Exception as e:
            logger.error(f"Error in _push(): {e}")
            logger.error(traceback.format_exc())


async def main():
    try:
        redis_channel = RedisChannel(
            host="localhost", port=6379, stream_key="realtime_stream"
        )

        realtime = PolygonRealtime(
            api_key=config.POLYGON_API_KEY, channel=redis_channel
        )

        await realtime.start()

        await realtime.subscribe({"AAPL", "MSFT", "GOOGL"})

        await asyncio.sleep(300)

        await realtime.unsubscribe({"MSFT", "GOOGL"})

        await realtime.stop()

    except Exception as e:
        logger.error(f"Error in main(): {e}")


if __name__ == "__main__":
    asyncio.run(main())
