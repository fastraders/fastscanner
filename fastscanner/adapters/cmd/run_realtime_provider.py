import asyncio
import logging
import traceback

import pandas as pd
import redis.asyncio as aioredis

from fastscanner.adapters.realtime.polygon_realtime import PolygonRealtime
from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


async def print_latest_redis_data(redis, symbol: str):
    stream_key = f"realtime_stream_{symbol}"
    try:
        entries = await redis.xrevrange(stream_key, count=10)
        if not entries:
            print(f"No data found for {symbol}.")
            return

        print(f"Latest data for {symbol}:")
        for entry_id, fields in entries:
            print(fields)

    except Exception as e:
        logger.error(f"Error reading Redis stream for {symbol}: {e}")


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
            api_key=config.POLYGON_API_KEY,
            channel=redis_channel,
        )

        redis = aioredis.Redis(
            host=config.REDIS_DB_HOST,
            port=config.REDIS_DB_PORT,
            db=0,
            password=None,
            decode_responses=True,
        )

        await realtime.start()
        await realtime.subscribe(["AAPL", "MSFT", "GOOGL"])

        await asyncio.sleep(5)

        for _ in range(10):
            await print_latest_redis_data(redis, "AAPL")
            await asyncio.sleep(5)
        await redis.close()

    except Exception as e:
        logger.error(f"Error in main(): {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(main())
