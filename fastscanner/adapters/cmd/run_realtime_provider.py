import asyncio
import logging
import traceback

import pandas as pd
import redis.asyncio as aioredis
import uvloop

from fastscanner.adapters.realtime.polygon_realtime import PolygonRealtime
from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


async def main():
    redis = None
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
        await realtime.subscribe(["*"])

        while True:
            await asyncio.sleep(5)

    except Exception as e:
        logger.error(f"Error in main(): {e}")
        logger.error(traceback.format_exc())
    finally:
        if redis:
            await redis.close()


if __name__ == "__main__":
    uvloop.run(main())
