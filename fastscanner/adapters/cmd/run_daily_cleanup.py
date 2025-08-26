import asyncio
import logging
import time

import redis.asyncio as aioredis
from redis import RedisError

from fastscanner.adapters.cmd.run_daily_collection import collect_daily_data
from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config
from fastscanner.pkg.logging import load_logging_config


async def daily_cleanup_job() -> None:
    logger = logging.getLogger(__name__)

    redis_channel = RedisChannel(
        host=config.REDIS_DB_HOST,
        port=config.REDIS_DB_PORT,
        unix_socket_path=config.UNIX_SOCKET_PATH,
        password=None,
        db=0,
    )
    logger.info("Starting daily cleanup job")
    start_time = time.time()
    await redis_channel.reset()
    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Daily cleanup job completed successfully in {duration:.2f} seconds")


def main() -> None:
    load_logging_config()
    asyncio.run(daily_cleanup_job())


if __name__ == "__main__":
    main()
