import asyncio
import logging

import redis.asyncio as aioredis
from redis import RedisError

from fastscanner.adapters.cmd.run_daily_collection import collect_daily_data
from fastscanner.pkg import config
from fastscanner.pkg.logging import load_logging_config

load_logging_config()
logger = logging.getLogger(__name__)


async def redis_cleanup() -> None:
    """Clear all Redis streams to prepare for fresh data."""
    redis_client = None
    try:
        redis_client = aioredis.Redis(
            unix_socket_path=config.UNIX_SOCKET_PATH,
            password=None,
            db=0,
            decode_responses=True,
        )

        await redis_client.flushdb(asynchronous=True)
        logger.info("Successfully cleared all Redis data in database 0")

    except RedisError as e:
        logger.error(f"Redis error during cleanup: {e}", exc_info=True)
        raise
    finally:
        if redis_client:
            await redis_client.aclose()
            logger.info("Redis connection closed")


async def daily_cleanup_job() -> None:

    logger.info("Starting daily cleanup job")
    start_time = asyncio.get_event_loop().time()

    try:
        logger.info("Step 1: Clearing Redis streams")
        await redis_cleanup()

        logger.info("Step 2: Starting daily data collection")
        await collect_daily_data()

        end_time = asyncio.get_event_loop().time()
        duration = end_time - start_time
        logger.info(
            f"Daily cleanup job completed successfully in {duration:.2f} seconds"
        )

    except Exception as e:
        logger.error(f"Daily cleanup job failed: {e}", exc_info=True)
        raise


def main() -> None:
    try:
        asyncio.run(daily_cleanup_job())
    except Exception as e:
        logger.error(f"Failed to run daily cleanup job: {e}")
        exit(1)


if __name__ == "__main__":
    main()
