import asyncio
import logging
import time

import redis.asyncio as aioredis
import uvloop
from redis import RedisError

from fastscanner.adapters.cmd.run_daily_collection import collect_daily_data
from fastscanner.adapters.realtime.nats_channel import NATSChannel
from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config
from fastscanner.pkg.logging import load_logging_config


async def daily_cleanup_job() -> None:
    logger = logging.getLogger(__name__)

    channel = NATSChannel(servers=config.NATS_SERVER)
    logger.info("Starting daily cleanup job")
    await channel.push(config.NATS_SYMBOL_UNSUBSCRIBE_CHANNEL, {"symbol": "__ALL__"})
    logger.info("Daily cleanup job completed successfully")


def main() -> None:
    load_logging_config()
    uvloop.run(daily_cleanup_job())


if __name__ == "__main__":
    main()
