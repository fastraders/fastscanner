import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Any

from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config
from fastscanner.services.indicators.ports import ChannelHandler

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SYMBOLS_FILE = "data/symbols/symbols.json"
STREAM_PREFIX = "candles_min_"
NO_DATA_TIMEOUT = 10

batch_start_time = None
last_received_time = None
total_messages = 0
batch_lock = asyncio.Lock()


async def get_or_load_symbols(redis: RedisChannel) -> list[str]:
    os.makedirs(os.path.dirname(SYMBOLS_FILE), exist_ok=True)

    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE, "r") as f:
            symbols = json.load(f)
            logger.info(f"Loaded {len(symbols)} symbols from file.")
            return symbols

    symbols = []
    async for key in redis.redis.scan_iter(f"{STREAM_PREFIX}*"):
        if await redis.redis.type(key) == "stream":
            symbol = key.replace(STREAM_PREFIX, "")
            symbols.append(symbol)

    with open(SYMBOLS_FILE, "w") as f:
        json.dump(symbols, f)

    logger.info(f"Discovered and saved {len(symbols)} symbols.")
    return symbols


class BenchmarkHandler(ChannelHandler):
    async def handle(self, channel_id: str, data: dict[Any, Any]):
        global batch_start_time, last_received_time, total_messages

        now = time.time()
        ts = float(data.get("timestamp", now))
        logger.info(f"[{channel_id}] Timestamp: {ts}, Data: {data}")

        if batch_start_time is None:
            batch_start_time = now
        last_received_time = now
        total_messages += 1


async def monitor_batch_timeout():
    global batch_start_time, last_received_time, total_messages

    while True:
        await asyncio.sleep(1)
        now = time.time()

        if last_received_time and (now - last_received_time) > NO_DATA_TIMEOUT:
            logger.info(
                f"No messages received for {NO_DATA_TIMEOUT} seconds at: {datetime.fromtimestamp(now).strftime('%M:%S.%f')[:-3]}"
            )
            if batch_start_time:
                batch_duration = last_received_time - batch_start_time
                logger.info(f"\nBatch Summary:")
                logger.info(
                    f"First message timestamp: {datetime.fromtimestamp(batch_start_time).strftime('%H:%M:%S.%f')[:-3]}"
                )
                logger.info(
                    f"Last message timestamp:  {datetime.fromtimestamp(last_received_time).strftime('%H:%M:%S.%f')[:-3]}"
                )
                logger.info(f"Total messages read: {total_messages}")
                logger.info(f"Batch duration: {batch_duration:.6f} seconds\n")
            batch_start_time = None
            last_received_time = None
            total_messages = 0


async def report_stats():
    global batch_start_time, last_received_time, total_messages

    while True:
        await asyncio.sleep(10)

        logger.info("\n--- Benchmark Report ---")
        if total_messages == 0:
            logger.info("No data received yet.")
            continue

        logger.info(f"Total Messages     : {total_messages}")

        if batch_start_time and last_received_time:
            logger.info(
                f"First message at   : {datetime.fromtimestamp(batch_start_time).strftime('%H:%M:%S.%f')[:-3]}"
            )
            logger.info(
                f"Last message at    : {datetime.fromtimestamp(last_received_time).strftime('%H:%M:%S.%f')[:-3]}"
            )
            logger.info(
                f"Batch duration     : {(last_received_time - batch_start_time):.6f}s"
            )


async def main():
    redis_channel = RedisChannel(
        unix_socket_path=config.UNIX_SOCKET_PATH,
        host=config.REDIS_DB_HOST,
        port=config.REDIS_DB_PORT,
        password=None,
        db=0,
    )
    symbols = await get_or_load_symbols(redis_channel)

    logger.info("Read benchmark started...\n")

    for symbol in symbols:
        stream_key = f"{STREAM_PREFIX}{symbol}"
        await redis_channel.subscribe(stream_key, BenchmarkHandler())

    monitor_task = asyncio.create_task(monitor_batch_timeout())
    stats_task = asyncio.create_task(report_stats())

    await asyncio.gather(monitor_task, stats_task)


if __name__ == "__main__":
    asyncio.run(main())
