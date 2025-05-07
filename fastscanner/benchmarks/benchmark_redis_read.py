import asyncio
import json
import logging
import os
import time
from datetime import datetime

import redis.asyncio as aioredis

from fastscanner.pkg import config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SYMBOLS_FILE = "data/symbols/symbols.json"
STREAM_PREFIX = "candles_min_"
NO_DATA_TIMEOUT = 10

batch_start_time = None
last_received_time = None
batch_lock = asyncio.Lock()
total_messages = 0


async def get_or_load_symbols(redis: aioredis.Redis) -> list[str]:
    os.makedirs(os.path.dirname(SYMBOLS_FILE), exist_ok=True)

    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE, "r") as f:
            symbols = json.load(f)
            logger.info(f"Loaded {len(symbols)} symbols from file.")
            return symbols

    symbols = []
    async for key in redis.scan_iter(f"{STREAM_PREFIX}*"):
        if await redis.type(key) == "stream":
            symbol = key.replace(STREAM_PREFIX, "")
            symbols.append(symbol)

    with open(SYMBOLS_FILE, "w") as f:
        json.dump(symbols, f)

    logger.info(f"Discovered and saved {len(symbols)} symbols.")
    return symbols


async def read_stream(redis: aioredis.Redis, symbol: str):
    global batch_start_time, last_received_time, total_messages

    stream_key = f"{STREAM_PREFIX}{symbol}"
    last_id = "$"

    logger.info(f"Listening for stream: {stream_key}")

    try:
        while True:
            entries = await redis.xread({stream_key: last_id}, block=1000, count=100)
            now = time.time()
            if entries:
                for _, stream_entries in entries:
                    for entry_id, data in stream_entries:
                        ts = float(data.get("timestamp", now))
                        logger.info(
                            f"[{symbol}] ID: {entry_id}, Timestamp: {ts}, Data: {data}"
                        )
                        last_id = entry_id

                        async with batch_lock:
                            if batch_start_time is None:
                                batch_start_time = now
                            last_received_time = now
                            total_messages += 1

    except Exception as e:
        logger.error(f"[{symbol}] Error: {e}")


async def monitor_batch_timeout():
    global batch_start_time, last_received_time,total_messages

    while True:
        await asyncio.sleep(1)
        now = time.time()

        async with batch_lock:
            if (
                batch_start_time
                and last_received_time
                and (now - last_received_time) > NO_DATA_TIMEOUT
            ):
                logger.info(
                    f"\nbatch started at: {datetime.fromtimestamp(batch_start_time).strftime('%M:%S.%f')}"
                )
                logger.info(
                    f"batch ended at:   {datetime.fromtimestamp(now).strftime('%M:%S.%f')}"
                )
                logger.info(f"total messages read: {total_messages}")
                logger.info(f"batch duration: {now - batch_start_time:.6f} seconds\n")
                batch_start_time = None
                last_received_time = None
                total_messages=0


async def main():
    redis = aioredis.Redis(
        unix_socket_path="/run/redis/redis-server.sock",
        password=None,
        db=0,
        decode_responses=True,
    )
    # redis = aioredis.Redis(
    #     host=config.REDIS_DB_HOST,
    #     port=config.REDIS_DB_PORT,
    #     password=None,
    #     db=0,
    #     decode_responses=True,
    # )
    symbols = await get_or_load_symbols(redis)

    logger.info("Read benchmark started...\n")
    tasks = [asyncio.create_task(read_stream(redis, symbol)) for symbol in symbols]
    tasks.append(asyncio.create_task(monitor_batch_timeout()))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
