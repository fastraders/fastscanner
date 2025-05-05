import asyncio
import logging
import time

import redis.asyncio as aioredis

from fastscanner.pkg import config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


async def fetch_latest_for_symbol(redis: aioredis.Redis, symbol: str):
    stream_key = f"candles_min_{symbol}"
    start_time = time.perf_counter()

    print(f"\nFetching latest record for symbol: {symbol}")
    try:
        entries = await redis.xrevrange(stream_key, count=1)

        if not entries:
            print(f"[{symbol}] No records found.")
            return 0, 0

        entry_id, data = entries[0]
        elapsed = time.perf_counter() - start_time
        print(f"[{symbol}] Latest record ID: {entry_id}, Data: {data}")
        return 1, elapsed

    except Exception as e:
        logger.error(f"Error fetching latest data for {symbol}: {e}")
        return 0, 0


async def main():
    symbols = ["AAPL", "MSFT", "GOOGL"]
    # redis = aioredis.Redis(
    #     host=config.REDIS_DB_HOST,
    #     port=config.REDIS_DB_PORT,
    #     password=None,
    #     db=0,
    #     decode_responses=True,
    # )
    redis = aioredis.Redis(
        unix_socket_path="/run/redis/redis-server.sock",
        password=None,
        db=0,
        decode_responses=True,
    )
    try:
        tasks = [fetch_latest_for_symbol(redis, symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks)

        print("\nSummary")
        for symbol, (count, duration) in zip(symbols, results):
            print(f"[{symbol}] {count} records in {duration:.6f}s")

    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
