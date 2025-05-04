import asyncio
import logging
import time

import redis.asyncio as aioredis

from fastscanner.pkg import config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


async def fetch_all_for_symbol(redis: aioredis.Redis, symbol: str):
    stream_key = f"candles_min_{symbol}"
    start_time = time.perf_counter()
    count = 0
    last_id = "0"

    print(f"\nFetching all records for symbol: {symbol}")
    try:
        while True:
            entries = await redis.xread({stream_key: last_id}, count=1000, block=0)
            if not entries:
                break

            for stream_name, stream_entries in entries:
                for entry_id, data in stream_entries:
                    count += 1
                    last_id = entry_id

            if len(stream_entries) < 1000:
                break

        elapsed = time.perf_counter() - start_time
        print(f"[{symbol}] Fetched {count} records in {elapsed:.6f}s")
        return count, elapsed

    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
        return 0, 0


async def main():
    symbols = ["AAPL", "MSFT", "GOOGL"]
    redis = aioredis.Redis(
        host=config.REDIS_DB_HOST,
        port=config.REDIS_DB_PORT,
        password=None,
        db=0,
        decode_responses=True,
    )

    try:
        tasks = [fetch_all_for_symbol(redis, symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks)

        print("\nSummary")
        for symbol, (count, duration) in zip(symbols, results):
            print(f"[{symbol}] {count} records in {duration:.2f}s")

    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
