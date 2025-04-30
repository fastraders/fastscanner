import asyncio
import logging
import time
import traceback

import pandas as pd
import redis.asyncio as aioredis

from fastscanner.adapters.realtime.polygon_realtime import PolygonRealtime
from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class BenchmarkStats:
    def __init__(self):
        self.total_records = 0
        self.total_batches = 0
        self.latencies = []

    def record_batch(self, count: int, latency: float):
        self.total_records += count
        self.total_batches += 1
        self.latencies.append(latency)

    def report(self):
        avg_latency = sum(self.latencies) / len(self.latencies) if self.latencies else 0
        max_latency = max(self.latencies) if self.latencies else 0
        print("\n Benchmark Report")
        print(f"Total Records Pushed: {self.total_records}")
        print(f"Total Batches: {self.total_batches}")
        print(f"Average Push Latency: {avg_latency:.6f}s")
        print(f"Max Push Latency: {max_latency:.6f}s")


def wrap_push(original_push, stats: BenchmarkStats):
    async def benchmarked_push(self, df: pd.DataFrame):
        start = time.perf_counter()
        await original_push(self, df)
        end = time.perf_counter()
        stats.record_batch(len(df), end - start)

    return benchmarked_push


async def main():
    stats = BenchmarkStats()
    try:
        redis_channel = RedisChannel(
            host=config.REDIS_DB_HOST,
            port=config.REDIS_DB_PORT,
            password=None,
            db=0,
        )

        realtime = PolygonRealtime(
            api_key=config.POLYGON_API_KEY,
            channel=redis_channel,
        )

        PolygonRealtime._push = wrap_push(PolygonRealtime._push, stats)

        await realtime.start()
        await realtime.subscribe({"*"})

        print("Benchmark running")

        while True:
            await asyncio.sleep(10)
            stats.report()

    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Error in benchmark: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(main())
