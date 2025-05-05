import asyncio
import logging
import time
import traceback
from functools import wraps
from types import MethodType

from fastscanner.adapters.realtime.polygon_realtime import PolygonRealtime
from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class BenchmarkStats:
    def __init__(self):
        self.latencies = []

    def record(self, latency: float):
        self.latencies.append(latency)

    def report(self):
        if not self.latencies:
            print("\n--- Benchmark Report ---")
            print("No data received yet.")
            return

        avg_latency = sum(self.latencies) / len(self.latencies)
        max_latency = max(self.latencies)

        print("\n--- Benchmark Report ---")
        print(f"Handle Calls: {len(self.latencies)}")
        print(f"Avg Latency: {avg_latency:.6f}s")
        print(f"Max Latency: {max_latency:.6f}s")


def wrap_handle_messages(realtime: PolygonRealtime, stats: BenchmarkStats):
    original_handle = realtime.handle_messages

    @wraps(original_handle)
    async def benchmarked_handle(self, msgs):
        start = time.perf_counter()
        await original_handle(msgs)
        end = time.perf_counter()
        stats.record(end - start)

    realtime.handle_messages = MethodType(benchmarked_handle, realtime)


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

        wrap_handle_messages(realtime, stats)

        await realtime.start()
        await realtime.subscribe({"AAPL", "MSFT", "GOOGL"})

        print("Benchmark running...\n")
        while True:
            await asyncio.sleep(10)
            stats.report()

    except Exception as e:
        logger.error(f"Error in benchmark: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(main())
