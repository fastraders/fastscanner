import asyncio
import logging
import time
import traceback

from fastscanner.adapters.realtime.polygon_realtime import PolygonRealtime
from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class BenchmarkStats:
    def __init__(self):
        self.total_records = 0
        self.total_flushes = 0
        self.latencies = []

    def record_flush(self, record_count: int, latency: float):
        self.total_records += record_count
        self.total_flushes += 1
        self.latencies.append(latency)

    def report(self):
        avg_latency = sum(self.latencies) / len(self.latencies) if self.latencies else 0
        max_latency = max(self.latencies) if self.latencies else 0
        print("\n--- Benchmark Report ---")
        print(f"Total Records Pushed: {self.total_records}")
        print(f"Total Flushes: {self.total_flushes}")
        print(f"Avg Flush Latency: {avg_latency:.6f}s")
        print(f"Max Flush Latency: {max_latency:.6f}s")


def wrap_flush(redis_channel: RedisChannel, stats: BenchmarkStats):
    original_flush = redis_channel.flush

    async def benchmarked_flush():
        start = time.perf_counter()
        pipeline = redis_channel._pipeline
        record_count = 0

        if pipeline:
            record_count = len(pipeline.command_stack)

        await original_flush()
        end = time.perf_counter()

        stats.record_flush(record_count, end - start)

    redis_channel.flush = benchmarked_flush


async def main():
    stats = BenchmarkStats()

    try:
        redis_channel = RedisChannel(
            host=config.REDIS_DB_HOST,
            port=config.REDIS_DB_PORT,
            password=None,
            db=0,
        )

        wrap_flush(redis_channel, stats)

        realtime = PolygonRealtime(
            api_key=config.POLYGON_API_KEY,
            channel=redis_channel,
        )

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
