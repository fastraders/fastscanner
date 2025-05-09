import asyncio
import json
import logging
import os
import time
import traceback
from datetime import datetime
from functools import wraps
from types import MethodType

from fastscanner.adapters.realtime.polygon_realtime import PolygonRealtime
from fastscanner.adapters.realtime.redis_channel import RedisChannel
from fastscanner.pkg import config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

NO_DATA_TIMEOUT = 10
SYMBOLS_FILE = "data/symbols/symbols.json"


class BenchmarkStats:
    def __init__(self):
        self.latencies = []
        self.total_messages = 0
        self.batch_start_time: datetime | None = None
        self.last_received_time: datetime | None = None
        self.lock = asyncio.Lock()

    async def record(self, message_count: int, latency: float):
        now = datetime.now()
        if self.batch_start_time is None:
            self.batch_start_time = now
        self.last_received_time = now
        self.latencies.append(latency)
        self.total_messages += message_count

    async def check_timeout(self):
        now = datetime.now()
        if (
            self.batch_start_time
            and self.last_received_time
            and (now - self.last_received_time).total_seconds() > NO_DATA_TIMEOUT
        ):
            logger.info(
                f"\nBatch started at: {self.batch_start_time.strftime('%M:%S.%f')}"
            )
            logger.info(f"Batch ended at:   {now.strftime('%M:%S.%f')}")
            logger.info(
                f"Batch duration: {(now - self.batch_start_time).total_seconds():.6f} seconds\n"
            )
            self.batch_start_time = None
            self.last_received_time = None
            self.total_messages = 0
            self.latencies.clear()

    def report(self):
        if not self.latencies:
            logger.info("\n--- Benchmark Report ---")
            logger.info("No data received yet.")
            return

        avg_latency = sum(self.latencies) / len(self.latencies)
        max_latency = max(self.latencies)

        logger.info("\n--- Benchmark Report ---")
        logger.info(f"Handle Calls       : {len(self.latencies)}")
        logger.info(f"Total Messages     : {self.total_messages}")
        logger.info(f"Avg Latency        : {avg_latency:.6f}s")
        logger.info(f"Max Latency        : {max_latency:.6f}s")


def wrap_handle_messages(realtime: PolygonRealtime, stats: BenchmarkStats):
    original_handle = realtime.handle_messages

    @wraps(original_handle)
    async def benchmarked_handle(self, msgs):
        start = time.perf_counter()
        await original_handle(msgs)
        end = time.perf_counter()
        await stats.record(len(msgs), end - start)

    realtime.handle_messages = MethodType(benchmarked_handle, realtime)


async def monitor_inactivity(stats: BenchmarkStats):
    while True:
        await asyncio.sleep(1)
        await stats.check_timeout()


def load_symbols() -> set[str]:
    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE, "r") as f:
            symbols = json.load(f)
            logger.info(f"Loaded {len(symbols)} symbols from file.")
            return set(symbols)
    else:
        logger.warning("No symbols.json found.")
        return set()


async def main():
    stats = BenchmarkStats()

    try:
        redis_channel = RedisChannel(
            unix_socket_path=config.UNIX_SOCKET_PATH,
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
        await asyncio.sleep(3)
        symbols = load_symbols()
        await realtime.subscribe(symbols)

        logger.info("Benchmark running...\n")

        asyncio.create_task(monitor_inactivity(stats))

        while True:
            await asyncio.sleep(10)
            stats.report()

    except Exception as e:
        logger.error(f"Error in benchmark: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(main())
