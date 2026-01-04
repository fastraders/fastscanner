import asyncio
import logging
import math
import multiprocessing
import time
from datetime import datetime

import pandas as pd

from fastscanner.adapters.candle.massive_adjusted import MassiveAdjustedCollector
from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.pkg import config
from fastscanner.pkg.clock import ClockRegistry, FixedClock, LocalClock
from fastscanner.pkg.logging import load_logging_config

load_logging_config()
logger = logging.getLogger(__name__)


async def _collect(
    symbol: str, candles: PartitionedCSVCandlesProvider, now: datetime
) -> None:
    # for year in range(2025, now.today().year + 1):
    start_year = 2025
    end_year = 2025
    freqs = ["1min", "2min", "1d"]
    for year in range(start_year, end_year + 1):
        await candles.collect(symbol, year, freqs)
        logger.info(f"Collected data for {symbol} in {year}")

    logger.info(f"Finished data collection for {symbol}")


async def _collect_batch(symbols: list[str], now: datetime) -> None:
    ClockRegistry.set(FixedClock(now))
    polygon = PolygonCandlesProvider(
        config.POLYGON_BASE_URL,
        config.POLYGON_API_KEY,
        max_requests_per_sec=10,
        max_concurrent_requests=10,
    )
    candles = PartitionedCSVCandlesProvider(polygon)

    logger.info(f"Collecting data for {len(symbols)} symbols")
    # start_time = time.time()
    # for i, symbol in enumerate(symbols, start=1):
    #     await _collect(symbol, candles)
    #     if i % 20 == 0:
    #         end_time = time.time()
    #         time_left = (end_time - start_time) * (len(symbols) - i) / i
    #         logger.info(f"Estimated remaining time: {time_left:.2f} seconds")

    tasks = [
        asyncio.create_task(_collect(symbol, candles, now), name=symbol)
        for symbol in symbols
    ]
    completed_tasks = 0
    start = time.time()
    failed_symbols: list[str] = []
    while len(tasks) > 0:
        done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if (ex := task.exception()) is not None:
                logger.exception(ex)
                logger.error(f"Error collecting data for {task.get_name()}: {ex}")
                failed_symbols.append(task.get_name())
        completed_tasks += len(done)
        total_time = time.time() - start
        logger.info(
            f"Estimated remaining time: {total_time * len(tasks) / completed_tasks:.2f} seconds"
        )
    logger.info(
        f"Finished collecting data for {len(symbols) - len(failed_symbols)} symbols"
    )
    if failed_symbols:
        logger.error(f"Failed to collect data for {len(failed_symbols)} symbols")
        logger.error(f"Failed symbols: {failed_symbols}")


def _run_batch(batch: tuple[list[str], datetime]) -> None:
    b, now = batch
    asyncio.run(_collect_batch(b, now))


async def run_data_collect():
    now = LocalClock().now()
    ClockRegistry.set(FixedClock(now))
    polygon = PolygonCandlesProvider(config.POLYGON_BASE_URL, config.POLYGON_API_KEY)
    collector = MassiveAdjustedCollector(
        base_dir=config.DATA_BASE_DIR,
        api_key=config.POLYGON_API_KEY,
        base_url=config.POLYGON_BASE_URL,
    )
    await collector.collect_all_splits()

    all_symbols = await polygon.all_symbols()
    # all_symbols = all_symbols[:10]

    n_workers = multiprocessing.cpu_count()
    batch_size = math.ceil(len(all_symbols) / n_workers)
    batches = [
        (all_symbols[i : i + batch_size], now)
        for i in range(0, len(all_symbols), batch_size)
    ]

    with multiprocessing.Pool(n_workers) as pool:
        pool.map(_run_batch, batches)


if __name__ == "__main__":
    asyncio.run(run_data_collect())
