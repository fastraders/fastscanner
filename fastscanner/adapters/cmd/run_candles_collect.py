import asyncio
import logging
import math
import multiprocessing
import multiprocessing.pool
import os
import time
from datetime import date, timedelta

import pandas as pd

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.pkg import config
from fastscanner.pkg.datetime import LOCAL_TIMEZONE_STR
from fastscanner.pkg.logging import load_logging_config

load_logging_config()
logger = logging.getLogger(__name__)


async def _collect(symbol: str, candles: PartitionedCSVCandlesProvider) -> None:
    end_date = pd.Timestamp.now(tz=LOCAL_TIMEZONE_STR).date() - timedelta(days=1)
    start_date = end_date - timedelta(days=365)
    cache_empty = False
    for _ in range(15):
        if not cache_empty:
            cache_empty = await candles.cache_all_freqs(symbol, start_date, end_date)
        else:
            await candles.cache_all_freqs_empty(symbol, start_date, end_date)

        end_date = start_date - timedelta(days=1)
        start_date = end_date - timedelta(days=365)
    logger.info(f"Collected data for {symbol} from {start_date}")


async def _collect_batch(symbols: list[str]) -> None:
    polygon = PolygonCandlesProvider(
        config.POLYGON_BASE_URL, config.POLYGON_API_KEY, max_requests_per_sec=9
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
        asyncio.create_task(_collect(symbol, candles), name=symbol)
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


def _run_batch(batch: list[str]) -> None:
    asyncio.run(_collect_batch(batch))


async def run_data_collect():
    polygon = PolygonCandlesProvider(config.POLYGON_BASE_URL, config.POLYGON_API_KEY)

    all_symbols = await polygon.all_symbols()
    n_workers = multiprocessing.cpu_count()
    batch_size = math.ceil(len(all_symbols) / n_workers)
    batches = [
        all_symbols[i : i + batch_size] for i in range(0, len(all_symbols), batch_size)
    ]

    with multiprocessing.Pool(n_workers) as pool:
        pool.map(_run_batch, batches)


if __name__ == "__main__":
    asyncio.run(run_data_collect())
