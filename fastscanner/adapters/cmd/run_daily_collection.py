import argparse
import asyncio
import logging
import math
import multiprocessing
import os
import time
from datetime import date, timedelta

import uvloop

from fastscanner.adapters.candle.massive_adjusted import MassiveAdjustedCollector
from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.pkg import config
from fastscanner.pkg.clock import ClockRegistry, FixedClock, LocalClock
from fastscanner.pkg.http import MaxRetryError
from fastscanner.pkg.logging import load_logging_config
from fastscanner.pkg.observability import init_metrics, metrics

load_logging_config()
logger = logging.getLogger(__name__)


def _classify_failure(ex: BaseException) -> str:
    if isinstance(ex, MaxRetryError):
        return "max_retries"
    name = type(ex).__name__
    if name == "HTTPStatusError":
        return "http_status"
    if name == "EmptyDataError":
        return "empty_data"
    if name in ("OSError", "PermissionError", "FileNotFoundError"):
        return "io_error"
    return "other"


async def _collect_batch(symbols: list[str]) -> None:
    init_metrics(role="daily_collection")
    metrics.daily_set_worker_active(True)
    metrics.daily_set_progress(total=len(symbols), remaining=len(symbols))
    ClockRegistry.set(FixedClock(LocalClock().now()))
    polygon = PolygonCandlesProvider(
        config.POLYGON_BASE_URL,
        config.POLYGON_API_KEY,
        max_requests_per_sec=2,
        max_concurrent_requests=2,
    )
    candles = PartitionedCSVCandlesProvider(polygon)

    logger.info(f"Collecting daily data for {len(symbols)} symbols")
    task_starts: dict[str, float] = {}
    tasks = []
    for symbol in symbols:
        task_starts[symbol] = time.perf_counter()
        tasks.append(
            asyncio.create_task(
                candles.collect_expired_data(symbol, ["1min", "2min", "1d"]),
                name=symbol,
            )
        )
    completed_tasks = 0
    failed_symbols: list[str] = []
    total = len(symbols)
    try:
        while len(tasks) > 0:
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                name = task.get_name()
                duration = time.perf_counter() - task_starts.get(
                    name, time.perf_counter()
                )
                if (ex := task.exception()) is not None:
                    logger.exception(ex)
                    logger.error(f"Error collecting data for {name}: {ex}")
                    failed_symbols.append(name)
                    metrics.daily_symbol_failure(_classify_failure(ex))
                    metrics.daily_symbol_processed("failed", duration)
                else:
                    metrics.daily_symbol_processed("ok", duration)
                completed_tasks += 1
            metrics.daily_set_progress(total=total, remaining=total - completed_tasks)
    finally:
        metrics.daily_set_worker_active(False)
    logger.info(
        f"Finished collecting data for {len(symbols) - len(failed_symbols)} symbols"
    )
    if failed_symbols:
        logger.error(f"Failed to collect daily data for {len(failed_symbols)} symbols")
        logger.error(f"Failed symbols: {failed_symbols}")


def _run_batch(batch: list[str]) -> None:
    uvloop.run(_collect_batch(batch))


async def collect_daily_data(only_active: bool = False) -> None:
    init_metrics(role="daily_collection")
    logger.error("Starting daily data collection")
    ClockRegistry.set(FixedClock(LocalClock().now()))
    provider = PolygonCandlesProvider(
        base_url=config.POLYGON_BASE_URL,
        api_key=config.POLYGON_API_KEY,
        max_requests_per_sec=20,
        max_concurrent_requests=20,
    )
    adjusted_provider = MassiveAdjustedCollector(
        base_dir=os.path.join(config.DATA_BASE_DIR, "data"),
        api_key=config.POLYGON_API_KEY,
        base_url=config.POLYGON_BASE_URL,
    )
    await adjusted_provider.collect_latest_splits()

    if only_active:
        all_symbols = await provider.active_symbols()
    else:
        all_symbols = await provider.all_symbols()
    # all_symbols = all_symbols[:100]
    # all_symbols = ["SOPA", "RDAC", "APVO"]

    n_workers = multiprocessing.cpu_count()
    batch_size = math.ceil(len(all_symbols) / n_workers)
    batches = [
        all_symbols[i : i + batch_size] for i in range(0, len(all_symbols), batch_size)
    ]

    with multiprocessing.Pool(n_workers) as pool:
        pool.map(_run_batch, batches)

    # collector = PolygonCandlesFromTradesCollector(
    #     base_url=config.MASSIVE_FILES_BASE_URL,
    #     base_trades_dir=config.TRADES_DATA_DIR,
    #     base_candles_dir=os.path.join(config.DATA_BASE_DIR, "data", "candles"),
    #     aws_access_key=config.MASSIVE_FILES_ACCESS_KEY,
    #     aws_secret_key=config.MASSIVE_FILES_SECRET_KEY,
    #     max_concurrency=10,
    # )
    # await collector.collect_latest(["5s"])
    logger.info("Data collection completed for all symbols")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run daily data collection")
    parser.add_argument(
        "--only-active",
        action="store_true",
        help="Collect data only for active symbols instead of all symbols",
        default=False,
    )
    args = parser.parse_args()

    asyncio.run(collect_daily_data(only_active=args.only_active))


if __name__ == "__main__":
    main()
