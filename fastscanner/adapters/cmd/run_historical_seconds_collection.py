import asyncio
import logging
import os.path
from time import time

from fastscanner.adapters.candle.polygon_trades import PolygonCandlesFromTradesCollector
from fastscanner.pkg import config
from fastscanner.pkg.clock import ClockRegistry, FixedClock, LocalClock
from fastscanner.pkg.logging import load_logging_config

load_logging_config()
logger = logging.getLogger(__name__)


async def run():
    now = LocalClock().now()
    ClockRegistry.set(FixedClock(now))
    collector = PolygonCandlesFromTradesCollector(
        base_url=config.MASSIVE_FILES_BASE_URL,
        base_trades_dir=config.TRADES_DATA_DIR,
        base_candles_dir=os.path.join(config.DATA_BASE_DIR, "data", "candles"),
        aws_access_key=config.MASSIVE_FILES_ACCESS_KEY,
        aws_secret_key=config.MASSIVE_FILES_SECRET_KEY,
        max_concurrency=10,
    )
    logger.info("Starting historical seconds collection")
    start_t = time()
    # today = ClockRegistry.clock.today()
    # start_year = 2025
    # end_year = today.year
    # end_month = today.month
    start_year = 2005
    end_year = 2005
    end_month = 12
    total_months = (end_year - start_year) * 12 + end_month
    curr_months = 0
    for year in range(start_year, end_year + 1):
        month_limit = end_month if year == end_year else 12
        for month in range(1, month_limit + 1):
            logger.info(f"Collecting historical seconds for {year}-{month:02d}")
            await collector.collect(year, month)
            curr_months += 1
            end_t = time()
            logger.info(
                f"Finished historical seconds collection for {year}-{month:02d}"
            )
            logger.info(
                f"Estimated remaining time: "
                f"{(end_t - start_t) * (total_months - curr_months) / curr_months:.2f} seconds"
            )


if __name__ == "__main__":
    asyncio.run(run())
