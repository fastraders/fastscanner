import asyncio
import logging
import sys
import time
from datetime import date
from pathlib import Path

from fastscanner.adapters.candle.parquet import ParquetCandlesProvider
from fastscanner.pkg import config
from fastscanner.pkg.logging import load_logging_config

load_logging_config()
logger = logging.getLogger(__name__)


async def main():
    provider = ParquetCandlesProvider(config.POLYGON_BASE_URL, config.POLYGON_API_KEY)
    start_dt = date(2023, 1, 1)
    end_dt = date(2023, 12, 1)
    freq = "1h"
    symbol = "AAPL"
    # Initial fetch should take longer as it calls the API
    df = await provider.get(
        symbol=symbol,
        start=start_dt,
        end=end_dt,
        freq=freq,
    )
    n = 100
    start = time.time()
    logger.info(f"Starting {n} requests...")
    for i in range(n):
        df = await provider.get(
            symbol=symbol,
            start=start_dt,
            end=end_dt,
            freq=freq,
        )
    end = time.time()
    logger.info(f"Time taken: {end - start:.2f} seconds")
    logger.info(f"Time taken per request: {(end - start) * 1000 / n:.2f} ms")


if __name__ == "__main__":
    asyncio.run(main())
