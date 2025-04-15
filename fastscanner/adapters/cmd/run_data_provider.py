import logging
import time
from datetime import date

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVBarsProvider
from fastscanner.pkg.logging import load_logging_config

load_logging_config()
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    provider = PartitionedCSVBarsProvider()
    df = provider.get(
        symbol="AAPL",
        start=date(2023, 1, 1),
        end=date(2023, 10, 1),
        freq="10min",
    )
    start = time.time()
    n = 100
    logger.info(f"Starting {n} requests...")
    for i in range(n):
        df = provider.get(
            symbol="AAPL",
            start=date(2023, 1, 1),
            end=date(2023, 3, 1),
            freq="3min",
        )
    end = time.time()
    logger.info(f"Time taken: {end - start:.2f} seconds")
    logger.info(f"Time taken per request: {(end - start) * 1000 / n:.2f} ms")
