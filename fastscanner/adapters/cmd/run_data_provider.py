import logging
import time
from datetime import date

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVBarsProvider
from fastscanner.adapters.candle.parquet import ParquetBarsProvider

from fastscanner.pkg.logging import load_logging_config
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
load_logging_config()
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    provider = ParquetBarsProvider()
    start_dt = date(2013, 1, 1)
    end_dt = date(2023, 10, 1)
    freq = "1h"
    symbol = "AAPL"
    # Initial fetch should take longer as it calls the API
    df = provider.get(
        symbol=symbol,
        start=start_dt,
        end=end_dt,
        freq=freq,
    )
    n = 100
    start = time.time()
    logger.info(f"Starting {n} requests...")
    for i in range(n):
        df = provider.get(
            symbol=symbol,
            start=start_dt,
            end=end_dt,
            freq=freq,
        )
    end = time.time()
    logger.info(f"Time taken: {end - start:.2f} seconds")
    logger.info(f"Time taken per request: {(end - start) * 1000 / n:.2f} ms")
