import asyncio
import logging
import time
from datetime import date

import pandas as pd

from fastscanner.adapters.candle.partitioned_csv import PartitionedCSVCandlesProvider
from fastscanner.adapters.candle.polygon import PolygonCandlesProvider
from fastscanner.adapters.fundamental.eodhd import EODHDFundamentalStore
from fastscanner.adapters.holiday.exchange_calendars import (
    ExchangeCalendarsPublicHolidaysStore,
)
from fastscanner.pkg import config
from fastscanner.pkg.logging import load_logging_config
from fastscanner.services.registry import ApplicationRegistry
from fastscanner.services.scanners.lib.parabolic import ATRParabolicDownScanner

load_logging_config()
logger = logging.getLogger(__name__)


async def run():
    polygon = PolygonCandlesProvider(
        config.POLYGON_BASE_URL,
        config.POLYGON_API_KEY,
        max_requests_per_sec=50,
    )
    candles = PartitionedCSVCandlesProvider(polygon)
    holidays = ExchangeCalendarsPublicHolidaysStore()
    fundamentals = EODHDFundamentalStore(
        config.EOD_HD_BASE_URL,
        config.EOD_HD_API_KEY,
    )
    ApplicationRegistry.init(candles, fundamentals, holidays)

    start_date = date(2023, 1, 1)
    end_date = date(2023, 3, 31)
    freq = "5min"
    symbols = (await polygon.all_symbols())[:100]
    result: pd.DataFrame | None = None
    scanner = ATRParabolicDownScanner(
        min_adv=1_000_000, min_adr=0.1, min_high_low_ratio=0.01, min_volume=10_000
    )

    logger.info(f"Running scanner for {len(symbols)} symbols")
    start_time = time.time()
    for i, symbol in enumerate(symbols, start=1):
        df = await scanner.scan(
            symbol=symbol,
            start=start_date,
            end=end_date,
            freq=freq,
        )
        if i % 20 == 0:
            end_time = time.time()
            time_left = (end_time - start_time) * (len(symbols) - i) / i
            logger.info(
                f"Processed {i} symbols. Estimated time left: {time_left:.2f} seconds"
            )

        if df.empty:
            continue

        df["symbol"] = symbol
        if result is None:
            result = df
        else:
            result = pd.concat([result, df])

    logger.info(
        f"Finished processing {len(symbols)} symbols in {time.time() - start_time:.4f} seconds"
    )
    if result is None:
        logger.info("No rows passed the given scanner criteria")
        return

    end_time = time.time()
    matching_symbols = result["symbol"].unique()
    logger.info(f"Found {len(matching_symbols)} matching symbols")
    logger.info(f"Average rows per symbol: {len(result) / len(matching_symbols):.2f}")
    logger.info(
        f"Processing rate: {len(symbols) / (end_time - start_time):.2f} symbols/second"
    )
    logger.info(result.head(10))


if __name__ == "__main__":
    asyncio.run(run())
